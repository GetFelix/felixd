//! How many workloads one Felix cluster carries. Every workload has its own
//! `.out` and `.snap` stream, so N workloads are 2N single-shard streams, each
//! with three replicas. This creates N counter workloads against the
//! three-broker cluster (`tests/felix/cluster.yaml`), waits until three agents
//! hold them all, and then kills an agent and a broker in turn.
//!
//! Skipped unless `FELIXD_SCALE_COUNT` is set; the `Scale` workflow sets it.
//! `FELIXD_SCALE_OUT` is the directory for `scale-<N>-ttl<T>.json` and
//! `scale-<N>-ttl<T>.row.md` (default `.`). `FELIXD_SCALE_TIMEOUT_SECS` bounds
//! creating and starting the workloads (default 1200), `FELIXD_SCALE_FAILOVER_SECS`
//! each later phase (default 300), and `FELIXD_SCALE_LEASE_TTL_SECS` is the
//! agents' lease TTL (default 10). A phase scans the output streams in rounds
//! of at most 10 s, each read has 3 s, and the leaders are checked every round,
//! so a stuck read cannot stretch what the failover figures measure. It also needs what `cluster.rs` needs:
//! `FELIXD_CLUSTER` and `FELIX_OPERATOR_TOKEN_FILE`.
//!
//! `FELIXD_SCALE_CP_DOWN=true` (with the brokers electing, `FELIXD_ELECTION=true`) runs the broker
//! kill with the control plane paused instead: see `control_plane_down`.

mod common;

use std::collections::{BTreeMap, HashSet};
use std::future::Future;
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use common::{Agent, INPUT, TestFelix, replication_factor, test_felix};
use felixd_agent::admin::{StreamKind, ensure_stream};
use felixd_agent::felix::Felix;
use felixd_agent::settings::Settings;
use felixd_agent::submit::{Submission, submit};
use felixd_core::lease::{DEFAULT_TTL, Lease, decode_epoch};
use felixd_core::{names, output};
use serde_json::{Value, json};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

const NODES: [&str; 3] = ["a", "b", "c"];
/// How many `felixd run` calls are in flight at once.
const SUBMITS: usize = 16;
/// How many output streams are read at once.
const READERS: usize = 32;
const POLL: Duration = Duration::from_secs(2);
/// The most one scan of the output streams may take. What it did not reach
/// counts as still without output and is read again next round.
const SCAN_ROUND: Duration = Duration::from_secs(10);
/// The most one read or probe may take before it counts as a miss.
const PROBE: Duration = Duration::from_secs(3);
const SAMPLE_MEMORY_EVERY: Duration = Duration::from_secs(20);

fn count(i: u64) -> Vec<u8> {
    i.to_be_bytes().to_vec()
}

fn secs(since: Instant) -> f64 {
    (since.elapsed().as_secs_f64() * 10.0).round() / 10.0
}

/// The result file, rewritten after each phase so a run that is cut off still
/// leaves what it measured.
struct Report {
    dir: PathBuf,
    n: usize,
    /// Names the result files.
    tag: String,
    body: serde_json::Map<String, Value>,
    peak: BTreeMap<String, f64>,
}

impl Report {
    fn set(&mut self, key: &str, value: Value) {
        self.body.insert(key.to_owned(), value);
        self.save();
    }

    fn note(&mut self, text: String) {
        eprintln!("note: {text}");
        let notes = self
            .body
            .entry("notes")
            .or_insert_with(|| json!([]))
            .as_array_mut()
            .unwrap();
        notes.push(json!(text));
        self.save();
    }

    fn memory(&mut self, phase: &str, sample: &Memory) {
        for (name, mb) in sample.containers.iter().chain(&sample.agents) {
            let peak = self.peak.entry(name.clone()).or_default();
            *peak = peak.max(*mb);
        }
        let mut memory = self
            .body
            .get("memory_mb")
            .cloned()
            .unwrap_or_else(|| json!({}));
        memory[phase] = sample.json();
        memory["peak"] = json!(self.peak);
        self.set("memory_mb", memory);
    }

    fn save(&self) {
        let path = self.dir.join(format!("scale-{}.json", self.tag));
        let text = serde_json::to_string_pretty(&self.body).unwrap();
        std::fs::write(path, text + "\n").unwrap();
        let path = self.dir.join(format!("scale-{}.row.md", self.tag));
        std::fs::write(path, self.row()).unwrap();
    }

    /// One row of the summary table.
    fn row(&self) -> String {
        let root = Value::Object(self.body.clone());
        let get = |path: &str| {
            root.pointer(&format!("/{}", path.replace('.', "/")))
                .cloned()
        };
        let number = |path: &str, unit: &str| match get(path).and_then(|v| v.as_f64()) {
            Some(x) => format!("{x}{unit}"),
            None => "no".to_owned(),
        };
        let largest = |phase: &str, wanted: &str| {
            let sample = get(&format!("memory_mb.{phase}.containers"));
            let sizes = sample.as_ref().and_then(Value::as_object);
            let sizes = sizes.into_iter().flatten();
            let max = sizes
                .filter(|(name, _)| name.starts_with(wanted))
                .filter_map(|(_, mb)| mb.as_f64())
                .fold(None, |a: Option<f64>, b| Some(a.map_or(b, |a| a.max(b))));
            max.map_or("no".to_owned(), |mb| format!("{mb:.0} MiB"))
        };
        // Only a control-plane-down run has these cells; the table header
        // always has the columns.
        let down = if get("cp_down_failover").is_some() {
            format!(
                " {} | {} | {} | {} |",
                number("cp_down_failover.outputs_secs", " s"),
                number("cp_down_failover.leaders_moved_secs", " s"),
                number("cp_down_after_restart.outputs_secs", " s"),
                number("cp_back.outputs_secs", " s"),
            )
        } else {
            String::new()
        };
        format!(
            "| {} | {}s | {} | {} | {} | {} | {} | {} | {} | {} | {} |{}\n",
            self.n,
            get("lease_ttl_secs").map_or("no".to_owned(), |v| v.to_string()),
            get("streams.total").map_or("no".to_owned(), |v| v.to_string()),
            number("create.secs", " s"),
            number("running.secs", " s"),
            largest("running", "broker"),
            largest("running", "controlplane"),
            number("first_output_secs", " s"),
            number("agent_failover.outputs_secs", " s"),
            number("broker_failover.outputs_secs", " s"),
            number("broker_failover.leaders_moved_secs", " s"),
            down,
        )
    }
}

/// What the cluster's containers and the agents use.
struct Memory {
    /// Docker's memory figure for each container, in MiB.
    containers: BTreeMap<String, f64>,
    /// Each agent's resident set, in MiB.
    agents: BTreeMap<String, f64>,
    /// Space in use on the runner's disk, in MiB.
    disk_used_mb: Option<f64>,
    /// Each container's processor use since docker's last reading, in percent
    /// of one processor.
    container_cpu: BTreeMap<String, f64>,
    /// Each agent's processor use since the previous sample, in percent of
    /// one processor.
    agent_cpu: BTreeMap<String, f64>,
    /// The runner's one-minute load average.
    load: Option<f64>,
}

impl Memory {
    fn json(&self) -> Value {
        json!({
            "containers": self.containers,
            "agents": self.agents,
            "disk_used_mb": self.disk_used_mb,
            "cpu_percent": {"containers": self.container_cpu, "agents": self.agent_cpu},
            "load": self.load,
        })
    }
}

fn compose_command() -> Vec<String> {
    let command = std::env::var("FELIXD_COMPOSE").unwrap_or_else(|_| "docker compose".into());
    command.split_whitespace().map(str::to_owned).collect()
}

fn compose(args: &[&str]) -> Result<()> {
    let file = std::env::var("FELIXD_CLUSTER").context("FELIXD_CLUSTER is not set")?;
    let words = compose_command();
    let status = Command::new(&words[0])
        .args(&words[1..])
        .args(["-f", &file])
        .args(args)
        .status()?;
    if !status.success() {
        bail!("compose {args:?} failed");
    }
    Ok(())
}

/// The output of a compose command, whether or not it succeeded.
fn compose_output(args: &[&str]) -> Result<String> {
    let file = std::env::var("FELIXD_CLUSTER").context("FELIXD_CLUSTER is not set")?;
    let words = compose_command();
    let out = Command::new(&words[0])
        .args(&words[1..])
        .args(["-f", &file])
        .args(args)
        .output()?;
    Ok(String::from_utf8_lossy(&out.stdout).into_owned() + &String::from_utf8_lossy(&out.stderr))
}

/// The shards brokers began standing for election on since `since`, as
/// `(stream, shard, old leader)`. A broker logs it when it takes over from a
/// leader it cannot reach, so it shows elections without the control plane.
fn elections(since: &str) -> Vec<(String, String, String)> {
    let logs = compose_output(&[
        "logs",
        "--no-color",
        "--since",
        since,
        "broker-1",
        "broker-2",
        "broker-3",
    ])
    .unwrap_or_default();
    parse_elections(&logs)
}

fn parse_elections(logs: &str) -> Vec<(String, String, String)> {
    let field = |line: &str, key: &str| {
        let rest = line.split(&format!("{key}=")).nth(1)?;
        Some(rest.split_whitespace().next()?.trim_matches('"').to_owned())
    };
    let mut found: Vec<_> = logs
        .lines()
        .filter(|line| line.contains("standing for election"))
        .filter_map(|line| {
            Some((
                field(line, "stream")?,
                field(line, "shard")?,
                field(line, "leader")?,
            ))
        })
        .collect();
    found.sort();
    found.dedup();
    found
}

fn now_rfc3339() -> String {
    time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap()
}

/// Parses docker's `1.5GiB`, `512MiB` or `900kB` into MiB.
fn parse_size(text: &str) -> Option<f64> {
    let text = text.trim();
    let split = text.find(|c: char| c.is_ascii_alphabetic())?;
    let (number, unit) = text.split_at(split);
    let number: f64 = number.parse().ok()?;
    let mib = match unit {
        "B" => 1.0 / (1024.0 * 1024.0),
        "KiB" => 1.0 / 1024.0,
        "kB" => 1000.0 / (1024.0 * 1024.0),
        "MiB" => 1.0,
        "MB" => 1_000_000.0 / (1024.0 * 1024.0),
        "GiB" => 1024.0,
        "GB" => 1_000_000_000.0 / (1024.0 * 1024.0),
        _ => return None,
    };
    Some((number * mib * 10.0).round() / 10.0)
}

/// The service name a container runs, such as `broker-2`.
fn service_of(container: &str) -> Option<String> {
    if let Some(at) = container.find("broker-") {
        let digits = container[at + "broker-".len()..]
            .chars()
            .take_while(char::is_ascii_digit)
            .collect::<String>();
        return Some(format!("broker-{digits}"));
    }
    container
        .contains("controlplane")
        .then(|| "controlplane".to_owned())
}

fn sample_memory(agents: &[Agent]) -> Memory {
    let command = compose_command();
    let stats = Command::new(&command[0])
        .args([
            "stats",
            "--no-stream",
            "--format",
            "{{.Name}}|{{.MemUsage}}|{{.CPUPerc}}",
        ])
        .output();
    let (mut containers, mut container_cpu) = (BTreeMap::new(), BTreeMap::new());
    if let Ok(out) = stats {
        for line in String::from_utf8_lossy(&out.stdout).lines() {
            let mut fields = line.split('|');
            let (Some(name), Some(usage), Some(cpu)) =
                (fields.next(), fields.next(), fields.next())
            else {
                continue;
            };
            let used = usage.split('/').next().and_then(parse_size);
            let cpu = cpu.trim().trim_end_matches('%').parse::<f64>().ok();
            if let (Some(service), Some(mb)) = (service_of(name), used) {
                containers.insert(service.clone(), mb);
                container_cpu.extend(cpu.map(|c| (service, c)));
            }
        }
    }
    let (mut resident, mut agent_cpu) = (BTreeMap::new(), BTreeMap::new());
    for agent in agents {
        if let Some(cpu) = process_cpu(agent.pid()) {
            agent_cpu.insert(format!("agent-{}", agent.node_id), cpu);
        }
        let out = Command::new("ps")
            .args(["-o", "rss=", "-p", &agent.pid().to_string()])
            .output();
        let kb = out.ok().and_then(|o| {
            String::from_utf8_lossy(&o.stdout)
                .trim()
                .parse::<f64>()
                .ok()
        });
        if let Some(kb) = kb {
            resident.insert(format!("agent-{}", agent.node_id), (kb / 1024.0).round());
        }
    }
    let disk = Command::new("df").args(["-Pm", "/"]).output().ok();
    let disk_used_mb = disk.and_then(|o| {
        let text = String::from_utf8_lossy(&o.stdout).into_owned();
        text.lines().nth(1)?.split_whitespace().nth(2)?.parse().ok()
    });
    let load = std::fs::read_to_string("/proc/loadavg")
        .ok()
        .and_then(|l| l.split_whitespace().next()?.parse().ok());
    Memory {
        containers,
        agents: resident,
        disk_used_mb,
        container_cpu,
        agent_cpu,
        load,
    }
}

/// A process's processor use since this was last called for it, in percent
/// of one processor. Linux only, and `None` the first time.
fn process_cpu(pid: u32) -> Option<f64> {
    static LAST: Mutex<BTreeMap<u32, (Instant, u64)>> = Mutex::new(BTreeMap::new());
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    // Fields 14 and 15 are user and system time in clock ticks (100 a second);
    // the command name in field 2 may hold spaces, so count from its end.
    let rest = stat.rsplit_once(')')?.1;
    let mut fields = rest.split_whitespace().skip(11);
    let ticks: u64 = fields.next()?.parse::<u64>().ok()? + fields.next()?.parse::<u64>().ok()?;
    let now = Instant::now();
    let before = LAST.lock().unwrap().insert(pid, (now, ticks))?;
    let seconds = now.duration_since(before.0).as_secs_f64();
    let percent = (ticks - before.1) as f64 / seconds;
    Some((percent * 10.0).round() / 10.0)
}

/// The runner: processors and memory in MiB.
fn runner() -> Value {
    let cpus = std::thread::available_parallelism().map_or(0, |n| n.get());
    let meminfo = std::fs::read_to_string("/proc/meminfo").unwrap_or_default();
    let total_kb: u64 = meminfo
        .lines()
        .find_map(|l| l.strip_prefix("MemTotal:"))
        .and_then(|l| l.split_whitespace().next())
        .and_then(|n| n.parse().ok())
        .unwrap_or(0);
    json!({"cpus": cpus, "memory_mb": total_kb / 1024})
}

/// Every shard assignment of the test's namespaces.
async fn assignments(t: &TestFelix) -> Result<Vec<Value>> {
    let url = format!(
        "{}/v1/shard-assignments/snapshot",
        t.settings.controlplane_url
    );
    let file = std::env::var("FELIX_OPERATOR_TOKEN_FILE")?;
    let token = std::fs::read_to_string(file)?.trim().to_owned();
    let body: Value = tokio::task::spawn_blocking(move || -> Result<Value> {
        let mut response = ureq::get(&url)
            .config()
            .timeout_global(Some(PROBE))
            .build()
            .header("Authorization", &format!("Bearer {token}"))
            .call()?;
        Ok(response.body_mut().read_json()?)
    })
    .await??;
    let system = names::system_namespace(&t.settings.namespace);
    let items = body["items"].as_array().context("no items")?.iter();
    Ok(items
        .filter(|item| {
            item["namespace"] == t.settings.namespace.as_str() || item["namespace"] == system
        })
        .cloned()
        .collect())
}

/// How many shards each broker leads.
fn led_by(items: &[Value]) -> BTreeMap<String, usize> {
    let mut led = BTreeMap::new();
    for item in items {
        *led.entry(item["leader"].as_str().unwrap_or("none").to_owned())
            .or_default() += 1;
    }
    led
}

struct Census {
    /// Leases whose holder has its epoch, so is running the workload.
    held: usize,
    by_owner: BTreeMap<String, usize>,
}

async fn census(felix: &Felix) -> Result<Census> {
    let mut census = Census {
        held: 0,
        by_owner: BTreeMap::new(),
    };
    for (_, value) in felix.entries(names::LEASES).await? {
        let lease = Lease::decode(&value)?;
        if lease.epoch > 0 {
            census.held += 1;
            *census.by_owner.entry(lease.owner).or_default() += 1;
        }
    }
    Ok(census)
}

struct Created {
    latencies: Vec<Duration>,
    failed: usize,
    first_error: Option<String>,
    elapsed: Duration,
}

/// `felixd run` for each of `ids`, as the command does it.
async fn create(
    settings: Settings,
    module: Vec<u8>,
    ids: Vec<String>,
    done: Arc<AtomicUsize>,
) -> Created {
    let started = Instant::now();
    let permits = Arc::new(Semaphore::new(SUBMITS));
    let mut tasks = JoinSet::new();
    for id in ids {
        let (settings, module, permits) = (settings.clone(), module.clone(), permits.clone());
        tasks.spawn(async move {
            let _permit = permits.acquire().await.unwrap();
            let begun = Instant::now();
            let mut last = None;
            for _ in 0..3 {
                match submit(&settings, Submission::new(&id, module.clone(), INPUT)).await {
                    Ok(_) => return (Ok(()), begun.elapsed()),
                    Err(error) => last = Some(format!("{id}: {error:#}")),
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            (Err(last.unwrap()), begun.elapsed())
        });
    }
    let mut created = Created {
        latencies: Vec::new(),
        failed: 0,
        first_error: None,
        elapsed: Duration::ZERO,
    };
    while let Some(finished) = tasks.join_next().await {
        let (result, took) = finished.unwrap();
        let so_far = done.fetch_add(1, Ordering::Relaxed) + 1;
        if so_far.is_multiple_of(500) {
            eprintln!("{so_far} workloads submitted after {:.0}s", secs(started));
        }
        created.latencies.push(took);
        if let Err(error) = result {
            created.failed += 1;
            created.first_error.get_or_insert(error);
        }
    }
    created.elapsed = started.elapsed();
    created
}

fn percentile(sorted: &[Duration], p: f64) -> u128 {
    if sorted.is_empty() {
        return 0;
    }
    let at = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted[at].as_millis()
}

/// Whether `id`'s output stream holds an output for input `target` or later.
async fn has_output(felix: &Felix, id: &str, target: u64) -> bool {
    let stream = names::output_stream(id);
    let Ok(tail) = felix.read_tail(&stream).await else {
        return false;
    };
    let Ok(records) = felix
        .read_range(&stream, tail.saturating_sub(8), tail)
        .await
    else {
        return false;
    };
    records.iter().any(|record| {
        output::decode(&record.payload)
            .ok()
            .and_then(|r| r.written())
            .is_some_and(|w| w.header.input_offset >= target)
    })
}

/// The workloads among `ids` with no output for `target` yet. One scan takes
/// at most `SCAN_ROUND` and one workload's read at most `PROBE`, so a stuck
/// read costs its workload a miss instead of the whole phase.
async fn without_output(
    felix: &Arc<Felix>,
    ids: Vec<String>,
    target: u64,
    round: Duration,
) -> Vec<String> {
    let felix = felix.clone();
    let check = move |id: String| {
        let felix = felix.clone();
        async move { has_output(&felix, &id, target).await }
    };
    scan(ids, READERS, PROBE, round, check).await
}

/// Runs `check` on every id from `readers` parallel readers and returns the
/// ids it did not confirm: those it answered `false` for, those whose check
/// took over `probe`, and those not reached within `round`. Ids confirmed
/// before `round` ends stay confirmed.
async fn scan<F, Fut>(
    ids: Vec<String>,
    readers: usize,
    probe: Duration,
    round: Duration,
    check: F,
) -> Vec<String>
where
    F: Fn(String) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = bool> + Send + 'static,
{
    let confirmed = Arc::new(Mutex::new(HashSet::new()));
    let size = ids.len().div_ceil(readers).max(1);
    let mut tasks = JoinSet::new();
    for chunk in ids.chunks(size) {
        let (check, confirmed, chunk) = (check.clone(), confirmed.clone(), chunk.to_vec());
        tasks.spawn(async move {
            for id in chunk {
                if let Ok(true) = tokio::time::timeout(probe, check(id.clone())).await {
                    confirmed.lock().unwrap().insert(id);
                }
            }
        });
    }
    let drain = async {
        while let Some(part) = tasks.join_next().await {
            part.unwrap();
        }
    };
    // Past the round the readers are dropped (aborted) with `tasks`.
    let _ = tokio::time::timeout(round, drain).await;
    let confirmed = confirmed.lock().unwrap();
    ids.into_iter()
        .filter(|id| !confirmed.contains(id))
        .collect()
}

/// Runs `work` for at most `limit`, and never past `deadline`.
async fn bounded<T>(
    limit: Duration,
    deadline: Instant,
    work: impl Future<Output = T>,
) -> Option<T> {
    let left = deadline.saturating_duration_since(Instant::now());
    tokio::time::timeout(limit.min(left), work).await.ok()
}

/// Publishes one input, retrying while the stream's leader is away, and
/// returns its offset and how long that took.
async fn publish_input(felix: &Felix, i: u64, within: Duration) -> Result<(u64, f64)> {
    let started = Instant::now();
    loop {
        match felix.publish(INPUT, count(i)).await {
            Ok(()) => break,
            Err(error) if started.elapsed() < within => {
                eprintln!("publish: {error:#}");
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            Err(error) => return Err(error),
        }
    }
    let took = secs(started);
    loop {
        match felix.read_tail(INPUT).await {
            // Nothing else publishes, so the record just written is the last.
            Ok(tail) => return Ok((tail - 1, took)),
            Err(error) if started.elapsed() < within => {
                eprintln!("tail: {error:#}");
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            Err(error) => return Err(error),
        }
    }
}

struct Run<'a> {
    t: &'a TestFelix,
    n: usize,
    ids: Vec<String>,
    phase: Duration,
    felix: Arc<Felix>,
    /// With the control plane down its assignments cannot be read, so the
    /// leaders have moved once brokers have stood for election on as many
    /// shards as the dead broker led: that count, and when the kill was.
    by_elections: Option<(usize, String)>,
}

impl Run<'_> {
    /// Whether no shard is led by `old` any more.
    async fn leaders_moved(&self, old: &str, deadline: Instant) -> bool {
        if let Some((led, since)) = &self.by_elections {
            let (since, old) = (since.clone(), old.to_owned());
            let found = tokio::task::spawn_blocking(move || elections(&since)).await;
            return found.is_ok_and(|found| found.iter().filter(|e| e.2 == old).count() >= *led);
        }
        let Some(Ok(items)) = bounded(PROBE, deadline, assignments(self.t)).await else {
            return false;
        };
        items.iter().all(|item| item["leader"] != old)
    }

    /// Opens a new connection, since the old one may still point at a dead
    /// broker. Not capped by the phase deadline: the next phase needs it.
    async fn reconnect(&mut self) -> bool {
        let connect = Felix::connect(&self.t.settings);
        match tokio::time::timeout(PROBE, connect).await {
            Ok(Ok(felix)) => {
                self.felix = Arc::new(felix);
                true
            }
            _ => false,
        }
    }

    /// Waits until every workload has an output for `target`, and notes when
    /// every lease is held by a live node and, if given, when no shard is led
    /// by `old_broker`. Each round scans the outputs for at most `SCAN_ROUND`
    /// and checks the leaders at the same time, all inside the phase.
    async fn recovery(
        &mut self,
        started: Instant,
        target: u64,
        old_owner: Option<&str>,
        old_broker: Option<&str>,
    ) -> Value {
        let (mut leases, mut leaders, mut outputs) = (None, None, None);
        let mut reconnected = false;
        let mut pending = self.ids.clone();
        let deadline = started + self.phase;
        loop {
            // The leaders are checked alongside the scan, every round, so
            // when they moved does not wait on reading the outputs.
            let round = SCAN_ROUND.min(deadline.saturating_duration_since(Instant::now()));
            let scan = without_output(&self.felix, pending.clone(), target, round);
            let check = async {
                match old_broker {
                    Some(old) if leaders.is_none() => self
                        .leaders_moved(old, deadline)
                        .await
                        .then(|| secs(started)),
                    _ => None,
                }
            };
            let (still, moved) = tokio::join!(scan, check);
            pending = still;
            leaders = leaders.or(moved);
            if pending.is_empty() {
                outputs = Some(secs(started));
                // The last round for this phase, so look once more.
                if leaders.is_none()
                    && let Some(old) = old_broker
                    && self.leaders_moved(old, deadline).await
                {
                    leaders = Some(secs(started));
                }
            }
            if leaders.is_some() && !reconnected {
                reconnected = self.reconnect().await;
            }
            if leases.is_none()
                && let Some(Ok(c)) = bounded(PROBE, deadline, census(&self.felix)).await
                && c.held >= self.n
                && old_owner.is_none_or(|o| !c.by_owner.contains_key(o))
            {
                leases = Some(secs(started));
            }
            eprintln!(
                "recovery {:.0}s: {} without output, leases back {:?}, leaders moved {:?}",
                secs(started),
                pending.len(),
                leases,
                leaders
            );
            if outputs.is_some() || Instant::now() >= deadline {
                break;
            }
            tokio::time::sleep(POLL.min(deadline.saturating_duration_since(Instant::now()))).await;
        }
        json!({
            "outputs_secs": outputs,
            "leases_secs": leases,
            "leaders_moved_secs": leaders,
            "without_output": pending.len(),
        })
    }
}

/// The lease TTL the agents run with: `FELIXD_SCALE_LEASE_TTL_SECS`, the
/// default 10 s if unset.
fn lease_ttl() -> Duration {
    std::env::var("FELIXD_SCALE_LEASE_TTL_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .map_or(DEFAULT_TTL, Duration::from_secs)
}

fn start(t: &TestFelix, node: &str, max: usize) -> Agent {
    let max = max.to_string();
    let args = ["--max-workloads", &max];
    t.start_node_with_args(node, "", lease_ttl(), &args)
}

fn seconds_from(name: &str, default: u64) -> Duration {
    let secs = std::env::var(name).ok().and_then(|s| s.parse().ok());
    Duration::from_secs(secs.unwrap_or(default))
}

/// The sum of every workload's last lease epoch, which is how many claims
/// have won so far.
async fn claims(felix: &Felix) -> Result<u64> {
    let mut total = 0;
    for (_, value) in felix.entries(names::EPOCHS).await? {
        total += decode_epoch(&value)?;
    }
    Ok(total)
}

async fn measure(t: &TestFelix, n: usize, report: &mut Report) -> Result<()> {
    // Creating and starting every workload, then each later phase.
    let build = seconds_from("FELIXD_SCALE_TIMEOUT_SECS", 1200);
    let phase = seconds_from("FELIXD_SCALE_FAILOVER_SECS", 300);
    let ids: Vec<String> = (0..n).map(|i| format!("w{i}")).collect();
    let module = felixd_testkit::workload("counter");
    let felix = Arc::new(Felix::connect(&t.settings).await?);
    let kind = StreamKind {
        replication_factor: replication_factor(),
    };
    ensure_stream(&t.settings, INPUT, kind).await?;
    let mut run = Run {
        t,
        n,
        ids: ids.clone(),
        phase,
        felix: felix.clone(),
        by_elections: None,
    };
    report.memory("baseline", &sample_memory(&[]));

    // The first submit creates the caches the agents watch, so they can start.
    let started = Instant::now();
    submit(&t.settings, Submission::new(&ids[0], module.clone(), INPUT)).await?;
    let mut agents: Vec<Agent> = NODES.iter().map(|node| start(t, node, n)).collect();
    let submitted = Arc::new(AtomicUsize::new(1));
    let mut creator = Some(tokio::spawn(create(
        t.settings.clone(),
        module.clone(),
        ids[1..].to_vec(),
        submitted.clone(),
    )));

    let (mut created, mut running) = (None, None);
    let mut curve: Vec<(f64, usize, Option<u64>)> = Vec::new();
    let (mut full_polls, mut polls, mut claimed) = (0, 0, None);
    let mut last_sample = Instant::now();
    let mut held = 0;
    loop {
        tokio::time::sleep(POLL).await;
        if creator.as_ref().is_some_and(|c| c.is_finished()) {
            let done = creator.take().unwrap().await?;
            let mut sorted = done.latencies.clone();
            sorted.sort();
            let first = done.first_error.clone();
            created = Some(json!({
                "secs": secs(started),
                "created": n - done.failed,
                "failed": done.failed,
                "first_error": first,
                "submit_ms": {
                    "p50": percentile(&sorted, 0.5),
                    "p95": percentile(&sorted, 0.95),
                    "max": percentile(&sorted, 1.0),
                },
            }));
            report.set("create", created.clone().unwrap());
        }
        match census(&felix).await {
            Ok(c) => {
                held = c.held;
                polls += 1;
                if polls % 5 == 1 {
                    claimed = claims(&felix).await.ok();
                }
                curve.push((secs(started), held, claimed));
                // Three polls in a row, so a workload that is lost and
                // claimed again does not count as running.
                full_polls = if held >= n { full_polls + 1 } else { 0 };
                if full_polls == 3 && running.is_none() {
                    running = Some(secs(started) - 2.0 * POLL.as_secs_f64());
                }
            }
            Err(error) => eprintln!("lease census: {error:#}"),
        }
        eprintln!(
            "{:.0}s: {} submitted, {held} running, {claimed:?} claims",
            secs(started),
            submitted.load(Ordering::Relaxed)
        );
        if last_sample.elapsed() > SAMPLE_MEMORY_EVERY {
            report.memory("during", &sample_memory(&agents));
            last_sample = Instant::now();
        }
        if created.is_some() && running.is_some() {
            break;
        }
        if started.elapsed() > build {
            report.note(format!(
                "after {}s, {} of {n} workloads were submitted and {held} running",
                build.as_secs(),
                submitted.load(Ordering::Relaxed)
            ));
            if let Some(creator) = creator.take() {
                creator.abort();
            }
            break;
        }
    }
    let step = (curve.len() / 60).max(1);
    let sampled: Vec<_> = curve.iter().step_by(step).collect();
    report.set(
        "running",
        json!({"secs": running, "reached": running.is_some(), "held": held, "curve": sampled}),
    );
    let items = assignments(t).await?;
    report.set(
        "streams",
        json!({"total": items.len(), "led_by": led_by(&items)}),
    );
    report.memory("running", &sample_memory(&agents));
    if let Ok(c) = census(&felix).await {
        report.set("held_by", json!(c.by_owner));
    }
    if running.is_none() {
        report.note("not every workload was running, so no failover was measured".into());
        return Ok(());
    }

    let (input, _) = publish_input(&felix, 0, phase).await?;
    let published = Instant::now();
    let mut pending = ids.clone();
    let mut first = None;
    while published.elapsed() < phase {
        let round = SCAN_ROUND.min(phase.saturating_sub(published.elapsed()));
        pending = without_output(&felix, pending, input, round).await;
        eprintln!("{} workloads without output", pending.len());
        if pending.is_empty() {
            first = Some(secs(published));
            break;
        }
        tokio::time::sleep(POLL).await;
    }
    report.set("first_output_secs", json!(first));
    report.memory("first_output", &sample_memory(&agents));
    if first.is_none() {
        report.note(format!("{} workloads had no output", pending.len()));
        return Ok(());
    }

    // An agent: the one holding the most workloads.
    let held_by = census(&felix).await?.by_owner;
    let (victim, share) = held_by.iter().max_by_key(|(_, held)| **held).unwrap();
    let (victim, share) = (victim.clone(), *share);
    eprintln!("killing agent {victim}, which holds {share}");
    let index = NODES.iter().position(|node| *node == victim).unwrap();
    agents[index].kill();
    let killed = Instant::now();
    let (input, accepted) = publish_input(&felix, 1, phase).await?;
    let mut found = run.recovery(killed, input, Some(&victim), None).await;
    found["killed"] = json!(victim);
    found["held"] = json!(share);
    found["publish_secs"] = json!(accepted);
    report.set("agent_failover", found);
    agents[index] = start(t, &victim, n);
    report.memory("agent_failover", &sample_memory(&agents));

    // A broker: the one leading the most shards.
    let items = assignments(t).await?;
    let led = led_by(&items);
    // The brokers lead equal numbers, so a tie goes to the highest name. A
    // shard with no leader is not a broker.
    let (broker, share) = led
        .iter()
        .filter(|(name, _)| name.as_str() != "none")
        .max_by_key(|(_, led)| **led)
        .context("no shard has a leader")?;
    let (broker, share) = (broker.clone(), *share);
    if std::env::var("FELIXD_SCALE_CP_DOWN").is_ok_and(|v| v == "true") {
        return control_plane_down(&mut run, report, &agents, &broker, share, &items).await;
    }
    eprintln!("killing {broker}, which leads {share} shards");
    compose(&["kill", &broker])?;
    let killed = Instant::now();
    let (input, accepted) = publish_input(&run.felix, 2, phase).await?;
    let mut found = run.recovery(killed, input, None, Some(&broker)).await;
    found["killed"] = json!(broker);
    found["led"] = json!(share);
    found["publish_secs"] = json!(accepted);
    let recovered = found["outputs_secs"].is_number();
    report.set("broker_failover", found);
    report.memory("broker_failover", &sample_memory(&agents));
    compose(&["up", "-d", "--wait", &broker])?;
    if !recovered {
        // Did the workloads come back once the broker did?
        let restarted = Instant::now();
        let mut found = run.recovery(restarted, input, None, None).await;
        found["after_broker_restart"] = json!(true);
        report.set("after_restart", found);
    }
    Ok(())
}

/// The broker kill with the control plane down the whole time. The control
/// plane is paused, not stopped: its catalog lives in memory, so a stop would
/// come back empty and no longer be the same control plane. Phases:
///
/// 1. `cp_down_failover`: the broker leading the most shards is killed; the
///    leaders must move through elections alone.
/// 2. `cp_down_after_restart`: the broker is started again, the control plane
///    still paused.
/// 3. `cp_back`: the control plane resumes. `elections_after` counts new
///    elections once it is back (churn), and `led` is its view of the leaders.
async fn control_plane_down(
    run: &mut Run<'_>,
    report: &mut Report,
    agents: &[Agent],
    broker: &str,
    share: usize,
    before: &[Value],
) -> Result<()> {
    let t = run.t;
    let notice = seconds_from("FELIXD_SCALE_CP_NOTICE_SECS", 30);
    report.set("cp_down", json!({"led_before": led_by(before)}));
    eprintln!("pausing the control plane");
    compose(&["pause", "felix-controlplane"])?;
    // Brokers learn it is gone from their failed syncs; log what they say.
    tokio::time::sleep(notice).await;
    let logs = compose_output(&[
        "logs",
        "--no-color",
        "--tail",
        "2000",
        "broker-1",
        "broker-2",
        "broker-3",
    ])
    .unwrap_or_default();
    let noticed = logs
        .lines()
        .filter(|l| {
            let l = l.to_lowercase();
            l.contains("control plane") || l.contains("controlplane")
        })
        .count();
    eprintln!("brokers logged {noticed} lines about the control plane after {notice:?}");
    report.set("cp_down_noticed_log_lines", json!(noticed));
    if assignments(t).await.is_ok() {
        report.note("the control plane still answered while paused".into());
    }

    eprintln!("killing {broker}, which leads {share} shards, control plane down");
    let killed_at = now_rfc3339();
    run.by_elections = Some((share, killed_at.clone()));
    compose(&["kill", broker])?;
    let killed = Instant::now();
    let (input, accepted) = publish_input(&run.felix, 2, run.phase).await?;
    let mut found = run.recovery(killed, input, None, Some(broker)).await;
    found["killed"] = json!(broker);
    found["led"] = json!(share);
    found["publish_secs"] = json!(accepted);
    found["elections"] = json!(
        elections(&killed_at)
            .iter()
            .filter(|e| e.2 == broker)
            .count()
    );
    report.set("cp_down_failover", found);
    report.memory("cp_down_failover", &sample_memory(agents));

    // Not waited on for readiness: a broker may not be ready without the
    // control plane, which is part of what this measures.
    let restarted_at = now_rfc3339();
    run.by_elections = None;
    compose(&["start", broker])?;
    let restarted = Instant::now();
    run.reconnect().await;
    let (input, accepted) = publish_input(&run.felix, 3, run.phase).await?;
    let mut found = run.recovery(restarted, input, None, None).await;
    found["after_broker_restart"] = json!(true);
    found["publish_secs"] = json!(accepted);
    report.set("cp_down_after_restart", found);
    report.memory("cp_down_after_restart", &sample_memory(agents));

    eprintln!("resuming the control plane");
    compose(&["unpause", "felix-controlplane"])?;
    let resumed_at = now_rfc3339();
    let resumed = Instant::now();
    run.reconnect().await;
    let (input, accepted) = publish_input(&run.felix, 4, run.phase).await?;
    let mut found = run.recovery(resumed, input, None, None).await;
    found["publish_secs"] = json!(accepted);
    found["elections_after"] = json!(elections(&resumed_at).len());
    found["elections_since_restart"] = json!(elections(&restarted_at).len());
    let until = Instant::now() + Duration::from_secs(60);
    loop {
        if let Ok(items) = assignments(t).await {
            found["led"] = json!(led_by(&items));
            break;
        }
        if Instant::now() >= until {
            report.note("the control plane did not answer 60 s after resuming".into());
            break;
        }
        tokio::time::sleep(POLL).await;
    }
    report.set("cp_back", found);
    report.memory("cp_back", &sample_memory(agents));
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn many_workloads() {
    let Ok(count) = std::env::var("FELIXD_SCALE_COUNT") else {
        eprintln!("skipping: FELIXD_SCALE_COUNT is not set");
        return;
    };
    let n: usize = count.parse().expect("FELIXD_SCALE_COUNT is a number");
    let Some(t) = test_felix().await else { return };
    let dir = PathBuf::from(std::env::var("FELIXD_SCALE_OUT").unwrap_or_else(|_| ".".into()));
    std::fs::create_dir_all(&dir).unwrap();
    let mut report = Report {
        dir,
        n,
        tag: format!("{n}-ttl{}", lease_ttl().as_secs()),
        body: serde_json::Map::new(),
        peak: BTreeMap::new(),
    };
    let date = time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap();
    report.set("workloads", json!(n));
    report.set("date", json!(date));
    report.set("felixd", json!(env!("CARGO_PKG_VERSION")));
    report.set(
        "felix",
        json!(std::env::var("FELIX_VERSION").unwrap_or_else(|_| "unknown".into())),
    );
    report.set("runner", runner());
    report.set("lease_ttl_secs", json!(lease_ttl().as_secs()));
    report.set("replicas", json!(replication_factor()));
    let result = measure(&t, n, &mut report).await;
    if let Err(error) = &result {
        report.note(format!("stopped: {error:#}"));
    }
    result.unwrap();
}

#[test]
fn election_lines_are_read() {
    let logs = "broker-2  | INFO standing for election: the leader is unreachable stream=w1.out shard=0 generation=3 leader=broker-1\n\
broker-3  | INFO standing for election: the leader is unreachable stream=w1.out shard=0 generation=3 leader=broker-1\n\
broker-3  | INFO something else stream=w2.out shard=0 leader=broker-1\n";
    let found = parse_elections(logs);
    assert_eq!(
        found,
        vec![("w1.out".into(), "0".into(), "broker-1".into())]
    );
}

#[test]
fn docker_figures_are_read() {
    assert_eq!(parse_size("1.5GiB"), Some(1536.0));
    assert_eq!(parse_size(" 512MiB "), Some(512.0));
    assert_eq!(parse_size("2048kB"), Some(2.0));
    assert_eq!(parse_size("n/a"), None);
    assert_eq!(
        service_of("felixd-cluster-broker-2-1").as_deref(),
        Some("broker-2")
    );
    assert_eq!(
        service_of("felixd-cluster-felix-controlplane-1").as_deref(),
        Some("controlplane")
    );
    assert_eq!(service_of("felixd-cluster-felix-fleet-run-1"), None);
}

fn ids(n: usize) -> Vec<String> {
    (0..n).map(|i| format!("w{i}")).collect()
}

/// Aborts the test binary if a test outlives `limit`. It runs on its own
/// thread, so it fires even when the runtime itself is stuck, and a hung test
/// never keeps the binary alive. Dropping the guard disarms it.
struct Watchdog {
    _disarm: std::sync::mpsc::Sender<()>,
}

fn watchdog(limit: Duration) -> Watchdog {
    let (disarm, armed) = std::sync::mpsc::channel::<()>();
    std::thread::spawn(move || {
        if let Err(std::sync::mpsc::RecvTimeoutError::Timeout) = armed.recv_timeout(limit) {
            use std::io::Write;
            // Straight to stderr: libtest's capture would swallow eprintln.
            let _ = writeln!(
                std::io::stderr(),
                "a test ran past {limit:?}; aborting the test binary"
            );
            std::process::abort();
        }
    });
    Watchdog { _disarm: disarm }
}

#[tokio::test]
async fn a_hung_read_costs_only_its_own_workload() {
    let _watchdog = watchdog(Duration::from_secs(10));
    let check = |id: String| async move {
        if id == "w3" {
            std::future::pending::<()>().await;
        }
        true
    };
    let started = Instant::now();
    let missing = scan(
        ids(8),
        1,
        Duration::from_millis(50),
        Duration::from_secs(5),
        check,
    )
    .await;
    assert_eq!(missing, ["w3"]);
    assert!(started.elapsed() < Duration::from_secs(2));
}

#[tokio::test]
async fn a_round_ends_at_its_bound_and_keeps_what_it_confirmed() {
    let _watchdog = watchdog(Duration::from_secs(10));
    // One reader and every read after the first hangs: ten probes of 200 ms
    // would take 2 s without the round bound.
    let check = |id: String| async move {
        if id != "w0" {
            std::future::pending::<()>().await;
        }
        true
    };
    let started = Instant::now();
    let missing = scan(
        ids(10),
        1,
        Duration::from_millis(200),
        Duration::from_millis(300),
        check,
    )
    .await;
    assert!(started.elapsed() < Duration::from_millis(1500));
    assert_eq!(missing, ids(10)[1..]);
}

#[tokio::test]
async fn a_negative_check_stays_missing() {
    let _watchdog = watchdog(Duration::from_secs(10));
    let check = |id: String| async move { id != "w1" };
    let missing = scan(ids(4), 2, PROBE, SCAN_ROUND, check).await;
    assert_eq!(missing, ["w1"]);
    assert!(
        scan(Vec::new(), 2, PROBE, SCAN_ROUND, check)
            .await
            .is_empty()
    );
}

#[tokio::test]
async fn a_step_never_runs_past_the_deadline() {
    let _watchdog = watchdog(Duration::from_secs(10));
    let deadline = Instant::now() + Duration::from_millis(100);
    let started = Instant::now();
    let hung = bounded(
        Duration::from_secs(60),
        deadline,
        std::future::pending::<()>(),
    )
    .await;
    assert!(hung.is_none());
    assert!(started.elapsed() < Duration::from_secs(2));
    // Past the deadline there is no time left, but ready work still answers.
    assert_eq!(bounded(PROBE, deadline, async { 7 }).await, Some(7));
}
