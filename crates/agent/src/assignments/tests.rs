use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use felixd_core::output::{Addressed, OutputHeader, encode_addressed, encode_claim};

use super::*;

fn out(offset: u64, record: &Record) -> Vec<u8> {
    let payload = record.encode();
    let header = OutputHeader {
        input_offset: offset,
        index: 0,
    };
    let addressed = Addressed {
        name: "out",
        key: record.key().map(str::as_bytes),
        payload: &payload,
    };
    encode_addressed(header, 1, addressed).unwrap()
}

fn assign(specs: u64, workload: &str, node: Option<&str>) -> Record {
    Record {
        specs,
        change: Change::Assign {
            workload: workload.into(),
            node: node.map(str::to_owned),
        },
    }
}

fn table(specs: u64, pairs: &[(&str, &str)]) -> Record {
    let assignments: BTreeMap<String, String> = pairs
        .iter()
        .map(|(w, n)| (w.to_string(), n.to_string()))
        .collect();
    Record {
        specs,
        change: Change::Table { assignments },
    }
}

fn placed(node: &str, offset: u64) -> Placed {
    Placed {
        node: node.into(),
        offset,
    }
}

#[test]
fn the_latest_change_per_workload_is_in_force_and_a_table_replaces_all() {
    let mut f = Followed::default();
    let records = [
        encode_claim(1, "n-1"),
        out(0, &assign(1, "a", Some("n-1"))),
        out(1, &assign(2, "b", Some("n-2"))),
        b"not a record".to_vec(),
        out(2, &assign(3, "a", Some("n-3"))),
        out(3, &assign(3, "b", None)),
    ];
    for (offset, record) in (10..).zip(&records) {
        f.fold(offset, record);
    }
    assert_eq!(
        f.table,
        HashMap::from([("a".to_owned(), placed("n-3", 14))])
    );
    assert_eq!(f.specs, 3);
    f.fold(20, &out(4, &table(5, &[("c", "n-1")])));
    assert_eq!(
        f.table,
        HashMap::from([("c".to_owned(), placed("n-1", 20))])
    );
    f.fold(
        21,
        &out(
            5,
            &Record {
                specs: 6,
                change: Change::Progress,
            },
        ),
    );
    assert_eq!(f.specs, 6);
}

/// A stream in memory that serves seven records a page and fails every
/// third call, like a broker that drops answers.
struct Memory {
    records: Vec<StreamRecord>,
    calls: AtomicUsize,
    pages: AtomicUsize,
    lowest_read: AtomicUsize,
}

impl Memory {
    fn new(payloads: Vec<Vec<u8>>) -> Memory {
        let records = (0..)
            .zip(payloads)
            .map(|(offset, payload)| StreamRecord {
                offset,
                payload: Bytes::from(payload),
                publisher: None,
                timestamp_micros: 0,
            })
            .collect();
        Memory {
            records,
            calls: AtomicUsize::new(0),
            pages: AtomicUsize::new(0),
            lowest_read: AtomicUsize::new(usize::MAX),
        }
    }

    fn flaky(&self) -> Result<()> {
        if self.calls.fetch_add(1, Ordering::SeqCst) % 3 == 2 {
            anyhow::bail!("dropped");
        }
        Ok(())
    }

    fn range(&self, from: u64, end: u64) -> Vec<StreamRecord> {
        self.lowest_read.fetch_min(from as usize, Ordering::SeqCst);
        self.records[from as usize..end as usize].to_vec()
    }
}

impl Source for Memory {
    async fn tail(&self) -> Result<u64> {
        self.flaky()?;
        Ok(self.records.len() as u64)
    }

    async fn back(&self, from: u64, end: u64) -> Result<(Vec<StreamRecord>, bool)> {
        self.flaky()?;
        Ok((self.range(from, end), from == 0))
    }

    async fn page(&self, from: u64, end: u64) -> Result<(Vec<StreamRecord>, u64)> {
        self.flaky()?;
        self.pages.fetch_add(1, Ordering::SeqCst);
        let to = (from + 7).min(end);
        Ok((self.range(from, to), to))
    }
}

#[tokio::test]
async fn a_reader_starts_at_the_latest_table_and_never_reads_a_page_twice() {
    // 2000 assignments, a table at 1500, then 600 more.
    let mut payloads: Vec<Vec<u8>> = (0..1500)
        .map(|i| out(i, &assign(i, &format!("w{}", i % 50), Some("old"))))
        .collect();
    payloads.push(out(1500, &table(1500, &[("w1", "n-1"), ("w2", "n-2")])));
    for i in 1501..2100 {
        payloads.push(out(i, &assign(i, &format!("x{i}"), Some("n-3"))));
    }
    let memory = Memory::new(payloads);
    let mut f = Followed::default();
    let mut tries = 0;
    loop {
        tries += 1;
        assert!(tries < 1000, "never caught up");
        if read_new(&memory, &mut f, |_| {}).await.is_ok() {
            break;
        }
    }
    let lowest = memory.lowest_read.load(Ordering::SeqCst) as u64;
    assert!(
        (1500 - BACK_WINDOW..=1500).contains(&lowest),
        "read back to {lowest}"
    );
    // Each page is read once however many reads failed: (2100 - 1500) / 7.
    assert_eq!(memory.pages.load(Ordering::SeqCst), 600 / 7 + 1);
    assert_eq!(f.table.len(), 2 + 599);
    assert_eq!(f.table["w1"], placed("n-1", 1500));
    assert_eq!(f.table["x2099"], placed("n-3", 2099));
    assert_eq!(f.specs, 2099);
}

#[tokio::test]
async fn without_a_table_a_reader_starts_at_the_beginning() {
    let payloads = (0..10)
        .map(|i| out(i, &assign(i, "w", Some("n"))))
        .collect();
    let memory = Memory::new(payloads);
    let mut f = Followed::default();
    let mut tries = 0;
    while read_new(&memory, &mut f, |_| {}).await.is_err() {
        tries += 1;
        assert!(tries < 100, "never caught up");
    }
    assert_eq!(f.table["w"], placed("n", 9));
}

#[test]
fn a_fact_is_written_when_it_changes_or_the_interval_passes() {
    let fact = NodeFact {
        node: "n".into(),
        every_ms: 1000,
        max_workloads: 4,
        held: 0,
        holding: Default::default(),
        draining: false,
        blocked: false,
    };
    let every = Duration::from_secs(60);
    assert!(due(None, &fact, every));
    let last = (fact.clone(), Instant::now());
    assert!(!due(Some(&last), &fact, every));
    let held = NodeFact {
        held: 1,
        ..fact.clone()
    };
    assert!(due(Some(&last), &held, every));
    assert!(due(Some(&last), &fact, Duration::ZERO));
}

#[test]
fn only_a_running_scheduler_whose_holder_renews_is_healthy() {
    let timing = LeaseTiming {
        ttl: Duration::from_millis(3000),
    };
    let a = Assignments::new();
    assert!(!a.healthy(&timing, 10_000));
    a.set_holder(Some(13_000));
    assert!(!a.healthy(&timing, 10_000), "the spec does not say it runs");
    a.set_scheduler(true);
    assert!(a.healthy(&timing, 10_000));
    // Not expired, but a renewal is overdue: the holder may be dead.
    assert!(a.healthy(&timing, 11_600));
    assert!(!a.healthy(&timing, 12_100));
    assert!(!a.healthy(&timing, 14_000));
    a.set_holder(None);
    assert!(!a.healthy(&timing, 10_000));
    a.set_holder(Some(0));
    assert!(
        !a.healthy(&timing, 10_000),
        "a lease that never expires is not renewed"
    );
}

#[tokio::test]
async fn a_change_wakes_whoever_waits_on_it() {
    let a = Assignments::new();
    let mut changes = a.changes();
    a.set_scheduler(true);
    changes.changed().await.unwrap();
    a.set_scheduler(true);
    assert!(!changes.has_changed().unwrap());
    a.set_holder(Some(5));
    assert!(changes.has_changed().unwrap());
    assert_eq!(a.get("w"), (None, 0));
}
