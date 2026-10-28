//! Faults the integration tests inject through `FELIXD_FAULTS`, a comma
//! separated list such as `crash-before-snapshot=3,drop-input=17`:
//!
//! - `crash-before-snapshot=N`: abort the process just before its Nth snapshot.
//! - `crash-after-snapshot=N`: abort it just after its Nth snapshot is written.
//! - `drop-input=K`: ignore the subscription's delivery of input offset `K`, as
//!   if Felix had dropped it.
//!
//! Only a build with the `faults` feature reads the variable. The felixd
//! crate turns it on for its own tests through a dev-dependency, so a binary
//! built with `cargo build` or `cargo install` ignores `FELIXD_FAULTS`.

#[cfg(feature = "faults")]
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

static SNAPSHOTS: AtomicU64 = AtomicU64::new(0);

#[cfg(not(feature = "faults"))]
fn fault(_name: &str) -> Option<u64> {
    None
}

#[cfg(feature = "faults")]
fn fault(name: &str) -> Option<u64> {
    static FAULTS: OnceLock<Vec<(String, u64)>> = OnceLock::new();
    FAULTS
        .get_or_init(|| {
            std::env::var("FELIXD_FAULTS")
                .unwrap_or_default()
                .split(',')
                .filter_map(|f| {
                    let (name, value) = f.trim().split_once('=')?;
                    Some((name.to_owned(), value.parse().ok()?))
                })
                .collect()
        })
        .iter()
        .find(|(n, _)| n == name)
        .map(|(_, value)| *value)
}

fn crash(name: &str) -> ! {
    tracing::error!(fault = name, "injected crash");
    std::process::abort()
}

pub(crate) fn before_snapshot() {
    let n = SNAPSHOTS.fetch_add(1, Ordering::Relaxed) + 1;
    if fault("crash-before-snapshot") == Some(n) {
        crash("crash-before-snapshot");
    }
}

pub(crate) fn after_snapshot() {
    if fault("crash-after-snapshot") == Some(SNAPSHOTS.load(Ordering::Relaxed)) {
        crash("crash-after-snapshot");
    }
}

pub(crate) fn drop_input(offset: u64) -> bool {
    fault("drop-input") == Some(offset)
}
