use std::cell::Cell;

use super::*;

/// Resolves `named` with `good` the records that verify, counting reads.
async fn resolve(named: &mut Named, good: &[u64]) -> (Option<(u64, u64)>, usize) {
    let reads = Cell::new(0);
    let found = newest(
        |f| f(named),
        |record, _| {
            reads.set(reads.get() + 1);
            std::future::ready(Ok(good.contains(&record)))
        },
    )
    .await
    .unwrap();
    (found, reads.get())
}

#[tokio::test]
async fn a_forged_decision_blocks_nothing() {
    assert!(admission::decision_end(u64::MAX, u64::MAX).is_none());
    assert!(admission::decision_end(10, 10).is_none());
    let mut named = Named::default();
    named.note(10, 5);
    // Forged: a huge decision that does not verify.
    named.note(11, u64::MAX);
    assert_eq!(resolve(&mut named, &[10]).await, (Some((5, 10)), 2));
    // Settled, so it is not read again.
    assert_eq!(resolve(&mut named, &[10]).await, (Some((5, 10)), 0));
}

#[tokio::test]
async fn a_later_verified_decision_wins_wherever_its_record_is() {
    let mut named = Named::default();
    named.note(10, 2);
    named.note(20, 3);
    // The check of the record at 10 verified it; the stop at 20 still counts.
    named.settle((2, 10), true);
    assert_eq!(resolve(&mut named, &[10, 20]).await, (Some((3, 20)), 1));
    // An older decision appended later changes nothing.
    named.note(30, 2);
    assert!(named.unverified.is_empty());
}

/// The first check on a node, and a projector starting, read one decision
/// however many records `sys.specs` holds. Before, each read every record's
/// decision: here 10,000 reads.
#[tokio::test]
async fn the_first_check_reads_one_decision_of_many_records() {
    let workloads = 100;
    let changes = 100;
    let mut gate: HashMap<u64, Named> = HashMap::new();
    let mut projector = Named::default();
    let mut record = 0;
    for change in 0..changes {
        for workload in 0..workloads {
            let decision = change * workloads + workload;
            gate.entry(workload).or_default().note(record, decision);
            projector.note(record, decision);
            record += 1;
        }
    }
    let all: Vec<u64> = (0..record).collect();
    // The gate checking workload 7's newest record, which it has just read.
    let named = gate.get_mut(&7).unwrap();
    let newest = (changes - 1) * workloads + 7;
    named.settle((newest, newest), true);
    assert_eq!(resolve(named, &all).await, (Some((newest, newest)), 0));
    assert_eq!(resolve(&mut projector, &all).await.1, 1);
}
