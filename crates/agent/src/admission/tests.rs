use std::cell::Cell;

use super::*;

/// Resolves `named`, with `verdict(record)` what reading a record finds,
/// counting reads.
async fn resolve(
    named: &mut Named,
    verdict: impl Fn(u64) -> Verdict,
) -> (Option<(u64, u64)>, usize) {
    let reads = Cell::new(0);
    let found = newest(
        |f| f(named),
        |record, _| {
            reads.set(reads.get() + 1);
            std::future::ready(Ok(verdict(record)))
        },
    )
    .await
    .unwrap();
    (found, reads.get())
}

fn verified_among(good: &[u64]) -> impl Fn(u64) -> Verdict + '_ {
    |record| match good.contains(&record) {
        true => Verdict::Verified,
        false => Verdict::Refused,
    }
}

/// The model's ForgedCount twin: a forged record naming a decision that
/// does not verify blocks nothing.
#[tokio::test]
async fn a_forged_decision_blocks_nothing() {
    assert!(admission::decision_end(u64::MAX, u64::MAX).is_none());
    assert!(admission::decision_end(10, 10).is_none());
    let mut named = Named::default();
    named.note(10, 5);
    named.note(11, 9);
    assert_eq!(
        resolve(&mut named, verified_among(&[10])).await,
        (Some((5, 10)), 2)
    );
    // Refused for good, so it is not read again.
    assert_eq!(
        resolve(&mut named, verified_among(&[10])).await,
        (Some((5, 10)), 0)
    );
}

/// The OldRecord twin, and the rollback this branch fixes: a later decision
/// wins wherever its record is, and an older one appended later changes
/// nothing.
#[tokio::test]
async fn a_later_verified_decision_wins_wherever_its_record_is() {
    let mut named = Named::default();
    named.note(10, 2);
    named.note(20, 3);
    // The check of the record at 10 verified it; the stop at 20 still counts.
    named.settle((2, 10), Verdict::Verified);
    assert_eq!(
        resolve(&mut named, verified_among(&[10, 20])).await,
        (Some((3, 20)), 1)
    );
    named.note(30, 2);
    assert!(named.unverified.is_empty());
}

/// The FoldMemo twin: a record naming a decision past the tail is read again
/// once that decision may exist, rather than kept as unverified. Kept, it
/// would let an older record run after it.
#[tokio::test]
async fn a_record_past_the_tail_is_read_again() {
    let mut named = Named::default();
    named.note(10, 2);
    named.note(20, 7);
    named.settle((2, 10), Verdict::Verified);
    let past_tail = |record| match record {
        20 => Verdict::NotYet,
        _ => Verdict::Refused,
    };
    assert_eq!(resolve(&mut named, past_tail).await, (Some((2, 10)), 1));
    // Decision 7 lands and accepted the record at 20.
    assert_eq!(
        resolve(&mut named, verified_among(&[20])).await,
        (Some((7, 20)), 1)
    );
}

/// The RetrySkipped twin: a skip record counts toward the projector's
/// progress, so a decision passed over stays passed over, while it never
/// counts as a spec to point `workloads` at.
#[tokio::test]
async fn a_skip_record_moves_progress_and_is_no_spec() {
    let spec = running("x", "m", vec!["in".to_owned()]);
    let mut recorded = Recorded::default();
    let mut ran = SpecRecord::new("x", spec.clone());
    ran.admitted = Some(1);
    recorded.note(5, &ran);
    let mut skip = SpecRecord::new("y", spec);
    skip.admitted = Some(4);
    skip.skipped = true;
    recorded.note(6, &skip);
    let all = verified_among(&[5, 6]);
    assert_eq!(resolve(&mut recorded.all, &all).await.0, Some((4, 6)));
    assert_eq!(resolve(&mut recorded.specs, &all).await.0, Some((1, 5)));
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
    named.settle((newest, newest), Verdict::Verified);
    assert_eq!(
        resolve(named, verified_among(&all)).await,
        (Some((newest, newest)), 0)
    );
    assert_eq!(resolve(&mut projector, verified_among(&all)).await.1, 1);
}
