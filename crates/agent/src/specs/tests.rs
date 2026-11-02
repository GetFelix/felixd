use super::*;

#[tokio::test]
async fn the_backward_look_never_reads_more_than_a_page_at_once() {
    // One claim at the start, then a long run of outputs.
    let total = 5000u64;
    let mut asked = Vec::new();
    let floor = claim_floor_in(total, |from, end| {
        asked.push(end - from);
        async move {
            Ok((from..end)
                .map(|offset| {
                    if offset == 0 {
                        output::encode_claim_under(1, 7, 7, "n")
                    } else {
                        output::encode(
                            output::OutputHeader {
                                input_offset: offset,
                                index: 0,
                            },
                            1,
                            b"x",
                        )
                    }
                })
                .collect::<Vec<_>>())
        }
    })
    .await
    .unwrap();
    assert_eq!(floor, Some(7));
    assert!(asked.iter().all(|&n| n <= LOOK_RECORDS), "{asked:?}");
}

async fn floor_of(records: Vec<Vec<u8>>) -> Option<u64> {
    let end = records.len() as u64;
    claim_floor_in(end, |from, end| {
        let page = records[from as usize..end as usize].to_vec();
        async move { Ok(page) }
    })
    .await
    .unwrap()
}

/// r0 ran, then a node recorded that r2 needs an operator without claiming.
/// A node still on r0 must see r2 as claimed and refuse, or it would run r0
/// over the record.
#[tokio::test]
async fn a_needs_operator_record_counts_toward_the_floor() {
    use felixd_core::class::OperatorReason;
    let marker = |spec| output::encode_needs_operator(1, spec, OperatorReason::ModuleMissing, "x");
    let out = output::encode(
        output::OutputHeader {
            input_offset: 0,
            index: 0,
        },
        1,
        b"x",
    );
    let claim = output::encode_claim_under(1, 0, 0, "a");
    assert_eq!(
        floor_of(vec![claim.clone(), out.clone(), marker(2)]).await,
        Some(2)
    );
    assert_eq!(floor_of(vec![marker(1)]).await, Some(1));
    // A marker below the last claim's floor does not lower it.
    let high = output::encode_claim_under(1, 5, 5, "a");
    assert_eq!(floor_of(vec![high, out, marker(3)]).await, Some(5));
    // A marker before the last claim is that claim's floor's business.
    assert_eq!(floor_of(vec![marker(4), claim]).await, Some(0));
}

/// A degraded record is an observation: a node still on an older spec runs.
#[tokio::test]
async fn a_degraded_record_does_not_count_toward_the_floor() {
    use felixd_core::class::ErrorClass;
    let degraded = output::encode_degraded(1, 9, ErrorClass::Transient, "node-a", "x");
    let claim = output::encode_claim_under(1, 2, 2, "a");
    assert_eq!(
        floor_of(vec![claim.clone(), degraded.clone()]).await,
        Some(2)
    );
    assert_eq!(floor_of(vec![degraded]).await, None);
}

/// As a newer build writes it: a reason this build has no name for.
fn unreadable_marker(spec: u64) -> Vec<u8> {
    use felixd_core::class::OperatorReason;
    let mut record = output::encode_needs_operator(1, spec, OperatorReason::ModuleMissing, "x");
    record[18..20].copy_from_slice(&u16::MAX.to_be_bytes());
    assert!(output::decode(&record).is_err());
    record
}

#[test]
fn a_needs_operator_record_with_an_unknown_reason_is_not_the_first_run() {
    use felixd_core::class::OperatorReason;
    assert!(!ran_something(&unreadable_marker(3)));
    assert!(!ran_something(&output::encode_needs_operator(
        1,
        3,
        OperatorReason::ModuleMissing,
        "x"
    )));
    assert!(ran_something(&output::encode_claim_under(1, 0, 0, "a")));
    // A record that is nothing this build knows is still one that may have run.
    assert!(ran_something(&[0xff, 0xff]));
}

#[tokio::test]
async fn a_needs_operator_record_with_an_unknown_reason_counts_toward_the_floor() {
    let claim = output::encode_claim_under(1, 0, 0, "a");
    assert_eq!(floor_of(vec![claim, unreadable_marker(4)]).await, Some(4));
}

/// As a newer build writes it: a class this build has no name for.
fn unreadable_degraded(spec: u64) -> Vec<u8> {
    use felixd_core::class::ErrorClass;
    let mut record = output::encode_degraded(1, spec, ErrorClass::Transient, "node-a", "x");
    record[18..20].copy_from_slice(&u16::MAX.to_be_bytes());
    assert!(output::decode(&record).is_err());
    record
}

#[test]
fn a_degraded_record_is_not_the_first_run_whatever_its_class() {
    use felixd_core::class::ErrorClass;
    let readable = output::encode_degraded(1, 3, ErrorClass::Transient, "node-a", "x");
    assert!(!ran_something(&readable));
    assert!(!ran_something(&unreadable_degraded(3)));
}

#[test]
fn a_degraded_record_this_build_cannot_read_is_an_unreadable_park() {
    use felixd_core::class::ErrorClass;
    assert!(unreadable_park(&unreadable_degraded(3)));
    let readable = output::encode_degraded(1, 3, ErrorClass::Transient, "node-a", "x");
    assert!(!unreadable_park(&readable));
}

#[test]
fn only_a_needs_operator_record_this_build_cannot_read_is_an_unreadable_park() {
    use felixd_core::class::OperatorReason;
    assert!(unreadable_park(&unreadable_marker(3)));
    assert!(!unreadable_park(&output::encode_needs_operator(
        1,
        3,
        OperatorReason::ModuleMissing,
        "x"
    )));
    assert!(!unreadable_park(&output::encode_claim_under(1, 0, 0, "a")));
    assert!(!unreadable_park(&[0xff, 0xff]));
}

fn divergence(spec: u64) -> Vec<u8> {
    use felixd_core::output::{Abi, Diverged, OutputHeader, encode_divergence};
    encode_divergence(
        1,
        &Diverged {
            spec,
            header: OutputHeader {
                input_offset: 0,
                index: 0,
            },
            snapshot: None,
            node: b"node-a",
            abi: Abi {
                contract: b"0.2.0",
                wasmtime: b"50.0.0",
                felixd: b"1.2.3",
            },
            expected: None,
            actual: None,
            detail: b"",
        },
    )
}

/// As a newer build writes it: a field this build has no reading for.
fn unreadable_divergence(spec: u64) -> Vec<u8> {
    let mut record = divergence(spec);
    record[30] = 2;
    assert!(output::decode(&record).is_err());
    record
}

/// A divergence record is an observation, like a degraded one.
#[tokio::test]
async fn a_divergence_record_does_not_count_toward_the_floor() {
    let claim = output::encode_claim_under(1, 2, 2, "a");
    assert_eq!(floor_of(vec![claim.clone(), divergence(9)]).await, Some(2));
    assert_eq!(floor_of(vec![divergence(9)]).await, None);
    assert_eq!(
        floor_of(vec![claim, unreadable_divergence(9)]).await,
        Some(2)
    );
}

#[test]
fn a_divergence_record_is_not_the_first_run_whatever_its_fields() {
    assert!(!ran_something(&divergence(3)));
    assert!(!ran_something(&unreadable_divergence(3)));
    assert!(ran_something(&output::encode_claim_under(1, 3, 3, "a")));
}

#[test]
fn a_divergence_record_this_build_cannot_read_is_an_unreadable_park() {
    assert!(unreadable_park(&unreadable_divergence(3)));
    assert!(!unreadable_park(&divergence(3)));
}
