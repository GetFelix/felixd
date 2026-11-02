use super::*;

#[tokio::test]
async fn the_backward_look_never_reads_more_than_a_page_at_once() {
    // One claim at the start, then a long run of outputs.
    let total = 5000u64;
    let mut asked = Vec::new();
    let floor = claim_floor_in(total, |from, end| {
        asked.push(end - from);
        async move {
            let page = (from..end)
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
                .collect::<Vec<_>>();
            Ok((page, from == 0))
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
        async move { Ok((page, from == 0)) }
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

fn output_at(input: u64) -> Vec<u8> {
    output::encode(
        output::OutputHeader {
            input_offset: input,
            index: 0,
        },
        1,
        b"x",
    )
}

fn trim_claiming(claimed: Option<u64>) -> Vec<u8> {
    output::encode_trim(
        1,
        &felixd_core::trim::Trim {
            snapshot: 0,
            step: 0,
            snap_floor: 0,
            merge_floor: 0,
            input_floors: vec![0],
            first_spec: Some(0),
            claimed,
        },
    )
}

/// A trim record carries the floor of the claim before it, so the walk back
/// ends there: the claim itself may be on a page Felix has deleted.
#[tokio::test]
async fn a_trim_record_stands_in_for_the_claim_before_it() {
    let total = 200u64;
    let mut records = vec![output::encode_claim_under(1, 7, 7, "n")];
    records.extend((1..40).map(output_at));
    records.push(trim_claiming(Some(7)));
    records.extend((40..total).map(output_at));
    let mut reached = u64::MAX;
    let floor = claim_floor_in(total, |from, end| {
        reached = reached.min(from);
        let page = records[from as usize..end as usize].to_vec();
        async move { Ok((page, from == 0)) }
    })
    .await
    .unwrap();
    assert_eq!(floor, Some(7));
    // It read no further back than the trim at offset 40.
    assert!(reached >= 40 - LOOK_RECORDS, "{reached}");
}

/// The head is gone, claim included: the walk ends at the oldest record
/// Felix keeps, and the trim there holds the floor.
#[tokio::test]
async fn a_floor_is_found_when_the_head_was_deleted() {
    let first_held = 100u64;
    let mut held = vec![trim_claiming(Some(7))];
    held.extend((101..130).map(output_at));
    let end = first_held + held.len() as u64;
    let floor = claim_floor_in(end, |from, end| {
        // Felix answers a read below its oldest offset with what it keeps.
        let from = from.max(first_held);
        let page = held[(from - first_held) as usize..(end - first_held) as usize].to_vec();
        async move { Ok((page, from == first_held)) }
    })
    .await
    .unwrap();
    assert_eq!(floor, Some(7));
}

/// With no claim and no trim record that holds a floor in what is kept, there
/// is none to find, and the walk still ends.
#[tokio::test]
async fn a_walk_over_a_cut_stream_with_no_floor_ends() {
    let first_held = 100u64;
    let held: Vec<Vec<u8>> = (100..130).map(output_at).collect();
    let end = first_held + held.len() as u64;
    let floor = claim_floor_in(end, |from, end| {
        let from = from.max(first_held);
        let page = held[(from - first_held) as usize..(end - first_held) as usize].to_vec();
        async move { Ok((page, from == first_held)) }
    })
    .await
    .unwrap();
    assert_eq!(floor, None);
}

#[test]
fn a_claim_and_a_trim_record_both_name_the_spec_the_stream_was_written_under() {
    assert_eq!(
        spec_named(&output::encode_claim_under(1, 4, 4, "n")).unwrap(),
        Some(4)
    );
    assert_eq!(spec_named(&output::encode_claim(1, "n")).unwrap(), None);
    assert_eq!(spec_named(&trim_claiming(None)).unwrap(), Some(0));
    assert_eq!(spec_named(&output_at(3)).unwrap(), None);
}

/// A stream as Felix serves it once the records below `oldest` are deleted:
/// a read that starts below it is refused, naming `oldest`.
fn served(
    stream: &'static str,
    oldest: u64,
    records: Vec<Vec<u8>>,
) -> impl FnMut(u64, u64) -> std::future::Ready<Result<Page>> {
    move |from, end| {
        std::future::ready(if from < oldest {
            Err(Trimmed {
                stream: stream.to_owned(),
                requested: from,
                oldest,
            }
            .into())
        } else {
            let page: Vec<_> = (from..end)
                .map(|offset| (offset, Bytes::from(records[offset as usize].clone())))
                .collect();
            Ok((page, end))
        })
    }
}

fn padded(before: u64, records: Vec<Vec<u8>>) -> Vec<Vec<u8>> {
    let mut all: Vec<Vec<u8>> = (0..before).map(|_| Vec::new()).collect();
    all.extend(records);
    all
}

/// Reads the spec record a stream was written under when Felix holds
/// records from offset `oldest` on.
async fn written(oldest: u64, held: Vec<Vec<u8>>) -> Option<u64> {
    let all = padded(oldest, held);
    let tail = all.len() as u64;
    written_record_in("wl.x.out", tail, served("wl.x.out", oldest, all))
        .await
        .unwrap()
}

#[tokio::test]
async fn the_first_claim_says_what_the_stream_was_written_under() {
    let held = vec![
        output::encode_needs_operator(1, 9, felixd_core::class::OperatorReason::ModuleMissing, "x"),
        output::encode_claim_under(1, 4, 4, "n"),
        output::encode_claim_under(2, 6, 6, "n"),
    ];
    assert_eq!(written(0, held).await, Some(4));
}

/// The head is gone and the oldest record kept is a trim record, which
/// carries the first claim's spec.
#[tokio::test]
async fn a_trim_record_at_the_head_carries_the_spec_the_claim_named() {
    let held = vec![trim_claiming(Some(7)), output_at(40), output_at(41)];
    assert_eq!(written(100, held).await, Some(0));
}

/// The head is gone and the oldest records kept are outputs, as when the cut
/// is at a snapshot's output tail: the next record that names a spec stands
/// in, since every claim's spec has the first claim's module and input.
#[tokio::test]
async fn outputs_at_the_head_are_looked_past_to_a_trim_or_a_claim() {
    let held = vec![output_at(40), output_at(41), trim_claiming(Some(7))];
    assert_eq!(written(100, held).await, Some(0));
    let held = vec![output_at(40), output_at(41), output_claim(9)];
    assert_eq!(written(100, held).await, Some(9));
}

fn output_claim(spec: u64) -> Vec<u8> {
    output::encode_claim_under(3, spec, spec, "n")
}

/// Outputs first with no claim is a stream that predates claims, not one with
/// its head cut, and a later claim does not say what it was written under.
#[tokio::test]
async fn outputs_at_the_start_of_a_stream_with_a_whole_head_name_nothing() {
    let held = vec![output_at(0), output_at(1), output_claim(9)];
    assert_eq!(written(0, held).await, None);
}

#[tokio::test]
async fn a_trim_record_that_carries_no_spec_names_nothing() {
    let mut trim = felixd_core::trim::Trim {
        snapshot: 0,
        step: 0,
        snap_floor: 0,
        merge_floor: 0,
        input_floors: vec![0],
        first_spec: None,
        claimed: None,
    };
    let held = vec![output::encode_trim(1, &trim), output_at(2)];
    assert_eq!(written(100, held).await, None);
    trim.first_spec = Some(5);
    let held = vec![output::encode_trim(1, &trim)];
    assert_eq!(written(100, held).await, Some(5));
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
