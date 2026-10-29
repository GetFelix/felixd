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
