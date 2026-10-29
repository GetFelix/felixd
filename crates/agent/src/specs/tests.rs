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
