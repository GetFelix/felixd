use super::*;
use felixd_core::class::ErrorClass;
use felixd_core::output;

const SPEC: u64 = 3;

/// A stream whose claims are far enough apart that the run spans several
/// reads, so the count carries over from one read to the next.
#[tokio::test]
async fn a_run_of_takeovers_is_counted_across_reads() {
    let mut stream = Vec::new();
    for epoch in 1..=3 {
        stream.push(output::encode_claim_under(epoch, SPEC, SPEC, "n"));
        for _ in 0..LOOK_RECORDS {
            stream.push(output::encode_degraded(
                epoch,
                SPEC,
                ErrorClass::Transient,
                "n",
                "x",
            ));
        }
    }
    let stream: Vec<(Bytes, Option<u64>)> = stream
        .into_iter()
        .enumerate()
        .map(|(i, r)| (Bytes::from(r), Some(1_000 + i as u64)))
        .collect();
    let read = |from: u64, end: u64| {
        let records = stream[from as usize..end as usize].to_vec();
        async move { Ok((records, from == 0)) }
    };
    let limit = |max_takeovers| RestartIntensity {
        max_takeovers,
        window_ms: 60_000,
    };
    let end = stream.len() as u64;
    assert_eq!(
        count_in(end, SPEC, limit(1), read).await.unwrap(),
        Verdict::Over { takeovers: 2 }
    );
    assert_eq!(
        count_in(end, SPEC, limit(2), read).await.unwrap(),
        Verdict::Within
    );
}
