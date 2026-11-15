use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use anyhow::anyhow;

use super::*;

#[tokio::test(start_paused = true)]
async fn a_failed_append_is_retried_until_it_lands() {
    let tries = AtomicUsize::new(0);
    let ended = append_until_landed(
        || async { true },
        || async { Ok(true) },
        || async {
            match tries.fetch_add(1, Ordering::SeqCst) {
                0 | 1 => Err(anyhow!("broker away")),
                _ => Ok(()),
            }
        },
    )
    .await;
    assert_eq!(ended, Ended::Landed);
    assert_eq!(tries.load(Ordering::SeqCst), 3);
}

#[tokio::test(start_paused = true)]
async fn retries_stop_once_the_lease_is_lost() {
    let tries = AtomicUsize::new(0);
    let held = AtomicBool::new(true);
    let appending = append_until_landed(
        || async { held.load(Ordering::SeqCst) },
        || async { Ok(true) },
        || async {
            if tries.fetch_add(1, Ordering::SeqCst) == 1 {
                held.store(false, Ordering::SeqCst);
            }
            Err(anyhow!("broker away"))
        },
    );
    let ended = tokio::time::timeout(Duration::from_secs(60), appending)
        .await
        .expect("still retrying after the lease was lost");
    assert_eq!(ended, Ended::Lost);
    assert_eq!(tries.load(Ordering::SeqCst), 2);
}

#[tokio::test(start_paused = true)]
async fn a_workload_with_no_placed_stream_appends_nothing() {
    let tries = AtomicUsize::new(0);
    let ended = append_until_landed(
        || async { true },
        || async { Ok(false) },
        || async {
            tries.fetch_add(1, Ordering::SeqCst);
            Ok(())
        },
    )
    .await;
    assert_eq!(ended, Ended::NoStream);
    assert_eq!(tries.load(Ordering::SeqCst), 0);
}

#[tokio::test(start_paused = true)]
async fn a_failed_look_for_the_stream_is_retried() {
    let looks = AtomicUsize::new(0);
    let ended = append_until_landed(
        || async { true },
        || async {
            match looks.fetch_add(1, Ordering::SeqCst) {
                0 => Err(anyhow!("broker away")),
                _ => Ok(true),
            }
        },
        || async { Ok(()) },
    )
    .await;
    assert_eq!(ended, Ended::Landed);
    assert_eq!(looks.load(Ordering::SeqCst), 2);
}

#[tokio::test(start_paused = true)]
async fn the_record_waits_for_the_claim() {
    let progress = Progress::default();
    let waiting = progress.claim_on_stream();
    tokio::pin!(waiting);
    assert!(
        tokio::time::timeout(Duration::from_secs(5), &mut waiting)
            .await
            .is_err()
    );
    progress.claim_landed();
    tokio::time::timeout(Duration::from_secs(1), waiting)
        .await
        .unwrap();
    // A later waiter sees it at once.
    progress.claim_on_stream().await;
}
