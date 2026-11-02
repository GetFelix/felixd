use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_misses_build_once() {
    let once = Arc::new(Once::<u32>::default());
    let builds = Arc::new(AtomicUsize::new(0));
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let (once, builds) = (once.clone(), builds.clone());
        tasks.push(tokio::spawn(async move {
            once.get("m", || async {
                builds.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(100)).await;
                Ok(7)
            })
            .await
            .unwrap()
        }));
    }
    for task in tasks {
        assert_eq!(task.await.unwrap(), 7);
    }
    assert_eq!(builds.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_failed_build_is_tried_again() {
    let once = Once::<u32>::default();
    assert!(
        once.get("m", || async { anyhow::bail!("no") })
            .await
            .is_err()
    );
    assert_eq!(once.get("m", || async { Ok(3) }).await.unwrap(), 3);
}

/// On a single runtime thread a blocking call made inline would stop the
/// ticker, as a long step stops lease renewals.
#[tokio::test(flavor = "current_thread")]
async fn a_blocking_call_leaves_the_runtime_free() {
    let ticks = Arc::new(AtomicUsize::new(0));
    let ticker = tokio::spawn({
        let ticks = ticks.clone();
        async move {
            loop {
                tokio::time::sleep(Duration::from_millis(10)).await;
                ticks.fetch_add(1, Ordering::SeqCst);
            }
        }
    });
    let permits = Semaphore::new(1);
    let mut slot = Some(0u32);
    let out = blocking(&permits, &mut slot, |n| {
        std::thread::sleep(Duration::from_millis(300));
        *n += 1;
        *n
    })
    .await
    .unwrap();
    ticker.abort();
    assert_eq!((out, slot), (1, Some(1)));
    assert!(ticks.load(Ordering::SeqCst) >= 5, "the runtime was blocked");
}

#[test]
fn an_input_is_behind_only_when_its_tail_is_below_the_position() {
    assert!(not_behind("in", 4, 4).is_ok());
    assert!(not_behind("in", 5, 4).is_ok());
    let error = not_behind("in", 3, 4).unwrap_err();
    let problem = error.downcast_ref::<NeedsOperator>().unwrap();
    assert_eq!(problem.reason, OperatorReason::InputBehind);
}

/// Raising `keep_snapshots` on a merged workload leaves a kept snapshot below
/// the last trim, whose merge entry Felix has dropped. That is a trim to keep,
/// not an error that reaches the workload as lost history. Felix trimming
/// further than our own trim did is still an error.
#[test]
fn a_merge_entry_below_the_last_trim_is_not_an_error() {
    let trimmed = |oldest| Trimmed {
        stream: "wl.a.merge".into(),
        requested: 3,
        oldest,
    };
    let last = Trim {
        snapshot: 20,
        step: 9,
        snap_floor: 15,
        merge_floor: 9,
        input_floors: vec![4, 5],
    };
    assert_eq!(
        below_the_trim(Err(trimmed(9).into()), Some(&last)).unwrap(),
        None
    );
    assert!(below_the_trim(Err(trimmed(10).into()), Some(&last)).is_err());
    assert!(below_the_trim(Err(trimmed(9).into()), None).is_err());
    assert_eq!(
        below_the_trim(Ok(vec![4, 5]), Some(&last)).unwrap(),
        Some(vec![4, 5])
    );
    let reset = anyhow::anyhow!("connection reset");
    assert!(below_the_trim(Err(reset), Some(&last)).is_err());
}
