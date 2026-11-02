use felix_client::RetryClass;

use super::*;

fn broker(code: ErrorCode) -> anyhow::Error {
    BrokerError::new(code, RetryClass::Fatal, "refused").into()
}

#[tokio::test]
async fn a_trim_at_the_right_tail_is_one_call() {
    let mut asked = Vec::new();
    let settled = settle(7, 0, |expected| {
        asked.push(expected);
        async {
            Ok(ConditionalTrim::Trimmed {
                offset: Some(7),
                oldest: 3,
            })
        }
    })
    .await
    .unwrap();
    assert_eq!((settled, asked), (Settled::Trimmed(Some(7)), vec![7]));
}

/// The caller's tail may be stale, as after a leader change, so with no
/// retries a refusal is not chased: the next snapshot asks again from a fresh
/// tail.
#[tokio::test]
async fn a_refused_tail_is_never_asked_again_at_the_tail_it_names() {
    let mut asked = Vec::new();
    let settled = settle(7, 0, |expected| {
        asked.push(expected);
        async { Ok(ConditionalTrim::Refused { tail: 9 }) }
    })
    .await
    .unwrap();
    assert_eq!((settled, asked), (Settled::TailMoved, vec![7]));
}

/// A stream with many writers has moved on by the time the trim is asked.
/// Felix says where its tail is and the next call goes there.
#[tokio::test]
async fn a_busy_stream_is_asked_again_at_the_tail_felix_names() {
    let mut asked = Vec::new();
    let settled = settle(7, 3, |expected| {
        asked.push(expected);
        let moved = asked.len() < 3;
        async move {
            Ok(match moved {
                true => ConditionalTrim::Refused { tail: expected + 4 },
                false => ConditionalTrim::Trimmed {
                    offset: Some(expected),
                    oldest: 3,
                },
            })
        }
    })
    .await
    .unwrap();
    assert_eq!(
        (settled, asked),
        (Settled::Trimmed(Some(15)), vec![7, 11, 15])
    );
}

/// The retries are bounded: a stream that never holds still ends in
/// `TailMoved` after the first call and the retries, and the next snapshot
/// takes it up.
#[tokio::test]
async fn the_retries_are_bounded() {
    let mut asked = Vec::new();
    let settled = settle(7, 3, |expected| {
        asked.push(expected);
        async move { Ok(ConditionalTrim::Refused { tail: expected + 1 }) }
    })
    .await
    .unwrap();
    assert_eq!((settled, asked), (Settled::TailMoved, vec![7, 8, 9, 10]));
}

#[tokio::test]
async fn an_error_while_retrying_is_returned() {
    let mut calls = 0;
    let result = settle(7, 3, |expected| {
        calls += 1;
        let first = calls == 1;
        async move {
            match first {
                true => Ok(ConditionalTrim::Refused { tail: expected + 1 }),
                false => Err(broker(ErrorCode::Forbidden)),
            }
        }
    })
    .await;
    assert!(result.is_err());
    assert_eq!(calls, 2);
}

#[tokio::test]
async fn a_floor_already_in_place_writes_nothing() {
    let settled = settle(7, 0, |_| async {
        Ok(ConditionalTrim::Trimmed {
            offset: None,
            oldest: 5,
        })
    })
    .await
    .unwrap();
    assert_eq!(settled, Settled::Trimmed(None));
}

#[tokio::test]
async fn an_error_is_returned_without_asking_again() {
    let mut calls = 0;
    let result = settle(7, 0, |_| {
        calls += 1;
        async { Err::<ConditionalTrim, _>(broker(ErrorCode::FeatureNotEnabled)) }
    })
    .await;
    assert!(result.is_err());
    assert_eq!(calls, 1);
}

#[test]
fn a_fleet_without_head_trim_is_told_apart_from_other_refusals() {
    assert_eq!(
        classify(&TrimError::Unsupported.into()),
        Failure::NotEnabled
    );
    assert_eq!(
        classify(&broker(ErrorCode::FeatureNotEnabled)),
        Failure::NotEnabled
    );
    assert_eq!(classify(&broker(ErrorCode::Forbidden)), Failure::NoGrant);
    assert_eq!(
        classify(&broker(ErrorCode::Unacknowledged)),
        Failure::TimedOut
    );
    assert_eq!(classify(&broker(ErrorCode::InvalidRequest)), Failure::Other);
    assert_eq!(
        classify(&anyhow::anyhow!("connection reset")),
        Failure::Other
    );
}

#[test]
fn a_pause_holds_off_deletion_until_it_ends() {
    // Run apart from other tests that pause: they share the process.
    reset();
    assert!(may_delete());
    pause(Kind::Owned);
    assert!(!may_delete());
    *PAUSED_UNTIL.lock().unwrap() = Some(Instant::now() - Duration::from_secs(1));
    assert!(may_delete());
    reset();
}

/// A node whose token lacks only the `sys.facts` grant must go on deleting
/// the streams it owns.
#[test]
fn a_missing_facts_grant_does_not_pause_the_other_deletions() {
    reset();
    pause(Kind::Facts);
    assert!(may_delete());
    assert!(!may_delete_facts());
    reset();
    pause(Kind::Owned);
    assert!(!may_delete_facts());
    reset();
}

/// Only `sys.facts` is asked again at a tail Felix names, and only a few times.
#[test]
fn only_the_facts_stream_is_asked_again() {
    assert_eq!(Kind::Owned.retries(), 0);
    assert_eq!(Kind::Facts.retries(), 3);
}

fn reset() {
    *PAUSED_UNTIL.lock().unwrap() = None;
    *FACTS_PAUSED_UNTIL.lock().unwrap() = None;
}
