use felix_client::RetryClass;

use super::*;

fn broker(code: ErrorCode) -> anyhow::Error {
    BrokerError::new(code, RetryClass::Fatal, "refused").into()
}

#[tokio::test]
async fn a_trim_at_the_right_tail_is_one_call() {
    let mut asked = Vec::new();
    let settled = settle(7, |expected| {
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

/// The caller's tail may be stale, as after a leader change, so a refusal
/// is not chased: the next snapshot asks again from a fresh tail.
#[tokio::test]
async fn a_refused_tail_is_never_asked_again_at_the_tail_it_names() {
    let mut asked = Vec::new();
    let settled = settle(7, |expected| {
        asked.push(expected);
        async { Ok(ConditionalTrim::Refused { tail: 9 }) }
    })
    .await
    .unwrap();
    assert_eq!((settled, asked), (Settled::TailMoved, vec![7]));
}

#[tokio::test]
async fn a_floor_already_in_place_writes_nothing() {
    let settled = settle(7, |_| async {
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
    let result = settle(7, |_| {
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
    *PAUSED_UNTIL.lock().unwrap() = None;
    assert!(may_delete());
    pause();
    assert!(!may_delete());
    *PAUSED_UNTIL.lock().unwrap() = Some(Instant::now() - Duration::from_secs(1));
    assert!(may_delete());
    *PAUSED_UNTIL.lock().unwrap() = None;
}
