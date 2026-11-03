use super::*;

const FRAME: usize = 16 << 20;
const MIB: usize = 1 << 20;

fn cache_value(len: usize) -> Result<(), TooLarge> {
    check(FRAME, Wire::Json, 40, [len], || {
        "the value for key `k`".into()
    })
}

#[test]
fn a_value_that_fits_is_allowed() {
    for len in [0, 1, MIB, 5 * MIB, 8 * MIB, 11 * MIB] {
        assert_eq!(cache_value(len), Ok(()), "{len} bytes");
    }
}

#[test]
fn a_value_over_the_frame_limit_is_refused_naming_the_value_and_the_limit() {
    let refused = cache_value(20 * MIB).unwrap_err();
    assert_eq!(refused.bytes(), 20 * MIB);
    let message = refused.to_string();
    assert!(
        message.starts_with("the value for key `k` is 20971520 bytes"),
        "{message}"
    );
    assert!(
        message.contains(&format!("over the {} bytes", refused.limit())),
        "{message}"
    );
    assert!(message.contains("16777216-byte frame limit"), "{message}");
}

#[test]
fn the_reported_limit_is_the_largest_value_that_is_allowed() {
    let limit = cache_value(20 * MIB).unwrap_err().limit();
    assert_eq!(cache_value(limit), Ok(()));
    assert!(cache_value(limit + 1).is_err());
}

#[test]
fn a_json_value_counts_its_base64_size() {
    // 13 MiB is under the frame limit raw and over it as base64.
    assert!(cache_value(13 * MIB).is_err());
    let raw = check(FRAME, Wire::Binary, 40, [13 * MIB], String::new);
    assert_eq!(raw, Ok(()));
}

#[test]
fn a_batch_counts_every_record() {
    let records = || vec![5 * MIB; 3];
    assert_eq!(check(FRAME, Wire::Json, 40, [5 * MIB], String::new), Ok(()));
    let refused = check(FRAME, Wire::Json, 40, records(), || "a batch".into()).unwrap_err();
    assert_eq!(refused.bytes(), 15 * MIB);
    // Each record rounds up to whole base64 groups, so leave a little.
    let fits = vec![refused.limit() / 3 - 3; 3];
    assert_eq!(check(FRAME, Wire::Json, 40, fits, String::new), Ok(()));
}

#[test]
fn the_error_survives_anyhow_so_callers_can_tell_it_apart() {
    let error: anyhow::Error = cache_value(20 * MIB).unwrap_err().into();
    assert!(error.is::<TooLarge>());
}

#[test]
fn a_smaller_frame_limit_lowers_the_allowed_size() {
    let small = 4 * MIB;
    assert!(check(small, Wire::Json, 40, [3 * MIB], String::new).is_err());
    assert_eq!(check(small, Wire::Json, 40, [2 * MIB], String::new), Ok(()));
}
