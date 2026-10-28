use super::*;

#[test]
fn streams_are_named_after_the_workload() {
    assert_eq!(output_stream("counter-1"), "wl.counter-1.out");
    assert_eq!(snapshot_stream("counter-1"), "wl.counter-1.snap");
}

#[test]
fn ids_are_checked() {
    assert!(validate_id("a").is_ok());
    assert!(validate_id("fraud-score-7").is_ok());
    assert!(validate_id(&"a".repeat(64)).is_ok());
    for bad in [
        "",
        "Upper",
        "has.dot",
        "has space",
        "slash/",
        &"a".repeat(65),
    ] {
        assert_eq!(validate_id(bad), Err(InvalidId), "{bad:?}");
    }
}
