use super::*;

#[test]
fn streams_are_named_after_the_workload() {
    assert_eq!(output_stream("counter-1"), "wl.counter-1.out");
    assert_eq!(snapshot_stream("counter-1"), "wl.counter-1.snap");
    assert_eq!(merge_stream("counter-1"), "wl.counter-1.merge");
    assert!(is_output_stream("wl.counter-1.out"));
    assert_eq!(output_of("wl.counter-1.out"), Some("counter-1"));
    assert_eq!(output_of("wl.counter-1.snap"), None);
    for other in ["in", "wl.counter-1.snap", "wl..out", "wl.Up.out"] {
        assert!(!is_output_stream(other), "{other}");
    }
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

#[test]
fn system_streams_live_in_their_own_namespace() {
    for stream in [SPECS, INTAKE, FACTS] {
        assert_eq!(namespace_of("prod", stream), "prod.sys", "{stream}");
    }
    for stream in ["in", "wl.admission.out", "wl.scheduler.out", "system"] {
        assert_eq!(namespace_of("prod", stream), "prod", "{stream}");
    }
}
