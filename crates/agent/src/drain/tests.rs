use super::*;

#[test]
fn a_missing_cache_reads_as_not_drained_only_until_the_cache_is_found() {
    let reads = MarkReads::default();
    assert!(!reads.decide(Lookup::NoCache).unwrap());
    assert!(!reads.decide(Lookup::Value(None)).unwrap());
    // Now it is a broker that is not serving it yet, and the answer is unknown.
    assert!(reads.decide(Lookup::NoCache).is_err());
}

#[test]
fn a_mark_this_build_cannot_read_still_drains() {
    let reads = MarkReads::default();
    let newer = Bytes::from_static(br#"{"format": 9, "value": {}}"#);
    assert!(reads.decide(Lookup::Value(Some(newer.clone()))).unwrap());
    assert!(reads.decide(Lookup::Value(Some(newer))).unwrap());
    let ours = Bytes::from(Drain {}.encode());
    assert!(reads.decide(Lookup::Value(Some(ours))).unwrap());
    assert!(reads.decide(Lookup::NoCache).is_err());
}

#[test]
fn a_drain_is_done_once_the_node_holds_nothing_and_has_said_so_or_is_down() {
    let progress = |up, acknowledged, held: &[&str]| Progress {
        up,
        acknowledged,
        held: held.iter().map(|&h| h.to_owned()).collect(),
    };
    assert!(progress(true, true, &[]).done());
    assert!(progress(false, false, &[]).done());
    // No lease yet, but a claim may be under way.
    assert!(!progress(true, false, &[]).done());
    assert!(!progress(true, true, &["w"]).done());
    assert!(!progress(false, false, &["w"]).done());
}
