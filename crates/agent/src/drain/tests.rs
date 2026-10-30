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
    let ours = Bytes::from(Drain { nonce: 5 }.encode());
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

#[test]
fn the_nonce_of_the_last_mark_read_is_kept_until_the_mark_goes() {
    let reads = MarkReads::default();
    let mark = |nonce| Lookup::Value(Some(Bytes::from(Drain { nonce }.encode())));
    reads.decide(mark(5)).unwrap();
    assert_eq!(reads.nonce(), Some(5));
    reads.decide(mark(6)).unwrap();
    assert_eq!(reads.nonce(), Some(6));
    // Unknown, so the node keeps what it read.
    assert!(reads.decide(Lookup::NoCache).is_err());
    assert_eq!(reads.nonce(), Some(6));
    reads.decide(Lookup::Value(None)).unwrap();
    assert_eq!(reads.nonce(), None);
}
