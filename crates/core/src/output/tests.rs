use super::*;

fn h(input_offset: u64, index: u32) -> OutputHeader {
    OutputHeader {
        input_offset,
        index,
    }
}

#[test]
fn a_record_round_trips() {
    let header = h(0x0102_0304_0506_0708, 9);
    let record = encode(header, 42, b"payload");
    assert_eq!(
        decode(&record),
        Ok((Written { header, epoch: 42 }, &b"payload"[..]))
    );
}

#[test]
fn the_layout_is_fixed() {
    let record = encode(h(7, 2), 5, b"x");
    assert_eq!(
        record,
        [
            2, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 5, b'x'
        ]
    );
}

#[test]
fn an_empty_payload_is_allowed() {
    let header = h(1, 0);
    assert_eq!(
        decode(&encode(header, 1, b"")),
        Ok((Written { header, epoch: 1 }, &b""[..]))
    );
}

#[test]
fn a_short_record_is_refused() {
    assert_eq!(
        decode(&[]),
        Err(DecodeError::Truncated { need: 21, got: 0 })
    );
    assert_eq!(
        decode(&[2, 0, 0]),
        Err(DecodeError::Truncated { need: 21, got: 3 })
    );
}

#[test]
fn an_unknown_version_is_refused() {
    let mut record = encode(h(1, 0), 1, b"");
    record[0] = 1;
    assert_eq!(decode(&record), Err(DecodeError::UnknownVersion(1)));
}

#[test]
fn headers_order_by_input_then_index() {
    assert!(h(4, 9) < h(5, 0));
    assert!(h(5, 0) < h(5, 1));
}

#[test]
fn the_same_output_from_two_epochs_is_the_same() {
    assert!(same_output(
        &encode(h(3, 0), 1, b"a"),
        &encode(h(3, 0), 9, b"a")
    ));
    assert!(!same_output(
        &encode(h(3, 0), 1, b"a"),
        &encode(h(3, 0), 1, b"b")
    ));
    assert!(!same_output(
        &encode(h(3, 0), 1, b"a"),
        &encode(h(3, 1), 1, b"a")
    ));
    assert!(!same_output(b"junk", &encode(h(3, 0), 1, b"a")));
}

fn batch(epoch: u64, inputs: &[u64]) -> Vec<Vec<u8>> {
    inputs
        .iter()
        .map(|&i| encode(h(i, 0), epoch, &i.to_be_bytes()))
        .collect()
}

fn refs(records: &[Vec<u8>]) -> Vec<&[u8]> {
    records.iter().map(Vec::as_slice).collect()
}

#[test]
fn an_earlier_owners_matching_records_are_skipped() {
    let ours = batch(7, &[10, 11]);
    let theirs = batch(3, &[10]);
    assert_eq!(reconcile(7, &ours, &refs(&theirs)), Reconciled::Skip(1));
    let theirs = batch(3, &[10, 11]);
    assert_eq!(reconcile(7, &ours, &refs(&theirs)), Reconciled::Skip(2));
}

#[test]
fn our_own_records_from_a_lost_answer_are_skipped() {
    let ours = batch(7, &[10, 11]);
    assert_eq!(reconcile(7, &ours, &refs(&ours)), Reconciled::Skip(2));
}

#[test]
fn a_later_owners_records_stop_this_owner() {
    // Identical records, but from a later epoch: skipping them and writing on
    // would interleave two owners on the stream.
    let ours = batch(3, &[10, 11]);
    let theirs = batch(7, &[10]);
    assert_eq!(
        reconcile(3, &ours, &refs(&theirs)),
        Reconciled::Superseded { epoch: 7 }
    );
    let theirs = batch(7, &[10, 11, 12]);
    assert_eq!(
        reconcile(3, &ours, &refs(&theirs)),
        Reconciled::Superseded { epoch: 7 }
    );
}

#[test]
fn an_earlier_owner_past_our_batch_means_resume() {
    let ours = batch(7, &[10]);
    let theirs = batch(3, &[10, 11, 12]);
    assert_eq!(reconcile(7, &ours, &refs(&theirs)), Reconciled::Behind);
}

#[test]
fn different_records_are_a_divergence() {
    let ours = batch(7, &[10]);
    let theirs = batch(3, &[11]);
    assert_eq!(reconcile(7, &ours, &refs(&theirs)), Reconciled::Diverged);
    assert_eq!(reconcile(7, &ours, &[b"junk"]), Reconciled::Diverged);
}
