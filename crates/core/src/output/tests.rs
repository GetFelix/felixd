use super::*;

fn h(input_offset: u64, index: u32) -> OutputHeader {
    OutputHeader {
        input_offset,
        index,
    }
}

fn out(header: OutputHeader, epoch: u64, payload: &[u8]) -> Record<'_> {
    Record::Output(Written { header, epoch }, payload)
}

#[test]
fn a_record_round_trips() {
    let header = h(0x0102_0304_0506_0708, 9);
    let record = encode(header, 42, b"payload");
    assert_eq!(decode(&record), Ok(out(header, 42, b"payload")));
}

#[test]
fn the_layout_is_fixed() {
    let record = encode(h(7, 2), 5, b"x");
    assert_eq!(
        record,
        [
            3, 0, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 2, b'x'
        ]
    );
    assert_eq!(encode_claim(5), [3, 1, 0, 0, 0, 0, 0, 0, 0, 5]);
}

#[test]
fn a_claim_round_trips_and_holds_no_output() {
    let record = encode_claim(9);
    assert_eq!(decode(&record), Ok(Record::Claim { epoch: 9 }));
    assert_eq!(decode(&record).unwrap().epoch(), 9);
    assert_eq!(decode(&record).unwrap().output(), None);
    assert!(is_marker(&record));
    assert!(!is_marker(&encode(h(1, 0), 9, b"")));
    assert!(!same_output(&record, &record));
}

#[test]
fn a_version_2_record_still_reads() {
    let record = [
        2, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 5, b'x',
    ];
    assert_eq!(decode(&record), Ok(out(h(7, 2), 5, b"x")));
}

#[test]
fn an_empty_payload_is_allowed() {
    let header = h(1, 0);
    assert_eq!(decode(&encode(header, 1, b"")), Ok(out(header, 1, b"")));
}

#[test]
fn a_short_record_is_refused() {
    assert_eq!(
        decode(&[]),
        Err(DecodeError::Truncated { need: 10, got: 0 })
    );
    assert_eq!(
        decode(&[3, 0, 0]),
        Err(DecodeError::Truncated { need: 22, got: 3 })
    );
    assert_eq!(
        decode(&[3, 1, 0]),
        Err(DecodeError::Truncated { need: 10, got: 3 })
    );
    assert_eq!(
        decode(&[2, 0, 0]),
        Err(DecodeError::Truncated { need: 21, got: 3 })
    );
}

#[test]
fn an_unknown_version_or_kind_is_refused() {
    let mut record = encode(h(1, 0), 1, b"");
    record[0] = 4;
    assert_eq!(decode(&record), Err(DecodeError::UnknownVersion(4)));
    record[0] = 3;
    record[1] = 3;
    assert_eq!(decode(&record), Err(DecodeError::UnknownKind(3)));
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

#[test]
fn a_later_claim_stops_this_owner() {
    let ours = batch(3, &[10]);
    let theirs = [encode_claim(7)];
    assert_eq!(
        reconcile(3, &ours, &refs(&theirs)),
        Reconciled::Superseded { epoch: 7 }
    );
}

#[test]
fn earlier_claims_are_passed_over() {
    let ours = batch(7, &[10, 11]);
    let mut theirs = vec![encode_claim(2)];
    theirs.extend(batch(3, &[10]));
    theirs.push(encode_claim(3));
    assert_eq!(reconcile(7, &ours, &refs(&theirs)), Reconciled::Skip(1));
    let theirs = [encode_claim(7)];
    assert_eq!(reconcile(7, &ours, &refs(&theirs)), Reconciled::Skip(0));
}

#[test]
fn a_failure_round_trips_and_holds_no_output() {
    let record = encode_failure(4, 12, "out of fuel");
    assert_eq!(&record[..2], [3, 2]);
    assert_eq!(
        decode(&record),
        Ok(Record::Failure {
            epoch: 4,
            spec_version: 12,
            reason: b"out of fuel",
        })
    );
    assert_eq!(decode(&record).unwrap().epoch(), 4);
    assert_eq!(decode(&record).unwrap().output(), None);
    assert!(is_marker(&record));
    assert!(!same_output(&record, &record));
    assert!(decode(&record[..17]).is_err());
}

#[test]
fn a_failure_ends_the_run_unless_a_later_owner_wrote() {
    let ours = batch(7, &[10]);
    let theirs = [encode_failure(5, 3, "trap")];
    assert_eq!(
        reconcile(7, &ours, &refs(&theirs)),
        Reconciled::Failed {
            spec_version: 3,
            reason: "trap".to_owned()
        }
    );
    let theirs = [encode_failure(5, 3, "trap"), encode_claim(8)];
    assert_eq!(
        reconcile(7, &ours, &refs(&theirs)),
        Reconciled::Superseded { epoch: 8 }
    );
}
