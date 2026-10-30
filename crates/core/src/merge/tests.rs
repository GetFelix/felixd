use super::*;

fn entry(input: u16, offset: u64) -> Entry {
    Entry {
        input,
        offset,
        timestamp_micros: 1000 + offset,
    }
}

#[test]
fn an_entry_round_trips_and_moves_next() {
    let mut next = vec![0, 17];
    let record = encode_entry(3, entry(0, 41), &mut next);
    assert_eq!(next, [42, 17]);
    assert_eq!(
        decode(&record),
        Ok(Record::Entry {
            epoch: 3,
            entry: entry(0, 41),
            next: vec![42, 17],
        })
    );
}

#[test]
fn the_layout_is_fixed() {
    let mut next = vec![0];
    let record = encode_entry(5, entry(0, 7), &mut next);
    assert_eq!(
        record,
        [
            1, 0, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 3, 239,
            0, 1, 0, 0, 0, 0, 0, 0, 0, 8
        ]
    );
    assert_eq!(
        encode_claim(5, &[8], "n"),
        [
            1, 1, 0, 0, 0, 0, 0, 0, 0, 5, 0, 1, 0, 0, 0, 0, 0, 0, 0, 8, b'n'
        ]
    );
}

#[test]
fn a_claim_round_trips() {
    let record = encode_claim(9, &[4, 0, 2], "node-a");
    let decoded = decode(&record).unwrap();
    assert_eq!(decoded.epoch(), 9);
    assert_eq!(decoded.next(), [4, 0, 2]);
    assert_eq!(decoded.entry(), None);
    assert!(matches!(
        decoded,
        Record::Claim {
            node: b"node-a",
            ..
        }
    ));
}

#[test]
fn unknown_versions_and_kinds_are_refused() {
    let mut record = encode_claim(1, &[0], "");
    record[0] = 2;
    assert_eq!(decode(&record), Err(DecodeError::UnknownVersion(2)));
    record[0] = 1;
    record[1] = 9;
    assert_eq!(decode(&record), Err(DecodeError::UnknownKind(9)));
    let mut entry = encode_entry(1, entry(0, 1), &mut [0]);
    entry.push(0);
    assert_eq!(decode(&entry), Err(DecodeError::TrailingBytes(1)));
}

#[test]
fn the_cursor_comes_from_the_last_record() {
    assert_eq!(cursor(None, 2), Ok(vec![0, 0]));
    let claim = encode_claim(1, &[3, 5], "");
    let claim = decode(&claim).unwrap();
    assert_eq!(cursor(Some(&claim), 2), Ok(vec![3, 5]));
    assert_eq!(
        cursor(Some(&claim), 3),
        Err(WrongInputs {
            found: 2,
            inputs: 3
        })
    );
}

#[test]
fn reconcile_skips_its_own_records_and_stops_for_a_later_holder() {
    let mut next = vec![0, 0];
    let batch = vec![
        encode_entry(4, entry(0, 0), &mut next),
        encode_entry(4, entry(1, 0), &mut next),
    ];
    assert_eq!(reconcile(4, &batch, &[&batch[0]]), Reconciled::Skip(1));
    assert_eq!(reconcile(4, &batch, &[]), Reconciled::Skip(0));
    let later = encode_claim(5, &[1, 0], "b");
    assert_eq!(
        reconcile(4, &batch, &[&batch[0], &later]),
        Reconciled::Superseded { epoch: 5 }
    );
    // An earlier holder cannot append past this holder's claim, so its entry
    // here is not one to adopt.
    let earlier = encode_entry(3, entry(0, 0), &mut [0, 0]);
    assert_eq!(reconcile(4, &batch, &[&earlier]), Reconciled::Diverged);
    assert_eq!(reconcile(4, &batch, &[b"junk"]), Reconciled::Diverged);
}

#[test]
fn a_write_of_entries_fits_in_one_frame() {
    for inputs in [2, 3, MAX_INPUTS] {
        let per_write = entries_per_write(inputs);
        let one = encode_entry(1, entry(0, 0), &mut vec![0; inputs]).len();
        assert!(per_write * one <= crate::MAX_WRITE_BYTES, "{inputs}");
        assert!((per_write + 1) * one > crate::MAX_WRITE_BYTES, "{inputs}");
    }
}
