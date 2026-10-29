use super::*;

fn h(input_offset: u64, index: u32) -> OutputHeader {
    OutputHeader {
        input_offset,
        index,
    }
}

fn out(header: OutputHeader, epoch: u64, payload: &[u8]) -> Record<'_> {
    Record::Output(Written { header, epoch }, Addressed::unnamed(payload))
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
            4, 0, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 2, 3, b'o', b'u', b't',
            0, b'x'
        ]
    );
    let keyed = Addressed {
        name: "a",
        key: Some(b"k"),
        payload: b"x",
    };
    assert_eq!(
        encode_addressed(h(7, 2), 5, keyed).unwrap()[22..],
        [1, b'a', 1, 0, 0, 0, 1, b'k', b'x']
    );
    assert_eq!(encode_claim(5, ""), [4, 1, 0, 0, 0, 0, 0, 0, 0, 5]);
    assert_eq!(
        encode_rejection(
            7,
            5,
            Rejected {
                code: 9,
                detail: b"d"
            }
        ),
        [
            4, 3, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0, 9, b'd'
        ]
    );
}

#[test]
fn a_claim_round_trips_and_holds_no_output() {
    let record = encode_claim(9, "");
    assert_eq!(
        decode(&record),
        Ok(Record::Claim {
            epoch: 9,
            node: b""
        })
    );
    assert_eq!(
        decode(&encode_claim(9, "node-a")),
        Ok(Record::Claim {
            epoch: 9,
            node: b"node-a"
        })
    );
    assert_eq!(decode(&record).unwrap().epoch(), 9);
    assert_eq!(decode(&record).unwrap().output(), None);
    assert!(is_marker(&record));
    assert!(!is_marker(&encode(h(1, 0), 9, b"")));
    assert!(!same_output(&record, &record));
}

#[test]
fn a_version_3_record_reads_as_out_with_no_key() {
    let output = [
        3, 0, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 2, b'x',
    ];
    assert_eq!(decode(&output), Ok(out(h(7, 2), 5, b"x")));
    let claim = [3, 1, 0, 0, 0, 0, 0, 0, 0, 5, b'n'];
    assert_eq!(
        decode(&claim),
        Ok(Record::Claim {
            epoch: 5,
            node: b"n"
        })
    );
    let failure = [3, 2, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 1, b'r'];
    assert_eq!(
        decode(&failure),
        Ok(Record::Failure {
            epoch: 5,
            spec_version: 1,
            reason: b"r"
        })
    );
    // Rejections are new in version 4.
    let mut rejection = encode_rejection(
        7,
        5,
        Rejected {
            code: 1,
            detail: b"",
        },
    );
    rejection[0] = 3;
    assert_eq!(decode(&rejection), Err(DecodeError::UnknownKind(3)));
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
    assert_eq!(decode(&[]), Err(DecodeError::Truncated { need: 1, got: 0 }));
    assert_eq!(
        decode(&[4, 0, 0]),
        Err(DecodeError::Truncated { need: 10, got: 3 })
    );
    assert_eq!(
        decode(&[4, 1, 0]),
        Err(DecodeError::Truncated { need: 10, got: 3 })
    );
    assert_eq!(
        decode(&[2, 0, 0]),
        Err(DecodeError::Truncated { need: 9, got: 3 })
    );
    let keyed = Addressed {
        name: "out",
        key: Some(b"key"),
        payload: b"",
    };
    let record = encode_addressed(h(1, 0), 1, keyed).unwrap();
    assert_eq!(
        decode(&record[..record.len() - 1]),
        Err(DecodeError::Truncated {
            need: record.len(),
            got: record.len() - 1
        })
    );
}

#[test]
fn every_cut_of_a_record_fails_without_panicking() {
    let keyed = Addressed {
        name: "n",
        key: Some(b"key"),
        payload: b"",
    };
    let records = [
        encode_addressed(h(1, 0), 1, keyed).unwrap(),
        encode_rejection(
            1,
            1,
            Rejected {
                code: 2,
                detail: b"",
            },
        ),
        encode_failure(1, 2, ""),
    ];
    for record in records {
        for cut in 0..record.len() {
            assert!(
                matches!(decode(&record[..cut]), Err(DecodeError::Truncated { .. })),
                "{record:?} cut at {cut}"
            );
        }
    }
    // A key length past the end of the record is truncated, not a panic.
    let mut record = encode_addressed(h(1, 0), 1, keyed).unwrap();
    record[25..29].copy_from_slice(&u32::MAX.to_be_bytes());
    assert!(matches!(
        decode(&record),
        Err(DecodeError::Truncated { .. })
    ));
}

#[test]
fn a_named_keyed_output_round_trips() {
    for key in [None, Some(&b""[..]), Some(&b"user-7"[..])] {
        let addressed = Addressed {
            name: "alerts",
            key,
            payload: b"payload",
        };
        let record = encode_addressed(h(3, 1), 2, addressed).unwrap();
        assert_eq!(
            decode(&record),
            Ok(Record::Output(
                Written {
                    header: h(3, 1),
                    epoch: 2
                },
                addressed
            ))
        );
    }
}

#[test]
fn output_names_are_checked() {
    for bad in ["", "Out", "a.b", &"a".repeat(65)] {
        let addressed = Addressed {
            name: bad,
            key: None,
            payload: b"",
        };
        assert_eq!(encode_addressed(h(0, 0), 1, addressed), Err(InvalidName));
    }
    let mut record = encode(h(0, 0), 1, b"");
    record[23] = b'O';
    assert_eq!(decode(&record), Err(DecodeError::InvalidName));
    record[22] = 0;
    assert_eq!(decode(&record), Err(DecodeError::InvalidName));
    let mut record = encode(h(0, 0), 1, b"");
    record[26] = 2;
    assert_eq!(decode(&record), Err(DecodeError::InvalidKeyFlag(2)));
}

#[test]
fn a_rejection_round_trips_and_holds_no_output() {
    let rejected = Rejected {
        code: 400,
        detail: b"bad input",
    };
    let record = encode_rejection(12, 3, rejected);
    let decoded = decode(&record).unwrap();
    let written = Written {
        header: h(12, 0),
        epoch: 3,
    };
    assert_eq!(decoded, Record::Rejection(written, rejected));
    assert_eq!(decoded.epoch(), 3);
    assert_eq!(decoded.written(), Some(written));
    assert_eq!(decoded.output(), None);
    assert!(!is_marker(&record));
    assert!(same_output(&record, &encode_rejection(12, 9, rejected)));
    assert!(!same_output(
        &record,
        &encode_rejection(
            12,
            3,
            Rejected {
                code: 401,
                ..rejected
            }
        )
    ));
    assert!(!same_output(&record, &encode(h(12, 0), 3, b"bad input")));
    let mut record = record;
    record[21] = 1;
    assert_eq!(decode(&record), Err(DecodeError::RejectionIndex(1)));
}

#[test]
fn a_reader_of_another_stream_gets_only_outputs_of_its_name() {
    let named = |name| Addressed {
        name,
        key: Some(b"k"),
        payload: b"p",
    };
    let ours = encode_addressed(h(1, 0), 1, named("out")).unwrap();
    assert_eq!(addressed_to(&ours, "out"), Ok(Some(named("out"))));
    assert!(!passed_over(&ours, "out"));
    let others = [
        encode_addressed(h(1, 1), 1, named("alerts")).unwrap(),
        encode_rejection(
            2,
            1,
            Rejected {
                code: 1,
                detail: b"",
            },
        ),
        encode_claim(1, "node"),
        encode_failure(1, 1, "trap"),
    ];
    for record in &others {
        assert_eq!(addressed_to(record, "out"), Ok(None), "{record:?}");
        assert!(passed_over(record, "out"), "{record:?}");
    }
    assert!(!passed_over(&others[0], "alerts"));
    // A record of an unknown format reaches the reader rather than vanish.
    assert!(addressed_to(b"\x09", "out").is_err());
    assert!(!passed_over(b"\x09", "out"));
}

#[test]
fn an_unknown_version_or_kind_is_refused() {
    let mut record = encode(h(1, 0), 1, b"");
    record[0] = 5;
    assert_eq!(decode(&record), Err(DecodeError::UnknownVersion(5)));
    record[0] = 4;
    record[1] = 4;
    assert_eq!(decode(&record), Err(DecodeError::UnknownKind(4)));
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
    let theirs = [encode_claim(7, "")];
    assert_eq!(
        reconcile(3, &ours, &refs(&theirs)),
        Reconciled::Superseded { epoch: 7 }
    );
}

#[test]
fn earlier_claims_are_passed_over() {
    let ours = batch(7, &[10, 11]);
    let mut theirs = vec![encode_claim(2, "")];
    theirs.extend(batch(3, &[10]));
    theirs.push(encode_claim(3, ""));
    assert_eq!(reconcile(7, &ours, &refs(&theirs)), Reconciled::Skip(1));
    let theirs = [encode_claim(7, "")];
    assert_eq!(reconcile(7, &ours, &refs(&theirs)), Reconciled::Skip(0));
}

#[test]
fn a_failure_round_trips_and_holds_no_output() {
    let record = encode_failure(4, 12, "out of fuel");
    assert_eq!(&record[..2], [4, 2]);
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
    let theirs = [encode_failure(5, 3, "trap"), encode_claim(8, "")];
    assert_eq!(
        reconcile(7, &ours, &refs(&theirs)),
        Reconciled::Superseded { epoch: 8 }
    );
}

#[test]
fn a_rejection_is_reconciled_like_an_output() {
    let rejected = Rejected {
        code: 7,
        detail: b"no",
    };
    let ours = vec![encode_rejection(10, 7, rejected)];
    let theirs = [encode_claim(3, ""), encode_rejection(10, 3, rejected)];
    assert_eq!(reconcile(7, &ours, &refs(&theirs)), Reconciled::Skip(1));
    // An output where we reject, or a rejection where we output, diverges.
    let theirs = batch(3, &[10]);
    assert_eq!(reconcile(7, &ours, &refs(&theirs)), Reconciled::Diverged);
    let theirs = vec![encode_rejection(10, 3, rejected)];
    assert_eq!(
        reconcile(7, &batch(7, &[10]), &refs(&theirs)),
        Reconciled::Diverged
    );
    // A rejection past our batch is a later input's, so resume.
    let theirs = [batch(3, &[10]).remove(0), encode_rejection(11, 3, rejected)];
    assert_eq!(
        reconcile(7, &batch(7, &[10]), &refs(&theirs)),
        Reconciled::Behind
    );
}
