use super::*;

const HEADER: SnapshotHeader = SnapshotHeader {
    input_offset: 1,
    output_tail: 2,
};

#[test]
fn a_record_round_trips() {
    let header = SnapshotHeader {
        input_offset: 41,
        output_tail: 1_000_000_000_000,
    };
    let record = encode(header, 7, &[1, 2, 3]);
    assert_eq!(
        decode(&record),
        Ok((Taken { header, epoch: 7 }, &[1u8, 2, 3][..]))
    );
}

#[test]
fn the_layout_is_fixed() {
    assert_eq!(
        encode(HEADER, 3, b"s"),
        [
            2, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 3, b's'
        ]
    );
}

#[test]
fn a_version_1_record_reads_as_epoch_0() {
    let record = [1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, b's'];
    assert_eq!(
        decode(&record),
        Ok((
            Taken {
                header: HEADER,
                epoch: 0
            },
            &b"s"[..]
        ))
    );
}

#[test]
fn a_short_record_is_refused() {
    assert_eq!(
        decode(&[2; 10]),
        Err(DecodeError::Truncated { need: 25, got: 10 })
    );
    assert_eq!(
        decode(&[1; 10]),
        Err(DecodeError::Truncated { need: 17, got: 10 })
    );
}

#[test]
fn an_unknown_version_is_refused() {
    let mut record = encode(HEADER, 1, b"");
    record[0] = 0;
    assert_eq!(decode(&record), Err(DecodeError::UnknownVersion(0)));
}
