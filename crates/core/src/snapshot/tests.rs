use super::*;

#[test]
fn a_record_round_trips() {
    let header = SnapshotHeader {
        input_offset: 41,
        output_tail: 1_000_000_000_000,
    };
    let record = encode(header, &[1, 2, 3]);
    assert_eq!(decode(&record), Ok((header, &[1u8, 2, 3][..])));
}

#[test]
fn the_layout_is_fixed() {
    let record = encode(
        SnapshotHeader {
            input_offset: 1,
            output_tail: 2,
        },
        b"s",
    );
    assert_eq!(
        record,
        [1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, b's']
    );
}

#[test]
fn a_short_record_is_refused() {
    assert_eq!(
        decode(&[1; 10]),
        Err(DecodeError::Truncated { need: 17, got: 10 })
    );
}

#[test]
fn an_unknown_version_is_refused() {
    let mut record = encode(
        SnapshotHeader {
            input_offset: 1,
            output_tail: 2,
        },
        b"",
    );
    record[0] = 0;
    assert_eq!(decode(&record), Err(DecodeError::UnknownVersion(0)));
}
