use super::*;

const HEADER: SnapshotHeader = SnapshotHeader {
    input_offset: 1,
    output_tail: 2,
};

#[test]
fn a_version_2_record_round_trips() {
    let header = SnapshotHeader {
        input_offset: 41,
        output_tail: 1_000_000_000_000,
    };
    let record = encode(header, 7, &[1, 2, 3]);
    assert_eq!(
        decode(&record),
        Ok(Record::Whole(Taken { header, epoch: 7 }, &[1u8, 2, 3][..]))
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
        Ok(Record::Whole(
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
        decode(&[2; 20]),
        Err(DecodeError::Truncated { need: 25, got: 20 })
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
    record[0] = 4;
    assert_eq!(decode(&record), Err(DecodeError::UnknownVersion(4)));
    let mut chunk = encode_chunk(1, &sha256(b""), b"");
    chunk[1] = 2;
    assert_eq!(decode(&chunk), Err(DecodeError::UnknownKind(2)));
}

fn manifest(epoch: u64, chunks: &[(&[u8], u64)]) -> Manifest {
    Manifest {
        taken: Taken {
            header: HEADER,
            epoch,
        },
        chunks: chunks
            .iter()
            .map(|&(bytes, offset)| ChunkRef {
                sha256: sha256(bytes),
                offset,
            })
            .collect(),
    }
}

#[test]
fn a_chunk_round_trips_and_is_not_a_snapshot() {
    let hash = sha256(b"state");
    let record = encode_chunk(4, &hash, b"state");
    assert_eq!(&record[..2], [3, 0]);
    let decoded = decode(&record).unwrap();
    assert_eq!(
        decoded,
        Record::Chunk {
            epoch: 4,
            sha256: hash,
            bytes: b"state"
        }
    );
    assert_eq!(decoded.epoch(), 4);
    assert_eq!(decoded.taken(), None);
}

#[test]
fn a_manifest_round_trips() {
    for chunks in [&[][..], &[(&b"a"[..], 3), (b"", 9)][..]] {
        let manifest = manifest(6, chunks);
        let record = encode_manifest(&manifest);
        assert_eq!(record.len(), 30 + 40 * chunks.len());
        let decoded = decode(&record).unwrap();
        assert_eq!(decoded.taken(), Some(manifest.taken));
        assert_eq!(decoded.epoch(), 6);
        assert_eq!(decoded, Record::Manifest(manifest));
    }
}

#[test]
fn the_manifest_layout_is_fixed() {
    let record = encode_manifest(&manifest(3, &[(b"a", 5)]));
    assert_eq!(
        record[..30],
        [
            3, 1, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0,
            1
        ]
    );
    assert_eq!(record[30..62], sha256(b"a"));
    assert_eq!(record[62..], [0, 0, 0, 0, 0, 0, 0, 5]);
}

#[test]
fn a_bad_manifest_is_refused() {
    let record = encode_manifest(&manifest(3, &[(b"a", 5), (b"b", 6)]));
    for cut in 0..record.len() {
        assert!(
            matches!(decode(&record[..cut]), Err(DecodeError::Truncated { .. })),
            "cut at {cut}"
        );
    }
    let mut long = record.clone();
    long.push(0);
    assert_eq!(decode(&long), Err(DecodeError::TrailingBytes(1)));
    // A huge count is refused before anything is allocated for it.
    let mut huge = record[..30].to_vec();
    huge[26..30].copy_from_slice(&u32::MAX.to_be_bytes());
    assert_eq!(decode(&huge), Err(DecodeError::TooManyChunks(u32::MAX)));
    huge[26..30].copy_from_slice(&MAX_CHUNKS.to_be_bytes());
    assert!(matches!(decode(&huge), Err(DecodeError::Truncated { .. })));
}

#[test]
fn a_chunk_is_read_only_if_its_hash_matches() {
    let chunk = ChunkRef {
        sha256: sha256(b"state"),
        offset: 4,
    };
    let record = encode_chunk(1, &chunk.sha256, b"state");
    assert_eq!(read_chunk(&chunk, &record), Ok(&b"state"[..]));
    let forged = encode_chunk(1, &chunk.sha256, b"other");
    assert_eq!(
        read_chunk(&chunk, &forged),
        Err(ChunkError::HashMismatch(4))
    );
    let other = encode_chunk(1, &sha256(b"other"), b"other");
    assert_eq!(read_chunk(&chunk, &other), Err(ChunkError::HashMismatch(4)));
    let manifest = encode_manifest(&manifest(1, &[]));
    assert_eq!(read_chunk(&chunk, &manifest), Err(ChunkError::NotAChunk(4)));
    assert!(matches!(
        read_chunk(&chunk, b""),
        Err(ChunkError::Undecodable { offset: 4, .. })
    ));
}

#[test]
fn a_first_snapshot_writes_every_distinct_chunk() {
    let chunks: [&[u8]; 3] = [b"a", b"b", b"a"];
    let plan = plan(None, &chunks);
    assert_eq!(plan.write, [0, 1]);
    let taken = Taken {
        header: HEADER,
        epoch: 2,
    };
    assert_eq!(plan.manifest(taken, &[10]), None);
    assert_eq!(
        plan.manifest(taken, &[10, 11]),
        Some(manifest(2, &[(b"a", 10), (b"b", 11), (b"a", 10)]))
    );
}

#[test]
fn a_later_snapshot_reuses_unchanged_chunks() {
    let previous = manifest(1, &[(b"a", 10), (b"b", 11), (b"c", 12)]);
    let chunks: [&[u8]; 4] = [b"a", b"B", b"c", b"d"];
    let plan = plan(Some(&previous), &chunks);
    assert_eq!(plan.write, [1, 3]);
    let taken = Taken {
        header: HEADER,
        epoch: 2,
    };
    assert_eq!(
        plan.manifest(taken, &[20, 21]),
        Some(manifest(
            2,
            &[(b"a", 10), (b"B", 20), (b"c", 12), (b"d", 21)]
        ))
    );
    let same = super::plan(Some(&previous), &[b"a", b"b", b"c"]);
    assert!(same.write.is_empty());
    assert_eq!(same.manifest(taken, &[]).unwrap().chunks, previous.chunks);
}

#[test]
fn an_input_in_a_new_interval_starts_one() {
    // 100 ms intervals: 0..100_000 us, 100_000..200_000 us, ...
    assert!(!starts_interval(100, Some(10_000), 99_999));
    assert!(starts_interval(100, Some(99_999), 100_000));
    assert!(starts_interval(100, Some(50_000), 750_000));
    assert!(!starts_interval(100, Some(150_000), 199_999));
    assert!(!starts_interval(100, None, 500_000));
}

#[test]
fn a_clock_that_stepped_back_starts_nothing() {
    assert!(!starts_interval(100, Some(350_000), 120_000));
}
