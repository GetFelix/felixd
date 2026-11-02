use felixd_core::snapshot::{ChunkRef, SnapshotHeader};

use super::*;

fn chunked(chunks: &[u64]) -> Snapshot {
    Snapshot::Chunked(Manifest {
        taken: Taken {
            header: SnapshotHeader {
                input_offset: 9,
                output_tail: 10,
            },
            epoch: 1,
        },
        chunks: chunks
            .iter()
            .map(|&offset| ChunkRef {
                sha256: [0; 32],
                offset,
            })
            .collect(),
    })
}

/// A snapshot whose chunk Felix no longer serves would fail a replay that
/// chose it, though a later snapshot restores.
#[test]
fn a_snapshot_with_a_chunk_below_the_first_offset_held_is_not_restorable() {
    assert!(chunked(&[12, 30]).restorable(12));
    assert!(!chunked(&[11, 30]).restorable(12));
    assert!(chunked(&[]).restorable(12));
}

#[test]
fn a_whole_snapshot_is_restorable_wherever_it_sits() {
    let taken = Taken {
        header: SnapshotHeader {
            input_offset: 1,
            output_tail: 2,
        },
        epoch: 1,
    };
    assert!(Snapshot::Whole(taken, Bytes::new()).restorable(100));
}

fn record(taken_step: u64, chunks: &[u64]) -> Bytes {
    let manifest = Manifest {
        taken: Taken {
            header: SnapshotHeader {
                input_offset: taken_step,
                output_tail: 0,
            },
            epoch: 1,
        },
        chunks: chunks
            .iter()
            .map(|&offset| ChunkRef {
                sha256: [0; 32],
                offset,
            })
            .collect(),
    };
    Bytes::from(snapshot::encode_manifest(&manifest))
}

/// A trim to a newer snapshot's floor leaves an older manifest above it when
/// that manifest shares a chunk from below. Replay must not choose it, though
/// its own offset is held.
#[test]
fn a_manifest_above_the_floor_naming_a_chunk_below_it_is_not_restorable() {
    let chunk = Bytes::from(snapshot::encode_chunk(1, &[0; 32], b"x"));
    // Felix holds from 9. The manifest at 10 shares a chunk at 5.
    let (older, newer) = (record(4, &[5, 9]), record(8, &[9, 12]));
    let held = [(9, &chunk), (10, &older), (12, &chunk), (15, &newer)];
    let found = restorable("wl.a.snap", held).unwrap();
    let steps: Vec<u64> = found.iter().map(|s| s.taken().header.input_offset).collect();
    assert_eq!(steps, [8]);
}
