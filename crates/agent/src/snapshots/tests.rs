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
