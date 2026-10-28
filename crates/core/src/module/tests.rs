use super::*;

#[test]
fn matches_the_known_sha256_of_abc() {
    assert_eq!(
        hash(b"abc"),
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );
}

#[test]
fn different_bytes_hash_differently() {
    assert_ne!(hash(b"a"), hash(b"b"));
}
