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

fn leb(mut n: usize) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let byte = (n & 0x7f) as u8;
        n >>= 7;
        if n == 0 {
            out.push(byte);
            return out;
        }
        out.push(byte | 0x80);
    }
}

fn section(id: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![id];
    out.extend(leb(body.len()));
    out.extend(body);
    out
}

fn custom(name: &str, data: &[u8]) -> Vec<u8> {
    let mut body = leb(name.len());
    body.extend(name.as_bytes());
    body.extend(data);
    section(0, &body)
}

const MODULE: &[u8] = b"\0asm\x01\0\0\0";
const COMPONENT: &[u8] = b"\0asm\x0d\0\x01\0";

fn module(sections: &[Vec<u8>]) -> Vec<u8> {
    [MODULE, &sections.concat()].concat()
}

/// A component holding `inner` as a core module, as `wasm-tools component
/// new` lays out a workload.
fn component(own: &[Vec<u8>], inner: &[u8]) -> Vec<u8> {
    [COMPONENT, &section(1, inner), &own.concat()].concat()
}

fn declared(json: &str) -> Vec<u8> {
    custom(SNAPSHOT_SECTION, json.as_bytes())
}

fn layouts_of(format: &str, reads: &[&str]) -> Layouts {
    Layouts {
        format: format.to_owned(),
        reads: reads.iter().map(|r| (*r).to_owned()).collect(),
    }
}

#[test]
fn a_module_without_the_section_declares_nothing() {
    assert_eq!(layouts(&module(&[])), Ok(None));
    assert_eq!(layouts(&module(&[custom("name", b"x")])), Ok(None));
    assert_eq!(layouts(&component(&[], &module(&[]))), Ok(None));
}

#[test]
fn reads_the_section_of_a_core_module() {
    let bytes = module(&[declared(r#"{"format":"counter","reads":["v0","v1"]}"#)]);
    assert_eq!(
        layouts(&bytes),
        Ok(Some(layouts_of("counter", &["v0", "v1"])))
    );
}

#[test]
fn reads_defaults_to_none() {
    let bytes = module(&[declared(r#"{"format":"counter"}"#)]);
    assert_eq!(layouts(&bytes), Ok(Some(layouts_of("counter", &[]))));
}

#[test]
fn finds_the_section_in_a_module_nested_in_a_component() {
    let inner = module(&[declared(r#"{"format":"nested"}"#)]);
    assert_eq!(
        layouts(&component(&[], &inner)),
        Ok(Some(layouts_of("nested", &[])))
    );
}

#[test]
fn finds_the_section_on_the_component() {
    let own = [declared(r#"{"format":"outer"}"#)];
    assert_eq!(
        layouts(&component(&own, &module(&[]))),
        Ok(Some(layouts_of("outer", &[])))
    );
}

#[test]
fn refuses_two_sections() {
    let one = declared(r#"{"format":"a"}"#);
    assert_eq!(
        layouts(&module(&[one.clone(), one.clone()])),
        Err(SectionError::Repeated)
    );
    let inner = module(std::slice::from_ref(&one));
    assert_eq!(
        layouts(&component(&[one], &inner)),
        Err(SectionError::Repeated)
    );
}

#[test]
fn refuses_a_section_that_is_not_the_declaration() {
    let bad = |json: &str| layouts(&module(&[declared(json)]));
    assert!(matches!(bad("not json"), Err(SectionError::Json(_))));
    assert!(matches!(bad("{}"), Err(SectionError::Json(_))));
    assert!(matches!(
        bad(r#"{"format":"a","extra":1}"#),
        Err(SectionError::Json(_))
    ));
    assert!(matches!(
        bad(r#"{"format":"a","reads":"b"}"#),
        Err(SectionError::Json(_))
    ));
    assert!(layouts(&module(&[custom(SNAPSHOT_SECTION, &[0xff])])).is_err());
}

#[test]
fn refuses_layout_ids_that_cannot_be_named() {
    let bad = |json: &str| layouts(&module(&[declared(json)]));
    for id in ["", "has space", "tab\\t", "caf\u{e9}"] {
        let format = format!(r#"{{"format":"{id}"}}"#);
        assert!(matches!(bad(&format), Err(SectionError::Id(_))), "{id:?}");
        let reads = format!(r#"{{"format":"ok","reads":["{id}"]}}"#);
        assert!(matches!(bad(&reads), Err(SectionError::Id(_))), "{id:?}");
    }
    let long = "x".repeat(MAX_LAYOUT_ID_BYTES + 1);
    assert!(matches!(
        bad(&format!(r#"{{"format":"{long}"}}"#)),
        Err(SectionError::Id(_))
    ));
    let longest = "x".repeat(MAX_LAYOUT_ID_BYTES);
    assert!(bad(&format!(r#"{{"format":"{longest}"}}"#)).is_ok());
}

#[test]
fn refuses_too_many_reads() {
    let reads: Vec<String> = (0..=MAX_READS).map(|i| format!("\"v{i}\"")).collect();
    let json = format!(r#"{{"format":"a","reads":[{}]}}"#, reads.join(","));
    assert_eq!(
        layouts(&module(&[declared(&json)])),
        Err(SectionError::TooManyReads)
    );
}

#[test]
fn refuses_bytes_that_are_not_wasm() {
    assert!(matches!(layouts(b"not wasm"), Err(SectionError::Module(_))));
}

fn info(hash: &str, layouts: Option<(&str, &[&str])>) -> ModuleInfo {
    ModuleInfo {
        hash: hash.to_owned(),
        layouts: layouts.map(|(format, reads)| layouts_of(format, reads)),
    }
}

#[test]
fn module_info_carries_the_hash_and_the_layouts() {
    let bytes = module(&[declared(r#"{"format":"a"}"#)]);
    let info = ModuleInfo::of(&bytes).unwrap();
    assert_eq!(info.hash, hash(&bytes));
    assert_eq!(info.layouts, Some(layouts_of("a", &[])));
}

#[test]
fn a_module_restores_its_own_snapshots_declared_or_not() {
    let declared = info("a", Some(("v1", &[])));
    assert_eq!(can_restore(&declared, &declared), Ok(()));
    let anonymous = info("b", None);
    assert_eq!(can_restore(&anonymous, &anonymous), Ok(()));
}

#[test]
fn a_candidate_restores_the_layout_it_writes_or_lists() {
    let recorded = info("a", Some(("v1", &[])));
    assert_eq!(
        can_restore(&recorded, &info("b", Some(("v1", &[])))),
        Ok(())
    );
    assert_eq!(
        can_restore(&recorded, &info("b", Some(("v2", &["v0", "v1"])))),
        Ok(())
    );
}

#[test]
fn a_candidate_that_does_not_list_the_layout_cannot() {
    let recorded = info("a", Some(("v1", &[])));
    assert_eq!(
        can_restore(&recorded, &info("b", Some(("v2", &["v0"])))),
        Err(Unreadable::NotListed {
            layout: "v1".to_owned(),
            writes: "v2".to_owned(),
        })
    );
}

#[test]
fn reads_names_layouts_it_restores_not_ones_it_writes_to() {
    // Reading is one way: a module that lists v2 restores v2 snapshots, and
    // the module that writes v2 does not thereby restore v1.
    let recorded = info("a", Some(("v1", &["v2"])));
    let candidate = info("b", Some(("v2", &[])));
    assert!(can_restore(&recorded, &candidate).is_err());
    assert_eq!(can_restore(&candidate, &recorded), Ok(()));
}

#[test]
fn an_undeclared_module_is_readable_by_itself_only() {
    let anonymous = info("a", None);
    assert_eq!(
        can_restore(&anonymous, &info("b", Some(("v1", &[])))),
        Err(Unreadable::RecordedUndeclared)
    );
    assert_eq!(
        can_restore(&anonymous, &info("b", None)),
        Err(Unreadable::RecordedUndeclared)
    );
    assert_eq!(
        can_restore(&info("a", Some(("v1", &[]))), &info("b", None)),
        Err(Unreadable::CandidateUndeclared)
    );
}

/// A module of `len` bytes that starts like WebAssembly and differs by
/// position, so a swapped or shifted chunk changes the hash.
fn wasm(len: usize) -> Vec<u8> {
    let mut bytes: Vec<u8> = (0..len).map(|i| (i * 31 + i / 7) as u8).collect();
    bytes[..4].copy_from_slice(b"\0asm");
    bytes
}

/// What a reader does: the head, then each chunk checked, then the whole.
fn read_back(key: &str, head: &[u8], chunks: &[(String, &[u8])]) -> Result<Vec<u8>, Corrupt> {
    match read_head(key, head)? {
        Head::Whole => Ok(head.to_vec()),
        Head::Chunked(manifest) => {
            assert_eq!(manifest.len(), chunks.len());
            let mut joined = Vec::new();
            for (n, (found_key, chunk)) in chunks.iter().enumerate() {
                assert_eq!(*found_key, chunk_key(key, n));
                manifest.check_chunk(key, n, chunk)?;
                joined.extend_from_slice(chunk);
            }
            assert_eq!(joined.len() as u64, manifest.bytes());
            verify(key, &joined)?;
            Ok(joined)
        }
    }
}

#[test]
fn a_module_up_to_one_chunk_is_stored_whole() {
    for len in [4, 1000, CHUNK_BYTES - 1, CHUNK_BYTES] {
        let module = wasm(len);
        let laid = split(&module);
        assert!(laid.chunks.is_empty(), "{len}");
        assert_eq!(&laid.head[..], &module[..], "{len}");
        assert_eq!(read_head(&hash(&module), &laid.head), Ok(Head::Whole));
    }
}

#[test]
fn a_module_over_one_chunk_is_split_and_joins_back() {
    // Just over, a full chunk and a sliver, and several chunks.
    for len in [
        CHUNK_BYTES + 1,
        2 * CHUNK_BYTES - 1,
        2 * CHUNK_BYTES,
        2 * CHUNK_BYTES + 1,
        5 * CHUNK_BYTES + 12345,
    ] {
        let module = wasm(len);
        let key = hash(&module);
        let laid = split(&module);
        let count = len.div_ceil(CHUNK_BYTES);
        assert_eq!(laid.chunks.len(), count, "{len}");
        assert!(
            laid.chunks[..count - 1]
                .iter()
                .all(|(_, c)| c.len() == CHUNK_BYTES)
        );
        assert_eq!(
            laid.chunks[count - 1].1.len(),
            len - (count - 1) * CHUNK_BYTES,
            "{len}"
        );
        assert!(laid.head.len() < 4096, "a manifest is small");
        assert_eq!(read_back(&key, &laid.head, &laid.chunks).unwrap(), module);
    }
}

#[test]
fn chunk_keys_are_the_module_key_and_an_index() {
    let module = wasm(2 * CHUNK_BYTES + 1);
    let key = hash(&module);
    let keys: Vec<_> = split(&module).chunks.into_iter().map(|(k, _)| k).collect();
    assert_eq!(
        keys,
        [format!("{key}/0"), format!("{key}/1"), format!("{key}/2")]
    );
}

#[test]
fn a_manifest_is_told_from_wasm_by_its_first_bytes() {
    let small = wasm(100);
    assert_eq!(read_head(&hash(&small), &small), Ok(Head::Whole));
    let big = wasm(CHUNK_BYTES + 1);
    let laid = split(&big);
    assert!(!laid.head.starts_with(b"\0asm"));
    assert!(matches!(
        read_head(&hash(&big), &laid.head),
        Ok(Head::Chunked(_))
    ));
}

#[test]
fn a_whole_module_larger_than_a_chunk_still_reads() {
    // Written before chunking, up to the old 8 MiB limit.
    let module = wasm(7 << 20);
    assert_eq!(read_head(&hash(&module), &module), Ok(Head::Whole));
}

#[test]
fn a_value_that_is_neither_is_corrupt() {
    for value in [&b""[..], b"hello", b"{", b"[1,2]", b"{\"format\":1}"] {
        assert!(matches!(
            read_head("abc", value),
            Err(Corrupt::NotAModule(_))
        ));
    }
}

#[test]
fn whole_bytes_under_the_wrong_key_are_corrupt() {
    let module = wasm(100);
    assert_eq!(
        read_head("abc", &module),
        Err(Corrupt::Hash("abc".to_owned()))
    );
}

#[test]
fn a_flipped_byte_in_a_chunk_is_found() {
    let module = wasm(3 * CHUNK_BYTES);
    let key = hash(&module);
    let laid = split(&module);
    let mut bad = laid.chunks[1].1.to_vec();
    bad[10] ^= 1;
    let chunks: Vec<(String, &[u8])> = vec![
        laid.chunks[0].clone(),
        (laid.chunks[1].0.clone(), &bad),
        laid.chunks[2].clone(),
    ];
    assert_eq!(
        read_back(&key, &laid.head, &chunks),
        Err(Corrupt::ChunkHash { hash: key, n: 1 })
    );
}

#[test]
fn a_short_or_long_chunk_is_found_by_length() {
    let module = wasm(2 * CHUNK_BYTES + 5);
    let key = hash(&module);
    let laid = split(&module);
    let Ok(Head::Chunked(manifest)) = read_head(&key, &laid.head) else {
        panic!("not a manifest");
    };
    let first = laid.chunks[0].1;
    assert_eq!(
        manifest.check_chunk(&key, 0, &first[1..]),
        Err(Corrupt::ChunkLength {
            hash: key.clone(),
            n: 0,
            found: CHUNK_BYTES - 1,
            expected: CHUNK_BYTES
        })
    );
    assert_eq!(
        manifest.check_chunk(&key, 2, laid.chunks[1].1),
        Err(Corrupt::ChunkLength {
            hash: key.clone(),
            n: 2,
            found: CHUNK_BYTES,
            expected: 5
        })
    );
}

#[test]
fn chunks_of_another_module_do_not_join_under_this_key() {
    let a = wasm(2 * CHUNK_BYTES);
    let mut b = a.clone();
    b[CHUNK_BYTES + 1] ^= 0xff;
    let (la, lb) = (split(&a), split(&b));
    // Right manifest, chunks swapped in from the other module.
    let Ok(Head::Chunked(manifest)) = read_head(&hash(&a), &la.head) else {
        panic!("not a manifest");
    };
    manifest.check_chunk(&hash(&a), 0, lb.chunks[0].1).unwrap();
    assert_eq!(
        manifest.check_chunk(&hash(&a), 1, lb.chunks[1].1),
        Err(Corrupt::ChunkHash {
            hash: hash(&a),
            n: 1
        })
    );
}

#[test]
fn a_manifest_that_lies_or_is_from_the_future_is_refused() {
    let key = hash(b"x");
    let bad = |json: &str| read_head(&key, json.as_bytes()).unwrap_err();
    assert!(matches!(
        bad(r#"{"format":2,"bytes":5242880,"chunks":["a","b"]}"#),
        Corrupt::UnknownFormat { format: 2, .. }
    ));
    for json in [
        // Wrong chunk count for the length.
        r#"{"format":1,"bytes":5242880,"chunks":["a"]}"#,
        r#"{"format":1,"bytes":5242880,"chunks":["a","b","c"]}"#,
        // Empty, and over the limit.
        r#"{"format":1,"bytes":0,"chunks":[]}"#,
        r#"{"format":1,"bytes":67108865,"chunks":[]}"#,
    ] {
        assert!(matches!(bad(json), Corrupt::Manifest { .. }), "{json}");
    }
}

#[test]
fn the_size_message_names_the_range() {
    assert_eq!(size_problem(1, 10), None);
    assert_eq!(size_problem(10, 10), None);
    assert_eq!(
        size_problem(0, 10).unwrap(),
        "the module is 0 bytes, not 1 to 10"
    );
    assert_eq!(
        size_problem(11, 10).unwrap(),
        "the module is 11 bytes, not 1 to 10"
    );
}
