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
