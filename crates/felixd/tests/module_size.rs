//! A module larger than one write to Felix (#21) is stored in chunks, joined
//! and checked when read, and runs like any other. A corrupt or missing chunk
//! is an error and a re-put repairs it.
//!
//! `measure_cache_values` prints how long Felix takes over cache values of
//! several sizes and is skipped unless `FELIXD_MEASURE_MODULES` is set; run it
//! with `--nocapture`. Needs a Felix broker; see `common`.

mod common;

use std::time::{Duration, Instant};

use bytes::Bytes;
use common::{ID, INPUT, OUTPUT, assert_uninterrupted, replication_factor, test_felix};
use felixd_agent::admin::{StreamKind, ensure_cache, ensure_stream};
use felixd_agent::module_store::{self, Fetched};
use felixd_core::module::{self, CHUNK_BYTES, Corrupt};
use felixd_core::names;

const MIB: usize = 1 << 20;

/// Bytes that do not compress and differ per size.
fn noise(len: usize, seed: u8) -> Bytes {
    let mut state = u64::from(seed) + 1;
    (0..len)
        .map(|_| {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            (state >> 56) as u8
        })
        .collect::<Vec<u8>>()
        .into()
}

#[tokio::test(flavor = "multi_thread")]
async fn measure_cache_values() {
    if std::env::var_os("FELIXD_MEASURE_MODULES").is_none() {
        eprintln!("skipping: FELIXD_MEASURE_MODULES is not set");
        return;
    }
    let Some(t) = test_felix().await else { return };
    let kind = StreamKind {
        replication_factor: replication_factor(),
    };
    ensure_cache(&t.settings, "measure", kind).await.unwrap();
    for (n, mib) in [1, 5, 20].into_iter().enumerate() {
        let value = noise(mib * MIB, n as u8);
        let key = format!("v{mib}");
        let started = Instant::now();
        let put = tokio::time::timeout(
            Duration::from_secs(90),
            t.felix.cache_put("measure", &key, value.clone()),
        )
        .await;
        let put_took = started.elapsed();
        match put {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                println!("MEASURE {mib} MiB put refused after {put_took:?}: {error:#}");
                continue;
            }
            Err(_) => {
                println!("MEASURE {mib} MiB put timed out after {put_took:?}");
                continue;
            }
        }
        let started = Instant::now();
        let got = t.felix.cache_get("measure", &key).await;
        let get_took = started.elapsed();
        match got {
            Ok(Some(found)) => println!(
                "MEASURE {mib} MiB put {put_took:?} get {get_took:?} intact {}",
                found == value
            ),
            Ok(None) => println!("MEASURE {mib} MiB put {put_took:?} get found nothing"),
            Err(error) => println!("MEASURE {mib} MiB put {put_took:?} get failed: {error:#}"),
        }
    }
}

/// The `modules` cache; `felixd run` creates it, but a test that stores a
/// module itself has to.
async fn modules_cache(t: &common::TestFelix) {
    let kind = StreamKind {
        replication_factor: replication_factor(),
    };
    ensure_cache(&t.settings, names::MODULES, kind)
        .await
        .unwrap();
}

/// A real component of `len` bytes, more than one write to Felix holds when
/// `len` is over 8 MiB.
fn counter_of(len: usize) -> Vec<u8> {
    felixd_testkit::padded(&felixd_testkit::workload("counter"), len)
}

fn count(i: u64) -> Vec<u8> {
    i.to_be_bytes().to_vec()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_module_over_one_write_runs_and_is_checked() {
    let Some(t) = test_felix().await else { return };
    let module = counter_of(9 * MIB + 17);
    ensure_stream(
        &t.settings,
        INPUT,
        StreamKind {
            replication_factor: replication_factor(),
        },
    )
    .await
    .unwrap();
    assert!(t.run(ID, &module, INPUT, &[]).success());
    let key = module::hash(&module);
    let manifest = t.felix.cache_get(names::MODULES, &key).await.unwrap();
    assert!(
        manifest.is_some_and(|m| !m.starts_with(b"\0asm")),
        "a module this size is stored as a manifest"
    );
    for n in 0..3 {
        let chunk = module::chunk_key(&key, n);
        assert!(
            t.felix
                .cache_get(names::MODULES, &chunk)
                .await
                .unwrap()
                .is_some()
        );
    }
    let _agent = t.start_agent();
    t.publish_all(INPUT, (0..5).map(count)).await;
    assert_uninterrupted(&t, &module, 5).await;
    let (ok, report) = t.replay(&["--id", ID, "--diff"]);
    assert!(ok, "{report}");
    assert_eq!(t.outputs(OUTPUT).await.len(), 5);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_corrupt_or_missing_chunk_is_an_error_and_a_re_put_repairs_it() {
    let Some(t) = test_felix().await else { return };
    modules_cache(&t).await;
    let module = counter_of(CHUNK_BYTES * 2 + 100);
    let felix = &t.felix;
    let key = module_store::put(felix, &module).await.unwrap();
    assert_eq!(key, module::hash(&module));
    let Fetched::Found(found) = module_store::fetch(felix, &key).await.unwrap() else {
        panic!("not found");
    };
    assert_eq!(&found[..], &module[..]);

    let chunk = module::chunk_key(&key, 1);
    let mut flipped = module[CHUNK_BYTES..2 * CHUNK_BYTES].to_vec();
    flipped[5] ^= 1;
    felix
        .cache_put(names::MODULES, &chunk, flipped.into())
        .await
        .unwrap();
    match module_store::fetch(felix, &key).await.unwrap() {
        Fetched::Corrupt(Corrupt::ChunkHash { n: 1, .. }) => {}
        other => panic!("{other:?}"),
    }

    felix.cache_delete(names::MODULES, &chunk).await.unwrap();
    match module_store::fetch(felix, &key).await.unwrap() {
        Fetched::Missing(what) => assert_eq!(what, format!("chunk 1 of module {key}")),
        other => panic!("{other:?}"),
    }

    // The same module put again, as `felixd run` does, writes it all back.
    assert_eq!(module_store::put(felix, &module).await.unwrap(), key);
    assert!(matches!(
        module_store::fetch(felix, &key).await.unwrap(),
        Fetched::Found(_)
    ));
    assert_eq!(module_store::put(felix, &module).await.unwrap(), key);

    // A manifest whose chunks were never written is missing them, not a panic.
    let lone = counter_of(CHUNK_BYTES + 4000);
    let lone_key = module::hash(&lone);
    let parts = module::split(&lone);
    felix
        .cache_put(names::MODULES, &lone_key, parts.head.to_vec().into())
        .await
        .unwrap();
    assert!(matches!(
        module_store::fetch(felix, &lone_key).await.unwrap(),
        Fetched::Missing(_)
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_module_over_the_limit_is_refused_before_anything_is_stored() {
    let Some(t) = test_felix().await else { return };
    let module = vec![0u8; module::MAX_MODULE_BYTES as usize + 1];
    let error = module_store::put(&t.felix, &module).await.unwrap_err();
    assert_eq!(
        error.to_string(),
        "the module is 67108865 bytes, not 1 to 67108864"
    );
}

/// Times `module_store::put` and `fetch` of modules of several sizes, which
/// are chunked above 4 MiB. Skipped unless `FELIXD_MEASURE_MODULES` is set.
#[tokio::test(flavor = "multi_thread")]
async fn measure_stored_modules() {
    if std::env::var_os("FELIXD_MEASURE_MODULES").is_none() {
        eprintln!("skipping: FELIXD_MEASURE_MODULES is not set");
        return;
    }
    let Some(t) = test_felix().await else { return };
    modules_cache(&t).await;
    for (n, mib) in [1, 5, 20, 64].into_iter().enumerate() {
        let mut module = noise(mib * MIB, 10 + n as u8).to_vec();
        module[..4].copy_from_slice(b"\0asm");
        let started = Instant::now();
        let key = module_store::put(&t.felix, &module).await.unwrap();
        let put_took = started.elapsed();
        let started = Instant::now();
        let fetched = module_store::fetch(&t.felix, &key).await.unwrap();
        let get_took = started.elapsed();
        let intact = matches!(&fetched, Fetched::Found(found) if found[..] == module[..]);
        println!("MEASURE module {mib} MiB put {put_took:?} fetch {get_took:?} intact {intact}");
    }
}
