use super::*;

fn component(wat: &str) -> Vec<u8> {
    wat::parse_str(wat).unwrap()
}

fn load_err(wat: &str) -> LoadError {
    match Runtime::new(Limits::default())
        .unwrap()
        .load(&component(wat))
    {
        Ok(_) => panic!("expected the load to fail"),
        Err(e) => e,
    }
}

#[test]
fn a_clock_import_is_rejected() {
    let err = load_err(
        r#"(component
            (import "wasi:clocks/monotonic-clock@0.2.0" (instance
                (export "now" (func (result u64))))))"#,
    );
    assert!(
        matches!(&err, LoadError::Imports(names) if names == &["wasi:clocks/monotonic-clock@0.2.0"]),
        "{err}"
    );
}

#[test]
fn a_randomness_import_is_rejected() {
    let err = load_err(
        r#"(component
            (import "wasi:random/random@0.2.0" (instance
                (export "get-random-u64" (func (result u64))))))"#,
    );
    assert!(matches!(err, LoadError::Imports(_)), "{err}");
}

#[test]
fn bytes_that_are_not_a_component_are_rejected() {
    let runtime = Runtime::new(Limits::default()).unwrap();
    assert!(matches!(
        runtime.load(b"not wasm"),
        Err(LoadError::Invalid(_))
    ));
}

#[test]
fn a_component_without_the_workload_exports_is_rejected() {
    let err = load_err("(component)");
    assert!(matches!(err, LoadError::Instantiate(_)), "{err}");
}

#[test]
fn nan_results_are_canonical() {
    let runtime = Runtime::new(Limits::default()).unwrap();
    let module = wasmtime::Module::new(
        &runtime.engine,
        component(
            r#"(module (func (export "nan") (result i32)
            f32.const nan:0x400001
            f32.const 0
            f32.add
            i32.reinterpret_f32))"#,
        ),
    )
    .unwrap();
    let mut store = Store::new(&runtime.engine, ());
    store.set_fuel(1_000).unwrap();
    let instance = wasmtime::Instance::new(&mut store, &module, &[]).unwrap();
    let nan = instance
        .get_typed_func::<(), i32>(&mut store, "nan")
        .unwrap()
        .call(&mut store, ())
        .unwrap();
    assert_eq!(nan as u32, 0x7fc0_0000);
}
