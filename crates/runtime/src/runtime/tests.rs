use super::*;

fn component(wat: &str) -> Vec<u8> {
    wat::parse_str(wat).unwrap()
}

fn load_err(wat: &str) -> LoadError {
    match Runtime::new()
        .unwrap()
        .load(&component(wat), Limits::default())
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
    let runtime = Runtime::new().unwrap();
    assert!(matches!(
        runtime.load(b"not wasm", Limits::default()),
        Err(LoadError::Invalid(_))
    ));
}

#[test]
fn a_component_without_the_workload_exports_is_rejected() {
    let err = load_err("(component)");
    assert!(matches!(err, LoadError::NotAWorkload(_)), "{err}");
}

#[test]
fn a_contract_01_component_is_refused_by_name() {
    let runtime = Runtime::new().unwrap();
    let err = runtime
        .compile(&felixd_testkit::contract_01_component())
        .err()
        .expect("a 0.1 component is refused");
    assert!(matches!(err, LoadError::Contract01), "{err}");
    assert_eq!(
        err.to_string(),
        "the component is built against felixd:workload@0.1.0, rebuild it against 0.2"
    );
}

#[test]
fn nan_results_are_canonical() {
    let runtime = Runtime::new().unwrap();
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

fn core_module(runtime: &Runtime, wat: &str) -> wasmtime::Module {
    wasmtime::Module::new(&runtime.engine, component(wat)).unwrap()
}

#[test]
fn a_table_past_the_element_bound_is_refused() {
    let runtime = Runtime::new().unwrap();
    let big = core_module(
        &runtime,
        &format!("(module (table {} funcref))", MAX_TABLE_ELEMENTS + 1),
    );
    let mut store = runtime.store(Limits::default());
    assert!(wasmtime::Instance::new(&mut store, &big, &[]).is_err());
    let fits = core_module(
        &runtime,
        &format!("(module (table {} funcref))", MAX_TABLE_ELEMENTS),
    );
    let mut store = runtime.store(Limits::default());
    assert!(wasmtime::Instance::new(&mut store, &fits, &[]).is_ok());
}

#[test]
fn instances_tables_and_memories_are_bounded() {
    let runtime = Runtime::new().unwrap();
    let module = core_module(&runtime, "(module (table 1 funcref) (memory 1))");
    let mut store = runtime.store(Limits::default());
    let mut made = 0;
    while wasmtime::Instance::new(&mut store, &module, &[]).is_ok() {
        made += 1;
        assert!(made <= MAX_INSTANCES, "no bound on instances");
    }
    assert_eq!(made, MAX_INSTANCES.min(MAX_TABLES).min(MAX_MEMORIES));
}
