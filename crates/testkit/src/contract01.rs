/// A component of workload contract 0.1: the top-level `step`, `snapshot`,
/// `restore` and `inspect` functions, with their 0.1 types. It outputs
/// nothing and its state is empty. A 0.2 host must refuse it.
pub fn contract_01_component() -> Vec<u8> {
    wat::parse_str(
        r#"(component
            (core module $m
                (memory (export "memory") 1)
                (func (export "realloc") (param i32 i32 i32 i32) (result i32) i32.const 1024)
                ;; Results point at eight zero bytes: an empty list or string.
                (func (export "step") (param i32 i32) (result i32) i32.const 2048)
                (func (export "snapshot") (result i32) i32.const 2048)
                (func (export "restore") (param i32 i32))
                (func (export "inspect") (result i32) i32.const 2048))
            (core instance $i (instantiate $m))
            (func (export "step") (param "input" (list u8)) (result (list (list u8)))
                (canon lift (core func $i "step") (memory (core memory $i "memory")) (realloc (core func $i "realloc"))))
            (func (export "snapshot") (result (list u8))
                (canon lift (core func $i "snapshot") (memory (core memory $i "memory"))))
            (func (export "restore") (param "state" (list u8))
                (canon lift (core func $i "restore") (memory (core memory $i "memory")) (realloc (core func $i "realloc"))))
            (func (export "inspect") (result string)
                (canon lift (core func $i "inspect") (memory (core memory $i "memory")))))"#,
    )
    .expect("the 0.1 fixture parses")
}
