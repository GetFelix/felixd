use std::process::Command;

#[test]
fn version_flag_prints_the_package_version() {
    let out = Command::new(env!("CARGO_BIN_EXE_felixd"))
        .arg("--version")
        .output()
        .expect("run felixd");
    assert!(out.status.success());
    let text = String::from_utf8(out.stdout).unwrap();
    assert_eq!(text.trim(), format!("felixd {}", env!("CARGO_PKG_VERSION")));
}
