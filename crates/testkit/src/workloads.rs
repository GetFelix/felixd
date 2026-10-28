use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Mutex;

use wit_component::ComponentEncoder;

/// Serialises builds: tests in one process ask for workloads in parallel and
/// cargo would otherwise queue them on its own lock anyway.
static BUILD: Mutex<()> = Mutex::new(());

/// Builds the example workload `name` (a directory under `workloads/`) as a
/// release component and returns its bytes.
///
/// Panics if the build fails, since a test cannot continue without it.
pub fn workload(name: &str) -> Vec<u8> {
    let _guard = BUILD.lock().unwrap_or_else(|e| e.into_inner());
    let root = repo_root();
    let target = target_dir(&root).join("workloads");
    let status = Command::new(env!("CARGO"))
        .current_dir(root.join("workloads"))
        .args([
            "build",
            "--release",
            "--quiet",
            "--target",
            "wasm32-unknown-unknown",
        ])
        .arg("--target-dir")
        .arg(&target)
        .args(["--package", name])
        .status()
        .expect("run cargo to build the workload");
    assert!(status.success(), "building workload {name} failed");
    let module = target
        .join("wasm32-unknown-unknown/release")
        .join(format!("{name}.wasm"));
    let core = std::fs::read(&module).unwrap_or_else(|e| panic!("read {}: {e}", module.display()));
    ComponentEncoder::default()
        .module(&core)
        .expect("a workload module")
        .validate(true)
        .encode()
        .expect("wrap the module as a component")
}

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("repo root exists")
}

fn target_dir(root: &Path) -> PathBuf {
    std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| root.join("target"))
}
