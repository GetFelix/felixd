use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Mutex;

use wit_component::ComponentEncoder;

/// Serialises builds: tests in one process ask for workloads in parallel and
/// cargo would otherwise queue them on its own lock anyway. A file lock does
/// the same across test binaries, since the variants of one workload are
/// built to one path.
static BUILD: Mutex<()> = Mutex::new(());

/// Builds the example workload `name` (a directory under `workloads/`) as a
/// release component and returns its bytes.
///
/// Panics if the build fails, since a test cannot continue without it.
pub fn workload(name: &str) -> Vec<u8> {
    workload_with(name, &[])
}

/// As [`workload`], built with the cargo `features` of the workload's crate,
/// which is how one source gives the variants of an upgrade test.
pub fn workload_with(name: &str, features: &[&str]) -> Vec<u8> {
    let _guard = BUILD.lock().unwrap_or_else(|e| e.into_inner());
    let root = repo_root();
    let target = target_dir(&root).join("workloads");
    std::fs::create_dir_all(&target).expect("create the workloads target directory");
    let lock = File::create(target.join(".build-lock")).expect("open the build lock");
    lock.lock().expect("lock the workloads build");
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
        .args(features.iter().flat_map(|f| ["--features", f]))
        .status()
        .expect("run cargo to build the workload");
    assert!(status.success(), "building workload {name} failed");
    let module = target
        .join("wasm32-unknown-unknown/release")
        // Cargo names the artifact after the crate, `-` turned to `_`.
        .join(format!("{}.wasm", name.replace('-', "_")));
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
