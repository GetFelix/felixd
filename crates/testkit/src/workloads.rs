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

/// `module` with a custom section appended, so it is `len` bytes long and
/// still a valid component that runs as before. `len` must be at least a few
/// bytes more than the module.
pub fn padded(module: &[u8], len: usize) -> Vec<u8> {
    const NAME: &[u8] = b"padding";
    let mut out = module.to_vec();
    // The section's size field is a LEB128 of up to five bytes; the name has
    // its own one-byte length.
    let body = |data: usize| 1 + NAME.len() + data;
    let leb = |mut n: usize| {
        let mut bytes = Vec::new();
        loop {
            let byte = (n & 0x7f) as u8;
            n >>= 7;
            if n == 0 {
                bytes.push(byte);
                return bytes;
            }
            bytes.push(byte | 0x80);
        }
    };
    let mut data = len - module.len();
    // Shrink until the whole section, with the header it needs, fits exactly.
    loop {
        let header = 1 + leb(body(data)).len();
        if header + body(data) <= len - module.len() {
            break;
        }
        data -= 1;
    }
    let size = body(data);
    out.push(0);
    out.extend(leb(size));
    out.push(NAME.len() as u8);
    out.extend_from_slice(NAME);
    out.extend(std::iter::repeat_n(0xa5, data));
    assert_eq!(out.len(), len, "the padding does not fit");
    out
}
