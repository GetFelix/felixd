//! How a module is identified, and which snapshot layouts it declares.
//!
//! A module is identified by the sha256 of its bytes, in lowercase hex.
//!
//! A module may carry a custom section named `felixd.snapshot`, UTF-8 JSON
//! `{"format": "<id>", "reads": ["<id>", ...]}`. `format` names the layout of
//! the snapshots it writes and `reads` the other layouts its `restore`
//! accepts. A module with no section writes a layout only that exact module
//! can read. A replay of another module uses this to decide whether it may
//! start from the snapshots the recorded module wrote ([`can_restore`]).

use serde::Deserialize;
use sha2::{Digest, Sha256};
use wasmparser::{Parser, Payload};

/// The custom section a module declares its snapshot layouts in.
pub const SNAPSHOT_SECTION: &str = "felixd.snapshot";

/// The longest layout id, in bytes.
pub const MAX_LAYOUT_ID_BYTES: usize = 128;
/// The most layouts a module may list in `reads`.
pub const MAX_READS: usize = 64;

/// The key a module is stored under in the `modules` cache.
pub fn hash(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// The snapshot layouts a module declares in its `felixd.snapshot` section.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Layouts {
    /// The layout of the snapshots the module writes.
    pub format: String,
    /// Other layouts its `restore` accepts. The module's own is implied.
    #[serde(default)]
    pub reads: Vec<String>,
}

/// What a replay needs to know of a module: its hash and the layouts it
/// declares, if it declares any.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ModuleInfo {
    pub hash: String,
    pub layouts: Option<Layouts>,
}

impl ModuleInfo {
    /// Reads `bytes`, a component. It does not check that the component is a
    /// workload.
    pub fn of(bytes: &[u8]) -> Result<ModuleInfo, SectionError> {
        Ok(ModuleInfo {
            hash: hash(bytes),
            layouts: layouts(bytes)?,
        })
    }
}

/// Why the `felixd.snapshot` section of a module cannot be read.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum SectionError {
    #[error("the module does not parse: {0}")]
    Module(String),
    #[error("the module has more than one {SNAPSHOT_SECTION} section")]
    Repeated,
    #[error("the {SNAPSHOT_SECTION} section is not the JSON it should be: {0}")]
    Json(String),
    #[error(
        "the {SNAPSHOT_SECTION} section has the layout id {0:?}, which is not 1 to {MAX_LAYOUT_ID_BYTES} bytes of printable ASCII without spaces"
    )]
    Id(String),
    #[error("the {SNAPSHOT_SECTION} section lists more than {MAX_READS} layouts in reads")]
    TooManyReads,
}

/// The layouts the `felixd.snapshot` section of component `bytes` declares, or
/// `None` when it has no such section. The section may sit in the component or
/// in a core module nested in it, which is where a toolchain puts it.
pub fn layouts(bytes: &[u8]) -> Result<Option<Layouts>, SectionError> {
    let mut section: Option<&[u8]> = None;
    for payload in Parser::new(0).parse_all(bytes) {
        let payload = payload.map_err(|e| SectionError::Module(e.to_string()))?;
        if let Payload::CustomSection(custom) = payload
            && custom.name() == SNAPSHOT_SECTION
            && section.replace(custom.data()).is_some()
        {
            return Err(SectionError::Repeated);
        }
    }
    section.map(parse_layouts).transpose()
}

fn parse_layouts(section: &[u8]) -> Result<Layouts, SectionError> {
    let layouts: Layouts =
        serde_json::from_slice(section).map_err(|e| SectionError::Json(e.to_string()))?;
    if layouts.reads.len() > MAX_READS {
        return Err(SectionError::TooManyReads);
    }
    for id in std::iter::once(&layouts.format).chain(&layouts.reads) {
        let printable = id.bytes().all(|b| b.is_ascii_graphic());
        if id.is_empty() || id.len() > MAX_LAYOUT_ID_BYTES || !printable {
            return Err(SectionError::Id(id.clone()));
        }
    }
    Ok(layouts)
}

/// Why a module cannot restore the snapshots another one wrote.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Unreadable {
    #[error(
        "the module the workload runs declares no snapshot layout, so only that module reads its snapshots"
    )]
    RecordedUndeclared,
    #[error("the new module declares no snapshot layout")]
    CandidateUndeclared,
    #[error(
        "the snapshots have the layout {layout:?}, which the new module neither writes ({writes:?}) nor lists in reads"
    )]
    NotListed { layout: String, writes: String },
}

/// Whether `candidate` can restore a snapshot `recorded` wrote. A module reads
/// its own snapshots. Another module reads them when it declares a layout and
/// the recorded module's layout is its own `format` or in its `reads`.
pub fn can_restore(recorded: &ModuleInfo, candidate: &ModuleInfo) -> Result<(), Unreadable> {
    if recorded.hash == candidate.hash {
        return Ok(());
    }
    let Some(written) = &recorded.layouts else {
        return Err(Unreadable::RecordedUndeclared);
    };
    let Some(reads) = &candidate.layouts else {
        return Err(Unreadable::CandidateUndeclared);
    };
    if reads.format == written.format || reads.reads.contains(&written.format) {
        return Ok(());
    }
    Err(Unreadable::NotListed {
        layout: written.format.clone(),
        writes: reads.format.clone(),
    })
}

#[cfg(test)]
mod tests;
