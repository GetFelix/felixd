//! Error classes. Every error felixd records has one, and a program decides
//! by the class and, for an error that needs an operator, by its reason. The
//! text that goes with them is detail for people.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::DecodeError;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ErrorClass {
    /// Retried while the lease is held, such as a broker that is down.
    Transient,
    /// No node of this build can run the workload until an operator acts,
    /// and it is not the workload's fault.
    NeedsOperator,
    /// The workload deterministically fails here, so every replay would too.
    Poison,
}

impl ErrorClass {
    pub(crate) fn code(self) -> u8 {
        match self {
            ErrorClass::Transient => 0,
            ErrorClass::NeedsOperator => 1,
            ErrorClass::Poison => 2,
        }
    }

    pub(crate) fn from_code(code: u8) -> Result<ErrorClass, DecodeError> {
        match code {
            0 => Ok(ErrorClass::Transient),
            1 => Ok(ErrorClass::NeedsOperator),
            2 => Ok(ErrorClass::Poison),
            other => Err(DecodeError::UnknownClass(other)),
        }
    }
}

impl fmt::Display for ErrorClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            ErrorClass::Transient => "transient",
            ErrorClass::NeedsOperator => "needs operator",
            ErrorClass::Poison => "poison",
        })
    }
}

/// What an operator has to fix. A needs-operator record is written once per
/// workload, spec record and reason.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum OperatorReason {
    /// This build refuses the module, such as one built against contract 0.1.
    ModuleRefused,
    /// The module is not in the `modules` cache.
    ModuleMissing,
    /// The spec record is missing or unreadable, or says something other
    /// than the `workloads` entry.
    SpecUnrecorded,
    /// The spec's module or input differ from those the output stream was
    /// written under.
    SpecConflict,
    /// A later spec record has already been claimed, or recorded as needing
    /// an operator. Only a node with a stale
    /// view hits this, and its watch restarts it, so it is never recorded.
    SpecStale,
    /// Restoring the snapshot ran out of `snapshot_fuel`.
    RestoreFuel,
    /// The input is another workload's output stream and holds a record of a
    /// version or kind this build does not read. Upgrading the node fixes it.
    InputUnreadable,
    /// The input stream ends before the inputs the workload has already
    /// stepped, so it was deleted and created again, or lost records.
    InputBehind,
    /// Felix no longer holds a record the workload still needs, on an input
    /// or one of its own streams: retention or a trim passed it.
    HistoryTrimmed,
}

impl OperatorReason {
    pub(crate) fn code(self) -> u16 {
        match self {
            OperatorReason::ModuleRefused => 1,
            OperatorReason::ModuleMissing => 2,
            OperatorReason::SpecUnrecorded => 3,
            OperatorReason::SpecConflict => 4,
            OperatorReason::SpecStale => 5,
            OperatorReason::RestoreFuel => 6,
            OperatorReason::InputUnreadable => 7,
            OperatorReason::InputBehind => 8,
            OperatorReason::HistoryTrimmed => 9,
        }
    }

    pub(crate) fn from_code(code: u16) -> Result<OperatorReason, DecodeError> {
        Ok(match code {
            1 => OperatorReason::ModuleRefused,
            2 => OperatorReason::ModuleMissing,
            3 => OperatorReason::SpecUnrecorded,
            4 => OperatorReason::SpecConflict,
            5 => OperatorReason::SpecStale,
            6 => OperatorReason::RestoreFuel,
            7 => OperatorReason::InputUnreadable,
            8 => OperatorReason::InputBehind,
            9 => OperatorReason::HistoryTrimmed,
            other => return Err(DecodeError::UnknownReason(other)),
        })
    }
}

impl fmt::Display for OperatorReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            OperatorReason::ModuleRefused => "module refused",
            OperatorReason::ModuleMissing => "module missing",
            OperatorReason::SpecUnrecorded => "spec not recorded",
            OperatorReason::SpecConflict => "spec conflicts with the output",
            OperatorReason::SpecStale => "spec stale",
            OperatorReason::RestoreFuel => "restore out of fuel",
            OperatorReason::InputUnreadable => "input record unreadable by this build",
            OperatorReason::InputBehind => "input stream behind the workload",
            OperatorReason::HistoryTrimmed => "a record the workload needs was trimmed",
        })
    }
}

/// Why a workload failed, as the `workloads` entry holds it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Failure {
    pub class: ErrorClass,
    /// For people.
    pub detail: String,
}

#[cfg(test)]
mod tests;
