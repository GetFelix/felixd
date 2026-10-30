//! Admission under a stricter policy: at most the default fuel per call. It
//! exists so the tests and the docs can show `felixd replay --diff --module`
//! reporting which recorded requests a policy change decides differently.

use felixd_core::admission::Policy;
use felixd_core::spec::Limits;

fn policy() -> Policy {
    Policy {
        max_fuel_per_call: Limits::default().fuel_per_call,
        ..Policy::default()
    }
}

include!("../../admission/src/component.rs");
