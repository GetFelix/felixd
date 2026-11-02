//! Admission that lets only two principals ask. It exists so the tests and
//! the docs can show a policy that depends on the requester, and
//! `felixd replay --diff --module` listing the recorded requests from anyone
//! else.

use felixd_core::admission::Policy;

fn policy() -> Policy {
    Policy {
        requesters: &["p:alice", "p:bob"],
        ..Policy::default()
    }
}

include!("../../admission/src/component.rs");
