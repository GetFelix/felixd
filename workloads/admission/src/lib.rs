//! Admission: reads requests from `sys.intake` and outputs one decision for
//! each, accepting the spec or rejecting it with a reason code. The policy is
//! `felixd_core::admission`; this is the component around it.

use felixd_core::admission::Policy;

fn policy() -> Policy {
    Policy::default()
}

include!("component.rs");
