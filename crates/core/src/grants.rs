//! The Felix grants each felixd role holds in a namespace, which `felixd init`
//! writes as RBAC policies. docs/design.md ("Grants") has the same table.
//!
//! Felix grants a stream by its name or every stream of a namespace, and
//! allows only. Nodes write every workload's streams, so they hold every
//! stream of the workload namespace, and the streams no node may write live
//! in the system namespace ([`names::system_namespace`]).

use serde::{Deserialize, Serialize};

use crate::names;

/// A felixd principal's role, assigned to principals by an operator.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    /// `felixd agent`: runs workloads.
    Node,
    /// Admission's credential, held by a node that runs admission: the only
    /// writer of `sys.specs` and the only one that creates workload streams.
    Admission,
    /// `felixd run` through admission.
    Submitter,
}

impl Role {
    pub const ALL: [Role; 3] = [Role::Node, Role::Admission, Role::Submitter];

    fn name(self) -> &'static str {
        match self {
            Role::Node => "node",
            Role::Admission => "admission",
            Role::Submitter => "submitter",
        }
    }

    /// The RBAC subject of this role in `namespace`. Roles are per
    /// namespace, so a role in one grants nothing in another.
    pub fn subject(self, namespace: &str) -> String {
        format!("role:felixd.{namespace}.{}", self.name())
    }
}

/// One Felix RBAC rule, as the control plane's policy API takes it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Policy {
    pub subject: String,
    pub object: String,
    pub action: String,
}

/// Every rule of every role in `namespace` of `tenant`.
pub fn policies(tenant: &str, namespace: &str) -> Vec<Policy> {
    Role::ALL
        .into_iter()
        .flat_map(|role| role_policies(role, tenant, namespace))
        .collect()
}

/// The rules of `role` in `namespace` of `tenant`.
pub fn role_policies(role: Role, tenant: &str, namespace: &str) -> Vec<Policy> {
    let sys = names::system_namespace(namespace);
    let stream = |ns: &str, name: &str| format!("stream:{tenant}/{ns}/{name}");
    let cache = |name: &str| format!("cache:{tenant}/{namespace}/{name}");
    let decisions = names::output_stream(names::ADMISSION);
    let rules: Vec<(&str, String)> = match role {
        // Every workload stream, admission's decisions included, until
        // per-lease tokens narrow a node to what it holds (#74).
        Role::Node => vec![
            ("stream.subscribe", stream(namespace, "*")),
            ("stream.publish", stream(namespace, "*")),
            ("stream.subscribe", stream(&sys, "*")),
            ("stream.publish", stream(&sys, names::FACTS)),
            ("cache.read", cache("*")),
            ("cache.write", cache(names::LEASES)),
            ("cache.write", cache(names::EPOCHS)),
            ("cache.write", cache(names::NODES)),
            // To mark a workload failed.
            ("cache.write", cache(names::WORKLOADS)),
        ],
        Role::Admission => vec![
            ("stream.subscribe", stream(namespace, &decisions)),
            ("stream.subscribe", stream(&sys, names::SPECS)),
            ("stream.publish", stream(&sys, names::SPECS)),
            ("cache.read", cache(names::MODULES)),
            ("cache.read", cache(names::WORKLOADS)),
            ("cache.write", cache(names::WORKLOADS)),
            // Creating an accepted workload's streams, never a system one. Felix has
            // no create-only action, so it also covers deleting them.
            ("stream.manage", stream(namespace, "*")),
        ],
        Role::Submitter => vec![
            ("stream.publish", stream(&sys, names::INTAKE)),
            ("stream.subscribe", stream(&sys, names::SPECS)),
            ("stream.subscribe", stream(namespace, &decisions)),
            ("cache.write", cache(names::MODULES)),
            // Whether admission runs.
            ("cache.read", cache(names::WORKLOADS)),
        ],
    };
    let subject = role.subject(namespace);
    rules
        .into_iter()
        .map(|(action, object)| Policy {
            subject: subject.clone(),
            object,
            action: action.to_owned(),
        })
        .collect()
}

/// One role assignment, as the control plane's grouping API has it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Grouping {
    pub user: String,
    pub role: String,
}

/// Whether `subject` is a felixd role of `namespace`, a known one or not.
/// Role names have no dot, so `role:felixd.a.b.node` is a role of namespace
/// `a.b`, not an unknown role of `a`.
fn is_felixd_subject(namespace: &str, subject: &str) -> bool {
    subject
        .strip_prefix(&format!("role:felixd.{namespace}."))
        .is_some_and(|name| !name.contains('.'))
}

/// Checks what the control plane holds for `namespace` against
/// [`policies`] and returns the rules still missing. Any rule under a felixd
/// role that is not in [`policies`] (a wider one, or another role) is an
/// error, and so is any grouping that makes a felixd role inherit another
/// role, or gives one principal two of the roles: the roles' separation
/// holds only if nothing else adds to them.
pub fn audit(
    tenant: &str,
    namespace: &str,
    held: &[Policy],
    groupings: &[Grouping],
) -> Result<Vec<Policy>, String> {
    let wanted = policies(tenant, namespace);
    for policy in held {
        if is_felixd_subject(namespace, &policy.subject) && !wanted.contains(policy) {
            return Err(format!(
                "{} holds {} on {}, which felixd does not grant it",
                policy.subject, policy.action, policy.object
            ));
        }
    }
    let roles: Vec<String> = Role::ALL.iter().map(|r| r.subject(namespace)).collect();
    for grouping in groupings {
        if is_felixd_subject(namespace, &grouping.user) {
            return Err(format!(
                "{} is assigned to {}; a felixd role must not inherit another",
                grouping.user, grouping.role
            ));
        }
        if is_felixd_subject(namespace, &grouping.role) && !roles.contains(&grouping.role) {
            return Err(format!(
                "{} is assigned to {}, which is not a felixd role",
                grouping.user, grouping.role
            ));
        }
    }
    for (i, grouping) in groupings.iter().enumerate() {
        let felixd = |g: &Grouping| roles.contains(&g.role);
        if felixd(grouping)
            && groupings[..i]
                .iter()
                .any(|other| other.user == grouping.user && felixd(other) && other != grouping)
        {
            return Err(format!(
                "{} holds more than one felixd role in {namespace}; node and admission must be different principals",
                grouping.user
            ));
        }
    }
    Ok(wanted.into_iter().filter(|p| !held.contains(p)).collect())
}

#[cfg(test)]
mod tests;
