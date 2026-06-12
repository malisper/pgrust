//! Plan-node base vocabulary (nodes/plannodes.h), trimmed.
//!
//! src-idiomatic hosts the canonical `Plan` base in this module; the name is
//! preserved. Trimmed to the fields ports consume (`outerPlan(node)` =
//! `plan.lefttree`); cost/targetlist/qual fields arrive with the units that
//! read them.

use alloc::boxed::Box;

/// `Plan` (nodes/plannodes.h) — the abstract base every plan-tree node embeds
/// first.
#[derive(Clone, Debug, Default)]
pub struct Plan {
    /// `struct Plan *lefttree` — input plan tree (`outerPlan(node)`).
    pub lefttree: Option<Box<crate::nodes::Node>>,
    /// `struct Plan *righttree` — `innerPlan(node)`.
    pub righttree: Option<Box<crate::nodes::Node>>,
}
