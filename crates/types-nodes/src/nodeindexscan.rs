//! Plan-node base vocabulary (nodes/plannodes.h), trimmed.
//!
//! src-idiomatic hosts the canonical `Plan` base in this module; the name is
//! preserved. Trimmed to the fields ports consume (`outerPlan(node)` =
//! `plan.lefttree`); cost/targetlist/qual fields arrive with the units that
//! read them.

use mcx::{alloc_in, Mcx, PgBox};
use types_core::PgResult;

/// `Plan` (nodes/plannodes.h) — the abstract base every plan-tree node embeds
/// first. The child links are context-allocated (the plan tree lives in a
/// memory context); copying a plan tree allocates, so it goes through the
/// fallible [`Plan::clone_in`] rather than a derived `Clone`.
#[derive(Debug, Default)]
pub struct Plan<'mcx> {
    /// `struct Plan *lefttree` — input plan tree (`outerPlan(node)`).
    pub lefttree: Option<PgBox<'mcx, crate::nodes::Node<'mcx>>>,
    /// `struct Plan *righttree` — `innerPlan(node)`.
    pub righttree: Option<PgBox<'mcx, crate::nodes::Node<'mcx>>>,
}

impl Plan<'_> {
    /// Deep copy of the plan subtree into `mcx` (C: `copyObject` shape).
    /// Fallible: copying allocates.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<Plan<'b>> {
        Ok(Plan {
            lefttree: match &self.lefttree {
                Some(n) => Some(alloc_in(mcx, n.clone_in(mcx)?)?),
                None => None,
            },
            righttree: match &self.righttree {
                Some(n) => Some(alloc_in(mcx, n.clone_in(mcx)?)?),
                None => None,
            },
        })
    }
}
