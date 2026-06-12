//! Plan-node base vocabulary (nodes/plannodes.h), trimmed.
//!
//! src-idiomatic hosts the canonical `Plan` base in this module; the name is
//! preserved. Trimmed to the fields ports consume (`outerPlan(node)` =
//! `plan.lefttree`); cost/targetlist/qual fields arrive with the units that
//! read them.

use mcx::{alloc_in, Mcx, PgBox};
use types_core::PgResult;

/// `CUSTOMPATH_SUPPORT_BACKWARD_SCAN` (nodes/extensible.h) — custom path/scan
/// flag: supports backward scanning.
pub const CUSTOMPATH_SUPPORT_BACKWARD_SCAN: u32 = 0x0001;
/// `CUSTOMPATH_SUPPORT_MARK_RESTORE` (nodes/extensible.h) — custom path/scan
/// flag: supports mark/restore.
pub const CUSTOMPATH_SUPPORT_MARK_RESTORE: u32 = 0x0002;

/// `Plan` (nodes/plannodes.h) — the abstract base every plan-tree node embeds
/// first. The child links are context-allocated (the plan tree lives in a
/// memory context); copying a plan tree allocates, so it goes through the
/// fallible [`Plan::clone_in`] rather than a derived `Clone`.
#[derive(Debug, Default)]
pub struct Plan<'mcx> {
    /// `bool parallel_aware` — engage parallel-aware logic?
    pub parallel_aware: bool,
    /// `struct Plan *lefttree` — input plan tree (`outerPlan(node)`).
    pub lefttree: Option<PgBox<'mcx, crate::nodes::Node<'mcx>>>,
    /// `struct Plan *righttree` — `innerPlan(node)`.
    pub righttree: Option<PgBox<'mcx, crate::nodes::Node<'mcx>>>,
    /// `Bitmapset *extParam` — indices of all external `PARAM_EXEC` params
    /// affecting this plan node or its children.
    pub extParam: Option<PgBox<'mcx, crate::bitmapset::Bitmapset<'mcx>>>,
}

impl Plan<'_> {
    /// Deep copy of the plan subtree into `mcx` (C: `copyObject` shape).
    /// Fallible: copying allocates.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<Plan<'b>> {
        Ok(Plan {
            parallel_aware: self.parallel_aware,
            lefttree: match &self.lefttree {
                Some(n) => Some(alloc_in(mcx, n.clone_in(mcx)?)?),
                None => None,
            },
            righttree: match &self.righttree {
                Some(n) => Some(alloc_in(mcx, n.clone_in(mcx)?)?),
                None => None,
            },
            extParam: match &self.extParam {
                Some(b) => Some(alloc_in(mcx, b.clone_in(mcx)?)?),
                None => None,
            },
        })
    }
}
