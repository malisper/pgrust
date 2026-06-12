//! The central plan-node dispatch enum (nodes/nodes.h `Node` over plan nodes),
//! trimmed.
//!
//! C's `Plan *` is a tagged pointer to any concrete plan node; the owned model
//! is this enum. Variants are added as the nodes' executor units are ported.

use mcx::{Mcx, PgBox};
use types_core::PgResult;

/// `CmdType` (nodes/nodes.h).
pub type CmdType = u32;

pub const CMD_UNKNOWN: CmdType = 0;
pub const CMD_SELECT: CmdType = 1;
pub const CMD_UPDATE: CmdType = 2;
pub const CMD_INSERT: CmdType = 3;
pub const CMD_DELETE: CmdType = 4;
pub const CMD_MERGE: CmdType = 5;
pub const CMD_UTILITY: CmdType = 6;
pub const CMD_NOTHING: CmdType = 7;

/// A plan-tree node (`Plan *` in C). The `NodeTag` is the enum discriminant.
/// Carries the allocator lifetime of the context the plan tree lives in;
/// copying allocates, so it goes through the fallible [`Node::clone_in`].
#[derive(Debug)]
#[non_exhaustive]
pub enum Node<'mcx> {
    /// `T_Material`.
    Material(crate::nodeforeigncustom::Material<'mcx>),
}

impl<'mcx> Node<'mcx> {
    /// `&((Plan *) node)->...` — the embedded `Plan` base.
    pub fn plan_head(&self) -> &crate::nodeindexscan::Plan<'mcx> {
        match self {
            Node::Material(m) => &m.plan,
        }
    }

    /// `outerPlan(node)` (plannodes.h) — `node->plan.lefttree`.
    pub fn outer_plan(&self) -> Option<&Node<'mcx>> {
        self.plan_head().lefttree.as_deref()
    }

    /// Deep copy of the node (and its plan subtree) into `mcx`
    /// (C: `copyObject` shape). Fallible: copying allocates.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<Node<'b>> {
        match self {
            Node::Material(m) => Ok(Node::Material(m.clone_in(mcx)?)),
        }
    }
}

/// Keep `PgBox<Node>` cheap to talk about in field positions.
pub type NodePtr<'mcx> = PgBox<'mcx, Node<'mcx>>;
