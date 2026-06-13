//! The central plan-state dispatch enum (`PlanState *` in C), trimmed.
//!
//! C's `PlanState *` is a tagged pointer to any concrete `<Node>State`; the
//! owned model is this enum (the `castNode` checks become match arms).
//! Variants are added as the nodes' executor units are ported.

use mcx::PgBox;
use crate::nodes::NodeTag;

use crate::execnodes::PlanStateData;
use crate::execnodes::T_MaterialState;
use crate::nodemergeappend::T_MergeAppendState;
use crate::nodemergejoin::T_MergeJoinState;

/// A plan-state-tree node (`PlanState *` in C). The `NodeTag` is the enum
/// discriminant. The state tree is context-allocated (C: `makeNode` in the
/// per-query context), so it carries the allocator lifetime.
#[derive(Debug)]
#[non_exhaustive]
pub enum PlanStateNode<'mcx> {
    /// `T_MaterialState`.
    Material(PgBox<'mcx, crate::nodeforeigncustom::MaterialState<'mcx>>),
    /// `T_MergeAppendState`.
    MergeAppend(PgBox<'mcx, crate::nodemergeappend::MergeAppendStateData<'mcx>>),
    /// `T_MergeJoinState`.
    MergeJoin(PgBox<'mcx, crate::nodemergejoin::MergeJoinStateData<'mcx>>),
}

impl<'mcx> PlanStateNode<'mcx> {
    /// `nodeTag(node)` — the C node tag of the concrete state node.
    pub fn tag(&self) -> NodeTag {
        match self {
            PlanStateNode::Material(_) => T_MaterialState,
            PlanStateNode::MergeAppend(_) => T_MergeAppendState,
            PlanStateNode::MergeJoin(_) => T_MergeJoinState,
        }
    }

    /// `&((PlanState *) node)->...` — the embedded `PlanState` head every
    /// `<Node>State` struct begins with.
    pub fn ps_head(&self) -> &PlanStateData<'mcx> {
        match self {
            PlanStateNode::Material(m) => &m.ss.ps,
            PlanStateNode::MergeAppend(m) => &m.ps,
            PlanStateNode::MergeJoin(m) => &m.js.ps,
        }
    }

    /// `&mut ((PlanState *) node)->...`.
    pub fn ps_head_mut(&mut self) -> &mut PlanStateData<'mcx> {
        match self {
            PlanStateNode::Material(m) => &mut m.ss.ps,
            PlanStateNode::MergeAppend(m) => &mut m.ps,
            PlanStateNode::MergeJoin(m) => &mut m.js.ps,
        }
    }
}
