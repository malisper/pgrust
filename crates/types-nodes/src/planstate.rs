//! The central plan-state dispatch enum (`PlanState *` in C), trimmed.
//!
//! C's `PlanState *` is a tagged pointer to any concrete `<Node>State`; the
//! owned model is this enum (the `castNode` checks become match arms).
//! Variants are added as the nodes' executor units are ported.

use mcx::PgBox;
use crate::nodes::NodeTag;

use crate::execnodes::PlanStateData;
use crate::execnodes::T_MaterialState;
use crate::execstate_tags::T_SetOpState;
use crate::nodegather::T_GatherState;
use crate::nodemergejoin::T_MergeJoinState;
use crate::noderesult::T_ResultState;
use crate::nodehashjoin::{HashJoinState, T_HashJoinState};

/// A plan-state-tree node (`PlanState *` in C). The `NodeTag` is the enum
/// discriminant. The state tree is context-allocated (C: `makeNode` in the
/// per-query context), so it carries the allocator lifetime.
#[derive(Debug)]
#[non_exhaustive]
pub enum PlanStateNode<'mcx> {
    /// `T_ResultState`.
    Result(PgBox<'mcx, crate::noderesult::ResultStateData<'mcx>>),
    /// `T_MaterialState`.
    Material(PgBox<'mcx, crate::nodeforeigncustom::MaterialState<'mcx>>),
    /// `T_MergeJoinState`.
    MergeJoin(PgBox<'mcx, crate::nodemergejoin::MergeJoinStateData<'mcx>>),
    /// `T_SetOpState`.
    SetOp(PgBox<'mcx, crate::nodesetop::SetOpStateData<'mcx>>),
    /// `T_GatherState`.
    Gather(PgBox<'mcx, crate::nodegather::GatherStateData<'mcx>>),
    /// `T_HashJoinState`.
    HashJoin(PgBox<'mcx, HashJoinState<'mcx>>),
}

impl<'mcx> PlanStateNode<'mcx> {
    /// `nodeTag(node)` — the C node tag of the concrete state node.
    pub fn tag(&self) -> NodeTag {
        match self {
            PlanStateNode::Result(_) => T_ResultState,
            PlanStateNode::Material(_) => T_MaterialState,
            PlanStateNode::MergeJoin(_) => T_MergeJoinState,
            PlanStateNode::SetOp(_) => T_SetOpState,
            PlanStateNode::Gather(_) => T_GatherState,
            PlanStateNode::HashJoin(_) => T_HashJoinState,
        }
    }

    /// `&((PlanState *) node)->...` — the embedded `PlanState` head every
    /// `<Node>State` struct begins with.
    pub fn ps_head(&self) -> &PlanStateData<'mcx> {
        match self {
            PlanStateNode::Result(m) => &m.ps,
            PlanStateNode::Material(m) => &m.ss.ps,
            PlanStateNode::MergeJoin(m) => &m.js.ps,
            PlanStateNode::SetOp(s) => &s.ps,
            PlanStateNode::Gather(m) => &m.ps,
            PlanStateNode::HashJoin(h) => &h.js.ps,
        }
    }

    /// `&mut ((PlanState *) node)->...`.
    pub fn ps_head_mut(&mut self) -> &mut PlanStateData<'mcx> {
        match self {
            PlanStateNode::Result(m) => &mut m.ps,
            PlanStateNode::Material(m) => &mut m.ss.ps,
            PlanStateNode::MergeJoin(m) => &mut m.js.ps,
            PlanStateNode::SetOp(s) => &mut s.ps,
            PlanStateNode::Gather(m) => &mut m.ps,
            PlanStateNode::HashJoin(h) => &mut h.js.ps,
        }
    }
}
