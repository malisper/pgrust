//! The central plan-state dispatch enum (`PlanState *` in C), trimmed.
//!
//! C's `PlanState *` is a tagged pointer to any concrete `<Node>State`; the
//! owned model is this enum (the `castNode` checks become match arms).
//! Variants are added as the nodes' executor units are ported.

use mcx::PgBox;
use crate::nodes::NodeTag;

use crate::execnodes::{PlanStateData, T_MaterialState};
use crate::nodelimit::T_LimitState;
use crate::execstate_tags::T_SortState;
use crate::nodemergeappend::T_MergeAppendState;
use crate::nodemergejoin::T_MergeJoinState;
use crate::nodetablefuncscan::T_TableFuncScanState;
use crate::nodenestloop::T_NestLoopState;
use crate::nodehashjoin::{HashJoinState, T_HashJoinState};

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
    /// `T_LimitState`.
    Limit(PgBox<'mcx, crate::nodelimit::LimitStateData<'mcx>>),
    /// `T_SortState`.
    Sort(PgBox<'mcx, crate::nodesort::SortStateData<'mcx>>),
    /// `T_TableFuncScanState`.
    TableFuncScan(PgBox<'mcx, crate::nodetablefuncscan::TableFuncScanState<'mcx>>),
    /// `T_NestLoopState`.
    NestLoop(PgBox<'mcx, crate::nodenestloop::NestLoopStateData<'mcx>>),
    /// `T_HashJoinState`.
    HashJoin(PgBox<'mcx, HashJoinState<'mcx>>),
}

impl<'mcx> PlanStateNode<'mcx> {
    /// `nodeTag(node)` — the C node tag of the concrete state node.
    pub fn tag(&self) -> NodeTag {
        match self {
            PlanStateNode::Material(_) => T_MaterialState,
            PlanStateNode::MergeAppend(_) => T_MergeAppendState,
            PlanStateNode::MergeJoin(_) => T_MergeJoinState,
            PlanStateNode::Limit(_) => T_LimitState,
            PlanStateNode::Sort(_) => T_SortState,
            PlanStateNode::TableFuncScan(_) => T_TableFuncScanState,
            PlanStateNode::NestLoop(_) => T_NestLoopState,
            PlanStateNode::HashJoin(_) => T_HashJoinState,
        }
    }

    /// `&((PlanState *) node)->...` — the embedded `PlanState` head every
    /// `<Node>State` struct begins with.
    pub fn ps_head(&self) -> &PlanStateData<'mcx> {
        match self {
            PlanStateNode::Material(m) => &m.ss.ps,
            PlanStateNode::MergeAppend(m) => &m.ps,
            PlanStateNode::MergeJoin(m) => &m.js.ps,
            PlanStateNode::Limit(m) => &m.ps,
            PlanStateNode::Sort(s) => &s.ss.ps,
            PlanStateNode::TableFuncScan(t) => &t.ss.ps,
            PlanStateNode::NestLoop(m) => &m.js.ps,
            PlanStateNode::HashJoin(h) => &h.js.ps,
        }
    }

    /// `&mut ((PlanState *) node)->...`.
    pub fn ps_head_mut(&mut self) -> &mut PlanStateData<'mcx> {
        match self {
            PlanStateNode::Material(m) => &mut m.ss.ps,
            PlanStateNode::MergeAppend(m) => &mut m.ps,
            PlanStateNode::MergeJoin(m) => &mut m.js.ps,
            PlanStateNode::Limit(m) => &mut m.ps,
            PlanStateNode::Sort(s) => &mut s.ss.ps,
            PlanStateNode::TableFuncScan(t) => &mut t.ss.ps,
            PlanStateNode::NestLoop(m) => &mut m.js.ps,
            PlanStateNode::HashJoin(h) => &mut h.js.ps,
        }
    }
}
