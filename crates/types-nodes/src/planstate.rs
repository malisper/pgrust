//! The central plan-state dispatch enum (`PlanState *` in C), trimmed.
//!
//! C's `PlanState *` is a tagged pointer to any concrete `<Node>State`; the
//! owned model is this enum (the `castNode` checks become match arms).
//! Variants are added as the nodes' executor units are ported.

use mcx::PgBox;
use crate::nodes::NodeTag;

use crate::execnodes::{PlanStateData, ScanStateData, T_MaterialState};
use crate::nodeindexonlyscan::T_IndexOnlyScanState;
use crate::nodeappend::{AppendStateData, T_AppendState};
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
    /// `T_AppendState`.
    Append(PgBox<'mcx, AppendStateData<'mcx>>),
    /// `T_MaterialState`.
    Material(PgBox<'mcx, crate::nodeforeigncustom::MaterialState<'mcx>>),
    /// `T_MergeAppendState`.
    MergeAppend(PgBox<'mcx, crate::nodemergeappend::MergeAppendStateData<'mcx>>),
    /// `T_MergeJoinState`.
    MergeJoin(PgBox<'mcx, crate::nodemergejoin::MergeJoinStateData<'mcx>>),
    /// `T_IndexOnlyScanState`.
    IndexOnlyScan(PgBox<'mcx, crate::nodeindexonlyscan::IndexOnlyScanState<'mcx>>),
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
    /// `T_SeqScanState`.
    SeqScan(PgBox<'mcx, crate::nodeseqscan::SeqScanState<'mcx>>),
    /// `T_ForeignScanState`.
    ForeignScan(PgBox<'mcx, crate::nodeforeigncustom::ForeignScanState<'mcx>>),
}

impl<'mcx> PlanStateNode<'mcx> {
    /// `nodeTag(node)` — the C node tag of the concrete state node.
    pub fn tag(&self) -> NodeTag {
        match self {
            PlanStateNode::Append(_) => T_AppendState,
            PlanStateNode::Material(_) => T_MaterialState,
            PlanStateNode::MergeAppend(_) => T_MergeAppendState,
            PlanStateNode::MergeJoin(_) => T_MergeJoinState,
            PlanStateNode::IndexOnlyScan(_) => T_IndexOnlyScanState,
            PlanStateNode::Limit(_) => T_LimitState,
            PlanStateNode::Sort(_) => T_SortState,
            PlanStateNode::TableFuncScan(_) => T_TableFuncScanState,
            PlanStateNode::NestLoop(_) => T_NestLoopState,
            PlanStateNode::HashJoin(_) => T_HashJoinState,
            PlanStateNode::SeqScan(_) => crate::execstate_tags::T_SeqScanState,
            PlanStateNode::ForeignScan(_) => crate::nodes::T_ForeignScanState,
        }
    }

    /// `&((PlanState *) node)->...` — the embedded `PlanState` head every
    /// `<Node>State` struct begins with.
    pub fn ps_head(&self) -> &PlanStateData<'mcx> {
        match self {
            PlanStateNode::Append(a) => &a.ps,
            PlanStateNode::Material(m) => &m.ss.ps,
            PlanStateNode::MergeAppend(m) => &m.ps,
            PlanStateNode::MergeJoin(m) => &m.js.ps,
            PlanStateNode::IndexOnlyScan(m) => &m.ss.ps,
            PlanStateNode::Limit(m) => &m.ps,
            PlanStateNode::Sort(s) => &s.ss.ps,
            PlanStateNode::TableFuncScan(t) => &t.ss.ps,
            PlanStateNode::NestLoop(m) => &m.js.ps,
            PlanStateNode::HashJoin(h) => &h.js.ps,
            PlanStateNode::SeqScan(s) => &s.ss.ps,
            PlanStateNode::ForeignScan(f) => &f.ss.ps,
        }
    }

    /// `&mut ((PlanState *) node)->...`.
    pub fn ps_head_mut(&mut self) -> &mut PlanStateData<'mcx> {
        match self {
            PlanStateNode::Append(a) => &mut a.ps,
            PlanStateNode::Material(m) => &mut m.ss.ps,
            PlanStateNode::MergeAppend(m) => &mut m.ps,
            PlanStateNode::MergeJoin(m) => &mut m.js.ps,
            PlanStateNode::IndexOnlyScan(m) => &mut m.ss.ps,
            PlanStateNode::Limit(m) => &mut m.ps,
            PlanStateNode::Sort(s) => &mut s.ss.ps,
            PlanStateNode::TableFuncScan(t) => &mut t.ss.ps,
            PlanStateNode::NestLoop(m) => &mut m.js.ps,
            PlanStateNode::HashJoin(h) => &mut h.js.ps,
            PlanStateNode::SeqScan(s) => &mut s.ss.ps,
            PlanStateNode::ForeignScan(f) => &mut f.ss.ps,
        }
    }

    /// `(ScanState *) node` — the embedded `ScanState` of a relation-scan-node
    /// state (`SeqScanState`, `IndexScanState`, ... — every concrete scan-node
    /// struct begins with a `ScanState`). `None` for non-scan nodes. Returns
    /// `None` for every current variant; relation-scan variants add their arm
    /// here as their executor units land.
    pub fn as_scan_state(&self) -> Option<&ScanStateData<'mcx>> {
        match self {
            // `SeqScanState` begins with a `ScanState`.
            PlanStateNode::SeqScan(s) => Some(&s.ss),
            // `ForeignScanState` begins with a `ScanState`.
            PlanStateNode::ForeignScan(f) => Some(&f.ss),
            // The remaining variants are join / non-relation-scan nodes (the C
            // `search_plan_tree` `default:` / join cases). Relation-scan
            // variants add their own arm here as their executor units land.
            _ => None,
        }
    }

    /// `outerPlanState(node)` (execnodes.h) — `node->lefttree`, the input plan
    /// state descended through by `Result`/`Limit`. `None` when there is none.
    pub fn outer_plan_state(&self) -> Option<&PlanStateNode<'mcx>> {
        self.ps_head().lefttree.as_deref()
    }

    /// `((AppendState *) node)->appendplans[0..as_nplans]` — the Append's input
    /// plan states. `None` until the `AppendState` variant lands.
    pub fn append_input_states(&self) -> Option<&[PgBox<'mcx, PlanStateNode<'mcx>>]> {
        match self {
            _ => None,
        }
    }

    /// `((SubqueryScanState *) node)->subplan` — the SubqueryScan's child plan
    /// state (kept separately from `lefttree`). `None` until the
    /// `SubqueryScanState` variant lands.
    pub fn subquery_subplan_state(&self) -> Option<&PlanStateNode<'mcx>> {
        match self {
            _ => None,
        }
    }
}
