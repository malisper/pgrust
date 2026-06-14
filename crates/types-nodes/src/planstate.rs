//! The central plan-state dispatch enum (`PlanState *` in C), trimmed.
//!
//! C's `PlanState *` is a tagged pointer to any concrete `<Node>State`; the
//! owned model is this enum (the `castNode` checks become match arms).
//! Variants are added as the nodes' executor units are ported.

use mcx::PgBox;
use crate::nodes::NodeTag;

use crate::nodememoize::T_MemoizeState;
use crate::execnodes::{PlanStateData, ScanStateData, T_MaterialState};
use crate::nodeindexonlyscan::T_IndexOnlyScanState;
use crate::nodeappend::{AppendStateData, T_AppendState};
use crate::nodelimit::T_LimitState;
use crate::nodeunique::T_UniqueState;
use crate::execstate_tags::T_SortState;
use crate::nodemergeappend::T_MergeAppendState;
use crate::nodemergejoin::T_MergeJoinState;
use crate::nodeprojectset::T_ProjectSetState;
use crate::noderesult::T_ResultState;
use crate::nodesetop::T_SetOpState;
use crate::nodetablefuncscan::T_TableFuncScanState;
use crate::nodenestloop::T_NestLoopState;
use crate::nodehashjoin::{HashJoinState, T_HashJoinState};
use crate::nodehash::HashState;
use crate::execstate_tags::T_HashState;

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
    /// `T_GroupState`.
    Group(PgBox<'mcx, crate::nodegroup::GroupStateData<'mcx>>),
    /// `T_ProjectSetState`.
    ProjectSet(PgBox<'mcx, crate::nodeprojectset::ProjectSetState<'mcx>>),
    /// `T_ResultState`.
    Result(PgBox<'mcx, crate::noderesult::ResultState<'mcx>>),
    /// `T_SetOpState`.
    SetOp(PgBox<'mcx, crate::nodesetop::SetOpStateData<'mcx>>),
    /// `T_MemoizeState`.
    Memoize(PgBox<'mcx, crate::nodememoize::MemoizeScanState<'mcx>>),
    /// `T_IndexOnlyScanState`.
    IndexOnlyScan(PgBox<'mcx, crate::nodeindexonlyscan::IndexOnlyScanState<'mcx>>),
    /// `T_LimitState`.
    Limit(PgBox<'mcx, crate::nodelimit::LimitStateData<'mcx>>),
    /// `T_UniqueState`.
    Unique(PgBox<'mcx, crate::nodeunique::UniqueStateData<'mcx>>),
    /// `T_SortState`.
    Sort(PgBox<'mcx, crate::nodesort::SortStateData<'mcx>>),
    /// `T_TableFuncScanState`.
    TableFuncScan(PgBox<'mcx, crate::nodetablefuncscan::TableFuncScanState<'mcx>>),
    /// `T_ValuesScanState`.
    ValuesScan(PgBox<'mcx, crate::nodevaluesscan::ValuesScanState<'mcx>>),
    /// `T_CteScanState`.
    CteScan(PgBox<'mcx, crate::nodectescan::CteScanState<'mcx>>),
    /// `T_NestLoopState`.
    NestLoop(PgBox<'mcx, crate::nodenestloop::NestLoopStateData<'mcx>>),
    /// `T_HashJoinState`.
    HashJoin(PgBox<'mcx, HashJoinState<'mcx>>),
    /// `T_SeqScanState`.
    SeqScan(PgBox<'mcx, crate::nodeseqscan::SeqScanState<'mcx>>),
    /// `T_SubqueryScanState`.
    SubqueryScan(PgBox<'mcx, crate::execnodes::SubqueryScanState<'mcx>>),
    /// `T_ForeignScanState`.
    ForeignScan(PgBox<'mcx, crate::nodeforeigncustom::ForeignScanState<'mcx>>),
    /// `T_HashState` — the inner Hash node of a hash join.
    Hash(PgBox<'mcx, HashState<'mcx>>),
}

impl<'mcx> PlanStateNode<'mcx> {
    /// `nodeTag(node)` — the C node tag of the concrete state node.
    pub fn tag(&self) -> NodeTag {
        match self {
            PlanStateNode::Append(_) => T_AppendState,
            PlanStateNode::Material(_) => T_MaterialState,
            PlanStateNode::MergeAppend(_) => T_MergeAppendState,
            PlanStateNode::MergeJoin(_) => T_MergeJoinState,
            PlanStateNode::Group(_) => crate::nodegroup::T_GroupState,
            PlanStateNode::ProjectSet(_) => T_ProjectSetState,
            PlanStateNode::Result(_) => T_ResultState,
            PlanStateNode::SetOp(_) => T_SetOpState,
            PlanStateNode::Memoize(_) => T_MemoizeState,
            PlanStateNode::IndexOnlyScan(_) => T_IndexOnlyScanState,
            PlanStateNode::Limit(_) => T_LimitState,
            PlanStateNode::Unique(_) => T_UniqueState,
            PlanStateNode::Sort(_) => T_SortState,
            PlanStateNode::TableFuncScan(_) => T_TableFuncScanState,
            PlanStateNode::ValuesScan(_) => crate::nodevaluesscan::T_ValuesScanState,
            PlanStateNode::CteScan(_) => crate::nodectescan::T_CteScanState,
            PlanStateNode::NestLoop(_) => T_NestLoopState,
            PlanStateNode::HashJoin(_) => T_HashJoinState,
            PlanStateNode::SeqScan(_) => crate::execstate_tags::T_SeqScanState,
            PlanStateNode::SubqueryScan(_) => crate::nodes::T_SubqueryScanState,
            PlanStateNode::ForeignScan(_) => crate::nodes::T_ForeignScanState,
            PlanStateNode::Hash(_) => T_HashState,
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
            PlanStateNode::Group(g) => &g.ss.ps,
            PlanStateNode::ProjectSet(p) => &p.ps,
            PlanStateNode::Result(r) => &r.ps,
            PlanStateNode::SetOp(s) => &s.ps,
            PlanStateNode::Memoize(m) => &m.ss.ps,
            PlanStateNode::IndexOnlyScan(m) => &m.ss.ps,
            PlanStateNode::Limit(m) => &m.ps,
            PlanStateNode::Unique(u) => &u.ps,
            PlanStateNode::Sort(s) => &s.ss.ps,
            PlanStateNode::TableFuncScan(t) => &t.ss.ps,
            PlanStateNode::ValuesScan(v) => &v.ss.ps,
            PlanStateNode::CteScan(c) => &c.ss.ps,
            PlanStateNode::NestLoop(m) => &m.js.ps,
            PlanStateNode::HashJoin(h) => &h.js.ps,
            PlanStateNode::SeqScan(s) => &s.ss.ps,
            PlanStateNode::SubqueryScan(s) => &s.ss.ps,
            PlanStateNode::ForeignScan(f) => &f.ss.ps,
            PlanStateNode::Hash(h) => &h.ps,
        }
    }

    /// `&mut ((PlanState *) node)->...`.
    pub fn ps_head_mut(&mut self) -> &mut PlanStateData<'mcx> {
        match self {
            PlanStateNode::Append(a) => &mut a.ps,
            PlanStateNode::Material(m) => &mut m.ss.ps,
            PlanStateNode::MergeAppend(m) => &mut m.ps,
            PlanStateNode::MergeJoin(m) => &mut m.js.ps,
            PlanStateNode::Group(g) => &mut g.ss.ps,
            PlanStateNode::ProjectSet(p) => &mut p.ps,
            PlanStateNode::Result(r) => &mut r.ps,
            PlanStateNode::SetOp(s) => &mut s.ps,
            PlanStateNode::Memoize(m) => &mut m.ss.ps,
            PlanStateNode::IndexOnlyScan(m) => &mut m.ss.ps,
            PlanStateNode::Limit(m) => &mut m.ps,
            PlanStateNode::Unique(u) => &mut u.ps,
            PlanStateNode::Sort(s) => &mut s.ss.ps,
            PlanStateNode::TableFuncScan(t) => &mut t.ss.ps,
            PlanStateNode::ValuesScan(v) => &mut v.ss.ps,
            PlanStateNode::CteScan(c) => &mut c.ss.ps,
            PlanStateNode::NestLoop(m) => &mut m.js.ps,
            PlanStateNode::HashJoin(h) => &mut h.js.ps,
            PlanStateNode::SeqScan(s) => &mut s.ss.ps,
            PlanStateNode::SubqueryScan(s) => &mut s.ss.ps,
            PlanStateNode::ForeignScan(f) => &mut f.ss.ps,
            PlanStateNode::Hash(h) => &mut h.ps,
        }
    }

    /// `(ScanState *) node` — the embedded `ScanState` of a relation-scan-node
    /// state (`SeqScanState`, `IndexScanState`, ... — every concrete scan-node
    /// struct begins with a `ScanState`). `None` for non-scan nodes. Returns
    /// `None` for every current variant; relation-scan variants add their arm
    /// here as their executor units land.
    pub fn as_scan_state(&self) -> Option<&ScanStateData<'mcx>> {
        match self {
            // `GroupState` begins with a `ScanState`.
            PlanStateNode::Group(g) => Some(&g.ss),
            // `SeqScanState` begins with a `ScanState`.
            PlanStateNode::SeqScan(s) => Some(&s.ss),
            // `ForeignScanState` begins with a `ScanState`.
            PlanStateNode::ForeignScan(f) => Some(&f.ss),
            // `SubqueryScanState` begins with a `ScanState`.
            PlanStateNode::SubqueryScan(s) => Some(&s.ss),
            // `ValuesScanState` begins with a `ScanState`.
            PlanStateNode::ValuesScan(v) => Some(&v.ss),
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

    /// `castNode(AggState, node)` — the `AggState` an aggregate-owned
    /// `ExprState`'s `parent` points at (read by `EEOP_GROUPING_FUNC` for
    /// `aggstate->grouped_cols`). `None` until nodeAgg threads its `AggState`
    /// into this enum (the same not-yet-landed-variant gap as
    /// [`Self::as_scan_state`]); the C `castNode` asserts the tag, so a caller
    /// that reaches this for a non-Agg parent is a planner/compiler bug.
    pub fn as_agg_state(&self) -> Option<&crate::nodeagg::AggStateData<'mcx>> {
        match self {
            // nodeAgg's `T_AggState` variant lands here when it threads into
            // this enum; no current variant carries an `AggState`.
            _ => None,
        }
    }

    /// `castNode(ModifyTableState, node)` — the `ModifyTableState` a
    /// MERGE-owned `ExprState`'s `parent` points at (read by
    /// `EEOP_MERGE_SUPPORT_FUNC` for `mtstate->mt_merge_action`). `None` until
    /// nodeModifyTable threads its `ModifyTableState` into this enum.
    pub fn as_modify_table_state(&self) -> Option<&crate::modifytable::ModifyTableState<'mcx>> {
        match self {
            // nodeModifyTable's `T_ModifyTableState` variant lands here when it
            // threads into this enum.
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
