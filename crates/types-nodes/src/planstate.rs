//! The central plan-state dispatch enum (`PlanState *` in C), trimmed.
//!
//! C's `PlanState *` is a tagged pointer to any concrete `<Node>State`; the
//! owned model is this enum (the `castNode` checks become match arms).
//! Variants are added as the nodes' executor units are ported.

use mcx::PgBox;
use crate::nodes::NodeTag;

use crate::nodememoize::T_MemoizeState;
use crate::execnodes::{PlanStateData, ScanStateData, T_MaterialState};
use crate::nodeindexonlyscan::{T_IndexOnlyScanState, T_IndexScanState};
use crate::nodeappend::{AppendStateData, T_AppendState};
use crate::nodelimit::T_LimitState;
use crate::nodelockrows::T_LockRowsState;
use crate::nodeunique::T_UniqueState;
use crate::execstate_tags::T_SortState;
use crate::nodemergeappend::T_MergeAppendState;
use crate::nodemergejoin::T_MergeJoinState;
use crate::noderecursiveunion::T_RecursiveUnionState;
use crate::nodeprojectset::T_ProjectSetState;
use crate::noderesult::T_ResultState;
use crate::nodesetop::T_SetOpState;
use crate::nodetablefuncscan::T_TableFuncScanState;
use crate::nodenestloop::T_NestLoopState;
use crate::nodehashjoin::{HashJoinState, T_HashJoinState};
use crate::nodehash::HashState;
use crate::execstate_tags::T_HashState;
use crate::aggstate_carrier::AggStateLive;
use crate::aggstate_carrier::downcast_agg_state_ref;
use crate::samplescanstate_carrier::SampleScanStateLive;

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
    /// `T_GatherState`.
    Gather(PgBox<'mcx, crate::nodegather::GatherStateData<'mcx>>),
    /// `T_GatherMergeState`.
    GatherMerge(PgBox<'mcx, crate::nodegathermerge::GatherMergeStateData<'mcx>>),
    /// `T_MergeAppendState`.
    MergeAppend(PgBox<'mcx, crate::nodemergeappend::MergeAppendStateData<'mcx>>),
    /// `T_BitmapAndState`.
    BitmapAnd(PgBox<'mcx, crate::nodebitmapand::BitmapAndState<'mcx>>),
    /// `T_BitmapOrState`.
    BitmapOr(PgBox<'mcx, crate::nodebitmapor::BitmapOrState<'mcx>>),
    /// `T_MergeJoinState`.
    MergeJoin(PgBox<'mcx, crate::nodemergejoin::MergeJoinStateData<'mcx>>),
    /// `T_RecursiveUnionState`.
    RecursiveUnion(PgBox<'mcx, crate::noderecursiveunion::RecursiveUnionStateData<'mcx>>),
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
    /// `T_IndexScanState`.
    IndexScan(PgBox<'mcx, crate::nodeindexonlyscan::IndexScanState<'mcx>>),
    /// `T_IndexOnlyScanState`.
    IndexOnlyScan(PgBox<'mcx, crate::nodeindexonlyscan::IndexOnlyScanState<'mcx>>),
    /// `T_BitmapIndexScanState`.
    BitmapIndexScan(PgBox<'mcx, crate::nodebitmapindexscan::BitmapIndexScanState<'mcx>>),
    /// `T_LimitState`.
    Limit(PgBox<'mcx, crate::nodelimit::LimitStateData<'mcx>>),
    /// `T_LockRowsState`.
    LockRows(PgBox<'mcx, crate::nodelockrows::LockRowsStateData<'mcx>>),
    /// `T_UniqueState`.
    Unique(PgBox<'mcx, crate::nodeunique::UniqueStateData<'mcx>>),
    /// `T_SortState`.
    Sort(PgBox<'mcx, crate::nodesort::SortStateData<'mcx>>),
    /// `T_IncrementalSortState`.
    IncrementalSort(PgBox<'mcx, crate::nodeincrementalsort::IncrementalSortStateData<'mcx>>),
    /// `T_WindowAggState`.
    WindowAgg(PgBox<'mcx, crate::nodewindowagg::WindowAggState<'mcx>>),
    /// `T_TableFuncScanState`.
    TableFuncScan(PgBox<'mcx, crate::nodetablefuncscan::TableFuncScanState<'mcx>>),
    /// `T_FunctionScanState`.
    FunctionScan(PgBox<'mcx, crate::nodefunctionscan::FunctionScanState<'mcx>>),
    /// `T_ValuesScanState`.
    ValuesScan(PgBox<'mcx, crate::nodevaluesscan::ValuesScanState<'mcx>>),
    /// `T_CteScanState`.
    CteScan(PgBox<'mcx, crate::nodectescan::CteScanState<'mcx>>),
    /// `T_NamedTuplestoreScanState`.
    NamedTuplestoreScan(
        PgBox<'mcx, crate::nodenamedtuplestorescan::NamedTuplestoreScanState<'mcx>>,
    ),
    /// `T_NestLoopState`.
    NestLoop(PgBox<'mcx, crate::nodenestloop::NestLoopStateData<'mcx>>),
    /// `T_HashJoinState`.
    HashJoin(PgBox<'mcx, HashJoinState<'mcx>>),
    /// `T_SeqScanState`.
    SeqScan(PgBox<'mcx, crate::nodeseqscan::SeqScanState<'mcx>>),
    /// `T_TidScanState`.
    TidScan(PgBox<'mcx, crate::nodetidscan::TidScanState<'mcx>>),
    /// `T_SampleScanState`. The concrete `SampleScanState` lives in
    /// `types-samplescan` (ABOVE this crate), so it is carried as an owned,
    /// tag-checked erased [`SampleScanStateLive`] trait object rather than a
    /// direct `PgBox<SampleScanState>` (which would be a crate cycle). See
    /// [`crate::samplescanstate_carrier`].
    SampleScan(PgBox<'mcx, dyn SampleScanStateLive<'mcx> + 'mcx>),
    /// `T_TidRangeScanState`.
    TidRangeScan(PgBox<'mcx, crate::nodetidrangescan::TidRangeScanState<'mcx>>),
    /// `T_WorkTableScanState`. The state struct lives in `types-nodes`
    /// (`nodeworktablescan`), so it is carried by value (no crate cycle).
    WorkTableScan(PgBox<'mcx, crate::nodeworktablescan::WorkTableScanStateData<'mcx>>),
    /// `T_BitmapHeapScanState`.
    BitmapHeapScan(PgBox<'mcx, crate::nodebitmapheapscan::BitmapHeapScanState<'mcx>>),
    /// `T_SubqueryScanState`.
    SubqueryScan(PgBox<'mcx, crate::execnodes::SubqueryScanState<'mcx>>),
    /// `T_ForeignScanState`.
    ForeignScan(PgBox<'mcx, crate::nodeforeigncustom::ForeignScanState<'mcx>>),
    /// `T_CustomScanState`.
    CustomScan(PgBox<'mcx, crate::nodeforeigncustom::CustomScanState<'mcx>>),
    /// `T_HashState` — the inner Hash node of a hash join.
    Hash(PgBox<'mcx, HashState<'mcx>>),
    /// `T_ModifyTableState`.
    ModifyTable(PgBox<'mcx, crate::modifytable::ModifyTableState<'mcx>>),
    /// `T_AggState`. The concrete `AggStateData` lives in
    /// `backend-executor-nodeAgg` (ABOVE this crate), so it is carried as an
    /// owned, tag-checked erased [`AggStateLive`] trait object rather than a
    /// direct `PgBox<AggStateData>` (which would be a crate cycle). See
    /// [`crate::aggstate_carrier`].
    Agg(PgBox<'mcx, dyn AggStateLive<'mcx> + 'mcx>),
}

impl<'mcx> PlanStateNode<'mcx> {
    /// `nodeTag(node)` — the C node tag of the concrete state node.
    pub fn tag(&self) -> NodeTag {
        match self {
            PlanStateNode::Append(_) => T_AppendState,
            PlanStateNode::Material(_) => T_MaterialState,
            PlanStateNode::Gather(_) => crate::nodegather::T_GatherState,
            PlanStateNode::GatherMerge(_) => crate::nodegathermerge::T_GatherMergeState,
            PlanStateNode::MergeAppend(_) => T_MergeAppendState,
            PlanStateNode::BitmapAnd(_) => crate::nodebitmapand::T_BitmapAndState,
            PlanStateNode::BitmapOr(_) => crate::nodebitmapor::T_BitmapOrState,
            PlanStateNode::MergeJoin(_) => T_MergeJoinState,
            PlanStateNode::RecursiveUnion(_) => T_RecursiveUnionState,
            PlanStateNode::Group(_) => crate::nodegroup::T_GroupState,
            PlanStateNode::ProjectSet(_) => T_ProjectSetState,
            PlanStateNode::Result(_) => T_ResultState,
            PlanStateNode::SetOp(_) => T_SetOpState,
            PlanStateNode::Memoize(_) => T_MemoizeState,
            PlanStateNode::IndexScan(_) => T_IndexScanState,
            PlanStateNode::IndexOnlyScan(_) => T_IndexOnlyScanState,
            PlanStateNode::BitmapIndexScan(_) => crate::execstate_tags::T_BitmapIndexScanState,
            PlanStateNode::Limit(_) => T_LimitState,
            PlanStateNode::LockRows(_) => T_LockRowsState,
            PlanStateNode::Unique(_) => T_UniqueState,
            PlanStateNode::Sort(_) => T_SortState,
            PlanStateNode::IncrementalSort(_) => {
                crate::execstate_tags::T_IncrementalSortState
            }
            PlanStateNode::WindowAgg(_) => crate::nodewindowagg::T_WindowAggState,
            PlanStateNode::TableFuncScan(_) => T_TableFuncScanState,
            PlanStateNode::FunctionScan(_) => crate::nodefunctionscan::T_FunctionScanState,
            PlanStateNode::ValuesScan(_) => crate::nodevaluesscan::T_ValuesScanState,
            PlanStateNode::CteScan(_) => crate::nodectescan::T_CteScanState,
            PlanStateNode::NamedTuplestoreScan(_) => {
                crate::nodenamedtuplestorescan::T_NamedTuplestoreScanState
            }
            PlanStateNode::NestLoop(_) => T_NestLoopState,
            PlanStateNode::HashJoin(_) => T_HashJoinState,
            PlanStateNode::SeqScan(_) => crate::execstate_tags::T_SeqScanState,
            PlanStateNode::TidScan(_) => crate::nodes::T_TidScanState,
            PlanStateNode::SampleScan(s) => s.tag(),
            PlanStateNode::TidRangeScan(_) => crate::nodes::T_TidRangeScanState,
            PlanStateNode::WorkTableScan(_) => crate::nodeworktablescan::T_WorkTableScanState,
            PlanStateNode::BitmapHeapScan(_) => crate::execstate_tags::T_BitmapHeapScanState,
            PlanStateNode::SubqueryScan(_) => crate::nodes::T_SubqueryScanState,
            PlanStateNode::ForeignScan(_) => crate::nodes::T_ForeignScanState,
            PlanStateNode::CustomScan(_) => crate::nodes::T_CustomScanState,
            PlanStateNode::Hash(_) => T_HashState,
            PlanStateNode::ModifyTable(_) => crate::nodes::T_ModifyTableState,
            PlanStateNode::Agg(a) => a.tag(),
        }
    }

    /// `&((PlanState *) node)->...` — the embedded `PlanState` head every
    /// `<Node>State` struct begins with.
    pub fn ps_head(&self) -> &PlanStateData<'mcx> {
        match self {
            PlanStateNode::Append(a) => &a.ps,
            PlanStateNode::Material(m) => &m.ss.ps,
            PlanStateNode::Gather(g) => &g.ps,
            PlanStateNode::GatherMerge(g) => &g.ps,
            PlanStateNode::MergeAppend(m) => &m.ps,
            PlanStateNode::BitmapAnd(b) => &b.ps,
            PlanStateNode::BitmapOr(b) => &b.ps,
            PlanStateNode::MergeJoin(m) => &m.js.ps,
            PlanStateNode::RecursiveUnion(r) => &r.ps,
            PlanStateNode::Group(g) => &g.ss.ps,
            PlanStateNode::ProjectSet(p) => &p.ps,
            PlanStateNode::Result(r) => &r.ps,
            PlanStateNode::SetOp(s) => &s.ps,
            PlanStateNode::Memoize(m) => &m.ss.ps,
            PlanStateNode::IndexScan(m) => &m.ss.ps,
            PlanStateNode::IndexOnlyScan(m) => &m.ss.ps,
            PlanStateNode::BitmapIndexScan(m) => &m.ss.ps,
            PlanStateNode::Limit(m) => &m.ps,
            PlanStateNode::LockRows(m) => &m.ps,
            PlanStateNode::Unique(u) => &u.ps,
            PlanStateNode::Sort(s) => &s.ss.ps,
            PlanStateNode::IncrementalSort(s) => &s.ss.ps,
            PlanStateNode::WindowAgg(w) => &w.ss.ps,
            PlanStateNode::TableFuncScan(t) => &t.ss.ps,
            PlanStateNode::FunctionScan(f) => &f.ss.ps,
            PlanStateNode::ValuesScan(v) => &v.ss.ps,
            PlanStateNode::CteScan(c) => &c.ss.ps,
            PlanStateNode::NamedTuplestoreScan(n) => &n.ss.ps,
            PlanStateNode::NestLoop(m) => &m.js.ps,
            PlanStateNode::HashJoin(h) => &h.js.ps,
            PlanStateNode::SeqScan(s) => &s.ss.ps,
            PlanStateNode::TidScan(t) => &t.ss.ps,
            PlanStateNode::SampleScan(s) => s.ps(),
            PlanStateNode::TidRangeScan(t) => &t.ss.ps,
            PlanStateNode::WorkTableScan(w) => &w.ss.ps,
            PlanStateNode::BitmapHeapScan(b) => &b.ss.ps,
            PlanStateNode::SubqueryScan(s) => &s.ss.ps,
            PlanStateNode::ForeignScan(f) => &f.ss.ps,
            PlanStateNode::CustomScan(c) => &c.ss.ps,
            PlanStateNode::Hash(h) => &h.ps,
            PlanStateNode::ModifyTable(m) => &m.ps,
            PlanStateNode::Agg(a) => a.ps(),
        }
    }

    /// `&mut ((PlanState *) node)->...`.
    pub fn ps_head_mut(&mut self) -> &mut PlanStateData<'mcx> {
        match self {
            PlanStateNode::Append(a) => &mut a.ps,
            PlanStateNode::Material(m) => &mut m.ss.ps,
            PlanStateNode::Gather(g) => &mut g.ps,
            PlanStateNode::GatherMerge(g) => &mut g.ps,
            PlanStateNode::MergeAppend(m) => &mut m.ps,
            PlanStateNode::BitmapAnd(b) => &mut b.ps,
            PlanStateNode::BitmapOr(b) => &mut b.ps,
            PlanStateNode::MergeJoin(m) => &mut m.js.ps,
            PlanStateNode::RecursiveUnion(r) => &mut r.ps,
            PlanStateNode::Group(g) => &mut g.ss.ps,
            PlanStateNode::ProjectSet(p) => &mut p.ps,
            PlanStateNode::Result(r) => &mut r.ps,
            PlanStateNode::SetOp(s) => &mut s.ps,
            PlanStateNode::Memoize(m) => &mut m.ss.ps,
            PlanStateNode::IndexScan(m) => &mut m.ss.ps,
            PlanStateNode::IndexOnlyScan(m) => &mut m.ss.ps,
            PlanStateNode::BitmapIndexScan(m) => &mut m.ss.ps,
            PlanStateNode::Limit(m) => &mut m.ps,
            PlanStateNode::LockRows(m) => &mut m.ps,
            PlanStateNode::Unique(u) => &mut u.ps,
            PlanStateNode::Sort(s) => &mut s.ss.ps,
            PlanStateNode::IncrementalSort(s) => &mut s.ss.ps,
            PlanStateNode::WindowAgg(w) => &mut w.ss.ps,
            PlanStateNode::TableFuncScan(t) => &mut t.ss.ps,
            PlanStateNode::FunctionScan(f) => &mut f.ss.ps,
            PlanStateNode::ValuesScan(v) => &mut v.ss.ps,
            PlanStateNode::CteScan(c) => &mut c.ss.ps,
            PlanStateNode::NamedTuplestoreScan(n) => &mut n.ss.ps,
            PlanStateNode::NestLoop(m) => &mut m.js.ps,
            PlanStateNode::HashJoin(h) => &mut h.js.ps,
            PlanStateNode::SeqScan(s) => &mut s.ss.ps,
            PlanStateNode::TidScan(t) => &mut t.ss.ps,
            PlanStateNode::SampleScan(s) => s.ps_mut(),
            PlanStateNode::TidRangeScan(t) => &mut t.ss.ps,
            PlanStateNode::WorkTableScan(w) => &mut w.ss.ps,
            PlanStateNode::BitmapHeapScan(b) => &mut b.ss.ps,
            PlanStateNode::SubqueryScan(s) => &mut s.ss.ps,
            PlanStateNode::ForeignScan(f) => &mut f.ss.ps,
            PlanStateNode::CustomScan(c) => &mut c.ss.ps,
            PlanStateNode::Hash(h) => &mut h.ps,
            PlanStateNode::ModifyTable(m) => &mut m.ps,
            PlanStateNode::Agg(a) => a.ps_mut(),
        }
    }

    /// Back-fill the non-owning `ExprState.parent` back-link on every `ExprState`
    /// this node OWNS, now that the node's enclosing [`PlanStateNode`] enum has a
    /// stable address.
    ///
    /// This is the owned-model rendering of C's `ExecInitExpr(node, parent)`
    /// contract (execExpr.c), where `parent` is the `PlanState *` cast of the
    /// already-`makeNode`-allocated, address-stable state struct. In C the cast
    /// target and the back-link identity are the *same* address, so `parent` can
    /// be set *during* the node's `ExecInit*`. In the owned tree the concrete
    /// `*State` struct and its enclosing `PlanStateNode` enum are two distinct
    /// allocations: the concrete struct is boxed first (by the node's `ExecInit*`,
    /// where the `ExecInitExpr`/`ExecInitQual` seams run with `parent: None`), and
    /// the enum wrapper — whose address the `GROUPING_FUNC`/`MERGE_SUPPORT_FUNC`/
    /// SubPlan consumers need, because `as_agg_state`/`as_modify_table_state` are
    /// enum methods — is boxed afterwards by `ExecInitNode`. So the back-link is
    /// stamped here, right after the wrap, from the now-stable `&PlanStateNode`.
    ///
    /// Covers the `ExprState`s reachable through the embedded `PlanState` head —
    /// `qual` (the node's filter, where `GROUPING()`/`MERGE_SUPPORT()` in a HAVING
    /// clause compile) and `ps_ProjInfo.pi_state` (the result projection, where
    /// targetlist `GROUPING()`/`MERGE_SUPPORT()`/SubPlan compile). These are the
    /// `parent`-consuming sites in the C executor; per-node side `ExprState`s
    /// (join quals, index quals) never reach a `parent`-reading opcode.
    pub fn stamp_expr_parents(&mut self) {
        // Compute the back-link first so the shared `&self` borrow is released
        // (`PlanStateLink` is `Copy` and erases the borrow into a raw address)
        // before taking the `&mut self` for the head; mirrors the C
        // `(PlanState *) node` pointer copy.
        let link = PlanStateLink::from_ref(&*self);
        let head = self.ps_head_mut();
        if let Some(qual) = head.qual.as_mut() {
            qual.parent = Some(link);
        }
        if let Some(proj) = head.ps_ProjInfo.as_mut() {
            proj.pi_state.parent = Some(link);
        }
    }

    /// Back-fill the `ExprState.parent` back-link on the MERGE- and
    /// RETURNING-related `ExprState`s a `ModifyTableState` owns but that live on
    /// the per-result-relation [`ResultRelInfo`](crate::execnodes::ResultRelInfo)
    /// in the `EState` (not on the node's own `PlanState` head, so
    /// [`Self::stamp_expr_parents`] does not reach them).
    ///
    /// In C every `ExecBuildProjectionInfo`/`ExecInitQual` call inside
    /// `ExecInitModifyTable`/`ExecInitMerge` passes `&mtstate->ps` as the parent,
    /// so the result-relation projections (`ri_projectReturning` — where a
    /// `merge_action()` in the RETURNING list compiles to `EEOP_MERGE_SUPPORT_FUNC`),
    /// the per-action projections/quals (`mas_proj`, `mas_whenqual`), and the MERGE
    /// join-condition qual (`ri_MergeJoinCondition`) are all linked back to the
    /// `ModifyTableState` at build time. In the owned model the enclosing
    /// `PlanStateNode::ModifyTable` enum is only address-stable after the node is
    /// boxed, so the back-link is stamped here, mirroring C's `(PlanState *) mtstate`.
    ///
    /// No-op for any non-`ModifyTable` node. `result_rel_ids` are the
    /// `ModifyTableState.resultRelInfo` ids; `estate` owns the `ResultRelInfo`s.
    pub fn stamp_modifytable_expr_parents(
        &self,
        estate: &mut crate::execnodes::EStateData<'mcx>,
        result_rel_ids: &[crate::execnodes::RriId],
    ) {
        // Only ModifyTable nodes carry these result-relation ExprStates.
        if !matches!(self, PlanStateNode::ModifyTable(_)) {
            return;
        }
        let link = PlanStateLink::from_ref(self);
        for &rri in result_rel_ids {
            stamp_result_rel_expr_parents(estate, rri, link);
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
            // `WindowAggState` begins with a `ScanState`.
            PlanStateNode::WindowAgg(w) => Some(&w.ss),
            // `AggState` begins with a `ScanState`.
            PlanStateNode::Agg(a) => Some(a.ss()),
            // `SeqScanState` begins with a `ScanState`.
            PlanStateNode::SeqScan(s) => Some(&s.ss),
            // `TidScanState` begins with a `ScanState`.
            PlanStateNode::TidScan(t) => Some(&t.ss),
            // `SampleScanState` begins with a `ScanState`.
            PlanStateNode::SampleScan(s) => Some(s.ss()),
            // `TidRangeScanState` begins with a `ScanState`.
            PlanStateNode::TidRangeScan(t) => Some(&t.ss),
            // `WorkTableScanState` begins with a `ScanState`.
            PlanStateNode::WorkTableScan(w) => Some(&w.ss),
            // `BitmapHeapScanState` begins with a `ScanState`.
            PlanStateNode::BitmapHeapScan(b) => Some(&b.ss),
            // `ForeignScanState` begins with a `ScanState`.
            PlanStateNode::ForeignScan(f) => Some(&f.ss),
            // `IndexScanState` begins with a `ScanState`.
            PlanStateNode::IndexScan(i) => Some(&i.ss),
            // `IndexOnlyScanState` begins with a `ScanState`.
            PlanStateNode::IndexOnlyScan(i) => Some(&i.ss),
            // `BitmapIndexScanState` begins with a `ScanState`.
            PlanStateNode::BitmapIndexScan(b) => Some(&b.ss),
            // `SubqueryScanState` begins with a `ScanState`.
            PlanStateNode::SubqueryScan(s) => Some(&s.ss),
            // `FunctionScanState` begins with a `ScanState`.
            PlanStateNode::FunctionScan(f) => Some(&f.ss),
            // `ValuesScanState` begins with a `ScanState`.
            PlanStateNode::ValuesScan(v) => Some(&v.ss),
            // `NamedTuplestoreScanState` begins with a `ScanState`.
            PlanStateNode::NamedTuplestoreScan(n) => Some(&n.ss),
            // `CustomScanState` begins with a `ScanState`.
            PlanStateNode::CustomScan(c) => Some(&c.ss),
            // The remaining variants are join / non-relation-scan nodes (the C
            // `search_plan_tree` `default:` / join cases). Relation-scan
            // variants add their own arm here as their executor units land.
            _ => None,
        }
    }

    /// `(IndexOnlyScanState *) node` — the concrete index-only-scan node state,
    /// or `None` if this is not an `IndexOnlyScanState`. Used by
    /// `execCurrentOf` to read `ioss_ScanDesc->xs_heaptid` (the index-only scan
    /// may store a virtual tuple without a ctid column).
    pub fn as_index_only_scan_state(
        &self,
    ) -> Option<&crate::nodeindexonlyscan::IndexOnlyScanState<'mcx>> {
        match self {
            PlanStateNode::IndexOnlyScan(i) => Some(i),
            _ => None,
        }
    }

    /// `outerPlanState(node)` (execnodes.h) — `node->lefttree`, the input plan
    /// state descended through by `Result`/`Limit`. `None` when there is none.
    pub fn outer_plan_state(&self) -> Option<&PlanStateNode<'mcx>> {
        self.ps_head().lefttree.as_deref()
    }

    /// `((AppendState *) node)->appendplans[0..as_nplans]` — the Append's input
    /// plan states, in array order. `None` for non-Append nodes (the C cast
    /// `(AppendState *) node` only happens after the `T_AppendState` tag check).
    /// The owned `appendplans` vector carries `Option<PgBox<..>>` slots; a
    /// not-yet-initialized slot reads as absent (filtered out), mirroring the C
    /// arrays which are fully populated by `ExecInitAppend`.
    pub fn append_input_states(&self) -> Option<alloc::vec::Vec<&PlanStateNode<'mcx>>> {
        let appendplans = match self {
            PlanStateNode::Append(a) => &a.appendplans,
            _ => return None,
        };
        let mut out: alloc::vec::Vec<&PlanStateNode<'mcx>> =
            alloc::vec::Vec::with_capacity(appendplans.len());
        for c in appendplans.iter() {
            if let Some(c) = c.as_deref() {
                out.push(c);
            }
        }
        Some(out)
    }

    /// `planstate_walk_members(planstate, ...)` — the per-node member child
    /// `PlanState`s of the special member-bearing nodes
    /// (`AppendState.appendplans`, `MergeAppendState.mergeplans`,
    /// `BitmapAndState`/`BitmapOrState.bitmapplans`), in array order, for shared
    /// (`&`) read walks such as `ExplainMemberNodes`. Returns the present
    /// (`Some`) children only — the C arrays are fully populated by the node's
    /// `ExecInit*`, but the owned vectors carry `Option<PgBox<..>>` slots so a
    /// not-yet-initialized slot reads as absent. `None` for non-member nodes
    /// (distinct from `Some(empty)` for a member node with no children).
    pub fn member_input_states(&self) -> Option<alloc::vec::Vec<&PlanStateNode<'mcx>>> {
        let members = match self {
            PlanStateNode::Append(a) => &a.appendplans,
            PlanStateNode::MergeAppend(m) => &m.mergeplans,
            PlanStateNode::BitmapAnd(b) => &b.bitmapplans,
            PlanStateNode::BitmapOr(b) => &b.bitmapplans,
            _ => return None,
        };
        let mut out: alloc::vec::Vec<&PlanStateNode<'mcx>> =
            alloc::vec::Vec::with_capacity(members.len());
        for c in members.iter() {
            if let Some(c) = c.as_deref() {
                out.push(c);
            }
        }
        Some(out)
    }

    /// `castNode(AggState, node)` — the `AggState` an aggregate-owned
    /// `ExprState`'s `parent` points at (read by `EEOP_GROUPING_FUNC` for
    /// `aggstate->grouped_cols`). `None` until nodeAgg threads its `AggState`
    /// into this enum (the same not-yet-landed-variant gap as
    /// [`Self::as_scan_state`]); the C `castNode` asserts the tag, so a caller
    /// that reaches this for a non-Agg parent is a planner/compiler bug.
    ///
    /// Returns the type-erased [`AggStateLive`] trait object: the concrete
    /// `AggStateData` lives in the `backend-executor-nodeAgg` crate (which sits
    /// ABOVE `types-nodes`), so this low crate cannot name it. The consumer
    /// (execExprInterp / execExpr) recovers the concrete type with
    /// [`crate::aggstate_carrier::downcast_agg_state_ref`]. `dyn Any` is unusable
    /// here because `AggStateData<'mcx>` is not `'static`; the tag-checked
    /// carrier is the faithful rendering of C's `castNode(AggState, ...)` across
    /// the crate boundary, not a side-table/registry.
    pub fn as_agg_state(&self) -> Option<&(dyn AggStateLive<'mcx> + 'mcx)> {
        match self {
            PlanStateNode::Agg(a) => Some(&**a),
            _ => None,
        }
    }

    /// `castNode(AggState, node)` recovered as the concrete `T` (the nodeAgg
    /// `AggStateData`), tag-checked. `None` if this is not an Agg node or the
    /// tag does not match `T`.
    pub fn as_agg_state_typed<T: crate::aggstate_carrier::AggStateTagged<'mcx>>(
        &self,
    ) -> Option<&T> {
        downcast_agg_state_ref::<T>(self.as_agg_state()?)
    }

    /// `&mut` form of [`Self::as_agg_state_typed`] — recovers `&mut T` (the
    /// nodeAgg `AggStateData`) from a `PlanStateNode::Agg`, tag-checked. Used by
    /// the `ExecProcNode`/`ExecReScan`/`ExecEndNode` dispatch wrappers.
    pub fn as_agg_state_mut_typed<T: crate::aggstate_carrier::AggStateTagged<'mcx>>(
        &mut self,
    ) -> Option<&mut T> {
        match self {
            PlanStateNode::Agg(a) => {
                crate::aggstate_carrier::downcast_agg_state_mut::<T>(&mut **a)
            }
            _ => None,
        }
    }

    /// `castNode(WindowAggState, node)` — the concrete `WindowAggState` a
    /// window-function-owned `ExprState`'s `parent` points at (read by
    /// `EEOP_WINDOW_FUNC` for `winstate->funcs[funcidx]->wfuncno`). `None` for
    /// any non-WindowAgg node. The `WindowAggState` lives in `types-nodes`
    /// (every field is here), so it is carried directly by value, like the
    /// `ModifyTableState`/`FunctionScanState` carriers — no tag-checked erased
    /// recovery is needed.
    pub fn as_window_agg_state(
        &self,
    ) -> Option<&crate::nodewindowagg::WindowAggState<'mcx>> {
        match self {
            PlanStateNode::WindowAgg(w) => Some(&**w),
            _ => None,
        }
    }

    /// `castNode(ModifyTableState, node)` — the `ModifyTableState` a
    /// MERGE-owned `ExprState`'s `parent` points at (read by
    /// `EEOP_MERGE_SUPPORT_FUNC` for `mtstate->mt_merge_action`). `None` until
    /// nodeModifyTable threads its `ModifyTableState` into this enum.
    pub fn as_modify_table_state(&self) -> Option<&crate::modifytable::ModifyTableState<'mcx>> {
        match self {
            // nodeModifyTable's `T_ModifyTableState` variant carries a
            // `ModifyTableState`.
            PlanStateNode::ModifyTable(m) => Some(&**m),
            _ => None,
        }
    }

    /// `castNode(FunctionScanState, node)` — the concrete `FunctionScanState`
    /// of a `T_FunctionScanState` node, or `None` for any other node. Unlike the
    /// erased `AggState` carrier, the concrete `FunctionScanState` lives in
    /// `types-nodes` (all its fields — `SetExprState`, `Tuplestorestate`, the
    /// `ScanState` head — are already here), so it is carried directly by value
    /// (the `TableFuncScanState`/`ValuesScanState` precedent) and the downcast
    /// is a plain enum match, not a tag-checked trait-object recovery.
    pub fn as_function_scan_state(
        &self,
    ) -> Option<&crate::nodefunctionscan::FunctionScanState<'mcx>> {
        match self {
            PlanStateNode::FunctionScan(f) => Some(&**f),
            _ => None,
        }
    }

    /// `&mut` form of [`Self::as_function_scan_state`].
    pub fn as_function_scan_state_mut(
        &mut self,
    ) -> Option<&mut crate::nodefunctionscan::FunctionScanState<'mcx>> {
        match self {
            PlanStateNode::FunctionScan(f) => Some(&mut **f),
            _ => None,
        }
    }

    /// `((SubqueryScanState *) node)->subplan` (C: nodeTag==T_SubqueryScanState)
    /// or `((CteScanState *) node)->cteplanstate` (C: nodeTag==T_CteScanState) —
    /// the subquery/CTE sub-plan's `PlanState`, used by `ExecInitWholeRowVar` to
    /// reach the subplan targetlist for resjunk detection. `CteScanState` carries
    /// its sub-plan only as the 1-based `ctePlanId` identity into
    /// `es_subplanstates` (no owned child node), so a CteScan parent yields the
    /// C `default:` NULL outcome here; only a SubqueryScan exposes the child node.
    pub fn subquery_subplan_state(&self) -> Option<&PlanStateNode<'mcx>> {
        match self {
            PlanStateNode::SubqueryScan(s) => s.subplan.as_deref(),
            _ => None,
        }
    }

    /// `node->plan->parallel_aware` — whether this plan node is engineered to
    /// participate in a parallel scan (read by the parallel-executor tree walks
    /// to decide whether to invoke a node's `Exec*Estimate`/`*InitializeDSM`
    /// methods). The C reads `planstate->plan->parallel_aware`; the embedded
    /// `Plan` head is reached through the node's `PlanState.plan` back-pointer.
    pub fn parallel_aware(&self) -> bool {
        self.ps_head()
            .plan
            .map(|p| p.plan_head().parallel_aware)
            .unwrap_or(false)
    }

    /// `node->plan->plan_node_id` — the plan node's id, the key under which the
    /// parallel executor accumulates per-node instrumentation in the DSM.
    pub fn plan_node_id(&self) -> i32 {
        self.ps_head()
            .plan
            .map(|p| p.plan_head().plan_node_id)
            .unwrap_or(0)
    }

    /// `planstate_tree_walker(planstate, ...)` — the child `PlanState` nodes,
    /// in walk order, that `nodeFuncs.c`'s `planstate_tree_walker` descends
    /// into: the init-plan and regular sub-plan state trees
    /// (`planstate_walk_subplans` over `initPlan`/`subPlan`), then
    /// `outerPlanState`/`innerPlanState` (`lefttree`/`righttree`), then the
    /// per-node child-state lists (`planstate_walk_members`):
    /// `AppendState.appendplans`, `MergeAppendState.mergeplans`,
    /// `BitmapAndState`/`BitmapOrState.bitmapplans`, and
    /// `CustomScanState.custom_ps`. Returns `&mut` to each present child so the
    /// owned tree walks (e.g. the parallel-executor estimate/init walks) can
    /// recurse. Child lists for node variants whose state does not yet model its
    /// children as `PlanStateNode` (`ModifyTableState.mt_plans`,
    /// `SubqueryScanState.subplan`) are added here as those units land.
    pub fn planstate_tree_walker_children_mut<'a>(
        &'a mut self,
    ) -> alloc::vec::Vec<&'a mut PlanStateNode<'mcx>> {
        let mut out: alloc::vec::Vec<&'a mut PlanStateNode<'mcx>> = alloc::vec::Vec::new();

        // Reach the embedded `PlanState` head *and* the per-node extra
        // child-state lists from the same concrete-variant borrow: the head
        // (`.ps` / `.ss.ps` / `.js.ps`) and a member list (`appendplans`,
        // `mergeplans`, `bitmapplans`, `custom_ps`) are disjoint fields of the
        // one struct, so both can be borrowed mutably at once. A single match
        // (rather than `ps_head_mut()` followed by a second match) keeps `self`
        // borrowed exactly once for the whole result.
        let head: &'a mut PlanStateData<'mcx> = match self {
            PlanStateNode::Append(a) => {
                // Deref the `PgBox` once so the field split-borrow
                // (`appendplans` vs `ps`) is visible to the borrow checker.
                let a: &'a mut AppendStateData<'mcx> = &mut *a;
                for c in a.appendplans.iter_mut() {
                    if let Some(c) = c.as_deref_mut() {
                        out.push(c);
                    }
                }
                &mut a.ps
            }
            PlanStateNode::MergeAppend(m) => {
                let m = &mut **m;
                for c in m.mergeplans.iter_mut() {
                    if let Some(c) = c.as_deref_mut() {
                        out.push(c);
                    }
                }
                &mut m.ps
            }
            PlanStateNode::BitmapAnd(b) => {
                let b = &mut **b;
                for c in b.bitmapplans.iter_mut() {
                    if let Some(c) = c.as_deref_mut() {
                        out.push(c);
                    }
                }
                &mut b.ps
            }
            PlanStateNode::BitmapOr(b) => {
                let b = &mut **b;
                for c in b.bitmapplans.iter_mut() {
                    if let Some(c) = c.as_deref_mut() {
                        out.push(c);
                    }
                }
                &mut b.ps
            }
            PlanStateNode::CustomScan(c) => {
                let c = &mut **c;
                if let Some(list) = c.custom_ps.as_mut() {
                    for ps in list.iter_mut() {
                        out.push(&mut **ps);
                    }
                }
                &mut c.ss.ps
            }
            // Every other variant has no extra `PlanStateNode` member list (yet);
            // its children are entirely the shared-head subplans + left/right.
            other => other.ps_head_mut(),
        };

        // planstate_walk_subplans(planstate->initPlan, ...) and
        // planstate_walk_subplans(planstate->subPlan, ...) — each SubPlanState's
        // `planstate` subtree.
        if let Some(init) = head.initPlan.as_mut() {
            for sps in init.iter_mut() {
                if let Some(ps) = sps.planstate.as_deref_mut() {
                    out.push(ps);
                }
            }
        }
        if let Some(sub) = head.subPlan.as_mut() {
            for sps in sub.iter_mut() {
                if let Some(ps) = sps.planstate.as_deref_mut() {
                    out.push(ps);
                }
            }
        }
        // outerPlanState / innerPlanState.
        if let Some(l) = head.lefttree.as_deref_mut() {
            out.push(l);
        }
        if let Some(r) = head.righttree.as_deref_mut() {
            out.push(r);
        }

        out
    }
}

/// Stamp `link` (a `ModifyTableState` back-link) onto every MERGE/RETURNING
/// `ExprState` a single `ResultRelInfo` owns: `ri_projectReturning` (where a
/// `merge_action()` in RETURNING compiles to `EEOP_MERGE_SUPPORT_FUNC`),
/// `ri_MergeJoinCondition`, and every `mas_proj`/`mas_whenqual` across the
/// `ri_MergeActions` match-kind lists. Used both by the up-front
/// [`PlanStateNode::stamp_modifytable_expr_parents`] pass and by the lazy
/// per-leaf-partition init in `ExecInitPartitionInfo`, which builds these
/// `ExprState`s after that pass and so must stamp them with the same
/// `ModifyTableState` identity (mirroring C's `&mtstate->ps` parent).
pub fn stamp_result_rel_expr_parents<'mcx>(
    estate: &mut crate::execnodes::EStateData<'mcx>,
    rri: crate::execnodes::RriId,
    link: PlanStateLink,
) {
    let rel = estate.result_rel_mut(rri);
    // ri_projectReturning (RETURNING list — holds merge_action()).
    if let Some(proj) = rel.ri_projectReturning.as_mut() {
        proj.pi_state.parent = Some(link);
    }
    // ri_MergeJoinCondition (MERGE ON qual).
    if let Some(jc) = rel.ri_MergeJoinCondition.as_mut() {
        jc.parent = Some(link);
    }
    // Per-action mas_proj / mas_whenqual across every match kind.
    for actions in rel.ri_MergeActions.iter_mut() {
        if let Some(actions) = actions.as_mut() {
            for action in actions.iter_mut() {
                if let Some(proj) = action.mas_proj.as_mut() {
                    proj.pi_state.parent = Some(link);
                }
                if let Some(qual) = action.mas_whenqual.as_mut() {
                    qual.parent = Some(link);
                }
            }
        }
    }
}

/// `PlanState *` back-link — the non-owning parent back-pointer stored in
/// `ExprState.parent`.
///
/// In C, `ExprState.parent` is a bare `PlanState *`: a NON-owning uplink from a
/// compiled expression to the plan-state node that owns it (the node's quals,
/// projections, etc.). An aggregate's `ExprState`s point back at the very
/// `AggState` that owns them; a MERGE action's `ExprState`s point back at the
/// owning `ModifyTableState`. Because the `PlanState` OWNS its `ExprState`s, an
/// *owning* `PgBox<PlanStateNode>` field here would be an ownership cycle (the
/// node cannot be owned by the plan tree AND by its own expressions), which is
/// exactly what blocked an in-flight `AggState` from being its own expressions'
/// parent.
///
/// Modelled as a **lifetime-free raw back-pointer** to the owning
/// `PlanStateNode`, identical to the established [`EStateLink`] uplink
/// (`PlanState.state`) and the `mcx` child→parent / `RelAlias` raw back-pointer
/// idioms: no lifetime to infect `ExprState`/`PlanStateNode`, `Copy` (so the C
/// `elemstate->parent = state->parent` raw-pointer copy is faithful), and the
/// `&` is re-derived per access. Validity is underwritten by the invariant that
/// the owning `PlanStateNode` OUTLIVES — and, because it OWNS the `ExprState`
/// carrying this link, never moves while linked — that `ExprState`.
///
/// [`EStateLink`]: crate::execnodes::EStateLink
#[derive(Clone, Copy, Debug)]
pub struct PlanStateLink(core::ptr::NonNull<PlanStateNode<'static>>);

impl PlanStateLink {
    /// Wrap the stable address of the owning `PlanStateNode` as a back-link. The
    /// caller must guarantee the node outlives every `ExprState` carrying the
    /// link (it does: the node owns those `ExprState`s); see the type docs. The
    /// `'mcx` is erased into the raw address.
    #[inline]
    pub fn from_ref<'mcx>(parent: &PlanStateNode<'mcx>) -> Self {
        PlanStateLink(core::ptr::NonNull::from(parent).cast())
    }

    /// Wrap a non-null pointer to the owning `PlanStateNode`. The caller takes on
    /// the same liveness obligation [`Self::from_ref`] discharges.
    #[inline]
    pub fn new(p: core::ptr::NonNull<PlanStateNode<'static>>) -> Self {
        PlanStateLink(p)
    }

    /// Momentary shared read of the owning `PlanStateNode` through the back-link —
    /// the single audited deref of the raw uplink (mirrors [`EStateLink::get`]
    /// and `RelAlias::get`). Re-derives the `&` per access at the caller-chosen
    /// lifetime; never stores a stale reference. This is the owned-model
    /// rendering of C's `state->parent` dereference (`castNode(..., parent)`).
    ///
    /// [`EStateLink::get`]: crate::execnodes::EStateLink::get
    #[allow(unsafe_code)]
    #[inline]
    pub fn get<'a>(&self) -> &'a PlanStateNode<'a> {
        // Re-derive a fresh, untagged `NonNull` from the raw address so this
        // deref's provenance is current (a once-captured `&`-tag would be
        // revoked by an intervening `&mut` to the owning node); never deref the
        // stored `self.0` directly. Mirrors `EStateLink::get` exactly.
        // SAFETY: `self.0` is non-null (newtype invariant).
        let fresh =
            unsafe { core::ptr::NonNull::new_unchecked(self.0.as_ptr() as *mut PlanStateNode<'a>) };
        debug_assert_eq!(
            fresh.as_ptr() as *mut (),
            self.0.as_ptr() as *mut (),
            "owning PlanStateNode moved under PlanStateLink"
        );
        // SAFETY: the uplink is set only to the `PlanStateNode` that OWNS — and
        // therefore outlives + never moves while linked — the `ExprState`
        // carrying this link. The cross-struct reference points from the
        // shorter-lived `ExprState` to the longer-lived owning node, exactly the
        // verified parent-outlives-child invariant of `EStateLink` / the `mcx`
        // parent uplink. `fresh` is re-derived this call from the raw address
        // (not a stored stale-tag pointer), so the deref is momentary.
        unsafe { fresh.as_ref() }
    }

    /// Momentary EXCLUSIVE read of the owning `PlanStateNode` through the
    /// back-link — the `&mut` form of [`Self::get`]. The owned-model rendering of
    /// C's `castNode(AggState, state->parent)` followed by a mutation of the
    /// aggregate's per-group/per-trans state from inside the
    /// transition-evaluation interpreter (`EEOP_AGG_PLAIN_TRANS_*`): C mutates
    /// `aggstate->all_pergroups[...]` while the `aggstate`-owned `evaltrans`
    /// `ExprState` is being run, i.e. the same node is reached both through the
    /// `ExprState` being walked and through this back-link. The two access paths
    /// are disjoint (the trans steps touch only per-group/per-trans state, never
    /// the `ExprState`'s own step program), exactly as in C.
    ///
    /// SAFETY: as [`Self::get`], but the exclusive borrow is justified because
    /// the interpreter does not touch the `ExprState` for the duration of the
    /// trans-step call. The owning node outlives + never moves while linked. The
    /// `'mcx` payload lifetime is re-attached at the caller's choice
    /// (lifetime-invariant at runtime), mirroring `AggStateContextLink::get_mut`.
    #[allow(unsafe_code)]
    #[inline]
    pub fn get_mut<'a, 'mcx>(&mut self) -> &'a mut PlanStateNode<'mcx> {
        // SAFETY: `self.0` is non-null (newtype invariant); re-derive a fresh
        // pointer so provenance is current. See the method docs for the
        // exclusive-borrow justification.
        let fresh = unsafe {
            core::ptr::NonNull::new_unchecked(self.0.as_ptr() as *mut PlanStateNode<'mcx>)
        };
        unsafe { &mut *fresh.as_ptr() }
    }

    /// Raw escape hatch (the bare `PlanState *` the C executor holds), for the
    /// rare spot where tying the borrow to `&self` is too restrictive. The
    /// caller takes on the liveness obligation [`Self::get`] discharges.
    #[inline]
    pub fn as_ptr(&self) -> *mut PlanStateNode<'static> {
        self.0.as_ptr()
    }
}
