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
    /// `T_UniqueState`.
    Unique(PgBox<'mcx, crate::nodeunique::UniqueStateData<'mcx>>),
    /// `T_SortState`.
    Sort(PgBox<'mcx, crate::nodesort::SortStateData<'mcx>>),
    /// `T_WindowAggState`.
    WindowAgg(PgBox<'mcx, crate::nodewindowagg::WindowAggState<'mcx>>),
    /// `T_TableFuncScanState`.
    TableFuncScan(PgBox<'mcx, crate::nodetablefuncscan::TableFuncScanState<'mcx>>),
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
            PlanStateNode::Unique(_) => T_UniqueState,
            PlanStateNode::Sort(_) => T_SortState,
            PlanStateNode::WindowAgg(_) => crate::nodewindowagg::T_WindowAggState,
            PlanStateNode::TableFuncScan(_) => T_TableFuncScanState,
            PlanStateNode::ValuesScan(_) => crate::nodevaluesscan::T_ValuesScanState,
            PlanStateNode::CteScan(_) => crate::nodectescan::T_CteScanState,
            PlanStateNode::NamedTuplestoreScan(_) => {
                crate::nodenamedtuplestorescan::T_NamedTuplestoreScanState
            }
            PlanStateNode::NestLoop(_) => T_NestLoopState,
            PlanStateNode::HashJoin(_) => T_HashJoinState,
            PlanStateNode::SeqScan(_) => crate::execstate_tags::T_SeqScanState,
            PlanStateNode::TidScan(_) => crate::nodes::T_TidScanState,
            PlanStateNode::BitmapHeapScan(_) => crate::execstate_tags::T_BitmapHeapScanState,
            PlanStateNode::SubqueryScan(_) => crate::nodes::T_SubqueryScanState,
            PlanStateNode::ForeignScan(_) => crate::nodes::T_ForeignScanState,
            PlanStateNode::CustomScan(_) => crate::nodes::T_CustomScanState,
            PlanStateNode::Hash(_) => T_HashState,
            PlanStateNode::ModifyTable(_) => crate::nodes::T_ModifyTableState,
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
            PlanStateNode::Unique(u) => &u.ps,
            PlanStateNode::Sort(s) => &s.ss.ps,
            PlanStateNode::WindowAgg(w) => &w.ss.ps,
            PlanStateNode::TableFuncScan(t) => &t.ss.ps,
            PlanStateNode::ValuesScan(v) => &v.ss.ps,
            PlanStateNode::CteScan(c) => &c.ss.ps,
            PlanStateNode::NamedTuplestoreScan(n) => &n.ss.ps,
            PlanStateNode::NestLoop(m) => &m.js.ps,
            PlanStateNode::HashJoin(h) => &h.js.ps,
            PlanStateNode::SeqScan(s) => &s.ss.ps,
            PlanStateNode::TidScan(t) => &t.ss.ps,
            PlanStateNode::BitmapHeapScan(b) => &b.ss.ps,
            PlanStateNode::SubqueryScan(s) => &s.ss.ps,
            PlanStateNode::ForeignScan(f) => &f.ss.ps,
            PlanStateNode::CustomScan(c) => &c.ss.ps,
            PlanStateNode::Hash(h) => &h.ps,
            PlanStateNode::ModifyTable(m) => &m.ps,
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
            PlanStateNode::Unique(u) => &mut u.ps,
            PlanStateNode::Sort(s) => &mut s.ss.ps,
            PlanStateNode::WindowAgg(w) => &mut w.ss.ps,
            PlanStateNode::TableFuncScan(t) => &mut t.ss.ps,
            PlanStateNode::ValuesScan(v) => &mut v.ss.ps,
            PlanStateNode::CteScan(c) => &mut c.ss.ps,
            PlanStateNode::NamedTuplestoreScan(n) => &mut n.ss.ps,
            PlanStateNode::NestLoop(m) => &mut m.js.ps,
            PlanStateNode::HashJoin(h) => &mut h.js.ps,
            PlanStateNode::SeqScan(s) => &mut s.ss.ps,
            PlanStateNode::TidScan(t) => &mut t.ss.ps,
            PlanStateNode::BitmapHeapScan(b) => &mut b.ss.ps,
            PlanStateNode::SubqueryScan(s) => &mut s.ss.ps,
            PlanStateNode::ForeignScan(f) => &mut f.ss.ps,
            PlanStateNode::CustomScan(c) => &mut c.ss.ps,
            PlanStateNode::Hash(h) => &mut h.ps,
            PlanStateNode::ModifyTable(m) => &mut m.ps,
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
            // `SeqScanState` begins with a `ScanState`.
            PlanStateNode::SeqScan(s) => Some(&s.ss),
            // `TidScanState` begins with a `ScanState`.
            PlanStateNode::TidScan(t) => Some(&t.ss),
            // `BitmapHeapScanState` begins with a `ScanState`.
            PlanStateNode::BitmapHeapScan(b) => Some(&b.ss),
            // `ForeignScanState` begins with a `ScanState`.
            PlanStateNode::ForeignScan(f) => Some(&f.ss),
            // `IndexScanState` begins with a `ScanState`.
            PlanStateNode::IndexScan(i) => Some(&i.ss),
            // `BitmapIndexScanState` begins with a `ScanState`.
            PlanStateNode::BitmapIndexScan(b) => Some(&b.ss),
            // `SubqueryScanState` begins with a `ScanState`.
            PlanStateNode::SubqueryScan(s) => Some(&s.ss),
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
            // nodeModifyTable's `T_ModifyTableState` variant carries a
            // `ModifyTableState`.
            PlanStateNode::ModifyTable(m) => Some(&**m),
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
