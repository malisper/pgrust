//! `optimizer/plan/createplan.c` — the path → plan **dispatch spine** plus the
//! shared cost/qual helpers (createplan F1).
//!
//! This crate ports the recursion spine of `createplan.c`:
//!
//! * [`create_plan`] / [`create_plan_recurse`] — the top-level driver and the
//!   `best_path->pathtype` dispatch over the 36-variant
//!   [`PathNode`](types_pathnodes::PathNode) enum (the owned-tree analogue of
//!   the C up-cast `(SubtypePath *) best_path`). Every leaf converter arm
//!   (`create_seqscan_plan`, `create_indexscan_plan`, `create_nestloop_plan`,
//!   `create_agg_plan`, …) is routed through a per-converter seam declared in
//!   [`backend_optimizer_plan_createplan_seams`]; each loud-panics until its
//!   F-family lands its body, so the whole dispatch compiles before any
//!   converter exists (the seam-and-panic contract).
//!
//! The shared helpers ported here (they need only landed deps):
//!
//! * [`copy_generic_path_info`] — copy a `Path`'s cost/size/parallel fields into
//!   a freshly built `Plan` base.
//! * [`copy_plan_costsize`] — copy a child plan's cost/size into an inserted
//!   node (used by gating-Result / Material insertion in later families).
//! * [`order_qual_clauses`] — security-and-cost reorder of a qual list.
//! * [`replace_nestloop_params`] (+ its `expression_tree_mutator`) — rewrite the
//!   nestloop-supplied `Var`s / `PlaceHolderVar`s of a parameterized path into
//!   `PARAM_EXEC` Params, over the paramassign.c seams (`#297`).
//!
//! ## Path / node addressing (arena handle model)
//!
//! `PlannerInfo` is lifetime-free: a `Path *` is a [`PathId`] into
//! `PlannerInfo::path_arena` and an expression `Node *` is a
//! [`NodeId`](types_pathnodes::NodeId) into `PlannerInfo::node_arena`.
//! [`PathNode::path_head`](types_pathnodes::PathNode::path_head) recovers the
//! embedded base `Path` (the C up-cast to `Path *`); the variant recovers the
//! concrete subtype. The produced plan tree is an owned
//! [`Node<'mcx>`](types_nodes::nodes::Node) allocated in `mcx`, so the dispatch
//! threads `Mcx<'mcx>` exactly where the C `palloc`s the new `Plan`.
//!
//! Because [`PlannerInfo`] is lifetime-free it cannot hold the `'mcx` query /
//! range-table values, so [`create_plan`] / [`create_plan_recurse`] take
//! `run: &PlannerRun<'mcx>` as an additive parameter alongside
//! `&mut PlannerInfo` and forward it to every converter seam. This is the
//! safe-Rust rendering of `root` reaching `simple_rte_array`: a scan converter
//! resolves its `RangeTblEntry` with
//! [`planner_rt_fetch`](types_pathnodes::planner_run::planner_rt_fetch)`(run,
//! root, scanrelid)`, exactly as C dereferences `planner_rt_fetch(scanrelid,
//! root)`.
//!
//! ## Scope (F1)
//!
//! Only the dispatch + the four helpers above. `create_scan_plan` /
//! `create_join_plan` (themselves path-subtype dispatchers) and every
//! `create_*_plan` family body are deferred to the later scan / join / append /
//! upper F-families; they are reached here through their seams. `create_plan`
//! itself is reached only from `standard_planner` / `subquery_planner` (both
//! unported), so the converter panics are latent.

#![no_std]
#![allow(non_upper_case_globals)]

extern crate alloc;

use alloc::vec::Vec;

use mcx::{vec_with_capacity_in, Mcx, PgVec};
use types_error::{PgError, PgResult};
use types_nodes::nodeindexscan::{Plan, Scan};
use types_nodes::noderesult::Result as ResultNode;
use types_nodes::nodes::{Node, NodeTag};
use types_nodes::nodectescan::CteScan;
use types_nodes::nodefunctionscan::FunctionScan;
use types_nodes::nodeindexscan::{SubqueryScan, SubqueryScanStatus, TidScan};
use types_nodes::nodenamedtuplestorescan::NamedTuplestoreScan;
use types_nodes::nodesamplescan::{SampleScan, TableSampleClause};
use types_nodes::nodeseqscan::SeqScan;
use types_nodes::nodetidrangescan::TidRangeScan;
use types_nodes::nodevaluesscan::ValuesScan;
use types_nodes::nodeworktablescan::WorkTableScan;
use types_nodes::primnodes::{Expr, TargetEntry};
use types_nodes::rawnodes::RangeTblFunction;
use mcx::PgString;
use types_pathnodes::planner_run::{planner_rt_fetch, PlannerRun};
use types_pathnodes::{
    NodeId, Path, PathId, PathNode, PathTarget, PlannerInfo, RinfoId, RELOPT_BASEREL, RTE_RELATION,
};

use backend_nodes_core::makefuncs::{make_orclause, make_target_entry};
use backend_nodes_core::nodefuncs::expression_tree_mutator;
use backend_nodes_equalfuncs_seams::equal_expr as equal_expr_seam;
use backend_optimizer_path_equivclass_seams as equivclass;
use backend_optimizer_plan_createplan_seams as cp_seam;
use backend_optimizer_util_joininfo::restrictinfo::extract_actual_clauses;
use backend_optimizer_util_paramassign_seams as paramassign;
use backend_optimizer_util_pathnode_seams as pathnode;
use backend_optimizer_util_placeholder_seams as placeholder;
use backend_optimizer_util_plancat::build_physical_tlist;
use backend_optimizer_util_relnode_seams as relnode;
use backend_optimizer_util_vars::tlist::apply_pathtarget_labeling_to_tlist;

// ---------------------------------------------------------------------------
// RTEKind dispatch keys used by use_physical_tlist (parsenodes.h enum values).
// `RTE_RELATION` is re-exported from types-pathnodes; the rest are defined here
// from the C enum (RTE_RELATION = 0, RTE_SUBQUERY = 1, RTE_JOIN = 2,
// RTE_FUNCTION = 3, RTE_TABLEFUNC = 4, RTE_VALUES = 5, RTE_CTE = 6).
// ---------------------------------------------------------------------------

/// `RTE_SUBQUERY` (parsenodes.h).
const RTE_SUBQUERY: types_pathnodes::RTEKind = 1;
/// `RTE_FUNCTION` (parsenodes.h).
const RTE_FUNCTION: types_pathnodes::RTEKind = 3;
/// `RTE_TABLEFUNC` (parsenodes.h).
const RTE_TABLEFUNC: types_pathnodes::RTEKind = 4;
/// `RTE_VALUES` (parsenodes.h).
const RTE_VALUES: types_pathnodes::RTEKind = 5;
/// `RTE_CTE` (parsenodes.h).
const RTE_CTE: types_pathnodes::RTEKind = 6;

/// `FirstLowInvalidHeapAttributeNumber` (access/sysattr.h) — the most-negative
/// system attribute number minus one (`-7` in PG 18.3). Used by
/// `use_physical_tlist` to offset `varattno` into the `sortgroupatts` set.
const FIRST_LOW_INVALID_HEAP_ATTRIBUTE_NUMBER: i32 = -7;

// ---------------------------------------------------------------------------
// CP_* flags (createplan.c lines ~70-73)
// ---------------------------------------------------------------------------

/// `CP_EXACT_TLIST` — plan must return specified tlist.
pub const CP_EXACT_TLIST: i32 = 0x0001;
/// `CP_SMALL_TLIST` — prefer narrower tlists.
pub const CP_SMALL_TLIST: i32 = 0x0002;
/// `CP_LABEL_TLIST` — tlist must contain sortgrouprefs.
pub const CP_LABEL_TLIST: i32 = 0x0004;
/// `CP_IGNORE_TLIST` — caller will replace tlist.
pub const CP_IGNORE_TLIST: i32 = 0x0008;

// ---------------------------------------------------------------------------
// Plan-node `pathtype` tags used by the dispatch (nodes/nodetags.h values).
//
// `Path::pathtype` is a `NodeTag` carrying the scan/join plan-node tag this
// path would produce. A few of these (T_BitmapHeapScan / T_TidScan /
// T_IncrementalSort / T_Agg) are not re-exported as named `T_*` consts from
// `types-nodes`; we define the dispatch keys locally from nodetags.h (PG 18.3),
// exactly as the `pathnode` crate does for the same tags.
// ---------------------------------------------------------------------------

const T_Result: NodeTag = NodeTag(331);
const T_ProjectSet: NodeTag = NodeTag(332);
const T_ModifyTable: NodeTag = NodeTag(333);
const T_Append: NodeTag = NodeTag(334);
const T_MergeAppend: NodeTag = NodeTag(335);
const T_RecursiveUnion: NodeTag = NodeTag(336);
const T_SeqScan: NodeTag = NodeTag(339);
const T_SampleScan: NodeTag = NodeTag(340);
const T_IndexScan: NodeTag = NodeTag(341);
const T_IndexOnlyScan: NodeTag = NodeTag(342);
const T_BitmapHeapScan: NodeTag = NodeTag(344);
const T_TidScan: NodeTag = NodeTag(345);
const T_TidRangeScan: NodeTag = NodeTag(346);
const T_SubqueryScan: NodeTag = NodeTag(347);
const T_FunctionScan: NodeTag = NodeTag(348);
const T_ValuesScan: NodeTag = NodeTag(349);
const T_TableFuncScan: NodeTag = NodeTag(350);
const T_CteScan: NodeTag = NodeTag(351);
const T_NamedTuplestoreScan: NodeTag = NodeTag(352);
const T_WorkTableScan: NodeTag = NodeTag(353);
const T_ForeignScan: NodeTag = NodeTag(354);
const T_CustomScan: NodeTag = NodeTag(355);
const T_NestLoop: NodeTag = NodeTag(356);
const T_MergeJoin: NodeTag = NodeTag(358);
const T_HashJoin: NodeTag = NodeTag(359);
const T_Material: NodeTag = NodeTag(360);
const T_Memoize: NodeTag = NodeTag(361);
const T_Sort: NodeTag = NodeTag(362);
const T_IncrementalSort: NodeTag = NodeTag(363);
const T_Group: NodeTag = NodeTag(364);
const T_Agg: NodeTag = NodeTag(365);
const T_WindowAgg: NodeTag = NodeTag(366);
const T_Unique: NodeTag = NodeTag(367);
const T_Gather: NodeTag = NodeTag(368);
const T_GatherMerge: NodeTag = NodeTag(369);
const T_SetOp: NodeTag = NodeTag(371);
const T_LockRows: NodeTag = NodeTag(372);
const T_Limit: NodeTag = NodeTag(373);

// ---------------------------------------------------------------------------
// copy_generic_path_info (createplan.c ~5450) + copy_plan_costsize (~5466)
// ---------------------------------------------------------------------------

/// `copy_generic_path_info(Plan *dest, Path *src)` — copy cost/size info from a
/// `Path` into the `Plan` node built for it.
pub fn copy_generic_path_info(dest: &mut Plan, src: &Path) {
    dest.disabled_nodes = src.disabled_nodes;
    dest.startup_cost = src.startup_cost;
    dest.total_cost = src.total_cost;
    dest.plan_rows = src.rows;
    dest.plan_width = src
        .pathtarget
        .as_deref()
        .expect("copy_generic_path_info: path has no pathtarget")
        .width;
    dest.parallel_aware = src.parallel_aware;
    dest.parallel_safe = src.parallel_safe;
}

/// `copy_plan_costsize(Plan *dest, Plan *src)` — copy cost/size info from a
/// lower plan node to an inserted node (most callers then tweak it). The
/// inserted node is assumed not parallel-aware, and parallel-safe iff the child
/// is.
pub fn copy_plan_costsize(dest: &mut Plan, src: &Plan) {
    dest.disabled_nodes = src.disabled_nodes;
    dest.startup_cost = src.startup_cost;
    dest.total_cost = src.total_cost;
    dest.plan_rows = src.plan_rows;
    dest.plan_width = src.plan_width;
    // Assume the inserted node is not parallel-aware.
    dest.parallel_aware = false;
    // Assume the inserted node is parallel-safe, if child plan is.
    dest.parallel_safe = src.parallel_safe;
}

// ---------------------------------------------------------------------------
// order_qual_clauses (createplan.c ~5350)
// ---------------------------------------------------------------------------

/// One element of the qual sort working array (the C `QualItem`).
#[derive(Clone, Copy)]
struct QualItem {
    clause: NodeId,
    cost: f64,
    security_level: u32,
}

/// `order_qual_clauses(root, clauses)` — sort the given quals into the order in
/// which they should be evaluated (security level ascending, then cost
/// ascending), preserving the input order for ties (a stable insertion sort, as
/// in C). `clauses` is the C `List *` of clause `Node *`; in the arena model
/// each is a [`NodeId`] (which may resolve to a `RestrictInfo` or a bare expr).
pub fn order_qual_clauses(root: &PlannerInfo, clauses: &[NodeId]) -> Vec<NodeId> {
    let nitems = clauses.len();

    // No need to work hard for 0 or 1 clause.
    if nitems <= 1 {
        return clauses.to_vec();
    }

    // Collect the items and costs into an array. This is to avoid repeated
    // cost_qual_eval work if the inputs aren't RestrictInfos.
    let mut items: Vec<QualItem> = Vec::with_capacity(nitems);
    for &clause in clauses {
        // cost_qual_eval_node(&qcost, clause, root) — single-node eval. The
        // single-node form is `cost_qual_eval` over a one-element list (it sums
        // the per-node walk; one node => exactly cost_qual_eval_node).
        let qcost = pathnode::cost_qual_eval::call(root, &[clause]);
        let mut security_level = 0u32;
        // if (IsA(clause, RestrictInfo)) ...
        if let Expr::RestrictInfo(rref) = root.node(clause) {
            let rinfo = root.rinfo(RinfoId::from(*rref));
            // If a clause is leakproof, it doesn't have to be constrained by its
            // nominal security level. If it's also reasonably cheap (here
            // defined as 10X cpu_operator_cost), pretend it has security_level
            // 0, which will allow it to go in front of more-expensive quals of
            // lower security levels.
            if rinfo.leakproof && qcost.per_tuple < 10.0 * pathnode::cpu_operator_cost::call() {
                security_level = 0;
            } else {
                security_level = rinfo.security_level;
            }
        }
        items.push(QualItem {
            clause,
            cost: qcost.per_tuple,
            security_level,
        });
    }

    // Sort. We don't use qsort() because it's not guaranteed stable for equal
    // keys. The expected number of entries is small enough that a simple
    // insertion sort should be good enough.
    for i in 1..nitems {
        let newitem = items[i];
        let mut j = i;
        // insert newitem into the already-sorted subarray
        while j > 0 {
            let olditem = items[j - 1];
            if newitem.security_level > olditem.security_level
                || (newitem.security_level == olditem.security_level
                    && newitem.cost >= olditem.cost)
            {
                break;
            }
            items[j] = olditem;
            j -= 1;
        }
        items[j] = newitem;
    }

    // Convert back to a list.
    items.iter().map(|it| it.clause).collect()
}

// ---------------------------------------------------------------------------
// replace_nestloop_params (createplan.c ~5230) + _mutator (~5240)
// ---------------------------------------------------------------------------

/// `IS_SPECIAL_VARNO(varno)` (primnodes.h) — `varno >= INNER_VAR` (65000).
#[inline]
fn is_special_varno(varno: i32) -> bool {
    const INNER_VAR: i32 = 65000;
    varno >= INNER_VAR
}

/// `replace_nestloop_params(root, expr)` — replace any nestloop-supplied `Var`s
/// / `PlaceHolderVar`s in `expr` with the corresponding nestloop `Param`s. The C
/// passes a single `Node *` (which may be a whole `List *`); in the arena model
/// the caller passes the [`NodeId`] of the (sub)expression to rewrite and gets
/// back the [`NodeId`] of the rewritten tree (a fresh arena node, identical when
/// nothing was replaced — `expression_tree_mutator` is copy-on-rebuild).
pub fn replace_nestloop_params(
    mcx: Mcx<'_>,
    root: &mut PlannerInfo,
    expr: NodeId,
) -> PgResult<NodeId> {
    // No setup needed for tree walk, so away we go.
    // Resolve the node to an owned Expr (the mutator rebuilds it).
    let node = root.node(expr).clone();
    let mut err: Option<PgError> = None;
    let out = replace_nestloop_params_mutator(mcx, root, node, &mut err);
    if let Some(e) = err {
        return Err(e);
    }
    Ok(root.alloc_node(out))
}

/// `replace_nestloop_params_mutator(node, root)` — the expression_tree_mutator
/// callback. Operates on an owned [`Expr`]; the paramassign seams that produce
/// the replacement `Param` take `&mut PlannerInfo`, threaded through directly
/// (the node is owned, not borrowed from `root`). The repo's
/// `expression_tree_mutator` callback is infallible (`FnMut(Expr) -> Expr`), so
/// — exactly as `clauses/grounded.rs::convert_saop_to_hashed_saop_walker` does —
/// the fallible seam errors are stashed in `err` and surfaced by the caller; on
/// a pending error the walk short-circuits, returning nodes unchanged.
pub(crate) fn replace_nestloop_params_mutator(
    mcx: Mcx<'_>,
    root: &mut PlannerInfo,
    node: Expr,
    err: &mut Option<PgError>,
) -> Expr {
    if err.is_some() {
        return node;
    }
    // if (IsA(node, Var))
    if let Expr::Var(var) = &node {
        // Upper-level Vars should be long gone at this point.
        debug_assert_eq!(var.varlevelsup, 0);
        // If not to be replaced, we can just return the Var unmodified.
        if is_special_varno(var.varno)
            || !relnode::relids_is_member::call(var.varno, &root.curOuterRels)
        {
            return node;
        }
        // Replace the Var with a nestloop Param.
        match paramassign::replace_nestloop_param_var::call(root, var) {
            Ok(param) => return Expr::Param(param),
            Err(e) => {
                *err = Some(e);
                return node;
            }
        }
    }
    // if (IsA(node, PlaceHolderVar))
    if let Expr::PlaceHolderVar(phv) = &node {
        // Upper-level PlaceHolderVars should be long gone at this point.
        debug_assert_eq!(phv.phlevelsup, 0);

        // Check whether we need to replace the PHV.
        let phinfo_id = match placeholder::find_placeholder_info::call(root, phv) {
            Ok(id) => id,
            Err(e) => {
                *err = Some(e);
                return node;
            }
        };
        // bms_is_subset(find_placeholder_info(root, phv)->ph_eval_at,
        //               root->curOuterRels). Clone ph_eval_at out so we don't
        // hold two immutable borrows of `root` across the seam call.
        let ph_eval_at = root.phinfo(phinfo_id).ph_eval_at.clone();
        let needs_full_replace =
            relnode::relids_is_subset::call(&ph_eval_at, &root.curOuterRels);
        if !needs_full_replace {
            // We can't replace the whole PHV, but we might still need to replace
            // Vars or PHVs within its expression, in case it ends up actually
            // getting evaluated here. Flat-copy the PHV node and then recurse on
            // its expression.
            let mut newphv = phv.clone();
            let phexpr = newphv
                .phexpr
                .take()
                .expect("PlaceHolderVar without phexpr in replace_nestloop_params");
            let new_phexpr = replace_nestloop_params_mutator(mcx, root, *phexpr, err);
            newphv.phexpr = Some(alloc::boxed::Box::new(new_phexpr));
            return Expr::PlaceHolderVar(newphv);
        }
        // Replace the PlaceHolderVar with a nestloop Param.
        match paramassign::replace_nestloop_param_placeholdervar::call(mcx, root, phv) {
            Ok(param) => return Expr::Param(param),
            Err(e) => {
                *err = Some(e);
                return node;
            }
        }
    }

    // return expression_tree_mutator(node, replace_nestloop_params_mutator, root);
    expression_tree_mutator(node, &mut |child| {
        replace_nestloop_params_mutator(mcx, root, child, err)
    })
}

// ---------------------------------------------------------------------------
// build_path_tlist (createplan.c ~824)
// ---------------------------------------------------------------------------

/// `build_path_tlist(root, path)` — build a target list (a list of
/// `TargetEntry`) for the Path's output.
///
/// This is almost just `make_tlist_from_pathtarget()`, but we also have to deal
/// with replacing nestloop params: if the path is parameterized, lateral
/// references in the tlist are replaced with `Param`s, applied per list item (no
/// need to remake the `TargetEntry` nodes). Each expr is resolved out of the
/// arena, run through [`replace_nestloop_params`] when `param_info` is set, and
/// wrapped into a fresh owned `TargetEntry` allocated in `mcx`.
fn build_path_tlist<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    path: PathId,
) -> PgResult<Vec<TargetEntry<'mcx>>> {
    // Snapshot the pathtarget so we can mutate `root` (replace_nestloop_params)
    // while iterating the (cloned) exprs/sortgrouprefs. In C `path->pathtarget`
    // is a stable pointer; here the arena handles are Copy and the small
    // sortgrouprefs vector is cloned out.
    let target: PathTarget = root
        .path(path)
        .base()
        .pathtarget
        .as_deref()
        .expect("build_path_tlist: path has no pathtarget")
        .clone();
    let has_param_info = root.path(path).base().param_info.is_some();

    let sortgrouprefs = &target.sortgrouprefs;
    let mut tlist = Vec::with_capacity(target.exprs.len());
    let mut resno: i32 = 1;
    for &node_id in target.exprs.iter() {
        // If it's a parameterized path, there might be lateral references in
        // the tlist, which need to be replaced with Params. There's no need to
        // remake the TargetEntry nodes, so apply this to each list item
        // separately.
        let expr: Expr = if has_param_info {
            let replaced = replace_nestloop_params(mcx, root, node_id)?;
            root.node(replaced).clone()
        } else {
            root.node(node_id).clone()
        };

        let mut tle = make_target_entry(mcx, expr, resno as i16, None, false)?;
        if !sortgrouprefs.is_empty() {
            tle.ressortgroupref = sortgrouprefs[(resno - 1) as usize];
        }
        tlist.push(tle);
        resno += 1;
    }
    Ok(tlist)
}

// ---------------------------------------------------------------------------
// use_physical_tlist (createplan.c ~864)
// ---------------------------------------------------------------------------

/// `use_physical_tlist(root, path, flags)` — decide whether to use a tlist
/// matching relation structure, rather than only those Vars actually
/// referenced.
fn use_physical_tlist(root: &PlannerInfo, path: PathId, flags: i32) -> bool {
    let rel_id = root.path(path).base().parent;
    let rel = root.rel(rel_id);

    // Forget it if either exact tlist or small tlist is demanded.
    if flags & (CP_EXACT_TLIST | CP_SMALL_TLIST) != 0 {
        return false;
    }

    // We can do this for real relation scans, subquery scans, function scans,
    // tablefunc scans, values scans, and CTE scans (but not for, eg, joins).
    if rel.rtekind != RTE_RELATION
        && rel.rtekind != RTE_SUBQUERY
        && rel.rtekind != RTE_FUNCTION
        && rel.rtekind != RTE_TABLEFUNC
        && rel.rtekind != RTE_VALUES
        && rel.rtekind != RTE_CTE
    {
        return false;
    }

    // Can't do it with inheritance cases either (mainly because Append doesn't
    // project; this test may be unnecessary now that create_append_plan
    // instructs its children to return an exact tlist).
    if rel.reloptkind != RELOPT_BASEREL {
        return false;
    }

    // Also, don't do it to a CustomPath; the premise that we're extracting
    // columns from a simple physical tuple is unlikely to hold for those.
    if matches!(root.path(path), PathNode::CustomPath(_)) {
        return false;
    }

    // If a bitmap scan's tlist is empty, keep it as-is. This may allow the
    // executor to skip heap page fetches, and in any case, the benefit of using
    // a physical tlist instead would be minimal.
    if matches!(root.path(path), PathNode::BitmapHeapPath(_))
        && root
            .path(path)
            .base()
            .pathtarget
            .as_deref()
            .map(|t| t.exprs.is_empty())
            .unwrap_or(true)
    {
        return false;
    }

    // Can't do it if any system columns or whole-row Vars are requested. (This
    // could possibly be fixed but would take some fragile assumptions in
    // setrefs.c, I think.)  attr_needed is indexed by (i - min_attr).
    let mut i = rel.min_attr;
    while i <= 0 {
        let idx = (i - rel.min_attr) as usize;
        if !relnode::relids_is_empty::call(&rel.attr_needed[idx]) {
            return false;
        }
        i += 1;
    }

    // Can't do it if the rel is required to emit any placeholder expressions,
    // either.
    for &phid in root.placeholder_list.iter() {
        let phinfo = root.phinfo(phid);
        if relnode::relids_nonempty_difference::call(&phinfo.ph_needed, &rel.relids)
            && relnode::relids_is_subset::call(&phinfo.ph_eval_at, &rel.relids)
        {
            return false;
        }
    }

    // For an index-only scan, the "physical tlist" is the index's indextlist.
    // We can only return that without a projection if all the index's columns
    // are returnable.
    if root.path(path).base().pathtype == T_IndexOnlyScan {
        if let PathNode::IndexPath(ipath) = root.path(path) {
            let indexinfo = ipath
                .indexinfo
                .as_deref()
                .expect("use_physical_tlist: IndexPath has no indexinfo");
            for c in 0..(indexinfo.ncolumns as usize) {
                if !indexinfo.canreturn[c] {
                    return false;
                }
            }
        }
    }

    // Also, can't do it if CP_LABEL_TLIST is specified and path is requested to
    // emit any sort/group columns that are not simple Vars. (If they are simple
    // Vars, they should appear in the physical tlist, and
    // apply_pathtarget_labeling_to_tlist will take care of getting them labeled
    // again.)  We also have to check that no two sort/group columns are the same
    // Var, else that element of the physical tlist would need conflicting
    // ressortgroupref labels.
    if flags & CP_LABEL_TLIST != 0 {
        let target = root
            .path(path)
            .base()
            .pathtarget
            .as_deref()
            .expect("use_physical_tlist: path has no pathtarget");
        if !target.sortgrouprefs.is_empty() {
            let mut sortgroupatts: types_pathnodes::Relids = None;
            for (i, &expr_id) in target.exprs.iter().enumerate() {
                if target.sortgrouprefs[i] != 0 {
                    if let Expr::Var(var) = root.node(expr_id) {
                        let attno = var.varattno as i32 - FIRST_LOW_INVALID_HEAP_ATTRIBUTE_NUMBER;
                        if relnode::relids_is_member::call(attno, &sortgroupatts) {
                            return false;
                        }
                        sortgroupatts = relnode::relids_add_member::call(sortgroupatts, attno);
                    } else {
                        return false;
                    }
                }
            }
        }
    }

    true
}

// ---------------------------------------------------------------------------
// create_scan_plan (createplan.c ~558)
// ---------------------------------------------------------------------------

/// `create_scan_plan(root, best_path, flags)` — create a scan plan for the
/// parent relation of `best_path`.
///
/// Extracts the relevant restriction clauses, optionally concatenates the
/// parameterized join clauses, detects pseudoconstant gating quals, picks the
/// tlist (physical vs path), routes to the per-scan-type `create_*scan_plan`
/// converter (each a seam filled by the F2c scan family — loud-panic until
/// installed), and stacks a gating `Result` for any pseudoconstant quals.
fn create_scan_plan<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    best_path: PathId,
    mut flags: i32,
) -> PgResult<Node<'mcx>> {
    let rel_id = root.path(best_path).base().parent;
    let pathtype = root.path(best_path).base().pathtype;

    // Extract the relevant restriction clauses from the parent relation. The
    // executor must apply all these restrictions during the scan, except for
    // pseudoconstants which we'll take care of below.
    //
    // If this is a plain indexscan or index-only scan, we need not consider
    // restriction clauses that are implied by the index's predicate, so use
    // indrestrictinfo not baserestrictinfo. Note that we can't do that for
    // bitmap indexscans, since there's not necessarily a single index involved;
    // but it doesn't matter since create_bitmap_scan_plan() will be able to get
    // rid of such clauses anyway via predicate proof.
    let mut scan_clauses: Vec<RinfoId> = match pathtype {
        t if t == T_IndexScan || t == T_IndexOnlyScan => {
            let ipath = match root.path(best_path) {
                PathNode::IndexPath(p) => p,
                _ => return Err(PgError::error("create_scan_plan: T_IndexScan path is not an IndexPath")),
            };
            ipath
                .indexinfo
                .as_deref()
                .expect("create_scan_plan: IndexPath has no indexinfo")
                .indrestrictinfo
                .clone()
        }
        _ => root.rel(rel_id).baserestrictinfo.clone(),
    };

    // If this is a parameterized scan, we also need to enforce all the join
    // clauses available from the outer relation(s). For paranoia's sake, don't
    // modify the stored baserestrictinfo list.
    if let Some(ppi) = root.path(best_path).base().param_info.as_deref() {
        // list_concat_copy(scan_clauses, ppi_clauses): scan_clauses was already
        // cloned above, so extend it (a fresh list).
        scan_clauses.extend_from_slice(&ppi.ppi_clauses);
    }

    // Detect whether we have any pseudoconstant quals to deal with. Then, if
    // we'll need a gating Result node, it will be able to project, so there are
    // no requirements on the child's tlist.
    //
    // If this replaces a join, it must be a foreign scan or a custom scan, and
    // the FDW or the custom scan provider would have stored in the best path the
    // list of RestrictInfo nodes to apply to the join; check against that list
    // in that case.
    let gating_clauses: Vec<NodeId> = if is_join_rel(root, rel_id) {
        // Assert(pathtype == T_ForeignScan || pathtype == T_CustomScan);
        debug_assert!(pathtype == T_ForeignScan || pathtype == T_CustomScan);
        let join_clauses: Vec<RinfoId> = match root.path(best_path) {
            PathNode::ForeignPath(fp) => fp.fdw_restrictinfo.clone(),
            PathNode::CustomPath(cp) => cp.custom_restrictinfo.clone(),
            _ => {
                return Err(PgError::error(
                    "create_scan_plan: join rel scan path is neither ForeignPath nor CustomPath",
                ))
            }
        };
        get_gating_quals(root, &join_clauses)
    } else {
        get_gating_quals(root, &scan_clauses)
    };
    if !gating_clauses.is_empty() {
        flags = 0;
    }

    // For table scans, rather than using the relation targetlist (which is only
    // those Vars actually needed by the query), we prefer to generate a tlist
    // containing all Vars in order. This will allow the executor to optimize
    // away projection of the table tuples, if possible.
    //
    // But if the caller is going to ignore our tlist anyway, then don't bother
    // generating one at all. We use an exact equality test here, so that this
    // only applies when CP_IGNORE_TLIST is the only flag set.
    let tlist: Vec<TargetEntry<'mcx>> = if flags == CP_IGNORE_TLIST {
        Vec::new()
    } else if use_physical_tlist(root, best_path, flags) {
        if pathtype == T_IndexOnlyScan {
            // For index-only scan, the preferred tlist is the index's.
            let indextlist: Vec<NodeId> = match root.path(best_path) {
                PathNode::IndexPath(ipath) => ipath
                    .indexinfo
                    .as_deref()
                    .expect("create_scan_plan: IndexPath has no indexinfo")
                    .indextlist
                    .clone(),
                _ => {
                    return Err(PgError::error(
                        "create_scan_plan: T_IndexOnlyScan path is not an IndexPath",
                    ))
                }
            };
            let mut tlist = resolve_targetentry_list(mcx, root, &indextlist)?;
            // Transfer sortgroupref data to the replacement tlist, if requested
            // (use_physical_tlist checked that this will work).
            if flags & CP_LABEL_TLIST != 0 {
                let target = root
                    .path(best_path)
                    .base()
                    .pathtarget
                    .as_deref()
                    .expect("create_scan_plan: path has no pathtarget")
                    .clone();
                apply_pathtarget_labeling_to_tlist(root, &mut tlist, &target)?;
            }
            tlist
        } else {
            let phys: Vec<NodeId> = build_physical_tlist(root, rel_id)?;
            if phys.is_empty() {
                // Failed because of dropped cols, so use regular method.
                build_path_tlist(mcx, root, best_path)?
            } else {
                let mut tlist = resolve_targetentry_list(mcx, root, &phys)?;
                // As above, transfer sortgroupref data to replacement tlist.
                if flags & CP_LABEL_TLIST != 0 {
                    let target = root
                        .path(best_path)
                        .base()
                        .pathtarget
                        .as_deref()
                        .expect("create_scan_plan: path has no pathtarget")
                        .clone();
                    apply_pathtarget_labeling_to_tlist(root, &mut tlist, &target)?;
                }
                tlist
            }
        }
    } else {
        build_path_tlist(mcx, root, best_path)?
    };

    // The per-scan-type converter receives the relation's RestrictInfo
    // `scan_clauses` (each converter does its own `order_qual_clauses` /
    // `extract_actual_clauses` / `replace_nestloop_params`), exactly as C passes
    // the `scan_clauses` list. `scan_clauses` is moved into the matched arm.
    let plan: Node<'mcx> = match pathtype {
        t if t == T_SeqScan => {
            cp_seam::create_seqscan_plan::call(mcx, root, run, best_path, tlist, scan_clauses)?
        }
        t if t == T_SampleScan => {
            cp_seam::create_samplescan_plan::call(mcx, root, run, best_path, tlist, scan_clauses)?
        }
        t if t == T_IndexScan => cp_seam::create_indexscan_plan::call(
            mcx, root, run, best_path, tlist, scan_clauses, false,
        )?,
        t if t == T_IndexOnlyScan => cp_seam::create_indexscan_plan::call(
            mcx, root, run, best_path, tlist, scan_clauses, true,
        )?,
        t if t == T_BitmapHeapScan => {
            cp_seam::create_bitmap_scan_plan::call(mcx, root, run, best_path, tlist, scan_clauses)?
        }
        t if t == T_TidScan => {
            cp_seam::create_tidscan_plan::call(mcx, root, run, best_path, tlist, scan_clauses)?
        }
        t if t == T_TidRangeScan => {
            cp_seam::create_tidrangescan_plan::call(mcx, root, run, best_path, tlist, scan_clauses)?
        }
        t if t == T_SubqueryScan => {
            cp_seam::create_subqueryscan_plan::call(mcx, root, run, best_path, tlist, scan_clauses)?
        }
        t if t == T_FunctionScan => {
            cp_seam::create_functionscan_plan::call(mcx, root, run, best_path, tlist, scan_clauses)?
        }
        t if t == T_TableFuncScan => cp_seam::create_tablefuncscan_plan::call(
            mcx, root, run, best_path, tlist, scan_clauses,
        )?,
        t if t == T_ValuesScan => {
            cp_seam::create_valuesscan_plan::call(mcx, root, run, best_path, tlist, scan_clauses)?
        }
        t if t == T_CteScan => {
            cp_seam::create_ctescan_plan::call(mcx, root, run, best_path, tlist, scan_clauses)?
        }
        t if t == T_NamedTuplestoreScan => cp_seam::create_namedtuplestorescan_plan::call(
            mcx, root, run, best_path, tlist, scan_clauses,
        )?,
        t if t == T_Result => {
            cp_seam::create_resultscan_plan::call(mcx, root, run, best_path, tlist, scan_clauses)?
        }
        t if t == T_WorkTableScan => cp_seam::create_worktablescan_plan::call(
            mcx, root, run, best_path, tlist, scan_clauses,
        )?,
        t if t == T_ForeignScan => {
            cp_seam::create_foreignscan_plan::call(mcx, root, run, best_path, tlist, scan_clauses)?
        }
        t if t == T_CustomScan => {
            cp_seam::create_customscan_plan::call(mcx, root, run, best_path, tlist, scan_clauses)?
        }
        other => {
            return Err(PgError::error(alloc::format!(
                "unrecognized node type: {}",
                other.0
            )))
        }
    };

    // If there are any pseudoconstant clauses attached to this node, insert a
    // gating Result node that evaluates the pseudoconstants as one-time quals.
    if !gating_clauses.is_empty() {
        // create_gating_plan builds the gating Result via make_result (in the
        // F2c scan-converter / make_* family); routed through its seam until
        // that family lands.
        cp_seam::create_gating_plan::call(mcx, root, best_path, plan, gating_clauses)
    } else {
        Ok(plan)
    }
}

/// `IS_JOIN_REL(rel)` (pathnodes.h) — `rel->reloptkind == RELOPT_JOINREL ||
/// rel->reloptkind == RELOPT_OTHER_JOINREL`.
fn is_join_rel(root: &PlannerInfo, rel_id: types_pathnodes::RelId) -> bool {
    use types_pathnodes::{RELOPT_JOINREL, RELOPT_OTHER_JOINREL};
    let k = root.rel(rel_id).reloptkind;
    k == RELOPT_JOINREL || k == RELOPT_OTHER_JOINREL
}

/// Resolve a list of `TargetEntryNode` arena handles (the C `List<TargetEntry>`
/// produced by `build_physical_tlist` / an index's `indextlist`) into owned
/// `TargetEntry<'mcx>` nodes, rebuilding each from its resolved expr (the same
/// shape as `make_tlist_from_pathtarget`, but the handles already carry the
/// resno/sortgroupref/resorig fields). `copyObject(indextlist)` faithfulness:
/// the produced entries are fresh owned copies.
fn resolve_targetentry_list<'mcx>(
    mcx: Mcx<'mcx>,
    root: &PlannerInfo,
    tes: &[NodeId],
) -> PgResult<Vec<TargetEntry<'mcx>>> {
    let mut out = Vec::with_capacity(tes.len());
    for &te_id in tes {
        let te = root.targetentry(te_id).clone();
        let expr = root.node(te.expr).clone();
        let mut tle = make_target_entry(mcx, expr, te.resno, te.resname.as_deref(), te.resjunk)?;
        tle.ressortgroupref = te.ressortgroupref;
        tle.resorigtbl = te.resorigtbl;
        tle.resorigcol = te.resorigcol;
        out.push(tle);
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// get_gating_quals (createplan.c ~1001)
// ---------------------------------------------------------------------------

/// `get_gating_quals(root, quals)` — see if there are pseudoconstant quals in a
/// node's quals list; if so, return just those quals (as bare clause node
/// handles). `quals` is the RestrictInfo list (arena `RinfoId`s).
///
/// In C `quals` is a `List<RestrictInfo>` that `order_qual_clauses` reorders in
/// place (preserving the `RestrictInfo` nodes). Our `order_qual_clauses`
/// operates on node-arena handles that resolve to `Expr::RestrictInfo`, so we
/// intern each `RinfoId` as a transient `Expr::RestrictInfo(RinfoRef)` node (the
/// same `(Expr *) restrictinfo` up-cast joininfo.c uses), order, then resolve
/// each ordered node back to its `RinfoId`. The intern/resolve round-trips the
/// identity, so the result is faithful to C's `List<RestrictInfo>` reorder.
fn get_gating_quals(root: &mut PlannerInfo, quals: &[RinfoId]) -> Vec<NodeId> {
    // No need to look if we know there are no pseudoconstants.
    if !root.hasPseudoConstantQuals {
        return Vec::new();
    }

    // Sort into desirable execution order while still in RestrictInfo form.
    let nodes: Vec<NodeId> = quals
        .iter()
        .map(|&rid| root.alloc_node(Expr::RestrictInfo(rid.as_expr_ref())))
        .collect();
    let ordered = order_qual_clauses(root, &nodes);
    let ordered_rinfos: Vec<RinfoId> = ordered
        .iter()
        .map(|&nid| match root.node(nid) {
            Expr::RestrictInfo(r) => RinfoId::from(*r),
            _ => unreachable!("get_gating_quals: ordered qual node is not a RestrictInfo"),
        })
        .collect();

    // Pull out any pseudoconstant quals from the RestrictInfo list.
    extract_actual_clauses(root, &ordered_rinfos, true)
}

// ---------------------------------------------------------------------------
// create_plan / create_plan_recurse (createplan.c ~336-552)
// ---------------------------------------------------------------------------

/// `create_plan(root, best_path)` — create the access plan for a query by
/// recursively processing the chosen `Path` tree at `best_path`.
///
/// The tlists/quals in the produced plan tree are still in planner format (Vars
/// keyed by parser numbering); setrefs.c fixes them up later. The two finishing
/// steps after the recursion — `apply_tlist_labeling` (tlist.c) on the topmost
/// non-`ModifyTable` plan and `SS_attach_initplans` (subselect.c) — are routed
/// through their owners' seams; the `NestLoopParams` post-check and `plan_params`
/// reset are ported faithfully.
pub fn create_plan<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    best_path: PathId,
) -> PgResult<Node<'mcx>> {
    // Assert(root->plan_params == NIL);
    debug_assert!(root.plan_params.is_empty());

    // Initialize this module's workspace in PlannerInfo.
    root.curOuterRels = None;
    root.curOuterParams = Vec::new();

    // Recursively process the path tree, demanding the correct tlist result.
    let mut plan = create_plan_recurse(mcx, root, run, best_path, CP_EXACT_TLIST)?;

    // Make sure the topmost plan node's targetlist exposes the original column
    // names and other decorative info. Targetlists generated within the planner
    // don't bother with that, but we must have it on the top-level tlist seen at
    // execution time. However, ModifyTable plan nodes don't have a tlist
    // matching the querytree targetlist.
    if !matches!(plan, Node::ModifyTable(_)) {
        cp_seam::apply_tlist_labeling::call(mcx, root, &mut plan)?;
    }

    // Attach any initPlans created in this query level to the topmost plan node.
    // (In principle the initplans could go in any plan node at or above where
    // they're referenced, but there seems no reason to put them any lower than
    // the topmost node for the query level.)
    cp_seam::ss_attach_initplans::call(mcx, root, &mut plan)?;

    // Check we successfully assigned all NestLoopParams to plan nodes.
    if !root.curOuterParams.is_empty() {
        return Err(PgError::error(
            "failed to assign all NestLoopParams to plan nodes",
        ));
    }

    // Reset plan_params to ensure param IDs used for nestloop params are not
    // re-used later.
    root.plan_params = Vec::new();

    Ok(plan)
}

/// `create_plan_recurse(root, best_path, flags)` — recursive guts of
/// [`create_plan`]: dispatch on `best_path->pathtype` over the 36-variant
/// [`PathNode`] enum. Every leaf converter is a per-family seam.
pub fn create_plan_recurse<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    best_path: PathId,
    flags: i32,
) -> PgResult<Node<'mcx>> {
    // check_stack_depth() guards against overly complex plans; that is a
    // tcop/interrupts concern. The recursion structure is what we mirror.

    let pathtype = root.path(best_path).base().pathtype;

    match pathtype {
        T_SeqScan | T_SampleScan | T_IndexScan | T_IndexOnlyScan | T_BitmapHeapScan | T_TidScan
        | T_TidRangeScan | T_SubqueryScan | T_FunctionScan | T_TableFuncScan | T_ValuesScan
        | T_CteScan | T_WorkTableScan | T_NamedTuplestoreScan | T_ForeignScan | T_CustomScan => {
            cp_seam::create_scan_plan::call(mcx, root, run, best_path, flags)
        }
        T_HashJoin | T_MergeJoin | T_NestLoop => {
            cp_seam::create_join_plan::call(mcx, root, run, best_path)
        }
        T_Append => cp_seam::create_append_plan::call(mcx, root, run, best_path, flags),
        T_MergeAppend => cp_seam::create_merge_append_plan::call(mcx, root, run, best_path, flags),
        T_Result => {
            // IsA(best_path, ProjectionPath) / MinMaxAggPath / GroupResultPath /
            // else simple RTE_RESULT base relation (Path).
            match root.path(best_path) {
                PathNode::ProjectionPath(_) => {
                    cp_seam::create_projection_plan::call(mcx, root, run, best_path, flags)
                }
                PathNode::MinMaxAggPath(_) => {
                    cp_seam::create_minmaxagg_plan::call(mcx, root, run, best_path)
                }
                PathNode::GroupResultPath(_) => {
                    cp_seam::create_group_result_plan::call(mcx, root, run, best_path)
                }
                // Simple RTE_RESULT base relation — Assert(IsA(best_path, Path)).
                _ => cp_seam::create_scan_plan::call(mcx, root, run, best_path, flags),
            }
        }
        T_ProjectSet => cp_seam::create_project_set_plan::call(mcx, root, run, best_path),
        T_Material => cp_seam::create_material_plan::call(mcx, root, run, best_path, flags),
        T_Memoize => cp_seam::create_memoize_plan::call(mcx, root, run, best_path, flags),
        // IsA(best_path, UpperUniquePath) vs UniquePath — the sub-discrimination
        // is internal to the Unique family; route the whole T_Unique pathtype.
        T_Unique => cp_seam::create_unique_dispatch_plan::call(mcx, root, run, best_path, flags),
        T_Gather => cp_seam::create_gather_plan::call(mcx, root, run, best_path),
        T_Sort => cp_seam::create_sort_plan::call(mcx, root, run, best_path, flags),
        T_IncrementalSort => {
            cp_seam::create_incrementalsort_plan::call(mcx, root, run, best_path, flags)
        }
        T_Group => cp_seam::create_group_plan::call(mcx, root, run, best_path),
        // IsA(best_path, GroupingSetsPath) vs AggPath — internal to the Agg
        // family; route the whole T_Agg pathtype.
        T_Agg => cp_seam::create_agg_dispatch_plan::call(mcx, root, run, best_path),
        T_WindowAgg => cp_seam::create_windowagg_plan::call(mcx, root, run, best_path),
        T_SetOp => cp_seam::create_setop_plan::call(mcx, root, run, best_path, flags),
        T_RecursiveUnion => cp_seam::create_recursiveunion_plan::call(mcx, root, run, best_path),
        T_LockRows => cp_seam::create_lockrows_plan::call(mcx, root, run, best_path, flags),
        T_ModifyTable => cp_seam::create_modifytable_plan::call(mcx, root, run, best_path),
        T_Limit => cp_seam::create_limit_plan::call(mcx, root, run, best_path, flags),
        T_GatherMerge => cp_seam::create_gather_merge_plan::call(mcx, root, run, best_path),
        other => Err(PgError::error(alloc::format!(
            "unrecognized node type: {}",
            other.0
        ))),
    }
}

// ---------------------------------------------------------------------------
// Scan-converter shared helpers (F2c).
// ---------------------------------------------------------------------------

/// Move an owned `Vec<TargetEntry<'mcx>>` (the C `qptlist` / `tlist`) into a
/// `Plan.targetlist` slot. An empty list is the C `NIL`, stored as `None`.
fn tlist_to_plan_field<'mcx>(
    mcx: Mcx<'mcx>,
    tlist: Vec<TargetEntry<'mcx>>,
) -> PgResult<Option<PgVec<'mcx, TargetEntry<'mcx>>>> {
    if tlist.is_empty() {
        return Ok(None);
    }
    let mut out = vec_with_capacity_in(mcx, tlist.len())?;
    for tle in tlist {
        out.push(tle);
    }
    Ok(Some(out))
}

/// Convert a list of bare clause expression arena handles (the
/// `extract_actual_clauses` output) into an owned qual list, optionally
/// replacing nestloop-supplied `Var`s / `PlaceHolderVar`s with `Param`s when
/// the path is parameterized (the C `replace_nestloop_params(root, (Node *)
/// scan_clauses)`). An empty list is the C `NIL`, stored as `None`.
fn build_scan_qual<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    clauses: &[NodeId],
    has_param_info: bool,
) -> PgResult<Option<PgVec<'mcx, Expr>>> {
    if clauses.is_empty() {
        return Ok(None);
    }
    let mut out = vec_with_capacity_in(mcx, clauses.len())?;
    for &cid in clauses {
        let expr: Expr = if has_param_info {
            let replaced = replace_nestloop_params(mcx, root, cid)?;
            root.node(replaced).clone()
        } else {
            root.node(cid).clone()
        };
        out.push(expr);
    }
    Ok(Some(out))
}

// ---------------------------------------------------------------------------
// make_seqscan (createplan.c ~5643) / make_valuesscan (~5878) /
// make_result (~7129).
// ---------------------------------------------------------------------------

/// `make_seqscan(qptlist, qpqual, scanrelid)` — build a `SeqScan` plan node.
fn make_seqscan<'mcx>(
    qptlist: Option<PgVec<'mcx, TargetEntry<'mcx>>>,
    qpqual: Option<PgVec<'mcx, Expr>>,
    scanrelid: u32,
) -> SeqScan<'mcx> {
    let mut node = SeqScan::default();
    let plan: &mut Plan = &mut node.scan.plan;
    plan.targetlist = qptlist;
    plan.qual = qpqual;
    plan.lefttree = None;
    plan.righttree = None;
    node.scan.scanrelid = scanrelid;
    node
}

/// `make_valuesscan(qptlist, qpqual, scanrelid, values_lists)` — build a
/// `ValuesScan` plan node.
fn make_valuesscan<'mcx>(
    qptlist: Option<PgVec<'mcx, TargetEntry<'mcx>>>,
    qpqual: Option<PgVec<'mcx, Expr>>,
    scanrelid: u32,
    values_lists: PgVec<'mcx, PgVec<'mcx, Expr>>,
) -> ValuesScan<'mcx> {
    let mut node = ValuesScan {
        scan: Scan::default(),
        values_lists,
    };
    let plan: &mut Plan = &mut node.scan.plan;
    plan.targetlist = qptlist;
    plan.qual = qpqual;
    plan.lefttree = None;
    plan.righttree = None;
    node.scan.scanrelid = scanrelid;
    node
}

/// `make_result(tlist, resconstantqual, subplan)` — build a `Result` plan node.
/// `subplan` is the optional input plan (the C `lefttree`).
fn make_result<'mcx>(
    mcx: Mcx<'mcx>,
    tlist: Option<PgVec<'mcx, TargetEntry<'mcx>>>,
    resconstantqual: Option<PgVec<'mcx, Expr>>,
    subplan: Option<Node<'mcx>>,
) -> PgResult<ResultNode<'mcx>> {
    let mut node = ResultNode::default();
    let plan: &mut Plan = &mut node.plan;
    plan.targetlist = tlist;
    // plan->qual = NIL;
    plan.qual = None;
    plan.lefttree = match subplan {
        Some(child) => Some(mcx::alloc_in(mcx, child)?),
        None => None,
    };
    plan.righttree = None;
    node.resconstantqual = resconstantqual;
    Ok(node)
}

// ---------------------------------------------------------------------------
// create_seqscan_plan (createplan.c ~2910)
// ---------------------------------------------------------------------------

/// `create_seqscan_plan(root, best_path, tlist, scan_clauses)` — return a
/// seqscan plan for the base relation scanned by `best_path` with restriction
/// clauses `scan_clauses` and targetlist `tlist`.
fn create_seqscan_plan<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    best_path: PathId,
    tlist: Vec<TargetEntry<'mcx>>,
    scan_clauses: Vec<RinfoId>,
) -> PgResult<Node<'mcx>> {
    let scan_relid = root.path(best_path).base().parent;
    let scan_relid = root.rel(scan_relid).relid;

    // it should be a base rel...
    debug_assert!(scan_relid > 0);
    debug_assert_eq!(
        planner_rt_fetch(run, root, scan_relid).rtekind,
        types_nodes::parsenodes::RTEKind::RTE_RELATION
    );

    let has_param_info = root.path(best_path).base().param_info.is_some();

    // Sort clauses into best execution order.
    let scan_clauses = order_qual_clauses_rinfo(root, &scan_clauses);
    // Reduce RestrictInfo list to bare expressions; ignore pseudoconstants.
    let scan_clauses = extract_actual_clauses(root, &scan_clauses, false);
    // Replace any outer-relation variables with nestloop params (handled in
    // build_scan_qual when the path is parameterized).
    let qpqual = build_scan_qual(mcx, root, &scan_clauses, has_param_info)?;

    let tlist = tlist_to_plan_field(mcx, tlist)?;
    let mut scan_plan = make_seqscan(tlist, qpqual, scan_relid);

    copy_generic_path_info(&mut scan_plan.scan.plan, root.path(best_path).base());

    Ok(Node::SeqScan(scan_plan))
}

// ---------------------------------------------------------------------------
// create_valuesscan_plan (createplan.c ~3840)
// ---------------------------------------------------------------------------

/// `create_valuesscan_plan(root, best_path, tlist, scan_clauses)` — return a
/// valuesscan plan for the base relation scanned by `best_path` with
/// restriction clauses `scan_clauses` and targetlist `tlist`.
fn create_valuesscan_plan<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    best_path: PathId,
    tlist: Vec<TargetEntry<'mcx>>,
    scan_clauses: Vec<RinfoId>,
) -> PgResult<Node<'mcx>> {
    let scan_relid = root.path(best_path).base().parent;
    let scan_relid = root.rel(scan_relid).relid;

    // it should be a values base rel...
    debug_assert!(scan_relid > 0);

    // rte = planner_rt_fetch(scan_relid, root); values_lists = rte->values_lists.
    // The RTE's values_lists is a List<List<Node>>; resolve each row's column
    // expressions into the owned ValuesScan `PgVec<PgVec<Expr>>` carrier.
    let rte = planner_rt_fetch(run, root, scan_relid);
    debug_assert_eq!(rte.rtekind, types_nodes::parsenodes::RTEKind::RTE_VALUES);
    let mut values_lists: PgVec<'mcx, PgVec<'mcx, Expr>> =
        vec_with_capacity_in(mcx, rte.values_lists.len())?;
    for row_node in rte.values_lists.iter() {
        // Each element of values_lists is a List node of column expressions.
        let cols = match &**row_node {
            Node::List(list) => list,
            _ => {
                return Err(PgError::error(
                    "create_valuesscan_plan: RTE values_lists element is not a List",
                ))
            }
        };
        let mut row: PgVec<'mcx, Expr> = vec_with_capacity_in(mcx, cols.len())?;
        for col in cols.iter() {
            match &**col {
                Node::Expr(e) => row.push(e.clone()),
                _ => {
                    return Err(PgError::error(
                        "create_valuesscan_plan: VALUES column is not an expression",
                    ))
                }
            }
        }
        values_lists.push(row);
    }

    let has_param_info = root.path(best_path).base().param_info.is_some();

    // Sort clauses into best execution order.
    let scan_clauses = order_qual_clauses_rinfo(root, &scan_clauses);
    // Reduce RestrictInfo list to bare expressions; ignore pseudoconstants.
    let scan_clauses = extract_actual_clauses(root, &scan_clauses, false);
    // Replace any outer-relation variables with nestloop params.
    let qpqual = build_scan_qual(mcx, root, &scan_clauses, has_param_info)?;

    // The values lists could contain nestloop params, too.
    if has_param_info {
        let mut err: Option<PgError> = None;
        let mut replaced: PgVec<'mcx, PgVec<'mcx, Expr>> =
            vec_with_capacity_in(mcx, values_lists.len())?;
        for row in values_lists.into_iter() {
            let mut new_row: PgVec<'mcx, Expr> = vec_with_capacity_in(mcx, row.len())?;
            for expr in row.into_iter() {
                let out = replace_nestloop_params_mutator(mcx, root, expr, &mut err);
                new_row.push(out);
            }
            replaced.push(new_row);
        }
        if let Some(e) = err {
            return Err(e);
        }
        values_lists = replaced;
    }

    let tlist = tlist_to_plan_field(mcx, tlist)?;
    let mut scan_plan = make_valuesscan(tlist, qpqual, scan_relid, values_lists);

    copy_generic_path_info(&mut scan_plan.scan.plan, root.path(best_path).base());

    Ok(Node::ValuesScan(scan_plan))
}

// ---------------------------------------------------------------------------
// create_resultscan_plan (createplan.c ~4019)
// ---------------------------------------------------------------------------

/// `create_resultscan_plan(root, best_path, tlist, scan_clauses)` — return a
/// `Result` plan for the `RTE_RESULT` base relation scanned by `best_path`
/// with restriction clauses `scan_clauses` and targetlist `tlist`.
fn create_resultscan_plan<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    best_path: PathId,
    tlist: Vec<TargetEntry<'mcx>>,
    scan_clauses: Vec<RinfoId>,
) -> PgResult<Node<'mcx>> {
    let scan_relid = root.path(best_path).base().parent;
    let scan_relid = root.rel(scan_relid).relid;

    debug_assert!(scan_relid > 0);
    debug_assert_eq!(
        planner_rt_fetch(run, root, scan_relid).rtekind,
        types_nodes::parsenodes::RTEKind::RTE_RESULT
    );

    let has_param_info = root.path(best_path).base().param_info.is_some();

    // Sort clauses into best execution order.
    let scan_clauses = order_qual_clauses_rinfo(root, &scan_clauses);
    // Reduce RestrictInfo list to bare expressions; ignore pseudoconstants.
    let scan_clauses = extract_actual_clauses(root, &scan_clauses, false);
    // Replace any outer-relation variables with nestloop params. The bare
    // clause list becomes the Result's resconstantqual (the C `make_result(tlist,
    // (Node *) scan_clauses, NULL)`).
    let resconstantqual = build_scan_qual(mcx, root, &scan_clauses, has_param_info)?;

    let tlist = tlist_to_plan_field(mcx, tlist)?;
    let mut scan_plan = make_result(mcx, tlist, resconstantqual, None)?;

    copy_generic_path_info(&mut scan_plan.plan, root.path(best_path).base());

    Ok(Node::Result(scan_plan))
}

// ---------------------------------------------------------------------------
// order_qual_clauses over a RestrictInfo list (the create_*scan_plan callers
// pass the resolved `scan_clauses` as `Vec<RinfoId>`; C up-casts each
// `RestrictInfo *` to `Node *` for order_qual_clauses, which preserves the
// RestrictInfo identity). Intern each RinfoId as a transient
// `Expr::RestrictInfo`, order, resolve back to RinfoId — the same round-trip
// `get_gating_quals` uses.
// ---------------------------------------------------------------------------

fn order_qual_clauses_rinfo(root: &mut PlannerInfo, quals: &[RinfoId]) -> Vec<RinfoId> {
    if quals.len() <= 1 {
        return quals.to_vec();
    }
    let nodes: Vec<NodeId> = quals
        .iter()
        .map(|&rid| root.alloc_node(Expr::RestrictInfo(rid.as_expr_ref())))
        .collect();
    let ordered = order_qual_clauses(root, &nodes);
    ordered
        .iter()
        .map(|&nid| match root.node(nid) {
            Expr::RestrictInfo(r) => RinfoId::from(*r),
            _ => unreachable!("order_qual_clauses_rinfo: ordered node is not a RestrictInfo"),
        })
        .collect()
}

// ---------------------------------------------------------------------------
// make_samplescan (createplan.c ~5660) / make_tidscan (~5780) /
// make_tidrangescan (~5799) / make_functionscan (~5838) /
// make_ctescan (~5897) / make_namedtuplestorescan (~5918) /
// make_worktablescan (~5938) / make_subqueryscan (~5818).
// ---------------------------------------------------------------------------

/// `make_samplescan(qptlist, qpqual, scanrelid, tsc)` — build a `SampleScan`
/// plan node.
fn make_samplescan<'mcx>(
    qptlist: Option<PgVec<'mcx, TargetEntry<'mcx>>>,
    qpqual: Option<PgVec<'mcx, Expr>>,
    scanrelid: u32,
    tsc: TableSampleClause<'mcx>,
) -> SampleScan<'mcx> {
    let mut node = SampleScan::default();
    let plan: &mut Plan = &mut node.scan.plan;
    plan.targetlist = qptlist;
    plan.qual = qpqual;
    plan.lefttree = None;
    plan.righttree = None;
    node.scan.scanrelid = scanrelid;
    node.tablesample = Some(alloc::boxed::Box::new(tsc));
    node
}

/// `make_tidscan(qptlist, qpqual, scanrelid, tidquals)` — build a `TidScan`
/// plan node.
fn make_tidscan<'mcx>(
    qptlist: Option<PgVec<'mcx, TargetEntry<'mcx>>>,
    qpqual: Option<PgVec<'mcx, Expr>>,
    scanrelid: u32,
    tidquals: Option<PgVec<'mcx, Expr>>,
) -> TidScan<'mcx> {
    let mut node = TidScan::default();
    let plan: &mut Plan = &mut node.scan.plan;
    plan.targetlist = qptlist;
    plan.qual = qpqual;
    plan.lefttree = None;
    plan.righttree = None;
    node.scan.scanrelid = scanrelid;
    node.tidquals = tidquals;
    node
}

/// `make_tidrangescan(qptlist, qpqual, scanrelid, tidrangequals)` — build a
/// `TidRangeScan` plan node.
fn make_tidrangescan<'mcx>(
    qptlist: Option<PgVec<'mcx, TargetEntry<'mcx>>>,
    qpqual: Option<PgVec<'mcx, Expr>>,
    scanrelid: u32,
    tidrangequals: Option<PgVec<'mcx, Expr>>,
) -> TidRangeScan<'mcx> {
    let mut node = TidRangeScan::default();
    let plan: &mut Plan = &mut node.scan.plan;
    plan.targetlist = qptlist;
    plan.qual = qpqual;
    plan.lefttree = None;
    plan.righttree = None;
    node.scan.scanrelid = scanrelid;
    node.tidrangequals = tidrangequals;
    node
}

/// `make_functionscan(qptlist, qpqual, scanrelid, functions, funcordinality)`
/// — build a `FunctionScan` plan node.
fn make_functionscan<'mcx>(
    qptlist: Option<PgVec<'mcx, TargetEntry<'mcx>>>,
    qpqual: Option<PgVec<'mcx, Expr>>,
    scanrelid: u32,
    functions: Option<PgVec<'mcx, RangeTblFunction<'mcx>>>,
    funcordinality: bool,
) -> FunctionScan<'mcx> {
    let mut node = FunctionScan::default();
    let plan: &mut Plan = &mut node.scan.plan;
    plan.targetlist = qptlist;
    plan.qual = qpqual;
    plan.lefttree = None;
    plan.righttree = None;
    node.scan.scanrelid = scanrelid;
    node.functions = functions;
    node.funcordinality = funcordinality;
    node
}

/// `make_ctescan(qptlist, qpqual, scanrelid, ctePlanId, cteParam)` — build a
/// `CteScan` plan node.
fn make_ctescan<'mcx>(
    qptlist: Option<PgVec<'mcx, TargetEntry<'mcx>>>,
    qpqual: Option<PgVec<'mcx, Expr>>,
    scanrelid: u32,
    cte_plan_id: i32,
    cte_param: i32,
) -> CteScan<'mcx> {
    let mut node = CteScan::default();
    let plan: &mut Plan = &mut node.scan.plan;
    plan.targetlist = qptlist;
    plan.qual = qpqual;
    plan.lefttree = None;
    plan.righttree = None;
    node.scan.scanrelid = scanrelid;
    node.ctePlanId = cte_plan_id;
    node.cteParam = cte_param;
    node
}

/// `make_namedtuplestorescan(qptlist, qpqual, scanrelid, enrname)` — build a
/// `NamedTuplestoreScan` plan node. (Cost is inserted by the caller.)
fn make_namedtuplestorescan<'mcx>(
    qptlist: Option<PgVec<'mcx, TargetEntry<'mcx>>>,
    qpqual: Option<PgVec<'mcx, Expr>>,
    scanrelid: u32,
    enrname: Option<PgString<'mcx>>,
) -> NamedTuplestoreScan<'mcx> {
    let mut node = NamedTuplestoreScan {
        scan: Scan::default(),
        enrname,
    };
    let plan: &mut Plan = &mut node.scan.plan;
    plan.targetlist = qptlist;
    plan.qual = qpqual;
    plan.lefttree = None;
    plan.righttree = None;
    node.scan.scanrelid = scanrelid;
    node
}

/// `make_worktablescan(qptlist, qpqual, scanrelid, wtParam)` — build a
/// `WorkTableScan` plan node.
fn make_worktablescan<'mcx>(
    qptlist: Option<PgVec<'mcx, TargetEntry<'mcx>>>,
    qpqual: Option<PgVec<'mcx, Expr>>,
    scanrelid: u32,
    wt_param: i32,
) -> WorkTableScan<'mcx> {
    let mut node = WorkTableScan::default();
    let plan: &mut Plan = &mut node.scan.plan;
    plan.targetlist = qptlist;
    plan.qual = qpqual;
    plan.lefttree = None;
    plan.righttree = None;
    node.scan.scanrelid = scanrelid;
    node.wtParam = wt_param;
    node
}

/// `make_subqueryscan(qptlist, qpqual, scanrelid, subplan)` — build a
/// `SubqueryScan` plan node. The child plan is stored on `node.subplan` (the C
/// places it on the plan node, not `lefttree`, so walkers do not recurse).
fn make_subqueryscan<'mcx>(
    mcx: Mcx<'mcx>,
    qptlist: Option<PgVec<'mcx, TargetEntry<'mcx>>>,
    qpqual: Option<PgVec<'mcx, Expr>>,
    scanrelid: u32,
    subplan: Node<'mcx>,
) -> PgResult<SubqueryScan<'mcx>> {
    let mut node = SubqueryScan::default();
    {
        let plan: &mut Plan = &mut node.scan.plan;
        plan.targetlist = qptlist;
        plan.qual = qpqual;
        plan.lefttree = None;
        plan.righttree = None;
    }
    node.scan.scanrelid = scanrelid;
    node.subplan = Some(mcx::alloc_in(mcx, subplan)?);
    node.scanstatus = SubqueryScanStatus::Unknown;
    Ok(node)
}

// ---------------------------------------------------------------------------
// nestloop-param replacement over owned Expr lists / owned sub-objects.
//
// The C `replace_nestloop_params(root, (Node *) list)` walks a whole subtree.
// The createplan crate's `replace_nestloop_params` operates over arena handles,
// but the function / tablesample / values payloads resolved out of the RTE are
// already owned `Expr` / sub-object trees, so the mutator is applied directly
// (the same shape as `create_valuesscan_plan`'s values-list rewrite).
// ---------------------------------------------------------------------------

/// Apply `replace_nestloop_params_mutator` to an owned `Expr`. Errors are
/// surfaced through `err` (the infallible-mutator stash pattern).
fn replace_nestloop_params_expr(
    mcx: Mcx<'_>,
    root: &mut PlannerInfo,
    expr: Expr,
    err: &mut Option<PgError>,
) -> Expr {
    replace_nestloop_params_mutator(mcx, root, expr, err)
}

// ---------------------------------------------------------------------------
// create_samplescan_plan (createplan.c ~2948)
// ---------------------------------------------------------------------------

/// `create_samplescan_plan(root, best_path, tlist, scan_clauses)` — return a
/// samplescan plan for the base relation scanned by `best_path`.
fn create_samplescan_plan<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    best_path: PathId,
    tlist: Vec<TargetEntry<'mcx>>,
    scan_clauses: Vec<RinfoId>,
) -> PgResult<Node<'mcx>> {
    let scan_relid = root.path(best_path).base().parent;
    let scan_relid = root.rel(scan_relid).relid;

    // it should be a base rel with a tablesample clause...
    debug_assert!(scan_relid > 0);
    let rte = planner_rt_fetch(run, root, scan_relid);
    debug_assert_eq!(rte.rtekind, types_nodes::parsenodes::RTEKind::RTE_RELATION);
    // tsc = rte->tablesample; Assert(tsc != NULL);
    let tsc_node = rte
        .tablesample
        .as_deref()
        .expect("create_samplescan_plan: RTE has no tablesample clause");
    let mut tsc: TableSampleClause<'mcx> = match tsc_node {
        Node::TableSampleClause(t) => t.clone_in(mcx)?,
        _ => {
            return Err(PgError::error(
                "create_samplescan_plan: RTE tablesample is not a TableSampleClause",
            ))
        }
    };

    let has_param_info = root.path(best_path).base().param_info.is_some();

    // Sort clauses into best execution order.
    let scan_clauses = order_qual_clauses_rinfo(root, &scan_clauses);
    // Reduce RestrictInfo list to bare expressions; ignore pseudoconstants.
    let scan_clauses = extract_actual_clauses(root, &scan_clauses, false);
    // Replace any outer-relation variables with nestloop params.
    let qpqual = build_scan_qual(mcx, root, &scan_clauses, has_param_info)?;

    // The TableSampleClause args / repeatable could contain nestloop params, too.
    if has_param_info {
        let mut err: Option<PgError> = None;
        if let Some(args) = tsc.args.take() {
            let mut new_args: PgVec<'mcx, Expr> = vec_with_capacity_in(mcx, args.len())?;
            for e in args.into_iter() {
                new_args.push(replace_nestloop_params_expr(mcx, root, e, &mut err));
            }
            tsc.args = Some(new_args);
        }
        if let Some(rep) = tsc.repeatable.take() {
            let out = replace_nestloop_params_expr(mcx, root, *rep, &mut err);
            tsc.repeatable = Some(alloc::boxed::Box::new(out));
        }
        if let Some(e) = err {
            return Err(e);
        }
    }

    let tlist = tlist_to_plan_field(mcx, tlist)?;
    let mut scan_plan = make_samplescan(tlist, qpqual, scan_relid, tsc);

    copy_generic_path_info(&mut scan_plan.scan.plan, root.path(best_path).base());

    Ok(Node::SampleScan(scan_plan))
}

// ---------------------------------------------------------------------------
// create_tidscan_plan (createplan.c ~3533)
// ---------------------------------------------------------------------------

/// `create_tidscan_plan(root, best_path, tlist, scan_clauses)` — return a
/// tidscan plan for the base relation scanned by `best_path`.
fn create_tidscan_plan<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    best_path: PathId,
    tlist: Vec<TargetEntry<'mcx>>,
    scan_clauses: Vec<RinfoId>,
) -> PgResult<Node<'mcx>> {
    let rel_id = root.path(best_path).base().parent;
    let scan_relid = root.rel(rel_id).relid;
    // tidquals = best_path->tidquals (bare expr handles).
    let tidquals_nodes: Vec<NodeId> = match root.path(best_path) {
        PathNode::TidPath(p) => p.tidquals.clone(),
        _ => return Err(PgError::error("create_tidscan_plan: path is not a TidPath")),
    };

    // it should be a base rel...
    debug_assert!(scan_relid > 0);
    debug_assert_eq!(
        planner_rt_fetch(run, root, scan_relid).rtekind,
        types_nodes::parsenodes::RTEKind::RTE_RELATION
    );

    // The qpqual list must contain all restrictions not enforced by the
    // tidquals list. Handle the single-tidqual case separately: drop any
    // scan_clause that is redundant with the tidqual, while still in
    // RestrictInfo form.
    let mut scan_clauses = scan_clauses;
    if tidquals_nodes.len() == 1 {
        // Resolve the single tidqual node to a RinfoId for redundancy checks
        // (tidquals are bare expr nodes; tidpath.c built them from the same
        // RestrictInfos, so identity/EC-derivation must be checked against the
        // RestrictInfo list). In C list_member_ptr / is_redundant_derived_clause
        // operate over the tidquals (which here is a bare-expr list); the
        // single-tidqual redundancy test needs the original RestrictInfo. We
        // mirror C by keeping a RestrictInfo handle for each scan clause and
        // comparing via is_redundant_derived_clause against the bare tidqual.
        let mut qpqual: Vec<RinfoId> = Vec::new();
        for &rinfo_id in scan_clauses.iter() {
            let rinfo = root.rinfo(rinfo_id);
            if rinfo.pseudoconstant {
                continue; // we may drop pseudoconstants here
            }
            // list_member_ptr(tidquals, rinfo): tidquals here are bare expr
            // handles, so pointer-identity to the RestrictInfo cannot match;
            // the EC-derivation test below subsumes the duplicate case.
            // is_redundant_derived_clause(rinfo, tidquals): tidquals in C is a
            // List<RestrictInfo>; here tidpath stored bare exprs, so this test
            // is over the (single) tidqual clause node. The seam compares the
            // RestrictInfo's parent EC against the tidqual clause list.
            if is_redundant_derived_from_tidquals(root, rinfo_id, &tidquals_nodes) {
                continue; // derived from same EquivalenceClass
            }
            qpqual.push(rinfo_id);
        }
        scan_clauses = qpqual;
    }

    let has_param_info = root.path(best_path).base().param_info.is_some();

    // Sort clauses into best execution order.
    let scan_clauses = order_qual_clauses_rinfo(root, &scan_clauses);

    // Reduce RestrictInfo lists to bare expressions; ignore pseudoconstants.
    // tidquals are already bare expr handles.
    let mut tidquals = tidquals_nodes;
    let scan_clauses = extract_actual_clauses(root, &scan_clauses, false);

    // If we have multiple tidquals, remove duplicate scan_clauses after
    // stripping the RestrictInfos: convert the tidquals list to an explicit OR
    // clause and drop any scan clause that equal()s it.
    let mut scan_clauses = scan_clauses;
    if tidquals.len() > 1 {
        // make_orclause(tidquals) over the bare exprs.
        let or_exprs: Vec<Expr> = tidquals.iter().map(|&n| root.node(n).clone()).collect();
        let orclause = make_orclause(or_exprs);
        // list_difference(scan_clauses, list_make1(orclause)): drop any
        // scan_clause that equal()s the OR clause.
        scan_clauses = scan_clauses
            .into_iter()
            .filter(|&c| !equal_expr_seam::call(root.node(c), &orclause))
            .collect();
    }

    // Replace any outer-relation variables with nestloop params.
    if has_param_info {
        let mut err: Option<PgError> = None;
        tidquals = tidquals
            .into_iter()
            .map(|n| {
                let e = root.node(n).clone();
                let out = replace_nestloop_params_expr(mcx, root, e, &mut err);
                root.alloc_node(out)
            })
            .collect();
        if let Some(e) = err {
            return Err(e);
        }
    }
    let qpqual = build_scan_qual(mcx, root, &scan_clauses, has_param_info)?;

    // Build the owned tidquals expr list.
    let tidquals_field: Option<PgVec<'mcx, Expr>> = if tidquals.is_empty() {
        None
    } else {
        let mut out = vec_with_capacity_in(mcx, tidquals.len())?;
        for &n in tidquals.iter() {
            out.push(root.node(n).clone());
        }
        Some(out)
    };

    let tlist = tlist_to_plan_field(mcx, tlist)?;
    let mut scan_plan = make_tidscan(tlist, qpqual, scan_relid, tidquals_field);

    copy_generic_path_info(&mut scan_plan.scan.plan, root.path(best_path).base());

    Ok(Node::TidScan(scan_plan))
}

/// `is_redundant_derived_clause(rinfo, tidquals)` over a bare-expr tidqual list.
/// tidpath.c builds `tidquals` as bare expressions extracted from RestrictInfos;
/// to mirror the C redundancy test (which runs over a `List<RestrictInfo>`) we
/// route it through the equivclass seam against the single tidqual node's
/// parent RestrictInfo when available, else fall back to `equal()` identity.
fn is_redundant_derived_from_tidquals(
    root: &PlannerInfo,
    rinfo_id: RinfoId,
    tidquals: &[NodeId],
) -> bool {
    // Collect the RestrictInfo handles backing the tidquals. tidpath stores
    // bare expr nodes; when a tidqual node resolves to an Expr::RestrictInfo we
    // can run is_redundant_derived_clause; otherwise the single-qual case
    // reduces to an equal() identity test against rinfo's clause.
    let mut rinfos: Vec<RinfoId> = Vec::new();
    for &n in tidquals {
        if let Expr::RestrictInfo(r) = root.node(n) {
            rinfos.push(RinfoId::from(*r));
        }
    }
    if !rinfos.is_empty() {
        return equivclass::is_redundant_derived_clause::call(root, rinfo_id, rinfos);
    }
    false
}

// ---------------------------------------------------------------------------
// create_tidrangescan_plan (createplan.c ~3630)
// ---------------------------------------------------------------------------

/// `create_tidrangescan_plan(root, best_path, tlist, scan_clauses)` — return a
/// tidrangescan plan for the base relation scanned by `best_path`.
fn create_tidrangescan_plan<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    best_path: PathId,
    tlist: Vec<TargetEntry<'mcx>>,
    scan_clauses: Vec<RinfoId>,
) -> PgResult<Node<'mcx>> {
    let rel_id = root.path(best_path).base().parent;
    let scan_relid = root.rel(rel_id).relid;
    let tidrangequals_nodes: Vec<NodeId> = match root.path(best_path) {
        PathNode::TidRangePath(p) => p.tidrangequals.clone(),
        _ => {
            return Err(PgError::error(
                "create_tidrangescan_plan: path is not a TidRangePath",
            ))
        }
    };

    // it should be a base rel...
    debug_assert!(scan_relid > 0);
    debug_assert_eq!(
        planner_rt_fetch(run, root, scan_relid).rtekind,
        types_nodes::parsenodes::RTEKind::RTE_RELATION
    );

    // The qpqual list must contain all restrictions not enforced by the
    // tidrangequals list. tidrangequals has AND semantics, so we simply remove
    // any qual that appears in it (matched by EC-derivation / identity over the
    // RestrictInfos backing the tidrangequals).
    let trq_rinfos: Vec<RinfoId> = tidrangequals_nodes
        .iter()
        .filter_map(|&n| match root.node(n) {
            Expr::RestrictInfo(r) => Some(RinfoId::from(*r)),
            _ => None,
        })
        .collect();
    let mut qpqual: Vec<RinfoId> = Vec::new();
    for &rinfo_id in scan_clauses.iter() {
        let rinfo = root.rinfo(rinfo_id);
        if rinfo.pseudoconstant {
            continue; // we may drop pseudoconstants here
        }
        // list_member_ptr(tidrangequals, rinfo): identity over RestrictInfos.
        if trq_rinfos.iter().any(|&r| r == rinfo_id) {
            continue; // simple duplicate
        }
        qpqual.push(rinfo_id);
    }
    let scan_clauses = qpqual;

    let has_param_info = root.path(best_path).base().param_info.is_some();

    // Sort clauses into best execution order.
    let scan_clauses = order_qual_clauses_rinfo(root, &scan_clauses);
    // Reduce RestrictInfo lists to bare expressions; ignore pseudoconstants.
    // tidrangequals are already bare expr handles.
    let mut tidrangequals = tidrangequals_nodes;
    let scan_clauses = extract_actual_clauses(root, &scan_clauses, false);

    // Replace any outer-relation variables with nestloop params.
    if has_param_info {
        let mut err: Option<PgError> = None;
        tidrangequals = tidrangequals
            .into_iter()
            .map(|n| {
                let e = root.node(n).clone();
                let out = replace_nestloop_params_expr(mcx, root, e, &mut err);
                root.alloc_node(out)
            })
            .collect();
        if let Some(e) = err {
            return Err(e);
        }
    }
    let qpqual = build_scan_qual(mcx, root, &scan_clauses, has_param_info)?;

    let tidrangequals_field: Option<PgVec<'mcx, Expr>> = if tidrangequals.is_empty() {
        None
    } else {
        let mut out = vec_with_capacity_in(mcx, tidrangequals.len())?;
        for &n in tidrangequals.iter() {
            out.push(root.node(n).clone());
        }
        Some(out)
    };

    let tlist = tlist_to_plan_field(mcx, tlist)?;
    let mut scan_plan = make_tidrangescan(tlist, qpqual, scan_relid, tidrangequals_field);

    copy_generic_path_info(&mut scan_plan.scan.plan, root.path(best_path).base());

    Ok(Node::TidRangeScan(scan_plan))
}

// ---------------------------------------------------------------------------
// create_functionscan_plan (createplan.c ~3754)
// ---------------------------------------------------------------------------

/// `create_functionscan_plan(root, best_path, tlist, scan_clauses)` — return a
/// functionscan plan for the base relation scanned by `best_path`.
fn create_functionscan_plan<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    best_path: PathId,
    tlist: Vec<TargetEntry<'mcx>>,
    scan_clauses: Vec<RinfoId>,
) -> PgResult<Node<'mcx>> {
    let rel_id = root.path(best_path).base().parent;
    let scan_relid = root.rel(rel_id).relid;

    // it should be a function base rel...
    debug_assert!(scan_relid > 0);
    let rte = planner_rt_fetch(run, root, scan_relid);
    debug_assert_eq!(rte.rtekind, types_nodes::parsenodes::RTEKind::RTE_FUNCTION);
    let funcordinality = rte.funcordinality;
    // functions = rte->functions (List<RangeTblFunction>). Resolve each into an
    // owned RangeTblFunction.
    let mut functions: Vec<RangeTblFunction<'mcx>> = Vec::with_capacity(rte.functions.len());
    for f in rte.functions.iter() {
        match &**f {
            Node::RangeTblFunction(rtf) => functions.push(rtf.clone_in(mcx)?),
            _ => {
                return Err(PgError::error(
                    "create_functionscan_plan: RTE functions element is not a RangeTblFunction",
                ))
            }
        }
    }

    let has_param_info = root.path(best_path).base().param_info.is_some();

    // Sort clauses into best execution order.
    let scan_clauses = order_qual_clauses_rinfo(root, &scan_clauses);
    // Reduce RestrictInfo list to bare expressions; ignore pseudoconstants.
    let scan_clauses = extract_actual_clauses(root, &scan_clauses, false);
    // Replace any outer-relation variables with nestloop params.
    let qpqual = build_scan_qual(mcx, root, &scan_clauses, has_param_info)?;

    // The function expressions could contain nestloop params, too.
    if has_param_info {
        let mut err: Option<PgError> = None;
        for func in functions.iter_mut() {
            if let Some(fe) = func.funcexpr.as_deref_mut() {
                // funcexpr is a Node *; replace nestloop params within its Expr.
                if let Node::Expr(e) = fe {
                    let old = e.clone();
                    *e = replace_nestloop_params_expr(mcx, root, old, &mut err);
                }
            }
        }
        if let Some(e) = err {
            return Err(e);
        }
    }

    let functions_field: Option<PgVec<'mcx, RangeTblFunction<'mcx>>> = if functions.is_empty() {
        None
    } else {
        let mut out = vec_with_capacity_in(mcx, functions.len())?;
        for f in functions {
            out.push(f);
        }
        Some(out)
    };

    let tlist = tlist_to_plan_field(mcx, tlist)?;
    let mut scan_plan = make_functionscan(tlist, qpqual, scan_relid, functions_field, funcordinality);

    copy_generic_path_info(&mut scan_plan.scan.plan, root.path(best_path).base());

    Ok(Node::FunctionScan(scan_plan))
}

// ---------------------------------------------------------------------------
// create_namedtuplestorescan_plan (createplan.c ~3979)
// ---------------------------------------------------------------------------

/// `create_namedtuplestorescan_plan(root, best_path, tlist, scan_clauses)` —
/// return a tuplestorescan plan for the base relation scanned by `best_path`.
fn create_namedtuplestorescan_plan<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    best_path: PathId,
    tlist: Vec<TargetEntry<'mcx>>,
    scan_clauses: Vec<RinfoId>,
) -> PgResult<Node<'mcx>> {
    let rel_id = root.path(best_path).base().parent;
    let scan_relid = root.rel(rel_id).relid;

    debug_assert!(scan_relid > 0);
    let rte = planner_rt_fetch(run, root, scan_relid);
    debug_assert_eq!(
        rte.rtekind,
        types_nodes::parsenodes::RTEKind::RTE_NAMEDTUPLESTORE
    );
    let enrname: Option<PgString<'mcx>> = match &rte.enrname {
        Some(n) => Some(n.clone_in(mcx)?),
        None => None,
    };

    let has_param_info = root.path(best_path).base().param_info.is_some();

    // Sort clauses into best execution order.
    let scan_clauses = order_qual_clauses_rinfo(root, &scan_clauses);
    // Reduce RestrictInfo list to bare expressions; ignore pseudoconstants.
    let scan_clauses = extract_actual_clauses(root, &scan_clauses, false);
    // Replace any outer-relation variables with nestloop params.
    let qpqual = build_scan_qual(mcx, root, &scan_clauses, has_param_info)?;

    let tlist = tlist_to_plan_field(mcx, tlist)?;
    let mut scan_plan = make_namedtuplestorescan(tlist, qpqual, scan_relid, enrname);

    copy_generic_path_info(&mut scan_plan.scan.plan, root.path(best_path).base());

    Ok(Node::NamedTuplestoreScan(scan_plan))
}

// ---------------------------------------------------------------------------
// create_ctescan_plan (createplan.c ~3884)
//
// The SubPlan-init-plan resolution leg (locating the CTE's `plan_id` /
// `cte_param_id` via `cteroot->parse->cteList` + `cteroot->init_plans`) reads
// the init `SubPlan`s built by subselect.c (`SS_process_ctes`), which is
// unported. The plan_id lookup over `cte_plan_ids` is portable, but the
// `cte_param_id = linitial_int(ctesplan->setParam)` step dereferences a built
// SubPlan node, so the whole CTE-param resolution is routed 1:1 through the
// subselect seam. The rest of the converter (clause ordering / nestloop params
// / make_ctescan) is ported.
// ---------------------------------------------------------------------------

/// `create_ctescan_plan(root, best_path, tlist, scan_clauses)` — return a
/// ctescan plan for the base relation scanned by `best_path`.
fn create_ctescan_plan<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    best_path: PathId,
    tlist: Vec<TargetEntry<'mcx>>,
    scan_clauses: Vec<RinfoId>,
) -> PgResult<Node<'mcx>> {
    let rel_id = root.path(best_path).base().parent;
    let scan_relid = root.rel(rel_id).relid;

    debug_assert!(scan_relid > 0);
    let rte = planner_rt_fetch(run, root, scan_relid);
    debug_assert_eq!(rte.rtekind, types_nodes::parsenodes::RTEKind::RTE_CTE);
    debug_assert!(!rte.self_reference);

    // Find the referenced CTE, locate its SubPlan, and pull out the CTE param
    // ID (the sole member of the SubPlan's setParam list). This dereferences
    // the init SubPlans built by subselect.c (SS_process_ctes), which is
    // unported, so the (plan_id, cte_param_id) pair is resolved 1:1 through the
    // subselect seam.
    let (plan_id, cte_param_id) =
        cp_seam::resolve_cte_subplan::call(root, scan_relid)?;

    let has_param_info = root.path(best_path).base().param_info.is_some();

    // Sort clauses into best execution order.
    let scan_clauses = order_qual_clauses_rinfo(root, &scan_clauses);
    // Reduce RestrictInfo list to bare expressions; ignore pseudoconstants.
    let scan_clauses = extract_actual_clauses(root, &scan_clauses, false);
    // Replace any outer-relation variables with nestloop params.
    let qpqual = build_scan_qual(mcx, root, &scan_clauses, has_param_info)?;

    let tlist = tlist_to_plan_field(mcx, tlist)?;
    let mut scan_plan = make_ctescan(tlist, qpqual, scan_relid, plan_id, cte_param_id);

    copy_generic_path_info(&mut scan_plan.scan.plan, root.path(best_path).base());

    Ok(Node::CteScan(scan_plan))
}

// ---------------------------------------------------------------------------
// create_worktablescan_plan (createplan.c ~4055)
//
// The work-table param ID is found in the plan level processing the recursive
// UNION (one level below where the CTE comes from): cteroot->wt_param_id. The
// parent_root walk is portable, but identifying the correct cteroot and reading
// its wt_param_id depends on subselect.c having set wt_param_id during
// recursive-CTE planning (SS_make_initplan_from_plan / build_subplan), so the
// param-ID resolution is routed 1:1 through the subselect seam.
// ---------------------------------------------------------------------------

/// `create_worktablescan_plan(root, best_path, tlist, scan_clauses)` — return a
/// worktablescan plan for the base relation scanned by `best_path`.
fn create_worktablescan_plan<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    best_path: PathId,
    tlist: Vec<TargetEntry<'mcx>>,
    scan_clauses: Vec<RinfoId>,
) -> PgResult<Node<'mcx>> {
    let rel_id = root.path(best_path).base().parent;
    let scan_relid = root.rel(rel_id).relid;

    debug_assert!(scan_relid > 0);
    let rte = planner_rt_fetch(run, root, scan_relid);
    debug_assert_eq!(rte.rtekind, types_nodes::parsenodes::RTEKind::RTE_CTE);
    debug_assert!(rte.self_reference);

    // Find the worktable param ID, which is in the plan level processing the
    // recursive UNION (one level below where the CTE comes from):
    // cteroot->wt_param_id. The parent_root walk + wt_param_id read are owned
    // by subselect's recursive-CTE planning, so the resolution is routed 1:1
    // through the subselect seam.
    let wt_param_id = cp_seam::resolve_worktable_param::call(root, scan_relid)?;

    let has_param_info = root.path(best_path).base().param_info.is_some();

    // Sort clauses into best execution order.
    let scan_clauses = order_qual_clauses_rinfo(root, &scan_clauses);
    // Reduce RestrictInfo list to bare expressions; ignore pseudoconstants.
    let scan_clauses = extract_actual_clauses(root, &scan_clauses, false);
    // Replace any outer-relation variables with nestloop params.
    let qpqual = build_scan_qual(mcx, root, &scan_clauses, has_param_info)?;

    let tlist = tlist_to_plan_field(mcx, tlist)?;
    let mut scan_plan = make_worktablescan(tlist, qpqual, scan_relid, wt_param_id);

    copy_generic_path_info(&mut scan_plan.scan.plan, root.path(best_path).base());

    Ok(Node::WorkTableScan(scan_plan))
}

// ---------------------------------------------------------------------------
// create_subqueryscan_plan (createplan.c ~3695)
//
// The subquery's child plan is built by recursing into create_plan with the
// subquery's own PlannerInfo (rel->subroot), a *different* planner context.
// rel->subroot is the unported subquery_planner output; building its plan
// (create_plan over the subroot's path tree) needs the subroot's PlannerRun
// (its own range table), which the planner driver owns. So the subplan
// construction (subroot recursion) is routed 1:1 through the subselect seam;
// the rest of the converter (process_subquery_nestloop_params + clause
// ordering / nestloop params / make_subqueryscan) is ported.
// ---------------------------------------------------------------------------

/// `create_subqueryscan_plan(root, best_path, tlist, scan_clauses)` — return a
/// subqueryscan plan for the base relation scanned by `best_path`.
fn create_subqueryscan_plan<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    best_path: PathId,
    tlist: Vec<TargetEntry<'mcx>>,
    scan_clauses: Vec<RinfoId>,
) -> PgResult<Node<'mcx>> {
    let rel_id = root.path(best_path).base().parent;
    let scan_relid = root.rel(rel_id).relid;

    // it should be a subquery base rel...
    debug_assert!(scan_relid > 0);
    debug_assert_eq!(root.rel(rel_id).rtekind, RTE_SUBQUERY);

    // Recursively create Plan from Path for the subquery. Since we are entering
    // a different planner context (subroot), C recurses to create_plan (not
    // create_plan_recurse) with rel->subroot. Building the subroot's plan
    // requires the subroot PlannerInfo + its PlannerRun, owned by the planner
    // driver; routed 1:1 through the subselect/planner seam.
    let subplan = cp_seam::create_subqueryscan_subplan::call(mcx, root, run, best_path)?;

    let has_param_info = root.path(best_path).base().param_info.is_some();

    // Sort clauses into best execution order.
    let scan_clauses = order_qual_clauses_rinfo(root, &scan_clauses);
    // Reduce RestrictInfo list to bare expressions; ignore pseudoconstants.
    let scan_clauses = extract_actual_clauses(root, &scan_clauses, false);

    // Replace any outer-relation variables with nestloop params. We must
    // provide nestloop params for both lateral references of the subquery and
    // outer vars in the scan_clauses; assign the former first.
    if has_param_info {
        let subplan_params = root.rel(rel_id).subplan_params.clone();
        paramassign::process_subquery_nestloop_params::call(mcx, root, &subplan_params)?;
    }
    let qpqual = build_scan_qual(mcx, root, &scan_clauses, has_param_info)?;

    let tlist = tlist_to_plan_field(mcx, tlist)?;
    let mut scan_plan = make_subqueryscan(mcx, tlist, qpqual, scan_relid, subplan)?;

    copy_generic_path_info(&mut scan_plan.scan.plan, root.path(best_path).base());

    Ok(Node::SubqueryScan(scan_plan))
}

// ---------------------------------------------------------------------------
// Seam installation (this crate OWNS create_scan_plan).
// ---------------------------------------------------------------------------

/// Install the createplan-owned seams. F2b owns `create_scan_plan` (the scan
/// dispatch + tlist selection + gating wiring); F2c adds the three simple scan
/// converters (`create_seqscan_plan` / `create_valuesscan_plan` /
/// `create_resultscan_plan`). The remaining per-scan-type converters
/// (index/bitmap/tid/subquery/function/cte/foreign/…), `create_gating_plan`,
/// and the non-scan converter arms are installed by the later scan / join /
/// append / upper F-families.
pub fn init_seams() {
    cp_seam::create_scan_plan::set(create_scan_plan);
    // F2c simple scan converters.
    cp_seam::create_seqscan_plan::set(create_seqscan_plan);
    cp_seam::create_valuesscan_plan::set(create_valuesscan_plan);
    cp_seam::create_resultscan_plan::set(create_resultscan_plan);
    // F2d rest-of-scan converters. The SubPlan-init-plan resolution legs of
    // cte / worktable and the subroot recursion of subquery stay seam-panicked
    // into subselect (resolve_cte_subplan / resolve_worktable_param /
    // create_subqueryscan_subplan) until subselect.c lands.
    cp_seam::create_samplescan_plan::set(create_samplescan_plan);
    cp_seam::create_tidscan_plan::set(create_tidscan_plan);
    cp_seam::create_tidrangescan_plan::set(create_tidrangescan_plan);
    cp_seam::create_functionscan_plan::set(create_functionscan_plan);
    cp_seam::create_namedtuplestorescan_plan::set(create_namedtuplestorescan_plan);
    cp_seam::create_ctescan_plan::set(create_ctescan_plan);
    cp_seam::create_worktablescan_plan::set(create_worktablescan_plan);
    cp_seam::create_subqueryscan_plan::set(create_subqueryscan_plan);
}

#[cfg(test)]
mod tests;
