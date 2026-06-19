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

use mcx::{vec_with_capacity_in, Mcx, PgBox, PgVec};
use types_error::{PgError, PgResult};
use types_nodes::nodeindexscan::{Plan, Scan};
use types_nodes::noderesult::Result as ResultNode;
use types_nodes::nodeforeigncustom::Material as MaterialNode;
use types_nodes::nodesort::Sort;
use types_nodes::nodelimit::Limit as LimitNode;
use types_nodes::nodeprojectset::ProjectSet as ProjectSetNode;
use types_nodes::nodes::{ntag, Node, NodeTag};
use types_nodes::nodectescan::CteScan;
use types_nodes::nodefunctionscan::FunctionScan;
use types_nodes::nodeindexscan::{SubqueryScan, SubqueryScanStatus, TidScan};
use types_nodes::nodenamedtuplestorescan::NamedTuplestoreScan;
use types_nodes::nodesamplescan::{SampleScan, TableSampleClause};
use types_nodes::nodeseqscan::SeqScan;
use types_nodes::nodeindexscan::IndexScan;
use types_nodes::nodeindexonlyscan::IndexOnlyScan;
use types_nodes::nodetidrangescan::TidRangeScan;
use types_nodes::nodevaluesscan::ValuesScan;
use types_nodes::nodeworktablescan::WorkTableScan;
use types_nodes::primnodes::{Expr, OpExpr, TargetEntry};
use types_nodes::jointype::{Join as JoinBase, JoinType as NodeJoinType};
use types_nodes::nodenestloop::{NestLoop, NestLoopParam};
use types_nodes::nodehashjoin::{Hash as HashNode, HashJoin};
use types_nodes::nodemergejoin::MergeJoin;
use types_nodes::nodeappend::Append as AppendNode;
use types_nodes::nodemergeappend::MergeAppend as MergeAppendNode;
use types_nodes::bitmapset::Bitmapset;
use types_nodes::nodegroup::Group as GroupNode;
use types_nodes::nodeunique::Unique as UniqueNode;
use types_nodes::nodesetop::SetOp as SetOpNode;
use types_nodes::noderecursiveunion::RecursiveUnion as RecursiveUnionNode;
use types_nodes::nodegather::Gather as GatherNode;
use types_nodes::nodegathermerge::GatherMerge as GatherMergeNode;
use types_nodes::nodeincrementalsort::IncrementalSort as IncrementalSortNode;
use types_nodes::rawnodes::RangeTblFunction;
use mcx::PgString;
use types_pathnodes::planner_run::{planner_rt_fetch, PlannerRun};
use types_pathnodes::{
    EcId, IndexOptInfo, MaterialPath, NodeId, Path, PathId, PathKey, PathNode, PathTarget,
    PlannerInfo, RelId, Relids, RinfoId,
    RELOPT_BASEREL, RELOPT_OTHER_MEMBER_REL, RTE_RELATION,
    UNIQUE_PATH_HASH, UNIQUE_PATH_NOOP, UNIQUE_PATH_SORT,
};
use types_nodes::nodebitmapand::BitmapAnd;
use types_nodes::nodebitmapheapscan::BitmapHeapScan;
use types_nodes::nodebitmapindexscan::BitmapIndexScan;
use types_nodes::nodebitmapor::BitmapOr;
use types_nodes::nodeagg::AGGSPLIT_SIMPLE;
use types_core::primitive::{AttrNumber, Index, InvalidOid, Oid};
use types_tuple::access::RELKIND_FOREIGN_TABLE;

use backend_nodes_core::makefuncs::{
    make_ands_explicit, make_bool_const, make_orclause, make_target_entry,
};
use backend_nodes_core::nodefuncs::expression_tree_mutator;
use backend_nodes_equalfuncs_seams::equal_expr as equal_expr_seam;
use backend_optimizer_path_equivclass_seams as equivclass;
use backend_optimizer_plan_createplan_seams as cp_seam;
use backend_optimizer_util_joininfo::restrictinfo::{
    extract_actual_clauses, extract_actual_join_clauses, get_actual_clauses,
};
use backend_optimizer_util_clauses::grounded::CommuteOpExpr;
use backend_optimizer_util_paramassign_seams as paramassign;
use backend_optimizer_util_pathnode_seams as pathnode;
use backend_optimizer_util_placeholder_seams as placeholder;
use backend_optimizer_util_plancat::build_physical_tlist;
use backend_optimizer_util_relnode_seams as relnode;
use backend_optimizer_util_vars::tlist::{
    apply_pathtarget_labeling_to_tlist, get_sortgroupref_tle, tlist_same_exprs,
};
use backend_optimizer_util_vars::tlist as util_tlist;
use backend_optimizer_path_costsize_seams as costsize;
use backend_partitioning_partprune_seams as partprune;
use backend_optimizer_path_equivclass::{find_computable_ec_member, find_ec_member_matching_expr};
use backend_utils_cache_lsyscache_seams as lsyscache;
use backend_utils_misc_guc_tables::vars;
// fix_indexqual_operand (createplan.c, homed in var.c crate) — index-column Var
// substitution for index quals.
use backend_optimizer_util_vars::fix_indexqual_operand;
// is_redundant_with_indexclauses (equivclass.c) — the real impl, over &[IndexClause].
use backend_optimizer_path_equivclass::is_redundant_with_indexclauses;
// predicate_implied_by (predtest.c) — prove a scan clause is implied by indexquals.
use backend_optimizer_util_predtest_seams as predtest;
// contain_mutable_functions (clauses.c) — guard the predicate_implied_by check.
use backend_optimizer_util_clauses::grounded::contain_mutable_functions;

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
/// `COMPARE_EQ` (`access/cmptype.h`) — the equality compare type (= 3).
const COMPARE_EQ: types_pathnodes::CompareType = 3;
/// `COMPARE_GT` (`access/cmptype.h`) — the greater-than compare type (= 5).
/// A mergeclause's outer pathkey sorted `COMPARE_GT` means the executor must
/// reverse the comparison (`mergeReversals[i] = true`).
const COMPARE_GT: types_pathnodes::CompareType = 5;

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

/// `IS_SPECIAL_VARNO(varno)` (primnodes.h) — `((int) (varno) < 0)`. The special
/// varnos (INNER_VAR/OUTER_VAR/INDEX_VAR/ROWID_VAR) are the C negative sentinels
/// (-1..-4); real range-table indices are >= 1.
#[inline]
fn is_special_varno(varno: i32) -> bool {
    varno < 0
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
    if let Some(var) = node.as_var() {
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
    if let Some(phv) = node.as_placeholdervar() {
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
        //
        // Materialize an owned `Expr` for the TargetEntry. The arena node may be
        // an `Aggref` (e.g. the count(*) tlist), whose `Clone` is a deliberate
        // panic-stub forcing the args-deep-copying `clone_in` path; route every
        // tlist expr through `Expr::clone_in` so the deep arena copy is used
        // (faithful to C's `copyObject` of the pathtarget expr).
        let expr: Expr = if has_param_info {
            let replaced = replace_nestloop_params(mcx, root, node_id)?;
            root.node(replaced).clone_in(mcx)?
        } else {
            root.node(node_id).clone_in(mcx)?
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
                    if let Some(var) = root.node(expr_id).as_var() {
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
            let phys: Vec<NodeId> = build_physical_tlist(run, root, rel_id)?;
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
    // Scan converters live in this crate — direct calls. The index/bitmap/
    // tablefunc/foreign/custom converters are NOT installed here (unported or
    // owned by another unit), so they stay `cp_seam::*` seam calls.
    let plan: Node<'mcx> = match pathtype {
        t if t == T_SeqScan => {
            create_seqscan_plan(mcx, root, run, best_path, tlist, scan_clauses)?
        }
        t if t == T_SampleScan => {
            create_samplescan_plan(mcx, root, run, best_path, tlist, scan_clauses)?
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
            create_tidscan_plan(mcx, root, run, best_path, tlist, scan_clauses)?
        }
        t if t == T_TidRangeScan => {
            create_tidrangescan_plan(mcx, root, run, best_path, tlist, scan_clauses)?
        }
        t if t == T_SubqueryScan => {
            create_subqueryscan_plan(mcx, root, run, best_path, tlist, scan_clauses)?
        }
        t if t == T_FunctionScan => {
            create_functionscan_plan(mcx, root, run, best_path, tlist, scan_clauses)?
        }
        t if t == T_TableFuncScan => cp_seam::create_tablefuncscan_plan::call(
            mcx, root, run, best_path, tlist, scan_clauses,
        )?,
        t if t == T_ValuesScan => {
            create_valuesscan_plan(mcx, root, run, best_path, tlist, scan_clauses)?
        }
        t if t == T_CteScan => {
            create_ctescan_plan(mcx, root, run, best_path, tlist, scan_clauses)?
        }
        t if t == T_NamedTuplestoreScan => {
            create_namedtuplestorescan_plan(mcx, root, run, best_path, tlist, scan_clauses)?
        }
        t if t == T_Result => {
            create_resultscan_plan(mcx, root, run, best_path, tlist, scan_clauses)?
        }
        t if t == T_WorkTableScan => {
            create_worktablescan_plan(mcx, root, run, best_path, tlist, scan_clauses)?
        }
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
        // create_gating_plan builds the gating Result via make_result; direct
        // call (same crate).
        create_gating_plan(mcx, root, best_path, plan, gating_clauses)
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
        // clone_in: the target expr may be an Aggref whose context-allocated
        // TargetEntry args a bare derived `.clone()` cannot copy.
        let expr = root.node(te.expr).clone_in(mcx)?;
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
    if !plan.is_modifytable() {
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

    // The per-family converters below are direct calls. The dispatch and every
    // converter live in this crate, so no `create_*_plan` indirection seam is
    // needed (createplan.c is a single translation unit). The remaining
    // `cp_seam::*` calls are genuine cross-/forward-boundary seams whose owner is
    // unported (minmaxagg/memoize/windowagg/modifytable/lockrows) or installed by
    // another crate.
    match pathtype {
        T_SeqScan | T_SampleScan | T_IndexScan | T_IndexOnlyScan | T_BitmapHeapScan | T_TidScan
        | T_TidRangeScan | T_SubqueryScan | T_FunctionScan | T_TableFuncScan | T_ValuesScan
        | T_CteScan | T_WorkTableScan | T_NamedTuplestoreScan | T_ForeignScan | T_CustomScan => {
            create_scan_plan(mcx, root, run, best_path, flags)
        }
        T_HashJoin | T_MergeJoin | T_NestLoop => create_join_plan(mcx, root, run, best_path),
        T_Append => create_append_plan(mcx, root, run, best_path, flags),
        T_MergeAppend => create_merge_append_plan(mcx, root, run, best_path, flags),
        T_Result => {
            // IsA(best_path, ProjectionPath) / MinMaxAggPath / GroupResultPath /
            // else simple RTE_RESULT base relation (Path).
            match root.path(best_path) {
                PathNode::ProjectionPath(_) => {
                    create_projection_plan(mcx, root, run, best_path, flags)
                }
                PathNode::MinMaxAggPath(_) => {
                    // create_minmaxagg_plan is not yet ported (MinMaxAggInfo
                    // subplan carrier); stays a forward seam-panic.
                    cp_seam::create_minmaxagg_plan::call(mcx, root, run, best_path)
                }
                PathNode::GroupResultPath(_) => {
                    create_group_result_plan(mcx, root, run, best_path)
                }
                // Simple RTE_RESULT base relation — Assert(IsA(best_path, Path)).
                _ => create_scan_plan(mcx, root, run, best_path, flags),
            }
        }
        T_ProjectSet => create_project_set_plan(mcx, root, run, best_path),
        T_Material => create_material_plan(mcx, root, run, best_path, flags),
        // create_memoize_plan is not yet ported; stays a forward seam-panic.
        T_Memoize => cp_seam::create_memoize_plan::call(mcx, root, run, best_path, flags),
        // IsA(best_path, UpperUniquePath) vs UniquePath — the sub-discrimination
        // is internal to the Unique family; route the whole T_Unique pathtype.
        T_Unique => create_unique_dispatch_plan(mcx, root, run, best_path, flags),
        T_Gather => create_gather_plan(mcx, root, run, best_path),
        T_Sort => create_sort_plan(mcx, root, run, best_path, flags),
        T_IncrementalSort => create_incrementalsort_plan(mcx, root, run, best_path, flags),
        T_Group => create_group_plan(mcx, root, run, best_path),
        // IsA(best_path, GroupingSetsPath) vs AggPath — internal to the Agg
        // family; route the whole T_Agg pathtype.
        T_Agg => create_agg_dispatch_plan(mcx, root, run, best_path),
        T_WindowAgg => create_windowagg_plan(mcx, root, run, best_path),
        T_SetOp => create_setop_plan(mcx, root, run, best_path, flags),
        T_RecursiveUnion => create_recursiveunion_plan(mcx, root, run, best_path),
        // create_lockrows_plan is not yet ported (PlanRowMark carrier gap); seam.
        T_LockRows => cp_seam::create_lockrows_plan::call(mcx, root, run, best_path, flags),
        T_ModifyTable => create_modifytable_plan(mcx, root, run, best_path),
        T_Limit => create_limit_plan(mcx, root, run, best_path, flags),
        T_GatherMerge => create_gather_merge_plan(mcx, root, run, best_path),
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

    Ok(Node::mk_seq_scan(mcx, scan_plan))
}

// ---------------------------------------------------------------------------
// fix_indexqual_clause / fix_indexqual_references / fix_indexorderby_references
// (createplan.c ~5121-5260)
// ---------------------------------------------------------------------------

/// `fix_indexqual_clause(root, index, indexcol, clause, indexcolnos)`
/// (createplan.c) — convert a single indexqual clause to the form needed by the
/// executor: replace nestloop params (which also copies the clause, so it's safe
/// to mutate in place) and replace the index key variable(s)/expression(s) with
/// index `Var` nodes.
///
/// `clause` is an owned `Expr` (the C `Node *`; here resolved from the
/// `RestrictInfo->clause` arena handle before the call). The mutation matches
/// each handled node shape (`OpExpr`/`RowCompareExpr`/`ScalarArrayOpExpr`/
/// `NullTest`).
fn fix_indexqual_clause(
    mcx: Mcx<'_>,
    root: &mut PlannerInfo,
    index: &IndexOptInfo,
    indexcol: i32,
    clause: Expr,
    indexcolnos: &[AttrNumber],
) -> PgResult<Expr> {
    // Replace any outer-relation variables with nestloop params. This also
    // makes a copy of the clause, so it's safe to modify it in place below.
    let mut err: Option<PgError> = None;
    let mut clause = replace_nestloop_params_expr(mcx, root, clause, &mut err);
    if let Some(e) = err {
        return Err(e);
    }

    match &mut clause {
        Expr::OpExpr(op) => {
            // Replace the indexkey expression with an index Var.
            let arg0 = op.args.remove(0);
            let fixed = fix_indexqual_operand(root, arg0, index, indexcol)?;
            op.args.insert(0, fixed);
        }
        Expr::RowCompareExpr(rc) => {
            // Replace the indexkey expressions with index Vars.
            debug_assert_eq!(rc.largs.len(), indexcolnos.len());
            let largs = core::mem::take(&mut rc.largs);
            let mut new_largs = Vec::with_capacity(largs.len());
            for (arg, &col) in largs.into_iter().zip(indexcolnos.iter()) {
                new_largs.push(fix_indexqual_operand(root, arg, index, col as i32)?);
            }
            rc.largs = new_largs;
        }
        Expr::ScalarArrayOpExpr(saop) => {
            // Replace the indexkey expression with an index Var.
            let arg0 = saop.args.remove(0);
            let fixed = fix_indexqual_operand(root, arg0, index, indexcol)?;
            saop.args.insert(0, fixed);
        }
        Expr::NullTest(nt) => {
            // Replace the indexkey expression with an index Var.
            let arg = *nt
                .arg
                .take()
                .expect("fix_indexqual_clause: NullTest.arg must be set");
            let fixed = fix_indexqual_operand(root, arg, index, indexcol)?;
            nt.arg = Some(alloc::boxed::Box::new(fixed));
        }
        _ => return Err(PgError::error("unsupported indexqual type")),
    }

    Ok(clause)
}

/// `fix_indexqual_references(root, index_path, &stripped, &fixed)` (createplan.c)
/// — extract the index qual expressions (stripped of RestrictInfos) from the
/// `IndexClauses` list, and prepare a copy with index Vars substituted for table
/// Vars. Returns `(stripped_indexquals, fixed_indexquals)`.
///
/// `stripped_indexquals` are kept as arena [`NodeId`] handles (the bare
/// `RestrictInfo->clause`), so they can drive `predicate_implied_by` and
/// `replace_nestloop_params` later; `fixed_indexquals` are owned `Expr`.
fn fix_indexqual_references(
    mcx: Mcx<'_>,
    root: &mut PlannerInfo,
    best_path: PathId,
) -> PgResult<(Vec<NodeId>, Vec<Expr>)> {
    // index = index_path->indexinfo (cloned out so we can mutate `root`).
    let index = match root.path(best_path) {
        PathNode::IndexPath(p) => p
            .indexinfo
            .as_deref()
            .expect("fix_indexqual_references: IndexPath has no indexinfo")
            .clone(),
        _ => {
            return Err(PgError::error(
                "fix_indexqual_references: best_path is not an IndexPath",
            ))
        }
    };
    let indexclauses = match root.path(best_path) {
        PathNode::IndexPath(p) => p.indexclauses.clone(),
        _ => unreachable!(),
    };

    let mut stripped_indexquals: Vec<NodeId> = Vec::new();
    let mut fixed_indexquals: Vec<Expr> = Vec::new();

    for iclause in &indexclauses {
        let indexcol = iclause.indexcol as i32;
        for &rinfo in &iclause.indexquals {
            let clause_id = root.rinfo(rinfo).clause;
            stripped_indexquals.push(clause_id);
            let clause = root.node(clause_id).clone();
            let fixed = fix_indexqual_clause(
                mcx,
                root,
                &index,
                indexcol,
                clause,
                &iclause.indexcols,
            )?;
            fixed_indexquals.push(fixed);
        }
    }

    Ok((stripped_indexquals, fixed_indexquals))
}

/// `fix_indexorderby_references(root, index_path)` (createplan.c) — adjust
/// indexorderby clauses to the executor form. A simplified version of
/// `fix_indexqual_references`: the input is bare clauses (the path's
/// `indexorderbys`) plus a separate `indexorderbycols` list.
fn fix_indexorderby_references(
    mcx: Mcx<'_>,
    root: &mut PlannerInfo,
    best_path: PathId,
) -> PgResult<Vec<Expr>> {
    let index = match root.path(best_path) {
        PathNode::IndexPath(p) => p
            .indexinfo
            .as_deref()
            .expect("fix_indexorderby_references: IndexPath has no indexinfo")
            .clone(),
        _ => {
            return Err(PgError::error(
                "fix_indexorderby_references: best_path is not an IndexPath",
            ))
        }
    };
    let (indexorderbys, indexorderbycols) = match root.path(best_path) {
        PathNode::IndexPath(p) => (p.indexorderbys.clone(), p.indexorderbycols.clone()),
        _ => unreachable!(),
    };

    let mut fixed_indexorderbys: Vec<Expr> = Vec::new();
    for (&clause_id, &indexcol) in indexorderbys.iter().zip(indexorderbycols.iter()) {
        let clause = root.node(clause_id).clone();
        // fix_indexqual_clause with indexcolnos = NIL.
        let fixed = fix_indexqual_clause(mcx, root, &index, indexcol, clause, &[])?;
        fixed_indexorderbys.push(fixed);
    }

    Ok(fixed_indexorderbys)
}

/// `make_indexscan(...)` (createplan.c) — build an `IndexScan` plan node.
fn make_indexscan<'mcx>(
    qptlist: Option<PgVec<'mcx, TargetEntry<'mcx>>>,
    qpqual: Option<PgVec<'mcx, Expr>>,
    scanrelid: u32,
    indexid: Oid,
    indexqual: Option<PgVec<'mcx, Expr>>,
    indexqualorig: Option<PgVec<'mcx, Expr>>,
    indexorderby: Option<PgVec<'mcx, Expr>>,
    indexorderbyorig: Option<PgVec<'mcx, Expr>>,
    indexorderbyops: Option<PgVec<'mcx, Oid>>,
    indexscandir: types_scan::sdir::ScanDirection,
) -> IndexScan<'mcx> {
    let mut node = IndexScan {
        scan: Scan::default(),
        indexid,
        indexqual,
        indexqualorig,
        indexorderby,
        indexorderbyorig,
        indexorderbyops,
        indexorderdir: indexscandir,
    };
    let plan: &mut Plan = &mut node.scan.plan;
    plan.targetlist = qptlist;
    plan.qual = qpqual;
    plan.lefttree = None;
    plan.righttree = None;
    node.scan.scanrelid = scanrelid;
    node
}

/// `make_indexonlyscan(...)` (createplan.c) — build an `IndexOnlyScan` plan node.
fn make_indexonlyscan<'mcx>(
    qptlist: Option<PgVec<'mcx, TargetEntry<'mcx>>>,
    qpqual: Option<PgVec<'mcx, Expr>>,
    scanrelid: u32,
    indexid: Oid,
    indexqual: Option<PgVec<'mcx, Expr>>,
    recheckqual: Option<PgVec<'mcx, Expr>>,
    indexorderby: Option<PgVec<'mcx, Expr>>,
    indextlist: Option<PgVec<'mcx, TargetEntry<'mcx>>>,
    indexscandir: types_scan::sdir::ScanDirection,
) -> IndexOnlyScan<'mcx> {
    let mut node = IndexOnlyScan {
        scan: Scan::default(),
        indexid,
        indexqual,
        recheckqual,
        indexorderby,
        indextlist,
        indexorderdir: indexscandir,
    };
    let plan: &mut Plan = &mut node.scan.plan;
    plan.targetlist = qptlist;
    plan.qual = qpqual;
    plan.lefttree = None;
    plan.righttree = None;
    node.scan.scanrelid = scanrelid;
    node
}

/// Convert the path's `indexscandir` (`i32`, types-pathnodes) into the
/// `ScanDirection` enum the plan node carries.
fn scan_direction_from_i32(dir: i32) -> types_scan::sdir::ScanDirection {
    use types_scan::sdir::ScanDirection;
    match dir {
        -1 => ScanDirection::BackwardScanDirection,
        1 => ScanDirection::ForwardScanDirection,
        _ => ScanDirection::NoMovementScanDirection,
    }
}

/// Move an owned `Vec<Expr>` into the plan-node `Option<PgVec<Expr>>` field
/// (empty list = `None`, the C `NIL`).
fn expr_vec_to_field<'mcx>(
    mcx: Mcx<'mcx>,
    v: Vec<Expr>,
) -> PgResult<Option<PgVec<'mcx, Expr>>> {
    if v.is_empty() {
        return Ok(None);
    }
    let mut out = vec_with_capacity_in(mcx, v.len())?;
    for e in v {
        out.push(e);
    }
    Ok(Some(out))
}

// ---------------------------------------------------------------------------
// create_indexscan_plan (createplan.c ~2999)
// ---------------------------------------------------------------------------

/// `create_indexscan_plan(root, best_path, tlist, scan_clauses, indexonly)`
/// (createplan.c) — return an indexscan plan for the base relation scanned by
/// `best_path` with restriction clauses `scan_clauses` and targetlist `tlist`.
/// Covers both `IndexScan` (`indexonly = false`) and `IndexOnlyScan`
/// (`indexonly = true`); the `BitmapIndexScan` path is a separate converter.
fn create_indexscan_plan<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    best_path: PathId,
    tlist: Vec<TargetEntry<'mcx>>,
    scan_clauses: Vec<RinfoId>,
    indexonly: bool,
) -> PgResult<Node<'mcx>> {
    // Snapshot the immutable path fields we need (the IndexPath / IndexOptInfo)
    // before we start mutating `root` (arena rewrites in the fixups).
    let (indexclauses, indexoid, indexscandir, has_param_info, baserelid) =
        match root.path(best_path) {
            PathNode::IndexPath(p) => {
                let indexinfo = p
                    .indexinfo
                    .as_deref()
                    .expect("create_indexscan_plan: IndexPath has no indexinfo");
                (
                    p.indexclauses.clone(),
                    indexinfo.indexoid,
                    p.indexscandir,
                    p.path.param_info.is_some(),
                    p.path.parent,
                )
            }
            _ => {
                return Err(PgError::error(
                    "create_indexscan_plan: best_path is not an IndexPath",
                ))
            }
        };

    let scan_relid = root.rel(baserelid).relid;

    // it should be a base rel...
    debug_assert!(scan_relid > 0);
    debug_assert_eq!(
        planner_rt_fetch(run, root, scan_relid).rtekind,
        types_nodes::parsenodes::RTEKind::RTE_RELATION
    );
    // check the scan direction is valid
    debug_assert!(indexscandir == 1 || indexscandir == -1);

    // Extract the index qual expressions (stripped of RestrictInfos) from the
    // IndexClauses list, and prepare a copy with index Vars substituted for
    // table Vars. (This step also does replace_nestloop_params on the
    // fixed_indexquals.)
    let (mut stripped_indexquals, fixed_indexquals) =
        fix_indexqual_references(mcx, root, best_path)?;

    // Likewise fix up index attr references in the ORDER BY expressions.
    let fixed_indexorderbys = fix_indexorderby_references(mcx, root, best_path)?;

    // The qpqual list must contain all restrictions not automatically handled
    // by the index, other than pseudoconstant clauses which will be handled by
    // a separate gating plan node, and clauses that are redundant with or
    // provably implied by the indexquals.
    let mut qpqual: Vec<RinfoId> = Vec::new();
    for &rinfo in &scan_clauses {
        if root.rinfo(rinfo).pseudoconstant {
            continue; // we may drop pseudoconstants here
        }
        if is_redundant_with_indexclauses(root, rinfo, &indexclauses) {
            continue; // dup or derived from same EquivalenceClass
        }
        // !contain_mutable_functions(rinfo->clause) &&
        //     predicate_implied_by(list_make1(rinfo->clause),
        //                          stripped_indexquals, false)
        let clause_id = root.rinfo(rinfo).clause;
        let clause_expr = root.node(clause_id).clone();
        if !contain_mutable_functions(Some(&clause_expr))? {
            let single = [clause_id];
            if predtest::predicate_implied_by::call(root, &single, &stripped_indexquals, false) {
                continue; // provably implied by indexquals
            }
        }
        qpqual.push(rinfo);
    }

    // Sort clauses into best execution order.
    let qpqual = order_qual_clauses_rinfo(root, &qpqual);
    // Reduce RestrictInfo list to bare expressions; ignore pseudoconstants.
    let qpqual = extract_actual_clauses(root, &qpqual, false);

    // We have to replace any outer-relation variables with nestloop params in
    // the indexqualorig, qpqual, and indexorderbyorig expressions. (A bit
    // annoying to have to do this separately from fix_indexqual_references.)
    //
    // `indexorderbys` is the path's original (unfixed) ORDER BY clause list
    // (bare node handles), used for `indexorderbyorig` on the plan node.
    let indexorderbys_orig: Vec<NodeId> = match root.path(best_path) {
        PathNode::IndexPath(p) => p.indexorderbys.clone(),
        _ => unreachable!(),
    };
    let (qpqual_nodes, indexorderbys_orig) = if has_param_info {
        stripped_indexquals = replace_nestloop_params_list(mcx, root, &stripped_indexquals)?;
        let qpqual_nodes = replace_nestloop_params_list(mcx, root, &qpqual)?;
        let indexorderbys_orig =
            replace_nestloop_params_list(mcx, root, &indexorderbys_orig)?;
        (qpqual_nodes, indexorderbys_orig)
    } else {
        (qpqual, indexorderbys_orig)
    };

    // If there are ORDER BY expressions, look up the sort operators for their
    // result datatypes.
    let mut indexorderbyops: Vec<Oid> = Vec::new();
    if !indexorderbys_orig.is_empty() {
        let pathkeys = match root.path(best_path) {
            PathNode::IndexPath(p) => p.path.pathkeys.clone(),
            _ => unreachable!(),
        };
        // Assert(list_length(pathkeys) == list_length(indexorderbys)).
        debug_assert_eq!(pathkeys.len(), indexorderbys_orig.len());
        for (pathkey, &expr_id) in pathkeys.iter().zip(indexorderbys_orig.iter()) {
            let expr = root.node(expr_id).clone();
            let exprtype = backend_nodes_core::nodefuncs::expr_type(Some(&expr))?;
            // Get sort operator from opfamily.
            let sortop = lsyscache::get_opfamily_member_for_cmptype::call(
                pathkey.pk_opfamily,
                exprtype,
                exprtype,
                pathkey.pk_cmptype,
            )?;
            if sortop == InvalidOid {
                return Err(PgError::error(alloc::format!(
                    "missing operator {}({},{}) in opfamily {}",
                    pathkey.pk_cmptype, exprtype, exprtype, pathkey.pk_opfamily
                )));
            }
            indexorderbyops.push(sortop);
        }
    }

    // For an index-only scan, mark indextlist entries as resjunk if they are
    // columns the index AM can't return. The owned indextlist for the plan node
    // is rebuilt from the IndexOptInfo with that resjunk applied.
    let indextlist_field: Option<PgVec<'mcx, TargetEntry<'mcx>>> = if indexonly {
        let (indextlist_ids, canreturn) = match root.path(best_path) {
            PathNode::IndexPath(p) => {
                let ii = p
                    .indexinfo
                    .as_deref()
                    .expect("create_indexscan_plan: IndexPath has no indexinfo");
                (ii.indextlist.clone(), ii.canreturn.clone())
            }
            _ => unreachable!(),
        };
        let mut tlist = resolve_targetentry_list(mcx, root, &indextlist_ids)?;
        for (i, tle) in tlist.iter_mut().enumerate() {
            tle.resjunk = !canreturn[i];
        }
        if tlist.is_empty() {
            None
        } else {
            let mut out = vec_with_capacity_in(mcx, tlist.len())?;
            for tle in tlist {
                out.push(tle);
            }
            Some(out)
        }
    } else {
        None
    };

    // Resolve owned-Expr field carriers.
    let tlist_field = tlist_to_plan_field(mcx, tlist)?;
    let qpqual_field = build_node_list_to_expr_field(mcx, root, &qpqual_nodes)?;
    let fixed_indexquals_field = expr_vec_to_field(mcx, fixed_indexquals)?;
    let stripped_field = build_node_list_to_expr_field(mcx, root, &stripped_indexquals)?;
    let fixed_indexorderbys_field = expr_vec_to_field(mcx, fixed_indexorderbys)?;
    let dir = scan_direction_from_i32(indexscandir);

    let plan: Node<'mcx> = if indexonly {
        let mut scan_plan = make_indexonlyscan(
            tlist_field,
            qpqual_field,
            scan_relid,
            indexoid,
            fixed_indexquals_field,
            stripped_field, // recheckqual
            fixed_indexorderbys_field,
            indextlist_field,
            dir,
        );
        copy_generic_path_info(&mut scan_plan.scan.plan, root.path(best_path).base());
        Node::mk_index_only_scan(mcx, scan_plan)
    } else {
        let indexorderbyorig_field =
            build_node_list_to_expr_field(mcx, root, &indexorderbys_orig)?;
        let indexorderbyops_field = oid_vec_opt(mcx, indexorderbyops)?;
        let mut scan_plan = make_indexscan(
            tlist_field,
            qpqual_field,
            scan_relid,
            indexoid,
            fixed_indexquals_field,
            stripped_field, // indexqualorig
            fixed_indexorderbys_field,
            indexorderbyorig_field,
            indexorderbyops_field,
            dir,
        );
        copy_generic_path_info(&mut scan_plan.scan.plan, root.path(best_path).base());
        Node::mk_index_scan(mcx, scan_plan)
    };

    Ok(plan)
}

// ---------------------------------------------------------------------------
// create_bitmap_scan_plan / create_bitmap_subplan / bitmap_subplan_mark_shared
// (createplan.c ~3190). Build a BitmapHeapScan over the Plan tree produced by
// recursively converting the BitmapHeapPath's `bitmapqual` (an IndexPath,
// BitmapAndPath, or BitmapOrPath) into BitmapIndexScan / BitmapAnd / BitmapOr
// nodes.
// ---------------------------------------------------------------------------

/// `create_bitmap_scan_plan(root, (BitmapHeapPath *) best_path, tlist,
/// scan_clauses)` — returns a bitmap scan plan for the base relation scanned by
/// `best_path` with restriction clauses `scan_clauses` and targetlist `tlist`.
fn create_bitmap_scan_plan<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    best_path: PathId,
    tlist: Vec<TargetEntry<'mcx>>,
    scan_clauses: Vec<RinfoId>,
) -> PgResult<Node<'mcx>> {
    let (bitmapqual, parallel_aware, has_param_info, parent_relid) = match root.path(best_path) {
        PathNode::BitmapHeapPath(p) => (
            p.bitmapqual
                .expect("create_bitmap_scan_plan: BitmapHeapPath has no bitmapqual"),
            p.path.parallel_aware,
            p.path.param_info.is_some(),
            p.path.parent,
        ),
        _ => {
            return Err(PgError::error(
                "create_bitmap_scan_plan: best_path is not a BitmapHeapPath",
            ))
        }
    };

    let baserelid = root.rel(parent_relid).relid;

    // it should be a base rel...
    debug_assert!(baserelid > 0);
    debug_assert_eq!(
        planner_rt_fetch(run, root, baserelid).rtekind,
        types_nodes::parsenodes::RTEKind::RTE_RELATION
    );

    // Process the bitmapqual tree into a Plan tree and qual lists.
    let mut bitmapqualplan = create_bitmap_subplan(mcx, root, run, bitmapqual)?;
    let mut bitmapqualorig = bitmapqualplan.qual; // bare expr arena handles
    let indexquals = bitmapqualplan.indexqual; // bare expr arena handles
    let index_ecs = bitmapqualplan.index_ecs; // EquivalenceClass ids

    if parallel_aware {
        bitmap_subplan_mark_shared(&mut bitmapqualplan.plan)?;
    }

    // The qpqual list must contain all restrictions not automatically handled
    // by the index, other than pseudoconstant clauses which will be handled by
    // a separate gating plan node. qpqual must contain scan_clauses minus
    // whatever appears in indexquals.
    let mut qpqual: Vec<RinfoId> = Vec::new();
    for &rinfo in &scan_clauses {
        let ri = root.rinfo(rinfo);
        if ri.pseudoconstant {
            continue; // we may drop pseudoconstants here
        }
        let clause_id = ri.clause;
        // list_member(indexquals, clause): equal() over the stripped indexquals.
        let clause_expr = root.node(clause_id).clone();
        if indexquals
            .iter()
            .any(|&iq| equal_expr_seam::call(root.node(iq), &clause_expr))
        {
            continue; // simple duplicate
        }
        // rinfo->parent_ec && list_member_ptr(indexECs, rinfo->parent_ec)
        if let Some(pec) = root.rinfo(rinfo).parent_ec {
            if index_ecs.iter().any(|&ec| ec == pec) {
                continue; // derived from same EquivalenceClass
            }
        }
        // !contain_mutable_functions(clause) &&
        //     predicate_implied_by(list_make1(clause), indexquals, false)
        if !contain_mutable_functions(Some(&clause_expr))? {
            let single = [clause_id];
            if predtest::predicate_implied_by::call(root, &single, &indexquals, false) {
                continue; // provably implied by indexquals
            }
        }
        qpqual.push(rinfo);
    }

    // Sort clauses into best execution order.
    let qpqual = order_qual_clauses_rinfo(root, &qpqual);
    // Reduce RestrictInfo list to bare expressions; ignore pseudoconstants.
    let qpqual = extract_actual_clauses(root, &qpqual, false);

    // When dealing with special operators, we'll have duplicate clauses in
    // qpqual and bitmapqualorig; drop them from bitmapqualorig.
    // list_difference_ptr(bitmapqualorig, qpqual) — identity over arena handles.
    bitmapqualorig.retain(|&b| !qpqual.iter().any(|&q| q == b));

    // We have to replace any outer-relation variables with nestloop params in
    // the qpqual and bitmapqualorig expressions.
    let (qpqual, bitmapqualorig) = if has_param_info {
        let qpqual = replace_nestloop_params_list(mcx, root, &qpqual)?;
        let bitmapqualorig = replace_nestloop_params_list(mcx, root, &bitmapqualorig)?;
        (qpqual, bitmapqualorig)
    } else {
        (qpqual, bitmapqualorig)
    };

    // Resolve owned-Expr field carriers.
    let tlist_field = tlist_to_plan_field(mcx, tlist)?;
    let qpqual_field = build_node_list_to_expr_field(mcx, root, &qpqual)?;
    let bitmapqualorig_field = node_list_to_expr_vec(mcx, root, &bitmapqualorig)?;

    let mut scan_plan = BitmapHeapScan {
        scan: Scan::default(),
        bitmapqualorig: bitmapqualorig_field,
    };
    {
        let plan: &mut Plan = &mut scan_plan.scan.plan;
        plan.targetlist = tlist_field;
        plan.qual = qpqual_field;
        plan.lefttree = Some(mcx::alloc_in(mcx, bitmapqualplan.plan)?);
        plan.righttree = None;
    }
    scan_plan.scan.scanrelid = baserelid;

    copy_generic_path_info(&mut scan_plan.scan.plan, root.path(best_path).base());

    Ok(Node::mk_bitmap_heap_scan(mcx, scan_plan))
}

/// The byproducts `create_bitmap_subplan` returns alongside the Plan tree:
/// `qual`/`indexqual` (bare-expression arena-handle lists, implicit-AND form)
/// and `indexECs` (EquivalenceClass ids) — the C out-parameters.
struct BitmapSubplan<'mcx> {
    plan: Node<'mcx>,
    qual: Vec<NodeId>,
    indexqual: Vec<NodeId>,
    index_ecs: Vec<EcId>,
}

/// `create_bitmap_subplan(root, bitmapqual, &qual, &indexqual, &indexECs)` —
/// given a bitmapqual path tree, generate the Plan tree that implements it,
/// returning the original-condition and generated-indexqual lists plus the
/// EquivalenceClass list for the top-level indexquals.
fn create_bitmap_subplan<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    bitmapqual: PathId,
) -> PgResult<BitmapSubplan<'mcx>> {
    match root.path(bitmapqual) {
        PathNode::BitmapAndPath(_) => {
            let (bitmapquals, startup_cost, total_cost, selectivity, parent_relid, parallel_safe) =
                match root.path(bitmapqual) {
                    PathNode::BitmapAndPath(p) => (
                        p.bitmapquals.clone(),
                        p.path.startup_cost,
                        p.path.total_cost,
                        p.bitmapselectivity,
                        p.path.parent,
                        p.path.parallel_safe,
                    ),
                    _ => unreachable!(),
                };

            // There may well be redundant quals among the subplans; eliminate
            // obvious duplicates with list_concat_unique.
            let mut subplans: Vec<Node<'mcx>> = Vec::new();
            let mut subquals: Vec<NodeId> = Vec::new();
            let mut subindexquals: Vec<NodeId> = Vec::new();
            let mut subindexecs: Vec<EcId> = Vec::new();
            for child in &bitmapquals {
                let sub = create_bitmap_subplan(mcx, root, run, *child)?;
                subplans.push(sub.plan);
                list_concat_unique_nodes(root, &mut subquals, &sub.qual);
                list_concat_unique_nodes(root, &mut subindexquals, &sub.indexqual);
                // Duplicates in indexECs aren't worth getting rid of.
                subindexecs.extend(sub.index_ecs);
            }

            let tuples = root.rel(parent_relid).tuples;
            let mut node = BitmapAnd {
                plan: Plan::default(),
                bitmapplans: subplans,
            };
            node.plan.startup_cost = startup_cost;
            node.plan.total_cost = total_cost;
            node.plan.plan_rows = costsize::clamp_row_est::call(selectivity * tuples);
            node.plan.plan_width = 0; // meaningless
            node.plan.parallel_aware = false;
            node.plan.parallel_safe = parallel_safe;

            Ok(BitmapSubplan {
                plan: Node::mk_bitmap_and(mcx, node),
                qual: subquals,
                indexqual: subindexquals,
                index_ecs: subindexecs,
            })
        }
        PathNode::BitmapOrPath(_) => {
            let (bitmapquals, startup_cost, total_cost, selectivity, parent_relid, parallel_safe) =
                match root.path(bitmapqual) {
                    PathNode::BitmapOrPath(p) => (
                        p.bitmapquals.clone(),
                        p.path.startup_cost,
                        p.path.total_cost,
                        p.bitmapselectivity,
                        p.path.parent,
                        p.path.parallel_safe,
                    ),
                    _ => unreachable!(),
                };

            // Here, we only detect qual-free subplans (an "... OR true ..."
            // reduces to "true"). We do not try to eliminate redundant
            // subclauses (could be hundreds/thousands of OR conditions).
            let mut subplans: Vec<Node<'mcx>> = Vec::new();
            let mut subquals: Vec<NodeId> = Vec::new();
            let mut subindexquals: Vec<NodeId> = Vec::new();
            let mut const_true_subqual = false;
            let mut const_true_subindexqual = false;
            for child in &bitmapquals {
                let sub = create_bitmap_subplan(mcx, root, run, *child)?;
                subplans.push(sub.plan);
                if sub.qual.is_empty() {
                    const_true_subqual = true;
                } else if !const_true_subqual {
                    let exprs = node_list_to_expr_vec_std(root, &sub.qual);
                    let anded = make_ands_explicit(exprs);
                    subquals.push(root.alloc_node(anded));
                }
                if sub.indexqual.is_empty() {
                    const_true_subindexqual = true;
                } else if !const_true_subindexqual {
                    let exprs = node_list_to_expr_vec_std(root, &sub.indexqual);
                    let anded = make_ands_explicit(exprs);
                    subindexquals.push(root.alloc_node(anded));
                }
            }

            // In the presence of ScalarArrayOpExpr quals, a BitmapOrPath may
            // have just one subpath; don't add an OR step.
            let plan: Node<'mcx> = if subplans.len() == 1 {
                subplans.into_iter().next().unwrap()
            } else {
                let tuples = root.rel(parent_relid).tuples;
                let mut node = BitmapOr {
                    plan: Plan::default(),
                    isshared: false,
                    bitmapplans: subplans,
                };
                node.plan.startup_cost = startup_cost;
                node.plan.total_cost = total_cost;
                node.plan.plan_rows = costsize::clamp_row_est::call(selectivity * tuples);
                node.plan.plan_width = 0; // meaningless
                node.plan.parallel_aware = false;
                node.plan.parallel_safe = parallel_safe;
                Node::mk_bitmap_or(mcx, node)
            };

            // If there were constant-TRUE subquals, the OR reduces to TRUE.
            // Also avoid generating one-element ORs.
            let qual = if const_true_subqual {
                Vec::new()
            } else if subquals.len() <= 1 {
                subquals
            } else {
                let exprs = node_list_to_expr_vec_std(root, &subquals);
                alloc::vec![root.alloc_node(make_orclause(exprs))]
            };
            let indexqual = if const_true_subindexqual {
                Vec::new()
            } else if subindexquals.len() <= 1 {
                subindexquals
            } else {
                let exprs = node_list_to_expr_vec_std(root, &subindexquals);
                alloc::vec![root.alloc_node(make_orclause(exprs))]
            };

            Ok(BitmapSubplan {
                plan,
                qual,
                indexqual,
                index_ecs: Vec::new(),
            })
        }
        PathNode::IndexPath(_) => {
            let (indexclauses, indpred, indexselectivity, indextotalcost, parent_relid, parallel_safe) =
                match root.path(bitmapqual) {
                    PathNode::IndexPath(p) => {
                        let ii = p
                            .indexinfo
                            .as_deref()
                            .expect("create_bitmap_subplan: IndexPath has no indexinfo");
                        (
                            p.indexclauses.clone(),
                            ii.indpred.clone(),
                            p.indexselectivity,
                            p.indextotalcost,
                            p.path.parent,
                            p.path.parallel_safe,
                        )
                    }
                    _ => unreachable!(),
                };

            // Use the regular indexscan plan build machinery, then convert the
            // produced IndexScan into a BitmapIndexScan.
            let iscan_node =
                create_indexscan_plan(mcx, root, run, bitmapqual, Vec::new(), Vec::new(), false)?;
            let iscan = match iscan_node.into_indexscan() {
                Some(s) => s,
                None => {
                    return Err(PgError::error(
                        "create_bitmap_subplan: create_indexscan_plan did not return an IndexScan",
                    ))
                }
            };

            let mut bnode = BitmapIndexScan {
                scan: Scan::default(),
                indexid: iscan.indexid,
                isshared: false,
                indexqual: iscan.indexqual,
                indexqualorig: iscan.indexqualorig,
            };
            bnode.scan.scanrelid = iscan.scan.scanrelid;
            // not used:
            bnode.scan.plan.targetlist = None;
            bnode.scan.plan.qual = None;
            bnode.scan.plan.lefttree = None;
            bnode.scan.plan.righttree = None;
            // set cost/width fields appropriately:
            let tuples = root.rel(parent_relid).tuples;
            bnode.scan.plan.startup_cost = 0.0;
            bnode.scan.plan.total_cost = indextotalcost;
            bnode.scan.plan.plan_rows =
                costsize::clamp_row_est::call(indexselectivity * tuples);
            bnode.scan.plan.plan_width = 0; // meaningless
            bnode.scan.plan.parallel_aware = false;
            bnode.scan.plan.parallel_safe = parallel_safe;

            // Extract original index clauses, actual index quals, relevant ECs.
            let mut subquals: Vec<NodeId> = Vec::new();
            let mut subindexquals: Vec<NodeId> = Vec::new();
            let mut subindexecs: Vec<EcId> = Vec::new();
            for iclause in &indexclauses {
                let rinfo = iclause
                    .rinfo
                    .expect("create_bitmap_subplan: IndexClause has no rinfo");
                let ri = root.rinfo(rinfo);
                debug_assert!(!ri.pseudoconstant);
                subquals.push(ri.clause);
                let actuals = get_actual_clauses(root, &iclause.indexquals);
                subindexquals.extend(actuals);
                if let Some(pec) = root.rinfo(rinfo).parent_ec {
                    subindexecs.push(pec);
                }
            }
            // We can add any index predicate conditions, too.
            for &pred in &indpred {
                // The index predicate was implied by the query as a whole, but
                // may or may not be implied by the conditions pushed into the
                // bitmapqual. Avoid generating redundant conditions.
                let single = [pred];
                if !predtest::predicate_implied_by::call(root, &single, &subquals, false) {
                    subquals.push(pred);
                    subindexquals.push(pred);
                }
            }

            Ok(BitmapSubplan {
                plan: Node::mk_bitmap_index_scan(mcx, bnode),
                qual: subquals,
                indexqual: subindexquals,
                index_ecs: subindexecs,
            })
        }
        _ => Err(PgError::error("unrecognized node type in create_bitmap_subplan")),
    }
}

/// `bitmap_subplan_mark_shared(plan)` (createplan.c) — set the `isshared` flag
/// in the bitmap subplan so it is created in shared memory (for a parallel
/// bitmap heap scan). Recurses to the leftmost leaf of an AND, sets `isshared`
/// on OR / BitmapIndexScan nodes.
fn bitmap_subplan_mark_shared(plan: &mut Node<'_>) -> PgResult<()> {
    match plan.node_tag() {
        ntag::T_BitmapAnd => {
            let a = plan.expect_bitmapand_mut();
            let first = a
                .bitmapplans
                .first_mut()
                .expect("bitmap_subplan_mark_shared: empty BitmapAnd");
            bitmap_subplan_mark_shared(first)
        }
        ntag::T_BitmapOr => {
            let o = plan.expect_bitmapor_mut();
            o.isshared = true;
            let first = o
                .bitmapplans
                .first_mut()
                .expect("bitmap_subplan_mark_shared: empty BitmapOr");
            bitmap_subplan_mark_shared(first)
        }
        ntag::T_BitmapIndexScan => {
            plan.expect_bitmapindexscan_mut().isshared = true;
            Ok(())
        }
        _ => Err(PgError::error(
            "bitmap_subplan_mark_shared: unrecognized node type",
        )),
    }
}

/// `list_concat_unique(dst, src)` over bare-expression arena handles: append
/// each element of `src` not already present (by `equal()`) in `dst`.
fn list_concat_unique_nodes(root: &PlannerInfo, dst: &mut Vec<NodeId>, src: &[NodeId]) {
    for &s in src {
        let s_expr = root.node(s);
        if !dst.iter().any(|&d| equal_expr_seam::call(root.node(d), s_expr)) {
            dst.push(s);
        }
    }
}

/// Resolve a list of bare clause arena handles into an owned (non-optional)
/// plan-node `PgVec<Expr>` field.
fn node_list_to_expr_vec<'mcx>(
    mcx: Mcx<'mcx>,
    root: &PlannerInfo,
    nodes: &[NodeId],
) -> PgResult<PgVec<'mcx, Expr>> {
    let mut out = vec_with_capacity_in(mcx, nodes.len())?;
    for &nid in nodes {
        out.push(root.node(nid).clone());
    }
    Ok(out)
}

/// Resolve a list of bare clause arena handles into a plain `Vec<Expr>` (for
/// `make_ands_explicit` / `make_orclause`, which take owned expression lists).
fn node_list_to_expr_vec_std(root: &PlannerInfo, nodes: &[NodeId]) -> Vec<Expr> {
    nodes.iter().map(|&nid| root.node(nid).clone()).collect()
}

/// Resolve a list of bare clause arena handles into an owned plan-node
/// `Option<PgVec<Expr>>` field (empty = `None`, the C `NIL`).
fn build_node_list_to_expr_field<'mcx>(
    mcx: Mcx<'mcx>,
    root: &PlannerInfo,
    nodes: &[NodeId],
) -> PgResult<Option<PgVec<'mcx, Expr>>> {
    if nodes.is_empty() {
        return Ok(None);
    }
    let mut out = vec_with_capacity_in(mcx, nodes.len())?;
    for &nid in nodes {
        out.push(root.node(nid).clone());
    }
    Ok(Some(out))
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
        let Some(cols) = (**row_node).as_list() else {
            return Err(PgError::error(
                "create_valuesscan_plan: RTE values_lists element is not a List",
            ));
        };
        let mut row: PgVec<'mcx, Expr> = vec_with_capacity_in(mcx, cols.len())?;
        for col in cols.iter() {
            if let Some(e) = (**col).as_expr() {
                row.push(e.clone());
            } else {
                return Err(PgError::error(
                    "create_valuesscan_plan: VALUES column is not an expression",
                ));
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

    Ok(Node::mk_values_scan(mcx, scan_plan))
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

    Ok(Node::mk_result(mcx, scan_plan))
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
    let mut tsc: TableSampleClause<'mcx> = if let Some(t) = tsc_node.as_tablesampleclause() {
        t.clone_in(mcx)?
    } else {
        return Err(PgError::error(
            "create_samplescan_plan: RTE tablesample is not a TableSampleClause",
        ));
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

    Ok(Node::mk_sample_scan(mcx, scan_plan))
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

    Ok(Node::mk_tid_scan(mcx, scan_plan))
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

    Ok(Node::mk_tid_range_scan(mcx, scan_plan))
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
        if let Some(rtf) = (**f).as_rangetblfunction() {
            functions.push(rtf.clone_in(mcx)?);
        } else {
            return Err(PgError::error(
                "create_functionscan_plan: RTE functions element is not a RangeTblFunction",
            ));
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
                if let Some(e) = fe.as_expr_mut() {
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

    Ok(Node::mk_function_scan(mcx, scan_plan))
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

    Ok(Node::mk_named_tuplestore_scan(mcx, scan_plan))
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
        cp_seam::resolve_cte_subplan::call(root, run, scan_relid)?;

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

    Ok(Node::mk_cte_scan(mcx, scan_plan))
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
    let wt_param_id = cp_seam::resolve_worktable_param::call(root, run, scan_relid)?;

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

    Ok(Node::mk_work_table_scan(mcx, scan_plan))
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

/// `create_subqueryscan_subplan` seam impl: build the subquery's child Plan by
/// recursing into the **subroot** planner context, exactly as C's
/// `create_plan(rel->subroot, best_path->subpath)`.
///
/// The `SubqueryScanPath`'s in-root `subpath` is a cost-only copy that was
/// deep-imported by `import_path_from_subroot`; building the subplan from it
/// in-root would resolve every leaf scan's `scanrelid` against the OUTER root's
/// range table, where a set-op leg's relation index (subroot-relative `= 1`)
/// collides with the SUBQUERY RTE the outer query holds at that slot — tripping
/// the `RTE_RELATION` assertion in `create_seqscan_plan`. Instead recurse into
/// the parent rel's `subroot` with the original subroot-arena path
/// (`subroot_subpath`): the subroot's `simple_rte_array` resolves the leg's
/// relation RTE at index 1 correctly. `set_subqueryscan_references` then
/// flattens the subroot's range table into `glob->finalrtable` with the right
/// rtoffset.
fn create_subqueryscan_subplan_inroot<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    best_path: PathId,
) -> PgResult<Node<'mcx>> {
    let (subpath_inroot, subroot_subpath) = match root.path(best_path) {
        types_pathnodes::PathNode::SubqueryScanPath(p) => (
            p.subpath
                .expect("create_subqueryscan_subplan: SubqueryScanPath has no subpath"),
            p.subroot_subpath,
        ),
        _ => panic!("create_subqueryscan_subplan: best_path is not a SubqueryScanPath"),
    };

    match subroot_subpath {
        // Set-op leg: the child path lives in a distinct subroot. Recurse with
        // `create_plan(rel->subroot, subroot_subpath)` (C createplan.c:3712).
        Some(sub_id) => {
            let rel_id = root.path(best_path).base().parent;
            let mut subroot = root
                .rel_mut(rel_id)
                .subroot
                .0
                .take()
                .expect("create_subqueryscan_subplan: set-op child rel has no subroot");
            let result = create_plan(mcx, &mut subroot, run, sub_id);
            // Restore the subroot so a later level (e.g. set_subqueryscan_references)
            // can still reach it, even on the error path.
            root.rel_mut(rel_id).subroot.0 = Some(subroot);
            result
        }
        // Non-set-op subquery scan whose `subpath` already lives in this root.
        None => create_plan_recurse(mcx, root, run, subpath_inroot, CP_EXACT_TLIST),
    }
}

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

    Ok(Node::mk_subquery_scan(mcx, scan_plan))
}

// ===========================================================================
// Upper-plan converters (createplan.c create_plan_recurse non-scan/non-join
// arms). These are reached from create_plan_recurse for the upper (post-scan)
// portion of the path tree. The SELECT-1 path is ProjectionPath ->
// GroupResultPath, so create_projection_plan + create_group_result_plan are the
// minimal subset; the rest are ported for createplan.c completeness.
// ===========================================================================

// ---------------------------------------------------------------------------
// make_material (createplan.c ~6641) / make_project_set (~7150).
// ---------------------------------------------------------------------------

/// `make_material(lefttree)` — build a `Material` plan node atop `lefttree`.
/// The Material's targetlist is the child's targetlist (it doesn't project).
fn make_material<'mcx>(
    mcx: Mcx<'mcx>,
    lefttree: Node<'mcx>,
) -> PgResult<MaterialNode<'mcx>> {
    let mut node = MaterialNode::default();
    // plan->targetlist = lefttree->targetlist;
    let tlist = clone_plan_tlist(mcx, &lefttree)?;
    {
        let plan: &mut Plan = &mut node.plan;
        plan.targetlist = tlist;
        plan.qual = None;
        plan.righttree = None;
    }
    node.plan.lefttree = Some(mcx::alloc_in(mcx, lefttree)?);
    Ok(node)
}

/// `materialize_finished_plan(subplan)` (createplan.c) — wrap a finished `Plan`
/// in a `Material` node. Used by `build_subplan` for an uncorrelated non-init
/// subplan when `enable_material` and the top node does not already
/// materialize its output. Moves the subplan's initPlans up to the Material
/// node and recomputes cost data.
fn materialize_finished_plan<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    mut subplan: Node<'mcx>,
) -> PgResult<Node<'mcx>> {
    // Read the cost inputs off the subplan before it is moved into the Material.
    let (
        sub_disabled_nodes,
        sub_startup_cost_in,
        sub_total_cost_in,
        sub_plan_rows,
        sub_plan_width,
        sub_parallel_safe,
    ) = {
        let p = subplan.plan_head();
        (
            p.disabled_nodes,
            p.startup_cost,
            p.total_cost,
            p.plan_rows,
            p.plan_width,
            p.parallel_safe,
        )
    };

    // XXX horrid kluge (per C): if there are any initPlans attached to the
    // subplan, move them up to the Material node, which is now effectively the
    // top plan node in its query level. This prevents failure in
    // SS_finalize_plan().
    let moved_init = subplan.plan_head_mut().initPlan.take();

    // Move the initplans' cost delta, as well. C: SS_compute_initplan_cost(
    // matplan->initPlan, &initplan_cost, &unsafe_initplans); the per-plan cost
    // of each init SubPlan is startup_cost + per_call_cost.
    let mut initplan_cost = 0.0_f64;
    if let Some(ref ips) = moved_init {
        for sp in ips.iter() {
            initplan_cost += sp.startup_cost + sp.per_call_cost;
        }
    }

    // subplan->startup_cost -= initplan_cost; subplan->total_cost -= initplan_cost;
    let sub_startup_cost = sub_startup_cost_in - initplan_cost;
    let sub_total_cost = sub_total_cost_in - initplan_cost;

    // matplan = (Plan *) make_material(subplan);
    let mut matnode = make_material(mcx, subplan)?;
    matnode.plan.initPlan = moved_init;

    // Set cost data via a throwaway path arena entry (the C `Path matpath` is a
    // stack-local scratch buffer; cost_material writes rows/disabled_nodes/
    // startup_cost/total_cost into it and never reads `parent`).
    let dummy = Path {
        type_: NodeTag(293),    // T_MaterialPath
        pathtype: NodeTag(360), // T_Material
        parent: RelId(0),
        pathtarget: None,
        param_info: None,
        parallel_aware: false,
        parallel_safe: false,
        parallel_workers: 0,
        rows: 0.0,
        disabled_nodes: 0,
        startup_cost: 0.0,
        total_cost: 0.0,
        pathkeys: Vec::new(),
    };
    let dummy_id = root.alloc_path(PathNode::MaterialPath(MaterialPath {
        path: dummy,
        subpath: None,
    }));
    pathnode::cost_material::call(
        root,
        dummy_id,
        sub_disabled_nodes,
        sub_startup_cost,
        sub_total_cost,
        sub_plan_rows,
        sub_plan_width,
    );
    let (mat_startup, mat_total) = {
        let p = root.path(dummy_id).base();
        (p.startup_cost, p.total_cost)
    };

    {
        let plan: &mut Plan = &mut matnode.plan;
        plan.disabled_nodes = sub_disabled_nodes;
        plan.startup_cost = mat_startup + initplan_cost;
        plan.total_cost = mat_total + initplan_cost;
        plan.plan_rows = sub_plan_rows;
        plan.plan_width = sub_plan_width;
        plan.parallel_aware = false;
        plan.parallel_safe = sub_parallel_safe;
    }

    Ok(Node::mk_material(mcx, matnode))
}

/// `make_project_set(tlist, subplan)` — build a `ProjectSet` plan node.
fn make_project_set<'mcx>(
    mcx: Mcx<'mcx>,
    tlist: Option<PgVec<'mcx, TargetEntry<'mcx>>>,
    subplan: Node<'mcx>,
) -> PgResult<ProjectSetNode<'mcx>> {
    let mut node = ProjectSetNode {
        plan: Plan::default(),
    };
    let sub = mcx::alloc_in(mcx, subplan)?;
    let plan: &mut Plan = &mut node.plan;
    plan.targetlist = tlist;
    plan.qual = None;
    plan.lefttree = Some(sub);
    plan.righttree = None;
    Ok(node)
}

/// Deep-copy a plan node's `targetlist` into a fresh `mcx` list (the C
/// `plan->targetlist = lefttree->targetlist` aliases the same `List`; in this
/// owned model the child plan is moved into `lefttree`, so the parent gets a
/// clone of the tlist before the move).
fn clone_plan_tlist<'mcx>(
    mcx: Mcx<'mcx>,
    plan_node: &Node<'mcx>,
) -> PgResult<Option<PgVec<'mcx, TargetEntry<'mcx>>>> {
    match plan_node.plan_head().targetlist {
        None => Ok(None),
        Some(ref tl) => {
            let mut out = vec_with_capacity_in(mcx, tl.len())?;
            for tle in tl.iter() {
                out.push(tle.clone_in(mcx)?);
            }
            Ok(Some(out))
        }
    }
}

// ---------------------------------------------------------------------------
// is_projection_capable_plan (createplan.c ~7442)
// ---------------------------------------------------------------------------

/// `is_projection_capable_plan(plan)` — can the given plan node do projection?
/// Most plan types can; this lists the ones that can't (read off `nodeTag`).
fn is_projection_capable_plan(plan: &Node<'_>) -> bool {
    match plan.node_tag() {
        ntag::T_Hash
        | ntag::T_Material
        | ntag::T_Memoize
        | ntag::T_Sort
        | ntag::T_Unique
        | ntag::T_SetOp
        // T_LockRows: has no `Node` arm yet (LockRows plan node unported); it
        // can't be constructed, so it never reaches here. When the arm lands it
        // must be added to this not-projection-capable list.
        | ntag::T_Limit
        | ntag::T_ModifyTable
        | ntag::T_Append
        | ntag::T_MergeAppend
        | ntag::T_RecursiveUnion => false,
        // CustomScan can project iff it advertises CUSTOMPATH_SUPPORT_PROJECTION.
        ntag::T_CustomScan => {
            let cs = plan.expect_customscan();
            cs.flags & types_pathnodes::CUSTOMPATH_SUPPORT_PROJECTION != 0
        }
        // ProjectSet projects, but say "no" so the planner won't replace its
        // tlist; the SRFs have to stay at top level.
        ntag::T_ProjectSet => false,
        _ => true,
    }
}

// ---------------------------------------------------------------------------
// create_group_result_plan (createplan.c ~1588)
// ---------------------------------------------------------------------------

/// `create_group_result_plan(root, best_path)` — create a `Result` plan for a
/// degenerate grouping case (no input rows / single group with HAVING quals).
fn create_group_result_plan<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    best_path: PathId,
) -> PgResult<Node<'mcx>> {
    let _ = run;
    let tlist = build_path_tlist(mcx, root, best_path)?;

    // best_path->quals is just bare clauses.
    let quals = match root.path(best_path) {
        PathNode::GroupResultPath(grp) => grp.quals.clone(),
        _ => unreachable!("create_group_result_plan on non-GroupResultPath"),
    };
    let quals = order_qual_clauses(root, &quals);

    // make_result(tlist, (Node *) quals, NULL).
    let resconstantqual = nodes_to_expr_qual(mcx, root, &quals)?;
    let tlist = tlist_to_plan_field(mcx, tlist)?;
    let mut plan = make_result(mcx, tlist, resconstantqual, None)?;

    copy_generic_path_info(&mut plan.plan, root.path(best_path).base());

    Ok(Node::mk_result(mcx, plan))
}

/// Convert a list of bare clause expression arena handles into an owned `Expr`
/// qual list (the `make_result`/`make_*` `(Node *) quals` argument). An empty
/// list is the C `NIL`, stored as `None`.
fn nodes_to_expr_qual<'mcx>(
    mcx: Mcx<'mcx>,
    root: &PlannerInfo,
    clauses: &[NodeId],
) -> PgResult<Option<PgVec<'mcx, Expr>>> {
    if clauses.is_empty() {
        return Ok(None);
    }
    let mut out = vec_with_capacity_in(mcx, clauses.len())?;
    for &cid in clauses {
        // Deep-copy via clone_in: a HAVING qual clause may carry an Aggref,
        // whose `args` TargetEntry list has context-allocated children that a
        // bare derived `.clone()` cannot copy (it panics by design).
        out.push(root.node(cid).clone_in(mcx)?);
    }
    Ok(Some(out))
}

// ---------------------------------------------------------------------------
// create_project_set_plan (createplan.c ~1613)
// ---------------------------------------------------------------------------

/// `create_project_set_plan(root, best_path)` — create a `ProjectSet` plan for
/// a tlist containing set-returning functions.
fn create_project_set_plan<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    best_path: PathId,
) -> PgResult<Node<'mcx>> {
    let subpath = match root.path(best_path) {
        PathNode::ProjectSetPath(p) => p.subpath,
        _ => unreachable!("create_project_set_plan on non-ProjectSetPath"),
    }
    .expect("create_project_set_plan: ProjectSetPath has no subpath");

    // Since we intend to project, we don't need to constrain child tlist.
    let subplan = create_plan_recurse(mcx, root, run, subpath, 0)?;

    let tlist = build_path_tlist(mcx, root, best_path)?;
    let tlist = tlist_to_plan_field(mcx, tlist)?;

    let mut plan = make_project_set(mcx, tlist, subplan)?;

    copy_generic_path_info(&mut plan.plan, root.path(best_path).base());

    Ok(Node::mk_project_set(mcx, plan))
}

// ---------------------------------------------------------------------------
// create_material_plan (createplan.c ~1639)
// ---------------------------------------------------------------------------

/// `create_material_plan(root, best_path, flags)` — create a `Material` plan
/// and (recursively) plans for its subpaths.
fn create_material_plan<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    best_path: PathId,
    flags: i32,
) -> PgResult<Node<'mcx>> {
    let subpath = match root.path(best_path) {
        PathNode::MaterialPath(p) => p.subpath,
        _ => unreachable!("create_material_plan on non-MaterialPath"),
    }
    .expect("create_material_plan: MaterialPath has no subpath");

    // We don't want any excess columns in the materialized tuples, so request a
    // smaller tlist. Otherwise, since Material doesn't project, tlist
    // requirements pass through.
    let subplan = create_plan_recurse(mcx, root, run, subpath, flags | CP_SMALL_TLIST)?;

    let mut plan = make_material(mcx, subplan)?;

    copy_generic_path_info(&mut plan.plan, root.path(best_path).base());

    Ok(Node::mk_material(mcx, plan))
}

// ===========================================================================
// Sort family: create_sort_plan + make_sort / make_sort_from_pathkeys /
// prepare_sort_from_pathkeys / inject_projection_plan (createplan.c).
// ===========================================================================

/// `inject_projection_plan(subplan, tlist, parallel_safe)` (createplan.c ~2117)
/// — stack a `Result` projection node atop `subplan` carrying `tlist`. Used by
/// `prepare_sort_from_pathkeys` when the input plan can't do projection but a
/// resjunk sort column must be computed.
fn inject_projection_plan<'mcx>(
    mcx: Mcx<'mcx>,
    subplan: Node<'mcx>,
    tlist: Option<PgVec<'mcx, TargetEntry<'mcx>>>,
    parallel_safe: bool,
) -> PgResult<Node<'mcx>> {
    // plan = (Plan *) make_result(tlist, NULL, subplan);
    // copy_plan_costsize(plan, subplan); plan->parallel_safe = parallel_safe;
    let child_plan = subplan.plan_head().clone_in(mcx)?;
    let mut node = make_result(mcx, tlist, None, Some(subplan))?;
    copy_plan_costsize(&mut node.plan, &child_plan);
    node.plan.parallel_safe = parallel_safe;
    Ok(Node::mk_result(mcx, node))
}

/// `make_sort(lefttree, numCols, sortColIdx, sortOperators, collations,
/// nullsFirst)` (createplan.c ~6203) — build a `Sort` plan node atop
/// `lefttree`. The Sort's targetlist is the child's (it doesn't project).
fn make_sort<'mcx>(
    mcx: Mcx<'mcx>,
    lefttree: Node<'mcx>,
    sort_col_idx: PgVec<'mcx, AttrNumber>,
    sort_operators: PgVec<'mcx, Oid>,
    collations: PgVec<'mcx, Oid>,
    nulls_first: PgVec<'mcx, bool>,
) -> PgResult<Sort<'mcx>> {
    let num_cols = sort_col_idx.len() as i32;
    // plan->targetlist = lefttree->targetlist;
    let tlist = clone_plan_tlist(mcx, &lefttree)?;
    // plan->disabled_nodes = lefttree->disabled_nodes + (enable_sort == false);
    let lefttree_disabled = lefttree.plan_head().disabled_nodes;
    let enable_sort = (vars::enable_sort.get().get)();
    let mut plan = Plan::default();
    plan.targetlist = tlist;
    plan.disabled_nodes = lefttree_disabled + i32::from(!enable_sort);
    plan.qual = None;
    plan.lefttree = Some(mcx::alloc_in(mcx, lefttree)?);
    plan.righttree = None;
    Ok(Sort {
        plan,
        numCols: num_cols,
        sortColIdx: sort_col_idx,
        sortOperators: sort_operators,
        collations,
        nullsFirst: nulls_first,
    })
}

/// `make_sort_from_sortclauses(sortcls, lefttree)` (createplan.c:6551) — create
/// a `Sort` plan that sorts according to the given `SortGroupClause` list,
/// locating the sort columns by `tleSortGroupRef` in the child's targetlist.
fn make_sort_from_sortclauses<'mcx>(
    mcx: Mcx<'mcx>,
    sortcls: &[types_nodes::rawnodes::SortGroupClause],
    lefttree: Node<'mcx>,
) -> PgResult<Sort<'mcx>> {
    // Convert list-ish representation to arrays wanted by executor, reading the
    // child's targetlist (the C `lefttree->targetlist`).
    let numsortkeys = sortcls.len();
    let mut sort_col_idx: PgVec<'mcx, AttrNumber> = vec_with_capacity_in(mcx, numsortkeys)?;
    let mut sort_operators: PgVec<'mcx, Oid> = vec_with_capacity_in(mcx, numsortkeys)?;
    let mut collations: PgVec<'mcx, Oid> = vec_with_capacity_in(mcx, numsortkeys)?;
    let mut nulls_first: PgVec<'mcx, bool> = vec_with_capacity_in(mcx, numsortkeys)?;

    {
        let sub_tlist: &[TargetEntry<'mcx>] =
            lefttree.plan_head().targetlist.as_deref().unwrap_or(&[]);
        for sortcl in sortcls {
            let tle = util_tlist::get_sortgroupclause_tle(sortcl, sub_tlist)?;
            sort_col_idx.push(tle.resno);
            sort_operators.push(sortcl.sortop);
            collations.push(expr_collation_of_tle(tle)?);
            nulls_first.push(sortcl.nulls_first);
        }
    }

    make_sort(mcx, lefttree, sort_col_idx, sort_operators, collations, nulls_first)
}

/// `make_sort_from_groupcols(groupcls, grpColIdx, lefttree)` (createplan.c:6599)
/// — create a `Sort` plan that sorts based on grouping columns. The sort
/// columns are located by the `grpColIdx[]` array (the child tlist is not
/// marked with sortgroupref labels appropriate to the grouping node); only the
/// sort ordering info is taken from the `SortGroupClause` entries.
fn make_sort_from_groupcols<'mcx>(
    mcx: Mcx<'mcx>,
    groupcls: &[types_nodes::rawnodes::SortGroupClause],
    grp_col_idx: &[AttrNumber],
    lefttree: Node<'mcx>,
) -> PgResult<Sort<'mcx>> {
    let numsortkeys = groupcls.len();
    let mut sort_col_idx: PgVec<'mcx, AttrNumber> = vec_with_capacity_in(mcx, numsortkeys)?;
    let mut sort_operators: PgVec<'mcx, Oid> = vec_with_capacity_in(mcx, numsortkeys)?;
    let mut collations: PgVec<'mcx, Oid> = vec_with_capacity_in(mcx, numsortkeys)?;
    let mut nulls_first: PgVec<'mcx, bool> = vec_with_capacity_in(mcx, numsortkeys)?;

    {
        let sub_tlist: &[TargetEntry<'mcx>] =
            lefttree.plan_head().targetlist.as_deref().unwrap_or(&[]);
        for (i, grpcl) in groupcls.iter().enumerate() {
            let tle = get_tle_by_resno(sub_tlist, grp_col_idx[i]).ok_or_else(|| {
                PgError::error("could not retrieve tle for sort-from-groupcols")
            })?;
            sort_col_idx.push(tle.resno);
            sort_operators.push(grpcl.sortop);
            collations.push(expr_collation_of_tle(tle)?);
            nulls_first.push(grpcl.nulls_first);
        }
    }

    make_sort(mcx, lefttree, sort_col_idx, sort_operators, collations, nulls_first)
}

/// `prepare_sort_from_pathkeys(lefttree, pathkeys, relids, reqColIdx,
/// adjust_tlist_in_place, ...)` (createplan.c ~6300) — convert the pathkey list
/// into executor sort-key arrays, adjusting the input plan's targetlist (adding
/// resjunk sort columns, possibly injecting a `Result`) as needed.
///
/// Returns the (possibly replaced) input node plus the four parallel
/// sort-key arrays (`sortColIdx` / `sortOperators` / `collations` /
/// `nullsFirst`), all `numsortkeys` long.
///
/// `reqColIdx` (the MergeAppend child case) is not yet exercised; `None` here.
/// `adjust_tlist_in_place` forces the lefttree tlist to be modified in place.
type SortKeyArrays<'mcx> = (
    Node<'mcx>,
    PgVec<'mcx, AttrNumber>,
    PgVec<'mcx, Oid>,
    PgVec<'mcx, Oid>,
    PgVec<'mcx, bool>,
);

fn prepare_sort_from_pathkeys<'mcx>(
    mcx: Mcx<'mcx>,
    root: &PlannerInfo,
    mut lefttree: Node<'mcx>,
    pathkeys: &[PathKey],
    relids: &Relids,
    req_col_idx: Option<&[AttrNumber]>,
    mut adjust_tlist_in_place: bool,
) -> PgResult<SortKeyArrays<'mcx>> {
    // We will need at most list_length(pathkeys) sort columns; possibly less.
    let mut sort_col_idx: PgVec<'mcx, AttrNumber> = vec_with_capacity_in(mcx, pathkeys.len())?;
    let mut sort_operators: PgVec<'mcx, Oid> = vec_with_capacity_in(mcx, pathkeys.len())?;
    let mut collations: PgVec<'mcx, Oid> = vec_with_capacity_in(mcx, pathkeys.len())?;
    let mut nulls_first: PgVec<'mcx, bool> = vec_with_capacity_in(mcx, pathkeys.len())?;

    let mut numsortkeys: usize = 0;

    for pathkey in pathkeys {
        let ec_id = pathkey
            .pk_eclass
            .expect("prepare_sort_from_pathkeys: PathKey has no EquivalenceClass");
        let ec = root.ec(ec_id);

        // The matched/created tlist entry's resno + datatype.
        let mut matched_resno: Option<AttrNumber> = None;
        let mut pk_datatype: Oid = InvalidOid;

        if ec.ec_has_volatile {
            // If the pathkey's EquivalenceClass is volatile, it must have come
            // from an ORDER BY clause, and we have to match it to that same
            // targetlist entry.
            if ec.ec_sortref == 0 {
                return Err(PgError::error("volatile EquivalenceClass has no sortref"));
            }
            let tlist = lefttree
                .plan_head()
                .targetlist
                .as_deref()
                .unwrap_or(&[]);
            let tle = get_sortgroupref_tle(ec.ec_sortref, tlist)?;
            matched_resno = Some(tle.resno);
            debug_assert_eq!(ec.ec_members.len(), 1);
            let first_em = ec.ec_members[0];
            pk_datatype = root.em(first_em).em_datatype;
        } else if let Some(req) = req_col_idx {
            // If we are given a sort column number to match, only consider the
            // single TLE at that position.
            let req_resno = req[numsortkeys];
            let tlist = lefttree
                .plan_head()
                .targetlist
                .as_deref()
                .unwrap_or(&[]);
            if let Some(tle) = tlist.iter().find(|t| t.resno == req_resno) {
                let tle_expr = tle.expr.as_deref().cloned().unwrap_or(Expr::Var(Default::default()));
                if let Some(em) = find_ec_member_matching_expr(root, ec_id, &tle_expr, relids) {
                    // found expr at right place in tlist
                    pk_datatype = em.em_datatype;
                    matched_resno = Some(tle.resno);
                }
            }
        } else {
            // Otherwise, we can sort by any non-constant expression listed in
            // the pathkey's EquivalenceClass. Take the first tlist item found.
            let tlist = lefttree
                .plan_head()
                .targetlist
                .as_deref()
                .unwrap_or(&[]);
            for tle in tlist.iter() {
                let tle_expr = tle.expr.as_deref().cloned().unwrap_or(Expr::Var(Default::default()));
                if let Some(em) = find_ec_member_matching_expr(root, ec_id, &tle_expr, relids) {
                    // found expr already in tlist
                    pk_datatype = em.em_datatype;
                    matched_resno = Some(tle.resno);
                    break;
                }
            }
        }

        let resno = match matched_resno {
            Some(resno) => resno,
            None => {
                // No matching tlist item; look for a computable expression.
                let em_id = find_computable_ec_member(root, ec_id, &[], relids, false)
                    .ok_or_else(|| PgError::error("could not find pathkey item to sort"))?;
                pk_datatype = root.em(em_id).em_datatype;
                // em_expr to be copied into a resjunk targetentry.
                let resjunk_expr = root.node(root.em(em_id).em_expr).clone();

                // Do we need to insert a Result node? If we can't modify the
                // tlist in place and the input plan can't project, stack a
                // Result. (Append/MergeAppend pass adjust_tlist_in_place=true.)
                if !adjust_tlist_in_place && !is_projection_capable_plan(&lefttree) {
                    let tlist_copy = clone_plan_tlist(mcx, &lefttree)?;
                    let parallel_safe = lefttree.plan_head().parallel_safe;
                    lefttree = inject_projection_plan(mcx, lefttree, tlist_copy, parallel_safe)?;
                }
                // Don't bother testing is_projection_capable_plan again.
                adjust_tlist_in_place = true;

                // Add resjunk entry to input's tlist: resno = len + 1.
                let new_resno = {
                    let cur = lefttree.plan_head().targetlist.as_deref().map(|t| t.len()).unwrap_or(0);
                    (cur + 1) as AttrNumber
                };
                let tle = make_target_entry(mcx, resjunk_expr, new_resno, None, true)?;
                let plan = lefttree.plan_head_mut();
                match plan.targetlist {
                    Some(ref mut tl) => tl.push(tle),
                    None => {
                        let mut tl = vec_with_capacity_in(mcx, 1)?;
                        tl.push(tle);
                        plan.targetlist = Some(tl);
                    }
                }
                new_resno
            }
        };

        // Look up the correct sort operator from the PathKey's abstracted
        // representation.
        let sortop = lsyscache::get_opfamily_member_for_cmptype::call(
            pathkey.pk_opfamily,
            pk_datatype,
            pk_datatype,
            pathkey.pk_cmptype,
        )?;
        if sortop == InvalidOid {
            return Err(PgError::error(alloc::format!(
                "missing operator {}({},{}) in opfamily {}",
                pathkey.pk_cmptype, pk_datatype, pk_datatype, pathkey.pk_opfamily
            )));
        }

        // Add the column to the sort arrays.
        sort_col_idx.push(resno);
        sort_operators.push(sortop);
        collations.push(ec.ec_collation);
        nulls_first.push(pathkey.pk_nulls_first);
        numsortkeys += 1;
    }

    let _ = numsortkeys;
    Ok((lefttree, sort_col_idx, sort_operators, collations, nulls_first))
}

/// `make_sort_from_pathkeys(lefttree, pathkeys, relids)` (createplan.c ~6482) —
/// create a `Sort` plan to sort `lefttree` according to `pathkeys`.
fn make_sort_from_pathkeys<'mcx>(
    mcx: Mcx<'mcx>,
    root: &PlannerInfo,
    lefttree: Node<'mcx>,
    pathkeys: &[PathKey],
    relids: &Relids,
) -> PgResult<Sort<'mcx>> {
    // Compute sort column info, and adjust lefttree as needed.
    let (lefttree, sort_col_idx, sort_operators, collations, nulls_first) =
        prepare_sort_from_pathkeys(mcx, root, lefttree, pathkeys, relids, None, false)?;
    // Now build the Sort node.
    make_sort(mcx, lefttree, sort_col_idx, sort_operators, collations, nulls_first)
}

/// `create_sort_plan(root, best_path, flags)` (createplan.c ~2177) — create a
/// `Sort` plan and (recursively) the plan for its subpath.
fn create_sort_plan<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    best_path: PathId,
    flags: i32,
) -> PgResult<Node<'mcx>> {
    let subpath = match root.path(best_path) {
        PathNode::SortPath(p) => p.subpath,
        _ => unreachable!("create_sort_plan on non-SortPath"),
    }
    .expect("create_sort_plan: SortPath has no subpath");

    // We don't want any excess columns in the sorted tuples, so request a
    // smaller tlist. Otherwise, since Sort doesn't project, tlist requirements
    // pass through.
    let subplan = create_plan_recurse(mcx, root, run, subpath, flags | CP_SMALL_TLIST)?;

    // make_sort_from_pathkeys indirectly calls find_ec_member_matching_expr,
    // which will ignore any child EC members that don't belong to the given
    // relids. Thus, if this sort path is based on a child relation, we must
    // pass its relids: IS_OTHER_REL(subpath->parent) ? path.parent->relids : NULL.
    let subpath_parent = root.path(subpath).base().parent;
    let relids: Relids = if root.rel(subpath_parent).reloptkind >= RELOPT_OTHER_MEMBER_REL {
        let parent = root.path(best_path).base().parent;
        root.rel(parent).relids.clone()
    } else {
        None
    };

    // Clone the pathkeys out so we can borrow root immutably while building.
    let pathkeys = root.path(best_path).base().pathkeys.clone();
    let mut plan = make_sort_from_pathkeys(mcx, root, subplan, &pathkeys, &relids)?;

    copy_generic_path_info(&mut plan.plan, root.path(best_path).base());

    Ok(Node::mk_sort(mcx, plan))
}

// ===========================================================================
// Limit family: create_limit_plan + make_limit (createplan.c).
// ===========================================================================

/// `make_limit(lefttree, limitOffset, limitCount, limitOption, uniqNumCols,
/// uniqColIdx, uniqOperators, uniqCollations)` (createplan.c ~7101) — build a
/// `Limit` plan node atop `lefttree`. Limit doesn't project; the targetlist
/// passes through.
#[allow(clippy::too_many_arguments)]
fn make_limit<'mcx>(
    mcx: Mcx<'mcx>,
    lefttree: Node<'mcx>,
    limit_offset: Option<PgBox<'mcx, Expr>>,
    limit_count: Option<PgBox<'mcx, Expr>>,
    limit_option: types_nodes::nodelimit::LimitOption,
    uniq_num_cols: i32,
    uniq_col_idx: Option<PgVec<'mcx, AttrNumber>>,
    uniq_operators: Option<PgVec<'mcx, Oid>>,
    uniq_collations: Option<PgVec<'mcx, Oid>>,
) -> PgResult<LimitNode<'mcx>> {
    let tlist = clone_plan_tlist(mcx, &lefttree)?;
    let mut node = LimitNode::default();
    {
        let plan: &mut Plan = &mut node.plan;
        plan.targetlist = tlist;
        plan.qual = None;
        plan.lefttree = Some(mcx::alloc_in(mcx, lefttree)?);
        plan.righttree = None;
    }
    node.limitOffset = limit_offset;
    node.limitCount = limit_count;
    node.limitOption = limit_option;
    node.uniqNumCols = uniq_num_cols;
    node.uniqColIdx = uniq_col_idx;
    node.uniqOperators = uniq_operators;
    node.uniqCollations = uniq_collations;
    Ok(node)
}

/// `create_limit_plan(root, best_path, flags)` (createplan.c ~2849) — create a
/// `Limit` plan and (recursively) the plan for its subpath.
fn create_limit_plan<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    best_path: PathId,
    flags: i32,
) -> PgResult<Node<'mcx>> {
    let (subpath, limit_offset_id, limit_count_id, limit_option) = match root.path(best_path) {
        PathNode::LimitPath(p) => (p.subpath, p.limitOffset, p.limitCount, p.limitOption),
        _ => unreachable!("create_limit_plan on non-LimitPath"),
    };
    let subpath = subpath.expect("create_limit_plan: LimitPath has no subpath");

    // Limit doesn't project, so tlist requirements pass through.
    let subplan = create_plan_recurse(mcx, root, run, subpath, flags)?;

    // Extract information necessary for comparing rows for WITH TIES. The
    // LIMIT_OPTION_COUNT (plain LIMIT/OFFSET) case needs no uniq-key arrays.
    // FETCH FIRST ... WITH TIES walks parse->sortClause matching each
    // SortGroupClause to parse->targetList for uniqColIdx/uniqOperators; that
    // requires the owned Query's sortClause/targetList (not yet threaded into
    // create_limit_plan), so the WITH TIES sub-case loud-errors until then.
    if limit_option == types_pathnodes::LIMIT_OPTION_WITH_TIES {
        return Err(PgError::error(
            "create_limit_plan: FETCH FIRST ... WITH TIES not yet supported",
        ));
    }
    let uniq_num_cols: i32 = 0;
    let uniq_col_idx: Option<PgVec<'mcx, AttrNumber>> = None;
    let uniq_operators: Option<PgVec<'mcx, Oid>> = None;
    let uniq_collations: Option<PgVec<'mcx, Oid>> = None;

    // best_path->limitOffset / limitCount are bare expr node handles; clone the
    // Expr out of the arena into the owned plan node.
    let limit_offset = match limit_offset_id {
        Some(id) => Some(mcx::alloc_in(mcx, root.node(id).clone())?),
        None => None,
    };
    let limit_count = match limit_count_id {
        Some(id) => Some(mcx::alloc_in(mcx, root.node(id).clone())?),
        None => None,
    };

    let mut plan = make_limit(
        mcx,
        subplan,
        limit_offset,
        limit_count,
        limit_option_to_node(limit_option),
        uniq_num_cols,
        uniq_col_idx,
        uniq_operators,
        uniq_collations,
    )?;

    copy_generic_path_info(&mut plan.plan, root.path(best_path).base());

    Ok(Node::mk_limit(mcx, plan))
}

// ===========================================================================
// ModifyTable: create_modifytable_plan + make_modifytable (createplan.c:2808).
// ===========================================================================

/// `create_modifytable_plan(root, best_path)` (createplan.c:2808) — create a
/// `ModifyTable` plan and (recursively) the plan for its subpath.
fn create_modifytable_plan<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    best_path: PathId,
) -> PgResult<Node<'mcx>> {
    let p = match root.path(best_path) {
        PathNode::ModifyTablePath(p) => p,
        _ => unreachable!("create_modifytable_plan on non-ModifyTablePath"),
    };
    let subpath = p.subpath.expect("create_modifytable_plan: ModifyTablePath has no subpath");
    let operation = p.operation;
    let can_set_tag = p.canSetTag;
    let nominal_relation = p.nominalRelation;
    let root_relation = p.rootRelation;
    let part_cols_updated = p.partColsUpdated;
    let result_relations: Vec<i32> = p.resultRelations.clone();
    let update_colnos_lists: Vec<Vec<AttrNumber>> = p.updateColnosLists.clone();
    let row_marks: Vec<NodeId> = p.rowMarks.clone();
    let onconflict = p.onconflict;
    let epq_param = p.epqParam;
    // INSERT-spine bound: WCO/RETURNING/MERGE legs need their node lists
    // resolved out of the arena into owned plan structures, which the executor
    // ModifyTable does not yet consume; defer them loudly when present.
    if !p.withCheckOptionLists.is_empty() {
        panic!("create_modifytable_plan: WITH CHECK OPTION lists not yet ported");
    }
    let returning_lists: Vec<Vec<NodeId>> = p.returningLists.clone();
    if !p.mergeActionLists.is_empty() || !p.mergeJoinConditions.is_empty() {
        panic!("create_modifytable_plan: MERGE action/join-condition lists not yet ported");
    }

    // Resolve the ON CONFLICT clause (carried as a presence marker in the path)
    // into the executor-shaped plan data. infer_arbiter_indexes needs &mut root,
    // so do it here before recursing into the subplan. (createplan.c:7220)
    let onconflict_data: Option<OnConflictPlanData<'mcx>> = if onconflict.is_some() {
        Some(resolve_onconflict_plan_data(mcx, root, run)?)
    } else {
        None
    };

    // Subplan must produce exactly the specified tlist.
    let mut subplan = create_plan_recurse(mcx, root, run, subpath, CP_EXACT_TLIST)?;

    // Transfer resname/resjunk labeling, too, to keep executor happy. C calls
    // apply_tlist_labeling(subplan->targetlist, root->processed_tlist); the
    // installed seam labels the plan's top tlist from root->processed_tlist.
    cp_seam::apply_tlist_labeling::call(mcx, root, &mut subplan)?;

    let mut plan = make_modifytable(
        mcx,
        root,
        run,
        subplan,
        operation,
        can_set_tag,
        nominal_relation,
        root_relation,
        part_cols_updated,
        result_relations,
        update_colnos_lists,
        returning_lists,
        row_marks,
        onconflict_data,
        epq_param,
    )?;

    copy_generic_path_info(&mut plan.plan, root.path(best_path).base());

    Ok(Node::mk_modify_table(mcx, plan))
}

/// `make_modifytable(...)` (createplan.c:7169) — build a `ModifyTable` plan node
/// atop `subplan`. The plain-table INSERT/UPDATE/DELETE spine: no ON CONFLICT
/// (arbiter inference), no foreign-table FDW plans.
#[allow(clippy::too_many_arguments)]
fn make_modifytable<'mcx>(
    mcx: Mcx<'mcx>,
    root: &PlannerInfo,
    run: &PlannerRun<'mcx>,
    subplan: Node<'mcx>,
    operation: types_pathnodes::CmdType,
    can_set_tag: bool,
    nominal_relation: Index,
    root_relation: Index,
    part_cols_updated: bool,
    result_relations: Vec<i32>,
    update_colnos_lists: Vec<Vec<AttrNumber>>,
    returning_lists: Vec<Vec<NodeId>>,
    row_marks: Vec<NodeId>,
    onconflict: Option<OnConflictPlanData<'mcx>>,
    epq_param: i32,
) -> PgResult<types_nodes::modifytable::ModifyTable<'mcx>> {
    use types_nodes::modifytable::ModifyTable;

    // Assert(operation == CMD_MERGE || (operation == CMD_UPDATE ?
    //   list_length(resultRelations) == list_length(updateColnosLists) :
    //   updateColnosLists == NIL));
    debug_assert!(
        operation == types_pathnodes::CMD_MERGE
            || if operation == types_pathnodes::CMD_UPDATE {
                result_relations.len() == update_colnos_lists.len()
            } else {
                update_colnos_lists.is_empty()
            }
    );

    // resultRelations -> integer List of RT indexes.
    let result_relations_field: Option<PgVec<'mcx, Index>> = if result_relations.is_empty() {
        None
    } else {
        let mut v = vec_with_capacity_in(mcx, result_relations.len())?;
        for &r in &result_relations {
            v.push(r as Index);
        }
        Some(v)
    };

    // updateColnosLists -> List of integer Lists.
    let update_colnos_lists_field: Option<PgVec<'mcx, PgVec<'mcx, i32>>> =
        if update_colnos_lists.is_empty() {
            None
        } else {
            let mut outer = vec_with_capacity_in(mcx, update_colnos_lists.len())?;
            for sub in &update_colnos_lists {
                let mut inner = vec_with_capacity_in(mcx, sub.len())?;
                for &c in sub {
                    inner.push(c as i32);
                }
                outer.push(inner);
            }
            Some(outer)
        };

    // rowMarks -> List of PlanRowMark nodes. Empty on the INSERT spine; a
    // non-empty list is a locking/EvalPlanQual path deferred to that family.
    if !row_marks.is_empty() {
        panic!(
            "make_modifytable: rowMarks (PlanRowMark list) resolution not yet ported \
             (needs the PlanRowMark carrier)"
        );
    }

    // returningLists -> List of per-result-rel RETURNING tlists. Resolve each
    // arena handle list back to an owned TargetEntry list (setrefs.c later
    // fix-ups the Var references via set_returning_clause_references).
    let returning_lists_field: Option<PgVec<'mcx, PgVec<'mcx, TargetEntry<'mcx>>>> =
        if returning_lists.is_empty() {
            None
        } else {
            let mut outer = vec_with_capacity_in(mcx, returning_lists.len())?;
            for sub in &returning_lists {
                let owned = resolve_targetentry_list(mcx, root, sub)?;
                let mut inner = vec_with_capacity_in(mcx, owned.len())?;
                for tle in owned {
                    inner.push(tle);
                }
                outer.push(inner);
            }
            Some(outer)
        };

    // returningOldAlias / returningNewAlias come off root->parse.
    let parse = run.resolve(root.parse);
    let returning_old_alias = match &parse.returningOldAlias {
        Some(s) => Some(mcx::slice_in(mcx, s.as_bytes())?.into_boxed_slice()),
        None => None,
    };
    let returning_new_alias = match &parse.returningNewAlias {
        Some(s) => Some(mcx::slice_in(mcx, s.as_bytes())?.into_boxed_slice()),
        None => None,
    };

    // FDW per-result-relation private data. For each result relation that is a
    // foreign table, the FDW would construct private plan data; plain tables
    // have no FdwRoutine, so fdw_private is NIL (None) for each. A foreign
    // result relation is deferred to the FDW campaign.
    let mut fdw_priv_lists: PgVec<'mcx, Option<PgBox<'mcx, Node<'mcx>>>> =
        vec_with_capacity_in(mcx, result_relations.len())?;
    for &rti_i in &result_relations {
        let rti = rti_i as Index;
        // If possible, get the FdwRoutine from our RelOptInfo; else the hard way.
        // (INSERT targets aren't scanned, so they're usually not baserels.)
        let rel_slot = root
            .simple_rel_array
            .get(rti as usize)
            .copied()
            .flatten();
        let is_foreign = match rel_slot {
            Some(rel_id) => root.rel(rel_id).fdwroutine.is_some(),
            None => {
                let rte = planner_rt_fetch(run, root, rti);
                rte.rtekind == types_nodes::parsenodes::RTEKind::RTE_RELATION
                    && rte.relkind == RELKIND_FOREIGN_TABLE as i8
            }
        };
        if is_foreign {
            panic!(
                "make_modifytable: foreign-table result relation (FDW PlanForeignModify / \
                 PlanDirectModify) not yet ported (FDW campaign)"
            );
        }
        // Plain table: fdwroutine == NULL, fdw_private = NIL.
        fdw_priv_lists.push(None);
    }

    let plan_base = {
        let mut p = Plan::default();
        // setrefs.c will fill in the targetlist, if needed.
        p.targetlist = None;
        p.qual = None;
        p.lefttree = Some(mcx::alloc_in(mcx, subplan)?);
        p.righttree = None;
        p
    };

    // Project the resolved ON CONFLICT data into the node fields, or the empty
    // !onconflict defaults (createplan.c:7211).
    let (
        oc_action,
        oc_arbiter_indexes,
        oc_set,
        oc_cols,
        oc_where,
        oc_excl_rel_rti,
        oc_excl_rel_tlist,
    ) = match onconflict {
        None => (
            types_nodes::nodes::OnConflictAction::ONCONFLICT_NONE,
            None,
            None,
            None,
            None,
            0,
            None,
        ),
        Some(d) => (
            d.action,
            Some(d.arbiter_indexes),
            Some(d.on_conflict_set),
            Some(d.on_conflict_cols),
            Some(d.on_conflict_where),
            d.excl_rel_rti,
            Some(d.excl_rel_tlist),
        ),
    };

    let node = ModifyTable {
        plan: plan_base,
        operation: cmdtype_path_to_node(operation),
        canSetTag: can_set_tag,
        nominalRelation: nominal_relation,
        rootRelation: root_relation,
        partColsUpdated: part_cols_updated,
        resultRelations: result_relations_field,
        updateColnosLists: update_colnos_lists_field,
        withCheckOptionLists: None,
        returningOldAlias: returning_old_alias,
        returningNewAlias: returning_new_alias,
        returningLists: returning_lists_field,
        fdwPrivLists: Some(fdw_priv_lists),
        fdwDirectModifyPlans: None,
        rowMarks: None,
        epqParam: epq_param,
        // ON CONFLICT fields. !onconflict => all empty/none (createplan.c:7211).
        onConflictAction: oc_action,
        arbiterIndexes: oc_arbiter_indexes,
        onConflictSet: oc_set,
        onConflictCols: oc_cols,
        onConflictWhere: oc_where,
        exclRelRTI: oc_excl_rel_rti,
        exclRelTlist: oc_excl_rel_tlist,
        mergeActionLists: None,
        mergeJoinConditions: None,
    };
    Ok(node)
}

/// Resolved, executor-shaped ON CONFLICT data, mirroring the
/// `createplan.c:7220` else-branch field assignments. Built by
/// [`resolve_onconflict_plan_data`] (which needs `&mut PlannerInfo` for
/// `infer_arbiter_indexes`) and consumed by `make_modifytable`.
struct OnConflictPlanData<'mcx> {
    action: types_nodes::nodes::OnConflictAction,
    arbiter_indexes: PgVec<'mcx, Oid>,
    on_conflict_set: PgVec<'mcx, TargetEntry<'mcx>>,
    on_conflict_cols: PgVec<'mcx, i32>,
    on_conflict_where: PgVec<'mcx, Expr>,
    excl_rel_rti: Index,
    excl_rel_tlist: PgVec<'mcx, TargetEntry<'mcx>>,
}

/// Resolve `root->parse->onConflict` (an owned `OnConflictExpr`) into the
/// executor-shaped `OnConflictPlanData`, mirroring `make_modifytable`'s
/// else-branch (createplan.c:7220-7245):
///   * onConflictSet TLEs are renumbered to consecutive resnos; the original
///     target column numbers become `onConflictCols`
///     (`extract_update_targetlist_colnos`);
///   * onConflictWhere is the implicit-AND list of qual `Expr`s;
///   * arbiterIndexes come from `infer_arbiter_indexes(root)`;
///   * exclRelRTI / exclRelTlist are copied straight through.
fn resolve_onconflict_plan_data<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
) -> PgResult<OnConflictPlanData<'mcx>> {
    // Snapshot the owned OnConflictExpr out of parse (deep copy) so we can drop
    // the borrow on root and still call infer_arbiter_indexes(&mut root).
    let oc = {
        let parse = run.resolve(root.parse);
        let oc = parse
            .onConflict
            .as_deref()
            .expect("resolve_onconflict_plan_data: parse->onConflict is None");
        oc.clone_in(mcx)?
    };

    let action = oc.action;

    // onConflictSet: list of TargetEntry nodes -> owned TLE vec, renumbered to
    // consecutive resnos with the original resnos captured as onConflictCols.
    let mut on_conflict_set: PgVec<'mcx, TargetEntry<'mcx>> =
        vec_with_capacity_in(mcx, oc.onConflictSet.len())?;
    let mut on_conflict_cols: PgVec<'mcx, i32> = vec_with_capacity_in(mcx, oc.onConflictSet.len())?;
    let mut nextresno: AttrNumber = 1;
    for n in oc.onConflictSet.iter() {
        let tle = n.as_ref().as_targetentry().unwrap_or_else(|| {
            panic!("onConflictSet element is not a TargetEntry (got {:?})", n.as_ref().tag())
        });
        let mut tle = tle.clone_in(mcx)?;
        if !tle.resjunk {
            on_conflict_cols.push(tle.resno as i32);
        }
        tle.resno = nextresno;
        nextresno += 1;
        on_conflict_set.push(tle);
    }

    // onConflictWhere: modeled as the implicit-AND list of Expr fed to
    // ExecInitQual. Flatten an AND tree / single qual into a Vec<Expr>.
    let mut on_conflict_where: PgVec<'mcx, Expr> = PgVec::new_in(mcx);
    if let Some(np) = oc.onConflictWhere.as_deref() {
        let e = np
            .as_expr()
            .unwrap_or_else(|| panic!("onConflictWhere is not an Expr (got {:?})", np.tag()))
            .clone_in(mcx)?;
        for q in backend_nodes_core::makefuncs::make_ands_implicit(Some(e)) {
            on_conflict_where.push(q);
        }
    }

    // exclRelRTI / exclRelTlist (only meaningful for DO UPDATE; 0 / empty for
    // DO NOTHING, which carries no EXCLUDED relation).
    let excl_rel_rti = oc.exclRelIndex as Index;
    let mut excl_rel_tlist: PgVec<'mcx, TargetEntry<'mcx>> =
        vec_with_capacity_in(mcx, oc.exclRelTlist.len())?;
    for n in oc.exclRelTlist.iter() {
        let tle = n.as_ref().as_targetentry().unwrap_or_else(|| {
            panic!("exclRelTlist element is not a TargetEntry (got {:?})", n.as_ref().tag())
        });
        excl_rel_tlist.push(tle.clone_in(mcx)?);
    }

    // arbiterIndexes = infer_arbiter_indexes(root) (createplan.c:7242).
    let arbiter_oids = backend_optimizer_util_plancat::infer_arbiter_indexes(run, root)?;
    let mut arbiter_indexes: PgVec<'mcx, Oid> = vec_with_capacity_in(mcx, arbiter_oids.len())?;
    for oid in arbiter_oids {
        arbiter_indexes.push(oid);
    }

    Ok(OnConflictPlanData {
        action,
        arbiter_indexes,
        on_conflict_set,
        on_conflict_cols,
        on_conflict_where,
        excl_rel_rti,
        excl_rel_tlist,
    })
}

/// Map the path-layer `CmdType` (raw `u32`) to the plan-node `CmdType`.
fn cmdtype_path_to_node(op: types_pathnodes::CmdType) -> types_nodes::nodes::CmdType {
    use types_nodes::nodes::CmdType as N;
    match op {
        types_pathnodes::CMD_UNKNOWN => N::CMD_UNKNOWN,
        types_pathnodes::CMD_SELECT => N::CMD_SELECT,
        types_pathnodes::CMD_UPDATE => N::CMD_UPDATE,
        types_pathnodes::CMD_INSERT => N::CMD_INSERT,
        types_pathnodes::CMD_DELETE => N::CMD_DELETE,
        types_pathnodes::CMD_MERGE => N::CMD_MERGE,
        _ => N::CMD_UTILITY,
    }
}

/// Map the path-layer `LimitOption` (types-pathnodes) to the plan-node
/// `LimitOption` (types-nodes). They are the same C enum; the two layers carry
/// distinct Rust types.
fn limit_option_to_node(
    opt: types_pathnodes::LimitOption,
) -> types_nodes::nodelimit::LimitOption {
    if opt == types_pathnodes::LIMIT_OPTION_WITH_TIES {
        types_nodes::nodelimit::LIMIT_OPTION_WITH_TIES
    } else {
        types_nodes::nodelimit::LIMIT_OPTION_COUNT
    }
}

// ===========================================================================
// Agg family: create_agg_plan + make_agg (createplan.c).
// ===========================================================================

/// `make_agg(tlist, qual, aggstrategy, aggsplit, numGroupCols, grpColIdx,
/// grpOperators, grpCollations, groupingSets, chain, dNumGroups,
/// transitionSpace, lefttree)` (createplan.c:6731) — build an `Agg` plan node
/// atop `lefttree`. Agg can project, so the tlist is the caller-supplied one
/// (built by `build_path_tlist`), not the child's.
#[allow(clippy::too_many_arguments)]
fn make_agg<'mcx>(
    mcx: Mcx<'mcx>,
    tlist: Option<PgVec<'mcx, TargetEntry<'mcx>>>,
    qual: Option<PgVec<'mcx, Expr>>,
    aggstrategy: types_nodes::nodeagg::AggStrategy,
    aggsplit: types_nodes::nodeagg::AggSplit,
    num_group_cols: i32,
    grp_col_idx: Vec<AttrNumber>,
    grp_operators: Vec<Oid>,
    grp_collations: Vec<Oid>,
    grouping_sets: Option<PgVec<'mcx, PgVec<'mcx, i32>>>,
    chain: Option<PgVec<'mcx, PgBox<'mcx, types_nodes::nodeagg::Agg<'mcx>>>>,
    d_num_groups: f64,
    transition_space: u64,
    lefttree: Node<'mcx>,
) -> PgResult<types_nodes::nodeagg::Agg<'mcx>> {
    // Reduce to long, but 'ware overflow! (clamp_cardinality_to_long).
    let num_groups = costsize::clamp_cardinality_to_long::call(d_num_groups);

    let mut node = types_nodes::nodeagg::Agg::default();
    node.aggstrategy = aggstrategy;
    node.aggsplit = aggsplit;
    node.num_cols = num_group_cols;
    // node->grpColIdx / grpOperators / grpCollations: the C palloc'd arrays. In
    // the owned model these are `Option<PgVec<..>>`; an empty group list (the
    // plain-aggregate count(*) case, numGroupCols == 0) maps to `None`, mirroring
    // a zero-length array.
    node.grp_col_idx = attrnum_vec_to_field(mcx, grp_col_idx)?;
    node.grp_operators = oid_vec_to_field(mcx, grp_operators)?;
    node.grp_collations = oid_vec_to_field(mcx, grp_collations)?;
    node.num_groups = num_groups;
    node.transition_space = transition_space;
    // node->aggParams = NULL — SS_finalize_plan() will fill this.
    node.agg_params = None;
    node.grouping_sets = grouping_sets;
    node.chain = chain;

    {
        let plan: &mut Plan = &mut node.plan;
        plan.qual = qual;
        plan.targetlist = tlist;
        plan.lefttree = Some(mcx::alloc_in(mcx, lefttree)?);
        plan.righttree = None;
    }

    Ok(node)
}

/// Convert a `Vec<AttrNumber>` (the C palloc'd `AttrNumber *` array) into the
/// owned plan-node field (`Option<PgVec<AttrNumber>>`); an empty list is `None`.
fn attrnum_vec_to_field<'mcx>(
    mcx: Mcx<'mcx>,
    v: Vec<AttrNumber>,
) -> PgResult<Option<PgVec<'mcx, AttrNumber>>> {
    if v.is_empty() {
        return Ok(None);
    }
    let mut out = vec_with_capacity_in(mcx, v.len())?;
    for x in v {
        out.push(x);
    }
    Ok(Some(out))
}

/// Convert a `Vec<Oid>` (the C palloc'd `Oid *` array) into the owned plan-node
/// field (`Option<PgVec<Oid>>`); an empty list is `None`.
fn oid_vec_to_field<'mcx>(
    mcx: Mcx<'mcx>,
    v: Vec<Oid>,
) -> PgResult<Option<PgVec<'mcx, Oid>>> {
    if v.is_empty() {
        return Ok(None);
    }
    let mut out = vec_with_capacity_in(mcx, v.len())?;
    for x in v {
        out.push(x);
    }
    Ok(Some(out))
}

/// Map the path-layer `AggStrategy` (types-pathnodes `u32`, the raw C enum) to
/// the plan-node `AggStrategy` (types-nodes). Same C enum, distinct Rust types.
fn aggstrategy_path_to_node(
    s: types_pathnodes::AggStrategy,
) -> types_nodes::nodeagg::AggStrategy {
    use types_nodes::nodeagg::AggStrategy as NS;
    match s {
        types_pathnodes::AGG_PLAIN => NS::AggPlain,
        types_pathnodes::AGG_SORTED => NS::AggSorted,
        types_pathnodes::AGG_HASHED => NS::AggHashed,
        types_pathnodes::AGG_MIXED => NS::AggMixed,
        _ => NS::AggPlain,
    }
}

/// `create_agg_plan(root, (AggPath *) best_path)` (createplan.c:2304) — create
/// an `Agg` plan for `best_path` and (recursively) plans for its subpaths.
fn create_agg_plan<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    best_path: PathId,
) -> PgResult<Node<'mcx>> {
    let (subpath, aggstrategy, aggsplit, group_clause_ids, qual_ids, num_groups, transition_space) =
        match root.path(best_path) {
            PathNode::AggPath(p) => (
                p.subpath,
                p.aggstrategy,
                p.aggsplit,
                p.groupClause.clone(),
                p.qual.clone(),
                p.numGroups,
                p.transitionSpace,
            ),
            _ => unreachable!("create_agg_plan on non-AggPath"),
        };
    let subpath = subpath.expect("create_agg_plan: AggPath has no subpath");

    // Agg can project, so no need to be terribly picky about child tlist, but we
    // do need grouping columns to be available.
    let subplan = create_plan_recurse(mcx, root, run, subpath, CP_LABEL_TLIST)?;

    let tlist = build_path_tlist(mcx, root, best_path)?;
    let tlist = tlist_to_plan_field(mcx, tlist)?;

    let quals = order_qual_clauses(root, &qual_ids);
    let quals = nodes_to_expr_qual(mcx, root, &quals)?;

    // Resolve the SortGroupClause list out of the arena (handles → values), then
    // derive the grouping-column arrays from the *subplan's* targetlist (C reads
    // subplan->targetlist for extract_grouping_cols/collations). For a plain
    // aggregate (count(*)) groupClause is NIL, so all three arrays are empty.
    let group_clauses: Vec<types_nodes::rawnodes::SortGroupClause> =
        group_clause_ids.iter().map(|&id| *root.sortgroupclause(id)).collect();
    let num_group_cols = group_clauses.len() as i32;

    let subplan_tlist: &[TargetEntry<'mcx>] = match subplan.plan_head().targetlist {
        Some(ref tl) => tl.as_slice(),
        None => &[],
    };
    let grp_col_idx = util_tlist::extract_grouping_cols(&group_clauses, subplan_tlist)?;
    let grp_operators = util_tlist::extract_grouping_ops(&group_clauses);
    let grp_collations =
        util_tlist::extract_grouping_collations(&group_clauses, subplan_tlist)?;

    // The path layer carries AggStrategy/AggSplit as the raw C enum integers
    // (types-pathnodes u32); the plan node uses the typed (types-nodes) forms.
    let aggstrategy = aggstrategy_path_to_node(aggstrategy);
    let aggsplit = aggsplit as types_nodes::nodeagg::AggSplit;

    let mut plan = make_agg(
        mcx,
        tlist,
        quals,
        aggstrategy,
        aggsplit,
        num_group_cols,
        grp_col_idx,
        grp_operators,
        grp_collations,
        None, // groupingSets = NIL
        None, // chain = NIL
        num_groups,
        transition_space,
        subplan,
    )?;

    copy_generic_path_info(&mut plan.plan, root.path(best_path).base());

    Ok(Node::mk_agg(mcx, plan))
}

/// `create_agg_dispatch_plan` — the `T_Agg` createplan arm. C discriminates on
/// `IsA(best_path, GroupingSetsPath)` between `create_groupingsets_plan` and
/// `create_agg_plan`; the dispatch routes the whole `T_Agg` pathtype here.
fn create_agg_dispatch_plan<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    best_path: PathId,
) -> PgResult<Node<'mcx>> {
    match root.path(best_path) {
        PathNode::AggPath(_) => create_agg_plan(mcx, root, run, best_path),
        PathNode::GroupingSetsPath(_) => create_groupingsets_plan(mcx, root, run, best_path),
        _ => unreachable!("create_agg_dispatch_plan on non-Agg pathtype"),
    }
}

/// `remap_groupColIdx(root, groupClause)` (createplan.c:2351) — translate a
/// rollup's groupClause sortgrouprefs into child-tlist column indexes via
/// `root->grouping_map`.
fn remap_group_col_idx(
    root: &PlannerInfo,
    group_clause: &[types_nodes::rawnodes::SortGroupClause],
) -> Vec<AttrNumber> {
    let grouping_map = &root.grouping_map;
    debug_assert!(!grouping_map.is_empty());
    group_clause
        .iter()
        .map(|clause| grouping_map[clause.tleSortGroupRef as usize])
        .collect()
}

/// `create_groupingsets_plan(root, (GroupingSetsPath *) best_path)`
/// (createplan.c:2389) — convert a `GroupingSetsPath` into the chained `Agg`
/// (with vestigial side `Agg`/`Sort` nodes) that implements GROUPING SETS.
fn create_groupingsets_plan<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    best_path: PathId,
) -> PgResult<Node<'mcx>> {
    let (subpath, aggstrategy, rollups, qual_ids, transition_space) = match root.path(best_path) {
        PathNode::GroupingSetsPath(p) => (
            p.subpath,
            p.aggstrategy,
            p.rollups.clone(),
            p.qual.clone(),
            p.transitionSpace,
        ),
        _ => unreachable!("create_groupingsets_plan on non-GroupingSetsPath"),
    };
    let subpath = subpath.expect("create_groupingsets_plan: GroupingSetsPath has no subpath");

    // Shouldn't get here without grouping sets.
    debug_assert!(!rollups.is_empty());

    // Agg can project, so no need to be terribly picky about child tlist, but we
    // do need grouping columns to be available.
    let subplan = create_plan_recurse(mcx, root, run, subpath, CP_LABEL_TLIST)?;

    // Compute the mapping from tleSortGroupRef to column index in the child's
    // tlist. First, identify max SortGroupRef in groupClause, for array sizing.
    let processed_group_clause: Vec<types_nodes::rawnodes::SortGroupClause> = root
        .processed_groupClause
        .iter()
        .map(|&id| *root.sortgroupclause(id))
        .collect();
    let mut maxref: Index = 0;
    for gc in processed_group_clause.iter() {
        if gc.tleSortGroupRef > maxref {
            maxref = gc.tleSortGroupRef;
        }
    }

    // grouping_map = palloc0((maxref + 1) * sizeof(AttrNumber)); then look up the
    // column numbers in the child's tlist.
    let mut grouping_map: Vec<AttrNumber> = alloc::vec![0; (maxref as usize) + 1];
    {
        let sub_tlist: &[TargetEntry<'mcx>] =
            subplan.plan_head().targetlist.as_deref().unwrap_or(&[]);
        for gc in processed_group_clause.iter() {
            let tle = util_tlist::get_sortgroupclause_tle(gc, sub_tlist)?;
            grouping_map[gc.tleSortGroupRef as usize] = tle.resno;
        }
    }

    // During setrefs.c, we'll need the grouping_map to fix up the cols lists in
    // GroupingFunc nodes. Save it for setrefs.c to use.
    debug_assert!(root.grouping_map.is_empty());
    root.grouping_map = grouping_map;

    // Resolve each rollup's groupClause handles into owned SortGroupClause lists
    // up front (the C `rollup->groupClause` Lists).
    let rollup_group_clauses: Vec<Vec<types_nodes::rawnodes::SortGroupClause>> = rollups
        .iter()
        .map(|r| r.groupClause.iter().map(|&id| *root.sortgroupclause(id)).collect())
        .collect();

    // Generate the side nodes that describe the other sort and group operations
    // besides the top one. We don't worry about accurate cost estimates in the
    // side nodes; only the topmost Agg node's costs will be shown by EXPLAIN.
    let mut chain: Vec<PgBox<'mcx, types_nodes::nodeagg::Agg<'mcx>>> = Vec::new();
    if rollups.len() > 1 {
        let mut is_first_sort = rollups[0].is_hashed;

        for idx in 1..rollups.len() {
            let rollup = &rollups[idx];
            let group_clause = &rollup_group_clauses[idx];
            let new_grp_col_idx = remap_group_col_idx(root, group_clause);

            let sort_plan: Option<Node<'mcx>> = if !rollup.is_hashed && !is_first_sort {
                // C builds the Sort over `subplan` (shared pointer). In the
                // owned model subplan is consumed by the top Agg, so the side
                // Sort gets a placeholder lefttree carrying a clone of subplan's
                // targetlist (all make_sort_from_groupcols needs to resolve sort
                // columns); the tlist/lefttree are then stripped below, matching
                // C's `sort_plan->targetlist = NIL; sort_plan->lefttree = NULL`.
                let placeholder = dummy_plan_with_tlist(mcx, &subplan)?;
                let sort =
                    make_sort_from_groupcols(mcx, group_clause, &new_grp_col_idx, placeholder)?;
                Some(Node::mk_sort(mcx, sort))
            } else {
                None
            };

            if !rollup.is_hashed {
                is_first_sort = false;
            }

            let strat = if rollup.is_hashed {
                types_nodes::nodeagg::AggStrategy::AggHashed
            } else if rollup.gsets.first().map(|g| g.is_empty()).unwrap_or(true) {
                types_nodes::nodeagg::AggStrategy::AggPlain
            } else {
                types_nodes::nodeagg::AggStrategy::AggSorted
            };

            let num_group_cols = rollup.gsets.first().map(|g| g.len()).unwrap_or(0) as i32;
            let grp_operators = util_tlist::extract_grouping_ops(group_clause);
            let grp_collations = {
                let sub_tlist: &[TargetEntry<'mcx>] =
                    subplan.plan_head().targetlist.as_deref().unwrap_or(&[]);
                util_tlist::extract_grouping_collations(group_clause, sub_tlist)?
            };
            let gsets = gsets_to_field(mcx, &rollup.gsets)?;

            // The C side-Agg lefttree is the Sort (or NULL). We give it the Sort
            // (or a dummy placeholder), then immediately strip the Sort's
            // tlist/lefttree, matching C's "Remove stuff we don't need to avoid
            // bloating debug output".
            let lefttree = match sort_plan {
                Some(mut sp) => {
                    let p = sp.plan_head_mut();
                    p.targetlist = None;
                    p.lefttree = None;
                    sp
                }
                None => dummy_plan(mcx)?,
            };

            let agg_plan = make_agg(
                mcx,
                None, // tlist = NIL
                None, // qual = NIL
                strat,
                AGGSPLIT_SIMPLE,
                num_group_cols,
                new_grp_col_idx,
                grp_operators,
                grp_collations,
                gsets,
                None, // chain = NIL
                rollup.numGroups,
                transition_space,
                lefttree,
            )?;
            chain.push(mcx::alloc_in(mcx, agg_plan)?);
        }
    }

    // Now make the real Agg node.
    let rollup = &rollups[0];
    let group_clause = &rollup_group_clauses[0];
    let top_grp_col_idx = remap_group_col_idx(root, group_clause);
    let num_group_cols = rollup.gsets.first().map(|g| g.len()).unwrap_or(0) as i32;

    let tlist = build_path_tlist(mcx, root, best_path)?;
    let tlist = tlist_to_plan_field(mcx, tlist)?;

    let quals = order_qual_clauses(root, &qual_ids);
    let quals = nodes_to_expr_qual(mcx, root, &quals)?;

    let grp_operators = util_tlist::extract_grouping_ops(group_clause);
    let grp_collations = {
        let sub_tlist: &[TargetEntry<'mcx>] =
            subplan.plan_head().targetlist.as_deref().unwrap_or(&[]);
        util_tlist::extract_grouping_collations(group_clause, sub_tlist)?
    };
    let gsets = gsets_to_field(mcx, &rollup.gsets)?;
    let chain_field = if chain.is_empty() {
        None
    } else {
        let mut out = vec_with_capacity_in(mcx, chain.len())?;
        for c in chain {
            out.push(c);
        }
        Some(out)
    };

    let aggstrategy = aggstrategy_path_to_node(aggstrategy);

    let mut plan = make_agg(
        mcx,
        tlist,
        quals,
        aggstrategy,
        AGGSPLIT_SIMPLE,
        num_group_cols,
        top_grp_col_idx,
        grp_operators,
        grp_collations,
        gsets,
        chain_field,
        rollup.numGroups,
        transition_space,
        subplan,
    )?;

    copy_generic_path_info(&mut plan.plan, root.path(best_path).base());

    Ok(Node::mk_agg(mcx, plan))
}

/// Convert a `&[Vec<i32>]` (the C `rollup->gsets`, a `List` of integer lists)
/// into the owned grouping-sets plan field (`Option<PgVec<PgVec<i32>>>`); an
/// empty outer list is `None`.
fn gsets_to_field<'mcx>(
    mcx: Mcx<'mcx>,
    gsets: &[Vec<i32>],
) -> PgResult<Option<PgVec<'mcx, PgVec<'mcx, i32>>>> {
    if gsets.is_empty() {
        return Ok(None);
    }
    let mut out = vec_with_capacity_in(mcx, gsets.len())?;
    for inner in gsets {
        let mut iv = vec_with_capacity_in(mcx, inner.len())?;
        for &x in inner {
            iv.push(x);
        }
        out.push(iv);
    }
    Ok(Some(out))
}

/// A throwaway empty `Result` plan node used as a placeholder lefttree for the
/// vestigial side `Agg`/`Sort` nodes that hang off a grouping-sets plan's
/// `chain`. These nodes never execute (the C side nodes carry `lefttree =
/// NULL`); the placeholder exists only because the owned plan model requires
/// every node to own its child.
fn dummy_plan<'mcx>(mcx: Mcx<'mcx>) -> PgResult<Node<'mcx>> {
    let _ = mcx;
    Ok(Node::mk_result(mcx, ResultNode::default()))
}

/// As [`dummy_plan`], but the placeholder carries a clone of `src`'s
/// targetlist, so a `make_sort_from_groupcols` over it can resolve its sort
/// columns out of the (subplan's) tlist before the tlist is stripped.
fn dummy_plan_with_tlist<'mcx>(mcx: Mcx<'mcx>, src: &Node<'mcx>) -> PgResult<Node<'mcx>> {
    let mut node = ResultNode::default();
    node.plan.targetlist = clone_plan_tlist(mcx, src)?;
    Ok(Node::mk_result(mcx, node))
}

// ---------------------------------------------------------------------------
// create_projection_plan (createplan.c ~2015)
// ---------------------------------------------------------------------------

/// `create_projection_plan(root, best_path, flags)` — create a plan tree to do
/// a projection step. We may need a `Result` node, but often we can let the
/// subplan do the projection itself.
fn create_projection_plan<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    best_path: PathId,
    flags: i32,
) -> PgResult<Node<'mcx>> {
    let subpath = match root.path(best_path) {
        PathNode::ProjectionPath(p) => p.subpath,
        _ => unreachable!("create_projection_plan on non-ProjectionPath"),
    }
    .expect("create_projection_plan: ProjectionPath has no subpath");

    let mut needs_result_node = false;

    // Convert our subpath to a Plan and determine whether we need a Result node.
    // (See the long comment in createplan.c: dummypp is unreliable, recheck.)
    let (mut subplan, tlist): (Node<'mcx>, Vec<TargetEntry<'mcx>>) =
        if use_physical_tlist(root, best_path, flags) {
            // Caller doesn't care what tlist we return — no need to project,
            // though we may still need sortgroupref labels.
            let subplan = create_plan_recurse(mcx, root, run, subpath, 0)?;
            let mut tlist = clone_node_tlist_vec(mcx, &subplan)?;
            if flags & CP_LABEL_TLIST != 0 {
                let target = root
                    .path(best_path)
                    .base()
                    .pathtarget
                    .clone()
                    .expect("create_projection_plan: path has no pathtarget");
                apply_pathtarget_labeling_to_tlist(root, &mut tlist, &target)?;
            }
            (subplan, tlist)
        } else if is_projection_capable_path(root, subpath) {
            // Caller requires the exact tlist, but no separate Result node is
            // needed because the subpath is projection-capable. Tell
            // create_plan_recurse we'll ignore the tlist it produces.
            let subplan = create_plan_recurse(mcx, root, run, subpath, CP_IGNORE_TLIST)?;
            debug_assert!(is_projection_capable_plan(&subplan));
            let tlist = build_path_tlist(mcx, root, best_path)?;
            (subplan, tlist)
        } else {
            // It looks like we need a result node, unless by good fortune the
            // requested tlist is exactly the one the child wants to produce.
            let subplan = create_plan_recurse(mcx, root, run, subpath, 0)?;
            let tlist = build_path_tlist(mcx, root, best_path)?;
            let sub_tlist = subplan.plan_head().targetlist.as_deref().unwrap_or(&[]);
            needs_result_node = !tlist_same_exprs(&tlist, sub_tlist);
            (subplan, tlist)
        };

    if !needs_result_node {
        // Don't need a separate Result, just assign tlist to subplan, and label
        // the subplan with the cost estimates we actually used.
        let base = root.path(best_path).base().clone();
        let width = base
            .pathtarget
            .as_ref()
            .expect("create_projection_plan: path has no pathtarget")
            .width;
        let plan_hd: &mut Plan = subplan.plan_head_mut();
        plan_hd.targetlist = tlist_to_plan_field(mcx, tlist)?;
        plan_hd.startup_cost = base.startup_cost;
        plan_hd.total_cost = base.total_cost;
        plan_hd.plan_rows = base.rows;
        plan_hd.plan_width = width;
        plan_hd.parallel_safe = base.parallel_safe;
        // ... but don't change subplan's parallel_aware flag.
        Ok(subplan)
    } else {
        // We need a Result node.
        let tlist = tlist_to_plan_field(mcx, tlist)?;
        let mut plan = make_result(mcx, tlist, None, Some(subplan))?;
        copy_generic_path_info(&mut plan.plan, root.path(best_path).base());
        Ok(Node::mk_result(mcx, plan))
    }
}

/// Clone a plan node's targetlist into an owned `Vec<TargetEntry>` (the C
/// `tlist = subplan->targetlist` alias). Returns an empty `Vec` for `NIL`.
fn clone_node_tlist_vec<'mcx>(
    mcx: Mcx<'mcx>,
    plan_node: &Node<'mcx>,
) -> PgResult<Vec<TargetEntry<'mcx>>> {
    match plan_node.plan_head().targetlist {
        None => Ok(Vec::new()),
        Some(ref tl) => {
            let mut out = Vec::with_capacity(tl.len());
            for tle in tl.iter() {
                out.push(tle.clone_in(mcx)?);
            }
            Ok(out)
        }
    }
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
/// `is_projection_capable_path(path)` (createplan.c:7392) — can the plan this
/// Path would produce do projection? Most plan types can; this lists the ones
/// that can't. Read off `path->pathtype` (the plan-node tag), with the two
/// data-dependent cases (CustomScan flags, dummy Append) inspecting the
/// `PathNode` variant.
pub fn is_projection_capable_path(root: &PlannerInfo, path: PathId) -> bool {
    use types_nodes::nodes as ntag;
    let node = root.path(path);
    let pathtype = node.base().pathtype;

    // T_Hash / T_Material / T_Memoize / T_Sort / T_IncrementalSort / T_Unique /
    // T_SetOp / T_LockRows / T_Limit / T_ModifyTable / T_MergeAppend /
    // T_RecursiveUnion — these can't project.
    if pathtype == types_nodes::nodehashjoin::T_Hash
        || pathtype == ntag::T_Material
        || pathtype == types_nodes::nodememoize::T_Memoize
        || pathtype == ntag::T_Sort
        || pathtype == types_nodes::nodeincrementalsort::T_IncrementalSort
        || pathtype == types_nodes::nodeunique::T_Unique
        || pathtype == ntag::T_SetOp
        || pathtype == ntag::T_LockRows
        || pathtype == ntag::T_Limit
        || pathtype == types_nodes::modifytable::T_ModifyTable
        || pathtype == ntag::T_MergeAppend
        || pathtype == types_nodes::noderecursiveunion::T_RecursiveUnion
    {
        return false;
    }
    if pathtype == ntag::T_CustomScan {
        // CustomScan can project iff it advertises CUSTOMPATH_SUPPORT_PROJECTION.
        if let PathNode::CustomPath(cp) = node {
            return cp.flags & types_pathnodes::CUSTOMPATH_SUPPORT_PROJECTION != 0;
        }
        return false;
    }
    if pathtype == ntag::T_Append {
        // Append can't project, but a dummy AppendPath actually generates a
        // Result, which can. IS_DUMMY_APPEND(p): IsA(p, AppendPath) && subpaths==NIL.
        return matches!(node, PathNode::AppendPath(ap) if ap.subpaths.is_empty());
    }
    if pathtype == types_nodes::nodeprojectset::T_ProjectSet {
        // ProjectSet projects, but say "no" so the planner won't replace its
        // tlist; the SRFs must stay at top level.
        return false;
    }
    true
}

// ===========================================================================
// Join family: create_join_plan dispatch + create_nestloop_plan /
// create_hashjoin_plan + make_nestloop / make_hashjoin / make_hash +
// get_switched_clauses + change_plan_targetlist + create_gating_plan
// (createplan.c). create_mergejoin_plan stays seam-panicked — see the STOP
// note at the join dispatch.
// ===========================================================================

/// `IS_OUTER_JOIN(jointype)` (nodes/nodes.h) — LEFT/FULL/RIGHT/ANTI/RIGHT_ANTI.
#[inline]
fn is_outer_join(jointype: types_pathnodes::JoinType) -> bool {
    // (1 << JOIN_LEFT) | (1 << JOIN_FULL) | (1 << JOIN_RIGHT) |
    // (1 << JOIN_ANTI) | (1 << JOIN_RIGHT_ANTI)
    const MASK: u32 = (1 << 1) | (1 << 2) | (1 << 3) | (1 << 5) | (1 << 7);
    (1u32 << jointype) & MASK != 0
}

/// Map the path-layer `JoinType` (types-pathnodes `u32`, the raw C enum) to the
/// plan-node `JoinType` (types-nodes). Same C enum, distinct Rust types.
fn jointype_path_to_node(j: types_pathnodes::JoinType) -> NodeJoinType {
    use NodeJoinType as N;
    match j {
        0 => N::JOIN_INNER,
        1 => N::JOIN_LEFT,
        2 => N::JOIN_FULL,
        3 => N::JOIN_RIGHT,
        4 => N::JOIN_SEMI,
        5 => N::JOIN_ANTI,
        6 => N::JOIN_RIGHT_SEMI,
        7 => N::JOIN_RIGHT_ANTI,
        8 => N::JOIN_UNIQUE_OUTER,
        9 => N::JOIN_UNIQUE_INNER,
        _ => N::JOIN_INNER,
    }
}

/// `change_plan_targetlist(subplan, tlist, tlist_parallel_safe)`
/// (createplan.c ~2080) — if the top plan node can't project and its existing
/// tlist isn't already what we need, inject a `Result` node; otherwise just
/// replace the plan node's tlist.
fn change_plan_targetlist<'mcx>(
    mcx: Mcx<'mcx>,
    subplan: Node<'mcx>,
    tlist: PgVec<'mcx, TargetEntry<'mcx>>,
    tlist_parallel_safe: bool,
) -> PgResult<Node<'mcx>> {
    let existing: &[TargetEntry<'mcx>] = subplan.plan_head().targetlist.as_deref().unwrap_or(&[]);
    if !is_projection_capable_plan(&subplan) && !tlist_same_exprs(&tlist, existing) {
        let child_safe = subplan.plan_head().parallel_safe;
        inject_projection_plan(mcx, subplan, Some(tlist), child_safe && tlist_parallel_safe)
    } else {
        let mut subplan = subplan;
        {
            let plan = subplan.plan_head_mut();
            plan.targetlist = Some(tlist);
            plan.parallel_safe &= tlist_parallel_safe;
        }
        Ok(subplan)
    }
}

/// `get_switched_clauses(clauses, outerrelids)` (createplan.c) — return the
/// per-`RestrictInfo` join-clause `OpExpr` expressions, rearranged so the outer
/// variable is always on the left, and mark each `RestrictInfo`'s
/// `outer_is_left` status. `clauses` is the C `List *` of `RestrictInfo *`
/// (here [`RinfoId`]s into `rinfo_arena`).
fn get_switched_clauses(
    root: &mut PlannerInfo,
    clauses: &[RinfoId],
    outerrelids: &Relids,
) -> PgResult<Vec<Expr>> {
    let mut t_list: Vec<Expr> = Vec::with_capacity(clauses.len());
    for &rid in clauses {
        let right_relids = root.rinfo(rid).right_relids.clone();
        let clause_id = root.rinfo(rid).clause;
        // (OpExpr *) restrictinfo->clause; Assert(is_opclause(clause));
        let op: OpExpr = match root.node(clause_id) {
            Expr::OpExpr(op) => op.clone(),
            other => {
                return Err(PgError::error(alloc::format!(
                    "get_switched_clauses: mergeclause/hashclause is not an OpExpr (got {:?})",
                    core::mem::discriminant(other)
                )))
            }
        };
        if bms_is_subset_relids(&right_relids, outerrelids) {
            // Duplicate just enough of the structure to allow commuting the
            // clause without changing the original list, then commute it.
            let mut temp = OpExpr {
                opno: op.opno,
                opfuncid: InvalidOid,
                opresulttype: op.opresulttype,
                opretset: op.opretset,
                opcollid: op.opcollid,
                inputcollid: op.inputcollid,
                args: op.args.clone(),
                location: op.location,
            };
            CommuteOpExpr(&mut temp)?;
            t_list.push(Expr::OpExpr(temp));
            root.rinfo_mut(rid).outer_is_left = false;
        } else {
            debug_assert!(bms_is_subset_relids(&root.rinfo(rid).left_relids, outerrelids));
            t_list.push(Expr::OpExpr(op));
            root.rinfo_mut(rid).outer_is_left = true;
        }
    }
    Ok(t_list)
}

/// `bms_is_subset(a, b)` over the `Relids` carrier (relnode seam).
#[inline]
fn bms_is_subset_relids(a: &Relids, b: &Relids) -> bool {
    relnode::relids_is_subset::call(a, b)
}

/// Resolve a list of clause expression arena handles into an owned qual
/// `Vec<Expr>` (the C qpqual / joinqual lists, post-`extract_actual_*`). Empty
/// list is the C `NIL` (`None`).
fn node_ids_to_expr_list<'mcx>(
    mcx: Mcx<'mcx>,
    root: &PlannerInfo,
    ids: &[NodeId],
) -> PgResult<Option<PgVec<'mcx, Expr>>> {
    if ids.is_empty() {
        return Ok(None);
    }
    let mut out = vec_with_capacity_in(mcx, ids.len())?;
    for &id in ids {
        out.push(root.node(id).clone());
    }
    Ok(Some(out))
}

/// `list_difference(joinclauses, switched)` over plain expression lists — drop
/// from `joinclauses` (arena [`NodeId`]s) any element `equal()` to a switched
/// clause expr; used by the merge/hash converters to remove the merge/hash
/// clauses from the qpqual list. Returns the surviving arena `NodeId`s.
fn list_difference_exprs(root: &PlannerInfo, joinclauses: &[NodeId], remove: &[Expr]) -> Vec<NodeId> {
    let mut result = Vec::new();
    for &cid in joinclauses {
        let cexpr = root.node(cid);
        let mut found = false;
        for r in remove {
            if equal_expr_seam::call(cexpr, r) {
                found = true;
                break;
            }
        }
        if !found {
            result.push(cid);
        }
    }
    result
}

/// `make_nestloop(tlist, joinclauses, otherclauses, nestParams, lefttree,
/// righttree, jointype, inner_unique)` (createplan.c:6083).
#[allow(clippy::too_many_arguments)]
fn make_nestloop<'mcx>(
    mcx: Mcx<'mcx>,
    tlist: Option<PgVec<'mcx, TargetEntry<'mcx>>>,
    joinclauses: Option<PgVec<'mcx, Expr>>,
    otherclauses: Option<PgVec<'mcx, Expr>>,
    nest_params: Vec<NestLoopParam>,
    lefttree: Node<'mcx>,
    righttree: Node<'mcx>,
    jointype: NodeJoinType,
    inner_unique: bool,
) -> PgResult<NestLoop<'mcx>> {
    let mut node = NestLoop::default();
    {
        let plan: &mut Plan = &mut node.join.plan;
        plan.targetlist = tlist;
        plan.qual = otherclauses;
        plan.lefttree = Some(mcx::alloc_in(mcx, lefttree)?);
        plan.righttree = Some(mcx::alloc_in(mcx, righttree)?);
    }
    node.join.jointype = jointype;
    node.join.inner_unique = inner_unique;
    node.join.joinqual = joinclauses;
    node.nestParams = nest_params;
    Ok(node)
}

/// `make_hash(lefttree, hashkeys, skewTable, skewColumn, skewInherit)`
/// (createplan.c:6139).
fn make_hash<'mcx>(
    mcx: Mcx<'mcx>,
    lefttree: Node<'mcx>,
    hashkeys: Option<PgVec<'mcx, Node<'mcx>>>,
    skew_table: Oid,
    skew_column: AttrNumber,
    skew_inherit: bool,
) -> PgResult<HashNode<'mcx>> {
    // plan->targetlist = lefttree->targetlist;
    let tlist = clone_plan_tlist(mcx, &lefttree)?;
    let mut node = HashNode::default();
    {
        let plan: &mut Plan = &mut node.plan;
        plan.targetlist = tlist;
        plan.qual = None;
        plan.lefttree = Some(mcx::alloc_in(mcx, lefttree)?);
        plan.righttree = None;
    }
    node.hashkeys = hashkeys;
    node.skewTable = skew_table;
    node.skewColumn = skew_column;
    node.skewInherit = skew_inherit;
    Ok(node)
}

/// `make_hashjoin(tlist, joinclauses, otherclauses, hashclauses, hashoperators,
/// hashcollations, hashkeys, lefttree, righttree, jointype, inner_unique)`
/// (createplan.c:6108).
#[allow(clippy::too_many_arguments)]
fn make_hashjoin<'mcx>(
    mcx: Mcx<'mcx>,
    tlist: Option<PgVec<'mcx, TargetEntry<'mcx>>>,
    joinclauses: Option<PgVec<'mcx, Expr>>,
    otherclauses: Option<PgVec<'mcx, Expr>>,
    hashclauses: Option<PgVec<'mcx, Node<'mcx>>>,
    hashoperators: PgVec<'mcx, Oid>,
    hashcollations: PgVec<'mcx, Oid>,
    hashkeys: Option<PgVec<'mcx, Node<'mcx>>>,
    lefttree: Node<'mcx>,
    righttree: Node<'mcx>,
    jointype: NodeJoinType,
    inner_unique: bool,
) -> PgResult<HashJoin<'mcx>> {
    let plan = {
        let mut plan = Plan::default();
        plan.targetlist = tlist;
        plan.qual = otherclauses;
        plan.lefttree = Some(mcx::alloc_in(mcx, lefttree)?);
        plan.righttree = Some(mcx::alloc_in(mcx, righttree)?);
        plan
    };
    let join = JoinBase {
        plan,
        jointype,
        inner_unique,
        joinqual: joinclauses,
    };
    Ok(HashJoin {
        join,
        hashclauses,
        hashoperators,
        hashcollations,
        hashkeys,
    })
}

/// `create_nestloop_plan(root, (NestPath *) best_path)` (createplan.c:4341).
fn create_nestloop_plan<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    best_path: PathId,
) -> PgResult<Node<'mcx>> {
    let (mut innerjoinpath, outerjoinpath, jointype, inner_unique, joinrestrictinfo, has_param_info) =
        match root.path(best_path) {
            PathNode::NestPath(p) => (
                p.jpath.innerjoinpath,
                p.jpath.outerjoinpath,
                p.jpath.jointype,
                p.jpath.inner_unique,
                p.jpath.joinrestrictinfo.clone(),
                p.jpath.path.param_info.is_some(),
            ),
            _ => unreachable!("create_nestloop_plan on non-NestPath"),
        };
    let outerjoinpath = outerjoinpath.expect("create_nestloop_plan: NestPath has no outerjoinpath");
    let innerjoinpath0 = innerjoinpath.expect("create_nestloop_plan: NestPath has no innerjoinpath");

    let tlist = build_path_tlist(mcx, root, best_path)?;

    let save_outer_rels = root.curOuterRels.clone();

    // If the inner path is parameterized by the topmost parent of the outer rel
    // rather than the outer rel itself, fix that.
    let outer_parent = root.path(outerjoinpath).base().parent;
    innerjoinpath =
        pathnode::reparameterize_path_by_child::call(root, innerjoinpath0, outer_parent)?;
    let innerjoinpath = innerjoinpath
        .expect("create_nestloop_plan: reparameterize_path_by_child returned NULL");

    // NestLoop can project, so no need to be picky about child tlists.
    let outer_plan = create_plan_recurse(mcx, root, run, outerjoinpath, 0)?;

    // For a nestloop, include outer relids in curOuterRels for inner side.
    let outerrelids = root.rel(outer_parent).relids.clone();
    root.curOuterRels = relnode::relids_union::call(&root.curOuterRels, &outerrelids);

    let inner_plan = create_plan_recurse(mcx, root, run, innerjoinpath, 0)?;

    // Restore curOuterRels.
    root.curOuterRels = save_outer_rels;

    // Sort join qual clauses into best execution order.
    let joinrestrictclauses = order_qual_clauses_rinfo(root, &joinrestrictinfo);

    // Get the join qual clauses (in plain expression form). Pseudoconstant
    // clauses are ignored here.
    let parent_relids = root.path(best_path).base().parent;
    let (mut joinclauses, mut otherclauses): (Vec<NodeId>, Vec<NodeId>) =
        if is_outer_join(jointype) {
            let relids = root.rel(parent_relids).relids.clone();
            extract_actual_join_clauses(root, &joinrestrictclauses, &relids)
        } else {
            (extract_actual_clauses(root, &joinrestrictclauses, false), Vec::new())
        };

    // Replace any outer-relation variables with nestloop params.
    if has_param_info {
        joinclauses = replace_nestloop_params_list(mcx, root, &joinclauses)?;
        otherclauses = replace_nestloop_params_list(mcx, root, &otherclauses)?;
    }

    // Identify nestloop params supplied by this join node.
    let path_req_outer = path_req_outer(root, best_path);
    let nest_param_ids =
        paramassign::identify_current_nestloop_params::call(mcx, root, run, &outerrelids, &path_req_outer)?;

    // PHV-tlist fixup loop (createplan.c:4435). For simple Vars there is nothing
    // to do; PHV params may need to be added to the outer plan's tlist. Build the
    // executor-side NestLoopParam list as we go.
    let mut outer_plan = outer_plan;
    let mut nest_params: Vec<NestLoopParam> = Vec::with_capacity(nest_param_ids.len());
    let mut outer_tlist_extra: Vec<TargetEntry<'mcx>> = Vec::new();
    let mut outer_tlist_changed = false;
    let mut outer_parallel_safe = outer_plan.plan_head().parallel_safe;
    for &nlp_id in &nest_param_ids {
        let paramno = root.nestloop_param(nlp_id).paramno;
        let paramval = root.nestloop_param(nlp_id).paramval.clone();
        match paramval {
            Expr::Var(var) => {
                // Nothing to do for simple Vars — already available from outer.
                nest_params.push(NestLoopParam { paramno, paramval: var });
            }
            Expr::PlaceHolderVar(_) => {
                // A PHV nestloop param. The executor-side NestLoopParam carrier
                // keeps a strict `Var` paramval, so this faithful path cannot be
                // expressed without widening that carrier (the planner-working
                // NestLoopParamNode widens paramval to Expr, but the plan node
                // does not). This only arises for lateral-PHV nestloops; the
                // common equijoin case never reaches here.
                let _ = (&mut outer_tlist_extra, &mut outer_tlist_changed, &mut outer_parallel_safe);
                return Err(PgError::error(
                    "create_nestloop_plan: PlaceHolderVar nestloop parameter is not supported \
                     — types_nodes NestLoopParam.paramval is a strict Var; widening the \
                     executor-side carrier is out of this lane (lateral-PHV nestloop)",
                ));
            }
            _ => {
                return Err(PgError::error(
                    "create_nestloop_plan: nestloop param is neither Var nor PlaceHolderVar",
                ));
            }
        }
    }
    // (No outer-tlist change for the supported Var-only subset.)
    let _ = outer_tlist_extra;
    if outer_tlist_changed {
        // Unreachable for the Var-only subset; left as the faithful shape.
        let new_tlist = clone_plan_tlist(mcx, &outer_plan)?.expect("outer tlist");
        outer_plan = change_plan_targetlist(mcx, outer_plan, new_tlist, outer_parallel_safe)?;
    }

    let tlist = tlist_to_plan_field(mcx, tlist)?;
    let joinclauses_e = node_ids_to_expr_list(mcx, root, &joinclauses)?;
    let otherclauses_e = node_ids_to_expr_list(mcx, root, &otherclauses)?;

    let mut join_plan = make_nestloop(
        mcx,
        tlist,
        joinclauses_e,
        otherclauses_e,
        nest_params,
        outer_plan,
        inner_plan,
        jointype_path_to_node(jointype),
        inner_unique,
    )?;

    copy_generic_path_info(&mut join_plan.join.plan, root.path(best_path).base());

    Ok(Node::mk_nest_loop(mcx, join_plan))
}

/// `create_hashjoin_plan(root, (HashPath *) best_path)` (createplan.c:4847).
fn create_hashjoin_plan<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    best_path: PathId,
) -> PgResult<Node<'mcx>> {
    let (
        outerjoinpath,
        innerjoinpath,
        jointype,
        inner_unique,
        joinrestrictinfo,
        path_hashclauses,
        num_batches,
        inner_rows_total,
        has_param_info,
        parallel_aware,
    ) = match root.path(best_path) {
        PathNode::HashPath(p) => (
            p.jpath.outerjoinpath,
            p.jpath.innerjoinpath,
            p.jpath.jointype,
            p.jpath.inner_unique,
            p.jpath.joinrestrictinfo.clone(),
            p.path_hashclauses.clone(),
            p.num_batches,
            p.inner_rows_total,
            p.jpath.path.param_info.is_some(),
            p.jpath.path.parallel_aware,
        ),
        _ => unreachable!("create_hashjoin_plan on non-HashPath"),
    };
    let outerjoinpath = outerjoinpath.expect("create_hashjoin_plan: HashPath has no outerjoinpath");
    let innerjoinpath = innerjoinpath.expect("create_hashjoin_plan: HashPath has no innerjoinpath");

    let tlist = build_path_tlist(mcx, root, best_path)?;

    // HashJoin can project; request small tlists where it helps.
    let outer_flags = if num_batches > 1 { CP_SMALL_TLIST } else { 0 };
    let outer_plan = create_plan_recurse(mcx, root, run, outerjoinpath, outer_flags)?;
    let inner_plan = create_plan_recurse(mcx, root, run, innerjoinpath, CP_SMALL_TLIST)?;

    // Sort join qual clauses into best execution order (not the hash clauses).
    let joinrestrictclauses = order_qual_clauses_rinfo(root, &joinrestrictinfo);

    let parent_relids = root.path(best_path).base().parent;
    let (mut joinclauses, mut otherclauses): (Vec<NodeId>, Vec<NodeId>) =
        if is_outer_join(jointype) {
            let relids = root.rel(parent_relids).relids.clone();
            extract_actual_join_clauses(root, &joinrestrictclauses, &relids)
        } else {
            (extract_actual_clauses(root, &joinrestrictclauses, false), Vec::new())
        };

    // Remove the hashclauses from the join qual clauses (qpqual remainder).
    let hashclauses_actual = get_actual_clauses(root, &path_hashclauses);
    let hashclauses_actual_exprs: Vec<Expr> =
        hashclauses_actual.iter().map(|&id| root.node(id).clone()).collect();
    joinclauses = list_difference_exprs(root, &joinclauses, &hashclauses_actual_exprs);

    if has_param_info {
        joinclauses = replace_nestloop_params_list(mcx, root, &joinclauses)?;
        otherclauses = replace_nestloop_params_list(mcx, root, &otherclauses)?;
    }

    // Rearrange hashclauses so the outer variable is always on the left.
    let outer_relids = root.rel(root.path(outerjoinpath).base().parent).relids.clone();
    let hashclauses = get_switched_clauses(root, &path_hashclauses, &outer_relids)?;

    // Skew optimization: a single join clause whose outer side is a simple Var.
    let mut skew_table = InvalidOid;
    let mut skew_column: AttrNumber = 0;
    let mut skew_inherit = false;
    if hashclauses.len() == 1 {
        if let Some(clause) = hashclauses[0].as_opexpr() {
            // node = (Node *) linitial(clause->args);
            if let Some(arg0) = clause.args.first() {
                let node = match arg0 {
                    Expr::RelabelType(rt) => rt.arg.as_deref().unwrap_or(arg0),
                    other => other,
                };
                if let Some(var) = node.as_var() {
                    let rte = planner_rt_fetch(run, root, var.varno as u32);
                    if rte.rtekind == types_nodes::parsenodes::RTEKind::RTE_RELATION {
                        skew_table = rte.relid;
                        skew_column = var.varattno;
                        skew_inherit = rte.inh;
                    }
                }
            }
        }
    }

    // Deconstruct hashclauses into outer/inner hashkeys + operator info.
    let mut hashoperators: PgVec<'mcx, Oid> = vec_with_capacity_in(mcx, hashclauses.len())?;
    let mut hashcollations: PgVec<'mcx, Oid> = vec_with_capacity_in(mcx, hashclauses.len())?;
    let mut outer_hashkeys: PgVec<'mcx, Node<'mcx>> = vec_with_capacity_in(mcx, hashclauses.len())?;
    let mut inner_hashkeys: PgVec<'mcx, Node<'mcx>> = vec_with_capacity_in(mcx, hashclauses.len())?;
    for hc in &hashclauses {
        let op = match hc {
            Expr::OpExpr(op) => op,
            _ => return Err(PgError::error("create_hashjoin_plan: hashclause is not an OpExpr")),
        };
        hashoperators.push(op.opno);
        hashcollations.push(op.inputcollid);
        let outer_arg = op
            .args
            .first()
            .ok_or_else(|| PgError::error("create_hashjoin_plan: hashclause OpExpr has no args"))?;
        let inner_arg = op
            .args
            .get(1)
            .ok_or_else(|| PgError::error("create_hashjoin_plan: hashclause OpExpr has one arg"))?;
        outer_hashkeys.push(expr_to_node(outer_arg.clone()));
        inner_hashkeys.push(expr_to_node(inner_arg.clone()));
    }

    // Build the Hash node over the inner plan.
    let mut hash_plan = make_hash(
        mcx,
        inner_plan,
        Some(inner_hashkeys),
        skew_table,
        skew_column,
        skew_inherit,
    )?;
    // Set Hash node's startup & total cost equal to inner plan's total cost.
    {
        let inner_cost = {
            let inner_ref = hash_plan
                .plan
                .lefttree
                .as_deref()
                .expect("hash plan has lefttree");
            plan_cost_snapshot(inner_ref.plan_head())
        };
        apply_cost_snapshot(&mut hash_plan.plan, &inner_cost);
        hash_plan.plan.startup_cost = hash_plan.plan.total_cost;
    }
    if parallel_aware {
        hash_plan.plan.parallel_aware = true;
        hash_plan.rows_total = inner_rows_total;
    }
    let tlist = tlist_to_plan_field(mcx, tlist)?;
    let joinclauses_e = node_ids_to_expr_list(mcx, root, &joinclauses)?;
    let otherclauses_e = node_ids_to_expr_list(mcx, root, &otherclauses)?;
    let hashclauses_e = exprs_to_node_list(mcx, hashclauses)?;
    let outer_hashkeys_opt = if outer_hashkeys.is_empty() { None } else { Some(outer_hashkeys) };

    let mut join_plan = make_hashjoin(
        mcx,
        tlist,
        joinclauses_e,
        otherclauses_e,
        hashclauses_e,
        hashoperators,
        hashcollations,
        outer_hashkeys_opt,
        // lefttree = outer_plan, righttree = the Hash node (over inner_plan).
        outer_plan,
        Node::mk_hash(mcx, hash_plan),
        jointype_path_to_node(jointype),
        inner_unique,
    )?;

    copy_generic_path_info(&mut join_plan.join.plan, root.path(best_path).base());

    Ok(Node::mk_hash_join(mcx, join_plan))
}

/// `label_sort_with_costsize(root, plan, limit_tuples)` (createplan.c:5553) —
/// re-figure a freshly-built `Sort` plan node's cost via `cost_sort` (over a
/// dummy stack `Path`) so EXPLAIN labels it nicely. The cost was already
/// included in the path cost we work from, but isn't split out, so we re-derive
/// it. Costs come from the `cost_sort_label` costsize seam.
fn label_sort_with_costsize(root: &mut PlannerInfo, plan: &mut Sort, limit_tuples: f64) {
    let lefttree = plan
        .plan
        .lefttree
        .as_deref()
        .expect("label_sort_with_costsize: Sort has no lefttree");
    let lt = lefttree.plan_head();
    let (lt_total_cost, lt_rows, lt_width, lt_parallel_safe) =
        (lt.total_cost, lt.plan_rows, lt.plan_width, lt.parallel_safe);

    let (startup_cost, total_cost) = costsize::cost_sort_label::call(
        root,
        plan.plan.disabled_nodes,
        lt_total_cost,
        lt_rows,
        lt_width,
        limit_tuples,
    );
    plan.plan.startup_cost = startup_cost;
    plan.plan.total_cost = total_cost;
    plan.plan.plan_rows = lt_rows;
    plan.plan.plan_width = lt_width;
    plan.plan.parallel_aware = false;
    plan.plan.parallel_safe = lt_parallel_safe;
}

/// `label_incrementalsort_with_costsize(root, plan, pathkeys, limit_tuples)`
/// (createplan.c:5581) — same as [`label_sort_with_costsize`] but labels an
/// `IncrementalSort` node.
fn label_incrementalsort_with_costsize<'mcx>(
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    plan: &mut IncrementalSortNode,
    pathkeys: &[PathKey],
    limit_tuples: f64,
) -> PgResult<()> {
    let lefttree = plan
        .sort
        .plan
        .lefttree
        .as_deref()
        .expect("label_incrementalsort_with_costsize: IncrementalSort has no lefttree");
    let lt = lefttree.plan_head();
    let (lt_startup_cost, lt_total_cost, lt_rows, lt_width, lt_parallel_safe) = (
        lt.startup_cost,
        lt.total_cost,
        lt.plan_rows,
        lt.plan_width,
        lt.parallel_safe,
    );

    let (startup_cost, total_cost) = costsize::cost_incremental_sort_label::call(
        run,
        root,
        pathkeys,
        plan.nPresortedCols,
        plan.sort.plan.disabled_nodes,
        lt_startup_cost,
        lt_total_cost,
        lt_rows,
        lt_width,
        limit_tuples,
    )?;
    plan.sort.plan.startup_cost = startup_cost;
    plan.sort.plan.total_cost = total_cost;
    plan.sort.plan.plan_rows = lt_rows;
    plan.sort.plan.plan_width = lt_width;
    plan.sort.plan.parallel_aware = false;
    plan.sort.plan.parallel_safe = lt_parallel_safe;
    Ok(())
}

/// `make_mergejoin(tlist, joinclauses, otherclauses, mergeclauses,
/// mergefamilies, mergecollations, mergereversals, mergenullsfirst, lefttree,
/// righttree, jointype, inner_unique, skip_mark_restore)` (createplan.c:6162).
#[allow(clippy::too_many_arguments)]
fn make_mergejoin<'mcx>(
    mcx: Mcx<'mcx>,
    tlist: Option<PgVec<'mcx, TargetEntry<'mcx>>>,
    joinclauses: Option<PgVec<'mcx, Expr>>,
    otherclauses: Option<PgVec<'mcx, Expr>>,
    mergeclauses: Vec<Expr>,
    mergefamilies: Vec<Oid>,
    mergecollations: Vec<Oid>,
    mergereversals: Vec<bool>,
    mergenullsfirst: Vec<bool>,
    lefttree: Node<'mcx>,
    righttree: Node<'mcx>,
    jointype: NodeJoinType,
    inner_unique: bool,
    skip_mark_restore: bool,
) -> PgResult<MergeJoin<'mcx>> {
    let plan = {
        let mut plan = Plan::default();
        plan.targetlist = tlist;
        plan.qual = otherclauses;
        plan.lefttree = Some(mcx::alloc_in(mcx, lefttree)?);
        plan.righttree = Some(mcx::alloc_in(mcx, righttree)?);
        plan
    };
    let join = JoinBase {
        plan,
        jointype,
        inner_unique,
        // node->join.joinqual = joinclauses;
        joinqual: joinclauses,
    };
    Ok(MergeJoin {
        join,
        skip_mark_restore,
        mergeclauses,
        mergeFamilies: mergefamilies,
        mergeCollations: mergecollations,
        mergeReversals: mergereversals,
        mergeNullsFirst: mergenullsfirst,
    })
}

/// `create_mergejoin_plan(root, (MergePath *) best_path)` (createplan.c:4493).
fn create_mergejoin_plan<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    best_path: PathId,
) -> PgResult<Node<'mcx>> {
    let (
        outerjoinpath,
        innerjoinpath,
        jointype,
        inner_unique,
        joinrestrictinfo,
        path_mergeclauses,
        outersortkeys,
        innersortkeys,
        outer_presorted_keys,
        skip_mark_restore,
        materialize_inner,
        has_param_info,
    ) = match root.path(best_path) {
        PathNode::MergePath(p) => (
            p.jpath.outerjoinpath,
            p.jpath.innerjoinpath,
            p.jpath.jointype,
            p.jpath.inner_unique,
            p.jpath.joinrestrictinfo.clone(),
            p.path_mergeclauses.clone(),
            p.outersortkeys.clone(),
            p.innersortkeys.clone(),
            p.outer_presorted_keys,
            p.skip_mark_restore,
            p.materialize_inner,
            p.jpath.path.param_info.is_some(),
        ),
        _ => unreachable!("create_mergejoin_plan on non-MergePath"),
    };
    let outerjoinpath =
        outerjoinpath.expect("create_mergejoin_plan: MergePath has no outerjoinpath");
    let innerjoinpath =
        innerjoinpath.expect("create_mergejoin_plan: MergePath has no innerjoinpath");

    let tlist = build_path_tlist(mcx, root, best_path)?;

    // MergeJoin can project; request a small tlist on a side we intend to sort.
    let outer_flags = if !outersortkeys.is_empty() { CP_SMALL_TLIST } else { 0 };
    let inner_flags = if !innersortkeys.is_empty() { CP_SMALL_TLIST } else { 0 };
    let mut outer_plan = create_plan_recurse(mcx, root, run, outerjoinpath, outer_flags)?;
    let mut inner_plan = create_plan_recurse(mcx, root, run, innerjoinpath, inner_flags)?;

    // Sort join qual clauses into best execution order (NB: do NOT reorder the
    // mergeclauses).
    let joinrestrictclauses = order_qual_clauses_rinfo(root, &joinrestrictinfo);

    // Get the join qual clauses (in plain expression form). Pseudoconstant
    // clauses are ignored here.
    let parent_relids = root.path(best_path).base().parent;
    let (mut joinclauses, mut otherclauses): (Vec<NodeId>, Vec<NodeId>) =
        if is_outer_join(jointype) {
            let relids = root.rel(parent_relids).relids.clone();
            extract_actual_join_clauses(root, &joinrestrictclauses, &relids)
        } else {
            // We can treat all clauses alike for an inner join.
            (extract_actual_clauses(root, &joinrestrictclauses, false), Vec::new())
        };

    // Remove the mergeclauses from the join qual clauses, leaving the quals to
    // be checked as qpquals.
    let mergeclauses_actual = get_actual_clauses(root, &path_mergeclauses);
    let mergeclauses_actual_exprs: Vec<Expr> =
        mergeclauses_actual.iter().map(|&id| root.node(id).clone()).collect();
    joinclauses = list_difference_exprs(root, &joinclauses, &mergeclauses_actual_exprs);

    // Replace any outer-relation variables with nestloop params. There should
    // not be any in the mergeclauses.
    if has_param_info {
        joinclauses = replace_nestloop_params_list(mcx, root, &joinclauses)?;
        otherclauses = replace_nestloop_params_list(mcx, root, &otherclauses)?;
    }

    // Rearrange mergeclauses so the outer variable is always on the left; mark
    // the mergeclause restrictinfos with correct outer_is_left status.
    let outer_relids = root.rel(root.path(outerjoinpath).base().parent).relids.clone();
    let mergeclauses = get_switched_clauses(root, &path_mergeclauses, &outer_relids)?;

    // Create explicit sort nodes for the outer and inner paths if necessary.
    let outerpathkeys: Vec<PathKey> = if !outersortkeys.is_empty() {
        let outer_path_parent = root.path(outerjoinpath).base().parent;
        let outer_sort_relids: Relids = root.rel(outer_path_parent).relids.clone();

        // We choose incremental sort if it is enabled and there are presorted
        // keys; otherwise a full sort.
        let enable_incremental_sort = (vars::enable_incremental_sort.get().get)();
        let sort_node: Node<'mcx> = if enable_incremental_sort && outer_presorted_keys > 0 {
            let mut sort_plan = make_incrementalsort_from_pathkeys(
                mcx,
                root,
                outer_plan,
                &outersortkeys,
                &outer_sort_relids,
                outer_presorted_keys,
            )?;
            label_incrementalsort_with_costsize(root, run, &mut sort_plan, &outersortkeys, -1.0)?;
            Node::mk_incremental_sort(mcx, sort_plan)
        } else {
            let mut sort_plan =
                make_sort_from_pathkeys(mcx, root, outer_plan, &outersortkeys, &outer_sort_relids)?;
            label_sort_with_costsize(root, &mut sort_plan, -1.0);
            Node::mk_sort(mcx, sort_plan)
        };
        outer_plan = sort_node;
        outersortkeys.clone()
    } else {
        root.path(outerjoinpath).base().pathkeys.clone()
    };

    let innerpathkeys: Vec<PathKey> = if !innersortkeys.is_empty() {
        // We do not consider incremental sort for the inner path, because
        // incremental sort does not support mark/restore.
        let inner_path_parent = root.path(innerjoinpath).base().parent;
        let inner_sort_relids: Relids = root.rel(inner_path_parent).relids.clone();
        let mut sort_plan =
            make_sort_from_pathkeys(mcx, root, inner_plan, &innersortkeys, &inner_sort_relids)?;
        label_sort_with_costsize(root, &mut sort_plan, -1.0);
        inner_plan = Node::mk_sort(mcx, sort_plan);
        innersortkeys.clone()
    } else {
        root.path(innerjoinpath).base().pathkeys.clone()
    };

    // If specified, add a materialize node to shield the inner plan from the
    // need to handle mark/restore.
    if materialize_inner {
        let mut matplan = make_material(mcx, inner_plan)?;
        // We assume the materialize will not spill to disk, and therefore charge
        // just cpu_operator_cost per tuple. (Keep in sync with
        // final_cost_mergejoin.)
        let inner_cost = {
            let inner_ref = matplan
                .plan
                .lefttree
                .as_deref()
                .expect("materialize has lefttree");
            plan_cost_snapshot(inner_ref.plan_head())
        };
        apply_cost_snapshot(&mut matplan.plan, &inner_cost);
        matplan.plan.total_cost += pathnode::cpu_operator_cost::call() * matplan.plan.plan_rows;
        inner_plan = Node::mk_material(mcx, matplan);
    }

    // Compute the opfamily/collation/strategy/nullsfirst arrays needed by the
    // executor. The information is in the pathkeys for the two inputs, but we
    // must be careful about mergeclauses sharing a pathkey, and about the inner
    // pathkeys possibly not being in mergeclause order.
    let n_clauses = mergeclauses.len();
    debug_assert_eq!(n_clauses, path_mergeclauses.len());
    let mut mergefamilies: Vec<Oid> = Vec::with_capacity(n_clauses);
    let mut mergecollations: Vec<Oid> = Vec::with_capacity(n_clauses);
    let mut mergereversals: Vec<bool> = Vec::with_capacity(n_clauses);
    let mut mergenullsfirst: Vec<bool> = Vec::with_capacity(n_clauses);

    // opathkey / opeclass track the current outer pathkey; lop/lip are cursors
    // into outer/inner pathkeys (index into the cloned pathkey vecs).
    let mut opathkey: Option<PathKey> = None;
    let mut opeclass: Option<types_pathnodes::EcId> = None;
    let mut lop: usize = 0;
    let mut lip: usize = 0;

    for &rinfo_id in &path_mergeclauses {
        // fetch outer/inner eclass from mergeclause
        let (oeclass, ieclass) = {
            let rinfo = root.rinfo(rinfo_id);
            if rinfo.outer_is_left {
                (rinfo.left_ec, rinfo.right_ec)
            } else {
                (rinfo.right_ec, rinfo.left_ec)
            }
        };
        let oeclass = oeclass.expect("create_mergejoin_plan: mergeclause has no outer eclass");
        let ieclass = ieclass.expect("create_mergejoin_plan: mergeclause has no inner eclass");

        // Identify the outer pathkey for this clause by matching eclasses.
        if Some(oeclass) != opeclass {
            // doesn't match the current opathkey, so must match the next
            if lop >= outerpathkeys.len() {
                return Err(PgError::error("outer pathkeys do not match mergeclauses"));
            }
            let opk = outerpathkeys[lop].clone();
            opeclass = opk.pk_eclass;
            opathkey = Some(opk);
            lop += 1;
            if Some(oeclass) != opeclass {
                return Err(PgError::error("outer pathkeys do not match mergeclauses"));
            }
        }

        // Identify the inner pathkey, coping with redundant inner pathkeys.
        let mut ipathkey: Option<PathKey> = None;
        let mut ipeclass: Option<types_pathnodes::EcId> = None;
        let mut first_inner_match = false;
        if lip < innerpathkeys.len() {
            let ipk = innerpathkeys[lip].clone();
            ipeclass = ipk.pk_eclass;
            if Some(ieclass) == ipeclass {
                // successful first match to this inner pathkey
                ipathkey = Some(ipk);
                lip += 1;
                first_inner_match = true;
            } else {
                ipathkey = Some(ipk);
            }
        }
        if !first_inner_match {
            // redundant clause ... must match something before lip
            let mut matched = false;
            for l2 in 0..lip {
                let ipk = innerpathkeys[l2].clone();
                ipeclass = ipk.pk_eclass;
                ipathkey = Some(ipk);
                if Some(ieclass) == ipeclass {
                    matched = true;
                    break;
                }
            }
            if !matched && Some(ieclass) != ipeclass {
                return Err(PgError::error("inner pathkeys do not match mergeclauses"));
            }
        }

        let opathkey_ref =
            opathkey.as_ref().expect("create_mergejoin_plan: outer pathkey not set");
        let ipathkey_ref =
            ipathkey.as_ref().expect("create_mergejoin_plan: inner pathkey not set");

        // The pathkeys should match as to opfamily and collation (which affect
        // equality); a redundant inner pathkey may differ in sort ordering.
        let o_collation = root.ec(opathkey_ref.pk_eclass.expect("outer pk_eclass")).ec_collation;
        let i_collation = root.ec(ipathkey_ref.pk_eclass.expect("inner pk_eclass")).ec_collation;
        if opathkey_ref.pk_opfamily != ipathkey_ref.pk_opfamily || o_collation != i_collation {
            return Err(PgError::error("left and right pathkeys do not match in mergejoin"));
        }
        if first_inner_match
            && (opathkey_ref.pk_cmptype != ipathkey_ref.pk_cmptype
                || opathkey_ref.pk_nulls_first != ipathkey_ref.pk_nulls_first)
        {
            return Err(PgError::error("left and right pathkeys do not match in mergejoin"));
        }

        // OK, save info for executor.
        mergefamilies.push(opathkey_ref.pk_opfamily);
        mergecollations.push(o_collation);
        mergereversals.push(opathkey_ref.pk_cmptype == COMPARE_GT);
        mergenullsfirst.push(opathkey_ref.pk_nulls_first);
    }

    // Note: it is not an error if we have additional pathkey elements (the input
    // paths might be better-sorted than we need).

    // Now build the mergejoin node.
    let tlist = tlist_to_plan_field(mcx, tlist)?;
    let joinclauses_e = node_ids_to_expr_list(mcx, root, &joinclauses)?;
    let otherclauses_e = node_ids_to_expr_list(mcx, root, &otherclauses)?;
    // mergeclauses field is the switched clause exprs (owned Vec<Expr>).
    let mergeclauses_field: Vec<Expr> = mergeclauses;

    let mut join_plan = make_mergejoin(
        mcx,
        tlist,
        joinclauses_e,
        otherclauses_e,
        mergeclauses_field,
        mergefamilies,
        mergecollations,
        mergereversals,
        mergenullsfirst,
        outer_plan,
        inner_plan,
        jointype_path_to_node(jointype),
        inner_unique,
        skip_mark_restore,
    )?;

    // Costs of sort and material steps are included in path cost already.
    copy_generic_path_info(&mut join_plan.join.plan, root.path(best_path).base());

    Ok(Node::mk_merge_join(mcx, join_plan))
}

// ===========================================================================
// Append / MergeAppend family (createplan.c:1216 / 1437).
// ===========================================================================

/// Convert a planner [`Relids`] (an `Option<Box<pathnodes::Bitmapset>>`) into an
/// owned plan-node `apprelids` (`Option<PgBox<nodes::Bitmapset>>`). Both are word
/// vectors; this re-homes the word storage into `mcx` like C's `bms_copy`.
fn relids_to_apprelids<'mcx>(
    mcx: Mcx<'mcx>,
    relids: &Relids,
) -> PgResult<Option<PgBox<'mcx, Bitmapset<'mcx>>>> {
    match relids {
        None => Ok(None),
        Some(bms) => {
            let mut words = vec_with_capacity_in(mcx, bms.words.len())?;
            for &w in &bms.words {
                words.push(w);
            }
            Ok(Some(mcx::alloc_in(mcx, Bitmapset { words })?))
        }
    }
}

/// `mark_async_capable_plan(plan, path)` (createplan.c:1140) — whether a child
/// `plan` built from `path` is async-capable, marking it if so. Only foreign
/// scans over an async-capable FDW (plus trivial SubqueryScan / Projection atop
/// such) are ever async-capable.
fn trivial_subqueryscan(scan: &SubqueryScan<'_>) -> bool {
    // `trivial_subqueryscan(plan)` (setrefs.c): whether a `SubqueryScan` is a
    // no-op. The expensive recompute lives in the (unported) setrefs.c floor;
    // here we honour the cached `scanstatus` flag the planner stamps. An
    // un-cached node is treated as non-trivial (the conservative answer).
    scan.scanstatus == SubqueryScanStatus::Trivial
}

/// `mark_async_capable_plan(plan, path)` (createplan.c:1140) — whether a child
/// `plan` built from `path` is async-capable, marking it (`plan->async_capable
/// = true`) if so. Only a foreign scan over an async-capable FDW — plus a
/// trivial `SubqueryScan` or a pulled-up `Projection` atop such — is
/// async-capable.
///
/// The owned model resolves `path` through `root` (the C up-cast
/// `(SubqueryScanPath *) path` / `(ProjectionPath *) path` plus the subpath
/// deref). `IsForeignPathAsyncCapable` is an FDW callback not modeled at this
/// layer, so the `T_ForeignPath` arm always returns `false`; the recursive
/// `T_SubqueryScanPath` / `T_ProjectionPath` arms are ported faithfully.
fn mark_async_capable_plan(root: &PlannerInfo, plan: &mut Node<'_>, path: PathId) -> bool {
    match root.path(path) {
        PathNode::SubqueryScanPath(p) => {
            let subpath = p.subpath;
            // If the generated plan node includes a gating Result node, we
            // can't execute it asynchronously.
            if plan.is_result() {
                return false;
            }
            // If a SubqueryScan node atop of an async-capable plan node is
            // deletable, consider it as async-capable.
            let Some(scan_plan) = plan.as_subqueryscan_mut() else {
                return false;
            };
            if !trivial_subqueryscan(scan_plan) {
                return false;
            }
            let Some(subpath) = subpath else {
                return false;
            };
            let Some(inner) = scan_plan.subplan.as_deref_mut() else {
                return false;
            };
            if !mark_async_capable_plan(root, inner, subpath) {
                return false;
            }
        }
        PathNode::ForeignPath(_) => {
            // If the generated plan node includes a gating Result node, we
            // can't execute it asynchronously.
            if plan.is_result() {
                return false;
            }
            // fdwroutine->IsForeignPathAsyncCapable is an FDW callback not
            // modeled at this layer; no FDW advertises async capability here, so
            // this is always the C `false` branch.
            return false;
        }
        PathNode::ProjectionPath(p) => {
            let subpath = p.subpath;
            // If the generated plan node includes a Result node for the
            // projection, we can't execute it asynchronously.
            if plan.is_result() {
                return false;
            }
            // create_projection_plan() would have pulled up the subplan, so
            // check the capability using the subpath.
            return match subpath {
                Some(subpath) => mark_async_capable_plan(root, plan, subpath),
                None => false,
            };
        }
        _ => return false,
    }

    plan.plan_head_mut().async_capable = true;
    true
}

/// `create_append_plan(root, (AppendPath *) best_path, flags)` (createplan.c:1216).
fn create_append_plan<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    best_path: PathId,
    flags: i32,
) -> PgResult<Node<'mcx>> {
    let (subpaths, first_partial_path, limit_tuples) = match root.path(best_path) {
        PathNode::AppendPath(p) => (p.subpaths.clone(), p.first_partial_path, p.limit_tuples),
        _ => unreachable!("create_append_plan on non-AppendPath"),
    };
    let rel_id = root.path(best_path).base().parent;
    let pathkeys = root.path(best_path).base().pathkeys.clone();
    let parallel_safe = root.path(best_path).base().parallel_safe;
    let has_param_info = root.path(best_path).base().param_info.is_some();

    let tlist = build_path_tlist(mcx, root, best_path)?;
    let orig_tlist_length = tlist.len();

    // The subpaths list could be empty if every child was proven empty by
    // constraint exclusion; generate a dummy Result that returns no rows.
    if subpaths.is_empty() {
        // make_result(tlist, (Node *) list_make1(makeBoolConst(false, false)), NULL)
        let mut resconstantqual: PgVec<'mcx, Expr> = vec_with_capacity_in(mcx, 1)?;
        resconstantqual.push(Expr::Const(make_bool_const(false, false)));
        let tlist = tlist_to_plan_field(mcx, tlist)?;
        let mut plan = make_result(mcx, tlist, Some(resconstantqual), None)?;
        copy_generic_path_info(&mut plan.plan, root.path(best_path).base());
        return Ok(Node::mk_result(mcx, plan));
    }

    // Otherwise build an Append plan. We don't split the Append's creation into
    // a make_xxx because we want to run prepare_sort_from_pathkeys on it before
    // doing so on the individual children, to ease cross-checking sort info.
    let apprelids = relids_to_apprelids(mcx, &root.rel(rel_id).relids.clone())?;
    let mut append = AppendNode::default();
    append.apprelids = apprelids;
    append.plan.targetlist = tlist_to_plan_field(mcx, tlist)?;
    append.plan.qual = None;
    append.plan.lefttree = None;
    append.plan.righttree = None;
    let mut append_node: Node<'mcx> = Node::mk_append(mcx, append);

    // For ordered Appends, compute the parent sort-key info (adjusting the
    // Append's tlist in place) so children can cross-check against it.
    let mut node_sort_col_idx: Option<PgVec<'mcx, AttrNumber>> = None;
    let mut tlist_was_changed = false;
    if !pathkeys.is_empty() {
        let rel_relids = root.rel(rel_id).relids.clone();
        let (n, sci, _sop, _coll, _nf) =
            prepare_sort_from_pathkeys(mcx, root, append_node, &pathkeys, &rel_relids, None, true)?;
        append_node = n;
        node_sort_col_idx = Some(sci);
        let new_len = append_node.plan_head().targetlist.as_deref().map(|t| t.len()).unwrap_or(0);
        tlist_was_changed = orig_tlist_length != new_len;
    }

    // If appropriate, consider async append.
    let enable_async_append = (vars::enable_async_append.get().get)();
    let consider_async =
        enable_async_append && pathkeys.is_empty() && !parallel_safe && subpaths.len() > 1;

    // Build the plan for each child.
    let mut subplans: Vec<Node<'mcx>> = Vec::with_capacity(subpaths.len());
    let mut nasyncplans: i32 = 0;
    for &subpath in &subpaths {
        // Must insist that all children return the same tlist.
        let mut subplan = create_plan_recurse(mcx, root, run, subpath, CP_EXACT_TLIST)?;

        // For ordered Appends, insert a Sort if the subplan isn't ordered enough.
        if !pathkeys.is_empty() {
            let sub_parent = root.path(subpath).base().parent;
            let sub_relids = root.rel(sub_parent).relids.clone();
            let req = node_sort_col_idx.as_deref();
            let (numsortkeys, sort_col_idx, sort_operators, collations, nulls_first) =
                prepare_sort_from_pathkeys(mcx, root, subplan, &pathkeys, &sub_relids, req, false)?;
            subplan = numsortkeys;

            // Check we got the same sort key columns the Append expects.
            if let Some(node_sci) = node_sort_col_idx.as_deref() {
                if sort_col_idx.as_slice() != node_sci {
                    return Err(PgError::error(
                        "Append child's targetlist doesn't match Append",
                    ));
                }
            }

            // Insert a Sort node if subplan isn't sufficiently ordered.
            let sub_pathkeys = root.path(subpath).base().pathkeys.clone();
            if !pathnode::pathkeys_contained_in::call(&pathkeys, &sub_pathkeys) {
                let mut sort = make_sort(
                    mcx,
                    subplan,
                    sort_col_idx,
                    sort_operators,
                    collations,
                    nulls_first,
                )?;
                label_sort_with_costsize(root, &mut sort, limit_tuples);
                subplan = Node::mk_sort(mcx, sort);
            }
        }

        // If needed, check whether the subplan can run asynchronously.
        if consider_async && mark_async_capable_plan(root, &mut subplan, subpath) {
            nasyncplans += 1;
        }

        subplans.push(subplan);
    }

    // Run-time partition pruning: gather pruning info if there are useful quals.
    let mut part_prune_index: i32 = -1;
    let enable_partition_pruning = (vars::enable_partition_pruning.get().get)();
    if enable_partition_pruning {
        let baserestrictinfo = root.rel(rel_id).baserestrictinfo.clone();
        let mut prunequal = extract_actual_clauses(root, &baserestrictinfo, false);
        if has_param_info {
            let ppi_clauses = match root.path(best_path).base().param_info.as_ref() {
                Some(ppi) => ppi.ppi_clauses.clone(),
                None => Vec::new(),
            };
            let mut prmquals = extract_actual_clauses(root, &ppi_clauses, false);
            prmquals = replace_nestloop_params_list(mcx, root, &prmquals)?;
            prunequal.extend(prmquals);
        }
        if !prunequal.is_empty() {
            part_prune_index =
                partprune::make_partition_pruneinfo::call(root, rel_id, &subpaths, &prunequal)?;
        }
    }

    // Fill the Append's child / async / partial fields and finalize costs.
    {
        let append = append_node
            .as_append_mut()
            .unwrap_or_else(|| unreachable!("append_node is not an Append"));
        append.appendplans = subplans;
        append.nasyncplans = nasyncplans;
        append.first_partial_plan = first_partial_path;
        append.part_prune_index = part_prune_index;
        copy_generic_path_info(&mut append.plan, root.path(best_path).base());
    }

    // If prepare_sort_from_pathkeys added sort columns but the caller wants the
    // exact / a narrow tlist, strip them again by injecting a projection.
    if tlist_was_changed && (flags & (CP_EXACT_TLIST | CP_SMALL_TLIST)) != 0 {
        let parallel_safe = append_node.plan_head().parallel_safe;
        let new_tlist = list_copy_head_tlist(mcx, &append_node, orig_tlist_length)?;
        return inject_projection_plan(mcx, append_node, new_tlist, parallel_safe);
    }
    Ok(append_node)
}

/// `create_merge_append_plan(root, (MergeAppendPath *) best_path, flags)`
/// (createplan.c:1437).
fn create_merge_append_plan<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    best_path: PathId,
    flags: i32,
) -> PgResult<Node<'mcx>> {
    let (subpaths, limit_tuples) = match root.path(best_path) {
        PathNode::MergeAppendPath(p) => (p.subpaths.clone(), p.limit_tuples),
        _ => unreachable!("create_merge_append_plan on non-MergeAppendPath"),
    };
    let rel_id = root.path(best_path).base().parent;
    let pathkeys = root.path(best_path).base().pathkeys.clone();
    debug_assert!(root.path(best_path).base().param_info.is_none());

    let tlist = build_path_tlist(mcx, root, best_path)?;
    let orig_tlist_length = tlist.len();

    // As with Append, we build the node and run prepare_sort_from_pathkeys on it
    // before the children, to ease cross-checking sort info.
    let apprelids = relids_to_apprelids(mcx, &root.rel(rel_id).relids.clone())?;
    let mut node = MergeAppendNode::default();
    node.apprelids = apprelids;
    node.plan.targetlist = tlist_to_plan_field(mcx, tlist)?;
    node.plan.qual = None;
    node.plan.lefttree = None;
    node.plan.righttree = None;
    // copy_generic_path_info(plan, path) happens up front in C; the targetlist
    // assignment above mirrors the field order. Copy cost/size now.
    copy_generic_path_info(&mut node.plan, root.path(best_path).base());
    let mut ma_node: Node<'mcx> = Node::mk_merge_append(mcx, node);

    // Compute parent sort-key info, adjusting the MergeAppend tlist in place.
    let rel_relids = root.rel(rel_id).relids.clone();
    let (n, node_sort_col_idx, node_sort_operators, node_collations, node_nulls_first) =
        prepare_sort_from_pathkeys(mcx, root, ma_node, &pathkeys, &rel_relids, None, true)?;
    ma_node = n;
    let num_cols = node_sort_col_idx.len() as i32;
    let new_len = ma_node.plan_head().targetlist.as_deref().map(|t| t.len()).unwrap_or(0);
    let tlist_was_changed = orig_tlist_length != new_len;

    // Now prepare the child plans, applying prepare_sort_from_pathkeys even to
    // subplans that don't need an explicit sort, so they return the same sort
    // key columns the MergeAppend expects.
    let mut subplans: Vec<Node<'mcx>> = Vec::with_capacity(subpaths.len());
    for &subpath in &subpaths {
        let mut subplan = create_plan_recurse(mcx, root, run, subpath, CP_EXACT_TLIST)?;

        let sub_parent = root.path(subpath).base().parent;
        let sub_relids = root.rel(sub_parent).relids.clone();
        let (np, sort_col_idx, sort_operators, collations, nulls_first) = prepare_sort_from_pathkeys(
            mcx,
            root,
            subplan,
            &pathkeys,
            &sub_relids,
            Some(&node_sort_col_idx),
            false,
        )?;
        subplan = np;

        // Cross-check the sort key columns match the MergeAppend.
        if sort_col_idx.as_slice() != node_sort_col_idx.as_slice() {
            return Err(PgError::error(
                "MergeAppend child's targetlist doesn't match MergeAppend",
            ));
        }

        // Insert a Sort node if subplan isn't sufficiently ordered.
        let sub_pathkeys = root.path(subpath).base().pathkeys.clone();
        if !pathnode::pathkeys_contained_in::call(&pathkeys, &sub_pathkeys) {
            let mut sort = make_sort(
                mcx,
                subplan,
                sort_col_idx,
                sort_operators,
                collations,
                nulls_first,
            )?;
            label_sort_with_costsize(root, &mut sort, limit_tuples);
            subplan = Node::mk_sort(mcx, sort);
        }

        subplans.push(subplan);
    }

    // Run-time partition pruning.
    let mut part_prune_index: i32 = -1;
    let enable_partition_pruning = (vars::enable_partition_pruning.get().get)();
    if enable_partition_pruning {
        let baserestrictinfo = root.rel(rel_id).baserestrictinfo.clone();
        let prunequal = extract_actual_clauses(root, &baserestrictinfo, false);
        // We don't currently generate any parameterized MergeAppend paths.
        debug_assert!(root.path(best_path).base().param_info.is_none());
        if !prunequal.is_empty() {
            part_prune_index =
                partprune::make_partition_pruneinfo::call(root, rel_id, &subpaths, &prunequal)?;
        }
    }

    // Finalize the MergeAppend node fields.
    {
        let node = ma_node
            .as_mergeappend_mut()
            .unwrap_or_else(|| unreachable!("ma_node is not a MergeAppend"));
        node.numCols = num_cols;
        node.sortColIdx = node_sort_col_idx.iter().copied().collect();
        node.sortOperators = node_sort_operators.iter().copied().collect();
        node.collations = node_collations.iter().copied().collect();
        node.nullsFirst = node_nulls_first.iter().copied().collect();
        node.mergeplans = subplans;
        node.part_prune_index = part_prune_index;
    }

    // Strip added sort columns again if the caller wanted exact/narrow tlist.
    if tlist_was_changed && (flags & (CP_EXACT_TLIST | CP_SMALL_TLIST)) != 0 {
        let parallel_safe = ma_node.plan_head().parallel_safe;
        let new_tlist = list_copy_head_tlist(mcx, &ma_node, orig_tlist_length)?;
        return inject_projection_plan(mcx, ma_node, new_tlist, parallel_safe);
    }
    Ok(ma_node)
}

/// `list_copy_head(plan->targetlist, n)` — clone the first `n` `TargetEntry`s of
/// a plan node's targetlist into a fresh `mcx` list (used to drop the resjunk
/// sort columns added by `prepare_sort_from_pathkeys`).
fn list_copy_head_tlist<'mcx>(
    mcx: Mcx<'mcx>,
    node: &Node<'mcx>,
    n: usize,
) -> PgResult<Option<PgVec<'mcx, TargetEntry<'mcx>>>> {
    let src = node.plan_head().targetlist.as_deref().unwrap_or(&[]);
    let take = n.min(src.len());
    if take == 0 {
        return Ok(None);
    }
    let mut out = vec_with_capacity_in(mcx, take)?;
    for tle in &src[..take] {
        out.push(tle.clone_in(mcx)?);
    }
    Ok(Some(out))
}

/// `create_join_plan(root, (JoinPath *) best_path)` (createplan.c:1081) — the
/// join dispatch over HashJoin / NestLoop / MergeJoin.
fn create_join_plan<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    best_path: PathId,
) -> PgResult<Node<'mcx>> {
    let pathtype = root.path(best_path).base().pathtype;
    let joinrestrictinfo = match root.path(best_path) {
        PathNode::NestPath(p) => p.jpath.joinrestrictinfo.clone(),
        PathNode::HashPath(p) => p.jpath.joinrestrictinfo.clone(),
        PathNode::MergePath(p) => p.jpath.joinrestrictinfo.clone(),
        _ => unreachable!("create_join_plan on non-JoinPath"),
    };

    let plan = match pathtype {
        T_HashJoin => create_hashjoin_plan(mcx, root, run, best_path)?,
        T_NestLoop => create_nestloop_plan(mcx, root, run, best_path)?,
        T_MergeJoin => create_mergejoin_plan(mcx, root, run, best_path)?,
        other => {
            return Err(PgError::error(alloc::format!(
                "unrecognized node type: {}",
                other.0
            )))
        }
    };

    // If there are any pseudoconstant clauses attached to this node, insert a
    // gating Result node that evaluates the pseudoconstants as one-time quals.
    let gating_clauses = get_gating_quals(root, &joinrestrictinfo);
    if !gating_clauses.is_empty() {
        return create_gating_plan(mcx, root, best_path, plan, gating_clauses);
    }
    Ok(plan)
}

/// `create_gating_plan(root, path, plan, gating_quals)` (createplan.c:1022) —
/// stack a gating `Result` node carrying the pseudoconstant `gating_quals` atop
/// the already-built `plan`.
fn create_gating_plan<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    path: PathId,
    plan: Node<'mcx>,
    gating_quals: Vec<NodeId>,
) -> PgResult<Node<'mcx>> {
    debug_assert!(!gating_quals.is_empty());

    // Snapshot the input plan's cost/safety before we (possibly) consume it.
    let cost = plan_cost_snapshot(plan.plan_head());
    let path_parallel_safe = root.path(path).base().parallel_safe;

    // We might have a trivial Result plan already. Stacking one Result atop
    // another is silly, so discard the input plan in that case.
    let is_trivial_result = matches!(
        &plan,
        Node::Result(rplan)
            if rplan.plan.lefttree.is_none() && rplan.resconstantqual.is_none()
    );
    let splan: Option<Node<'mcx>> = if is_trivial_result { None } else { Some(plan) };

    // Always return the path's requested tlist; that's never a wrong choice.
    let tlist = build_path_tlist(mcx, root, path)?;
    let tlist = tlist_to_plan_field(mcx, tlist)?;
    let resconstantqual = nodes_to_expr_qual(mcx, root, &gating_quals)?;

    let mut gplan = make_result(mcx, tlist, resconstantqual, splan)?;

    // We don't change cost or size estimates when gating (copy_plan_costsize).
    apply_cost_snapshot(&mut gplan.plan, &cost);
    gplan.plan.parallel_safe = path_parallel_safe;

    Ok(Node::mk_result(mcx, gplan))
}

// ===========================================================================
// Upper converters: group / unique / setop / recursiveunion / gather /
// gather_merge / incrementalsort (createplan.c).
// ===========================================================================

/// Resolve a `SortGroupClause` arena `NodeId` list into owned values (the C
/// `List *` of `SortGroupClause *`). Used by group / setop / recursiveunion /
/// unique-from-sortclauses converters.
fn resolve_sort_group_clauses(
    root: &PlannerInfo,
    ids: &[NodeId],
) -> Vec<types_nodes::rawnodes::SortGroupClause> {
    ids.iter().map(|&id| *root.sortgroupclause(id)).collect()
}

/// `make_group(tlist, qual, numGroupCols, grpColIdx, grpOperators,
/// grpCollations, lefttree)` (createplan.c:6805).
#[allow(clippy::too_many_arguments)]
fn make_group<'mcx>(
    mcx: Mcx<'mcx>,
    tlist: Option<PgVec<'mcx, TargetEntry<'mcx>>>,
    qual: Option<PgVec<'mcx, Expr>>,
    num_group_cols: i32,
    grp_col_idx: Vec<AttrNumber>,
    grp_operators: Vec<Oid>,
    grp_collations: Vec<Oid>,
    lefttree: Node<'mcx>,
) -> PgResult<GroupNode<'mcx>> {
    let mut plan = Plan::default();
    plan.qual = qual;
    plan.targetlist = tlist;
    plan.lefttree = Some(mcx::alloc_in(mcx, lefttree)?);
    plan.righttree = None;
    Ok(GroupNode {
        plan,
        numCols: num_group_cols,
        grpColIdx: oid_attr_vec(mcx, grp_col_idx)?,
        grpOperators: oid_vec(mcx, grp_operators)?,
        grpCollations: oid_vec(mcx, grp_collations)?,
    })
}

/// `create_group_plan(root, (GroupPath *) best_path)` (createplan.c:2238).
fn create_group_plan<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    best_path: PathId,
) -> PgResult<Node<'mcx>> {
    let (subpath, group_clause_ids, qual_ids) = match root.path(best_path) {
        PathNode::GroupPath(p) => (p.subpath, p.groupClause.clone(), p.qual.clone()),
        _ => unreachable!("create_group_plan on non-GroupPath"),
    };
    let subpath = subpath.expect("create_group_plan: GroupPath has no subpath");

    // Group can project, but grouping columns must be available.
    let subplan = create_plan_recurse(mcx, root, run, subpath, CP_LABEL_TLIST)?;

    let tlist = build_path_tlist(mcx, root, best_path)?;
    let tlist = tlist_to_plan_field(mcx, tlist)?;

    let quals = order_qual_clauses(root, &qual_ids);
    let quals = nodes_to_expr_qual(mcx, root, &quals)?;

    let group_clauses = resolve_sort_group_clauses(root, &group_clause_ids);
    let num_group_cols = group_clauses.len() as i32;

    let subplan_tlist: &[TargetEntry<'mcx>] = match subplan.plan_head().targetlist {
        Some(ref tl) => tl.as_slice(),
        None => &[],
    };
    let grp_col_idx = util_tlist::extract_grouping_cols(&group_clauses, subplan_tlist)?;
    let grp_operators = util_tlist::extract_grouping_ops(&group_clauses);
    let grp_collations =
        util_tlist::extract_grouping_collations(&group_clauses, subplan_tlist)?;

    let mut plan = make_group(
        mcx,
        tlist,
        quals,
        num_group_cols,
        grp_col_idx,
        grp_operators,
        grp_collations,
        subplan,
    )?;

    copy_generic_path_info(&mut plan.plan, root.path(best_path).base());

    Ok(Node::mk_group(mcx, plan))
}

/// `make_windowagg(tlist, wc, partNumCols, partColIdx, partOperators,
/// partCollations, ordNumCols, ordColIdx, ordOperators, ordCollations,
/// runCondition, qual, topWindow, lefttree)` (createplan.c:6765).
fn make_windowagg<'mcx>(
    mcx: Mcx<'mcx>,
    tlist: Option<PgVec<'mcx, TargetEntry<'mcx>>>,
    wc: &types_pathnodes::WindowClauseNode,
    win_name: Option<PgString<'mcx>>,
    start_offset: Option<Expr>,
    end_offset: Option<Expr>,
    part_num_cols: i32,
    part_col_idx: Vec<AttrNumber>,
    part_operators: Vec<Oid>,
    part_collations: Vec<Oid>,
    ord_num_cols: i32,
    ord_col_idx: Vec<AttrNumber>,
    ord_operators: Vec<Oid>,
    ord_collations: Vec<Oid>,
    run_condition: Option<PgVec<'mcx, Expr>>,
    qual: Option<PgVec<'mcx, Expr>>,
    top_window: bool,
    lefttree: Node<'mcx>,
) -> PgResult<types_nodes::nodewindowagg::WindowAgg<'mcx>> {
    // node->runConditionOrig is a duplicate of runCondition for EXPLAIN.
    let run_condition_orig = match &run_condition {
        Some(v) => {
            let mut out = vec_with_capacity_in(mcx, v.len())?;
            for e in v.iter() {
                out.push(e.clone());
            }
            Some(out)
        }
        None => None,
    };

    let mut plan = Plan::default();
    plan.targetlist = tlist;
    plan.lefttree = Some(mcx::alloc_in(mcx, lefttree)?);
    plan.righttree = None;
    plan.qual = qual;

    Ok(types_nodes::nodewindowagg::WindowAgg {
        plan,
        winname: win_name,
        winref: wc.winref,
        partNumCols: part_num_cols,
        partColIdx: oid_attr_vec_opt(mcx, part_col_idx)?,
        partOperators: oid_vec_opt(mcx, part_operators)?,
        partCollations: oid_vec_opt(mcx, part_collations)?,
        ordNumCols: ord_num_cols,
        ordColIdx: oid_attr_vec_opt(mcx, ord_col_idx)?,
        ordOperators: oid_vec_opt(mcx, ord_operators)?,
        ordCollations: oid_vec_opt(mcx, ord_collations)?,
        frameOptions: wc.frameOptions,
        startOffset: match start_offset {
            Some(e) => Some(mcx::alloc_in(mcx, e)?),
            None => None,
        },
        endOffset: match end_offset {
            Some(e) => Some(mcx::alloc_in(mcx, e)?),
            None => None,
        },
        runCondition: run_condition,
        runConditionOrig: run_condition_orig,
        startInRangeFunc: wc.startInRangeFunc,
        endInRangeFunc: wc.endInRangeFunc,
        inRangeColl: wc.inRangeColl,
        inRangeAsc: wc.inRangeAsc,
        inRangeNullsFirst: wc.inRangeNullsFirst,
        topWindow: top_window,
    })
}

/// `create_windowagg_plan(root, (WindowAggPath *) best_path)` (createplan.c:2614).
fn create_windowagg_plan<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    best_path: PathId,
) -> PgResult<Node<'mcx>> {
    let (subpath, winclause_id, run_cond_ids, qual_ids, topwindow) = match root.path(best_path) {
        PathNode::WindowAggPath(p) => (
            p.subpath,
            p.winclause,
            p.runCondition.clone(),
            p.qual.clone(),
            p.topwindow,
        ),
        _ => unreachable!("create_windowagg_plan on non-WindowAggPath"),
    };
    let subpath = subpath.expect("create_windowagg_plan: WindowAggPath has no subpath");

    // Read the WindowClause fields we need before recursing (the arena borrow
    // must be released before create_plan_recurse takes &mut root).
    let (win_name, part_ids, ord_ids, start_off_id, end_off_id) = {
        let wc = root.windowclause(winclause_id);
        (
            wc.name.clone(),
            wc.partitionClause.clone(),
            wc.orderClause.clone(),
            wc.startOffset,
            wc.endOffset,
        )
    };

    // WindowAgg stores input rows in a tuplestore, so request a small tlist;
    // grouping columns must remain available (CP_LABEL_TLIST | CP_SMALL_TLIST).
    let subplan = create_plan_recurse(mcx, root, run, subpath, CP_LABEL_TLIST | CP_SMALL_TLIST)?;

    let tlist = build_path_tlist(mcx, root, best_path)?;
    let tlist = tlist_to_plan_field(mcx, tlist)?;

    // Convert the PARTITION BY / ORDER BY SortGroupClause lists into the
    // executor's attr-index / operator / collation arrays.
    let part_clauses = resolve_sort_group_clauses(root, &part_ids);
    let ord_clauses = resolve_sort_group_clauses(root, &ord_ids);

    let subplan_tlist: &[TargetEntry<'mcx>] = match subplan.plan_head().targetlist {
        Some(ref tl) => tl.as_slice(),
        None => &[],
    };

    let mut part_col_idx: Vec<AttrNumber> = Vec::with_capacity(part_clauses.len());
    let mut part_operators: Vec<Oid> = Vec::with_capacity(part_clauses.len());
    let mut part_collations: Vec<Oid> = Vec::with_capacity(part_clauses.len());
    for sgc in &part_clauses {
        let tle = util_tlist::get_sortgroupclause_tle(sgc, subplan_tlist)?;
        debug_assert!(sgc.eqop != InvalidOid);
        part_col_idx.push(tle.resno);
        part_operators.push(sgc.eqop);
        part_collations.push(expr_collation_of_tle(tle)?);
    }
    let part_num_cols = part_col_idx.len() as i32;

    let mut ord_col_idx: Vec<AttrNumber> = Vec::with_capacity(ord_clauses.len());
    let mut ord_operators: Vec<Oid> = Vec::with_capacity(ord_clauses.len());
    let mut ord_collations: Vec<Oid> = Vec::with_capacity(ord_clauses.len());
    for sgc in &ord_clauses {
        let tle = util_tlist::get_sortgroupclause_tle(sgc, subplan_tlist)?;
        debug_assert!(sgc.eqop != InvalidOid);
        ord_col_idx.push(tle.resno);
        ord_operators.push(sgc.eqop);
        ord_collations.push(expr_collation_of_tle(tle)?);
    }
    let ord_num_cols = ord_col_idx.len() as i32;

    // runCondition / qual are bare Expr (OpExpr) clause handles in the arena.
    let run_condition = nodes_to_expr_qual(mcx, root, &run_cond_ids)?;
    let qual = nodes_to_expr_qual(mcx, root, &qual_ids)?;

    // The frame start/end offset expressions are arena Expr handles.
    let start_offset = start_off_id.map(|id| root.node(id).clone());
    let end_offset = end_off_id.map(|id| root.node(id).clone());

    // wc->name -> owned PgString.
    let win_name: Option<PgString<'mcx>> = match win_name {
        Some(s) => Some(PgString::from_str_in(&s, mcx)?),
        None => None,
    };

    let wc = root.windowclause(winclause_id).clone();
    let mut plan = make_windowagg(
        mcx,
        tlist,
        &wc,
        win_name,
        start_offset,
        end_offset,
        part_num_cols,
        part_col_idx,
        part_operators,
        part_collations,
        ord_num_cols,
        ord_col_idx,
        ord_operators,
        ord_collations,
        run_condition,
        qual,
        topwindow,
        subplan,
    )?;

    copy_generic_path_info(&mut plan.plan, root.path(best_path).base());

    Ok(Node::mk_window_agg(mcx, plan))
}

/// `make_unique_from_sortclauses(lefttree, distinctList)` (createplan.c:6835) —
/// build a `Unique` plan over `lefttree`, with the dedup columns/operators
/// taken from the `SortGroupClause` list, locating each column by
/// `tleSortGroupRef` in the child's targetlist.
fn make_unique_from_sortclauses<'mcx>(
    mcx: Mcx<'mcx>,
    lefttree: Node<'mcx>,
    distinct_list: &[types_nodes::rawnodes::SortGroupClause],
) -> PgResult<UniqueNode<'mcx>> {
    let num_cols = distinct_list.len() as i32;
    debug_assert!(num_cols > 0);

    let tlist = clone_plan_tlist(mcx, &lefttree)?;

    let mut uniq_col_idx: Vec<AttrNumber> = Vec::with_capacity(num_cols as usize);
    let mut uniq_operators: Vec<Oid> = Vec::with_capacity(num_cols as usize);
    let mut uniq_collations: Vec<Oid> = Vec::with_capacity(num_cols as usize);

    {
        let plan_tlist: &[TargetEntry<'mcx>] = tlist.as_deref().unwrap_or(&[]);
        for sortcl in distinct_list {
            let tle = util_tlist::get_sortgroupclause_tle(sortcl, plan_tlist)?;
            uniq_col_idx.push(tle.resno);
            uniq_operators.push(sortcl.eqop);
            uniq_collations.push(expr_collation_of_tle(tle)?);
            debug_assert!(sortcl.eqop != InvalidOid);
        }
    }

    let mut plan = Plan::default();
    plan.targetlist = tlist;
    plan.qual = None;
    plan.lefttree = Some(mcx::alloc_in(mcx, lefttree)?);
    plan.righttree = None;
    Ok(UniqueNode {
        plan,
        numCols: num_cols,
        uniqColIdx: oid_attr_vec_opt(mcx, uniq_col_idx)?,
        uniqOperators: oid_vec_opt(mcx, uniq_operators)?,
        uniqCollations: oid_vec_opt(mcx, uniq_collations)?,
    })
}

/// `make_unique_from_pathkeys(lefttree, pathkeys, numCols)` (createplan.c:6884).
fn make_unique_from_pathkeys<'mcx>(
    mcx: Mcx<'mcx>,
    root: &PlannerInfo,
    lefttree: Node<'mcx>,
    pathkeys: &[PathKey],
    num_cols: i32,
) -> PgResult<UniqueNode<'mcx>> {
    let tlist = clone_plan_tlist(mcx, &lefttree)?;

    let mut uniq_col_idx: Vec<AttrNumber> = Vec::with_capacity(num_cols as usize);
    let mut uniq_operators: Vec<Oid> = Vec::with_capacity(num_cols as usize);
    let mut uniq_collations: Vec<Oid> = Vec::with_capacity(num_cols as usize);

    let plan_tlist: &[TargetEntry<'mcx>] = tlist.as_deref().unwrap_or(&[]);

    let mut keyno = 0i32;
    for pathkey in pathkeys {
        if keyno >= num_cols {
            break;
        }
        let ec_id = pathkey
            .pk_eclass
            .expect("make_unique_from_pathkeys: PathKey has no EquivalenceClass");
        let ec = root.ec(ec_id);

        let mut matched: Option<(AttrNumber, Oid)> = None; // (resno, pk_datatype)
        if ec.ec_has_volatile {
            if ec.ec_sortref == 0 {
                return Err(PgError::error("volatile EquivalenceClass has no sortref"));
            }
            let tle = get_sortgroupref_tle(ec.ec_sortref, plan_tlist)?;
            debug_assert_eq!(ec.ec_members.len(), 1);
            let em = root.em(ec.ec_members[0]);
            matched = Some((tle.resno, em.em_datatype));
        } else {
            for tle in plan_tlist.iter() {
                let tle_expr = tle.expr.as_deref().cloned().unwrap_or_else(|| Expr::Var(Default::default()));
                if let Some(em) = find_ec_member_matching_expr(root, ec_id, &tle_expr, &None) {
                    matched = Some((tle.resno, em.em_datatype));
                    break;
                }
            }
        }
        let (resno, pk_datatype) =
            matched.ok_or_else(|| PgError::error("could not find pathkey item to sort"))?;

        let eqop = lsyscache::get_opfamily_member_for_cmptype::call(
            pathkey.pk_opfamily,
            pk_datatype,
            pk_datatype,
            COMPARE_EQ,
        )?;
        if eqop == InvalidOid {
            return Err(PgError::error(alloc::format!(
                "missing operator {}({},{}) in opfamily {}",
                COMPARE_EQ, pk_datatype, pk_datatype, pathkey.pk_opfamily
            )));
        }
        uniq_col_idx.push(resno);
        uniq_operators.push(eqop);
        uniq_collations.push(ec.ec_collation);
        keyno += 1;
    }

    let mut plan = Plan::default();
    plan.targetlist = tlist;
    plan.qual = None;
    plan.lefttree = Some(mcx::alloc_in(mcx, lefttree)?);
    plan.righttree = None;
    Ok(UniqueNode {
        plan,
        numCols: num_cols,
        uniqColIdx: oid_attr_vec_opt(mcx, uniq_col_idx)?,
        uniqOperators: oid_vec_opt(mcx, uniq_operators)?,
        uniqCollations: oid_vec_opt(mcx, uniq_collations)?,
    })
}

/// `create_upper_unique_plan(root, (UpperUniquePath *) best_path, flags)`
/// (createplan.c:2277). The `T_Unique` dispatch arm: C also handles
/// `create_unique_plan` (a `UniquePath`, the JOIN_UNIQUE de-dup), which is not
/// reached on the SELECT-DISTINCT path and stays errored here.
fn create_unique_dispatch_plan<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    best_path: PathId,
    flags: i32,
) -> PgResult<Node<'mcx>> {
    match root.path(best_path) {
        PathNode::UpperUniquePath(_) => create_upper_unique_plan(mcx, root, run, best_path, flags),
        PathNode::UniquePath(_) => create_unique_plan(mcx, root, run, best_path, flags),
        _ => unreachable!("create_unique_dispatch_plan on non-Unique pathtype"),
    }
}

/// `create_unique_plan(root, (UniquePath *) best_path, flags)`
/// (createplan.c:1721) — convert a `UniquePath` (the JOIN_UNIQUE de-dup, also
/// the `UNION`/`DISTINCT`-via-unique path) into a `Unique`, hashed `Agg`, or
/// `Sort`+`Unique` plan over the subpath.
fn create_unique_plan<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    best_path: PathId,
    flags: i32,
) -> PgResult<Node<'mcx>> {
    let (subpath, umethod, in_operators, uniq_expr_ids) = match root.path(best_path) {
        PathNode::UniquePath(p) => (
            p.subpath,
            p.umethod,
            p.in_operators.clone(),
            p.uniq_exprs.clone(),
        ),
        _ => unreachable!("create_unique_plan on non-UniquePath"),
    };
    let subpath = subpath.expect("create_unique_plan: UniquePath has no subpath");

    // Unique doesn't project, so tlist requirements pass through.
    let mut subplan = create_plan_recurse(mcx, root, run, subpath, flags)?;

    // Done if we don't need to do any actual unique-ifying.
    if umethod == UNIQUE_PATH_NOOP {
        return Ok(subplan);
    }

    // Resolve the uniq_exprs (arena handles → owned exprs); the values we are
    // supposed to unique-ify may be expressions over the subplan's Vars.
    let uniq_exprs: Vec<Expr> = uniq_expr_ids
        .iter()
        .map(|&id| root.node(id).clone_in(mcx))
        .collect::<PgResult<Vec<_>>>()?;

    // Initialize modified subplan tlist as just the "required" vars. newtlist
    // starts from build_path_tlist() not just a copy of the subplan's tlist; we
    // don't install it into the subplan unless we are sorting or stuff has to be
    // added.
    let mut newtlist = build_path_tlist(mcx, root, best_path)?;
    let mut nextresno = newtlist.len() as i32 + 1;
    let mut newitems = false;

    for uniqexpr in uniq_exprs.iter() {
        if util_tlist::tlist_member(uniqexpr, &newtlist).is_none() {
            let tle = make_target_entry(mcx, uniqexpr.clone(), nextresno as i16, None, false)?;
            newtlist.push(tle);
            nextresno += 1;
            newitems = true;
        }
    }

    // Use change_plan_targetlist in case we need to insert a Result node.
    if newitems || umethod == UNIQUE_PATH_SORT {
        let parallel_safe = root.path(best_path).base().parallel_safe;
        let newtlist_field = tlist_to_plan_field(mcx, newtlist)?
            .expect("create_unique_plan: newtlist is non-empty");
        subplan = change_plan_targetlist(mcx, subplan, newtlist_field, parallel_safe)?;
    }

    // Build control information showing which subplan output columns are to be
    // examined by the grouping step. Read off the (possibly replaced) subplan
    // tlist.
    let num_group_cols = uniq_exprs.len();
    let mut group_col_idx: Vec<AttrNumber> = Vec::with_capacity(num_group_cols);
    let mut group_collations: Vec<Oid> = Vec::with_capacity(num_group_cols);

    {
        let sub_tlist: &[TargetEntry<'mcx>] =
            subplan.plan_head().targetlist.as_deref().unwrap_or(&[]);
        for uniqexpr in uniq_exprs.iter() {
            let tle = util_tlist::tlist_member(uniqexpr, sub_tlist).ok_or_else(|| {
                PgError::error("failed to find unique expression in subplan tlist")
            })?;
            group_col_idx.push(tle.resno);
            group_collations.push(expr_collation_of_tle(tle)?);
        }
    }

    let plan: Node<'mcx> = if umethod == UNIQUE_PATH_HASH {
        // Get the hashable equality operators for the Agg node to use. Normally
        // these are the same as the IN clause operators, but if those are
        // cross-type operators then the equality operators are the ones for the
        // IN clause operators' RHS datatype.
        let mut group_operators: Vec<Oid> = Vec::with_capacity(num_group_cols);
        for &in_oper in in_operators.iter() {
            let eq_oper = lsyscache::get_compatible_hash_operators::call(in_oper)?
                .map(|(_lhs, rhs)| rhs)
                .ok_or_else(|| {
                    PgError::error(alloc::format!(
                        "could not find compatible hash operator for operator {}",
                        in_oper
                    ))
                })?;
            group_operators.push(eq_oper);
        }

        // Since the Agg node is going to project anyway, give it the minimum
        // output tlist (build_path_tlist), without anything we added to the
        // subplan tlist.
        let agg_tlist = build_path_tlist(mcx, root, best_path)?;
        let agg_tlist = tlist_to_plan_field(mcx, agg_tlist)?;
        let rows = root.path(best_path).base().rows;

        let agg = make_agg(
            mcx,
            agg_tlist,
            None, // qual = NIL
            types_nodes::nodeagg::AggStrategy::AggHashed,
            AGGSPLIT_SIMPLE,
            num_group_cols as i32,
            group_col_idx,
            group_operators,
            group_collations,
            None, // groupingSets = NIL
            None, // chain = NIL
            rows,
            0, // transitionSpace
            subplan,
        )?;
        Node::mk_agg(mcx, agg)
    } else {
        // Create an ORDER BY list to sort the input compatibly, deriving the
        // SortGroupClause for each IN-clause operator.
        let mut sort_list: Vec<types_nodes::rawnodes::SortGroupClause> =
            Vec::with_capacity(in_operators.len());
        for (group_col_pos, &in_oper) in in_operators.iter().enumerate() {
            let sortop = lsyscache::get_ordering_op_for_equality_op::call(in_oper, false)?;
            if sortop == InvalidOid {
                return Err(PgError::error(alloc::format!(
                    "could not find ordering operator for equality operator {}",
                    in_oper
                )));
            }
            // The Unique node will need equality operators. Normally these are
            // the same as the IN clause operators, but if those are cross-type
            // operators then the equality operators are the ones for the IN
            // clause operators' RHS datatype.
            let eqop = lsyscache::get_equality_op_for_ordering_op::call(sortop)?
                .map(|(op, _reverse)| op)
                .unwrap_or(InvalidOid);
            if eqop == InvalidOid {
                return Err(PgError::error(alloc::format!(
                    "could not find equality operator for ordering operator {}",
                    sortop
                )));
            }

            // Locate the tle in the subplan tlist by groupColIdx[group_col_pos]
            // and assign it a sortgroupref so make_sort_from_sortclauses can
            // find it. This mutates the subplan's targetlist in place.
            let resno = group_col_idx[group_col_pos];
            let sortref = {
                let sub_tlist: &mut [TargetEntry<'mcx>] =
                    subplan.plan_head_mut().targetlist.as_deref_mut().unwrap_or(&mut []);
                let tle_idx = sub_tlist
                    .iter()
                    .position(|tle| tle.resno == resno)
                    .expect("create_unique_plan: groupColIdx tle not in subplan tlist");
                assign_sort_group_ref(tle_idx, sub_tlist)
            };

            sort_list.push(types_nodes::rawnodes::SortGroupClause {
                tleSortGroupRef: sortref,
                eqop,
                sortop,
                reverse_sort: false,
                nulls_first: false,
                hashable: false, // no need to make this accurate
            });
        }

        let mut sort = make_sort_from_sortclauses(mcx, &sort_list, subplan)?;
        label_sort_with_costsize(root, &mut sort, -1.0);
        let unique = make_unique_from_sortclauses(mcx, Node::mk_sort(mcx, sort), &sort_list)?;
        Node::mk_unique(mcx, unique)
    };

    // Copy cost data from Path to Plan.
    let mut plan = plan;
    copy_generic_path_info(plan.plan_head_mut(), root.path(best_path).base());

    Ok(plan)
}

/// `assignSortGroupRef(tle, tlist)` (parse_clause.c) — ensure the targetlist
/// entry at `tle_idx` has a `ressortgroupref`, picking max-used+1 if unset.
fn assign_sort_group_ref(tle_idx: usize, tlist: &mut [TargetEntry<'_>]) -> Index {
    if tlist[tle_idx].ressortgroupref != 0 {
        return tlist[tle_idx].ressortgroupref;
    }
    let mut max_ref: Index = 0;
    for tle in tlist.iter() {
        if tle.ressortgroupref > max_ref {
            max_ref = tle.ressortgroupref;
        }
    }
    tlist[tle_idx].ressortgroupref = max_ref + 1;
    tlist[tle_idx].ressortgroupref
}

fn create_upper_unique_plan<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    best_path: PathId,
    flags: i32,
) -> PgResult<Node<'mcx>> {
    let (subpath, numkeys) = match root.path(best_path) {
        PathNode::UpperUniquePath(p) => (p.subpath, p.numkeys),
        _ => unreachable!("create_upper_unique_plan on non-UpperUniquePath"),
    };
    let subpath = subpath.expect("create_upper_unique_plan: UpperUniquePath has no subpath");

    // Unique doesn't project; tlist requirements pass through, and grouping
    // columns must be labeled.
    let subplan = create_plan_recurse(mcx, root, run, subpath, flags | CP_LABEL_TLIST)?;

    let pathkeys = root.path(best_path).base().pathkeys.clone();
    let mut plan = make_unique_from_pathkeys(mcx, root, subplan, &pathkeys, numkeys)?;

    copy_generic_path_info(&mut plan.plan, root.path(best_path).base());

    Ok(Node::mk_unique(mcx, plan))
}

/// `make_setop(cmd, strategy, tlist, lefttree, righttree, groupList, numGroups)`
/// (createplan.c:7019).
#[allow(clippy::too_many_arguments)]
fn make_setop<'mcx>(
    mcx: Mcx<'mcx>,
    cmd: types_nodes::nodesetop::SetOpCmd,
    strategy: types_nodes::nodesetop::SetOpStrategy,
    tlist: Option<PgVec<'mcx, TargetEntry<'mcx>>>,
    lefttree: Node<'mcx>,
    righttree: Node<'mcx>,
    group_list: &[types_nodes::rawnodes::SortGroupClause],
    num_groups: i64,
) -> PgResult<SetOpNode<'mcx>> {
    let num_cols = group_list.len() as i32;

    let mut plan = Plan::default();
    plan.targetlist = tlist;
    plan.qual = None;
    plan.lefttree = Some(mcx::alloc_in(mcx, lefttree)?);
    plan.righttree = Some(mcx::alloc_in(mcx, righttree)?);

    let plan_tlist: &[TargetEntry<'mcx>] = plan.targetlist.as_deref().unwrap_or(&[]);

    let mut cmp_col_idx: PgVec<'mcx, AttrNumber> = vec_with_capacity_in(mcx, group_list.len())?;
    let mut cmp_operators: PgVec<'mcx, Oid> = vec_with_capacity_in(mcx, group_list.len())?;
    let mut cmp_collations: PgVec<'mcx, Oid> = vec_with_capacity_in(mcx, group_list.len())?;
    let mut cmp_nulls_first: PgVec<'mcx, bool> = vec_with_capacity_in(mcx, group_list.len())?;
    for sortcl in group_list {
        let tle = util_tlist::get_sortgroupclause_tle(sortcl, plan_tlist)?;
        cmp_col_idx.push(tle.resno);
        if strategy == types_nodes::nodesetop::SETOP_HASHED {
            cmp_operators.push(sortcl.eqop);
        } else {
            cmp_operators.push(sortcl.sortop);
        }
        cmp_collations.push(expr_collation_of_tle(tle)?);
        cmp_nulls_first.push(sortcl.nulls_first);
    }

    Ok(SetOpNode {
        plan,
        cmd,
        strategy,
        numCols: num_cols,
        cmpColIdx: cmp_col_idx,
        cmpOperators: cmp_operators,
        cmpCollations: cmp_collations,
        cmpNullsFirst: cmp_nulls_first,
        numGroups: num_groups,
    })
}

/// `create_setop_plan(root, (SetOpPath *) best_path, flags)` (createplan.c:2709).
fn create_setop_plan<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    best_path: PathId,
    flags: i32,
) -> PgResult<Node<'mcx>> {
    let (leftpath, rightpath, cmd, strategy, group_list_ids, num_groups) =
        match root.path(best_path) {
            PathNode::SetOpPath(p) => (
                p.leftpath,
                p.rightpath,
                p.cmd,
                p.strategy,
                p.groupList.clone(),
                p.numGroups,
            ),
            _ => unreachable!("create_setop_plan on non-SetOpPath"),
        };
    let leftpath = leftpath.expect("create_setop_plan: SetOpPath has no leftpath");
    let rightpath = rightpath.expect("create_setop_plan: SetOpPath has no rightpath");

    let tlist = build_path_tlist(mcx, root, best_path)?;

    // SetOp doesn't project; grouping columns must be labeled.
    let leftplan = create_plan_recurse(mcx, root, run, leftpath, flags | CP_LABEL_TLIST)?;
    let rightplan = create_plan_recurse(mcx, root, run, rightpath, flags | CP_LABEL_TLIST)?;

    let num_groups = costsize::clamp_cardinality_to_long::call(num_groups);

    let group_list = resolve_sort_group_clauses(root, &group_list_ids);
    let tlist = tlist_to_plan_field(mcx, tlist)?;

    let mut plan = make_setop(
        mcx,
        cmd as types_nodes::nodesetop::SetOpCmd,
        strategy as types_nodes::nodesetop::SetOpStrategy,
        tlist,
        leftplan,
        rightplan,
        &group_list,
        num_groups,
    )?;

    copy_generic_path_info(&mut plan.plan, root.path(best_path).base());

    Ok(Node::mk_set_op(mcx, plan))
}

/// `make_recursive_union(tlist, lefttree, righttree, wtParam, distinctList,
/// numGroups)` (createplan.c:5997).
#[allow(clippy::too_many_arguments)]
fn make_recursive_union<'mcx>(
    mcx: Mcx<'mcx>,
    tlist: Option<PgVec<'mcx, TargetEntry<'mcx>>>,
    lefttree: Node<'mcx>,
    righttree: Node<'mcx>,
    wt_param: i32,
    distinct_list: &[types_nodes::rawnodes::SortGroupClause],
    num_groups: i64,
) -> PgResult<RecursiveUnionNode<'mcx>> {
    let num_cols = distinct_list.len() as i32;

    let mut plan = Plan::default();
    plan.targetlist = tlist;
    plan.qual = None;
    plan.lefttree = Some(mcx::alloc_in(mcx, lefttree)?);
    plan.righttree = Some(mcx::alloc_in(mcx, righttree)?);

    let plan_tlist: &[TargetEntry<'mcx>] = plan.targetlist.as_deref().unwrap_or(&[]);

    let mut dup_col_idx: PgVec<'mcx, AttrNumber> = vec_with_capacity_in(mcx, distinct_list.len())?;
    let mut dup_operators: PgVec<'mcx, Oid> = vec_with_capacity_in(mcx, distinct_list.len())?;
    let mut dup_collations: PgVec<'mcx, Oid> = vec_with_capacity_in(mcx, distinct_list.len())?;
    for sortcl in distinct_list {
        let tle = util_tlist::get_sortgroupclause_tle(sortcl, plan_tlist)?;
        dup_col_idx.push(tle.resno);
        dup_operators.push(sortcl.eqop);
        dup_collations.push(expr_collation_of_tle(tle)?);
    }

    Ok(RecursiveUnionNode {
        plan,
        wtParam: wt_param,
        numCols: num_cols,
        dupColIdx: dup_col_idx,
        dupOperators: dup_operators,
        dupCollations: dup_collations,
        numGroups: num_groups,
    })
}

/// `create_recursiveunion_plan(root, (RecursiveUnionPath *) best_path)`
/// (createplan.c:2749).
fn create_recursiveunion_plan<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    best_path: PathId,
) -> PgResult<Node<'mcx>> {
    let (leftpath, rightpath, distinct_list_ids, wt_param, num_groups) =
        match root.path(best_path) {
            PathNode::RecursiveUnionPath(p) => (
                p.leftpath,
                p.rightpath,
                p.distinctList.clone(),
                p.wtParam,
                p.numGroups,
            ),
            _ => unreachable!("create_recursiveunion_plan on non-RecursiveUnionPath"),
        };
    let leftpath = leftpath.expect("create_recursiveunion_plan: RecursiveUnionPath has no leftpath");
    let rightpath =
        rightpath.expect("create_recursiveunion_plan: RecursiveUnionPath has no rightpath");

    // Need both children to produce the same tlist, so force it.
    let leftplan = create_plan_recurse(mcx, root, run, leftpath, CP_EXACT_TLIST)?;
    let rightplan = create_plan_recurse(mcx, root, run, rightpath, CP_EXACT_TLIST)?;

    let tlist = build_path_tlist(mcx, root, best_path)?;
    let tlist = tlist_to_plan_field(mcx, tlist)?;

    let num_groups = costsize::clamp_cardinality_to_long::call(num_groups);
    let distinct_list = resolve_sort_group_clauses(root, &distinct_list_ids);

    let mut plan = make_recursive_union(
        mcx,
        tlist,
        leftplan,
        rightplan,
        wt_param,
        &distinct_list,
        num_groups,
    )?;

    copy_generic_path_info(&mut plan.plan, root.path(best_path).base());

    Ok(Node::mk_recursive_union(mcx, plan))
}

/// `make_gather(qptlist, qpqual, nworkers, rescan_param, single_copy, subplan)`
/// (createplan.c:6990).
fn make_gather<'mcx>(
    mcx: Mcx<'mcx>,
    qptlist: Option<PgVec<'mcx, TargetEntry<'mcx>>>,
    nworkers: i32,
    rescan_param: i32,
    single_copy: bool,
    subplan: Node<'mcx>,
) -> PgResult<GatherNode<'mcx>> {
    let mut node = GatherNode::default();
    {
        let plan: &mut Plan = &mut node.plan;
        plan.targetlist = qptlist;
        plan.qual = None; // qpqual is NIL at the only call site
        plan.lefttree = Some(mcx::alloc_in(mcx, subplan)?);
        plan.righttree = None;
    }
    node.num_workers = nworkers;
    node.rescan_param = rescan_param;
    node.single_copy = single_copy;
    node.invisible = false;
    node.initParam = None;
    Ok(node)
}

/// `create_gather_plan(root, (GatherPath *) best_path)` (createplan.c:1921).
fn create_gather_plan<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    best_path: PathId,
) -> PgResult<Node<'mcx>> {
    let (subpath, num_workers, single_copy) = match root.path(best_path) {
        PathNode::GatherPath(p) => (p.subpath, p.num_workers, p.single_copy),
        _ => unreachable!("create_gather_plan on non-GatherPath"),
    };
    let subpath = subpath.expect("create_gather_plan: GatherPath has no subpath");

    // Push projection down to the child node.
    let subplan = create_plan_recurse(mcx, root, run, subpath, CP_EXACT_TLIST)?;

    let tlist = build_path_tlist(mcx, root, best_path)?;
    let tlist = tlist_to_plan_field(mcx, tlist)?;

    let rescan_param = paramassign::assign_special_exec_param::call(root)?;

    let mut gather_plan = make_gather(mcx, tlist, num_workers, rescan_param, single_copy, subplan)?;

    copy_generic_path_info(&mut gather_plan.plan, root.path(best_path).base());

    // Use parallel mode for parallel plans.
    if let Some(g) = root.glob.as_mut() {
        g.parallel_mode_needed = true;
    }

    Ok(Node::mk_gather(mcx, gather_plan))
}

/// `create_gather_merge_plan(root, (GatherMergePath *) best_path)`
/// (createplan.c:1959).
fn create_gather_merge_plan<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    best_path: PathId,
) -> PgResult<Node<'mcx>> {
    let (subpath, num_workers) = match root.path(best_path) {
        PathNode::GatherMergePath(p) => (p.subpath, p.num_workers),
        _ => unreachable!("create_gather_merge_plan on non-GatherMergePath"),
    };
    let subpath = subpath.expect("create_gather_merge_plan: GatherMergePath has no subpath");
    let pathkeys = root.path(best_path).base().pathkeys.clone();

    let tlist = build_path_tlist(mcx, root, best_path)?;

    // As with Gather, project away columns in the workers.
    let subplan = create_plan_recurse(mcx, root, run, subpath, CP_EXACT_TLIST)?;

    let rescan_param = paramassign::assign_special_exec_param::call(root)?;

    // Gather Merge is pointless with no pathkeys.
    debug_assert!(!pathkeys.is_empty());

    let subpath_parent = root.path(subpath).base().parent;
    let relids = root.rel(subpath_parent).relids.clone();
    let (subplan, sort_col_idx, sort_operators, collations, nulls_first) =
        prepare_sort_from_pathkeys(mcx, root, subplan, &pathkeys, &relids, None, false)?;

    let num_cols = sort_col_idx.len() as i32;

    let mut node = GatherMergeNode::default();
    node.plan.targetlist = tlist_to_plan_field(mcx, tlist)?;
    node.num_workers = num_workers;
    copy_generic_path_info(&mut node.plan, root.path(best_path).base());
    node.rescan_param = rescan_param;
    node.numCols = num_cols;
    node.sortColIdx = sort_col_idx.iter().copied().collect();
    node.sortOperators = sort_operators.iter().copied().collect();
    node.collations = collations.iter().copied().collect();
    node.nullsFirst = nulls_first.iter().copied().collect();
    node.plan.lefttree = Some(mcx::alloc_in(mcx, subplan)?);

    if let Some(g) = root.glob.as_mut() {
        g.parallel_mode_needed = true;
    }

    Ok(Node::mk_gather_merge(mcx, node))
}

/// `make_incrementalsort(lefttree, numCols, nPresortedCols, sortColIdx,
/// sortOperators, collations, nullsFirst)` (createplan.c:6234).
fn make_incrementalsort<'mcx>(
    mcx: Mcx<'mcx>,
    lefttree: Node<'mcx>,
    n_presorted_cols: i32,
    sort_col_idx: PgVec<'mcx, AttrNumber>,
    sort_operators: PgVec<'mcx, Oid>,
    collations: PgVec<'mcx, Oid>,
    nulls_first: PgVec<'mcx, bool>,
) -> PgResult<IncrementalSortNode<'mcx>> {
    let num_cols = sort_col_idx.len() as i32;
    let tlist = clone_plan_tlist(mcx, &lefttree)?;
    let mut plan = Plan::default();
    plan.targetlist = tlist;
    plan.qual = None;
    plan.lefttree = Some(mcx::alloc_in(mcx, lefttree)?);
    plan.righttree = None;
    let sort = Sort {
        plan,
        numCols: num_cols,
        sortColIdx: sort_col_idx,
        sortOperators: sort_operators,
        collations,
        nullsFirst: nulls_first,
    };
    Ok(IncrementalSortNode {
        sort,
        nPresortedCols: n_presorted_cols,
    })
}

/// `make_incrementalsort_from_pathkeys(lefttree, pathkeys, relids,
/// nPresortedCols)` (createplan.c:6516).
fn make_incrementalsort_from_pathkeys<'mcx>(
    mcx: Mcx<'mcx>,
    root: &PlannerInfo,
    lefttree: Node<'mcx>,
    pathkeys: &[PathKey],
    relids: &Relids,
    n_presorted_cols: i32,
) -> PgResult<IncrementalSortNode<'mcx>> {
    let (lefttree, sort_col_idx, sort_operators, collations, nulls_first) =
        prepare_sort_from_pathkeys(mcx, root, lefttree, pathkeys, relids, None, false)?;
    make_incrementalsort(
        mcx,
        lefttree,
        n_presorted_cols,
        sort_col_idx,
        sort_operators,
        collations,
        nulls_first,
    )
}

/// `create_incrementalsort_plan(root, (IncrementalSortPath *) best_path, flags)`
/// (createplan.c:2211).
fn create_incrementalsort_plan<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    best_path: PathId,
    flags: i32,
) -> PgResult<Node<'mcx>> {
    let (subpath, n_presorted_cols) = match root.path(best_path) {
        PathNode::IncrementalSortPath(p) => (p.spath.subpath, p.nPresortedCols),
        _ => unreachable!("create_incrementalsort_plan on non-IncrementalSortPath"),
    };
    let subpath = subpath.expect("create_incrementalsort_plan: IncrementalSortPath has no subpath");

    // See create_sort_plan: request a small tlist.
    let subplan = create_plan_recurse(mcx, root, run, subpath, flags | CP_SMALL_TLIST)?;

    // IS_OTHER_REL(subpath->parent) ? path.parent->relids : NULL.
    let subpath_parent = root.path(subpath).base().parent;
    let relids: Relids = if root.rel(subpath_parent).reloptkind >= RELOPT_OTHER_MEMBER_REL {
        let parent = root.path(best_path).base().parent;
        root.rel(parent).relids.clone()
    } else {
        None
    };
    let pathkeys = root.path(best_path).base().pathkeys.clone();

    let mut plan =
        make_incrementalsort_from_pathkeys(mcx, root, subplan, &pathkeys, &relids, n_presorted_cols)?;

    copy_generic_path_info(&mut plan.sort.plan, root.path(best_path).base());

    Ok(Node::mk_incremental_sort(mcx, plan))
}

// ---------------------------------------------------------------------------
// Small owned-field / list helpers shared by the join + upper converters.
// ---------------------------------------------------------------------------

/// The cost/size fields `copy_plan_costsize` reads from a source `Plan`. We
/// snapshot them by value (rather than borrowing the whole `Plan`, which holds
/// non-`Clone` owned children) so the source node can then be moved.
struct PlanCostSnapshot {
    disabled_nodes: i32,
    startup_cost: f64,
    total_cost: f64,
    plan_rows: f64,
    plan_width: i32,
    parallel_safe: bool,
}

fn plan_cost_snapshot(p: &Plan) -> PlanCostSnapshot {
    PlanCostSnapshot {
        disabled_nodes: p.disabled_nodes,
        startup_cost: p.startup_cost,
        total_cost: p.total_cost,
        plan_rows: p.plan_rows,
        plan_width: p.plan_width,
        parallel_safe: p.parallel_safe,
    }
}

/// `copy_plan_costsize(dest, src)` over the by-value snapshot: the inserted node
/// is assumed not parallel-aware and parallel-safe iff the child is.
fn apply_cost_snapshot(dest: &mut Plan, src: &PlanCostSnapshot) {
    dest.disabled_nodes = src.disabled_nodes;
    dest.startup_cost = src.startup_cost;
    dest.total_cost = src.total_cost;
    dest.plan_rows = src.plan_rows;
    dest.plan_width = src.plan_width;
    dest.parallel_aware = false;
    dest.parallel_safe = src.parallel_safe;
}

/// `exprCollation((Node *) tle->expr)` over a `TargetEntry`.
fn expr_collation_of_tle(tle: &TargetEntry<'_>) -> PgResult<Oid> {
    backend_nodes_core::nodefuncs::expr_collation(tle.expr.as_deref())
}

/// `get_tle_by_resno(tlist, resno)` (parse_relation.c / tlist helpers) — the
/// (first) targetlist entry with the given `resno`, or `None`.
fn get_tle_by_resno<'a, 'mcx>(
    tlist: &'a [TargetEntry<'mcx>],
    resno: AttrNumber,
) -> Option<&'a TargetEntry<'mcx>> {
    tlist.iter().find(|tle| tle.resno == resno)
}

/// Replace nestloop params over a list of clause `NodeId`s (the C
/// `replace_nestloop_params(root, (Node *) clause_list)` applied per element).
fn replace_nestloop_params_list(
    mcx: Mcx<'_>,
    root: &mut PlannerInfo,
    clauses: &[NodeId],
) -> PgResult<Vec<NodeId>> {
    let mut out = Vec::with_capacity(clauses.len());
    for &cid in clauses {
        out.push(replace_nestloop_params(mcx, root, cid)?);
    }
    Ok(out)
}

/// `PATH_REQ_OUTER(path)` — the path's required-outer relids (from
/// `param_info->ppi_req_outer`), or NULL.
fn path_req_outer(root: &PlannerInfo, path: PathId) -> Relids {
    match root.path(path).base().param_info.as_deref() {
        Some(ppi) => ppi.ppi_req_outer.clone(),
        None => None,
    }
}

/// Convert a `Vec<AttrNumber>` (palloc'd array) into the owned plan-node field
/// `PgVec<AttrNumber>` (Group's are non-optional).
fn oid_attr_vec<'mcx>(mcx: Mcx<'mcx>, v: Vec<AttrNumber>) -> PgResult<PgVec<'mcx, AttrNumber>> {
    let mut out = vec_with_capacity_in(mcx, v.len())?;
    for x in v {
        out.push(x);
    }
    Ok(out)
}

/// Convert a `Vec<Oid>` (palloc'd array) into the owned plan-node field
/// `PgVec<Oid>` (non-optional).
fn oid_vec<'mcx>(mcx: Mcx<'mcx>, v: Vec<Oid>) -> PgResult<PgVec<'mcx, Oid>> {
    let mut out = vec_with_capacity_in(mcx, v.len())?;
    for x in v {
        out.push(x);
    }
    Ok(out)
}

/// `Option<PgVec<AttrNumber>>` field (Unique's; empty = `None`).
fn oid_attr_vec_opt<'mcx>(
    mcx: Mcx<'mcx>,
    v: Vec<AttrNumber>,
) -> PgResult<Option<PgVec<'mcx, AttrNumber>>> {
    if v.is_empty() {
        return Ok(None);
    }
    Ok(Some(oid_attr_vec(mcx, v)?))
}

/// `Option<PgVec<Oid>>` field (Unique's; empty = `None`).
fn oid_vec_opt<'mcx>(mcx: Mcx<'mcx>, v: Vec<Oid>) -> PgResult<Option<PgVec<'mcx, Oid>>> {
    if v.is_empty() {
        return Ok(None);
    }
    Ok(Some(oid_vec(mcx, v)?))
}

/// Wrap an owned `Expr` into the `Node<'mcx>` carrier the HashJoin hashkey /
/// hashclause lists hold (`PgVec<Node>`).
fn expr_to_node<'mcx>(e: Expr) -> Node<'mcx> {
    Node::Expr(e)
}

/// Move an owned `Vec<Expr>` into a `PgVec<Node>` (the HashJoin `hashclauses`
/// field; empty = `None`).
fn exprs_to_node_list<'mcx>(
    mcx: Mcx<'mcx>,
    v: Vec<Expr>,
) -> PgResult<Option<PgVec<'mcx, Node<'mcx>>>> {
    if v.is_empty() {
        return Ok(None);
    }
    let mut out = vec_with_capacity_in(mcx, v.len())?;
    for e in v {
        out.push(Node::Expr(e));
    }
    Ok(Some(out))
}

pub fn init_seams() {
    // `is_projection_capable_path` is owned here but consumed by pathnode/other
    // optimizer crates, so it stays a real (cross-crate) seam install.
    pathnode::is_projection_capable_path::set(is_projection_capable_path);

    // `materialize_finished_plan` is owned here (createplan.c) but consumed by
    // the subselect cohort (`build_subplan` wraps an uncorrelated non-init
    // ANY/ALL subplan in a Material node). Cross-crate install through the
    // init-subselect-ext seam crate.
    backend_optimizer_plan_init_subselect_ext_seams::materialize_finished_plan::set(
        materialize_finished_plan,
    );

    // `create_indexscan_plan` is reached from create_scan_plan's dispatch via the
    // `cp_seam` indirection (declared in this unit's -seams crate). Install it now
    // that the converter is ported (covers both T_IndexScan and T_IndexOnlyScan).
    cp_seam::create_indexscan_plan::set(create_indexscan_plan);

    // `create_bitmap_scan_plan` is reached from create_scan_plan's dispatch via
    // the `cp_seam` indirection (declared in this unit's -seams crate). Install
    // it now that the converter is ported (BitmapHeapScan over BitmapIndexScan/
    // BitmapAnd/BitmapOr).
    cp_seam::create_bitmap_scan_plan::set(create_bitmap_scan_plan);

    // `create_subqueryscan_subplan`: the subroot-recursion leg of
    // create_subqueryscan_plan. For set-operation children the subquery path
    // subtree has been deep-imported into THIS root's arena
    // (import_path_from_subroot), so the SubqueryScanPath's subpath resolves here
    // and create_plan_recurse(root, subpath) builds the child plan directly — no
    // separate subroot context. Install that in-root resolution.
    cp_seam::create_subqueryscan_subplan::set(create_subqueryscan_subplan_inroot);

    // The per-family `create_*_plan` converters are NO LONGER seams: createplan.c
    // is a single translation unit, so create_plan_recurse / create_scan_plan
    // dispatch to them as direct in-crate calls (see the match arms above). Their
    // former `cp_seam::create_*_plan` indirection seams have been deleted. The
    // converters that remain `cp_seam::*` calls (index/bitmap/tablefunc/foreign/
    // custom scan; minmaxagg/memoize/windowagg/lockrows/modifytable; the
    // cte/worktable/subquery SubPlan resolution legs; apply_tlist_labeling /
    // ss_attach_initplans) are genuine cross-/forward-boundary seams owned by an
    // unported unit or installed by another crate — those decls are kept.
}

#[cfg(test)]
mod tests;
