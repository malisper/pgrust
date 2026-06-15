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

use mcx::Mcx;
use types_error::{PgError, PgResult};
use types_nodes::nodeindexscan::Plan;
use types_nodes::nodes::{Node, NodeTag};
use types_nodes::primnodes::Expr;
use types_pathnodes::planner_run::PlannerRun;
use types_pathnodes::{NodeId, Path, PathId, PathNode, PlannerInfo, RinfoId};

use backend_nodes_core::nodefuncs::expression_tree_mutator;
use backend_optimizer_plan_createplan_seams as cp_seam;
use backend_optimizer_util_paramassign_seams as paramassign;
use backend_optimizer_util_pathnode_seams as pathnode;
use backend_optimizer_util_placeholder_seams as placeholder;
use backend_optimizer_util_relnode_seams as relnode;

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
fn replace_nestloop_params_mutator(
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
