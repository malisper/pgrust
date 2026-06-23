//! `optimizer/plan/planner.c` — the query optimizer external interface.
//!
//! This crate ports `planner()` / `standard_planner()` / `subquery_planner()`
//! and the planner-entry helpers (`preprocess_expression`,
//! `preprocess_qual_conditions`, `grouping_planner` spine,
//! `preprocess_rowmarks`, `select_rowmark_type`, `preprocess_limit`,
//! `limit_needed`, `preprocess_groupclause`, `expression_planner`,
//! `plan_cluster_use_sort`).
//!
//! Model notes (mirror PG and panic):
//!
//!  * `PlannerInfo` is lifetime-free; the owned top `Query<'mcx>` (and every
//!    sub-Query / RTE / SubPlan / PlanRowMark) lives in the
//!    [`PlannerRun`](pathnodes::planner_run::PlannerRun) store behind a
//!    handle (`root.parse: QueryId`). The planner driver threads
//!    `&mut PlannerRun<'mcx>` alongside `&mut PlannerInfo`.
//!
//!  * `grouping_planner`'s simple-SELECT spine (no GROUP BY / aggregate /
//!    window / DISTINCT / ORDER BY / set-op / SRF / LIMIT, plain `CMD_SELECT`,
//!    no rowmarks) is ported in full: `create_pathtarget` over
//!    `processed_tlist` (`make_pathtarget_from_tlist` + `set_pathtarget_cost_width`),
//!    `apply_scanjoin_target_to_paths` (the non-partitioned single-target case),
//!    saving `upper_targets[]`, building the `UPPERREL_FINAL` rel, and adding
//!    every surviving scan/join path to it. The grouping / window / distinct /
//!    ordered / SRF / limit / ModifyTable upper-path builders are NOT ported and
//!    each path through `grouping_planner` that needs one loud-panics at the
//!    precise C site.
//!
//!  * The per-field expression-preprocessing block in `subquery_planner`
//!    (`preprocess_expression` over `targetList` / quals / etc.) crosses the
//!    `Query` field model (`NodePtr<'mcx>` = `PgBox<Node>`) and the arena
//!    `Expr` model that `eval_const_expressions` / `canonicalize_qual` operate
//!    on. There is no `Node`↔arena-`Expr` bridge for these `Query` fields in
//!    this repo, and several callees (`flatten_join_alias_vars`,
//!    `flatten_group_exprs`, `expand_grouping_sets`, `transform_MERGE_to_join`)
//!    have no ported owner. That whole block is therefore reached as a precise
//!    loud panic naming the unported owners, exactly where the C performs it.
//!    The `preprocess_expression` / `preprocess_qual_conditions` *functions*
//!    are ported in full against the arena `Expr` model (over
//!    `eval_const_expressions` / `canonicalize_qual` / `make_ands_implicit`),
//!    so they are correct once a `Node`↔`Expr` bridge for `Query` fields
//!    lands.

#![no_std]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

extern crate alloc;
// `std` for the GUC backing-store `thread_local!` cells (the `conf->variable`
// process-private storage for this crate's three planner GUCs).
extern crate std;

use alloc::boxed::Box;
use alloc::vec::Vec;

use mcx::Mcx;
use types_error::{PgError, PgResult};

use ::nodes::copy_query::Query;
use ::nodes::nodeindexscan::PlannedStmt;
use ::nodes::nodes::{ntag, CmdType, Node};
use ::nodes::execnodes::RowMarkType;
use ::nodes::primnodes::Expr;
use ::nodes::TargetEntry;
use ::nodes::parsenodes::RangeTblEntry;
use ::nodes::rawnodes::LockClauseStrength;

use pathnodes::planner_run::PlannerRun;
use pathnodes::{
    JoinDomain, PathId, PathTarget, PlannerGlobal, PlannerInfo, RangeTblEntryId, RelId, Relids,
    UPPERREL_DISTINCT, UPPERREL_FINAL, UPPERREL_GROUP_AGG, UPPERREL_ORDERED,
    UPPERREL_PARTIAL_DISTINCT, UPPERREL_PARTIAL_GROUP_AGG, UPPERREL_WINDOW,
};

use types_core::Oid;

use parsenodes::{PROPARALLEL_SAFE, PROPARALLEL_UNSAFE};

use guc_tables::consts::{
    DEBUG_PARALLEL_OFF, DEBUG_PARALLEL_REGRESS, DEFAULT_CURSOR_TUPLE_FRACTION,
};

// Cursor-option flags (portalcmds.h).
use ::nodes::portalcmds::{
    CURSOR_OPT_FAST_PLAN, CURSOR_OPT_PARALLEL_OK, CURSOR_OPT_SCROLL,
};

// PGJIT_NONE (jit/jit.h).
const PGJIT_NONE: i32 = 0;

// `cursor_tuple_fraction` GUC, read through the guc-tables slot (the planner
// owns the `conf->variable` backing — see the GUC backing-storage block below
// — so the live value reaches the C read site, defaulting to
// `DEFAULT_CURSOR_TUPLE_FRACTION`).
#[inline]
fn cursor_tuple_fraction() -> f64 {
    guc_tables::vars::cursor_tuple_fraction.read()
}

// `debug_parallel_query` GUC. The parallel-Gather test path of
// `standard_planner` only runs when this != OFF. planner.c owns the `int
// debug_parallel_query` global; we read its runtime-mutable backing cell (boot
// value `DEBUG_PARALLEL_OFF`), so a SET takes effect exactly as in C.
fn debug_parallel_query() -> i32 {
    DEBUG_PARALLEL_QUERY.with(core::cell::Cell::get)
}

// ===========================================================================
// GUC variable backing storage.
//
// Three planner GUCs whose C `conf->variable` backing globals live in
// planner.c / clauses.c:
//
//   * `double cursor_tuple_fraction = DEFAULT_CURSOR_TUPLE_FRACTION;`
//     (planner.c) — the documented default.
//   * `bool enable_distinct_reordering = true;` (clauses.c) — read by
//     `standard_qp_callback`/grouping_planner via the planner.
//   * `bool parallel_leader_participation = true;` (clauses.c) — read at
//     plan/execute time for Gather and Gather Merge.
//
// Per PG 18.3 guc_tables.c all three are plain GUC slot variables read
// directly from `conf->variable` (none come from the ControlFile). Each is a
// process-private backend variable, mirrored here as a `thread_local!` cell
// with C-named get/set accessors that the GUC engine installs and reads via
// the guc-tables variable slot (`vars::<name>.read()/.write()`).
// ===========================================================================

std::thread_local! { // expanded via the std prelude macro
    // `double cursor_tuple_fraction = DEFAULT_CURSOR_TUPLE_FRACTION;`
    static CURSOR_TUPLE_FRACTION: core::cell::Cell<f64> =
        const { core::cell::Cell::new(DEFAULT_CURSOR_TUPLE_FRACTION) };
    // `bool enable_distinct_reordering = true;`
    static ENABLE_DISTINCT_REORDERING: core::cell::Cell<bool> =
        const { core::cell::Cell::new(true) };
    // `bool parallel_leader_participation = true;`
    static PARALLEL_LEADER_PARTICIPATION: core::cell::Cell<bool> =
        const { core::cell::Cell::new(true) };
    // `int debug_parallel_query = DEBUG_PARALLEL_OFF;` (planner.c)
    static DEBUG_PARALLEL_QUERY: core::cell::Cell<i32> =
        const { core::cell::Cell::new(DEBUG_PARALLEL_OFF) };
}

#[inline]
fn get_debug_parallel_query() -> i32 {
    DEBUG_PARALLEL_QUERY.with(core::cell::Cell::get)
}

#[inline]
fn set_debug_parallel_query(value: i32) {
    DEBUG_PARALLEL_QUERY.with(|c| c.set(value));
}

#[inline]
fn get_cursor_tuple_fraction() -> f64 {
    CURSOR_TUPLE_FRACTION.with(core::cell::Cell::get)
}

#[inline]
fn set_cursor_tuple_fraction(value: f64) {
    CURSOR_TUPLE_FRACTION.with(|c| c.set(value));
}

#[inline]
fn get_enable_distinct_reordering() -> bool {
    ENABLE_DISTINCT_REORDERING.with(core::cell::Cell::get)
}

#[inline]
fn set_enable_distinct_reordering(value: bool) {
    ENABLE_DISTINCT_REORDERING.with(|c| c.set(value));
}

#[inline]
fn get_parallel_leader_participation() -> bool {
    PARALLEL_LEADER_PARTICIPATION.with(core::cell::Cell::get)
}

#[inline]
fn set_parallel_leader_participation(value: bool) {
    PARALLEL_LEADER_PARTICIPATION.with(|c| c.set(value));
}

// ===========================================================================
// planner() / standard_planner()  — installed as the `pg_plan_query` seam.
// ===========================================================================

/// `planner(parse, query_string, cursorOptions, boundParams)` (planner.c:286).
///
/// With no `planner_hook`, this is just `standard_planner`. The
/// `pgstat_report_plan_id(result->planId, false)` call is skipped (pgstat plan-id
/// reporting is not threaded through this value-based entry; the field is not on
/// the owned `PlannedStmt` model).
///
/// Installed as the `pg_plan_query` inward seam (the COPY-TO driver and tcop
/// call it). `boundParams` is not part of the seam signature (COPY passes none);
/// the planner runs with no bound params.
fn pg_plan_query_impl<'mcx>(
    mcx: Mcx<'mcx>,
    querytree: &Query<'mcx>,
    query_string: &str,
    cursor_options: i32,
) -> PgResult<PlannedStmt<'mcx>> {
    // planner.c: `result = planner_hook ? planner_hook(...) : standard_planner(...)`.
    // With no hook registered this is exactly `standard_planner`.
    if planner_seams::planner_hook_present() {
        planner_seams::call_planner_hook(
            mcx,
            querytree,
            query_string,
            cursor_options,
            None,
        )
    } else {
        standard_planner(mcx, querytree, query_string, cursor_options, None)
    }
}

/// `planner(parse, query_string, cursorOptions, boundParams)` (planner.c:286)
/// with the bound external-parameter values threaded in. Installed as the
/// `pg_plan_query_params` seam (the tcop value path consumes it for cached /
/// custom plans).
fn pg_plan_query_params_impl<'mcx>(
    mcx: Mcx<'mcx>,
    querytree: &Query<'mcx>,
    query_string: &str,
    cursor_options: i32,
    bound_params: ::nodes::params::ParamListInfo,
) -> PgResult<PlannedStmt<'mcx>> {
    // planner.c: `result = planner_hook ? planner_hook(...) : standard_planner(...)`.
    if planner_seams::planner_hook_present() {
        planner_seams::call_planner_hook(
            mcx,
            querytree,
            query_string,
            cursor_options,
            bound_params,
        )
    } else {
        standard_planner(mcx, querytree, query_string, cursor_options, bound_params)
    }
}

/// `standard_planner(parse, query_string, cursorOptions, boundParams)`
/// (planner.c:302).
pub fn standard_planner<'mcx>(
    mcx: Mcx<'mcx>,
    parse: &Query<'mcx>,
    _query_string: &str,
    cursor_options: i32,
    bound_params: ::nodes::params::ParamListInfo,
) -> PgResult<PlannedStmt<'mcx>> {
    // glob = makeNode(PlannerGlobal); + field init (C:322-345). All other
    // fields are the C zero-defaults via Default.
    let mut glob = PlannerGlobal::default();

    // glob->boundParams = boundParams (planner.c:347) — the const-folder reads
    // this through `root->glob->boundParams` to substitute PARAM_EXTERN values.
    glob.bound_params = bound_params;

    // Assess parallel-mode feasibility (C:368-384).
    //
    // C:
    //   if ((cursorOptions & CURSOR_OPT_PARALLEL_OK) != 0 &&
    //       IsUnderPostmaster &&
    //       parse->commandType == CMD_SELECT &&
    //       !parse->hasModifyingCTE &&
    //       max_parallel_workers_per_gather > 0 &&
    //       !IsParallelWorker())
    //   {
    //       glob->maxParallelHazard = max_parallel_hazard(parse);
    //       glob->parallelModeOK = (glob->maxParallelHazard != PROPARALLEL_UNSAFE);
    //   }
    //   else { glob->maxParallelHazard = PROPARALLEL_UNSAFE; glob->parallelModeOK = false; }
    //
    // `IsParallelWorker()` is owned by `backend-access-transam-parallel`; the
    // planner reads it through the planner seam (a non-worker / `--single`
    // backend reports `false`, the seam's unset default, matching C's leader
    // backend). `max_parallel_hazard(parse)` is the now-ported top-level
    // whole-query parallel-hazard scan.
    if (cursor_options & CURSOR_OPT_PARALLEL_OK) != 0
        && init_small::globals::IsUnderPostmaster()
        && parse.commandType == CmdType::CMD_SELECT
        && !parse.hasModifyingCTE
        && costsize::max_parallel_workers_per_gather() > 0
        && !planner_seams::is_parallel_worker::call()
    {
        // All the cheap tests pass, so scan the query tree.
        glob.max_parallel_hazard =
            clauses::max_parallel_hazard(parse)? as i8;
        glob.parallel_mode_ok = glob.max_parallel_hazard != PROPARALLEL_UNSAFE;
    } else {
        // Skip the query tree scan, just assume it's unsafe.
        glob.max_parallel_hazard = PROPARALLEL_UNSAFE;
        glob.parallel_mode_ok = false;
    }

    // glob->parallelModeNeeded (C:403-404).
    glob.parallel_mode_needed =
        glob.parallel_mode_ok && (debug_parallel_query() != DEBUG_PARALLEL_OFF);

    // Determine tuple_fraction from cursor options (C:407-432).
    let mut tuple_fraction: f64;
    if (cursor_options & CURSOR_OPT_FAST_PLAN) != 0 {
        tuple_fraction = cursor_tuple_fraction();
        if tuple_fraction >= 1.0 {
            tuple_fraction = 0.0;
        } else if tuple_fraction <= 0.0 {
            tuple_fraction = 1e-10;
        }
    } else {
        tuple_fraction = 0.0;
    }

    // The planner run owns every interned Query/RTE/SubPlan/PlanRowMark.
    let mut run = PlannerRun::new(mcx);

    // Intern the (cloned) top Query — the seam hands us a borrow; the run needs
    // ownership (C scribbles on its Query input, so a private copy is correct).
    let top_query = parse.clone_in(mcx)?;
    let top_query_id = run.intern(top_query);

    // primary planning entry point (may recurse for subqueries) (C:435).
    let mut root =
        subquery_planner(mcx, &mut run, glob, top_query_id, None, false, tuple_fraction, None)?;

    // Select best Path and turn it into a Plan (C:438-441).
    let final_rel = relnode::fetch_upper_rel(
        &mut root,
        UPPERREL_FINAL,
        &None,
    );
    let best_path = allpaths::seams::get_cheapest_fractional_path::call(
        &root,
        final_rel,
        tuple_fraction,
    );

    let mut top_plan = createplan::create_plan(mcx, &mut root, &run, best_path)?;

    // Scrollable-cursor backwards-scan guard (C:447-451):
    //   if (cursorOptions & CURSOR_OPT_SCROLL)
    //   {
    //       if (!ExecSupportsBackwardScan(top_plan))
    //           top_plan = materialize_finished_plan(top_plan);
    //   }
    // `ExecSupportsBackwardScan` recursively walks the finished `Plan` tree
    // (execAmi.c); when the top node cannot scan backward we wrap it in a
    // `Material` node so the executor can buffer the result and rewind it.
    if (cursor_options & CURSOR_OPT_SCROLL) != 0
        && !init_subselect_ext_seams::exec_supports_backward_scan::call(
            Some(&top_plan),
        )?
    {
        top_plan =
            init_subselect_ext_seams::materialize_finished_plan::call(
                mcx,
                &mut root,
                top_plan,
            )?;
    }

    // Optionally add a Gather node for testing parallel-query infrastructure
    // (C:465-518). This is the `debug_parallel_query` (force-parallel) leg:
    // when the GUC is `on`/`regress`, the top plan is parallel-safe, and (for
    // `regress`) it has no initPlans, wrap it in a single-worker single-copy
    // Gather to exercise the parallel-execution machinery deterministically.
    //
    // `top_plan` is parallel-safe only when `glob->parallelModeOK` was set true
    // above (which requires the cheap-test gate to pass and
    // `max_parallel_hazard(parse) != PROPARALLEL_UNSAFE`); otherwise this branch
    // is skipped exactly as in C.
    {
        let dpq = debug_parallel_query();
        // Read the top plan's base (parallel_safe / initPlan presence).
        let (top_parallel_safe, top_has_initplan) = {
            let base = top_plan.plan_head();
            (
                base.parallel_safe,
                base.initPlan.as_ref().map(|l| !l.is_empty()).unwrap_or(false),
            )
        };

        if dpq != DEBUG_PARALLEL_OFF
            && top_parallel_safe
            && (!top_has_initplan || dpq != DEBUG_PARALLEL_REGRESS)
        {
            use ::nodes::nodegather::Gather as GatherNode;

            // Read the fields the Gather copies from the subplan, and move its
            // initPlan list out (C transfers it to the Gather: `gather->plan.initPlan
            // = top_plan->initPlan; top_plan->initPlan = NIL;`).
            //
            // C aliases the subplan's targetlist pointer onto the Gather
            // (`gather->plan.targetlist = top_plan->targetlist`); the subplan
            // keeps the same list. In the owned model we deep-copy it (via
            // `TargetEntry::clone_in`) so the Gather owns its own copy while the
            // subplan retains its list intact.
            let sub_targetlist: Option<mcx::PgVec<'mcx, TargetEntry<'mcx>>> = {
                let base = top_plan.plan_head();
                match base.targetlist.as_ref() {
                    Some(tl) => {
                        let mut out: mcx::PgVec<'mcx, TargetEntry<'mcx>> =
                            mcx::PgVec::new_in(mcx);
                        for te in tl.iter() {
                            out.push(te.clone_in(mcx)?);
                        }
                        Some(out)
                    }
                    None => None,
                }
            };
            let (sub_startup_cost, sub_total_cost, sub_plan_rows, sub_plan_width, moved_initplan) = {
                let base = top_plan.plan_head_mut();
                (
                    base.startup_cost,
                    base.total_cost,
                    base.plan_rows,
                    base.plan_width,
                    base.initPlan.take(),
                )
            };

            let setup_cost = costsize::parallel_setup_cost();
            let tuple_cost = costsize::parallel_tuple_cost();

            // SS_compute_initplan_cost(gather->plan.initPlan, ...): the moved
            // initplans' cost is deleted from top_plan (it was double-counted —
            // already included in the Gather's startup/total via the copy below).
            // For the simple no-initplan case this is exactly 0.
            let mut initplan_cost = 0.0_f64;
            if let Some(list) = moved_initplan.as_ref() {
                for sp in list.iter() {
                    initplan_cost += sp.startup_cost + sp.per_call_cost;
                }
            }

            // Subtract the initplans' cost from top_plan now (before we move it
            // under the Gather). C:511-512.
            {
                let base = top_plan.plan_head_mut();
                base.startup_cost = sub_startup_cost - initplan_cost;
                base.total_cost = sub_total_cost - initplan_cost;
            }

            let mut gather = GatherNode::default();
            {
                let gp = &mut gather.plan;
                // gather->plan.targetlist = top_plan->targetlist; qual = NIL.
                gp.targetlist = sub_targetlist;
                gp.qual = None;
                // gather->plan.lefttree = top_plan; righttree = NULL.
                gp.lefttree = Some(mcx::alloc_in(mcx, top_plan)?);
                gp.righttree = None;
                // gather->plan.initPlan = top_plan->initPlan (transferred).
                gp.initPlan = moved_initplan;
                // Costs (C:506-515): include parallel_setup_cost / parallel_tuple_cost;
                // the sub costs already include the initplan cost, so they are NOT
                // re-subtracted on the Gather (the above coding included it here).
                gp.startup_cost = sub_startup_cost + setup_cost;
                gp.total_cost = sub_total_cost + setup_cost + tuple_cost * sub_plan_rows;
                gp.plan_rows = sub_plan_rows;
                gp.plan_width = sub_plan_width;
                gp.parallel_aware = false;
                gp.parallel_safe = false;
            }
            gather.num_workers = 1;
            gather.single_copy = true;
            gather.invisible = dpq == DEBUG_PARALLEL_REGRESS;
            // Since this Gather has no parallel-aware descendants to signal to,
            // we don't need a rescan Param.
            gather.rescan_param = -1;
            gather.initParam = None;

            // Use parallel mode for parallel plans (C:516).
            if let Some(g) = root.glob.as_mut() {
                g.parallel_mode_needed = true;
            }

            top_plan = Node::mk_gather(mcx, gather)?;
        }
    }

    // SS_finalize_plan over subplans + top plan (C:526-537).
    //
    // C: `if (glob->paramExecTypes != NIL) { forboth(subplans, subroots)
    //        SS_finalize_plan(subroot, subplan); SS_finalize_plan(root, top_plan); }`
    //
    // The `forboth` over `glob->subplans`/`subroots` finalizes each child
    // subplan *before* the main plan. Each subroot is moved out of the run store
    // and lent the shared `glob` for the duration of its `SS_finalize_plan`
    // (mirroring the set_plan_references loop), so the recursion's
    // `planner_subplan_get_plan(subroot, …)` over a NESTED subplan resolves
    // against the one shared `glob->subplans` list.
    //
    // For the no-subplan case (the simple INSERT/SELECT and the type tests),
    // `glob->subplans` is empty so the `forboth` is a pure no-op, and the only
    // work is `SS_finalize_plan(root, top_plan)`: a plain recursive walk of the
    // top plan tree that sets each node's extParam/allParam from its direct Param
    // references (the owner's `finalize.rs` reaches the empty subplan store
    // harmlessly — `planner_subplan_get_plan` is only called for initPlan/SubPlan
    // children, of which there are none here). This is the bounded path.
    if !root.glob.as_ref().map(|g| g.param_exec_types.is_empty()).unwrap_or(true) {
        // forboth(lp, glob->subplans, lr, glob->subroots): SS_finalize_plan on
        // each subplan before the top plan (C:528-535). Each subplan node and
        // its subroot both live in the run store; to finalize a subplan we move
        // its Plan node out of the store (take_subplan), move its subroot out
        // (take_subroot) and lend it the shared `glob` for the duration of the
        // call, then put both back. The glob lend is mandatory: C's subroot
        // shares the one `glob` pointer, and `SS_finalize_plan` recurses into
        // `planner_subplan_get_plan(subroot, child_plan_id)` to read a NESTED
        // subplan's `glob->subplans[child_plan_id-1]` (e.g. an `ARRAY(SELECT …
        // (SELECT …))` — a SubPlan inside a SubPlan). Without the lend the
        // subroot's `glob` is None and that nested-subplan finalize errors with
        // `root->glob is NULL`. Mirrors the set_plan_references loop below.
        let subplan_ids: Vec<pathnodes::PlanId> = root
            .glob
            .as_ref()
            .map(|g| g.subplans.clone())
            .unwrap_or_default();
        for pid in subplan_ids.iter().copied() {
            let mut subroot = run.take_subroot(pid);
            subroot.glob = root.glob.take();
            let mut subplan_node = run.take_subplan(pid)?;
            init_subselect::finalize::SS_finalize_plan(
                mcx,
                &subroot,
                &run,
                &mut subplan_node,
            )?;
            run.put_subplan(pid, subplan_node);
            // Move the (possibly accumulated) shared glob back to the parent.
            root.glob = subroot.glob.take();
            run.put_subroot(pid, subroot);
        }

        // SS_finalize_plan(root, top_plan) (C:536). Walk the top plan tree to
        // populate extParam/allParam.
        init_subselect::finalize::SS_finalize_plan(
            mcx,
            &root,
            &run,
            &mut top_plan,
        )?;
    }

    // set_plan_references on the top plan (C:540-545).
    top_plan = setrefs::set_plan_references(mcx, &mut run, &mut root, top_plan)?;

    // ... and the subplans, both regular subplans and initplans (C:547-554):
    // forboth(subplans, subroots) lfirst(lp) = set_plan_references(subroot,
    // subplan). Each subplan node and its subroot live in the run store, and
    // set_plan_references needs `&mut subroot` plus `&mut run`. The C subroot
    // shares the parent root's `glob` by pointer; here glob lives on the parent
    // root, so we move it into the subroot for the duration of its setrefs call
    // (so the accumulated glob->finalrtable / relationOids / ... thread through
    // every call as one shared glob) and move it back afterwards. The subplan
    // node is taken out of the store, rewritten, and the result stored back.
    {
        let subplan_ids: Vec<pathnodes::PlanId> = root
            .glob
            .as_ref()
            .map(|g| g.subplans.clone())
            .unwrap_or_default();
        for pid in subplan_ids {
            // Move subroot out of the run, give it the shared glob.
            let mut subroot = run.take_subroot(pid);
            subroot.glob = root.glob.take();
            // Move the subplan node out, run setrefs, store the result back.
            let subplan_node = run.take_subplan(pid)?;
            let new_node = setrefs::set_plan_references(
                mcx,
                &mut run,
                &mut subroot,
                subplan_node,
            )?;
            run.put_subplan(pid, new_node);
            // Move the (accumulated) shared glob back to the parent root.
            root.glob = subroot.glob.take();
            run.put_subroot(pid, subroot);
        }
    }

    // Build the PlannedStmt result (C:557-583).
    let command_type: CmdType = parse.commandType;

    // rtable: resolve glob->finalrtable (Vec<RangeTblEntryId>) to owned RTEs.
    let final_rtable: Vec<RangeTblEntryId> = root
        .glob
        .as_ref()
        .map(|g| g.finalrtable.clone())
        .unwrap_or_default();
    let mut rtable: mcx::PgVec<'mcx, RangeTblEntry<'mcx>> = mcx::PgVec::new_in(mcx);
    for id in &final_rtable {
        rtable.push(run.resolve_rte(*id).clone_in(mcx)?);
    }

    // permInfos: resolve glob->finalrteperminfos (Vec<RtePermInfoId>) to owned
    // RTEPermissionInfos (C `result->permInfos = glob->finalrteperminfos`).
    let final_perminfos: Vec<pathnodes::RtePermInfoId> = root
        .glob
        .as_ref()
        .map(|g| g.finalrteperminfos.clone())
        .unwrap_or_default();
    let mut perm_infos: mcx::PgVec<
        'mcx,
        ::nodes::parsenodes::RTEPermissionInfo<'mcx>,
    > = mcx::PgVec::new_in(mcx);
    for id in &final_perminfos {
        perm_infos.push(run.resolve_rte_perminfo(*id).clone_in(mcx)?);
    }

    // subplans: resolve glob->subplans (Vec<PlanId>) to owned Node trees.
    let subplan_ids: Vec<pathnodes::PlanId> = root
        .glob
        .as_ref()
        .map(|g| g.subplans.clone())
        .unwrap_or_default();
    let mut subplans: mcx::PgVec<'mcx, Option<mcx::PgBox<'mcx, Node<'mcx>>>> =
        mcx::PgVec::new_in(mcx);
    for pid in &subplan_ids {
        // Move the owned plan tree out of the run store into a PgBox for the
        // stmt, leaving a default Result placeholder behind (the run store is
        // not read again after assembly). On the simple SELECT path
        // glob->subplans is empty, so this loop never runs.
        let node = core::mem::replace(
            run.resolve_subplan_mut(*pid),
            Node::mk_result(mcx, ::nodes::noderesult::Result::default())?,
        );
        subplans.push(Some(mcx::alloc_in(mcx, node)?));
    }

    let has_returning = !parse.returningList.is_empty();

    // jitFlags = PGJIT_NONE. The C jit-cost branch (C:587-609) consults the jit_*
    // GUCs (jit_enabled / jit_above_cost / ...), none of which are threaded into
    // this crate. We faithfully keep jitFlags = PGJIT_NONE (the result when
    // jit_enabled is false / cost below threshold), preserving the structure
    // without inventing GUC values.
    let jit_flags = PGJIT_NONE;

    // rowMarks: glob->finalrowmarks holds PlanRowMarkId handles into the
    // PlannerRun rowmark store (set_plan_references flat-copied each
    // root->rowMarks PlanRowMark here, rti/prti already rtoffset-adjusted). The
    // PlannedStmt carries the resolved owned PlanRowMark values (the scalar
    // struct is Copy); InitPlan reads them to build es_rowmarks. On the simple
    // SELECT path finalrowmarks is empty → None (the C NIL).
    let row_marks: Option<mcx::PgVec<'mcx, ::nodes::nodelockrows::PlanRowMark>> = {
        let final_ids: Vec<pathnodes::PlanRowMarkId> = root
            .glob
            .as_ref()
            .map(|g| g.finalrowmarks.clone())
            .unwrap_or_default();
        if final_ids.is_empty() {
            None
        } else {
            let mut out = mcx::vec_with_capacity_in(mcx, final_ids.len())?;
            for id in &final_ids {
                out.push(*run.resolve_rowmark(*id));
            }
            Some(out)
        }
    };

    // unprunableRelids = bms_difference(glob->allRelids, glob->prunableRelids)
    // (planner.c: standard_planner, after set_plan_references populated
    // glob->allRelids with every finalrtable index and glob->prunableRelids
    // with the partition-pruning-only indexes). Both are the lifetime-free
    // `Relids` on glob; the stmt field is `PgBox<Bitmapset<'mcx>>`. We compute
    // the word-level difference and materialize it into `mcx` (an empty result
    // is the C `NULL`/None). On the simple SELECT path prunableRelids is empty,
    // so this is bms_copy(allRelids) = {1..=len(finalrtable)}, which the
    // executor's ExecGetRangeTableRelation requires to open scan relations.
    let unprunable_relids = {
        let (all_words, prunable_words): (Vec<u64>, Vec<u64>) = root
            .glob
            .as_ref()
            .map(|g| {
                (
                    g.all_relids.as_ref().map(|b| b.words.clone()).unwrap_or_default(),
                    g.prunable_relids.as_ref().map(|b| b.words.clone()).unwrap_or_default(),
                )
            })
            .unwrap_or_default();
        // bms_difference word-wise: result[i] = all[i] & ~prunable[i].
        let mut diff: Vec<u64> = Vec::with_capacity(all_words.len());
        for (i, &aw) in all_words.iter().enumerate() {
            let pw = prunable_words.get(i).copied().unwrap_or(0);
            diff.push(aw & !pw);
        }
        // Trim trailing zero words (bms canonical form: nwords has no trailing
        // empty word), then None for the empty set.
        while diff.last() == Some(&0) {
            diff.pop();
        }
        if diff.is_empty() {
            None
        } else {
            let mut words: mcx::PgVec<'mcx, u64> = mcx::PgVec::new_in(mcx);
            for w in diff {
                words.push(w);
            }
            Some(mcx::alloc_in(
                mcx,
                ::nodes::bitmapset::Bitmapset { words },
            )?)
        }
    };

    let result_relations: Option<mcx::PgVec<'mcx, i32>> = {
        let v = root.glob.as_ref().map(|g| g.result_relations.clone()).unwrap_or_default();
        if v.is_empty() {
            None
        } else {
            let mut pv = mcx::PgVec::new_in(mcx);
            for x in v {
                pv.push(x);
            }
            Some(pv)
        }
    };

    let relation_oids: Option<mcx::PgVec<'mcx, Oid>> = {
        let v = root.glob.as_ref().map(|g| g.relation_oids.clone()).unwrap_or_default();
        if v.is_empty() {
            None
        } else {
            let mut pv = mcx::PgVec::new_in(mcx);
            for x in v {
                pv.push(x);
            }
            Some(pv)
        }
    };

    let param_exec_types: Option<mcx::PgVec<'mcx, Oid>> = {
        let v = root.glob.as_ref().map(|g| g.param_exec_types.clone()).unwrap_or_default();
        if v.is_empty() {
            None
        } else {
            let mut pv = mcx::PgVec::new_in(mcx);
            for x in v {
                pv.push(x);
            }
            Some(pv)
        }
    };

    let utility_stmt = match &parse.utilityStmt {
        Some(u) => Some(mcx::alloc_in(mcx, u.clone_in(mcx)?)?),
        None => None,
    };

    let glob_transient = root.glob.as_ref().map(|g| g.transient_plan).unwrap_or(false);
    let glob_depends_on_role = root.glob.as_ref().map(|g| g.depends_on_role).unwrap_or(false);
    let glob_parallel_mode_needed =
        root.glob.as_ref().map(|g| g.parallel_mode_needed).unwrap_or(false);

    // result->invalItems = glob->invalItems; (planner.c:579). The glob list
    // carries concrete `PlanInvalItem` pairs (recorded via the record_inval_item
    // seam during set_plan_references).
    let inval_items: Option<mcx::PgVec<'mcx, ::nodes::nodeindexscan::PlanInvalItem>> = {
        let v = root.glob.as_ref().map(|g| g.inval_items.clone()).unwrap_or_default();
        if v.is_empty() {
            None
        } else {
            let mut pv = mcx::PgVec::new_in(mcx);
            for x in v {
                pv.push(x);
            }
            Some(pv)
        }
    };

    // result->partPruneInfos = glob->partPruneInfos; (planner.c:580). The glob
    // list carries the `PartitionPruneInfo` carriers registered by
    // `register_partpruneinfo` during set_plan_references.
    // The glob carriers are arena-interned at the notional `'static`; deep-clone
    // each into the PlannedStmt's `mcx` (the carrier is invariant over its
    // lifetime, so a `clone_in` re-intern is required, not a coercion).
    let part_prune_infos: alloc::vec::Vec<
        ::nodes::partprune_carrier::PartitionPruneInfo<'mcx>,
    > = match root.glob.as_ref() {
        Some(g) => {
            let mut v = alloc::vec::Vec::with_capacity(g.part_prune_infos.len());
            for pinfo in g.part_prune_infos.iter() {
                v.push(pinfo.clone_in(mcx)?);
            }
            v
        }
        None => alloc::vec::Vec::new(),
    };

    // result->appendRelations = glob->appendRelations; (planner.c:574). The
    // glob list carries the flattened `AppendRelInfo` carriers accumulated by
    // set_plan_references; the deparser indexes them by child relid.
    let append_relations: alloc::vec::Vec<
        ::nodes::appendrel_carrier::AppendRelInfoCarrier,
    > = root.glob.as_ref().map(|g| g.append_relations.clone()).unwrap_or_default();

    // if (glob->partition_directory != NULL)
    //     DestroyPartitionDirectory(glob->partition_directory); (planner.c:611)
    //
    // The PartitionDirectory pins one relcache entry per partitioned table it
    // looked up (RelationIncrementReferenceCount) for the plan's lifetime; tear
    // it down now so those pins are released (else the resource owner warns
    // "resource was not closed: relation with OID ..." at transaction end).
    if let Some(glob) = root.glob.as_mut() {
        if let Some(dir) = glob.partition_directory.0.take() {
            partitioning_core_seams::destroy_partition_directory::call(dir);
        }
    }

    let result = PlannedStmt {
        commandType: command_type,
        // result->queryId = parse->queryId; (planner.c:578)
        queryId: parse.queryId,
        utilityStmt: utility_stmt,
        resultRelations: result_relations,
        relationOids: relation_oids,
        planTree: Some(mcx::alloc_in(mcx, top_plan)?),
        rowMarks: row_marks,
        canSetTag: parse.canSetTag,
        hasReturning: has_returning,
        hasModifyingCTE: parse.hasModifyingCTE,
        parallelModeNeeded: glob_parallel_mode_needed,
        jitFlags: jit_flags,
        permInfos: if perm_infos.is_empty() { None } else { Some(perm_infos) },
        paramExecTypes: param_exec_types,
        rtable: if rtable.is_empty() { None } else { Some(rtable) },
        unprunableRelids: unprunable_relids,
        subplans: if subplans.is_empty() { None } else { Some(subplans) },
        // result->stmt_location = parse->stmt_location;
        // result->stmt_len = parse->stmt_len; (planner.c:601-602)
        stmt_location: parse.stmt_location,
        stmt_len: parse.stmt_len,
        // result->transientPlan = glob->transientPlan;
        // result->dependsOnRole = glob->dependsOnRole; (planner.c:564-565)
        transientPlan: glob_transient,
        dependsOnRole: glob_depends_on_role,
        invalItems: inval_items,
        partPruneInfos: part_prune_infos,
        appendRelations: append_relations,
    };

    Ok(result)
}

// ===========================================================================
// subquery_planner()  (planner.c:650)
// ===========================================================================

/// Seam wrapper installed as
/// `planner_seams::subquery_planner_for_setop`, letting
/// `prepunion.c`'s `recurse_set_operations` plan one leaf subquery of a set-op
/// tree. Delegates to the private [`subquery_planner`]; `parent_root` and the
/// `setops` parentOp are passed as `None` (the cheapest-path leg of
/// `build_setop_child_paths`, which does not request per-child sorted paths).
fn subquery_planner_for_setop_impl<'mcx>(
    mcx: Mcx<'mcx>,
    run: &mut PlannerRun<'mcx>,
    glob: PlannerGlobal,
    subquery_id: pathnodes::QueryId,
    parent_root: PlannerInfo,
    recursion_carry: Option<(i32, f64)>,
    has_recursion: bool,
    tuple_fraction: f64,
    setop_op: Option<&'mcx ::nodes::rawnodes::SetOperationStmt<'mcx>>,
) -> PgResult<PlannerInfo> {
    subquery_planner_carried(
        mcx,
        run,
        glob,
        subquery_id,
        Some(parent_root),
        recursion_carry,
        has_recursion,
        tuple_fraction,
        setop_op,
    )
}

/// Seam wrapper installed as
/// `planner_seams::subquery_planner_for_fromsubquery`,
/// letting `allpaths.c`'s `set_subquery_pathlist` plan a plain `RTE_SUBQUERY`
/// in the FROM clause into its own subroot (C:683). Mirrors the
/// `subquery_planner_for_setop` glob-threading contract: the shared
/// [`PlannerGlobal`] is moved in (the caller took it off the outer root) and
/// carried back out inside `subroot.glob`. `parent_query_level` is the outer
/// root's level, so the subroot's level is `parent + 1` (and `plan_params`
/// upper references land on the parent, which the caller reads off
/// `subroot.plan_params`). `has_recursion` is `false` and `setops` is `None`
/// for a plain FROM-subquery.
fn subquery_planner_for_fromsubquery_impl<'mcx>(
    mcx: Mcx<'mcx>,
    run: &mut PlannerRun<'mcx>,
    glob: PlannerGlobal,
    subquery_id: pathnodes::QueryId,
    parent_root: PlannerInfo,
    tuple_fraction: f64,
) -> PgResult<PlannerInfo> {
    subquery_planner_carried(
        mcx,
        run,
        glob,
        subquery_id,
        Some(parent_root),
        None,
        false,
        tuple_fraction,
        None,
    )
}

/// `subquery_planner(glob, parse, parent_root, hasRecursion, tuple_fraction, setops)`
/// (planner.c:650). Returns the owned `PlannerInfo` ("root") with its glob
/// attached and `final_rel`'s cheapest path set.
#[allow(clippy::too_many_arguments)]
fn subquery_planner<'mcx>(
    mcx: Mcx<'mcx>,
    run: &mut PlannerRun<'mcx>,
    glob: PlannerGlobal,
    parse_id: pathnodes::QueryId,
    parent_root: Option<PlannerInfo>,
    has_recursion: bool,
    tuple_fraction: f64,
    setops: Option<&'mcx ::nodes::rawnodes::SetOperationStmt<'mcx>>,
) -> PgResult<PlannerInfo> {
    subquery_planner_carried(
        mcx, run, glob, parse_id, parent_root, None, has_recursion,
        tuple_fraction, setops,
    )
}

// ===========================================================================
// preprocess_query_expressions cold-clause helpers.
//
// FRAME-BLOAT: `preprocess_query_expressions` (a nested fn in
// `subquery_planner_carried`) holds a large union of branch-local temporaries
// (owned `Expr`s, cloned `Query` nodes, per-clause `PgBox`es) across ~15
// sequential per-clause blocks. In an unoptimized (dev) build the compiler
// reserves stack for every local in the function at once, so a trivial query
// (e.g. `SELECT 1`, which only runs the `targetList` block) still pays for the
// whole union — the dominant per-statement stack cost. Each cold clause block
// is hoisted here behind its own `#[inline(never)]` boundary so its locals get
// a separate frame allocated only when that clause is actually present.
// Behavior-preserving: statements are moved verbatim, `?` early-exits now
// return from the helper and propagate unchanged. Mirrors C, where these are
// sequential statements but the equivalent expression walks live in callees.
// ===========================================================================

/// withCheckOptions preprocessing (planner.c:907-916). INSERT/UPDATE on an
/// updatable view with WITH CHECK OPTION only.
#[inline(never)]
fn pqe_with_check_options<'mcx>(
    mcx: Mcx<'mcx>,
    run: &mut PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    outer_query_ref: Option<&Node<'mcx>>,
) -> PgResult<()> {
    let n_wco = run.resolve(root.parse).withCheckOptions.len();
    for i in 0..n_wco {
        // wco->qual = preprocess_expression(root, wco->qual, EXPRKIND_QUAL).
        // Take the qual NodePtr out of the WCO node, convert to an owned
        // Expr, preprocess, and write the result back.
        let qual_expr: Option<Expr> = {
            let wco_node = run.resolve_mut(root.parse).withCheckOptions[i].as_mut();
            let wco = wco_node.as_withcheckoption_mut().expect(
                "subquery_planner: withCheckOptions element is not a WithCheckOption node",
            );
            match wco.qual.take() {
                None => None,
                Some(q) => match mcx::PgBox::into_inner(q).into_expr() {
                    Some(e) => Some(e),
                    None => {
                        return Err(PgError::error(
                            "subquery_planner: WithCheckOption qual is not an \
                             expression node",
                        ));
                    }
                },
            }
        };

        let processed =
            preprocess_expression(mcx, &mut *root, run, outer_query_ref, qual_expr, EXPRKIND_QUAL)?;

        let wco_node = run.resolve_mut(root.parse).withCheckOptions[i].as_mut();
        let wco = wco_node.as_withcheckoption_mut().expect(
            "subquery_planner: withCheckOptions element is not a WithCheckOption node",
        );
        wco.qual = match processed {
            Some(pe) => Some(mcx::alloc_in(mcx, Node::mk_expr(mcx, pe)?)?),
            None => None,
        };
    }

    // newWithCheckOptions = keep only the WCOs whose qual survived.
    let mut kept: mcx::PgVec<'mcx, ::nodes::nodes::NodePtr<'mcx>> =
        mcx::PgVec::new_in(mcx);
    for wco_ptr in run.resolve_mut(root.parse).withCheckOptions.drain(..) {
        let keep = wco_ptr
            .as_withcheckoption()
            .map(|w| w.qual.is_some())
            .unwrap_or(false);
        if keep {
            kept.push(wco_ptr);
        }
    }
    run.resolve_mut(root.parse).withCheckOptions = kept;
    Ok(())
}

/// returningList preprocessing (planner.c:918-920). INSERT/UPDATE/DELETE
/// ... RETURNING only.
#[inline(never)]
fn pqe_returning_list<'mcx>(
    mcx: Mcx<'mcx>,
    run: &mut PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    outer_query_ref: Option<&Node<'mcx>>,
) -> PgResult<()> {
    let n = run.resolve(root.parse).returningList.len();
    for i in 0..n {
        let e = run.resolve_mut(root.parse).returningList[i].expr.take();
        let e = match e {
            Some(b) => Some(mcx::PgBox::into_inner(b)),
            None => None,
        };
        let processed = preprocess_expression(mcx, &mut *root, run, outer_query_ref, e, EXPRKIND_TARGET)?;
        run.resolve_mut(root.parse).returningList[i].expr = match processed {
            Some(pe) => Some(mcx::alloc_in(mcx, pe)?),
            None => None,
        };
    }
    Ok(())
}

/// windowClause start/end offset preprocessing (planner.c:927-936). Windowed
/// queries with frame offsets only.
#[inline(never)]
fn pqe_window_clause<'mcx>(
    mcx: Mcx<'mcx>,
    run: &mut PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    outer_query_ref: Option<&Node<'mcx>>,
) -> PgResult<()> {
    let n_wc = run.resolve(root.parse).windowClause.len();
    for i in 0..n_wc {
        // wc->startOffset = preprocess_expression(startOffset, EXPRKIND_LIMIT).
        let start = extract_windowclause_offset(mcx, run, &root, i, true)?;
        let processed =
            preprocess_expression(mcx, &mut *root, run, outer_query_ref, start, EXPRKIND_LIMIT)?;
        set_windowclause_offset(mcx, run, &root, i, true, processed)?;

        // wc->endOffset = preprocess_expression(endOffset, EXPRKIND_LIMIT).
        let end = extract_windowclause_offset(mcx, run, &root, i, false)?;
        let processed =
            preprocess_expression(mcx, &mut *root, run, outer_query_ref, end, EXPRKIND_LIMIT)?;
        set_windowclause_offset(mcx, run, &root, i, false, processed)?;
    }
    Ok(())
}

/// limitOffset / limitCount preprocessing (planner.c:938-941). LIMIT/OFFSET
/// queries only.
#[inline(never)]
fn pqe_limit<'mcx>(
    mcx: Mcx<'mcx>,
    run: &mut PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    outer_query_ref: Option<&Node<'mcx>>,
) -> PgResult<()> {
    {
        let lo = run.resolve_mut(root.parse).limitOffset.take();
        let lo = lo.map(mcx::PgBox::into_inner);
        let processed = preprocess_expression(mcx, &mut *root, run, outer_query_ref, lo, EXPRKIND_LIMIT)?;
        run.resolve_mut(root.parse).limitOffset = match processed {
            Some(pe) => Some(mcx::alloc_in(mcx, pe)?),
            None => None,
        };
    }
    {
        let lc = run.resolve_mut(root.parse).limitCount.take();
        let lc = lc.map(mcx::PgBox::into_inner);
        let processed = preprocess_expression(mcx, &mut *root, run, outer_query_ref, lc, EXPRKIND_LIMIT)?;
        run.resolve_mut(root.parse).limitCount = match processed {
            Some(pe) => Some(mcx::alloc_in(mcx, pe)?),
            None => None,
        };
    }
    Ok(())
}

/// onConflict expression lists preprocessing (planner.c:943-963). INSERT ...
/// ON CONFLICT only.
#[inline(never)]
fn pqe_on_conflict<'mcx>(
    mcx: Mcx<'mcx>,
    run: &mut PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    outer_query_ref: Option<&Node<'mcx>>,
) -> PgResult<()> {
    // arbiterElems: each element is an InferenceElem Expr wrapped in a
    // Node::Expr (EXPRKIND_ARBITER_ELEM).
    let n = run
        .resolve(root.parse)
        .onConflict
        .as_deref()
        .map(|oc| oc.arbiterElems.len())
        .unwrap_or(0);
    for i in 0..n {
        let e = take_onconflict_list_expr(run, &root, OcList::ArbiterElems, i);
        let processed = preprocess_expression(
            mcx, &mut *root, run, outer_query_ref, e, EXPRKIND_ARBITER_ELEM,
        )?;
        set_onconflict_list_expr(mcx, run, &root, OcList::ArbiterElems, i, processed)?;
    }

    // arbiterWhere (EXPRKIND_QUAL).
    {
        let w = take_onconflict_scalar(run, &root, OcScalar::ArbiterWhere);
        let processed =
            preprocess_expression(mcx, &mut *root, run, outer_query_ref, w, EXPRKIND_QUAL)?;
        set_onconflict_scalar(mcx, run, &root, OcScalar::ArbiterWhere, processed)?;
    }

    // onConflictSet: each element is a TargetEntry; preprocess its expr
    // (EXPRKIND_TARGET).
    let n = run
        .resolve(root.parse)
        .onConflict
        .as_deref()
        .map(|oc| oc.onConflictSet.len())
        .unwrap_or(0);
    for i in 0..n {
        let e = take_onconflict_list_expr(run, &root, OcList::OnConflictSet, i);
        let processed = preprocess_expression(
            mcx, &mut *root, run, outer_query_ref, e, EXPRKIND_TARGET,
        )?;
        set_onconflict_list_expr(mcx, run, &root, OcList::OnConflictSet, i, processed)?;
    }

    // onConflictWhere (EXPRKIND_QUAL).
    {
        let w = take_onconflict_scalar(run, &root, OcScalar::OnConflictWhere);
        let processed =
            preprocess_expression(mcx, &mut *root, run, outer_query_ref, w, EXPRKIND_QUAL)?;
        set_onconflict_scalar(mcx, run, &root, OcScalar::OnConflictWhere, processed)?;
    }
    // exclRelTlist contains only Vars, so no preprocessing needed.
    Ok(())
}

/// mergeActionList preprocessing (planner.c:965-978). MERGE only.
#[inline(never)]
fn pqe_merge_action_list<'mcx>(
    mcx: Mcx<'mcx>,
    run: &mut PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    outer_query_ref: Option<&Node<'mcx>>,
) -> PgResult<()> {
    let n_actions = run.resolve(root.parse).mergeActionList.len();
    for ai in 0..n_actions {
        // action->targetList: preprocess each TargetEntry expr.
        let n_tle = {
            let action = run.resolve(root.parse).mergeActionList[ai]
                .as_mergeaction()
                .expect("mergeActionList entry is a MergeAction");
            action.targetList.len()
        };
        for ti in 0..n_tle {
            // Take the TargetEntry expr out of the action's node.
            let e = {
                let action = run.resolve_mut(root.parse).mergeActionList[ai]
                    .as_mergeaction_mut()
                    .unwrap();
                action.targetList[ti]
                    .as_targetentry_mut()
                    .and_then(|tle| tle.expr.take())
                    .map(mcx::PgBox::into_inner)
            };
            let processed = preprocess_expression(
                mcx, &mut *root, run, outer_query_ref, e, EXPRKIND_TARGET,
            )?;
            let processed = match processed {
                Some(pe) => Some(mcx::alloc_in(mcx, pe)?),
                None => None,
            };
            let action = run.resolve_mut(root.parse).mergeActionList[ai]
                .as_mergeaction_mut()
                .unwrap();
            if let Some(tle) = action.targetList[ti].as_targetentry_mut() {
                tle.expr = processed;
            }
        }

        // action->qual: preprocess the WHEN condition (EXPRKIND_QUAL).
        let q = {
            let action = run.resolve_mut(root.parse).mergeActionList[ai]
                .as_mergeaction_mut()
                .unwrap();
            action
                .qual
                .take()
                .map(mcx::PgBox::into_inner)
                .and_then(|node| node.into_expr())
        };
        let processed =
            preprocess_expression(mcx, &mut *root, run, outer_query_ref, q, EXPRKIND_QUAL)?;
        let action = run.resolve_mut(root.parse).mergeActionList[ai]
            .as_mergeaction_mut()
            .unwrap();
        action.qual = match processed {
            Some(pe) => Some(mcx::alloc_in(mcx, Node::mk_expr(mcx, pe)?)?),
            None => None,
        };
    }
    Ok(())
}

/// mergeJoinCondition preprocessing (planner.c:980-981). MERGE only.
#[inline(never)]
fn pqe_merge_join_condition<'mcx>(
    mcx: Mcx<'mcx>,
    run: &mut PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    outer_query_ref: Option<&Node<'mcx>>,
) -> PgResult<()> {
    let c = run.resolve_mut(root.parse).mergeJoinCondition.take();
    let c = c.map(mcx::PgBox::into_inner);
    let processed = preprocess_expression(mcx, &mut *root, run, outer_query_ref, c, EXPRKIND_QUAL)?;
    run.resolve_mut(root.parse).mergeJoinCondition = match processed {
        Some(pe) => Some(mcx::alloc_in(mcx, pe)?),
        None => None,
    };
    Ok(())
}

/// append_rel_list translated_vars preprocessing (planner.c:983-985).
/// Inheritance / UNION ALL flattening only.
#[inline(never)]
fn pqe_append_rel_list<'mcx>(
    mcx: Mcx<'mcx>,
    run: &mut PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    outer_query_ref: Option<&Node<'mcx>>,
) -> PgResult<()> {
    // Snapshot (appinfo index, var slot index, NodeId) for every
    // translated_var; a default-NodeId (0) slot is a dropped column
    // (C NULL) and is skipped.
    let mut work: Vec<(usize, usize, pathnodes::NodeId)> = Vec::new();
    for (ai, appinfo) in root.append_rel_list.iter().enumerate() {
        for (vi, id) in appinfo.translated_vars.iter().enumerate() {
            if *id == pathnodes::NodeId::default() {
                continue;
            }
            work.push((ai, vi, *id));
        }
    }
    for (_ai, _vi, id) in work {
        let live = root.node(id).clone_in(mcx)?;
        let processed = preprocess_expression(
            mcx,
            &mut *root,
            run,
            outer_query_ref,
            Some(live),
            EXPRKIND_APPINFO,
        )?;
        match processed {
            Some(e) => *root.node_mut(id) = e.erase_lifetime(),
            // EXPRKIND_APPINFO (not a QUAL) never reduces an expression
            // to NULL; preprocess_expression only returns None for a
            // None input or a canonicalize_qual collapse, neither of
            // which applies here. Keep the original node if it ever did.
            None => {}
        }
    }
    Ok(())
}

/// Per-RTE expression preprocessing (planner.c:987-1054): tablesample /
/// subquery join-alias flattening / function / tablefunc / values / groupexprs,
/// plus per-element securityQuals. A plain RTE_RELATION SELECT has none of these.
#[inline(never)]
fn pqe_per_rte<'mcx>(
    mcx: Mcx<'mcx>,
    run: &mut PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    outer_query_ref: Option<&Node<'mcx>>,
) -> PgResult<()> {
    use ::nodes::nodes::Node;
    use ::nodes::parsenodes::RTEKind;
    let n = run.resolve(root.parse).rtable.len();
    for i in 0..n {
        let rtekind = run.resolve(root.parse).rtable[i].rtekind;
        let lateral = run.resolve(root.parse).rtable[i].lateral;

        match rtekind {
            RTEKind::RTE_RELATION => {
                // planner.c:993-998: rte->tablesample =
                //   preprocess_expression(root, (Node *) rte->tablesample,
                //                         EXPRKIND_TABLESAMPLE).
                // C casts the whole TableSampleClause* to a Node and folds it.
                // In the owned model the clause is a `Node::TableSampleClause`
                // node pointed at by `rte->tablesample`; the only expression
                // subtrees it carries are its `args` list and its optional
                // `repeatable` Expr. EXPRKIND_TABLESAMPLE runs
                // eval_const_expressions (+ SS_process_sublinks /
                // SS_replace_correlation_vars), all per-expression and
                // recursive, so preprocessing each `args` element and the
                // `repeatable` Expr individually is equivalent to folding the
                // whole clause node once.
                if run.resolve(root.parse).rtable[i].tablesample.is_some() {
                    // Pull the args + repeatable Exprs out of the clause node.
                    let (args, repeatable): (Vec<Expr>, Option<Expr>) = {
                        let parse = run.resolve(root.parse);
                        let ts_node = parse.rtable[i]
                            .tablesample
                            .as_deref()
                            .expect("tablesample is_some");
                        match ts_node.as_tablesampleclause() {
                            Some(tsc) => {
                                let args: Vec<Expr> = match &tsc.args {
                                    Some(list) => list.iter().cloned().collect(),
                                    None => Vec::new(),
                                };
                                let repeatable =
                                    tsc.repeatable.as_deref().cloned();
                                (args, repeatable)
                            }
                            None => {
                                return Err(types_error::PgError::error(
                                    "subquery_planner: RTE_RELATION tablesample \
                                     is not a TableSampleClause node",
                                ))
                            }
                        }
                    };

                    // Preprocess each args element.
                    let mut new_args: mcx::PgVec<'mcx, Expr> =
                        mcx::vec_with_capacity_in(mcx, args.len())?;
                    for e in args.into_iter() {
                        let pe = preprocess_expression(
                            mcx,
                            &mut *root,
                            run,
                            outer_query_ref,
                            Some(e),
                            EXPRKIND_TABLESAMPLE,
                        )?;
                        let pe = pe.ok_or_else(|| {
                            types_error::PgError::error(
                                "subquery_planner: TABLESAMPLE arg folded to NULL",
                            )
                        })?;
                        new_args.push(pe);
                    }

                    // Preprocess the optional repeatable Expr.
                    let new_repeatable: Option<alloc::boxed::Box<Expr>> =
                        match repeatable {
                            Some(e) => {
                                let pe = preprocess_expression(
                                    mcx,
                                    &mut *root,
                                    run,
                                    outer_query_ref,
                                    Some(e),
                                    EXPRKIND_TABLESAMPLE,
                                )?;
                                let pe = pe.ok_or_else(|| {
                                    types_error::PgError::error(
                                        "subquery_planner: TABLESAMPLE repeatable \
                                         folded to NULL",
                                    )
                                })?;
                                Some(alloc::boxed::Box::new(pe))
                            }
                            None => None,
                        };

                    // Write the preprocessed expressions back into the clause
                    // node in place.
                    if let Some(tsc) = run
                        .resolve_mut(root.parse)
                        .rtable[i]
                        .tablesample
                        .as_mut()
                        .expect("tablesample is_some")
                        .as_tablesampleclause_mut()
                    {
                        tsc.args = Some(new_args);
                        tsc.repeatable = new_repeatable;
                    }
                }
            }
            RTEKind::RTE_SUBQUERY => {
                // planner.c:1001-1012:
                //   if (rte->lateral && root->hasJoinRTEs)
                //       rte->subquery = (Query *)
                //           flatten_join_alias_vars(root, root->parse,
                //                                   (Node *) rte->subquery);
                //
                // We don't want to do all preprocessing yet on the subquery's
                // expressions, since that will happen when we plan it. But if it
                // contains any join aliases of our level, those have to get
                // expanded now, because planning of the subquery won't do it.
                // That's only possible if the subquery is LATERAL.
                if lateral && root.hasJoinRTEs {
                    // `flatten_join_alias_vars` consults the *outer* query's
                    // range table (root->parse) for the RTE_JOIN joinaliasvars
                    // lists; the same `outer_query_ref` node threaded into the
                    // per-expression preprocess calls above is exactly that
                    // `root->parse` clone. In the C, the whole subquery `Query`
                    // is cast to a `Node` and the freshly-built result reassigned
                    // to `rte->subquery`; here the seam mutates the owned tree, so
                    // take the subquery out, wrap it as a `Node::Query`, flatten,
                    // and write the result back into the RTE (mirroring the
                    // OffsetVarNodes / pullup_replace_vars_subquery write-back
                    // pattern in prepjointree). The flatten mutator handles a
                    // top-level `T_Query` node directly (incrementing
                    // sublevels_up and recursing via query_tree_mutator).
                    let query_node = outer_query_ref.ok_or_else(|| {
                        types_error::PgError::error(
                            "subquery_planner: LATERAL subquery join-alias \
                             flattening needs the outer Query (root->parse) but \
                             none was threaded in (root.hasJoinRTEs is set)",
                        )
                    })?;
                    let subq = run.resolve_mut(root.parse).rtable[i].subquery.take();
                    if let Some(sq) = subq {
                        let q = mcx::PgBox::into_inner(sq);
                        let sub_node = Node::mk_query(mcx, q)?;
                        let flat =
                            rewritemanip_seams::flatten_join_alias_vars::call(
                                mcx,
                                Some(&mut *root), // C planner.c passes live root
                                query_node,
                                sub_node,
                            )?;
                        let flat_q = flat.into_query().ok_or_else(|| {
                            types_error::PgError::error(
                                "subquery_planner: flatten_join_alias_vars over a \
                                 LATERAL subquery returned a non-Query node",
                            )
                        })?;
                        run.resolve_mut(root.parse).rtable[i].subquery =
                            Some(mcx::alloc_in(mcx, flat_q)?);
                    }
                }
            }
            RTEKind::RTE_VALUES => {
                // Preprocess the values lists fully (planner.c:1029-1034):
                // const-fold each row's each column expression. EXPRKIND_VALUES
                // skips flatten_join_alias_vars/canonicalize. LATERAL VALUES
                // (CREATE RULE only) panics in preprocess_expression if any
                // join RTEs are present, matching the C gate.
                let kind = if lateral {
                    EXPRKIND_VALUES_LATERAL
                } else {
                    EXPRKIND_VALUES
                };
                let nrows = run.resolve(root.parse).rtable[i].values_lists.len();
                for r in 0..nrows {
                    // Take the row's column expressions out, process, put back.
                    let row_exprs: mcx::PgVec<'mcx, Expr> = {
                        let parse = run.resolve(root.parse);
                        let row_node = &parse.rtable[i].values_lists[r];
                        let row_node = &**row_node;
                        let cols = match row_node.node_tag() {
                            ntag::T_List => row_node.expect_list(),
                            _ => {
                                return Err(types_error::PgError::error(
                                    "subquery_planner: VALUES row is not a List",
                                ))
                            }
                        };
                        let mut v = mcx::vec_with_capacity_in(mcx, cols.len())?;
                        for c in cols.iter() {
                            match c.as_expr() {
                                // Deep-copy through `clone_in` (copyObject shape),
                                // not a plain `.clone()`: a VALUES column may be a
                                // `SubLink` (e.g. `(select 2)`) whose embedded owned
                                // `Query` is context-allocated and panics under the
                                // derived `Clone` (mirrors Aggref/SubPlan).
                                Some(e) => v.push(e.clone_in(mcx)?),
                                None => {
                                    return Err(types_error::PgError::error(
                                        "subquery_planner: VALUES column is not an Expr",
                                    ))
                                }
                            }
                        }
                        v
                    };
                    let mut new_cols: mcx::PgVec<'mcx, ::nodes::nodes::NodePtr<'mcx>> =
                        mcx::vec_with_capacity_in(mcx, row_exprs.len())?;
                    for e in row_exprs.into_iter() {
                        let pe = preprocess_expression(mcx, &mut *root, run, outer_query_ref, Some(e), kind)?;
                        let pe = pe.ok_or_else(|| {
                            types_error::PgError::error(
                                "subquery_planner: VALUES column folded to NULL",
                            )
                        })?;
                        new_cols.push(mcx::alloc_in(mcx, Node::mk_expr(mcx, pe)?)?);
                    }
                    let new_row = mcx::alloc_in(mcx, Node::mk_list(mcx, new_cols)?)?;
                    run.resolve_mut(root.parse).rtable[i].values_lists[r] = new_row;
                }
            }
            RTEKind::RTE_GROUP => {
                // Preprocess the groupexprs list fully (planner.c:1035-1038):
                // rte->groupexprs = preprocess_expression(root, groupexprs,
                // EXPRKIND_GROUPEXPR). The list is a flat PgVec<NodePtr> of
                // Node::Expr; const-fold each element. EXPRKIND_GROUPEXPR runs
                // eval_const_expressions but not canonicalize/saop.
                let ng = run.resolve(root.parse).rtable[i].groupexprs.len();
                for g in 0..ng {
                    let e: Expr = {
                        let parse = run.resolve(root.parse);
                        match parse.rtable[i].groupexprs[g].as_expr() {
                            // Deep-copy through `clone_in` (copyObject shape), not
                            // the derived `.clone()`: a groupexpr can carry a
                            // SubLink-bearing subtree, whose derived Clone panics.
                            Some(e) => e.clone_in(mcx)?,
                            None => {
                                return Err(types_error::PgError::error(
                                    "subquery_planner: RTE_GROUP groupexpr is not an Expr",
                                ))
                            }
                        }
                    };
                    let pe = preprocess_expression(
                        mcx,
                        &mut *root,
                        run,
                        outer_query_ref,
                        Some(e),
                        EXPRKIND_GROUPEXPR,
                    )?;
                    let pe = pe.ok_or_else(|| {
                        types_error::PgError::error(
                            "subquery_planner: RTE_GROUP groupexpr folded to NULL",
                        )
                    })?;
                    run.resolve_mut(root.parse).rtable[i].groupexprs[g] =
                        mcx::alloc_in(mcx, Node::mk_expr(mcx, pe)?)?;
                }
            }
            RTEKind::RTE_FUNCTION => {
                // Preprocess the function expression(s) fully (planner.c:
                // 1015-1021): rte->functions = preprocess_expression(root,
                // (Node *) rte->functions, EXPRKIND_RTFUNC[_LATERAL]). C folds
                // the whole `functions` list as one node; here the typed list
                // is a flat PgVec<NodePtr> of RangeTblFunction nodes, so
                // preprocess each RangeTblFunction's funcexpr (the only
                // expression subtree it carries).
                let kind = if lateral {
                    EXPRKIND_RTFUNC_LATERAL
                } else {
                    EXPRKIND_RTFUNC
                };
                let nfuncs = run.resolve(root.parse).rtable[i].functions.len();
                for f in 0..nfuncs {
                    // Take the RangeTblFunction's funcexpr Expr out, preprocess,
                    // put it back. A funcexpr that is not a Node::Expr (or is
                    // absent) is left untouched.
                    let fe: Option<Expr> = {
                        let parse = run.resolve(root.parse);
                        let fn_node = &*parse.rtable[i].functions[f];
                        match fn_node.node_tag() {
                            ntag::T_RangeTblFunction => {
                                // Route through `clone_in` (not the derived
                                // `.clone()`): a funcexpr substituted in by
                                // subquery pullup can carry a SubLink-bearing
                                // subtree (e.g. a view column whose expression is
                                // a CASE containing a scalar subselect), whose
                                // derived Clone is a guard that panics.
                                match fn_node
                                    .expect_rangetblfunction()
                                    .funcexpr
                                    .as_deref()
                                    .and_then(|n| n.as_expr())
                                {
                                    Some(e) => Some(e.clone_in(mcx)?),
                                    None => None,
                                }
                            }
                            _ => {
                                return Err(types_error::PgError::error(
                                    "subquery_planner: RTE_FUNCTION functions entry is not a RangeTblFunction",
                                ))
                            }
                        }
                    };
                    if let Some(e) = fe {
                        let pe = preprocess_expression(
                            mcx,
                            &mut *root,
                            run,
                            outer_query_ref,
                            Some(e),
                            kind,
                        )?;
                        let pe = pe.ok_or_else(|| {
                            types_error::PgError::error(
                                "subquery_planner: RTE_FUNCTION funcexpr folded to NULL",
                            )
                        })?;
                        if let Some(rtf) =
                            (*run.resolve_mut(root.parse).rtable[i].functions[f]).as_rangetblfunction_mut()
                        {
                            rtf.funcexpr = Some(mcx::alloc_in(mcx, Node::mk_expr(mcx, pe)?)?);
                        }
                    }
                }
            }
            RTEKind::RTE_TABLEFUNC => {
                // planner.c:1022-1027: rte->tablefunc = preprocess_expression(
                //   root, (Node *) rte->tablefunc, EXPRKIND_TABLEFUNC[_LATERAL]).
                // C folds the whole TableFunc node as one; in the owned model the
                // TableFunc carries several Expr subtrees (docexpr, rowexpr, the
                // per-column colexprs/coldefexprs, and the namespace ns_uris), so
                // we preprocess each subtree in place — exactly as the RTE_FUNCTION
                // arm above does for each RangeTblFunction funcexpr.
                let kind = if lateral {
                    EXPRKIND_TABLEFUNC_LATERAL
                } else {
                    EXPRKIND_TABLEFUNC
                };

                // Helper: take an Expr out, preprocess it, hand it back.
                macro_rules! preprocess_one {
                    ($e:expr) => {{
                        let taken: Option<Expr> = $e;
                        match taken {
                            Some(e) => preprocess_expression(
                                mcx,
                                &mut *root,
                                run,
                                outer_query_ref,
                                Some(e),
                                kind,
                            )?,
                            None => None,
                        }
                    }};
                }

                // docexpr
                let doc = {
                    let parse = run.resolve(root.parse);
                    match parse.rtable[i]
                        .tablefunc
                        .as_deref()
                        .and_then(|n| n.as_table_func())
                        .and_then(|tf| tf.docexpr.as_deref())
                    {
                        Some(e) => Some(e.clone_in(mcx)?),
                        None => None,
                    }
                };
                if let Some(pe) = preprocess_one!(doc) {
                    if let Some(tf) = (*run.resolve_mut(root.parse).rtable[i].tablefunc.as_mut().unwrap()).as_table_func_mut() {
                        tf.docexpr = Some(mcx::alloc_in(mcx, pe)?);
                    }
                }
                // rowexpr
                let row = {
                    let parse = run.resolve(root.parse);
                    match parse.rtable[i]
                        .tablefunc
                        .as_deref()
                        .and_then(|n| n.as_table_func())
                        .and_then(|tf| tf.rowexpr.as_deref())
                    {
                        Some(e) => Some(e.clone_in(mcx)?),
                        None => None,
                    }
                };
                if let Some(pe) = preprocess_one!(row) {
                    if let Some(tf) = (*run.resolve_mut(root.parse).rtable[i].tablefunc.as_mut().unwrap()).as_table_func_mut() {
                        tf.rowexpr = Some(mcx::alloc_in(mcx, pe)?);
                    }
                }
                // colexprs[k] / coldefexprs[k]
                let ncols = {
                    let parse = run.resolve(root.parse);
                    parse.rtable[i]
                        .tablefunc
                        .as_deref()
                        .and_then(|n| n.as_table_func())
                        .and_then(|tf| tf.colexprs.as_ref().map(|v| v.len()))
                        .unwrap_or(0)
                };
                for k in 0..ncols {
                    let ce = {
                        let parse = run.resolve(root.parse);
                        match parse.rtable[i]
                            .tablefunc
                            .as_deref()
                            .and_then(|n| n.as_table_func())
                            .and_then(|tf| tf.colexprs.as_ref())
                            .and_then(|v| v.get(k))
                            .and_then(|o| o.as_deref())
                        {
                            Some(e) => Some(e.clone_in(mcx)?),
                            None => None,
                        }
                    };
                    if let Some(pe) = preprocess_one!(ce) {
                        if let Some(tf) = (*run.resolve_mut(root.parse).rtable[i].tablefunc.as_mut().unwrap()).as_table_func_mut() {
                            if let Some(slot) = tf.colexprs.as_mut().and_then(|v| v.get_mut(k)) {
                                *slot = Some(mcx::alloc_in(mcx, pe)?);
                            }
                        }
                    }
                    let cde = {
                        let parse = run.resolve(root.parse);
                        match parse.rtable[i]
                            .tablefunc
                            .as_deref()
                            .and_then(|n| n.as_table_func())
                            .and_then(|tf| tf.coldefexprs.as_ref())
                            .and_then(|v| v.get(k))
                            .and_then(|o| o.as_deref())
                        {
                            Some(e) => Some(e.clone_in(mcx)?),
                            None => None,
                        }
                    };
                    if let Some(pe) = preprocess_one!(cde) {
                        if let Some(tf) = (*run.resolve_mut(root.parse).rtable[i].tablefunc.as_mut().unwrap()).as_table_func_mut() {
                            if let Some(slot) = tf.coldefexprs.as_mut().and_then(|v| v.get_mut(k)) {
                                *slot = Some(mcx::alloc_in(mcx, pe)?);
                            }
                        }
                    }
                }
                // ns_uris[k]
                let nns = {
                    let parse = run.resolve(root.parse);
                    parse.rtable[i]
                        .tablefunc
                        .as_deref()
                        .and_then(|n| n.as_table_func())
                        .and_then(|tf| tf.ns_uris.as_ref().map(|v| v.len()))
                        .unwrap_or(0)
                };
                for k in 0..nns {
                    let ne = {
                        let parse = run.resolve(root.parse);
                        match parse.rtable[i]
                            .tablefunc
                            .as_deref()
                            .and_then(|n| n.as_table_func())
                            .and_then(|tf| tf.ns_uris.as_ref())
                            .and_then(|v| v.get(k))
                        {
                            Some(b) => Some((**b).clone_in(mcx)?),
                            None => None,
                        }
                    };
                    if let Some(pe) = preprocess_one!(ne) {
                        if let Some(tf) = (*run.resolve_mut(root.parse).rtable[i].tablefunc.as_mut().unwrap()).as_table_func_mut() {
                            if let Some(slot) = tf.ns_uris.as_mut().and_then(|v| v.get_mut(k)) {
                                *slot = mcx::alloc_in(mcx, pe)?;
                            }
                        }
                    }
                }
            }
            _ => {}
        }

        // Process each element of the securityQuals list as if it were a
        // separate qual expression (planner.c:1044-1054). We need to do it this
        // way to get proper canonicalization of AND/OR structure. In C each
        // element is treated as `(Node *) lfirst(lcsq)` and re-stored in place;
        // EXPRKIND_QUAL preprocessing converts it into an implicit-AND sublist.
        //
        // In this owned RTE model each securityQuals element is a single
        // `Node::Expr` (the RLS rewriter and security-barrier-view rewriter both
        // build one Expr per barrier qual — an OpExpr/BoolExpr/Const, possibly
        // OR-combined). Take each Expr out, run preprocess_expression with
        // EXPRKIND_QUAL (flatten_join_alias_vars / eval_const_expressions /
        // canonicalize_qual / SS_process_sublinks), and write the result back.
        // The downstream consumer (process_security_barrier_quals in initsplan)
        // runs each stored element through make_ands_implicit, mirroring C's
        // make_ands_implicit on the preprocessed Node, so a single canonicalized
        // Expr is the right stored shape.
        let n_sec = run.resolve(root.parse).rtable[i].securityQuals.len();
        for sq in 0..n_sec {
            // Take the securityQuals element NodePtr out, convert to an owned
            // Expr, preprocess, and write the result back.
            let qual_expr: Option<Expr> = {
                let node = &*run.resolve(root.parse).rtable[i].securityQuals[sq];
                match node.as_expr() {
                    Some(e) => Some(e.clone_in(mcx)?),
                    None => {
                        return Err(PgError::error(
                            "subquery_planner: securityQuals element is not an \
                             expression node",
                        ));
                    }
                }
            };

            let processed = preprocess_expression(
                mcx,
                &mut *root,
                run,
                outer_query_ref,
                qual_expr,
                EXPRKIND_QUAL,
            )?;

            // canonicalize_qual may fold a security qual to nothing (e.g. a
            // constant-true barrier); store NULL in that case. C re-stores the
            // (possibly NULL) Node pointer in place; here we replace the element
            // with a freshly-allocated Node::Expr, or a Const-true / NULL marker
            // when it folds away. A folded-to-NULL barrier means the qual is
            // unconditionally satisfied, matching C where lfirst(lcsq) becomes
            // NIL and make_ands_implicit(NULL) yields an empty conjunct list.
            match processed {
                Some(pe) => {
                    run.resolve_mut(root.parse).rtable[i].securityQuals[sq] =
                        mcx::alloc_in(mcx, Node::mk_expr(mcx, pe)?)?;
                }
                None => {
                    // Folded to constant-true: represent as a Const(true) so the
                    // element survives as an always-satisfied barrier (matches
                    // make_ands_implicit over a NULL/true qual yielding no rows
                    // filtered). Use a boolean true Const.
                    let true_const = nodes_core::makefuncs::make_bool_const(true, false);
                    run.resolve_mut(root.parse).rtable[i].securityQuals[sq] =
                        mcx::alloc_in(mcx, Node::mk_const(mcx, true_const)?)?;
                }
            }
        }
    }
    Ok(())
}

/// flatten_group_exprs over targetList/havingQual (planner.c:1088-1095).
/// GROUP-RTE queries only.
#[inline(never)]
fn pqe_flatten_group_exprs<'mcx>(
    mcx: Mcx<'mcx>,
    run: &mut PlannerRun<'mcx>,
    root: &mut PlannerInfo,
) -> PgResult<()> {
    use ::nodes::nodes::Node;
    // flatten_group_exprs reads query->rtable (the RTE_GROUP groupexprs)
    // and query->hasSubLinks; clone the parse Query into the run arena as
    // the immutable context so we can mutate targetList/havingQual on the
    // live Query in place. The clone already carries the preprocessed
    // groupexprs.
    let ctx_query = run.resolve(root.parse).clone_in(mcx)?;

    // targetList: apply to each TargetEntry's expr (walking the list is
    // equivalent to the C flatten over the whole List node).
    let ntargets = run.resolve(root.parse).targetList.len();
    for t in 0..ntargets {
        let expr_opt: Option<Expr> = {
            let parse = run.resolve(root.parse);
            match parse.targetList[t].expr.as_deref() {
                Some(e) => Some(e.clone_in(mcx)?),
                None => None,
            }
        };
        if let Some(e) = expr_opt {
            let node = Node::mk_expr(mcx, e)?;
            let flattened = vars::flatten::flatten_group_exprs(
                mcx, Some(&mut *root), &ctx_query, node,
            )?;
            if let Some(ne) = flattened.into_expr() {
                run.resolve_mut(root.parse).targetList[t].expr =
                    Some(mcx::alloc_in(mcx, ne)?);
            }
        }
    }

    // havingQual.
    let having_opt: Option<Expr> = match run.resolve(root.parse).havingQual.as_deref() {
        Some(e) => Some(e.clone_in(mcx)?),
        None => None,
    };
    if let Some(e) = having_opt {
        let node = Node::mk_expr(mcx, e)?;
        let flattened = vars::flatten::flatten_group_exprs(
                mcx, Some(&mut *root), &ctx_query, node,
            )?;
        if let Some(ne) = flattened.into_expr() {
            run.resolve_mut(root.parse).havingQual = Some(mcx::alloc_in(mcx, ne)?);
        }
    }
    Ok(())
}

/// expand_grouping_sets (planner.c:1107-1110). GROUPING SETS only.
#[inline(never)]
fn pqe_expand_grouping_sets<'mcx>(
    mcx: Mcx<'mcx>,
    run: &mut PlannerRun<'mcx>,
    root: &mut PlannerInfo,
) -> PgResult<()> {
    let grouping_sets_nodes: alloc::vec::Vec<Node<'mcx>> = {
        let parse = run.resolve(root.parse);
        let mut v: alloc::vec::Vec<Node<'mcx>> =
            alloc::vec::Vec::with_capacity(parse.groupingSets.len());
        for gs in parse.groupingSets.iter() {
            v.push(gs.as_ref().clone_in(mcx)?);
        }
        v
    };
    let group_distinct = run.resolve(root.parse).groupDistinct;
    let expanded = parse_agg_seams::expand_grouping_sets::call(
        mcx,
        &grouping_sets_nodes,
        group_distinct,
        -1,
    )?;

    let mut new_gsets: mcx::PgVec<'mcx, ::nodes::nodes::NodePtr<'mcx>> =
        mcx::PgVec::new_in(mcx);
    if let Some(sets) = expanded {
        for set in sets.iter() {
            let mut intlist: mcx::PgVec<'mcx, i32> = mcx::PgVec::new_in(mcx);
            intlist.extend(set.iter().copied());
            new_gsets.push(mcx::alloc_in(mcx, Node::mk_int_list(mcx, intlist)?)?);
        }
    }
    run.resolve_mut(root.parse).groupingSets = new_gsets;
    Ok(())
}

/// newHaving HAVING→WHERE transfer loop (planner.c:1154-1199). Runs only when
/// there is a havingQual.
#[inline(never)]
fn pqe_having_transfer<'mcx>(
    mcx: Mcx<'mcx>,
    run: &mut PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    outer_query_ref: Option<&Node<'mcx>>,
) -> PgResult<()> {
    use nodes_core::makefuncs::{make_ands_explicit, make_ands_implicit};

    // havingQual is the implicitly-ANDed list of HAVING clauses.
    let having_expr: Option<Expr> = run
        .resolve_mut(root.parse)
        .havingQual
        .take()
        .map(mcx::PgBox::into_inner);
    let having_clauses: alloc::vec::Vec<Expr> = make_ands_implicit(having_expr);

    // Snapshot the grouping flags the loop branches on. groupClause and
    // groupingSets are NIL/non-NIL tests; for grouping sets we also need
    // whether the first grouping set is empty (linitial(groupingSets) != NIL).
    let (has_group_clause, has_grouping_sets, first_gset_nonempty) = {
        let parse = run.resolve(root.parse);
        let has_group_clause = !parse.groupClause.is_empty();
        let has_grouping_sets = !parse.groupingSets.is_empty();
        // This loop runs AFTER expand_grouping_sets, so parse->groupingSets is
        // a flat List of T_IntList sets sorted by length ascending (the empty
        // set, if present, sorts first). C's test `linitial(groupingSets) != NIL`
        // therefore checks whether the first (shortest) set is the empty set.
        let first_gset_nonempty = match parse.groupingSets.first() {
            Some(gs) => match gs.as_ref().as_intlist() {
                Some(l) => !l.is_empty(),
                // Not yet expanded (still a GroupingSet tree) or unexpected:
                // treat as non-empty (the conservative branch keeps a WHERE copy).
                None => true,
            },
            None => false,
        };
        (has_group_clause, has_grouping_sets, first_gset_nonempty)
    };
    let group_rtindex = root.group_rtindex;

    let mut new_having: alloc::vec::Vec<Expr> = alloc::vec::Vec::new();
    // Accumulate clauses moved/copied into WHERE; appended to the
    // jointree's existing quals after the loop (C list_concat).
    let mut moved_to_where: alloc::vec::Vec<Expr> = alloc::vec::Vec::new();

    for havingclause in having_clauses {
        // contain_agg_clause / contain_volatile_functions / contain_subplans
        // / (grouping-sets GROUP-Var membership) => keep in HAVING (C:1158-1166).
        let keep_in_having = clauses::grounded::contain_agg_clause(
            Some(&havingclause),
        )? || clauses::grounded::contain_volatile_functions(
            Some(&havingclause),
        )? || clauses::grounded::contain_subplans(
            Some(&havingclause),
        )? || (has_group_clause
            && has_grouping_sets
            && {
                // bms_is_member(root->group_rtindex, pull_varnos(root, havingclause))
                let node = Node::mk_expr(mcx, havingclause.clone_in(mcx)?)?;
                let varnos =
                    vars::var::pull_varnos(Some(&root), &node);
                bms_is_member_relids(group_rtindex, &varnos)
            });

        if keep_in_having {
            // keep it in HAVING (C:1166).
            new_having.push(havingclause);
        } else if has_group_clause && (!has_grouping_sets || first_gset_nonempty) {
            // There is GROUP BY, but no empty grouping set (C:1168-1180):
            // preprocess fully and move it to WHERE.
            let whereclause = preprocess_expression(
                mcx,
                &mut *root,
                run,
                outer_query_ref,
                Some(havingclause),
                EXPRKIND_QUAL,
            )?;
            // make_ands_implicit so the moved clause matches the
            // implicitly-ANDed list form of jointree->quals (the C
            // list_concat splices a List* directly; preprocess_expression
            // here returns an already make_ands_implicit'd single Expr).
            for c in make_ands_implicit(whereclause) {
                moved_to_where.push(c);
            }
        } else {
            // There is an empty grouping set, perhaps implicitly (C:1182-1197):
            // preprocess a *copy* into WHERE and also keep the original in
            // HAVING.
            let copy = havingclause.clone_in(mcx)?;
            let whereclause = preprocess_expression(
                mcx,
                &mut *root,
                run,
                outer_query_ref,
                Some(copy),
                EXPRKIND_QUAL,
            )?;
            for c in make_ands_implicit(whereclause) {
                moved_to_where.push(c);
            }
            new_having.push(havingclause);
        }
    }

    // parse->havingQual = (Node *) newHaving (C:1199). Empty list => NULL.
    run.resolve_mut(root.parse).havingQual = if new_having.is_empty() {
        None
    } else {
        Some(mcx::alloc_in(mcx, make_ands_explicit(new_having))?)
    };

    // list_concat the moved clauses onto jointree->quals (C:1173-1180,
    // 1190-1197). The jointree quals live as a single `Node::Expr`; splice
    // by re-imploding the existing-plus-moved implicit-AND list.
    if !moved_to_where.is_empty() {
        let jt = run.resolve_mut(root.parse).jointree.take();
        if let Some(jt) = jt {
            let mut f = mcx::PgBox::into_inner(jt);
            let existing: Option<Expr> = match f.quals.take() {
                None => None,
                Some(n) => match mcx::PgBox::into_inner(n) {
                    other if other.is_expr() => Some(other.into_expr().unwrap()),
                    other => {
                        return Err(PgError::error(alloc::format!(
                            "subquery_planner: jointree quals is a non-Expr \
                             node during HAVING transfer: {:?}",
                            other.node_tag()
                        )));
                    }
                },
            };
            let mut combined = make_ands_implicit(existing);
            combined.append(&mut moved_to_where);
            f.quals = Some(mcx::alloc_in(
                mcx,
                Node::mk_expr(mcx, make_ands_explicit(combined))?,
            )?);
            run.resolve_mut(root.parse).jointree = Some(mcx::alloc_in(mcx, f)?);
        } else {
            // No jointree (replace_empty_jointree should have created one);
            // build a bare FromExpr to hold the moved quals.
            let f = ::nodes::rawnodes::FromExpr {
                fromlist: mcx::PgVec::new_in(mcx),
                quals: Some(mcx::alloc_in(
                    mcx,
                    Node::mk_expr(mcx, make_ands_explicit(moved_to_where))?,
                )?),
            };
            run.resolve_mut(root.parse).jointree = Some(mcx::alloc_in(mcx, f)?);
        }
    }
    Ok(())
}

/// As [`subquery_planner`], but with the recursive-term carrier
/// (`(wt_param_id, non_recursive_rows)`) seeded onto the root before its access
/// paths (including the self-reference WorkTableScan) are built.
#[allow(clippy::too_many_arguments)]
fn subquery_planner_carried<'mcx>(
    mcx: Mcx<'mcx>,
    run: &mut PlannerRun<'mcx>,
    glob: PlannerGlobal,
    parse_id: pathnodes::QueryId,
    parent_root: Option<PlannerInfo>,
    recursion_carry: Option<(i32, f64)>,
    has_recursion: bool,
    tuple_fraction: f64,
    setops: Option<&'mcx ::nodes::rawnodes::SetOperationStmt<'mcx>>,
) -> PgResult<PlannerInfo> {
    // root = makeNode(PlannerInfo); + field init (C:664-703).
    let mut root = PlannerInfo::default();
    root.parse = parse_id;
    root.glob = Some(Box::new(glob));
    // root->query_level = parent_root ? parent_root->query_level + 1 : 1 (C:666).
    // root->parent_root = parent_root (C:688). The parent PlannerInfo is moved
    // in by value (its `glob` was already taken out by the caller and threaded
    // separately as `glob` above); CTE / upper-Var resolution walks this chain.
    // The caller recovers the parent by taking `subroot.parent_root` back out.
    root.query_level = match &parent_root {
        Some(p) => p.query_level + 1,
        None => 1,
    };
    root.parent_root = parent_root.map(Box::new);
    root.hasRecursion = has_recursion;
    // For a recursive WITH query, assign the work-table PARAM_EXEC id now so the
    // self-reference's WorkTableScan and the RecursiveUnion path agree (C:698).
    root.wt_param_id = if has_recursion {
        paramassign_seams::assign_special_exec_param::call(&mut root)?
    } else {
        -1
    };

    // Seed the recursive-term carrier (see set_worktable_pathlist): the
    // work-table param id this leaf's WorkTableScan reads (overriding the -1 just
    // assigned for a non-recursive leaf), and the non-recursive term's row
    // estimate used to size it. Set before make_one_rel builds the scan path.
    if let Some((wt_param_id, nr_rows)) = recursion_carry {
        root.wt_param_id = wt_param_id;
        root.non_recursive_rows = Some(nr_rows);
    }

    // root->all_result_relids = parse->resultRelation ?
    //     bms_make_singleton(parse->resultRelation) : NULL; (planner.c:699-700).
    // root->leaf_result_relids = NULL; we'll find out leaf-ness later (C:701).
    // Seeding all_result_relids with the target relation here is the prerequisite
    // for expand_single_inheritance_child (inherit.c) to recognize the children of
    // an inherited UPDATE/DELETE/MERGE target via bms_is_member(parentRTindex,
    // all_result_relids) and record each leaf in all_result_relids/leaf_result_relids.
    {
        let result_relation = run.resolve(root.parse).resultRelation;
        root.all_result_relids = if result_relation != 0 {
            bms_make_singleton_relids(result_relation)
        } else {
            None
        };
        root.leaf_result_relids = None;
    }

    // Top-level join domain (C:710).
    root.join_domains.push(JoinDomain { jd_relids: None });

    // SS_process_ctes if cteList (C:716-717). CTEs are converted to
    // RTE_SUBQUERY or initplan SubPlans. The owner (init-subselect subplan.rs)
    // threads (&mut root, &mut run) and recurses into each CTE's ctequery.
    {
        let has_ctes = !run.resolve(root.parse).cteList.is_empty();
        if has_ctes {
            init_subselect::subplan::SS_process_ctes(mcx, &mut root, run)?;
        }
    }

    // transform_MERGE_to_join(parse) (C:722). For non-MERGE this is a no-op.
    {
        let parse = run.resolve_mut(root.parse);
        prepjointree::transform_MERGE_to_join(mcx, parse)?;
    }

    // replace_empty_jointree(parse) (C:728). If the Query's jointree is empty,
    // inject a dummy RTE_RESULT relation (so e.g. `SELECT 1` plans). The owner
    // (prepjointree.c, ported in subselect-pullup) no-ops if the fromlist is
    // already non-empty or if this is the top of a setop tree.
    {
        let parse = run.resolve_mut(root.parse);
        subselect_pullup::replace_empty_jointree(mcx, parse)?;
    }

    // pull_up_sublinks if hasSubLinks (C:736-737).
    {
        let parse_has_sublinks = run.resolve(root.parse).hasSubLinks;
        if parse_has_sublinks {
            // pull_up_sublinks(mcx, root, parse): resolve parse for &mut.
            let parse = run.resolve_mut(root.parse);
            // SAFETY/borrow: pull_up_sublinks takes (&mut root, &mut parse) which
            // are distinct objects in the C model; but here `parse` borrows `run`
            // and `root` is separate, satisfying the borrow checker.
            prepjointree::pull_up_sublinks(mcx, &mut root, parse)?;
        }
    }

    // preprocess_function_rtes(root) (C:745). The owner takes (&mut root, &mut
    // parse); `parse` borrows `run`, `root` is a separate local, so the two
    // disjoint &mut borrows are fine.
    {
        let parse = run.resolve_mut(root.parse);
        prepjointree::preprocess_function_rtes(mcx, &mut root, parse)?;
    }

    // parse = root->parse = expand_virtual_generated_columns(root) (C:753). The
    // owner consumes and returns an owned Query, so swap it out of the run,
    // transform, and store the result back.
    {
        let parse_owned = run.resolve(root.parse).clone_in(mcx)?;
        let parse_new = prepjointree::expand_virtual_generated_columns(
            mcx, &mut root, parse_owned,
        )?;
        *run.resolve_mut(root.parse) = parse_new;
    }

    // pull_up_subqueries(root) (C:759).
    {
        let parse = run.resolve_mut(root.parse);
        prepjointree::pull_up_subqueries(mcx, &mut root, parse)?;
    }

    // flatten_simple_union_all if setOperations (C:767-768).
    {
        let has_setops = run.resolve(root.parse).setOperations.is_some();
        if has_setops {
            let parse = run.resolve_mut(root.parse);
            prepjointree::flatten_simple_union_all(mcx, &mut root, parse)?;
        }
    }

    // Survey the rangetable (C:780-837).
    root.hasJoinRTEs = false;
    root.hasLateralRTEs = false;
    root.group_rtindex = 0;
    let mut has_outer_joins = false;
    let mut has_result_rtes = false;
    {
        // We need to mutate rte->inh (has_subclass) and read fields; resolve the
        // Query mutably.
        let parse = run.resolve_mut(root.parse);
        let rtable_len = parse.rtable.len();
        for idx in 0..rtable_len {
            // Read the fields we need before any catalog call to limit borrows.
            let rtekind = parse.rtable[idx].rtekind;
            let inh = parse.rtable[idx].inh;
            let relid = parse.rtable[idx].relid;
            let jointype = parse.rtable[idx].jointype;
            let lateral = parse.rtable[idx].lateral;
            let sec_qual_len = parse.rtable[idx].securityQuals.len();

            use ::nodes::parsenodes::RTEKind;
            match rtekind {
                RTEKind::RTE_RELATION => {
                    if inh {
                        let has_sub = pg_inherits::has_subclass(relid)?;
                        parse.rtable[idx].inh = has_sub;
                    }
                }
                RTEKind::RTE_JOIN => {
                    root.hasJoinRTEs = true;
                    if is_outer_join(jointype) {
                        has_outer_joins = true;
                    }
                }
                RTEKind::RTE_RESULT => {
                    has_result_rtes = true;
                }
                RTEKind::RTE_GROUP => {
                    // Assert(parse->hasGroupRTE); group_rtindex = idx + 1.
                    root.group_rtindex = (idx + 1) as i32;
                }
                _ => {}
            }

            if lateral {
                root.hasLateralRTEs = true;
            }

            if sec_qual_len > 0 {
                let cur = root.qual_security_level as usize;
                root.qual_security_level = core::cmp::max(cur, sec_qual_len) as u32;
            }
        }
    }

    // leaf_result_relids (C:843-850).
    {
        let parse = run.resolve(root.parse);
        if parse.resultRelation != 0 {
            let rti = parse.resultRelation as usize;
            let rte_inh = parse.rtable[rti - 1].inh;
            if !rte_inh {
                // bms_make_singleton(resultRelation) over the lifetime-free Relids.
                root.leaf_result_relids = bms_make_singleton_relids(parse.resultRelation);
            }
        }
    }

    // View-permission ACL loop (C:866-882). We need to check access permissions
    // for any view relations mentioned in the query, in order to prevent
    // information being leaked by selectivity estimation functions, which only
    // check view owner permissions on underlying tables. This means access
    // permissions for views are checked twice (the executor checks them again).
    {
        let parse = run.resolve(root.parse);
        const RELKIND_VIEW: i8 = b'v' as i8;
        for rte in parse.rtable.iter() {
            if rte.perminfoindex != 0 && rte.relkind == RELKIND_VIEW {
                // perminfo = getRTEPermissionInfo(parse->rteperminfos, rte);
                let idx = parser_relation_seams::get_rte_permission_info::call(
                    parse.rteperminfos.as_slice(),
                    rte,
                )?;
                // result = ExecCheckOneRelPerms(perminfo);
                // if (!result) aclcheck_error(ACLCHECK_NO_PRIV, OBJECT_VIEW, ...);
                execMain_seams::exec_check_one_rel_perms_view::call(
                    &parse.rteperminfos[idx],
                )?;
            }
        }
    }

    // preprocess_rowmarks(root) (C:888).
    preprocess_rowmarks(mcx, run, &mut root)?;

    // hasHavingQual (C:895).
    {
        let has_having = run.resolve(root.parse).havingQual.is_some();
        root.hasHavingQual = has_having;
    }

    // Expression-preprocessing block (C:903-1056). Run `preprocess_expression`
    // over the querytree's expressions. The expression-only `Query` fields are
    // now concretely typed (`targetList`/`returningList` as `Vec<TargetEntry>`,
    // `havingQual`/`limitOffset`/`limitCount`/`mergeJoinCondition` as
    // `Option<PgBox<Expr>>`, jointree quals via `preprocess_qual_conditions`), so
    // each owned `Expr` is handed to `preprocess_expression` (eval_const_expressions
    // / canonicalize_qual / convert_saop_to_hashed_saop) and stored back. Paths
    // with no ported owner (`flatten_join_alias_vars` when `hasJoinRTEs`,
    // `SS_process_sublinks` when `hasSubLinks`, `flatten_group_exprs` /
    // `expand_grouping_sets` for grouping queries) panic precisely inside the
    // helpers, exactly where the C performs them.
    //
    // FRAME-BLOAT: this ~860-line expression-preprocessing block holds a large
    // union of branch-local temporaries (owned `Expr`s, cloned `Query` nodes,
    // per-clause `PgBox`es). In an unoptimized (dev) build the compiler reserves
    // stack for every local in the enclosing function at once, so inlining this
    // block into `subquery_planner_carried` makes its single frame ~260 KB —
    // the dominant per-statement stack cost (it sits on every query's path,
    // including `SELECT 1`). Hoisting it behind an `#[inline(never)]` boundary
    // gives it its own frame that is released before `grouping_planner` runs, so
    // peak stack becomes max(preprocess, grouping) instead of their sum. This is
    // purely a stack-layout change; the body and its control flow (including the
    // `return Err(..)` early exits, which now return from the helper and are
    // propagated by `?`) are unchanged. Mirrors C, where preprocessing lives in
    // its own callee frames.
    #[inline(never)]
    fn preprocess_query_expressions<'mcx>(
        mcx: Mcx<'mcx>,
        run: &mut PlannerRun<'mcx>,
        root: &mut PlannerInfo,
    ) -> PgResult<()> {
        // When the query has join RTEs, `preprocess_expression` must flatten
        // join-alias Vars first (C:1274-1279, via flatten_join_alias_vars). That
        // seam reads the outer Query's range table (root->parse) for the
        // RTE_JOIN joinaliasvars lists. The arena PlannerInfo carries `parse` as a
        // handle, not the Query itself, so clone the parse Query into the run
        // arena as the immutable context node, exactly as the hasGroupRTE block
        // below does for flatten_group_exprs. Cheap and only built when needed.
        let outer_query: Option<Node<'mcx>> = if root.hasJoinRTEs {
            Some(Node::mk_query(mcx, run.resolve(root.parse).clone_in(mcx)?)?)
        } else {
            None
        };
        let outer_query_ref: Option<&Node<'mcx>> = outer_query.as_ref();

        // parse->targetList = preprocess_expression(..., EXPRKIND_TARGET) (C:903).
        // The C casts the whole `List *` to a Node and processes it; in the
        // value model each TargetEntry's `expr` is preprocessed in place.
        {
            let n = run.resolve(root.parse).targetList.len();
            for i in 0..n {
                let e = run.resolve_mut(root.parse).targetList[i].expr.take();
                let e = match e {
                    Some(b) => Some(mcx::PgBox::into_inner(b)),
                    None => None,
                };
                let processed = preprocess_expression(mcx, &mut *root, run, outer_query_ref, e, EXPRKIND_TARGET)?;
                run.resolve_mut(root.parse).targetList[i].expr = match processed {
                    Some(pe) => Some(mcx::alloc_in(mcx, pe)?),
                    None => None,
                };
            }
        }

        // withCheckOptions (C:907-916):
        //
        //   newWithCheckOptions = NIL;
        //   foreach(l, parse->withCheckOptions) {
        //       WithCheckOption *wco = lfirst_node(WithCheckOption, l);
        //       wco->qual = preprocess_expression(root, wco->qual, EXPRKIND_QUAL);
        //       if (wco->qual != NULL)
        //           newWithCheckOptions = lappend(newWithCheckOptions, wco);
        //   }
        //   parse->withCheckOptions = newWithCheckOptions;
        //
        // Each `withCheckOptions` element is a `NodePtr` to a `WithCheckOption`
        // node whose `qual` is itself a `NodePtr` to the (Expr-typed) check
        // qual. Preprocess each in place, then drop WCOs whose qual reduced to
        // NULL. (Previously a loud panic — exercising it left a SubLink-bearing
        // WCO qual half-cloned into the planner arena, so the panic's unwind of
        // a partly-built PlannerInfo double-freed the SubLink's `Box<Query, Mcx>`
        // child against an already-released context -> SIGSEGV in
        // `Mcx::deallocate`; observed on updatable_views' WITH CHECK OPTION
        // views.)
        if !run.resolve(root.parse).withCheckOptions.is_empty() {
            pqe_with_check_options(mcx, run, &mut *root, outer_query_ref)?;
        }

        // parse->returningList = preprocess_expression(..., EXPRKIND_TARGET)
        // (C:918-920). Same per-TargetEntry handling as targetList.
        if !run.resolve(root.parse).returningList.is_empty() {
            pqe_returning_list(mcx, run, &mut *root, outer_query_ref)?;
        }

        // preprocess_qual_conditions(root, (Node *) parse->jointree) (C:922).
        preprocess_qual_conditions_query(mcx, &mut *root, outer_query_ref, run)?;

        // parse->havingQual = preprocess_expression(..., EXPRKIND_QUAL) (C:924).
        {
            let h = run.resolve_mut(root.parse).havingQual.take();
            let h = h.map(mcx::PgBox::into_inner);
            let processed = preprocess_expression(mcx, &mut *root, run, outer_query_ref, h, EXPRKIND_QUAL)?;
            run.resolve_mut(root.parse).havingQual = match processed {
                Some(pe) => Some(mcx::alloc_in(mcx, pe)?),
                None => None,
            };
        }

        // windowClause start/end offsets (C:927-936). For each WindowClause,
        // preprocess wc->startOffset and wc->endOffset (EXPRKIND_LIMIT). The
        // partition/order clauses are sort/group expressions handled elsewhere.
        if !run.resolve(root.parse).windowClause.is_empty() {
            pqe_window_clause(mcx, run, &mut *root, outer_query_ref)?;
        }

        // parse->limitOffset / parse->limitCount = preprocess_expression(...,
        // EXPRKIND_LIMIT) (C:938-941).
        if run.resolve(root.parse).limitOffset.is_some()
            || run.resolve(root.parse).limitCount.is_some()
        {
            pqe_limit(mcx, run, &mut *root, outer_query_ref)?;
        }

        // onConflict expression lists (C:943-963). onConflict is only present
        // for INSERT ... ON CONFLICT.
        if run.resolve(root.parse).onConflict.is_some() {
            pqe_on_conflict(mcx, run, &mut *root, outer_query_ref)?;
        }

        // mergeActionList (C:965-978). MERGE-only; a SELECT has none. For each
        // MergeAction, preprocess its targetList (EXPRKIND_TARGET, per
        // TargetEntry expr) and its qual (EXPRKIND_QUAL). In the owned parse
        // tree the action lives as a `Node::MergeAction` whose `targetList` is a
        // `Vec<Node::TargetEntry>` and whose `qual` is an `Option<Node::Expr>`.
        if !run.resolve(root.parse).mergeActionList.is_empty() {
            pqe_merge_action_list(mcx, run, &mut *root, outer_query_ref)?;
        }

        // parse->mergeJoinCondition = preprocess_expression(..., EXPRKIND_QUAL)
        // (C:980-981).
        if run.resolve(root.parse).mergeJoinCondition.is_some() {
            pqe_merge_join_condition(mcx, run, &mut *root, outer_query_ref)?;
        }

        // root->append_rel_list = preprocess_expression(..., EXPRKIND_APPINFO)
        // (C:983-985). The C casts the whole `List *` of AppendRelInfos to a
        // Node and runs `preprocess_expression`; the only expression-bearing
        // field walked is each AppendRelInfo's `translated_vars` list (every
        // other field is scalar). In the owned model `translated_vars` is a
        // `Vec<NodeId>` of arena `Expr`s, so process each translated Var in
        // place: take it out of the node arena (resolving the borrow against
        // `&mut *root`, as the processed_tlist aggref block does), run
        // `preprocess_expression` with EXPRKIND_APPINFO, and write it back.
        if !root.append_rel_list.is_empty() {
            pqe_append_rel_list(mcx, run, &mut *root, outer_query_ref)?;
        }

        // Preprocess expressions within RTEs (C:987-1054): tablesample / subquery
        // join-alias flattening / function / tablefunc / values / groupexprs, plus
        // per-element securityQuals. A plain RTE_RELATION SELECT has none of these
        // (no TABLESAMPLE, no function/values/group RTEs, no securityQuals); scan
        // and panic precisely if any present.
        pqe_per_rte(mcx, run, &mut *root, outer_query_ref)?;

        // Drop joinaliasvars lists once flattening is done (C:1067-1078). The
        // lists no longer match what expressions in the rest of the tree look
        // like (the join-alias Vars have all been expanded), and leaving them in
        // place creates a hazard for later scans of the tree. C sets each RTE's
        // `joinaliasvars = NIL`; the owned model clears the PgVec.
        if root.hasJoinRTEs {
            let nrtes = run.resolve(root.parse).rtable.len();
            for i in 0..nrtes {
                run.resolve_mut(root.parse).rtable[i].joinaliasvars =
                    mcx::PgVec::new_in(mcx);
            }
        }

        // Replace any Vars in targetList/havingQual that reference GROUP outputs
        // with the underlying grouping expressions (C:1088-1095). GROUP-RTE only;
        // a non-grouped SELECT has hasGroupRTE == false. Performed after the
        // grouping expressions were preprocessed above.
        if run.resolve(root.parse).hasGroupRTE {
            pqe_flatten_group_exprs(mcx, run, &mut *root)?;
        }

        // hasTargetSRFs re-check (C:1098-1099). Constant-folding can remove all
        // SRFs; recompute via expression_returns_set over the targetList. C folds
        // the whole list as one node; here OR `expression_returns_set` over each
        // TargetEntry's expr.
        if run.resolve(root.parse).hasTargetSRFs {
            let parse = run.resolve(root.parse);
            let mut any_srf = false;
            for te in parse.targetList.iter() {
                if nodes_core::nodefuncs::expression_returns_set(te.expr.as_deref()) {
                    any_srf = true;
                    break;
                }
            }
            run.resolve_mut(root.parse).hasTargetSRFs = any_srf;
        }

        // expand_grouping_sets (C:1107-1110). GROUPING SETS only.
        //
        // C stuffs the flat list of integer grouping sets (a `List *` of
        // `T_IntList`s) back into `parse->groupingSets`. The expander takes the
        // groupingSets tree (a `List *` of `GroupingSet` nodes) and returns the
        // expanded representation; here it goes through the parse-agg seam,
        // which yields `PgVec<PgVec<i32>>`. Each inner integer set is wrapped as
        // a `Node::IntList` so the field keeps its `PgVec<NodePtr>` shape with
        // `T_IntList` element tags, matching C's stored representation.
        if !run.resolve(root.parse).groupingSets.is_empty() {
            pqe_expand_grouping_sets(mcx, run, &mut *root)?;
        }

        // newHaving HAVING→WHERE transfer loop (C:1154-1199). Runs only when there
        // is a havingQual; a SELECT without HAVING skips it entirely.
        //
        // Both havingQual and jointree->quals are in implicitly-ANDed-list form
        // (C:1152). In the owned model each is a single `Expr`; `make_ands_implicit`
        // splits it into the per-clause Vec the C `foreach` iterates, and
        // `make_ands_explicit` reassembles the residual list back into a single
        // `Expr` (empty list => NULL/None, matching C's NIL).
        if run.resolve(root.parse).havingQual.is_some() {
            pqe_having_transfer(mcx, run, &mut *root, outer_query_ref)?;
        }
        Ok(())
    }
    preprocess_query_expressions(mcx, run, &mut root)?;

    // reduce_outer_joins / remove_useless_result_rtes (C:1206-1216):
    {
        if has_outer_joins {
            let parse = run.resolve_mut(root.parse);
            prepjointree::reduce_outer_joins(mcx, &mut root, parse)?;
        }
        if has_result_rtes || has_outer_joins {
            // C `remove_useless_result_rtes` reads `rc->rti` for each
            // `root->rowMarks` `PlanRowMark *` to drop the ones referencing an
            // `RTE_RESULT` RTE. `root.rowMarks` carries `PlanRowMarkId` handles
            // into the run's rowmark store; the owner does not hold `run`, so
            // resolve each handle's `rti` here (parallel to `root.rowMarks`) and
            // thread it in.
            let rowmark_rtis: Vec<types_core::Index> = root
                .rowMarks
                .iter()
                .map(|&id| run.resolve_rowmark(id).rti)
                .collect();
            let parse = run.resolve_mut(root.parse);
            prepjointree::remove_useless_result_rtes(
                mcx,
                &mut root,
                parse,
                &rowmark_rtis,
            )?;
        }

        // grouping_planner(root, tuple_fraction, setops) (C:1221).
        grouping_planner(mcx, run, &mut root, tuple_fraction, setops)?;

        // SS_identify_outer_params(root) (C:1227).
        init_subselect::correlation::SS_identify_outer_params(&mut root);

        // final_rel = fetch_upper_rel(...); SS_charge_for_initplans(root, final_rel)
        // (C:1235-1236).
        let final_rel = relnode::fetch_upper_rel(
            &mut root,
            UPPERREL_FINAL,
            &None,
        );
        init_subselect::finalize::SS_charge_for_initplans(
            &mut root, final_rel,
        );

        // The set of relations consulted to prepare the query is what we will
        // need to lock before executing the resulting plan.  ... (C:1238-1241)
        // glob->finalrtable etc. is assembled in standard_planner's PlannedStmt
        // build; nothing extra to do here on the value model.

        // set_cheapest(final_rel) (C:1243).
        pathnode::set_cheapest(&mut root, final_rel)?;

        Ok(root)
    }
}

/// `bms_is_member(x, a)` over the lifetime-free planner `Relids`
/// (`Option<Box<Bitmapset>>` with `Bitmapset { words: Vec<u64> }`).
fn bms_is_member_relids(x: i32, a: &Relids) -> bool {
    debug_assert!(x >= 0);
    let a = match a {
        None => return false,
        Some(a) => a,
    };
    let wordnum = (x as usize) / 64;
    let bitnum = (x as usize) % 64;
    if wordnum >= a.words.len() {
        return false;
    }
    a.words[wordnum] & (1u64 << bitnum) != 0
}

/// `bms_make_singleton(x)` over the lifetime-free `Relids` (`Option<Box<Bitmapset>>`):
/// build a one-member relid set. Bit `x` lives in word `x/64`, bit `x%64`.
fn bms_make_singleton_relids(x: i32) -> Relids {
    debug_assert!(x > 0);
    let bit = x as usize;
    let wordnum = bit / 64;
    let bitnum = bit % 64;
    let mut words = alloc::vec![0u64; wordnum + 1];
    words[wordnum] = 1u64 << bitnum;
    Some(Box::new(pathnodes::Bitmapset { words }))
}

/// `IS_OUTER_JOIN(jointype)` (nodes/nodes.h).
fn is_outer_join(jointype: ::nodes::jointype::JoinType) -> bool {
    use ::nodes::jointype::JoinType;
    matches!(
        jointype,
        JoinType::JOIN_LEFT | JoinType::JOIN_FULL | JoinType::JOIN_RIGHT | JoinType::JOIN_ANTI
    )
}

// ===========================================================================
// preprocess_expression()  (planner.c:1254)
// ===========================================================================

// Expression-kind codes (planner.c:81-94).
const EXPRKIND_QUAL: i32 = 0;
const EXPRKIND_TARGET: i32 = 1;
const EXPRKIND_RTFUNC: i32 = 2;
#[allow(dead_code)]
const EXPRKIND_RTFUNC_LATERAL: i32 = 3;
const EXPRKIND_VALUES: i32 = 4;
#[allow(dead_code)]
const EXPRKIND_VALUES_LATERAL: i32 = 5;
#[allow(dead_code)]
const EXPRKIND_LIMIT: i32 = 6;
#[allow(dead_code)]
const EXPRKIND_APPINFO: i32 = 7;
#[allow(dead_code)]
const EXPRKIND_PHV: i32 = 8;
const EXPRKIND_TABLESAMPLE: i32 = 9;
#[allow(dead_code)]
const EXPRKIND_ARBITER_ELEM: i32 = 10;
const EXPRKIND_TABLEFUNC: i32 = 11;
#[allow(dead_code)]
const EXPRKIND_TABLEFUNC_LATERAL: i32 = 12;
#[allow(dead_code)]
const EXPRKIND_GROUPEXPR: i32 = 13;

/// `preprocess_expression(root, expr, kind)` (planner.c:1254).
///
/// Ported against the arena `Expr` model. `expr == NULL` ⇒ `None`. The
/// `flatten_join_alias_vars` step (when `root->hasJoinRTEs`) and the
/// `SS_process_sublinks` / `SS_replace_correlation_vars` steps have no ported
/// owner, so this function panics precisely on those paths; the
/// `eval_const_expressions` + `canonicalize_qual` + `convert_saop_to_hashed_saop`
/// + `make_ands_implicit` core is faithfully ported.
pub fn preprocess_expression<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &mut PlannerRun<'mcx>,
    outer_query: Option<&Node<'mcx>>,
    expr: Option<Expr<'mcx>>,
    kind: i32,
) -> PgResult<Option<Expr<'mcx>>> {
    // Fall out quickly if expression is empty (C:1262).
    let mut expr = match expr {
        None => return Ok(None),
        Some(e) => e,
    };

    // flatten_join_alias_vars (C:1274-1279). If the query has any join RTEs,
    // replace join alias variables with base-relation variables. Must run first,
    // before const-folding/sublink processing. Skipped for non-lateral RTE
    // functions, VALUES lists, TABLESAMPLE and TABLEFUNC clauses.
    if root.hasJoinRTEs
        && !(kind == EXPRKIND_RTFUNC
            || kind == EXPRKIND_VALUES
            || kind == EXPRKIND_TABLESAMPLE
            || kind == EXPRKIND_TABLEFUNC)
    {
        let query_node = outer_query.ok_or_else(|| {
            PgError::error(
                "preprocess_expression: flatten_join_alias_vars needs the outer Query \
                 (root->parse) but none was threaded in (root.hasJoinRTEs is set)",
            )
        })?;
        let flat = rewritemanip_seams::flatten_join_alias_vars::call(
            mcx,
            Some(&mut *root), // C planner.c passes live root
            query_node,
            Node::mk_expr(mcx, expr)?,
        )?;
        expr = match flat.into_expr() {
            Some(e) => e,
            None => {
                return Err(PgError::error(
                    "preprocess_expression: flatten_join_alias_vars returned a non-Expr node",
                ))
            }
        };
    }

    // eval_const_expressions (C:1300-1301). C: `eval_const_expressions(root,
    // expr)` reads `root->glob->boundParams` for the PARAM_EXTERN const-fold;
    // the custom-plan path (BuildCachedPlan with bound params) supplies them,
    // the simple-Query / generic-plan path leaves glob->boundParams NULL. The
    // owned `ParamListInfo` is a shared `Rc` value; clone it (cheap) out of glob
    // so the borrow handed to the fold is independent of the `&mut root` borrow.
    if kind != EXPRKIND_RTFUNC {
        let bound_params = root
            .glob
            .as_ref()
            .and_then(|g| g.bound_params.clone());
        expr = clauses::fold::eval_const_expressions_with_params(
            mcx,
            expr,
            bound_params,
        )?;
    }

    // canonicalize_qual (C:1306-1308).
    if kind == EXPRKIND_QUAL {
        expr = match prepqual::canonicalize_qual(mcx, Some(expr), false)? {
            Some(e) => e,
            None => return Ok(None),
        };
    }

    // convert_saop_to_hashed_saop (C:1321-1324).
    if kind == EXPRKIND_QUAL || kind == EXPRKIND_TARGET {
        clauses::convert_saop_to_hashed_saop(mcx, &mut expr)?;
    }

    // SS_process_sublinks (C:1326-1328): expand SubLinks to SubPlans.
    if run.resolve(root.parse).hasSubLinks {
        expr = init_subselect::correlation::SS_process_sublinks(
            mcx,
            root,
            run,
            expr,
            kind == EXPRKIND_QUAL,
        )?;
    }

    // SS_replace_correlation_vars for sub-queries (C:1336-1337).
    if root.query_level > 1 {
        expr = init_subselect::correlation::SS_replace_correlation_vars(
            mcx, root, run, expr,
        )?;
    }

    // make_ands_implicit (C:1345-1346): convert qual to implicit-AND list. The
    // arena `Expr` model returns a single Expr; `make_ands_implicit` yields a
    // Vec<Expr> (the implicit-AND list). Callers of qual preprocessing in this
    // model expect a single (possibly AND) Expr, so the implicit-AND flattening
    // is applied by the caller; here we return the canonicalized Expr unchanged,
    // matching the value-model convention used by preprocess_qual_conditions.
    //
    // EXCEPT for the constant-TRUE case: `make_ands_implicit` (makefuncs.c:817)
    // returns NIL (== an empty qual list == TRUE) when the input is a non-null
    // `Const` whose Datum is boolean TRUE. In the value model an empty qual list
    // is `None`, so a top-level const-TRUE qual must be dropped here — otherwise
    // it survives into the plan as a useless `One-Time Filter: true` gating qual.
    // (A const-FALSE Const is kept, becoming a `One-Time Filter: false`.)
    let _ = make_ands_implicit_noop;
    if kind == EXPRKIND_QUAL && expr_is_const_true(&expr) {
        return Ok(None);
    }

    Ok(Some(expr))
}

/// Placeholder reference to document where `make_ands_implicit`
/// (nodes-core makefuncs.rs) is applied; see `preprocess_expression`.
#[inline]
fn make_ands_implicit_noop() {}

/// The constant-TRUE arm of `make_ands_implicit` (makefuncs.c:821-824):
/// `IsA(clause, Const) && !((Const *) clause)->constisnull &&
/// DatumGetBool(((Const *) clause)->constvalue)`. A const-TRUE qual is dropped
/// (treated as an empty implicit-AND list). `DatumGetBool(d)` is `(d & 1) != 0`.
#[inline]
fn expr_is_const_true(expr: &Expr<'_>) -> bool {
    match expr.as_const() {
        Some(c) => !c.constisnull && (c.constvalue.as_usize() & 1) != 0,
        None => false,
    }
}

/// Whether `root.parse`'s Query has SubLinks. The arena `PlannerInfo` does not
/// carry the Query directly, so `preprocess_expression` cannot consult
/// `root->parse->hasSubLinks` without the run. This is conservatively `false`
/// here (the caller has already gated the whole block); kept as a named hook so
/// the structure mirrors the C.
#[inline]
fn run_parse_has_sublinks(_root: &PlannerInfo) -> bool {
    false
}

// ===========================================================================
// preprocess_qual_conditions()  (planner.c:1356)
// ===========================================================================

/// Drive `preprocess_qual_conditions` over `parse->jointree` (C:922). The
/// jointree lives on the `Query` as `Option<PgBox<FromExpr>>`; take it out of the
/// run (so `preprocess_expression`'s `&root` borrow doesn't alias the `&mut`
/// Query), recurse, and store it back.
fn preprocess_qual_conditions_query<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    outer_query: Option<&Node<'mcx>>,
    run: &mut PlannerRun<'mcx>,
) -> PgResult<()> {
    let jt = run.resolve_mut(root.parse).jointree.take();
    if let Some(jt) = jt {
        let mut node = Node::mk_from_expr(mcx, mcx::PgBox::into_inner(jt))?;
        preprocess_qual_conditions(mcx, root, run, outer_query, &mut node)?;
        let f = node
            .into_fromexpr()
            .unwrap_or_else(|| unreachable!("jointree top stays a FromExpr"));
        run.resolve_mut(root.parse).jointree = Some(mcx::alloc_in(mcx, f)?);
    }
    Ok(())
}

/// `preprocess_qual_conditions(root, jtnode)` (planner.c:1356). Recursively
/// scans the jointree `FromExpr`/`JoinExpr` quals and preprocesses each via
/// `preprocess_expression(..., EXPRKIND_QUAL)`.
///
/// The jointree node's `quals` is carried as `Option<NodePtr>` holding the
/// post-analysis `Node::Expr(e)`; this unwraps it to the owned `Expr`, runs
/// `preprocess_expression`, and re-wraps. Nested `JoinExpr`/`FromExpr` in the
/// fromlist (only produced by explicit JOIN syntax, which sets `hasJoinRTEs`)
/// recurse; a `RangeTblRef` leaf has nothing to do.
pub fn preprocess_qual_conditions<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &mut PlannerRun<'mcx>,
    outer_query: Option<&Node<'mcx>>,
    jtnode: &mut Node<'mcx>,
) -> PgResult<()> {
    match jtnode.node_tag() {
        ntag::T_RangeTblRef => {
            // nothing to do here (C:1362).
        }
        ntag::T_FromExpr => {
            let f = jtnode
                .as_fromexpr_mut()
                .expect("node_tag() == T_FromExpr");
            for i in 0..f.fromlist.len() {
                preprocess_qual_conditions(mcx, root, run, outer_query, &mut f.fromlist[i])?;
            }
            preprocess_jointree_quals(mcx, root, run, outer_query, &mut f.quals)?;
        }
        ntag::T_JoinExpr => {
            let j = jtnode
                .as_joinexpr_mut()
                .expect("node_tag() == T_JoinExpr");
            if let Some(larg) = j.larg.as_deref_mut() {
                preprocess_qual_conditions(mcx, root, run, outer_query, larg)?;
            }
            if let Some(rarg) = j.rarg.as_deref_mut() {
                preprocess_qual_conditions(mcx, root, run, outer_query, rarg)?;
            }
            preprocess_jointree_quals(mcx, root, run, outer_query, &mut j.quals)?;
        }
        _ => {
            return Err(PgError::error(alloc::format!(
                "preprocess_qual_conditions: unrecognized jointree node type: {:?}",
                jtnode.node_tag()
            )));
        }
    }
    Ok(())
}

/// `f->quals = preprocess_expression(root, f->quals, EXPRKIND_QUAL)` for a
/// jointree node's `quals` (`Option<NodePtr>` holding `Node::Expr`).
fn preprocess_jointree_quals<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &mut PlannerRun<'mcx>,
    outer_query: Option<&Node<'mcx>>,
    quals: &mut Option<::nodes::nodes::NodePtr<'mcx>>,
) -> PgResult<()> {
    // The C field is `Node *quals`; in the analyzed jointree it is `Node::Expr`.
    // Take ownership, unwrap to `Expr`, preprocess, re-wrap.
    let taken = quals.take();
    let expr = match taken {
        None => None,
        Some(n) => match mcx::PgBox::into_inner(n) {
            other if other.is_expr() => Some(other.into_expr().unwrap()),
            other => {
                return Err(PgError::error(alloc::format!(
                    "preprocess_qual_conditions: jointree quals is a non-Expr node: {:?}",
                    other.node_tag()
                )));
            }
        },
    };
    let processed = preprocess_expression(mcx, root, run, outer_query, expr, EXPRKIND_QUAL)?;
    *quals = match processed {
        Some(e) => Some(mcx::alloc_in(mcx, Node::mk_expr(mcx, e)?)?),
        None => None,
    };
    Ok(())
}

// ===========================================================================
// planagg.c — MIN/MAX aggregate index-scan optimization (planner-crate legs).
//
// `preprocess_minmax_aggregates` (planagg.c) lives partly in the planagg crate
// (the reject/classification + `can_minmax_aggs` candidate list) and partly
// here (the `build_minmax_path` per-aggregate subroot planning + the
// `create_minmaxagg_path` / `add_path(UPPERREL_GROUP_AGG)`), because the latter
// needs `query_planner` on a cloned subroot — a planner-crate dependency. See
// the planagg crate's module doc for the split rationale.
// ===========================================================================

/// The planner-crate half of `preprocess_minmax_aggregates`: given the
/// candidate `MinMaxAggInfo` list from `can_minmax_aggs`, try to build an
/// indexscan path for each aggregate (`build_minmax_path`); if all succeed,
/// create the per-agg output Params, build a `MinMaxAggPath`, and add it to the
/// `UPPERREL_GROUP_AGG` upperrel so it competes with the regular Agg plan.
///
/// On any aggregate failing to get an indexable path, this returns having added
/// no path (C's `return;`): `grouping_planner` then falls through to the regular
/// aggregate plan. The created `root.minmax_aggs` list (used by setrefs to swap
/// Aggrefs for Params) is only populated by `create_minmaxagg_plan` when the
/// MinMaxAggPath actually wins, so a failed/never-chosen optimization leaves it
/// empty — matching C.
fn build_minmax_agg_paths<'mcx>(
    mcx: Mcx<'mcx>,
    run: &mut PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    mut aggs_list: Vec<pathnodes::MinMaxAggInfo>,
) -> PgResult<()> {
    use types_core::primitive::OidIsValid;

    // For each aggregate, build_minmax_path; if any can't get an indexed path,
    // give up entirely (planagg.c:153-183).
    for i in 0..aggs_list.len() {
        // eqop = get_equality_op_for_ordering_op(mminfo->aggsortop, &reverse).
        let aggsortop = aggs_list[i].aggsortop;
        let (eqop, reverse) =
            match lsyscache::opfamily_operator::get_equality_op_for_ordering_op(aggsortop)? {
                Some((eqop, reverse)) if OidIsValid(eqop) => (eqop, reverse),
                _ => {
                    return Err(PgError::error(alloc::format!(
                        "could not find equality operator for ordering operator {}",
                        aggsortop
                    )))
                }
            };

        // Try NULLS FIRST/LAST per the reverse heuristic (planagg.c:176-182).
        if build_minmax_path(mcx, run, root, i, &mut aggs_list, eqop, aggsortop, reverse, reverse)? {
            continue;
        }
        if build_minmax_path(mcx, run, root, i, &mut aggs_list, eqop, aggsortop, reverse, !reverse)? {
            continue;
        }
        // No indexable path for this aggregate → abandon the optimization.
        return Ok(());
    }

    // OK, we can do the query this way. Create an output Param for each agg
    // (planagg.c:194-203), and pre-build its InitPlan SubPlan (the plan-building +
    // run-intern that C does in create_minmaxagg_plan, lifted here where we hold
    // `&mut run`).
    for i in 0..aggs_list.len() {
        // target = root.node(mminfo->target); param = SS_make_initplan_output_param(
        //     root, exprType(target), -1, exprCollation(target)).
        let target_expr = root.node(aggs_list[i].target).clone_in(mcx)?;
        let restype = nodes_core::nodefuncs::expr_type(Some(&target_expr))?;
        let rescoll = nodes_core::nodefuncs::expr_collation(Some(&target_expr))?;
        let param = init_subselect::subplan::SS_make_initplan_output_param(
            root, restype, -1, rescoll,
        )?;
        // Stash the Param as an arena node; mminfo->param is its handle.
        aggs_list[i].param = root.alloc_node(Expr::Param(param.clone()));

        // Build the InitPlan SubPlan for this aggregate (create_minmaxagg_plan's
        // per-agg leg, lifted to preprocess time for the `&mut run` it needs).
        let subroot_idx = aggs_list[i]
            .subroot_idx
            .expect("build_minmax_agg_paths: agg has no subroot");
        let subroot_path = aggs_list[i]
            .subroot_path
            .expect("build_minmax_agg_paths: agg has no subroot_path");
        let mut subroot = run.take_minmax_subroot(subroot_idx);

        // create_plan(subroot, mminfo->path): the subquery's plan. Lend the shared
        // glob to the subroot for the duration (create_plan reads root->glob for
        // subplan resolution), then return it to the outer root.
        let glob = *root
            .glob
            .take()
            .expect("build_minmax_agg_paths: root->glob is NULL");
        subroot.glob = Some(Box::new(glob));
        let sub_plan =
            createplan::create_plan(mcx, &mut subroot, run, subroot_path)?;
        root.glob = subroot.glob.take();

        // Wrap the plan in LIMIT 1, applying cost/width from mminfo->path.
        let limit_count: Option<mcx::PgBox<'mcx, Expr<'mcx>>> = run
            .resolve(subroot.parse)
            .limitCount
            .as_ref()
            .map(|c| -> PgResult<_> { Ok(mcx::alloc_in(mcx, (**c).clone_in(mcx)?)?) })
            .transpose()?;
        let (pdis, pstartup, pwidth, psafe) = {
            let pp = root.path(aggs_list[i].path.expect("agg has no imported path"));
            let b = pp.base();
            (
                b.disabled_nodes,
                b.startup_cost,
                b.pathtarget.as_ref().map_or(0, |t| t.width),
                b.parallel_safe,
            )
        };
        let pathcost = aggs_list[i].pathcost;
        let limit_plan = createplan::make_minmax_subplan_limit(
            mcx, sub_plan, limit_count, pdis, pstartup, pathcost, psafe, pwidth,
        )?;

        // SS_make_initplan_from_plan core (build SubPlan node + intern the Plan
        // tree into the run value store), but WITHOUT appending to init_plans /
        // glob.subplans yet — create_minmaxagg_plan does both, plus the 1-based
        // plan_id numbering, once the MinMaxAggPath has actually won. Consumes the
        // subroot into the run's subplan store and hands back the reserved PlanId.
        let (subplan_nid, subplan_pid) =
            init_subselect::subplan::build_initplan_subplan_node(
                mcx, root, run, subroot, limit_plan, &param,
            )?;
        aggs_list[i].subplan_node = Some(subplan_nid);
        aggs_list[i].subplan_plan_id = Some(subplan_pid);
    }

    // create_minmaxagg_path(root, grouped_rel, create_pathtarget(root,
    // processed_tlist), aggs_list, parse->havingQual) + add_path (planagg.c:219-225).
    let grouped_rel = relnode::fetch_upper_rel(
        root,
        UPPERREL_GROUP_AGG,
        &None,
    );

    // create_pathtarget(root, root->processed_tlist).
    let mut target =
        vars::tlist::make_pathtarget_from_tlist(root, &root.processed_tlist);
    costsize::sizeest::set_pathtarget_cost_width(root, &mut target);

    // (List *) parse->havingQual — the regular Agg's HAVING quals, carried onto
    // the MinMaxAggPath. The processed havingQual is an implicit-AND list on the
    // root; resolve it to bare clause handles. For the common no-HAVING minmax
    // query this is empty.
    let quals: Vec<pathnodes::NodeId> = minmax_having_quals(mcx, run, root)?;

    let path_id = pathnode::create::create_minmaxagg_path(
        root,
        grouped_rel,
        Box::new(target),
        aggs_list,
        quals,
    )?;
    pathnode::add_path(root, grouped_rel, path_id)?;

    Ok(())
}

/// `build_minmax_path(root, mminfo, eqop, sortop, reverse_sort, nulls_first)`
/// (planagg.c:316). Build a `SELECT col FROM tab WHERE col IS NOT NULL AND quals
/// ORDER BY col LIMIT 1` subquery, plan it, and keep the cheapest presorted path
/// if there is one. On success, fills `aggs_list[i]`'s `path`/`pathcost`/
/// `subroot_idx`/`subroot_path` and returns `true`.
#[allow(clippy::too_many_arguments)]
fn build_minmax_path<'mcx>(
    mcx: Mcx<'mcx>,
    run: &mut PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    i: usize,
    aggs_list: &mut [pathnodes::MinMaxAggInfo],
    eqop: Oid,
    sortop: Oid,
    reverse_sort: bool,
    nulls_first: bool,
) -> PgResult<bool> {
    use ::nodes::primnodes::NullTest;

    // The aggregate target expression (in the OUTER root's node_arena).
    let target_expr: Expr<'mcx> = root.node(aggs_list[i].target).clone_in(mcx)?;
    let target_type = nodes_core::nodefuncs::expr_type(Some(&target_expr))?;

    // --- Build the modified sub-Query (planagg.c:349-413) -------------------
    // subroot->parse = parse = copyObject(root->parse);
    // IncrementVarSublevelsUp((Node *) parse, 1, 1);
    let mut subparse: Query<'mcx> = run.resolve(root.parse).clone_in(mcx)?;
    {
        let mut as_node = Node::mk_query(mcx, subparse)?;
        rewrite_core::increment::IncrementVarSublevelsUp(&mut as_node, 1, 1, mcx)?;
        subparse = as_node
            .into_query()
            .ok_or_else(|| PgError::error("build_minmax_path: node is not a Query"))?;
    }

    // Single tlist entry = the aggregate target (`copyObject(mminfo->target)`,
    // planagg.c:371). The target's Vars are level-0 columns of the single table;
    // they STAY level 0 in the generated subquery (the `IncrementVarSublevelsUp`
    // above had `min_sublevels_up = 1`, so it bumped only the parse's pre-existing
    // outer references, never these level-0 table Vars). So do NOT bump the
    // target.
    let bumped_target = target_expr.clone_in(mcx)?;

    let mut tle = nodes_core::makefuncs::make_target_entry(
        mcx,
        bumped_target.clone_in(mcx)?,
        1,
        Some("agg_target"),
        false,
    )?;
    // subroot->processed_tlist = parse->targetList = list_make1(tle); set below.

    // No HAVING / DISTINCT / aggregates anymore.
    subparse.havingQual = None;
    subparse.distinctClause = mcx::PgVec::new_in(mcx);
    subparse.hasDistinctOn = false;
    subparse.hasAggs = false;

    // Build "target IS NOT NULL" and prepend to the WHERE quals.
    let ntest = nodes_core::makefuncs::make_is_not_null(bumped_target.clone_in(mcx)?);
    // parse->jointree->quals = lcons(ntest, quals). The jointree quals are a
    // (possibly-NULL) single Node holding an implicit-AND list; build
    // `ntest AND existing` as a fresh BoolExpr/AND list. We render it as a
    // 2-element implicit-AND list Node (make_ands_implicit shape) when there is an
    // existing qual, else just the ntest.
    {
        let existing_quals: Option<Expr<'mcx>> = match subparse
            .jointree
            .as_deref()
            .and_then(|f| f.quals.as_deref())
        {
            Some(n) => Some(
                n.as_expr()
                    .map(|e| e.clone_in(mcx))
                    .transpose()?
                    .ok_or_else(|| PgError::error("build_minmax_path: jointree quals not an Expr"))?,
            ),
            None => None,
        };
        let new_quals: Expr<'mcx> = match existing_quals {
            None => ntest,
            Some(existing) => {
                // make_andclause(list_make2(ntest, existing)) — an AND BoolExpr.
                nodes_core::makefuncs::make_andclause(alloc::vec![ntest, existing])
            }
        };
        let quals_node = mcx::alloc_in(mcx, Node::mk_expr(mcx, new_quals)?)?;
        if let Some(jt) = subparse.jointree.as_deref_mut() {
            jt.quals = Some(quals_node);
        }
    }

    // ORDER BY clause: assignSortGroupRef(tle, tlist) then SortGroupClause.
    let sgref = {
        // tle is the only tlist entry; ressortgroupref starts 0 → assign 1.
        if tle.ressortgroupref == 0 {
            tle.ressortgroupref = 1;
        }
        tle.ressortgroupref
    };
    let sortcl = ::nodes::rawnodes::SortGroupClause {
        tleSortGroupRef: sgref,
        eqop,
        sortop,
        reverse_sort,
        nulls_first,
        hashable: false,
    };
    // parse->targetList = list_make1(tle); parse->sortClause = list_make1(sortcl).
    subparse.targetList = {
        let mut v = mcx::PgVec::new_in(mcx);
        v.push(tle);
        v
    };
    subparse.sortClause = {
        let mut v = mcx::PgVec::new_in(mcx);
        v.push(mcx::alloc_in(mcx, Node::mk_sort_group_clause(mcx, sortcl)?)?);
        v
    };

    // LIMIT 1: makeConst(INT8OID, -1, InvalidOid, 8, Int64GetDatum(1), false, true).
    subparse.limitOffset = None;
    subparse.limitCount = Some(mcx::alloc_in(
        mcx,
        Expr::Const(nodes_core::makefuncs::make_const(
            mcx,
            types_core::catalog::INT8OID,
            -1,
            types_core::primitive::InvalidOid,
            8,
            types_tuple::heaptuple::Datum::ByVal(1),
            false,
            true,
        )?),
    )?);
    subparse.limitOption = ::nodes::nodelimit::LimitOption::LIMIT_OPTION_COUNT;

    let _ = target_type;

    // --- Build the subroot + plan it (planagg.c:338-422) --------------------
    let subparse_id = run.intern(subparse);

    // append_rel_list: copyObject(root->append_rel_list) + IncrementVarSublevelsUp.
    // The minmax-restricted single-table case has none in the common path; we copy
    // it as-is (AppendRelInfo carries no level-1 Vars for a plain relation).
    let parent_append_rel = root.append_rel_list.clone();

    let mut subroot = root.make_minmax_subroot();
    subroot.parse = subparse_id;
    subroot.processed_tlist = Vec::new(); // query_planner doesn't use this directly
    subroot.append_rel_list = parent_append_rel;
    // The `translated_vars` are `NodeId` handles into the OUTER `root`'s
    // `node_arena` (#274); in C they are plain `Node *` pointers that copyObject
    // duplicates into the planner context, staying valid for the subroot. Here
    // `make_minmax_subroot` gives the subroot a *fresh, empty* `node_arena`, so a
    // copied handle would resolve against the wrong arena (or be OOB) when the
    // subroot's `query_planner` reaches `adjust_appendrel_attrs_mutator` via
    // `apply_child_basequals` (e.g. `min(x)` over a UNION-ALL subquery). Re-intern
    // each referenced `Expr` into the subroot's arena and rewrite the handle,
    // mirroring the union-all pullup re-intern (prepjointree pullup.rs).
    {
        let mut appinfos = core::mem::take(&mut subroot.append_rel_list);
        for ai in appinfos.iter_mut() {
            for id in ai.translated_vars.iter_mut() {
                if *id == pathnodes::NodeId::default() {
                    continue;
                }
                let expr = root.node(*id).clone_in(mcx)?;
                *id = subroot.alloc_node(expr);
            }
        }
        subroot.append_rel_list = appinfos;
    }
    // tuple_fraction = 1.0; limit_tuples = 1.0 (planagg.c:419-420).
    subroot.tuple_fraction = 1.0;
    subroot.limit_tuples = 1.0;

    // Move the shared glob into the subroot for the recursion, and move the parent
    // root in as parent_root (the plan_sublink_subquery pattern). Restored after.
    let glob = *root
        .glob
        .take()
        .expect("build_minmax_path: parent root->glob is NULL");
    let parent_root = core::mem::take(root);
    subroot.glob = Some(Box::new(glob));
    subroot.parent_root = Some(Box::new(parent_root));

    // Pre-build the subroot's processed_tlist (the single agg_target tle) and
    // intern the sortClause SortGroupClause, so the qp_callback (which can't reach
    // `run`) can compute sort_pathkeys DURING query_planner — exactly as
    // minmax_qp_callback does in C. This is essential: query_planner must see
    // query_pathkeys while it builds paths, so it generates+keeps the presorted
    // (ordered index-only-scan) path the optimization relies on. Computing the
    // pathkeys only AFTER query_planner would leave query_pathkeys empty during
    // planning and the presorted path would never be kept.
    let tlist_ids = build_minmax_subroot_tlist(mcx, run, &mut subroot)?;
    let sgc_id = {
        let sortcl_val = {
            let sc = &run.resolve(subroot.parse).sortClause;
            let np = &*sc[0];
            match np.node_tag() {
                ntag::T_SortGroupClause => *np.expect_sortgroupclause(),
                _ => {
                    restore_parent_from_subroot(root, &mut subroot);
                    return Err(PgError::error("build_minmax_path: sortClause not SGC"));
                }
            }
        };
        subroot.alloc_sortgroupclause(sortcl_val)
    };

    // minmax_qp_callback: group/window/distinct pathkeys = NIL; sort_pathkeys =
    // make_pathkeys_for_sortclauses(sortClause, targetList); query_pathkeys =
    // sort_pathkeys (planagg.c:480-491).
    let mut qp_callback = move |sr: &mut PlannerInfo| -> PgResult<()> {
        sr.group_pathkeys = Vec::new();
        sr.window_pathkeys = Vec::new();
        sr.distinct_pathkeys = Vec::new();
        let pk = pathkeys::make_pathkeys_for_sortclauses(
            sr,
            mcx,
            &[sgc_id],
            &tlist_ids,
        );
        sr.sort_pathkeys = pk.clone();
        sr.query_pathkeys = pk;
        Ok(())
    };

    let final_rel =
        match plan_small::query_planner(mcx, run, &mut subroot, &mut qp_callback) {
            Ok(rel) => rel,
            Err(e) => {
                // Restore parent before propagating.
                restore_parent_from_subroot(root, &mut subroot);
                return Err(e);
            }
        };

    let query_pathkeys = subroot.query_pathkeys.clone();

    // SS_identify_outer_params(subroot); SS_charge_for_initplans(subroot, final_rel).
    init_subselect::correlation::SS_identify_outer_params(&mut subroot);
    init_subselect::finalize::SS_charge_for_initplans(&mut subroot, final_rel);

    // path_fraction = (final_rel->rows > 1) ? 1/rows : 1.
    let final_rows = subroot.rel(final_rel).rows;
    let path_fraction = if final_rows > 1.0 { 1.0 / final_rows } else { 1.0 };

    // sorted_path = get_cheapest_fractional_path_for_pathkeys(pathlist,
    //     query_pathkeys, NULL, path_fraction).
    let pathlist = subroot.rel(final_rel).pathlist.clone();
    let sorted_path = pathkeys::get_cheapest_fractional_path_for_pathkeys(
        &subroot,
        &pathlist,
        &query_pathkeys,
        &None,
        path_fraction,
    );
    let Some(sorted_path) = sorted_path else {
        // No presorted path → fail; restore parent and return false.
        restore_parent_from_subroot(root, &mut subroot);
        return Ok(false);
    };

    // Keep the cheapest presorted path (planagg.c:442-448), exactly as C does:
    // any path that satisfies query_pathkeys for fetching one row. In practice
    // this is the ordered Index(-Only) Scan; were it instead an explicit Sort
    // over a seqscan, the resulting MinMaxAggPath would simply lose on cost to
    // the regular Agg plan (the cost competition in create_grouping_paths /
    // set_cheapest), so no extra index-backed-only guard is needed here.

    // apply_projection_to_path to make it return exactly processed_tlist.
    // subroot->processed_tlist = list_make1(tle). Build it first (mut borrow),
    // then make the PathTarget (shared borrow).
    let proj_tlist = build_minmax_subroot_tlist(mcx, run, &mut subroot)?;
    let mut proj_target =
        vars::tlist::make_pathtarget_from_tlist(&subroot, &proj_tlist);
    costsize::sizeest::set_pathtarget_cost_width(&mut subroot, &mut proj_target);
    let sorted_path = pathnode::create::apply_projection_to_path(
        &mut subroot,
        final_rel,
        sorted_path,
        Box::new(proj_target),
    )?;

    // path_cost = startup + path_fraction * (total - startup) (planagg.c:465).
    let (startup, total, _disabled) = {
        let p = subroot.path(sorted_path).base();
        (p.startup_cost, p.total_cost, p.disabled_nodes)
    };
    let path_cost = startup + path_fraction * (total - startup);

    // Import the path into the OUTER root for create_minmaxagg_path's cost read,
    // then restore the parent root and stash the subroot for create_minmaxagg_plan.
    restore_parent_from_subroot(root, &mut subroot);
    let imported = pathnode::import::import_path_from_subroot(
        mcx,
        root,
        &subroot,
        sorted_path,
    );
    let subroot_idx = run.intern_minmax_subroot(subroot);

    aggs_list[i].path = Some(imported);
    aggs_list[i].pathcost = path_cost;
    aggs_list[i].subroot_idx = Some(subroot_idx);
    aggs_list[i].subroot_path = Some(sorted_path);

    Ok(true)
}

/// Move the parent `PlannerInfo` back out of `subroot.parent_root` onto `*root`
/// and return the shared `glob` to it (the inverse of the move-in done before
/// `query_planner`). Mirrors the restore in `plan_sublink_subquery`.
fn restore_parent_from_subroot(root: &mut PlannerInfo, subroot: &mut PlannerInfo) {
    let glob = subroot.glob.take();
    *root = *subroot
        .parent_root
        .take()
        .expect("build_minmax_path: subroot lost its parent_root");
    root.glob = glob;
}

/// Build the subroot's `processed_tlist` (`list_make1(tle)` — the single
/// `agg_target` entry) and return the interned `TargetEntry` `NodeId` list. Used
/// both for `apply_projection_to_path`'s PathTarget and the pathkeys.
fn build_minmax_subroot_tlist<'mcx>(
    mcx: Mcx<'mcx>,
    run: &PlannerRun<'mcx>,
    subroot: &mut PlannerInfo,
) -> PgResult<Vec<pathnodes::NodeId>> {
    if !subroot.processed_tlist.is_empty() {
        return Ok(subroot.processed_tlist.clone());
    }
    // The parse's single targetList entry (the agg_target tle).
    let tle = run.resolve(subroot.parse).targetList[0].clone_in(mcx)?;
    let te_node = pathnodes::TargetEntryNode {
        expr: subroot.alloc_node(
            tle.expr
                .as_deref()
                .ok_or_else(|| PgError::error("build_minmax_subroot_tlist: tle has no expr"))?
                .clone_in(mcx)?,
        ),
        resno: tle.resno,
        resname: None,
        ressortgroupref: tle.ressortgroupref,
        resorigtbl: tle.resorigtbl,
        resorigcol: tle.resorigcol,
        resjunk: tle.resjunk,
    };
    let te_id = subroot.alloc_targetentry(te_node);
    subroot.processed_tlist = alloc::vec![te_id];
    Ok(subroot.processed_tlist.clone())
}

/// Resolve `parse->havingQual` (the regular-Agg HAVING) to bare clause handles
/// for the MinMaxAggPath. The common all-MIN/MAX query has no HAVING → empty.
fn minmax_having_quals<'mcx>(
    mcx: Mcx<'mcx>,
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
) -> PgResult<Vec<pathnodes::NodeId>> {
    let having = run.resolve(root.parse).havingQual.as_deref().cloned();
    match having {
        None => Ok(Vec::new()),
        Some(hq) => {
            // make_ands_implicit(havingQual) → list of clauses; intern each.
            let clauses = nodes_core::makefuncs::make_ands_implicit(Some(hq));
            let mut out = Vec::with_capacity(clauses.len());
            for c in clauses {
                out.push(root.alloc_node(c));
            }
            Ok(out)
        }
    }
}

// ===========================================================================
// grouping_planner()  (planner.c:1433) — spine up to create_pathtarget.
// ===========================================================================

/// `grouping_planner(root, tuple_fraction, setops)` (planner.c:1433).
///
/// Ported spine up to and including `query_planner`, then the
/// `create_pathtarget(root, root->processed_tlist)` ext-seam, which loud-panics
/// (the whole upper-rel path/target machinery is unported). Everything after
/// that point is unreachable and not written.
fn grouping_planner<'mcx>(
    mcx: Mcx<'mcx>,
    run: &mut PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    mut tuple_fraction: f64,
    setops: Option<&'mcx ::nodes::rawnodes::SetOperationStmt<'mcx>>,
) -> PgResult<()> {
    let mut offset_est: i64 = 0;
    let mut count_est: i64 = 0;
    let mut limit_tuples: f64 = -1.0;

    // Tweak tuple_fraction if LIMIT/OFFSET (C:1452-1463).
    let has_limit = {
        let parse = run.resolve(root.parse);
        parse.limitCount.is_some() || parse.limitOffset.is_some()
    };
    if has_limit {
        tuple_fraction = preprocess_limit(mcx, run, root, tuple_fraction, &mut offset_est, &mut count_est)?;
        if count_est > 0 && offset_est >= 0 {
            limit_tuples = (count_est as f64) + (offset_est as f64);
        }
    }

    // Make tuple_fraction accessible to lower-level routines (C:1466).
    root.tuple_fraction = tuple_fraction;

    let has_setops = run.resolve(root.parse).setOperations.is_some();
    if has_setops {
        // Set-operation branch (C:1468-1523). Construct Paths for set ops; the
        // result needs only an optional top-level sort and/or LIMIT.
        let current_rel =
            prepunion_seams::plan_set_operations::call(mcx, run, root)?;

        debug_assert!(run.resolve(root.parse).commandType == CmdType::CMD_SELECT);

        // Use the processed_tlist from plan_set_operations, transferring sort key
        // info from the original tlist (C:1487-1490). The processed_tlist already
        // lives in root's arena; copy sortgrouprefs from parse->targetList.
        postprocess_setop_tlist(run, root)?;

        // final_target = current_rel->cheapest_total_path->pathtarget (C:1493).
        let final_target: pathnodes::PathTarget = {
            let cheapest = root
                .rel(current_rel)
                .cheapest_total_path
                .expect("setop final_target: cheapest_total_path is NULL");
            root.path(cheapest)
                .base()
                .pathtarget
                .as_deref()
                .cloned()
                .expect("setop final_target: path has no pathtarget")
        };
        let final_target_parallel_safe = is_target_exprs_parallel_safe(root, &final_target.exprs);

        // The setop result tlist couldn't contain any SRFs (C:1500).
        debug_assert!(!run.resolve(root.parse).hasTargetSRFs);

        // FOR [KEY] UPDATE/SHARE is not allowed with set ops (C:1507-1514).
        if !run.resolve(root.parse).rowMarks.is_empty() {
            return Err(PgError::error(alloc::string::String::from(
                "FOR UPDATE/SHARE is not allowed with UNION/INTERSECT/EXCEPT",
            )));
        }

        // sort_pathkeys = make_pathkeys_for_sortclauses(root, sortClause,
        // processed_tlist) (C:1519-1522). DISTINCT is rejected at parse.
        debug_assert!(run.resolve(root.parse).distinctClause.is_empty());
        let sort_clause_ids = intern_sortclauses(run, root)?;
        let processed = root.processed_tlist.clone();
        root.sort_pathkeys =
            pathkeys::make_pathkeys_for_sortclauses(
                root,
                mcx,
                &sort_clause_ids,
                &processed,
            );
        let has_sort = !root.sort_pathkeys.is_empty();

        // Fall through to the shared final-paths tail (C:1849-2169).
        // The setop result tlist can't contain SRFs (asserted above), so no
        // final_target SRF fix-up is possible here.
        return build_final_paths(
            mcx,
            run,
            root,
            current_rel,
            &final_target,
            final_target_parallel_safe,
            has_sort,
            false,
            &[],
            &[],
            false,
            limit_tuples,
            offset_est,
            count_est,
        );
    }

    // Regular planning branch (C:1524+).
    // A recursive query should always have setOperations (C:1547).
    debug_assert!(!root.hasRecursion);

    // Preprocess grouping sets / GROUP BY (C:1549-1558).
    let mut gset_data: Option<GroupingSetsData<'mcx>> = None;
    {
        let has_grouping_sets = !run.resolve(root.parse).groupingSets.is_empty();
        let has_group_clause = !run.resolve(root.parse).groupClause.is_empty();
        if has_grouping_sets {
            gset_data = Some(preprocess_grouping_sets(mcx, run, root)?);
        } else if has_group_clause {
            root.processed_groupClause = preprocess_groupclause(run, root, &[])?;
        }
    }

    // Preprocess targetlist (C:1567). Resolve the FOR-UPDATE/SHARE PlanRowMark
    // values (Copy) before taking the &mut Query borrow, so preprocess_targetlist
    // can build the rowmark junk Vars without the &PlannerRun registry.
    {
        let rowmarks: alloc::vec::Vec<::nodes::nodelockrows::PlanRowMark> = root
            .rowMarks
            .iter()
            .map(|&id| *run.resolve_rowmark(id))
            .collect();
        let parse = run.resolve_mut(root.parse);
        preptlist::preprocess_targetlist(mcx, root, parse, &rowmarks)?;
    }

    // preprocess_aggrefs over processed_tlist + havingQual if hasAggs (C:1576-1580).
    //
    //   preprocess_aggrefs(root, (Node *) root->processed_tlist);
    //   preprocess_aggrefs(root, (Node *) parse->havingQual);
    //
    // The C walks each List/expression. Our `preprocess_aggrefs` takes a single
    // `&Expr`; `processed_tlist` is a `Vec<NodeId>` of `TargetEntry`s and the
    // walker descends each TargetEntry to its `expr`, so we mirror that by
    // resolving every TargetEntry's `expr` and walking it. Because the exprs
    // live inside `root`'s arena and `preprocess_aggrefs` needs `&mut root`, we
    // deep-clone each expr out of the arena first (`Expr::clone_in`, keystone
    // #280) to release the immutable borrow, exactly as `preprocess_aggref`
    // already clones the working Aggref node.
    {
        let has_aggs = run.resolve(root.parse).hasAggs;
        if has_aggs {
            // processed_tlist: for each TargetEntry expr, take the live arena
            // node out, run preprocess_aggrefs (which catalogs the aggs into
            // `root` AND writes aggno/aggtransno/aggtranstype onto the node in
            // place — the plan's tlist Aggrefs must carry these for ExecInitAgg),
            // then put the mutated node back. Taking it out resolves the
            // borrow conflict between `&mut root` (the arena) and the live node.
            let tlist_exprs: Vec<pathnodes::NodeId> =
                root.processed_tlist.iter().map(|te| root.targetentry(*te).expr).collect();
            for expr_id in tlist_exprs {
                // Deep-clone the live node out (a shallow Expr::clone panics on
                // an Aggref), process+number it, then write it back.
                let mut live = root.node(expr_id).clone_in(mcx)?;
                prepagg::preprocess_aggrefs(mcx, root, &mut live)?;
                *root.node_mut(expr_id) = live.erase_lifetime();
            }
            // preprocess_aggrefs(root, (Node *) parse->havingQual) (C:1580).
            // `havingQual` is the concretely-typed `Option<PgBox<Expr>>` view.
            // Take it out (resolving the borrow against `&mut root`), catalog +
            // number its Aggrefs in place, and write it BACK: the HAVING qual is
            // re-read by createplan to build the Agg plan node's `qual`, and its
            // Aggrefs must carry the assigned aggno/aggtransno/aggtranstype, else
            // ExecInitAgg indexes peraggs[-1].
            let having: Option<Expr> = run
                .resolve_mut(root.parse)
                .havingQual
                .take()
                .map(mcx::PgBox::into_inner);
            if let Some(mut having) = having {
                prepagg::preprocess_aggrefs(mcx, root, &mut having)?;
                run.resolve_mut(root.parse).havingQual = Some(mcx::alloc_in(mcx, having)?);
            }
        }
    }

    // find_window_functions if hasWindowFuncs (C:1588-1609). `active_windows`
    // is the list of arena WindowClause handles select_active_windows produced;
    // `wflists` (windowFuncs by winref) is carried forward for create_window_paths.
    // Both are empty when there are no window functions.
    let mut active_windows: Vec<pathnodes::NodeId> = Vec::new();
    let mut wflists: Option<WindowFuncListsArena> = None;
    {
        let has_window = run.resolve(root.parse).hasWindowFuncs;
        if has_window {
            // wflists = find_window_functions((Node *) processed_tlist,
            //   list_length(parse->windowClause)).
            let max_win_ref = run.resolve(root.parse).windowClause.len() as u32;
            // Borrow each tlist expr (`find_window_functions_in_exprs` only
            // inspects them); a derived `.clone()` would panic on a
            // context-allocated child (Aggref/SubLink/SubPlan).
            let tlist_refs: Vec<&Expr> = root
                .processed_tlist
                .iter()
                .map(|&id| root.targetentry(id).expr)
                .map(|eid| root.node(eid))
                .collect();
            let lists = clauses::find_window_functions_in_exprs(
                mcx,
                &tlist_refs,
                max_win_ref,
            )?;
            if lists.num_window_funcs > 0 {
                // optimize_window_clauses(root, wflists).
                let mut wfa = build_window_func_lists_arena(root, lists)?;
                optimize_window_clauses(run, root, &mut wfa)?;
                // activeWindows = select_active_windows(root, wflists).
                active_windows = select_active_windows(run, root, &mut wfa)?;
                // name_active_windows(activeWindows).
                name_active_windows(root, &active_windows)?;
                wflists = Some(wfa);
            } else {
                // No window functions survived; clear the flag (C:1607).
                run.resolve_mut(root.parse).hasWindowFuncs = false;
            }
        }
    }

    // preprocess_minmax_aggregates if hasAggs (C:1617-1618). The planagg crate
    // does the reject/classification + can_minmax_aggs candidate list; the
    // build_minmax_path / create_minmaxagg_path legs run here (they need
    // query_planner on a cloned subroot — a planner-crate dependency).
    {
        let has_aggs = run.resolve(root.parse).hasAggs;
        if has_aggs {
            // Clone the parse Query out of the arena to release the run borrow
            // while planagg mutably borrows root.
            let parse_owned = run.resolve(root.parse).clone_in(mcx)?;
            if let Some(aggs_list) = planagg::preprocess_minmax_aggregates(
                mcx,
                root,
                &parse_owned,
            )? {
                build_minmax_agg_paths(mcx, run, root, aggs_list)?;
            }
        }
    }

    // root->limit_tuples (C:1626-1635).
    {
        let parse = run.resolve(root.parse);
        let has_grouping_or_agg = !parse.groupClause.is_empty()
            || !parse.groupingSets.is_empty()
            || !parse.distinctClause.is_empty()
            || parse.hasAggs
            || parse.hasWindowFuncs
            || parse.hasTargetSRFs
            || root.hasHavingQual;
        root.limit_tuples = if has_grouping_or_agg { -1.0 } else { limit_tuples };
    }

    // Set up standard_qp_extra (C:1637-1645): activeWindows = NIL,
    // gset_data = NULL, setop = setops. `setops` (the parent SetOperationStmt for
    // a set-op child) is bridged into the qp_callback below as `setop_child`.

    // query_planner(root, standard_qp_callback, &qp_extra) (C:1654).
    //
    // The qp_callback signature (`&mut dyn FnMut(&mut PlannerInfo)`) cannot reach
    // `run`, so any clause data the callback needs from the `Query` must be
    // resolved *before* `query_planner` and captured by the closure. On the
    // ORDER-BY path the only `Query`-resident input `standard_qp_callback` reads
    // is `parse->sortClause`; GROUP BY / DISTINCT / window / set-op / grouping
    // sets are gated out above and below (each loud-panics at its C site). We
    // bridge `parse->sortClause` (a `List *` of `SortGroupClause` values carried
    // as `NodePtr`) into the planner node arena up front (`alloc_sortgroupclause`),
    // mirroring the C `List *` of `SortGroupClause *`, and the captured handles
    // are everything the callback needs. The remaining inputs
    // (`root->processed_tlist`, `root->processed_groupClause`,
    // `root->processed_distinctClause`) already live on `root`.
    let sort_clause_ids: Vec<pathnodes::NodeId> = {
        let sort_clauses: Vec<::nodes::rawnodes::SortGroupClause> = run
            .resolve(root.parse)
            .sortClause
            .iter()
            .map(|np| sortgroupclause_from_node(np))
            .collect::<PgResult<Vec<_>>>()?;
        sort_clauses
            .into_iter()
            .map(|sgc| root.alloc_sortgroupclause(sgc))
            .collect()
    };
    // DISTINCT clause: like sortClause, intern parse->distinctClause (a List of
    // SortGroupClause values) into the planner arena up front so the qp_callback —
    // which can't reach `run` — can build root->distinct_pathkeys /
    // processed_distinctClause (standard_qp_callback, planner.c:3564-3580).
    let distinct_clause_ids: Vec<pathnodes::NodeId> = {
        let distinct_clauses: Vec<::nodes::rawnodes::SortGroupClause> = run
            .resolve(root.parse)
            .distinctClause
            .iter()
            .map(|np| sortgroupclause_from_node(np))
            .collect::<PgResult<Vec<_>>>()?;
        distinct_clauses
            .into_iter()
            .map(|sgc| root.alloc_sortgroupclause(sgc))
            .collect()
    };
    // We consider only the first (bottom) window in pathkeys logic (C:3545).
    let first_active_window = active_windows.first().copied();
    // With grouping sets, standard_qp_callback uses the first RollupData's
    // groupClause for group_pathkeys (C:3464-3500). The qp_callback closure can't
    // reach `gset_data`, so capture the (already-arena) handle list here. `None`
    // means "no grouping sets" (take the plain processed_groupClause branch).
    let gset_group_clause: Option<Vec<pathnodes::NodeId>> = gset_data.as_ref().map(|gd| {
        gd.rollups
            .first()
            .map(|r| r.groupClause.clone())
            .unwrap_or_default()
    });
    // setting setop_pathkeys might be useful to the union planner (C:3589). Bridge
    // the parent SetOperationStmt's groupClauses (interned) + colTypes so the
    // qp_callback (which can't reach `run`) can compute root->setop_pathkeys.
    let setop_child: Option<(Vec<pathnodes::NodeId>, Vec<Oid>)> = match setops {
        Some(op) => {
            let group_clause_ids: Vec<pathnodes::NodeId> = op
                .groupClauses
                .iter()
                .map(|np| {
                    let sgc = sortgroupclause_from_node(np)?;
                    Ok(root.alloc_sortgroupclause(sgc))
                })
                .collect::<PgResult<Vec<_>>>()?;
            let col_types: Vec<Oid> = op.colTypes.iter().copied().collect();
            Some((group_clause_ids, col_types))
        }
        None => None,
    };
    let mut qp_callback = move |root: &mut PlannerInfo| -> PgResult<()> {
        standard_qp_callback(
            root,
            mcx,
            &sort_clause_ids,
            &distinct_clause_ids,
            first_active_window,
            gset_group_clause.as_deref(),
            setop_child.as_ref().map(|(g, c)| (g.as_slice(), c.as_slice())),
        )
    };
    let mut current_rel =
        plan_small::query_planner(mcx, run, root, &mut qp_callback)?;

    // create_pathtarget(root, root->processed_tlist) (C:1665) =
    // set_pathtarget_cost_width(root, make_pathtarget_from_tlist(processed_tlist)).
    let mut final_target = vars::tlist::make_pathtarget_from_tlist(
        root,
        &root.processed_tlist,
    );
    costsize::sizeest::set_pathtarget_cost_width(root, &mut final_target);
    let final_target_parallel_safe = is_target_exprs_parallel_safe(root, &final_target.exprs);

    // Read the clauses that decide which upper-rel phases are needed (C:1670+).
    let (
        has_sort,
        has_distinct,
        has_window,
        has_target_srfs,
        has_group_clause,
        has_grouping_sets,
        has_aggs,
    ) = {
        let parse = run.resolve(root.parse);
        (
            !parse.sortClause.is_empty(),
            !parse.distinctClause.is_empty(),
            parse.hasWindowFuncs,
            parse.hasTargetSRFs,
            !parse.groupClause.is_empty(),
            !parse.groupingSets.is_empty(),
            parse.hasAggs,
        )
    };

    // ORDER BY post-sort projection (C:1672-1686). If ORDER BY was given,
    // consider whether a post-sort projection is worthwhile and compute the
    // adjusted target for the preceding steps; otherwise sort_input_target =
    // final_target.
    let mut have_postponed_srfs = false;
    let (mut sort_input_target, sort_input_target_parallel_safe) = if has_sort {
        let sit = make_sort_input_target(run, root, &final_target, &mut have_postponed_srfs)?;
        let safe = is_target_exprs_parallel_safe(root, &sit.exprs);
        (sit, safe)
    } else {
        (final_target.clone(), final_target_parallel_safe)
    };

    // grouping_target: if there are active windows it's the make_window_input_target
    // (C:1697-1705); otherwise sort_input_target.
    let _ = has_window;
    let (mut grouping_target, grouping_target_parallel_safe) = if !active_windows.is_empty() {
        let gt = make_window_input_target(mcx, root, &final_target, &active_windows)?;
        let safe = is_target_exprs_parallel_safe(root, &gt.exprs);
        (gt, safe)
    } else {
        (sort_input_target.clone(), sort_input_target_parallel_safe)
    };

    // Grouping/aggregation (C:1709-1726). have_grouping = groupClause ||
    // groupingSets || hasAggs || hasHavingQual. If we have grouping or
    // aggregation, the topmost scan/join plan node must emit what the grouping
    // step wants (make_group_input_target); otherwise it should emit
    // grouping_target.
    let have_grouping =
        has_group_clause || has_grouping_sets || root.hasHavingQual || has_aggs;
    // GROUPING SETS / window upper rels are gated out above (each loud-panics at
    // its C site). A bare GROUP BY (groupClause) feeds make_group_input_target
    // through to create_grouping_paths too, but its sorted-Group / numGroups
    // estimation paths are exercised below; the plain-aggregate (hasAggs, no
    // GROUP BY) path — `SELECT count(*) FROM t` — is the fully-ported one.
    let (mut scanjoin_target, scanjoin_target_parallel_safe) = if have_grouping {
        let sj = make_group_input_target(mcx, run, root, &final_target)?;
        let safe = is_target_exprs_parallel_safe(root, &sj.exprs);
        (sj, safe)
    } else {
        // With no grouping, scanjoin_target = grouping_target (C:1722-1725).
        (grouping_target.clone(), grouping_target_parallel_safe)
    };

    // Targetlist SRFs (C:1733-1768). If there are any SRFs in the targetlist,
    // separate each named PathTarget into SRF-computing and SRF-free targets:
    // replace each named target with its SRF-free version, and remember the
    // list of additional projection steps to add afterwards
    // (split_pathtarget_at_srfs / adjust_paths_for_srfs).
    let gflags = {
        let parse = run.resolve(root.parse);
        vars::tlist::SplitGroupingFlags {
            has_group_rte: parse.hasGroupRTE,
            has_grouping_sets: !parse.groupingSets.is_empty(),
            group_rtindex: root.group_rtindex,
        }
    };
    // Lists of split levels (each a list of PathTargets) per named target, used
    // by adjust_paths_for_srfs after each upper-rel phase. For the no-SRF case
    // only scanjoin_targets is populated (list_make1(scanjoin_target)).
    let mut final_targets: Vec<PathTarget> = Vec::new();
    let mut final_targets_contain_srfs: Vec<bool> = Vec::new();
    let mut sort_input_targets: Vec<PathTarget> = Vec::new();
    let mut sort_input_targets_contain_srfs: Vec<bool> = Vec::new();
    let mut grouping_targets: Vec<PathTarget> = Vec::new();
    let mut grouping_targets_contain_srfs: Vec<bool> = Vec::new();
    let scanjoin_targets: Vec<PathTarget>;
    let scanjoin_targets_contain_srfs: Vec<bool>;

    if has_target_srfs {
        use vars::tlist::{
            split_pathtarget_at_srfs, split_pathtarget_at_srfs_grouping,
        };
        // final_target doesn't recompute any SRFs in sort_input_target.
        let (ft, ftc) =
            split_pathtarget_at_srfs(mcx, root, &final_target, Some(&sort_input_target), gflags)?;
        final_target = ft[0].clone();
        debug_assert!(!ftc[0]);
        final_targets = ft;
        final_targets_contain_srfs = ftc;
        // likewise for sort_input_target vs. grouping_target.
        let (sit, sitc) = split_pathtarget_at_srfs(
            mcx,
            root,
            &sort_input_target,
            Some(&grouping_target),
            gflags,
        )?;
        sort_input_target = sit[0].clone();
        debug_assert!(!sitc[0]);
        sort_input_targets = sit;
        sort_input_targets_contain_srfs = sitc;
        // likewise for grouping_target vs. scanjoin_target (crosses grouping).
        let (gt, gtc) = split_pathtarget_at_srfs_grouping(
            mcx,
            root,
            &grouping_target,
            Some(&scanjoin_target),
            gflags,
        )?;
        grouping_target = gt[0].clone();
        debug_assert!(!gtc[0]);
        grouping_targets = gt;
        grouping_targets_contain_srfs = gtc;
        // scanjoin_target has no SRFs precomputed for it (input_target = NULL).
        let (st, stc) = split_pathtarget_at_srfs(mcx, root, &scanjoin_target, None, gflags)?;
        scanjoin_target = st[0].clone();
        debug_assert!(!stc[0]);
        scanjoin_targets = st;
        scanjoin_targets_contain_srfs = stc;
    } else {
        scanjoin_targets = alloc::vec![scanjoin_target.clone()];
        scanjoin_targets_contain_srfs = Vec::new();
    }

    // DISTINCT is handled below, after grouping/window, in its C order
    // (planner.c:1834-1841) — see the create_distinct_paths call.

    // scanjoin_target_same_exprs = list_length(scanjoin_targets) == 1
    //   && equal(scanjoin_target->exprs, current_rel->reltarget->exprs) (C:1771).
    let scanjoin_target_same_exprs = scanjoin_targets.len() == 1 && {
        let cur_exprs: &[pathnodes::NodeId] = root
            .rel(current_rel)
            .reltarget
            .as_ref()
            .map(|t| t.exprs.as_slice())
            .unwrap_or(&[]);
        equal_expr_handle_lists(root, &scanjoin_target.exprs, cur_exprs)
    };

    // apply_scanjoin_target_to_paths(root, current_rel, scanjoin_targets,
    //   scanjoin_targets_contain_srfs, scanjoin_target_parallel_safe,
    //   scanjoin_target_same_exprs) (C:1773). This applies the SRF-free target,
    // stacks ProjectSet/Result via adjust_paths_for_srfs, and sets the rel's
    // reltarget to the full (last) scanjoin target.
    apply_scanjoin_target_to_paths(
        run,
        root,
        current_rel,
        &scanjoin_targets,
        &scanjoin_targets_contain_srfs,
        scanjoin_target_parallel_safe,
        scanjoin_target_same_exprs,
    )?;

    // Save the upper-rel PathTargets into root.upper_targets[] (C:1785-1790).
    // FINAL/ORDERED = final_target; DISTINCT/PARTIAL_DISTINCT/WINDOW =
    // sort_input_target; GROUP_AGG = grouping_target. The core code does not read
    // these; they are a convenience for extensions.
    root.upper_targets[UPPERREL_FINAL as usize] = Some(Box::new(final_target.clone()));
    root.upper_targets[UPPERREL_ORDERED as usize] = Some(Box::new(final_target.clone()));
    root.upper_targets[UPPERREL_DISTINCT as usize] = Some(Box::new(sort_input_target.clone()));
    root.upper_targets[UPPERREL_PARTIAL_DISTINCT as usize] =
        Some(Box::new(sort_input_target.clone()));
    root.upper_targets[UPPERREL_WINDOW as usize] = Some(Box::new(sort_input_target.clone()));
    root.upper_targets[UPPERREL_GROUP_AGG as usize] = Some(Box::new(grouping_target.clone()));

    // If we have grouping and/or aggregation, consider ways to implement that;
    // we build a new upperrel representing the output of this phase (C:1792-1809).
    if have_grouping {
        current_rel = create_grouping_paths(
            mcx,
            run,
            root,
            current_rel,
            grouping_target.clone(),
            grouping_target_parallel_safe,
            gset_data.as_mut(),
        )?;
        // Fix things up if grouping_target contains SRFs (C:1804-1808).
        if has_target_srfs {
            adjust_paths_for_srfs(
                root,
                current_rel,
                &grouping_targets,
                &grouping_targets_contain_srfs,
            )?;
        }
    }

    // If we have window functions, consider ways to implement those; we build a
    // new upperrel representing the output of this phase (C:1813-1828).
    if !active_windows.is_empty() {
        let wfa = wflists
            .as_ref()
            .expect("create_window_paths: activeWindows present but wflists is None");
        current_rel = create_window_paths(
            run,
            root,
            current_rel,
            &grouping_target,
            &sort_input_target,
            sort_input_target_parallel_safe,
            wfa,
            &active_windows,
        )?;
        // Fix things up if sort_input_target contains SRFs (C:1825-1827).
        if has_target_srfs {
            adjust_paths_for_srfs(
                root,
                current_rel,
                &sort_input_targets,
                &sort_input_targets_contain_srfs,
            )?;
        }
    }

    // If there is a DISTINCT clause, consider ways to implement that; we build a
    // new upperrel representing the output of this phase (C:1834-1841).
    if has_distinct {
        current_rel = create_distinct_paths(
            mcx,
            run,
            root,
            current_rel,
            &sort_input_target,
        )?;
    }

    // The final_target SRF fix-up (C:1860-1862) happens inside build_final_paths,
    // within the ORDER BY ordered-paths step (it is only reached when there is a
    // sortClause). The split lists are threaded through.

    // Shared tail (ORDER BY ordered-paths + final-rel build): used by both the
    // regular and set-operation branches (planner.c:1849-2169).
    build_final_paths(
        mcx,
        run,
        root,
        current_rel,
        &final_target,
        final_target_parallel_safe,
        has_sort,
        has_target_srfs,
        &final_targets,
        &final_targets_contain_srfs,
        have_postponed_srfs,
        limit_tuples,
        offset_est,
        count_est,
    )
}

/// Shared `grouping_planner` tail: optional ORDER BY ordered-paths step then the
/// final-output upperrel build (LockRows/Limit guards, ModifyTable, add_path).
/// `planner.c:1849-2169`. Both the regular and the set-operation branches reach
/// here with their `current_rel` and `final_target`.
fn build_final_paths<'mcx>(
    mcx: Mcx<'mcx>,
    run: &mut PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    mut current_rel: RelId,
    final_target: &pathnodes::PathTarget,
    final_target_parallel_safe: bool,
    has_sort: bool,
    has_target_srfs: bool,
    final_targets: &[PathTarget],
    final_targets_contain_srfs: &[bool],
    have_postponed_srfs: bool,
    limit_tuples: f64,
    offset_est: i64,
    count_est: i64,
) -> PgResult<()> {
    let _ = mcx;
    // If ORDER BY was given, generate a new upperrel of paths that emit the
    // correct ordering and project final_target (C:1849-1866). We can apply the
    // limit_tuples bound in sort costing only if there are no postponed SRFs.
    if has_sort {
        let limit_for_sort = if have_postponed_srfs { -1.0 } else { limit_tuples };
        let ordered_rel = create_ordered_paths(
            root,
            run,
            current_rel,
            final_target,
            final_target_parallel_safe,
            limit_for_sort,
        )?;
        // current_rel becomes the ordered upperrel for the final-output build.
        current_rel = ordered_rel;
        // Fix things up if final_target contains SRFs (C:1860-1862).
        if has_target_srfs {
            adjust_paths_for_srfs(
                root,
                current_rel,
                final_targets,
                final_targets_contain_srfs,
            )?;
        }
    }

    // Now build the final-output upperrel (C:1868).
    let final_rel = relnode::fetch_upper_rel(root, UPPERREL_FINAL, &None);

    // consider_parallel propagation (C:1870-1880). If current_rel is
    // consider_parallel and nothing in the LIMIT clause is parallel-unsafe, the
    // final_rel can be consider_parallel too.
    if root.rel(current_rel).consider_parallel {
        let limit_safe = {
            let parse = run.resolve(root.parse);
            let off = parse.limitOffset.as_deref();
            let cnt = parse.limitCount.as_deref();
            is_opt_expr_parallel_safe(root, off) && is_opt_expr_parallel_safe(root, cnt)
        };
        if limit_safe {
            root.rel_mut(final_rel).consider_parallel = true;
        }
    }

    // If current_rel belongs to a single FDW, so does final_rel (C:1883-1888).
    {
        let (serverid, userid, useridiscurrent, has_fdwroutine) = {
            let cr = root.rel(current_rel);
            (cr.serverid, cr.userid, cr.useridiscurrent, cr.has_fdwroutine)
        };
        let fr = root.rel_mut(final_rel);
        fr.serverid = serverid;
        fr.userid = userid;
        fr.useridiscurrent = useridiscurrent;
        fr.has_fdwroutine = has_fdwroutine;
    }

    // Generate paths for final_rel: insert all surviving current_rel paths with
    // LockRows / Limit / ModifyTable added if needed (C:1891-2131). On the
    // simple SELECT path (no rowMarks, no LIMIT/OFFSET, CMD_SELECT) each path is
    // inserted verbatim.
    let (has_rowmarks, command_type) = {
        let parse = run.resolve(root.parse);
        (
            !parse.rowMarks.is_empty(),
            parse.commandType,
        )
    };
    // FOR [KEY] UPDATE/SHARE wraps each surviving path in a LockRows node
    // (create_lockrows_path over root->rowMarks, planner.c:1905-1910). The
    // PlanRowMark list lives in the PlannerRun rowmark store keyed by
    // PlanRowMarkId (the same id-space create_lockrows_path / LockRowsPath /
    // LockRows now carry); the EvalPlanQual re-eval Param is a fresh special
    // exec param assigned once for the whole LockRows wrapper, as in C.
    let lockrows_epq_param: i32 = if has_rowmarks {
        paramassign_seams::assign_special_exec_param::call(root)?
    } else {
        0
    };
    let lockrows_rowmarks: Vec<pathnodes::PlanRowMarkId> =
        if has_rowmarks { root.rowMarks.clone() } else { Vec::new() };
    // If there is a LIMIT/OFFSET clause, each surviving path gets a LimitPath
    // wrapper (create_limit_path, planner.c:1915-1922). limit_needed() is the
    // exact gate (a constant-NULL/zero OFFSET or constant-NULL LIMIT adds no
    // node). The limitOffset / limitCount Expr nodes were const-folded into
    // parse during the EXPRKIND_LIMIT preprocess pass above; clone each into the
    // node arena to get the NodeId create_limit_path expects.
    let (limit_needed_flag, limit_option) = {
        let parse = run.resolve(root.parse);
        (limit_needed(parse), parse.limitOption as pathnodes::LimitOption)
    };
    let limit_offset_id: Option<pathnodes::NodeId> = if limit_needed_flag {
        let off = run.resolve(root.parse).limitOffset.as_deref().map(|e| e.clone());
        off.map(|e| root.alloc_node(e))
    } else {
        None
    };
    let limit_count_id: Option<pathnodes::NodeId> = if limit_needed_flag {
        let cnt = run.resolve(root.parse).limitCount.as_deref().map(|e| e.clone());
        cnt.map(|e| root.alloc_node(e))
    } else {
        None
    };

    // For each surviving path of current_rel, optionally wrap it in a LockRows
    // node (FOR UPDATE/SHARE), then a Limit node, then (for
    // INSERT/UPDATE/DELETE/MERGE) a ModifyTable node, then shove it into
    // final_rel (C:1891-2131).
    let surviving: Vec<PathId> = root.rel(current_rel).pathlist.clone();
    for path in surviving {
        let path = if has_rowmarks {
            pathnode::create::create_lockrows_path(
                root,
                final_rel,
                path,
                lockrows_rowmarks.clone(),
                lockrows_epq_param,
            )?
        } else {
            path
        };
        let path = if limit_needed_flag {
            pathnode::create::create_limit_path(
                root,
                final_rel,
                path,
                limit_offset_id,
                limit_count_id,
                limit_option,
                offset_est,
                count_est,
            )?
        } else {
            path
        };
        let final_path = if command_type != CmdType::CMD_SELECT {
            add_modifytable_to_path(root, run, final_rel, path)?
        } else {
            path
        };
        pathnode::add_path(root, final_rel, final_path)?;
    }

    // Generate partial paths for final_rel, too, if outer query levels might
    // be able to make use of them (C:2134-2146). This is what lets a parallel-
    // safe subquery (query_level > 1) expose its partial paths to the parent
    // query — e.g. each arm of a UNION ALL or a subquery-in-FROM can then feed
    // a Parallel Append / Gather above. Top-level queries (query_level == 1)
    // and queries needing LIMIT/OFFSET are excluded. C asserts !rowMarks &&
    // CMD_SELECT here because consider_parallel is supposed to be forced false
    // for any query with rowMarks or a non-SELECT command; we additionally test
    // those two flags directly (rather than asserting) so a partial path is
    // never exposed for a FOR UPDATE/SHARE or DML subquery even if the
    // consider_parallel propagation upstream is more permissive.
    if root.rel(final_rel).consider_parallel
        && root.query_level > 1
        && !limit_needed_flag
        && !has_rowmarks
        && command_type == CmdType::CMD_SELECT
    {
        let cur_partials: Vec<PathId> = root.rel(current_rel).partial_pathlist.clone();
        for partial_path in cur_partials {
            pathnode::add_partial_path(root, final_rel, partial_path)?;
        }
    }

    // GetForeignUpperPaths / create_upper_paths_hook (C:2152-2167): no FDW
    // upper-path routine is modeled and there is no hook; nothing to add.

    // Note: caller (subquery_planner) does set_cheapest(final_rel) (C:2169).
    Ok(())
}

/// Build the path/plan carrier for one ModifyTable RETURNING list, materializing
/// `parse->returningList` into node-arena `TargetEntryNode` handles. When
/// `this_result_rel != top_result_rel` (an inherited leaf), each TargetEntry's
/// expression is translated from the top target rel's attribute namespace to the
/// leaf's via `adjust_appendrel_attrs_multilevel`, mirroring C planner.c:2031-2038
/// (`returningList = adjust_appendrel_attrs_multilevel(...)`). For the
/// single-relation / top-rel case the expressions are cloned unchanged.
fn build_returning_list_for_leaf<'mcx>(
    run: &mut PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    // `Some((this, top))` requests attribute-namespace translation of the
    // RETURNING expressions from `top` down to `this` (the inherited leaf case).
    // `None` is the single-relation case (C: `returningLists =
    // list_make1(parse->returningList)`), which copies the list verbatim and
    // must NOT touch the planner RelOptInfo array — the single-relation target
    // rel of an INSERT/UPDATE/DELETE has no base RelOptInfo, so calling
    // find_base_rel on it would panic.
    translate_rels: Option<(pathnodes::RelId, pathnodes::RelId)>,
) -> PgResult<Vec<pathnodes::NodeId>> {
    let mcx = run.mcx();
    let n = run.resolve(root.parse).returningList.len();
    let mut ids: Vec<pathnodes::NodeId> = Vec::with_capacity(n);
    for i in 0..n {
        let (expr_clone, resno, resname, ressortgroupref, resorigtbl, resorigcol, resjunk) = {
            let tle = &run.resolve(root.parse).returningList[i];
            let expr = tle
                .expr
                .as_deref()
                .expect("grouping_planner: RETURNING TargetEntry with NULL expr (parser bug)");
            (
                expr.clone_in(mcx)?,
                tle.resno,
                tle.resname.as_ref().map(|s| alloc::string::String::from(s.as_str())),
                tle.ressortgroupref,
                tle.resorigtbl,
                tle.resorigcol,
                tle.resjunk,
            )
        };
        let expr_final = if let Some((this_result_rel, top_result_rel)) = translate_rels {
            appendinfo::adjust_appendrel_attrs_multilevel(
                root,
                expr_clone,
                this_result_rel,
                top_result_rel,
            )?
        } else {
            expr_clone
        };
        let expr_id = root.alloc_node(expr_final);
        let te = pathnodes::TargetEntryNode {
            expr: expr_id,
            resno,
            resname,
            ressortgroupref,
            resorigtbl,
            resorigcol,
            resjunk,
        };
        ids.push(root.alloc_targetentry(te));
    }
    Ok(ids)
}

/// Build the per-result-rel WITH CHECK OPTION list for one ModifyTable target
/// rel, mirroring C planner.c:1979-1989 (the inherited branch) and the
/// single-relation `withCheckOptionLists = list_make1(parse->withCheckOptions)`
/// (planner.c:2090). Each `parse->withCheckOptions` element (a `WithCheckOption`
/// node whose `qual` is the preprocessed constraint expr) is interned into the
/// planner arena as a lifetime-free [`WithCheckOptionNode`]; for an inherited
/// leaf that isn't the top target rel the `qual` is translated from the top
/// target rel's attribute namespace down to the leaf's via
/// `adjust_appendrel_attrs_multilevel`. `setrefs.c` later fixes up the Vars.
fn build_wco_list_for_leaf<'mcx>(
    run: &mut PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    translate_rels: Option<(pathnodes::RelId, pathnodes::RelId)>,
) -> PgResult<Vec<pathnodes::NodeId>> {
    let mcx = run.mcx();
    let n = run.resolve(root.parse).withCheckOptions.len();
    let mut ids: Vec<pathnodes::NodeId> = Vec::with_capacity(n);
    for i in 0..n {
        let (kind, relname, polname, qual_expr, cascaded) = {
            let wco = run.resolve(root.parse).withCheckOptions[i]
                .as_withcheckoption()
                .expect("grouping_planner: withCheckOptions element is not a WithCheckOption");
            let qual_expr = match wco.qual.as_deref() {
                None => None,
                Some(node) => Some(
                    node.as_expr()
                        .expect("grouping_planner: WithCheckOption qual is not an Expr")
                        .clone_in(mcx)?,
                ),
            };
            (
                wco.kind,
                wco.relname.as_ref().map(|s| alloc::string::String::from(s.as_str())),
                wco.polname.as_ref().map(|s| alloc::string::String::from(s.as_str())),
                qual_expr,
                wco.cascaded,
            )
        };
        let qual_id = match qual_expr {
            None => pathnodes::NodeId::default(),
            Some(expr) => {
                let expr_final = if let Some((this_rel, top_rel)) = translate_rels {
                    appendinfo::adjust_appendrel_attrs_multilevel(
                        root, expr, this_rel, top_rel,
                    )?
                } else {
                    expr
                };
                root.alloc_node(expr_final)
            }
        };
        let node = pathnodes::WithCheckOptionNode {
            kind,
            relname,
            polname,
            qual: qual_id,
            cascaded,
        };
        ids.push(root.alloc_with_check_option(node));
    }
    Ok(ids)
}

/// Build the per-result-rel MERGE action list for one ModifyTable target rel,
/// mirroring C planner.c:2004-2031 (the inherited branch's `copyObject(action)`
/// + per-leaf translation) and the single-relation
/// `mergeActionLists = list_make1(parse->mergeActionList)` (planner.c:2096).
/// Each `parse->mergeActionList` MergeAction is interned into the planner arena
/// as a lifetime-free [`MergeActionNode`] (its `qual` Expr and `targetList`
/// TargetEntry exprs re-interned as their own arena handles); for an inherited
/// leaf that isn't the top target rel the `qual`/`targetList` are translated via
/// `adjust_appendrel_attrs_multilevel` and `updateColnos` via
/// `adjust_inherited_attnums_multilevel`. `setrefs.c` later fixes up the Vars.
fn build_merge_action_list_for_leaf<'mcx>(
    run: &mut PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    translate_rels: Option<(pathnodes::RelId, pathnodes::RelId)>,
) -> PgResult<Vec<pathnodes::NodeId>> {
    let mcx = run.mcx();
    let n = run.resolve(root.parse).mergeActionList.len();
    let mut ids: Vec<pathnodes::NodeId> = Vec::with_capacity(n);
    for i in 0..n {
        // Snapshot the action's fields (cloning the lifetime-bearing exprs into
        // mcx) so we can translate + intern without holding a parse borrow.
        let (match_kind, command_type, overriding, qual_expr, tl_exprs, update_colnos) = {
            let action = run.resolve(root.parse).mergeActionList[i]
                .as_mergeaction()
                .expect("grouping_planner: mergeActionList element is not a MergeAction");
            let qual_expr = match action.qual.as_deref() {
                None => None,
                Some(node) => Some(
                    node.as_expr()
                        .expect("grouping_planner: MergeAction qual is not an Expr")
                        .clone_in(mcx)?,
                ),
            };
            let mut tl_exprs: Vec<(
                Expr,
                types_core::primitive::AttrNumber,
                Option<alloc::string::String>,
                types_core::primitive::Index,
                Oid,
                types_core::primitive::AttrNumber,
                bool,
            )> = Vec::with_capacity(action.targetList.len());
            for tle_node in action.targetList.iter() {
                let tle = tle_node
                    .as_targetentry()
                    .expect("grouping_planner: MergeAction targetList entry is a TargetEntry");
                let expr = tle
                    .expr
                    .as_deref()
                    .expect("grouping_planner: MergeAction TargetEntry with NULL expr")
                    .clone_in(mcx)?;
                tl_exprs.push((
                    expr,
                    tle.resno,
                    tle.resname.as_ref().map(|s| alloc::string::String::from(s.as_str())),
                    tle.ressortgroupref,
                    tle.resorigtbl,
                    tle.resorigcol,
                    tle.resjunk,
                ));
            }
            let update_colnos: Vec<i32> = action.updateColnos.iter().copied().collect();
            (
                action.matchKind,
                action.commandType,
                action.r#override,
                qual_expr,
                tl_exprs,
                update_colnos,
            )
        };

        // qual: translate when an inherited non-top leaf.
        let qual_id = match qual_expr {
            None => pathnodes::NodeId::default(),
            Some(expr) => {
                let expr_final = if let Some((this_rel, top_rel)) = translate_rels {
                    appendinfo::adjust_appendrel_attrs_multilevel(
                        root, expr, this_rel, top_rel,
                    )?
                } else {
                    expr
                };
                root.alloc_node(expr_final)
            }
        };

        // targetList: translate each entry's expr, intern as a TargetEntry.
        let mut tl_ids: Vec<pathnodes::NodeId> = Vec::with_capacity(tl_exprs.len());
        for (expr, resno, resname, ressortgroupref, resorigtbl, resorigcol, resjunk) in tl_exprs {
            let expr_final = if let Some((this_rel, top_rel)) = translate_rels {
                appendinfo::adjust_appendrel_attrs_multilevel(
                    root, expr, this_rel, top_rel,
                )?
            } else {
                expr
            };
            let expr_id = root.alloc_node(expr_final);
            let te = pathnodes::TargetEntryNode {
                expr: expr_id,
                resno,
                resname,
                ressortgroupref,
                resorigtbl,
                resorigcol,
                resjunk,
            };
            tl_ids.push(root.alloc_targetentry(te));
        }

        // updateColnos: translate via adjust_inherited_attnums_multilevel when
        // this is an inherited UPDATE action on a non-top leaf. The colnos are
        // `int` (i32) in the parse node but the appendinfo translation works on
        // `AttrNumber` (i16), so round-trip through i16.
        let update_colnos_final: Vec<i32> = if command_type == CmdType::CMD_UPDATE {
            if let Some((this_rel, top_rel)) = translate_rels {
                let attnums: Vec<types_core::primitive::AttrNumber> =
                    update_colnos.iter().map(|&c| c as types_core::primitive::AttrNumber).collect();
                let (this_relid, top_relid) =
                    (root.rel(this_rel).relid, root.rel(top_rel).relid);
                let translated = appendinfo::adjust_inherited_attnums_multilevel(
                    root,
                    &attnums,
                    this_relid,
                    top_relid,
                )?;
                translated.into_iter().map(|a| a as i32).collect()
            } else {
                update_colnos
            }
        } else {
            update_colnos
        };

        let node = pathnodes::MergeActionNode {
            matchKind: match_kind,
            commandType: command_type as u32,
            overriding,
            qual: qual_id,
            targetList: tl_ids,
            updateColnos: update_colnos_final,
        };
        ids.push(root.alloc_merge_action(node));
    }
    Ok(ids)
}

/// Build the per-result-rel MERGE join-condition entry for one ModifyTable
/// target rel, mirroring C planner.c:2040-2048 / the single-relation
/// `mergeJoinConditions = list_make1(parse->mergeJoinCondition)`. The
/// `parse->mergeJoinCondition` is a single `Node` (an `Expr`) interned into the
/// arena; for an inherited non-top leaf it is translated via
/// `adjust_appendrel_attrs_multilevel`. A NULL join condition (no condition)
/// yields the `NodeId(0)` NULL marker. The outer list always has one entry per
/// result rel (even when the inner condition is NULL).
fn build_merge_join_condition_for_leaf<'mcx>(
    run: &mut PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    translate_rels: Option<(pathnodes::RelId, pathnodes::RelId)>,
) -> PgResult<Vec<pathnodes::NodeId>> {
    let mcx = run.mcx();
    let cond_expr: Option<Expr> = match run.resolve(root.parse).mergeJoinCondition.as_deref() {
        None => None,
        Some(expr) => Some(expr.clone_in(mcx)?),
    };
    let id = match cond_expr {
        None => pathnodes::NodeId::default(),
        Some(expr) => {
            let expr_final = if let Some((this_rel, top_rel)) = translate_rels {
                appendinfo::adjust_appendrel_attrs_multilevel(
                    root, expr, this_rel, top_rel,
                )?
            } else {
                expr
            };
            root.alloc_node(expr_final)
        }
    };
    // One inner entry (the join condition handle, possibly the NULL marker).
    let mut out: Vec<pathnodes::NodeId> = Vec::with_capacity(1);
    out.push(id);
    Ok(out)
}

/// `bms_membership(set) == BMS_MULTIPLE` (bitmapset.c) — true if the relid set
/// has more than one member. Counts set bits over the planner `Relids` word
/// storage (`None` == empty == not multiple).
fn relids_is_multiple(relids: &Relids) -> bool {
    match relids {
        None => false,
        Some(bms) => bms.words.iter().map(|w| w.count_ones()).sum::<u32>() > 1,
    }
}

/// The INSERT/UPDATE/DELETE/MERGE ModifyTable wrapper from the per-path loop of
/// `grouping_planner` (planner.c:1925-2112). Builds the per-target-rel lists and
/// calls `create_modifytable_path` over `subpath`, returning the wrapping
/// ModifyTablePath. Bounded to the non-inherited, non-ON-CONFLICT, non-RETURNING
/// INSERT/UPDATE/DELETE case (a single-row VALUES INSERT lands here); the
/// inherited (`BMS_MULTIPLE`), MERGE, ON CONFLICT, RETURNING and
/// WITH-CHECK-OPTION sub-cases error out faithfully because their per-target-rel
/// node-list translation (`adjust_appendrel_attrs_multilevel` / the
/// OnConflict/RETURNING/WCO node carriers) is not modeled here.
fn add_modifytable_to_path<'mcx>(
    root: &mut PlannerInfo,
    run: &mut PlannerRun<'mcx>,
    final_rel: RelId,
    subpath: PathId,
) -> PgResult<PathId> {
    // Snapshot the parse fields and the root planner fields we need.
    let (
        command_type,
        result_relation,
        can_set_tag,
        has_returning,
        has_wco,
        has_merge_action,
        has_onconflict,
    ) = {
        let parse = run.resolve(root.parse);
        (
            parse.commandType,
            parse.resultRelation,
            parse.canSetTag,
            !parse.returningList.is_empty(),
            !parse.withCheckOptions.is_empty(),
            !parse.mergeActionList.is_empty(),
            parse.onConflict.is_some(),
        )
    };

    // Inherited UPDATE/DELETE/MERGE (BMS_MULTIPLE, planner.c:1946-2079): the
    // ModifyTable is given the leaf partitions (root->leaf_result_relids) as its
    // result relations, the partitioned root is passed forward as rootRelation,
    // and per-leaf updateColnos / RETURNING lists are translated from the top
    // target rel's namespace via adjust_inherited_attnums_multilevel /
    // adjust_appendrel_attrs_multilevel. When taken, `inherited_lists` carries
    // (rootRelation, resultRelations, updateColnosLists, returningLists) and the
    // single-relation builders below are skipped.
    //
    // The per-leaf WITH-CHECK-OPTION (`withCheckOptionLists`) and
    // MERGE-action/join-condition (`mergeActionLists`/`mergeJoinConditions`)
    // lists are built per leaf here too (translated from the top target rel's
    // namespace via adjust_appendrel_attrs_multilevel), interned into the
    // planner arena, and carried on the path as NodeId handle lists.
    type InheritedLists = (
        types_core::primitive::Index,
        Vec<i32>,
        Vec<Vec<types_core::primitive::AttrNumber>>,
        Vec<Vec<pathnodes::NodeId>>,
        // withCheckOptionLists / mergeActionLists / mergeJoinConditions
        Vec<Vec<pathnodes::NodeId>>,
        Vec<Vec<pathnodes::NodeId>>,
        Vec<Vec<pathnodes::NodeId>>,
    );
    let inherited_lists: Option<InheritedLists> = if relids_is_multiple(&root.all_result_relids) {
        // top_result_rel = find_base_rel(root, parse->resultRelation).
        let top_result_relid = result_relation as types_core::primitive::Index;
        let top_result_rel =
            relnode::find_base_rel(root, result_relation);
        // RelOptInfo->relid is the RT index; for the leaf loop the bms members of
        // leaf_result_relids ARE the RT indexes, so this_relid == rrelid and
        // top_parent_relid == parse->resultRelation directly.
        let top_parent_relid = result_relation as types_core::primitive::Index;

        // Pass the root result rel forward to the executor.
        let root_relation = top_result_relid;

        let mut result_relations: Vec<i32> = Vec::new();
        let mut update_colnos_lists: Vec<Vec<types_core::primitive::AttrNumber>> = Vec::new();
        let mut returning_lists: Vec<Vec<pathnodes::NodeId>> = Vec::new();
        let mut wco_lists: Vec<Vec<pathnodes::NodeId>> = Vec::new();
        let mut merge_action_lists: Vec<Vec<pathnodes::NodeId>> = Vec::new();
        let mut merge_join_conditions: Vec<Vec<pathnodes::NodeId>> = Vec::new();

        // Iterate leaf_result_relids (bms_next_member over the planner Relids word
        // storage), adding only non-dummy leaves to the ModifyTable.
        let leaf_members: Vec<i32> = match root.leaf_result_relids.as_ref() {
            None => Vec::new(),
            Some(bms) => {
                let mut v = Vec::new();
                for (wi, w) in bms.words.iter().enumerate() {
                    let mut word = *w;
                    while word != 0 {
                        let bit = word.trailing_zeros() as usize;
                        v.push((wi * (u64::BITS as usize) + bit) as i32);
                        word &= word - 1;
                    }
                }
                v
            }
        };
        for rrelid in leaf_members {
            let this_result_rel =
                relnode::find_base_rel(root, rrelid);
            // Exclude any leaf rels that have turned dummy (e.g. by constraint
            // exclusion) since being added to leaf_result_relids.
            if joinrels::is_dummy_rel(root, this_result_rel) {
                continue;
            }
            let this_relid = rrelid as types_core::primitive::Index;

            result_relations.push(rrelid);

            if command_type == CmdType::CMD_UPDATE {
                let update_colnos = if this_relid != top_parent_relid {
                    let top_update_colnos = root.update_colnos.clone();
                    appendinfo::adjust_inherited_attnums_multilevel(
                        root,
                        &top_update_colnos,
                        this_relid,
                        top_parent_relid,
                    )?
                } else {
                    root.update_colnos.clone()
                };
                update_colnos_lists.push(update_colnos);
            }

            if has_returning {
                // returningList = parse->returningList, translated to this leaf's
                // attribute namespace when it isn't the top target rel.
                // Translate only when this leaf isn't the top target rel
                // (C: adjust_appendrel_attrs_multilevel for non-top leaves).
                let translate_rels = if this_result_rel != top_result_rel {
                    Some((this_result_rel, top_result_rel))
                } else {
                    None
                };
                let ids = build_returning_list_for_leaf(run, root, translate_rels)?;
                returning_lists.push(ids);
            }

            // Translation flag shared by WCO / MERGE-action / join-condition.
            let translate_rels = if this_result_rel != top_result_rel {
                Some((this_result_rel, top_result_rel))
            } else {
                None
            };

            if has_wco {
                wco_lists.push(build_wco_list_for_leaf(run, root, translate_rels)?);
            }
            if has_merge_action {
                merge_action_lists
                    .push(build_merge_action_list_for_leaf(run, root, translate_rels)?);
            }
            if command_type == CmdType::CMD_MERGE {
                merge_join_conditions
                    .push(build_merge_join_condition_for_leaf(run, root, translate_rels)?);
            }
        }

        // If we managed to exclude every child rel, generate a dummy one-relation
        // plan using info for the top target rel (statement triggers still fire).
        if result_relations.is_empty() {
            result_relations.push(result_relation);
            if command_type == CmdType::CMD_UPDATE {
                update_colnos_lists.push(root.update_colnos.clone());
            }
            if has_returning {
                // Dummy single-rel fallback for the top target rel: no
                // translation (top == top).
                let ids = build_returning_list_for_leaf(run, root, None)?;
                returning_lists.push(ids);
            }
            if has_wco {
                wco_lists.push(build_wco_list_for_leaf(run, root, None)?);
            }
            if has_merge_action {
                merge_action_lists.push(build_merge_action_list_for_leaf(run, root, None)?);
            }
            if command_type == CmdType::CMD_MERGE {
                merge_join_conditions
                    .push(build_merge_join_condition_for_leaf(run, root, None)?);
            }
        }

        Some((
            root_relation,
            result_relations,
            update_colnos_lists,
            returning_lists,
            wco_lists,
            merge_action_lists,
            merge_join_conditions,
        ))
    } else {
        None
    };
    // rootRelation / resultRelations / updateColnosLists / returningLists /
    // withCheckOptionLists / mergeActionLists / mergeJoinConditions: from the
    // inherited (BMS_MULTIPLE) computation above when that branch was taken,
    // else the single-relation INSERT/UPDATE/DELETE/MERGE case (C:2099-2112)
    // with rootRelation = 0 (no separate root rel). For the single-rel case the
    // lists are `list_make1` of the parse-direct values, interned with no
    // attribute-namespace translation (the target rel is the top rel itself).
    #[allow(clippy::type_complexity)]
    let (
        root_relation,
        result_relations,
        update_colnos_lists,
        returning_lists,
        wco_lists,
        merge_action_lists,
        merge_join_conditions,
    ): (
        types_core::primitive::Index,
        Vec<i32>,
        Vec<Vec<types_core::primitive::AttrNumber>>,
        Vec<Vec<pathnodes::NodeId>>,
        Vec<Vec<pathnodes::NodeId>>,
        Vec<Vec<pathnodes::NodeId>>,
        Vec<Vec<pathnodes::NodeId>>,
    ) = if let Some((rr, rels, ucl, rl, wcl, mal, mjc)) = inherited_lists {
        (rr, rels, ucl, rl, wcl, mal, mjc)
    } else {
        let mut result_relations: Vec<i32> = Vec::with_capacity(1);
        result_relations.push(result_relation);
        let mut update_colnos_lists: Vec<Vec<types_core::primitive::AttrNumber>> = Vec::new();
        if command_type == CmdType::CMD_UPDATE {
            update_colnos_lists.push(root.update_colnos.clone());
        }
        // returningLists = list_make1(parse->returningList).
        let mut returning_lists: Vec<Vec<pathnodes::NodeId>> = Vec::new();
        if has_returning {
            // Single-relation case (C: `returningLists =
            // list_make1(parse->returningList)`): no translation, and crucially
            // no find_base_rel on the result relation — it has no base
            // RelOptInfo for a non-inherited INSERT/UPDATE/DELETE.
            let ids = build_returning_list_for_leaf(run, root, None)?;
            returning_lists.push(ids);
        }
        // withCheckOptionLists = list_make1(parse->withCheckOptions).
        let mut wco_lists: Vec<Vec<pathnodes::NodeId>> = Vec::new();
        if has_wco {
            wco_lists.push(build_wco_list_for_leaf(run, root, None)?);
        }
        // mergeActionLists = list_make1(parse->mergeActionList) and
        // mergeJoinConditions = list_make1(parse->mergeJoinCondition).
        let mut merge_action_lists: Vec<Vec<pathnodes::NodeId>> = Vec::new();
        if has_merge_action {
            merge_action_lists.push(build_merge_action_list_for_leaf(run, root, None)?);
        }
        let mut merge_join_conditions: Vec<Vec<pathnodes::NodeId>> = Vec::new();
        if command_type == CmdType::CMD_MERGE {
            merge_join_conditions.push(build_merge_join_condition_for_leaf(run, root, None)?);
        }
        (
            0,
            result_relations,
            update_colnos_lists,
            returning_lists,
            wco_lists,
            merge_action_lists,
            merge_join_conditions,
        )
    };

    // If there was a FOR [KEY] UPDATE/SHARE clause the LockRows node dealt with
    // it; else ModifyTable handles it. parse->rowMarks is empty here (guarded
    // out above), so rowMarks = root->rowMarks (also empty on this path).
    let row_marks: Vec<pathnodes::NodeId> = Vec::new();

    let part_cols_updated = root.partColsUpdated;
    let epq_param = paramassign_seams::assign_special_exec_param::call(root)?;

    // The owned `OnConflictExpr` lives on `root->parse->onConflict`; the path /
    // plan carrier holds only a presence handle (the analysis runs in
    // createplan.c, which reads parse->onConflict directly). Allocate a marker
    // node so the path's `onconflict` slot is Some(..) iff ON CONFLICT is present.
    let onconflict_marker: Option<pathnodes::NodeId> = if has_onconflict {
        Some(root.alloc_node(Expr::Const(::nodes::primnodes::Const::default())))
    } else {
        None
    };

    // The planner's `CmdType` is the enum (nodes); the pathnode seam's
    // `CmdType` is the `u32` alias (pathnodes). The discriminants agree
    // (CMD_SELECT=1 .. CMD_MERGE=5), so cast.
    let operation = command_type as u32;

    pathnode::create::create_modifytable_path(
        root,
        final_rel,
        subpath,
        operation,
        can_set_tag,
        result_relation as types_core::primitive::Index,
        root_relation,
        part_cols_updated,
        result_relations,
        update_colnos_lists,
        wco_lists,
        returning_lists,
        row_marks,
        onconflict_marker,
        merge_action_lists,
        merge_join_conditions,
        epq_param,
    )
}

// ===========================================================================
// make_group_input_target (planner.c:5527)
// ===========================================================================

/// `make_group_input_target(root, final_target)` (planner.c:5527) — generate the
/// `PathTarget` for the initial input to grouping nodes. We build a target
/// containing all grouping columns, plus any other Vars mentioned in the query's
/// targetlist and HAVING qual (with non-grouping expressions flattened into their
/// component Vars). For a plain aggregate with no GROUP BY / HAVING (the
/// `count(*)` path) the grouping columns are none and the aggregate's argument
/// Vars are pulled out; `count(*)` has no argument Vars, so the result is an
/// empty target.
fn make_group_input_target<'mcx>(
    mcx: Mcx<'mcx>,
    run: &mut PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    final_target: &PathTarget,
) -> PgResult<PathTarget> {
    use vars::tlist::{
        add_column_to_pathtarget, add_new_columns_to_pathtarget, create_empty_pathtarget,
        get_sortgroupref_clause_noerr,
    };
    use vars::var::{
        pull_var_clause, PVC_INCLUDE_PLACEHOLDERS, PVC_RECURSE_AGGREGATES, PVC_RECURSE_WINDOWFUNCS,
    };

    // We must build a target containing all grouping columns, plus any other
    // Vars mentioned in the query's targetlist and HAVING qual (C:5541).
    let mut input_target = create_empty_pathtarget();

    // Resolve processed_groupClause handles into SortGroupClause values once
    // (the C `root->processed_groupClause` List). For a plain aggregate this is
    // empty, so every column drops to the non-group path.
    let group_clauses: Vec<::nodes::rawnodes::SortGroupClause> = root
        .processed_groupClause
        .iter()
        .map(|&id| *root.sortgroupclause(id))
        .collect();

    // The target is logically below the grouping step. So with grouping sets we
    // need to remove the RT index of the grouping step (if any) from the target
    // expressions, otherwise the input Vars carry the group nulling that only
    // belongs above the grouping step — and the sort built over this input
    // (whose pathkeys are the un-nulled group_pathkeys) can't find its keys
    // ("could not find pathkey item to sort"). (planner.c:5560 / :5605.)
    let strip_group_rtindex = {
        let parse = run.resolve(root.parse);
        parse.hasGroupRTE && !parse.groupingSets.is_empty()
    };
    let group_singleton: Relids = if strip_group_rtindex {
        debug_assert!(root.group_rtindex > 0);
        bms_make_singleton_relids(root.group_rtindex)
    } else {
        None
    };

    let mut non_group_cols: Vec<pathnodes::NodeId> = Vec::new();
    for (i, &expr_id) in final_target.exprs.iter().enumerate() {
        let sgref = get_pathtarget_sortgroupref(final_target, i);
        let is_group_col = sgref != 0
            && !group_clauses.is_empty()
            && get_sortgroupref_clause_noerr(sgref, &group_clauses).is_some();
        if is_group_col {
            // It's a grouping column; add it to the input target. With grouping
            // sets, strip the grouping step's RT index from the expr first.
            let col_id = if strip_group_rtindex {
                let owned = root.node(expr_id).clone_in(mcx)?;
                let stripped = nodeFuncs_seams::remove_nulling_relids::call(
                    mcx,
                    owned,
                    &group_singleton,
                    &None,
                );
                root.alloc_node(stripped)
            } else {
                expr_id
            };
            add_column_to_pathtarget(&mut input_target, col_id, sgref);
        } else {
            // Non-grouping column; remember the expression for the later
            // pull_var_clause call.
            non_group_cols.push(expr_id);
        }
    }

    // If there's a HAVING clause, we'll need the Vars it uses, too (C:5586).
    // havingQual is the concretely-typed owned `Option<PgBox<Expr>>` view; clone
    // it into the arena to obtain a handle that pull_var_clause can walk.
    let having_id: Option<pathnodes::NodeId> =
        match run.resolve(root.parse).havingQual.as_deref() {
            Some(e) => {
                let cloned = e.clone_in(mcx)?;
                Some(root.alloc_node(cloned))
            }
            None => None,
        };
    if let Some(id) = having_id {
        non_group_cols.push(id);
    }

    // Pull out all the Vars mentioned in non-group cols (plus HAVING), and add
    // them to the input target if not already present (C:5601). pull_var_clause
    // walks a Node; here it walks each handle's resolved Expr and unions the
    // results (equivalent to walking the C `List` node).
    let mut non_group_var_ids: Vec<pathnodes::NodeId> = Vec::new();
    let flags = PVC_RECURSE_AGGREGATES | PVC_RECURSE_WINDOWFUNCS | PVC_INCLUDE_PLACEHOLDERS;
    for &nid in &non_group_cols {
        // node_expr_wrapper deep-copies the input Expr into a scratch context
        // (a shallow Expr::clone panics on an Aggref); PVC_RECURSE_AGGREGATES
        // recurses into the agg's args rather than pushing the agg itself.
        let scratch = mcx::MemoryContext::new("make_group_input_target pull_var_clause");
        let node = nodes_core::node_walker::node_expr_wrapper(root.node(nid), scratch.mcx());
        // Deep-copy collected nodes into the planner mcx (re-interned via
        // `alloc_node` below); a plain `.clone()` panics on a context-allocated
        // child (Aggref TargetEntry args).
        let vars = pull_var_clause(mcx, &node, flags)?;
        for v in vars {
            // The target is logically below the grouping step; with grouping
            // sets, strip the grouping step's RT index from the pulled Vars too
            // (C:5605). Otherwise these non-group input Vars carry group nulling
            // that breaks downstream sort-key matching.
            let vid = if strip_group_rtindex {
                let stripped = nodeFuncs_seams::remove_nulling_relids::call(
                    mcx,
                    v,
                    &group_singleton,
                    &None,
                );
                root.alloc_node(stripped)
            } else {
                root.alloc_node(v)
            };
            non_group_var_ids.push(vid);
        }
    }
    add_new_columns_to_pathtarget(root, &mut input_target, &non_group_var_ids);

    // XXX this causes some redundant cost calculation ... (C:5619).
    costsize::sizeest::set_pathtarget_cost_width(root, &mut input_target);
    Ok(input_target)
}

// ===========================================================================
// Window functions (planner.c) — optimize_window_clauses,
// select_active_windows, make_window_input_target, make_pathkeys_for_window,
// create_window_paths, create_one_window_path
// ===========================================================================

/// The window-function lists found by `find_window_functions`, with each
/// `WindowFunc` interned into the planner arena (so cost/createplan can read
/// it through a `NodeId`). `window_funcs[winref]` is the handle list for that
/// winref; `clauses[winref]` is the interned arena `WindowClause` handle for
/// the WindowClause with that winref (or `None` if none). Mirrors the C
/// `WindowFuncLists` plus the `WindowClause` list it is indexed alongside.
struct WindowFuncListsArena {
    num_window_funcs: i32,
    /// `maxWinRef` — mirrors the C `WindowFuncLists` field (the highest winref);
    /// retained for fidelity / assertions even where not read directly.
    #[allow(dead_code)]
    max_win_ref: u32,
    /// `windowFuncs[winref]` — interned WindowFunc Expr handles.
    window_funcs: Vec<Vec<pathnodes::NodeId>>,
    /// Interned arena `WindowClause` handle per winref (parse->windowClause).
    clauses: Vec<Option<pathnodes::NodeId>>,
    /// The order of WindowClauses as they appear in parse->windowClause
    /// (their winrefs), so we can iterate them like the C `foreach`.
    clause_order: Vec<u32>,
}

/// Intern the `find_window_functions` result + every `parse->windowClause`
/// WindowClause into the planner arena, returning a [`WindowFuncListsArena`].
/// Each WindowClause's partition/order `SortGroupClause`s and start/end offsets
/// are interned as their own arena handles.
fn build_window_func_lists_arena<'mcx>(
    root: &mut PlannerInfo,
    lists: clauses::WindowFuncLists,
) -> PgResult<WindowFuncListsArena> {
    let max = lists.max_win_ref;
    let mut window_funcs: Vec<Vec<pathnodes::NodeId>> =
        Vec::with_capacity((max as usize) + 1);
    for funcs in lists.window_funcs.into_iter() {
        let mut ids = Vec::with_capacity(funcs.len());
        for f in funcs {
            ids.push(root.alloc_node(f));
        }
        window_funcs.push(ids);
    }

    let mut clauses: Vec<Option<pathnodes::NodeId>> = Vec::with_capacity((max as usize) + 1);
    for _ in 0..=(max as usize) {
        clauses.push(None);
    }
    Ok(WindowFuncListsArena {
        num_window_funcs: lists.num_window_funcs,
        max_win_ref: max,
        window_funcs,
        clauses,
        clause_order: Vec::new(),
    })
}

/// Read all of `parse->windowClause` into arena [`WindowClauseNode`]s, keyed by
/// winref, populating `wfa.clauses` / `wfa.clause_order`. Interns the
/// partition/order `SortGroupClause`s and the (already-preprocessed) start/end
/// offset `Expr`s as arena handles. Idempotent within a `wfa` (only fills
/// empty slots).
fn intern_parse_window_clauses<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    wfa: &mut WindowFuncListsArena,
) -> PgResult<()> {
    if !wfa.clause_order.is_empty() {
        return Ok(());
    }
    let n = run.resolve(root.parse).windowClause.len();
    for i in 0..n {
        // Read the parse WindowClause into a lifetime-free arena form.
        let (
            name,
            part_sgcs,
            ord_sgcs,
            frame_options,
            start_off,
            end_off,
            start_in_range,
            end_in_range,
            in_range_coll,
            in_range_asc,
            in_range_nulls_first,
            winref,
        ) = {
            let wc_node = &*run.resolve(root.parse).windowClause[i];
            let wc = match wc_node.node_tag() {
                ntag::T_WindowClause => wc_node.expect_windowclause(),
                _ => panic!("windowClause element is not a WindowClause (got {:?})", wc_node.tag()),
            };
            let name = wc.name.as_ref().map(|s| alloc::string::String::from(s.as_str()));
            let part: Vec<::nodes::rawnodes::SortGroupClause> = wc
                .partitionClause
                .iter()
                .map(|np| sortgroupclause_from_node(np))
                .collect::<PgResult<Vec<_>>>()?;
            let ord: Vec<::nodes::rawnodes::SortGroupClause> = wc
                .orderClause
                .iter()
                .map(|np| sortgroupclause_from_node(np))
                .collect::<PgResult<Vec<_>>>()?;
            let start_off = wc.startOffset.as_ref().map(|np| match np.as_expr() {
                Some(e) => e.clone(),
                None => panic!("WindowClause startOffset is not an Expr (got {:?})", np.tag()),
            });
            let end_off = wc.endOffset.as_ref().map(|np| match np.as_expr() {
                Some(e) => e.clone(),
                None => panic!("WindowClause endOffset is not an Expr (got {:?})", np.tag()),
            });
            (
                name,
                part,
                ord,
                wc.frameOptions,
                start_off,
                end_off,
                wc.startInRangeFunc,
                wc.endInRangeFunc,
                wc.inRangeColl,
                wc.inRangeAsc,
                wc.inRangeNullsFirst,
                wc.winref,
            )
        };

        let mut partition_clause: Vec<pathnodes::NodeId> = Vec::with_capacity(part_sgcs.len());
        for sgc in part_sgcs {
            partition_clause.push(root.alloc_sortgroupclause(sgc));
        }
        let mut order_clause: Vec<pathnodes::NodeId> = Vec::with_capacity(ord_sgcs.len());
        for sgc in ord_sgcs {
            order_clause.push(root.alloc_sortgroupclause(sgc));
        }
        let start_offset: Option<pathnodes::NodeId> = start_off.map(|e| root.alloc_node(e));
        let end_offset: Option<pathnodes::NodeId> = end_off.map(|e| root.alloc_node(e));

        let wc_arena = pathnodes::WindowClauseNode {
            name,
            partitionClause: partition_clause,
            orderClause: order_clause,
            frameOptions: frame_options,
            startOffset: start_offset,
            endOffset: end_offset,
            startInRangeFunc: start_in_range,
            endInRangeFunc: end_in_range,
            inRangeColl: in_range_coll,
            inRangeAsc: in_range_asc,
            inRangeNullsFirst: in_range_nulls_first,
            winref,
        };
        let id = root.alloc_windowclause(wc_arena);
        wfa.clauses[winref as usize] = Some(id);
        wfa.clause_order.push(winref);
    }
    Ok(())
}

/// `optimize_window_clauses(root, wflists)` (planner.c:5815) — apply
/// support-function frame-option optimizations + the duplicate-WindowFunc
/// dedup. The support-function (`SupportRequestOptimizeWindowClause`) dispatch
/// rides the planner-support fmgr machinery, unported workspace-wide; functions
/// with no `prosupport` (every basic ranking/offset window fn) `break` before
/// reaching it, so the support path is loud-panic until that machinery lands.
fn optimize_window_clauses<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    wfa: &mut WindowFuncListsArena,
) -> PgResult<()> {
    intern_parse_window_clauses(run, root, wfa)?;

    let order = wfa.clause_order.clone();
    for &winref in &order {
        let wc_id = match wfa.clauses[winref as usize] {
            Some(id) => id,
            None => continue,
        };
        // skip any WindowClauses that have no WindowFuncs.
        if wfa.window_funcs[winref as usize].is_empty() {
            continue;
        }

        // For each WindowFunc, check for a support function and, if present, call
        // the SupportRequestOptimizeWindowClause request to optimize the frame
        // options. The optimization is purely an efficiency one — it narrows
        // RANGE/GROUPS frames to equivalent ROWS frames (e.g. row_number()'s
        // frame to "ROWS UNBOUNDED PRECEDING AND CURRENT ROW"), which yields
        // identical results, just with fewer peer checks at execution.
        let cur_frame = root.windowclause(wc_id).frameOptions;
        let mut optimized_frame_options: i32 = 0;
        let mut all_agree = true;
        let func_ids = wfa.window_funcs[winref as usize].clone();
        for (idx, &wfunc_id) in func_ids.iter().enumerate() {
            let Some(w) = root.node(wfunc_id).as_windowfunc() else {
                panic!("optimize_window_clauses: windowFuncs entry is not a WindowFunc");
            };
            let winfnoid = w.winfnoid;
            let prosupport =
                lsyscache::function::get_func_support(winfnoid)?;

            // Check if there's a support function for 'wfunc'.
            if prosupport == types_core::primitive::InvalidOid {
                all_agree = false;
                break; // can't optimize this WindowClause
            }

            // Call the support function (SupportRequestOptimizeWindowClause).
            let res = clauses::support_optimize_window::call_support_optimize_window(
                prosupport,
                winfnoid,
                cur_frame,
            )?;

            // Skip to next WindowClause if the support function does not support
            // this request type (the C `res == NULL` path).
            let new_frame = match res {
                Some(f) => f,
                None => {
                    all_agree = false;
                    break;
                }
            };

            if idx == 0 {
                // Save these frameOptions for the first WindowFunc.
                optimized_frame_options = new_frame;
            } else if optimized_frame_options != new_frame {
                // Subsequent WindowFuncs must agree, else we can't optimize.
                all_agree = false;
                break;
            }
        }

        // Adjust the frameOptions if all WindowFunc's agree that it's ok.
        if all_agree && cur_frame != optimized_frame_options {
            // Apply the new frame options.
            root.windowclause_mut(wc_id).frameOptions = optimized_frame_options;

            // Check whether changing the frameOptions has made this WindowClause
            // a duplicate of another. This can only happen with multiple
            // WindowClauses, so don't bother if there's only one.
            if order.len() > 1 {
                optimize_window_reuse_duplicate(root, wfa, winref, wc_id)?;
            }
        }
    }

    // XXX remove any duplicate WindowFuncs from each WindowClause (planner.c:5950).
    for &winref in &order {
        let list = wfa.window_funcs[winref as usize].clone();
        if list.is_empty() {
            continue;
        }
        let mut newlist: Vec<pathnodes::NodeId> = Vec::with_capacity(list.len());
        for id in list {
            // list_member(newlist, lfirst(lc2)) — equality by node value.
            let already = newlist
                .iter()
                .any(|&n| exprs_equal_by_value(root, n, id));
            if !already {
                newlist.push(id);
            } else {
                wfa.num_window_funcs -= 1;
            }
        }
        wfa.window_funcs[winref as usize] = newlist;
    }
    Ok(())
}

/// `equal((Node *) a, (Node *) b)` over two arena Expr handles, by value.
fn exprs_equal_by_value(root: &PlannerInfo, a: pathnodes::NodeId, b: pathnodes::NodeId) -> bool {
    equalfuncs_seams::equal_expr::call(root.node(a), root.node(b))
}

/// Recursively rewrite every `WindowFunc` whose `winref == from` to `winref =
/// to` within an owned `Expr` tree.
///
/// In C, `wflists->windowFuncs[winref]` aliases the same `WindowFunc` structs
/// that live in `parse->targetList`, so `optimize_window_clauses` updating
/// `wfunc->winref` in place (planner.c:5925-5930) is automatically reflected in
/// the targetlist (and `setrefs.c` then does no winref rewriting). The arena
/// port interns `wflists.window_funcs` as deep copies, breaking that aliasing,
/// so after a window-clause merge we must rewrite the `processed_tlist` (and
/// other) WindowFunc trees explicitly.
fn rewrite_windowfunc_winref(node: Expr, from: u32, to: u32) -> Expr {
    use nodes_core::nodefuncs::expression_tree_mutator;
    // expression_tree_mutator is single-level (it invokes the closure on each
    // immediate child); self-recurse the closure to reach nested WindowFuncs.
    let mut out = expression_tree_mutator(node, &mut |child| {
        rewrite_windowfunc_winref(child, from, to)
    });
    if let Expr::WindowFunc(w) = &mut out {
        if w.winref == from {
            w.winref = to;
        }
    }
    out
}

/// `equal(a, b)` over two `partitionClause`/`orderClause` arena handle lists,
/// used by `optimize_window_clauses`'s duplicate check. These lists hold
/// `SortGroupClause` handles (interned via `alloc_sortgroupclause`), NOT `Expr`
/// handles, so they must be resolved as `SortGroupClause`s and compared by value
/// — mirroring C's node-type-dispatched `equal()` landing on
/// `_equalSortGroupClause`, not `_equalExpr`.
fn sortgroupclause_lists_equal(
    root: &PlannerInfo,
    a: &[pathnodes::NodeId],
    b: &[pathnodes::NodeId],
) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let av: alloc::vec::Vec<::nodes::rawnodes::SortGroupClause> =
        a.iter().map(|&id| *root.sortgroupclause(id)).collect();
    let bv: alloc::vec::Vec<::nodes::rawnodes::SortGroupClause> =
        b.iter().map(|&id| *root.sortgroupclause(id)).collect();
    equalfuncs_seams::equal_sortgroupclause_list::call(&av, &bv)
}

/// `equal(a, b)` over two optional arena handles (`Node *` that may be NULL),
/// used by `optimize_window_clauses`'s duplicate check for
/// `startOffset`/`endOffset`.
fn opt_nodes_equal(
    root: &PlannerInfo,
    a: Option<pathnodes::NodeId>,
    b: Option<pathnodes::NodeId>,
) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(x), Some(y)) => exprs_equal_by_value(root, x, y),
        _ => false,
    }
}

/// The duplicate-check-and-reuse branch of `optimize_window_clauses`
/// (planner.c:5876-5949). After a WindowClause's `frameOptions` have been
/// optimized, check whether it now matches another WindowClause's
/// partition/order/frame/offsets; if so, move all of this clause's WindowFuncs
/// into the existing clause (adjusting their `winref`) and empty this clause's
/// WindowFunc list. `transformWindowFuncCall` guarantees at most one such
/// duplicate, so we stop at the first match.
fn optimize_window_reuse_duplicate(
    root: &mut PlannerInfo,
    wfa: &mut WindowFuncListsArena,
    winref: u32,
    wc_id: pathnodes::NodeId,
) -> PgResult<()> {
    // Snapshot this clause's comparison fields.
    let (wc_part, wc_ord, wc_frame, wc_start, wc_end) = {
        let wc = root.windowclause(wc_id);
        (
            wc.partitionClause.clone(),
            wc.orderClause.clone(),
            wc.frameOptions,
            wc.startOffset,
            wc.endOffset,
        )
    };

    let order = wfa.clause_order.clone();
    for &other_winref in &order {
        let Some(existing_id) = wfa.clauses[other_winref as usize] else {
            continue;
        };
        // skip over the WindowClause we're currently editing.
        if existing_id == wc_id {
            continue;
        }

        let (e_part, e_ord, e_frame, e_start, e_end, e_winref) = {
            let ewc = root.windowclause(existing_id);
            (
                ewc.partitionClause.clone(),
                ewc.orderClause.clone(),
                ewc.frameOptions,
                ewc.startOffset,
                ewc.endOffset,
                ewc.winref,
            )
        };

        // Perform the same duplicate check that is done in
        // transformWindowFuncCall.
        if sortgroupclause_lists_equal(root, &wc_part, &e_part)
            && sortgroupclause_lists_equal(root, &wc_ord, &e_ord)
            && wc_frame == e_frame
            && opt_nodes_equal(root, wc_start, e_start)
            && opt_nodes_equal(root, wc_end, e_end)
        {
            // Move each WindowFunc in 'wc' into 'existing_wc': adjust its winref
            // and append it to existing_wc's WindowFunc list.
            let moved = wfa.window_funcs[winref as usize].clone();
            for &wfunc_id in &moved {
                if let ::nodes::primnodes::Expr::WindowFunc(w) = root.node_mut(wfunc_id) {
                    w.winref = e_winref;
                } else {
                    panic!("optimize_window_clauses: windowFuncs entry is not a WindowFunc");
                }
            }
            // The `wfa.window_funcs` handles are deep copies of the targetlist
            // WindowFuncs (build_window_func_lists_arena), so the in-place winref
            // update above does NOT reach `processed_tlist`. C relies on pointer
            // aliasing here; the arena port must rewrite the targetlist trees
            // explicitly so createplan/executor see the merged winref.
            let tlist_ids: alloc::vec::Vec<pathnodes::NodeId> = root.processed_tlist.clone();
            for te_id in tlist_ids {
                let expr_id = root.targetentry(te_id).expr;
                let taken = core::mem::replace(
                    root.node_mut(expr_id),
                    Expr::Const(::nodes::primnodes::Const::default()),
                );
                *root.node_mut(expr_id) = rewrite_windowfunc_winref(taken, winref, e_winref);
            }
            // list_concat(existing, wc); wc->list = NIL.
            wfa.window_funcs[other_winref as usize].extend(moved);
            wfa.window_funcs[winref as usize].clear();

            // transformWindowFuncCall() ensures no other duplicates exist.
            break;
        }
    }
    Ok(())
}

/// `select_active_windows(root, wflists)` (planner.c:5990) — build the ordered
/// list of active WindowClause handles (those with related WindowFuncs), sorted
/// by partition/order clauses via `common_prefix_cmp`.
fn select_active_windows<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    wfa: &mut WindowFuncListsArena,
) -> PgResult<Vec<pathnodes::NodeId>> {
    intern_parse_window_clauses(run, root, wfa)?;

    // Construct the array of active windows, each carrying its uniqueOrder =
    // list_concat_unique(partitionClause, orderClause).
    struct ActiveWin {
        wc: pathnodes::NodeId,
        unique_order: Vec<pathnodes::NodeId>,
    }
    let order = wfa.clause_order.clone();
    let mut actives: Vec<ActiveWin> = Vec::new();
    for &winref in &order {
        if wfa.window_funcs[winref as usize].is_empty() {
            continue;
        }
        let wc_id = wfa.clauses[winref as usize].expect("active window has no interned clause");
        let (part, ord) = {
            let wc = root.windowclause(wc_id);
            (wc.partitionClause.clone(), wc.orderClause.clone())
        };
        // list_concat_unique(list_copy(partitionClause), orderClause): partition
        // keys followed by order keys that don't duplicate a partition key (by
        // SortGroupClause value).
        let mut unique_order = part.clone();
        for o in ord {
            let o_sgc = *root.sortgroupclause(o);
            let dup = unique_order
                .iter()
                .any(|&p| *root.sortgroupclause(p) == o_sgc);
            if !dup {
                unique_order.push(o);
            }
        }
        actives.push(ActiveWin { wc: wc_id, unique_order });
    }

    // qsort(actives, common_prefix_cmp). Stable-sort is fine; C's qsort isn't
    // stable but the comparator is a total order on the keys it inspects.
    actives.sort_by(|a, b| common_prefix_cmp(root, &a.unique_order, &b.unique_order));

    Ok(actives.into_iter().map(|a| a.wc).collect())
}

/// `common_prefix_cmp(a, b)` (planner.c:6133) — order two windows by their
/// sorting clauses (highest tleSortGroupRef first; then sortop; then
/// nulls_first), and put the longer uniqueOrder first when one is a prefix.
fn common_prefix_cmp(
    root: &PlannerInfo,
    a: &[pathnodes::NodeId],
    b: &[pathnodes::NodeId],
) -> core::cmp::Ordering {
    use core::cmp::Ordering;
    let n = a.len().min(b.len());
    for i in 0..n {
        let sca = root.sortgroupclause(a[i]);
        let scb = root.sortgroupclause(b[i]);
        if sca.tleSortGroupRef > scb.tleSortGroupRef {
            return Ordering::Less;
        } else if sca.tleSortGroupRef < scb.tleSortGroupRef {
            return Ordering::Greater;
        } else if sca.sortop > scb.sortop {
            return Ordering::Less;
        } else if sca.sortop < scb.sortop {
            return Ordering::Greater;
        } else if sca.nulls_first && !scb.nulls_first {
            return Ordering::Less;
        } else if !sca.nulls_first && scb.nulls_first {
            return Ordering::Greater;
        }
        // eqop is fully determined by sortop; no need to compare.
    }
    b.len().cmp(&a.len())
}

/// `name_active_windows(activeWindows)` (planner.c:6055) — assign made-up names
/// (`w1`, `w2`, ...) to any unnamed WindowClauses, for EXPLAIN. Names must be
/// unique within the active list.
fn name_active_windows(
    root: &mut PlannerInfo,
    active_windows: &[pathnodes::NodeId],
) -> PgResult<()> {
    let mut next_n: i32 = 1;
    for &wc_id in active_windows {
        if root.windowclause(wc_id).name.is_some() {
            continue;
        }
        // Select a name not currently present in the list.
        loop {
            let newname = alloc::format!("w{next_n}");
            next_n += 1;
            let mut matched = false;
            for &wc2_id in active_windows {
                if let Some(n2) = &root.windowclause(wc2_id).name {
                    if n2 == &newname {
                        matched = true;
                        break;
                    }
                }
            }
            if !matched {
                root.windowclause_mut(wc_id).name = Some(newname);
                break;
            }
        }
    }
    Ok(())
}

/// `make_window_input_target(root, final_target, activeWindows)` (planner.c:6193)
/// — the PathTarget for the input to the first WindowAgg node. Non-flattenable
/// columns (window PARTITION/ORDER BY + GROUP BY items) are kept as-is; the rest
/// are flattened into their component Vars/Aggrefs.
fn make_window_input_target<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    final_target: &PathTarget,
    active_windows: &[pathnodes::NodeId],
) -> PgResult<PathTarget> {
    use vars::tlist::{
        add_column_to_pathtarget, add_new_columns_to_pathtarget, create_empty_pathtarget,
    };
    use vars::var::{
        pull_var_clause, PVC_INCLUDE_AGGREGATES, PVC_INCLUDE_PLACEHOLDERS, PVC_RECURSE_WINDOWFUNCS,
    };

    // Collect the sortgroupref numbers of window PARTITION/ORDER BY clauses + GROUP BY.
    let mut sgrefs: Vec<u32> = Vec::new();
    let add_sgref = |v: &mut Vec<u32>, r: u32| {
        if r != 0 && !v.contains(&r) {
            v.push(r);
        }
    };
    for &wc_id in active_windows {
        let (part, ord) = {
            let wc = root.windowclause(wc_id);
            (wc.partitionClause.clone(), wc.orderClause.clone())
        };
        for sgc in part {
            add_sgref(&mut sgrefs, root.sortgroupclause(sgc).tleSortGroupRef);
        }
        for sgc in ord {
            add_sgref(&mut sgrefs, root.sortgroupclause(sgc).tleSortGroupRef);
        }
    }
    let group_clause = root.processed_groupClause.clone();
    for sgc in group_clause {
        add_sgref(&mut sgrefs, root.sortgroupclause(sgc).tleSortGroupRef);
    }

    // Non-flattenable items kept as-is; the rest saved for pull_var_clause.
    let mut input_target = create_empty_pathtarget();
    let mut flattenable_cols: Vec<pathnodes::NodeId> = Vec::new();
    for (i, &expr_id) in final_target.exprs.iter().enumerate() {
        let sgref = get_pathtarget_sortgroupref(final_target, i);
        if sgref != 0 && sgrefs.contains(&sgref) {
            // Don't deconstruct this value; add it as-is.
            add_column_to_pathtarget(&mut input_target, expr_id, sgref);
        } else {
            flattenable_cols.push(expr_id);
        }
    }

    // Pull out all Vars and Aggrefs in flattenable columns (recursing into
    // WindowFuncs to reach their input expressions), and add them.
    let flags = PVC_INCLUDE_AGGREGATES | PVC_RECURSE_WINDOWFUNCS | PVC_INCLUDE_PLACEHOLDERS;
    let mut flattenable_var_ids: Vec<pathnodes::NodeId> = Vec::new();
    for &nid in &flattenable_cols {
        let scratch = mcx::MemoryContext::new("make_window_input_target pull_var_clause");
        let node = nodes_core::node_walker::node_expr_wrapper(root.node(nid), scratch.mcx());
        // Deep-copy collected nodes into the planner mcx (re-interned via
        // `alloc_node` below); a plain `.clone()` panics on a context-allocated
        // child (Aggref TargetEntry args, e.g. `SUM(SUM(x)) OVER ...`).
        let vars = pull_var_clause(mcx, &node, flags)?;
        for v in vars {
            flattenable_var_ids.push(root.alloc_node(v));
        }
    }
    add_new_columns_to_pathtarget(root, &mut input_target, &flattenable_var_ids);

    // XXX this causes some redundant cost calculation ...
    costsize::sizeest::set_pathtarget_cost_width(root, &mut input_target);
    Ok(input_target)
}

/// `make_pathkeys_for_window(root, wc, tlist)` (planner.c:6313) — the required
/// input ordering for a WindowClause: PARTITION keys then ORDER keys. Removes
/// redundant partition clauses from `wc->partitionClause` in place.
fn make_pathkeys_for_window(
    root: &mut PlannerInfo,
    mcx: Mcx<'_>,
    wc_id: pathnodes::NodeId,
    tlist: &[pathnodes::NodeId],
) -> PgResult<Vec<pathnodes::PathKey>> {
    // Throw error if can't sort (grouping_is_sortable over partition/order clauses).
    let (part_ids, ord_ids) = {
        let wc = root.windowclause(wc_id);
        (wc.partitionClause.clone(), wc.orderClause.clone())
    };
    let part_sgcs: Vec<::nodes::rawnodes::SortGroupClause> =
        part_ids.iter().map(|&id| *root.sortgroupclause(id)).collect();
    let ord_sgcs: Vec<::nodes::rawnodes::SortGroupClause> =
        ord_ids.iter().map(|&id| *root.sortgroupclause(id)).collect();
    if !vars::tlist::grouping_is_sortable(&part_sgcs) {
        return Err(PgError::error("could not implement window PARTITION BY")
            .with_sqlstate(types_error::error::ERRCODE_FEATURE_NOT_SUPPORTED)
            .with_detail("Window partitioning columns must be of sortable datatypes."));
    }
    if !vars::tlist::grouping_is_sortable(&ord_sgcs) {
        return Err(PgError::error("could not implement window ORDER BY")
            .with_sqlstate(types_error::error::ERRCODE_FEATURE_NOT_SUPPORTED)
            .with_detail("Window ordering columns must be of sortable datatypes."));
    }

    let mut window_pathkeys: Vec<pathnodes::PathKey> = Vec::new();

    // PARTITION BY pathkeys, removing redundant clauses from wc->partitionClause.
    if !part_ids.is_empty() {
        let mut part = part_ids.clone();
        let (pathkeys, sortable) =
            pathkeys::make_pathkeys_for_sortclauses_extended(
                root, mcx, &mut part, tlist, true, // remove_redundant
                false, false,
            );
        debug_assert!(sortable);
        // Reflect the redundant-clause removal back onto wc->partitionClause.
        root.windowclause_mut(wc_id).partitionClause = part;
        window_pathkeys = pathkeys;
    }

    // ORDER BY pathkeys appended (we must NOT remove redundant ones — RANGE
    // OFFSET needs the ordering column for in_range tests).
    if !ord_ids.is_empty() {
        let orderby_pathkeys = pathkeys::make_pathkeys_for_sortclauses(
            root, mcx, &ord_ids, tlist,
        );
        window_pathkeys = if !window_pathkeys.is_empty() {
            pathkeys::append_pathkeys(root, window_pathkeys, &orderby_pathkeys)
        } else {
            orderby_pathkeys
        };
    }

    Ok(window_pathkeys)
}

/// `create_window_paths(root, input_rel, input_target, output_target,
/// output_target_parallel_safe, wflists, activeWindows)` (planner.c:4533).
fn create_window_paths<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    input_rel: RelId,
    input_target: &PathTarget,
    output_target: &PathTarget,
    _output_target_parallel_safe: bool,
    wfa: &WindowFuncListsArena,
    active_windows: &[pathnodes::NodeId],
) -> PgResult<RelId> {
    // For now, do all work in the (WINDOW, NULL) upperrel.
    let window_rel = relnode::fetch_upper_rel(root, UPPERREL_WINDOW, &None);

    // consider_parallel: glob is parallel-unsafe on this path, so the input rel
    // is never consider_parallel and window_rel stays non-parallel.
    // FDW propagation.
    {
        let (serverid, userid, useridiscurrent, has_fdwroutine) = {
            let ir = root.rel(input_rel);
            (ir.serverid, ir.userid, ir.useridiscurrent, ir.has_fdwroutine)
        };
        let wr = root.rel_mut(window_rel);
        wr.serverid = serverid;
        wr.userid = userid;
        wr.useridiscurrent = useridiscurrent;
        wr.has_fdwroutine = has_fdwroutine;
    }

    // Consider computing window functions starting from the cheapest-total path
    // as well as any existing paths that satisfy/partially satisfy
    // root->window_pathkeys.
    let cheapest_total = root.rel(input_rel).cheapest_total_path;
    let window_pathkeys = root.window_pathkeys.clone();
    let pathlist = root.rel(input_rel).pathlist.clone();
    for path in pathlist {
        let path_pathkeys = root.path(path).base().pathkeys.clone();
        let (contained, presorted_keys) =
            pathkeys::pathkeys_count_contained_in(
                &window_pathkeys,
                &path_pathkeys,
            );
        if Some(path) == cheapest_total || contained || presorted_keys > 0 {
            create_one_window_path(
                run,
                root,
                window_rel,
                path,
                input_target,
                output_target,
                wfa,
                active_windows,
            )?;
        }
    }

    // GetForeignUpperPaths / create_upper_paths_hook: none modeled.
    pathnode::set_cheapest(root, window_rel)?;
    Ok(window_rel)
}

/// `create_one_window_path(root, window_rel, path, input_target, output_target,
/// wflists, activeWindows)` (planner.c:4620) — stack a WindowAgg node per active
/// window clause (with sorts between as needed) atop `path`, adding the result
/// to `window_rel`.
fn create_one_window_path<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    window_rel: RelId,
    mut path: PathId,
    input_target: &PathTarget,
    output_target: &PathTarget,
    wfa: &WindowFuncListsArena,
    active_windows: &[pathnodes::NodeId],
) -> PgResult<()> {
    // window_target starts as input_target; each intermediate WindowAgg adds its
    // window-func outputs; the topmost installs output_target.
    let mut window_target: PathTarget = input_target.clone();
    // topqual accumulates the lower windows' runconditions (planner.c): a
    // run-condition built for a non-top WindowAgg must still be applied as a qual
    // on the top WindowAgg.
    let mut topqual: Vec<pathnodes::NodeId> = Vec::new();
    let tlist = root.processed_tlist.clone();
    let n = active_windows.len();

    for (idx, &wc_id) in active_windows.iter().enumerate() {
        let winref = root.windowclause(wc_id).winref;
        let window_pathkeys = make_pathkeys_for_window(root, run.mcx(), wc_id, &tlist)?;

        let path_pathkeys = root.path(path).base().pathkeys.clone();
        let (is_sorted, presorted_keys) =
            pathkeys::pathkeys_count_contained_in(
                &window_pathkeys,
                &path_pathkeys,
            );

        // Sort if necessary.
        if !is_sorted {
            if presorted_keys == 0 || !enable_incremental_sort() {
                path = pathnode::create::create_sort_path(
                    root,
                    window_rel,
                    path,
                    window_pathkeys.clone(),
                    -1.0,
                )?;
            } else {
                path = pathnode::create::create_incremental_sort_path(
                    root,
                    run,
                    window_rel,
                    path,
                    window_pathkeys.clone(),
                    presorted_keys,
                    -1.0,
                )?;
            }
        }

        let is_last = idx == n - 1;
        let func_ids = wfa.window_funcs[winref as usize].clone();
        if !is_last {
            // Add the current WindowFuncs to an intermediate window_target (copy
            // to avoid mutating the previous path's target).
            let mut wt = vars::tlist::copy_pathtarget(&window_target);
            let mut tuple_width: i64 = wt.width as i64;
            for &wfn in &func_ids {
                vars::tlist::add_column_to_pathtarget(&mut wt, wfn, 0);
                let Some(w) = root.node(wfn).as_windowfunc() else {
                    panic!("create_one_window_path: windowFuncs entry is not a WindowFunc");
                };
                let wintype = w.wintype;
                tuple_width +=
                    lsyscache::type_::get_typavgwidth(wintype, -1)? as i64;
            }
            wt.width = costsize::clamp_width_est(tuple_width);
            window_target = wt;
        } else {
            // Install the goal target in the topmost WindowAgg.
            window_target = output_target.clone();
        }

        // topwindow = last item.
        let topwindow = is_last;

        // Collect WindowFuncRunConditions from each WindowFunc and convert them
        // into OpExprs (planner.c:4726-4766). For each WindowFuncRunCondition we
        // build `<wfunc> op <arg>` (or `<arg> op <wfunc>` when !wfunc_left), a
        // boolean-returning OpExpr, and append it to `runcondition` (and to
        // `topqual` when this is not the top window).
        const BOOLOID: types_core::primitive::Oid = 16;
        let mcx = run.mcx();
        let mut runcondition: Vec<pathnodes::NodeId> = Vec::new();
        for &wfn in &func_ids {
            // Snapshot the WindowFunc and its run-conditions out of the arena so
            // we can re-borrow `root` mutably for `alloc_node`.
            let (wfunc_expr, run_conds): (Expr<'mcx>, Vec<::nodes::primnodes::WindowFuncRunCondition<'mcx>>) = {
                let Some(w) = root.node(wfn).as_windowfunc() else {
                    panic!("create_one_window_path: windowFuncs entry is not a WindowFunc");
                };
                let mut rcs = Vec::with_capacity(w.runCondition.len());
                for rc in w.runCondition.iter() {
                    match rc {
                        Expr::WindowFuncRunCondition(r) => rcs.push(r.clone_in(mcx)?),
                        other => panic!(
                            "create_one_window_path: WindowFunc.runCondition holds a \
                             non-WindowFuncRunCondition Expr ({:?})",
                            other.expr_tag()
                        ),
                    }
                }
                (Expr::WindowFunc(w.clone_in(mcx)?), rcs)
            };

            for wfuncrc in run_conds.into_iter() {
                let arg = match wfuncrc.arg {
                    Some(a) => *a,
                    None => panic!("create_one_window_path: WindowFuncRunCondition has no arg"),
                };
                let (leftop, rightop) = if wfuncrc.wfunc_left {
                    (wfunc_expr.clone_in(mcx)?, arg)
                } else {
                    (arg, wfunc_expr.clone_in(mcx)?)
                };
                let opexpr = nodes_core::makefuncs::make_opclause(
                    wfuncrc.opno,
                    BOOLOID,
                    false,
                    leftop,
                    Some(rightop),
                    types_core::primitive::InvalidOid,
                    wfuncrc.inputcollid,
                );
                let opexpr_id = root.alloc_node(opexpr);
                runcondition.push(opexpr_id);
                if !topwindow {
                    topqual.push(opexpr_id);
                }
            }
        }

        path = pathnode_seams::create_windowagg_path::call(
            run,
            root,
            window_rel,
            path,
            Box::new(window_target.clone()),
            func_ids.clone(),
            runcondition,
            wc_id,
            if topwindow { topqual.clone() } else { Vec::new() },
            topwindow,
        )?;
    }

    pathnode::add_path(root, window_rel, path)?;
    Ok(())
}

/// `windowfunc_cost(root, wfunc)` (cost_windowagg inner loop) — the per-WindowFunc
/// cost contribution: `add_function_cost(winfnoid)` startup + per-row, plus
/// `cost_qual_eval_node(wfunc->args)` + `cost_qual_eval_node(wfunc->aggfilter)`
/// per-row contributions.
fn windowfunc_cost_impl<'mcx>(
    run: &pathnodes::planner_run::PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    wfunc: pathnodes::NodeId,
) -> (types_core::primitive::Cost, types_core::primitive::Cost) {
    let Some(w) = root.node(wfunc).as_windowfunc() else {
        panic!("windowfunc_cost: node is not a WindowFunc");
    };
    // Deep-copy the args/aggfilter into the planner mcx (re-interned via
    // `alloc_node` below). A plain `.clone()` panics on a context-allocated
    // child: the WindowFunc args may carry an `Aggref` (e.g. the inner agg of
    // `SUM(SUM(x)) OVER ...`).
    let winfnoid = w.winfnoid;
    let mut args: Vec<Expr> = Vec::with_capacity(w.args.len());
    for a in w.args.iter() {
        match a.clone_in(run.mcx()) {
            Ok(c) => args.push(c),
            Err(_) => return (0.0, 0.0),
        }
    }
    let aggfilter: Option<Expr> = match &w.aggfilter {
        Some(f) => match f.clone_in(run.mcx()) {
            Ok(c) => Some(c),
            Err(_) => return (0.0, 0.0),
        },
        None => None,
    };

    // add_function_cost(root, winfnoid, (Node *) wfunc): startup + per_tuple.
    let (mut startup, mut per_tuple) =
        match plancat::add_function_cost(Some(root), winfnoid, Some(wfunc)) {
            Ok(v) => v,
            Err(_) => (0.0, 0.0),
        };

    // cost_qual_eval_node(&argcosts, (Node *) wfunc->args, root).
    for arg in args {
        let id = root.alloc_node(arg);
        let qc = costsize::cost_qual_eval_node(root, id);
        startup += qc.startup;
        per_tuple += qc.per_tuple;
    }
    // cost_qual_eval_node(&argcosts, (Node *) wfunc->aggfilter, root).
    if let Some(f) = aggfilter {
        let id = root.alloc_node(f);
        let qc = costsize::cost_qual_eval_node(root, id);
        startup += qc.startup;
        per_tuple += qc.per_tuple;
    }

    (startup, per_tuple)
}

/// `windowclause_cost_info` seam owner: the WindowClause column counts +
/// `get_windowclause_startup_tuples` estimate.
fn windowclause_cost_info_impl<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    winclause: pathnodes::NodeId,
    input_tuples: f64,
) -> PgResult<costsize_seams::WindowClauseCostInfo> {
    let (num_part_cols, num_order_cols) = {
        let wc = root.windowclause(winclause);
        (wc.partitionClause.len() as i32, wc.orderClause.len() as i32)
    };
    let startup_tuples = get_windowclause_startup_tuples(run, root, winclause, input_tuples)?;
    Ok(costsize_seams::WindowClauseCostInfo {
        num_part_cols,
        num_order_cols,
        startup_tuples,
    })
}

/// `get_windowclause_startup_tuples(root, wc, input_tuples)` (costsize.c:2884) —
/// estimate how many subnode tuples must be read before the first WindowAgg row.
fn get_windowclause_startup_tuples<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    winclause: pathnodes::NodeId,
    input_tuples: f64,
) -> PgResult<f64> {
    // Frame-option bits (parsenodes.h).
    const FRAMEOPTION_ROWS: i32 = 0x00004;
    const FRAMEOPTION_RANGE: i32 = 0x00002;
    const FRAMEOPTION_GROUPS: i32 = 0x00008;
    const FRAMEOPTION_END_UNBOUNDED_FOLLOWING: i32 = 0x00100;
    const FRAMEOPTION_END_CURRENT_ROW: i32 = 0x00400;
    const FRAMEOPTION_END_OFFSET_PRECEDING: i32 = 0x01000;
    const FRAMEOPTION_END_OFFSET_FOLLOWING: i32 = 0x04000;

    let (frame_options, part_ids, ord_ids, end_off_id) = {
        let wc = root.windowclause(winclause);
        (
            wc.frameOptions,
            wc.partitionClause.clone(),
            wc.orderClause.clone(),
            wc.endOffset,
        )
    };
    let tlist = root.processed_tlist.clone();

    // partition_tuples = input_tuples / num_partitions.
    let partition_tuples = if !part_ids.is_empty() {
        let part_exprs = sortgrouplist_exprs_arena(root, &part_ids, &tlist);
        let num_partitions = selfuncs_seams::estimate_num_groups::call(
            run, root, &part_exprs, input_tuples, None,
        )?;
        input_tuples / num_partitions
    } else {
        input_tuples
    };

    // peer_tuples = partition_tuples / num_groups (per ORDER BY peer group).
    let peer_tuples = if !ord_ids.is_empty() {
        let order_exprs = sortgrouplist_exprs_arena(root, &ord_ids, &tlist);
        let num_groups = selfuncs_seams::estimate_num_groups::call(
            run, root, &order_exprs, partition_tuples, None,
        )?;
        partition_tuples / num_groups
    } else {
        1.0
    };

    let return_tuples = if frame_options & FRAMEOPTION_END_UNBOUNDED_FOLLOWING != 0 {
        partition_tuples
    } else if frame_options & FRAMEOPTION_END_CURRENT_ROW != 0 {
        if frame_options & FRAMEOPTION_ROWS != 0 {
            1.0
        } else if frame_options & (FRAMEOPTION_RANGE | FRAMEOPTION_GROUPS) != 0 {
            if ord_ids.is_empty() {
                partition_tuples
            } else {
                peer_tuples
            }
        } else {
            1.0
        }
    } else if frame_options & FRAMEOPTION_END_OFFSET_PRECEDING != 0 {
        1.0
    } else if frame_options & FRAMEOPTION_END_OFFSET_FOLLOWING != 0 {
        // INT2OID = 21, INT4OID = 23, INT8OID = 20 (pg_type_d.h);
        // DEFAULT_INEQ_SEL = 1/3 (selfuncs.h).
        const INT2OID: types_core::primitive::Oid = 21;
        const DEFAULT_INEQ_SEL: f64 = 0.3333333333333333;
        // try and figure out the value specified in the endOffset. Borrow the
        // node (only inspected here); a derived `.clone()` would panic on a
        // context-allocated child.
        let end_off_node: Option<&Expr> = end_off_id.map(|id| root.node(id));
        let end_offset_value = if let Some(c) = end_off_node.and_then(|e| e.as_const()) {
            if c.constisnull {
                // NULLs aren't allowed; pretend just the first row is needed.
                1.0
            } else {
                match c.consttype {
                    x if x == INT2OID => c.constvalue.as_i16() as f64,
                    x if x == types_core::catalog::INT4OID => c.constvalue.as_i32() as f64,
                    x if x == types_core::catalog::INT8OID => c.constvalue.as_i64() as f64,
                    _ => partition_tuples / peer_tuples * DEFAULT_INEQ_SEL,
                }
            }
        } else {
            // Non-Const end bound: guess via DEFAULT_INEQ_SEL.
            partition_tuples / peer_tuples * DEFAULT_INEQ_SEL
        };
        if frame_options & FRAMEOPTION_ROWS != 0 {
            // include the N FOLLOWING and the current row.
            end_offset_value + 1.0
        } else if frame_options & (FRAMEOPTION_RANGE | FRAMEOPTION_GROUPS) != 0 {
            // include N FOLLOWING ranges/groups and the initial range/group.
            peer_tuples * (end_offset_value + 1.0)
        } else {
            1.0
        }
    } else {
        1.0
    };

    // Cap the return value to the estimated partition tuples (+1 for the extra
    // tuple WindowAgg reads to confirm the partition/peer boundary).
    let return_tuples = if !part_ids.is_empty() || !ord_ids.is_empty() {
        (return_tuples + 1.0).min(partition_tuples)
    } else {
        return_tuples.min(partition_tuples)
    };

    Ok(costsize::clamp_row_est(return_tuples))
}

/// `get_sortgrouplist_exprs(sgClauses, targetList)` over arena handles, returning
/// the interned tlist-expression handles the `estimate_num_groups` seam expects.
fn sortgrouplist_exprs_arena(
    root: &mut PlannerInfo,
    sg_ids: &[pathnodes::NodeId],
    tlist: &[pathnodes::NodeId],
) -> Vec<pathnodes::NodeId> {
    let mut out = Vec::with_capacity(sg_ids.len());
    for &sgc in sg_ids {
        let id = nodeFuncs_seams::get_sortgroupclause_expr::call(root, sgc, tlist);
        out.push(id);
    }
    out
}

// ===========================================================================
// GROUPING SETS preprocessing (planner.c:2181) — preprocess_grouping_sets,
// extract_rollup_sets, reorder_grouping_sets, remap_to_groupclause_idx.
// ===========================================================================

use nodes_core::bitmapset as bms;
use ::nodes::bitmapset::Bitmapset;
use mcx::PgBox;
use pathnodes::{GroupingSetData, RollupData};

/// `grouping_sets_data` (planner.c:99) — data specific to grouping sets, carried
/// through grouping-path generation. `tleref_to_colnum_map` is the scratch array
/// `remap_to_groupclause_idx` rewrites on each call; `unsortable_refs` /
/// `unhashable_refs` are sortgroupref bitmapsets.
struct GroupingSetsData<'mcx> {
    rollups: Vec<RollupData>,
    hash_sets_idx: Vec<Vec<i32>>,
    #[allow(non_snake_case)]
    dNumHashGroups: f64,
    any_hashable: bool,
    unsortable_refs: Option<PgBox<'mcx, Bitmapset<'mcx>>>,
    unhashable_refs: Option<PgBox<'mcx, Bitmapset<'mcx>>>,
    unsortable_sets: Vec<GroupingSetData>,
    tleref_to_colnum_map: Vec<i32>,
}

/// `remap_to_groupclause_idx(groupClause, gsets, tleref_to_colnum_map)`
/// (planner.c:2362). Given a groupClause (arena SortGroupClause handles) and a
/// list of GroupingSetData, return equivalent sets mapped to 0-based indexes
/// into the groupClause. The scratch `tleref_to_colnum_map` is rewritten here.
fn remap_to_groupclause_idx(
    root: &PlannerInfo,
    group_clause: &[pathnodes::NodeId],
    gsets: &[GroupingSetData],
    tleref_to_colnum_map: &mut [i32],
) -> Vec<Vec<i32>> {
    let mut refn: i32 = 0;
    for &gid in group_clause {
        let sgr = root.sortgroupclause(gid).tleSortGroupRef as usize;
        tleref_to_colnum_map[sgr] = refn;
        refn += 1;
    }

    let mut result: Vec<Vec<i32>> = Vec::with_capacity(gsets.len());
    for gs in gsets {
        let mut set: Vec<i32> = Vec::with_capacity(gs.set.len());
        for &gref in &gs.set {
            set.push(tleref_to_colnum_map[gref as usize]);
        }
        result.push(set);
    }
    result
}

/// `reorder_grouping_sets(groupingSets, sortclause)` (planner.c:3136). Reorder
/// the elements of a list of grouping sets so they have correct prefix
/// relationships, inserting the GroupingSetData annotations. Input ordered with
/// smallest sets first; result returned with largest sets first. The result
/// shares no list substructure with the input. `sortclause` is a slice of
/// SortGroupClause values (parse->sortClause) used to follow column order where
/// possible.
fn reorder_grouping_sets(
    grouping_sets: &[Vec<i32>],
    sortclause: &[::nodes::rawnodes::SortGroupClause],
) -> Vec<GroupingSetData> {
    let mut previous: Vec<i32> = Vec::new();
    let mut result: Vec<GroupingSetData> = Vec::new();
    // The sortclause is "given up on" (treated as NIL) once we diverge.
    let mut sortclause_len = sortclause.len();

    for candidate in grouping_sets {
        // new_elems = list_difference_int(candidate, previous).
        let mut new_elems: Vec<i32> = candidate
            .iter()
            .copied()
            .filter(|e| !previous.contains(e))
            .collect();

        while sortclause_len > previous.len() && !new_elems.is_empty() {
            let sc = &sortclause[previous.len()];
            let r = sc.tleSortGroupRef as i32;

            if let Some(pos) = new_elems.iter().position(|&e| e == r) {
                previous.push(r);
                new_elems.remove(pos);
            } else {
                // diverged from the sortclause; give up on it.
                sortclause_len = 0;
                break;
            }
        }

        previous.extend_from_slice(&new_elems);

        let gs = GroupingSetData {
            set: previous.iter().map(|&i| i as types_core::Index).collect(),
            numGroups: 0.0,
        };
        // lcons(gs, result) — prepend.
        result.insert(0, gs);
    }

    result
}

/// `extract_rollup_sets(groupingSets)` (planner.c:2924). Extract lists of
/// grouping sets that can each be implemented using a single rollup-type
/// aggregate pass. Returns a list of lists of grouping sets. Input must be
/// sorted with smallest sets first; each result sublist is sorted likewise.
///
/// Uses bipartite matching to find the minimal partition of the grouping-set
/// poset (ordered by set inclusion) into chains.
fn extract_rollup_sets<'mcx>(
    mcx: Mcx<'mcx>,
    grouping_sets: &[Vec<i32>],
) -> PgResult<Vec<Vec<Vec<i32>>>> {
    let num_sets_raw = grouping_sets.len();
    let mut num_empty = 0usize;

    // Strip out leading empty sets (the planner needs them all in the first
    // list; they are added back after).
    let mut lc1 = 0usize;
    while lc1 < grouping_sets.len() && grouping_sets[lc1].is_empty() {
        num_empty += 1;
        lc1 += 1;
    }

    // Bail out now if all we had were empty sets.
    if lc1 >= grouping_sets.len() {
        return Ok(alloc::vec![grouping_sets.to_vec()]);
    }

    // orig_sets[i], set_masks[i], adjacency[i] are 1-indexed (0 left free for
    // the NIL node in the graph algorithm).
    let mut orig_sets: Vec<Vec<Vec<i32>>> = (0..=num_sets_raw).map(|_| Vec::new()).collect();
    let mut set_masks: Vec<Option<PgBox<Bitmapset>>> =
        (0..=num_sets_raw).map(|_| None).collect();
    let mut adjacency: Vec<Vec<i16>> = (0..=num_sets_raw).map(|_| Vec::new()).collect();
    let mut adjacency_buf: Vec<i16> = alloc::vec![0i16; num_sets_raw + 1];

    let mut j_size: usize = 0;
    let mut j: usize = 0;
    let mut i: usize = 1;

    for candidate in &grouping_sets[lc1..] {
        // candidate_set = bms of candidate's members.
        let mut candidate_set: Option<PgBox<Bitmapset>> = None;
        for &m in candidate {
            candidate_set = Some(bms::bms_add_member(mcx, candidate_set, m)?);
        }

        let mut dup_of: usize = 0;
        // we can only be a dup if we're the same length as a previous set.
        if j_size == candidate.len() {
            for k in j..i {
                if bms::bms_equal(
                    set_masks[k].as_deref(),
                    candidate_set.as_deref(),
                ) {
                    dup_of = k;
                    break;
                }
            }
        } else if j_size < candidate.len() {
            j_size = candidate.len();
            j = i;
        }

        if dup_of > 0 {
            orig_sets[dup_of].push(candidate.clone());
            // bms_free(candidate_set) — drop.
        } else {
            let mut n_adj: usize = 0;
            orig_sets[i].push(candidate.clone());

            // fill in adjacency list; no need to compare equal-size sets.
            let mut k = j;
            while k > 1 {
                k -= 1;
                if bms::bms_is_subset(set_masks[k].as_deref(), candidate_set.as_deref()) {
                    n_adj += 1;
                    adjacency_buf[n_adj] = k as i16;
                }
            }

            set_masks[i] = candidate_set;

            if n_adj > 0 {
                adjacency_buf[0] = n_adj as i16;
                adjacency[i] = adjacency_buf[0..=n_adj].to_vec();
            } else {
                adjacency[i] = Vec::new();
            }

            i += 1;
        }
    }

    let num_sets = i - 1;

    // Apply the graph matching algorithm.
    let adj_slices: Vec<&[i16]> = adjacency.iter().map(|v| v.as_slice()).collect();
    let state = bipartite_match::BipartiteMatch(
        num_sets as i32,
        num_sets as i32,
        &adj_slices,
    )?;

    // Assign sets to chains. Two sets (u,v) belong to the same chain if
    // pair_uv[u] = v or pair_vu[v] = u.
    let mut chains: Vec<i32> = alloc::vec![0i32; num_sets + 1];
    let mut num_chains: i32 = 0;
    for idx in 1..=num_sets {
        let u = state.pair_vu[idx] as i32;
        let v = state.pair_uv[idx] as i32;

        if u > 0 && (u as usize) < idx {
            chains[idx] = chains[u as usize];
        } else if v > 0 && (v as usize) < idx {
            chains[idx] = chains[v as usize];
        } else {
            num_chains += 1;
            chains[idx] = num_chains;
        }
    }

    // build result lists.
    let mut results: Vec<Vec<Vec<i32>>> = (0..=num_chains as usize).map(|_| Vec::new()).collect();
    for idx in 1..=num_sets {
        let c = chains[idx] as usize;
        debug_assert!(c > 0);
        let taken = core::mem::take(&mut orig_sets[idx]);
        results[c].extend(taken);
    }

    // push any empty sets back on the first list.
    let mut ne = num_empty;
    while ne > 0 {
        ne -= 1;
        results[1].insert(0, Vec::new());
    }

    // make result list.
    let mut result: Vec<Vec<Vec<i32>>> = Vec::with_capacity(num_chains as usize);
    for idx in 1..=num_chains as usize {
        result.push(core::mem::take(&mut results[idx]));
    }

    bipartite_match::BipartiteMatchFree(state);

    Ok(result)
}

/// `preprocess_grouping_sets(root)` (planner.c:2181). Build the
/// `grouping_sets_data` from `parse->groupingSets` (already expanded into a list
/// of `T_IntList` sortgroupref sets by subquery_planner). Also duplicates
/// `parse->groupClause` into `root->processed_groupClause`.
fn preprocess_grouping_sets<'mcx>(
    mcx: Mcx<'mcx>,
    run: &mut PlannerRun<'mcx>,
    root: &mut PlannerInfo,
) -> PgResult<GroupingSetsData<'mcx>> {
    // We don't optimize the groupClause with grouping sets; just duplicate it
    // into processed_groupClause (C:2191). Intern parse->groupClause to stable
    // arena handles, mirroring preprocess_groupclause's identity contract.
    let processed_group_clause: Vec<pathnodes::NodeId> = {
        let group_clauses: Vec<::nodes::rawnodes::SortGroupClause> = run
            .resolve(root.parse)
            .groupClause
            .iter()
            .map(|np| sortgroupclause_from_node(np))
            .collect::<PgResult<Vec<_>>>()?;
        group_clauses
            .into_iter()
            .map(|sgc| root.alloc_sortgroupclause(sgc))
            .collect()
    };
    root.processed_groupClause = processed_group_clause.clone();

    let mut any_hashable = false;
    let mut unhashable_refs: Option<PgBox<Bitmapset>> = None;
    let mut unsortable_refs: Option<PgBox<Bitmapset>> = None;
    let mut unsortable_sets: Vec<GroupingSetData> = Vec::new();

    // Detect unhashable and unsortable grouping expressions (C:2211).
    let mut maxref: i32 = 0;
    {
        let group_clauses: Vec<::nodes::rawnodes::SortGroupClause> = run
            .resolve(root.parse)
            .groupClause
            .iter()
            .map(|np| sortgroupclause_from_node(np))
            .collect::<PgResult<Vec<_>>>()?;
        for gc in &group_clauses {
            let refn = gc.tleSortGroupRef as i32;
            if refn > maxref {
                maxref = refn;
            }
            if !gc.hashable {
                unhashable_refs = Some(bms::bms_add_member(mcx, unhashable_refs, refn)?);
            }
            if gc.sortop == 0 {
                unsortable_refs = Some(bms::bms_add_member(mcx, unsortable_refs, refn)?);
            }
        }
    }

    // Allocate workspace array for remapping (C:2222).
    let mut tleref_to_colnum_map: Vec<i32> = alloc::vec![0i32; (maxref + 1) as usize];

    // Read the expanded grouping sets (each a T_IntList of sortgrouprefs).
    let grouping_sets: Vec<Vec<i32>> = {
        let parse = run.resolve(root.parse);
        let mut out: Vec<Vec<i32>> = Vec::with_capacity(parse.groupingSets.len());
        for np in parse.groupingSets.iter() {
            let np = np.as_ref();
            match np.as_intlist() {
                Some(l) => out.push(l.iter().copied().collect()),
                None => {
                    return Err(PgError::error(alloc::format!(
                        "preprocess_grouping_sets: groupingSets element is not a T_IntList (tag {:?})",
                        np.node_tag()
                    )));
                }
            }
        }
        out
    };

    // If we have any unsortable sets, extract them before preparing rollups.
    // Unsortable sets don't go through reorder_grouping_sets, so the
    // GroupingSetData annotation is applied here (C:2231).
    let sets: Vec<Vec<Vec<i32>>>;
    if !bms::bms_is_empty(unsortable_refs.as_deref()) {
        let mut sortable_sets: Vec<Vec<i32>> = Vec::new();
        for gset in &grouping_sets {
            if bms::bms_overlap_list(unsortable_refs.as_deref(), gset) {
                let gs = GroupingSetData {
                    set: gset.iter().map(|&i| i as types_core::Index).collect(),
                    numGroups: 0.0,
                };
                unsortable_sets.push(gs);

                // An unsortable set must be hashable; later code assumes this.
                if bms::bms_overlap_list(unhashable_refs.as_deref(), gset) {
                    return Err(PgError::error("could not implement GROUP BY")
                        .with_sqlstate(types_error::error::ERRCODE_FEATURE_NOT_SUPPORTED)
                        .with_detail(
                            "Some of the datatypes only support hashing, while others \
                             only support sorting.",
                        ));
                }
            } else {
                sortable_sets.push(gset.clone());
            }
        }

        sets = if !sortable_sets.is_empty() {
            extract_rollup_sets(mcx, &sortable_sets)?
        } else {
            Vec::new()
        };
    } else {
        sets = extract_rollup_sets(mcx, &grouping_sets)?;
    }

    // parse->sortClause as SortGroupClause values, for reorder_grouping_sets.
    let sort_clause_vals: Vec<::nodes::rawnodes::SortGroupClause> = run
        .resolve(root.parse)
        .sortClause
        .iter()
        .map(|np| sortgroupclause_from_node(np))
        .collect::<PgResult<Vec<_>>>()?;

    let mut rollups: Vec<RollupData> = Vec::new();
    let sets_len = sets.len();
    for current_sets_raw in sets {
        // Reorder the current list of grouping sets into correct prefix order.
        // If only one aggregation pass is needed, try to match the ORDER BY
        // clause; otherwise don't bother. This reorders from smallest-first to
        // largest-first and applies GroupingSetData annotations (C:2289).
        let sortclause: &[::nodes::rawnodes::SortGroupClause] = if sets_len == 1 {
            &sort_clause_vals
        } else {
            &[]
        };
        let current_sets = reorder_grouping_sets(&current_sets_raw, sortclause);

        // Get the initial (largest) grouping set (C:2298).
        let gs0_set: Vec<types_core::Index> = current_sets[0].set.clone();

        // Order the groupClause appropriately. If the first grouping set is
        // empty, the groupClause is empty too; otherwise force it to match the
        // grouping set's order (C:2308).
        let group_clause: Vec<pathnodes::NodeId> = if !gs0_set.is_empty() {
            preprocess_groupclause(run, root, &gs0_set)?
        } else {
            Vec::new()
        };

        // Hashable? Pretend empty sets are hashable (forced not-hashed later),
        // but not if there's nothing but empty sets (C:2322).
        let hashable = !gs0_set.is_empty()
            && !bms::bms_overlap_list(
                unhashable_refs.as_deref(),
                &gs0_set.iter().map(|&i| i as i32).collect::<Vec<_>>(),
            );
        if hashable {
            any_hashable = true;
        }

        // Remap the grouping sets from sortgrouprefs to plain groupClause
        // indices (C:2333).
        let gsets = remap_to_groupclause_idx(
            root,
            &group_clause,
            &current_sets,
            &mut tleref_to_colnum_map,
        );

        rollups.push(RollupData {
            groupClause: group_clause,
            gsets,
            gsets_data: current_sets,
            numGroups: 0.0,
            hashable,
            is_hashed: false,
        });
    }

    // If we have unsortable sets, build index-based hash_sets_idx based on the
    // entire original groupClause for estimation (C:2344).
    let mut hash_sets_idx: Vec<Vec<i32>> = Vec::new();
    if !unsortable_sets.is_empty() {
        hash_sets_idx = remap_to_groupclause_idx(
            root,
            &processed_group_clause,
            &unsortable_sets,
            &mut tleref_to_colnum_map,
        );
        any_hashable = true;
    }

    Ok(GroupingSetsData {
        rollups,
        hash_sets_idx,
        dNumHashGroups: 0.0,
        any_hashable,
        unsortable_refs,
        unhashable_refs,
        unsortable_sets,
        tleref_to_colnum_map,
    })
}

// ===========================================================================
// create_grouping_paths + helpers (planner.c:3779)
// ===========================================================================

/// `get_number_of_groups(root, path_rows, gd, target_list)` (planner.c:3658),
/// restricted to the no-grouping-sets cases reachable here. For a plain GROUP BY
/// it estimates from the optimized groupClause; for plain aggregation (no GROUP
/// BY) it's a single result row.
fn get_number_of_groups<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    path_rows: f64,
    gd: Option<&mut GroupingSetsData<'mcx>>,
    // extra->targetList: the target list used to resolve the group-clause
    // sortgrouprefs into exprs. None means "use root.processed_tlist" (the top
    // grouping rel); Some carries the per-child translated target list produced
    // by adjust_appendrel_attrs in the partitionwise recursion, so the group
    // exprs carry the child's varnos and estimate_num_groups reads the child
    // partition's ndistinct rather than the parent's (planner.c:3661/4133/7452).
    target_list_override: Option<&[pathnodes::NodeId]>,
) -> PgResult<f64> {
    let (has_group_clause, has_grouping_sets, has_aggs, num_grouping_sets) = {
        let parse = run.resolve(root.parse);
        (
            !parse.groupClause.is_empty(),
            !parse.groupingSets.is_empty(),
            parse.hasAggs,
            parse.groupingSets.len(),
        )
    };
    let tlist: Vec<pathnodes::NodeId> = match target_list_override {
        Some(t) => t.to_vec(),
        None => root.processed_tlist.clone(),
    };

    if has_group_clause {
        if has_grouping_sets {
            // GROUPING SETS — add up the estimates for each grouping set (C:3670).
            let gd = gd.expect("get_number_of_groups: grouping sets without GroupingSetsData");
            let mut d_num_groups = 0.0;

            for rollup in gd.rollups.iter_mut() {
                // groupExprs = get_sortgrouplist_exprs(rollup->groupClause, tlist).
                let group_exprs: Vec<pathnodes::NodeId> = rollup
                    .groupClause
                    .iter()
                    .map(|&sgc| {
                        nodeFuncs_seams::get_sortgroupclause_expr::call(
                            root, sgc, &tlist,
                        )
                    })
                    .collect();

                rollup.numGroups = 0.0;
                for (gset, gs) in rollup.gsets.iter().zip(rollup.gsets_data.iter_mut()) {
                    // estimate_num_groups(root, groupExprs, path_rows, &gset, NULL):
                    // the pgset selects, by 0-based index, which groupExprs are in
                    // this grouping set. Build that restricted sublist and estimate
                    // over it (an empty sublist yields exactly one group).
                    let restricted: Vec<pathnodes::NodeId> = gset
                        .iter()
                        .filter_map(|&idx| group_exprs.get(idx as usize).copied())
                        .collect();
                    let num_groups = estimate_num_groups_for_gset(run, root, &restricted, path_rows)?;
                    gs.numGroups = num_groups;
                    rollup.numGroups += num_groups;
                }
                d_num_groups += rollup.numGroups;
            }

            if !gd.hash_sets_idx.is_empty() {
                gd.dNumHashGroups = 0.0;
                // groupExprs = get_sortgrouplist_exprs(parse->groupClause, tlist).
                let full_group_clause = root.processed_groupClause.clone();
                let group_exprs: Vec<pathnodes::NodeId> = full_group_clause
                    .iter()
                    .map(|&sgc| {
                        nodeFuncs_seams::get_sortgroupclause_expr::call(
                            root, sgc, &tlist,
                        )
                    })
                    .collect();

                for (gset, gs) in gd.hash_sets_idx.iter().zip(gd.unsortable_sets.iter_mut()) {
                    let restricted: Vec<pathnodes::NodeId> = gset
                        .iter()
                        .filter_map(|&idx| group_exprs.get(idx as usize).copied())
                        .collect();
                    let num_groups = estimate_num_groups_for_gset(run, root, &restricted, path_rows)?;
                    gs.numGroups = num_groups;
                    gd.dNumHashGroups += num_groups;
                }
                d_num_groups += gd.dNumHashGroups;
            }

            Ok(d_num_groups)
        } else {
            // Plain GROUP BY — estimate based on the optimized groupClause (C:3735).
            let group_clause = root.processed_groupClause.clone();
            let group_exprs: Vec<pathnodes::NodeId> = group_clause
                .iter()
                .map(|&sgc| {
                    nodeFuncs_seams::get_sortgroupclause_expr::call(root, sgc, &tlist)
                })
                .collect();
            selfuncs_seams::estimate_num_groups::call(
                run,
                root,
                &group_exprs,
                path_rows,
                None,
            )
        }
    } else if has_grouping_sets {
        // Empty grouping sets — one result row for each (C:3743).
        Ok(num_grouping_sets as f64)
    } else if has_aggs || root.hasHavingQual {
        // Plain aggregation, one result row.
        Ok(1.0)
    } else {
        // Not grouping.
        Ok(1.0)
    }
}

/// `estimate_num_groups(root, groupExprs, input_rows, &gset, NULL)` for one
/// grouping set, where the caller has already restricted `group_exprs` to the
/// members of the set (the `pgset` filter in selfuncs.c selects which groupExprs
/// participate; building the restricted sublist and estimating over it is
/// equivalent, and an empty sublist yields exactly one group). This avoids
/// extending the by-ref-owned `estimate_num_groups` seam with the `pgset` arg.
fn estimate_num_groups_for_gset<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    group_exprs: &[pathnodes::NodeId],
    input_rows: f64,
) -> PgResult<f64> {
    if group_exprs.is_empty() {
        return Ok(1.0);
    }
    selfuncs_seams::estimate_num_groups::call(
        run, root, group_exprs, input_rows, None,
    )
}

/// `PartitionwiseAggregateType` (pathnodes.h) — drives the partitionwise
/// aggregation branching in `create_ordinary_grouping_paths` /
/// `create_partitionwise_grouping_paths`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum PartitionwiseAggregateType {
    /// PARTITIONWISE_AGGREGATE_NONE — no partitionwise aggregation.
    None,
    /// PARTITIONWISE_AGGREGATE_FULL — full partitionwise aggregation (GROUP BY
    /// includes the partition keys): aggregate each partition then Append.
    Full,
    /// PARTITIONWISE_AGGREGATE_PARTIAL — partial partitionwise aggregation:
    /// partial-aggregate each partition, Append, then finalize.
    Partial,
}

/// `create_grouping_paths(root, input_rel, target, target_parallel_safe, gd)`
/// (planner.c:3779) — build a new upperrel containing Paths for grouping and/or
/// aggregation. Restricted to the non-degenerate, non-grouping-sets,
/// non-parallel cases reachable on this path: the sorted Agg / Group path over
/// the cheapest input, plus partitionwise aggregation when the input rel is
/// partitioned. Parallel/partial-only legs loud-panic (gated out upstream).
fn create_grouping_paths<'mcx>(
    mcx: Mcx<'mcx>,
    run: &mut PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    input_rel: RelId,
    target: PathTarget,
    target_parallel_safe: bool,
    gd: Option<&mut GroupingSetsData<'mcx>>,
) -> PgResult<RelId> {
    // MemSet(&agg_costs, 0); get_agg_clause_costs(root, AGGSPLIT_SIMPLE, &agg_costs).
    let mut agg_costs = pathnodes::AggClauseCosts::default();
    prepagg::get_agg_clause_costs(
        root,
        ::nodes::nodeagg::AGGSPLIT_SIMPLE,
        &mut agg_costs,
    )?;
    let agg_costs_lite = agg_clause_costs_to_lite(&agg_costs);

    // make_grouping_rel(root, input_rel, target, target_parallel_safe,
    // parse->havingQual) (C:3798). IS_OTHER_REL(input_rel) is false here (top
    // grouping rel), so the relids set is NULL.
    let grouped_rel = relnode::fetch_upper_rel(root, UPPERREL_GROUP_AGG, &None);
    root.rel_mut(grouped_rel).reltarget = Some(Box::new(target.clone()));
    {
        // consider_parallel: input_rel->consider_parallel && target_parallel_safe
        // && is_parallel_safe(havingQual). is_parallel_safe(havingQual) folds into
        // the target-expr check; on this path glob is parallel-unsafe so the input
        // rel is never consider_parallel and this stays false.
        let input_cp = root.rel(input_rel).consider_parallel;
        let cp = input_cp && target_parallel_safe;
        root.rel_mut(grouped_rel).consider_parallel = cp;
        // FDW propagation (C:3930-3933).
        let (serverid, userid, useridiscurrent, has_fdwroutine) = {
            let ir = root.rel(input_rel);
            (ir.serverid, ir.userid, ir.useridiscurrent, ir.has_fdwroutine)
        };
        let gr = root.rel_mut(grouped_rel);
        gr.serverid = serverid;
        gr.userid = userid;
        gr.useridiscurrent = useridiscurrent;
        gr.has_fdwroutine = has_fdwroutine;
    }

    // is_degenerate_grouping(root): (hasHavingQual || groupingSets) && !hasAggs
    // && groupClause == NIL. A HAVING-only query with no aggregates and no
    // GROUP BY would be degenerate, but that needs create_group_result_path
    // (unported here). Mirror PG and panic only when actually degenerate.
    let (has_aggs, has_group_clause, has_grouping_sets) = {
        let parse = run.resolve(root.parse);
        (
            parse.hasAggs,
            !parse.groupClause.is_empty(),
            !parse.groupingSets.is_empty(),
        )
    };
    let is_degenerate = (root.hasHavingQual || has_grouping_sets) && !has_aggs && !has_group_clause;
    if is_degenerate {
        // create_degenerate_grouping_paths(root, input_rel, grouped_rel) (C:3966).
        create_degenerate_grouping_paths(mcx, run, root, grouped_rel)?;
        // set_cheapest(grouped_rel) (C:3880).
        pathnode::set_cheapest(root, grouped_rel)?;
        return Ok(grouped_rel);
    }

    // Determine whether partitionwise aggregation is in theory possible. It can
    // be disabled by the user, and for now we don't try to support grouping sets.
    // create_ordinary_grouping_paths checks additional conditions, such as
    // whether input_rel is partitioned (C:3864-3873).
    let parent_patype = if costsize::ENABLE_PARTITIONWISE_AGGREGATE()
        && !has_grouping_sets
    {
        PartitionwiseAggregateType::Full
    } else {
        PartitionwiseAggregateType::None
    };

    // create_ordinary_grouping_paths(root, input_rel, grouped_rel, &agg_costs,
    // gd, &extra, &partially_grouped_rel) (C:3875). For the top grouping rel the
    // havingQual is parse->havingQual (no per-child translation), so the override
    // is None.
    create_ordinary_grouping_paths(
        mcx,
        run,
        root,
        input_rel,
        grouped_rel,
        agg_costs_lite,
        has_group_clause,
        has_aggs,
        gd,
        parent_patype,
        None,
        None,
    )?;

    // set_cheapest(grouped_rel) (C:3880).
    pathnode::set_cheapest(root, grouped_rel)?;
    Ok(grouped_rel)
}

/// `create_degenerate_grouping_paths(root, input_rel, grouped_rel)`
/// (planner.c:3953). The degenerate case: a query with `HAVING`/grouping sets
/// but no aggregates and an empty `groupClause`. The only path is a trivial
/// `GroupResultPath` carrying the HAVING qual; with multiple grouping sets we
/// emit one clone per set and `Append` them (a volatile HAVING then yields
/// between 0 and N rows, which is the desired behaviour). `input_rel` is unused
/// here (mirrors C: it is only passed for symmetry), so it is omitted from the
/// Rust signature.
fn create_degenerate_grouping_paths<'mcx>(
    mcx: Mcx<'mcx>,
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    grouped_rel: RelId,
) -> PgResult<()> {
    // nrows = list_length(parse->groupingSets).
    let nrows = run.resolve(root.parse).groupingSets.len();

    // havingqual: (List *) parse->havingQual — clone the qual Expr into the
    // planner arena as a one-element implicit-AND list (empty when no HAVING).
    let make_havingqual = |root: &mut PlannerInfo| -> PgResult<Vec<pathnodes::NodeId>> {
        match run.resolve(root.parse).havingQual.as_deref() {
            Some(e) => {
                let cloned = e.clone_in(mcx)?;
                Ok(alloc::vec![root.alloc_node(cloned)])
            }
            None => Ok(Vec::new()),
        }
    };

    // grouped_rel->reltarget (clone: create_group_result_path takes it by value).
    let target = |root: &PlannerInfo| -> Box<pathnodes::PathTarget> {
        root.rel(grouped_rel)
            .reltarget
            .clone()
            .expect("create_degenerate_grouping_paths: grouped_rel has no reltarget")
    };

    let path: PathId = if nrows > 1 {
        // Make N clones and Append them (one GroupResultPath per grouping set).
        let mut paths: Vec<PathId> = Vec::with_capacity(nrows);
        for _ in 0..nrows {
            let t = target(root);
            let hq = make_havingqual(root)?;
            let p = pathnode::create::create_group_result_path(
                root,
                grouped_rel,
                t,
                hq,
            )?;
            paths.push(p);
        }
        pathnode::create::create_append_path(
            root,
            run,
            /* have_root */ true,
            grouped_rel,
            paths,
            /* partial_subpaths */ Vec::new(),
            /* pathkeys */ Vec::new(),
            /* required_outer */ &None,
            /* parallel_workers */ 0,
            /* parallel_aware */ false,
            /* rows */ -1.0,
        )?
    } else {
        // No grouping sets, or just one, so one output row.
        let t = target(root);
        let hq = make_havingqual(root)?;
        pathnode::create::create_group_result_path(
            root,
            grouped_rel,
            t,
            hq,
        )?
    };

    pathnode::add_path(root, grouped_rel, path)?;
    Ok(())
}

/// `create_ordinary_grouping_paths` (planner.c:4031): estimate the number of
/// groups and add the final grouping/aggregation paths (`add_paths_to_grouping_rel`).
/// Before doing so, generate any possible partially-grouped paths (parallel
/// partial aggregation), so the final code can consider both parallel and
/// non-parallel approaches. The partitionwise branch
/// (`create_partitionwise_grouping_paths`) requires `IS_PARTITIONED_REL(input_rel)`;
/// on the non-partitioned path here `patype == PARTITIONWISE_AGGREGATE_NONE`
/// and it is not reached (the upper `enable_partitionwise_aggregate` gate is
/// computed in `create_grouping_paths`).
fn create_ordinary_grouping_paths<'mcx>(
    mcx: Mcx<'mcx>,
    run: &mut PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    input_rel: RelId,
    grouped_rel: RelId,
    agg_costs: Option<pathnode_seams::AggClauseCostsLite>,
    has_group_clause: bool,
    has_aggs: bool,
    mut gd: Option<&mut GroupingSetsData<'mcx>>,
    // extra->patype: the partitionwise-aggregate type computed for the parent
    // rel (FULL/NONE at the top; for a partitionwise child it is the parent's
    // local patype, threaded down by create_partitionwise_grouping_paths).
    parent_patype: PartitionwiseAggregateType,
    // extra->havingQual: the HAVING qual to use for this rel. None means "read
    // parse->havingQual" (the top rel); Some carries the per-child translated
    // qual produced by adjust_appendrel_attrs in the partitionwise recursion.
    having_qual_override: Option<&Expr<'mcx>>,
    // extra->targetList: the target list used to resolve group-clause
    // sortgrouprefs. None means "use root.processed_tlist" (the top rel); Some
    // carries the per-child translated target list (adjust_appendrel_attrs) so
    // group-count estimates read child partition ndistinct, not the parent's.
    target_list_override: Option<&[pathnodes::NodeId]>,
) -> PgResult<Option<RelId>> {
    // If this is the topmost grouping relation or if the parent relation is
    // doing some form of partitionwise aggregation, then we may be able to do it
    // at this level also.  However, if the input relation is not partitioned,
    // partitionwise aggregate is impossible (C:4043-4073).
    let mut patype = PartitionwiseAggregateType::None;
    if parent_patype != PartitionwiseAggregateType::None
        && is_partitioned_rel(root, input_rel)
    {
        // If this is the topmost relation or the parent is doing full
        // partitionwise aggregation, we can do full partitionwise aggregation
        // provided the GROUP BY clause contains all of the partitioning columns
        // at this level (with matching collation). Otherwise at most partial.
        // C checks parse->groupClause (not processed_groupClause) so a partition
        // column proven redundant still counts. The port stores the resolvable
        // SortGroupClause handles only in processed_groupClause; the two differ
        // only when eval_const_expressions proved a partition key redundant
        // (e.g. GROUP BY const), which the partitionwise tests don't exercise.
        let group_clause = root.processed_groupClause.clone();
        if parent_patype == PartitionwiseAggregateType::Full
            && group_by_has_partkey(root, input_rel, &group_clause, target_list_override)?
        {
            patype = PartitionwiseAggregateType::Full;
        } else if can_partial_agg(run, root) {
            patype = PartitionwiseAggregateType::Partial;
        } else {
            patype = PartitionwiseAggregateType::None;
        }
    }

    // Compute the GROUPING_CAN_USE_{SORT,HASH} flags (C create_grouping_paths
    // 3812-3845). can_sort if any rollup is sortable or the processed_groupClause
    // is sortable; can_hash if there's a GROUP BY, no ordered aggs, and the
    // grouping is hashable (gd->any_hashable with grouping sets).
    let processed_group_clauses: Vec<::nodes::rawnodes::SortGroupClause> = root
        .processed_groupClause
        .iter()
        .map(|&id| *root.sortgroupclause(id))
        .collect();
    let gd_has_rollups = gd.as_ref().is_some_and(|g| !g.rollups.is_empty());
    let gd_any_hashable = gd.as_ref().is_some_and(|g| g.any_hashable);
    let can_sort = gd_has_rollups
        || vars::tlist::grouping_is_sortable(&processed_group_clauses);
    let can_hash = has_group_clause
        && root.numOrderedAggs == 0
        && (if gd.is_some() {
            gd_any_hashable
        } else {
            vars::tlist::grouping_is_hashable(&processed_group_clauses)
        });

    // GROUPING_CAN_PARTIAL_AGG (C create_grouping_paths:3853): can_partial_agg(root).
    let can_partial = can_partial_agg(run, root);

    // cheapest_path = input_rel->cheapest_total_path.
    let cheapest_path = root
        .rel(input_rel)
        .cheapest_total_path
        .ok_or_else(|| PgError::error("create_ordinary_grouping_paths: input rel has no cheapest_total_path"))?;
    let cheapest_rows = root.path(cheapest_path).base().rows;

    // Before generating paths for grouped_rel, generate any possible partially
    // grouped paths; that way, later code can easily consider both parallel and
    // non-parallel approaches to grouping (C:4079). If we're doing partitionwise
    // aggregation at this level, force creation of a partially_grouped_rel so we
    // can add partitionwise paths to it (C:4082-4089).
    //
    // agg_partial_costs (AGGSPLIT_INITIAL_SERIAL) / agg_final_costs
    // (AGGSPLIT_FINAL_DESERIAL) correspond to extra->agg_partial_costs /
    // extra->agg_final_costs, computed lazily by create_partial_grouping_paths
    // (extra->partial_costs_set). We compute them here and thread both down.
    let mut partially_grouped_rel: Option<RelId> = None;
    let mut agg_partial_costs_saved: Option<
        pathnode_seams::AggClauseCostsLite,
    > = None;
    let mut agg_final_costs: Option<pathnode_seams::AggClauseCostsLite> = None;
    if can_partial {
        let force_rel_creation = patype == PartitionwiseAggregateType::Partial;
        let (agg_partial_costs, agg_final) = if run.resolve(root.parse).hasAggs {
            let mut p = pathnodes::AggClauseCosts::default();
            prepagg::get_agg_clause_costs(
                root,
                ::nodes::nodeagg::AGGSPLIT_INITIAL_SERIAL,
                &mut p,
            )?;
            let mut f = pathnodes::AggClauseCosts::default();
            prepagg::get_agg_clause_costs(
                root,
                ::nodes::nodeagg::AGGSPLIT_FINAL_DESERIAL,
                &mut f,
            )?;
            (agg_clause_costs_to_lite(&p), agg_clause_costs_to_lite(&f))
        } else {
            (
                agg_clause_costs_to_lite(&pathnodes::AggClauseCosts::default()),
                agg_clause_costs_to_lite(&pathnodes::AggClauseCosts::default()),
            )
        };
        agg_final_costs = agg_final;
        agg_partial_costs_saved = agg_partial_costs;
        partially_grouped_rel = create_partial_grouping_paths(
            mcx,
            run,
            root,
            grouped_rel,
            input_rel,
            can_sort,
            can_hash,
            agg_partial_costs,
            gd.as_deref_mut(),
            force_rel_creation,
            parent_patype,
            having_qual_override,
            target_list_override,
        )?;
    }

    // Apply partitionwise aggregation technique, if possible (C:4104).
    if patype != PartitionwiseAggregateType::None {
        create_partitionwise_grouping_paths(
            mcx,
            run,
            root,
            input_rel,
            grouped_rel,
            partially_grouped_rel,
            agg_costs,
            agg_partial_costs_saved,
            agg_final_costs,
            gd.as_deref_mut(),
            patype,
            having_qual_override,
            target_list_override,
        )?;
    }

    // If we are doing partial aggregation only, return (C:4109-4118). The parent
    // will finalize the partially-grouped Append we just built.
    if parent_patype == PartitionwiseAggregateType::Partial {
        let pgr = partially_grouped_rel
            .expect("create_ordinary_grouping_paths: PARTIAL parent without partially_grouped_rel");
        if !root.rel(pgr).pathlist.is_empty() {
            pathnode::set_cheapest(root, pgr)?;
        }
        return Ok(partially_grouped_rel);
    }

    // Gather any partially grouped partial paths (C:4121).
    if let Some(pgr) = partially_grouped_rel {
        if !root.rel(pgr).partial_pathlist.is_empty() {
            gather_grouping_paths(run, root, pgr)?;
            pathnode::set_cheapest(root, pgr)?;
        }
    }

    // Estimate number of groups (C:4130). For grouping sets this also fills
    // rollup->numGroups, gs->numGroups, and gd->dNumHashGroups in place.
    let d_num_groups =
        get_number_of_groups(run, root, cheapest_rows, gd.as_deref_mut(), target_list_override)?;

    // Build final grouping paths (C:4136).
    add_paths_to_grouping_rel(
        mcx,
        run,
        root,
        input_rel,
        grouped_rel,
        partially_grouped_rel,
        agg_costs,
        agg_final_costs,
        d_num_groups,
        has_group_clause,
        has_aggs,
        can_sort,
        can_hash,
        gd.as_deref_mut(),
        having_qual_override,
    )?;

    // Give a helpful error if we failed to find any implementation (C:4141).
    if root.rel(grouped_rel).pathlist.is_empty() {
        return Err(PgError::error("could not implement GROUP BY")
            .with_sqlstate(types_error::error::ERRCODE_FEATURE_NOT_SUPPORTED)
            .with_detail(
                "Some of the datatypes only support hashing, while others \
                 only support sorting.",
            ));
    }
    Ok(partially_grouped_rel)
}

/// `IS_PARTITIONED_REL(rel)` (pathnodes.h:1086): the rel has a partitioning
/// scheme, partition bounds, live partition children, and is not a dummy rel.
fn is_partitioned_rel(root: &PlannerInfo, rel: RelId) -> bool {
    let r = root.rel(rel);
    r.part_scheme.is_some()
        && r.boundinfo.is_some()
        && r.nparts > 0
        && !r.part_rels.is_empty()
        && !joinrels::is_dummy_rel(root, rel)
}

/// `make_grouping_rel(root, input_rel, target, target_parallel_safe, havingQual)`
/// (planner.c:3893) — construct the upper grouping relation for `input_rel`. For
/// the top grouping rel the relids set is NULL by tradition; for a partition
/// child (IS_OTHER_REL) it is the child's relids and the rel is marked
/// RELOPT_OTHER_UPPER_REL. The havingQual parallel-safety check folds into the
/// consider_parallel computation; on this non-parallel path the child rels are
/// never consider_parallel so it stays false.
fn make_grouping_rel<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    input_rel: RelId,
    target: Box<pathnodes::PathTarget>,
    target_parallel_safe: bool,
) -> PgResult<RelId> {
    // IS_OTHER_REL(input_rel): an OTHER_MEMBER / OTHER_JOINREL / OTHER_UPPER rel.
    let is_other_rel = matches!(
        root.rel(input_rel).reloptkind,
        pathnodes::RELOPT_OTHER_MEMBER_REL
            | pathnodes::RELOPT_OTHER_JOINREL
            | pathnodes::RELOPT_OTHER_UPPER_REL
    );

    let grouped_rel = if is_other_rel {
        let relids = root.rel(input_rel).relids.clone();
        let g = relnode::fetch_upper_rel(root, UPPERREL_GROUP_AGG, &relids);
        root.rel_mut(g).reloptkind = pathnodes::RELOPT_OTHER_UPPER_REL;
        g
    } else {
        relnode::fetch_upper_rel(root, UPPERREL_GROUP_AGG, &None)
    };

    // Set target.
    root.rel_mut(grouped_rel).reltarget = Some(target);

    // If the input relation is not parallel-safe, the grouped relation can't be
    // either; otherwise it's parallel-safe if the target list and HAVING quals
    // are parallel-safe. On this path is_parallel_safe(havingQual) folds into the
    // target check and child input rels are not consider_parallel.
    let input_cp = root.rel(input_rel).consider_parallel;
    root.rel_mut(grouped_rel).consider_parallel = input_cp && target_parallel_safe;

    // If the input rel belongs to a single FDW, so does the grouped rel.
    let (serverid, userid, useridiscurrent, has_fdwroutine) = {
        let ir = root.rel(input_rel);
        (ir.serverid, ir.userid, ir.useridiscurrent, ir.has_fdwroutine)
    };
    {
        let gr = root.rel_mut(grouped_rel);
        gr.serverid = serverid;
        gr.userid = userid;
        gr.useridiscurrent = useridiscurrent;
        gr.has_fdwroutine = has_fdwroutine;
    }
    let _ = run;
    Ok(grouped_rel)
}

/// `group_by_has_partkey(input_rel, targetList, groupClause)` (planner.c:8207).
/// Returns true if all the partition keys of `input_rel` are part of the GROUP
/// BY clauses, including matching collation. `groupClause` is the raw
/// `parse->groupClause` (SortGroupClause handles); the target list used to
/// resolve sortgrouprefs is `root.processed_tlist` (equivalent to
/// `parse->targetList` for the grouping columns, since sortgrouprefs are
/// preserved).
fn group_by_has_partkey(
    root: &mut PlannerInfo,
    input_rel: RelId,
    group_clause: &[pathnodes::NodeId],
    // extra->targetList: None => root.processed_tlist (top rel); Some => the
    // per-child translated target list (planner.c:4066).
    target_list_override: Option<&[pathnodes::NodeId]>,
) -> PgResult<bool> {
    // groupexprs = get_sortgrouplist_exprs(groupClause, targetList).
    let tlist: Vec<pathnodes::NodeId> = match target_list_override {
        Some(t) => t.to_vec(),
        None => root.processed_tlist.clone(),
    };
    let group_expr_ids: Vec<pathnodes::NodeId> = group_clause
        .iter()
        .map(|&sgc| {
            nodeFuncs_seams::get_sortgroupclause_expr::call(root, sgc, &tlist)
        })
        .collect();

    // Rule out early if there are no partition keys present.
    let partexprs = root.rel(input_rel).partexprs.clone();
    if partexprs.is_empty() {
        return Ok(false);
    }

    let part_scheme = root
        .rel(input_rel)
        .part_scheme
        .clone()
        .expect("group_by_has_partkey: input rel has no part_scheme");
    let partnatts = part_scheme.partnatts as usize;

    for cnt in 0..partnatts {
        let this_partexprs = match partexprs.get(cnt) {
            Some(v) => v,
            None => return Ok(false),
        };
        let partcoll = *part_scheme
            .partcollation
            .get(cnt)
            .expect("group_by_has_partkey: partcollation shorter than partnatts");
        let mut found = false;

        for &partexpr_id in this_partexprs.iter() {
            let partexpr = root.node(partexpr_id).clone();

            for &groupexpr_id in group_expr_ids.iter() {
                // Note: we can assume there is at most one RelabelType node;
                // eval_const_expressions() will have simplified if more than one.
                let mut groupexpr = root.node(groupexpr_id).clone();
                if let ::nodes::primnodes::Expr::RelabelType(rt) = &groupexpr {
                    if let Some(arg) = rt.arg.as_deref() {
                        groupexpr = arg.clone();
                    }
                }
                let groupcoll =
                    nodeFuncs_seams::exprCollation::call(&groupexpr);

                if equalfuncs_seams::equal_expr::call(&groupexpr, &partexpr) {
                    // Reject a match if the grouping collation does not match the
                    // partitioning collation.
                    if types_core::primitive::OidIsValid(partcoll)
                        && types_core::primitive::OidIsValid(groupcoll)
                        && partcoll != groupcoll
                    {
                        return Ok(false);
                    }
                    found = true;
                    break;
                }
            }
            if found {
                break;
            }
        }

        // If none of the partition key expressions match any GROUP BY
        // expression, return false.
        if !found {
            return Ok(false);
        }
    }

    Ok(true)
}

/// `create_partitionwise_grouping_paths(root, input_rel, grouped_rel,
/// partially_grouped_rel, agg_costs, gd, patype, extra)` (planner.c:8064). Break
/// aggregation/grouping over a partitioned relation down into per-partition
/// aggregation, appending the results. For FULL partitionwise aggregation each
/// partition is fully aggregated and the results Appended; for PARTIAL, each
/// partition is partially aggregated, Appended, then finalized by the caller.
#[allow(clippy::too_many_arguments)]
fn create_partitionwise_grouping_paths<'mcx>(
    mcx: Mcx<'mcx>,
    run: &mut PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    input_rel: RelId,
    grouped_rel: RelId,
    partially_grouped_rel: Option<RelId>,
    agg_costs: Option<pathnode_seams::AggClauseCostsLite>,
    _agg_partial_costs: Option<pathnode_seams::AggClauseCostsLite>,
    _agg_final_costs: Option<pathnode_seams::AggClauseCostsLite>,
    mut gd: Option<&mut GroupingSetsData<'mcx>>,
    patype: PartitionwiseAggregateType,
    having_qual_override: Option<&Expr<'mcx>>,
    // extra->targetList for this (parent) rel: None => root.processed_tlist (top
    // rel); Some => already-translated target list of a nested partitionwise
    // child. Each leaf child's target list is this list further translated by
    // adjust_appendrel_attrs (planner.c:8122).
    target_list_override: Option<&[pathnodes::NodeId]>,
) -> PgResult<()> {
    debug_assert!(patype != PartitionwiseAggregateType::None);
    debug_assert!(
        patype != PartitionwiseAggregateType::Partial || partially_grouped_rel.is_some()
    );
    let mut grouped_live_children: Vec<RelId> = Vec::new();
    let mut partially_grouped_live_children: Vec<RelId> = Vec::new();
    let mut partial_grouping_valid = true;

    // target = grouped_rel->reltarget.
    let target = root
        .rel(grouped_rel)
        .reltarget
        .clone()
        .ok_or_else(|| PgError::error("create_partitionwise_grouping_paths: grouped rel has no reltarget"))?;

    // extra->target_parallel_safe — used by make_grouping_rel for the child.
    let target_parallel_safe = root.rel(grouped_rel).consider_parallel;

    // extra->havingQual for the parent rel (parse->havingQual at the top, or the
    // translated qual for a nested partitionwise child). Deep-copy via clone_in:
    // the HAVING qual carries Aggrefs whose derived Expr::clone guards a shallow
    // copy.
    let parent_having: Option<Expr<'mcx>> = match having_qual_override {
        Some(e) => Some(e.clone_in(mcx)?),
        None => match run.resolve(root.parse).havingQual.as_deref() {
            Some(e) => Some(e.clone_in(mcx)?),
            None => None,
        },
    };

    // extra->targetList for the parent rel: the override if provided (nested
    // partitionwise child) else root.processed_tlist (top rel). Each leaf child's
    // targetList is this list translated by adjust_appendrel_attrs (C:8122), so
    // get_number_of_groups resolves the child's group exprs to child Vars and
    // reads the child partition's ndistinct.
    let parent_target_list: Vec<pathnodes::NodeId> = match target_list_override {
        Some(t) => t.to_vec(),
        None => root.processed_tlist.clone(),
    };

    // Add paths for partitionwise aggregation/grouping: walk input_rel->live_parts.
    let live_indexes: Vec<usize> = root
        .rel(input_rel)
        .live_parts
        .as_ref()
        .map(|b| {
            let mut out = Vec::new();
            for (wi, &word) in b.words.iter().enumerate() {
                let mut w = word;
                while w != 0 {
                    let bit = w.trailing_zeros() as usize;
                    out.push(wi * 64 + bit);
                    w &= w - 1;
                }
            }
            out
        })
        .unwrap_or_default();

    for i in live_indexes {
        let child_input_rel = root.rel(input_rel).part_rels[i]
            .expect("create_partitionwise_grouping_paths: live part_rels slot is NULL");

        // Dummy children can be ignored.
        if joinrels::is_dummy_rel(root, child_input_rel) {
            continue;
        }

        // child_target = copy_pathtarget(target); translate its exprs.
        let child_relids = root.rel(child_input_rel).relids.clone();
        let appinfos =
            appendinfo::find_appinfos_by_relids(root, &child_relids)?;

        let mut child_target = vars::tlist::copy_pathtarget(&target);
        let mut new_exprs: Vec<pathnodes::NodeId> = Vec::with_capacity(child_target.exprs.len());
        for &expr_id in child_target.exprs.iter() {
            // Deep-copy via clone_in: the grouped-rel reltarget exprs include
            // Aggrefs whose args are context-allocated TargetEntry lists; the
            // derived Expr::clone guards against a shallow copy.
            let expr = root.node(expr_id).clone_in(mcx)?;
            let adjusted = appendinfo::adjust_appendrel_attrs_run(
                Some(run),
                root,
                expr,
                &appinfos,
            )?;
            new_exprs.push(root.alloc_node(adjusted));
        }
        child_target.exprs = new_exprs;

        // Translate havingQual for this child (child_extra.havingQual). Deep-copy
        // via clone_in: the qual carries Aggrefs (the derived Expr::clone guards
        // a shallow copy); adjust_appendrel_attrs_run then mutates it in place.
        let child_having: Option<Expr> = match &parent_having {
            Some(hq) => Some(appendinfo::adjust_appendrel_attrs_run(
                Some(run),
                root,
                hq.clone_in(mcx)?,
                &appinfos,
            )?),
            None => None,
        };

        // Translate the target list for this child (child_extra.targetList,
        // C:8122). get_number_of_groups uses it to resolve the group exprs to the
        // child's Vars, so the partial-group estimate reads the child partition's
        // ndistinct rather than the parent's.
        let child_target_list = appendinfo::adjust_targetlist_by_appinfos(
            Some(run),
            mcx,
            root,
            &parent_target_list,
            &appinfos,
        )?;

        // child_extra.patype = patype (this rel's value becomes the child's
        // parent value).
        let child_parent_patype = patype;

        // make_grouping_rel for the child (holds fully aggregated paths).
        let child_grouped_rel = make_grouping_rel(
            run,
            root,
            child_input_rel,
            Box::new(child_target),
            target_parallel_safe,
        )?;

        // Create grouping paths for this child relation.
        let child_partially_grouped_rel = create_ordinary_grouping_paths(
            mcx,
            run,
            root,
            child_input_rel,
            child_grouped_rel,
            agg_costs,
            // has_group_clause / has_aggs are global (parse-level) properties and
            // identical for every child.
            !run.resolve(root.parse).groupClause.is_empty(),
            run.resolve(root.parse).hasAggs,
            gd.as_deref_mut(),
            child_parent_patype,
            child_having.as_ref(),
            Some(&child_target_list),
        )?;

        if let Some(cpg) = child_partially_grouped_rel {
            partially_grouped_live_children.push(cpg);
        } else {
            partial_grouping_valid = false;
        }

        if patype == PartitionwiseAggregateType::Full {
            pathnode::set_cheapest(root, child_grouped_rel)?;
            grouped_live_children.push(child_grouped_rel);
        }
    }

    // Try to create append paths for partially grouped children. We must have a
    // partially grouped path for every child to generate one for this relation.
    if let Some(pgr) = partially_grouped_rel {
        if partial_grouping_valid {
            debug_assert!(!partially_grouped_live_children.is_empty());
            allpaths::add_paths_to_append_rel(
                mcx,
                root,
                run,
                pgr,
                &partially_grouped_live_children,
            )?;
            // set_cheapest, since the finalization step uses the cheapest path.
            if !root.rel(pgr).pathlist.is_empty() {
                pathnode::set_cheapest(root, pgr)?;
            }
        }
    }

    // If possible, create append paths for fully grouped children.
    if patype == PartitionwiseAggregateType::Full {
        debug_assert!(!grouped_live_children.is_empty());
        allpaths::add_paths_to_append_rel(
            mcx,
            root,
            run,
            grouped_rel,
            &grouped_live_children,
        )?;
    }

    Ok(())
}

/// `can_partial_agg(root)` (planner.c:7785) — determine whether partial grouping
/// and/or aggregation is possible.
fn can_partial_agg<'mcx>(run: &PlannerRun<'mcx>, root: &PlannerInfo) -> bool {
    let parse = run.resolve(root.parse);
    if !parse.hasAggs && parse.groupClause.is_empty() {
        // We don't know how to do parallel aggregation unless we have either
        // some aggregates or a grouping clause.
        false
    } else if !parse.groupingSets.is_empty() {
        // We don't know how to do grouping sets in parallel.
        false
    } else if root.hasNonPartialAggs || root.hasNonSerialAggs {
        // Insufficient support for partial mode.
        false
    } else {
        true
    }
}

/// `add_paths_to_grouping_rel` (planner.c:7113): add the sorted-input Agg / Group
/// paths over each input path, plus Finalize Agg / Group paths over the
/// partially-grouped rel's paths (parallel partial aggregation), and the hashed
/// legs. For each input path we consider its useful group-key orderings; for the
/// empty group clause that's the single original (empty) ordering, so
/// make_ordered_path returns the path unchanged.
#[allow(clippy::too_many_arguments)]
fn add_paths_to_grouping_rel<'mcx>(
    mcx: Mcx<'mcx>,
    run: &mut PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    input_rel: RelId,
    grouped_rel: RelId,
    partially_grouped_rel: Option<RelId>,
    agg_costs: Option<pathnode_seams::AggClauseCostsLite>,
    agg_final_costs: Option<pathnode_seams::AggClauseCostsLite>,
    d_num_groups: f64,
    has_group_clause: bool,
    has_aggs: bool,
    can_sort: bool,
    can_hash: bool,
    mut gd: Option<&mut GroupingSetsData<'mcx>>,
    // extra->havingQual override (per-child translated qual). None => read
    // parse->havingQual (top rel).
    having_qual_override: Option<&Expr<'mcx>>,
) -> PgResult<()> {
    let has_grouping_sets = gd.is_some();

    if !can_sort && !can_hash {
        return Err(PgError::error(
            "add_paths_to_grouping_rel: non-sortable, non-hashable grouping",
        ));
    }

    // havingQual (extra->havingQual) — the HAVING qual clause list as bare node
    // handles. For the top rel parse->havingQual is the owned Option<PgBox<Expr>>;
    // for a partitionwise child it is the translated qual passed via
    // having_qual_override. Clone it into the arena to obtain the qual list the
    // Agg/Group path carries.
    let having_quals: Vec<pathnodes::NodeId> = {
        let src: Option<&Expr<'mcx>> = match having_qual_override {
            Some(e) => Some(e),
            None => run.resolve(root.parse).havingQual.as_deref(),
        };
        match src {
            Some(e) => {
                // Deep-copy via clone_in: the qual carries Aggrefs.
                let cloned = e.clone_in(mcx)?;
                alloc::vec![root.alloc_node(cloned)]
            }
            None => Vec::new(),
        }
    };

    let target = root
        .rel(grouped_rel)
        .reltarget
        .clone()
        .ok_or_else(|| PgError::error("add_paths_to_grouping_rel: grouped rel has no reltarget"))?;

    if can_sort {
        // foreach(input_rel->pathlist) — consider each input path, then loop
        // over the interesting GROUP BY key orderings produced by
        // get_useful_group_keys_orderings (C:7144). Reordering the group keys
        // to match the input path's sort order lets a presorted Index/Incremental
        // Sort serve the GroupAggregate instead of a full Sort.
        let cheapest_path = root.rel(input_rel).cheapest_total_path;
        let input_paths: Vec<PathId> = root.rel(input_rel).pathlist.clone();
        for path in input_paths {
            let orderings = pathkeys::get_useful_group_keys_orderings(
                root,
                path,
                guc_tables::vars::enable_group_by_reordering.read(),
                enable_incremental_sort(),
                has_grouping_sets,
            );
            for info in orderings {
                // restore the path (make_ordered_path replaces it per ordering).
                let ordered = make_ordered_path(
                    root,
                    run,
                    grouped_rel,
                    path,
                    cheapest_path.unwrap_or(path),
                    info.pathkeys.clone(),
                    -1.0,
                )?;
                let ordered = match ordered {
                    Some(p) => p,
                    None => continue,
                };

                if has_grouping_sets {
                    // consider_groupingsets_paths(root, grouped_rel, path, true,
                    // can_hash, gd, agg_costs, dNumGroups) (C:7164).
                    consider_groupingsets_paths(
                        run,
                        root,
                        grouped_rel,
                        ordered,
                        true, // is_sorted
                        can_hash,
                        gd.as_deref_mut().unwrap(),
                        agg_costs,
                        d_num_groups,
                        &having_quals,
                    )?;
                } else if has_aggs {
                    // We have aggregation, possibly with plain GROUP BY (C:7177).
                    let aggstrategy = if has_group_clause {
                        pathnodes::AGG_SORTED
                    } else {
                        pathnodes::AGG_PLAIN
                    };
                    let agg_path = pathnode::create::create_agg_path(
                        run,
                        root,
                        grouped_rel,
                        ordered,
                        target.clone(),
                        aggstrategy,
                        pathnodes::AGGSPLIT_SIMPLE,
                        info.clauses.clone(),
                        having_quals.clone(),
                        agg_costs,
                        d_num_groups,
                    )?;
                    pathnode::add_path(root, grouped_rel, agg_path)?;
                } else if has_group_clause {
                    // GROUP BY without aggregation or grouping sets — make a
                    // GroupPath (C:7195). info->clauses is the (reordered)
                    // processed group clause; havingQual is the HAVING qual list.
                    let group_path = pathnode::create::create_group_path(
                        run,
                        root,
                        grouped_rel,
                        ordered,
                        info.clauses.clone(),
                        having_quals.clone(),
                        d_num_groups,
                    )?;
                    pathnode::add_path(root, grouped_rel, group_path)?;
                } else {
                    unreachable!("add_paths_to_grouping_rel: no agg and no group clause");
                }
            }
        }

        // Instead of operating directly on the input relation, we can consider
        // finalizing a partially aggregated path (C:7211).
        if let Some(pgr) = partially_grouped_rel {
            let pgr_cheapest = root.rel(pgr).cheapest_total_path;
            let pgr_paths: Vec<PathId> = root.rel(pgr).pathlist.clone();
            for path in pgr_paths {
                let cheapest = pgr_cheapest.unwrap_or(path);
                let orderings = pathkeys::get_useful_group_keys_orderings(
                    root,
                    path,
                    guc_tables::vars::enable_group_by_reordering.read(),
                    enable_incremental_sort(),
                    has_grouping_sets,
                );
                for info in orderings {
                    let ordered = make_ordered_path(
                        root,
                        run,
                        grouped_rel,
                        path,
                        cheapest,
                        info.pathkeys.clone(),
                        -1.0,
                    )?;
                    let ordered = match ordered {
                        Some(p) => p,
                        None => continue,
                    };

                    if has_aggs {
                        // Finalize Aggregate over the partially-aggregated path
                        // (AGGSPLIT_FINAL_DESERIAL, agg_final_costs) (C:7247).
                        let aggstrategy = if has_group_clause {
                            pathnodes::AGG_SORTED
                        } else {
                            pathnodes::AGG_PLAIN
                        };
                        let agg_path = pathnode::create::create_agg_path(
                            run,
                            root,
                            grouped_rel,
                            ordered,
                            target.clone(),
                            aggstrategy,
                            pathnodes::AGGSPLIT_FINAL_DESERIAL,
                            info.clauses.clone(),
                            having_quals.clone(),
                            agg_final_costs,
                            d_num_groups,
                        )?;
                        pathnode::add_path(root, grouped_rel, agg_path)?;
                    } else {
                        // GROUP BY without aggregation — finalize via GroupPath (C:7263).
                        let group_path = pathnode::create::create_group_path(
                            run,
                            root,
                            grouped_rel,
                            ordered,
                            info.clauses.clone(),
                            having_quals.clone(),
                            d_num_groups,
                        )?;
                        pathnode::add_path(root, grouped_rel, group_path)?;
                    }
                }
            }
        }
    }

    if can_hash {
        if has_grouping_sets {
            // Try for a hash-only groupingsets path over unsorted input (C:7280).
            let cheapest_path = root
                .rel(input_rel)
                .cheapest_total_path
                .ok_or_else(|| PgError::error("add_paths_to_grouping_rel: no cheapest_total_path"))?;
            consider_groupingsets_paths(
                run,
                root,
                grouped_rel,
                cheapest_path,
                false, // is_sorted
                true,  // can_hash
                gd.as_deref_mut().unwrap(),
                agg_costs,
                d_num_groups,
                &having_quals,
            )?;
        } else {
            // Generate a HashAgg Path over the cheapest-total input (C:7289).
            let cheapest_path = root
                .rel(input_rel)
                .cheapest_total_path
                .ok_or_else(|| PgError::error("add_paths_to_grouping_rel: no cheapest_total_path"))?;
            let agg_path = pathnode::create::create_agg_path(
                run,
                root,
                grouped_rel,
                cheapest_path,
                target.clone(),
                pathnodes::AGG_HASHED,
                pathnodes::AGGSPLIT_SIMPLE,
                root.processed_groupClause.clone(),
                having_quals.clone(),
                agg_costs,
                d_num_groups,
            )?;
            pathnode::add_path(root, grouped_rel, agg_path)?;
        }

        // Generate a Finalize HashAgg Path atop of the cheapest partially
        // grouped path, assuming there is one (C:7306).
        if let Some(pgr) = partially_grouped_rel {
            if !root.rel(pgr).pathlist.is_empty() {
                if let Some(path) = root.rel(pgr).cheapest_total_path {
                    let agg_path = pathnode::create::create_agg_path(
                        run,
                        root,
                        grouped_rel,
                        path,
                        target.clone(),
                        pathnodes::AGG_HASHED,
                        pathnodes::AGGSPLIT_FINAL_DESERIAL,
                        root.processed_groupClause.clone(),
                        having_quals.clone(),
                        agg_final_costs,
                        d_num_groups,
                    )?;
                    pathnode::add_path(root, grouped_rel, agg_path)?;
                }
            }
        }
    }

    // When partitionwise aggregate is used, we might have fully aggregated paths
    // in the partial pathlist (Parallel Append of non-partial child paths); gather
    // them (C:7331).
    if !root.rel(grouped_rel).partial_pathlist.is_empty() {
        gather_grouping_paths(run, root, grouped_rel)?;
    }

    Ok(())
}

/// `create_partial_grouping_paths` (planner.c:7351) — build the
/// `partially_grouped_rel` (UPPERREL_PARTIAL_GROUP_AGG) and add partially-grouped
/// regular + partial paths (AGGSPLIT_INITIAL_SERIAL). Returns `None` when no
/// partial-aggregation input is available and `force_rel_creation` is false. On
/// the non-partitionwise path, `extra->patype == NONE`, so `cheapest_total_path`
/// (the partitionwise-partial-only non-partial input) is never set.
#[allow(clippy::too_many_arguments)]
fn create_partial_grouping_paths<'mcx>(
    mcx: Mcx<'mcx>,
    run: &mut PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    grouped_rel: RelId,
    input_rel: RelId,
    can_sort: bool,
    can_hash: bool,
    agg_partial_costs: Option<pathnode_seams::AggClauseCostsLite>,
    mut gd: Option<&mut GroupingSetsData<'mcx>>,
    force_rel_creation: bool,
    // extra->patype: the partitionwise-aggregate type at the PARENT level (NONE
    // off the partitionwise path; PARTIAL when the parent does partial
    // partitionwise aggregation, which enables the non-partial partial-agg leg).
    parent_patype: PartitionwiseAggregateType,
    // extra->havingQual: per-child translated qual (None => parse->havingQual).
    having_qual_override: Option<&Expr<'mcx>>,
    // extra->targetList: per-child translated target list (None =>
    // root.processed_tlist), used by get_number_of_groups for the partial-group
    // count estimate so a child reads its own ndistinct (planner.c:7452/7458).
    target_list_override: Option<&[pathnodes::NodeId]>,
) -> PgResult<Option<RelId>> {
    let has_aggs = run.resolve(root.parse).hasAggs;

    // Consider generating partially aggregated non-partial paths. We can only do
    // this if we have a non-partial path, and only if the parent of the input rel
    // is performing partial partitionwise aggregation. (extra->patype is the type
    // at the PARENT level, not this level.) (C:7370-7379).
    let cheapest_total_path: Option<PathId> =
        if !root.rel(input_rel).pathlist.is_empty()
            && parent_patype == PartitionwiseAggregateType::Partial
        {
            root.rel(input_rel).cheapest_total_path
        } else {
            None
        };

    // If parallelism is possible for grouped_rel and the input has partial paths,
    // consider partially-grouped partial paths (C:7381-7388).
    let cheapest_partial_path: Option<PathId> =
        if root.rel(grouped_rel).consider_parallel
            && !root.rel(input_rel).partial_pathlist.is_empty()
        {
            Some(root.rel(input_rel).partial_pathlist[0])
        } else {
            None
        };

    // If we can't partially aggregate either, don't create the rel unless forced
    // (C:7390-7396).
    if cheapest_total_path.is_none() && cheapest_partial_path.is_none() && !force_rel_creation {
        return Ok(None);
    }

    // Build the partially-grouped upper relation (C:7402).
    let grouped_relids = root.rel(grouped_rel).relids.clone();
    let partially_grouped_rel = relnode::fetch_upper_rel(
        root,
        UPPERREL_PARTIAL_GROUP_AGG,
        &grouped_relids,
    );
    {
        let (cp, rok, serverid, userid, useridiscurrent, has_fdwroutine) = {
            let g = root.rel(grouped_rel);
            (
                g.consider_parallel,
                g.reloptkind,
                g.serverid,
                g.userid,
                g.useridiscurrent,
                g.has_fdwroutine,
            )
        };
        let pg = root.rel_mut(partially_grouped_rel);
        pg.consider_parallel = cp;
        pg.reloptkind = rok;
        pg.serverid = serverid;
        pg.userid = userid;
        pg.useridiscurrent = useridiscurrent;
        pg.has_fdwroutine = has_fdwroutine;
    }

    // reltarget = make_partial_grouping_target(root, grouped_rel->reltarget,
    // extra->havingQual) (C:7421).
    let grouped_target = root
        .rel(grouped_rel)
        .reltarget
        .clone()
        .ok_or_else(|| PgError::error("create_partial_grouping_paths: grouped rel has no reltarget"))?;
    let partial_target =
        make_partial_grouping_target(mcx, run, root, &grouped_target, having_qual_override)?;
    let partial_target = Box::new(partial_target);
    root.rel_mut(partially_grouped_rel).reltarget = Some(partial_target.clone());

    // Estimate number of partial groups (C:7448).
    let d_num_partial_groups = match cheapest_total_path {
        Some(p) => {
            let rows = root.path(p).base().rows;
            get_number_of_groups(run, root, rows, gd.as_deref_mut(), target_list_override)?
        }
        None => 0.0,
    };
    let d_num_partial_partial_groups = match cheapest_partial_path {
        Some(p) => {
            let rows = root.path(p).base().rows;
            get_number_of_groups(run, root, rows, gd.as_deref_mut(), target_list_override)?
        }
        None => 0.0,
    };

    // can_sort over cheapest_total_path (C:7460). cheapest_total_path is None on
    // this path, so this leg is not reached; ported for completeness via the
    // partial-path leg below sharing the same structure.
    if can_sort && cheapest_total_path.is_some() {
        let ctp = cheapest_total_path.unwrap();
        let input_paths: Vec<PathId> = root.rel(input_rel).pathlist.clone();
        for path in input_paths {
            let group_pathkeys = root.group_pathkeys.clone();
            let ordered = make_ordered_path(
                root,
                run,
                partially_grouped_rel,
                path,
                ctp,
                group_pathkeys,
                -1.0,
            )?;
            let ordered = match ordered {
                Some(p) => p,
                None => continue,
            };
            if has_aggs {
                let aggstrategy = if !root.processed_groupClause.is_empty() {
                    pathnodes::AGG_SORTED
                } else {
                    pathnodes::AGG_PLAIN
                };
                let agg_path = pathnode::create::create_agg_path(
                    run,
                    root,
                    partially_grouped_rel,
                    ordered,
                    partial_target.clone(),
                    aggstrategy,
                    pathnodes::AGGSPLIT_INITIAL_SERIAL,
                    root.processed_groupClause.clone(),
                    Vec::new(),
                    agg_partial_costs,
                    d_num_partial_groups,
                )?;
                pathnode::add_path(root, partially_grouped_rel, agg_path)?;
            } else {
                let group_path = pathnode::create::create_group_path(
                    run,
                    root,
                    partially_grouped_rel,
                    ordered,
                    root.processed_groupClause.clone(),
                    Vec::new(),
                    d_num_partial_groups,
                )?;
                pathnode::add_path(root, partially_grouped_rel, group_path)?;
            }
        }
    }

    // Similar logic, but for partial paths -> add_partial_path (C:7521).
    if can_sort && cheapest_partial_path.is_some() {
        let cpp = cheapest_partial_path.unwrap();
        let input_partial_paths: Vec<PathId> = root.rel(input_rel).partial_pathlist.clone();
        for path in input_partial_paths {
            let group_pathkeys = root.group_pathkeys.clone();
            let ordered = make_ordered_path(
                root,
                run,
                partially_grouped_rel,
                path,
                cpp,
                group_pathkeys,
                -1.0,
            )?;
            let ordered = match ordered {
                Some(p) => p,
                None => continue,
            };
            if has_aggs {
                let aggstrategy = if !root.processed_groupClause.is_empty() {
                    pathnodes::AGG_SORTED
                } else {
                    pathnodes::AGG_PLAIN
                };
                let agg_path = pathnode::create::create_agg_path(
                    run,
                    root,
                    partially_grouped_rel,
                    ordered,
                    partial_target.clone(),
                    aggstrategy,
                    pathnodes::AGGSPLIT_INITIAL_SERIAL,
                    root.processed_groupClause.clone(),
                    Vec::new(),
                    agg_partial_costs,
                    d_num_partial_partial_groups,
                )?;
                pathnode::add_partial_path(
                    root,
                    partially_grouped_rel,
                    agg_path,
                )?;
            } else {
                let group_path = pathnode::create::create_group_path(
                    run,
                    root,
                    partially_grouped_rel,
                    ordered,
                    root.processed_groupClause.clone(),
                    Vec::new(),
                    d_num_partial_partial_groups,
                )?;
                pathnode::add_partial_path(
                    root,
                    partially_grouped_rel,
                    group_path,
                )?;
            }
        }
    }

    // Add a partially-grouped HashAgg Path where possible (C:7578).
    if can_hash && cheapest_total_path.is_some() {
        let ctp = cheapest_total_path.unwrap();
        let agg_path = pathnode::create::create_agg_path(
            run,
            root,
            partially_grouped_rel,
            ctp,
            partial_target.clone(),
            pathnodes::AGG_HASHED,
            pathnodes::AGGSPLIT_INITIAL_SERIAL,
            root.processed_groupClause.clone(),
            Vec::new(),
            agg_partial_costs,
            d_num_partial_groups,
        )?;
        pathnode::add_path(root, partially_grouped_rel, agg_path)?;
    }

    // Now add a partially-grouped HashAgg partial Path where possible (C:7600).
    if can_hash && cheapest_partial_path.is_some() {
        let cpp = cheapest_partial_path.unwrap();
        let agg_path = pathnode::create::create_agg_path(
            run,
            root,
            partially_grouped_rel,
            cpp,
            partial_target.clone(),
            pathnodes::AGG_HASHED,
            pathnodes::AGGSPLIT_INITIAL_SERIAL,
            root.processed_groupClause.clone(),
            Vec::new(),
            agg_partial_costs,
            d_num_partial_partial_groups,
        )?;
        pathnode::add_partial_path(root, partially_grouped_rel, agg_path)?;
    }

    // FDW partially-grouped ForeignPaths (C:7615): the FDW GetForeignUpperPaths
    // hook is not modeled in this build (no FDW is loaded).
    debug_assert!(!root.rel(partially_grouped_rel).has_fdwroutine);

    Ok(Some(partially_grouped_rel))
}

/// `gather_grouping_paths(root, rel)` (planner.c:7693) — generate Gather and
/// Gather Merge paths for a grouped or partially-grouped relation.
/// `generate_useful_gather_paths` does most of the work; we additionally
/// consider sorting by the group pathkeys and applying Gather Merge.
fn gather_grouping_paths<'mcx>(
    run: &mut PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    rel: RelId,
) -> PgResult<()> {
    // Trim off any pathkeys added for ORDER BY / DISTINCT aggregates (C:7704).
    let groupby_pathkeys: Vec<pathnodes::PathKey> = {
        let gp = &root.group_pathkeys;
        let n = root.num_groupby_pathkeys as usize;
        if gp.len() > n {
            gp[..n].to_vec()
        } else {
            gp.clone()
        }
    };

    // Gather for unordered paths and Gather Merge for ordered ones (C:7713).
    allpaths::generate_useful_gather_paths(root, run, rel, true)?;

    if root.rel(rel).partial_pathlist.is_empty() {
        return Ok(());
    }
    let cheapest_partial_path = root.rel(rel).partial_pathlist[0];

    let partial_paths: Vec<PathId> = root.rel(rel).partial_pathlist.clone();
    for path in partial_paths {
        let path_pathkeys = root.path(path).base().pathkeys.clone();
        let (is_sorted, presorted_keys) =
            pathkeys::pathkeys_count_contained_in(
                &groupby_pathkeys,
                &path_pathkeys,
            );
        if is_sorted {
            continue;
        }

        // Sort the cheapest path; incrementally sort partially-sorted paths
        // (C:7740).
        if path != cheapest_partial_path
            && (presorted_keys == 0 || !enable_incremental_sort())
        {
            continue;
        }

        let sorted_path = if presorted_keys == 0 || !enable_incremental_sort() {
            pathnode::create::create_sort_path(
                root,
                rel,
                path,
                groupby_pathkeys.clone(),
                -1.0,
            )?
        } else {
            pathnode::create::create_incremental_sort_path(
                root,
                run,
                rel,
                path,
                groupby_pathkeys.clone(),
                presorted_keys,
                -1.0,
            )?
        };

        let total_groups = costsize::compute_gather_rows(
            root.path(sorted_path).base(),
        );
        let gm_path = pathnode::create::create_gather_merge_path(
            root,
            run,
            rel,
            sorted_path,
            None,
            groupby_pathkeys.clone(),
            &None,
            Some(total_groups),
        )?;
        pathnode::add_path(root, rel, gm_path)?;
    }

    Ok(())
}

/// `make_partial_grouping_target(root, grouping_target, havingQual)`
/// (planner.c:5640) — generate the PathTarget for the output of a partial
/// aggregate (or partial grouping) node. Emits the same aggregates a regular
/// aggregate would (plus HAVING aggregates) with the Aggrefs marked partial,
/// and the Vars/PlaceHolderVars used outside Aggrefs.
fn make_partial_grouping_target<'mcx>(
    mcx: Mcx<'mcx>,
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    grouping_target: &PathTarget,
    // extra->havingQual: the HAVING qual to source HAVING Vars/Aggrefs from. None
    // => parse->havingQual (top rel); Some => the per-child translated qual, so
    // the pulled columns carry the child's varnos (matching the child scan).
    having_qual_override: Option<&Expr<'mcx>>,
) -> PgResult<PathTarget> {
    let mut partial_target = vars::tlist::create_empty_pathtarget();
    let mut non_group_cols: Vec<pathnodes::NodeId> = Vec::new();

    let processed_group_clauses: Vec<::nodes::rawnodes::SortGroupClause> = root
        .processed_groupClause
        .iter()
        .map(|&id| *root.sortgroupclause(id))
        .collect();

    for (i, &expr) in grouping_target.exprs.iter().enumerate() {
        let sgref = get_pathtarget_sortgroupref(grouping_target, i);
        let is_group_col = sgref != 0
            && !processed_group_clauses.is_empty()
            && vars::tlist::get_sortgroupref_clause_noerr(
                sgref,
                &processed_group_clauses,
            )
            .is_some();
        if is_group_col {
            // Grouping column: add as-is so the upper agg can repeat the calcs.
            vars::tlist::add_column_to_pathtarget(
                &mut partial_target,
                expr,
                sgref,
            );
        } else {
            // Non-grouping column: remember for pull_var_clause.
            non_group_cols.push(expr);
        }
    }

    // If there's a HAVING clause, we'll need the Vars/Aggrefs it uses too (C:5684).
    let having_src: Option<&Expr> = match having_qual_override {
        Some(e) => Some(e),
        None => run.resolve(root.parse).havingQual.as_deref(),
    };
    if let Some(hq) = having_src {
        let cloned = hq.clone_in(mcx)?;
        non_group_cols.push(root.alloc_node(cloned));
    }

    // Pull out all the Vars, PlaceHolderVars, and Aggrefs mentioned in
    // non_group_cols, and add them if not already present (C:5694).
    let flags = vars::var::PVC_INCLUDE_AGGREGATES
        | vars::var::PVC_RECURSE_WINDOWFUNCS
        | vars::var::PVC_INCLUDE_PLACEHOLDERS;
    let mut non_group_exprs: Vec<pathnodes::NodeId> = Vec::new();
    for &col in &non_group_cols {
        // node_expr_wrapper deep-copies the input Expr into a scratch context
        // (a shallow Expr::clone panics on an Aggref).
        let scratch = mcx::MemoryContext::new("make_partial_grouping_target pull_var_clause");
        let node = nodes_core::node_walker::node_expr_wrapper(root.node(col), scratch.mcx());
        let vars = vars::var::pull_var_clause(mcx, &node, flags)?;
        for v in vars {
            non_group_exprs.push(root.alloc_node(v));
        }
    }
    vars::tlist::add_new_columns_to_pathtarget(
        root,
        &mut partial_target,
        &non_group_exprs,
    );

    // Adjust Aggrefs to put them in partial mode (C:5705). At this point all
    // Aggrefs are at the top level of the target list. C flat-copies each Aggref
    // before marking it to avoid damaging shared trees; we mirror that by
    // cloning the Aggref node into a fresh arena slot.
    let target_exprs: Vec<pathnodes::NodeId> = partial_target.exprs.clone();
    for (idx, &expr_id) in target_exprs.iter().enumerate() {
        let is_aggref = matches!(root.node(expr_id), ::nodes::primnodes::Expr::Aggref(_));
        if is_aggref {
            let cloned = root.node(expr_id).clone_in(mcx)?;
            let new_id = root.alloc_node(cloned);
            if let ::nodes::primnodes::Expr::Aggref(aggref) = root.node_mut(new_id) {
                mark_partial_aggref_impl(aggref, ::nodes::nodeagg::AGGSPLIT_INITIAL_SERIAL)?;
            }
            partial_target.exprs[idx] = new_id;
        }
    }

    // set_pathtarget_cost_width(root, partial_target) (C:5731).
    costsize::sizeest::set_pathtarget_cost_width(root, &mut partial_target);

    Ok(partial_target)
}

/// `estimate_hashagg_tablesize(root, path, agg_costs, dNumGroups)`
/// (selfuncs.c:4179). Estimate the bytes a hash-aggregate hashtable requires,
/// based on the agg_costs, path width, and number of groups. Ported locally (the
/// selfuncs.c owner is the adt by-ref campaign's; this is a small leaf helper
/// over `hash_agg_entry_size`, which is wired through the costsize seam).
fn estimate_hashagg_tablesize(
    root: &PlannerInfo,
    path: PathId,
    agg_costs: Option<pathnode_seams::AggClauseCostsLite>,
    d_num_groups: f64,
) -> f64 {
    let width = root
        .path(path)
        .base()
        .pathtarget
        .as_ref()
        .map(|t| t.width)
        .unwrap_or(0) as f64;
    let transition_space = agg_costs.map(|a| a.transition_space as u64).unwrap_or(0);
    let hashentrysize = costsize_seams::hash_agg_entry_size::call(
        root.aggtransinfos.len() as i32,
        width,
        transition_space,
    );
    hashentrysize * d_num_groups
}

/// `consider_groupingsets_paths(root, grouped_rel, path, is_sorted, can_hash, gd,
/// agg_costs, dNumGroups)` (planner.c:4171). Generate GroupingSetsPaths for the
/// given input `path`. When `is_sorted` is false, only hash-only plans are
/// considered; otherwise both a sorted plan and a mixed sort/hash plan are tried.
#[allow(clippy::too_many_arguments)]
fn consider_groupingsets_paths<'mcx>(
    run: &mut PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    grouped_rel: RelId,
    path: PathId,
    is_sorted: bool,
    can_hash: bool,
    gd: &mut GroupingSetsData<'mcx>,
    agg_costs: Option<pathnode_seams::AggClauseCostsLite>,
    d_num_groups: f64,
    having_qual: &[pathnodes::NodeId],
) -> PgResult<()> {
    let hash_mem_limit = pathnode_seams::get_hash_memory_limit::call();

    if !is_sorted {
        // Only consider plans that can be done entirely by hashing (C:4193).
        debug_assert!(can_hash);

        let mut new_rollups: Vec<RollupData> = Vec::new();
        let mut unhashed_rollup: Option<RollupData> = None;
        let mut empty_sets_data: Vec<GroupingSetData> = Vec::new();
        let mut empty_sets_count: usize = 0; // empty_sets (list of NILs) length
        let mut strat = pathnodes::AGG_HASHED;
        let mut exclude_groups = 0.0f64;

        // l_start = list_head(gd->rollups); start index into gd->rollups.
        let mut l_start: usize = 0;

        // If the input is coincidentally sorted usefully, save hashtable space by
        // making use of it (C:4228).
        if !gd.rollups.is_empty() {
            let group_pathkeys = root.group_pathkeys.clone();
            let path_pathkeys = root.path(path).base().pathkeys.clone();
            if pathkeys::pathkeys_contained_in(
                &group_pathkeys,
                &path_pathkeys,
            ) {
                let r = gd.rollups[0].clone();
                exclude_groups = r.numGroups;
                unhashed_rollup = Some(r);
                l_start = 1;
            }
        }

        let hashsize = estimate_hashagg_tablesize(root, path, agg_costs, d_num_groups - exclude_groups);

        // gd->rollups is empty only if we have unsortable columns; override the
        // hash_mem limit in that case (C:4240).
        if hashsize > hash_mem_limit && !gd.rollups.is_empty() {
            return Ok(()); // nope, won't fit
        }

        // Burst the existing rollups into individual grouping sets and recompute
        // a groupClause for each set (C:4248).
        let mut sets_data: Vec<GroupingSetData> = gd.unsortable_sets.clone();

        for rollup in gd.rollups.iter().skip(l_start) {
            // An unhashable rollup not skipped by the "actually sorted" check
            // means we'd need differently-sorted input; bail (C:4264).
            if !rollup.hashable {
                return Ok(());
            }
            sets_data.extend(rollup.gsets_data.iter().cloned());
        }

        for gs in &sets_data {
            if gs.set.is_empty() {
                // Empty grouping sets can't be hashed (C:4280).
                empty_sets_data.push(gs.clone());
                empty_sets_count += 1;
            } else {
                let gset: Vec<types_core::Index> = gs.set.clone();
                let group_clause = preprocess_groupclause(run, root, &gset)?;
                let gsets_data = alloc::vec![gs.clone()];
                let gsets = remap_to_groupclause_idx(
                    root,
                    &group_clause,
                    &gsets_data,
                    &mut gd.tleref_to_colnum_map,
                );
                new_rollups.push(RollupData {
                    groupClause: group_clause,
                    gsets,
                    gsets_data,
                    numGroups: gs.numGroups,
                    hashable: true,
                    is_hashed: true,
                });
            }
        }

        // If we didn't find anything nonempty to hash, bail (C:4302).
        if new_rollups.is_empty() {
            return Ok(());
        }

        // If there were empty grouping sets they should have been in the first
        // rollup (C:4310).
        debug_assert!(unhashed_rollup.is_none() || empty_sets_count == 0);

        if let Some(unhashed) = unhashed_rollup {
            new_rollups.push(unhashed);
            strat = pathnodes::AGG_MIXED;
        } else if empty_sets_count > 0 {
            new_rollups.push(RollupData {
                groupClause: Vec::new(),
                gsets: alloc::vec![Vec::new(); empty_sets_count],
                gsets_data: empty_sets_data,
                numGroups: empty_sets_count as f64,
                hashable: false,
                is_hashed: false,
            });
            strat = pathnodes::AGG_MIXED;
        }

        let gsp = pathnode::create::create_groupingsets_path(
            run,
            root,
            grouped_rel,
            path,
            having_qual.to_vec(),
            strat,
            new_rollups,
            agg_costs,
        )?;
        pathnode::add_path(root, grouped_rel, gsp)?;
        return Ok(());
    }

    // Sorted input but nothing we can do with it (C:4339).
    if gd.rollups.is_empty() {
        return Ok(());
    }

    // Given sorted input, try two paths: one sorted and one mixed sort/hash
    // (C:4350).
    if can_hash && gd.any_hashable {
        let mut rollups: Vec<RollupData> = Vec::new();
        let mut hash_sets: Vec<GroupingSetData> = gd.unsortable_sets.clone();
        let mut availspace = hash_mem_limit;

        // Account first for space needed for groups we can't sort at all (C:4368).
        availspace -= estimate_hashagg_tablesize(root, path, agg_costs, gd.dNumHashGroups);

        if availspace > 0.0 && gd.rollups.len() > 1 {
            // Knapsack: capacity = hash_mem, item weights = hashtable memory per
            // rollup, equal item values (C:4378).
            let num_rollups = gd.rollups.len();
            let scale = (availspace / (20.0 * num_rollups as f64)).max(1.0);
            let k_capacity = (availspace / scale).floor() as i32;

            // Leave the first rollup out (matches the input sort order). Assign
            // indexes "i" to only those entries considered for hashing (C:4408).
            let mut k_weights: Vec<i32> = Vec::new();
            for rollup in gd.rollups.iter().skip(1) {
                if rollup.hashable {
                    let sz = estimate_hashagg_tablesize(root, path, agg_costs, rollup.numGroups);
                    let w = (sz / scale).floor().min(k_capacity as f64 + 1.0) as i32;
                    k_weights.push(w);
                }
            }

            // Apply knapsack (C:4438).
            let hash_items = if !k_weights.is_empty() {
                knapsack::DiscreteKnapsack(
                    run.mcx(),
                    k_capacity,
                    k_weights.len() as i32,
                    &k_weights,
                    None,
                )?
            } else {
                None
            };

            if !nodes_core::bitmapset::bms_is_empty(hash_items.as_deref()) {
                // rollups = list_make1(linitial(gd->rollups)) (C:4444).
                rollups.push(gd.rollups[0].clone());

                let mut i: i32 = 0;
                for rollup in gd.rollups.iter().skip(1) {
                    if rollup.hashable {
                        if nodes_core::bitmapset::bms_is_member(i, hash_items.as_deref()) {
                            hash_sets.extend(rollup.gsets_data.iter().cloned());
                        } else {
                            rollups.push(rollup.clone());
                        }
                        i += 1;
                    } else {
                        rollups.push(rollup.clone());
                    }
                }
            }
        }

        // if (!rollups && hash_sets) rollups = list_copy(gd->rollups) (C:4470).
        if rollups.is_empty() && !hash_sets.is_empty() {
            rollups = gd.rollups.clone();
        }

        for gs in &hash_sets {
            debug_assert!(!gs.set.is_empty());
            let gset: Vec<types_core::Index> = gs.set.clone();
            let group_clause = preprocess_groupclause(run, root, &gset)?;
            let gsets_data = alloc::vec![gs.clone()];
            let gsets = remap_to_groupclause_idx(
                root,
                &group_clause,
                &gsets_data,
                &mut gd.tleref_to_colnum_map,
            );
            // lcons(rollup, rollups) — prepend.
            rollups.insert(
                0,
                RollupData {
                    groupClause: group_clause,
                    gsets,
                    gsets_data,
                    numGroups: gs.numGroups,
                    hashable: true,
                    is_hashed: true,
                },
            );
        }

        if !rollups.is_empty() {
            let gsp = pathnode::create::create_groupingsets_path(
                run,
                root,
                grouped_rel,
                path,
                having_qual.to_vec(),
                pathnodes::AGG_MIXED,
                rollups,
                agg_costs,
            )?;
            pathnode::add_path(root, grouped_rel, gsp)?;
        }
    }

    // Now try the simple sorted case (C:4503).
    if gd.unsortable_sets.is_empty() {
        let gsp = pathnode::create::create_groupingsets_path(
            run,
            root,
            grouped_rel,
            path,
            having_qual.to_vec(),
            pathnodes::AGG_SORTED,
            gd.rollups.clone(),
            agg_costs,
        )?;
        pathnode::add_path(root, grouped_rel, gsp)?;
    }

    Ok(())
}

/// Convert the full `AggClauseCosts` (types-pathnodes) into the trimmed
/// `AggClauseCostsLite` that `create_agg_path` / `cost_agg` consume.
fn agg_clause_costs_to_lite(
    c: &pathnodes::AggClauseCosts,
) -> Option<pathnode_seams::AggClauseCostsLite> {
    Some(pathnode_seams::AggClauseCostsLite {
        trans_startup: c.transCost.startup,
        trans_per_tuple: c.transCost.per_tuple,
        final_startup: c.finalCost.startup,
        final_per_tuple: c.finalCost.per_tuple,
        transition_space: c.transitionSpace as i32,
    })
}

/// `make_ordered_path(root, rel, path, cheapest_path, pathkeys, limit_tuples)`
/// (planner.c:7644) — return a path ordered by `pathkeys` based on `path`, or
/// `None` if it doesn't make sense to generate an ordered path here. An empty
/// `pathkeys` is contained in any path, so the path is returned unchanged.
fn make_ordered_path<'mcx>(
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    rel: RelId,
    path: PathId,
    cheapest_path: PathId,
    pathkeys: Vec<pathnodes::PathKey>,
    limit_tuples: f64,
) -> PgResult<Option<PathId>> {
    let path_pathkeys = root.path(path).base().pathkeys.clone();
    let (is_sorted, presorted_keys) =
        pathkeys::pathkeys_count_contained_in(&pathkeys, &path_pathkeys);

    if is_sorted {
        return Ok(Some(path));
    }

    // Try at least sorting the cheapest path and also incrementally sorting any
    // path which is partially sorted already (C:7656-7684).
    if path != cheapest_path && (presorted_keys == 0 || !enable_incremental_sort()) {
        return Ok(None);
    }
    let sorted = if presorted_keys == 0 || !enable_incremental_sort() {
        pathnode::create::create_sort_path(
            root,
            rel,
            path,
            pathkeys,
            limit_tuples,
        )?
    } else {
        pathnode::create::create_incremental_sort_path(
            root,
            run,
            rel,
            path,
            pathkeys,
            presorted_keys,
            limit_tuples,
        )?
    };
    Ok(Some(sorted))
}

/// `is_parallel_safe(root, (Node *) exprs)` over a PathTarget's expr handle list
/// (planner.c uses this on `final_target->exprs`). Mirrors clauses.c
/// `is_parallel_safe`, which walks a `List` node by recursing into each element;
/// AND-ing the per-expr result is equivalent. `safe_param_ids` is gathered from
/// this root's (and parents') init_plans `SubPlan.setParam`, as in C; on the
/// simple SELECT path init_plans is empty so it is the empty set.
fn is_target_exprs_parallel_safe(root: &PlannerInfo, exprs: &[pathnodes::NodeId]) -> bool {
    let max_hazard: u8 = root
        .glob
        .as_ref()
        .map(|g| g.max_parallel_hazard as u8)
        .unwrap_or(PROPARALLEL_UNSAFE as u8);
    let param_exec_empty = root
        .glob
        .as_ref()
        .map(|g| g.param_exec_types.is_empty())
        .unwrap_or(true);

    // safe_param_ids = concat of init_plans' SubPlan.setParam, this level + parents.
    // (parent_root chain is not modeled on the value root; this level only.)
    let mut safe_param_ids: Vec<i32> = Vec::new();
    for &ipl in &root.init_plans {
        if let Some(sp) = root.node(ipl).as_subplan() {
            for &p in sp.0.setParam.iter() {
                safe_param_ids.push(p);
            }
        }
    }

    for &id in exprs {
        let node = root.node(id);
        let safe = clauses::is_parallel_safe(
            max_hazard,
            param_exec_empty,
            safe_param_ids.clone(),
            Some(node),
        )
        .unwrap_or(false);
        if !safe {
            return false;
        }
    }
    true
}

/// `is_parallel_safe(root, (Node *) <Expr>)` over a single optional `Expr`
/// (used by `grouping_planner` on `parse->limitOffset` / `parse->limitCount`).
/// `None` (no clause) is parallel-safe, matching C's `is_parallel_safe(root,
/// NULL)` returning `true`.
fn is_opt_expr_parallel_safe(root: &PlannerInfo, expr: Option<&Expr>) -> bool {
    let max_hazard: u8 = root
        .glob
        .as_ref()
        .map(|g| g.max_parallel_hazard as u8)
        .unwrap_or(PROPARALLEL_UNSAFE as u8);
    let param_exec_empty = root
        .glob
        .as_ref()
        .map(|g| g.param_exec_types.is_empty())
        .unwrap_or(true);

    let mut safe_param_ids: Vec<i32> = Vec::new();
    for &ipl in &root.init_plans {
        if let Some(sp) = root.node(ipl).as_subplan() {
            for &p in sp.0.setParam.iter() {
                safe_param_ids.push(p);
            }
        }
    }

    clauses::is_parallel_safe(
        max_hazard,
        param_exec_empty,
        safe_param_ids,
        expr,
    )
    .unwrap_or(false)
}

/// `equal((Node *) a, (Node *) b)` over two PathTarget expr handle lists. Used
/// for the `scanjoin_target_same_exprs` test (planner.c:1771). Equal iff same
/// length and each pair is structurally `equal()` (resolved through the arena).
fn equal_expr_handle_lists(
    root: &PlannerInfo,
    a: &[pathnodes::NodeId],
    b: &[pathnodes::NodeId],
) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b.iter()).all(|(&x, &y)| {
        equalfuncs_seams::equal_expr::call(root.node(x), root.node(y))
    })
}

/// `apply_scanjoin_target_to_paths(root, rel, scanjoin_targets, ...)`
/// (planner.c:7669). Ported for the non-partitioned case reached by a simple
/// SELECT: apply the SRF-free (first) scan/join target to every existing path
/// of `rel`, stack SRF-evaluation nodes via `adjust_paths_for_srfs` when the
/// target contained SRFs, then set `rel->reltarget` to the full (last) target.
///
/// Partitioned rels (`IS_PARTITIONED_REL`) and the recursive per-partition /
/// `add_paths_to_append_rel` machinery are not reached here and panic precisely.
fn apply_scanjoin_target_to_paths<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    rel: RelId,
    scanjoin_targets: &[PathTarget],
    scanjoin_targets_contain_srfs: &[bool],
    scanjoin_target_parallel_safe: bool,
    tlist_same_exprs: bool,
) -> PgResult<()> {
    // scanjoin_target = linitial_node(PathTarget, scanjoin_targets) (C:7892):
    // the SRF-free target the scan/join paths must emit.
    let scanjoin_target = &scanjoin_targets[0];
    // IS_PARTITIONED_REL(rel) (pathnodes.h:1086): part_scheme && boundinfo &&
    // nparts > 0 && part_rels && !IS_DUMMY_REL(rel). The macro does NOT gate on
    // reloptkind — a partition that is itself partitioned is an
    // RELOPT_OTHER_MEMBER_REL, and multi-level partitionwise aggregation relies
    // on recursing into it; gating on BASEREL/JOINREL would skip the recursion
    // and leave the sub-partition scan pathtargets unlabeled (no sortgrouprefs).
    let rel_is_partitioned = {
        let r = root.rel(rel);
        r.part_scheme.is_some()
            && r.boundinfo.is_some()
            && r.nparts > 0
            && !r.part_rels.is_empty()
    } && !joinrels::is_dummy_rel(root, rel);

    // If the rel is partitioned, drop its existing paths and generate new ones,
    // computing the scan/join target below the partitioning Append rather than
    // above it (cheaper or equal cost, and stable across platforms). Some care
    // is needed: zap the main pathlist now so generate_useful_gather_paths can
    // still see the old PARTIAL paths in the next stanza, then zap the partial
    // pathlist afterwards.
    if rel_is_partitioned {
        root.rel_mut(rel).pathlist = Vec::new();
    }

    // If the scan/join target is not parallel-safe, partial paths cannot
    // generate it; build Gather path(s) over the partials, then drop them
    // (C:7700-7716). On the simple single-table path there are no partial paths,
    // so generate_useful_gather_paths is a no-op; we still mirror the structure.
    if !scanjoin_target_parallel_safe {
        allpaths::generate_useful_gather_paths(root, run, rel, false)?;
        root.rel_mut(rel).partial_pathlist = Vec::new();
        root.rel_mut(rel).consider_parallel = false;
    }

    // Finish dropping old paths for a partitioned rel, per comment above.
    if rel_is_partitioned {
        root.rel_mut(rel).partial_pathlist = Vec::new();
    }

    // Apply the SRF-free scan/join target to each existing path (C:7727-7747).
    let pathlist: Vec<PathId> = root.rel(rel).pathlist.clone();
    for (i, &subpath) in pathlist.iter().enumerate() {
        // Shouldn't have any parameterized paths anymore.
        debug_assert!(root.path(subpath).base().param_info.is_none());
        if tlist_same_exprs {
            // Inject the sortgroupref info into the existing pathtarget.
            let sgr = scanjoin_target.sortgrouprefs.clone();
            if let Some(t) = root.path_mut(subpath).base_mut().pathtarget.as_deref_mut() {
                t.sortgrouprefs = sgr;
            }
        } else {
            // Replace with a projection path that generates the scan/join target.
            let newpath = pathnode::create::create_projection_path(
                root,
                rel,
                subpath,
                Box::new(scanjoin_target.clone()),
            )?;
            root.rel_mut(rel).pathlist[i] = newpath;
        }
    }

    // Likewise adjust the targets for any partial paths (C:7750-7770).
    let partial: Vec<PathId> = root.rel(rel).partial_pathlist.clone();
    for (i, &subpath) in partial.iter().enumerate() {
        debug_assert!(root.path(subpath).base().param_info.is_none());
        if tlist_same_exprs {
            let sgr = scanjoin_target.sortgrouprefs.clone();
            if let Some(t) = root.path_mut(subpath).base_mut().pathtarget.as_deref_mut() {
                t.sortgrouprefs = sgr;
            }
        } else {
            let newpath = pathnode::create::create_projection_path(
                root,
                rel,
                subpath,
                Box::new(scanjoin_target.clone()),
            )?;
            root.rel_mut(rel).partial_pathlist[i] = newpath;
        }
    }

    // Now, if the final scan/join target contains SRFs, insert ProjectSetPath(s)
    // atop each existing path (C:7944-7953).
    if run.resolve(root.parse).hasTargetSRFs {
        adjust_paths_for_srfs(
            root,
            rel,
            scanjoin_targets,
            scanjoin_targets_contain_srfs,
        )?;
    }

    // Update the rel's target to be the final (with SRFs) scan/join target —
    // llast(scanjoin_targets) (C:7966). This matches the actual output of all
    // paths and is required so create_plan / create_append_path see the right
    // pathtarget.
    let last_target = scanjoin_targets[scanjoin_targets.len() - 1].clone();
    root.rel_mut(rel).reltarget = Some(Box::new(last_target));

    // If the relation is partitioned, recursively apply the scan/join target to
    // all partitions and generate brand-new Append paths in which the scan/join
    // target is computed below the Append rather than above it. Since Append is
    // not projection-capable, this can save a separate Result node, and is
    // important for partitionwise aggregate (C:7972-8038).
    if rel_is_partitioned {
        let mut live_children: Vec<RelId> = Vec::new();

        // Adjust each partition. Enumerate the live-part indexes by walking the
        // live_parts Bitmapset words (bms_next_member over rel->live_parts).
        let live_indexes: Vec<usize> = root
            .rel(rel)
            .live_parts
            .as_ref()
            .map(|b| {
                let mut out = Vec::new();
                for (wi, &word) in b.words.iter().enumerate() {
                    let mut w = word;
                    while w != 0 {
                        let bit = w.trailing_zeros() as usize;
                        out.push(wi * 64 + bit);
                        w &= w - 1;
                    }
                }
                out
            })
            .unwrap_or_default();
        for i in live_indexes {
            let child_rel = root.rel(rel).part_rels[i]
                .expect("apply_scanjoin_target_to_paths: live part_rels slot is NULL");

            // Dummy children can be ignored.
            if joinrels::is_dummy_rel(root, child_rel) {
                continue;
            }

            // Translate scan/join targets for this child.
            let child_relids = root.rel(child_rel).relids.clone();
            let appinfos =
                appendinfo::find_appinfos_by_relids(root, &child_relids)?;

            let mut child_scanjoin_targets: Vec<PathTarget> =
                Vec::with_capacity(scanjoin_targets.len());
            for target in scanjoin_targets.iter() {
                // target = copy_pathtarget(target);
                let mut t = vars::tlist::copy_pathtarget(target);
                // target->exprs = adjust_appendrel_attrs(root, target->exprs, appinfos);
                let mut new_exprs: Vec<pathnodes::NodeId> =
                    Vec::with_capacity(t.exprs.len());
                let mcx = run.mcx();
                for &expr_id in t.exprs.iter() {
                    // Deep-copy via `clone_in` into the planner-run arena rather
                    // than the derived `Expr::clone`: a scan/join target expr can
                    // carry an owned-subtree child (`SubPlan`/`SubLink`/`Aggref`,
                    // e.g. a correlated MULTIEXPR `SET (a,b)=(SELECT ...)` over a
                    // partitioned UPDATE) whose derived `clone` panics
                    // (`SubPlanExpr::clone: SubPlan carries context-allocated
                    // children`). `clone_in` routes the `SubPlan` arm through
                    // `SubPlan::clone_in`; the copy is interned back into `root`'s
                    // node arena via `alloc_node`, so it must outlive the whole
                    // run (the long-lived planner `mcx` = `run.mcx()`). Threaded
                    // through `'mcx`, re-erased to the arena at `alloc_node`
                    // (`Expr` is invariant — sanctioned intern boundary).
                    let expr = root.node(expr_id).clone_in(mcx)?;
                    let adjusted = appendinfo::adjust_appendrel_attrs_run(
                        Some(run), root, expr, &appinfos,
                    )?;
                    new_exprs.push(root.alloc_node(adjusted.erase_lifetime()));
                }
                t.exprs = new_exprs;
                child_scanjoin_targets.push(t);
            }

            // Recursion does the real work.
            apply_scanjoin_target_to_paths(
                run,
                root,
                child_rel,
                &child_scanjoin_targets,
                scanjoin_targets_contain_srfs,
                scanjoin_target_parallel_safe,
                tlist_same_exprs,
            )?;

            // Save non-dummy children for Append paths.
            if !joinrels::is_dummy_rel(root, child_rel) {
                live_children.push(child_rel);
            }
        }

        // Build new paths for this relation by appending child paths.
        allpaths::add_paths_to_append_rel(
            run.mcx(),
            root,
            run,
            rel,
            &live_children,
        )?;
    }

    // Consider generating Gather or Gather Merge paths (C:8060-8069). We must
    // only do this if the relation is parallel safe, and we don't do it for
    // child rels (IS_OTHER_REL) to avoid creating multiple Gather nodes within
    // the same plan. This must happen after all paths have been generated and
    // before set_cheapest, since one of the generated paths may turn out to be
    // the cheapest one. This is the postponed gather-path generation for the
    // topmost scan/join rel (set_rel_pathlist skips it for the topmost rel).
    if root.rel(rel).consider_parallel
        && root.rel(rel).reloptkind != pathnodes::RELOPT_OTHER_MEMBER_REL
        && root.rel(rel).reloptkind != pathnodes::RELOPT_OTHER_JOINREL
        && root.rel(rel).reloptkind != pathnodes::RELOPT_OTHER_UPPER_REL
    {
        allpaths::generate_useful_gather_paths(root, run, rel, false)?;
    }

    // We may have added paths (replacing existing ones with projection paths),
    // so recompute the rel's cheapest-path info (C:8043). Without this, the
    // rel's cheapest_total_path still points at a path that is no longer in the
    // pathlist, breaking later steps (create_ordered_paths / create_distinct_paths)
    // that key off cheapest_total_path.
    pathnode::set_cheapest(root, rel)?;

    Ok(())
}

/// `adjust_paths_for_srfs(root, rel, targets, targets_contain_srfs)`
/// (planner.c:6663). Fix up the Paths of `rel` to evaluate tSRFs properly:
/// stack SRF-evaluation (ProjectSet) and regular projection nodes atop each
/// path, following the level chain produced by `split_pathtarget_at_srfs`.
/// The existing Paths are assumed to emit the first target in `targets`.
fn adjust_paths_for_srfs(
    root: &mut PlannerInfo,
    rel: RelId,
    targets: &[PathTarget],
    targets_contain_srfs: &[bool],
) -> PgResult<()> {
    debug_assert_eq!(targets.len(), targets_contain_srfs.len());
    debug_assert!(targets_contain_srfs.first().map(|b| !*b).unwrap_or(true));

    // If no SRFs appear at this plan level, nothing to do.
    if targets.len() == 1 {
        return Ok(());
    }

    let cheapest_startup = root.rel(rel).cheapest_startup_path;
    let cheapest_total = root.rel(rel).cheapest_total_path;

    // Stack SRF-evaluation nodes atop each path for the rel.
    let pathlist: Vec<PathId> = root.rel(rel).pathlist.clone();
    for (li, &subpath) in pathlist.iter().enumerate() {
        debug_assert!(root.path(subpath).base().param_info.is_none());
        let mut newpath = subpath;
        // The first target is what the existing path already emits; the
        // remaining levels are stacked. C iterates forboth(targets,
        // targets_contain_srfs), starting from the level the path emits and
        // re-projecting up the chain (the first level's projection is a no-op
        // onto the same exprs).
        for (thistarget, &contains_srfs) in targets.iter().zip(targets_contain_srfs.iter()) {
            if contains_srfs {
                newpath = pathnode::create::create_set_projection_path(
                    root,
                    rel,
                    newpath,
                    Box::new(thistarget.clone()),
                )?;
            } else {
                newpath = pathnode::create::apply_projection_to_path(
                    root,
                    rel,
                    newpath,
                    Box::new(thistarget.clone()),
                )?;
            }
        }
        root.rel_mut(rel).pathlist[li] = newpath;
        if Some(subpath) == cheapest_startup {
            root.rel_mut(rel).cheapest_startup_path = Some(newpath);
        }
        if Some(subpath) == cheapest_total {
            root.rel_mut(rel).cheapest_total_path = Some(newpath);
        }
    }

    // Likewise for partial paths, if any. These avoid apply_projection_to_path
    // (in case of multiple refs) and use create_projection_path directly.
    let partial: Vec<PathId> = root.rel(rel).partial_pathlist.clone();
    for (li, &subpath) in partial.iter().enumerate() {
        debug_assert!(root.path(subpath).base().param_info.is_none());
        let mut newpath = subpath;
        for (thistarget, &contains_srfs) in targets.iter().zip(targets_contain_srfs.iter()) {
            if contains_srfs {
                newpath = pathnode::create::create_set_projection_path(
                    root,
                    rel,
                    newpath,
                    Box::new(thistarget.clone()),
                )?;
            } else {
                newpath = pathnode::create::create_projection_path(
                    root,
                    rel,
                    newpath,
                    Box::new(thistarget.clone()),
                )?;
            }
        }
        root.rel_mut(rel).partial_pathlist[li] = newpath;
    }

    Ok(())
}

/// `has_volatile_pathkey(keys)` (planner.c:3179) — true if any pathkey's
/// EquivalenceClass has a volatile member.
fn has_volatile_pathkey(root: &PlannerInfo, keys: &[pathnodes::PathKey]) -> bool {
    for pathkey in keys {
        if let Some(ec_id) = pathkey.pk_eclass {
            if root.ec(ec_id).ec_has_volatile {
                return true;
            }
        }
    }
    false
}

/// `adjust_group_pathkeys_for_groupagg(root)` (planner.c:3229) — add pathkeys to
/// `root->group_pathkeys` to reflect the best set of pre-ordered input for
/// ordered/DISTINCT aggregates, marking each covered `Aggref` `aggpresorted`.
///
/// "Best" = the pathkeys that suit the largest number of aggregates. We take the
/// pathkeys of the first ORDER BY / DISTINCT aggregate, then search for others
/// requiring the same or a stricter variation, repeating for any remaining
/// aggregates with different pathkeys. The `Bitmapset` sets of AggInfo indexes
/// are rendered as sorted `Vec<usize>` (the C uses small index sets).
fn adjust_group_pathkeys_for_groupagg(root: &mut PlannerInfo, mcx: Mcx<'_>) {
    use pathnode_seams::PathKeysComparison;

    // grouppathkeys = root->group_pathkeys (clone so the by-value list arithmetic
    // below — append_pathkeys / list_copy — does not alias root->group_pathkeys).
    let grouppathkeys = root.group_pathkeys.clone();

    // Shouldn't be here unless there are some ordered aggregates.
    debug_assert!(root.numOrderedAggs > 0);

    // Do nothing if disabled.
    if !guc_tables::vars::enable_presorted_aggregate.read() {
        return;
    }

    // First pass: collect the AggInfo indexes to be processed below.
    let mut unprocessed_aggs: Vec<usize> = Vec::new();
    for (idx, &agginfo_id) in root.agginfos.clone().iter().enumerate() {
        // aggref = linitial_node(Aggref, agginfo->aggrefs)
        let aggref_id = root.agg_info(agginfo_id).aggrefs[0];
        let aggref = aggref_of_node(root, aggref_id);

        // AGGKIND_IS_ORDERED_SET(aggref->aggkind): kind != AGGKIND_NORMAL.
        if aggref.aggkind != parsenodes::AGGKIND_NORMAL {
            continue;
        }

        // Skip unless there's a DISTINCT or ORDER BY clause.
        if aggref.aggdistinct.is_empty() && aggref.aggorder.is_empty() {
            continue;
        }

        // Additional safety checks are needed if there's a FILTER clause: the
        // filter removes rows that could error when sorted, and presorting
        // happens before the FILTER. Only Vars and Consts (peeling RelabelType)
        // are guaranteed safe to presort.
        if aggref.aggfilter.is_some() {
            let mut allow_presort = true;
            for tle in aggref.args.iter() {
                let mut expr: &::nodes::primnodes::Expr = match tle.expr.as_deref() {
                    Some(e) => e,
                    None => {
                        allow_presort = false;
                        break;
                    }
                };
                while let ::nodes::primnodes::Expr::RelabelType(rt) = expr {
                    match rt.arg.as_deref() {
                        Some(inner) => expr = inner,
                        None => break,
                    }
                }
                match expr {
                    ::nodes::primnodes::Expr::Var(_)
                    | ::nodes::primnodes::Expr::Const(_) => continue,
                    _ => {
                        allow_presort = false;
                        break;
                    }
                }
            }
            if !allow_presort {
                continue;
            }
        }

        unprocessed_aggs.push(idx);
    }

    // Process unprocessed_aggs to find the best set of pathkeys.
    let mut bestpathkeys: Vec<pathnodes::PathKey> = Vec::new();
    let mut bestaggs: Vec<usize> = Vec::new();

    while unprocessed_aggs.len() > bestaggs.len() {
        let mut aggindexes: Vec<usize> = Vec::new();
        let mut currpathkeys: Vec<pathnodes::PathKey> = Vec::new();
        let mut currpathkeys_set = false;
        // Track the volatile aggs we drop from unprocessed during this pass.
        let mut to_drop: Vec<usize> = Vec::new();

        for &i in unprocessed_aggs.iter() {
            let agginfo_id = root.agginfos[i];
            let aggref_id = root.agg_info(agginfo_id).aggrefs[0];

            // sortlist = aggref->aggdistinct ? : aggref->aggorder; args =
            // aggref->args. Intern both into the planner node arena (the form
            // make_pathkeys_for_sortclauses consumes).
            let (sortlist_ids, args_ids) = intern_aggref_sort_inputs(root, aggref_id, mcx);
            let pathkeys = pathkeys::make_pathkeys_for_sortclauses(
                root,
                mcx,
                &sortlist_ids,
                &args_ids,
            );

            // Ignore Aggrefs with volatile functions in their sort clause.
            if has_volatile_pathkey(root, &pathkeys) {
                to_drop.push(i);
                continue;
            }

            if !currpathkeys_set {
                // Take the pathkeys from the first unprocessed aggregate.
                currpathkeys = pathkeys;
                // include the GROUP BY pathkeys, if they exist.
                if !grouppathkeys.is_empty() {
                    currpathkeys = pathkeys::append_pathkeys(
                        root,
                        grouppathkeys.clone(),
                        &currpathkeys,
                    );
                }
                currpathkeys_set = true;
                aggindexes.push(i);
            } else {
                // Look for a stronger set of matching pathkeys.
                let pathkeys = if !grouppathkeys.is_empty() {
                    pathkeys::append_pathkeys(
                        root,
                        grouppathkeys.clone(),
                        &pathkeys,
                    )
                } else {
                    pathkeys
                };
                match pathkeys::compare_pathkeys(&currpathkeys, &pathkeys) {
                    PathKeysComparison::Better2 => {
                        // 'pathkeys' are stronger, use these instead; FALLTHROUGH
                        // to mark this aggregate as covered.
                        currpathkeys = pathkeys;
                        aggindexes.push(i);
                    }
                    PathKeysComparison::Better1 | PathKeysComparison::Equal => {
                        aggindexes.push(i);
                    }
                    PathKeysComparison::Different => {}
                }
            }
        }

        // remove the aggregates that we've just processed (the covered ones and
        // the volatile ones dropped above).
        unprocessed_aggs.retain(|x| !aggindexes.contains(x) && !to_drop.contains(x));

        // If this pass included more aggregates than the previous best, use these.
        if aggindexes.len() > bestaggs.len() {
            bestaggs = aggindexes;
            bestpathkeys = currpathkeys;
        }
    }

    // If we found any ordered aggregates, update root->group_pathkeys to add the
    // best set of aggregate pathkeys (bestpathkeys already includes GROUP BY).
    if !bestpathkeys.is_empty() {
        root.group_pathkeys = bestpathkeys;
    }

    // Set aggpresorted on every Aggref of the covered AggInfos so the executor
    // needn't perform a sort for them. In C, `agginfo->aggrefs` and the live
    // processed-tlist / HAVING-qual Aggrefs are the same pointers; this port
    // interns a separate working clone into `AggInfo.aggrefs` (carrying the
    // assigned `aggno`), while the executor reads the live processed-tlist arena
    // Aggrefs (cloned into the plan tree by createplan). So mark by `aggno`: each
    // covered AggInfo's index is the `aggno` stamped on its Aggrefs by
    // `preprocess_aggref`. Walk processed_tlist (the gate) and set the flag on
    // every Aggref whose aggno is covered.
    if !bestaggs.is_empty() {
        let aggnos: Vec<i32> = bestaggs
            .iter()
            .map(|&i| {
                let agginfo_id = root.agginfos[i];
                aggref_of_node(root, root.agg_info(agginfo_id).aggrefs[0]).aggno
            })
            .collect();
        let tlist_exprs: Vec<pathnodes::NodeId> = root
            .processed_tlist
            .iter()
            .map(|&te| root.targetentry(te).expr)
            .collect();
        for expr_id in tlist_exprs {
            // node_mut yields the live arena Expr; mark in place (mirrors C).
            let node = root.node_mut(expr_id);
            prepagg::mark_aggrefs_presorted(node, &aggnos);
        }
    }
}

/// `linitial_node(Aggref, ...)` — resolve an arena node handle to its `Aggref`.
fn aggref_of_node<'a>(
    root: &'a PlannerInfo,
    id: pathnodes::NodeId,
) -> &'a ::nodes::primnodes::Aggref<'static> {
    match root.node(id) {
        ::nodes::primnodes::Expr::Aggref(a) => a,
        other => panic!(
            "adjust_group_pathkeys_for_groupagg: agginfo->aggrefs node is not an Aggref: {:?}",
            core::mem::discriminant(other)
        ),
    }
}

/// Intern an `Aggref`'s DISTINCT/ORDER BY `SortGroupClause` list (whichever is
/// present — DISTINCT preferred, as in C) and its `args` (`TargetEntry` list)
/// into the planner node arena, returning the `(sortclause_ids, tlist_ids)` that
/// `make_pathkeys_for_sortclauses(root, sortlist, aggref->args)` consumes.
fn intern_aggref_sort_inputs<'mcx>(
    root: &mut PlannerInfo,
    aggref_id: pathnodes::NodeId,
    mcx: Mcx<'mcx>,
) -> (Vec<pathnodes::NodeId>, Vec<pathnodes::NodeId>) {
    // Snapshot the inputs out of the arena into plain (cloneable) values so we
    // can re-intern them without holding a borrow on root. `TargetEntry<'static>`
    // is not `Clone` (it holds PgBox/PgString), so extract the per-TLE fields we
    // need (the expr cloned, plus the scalar labels) up front.
    type TleSnapshot = (
        ::nodes::primnodes::Expr<'static>,
        types_core::primitive::AttrNumber,
        Option<alloc::string::String>,
        types_core::primitive::Index,
        types_core::primitive::Oid,
        types_core::primitive::AttrNumber,
        bool,
    );
    // Deep-clone the per-TLE expr into the planner `mcx` so the snapshot owns its
    // nodes independently of the `root` borrow (releasing it before the re-intern
    // below). The clone lands in the planner's permanent context (NOT a scratch),
    // so the node stays valid after `alloc_node` interns it into root's arena.
    let (sortlist, args): (Vec<::nodes::rawnodes::SortGroupClause>, Vec<TleSnapshot>) = {
        let aggref = aggref_of_node(root, aggref_id);
        let sortlist = if !aggref.aggdistinct.is_empty() {
            aggref.aggdistinct.clone()
        } else {
            aggref.aggorder.clone()
        };
        let mut args: Vec<TleSnapshot> = Vec::with_capacity(aggref.args.len());
        for tle in aggref.args.iter() {
            let expr = match tle.expr.as_deref() {
                // C `copyObject` — an OOM here aborts the backend (palloc), so
                // mirror that with `expect` rather than thread fallibility through
                // the infallible `standard_qp_callback` upcall chain.
                Some(e) => e
                    .clone_in(mcx)
                    .expect("intern_aggref_sort_inputs: copyObject OOM")
                    .erase_lifetime(),
                None => ::nodes::primnodes::Expr::Const(Default::default()),
            };
            args.push((
                expr,
                tle.resno,
                tle.resname.as_ref().map(|s| alloc::string::String::from(s.as_str())),
                tle.ressortgroupref,
                tle.resorigtbl,
                tle.resorigcol,
                tle.resjunk,
            ));
        }
        (sortlist, args)
    };

    let sortlist_ids: Vec<pathnodes::NodeId> = sortlist
        .into_iter()
        .map(|sgc| root.alloc_sortgroupclause(sgc))
        .collect();

    let args_ids: Vec<pathnodes::NodeId> = args
        .into_iter()
        .map(|(expr, resno, resname, ressortgroupref, resorigtbl, resorigcol, resjunk)| {
            let expr_id = root.alloc_node(expr);
            let te = pathnodes::TargetEntryNode {
                expr: expr_id,
                resno,
                resname,
                ressortgroupref,
                resorigtbl,
                resorigcol,
                resjunk,
            };
            root.alloc_targetentry(te)
        })
        .collect();

    (sortlist_ids, args_ids)
}

/// `generate_setop_child_grouplist(op, targetlist)` (planner.c:8295). Pair each
/// non-resjunk target with the parent op's groupClause (`group_clause_ids`, an
/// arena copy of `op->groupClauses`) and colType, reject on any type mismatch
/// (returns `None` -> NIL), and assign each SortGroupClause a `tleSortGroupRef`
/// matching the target. Mutates `root.processed_tlist` (assignSortGroupRef).
fn generate_setop_child_grouplist(
    root: &mut PlannerInfo,
    group_clause_ids: &[pathnodes::NodeId],
    col_types: &[Oid],
    targetlist: &[pathnodes::NodeId],
) -> Option<Vec<pathnodes::NodeId>> {
    let mut out: Vec<pathnodes::NodeId> = Vec::with_capacity(group_clause_ids.len());
    let mut lg = 0usize;
    let mut ct = 0usize;
    for &tnode in targetlist {
        if root.targetentry(tnode).resjunk {
            continue;
        }
        debug_assert!(lg < group_clause_ids.len());
        debug_assert!(ct < col_types.len());
        let coltype = col_types[ct];
        let te_expr = root.targetentry(tnode).expr;
        let exprtype =
            nodes_core::nodefuncs::expr_type(Some(root.node(te_expr))).unwrap_or(0);
        if coltype != exprtype {
            return None;
        }
        let sortgroupref = assign_setop_sort_group_ref(root, tnode, targetlist);
        let mut sgc = *root.sortgroupclause(group_clause_ids[lg]);
        sgc.tleSortGroupRef = sortgroupref;
        out.push(root.alloc_sortgroupclause(sgc));
        lg += 1;
        ct += 1;
    }
    debug_assert!(lg == group_clause_ids.len());
    debug_assert!(ct == col_types.len());
    Some(out)
}

/// `assignSortGroupRef(tle, tlist)` over the arena targetlist: ensure `tnode` has
/// a `ressortgroupref`, picking max-used+1 if unset, and return it.
fn assign_setop_sort_group_ref(
    root: &mut PlannerInfo,
    tnode: pathnodes::NodeId,
    tlist: &[pathnodes::NodeId],
) -> types_core::Index {
    let cur = root.targetentry(tnode).ressortgroupref;
    if cur != 0 {
        return cur;
    }
    let mut max_ref: types_core::Index = 0;
    for &t in tlist {
        let r = root.targetentry(t).ressortgroupref;
        if r > max_ref {
            max_ref = r;
        }
    }
    let new_ref = max_ref + 1;
    root.targetentry_mut(tnode).ressortgroupref = new_ref;
    new_ref
}

/// `standard_qp_callback(root, extra)` (planner.c:3453) — the
/// `query_pathkeys_callback` upcall, computing the grouping/ordering pathkeys
/// once EquivalenceClasses are canonical.
///
/// `sort_clause_ids` are the `parse->sortClause` `SortGroupClause`s bridged into
/// the planner node arena by `grouping_planner` (the only `Query`-resident input
/// the callback needs; the `query_planner` callback signature cannot reach
/// `run`). GROUP BY / DISTINCT / window / set-op / grouping-sets / ordered-aggs
/// are gated out by `grouping_planner`'s upstream loud-panics, so on this path:
///
///  * `group_pathkeys` / `window_pathkeys` / `distinct_pathkeys` /
///    `setop_pathkeys` are all NIL (their producing clauses are empty here), and
///  * `sort_pathkeys = make_pathkeys_for_sortclauses(parse->sortClause, tlist)`.
///
/// The `query_pathkeys` selection then reduces (group/window/distinct/setop all
/// empty) to `sort_pathkeys` when ORDER BY is present, else NIL — exactly the C
/// cascade (planner.c:3630-3642).
fn standard_qp_callback(
    root: &mut PlannerInfo,
    mcx: Mcx<'_>,
    sort_clause_ids: &[pathnodes::NodeId],
    distinct_clause_ids: &[pathnodes::NodeId],
    first_active_window: Option<pathnodes::NodeId>,
    gset_group_clause: Option<&[pathnodes::NodeId]>,
    setop_child: Option<(&[pathnodes::NodeId], &[Oid])>,
) -> PgResult<()> {
    // tlist = root->processed_tlist (C:3457).
    let tlist = root.processed_tlist.clone();
    // parse->hasGroupRTE == (root->group_rtindex != 0).
    let has_group_rte = root.group_rtindex != 0;

    // Grouping pathkeys (C:3463). With grouping sets, just use the first
    // RollupData's groupClause; we don't optimize grouping clauses nor combine
    // aggregate ordering keys with grouping (C:3464-3500).
    if let Some(gc_handles) = gset_group_clause {
        let group_clauses: Vec<::nodes::rawnodes::SortGroupClause> = gc_handles
            .iter()
            .map(|&id| *root.sortgroupclause(id))
            .collect();
        if vars::tlist::grouping_is_sortable(&group_clauses) {
            let mut processed = gc_handles.to_vec();
            let (pathkeys, _sortable) =
                pathkeys::make_pathkeys_for_sortclauses_extended(
                    root,
                    mcx,
                    &mut processed,
                    &tlist,
                    false,          // remove_redundant
                    has_group_rte,  // remove_group_rtindex
                    false,          // set_ec_sortref
                );
            // Assert(sortable) in C — grouping_is_sortable just verified it.
            root.num_groupby_pathkeys = pathkeys.len() as i32;
            root.group_pathkeys = pathkeys;
        } else {
            root.group_pathkeys = Vec::new();
            root.num_groupby_pathkeys = 0;
        }
    } else if !root.processed_groupClause.is_empty() || root.numOrderedAggs > 0 {
        let mut processed = root.processed_groupClause.clone();
        let (pathkeys, sortable) =
            pathkeys::make_pathkeys_for_sortclauses_extended(
                root,
                mcx,
                &mut processed,
                &tlist,
                true,  // remove_redundant
                false, // remove_group_rtindex
                true,  // set_ec_sortref
            );
        // make_pathkeys_for_sortclauses_extended may drop redundant clauses from
        // the passed list; reflect that back onto processed_groupClause.
        root.processed_groupClause = processed;
        if !sortable {
            // Can't sort; no point in considering aggregate ordering either.
            root.group_pathkeys = Vec::new();
            root.num_groupby_pathkeys = 0;
        } else {
            root.num_groupby_pathkeys = pathkeys.len() as i32;
            root.group_pathkeys = pathkeys;
            // If we have ordered aggs, consider adding onto group_pathkeys.
            if root.numOrderedAggs > 0 {
                adjust_group_pathkeys_for_groupagg(root, mcx);
            }
        }
    } else {
        root.group_pathkeys = Vec::new();
        root.num_groupby_pathkeys = 0;
    }

    // Window pathkeys: only the first (bottom) active window participates
    // (C:3545-3554). make_pathkeys_for_window also removes redundant partition
    // clauses from that WindowClause in place.
    root.window_pathkeys = match first_active_window {
        Some(wc_id) => make_pathkeys_for_window(root, mcx, wc_id, &tlist)?,
        None => Vec::new(),
    };

    // DISTINCT pathkeys (C:3564-3580). As with GROUP BY, discard DISTINCT items
    // proven redundant by EquivalenceClass processing; the non-redundant list is
    // kept in root->processed_distinctClause, leaving parse->distinctClause alone.
    // The interned distinctClause handles (a List of SortGroupClause values) were
    // bridged up front by the caller.
    if !distinct_clause_ids.is_empty() {
        // root->processed_distinctClause = list_copy(parse->distinctClause).
        let mut processed = distinct_clause_ids.to_vec();
        let (pathkeys, sortable) =
            pathkeys::make_pathkeys_for_sortclauses_extended(
                root,
                mcx,
                &mut processed,
                &tlist,
                true,  // remove_redundant
                false, // remove_group_rtindex
                false, // set_ec_sortref
            );
        root.processed_distinctClause = processed;
        root.distinct_pathkeys = if sortable { pathkeys } else { Vec::new() };
    } else {
        root.processed_distinctClause = Vec::new();
        root.distinct_pathkeys = Vec::new();
    }

    // set-op pathkeys: NIL on this path (set operations gated out upstream).
    root.setop_pathkeys = match setop_child {
        Some((group_clause_ids, col_types)) => {
            match generate_setop_child_grouplist(root, group_clause_ids, col_types, &tlist) {
                Some(mut group_clauses) => {
                    let (pathkeys, sortable) =
                        pathkeys::make_pathkeys_for_sortclauses_extended(
                            root, mcx, &mut group_clauses, &tlist, false, false, false,
                        );
                    // A volatile sort key's EquivalenceClass identifies its target
                    // entry only by ec_sortref, which a setop child's projected plan
                    // targetlist does not carry through our arena rebuild; such a key
                    // can't serve as a useful presort for the parent set-op's merge
                    // anyway, so don't advertise setop_pathkeys when any is volatile
                    // (PG plans these via Sort-over-Append regardless).
                    let any_volatile = pathkeys
                        .iter()
                        .any(|pk| pk.pk_eclass.is_some_and(|ec| root.ec(ec).ec_has_volatile));
                    if sortable && !any_volatile { pathkeys } else { Vec::new() }
                }
                None => Vec::new(),
            }
        }
        None => Vec::new(),
    };

    // root->sort_pathkeys =
    //   make_pathkeys_for_sortclauses(root, parse->sortClause, tlist) (C:3583).
    root.sort_pathkeys = pathkeys::make_pathkeys_for_sortclauses(
        root,
        mcx,
        sort_clause_ids,
        &tlist,
    );

    // query_pathkeys cascade (C:3630-3642): group/window empty, distinct empty
    // (so its length 0 is not > sort_pathkeys length), so query_pathkeys =
    // sort_pathkeys if non-empty, else (setop empty) NIL.
    root.query_pathkeys = if !root.group_pathkeys.is_empty() {
        root.group_pathkeys.clone()
    } else if !root.window_pathkeys.is_empty() {
        root.window_pathkeys.clone()
    } else if root.distinct_pathkeys.len() > root.sort_pathkeys.len() {
        root.distinct_pathkeys.clone()
    } else if !root.sort_pathkeys.is_empty() {
        root.sort_pathkeys.clone()
    } else if !root.setop_pathkeys.is_empty() {
        root.setop_pathkeys.clone()
    } else {
        Vec::new()
    };

    Ok(())
}

/// Read a `parse->sortClause` element (a `SortGroupClause` carried as
/// `NodePtr<Node>`) as a plain (`Copy`) [`::nodes::rawnodes::SortGroupClause`]
/// value for interning into the planner node arena. A `sortClause` element is
/// always a `SortGroupClause` (parser invariant); any other node is a bug.
fn sortgroupclause_from_node(
    np: &mcx::PgBox<'_, Node<'_>>,
) -> PgResult<::nodes::rawnodes::SortGroupClause> {
    let np = &**np;
    match np.node_tag() {
        ntag::T_SortGroupClause => Ok(*np.expect_sortgroupclause()),
        _ => panic!(
            "grouping_planner: sortClause element is not a SortGroupClause (got {:?})",
            np.tag()
        ),
    }
}

/// Intern `parse->sortClause` (a List of `SortGroupClause`) into the planner
/// node arena, returning the `NodeId` handles (the form
/// `make_pathkeys_for_sortclauses` consumes).
fn intern_sortclauses<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
) -> PgResult<Vec<pathnodes::NodeId>> {
    let clauses: Vec<::nodes::rawnodes::SortGroupClause> = run
        .resolve(root.parse)
        .sortClause
        .iter()
        .map(sortgroupclause_from_node)
        .collect::<PgResult<Vec<_>>>()?;
    Ok(clauses
        .into_iter()
        .map(|sgc| root.alloc_sortgroupclause(sgc))
        .collect())
}

/// `postprocess_setop_tlist(processed_tlist, parse->targetList)` (planner.c:5778)
/// — transfer `ressortgroupref` from the original parser tlist onto the setop
/// result tlist (already in `root`'s arena), skipping resjunk columns.
fn postprocess_setop_tlist<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
) -> PgResult<()> {
    // Snapshot the original tlist's (resno, resjunk, ressortgroupref).
    let orig: Vec<(types_core::primitive::AttrNumber, bool, types_core::primitive::Index)> = run
        .resolve(root.parse)
        .targetList
        .iter()
        .map(|te| (te.resno, te.resjunk, te.ressortgroupref))
        .collect();

    let tlist = root.processed_tlist.clone();
    let mut orig_idx = 0usize;
    for &te_id in tlist.iter() {
        if root.targetentry(te_id).resjunk {
            continue;
        }
        debug_assert!(orig_idx < orig.len());
        let (orig_resno, orig_resjunk, orig_ref) = orig[orig_idx];
        orig_idx += 1;
        if orig_resjunk {
            return Err(PgError::error(alloc::string::String::from(
                "resjunk output columns are not implemented",
            )));
        }
        debug_assert!(root.targetentry(te_id).resno == orig_resno);
        root.targetentry_mut(te_id).ressortgroupref = orig_ref;
    }
    if orig_idx != orig.len() {
        return Err(PgError::error(alloc::string::String::from(
            "resjunk output columns are not implemented",
        )));
    }
    Ok(())
}

/// `enable_incremental_sort` GUC, read through the guc-tables slot.
fn enable_incremental_sort() -> bool {
    guc_tables::vars::enable_incremental_sort.read()
}

/// `get_pathtarget_sortgroupref(target, i)` (pathnodes.h macro) — the i'th
/// sortgroupref, or 0 if the target carries no sortgrouprefs array / the slot is
/// unset.
#[inline]
fn get_pathtarget_sortgroupref(target: &PathTarget, i: usize) -> u32 {
    target.sortgrouprefs.get(i).copied().unwrap_or(0)
}

/// `make_sort_input_target(root, final_target, &have_postponed_srfs)`
/// (planner.c:6441).
///
/// Decide whether a post-sort projection is worthwhile and, if so, build the
/// PathTarget to be computed by the plan node immediately below the Sort (and
/// any Distinct) step. Returns `final_target` unchanged when no projection helps;
/// sets `*have_postponed_srfs` per the C contract.
fn make_sort_input_target<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    final_target: &PathTarget,
    have_postponed_srfs: &mut bool,
) -> PgResult<PathTarget> {
    // Shouldn't get here unless query has ORDER BY (C:6460).
    debug_assert!(!run.resolve(root.parse).sortClause.is_empty());

    *have_postponed_srfs = false; // default result.

    let has_target_srfs = run.resolve(root.parse).hasTargetSRFs;
    let tuple_fraction = root.tuple_fraction;
    let has_limit_count = run.resolve(root.parse).limitCount.is_some();

    // Inspect tlist and collect per-column information (C:6463-6535).
    let ncols = final_target.exprs.len();
    let mut col_is_srf = alloc::vec![false; ncols];
    let mut postpone_col = alloc::vec![false; ncols];
    let mut have_srf = false;
    let mut have_volatile = false;
    let mut have_expensive = false;
    let mut have_srf_sortcols = false;

    let cpu_op_cost = costsize::cpu_operator_cost();

    for i in 0..ncols {
        let expr_id = final_target.exprs[i];

        if get_pathtarget_sortgroupref(final_target, i) == 0 {
            // Check for SRF or volatile functions. SRF first (we must know
            // whether we have any postponed SRFs) (C:6486-6491).
            let returns_set = has_target_srfs
                && nodes_core::nodefuncs::expression_returns_set(Some(root.node(expr_id)));
            if returns_set {
                col_is_srf[i] = true;
                have_srf = true;
            } else if clauses::contain_volatile_functions(Some(
                root.node(expr_id),
            ))? {
                // Unconditionally postpone (C:6493-6498).
                postpone_col[i] = true;
                have_volatile = true;
            } else {
                // Else check the cost (C:6500-6519).
                let cost = costsize::cost_qual_eval_node(root, expr_id);
                // "expensive" = more than 10X cpu_operator_cost.
                if cost.per_tuple > 10.0 * cpu_op_cost {
                    postpone_col[i] = true;
                    have_expensive = true;
                }
            }
        } else {
            // For sortgroupref cols, just check if any contain SRFs (C:6522-6529).
            if !have_srf_sortcols
                && has_target_srfs
                && nodes_core::nodefuncs::expression_returns_set(Some(root.node(expr_id)))
            {
                have_srf_sortcols = true;
            }
        }
    }

    // We can postpone SRFs if we have some but none are in sortgroupref cols
    // (C:6538).
    let postpone_srfs = have_srf && !have_srf_sortcols;

    // If we don't need a post-sort projection, just return final_target
    // (C:6543-6546).
    if !(postpone_srfs
        || have_volatile
        || (have_expensive && (has_limit_count || tuple_fraction > 0.0)))
    {
        return Ok(final_target.clone());
    }

    // Report whether the post-sort projection contains SRFs (C:6554).
    *have_postponed_srfs = postpone_srfs;

    // Construct the sort-input target: all non-postponable columns, then add
    // Vars/PHVs/Aggrefs/WindowFuncs found in the postponable ones (C:6560-6583).
    let mut input_target = vars::tlist::create_empty_pathtarget();
    let mut postponable_cols: Vec<pathnodes::NodeId> = Vec::new();

    for i in 0..ncols {
        let expr_id = final_target.exprs[i];
        if postpone_col[i] || (postpone_srfs && col_is_srf[i]) {
            postponable_cols.push(expr_id);
        } else {
            vars::tlist::add_column_to_pathtarget(
                &mut input_target,
                expr_id,
                get_pathtarget_sortgroupref(final_target, i),
            );
        }
    }

    // Pull out all Vars/Aggrefs/WindowFuncs/PHVs in postponable columns and add
    // them to the sort-input target if not already present (C:6590-6595). We
    // mustn't deconstruct Aggrefs or WindowFuncs (use the INCLUDE flags).
    let pvc_flags = vars::PVC_INCLUDE_AGGREGATES
        | vars::PVC_INCLUDE_WINDOWFUNCS
        | vars::PVC_INCLUDE_PLACEHOLDERS;
    // pull_var_clause operates over a `Node`; run it per postponable column
    // (equivalent to walking the C `List *` of postponable exprs), interning each
    // pulled Var/Aggref/WindowFunc/PHV into the arena to obtain its handle.
    let mut postponable_vars: Vec<pathnodes::NodeId> = Vec::new();
    for &col in &postponable_cols {
        // Deep-copy via `clone_in` (C copyObject); a derived `Expr::clone`
        // panics on a context-allocated child (Aggref/SubLink/SubPlan).
        let node = Node::mk_expr(run.mcx(), root.node(col).clone_in(run.mcx())?)?;
        for v in vars::pull_var_clause(run.mcx(), &node, pvc_flags)? {
            postponable_vars.push(root.alloc_node(v));
        }
    }
    vars::tlist::add_new_columns_to_pathtarget(
        root,
        &mut input_target,
        &postponable_vars,
    );

    // set_pathtarget_cost_width(root, input_target) (C:6603).
    costsize::sizeest::set_pathtarget_cost_width(root, &mut input_target);
    Ok(input_target)
}

// ===========================================================================
// create_distinct_paths()  (planner.c:4779)
// ===========================================================================

/// `create_distinct_paths(root, input_rel, target)` (planner.c:4790).
///
/// Build a new `UPPERREL_DISTINCT` upperrel containing Paths for SELECT DISTINCT
/// evaluation. Input paths must already compute the desired pathtarget (Sort/
/// Unique won't project anything).
fn create_distinct_paths<'mcx>(
    _mcx: Mcx<'mcx>,
    run: &mut PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    input_rel: RelId,
    target: &PathTarget,
) -> PgResult<RelId> {
    // distinct_rel = fetch_upper_rel(root, UPPERREL_DISTINCT, NULL) (C:4796).
    let distinct_rel =
        relnode::fetch_upper_rel(root, UPPERREL_DISTINCT, &None);

    // distinct_rel is parallel-safe iff the input rel is; the FDW fields are
    // copied from input_rel (C:4805-4813).
    {
        let (cp, serverid, userid, useridiscurrent, has_fdwroutine) = {
            let ir = root.rel(input_rel);
            (
                ir.consider_parallel,
                ir.serverid,
                ir.userid,
                ir.useridiscurrent,
                ir.has_fdwroutine,
            )
        };
        let dr = root.rel_mut(distinct_rel);
        dr.consider_parallel = cp;
        dr.serverid = serverid;
        dr.userid = userid;
        dr.useridiscurrent = useridiscurrent;
        dr.has_fdwroutine = has_fdwroutine;
    }

    // Build distinct paths based on input_rel's pathlist (C:4815).
    create_final_distinct_paths(run, root, input_rel, distinct_rel)?;

    // Build distinct paths based on input_rel's partial_pathlist (C:4818).
    create_partial_distinct_paths(run, root, input_rel, distinct_rel, target)?;

    // Give a helpful error if we failed to create any paths (C:4821-4826).
    if root.rel(distinct_rel).pathlist.is_empty() {
        return Err(PgError::error(
            "could not implement DISTINCT: some of the datatypes only support \
             hashing, while others only support sorting",
        ));
    }

    // FDW GetForeignUpperPaths (C:4831-4838) and create_upper_paths_hook
    // (C:4841-4843): no FDW upper paths and no extension hook are modeled on this
    // path; has_fdwroutine being set would mean a foreign-table query, which the
    // scan path does not reach here. Mirror PG: skip when absent.
    debug_assert!(!root.rel(distinct_rel).has_fdwroutine);

    // set_cheapest(distinct_rel) (C:4846).
    pathnode::set_cheapest(root, distinct_rel)?;

    Ok(distinct_rel)
}

/// `create_partial_distinct_paths(root, input_rel, final_distinct_rel, target)`
/// (planner.c:4860).
///
/// Process `input_rel`'s partial paths and add unique/aggregate paths to the
/// `UPPERREL_PARTIAL_DISTINCT` rel, then add Gather/GatherMerge on top and a
/// final unique/aggregate to de-duplicate combined worker rows.
fn create_partial_distinct_paths<'mcx>(
    run: &mut PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    input_rel: RelId,
    final_distinct_rel: RelId,
    target: &PathTarget,
) -> PgResult<()> {
    // Nothing to do when there are no partial paths in the input rel (C:4874).
    let (consider_parallel, partial_pathlist_empty) = {
        let ir = root.rel(input_rel);
        (ir.consider_parallel, ir.partial_pathlist.is_empty())
    };
    if !consider_parallel || partial_pathlist_empty {
        return Ok(());
    }

    // can't do parallel DISTINCT ON (C:4880-4881).
    let has_distinct_on = run.resolve(root.parse).hasDistinctOn;
    if has_distinct_on {
        return Ok(());
    }

    // partial_distinct_rel = fetch_upper_rel(root, UPPERREL_PARTIAL_DISTINCT,
    //   NULL); reltarget = target; copy parallel/FDW fields (C:4883-4895).
    let partial_distinct_rel = relnode::fetch_upper_rel(
        root,
        UPPERREL_PARTIAL_DISTINCT,
        &None,
    );
    {
        let (cp, serverid, userid, useridiscurrent, has_fdwroutine) = {
            let ir = root.rel(input_rel);
            (
                ir.consider_parallel,
                ir.serverid,
                ir.userid,
                ir.useridiscurrent,
                ir.has_fdwroutine,
            )
        };
        let pdr = root.rel_mut(partial_distinct_rel);
        pdr.reltarget = Some(Box::new(target.clone()));
        pdr.consider_parallel = cp;
        pdr.serverid = serverid;
        pdr.userid = userid;
        pdr.useridiscurrent = useridiscurrent;
        pdr.has_fdwroutine = has_fdwroutine;
    }

    // cheapest_partial_path = linitial(input_rel->partial_pathlist) (C:4897).
    let cheapest_partial_path = root.rel(input_rel).partial_pathlist[0];

    // distinctExprs = get_sortgrouplist_exprs(processed_distinctClause, targetList)
    // (C:4899); numDistinctRows = estimate_num_groups(...) (C:4903) over the
    // cheapest partial path's row estimate.
    let distinct_exprs = distinct_list_exprs(root)?;
    let cheapest_partial_rows = root.path(cheapest_partial_path).base().rows;
    let num_distinct_rows = selfuncs_seams::estimate_num_groups::call(
        run,
        root,
        &distinct_exprs,
        cheapest_partial_rows,
        None,
    )?;

    let distinct_clauses: Vec<::nodes::rawnodes::SortGroupClause> = root
        .processed_distinctClause
        .iter()
        .map(|&id| *root.sortgroupclause(id))
        .collect();

    // Sort-based partial distinct (C:4912-4969).
    if vars::tlist::grouping_is_sortable(&distinct_clauses) {
        let input_paths = root.rel(input_rel).partial_pathlist.clone();
        for input_path in input_paths {
            let input_path_pathkeys = root.path(input_path).base().pathkeys.clone();
            let distinct_pathkeys = root.distinct_pathkeys.clone();
            // hasDistinctOn is false here (parallel DISTINCT ON returned early).
            let useful_pathkeys_list = get_useful_pathkeys_for_distinct(
                root,
                false,
                &distinct_pathkeys,
                &input_path_pathkeys,
            )?;
            debug_assert!(!useful_pathkeys_list.is_empty());

            for useful_pathkeys in useful_pathkeys_list {
                let sorted_path = make_ordered_path(
                    root,
                    run,
                    partial_distinct_rel,
                    input_path,
                    cheapest_partial_path,
                    useful_pathkeys,
                    -1.0,
                )?;
                let sorted_path = match sorted_path {
                    Some(p) => p,
                    None => continue,
                };

                if root.distinct_pathkeys.is_empty() {
                    // All tuples have the same value for the DISTINCT clause; cap
                    // each worker to 1 row via a LimitPath (C:4938-4961).
                    let limit_count = make_int8_one_const(root);
                    let limit_path = pathnode::create::create_limit_path(
                        root,
                        partial_distinct_rel,
                        sorted_path,
                        None,
                        Some(limit_count),
                        pathnodes::LIMIT_OPTION_COUNT,
                        0,
                        1,
                    )?;
                    pathnode::add_partial_path(
                        root,
                        partial_distinct_rel,
                        limit_path,
                    )?;
                } else {
                    let num_cols = root.distinct_pathkeys.len() as i32;
                    let unique_path =
                        pathnode::create::create_upper_unique_path(
                            root,
                            partial_distinct_rel,
                            sorted_path,
                            num_cols,
                            num_distinct_rows,
                        )?;
                    pathnode::add_partial_path(
                        root,
                        partial_distinct_rel,
                        unique_path,
                    )?;
                }
            }
        }
    }

    // Hash-aggregate partial distinct (C:4977-4990). enable_hashagg is a hard
    // off-switch here.
    let enable_hashagg = guc_tables::vars::enable_hashagg.read();
    if enable_hashagg
        && vars::tlist::grouping_is_hashable(&distinct_clauses)
    {
        let target = root
            .path(cheapest_partial_path)
            .base()
            .pathtarget
            .clone()
            .expect("create_partial_distinct_paths: cheapest_partial_path has no pathtarget");
        let group_clause = root.processed_distinctClause.clone();
        let agg_path = pathnode::create::create_agg_path(
            run,
            root,
            partial_distinct_rel,
            cheapest_partial_path,
            target,
            pathnodes::AGG_HASHED,
            pathnodes::AGGSPLIT_SIMPLE,
            group_clause,
            Vec::new(),
            None,
            num_distinct_rows,
        )?;
        pathnode::add_partial_path(root, partial_distinct_rel, agg_path)?;
    }

    // FDW upper paths / create_upper_paths_hook (C:4996-5008): not modeled.
    debug_assert!(!root.rel(partial_distinct_rel).has_fdwroutine);

    // If we made any partial paths, Gather them and add a final distinctify step
    // to remove duplicates from combining workers (C:5010-5034).
    if !root.rel(partial_distinct_rel).partial_pathlist.is_empty() {
        allpaths::generate_useful_gather_paths(
            root,
            run,
            partial_distinct_rel,
            true,
        )?;
        pathnode::set_cheapest(root, partial_distinct_rel)?;
        create_final_distinct_paths(run, root, partial_distinct_rel, final_distinct_rel)?;
    }

    Ok(())
}

/// `create_final_distinct_paths(root, input_rel, distinct_rel)` (planner.c:5043).
///
/// Create distinct paths in `distinct_rel` based on `input_rel`'s pathlist.
fn create_final_distinct_paths<'mcx>(
    run: &mut PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    input_rel: RelId,
    distinct_rel: RelId,
) -> PgResult<RelId> {
    let cheapest_input_path = root
        .rel(input_rel)
        .cheapest_total_path
        .expect("create_final_distinct_paths: input_rel has no cheapest_total_path");

    // Estimate number of distinct rows (C:5052-5074).
    let (has_group_clause, has_grouping_sets, has_aggs) = {
        let parse = run.resolve(root.parse);
        (
            !parse.groupClause.is_empty(),
            !parse.groupingSets.is_empty(),
            parse.hasAggs,
        )
    };
    let cheapest_input_rows = root.path(cheapest_input_path).base().rows;
    let num_distinct_rows =
        if has_group_clause || has_grouping_sets || has_aggs || root.hasHavingQual {
            // Grouping/aggregation already mostly unique: assume input rows.
            cheapest_input_rows
        } else {
            // UNIQUE filter has effects comparable to GROUP BY.
            let distinct_exprs = distinct_list_exprs(root)?;
            selfuncs_seams::estimate_num_groups::call(
                run,
                root,
                &distinct_exprs,
                cheapest_input_rows,
                None,
            )?
        };

    let distinct_clauses: Vec<::nodes::rawnodes::SortGroupClause> = root
        .processed_distinctClause
        .iter()
        .map(|&id| *root.sortgroupclause(id))
        .collect();

    // Sort-based DISTINCT (C:5079-5184).
    if vars::tlist::grouping_is_sortable(&distinct_clauses) {
        let limittuples = if root.distinct_pathkeys.is_empty() { 1.0 } else { -1.0 };

        // With DISTINCT ON, sort by the more rigorous of DISTINCT and ORDER BY
        // (C:5099-5104). The parser ensures one is a prefix of the other.
        let has_distinct_on = run.resolve(root.parse).hasDistinctOn;
        let needed_pathkeys = if has_distinct_on
            && root.distinct_pathkeys.len() < root.sort_pathkeys.len()
        {
            root.sort_pathkeys.clone()
        } else {
            root.distinct_pathkeys.clone()
        };

        let input_paths = root.rel(input_rel).pathlist.clone();
        for input_path in input_paths {
            let input_path_pathkeys = root.path(input_path).base().pathkeys.clone();
            let useful_pathkeys_list =
                get_useful_pathkeys_for_distinct(
                    root,
                    has_distinct_on,
                    &needed_pathkeys,
                    &input_path_pathkeys,
                )?;
            debug_assert!(!useful_pathkeys_list.is_empty());

            for useful_pathkeys in useful_pathkeys_list {
                let sorted_path = make_ordered_path(
                    root,
                    run,
                    distinct_rel,
                    input_path,
                    cheapest_input_path,
                    useful_pathkeys,
                    limittuples,
                )?;
                let sorted_path = match sorted_path {
                    Some(p) => p,
                    None => continue,
                };

                if root.distinct_pathkeys.is_empty() {
                    // All pathkeys redundant => every DISTINCT target single-valued
                    // => "LIMIT 1" suffices (C:5147-5168).
                    let limit_count = make_int8_one_const(root);
                    let limit_path = pathnode::create::create_limit_path(
                        root,
                        distinct_rel,
                        sorted_path,
                        None,
                        Some(limit_count),
                        pathnodes::LIMIT_OPTION_COUNT,
                        0,
                        1,
                    )?;
                    pathnode::add_path(root, distinct_rel, limit_path)?;
                } else {
                    let num_cols = root.distinct_pathkeys.len() as i32;
                    let unique_path =
                        pathnode::create::create_upper_unique_path(
                            root,
                            distinct_rel,
                            sorted_path,
                            num_cols,
                            num_distinct_rows,
                        )?;
                    pathnode::add_path(root, distinct_rel, unique_path)?;
                }
            }
        }
    }

    // Hash-based DISTINCT (C:5186-5213).
    let enable_hashagg = guc_tables::vars::enable_hashagg.read();
    let has_distinct_on = run.resolve(root.parse).hasDistinctOn;
    let allow_hash = if root.rel(distinct_rel).pathlist.is_empty() {
        // No alternatives — we *must* hash or die trying.
        true
    } else if has_distinct_on || !enable_hashagg {
        // Policy-based decision not to hash.
        false
    } else {
        true
    };

    if allow_hash
        && vars::tlist::grouping_is_hashable(&distinct_clauses)
    {
        let target = root
            .path(cheapest_input_path)
            .base()
            .pathtarget
            .clone()
            .expect("create_final_distinct_paths: cheapest_input_path has no pathtarget");
        let group_clause = root.processed_distinctClause.clone();
        let agg_path = pathnode::create::create_agg_path(
            run,
            root,
            distinct_rel,
            cheapest_input_path,
            target,
            pathnodes::AGG_HASHED,
            pathnodes::AGGSPLIT_SIMPLE,
            group_clause,
            Vec::new(),
            None,
            num_distinct_rows,
        )?;
        pathnode::add_path(root, distinct_rel, agg_path)?;
    }

    Ok(distinct_rel)
}

/// `get_useful_pathkeys_for_distinct(root, needed_pathkeys, path_pathkeys)`
/// (planner.c:5223).
///
/// Returns a list of pathkey orderings useful for DISTINCT / DISTINCT ON by
/// reordering `needed_pathkeys` to match `path_pathkeys` as much as possible.
/// Always includes `needed_pathkeys` itself.
fn get_useful_pathkeys_for_distinct(
    root: &PlannerInfo,
    has_distinct_on: bool,
    needed_pathkeys: &[pathnodes::PathKey],
    path_pathkeys: &[pathnodes::PathKey],
) -> PgResult<Vec<Vec<pathnodes::PathKey>>> {
    // Always include the given 'needed_pathkeys' (C:5230).
    let mut useful_pathkeys_list: Vec<Vec<pathnodes::PathKey>> =
        alloc::vec![needed_pathkeys.to_vec()];

    if !get_enable_distinct_reordering() {
        return Ok(useful_pathkeys_list);
    }

    // Scan path_pathkeys, building the longest prefix that matches needed_pathkeys
    // (C:5241-5258). PathKeys are canonical: pointer comparison in C == PathKey
    // value equality here (the Ec id + opfamily + cmptype + nulls_first identify
    // the canonical key).
    let mut useful_pathkeys: Vec<pathnodes::PathKey> = Vec::new();
    for pathkey in path_pathkeys {
        if !needed_pathkeys.contains(pathkey) {
            break;
        }
        if has_distinct_on && !root.distinct_pathkeys.contains(pathkey) {
            break;
        }
        useful_pathkeys.push(pathkey.clone());
    }

    // If no match at all, no point in reordering needed_pathkeys (C:5261).
    if useful_pathkeys.is_empty() {
        return Ok(useful_pathkeys_list);
    }

    // If not a full match, the result is not useful without incremental sort
    // (C:5267-5269).
    if useful_pathkeys.len() < needed_pathkeys.len() && !enable_incremental_sort() {
        return Ok(useful_pathkeys_list);
    }

    // Append the remaining PathKey nodes in needed_pathkeys
    // (list_concat_unique_ptr) (C:5272).
    for pk in needed_pathkeys {
        if !useful_pathkeys.contains(pk) {
            useful_pathkeys.push(pk.clone());
        }
    }

    // If the resulting list equals needed_pathkeys, just drop it (C:5277-5279).
    if pathkeys::compare_pathkeys(needed_pathkeys, &useful_pathkeys)
        == pathnode_seams::PathKeysComparison::Equal
    {
        return Ok(useful_pathkeys_list);
    }

    useful_pathkeys_list.push(useful_pathkeys);
    Ok(useful_pathkeys_list)
}

/// `get_sortgrouplist_exprs(root->processed_distinctClause, parse->targetList)` —
/// the DISTINCT expressions as arena node handles, for estimate_num_groups. Each
/// SortGroupClause's expression resolves against processed_tlist (the planner's
/// working targetlist, which derives from parse->targetList).
fn distinct_list_exprs(root: &mut PlannerInfo) -> PgResult<Vec<pathnodes::NodeId>> {
    let distinct_clause = root.processed_distinctClause.clone();
    let tlist = root.processed_tlist.clone();
    Ok(distinct_clause
        .iter()
        .map(|&sgc| nodeFuncs_seams::get_sortgroupclause_expr::call(root, sgc, &tlist))
        .collect())
}

/// Build an `INT8` `Const` of value 1 (`makeConst(INT8OID, -1, InvalidOid,
/// sizeof(int64), Int64GetDatum(1), false, FLOAT8PASSBYVAL)`) and intern it,
/// returning its arena handle — the `LIMIT 1` count used by the all-redundant-
/// pathkeys DISTINCT legs (planner.c:4942 / 5151).
fn make_int8_one_const(root: &mut PlannerInfo) -> pathnodes::NodeId {
    let c = ::nodes::primnodes::Const {
        consttype: types_core::catalog::INT8OID,
        consttypmod: -1,
        constcollid: 0,
        constlen: core::mem::size_of::<i64>() as i32,
        constvalue: types_tuple::heaptuple::Datum::ByVal(1),
        constisnull: false,
        constbyval: true,
        location: -1,
    };
    root.alloc_node(::nodes::primnodes::Expr::Const(c))
}

/// `create_ordered_paths(root, input_rel, target, target_parallel_safe,
/// limit_tuples)` (planner.c:5308).
///
/// Build a new `UPPERREL_ORDERED` upperrel whose Paths satisfy `root->sort_pathkeys`
/// and project `target`. Considers an explicit full sort and an incremental sort
/// over the cheapest-total existing path; the partial-path / Gather-Merge block
/// is reached only when the input rel is parallel-safe.
fn create_ordered_paths<'mcx>(
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    input_rel: RelId,
    target: &PathTarget,
    target_parallel_safe: bool,
    limit_tuples: f64,
) -> PgResult<RelId> {
    let cheapest_input_path = root
        .rel(input_rel)
        .cheapest_total_path
        .expect("create_ordered_paths: input_rel has no cheapest_total_path");

    // For now, do all work in the (ORDERED, NULL) upperrel (C:5318).
    let ordered_rel = relnode::fetch_upper_rel(root, UPPERREL_ORDERED, &None);

    // consider_parallel propagation (C:5325-5326).
    let input_consider_parallel = root.rel(input_rel).consider_parallel;
    if input_consider_parallel && target_parallel_safe {
        root.rel_mut(ordered_rel).consider_parallel = true;
    }

    // If the input rel belongs to a single FDW, so does ordered_rel (C:5331-5335).
    {
        let (serverid, userid, useridiscurrent, has_fdwroutine) = {
            let ir = root.rel(input_rel);
            (ir.serverid, ir.userid, ir.useridiscurrent, ir.has_fdwroutine)
        };
        let or = root.rel_mut(ordered_rel);
        or.serverid = serverid;
        or.userid = userid;
        or.useridiscurrent = useridiscurrent;
        or.has_fdwroutine = has_fdwroutine;
    }

    let sort_pathkeys = root.sort_pathkeys.clone();

    // foreach(lc, input_rel->pathlist) (C:5337).
    let pathlist = root.rel(input_rel).pathlist.clone();
    for input_path in pathlist {
        let input_pathkeys = root.path(input_path).base().pathkeys.clone();
        let (is_sorted, presorted_keys) = pathkeys::pathkeys_count_contained_in(
            &sort_pathkeys,
            &input_pathkeys,
        );

        let mut sorted_path = if is_sorted {
            input_path
        } else {
            // Try at least sorting the cheapest path and also incrementally
            // sorting any partially-sorted path (C:5352-5379).
            if input_path != cheapest_input_path
                && (presorted_keys == 0 || !enable_incremental_sort())
            {
                continue;
            }
            if presorted_keys == 0 || !enable_incremental_sort() {
                pathnode::create::create_sort_path(
                    root,
                    ordered_rel,
                    input_path,
                    sort_pathkeys.clone(),
                    limit_tuples,
                )?
            } else {
                pathnode::create::create_incremental_sort_path(
                    root,
                    run,
                    ordered_rel,
                    input_path,
                    sort_pathkeys.clone(),
                    presorted_keys,
                    limit_tuples,
                )?
            }
        };

        // If the result path's pathtarget differs from `target`, project
        // (C:5384-5387).
        if !pathtarget_exprs_equal(root, sorted_path, target) {
            sorted_path = pathnode::create::apply_projection_to_path(
                root,
                ordered_rel,
                sorted_path,
                Box::new(target.clone()),
            )?;
        }

        pathnode::add_path(root, ordered_rel, sorted_path)?;
    }

    // Generate a partial-path + Gather-Merge plan for the relation, in case the
    // sort is computed in parallel (C:5395-5472). It may make sense to sort the
    // cheapest partial path or incrementally sort any partial path that is
    // partially sorted according to the required output order and then use
    // Gather Merge. Only reachable when ordered_rel is parallel-safe and there
    // are partial paths; on the non-parallel SELECT path this is skipped.
    let ordered_consider_parallel = root.rel(ordered_rel).consider_parallel;
    let input_has_partials = !root.rel(input_rel).partial_pathlist.is_empty();
    if ordered_consider_parallel && !sort_pathkeys.is_empty() && input_has_partials {
        let cheapest_partial_path = root.rel(input_rel).partial_pathlist[0];

        let partial_pathlist = root.rel(input_rel).partial_pathlist.clone();
        for input_path in partial_pathlist {
            let input_pathkeys = root.path(input_path).base().pathkeys.clone();
            let (is_sorted, presorted_keys) =
                pathkeys::pathkeys_count_contained_in(
                    &sort_pathkeys,
                    &input_pathkeys,
                );

            if is_sorted {
                continue;
            }

            // Try at least sorting the cheapest path and also try incrementally
            // sorting any path which is partially sorted already (no need to
            // deal with paths which have presorted keys when incremental sort
            // is disabled unless it's the cheapest partial path).
            if input_path != cheapest_partial_path
                && (presorted_keys == 0 || !enable_incremental_sort())
            {
                continue;
            }

            // We've no need to consider both a sort and incremental sort. We'll
            // just do a sort if there are no presorted keys and an incremental
            // sort when there are presorted keys.
            let mut sorted_path = if presorted_keys == 0 || !enable_incremental_sort() {
                pathnode::create::create_sort_path(
                    root,
                    ordered_rel,
                    input_path,
                    sort_pathkeys.clone(),
                    limit_tuples,
                )?
            } else {
                pathnode::create::create_incremental_sort_path(
                    root,
                    run,
                    ordered_rel,
                    input_path,
                    sort_pathkeys.clone(),
                    presorted_keys,
                    limit_tuples,
                )?
            };

            let total_groups = costsize::compute_gather_rows(
                root.path(sorted_path).base(),
            );
            // create_gather_merge_path(root, ordered_rel, sorted_path,
            //   sorted_path->pathtarget, root->sort_pathkeys, NULL, &total_groups).
            let sorted_pathtarget = root
                .path(sorted_path)
                .base()
                .pathtarget
                .clone();
            sorted_path = pathnode::create::create_gather_merge_path(
                root,
                run,
                ordered_rel,
                sorted_path,
                sorted_pathtarget,
                sort_pathkeys.clone(),
                &None,
                Some(total_groups),
            )?;

            // If the pathtarget of the result path has different expressions
            // from the target to be applied, a projection step is needed.
            if !pathtarget_exprs_equal(root, sorted_path, target) {
                sorted_path = pathnode::create::apply_projection_to_path(
                    root,
                    ordered_rel,
                    sorted_path,
                    Box::new(target.clone()),
                )?;
            }

            pathnode::add_path(root, ordered_rel, sorted_path)?;
        }
    }

    // GetForeignUpperPaths (C:5475-5481): no FDW upper-path routine modeled.
    // create_upper_paths_hook (C:5484-5486): no hook.

    // set_cheapest is not needed here (grouping_planner doesn't require it).
    debug_assert!(!root.rel(ordered_rel).pathlist.is_empty());
    Ok(ordered_rel)
}

/// `equal(path->pathtarget->exprs, target->exprs)` over a path's PathTarget and
/// a target. A path always has a non-NULL pathtarget here.
fn pathtarget_exprs_equal(root: &PlannerInfo, path: PathId, target: &PathTarget) -> bool {
    let path_exprs: Vec<pathnodes::NodeId> = root
        .path(path)
        .base()
        .pathtarget
        .as_ref()
        .map(|t| t.exprs.clone())
        .unwrap_or_default();
    equal_expr_handle_lists(root, &path_exprs, &target.exprs)
}

// ===========================================================================
// preprocess_rowmarks()  (planner.c:2399)
// ===========================================================================

/// Read a `parse->rowMarks` element (a `RowMarkClause` carried as `NodePtr<Node>`)
/// as a plain (`Copy`) [`::nodes::rawnodes::RowMarkClause`] value. Every
/// rowMarks element is a `RowMarkClause` (parser invariant: built by
/// `transformLockingClause`).
fn rowmarkclause_from_node(
    np: &mcx::PgBox<'_, Node<'_>>,
) -> PgResult<::nodes::rawnodes::RowMarkClause> {
    let np = &**np;
    match np.node_tag() {
        ntag::T_RowMarkClause => Ok(*np.expect_rowmarkclause()),
        _ => panic!(
            "preprocess_rowmarks: rowMarks element is not a RowMarkClause (got {:?})",
            np.tag()
        ),
    }
}

/// `preprocess_rowmarks(root)` (planner.c:2399).
fn preprocess_rowmarks<'mcx>(
    mcx: Mcx<'mcx>,
    run: &mut PlannerRun<'mcx>,
    root: &mut PlannerInfo,
) -> PgResult<()> {
    // Snapshot the RowMarkClause values + command type from the Query. The
    // rowMarks list elements are RowMarkClause nodes (Copy values).
    let (row_mark_clauses, command_type, result_relation) = {
        let parse = run.resolve(root.parse);
        let rmcs: Vec<::nodes::rawnodes::RowMarkClause> = parse
            .rowMarks
            .iter()
            .map(|np| rowmarkclause_from_node(np))
            .collect::<PgResult<Vec<_>>>()?;
        (rmcs, parse.commandType, parse.resultRelation)
    };

    if !row_mark_clauses.is_empty() {
        // We've got trouble if FOR [KEY] UPDATE/SHARE appears inside grouping,
        // since grouping renders a reference to individual tuple CTIDs invalid.
        // CheckSelectLocking(parse, linitial(rowMarks)->strength) (C:2415-2416).
        let first_strength = row_mark_clauses[0].strength;
        let parse = run.resolve(root.parse);
        parser_analyze::CheckSelectLocking(parse, first_strength)?;
    } else {
        // We only need rowmarks for UPDATE, DELETE, MERGE, or FOR [KEY]
        // UPDATE/SHARE (C:2419-2427).
        if command_type != CmdType::CMD_UPDATE
            && command_type != CmdType::CMD_DELETE
            && command_type != CmdType::CMD_MERGE
        {
            return Ok(());
        }
    }

    // We need rowmarks for all base relations except the target. Make a bitmapset
    // of all base rels, then remove items we don't need or have FOR [KEY]
    // UPDATE/SHARE marks for (C:2429-2437).
    //
    // rels = get_relids_in_jointree((Node *) parse->jointree, false, false).
    let mut rels: Option<mcx::PgBox<'mcx, ::nodes::bitmapset::Bitmapset<'mcx>>> = {
        let jointree_node: Option<Node<'mcx>> = match run.resolve(root.parse).jointree.as_deref() {
            Some(f) => Some(Node::mk_from_expr(mcx, f.clone_in(mcx)?)?),
            None => None,
        };
        match jointree_node {
            Some(node) => prepjointree::get_relids_in_jointree(
                mcx, &node, false, false,
            )?,
            None => None,
        }
    };
    if result_relation != 0 {
        rels = nodes_core::bitmapset::bms_del_member(rels, result_relation);
    }

    // Convert RowMarkClauses to PlanRowMark representation (C:2439-2479).
    let mut prowmarks: Vec<pathnodes::PlanRowMarkId> = Vec::new();
    for rc in &row_mark_clauses {
        // rte = rt_fetch(rc->rti, parse->rtable).
        let rte_is_relation = {
            let parse = run.resolve(root.parse);
            let rte = &parse.rtable[(rc.rti - 1) as usize];
            rte.rtekind == ::nodes::parsenodes::RTEKind::RTE_RELATION
        };

        // Currently it is syntactically impossible to have FOR UPDATE et al
        // applied to an update/delete target rel (C:2451 Assert).
        debug_assert!(rc.rti as i32 != result_relation);

        // Ignore RowMarkClauses for subqueries; they aren't real tables and can't
        // support true locking. Any non-flattened subquery RTE gets a
        // ROW_MARK_COPY item in the next loop (C:2460-2467).
        if !rte_is_relation {
            continue;
        }

        rels = nodes_core::bitmapset::bms_del_member(rels, rc.rti as i32);

        let mark_type = {
            let parse = run.resolve(root.parse);
            let rte = &parse.rtable[(rc.rti - 1) as usize];
            select_rowmark_type(rte, rc.strength)?
        };
        let rowmark_id = {
            let glob = root.glob.as_mut().expect("preprocess_rowmarks: glob");
            glob.last_row_mark_id += 1;
            glob.last_row_mark_id
        };
        let newrc = ::nodes::nodelockrows::PlanRowMark {
            type_: ::nodes::nodelockrows::T_PlanRowMark,
            rti: rc.rti,
            prti: rc.rti,
            rowmarkId: rowmark_id,
            markType: mark_type as u32 as i32,
            allMarkTypes: 1 << (mark_type as u32),
            strength: rc.strength as u32 as i32,
            waitPolicy: rc.waitPolicy as u32 as i32,
            isParent: false,
        };
        prowmarks.push(run.intern_rowmark(newrc));
    }

    // Now add rowmarks for any non-target, non-locked base relations (C:2481-2503).
    let rtable_len = run.resolve(root.parse).rtable.len();
    for idx in 0..rtable_len {
        // i is the 1-based RT index.
        let i = (idx + 1) as i32;
        if !nodes_core::bitmapset::bms_is_member(i, rels.as_deref()) {
            continue;
        }

        let mark_type = {
            let parse = run.resolve(root.parse);
            let rte = &parse.rtable[idx];
            select_rowmark_type(rte, LockClauseStrength::LCS_NONE)?
        };
        let rowmark_id = {
            let glob = root.glob.as_mut().expect("preprocess_rowmarks: glob");
            glob.last_row_mark_id += 1;
            glob.last_row_mark_id
        };
        let newrc = ::nodes::nodelockrows::PlanRowMark {
            type_: ::nodes::nodelockrows::T_PlanRowMark,
            rti: i as types_core::Index,
            prti: i as types_core::Index,
            rowmarkId: rowmark_id,
            markType: mark_type as u32 as i32,
            allMarkTypes: 1 << (mark_type as u32),
            strength: LockClauseStrength::LCS_NONE as u32 as i32,
            // waitPolicy doesn't matter for a reference rowmark.
            waitPolicy: ::nodes::rawnodes::LockWaitPolicy::LockWaitBlock as u32 as i32,
            isParent: false,
        };
        prowmarks.push(run.intern_rowmark(newrc));
    }

    // root->rowMarks = prowmarks (C:2505).
    root.rowMarks = prowmarks;
    Ok(())
}

// ===========================================================================
// select_rowmark_type()  (planner.c:2510)
// ===========================================================================

const RELKIND_FOREIGN_TABLE: i8 = b'f' as i8;
const RELKIND_RELATION_KIND: i8 = b'r' as i8; // unused but documents the regular case
const _: i8 = RELKIND_RELATION_KIND;

/// `select_rowmark_type(rte, strength)` (planner.c:2510).
pub fn select_rowmark_type(
    rte: &RangeTblEntry,
    strength: LockClauseStrength,
) -> PgResult<RowMarkType> {
    use ::nodes::parsenodes::RTEKind;

    if rte.rtekind != RTEKind::RTE_RELATION {
        // If it's not a table at all, use ROW_MARK_COPY (C:2513-2517).
        return Ok(RowMarkType::Copy);
    } else if rte.relkind == RELKIND_FOREIGN_TABLE {
        // Let the FDW select the rowmark type, if it wants to (C:2518-2526).
        // GetFdwRoutineByRelId(relid)->GetForeignRowMarkType — the FdwRoutine
        // vtable's GetForeignRowMarkType callback is not modeled in the ported
        // FdwRoutine. Mirror PG and panic precisely; falls back to ROW_MARK_COPY
        // only when the FDW provides no callback, which we cannot determine.
        panic!(
            "select_rowmark_type: foreign-table rowmark via \
             FdwRoutine.GetForeignRowMarkType (fdwapi.h) is not modeled on the \
             ported FdwRoutine vtable"
        );
    } else {
        // Regular table, apply the appropriate lock type (C:2528-2556).
        match strength {
            LockClauseStrength::LCS_NONE => Ok(RowMarkType::Reference),
            LockClauseStrength::LCS_FORKEYSHARE => Ok(RowMarkType::KeyShare),
            LockClauseStrength::LCS_FORSHARE => Ok(RowMarkType::Share),
            LockClauseStrength::LCS_FORNOKEYUPDATE => Ok(RowMarkType::NoKeyExclusive),
            LockClauseStrength::LCS_FORUPDATE => Ok(RowMarkType::Exclusive),
        }
    }
}

// ===========================================================================
// preprocess_limit()  (planner.c:2577) + limit_needed() (planner.c:2762)
// ===========================================================================

/// `preprocess_limit(root, tuple_fraction, &offset_est, &count_est)`
/// (planner.c:2577).
fn preprocess_limit<'mcx>(
    mcx: Mcx<'mcx>,
    run: &mut PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    mut tuple_fraction: f64,
    offset_est: &mut i64,
    count_est: &mut i64,
) -> PgResult<f64> {
    // Assert(parse->limitCount || parse->limitOffset) (C:2585).

    // limitCount estimation (C:2591-2612).
    let limit_count_expr = expr_from_nodeptr(run, root, |q| q.limitCount.as_ref())?;
    if let Some(e) = limit_count_expr {
        let est = clauses::estimate_expression_value(mcx, e)?;
        if let Some(c) = est.as_const() {
            if c.constisnull {
                *count_est = 0; // LIMIT ALL
            } else {
                *count_est = datum_get_int64(c);
                if *count_est <= 0 {
                    *count_est = 1;
                }
            }
        } else {
            *count_est = -1; // can't estimate
        }
    } else {
        *count_est = 0; // not present
    }

    // limitOffset estimation (C:2614-2635).
    let limit_offset_expr = expr_from_nodeptr(run, root, |q| q.limitOffset.as_ref())?;
    if let Some(e) = limit_offset_expr {
        let est = clauses::estimate_expression_value(mcx, e)?;
        if let Some(c) = est.as_const() {
            if c.constisnull {
                *offset_est = 0;
            } else {
                *offset_est = datum_get_int64(c);
                if *offset_est < 0 {
                    *offset_est = 0;
                }
            }
        } else {
            *offset_est = -1;
        }
    } else {
        *offset_est = 0;
    }

    // Adjust tuple_fraction (C:2637-2742).
    if *count_est != 0 {
        let limit_fraction: f64;
        if *count_est < 0 || *offset_est < 0 {
            limit_fraction = 0.10;
        } else {
            limit_fraction = (*count_est as f64) + (*offset_est as f64);
        }

        if tuple_fraction >= 1.0 {
            if limit_fraction >= 1.0 {
                tuple_fraction = f64::min(tuple_fraction, limit_fraction);
            } else {
                // caller absolute, limit fractional; use caller's value.
            }
        } else if tuple_fraction > 0.0 {
            if limit_fraction >= 1.0 {
                tuple_fraction = limit_fraction;
            } else {
                tuple_fraction = f64::min(tuple_fraction, limit_fraction);
            }
        } else {
            tuple_fraction = limit_fraction;
        }
    } else if *offset_est != 0 && tuple_fraction > 0.0 {
        let limit_fraction: f64 = if *offset_est < 0 { 0.10 } else { *offset_est as f64 };

        if tuple_fraction >= 1.0 {
            if limit_fraction >= 1.0 {
                tuple_fraction += limit_fraction;
            } else {
                tuple_fraction = limit_fraction;
            }
        } else {
            if limit_fraction >= 1.0 {
                // caller fractional, limit absolute; use caller's value.
            } else {
                tuple_fraction += limit_fraction;
                if tuple_fraction >= 1.0 {
                    tuple_fraction = 0.0;
                }
            }
        }
    }

    Ok(tuple_fraction)
}

/// `limit_needed(parse)` (planner.c:2762).
pub fn limit_needed(parse: &Query) -> bool {
    // limitCount (C:2766-2777).
    if let Some(node) = parse.limitCount.as_deref() {
        match nodeptr_as_const_isnull_value(node) {
            Some((isnull, _)) => {
                if !isnull {
                    return true; // LIMIT with a constant value
                }
            }
            None => return true, // non-constant LIMIT
        }
    }

    // limitOffset (C:2779-2795).
    if let Some(node) = parse.limitOffset.as_deref() {
        match nodeptr_as_const_isnull_value(node) {
            Some((isnull, value)) => {
                if !isnull && value != 0 {
                    return true; // OFFSET with a nonzero value
                }
            }
            None => return true, // non-constant OFFSET
        }
    }

    false
}

// ===========================================================================
// preprocess_groupclause()  (planner.c:2828)
// ===========================================================================

/// `preprocess_groupclause(root, force)` (planner.c:2828).
///
/// Adjusts GROUP BY ordering to match ORDER BY. Returns a fresh list of
/// `SortGroupClause` handles (NodeIds). The `force` path (grouping sets) and the
/// `equal(gc, sc)` comparison need the SortGroupClause node values; the regular
/// SELECT path with no GROUP BY does not reach here.
fn preprocess_groupclause<'mcx>(
    run: &mut PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    force: &[types_core::Index],
) -> PgResult<Vec<pathnodes::NodeId>> {
    // Bridge parse->groupClause (a List* of SortGroupClause* carried as NodePtr)
    // into the planner node arena once, preserving element identity: each
    // original SortGroupClause is interned to a single stable NodeId so that the
    // C `list_member_ptr` (pointer identity) maps to NodeId equality here, and so
    // that the returned processed_groupClause elements are "the same" clauses as
    // parse->groupClause (the C contract — later processing may modify
    // processed_groupClause but not parse->groupClause).
    let group_clause_ids: Vec<pathnodes::NodeId> = {
        let group_clauses: Vec<::nodes::rawnodes::SortGroupClause> = run
            .resolve(root.parse)
            .groupClause
            .iter()
            .map(|np| sortgroupclause_from_node(np))
            .collect::<PgResult<Vec<_>>>()?;
        group_clauses
            .into_iter()
            .map(|sgc| root.alloc_sortgroupclause(sgc))
            .collect()
    };

    // For grouping sets, we need to force the ordering (C:2835). `force` is a
    // list of tleSortGroupRefs; for each, pick the matching groupClause element.
    if !force.is_empty() {
        let mut new_groupclause: Vec<pathnodes::NodeId> = Vec::new();
        for &ref_idx in force {
            // get_sortgroupref_clause(ref, parse->groupClause) — find the clause
            // with tleSortGroupRef == ref, reusing its interned NodeId.
            let id = *group_clause_ids
                .iter()
                .find(|&&id| root.sortgroupclause(id).tleSortGroupRef == ref_idx)
                .ok_or_else(|| {
                    PgError::error(
                        "preprocess_groupclause: ORDER/GROUP reference is not in the \
                         GROUP BY clause",
                    )
                })?;
            new_groupclause.push(id);
        }
        return Ok(new_groupclause);
    }

    // If no ORDER BY, nothing useful to do here (C:2848).
    let sort_clauses: Vec<::nodes::rawnodes::SortGroupClause> = run
        .resolve(root.parse)
        .sortClause
        .iter()
        .map(|np| sortgroupclause_from_node(np))
        .collect::<PgResult<Vec<_>>>()?;
    if sort_clauses.is_empty() {
        return Ok(group_clause_ids);
    }

    // Scan the ORDER BY clause and construct a list of matching GROUP BY items,
    // but only as far as we can make a matching prefix (C:2858). This assumes
    // sortClause contains no duplicate items.
    let mut new_groupclause: Vec<pathnodes::NodeId> = Vec::new();
    'sort: for sc in &sort_clauses {
        for &gid in &group_clause_ids {
            let gc = *root.sortgroupclause(gid);
            // equal(gc, sc) — SortGroupClause derives PartialEq.
            if gc == *sc {
                new_groupclause.push(gid);
                continue 'sort;
            }
        }
        // No match for this sort item, so stop scanning (C:2875: gl == NULL).
        break;
    }

    // If no match at all, no point in reordering GROUP BY (C:2880).
    if new_groupclause.is_empty() {
        return Ok(group_clause_ids);
    }

    // Add any remaining GROUP BY items to the new list (C:2891). Partial match
    // still allows ORDER BY via incremental sort. Give up if any non-sortable
    // GROUP BY item is found.
    for &gid in &group_clause_ids {
        if new_groupclause.contains(&gid) {
            continue; // it matched an ORDER BY item
        }
        if root.sortgroupclause(gid).sortop == 0 {
            // GROUP BY can't be sorted — give up on reordering.
            return Ok(group_clause_ids);
        }
        new_groupclause.push(gid);
    }

    debug_assert_eq!(group_clause_ids.len(), new_groupclause.len());
    Ok(new_groupclause)
}

// ===========================================================================
// expression_planner()  (planner.c:6779)
// ===========================================================================

/// `expression_planner(expr)` (planner.c:6779). Prepares an expression tree for
/// execution (used outside the main planner). `eval_const_expressions(NULL, expr)`
/// then `fix_opfuncids`.
pub fn expression_planner<'mcx>(mcx: Mcx<'mcx>, expr: Expr<'mcx>) -> PgResult<Expr<'mcx>> {
    // eval_const_expressions(NULL, expr) (C:6789). The owner takes an Mcx (no
    // PlannerInfo needed; the `NULL` root path).
    let mut result = clauses::eval_const_expressions(mcx, expr)?;

    // fix_opfuncids((Node *) result) (C:6791). Not a no-op: const-folding can
    // emit nodes with an unresolved opfuncid — e.g. `negate_clause` rewrites
    // `NOT (a = b)` to a fresh `a <> b` OpExpr with opfuncid = InvalidOid (the
    // DEFAULT-partition NOT-constraint reaches this) — so resolve any remaining
    // opfuncids over the whole tree, exactly as C does after eval_const_expressions.
    nodes_core::nodefuncs::fix_opfuncids(&mut result)?;

    Ok(result)
}

/// `expression_planner_with_deps(expr, &relationOids, &invalItems)`
/// (clauses.c:5479). Like [`expression_planner`] but also extracts the
/// relation-OID and function-inval-item dependencies of the const-folded
/// expression — the form `GetCachedExpression` (`plancache.c`) uses so the
/// cached expression is invalidated when its dependencies change. C makes up a
/// dummy `PlannerGlobal`/`PlannerInfo` and runs `extract_query_dependencies_walker`
/// over the planned result; we delegate the walk to setrefs'
/// [`extract_expr_dependencies_value`](setrefs::extract_expr_dependencies_value)
/// (the owner of the dependency-extraction machinery — no cycle, the planner
/// already depends on setrefs).
pub fn expression_planner_with_deps<'mcx>(
    mcx: Mcx<'mcx>,
    expr: Expr<'mcx>,
) -> PgResult<(Expr<'mcx>, alloc::vec::Vec<Oid>, alloc::vec::Vec<types_plancache::InvalItemKey>)> {
    // result = (Expr *) expression_planner((Expr *) expr); (const-fold + opfuncids)
    let result = expression_planner(mcx, expr)?;

    // (void) extract_query_dependencies_walker((Node *) result, &root);
    let deps = setrefs::extract_expr_dependencies_value(mcx, &result)?;

    // *relationOids = glob.relationOids; *invalItems = glob.invalItems;
    let inval_items = deps
        .inval_items
        .into_iter()
        .map(|(cache_id, hash_value)| types_plancache::InvalItemKey {
            cache_id,
            hash_value,
        })
        .collect();
    Ok((result, deps.relation_oids, inval_items))
}

// ===========================================================================
// plan_cluster_use_sort()  (planner.c:6859) — installed as a seam.
// ===========================================================================

/// `plan_cluster_use_sort(tableOid, indexOid)` (planner.c:6859). Decides whether
/// a seqscan+sort beats an indexscan for CLUSTER's table copy.
fn plan_cluster_use_sort_impl<'mcx>(
    mcx: Mcx<'mcx>,
    table_oid: Oid,
    index_oid: Oid,
) -> PgResult<bool> {
    use pathnodes::{ForwardScanDirection, JoinDomain};

    /* We can short-circuit the cost comparison if indexscans are disabled */
    if !costsize::ENABLE_INDEXSCAN() {
        return Ok(true); /* use sort */
    }

    /* Set up mostly-dummy planner state. */
    // query = makeNode(Query); query->commandType = CMD_SELECT;
    let mut query = Query::new(mcx);
    query.commandType = ::nodes::nodes::CmdType::CMD_SELECT;

    /* Build a minimal RTE for the rel */
    // rte = makeNode(RangeTblEntry); ...
    let mut rte = ::nodes::parsenodes::RangeTblEntry::new_in(mcx);
    rte.rtekind = ::nodes::parsenodes::RTEKind::RTE_RELATION;
    rte.relid = table_oid;
    rte.relkind = types_tuple::access::RELKIND_RELATION as i8; /* Don't be too picky. */
    rte.rellockmode = types_storage::lock::AccessShareLock;
    rte.lateral = false;
    rte.inh = false;
    rte.inFromCl = true;

    // query->rtable = list_make1(rte); addRTEPermissionInfo(&query->rteperminfos, rte);
    parser_relation::addRTEPermissionInfo(&mut query.rteperminfos, &mut rte)?;
    query.rtable.push(rte);

    // glob = makeNode(PlannerGlobal);
    let glob = PlannerGlobal::default();

    // The dummy planner run owns the interned Query/RTE arenas.
    let mut run = PlannerRun::new(mcx);
    let query_id = run.intern(query);

    // root = makeNode(PlannerInfo); + field init.
    let mut root = PlannerInfo::default();
    root.parse = query_id;
    root.glob = Some(Box::new(glob));
    root.query_level = 1;
    root.planner_cxt = None; /* C: CurrentMemoryContext — the value model uses `mcx`. */
    root.wt_param_id = -1;
    // root->join_domains = list_make1(makeNode(JoinDomain));
    root.join_domains = alloc::vec![JoinDomain::default()];

    /* Set up RTE/RelOptInfo arrays */
    relnode::setup_simple_rel_arrays(&mut run, &mut root, mcx)?;

    /* Build RelOptInfo (get_relation_info fills indexlist/tuples/pages) */
    let rel_id = relnode::build_simple_rel(&run, &mut root, 1, None)?;

    /* Locate IndexOptInfo for the target index */
    // foreach(lc, rel->indexlist) { if indexInfo->indexoid == indexOid break; }
    let index_info = {
        let rel = root.rel(rel_id);
        rel.indexlist
            .iter()
            .find(|ix| ix.indexoid == index_oid)
            .cloned()
    };

    /*
     * It's possible that get_relation_info did not generate an IndexOptInfo
     * for the desired index; this could happen if it's not yet reached its
     * indcheckxmin usability horizon, or if it's a system index and we're
     * ignoring system indexes.  In such cases we should tell CLUSTER to not
     * trust the index contents but use seqscan-and-sort.
     */
    let index_info = match index_info {
        Some(ix) => ix,
        None => return Ok(true), /* not in the list? -> use sort */
    };

    /*
     * Rather than doing all the pushups that would be needed to use
     * set_baserel_size_estimates, just do a quick hack for rows and width.
     */
    // rel->rows = rel->tuples; rel->reltarget->width = get_relation_data_width(tableOid, NULL);
    let data_width = plancat::get_relation_data_width(table_oid, &[], 1)?;
    let (rel_tuples, rel_pages) = {
        let rel = root.rel_mut(rel_id);
        rel.rows = rel.tuples;
        if let Some(rt) = rel.reltarget.as_mut() {
            rt.width = data_width;
        }
        (rel.tuples, rel.pages)
    };

    // root->total_table_pages = rel->pages;
    root.total_table_pages = rel_pages as f64;

    /*
     * Determine eval cost of the index expressions, if any.  We need to
     * charge twice that amount for each tuple comparison that happens during
     * the sort, since tuplesort.c will have to re-evaluate the index
     * expressions each time.  (XXX that's pretty inefficient...)
     */
    // cost_qual_eval(&indexExprCost, indexInfo->indexprs, root);
    let index_expr_cost =
        costsize::cost_qual_eval(&root, &index_info.indexprs);
    // comparisonCost = 2.0 * (indexExprCost.startup + indexExprCost.per_tuple);
    let comparison_cost = 2.0 * (index_expr_cost.startup + index_expr_cost.per_tuple);

    /* Estimate the cost of seq scan + sort */
    // seqScanPath = create_seqscan_path(root, rel, NULL, 0);
    let seq_scan_path =
        pathnode::create::create_seqscan_path(&mut root, &run, rel_id, &None, 0)?;
    // Read seqscan's cost/disabled_nodes before cost_sort (avoid overlapping borrows).
    let (seq_disabled, seq_total) = {
        let base = root.path(seq_scan_path).base();
        (base.disabled_nodes, base.total_cost)
    };

    // cost_sort(&seqScanAndSortPath, root, NIL, seqScanPath->disabled_nodes,
    //           seqScanPath->total_cost, rel->tuples, rel->reltarget->width,
    //           comparisonCost, maintenance_work_mem, -1.0);
    //
    // C `cost_sort`s into a stack-allocated `Path seqScanAndSortPath`. The
    // value-model `cost_sort` writes into a PathId in the arena, so allocate a
    // throwaway path node to receive the result. We reuse `create_seqscan_path`
    // to mint a fresh base path over the same rel; `cost_sort` overwrites the
    // rows/disabled_nodes/startup_cost/total_cost fields it cares about.
    let sort_path =
        pathnode::create::create_seqscan_path(&mut root, &run, rel_id, &None, 0)?;
    let maintenance_work_mem =
        init_small::globals::maintenance_work_mem();
    costsize::cost_sort(
        &mut root,
        sort_path,
        &[],
        seq_disabled,
        seq_total,
        rel_tuples,
        data_width,
        comparison_cost,
        maintenance_work_mem,
        -1.0,
    );
    let seq_scan_and_sort_total = root.path(sort_path).base().total_cost;

    /* Estimate the cost of index scan */
    // indexScanPath = create_index_path(root, indexInfo, NIL, NIL, NIL, NIL,
    //                                   ForwardScanDirection, false, NULL, 1.0, false);
    let index_scan_path = pathnode::create::create_index_path(
        &mut root,
        &run,
        Box::new(index_info),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        ForwardScanDirection,
        false,
        &None,
        1.0,
        false,
    )?;
    let index_scan_total = root.path(index_scan_path).base().total_cost;

    // return (seqScanAndSortPath.total_cost < indexScanPath->path.total_cost);
    Ok(seq_scan_and_sort_total < index_scan_total)
}

// ===========================================================================
// Small helpers
// ===========================================================================

/// Resolve a `Query` `Option<NodePtr>` field to an owned arena `Expr` for the
/// limit-estimation path. The limit expressions are simple scalar Exprs
/// (`Const` after const-folding), carried on the `Query` as `NodePtr<Node>`. We
/// extract the embedded `Expr` from the `Node`. If the `Node` is not an Expr-
/// bearing node this is a parser bug.
fn expr_from_nodeptr<'mcx, F>(
    run: &PlannerRun<'mcx>,
    root: &PlannerInfo,
    pick: F,
) -> PgResult<Option<Expr<'mcx>>>
where
    F: for<'a> Fn(&'a Query<'mcx>) -> Option<&'a mcx::PgBox<'mcx, Expr<'mcx>>>,
{
    let parse = run.resolve(root.parse);
    match pick(parse) {
        None => Ok(None),
        // `limitCount` / `limitOffset` are the concretely-typed
        // `Option<PgBox<Expr>>` view; the owned `Expr` is in hand directly.
        Some(e) => Ok(Some((**e).clone())),
    }
}

/// `((Const *) node)->constisnull` / `DatumGetInt64(constvalue)` over a limit/
/// offset `Expr` that may be a `Const`. Returns `Some((isnull, value))` if the
/// expression is a `Const`, else `None` (non-constant).
fn nodeptr_as_const_isnull_value(expr: &Expr<'_>) -> Option<(bool, i64)> {
    expr.as_const()
        .map(|c| (c.constisnull, datum_get_int64(c)))
}

/// `DatumGetInt64(((Const *) est)->constvalue)` — read an `int8` from a `Const`'s
/// by-value Datum.
fn datum_get_int64(c: &::nodes::primnodes::Const<'_>) -> i64 {
    if c.constisnull {
        0
    } else {
        c.constvalue.as_i64()
    }
}

/// Extract `wc->startOffset` (if `start`) or `wc->endOffset` of the `i`th
/// WindowClause from the `Query` as an owned `Option<Expr>` (the offset is a
/// `Node::Expr`). Leaves the field in place; the preprocessed value is written
/// back by [`set_windowclause_offset`].
fn extract_windowclause_offset<'mcx>(
    mcx: Mcx<'mcx>,
    run: &PlannerRun<'mcx>,
    root: &PlannerInfo,
    i: usize,
    start: bool,
) -> PgResult<Option<Expr<'mcx>>> {
    let wc_node = &*run.resolve(root.parse).windowClause[i];
    let wc = match wc_node.node_tag() {
        ntag::T_WindowClause => wc_node.expect_windowclause(),
        _ => panic!(
            "windowClause element is not a WindowClause (got {:?})",
            wc_node.tag()
        ),
    };
    let offset = if start { &wc.startOffset } else { &wc.endOffset };
    match offset.as_ref() {
        None => Ok(None),
        Some(np) => match np.as_expr() {
            Some(e) => Ok(Some(e.clone_in(mcx)?)),
            None => panic!(
                "WindowClause offset is not an Expr (got {:?})",
                np.tag()
            ),
        },
    }
}

/// Write `processed` back into `wc->startOffset`/`wc->endOffset` of the `i`th
/// WindowClause (wrapping the `Expr` in a `Node::Expr` NodePtr, or `None`).
fn set_windowclause_offset<'mcx>(
    mcx: Mcx<'mcx>,
    run: &mut PlannerRun<'mcx>,
    root: &PlannerInfo,
    i: usize,
    start: bool,
    processed: Option<Expr<'mcx>>,
) -> PgResult<()> {
    let wrapped = match processed {
        Some(e) => Some(mcx::alloc_in(mcx, Node::mk_expr(mcx, e)?)?),
        None => None,
    };
    let wc_node = &mut *run.resolve_mut(root.parse).windowClause[i];
    let wc = match wc_node.node_tag() {
        ntag::T_WindowClause => wc_node
            .as_windowclause_mut()
            .expect("node_tag() == T_WindowClause"),
        _ => panic!(
            "windowClause element is not a WindowClause (got {:?})",
            wc_node.tag()
        ),
    };
    if start {
        wc.startOffset = wrapped;
    } else {
        wc.endOffset = wrapped;
    }
    Ok(())
}

/// Which list field of the `OnConflictExpr` a preprocessing helper targets.
#[derive(Clone, Copy)]
enum OcList {
    ArbiterElems,
    OnConflictSet,
}

/// Which scalar (`Option<NodePtr>`) field of the `OnConflictExpr` a helper
/// targets.
#[derive(Clone, Copy)]
enum OcScalar {
    ArbiterWhere,
    OnConflictWhere,
}

/// Take the preprocessable `Expr` out of the `i`th element of an
/// `OnConflictExpr` list field. For `arbiterElems` the element is itself an
/// `Expr` (InferenceElem); for `onConflictSet` it is a `TargetEntry` and the
/// preprocessed value is its `.expr`.
fn take_onconflict_list_expr<'mcx>(
    run: &mut PlannerRun<'mcx>,
    root: &PlannerInfo,
    which: OcList,
    i: usize,
) -> Option<Expr<'mcx>> {
    let oc = run.resolve_mut(root.parse).onConflict.as_deref_mut()?;
    match which {
        OcList::ArbiterElems => match oc.arbiterElems[i].as_expr() {
            Some(e) => Some(e.clone()),
            None => panic!("arbiterElems element is not an Expr (got {:?})", oc.arbiterElems[i].tag()),
        },
        OcList::OnConflictSet => {
            let el = &mut oc.onConflictSet[i];
            let tag = el.tag();
            match el.as_targetentry_mut() {
                Some(te) => te.expr.take().map(mcx::PgBox::into_inner),
                None => panic!(
                    "onConflictSet element is not a TargetEntry (got {:?})",
                    tag
                ),
            }
        }
    }
}

/// Store the preprocessed `Expr` back into the `i`th element of an
/// `OnConflictExpr` list field.
fn set_onconflict_list_expr<'mcx>(
    mcx: Mcx<'mcx>,
    run: &mut PlannerRun<'mcx>,
    root: &PlannerInfo,
    which: OcList,
    i: usize,
    processed: Option<Expr<'mcx>>,
) -> PgResult<()> {
    let oc = match run.resolve_mut(root.parse).onConflict.as_deref_mut() {
        Some(oc) => oc,
        None => return Ok(()),
    };
    match which {
        OcList::ArbiterElems => {
            let e = processed.ok_or_else(|| {
                PgError::error("preprocess_expression reduced an arbiterElem to NULL")
            })?;
            *oc.arbiterElems[i] = Node::mk_expr(mcx, e)?;
        }
        OcList::OnConflictSet => {
            let el = &mut oc.onConflictSet[i];
            let tag = el.tag();
            match el.as_targetentry_mut() {
                Some(te) => {
                    te.expr = match processed {
                        Some(e) => Some(mcx::alloc_in(mcx, e)?),
                        None => None,
                    };
                }
                None => panic!(
                    "onConflictSet element is not a TargetEntry (got {:?})",
                    tag
                ),
            }
        }
    }
    Ok(())
}

/// Take the inner `Expr` out of a scalar (`Option<NodePtr>`) `OnConflictExpr`
/// field for preprocessing.
fn take_onconflict_scalar<'mcx>(
    run: &mut PlannerRun<'mcx>,
    root: &PlannerInfo,
    which: OcScalar,
) -> Option<Expr<'mcx>> {
    let oc = run.resolve_mut(root.parse).onConflict.as_deref_mut()?;
    let slot = match which {
        OcScalar::ArbiterWhere => &mut oc.arbiterWhere,
        OcScalar::OnConflictWhere => &mut oc.onConflictWhere,
    };
    slot.take().map(|np| {
        let node = mcx::PgBox::into_inner(np);
        let tag = node.tag();
        match node.into_expr() {
            Some(e) => e,
            None => panic!(
                "OnConflict scalar clause is not an Expr (got {:?})",
                tag
            ),
        }
    })
}

/// Store the preprocessed `Expr` back into a scalar `OnConflictExpr` field.
fn set_onconflict_scalar<'mcx>(
    mcx: Mcx<'mcx>,
    run: &mut PlannerRun<'mcx>,
    root: &PlannerInfo,
    which: OcScalar,
    processed: Option<Expr<'mcx>>,
) -> PgResult<()> {
    let oc = match run.resolve_mut(root.parse).onConflict.as_deref_mut() {
        Some(oc) => oc,
        None => return Ok(()),
    };
    let wrapped = match processed {
        Some(e) => Some(mcx::alloc_in(mcx, Node::mk_expr(mcx, e)?)?),
        None => None,
    };
    match which {
        OcScalar::ArbiterWhere => oc.arbiterWhere = wrapped,
        OcScalar::OnConflictWhere => oc.onConflictWhere = wrapped,
    }
    Ok(())
}

// ===========================================================================
// Seam installation
// ===========================================================================

/// Install this crate's inward seams: `pg_plan_query` (the planner entry the
/// COPY-TO driver / tcop call) and `plan_cluster_use_sort` (CLUSTER's
/// seqscan-vs-indexscan decision).
/// `apply_tlist_labeling(plan->targetlist, root->processed_tlist)` — the
/// create_plan-tail invocation. Copies the labeling attributes
/// (`resname`/`ressortgroupref`/`resorigtbl`/`resorigcol`/`resjunk`) of the
/// query's processed targetlist onto the topmost plan node's targetlist,
/// element-wise (tlist.c:327). The source `processed_tlist` is a list of arena
/// `TargetEntryNode`s; the dest is the plan's owned `TargetEntry<'mcx>` list.
fn apply_tlist_labeling_impl<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    plan: &mut Node<'mcx>,
) -> types_error::PgResult<()> {
    // Snapshot the (label, resno) tuples from the arena source tlist first, so
    // the immutable `root` borrow is released before we mutably borrow `plan`.
    let src_labels: alloc::vec::Vec<_> = root
        .processed_tlist
        .iter()
        .map(|id| {
            let te = root.targetentry(*id);
            (
                te.resno,
                te.resname.clone(),
                te.ressortgroupref,
                te.resorigtbl,
                te.resorigcol,
                te.resjunk,
            )
        })
        .collect();

    // Apply the labeling to the topmost plan node, then propagate it down
    // through tlist-passthrough (non-projecting) plan nodes.
    //
    // In C, pass-through plan nodes (Sort, IncrementalSort, Unique, Material,
    // Memoize, LockRows, Limit, Hash, Gather, GatherMerge) reuse their child's
    // `targetlist` *List pointer*, so the whole chain shares the same
    // TargetEntry objects down to the bottom-most node that builds its own
    // tlist (e.g. a SubqueryScan or table scan). `apply_tlist_labeling`'s single
    // in-place mutation in C therefore labels every node in that shared chain.
    // Our owned model gives each plan node its own TargetEntry copies, so we
    // must walk the chain and apply the labeling to each node explicitly.
    // Notably `trivial_subqueryscan` (setrefs.c) compares the SubqueryScan
    // tlist's `resjunk` against its subplan's; without this propagation the
    // SubqueryScan keeps `resjunk=false` on junk sort columns and is wrongly
    // judged trivial and removed.
    let mut cur: &mut Node<'mcx> = plan;
    loop {
        {
            let dest = match cur.plan_head_mut().targetlist.as_deref_mut() {
                Some(d) => d,
                None => {
                    // An empty dest is only valid when the source is also empty.
                    assert!(
                        src_labels.is_empty(),
                        "apply_tlist_labeling: dest tlist is NIL but processed_tlist is not"
                    );
                    return Ok(());
                }
            };
            // If lengths differ, the child projects to a different tlist (this
            // node does not share its parent's TLEs); stop the descent.
            if dest.len() != src_labels.len() {
                break;
            }
            for (dest_tle, (resno, resname, ressortgroupref, resorigtbl, resorigcol, resjunk)) in
                dest.iter_mut().zip(src_labels.iter())
            {
                debug_assert_eq!(dest_tle.resno, *resno);
                dest_tle.resname = match resname {
                    Some(s) => Some(mcx::PgString::from_str_in(s.as_str(), mcx)?),
                    None => None,
                };
                dest_tle.ressortgroupref = *ressortgroupref;
                dest_tle.resorigtbl = *resorigtbl;
                dest_tle.resorigcol = *resorigcol;
                dest_tle.resjunk = *resjunk;
            }
        }

        // Descend into the lefttree only through tlist-passthrough node types,
        // mirroring the createplan.c make_* functions that set
        // `plan->targetlist = lefttree->targetlist`.
        let is_passthrough = matches!(
            cur.node_tag(),
            ::nodes::nodes::T_Sort
                | ::nodes::nodeincrementalsort::T_IncrementalSort
                | ::nodes::nodeunique::T_Unique
                | ::nodes::nodes::T_Material
                | ::nodes::nodememoize::T_Memoize
                | ::nodes::nodes::T_LockRows
                | ::nodes::nodes::T_Limit
                | ::nodes::nodehashjoin::T_Hash
                | ::nodes::nodegather::T_Gather
                | ::nodes::nodegathermerge::T_GatherMerge
        );
        if !is_passthrough {
            break;
        }
        match cur.plan_head_mut().lefttree.as_deref_mut() {
            Some(child) => cur = child,
            None => break,
        }
    }
    Ok(())
}

/// `record_inval_item` impl (the `makeNode(PlanInvalItem)` + `GetSysCacheHashValue1`
/// + `lappend` tail of `record_plan_function_dependency`/`record_plan_type_dependency`,
/// setrefs.c:3553/3593). Computes the syscache hash for `(cache_id, oid)` and pushes
/// the concrete `PlanInvalItem` onto `glob->invalItems`.
fn record_inval_item_impl(
    inval_items: &mut Vec<::nodes::nodeindexscan::PlanInvalItem>,
    cache_id: i32,
    oid: Oid,
) -> types_error::PgResult<()> {
    // inval_item->hashValue = GetSysCacheHashValue1(cacheId, ObjectIdGetDatum(oid));
    let hash_value =
        syscache_seams::get_syscache_hash_value_oid::call(cache_id, oid)?;
    inval_items.push(::nodes::nodeindexscan::PlanInvalItem {
        cacheId: cache_id,
        hashValue: hash_value,
    });
    Ok(())
}

/// `mark_partial_aggref(agg, aggsplit)` (planner.c:5743). Adjust an `Aggref` to
/// represent a partial-aggregation step, in place. Owned by planner.c; routed
/// through the setrefs seam for `convert_combining_aggrefs`.
fn mark_partial_aggref_impl(
    agg: &mut ::nodes::primnodes::Aggref,
    aggsplit: ::nodes::nodeagg::AggSplit,
) -> types_error::PgResult<()> {
    use ::nodes::nodeagg::{do_aggsplit_serialize, do_aggsplit_skipfinal};

    // aggtranstype should be computed by this point; aggsplit should still be
    // AGGSPLIT_SIMPLE as the parser left it.
    debug_assert!(agg.aggtranstype != 0);
    debug_assert_eq!(agg.aggsplit, ::nodes::nodeagg::AGGSPLIT_SIMPLE);

    // Mark the Aggref with the intended partial-aggregation mode.
    agg.aggsplit = aggsplit;

    // Adjust result type if needed. Normally a partial aggregate returns the
    // aggregate's transition type; but if that's INTERNAL and we're serializing,
    // it returns BYTEA instead.
    const INTERNALOID: Oid = 2281;
    const BYTEAOID: Oid = 17;
    if do_aggsplit_skipfinal(aggsplit) {
        if agg.aggtranstype == INTERNALOID && do_aggsplit_serialize(aggsplit) {
            agg.aggtype = BYTEAOID;
        } else {
            agg.aggtype = agg.aggtranstype;
        }
    }
    Ok(())
}

/// `plan_create_index_workers(tableOid, indexOid)` (planner.c) — decide how
/// many parallel workers a CREATE INDEX should use.
///
/// The standalone / parallelism-disabled gate is the C's exact early return.
///
/// The parallel-build estimation tail (dummy-`PlannerInfo` assembly +
/// `build_simple_rel` + `estimate_rel_size` + `compute_parallel_worker` +
/// `maintenance_work_mem` cap) would compute a candidate worker count — but the
/// entire parallel CREATE INDEX *launch* substrate it feeds is unported: the
/// AM-side coordination (`_bt_begin_parallel` / `_bt_end_parallel` / the
/// `ParallelContext` + DSM segment + parallel table scan) all loudly panic
/// (see `backend-access-nbtree-nbtsort::deferred`). With no way to launch
/// background workers, the only correct worker count is 0 — a serial build.
///
/// This mirrors C's own behavior when the parallel infrastructure is
/// unavailable: `compute_parallel_worker()` returns 0 (and `_bt_begin_parallel`
/// falls back to a serial build) whenever no parallel workers can be obtained.
/// Returning 0 here is therefore faithful, not a stub: it is the exact value C
/// arrives at on a system where parallel maintenance workers cannot run. When
/// the parallel-build launch substrate lands, this is replaced by the full
/// estimation tail; until then 0 is the complete, correct answer for every mode.
fn plan_create_index_workers(table_oid: Oid, index_oid: Oid) -> PgResult<i32> {
    use guc_tables::vars;

    // We don't allow performing parallel operation in standalone backend or
    // when parallelism is disabled.
    if !init_small::globals::IsUnderPostmaster()
        || vars::max_parallel_maintenance_workers.read() == 0
    {
        return Ok(0);
    }

    // Parallel maintenance-worker launch is unported (see module doc above):
    // the build-side coordination panics, so no workers can be obtained. Mirror
    // C's "no parallel workers available" outcome and build serially.
    let _ = (table_oid, index_oid);
    Ok(0)
}

pub fn init_seams() {
    use guc_tables::vars;
    use guc_tables::GucVarAccessors;

    index_seams::plan_create_index_workers::set(plan_create_index_workers);

    planner_seams::pg_plan_query::set(pg_plan_query_impl);
    planner_seams::pg_plan_query_params::set(pg_plan_query_params_impl);
    planner_seams::plan_cluster_use_sort::set(plan_cluster_use_sort_impl);
    planner_seams::select_rowmark_type::set(|rte, strength| {
        select_rowmark_type(rte, strength)
    });

    // Window-clause cost seams (costsize.c's cost_windowagg reads them; the
    // planner owns the arena WindowClause + the parse Query they need).
    costsize_seams::windowfunc_cost::set(windowfunc_cost_impl);
    costsize_seams::windowclause_cost_info::set(windowclause_cost_info_impl);
    planner_seams::subquery_planner_for_setop::set(
        subquery_planner_for_setop_impl,
    );
    planner_seams::subquery_planner_for_fromsubquery::set(
        subquery_planner_for_fromsubquery_impl,
    );
    // The VALUE seams carry the parser-arena `'static` Expr across the typcache /
    // execExpr serialization boundary (callers erase-in / clone-out); the public
    // `expression_planner[_with_deps]` thread `'mcx` for in-planner direct callers,
    // so bridge at the seam with a `clone_in(mcx)` round-trip (Expr is invariant).
    planner_pc_seams::expression_planner_with_deps_value::set(
        |mcx, expr| {
            let (planned, oids, items) = expression_planner_with_deps(mcx, expr.clone_in(mcx)?)?;
            Ok((planned.erase_lifetime(), oids, items))
        },
    );
    planner_pc_seams::expression_planner_value::set(|mcx, expr| {
        Ok(expression_planner(mcx, expr.clone_in(mcx)?)?.erase_lifetime())
    });

    // `preprocess_phv_expression(root, expr)` = `preprocess_expression(root, expr,
    // EXPRKIND_PHV)` (planner.c) — consumed by `extract_lateral_references`
    // (initsplan.c, in init-subselect) for upper-level LATERAL PlaceHolderVars.
    // The planner owns `preprocess_expression`; a non-NULL input always yields a
    // non-NULL result (the only NULL fall-out is the empty-input fast path).
    init_subselect_ext_seams::preprocess_phv_expression::set(
        |root, run, expr| {
            // C: preprocess_phv_expression(root, expr) = preprocess_expression(root,
            // expr, EXPRKIND_PHV), and preprocess_expression reads root->parse for
            // flatten_join_alias_vars when root->hasJoinRTEs. Thread the outer Query
            // (root->parse, cloned into the run arena as the immutable context node)
            // exactly as preprocess_query_expressions does, so upper-level LATERAL
            // PlaceHolderVars over a query with join RTEs flatten their join-alias
            // Vars correctly instead of erroring.
            let mcx = run.mcx();
            let outer_query: Option<Node<'_>> = if root.hasJoinRTEs {
                Some(Node::mk_query(mcx, run.resolve(root.parse).clone_in(mcx)?)?)
            } else {
                None
            };
            let outer_query_ref = outer_query.as_ref();
            Ok(
                preprocess_expression(mcx, root, run, outer_query_ref, Some(expr), EXPRKIND_PHV)?
                    .expect("preprocess_expression of a non-NULL PHV expr is non-NULL"),
            )
        },
    );

    // `plan_sublink_subquery` = the lower-planner recursion (planner.c
    // subquery_planner -> fetch_upper_rel(UPPERREL_FINAL) ->
    // get_cheapest_fractional_path -> create_plan) consumed by make_subplan /
    // build_subplan / SS_process_ctes (init-subselect) to turn a SubLink's /
    // CTE's owned sub-Query into a finished (subroot, plan, path) triple. The C
    // glob is shared by pointer across all planning levels; in the owned model
    // glob lives on the parent root, so we move it down into the subroot for the
    // duration of the recursion (all paramExecTypes / subplans accumulated by the
    // sub-planning land in the one glob) and move it back to the parent on the
    // way out, exactly mirroring the single shared `PlannerGlobal`.
    init_subselect_ext_seams::plan_sublink_subquery::set(
        |root, run, subquery, has_recursion, tuple_fraction| {
            let mcx = run.mcx();

            // Intern the owned sub-Query so the run can resolve its targetList.
            let subquery_id = run.intern(subquery);

            // Move the shared glob from the parent root into the recursion, then
            // move the parent root itself out by value so it can become the
            // subroot's `parent_root` (C passes `root` as the parent_root arg).
            // We restore both back onto `*root` after the subquery is planned.
            let glob = *root
                .glob
                .take()
                .expect("plan_sublink_subquery: parent root->glob is NULL");
            let parent_root = core::mem::take(root);

            // subroot = subquery_planner(glob, subquery, root, hasRecursion,
            //                            tuple_fraction, NULL) (subselect.c:221).
            let mut subroot = subquery_planner(
                mcx,
                run,
                glob,
                subquery_id,
                Some(parent_root),
                has_recursion,
                tuple_fraction,
                None, // setops
            )?;

            // C keeps the parent_root pointer live on `subroot` throughout
            // path-building AND create_plan: a CTE reference in the subquery
            // carries `ctelevelsup > 0` and `create_ctescan_plan` /
            // `create_worktablescan_plan` (resolve_cte_subplan / set_cte_pathlist)
            // walk `subroot->parent_root` to reach the level that defines the CTE.
            // In this owned model `parent_root` is the moved-in parent value, so
            // we must NOT take it back out until create_plan has finished — else a
            // CTE referenced from inside a SubLink subquery (e.g. a MERGE WHEN
            // action's correlated `(SELECT ... FROM cte)`) fails with
            // "bad levelsup for CTE". The restore is deferred to after create_plan.

            // final_rel = fetch_upper_rel(subroot, UPPERREL_FINAL, NULL);
            // best_path = get_cheapest_fractional_path(final_rel, tuple_fraction);
            let final_rel = relnode::fetch_upper_rel(
                &mut subroot,
                UPPERREL_FINAL,
                &None,
            );
            let best_path =
                allpaths::seams::get_cheapest_fractional_path::call(
                    &subroot,
                    final_rel,
                    tuple_fraction,
                );

            // plan = create_plan(subroot, best_path) (subselect.c:235).
            let plan =
                createplan::create_plan(mcx, &mut subroot, run, best_path)?;

            // Now recover the parent root (taken in by value, mutated for any
            // upper-Var plan_params) and restore it onto `*root`, then move the
            // (now-accumulated) glob back to the parent root so build_subplan /
            // generate_new_exec_param see one shared glob.
            let glob = subroot.glob.take();
            *root = *subroot
                .parent_root
                .take()
                .expect("plan_sublink_subquery: subroot lost its parent_root");
            root.glob = glob;

            Ok(
                init_subselect_ext_seams::SublinkPlanResult {
                    subroot,
                    plan,
                    subpath: Some(best_path),
                    subquery_id,
                },
            )
        },
    );

    // create_plan-tail: apply_tlist_labeling(plan->targetlist,
    // root->processed_tlist) (createplan.c create_plan, tlist.c:327). The
    // generic two-tlist label-copy leaf lives in the tlist unit
    // (vars::tlist::apply_tlist_labeling); this seam is
    // the createplan-tail invocation, which the planner owns because the source
    // tlist is `root->processed_tlist` (a Vec<NodeId> of arena TargetEntrys).
    createplan_seams::apply_tlist_labeling::set(apply_tlist_labeling_impl);

    // record_inval_item: record_plan_function_dependency / record_plan_type_dependency
    // append a `PlanInvalItem{cacheId, hashValue=GetSysCacheHashValue1(cacheId, oid)}`
    // to `glob->invalItems`. The syscache hash lives with the syscache subsystem,
    // so the planner installs this owner seam to compute the hash + push the
    // concrete pair (then read straight into PlannedStmt.invalItems).
    setrefs_seams::record_inval_item::set(record_inval_item_impl);

    // mark_partial_aggref (planner.c:5743) — adjust an Aggref to a partial-
    // aggregation step. planner.c owns it; setrefs's convert_combining_aggrefs
    // routes through this seam.
    setrefs_seams::mark_partial_aggref::set(mark_partial_aggref_impl);

    // GUC `conf->variable` accessors for the three planner GUCs whose backing
    // globals live in planner.c / clauses.c. Per PG 18.3 guc_tables.c all three
    // are plain GUC slot variables (read directly from `conf->variable`, never
    // from the ControlFile), so the GUC engine reads/writes them through the
    // process-private cells installed here.
    vars::cursor_tuple_fraction.install(GucVarAccessors {
        get: get_cursor_tuple_fraction,
        set: set_cursor_tuple_fraction,
    });
    vars::enable_distinct_reordering.install(GucVarAccessors {
        get: get_enable_distinct_reordering,
        set: set_enable_distinct_reordering,
    });
    vars::parallel_leader_participation.install(GucVarAccessors {
        get: get_parallel_leader_participation,
        set: set_parallel_leader_participation,
    });
    // planner.c owns `int debug_parallel_query`.
    vars::debug_parallel_query.install(GucVarAccessors {
        get: get_debug_parallel_query,
        set: set_debug_parallel_query,
    });

    // set_relation_partition_info / set_baserel_partition_constraint (plancat.c):
    // the partitioning ext-seams whose bodies live in this crate (substrate-cycle
    // workaround; see partition_info.rs).
    partition_info::init_seams();
}

mod partition_info;

#[cfg(test)]
mod tests;
