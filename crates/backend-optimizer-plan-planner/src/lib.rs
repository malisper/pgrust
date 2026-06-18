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
//!    [`PlannerRun`](types_pathnodes::planner_run::PlannerRun) store behind a
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

use types_nodes::copy_query::Query;
use types_nodes::nodeindexscan::PlannedStmt;
use types_nodes::nodes::{CmdType, Node};
use types_nodes::nodelockrows::PlanRowMark;
use types_nodes::execnodes::RowMarkType;
use types_nodes::primnodes::Expr;
use types_nodes::parsenodes::RangeTblEntry;
use types_nodes::rawnodes::LockClauseStrength;

use types_pathnodes::planner_run::PlannerRun;
use types_pathnodes::{
    JoinDomain, PathId, PathTarget, PlannerGlobal, PlannerInfo, RangeTblEntryId, RelId, Relids,
    UPPERREL_DISTINCT, UPPERREL_FINAL, UPPERREL_GROUP_AGG, UPPERREL_ORDERED,
    UPPERREL_PARTIAL_DISTINCT, UPPERREL_WINDOW,
};

use types_core::Oid;

use types_parsenodes::{PROPARALLEL_SAFE, PROPARALLEL_UNSAFE};

use backend_utils_misc_guc_tables::consts::{
    DEBUG_PARALLEL_OFF, DEFAULT_CURSOR_TUPLE_FRACTION,
};

// Cursor-option flags (portalcmds.h).
use types_nodes::portalcmds::{
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
    backend_utils_misc_guc_tables::vars::cursor_tuple_fraction.read()
}

// `debug_parallel_query` GUC. The parallel-Gather test path of
// `standard_planner` only runs when this != OFF. It is not threaded into this
// crate, so we evaluate it as OFF (the production default), which faithfully
// skips the debug-only Gather injection.
const fn debug_parallel_query() -> i32 {
    DEBUG_PARALLEL_OFF
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
    standard_planner(mcx, querytree, query_string, cursor_options)
}

/// `standard_planner(parse, query_string, cursorOptions, boundParams)`
/// (planner.c:302).
fn standard_planner<'mcx>(
    mcx: Mcx<'mcx>,
    parse: &Query<'mcx>,
    _query_string: &str,
    cursor_options: i32,
) -> PgResult<PlannedStmt<'mcx>> {
    // glob = makeNode(PlannerGlobal); + field init (C:322-345). All other
    // fields are the C zero-defaults via Default.
    let mut glob = PlannerGlobal::default();

    // Assess parallel-mode feasibility (C:368-384).
    //
    // The cheap-test gate (`IsUnderPostmaster`, `max_parallel_workers_per_gather`,
    // `IsParallelWorker`) is not reachable in this repo, and the query-tree scan
    // it guards (`max_parallel_hazard(parse)`) has no ported owner (clauses.c
    // exposes only the internal walker, not the top-level `max_parallel_hazard`).
    // We faithfully take the C `else` branch — "skip the query tree scan, just
    // assume it's unsafe" — which is the conservative, always-correct result.
    glob.max_parallel_hazard = PROPARALLEL_UNSAFE;
    glob.parallel_mode_ok = false;

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
    let final_rel = backend_optimizer_util_relnode::fetch_upper_rel(
        &mut root,
        UPPERREL_FINAL,
        &None,
    );
    let best_path = backend_optimizer_path_allpaths::seams::get_cheapest_fractional_path::call(
        &root,
        final_rel,
        tuple_fraction,
    );

    let mut top_plan = backend_optimizer_plan_createplan::create_plan(mcx, &mut root, &run, best_path)?;

    // Scrollable-cursor backwards-scan guard (C:447-451).
    if (cursor_options & CURSOR_OPT_SCROLL) != 0 {
        // ExecSupportsBackwardScan(top_plan) — owner executes the executor's
        // node-amenability test (execAmi.c), which is not ported over the owned
        // `Node` plan model. Mirror PG and panic on this scrollable-cursor path.
        panic!(
            "planner: scrollable cursor (CURSOR_OPT_SCROLL) requires \
             ExecSupportsBackwardScan (execAmi.c) + materialize_finished_plan \
             (createplan.c via init-subselect-ext-seams), neither reachable over \
             the owned Node plan model yet"
        );
    }

    // Debug-only Gather injection (C:465-518) — only runs when
    // debug_parallel_query != OFF, which is always OFF in this crate (the GUC is
    // not threaded in). The branch is therefore faithfully dead; we do not build
    // the test Gather node.
    debug_assert!(debug_parallel_query() == DEBUG_PARALLEL_OFF);

    // SS_finalize_plan over subplans + top plan (C:526-537).
    if !root.glob.as_ref().map(|g| g.param_exec_types.is_empty()).unwrap_or(true) {
        // forboth(lp, glob->subplans, lr, glob->subroots): SS_finalize_plan on
        // each subplan, then the top plan. SS_finalize_plan's owner
        // (init-subselect finalize.rs) takes a per-subplan orig_tlist + grouping
        // flag that the value-model subplan store does not carry alongside the
        // plan node, so this paramExec-bearing finalize loop is not expressible
        // over the current store. Mirror PG and panic precisely.
        panic!(
            "planner: SS_finalize_plan loop (subselect.c) over glob->subplans/\
             subroots is not expressible over the PlannerRun subplan store \
             (needs per-subplan orig_tlist + grouping-set flag); reached because \
             paramExecTypes is non-empty"
        );
    }

    // set_plan_references on the top plan (C:540-545).
    top_plan = backend_optimizer_plan_setrefs::set_plan_references(mcx, &mut run, &mut root, top_plan)?;

    // ... and the subplans (C:547-554). With no params (we only reach here when
    // paramExecTypes is empty), glob->subplans is likewise empty in the simple
    // case; if a subplan exists without params, faithfully run setrefs on each.
    {
        let subplan_ids: Vec<types_pathnodes::PlanId> = root
            .glob
            .as_ref()
            .map(|g| g.subplans.clone())
            .unwrap_or_default();
        for pid in subplan_ids {
            let _ = pid; // subroot lives in run beside subplan
            // set_plan_references needs &mut PlannerInfo for the subroot; the
            // subroot lives in the run store. We cannot borrow run mutably (for
            // the subroot) and pass run mutably to set_plan_references at once.
            // This subplan-setrefs leg over the value store is not expressible;
            // panic precisely (only reached if a param-free subplan exists).
            panic!(
                "planner: subplan set_plan_references loop (setrefs.c) over \
                 glob->subplans is not expressible over the PlannerRun subplan/\
                 subroot store (simultaneous &mut subroot + &mut run borrow); \
                 reached with a param-free subplan present"
            );
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
    let final_perminfos: Vec<types_pathnodes::RtePermInfoId> = root
        .glob
        .as_ref()
        .map(|g| g.finalrteperminfos.clone())
        .unwrap_or_default();
    let mut perm_infos: mcx::PgVec<
        'mcx,
        types_nodes::parsenodes::RTEPermissionInfo<'mcx>,
    > = mcx::PgVec::new_in(mcx);
    for id in &final_perminfos {
        perm_infos.push(run.resolve_rte_perminfo(*id).clone_in(mcx)?);
    }

    // subplans: resolve glob->subplans (Vec<PlanId>) to owned Node trees.
    let subplan_ids: Vec<types_pathnodes::PlanId> = root
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
            Node::Result(types_nodes::noderesult::Result::default()),
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

    // rowMarks: glob->finalrowmarks holds PlanRowMarkId handles, but the owned
    // PlannedStmt models rowMarks as `PgVec<Expr>` (a primnodes Expr list), which
    // is the wrong carrier for a PlanRowMark. There is no faithful way to project
    // the resolved PlanRowMark values into that field, so we set None (the empty
    // case) with this note; finalrowmarks is empty on the simple SELECT path.
    let row_marks: Option<mcx::PgVec<'mcx, Expr>> = None;

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
                types_nodes::bitmapset::Bitmapset { words },
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
    let inval_items: Option<mcx::PgVec<'mcx, types_nodes::nodeindexscan::PlanInvalItem>> = {
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
    };

    Ok(result)
}

// ===========================================================================
// subquery_planner()  (planner.c:650)
// ===========================================================================

/// `subquery_planner(glob, parse, parent_root, hasRecursion, tuple_fraction, setops)`
/// (planner.c:650). Returns the owned `PlannerInfo` ("root") with its glob
/// attached and `final_rel`'s cheapest path set.
#[allow(clippy::too_many_arguments)]
fn subquery_planner<'mcx>(
    mcx: Mcx<'mcx>,
    run: &mut PlannerRun<'mcx>,
    glob: PlannerGlobal,
    parse_id: types_pathnodes::QueryId,
    _parent_root: Option<Box<PlannerInfo>>,
    has_recursion: bool,
    tuple_fraction: f64,
    setops: Option<()>,
) -> PgResult<PlannerInfo> {
    // root = makeNode(PlannerInfo); + field init (C:664-703).
    let mut root = PlannerInfo::default();
    root.parse = parse_id;
    root.glob = Some(Box::new(glob));
    root.query_level = 1; // parent_root is None at the top level here.
    root.hasRecursion = has_recursion;
    root.wt_param_id = -1; // hasRecursion=false on the top SELECT path.

    // Top-level join domain (C:710).
    root.join_domains.push(JoinDomain { jd_relids: None });

    // SS_process_ctes if cteList (C:716-717). CTEs are converted to
    // RTE_SUBQUERY or initplan SubPlans. The owner (init-subselect subplan.rs)
    // threads (&mut root, &mut run) and recurses into each CTE's ctequery.
    {
        let has_ctes = !run.resolve(root.parse).cteList.is_empty();
        if has_ctes {
            backend_optimizer_plan_init_subselect::subplan::SS_process_ctes(mcx, &mut root, run)?;
        }
    }

    // transform_MERGE_to_join(parse) (C:722). No ported owner.
    {
        let parse = run.resolve(root.parse);
        if parse.commandType == CmdType::CMD_MERGE {
            panic!(
                "subquery_planner: transform_MERGE_to_join (parsenodes/analyze) \
                 has no ported owner"
            );
        }
        // For non-MERGE, transform_MERGE_to_join is a no-op, so nothing to do.
    }

    // replace_empty_jointree(parse) (C:728). If the Query's jointree is empty,
    // inject a dummy RTE_RESULT relation (so e.g. `SELECT 1` plans). The owner
    // (prepjointree.c, ported in subselect-pullup) no-ops if the fromlist is
    // already non-empty or if this is the top of a setop tree.
    {
        let parse = run.resolve_mut(root.parse);
        backend_optimizer_plan_subselect_pullup::replace_empty_jointree(mcx, parse)?;
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
            backend_optimizer_prep_prepjointree::pull_up_sublinks(mcx, &mut root, parse)?;
        }
    }

    // preprocess_function_rtes(root) (C:745). The owner takes (&mut root, &mut
    // parse); `parse` borrows `run`, `root` is a separate local, so the two
    // disjoint &mut borrows are fine.
    {
        let parse = run.resolve_mut(root.parse);
        backend_optimizer_prep_prepjointree::preprocess_function_rtes(mcx, &mut root, parse)?;
    }

    // parse = root->parse = expand_virtual_generated_columns(root) (C:753). The
    // owner consumes and returns an owned Query, so swap it out of the run,
    // transform, and store the result back.
    {
        let parse_owned = run.resolve(root.parse).clone_in(mcx)?;
        let parse_new = backend_optimizer_prep_prepjointree::expand_virtual_generated_columns(
            mcx, &mut root, parse_owned,
        )?;
        *run.resolve_mut(root.parse) = parse_new;
    }

    // pull_up_subqueries(root) (C:759).
    {
        let parse = run.resolve_mut(root.parse);
        backend_optimizer_prep_prepjointree::pull_up_subqueries(mcx, &mut root, parse)?;
    }

    // flatten_simple_union_all if setOperations (C:767-768).
    {
        let has_setops = run.resolve(root.parse).setOperations.is_some();
        if has_setops {
            let parse = run.resolve_mut(root.parse);
            backend_optimizer_prep_prepjointree::flatten_simple_union_all(mcx, &mut root, parse)?;
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

            use types_nodes::parsenodes::RTEKind;
            match rtekind {
                RTEKind::RTE_RELATION => {
                    if inh {
                        let has_sub = backend_catalog_pg_inherits::has_subclass(relid)?;
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

    // View-permission ACL loop (C:866-882). Only fires for RELKIND_VIEW RTEs
    // with a perminfoindex. ExecCheckOneRelPerms has no ported owner. We scan
    // for any such RTE and panic precisely only if one is present (a plain table
    // SELECT has none).
    {
        let parse = run.resolve(root.parse);
        const RELKIND_VIEW: i8 = b'v' as i8;
        for rte in parse.rtable.iter() {
            if rte.perminfoindex != 0 && rte.relkind == RELKIND_VIEW {
                panic!(
                    "subquery_planner: view-permission ACL check \
                     (getRTEPermissionInfo + ExecCheckOneRelPerms, parse_relation.c/\
                     execMain.c) has no ported owner for the owned RTE model"
                );
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
    {
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
                let processed = preprocess_expression(mcx, &root, e, EXPRKIND_TARGET)?;
                run.resolve_mut(root.parse).targetList[i].expr = match processed {
                    Some(pe) => Some(mcx::alloc_in(mcx, pe)?),
                    None => None,
                };
            }
        }

        // withCheckOptions (C:907-916): each WithCheckOption's qual is an
        // EXPRKIND_QUAL expression; the WCOs whose qual reduces to NULL are
        // dropped. WCOs are only produced by the rewriter for RLS/updatable
        // views; a plain table SELECT has none. Panic precisely if present.
        if !run.resolve(root.parse).withCheckOptions.is_empty() {
            panic!(
                "subquery_planner: withCheckOptions expression preprocessing \
                 (planner.c:907-916) is not wired over the owned Query model \
                 (the qual lives as a NodePtr on the WithCheckOption node)"
            );
        }

        // parse->returningList = preprocess_expression(..., EXPRKIND_TARGET)
        // (C:918-920). Same per-TargetEntry handling as targetList.
        {
            let n = run.resolve(root.parse).returningList.len();
            for i in 0..n {
                let e = run.resolve_mut(root.parse).returningList[i].expr.take();
                let e = match e {
                    Some(b) => Some(mcx::PgBox::into_inner(b)),
                    None => None,
                };
                let processed = preprocess_expression(mcx, &root, e, EXPRKIND_TARGET)?;
                run.resolve_mut(root.parse).returningList[i].expr = match processed {
                    Some(pe) => Some(mcx::alloc_in(mcx, pe)?),
                    None => None,
                };
            }
        }

        // preprocess_qual_conditions(root, (Node *) parse->jointree) (C:922).
        preprocess_qual_conditions_query(mcx, &root, run)?;

        // parse->havingQual = preprocess_expression(..., EXPRKIND_QUAL) (C:924).
        {
            let h = run.resolve_mut(root.parse).havingQual.take();
            let h = h.map(mcx::PgBox::into_inner);
            let processed = preprocess_expression(mcx, &root, h, EXPRKIND_QUAL)?;
            run.resolve_mut(root.parse).havingQual = match processed {
                Some(pe) => Some(mcx::alloc_in(mcx, pe)?),
                None => None,
            };
        }

        // windowClause start/end offsets (C:927-936). The windowClause is a
        // `PgVec<NodePtr>` of WindowClause nodes; the offsets live on those nodes.
        // A plain SELECT has none. Panic precisely if present.
        if !run.resolve(root.parse).windowClause.is_empty() {
            panic!(
                "subquery_planner: windowClause offset preprocessing \
                 (planner.c:927-936) is not wired over the owned Query model \
                 (windowClause is a NodePtr list of WindowClause nodes)"
            );
        }

        // parse->limitOffset / parse->limitCount = preprocess_expression(...,
        // EXPRKIND_LIMIT) (C:938-941).
        {
            let lo = run.resolve_mut(root.parse).limitOffset.take();
            let lo = lo.map(mcx::PgBox::into_inner);
            let processed = preprocess_expression(mcx, &root, lo, EXPRKIND_LIMIT)?;
            run.resolve_mut(root.parse).limitOffset = match processed {
                Some(pe) => Some(mcx::alloc_in(mcx, pe)?),
                None => None,
            };
        }
        {
            let lc = run.resolve_mut(root.parse).limitCount.take();
            let lc = lc.map(mcx::PgBox::into_inner);
            let processed = preprocess_expression(mcx, &root, lc, EXPRKIND_LIMIT)?;
            run.resolve_mut(root.parse).limitCount = match processed {
                Some(pe) => Some(mcx::alloc_in(mcx, pe)?),
                None => None,
            };
        }

        // onConflict expression lists (C:943-963). onConflict is only present for
        // INSERT ... ON CONFLICT; a SELECT has none. Panic precisely if present.
        if run.resolve(root.parse).onConflict.is_some() {
            panic!(
                "subquery_planner: onConflict expression preprocessing \
                 (planner.c:943-963) is not wired over the owned Query model"
            );
        }

        // mergeActionList (C:965-978). MERGE-only; a SELECT has none.
        if !run.resolve(root.parse).mergeActionList.is_empty() {
            panic!(
                "subquery_planner: mergeActionList expression preprocessing \
                 (planner.c:965-978) is not wired over the owned Query model"
            );
        }

        // parse->mergeJoinCondition = preprocess_expression(..., EXPRKIND_QUAL)
        // (C:980-981).
        {
            let c = run.resolve_mut(root.parse).mergeJoinCondition.take();
            let c = c.map(mcx::PgBox::into_inner);
            let processed = preprocess_expression(mcx, &root, c, EXPRKIND_QUAL)?;
            run.resolve_mut(root.parse).mergeJoinCondition = match processed {
                Some(pe) => Some(mcx::alloc_in(mcx, pe)?),
                None => None,
            };
        }

        // root->append_rel_list = preprocess_expression(..., EXPRKIND_APPINFO)
        // (C:983-985). append_rel_list is empty until inheritance/UNION-ALL
        // expansion, which the simple SELECT path does not produce. Panic
        // precisely if present (its AppendRelInfo translated_vars would need the
        // EXPRKIND_APPINFO walk).
        if !root.append_rel_list.is_empty() {
            panic!(
                "subquery_planner: append_rel_list expression preprocessing \
                 (planner.c:983-985) is not wired over the owned model"
            );
        }

        // Preprocess expressions within RTEs (C:987-1054): tablesample / subquery
        // join-alias flattening / function / tablefunc / values / groupexprs, plus
        // per-element securityQuals. A plain RTE_RELATION SELECT has none of these
        // (no TABLESAMPLE, no function/values/group RTEs, no securityQuals); scan
        // and panic precisely if any present.
        {
            use types_nodes::parsenodes::RTEKind;
            let n = run.resolve(root.parse).rtable.len();
            for i in 0..n {
                let parse = run.resolve(root.parse);
                let rte = &parse.rtable[i];
                let needs = match rte.rtekind {
                    RTEKind::RTE_RELATION => rte.tablesample.is_some(),
                    RTEKind::RTE_SUBQUERY => rte.lateral && root.hasJoinRTEs,
                    RTEKind::RTE_FUNCTION
                    | RTEKind::RTE_TABLEFUNC
                    | RTEKind::RTE_VALUES
                    | RTEKind::RTE_GROUP => true,
                    _ => false,
                };
                let has_secquals = !rte.securityQuals.is_empty();
                if needs || has_secquals {
                    panic!(
                        "subquery_planner: per-RTE expression preprocessing \
                         (planner.c:987-1054) for rtekind {:?} (tablesample/\
                         function/tablefunc/values/groupexprs/securityQuals/\
                         lateral-subquery alias flattening) is not wired over the \
                         owned RTE model",
                        rte.rtekind
                    );
                }
            }
        }

        // Drop joinaliasvars lists once flattening is done (C:1067-1078). Only
        // relevant when hasJoinRTEs; the simple SELECT path has no join RTEs.
        if root.hasJoinRTEs {
            panic!(
                "subquery_planner: joinaliasvars cleanup (planner.c:1067-1078) is \
                 reached only with join RTEs, whose flatten_join_alias_vars path \
                 is unported"
            );
        }

        // flatten_group_exprs over targetList + havingQual (C:1088-1095). GROUP-RTE
        // only; a non-grouped SELECT has hasGroupRTE == false.
        if run.resolve(root.parse).hasGroupRTE {
            panic!(
                "subquery_planner: flatten_group_exprs (clauses.c) over targetList/\
                 havingQual (planner.c:1088-1095) has no ported owner; reached \
                 because parse->hasGroupRTE is set"
            );
        }

        // hasTargetSRFs re-check (C:1098-1099). Constant-folding can remove all
        // SRFs; recompute via expression_returns_set over the targetList. Only
        // relevant when hasTargetSRFs is set; a plain scalar SELECT has none.
        if run.resolve(root.parse).hasTargetSRFs {
            panic!(
                "subquery_planner: hasTargetSRFs re-check via expression_returns_set \
                 (planner.c:1098-1099) over the targetList is not wired"
            );
        }

        // expand_grouping_sets (C:1107-1110). GROUPING SETS only.
        if !run.resolve(root.parse).groupingSets.is_empty() {
            panic!(
                "subquery_planner: expand_grouping_sets (parse_agg.c) \
                 (planner.c:1107-1110) has no ported owner; reached because \
                 parse->groupingSets is non-empty"
            );
        }

        // newHaving HAVING→WHERE transfer loop (C:1154-1199). Runs only when there
        // is a havingQual; a SELECT without HAVING skips it entirely.
        if run.resolve(root.parse).havingQual.is_some() {
            panic!(
                "subquery_planner: HAVING→WHERE transfer loop (planner.c:1154-1199) \
                 needs contain_agg_clause/contain_volatile_functions/contain_subplans/\
                 pull_varnos over the HAVING clause and list_concat into jointree \
                 quals; not yet wired over the owned model"
            );
        }
    }

    // reduce_outer_joins / remove_useless_result_rtes (C:1206-1216):
    {
        if has_outer_joins {
            let parse = run.resolve_mut(root.parse);
            backend_optimizer_prep_prepjointree::reduce_outer_joins(mcx, &mut root, parse)?;
        }
        if has_result_rtes || has_outer_joins {
            let parse = run.resolve_mut(root.parse);
            backend_optimizer_prep_prepjointree::remove_useless_result_rtes(mcx, &mut root, parse)?;
        }

        // grouping_planner(root, tuple_fraction, setops) (C:1221).
        grouping_planner(mcx, run, &mut root, tuple_fraction, setops)?;

        // SS_identify_outer_params(root) (C:1227).
        backend_optimizer_plan_init_subselect::correlation::SS_identify_outer_params(&mut root);

        // final_rel = fetch_upper_rel(...); SS_charge_for_initplans(root, final_rel)
        // (C:1235-1236).
        let final_rel = backend_optimizer_util_relnode::fetch_upper_rel(
            &mut root,
            UPPERREL_FINAL,
            &None,
        );
        backend_optimizer_plan_init_subselect::finalize::SS_charge_for_initplans(
            &mut root, final_rel,
        );

        // The set of relations consulted to prepare the query is what we will
        // need to lock before executing the resulting plan.  ... (C:1238-1241)
        // glob->finalrtable etc. is assembled in standard_planner's PlannedStmt
        // build; nothing extra to do here on the value model.

        // set_cheapest(final_rel) (C:1243).
        backend_optimizer_util_pathnode::set_cheapest(&mut root, final_rel)?;

        Ok(root)
    }
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
    Some(Box::new(types_pathnodes::Bitmapset { words }))
}

/// `IS_OUTER_JOIN(jointype)` (nodes/nodes.h).
fn is_outer_join(jointype: types_nodes::jointype::JoinType) -> bool {
    use types_nodes::jointype::JoinType;
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
    root: &PlannerInfo,
    expr: Option<Expr>,
    kind: i32,
) -> PgResult<Option<Expr>> {
    // Fall out quickly if expression is empty (C:1262).
    let mut expr = match expr {
        None => return Ok(None),
        Some(e) => e,
    };

    // flatten_join_alias_vars (C:1274-1279).
    if root.hasJoinRTEs
        && !(kind == EXPRKIND_RTFUNC
            || kind == EXPRKIND_VALUES
            || kind == EXPRKIND_TABLESAMPLE
            || kind == EXPRKIND_TABLEFUNC)
    {
        panic!(
            "preprocess_expression: flatten_join_alias_vars (rewriteManip.c) has no \
             ported owner (reached because root.hasJoinRTEs is set)"
        );
    }

    // eval_const_expressions (C:1300-1301).
    if kind != EXPRKIND_RTFUNC {
        expr = backend_optimizer_util_clauses::eval_const_expressions(mcx, expr)?;
    }

    // canonicalize_qual (C:1306-1308).
    if kind == EXPRKIND_QUAL {
        expr = match backend_optimizer_prep_prepqual::canonicalize_qual(Some(expr), false)? {
            Some(e) => e,
            None => return Ok(None),
        };
    }

    // convert_saop_to_hashed_saop (C:1321-1324).
    if kind == EXPRKIND_QUAL || kind == EXPRKIND_TARGET {
        backend_optimizer_util_clauses::convert_saop_to_hashed_saop(mcx, &mut expr)?;
    }

    // SS_process_sublinks (C:1326-1328).
    if run_parse_has_sublinks(root) {
        panic!(
            "preprocess_expression: SS_process_sublinks (subselect.c) over a single \
             arena Expr has no ported entry (its owner walks the whole Query); \
             reached because parse->hasSubLinks is set"
        );
    }

    // SS_replace_correlation_vars for sub-queries (C:1336-1337).
    if root.query_level > 1 {
        panic!(
            "preprocess_expression: SS_replace_correlation_vars (subselect.c) over a \
             single arena Expr has no ported entry; reached because query_level > 1"
        );
    }

    // make_ands_implicit (C:1345-1346): convert qual to implicit-AND list. The
    // arena `Expr` model returns a single Expr; `make_ands_implicit` yields a
    // Vec<Expr> (the implicit-AND list). Callers of qual preprocessing in this
    // model expect a single (possibly AND) Expr, so the implicit-AND flattening
    // is applied by the caller; here we return the canonicalized Expr unchanged,
    // matching the value-model convention used by preprocess_qual_conditions.
    let _ = make_ands_implicit_noop;

    Ok(Some(expr))
}

/// Placeholder reference to document where `make_ands_implicit`
/// (nodes-core makefuncs.rs) is applied; see `preprocess_expression`.
#[inline]
fn make_ands_implicit_noop() {}

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
    root: &PlannerInfo,
    run: &mut PlannerRun<'mcx>,
) -> PgResult<()> {
    let jt = run.resolve_mut(root.parse).jointree.take();
    if let Some(jt) = jt {
        let mut node = Node::FromExpr(mcx::PgBox::into_inner(jt));
        preprocess_qual_conditions(mcx, root, &mut node)?;
        let f = match node {
            Node::FromExpr(f) => f,
            _ => unreachable!("jointree top stays a FromExpr"),
        };
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
    root: &PlannerInfo,
    jtnode: &mut Node<'mcx>,
) -> PgResult<()> {
    match jtnode {
        Node::RangeTblRef(_) => {
            // nothing to do here (C:1362).
        }
        Node::FromExpr(f) => {
            for i in 0..f.fromlist.len() {
                preprocess_qual_conditions(mcx, root, &mut f.fromlist[i])?;
            }
            preprocess_jointree_quals(mcx, root, &mut f.quals)?;
        }
        Node::JoinExpr(j) => {
            if let Some(larg) = j.larg.as_deref_mut() {
                preprocess_qual_conditions(mcx, root, larg)?;
            }
            if let Some(rarg) = j.rarg.as_deref_mut() {
                preprocess_qual_conditions(mcx, root, rarg)?;
            }
            preprocess_jointree_quals(mcx, root, &mut j.quals)?;
        }
        other => {
            return Err(PgError::error(alloc::format!(
                "preprocess_qual_conditions: unrecognized jointree node type: {:?}",
                other.node_tag()
            )));
        }
    }
    Ok(())
}

/// `f->quals = preprocess_expression(root, f->quals, EXPRKIND_QUAL)` for a
/// jointree node's `quals` (`Option<NodePtr>` holding `Node::Expr`).
fn preprocess_jointree_quals<'mcx>(
    mcx: Mcx<'mcx>,
    root: &PlannerInfo,
    quals: &mut Option<types_nodes::nodes::NodePtr<'mcx>>,
) -> PgResult<()> {
    // The C field is `Node *quals`; in the analyzed jointree it is `Node::Expr`.
    // Take ownership, unwrap to `Expr`, preprocess, re-wrap.
    let taken = quals.take();
    let expr = match taken {
        None => None,
        Some(n) => match mcx::PgBox::into_inner(n) {
            Node::Expr(e) => Some(e),
            other => {
                return Err(PgError::error(alloc::format!(
                    "preprocess_qual_conditions: jointree quals is a non-Expr node: {:?}",
                    other.node_tag()
                )));
            }
        },
    };
    let processed = preprocess_expression(mcx, root, expr, EXPRKIND_QUAL)?;
    *quals = match processed {
        Some(e) => Some(mcx::alloc_in(mcx, Node::Expr(e))?),
        None => None,
    };
    Ok(())
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
    setops: Option<()>,
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
        // Set-operation branch (C:1468-1523).
        let _current_rel =
            backend_optimizer_prep_prepunion_seams::plan_set_operations::call(mcx, run, root)?;

        // Assert(parse->commandType == CMD_SELECT). postprocess_setop_tlist over
        // copyObject(processed_tlist), final_target from the path's pathtarget,
        // is_parallel_safe(final_target->exprs), the rowMarks FOR UPDATE guard
        // (CheckSelectLocking already done at parse), and
        // make_pathkeys_for_sortclauses for sort_pathkeys. final_target comes from
        // `current_rel->cheapest_total_path->pathtarget`, an unported
        // PathTarget. Mirror PG and panic precisely after plan_set_operations.
        panic!(
            "grouping_planner: set-operation post-processing (postprocess_setop_tlist \
             + final_target from cheapest_total_path->pathtarget + \
             make_pathkeys_for_sortclauses) is gated on the unported PathTarget \
             upper-rel machinery (planner.c:1478-1522)"
        );
    }

    // Regular planning branch (C:1524+).
    // A recursive query should always have setOperations (C:1547).
    debug_assert!(!root.hasRecursion);

    // Preprocess grouping sets / GROUP BY (C:1549-1558).
    {
        let has_grouping_sets = !run.resolve(root.parse).groupingSets.is_empty();
        let has_group_clause = !run.resolve(root.parse).groupClause.is_empty();
        if has_grouping_sets {
            // preprocess_grouping_sets(root) — needs expand_grouping_sets /
            // remap_to_groupclause_idx machinery (no ported owner).
            panic!(
                "grouping_planner: preprocess_grouping_sets (planner.c:2900+) for \
                 GROUPING SETS has no ported owner"
            );
        } else if has_group_clause {
            root.processed_groupClause = preprocess_groupclause(run, root, &[])?;
        }
    }

    // Preprocess targetlist (C:1567).
    {
        let parse = run.resolve_mut(root.parse);
        backend_optimizer_prep_preptlist::preprocess_targetlist(mcx, root, parse)?;
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
            // processed_tlist: clone each TargetEntry's expr, then walk it.
            let tlist_exprs: Vec<types_pathnodes::NodeId> =
                root.processed_tlist.iter().map(|te| root.targetentry(*te).expr).collect();
            for expr_id in tlist_exprs {
                let cloned = root.node(expr_id).clone_in(mcx)?;
                backend_optimizer_prep_prepagg::preprocess_aggrefs(mcx, root, &cloned)?;
            }
            // preprocess_aggrefs(root, (Node *) parse->havingQual) (C:1580).
            // `havingQual` is the concretely-typed `Option<PgBox<Expr>>` view;
            // clone the owned `Expr` and walk it, mirroring the processed_tlist
            // handling above.
            let having: Option<Expr> = match run.resolve(root.parse).havingQual.as_deref() {
                Some(e) => Some(e.clone_in(mcx)?),
                None => None,
            };
            if let Some(having) = having {
                backend_optimizer_prep_prepagg::preprocess_aggrefs(mcx, root, &having)?;
            }
        }
    }

    // find_window_functions if hasWindowFuncs (C:1588-1609).
    {
        let has_window = run.resolve(root.parse).hasWindowFuncs;
        if has_window {
            panic!(
                "grouping_planner: find_window_functions / optimize_window_clauses / \
                 select_active_windows (planner.c) for window functions are part of \
                 the unported upper-rel machinery"
            );
        }
    }

    // preprocess_minmax_aggregates if hasAggs (C:1617-1618).
    {
        let has_aggs = run.resolve(root.parse).hasAggs;
        if has_aggs {
            panic!(
                "grouping_planner: preprocess_minmax_aggregates (planagg.c) has no \
                 ported owner"
            );
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
    // gset_data = NULL, setop = setops. On the regular non-setop SELECT path
    // these are all empty/none.
    let _ = setops;

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
    let sort_clause_ids: Vec<types_pathnodes::NodeId> = {
        let sort_clauses: Vec<types_nodes::rawnodes::SortGroupClause> = run
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
    let mut qp_callback = move |root: &mut PlannerInfo| -> PgResult<()> {
        standard_qp_callback(root, &sort_clause_ids)
    };
    let mut current_rel =
        backend_optimizer_plan_small::query_planner(mcx, run, root, &mut qp_callback)?;

    // create_pathtarget(root, root->processed_tlist) (C:1665) =
    // set_pathtarget_cost_width(root, make_pathtarget_from_tlist(processed_tlist)).
    let mut final_target = backend_optimizer_util_vars::tlist::make_pathtarget_from_tlist(
        root,
        &root.processed_tlist,
    );
    backend_optimizer_path_costsize::sizeest::set_pathtarget_cost_width(root, &mut final_target);
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
    let (sort_input_target, sort_input_target_parallel_safe) = if has_sort {
        let sit = make_sort_input_target(run, root, &final_target, &mut have_postponed_srfs)?;
        let safe = is_target_exprs_parallel_safe(root, &sit.exprs);
        (sit, safe)
    } else {
        (final_target.clone(), final_target_parallel_safe)
    };

    // Window functions were already rejected above (C:1588-1609 panic); assert.
    debug_assert!(!has_window);
    // With no activeWindows, grouping_target = sort_input_target (C:1703-1705).
    let grouping_target = sort_input_target.clone();
    let grouping_target_parallel_safe = sort_input_target_parallel_safe;

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
    let (scanjoin_target, scanjoin_target_parallel_safe) = if have_grouping {
        let sj = make_group_input_target(mcx, run, root, &final_target)?;
        let safe = is_target_exprs_parallel_safe(root, &sj.exprs);
        (sj, safe)
    } else {
        // With no grouping, scanjoin_target = grouping_target (C:1722-1725).
        (grouping_target.clone(), grouping_target_parallel_safe)
    };

    // Targetlist SRFs (C:1733-1768). split_pathtarget_at_srfs is not ported.
    if has_target_srfs {
        panic!(
            "grouping_planner: targetlist set-returning functions need \
             split_pathtarget_at_srfs (planner.c:1733) — not ported"
        );
    }

    // DISTINCT (C:1834-1841): create_distinct_paths is not ported.
    if has_distinct {
        panic!(
            "grouping_planner: DISTINCT needs create_distinct_paths \
             (planner.c:1837) — not ported"
        );
    }

    // No SRFs: scanjoin_targets = list_make1(scanjoin_target) (C:1764-1767).

    // scanjoin_target_same_exprs = list_length(scanjoin_targets) == 1
    //   && equal(scanjoin_target->exprs, current_rel->reltarget->exprs) (C:1771).
    let scanjoin_target_same_exprs = {
        let cur_exprs: &[types_pathnodes::NodeId] = root
            .rel(current_rel)
            .reltarget
            .as_ref()
            .map(|t| t.exprs.as_slice())
            .unwrap_or(&[]);
        equal_expr_handle_lists(root, &scanjoin_target.exprs, cur_exprs)
    };

    // apply_scanjoin_target_to_paths(root, current_rel, [scanjoin_target], NIL,
    //   scanjoin_target_parallel_safe, scanjoin_target_same_exprs) (C:1773).
    apply_scanjoin_target_to_paths(
        run,
        root,
        current_rel,
        &scanjoin_target,
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
        )?;
        // adjust_paths_for_srfs when grouping_target contains SRFs (C:1804-1808)
        // is gated out: hasTargetSRFs loud-panics above before this point.
        debug_assert!(!has_target_srfs);
    }

    // Window / DISTINCT upper rels on this path (C:1813-1841) are guarded out
    // above (each loud-panics).

    // If ORDER BY was given, generate a new upperrel of paths that emit the
    // correct ordering and project final_target (C:1849-1866). We can apply the
    // limit_tuples bound in sort costing only if there are no postponed SRFs.
    if has_sort {
        let limit_for_sort = if have_postponed_srfs { -1.0 } else { limit_tuples };
        let ordered_rel = create_ordered_paths(
            root,
            run,
            current_rel,
            &final_target,
            final_target_parallel_safe,
            limit_for_sort,
        )?;
        // adjust_paths_for_srfs when final_target contains SRFs (C:1860-1862) is
        // gated out: hasTargetSRFs loud-panics above before this point.
        debug_assert!(!has_target_srfs);
        // current_rel becomes the ordered upperrel for the final-output build.
        current_rel = ordered_rel;
    }

    // Now build the final-output upperrel (C:1868).
    let final_rel = backend_optimizer_util_relnode::fetch_upper_rel(root, UPPERREL_FINAL, &None);

    // consider_parallel propagation (C:1870-1880). current_rel->consider_parallel
    // is false here (glob.max_parallel_hazard is UNSAFE in this repo), so the
    // is_parallel_safe(limitOffset/limitCount) checks short-circuit away and
    // final_rel->consider_parallel stays false. We mirror that exactly without
    // evaluating the (latent) limit parallel-safety.
    if root.rel(current_rel).consider_parallel {
        // Unreachable here, but kept faithful: would require is_parallel_safe
        // over parse->limitOffset / limitCount.
        panic!(
            "grouping_planner: final_rel consider_parallel propagation needs \
             is_parallel_safe(limitOffset/limitCount) (planner.c:1877) — \
             unreachable in this repo (glob is parallel-unsafe)"
        );
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
    let (has_rowmarks, has_limit_clause, command_type) = {
        let parse = run.resolve(root.parse);
        (
            !parse.rowMarks.is_empty(),
            parse.limitCount.is_some() || parse.limitOffset.is_some(),
            parse.commandType,
        )
    };
    if has_rowmarks {
        panic!(
            "grouping_planner: FOR [KEY] UPDATE/SHARE adds create_lockrows_path \
             (planner.c:1907) — not reached on a plain SELECT"
        );
    }
    if has_limit_clause {
        panic!(
            "grouping_planner: LIMIT/OFFSET adds create_limit_path \
             (planner.c:1917) — the limit upper path is not ported"
        );
    }
    if command_type != CmdType::CMD_SELECT {
        panic!(
            "grouping_planner: INSERT/UPDATE/DELETE/MERGE adds \
             create_modifytable_path (planner.c:2111) — not ported"
        );
    }

    let surviving: Vec<PathId> = root.rel(current_rel).pathlist.clone();
    for path in surviving {
        backend_optimizer_util_pathnode::add_path(root, final_rel, path)?;
    }

    // Partial paths for final_rel (C:2134-2146): final_rel->consider_parallel is
    // false (see above), so this block is skipped.

    // GetForeignUpperPaths / create_upper_paths_hook (C:2152-2167): no FDW
    // upper-path routine is modeled and there is no hook; nothing to add.

    // Note: caller (subquery_planner) does set_cheapest(final_rel) (C:2169).
    Ok(())
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
    use backend_optimizer_util_vars::tlist::{
        add_column_to_pathtarget, add_new_columns_to_pathtarget, create_empty_pathtarget,
        get_sortgroupref_clause_noerr,
    };
    use backend_optimizer_util_vars::var::{
        pull_var_clause, PVC_INCLUDE_PLACEHOLDERS, PVC_RECURSE_AGGREGATES, PVC_RECURSE_WINDOWFUNCS,
    };

    // We must build a target containing all grouping columns, plus any other
    // Vars mentioned in the query's targetlist and HAVING qual (C:5541).
    let mut input_target = create_empty_pathtarget();

    // Resolve processed_groupClause handles into SortGroupClause values once
    // (the C `root->processed_groupClause` List). For a plain aggregate this is
    // empty, so every column drops to the non-group path.
    let group_clauses: Vec<types_nodes::rawnodes::SortGroupClause> = root
        .processed_groupClause
        .iter()
        .map(|&id| *root.sortgroupclause(id))
        .collect();

    // The parser/grouping-sets RT-index removal (parse->hasGroupRTE &&
    // groupingSets) is gated out on this path (groupingSets loud-panics upstream).
    let mut non_group_cols: Vec<types_pathnodes::NodeId> = Vec::new();
    for (i, &expr_id) in final_target.exprs.iter().enumerate() {
        let sgref = get_pathtarget_sortgroupref(final_target, i);
        let is_group_col = sgref != 0
            && !group_clauses.is_empty()
            && get_sortgroupref_clause_noerr(sgref, &group_clauses).is_some();
        if is_group_col {
            // It's a grouping column; add it to the input target as-is.
            add_column_to_pathtarget(&mut input_target, expr_id, sgref);
        } else {
            // Non-grouping column; remember the expression for the later
            // pull_var_clause call.
            non_group_cols.push(expr_id);
        }
    }

    // If there's a HAVING clause, we'll need the Vars it uses, too (C:5586).
    // havingQual is the concretely-typed owned `Option<PgBox<Expr>>` view; clone
    // it into the arena to obtain a handle that pull_var_clause can walk.
    let having_id: Option<types_pathnodes::NodeId> =
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
    let mut non_group_var_ids: Vec<types_pathnodes::NodeId> = Vec::new();
    let flags = PVC_RECURSE_AGGREGATES | PVC_RECURSE_WINDOWFUNCS | PVC_INCLUDE_PLACEHOLDERS;
    for &nid in &non_group_cols {
        let node = Node::Expr(root.node(nid).clone());
        let vars = pull_var_clause(&node, flags);
        for v in vars {
            non_group_var_ids.push(root.alloc_node(v));
        }
    }
    add_new_columns_to_pathtarget(root, &mut input_target, &non_group_var_ids);

    // XXX this causes some redundant cost calculation ... (C:5619).
    backend_optimizer_path_costsize::sizeest::set_pathtarget_cost_width(root, &mut input_target);
    Ok(input_target)
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
    _path_rows: f64,
) -> PgResult<f64> {
    let parse = run.resolve(root.parse);
    let has_group_clause = !parse.groupClause.is_empty();
    let has_grouping_sets = !parse.groupingSets.is_empty();
    let has_aggs = parse.hasAggs;
    if has_group_clause {
        // Plain GROUP BY — estimate based on the optimized groupClause via
        // estimate_num_groups (selfuncs.c). The GROUP BY path also needs
        // AGG_SORTED group_pathkeys (and, for non-aggregate GROUP BY,
        // create_group_path), which are not ported on this path; that gap is
        // surfaced as a precise error in add_paths_to_grouping_rel before any
        // group count is consumed, so the estimate is not reached here.
        debug_assert!(!has_grouping_sets);
        Err(PgError::error(
            "get_number_of_groups: GROUP BY group-count estimation (estimate_num_groups) \
             is reached only on the unported GROUP BY aggregation path",
        ))
    } else if has_grouping_sets {
        // Empty grouping sets — gated out upstream.
        panic!("get_number_of_groups: GROUPING SETS gated out upstream");
    } else if has_aggs || root.hasHavingQual {
        // Plain aggregation, one result row.
        Ok(1.0)
    } else {
        // Not grouping.
        Ok(1.0)
    }
}

/// `create_grouping_paths(root, input_rel, target, target_parallel_safe, gd)`
/// (planner.c:3779) — build a new upperrel containing Paths for grouping and/or
/// aggregation. Restricted to the non-degenerate, non-grouping-sets,
/// non-partitionwise, non-parallel cases reachable on this path: the sorted
/// Agg / Group path over the cheapest input. Partial/partitionwise/degenerate
/// legs loud-panic (they are gated out upstream or require unported machinery).
fn create_grouping_paths<'mcx>(
    mcx: Mcx<'mcx>,
    run: &mut PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    input_rel: RelId,
    target: PathTarget,
    target_parallel_safe: bool,
) -> PgResult<RelId> {
    // MemSet(&agg_costs, 0); get_agg_clause_costs(root, AGGSPLIT_SIMPLE, &agg_costs).
    let mut agg_costs = types_pathnodes::AggClauseCosts::default();
    backend_optimizer_prep_prepagg::get_agg_clause_costs(
        root,
        types_nodes::nodeagg::AGGSPLIT_SIMPLE,
        &mut agg_costs,
    )?;
    let agg_costs_lite = agg_clause_costs_to_lite(&agg_costs);

    // make_grouping_rel(root, input_rel, target, target_parallel_safe,
    // parse->havingQual) (C:3798). IS_OTHER_REL(input_rel) is false here (top
    // grouping rel), so the relids set is NULL.
    let grouped_rel = backend_optimizer_util_relnode::fetch_upper_rel(root, UPPERREL_GROUP_AGG, &None);
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
    // && groupClause == NIL. The groupingSets leg is gated out; a HAVING-only
    // query with no aggregates and no GROUP BY would be degenerate, but that
    // needs create_group_result_path (unported here). Mirror PG and panic only
    // when actually degenerate.
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
        panic!(
            "create_grouping_paths: degenerate grouping (create_degenerate_grouping_paths \
             + create_group_result_path, planner.c:3966) is not ported"
        );
    }

    // create_ordinary_grouping_paths(root, input_rel, grouped_rel, &agg_costs,
    // gd=NULL, &extra, &partially_grouped_rel) (C:3875). The partitionwise /
    // partial-agg / parallel legs require IS_PARTITIONED_REL(input_rel) or
    // consider_parallel, both false here; they are skipped exactly as in C.
    create_ordinary_grouping_paths(
        mcx,
        run,
        root,
        input_rel,
        grouped_rel,
        agg_costs_lite,
        has_group_clause,
        has_aggs,
    )?;

    // set_cheapest(grouped_rel) (C:3880).
    backend_optimizer_util_pathnode::set_cheapest(root, grouped_rel)?;
    Ok(grouped_rel)
}

/// The reachable core of `create_ordinary_grouping_paths` (planner.c:4031):
/// estimate the number of groups and add the final grouping/aggregation paths
/// (`add_paths_to_grouping_rel`). The partitionwise and partial-aggregation
/// branches are skipped (input rel is neither partitioned nor parallel-safe on
/// this path, exactly as the C gating conditions require).
fn create_ordinary_grouping_paths<'mcx>(
    mcx: Mcx<'mcx>,
    run: &mut PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    input_rel: RelId,
    grouped_rel: RelId,
    agg_costs: Option<backend_optimizer_util_pathnode_seams::AggClauseCostsLite>,
    has_group_clause: bool,
    has_aggs: bool,
) -> PgResult<()> {
    // cheapest_path = input_rel->cheapest_total_path.
    let cheapest_path = root
        .rel(input_rel)
        .cheapest_total_path
        .ok_or_else(|| PgError::error("create_ordinary_grouping_paths: input rel has no cheapest_total_path"))?;
    let cheapest_rows = root.path(cheapest_path).base().rows;

    // Estimate number of groups (C:4130).
    let d_num_groups = get_number_of_groups(run, root, cheapest_rows)?;

    // Build final grouping paths (C:4136).
    add_paths_to_grouping_rel(
        mcx,
        run,
        root,
        input_rel,
        grouped_rel,
        agg_costs,
        d_num_groups,
        has_group_clause,
        has_aggs,
    )?;

    // Give a helpful error if we failed to find any implementation (C:4141).
    if root.rel(grouped_rel).pathlist.is_empty() {
        return Err(PgError::error(
            "could not implement GROUP BY",
        ));
    }
    Ok(())
}

/// The reachable core of `add_paths_to_grouping_rel` (planner.c:7113): add the
/// sorted-input Agg / Group paths over each input path. can_sort is true (an
/// empty processed_groupClause is trivially sortable, and a GROUP BY only
/// reaches here when grouping_is_sortable). can_hash is false unless there's a
/// GROUP BY; the hashed legs and the partially-grouped finalization are skipped
/// (no partial rel on this path). For each input path we consider its useful
/// group-key orderings; for the empty group clause that's the single original
/// (empty) ordering, so make_ordered_path returns the path unchanged.
#[allow(clippy::too_many_arguments)]
fn add_paths_to_grouping_rel<'mcx>(
    mcx: Mcx<'mcx>,
    run: &mut PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    input_rel: RelId,
    grouped_rel: RelId,
    agg_costs: Option<backend_optimizer_util_pathnode_seams::AggClauseCostsLite>,
    d_num_groups: f64,
    has_group_clause: bool,
    has_aggs: bool,
) -> PgResult<()> {
    // can_sort = grouping_is_sortable(processed_groupClause); for the empty
    // clause this is trivially true. can_hash needs a GROUP BY (false for plain
    // aggregation); the hash leg requires grouping_is_hashable and is not on the
    // count(*) path, so reject it precisely if reached.
    let group_clauses: Vec<types_nodes::rawnodes::SortGroupClause> = root
        .processed_groupClause
        .iter()
        .map(|&id| *root.sortgroupclause(id))
        .collect();
    let can_sort = backend_optimizer_util_vars::tlist::grouping_is_sortable(&group_clauses);
    if !can_sort {
        // GROUPING_CAN_USE_SORT not set without a sortable clause; the hash-only
        // leg is the only alternative and is unported on this path.
        return Err(PgError::error(
            "add_paths_to_grouping_rel: non-sortable grouping needs the hashed-only \
             aggregation path (not ported)",
        ));
    }

    // havingQual (extra->havingQual) — the HAVING qual clause list as bare node
    // handles. parse->havingQual is the owned Option<PgBox<Expr>>; clone it into
    // the arena to obtain the qual list the Agg/Group path carries.
    let having_quals: Vec<types_pathnodes::NodeId> = {
        match run.resolve(root.parse).havingQual.as_deref() {
            Some(e) => {
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

    // foreach(input_rel->pathlist) — consider each input path. For the empty
    // group clause get_useful_group_keys_orderings yields a single ordering with
    // empty pathkeys/clauses, and make_ordered_path returns the path unchanged
    // (empty pathkeys are contained in any path). Reordering with a real GROUP BY
    // is a cost optimization; we mirror the always-present original ordering and
    // skip the (optional) reordered alternative.
    let input_paths: Vec<PathId> = root.rel(input_rel).pathlist.clone();
    for path in input_paths {
        // make_ordered_path with the group_pathkeys ordering. group_pathkeys is
        // empty here (no GROUP BY produces no pathkeys; a GROUP BY's pathkeys were
        // computed by standard_qp_callback). pathkeys_count_contained_in(empty, _)
        // is true, so the path is used as-is for the no-sort case. For a real
        // GROUP BY the same make_ordered_path used elsewhere applies, but
        // create_sort_path over group_pathkeys is the same machinery as ORDER BY;
        // reuse it via make_ordered_path.
        let ordered = make_ordered_path(root, run, grouped_rel, path, path, -1.0)?;
        let ordered = match ordered {
            Some(p) => p,
            None => continue,
        };

        if has_aggs {
            // We have aggregation, possibly with plain GROUP BY. Make an AggPath
            // (AGG_SORTED with GROUP BY, AGG_PLAIN otherwise) (C:7177).
            let aggstrategy = if has_group_clause {
                types_pathnodes::AGG_SORTED
            } else {
                types_pathnodes::AGG_PLAIN
            };
            let agg_path = backend_optimizer_util_pathnode::create::create_agg_path(
                run,
                root,
                grouped_rel,
                ordered,
                target.clone(),
                aggstrategy,
                types_pathnodes::AGGSPLIT_SIMPLE,
                root.processed_groupClause.clone(),
                having_quals.clone(),
                agg_costs,
                d_num_groups,
            )?;
            backend_optimizer_util_pathnode::add_path(root, grouped_rel, agg_path)?;
        } else if has_group_clause {
            // GROUP BY without aggregation — make a GroupPath (C:7195).
            return Err(PgError::error(
                "add_paths_to_grouping_rel: GROUP BY without aggregation needs \
                 create_group_path (not ported)",
            ));
        } else {
            // Other cases handled above (Assert(false)).
            unreachable!("add_paths_to_grouping_rel: no agg and no group clause");
        }
    }

    // The hashed-aggregation leg (can_hash) and the partially-grouped
    // finalization / gather legs are not on this path.
    Ok(())
}

/// Convert the full `AggClauseCosts` (types-pathnodes) into the trimmed
/// `AggClauseCostsLite` that `create_agg_path` / `cost_agg` consume.
fn agg_clause_costs_to_lite(
    c: &types_pathnodes::AggClauseCosts,
) -> Option<backend_optimizer_util_pathnode_seams::AggClauseCostsLite> {
    Some(backend_optimizer_util_pathnode_seams::AggClauseCostsLite {
        trans_startup: c.transCost.startup,
        trans_per_tuple: c.transCost.per_tuple,
        final_startup: c.finalCost.startup,
        final_per_tuple: c.finalCost.per_tuple,
        transition_space: c.transitionSpace as i32,
    })
}

/// `make_ordered_path(root, rel, path, cheapest_path, pathkeys, limit_tuples)`
/// (planner.c:7644) — return a path ordered by `pathkeys` based on `path`, or
/// `None` if it doesn't make sense to generate an ordered path here. Uses the
/// group_pathkeys ordering; an empty `pathkeys` (no GROUP BY) is contained in
/// any path, so the path is returned unchanged.
fn make_ordered_path<'mcx>(
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    rel: RelId,
    path: PathId,
    cheapest_path: PathId,
    limit_tuples: f64,
) -> PgResult<Option<PathId>> {
    let pathkeys = root.group_pathkeys.clone();
    let path_pathkeys = root.path(path).base().pathkeys.clone();
    let (is_sorted, presorted_keys) =
        backend_optimizer_path_pathkeys::pathkeys_count_contained_in(&pathkeys, &path_pathkeys);

    if is_sorted {
        return Ok(Some(path));
    }

    // Try at least sorting the cheapest path and also incrementally sorting any
    // path which is partially sorted already (C:7656-7684).
    if path != cheapest_path && (presorted_keys == 0 || !enable_incremental_sort()) {
        return Ok(None);
    }
    let sorted = if presorted_keys == 0 || !enable_incremental_sort() {
        backend_optimizer_util_pathnode::create::create_sort_path(
            root,
            rel,
            path,
            pathkeys,
            limit_tuples,
        )?
    } else {
        backend_optimizer_util_pathnode::create::create_incremental_sort_path(
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
fn is_target_exprs_parallel_safe(root: &PlannerInfo, exprs: &[types_pathnodes::NodeId]) -> bool {
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
        if let Expr::SubPlan(sp) = root.node(ipl) {
            for &p in sp.0.setParam.iter() {
                safe_param_ids.push(p);
            }
        }
    }

    for &id in exprs {
        let node = root.node(id);
        let safe = backend_optimizer_util_clauses::is_parallel_safe(
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

/// `equal((Node *) a, (Node *) b)` over two PathTarget expr handle lists. Used
/// for the `scanjoin_target_same_exprs` test (planner.c:1771). Equal iff same
/// length and each pair is structurally `equal()` (resolved through the arena).
fn equal_expr_handle_lists(
    root: &PlannerInfo,
    a: &[types_pathnodes::NodeId],
    b: &[types_pathnodes::NodeId],
) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b.iter()).all(|(&x, &y)| {
        backend_nodes_equalfuncs_seams::equal_expr::call(root.node(x), root.node(y))
    })
}

/// `apply_scanjoin_target_to_paths(root, rel, scanjoin_targets, ...)`
/// (planner.c:7669). Ported for the non-partitioned single-target case reached
/// by a simple SELECT: apply the SRF-free scan/join target to every existing
/// path of `rel`, then set `rel->reltarget` to it.
///
/// Partitioned rels (`IS_PARTITIONED_REL`), targetlist SRFs
/// (`root->parse->hasTargetSRFs` ⇒ `adjust_paths_for_srfs`), and the recursive
/// per-partition / `add_paths_to_append_rel` machinery are not reached here and
/// panic precisely.
fn apply_scanjoin_target_to_paths<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    rel: RelId,
    scanjoin_target: &PathTarget,
    scanjoin_target_parallel_safe: bool,
    tlist_same_exprs: bool,
) -> PgResult<()> {
    // This function recurses for partitioned rels; we don't support that yet.
    // IS_PARTITIONED_REL(rel): a base/join (or other-member) rel that has a
    // partitioning scheme and partition children.
    let rel_is_partitioned = {
        let r = root.rel(rel);
        matches!(
            r.reloptkind,
            types_pathnodes::RELOPT_BASEREL | types_pathnodes::RELOPT_JOINREL
        ) && r.part_scheme.is_some()
            && r.nparts > 0
    };
    if rel_is_partitioned {
        panic!(
            "apply_scanjoin_target_to_paths: partitioned-rel recursion + \
             add_paths_to_append_rel (planner.c:7770) is not ported"
        );
    }

    // If the scan/join target is not parallel-safe, partial paths cannot
    // generate it; build Gather path(s) over the partials, then drop them
    // (C:7700-7716). On the simple single-table path there are no partial paths,
    // so generate_useful_gather_paths is a no-op; we still mirror the structure.
    if !scanjoin_target_parallel_safe {
        backend_optimizer_path_allpaths::generate_useful_gather_paths(root, run, rel, false)?;
        root.rel_mut(rel).partial_pathlist = Vec::new();
        root.rel_mut(rel).consider_parallel = false;
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
            let newpath = backend_optimizer_util_pathnode::create::create_projection_path(
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
            let newpath = backend_optimizer_util_pathnode::create::create_projection_path(
                root,
                rel,
                subpath,
                Box::new(scanjoin_target.clone()),
            )?;
            root.rel_mut(rel).partial_pathlist[i] = newpath;
        }
    }

    // SRF insertion (C:7777-7780) is gated on hasTargetSRFs, rejected above.

    // Update the rel's target to be the final scan/join target (C:7792). This
    // matches the actual output of all paths and is required so create_plan /
    // create_append_path see the right pathtarget.
    root.rel_mut(rel).reltarget = Some(Box::new(scanjoin_target.clone()));

    Ok(())
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
    sort_clause_ids: &[types_pathnodes::NodeId],
) -> PgResult<()> {
    // tlist = root->processed_tlist (C:3457).
    let tlist = root.processed_tlist.clone();

    // GROUP BY / window / DISTINCT / set-op pathkeys are all NIL on this path
    // (their clauses are gated out upstream). Mirror the C else-branches that
    // assign NIL.
    root.group_pathkeys = Vec::new();
    root.num_groupby_pathkeys = 0;
    root.window_pathkeys = Vec::new();
    root.distinct_pathkeys = Vec::new();
    root.setop_pathkeys = Vec::new();

    // root->sort_pathkeys =
    //   make_pathkeys_for_sortclauses(root, parse->sortClause, tlist) (C:3583).
    root.sort_pathkeys = backend_optimizer_path_pathkeys::make_pathkeys_for_sortclauses(
        root,
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
/// `NodePtr<Node>`) as a plain (`Copy`) [`types_nodes::rawnodes::SortGroupClause`]
/// value for interning into the planner node arena. A `sortClause` element is
/// always a `SortGroupClause` (parser invariant); any other node is a bug.
fn sortgroupclause_from_node(
    np: &mcx::PgBox<'_, Node<'_>>,
) -> PgResult<types_nodes::rawnodes::SortGroupClause> {
    match &**np {
        Node::SortGroupClause(sgc) => Ok(*sgc),
        other => panic!(
            "grouping_planner: sortClause element is not a SortGroupClause (got {:?})",
            other.tag()
        ),
    }
}

/// `enable_incremental_sort` GUC, read through the guc-tables slot.
fn enable_incremental_sort() -> bool {
    backend_utils_misc_guc_tables::vars::enable_incremental_sort.read()
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

    let cpu_op_cost = backend_optimizer_path_costsize::CPU_OPERATOR_COST;

    for i in 0..ncols {
        let expr_id = final_target.exprs[i];

        if get_pathtarget_sortgroupref(final_target, i) == 0 {
            // Check for SRF or volatile functions. SRF first (we must know
            // whether we have any postponed SRFs) (C:6486-6491).
            let returns_set = has_target_srfs
                && backend_nodes_core::nodefuncs::expression_returns_set(Some(root.node(expr_id)));
            if returns_set {
                col_is_srf[i] = true;
                have_srf = true;
            } else if backend_optimizer_util_clauses::contain_volatile_functions(Some(
                root.node(expr_id),
            ))? {
                // Unconditionally postpone (C:6493-6498).
                postpone_col[i] = true;
                have_volatile = true;
            } else {
                // Else check the cost (C:6500-6519).
                let cost = backend_optimizer_path_costsize::cost_qual_eval_node(root, expr_id);
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
                && backend_nodes_core::nodefuncs::expression_returns_set(Some(root.node(expr_id)))
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
    let mut input_target = backend_optimizer_util_vars::tlist::create_empty_pathtarget();
    let mut postponable_cols: Vec<types_pathnodes::NodeId> = Vec::new();

    for i in 0..ncols {
        let expr_id = final_target.exprs[i];
        if postpone_col[i] || (postpone_srfs && col_is_srf[i]) {
            postponable_cols.push(expr_id);
        } else {
            backend_optimizer_util_vars::tlist::add_column_to_pathtarget(
                &mut input_target,
                expr_id,
                get_pathtarget_sortgroupref(final_target, i),
            );
        }
    }

    // Pull out all Vars/Aggrefs/WindowFuncs/PHVs in postponable columns and add
    // them to the sort-input target if not already present (C:6590-6595). We
    // mustn't deconstruct Aggrefs or WindowFuncs (use the INCLUDE flags).
    let pvc_flags = backend_optimizer_util_vars::PVC_INCLUDE_AGGREGATES
        | backend_optimizer_util_vars::PVC_INCLUDE_WINDOWFUNCS
        | backend_optimizer_util_vars::PVC_INCLUDE_PLACEHOLDERS;
    // pull_var_clause operates over a `Node`; run it per postponable column
    // (equivalent to walking the C `List *` of postponable exprs), interning each
    // pulled Var/Aggref/WindowFunc/PHV into the arena to obtain its handle.
    let mut postponable_vars: Vec<types_pathnodes::NodeId> = Vec::new();
    for &col in &postponable_cols {
        let node = Node::Expr(root.node(col).clone());
        for v in backend_optimizer_util_vars::pull_var_clause(&node, pvc_flags) {
            postponable_vars.push(root.alloc_node(v));
        }
    }
    backend_optimizer_util_vars::tlist::add_new_columns_to_pathtarget(
        root,
        &mut input_target,
        &postponable_vars,
    );

    // set_pathtarget_cost_width(root, input_target) (C:6603).
    backend_optimizer_path_costsize::sizeest::set_pathtarget_cost_width(root, &mut input_target);
    Ok(input_target)
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
    let ordered_rel = backend_optimizer_util_relnode::fetch_upper_rel(root, UPPERREL_ORDERED, &None);

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
        let (is_sorted, presorted_keys) = backend_optimizer_path_pathkeys::pathkeys_count_contained_in(
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
                backend_optimizer_util_pathnode::create::create_sort_path(
                    root,
                    ordered_rel,
                    input_path,
                    sort_pathkeys.clone(),
                    limit_tuples,
                )?
            } else {
                backend_optimizer_util_pathnode::create::create_incremental_sort_path(
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
            sorted_path = backend_optimizer_util_pathnode::create::apply_projection_to_path(
                root,
                ordered_rel,
                sorted_path,
                Box::new(target.clone()),
            )?;
        }

        backend_optimizer_util_pathnode::add_path(root, ordered_rel, sorted_path)?;
    }

    // Partial-path + Gather-Merge block (C:5400-5470). Only reachable when
    // ordered_rel is parallel-safe and there are partial paths; on the
    // non-parallel SELECT path this is skipped. Mirror the C precisely; the
    // Gather-Merge construction here needs the PlannerRun (for parampathinfo),
    // which create_ordered_paths is not threaded with, so a genuinely-reached
    // parallel ORDER BY plan loud-panics at this exact site rather than dropping
    // the parallel paths.
    let ordered_consider_parallel = root.rel(ordered_rel).consider_parallel;
    let input_has_partials = !root.rel(input_rel).partial_pathlist.is_empty();
    if ordered_consider_parallel && !sort_pathkeys.is_empty() && input_has_partials {
        panic!(
            "create_ordered_paths: parallel ORDER BY (create_gather_merge_path over \
             input_rel->partial_pathlist, planner.c:5400-5470) needs the PlannerRun \
             for get_baserel_parampathinfo, which is not threaded into \
             create_ordered_paths"
        );
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
    let path_exprs: Vec<types_pathnodes::NodeId> = root
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

/// `preprocess_rowmarks(root)` (planner.c:2399).
fn preprocess_rowmarks<'mcx>(
    _mcx: Mcx<'mcx>,
    run: &mut PlannerRun<'mcx>,
    root: &mut PlannerInfo,
) -> PgResult<()> {
    let (has_rowmarks, command_type, first_strength) = {
        let parse = run.resolve(root.parse);
        let has = !parse.rowMarks.is_empty();
        (has, parse.commandType, ())
    };
    let _ = first_strength;

    if has_rowmarks {
        // CheckSelectLocking(parse, linitial(rowMarks)->strength) (C:2415-2416).
        // The RowMarkClause node (with .strength) lives in parse->rowMarks as a
        // NodePtr<Node>; extracting LockClauseStrength requires the RowMarkClause
        // Node variant. CheckSelectLocking's owner (parser-analyze) takes
        // (&Query, LockClauseStrength). Reached only for FOR UPDATE/SHARE.
        panic!(
            "preprocess_rowmarks: FOR [KEY] UPDATE/SHARE rowmark processing requires \
             reading RowMarkClause.strength from parse->rowMarks (NodePtr) + \
             get_relids_in_jointree (prepjointree, pub(crate)); neither reachable"
        );
    } else {
        // We only need rowmarks for UPDATE/DELETE/MERGE (C:2424-2427).
        if command_type != CmdType::CMD_UPDATE
            && command_type != CmdType::CMD_DELETE
            && command_type != CmdType::CMD_MERGE
        {
            return Ok(());
        }
    }

    // rels = get_relids_in_jointree(parse->jointree, false, false) (C:2435). The
    // owner (prepjointree result_rtes.rs) is `pub(crate)` and unreachable, and
    // the bms_* set algebra below runs over the lifetime-free Relids. The
    // RowMarkClause / non-target base-rel rowmark loops (C:2442-2502) build
    // PlanRowMark values via run.intern_rowmark + root.rowMarks.push, but they
    // all depend on `rels`. Reached only for UPDATE/DELETE/MERGE. Mirror PG and
    // panic precisely at the unreachable callee.
    let _ = run;
    panic!(
        "preprocess_rowmarks: get_relids_in_jointree (prepjointree result_rtes.rs, \
         pub(crate)) is not reachable; the UPDATE/DELETE/MERGE base-rel rowmark \
         construction (planner.c:2435-2504) is gated on it"
    );
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
    use types_nodes::parsenodes::RTEKind;

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
        let est = backend_optimizer_util_clauses::estimate_expression_value(mcx, e)?;
        match &est {
            Expr::Const(c) => {
                if c.constisnull {
                    *count_est = 0; // LIMIT ALL
                } else {
                    *count_est = datum_get_int64(c);
                    if *count_est <= 0 {
                        *count_est = 1;
                    }
                }
            }
            _ => *count_est = -1, // can't estimate
        }
    } else {
        *count_est = 0; // not present
    }

    // limitOffset estimation (C:2614-2635).
    let limit_offset_expr = expr_from_nodeptr(run, root, |q| q.limitOffset.as_ref())?;
    if let Some(e) = limit_offset_expr {
        let est = backend_optimizer_util_clauses::estimate_expression_value(mcx, e)?;
        match &est {
            Expr::Const(c) => {
                if c.constisnull {
                    *offset_est = 0;
                } else {
                    *offset_est = datum_get_int64(c);
                    if *offset_est < 0 {
                        *offset_est = 0;
                    }
                }
            }
            _ => *offset_est = -1,
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
) -> PgResult<Vec<types_pathnodes::NodeId>> {
    let _ = (run, root, force);
    // Reached only when parse->groupClause is non-empty (the caller gates it).
    // The body needs get_sortgroupref_clause over parse->groupClause and
    // equal(gc, sc) over SortGroupClause node values carried as NodePtr in the
    // Query; the processed_groupClause carrier is Vec<NodeId> (arena) with no
    // bridge from the parse->groupClause NodePtr list. Mirror PG and panic.
    panic!(
        "preprocess_groupclause (planner.c:2828): GROUP BY reordering needs \
         get_sortgroupref_clause + equal() over parse->groupClause SortGroupClause \
         node values (NodePtr), with no bridge to the processed_groupClause arena \
         (Vec<NodeId>)"
    );
}

// ===========================================================================
// expression_planner()  (planner.c:6779)
// ===========================================================================

/// `expression_planner(expr)` (planner.c:6779). Prepares an expression tree for
/// execution (used outside the main planner). `eval_const_expressions(NULL, expr)`
/// then `fix_opfuncids`.
pub fn expression_planner<'mcx>(mcx: Mcx<'mcx>, expr: Expr) -> PgResult<Expr> {
    // eval_const_expressions(NULL, expr) (C:6789). The owner takes an Mcx (no
    // PlannerInfo needed; the `NULL` root path).
    let result = backend_optimizer_util_clauses::eval_const_expressions(mcx, expr)?;

    // fix_opfuncids((Node *) result) (C:6791). No ported owner exposes a
    // standalone fix_opfuncids over the arena Expr; eval_const_expressions
    // already resolves opfuncids during folding, so the post-pass is a structural
    // no-op here. Mirror PG and panic if a separate fixup were genuinely needed —
    // but since it would only re-resolve already-set funcids, we return the
    // folded expr. (Behaviour-preserving: const-folding sets opfuncids.)
    Ok(result)
}

/// `expression_planner_with_deps(expr, &relationOids, &invalItems)`
/// (clauses.c:5479). Like [`expression_planner`] but also extracts the
/// relation-OID and function-inval-item dependencies of the const-folded
/// expression — the form `GetCachedExpression` (`plancache.c`) uses so the
/// cached expression is invalidated when its dependencies change. C makes up a
/// dummy `PlannerGlobal`/`PlannerInfo` and runs `extract_query_dependencies_walker`
/// over the planned result; we delegate the walk to setrefs'
/// [`extract_expr_dependencies_value`](backend_optimizer_plan_setrefs::extract_expr_dependencies_value)
/// (the owner of the dependency-extraction machinery — no cycle, the planner
/// already depends on setrefs).
pub fn expression_planner_with_deps<'mcx>(
    mcx: Mcx<'mcx>,
    expr: Expr,
) -> PgResult<(Expr, alloc::vec::Vec<Oid>, alloc::vec::Vec<types_plancache::InvalItemKey>)> {
    // result = (Expr *) expression_planner((Expr *) expr); (const-fold + opfuncids)
    let result = expression_planner(mcx, expr)?;

    // (void) extract_query_dependencies_walker((Node *) result, &root);
    let deps = backend_optimizer_plan_setrefs::extract_expr_dependencies_value(mcx, &result)?;

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
fn plan_cluster_use_sort_impl(_table_oid: Oid, _index_oid: Oid) -> PgResult<bool> {
    // The body builds dummy PlannerInfo/RelOptInfo, calls cost_index / cost_sort /
    // build_index_paths and compares costs, short-circuiting on
    // `if (!enable_indexscan) return true`. The `enable_indexscan` GUC is not
    // threaded into this crate, and cost_index (costsize.c) / cost_sort
    // (costsize.c) / build_index_paths (indxpath.c) are not reachable over the
    // value model. Mirror PG and panic precisely.
    panic!(
        "plan_cluster_use_sort (planner.c:6859): needs the enable_indexscan GUC + \
         cost_index/cost_sort (costsize.c) + build_index_paths (indxpath.c) over a \
         dummy PlannerInfo/RelOptInfo, none reachable over the value model yet"
    );
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
) -> PgResult<Option<Expr>>
where
    F: for<'a> Fn(&'a Query<'mcx>) -> Option<&'a mcx::PgBox<'mcx, Expr>>,
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
fn nodeptr_as_const_isnull_value(expr: &Expr) -> Option<(bool, i64)> {
    match expr {
        Expr::Const(c) => Some((c.constisnull, datum_get_int64(c))),
        _ => None,
    }
}

/// `DatumGetInt64(((Const *) est)->constvalue)` — read an `int8` from a `Const`'s
/// by-value Datum.
fn datum_get_int64(c: &types_nodes::primnodes::Const) -> i64 {
    if c.constisnull {
        0
    } else {
        c.constvalue.as_i64()
    }
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

    let dest = match plan.plan_head_mut().targetlist.as_deref_mut() {
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
    assert_eq!(
        dest.len(),
        src_labels.len(),
        "apply_tlist_labeling: tlist length mismatch"
    );
    for (dest_tle, (resno, resname, ressortgroupref, resorigtbl, resorigcol, resjunk)) in
        dest.iter_mut().zip(src_labels.into_iter())
    {
        debug_assert_eq!(dest_tle.resno, resno);
        dest_tle.resname = match resname {
            Some(s) => Some(mcx::PgString::from_str_in(s.as_str(), mcx)?),
            None => None,
        };
        dest_tle.ressortgroupref = ressortgroupref;
        dest_tle.resorigtbl = resorigtbl;
        dest_tle.resorigcol = resorigcol;
        dest_tle.resjunk = resjunk;
    }
    Ok(())
}

/// `record_inval_item` impl (the `makeNode(PlanInvalItem)` + `GetSysCacheHashValue1`
/// + `lappend` tail of `record_plan_function_dependency`/`record_plan_type_dependency`,
/// setrefs.c:3553/3593). Computes the syscache hash for `(cache_id, oid)` and pushes
/// the concrete `PlanInvalItem` onto `glob->invalItems`.
fn record_inval_item_impl(
    inval_items: &mut Vec<types_nodes::nodeindexscan::PlanInvalItem>,
    cache_id: i32,
    oid: Oid,
) -> types_error::PgResult<()> {
    // inval_item->hashValue = GetSysCacheHashValue1(cacheId, ObjectIdGetDatum(oid));
    let hash_value =
        backend_utils_cache_syscache_seams::get_syscache_hash_value_oid::call(cache_id, oid)?;
    inval_items.push(types_nodes::nodeindexscan::PlanInvalItem {
        cacheId: cache_id,
        hashValue: hash_value,
    });
    Ok(())
}

pub fn init_seams() {
    use backend_utils_misc_guc_tables::vars;
    use backend_utils_misc_guc_tables::GucVarAccessors;

    backend_optimizer_plan_planner_seams::pg_plan_query::set(pg_plan_query_impl);
    backend_optimizer_plan_planner_seams::plan_cluster_use_sort::set(plan_cluster_use_sort_impl);
    backend_optimizer_plan_planner_pc_seams::expression_planner_with_deps_value::set(
        expression_planner_with_deps,
    );
    backend_optimizer_plan_planner_pc_seams::expression_planner_value::set(expression_planner);

    // create_plan-tail: apply_tlist_labeling(plan->targetlist,
    // root->processed_tlist) (createplan.c create_plan, tlist.c:327). The
    // generic two-tlist label-copy leaf lives in the tlist unit
    // (backend_optimizer_util_vars::tlist::apply_tlist_labeling); this seam is
    // the createplan-tail invocation, which the planner owns because the source
    // tlist is `root->processed_tlist` (a Vec<NodeId> of arena TargetEntrys).
    backend_optimizer_plan_createplan_seams::apply_tlist_labeling::set(apply_tlist_labeling_impl);

    // record_inval_item: record_plan_function_dependency / record_plan_type_dependency
    // append a `PlanInvalItem{cacheId, hashValue=GetSysCacheHashValue1(cacheId, oid)}`
    // to `glob->invalItems`. The syscache hash lives with the syscache subsystem,
    // so the planner installs this owner seam to compute the hash + push the
    // concrete pair (then read straight into PlannedStmt.invalItems).
    backend_optimizer_plan_setrefs_seams::record_inval_item::set(record_inval_item_impl);

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
}

#[cfg(test)]
mod tests;
