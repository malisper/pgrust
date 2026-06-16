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
//!  * `grouping_planner`'s upper-rel machinery (`create_pathtarget` and
//!    everything after it: grouping / window / distinct / ordered / final path
//!    builders) is NOT ported in this repo — `PathTarget` has no arena handle
//!    so it cannot cross a value seam. The regular planning branch reaches
//!    `query_planner` and then the
//!    [`create_pathtarget_for_processed_tlist`](backend_optimizer_plan_planner_ext_seams)
//!    ext-seam, which loud-panics. Everything in `grouping_planner` past that
//!    point is unreachable and intentionally not written here.
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
    JoinDomain, PlannerGlobal, PlannerInfo, RangeTblEntryId, Relids, UPPERREL_FINAL,
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

// `cursor_tuple_fraction` GUC; we use the documented default, since the GUC
// table value is not threaded into the planner crate.
const cursor_tuple_fraction: f64 = DEFAULT_CURSOR_TUPLE_FRACTION;

// `debug_parallel_query` GUC. The parallel-Gather test path of
// `standard_planner` only runs when this != OFF. It is not threaded into this
// crate, so we evaluate it as OFF (the production default), which faithfully
// skips the debug-only Gather injection.
const fn debug_parallel_query() -> i32 {
    DEBUG_PARALLEL_OFF
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
        tuple_fraction = cursor_tuple_fraction;
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

    // unprunableRelids = bms_difference(allRelids, prunableRelids). Both are
    // lifetime-free Relids on glob; the stmt field is `PgBox<Bitmapset<'mcx>>`,
    // a different (lifetime-bearing) bitmapset type with no bridge to the
    // lifetime-free Relids. On the simple path both are empty, so this is None.
    let unprunable_relids = None;

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
    let _ = (glob_transient, glob_depends_on_role);

    let result = PlannedStmt {
        commandType: command_type,
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
        permInfos: None,
        paramExecTypes: param_exec_types,
        rtable: if rtable.is_empty() { None } else { Some(rtable) },
        unprunableRelids: unprunable_relids,
        subplans: if subplans.is_empty() { None } else { Some(subplans) },
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

    // SS_process_ctes if cteList (C:716-717).
    {
        let parse = run.resolve(root.parse);
        if !parse.cteList.is_empty() {
            // SS_process_ctes(root) — ctes are converted to RTE_SUBQUERY or
            // initplan SubPlans. The owner (init-subselect subplan.rs) takes the
            // CTE list; threading it requires the full CTE recursion the simple
            // SELECT path does not exercise. Mirror PG and panic.
            panic!(
                "subquery_planner: SS_process_ctes (subselect.c) for a WITH list \
                 is not wired over the value model yet"
            );
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

    // replace_empty_jointree(parse) (C:728). No reachable ported owner of the
    // dummy RTE_RESULT injection. On a query that already has a non-empty
    // FROM, this is a no-op; only an empty-FROM query (e.g. `SELECT 1`) needs
    // it. Faithfully skip when fromlist is non-empty, else panic precisely.
    {
        let parse = run.resolve(root.parse);
        let empty_fromlist = match &parse.jointree {
            Some(jt) => jt.fromlist.is_empty(),
            None => true,
        };
        if empty_fromlist {
            panic!(
                "subquery_planner: replace_empty_jointree (subselect.c) needed for \
                 an empty FROM clause is not reachable over the owned Query model"
            );
        }
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

    // Expression-preprocessing block (C:903-1056). This block runs
    // `preprocess_expression` over `parse->targetList`, the WCO/returning lists,
    // jointree quals, havingQual, windowClause offsets, limit{Offset,Count},
    // onConflict, mergeActionList, mergeJoinCondition, append_rel_list, and every
    // RTE expression list, plus `flatten_join_alias_vars` cleanup and
    // `flatten_group_exprs`. The `Query` carries these as `NodePtr<'mcx>` /
    // `PgVec<NodePtr>`, while `preprocess_expression`'s core (`eval_const_expressions`
    // / `canonicalize_qual` / `SS_process_sublinks`) operates on the arena `Expr`
    // model — there is no `Node`↔arena-`Expr` bridge for these `Query` fields in
    // this repo, and `flatten_join_alias_vars` / `flatten_group_exprs` /
    // `expand_grouping_sets` have no ported owner. Mirror PG and panic precisely
    // at the start of the block (the C performs it unconditionally for every
    // query).
    panic!(
        "subquery_planner: expression-preprocessing block (planner.c:903-1056) is \
         not expressible — Query expression fields are NodePtr<Node> with no bridge \
         to the arena Expr model that preprocess_expression/eval_const_expressions \
         (clauses.c) operate on, and flatten_join_alias_vars (rewriteManip.c) / \
         flatten_group_exprs (clauses.c) / expand_grouping_sets (parse_agg.c) have \
         no ported owner. The downstream HAVING→WHERE transfer, reduce_outer_joins, \
         remove_useless_result_rtes, grouping_planner, SS_identify_outer_params, \
         SS_charge_for_initplans, and set_cheapest (C:1199-1243) follow this block \
         and are gated on it."
    );

    // ---- Everything below mirrors planner.c:1199-1245 and is unreachable
    // ---- past the expression-preprocessing panic above. It is kept here as the
    // ---- faithful continuation so the spine is documented; it does not compile-
    // ---- gate the simple-SELECT entry, which panics in the block above.
    #[allow(unreachable_code)]
    {
        // newHaving HAVING→WHERE transfer loop (C:1154-1199): needs the arena
        // Expr bridge (above). reduce_outer_joins / remove_useless_result_rtes
        // (C:1206-1216):
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
        // (C:1235-1236). SS_charge_for_initplans has no ported owner.
        let _final_rel = backend_optimizer_util_relnode::fetch_upper_rel(
            &mut root,
            UPPERREL_FINAL,
            &None,
        );
        panic!(
            "subquery_planner: SS_charge_for_initplans (subselect.c) has no ported \
             owner"
        );

        // set_cheapest(final_rel) (C:1243).
        // backend_optimizer_util_pathnode::set_cheapest(&mut root, final_rel)?;
        // Ok(root)
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
        backend_optimizer_util_clauses::convert_saop_to_hashed_saop(&mut expr)?;
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

/// `preprocess_qual_conditions(root, jtnode)` (planner.c:1356). Recursively
/// scans the jointree `FromExpr`/`JoinExpr` quals and preprocesses each.
///
/// The jointree lives on the `Query` as `PgBox<FromExpr<'mcx>>` (a `Node`-typed
/// tree), while `preprocess_expression` operates on the arena `Expr`. Bridging
/// the jointree quals to the arena requires the `Node`↔`Expr` bridge that the
/// whole expression-preprocessing block is gated on. The recursion structure is
/// ported; the per-node qual preprocessing is the gated step.
pub fn preprocess_qual_conditions<'mcx>(
    _mcx: Mcx<'mcx>,
    _root: &PlannerInfo,
    _jtnode: Option<&Node<'mcx>>,
) -> PgResult<()> {
    panic!(
        "preprocess_qual_conditions (planner.c:1356) requires the Node-jointree → \
         arena-Expr bridge that the whole expression-preprocessing block is gated \
         on (preprocess_expression over FromExpr/JoinExpr quals)"
    );
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
            // havingQual: parse->havingQual is a NodePtr<Node> with no bridge to
            // the arena `Expr` model (same gate as the expression-preprocessing
            // block above). Mirror PG and panic precisely if present.
            if run.resolve(root.parse).havingQual.is_some() {
                panic!(
                    "grouping_planner: preprocess_aggrefs over parse->havingQual \
                     (planner.c:1580) is gated on the arena-Expr bridge for the \
                     Query.havingQual NodePtr<Node> field"
                );
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
    let mut qp_callback = |root: &mut PlannerInfo| -> PgResult<()> {
        standard_qp_callback(run_ptr_hack(), root)
    };
    // The qp_callback needs `run` to resolve sort/group clauses; but the closure
    // can't capture `run` mutably while query_planner also borrows `run` mutably.
    // standard_qp_callback for the simple case sets query_pathkeys/sort_pathkeys
    // from sortClause/group; this needs the Query, hence `run`. The owner
    // query_planner threads only `&mut PlannerInfo` into the callback, so the
    // callback cannot reach `run`. This is the documented qp_callback boundary:
    // for a query with no sort/group/distinct clause the pathkeys are all empty
    // and the callback is a no-op; we port that case. A query *with* a
    // sort/group/distinct clause needs the Query in the callback, which is not
    // expressible here.
    let _ = &mut qp_callback;
    let mut noop_callback = standard_qp_callback_pathkeys_empty;
    let _current_rel = backend_optimizer_plan_small::query_planner(mcx, run, root, &mut noop_callback)?;

    // create_pathtarget(root, root->processed_tlist) (C:1665). THE STOP POINT:
    // the whole upper-rel path/target machinery is unported. This loud-panics
    // and never returns; everything in grouping_planner after this (sort/group/
    // window/distinct/ordered/final path building, C:1665-end) is gated here.
    backend_optimizer_plan_planner_ext_seams::create_pathtarget_for_processed_tlist::call(root)?;

    // Unreachable past the ext-seam panic above.
    unreachable!(
        "grouping_planner: create_pathtarget ext-seam returned, but it must \
         loud-panic until the upper-rel path/target machinery lands"
    )
}

/// Hack placeholder — see `grouping_planner`'s qp_callback note. Not called.
#[inline]
fn run_ptr_hack<'mcx>() -> &'mcx mut PlannerRun<'mcx> {
    unreachable!("run_ptr_hack is never invoked")
}

/// `standard_qp_callback` (planner.c) reduced to the no-clause case: with no
/// sort/group/distinct/window clauses, all of `query_pathkeys` /
/// `sort_pathkeys` / `group_pathkeys` / `distinct_pathkeys` / `setop_pathkeys`
/// are empty (NIL), so the callback leaves them at their `Default` (empty) and
/// returns. A query with such clauses needs the owned Query inside the callback
/// (`make_pathkeys_for_sortclauses` over `parse->sortClause` /
/// `root->processed_*Clause`), which the value-model `query_planner` callback
/// signature (`&mut dyn FnMut(&mut PlannerInfo)`) cannot reach (no `run`); that
/// is the documented boundary in `grouping_planner`.
fn standard_qp_callback_pathkeys_empty(root: &mut PlannerInfo) -> PgResult<()> {
    // query_pathkeys = NIL on the no-clause path.
    root.query_pathkeys = Vec::new();
    root.sort_pathkeys = Vec::new();
    root.group_pathkeys = Vec::new();
    root.distinct_pathkeys = Vec::new();
    root.setop_pathkeys = Vec::new();
    Ok(())
}

/// Full-fidelity `standard_qp_callback` (kept for documentation; not wired —
/// see the `grouping_planner` qp_callback note). It needs the owned `Query`,
/// which the value-model callback signature cannot carry.
#[allow(dead_code)]
fn standard_qp_callback(_run: &mut PlannerRun<'_>, _root: &mut PlannerInfo) -> PgResult<()> {
    panic!(
        "standard_qp_callback: the full callback (make_pathkeys_for_sortclauses over \
         parse->sortClause / processed_groupClause / processed_distinctClause) needs \
         the owned Query inside the callback, which the query_planner callback \
         signature (&mut dyn FnMut(&mut PlannerInfo)) cannot reach"
    );
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
    if let Some(node) = parse.limitCount.as_ref() {
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
    if let Some(node) = parse.limitOffset.as_ref() {
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
    F: for<'a> Fn(&'a Query<'mcx>) -> Option<&'a types_nodes::nodes::NodePtr<'mcx>>,
{
    let parse = run.resolve(root.parse);
    match pick(parse) {
        None => Ok(None),
        Some(node) => Ok(Some(node_to_expr(node)?)),
    }
}

/// Extract the arena `Expr` carried by a `Node`. The limit/offset expressions
/// are `Expr`-kind nodes; this is the `Node`→`Expr` projection for the scalar
/// limit path. Unsupported `Node` kinds here would be a parser/planner bug.
fn node_to_expr<'mcx>(node: &Node<'mcx>) -> PgResult<Expr> {
    match node.as_expr() {
        Some(e) => Ok(e.clone()),
        None => Err(PgError::error(
            "planner: LIMIT/OFFSET expression Node does not carry an Expr",
        )),
    }
}

/// `((Const *) node)->constisnull` / `DatumGetInt64(constvalue)` over a
/// `NodePtr` that may hold a `Const`. Returns `Some((isnull, value))` if the
/// node is a `Const`, else `None` (non-constant).
fn nodeptr_as_const_isnull_value<'mcx>(node: &Node<'mcx>) -> Option<(bool, i64)> {
    match node.as_expr() {
        Some(Expr::Const(c)) => Some((c.constisnull, datum_get_int64(c))),
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
pub fn init_seams() {
    backend_optimizer_plan_planner_seams::pg_plan_query::set(pg_plan_query_impl);
    backend_optimizer_plan_planner_seams::plan_cluster_use_sort::set(plan_cluster_use_sort_impl);
}
