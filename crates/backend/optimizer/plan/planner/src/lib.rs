//! planner.c spine for the no-FROM lane (`SELECT <exprs>`) plus the minimal
//! createplan/setrefs/planmain/pathnode/relnode/tlist/costsize/prep slices it
//! needs; everything past the lane is a named panic citing C fn + lane.

pub mod allpaths;
pub mod analyzejoins;
pub mod clausesel;
pub mod cluster;
pub mod extended_stats;
pub mod costsize;
pub mod createplan;
pub mod equivclass;
pub mod fdwplan;
pub mod geqo;
pub mod flatten_group;
pub mod grouping;
pub mod groupingsets;
pub mod indxpath;
mod tidpath;
mod inherit;
pub mod initsplan;
pub mod joinpath;
pub mod joinrels;
pub mod pathkeys;
pub mod placeholder;
pub mod planagg;
pub mod orclauses;
pub mod pathnode;
pub mod plancat;
pub mod partprune;
pub mod like_support;
mod m5_partwise;
pub mod m5_suppress;
pub mod multirangetypes_selfuncs;
pub mod network_selfuncs;
pub mod array_selfuncs;
pub mod intarray_selfuncs;
pub mod rangetypes_selfuncs;
pub mod selfuncs;
pub(crate) mod syscache_memo;
pub mod ts_selfuncs;
pub mod planmain;
pub mod prep;
pub mod prepunion;
pub mod prepqual;
pub mod predtest;
pub mod prepjointree;
mod pushdown;
pub mod prepagg;
pub mod relnode;
pub mod run;
pub mod setrefs;
pub mod srf;
pub mod cte;
pub mod subquery;
pub mod window;
pub mod subselect;
pub mod paramassign;

#[cfg(test)]
mod tests;

use mcx::Mcx;
use types_error::PgResult;
use types_nodes::list::NodeList;
use types_nodes::nodes_enums::CmdType;
use types_nodes::parsenodes::Query;
use types_nodes::plannodes::PlannedStmt;
use types_nodes::Node;
use types_portal::{ParamListHandle, CURSOR_OPT_FAST_PLAN, CURSOR_OPT_PARALLEL_OK};
use types_pathnodes::PtId;

use crate::createplan::create_plan;
use crate::pathnode::get_cheapest_fractional_path;
use crate::planmain::fetch_final_rel;
use crate::run::PlannerRun;
use crate::setrefs::set_plan_references;
use crate::subquery::subquery_planner;

const PROPARALLEL_UNSAFE: i8 = b'u' as i8;

const PGJIT_PERFORM: i32 = 1 << 0;
const PGJIT_OPT3: i32 = 1 << 1;
const PGJIT_INLINE: i32 = 1 << 2;
const PGJIT_EXPR: i32 = 1 << 3;
const PGJIT_DEFORM: i32 = 1 << 4;

// GUC backing this crate reads (double-install panics flag future homes).
pub mod gucs {
    guc_tables::session_guc_cluster!(PlannerGucs, PLANNER_GUCS:
        (cursor_tuple_fraction_cell, f64, cursor_tuple_fraction, set_cursor_tuple_fraction, (0.1) as f64),
        // pgrust copy-and-patch JIT defaults (was C's 100000/500000/500000);
        // must match guc_tables::tables boot_vals. 200 amortizes the WHOLE
        // per-execution compile bill over ONE execution: expression kernels
        // are estate-owned (no cross-execution cache — every EXECUTE of a
        // prepared statement recompiles ~4-6 kernels at ~4-5us each), so the
        // threshold must clear ~20us of compile per execution, not the ~5us
        // one-kernel figure the earlier default of 10 was derived from.
        // optimize/inline are inert under copy-and-patch (no LLVM phase) so
        // they equal jit_above_cost. Full derivation in guc_tables/tables.rs
        // + docs/design/jit-parallel-defaults.md.
        (jit_above_cost_cell, f64, jit_above_cost, set_jit_above_cost, (200.0) as f64),
        (jit_optimize_above_cost_cell, f64, jit_optimize_above_cost, set_jit_optimize_above_cost, (200.0) as f64),
        (jit_inline_above_cost_cell, f64, jit_inline_above_cost, set_jit_inline_above_cost, (200.0) as f64),
        (from_collapse_limit_cell, i32, from_collapse_limit, set_from_collapse_limit, 8),
        (join_collapse_limit_cell, i32, join_collapse_limit, set_join_collapse_limit, 8),
        (constraint_exclusion_cell, i32, constraint_exclusion, set_constraint_exclusion, guc_tables::consts::CONSTRAINT_EXCLUSION_PARTITION),
        (debug_parallel_query_cell, i32, debug_parallel_query, set_debug_parallel_query, guc_tables::consts::DEBUG_PARALLEL_OFF),
        (max_parallel_workers_per_gather_cell, i32, max_parallel_workers_per_gather, set_max_parallel_workers_per_gather, 4),
        (jit_enabled_cell, bool, jit_enabled, set_jit_enabled, true),
        (enable_self_join_elimination_cell, bool, enable_self_join_elimination, set_enable_self_join_elimination, true),
        (jit_expressions_cell, bool, jit_expressions, set_jit_expressions, true),
        (jit_tuple_deforming_cell, bool, jit_tuple_deforming, set_jit_tuple_deforming, true),
    );

    pub use ::costsize::gucs::*;
    
    
    
    
    
    
    
    
    
    
    
    
    
}

pub fn init_seams() {
    planner_seams::planner::set(|mcx, parse, query_string, cursor_options, bound_params| {
        planner(mcx, parse, query_string, cursor_options, bound_params)
    });
    // find_simplified_clause's gate (rangetypes.c): volatile / subplan /
    // >10*cpu_operator_cost elemExprs must not be evaluated twice.
    clauses_seams::expr_safe_to_evaluate_twice::set(|node| {
        if clauses::contain_volatile_functions(node)? || clauses::contain_subplans(node)? {
            return Ok(false);
        }
        let cost = costsize::cost_qual_eval_node(None, node)?;
        Ok(cost.startup + cost.per_tuple <= 10.0 * costsize::gucs::cpu_operator_cost())
    });
    planner_seams::clauselist_selectivity::set(crate::clausesel::clauselist_selectivity);
    planner_seams::clause_selectivity::set(crate::clausesel::clause_selectivity);
    planner_seams::make_restrictinfo::set(crate::initsplan::make_restrictinfo);
    planner_seams::amcostestimate::set(crate::selfuncs::amcostestimate);
    planner_seams::estimate_num_groups::set(crate::selfuncs::estimate_num_groups);
    planner_seams::estimate_num_groups_estinfo::set(crate::selfuncs::estimate_num_groups_estinfo);
    planner_seams::estimate_array_length::set(crate::selfuncs::estimate_array_length);
    planner_seams::query_supports_distinctness::set(
        crate::analyzejoins::query_supports_distinctness,
    );
    planner_seams::query_is_distinct_for::set(crate::analyzejoins::query_is_distinct_for);
    planner_seams::make_pathkey_from_sortop::set(crate::pathkeys::make_pathkey_from_sortop);
    planner_seams::pathkey_is_redundant::set(crate::pathkeys::pathkey_is_redundant);
    planner_seams::mergejoinscansel::set(crate::selfuncs::mergejoinscansel);
    planner_seams::estimate_hash_bucket_stats::set(crate::selfuncs::estimate_hash_bucket_stats);
    planner_seams::estimate_multivariate_bucketsize::set(
        crate::selfuncs::estimate_multivariate_bucketsize,
    );
    planner_seams::add_function_cost::set(crate::plancat::add_function_cost);
    indexam_seams::index_can_return::set(crate::plancat::index_can_return);
    planner_seams::get_function_rows::set(crate::plancat::get_function_rows);
    planner_seams::get_rel_data_width::set(crate::plancat::get_rel_data_width);
    planner_seams::match_index_to_operand::set(crate::indxpath::match_index_to_operand);
    planner_seams::generate_join_implied_equalities::set(
        crate::equivclass::generate_join_implied_equalities,
    );
    planner_seams::generate_join_implied_equalities_for_ecs::set(
        crate::equivclass::generate_join_implied_equalities_for_ecs,
    );
    planner_seams::find_derived_clause_for_ec_member::set(
        crate::equivclass::find_derived_clause_for_ec_member,
    );
    planner_seams::distribute_restrictinfo_to_rels::set(
        crate::initsplan::distribute_restrictinfo_to_rels,
    );
    planner_seams::build_implied_join_equality::set(crate::initsplan::build_implied_join_equality);
    planner_seams::process_implied_equality::set(crate::initsplan::process_implied_equality);
    planner_seams::pull_var_nodes::set(crate::initsplan::pull_var_nodes);
    planner_seams::pull_varnos_relids::set(crate::initsplan::pull_varnos_relids);
    planner_seams::add_vars_to_targetlist::set(crate::initsplan::add_vars_to_targetlist);
    planner_seams::add_vars_to_attr_needed::set(crate::initsplan::add_vars_to_attr_needed);
    planner_seams::commute_restrictinfo::set(crate::initsplan::commute_restrictinfo);
    planner_seams::remove_rel_from_restrictinfo::set(
        crate::analyzejoins::remove_rel_from_restrictinfo,
    );
    planner_seams::adjust_appendrel_attrs::set(crate::inherit::adjust_appendrel_attrs);
    planner_seams::adjust_appendrel_attrs_multi::set(crate::inherit::adjust_appendrel_attrs_multi);
    planner_seams::adjust_appendrel_attrs_multilevel::set(
        crate::inherit::adjust_appendrel_attrs_multilevel,
    );
    planner_seams::adjust_child_rinfo_multilevel::set(
        crate::inherit::adjust_child_rinfo_multilevel,
    );
    planner_seams::expr_collation::set(crate::pathkeys::expr_collation);
    planner_seams::is_dummy_rel::set(crate::joinrels::is_dummy_rel);
    planner_seams::make_opclause::set(crate::like_support::make_opclause);
    planner_seams::match_pattern_prefix::set(crate::like_support::match_pattern_prefix);
    planner_seams::predicate_implied_by::set(crate::predtest::predicate_implied_by);
    planner_seams::build_index_pathkeys::set(crate::pathkeys::build_index_pathkeys);
    planner_seams::truncate_useless_pathkeys::set(crate::pathkeys::truncate_useless_pathkeys);
    planner_seams::inet_ref::set(crate::network_selfuncs::inet_ref);
    use guc_tables::GucVarAccessors;
    guc_tables::vars::cursor_tuple_fraction.install(GucVarAccessors {
        get: gucs::cursor_tuple_fraction,
        set: gucs::set_cursor_tuple_fraction,
    });
    guc_tables::vars::debug_parallel_query.install(GucVarAccessors {
        get: gucs::debug_parallel_query,
        set: gucs::set_debug_parallel_query,
    });
    guc_tables::vars::max_parallel_workers_per_gather.install(GucVarAccessors {
        get: gucs::max_parallel_workers_per_gather,
        set: gucs::set_max_parallel_workers_per_gather,
    });
    guc_tables::vars::jit_enabled
        .install(GucVarAccessors { get: gucs::jit_enabled, set: gucs::set_jit_enabled });
    guc_tables::vars::jit_above_cost
        .install(GucVarAccessors { get: gucs::jit_above_cost, set: gucs::set_jit_above_cost });
    guc_tables::vars::jit_optimize_above_cost.install(GucVarAccessors {
        get: gucs::jit_optimize_above_cost,
        set: gucs::set_jit_optimize_above_cost,
    });
    guc_tables::vars::jit_inline_above_cost.install(GucVarAccessors {
        get: gucs::jit_inline_above_cost,
        set: gucs::set_jit_inline_above_cost,
    });
    guc_tables::vars::jit_expressions
        .install(GucVarAccessors { get: gucs::jit_expressions, set: gucs::set_jit_expressions });
    guc_tables::vars::from_collapse_limit.install(GucVarAccessors {
        get: gucs::from_collapse_limit,
        set: gucs::set_from_collapse_limit,
    });
    guc_tables::vars::join_collapse_limit.install(GucVarAccessors {
        get: gucs::join_collapse_limit,
        set: gucs::set_join_collapse_limit,
    });
    guc_tables::vars::constraint_exclusion.install(GucVarAccessors {
        get: gucs::constraint_exclusion,
        set: gucs::set_constraint_exclusion,
    });
    guc_tables::vars::jit_tuple_deforming.install(GucVarAccessors {
        get: gucs::jit_tuple_deforming,
        set: gucs::set_jit_tuple_deforming,
    });
    guc_tables::vars::enable_self_join_elimination.install(GucVarAccessors {
        get: gucs::enable_self_join_elimination,
        set: gucs::set_enable_self_join_elimination,
    });
    // The 7 GEQO GUCs (enable_geqo, geqo_threshold, geqo_effort,
    // geqo_pool_size, geqo_generations, geqo_selection_bias, geqo_seed) — read
    // for real by the genetic optimizer; inert until this install ran.
    geqo::install_gucs();
}

// planner_hook (hook-surface.md section 2): enter/leave pair so a consumer
// (pg_stat_statements) can time track_planning and track nesting depth. The
// leave fires on the error path too (C PG_FINALLY parity), with ok=false so
// no stats are stored. Empty by default (S1 ships no consumer).
seam_core::tap!(pub fn tap_planner_enter(query_id: i64));
seam_core::tap!(
    pub fn tap_planner_leave<'a>(
        ok: bool,
        query_id: i64,
        stmt_location: i32,
        stmt_len: i32,
        query_string: &'a str,
    )
);

pub fn planner<'mcx>(
    mcx: Mcx<'mcx>,
    parse: &'mcx mut Query<'mcx>,
    query_string: &str,
    cursor_options: i32,
    bound_params: ParamListHandle,
) -> PgResult<PlannedStmt<'mcx>> {
    let (query_id, stmt_location, stmt_len) = (parse.queryId, parse.stmt_location, parse.stmt_len);
    tap_planner_enter::call_if(|f| f(query_id));
    let result = standard_planner(mcx, parse, query_string, cursor_options, bound_params);
    tap_planner_leave::call_if(|f| {
        f(result.is_ok(), query_id, stmt_location, stmt_len, query_string)
    });
    let result = result?;
    backend_status_seams::pgstat_report_plan_id::call(result.planId, false);
    Ok(result)
}

pub fn standard_planner<'mcx>(
    mcx: Mcx<'mcx>,
    parse: &'mcx mut Query<'mcx>,
    _query_string: &str,
    cursor_options: i32,
    bound_params: ParamListHandle,
) -> PgResult<PlannedStmt<'mcx>> {
    // Step-2 cost-shadow EXPLAIN hygiene (m5_suppress::cost_shadow): a stale
    // sample from an earlier planning must never annotate this query's
    // EXPLAIN. One cached-bool load when PGRUST_M5_COST_EXPLAIN is off (the
    // m5 probe's own inertness idiom).
    m5_suppress::cost_shadow::clear_last_sample();

    // C frees the planner's data with one context reset; the run is forgotten
    // (drop glue never runs), success or error — mcx reclaims it wholesale.
    let mut run_owner = mcx::ArenaForget::new(PlannerRun::new(mcx));
    let mut run = &mut *run_owner;
    run.glob.bound_params = bound_params;

    // The raw-tree hazard scan (planner.c:349-353) runs at subquery_planner
    // entry, before any sublink subquery is planned.
    run.glob.max_parallel_hazard = PROPARALLEL_UNSAFE;
    run.assess_parallel = (cursor_options & CURSOR_OPT_PARALLEL_OK) != 0
        && init_small::globals::IsUnderPostmaster()
        && parse.commandType == CmdType::CMD_SELECT
        && !parse.hasModifyingCTE
        && gucs::max_parallel_workers_per_gather() > 0
        && !is_parallel_worker();

    let tuple_fraction = if (cursor_options & CURSOR_OPT_FAST_PLAN) != 0 {
        let f = gucs::cursor_tuple_fraction();
        if f >= 1.0 {
            0.0
        } else if f <= 0.0 {
            1e-10
        } else {
            f
        }
    } else {
        0.0
    };

    subquery_planner(&mut run, parse, false, tuple_fraction, None)?;

    let final_rel = fetch_final_rel(&mut run);
    let best_path = get_cheapest_fractional_path(&run, final_rel, tuple_fraction);
    let mut top_plan = create_plan(&mut run, best_path)?;

    // C planner.c:444-451 wraps a SCROLL cursor's plan in Material when
    // !ExecSupportsBackwardScan, so the EXECUTOR can run backwards on demand.
    // DELETED (backward-execution wave B3, cursors inc-2 §6 rider row 2):
    // the executor never runs backwards — every backward cursor read is
    // served by the portal tuplestore (SE13 CURSORS flip), and the run seam
    // refuses backward entry outright (deletion-prep B1). The store gives
    // the same re-read stability the Material wrap existed to provide.
    // Ratified strategy divergence from C: Michael's 2026-07-17
    // SCROLL/WITH-HOLD decision (notes/se-wave10-integration.md §5 item 2).
    // debug_parallel_query test wrap: a single-copy Gather over the whole
    // plan. Under =regress with initPlans present the wrap is skipped (moving
    // the initPlans to the Gather would change EXPLAIN output; C skips too).
    if gucs::debug_parallel_query() != guc_tables::consts::DEBUG_PARALLEL_OFF
        && top_plan.as_plan().expect("plan node").parallel_safe
        && (top_plan.as_plan().expect("plan node").initPlan.is_nil()
            || gucs::debug_parallel_query() != guc_tables::consts::DEBUG_PARALLEL_REGRESS)
    {
        let (tlist, init_plan, startup_cost, total_cost, plan_rows, plan_width) = {
            let tp = top_plan.as_plan().expect("plan node");
            (
                tp.targetlist.clone_in(mcx)?,
                tp.initPlan.clone_in(mcx)?,
                tp.startup_cost,
                tp.total_cost,
                tp.plan_rows,
                tp.plan_width,
            )
        };
        let mut gather = Node::build::<types_nodes::plannodes::Gather>(mcx)?;
        gather.plan.targetlist = tlist;
        gather.plan.qual = NodeList::nil();
        gather.plan.lefttree = Some(top_plan);
        gather.num_workers = 1;
        gather.single_copy = true;
        gather.invisible =
            gucs::debug_parallel_query() == guc_tables::consts::DEBUG_PARALLEL_REGRESS;
        // This Gather has no parallel-aware descendants to signal.
        gather.rescan_param = -1;
        gather.plan.startup_cost = startup_cost + ::costsize::gucs::parallel_setup_cost();
        gather.plan.total_cost = total_cost
            + ::costsize::gucs::parallel_setup_cost()
            + ::costsize::gucs::parallel_tuple_cost() * plan_rows;
        gather.plan.plan_rows = plan_rows;
        gather.plan.plan_width = plan_width;
        gather.plan.parallel_aware = false;
        gather.plan.parallel_safe = false;
        // Transfer initPlans; SS_compute_initplan_cost's total leaves the
        // child (the Gather's costs above already include it).
        let mut initplan_cost = 0.0;
        for sp_node in &init_plan {
            let sp = sp_node.as_sub_plan().expect("initPlan holds SubPlan nodes");
            initplan_cost += sp.startup_cost + sp.per_call_cost;
        }
        gather.plan.initPlan = init_plan;
        // SAFETY: exclusive plan-tree ownership (the tree was just built).
        unsafe {
            top_plan.with_plan_mut(|c| {
                c.initPlan = NodeList::nil();
                c.startup_cost -= initplan_cost;
                c.total_cost -= initplan_cost;
            })
        }
        .expect("plan node");
        run.glob.parallel_mode_needed = true;
        top_plan = gather.seal();
    }
    if !run.glob.param_exec_types.is_nil() {
        // C: subplans are finalized before the main plan (they set the params
        // the main plan's extParam computation validates against).
        debug_assert_eq!(run.subroots.len(), run.glob.subplans.len());
        for i in 0..run.glob.subplans.len() {
            let subplan = run.glob.subplans.nth(i);
            let subroot = &run.subroots[i].root;
            crate::subselect::ss_finalize_plan(&run, subroot, subplan, &subroot.outer_params)?;
        }
        crate::subselect::ss_finalize_plan(&run, &run.root, top_plan, &run.root.outer_params)?;
    }

    debug_assert!(run.glob.finalrtable.is_nil());
    let top_plan = set_plan_references(&mut run, top_plan)?;
    // ... and the subplans, each under its own root (C's forboth over
    // glob->subplans/glob->subroots).
    if !run.glob.subplans.is_nil() {
        let mut fixed_subplans = NodeList::nil();
        for i in 0..run.glob.subplans.len() {
            let subplan = run.glob.subplans.nth(i);
            // Swaps, not replace-with-placeholder: no PlannerInfo ever drops.
            core::mem::swap(&mut run.subroots[i].root, &mut run.root);
            let top_tlist =
                core::mem::replace(&mut run.processed_tlist, run.subroots[i].processed_tlist);
            let fixed = set_plan_references(&mut run, subplan)?;
            core::mem::swap(&mut run.subroots[i].root, &mut run.root);
            run.processed_tlist = top_tlist;
            fixed_subplans.lappend(mcx, fixed)?;
        }
        run.glob.subplans = fixed_subplans;
    }

    let parse = run.parse();
    let total_cost = top_plan.as_plan().expect("plan node").total_cost;
    let mut jit_flags = 0;
    if gucs::jit_enabled()
        && gucs::jit_above_cost() >= 0.0
        && total_cost > gucs::jit_above_cost()
    {
        jit_flags |= PGJIT_PERFORM;
        if gucs::jit_optimize_above_cost() >= 0.0 && total_cost > gucs::jit_optimize_above_cost() {
            jit_flags |= PGJIT_OPT3;
        }
        if gucs::jit_inline_above_cost() >= 0.0 && total_cost > gucs::jit_inline_above_cost() {
            jit_flags |= PGJIT_INLINE;
        }
        if gucs::jit_expressions() {
            jit_flags |= PGJIT_EXPR;
        }
        if gucs::jit_tuple_deforming() {
            jit_flags |= PGJIT_DEFORM;
        }
    }

    let glob = core::mem::replace(&mut run.glob, run::Glob::new());
    let stmt = PlannedStmt {
        commandType: parse.commandType,
        queryId: types_nodes::SyncCell::new(parse.queryId),
        planId: 0,
        hasReturning: !parse.returningList.is_nil(),
        hasModifyingCTE: parse.hasModifyingCTE,
        canSetTag: parse.canSetTag,
        transientPlan: glob.transient_plan,
        dependsOnRole: glob.depends_on_role,
        parallelModeNeeded: glob.parallel_mode_needed,
        jitFlags: jit_flags,
        planTree: Some(top_plan),
        partPruneInfos: glob.part_prune_infos,
        rtable: glob.finalrtable,
        unprunableRelids: glob.all_relids.difference(&glob.prunable_relids, mcx)?,
        permInfos: glob.finalrteperminfos,
        resultRelations: glob.result_relations,
        appendRelations: glob.append_relations,
        // The planner never leaves holes; NULL cells appear only in
        // ExecSerializePlan's worker copy.
        subplans: {
            let mut sp = types_nodes::list::OptNodeList::nil();
            for p in glob.subplans.iter() {
                sp.lappend(mcx, Some(p))?;
            }
            sp
        },
        rewindPlanIDs: glob.rewind_plan_ids,
        rowMarks: glob.finalrowmarks,
        relationOids: glob.relation_oids,
        invalItems: glob.inval_items,
        paramExecTypes: glob.param_exec_types,
        utilityStmt: parse.utilityStmt,
        stmt_location: parse.stmt_location,
        stmt_len: parse.stmt_len,
    };
    Ok(stmt)
}

fn is_parallel_worker() -> bool {
    parallel_seams::is_parallel_worker::call()
}

pub(crate) use clauses::{is_parallel_safe_exprs, is_parallel_safe_opt};
