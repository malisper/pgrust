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
pub mod grouping;
pub mod groupingsets;
pub mod indxpath;
mod inherit;
pub mod initsplan;
pub mod joinpath;
pub mod joinrels;
pub mod pathkeys;
pub mod planagg;
pub mod pathnode;
pub mod plancat;
pub mod like_support;
pub mod multirangetypes_selfuncs;
pub mod network_selfuncs;
pub mod rangetypes_selfuncs;
pub mod selfuncs;
pub mod planmain;
pub mod prep;
pub mod prepunion;
pub mod prepqual;
pub mod predtest;
pub mod prepjointree;
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
use types_portal::{ParamListHandle, CURSOR_OPT_FAST_PLAN, CURSOR_OPT_PARALLEL_OK, CURSOR_OPT_SCROLL};
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
    use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, Ordering};

    macro_rules! real_guc {
        ($cell:ident, $get:ident, $set:ident, $boot:expr) => {
            static $cell: AtomicU64 = AtomicU64::new(($boot as f64).to_bits());
            pub fn $get() -> f64 {
                f64::from_bits($cell.load(Ordering::Relaxed))
            }
            pub fn $set(v: f64) {
                $cell.store(v.to_bits(), Ordering::Relaxed);
            }
        };
    }
    macro_rules! int_guc {
        ($cell:ident, $get:ident, $set:ident, $boot:expr) => {
            static $cell: AtomicI32 = AtomicI32::new($boot);
            pub fn $get() -> i32 {
                $cell.load(Ordering::Relaxed)
            }
            pub fn $set(v: i32) {
                $cell.store(v, Ordering::Relaxed);
            }
        };
    }
    macro_rules! bool_guc {
        ($cell:ident, $get:ident, $set:ident, $boot:expr) => {
            static $cell: AtomicBool = AtomicBool::new($boot);
            pub fn $get() -> bool {
                $cell.load(Ordering::Relaxed)
            }
            pub fn $set(v: bool) {
                $cell.store(v, Ordering::Relaxed);
            }
        };
    }

    pub use ::costsize::gucs::*;
    real_guc!(CURSOR_TUPLE_FRACTION, cursor_tuple_fraction, set_cursor_tuple_fraction, 0.1);
    real_guc!(JIT_ABOVE_COST, jit_above_cost, set_jit_above_cost, 100000.0);
    real_guc!(JIT_OPTIMIZE_ABOVE_COST, jit_optimize_above_cost, set_jit_optimize_above_cost, 500000.0);
    real_guc!(JIT_INLINE_ABOVE_COST, jit_inline_above_cost, set_jit_inline_above_cost, 500000.0);
    int_guc!(JOIN_COLLAPSE_LIMIT, join_collapse_limit, set_join_collapse_limit, 8);
    int_guc!(CONSTRAINT_EXCLUSION, constraint_exclusion, set_constraint_exclusion, guc_tables::consts::CONSTRAINT_EXCLUSION_PARTITION);
    int_guc!(DEBUG_PARALLEL_QUERY, debug_parallel_query, set_debug_parallel_query, guc_tables::consts::DEBUG_PARALLEL_OFF);
    int_guc!(MAX_PARALLEL_WORKERS_PER_GATHER, max_parallel_workers_per_gather, set_max_parallel_workers_per_gather, 2);
    bool_guc!(JIT_ENABLED, jit_enabled, set_jit_enabled, true);
    bool_guc!(JIT_EXPRESSIONS, jit_expressions, set_jit_expressions, true);
    bool_guc!(JIT_TUPLE_DEFORMING, jit_tuple_deforming, set_jit_tuple_deforming, true);
}

pub fn init_seams() {
    planner_seams::planner::set(|mcx, parse, query_string, cursor_options, bound_params| {
        planner(mcx, parse, query_string, cursor_options, bound_params)
    });
    planner_seams::clauselist_selectivity::set(crate::clausesel::clauselist_selectivity);
    planner_seams::clause_selectivity::set(crate::clausesel::clause_selectivity);
    planner_seams::make_restrictinfo::set(crate::initsplan::make_restrictinfo);
    planner_seams::amcostestimate::set(crate::selfuncs::amcostestimate);
    planner_seams::estimate_num_groups::set(crate::selfuncs::estimate_num_groups);
    planner_seams::estimate_num_groups_estinfo::set(crate::selfuncs::estimate_num_groups_estinfo);
    planner_seams::estimate_array_length::set(crate::selfuncs::estimate_array_length);
    planner_seams::mergejoinscansel::set(crate::selfuncs::mergejoinscansel);
    planner_seams::estimate_hash_bucket_stats::set(crate::selfuncs::estimate_hash_bucket_stats);
    planner_seams::estimate_multivariate_bucketsize::set(
        crate::selfuncs::estimate_multivariate_bucketsize,
    );
    planner_seams::add_function_cost::set(crate::plancat::add_function_cost);
    planner_seams::get_function_rows::set(crate::plancat::get_function_rows);
    planner_seams::get_rel_data_width::set(crate::plancat::get_rel_data_width);
    planner_seams::match_index_to_operand::set(crate::indxpath::match_index_to_operand);
    planner_seams::generate_join_implied_equalities::set(
        crate::equivclass::generate_join_implied_equalities,
    );
    planner_seams::generate_join_implied_equalities_for_ecs::set(
        crate::equivclass::generate_join_implied_equalities_for_ecs,
    );
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
}

// planner_hook is absent by design.
pub fn planner<'mcx>(
    mcx: Mcx<'mcx>,
    parse: Query<'mcx>,
    query_string: &str,
    cursor_options: i32,
    bound_params: ParamListHandle,
) -> PgResult<PlannedStmt<'mcx>> {
    let result = standard_planner(mcx, parse, query_string, cursor_options, bound_params)?;
    backend_status_seams::pgstat_report_plan_id::call(result.planId, false);
    Ok(result)
}

pub fn standard_planner<'mcx>(
    mcx: Mcx<'mcx>,
    parse: Query<'mcx>,
    _query_string: &str,
    cursor_options: i32,
    bound_params: ParamListHandle,
) -> PgResult<PlannedStmt<'mcx>> {
    // C frees the planner's data with one context reset; the run is forgotten
    // (drop glue never runs), success or error — mcx reclaims it wholesale.
    let mut run_owner = mcx::ArenaForget::new(PlannerRun::new(mcx));
    let mut run = &mut *run_owner;
    run.glob.bound_params = bound_params;

    // Divergence: the max_parallel_hazard scan runs in subquery_planner after
    // the Query is arena-sealed (walker needs &'mcx Query); C scans first.
    // Unobservable on this lane -- every Gather consumer is a panic arm.
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

    if (cursor_options & CURSOR_OPT_SCROLL) != 0
        && !execmain::exec_supports_backward_scan(Some(top_plan))
    {
        top_plan = crate::subselect::materialize_finished_plan(mcx, top_plan)?;
    }
    if run.glob.parallel_mode_needed {
        panic!("standard_planner (planner.c): debug_parallel_query Gather; M3 parallel lane");
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
    Ok(PlannedStmt {
        commandType: parse.commandType,
        queryId: parse.queryId,
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
        subplans: glob.subplans,
        rewindPlanIDs: glob.rewind_plan_ids,
        rowMarks: glob.finalrowmarks,
        relationOids: glob.relation_oids,
        invalItems: glob.inval_items,
        paramExecTypes: glob.param_exec_types,
        utilityStmt: parse.utilityStmt,
        stmt_location: parse.stmt_location,
        stmt_len: parse.stmt_len,
    })
}

// IsParallelWorker(): no parallel-worker backends exist yet (bgworker lane).
fn is_parallel_worker() -> bool {
    false
}

pub(crate) use clauses::{is_parallel_safe_exprs, is_parallel_safe_opt};
