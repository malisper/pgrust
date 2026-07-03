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
pub mod partprune;
pub mod like_support;
pub mod multirangetypes_selfuncs;
pub mod network_selfuncs;
pub mod rangetypes_selfuncs;
pub mod selfuncs;
pub mod planmain;
pub mod prep;
pub mod prepunion;
pub mod prepqual;
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

    real_guc!(CPU_TUPLE_COST, cpu_tuple_cost, set_cpu_tuple_cost, guc_tables::consts::DEFAULT_CPU_TUPLE_COST);
    real_guc!(SEQ_PAGE_COST, seq_page_cost, set_seq_page_cost, guc_tables::consts::DEFAULT_SEQ_PAGE_COST);
    real_guc!(RANDOM_PAGE_COST, random_page_cost, set_random_page_cost, guc_tables::consts::DEFAULT_RANDOM_PAGE_COST);
    real_guc!(CPU_INDEX_TUPLE_COST, cpu_index_tuple_cost, set_cpu_index_tuple_cost, guc_tables::consts::DEFAULT_CPU_INDEX_TUPLE_COST);
    real_guc!(CPU_OPERATOR_COST, cpu_operator_cost, set_cpu_operator_cost, guc_tables::consts::DEFAULT_CPU_OPERATOR_COST);
    int_guc!(EFFECTIVE_CACHE_SIZE, effective_cache_size, set_effective_cache_size, guc_tables::consts::DEFAULT_EFFECTIVE_CACHE_SIZE);
    bool_guc!(ENABLE_SEQSCAN, enable_seqscan, set_enable_seqscan, true);
    bool_guc!(ENABLE_INDEXSCAN, enable_indexscan, set_enable_indexscan, true);
    bool_guc!(ENABLE_INDEXONLYSCAN, enable_indexonlyscan, set_enable_indexonlyscan, true);
    bool_guc!(ENABLE_BITMAPSCAN, enable_bitmapscan, set_enable_bitmapscan, true);
    bool_guc!(ENABLE_HASHAGG, enable_hashagg, set_enable_hashagg, true);
    bool_guc!(ENABLE_SORT, enable_sort, set_enable_sort, true);
    bool_guc!(ENABLE_NESTLOOP, enable_nestloop, set_enable_nestloop, true);
    bool_guc!(ENABLE_HASHJOIN, enable_hashjoin, set_enable_hashjoin, true);
    bool_guc!(ENABLE_MERGEJOIN, enable_mergejoin, set_enable_mergejoin, true);
    bool_guc!(ENABLE_MATERIAL, enable_material, set_enable_material, true);
    bool_guc!(ENABLE_INCREMENTAL_SORT, enable_incremental_sort, set_enable_incremental_sort, true);
    bool_guc!(ENABLE_GROUP_BY_REORDERING, enable_group_by_reordering, set_enable_group_by_reordering, true);
    bool_guc!(ENABLE_DISTINCT_REORDERING, enable_distinct_reordering, set_enable_distinct_reordering, true);
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
    bool_guc!(ENABLE_PARTITION_PRUNING, enable_partition_pruning, set_enable_partition_pruning, true);
}

pub fn init_seams() {
    planner_seams::planner::set(|mcx, parse, query_string, cursor_options, bound_params| {
        planner(mcx, parse, query_string, cursor_options, bound_params)
    });
    use guc_tables::GucVarAccessors;
    guc_tables::vars::cpu_tuple_cost
        .install(GucVarAccessors { get: gucs::cpu_tuple_cost, set: gucs::set_cpu_tuple_cost });
    guc_tables::vars::seq_page_cost
        .install(GucVarAccessors { get: gucs::seq_page_cost, set: gucs::set_seq_page_cost });
    guc_tables::vars::random_page_cost
        .install(GucVarAccessors { get: gucs::random_page_cost, set: gucs::set_random_page_cost });
    guc_tables::vars::cpu_index_tuple_cost.install(GucVarAccessors {
        get: gucs::cpu_index_tuple_cost,
        set: gucs::set_cpu_index_tuple_cost,
    });
    guc_tables::vars::cpu_operator_cost.install(GucVarAccessors {
        get: gucs::cpu_operator_cost,
        set: gucs::set_cpu_operator_cost,
    });
    guc_tables::vars::effective_cache_size.install(GucVarAccessors {
        get: gucs::effective_cache_size,
        set: gucs::set_effective_cache_size,
    });
    guc_tables::vars::enable_seqscan
        .install(GucVarAccessors { get: gucs::enable_seqscan, set: gucs::set_enable_seqscan });
    guc_tables::vars::enable_partition_pruning.install(GucVarAccessors {
        get: gucs::enable_partition_pruning,
        set: gucs::set_enable_partition_pruning,
    });
    guc_tables::vars::enable_indexscan
        .install(GucVarAccessors { get: gucs::enable_indexscan, set: gucs::set_enable_indexscan });
    guc_tables::vars::enable_indexonlyscan.install(GucVarAccessors {
        get: gucs::enable_indexonlyscan,
        set: gucs::set_enable_indexonlyscan,
    });
    guc_tables::vars::enable_bitmapscan.install(GucVarAccessors {
        get: gucs::enable_bitmapscan,
        set: gucs::set_enable_bitmapscan,
    });
    guc_tables::vars::enable_sort
        .install(GucVarAccessors { get: gucs::enable_sort, set: gucs::set_enable_sort });
    guc_tables::vars::enable_nestloop
        .install(GucVarAccessors { get: gucs::enable_nestloop, set: gucs::set_enable_nestloop });
    guc_tables::vars::enable_hashjoin
        .install(GucVarAccessors { get: gucs::enable_hashjoin, set: gucs::set_enable_hashjoin });
    guc_tables::vars::enable_mergejoin
        .install(GucVarAccessors { get: gucs::enable_mergejoin, set: gucs::set_enable_mergejoin });
    guc_tables::vars::enable_material
        .install(GucVarAccessors { get: gucs::enable_material, set: gucs::set_enable_material });
    guc_tables::vars::enable_incremental_sort.install(GucVarAccessors {
        get: gucs::enable_incremental_sort,
        set: gucs::set_enable_incremental_sort,
    });
    guc_tables::vars::enable_hashagg
        .install(GucVarAccessors { get: gucs::enable_hashagg, set: gucs::set_enable_hashagg });
    guc_tables::vars::enable_group_by_reordering.install(GucVarAccessors {
        get: gucs::enable_group_by_reordering,
        set: gucs::set_enable_group_by_reordering,
    });
    guc_tables::vars::enable_distinct_reordering.install(GucVarAccessors {
        get: gucs::enable_distinct_reordering,
        set: gucs::set_enable_distinct_reordering,
    });
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

    subquery_planner(&mut run, parse, tuple_fraction, None)?;

    let final_rel = fetch_final_rel(&mut run);
    let best_path = get_cheapest_fractional_path(&run, final_rel, tuple_fraction);
    let top_plan = create_plan(&mut run, best_path)?;

    if (cursor_options & CURSOR_OPT_SCROLL) != 0
        && !execmain::exec_supports_backward_scan(Some(top_plan))
    {
        panic!(
            "materialize_finished_plan (createplan.c): SCROLL cursor over a \
             non-backward plan needs a Material wrapper; M2 cursor lane"
        );
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
            crate::subselect::ss_finalize_plan(&run, subplan, &run.subroots[i].root.outer_params)?;
        }
        crate::subselect::ss_finalize_plan(&run, top_plan, &run.root.outer_params)?;
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

// is_parallel_safe (clauses.c) over a PathTarget's exprs; C passes the List*.
pub(crate) fn is_parallel_safe_exprs(run: &PlannerRun<'_>, target: PtId) -> PgResult<bool> {
    if run.glob.max_parallel_hazard == b's' as i8 && run.glob.param_exec_types.is_nil() {
        return Ok(true);
    }
    let mcx = run.mcx;
    let mut list = NodeList::nil();
    let n = run.root.pathtarget(target).exprs.len();
    for i in 0..n {
        let id = run.root.pathtarget(target).exprs[i];
        list.lappend(mcx, *run.root.expr_node(id))?;
    }
    let node = Node::mk_list(mcx, list)?;
    clauses::is_parallel_safe(
        run.glob.max_parallel_hazard,
        run.glob.param_exec_types.is_nil(),
        &[],
        node,
    )
}

pub(crate) fn is_parallel_safe_opt(
    run: &PlannerRun<'_>,
    node: Option<Node<'_>>,
) -> PgResult<bool> {
    match node {
        // C is_parallel_safe(root, NULL): the walker sees nothing unsafe.
        None => Ok(true),
        Some(n) => clauses::is_parallel_safe(
            run.glob.max_parallel_hazard,
            run.glob.param_exec_types.is_nil(),
            &[],
            n,
        ),
    }
}
