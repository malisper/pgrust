//! Seam declarations for the `backend-optimizer-plan-planner` unit
//! (`optimizer/plan/planner.c`), including the planner entry point
//! (`pg_plan_query`) the COPY-(query)-TO driver calls.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

use ::mcx::Mcx;
use ::types_core::Oid;
use ::types_error::PgResult;
use ::nodes::copy_query::Query;
use ::nodes::nodeindexscan::PlannedStmt;
use ::nodes::rawnodes::SetOperationStmt;
use ::pathnodes::planner_run::PlannerRun;
use pathnodes::{PlannerGlobal, PlannerInfo, QueryId};

seam_core::seam!(
    /// `IsParallelWorker()` (access/parallel.h:60) — `ParallelWorkerNumber >= 0`.
    /// `standard_planner` (planner.c:373) consults this in the cheap-test gate
    /// for `glob->parallelModeOK` (we don't try parallel mode inside a parallel
    /// worker). Owner is `backend-access-transam-parallel`; the planner reads it
    /// through this seam to avoid depending on the parallel-executor crate.
    /// When unset (e.g. `postgres --single`) the planner treats it as `false`,
    /// matching a non-worker backend.
    pub fn is_parallel_worker() -> bool
);

seam_core::seam!(
    /// `select_rowmark_type(rte, strength)` (planner.c:2503) — choose the
    /// `RowMarkType` for a relation RTE given a `FOR UPDATE/SHARE` strength.
    /// inherit.c's `expand_single_inheritance_child` re-selects the mark type
    /// per child (relkind may differ from the parent). Owner planner.c is
    /// ported; the cyclic edge from inherit.c routes through this seam.
    pub fn select_rowmark_type(
        rte: &::nodes::parsenodes::RangeTblEntry<'_>,
        strength: ::nodes::rawnodes::LockClauseStrength,
    ) -> PgResult<::nodes::execnodes::RowMarkType>
);

seam_core::seam!(
    /// `pg_plan_query(querytree, query_string, cursorOptions, boundParams)`
    /// (tcop/postgres.c → planner): plan one rewritten `Query` into a
    /// `PlannedStmt`. COPY passes `CURSOR_OPT_PARALLEL_OK` and no bound params.
    /// The plan is allocated in `mcx`. `Err` carries any planning
    /// `ereport(ERROR)`.
    pub fn pg_plan_query<'mcx>(
        mcx: Mcx<'mcx>,
        querytree: &Query<'mcx>,
        query_string: &str,
        cursor_options: i32,
    ) -> PgResult<PlannedStmt<'mcx>>
);

seam_core::seam!(
    /// `planner(parse, query_string, cursorOptions, boundParams)` (planner.c:286)
    /// with the bound external-parameter values threaded in. Identical to
    /// [`pg_plan_query`] but `bound_params` (`None` is the C NULL) is recorded on
    /// `glob->boundParams` so the const-folder substitutes a PARAM_EXTERN `$n`
    /// for its bound `Const` (the custom-plan path; `BuildCachedPlan` →
    /// `pg_plan_queries` → `pg_plan_query`). The simple-Query/COPY path passes
    /// `None` and is identical to [`pg_plan_query`].
    pub fn pg_plan_query_params<'mcx>(
        mcx: Mcx<'mcx>,
        querytree: &Query<'mcx>,
        query_string: &str,
        cursor_options: i32,
        bound_params: ::nodes::params::ParamListInfo,
    ) -> PgResult<PlannedStmt<'mcx>>
);

// ===========================================================================
// `planner_hook` (planner.c): the loadable-module interposition point for query
// planning. In C `planner()` calls `planner_hook ? planner_hook(parse,
// query_string, cursorOptions, boundParams) : standard_planner(...)`. Modeled
// like `shmem_request_hook` (miscinit.c): a per-backend thread-local
// `Cell<Option<fn>>` with `set_planner_hook` / `planner_hook_present` /
// `call_planner_hook`. The slot lives in this `-seams` crate so a hook-installing
// module (e.g. pg_stat_statements) can register without a dependency cycle; the
// hook wraps + calls the owner's public `standard_planner`. With no hook set,
// `planner_hook_present()` is false and the owner runs `standard_planner`
// directly — byte-identical to today.
// ===========================================================================

/// `planner_hook_type` (planner.h): `PlannedStmt *(*)(Query *parse, const char
/// *query_string, int cursorOptions, ParamListInfo boundParams)`. Higher-ranked
/// over the planner arena lifetime so a single registered hook plans any query.
pub type PlannerHook = for<'mcx> fn(
    mcx: Mcx<'mcx>,
    parse: &Query<'mcx>,
    query_string: &str,
    cursor_options: i32,
    bound_params: ::nodes::params::ParamListInfo,
) -> PgResult<PlannedStmt<'mcx>>;

thread_local! {
    /// `planner_hook_type planner_hook = NULL;` (planner.c).
    static PLANNER_HOOK: std::cell::Cell<Option<PlannerHook>> =
        const { std::cell::Cell::new(None) };
}

/// `planner_hook != NULL` — whether a module registered a `planner` hook.
pub fn planner_hook_present() -> bool {
    PLANNER_HOOK.with(|c| c.get().is_some())
}
/// Register a module's `planner_hook` (the `planner_hook = my_hook` assignment
/// in `_PG_init`). The hook wraps + calls the owner's public `standard_planner`.
pub fn set_planner_hook(hook: Option<PlannerHook>) -> Option<PlannerHook> {
    PLANNER_HOOK.with(|c| c.replace(hook))
}
/// Invoke the registered `planner_hook(parse, query_string, cursorOptions,
/// boundParams)`. Panics if none is registered (the call site guards with
/// [`planner_hook_present`], mirroring C's `if (planner_hook)`).
pub fn call_planner_hook<'mcx>(
    mcx: Mcx<'mcx>,
    parse: &Query<'mcx>,
    query_string: &str,
    cursor_options: i32,
    bound_params: ::nodes::params::ParamListInfo,
) -> PgResult<PlannedStmt<'mcx>> {
    match PLANNER_HOOK.with(std::cell::Cell::get) {
        Some(hook) => hook(mcx, parse, query_string, cursor_options, bound_params),
        None => panic!("call_planner_hook() called with no hook registered"),
    }
}

seam_core::seam!(
    /// `plan_cluster_use_sort(tableOid, indexOid)` (planner.c): whether a
    /// seqscan+sort beats an indexscan for the cluster copy. C runs in
    /// `CurrentMemoryContext`; the value-model port threads the caller's `mcx`
    /// in (it owns the throwaway dummy `PlannerRun`/`PlannerInfo`).
    pub fn plan_cluster_use_sort<'mcx>(
        mcx: Mcx<'mcx>,
        table_oid: Oid,
        index_oid: Oid,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `subquery_planner(glob, subquery, parent_root, hasRecursion,
    /// tuple_fraction, setops)` (planner.c) — plan one leaf subquery of a set-op
    /// tree, used by `prepunion.c`'s `recurse_set_operations`.
    ///
    /// The caller hands ownership of the (shared) [`PlannerGlobal`] in and
    /// receives the leaf's [`PlannerInfo`] (`subroot`) back; the mutated glob is
    /// carried inside `subroot.glob` so the caller can move it back out and
    /// thread it into the next leaf, mirroring C's single shared `glob` pointer.
    /// `subquery_id` is the interned leaf Query; the subroot's `parse` resolves
    /// in the same [`PlannerRun`].
    pub fn subquery_planner_for_setop<'mcx>(
        mcx: Mcx<'mcx>,
        run: &mut PlannerRun<'mcx>,
        glob: PlannerGlobal,
        subquery_id: QueryId,
        parent_root: PlannerInfo,
        recursion_carry: Option<(i32, f64)>,
        has_recursion: bool,
        tuple_fraction: f64,
        setop_op: Option<&'mcx SetOperationStmt<'mcx>>,
    ) -> PgResult<PlannerInfo>
);

seam_core::seam!(
    /// `subquery_planner(glob, subquery, parent_root, false, tuple_fraction,
    /// NULL)` (planner.c:683) as invoked by `allpaths.c`'s
    /// `set_subquery_pathlist` for a plain `RTE_SUBQUERY` in the FROM clause.
    ///
    /// Same glob-threading contract as [`subquery_planner_for_setop`]: the
    /// caller (allpaths) hands the shared [`PlannerGlobal`] in (moved out of the
    /// outer `root`) and receives the FROM-subquery's [`PlannerInfo`] (`subroot`)
    /// back, with the mutated glob carried inside `subroot.glob` so the caller
    /// can move it back onto the outer root. `parent_query_level` is the outer
    /// `root.query_level` (the subroot's level becomes `parent + 1`, and any
    /// `plan_params` upper references land on the parent). `subquery_id` is the
    /// interned (already copyObject'd) subquery Query.
    pub fn subquery_planner_for_fromsubquery<'mcx>(
        mcx: Mcx<'mcx>,
        run: &mut PlannerRun<'mcx>,
        glob: PlannerGlobal,
        subquery_id: QueryId,
        parent_root: PlannerInfo,
        tuple_fraction: f64,
    ) -> PgResult<PlannerInfo>
);
