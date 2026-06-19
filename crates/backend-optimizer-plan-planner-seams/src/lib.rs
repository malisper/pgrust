//! Seam declarations for the `backend-optimizer-plan-planner` unit
//! (`optimizer/plan/planner.c`), including the planner entry point
//! (`pg_plan_query`) the COPY-(query)-TO driver calls.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

use mcx::Mcx;
use types_core::Oid;
use types_error::PgResult;
use types_nodes::copy_query::Query;
use types_nodes::nodeindexscan::PlannedStmt;
use types_pathnodes::planner_run::PlannerRun;
use types_pathnodes::{PlannerGlobal, PlannerInfo, QueryId};

seam_core::seam!(
    /// `select_rowmark_type(rte, strength)` (planner.c:2503) — choose the
    /// `RowMarkType` for a relation RTE given a `FOR UPDATE/SHARE` strength.
    /// inherit.c's `expand_single_inheritance_child` re-selects the mark type
    /// per child (relkind may differ from the parent). Owner planner.c is
    /// ported; the cyclic edge from inherit.c routes through this seam.
    pub fn select_rowmark_type(
        rte: &types_nodes::parsenodes::RangeTblEntry<'_>,
        strength: types_nodes::rawnodes::LockClauseStrength,
    ) -> PgResult<types_nodes::execnodes::RowMarkType>
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
    /// `plan_cluster_use_sort(tableOid, indexOid)` (planner.c): whether a
    /// seqscan+sort beats an indexscan for the cluster copy.
    pub fn plan_cluster_use_sort(table_oid: Oid, index_oid: Oid) -> PgResult<bool>
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
