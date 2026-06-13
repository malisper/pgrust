//! Seam declarations for the planner entry point (`optimizer/plan/planner.c`'s
//! `pg_plan_query` wrapper) the COPY-(query)-TO driver calls.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use mcx::Mcx;
use types_error::PgResult;
use types_nodes::copy_query::Query;
use types_nodes::nodeindexscan::PlannedStmt;

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
