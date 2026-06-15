//! Seam declarations for the `backend-commands-explain` unit
//! (`commands/explain.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly. The EXPLAIN-EXECUTE driver's memory/buffer
//! bookkeeping (planner context creation+switch, `pgBufferUsage` snapshot,
//! `instr_time` planstart/planduration, `MemoryContextCounters`) is all
//! explain-owned C state; the driver threads it through these seams as an
//! opaque [`ExplainBookkeeping`] token the explain unit re-materialises.

use types_error::PgResult;
use types_explain::ExplainState;
use types_nodes::nodeindexscan::PlannedStmt;
use types_nodes::nodes::Node;
use types_nodes::parsestmt::{IntoClause, ParamListInfoHandle};
use types_nodes::queryenvironment::QueryEnvironment;

/// Opaque token for the EXPLAIN-EXECUTE bookkeeping the explain unit owns
/// (`planner_ctx`/`saved_ctx`/`bufusage_start`/`planstart`/`planduration`/
/// `mem_counters`). The driver carries it from `explain_execute_begin` through
/// the accounting + per-plan seams without inspecting it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ExplainBookkeeping(pub u64);

seam_core::seam!(
    /// The pre-lookup EXPLAIN bookkeeping: when `es->memory`, create the
    /// "explain analyze planner context" and switch to it; when `es->buffers`,
    /// snapshot `pgBufferUsage`; and `INSTR_TIME_SET_CURRENT(planstart)`.
    /// Reads `es->memory`/`es->buffers`. Allocates / can `ereport(ERROR)`.
    pub fn explain_execute_begin<'mcx>(es: &ExplainState<'mcx>) -> PgResult<ExplainBookkeeping>
);

seam_core::seam!(
    /// `INSTR_TIME_SET_CURRENT(planduration);
    /// INSTR_TIME_SUBTRACT(planduration, planstart)` — record the elapsed
    /// planning time into the bookkeeping.
    pub fn explain_planduration(bk: ExplainBookkeeping) -> PgResult<()>
);

seam_core::seam!(
    /// `if (es->memory) { MemoryContextSwitchTo(saved_ctx);
    /// MemoryContextMemConsumed(planner_ctx, &mem_counters); }` — the
    /// memory-accounting branch (guarded by the driver on `es->memory`).
    pub fn explain_memory_accounting(bk: ExplainBookkeeping) -> PgResult<()>
);

seam_core::seam!(
    /// `if (es->buffers) { memset(&bufusage, 0, ...);
    /// BufferUsageAccumDiff(&bufusage, &pgBufferUsage, &bufusage_start); }` —
    /// the buffer-accounting branch (guarded by the driver on `es->buffers`).
    pub fn explain_buffer_accounting(bk: ExplainBookkeeping) -> PgResult<()>
);

seam_core::seam!(
    /// `ExplainOnePlan(pstmt, into, es, query_string, paramLI, queryEnv,
    /// &planduration, bufusage?, mem_counters?)` (explain.c). The explain unit
    /// supplies the stashed bufusage/mem_counters from `bk` per
    /// `es_buffers`/`es_memory`. Can `ereport(ERROR)`.
    pub fn explain_one_plan<'mcx>(
        pstmt: &PlannedStmt<'mcx>,
        into: Option<&IntoClause<'mcx>>,
        es: &mut ExplainState<'mcx>,
        query_string: &str,
        param_li: ParamListInfoHandle,
        query_env: Option<&QueryEnvironment<'mcx>>,
        bk: ExplainBookkeeping,
        es_buffers: bool,
        es_memory: bool,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `ExplainOneUtility(pstmt->utilityStmt, into, es, pstate, paramLI)`
    /// (explain.c). `source_text` is `pstate->p_sourcetext`. Can
    /// `ereport(ERROR)`.
    pub fn explain_one_utility<'mcx>(
        utility_stmt: &Node<'mcx>,
        into: Option<&IntoClause<'mcx>>,
        es: &mut ExplainState<'mcx>,
        source_text: &str,
        query_env: Option<&QueryEnvironment<'mcx>>,
        param_li: ParamListInfoHandle,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `ExplainSeparatePlans(es)` (explain.c).
    pub fn explain_separate_plans<'mcx>(es: &mut ExplainState<'mcx>) -> PgResult<()>
);
