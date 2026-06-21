//! Seam declarations for the `backend-commands-explain` unit
//! (`commands/explain.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly. The EXPLAIN-EXECUTE driver's memory/buffer
//! bookkeeping (planner context creation+switch, `pgBufferUsage` snapshot,
//! `instr_time` planstart/planduration, `MemoryContextCounters`) is all
//! explain-owned C state. Mirroring C's `ExplainExecuteQuery`, the driver keeps
//! it as a plain stack local ([`Bookkeeping`]) and threads it through these
//! seams by value / `&mut` — no opaque token, no registry (C uses none here).

use types_core::instrument::{instr_time, BufferUsage};
use types_error::PgResult;
use types_explain::ExplainState;
use types_nodes::ddlnodes::ExecuteStmt;
use types_nodes::nodeindexscan::PlannedStmt;
use types_nodes::nodes::Node;
use types_nodes::params::ParamListInfo;
use types_nodes::parsestmt::IntoClause;
use types_nodes::queryenvironment::QueryEnvironment;

/// The EXPLAIN-EXECUTE bookkeeping the C `ExplainExecuteQuery` keeps on its
/// stack and threads through `ExplainOnePlan` (`instr_time planstart/
/// planduration`, `BufferUsage bufusage_start/bufusage`, the planner
/// `MemoryContext`/`MemoryContextCounters`). The driver owns one of these as a
/// plain local; `explain_execute_begin` produces it, the accounting seams
/// mutate it, and `explain_one_plan` reads the stashed `bufusage`/`planduration`
/// out of it. (The planner `MemoryContext`/`MemoryContextCounters` `es->memory`
/// fields are unported and live behind the `explain_memory_accounting` panic;
/// they are not carried here yet.)
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Bookkeeping {
    /// `instr_time planstart` — set at `explain_execute_begin`.
    pub planstart: instr_time,
    /// `instr_time planduration` — elapsed planning time (set at planduration).
    pub planduration: instr_time,
    /// `BufferUsage bufusage_start` — snapshot of `pgBufferUsage` at begin
    /// (only when `es->buffers`).
    pub bufusage_start: BufferUsage,
    /// `BufferUsage bufusage` — accumulated planning buffer usage (set at
    /// buffer_accounting).
    pub bufusage: BufferUsage,
    /// whether `es->buffers` was set at begin.
    pub buffers: bool,
    /// whether `es->memory` was set at begin (drives the planner-context
    /// `MemoryContextMemConsumed` accounting + the `mem_counters` Planning leg).
    pub memory: bool,
    /// the planner `MemoryContext` created when `es->memory` (C's
    /// `planner_ctx`). Carried so `explain_memory_accounting` can switch back to
    /// `saved_ctx`, measure consumption, and delete it. `0` when unset.
    pub planner_ctx: u64,
    /// the context active before the planner-context switch (C's `saved_ctx`).
    pub saved_ctx: u64,
    /// `MemoryContextCounters.totalspace` measured by `MemoryContextMemConsumed`
    /// (only meaningful when `memory`). Consumed by `show_memory_counters`.
    pub mem_totalspace: i64,
    /// `MemoryContextCounters.freespace` measured by `MemoryContextMemConsumed`.
    pub mem_freespace: i64,
}

seam_core::seam!(
    /// The pre-lookup EXPLAIN bookkeeping: when `es->memory`, create the
    /// "explain analyze planner context" and switch to it; when `es->buffers`,
    /// snapshot `pgBufferUsage`; and `INSTR_TIME_SET_CURRENT(planstart)`.
    /// Reads `es->memory`/`es->buffers`. Allocates / can `ereport(ERROR)`.
    /// Returns the freshly-initialised stack [`Bookkeeping`] the driver owns.
    pub fn explain_execute_begin<'mcx>(es: &ExplainState<'mcx>) -> PgResult<Bookkeeping>
);

seam_core::seam!(
    /// `INSTR_TIME_SET_CURRENT(planduration);
    /// INSTR_TIME_SUBTRACT(planduration, planstart)` — record the elapsed
    /// planning time into the bookkeeping.
    pub fn explain_planduration(bk: &mut Bookkeeping) -> PgResult<()>
);

seam_core::seam!(
    /// `if (es->memory) { MemoryContextSwitchTo(saved_ctx);
    /// MemoryContextMemConsumed(planner_ctx, &mem_counters); }` — the
    /// memory-accounting branch (guarded by the driver on `es->memory`).
    pub fn explain_memory_accounting(bk: &mut Bookkeeping) -> PgResult<()>
);

seam_core::seam!(
    /// `if (es->buffers) { memset(&bufusage, 0, ...);
    /// BufferUsageAccumDiff(&bufusage, &pgBufferUsage, &bufusage_start); }` —
    /// the buffer-accounting branch (guarded by the driver on `es->buffers`).
    pub fn explain_buffer_accounting(bk: &mut Bookkeeping) -> PgResult<()>
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
        param_li: ParamListInfo,
        query_env: Option<&QueryEnvironment<'mcx>>,
        bk: &Bookkeeping,
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
        param_li: ParamListInfo,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `ExplainSeparatePlans(es)` (explain.c).
    pub fn explain_separate_plans<'mcx>(es: &mut ExplainState<'mcx>) -> PgResult<()>
);

seam_core::seam!(
    /// `ExplainExecuteQuery((ExecuteStmt *) utilityStmt, into, es, pstate,
    /// params)` (prepare.c) — the `EXPLAIN EXECUTE` leg of `ExplainOneUtility`.
    /// Owned/installed by `backend-commands-prepare` (the prepared-statement
    /// cache lives there); `backend-commands-explain`'s `ExplainOneUtility`
    /// calls it. `source_text` is `pstate->p_sourcetext`. Can `ereport(ERROR)`.
    pub fn explain_execute_query<'mcx>(
        execstmt: &ExecuteStmt<'mcx>,
        into: Option<&IntoClause<'mcx>>,
        es: &mut ExplainState<'mcx>,
        source_text: &str,
        query_env: Option<&QueryEnvironment<'mcx>>,
        params: ParamListInfo,
    ) -> PgResult<()>
);
