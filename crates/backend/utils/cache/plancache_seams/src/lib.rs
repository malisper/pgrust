//! Seam declarations for the `backend-utils-cache-plancache` unit
//! (`utils/cache/plancache.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly. The `CachedPlanSource` / `CachedPlan` live
//! structs are owned by the plancache unit; consumers thread the handles
//! ([`::nodes::parsestmt`]) and reach individual fields through the
//! accessor seams below.

use mcx::Mcx;
use types_core::Oid;
use types_error::PgResult;
use ::nodes::nodes::Node;
use ::nodes::nodeindexscan::PlannedStmt;
use ::nodes::params::ParamListInfo;
use ::nodes::parsestmt::{
    CachedPlanHandle, CachedPlanSourceHandle, CommandTag, RawStmt,
    ResourceOwnerHandle,
};
use ::nodes::queryenvironment::QueryEnvironment;
use types_tuple::heaptuple::TupleDescData;

seam_core::seam!(
    /// `CreateCachedPlan(raw_parse_tree, query_string, commandTag)`
    /// (plancache.c) — allocate a `CachedPlanSource` and copy the raw parse
    /// tree into it. `raw_stmt` is the `RawStmt` wrapper carrying the contained
    /// query plus its `stmt_location`/`stmt_len` source span. Allocates / can
    /// `ereport(ERROR)`.
    pub fn create_cached_plan<'mcx>(
        mcx: Mcx<'mcx>,
        raw_stmt: &RawStmt<'mcx>,
        query_string: &str,
        command_tag: CommandTag,
    ) -> PgResult<CachedPlanSourceHandle>
);

seam_core::seam!(
    /// `CompleteCachedPlan(plansource, querytree_list, NULL, argtypes, nargs,
    /// NULL, NULL, CURSOR_OPT_PARALLEL_OK, true)` (plancache.c). `query_list`
    /// is the rewritten `List *` of `Query *`; `arg_types` the resolved
    /// parameter OID array.
    pub fn complete_cached_plan<'mcx>(
        mcx: Mcx<'mcx>,
        plansource: CachedPlanSourceHandle,
        query_list: &[Node<'mcx>],
        arg_types: &[Oid],
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `SaveCachedPlan(plansource)` (plancache.c) — move the source to
    /// permanent memory.
    pub fn save_cached_plan(plansource: CachedPlanSourceHandle) -> PgResult<()>
);

seam_core::seam!(
    /// `DropCachedPlan(plansource)` (plancache.c).
    pub fn drop_cached_plan(plansource: CachedPlanSourceHandle) -> PgResult<()>
);

seam_core::seam!(
    /// `GetCachedPlan(plansource, boundParams, owner, queryEnv)` (plancache.c).
    /// `owner == NULL` means no transient refcount. Replans if needed and
    /// (when `owner != NULL`) registers the refcount with the resource owner.
    /// Can `ereport(ERROR)`.
    pub fn get_cached_plan<'mcx>(
        plansource: CachedPlanSourceHandle,
        bound_params: ParamListInfo,
        owner: ResourceOwnerHandle,
        query_env: Option<&QueryEnvironment<'mcx>>,
    ) -> PgResult<CachedPlanHandle>
);

seam_core::seam!(
    /// `ReleaseCachedPlan(plan, owner)` (plancache.c).
    pub fn release_cached_plan(
        cplan: CachedPlanHandle,
        owner: ResourceOwnerHandle,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `CachedPlanGetTargetList(plansource, NULL)` (plancache.c) — the plan's
    /// primary targetlist as owned `TargetEntry` nodes (NIL == empty slice).
    /// Allocated in `mcx`. Can `ereport(ERROR)` (replan).
    pub fn cached_plan_get_target_list<'mcx>(
        mcx: Mcx<'mcx>,
        plansource: CachedPlanSourceHandle,
    ) -> PgResult<mcx::PgVec<'mcx, Node<'mcx>>>
);

// --- CachedPlanSource / CachedPlan field accessors --------------------------

seam_core::seam!(
    /// `plansource->fixed_result`.
    pub fn plansource_fixed_result(plansource: CachedPlanSourceHandle) -> PgResult<bool>
);

seam_core::seam!(
    /// `plansource->num_params`.
    pub fn plansource_num_params(plansource: CachedPlanSourceHandle) -> PgResult<i32>
);

seam_core::seam!(
    /// `plansource->param_types` — the `num_params`-element OID array, copied
    /// into `mcx`.
    pub fn plansource_param_types<'mcx>(
        mcx: Mcx<'mcx>,
        plansource: CachedPlanSourceHandle,
    ) -> PgResult<mcx::PgVec<'mcx, Oid>>
);

seam_core::seam!(
    /// `plansource->query_string` — copied into `mcx`.
    pub fn plansource_query_string<'mcx>(
        mcx: Mcx<'mcx>,
        plansource: CachedPlanSourceHandle,
    ) -> PgResult<mcx::PgString<'mcx>>
);

seam_core::seam!(
    /// `plansource->commandTag`.
    pub fn plansource_command_tag(plansource: CachedPlanSourceHandle) -> PgResult<CommandTag>
);

seam_core::seam!(
    /// `plansource->resultDesc` — `None` if no result tupdesc (e.g. a DML
    /// stmt); else an owned copy of the stored descriptor in `mcx`.
    pub fn plansource_result_desc<'mcx>(
        mcx: Mcx<'mcx>,
        plansource: CachedPlanSourceHandle,
    ) -> PgResult<Option<TupleDescData<'mcx>>>
);

seam_core::seam!(
    /// `plansource->resultDesc != NULL` — whether the prepared statement
    /// produces a result tuple descriptor, without copying it. Used by
    /// `ExecuteStmtHasResult` (prepare.c).
    pub fn plansource_has_result_desc(plansource: CachedPlanSourceHandle) -> PgResult<bool>
);

seam_core::seam!(
    /// `plansource->num_generic_plans`.
    pub fn plansource_num_generic_plans(plansource: CachedPlanSourceHandle) -> PgResult<i64>
);

seam_core::seam!(
    /// `plansource->num_custom_plans`.
    pub fn plansource_num_custom_plans(plansource: CachedPlanSourceHandle) -> PgResult<i64>
);

seam_core::seam!(
    /// `cplan->stmt_list` — the cached plan's `List *` of `PlannedStmt *`,
    /// copied into `mcx` as owned `PlannedStmt`s.
    pub fn cached_plan_stmt_list<'mcx>(
        mcx: Mcx<'mcx>,
        cplan: CachedPlanHandle,
    ) -> PgResult<mcx::PgVec<'mcx, PlannedStmt<'mcx>>>
);

seam_core::seam!(
    /// `InitPlanCache()` (plancache.c): set up the plan-cache invalidation
    /// callbacks. `Err` carries its `ereport` surface.
    pub fn init_plan_cache() -> PgResult<()>
);

seam_core::seam!(
    /// `CreateCachedPlan(NULL, query_string, commandTag)` (plancache.c) — the
    /// empty-query Parse case (a `CachedPlanSource` with no raw parse tree).
    /// Allocates / can `ereport(ERROR)`.
    pub fn create_cached_plan_empty<'mcx>(
        mcx: Mcx<'mcx>,
        query_string: &str,
        command_tag: CommandTag,
    ) -> PgResult<CachedPlanSourceHandle>
);

seam_core::seam!(
    /// `psrc->raw_parse_tree && IsTransactionExitStmt(psrc->raw_parse_tree->stmt)`
    /// (postgres.c, exec_bind_message's aborted-xact guard): whether the cached
    /// source's raw statement is a transaction-exit command
    /// (COMMIT/PREPARE/ROLLBACK/ROLLBACK-TO). `false` when there is no raw tree.
    pub fn plansource_raw_is_transaction_exit_stmt(
        plansource: CachedPlanSourceHandle,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `psrc->raw_parse_tree && analyze_requires_snapshot(psrc->raw_parse_tree)`
    /// (postgres.c, exec_bind_message): whether binding/planning the cached
    /// source's raw statement needs a transaction snapshot. `false` when there
    /// is no raw tree.
    pub fn plansource_raw_requires_snapshot(
        plansource: CachedPlanSourceHandle,
    ) -> PgResult<bool>
);
