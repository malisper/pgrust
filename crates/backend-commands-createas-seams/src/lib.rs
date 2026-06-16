//! Seam declarations for the `backend-commands-createas` unit
//! (`commands/createas.c`).
//!
//! Two kinds of seam live here:
//!
//! * **Inward** seams (`exec_create_table_as`, `create_table_as_rel_exists`,
//!   `create_into_rel_dest_receiver`, `get_into_rel_eflags`) — entry points
//!   other units (`tcop/utility.c`, the dest router's `DestIntoRel` arm,
//!   `prepare.c` / `explain.c`) call across a dependency cycle. The owner
//!   (`backend-commands-createas`) installs them from its `init_seams()`.
//!
//! * **Outward** seams the createas unit *owns the declaration of* because their
//!   real owners are not ported (or would cycle): `define_relation`,
//!   `store_view_query`, `table_finish_bulk_insert`, `execute_query`. These
//!   panic loudly until their owners land and install them.

use mcx::Mcx;
use types_catalog::catalog_dependency::ObjectAddress;
use types_error::PgResult;
use types_nodes::ddlnodes::{CreateTableAsStmt, ExecuteStmt, IntoClause as DdlIntoClause};
use types_nodes::parsestmt::{DestReceiverHandle, IntoClause, ParamListInfoHandle};
use types_portal::QueryCompletion;

seam_core::seam!(
    /// `GetIntoRelEFlags(intoClause)` (createas.c) — the executor eflags for a
    /// CREATE TABLE AS target (e.g. `EXEC_FLAG_SKIP_TRIGGERS`,
    /// `EXEC_FLAG_WITH_NO_DATA`). Reads the clause; cannot `ereport`.
    pub fn get_into_rel_eflags<'mcx>(into: &IntoClause<'mcx>) -> PgResult<i32>
);

seam_core::seam!(
    /// `ExecCreateTableAs(pstate, stmt, params, queryEnv, qc)` (createas.c) —
    /// execute a CREATE TABLE AS / SELECT INTO / CREATE MATERIALIZED VIEW. The
    /// caller (`tcop/utility.c`) supplies the parsed statement, the source text
    /// (`pstate->p_sourcetext`), the bound params, and the optional
    /// `QueryCompletion` to fill. Returns the created relation's address and the
    /// (possibly updated) completion. Can `ereport(ERROR)`.
    pub fn exec_create_table_as<'mcx>(
        mcx: Mcx<'mcx>,
        stmt: &CreateTableAsStmt<'mcx>,
        query_string: &str,
        params: ParamListInfoHandle,
        qc: Option<QueryCompletion>,
    ) -> PgResult<(ObjectAddress, Option<QueryCompletion>)>
);

seam_core::seam!(
    /// `CreateTableAsRelExists(ctas)` (createas.c) — whether the relation the
    /// statement would create already exists (raising the duplicate-table error
    /// when `IF NOT EXISTS` was not given). Used by `tcop/utility.c` to decide
    /// whether to skip the command. Can `ereport(ERROR)`.
    pub fn create_table_as_rel_exists<'mcx>(
        mcx: Mcx<'mcx>,
        ctas: &CreateTableAsStmt<'mcx>,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `CreateIntoRelDestReceiver(intoClause)` (createas.c) — build the
    /// `DR_intorel` `DestReceiver` and register it into the tcop-dest router.
    /// `into` may be `None` (the `CreateDestReceiver()` deferred-`into` contract).
    /// The dest router's `DestIntoRel` arm reaches this.
    pub fn create_into_rel_dest_receiver<'mcx>(
        into: Option<&DdlIntoClause<'mcx>>,
    ) -> PgResult<DestReceiverHandle>
);

seam_core::seam!(
    /// The CTAS executor-driven leg (createas.c 300-361, the `else` branch):
    /// `QueryRewrite(query)` → single-`SELECT` check → `pg_plan_query(query,
    /// queryString, CURSOR_OPT_PARALLEL_OK, params)` →
    /// `PushCopiedSnapshot(GetActiveSnapshot()) + UpdateActiveSnapshotCommandId`
    /// → `CreateQueryDesc(plan, …, dest, …)` → `ExecutorStart(qd,
    /// GetIntoRelEFlags(into))` → `ExecutorRun(qd, Forward, 0)` →
    /// `SetQueryCompletion(qc, CMDTAG_SELECT, qd->estate->es_processed)` →
    /// `ExecutorFinish/End + FreeQueryDesc + PopActiveSnapshot`. The output flows
    /// into the `DR_intorel` receiver named by `dest`.
    ///
    /// Bundled into one seam because `QueryRewrite` / `pg_plan_query` take the
    /// trimmed handle-style `portalcmds::Query`, while the CTAS `Query` is the
    /// canonical arena-lifetimed `copy_query::Query<'mcx>` — the two are
    /// incompatible models (see types-nodes::portalcmds). The rewriter/planner
    /// own that reconciliation; this is the prompt's sanctioned `pg_plan_query`
    /// panic until they land with a real-`Query` contract. The receiver itself
    /// is fully real (registered in the tcop-dest router); only the
    /// rewrite/plan/run pipeline is deferred. Returns the (possibly filled)
    /// `QueryCompletion`.
    pub fn run_ctas_executor<'mcx>(
        mcx: Mcx<'mcx>,
        query: types_nodes::copy_query::Query<'mcx>,
        into: types_nodes::ddlnodes::IntoClause<'mcx>,
        query_string: &str,
        params: ParamListInfoHandle,
        dest: DestReceiverHandle,
        qc: Option<QueryCompletion>,
    ) -> PgResult<Option<QueryCompletion>>
);

// ===========================================================================
// Outward seams owned here: unported / cyclic callees.
// ===========================================================================

seam_core::seam!(
    /// The CTAS query-jumble + post-parse-analyze preamble (createas.c 244-249):
    /// `if (IsQueryIdEnabled()) jstate = JumbleQuery(query); if
    /// (post_parse_analyze_hook) (*post_parse_analyze_hook)(pstate, query,
    /// jstate);`. Bundled because `JumbleQuery`/`post_parse_analyze_hook` operate
    /// on the `portalcmds::Query`/`ParseState` model while the CTAS `Query` is a
    /// `copy_query::Query`; the queryjumble/analyze owners (not ported through
    /// this path) own the model reconciliation. `query_string` is
    /// `pstate->p_sourcetext`. Panics until installed.
    pub fn jumble_and_post_analyze<'mcx>(
        mcx: Mcx<'mcx>,
        query: &types_nodes::copy_query::Query<'mcx>,
        query_string: &str,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// The `create_ctas_internal` catalog-creation sequence (createas.c 81-145):
    /// fake up a `CreateStmt` from `into`, `DefineRelation(create, relkind, ...)`
    /// (tablecmds.c), `CommandCounterIncrement`, the TOAST-options validation +
    /// `NewRelationCreateToastTable`, and — for a matview — the
    /// `copyObject(into->viewQuery)` + `StoreViewQuery` (view.c) +
    /// `CommandCounterIncrement`. These steps have no createas-observable
    /// intermediate state (everything lands in the catalog) and share
    /// `create->options` and the new relation OID, including the
    /// `Datum`-varlena reloptions the toast step consumes; they are performed as
    /// one step in the tablecmds/view/heap owner. Returns the new relation's
    /// `ObjectAddress`. `DefineRelation`/`StoreViewQuery` are not ported, so this
    /// panics until tablecmds.c/view.c land and install it.
    pub fn create_ctas_relation<'mcx>(
        mcx: Mcx<'mcx>,
        into: types_nodes::ddlnodes::IntoClause<'mcx>,
        attr_list: mcx::PgVec<'mcx, mcx::PgBox<'mcx, types_nodes::nodes::Node<'mcx>>>,
        relkind: u8,
        is_matview: bool,
    ) -> PgResult<ObjectAddress>
);

seam_core::seam!(
    /// `table_finish_bulk_insert(rel, options)` (tableam.h inline) — flush any
    /// remaining buffered tuples and, for `TABLE_INSERT_SKIP_WAL`, fsync the
    /// relation. Called by `createas.c`'s `intorel_shutdown`. The tableam AM
    /// routine's `finish_bulk_insert` vtable slot is not yet ported (the heap AM
    /// handler is unported), so this is declared on the createas (consumer) side
    /// and panics until the AM slot lands. `Err` carries the AM's
    /// `ereport(ERROR)`.
    pub fn table_finish_bulk_insert<'mcx>(
        rel: &types_rel::Relation<'mcx>,
        options: i32,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `ExecuteQuery(pstate, stmt, intoClause, params, dest, qc)` (prepare.c) —
    /// the `CREATE TABLE ... AS EXECUTE` leg: run a prepared statement, sending
    /// its output to `dest` (the `DR_intorel` receiver). `prepare.c`'s
    /// `ExecuteQuery` owns this; it installs the seam (it already depends on this
    /// crate). Panics until wired.
    pub fn execute_query<'mcx>(
        mcx: Mcx<'mcx>,
        estmt: ExecuteStmt<'mcx>,
        into: DdlIntoClause<'mcx>,
        query_string: &str,
        params: ParamListInfoHandle,
        dest: DestReceiverHandle,
        qc: Option<QueryCompletion>,
    ) -> PgResult<Option<QueryCompletion>>
);
