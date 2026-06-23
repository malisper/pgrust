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

use ::mcx::Mcx;
use ::types_catalog::catalog_dependency::ObjectAddress;
use ::types_error::PgResult;
use ::nodes::ddlnodes::{CreateTableAsStmt, ExecuteStmt, IntoClause as DdlIntoClause};
use ::nodes::params::ParamListInfo;
use ::nodes::parsestmt::{DestReceiverHandle, IntoClause};
use ::portal::QueryCompletion;

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
        params: ParamListInfo,
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
    /// `CreateIntoRelDestReceiver(intoClause)` *plus* binding the receiver's
    /// run-state with the live `into` (the owned-model stand-in for C's
    /// `CreateIntoRelDestReceiver` storing `self->into` at receiver creation).
    /// Used by the EXPLAIN-(CTAS) leg (`ExplainOnePlan`), which — unlike
    /// `ExecCreateTableAs` — drives the executor itself and so must set up the
    /// run-state before `ExecutorStart`/`ExecutorRun` invoke `intorel_startup`.
    /// `into` is cloned into the per-query arena `mcx` for the duration of the run.
    /// Returns the receiver handle's raw value (`DestReceiverHandle.0`), which the
    /// EXPLAIN executor-start reconstitutes into the run's dest as `into_receiver`.
    pub fn create_into_rel_dest_receiver_setup<'mcx>(
        mcx: Mcx<'mcx>,
        into: &IntoClause<'mcx>,
    ) -> PgResult<u64>
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
    /// Bundled into one seam because the whole rewrite→plan→run pipeline shares
    /// the active snapshot and the `DR_intorel` receiver and has no
    /// createas-observable intermediate state. INSTALLED by `backend-tcop-pquery`
    /// (the executor-driving layer that owns `CreateQueryDesc` /
    /// `ExecutorStart`..`End` and reaches the rewriter/planner/active-snapshot
    /// machinery): `query_rewrite_canonical` and `pg_plan_query` both take the
    /// value-typed `copy_query::Query<'mcx>` — the same model CTAS carries — so
    /// the prior `portalcmds::Query` reconciliation keystone no longer applies.
    /// Returns the (possibly filled) `QueryCompletion`.
    pub fn run_ctas_executor<'mcx>(
        mcx: Mcx<'mcx>,
        query: ::nodes::copy_query::Query<'mcx>,
        into: ::nodes::ddlnodes::IntoClause<'mcx>,
        query_string: &str,
        params: ParamListInfo,
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
        query: &::nodes::copy_query::Query<'mcx>,
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
        into: ::nodes::ddlnodes::IntoClause<'mcx>,
        attr_list: ::mcx::PgVec<'mcx, ::mcx::PgBox<'mcx, ::nodes::nodes::Node<'mcx>>>,
        relkind: u8,
        is_matview: bool,
    ) -> PgResult<ObjectAddress>
);

seam_core::seam!(
    /// `table_finish_bulk_insert(rel, options)` (tableam.h inline) — flush any
    /// remaining buffered tuples and, for `TABLE_INSERT_SKIP_WAL`, fsync the
    /// relation. Called by `createas.c`'s `intorel_shutdown`. INSTALLED by
    /// `backend-access-table-tableam` (the owner of the tableam.h inline
    /// dispatch wrappers): the C inline only calls the *optional*
    /// `rd_tableam->finish_bulk_insert` slot, which the heap AM
    /// (`heapam_methods`) leaves NULL, so for the only AM in the tree this is a
    /// faithful no-op. `Err` carries the AM's `ereport(ERROR)`.
    pub fn table_finish_bulk_insert<'mcx>(
        rel: &rel::Relation<'mcx>,
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
        params: ParamListInfo,
        dest: DestReceiverHandle,
        qc: Option<QueryCompletion>,
    ) -> PgResult<Option<QueryCompletion>>
);
