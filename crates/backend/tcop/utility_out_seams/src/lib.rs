//! Outward seams the `tcop/utility.c` **classifiers** consult.
//!
//! `backend-tcop-utility` ports the parse-tree classifiers in-crate
//! (read-only classification, command-tag / log-level derivation, the
//! returns-tuples / tuple-descriptor predicates). The only things that cross a
//! seam are the genuine backend-state predicates the read-only / parallel /
//! recovery / security guards read, and the per-statement-source descriptor
//! lookups (`UtilityReturnsTuples` / `UtilityTupleDescriptor`) which reach into
//! the portal / prepared-statement / explain / SHOW owners. Each owning
//! subsystem installs its real implementation when it lands; until then a call
//! panics loudly with the seam path (mirror-PG-and-panic).
//!
//! (The full `ProcessUtility` dispatch — which fans out to ~70 command owners —
//! is not yet ported in `backend-tcop-utility`; see that crate's docs for the
//! `mcx`-threading keystone that blocks it. Its per-command leaf seams will be
//! added here when the dispatch lands.)

#![allow(non_snake_case)]

use mcx::Mcx;
use seam_core::seam;
use nodes::nodes::Node;
use types_tuple::heaptuple::TupleDesc;

/* ===========================================================================
 * backend-state predicates the read-only / parallel / recovery / security
 * guards consult (xact.c / xlog.c / miscinit.c).
 * ======================================================================== */

seam!(
    /// `XactReadOnly` (xact.c) — is the current transaction read-only?
    pub fn xact_read_only() -> bool
);
seam!(
    /// `IsInParallelMode()` (xact.c) — is the current (sub)transaction parallel?
    pub fn is_in_parallel_mode() -> bool
);
seam!(
    /// `RecoveryInProgress()` (xlog.c) — is the server in recovery / hot standby?
    pub fn recovery_in_progress() -> bool
);
seam!(
    /// `InSecurityRestrictedOperation()` (miscinit.c).
    pub fn in_security_restricted_operation() -> bool
);

/* ===========================================================================
 * tuple-returning utility descriptor sources (UtilityReturnsTuples /
 * UtilityTupleDescriptor). A missing portal / prepared statement folds to
 * `false` / `None`, matching the C switches, so these are infallible.
 * ======================================================================== */

seam!(
    /// `GetPortalByName(name) && portal->tupDesc != NULL` (FETCH returns-tuples
    /// predicate; folds the invalid-portal guard).
    pub fn fetch_stmt_portal_tupdesc(stmt: &Node) -> bool
);
seam!(
    /// `FetchPreparedStatement(name, false) && entry->plansource->resultDesc !=
    /// NULL` (EXECUTE returns-tuples predicate).
    pub fn execute_stmt_has_result(stmt: &Node) -> bool
);
seam!(
    /// `CallStmtResultDesc(stmt)` (functioncmds.c) — the CALL result descriptor
    /// from the procedure's OUT/INOUT params, re-typed from `stmt->outargs`. Can
    /// `ereport(ERROR)` on the `build_function_result_tupdesc_t` / `exprType`
    /// cache lookups.
    pub fn call_stmt_result_desc<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>) -> PgResult<TupleDesc<'mcx>>
);
seam!(
    /// FETCH: `CreateTupleDescCopy(GetPortalByName(name)->tupDesc)` (portalmem.c).
    pub fn fetch_stmt_result_desc<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>) -> TupleDesc<'mcx>
);
seam!(
    /// EXECUTE: `FetchPreparedStatementResultDesc(entry)` (prepare.c).
    pub fn execute_stmt_result_desc<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>) -> TupleDesc<'mcx>
);
seam!(
    /// `ExplainResultDesc(stmt)` (explain.c).
    pub fn explain_result_desc<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>) -> TupleDesc<'mcx>
);
seam!(
    /// `GetPGVariableResultDesc(name)` (guc.c) — SHOW result descriptor. Can
    /// `ereport` (the canonical-name lookup runs with `missing_ok = false`).
    pub fn get_pg_variable_result_desc<'mcx>(mcx: Mcx<'mcx>, name: Option<&str>) -> PgResult<TupleDesc<'mcx>>
);

/* ===========================================================================
 * GetCommandLogLevel helpers (define.c / prepare.c).
 * ======================================================================== */

seam!(
    /// `defGetBoolean(opt)` (define.c) — EXPLAIN ANALYZE option scan.
    pub fn def_get_boolean(opt: &Node) -> bool
);
seam!(
    /// EXECUTE: `FetchPreparedStatement(name, false)->plansource->raw_parse_tree`
    /// (prepare.c) — the raw parse tree `GetCommandLogLevel` looks through.
    /// Returns the cached raw parse-tree node, or `None`; a cache read, so
    /// infallible.
    pub fn execute_stmt_raw_parse_tree<'mcx>(stmt: &Node<'mcx>) -> Option<nodes::nodes::NodePtr<'mcx>>
);

/* ===========================================================================
 * `standard_ProcessUtility` dispatch — the per-command leaf seams.
 *
 * The dispatch switch (`crate`'s sibling `backend-tcop-utility::dispatch`) is
 * ported 1:1 over the owned `Node` tree; every *command body* it routes to
 * lives in another subsystem (xact / portalcmds / the `commands` handlers /
 * the event-trigger machinery / the checkpointer / …) and is reached through
 * one of these forwarding seams. A seam carries no dispatch logic — it forwards
 * the already-classified parse tree plus the runtime context the handler needs
 * (the per-utility working `mcx`, the dispatch-owned `ParseState`, the
 * `is_top_level` / atomic flags, …). Each defaults to a loud panic until the
 * owning subsystem installs its real handler at single-threaded startup —
 * never a silent stub (mirror-PG-and-panic). The `&Node` parse-tree form is the
 * dispatch's available shape (the C call-site argument); owners that want an
 * owned typed statement re-form it in their installer via the node's
 * `clone_in`, exactly as the C handlers `copyObject` when they must.
 * ======================================================================== */

use nodes::parsestmt::{ParseState, ProcessUtilityContext};
use nodes::portalcmds::ParamListInfo;
use nodes::parsestmt::DestReceiverHandle;
use nodes::parsenodes::ObjectType;
use portal::QueryCompletion;
use types_error::PgResult;
use types_core::primitive::Oid;
use types_core::init::BackendType;
use types_catalog::catalog_dependency::ObjectAddress;
use types_storage::lock::LOCKMODE;
use nodes::nodes::NodePtr;
use nodes::ddlnodes::CreateStmt;
use nodes::nodeindexscan::PlannedStmt;

/* ---- recursion / readOnlyTree / transaction-state helpers ---- */

seam!(
    /// `check_stack_depth()` (tcop/postgres.c) — guard against the dispatch's
    /// own recursion (utility.c:556). `ereport(ERROR)` on overflow.
    pub fn check_stack_depth() -> PgResult<()>
);
seam!(
    /// `IsTransactionBlock()` (xact.c) — used to compute the atomic-context flag
    /// (utility.c:551).
    pub fn is_transaction_block() -> bool
);
seam!(
    /// `CommandCounterIncrement()` (xact.c) — make the command's effects visible
    /// after the switch (utility.c:1088). `ereport(ERROR)` on CID overflow.
    pub fn command_counter_increment() -> PgResult<()>
);
seam!(
    /// `PreventInTransactionBlock(isTopLevel, stmtType)` (xact.c) — reject a
    /// command that cannot run inside a transaction block. `ereport(ERROR)`.
    pub fn prevent_in_transaction_block(is_top_level: bool, stmt_type: &str) -> PgResult<()>
);
seam!(
    /// `RequireTransactionBlock(isTopLevel, stmtType)` (xact.c) — require a
    /// transaction block (SAVEPOINT/RELEASE/ROLLBACK TO/LOCK). `ereport(ERROR)`.
    pub fn require_transaction_block(is_top_level: bool, stmt_type: &str) -> PgResult<()>
);
seam!(
    /// `WarnNoTransactionBlock(isTopLevel, stmtType)` (xact.c) — warn (not error)
    /// when SET CONSTRAINTS runs outside a transaction block.
    pub fn warn_no_transaction_block(is_top_level: bool, stmt_type: &str) -> PgResult<()>
);

/* ---- privilege / role / backend-identity helpers ---- */

seam!(
    /// `GetUserId()` (miscinit.c) — the current user OID (CHECKPOINT privilege
    /// check, utility.c:951).
    pub fn get_user_id() -> Oid
);
seam!(
    /// `has_privs_of_role(member, role)` (acl.c) — membership test for the
    /// `pg_checkpoint` predefined role (utility.c:951).
    pub fn has_privs_of_role(member: Oid, role: Oid) -> bool
);
seam!(
    /// `superuser()` (superuser.c) — LOAD restricts the allowed filenames for a
    /// non-superuser (utility.c:893).
    pub fn superuser() -> bool
);
seam!(
    /// `MyBackendType` (miscinit.c) — LISTEN is rejected in background processes
    /// (utility.c:822).
    pub fn my_backend_type() -> BackendType
);
seam!(
    /// `RecoveryInProgress()` (xlog.c) — CHECKPOINT omits `CHECKPOINT_FORCE`
    /// during recovery (utility.c:963). (Distinct from the classifier's
    /// `recovery_in_progress` above; same backend predicate.)
    pub fn checkpoint_recovery_in_progress() -> bool
);

/* ---- event-trigger machinery (the "fast path" arms + slow path) ---- */

seam!(
    /// `EventTriggerSupportsObjectType(objtype)` (event_trigger.c) — does this
    /// object type have event-trigger support? The GRANT/DROP/RENAME/ALTER…/
    /// COMMENT/SECURITY LABEL arms route to [`process_utility_slow`] when it
    /// does, and to the direct handler otherwise (utility.c:1003-1073).
    pub fn event_trigger_supports_object_type(objtype: ObjectType) -> bool
);
seam!(
    /// `ProcessUtilitySlow(pstate, pstmt, queryString, context, params,
    /// queryEnv, dest, qc)` (utility.c:1158, static) — the event-trigger-fenced
    /// half of the dispatch, which fans out to every event-trigger-supporting
    /// DDL command (CREATE/ALTER TABLE → `DefineRelation`/`AlterTable`, CREATE
    /// INDEX → `DefineIndex`, CREATE FUNCTION, …) plus the `parse_utilcmd.c`
    /// transforms. It owns the `EventTriggerData` complex-command fences. The
    /// owner is a dedicated wiring point installed when its (large) command fan-
    /// out lands; until then this panics. `ereport(ERROR)`.
    pub fn process_utility_slow<'mcx>(
        mcx: Mcx<'mcx>,
        pstate: &mut ParseState<'mcx>,
        pstmt: &nodes::nodeindexscan::PlannedStmt<'mcx>,
        query_string: &str,
        context: ProcessUtilityContext,
        params: ParamListInfo,
        dest: DestReceiverHandle,
        is_top_level: bool,
        qc: Option<&mut QueryCompletion>,
    ) -> PgResult<()>
);
seam!(
    /// `EventTriggerAlterTableEnd()` (event_trigger.c) — close the current
    /// complex-command set before an ALTER TABLE subcommand re-enters the
    /// dispatch (`ProcessUtilityForAlterTable`, utility.c:1593).
    pub fn event_trigger_alter_table_end() -> PgResult<()>
);
seam!(
    /// `EventTriggerAlterTableStart(parsetree)` (event_trigger.c) — open a new
    /// complex-command set after the subcommand returns
    /// (`ProcessUtilityForAlterTable`, utility.c:1607).
    pub fn event_trigger_alter_table_start<'mcx>(parsetree: &Node<'mcx>) -> PgResult<()>
);
seam!(
    /// `EventTriggerAlterTableRelid(relid)` (event_trigger.c) — record the
    /// relation being altered (`ProcessUtilityForAlterTable`, utility.c:1608).
    pub fn event_trigger_alter_table_relid(relid: Oid)
);
seam!(
    /// `EventTriggerCollectAlterTableSubcmd(subcmd, address)` (event_trigger.c)
    /// — record one ALTER TABLE subcommand and its resulting object address on
    /// the in-progress `SCT_AlterTable` command (`ATExecCmd`, tablecmds.c:5688).
    /// A no-op without an active collection state.
    pub fn event_trigger_collect_alter_table_subcmd<'mcx>(
        subcmd: &Node<'mcx>,
        address: ObjectAddress,
    ) -> PgResult<()>
);
seam!(
    /// `ProcessUtility(wrapper)` (utility.c:1595-1606) — build the subcommand
    /// `PlannedStmt` around `stmt` with a `None` (DestNone) receiver and re-enter
    /// the dispatch. Encapsulated as a seam because the wrapper-`PlannedStmt`
    /// build + `None`-receiver creation reach the portal/dest owners. The owner
    /// installs it alongside the event-trigger machinery; until then it panics.
    pub fn process_utility_wrapper<'mcx>(
        mcx: Mcx<'mcx>,
        stmt: &Node<'mcx>,
        query_string: &str,
        stmt_location: i32,
        stmt_len: i32,
    ) -> PgResult<()>
);

/* ---- transaction-control verbs (xact.c) ---- */

seam!(
    /// `BeginTransactionBlock()` (xact.c) — BEGIN / START TRANSACTION.
    pub fn begin_transaction_block() -> PgResult<()>
);
seam!(
    /// `EndTransactionBlock(chain)` (xact.c) — COMMIT; returns whether the
    /// commit will actually happen (`false` ⇒ report ROLLBACK in the qc).
    pub fn end_transaction_block(chain: bool) -> PgResult<bool>
);
seam!(
    /// `PrepareTransactionBlock(gid)` (xact.c) — PREPARE TRANSACTION; returns
    /// whether it will commit.
    pub fn prepare_transaction_block(gid: Option<&str>) -> PgResult<bool>
);
seam!(
    /// `FinishPreparedTransaction(gid, isCommit)` (twophase.c) — COMMIT/ROLLBACK
    /// PREPARED.
    pub fn finish_prepared_transaction(gid: Option<&str>, is_commit: bool) -> PgResult<()>
);
seam!(
    /// `UserAbortTransactionBlock(chain)` (xact.c) — ROLLBACK.
    pub fn user_abort_transaction_block(chain: bool) -> PgResult<()>
);
seam!(
    /// `DefineSavepoint(name)` (xact.c) — SAVEPOINT.
    pub fn define_savepoint(name: Option<&str>) -> PgResult<()>
);
seam!(
    /// `ReleaseSavepoint(name)` (xact.c) — RELEASE SAVEPOINT.
    pub fn release_savepoint(name: Option<&str>) -> PgResult<()>
);
seam!(
    /// `RollbackToSavepoint(name)` (xact.c) — ROLLBACK TO SAVEPOINT.
    pub fn rollback_to_savepoint(name: Option<&str>) -> PgResult<()>
);
seam!(
    /// `SetPGVariable(name, list_make1(arg), isLocal)` (guc.c) — the BEGIN
    /// transaction-characteristics options (`transaction_isolation` etc.,
    /// utility.c:611-642). The seam takes the single option-value `Node`
    /// directly; the handler re-forms the C `list_make1`.
    pub fn set_pg_variable<'mcx>(name: &str, arg: &Node<'mcx>, is_local: bool) -> PgResult<()>
);

/* ---- portal (cursor) verbs (portalcmds.c) ---- */

seam!(
    /// `PerformCursorOpen(pstate, cstmt, params, isTopLevel)` (portalcmds.c) —
    /// DECLARE CURSOR.
    pub fn perform_cursor_open<'mcx>(
        mcx: Mcx<'mcx>,
        pstate: &mut ParseState<'mcx>,
        cstmt: &Node<'mcx>,
        params: ParamListInfo,
        is_top_level: bool,
    ) -> PgResult<()>
);
seam!(
    /// `PerformPortalClose(name)` (portalcmds.c) — CLOSE.
    pub fn perform_portal_close(name: Option<&str>) -> PgResult<()>
);
seam!(
    /// `PerformPortalFetch(stmt, dest, qc)` (portalcmds.c) — FETCH/MOVE.
    pub fn perform_portal_fetch<'mcx>(
        mcx: Mcx<'mcx>,
        stmt: &Node<'mcx>,
        dest: DestReceiverHandle,
        qc: Option<&mut QueryCompletion>,
    ) -> PgResult<()>
);

/* ---- DO / CALL (functioncmds.c) ---- */

seam!(
    /// `ExecuteDoStmt(pstate, stmt, atomic)` (functioncmds.c) — DO.
    pub fn execute_do_stmt<'mcx>(
        mcx: Mcx<'mcx>,
        pstate: &mut ParseState<'mcx>,
        stmt: &Node<'mcx>,
        atomic: bool,
    ) -> PgResult<()>
);
seam!(
    /// `ExecuteCallStmt(stmt, params, atomic, dest)` (functioncmds.c) — CALL.
    pub fn execute_call_stmt<'mcx>(
        mcx: Mcx<'mcx>,
        stmt: &Node<'mcx>,
        params: ParamListInfo,
        atomic: bool,
        dest: DestReceiverHandle,
    ) -> PgResult<()>
);

/* ---- tablespace globals (tablespace.c) ---- */

seam!(
    /// `CreateTableSpace(stmt)` (tablespace.c) — CREATE TABLESPACE.
    pub fn create_table_space<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>) -> PgResult<()>
);
seam!(
    /// `DropTableSpace(stmt)` (tablespace.c) — DROP TABLESPACE.
    pub fn drop_table_space<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>) -> PgResult<()>
);
seam!(
    /// `AlterTableSpaceOptions(stmt)` (tablespace.c) — ALTER TABLESPACE … SET.
    pub fn alter_table_space_options<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>) -> PgResult<()>
);

/* ---- TRUNCATE / COPY (tablecmds.c / copy.c) ---- */

seam!(
    /// `ExecuteTruncate(stmt)` (tablecmds.c) — TRUNCATE.
    pub fn execute_truncate<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>) -> PgResult<()>
);
seam!(
    /// `DoCopy(pstate, stmt, stmt_location, stmt_len, &processed)` (copy.c) —
    /// COPY; returns the number of rows processed.
    pub fn do_copy<'mcx>(
        mcx: Mcx<'mcx>,
        pstate: &mut ParseState<'mcx>,
        stmt: &Node<'mcx>,
        stmt_location: i32,
        stmt_len: i32,
    ) -> PgResult<u64>
);

/* ---- PREPARE / EXECUTE / DEALLOCATE (prepare.c) ---- */

seam!(
    /// `PrepareQuery(pstate, stmt, stmt_location, stmt_len)` (prepare.c).
    pub fn prepare_query<'mcx>(
        mcx: Mcx<'mcx>,
        pstate: &mut ParseState<'mcx>,
        stmt: &Node<'mcx>,
        stmt_location: i32,
        stmt_len: i32,
    ) -> PgResult<()>
);
seam!(
    /// `ExecuteQuery(pstate, stmt, NULL, params, dest, qc)` (prepare.c) — EXECUTE
    /// (the standalone, non-CTAS form; `intoClause` is `NULL`).
    pub fn execute_query<'mcx>(
        mcx: Mcx<'mcx>,
        pstate: &mut ParseState<'mcx>,
        stmt: &Node<'mcx>,
        params: ParamListInfo,
        dest: DestReceiverHandle,
        qc: Option<&mut QueryCompletion>,
    ) -> PgResult<()>
);
seam!(
    /// `DeallocateQuery(stmt)` (prepare.c) — DEALLOCATE.
    pub fn deallocate_query<'mcx>(stmt: &Node<'mcx>) -> PgResult<()>
);

/* ---- database / role globals (dbcommands.c / user.c) ---- */

seam!(
    /// `GrantRole(pstate, stmt)` (acl.c) — GRANT/REVOKE role.
    pub fn grant_role<'mcx>(mcx: Mcx<'mcx>, pstate: &mut ParseState<'mcx>, stmt: &Node<'mcx>) -> PgResult<()>
);
seam!(
    /// `createdb(pstate, stmt)` (dbcommands.c) — CREATE DATABASE.
    pub fn createdb<'mcx>(mcx: Mcx<'mcx>, pstate: &mut ParseState<'mcx>, stmt: &Node<'mcx>) -> PgResult<()>
);
seam!(
    /// `AlterDatabase(pstate, stmt, isTopLevel)` (dbcommands.c).
    pub fn alter_database<'mcx>(
        mcx: Mcx<'mcx>,
        pstate: &mut ParseState<'mcx>,
        stmt: &Node<'mcx>,
        is_top_level: bool,
    ) -> PgResult<()>
);
seam!(
    /// `AlterDatabaseRefreshColl(stmt)` (dbcommands.c).
    pub fn alter_database_refresh_coll<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>) -> PgResult<()>
);
seam!(
    /// `AlterDatabaseSet(stmt)` (dbcommands.c) — ALTER DATABASE … SET.
    pub fn alter_database_set<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>) -> PgResult<()>
);
seam!(
    /// `DropDatabase(pstate, stmt)` (dbcommands.c) — DROP DATABASE.
    pub fn drop_database<'mcx>(mcx: Mcx<'mcx>, pstate: &mut ParseState<'mcx>, stmt: &Node<'mcx>) -> PgResult<()>
);

/* ---- LISTEN / NOTIFY / UNLISTEN (async.c) ---- */

seam!(
    /// `Async_Notify(conditionname, payload)` (async.c) — NOTIFY.
    pub fn async_notify(conditionname: Option<&str>, payload: Option<&str>) -> PgResult<()>
);
seam!(
    /// `Async_Listen(conditionname)` (async.c) — LISTEN.
    pub fn async_listen(conditionname: &str) -> PgResult<()>
);
seam!(
    /// `Async_Unlisten(conditionname)` (async.c) — UNLISTEN <name>.
    pub fn async_unlisten(conditionname: &str) -> PgResult<()>
);
seam!(
    /// `Async_UnlistenAll()` (async.c) — UNLISTEN *.
    pub fn async_unlisten_all() -> PgResult<()>
);

/* ---- LOAD (dfmgr.c / fd.c) ---- */

seam!(
    /// `closeAllVfds()` (fd.c) — LOAD closes all VFDs first (utility.c:888).
    pub fn close_all_vfds()
);
seam!(
    /// `load_file(filename, restricted)` (dfmgr.c) — LOAD.
    pub fn load_file(filename: Option<&str>, restricted: bool) -> PgResult<()>
);

/* ---- CLUSTER / VACUUM / EXPLAIN (cluster.c / vacuum.c / explain.c) ---- */

seam!(
    /// `cluster(pstate, stmt, isTopLevel)` (cluster.c) — CLUSTER.
    pub fn cluster<'mcx>(
        mcx: Mcx<'mcx>,
        pstate: &mut ParseState<'mcx>,
        stmt: &Node<'mcx>,
        is_top_level: bool,
    ) -> PgResult<()>
);
seam!(
    /// `ExecVacuum(pstate, stmt, isTopLevel)` (vacuum.c) — VACUUM / ANALYZE.
    pub fn exec_vacuum<'mcx>(
        mcx: Mcx<'mcx>,
        pstate: &mut ParseState<'mcx>,
        stmt: &Node<'mcx>,
        is_top_level: bool,
    ) -> PgResult<()>
);
seam!(
    /// `ExplainQuery(pstate, stmt, params, dest)` (explain.c) — EXPLAIN.
    pub fn explain_query<'mcx>(
        mcx: Mcx<'mcx>,
        pstate: &mut ParseState<'mcx>,
        stmt: &Node<'mcx>,
        params: ParamListInfo,
        dest: DestReceiverHandle,
    ) -> PgResult<()>
);

/* ---- ALTER SYSTEM / SET / SHOW / DISCARD (guc-funcs / guc.c / discard.c) ---- */

seam!(
    /// `AlterSystemSetConfigFile(stmt)` (guc.c) — ALTER SYSTEM.
    pub fn alter_system_set_config_file<'mcx>(stmt: &Node<'mcx>) -> PgResult<()>
);
seam!(
    /// `ExecSetVariableStmt(stmt, isTopLevel)` (guc-funcs.c) — SET.
    pub fn exec_set_variable_stmt<'mcx>(stmt: &Node<'mcx>, is_top_level: bool) -> PgResult<()>
);
seam!(
    /// `GetPGVariable(name, dest)` (guc-funcs.c) — SHOW. The dispatch passes only
    /// the variable name; the handler reaches the portal/dest sink it writes to.
    pub fn get_pg_variable<'mcx>(mcx: Mcx<'mcx>, name: Option<&str>, dest: DestReceiverHandle) -> PgResult<()>
);
seam!(
    /// `DiscardCommand(stmt, isTopLevel)` (discard.c) — DISCARD.
    pub fn discard_command<'mcx>(stmt: &Node<'mcx>, is_top_level: bool) -> PgResult<()>
);

/* ---- event triggers (event_trigger.c) ---- */

seam!(
    /// `CreateEventTrigger(stmt)` (event_trigger.c) — CREATE EVENT TRIGGER.
    pub fn create_event_trigger<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>) -> PgResult<()>
);
seam!(
    /// `AlterEventTrigger(stmt)` (event_trigger.c) — ALTER EVENT TRIGGER.
    pub fn alter_event_trigger<'mcx>(stmt: &Node<'mcx>) -> PgResult<()>
);

/* ---- ROLE statements (user.c) ---- */

seam!(
    /// `CreateRole(pstate, stmt)` (user.c) — CREATE ROLE/USER/GROUP.
    pub fn create_role<'mcx>(mcx: Mcx<'mcx>, pstate: &mut ParseState<'mcx>, stmt: &Node<'mcx>) -> PgResult<()>
);
seam!(
    /// `AlterRole(pstate, stmt)` (user.c) — ALTER ROLE.
    pub fn alter_role<'mcx>(mcx: Mcx<'mcx>, pstate: &mut ParseState<'mcx>, stmt: &Node<'mcx>) -> PgResult<()>
);
seam!(
    /// `AlterRoleSet(stmt)` (user.c) — ALTER ROLE … SET.
    pub fn alter_role_set<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>) -> PgResult<()>
);
seam!(
    /// `DropRole(stmt)` (user.c) — DROP ROLE.
    pub fn drop_role<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>) -> PgResult<()>
);
seam!(
    /// `ReassignOwnedObjects(stmt)` (user.c) — REASSIGN OWNED. Allocates the
    /// role-name lookups in the caller's context.
    pub fn reassign_owned_objects<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>) -> PgResult<()>
);

/* ---- LOCK / SET CONSTRAINTS / CHECKPOINT (lockcmds / trigger / xlog) ---- */

seam!(
    /// `LockTableCommand(stmt)` (lockcmds.c) — LOCK TABLE.
    pub fn lock_table_command<'mcx>(stmt: &Node<'mcx>) -> PgResult<()>
);
seam!(
    /// `AfterTriggerSetState(stmt)` (trigger.c) — SET CONSTRAINTS.
    pub fn after_trigger_set_state<'mcx>(stmt: &Node<'mcx>) -> PgResult<()>
);
seam!(
    /// `RequestCheckpoint(flags)` (checkpointer.c) — CHECKPOINT.
    pub fn request_checkpoint(flags: i32) -> PgResult<()>
);

/* ---- the "fast path" direct handlers (used when there is no event-trigger
 * support for the object type; otherwise the arm routes to
 * `process_utility_slow`) ---- */

seam!(
    /// `ExecuteGrantStmt(stmt)` (aclchk.c) — GRANT/REVOKE (non-event-trigger).
    pub fn execute_grant_stmt<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>) -> PgResult<()>
);
seam!(
    /// `RemoveRelations(stmt)` (tablecmds.c) — DROP TABLE/SEQUENCE/VIEW/MATVIEW/
    /// FOREIGN TABLE/INDEX (the `ExecDropStmt` relation legs).
    pub fn remove_relations<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>) -> PgResult<()>
);
seam!(
    /// `RemoveObjects(stmt)` (dropcmds.c) — DROP <general object> (the
    /// `ExecDropStmt` default leg).
    pub fn remove_objects<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>) -> PgResult<()>
);
seam!(
    /// `ExecRenameStmt(stmt)` (alter.c) — RENAME (non-event-trigger).
    pub fn exec_rename_stmt<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>) -> PgResult<()>
);
seam!(
    /// `ExecAlterObjectDependsStmt(stmt, NULL)` (alter.c) — ALTER … DEPENDS ON
    /// EXTENSION (non-event-trigger).
    pub fn exec_alter_object_depends_stmt<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>) -> PgResult<()>
);
seam!(
    /// `ExecAlterObjectSchemaStmt(stmt, NULL)` (alter.c) — ALTER … SET SCHEMA
    /// (non-event-trigger).
    pub fn exec_alter_object_schema_stmt<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>) -> PgResult<()>
);
seam!(
    /// `ExecAlterOwnerStmt(stmt)` (alter.c) — ALTER … OWNER TO (non-event-trigger).
    pub fn exec_alter_owner_stmt<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>) -> PgResult<()>
);
seam!(
    /// `CommentObject(stmt)` (comment.c) — COMMENT (non-event-trigger).
    pub fn comment_object<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>) -> PgResult<()>
);
seam!(
    /// `ExecSecLabelStmt(stmt)` (seclabel.c) — SECURITY LABEL (non-event-trigger).
    pub fn exec_sec_label_stmt<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>) -> PgResult<()>
);

/* ===========================================================================
 * `ProcessUtilitySlow` (utility.c:1092-1581) — the event-trigger-fenced DDL
 * fan-out. Reached from the dispatch's GRANT/DROP/RENAME/ALTER…/COMMENT/SECURITY
 * LABEL "fast path" arms (when `EventTriggerSupportsObjectType` is true) and the
 * dispatch `_ =>` arm. Every command body lives in its own subsystem and is
 * reached through one of these thin forwarding seams; each defaults to a loud
 * documented panic until its owning subsystem installs the real handler.
 *
 * INSTALLED today (the reachable CREATE TABLE spine):
 *   * `transform_create_stmt`  ← backend-parser-parse-utilcmd
 *   * `define_relation`        ← backend-commands-tablecmds
 *   * `create_toast_for_relation` ← backend-commands-tablecmds (the
 *     `transformRelOptions("toast") + heap_reloptions + NewRelationCreateToastTable`
 *     sequence; bundled because it shares the new relation OID + reloptions Datum
 *     and lands entirely in the catalog, exactly as createas.c::create_ctas_relation).
 *
 * UNINSTALLED (documented panic until the owner lands): the event-trigger fences
 * (event_trigger.c is unported), every other DDL command handler, and the
 * `transform_index_stmt` / `transform_stats_stmt` parse-analysis transforms whose
 * owners are not yet wired.
 * ======================================================================== */

/* ---- parse_utilcmd.c transforms ---- */

seam!(
    /// `transformCreateStmt(stmt, queryString)` (parse_utilcmd.c) — parse analysis
    /// for CREATE TABLE / CREATE FOREIGN TABLE. Returns the post-transform list of
    /// statements (`CreateStmt`, `CreateForeignTableStmt`, `TableLikeClause`, or
    /// other nodes to recurse on). Installed by backend-parser-parse-utilcmd.
    pub fn transform_create_stmt<'mcx>(
        mcx: Mcx<'mcx>,
        stmt: NodePtr<'mcx>,
        query_string: &str,
    ) -> PgResult<mcx::PgVec<'mcx, NodePtr<'mcx>>>
);
seam!(
    /// `transformIndexStmt(relid, stmt, queryString)` (parse_utilcmd.c) — parse
    /// analysis for CREATE INDEX. Returns the transformed `IndexStmt`.
    pub fn transform_index_stmt<'mcx>(
        mcx: Mcx<'mcx>,
        relid: Oid,
        stmt: NodePtr<'mcx>,
        query_string: &str,
    ) -> PgResult<NodePtr<'mcx>>
);
seam!(
    /// `transformStatsStmt(relid, stmt, queryString)` (parse_utilcmd.c) — parse
    /// analysis for CREATE STATISTICS. Returns the transformed `CreateStatsStmt`.
    pub fn transform_stats_stmt<'mcx>(
        mcx: Mcx<'mcx>,
        relid: Oid,
        stmt: NodePtr<'mcx>,
        query_string: &str,
    ) -> PgResult<NodePtr<'mcx>>
);
seam!(
    /// `expandTableLikeClause(heapRel, like_clause)` (tablecmds.c) — the delayed
    /// CREATE TABLE … (LIKE …) processing, producing additional sub-statements.
    pub fn expand_table_like_clause<'mcx>(
        mcx: Mcx<'mcx>,
        heap_rv: NodePtr<'mcx>,
        like_clause: NodePtr<'mcx>,
    ) -> PgResult<mcx::PgVec<'mcx, NodePtr<'mcx>>>
);

/* ---- event-trigger fences (event_trigger.c — unported) ---- */

seam!(
    /// `EventTriggerBeginCompleteQuery()` (event_trigger.c) — install event-trigger
    /// query state for the duration of a complete query; returns whether cleanup is
    /// needed (false when no sql_drop/table_rewrite/ddl_command_end triggers exist).
    /// The interest test calls `EventCacheLookup`, whose cache rebuild scans
    /// `pg_event_trigger` and can `ereport(ERROR)`, carried on `Err`.
    pub fn event_trigger_begin_complete_query() -> PgResult<bool>
);
seam!(
    /// `EventTriggerEndCompleteQuery()` (event_trigger.c) — tear down the state
    /// installed by `EventTriggerBeginCompleteQuery` (run only when it returned true).
    pub fn event_trigger_end_complete_query()
);
seam!(
    /// `EventTriggerDDLCommandStart(parsetree)` (event_trigger.c) — fire
    /// `ddl_command_start` event triggers.
    pub fn event_trigger_ddl_command_start<'mcx>(parsetree: &Node<'mcx>) -> PgResult<()>
);
seam!(
    /// `EventTriggerDDLCommandEnd(parsetree)` (event_trigger.c) — fire
    /// `ddl_command_end` event triggers.
    pub fn event_trigger_ddl_command_end<'mcx>(parsetree: &Node<'mcx>) -> PgResult<()>
);
seam!(
    /// `EventTriggerSQLDrop(parsetree)` (event_trigger.c) — fire `sql_drop` event
    /// triggers for the objects dropped by the command.
    pub fn event_trigger_sql_drop<'mcx>(parsetree: &Node<'mcx>) -> PgResult<()>
);
seam!(
    /// `EventTriggerCollectSimpleCommand(address, secondaryObject, parsetree)`
    /// (event_trigger.c) — stash a completed command for `ddl_command_end`. The
    /// C body `copyObject`s the parse tree into the event-trigger state context;
    /// that allocation can `ereport(ERROR)` (palloc OOM), carried on `Err`.
    pub fn event_trigger_collect_simple_command<'mcx>(
        address: ObjectAddress,
        secondary_object: ObjectAddress,
        parsetree: &Node<'mcx>,
    ) -> PgResult<()>
);
seam!(
    /// `EventTriggerCollectAlterDefPrivs(stmt)` (event_trigger.c) — stash an ALTER
    /// DEFAULT PRIVILEGES command. The active-collection path deep-copies the
    /// parse tree (palloc can `ereport(ERROR)`, carried on `Err`); a no-op
    /// (early `Ok`) without an active collection state.
    pub fn event_trigger_collect_alter_def_privs<'mcx>(stmt: &Node<'mcx>) -> PgResult<()>
);
seam!(
    /// `EventTriggerInhibitCommandCollection()` (event_trigger.c) — suppress DDL
    /// command collection (REFRESH MATERIALIZED VIEW CONCURRENTLY).
    pub fn event_trigger_inhibit_command_collection()
);
seam!(
    /// `EventTriggerUndoInhibitCommandCollection()` (event_trigger.c) — restore DDL
    /// command collection after REFRESH … CONCURRENTLY.
    pub fn event_trigger_undo_inhibit_command_collection()
);

/* ---- relation / type creation (tablecmds.c / heap.c / typecmds.c) ---- */

seam!(
    /// `DefineRelation(stmt, relkind, ownerId, NULL, queryString)` (tablecmds.c) —
    /// the CREATE TABLE / CREATE relation driver. Returns the new relation's
    /// `ObjectAddress`. Installed by backend-commands-tablecmds.
    pub fn define_relation<'mcx>(
        mcx: Mcx<'mcx>,
        stmt: CreateStmt<'mcx>,
        relkind: u8,
        owner_id: Oid,
        query_string: Option<&str>,
    ) -> PgResult<ObjectAddress>
);
seam!(
    /// The CREATE TABLE TOAST-table follow-on (utility.c:1170-1188):
    /// `transformRelOptions((Datum) 0, cstmt->options, "toast", HEAP_RELOPT_NAMESPACES,
    /// true, false)` + `heap_reloptions(RELKIND_TOASTVALUE, toast_options, true)` +
    /// `NewRelationCreateToastTable(relid, toast_options)`. Bundled into one owner
    /// step because it shares the new relation OID and the reloptions Datum and
    /// lands entirely in the catalog (cf. createas.c::create_ctas_relation).
    /// Installed by backend-commands-tablecmds.
    pub fn create_toast_for_relation<'mcx>(
        mcx: Mcx<'mcx>,
        relid: Oid,
        options: &mcx::PgVec<'mcx, NodePtr<'mcx>>,
    ) -> PgResult<()>
);
seam!(
    /// `CreateForeignTable(stmt, relid)` (foreigncmds.c) — CREATE FOREIGN TABLE.
    pub fn create_foreign_table<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>, relid: Oid) -> PgResult<()>
);
seam!(
    /// `CreateSchemaCommand(stmt, queryString, stmt_location, stmt_len)`
    /// (schemacmds.c) — CREATE SCHEMA.
    pub fn create_schema_command<'mcx>(
        mcx: Mcx<'mcx>,
        stmt: &Node<'mcx>,
        query_string: &str,
        stmt_location: i32,
        stmt_len: i32,
    ) -> PgResult<()>
);
seam!(
    /// `DefineCompositeType(typevar, coldeflist)` (typecmds.c) — CREATE TYPE (composite).
    pub fn define_composite_type<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>) -> PgResult<ObjectAddress>
);
seam!(
    /// `DefineEnum(stmt)` (typecmds.c) — CREATE TYPE AS ENUM.
    pub fn define_enum<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>) -> PgResult<ObjectAddress>
);
seam!(
    /// `DefineRange(pstate, stmt)` (typecmds.c) — CREATE TYPE AS RANGE.
    pub fn define_range<'mcx>(mcx: Mcx<'mcx>, pstate: &mut ParseState<'mcx>, stmt: &Node<'mcx>) -> PgResult<ObjectAddress>
);
seam!(
    /// `AlterEnum(stmt)` (enum.c) — ALTER TYPE (enum).
    pub fn alter_enum<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>) -> PgResult<ObjectAddress>
);
seam!(
    /// `CreateForeignDataWrapper(pstate, stmt)` (foreigncmds.c) — CREATE FOREIGN
    /// DATA WRAPPER. The owner adapter decodes the rich `CreateFdwStmt` into the
    /// flat `types_foreigncmds` form the ported body consumes.
    pub fn create_foreign_data_wrapper<'mcx>(
        mcx: Mcx<'mcx>,
        pstate: &mut ParseState<'mcx>,
        stmt: &Node<'mcx>,
    ) -> PgResult<ObjectAddress>
);
seam!(
    /// `AlterForeignDataWrapper(pstate, stmt)` (foreigncmds.c) — ALTER FOREIGN
    /// DATA WRAPPER.
    pub fn alter_foreign_data_wrapper<'mcx>(
        mcx: Mcx<'mcx>,
        pstate: &mut ParseState<'mcx>,
        stmt: &Node<'mcx>,
    ) -> PgResult<ObjectAddress>
);
seam!(
    /// `CreateForeignServer(stmt)` (foreigncmds.c) — CREATE SERVER.
    pub fn create_foreign_server<'mcx>(
        mcx: Mcx<'mcx>,
        stmt: &Node<'mcx>,
    ) -> PgResult<ObjectAddress>
);
seam!(
    /// `AlterForeignServer(stmt)` (foreigncmds.c) — ALTER SERVER.
    pub fn alter_foreign_server<'mcx>(
        mcx: Mcx<'mcx>,
        stmt: &Node<'mcx>,
    ) -> PgResult<ObjectAddress>
);
seam!(
    /// `CreateUserMapping(stmt)` (foreigncmds.c) — CREATE USER MAPPING.
    pub fn create_user_mapping<'mcx>(
        mcx: Mcx<'mcx>,
        stmt: &Node<'mcx>,
    ) -> PgResult<ObjectAddress>
);
seam!(
    /// `AlterUserMapping(stmt)` (foreigncmds.c) — ALTER USER MAPPING.
    pub fn alter_user_mapping<'mcx>(
        mcx: Mcx<'mcx>,
        stmt: &Node<'mcx>,
    ) -> PgResult<ObjectAddress>
);
seam!(
    /// `RemoveUserMapping(stmt)` (foreigncmds.c) — DROP USER MAPPING. No commands
    /// stashed for DROP; the returned OID is unused by the slow path.
    pub fn remove_user_mapping<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>) -> PgResult<()>
);
seam!(
    /// `ImportForeignSchema(stmt)` (foreigncmds.c) — IMPORT FOREIGN SCHEMA.
    /// Stashes its own commands; returns no address.
    pub fn import_foreign_schema<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>) -> PgResult<()>
);
seam!(
    /// `DefineDomain(pstate, stmt)` (typecmds.c) — CREATE DOMAIN.
    pub fn define_domain<'mcx>(mcx: Mcx<'mcx>, pstate: &mut ParseState<'mcx>, stmt: &Node<'mcx>) -> PgResult<ObjectAddress>
);
seam!(
    /// The `AlterDomainStmt` subtype switch (utility.c:1289-1340): `AlterDomainDefault`
    /// / `AlterDomainNotNull` / `AlterDomainAddConstraint` / `AlterDomainDropConstraint`
    /// / `AlterDomainValidateConstraint` (typecmds.c). Returns the address; the
    /// `secondaryObject` (ADD CONSTRAINT) is not carried at this leaf.
    pub fn alter_domain<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>) -> PgResult<ObjectAddress>
);

/* ---- object-creation fan-out (define.c / typecmds.c / functioncmds.c / …) ---- */

seam!(
    /// The `DefineStmt` `kind` switch (utility.c:1343-1393): `DefineAggregate` /
    /// `DefineOperator` / `DefineType` / `DefineTS*` / `DefineCollation`
    /// (aggregatecmds.c / operatorcmds.c / typecmds.c / tsearchcmds.c). Returns the
    /// address; the TS-CONFIGURATION/`secondaryObject` is not carried at this leaf.
    pub fn define_stmt<'mcx>(mcx: Mcx<'mcx>, pstate: &mut ParseState<'mcx>, stmt: &Node<'mcx>) -> PgResult<ObjectAddress>
);
seam!(
    /// The whole `T_AlterTableStmt` arm (utility.c:1261-1287): the DETACH
    /// CONCURRENTLY transaction-block guard, `AlterTableGetLockLevel` +
    /// `AlterTableLookupRelation` (tablecmds.c), the `EventTriggerAlterTableStart`
    /// / `EventTriggerAlterTableRelid` fence, `AlterTable`, and the
    /// `EventTriggerAlterTableEnd` close (or the "does not exist, skipping" NOTICE).
    /// Bundled because the lock mode / relid / `AlterTableUtilityContext` are all
    /// tablecmds-internal and interleave with the event-trigger fence. The original
    /// `pstmt` is threaded so the recursive callbacks can rebuild it.
    pub fn alter_table_slow<'mcx>(
        mcx: Mcx<'mcx>,
        pstmt: &PlannedStmt<'mcx>,
        stmt: &Node<'mcx>,
        query_string: &str,
        params: ParamListInfo,
        is_top_level: bool,
    ) -> PgResult<()>
);
seam!(
    /// `AlterTableMoveAll(stmt)` (tablecmds.c:16985) — ALTER (TABLE|INDEX|
    /// MATERIALIZED VIEW) ALL IN TABLESPACE x [OWNED BY ...] SET TABLESPACE y
    /// (utility.c:1767). Scans `pg_class` for the relations in the source
    /// tablespace matching the requested relkind (skipping catalog/shared/temp/
    /// toast and, when `OWNED BY` was given, relations not owned by one of the
    /// named roles), then drives `ATExecSetTableSpace` per relation through
    /// `AlterTableInternal` inside an event-trigger fence. The commands are
    /// stashed inside, so the dispatcher sets `commandCollected = true`. The
    /// `Node` carries the `AlterTableMoveAllStmt`.
    pub fn alter_table_move_all<'mcx>(
        mcx: Mcx<'mcx>,
        stmt: &Node<'mcx>,
    ) -> PgResult<()>
);
seam!(
    /// The CREATE INDEX partition-recursion pre-check (utility.c:1418-1452): when
    /// `stmt->relation->inh` and the relation is a partitioned table, lock all
    /// inheritors (`find_all_inheritors`), validate each partition relkind, and
    /// return the partition count (`list_length(inheritors) - 1`); otherwise `-1`.
    /// Bundled because it shares `relid`/`lockmode` and reaches the relcache /
    /// inheritance machinery.
    pub fn create_index_count_partitions<'mcx>(
        mcx: Mcx<'mcx>,
        relid: Oid,
        stmt: NodePtr<'mcx>,
        lockmode: LOCKMODE,
    ) -> PgResult<i32>
);
seam!(
    /// `DefineIndex(relid, stmt, InvalidOid, InvalidOid, InvalidOid, nparts,
    /// is_alter_table, true, true, false, false)` (indexcmds.c) — CREATE INDEX.
    /// Returns the new index's `ObjectAddress`.
    pub fn define_index<'mcx>(
        mcx: Mcx<'mcx>,
        relid: Oid,
        stmt: NodePtr<'mcx>,
        nparts: i32,
        is_alter_table: bool,
    ) -> PgResult<ObjectAddress>
);
seam!(
    /// `RangeVarGetRelidExtended(stmt->relation, lockmode, 0,
    /// RangeVarCallbackOwnsRelation, NULL)` (namespace.c) — the CREATE INDEX
    /// relation-OID lookup with the owns-relation callback.
    pub fn range_var_get_relid_owns_relation<'mcx>(
        mcx: Mcx<'mcx>,
        relation: NodePtr<'mcx>,
        lockmode: LOCKMODE,
    ) -> PgResult<Oid>
);
seam!(
    /// `ExecReindex(pstate, stmt, isTopLevel)` (indexcmds.c) — REINDEX.
    pub fn exec_reindex<'mcx>(mcx: Mcx<'mcx>, pstate: &mut ParseState<'mcx>, stmt: &Node<'mcx>, is_top_level: bool) -> PgResult<()>
);
seam!(
    /// `DefineView(stmt, queryString, stmt_location, stmt_len)` (view.c) — CREATE
    /// VIEW. Returns the new view's `ObjectAddress`.
    pub fn define_view<'mcx>(
        mcx: Mcx<'mcx>,
        stmt: &Node<'mcx>,
        query_string: &str,
        stmt_location: i32,
        stmt_len: i32,
    ) -> PgResult<ObjectAddress>
);
seam!(
    /// `CreateFunction(pstate, stmt)` (functioncmds.c) — CREATE FUNCTION/PROCEDURE.
    pub fn create_function<'mcx>(mcx: Mcx<'mcx>, pstate: &mut ParseState<'mcx>, stmt: &Node<'mcx>) -> PgResult<ObjectAddress>
);
seam!(
    /// `CreateCast(stmt)` (functioncmds.c) — CREATE CAST.
    pub fn create_cast<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>) -> PgResult<ObjectAddress>
);
seam!(
    /// `AlterFunction(pstate, stmt)` (functioncmds.c) — ALTER FUNCTION.
    pub fn alter_function<'mcx>(mcx: Mcx<'mcx>, pstate: &mut ParseState<'mcx>, stmt: &Node<'mcx>) -> PgResult<ObjectAddress>
);
seam!(
    /// `CreateAccessMethod(stmt)` (amcmds.c) — CREATE ACCESS METHOD
    /// (utility.c:1841). Returns the new access method's `ObjectAddress`.
    pub fn create_access_method<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>) -> PgResult<ObjectAddress>
);
seam!(
    /// `CreatePublication(pstate, stmt)` (publicationcmds.c) — CREATE PUBLICATION
    /// (utility.c:1845). Returns the new publication's `ObjectAddress`.
    pub fn create_publication<'mcx>(mcx: Mcx<'mcx>, pstate: &mut ParseState<'mcx>, stmt: &Node<'mcx>) -> PgResult<ObjectAddress>
);
seam!(
    /// `CreateConversionCommand(stmt)` (conversioncmds.c) — CREATE CONVERSION
    /// (utility.c:1718). Returns the new conversion's `ObjectAddress`.
    pub fn create_conversion_command<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>) -> PgResult<ObjectAddress>
);
seam!(
    /// `AlterPublication(pstate, stmt)` (publicationcmds.c) — ALTER PUBLICATION
    /// (utility.c:1849). `AlterPublication` calls
    /// `EventTriggerCollectSimpleCommand` directly, so the dispatcher sets
    /// `commandCollected = true` for this arm.
    pub fn alter_publication<'mcx>(mcx: Mcx<'mcx>, pstate: &mut ParseState<'mcx>, stmt: &Node<'mcx>) -> PgResult<()>
);
seam!(
    /// `DefineRule(stmt, queryString)` (rewriteDefine.c) — CREATE RULE.
    pub fn define_rule<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>, query_string: &str) -> PgResult<ObjectAddress>
);
seam!(
    /// `DefineSequence(pstate, stmt)` (sequence.c) — CREATE SEQUENCE. Returns the
    /// new sequence's `ObjectAddress`.
    pub fn define_sequence<'mcx>(mcx: Mcx<'mcx>, pstate: &mut ParseState<'mcx>, stmt: &Node<'mcx>) -> PgResult<ObjectAddress>
);
seam!(
    /// `AlterSequence(pstate, stmt)` (sequence.c) — ALTER SEQUENCE.
    pub fn alter_sequence<'mcx>(mcx: Mcx<'mcx>, pstate: &mut ParseState<'mcx>, stmt: &Node<'mcx>) -> PgResult<ObjectAddress>
);
seam!(
    /// `CreateTrigger(stmt, queryString, InvalidOid×6, NULL, false, false)`
    /// (trigger.c) — CREATE TRIGGER. Returns the new trigger's `ObjectAddress`.
    pub fn create_trigger<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>, query_string: &str) -> PgResult<ObjectAddress>
);
seam!(
    /// `CreatePolicy(stmt)` (policy.c) — CREATE POLICY.
    pub fn create_policy<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>) -> PgResult<ObjectAddress>
);
seam!(
    /// `AlterPolicy(stmt)` (policy.c) — ALTER POLICY.
    pub fn alter_policy<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>) -> PgResult<ObjectAddress>
);
seam!(
    /// `DefineOpClass(stmt)` (opclasscmds.c) — CREATE OPERATOR CLASS.
    pub fn define_op_class<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>) -> PgResult<ObjectAddress>
);
seam!(
    /// `DefineOpFamily(stmt)` (opclasscmds.c) — CREATE OPERATOR FAMILY.
    pub fn define_op_family<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>) -> PgResult<ObjectAddress>
);
seam!(
    /// `AlterOpFamily(stmt)` (opclasscmds.c) — ALTER OPERATOR FAMILY ADD/DROP.
    /// Returns the opfamily OID; the dispatch builds the `ObjectAddress`.
    pub fn alter_op_family<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>) -> PgResult<Oid>
);
seam!(
    /// `CreateStatistics(stmt, true)` (statscmds.c) — CREATE STATISTICS.
    pub fn create_statistics<'mcx>(mcx: Mcx<'mcx>, stmt: NodePtr<'mcx>) -> PgResult<ObjectAddress>
);
seam!(
    /// `CreateStatistics(stmt, false)` (statscmds.c) — the `ATExecAddStatistics`
    /// rebuild leg (`AT_ReAddStatistics`, tablecmds.c:9683). `is_rebuild` is
    /// always true on this path, so `check_rights = !is_rebuild = false`.
    pub fn create_statistics_rebuild<'mcx>(mcx: Mcx<'mcx>, stmt: NodePtr<'mcx>) -> PgResult<ObjectAddress>
);
seam!(
    /// `RangeVarGetRelid(rel, ShareUpdateExclusiveLock, false)` (namespace.c) — the
    /// CREATE STATISTICS relation-OID lookup.
    pub fn range_var_get_relid_share_update<'mcx>(mcx: Mcx<'mcx>, rel: NodePtr<'mcx>) -> PgResult<Oid>
);
seam!(
    /// `CommentObject(stmt)` (comment.c) — COMMENT (the slow-path / event-trigger leg).
    pub fn comment_object_slow<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>) -> PgResult<ObjectAddress>
);
seam!(
    /// `ExecSecLabelStmt(stmt)` (seclabel.c) — SECURITY LABEL (the slow-path leg).
    pub fn exec_sec_label_stmt_slow<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>) -> PgResult<ObjectAddress>
);
seam!(
    /// `ExecuteGrantStmt(stmt)` (aclchk.c) — GRANT/REVOKE (the slow-path leg).
    pub fn execute_grant_stmt_slow<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>) -> PgResult<()>
);
seam!(
    /// `ExecRenameStmt(stmt)` (alter.c) — RENAME (the slow-path leg). Returns the address.
    pub fn exec_rename_stmt_slow<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>) -> PgResult<ObjectAddress>
);
seam!(
    /// `ExecAlterObjectDependsStmt(stmt, &secondaryObject)` (alter.c) — ALTER …
    /// DEPENDS ON EXTENSION (the slow-path leg).
    pub fn exec_alter_object_depends_stmt_slow<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>) -> PgResult<ObjectAddress>
);
seam!(
    /// `ExecAlterObjectSchemaStmt(stmt, &secondaryObject)` (alter.c) — ALTER … SET
    /// SCHEMA (the slow-path leg).
    pub fn exec_alter_object_schema_stmt_slow<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>) -> PgResult<ObjectAddress>
);
seam!(
    /// `ExecAlterOwnerStmt(stmt)` (alter.c) — ALTER … OWNER TO (the slow-path leg).
    pub fn exec_alter_owner_stmt_slow<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>) -> PgResult<ObjectAddress>
);
seam!(
    /// `AlterOperator(stmt)` (operatorcmds.c) — ALTER OPERATOR.
    pub fn alter_operator<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>) -> PgResult<ObjectAddress>
);
seam!(
    /// `AlterType(stmt)` (typecmds.c) — ALTER TYPE … SET.
    pub fn alter_type<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>) -> PgResult<ObjectAddress>
);
seam!(
    /// `AlterCollation(stmt)` (collationcmds.c) — ALTER COLLATION … REFRESH VERSION.
    pub fn alter_collation<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>) -> PgResult<ObjectAddress>
);
seam!(
    /// `AlterStatistics(stmt)` (statscmds.c) — ALTER STATISTICS.
    pub fn alter_statistics<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>) -> PgResult<ObjectAddress>
);
seam!(
    /// `ExecCreateTableAs(pstate, stmt, params, queryEnv, qc)` (createas.c) —
    /// CREATE TABLE AS / SELECT INTO / CREATE MATERIALIZED VIEW.
    pub fn exec_create_table_as<'mcx>(
        mcx: Mcx<'mcx>,
        pstate: &mut ParseState<'mcx>,
        stmt: &Node<'mcx>,
        params: ParamListInfo,
        qc: Option<&mut QueryCompletion>,
    ) -> PgResult<ObjectAddress>
);
seam!(
    /// `ExecRefreshMatView(stmt, queryString, qc)` (matview.c) — REFRESH
    /// MATERIALIZED VIEW.
    pub fn exec_refresh_mat_view<'mcx>(
        mcx: Mcx<'mcx>,
        stmt: &Node<'mcx>,
        query_string: &str,
        qc: Option<&mut QueryCompletion>,
    ) -> PgResult<ObjectAddress>
);
seam!(
    /// `ExecAlterDefaultPrivilegesStmt(pstate, stmt)` (aclchk.c) — ALTER DEFAULT
    /// PRIVILEGES.
    pub fn exec_alter_default_privileges_stmt<'mcx>(mcx: Mcx<'mcx>, pstate: &mut ParseState<'mcx>, stmt: &Node<'mcx>) -> PgResult<()>
);
seam!(
    /// `DropOwnedObjects(stmt)` (user.c) — DROP OWNED.
    pub fn drop_owned_objects<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>) -> PgResult<()>
);
seam!(
    /// `CreateExtension(pstate, stmt)` (extension.c) — CREATE EXTENSION.
    pub fn create_extension<'mcx>(mcx: Mcx<'mcx>, pstate: &mut ParseState<'mcx>, stmt: &Node<'mcx>) -> PgResult<ObjectAddress>
);
seam!(
    /// `CreateProceduralLanguage(stmt)` (proclang.c) — CREATE LANGUAGE.
    pub fn create_procedural_language<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>) -> PgResult<ObjectAddress>
);
seam!(
    /// `CreateTransform(stmt)` (functioncmds.c) — CREATE TRANSFORM.
    pub fn create_transform<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>) -> PgResult<ObjectAddress>
);
seam!(
    /// `AlterTSDictionary(stmt)` (tsearchcmds.c) — ALTER TEXT SEARCH DICTIONARY.
    pub fn alter_ts_dictionary<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>) -> PgResult<ObjectAddress>
);
seam!(
    /// `AlterTSConfiguration(stmt)` (tsearchcmds.c) — ALTER TEXT SEARCH
    /// CONFIGURATION. Commands are stashed in MakeConfigurationMapping /
    /// DropConfigurationMapping; the dispatcher sets `commandCollected = true`.
    pub fn alter_ts_configuration<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>) -> PgResult<ObjectAddress>
);

seam!(
    /// The extension / FDW / AM / publication / subscription / transform / cast /
    /// conversion / language / op-class / op-family DDL handlers (utility.c:
    /// 1395-1581) — `CreateExtension` / `ExecAlterExtension*` / `Create/AlterFdw` /
    /// `Create/AlterForeignServer` / `Create/Alter/DropUserMapping` /
    /// `ImportForeignSchema` / `CreateProceduralLanguage` / `CreateConversionCommand`
    /// / `CreateCast` / `Define/AlterOpClass` / `Define/AlterOpFamily` /
    /// `CreateTransform` / `CreateAccessMethod` / `Create/Alter/DropSubscription` /
    /// `Create/AlterPublication`. These owners are not yet ported; the slow path
    /// routes the remaining arms here so the unported-handler set is one documented
    /// seam-panic rather than many. The `Node` carries the discriminant.
    pub fn process_utility_slow_unported<'mcx>(
        mcx: Mcx<'mcx>,
        pstate: &mut ParseState<'mcx>,
        stmt: &Node<'mcx>,
        is_top_level: bool,
    ) -> PgResult<ObjectAddress>
);
seam!(
    /// `CreateSubscription(pstate, stmt, isTopLevel)` (subscriptioncmds.c) —
    /// CREATE SUBSCRIPTION (utility.c:1853). Returns the new subscription's
    /// `ObjectAddress`.
    pub fn create_subscription<'mcx>(
        mcx: Mcx<'mcx>,
        pstate: &mut ParseState<'mcx>,
        stmt: &Node<'mcx>,
        is_top_level: bool,
    ) -> PgResult<ObjectAddress>
);
seam!(
    /// `AlterSubscription(pstate, stmt, isTopLevel)` (subscriptioncmds.c) —
    /// ALTER SUBSCRIPTION (utility.c:1857). Returns the subscription's
    /// `ObjectAddress`.
    pub fn alter_subscription<'mcx>(
        mcx: Mcx<'mcx>,
        pstate: &mut ParseState<'mcx>,
        stmt: &Node<'mcx>,
        is_top_level: bool,
    ) -> PgResult<ObjectAddress>
);
seam!(
    /// `DropSubscription(stmt, isTopLevel)` (subscriptioncmds.c) — DROP
    /// SUBSCRIPTION (utility.c:1861). No `ObjectAddress` is returned;
    /// `DropSubscription` stashes the dropped object via
    /// `EventTriggerSQLDropAddObject`, so the dispatcher sets
    /// `commandCollected = true`.
    pub fn drop_subscription<'mcx>(
        mcx: Mcx<'mcx>,
        stmt: &Node<'mcx>,
        is_top_level: bool,
    ) -> PgResult<()>
);
