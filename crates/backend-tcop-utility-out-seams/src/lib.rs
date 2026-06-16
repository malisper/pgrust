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
use types_nodes::nodes::Node;
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
    /// `CallStmtResultDesc(stmt)` (functioncmds.c).
    pub fn call_stmt_result_desc<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>) -> TupleDesc<'mcx>
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
    /// `GetPGVariableResultDesc(name)` (guc.c) — SHOW result descriptor.
    pub fn get_pg_variable_result_desc<'mcx>(mcx: Mcx<'mcx>, name: Option<&str>) -> TupleDesc<'mcx>
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
    pub fn execute_stmt_raw_parse_tree<'mcx>(stmt: &Node<'mcx>) -> Option<types_nodes::nodes::NodePtr<'mcx>>
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

use types_nodes::parsestmt::{ParseState, ProcessUtilityContext};
use types_nodes::portalcmds::ParamListInfo;
use types_nodes::parsestmt::DestReceiverHandle;
use types_nodes::parsenodes::ObjectType;
use types_portal::QueryCompletion;
use types_error::PgResult;
use types_core::primitive::Oid;
use types_core::init::BackendType;

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
        pstmt: &types_nodes::nodeindexscan::PlannedStmt<'mcx>,
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
    pub fn event_trigger_alter_table_end()
);
seam!(
    /// `EventTriggerAlterTableStart(parsetree)` (event_trigger.c) — open a new
    /// complex-command set after the subcommand returns
    /// (`ProcessUtilityForAlterTable`, utility.c:1607).
    pub fn event_trigger_alter_table_start<'mcx>(parsetree: &Node<'mcx>)
);
seam!(
    /// `EventTriggerAlterTableRelid(relid)` (event_trigger.c) — record the
    /// relation being altered (`ProcessUtilityForAlterTable`, utility.c:1608).
    pub fn event_trigger_alter_table_relid(relid: Oid)
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
    pub fn drop_role<'mcx>(stmt: &Node<'mcx>) -> PgResult<()>
);
seam!(
    /// `ReassignOwnedObjects(stmt)` (user.c) — REASSIGN OWNED.
    pub fn reassign_owned_objects<'mcx>(stmt: &Node<'mcx>) -> PgResult<()>
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
