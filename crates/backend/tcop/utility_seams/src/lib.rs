//! Seam declarations for the `backend-tcop-utility` unit (`tcop/utility.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_error::PgResult;
use ::nodes::nodeindexscan::PlannedStmt;
use ::nodes::nodes::Node;
use ::nodes::parsestmt::CommandTag;

seam_core::seam!(
    /// `CreateCommandTag(parsetree)` (utility.c) — the `CommandTag` for a raw
    /// parse-tree node (the PREPARE'd query). Pure classification, but reads
    /// the node tree; cannot `ereport` for well-formed nodes.
    pub fn create_command_tag<'mcx>(query: &Node<'mcx>) -> PgResult<CommandTag>
);

seam_core::seam!(
    /// `CommandIsReadOnly(pstmt)` (utility.c:94) — is the planned statement
    /// *in truth* read-only? Stricter than `XactReadOnly`; the SPI cursor open
    /// read-only check (`SPI_cursor_open_internal`) uses it. Reads the node;
    /// `ereport`s only the internal `unrecognized commandType` WARNING path.
    pub fn command_is_read_only<'mcx>(pstmt: &PlannedStmt<'mcx>) -> PgResult<bool>
);

seam_core::seam!(
    /// `CreateCommandTag((Node *) pstmt)` (utility.c, `case T_PlannedStmt:`) —
    /// the `CommandTag` for a planned statement, used by
    /// `CreateCommandName((Node *) pstmt)` in the SPI "cannot open %s query as
    /// cursor" / "%s is not allowed in a non-volatile function" messages.
    pub fn planned_stmt_command_tag<'mcx>(pstmt: &PlannedStmt<'mcx>) -> PgResult<CommandTag>
);

seam_core::seam!(
    /// `GetCommandLogLevel(parsetree)` (utility.c:3249) — the `LogStmtLevel`
    /// (carried as `i32`, `tcop/tcopprot.h`) for a raw parse-tree node, used by
    /// `check_log_statement` to decide whether `log_statement` covers it. Pure
    /// classification; can `ereport` only on a malformed node.
    pub fn get_command_log_level<'mcx>(parsetree: &Node<'mcx>) -> PgResult<i32>
);

seam_core::seam!(
    /// `ProcessUtility(pstmt, queryString, readOnlyTree, context, params,
    /// queryEnv, dest, qc)` (utility.c) — execute a utility (non-optimizable)
    /// statement. `pquery.c`'s `PortalRunUtility` drives it for the portal's
    /// `PlannedStmt`. The owned model drops the C `queryEnv` argument (as
    /// `QueryDesc::create` does). The receiver is the router-keyed
    /// [`DestReceiverHandle`]; `qc` is filled in place. Can `ereport(ERROR)`.
    ///
    /// `mcx` is the per-utility working context (C: `CurrentMemoryContext`
    /// during the portal's utility run — the per-message context, reset after
    /// the command). `standard_ProcessUtility` allocates the `readOnlyTree`
    /// `copyObject(pstmt)` deep-copy and the `make_parsestate(NULL)` parse state
    /// in it; the caller (pquery `PortalRunUtility`) owns the context and drops
    /// it when this returns — the owned analogue of C's per-message context
    /// reset. Nothing the dispatch returns escapes `mcx` (`qc` is owned, `dest`
    /// is a handle), so a per-call scratch context is sound.
    pub fn process_utility<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        pstmt: &::nodes::nodeindexscan::PlannedStmt<'mcx>,
        query_string: &str,
        read_only_tree: bool,
        context: ::nodes::parsestmt::ProcessUtilityContext,
        params: ::nodes::portalcmds::ParamListInfo,
        dest: ::nodes::parsestmt::DestReceiverHandle,
        qc: &mut portal::QueryCompletion,
    ) -> PgResult<()>
);

// ===========================================================================
// `ProcessUtility_hook` (utility.c): the loadable-module interposition point for
// utility-command execution. In C `ProcessUtility()` calls `ProcessUtility_hook
// ? ProcessUtility_hook(...) : standard_ProcessUtility(...)`. Modeled like
// `shmem_request_hook` (miscinit.c): a per-backend thread-local
// `Cell<Option<fn>>` with `set_process_utility_hook` /
// `process_utility_hook_present` / `call_process_utility_hook`. The slot lives in
// this `-seams` crate so a hook-installing module (e.g. pg_stat_statements) can
// register without a dependency cycle; the hook wraps + calls the owner's public
// `standard_ProcessUtility`. With no hook set, `process_utility_hook_present()`
// is false and the owner runs `standard_ProcessUtility` directly —
// byte-identical to today.
// ===========================================================================

/// `ProcessUtility_hook_type` (utility.h): `void (*)(PlannedStmt *pstmt, const
/// char *queryString, bool readOnlyTree, ProcessUtilityContext context,
/// ParamListInfo params, QueryEnvironment *queryEnv, DestReceiver *dest,
/// QueryCompletion *qc)`. The owned model drops the `queryEnv` argument (as the
/// `process_utility` seam does). Higher-ranked over the per-utility context
/// lifetime so one registered hook handles any utility statement.
#[allow(clippy::type_complexity)]
pub type ProcessUtilityHook = for<'mcx> fn(
    mcx: mcx::Mcx<'mcx>,
    pstmt: &::nodes::nodeindexscan::PlannedStmt<'mcx>,
    query_string: &str,
    read_only_tree: bool,
    context: ::nodes::parsestmt::ProcessUtilityContext,
    params: ::nodes::portalcmds::ParamListInfo,
    dest: ::nodes::parsestmt::DestReceiverHandle,
    qc: &mut portal::QueryCompletion,
) -> PgResult<()>;

thread_local! {
    /// `ProcessUtility_hook_type ProcessUtility_hook = NULL;` (utility.c).
    static PROCESS_UTILITY_HOOK: std::cell::Cell<Option<ProcessUtilityHook>> =
        const { std::cell::Cell::new(None) };
}

/// `ProcessUtility_hook != NULL` — whether a module registered a
/// `ProcessUtility` hook.
pub fn process_utility_hook_present() -> bool {
    PROCESS_UTILITY_HOOK.with(|c| c.get().is_some())
}
/// Register a module's `ProcessUtility_hook` (the `ProcessUtility_hook = my_hook`
/// assignment in `_PG_init`). The hook wraps + calls the owner's public
/// `standard_ProcessUtility`.
pub fn set_process_utility_hook(
    hook: Option<ProcessUtilityHook>,
) -> Option<ProcessUtilityHook> {
    PROCESS_UTILITY_HOOK.with(|c| c.replace(hook))
}
/// Invoke the registered `ProcessUtility_hook(...)`. Panics if none is
/// registered (the call site guards with [`process_utility_hook_present`],
/// mirroring C's `if (ProcessUtility_hook)`).
#[allow(clippy::too_many_arguments)]
pub fn call_process_utility_hook<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    pstmt: &::nodes::nodeindexscan::PlannedStmt<'mcx>,
    query_string: &str,
    read_only_tree: bool,
    context: ::nodes::parsestmt::ProcessUtilityContext,
    params: ::nodes::portalcmds::ParamListInfo,
    dest: ::nodes::parsestmt::DestReceiverHandle,
    qc: &mut portal::QueryCompletion,
) -> PgResult<()> {
    match PROCESS_UTILITY_HOOK.with(std::cell::Cell::get) {
        Some(hook) => hook(
            mcx,
            pstmt,
            query_string,
            read_only_tree,
            context,
            params,
            dest,
            qc,
        ),
        None => panic!("call_process_utility_hook() called with no hook registered"),
    }
}

seam_core::seam!(
    /// `UtilityReturnsTuples(parsetree)` (utility.c) — does running this utility
    /// statement produce a result set? `ChoosePortalStrategy`'s `CMD_UTILITY`
    /// leg uses it to pick `PORTAL_UTIL_SELECT`. Pure classification over the
    /// utility node; cannot `ereport` for well-formed nodes.
    pub fn utility_returns_tuples<'mcx>(stmt: &Node<'mcx>) -> PgResult<bool>
);

seam_core::seam!(
    /// `UtilityTupleDescriptor(parsetree)` (utility.c) — the result tuple
    /// descriptor a tuple-returning utility statement produces (`None` when it
    /// returns no tuples). `PortalStart`'s `PORTAL_UTIL_SELECT` leg uses it.
    /// Allocates the descriptor in `mcx`; fallible.
    pub fn utility_tuple_descriptor<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        stmt: &Node<'mcx>,
    ) -> PgResult<types_tuple::heaptuple::TupleDesc<'mcx>>
);

seam_core::seam!(
    /// `PreventCommandDuringRecovery(cmdname)` (utility.c) — `ereport(ERROR,
    /// ERRCODE_READ_ONLY_SQL_TRANSACTION)` "cannot execute <cmdname> during
    /// recovery" when `RecoveryInProgress()`. `pg_notify` calls it with
    /// `"NOTIFY"`. Errors out on `Err`.
    pub fn prevent_command_during_recovery(cmdname: &str) -> types_error::PgResult<()>
);
