//! Session/process context provider and the `emit_log_hook` slot.
//!
//! elog.c reads a pile of per-process globals when formatting log lines
//! (`MyProcPort`, `MyProc`, `MyProcPid`, `MyStartTime`, `MyBackendType`,
//! `application_name`, `debug_query_string`, `GetTopTransactionIdIfAny()`,
//! `pgstat_get_my_query_id()`, `get_ps_display()`). In this port those live
//! behind the [`BackendLogContext`] trait: the owning units install a provider
//! via [`set_backend_log_context`] when they land. Those globals are
//! per-backend state, so the provider slot is `thread_local!` (AGENTS.md
//! "Backend-global state") and installation is part of per-backend init. With
//! no provider installed every default mirrors the C boot state (no client
//! port, no PGPROC, pid = `getpid()`), so the logging path never panics.

use std::cell::Cell;

use ::types_error::PgError;

pub trait BackendLogContext: Sync {
    /// `MyProcPort != NULL`.
    fn has_client_port(&self) -> bool {
        false
    }

    /// `application_name` GUC (only meaningful with a client port).
    fn application_name(&self) -> Option<&str> {
        None
    }

    /// `MyProcPort->user_name`.
    fn user_name(&self) -> Option<&str> {
        None
    }

    /// `MyProcPort->database_name`.
    fn database_name(&self) -> Option<&str> {
        None
    }

    /// `MyProcPort->remote_host`.
    fn remote_host(&self) -> Option<&str> {
        None
    }

    /// `MyProcPort->remote_port`.
    fn remote_port(&self) -> Option<&str> {
        None
    }

    /// `MyProcPort->laddr` rendered numerically (`%L`); the C caches the
    /// `pg_getnameinfo_all` lookup in the Port, so the provider owns caching.
    fn local_host(&self) -> Option<&str> {
        None
    }

    /// `get_backend_type_for_log()`'s source data: "postmaster" when
    /// `MyProcPid == PostmasterPid`, the bgworker's `bgw_type` for
    /// `B_BG_WORKER`, else `GetBackendTypeDesc(MyBackendType)`.
    fn backend_type(&self) -> Option<&str> {
        None
    }

    /// `MyProcPid`.
    fn process_id(&self) -> u32 {
        std::process::id()
    }

    /// `MyProc->lockGroupLeader->pid` (`None` when `MyProc` is NULL or there
    /// is no lock group leader).
    fn lock_group_leader_pid(&self) -> Option<u32> {
        None
    }

    /// `MyProc->vxid` as `(procNumber, lxid)`; `None` when `MyProc` is NULL
    /// or `procNumber == INVALID_PROC_NUMBER`.
    fn virtual_transaction_id(&self) -> Option<(i32, u32)> {
        None
    }

    /// `GetTopTransactionIdIfAny()` (0 = InvalidTransactionId outside a xact).
    fn top_transaction_id(&self) -> u32 {
        0
    }

    /// `pgstat_get_my_query_id()` (0 when none).
    fn query_id(&self) -> i64 {
        0
    }

    /// `debug_query_string` (tcop's currently-executing statement).
    fn query_string(&self) -> Option<&str> {
        None
    }

    /// `MyStartTime` (seconds since the Unix epoch).
    fn session_start_time(&self) -> i64 {
        0
    }

    /// `get_ps_display()`.
    fn ps_display(&self) -> Option<&str> {
        None
    }
}

thread_local! {
    static BACKEND_LOG_CONTEXT: Cell<Option<&'static dyn BackendLogContext>> =
        const { Cell::new(None) };
}

pub fn set_backend_log_context(
    context: Option<&'static dyn BackendLogContext>,
) -> Option<&'static dyn BackendLogContext> {
    BACKEND_LOG_CONTEXT.with(|slot| slot.replace(context))
}

pub fn backend_log_context() -> Option<&'static dyn BackendLogContext> {
    BACKEND_LOG_CONTEXT.with(Cell::get)
}

/// `emit_log_hook` (elog.c): called before a report is sent to the server
/// log. The hook may turn OFF `output_to_server` (and only off — see the C
/// comment in `EmitErrorReport`); any other edata change is unsupported, so
/// the error is passed by shared reference.
pub type EmitLogHook = fn(&PgError, output_to_server: &mut bool);

// `emit_log_hook` is a per-process C global (each backend gets its own copy
// at fork); per backend == per thread here, so the slot is thread_local and
// is installed during per-backend init.
thread_local! { static EMIT_LOG_HOOK: Cell<Option<EmitLogHook>> = const { Cell::new(None) }; }

pub fn set_emit_log_hook(hook: Option<EmitLogHook>) -> Option<EmitLogHook> {
    EMIT_LOG_HOOK.with(|slot| slot.replace(hook))
}

pub(crate) fn call_emit_log_hook(error: &PgError, output_to_server: &mut bool) {
    if let Some(hook) = EMIT_LOG_HOOK.with(Cell::get) {
        hook(error, output_to_server);
    }
}
