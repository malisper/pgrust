//! Session/process context provider (`MyProcPort`, `MyProc`, `MyStartTime`,
//! `debug_query_string`, ...) and the `emit_log_hook` slot. Defaults mirror
//! the C boot state, so the logging path never panics with no provider.

use std::cell::Cell;

use ::types_error::PgError;

pub(crate) fn current_pid() -> u32 {
    std::process::id()
}

pub trait BackendLogContext: Sync {
    fn has_client_port(&self) -> bool {
        false
    }

    fn application_name(&self) -> Option<&str> {
        None
    }

    fn user_name(&self) -> Option<&str> {
        None
    }

    fn database_name(&self) -> Option<&str> {
        None
    }

    fn remote_host(&self) -> Option<&str> {
        None
    }

    fn remote_port(&self) -> Option<&str> {
        None
    }

    fn local_host(&self) -> Option<&str> {
        None
    }

    fn backend_type(&self) -> Option<&str> {
        None
    }

    fn process_id(&self) -> u32 {
        current_pid()
    }

    fn lock_group_leader_pid(&self) -> Option<u32> {
        None
    }

    fn virtual_transaction_id(&self) -> Option<(i32, u32)> {
        None
    }

    fn top_transaction_id(&self) -> u32 {
        0
    }

    fn query_id(&self) -> i64 {
        0
    }

    fn query_string(&self) -> Option<&str> {
        None
    }

    fn session_start_time(&self) -> i64 {
        0
    }

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

pub type EmitLogHook = fn(&PgError, output_to_server: &mut bool);

thread_local! { static EMIT_LOG_HOOK: Cell<Option<EmitLogHook>> = const { Cell::new(None) }; }

pub fn set_emit_log_hook(hook: Option<EmitLogHook>) -> Option<EmitLogHook> {
    EMIT_LOG_HOOK.with(|slot| slot.replace(hook))
}

pub(crate) fn call_emit_log_hook(error: &PgError, output_to_server: &mut bool) {
    if let Some(hook) = EMIT_LOG_HOOK.with(Cell::get) {
        hook(error, output_to_server);
    }
}
