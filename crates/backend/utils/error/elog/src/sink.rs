//! Session/process context provider (`MyProcPort`, `MyProc`, `MyStartTime`,
//! `debug_query_string`, ...) and the `emit_log_hook` slot. Defaults mirror
//! the C boot state, so the logging path never panics with no provider.

use std::cell::Cell;

use ::types_error::PgError;

pub(crate) fn current_pid() -> u32 {
    // wasm32: std::process::id() PANICS on WASI (no pids); 1 is the synthetic
    // single-process pid (init_small::globals::process_id's convention —
    // elog sits below init_small in the crate DAG, hence the local twin).
    // pgrust_sim (p4-simnet inc-2, review observation 1): the OS pid is
    // ambient entropy reaching server-log line prefixes — the sim arm pins
    // it to the same synthetic 1, mirroring globals.rs.
    #[cfg(not(any(target_family = "wasm", pgrust_sim)))]
    {
        std::process::id()
    }
    #[cfg(any(target_family = "wasm", pgrust_sim))]
    {
        1
    }
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

// C's pq_redirect_to_shm_mq: while installed, client-bound reports go to the
// closure (structured, no wire encode) instead of the frontend socket.
pub type FrontendRedirect = Box<dyn Fn(&PgError)>;

thread_local! {
    static FRONTEND_REDIRECT: std::cell::RefCell<Option<FrontendRedirect>> =
        const { std::cell::RefCell::new(None) };
}

pub fn set_frontend_redirect(redirect: Option<FrontendRedirect>) -> Option<FrontendRedirect> {
    FRONTEND_REDIRECT.with(|slot| std::mem::replace(&mut *slot.borrow_mut(), redirect))
}

pub(crate) fn call_frontend_redirect(error: &PgError) -> bool {
    FRONTEND_REDIRECT.with(|slot| match slot.borrow().as_ref() {
        Some(redirect) => {
            redirect(error);
            true
        }
        None => false,
    })
}

// ---------------------------------------------------------------------------
// debug_query_string (C: tcop/postgres.c global). C stores a bare
// `const char *` armed by exec_simple_query / exec_parse_message /
// exec_bind_message / exec_execute_message and cleared when the statement
// frame ends (tail assignment, plus the sigsetjmp `debug_query_string =
// NULL` on error recovery); current_query() reads it. The (ptr, len) pair
// here carries the identical lifetime contract, made structural by the RAII
// scope: armed from a &str that outlives the statement frame, restored to
// the previous value when the frame drops — Err-unwind included.
// ---------------------------------------------------------------------------
thread_local! {
    static DEBUG_QUERY_STRING: Cell<Option<(*const u8, usize)>> = const { Cell::new(None) };
}

pub struct DebugQueryStringScope {
    prev: Option<(*const u8, usize)>,
}

pub fn debug_query_string_scope(query: &str) -> DebugQueryStringScope {
    let prev =
        DEBUG_QUERY_STRING.with(|c| c.replace(Some((query.as_ptr(), query.len()))));
    DebugQueryStringScope { prev }
}

impl Drop for DebugQueryStringScope {
    fn drop(&mut self) {
        DEBUG_QUERY_STRING.with(|c| c.set(self.prev));
    }
}

// current_query()'s read: the borrowed text is handed to `f` so the raw
// parts never escape this module.
pub fn with_debug_query_string<R>(f: impl FnOnce(Option<&str>) -> R) -> R {
    match DEBUG_QUERY_STRING.with(Cell::get) {
        // SAFETY: scope contract above — a Some slot points at a live str
        // (its owning frame encloses every reader's frame on this thread)
        // minted from a valid &str, so the bytes are utf8.
        Some((p, len)) => f(Some(unsafe {
            core::str::from_utf8_unchecked(core::slice::from_raw_parts(p, len))
        })),
        None => f(None),
    }
}
