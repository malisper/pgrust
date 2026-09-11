//! Session/process context provider (`MyProcPort`, `MyProc`, `MyStartTime`,
//! `debug_query_string`, ...) and the `emit_log_hook` slot. Defaults mirror
//! the C boot state, so the logging path never panics with no provider.

use std::cell::Cell;
use std::marker::PhantomData;

use ::types_error::PgError;

fn os_pid() -> u32 {
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

/// C's `MyProcPid` as the log writers see it: the per-backend pid every
/// backend thread gets at InitProcessGlobals (the value `pg_backend_pid()`
/// returns and `pg_terminate_backend()` accepts), so `%p`, `%c` and the
/// csvlog/jsonlog `pid` columns attribute a line to its session rather than
/// to the one OS process every session shares. Before InitProcessGlobals
/// (early boot, threads outside the child model) it is 0 and the OS pid —
/// the postmaster's own MyProcPid — stands in.
pub fn current_pid() -> u32 {
    let pid = init_small::globals::MyProcPid();
    if pid > 0 {
        pid as u32
    } else {
        os_pid()
    }
}

/// The per-session facts log_line_prefix and the csvlog/jsonlog writers
/// print (C reads them straight off MyProcPort/MyProc/MyBackendType). The
/// string accessors return owned values: the provider reads thread-local
/// session state (MyProcPort) that cannot be borrowed out of the slot, and
/// a log line is never a hot path. Defaults mirror the C boot state.
pub trait BackendLogContext: Sync {
    fn has_client_port(&self) -> bool {
        false
    }

    fn application_name(&self) -> Option<String> {
        None
    }

    fn user_name(&self) -> Option<String> {
        None
    }

    fn database_name(&self) -> Option<String> {
        None
    }

    fn remote_host(&self) -> Option<String> {
        None
    }

    fn remote_port(&self) -> Option<String> {
        None
    }

    fn local_host(&self) -> Option<String> {
        None
    }

    fn backend_type(&self) -> Option<String> {
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

    fn query_string(&self) -> Option<String> {
        None
    }

    /// C's `MyStartTime` (session id `%c`/`%s` and the csvlog/jsonlog
    /// session_id column derive from it).
    fn session_start_time(&self) -> i64 {
        init_small::globals::MyStartTime()
    }

    fn ps_display(&self) -> Option<String> {
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
// NULL` on error recovery); current_query() reads it.
//
// Here the value is a raw (ptr, len) pair, but soundness is made structural
// by the RAII guard rather than left to a comment-level contract:
//
//   * `DebugQueryStringScope<'a>` borrows the query for its whole life
//     (`PhantomData<&'a str>`), so safe code cannot free or reallocate the
//     backing buffer while the pair is armed — dropping the string before the
//     guard is a compile error.
//   * The armed pairs live on a per-thread stack keyed by a unique id, and a
//     guard's Drop removes *its own* entry wherever it sits in the stack. So
//     every entry still present belongs to a live guard (hence a live `&str`),
//     even if guards drop out of LIFO order. The reader observes the top
//     entry, which is therefore never dangling. This is strictly stronger than
//     C's single-slot save/restore, which cannot survive out-of-order teardown.
// ---------------------------------------------------------------------------
thread_local! {
    // Stack of currently-armed queries: (unique id, ptr, len). Depth equals the
    // number of live guards on this thread (typically 0 or 1), so the Vec stays
    // tiny and its scan/retain is effectively O(1).
    static DEBUG_QUERY_STACK: std::cell::RefCell<Vec<(u64, *const u8, usize)>> =
        const { std::cell::RefCell::new(Vec::new()) };
    static DEBUG_QUERY_NEXT_ID: Cell<u64> = const { Cell::new(0) };
    // The text of the last outermost guard to drop, for the server-log
    // STATEMENT line: an ERROR unwinds out of the exec_* frame that armed the
    // slot before the tcop catch reports it, where C's global is still set
    // (its `debug_query_string = NULL` comes after EmitErrorReport). Cleared
    // by the message loop at that point and before the next read. Retained
    // capacity: one memcpy per message, no allocation after warmup.
    static RETIRED_QUERY: std::cell::RefCell<String> = const { std::cell::RefCell::new(String::new()) };
}

/// RAII guard for the `debug_query_string` TLS slot (C: the `debug_query_string`
/// global). The `'a` lifetime ties the guard to the borrowed query for the
/// guard's entire life: the backing `&'a str` provably outlives the guard, so
/// safe code cannot free (or reallocate) the buffer while the raw (ptr, len)
/// pair is still armed. Dropping the string before the guard is a compile error
/// — see the `compile_fail` doctest on [`debug_query_string_scope`].
pub struct DebugQueryStringScope<'a> {
    // Unique id of this guard's entry on the per-thread stack; Drop removes it.
    id: u64,
    // Borrow the query for the whole life of the guard: makes it impossible for
    // safe code to drop the backing storage while the TLS pointer is armed.
    _borrow: PhantomData<&'a str>,
}

/// Arm the `debug_query_string` TLS slot with `query` for the life of the
/// returned guard (C: `debug_query_string = query;`). The guard borrows `query`
/// for its whole life, so the backing storage cannot be freed while the slot is
/// armed.
///
/// Dropping the backing string before the guard fails to compile:
///
/// ```compile_fail
/// let s = String::from("SELECT 1");
/// let guard = elog::debug_query_string_scope(&s);
/// drop(s); // error[E0505]: cannot move out of `s` because it is borrowed
/// drop(guard);
/// ```
pub fn debug_query_string_scope(query: &str) -> DebugQueryStringScope<'_> {
    let id = DEBUG_QUERY_NEXT_ID.with(|c| {
        let id = c.get();
        c.set(id.wrapping_add(1));
        id
    });
    let outermost = DEBUG_QUERY_STACK.with(|s| {
        let mut st = s.borrow_mut();
        st.push((id, query.as_ptr(), query.len()));
        st.len() == 1
    });
    if outermost {
        clear_retired_debug_query_string();
    }
    DebugQueryStringScope {
        id,
        _borrow: PhantomData,
    }
}

impl Drop for DebugQueryStringScope<'_> {
    fn drop(&mut self) {
        // Remove *this* guard's entry wherever it sits — the common LIFO case
        // pops the tail; an out-of-order drop removes from the middle. Either
        // way every remaining entry still belongs to a live guard, so the stack
        // can never retain a pointer into freed storage.
        let mine = DEBUG_QUERY_STACK.with(|s| {
            let mut st = s.borrow_mut();
            let mine = st.iter().find(|&&(id, _, _)| id == self.id).map(|&(_, p, len)| (p, len));
            st.retain(|&(id, _, _)| id != self.id);
            if st.is_empty() { mine } else { None }
        });
        if let Some((p, len)) = mine {
            // SAFETY: this guard is still alive, so its borrowed `&str` is.
            let text = unsafe {
                core::str::from_utf8_unchecked(core::slice::from_raw_parts(p, len))
            };
            RETIRED_QUERY.with(|r| {
                let mut r = r.borrow_mut();
                r.clear();
                r.push_str(text);
            });
        }
    }
}

/// C's `debug_query_string = NULL` at the points where no statement is in
/// flight (the tcop catch after EmitErrorReport, and before each command
/// read): forget the text the last guard retired for the log writers.
pub fn clear_retired_debug_query_string() {
    RETIRED_QUERY.with(|r| r.borrow_mut().clear());
}

pub(crate) fn retired_debug_query_string() -> Option<String> {
    RETIRED_QUERY.with(|r| {
        let r = r.borrow();
        if r.is_empty() { None } else { Some(r.clone()) }
    })
}

// current_query()'s read: the borrowed text is handed to `f` so the raw
// parts never escape this module.
pub fn with_debug_query_string<R>(f: impl FnOnce(Option<&str>) -> R) -> R {
    let armed = DEBUG_QUERY_STACK.with(|s| s.borrow().last().map(|&(_, p, len)| (p, len)));
    match armed {
        // SAFETY: the top entry belongs to a live guard (Drop removes an entry
        // the instant its guard dies), and that guard borrows its `&str` for
        // its whole life, so the buffer is live and the bytes are valid utf8.
        Some((p, len)) => f(Some(unsafe {
            core::str::from_utf8_unchecked(core::slice::from_raw_parts(p, len))
        })),
        None => f(None),
    }
}

#[cfg(test)]
mod debug_query_string_tests {
    use super::*;

    #[test]
    fn arms_and_restores_lifo() {
        with_debug_query_string(|q| assert_eq!(q, None));
        let outer = String::from("SELECT outer");
        {
            let _g = debug_query_string_scope(&outer);
            with_debug_query_string(|q| assert_eq!(q, Some("SELECT outer")));
            let inner = String::from("SELECT inner");
            {
                let _g2 = debug_query_string_scope(&inner);
                with_debug_query_string(|q| assert_eq!(q, Some("SELECT inner")));
            }
            // inner guard dropped: back to outer.
            with_debug_query_string(|q| assert_eq!(q, Some("SELECT outer")));
        }
        with_debug_query_string(|q| assert_eq!(q, None));
    }

    #[test]
    fn out_of_lifo_drop_does_not_clobber_live_pointer() {
        // Both strings outlive both guards (required by the borrow checker /
        // the guard's lifetime). Drop the *inner-armed* guard first: it must
        // not clobber the still-armed later pointer, and the reader must never
        // observe a dangling slot.
        let a = String::from("SELECT a");
        let b = String::from("SELECT b");
        let ga = debug_query_string_scope(&a);
        let gb = debug_query_string_scope(&b);
        with_debug_query_string(|q| assert_eq!(q, Some("SELECT b")));
        // Out-of-LIFO drop of the earlier-armed guard: slot still points at b,
        // so ga's drop leaves it untouched (rather than restoring its stale
        // prev over gb's live pointer).
        drop(ga);
        with_debug_query_string(|q| assert_eq!(q, Some("SELECT b")));
        drop(gb);
        with_debug_query_string(|q| assert_eq!(q, None));
    }
}
