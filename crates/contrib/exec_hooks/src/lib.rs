//! Multi-consumer dispatch over execmain's single-consumer executor taps.
//!
//! C chains executor hooks: each module's `_PG_init` saves the previous
//! `ExecutorStart_hook` (etc.) and calls it from inside its own hook, so any
//! number of preloaded modules stack. The `tap!` seam is deliberately
//! install-once with no chaining, which is fine for a single consumer but
//! makes two executor-hook modules (pg_stat_statements + auto_explain, a
//! completely standard C pairing) collide with a boot panic.
//!
//! This crate is the chain. Modules call [`register`] from their `_PG_init`
//! (boot window only — same window `tap::install` itself enforces); the first
//! registration installs mux dispatchers into the six executor taps, later
//! registrations append. The not-loaded zero cost is unchanged: when no
//! module registers, the taps stay empty and the executor pays only the
//! `call_if` null test.
//!
//! Dispatch order mirrors C's chain: the last-loaded module's hook is the
//! outermost wrapper there, so enter-style taps (start, run, finish, end) run
//! consumers in REVERSE registration order and leave-style taps (run_leave,
//! finish_leave — C's PG_FINALLY unwind) run them in registration order.

use std::sync::atomic::{AtomicPtr, Ordering};
use pgsync::Mutex;

use types_error::PgResult;
use types_portal::QueryDescHandle;

type Hook = fn(QueryDescHandle);
/// The ExecutorEnd_hook body may ereport(ERROR) with no PG_TRY between it
/// and the executor (auto_explain.c:394-424): the first error in dispatch
/// order — the outermost C wrapper — unwinds past the inner hooks and
/// standard_ExecutorEnd.
type EndHook = fn(QueryDescHandle) -> PgResult<()>;

/// One module's executor hook set (all optional).
#[derive(Clone, Copy, Default)]
pub struct ExecutorHooks {
    /// C `ExecutorStart_hook` (before standard_ExecutorStart).
    pub start: Option<Hook>,
    /// C `ExecutorRun_hook` entry (before standard_ExecutorRun).
    pub run: Option<Hook>,
    /// C `ExecutorRun_hook` PG_FINALLY (after standard_ExecutorRun, also on
    /// the error path).
    pub run_leave: Option<Hook>,
    /// C `ExecutorFinish_hook` entry.
    pub finish: Option<Hook>,
    /// C `ExecutorFinish_hook` PG_FINALLY.
    pub finish_leave: Option<Hook>,
    /// C `ExecutorEnd_hook` (before standard_ExecutorEnd); an `Err` aborts
    /// the statement there, exactly like the C hook's ERROR.
    pub end: Option<EndHook>,
}

pgsync::process_global! {
    static REGISTRY: Mutex<Vec<ExecutorHooks>> = Mutex::new(Vec::new());
}

// Published snapshot of the registry for lock-free dispatch. Written only
// during the single-threaded boot window; backend threads spawn afterwards,
// so an Acquire load always sees the final slice.
static PUBLISHED: AtomicPtr<Vec<ExecutorHooks>> = AtomicPtr::new(std::ptr::null_mut());

/// Register one module's executor hooks. Boot window only (a module
/// `_PG_init` under shared_preload_libraries); the first caller claims the
/// underlying taps, which re-enforces the boot-phase rule.
pub fn register(hooks: ExecutorHooks) {
    assert!(
        seam_core::tap_boot_phase_open(),
        "exec_hooks::register after boot"
    );
    let mut reg = REGISTRY.lock().unwrap();
    let first = reg.is_empty();
    reg.push(hooks);
    // Publish a fresh snapshot; the superseded one leaks (bounded by the
    // number of preloaded modules, boot-only).
    let snapshot = Box::into_raw(Box::new(reg.clone()));
    PUBLISHED.store(snapshot, Ordering::Release);
    drop(reg);

    if first {
        execmain::tap_executor_start::install(dispatch_start);
        execmain::tap_executor_run::install(dispatch_run);
        execmain::tap_executor_run_leave::install(dispatch_run_leave);
        execmain::tap_executor_finish::install(dispatch_finish);
        execmain::tap_executor_finish_leave::install(dispatch_finish_leave);
        execmain::tap_executor_end::install(dispatch_end);
    }
}

#[inline]
fn consumers() -> &'static [ExecutorHooks] {
    let p = PUBLISHED.load(Ordering::Acquire);
    if p.is_null() {
        &[]
    } else {
        // SAFETY: published snapshots are leaked and never freed; the pointer
        // always refers to a live Vec written before any dispatch can run.
        unsafe { (*p).as_slice() }
    }
}

macro_rules! dispatch_enter {
    ($name:ident, $field:ident) => {
        fn $name(h: QueryDescHandle) {
            for c in consumers().iter().rev() {
                if let Some(f) = c.$field {
                    f(h);
                }
            }
        }
    };
}

macro_rules! dispatch_leave {
    ($name:ident, $field:ident) => {
        fn $name(h: QueryDescHandle) {
            for c in consumers().iter() {
                if let Some(f) = c.$field {
                    f(h);
                }
            }
        }
    };
}

dispatch_enter!(dispatch_start, start);
dispatch_enter!(dispatch_run, run);
dispatch_leave!(dispatch_run_leave, run_leave);
dispatch_enter!(dispatch_finish, finish);
dispatch_leave!(dispatch_finish_leave, finish_leave);

// ExecutorEnd: enter order (last-registered first); the first Err returns at
// once — C's inner `prev_ExecutorEnd` / standard_ExecutorEnd never run once
// the outer hook has raised.
fn dispatch_end(h: QueryDescHandle) -> PgResult<()> {
    for c in consumers().iter().rev() {
        if let Some(f) = c.end {
            f(h)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU32;

    static ORDER: AtomicU32 = AtomicU32::new(0);
    static FIRST_START_AT: AtomicU32 = AtomicU32::new(0);
    static SECOND_START_AT: AtomicU32 = AtomicU32::new(0);
    static FIRST_LEAVE_AT: AtomicU32 = AtomicU32::new(0);
    static SECOND_LEAVE_AT: AtomicU32 = AtomicU32::new(0);
    static FIRST_END_AT: AtomicU32 = AtomicU32::new(0);
    static THIRD_END_AT: AtomicU32 = AtomicU32::new(0);

    fn stamp(slot: &AtomicU32) {
        slot.store(ORDER.fetch_add(1, Ordering::Relaxed) + 1, Ordering::Relaxed);
    }

    // C's ExecutorEnd_hook chain has no PG_TRY around a consumer: an ERROR
    // raised by a hook body (auto_explain.c:394-424 explain_ExecutorEnd
    // renders the plan with no catch) longjmps past the inner hooks and
    // standard_ExecutorEnd and aborts the statement. The end tap therefore
    // must carry an error channel — a consumer signature returning a Result
    // — where the other five taps stay infallible (their C bodies are
    // counters and PG_FINALLY bookkeeping). Observed through the tap's
    // published Signature so the assertion compiles on a tree whose end tap
    // is still `fn(QueryDescHandle)` and fails there for exactly that reason.
    #[test]
    fn end_tap_carries_an_error_channel() {
        let end = std::any::type_name::<execmain::tap_executor_end::Signature>();
        let start = std::any::type_name::<execmain::tap_executor_start::Signature>();
        assert!(
            end.contains("-> core::result::Result<(), "),
            "tap_executor_end must return PgResult<()> (C: a hook ERROR aborts the statement), got `{end}`"
        );
        assert!(
            !start.contains("->"),
            "tap_executor_start stays infallible (C: no error path in the counters), got `{start}`"
        );
    }

    // One test only: registration is process-global (like the taps it owns),
    // so the not-loaded assertion, the install-on-first-register assertion,
    // and the C chain ordering assertion must share a process sequence.
    #[test]
    fn not_loaded_is_empty_then_chain_dispatches_in_c_order() {
        // Not loaded: no consumer registered, taps stay empty — the
        // executor's call_if pays only the null test (seam_core tap!).
        assert!(consumers().is_empty());
        assert!(!execmain::tap_executor_start::is_installed());

        register(ExecutorHooks {
            start: Some(|_| stamp(&FIRST_START_AT)),
            run_leave: Some(|_| stamp(&FIRST_LEAVE_AT)),
            end: Some(|_| {
                stamp(&FIRST_END_AT);
                Ok(())
            }),
            ..Default::default()
        });
        assert!(execmain::tap_executor_start::is_installed());

        register(ExecutorHooks {
            start: Some(|_| stamp(&SECOND_START_AT)),
            run_leave: Some(|_| stamp(&SECOND_LEAVE_AT)),
            end: Some(|_| Err(Box::new(types_error::PgError::error("end hook raised")))),
            ..Default::default()
        });

        let h = QueryDescHandle::NULL;
        execmain::tap_executor_start::call_if(|f| f(h));
        execmain::tap_executor_run_leave::call_if(|f| f(h));

        // C chain semantics: the later-loaded module wraps the earlier one —
        // enter runs last-registered first, the PG_FINALLY leave runs
        // first-registered first.
        let (f_start, s_start) =
            (FIRST_START_AT.load(Ordering::Relaxed), SECOND_START_AT.load(Ordering::Relaxed));
        let (f_leave, s_leave) =
            (FIRST_LEAVE_AT.load(Ordering::Relaxed), SECOND_LEAVE_AT.load(Ordering::Relaxed));
        assert!(s_start < f_start, "enter: last-registered runs first");
        assert!(f_leave < s_leave, "leave: first-registered runs first");

        // ExecutorEnd with no PG_TRY (auto_explain.c:394-424): the third
        // (outermost) consumer runs, the second raises, and the first —
        // C's inner prev_ExecutorEnd — never runs; the error reaches the
        // executor, which skips standard_ExecutorEnd.
        register(ExecutorHooks {
            end: Some(|_| {
                stamp(&THIRD_END_AT);
                Ok(())
            }),
            ..Default::default()
        });
        let r = execmain::tap_executor_end::call_if_or(Ok(()), |f| f(h));
        let e = r.expect_err("the raising end consumer must surface from the tap");
        assert_eq!(e.message(), "end hook raised");
        assert!(THIRD_END_AT.load(Ordering::Relaxed) > 0, "outermost end hook ran");
        assert_eq!(FIRST_END_AT.load(Ordering::Relaxed), 0, "inner end hook skipped after the error");
    }
}
