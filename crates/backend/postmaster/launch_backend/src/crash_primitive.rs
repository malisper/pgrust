//! B1 — deterministic single-backend crash primitive (lifecycle test design
//! §7). The keystone that D-1, D-4-immediate, D-5, I-1 and I-3 all depend on:
//! a way to crash *exactly one* backend thread on demand, in a chosen crash
//! class, so the postmaster firewall can be exercised deterministically.
//!
//! Under pgrust's thread-per-backend model there is no `kill -SEGV <pid>`
//! that hits one backend without hitting the whole process, so per-backend
//! death must be induced from *inside* the backend thread body. These entry
//! points do exactly that: called on a backend thread, each unwinds a payload
//! that `run_child_task`'s `catch_unwind` seam catches and maps (via
//! [`crate::panic_payload_to_exit_status`]) to a postmaster-visible wait
//! status — a real Rust `panic!` (→ `SIGABRT`, the generic crash class) or a
//! synthetic signal death (→ `WTERMSIG(signo)`, e.g. `SIGSEGV`, the
//! `crash_signals` hard-signal net's class).
//!
//! GATING (design §7 "must be compiled out of / refused in release builds"):
//! this whole module exists only under `cfg(test)` or the opt-in
//! `test-crash-primitive` Cargo feature. A shipped release server carries
//! neither, so the crash symbols are not present in a production binary at
//! all — the strongest form of "refused in release".

/// Crash class to induce, matching the two firewall paths under test.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrashKind {
    /// A raw Rust `panic!`. `run_child_task`'s status map turns any
    /// non-`ProcExitThread`/`KilledBySignal` payload into `SIGABRT` — the
    /// generic crash class the reaper treats as a crash (D-1 / I-1).
    Panic,
    /// A synthetic signal death of the given number (e.g. `libc::SIGSEGV`),
    /// exercising the `crash_signals` hard-signal class rather than the plain
    /// Rust-panic seam. Rendered as an [`ipc::KilledBySignal`] unwind so the
    /// reaper reports `WTERMSIG(signo)`.
    Signal(i32),
}

/// Crash the CURRENT backend thread with the requested class. Never returns:
/// it unwinds the crash payload up into `run_child_task`'s `catch_unwind`.
///
/// This is the primitive a test-only SQL function (`pgrust_test_backend_panic`)
/// or an admin fault-injector would call once dispatched onto a backend
/// thread. It deliberately does NOT consult any runtime "armed" knob: because
/// the symbol only exists under the test cfg/feature, presence is the gate.
pub fn crash_current_backend(kind: CrashKind) -> ! {
    match kind {
        CrashKind::Panic => {
            // The message is diagnostic only; the payload's *type* (a plain
            // &str panic, not ProcExitThread/KilledBySignal) is what drives
            // the SIGABRT mapping.
            panic!("pgrust_test_backend_panic: deterministic test-only backend crash (SIGABRT class)")
        }
        CrashKind::Signal(signo) => ipc::exit_thread_killed(signo),
    }
}

/// Test-only SQL-surface name from the design (§7 B1): panic exactly one
/// backend thread → `SIGABRT` crash class. Thin alias over
/// [`crash_current_backend`].
pub fn pgrust_test_backend_panic() -> ! {
    crash_current_backend(CrashKind::Panic)
}

/// The `KilledBySignal(SIGSEGV)` variant the design calls for, to drive the
/// hard-signal (`crash_signals`) net rather than the Rust-panic seam.
pub fn pgrust_test_backend_kill_sigsegv() -> ! {
    crash_current_backend(CrashKind::Signal(libc::SIGSEGV))
}
