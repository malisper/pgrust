//! Port of `src/port/pqsignal.c` — the backend's reliable-signal `pqsignal()`
//! installer (`pqsignal_be`).
//!
//! This is the `signal()` implementation from "Advanced Programming in the
//! UNIX Environment". Rather than the C-library `signal()` — whose
//! handler-reset and restart behavior is implementation-defined — it calls
//! `sigaction(2)` with explicit `sa_flags` (`SA_RESTART`, plus `SA_NOCLDSTOP`
//! for `SIGCHLD`), wrapped in the convenient traditional interface.
//!
//! Every concrete handler is installed indirectly: `pqsignal_be` registers the
//! caller's handler in [`PQSIGNAL_HANDLERS`] and installs [`wrapper_handler`]
//! as the actual kernel disposition. The wrapper restores `errno`, guards
//! against running in a `fork(2)`ed child (a process whose `MyProcPid` no
//! longer matches its real PID — typically a child created by `system(3)`),
//! and otherwise dispatches to the registered handler. That fork guard ensures
//! such children do not modify shared memory, which is usually detrimental.
//!
//! `pqsignal_handlers` is `static volatile` in C: signal dispositions are
//! process-wide (not per-backend), and the array must be reachable from the
//! async-signal-context `wrapper_handler`. The port keeps it as a process
//! `static` of atomics — the genuinely-shared, async-signal-safe shape — not a
//! `thread_local`.
//!
//! Windows and the `-DFRONTEND` (`pqsignal_fe`) build are not targeted here;
//! the legacy libpq `pqsignal()` lives in the
//! `interfaces-libpq-legacy-pqsignal` crate, and the backend signal-mask setup
//! (`pqinitmask`) in `backend-libpq-pqsignal`.

use core::sync::atomic::{AtomicUsize, Ordering};
use signal::SigHandler;

/// `PG_NSIG` — the bound on signal numbers. C uses `NSIG` (the platform's
/// highest signal number plus one). The `StaticAssertDecl`s in the C file
/// only require that the common signals (`SIGUSR2`, `SIGHUP`, `SIGTERM`,
/// `SIGALRM`) fit; this value matches `NSIG` on the platforms we build for.
#[cfg(target_os = "linux")]
const PG_NSIG: usize = 65;
#[cfg(not(target_os = "linux"))]
const PG_NSIG: usize = 32;

// Mirror of the C `StaticAssertDecl(SIG* < PG_NSIG, ...)` accuracy checks.
const _: () = assert!((libc::SIGUSR2 as usize) < PG_NSIG, "SIGUSR2 >= PG_NSIG");
const _: () = assert!((libc::SIGHUP as usize) < PG_NSIG, "SIGHUP >= PG_NSIG");
const _: () = assert!((libc::SIGTERM as usize) < PG_NSIG, "SIGTERM >= PG_NSIG");
const _: () = assert!((libc::SIGALRM as usize) < PG_NSIG, "SIGALRM >= PG_NSIG");

/// `static volatile pqsigfunc pqsignal_handlers[PG_NSIG]` — the handler
/// registered for each signal, looked up by [`wrapper_handler`]. Each slot
/// holds a `fn(i32)` address (or 0 for "none"); stores are atomic, matching
/// the C "assumed atomic" pointer write.
static PQSIGNAL_HANDLERS: [AtomicUsize; PG_NSIG] =
    [const { AtomicUsize::new(0) }; PG_NSIG];

/// `static void wrapper_handler(SIGNAL_ARGS)` — installed as the kernel
/// disposition for every concrete handler. Restores `errno`, checks that we
/// are still the process that called `pqsignal()` (and not a `fork`ed child),
/// then dispatches to the registered handler.
extern "C" fn wrapper_handler(postgres_signal_arg: i32) {
    // SAFETY: `__errno_location`/`errno` access is async-signal-safe; we read
    // and later restore the caller's errno exactly as the C wrapper does.
    let save_errno = errno();

    debug_assert!(postgres_signal_arg > 0);
    debug_assert!((postgres_signal_arg as usize) < PG_NSIG);

    // We expect processes to set MyProcPid before calling pqsignal() or before
    // accepting signals. (C also Asserts MyProcPid != PostmasterPid ||
    // !IsUnderPostmaster; PostmasterPid is not exposed through a seam, so that
    // debug-only invariant is not reproduced here.)
    debug_assert!(init_small_seams::my_proc_pid::call() != 0);

    // SAFETY: getpid() is async-signal-safe.
    if init_small_seams::my_proc_pid::call() != unsafe { libc::getpid() } {
        // We are a forked child that should not run this handler: restore the
        // default disposition and re-raise so the default action takes over.
        pqsignal_be(postgres_signal_arg, SigHandler::Default);
        // SAFETY: raise() is async-signal-safe.
        unsafe {
            libc::raise(postgres_signal_arg);
        }
        return;
    }

    let slot = PQSIGNAL_HANDLERS[postgres_signal_arg as usize].load(Ordering::Relaxed);
    // The wrapper is installed only when a concrete handler was registered, so
    // the slot is always populated when we reach here.
    debug_assert!(slot != 0);
    // SAFETY: `slot` was stored from a `fn(i32)` by `pqsignal_be` below.
    let handler: fn(i32) = unsafe { core::mem::transmute::<usize, fn(i32)>(slot) };
    handler(postgres_signal_arg);

    set_errno(save_errno);
}

/// `void pqsignal(int signo, pqsigfunc func)` — actual symbol `pqsignal_be` —
/// set up a signal handler for `signo`, with `SA_RESTART` (plus
/// `SA_NOCLDSTOP` for `SIGCHLD`).
///
/// For a concrete handler the function registers it in [`PQSIGNAL_HANDLERS`]
/// and installs [`wrapper_handler`] as the kernel disposition; `SIG_DFL` and
/// `SIG_IGN` are installed directly. A failing `sigaction(2)` indicates a
/// coding error (`Assert(false)` in C); the port mirrors that with a
/// `debug_assert!`.
pub fn pqsignal_be(signo: i32, func: SigHandler) {
    debug_assert!(signo > 0);
    debug_assert!((signo as usize) < PG_NSIG);

    // For a concrete handler, register it and install the wrapper instead.
    // SIG_IGN/SIG_DFL go straight to the kernel.
    let disposition: libc::sighandler_t = match func {
        SigHandler::Default => libc::SIG_DFL,
        SigHandler::Ignore => libc::SIG_IGN,
        SigHandler::Handler(f) => {
            PQSIGNAL_HANDLERS[signo as usize].store(f as usize, Ordering::Relaxed);
            wrapper_handler as *const () as usize as libc::sighandler_t
        }
    };

    // SAFETY: installing a process signal disposition. Mirrors the C body:
    // empty mask, `SA_RESTART`, plus `SA_NOCLDSTOP` for SIGCHLD.
    unsafe {
        let mut act: libc::sigaction = core::mem::zeroed();
        act.sa_sigaction = disposition;
        libc::sigemptyset(&mut act.sa_mask);
        act.sa_flags = libc::SA_RESTART;
        if signo == libc::SIGCHLD {
            act.sa_flags |= libc::SA_NOCLDSTOP;
        }
        if libc::sigaction(signo, &act, core::ptr::null_mut()) < 0 {
            // C: Assert(false) — "probably indicates coding error".
            debug_assert!(false, "sigaction failed in pqsignal_be");
        }
    }
}

/// Read the current `errno`.
#[inline]
fn errno() -> i32 {
    // SAFETY: the platform errno location is always valid for the calling
    // thread.
    unsafe { *errno_location() }
}

/// Restore `errno`.
#[inline]
fn set_errno(value: i32) {
    // SAFETY: as above.
    unsafe {
        *errno_location() = value;
    }
}

#[cfg(target_os = "linux")]
unsafe fn errno_location() -> *mut i32 {
    libc::__errno_location()
}

#[cfg(not(target_os = "linux"))]
unsafe fn errno_location() -> *mut i32 {
    libc::__error()
}

/// Install this crate's seams: wire `port-pqsignal-seams::pqsignal` to the
/// real `pqsignal_be`.
pub fn init_seams() {
    port_pqsignal_seams::pqsignal::set(pqsignal_be);
}

#[cfg(test)]
mod tests {
    use super::*;

    static FIRED: AtomicUsize = AtomicUsize::new(0);

    fn a_handler(_signo: i32) {
        FIRED.fetch_add(1, Ordering::SeqCst);
    }

    #[test]
    fn ignore_then_default_round_trip() {
        // SIGWINCH is benign in the test process. Installing SIG_IGN then
        // SIG_DFL must not panic and must not require the wrapper.
        pqsignal_be(libc::SIGWINCH, SigHandler::Ignore);
        pqsignal_be(libc::SIGWINCH, SigHandler::Default);
    }

    #[test]
    fn concrete_handler_dispatches_through_wrapper() {
        // Install a concrete handler on SIGURG (benign, unused), raise the
        // signal, and confirm the wrapper dispatched to it. MyProcPid must be
        // set for the wrapper's fork guard; install the seam locally.
        if !init_small_seams::my_proc_pid::is_installed() {
            init_small_seams::my_proc_pid::set(|| unsafe { libc::getpid() });
        }
        FIRED.store(0, Ordering::SeqCst);
        pqsignal_be(libc::SIGURG, SigHandler::Handler(a_handler));
        // SAFETY: deliver SIGURG to ourselves.
        unsafe {
            libc::raise(libc::SIGURG);
        }
        assert_eq!(FIRED.load(Ordering::SeqCst), 1);
        pqsignal_be(libc::SIGURG, SigHandler::Default);
    }
}
