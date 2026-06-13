//! Port of `src/interfaces/libpq/legacy-pqsignal.c`.
//!
//! The historic, BSD-style `signal(2)` wrapper. This version of `pqsignal()`
//! exists only because pre-9.3 releases of libpq exported `pqsignal()`, and
//! some old client programs still depend on that ABI symbol. Because it is
//! purely a backwards-compatibility shim, PostgreSQL freezes it with the
//! semantics it had in 9.2 — in particular, *different* `SIGALRM` behavior
//! than the modern `src/port/pqsignal.c`: this legacy version adds
//! `SA_RESTART` for every signal **except** `SIGALRM` (so an alarm interrupts
//! blocking syscalls). libpq itself does not use this, nor does anything else
//! in the tree.
//!
//! C's `pqsigfunc` is `void (*)(int)`, overloaded with the sentinels
//! `SIG_DFL`, `SIG_IGN`, and (as a return value only) `SIG_ERR`. The port
//! takes the installable cases as [`SigHandler`] and reports the previous
//! disposition as [`SigDisposition`] (whose `Error` variant is C's `SIG_ERR`,
//! excluded from the input by construction).

pub use types_signal::{SigDisposition, SigHandler};

/// Install this crate's seams. The unit is a leaf with no inward seam
/// declarations, so there is nothing to `set()`.
pub fn init_seams() {}

/// `pqsigfunc pqsignal(int signo, pqsigfunc func)` — install `func` as the
/// handler for `signo`, with the frozen 9.2 semantics, and return the
/// previous disposition (or [`SigDisposition::Error`], C's `SIG_ERR`, if
/// `sigaction(2)` failed).
///
/// This is the non-WIN32 path of the C function (the only path this tree
/// targets): install with an empty signal mask; `sa_flags = SA_RESTART`
/// unless the signal is `SIGALRM`, plus `SA_NOCLDSTOP` for `SIGCHLD` (the C
/// `#ifdef SA_NOCLDSTOP` is always satisfied on our platforms).
pub fn pqsignal(signo: i32, func: SigHandler) -> SigDisposition {
    let handler: libc::sighandler_t = match func {
        SigHandler::Default => libc::SIG_DFL,
        SigHandler::Ignore => libc::SIG_IGN,
        SigHandler::Handler(f) => f as libc::sighandler_t,
    };

    // SAFETY: installing a process signal disposition; `oact` receives the
    // previous disposition. Mirrors the C pqsignal() body: empty mask, flags
    // as computed above.
    unsafe {
        let mut act: libc::sigaction = core::mem::zeroed();
        act.sa_sigaction = handler;
        libc::sigemptyset(&mut act.sa_mask);
        act.sa_flags = 0;
        if signo != libc::SIGALRM {
            act.sa_flags |= libc::SA_RESTART;
        }
        if signo == libc::SIGCHLD {
            act.sa_flags |= libc::SA_NOCLDSTOP;
        }
        let mut oact: libc::sigaction = core::mem::zeroed();
        if libc::sigaction(signo, &act, &mut oact) < 0 {
            return SigDisposition::Error; // C: return SIG_ERR
        }
        match oact.sa_sigaction {
            x if x == libc::SIG_DFL => SigDisposition::Default,
            x if x == libc::SIG_IGN => SigDisposition::Ignore,
            // SAFETY: the previous action is neither SIG_DFL nor SIG_IGN, so
            // it is the address of a handler function installed earlier with
            // the C `void (*)(int)` ABI.
            x => SigDisposition::Handler(core::mem::transmute::<
                libc::sighandler_t,
                fn(i32),
            >(x)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn a_handler(_signo: i32) {}

    #[test]
    fn handler_disposition_round_trips_through_sigaction() {
        // Install a concrete handler on a benign signal (SIGURG: unused
        // elsewhere in the test process, and distinct from the SIGWINCH the
        // sentinel test uses, since dispositions are process-wide); the next
        // install must report it back as the same typed fn(i32).
        let original = pqsignal(libc::SIGURG, SigHandler::Handler(a_handler));
        let prev = pqsignal(libc::SIGURG, SigHandler::Default);
        assert_eq!(prev, SigDisposition::Handler(a_handler));
        pqsignal(
            libc::SIGURG,
            original
                .as_handler()
                .expect("previous disposition is reinstallable"),
        );
    }

    #[test]
    fn invalid_signal_returns_sig_err() {
        // sigaction(2) rejects signal 0 / out-of-range signals: C returns
        // SIG_ERR, the port returns SigDisposition::Error.
        assert_eq!(pqsignal(0, SigHandler::Default), SigDisposition::Error);
        assert_eq!(pqsignal(99999, SigHandler::Ignore), SigDisposition::Error);
    }

    #[test]
    fn install_returns_previous_disposition() {
        // SIGWINCH is benign and unused elsewhere in the test process. The
        // previous disposition reported by the second call must be exactly
        // what the first call installed; restore the original at the end.
        let original = pqsignal(libc::SIGWINCH, SigHandler::Ignore);
        let prev = pqsignal(libc::SIGWINCH, SigHandler::Default);
        assert_eq!(prev, SigDisposition::Ignore);
        let prev = pqsignal(
            libc::SIGWINCH,
            original.as_handler().expect("install succeeded"),
        );
        assert_eq!(prev, SigDisposition::Default);
    }

    #[test]
    fn handler_round_trips_through_kernel() {
        // Uses SIGURG (not SIGWINCH) so the parallel-running test above can't
        // race on the same process-global disposition.
        let original = pqsignal(libc::SIGURG, SigHandler::Handler(a_handler));
        assert_ne!(original, SigDisposition::Error);
        let prev = pqsignal(libc::SIGURG, SigHandler::Default);
        assert_eq!(prev, SigDisposition::Handler(a_handler));
        pqsignal(
            libc::SIGURG,
            original.as_handler().expect("install succeeded"),
        );
    }
}
