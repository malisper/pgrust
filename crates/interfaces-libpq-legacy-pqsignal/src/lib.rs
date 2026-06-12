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
//! models those as the owned [`SigDisposition`] enum so no raw function
//! pointer crosses the API. A concrete handler is carried as the `usize`
//! address of a `fn(i32)`; convert with [`disposition_from_handler`] /
//! [`handler_from_disposition`].

/// Install this crate's seams. The unit is a leaf with no inward seam
/// declarations, so there is nothing to `set()`.
pub fn init_seams() {}

/// A signal handler disposition, the owned stand-in for C's `pqsigfunc`
/// (`void (*)(int)`) once the three magic pointer values are distinguished.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SigDisposition {
    /// `SIG_DFL` — restore the default action for the signal.
    Default,
    /// `SIG_IGN` — ignore the signal.
    Ignore,
    /// A concrete handler function: the address of a `fn(i32)`. Function
    /// pointers are `Copy` and round-trip losslessly through `usize`; the OS
    /// layer ultimately needs the raw address, but the public API stays free
    /// of raw pointer types.
    Handler(usize),
    /// `SIG_ERR` — used by C only as the failure return of `signal()`/our
    /// `pqsignal()`; never installable.
    Error,
}

/// Build a [`SigDisposition::Handler`] from a concrete `fn(i32)` handler.
#[inline]
pub fn disposition_from_handler(handler: fn(i32)) -> SigDisposition {
    SigDisposition::Handler(handler as usize)
}

/// Recover the `fn(i32)` handler from a [`SigDisposition::Handler`], if any.
///
/// Returns `None` for `Default` / `Ignore` / `Error` (and for the degenerate
/// null address).
#[inline]
pub fn handler_from_disposition(disp: SigDisposition) -> Option<fn(i32)> {
    match disp {
        SigDisposition::Handler(addr) if addr != 0 => {
            // SAFETY: `addr` is the address of an `fn(i32)`, captured by
            // `disposition_from_handler` (fn pointers round-trip through
            // `usize`). The disposition type guarantees this is a Handler,
            // not one of the `SIG_*` sentinels.
            Some(unsafe { core::mem::transmute::<usize, fn(i32)>(addr) })
        }
        _ => None,
    }
}

/// `pqsigfunc pqsignal(int signo, pqsigfunc func)` — install `func` as the
/// handler for `signo`, with the frozen 9.2 semantics, and return the
/// previous disposition (or [`SigDisposition::Error`], C's `SIG_ERR`, if
/// `sigaction(2)` failed).
///
/// This is the non-WIN32 path of the C function (the only path this tree
/// targets): install with an empty signal mask; `sa_flags = SA_RESTART`
/// unless the signal is `SIGALRM`, plus `SA_NOCLDSTOP` for `SIGCHLD` (the C
/// `#ifdef SA_NOCLDSTOP` is always satisfied on our platforms).
pub fn pqsignal(signo: i32, func: SigDisposition) -> SigDisposition {
    let handler: libc::sighandler_t = match func {
        SigDisposition::Default => libc::SIG_DFL,
        SigDisposition::Ignore => libc::SIG_IGN,
        SigDisposition::Handler(addr) => addr as libc::sighandler_t,
        SigDisposition::Error => panic!(
            "pqsignal: SIG_ERR is a return-only sentinel and is never installable"
        ),
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
            x => SigDisposition::Handler(x as usize),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn a_handler(_signo: i32) {}

    #[test]
    fn handler_disposition_round_trips() {
        let handler: fn(i32) = a_handler;
        let disp = disposition_from_handler(handler);
        match disp {
            SigDisposition::Handler(addr) => assert_eq!(addr, handler as usize),
            other => panic!("expected Handler, got {other:?}"),
        }

        let recovered =
            handler_from_disposition(disp).expect("handler round-trips");
        assert_eq!(recovered as usize, handler as usize);
    }

    #[test]
    fn sentinels_have_no_handler() {
        assert!(handler_from_disposition(SigDisposition::Default).is_none());
        assert!(handler_from_disposition(SigDisposition::Ignore).is_none());
        assert!(handler_from_disposition(SigDisposition::Error).is_none());
        assert!(handler_from_disposition(SigDisposition::Handler(0)).is_none());
    }

    #[test]
    fn invalid_signal_returns_sig_err() {
        // sigaction(2) rejects signal 0 / out-of-range signals: C returns
        // SIG_ERR, the port returns SigDisposition::Error.
        assert_eq!(pqsignal(0, SigDisposition::Default), SigDisposition::Error);
        assert_eq!(
            pqsignal(99999, SigDisposition::Ignore),
            SigDisposition::Error
        );
    }

    #[test]
    fn install_returns_previous_disposition() {
        // SIGWINCH is benign and unused elsewhere in the test process. The
        // previous disposition reported by the second call must be exactly
        // what the first call installed; restore the original at the end.
        let original = pqsignal(libc::SIGWINCH, SigDisposition::Ignore);
        let prev = pqsignal(libc::SIGWINCH, SigDisposition::Default);
        assert_eq!(prev, SigDisposition::Ignore);
        let prev = pqsignal(libc::SIGWINCH, original);
        assert_eq!(prev, SigDisposition::Default);
    }
}
