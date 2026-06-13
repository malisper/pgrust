//! C's `pqsigfunc` is `void (*)(int)`, overloaded with the sentinels
//! `SIG_DFL`, `SIG_IGN`, and (as a return value only) `SIG_ERR`. The port
//! models those as the owned [`SigDisposition`] enum so the magic pointer
//! values never cross an API. A concrete handler is carried as the `fn(i32)`
//! it is.
//!
//! (Module name kept from src-idiomatic `types::legacy_pqsignal`; the type
//! serves both the backend `src/port/pqsignal.c` wrapper and the legacy libpq
//! one.)

/// A signal handler disposition, the owned stand-in for C's `pqsigfunc`
/// (`void (*)(int)`) once the three magic pointer values are distinguished.
// Equality exists for the sentinel variants (C compares against
// SIG_DFL/SIG_IGN/SIG_ERR); comparing two `Handler` values inherits C's
// function-pointer-comparison semantics.
#[allow(unpredictable_function_pointer_comparisons)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SigDisposition {
    /// `SIG_DFL` — restore the default action for the signal.
    Default,
    /// `SIG_IGN` — ignore the signal.
    Ignore,
    /// A concrete handler function.
    Handler(fn(i32)),
    /// `SIG_ERR` — used by C only as the failure return of `signal()`/
    /// `pqsignal()`; never installable.
    Error,
}

/// Build a [`SigDisposition::Handler`] from a concrete `fn(i32)` handler.
#[inline]
pub fn disposition_from_handler(handler: fn(i32)) -> SigDisposition {
    SigDisposition::Handler(handler)
}

/// Recover the `fn(i32)` handler from a [`SigDisposition::Handler`], if any.
///
/// Returns `None` for `Default` / `Ignore` / `Error`.
#[inline]
pub fn handler_from_disposition(disp: SigDisposition) -> Option<fn(i32)> {
    match disp {
        SigDisposition::Handler(handler) => Some(handler),
        _ => None,
    }
}
