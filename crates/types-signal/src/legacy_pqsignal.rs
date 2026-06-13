//! C's `pqsigfunc` is `void (*)(int)`, overloaded with the sentinels
//! `SIG_DFL`, `SIG_IGN`, and (as a return value only) `SIG_ERR`. The port
//! splits that into two owned enums: [`SigHandler`] for what a caller may
//! install (no `SIG_ERR`, which C never accepts as input) and
//! [`SigDisposition`] for what `pqsignal()` reports back (previous handler,
//! or `SIG_ERR` on failure). A concrete handler is a typed `fn(i32)`.
//!
//! (Module name kept from src-idiomatic `types::legacy_pqsignal`; the types
//! serve both the backend `src/port/pqsignal.c` wrapper and the legacy libpq
//! one.)

/// An installable signal handler — the input half of C's `pqsigfunc`.
/// `SIG_ERR` is excluded by construction: C only ever returns it.
// Handler equality is fn-pointer address equality, exactly the C semantics
// (comparing pqsigfunc values against SIG_DFL/SIG_IGN/installed handlers).
#[allow(unpredictable_function_pointer_comparisons)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SigHandler {
    /// `SIG_DFL` — restore the default action for the signal.
    Default,
    /// `SIG_IGN` — ignore the signal.
    Ignore,
    /// A concrete handler function.
    Handler(fn(i32)),
}

/// A reported signal disposition — the return half of C's `pqsigfunc`:
/// the previous handler, or [`SigDisposition::Error`] (C's `SIG_ERR`) when
/// installation failed.
// Handler equality is fn-pointer address equality, exactly the C semantics.
#[allow(unpredictable_function_pointer_comparisons)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SigDisposition {
    /// `SIG_DFL` — the default action was in effect.
    Default,
    /// `SIG_IGN` — the signal was being ignored.
    Ignore,
    /// A concrete handler function was installed.
    Handler(fn(i32)),
    /// `SIG_ERR` — the failure return of `signal()`/`pqsignal()`.
    Error,
}

impl SigDisposition {
    /// The installable handler this disposition reports, if any — used to
    /// reinstall a previously returned disposition. `None` for [`Error`].
    ///
    /// [`Error`]: SigDisposition::Error
    pub fn as_handler(self) -> Option<SigHandler> {
        match self {
            SigDisposition::Default => Some(SigHandler::Default),
            SigDisposition::Ignore => Some(SigHandler::Ignore),
            SigDisposition::Handler(f) => Some(SigHandler::Handler(f)),
            SigDisposition::Error => None,
        }
    }
}
