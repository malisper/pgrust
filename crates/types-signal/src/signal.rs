//! C's `pqsigfunc` is `void (*)(int)`, overloaded with the sentinels
//! `SIG_DFL`, `SIG_IGN`, and (as a return value only) `SIG_ERR`. The port
//! models those as the owned [`SigDisposition`] enum so the magic pointer
//! values never masquerade as handlers; a concrete handler stays a typed
//! `fn(i32)`.

/// A signal handler disposition, the owned stand-in for C's `pqsigfunc`
/// (`void (*)(int)`) once the three magic pointer values are distinguished.
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
