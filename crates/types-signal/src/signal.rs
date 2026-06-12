//! C's `pqsigfunc` is `void (*)(int)`, overloaded with the sentinels
//! `SIG_DFL`, `SIG_IGN`, and (as a return value only) `SIG_ERR`. The port
//! models those as the owned [`SigDisposition`] enum so no raw function
//! pointer crosses an API. A concrete handler is carried as the `usize`
//! address of a `fn(i32)`; convert with [`disposition_from_handler`] /
//! [`handler_from_disposition`].

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
    /// `SIG_ERR` — used by C only as the failure return of `signal()`/
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
