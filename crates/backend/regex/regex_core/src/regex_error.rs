
use crate::regex_consts;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct RegError(pub i32);

pub type RegResult<T> = Result<T, RegError>;

impl RegError {
    #[inline]
    pub const fn new(code: i32) -> Self {
        RegError(code)
    }

    #[inline]
    pub const fn code(self) -> i32 {
        self.0
    }
}

impl From<i32> for RegError {
    #[inline]
    fn from(code: i32) -> Self {
        RegError(code)
    }
}


#[inline]
pub const fn err_espace() -> RegError {
    RegError(regex_consts::REG_ESPACE)
}

#[inline]
pub const fn err_assert() -> RegError {
    RegError(regex_consts::REG_ASSERT)
}

#[inline]
pub const fn err_invarg() -> RegError {
    RegError(regex_consts::REG_INVARG)
}

#[inline]
pub const fn err_etoobig() -> RegError {
    RegError(regex_consts::REG_ETOOBIG)
}

#[inline]
pub const fn err_ecolors() -> RegError {
    RegError(regex_consts::REG_ECOLORS)
}

impl From<alloc::collections::TryReserveError> for RegError {
    #[inline]
    fn from(_: alloc::collections::TryReserveError) -> Self {
        err_espace()
    }
}

// ---- Cooperative cancellation: pgrust rendering of C's INTERRUPT(re) ----
//
// C's regcustom.h defines `INTERRUPT(re)` as `CHECK_FOR_INTERRUPTS()`, which
// aborts the operation via longjmp when a cancel/termination is pending. The
// ported engine unwinds through `RegResult` instead, so `check_interrupt`
// stashes the real ereport error (the `PgResult::Err` produced by the interrupt
// seam) and returns the engine-internal REG_CANCEL sentinel; the
// regcomp/regexec/regprefix seam boundaries re-surface the stashed error as
// their own `PgResult::Err`, exactly as C's longjmp would have aborted the
// whole operation. REG_CANCEL is never shown to users (it is intercepted at the
// seam), so it is not added to the pg_regerror message table.
std::thread_local! {
    static PENDING_CANCEL: core::cell::Cell<Option<alloc::boxed::Box<types_error::PgError>>> =
        const { core::cell::Cell::new(None) };
}

#[inline]
pub const fn err_cancel() -> RegError {
    RegError(regex_consts::REG_CANCEL)
}

/// `INTERRUPT(re)` — a cooperative cancellation point. When a
/// statement_timeout / query cancel / backend termination is pending, stashes
/// the real ereport error and returns `Err(REG_CANCEL)` so a caller's `?`
/// unwinds the engine promptly. The `InterruptPending` fast path keeps the hot
/// NFA/DFA loops branch-only when nothing is pending (matching the inline flag
/// test other crates use before the seam call).
#[inline]
pub fn check_interrupt() -> RegResult<()> {
    if init_small::globals::InterruptPending() {
        if let Err(e) = postgres_seams::check_for_interrupts::call() {
            PENDING_CANCEL.with(|c| c.set(Some(e)));
            return Err(err_cancel());
        }
    }
    Ok(())
}

/// Consume the ereport error stashed by the most recent `check_interrupt` that
/// reported REG_CANCEL. The seam boundaries call this on the REG_CANCEL arm.
/// The fallback (empty stash) is not expected to be reachable — REG_CANCEL is
/// only ever returned immediately after a stash — but yields a well-formed
/// query-canceled error rather than a bogus regex message if it ever is.
pub fn take_cancel_error() -> alloc::boxed::Box<types_error::PgError> {
    PENDING_CANCEL.with(|c| c.take()).unwrap_or_else(|| {
        alloc::boxed::Box::new(
            types_error::PgError::error("canceling statement due to user request")
                .with_sqlstate(types_error::ERRCODE_QUERY_CANCELED),
        )
    })
}
