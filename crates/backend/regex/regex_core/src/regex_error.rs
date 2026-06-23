//! The regex engine's INTERNAL error contract.
//!
//! The regex package communicates failures via integer `REG_*` codes (see
//! [`crate::regex_consts`]). That is the *public* channel exposed through
//! `pg_regcomp`/`pg_regexec` return values. Internally, the Rust port threads
//! results through `RegResult<T>`, where the error variant carries the same
//! `REG_*` code.
//!
//! Deliberately does NOT use `PgError`: the core regex engine is independent of
//! the backend error machinery; callers map `REG_*` codes to ereport() at the
//! boundary (the export-free-error family wraps the seam results).

use crate::regex_consts;

/// An internal regex error carrying a `REG_*` code (see [`crate::regex_consts`]).
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct RegError(pub i32);

/// Standard result type for the regex engine's internal functions.
pub type RegResult<T> = Result<T, RegError>;

impl RegError {
    /// Construct an error from a `REG_*` code.
    #[inline]
    pub const fn new(code: i32) -> Self {
        RegError(code)
    }

    /// The underlying `REG_*` code.
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

// Convenience constructors for the common error codes. These are trivial
// inline helpers; no logic beyond wrapping the constant.

/// REG_ESPACE: out of memory.
#[inline]
pub const fn err_espace() -> RegError {
    RegError(regex_consts::REG_ESPACE)
}

/// REG_ASSERT: "can't happen" -- you found a bug.
#[inline]
pub const fn err_assert() -> RegError {
    RegError(regex_consts::REG_ASSERT)
}

/// REG_INVARG: invalid argument to regex function.
#[inline]
pub const fn err_invarg() -> RegError {
    RegError(regex_consts::REG_INVARG)
}

/// REG_ETOOBIG: regular expression is too complex.
#[inline]
pub const fn err_etoobig() -> RegError {
    RegError(regex_consts::REG_ETOOBIG)
}

/// REG_ECOLORS: too many colors.
#[inline]
pub const fn err_ecolors() -> RegError {
    RegError(regex_consts::REG_ECOLORS)
}

impl From<alloc::collections::TryReserveError> for RegError {
    /// A `try_reserve` failure is the idiomatic analogue of C's `MALLOC`
    /// returning NULL, which the regex engine reports as `REG_ESPACE`.
    #[inline]
    fn from(_: alloc::collections::TryReserveError) -> Self {
        err_espace()
    }
}
