//! `regex/regex.h` vocabulary, trimmed to what the SQL-level consumers use.
//!
//! C's `regex_t` is a struct whose only consumer-visible field is `re_nsub`;
//! everything else (`re_guts`, `re_fns`, ...) is engine-internal state that
//! C reaches through internal function tables. The compiled regex therefore
//! crosses the engine seam as [`RegexCompiled`]: the consumed `re_nsub` plus
//! an opaque [`RegexHandle`] naming the engine-owned compiled state (the
//! inherited-opacity token for `re_guts`, same pattern as `types-scan`'s
//! `SysScanHandle`). The handle is released with the `pg_regfree` seam.
//!
//! The non-OK return codes of `pg_regcomp`/`pg_regexec`/`pg_regprefix` are
//! mirrored as enums below; the "hard failure" arms carry the
//! `pg_regerror`-formatted message so the *caller* can raise its own
//! `ereport` (`regexp.c` uses different message prefixes per call site).

use alloc::string::String;

use mcx::PgVec;
use types_core::PgWChar;

/// C: `pg_regoff_t` (`regex/regex.h`) — a match offset, in characters.
pub type pg_regoff_t = i64;

/// C: `regmatch_t` (`regex/regex.h`) — one (sub)match location. Offsets are
/// character (not byte) positions; `-1` means "no match for this
/// subexpression".
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RegMatch {
    /// `rm_so` — start of substring.
    pub rm_so: pg_regoff_t,
    /// `rm_eo` — end of substring.
    pub rm_eo: pg_regoff_t,
}

impl RegMatch {
    /// C: `{-1, -1}` — the "no match" value `pg_regexec` stores for unset
    /// submatch slots.
    pub const UNSET: RegMatch = RegMatch { rm_so: -1, rm_eo: -1 };
}

/// An opaque identity for a compiled regex living inside the regex-engine
/// subsystem (`backend-regex-core`). C: a `regex_t *`; the engine owns the
/// `re_guts` behind it. Freed via the `pg_regfree` seam.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct RegexHandle(pub u64);

/// The successful result of `pg_regcomp`: a live compiled RE plus the one
/// `regex_t` field its consumers read.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RegexCompiled {
    /// The engine-owned compiled-RE identity, threaded into later
    /// execute/prefix/free calls.
    pub handle: RegexHandle,
    /// C: `regex_t.re_nsub` — the number of capturing subexpressions.
    pub re_nsub: usize,
}

/// A non-OK, non-NOMATCH engine return code, already formatted through
/// `pg_regerror` (the engine owns the error-message table). The caller wraps
/// this in its own `ereport(ERROR)`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegexFailure {
    /// The `pg_regerror(code, re, errMsg, sizeof(errMsg))` text.
    pub message: String,
}

/// The outcome of one `pg_regcomp` call. C: the `REG_OKAY` /
/// everything-else return-code arms.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RegcompResult {
    /// C: `REG_OKAY`.
    Compiled(RegexCompiled),
    /// Any other return code, `pg_regerror`-formatted.
    Failed(RegexFailure),
}

/// The outcome of one `pg_regexec` call (the requested `pmatch` slots are
/// filled in place on a match). C: the `REG_OKAY` / `REG_NOMATCH` / other
/// return-code arms.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RegexecResult {
    /// C: `REG_OKAY` — matched; `pmatch` has been filled.
    Matched,
    /// C: `REG_NOMATCH` — no match at/after the search start.
    NoMatch,
    /// Any other return code, `pg_regerror`-formatted.
    Failed(RegexFailure),
}

/// The outcome of one `pg_regprefix` call. C: the `REG_NOMATCH` /
/// `REG_PREFIX` / `REG_EXACT` / other return-code arms. The prefix is
/// `pg_wchar` code points, allocated in the `Mcx` passed through the seam
/// (C: `palloc` in the caller's current context).
#[derive(Debug)]
pub enum RegprefixResult<'mcx> {
    /// C: `REG_NOMATCH` — there is no fixed prefix.
    NoMatch,
    /// C: `REG_PREFIX` — a fixed prefix, not the whole match.
    Prefix(PgVec<'mcx, PgWChar>),
    /// C: `REG_EXACT` — the exact (whole) match string.
    Exact(PgVec<'mcx, PgWChar>),
    /// Any other return code, `pg_regerror`-formatted.
    Failed(RegexFailure),
}

// ---------------------------------------------------------------------------
// Compile flags (`regex/regex.h` "regcomp() flags"). int bitmask in C.
// ---------------------------------------------------------------------------

/// C: `REG_BASIC` — BREs (convenience).
pub const REG_BASIC: i32 = 0o0;
/// C: `REG_EXTENDED` — EREs.
pub const REG_EXTENDED: i32 = 0o1;
/// C: `REG_ADVF` — advanced features in EREs.
pub const REG_ADVF: i32 = 0o2;
/// C: `REG_ADVANCED` — AREs (which are also EREs).
pub const REG_ADVANCED: i32 = 0o3;
/// C: `REG_QUOTE` — no special characters, none.
pub const REG_QUOTE: i32 = 0o4;
/// C: `REG_NOSPEC` — historical synonym for `REG_QUOTE`.
pub const REG_NOSPEC: i32 = REG_QUOTE;
/// C: `REG_ICASE` — ignore case.
pub const REG_ICASE: i32 = 0o10;
/// C: `REG_NOSUB` — caller doesn't need subexpr match data.
pub const REG_NOSUB: i32 = 0o20;
/// C: `REG_EXPANDED` — expanded format, white space & comments.
pub const REG_EXPANDED: i32 = 0o40;
/// C: `REG_NLSTOP` — \n doesn't match . or [^ ].
pub const REG_NLSTOP: i32 = 0o100;
/// C: `REG_NLANCH` — ^ matches after \n, $ before.
pub const REG_NLANCH: i32 = 0o200;
/// C: `REG_NEWLINE` — newlines are line terminators (`REG_NLSTOP | REG_NLANCH`).
pub const REG_NEWLINE: i32 = 0o300;
/// C: `REG_PEND` — backward-compatibility hack.
pub const REG_PEND: i32 = 0o400;
/// C: `REG_EXPECT` — report details on partial/limited matches.
pub const REG_EXPECT: i32 = 0o1000;
/// C: `REG_BOSONLY` — temporary kludge for BOS-only matches.
pub const REG_BOSONLY: i32 = 0o2000;
