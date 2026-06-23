//! `regex/regex.h` vocabulary, trimmed to what the SQL-level consumers use.
//!
//! C's `regex_t` is a struct whose only consumer-visible field is `re_nsub`;
//! everything else (`re_guts`, `re_fns`, ...) is engine-internal state that
//! C reaches through internal function tables. The compiled regex therefore
//! crosses the engine seam as [`RegexCompiled`]: the consumed `re_nsub` plus
//! the real engine-owned compiled state, carried type-erased as an
//! `Rc<dyn Any>`. The engine downcasts it back to its own `regex_t` at the
//! seam boundary (the leaf cycle-break used for the relcache cell): the real
//! value is carried and recovered, not an introduced handle/opacity token.
//! The compiled state is released by dropping the `Rc` (the `pg_regfree`
//! seam takes the carrier by value).
//!
//! The non-OK return codes of `pg_regcomp`/`pg_regexec`/`pg_regprefix` are
//! mirrored as enums below; the "hard failure" arms carry the
//! `pg_regerror`-formatted message so the *caller* can raise its own
//! `ereport` (`regexp.c` uses different message prefixes per call site).

use alloc::rc::Rc;
use alloc::string::String;
use core::any::Any;

use ::mcx::PgVec;
use ::types_core::PgWChar;

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

/// The successful result of `pg_regcomp`: a live compiled RE plus the one
/// `regex_t` field its consumers read.
///
/// The engine-owned compiled state (the C `regex_t` with its `re_guts`) is
/// carried here type-erased as `Rc<dyn Any>`; the engine seam adapters
/// recover the concrete `regex_t` with `downcast_ref`. This is the same
/// real-value-carried, recovered-by-downcast cycle-break used for the
/// relcache cell — no handle or registry stands between the consumer and the
/// engine state. Sharing is `Rc` so a cache entry can hand out clones; the
/// last drop frees the engine state (`pg_regfree`).
#[derive(Clone)]
pub struct RegexCompiled {
    /// The engine-owned compiled-RE value (`regex_t`), type-erased; the engine
    /// downcasts it back at the seam. Threaded into later execute/prefix/free
    /// calls.
    pub engine: Rc<dyn Any>,
    /// C: `regex_t.re_nsub` — the number of capturing subexpressions.
    pub re_nsub: usize,
}

impl core::fmt::Debug for RegexCompiled {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        // The engine value is opaque (`dyn Any`); show only the read field.
        f.debug_struct("RegexCompiled")
            .field("re_nsub", &self.re_nsub)
            .finish_non_exhaustive()
    }
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
///
/// No `Eq`/`PartialEq`: the `Compiled` arm carries an `Rc<dyn Any>` engine
/// value, which is neither comparable nor hashable.
#[derive(Clone, Debug)]
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
