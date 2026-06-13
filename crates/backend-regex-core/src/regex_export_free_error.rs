//! Family: **regex-export-free-error** — `regexport.c` (the 11 `pg_reg_get*`
//! NFA/color exporters), `regfree.c` (`pg_regfree`), `regerror.c`
//! (`pg_regerror` message table), plus the opaque-[`RegexHandle`] seam adapter
//! and the per-backend `thread_local!` handle registry that the four inward
//! seams marshal through.
//!
//! # The opaque-handle boundary
//!
//! Across the public seam ([`backend_regex_core_seams`]) the compiled regex is
//! the opaque [`types_regex::RegexHandle`] token (see `types-regex` for why
//! that opacity is inherited from the engine's compile/exec split, not
//! introduced). The real owned [`RegexT`] lives in this crate; the registry
//! maps a handle to its `RegexT`. `pg_regcomp` registers and returns a handle;
//! `pg_regexec`/`pg_regprefix` look it up; `pg_regfree` removes it.
//!
//! The registry is **per-backend** (`thread_local!`): every backend compiles
//! its own regexes (the ADT cache is itself per-backend), so a shared static
//! would cross-link sessions — forbidden by AGENTS.md backend-global rules.

use std::cell::RefCell;
use std::collections::HashMap;

use mcx::Mcx;
use types_core::PgWChar;
use types_error::PgResult;
use types_regex::{
    RegMatch, RegcompResult, RegexCompiled, RegexFailure, RegexHandle, RegexecResult,
    RegprefixResult,
};

use crate::regguts::RegexT;

// ===========================================================================
// per-backend handle registry
// ===========================================================================

thread_local! {
    /// Maps a live [`RegexHandle`] to its owned [`RegexT`]. Per-backend; see
    /// the module docs for why this is `thread_local!`, not a shared static.
    static REGEX_REGISTRY: RefCell<HashMap<u64, RegexT>> = RefCell::new(HashMap::new());
    /// Monotonic handle id allocator (C uses the `regex_t *` pointer identity;
    /// here a per-backend counter mints the opaque token).
    static NEXT_HANDLE: RefCell<u64> = const { RefCell::new(1) };
}

/// Register an owned [`RegexT`], returning the opaque handle the public seam
/// hands back to the ADT layer.
fn register(_re: RegexT) -> RegexHandle {
    todo!("regex_export_free_error: handle registry insert")
}

/// Run `f` with a shared reference to the [`RegexT`] behind `handle` (the
/// exec/prefix/export read path). Returns the registry-miss case to the caller.
fn with_regex<R>(_handle: RegexHandle, _f: impl FnOnce(&RegexT) -> R) -> Option<R> {
    todo!("regex_export_free_error: handle registry lookup")
}

// ===========================================================================
// public seam adapters (installed by `crate::init_seams`)
// ===========================================================================

/// Adapter for the `pg_regcomp` inward seam: compile the pattern (compile
/// family), register the result, and return the handle as `RegcompResult`. The
/// non-OK `REG_*` code is mapped through [`pg_regerror`] into
/// [`RegexFailure`].
pub fn seam_pg_regcomp(
    _pattern: &[PgWChar],
    _cflags: i32,
    _collation: types_core::Oid,
) -> PgResult<RegcompResult> {
    // Scaffold shape: the body will run `regex_compile::pg_regcomp`, then
    // either `register(...)` -> `RegcompResult::Compiled(RegexCompiled { handle,
    // re_nsub })` or `RegcompResult::Failed(RegexFailure { message:
    // pg_regerror(code) })`.
    let _ = |c: RegexCompiled| RegcompResult::Compiled(c);
    let _ = |f: RegexFailure| RegcompResult::Failed(f);
    todo!("regcomp.c:pg_regcomp seam adapter")
}

/// Adapter for the `pg_regexec` inward seam: look up `handle`, run the matcher
/// (exec family), fill `pmatch` in place, and map the result code.
pub fn seam_pg_regexec(
    _handle: RegexHandle,
    _data: &[PgWChar],
    _search_start: i32,
    _pmatch: &mut [RegMatch],
) -> PgResult<RegexecResult> {
    todo!("regexec.c:pg_regexec seam adapter")
}

/// Adapter for the `pg_regprefix` inward seam: look up `handle`, run the prefix
/// extractor (exec family), and copy the prefix into `mcx`.
pub fn seam_pg_regprefix<'mcx>(
    _mcx: Mcx<'mcx>,
    _handle: RegexHandle,
) -> PgResult<RegprefixResult<'mcx>> {
    todo!("regprefix.c:pg_regprefix seam adapter")
}

/// Adapter for the `pg_regfree` inward seam: remove `handle` from the registry,
/// dropping the owned [`RegexT`] (which frees the engine state).
pub fn seam_pg_regfree(_handle: RegexHandle) {
    todo!("regfree.c:pg_regfree seam adapter")
}

// ===========================================================================
// regfree.c
// ===========================================================================

/// `pg_regfree(regex_t *re)` — free a compiled RE. Under Rust ownership this
/// drops the owned [`RegexT`] (and thus its `guts`); the registry removal is
/// done by [`seam_pg_regfree`].
pub fn pg_regfree(_re: RegexT) {
    todo!("regfree.c:pg_regfree")
}

// ===========================================================================
// regerror.c
// ===========================================================================

/// `pg_regerror(int errcode, const regex_t *preg, char *errbuf, size_t
/// errbuf_size)` — format a `REG_*` code into its human-readable message,
/// honoring the `REG_ATOI`/`REG_ITOA` debug specials. Returns the full message
/// (the caller truncates to its buffer); the message table is owned here.
pub fn pg_regerror(_errcode: i32) -> alloc::string::String {
    todo!("regerror.c:pg_regerror")
}

// ===========================================================================
// regexport.c — NFA / color exporters  (regexport.h)
// ===========================================================================

/// `regex_arc_t` (regexport.h) — one exported NFA arc.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct RegexArc {
    /// `co` — label (character-set color) of the arc.
    pub co: i32,
    /// `to` — next state number.
    pub to: i32,
}

/// `pg_reg_getnumstates(const regex_t *regex)`.
pub fn pg_reg_getnumstates(_re: &RegexT) -> i32 {
    todo!("regexport.c:pg_reg_getnumstates")
}

/// `pg_reg_getinitialstate(const regex_t *regex)`.
pub fn pg_reg_getinitialstate(_re: &RegexT) -> i32 {
    todo!("regexport.c:pg_reg_getinitialstate")
}

/// `pg_reg_getfinalstate(const regex_t *regex)`.
pub fn pg_reg_getfinalstate(_re: &RegexT) -> i32 {
    todo!("regexport.c:pg_reg_getfinalstate")
}

/// `pg_reg_getnumoutarcs(const regex_t *regex, int st)`.
pub fn pg_reg_getnumoutarcs(_re: &RegexT, _st: i32) -> i32 {
    todo!("regexport.c:pg_reg_getnumoutarcs")
}

/// `pg_reg_getoutarcs(const regex_t *regex, int st, regex_arc_t *arcs, int
/// arcs_len)` — fill `arcs` (up to `arcs.len()`) with state `st`'s out-arcs.
pub fn pg_reg_getoutarcs(_re: &RegexT, _st: i32, _arcs: &mut [RegexArc]) {
    todo!("regexport.c:pg_reg_getoutarcs")
}

/// `pg_reg_getnumcolors(const regex_t *regex)`.
pub fn pg_reg_getnumcolors(_re: &RegexT) -> i32 {
    todo!("regexport.c:pg_reg_getnumcolors")
}

/// `pg_reg_colorisbegin(const regex_t *regex, int co)`.
pub fn pg_reg_colorisbegin(_re: &RegexT, _co: i32) -> bool {
    todo!("regexport.c:pg_reg_colorisbegin")
}

/// `pg_reg_colorisend(const regex_t *regex, int co)`.
pub fn pg_reg_colorisend(_re: &RegexT, _co: i32) -> bool {
    todo!("regexport.c:pg_reg_colorisend")
}

/// `pg_reg_getnumcharacters(const regex_t *regex, int co)`.
pub fn pg_reg_getnumcharacters(_re: &RegexT, _co: i32) -> i32 {
    todo!("regexport.c:pg_reg_getnumcharacters")
}

/// `pg_reg_getcharacters(const regex_t *regex, int co, pg_wchar *chars, int
/// chars_len)` — fill `chars` (up to `chars.len()`) with the characters of
/// color `co`.
pub fn pg_reg_getcharacters(_re: &RegexT, _co: i32, _chars: &mut [PgWChar]) {
    todo!("regexport.c:pg_reg_getcharacters")
}
