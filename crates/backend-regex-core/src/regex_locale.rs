//! Family: **regex-locale** — `regc_locale.c` + `regc_pg_locale.c`: the
//! locale-ctype boundary of the regex engine.
//!
//! This family OWNS the `pg_wc_is*` probe family, `pg_wc_toupper`/`tolower`,
//! `pg_set_regex_collation`, `pg_ctype_get_cache`, the column-index selector,
//! and the leaf collating-element / equivalence-class / named-class routines.
//! These are the functions `regc_color.c`/`regc_lex.c` reach through to learn
//! character classification.
//!
//! The probes CONSUME `pg_locale_t`, ICU/libc ctype, and `pg_strncoll`/
//! `pg_strnxfrm`, which are owned by `backend-utils-adt-pg-locale` (not yet
//! ported). When the family logic lands, those cross-crate calls route through
//! that owner's `-seams` crate; at this scaffold stage they live inside the
//! `todo!()` bodies.
//!
//! `pg_set_regex_collation` selects the active locale for the duration of one
//! compile/exec; per AGENTS.md backend-global rules the resolved locale state
//! is per-backend `thread_local!`, not a shared static (added with the logic).

use mcx::Mcx;
use types_core::{Oid, PgWChar};

use crate::regex_error::RegResult;
use crate::regguts::{chr, color, ColorMap, Cvec};

// ---------------------------------------------------------------------------
// regc_pg_locale.c — collation selection + the pg_wc_* probe family
// ---------------------------------------------------------------------------

/// `pg_set_regex_collation(Oid collation)` — set the collation these probes
/// obey for the current compile/exec. Resolves the `pg_locale_t` from the
/// (unported) pg-locale owner; can ereport on a bad collation, hence
/// `PgResult`.
pub fn pg_set_regex_collation(_collation: Oid) -> RegResult<()> {
    todo!("regc_pg_locale.c:pg_set_regex_collation")
}

/// `pg_wc_isdigit(pg_wchar c)`.
pub fn pg_wc_isdigit(_c: PgWChar) -> bool {
    todo!("regc_pg_locale.c:pg_wc_isdigit")
}

/// `pg_wc_isalpha(pg_wchar c)`.
pub fn pg_wc_isalpha(_c: PgWChar) -> bool {
    todo!("regc_pg_locale.c:pg_wc_isalpha")
}

/// `pg_wc_isalnum(pg_wchar c)`.
pub fn pg_wc_isalnum(_c: PgWChar) -> bool {
    todo!("regc_pg_locale.c:pg_wc_isalnum")
}

/// `pg_wc_isword(pg_wchar c)`.
pub fn pg_wc_isword(_c: PgWChar) -> bool {
    todo!("regc_pg_locale.c:pg_wc_isword")
}

/// `pg_wc_isupper(pg_wchar c)`.
pub fn pg_wc_isupper(_c: PgWChar) -> bool {
    todo!("regc_pg_locale.c:pg_wc_isupper")
}

/// `pg_wc_islower(pg_wchar c)`.
pub fn pg_wc_islower(_c: PgWChar) -> bool {
    todo!("regc_pg_locale.c:pg_wc_islower")
}

/// `pg_wc_isgraph(pg_wchar c)`.
pub fn pg_wc_isgraph(_c: PgWChar) -> bool {
    todo!("regc_pg_locale.c:pg_wc_isgraph")
}

/// `pg_wc_isprint(pg_wchar c)`.
pub fn pg_wc_isprint(_c: PgWChar) -> bool {
    todo!("regc_pg_locale.c:pg_wc_isprint")
}

/// `pg_wc_ispunct(pg_wchar c)`.
pub fn pg_wc_ispunct(_c: PgWChar) -> bool {
    todo!("regc_pg_locale.c:pg_wc_ispunct")
}

/// `pg_wc_isspace(pg_wchar c)`.
pub fn pg_wc_isspace(_c: PgWChar) -> bool {
    todo!("regc_pg_locale.c:pg_wc_isspace")
}

/// `pg_wc_toupper(pg_wchar c)`.
pub fn pg_wc_toupper(_c: PgWChar) -> PgWChar {
    todo!("regc_pg_locale.c:pg_wc_toupper")
}

/// `pg_wc_tolower(pg_wchar c)`.
pub fn pg_wc_tolower(_c: PgWChar) -> PgWChar {
    todo!("regc_pg_locale.c:pg_wc_tolower")
}

/// A `pg_wc_probefunc`: one of the `pg_wc_is*` probes, used to populate a
/// ctype cache (`pg_ctype_get_cache`).
pub type PgWcProbeFunc = fn(PgWChar) -> bool;

/// `pg_ctype_get_cache(pg_wc_probefunc probefunc, int cclasscode)` — build (or
/// return the cached) cvec of all simple chrs matching a probe. Allocates the
/// cvec, hence `Mcx`/`RegResult` (NULL on OOM in C -> `REG_ESPACE`).
pub fn pg_ctype_get_cache<'mcx>(
    _mcx: Mcx<'mcx>,
    _probefunc: PgWcProbeFunc,
    _cclasscode: i32,
) -> RegResult<Cvec> {
    todo!("regc_pg_locale.c:pg_ctype_get_cache")
}

// ---------------------------------------------------------------------------
// regc_locale.c — collating elements, ranges, classes, casing
// ---------------------------------------------------------------------------

/// `element(struct vars *v, const chr *startp, const chr *endp)` — look up a
/// [.collating element.] by name.
pub fn element(_startp: &[chr]) -> RegResult<chr> {
    todo!("regc_locale.c:element")
}

/// `range(struct vars *v, chr a, chr b, int cases)` — build the cvec for a
/// character range `a-b`, optionally case-expanded.
pub fn range<'mcx>(_mcx: Mcx<'mcx>, _a: chr, _b: chr, _cases: i32) -> RegResult<Cvec> {
    todo!("regc_locale.c:range")
}

/// `eclass(struct vars *v, chr c, int cases)` — build the cvec for the
/// equivalence class `[=c=]`.
pub fn eclass<'mcx>(_mcx: Mcx<'mcx>, _c: chr, _cases: i32) -> RegResult<Cvec> {
    todo!("regc_locale.c:eclass")
}

/// `lookupcclass(struct vars *v, const chr *startp, const chr *endp)` — map a
/// `[:name:]` to its `enum char_classes` code.
pub fn lookupcclass(_startp: &[chr]) -> RegResult<i32> {
    todo!("regc_locale.c:lookupcclass")
}

/// `cclasscvec(struct vars *v, int cclasscode, int cases)` — build the cvec
/// for a named character class.
pub fn cclasscvec<'mcx>(_mcx: Mcx<'mcx>, _cclasscode: i32, _cases: i32) -> RegResult<Cvec> {
    todo!("regc_locale.c:cclasscvec")
}

/// `cclass_column_index(struct colormap *cm, chr c)` — the locale-dependent
/// high-colormap column selector: ORs together the classbits of every cclass
/// `c` belongs to. Dispatches to the `pg_wc_is*` probes above.
pub fn cclass_column_index(_cm: &ColorMap, _c: chr) -> i32 {
    todo!("regc_locale.c:cclass_column_index")
}

/// `allcases(struct vars *v, chr c)` — build the cvec of all case variants of
/// `c`.
pub fn allcases<'mcx>(_mcx: Mcx<'mcx>, _c: chr) -> RegResult<Cvec> {
    todo!("regc_locale.c:allcases")
}

/// `cmp(const chr *x, const chr *y, size_t len)` — the case-sensitive chr-string
/// comparator (the default `g->compare`).
pub fn cmp(_x: &[chr], _y: &[chr], _len: usize) -> i32 {
    todo!("regc_locale.c:cmp")
}

/// `casecmp(const chr *x, const chr *y, size_t len)` — the case-insensitive
/// chr-string comparator (the `REG_ICASE` `g->compare`).
pub fn casecmp(_x: &[chr], _y: &[chr], _len: usize) -> i32 {
    todo!("regc_locale.c:casecmp")
}

/// Suppress unused-import warnings for `color` until the leaf routines that
/// return colors land.
#[allow(dead_code)]
fn _uses_color(_c: color) {}
