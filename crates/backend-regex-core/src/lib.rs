#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
// Some foundation helpers and accessors mirror C entry points that have no
// in-tree caller yet; keep them rather than diverge from the C surface.
#![allow(dead_code)]
// Every fallible function returns the project-wide `PgResult` (== `Result<_,
// PgError>`); `PgError` is a large owned struct, so the un-boxed `Err` variant
// trips `clippy::result_large_err`. The un-boxed return is the project's error
// contract, so accept the lint crate-wide.
#![allow(clippy::result_large_err)]

//! `backend-regex-core` — port of the PostgreSQL regex engine
//! (`src/backend/regex/*`: `regcomp.c`, `regexec.c`, `rege_dfa.c`,
//! `regprefix.c`, `regexport.c`, `regfree.c`, `regerror.c`, plus the
//! `regc_*.c` compile-time machinery).
//!
//! This is Henry Spencer's regex package as customized by PostgreSQL: a
//! compile front-end that turns a pattern (already `pg_mb2wchar`'d to
//! `pg_wchar`/`chr` code points) into a colormap + NFA + compacted NFA + a
//! subexpression tree, and a lazy-DFA matcher that executes it. The engine is
//! reached from SQL through [`types_regex::RegexCompiled`], which carries the
//! real compiled `regex_t` value type-erased; the ADT layer
//! (`backend-utils-adt-regexp`) compiles, caches, executes, and frees compiled
//! regexes via the four inward seams this unit owns
//! ([`backend_regex_core_seams`]).
//!
//! # Decomposition
//!
//! The unit (~4900 LOC) is split into six family modules:
//!
//!  * [`regex_foundation`] — `regguts` types, `regex_consts`, `regex_error`,
//!    the cvec utilities (`regc_cvec.c`) and the compile-time colormap engine
//!    (`regc_color.c`): subcolor/color allocation, the high-colormap
//!    range/row/column machinery, `subcolorcvec`. (`regguts`/`regex_consts`/
//!    `regex_error` are split into their own modules below so the other
//!    families can depend on the type contract.)
//!  * [`regex_nfa`] — the NFA graph (`regc_nfa.c`): arc/state arena primitives,
//!    sort/bulk ops, traversals, empty/constraint elimination, matchall
//!    analysis, optimize/analyze, NFA -> compacted-NFA build; plus the
//!    colormap<->arc bridge (`okcolors`/`colorchain`/`colorcomplement`/
//!    `rainbow`/`uncolorchain`).
//!  * [`regex_locale`] — `regc_locale.c` + `regc_pg_locale.c`: the
//!    `pg_wc_is*` probe family, `pg_set_regex_collation`,
//!    `pg_ctype_get_cache`, collating elements, equivalence/named classes.
//!    This is the locale-ctype boundary: the probes are owned here; the
//!    underlying `pg_locale_t`/ICU lookups route to the (unported)
//!    `backend-utils-adt-pg-locale` owner.
//!  * [`regex_compile`] — `regcomp.c`: the `struct vars` compile context, the
//!    `regc_lex.c` lexer, `scannum`, the recursive-descent parser
//!    (`parse`/`parsebranch`/`parseqatom`), the atom-emitter cluster, and the
//!    `pg_regcomp` orchestration.
//!  * [`regex_exec`] — `regexec.c` + `rege_dfa.c` (lazy-DFA matcher:
//!    `pg_regexec`, longest/shortest, miss, the dissectors, find/cfind) and
//!    `regprefix.c` (`pg_regprefix`/`findprefix`).
//!  * [`regex_export_free_error`] — `regexport.c` (the 11 `pg_reg_get*`
//!    NFA/color exporters), `regfree.c` (`pg_regfree`), `regerror.c`
//!    (`pg_regerror` message table), and the seam adapters that carry the
//!    compiled `regex_t` across the public seam (boxed type-erased into
//!    [`types_regex::RegexCompiled`], downcast back here).
//!
//! The C ground truth lives in `src/include/regex/{regex.h,regguts.h,
//! regcustom.h,regexport.h,regerrs.h}` and `src/backend/regex/*.c`.

extern crate alloc;

// Foundational type contract (depended on by every family).
pub mod regex_consts;
pub mod regex_error;
pub mod regguts;

// The six decomposition families.
pub mod regex_compile;
pub mod regex_exec;
pub mod regex_export_free_error;
pub mod regex_foundation;
pub mod regex_locale;
pub mod regex_nfa;

/// Install every seam this unit owns.
///
/// The unit owns `backend-regex-core-seams` (by C-source coverage of
/// `regcomp.c`/`regexec.c`/`regprefix.c`/`regfree.c`). Every declaration in it
/// is installed here, exactly once; the adapters live in
/// [`regex_export_free_error`] (they box the owned `RegexT` into the public
/// [`RegexCompiled`] carrier and downcast it back).
///
/// [`RegexCompiled`]: types_regex::RegexCompiled
pub fn init_seams() {
    use backend_regex_core_seams as seams;

    seams::pg_regcomp::set(regex_export_free_error::seam_pg_regcomp);
    seams::pg_regexec::set(regex_export_free_error::seam_pg_regexec);
    seams::pg_regprefix::set(regex_export_free_error::seam_pg_regprefix);
    seams::pg_regfree::set(regex_export_free_error::seam_pg_regfree);
}
