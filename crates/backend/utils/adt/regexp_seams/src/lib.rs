//! Seam declarations for the `backend-utils-adt-regexp` unit
//! (`utils/adt/regexp.c`), for callers that would otherwise form a
//! dependency cycle: `utils/adt/varlena.c` (`replace_text_regexp` calls
//! `RE_compile_and_cache` while `regexp.c`'s replace functions call back
//! into varlena), `utils/adt/like_support.c` and `utils/adt/jsonpath_exec.c`
//! (`regexp_fixed_prefix` / `RE_compile_and_execute`).
//!
//! `backend-utils-adt-regexp` installs all of these from its
//! `init_seams()`.

#![allow(non_snake_case)]

use mcx::{Mcx, PgVec};
use types_core::Oid;
use types_error::PgResult;
use regex::{RegMatch, RegexCompiled};

seam_core::seam!(
    /// `RE_compile_and_cache(text_re, cflags, collation)` — compile a RE,
    /// using the backend's self-organizing compiled-RE cache. `pattern` is
    /// the `text` payload in the database encoding. C returns a `regex_t *`
    /// into the cache; here the compiled RE crosses as
    /// [`regex::RegexCompiled`] (the engine handle plus `re_nsub`).
    /// `mcx` is for the wide-character conversion scratch (C: palloc +
    /// pfree in the caller's current context). `Err` carries the "invalid
    /// regular expression" `ereport(ERROR)` and OOM.
    pub fn RE_compile_and_cache<'mcx>(
        mcx: Mcx<'mcx>,
        pattern: &[u8],
        cflags: i32,
        collation: Oid,
    ) -> PgResult<RegexCompiled>
);

seam_core::seam!(
    /// `RE_compile_and_execute(text_re, dat, dat_len, cflags, collation,
    /// nmatch, pmatch)` — compile (with caching) and execute a RE against
    /// `dat` (database encoding). `nmatch` is `pmatch.len()`; on a match the
    /// slots are filled in place. Returns true on match. `mcx` is for the
    /// wide-character conversion scratch.
    pub fn RE_compile_and_execute<'mcx>(
        mcx: Mcx<'mcx>,
        pattern: &[u8],
        dat: &[u8],
        cflags: i32,
        collation: Oid,
        pmatch: &mut [RegMatch],
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `regexp_fixed_prefix(text_re, case_insensitive, collation, &exact)` —
    /// extract the fixed prefix, if any, of a regexp. C returns NULL or a
    /// palloc'd string plus the `*exact` flag; here `None`, or
    /// `Some((prefix, exact))` with the prefix (database encoding) allocated
    /// in `mcx`.
    pub fn regexp_fixed_prefix<'mcx>(
        mcx: Mcx<'mcx>,
        text_re: &[u8],
        case_insensitive: bool,
        collation: Oid,
    ) -> PgResult<Option<(PgVec<'mcx, u8>, bool)>>
);
