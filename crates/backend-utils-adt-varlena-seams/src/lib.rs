//! Seam declarations for the `backend-utils-adt-varlena` unit
//! (`utils/adt/varlena.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

use mcx::{Mcx, PgString, PgVec};
use types_error::PgResult;

seam_core::seam!(
    /// `cstring_to_text(s)` (varlena.c) — build a `text` varlena from a
    /// string, allocated in `mcx` (C: palloc in the caller's current
    /// context), returned as the `Datum` callers pass on (the
    /// `CStringGetTextDatum(s)` macro of builtins.h). OOM `ereport(ERROR)`
    /// carried on `Err`.
    pub fn cstring_to_text<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        s: &str,
    ) -> types_error::PgResult<types_datum::Datum>
);

seam_core::seam!(
    /// `SplitIdentifierString(rawstring, ',', &namelist)` (varlena.c) for the
    /// comma separator: parse a comma-separated list of identifiers,
    /// downcasing and dequoting per identifier rules. `Ok(None)` is the C
    /// `false` return (syntax error); the returned strings are the
    /// truncated/downcased names, allocated in `mcx` (C: pstrdup + List in
    /// the current context). `Err` carries OOM from the copies.
    pub fn split_identifier_string<'mcx>(
        mcx: Mcx<'mcx>,
        raw: &str,
    ) -> PgResult<Option<PgVec<'mcx, PgString<'mcx>>>>
);

seam_core::seam!(
    /// `text_substr(PG_FUNCTION_ARGS)` -> `text_substring(str, start, length,
    /// false)` (varlena.c) — the SQL `substring(string, start, length)`
    /// worker, on character positions. `regexp.c` reaches it via
    /// `DirectFunctionCall3(text_substr, ...)`. `str` is the `text` payload
    /// in the database encoding; the extracted substring payload is
    /// allocated in `mcx` (C: palloc in the caller's current context).
    /// `Err` carries `text_substring`'s "negative substring length not
    /// allowed" `ereport(ERROR)` and palloc OOM.
    pub fn text_substr<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        str: &[u8],
        start: i32,
        length: i32,
    ) -> types_error::PgResult<mcx::PgVec<'mcx, u8>>
);

seam_core::seam!(
    /// `replace_text_regexp(src_text, pattern_text, replace_text, cflags,
    /// collation, search_start, n)` (varlena.c) — replace match(es) of
    /// `pattern_text` in `src_text` with `replace_text` (which may contain
    /// `\1`-`\9` / `\&` references). `n = 0` replaces all matches, `n > 0`
    /// only the n'th; `search_start` is a character (not byte) offset. All
    /// three texts are payload bytes in the database encoding; the result
    /// payload is allocated in `mcx`. `Err` carries the regex-compile/
    /// execute `ereport(ERROR)`s and palloc OOM.
    pub fn replace_text_regexp<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        src_text: &[u8],
        pattern_text: &[u8],
        replace_text: &[u8],
        cflags: i32,
        collation: types_core::Oid,
        search_start: i32,
        n: i32,
    ) -> types_error::PgResult<mcx::PgVec<'mcx, u8>>
);
