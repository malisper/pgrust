//! Seam declarations for the `backend-utils-adt-varlena` unit
//! (`utils/adt/varlena.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

use mcx::{Mcx, PgString, PgVec};
use types_error::PgResult;
// The canonical unified value type (Datum-unification keystone). The `*_v` seam
// variants below take/return it; the bare-word `types_datum::Datum` variants are
// transitional shims kept until every consumer migrates (removed in Cleanup).
use types_tuple::backend_access_common_heaptuple::Datum as DatumV;

seam_core::seam!(
    /// `int varstr_cmp(const char *arg1, int len1, const char *arg2, int len2,
    /// Oid collid)` (varlena.c) — collation-aware 3-way comparison of two
    /// strings. `arg1`/`arg2` are the string payload bytes in the database
    /// encoding. Reached from `jsonb_util.c`'s `compareJsonbScalarValue`
    /// `jbvString` arm with `collid = DEFAULT_COLLATION_OID` (B-tree operator
    /// support only, off the `jsonb_in`/`jsonb_out` I/O path). The C-collation
    /// fast path is a byte compare; non-C collations delegate to the locale
    /// providers. `Err` carries the locale-comparison `ereport(ERROR)`.
    pub fn varstr_cmp(arg1: &[u8], arg2: &[u8], collid: types_core::Oid) -> PgResult<i32>
);

seam_core::seam!(
    /// `textToQualifiedNameList(textval)` (varlena.c): split a (possibly
    /// qualified) name `text` on `.` into its identifier parts, downcasing
    /// and dequoting per `SplitIdentifierString`. `textval` is the `text`
    /// payload bytes (database encoding). Invalid name syntax (or an empty
    /// list) raises `ERRCODE_INVALID_NAME` (`Err`); the returned parts are
    /// `pstrdup`'d into `mcx`. `Err` includes OOM.
    pub fn text_to_qualified_name_list<'mcx>(
        mcx: Mcx<'mcx>,
        textval: &[u8],
    ) -> PgResult<PgVec<'mcx, PgString<'mcx>>>
);

seam_core::seam!(
    /// `cstring_to_text(s)` (varlena.c) — build a `text` varlena from a
    /// string, allocated in `mcx` (C: palloc in the caller's current
    /// context), returned as the `Datum` callers pass on (the
    /// `CStringGetTextDatum(s)` macro of builtins.h). OOM `ereport(ERROR)`
    /// carried on `Err`.
    ///
    /// TRANSITIONAL SHIM: superseded by [`cstring_to_text_v`], which returns the
    /// unified `types_tuple::Datum` value. Kept until callers migrate.
    pub fn cstring_to_text<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        s: &str,
    ) -> types_error::PgResult<types_datum::Datum>
);

seam_core::seam!(
    /// `cstring_to_text(s)` (varlena.c) over the unified value type — the
    /// migration-target form of [`cstring_to_text`]. The `text` varlena is
    /// always pass-by-reference, so the result is a `Datum::ByRef` holding the
    /// freshly built varlena bytes (header + payload) allocated in `mcx`. OOM
    /// `ereport(ERROR)` carried on `Err`.
    pub fn cstring_to_text_v<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        s: &str,
    ) -> types_error::PgResult<DatumV<'mcx>>
);

seam_core::seam!(
    /// Wrap raw bytes into a `bytea`/`text` varlena `Datum` — the genfile.c
    /// `read_binary_file` idiom of `palloc(len + VARHDRSZ)` + memcpy +
    /// `SET_VARSIZE(buf, len + VARHDRSZ)`. The bytes are copied into `mcx`. For
    /// `text` results the caller has already run `pg_verifymbstr`; the
    /// representation is identical. OOM `ereport(ERROR)` carried on `Err`.
    ///
    /// TRANSITIONAL SHIM: superseded by [`bytes_to_varlena_v`], which returns the
    /// unified `types_tuple::Datum` value. Kept until callers migrate.
    pub fn bytes_to_varlena<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        bytes: &[u8],
    ) -> types_error::PgResult<types_datum::Datum>
);

seam_core::seam!(
    /// Wrap raw bytes into a `bytea`/`text` varlena over the unified value type —
    /// the migration-target form of [`bytes_to_varlena`]. The varlena is
    /// pass-by-reference, so the result is a `Datum::ByRef` holding the built
    /// varlena bytes (`VARHDRSZ` header + payload) copied into `mcx`. OOM
    /// `ereport(ERROR)` carried on `Err`.
    pub fn bytes_to_varlena_v<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        bytes: &[u8],
    ) -> types_error::PgResult<DatumV<'mcx>>
);

seam_core::seam!(
    /// `bool SplitIdentifierString(char *rawstring, char separator,
    /// List **namelist)` (varlena.c) — parse a `separator`-separated list of
    /// identifiers, downcasing and dequoting per identifier rules. `Ok(None)`
    /// is the C `false` return (syntax error); the returned strings are the
    /// truncated/downcased names, allocated in `mcx` (C: pstrdup + List in
    /// the current context). `Err` carries OOM from the copies.
    pub fn split_identifier_string<'mcx>(
        mcx: Mcx<'mcx>,
        raw: &str,
        separator: char,
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

seam_core::seam!(
    /// `SplitDirectoriesString(rawstring, ',', &elemlist)` (varlena.c) — split
    /// a comma-separated, possibly-quoted directory list into canonicalized
    /// path elements, each allocated in `mcx`. `Ok(None)` is the C `false`
    /// return (syntax error); `Ok(Some(list))` carries the parsed elements.
    pub fn split_directories_string<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        rawstring: &str,
    ) -> types_error::PgResult<Option<mcx::PgVec<'mcx, mcx::PgString<'mcx>>>>
);

seam_core::seam!(
    /// `text_to_cstring(t)` (varlena.c), reached via the `TextDatumGetCString(d)`
    /// macro (`text_to_cstring((text *) DatumGetPointer(d))`): detoast the
    /// `text` varlena `d` points at and copy its payload out as a NUL-free
    /// `String` in `mcx` (C: palloc in the caller's current context). `Err`
    /// carries detoast/OOM `ereport(ERROR)`.
    ///
    /// TRANSITIONAL SHIM: superseded by [`text_to_cstring_v`], which takes the
    /// unified `types_tuple::Datum` value. Kept until callers migrate.
    pub fn text_to_cstring<'mcx>(
        mcx: Mcx<'mcx>,
        d: types_datum::Datum,
    ) -> PgResult<PgString<'mcx>>
);

seam_core::seam!(
    /// `text_to_cstring(t)` (varlena.c) over the unified value type — the
    /// migration-target form of [`text_to_cstring`]. The `text` argument is a
    /// pass-by-reference value, so `d` is a `Datum::ByRef` whose bytes are the
    /// `text` varlena (`TextDatumGetCString` detoasts then copies the payload
    /// out as a NUL-free `String` in `mcx`). `Err` carries detoast/OOM
    /// `ereport(ERROR)`.
    pub fn text_to_cstring_v<'mcx>(
        mcx: Mcx<'mcx>,
        d: &DatumV<'_>,
    ) -> PgResult<PgString<'mcx>>
);
