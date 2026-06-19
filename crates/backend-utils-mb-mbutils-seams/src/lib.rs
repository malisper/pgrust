//! Seam declarations for the `backend-utils-mb-mbutils` unit
//! (`utils/mb/mbutils.c`): the client/server encoding-conversion dispatchers.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.
//!
//! In C both functions take `(const char *s, int len)` and return either the
//! *same pointer* (no conversion required — same encoding, or not in a
//! transaction) or a freshly palloc'd NUL-terminated converted string; callers
//! branch on pointer identity (`p != s`). Pointer identity does not cross a
//! seam, so the two outcomes are modelled as `Option`:
//!
//! * `Ok(None)` — no conversion was performed; the caller's bytes stand.
//! * `Ok(Some(v))` — conversion happened; `v` holds the converted bytes
//!   (without the trailing NUL; its length is C's `strlen(p)`), allocated in
//!   `mcx` (C allocates in `CurrentMemoryContext`). The caller frees it by
//!   dropping (C: `pfree(p)`).
//!
//! `Err` carries the conversion `ereport(ERROR)`s (untranslatable characters,
//! invalid byte sequences) and palloc out-of-memory.

use mcx::{Mcx, PgVec};
use types_core::PgWChar;
use types_error::PgResult;

seam_core::seam!(
    /// `pg_verifymbstr(mbstr, len, noError)` (`utils/mb/mbutils.c`): verify
    /// that `mbstr` is valid in the database encoding. With `no_error = false`
    /// an invalid byte sequence raises `ereport(ERROR)` (carried on `Err`);
    /// the seam returns whether the string is valid (the C `bool`).
    pub fn pg_verifymbstr(mbstr: &[u8], no_error: bool) -> PgResult<bool>
);

seam_core::seam!(
    /// `pg_server_to_client(s, len)` — convert from the server encoding to the
    /// current client encoding.
    pub fn pg_server_to_client<'mcx>(
        mcx: Mcx<'mcx>,
        s: &[u8],
    ) -> PgResult<Option<PgVec<'mcx, u8>>>
);

seam_core::seam!(
    /// `pg_client_to_server(s, len)` — convert from the current client
    /// encoding to the server encoding (verifying the bytes).
    pub fn pg_client_to_server<'mcx>(
        mcx: Mcx<'mcx>,
        s: &[u8],
    ) -> PgResult<Option<PgVec<'mcx, u8>>>
);

seam_core::seam!(
    /// `pg_mbstrlen_with_len(mbstr, limit)` (mbutils.c): the number of
    /// characters (not bytes) in the first `limit` bytes of `mbstr`, in the
    /// current database encoding. C takes `const char *` bytes in the
    /// database encoding (not necessarily UTF-8). Infallible.
    pub fn pg_mbstrlen_with_len(mbstr: &[u8], limit: i32) -> i32
);

seam_core::seam!(
    /// `pg_mbcliplen(mbstr, len, limit)` (mbutils.c): the byte length of the
    /// longest prefix of the first `len` bytes of `mbstr` that does not exceed
    /// `limit` bytes and does not split a multibyte character, in the current
    /// database encoding. C takes `const char *` bytes in the database
    /// encoding. Infallible.
    pub fn pg_mbcliplen(mbstr: &[u8], len: i32, limit: i32) -> i32
);

seam_core::seam!(
    /// `pg_mbcharcliplen(mbstr, len, limit)` (mbutils.c): like
    /// [`pg_mbcliplen`], except `limit` is measured in *characters* (not bytes)
    /// — the byte length of the longest prefix of the first `len` bytes of
    /// `mbstr` holding at most `limit` characters without splitting a multibyte
    /// character, in the current database encoding. C takes `const char *`
    /// bytes in the database encoding. Infallible.
    pub fn pg_mbcharcliplen(mbstr: &[u8], len: i32, limit: i32) -> i32
);

seam_core::seam!(
    /// `pg_mb2wchar_with_len(from, to, len)` (mbutils.c): convert the first
    /// `len` (here: all) bytes of a database-encoding string to `pg_wchar`
    /// code points. In C the caller pallocs the `(len + 1) * sizeof(pg_wchar)`
    /// output buffer in its current context and this fills it; here the seam
    /// allocates the converted code points (without the trailing NUL C
    /// stores) in `mcx`. OOM carried on `Err`.
    pub fn pg_mb2wchar_with_len<'mcx>(
        mcx: Mcx<'mcx>,
        from: &[u8],
    ) -> PgResult<PgVec<'mcx, PgWChar>>
);

seam_core::seam!(
    /// `pg_wchar2mb_with_len(from, to, len)` (mbutils.c): convert `pg_wchar`
    /// code points back to a database-encoding string. As above, C fills a
    /// caller-palloc'd buffer; here the encoded bytes (without the trailing
    /// NUL) are allocated in `mcx`. OOM carried on `Err`.
    pub fn pg_wchar2mb_with_len<'mcx>(
        mcx: Mcx<'mcx>,
        from: &[PgWChar],
    ) -> PgResult<PgVec<'mcx, u8>>
);

seam_core::seam!(
    /// `pg_mblen_range(mbstr, end)` (mbutils.c): byte length of the leading
    /// encoded character of `mbstr`, clamped so it never extends past `end`
    /// (here: the slice end). Infallible.
    pub fn pg_mblen_range(mbstr: &[u8]) -> i32
);

seam_core::seam!(
    /// `pg_database_encoding_max_length()` (mbutils.c): maximum bytes per
    /// character in the current database encoding. This is mbutils.c's own
    /// exported accessor for its per-backend `DatabaseEncoding` state (the C
    /// callers call exactly this function), not an invented getter for a
    /// foreign global; the owner installs it over its own `thread_local`.
    /// Infallible.
    pub fn pg_database_encoding_max_length() -> i32
);

seam_core::seam!(
    /// `GetDatabaseEncoding()` (mbutils.c): the database encoding id. Pure
    /// global read.
    pub fn get_database_encoding() -> i32
);

seam_core::seam!(
    /// `GetDatabaseEncodingName()` (mbutils.c): the database encoding's name
    /// — a pointer into the static `pg_enc2name` table in C, so no
    /// allocation: `&'static str` mirrors the static-table read.
    pub fn get_database_encoding_name() -> &'static str
);

seam_core::seam!(
    /// `is_encoding_supported_by_icu(encoding)` (mbutils.c): whether ICU
    /// collations work with the encoding. Pure table lookup.
    pub fn is_encoding_supported_by_icu(encoding: i32) -> bool
);

// --- backend-utils-init-postinit consumers (mbutils.c) ---

seam_core::seam!(
    /// `SetDatabaseEncoding(encoding)` (mbutils.c): set the server (database)
    /// encoding. `Err` carries its `ereport` surface.
    pub fn set_database_encoding(encoding: i32) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `InitializeClientEncoding()` (mbutils.c): finalize the client_encoding
    /// conversion setup. `Err` carries its `ereport` surface.
    pub fn initialize_client_encoding() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `pg_server_to_any(s, len, encoding)` (mbutils.c): convert from the
    /// server encoding to an arbitrary `encoding`. As with the client/server
    /// dispatchers, `Ok(None)` means no conversion happened (the caller's
    /// bytes stand); `Ok(Some(v))` carries the converted bytes (no trailing
    /// NUL) allocated in `mcx`. `Err` carries the conversion failure /
    /// out-of-memory `ereport(ERROR)`.
    pub fn pg_server_to_any<'mcx>(
        mcx: Mcx<'mcx>,
        s: &[u8],
        encoding: i32,
    ) -> PgResult<Option<PgVec<'mcx, u8>>>
);

seam_core::seam!(
    /// `pg_any_to_server(s, len, encoding)` (mbutils.c): convert from an
    /// arbitrary `encoding` to the server encoding, always validating (input
    /// comes from outside the database). As with the other dispatchers,
    /// `Ok(None)` means no conversion happened (the caller's bytes stand);
    /// `Ok(Some(v))` carries the converted bytes (no trailing NUL) allocated in
    /// `mcx`. `Err` carries the conversion failure / out-of-memory
    /// `ereport(ERROR)`.
    pub fn pg_any_to_server<'mcx>(
        mcx: Mcx<'mcx>,
        s: &[u8],
        encoding: i32,
    ) -> PgResult<Option<PgVec<'mcx, u8>>>
);

seam_core::seam!(
    /// `pg_get_client_encoding()` (mbutils.c): the current client encoding id.
    /// Pure global read.
    pub fn pg_get_client_encoding() -> i32
);

seam_core::seam!(
    /// `pg_encoding_mblen(encoding, mbstr)` (wchar.c): the byte length of the
    /// first character of `mbstr` (the remaining bytes from the current scan
    /// position) in the explicit `encoding`. Result is `>= 1`. Infallible.
    pub fn pg_encoding_mblen(encoding: i32, mbstr: &[u8]) -> i32
);

seam_core::seam!(
    /// `PG_ENCODING_IS_CLIENT_ONLY(encoding)` (`mb/pg_wchar.h`): whether the
    /// encoding may appear only on the client side (so ASCII can be a
    /// non-first byte of a multibyte char). Pure table predicate.
    pub fn pg_encoding_is_client_only(encoding: i32) -> bool
);

seam_core::seam!(
    /// `pg_unicode_to_server(pg_wchar c, unsigned char *s)` (mbutils.c) —
    /// convert the single Unicode code point `c` into the current server
    /// encoding, returning the encoded bytes (no trailing NUL) allocated in
    /// `mcx` (C writes into a caller `[MAX_UNICODE_EQUIVALENT_STRING + 1]`
    /// buffer; here the carrier is the encoded byte run). `Err` carries the
    /// "invalid Unicode code point" / "character ... has no equivalent in
    /// encoding" `ereport(ERROR)`s and palloc OOM.
    pub fn pg_unicode_to_server<'mcx>(
        mcx: Mcx<'mcx>,
        c: PgWChar,
    ) -> PgResult<PgVec<'mcx, u8>>
);

seam_core::seam!(
    /// `report_invalid_encoding(encoding, mbstr, len)` (mbutils.c): the
    /// `pg_noreturn` reporter that raises `ERRCODE_CHARACTER_NOT_IN_REPERTOIRE`
    /// ("invalid byte sequence for encoding ..."). `mbstr` is the remaining
    /// input from the offending position (C derives the bad-char length and
    /// hex dump itself). Always returns `Err`.
    pub fn report_invalid_encoding(encoding: i32, mbstr: &[u8]) -> PgResult<()>
);

seam_core::seam!(
    /// `report_untranslatable_char(src_encoding, dest_encoding, mbstr, len)`
    /// (mbutils.c): the `pg_noreturn` reporter that raises
    /// `ERRCODE_UNTRANSLATABLE_CHARACTER` ("character ... has no equivalent in
    /// encoding ..."). `mbstr` is the remaining input from the offending
    /// character. Always returns `Err`.
    pub fn report_untranslatable_char(
        src_encoding: i32,
        dest_encoding: i32,
        mbstr: &[u8],
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `FindDefaultConversionProc(for_encoding, to_encoding)`
    /// (`catalog/namespace.c`): return the OID of the default conversion
    /// procedure between the two encodings (searching the active search path),
    /// or `InvalidOid` if none exists. Mirrors the C signature; the seam carries
    /// no `Mcx` (the owner runs the catalog lookup in a scratch context).
    pub fn find_default_conversion_proc(
        for_encoding: i32,
        to_encoding: i32,
    ) -> PgResult<types_core::primitive::Oid>
);

seam_core::seam!(
    /// `pg_do_encoding_conversion_buf(proc, src_encoding, dest_encoding, src,
    /// srclen, dest, dstlen, noError)` (`utils/mb/mbutils.c`): convert `src`
    /// from `src_encoding` to `dest_encoding` using the already-resolved
    /// conversion `proc`, limiting the input so the worst-case output fits a
    /// destination buffer of capacity `dst_capacity`. Returns the number of
    /// source bytes consumed (C's `convertedlen`, 0 if nothing could be
    /// converted) together with the converted output bytes (excluding the
    /// trailing NUL), allocated in `mcx`. With `no_error = true` an invalid
    /// byte sequence stops the conversion short (returns what converted); with
    /// `no_error = false` the conversion procedure raises (carried on `Err`).
    pub fn pg_do_encoding_conversion_buf<'mcx>(
        mcx: Mcx<'mcx>,
        proc: types_core::primitive::Oid,
        src_encoding: i32,
        dest_encoding: i32,
        src: &[u8],
        dst_capacity: i32,
        no_error: bool,
    ) -> PgResult<(i32, PgVec<'mcx, u8>)>
);

seam_core::seam!(
    /// `check_encoding_conversion_args(src_encoding, dest_encoding, len,
    /// expected_src_encoding, expected_dest_encoding)` (mbutils.c): validate the
    /// source/destination encoding ids and the length argument passed to a
    /// conversion procedure (the `CHECK_ENCODING_CONVERSION_ARGS` macro expands
    /// to a call to this). `expected_src_encoding`/`expected_dest_encoding` may
    /// be `-1` to mean "any valid encoding accepted". Raises `elog(ERROR)` on a
    /// bad encoding id, an encoding mismatch, or a negative length (carried on
    /// `Err`); returns `Ok(())` when the arguments are valid.
    pub fn check_encoding_conversion_args(
        src_encoding: i32,
        dest_encoding: i32,
        len: i32,
        expected_src_encoding: i32,
        expected_dest_encoding: i32,
    ) -> PgResult<()>
);
