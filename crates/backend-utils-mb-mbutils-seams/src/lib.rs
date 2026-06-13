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
