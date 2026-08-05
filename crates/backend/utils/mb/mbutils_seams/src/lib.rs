use mcx::{Mcx, PgVec};
use types_core::PgWChar;
use types_error::PgResult;

seam_core::seam!(
    // pg_mb2wchar_with_len(from,to,len) (mbutils.c): database-encoding bytes ->
    // pg_wchar code points, allocated in mcx (no trailing NUL). Used by the
    // regex ADT layer before pg_regcomp/pg_regexec.
    pub fn pg_mb2wchar_with_len<'mcx>(
        mcx: Mcx<'mcx>,
        from: &[u8],
    ) -> PgResult<PgVec<'mcx, PgWChar>>
);

seam_core::seam!(
    // pg_wchar2mb_with_len(from,to,len) (mbutils.c): pg_wchar code points ->
    // database-encoding bytes in mcx (no trailing NUL).
    pub fn pg_wchar2mb_with_len<'mcx>(
        mcx: Mcx<'mcx>,
        from: &[PgWChar],
    ) -> PgResult<PgVec<'mcx, u8>>
);

seam_core::seam!(
    // pg_encoding_mblen(encoding, mbstr) (wchar.c): byte length of the first
    // char of mbstr in the given encoding. Result >= 1. Infallible.
    pub fn pg_encoding_mblen(encoding: i32, mbstr: &[u8]) -> i32
);

// C signals "no conversion required" by returning the caller's pointer;
// identity does not cross a seam: Ok(None) = caller's bytes stand,
// Ok(Some(v)) = converted bytes (len = C's strlen(p), no trailing NUL) in mcx.

seam_core::seam!(
    pub fn pg_server_to_client<'mcx>(
        mcx: Mcx<'mcx>,
        s: &[u8],
    ) -> PgResult<Option<PgVec<'mcx, u8>>>
);

seam_core::seam!(
    // pg_server_to_client's encoding test (ClientEncoding == ServerEncoding
    // or PG_SQL_ASCII => identity), hoistable to resolve-once carriers so
    // per-row output skips the conversion seam entirely (strategy lever 2).
    pub fn server_to_client_conversion_needed() -> bool
);

seam_core::seam!(
    // C: pg_database_encoding_max_length() (mbutils.c).
    pub fn pg_database_encoding_max_length() -> i32
);

seam_core::seam!(
    // C: pg_mbstrlen_with_len(mbstr, limit) — the slice carries the limit.
    // Errors like C when the trailing character's claimed length overruns
    // the slice.
    pub fn pg_mbstrlen_with_len(s: &[u8]) -> PgResult<i32>
);

seam_core::seam!(
    // C: pg_mblen_range(mbstr, end) — the slice carries the end bound.
    pub fn pg_mblen_range(s: &[u8]) -> PgResult<i32>
);

seam_core::seam!(
    // C: GetDatabaseEncoding() (mbutils.c); the i32 is a pg_enc.
    pub fn get_database_encoding() -> i32
);

seam_core::seam!(
    // C: GetDatabaseEncodingName() (mbutils.c).
    pub fn get_database_encoding_name() -> &'static str
);

seam_core::seam!(
    // SetDatabaseEncoding(encoding) (mbutils.c); ERROR on non-server encoding.
    pub fn set_database_encoding(encoding: i32) -> PgResult<()>
);

seam_core::seam!(
    // InitializeClientEncoding (mbutils.c).
    pub fn initialize_client_encoding() -> PgResult<()>
);

seam_core::seam!(
    // pg_mbcliplen(mbstr, len, limit) (mbutils.c): byte length of the longest
    // encoded-character-boundary prefix <= limit bytes; no ereport.
    pub fn pg_mbcliplen(mbstr: &[u8], len: i32, limit: i32) -> i32
);
