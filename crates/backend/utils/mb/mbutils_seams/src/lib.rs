use mcx::{Mcx, PgVec};
use types_error::PgResult;

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
    pub fn pg_client_to_server<'mcx>(
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
    pub fn pg_mbstrlen_with_len(s: &[u8]) -> i32
);

seam_core::seam!(
    // C: GetDatabaseEncoding() (mbutils.c); the i32 is a pg_enc.
    pub fn get_database_encoding() -> i32
);

seam_core::seam!(
    // C: GetDatabaseEncodingName() (mbutils.c).
    pub fn get_database_encoding_name() -> &'static str
);
