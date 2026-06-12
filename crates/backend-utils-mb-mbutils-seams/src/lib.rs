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
    /// current database encoding. Infallible.
    pub fn pg_mbstrlen_with_len(mbstr: &str, limit: i32) -> i32
);
