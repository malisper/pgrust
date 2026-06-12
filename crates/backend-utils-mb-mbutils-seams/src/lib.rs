//! Seam declarations for the `backend-utils-mb-mbutils` unit
//! (`utils/mb/mbutils.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

seam_core::seam!(
    /// `pg_mbstrlen_with_len(mbstr, limit)` (mbutils.c): the number of
    /// characters (not bytes) in the first `limit` bytes of `mbstr`, in the
    /// current database encoding. Infallible.
    pub fn pg_mbstrlen_with_len(mbstr: &str, limit: i32) -> i32
);
