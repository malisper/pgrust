//! Seam declarations for `storage/sync/sync.c`.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_error::PgResult;

seam_core::seam!(
    /// `InitSync()` (sync.c): initialize the backend-local sync request
    /// machinery (the pending-ops hashtable). `Err` carries its OOM surface.
    pub fn init_sync() -> PgResult<()>
);
