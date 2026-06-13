//! Seam declarations for `access/common/session.c`.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_error::PgResult;

seam_core::seam!(
    /// `InitializeSession()` (session.c): initialize this backend's session
    /// state (the per-session DSM/typmod registry). `Err` carries its
    /// `ereport` surface.
    pub fn initialize_session() -> PgResult<()>
);
