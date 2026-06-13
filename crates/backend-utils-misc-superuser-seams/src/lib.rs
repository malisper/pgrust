//! Seam declarations for `utils/misc/superuser.c`.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_error::PgResult;

seam_core::seam!(
    /// `superuser()` (superuser.c): is the current session user a superuser?
    /// Consults `pg_authid` via the catcache, so it can `ereport(ERROR)`,
    /// carried on `Err`.
    pub fn superuser() -> PgResult<bool>
);
