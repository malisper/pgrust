//! Seam declarations for reading `pg_authid` during backend startup
//! (postinit.c `ThereIsAtLeastOneRole`).
//!
//! The owning catalog-read unit installs these from its `init_seams()` when it
//! lands; until then a call panics loudly.

use mcx::Mcx;
use types_error::PgResult;

seam_core::seam!(
    /// `ThereIsAtLeastOneRole()` (postinit.c): table_open(AuthIdRelationId,
    /// AccessShareLock) + table_beginscan_catalog + heap_getnext(Forward) !=
    /// NULL + table_endscan + table_close. The relcache/heap-scan machinery is
    /// owned by the catalog-read unit; returns whether any role row exists.
    /// `Err` carries the scan/catalog-open `ereport(ERROR)` surface.
    pub fn there_is_at_least_one_role(mcx: Mcx<'_>) -> PgResult<bool>
);
