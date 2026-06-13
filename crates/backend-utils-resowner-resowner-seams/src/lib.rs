//! Seam declarations for the `backend-utils-resowner-resowner` unit
//! (`utils/resowner/resowner.c`).
//!
//! The query-lifecycle model (docs/query-lifecycle-raii.md) replaces the
//! ambient `CurrentResourceOwner` with owner values, but until that owner
//! lands the EXPLAIN-EXECUTE driver threads the current resource owner handle
//! into `GetCachedPlan`/`ReleaseCachedPlan`. The owning unit installs these
//! from its `init_seams()` when it lands; until then a call panics loudly.

use types_error::PgResult;
use types_nodes::parsestmt::ResourceOwnerHandle;

seam_core::seam!(
    /// `CurrentResourceOwner` (resowner.c global) — the backend's current
    /// resource owner. Pure read of backend-local state.
    pub fn current_resource_owner() -> PgResult<ResourceOwnerHandle>
);
