//! Seam declarations for the resource-owner unit
//! (`utils/resowner/resowner.c`).
//!
//! Two distinct consumers, two distinct seams:
//!
//! 1. portalcmds models the C `saveResourceOwner = CurrentResourceOwner;
//!    CurrentResourceOwner = portal->resowner; ...; CurrentResourceOwner =
//!    saveResourceOwner;` save/run/restore idiom as a single scoped callback
//!    (`with_current_resource_owner`). `CurrentResourceOwner` is *not* exposed
//!    as a save/restore global pair to portalcmds — that is the ambient-state
//!    anti-pattern the lifecycle model forbids (docs/query-lifecycle-raii.md;
//!    resowner dissolves into RAII/scoped capability).
//!
//! 2. logical decoding's slot-advance helper needs the raw
//!    get/set on `CurrentResourceOwner` (it saves/restores the executor's
//!    resource owner across decoding), so the bare global accessors are also
//!    exposed.
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

#![allow(non_snake_case)]

use types_error::PgResult;
use types_logical::ResourceOwnerHandle;
use types_portal::ResourceOwner;

seam_core::seam!(
    /// Run `f` with `owner` installed as the current resource owner, restoring
    /// the previous current owner afterwards (and on error). When `owner`
    /// is the C NULL (`ResourceOwner::is_null`), the current owner is left
    /// unchanged for the duration (mirrors `if (portal->resowner)
    /// CurrentResourceOwner = portal->resowner;`). `f`'s error propagates.
    pub fn with_current_resource_owner(
        owner: ResourceOwner,
        f: &mut dyn FnMut() -> PgResult<()>,
    ) -> PgResult<()>
);
seam_core::seam!(
    /// Read `CurrentResourceOwner`.
    pub fn CurrentResourceOwner() -> ResourceOwnerHandle
);
seam_core::seam!(
    /// `CurrentResourceOwner = value`.
    pub fn set_CurrentResourceOwner(value: ResourceOwnerHandle)
);
