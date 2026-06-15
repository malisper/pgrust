//! Seam declarations for the resource-owner unit
//! (`utils/resowner/resowner.c`).
//!
//! Consumers:
//!
//! 1. portalcmds models the C `saveResourceOwner = CurrentResourceOwner;
//!    CurrentResourceOwner = portal->resowner; ...; CurrentResourceOwner =
//!    saveResourceOwner;` save/run/restore idiom as a single scoped callback
//!    (`with_current_resource_owner`). `CurrentResourceOwner` is *not* exposed
//!    as a save/restore global pair to portalcmds — that is the ambient-state
//!    anti-pattern the lifecycle model forbids (docs/query-lifecycle-raii.md;
//!    resowner dissolves into RAII/scoped capability).
//!
//! 2. logical decoding's slot-advance helper needs the raw get/set on
//!    `CurrentResourceOwner` (it saves/restores the executor's resource owner
//!    across decoding), so the bare global accessors are also exposed.
//!
//! 3. the PREPARE/EXECUTE EXPLAIN driver threads the current resource owner
//!    handle into `GetCachedPlan`/`ReleaseCachedPlan`, so a plain read of the
//!    current owner (as the parsestmt opaque handle) is exposed too.
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

#![allow(non_snake_case)]

use types_error::PgResult;
use types_logical::ResourceOwnerHandle;
use types_nodes::parsestmt::ResourceOwnerHandle as ParsestmtResourceOwnerHandle;
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

seam_core::seam!(
    /// `CurrentResourceOwner` (resowner.c global) — the backend's current
    /// resource owner, as the parsestmt opaque handle the PREPARE/EXECUTE
    /// driver threads into the plan-cache calls. Pure read of backend-local
    /// state.
    pub fn current_resource_owner() -> PgResult<ParsestmtResourceOwnerHandle>
);

seam_core::seam!(
    /// `CreateAuxProcessResourceOwner()` (resowner.c): create the aux-process
    /// resource owner and set `CurrentResourceOwner` to it, registering a
    /// cleanup callback. `Err` carries its `ereport` surface.
    pub fn create_aux_process_resource_owner() -> PgResult<()>
);

seam_core::seam!(
    /// `ReleaseAuxProcessResources(isCommit)` (resowner.c): release everything
    /// held by the aux-process resource owner (warning about leaked buffer
    /// pins). `Err` carries its `ereport` surface.
    pub fn release_aux_process_resources(is_commit: bool) -> PgResult<()>
);

seam_core::seam!(
    /// `CurrentResourceOwner = NULL` (resowner.c global): reset the current
    /// resource owner to nothing.
    pub fn reset_current_resource_owner()
);

seam_core::seam!(
    /// `CurrentResourceOwner = owner` (resowner.c global): restore the current
    /// resource owner. snapbuild.c's SnapBuildClearExportedSnapshot restores
    /// the owner saved before StartTransactionCommand (NULL handle == C NULL).
    pub fn set_current_resource_owner(owner: ParsestmtResourceOwnerHandle)
);
