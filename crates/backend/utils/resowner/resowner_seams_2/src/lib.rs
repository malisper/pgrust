//! Seam declarations for `utils/resowner/resowner.c` functions.
//!
//! Resource owners dissolve into RAII owner values (docs/query-lifecycle-raii.md);
//! until that owner lands, callers thread the C `ResourceOwner` pointer as the
//! shared [`ResourceOwner`] handle (`portal`, the same value
//! `portal->resowner` holds). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

use portal::{ResourceOwner, ResourceReleasePhase};

seam_core::seam!(
    /// `ResourceOwnerCreate(parent, name)` for a portal — returns the new
    /// owner. portalmem always passes `CurrentResourceOwner` as the parent and
    /// the name "Portal", so the seam takes no arguments.
    pub fn resource_owner_create_portal() -> ResourceOwner
);

seam_core::seam!(
    /// `ResourceOwnerRelease(owner, phase, isCommit, isTopLevel)`.
    pub fn resource_owner_release(
        owner: ResourceOwner,
        phase: ResourceReleasePhase,
        is_commit: bool,
        is_top_level: bool,
    )
);

seam_core::seam!(
    /// `ResourceOwnerDelete(owner)`.
    pub fn resource_owner_delete(owner: ResourceOwner)
);

seam_core::seam!(
    /// `ResourceOwnerNewParent(owner, newparent)`.
    pub fn resource_owner_new_parent(owner: ResourceOwner, new_parent: ResourceOwner)
);

seam_core::seam!(
    /// `ReleaseAuxProcessResources(isCommit)` (resowner.c) — release all
    /// resources held by `AuxProcessResourceOwner`. Called from auxiliary
    /// processes' error-recovery cleanup with `isCommit = false`. `Err`
    /// carries any `ereport` from a release callback.
    pub fn release_aux_process_resources(is_commit: bool) -> types_error::PgResult<()>
);

// --- resowner.c heavyweight-lock bookkeeping (lock.c consumers) ------------
//
// lock.c's LOCALLOCK grant/release paths call ResourceOwnerRememberLock /
// ResourceOwnerForgetLock to keep each ResourceOwner's lock array in sync, and
// ResourceOwnerGetParent during LockReassignCurrentOwner. resowner.c stores the
// LOCALLOCK by pointer; in the handle model lock.c passes the LOCALLOCK's stable
// hash-table key (its LOCALLOCKTAG) as the lock identity. The owner is the
// heavyweight-lock subsystem's `ResourceOwnerHandle` (`types_storage::lock`).
// resowner.c is unported, so these panic until it lands.

seam_core::seam!(
    /// `ResourceOwnerRememberLock(owner, locallock)` (resowner.c) — record that
    /// `owner` holds the heavyweight lock identified by `lock` (the LOCALLOCK's
    /// LOCALLOCKTAG key).
    pub fn resource_owner_remember_lock(
        owner: types_storage::lock::ResourceOwnerHandle,
        lock: types_storage::lock::LOCALLOCKTAG,
    )
);

seam_core::seam!(
    /// `ResourceOwnerForgetLock(owner, locallock)` (resowner.c) — drop the
    /// heavyweight lock identified by `lock` from `owner`'s lock array.
    pub fn resource_owner_forget_lock(
        owner: types_storage::lock::ResourceOwnerHandle,
        lock: types_storage::lock::LOCALLOCKTAG,
    )
);

seam_core::seam!(
    /// `ResourceOwnerGetParent(owner)` (resowner.c) — the parent resource
    /// owner of `owner`.
    pub fn resource_owner_get_parent(
        owner: types_storage::lock::ResourceOwnerHandle,
    ) -> types_storage::lock::ResourceOwnerHandle
);

seam_core::seam!(
    /// `CurrentResourceOwner` (resowner.c global) — the current resource owner,
    /// as the heavyweight-lock subsystem's `ResourceOwnerHandle`, or `None`
    /// when `CurrentResourceOwner == NULL`.
    pub fn lock_current_resource_owner() -> Option<types_storage::lock::ResourceOwnerHandle>
);
