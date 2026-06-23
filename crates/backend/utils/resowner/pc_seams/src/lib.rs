//! plancache's slice of resource-owner plan-ref bookkeeping
//! (`utils/resowner/resowner.c`).
//!
//! In this repo the resource-owner registry dissolves into RAII guards +
//! owner values (`docs/query-lifecycle-raii.md`); there is no faithful
//! `resowner.c` port. plancache's `CachedPlan` refs will become a guard whose
//! `Drop` calls `ReleaseCachedPlan(plan, NULL)`. Until that guard layer exists
//! (the bufmgr-pin forcing case builds it), the C `ResourceOwner` plan-ref
//! operations are mirrored as seams the resowner-equivalent owner installs;
//! a call panics loudly until then. The `plan` argument is the plancache
//! crate's own `CachedPlan` registry id (an internal `u64`).

extern crate alloc;
use alloc::vec::Vec;

use ::types_error::PgResult;
use ::types_plancache::ResourceOwnerHandle;

seam_core::seam!(
    /// `ResourceOwnerEnlarge(owner)`.
    pub fn resource_owner_enlarge(owner: ResourceOwnerHandle) -> PgResult<()>
);

seam_core::seam!(
    /// `ResourceOwnerRememberPlanCacheRef(owner, plan)`.
    pub fn resource_owner_remember_plan(owner: ResourceOwnerHandle, plan: u64) -> PgResult<()>
);

seam_core::seam!(
    /// `ResourceOwnerForgetPlanCacheRef(owner, plan)`.
    pub fn resource_owner_forget_plan(owner: ResourceOwnerHandle, plan: u64) -> PgResult<()>
);

seam_core::seam!(
    /// `ResourceOwnerReleaseAllOfKind(owner, &planref_resowner_desc)` —
    /// returns the plancache `CachedPlan` ids the owner still held, so
    /// plancache re-enters the in-crate `ReleaseCachedPlan(plan, NULL)` for
    /// each (mirrors the C resowner release callback).
    pub fn resource_owner_release_all_plan_refs(owner: ResourceOwnerHandle) -> PgResult<Vec<u64>>
);
