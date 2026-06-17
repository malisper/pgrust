//! Installation of every seam this crate owns. `init_seams()` contains only
//! `set()` calls; each closure marshals at the boundary and delegates to the
//! real `resowner.c` routine in `lib.rs`.

use types_datum::Datum;
use types_error::PgResult;
use types_resowner::{
    ResourceOwner, RESOURCE_RELEASE_AFTER_LOCKS, RESOURCE_RELEASE_BEFORE_LOCKS,
    RESOURCE_RELEASE_LOCKS,
};
use types_storage::lock::LOCALLOCKTAG;

use crate::{
    AuxProcessResourceOwner, CreateAuxProcessResourceOwner, CurrentResourceOwner,
    ReleaseAuxProcessResources, ResourceOwnerCreate, ResourceOwnerDelete, ResourceOwnerEnlarge,
    ResourceOwnerForget, ResourceOwnerForgetLock, ResourceOwnerGetParent, ResourceOwnerNewParent,
    ResourceOwnerReleaseAllOfKind, ResourceOwnerRemember, ResourceOwnerRememberLock,
    SetCurrentResourceOwner,
};

mod rr {
    pub use backend_utils_resowner_resowner_seams::*;
}
mod rs {
    pub use backend_utils_resowner_seams::*;
}
mod ra {
    pub use backend_utils_resowner_all_seams::*;
}
mod rp {
    pub use backend_utils_resowner_pc_seams::*;
}
mod bm {
    pub use backend_storage_buffer_bufmgr_seams::*;
}

/// `Option<ResourceOwner>` → the seam carrier (`ResourceOwner::NULL` for None).
fn flat(o: Option<ResourceOwner>) -> ResourceOwner {
    o.unwrap_or(ResourceOwner::NULL)
}

/// The seam carrier → `Option<ResourceOwner>` (None for `NULL`).
fn opt(o: ResourceOwner) -> Option<ResourceOwner> {
    if o.is_null() {
        None
    } else {
        Some(o)
    }
}

/// `utils/portal.h` `ResourceReleasePhase` enum (0/1/2) → resowner.h phase
/// value (1/2/3).
fn phase_from_portal(p: types_portal::ResourceReleasePhase) -> u32 {
    match p {
        types_portal::ResourceReleasePhase::BeforeLocks => RESOURCE_RELEASE_BEFORE_LOCKS,
        types_portal::ResourceReleasePhase::Locks => RESOURCE_RELEASE_LOCKS,
        types_portal::ResourceReleasePhase::AfterLocks => RESOURCE_RELEASE_AFTER_LOCKS,
    }
}

// ===========================================================================
// Buffer-pin / buffer-IO ResourceOwnerDesc (defined in bufmgr.c; the release
// callbacks delegate to bufmgr through the buffer-release seams).
// ===========================================================================

fn release_buffer_pin(res: Datum) {
    let _ = bm::release_buffer_pin::call(res.as_i32());
}

fn release_buffer_io(res: Datum) {
    let _ = bm::release_buffer_io::call(res.as_i32());
}

static BUFFER_PIN_DESC: types_resowner::ResourceOwnerDesc = types_resowner::ResourceOwnerDesc {
    name: None, // "buffer pin" — printed via DebugPrint when present
    release_phase: RESOURCE_RELEASE_BEFORE_LOCKS,
    release_priority: types_resowner::RELEASE_PRIO_BUFFER_PINS,
    ReleaseResource: Some(release_buffer_pin),
    DebugPrint: None,
};

static BUFFER_IO_DESC: types_resowner::ResourceOwnerDesc = types_resowner::ResourceOwnerDesc {
    name: None, // "buffer io"
    release_phase: RESOURCE_RELEASE_BEFORE_LOCKS,
    release_priority: types_resowner::RELEASE_PRIO_BUFFER_IOS,
    ReleaseResource: Some(release_buffer_io),
    DebugPrint: None,
};

/// Get the current resource owner, erroring if there is none (the bufmgr
/// remember/forget seams require `CurrentResourceOwner != NULL`).
fn current_or_err() -> PgResult<ResourceOwner> {
    CurrentResourceOwner().ok_or_else(|| types_error::PgError::error("CurrentResourceOwner is NULL"))
}

pub fn install() {
    // --- resowner-resowner-seams -------------------------------------------
    rr::with_current_resource_owner::set(|owner, f| {
        // if (owner) CurrentResourceOwner = owner; run f; restore
        if owner.is_null() {
            return f();
        }
        let save = CurrentResourceOwner();
        SetCurrentResourceOwner(Some(owner));
        let r = f();
        SetCurrentResourceOwner(save);
        r
    });

    rr::CurrentResourceOwner::set(|| flat(CurrentResourceOwner()));

    rr::set_CurrentResourceOwner::set(|value| SetCurrentResourceOwner(opt(value)));

    rr::current_resource_owner::set(|| Ok(flat(CurrentResourceOwner())));

    rr::create_aux_process_resource_owner::set(CreateAuxProcessResourceOwner);

    rr::release_aux_process_resources::set(ReleaseAuxProcessResources);

    rr::reset_current_resource_owner::set(|| SetCurrentResourceOwner(None));

    rr::set_current_resource_owner::set(|owner| SetCurrentResourceOwner(opt(owner)));

    // --- resowner-seams ----------------------------------------------------
    rs::resource_owner_create_portal::set(|| {
        // portalmem always passes CurrentResourceOwner as the parent, name "Portal".
        let parent = CurrentResourceOwner();
        ResourceOwnerCreate(parent, "Portal").expect("ResourceOwnerCreate(Portal) out of memory")
    });

    rs::resource_owner_release::set(|owner, phase, is_commit, is_top_level| {
        let _ = crate::ResourceOwnerRelease(owner, phase_from_portal(phase), is_commit, is_top_level);
    });

    rs::resource_owner_delete::set(|owner| {
        let _ = ResourceOwnerDelete(owner);
    });

    rs::resource_owner_new_parent::set(|owner, new_parent| {
        let _ = ResourceOwnerNewParent(owner, opt(new_parent));
    });

    rs::release_aux_process_resources::set(ReleaseAuxProcessResources);

    rs::resource_owner_remember_lock::set(|owner: ResourceOwner, lock: LOCALLOCKTAG| {
        ResourceOwnerRememberLock(owner, lock);
    });

    rs::resource_owner_forget_lock::set(|owner: ResourceOwner, lock: LOCALLOCKTAG| {
        let _ = ResourceOwnerForgetLock(owner, lock);
    });

    rs::resource_owner_get_parent::set(|owner: ResourceOwner| flat(ResourceOwnerGetParent(owner)));

    rs::lock_current_resource_owner::set(CurrentResourceOwner);

    // --- resowner-all-seams ------------------------------------------------
    ra::release_aux_process_resources::set(|is_commit| {
        let _ = ReleaseAuxProcessResources(is_commit);
    });

    // --- resowner-pc-seams (plancache plan refs) ---------------------------
    rp::resource_owner_enlarge::set(|owner| ResourceOwnerEnlarge(owner));

    rp::resource_owner_remember_plan::set(|owner, plan| {
        ResourceOwnerRemember(owner, Datum::from_usize(plan as usize), &PLANCACHE_DESC)
    });

    rp::resource_owner_forget_plan::set(|owner, plan| {
        ResourceOwnerForget(owner, Datum::from_usize(plan as usize), &PLANCACHE_DESC)
    });

    rp::resource_owner_release_all_plan_refs::set(|owner| {
        // Collect the plan ids the owner still holds, then release them so the
        // plancache crate re-enters ReleaseCachedPlan(plan, NULL) for each.
        let ids = collect_plan_refs(owner);
        ResourceOwnerReleaseAllOfKind(owner, &PLANCACHE_DESC)?;
        Ok(ids)
    });

    // --- bufmgr-seams (buffer pin / IO bookkeeping) ------------------------
    bm::remember_buffer::set(|buffer| {
        let owner = current_or_err().expect("remember_buffer: CurrentResourceOwner is NULL");
        ResourceOwnerRemember(owner, Datum::from_i32(buffer), &BUFFER_PIN_DESC)
            .expect("ResourceOwnerRememberBuffer");
    });

    bm::forget_buffer::set(|buffer| {
        let owner = current_or_err().expect("forget_buffer: CurrentResourceOwner is NULL");
        ResourceOwnerForget(owner, Datum::from_i32(buffer), &BUFFER_PIN_DESC)
            .expect("ResourceOwnerForgetBuffer");
    });

    bm::resowner_enlarge::set(|| {
        let owner = current_or_err()?;
        ResourceOwnerEnlarge(owner)
    });

    bm::remember_buffer_io::set(|buffer| {
        let owner = current_or_err().expect("remember_buffer_io: CurrentResourceOwner is NULL");
        ResourceOwnerRemember(owner, Datum::from_i32(buffer), &BUFFER_IO_DESC)
            .expect("ResourceOwnerRememberBufferIO");
    });

    bm::forget_buffer_io::set(|buffer| {
        let owner = current_or_err().expect("forget_buffer_io: CurrentResourceOwner is NULL");
        ResourceOwnerForget(owner, Datum::from_i32(buffer), &BUFFER_IO_DESC)
            .expect("ResourceOwnerForgetBufferIO");
    });

    let _ = AuxProcessResourceOwner; // referenced for clarity; silence unused
}

/// `planref_resowner_desc` (plancache.c) — `ResourceOwnerReleaseAll` invokes its
/// `ReleaseResource` for each leaked plan ref. The plancache crate re-enters its
/// own `ReleaseCachedPlan` through `resource_owner_release_all_plan_refs`'s
/// returned list, so the desc callback here is a no-op marker (the release is
/// driven by the returned id list, matching the seam contract).
static PLANCACHE_DESC: types_resowner::ResourceOwnerDesc = types_resowner::ResourceOwnerDesc {
    name: None,
    release_phase: RESOURCE_RELEASE_AFTER_LOCKS,
    release_priority: types_resowner::RELEASE_PRIO_PLANCACHE_REFS,
    ReleaseResource: None,
    DebugPrint: None,
};

/// Collect the plan-cache ref ids still held by `owner` (array + hash) without
/// removing them — used to drive plancache's `ReleaseCachedPlan` re-entry.
fn collect_plan_refs(owner: ResourceOwner) -> Vec<u64> {
    crate::collect_kind_items(owner, &PLANCACHE_DESC)
        .into_iter()
        .map(|d| d.as_usize() as u64)
        .collect()
}
