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
    SetCurrentResourceOwner, SetCurTransactionResourceOwner, SetTopTransactionResourceOwner,
    TopTransactionResourceOwner,
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
mod ac {
    pub use backend_storage_aio_completion_seams::*;
}
mod rcr {
    pub use backend_utils_cache_relcache_seams::*;
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

// ===========================================================================
// Relation-ref ResourceOwnerDesc (`relref_resowner_desc`, defined in
// relcache.c). The release callback delegates to the relcache crate through
// the `release_relation_ref` seam (which runs `ResOwnerReleaseRelation`).
// The remembered `Datum` is the relation's `Oid` handle (the relcache entry
// key), mirroring the C `Relation` pointer.
// ===========================================================================

fn release_relation_ref(res: Datum) {
    let relid: types_core::primitive::Oid = res.as_usize() as u32;
    rcr::release_relation_ref::call(relid)
        .expect("ResOwnerReleaseRelation: leaked relcache pin release failed");
}

/// `ResOwnerPrintRelCache(Datum res)` (relcache.c) — leak-warning formatter for
/// `relref_resowner_desc`. C prints `relation "<relname>"`; the relname lives
/// behind the relcache entry, so this port prints the relation's OID identity
/// (`relation with OID <oid>`), which is what the leak warning needs to point at
/// the offending pin. With the remember/forget wiring below the leak path no
/// longer fires for the CREATE INDEX case this addresses.
fn print_relation_ref(res: Datum) -> Option<String> {
    let relid = res.as_usize() as u32;
    Some(format!("relation with OID {relid}"))
}

static RELCACHE_DESC: types_resowner::ResourceOwnerDesc = types_resowner::ResourceOwnerDesc {
    name: None, // "relcache reference" — rendered via DebugPrint below
    release_phase: RESOURCE_RELEASE_BEFORE_LOCKS,
    release_priority: types_resowner::RELEASE_PRIO_RELCACHE_REFS,
    ReleaseResource: Some(release_relation_ref),
    DebugPrint: Some(print_relation_ref),
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

    // `AtStart_ResourceOwner()` (xact.c:1330): create the toplevel transaction
    // resource owner and publish it to all three transaction-owner globals.
    rr::at_start_resource_owner::set(|| {
        // Assert(TopTransactionResourceOwner == NULL);
        debug_assert!(TopTransactionResourceOwner().is_none());
        let owner = ResourceOwnerCreate(None, "TopTransaction")?;
        SetTopTransactionResourceOwner(Some(owner));
        SetCurTransactionResourceOwner(Some(owner));
        SetCurrentResourceOwner(Some(owner));
        Ok(())
    });

    // CommitTransaction/AbortTransaction/PrepareTransaction first release leg
    // (xact.c): ResourceOwnerRelease(TopTransactionResourceOwner, BEFORE_LOCKS,
    // true, isCommit). The `CurrentResourceOwner = NULL` that Commit/Abort do
    // immediately before this (but Prepare does not) is the separate
    // `reset_current_resource_owner` seam, called explicitly by those paths.
    rr::release_transaction_owner_before_locks::set(|is_commit| {
        if let Some(owner) = TopTransactionResourceOwner() {
            crate::ResourceOwnerRelease(owner, RESOURCE_RELEASE_BEFORE_LOCKS, is_commit, true)?;
        }
        Ok(())
    });

    // CommitTransaction/AbortTransaction second release legs (xact.c):
    //   ResourceOwnerRelease(TopTransactionResourceOwner, LOCKS, true, isCommit);
    //   ResourceOwnerRelease(TopTransactionResourceOwner, AFTER_LOCKS, true, isCommit);
    rr::release_transaction_owner_locks::set(|is_commit| {
        if let Some(owner) = TopTransactionResourceOwner() {
            crate::ResourceOwnerRelease(owner, RESOURCE_RELEASE_LOCKS, is_commit, true)?;
            crate::ResourceOwnerRelease(owner, RESOURCE_RELEASE_AFTER_LOCKS, is_commit, true)?;
        }
        Ok(())
    });

    // CommitTransaction/AbortTransaction/CleanupTransaction final leg (xact.c):
    //   ResourceOwnerDelete(TopTransactionResourceOwner);
    //   CurTransactionResourceOwner = NULL; TopTransactionResourceOwner = NULL;
    rr::delete_transaction_owner::set(|| {
        if let Some(owner) = TopTransactionResourceOwner() {
            ResourceOwnerDelete(owner)?;
        }
        SetCurTransactionResourceOwner(None);
        SetTopTransactionResourceOwner(None);
        Ok(())
    });

    // --- resowner-seams ----------------------------------------------------
    rs::resource_owner_create_portal::set(|| {
        // C's `CreatePortal` (portalmem.c) creates the portal's resource owner as
        // a child of `CurTransactionResourceOwner`, NOT the per-command
        // `CurrentResourceOwner`. A portal can outlive the utility command that
        // created it (a DECLARE CURSOR portal is fetched by later FETCH commands),
        // so its owner must be parented under the transaction-lifetime owner;
        // parenting under the per-command owner would cascade-free the portal
        // owner when that command's owner is released, leaving a stale handle for
        // the next FETCH ("stale ResourceOwner" / "resource was not closed").
        // Fall back to `CurrentResourceOwner` only outside a transaction (the
        // bootstrap / single-command path where no CurTransaction owner exists).
        let parent = crate::CurTransactionResourceOwner().or_else(CurrentResourceOwner);
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

    // --- relcache-seams (relation-ref pin bookkeeping) ---------------------
    // `ResourceOwnerEnlarge(CurrentResourceOwner)` /
    // `ResourceOwnerRememberRelationRef(CurrentResourceOwner, rel)` /
    // `ResourceOwnerForgetRelationRef(CurrentResourceOwner, rel)` (relcache.c).
    // The remembered Datum is the relation's Oid handle.
    rcr::resource_owner_enlarge_relation::set(|| {
        let owner = current_or_err()?;
        ResourceOwnerEnlarge(owner)
    });

    rcr::resource_owner_remember_relation::set(|relid| {
        let owner =
            current_or_err().expect("RelationIncrementReferenceCount: CurrentResourceOwner is NULL");
        ResourceOwnerRemember(
            owner,
            Datum::from_usize(relid as usize),
            &RELCACHE_DESC,
        )
        .expect("ResourceOwnerRememberRelationRef");
    });

    rcr::resource_owner_forget_relation::set(|relid| {
        let owner =
            current_or_err().expect("RelationDecrementReferenceCount: CurrentResourceOwner is NULL");
        ResourceOwnerForget(
            owner,
            Datum::from_usize(relid as usize),
            &RELCACHE_DESC,
        )
        .expect("ResourceOwnerForgetRelationRef");
    });

    // --- aio-completion-seams: AIO-handle resowner registry ----------------
    // `ResourceOwnerRememberAioHandle(owner, &ioh->resowner_node)` /
    // `ResourceOwnerForgetAioHandle(...)` (resowner.c) — the node identity is
    // the io-handle index.
    ac::resource_owner_remember_aio_handle::set(|owner, ioh_index| {
        crate::ResourceOwnerRememberAioHandle(owner, ioh_index as u64)
    });

    ac::resource_owner_forget_aio_handle::set(|owner, ioh_index| {
        crate::ResourceOwnerForgetAioHandle(owner, ioh_index as u64);
        Ok(())
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
