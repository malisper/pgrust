use std::rc::Rc;

use ::datum::Datum;
use ::mcx::{Mcx, PgString};
use ::types_error::PgResult;
use ::types_resowner::{
    ResourceOwner, ResourceOwnerDesc, RELEASE_PRIO_SNAPSHOT_REFS, RESOURCE_RELEASE_AFTER_LOCKS,
    RESOURCE_RELEASE_BEFORE_LOCKS, RESOURCE_RELEASE_LOCKS,
};
use ::types_snapshot::SnapshotData;

use crate::{
    CurTransactionResourceOwner, CurrentResourceOwner, ResourceOwnerCreate, ResourceOwnerDelete,
    ResourceOwnerEnlarge, ResourceOwnerForget, ResourceOwnerForgetLock, ResourceOwnerGetParent,
    ResourceOwnerNewParent, ResourceOwnerRelease, ResourceOwnerRemember, ResourceOwnerRememberLock,
    SetCurTransactionResourceOwner, SetCurrentResourceOwner, SetTopTransactionResourceOwner,
    TopTransactionResourceOwner,
};

// snapmgr.c's snapshot_resowner_desc. The remembered Datum is the raw Rc
// pointer (C remembers the Snapshot pointer); the resowner entry holds one
// strong count — C's resowner-held regd_count reference.
static SNAPSHOT_REF_DESC: ResourceOwnerDesc = ResourceOwnerDesc {
    name: "snapshot reference",
    release_phase: RESOURCE_RELEASE_AFTER_LOCKS,
    release_priority: RELEASE_PRIO_SNAPSHOT_REFS,
    ReleaseResource: release_snapshot_ref,
    DebugPrint: Some(print_snapshot_ref),
};

fn release_snapshot_ref(res: Datum) {
    // SAFETY: pairs the Rc::into_raw in resource_owner_remember_snapshot;
    // the entry's removal from the owner is the only path that drops it.
    unsafe { Rc::decrement_strong_count(res.as_usize() as *const SnapshotData<'static>) };
}

fn print_snapshot_ref<'a>(mcx: Mcx<'a>, res: Datum) -> PgResult<PgString<'a>> {
    PgString::from_str_in(&format!("snapshot 0x{:x}", res.as_usize()), mcx)
}

fn owner_token(owner: ResourceOwner) -> usize {
    (owner.slot() as usize) | ((owner.generation() as usize) << 32)
}

fn token_owner(token: usize) -> ResourceOwner {
    ResourceOwner::from_parts(token as u32, (token >> 32) as u32)
}

pub(crate) fn install() {
    resowner_seams::at_start_resource_owner::set(|| {
        debug_assert!(TopTransactionResourceOwner().is_null());
        let owner = ResourceOwnerCreate(ResourceOwner::NULL, "TopTransaction")?;
        SetTopTransactionResourceOwner(owner);
        SetCurTransactionResourceOwner(owner);
        SetCurrentResourceOwner(owner);
        Ok(())
    });

    resowner_seams::at_substart_resource_owner::set(|| {
        let owner = ResourceOwnerCreate(CurTransactionResourceOwner(), "SubTransaction")?;
        SetCurTransactionResourceOwner(owner);
        SetCurrentResourceOwner(owner);
        Ok(())
    });

    resowner_seams::release_transaction_owner_before_locks::set(|is_commit| {
        let owner = TopTransactionResourceOwner();
        if !owner.is_null() {
            ResourceOwnerRelease(owner, RESOURCE_RELEASE_BEFORE_LOCKS, is_commit, true)?;
        }
        Ok(())
    });

    resowner_seams::release_transaction_owner_locks::set(|is_commit| {
        let owner = TopTransactionResourceOwner();
        if !owner.is_null() {
            ResourceOwnerRelease(owner, RESOURCE_RELEASE_LOCKS, is_commit, true)?;
            ResourceOwnerRelease(owner, RESOURCE_RELEASE_AFTER_LOCKS, is_commit, true)?;
        }
        Ok(())
    });

    resowner_seams::release_subxact_owner_before_locks::set(|is_commit| {
        let owner = CurTransactionResourceOwner();
        if !owner.is_null() {
            ResourceOwnerRelease(owner, RESOURCE_RELEASE_BEFORE_LOCKS, is_commit, false)?;
        }
        Ok(())
    });

    resowner_seams::release_subxact_owner_locks::set(|is_commit| {
        let owner = CurTransactionResourceOwner();
        if !owner.is_null() {
            ResourceOwnerRelease(owner, RESOURCE_RELEASE_LOCKS, is_commit, false)?;
            ResourceOwnerRelease(owner, RESOURCE_RELEASE_AFTER_LOCKS, is_commit, false)?;
        }
        Ok(())
    });

    resowner_seams::delete_transaction_owner::set(|| {
        let owner = TopTransactionResourceOwner();
        if !owner.is_null() {
            ResourceOwnerDelete(owner);
        }
        SetCurTransactionResourceOwner(ResourceOwner::NULL);
        SetTopTransactionResourceOwner(ResourceOwner::NULL);
        Ok(())
    });

    resowner_seams::cleanup_subxact_owner::set(|| {
        let owner = CurTransactionResourceOwner();
        if !owner.is_null() {
            let parent = ResourceOwnerGetParent(owner);
            SetCurrentResourceOwner(parent);
            SetCurTransactionResourceOwner(parent);
            ResourceOwnerDelete(owner);
        }
        Ok(())
    });

    resowner_seams::reset_current_resource_owner::set(|| {
        SetCurrentResourceOwner(ResourceOwner::NULL);
    });

    resowner_seams::set_current_to_cur_transaction::set(|| {
        SetCurrentResourceOwner(CurTransactionResourceOwner());
    });

    resowner_seams::swap_current_to_cur_transaction_ancestor::set(|levels_up| {
        let prev = CurrentResourceOwner();
        let base = CurTransactionResourceOwner();
        if !base.is_null() {
            let mut owner = base;
            for _ in 0..levels_up {
                let parent = ResourceOwnerGetParent(owner);
                if parent.is_null() {
                    // The owner tree mirrors the transaction stack; keep the
                    // deepest owner rather than installing NULL on overshoot.
                    owner = base;
                    break;
                }
                owner = parent;
            }
            SetCurrentResourceOwner(owner);
        }
        owner_token(prev)
    });

    resowner_seams::restore_current_resource_owner::set(|token| {
        SetCurrentResourceOwner(token_owner(token));
    });

    resowner_seams::current_resource_owner::set(CurrentResourceOwner);

    resowner_seams::resource_owner_enlarge::set(ResourceOwnerEnlarge);

    resowner_seams::resource_owner_remember_snapshot::set(|owner, snapshot| {
        let ptr = Rc::into_raw(snapshot);
        ResourceOwnerRemember(owner, Datum::from_usize(ptr as usize), &SNAPSHOT_REF_DESC)
            .expect("ResourceOwnerRememberSnapshot");
    });

    resowner_seams::resource_owner_forget_snapshot::set(|owner, snapshot| {
        let ptr = Rc::as_ptr(&snapshot);
        ResourceOwnerForget(owner, Datum::from_usize(ptr as usize), &SNAPSHOT_REF_DESC)
            .expect("ResourceOwnerForgetSnapshot");
        // SAFETY: releases the strong count taken by Rc::into_raw at remember;
        // the entry was just removed, so release_snapshot_ref cannot re-drop it.
        unsafe { Rc::decrement_strong_count(ptr) };
    });

    resowner_seams::resource_owner_remember_lock::set(ResourceOwnerRememberLock);

    resowner_seams::resource_owner_forget_lock::set(|owner, tag| {
        ResourceOwnerForgetLock(owner, tag).expect("ResourceOwnerForgetLock");
    });

    resowner_seams::resource_owner_get_parent::set(ResourceOwnerGetParent);

    resowner_portal_seams::resource_owner_create_portal::set(|| {
        ResourceOwnerCreate(CurTransactionResourceOwner(), "Portal")
            .expect("ResourceOwnerCreate(Portal)")
    });

    resowner_portal_seams::resource_owner_release::set(|owner, phase, is_commit, is_top_level| {
        ResourceOwnerRelease(owner, phase, is_commit, is_top_level)
            .expect("ResourceOwnerRelease(Portal)");
    });

    resowner_portal_seams::resource_owner_delete::set(ResourceOwnerDelete);

    resowner_portal_seams::resource_owner_new_parent::set(ResourceOwnerNewParent);
}
