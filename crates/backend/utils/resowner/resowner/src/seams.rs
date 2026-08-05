use std::rc::Rc;

use ::datum::Datum;
use ::mcx::{Mcx, PgString};
use ::types_error::PgResult;
use ::types_resowner::{
    ResourceOwnerDesc, RELEASE_PRIO_SNAPSHOT_REFS, RESOURCE_RELEASE_AFTER_LOCKS,
};
use ::types_snapshot::SnapshotData;

use crate::{
    CurTransactionResourceOwner, CurrentResourceOwner, ResourceOwnerCreate, ResourceOwnerDelete,
    ResourceOwnerEnlarge, ResourceOwnerForget, ResourceOwnerNewParent, ResourceOwnerRelease,
    ResourceOwnerRemember, SetCurrentResourceOwner, TopTransactionResourceOwner,
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
    // ResOwnerReleaseSnapshot (snapmgr.c): the still-registered snapshot must
    // leave snapmgr's registry too, not just drop its owner reference.
    // SAFETY: pairs the Rc::into_raw in resource_owner_remember_snapshot;
    // the entry's removal from the owner is the only path that reclaims it.
    let snap = unsafe { Rc::from_raw(res.as_usize() as *const SnapshotData<'static>) };
    snapmgr_seams::unregister_snapshot_no_owner::call(snap);
}

fn print_snapshot_ref<'a>(mcx: Mcx<'a>, res: Datum) -> PgResult<PgString<'a>> {
    PgString::from_str_in(&format!("snapshot 0x{:x}", res.as_usize()), mcx)
}

pub(crate) fn install() {
    resowner_seams::current_resource_owner::set(CurrentResourceOwner);

    resowner_seams::set_current_resource_owner::set(SetCurrentResourceOwner);

    resowner_seams::top_transaction_resource_owner::set(TopTransactionResourceOwner);

    resowner_seams::resource_owner_enlarge::set(ResourceOwnerEnlarge);

    resowner_seams::resource_owner_remember::set(ResourceOwnerRemember);

    resowner_seams::resource_owner_forget::set(ResourceOwnerForget);

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
