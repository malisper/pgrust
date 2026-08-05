use mcx::{Mcx, PgVec};
use types_core::{InvalidOid, Oid};
use types_error::PgResult;
use types_storage::{
    SharedInvalCatalogMsg, SharedInvalCatcacheMsg, SharedInvalRelSyncMsg, SharedInvalRelcacheMsg,
    SharedInvalSnapshotMsg, SharedInvalidationMessage,
};

use crate::{CAT_CACHE_MSGS, REL_CACHE_MSGS};

pub(crate) type MsgArrays<'mcx> = [PgVec<'mcx, SharedInvalidationMessage>; 2];

#[derive(Clone, Copy, Default)]
pub struct InvalidationMsgsGroup {
    pub(crate) firstmsg: [usize; 2],
    pub(crate) nextmsg: [usize; 2],
}

impl InvalidationMsgsGroup {
    pub(crate) fn set_sub_group_to_follow(&mut self, prior: &Self, subgroup: usize) {
        self.firstmsg[subgroup] = prior.nextmsg[subgroup];
        self.nextmsg[subgroup] = prior.nextmsg[subgroup];
    }

    pub(crate) fn set_group_to_follow(&mut self, prior: &Self) {
        self.set_sub_group_to_follow(prior, CAT_CACHE_MSGS);
        self.set_sub_group_to_follow(prior, REL_CACHE_MSGS);
    }

    pub(crate) fn num_in_sub_group(&self, subgroup: usize) -> usize {
        self.nextmsg[subgroup] - self.firstmsg[subgroup]
    }

    pub(crate) fn num_in_group(&self) -> usize {
        self.num_in_sub_group(CAT_CACHE_MSGS) + self.num_in_sub_group(REL_CACHE_MSGS)
    }
}

pub(crate) fn subgroup_slice<'a, 'mcx>(
    arrays: &'a MsgArrays<'mcx>,
    group: &InvalidationMsgsGroup,
    subgroup: usize,
) -> &'a [SharedInvalidationMessage] {
    &arrays[subgroup][group.firstmsg[subgroup]..group.nextmsg[subgroup]]
}

// C writes at the group cursor (`ima->msgs[nextindex] = *msg`), which is not
// always the physical end: ForgetInplace_Inval rolls the cursor back over
// slots a stashed inplace group occupied, and the next transactional add must
// overwrite them in place. A blind push breaks that reclaim.
pub(crate) fn add_invalidation_message<'mcx>(
    mcx: Mcx<'mcx>,
    arrays: &mut MsgArrays<'mcx>,
    group: &mut InvalidationMsgsGroup,
    subgroup: usize,
    msg: SharedInvalidationMessage,
) -> PgResult<()> {
    let ima = &mut arrays[subgroup];
    let nextindex = group.nextmsg[subgroup];
    debug_assert!(nextindex <= ima.len());
    if nextindex < ima.len() {
        ima[nextindex] = msg;
    } else {
        ima.try_reserve(1)
            .map_err(|_| mcx.oom(size_of::<SharedInvalidationMessage>()))?;
        ima.push(msg);
    }
    group.nextmsg[subgroup] += 1;
    Ok(())
}

pub(crate) fn append_invalidation_message_sub_group(
    dest: &mut InvalidationMsgsGroup,
    src: &mut InvalidationMsgsGroup,
    subgroup: usize,
) {
    debug_assert_eq!(dest.nextmsg[subgroup], src.firstmsg[subgroup]);
    dest.nextmsg[subgroup] = src.nextmsg[subgroup];
    src.set_sub_group_to_follow(dest, subgroup);
}

pub(crate) fn append_invalidation_messages(
    dest: &mut InvalidationMsgsGroup,
    src: &mut InvalidationMsgsGroup,
) {
    append_invalidation_message_sub_group(dest, src, CAT_CACHE_MSGS);
    append_invalidation_message_sub_group(dest, src, REL_CACHE_MSGS);
}

pub(crate) fn add_catcache_invalidation_message<'mcx>(
    mcx: Mcx<'mcx>,
    arrays: &mut MsgArrays<'mcx>,
    group: &mut InvalidationMsgsGroup,
    id: i32,
    hash_value: u32,
    db_id: Oid,
) -> PgResult<()> {
    debug_assert!(id < i8::MAX as i32);
    let msg = SharedInvalidationMessage::Catcache(SharedInvalCatcacheMsg {
        id: id as i8,
        dbId: db_id,
        hashValue: hash_value,
    });
    add_invalidation_message(mcx, arrays, group, CAT_CACHE_MSGS, msg)
}

pub(crate) fn add_catalog_invalidation_message<'mcx>(
    mcx: Mcx<'mcx>,
    arrays: &mut MsgArrays<'mcx>,
    group: &mut InvalidationMsgsGroup,
    db_id: Oid,
    cat_id: Oid,
) -> PgResult<()> {
    let msg = SharedInvalidationMessage::Catalog(SharedInvalCatalogMsg {
        dbId: db_id,
        catId: cat_id,
    });
    add_invalidation_message(mcx, arrays, group, CAT_CACHE_MSGS, msg)
}

pub(crate) fn add_relcache_invalidation_message<'mcx>(
    mcx: Mcx<'mcx>,
    arrays: &mut MsgArrays<'mcx>,
    group: &mut InvalidationMsgsGroup,
    db_id: Oid,
    rel_id: Oid,
) -> PgResult<()> {
    for existing in subgroup_slice(arrays, group, REL_CACHE_MSGS) {
        if let SharedInvalidationMessage::Relcache(rc) = existing {
            if rc.relId == rel_id || rc.relId == InvalidOid {
                return Ok(());
            }
        }
    }
    let msg = SharedInvalidationMessage::Relcache(SharedInvalRelcacheMsg {
        dbId: db_id,
        relId: rel_id,
    });
    add_invalidation_message(mcx, arrays, group, REL_CACHE_MSGS, msg)
}

pub(crate) fn add_relsync_invalidation_message<'mcx>(
    mcx: Mcx<'mcx>,
    arrays: &mut MsgArrays<'mcx>,
    group: &mut InvalidationMsgsGroup,
    db_id: Oid,
    rel_id: Oid,
) -> PgResult<()> {
    for existing in subgroup_slice(arrays, group, REL_CACHE_MSGS) {
        if let SharedInvalidationMessage::RelSync(rs) = existing {
            if rs.relid == rel_id || rs.relid == InvalidOid {
                return Ok(());
            }
        }
    }
    let msg = SharedInvalidationMessage::RelSync(SharedInvalRelSyncMsg {
        dbId: db_id,
        relid: rel_id,
    });
    add_invalidation_message(mcx, arrays, group, REL_CACHE_MSGS, msg)
}

pub(crate) fn add_snapshot_invalidation_message<'mcx>(
    mcx: Mcx<'mcx>,
    arrays: &mut MsgArrays<'mcx>,
    group: &mut InvalidationMsgsGroup,
    db_id: Oid,
    rel_id: Oid,
) -> PgResult<()> {
    for existing in subgroup_slice(arrays, group, REL_CACHE_MSGS) {
        if let SharedInvalidationMessage::Snapshot(sn) = existing {
            if sn.relId == rel_id {
                return Ok(());
            }
        }
    }
    let msg = SharedInvalidationMessage::Snapshot(SharedInvalSnapshotMsg {
        dbId: db_id,
        relId: rel_id,
    });
    add_invalidation_message(mcx, arrays, group, REL_CACHE_MSGS, msg)
}
