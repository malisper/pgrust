//! SI message array / group construction (inval.c `InvalMessageArray`,
//! `InvalidationMsgsGroup`, and the `Add*InvalidationMessage` /
//! `AppendInvalidationMessage*` / `ProcessMessageSubGroup*` family).

use mcx::{Mcx, PgVec};
use types_core::{InvalidOid, Oid};
use types_error::PgResult;
use types_storage::{
    SharedInvalCatalogMsg, SharedInvalCatcacheMsg, SharedInvalRelSyncMsg, SharedInvalRelcacheMsg,
    SharedInvalSnapshotMsg, SharedInvalidationMessage,
};

use crate::{CAT_CACHE_MSGS, REL_CACHE_MSGS};

/// Pointers to main arrays in TopTransactionContext (inval.c `InvalMessageArray`).
///
/// In C this is a `(palloc'd array, maxmsgs)` pair; here the densely-packed
/// array is a plain context-charged [`PgVec`] (the persistent working buffer of
/// this crate) and the dense-append `nextmsg == len` invariant is what the
/// group index bookkeeping relies on.
pub(crate) struct InvalMessageArray<'mcx> {
    pub(crate) msgs: PgVec<'mcx, SharedInvalidationMessage>,
}

impl<'mcx> InvalMessageArray<'mcx> {
    /// An empty array charged to `mcx` (the `maxmsgs = 0` starting state).
    pub(crate) fn new(mcx: Mcx<'mcx>) -> Self {
        InvalMessageArray {
            msgs: PgVec::new_in(mcx),
        }
    }
}

/// Control information for one logical group of messages (inval.c).
///
/// Public because the `ProcessInvalidationMessages(group, func)` entry point
/// (used by siblings to walk a collected group) names it.
#[derive(Clone, Copy, Default)]
pub struct InvalidationMsgsGroup {
    pub(crate) firstmsg: [usize; 2],
    pub(crate) nextmsg: [usize; 2],
}

impl InvalidationMsgsGroup {
    /// `SetSubGroupToFollow(target, prior, subgroup)`:
    /// `target->firstmsg[sg] = target->nextmsg[sg] = prior->nextmsg[sg]`.
    pub(crate) fn set_sub_group_to_follow(&mut self, prior: &InvalidationMsgsGroup, subgroup: usize) {
        self.firstmsg[subgroup] = prior.nextmsg[subgroup];
        self.nextmsg[subgroup] = prior.nextmsg[subgroup];
    }

    /// `SetGroupToFollow(target, prior)`.
    pub(crate) fn set_group_to_follow(&mut self, prior: &InvalidationMsgsGroup) {
        self.set_sub_group_to_follow(prior, CAT_CACHE_MSGS);
        self.set_sub_group_to_follow(prior, REL_CACHE_MSGS);
    }

    /// `NumMessagesInSubGroup(group, subgroup)`.
    pub(crate) fn num_messages_in_sub_group(&self, subgroup: usize) -> usize {
        self.nextmsg[subgroup] - self.firstmsg[subgroup]
    }

    /// `NumMessagesInGroup(group)`.
    pub(crate) fn num_messages_in_group(&self) -> usize {
        self.num_messages_in_sub_group(CAT_CACHE_MSGS)
            + self.num_messages_in_sub_group(REL_CACHE_MSGS)
    }
}

/// `AddInvalidationMessage` — add a message to the end of a (sub)group's
/// subgroup, appending to the dense array.
///
/// The group must be the last active one, since we assume we can add to the
/// end of the relevant InvalMessageArray. With the [`PgVec`]-backed dense array
/// the C `maxmsgs`/`repalloc` growth bookkeeping is the `Vec`'s own capacity
/// management, and the C `Assert(nextindex == 0)` first-alloc invariant is
/// implied by the `nextindex == msgs.len()` push.
pub(crate) fn add_invalidation_message<'mcx>(
    _mcx: Mcx<'mcx>,
    arrays: &mut [InvalMessageArray<'mcx>; 2],
    group: &mut InvalidationMsgsGroup,
    subgroup: usize,
    msg: SharedInvalidationMessage,
) -> PgResult<()> {
    let ima = &mut arrays[subgroup];
    let nextindex = group.nextmsg[subgroup];
    debug_assert_eq!(nextindex, ima.msgs.len());
    // Okay, add message to current group
    ima.msgs.push(msg);
    group.nextmsg[subgroup] += 1;
    Ok(())
}

/// `AppendInvalidationMessageSubGroup` — append one subgroup to another,
/// resetting the source subgroup to empty.
pub(crate) fn append_invalidation_message_sub_group(
    dest: &mut InvalidationMsgsGroup,
    src: &mut InvalidationMsgsGroup,
    subgroup: usize,
) {
    // Messages must be adjacent in main array
    debug_assert_eq!(dest.nextmsg[subgroup], src.firstmsg[subgroup]);

    // ... which makes this easy:
    dest.nextmsg[subgroup] = src.nextmsg[subgroup];

    // This is handy for some callers and irrelevant for others.  But we do it
    // always, reasoning that it's bad to leave different groups pointing at the
    // same fragment of the message array.
    src.set_sub_group_to_follow(dest, subgroup);
}

/// `AddCatcacheInvalidationMessage`.
pub(crate) fn add_catcache_invalidation_message<'mcx>(
    mcx: Mcx<'mcx>,
    arrays: &mut [InvalMessageArray<'mcx>; 2],
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
    // (The C VALGRIND_MAKE_MEM_DEFINED only marks the union padding bytes
    // defined for the sinvaladt.c ringbuffer; the Rust enum carries no such
    // padding hazard.)
    add_invalidation_message(mcx, arrays, group, CAT_CACHE_MSGS, msg)
}

/// `AddCatalogInvalidationMessage`.
pub(crate) fn add_catalog_invalidation_message<'mcx>(
    mcx: Mcx<'mcx>,
    arrays: &mut [InvalMessageArray<'mcx>; 2],
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

/// `AddRelcacheInvalidationMessage` (with the dedup scan).
pub(crate) fn add_relcache_invalidation_message<'mcx>(
    mcx: Mcx<'mcx>,
    arrays: &mut [InvalMessageArray<'mcx>; 2],
    group: &mut InvalidationMsgsGroup,
    db_id: Oid,
    rel_id: Oid,
) -> PgResult<()> {
    // Don't add a duplicate item. We assume dbId need not be checked because
    // it will never change. InvalidOid for relId means all relations so we
    // don't need to add individual ones when it is present.
    let first = group.firstmsg[REL_CACHE_MSGS];
    let end = group.nextmsg[REL_CACHE_MSGS];
    for existing in &arrays[REL_CACHE_MSGS].msgs[first..end] {
        if let SharedInvalidationMessage::Relcache(rc) = existing {
            if rc.relId == rel_id || rc.relId == InvalidOid {
                return Ok(());
            }
        }
    }

    // OK, add the item
    let msg = SharedInvalidationMessage::Relcache(SharedInvalRelcacheMsg {
        dbId: db_id,
        relId: rel_id,
    });
    add_invalidation_message(mcx, arrays, group, REL_CACHE_MSGS, msg)
}

/// `AddRelsyncInvalidationMessage` (relcache subgroup; `Rs` variant).
pub(crate) fn add_relsync_invalidation_message<'mcx>(
    mcx: Mcx<'mcx>,
    arrays: &mut [InvalMessageArray<'mcx>; 2],
    group: &mut InvalidationMsgsGroup,
    db_id: Oid,
    rel_id: Oid,
) -> PgResult<()> {
    // Don't add a duplicate item.
    let first = group.firstmsg[REL_CACHE_MSGS];
    let end = group.nextmsg[REL_CACHE_MSGS];
    for existing in &arrays[REL_CACHE_MSGS].msgs[first..end] {
        if let SharedInvalidationMessage::RelSync(rs) = existing {
            if rs.relid == rel_id || rs.relid == InvalidOid {
                return Ok(());
            }
        }
    }

    // OK, add the item
    let msg = SharedInvalidationMessage::RelSync(SharedInvalRelSyncMsg {
        dbId: db_id,
        relid: rel_id,
    });
    add_invalidation_message(mcx, arrays, group, REL_CACHE_MSGS, msg)
}

/// `AddSnapshotInvalidationMessage` (with the dedup scan).
pub(crate) fn add_snapshot_invalidation_message<'mcx>(
    mcx: Mcx<'mcx>,
    arrays: &mut [InvalMessageArray<'mcx>; 2],
    group: &mut InvalidationMsgsGroup,
    db_id: Oid,
    rel_id: Oid,
) -> PgResult<()> {
    // Don't add a duplicate item
    // We assume dbId need not be checked because it will never change
    let first = group.firstmsg[REL_CACHE_MSGS];
    let end = group.nextmsg[REL_CACHE_MSGS];
    for existing in &arrays[REL_CACHE_MSGS].msgs[first..end] {
        if let SharedInvalidationMessage::Snapshot(sn) = existing {
            if sn.relId == rel_id {
                return Ok(());
            }
        }
    }

    // OK, add the item
    let msg = SharedInvalidationMessage::Snapshot(SharedInvalSnapshotMsg {
        dbId: db_id,
        relId: rel_id,
    });
    add_invalidation_message(mcx, arrays, group, REL_CACHE_MSGS, msg)
}

/// `AppendInvalidationMessages` — append one whole group to another.
pub(crate) fn append_invalidation_messages(
    dest: &mut InvalidationMsgsGroup,
    src: &mut InvalidationMsgsGroup,
) {
    append_invalidation_message_sub_group(dest, src, CAT_CACHE_MSGS);
    append_invalidation_message_sub_group(dest, src, REL_CACHE_MSGS);
}

/// `ProcessMessageSubGroup` — run `f(&msg)` for each message in a subgroup.
pub(crate) fn process_message_sub_group<'mcx, F: FnMut(&SharedInvalidationMessage) -> PgResult<()>>(
    arrays: &[InvalMessageArray<'mcx>; 2],
    group: &InvalidationMsgsGroup,
    subgroup: usize,
    mut f: F,
) -> PgResult<()> {
    let first = group.firstmsg[subgroup];
    let end = group.nextmsg[subgroup];
    for msg in &arrays[subgroup].msgs[first..end] {
        f(msg)?;
    }
    Ok(())
}

/// `ProcessInvalidationMessages` (static helper) — run `func` for every message
/// in a group, catcache entries first.
pub(crate) fn process_invalidation_messages_group<
    'mcx,
    F: FnMut(&SharedInvalidationMessage) -> PgResult<()>,
>(
    arrays: &[InvalMessageArray<'mcx>; 2],
    group: &InvalidationMsgsGroup,
    mut func: F,
) -> PgResult<()> {
    process_message_sub_group(arrays, group, CAT_CACHE_MSGS, &mut func)?;
    process_message_sub_group(arrays, group, REL_CACHE_MSGS, &mut func)?;
    Ok(())
}

/// `ProcessInvalidationMessagesMulti` (static helper) — pass each subgroup as a
/// contiguous slice to `func`.
///
/// Mirrors `ProcessMessageSubGroupMulti`: `func` is only invoked for a subgroup
/// when it holds at least one message.
pub(crate) fn process_invalidation_messages_multi<
    'mcx,
    F: FnMut(&[SharedInvalidationMessage]) -> PgResult<()>,
>(
    arrays: &[InvalMessageArray<'mcx>; 2],
    group: &InvalidationMsgsGroup,
    mut func: F,
) -> PgResult<()> {
    for subgroup in [CAT_CACHE_MSGS, REL_CACHE_MSGS] {
        let n = group.num_messages_in_sub_group(subgroup);
        if n > 0 {
            let first = group.firstmsg[subgroup];
            func(&arrays[subgroup].msgs[first..first + n])?;
        }
    }
    Ok(())
}
