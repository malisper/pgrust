//! SI message array / group construction (inval.c `InvalMessageArray`,
//! `InvalidationMsgsGroup`, and the `Add*InvalidationMessage` /
//! `AppendInvalidationMessage*` / `ProcessMessageSubGroup*` family).

use mcx::{Mcx, PgVec};
use types_core::Oid;
use types_error::PgResult;
use types_storage::SharedInvalidationMessage;

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
    /// `SetSubGroupToFollow(target, prior, subgroup)`.
    pub(crate) fn set_sub_group_to_follow(&mut self, _prior: &InvalidationMsgsGroup, _subgroup: usize) {
        todo!("SetSubGroupToFollow: firstmsg=nextmsg=prior.nextmsg[subgroup]")
    }

    /// `SetGroupToFollow(target, prior)`.
    pub(crate) fn set_group_to_follow(&mut self, _prior: &InvalidationMsgsGroup) {
        todo!("SetGroupToFollow: set_sub_group_to_follow for both subgroups")
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

/// The dense backing slice for one subgroup of a group: `arrays[subgroup].msgs`
/// indexed by `[group.firstmsg[subgroup] .. group.nextmsg[subgroup]]`. This is
/// the `&InvalMessageArrays[subgroup].msgs[group->firstmsg[subgroup]]` pointer +
/// `NumMessagesInSubGroup(group, subgroup)` length that the C `memcpy` /
/// `XLogRegisterData` calls read.
pub(crate) fn num_messages_in_subgroup_slice<'a, 'mcx>(
    arrays: &'a [InvalMessageArray<'mcx>; 2],
    group: &InvalidationMsgsGroup,
    subgroup: usize,
) -> &'a [SharedInvalidationMessage] {
    &arrays[subgroup].msgs[group.firstmsg[subgroup]..group.nextmsg[subgroup]]
}

/// `AddInvalidationMessage` — add a message to the end of a (sub)group's
/// subgroup, appending to the dense array.
pub(crate) fn add_invalidation_message<'mcx>(
    _mcx: Mcx<'mcx>,
    _arrays: &mut [InvalMessageArray<'mcx>; 2],
    _group: &mut InvalidationMsgsGroup,
    _subgroup: usize,
    _msg: SharedInvalidationMessage,
) -> PgResult<()> {
    todo!("AddInvalidationMessage: push onto arrays[subgroup].msgs, bump group.nextmsg[subgroup]")
}

/// `AppendInvalidationMessageSubGroup` — append one subgroup to another,
/// resetting the source subgroup to empty.
pub(crate) fn append_invalidation_message_sub_group(
    _dest: &mut InvalidationMsgsGroup,
    _src: &mut InvalidationMsgsGroup,
    _subgroup: usize,
) {
    todo!("AppendInvalidationMessageSubGroup")
}

/// `AddCatcacheInvalidationMessage`.
pub(crate) fn add_catcache_invalidation_message<'mcx>(
    _mcx: Mcx<'mcx>,
    _arrays: &mut [InvalMessageArray<'mcx>; 2],
    _group: &mut InvalidationMsgsGroup,
    _id: i32,
    _hash_value: u32,
    _db_id: Oid,
) -> PgResult<()> {
    todo!("AddCatcacheInvalidationMessage")
}

/// `AddCatalogInvalidationMessage`.
pub(crate) fn add_catalog_invalidation_message<'mcx>(
    _mcx: Mcx<'mcx>,
    _arrays: &mut [InvalMessageArray<'mcx>; 2],
    _group: &mut InvalidationMsgsGroup,
    _db_id: Oid,
    _cat_id: Oid,
) -> PgResult<()> {
    todo!("AddCatalogInvalidationMessage")
}

/// `AddRelcacheInvalidationMessage` (with the dedup scan).
pub(crate) fn add_relcache_invalidation_message<'mcx>(
    _mcx: Mcx<'mcx>,
    _arrays: &mut [InvalMessageArray<'mcx>; 2],
    _group: &mut InvalidationMsgsGroup,
    _db_id: Oid,
    _rel_id: Oid,
) -> PgResult<()> {
    todo!("AddRelcacheInvalidationMessage")
}

/// `AddRelsyncInvalidationMessage` (relcache subgroup; `Rs` variant).
pub(crate) fn add_relsync_invalidation_message<'mcx>(
    _mcx: Mcx<'mcx>,
    _arrays: &mut [InvalMessageArray<'mcx>; 2],
    _group: &mut InvalidationMsgsGroup,
    _db_id: Oid,
    _rel_id: Oid,
) -> PgResult<()> {
    todo!("AddRelsyncInvalidationMessage")
}

/// `AddSnapshotInvalidationMessage` (with the dedup scan).
pub(crate) fn add_snapshot_invalidation_message<'mcx>(
    _mcx: Mcx<'mcx>,
    _arrays: &mut [InvalMessageArray<'mcx>; 2],
    _group: &mut InvalidationMsgsGroup,
    _db_id: Oid,
    _rel_id: Oid,
) -> PgResult<()> {
    todo!("AddSnapshotInvalidationMessage")
}

/// `AppendInvalidationMessages` — append one whole group to another.
pub(crate) fn append_invalidation_messages(
    _dest: &mut InvalidationMsgsGroup,
    _src: &mut InvalidationMsgsGroup,
) {
    todo!("AppendInvalidationMessages: both subgroups")
}

/// `ProcessMessageSubGroup` — run `f(&msg)` for each message in a subgroup.
pub(crate) fn process_message_sub_group<'mcx, F: FnMut(&SharedInvalidationMessage) -> PgResult<()>>(
    _arrays: &[InvalMessageArray<'mcx>; 2],
    _group: &InvalidationMsgsGroup,
    _subgroup: usize,
    _f: F,
) -> PgResult<()> {
    todo!("ProcessMessageSubGroup")
}

/// `ProcessInvalidationMessages` (static helper) — run `func` for every message
/// in a group, catcache entries first.
pub(crate) fn process_invalidation_messages_group<
    'mcx,
    F: FnMut(&SharedInvalidationMessage) -> PgResult<()>,
>(
    _arrays: &[InvalMessageArray<'mcx>; 2],
    _group: &InvalidationMsgsGroup,
    _func: F,
) -> PgResult<()> {
    todo!("ProcessInvalidationMessages: cat subgroup then rel subgroup")
}

/// `ProcessInvalidationMessagesMulti` (static helper) — pass each subgroup as a
/// contiguous slice to `func`.
pub(crate) fn process_invalidation_messages_multi<
    'mcx,
    F: FnMut(&[SharedInvalidationMessage]) -> PgResult<()>,
>(
    _arrays: &[InvalMessageArray<'mcx>; 2],
    _group: &InvalidationMsgsGroup,
    _func: F,
) -> PgResult<()> {
    todo!("ProcessInvalidationMessagesMulti")
}
