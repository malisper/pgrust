//! Fetch lane: heap_fetch / heap_hot_search_buffer / heap_get_latest_tid.
//! [`HeapFetchResult`] re-derives its tuple from the pin it carries, so the
//! borrow can never outlive the pin.

use ::bufmgr_seams::BufferPin;
use ::types_core::xact::InvalidTransactionId;
use ::types_core::xact::TransactionIdIsValid;
use ::types_core::{GlobalVisStateHandle, TransactionId};
use ::types_error::PgResult;
use ::types_rel::RelationData;
use ::types_snapshot::SnapshotData;
use ::types_storage::bufpage::ItemIdData;
use ::types_tuple::{
    FirstOffsetNumber, HeapTupleData, ItemPointerData, ItemPointerEquals,
    ItemPointerGetBlockNumber, ItemPointerGetOffsetNumber, ItemPointerIndicatesMovedPartitions,
    ItemPointerIsValid, ItemPointerSetOffsetNumber, HEAP_XMAX_INVALID,
};

use crate::{HeapCheckForSerializableConflictOut, HeapTupleHeaderGetUpdateXid};
use heapam_visibility_seams as hv_seam;

pub struct HeapFetchResult {
    pub found: bool,
    /// Pinned iff `Some` (success, or `keep_buf` after a failed time qual).
    pub pin: Option<BufferPin>,
    tid: ItemPointerData,
    table_oid: ::types_core::Oid,
    lp: ItemIdData,
}

impl HeapFetchResult {
    /// C's filled `*tuple` (`None` = `t_data == NULL`), scoped to the pin-holding `&self`.
    pub fn tuple(&self) -> Option<HeapTupleData<'_>> {
        let pin = self.pin.as_ref()?;
        // SAFETY: pinned page, normal line pointer (checked in heap_fetch);
        // normal items satisfy the page invariant (item_raw_unchecked).
        let (ptr, len) = unsafe { pin.page().item_raw_unchecked(self.lp) };
        // SAFETY: pinned page, normal line pointer (checked in heap_fetch).
        Some(unsafe { HeapTupleData::from_raw_parts(ptr, len, self.tid, self.table_oid) })
    }
}

pub fn heap_fetch<'mcx>(
    relation: &RelationData<'mcx>,
    snapshot: &SnapshotData<'_>,
    tid: ItemPointerData,
    keep_buf: bool,
) -> PgResult<HeapFetchResult> {
    let not_found = HeapFetchResult {
        found: false,
        pin: None,
        tid,
        table_oid: relation.rd_id,
        lp: ItemIdData::new(0, 0, 0),
    };

    let pin = BufferPin::adopt(bufmgr_seams::read_buffer::call(
        relation,
        ItemPointerGetBlockNumber(&tid),
    )?)
    .expect("ReadBuffer returned InvalidBuffer");

    let lock = pin.lock_share()?;
    let page = pin.page();

    // VACUUM may have shrunk the page since the TID was obtained.
    let offnum = ItemPointerGetOffsetNumber(&tid);
    if offnum < FirstOffsetNumber || offnum > page.max_offset_number() {
        drop(lock);
        pin.release();
        return Ok(not_found);
    }

    let lp = page.item_id(offnum);
    if !lp.is_normal() {
        drop(lock);
        pin.release();
        return Ok(not_found);
    }

    // SAFETY: pinned + share-locked page, normal line pointer (page
    // invariant, item_raw_unchecked contract).
    let (ptr, len) = unsafe { page.item_raw_unchecked(lp) };
    // SAFETY: pinned + share-locked page, normal line pointer.
    let mut tuple = unsafe { HeapTupleData::from_raw_parts(ptr, len, tid, relation.rd_id) };

    let valid = hv_seam::heap_tuple_satisfies_visibility::call(&mut tuple, snapshot, pin.buffer())?;

    if valid {
        predicate_seams::predicate_lock_tid::call(relation, tid, snapshot, tuple.t_data().xmin())?;
    }
    HeapCheckForSerializableConflictOut(valid, relation, &mut tuple, pin.buffer(), snapshot)?;

    drop(lock);

    if valid || keep_buf {
        return Ok(HeapFetchResult {
            found: valid,
            pin: Some(pin),
            tid,
            table_oid: relation.rd_id,
            lp,
        });
    }

    pin.release();
    Ok(not_found)
}

/// `heap_fetch` under a caller-owned SnapshotDirty: xmin/xmax written back
/// (the EPQ update-chain wait probe; heapam_handler.c heapam_tuple_lock).
pub fn heap_fetch_dirty<'mcx>(
    relation: &RelationData<'mcx>,
    snapshot: &mut SnapshotData<'_>,
    tid: ItemPointerData,
    keep_buf: bool,
) -> PgResult<HeapFetchResult> {
    debug_assert!(snapshot.snapshot_type == ::types_snapshot::SNAPSHOT_DIRTY);
    let not_found = HeapFetchResult {
        found: false,
        pin: None,
        tid,
        table_oid: relation.rd_id,
        lp: ItemIdData::new(0, 0, 0),
    };

    let pin = BufferPin::adopt(bufmgr_seams::read_buffer::call(
        relation,
        ItemPointerGetBlockNumber(&tid),
    )?)
    .expect("ReadBuffer returned InvalidBuffer");

    let lock = pin.lock_share()?;
    let page = pin.page();

    let offnum = ItemPointerGetOffsetNumber(&tid);
    if offnum < FirstOffsetNumber || offnum > page.max_offset_number() {
        drop(lock);
        pin.release();
        return Ok(not_found);
    }
    let lp = page.item_id(offnum);
    if !lp.is_normal() {
        drop(lock);
        pin.release();
        return Ok(not_found);
    }

    // SAFETY: pinned + share-locked page, normal line pointer (page
    // invariant, item_raw_unchecked contract).
    let (ptr, len) = unsafe { page.item_raw_unchecked(lp) };
    // SAFETY: pinned + share-locked page, normal line pointer.
    let mut tuple = unsafe { HeapTupleData::from_raw_parts(ptr, len, tid, relation.rd_id) };

    let valid = hv_seam::heap_tuple_satisfies_dirty::call(&mut tuple, snapshot, pin.buffer())?;

    if valid {
        predicate_seams::predicate_lock_tid::call(relation, tid, snapshot, tuple.t_data().xmin())?;
    }
    HeapCheckForSerializableConflictOut(valid, relation, &mut tuple, pin.buffer(), snapshot)?;

    drop(lock);

    if valid || keep_buf {
        return Ok(HeapFetchResult {
            found: valid,
            pin: Some(pin),
            tid,
            table_oid: relation.rd_id,
            lp,
        });
    }
    pin.release();
    Ok(not_found)
}

pub struct HotSearchResult<'p> {
    pub found: bool,
    /// The matching member's TID on success; the input TID otherwise.
    pub tid: ItemPointerData,
    pub tuple: Option<HeapTupleData<'p>>,
    /// `Some` iff requested: whether every chain member is vacuumable.
    pub all_dead: Option<bool>,
}

pub fn heap_hot_search_buffer<'p, 'mcx>(
    tid: ItemPointerData,
    relation: &RelationData<'mcx>,
    pin: &'p BufferPin,
    snapshot: &SnapshotData<'_>,
    want_all_dead: bool,
    first_call: bool,
) -> PgResult<HotSearchResult<'p>> {
    let blkno = ItemPointerGetBlockNumber(&tid);
    let mut offnum = ItemPointerGetOffsetNumber(&tid);
    let mut at_chain_start = first_call;
    let mut skip = !first_call;
    let mut prev_xmax: TransactionId = InvalidTransactionId;
    let mut vistest: Option<GlobalVisStateHandle> = None;
    // Not first call: the previous call returned a live tuple.
    let mut all_dead = if want_all_dead {
        Some(first_call)
    } else {
        None
    };

    debug_assert!(pin.block_number() == blkno);
    let page = pin.page();
    let mut result_tid = tid;

    loop {
        if offnum < FirstOffsetNumber || offnum > page.max_offset_number() {
            break;
        }

        let lp = page.item_id(offnum);
        if !lp.is_normal() {
            // A redirect is legal only at chain start.
            if lp.is_redirected() && at_chain_start {
                offnum = lp.lp_off();
                at_chain_start = false;
                continue;
            }
            break;
        }

        // SAFETY: normal line pointer on a pinned page (page invariant).
        let (ptr, len) = unsafe { page.item_raw_unchecked(lp) };
        // SAFETY: caller holds pin + at least share lock (C contract).
        let mut heap_tuple: HeapTupleData<'p> = unsafe {
            HeapTupleData::from_raw_parts(
                ptr,
                len,
                ItemPointerData::new(blkno, offnum),
                relation.rd_id,
            )
        };

        if at_chain_start && heap_tuple.is_heap_only() {
            break;
        }

        // xmin must match the previous xmax, else the chain is broken.
        if TransactionIdIsValid(prev_xmax) && prev_xmax != heap_tuple.t_data().xmin() {
            break;
        }

        // Later passes enter at the previously returned tuple: skip or loop forever.
        if !skip {
            let valid = hv_seam::heap_tuple_satisfies_visibility::call(
                &mut heap_tuple,
                snapshot,
                pin.buffer(),
            )?;
            HeapCheckForSerializableConflictOut(
                valid,
                relation,
                &mut heap_tuple,
                pin.buffer(),
                snapshot,
            )?;

            if valid {
                ItemPointerSetOffsetNumber(&mut result_tid, offnum);
                predicate_seams::predicate_lock_tid::call(
                    relation,
                    heap_tuple.t_self,
                    snapshot,
                    heap_tuple.t_data().xmin(),
                )?;
                if want_all_dead {
                    all_dead = Some(false);
                }
                return Ok(HotSearchResult {
                    found: true,
                    tid: result_tid,
                    tuple: Some(heap_tuple),
                    all_dead,
                });
            }
        }
        skip = false;

        if all_dead == Some(true) {
            if vistest.is_none() {
                vistest = Some(procarray_seams::global_vis_test_for::call(relation));
            }
            if !hv_seam::heap_tuple_is_surely_dead::call(&heap_tuple, vistest.unwrap())? {
                all_dead = Some(false);
            }
        }

        if heap_tuple.is_hot_updated() {
            debug_assert!(ItemPointerGetBlockNumber(&heap_tuple.t_data().t_ctid) == blkno);
            offnum = ItemPointerGetOffsetNumber(&heap_tuple.t_data().t_ctid);
            at_chain_start = false;
            prev_xmax = HeapTupleHeaderGetUpdateXid(heap_tuple.t_data())?;
        } else {
            break;
        }
    }

    Ok(HotSearchResult {
        found: false,
        tid,
        tuple: None,
        all_dead,
    })
}

/// Chase `t_ctid` links to the latest version visible to `snapshot`;
/// unchanged if no version passes the test.
pub fn heap_get_latest_tid<'mcx>(
    relation: &RelationData<'mcx>,
    snapshot: &SnapshotData<'_>,
    mut tid: ItemPointerData,
) -> PgResult<ItemPointerData> {
    debug_assert!(ItemPointerIsValid(&tid));

    let mut ctid = tid;
    let mut prior_xmax: TransactionId = InvalidTransactionId; // cannot check first XMIN

    loop {
        let pin = BufferPin::adopt(bufmgr_seams::read_buffer::call(
            relation,
            ItemPointerGetBlockNumber(&ctid),
        )?)
        .expect("ReadBuffer returned InvalidBuffer");
        let lock = pin.lock_share()?;
        let page = pin.page();

        // A bogus item number off a t_ctid link keeps the prior answer.
        let offnum = ItemPointerGetOffsetNumber(&ctid);
        if offnum < FirstOffsetNumber || offnum > page.max_offset_number() {
            break;
        }
        let lp = page.item_id(offnum);
        if !lp.is_normal() {
            break;
        }

        // SAFETY: normal line pointer on a pinned page (page invariant).
        let (ptr, len) = unsafe { page.item_raw_unchecked(lp) };
        // SAFETY: pinned + share-locked page, normal line pointer.
        let mut tp = unsafe { HeapTupleData::from_raw_parts(ptr, len, ctid, relation.rd_id) };

        // A t_ctid link can land on an unrelated tuple: XMIN must match.
        if TransactionIdIsValid(prior_xmax) && prior_xmax != tp.t_data().xmin() {
            break;
        }

        let valid =
            hv_seam::heap_tuple_satisfies_visibility::call(&mut tp, snapshot, pin.buffer())?;
        HeapCheckForSerializableConflictOut(valid, relation, &mut tp, pin.buffer(), snapshot)?;
        if valid {
            tid = ctid;
        }

        let hdr = tp.t_data();
        if (hdr.t_infomask & HEAP_XMAX_INVALID) != 0
            || hv_seam::heap_tuple_header_is_only_locked::call(hdr)?
            || ItemPointerIndicatesMovedPartitions(&hdr.t_ctid)
            || ItemPointerEquals(&tp.t_self, &hdr.t_ctid)
        {
            break;
        }

        ctid = hdr.t_ctid;
        prior_xmax = HeapTupleHeaderGetUpdateXid(hdr)?;
        drop(lock);
        pin.release();
    }

    Ok(tid)
}
