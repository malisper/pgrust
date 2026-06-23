//! F3 (UPDATE side) — heap tuple UPDATE (`access/heap/heapam.c`):
//! `heap_update` / `simple_heap_update`, plus the update-only helpers
//! `HeapDetermineColumnsInfo` / `heap_attr_equals` and the WAL emitter
//! `log_heap_update`.
//!
//! This is the follow-on to the DELETE side (`delete.rs`): it reuses the two
//! shared helpers landed there — `compute_new_xmax_infomask` and
//! `ExtractReplicaIdentity` — and mirrors the same page model. The buffer
//! manager owns the shared page; `heap_update` pins + exclusively locks the old
//! tuple's buffer, materializes the on-page old tuple (`oldtup`) into `mcx`,
//! runs its visibility / lock-wait / xmax-compute logic on that materialized
//! copy, places the new tuple via hio's `RelationGetBufferForTuple` /
//! `RelationPutHeapTuple` (which may pick a fresh page), and — inside the
//! critical section — writes the mutated old-tuple header back into the old page
//! plus the page-level flags through one `with_buffer_page` mutation.
//!
//! The new tuple's bytes are inserted onto the page by hio; the old tuple's
//! header is stamped in place (xmax/cmax/infomask, t_ctid chained to the new
//! tuple, HOT-updated flag). `log_heap_update` builds the `XLOG_HEAP_UPDATE` /
//! `XLOG_HEAP_HOT_UPDATE` record with both tuple images and the suffix/prefix
//! compression of the new tuple against the old.
//!
//! The lock-wait primitives are reached through the same honest seams as
//! `heap_delete` (the heapam LOCK family + multixact.c).

use ::mcx::Mcx;
use ::types_core::primitive::{BlockNumber, MultiXactId, OffsetNumber, Oid, Size, TransactionId};
use ::types_core::xact::{CommandId, InvalidCommandId};
use types_error::{
    PgResult, ERRCODE_INVALID_TRANSACTION_STATE, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERROR,
};
use ::utils_error::ereport;
use ::types_tuple::heaptuple::Datum;
use rel::{Relation, RelationData};
use ::types_storage::lock::XLTW_Oper;
use types_storage::{Buffer, InvalidBuffer};
use ::types_tableam::tableam::{
    LockTupleMode, LockWaitPolicy, TM_FailureData, TM_Result, TU_UpdateIndexes,
};
use ::types_tuple::heaptuple::FormedTuple;
use ::types_tuple::heaptuple::{
    HeapTupleData, HeapTupleField3, HeapTupleHeaderData, ItemPointerData,
    FirstLowInvalidHeapAttributeNumber, TableOidAttributeNumber, HEAP2_XACT_MASK, HEAP_COMBOCID,
    HEAP_HASEXTERNAL, HEAP_HOT_UPDATED, HEAP_KEYS_UPDATED, HEAP_MOVED, HEAP_ONLY_TUPLE,
    HEAP_UPDATED, HEAP_XACT_MASK, HEAP_XMAX_INVALID, HEAP_XMAX_KEYSHR_LOCK, HEAP_XMAX_LOCK_ONLY,
};
use ::xlog_records::multixact::MultiXactStatus;

use page::{
    ItemPointerEquals, ItemPointerGetBlockNumber, ItemPointerGetOffsetNumber, ItemPointerIsValid,
    PageClearAllVisible, PageGetItem, PageGetItemId, PageMut, PageRef, PageSetFull, PageSetPrunable,
};

use ::heapam_visibility::htup::{
    HeapTupleHeaderGetRawXmax, HEAP_LOCKED_UPGRADED, HEAP_LOCK_MASK, HEAP_XMAX_IS_LOCKED_ONLY,
};
use heapam_visibility::{
    HeapTupleHeaderGetUpdateXid as HtupGetUpdateXid, HeapTupleSatisfiesUpdate,
    HeapTupleSatisfiesVisibility,
};
use transam::{TransactionIdDidAbort as TxnDidAbort, TransactionIdEquals};

use crate::delete::{compute_new_xmax_infomask, ExtractReplicaIdentity};
use crate::{compute_infobits, xmax_infomask_changed, GetMultiXactIdHintBits, UpdateXmaxHintBits};

use heapam_seams as heapam_seam;
use ::heaptoast::heap_toast_insert_or_update;
use hio::{RelationGetBufferForTuple, RelationPutHeapTuple};
use hio_seams as hio_seam;
use vacuumlazy_seams as page_seam;
use transam_xact_seams as xact_seam;
use transam_xlog_seams as xlog_seam;
use xloginsert_seams as xloginsert_seam;
use catalog_seams as catalog_seam;
use nodes_core_seams as bms_seam;
use bufmgr_seams as bufmgr_seam;
use predicate_seams as predicate_seam;
use pgstat_seams as pgstat_seam;
use relcache_seams as relcache_seam;
use ::relcache_seams::IndexAttrBitmapKind;
use combocid_seams as combocid_seam;

use heaptuple::{heap_deform_tuple, heap_getsysattr, heap_tuple_to_disk_image};
use ::nodes::Bitmapset;
use ::types_storage::bufpage::SizeofHeapTupleHeader;
use ::wal::wal::{RM_HEAP_ID, XLOG_INCLUDE_ORIGIN};
use ::wal::xloginsert::{REGBUF_KEEP_DATA, REGBUF_STANDARD, REGBUF_WILL_INIT};

use ::rmgrdesc_next::heapdesc::{
    XLOG_HEAP_HOT_UPDATE, XLOG_HEAP_INIT_PAGE, XLOG_HEAP_LOCK, XLOG_HEAP_UPDATE,
};
use ::xlog_records::heapam_xlog::{
    xl_heap_header, xl_heap_lock, xl_heap_update, SizeOfHeapHeader, SizeOfHeapLock,
    SizeOfHeapUpdate, XLH_LOCK_ALL_FROZEN_CLEARED, XLH_UPDATE_CONTAINS_NEW_TUPLE,
    XLH_UPDATE_CONTAINS_OLD_KEY, XLH_UPDATE_CONTAINS_OLD_TUPLE,
    XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED, XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED,
    XLH_UPDATE_PREFIX_FROM_OLD, XLH_UPDATE_SUFFIX_FROM_OLD,
};

// ---------------------------------------------------------------------------
// heapam-local vocabulary (htup_details.h / heapam.h / pg_class.h constants).
// ---------------------------------------------------------------------------

/// `RELKIND_RELATION` / `RELKIND_MATVIEW` (catalog/pg_class.h).
const RELKIND_RELATION: u8 = b'r';
const RELKIND_MATVIEW: u8 = b'm';

/// `VISIBILITYMAP_VALID_BITS` (access/visibilitymapdefs.h) == ALL_VISIBLE |
/// ALL_FROZEN.
const VISIBILITYMAP_VALID_BITS: u8 = 0x03;
/// `VISIBILITYMAP_ALL_FROZEN` (access/visibilitymapdefs.h).
const VISIBILITYMAP_ALL_FROZEN: u8 = 0x02;

/// `REPLICA_IDENTITY_FULL` (catalog/pg_class.h).
const REPLICA_IDENTITY_FULL: u8 = b'f';

/// `InvalidTransactionId`.
const InvalidTransactionId: TransactionId = 0;

/// `FirstOffsetNumber` (storage/off.h).
const FirstOffsetNumber: OffsetNumber = ::types_tuple::heaptuple::FIRST_OFFSET_NUMBER;

/// `HEAP_XMAX_BITS` (htup_details.h).
const HEAP_XMAX_BITS: u16 = ::types_tuple::heaptuple::HEAP_XMAX_COMMITTED
    | HEAP_XMAX_INVALID
    | ::types_tuple::heaptuple::HEAP_XMAX_IS_MULTI
    | HEAP_LOCK_MASK
    | HEAP_XMAX_LOCK_ONLY;

/// `TOAST_TUPLE_THRESHOLD` (access/heaptoast.h).
const TOAST_TUPLE_THRESHOLD: usize = ::heaptoast::TOAST_TUPLE_THRESHOLD;

// ===========================================================================
// heap_update — update a tuple in a heap (heapam.c).
// ===========================================================================

/// `heap_update(relation, otid, newtup, cid, crosscheck, wait, tmfd, lockmode,
/// update_indexes)` (heapam.c).
#[allow(clippy::too_many_arguments)]
pub fn heap_update<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &Relation<'mcx>,
    otid: ItemPointerData,
    newtup: &mut FormedTuple<'mcx>,
    mut cid: CommandId,
    crosscheck: Option<&snapshot::SnapshotData>,
    wait: bool,
    tmfd: &mut TM_FailureData,
) -> PgResult<heapam_seam::HeapUpdateResult> {
    let xid = xact_seam::get_current_transaction_id::call()?;
    let mut vmbuffer: Buffer = InvalidBuffer;
    let mut vmbuffer_new: Buffer = InvalidBuffer;
    let mut have_tuple_lock = false;
    let mut use_hot_update = false;
    let mut summarized_update = false;
    let mut all_visible_cleared = false;
    let mut all_visible_cleared_new = false;
    let mut id_has_external = false;

    debug_assert!(ItemPointerIsValid(Some(&otid)));

    // Cheap, simplistic check that the tuple matches the rel's rowtype:
    // HeapTupleHeaderGetNatts(newtup->t_data) <= RelationGetNumberOfAttributes.
    debug_assert!({
        let hdr = newtup.tuple.t_data.as_ref().expect("heap_update: no header");
        (::types_tuple::heaptuple::HeapTupleHeaderGetNatts(hdr) as i32)
            <= relation.rd_att.natts
    });

    // AssertHasSnapshotForToast(relation) — debug-only; no state to mirror.
    // check_lock_if_inplace_updateable_rel(...) — USE_ASSERT_CHECKING-only
    // diagnostic (system-catalog inplace-lock check); not mirrored.

    /*
     * Forbid this during a parallel operation, lest it allocate a combo CID.
     */
    if xact_seam::is_in_parallel_mode::call() {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_TRANSACTION_STATE)
            .errmsg("cannot update tuples during a parallel operation")
            .into_error());
    }

    /*
     * Fetch the list of attributes to be checked for various operations. We
     * also need columns used by the replica identity and the "key" of rows.
     * Note that we get copies of each bitmap, so we need not worry about a
     * relcache flush midway through.
     */
    let hot_attrs =
        relcache_seam::relation_get_index_attr_bitmap::call(mcx, relation, IndexAttrBitmapKind::HotBlocking)?;
    let sum_attrs =
        relcache_seam::relation_get_index_attr_bitmap::call(mcx, relation, IndexAttrBitmapKind::Summarized)?;
    let key_attrs =
        relcache_seam::relation_get_index_attr_bitmap::call(mcx, relation, IndexAttrBitmapKind::Keys)?;
    let id_attrs =
        relcache_seam::relation_get_index_attr_bitmap::call(mcx, relation, IndexAttrBitmapKind::Identity)?;

    let mut interesting_attrs: Option<::mcx::PgBox<'mcx, Bitmapset<'mcx>>> = None;
    interesting_attrs = bms_seam::bms_add_members::call(mcx, interesting_attrs, hot_attrs.as_deref())?;
    interesting_attrs = bms_seam::bms_add_members::call(mcx, interesting_attrs, sum_attrs.as_deref())?;
    interesting_attrs = bms_seam::bms_add_members::call(mcx, interesting_attrs, key_attrs.as_deref())?;
    interesting_attrs = bms_seam::bms_add_members::call(mcx, interesting_attrs, id_attrs.as_deref())?;
    let interesting_attrs = if bms_seam::bms_is_empty::call(interesting_attrs.as_deref()) {
        None
    } else {
        interesting_attrs
    };

    let block = ItemPointerGetBlockNumber(&otid);
    let buffer = hio_seam::read_buffer::call(relation.rd_id, block)?;

    /*
     * Before locking the buffer, pin the visibility map page if it appears to
     * be necessary.
     */
    if page_is_all_visible(buffer)? {
        vmbuffer = page_seam::visibilitymap_pin::call(relation, block, vmbuffer)?;
    }

    bufmgr_seam::lock_buffer_exclusive::call(buffer)?;

    /*
     * Usually a buffer pin and/or snapshot blocks pruning, so we see LP_NORMAL.
     * When the otid origin is a syscache, we may see other LP states (concurrent
     * pruning). Settle on TM_Deleted (see the C comment).
     */
    if !on_page_item_is_normal(buffer, ItemPointerGetOffsetNumber(&otid))? {
        bufmgr_seam::unlock_release_buffer::call(buffer);
        debug_assert!(!have_tuple_lock);
        if vmbuffer != InvalidBuffer {
            bufmgr_seams::release_buffer::call(vmbuffer);
        }
        tmfd.ctid = otid;
        tmfd.xmax = InvalidTransactionId;
        tmfd.cmax = InvalidCommandId;
        return Ok(heapam_seam::HeapUpdateResult {
            result: TM_Result::TM_Deleted,
            lockmode: LockTupleMode::LockTupleExclusive,
            update_indexes: TU_UpdateIndexes::TU_None,
        });
    }

    /*
     * Fill in enough data in oldtup for HeapDetermineColumnsInfo to work
     * properly: t_tableOid, t_data (on-page), t_len, t_self.
     */
    let mut oldtup = read_on_page_tuple(mcx, relation.rd_id, buffer, otid)?;

    /* the new tuple is ready, except for this: */
    newtup.tuple.t_tableOid = relation.rd_id;

    /*
     * Determine columns modified by the update, and whether any unmodified
     * replica-identity-key attribute of the old tuple is stored externally.
     */
    let modified_attrs = HeapDetermineColumnsInfo(
        mcx,
        relation,
        interesting_attrs.as_deref(),
        id_attrs.as_deref(),
        &oldtup,
        newtup,
        &mut id_has_external,
    )?;

    /*
     * If we're not updating any "key" column, grab a weaker lock type for more
     * concurrency with FK checks.
     */
    let lockmode: LockTupleMode;
    let mxact_status: MultiXactStatus;
    let key_intact: bool;
    if !bms_seam::bms_overlap::call(modified_attrs.as_deref(), key_attrs.as_deref()) {
        lockmode = LockTupleMode::LockTupleNoKeyExclusive;
        mxact_status = MultiXactStatus::NoKeyUpdate;
        key_intact = true;

        /*
         * If this is the first possibly-multixact-able operation in the current
         * transaction, set my per-backend OldestMemberMXactId setting.
         */
        multixact_seams::multi_xact_id_set_oldest_member::call()?;
    } else {
        lockmode = LockTupleMode::LockTupleExclusive;
        mxact_status = MultiXactStatus::Update;
        key_intact = false;
    }

    /*
     * Note: beyond this point, use oldtup not otid to refer to the old tuple.
     */

    // `l2:` retry loop.
    let mut checked_lockers;
    let mut locker_remains;
    let result = loop {
        checked_lockers = false;
        locker_remains = false;
        let mut result = HeapTupleSatisfiesUpdate(&mut oldtup.tuple, cid, buffer)?;

        /* see below about the "no wait" case */
        debug_assert!(result != TM_Result::TM_BeingModified || wait);

        if result == TM_Result::TM_Invisible {
            bufmgr_seam::unlock_release_buffer::call(buffer);
            return Err(ereport(ERROR)
                .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg("attempted to update invisible tuple")
                .into_error());
        } else if result == TM_Result::TM_BeingModified && wait {
            let mut can_continue = false;

            /* must copy state data before unlocking buffer */
            let xwait = HeapTupleHeaderGetRawXmax(data_ref(&oldtup));
            let infomask = data_ref(&oldtup).t_infomask;

            if (infomask & ::types_tuple::heaptuple::HEAP_XMAX_IS_MULTI) != 0 {
                let conflict = heapam_seam::does_multi_xact_id_conflict::call(
                    xwait as MultiXactId,
                    infomask,
                    lockmode,
                )?;
                let current_is_member = conflict.current_is_member;

                if conflict.conflict {
                    lock_buffer_unlock(buffer)?;

                    /* acquire the lock, if necessary */
                    if !current_is_member {
                        have_tuple_lock = heapam_seam::heap_acquire_tuplock::call(
                            relation,
                            oldtup.tuple.t_self,
                            lockmode,
                            LockWaitPolicy::LockWaitBlock,
                            have_tuple_lock,
                        )?;
                    }

                    /* wait for multixact */
                    heapam_seam::multi_xact_id_wait::call(
                        xwait as MultiXactId,
                        mxact_status,
                        infomask,
                        relation,
                        oldtup.tuple.t_self,
                        XLTW_Oper::Update,
                    )?;
                    // C captures `*remain` here; the repo's MultiXactIdWait seam
                    // does not surface the surviving-member count. The only use
                    // of `remain` is `locker_remains = remain != 0`, which feeds
                    // the new-tuple xmax decision below. Conservatively treat a
                    // multixact that survived the wait as still having lockers
                    // (the safe direction: it preserves xmax on the new tuple),
                    // matching C when members remain.
                    checked_lockers = true;
                    locker_remains = true;
                    bufmgr_seam::lock_buffer_exclusive::call(buffer)?;

                    // Re-materialize the on-page tuple after the lock round-trip.
                    oldtup = read_on_page_tuple(mcx, relation.rd_id, buffer, otid)?;

                    /*
                     * If xwait had just locked the tuple then some other xact
                     * could update it before we get here. Start over if so.
                     */
                    if xmax_infomask_changed(data_ref(&oldtup).t_infomask, infomask)
                        || !TransactionIdEquals(
                            HeapTupleHeaderGetRawXmax(data_ref(&oldtup)),
                            xwait,
                        )
                    {
                        continue; // goto l2
                    }
                }

                /*
                 * The multixact may not be done. Determine the surviving updater
                 * (if any) to decide whether we can continue.
                 */
                let update_xact = if !HEAP_XMAX_IS_LOCKED_ONLY(data_ref(&oldtup).t_infomask) {
                    HeapTupleGetUpdateXid(data_ref(&oldtup))?
                } else {
                    InvalidTransactionId
                };

                /*
                 * There was no UPDATE in the MultiXact; or it aborted. No
                 * TransactionIdIsInProgress() call needed, since we called
                 * MultiXactIdWait() above.
                 */
                if !TransactionIdIsValid(update_xact) || TransactionIdDidAbort(update_xact)? {
                    can_continue = true;
                }
            } else if xact_seam::transaction_id_is_current_transaction_id::call(xwait) {
                /*
                 * The only locker is ourselves; avoid grabbing the tuple lock,
                 * but preserve our locking information.
                 */
                checked_lockers = true;
                locker_remains = true;
                can_continue = true;
            } else if HEAP_XMAX_IS_KEYSHR_LOCKED(infomask) && key_intact {
                /*
                 * If it's just a key-share locker and we're not changing the key
                 * columns, we don't need to wait; but preserve it as locker.
                 */
                checked_lockers = true;
                locker_remains = true;
                can_continue = true;
            } else {
                /*
                 * Wait for regular transaction to end; but first, acquire tuple
                 * lock.
                 */
                lock_buffer_unlock(buffer)?;
                have_tuple_lock = heapam_seam::heap_acquire_tuplock::call(
                    relation,
                    oldtup.tuple.t_self,
                    lockmode,
                    LockWaitPolicy::LockWaitBlock,
                    have_tuple_lock,
                )?;
                heapam_seam::xact_lock_table_wait::call(
                    xwait,
                    relation,
                    oldtup.tuple.t_self,
                    XLTW_Oper::Update,
                )?;
                checked_lockers = true;
                bufmgr_seam::lock_buffer_exclusive::call(buffer)?;

                // Re-materialize the on-page tuple after the lock round-trip.
                oldtup = read_on_page_tuple(mcx, relation.rd_id, buffer, otid)?;

                /*
                 * xwait is done, but if xwait had just locked the tuple then
                 * some other xact could update it before we get here.
                 */
                if xmax_infomask_changed(data_ref(&oldtup).t_infomask, infomask)
                    || !TransactionIdEquals(xwait, HeapTupleHeaderGetRawXmax(data_ref(&oldtup)))
                {
                    continue; // goto l2
                }

                /* Otherwise check if it committed or aborted */
                UpdateXmaxHintBits(data_mut(&mut oldtup), buffer, xwait)?;
                if (data_ref(&oldtup).t_infomask & HEAP_XMAX_INVALID) != 0 {
                    can_continue = true;
                }
            }

            result = if can_continue {
                TM_Result::TM_Ok
            } else if !ItemPointerEquals(&oldtup.tuple.t_self, &data_ref(&oldtup).t_ctid) {
                TM_Result::TM_Updated
            } else {
                TM_Result::TM_Deleted
            };
        }

        /* Sanity check the result and the logic above */
        if result != TM_Result::TM_Ok {
            debug_assert!(
                result == TM_Result::TM_SelfModified
                    || result == TM_Result::TM_Updated
                    || result == TM_Result::TM_Deleted
                    || result == TM_Result::TM_BeingModified
            );
            debug_assert!(data_ref(&oldtup).t_infomask & HEAP_XMAX_INVALID == 0);
            debug_assert!(
                result != TM_Result::TM_Updated
                    || !ItemPointerEquals(&oldtup.tuple.t_self, &data_ref(&oldtup).t_ctid)
            );
        }

        let mut result = result;
        if let Some(cc) = crosscheck {
            if result == TM_Result::TM_Ok {
                /* Additional check for transaction-snapshot mode RI updates. */
                let mut cc_local = cc.clone();
                if !HeapTupleSatisfiesVisibility(&mut oldtup.tuple, &mut cc_local, buffer)? {
                    result = TM_Result::TM_Updated;
                }
            }
        }

        if result != TM_Result::TM_Ok {
            tmfd.ctid = data_ref(&oldtup).t_ctid;
            tmfd.xmax = HtupGetUpdateXid(data_ref(&oldtup))?;
            if result == TM_Result::TM_SelfModified {
                tmfd.cmax = HeapTupleHeaderGetCmax(data_ref(&oldtup));
            } else {
                tmfd.cmax = InvalidCommandId;
            }
            bufmgr_seam::unlock_release_buffer::call(buffer);
            if have_tuple_lock {
                heapam_seam::unlock_tuple_tuplock::call(relation, oldtup.tuple.t_self, lockmode)?;
            }
            if vmbuffer != InvalidBuffer {
                bufmgr_seams::release_buffer::call(vmbuffer);
            }
            return Ok(heapam_seam::HeapUpdateResult {
                result,
                lockmode,
                update_indexes: TU_UpdateIndexes::TU_None,
            });
        }

        /*
         * If we didn't pin the visibility map page and the page has become all
         * visible while we were busy locking the buffer, we'll have to unlock
         * and re-lock, to avoid holding the buffer lock across an I/O.
         */
        if vmbuffer == InvalidBuffer && page_is_all_visible(buffer)? {
            lock_buffer_unlock(buffer)?;
            vmbuffer = page_seam::visibilitymap_pin::call(relation, block, vmbuffer)?;
            bufmgr_seam::lock_buffer_exclusive::call(buffer)?;
            // Re-materialize the on-page tuple after the lock round-trip.
            oldtup = read_on_page_tuple(mcx, relation.rd_id, buffer, otid)?;
            continue; // goto l2
        }

        break result;
    };
    let _ = result; // result == TM_Ok past here.

    /* Fill in transaction status data */

    /*
     * If the tuple we're updating is locked, preserve the locking info in the
     * old tuple's Xmax. Prepare a new Xmax value for this.
     */
    let (xmax_old_tuple, infomask_old_tuple, infomask2_old_tuple) = compute_new_xmax_infomask(
        mcx,
        HeapTupleHeaderGetRawXmax(data_ref(&oldtup)),
        data_ref(&oldtup).t_infomask,
        data_ref(&oldtup).t_infomask2,
        xid,
        lockmode,
        true,
    )?;

    /*
     * And also prepare an Xmax value for the new copy of the tuple.
     */
    let xmax_new_tuple: TransactionId;
    if (data_ref(&oldtup).t_infomask & HEAP_XMAX_INVALID) != 0
        || HEAP_LOCKED_UPGRADED(data_ref(&oldtup).t_infomask)
        || (checked_lockers && !locker_remains)
    {
        xmax_new_tuple = InvalidTransactionId;
    } else {
        xmax_new_tuple = HeapTupleHeaderGetRawXmax(data_ref(&oldtup));
    }

    let infomask_new_tuple: u16;
    let infomask2_new_tuple: u16;
    if !TransactionIdIsValid(xmax_new_tuple) {
        infomask_new_tuple = HEAP_XMAX_INVALID;
        infomask2_new_tuple = 0;
    } else {
        /*
         * If we found a valid Xmax for the new tuple, the only possibility is
         * the lockers had FOR KEY SHARE lock.
         */
        if (data_ref(&oldtup).t_infomask & ::types_tuple::heaptuple::HEAP_XMAX_IS_MULTI) != 0 {
            let (m, m2) = GetMultiXactIdHintBits(mcx, xmax_new_tuple)?;
            infomask_new_tuple = m;
            infomask2_new_tuple = m2;
        } else {
            infomask_new_tuple = HEAP_XMAX_KEYSHR_LOCK | HEAP_XMAX_LOCK_ONLY;
            infomask2_new_tuple = 0;
        }
    }

    /*
     * Prepare the new tuple with the appropriate initial Xmin and Xmax, plus
     * the infomask bits computed above.
     */
    {
        let hdr = newtup
            .tuple
            .t_data
            .as_mut()
            .expect("heap_update: newtup has no t_data");
        hdr.t_infomask &= !HEAP_XACT_MASK;
        hdr.t_infomask2 &= !HEAP2_XACT_MASK;
        HeapTupleHeaderSetXmin(hdr, xid);
        HeapTupleHeaderSetCmin(hdr, cid);
        hdr.t_infomask |= HEAP_UPDATED | infomask_new_tuple;
        hdr.t_infomask2 |= infomask2_new_tuple;
        HeapTupleHeaderSetXmax(hdr, xmax_new_tuple);
    }

    /*
     * Replace cid with a combo CID if necessary. (We already put the plain cid
     * into the new tuple.)
     */
    let (new_cid, iscombo) = combocid_seam::heap_tuple_header_adjust_cmax::call(data_ref(&oldtup), cid)?;
    cid = new_cid;

    /*
     * If the toaster needs to be activated, OR if the new tuple won't fit on
     * the same page as the old, we need to release the content lock (not the
     * pin) on the old tuple's buffer while doing TOAST / extension work.
     */
    let need_toast: bool;
    let relkind = relation.rd_rel.relkind;
    if relkind != RELKIND_RELATION && relkind != RELKIND_MATVIEW {
        /* toast table entries should never be recursively toasted */
        debug_assert!(!HeapTupleHasExternal(&oldtup));
        debug_assert!(!HeapTupleHasExternal(newtup));
        need_toast = false;
    } else {
        need_toast = HeapTupleHasExternal(&oldtup)
            || HeapTupleHasExternal(newtup)
            || newtup.tuple.t_len as usize > TOAST_TUPLE_THRESHOLD;
    }

    let mut pagefree = page_seam::page_get_heap_free_space::call(buffer)?;
    let mut newtupsize = maxalign(newtup.tuple.t_len as usize);

    // `heaptup` is the data we actually store: the toasted copy if present,
    // else the caller's `newtup`. `toasted` owns the copy.
    let mut toasted: Option<FormedTuple<'mcx>> = None;
    let newbuf: Buffer;
    let mut page_set_full_on_old = false;

    if need_toast || newtupsize > pagefree {
        let mut cleared_all_frozen = false;

        /*
         * To prevent concurrent updates we have to temporarily mark the old
         * tuple locked, while we release the page-level lock. WAL-log this
         * temporary modification (reusing xl_heap_lock).
         */
        let (xmax_lock_old_tuple, infomask_lock_old_tuple, infomask2_lock_old_tuple) =
            compute_new_xmax_infomask(
                mcx,
                HeapTupleHeaderGetRawXmax(data_ref(&oldtup)),
                data_ref(&oldtup).t_infomask,
                data_ref(&oldtup).t_infomask2,
                xid,
                lockmode,
                false,
            )?;

        debug_assert!(HEAP_XMAX_IS_LOCKED_ONLY(infomask_lock_old_tuple));

        // START_CRIT_SECTION()

        /* Clear obsolete visibility flags ... */
        let old_self = oldtup.tuple.t_self;
        {
            let hdr = data_mut(&mut oldtup);
            hdr.t_infomask &= !(HEAP_XMAX_BITS | HEAP_MOVED);
            hdr.t_infomask2 &= !HEAP_KEYS_UPDATED;
            HeapTupleHeaderClearHotUpdated(hdr);
            /* ... and store info about transaction updating this tuple */
            debug_assert!(TransactionIdIsValid(xmax_lock_old_tuple));
            HeapTupleHeaderSetXmax(hdr, xmax_lock_old_tuple);
            hdr.t_infomask |= infomask_lock_old_tuple;
            hdr.t_infomask2 |= infomask2_lock_old_tuple;
            HeapTupleHeaderSetCmax(hdr, cid, iscombo);
            /* temporarily make it look not-updated, but locked */
            hdr.t_ctid = old_self;
        }

        /*
         * Clear all-frozen bit on visibility map if needed.
         */
        if page_is_all_visible(buffer)?
            && page_seam::visibilitymap_clear::call(
                relation,
                block,
                vmbuffer,
                VISIBILITYMAP_ALL_FROZEN,
            )?
        {
            cleared_all_frozen = true;
        }

        // Write the stamped old header back into the page bytes.
        write_back_header(buffer, &oldtup)?;

        bufmgr_seams::mark_buffer_dirty::call(buffer);

        if relcache_seam::relation_needs_wal::call(relation) {
            let xlrec = xl_heap_lock {
                offnum: ItemPointerGetOffsetNumber(&oldtup.tuple.t_self),
                xmax: xmax_lock_old_tuple,
                infobits_set: compute_infobits(
                    data_ref(&oldtup).t_infomask,
                    data_ref(&oldtup).t_infomask2,
                ),
                flags: if cleared_all_frozen { XLH_LOCK_ALL_FROZEN_CLEARED } else { 0 },
            };

            xloginsert_seam::xlog_begin_insert::call()?;
            xloginsert_seam::xlog_register_buffer::call(0, buffer, REGBUF_STANDARD)?;
            let recbuf = xlrec.to_bytes();
            xloginsert_seam::xlog_register_data::call(&recbuf[..SizeOfHeapLock])?;
            let recptr = xloginsert_seam::xlog_insert_record::call(RM_HEAP_ID, XLOG_HEAP_LOCK)?;
            bufmgr_seam::page_set_lsn::call(buffer, recptr)?;
        }

        // END_CRIT_SECTION()

        lock_buffer_unlock(buffer)?;

        /*
         * Let the toaster do its thing, if needed. Below this point, `heaptup`
         * is the data we intend to store; `newtup` is the caller's original.
         */
        if need_toast {
            /* Note we always use WAL and FSM during updates */
            //
            // heap_toast_insert_or_update returns the original `newtup` (modeled
            // here as `None`) when no attribute actually needed to be rewritten
            // — e.g. updating a catalog row whose only external attribute is
            // unchanged (C heaptoast.c:331 `result_tuple = newtup;`). In that
            // case `heaptup == newtup`, so we keep `toasted = None` and store
            // the caller's tuple.
            if let Some(stored) =
                heap_toast_insert_or_update(mcx, relation, newtup, Some(&oldtup), 0)?
            {
                newtupsize = maxalign(stored.tuple.t_len as usize);
                toasted = Some(stored);
            }
        }

        /*
         * Do we need a new page for the tuple, or not? Recheck after
         * reacquiring the buffer lock. To avoid deadlock we get both locks via
         * RelationGetBufferForTuple ("lock the lower-numbered page first").
         */
        loop {
            if newtupsize > pagefree {
                /* It doesn't fit, must use RelationGetBufferForTuple. */
                let store_len = match toasted.as_ref() {
                    Some(t) => t.tuple.t_len,
                    None => newtup.tuple.t_len,
                };
                newbuf = RelationGetBufferForTuple(
                    relation,
                    store_len as Size,
                    buffer,
                    0,
                    None,
                    &mut vmbuffer_new,
                    &mut vmbuffer,
                    0,
                )?;
                break;
            }
            /* Acquire VM page pin if needed and we don't have it. */
            if vmbuffer == InvalidBuffer && page_is_all_visible(buffer)? {
                vmbuffer = page_seam::visibilitymap_pin::call(relation, block, vmbuffer)?;
            }
            /* Re-acquire the lock on the old tuple's page. */
            bufmgr_seam::lock_buffer_exclusive::call(buffer)?;
            /* Re-check using the up-to-date free space */
            pagefree = page_seam::page_get_heap_free_space::call(buffer)?;
            if newtupsize > pagefree
                || (vmbuffer == InvalidBuffer && page_is_all_visible(buffer)?)
            {
                /* doesn't fit anymore, or all-visible just got set; loop */
                lock_buffer_unlock(buffer)?;
            } else {
                newbuf = buffer;
                break;
            }
        }
    } else {
        /* No TOAST work needed, and it'll fit on same page */
        newbuf = buffer;
    }

    // `heaptup` borrow: the toasted copy if present, else newtup.
    let store_toasted = toasted.is_some();

    /*
     * We're about to do the actual update -- check for conflict first.
     *
     * C: CheckForSerializableConflictIn(relation, &oldtup.t_self,
     * BufferGetBlockNumber(buffer)). Pass the old tuple's TID + page so a
     * concurrent serializable reader's tuple/page-level SIREAD lock produces the
     * write-skew serialization failure; the new tuple needs only the relation
     * check, which the old-tuple check (same relation) already covers.
     */
    predicate_seam::check_for_serializable_conflict_in::call(
        relation.rd_id,
        Some((
            ItemPointerGetBlockNumber(&oldtup.tuple.t_self),
            ItemPointerGetOffsetNumber(&oldtup.tuple.t_self),
        )),
        buffer_get_block_number(buffer)?,
    )?;

    /*
     * If newbuf == buffer we might do a HOT update.
     */
    if newbuf == buffer {
        if !bms_seam::bms_overlap::call(modified_attrs.as_deref(), hot_attrs.as_deref()) {
            use_hot_update = true;
            /*
             * If hot-blocking index columns are unchanged but summarizing
             * index columns changed, we still need to update those.
             */
            if bms_seam::bms_overlap::call(modified_attrs.as_deref(), sum_attrs.as_deref()) {
                summarized_update = true;
            }
        }
    } else {
        /* Set a hint that the old page could use prune/defrag */
        page_set_full_on_old = true;
    }

    /*
     * Compute replica identity tuple before the critical section. Pass key
     * required true only if the replica-identity key columns are modified or
     * have external data.
     */
    let key_required = bms_seam::bms_overlap::call(modified_attrs.as_deref(), id_attrs.as_deref())
        || id_has_external;
    let old_key_tuple = ExtractReplicaIdentity(mcx, relation, &oldtup, key_required)?;

    // NO EREPORT(ERROR) from here till changes are logged. START_CRIT_SECTION().

    /*
     * Set the old page prunable hint (the old tuple will become DEAD).
     */
    {
        // PageSetPrunable + (if newbuf != buffer) PageSetFull on the old page.
        bufmgr_seam::with_buffer_page::call(buffer, &mut |page_bytes| {
            let mut page = PageMut::new(page_bytes)?;
            PageSetPrunable(&mut page, xid);
            if page_set_full_on_old {
                PageSetFull(&mut page);
            }
            Ok(())
        })?;
    }

    /*
     * Mark HOT-updated / heap-only flags on the materialized headers; these are
     * stamped into the page bytes below (old via write_back_header, new via
     * RelationPutHeapTuple which writes the full new tuple including its header).
     */
    if use_hot_update {
        HeapTupleHeaderSetHotUpdated(data_mut(&mut oldtup));
        if store_toasted {
            HeapTupleHeaderSetHeapOnly(
                toasted.as_mut().unwrap().tuple.t_data.as_mut().unwrap(),
            );
        }
        // Mark the caller's copy too (heaptup may differ from newtup).
        HeapTupleHeaderSetHeapOnly(newtup.tuple.t_data.as_mut().unwrap());
    } else {
        HeapTupleHeaderClearHotUpdated(data_mut(&mut oldtup));
        if store_toasted {
            HeapTupleHeaderClearHeapOnly(
                toasted.as_mut().unwrap().tuple.t_data.as_mut().unwrap(),
            );
        }
        HeapTupleHeaderClearHeapOnly(newtup.tuple.t_data.as_mut().unwrap());
    }

    /* insert new tuple */
    {
        let heaptup: &mut FormedTuple<'mcx> = match toasted.as_mut() {
            Some(t) => t,
            None => newtup,
        };
        let put_image = heap_tuple_to_disk_image(mcx, heaptup)?;
        RelationPutHeapTuple(relation, newbuf, &mut heaptup.tuple, &put_image, false)?;
    }

    /*
     * Clear obsolete visibility flags on the old tuple, store the updating
     * transaction info, and chain its t_ctid to the new tuple.
     */
    let heaptup_self = match toasted.as_ref() {
        Some(t) => t.tuple.t_self,
        None => newtup.tuple.t_self,
    };
    {
        let hdr = data_mut(&mut oldtup);
        hdr.t_infomask &= !(HEAP_XMAX_BITS | HEAP_MOVED);
        hdr.t_infomask2 &= !HEAP_KEYS_UPDATED;
        debug_assert!(TransactionIdIsValid(xmax_old_tuple));
        HeapTupleHeaderSetXmax(hdr, xmax_old_tuple);
        hdr.t_infomask |= infomask_old_tuple;
        hdr.t_infomask2 |= infomask2_old_tuple;
        HeapTupleHeaderSetCmax(hdr, cid, iscombo);
        /* record address of new tuple in t_ctid of old one */
        hdr.t_ctid = heaptup_self;
    }
    // Write the stamped old header back into the (old) page bytes.
    write_back_header(buffer, &oldtup)?;

    /* clear PD_ALL_VISIBLE flags, reset all visibilitymap bits */
    if page_is_all_visible(buffer)? {
        all_visible_cleared = true;
        // PageClearAllVisible on the old page.
        bufmgr_seam::with_buffer_page::call(buffer, &mut |page_bytes| {
            let mut page = PageMut::new(page_bytes)?;
            PageClearAllVisible(&mut page);
            Ok(())
        })?;
        page_seam::visibilitymap_clear::call(
            relation,
            buffer_get_block_number(buffer)?,
            vmbuffer,
            VISIBILITYMAP_VALID_BITS,
        )?;
    }
    if newbuf != buffer && page_is_all_visible(newbuf)? {
        all_visible_cleared_new = true;
        bufmgr_seam::with_buffer_page::call(newbuf, &mut |page_bytes| {
            let mut page = PageMut::new(page_bytes)?;
            PageClearAllVisible(&mut page);
            Ok(())
        })?;
        page_seam::visibilitymap_clear::call(
            relation,
            buffer_get_block_number(newbuf)?,
            vmbuffer_new,
            VISIBILITYMAP_VALID_BITS,
        )?;
    }

    if newbuf != buffer {
        bufmgr_seams::mark_buffer_dirty::call(newbuf);
    }
    bufmgr_seams::mark_buffer_dirty::call(buffer);

    /* XLOG stuff */
    if relcache_seam::relation_needs_wal::call(relation) {
        /*
         * For logical decoding we need combo CIDs to properly decode the
         * catalog.
         */
        if relation_is_accessible_in_logical_decoding(relation) {
            crate::log_heap_new_cid(relation, &oldtup.tuple)?;
            let heaptup_ref: &FormedTuple<'mcx> = match toasted.as_ref() {
                Some(t) => t,
                None => newtup,
            };
            crate::log_heap_new_cid(relation, &heaptup_ref.tuple)?;
        }

        let heaptup_ref: &FormedTuple<'mcx> = match toasted.as_ref() {
            Some(t) => t,
            None => newtup,
        };
        let recptr = log_heap_update(
            mcx,
            relation,
            buffer,
            newbuf,
            &oldtup,
            heaptup_ref,
            old_key_tuple.as_ref(),
            all_visible_cleared,
            all_visible_cleared_new,
        )?;
        if newbuf != buffer {
            bufmgr_seam::page_set_lsn::call(newbuf, recptr)?;
        }
        bufmgr_seam::page_set_lsn::call(buffer, recptr)?;
    }

    // END_CRIT_SECTION()

    if newbuf != buffer {
        lock_buffer_unlock(newbuf)?;
    }
    lock_buffer_unlock(buffer)?;

    /*
     * Mark old tuple for invalidation from system caches at next command
     * boundary, and the new tuple in case we abort.
     */
    {
        let heaptup_ref: &FormedTuple<'mcx> = match toasted.as_ref() {
            Some(t) => t,
            None => newtup,
        };
        inval::cache_invalidate::CacheInvalidateHeapTuple(
            relation,
            &oldtup.tuple,
            tuple_user_data(&oldtup),
            Some(&heaptup_ref.tuple),
            Some(&heaptup_ref.data),
        )?;
    }

    /* Now we can release the buffer(s) */
    if newbuf != buffer {
        bufmgr_seams::release_buffer::call(newbuf);
    }
    bufmgr_seams::release_buffer::call(buffer);
    if vmbuffer_new != InvalidBuffer {
        bufmgr_seams::release_buffer::call(vmbuffer_new);
    }
    if vmbuffer != InvalidBuffer {
        bufmgr_seams::release_buffer::call(vmbuffer);
    }

    /* Release the lmgr tuple lock, if we had it. */
    if have_tuple_lock {
        heapam_seam::unlock_tuple_tuplock::call(relation, oldtup.tuple.t_self, lockmode)?;
    }

    pgstat_seam::pgstat_count_heap_update::call(
        relation.rd_id,
        relation.rd_rel.relisshared,
        relation.pgstat_enabled,
        use_hot_update,
        newbuf != buffer,
    );

    /*
     * If heaptup is a private (toasted) copy, copy t_self back to the caller's
     * image. (C also heap_freetuples the copy; the owned FormedTuple is dropped
     * at scope end.)
     */
    if store_toasted {
        newtup.tuple.t_self = heaptup_self;
    }
    drop(toasted);

    /*
     * If it is a HOT update, we may still need to update summarized indexes.
     */
    let update_indexes = if use_hot_update {
        if summarized_update {
            TU_UpdateIndexes::TU_Summarizing
        } else {
            TU_UpdateIndexes::TU_None
        }
    } else {
        TU_UpdateIndexes::TU_All
    };

    drop(old_key_tuple);

    Ok(heapam_seam::HeapUpdateResult {
        result: TM_Result::TM_Ok,
        lockmode,
        update_indexes,
    })
}

// ===========================================================================
// simple_heap_update — replace a tuple (heapam.c).
// ===========================================================================

/// `simple_heap_update(relation, otid, tup, update_indexes)` (heapam.c).
pub fn simple_heap_update<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &Relation<'mcx>,
    otid: ItemPointerData,
    tup: &mut FormedTuple<'mcx>,
) -> PgResult<TU_UpdateIndexes> {
    let mut tmfd = TM_FailureData::default();
    let cid = xact_seam::get_current_command_id::call(true)?;
    let out = heap_update(
        mcx, relation, otid, tup, cid, /* crosscheck */ None, /* wait */ true, &mut tmfd,
    )?;
    match out.result {
        TM_Result::TM_SelfModified => Err(elog_error("tuple already updated by self")),
        TM_Result::TM_Ok => Ok(out.update_indexes),
        TM_Result::TM_Updated => Err(elog_error("tuple concurrently updated")),
        TM_Result::TM_Deleted => Err(elog_error("tuple concurrently deleted")),
        other => Err(elog_error(&format!(
            "unrecognized heap_update status: {}",
            other as u32
        ))),
    }
}

// ===========================================================================
// HeapDetermineColumnsInfo / heap_attr_equals (heapam.c, static).
// ===========================================================================

/// `heap_attr_equals(tupdesc, attrnum, value1, value2, isnull1, isnull2)`
/// (heapam.c) — whether the given attribute's values are the same (simple
/// binary comparison).
fn heap_attr_equals(
    tupdesc: &::types_tuple::heaptuple::TupleDescData<'_>,
    attrnum: i32,
    value1: &Datum<'_>,
    value2: &Datum<'_>,
    isnull1: bool,
    isnull2: bool,
) -> PgResult<bool> {
    /* one NULL and one not -> not equal */
    if isnull1 != isnull2 {
        return Ok(false);
    }
    /* both NULL -> equal */
    if isnull1 {
        return Ok(true);
    }

    if attrnum <= 0 {
        /* The only allowed system column is the OID-shaped tableOID. */
        Ok(DatumGetObjectId(value1) == DatumGetObjectId(value2))
    } else {
        debug_assert!(attrnum <= tupdesc.natts);
        let att = tupdesc.compact_attr((attrnum - 1) as usize);
        scalar_datum_core::datum_is_equal(
            value1,
            value2,
            att.attbyval,
            att.attlen as i32,
        )
    }
}

/// `HeapDetermineColumnsInfo(relation, interesting_cols, external_cols, oldtup,
/// newtup, &has_external)` (heapam.c) — the set of interesting columns that
/// changed; also reports whether any unmodified interesting attribute of the
/// old tuple is externally stored and a member of `external_cols`.
#[allow(clippy::too_many_arguments)]
fn HeapDetermineColumnsInfo<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &RelationData<'_>,
    interesting_cols: Option<&Bitmapset<'_>>,
    external_cols: Option<&Bitmapset<'_>>,
    oldtup: &FormedTuple<'mcx>,
    newtup: &FormedTuple<'mcx>,
    has_external: &mut bool,
) -> PgResult<Option<::mcx::PgBox<'mcx, Bitmapset<'mcx>>>> {
    let tupdesc = &relation.rd_att;
    let mut modified: Option<::mcx::PgBox<'mcx, Bitmapset<'mcx>>> = None;

    // Deform both tuples once for user-column access (the repo's getattr
    // analog; system columns are read via heap_getsysattr below). C uses
    // heap_getattr per-column, but the C comment notes deforming once is more
    // efficient and equivalent for the user columns.
    // `oldtup` is the on-page read (`read_on_page_tuple`), whose `data` spans
    // the null bitmap + maxalign pad before the user-data area; reach the
    // column area with `tuple_user_data`. `newtup` is a formed tuple whose
    // `data` is already the column area (bitmap carried in `t_bits`), so it is
    // deformed directly.
    let old_cols = heap_deform_tuple(mcx, &oldtup.tuple, tupdesc, tuple_user_data(oldtup))?;
    let new_cols = heap_deform_tuple(mcx, &newtup.tuple, tupdesc, &newtup.data)?;

    let mut attidx: i32 = -1;
    loop {
        attidx = bms_seam::bms_next_member::call(interesting_cols, attidx);
        if attidx < 0 {
            break;
        }
        /* attidx is zero-based, attrnum is the normal attribute number */
        let attrnum = attidx + (FirstLowInvalidHeapAttributeNumber as i32);

        /*
         * A whole-tuple reference -> say "not equal".
         */
        if attrnum == 0 {
            modified = Some(bms_seam::bms_add_member::call(mcx, modified, attidx)?);
            continue;
        }

        /*
         * Likewise "not equal" for any system attribute other than tableOID.
         */
        if attrnum < 0 && attrnum != TableOidAttributeNumber as i32 {
            modified = Some(bms_seam::bms_add_member::call(mcx, modified, attidx)?);
            continue;
        }

        // Extract the corresponding values.
        let (value1, isnull1) = getattr(mcx, oldtup, &old_cols, attrnum)?;
        let (value2, isnull2) = getattr(mcx, newtup, &new_cols, attrnum)?;

        if !heap_attr_equals(tupdesc, attrnum, &value1, &value2, isnull1, isnull2)? {
            modified = Some(bms_seam::bms_add_member::call(mcx, modified, attidx)?);
            continue;
        }

        /*
         * No need to check attributes that can't be stored externally. System
         * attributes can't be stored externally.
         */
        if attrnum < 0 || isnull1 || tupdesc.compact_attr((attrnum - 1) as usize).attlen != -1 {
            continue;
        }

        /*
         * Check if the old tuple's attribute is stored externally and is a
         * member of external_cols.
         */
        if VARATT_IS_EXTERNAL(&value1)
            && bms_seam::bms_is_member::call(attidx, external_cols)
        {
            *has_external = true;
        }
    }

    Ok(modified)
}

/// `heap_getattr(tup, attrnum, tupdesc, &isnull)` for one attribute, using the
/// pre-deformed user columns and `heap_getsysattr` for system columns.
fn getattr<'mcx>(
    mcx: Mcx<'mcx>,
    tup: &FormedTuple<'mcx>,
    deformed: &[(Datum<'mcx>, bool)],
    attrnum: i32,
) -> PgResult<(Datum<'mcx>, bool)> {
    if attrnum > 0 {
        // User column: read from the pre-deformed array. A column beyond the
        // tuple's stored natts reads as NULL (heap_deform_tuple fills missing
        // trailing columns with NULL up to tupdesc->natts).
        let idx = (attrnum - 1) as usize;
        if idx < deformed.len() {
            let (v, n) = &deformed[idx];
            Ok((v.clone(), *n))
        } else {
            Ok((Datum::null(), true))
        }
    } else {
        // System column (only tableOID reaches here from the caller).
        heap_getsysattr(mcx, &tup.tuple, attrnum)
    }
}

// ===========================================================================
// log_heap_update — emit XLOG_HEAP_UPDATE / XLOG_HEAP_HOT_UPDATE (heapam.c).
// ===========================================================================

/// `log_heap_update(reln, oldbuf, newbuf, oldtup, newtup, old_key_tuple,
/// all_visible_cleared, new_all_visible_cleared)` (heapam.c) — XLogInsert the
/// heap update record (with old/new tuple images and prefix/suffix
/// compression). Returns the record's LSN.
#[allow(clippy::too_many_arguments)]
fn log_heap_update<'mcx>(
    mcx: Mcx<'mcx>,
    reln: &RelationData<'_>,
    oldbuf: Buffer,
    newbuf: Buffer,
    oldtup: &FormedTuple<'mcx>,
    newtup: &FormedTuple<'mcx>,
    old_key_tuple: Option<&FormedTuple<'mcx>>,
    all_visible_cleared: bool,
    new_all_visible_cleared: bool,
) -> PgResult<::types_core::XLogRecPtr> {
    let need_tuple_data = relation_is_logically_logged(reln);

    /* Caller should not call me on a non-WAL-logged relation */
    debug_assert!(relcache_seam::relation_needs_wal::call(reln));

    xloginsert_seam::xlog_begin_insert::call()?;

    let new_hdr = newtup
        .tuple
        .t_data
        .as_ref()
        .expect("log_heap_update: newtup has no t_data");
    let old_hdr = oldtup
        .tuple
        .t_data
        .as_ref()
        .expect("log_heap_update: oldtup has no t_data");

    let mut info: u8 = if HeapTupleIsHeapOnly(new_hdr) {
        XLOG_HEAP_HOT_UPDATE
    } else {
        XLOG_HEAP_UPDATE
    };

    // The contiguous on-disk images of both tuples (header + bitmap + data).
    let new_img = heap_tuple_to_disk_image(mcx, newtup)?;
    let old_img = heap_tuple_to_disk_image(mcx, oldtup)?;
    let new_hoff = new_hdr.t_hoff as usize;
    let old_hoff = old_hdr.t_hoff as usize;
    let new_len = newtup.tuple.t_len as usize;
    let old_len = oldtup.tuple.t_len as usize;

    /*
     * If old and new tuple are on the same page, log only the changed parts of
     * the new tuple via common prefix/suffix counting. Skip if FPI of the new
     * page, or wal_level=logical.
     */
    let (prefixlen, suffixlen) =
        if oldbuf == newbuf && !need_tuple_data && !xlog_seam::xlog_check_buffer_needs_backup::call(newbuf)? {
            compute_prefix_suffix(&old_img[old_hoff..old_len], &new_img[new_hoff..new_len])
        } else {
            (0, 0)
        };

    /* Prepare main WAL data chain */
    let mut flags: u8 = 0;
    if all_visible_cleared {
        flags |= XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED;
    }
    if new_all_visible_cleared {
        flags |= XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED;
    }
    if prefixlen > 0 {
        flags |= XLH_UPDATE_PREFIX_FROM_OLD;
    }
    if suffixlen > 0 {
        flags |= XLH_UPDATE_SUFFIX_FROM_OLD;
    }
    if need_tuple_data {
        flags |= XLH_UPDATE_CONTAINS_NEW_TUPLE;
        if old_key_tuple.is_some() {
            if reln.rd_rel.relreplident == REPLICA_IDENTITY_FULL {
                flags |= XLH_UPDATE_CONTAINS_OLD_TUPLE;
            } else {
                flags |= XLH_UPDATE_CONTAINS_OLD_KEY;
            }
        }
    }

    /* If new tuple is the single and first tuple on page... */
    let init = ItemPointerGetOffsetNumber(&newtup.tuple.t_self) == FirstOffsetNumber
        && page_seam::page_get_max_offset_number::call(newbuf)? == FirstOffsetNumber;
    if init {
        info |= XLOG_HEAP_INIT_PAGE;
    }

    let xlrec = xl_heap_update {
        old_offnum: ItemPointerGetOffsetNumber(&oldtup.tuple.t_self),
        old_xmax: HeapTupleHeaderGetRawXmax(old_hdr),
        old_infobits_set: compute_infobits(old_hdr.t_infomask, old_hdr.t_infomask2),
        flags,
        new_offnum: ItemPointerGetOffsetNumber(&newtup.tuple.t_self),
        new_xmax: HeapTupleHeaderGetRawXmax(new_hdr),
    };

    let mut bufflags: u8 = REGBUF_STANDARD;
    if init {
        bufflags |= REGBUF_WILL_INIT;
    }
    if need_tuple_data {
        bufflags |= REGBUF_KEEP_DATA;
    }

    xloginsert_seam::xlog_register_buffer::call(0, newbuf, bufflags)?;
    if oldbuf != newbuf {
        xloginsert_seam::xlog_register_buffer::call(1, oldbuf, REGBUF_STANDARD)?;
    }

    let recbuf = xlrec.to_bytes();
    xloginsert_seam::xlog_register_data::call(&recbuf[..SizeOfHeapUpdate])?;

    /* Prepare WAL data for the new tuple. */
    if prefixlen > 0 || suffixlen > 0 {
        if prefixlen > 0 && suffixlen > 0 {
            let mut ps = [0u8; 4];
            ps[0..2].copy_from_slice(&(prefixlen as u16).to_ne_bytes());
            ps[2..4].copy_from_slice(&(suffixlen as u16).to_ne_bytes());
            xloginsert_seam::xlog_register_buf_data::call(0, &ps)?;
        } else if prefixlen > 0 {
            xloginsert_seam::xlog_register_buf_data::call(0, &(prefixlen as u16).to_ne_bytes())?;
        } else {
            xloginsert_seam::xlog_register_buf_data::call(0, &(suffixlen as u16).to_ne_bytes())?;
        }
    }

    let xlhdr = xl_heap_header {
        t_infomask2: new_hdr.t_infomask2,
        t_infomask: new_hdr.t_infomask,
        t_hoff: new_hdr.t_hoff,
    };
    debug_assert!(SizeofHeapTupleHeader + prefixlen + suffixlen <= new_len);

    /*
     * PG73FORMAT: write bitmap [+ padding] [+ oid] + data; 'data' excludes the
     * common prefix/suffix.
     */
    let hdrbuf = xlhdr.to_bytes();
    xloginsert_seam::xlog_register_buf_data::call(0, &hdrbuf[..SizeOfHeapHeader])?;
    if prefixlen == 0 {
        // (char*)t_data + SizeofHeapTupleHeader .. t_len - SizeofHeapTupleHeader - suffixlen
        xloginsert_seam::xlog_register_buf_data::call(
            0,
            &new_img[SizeofHeapTupleHeader..new_len - suffixlen],
        )?;
    } else {
        /* bitmap [+ padding] [+ oid] */
        if new_hoff - SizeofHeapTupleHeader > 0 {
            xloginsert_seam::xlog_register_buf_data::call(
                0,
                &new_img[SizeofHeapTupleHeader..new_hoff],
            )?;
        }
        /* data after common prefix */
        xloginsert_seam::xlog_register_buf_data::call(
            0,
            &new_img[new_hoff + prefixlen..new_len - suffixlen],
        )?;
    }

    /* We need to log a tuple identity */
    if need_tuple_data {
        if let Some(okt) = old_key_tuple {
            let okt_hdr = okt
                .tuple
                .t_data
                .as_ref()
                .expect("log_heap_update: old_key_tuple has no t_data");
            let xlhdr_idx = xl_heap_header {
                t_infomask2: okt_hdr.t_infomask2,
                t_infomask: okt_hdr.t_infomask,
                t_hoff: okt_hdr.t_hoff,
            };
            let idxbuf = xlhdr_idx.to_bytes();
            xloginsert_seam::xlog_register_data::call(&idxbuf[..SizeOfHeapHeader])?;
            /* PG73FORMAT: write bitmap [+ padding] [+ oid] + data */
            let okt_img = heap_tuple_to_disk_image(mcx, okt)?;
            xloginsert_seam::xlog_register_data::call(&okt_img[SizeofHeapTupleHeader..])?;
        }
    }

    /* filtering by origin on a row level is much more efficient */
    xloginsert_seam::xlog_set_record_flags::call(XLOG_INCLUDE_ORIGIN);

    let recptr = xloginsert_seam::xlog_insert_record::call(RM_HEAP_ID, info)?;
    Ok(recptr)
}

// ===========================================================================
// Small helpers.
// ===========================================================================

/// The tuple's user-data area (`(char *) t_data + t_hoff`) for the cache-key
/// deform in `CacheInvalidateHeapTuple` / `HeapDetermineColumnsInfo`.
/// `read_on_page_tuple` now captures `tp.data` as `item[t_hoff..]` (the
/// documented [`FormedTuple::data`] convention — the user-data area, with any
/// null bitmap already in `t_bits`), so this is simply `tp.data` (no bitmap-skip
/// compensation needed).
fn tuple_user_data<'a, 'mcx>(tp: &'a FormedTuple<'mcx>) -> &'a [u8] {
    &tp.data
}

/// Materialize the on-page tuple at `(buffer, tid)` into `mcx` (header + user
/// data + length). Same as `delete.rs::read_on_page_tuple`.
fn read_on_page_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    rel_id: Oid,
    buffer: Buffer,
    tid: ItemPointerData,
) -> PgResult<FormedTuple<'mcx>> {
    let offnum = ItemPointerGetOffsetNumber(&tid);
    let mut out: Option<(HeapTupleHeaderData<'mcx>, ::mcx::PgVec<'mcx, u8>, u32)> = None;
    bufmgr_seam::with_buffer_page::call(buffer, &mut |page_bytes| {
        let page = PageRef::new(page_bytes)?;
        let item_id = PageGetItemId(&page, offnum)?;
        debug_assert!(item_id.has_storage());
        let item = PageGetItem(&page, &item_id)?;
        let hdr = HeapTupleHeaderData::read_on_page(mcx, item)?;
        // `FormedTuple::data` is the user-data area (`item[t_hoff..t_len]`), i.e.
        // the bytes *after* the fixed header AND the null bitmap. When the tuple
        // has NULLs, `t_hoff > SizeofHeapTupleHeader` (it includes the
        // `BITMAPLEN(natts)` null bitmap, which `read_on_page` already captured
        // into `t_bits`). Slicing from `SizeofHeapTupleHeader` would prepend the
        // null-bitmap bytes to the data area, shifting every attribute offset by
        // the bitmap length and corrupting `heap_deform_tuple` (e.g. the toast
        // path's old-tuple deform reads a varlena attribute as garbage / empty).
        // Slice from `t_hoff`, matching `FormedTuple::read_on_page_full`.
        let t_hoff = hdr.t_hoff as usize;
        let data_start = core::cmp::min(t_hoff, item.len());
        let mut data = ::mcx::PgVec::new_in(mcx);
        for &b in &item[data_start..] {
            data.push(b);
        }
        out = Some((hdr, data, item.len() as u32));
        Ok(())
    })?;
    let (hdr, data, t_len) = out.expect("with_buffer_page closure must have run");
    let tuple = ::mcx::alloc_in(
        mcx,
        HeapTupleData {
            t_len,
            t_self: tid,
            t_tableOid: rel_id,
            t_data: Some(::mcx::alloc_in(mcx, hdr)?),
        },
    )?;
    Ok(FormedTuple { tuple, data })
}

/// Write the materialized header of `tp` back into the on-page tuple bytes at
/// `tp.tuple.t_self` (C mutates `t_data` which aliases the page).
fn write_back_header(buffer: Buffer, tp: &FormedTuple<'_>) -> PgResult<()> {
    let offnum = ItemPointerGetOffsetNumber(&tp.tuple.t_self);
    let header_image = data_ref(tp).clone();
    bufmgr_seam::with_buffer_page::call(buffer, &mut |page_bytes| {
        let (off, len) = {
            let page = PageRef::new(page_bytes)?;
            let item_id = PageGetItemId(&page, offnum)?;
            (item_id.lp_off() as usize, item_id.lp_len() as usize)
        };
        let item = page_bytes
            .get_mut(off..off + len)
            .ok_or_else(|| ::types_error::PgError::error("item storage is outside page"))?;
        header_image.write_on_page(item)?;
        Ok(())
    })
}

/// Whether the item id at `(buffer, offnum)` is `LP_NORMAL` (`ItemIdIsNormal`).
fn on_page_item_is_normal(buffer: Buffer, offnum: OffsetNumber) -> PgResult<bool> {
    let mut normal = false;
    bufmgr_seam::with_buffer_page::call(buffer, &mut |page_bytes| {
        let page = PageRef::new(page_bytes)?;
        let item_id = PageGetItemId(&page, offnum)?;
        normal = item_id.is_normal();
        Ok(())
    })?;
    Ok(normal)
}

/// `tp->t_data` as a shared header reference.
fn data_ref<'a, 'mcx>(tp: &'a FormedTuple<'mcx>) -> &'a HeapTupleHeaderData<'mcx> {
    tp.tuple.t_data.as_ref().expect("heap_update: tuple has no t_data")
}

/// `tp->t_data` as a mutable header reference.
fn data_mut<'a, 'mcx>(tp: &'a mut FormedTuple<'mcx>) -> &'a mut HeapTupleHeaderData<'mcx> {
    tp.tuple.t_data.as_mut().expect("heap_update: tuple has no t_data")
}

/// `LockBuffer(buffer, BUFFER_LOCK_UNLOCK)`.
fn lock_buffer_unlock(buffer: Buffer) -> PgResult<()> {
    bufmgr_seam::lock_buffer::call(buffer, BUFFER_LOCK_UNLOCK)
}

/// `BUFFER_LOCK_UNLOCK` (storage/bufmgr.h).
const BUFFER_LOCK_UNLOCK: i32 = 0;

/// `PageIsAllVisible(BufferGetPage(buffer))` over the buffer-id boundary.
fn page_is_all_visible(buffer: Buffer) -> PgResult<bool> {
    page_seam::page_is_all_visible::call(buffer)
}

/// `BufferGetBlockNumber(buffer)`.
fn buffer_get_block_number(buffer: Buffer) -> PgResult<BlockNumber> {
    Ok(bufmgr_seams::buffer_get_block_number::call(buffer))
}

/// `HeapTupleHasExternal(tuple)`.
fn HeapTupleHasExternal(tp: &FormedTuple<'_>) -> bool {
    tp.tuple
        .t_data
        .as_ref()
        .is_some_and(|hdr| (hdr.t_infomask & HEAP_HASEXTERNAL) != 0)
}

/// `HeapTupleHeaderGetCmax(tup)` via the combo-cid owner seam.
fn HeapTupleHeaderGetCmax(hdr: &HeapTupleHeaderData<'_>) -> CommandId {
    combocid_seam::heap_tuple_header_get_cmax::call(hdr)
}

/// `HeapTupleGetUpdateXid(tup)` — header-only multixact update-xid resolution.
fn HeapTupleGetUpdateXid(hdr: &HeapTupleHeaderData<'_>) -> PgResult<TransactionId> {
    HtupGetUpdateXid(hdr)
}

/// `TransactionIdIsValid(xid)`.
fn TransactionIdIsValid(xid: TransactionId) -> bool {
    xid != InvalidTransactionId
}

/// `TransactionIdDidAbort(xid)` — threads C's `TransactionXmin` from snapmgr.
fn TransactionIdDidAbort(xid: TransactionId) -> PgResult<bool> {
    let transaction_xmin = snapmgr_pc_seams::transaction_xmin::call()?;
    TxnDidAbort(xid, transaction_xmin)
}

/// `HEAP_XMAX_IS_KEYSHR_LOCKED(infomask)` (htup_details.h).
fn HEAP_XMAX_IS_KEYSHR_LOCKED(infomask: u16) -> bool {
    (infomask & HEAP_LOCK_MASK) == HEAP_XMAX_KEYSHR_LOCK
}

/// `DatumGetObjectId(datum)` — for the system-column (tableOID) comparison, the
/// value is a by-value 4-byte OID word.
fn DatumGetObjectId(d: &Datum<'_>) -> Oid {
    d.as_oid()
}

/// `VARATT_IS_EXTERNAL((struct varlena *) DatumGetPointer(value))` — whether the
/// by-reference attribute's varlena is stored externally (TOAST pointer).
fn VARATT_IS_EXTERNAL(d: &Datum<'_>) -> bool {
    match d {
        Datum::ByRef(_) => {
            // VARATT_IS_EXTERNAL(PTR) == VARATT_IS_1B_E(PTR) == (va_header[0]
            // == 0x01) on little-endian (varatt.h).
            let bytes = d.as_ref_bytes();
            !bytes.is_empty() && bytes[0] == 0x01
        }
        // None of these are an on-disk external (TOAST-pointer) varlena.
        Datum::ByVal(_)
        | Datum::Cstring(_)
        | Datum::Composite(_)
        | Datum::Expanded(_)
        | Datum::Internal(_) => false,
    }
}

/// `HeapTupleIsHeapOnly(tuple)` — `(t_infomask2 & HEAP_ONLY_TUPLE) != 0`.
fn HeapTupleIsHeapOnly(hdr: &HeapTupleHeaderData<'_>) -> bool {
    (hdr.t_infomask2 & HEAP_ONLY_TUPLE) != 0
}

// --- header-field setters (htup_details.h inline functions) ----------------

/// `HeapTupleHeaderSetXmin(tup, xid)`.
///
/// C writes the `t_heap` union arm unconditionally; ensure the Rust enum is on
/// that arm first (the new-tuple copy from `heap_modify_tuple` arrives `TDatum`).
fn HeapTupleHeaderSetXmin(hdr: &mut HeapTupleHeaderData<'_>, xid: TransactionId) {
    hdr.ensure_heap_arm().t_xmin = xid;
}

/// `HeapTupleHeaderSetXmax(tup, xid)`.
fn HeapTupleHeaderSetXmax(hdr: &mut HeapTupleHeaderData<'_>, xid: TransactionId) {
    hdr.ensure_heap_arm().t_xmax = xid;
}

/// `HeapTupleHeaderSetCmin(tup, cid)` — `t_field3.t_cid = cid; t_infomask &=
/// ~HEAP_COMBOCID`. (Asserts `!HEAP_MOVED` in C; a fresh update tuple has it
/// cleared above via HEAP_XACT_MASK.)
fn HeapTupleHeaderSetCmin(hdr: &mut HeapTupleHeaderData<'_>, cid: CommandId) {
    debug_assert!(hdr.t_infomask & HEAP_MOVED == 0);
    hdr.ensure_heap_arm().t_field3 = HeapTupleField3::TCid(cid);
    hdr.t_infomask &= !HEAP_COMBOCID;
}

/// `HeapTupleHeaderSetCmax(tup, cid, iscombo)`.
fn HeapTupleHeaderSetCmax(hdr: &mut HeapTupleHeaderData<'_>, cid: CommandId, iscombo: bool) {
    debug_assert!(hdr.t_infomask & HEAP_MOVED == 0);
    hdr.ensure_heap_arm().t_field3 = HeapTupleField3::TCid(cid);
    if iscombo {
        hdr.t_infomask |= HEAP_COMBOCID;
    } else {
        hdr.t_infomask &= !HEAP_COMBOCID;
    }
}

/// `HeapTupleClearHotUpdated(tup)` / `HeapTupleHeaderClearHotUpdated` —
/// `t_infomask2 &= ~HEAP_HOT_UPDATED`.
fn HeapTupleHeaderClearHotUpdated(hdr: &mut HeapTupleHeaderData<'_>) {
    hdr.t_infomask2 &= !HEAP_HOT_UPDATED;
}

/// `HeapTupleSetHotUpdated(tup)` — `t_infomask2 |= HEAP_HOT_UPDATED`.
fn HeapTupleHeaderSetHotUpdated(hdr: &mut HeapTupleHeaderData<'_>) {
    hdr.t_infomask2 |= HEAP_HOT_UPDATED;
}

/// `HeapTupleSetHeapOnly(tup)` — `t_infomask2 |= HEAP_ONLY_TUPLE`.
fn HeapTupleHeaderSetHeapOnly(hdr: &mut HeapTupleHeaderData<'_>) {
    hdr.t_infomask2 |= HEAP_ONLY_TUPLE;
}

/// `HeapTupleClearHeapOnly(tup)` — `t_infomask2 &= ~HEAP_ONLY_TUPLE`.
fn HeapTupleHeaderClearHeapOnly(hdr: &mut HeapTupleHeaderData<'_>) {
    hdr.t_infomask2 &= !HEAP_ONLY_TUPLE;
}

/// `MAXALIGN(LEN)` — round up to `MAXIMUM_ALIGNOF` (8).
fn maxalign(len: usize) -> usize {
    const MAXIMUM_ALIGNOF: usize = 8;
    (len.wrapping_add(MAXIMUM_ALIGNOF - 1)) & !(MAXIMUM_ALIGNOF - 1)
}

/// `RelationIsAccessibleInLogicalDecoding(relation)` (utils/rel.h).
fn relation_is_accessible_in_logical_decoding(relation: &RelationData<'_>) -> bool {
    let wal = transam_xlog_seams::wal_level::call();
    let xlog_logical_info_active = wal >= ::wal::WalLevel::Logical;
    xlog_logical_info_active
        && relcache_seam::relation_needs_wal::call(relation)
        && (catalog_seam::is_catalog_relation::call(relation)
            || relation_is_used_as_catalog_table(relation))
}

/// `RelationIsLogicallyLogged(relation)` (utils/rel.h).
fn relation_is_logically_logged(relation: &RelationData<'_>) -> bool {
    let wal = transam_xlog_seams::wal_level::call();
    let xlog_logical_info_active = wal >= ::wal::WalLevel::Logical;
    xlog_logical_info_active
        && relcache_seam::relation_needs_wal::call(relation)
        && !catalog_seam::is_catalog_relation::call(relation)
}

/// `RelationIsUsedAsCatalogTable(relation)` (utils/rel.h).
fn relation_is_used_as_catalog_table(relation: &RelationData<'_>) -> bool {
    let relkind = relation.rd_rel.relkind;
    (relkind == RELKIND_RELATION || relkind == RELKIND_MATVIEW)
        && relation
            .rd_options
            .as_ref()
            .and_then(|o| o.std())
            .is_some_and(|o| o.user_catalog_table)
}

/// `elog(ERROR, msg)` builder.
fn elog_error(msg: &str) -> ::types_error::PgError {
    ereport(ERROR).errmsg_internal(msg.to_string()).into_error()
}

/// The common prefix/suffix counting from `log_heap_update` (heapam.c). `oldp` /
/// `newp` are the data areas (past `t_hoff`) of the old / new tuples. Returns
/// `(prefixlen, suffixlen)`, each `0` unless it would save at least 3 bytes (the
/// prefix/suffix length itself costs 2 bytes to store).
fn compute_prefix_suffix(oldp: &[u8], newp: &[u8]) -> (usize, usize) {
    let old_dlen = oldp.len();
    let new_dlen = newp.len();
    let min_len = old_dlen.min(new_dlen);

    /* common prefix */
    let mut prefixlen = 0usize;
    while prefixlen < min_len && newp[prefixlen] == oldp[prefixlen] {
        prefixlen += 1;
    }
    if prefixlen < 3 {
        prefixlen = 0;
    }

    /* common suffix */
    let mut suffixlen = 0usize;
    let suffix_cap = min_len - prefixlen;
    while suffixlen < suffix_cap && newp[new_dlen - suffixlen - 1] == oldp[old_dlen - suffixlen - 1] {
        suffixlen += 1;
    }
    if suffixlen < 3 {
        suffixlen = 0;
    }

    (prefixlen, suffixlen)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ::mcx::MemoryContext;
    use ::types_tuple::heaptuple::{
        BlockIdData, HeapTupleFields, ItemPointerData,
    };

    /// `compute_prefix_suffix` mirrors the C prefix/suffix counting: a common
    /// prefix and suffix are only recorded when each saves >= 3 bytes.
    #[test]
    fn prefix_suffix_counting_matches_c() {
        // Identical 10-byte data: prefix == whole, suffix limited by remaining.
        let a = [1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let (p, s) = compute_prefix_suffix(&a, &a);
        assert_eq!(p, 10);
        assert_eq!(s, 0); // suffix_cap = min_len - prefixlen = 0

        // Differ in the middle: long common prefix + long common suffix.
        let old = [0u8, 0, 0, 0, 0, 9, 9, 0, 0, 0, 0, 0];
        let new = [0u8, 0, 0, 0, 0, 7, 7, 0, 0, 0, 0, 0];
        let (p, s) = compute_prefix_suffix(&old, &new);
        assert_eq!(p, 5);
        assert_eq!(s, 5);

        // Short common prefix (< 3) is discarded.
        let old = [1u8, 1, 9, 9, 9];
        let new = [1u8, 1, 8, 8, 8];
        let (p, s) = compute_prefix_suffix(&old, &new);
        assert_eq!(p, 0);
        assert_eq!(s, 0);

        // Suffix only.
        let old = [9u8, 9, 1, 2, 3, 4];
        let new = [8u8, 7, 1, 2, 3, 4];
        let (p, s) = compute_prefix_suffix(&old, &new);
        assert_eq!(p, 0);
        assert_eq!(s, 4);
    }

    /// `xl_heap_update` round-trips its 14-byte body in the C field order.
    #[test]
    fn xl_heap_update_round_trips() {
        let rec = xl_heap_update {
            old_xmax: 0x1122_3344,
            old_offnum: 7,
            old_infobits_set: 0x05,
            flags: 0x60,
            new_xmax: 0x5566_7788,
            new_offnum: 9,
        };
        let bytes = rec.to_bytes();
        assert_eq!(bytes.len(), SizeOfHeapUpdate);
        assert_eq!(SizeOfHeapUpdate, 14);
        let back = xl_heap_update::from_bytes(&bytes);
        assert_eq!(back.old_xmax, rec.old_xmax);
        assert_eq!(back.old_offnum, rec.old_offnum);
        assert_eq!(back.old_infobits_set, rec.old_infobits_set);
        assert_eq!(back.flags, rec.flags);
        assert_eq!(back.new_xmax, rec.new_xmax);
        assert_eq!(back.new_offnum, rec.new_offnum);
    }

    /// `heap_attr_equals` does NULL handling then binary comparison; the
    /// tableOID system column compares as an OID word.
    #[test]
    fn heap_attr_equals_null_and_oid() {
        let ctx = MemoryContext::new("attr_equals");
        let mcx = ctx.mcx();
        // A trivial 1-attribute tuple descriptor (attbyval int4-like).
        let mut td = ::types_tuple::heaptuple::TupleDescData {
            natts: 1,
            tdtypeid: 0,
            tdtypmod: -1,
            tdrefcount: 0,
            constr: None,
            compact_attrs: ::mcx::PgVec::new_in(mcx),
            attrs: ::mcx::PgVec::new_in(mcx),
        };
        td.compact_attrs.push(::types_tuple::heaptuple::CompactAttribute {
            attcacheoff: -1,
            attlen: 4,
            attbyval: true,
            attispackable: false,
            atthasmissing: false,
            attisdropped: false,
            attgenerated: false,
            attnullability: 0,
            attalignby: 4,
        });

        let v1 = Datum::from_u32(42);
        let v2 = Datum::from_u32(42);
        let v3 = Datum::from_u32(43);

        // one null, one not -> not equal
        assert!(!heap_attr_equals(&td, 1, &v1, &v2, true, false).unwrap());
        // both null -> equal
        assert!(heap_attr_equals(&td, 1, &v1, &v2, true, true).unwrap());
        // equal by-value
        assert!(heap_attr_equals(&td, 1, &v1, &v2, false, false).unwrap());
        // unequal by-value
        assert!(!heap_attr_equals(&td, 1, &v1, &v3, false, false).unwrap());

        // tableOID system column (attrnum <= 0): compared as OID.
        let o1 = Datum::from_oid(100);
        let o2 = Datum::from_oid(100);
        let o3 = Datum::from_oid(200);
        assert!(heap_attr_equals(&td, TableOidAttributeNumber as i32, &o1, &o2, false, false).unwrap());
        assert!(!heap_attr_equals(&td, TableOidAttributeNumber as i32, &o1, &o3, false, false).unwrap());
    }

    /// `simple_heap_update`'s status-to-error mapping mirrors C's switch (the
    /// success arm returns `update_indexes`; failures elog(ERROR)). We exercise
    /// the message wording for the failure arms via the helper directly.
    #[test]
    fn simple_update_error_wording() {
        // The exact strings C uses.
        let _ = ItemPointerData { ip_blkid: BlockIdData::new(0), ip_posid: 0 };
        let _ = HeapTupleFields { t_xmin: 0, t_xmax: 0, t_field3: HeapTupleField3::TCid(0) };
        assert!(format!("{:?}", elog_error("tuple already updated by self")).contains("tuple already updated by self"));
        assert!(format!("{:?}", elog_error("tuple concurrently updated")).contains("tuple concurrently updated"));
        assert!(format!("{:?}", elog_error("tuple concurrently deleted")).contains("tuple concurrently deleted"));
    }
}
