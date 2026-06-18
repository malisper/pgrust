//! F3 (DELETE side) — heap tuple DELETE (`access/heap/heapam.c`):
//! `heap_delete` / `simple_heap_delete`, plus the two helpers shared with the
//! (not-yet-ported) UPDATE side: `compute_new_xmax_infomask` and
//! `ExtractReplicaIdentity`.
//!
//! `heap_update` (and its update-only helpers `HeapDetermineColumnsInfo` /
//! `heap_attr_equals` / `log_heap_update`) are **not** ported in this family:
//! `heap_update` alone is ~1100 lines, so per the F3 split authorization the
//! UPDATE side is deferred to a follow-on family that builds on the two shared
//! helpers landed here.
//!
//! Page model (the freespace.c / visibilitymap precedent): the buffer manager
//! owns the shared page. `heap_delete` pins + exclusively locks the target
//! buffer (`ReadBuffer` + `LockBuffer`), materializes the on-page tuple
//! (`tp`) into `mcx` (header + length, the faithful analog of C's
//! `tp.t_data = PageGetItem(...)`), runs all of its visibility / lock-wait /
//! xmax-compute logic on that materialized copy — the visibility predicate sets
//! its own hint bits on the page through the visibility crate's buffer-bound
//! setter — and, inside the critical section, writes the mutated header back
//! into the page plus the page-level flags (`PageSetPrunable` /
//! `PageClearAllVisible`) through one `with_buffer_page` mutation.
//!
//! The lock-wait primitives (`heap_acquire_tuplock` / `DoesMultiXactIdConflict`
//! / `MultiXactIdWait` / `XactLockTableWait` / `UnlockTupleTuplock`) and the
//! multixact create/expand/update-xid primitives live in not-yet-ported
//! families (the heapam LOCK family + multixact.c); they are reached through
//! honest seams that panic until those owners land.

use mcx::Mcx;
use types_core::primitive::{BlockNumber, MultiXactId, OffsetNumber, Oid, TransactionId};
use types_core::xact::{CommandId, InvalidCommandId};
use types_error::{
    PgResult, ERRCODE_INVALID_TRANSACTION_STATE, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERROR,
};
use backend_utils_error::ereport;
use types_tuple::backend_access_common_heaptuple::Datum;
use types_rel::{Relation, RelationData};
use types_storage::lock::XLTW_Oper;
use types_storage::{Buffer, InvalidBuffer};
use types_tableam::tableam::{LockTupleMode, LockWaitPolicy, TM_FailureData, TM_Result};
use types_tuple::backend_access_common_heaptuple::FormedTuple;
use types_tuple::heaptuple::{
    HeapTupleData, HeapTupleField3, HeapTupleHeaderChoice, HeapTupleHeaderData, ItemPointerData,
    FirstLowInvalidHeapAttributeNumber, HEAP_COMBOCID, HEAP_HASEXTERNAL, HEAP_KEYS_UPDATED,
    HEAP_MOVED, HEAP_HOT_UPDATED, HEAP_XMAX_INVALID,
};
use types_xlog_records::multixact::MultiXactStatus;

use backend_storage_page::{
    ItemPointerEquals, ItemPointerGetBlockNumber, ItemPointerGetOffsetNumber, ItemPointerIsValid,
    PageClearAllVisible, PageGetItem, PageGetItemId, PageIsAllVisible, PageMut, PageRef,
    PageSetPrunable,
};

use backend_access_heap_heapam_visibility::htup::{
    HeapTupleHeaderGetRawXmax, HEAP_LOCK_MASK, HEAP_XMAX_IS_LOCKED_ONLY,
};
use backend_access_heap_heapam_visibility::{
    HeapTupleHeaderGetUpdateXid as HtupGetUpdateXid, HeapTupleHeaderIsOnlyLocked,
    HeapTupleSatisfiesUpdate, HeapTupleSatisfiesVisibility,
};
use backend_access_transam_transam::TransactionIdEquals;

use crate::{
    compute_infobits, xmax_infomask_changed, GetMultiXactIdHintBits, UpdateXmaxHintBits,
};

use backend_access_heap_heapam_seams as heapam_seam;
use backend_access_heap_hio_seams as hio_seam;
use backend_access_heap_vacuumlazy_seams as page_seam;
use backend_access_transam_multixact_seams as multixact_seam;
use backend_access_transam_transam_seams as transam_seam;
use backend_access_transam_xact_seams as xact_seam;
use backend_access_transam_xloginsert_seams as xloginsert_seam;
use backend_catalog_catalog_seams as catalog_seam;
use backend_storage_buffer_bufmgr_seams as bufmgr_seam;
use backend_storage_ipc_procarray_seams as procarray_seam;
use backend_storage_lmgr_predicate_seams as predicate_seam;
use backend_utils_activity_pgstat_seams as pgstat_seam;
use backend_utils_cache_relcache_seams as relcache_seam;
use backend_utils_time_combocid_seams as combocid_seam;

use types_storage::bufpage::SizeofHeapTupleHeader;
use types_wal::wal::{RM_HEAP_ID, XLOG_INCLUDE_ORIGIN};
use types_wal::xloginsert::REGBUF_STANDARD;

use backend_rmgrdesc_next::heapdesc::XLOG_HEAP_DELETE;
use types_xlog_records::heapam_xlog::{
    xl_heap_delete, xl_heap_header, SizeOfHeapDelete, SizeOfHeapHeader,
    XLH_DELETE_ALL_VISIBLE_CLEARED, XLH_DELETE_CONTAINS_OLD_KEY, XLH_DELETE_CONTAINS_OLD_TUPLE,
    XLH_DELETE_IS_PARTITION_MOVE,
};

use backend_access_common_heaptuple::heap_tuple_to_disk_image;

// ---------------------------------------------------------------------------
// heapam-local vocabulary (htup_details.h / heapam.h / pg_class.h / rel.h /
// snapmgr.h / lockoptions.h constants).
// ---------------------------------------------------------------------------

/// `RELKIND_RELATION` / `RELKIND_MATVIEW` (catalog/pg_class.h).
const RELKIND_RELATION: u8 = b'r';
const RELKIND_MATVIEW: u8 = b'm';

/// `VISIBILITYMAP_VALID_BITS` (access/visibilitymapdefs.h) == ALL_VISIBLE |
/// ALL_FROZEN.
const VISIBILITYMAP_VALID_BITS: u8 = 0x03;

/// `REPLICA_IDENTITY_NOTHING` / `REPLICA_IDENTITY_FULL` (catalog/pg_class.h).
const REPLICA_IDENTITY_NOTHING: u8 = b'n';
const REPLICA_IDENTITY_FULL: u8 = b'f';

/// `InvalidTransactionId`.
const InvalidTransactionId: TransactionId = 0;

// ===========================================================================
// heap_delete — delete a tuple from a heap (heapam.c).
// ===========================================================================

/// `heap_delete(relation, tid, cid, crosscheck, wait, tmfd, changingPart)`
/// (heapam.c).
pub fn heap_delete<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &Relation<'mcx>,
    tid: ItemPointerData,
    mut cid: CommandId,
    crosscheck: Option<&types_snapshot::SnapshotData>,
    wait: bool,
    tmfd: &mut TM_FailureData,
    changing_part: bool,
) -> PgResult<TM_Result> {
    let xid = xact_seam::get_current_transaction_id::call()?;
    let mut vmbuffer: Buffer = InvalidBuffer;
    let mut have_tuple_lock = false;
    let mut all_visible_cleared = false;

    debug_assert!(ItemPointerIsValid(Some(&tid)));

    // AssertHasSnapshotForToast(relation) — debug-only snapshot assertion; no
    // state to mirror here.

    /*
     * Forbid this during a parallel operation, lest it allocate a combo CID.
     * Other workers might need that combo CID for visibility checks, and we
     * have no provision for broadcasting it to them.
     */
    if xact_seam::is_in_parallel_mode::call() {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_TRANSACTION_STATE)
            .errmsg("cannot delete tuples during a parallel operation")
            .into_error());
    }

    let block = ItemPointerGetBlockNumber(&tid);
    let buffer = hio_seam::read_buffer::call(relation.rd_id, block)?;

    /*
     * Before locking the buffer, pin the visibility map page if it appears to
     * be necessary.  Since we haven't got the lock yet, someone else might be
     * in the middle of changing this, so we'll need to recheck after we have
     * the lock.
     */
    if page_is_all_visible(buffer)? {
        vmbuffer = page_seam::visibilitymap_pin::call(relation, block, vmbuffer)?;
    }

    bufmgr_seam::lock_buffer_exclusive::call(buffer)?;

    // lp = PageGetItemId(page, off); Assert(ItemIdIsNormal(lp));
    // tp = { t_tableOid, t_data = PageGetItem(page, lp), t_len, t_self }.
    let mut tp = read_on_page_tuple(mcx, relation.rd_id, buffer, tid)?;

    // `l1:` retry loop — re-evaluated whenever we release+reacquire the buffer
    // lock and must recheck tuple state.
    let result = loop {
        /*
         * If we didn't pin the visibility map page and the page has become all
         * visible while we were busy locking the buffer, we'll have to unlock
         * and re-lock, to avoid holding the buffer lock across an I/O.
         */
        if vmbuffer == InvalidBuffer && page_is_all_visible(buffer)? {
            // C: release only the content lock (keep the pin), pin the VM page,
            // re-lock exclusive, fall through.
            lock_buffer_unlock(buffer)?;
            vmbuffer = page_seam::visibilitymap_pin::call(relation, block, vmbuffer)?;
            bufmgr_seam::lock_buffer_exclusive::call(buffer)?;
            // Re-materialize the on-page tuple after the lock round-trip.
            tp = read_on_page_tuple(mcx, relation.rd_id, buffer, tid)?;
        }

        let mut result = HeapTupleSatisfiesUpdate(&mut tp.tuple, cid, buffer)?;

        if result == TM_Result::TM_Invisible {
            bufmgr_seam::unlock_release_buffer::call(buffer);
            return Err(ereport(ERROR)
                .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg("attempted to delete invisible tuple")
                .into_error());
        } else if result == TM_Result::TM_BeingModified && wait {
            /* must copy state data before unlocking buffer */
            let xwait = HeapTupleHeaderGetRawXmax(data_ref(&tp));
            let infomask = data_ref(&tp).t_infomask;

            /*
             * Sleep until concurrent transaction ends -- except when there's a
             * single locker and it's our own transaction.
             */
            if (infomask & types_tuple::heaptuple::HEAP_XMAX_IS_MULTI) != 0 {
                let conflict = heapam_seam::does_multi_xact_id_conflict::call(
                    xwait as MultiXactId,
                    infomask,
                    LockTupleMode::LockTupleExclusive,
                )?;

                if conflict.conflict {
                    lock_buffer_unlock(buffer)?;

                    /*
                     * Acquire the lock, if necessary (but skip it when we're
                     * requesting a lock and already have one).
                     */
                    if !conflict.current_is_member {
                        have_tuple_lock = heapam_seam::heap_acquire_tuplock::call(
                            relation,
                            tp.tuple.t_self,
                            LockTupleMode::LockTupleExclusive,
                            LockWaitPolicy::LockWaitBlock,
                            have_tuple_lock,
                        )?;
                    }

                    /* wait for multixact */
                    heapam_seam::multi_xact_id_wait::call(
                        xwait as MultiXactId,
                        MultiXactStatus::Update,
                        infomask,
                        relation,
                        tp.tuple.t_self,
                        XLTW_Oper::Delete,
                    )?;
                    bufmgr_seam::lock_buffer_exclusive::call(buffer)?;

                    // Re-materialize the on-page tuple after the lock round-trip.
                    tp = read_on_page_tuple(mcx, relation.rd_id, buffer, tid)?;

                    /*
                     * If xwait had just locked the tuple then some other xact
                     * could update this tuple before we get to this point.
                     */
                    if (vmbuffer == InvalidBuffer && page_is_all_visible(buffer)?)
                        || xmax_infomask_changed(data_ref(&tp).t_infomask, infomask)
                        || !TransactionIdEquals(HeapTupleHeaderGetRawXmax(data_ref(&tp)), xwait)
                    {
                        continue; // goto l1
                    }
                }

                /*
                 * You might think the multixact is necessarily done here, but
                 * not so: it could have surviving members (our own xact / other
                 * subxacts). It is legal for us to delete in either case; we
                 * don't bother changing on-disk hint bits since we're about to
                 * overwrite the xmax altogether.
                 */
            } else if !xact_seam::transaction_id_is_current_transaction_id::call(xwait) {
                /*
                 * Wait for regular transaction to end; but first, acquire tuple
                 * lock.
                 */
                lock_buffer_unlock(buffer)?;
                have_tuple_lock = heapam_seam::heap_acquire_tuplock::call(
                    relation,
                    tp.tuple.t_self,
                    LockTupleMode::LockTupleExclusive,
                    LockWaitPolicy::LockWaitBlock,
                    have_tuple_lock,
                )?;
                heapam_seam::xact_lock_table_wait::call(
                    xwait,
                    relation,
                    tp.tuple.t_self,
                    XLTW_Oper::Delete,
                )?;
                bufmgr_seam::lock_buffer_exclusive::call(buffer)?;

                // Re-materialize the on-page tuple after the lock round-trip.
                tp = read_on_page_tuple(mcx, relation.rd_id, buffer, tid)?;

                /*
                 * xwait is done, but if xwait had just locked the tuple then
                 * some other xact could update this tuple before we get here.
                 */
                if (vmbuffer == InvalidBuffer && page_is_all_visible(buffer)?)
                    || xmax_infomask_changed(data_ref(&tp).t_infomask, infomask)
                    || !TransactionIdEquals(HeapTupleHeaderGetRawXmax(data_ref(&tp)), xwait)
                {
                    continue; // goto l1
                }

                /* Otherwise check if it committed or aborted */
                UpdateXmaxHintBits(data_mut(&mut tp), buffer, xwait)?;
            }

            /*
             * We may overwrite if previous xmax aborted, or if it committed but
             * only locked the tuple without updating it.
             */
            if (data_ref(&tp).t_infomask & HEAP_XMAX_INVALID) != 0
                || HEAP_XMAX_IS_LOCKED_ONLY(data_ref(&tp).t_infomask)
                || HeapTupleHeaderIsOnlyLocked(data_ref(&tp))?
            {
                result = TM_Result::TM_Ok;
            } else if !ItemPointerEquals(&tp.tuple.t_self, &data_ref(&tp).t_ctid) {
                result = TM_Result::TM_Updated;
            } else {
                result = TM_Result::TM_Deleted;
            }
        }

        /* sanity check the result and the logic above */
        if result != TM_Result::TM_Ok {
            debug_assert!(
                result == TM_Result::TM_SelfModified
                    || result == TM_Result::TM_Updated
                    || result == TM_Result::TM_Deleted
                    || result == TM_Result::TM_BeingModified
            );
            debug_assert!(data_ref(&tp).t_infomask & HEAP_XMAX_INVALID == 0);
            debug_assert!(
                result != TM_Result::TM_Updated
                    || !ItemPointerEquals(&tp.tuple.t_self, &data_ref(&tp).t_ctid)
            );
        }

        let mut result = result;
        if result == TM_Result::TM_Ok {
            if let Some(cc) = crosscheck {
                /* Additional check for transaction-snapshot mode RI updates. The
                 * visibility predicate takes `&mut SnapshotData` (it may advance
                 * snapshot caches); the caller's crosscheck is borrowed, so work
                 * on a local copy. */
                let mut cc_local = cc.clone();
                if !HeapTupleSatisfiesVisibility(&mut tp.tuple, &mut cc_local, buffer)? {
                    result = TM_Result::TM_Updated;
                }
            }
        }

        break result;
    };

    if result != TM_Result::TM_Ok {
        tmfd.ctid = data_ref(&tp).t_ctid;
        tmfd.xmax = HtupGetUpdateXid(data_ref(&tp))?;
        if result == TM_Result::TM_SelfModified {
            tmfd.cmax = HeapTupleHeaderGetCmax(data_ref(&tp));
        } else {
            tmfd.cmax = InvalidCommandId;
        }
        bufmgr_seam::unlock_release_buffer::call(buffer);
        if have_tuple_lock {
            heapam_seam::unlock_tuple_tuplock::call(
                relation,
                tp.tuple.t_self,
                LockTupleMode::LockTupleExclusive,
            )?;
        }
        if vmbuffer != InvalidBuffer {
            backend_storage_buffer_bufmgr_seams::release_buffer::call(vmbuffer);
        }
        return Ok(result);
    }

    /*
     * We're about to do the actual delete -- check for conflict first, to
     * avoid possibly having to roll back work we've just done.
     */
    predicate_seam::check_for_serializable_conflict_in::call(relation.rd_id)?;

    /* replace cid with a combo CID if necessary */
    let (new_cid, iscombo) =
        combocid_seam::heap_tuple_header_adjust_cmax::call(data_ref(&tp), cid)?;
    cid = new_cid;

    /*
     * Compute replica identity tuple before entering the critical section so we
     * don't PANIC upon a memory allocation failure.
     */
    let old_key_tuple = ExtractReplicaIdentity(mcx, relation, &tp, true)?;

    /*
     * If this is the first possibly-multixact-able operation in the current
     * transaction, set my per-backend OldestMemberMXactId setting.
     */
    multixact_seam::multi_xact_id_set_oldest_member::call()?;

    let (new_xmax, new_infomask, new_infomask2) = compute_new_xmax_infomask(
        mcx,
        HeapTupleHeaderGetRawXmax(data_ref(&tp)),
        data_ref(&tp).t_infomask,
        data_ref(&tp).t_infomask2,
        xid,
        LockTupleMode::LockTupleExclusive,
        true,
    )?;

    // START_CRIT_SECTION() — the crit-section bookkeeping lives behind the
    // buffer/WAL substrate; the panic-on-error contract is mirrored by the seam
    // signatures (a seam erroring here is a PANIC-class bug).

    /*
     * Stamp the on-page tuple header (and page-level flags) in place. C mutates
     * `tp.t_data` (an alias into the page) and then sets the page prunable /
     * clears all-visible; in this repo the on-page bytes are reachable only
     * inside a `with_buffer_page` mutation, so the same field writes are applied
     * there, also updating our materialized `tp` to match.
     */
    let self_tid = tp.tuple.t_self;
    {
        let hdr = data_mut(&mut tp);
        hdr.t_infomask &= !(HEAP_XMAX_BITS | HEAP_MOVED);
        hdr.t_infomask2 &= !HEAP_KEYS_UPDATED;
        hdr.t_infomask |= new_infomask;
        hdr.t_infomask2 |= new_infomask2;
        HeapTupleHeaderClearHotUpdated(hdr);
        HeapTupleHeaderSetXmax(hdr, new_xmax);
        HeapTupleHeaderSetCmax(hdr, cid, iscombo);
        /* Make sure there is no forward chain link in t_ctid */
        hdr.t_ctid = self_tid;
        /* Signal that this is actually a move into another partition */
        if changing_part {
            HeapTupleHeaderSetMovedPartitions(hdr);
        }
    }

    // Apply PageSetPrunable / PageClearAllVisible and write the mutated header
    // back into the page bytes, all under one content-locked mutation.
    let offnum = ItemPointerGetOffsetNumber(&tp.tuple.t_self);
    let header_image = data_ref(&tp).clone();
    bufmgr_seam::with_buffer_page::call(buffer, &mut |page_bytes| {
        let (off, len) = {
            let page = PageRef::new(page_bytes)?;
            let item_id = PageGetItemId(&page, offnum)?;
            (item_id.lp_off() as usize, item_id.lp_len() as usize)
        };
        // PageSetPrunable(page, xid) + (if all-visible) PageClearAllVisible.
        {
            let mut page = PageMut::new(page_bytes)?;
            PageSetPrunable(&mut page, xid);
        }
        let cleared = {
            let page = PageRef::new(page_bytes)?;
            PageIsAllVisible(&page)
        };
        if cleared {
            let mut page = PageMut::new(page_bytes)?;
            PageClearAllVisible(&mut page);
        }
        // Write the stamped header back into the on-page tuple.
        let item = page_bytes
            .get_mut(off..off + len)
            .ok_or_else(|| types_error::PgError::error("item storage is outside page"))?;
        header_image.write_on_page(item)?;
        Ok(())
    })?;

    if page_is_all_visible(buffer)? {
        all_visible_cleared = true;
        // The PageClearAllVisible above already cleared the page flag; clear the
        // visibility-map bit too.
        page_seam::visibilitymap_clear::call(
            relation,
            buffer_get_block_number(buffer)?,
            vmbuffer,
            VISIBILITYMAP_VALID_BITS,
        )?;
    }

    backend_storage_buffer_bufmgr_seams::mark_buffer_dirty::call(buffer);

    /*
     * XLOG stuff
     *
     * NB: heap_abort_speculative() uses the same xlog record and replay
     * routines.
     */
    if relcache_seam::relation_needs_wal::call(relation) {
        /*
         * For logical decode we need combo CIDs to properly decode the catalog.
         */
        if relation_is_accessible_in_logical_decoding(relation) {
            crate::log_heap_new_cid(relation, &tp.tuple)?;
        }

        let mut flags: u8 = 0;
        if all_visible_cleared {
            flags |= XLH_DELETE_ALL_VISIBLE_CLEARED;
        }
        if changing_part {
            flags |= XLH_DELETE_IS_PARTITION_MOVE;
        }
        if let Some(okt) = old_key_tuple.as_ref() {
            if relation.rd_rel.relreplident == REPLICA_IDENTITY_FULL {
                flags |= XLH_DELETE_CONTAINS_OLD_TUPLE;
            } else {
                flags |= XLH_DELETE_CONTAINS_OLD_KEY;
            }
            let _ = okt;
        }

        let xlrec = xl_heap_delete {
            xmax: new_xmax,
            offnum: ItemPointerGetOffsetNumber(&tp.tuple.t_self),
            infobits_set: compute_infobits(
                data_ref(&tp).t_infomask,
                data_ref(&tp).t_infomask2,
            ),
            flags,
        };

        xloginsert_seam::xlog_begin_insert::call()?;
        let recbuf = xlrec.to_bytes();
        xloginsert_seam::xlog_register_data::call(&recbuf[..SizeOfHeapDelete])?;

        xloginsert_seam::xlog_register_buffer::call(0, buffer, REGBUF_STANDARD)?;

        /*
         * Log replica identity of the deleted tuple if there is one.
         */
        if let Some(okt) = old_key_tuple.as_ref() {
            let okt_hdr = data_ref(okt);
            let xlhdr = xl_heap_header {
                t_infomask2: okt_hdr.t_infomask2,
                t_infomask: okt_hdr.t_infomask,
                t_hoff: okt_hdr.t_hoff,
            };
            let hdrbuf = xlhdr.to_bytes();
            xloginsert_seam::xlog_register_data::call(&hdrbuf[..SizeOfHeapHeader])?;
            // (char *) old_key_tuple->t_data + SizeofHeapTupleHeader ..
            //   old_key_tuple->t_len - SizeofHeapTupleHeader
            let img = heap_tuple_to_disk_image(mcx, okt)?;
            xloginsert_seam::xlog_register_data::call(&img[SizeofHeapTupleHeader..])?;
        }

        /* filtering by origin on a row level is much more efficient */
        xloginsert_seam::xlog_set_record_flags::call(XLOG_INCLUDE_ORIGIN);

        let recptr = xloginsert_seam::xlog_insert_record::call(RM_HEAP_ID, XLOG_HEAP_DELETE)?;

        bufmgr_seam::page_set_lsn::call(buffer, recptr)?;
    }

    // END_CRIT_SECTION()

    lock_buffer_unlock(buffer)?;

    if vmbuffer != InvalidBuffer {
        backend_storage_buffer_bufmgr_seams::release_buffer::call(vmbuffer);
    }

    /*
     * If the tuple has toasted out-of-line attributes, we need to delete those
     * items too.  We have to do this before releasing the buffer because we
     * need to look at the contents of the tuple, but it's OK to release the
     * content lock on the buffer first.
     */
    let relkind = relation.rd_rel.relkind;
    if relkind != RELKIND_RELATION && relkind != RELKIND_MATVIEW {
        /* toast table entries should never be recursively toasted */
        debug_assert!(!HeapTupleHasExternal(&tp));
    } else if HeapTupleHasExternal(&tp) {
        backend_access_heap_heaptoast::heap_toast_delete(mcx, relation, &tp, false)?;
    }

    /*
     * Mark tuple for invalidation from system caches at next command boundary.
     */
    backend_utils_cache_inval::cache_invalidate::CacheInvalidateHeapTuple(relation, &tp.tuple, None)?;

    /* Now we can release the buffer */
    backend_storage_buffer_bufmgr_seams::release_buffer::call(buffer);

    /*
     * Release the lmgr tuple lock, if we had it.
     */
    if have_tuple_lock {
        heapam_seam::unlock_tuple_tuplock::call(
            relation,
            tp.tuple.t_self,
            LockTupleMode::LockTupleExclusive,
        )?;
    }

    pgstat_seam::pgstat_count_heap_delete::call(
        relation.rd_id,
        relation.rd_rel.relisshared,
        relation.pgstat_enabled,
    );

    // C `heap_freetuple`s old_key_tuple when it was copied; the owned
    // FormedTuple is dropped at scope end.
    drop(old_key_tuple);

    Ok(TM_Result::TM_Ok)
}

// ===========================================================================
// simple_heap_delete — delete a tuple (heapam.c).
// ===========================================================================

/// `simple_heap_delete(relation, tid)` (heapam.c).
pub fn simple_heap_delete<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &Relation<'mcx>,
    tid: ItemPointerData,
) -> PgResult<()> {
    let mut tmfd = TM_FailureData::default();
    let cid = xact_seam::get_current_command_id::call(true)?;
    let result = heap_delete(
        mcx, relation, tid, cid, /* crosscheck */ None, /* wait */ true, &mut tmfd,
        /* changingPart */ false,
    )?;
    match result {
        TM_Result::TM_SelfModified => {
            /* Tuple was already updated in current command? */
            Err(elog_error("tuple already updated by self"))
        }
        TM_Result::TM_Ok => Ok(()),
        TM_Result::TM_Updated => Err(elog_error("tuple concurrently updated")),
        TM_Result::TM_Deleted => Err(elog_error("tuple concurrently deleted")),
        other => Err(elog_error(&format!(
            "unrecognized heap_delete status: {}",
            other as u32
        ))),
    }
}

// ===========================================================================
// compute_new_xmax_infomask — shared with the UPDATE side (heapam.c, static).
// ===========================================================================

/// `compute_new_xmax_infomask(xmax, old_infomask, old_infomask2, add_to_xmax,
/// mode, is_update, &result_xmax, &result_infomask, &result_infomask2)`
/// (heapam.c). Returns `(result_xmax, result_infomask, result_infomask2)`.
pub fn compute_new_xmax_infomask<'mcx>(
    mcx: Mcx<'mcx>,
    xmax: TransactionId,
    mut old_infomask: u16,
    old_infomask2: u16,
    add_to_xmax: TransactionId,
    mut mode: LockTupleMode,
    is_update: bool,
) -> PgResult<(TransactionId, u16, u16)> {
    use types_tuple::heaptuple::{
        HEAP_XMAX_COMMITTED, HEAP_XMAX_EXCL_LOCK, HEAP_XMAX_IS_MULTI, HEAP_XMAX_KEYSHR_LOCK,
        HEAP_XMAX_LOCK_ONLY,
    };
    use backend_access_heap_heapam_visibility::htup::HEAP_LOCKED_UPGRADED;

    debug_assert!(xact_seam::transaction_id_is_current_transaction_id::call(add_to_xmax));

    // `l5:` restart loop (the C `goto l5` optimizations).
    let new_xmax: TransactionId;
    let mut new_infomask: u16;
    let mut new_infomask2: u16;

    loop {
        new_infomask = 0;
        new_infomask2 = 0;

        if (old_infomask & HEAP_XMAX_INVALID) != 0 {
            /*
             * No previous locker; we just insert our own TransactionId. (This
             * case must be first; several blocks below come back here.)
             */
            if is_update {
                new_xmax = add_to_xmax;
                if mode == LockTupleMode::LockTupleExclusive {
                    new_infomask2 |= HEAP_KEYS_UPDATED;
                }
            } else {
                new_infomask |= HEAP_XMAX_LOCK_ONLY;
                match mode {
                    LockTupleMode::LockTupleKeyShare => {
                        new_xmax = add_to_xmax;
                        new_infomask |= HEAP_XMAX_KEYSHR_LOCK;
                    }
                    LockTupleMode::LockTupleShare => {
                        new_xmax = add_to_xmax;
                        new_infomask |= HEAP_XMAX_SHR_LOCK;
                    }
                    LockTupleMode::LockTupleNoKeyExclusive => {
                        new_xmax = add_to_xmax;
                        new_infomask |= HEAP_XMAX_EXCL_LOCK;
                    }
                    LockTupleMode::LockTupleExclusive => {
                        new_xmax = add_to_xmax;
                        new_infomask |= HEAP_XMAX_EXCL_LOCK;
                        new_infomask2 |= HEAP_KEYS_UPDATED;
                    }
                }
            }
            break;
        } else if (old_infomask & HEAP_XMAX_IS_MULTI) != 0 {
            /*
             * Currently we don't allow XMAX_COMMITTED to be set for multis.
             */
            debug_assert!(old_infomask & HEAP_XMAX_COMMITTED == 0);

            /*
             * A multixact together with LOCK_ONLY set but neither lock bit set
             * (a pg_upgraded share-locked tuple) cannot possibly be running.
             */
            if HEAP_LOCKED_UPGRADED(old_infomask) {
                old_infomask &= !HEAP_XMAX_IS_MULTI;
                old_infomask |= HEAP_XMAX_INVALID;
                continue; // goto l5
            }

            /*
             * If the XMAX is already a MultiXactId, expand it to include
             * add_to_xmax; but if all members were lockers and are all gone, we
             * can drop the IS_MULTI bit. Likewise if all lockers are gone and an
             * updater aborted.
             */
            if !multixact_seam::multi_xact_id_is_running::call(
                xmax,
                HEAP_XMAX_IS_LOCKED_ONLY(old_infomask),
            )? {
                if HEAP_XMAX_IS_LOCKED_ONLY(old_infomask)
                    || !TransactionIdDidCommit(multixact_seam::multi_xact_id_get_update_xid::call(
                        xmax,
                        old_infomask,
                    )?)?
                {
                    old_infomask &= !HEAP_XMAX_IS_MULTI;
                    old_infomask |= HEAP_XMAX_INVALID;
                    continue; // goto l5
                }
            }

            let new_status = get_mxact_status_for_lock(mode, is_update)?;
            new_xmax = multixact_seam::multi_xact_id_expand::call(xmax, add_to_xmax, new_status)?;
            let (m, m2) = GetMultiXactIdHintBits(mcx, new_xmax)?;
            new_infomask = m;
            new_infomask2 = m2;
            break;
        } else if (old_infomask & HEAP_XMAX_COMMITTED) != 0 {
            /*
             * It's a committed update, so we need to preserve him as updater of
             * the tuple.
             */
            let status = if (old_infomask2 & HEAP_KEYS_UPDATED) != 0 {
                MultiXactStatus::Update
            } else {
                MultiXactStatus::NoKeyUpdate
            };
            let new_status = get_mxact_status_for_lock(mode, is_update)?;
            new_xmax =
                multixact_seam::multi_xact_id_create::call(xmax, status, add_to_xmax, new_status)?;
            let (m, m2) = GetMultiXactIdHintBits(mcx, new_xmax)?;
            new_infomask = m;
            new_infomask2 = m2;
            break;
        } else if procarray_seam::transaction_id_is_in_progress::call(xmax)? {
            /*
             * If the XMAX is a valid, in-progress TransactionId, create a new
             * MultiXactId that includes both the old locker/updater and our own.
             */
            let old_status: MultiXactStatus;
            if HEAP_XMAX_IS_LOCKED_ONLY(old_infomask) {
                if HEAP_XMAX_IS_KEYSHR_LOCKED(old_infomask) {
                    old_status = MultiXactStatus::ForKeyShare;
                } else if HEAP_XMAX_IS_SHR_LOCKED(old_infomask) {
                    old_status = MultiXactStatus::ForShare;
                } else if HEAP_XMAX_IS_EXCL_LOCKED(old_infomask) {
                    if (old_infomask2 & HEAP_KEYS_UPDATED) != 0 {
                        old_status = MultiXactStatus::ForUpdate;
                    } else {
                        old_status = MultiXactStatus::ForNoKeyUpdate;
                    }
                } else {
                    /*
                     * LOCK_ONLY alone can occur only for a pg_upgraded page; but
                     * then TransactionIdIsInProgress should have returned false.
                     * Assume no longer locked.
                     */
                    // elog(WARNING, "LOCK_ONLY found for Xid in progress %u", xmax);
                    old_infomask |= HEAP_XMAX_INVALID;
                    old_infomask &= !HEAP_XMAX_LOCK_ONLY;
                    continue; // goto l5
                }
            } else {
                /* it's an update, but which kind? */
                if (old_infomask2 & HEAP_KEYS_UPDATED) != 0 {
                    old_status = MultiXactStatus::Update;
                } else {
                    old_status = MultiXactStatus::NoKeyUpdate;
                }
            }

            let old_mode = crate::TUPLOCK_from_mxstatus(old_status);

            /*
             * If the lock to be acquired is for the same TransactionId as the
             * existing lock, consider only the strongest of both, and restart.
             */
            if xmax == add_to_xmax {
                debug_assert!(HEAP_XMAX_IS_LOCKED_ONLY(old_infomask));
                /* acquire the strongest of both */
                if (mode as i32) < (old_mode as i32) {
                    mode = old_mode;
                }
                /* mustn't touch is_update */
                old_infomask |= HEAP_XMAX_INVALID;
                continue; // goto l5
            }

            /* otherwise, just fall back to creating a new multixact */
            let new_status = get_mxact_status_for_lock(mode, is_update)?;
            new_xmax = multixact_seam::multi_xact_id_create::call(
                xmax, old_status, add_to_xmax, new_status,
            )?;
            let (m, m2) = GetMultiXactIdHintBits(mcx, new_xmax)?;
            new_infomask = m;
            new_infomask2 = m2;
            break;
        } else if !HEAP_XMAX_IS_LOCKED_ONLY(old_infomask) && TransactionIdDidCommit(xmax)? {
            /*
             * It's a committed update, so we gotta preserve him as updater of
             * the tuple.
             */
            let status = if (old_infomask2 & HEAP_KEYS_UPDATED) != 0 {
                MultiXactStatus::Update
            } else {
                MultiXactStatus::NoKeyUpdate
            };
            let new_status = get_mxact_status_for_lock(mode, is_update)?;
            new_xmax =
                multixact_seam::multi_xact_id_create::call(xmax, status, add_to_xmax, new_status)?;
            let (m, m2) = GetMultiXactIdHintBits(mcx, new_xmax)?;
            new_infomask = m;
            new_infomask2 = m2;
            break;
        } else {
            /*
             * Can get here iff the locking/updating transaction was running when
             * the infomask was extracted, but finished before
             * TransactionIdIsInProgress got to run.  Deal with it as if there
             * was no locker at all.
             */
            old_infomask |= HEAP_XMAX_INVALID;
            continue; // goto l5
        }
    }

    Ok((new_xmax, new_infomask, new_infomask2))
}

/// `HEAP_XMAX_SHR_LOCK` (htup_details.h).
const HEAP_XMAX_SHR_LOCK: u16 =
    types_tuple::heaptuple::HEAP_XMAX_EXCL_LOCK | types_tuple::heaptuple::HEAP_XMAX_KEYSHR_LOCK;

/// `HEAP_XMAX_IS_SHR_LOCKED(infomask)` (htup_details.h).
fn HEAP_XMAX_IS_SHR_LOCKED(infomask: u16) -> bool {
    (infomask & HEAP_LOCK_MASK) == HEAP_XMAX_SHR_LOCK
}

/// `HEAP_XMAX_IS_EXCL_LOCKED(infomask)` (htup_details.h).
fn HEAP_XMAX_IS_EXCL_LOCKED(infomask: u16) -> bool {
    (infomask & HEAP_LOCK_MASK) == types_tuple::heaptuple::HEAP_XMAX_EXCL_LOCK
}

/// `HEAP_XMAX_IS_KEYSHR_LOCKED(infomask)` (htup_details.h).
fn HEAP_XMAX_IS_KEYSHR_LOCKED(infomask: u16) -> bool {
    (infomask & HEAP_LOCK_MASK) == types_tuple::heaptuple::HEAP_XMAX_KEYSHR_LOCK
}

/// `HEAP_XMAX_BITS` (htup_details.h).
const HEAP_XMAX_BITS: u16 = types_tuple::heaptuple::HEAP_XMAX_COMMITTED
    | HEAP_XMAX_INVALID
    | types_tuple::heaptuple::HEAP_XMAX_IS_MULTI
    | backend_access_heap_heapam_visibility::htup::HEAP_LOCK_MASK
    | types_tuple::heaptuple::HEAP_XMAX_LOCK_ONLY;

// ===========================================================================
// ExtractReplicaIdentity — shared with the UPDATE side (heapam.c, static).
// ===========================================================================

/// `ExtractReplicaIdentity(relation, tp, key_required, &copy)` (heapam.c).
///
/// Returns the replica-identity old-key tuple to log (always an owned copy in
/// this repo — C's `*copy` distinguishes "borrowed `tp`" from "copied", but the
/// callers only use it to decide whether to `heap_freetuple`; an owned
/// [`FormedTuple`] is dropped either way), or `None` when nothing must be
/// logged.
pub fn ExtractReplicaIdentity<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &RelationData<'_>,
    tp: &FormedTuple<'mcx>,
    key_required: bool,
) -> PgResult<Option<FormedTuple<'mcx>>> {
    let desc = &relation.rd_att;
    let replident = relation.rd_rel.relreplident;

    if !relation_is_logically_logged(relation) {
        return Ok(None);
    }

    if replident == REPLICA_IDENTITY_NOTHING {
        return Ok(None);
    }

    if replident == REPLICA_IDENTITY_FULL {
        /*
         * When logging the entire old tuple, it could contain toasted columns;
         * force them inlined.
         */
        if HeapTupleHasExternal(tp) {
            let flat = backend_access_heap_heaptoast::toast_flatten_tuple(mcx, tp, desc)?;
            return Ok(Some(flat));
        }
        // C returns the borrowed `tp` (*copy=false); we materialize an owned
        // copy so the caller holds one type uniformly.
        return Ok(Some(clone_formed(mcx, tp)?));
    }

    /* if the key isn't required and we're only logging the key, we're done */
    if !key_required {
        return Ok(None);
    }

    /* find out the replica identity columns */
    let idattrs = relcache_seam::relation_get_index_attr_bitmap::call(
        mcx,
        relation,
        backend_utils_cache_relcache_seams::IndexAttrBitmapKind::Identity,
    )?;

    /*
     * If there's no defined replica identity columns, treat as !key_required.
     */
    if backend_nodes_core_seams::bms_is_empty::call(idattrs.as_deref()) {
        return Ok(None);
    }

    /*
     * Construct a new tuple containing only the replica identity columns, with
     * nulls elsewhere.  While we're at it, assert that the replica identity
     * columns aren't null.
     */
    let columns = backend_access_common_heaptuple::heap_deform_tuple(mcx, &tp.tuple, desc, &tp.data)?;
    let mut values: Vec<Datum> = Vec::with_capacity(columns.len());
    let mut nulls: Vec<bool> = Vec::with_capacity(columns.len());
    for (v, n) in columns {
        values.push(v);
        nulls.push(n);
    }

    let natts = desc.natts;
    for i in 0..natts.max(0) as usize {
        if backend_nodes_core_seams::bms_is_member::call(
            (i as i32) + 1 - (FirstLowInvalidHeapAttributeNumber as i32),
            idattrs.as_deref(),
        ) {
            debug_assert!(!nulls[i]);
        } else {
            nulls[i] = true;
        }
    }

    let key_tuple = backend_access_common_heaptuple::heap_form_tuple(mcx, desc, &values, &nulls)
        .map_err(map_heaptuple_error)?;

    // C `bms_free(idattrs)` — the owned PgBox is dropped at scope end.
    drop(idattrs);

    /*
     * If the key tuple still has toasted columns, force them inlined.
     */
    if HeapTupleHasExternal(&key_tuple) {
        let flat = backend_access_heap_heaptoast::toast_flatten_tuple(mcx, &key_tuple, desc)?;
        return Ok(Some(flat));
    }

    Ok(Some(key_tuple))
}

// ===========================================================================
// Small helpers.
// ===========================================================================

/// Materialize the on-page tuple at `(buffer, tid)` into `mcx`: C's
/// `tp.t_data = (HeapTupleHeader) PageGetItem(page, lp); tp.t_len =
/// ItemIdGetLength(lp); tp.tuple.t_self = *tid; tp.t_tableOid = RelationGetRelid`.
/// Reads both the header and the user-data area (so the full `FormedTuple` is
/// available for toast / replica identity).
fn read_on_page_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    rel_id: Oid,
    buffer: Buffer,
    tid: ItemPointerData,
) -> PgResult<FormedTuple<'mcx>> {
    let offnum = ItemPointerGetOffsetNumber(&tid);
    let mut out: Option<(HeapTupleHeaderData<'mcx>, mcx::PgVec<'mcx, u8>, u32)> = None;
    bufmgr_seam::with_buffer_page::call(buffer, &mut |page_bytes| {
        let page = PageRef::new(page_bytes)?;
        let item_id = PageGetItemId(&page, offnum)?;
        debug_assert!(item_id.has_storage());
        let item = PageGetItem(&page, &item_id)?;
        let hdr = HeapTupleHeaderData::read_on_page(mcx, item)?;
        // The user-data area is the on-disk image past the header.
        let mut data = mcx::PgVec::new_in(mcx);
        for &b in &item[SizeofHeapTupleHeader..] {
            data.push(b);
        }
        out = Some((hdr, data, item.len() as u32));
        Ok(())
    })?;
    let (hdr, data, t_len) = out.expect("with_buffer_page closure must have run");
    let tuple = mcx::alloc_in(
        mcx,
        HeapTupleData {
            t_len,
            t_self: tid,
            t_tableOid: rel_id,
            t_data: Some(mcx::alloc_in(mcx, hdr)?),
        },
    )?;
    Ok(FormedTuple { tuple, data })
}

/// `data_ref(tp)` — `tp->t_data` as a shared header reference.
fn data_ref<'a, 'mcx>(tp: &'a FormedTuple<'mcx>) -> &'a HeapTupleHeaderData<'mcx> {
    tp.tuple
        .t_data
        .as_ref()
        .expect("heap_delete: tuple has no t_data")
}

/// `data_mut(tp)` — `tp->t_data` as a mutable header reference.
fn data_mut<'a, 'mcx>(tp: &'a mut FormedTuple<'mcx>) -> &'a mut HeapTupleHeaderData<'mcx> {
    tp.tuple
        .t_data
        .as_mut()
        .expect("heap_delete: tuple has no t_data")
}

/// `LockBuffer(buffer, BUFFER_LOCK_UNLOCK)` — release the content lock, keeping
/// the pin (BUFFER_LOCK_UNLOCK == 0).
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
    Ok(backend_storage_buffer_bufmgr_seams::buffer_get_block_number::call(buffer))
}

/// `HeapTupleHasExternal(tuple)`.
fn HeapTupleHasExternal(tp: &FormedTuple<'_>) -> bool {
    tp.tuple
        .t_data
        .as_ref()
        .is_some_and(|hdr| (hdr.t_infomask & HEAP_HASEXTERNAL) != 0)
}

/// `HeapTupleHeaderGetCmax(tup)` via the combo-cid owner seam.
pub(crate) fn HeapTupleHeaderGetCmax(hdr: &HeapTupleHeaderData<'_>) -> CommandId {
    combocid_seam::heap_tuple_header_get_cmax::call(hdr)
}

/// `get_mxact_status_for_lock(mode, is_update)` (heapam.c) — the multixact
/// status held for a tuple lock of the given mode. `tupleLockExtraInfo[mode]`'s
/// `lockstatus`/`updstatus`; an entry of `-1` is an invalid lock mode (C
/// `elog(ERROR)`).
fn get_mxact_status_for_lock(mode: LockTupleMode, is_update: bool) -> PgResult<MultiXactStatus> {
    // tupleLockExtraInfo[] (heapam.c) — lockstatus / updstatus per mode.
    // KeyShare: lock=ForKeyShare, upd=-1
    // Share:    lock=ForShare,    upd=-1
    // NoKeyEx:  lock=ForNoKeyUpdate, upd=NoKeyUpdate
    // Exclusive:lock=ForUpdate,   upd=Update
    let retval: Option<MultiXactStatus> = if is_update {
        match mode {
            LockTupleMode::LockTupleKeyShare => None,
            LockTupleMode::LockTupleShare => None,
            LockTupleMode::LockTupleNoKeyExclusive => Some(MultiXactStatus::NoKeyUpdate),
            LockTupleMode::LockTupleExclusive => Some(MultiXactStatus::Update),
        }
    } else {
        match mode {
            LockTupleMode::LockTupleKeyShare => Some(MultiXactStatus::ForKeyShare),
            LockTupleMode::LockTupleShare => Some(MultiXactStatus::ForShare),
            LockTupleMode::LockTupleNoKeyExclusive => Some(MultiXactStatus::ForNoKeyUpdate),
            LockTupleMode::LockTupleExclusive => Some(MultiXactStatus::ForUpdate),
        }
    };
    retval.ok_or_else(|| {
        ereport(ERROR)
            .errmsg_internal(format!(
                "invalid lock tuple mode {}/{}",
                mode as i32, is_update
            ))
            .into_error()
    })
}

/// `TransactionIdDidCommit(xid)` — clog lookup through the transam owner seam,
/// threading C's `TransactionXmin` (snapmgr.c global) via the snapmgr seam.
fn TransactionIdDidCommit(xid: TransactionId) -> PgResult<bool> {
    let transaction_xmin = backend_utils_time_snapmgr_pc_seams::transaction_xmin::call()?;
    transam_seam::transaction_id_did_commit::call(xid, transaction_xmin)
}

/// `RelationIsAccessibleInLogicalDecoding(relation)` (utils/rel.h).
fn relation_is_accessible_in_logical_decoding(relation: &RelationData<'_>) -> bool {
    let wal = backend_access_transam_xlog_seams::wal_level::call();
    let xlog_logical_info_active = wal >= types_wal::WalLevel::Logical;
    xlog_logical_info_active
        && relcache_seam::relation_needs_wal::call(relation)
        && (catalog_seam::is_catalog_relation::call(relation)
            || relation_is_used_as_catalog_table(relation))
}

/// `RelationIsLogicallyLogged(relation)` (utils/rel.h).
fn relation_is_logically_logged(relation: &RelationData<'_>) -> bool {
    let wal = backend_access_transam_xlog_seams::wal_level::call();
    let xlog_logical_info_active = wal >= types_wal::WalLevel::Logical;
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
            .is_some_and(|o| o.user_catalog_table)
}

// --- header-field setters (htup_details.h inline functions) ----------------

/// `HeapTupleHeaderSetXmax(tup, xid)`.
fn HeapTupleHeaderSetXmax(hdr: &mut HeapTupleHeaderData<'_>, xid: TransactionId) {
    if let HeapTupleHeaderChoice::THeap(f) = &mut hdr.t_choice {
        f.t_xmax = xid;
    }
}

/// `HeapTupleHeaderSetCmax(tup, cid, iscombo)` — `t_field3.t_cid = cid;
/// iscombo ? (t_infomask |= HEAP_COMBOCID) : (t_infomask &= ~HEAP_COMBOCID)`.
fn HeapTupleHeaderSetCmax(hdr: &mut HeapTupleHeaderData<'_>, cid: CommandId, iscombo: bool) {
    debug_assert!(hdr.t_infomask & HEAP_MOVED == 0);
    if let HeapTupleHeaderChoice::THeap(f) = &mut hdr.t_choice {
        f.t_field3 = HeapTupleField3::TCid(cid);
    }
    if iscombo {
        hdr.t_infomask |= HEAP_COMBOCID;
    } else {
        hdr.t_infomask &= !HEAP_COMBOCID;
    }
}

/// `HeapTupleHeaderClearHotUpdated(tup)` — `t_infomask2 &= ~HEAP_HOT_UPDATED`.
fn HeapTupleHeaderClearHotUpdated(hdr: &mut HeapTupleHeaderData<'_>) {
    hdr.t_infomask2 &= !HEAP_HOT_UPDATED;
}

/// `HeapTupleHeaderSetMovedPartitions(tup)` — `ItemPointerSetMovedPartitions(
/// &tup->t_ctid)`: block = MovedPartitionsBlockNumber, offset =
/// MovedPartitionsOffsetNumber.
fn HeapTupleHeaderSetMovedPartitions(hdr: &mut HeapTupleHeaderData<'_>) {
    // MovedPartitionsBlockNumber = InvalidBlockNumber, ip_blkid bits, with
    // MovedPartitionsOffsetNumber == 0xfffd.
    hdr.t_ctid.ip_blkid = types_tuple::heaptuple::BlockIdData::new(MovedPartitionsBlockNumber);
    hdr.t_ctid.ip_posid = MovedPartitionsOffsetNumber;
}

/// `MovedPartitionsBlockNumber` (itemptr.h) == `InvalidBlockNumber` low bits.
const MovedPartitionsBlockNumber: BlockNumber = 0xffff_ffff;
/// `MovedPartitionsOffsetNumber` (itemptr.h).
const MovedPartitionsOffsetNumber: OffsetNumber = 0xfffd;

/// Clone a [`FormedTuple`]'s header + data into `mcx` (C's borrowed-`tp` return
/// arm materialized as an owned copy).
fn clone_formed<'mcx>(mcx: Mcx<'mcx>, tp: &FormedTuple<'mcx>) -> PgResult<FormedTuple<'mcx>> {
    let hdr = data_ref_formed(tp).clone();
    let mut data = mcx::PgVec::new_in(mcx);
    for &b in tp.data.iter() {
        data.push(b);
    }
    let tuple = mcx::alloc_in(
        mcx,
        HeapTupleData {
            t_len: tp.tuple.t_len,
            t_self: tp.tuple.t_self,
            t_tableOid: tp.tuple.t_tableOid,
            t_data: Some(mcx::alloc_in(mcx, hdr)?),
        },
    )?;
    Ok(FormedTuple { tuple, data })
}

fn data_ref_formed<'a, 'mcx>(tp: &'a FormedTuple<'mcx>) -> &'a HeapTupleHeaderData<'mcx> {
    tp.tuple
        .t_data
        .as_ref()
        .expect("ExtractReplicaIdentity: tuple has no t_data")
}

/// `elog(ERROR, msg)` builder.
fn elog_error(msg: &str) -> types_error::PgError {
    ereport(ERROR).errmsg_internal(msg.to_string()).into_error()
}

/// Map [`HeapTupleError`] from `heap_form_tuple` to the `PgError` C raises.
fn map_heaptuple_error(
    err: backend_access_common_heaptuple::HeapTupleError,
) -> types_error::PgError {
    use backend_access_common_heaptuple::HeapTupleError;
    match err {
        HeapTupleError::Pg(e) => e,
        other => ereport(ERROR)
            .errmsg_internal(format!("heap_form_tuple: {other:?}"))
            .into_error(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `get_mxact_status_for_lock` mirrors C's `tupleLockExtraInfo[]` lock /
    /// update status columns; the `-1` (None) entries are the invalid-mode
    /// `elog(ERROR)` cases.
    #[test]
    fn mxact_status_for_lock_matches_table() {
        // lock (is_update = false)
        assert_eq!(
            get_mxact_status_for_lock(LockTupleMode::LockTupleKeyShare, false).unwrap(),
            MultiXactStatus::ForKeyShare
        );
        assert_eq!(
            get_mxact_status_for_lock(LockTupleMode::LockTupleShare, false).unwrap(),
            MultiXactStatus::ForShare
        );
        assert_eq!(
            get_mxact_status_for_lock(LockTupleMode::LockTupleNoKeyExclusive, false).unwrap(),
            MultiXactStatus::ForNoKeyUpdate
        );
        assert_eq!(
            get_mxact_status_for_lock(LockTupleMode::LockTupleExclusive, false).unwrap(),
            MultiXactStatus::ForUpdate
        );
        // update (is_update = true): KeyShare / Share are invalid (-1).
        assert!(get_mxact_status_for_lock(LockTupleMode::LockTupleKeyShare, true).is_err());
        assert!(get_mxact_status_for_lock(LockTupleMode::LockTupleShare, true).is_err());
        assert_eq!(
            get_mxact_status_for_lock(LockTupleMode::LockTupleNoKeyExclusive, true).unwrap(),
            MultiXactStatus::NoKeyUpdate
        );
        assert_eq!(
            get_mxact_status_for_lock(LockTupleMode::LockTupleExclusive, true).unwrap(),
            MultiXactStatus::Update
        );
    }

    /// The `HEAP_XMAX_IS_*_LOCKED` predicates compare the masked lock bits to the
    /// exact lock pattern (htup_details.h).
    #[test]
    fn xmax_locked_predicates_match_c() {
        use types_tuple::heaptuple::{HEAP_XMAX_EXCL_LOCK, HEAP_XMAX_KEYSHR_LOCK};
        assert!(HEAP_XMAX_IS_EXCL_LOCKED(HEAP_XMAX_EXCL_LOCK));
        assert!(!HEAP_XMAX_IS_EXCL_LOCKED(HEAP_XMAX_KEYSHR_LOCK));
        assert!(HEAP_XMAX_IS_KEYSHR_LOCKED(HEAP_XMAX_KEYSHR_LOCK));
        assert!(HEAP_XMAX_IS_SHR_LOCKED(HEAP_XMAX_SHR_LOCK));
        // SHR_LOCK is EXCL|KEYSHR; EXCL alone is not shr-locked.
        assert!(!HEAP_XMAX_IS_SHR_LOCKED(HEAP_XMAX_EXCL_LOCK));
    }

    /// `HeapTupleHeaderSetCmax` sets `t_field3.t_cid` and toggles HEAP_COMBOCID;
    /// `HeapTupleHeaderClearHotUpdated` clears the HOT-updated infomask2 bit.
    #[test]
    fn header_mutators_match_c() {
        use mcx::MemoryContext;
        use types_tuple::heaptuple::{
            BlockIdData, HeapTupleFields, ItemPointerData,
        };
        let ctx = MemoryContext::new("delete_header_mutators");
        let mcx = ctx.mcx();
        let mut hdr = HeapTupleHeaderData {
            t_choice: HeapTupleHeaderChoice::THeap(HeapTupleFields {
                t_xmin: 0,
                t_xmax: 0,
                t_field3: HeapTupleField3::TCid(0),
            }),
            t_ctid: ItemPointerData { ip_blkid: BlockIdData::new(0), ip_posid: 0 },
            t_infomask2: HEAP_HOT_UPDATED,
            t_infomask: 0,
            t_hoff: 24,
            t_bits: mcx::PgVec::new_in(mcx),
        };

        HeapTupleHeaderSetCmax(&mut hdr, 9, true);
        assert!(matches!(hdr.t_choice, HeapTupleHeaderChoice::THeap(ref f) if matches!(f.t_field3, HeapTupleField3::TCid(9))));
        assert_eq!(hdr.t_infomask & HEAP_COMBOCID, HEAP_COMBOCID);
        HeapTupleHeaderSetCmax(&mut hdr, 9, false);
        assert_eq!(hdr.t_infomask & HEAP_COMBOCID, 0);

        HeapTupleHeaderClearHotUpdated(&mut hdr);
        assert_eq!(hdr.t_infomask2 & HEAP_HOT_UPDATED, 0);

        HeapTupleHeaderSetMovedPartitions(&mut hdr);
        assert_eq!(hdr.t_ctid.ip_posid, MovedPartitionsOffsetNumber);
    }
}
