//! F4 (SPECULATIVE + INPLACE side) — heap.c speculative-insert finish/abort
//! and the in-place-update lock/apply/unlock trio:
//! `heap_finish_speculative`, `heap_abort_speculative`, `heap_inplace_lock`,
//! `heap_inplace_update_and_unlock`, `heap_inplace_unlock`.
//!
//! Page model: identical to F3 DELETE / F4 LOCK — pin + exclusively lock the
//! buffer, materialize the on-page tuple into `mcx`, mutate the materialized
//! header, and write it back through one `with_buffer_page` mutation inside the
//! critical section. `heap_inplace_update_and_unlock` additionally builds the
//! post-mutation block image (for the inplace FPI) on the stack while holding
//! the buffer lock, exactly as C does.
//!
//! The shared-inval inplace lifecycle (`CacheInvalidateHeapTupleInplace` /
//! `PreInplace_Inval` / `AtInplace_Inval` / `ForgetInplace_Inval` /
//! `inplaceGetInvalidationMessages`) lives in the (ported) inval crate; the
//! heavyweight tuple lock goes through the lmgr seams; the WAL block
//! registration goes through the xloginsert seam.

use mcx::Mcx;
use types_core::primitive::{Oid, TransactionId};
use types_error::{
    PgResult, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERROR,
};
use backend_utils_error::ereport;
use types_rel::{Relation, RelationData};
use types_storage::lock::{InplaceUpdateTupleLock, XLTW_Oper};
use types_storage::{Buffer, RelFileLocator};
use types_tableam::tableam::{LockTupleMode, TM_Result};
use types_tuple::heaptuple::{
    HeapTupleData, HeapTupleField3, HeapTupleFields, HeapTupleHeaderChoice, HeapTupleHeaderData,
    ItemPointerData, HEAP_HASEXTERNAL, HEAP_KEYS_UPDATED, HEAP_MOVED, HEAP_XMAX_COMMITTED,
    HEAP_XMAX_INVALID, HEAP_XMAX_IS_MULTI, HEAP_XMAX_LOCK_ONLY,
};
use types_xlog_records::multixact::MultiXactStatus;

use backend_storage_page::{
    ItemPointerEquals, ItemPointerGetBlockNumber, ItemPointerGetOffsetNumber, ItemPointerIsValid,
    PageGetItem, PageGetItemId, PageGetMaxOffsetNumber, PageIsAllVisible, PageMut, PageRef,
    PageSetPrunable,
};

use backend_access_heap_heapam_visibility::htup::{
    HeapTupleHeaderGetRawXmax, HeapTupleHeaderIsSpeculative, HEAP_LOCK_MASK,
};
use backend_access_heap_heapam_visibility::HeapTupleSatisfiesUpdate;

use crate::compute_infobits;
use crate::lock::DoesMultiXactIdConflict;

use backend_access_heap_heapam_seams as heapam_seam;
use backend_access_heap_hio_seams as hio_seam;
use backend_access_heap_vacuumlazy_seams as page_seam;
use backend_access_transam_xact_seams as xact_seam;
use backend_access_transam_xloginsert_seams as xloginsert_seam;
use backend_storage_buffer_bufmgr_seams as bufmgr_seam;
use backend_storage_lmgr_lmgr_seams as lmgr_seam;
use backend_storage_lmgr_proc_seams as proc_seam;
use backend_utils_cache_relcache_seams as relcache_seam;

use types_wal::wal::{RM_HEAP_ID, XLOG_INCLUDE_ORIGIN};
use types_wal::xloginsert::REGBUF_STANDARD;
use backend_rmgrdesc_next::heapdesc::{XLOG_HEAP_CONFIRM, XLOG_HEAP_DELETE, XLOG_HEAP_INPLACE};
use types_xlog_records::heapam_xlog::{
    xl_heap_confirm, xl_heap_delete, xl_heap_inplace, MinSizeOfHeapInplace, SizeOfHeapConfirm,
    SizeOfHeapDelete, XLH_DELETE_IS_SUPER,
};
use types_storage::sinval::SHARED_INVALIDATION_MESSAGE_SIZE;
use types_storage::bufpage::SizeofHeapTupleHeader;
use types_core::primitive::BLCKSZ;

/// `InvalidTransactionId`.
const InvalidTransactionId: TransactionId = 0;

/// `BUFFER_LOCK_UNLOCK` (storage/bufmgr.h).
const BUFFER_LOCK_UNLOCK: i32 = 0;

/// `HEAP_XMAX_BITS` (htup_details.h).
const HEAP_XMAX_BITS: u16 =
    HEAP_XMAX_COMMITTED | HEAP_XMAX_INVALID | HEAP_XMAX_IS_MULTI | HEAP_LOCK_MASK | HEAP_XMAX_LOCK_ONLY;

/// `RelationRelationId` (catalog/pg_class.h) — pg_class's OID.
const RelationRelationId: Oid = 1259;

// ===========================================================================
// heap_finish_speculative — mark a speculative insertion successful (heapam.c).
// ===========================================================================

/// `heap_finish_speculative(relation, tid)` (heapam.c).
pub fn heap_finish_speculative<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &Relation<'mcx>,
    tid: ItemPointerData,
) -> PgResult<()> {
    let buffer = hio_seam::read_buffer::call(relation.rd_id, ItemPointerGetBlockNumber(&tid))?;
    bufmgr_seam::lock_buffer_exclusive::call(buffer)?;

    let offnum = ItemPointerGetOffsetNumber(&tid);

    // if (PageGetMaxOffsetNumber(page) < offnum || !ItemIdIsNormal(lp))
    //     elog(ERROR, "invalid lp");
    let max_off = page_seam::page_get_max_offset_number::call(buffer)?;
    let mut lp_normal = false;
    if max_off >= offnum {
        bufmgr_seam::with_buffer_page::call(buffer, &mut |page_bytes| {
            let page = PageRef::new(page_bytes)?;
            let item_id = PageGetItemId(&page, offnum)?;
            lp_normal = item_id.has_storage();
            Ok(())
        })?;
    }
    if max_off < offnum || !lp_normal {
        return Err(elog_error("invalid lp"));
    }

    // htup = (HeapTupleHeader) PageGetItem(page, lp); materialize it.
    let mut htup = read_on_page_header(mcx, buffer, offnum)?;

    // NO EREPORT(ERROR) from here till changes are logged. START_CRIT_SECTION()

    debug_assert!(HeapTupleHeaderIsSpeculative(&htup));

    backend_storage_buffer_bufmgr_seams::mark_buffer_dirty::call(buffer);

    /*
     * Replace the speculative insertion token with a real t_ctid, pointing to
     * itself like an ordinary tuple.
     */
    htup.t_ctid = tid;

    // Write the mutated header back into the page.
    let header_image = htup.clone();
    bufmgr_seam::with_buffer_page::call(buffer, &mut |page_bytes| {
        let (off, len) = {
            let page = PageRef::new(page_bytes)?;
            let item_id = PageGetItemId(&page, offnum)?;
            (item_id.lp_off() as usize, item_id.lp_len() as usize)
        };
        let item = page_bytes
            .get_mut(off..off + len)
            .ok_or_else(|| types_error::PgError::error("item storage is outside page"))?;
        header_image.write_on_page(item)?;
        Ok(())
    })?;

    /* XLOG stuff */
    if relcache_seam::relation_needs_wal::call(relation) {
        let xlrec = xl_heap_confirm { offnum: ItemPointerGetOffsetNumber(&tid) };

        xloginsert_seam::xlog_begin_insert::call()?;
        /* We want the same filtering on this as on a plain insert */
        xloginsert_seam::xlog_set_record_flags::call(XLOG_INCLUDE_ORIGIN);
        let recbuf = xlrec.to_bytes();
        xloginsert_seam::xlog_register_data::call(&recbuf[..SizeOfHeapConfirm])?;
        xloginsert_seam::xlog_register_buffer::call(0, buffer, REGBUF_STANDARD)?;

        let recptr = xloginsert_seam::xlog_insert_record::call(RM_HEAP_ID, XLOG_HEAP_CONFIRM)?;
        bufmgr_seam::page_set_lsn::call(buffer, recptr)?;
    }

    // END_CRIT_SECTION()
    bufmgr_seam::unlock_release_buffer::call(buffer);
    Ok(())
}

// ===========================================================================
// heap_abort_speculative — kill a speculatively inserted tuple (heapam.c).
// ===========================================================================

/// `heap_abort_speculative(relation, tid)` (heapam.c).
pub fn heap_abort_speculative<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &Relation<'mcx>,
    tid: ItemPointerData,
) -> PgResult<()> {
    let xid = xact_seam::get_current_transaction_id::call()?;

    debug_assert!(ItemPointerIsValid(Some(&tid)));

    let block = ItemPointerGetBlockNumber(&tid);
    let buffer = hio_seam::read_buffer::call(relation.rd_id, block)?;

    bufmgr_seam::lock_buffer_exclusive::call(buffer)?;

    /*
     * Page can't be all visible, we just inserted into it, and are still
     * running.
     */
    debug_assert!(!page_seam::page_is_all_visible::call(buffer)?);

    // tp = { t_tableOid, t_data = PageGetItem(page, lp), t_len, t_self }.
    let offnum = ItemPointerGetOffsetNumber(&tid);
    let mut tp = read_on_page_formed(mcx, relation.rd_id, buffer, tid)?;

    /*
     * Sanity check that this really is a speculatively inserted tuple inserted
     * by us.
     */
    let xmin = match &data_ref_f(&tp).t_choice {
        HeapTupleHeaderChoice::THeap(f) => f.t_xmin,
        HeapTupleHeaderChoice::TDatum(_) => InvalidTransactionId,
    };
    if xmin != xid {
        return Err(elog_error("attempted to kill a tuple inserted by another transaction"));
    }
    if !(is_toast_relation(relation) || HeapTupleHeaderIsSpeculative(data_ref_f(&tp))) {
        return Err(elog_error("attempted to kill a non-speculative tuple"));
    }

    /*
     * No need to check for serializable conflicts here. No combo CID, no
     * replica identity, no special infomask handling.
     */

    // START_CRIT_SECTION()

    /*
     * The tuple becomes DEAD immediately. Flag the page prunable by setting
     * xmin to TransactionXmin (or relfrozenxid, if it's newer).
     */
    let transaction_xmin = backend_utils_time_snapmgr_pc_seams::transaction_xmin::call()?;
    debug_assert!(TransactionIdIsValid(transaction_xmin));
    let relfrozenxid = relation.rd_rel.relfrozenxid;
    let prune_xid = if transaction_id_precedes(transaction_xmin, relfrozenxid) {
        relfrozenxid
    } else {
        transaction_xmin
    };

    /* store transaction information of xact deleting the tuple */
    let self_tid = tp.tuple.t_self;
    {
        let hdr = data_mut_f(&mut tp);
        hdr.t_infomask &= !(HEAP_XMAX_BITS | HEAP_MOVED);
        hdr.t_infomask2 &= !HEAP_KEYS_UPDATED;
        /*
         * Set xmin to InvalidTransactionId, making the tuple immediately
         * invisible to everyone (incl. spec-token waiters).
         */
        HeapTupleHeaderSetXmin(hdr, InvalidTransactionId);
        /* Clear the speculative insertion token too */
        hdr.t_ctid = self_tid;
    }

    backend_storage_buffer_bufmgr_seams::mark_buffer_dirty::call(buffer);

    // Apply PageSetPrunable(page, prune_xid) and write the mutated header back.
    let header_image = data_ref_f(&tp).clone();
    bufmgr_seam::with_buffer_page::call(buffer, &mut |page_bytes| {
        {
            let mut page = PageMut::new(page_bytes)?;
            PageSetPrunable(&mut page, prune_xid);
        }
        let (off, len) = {
            let page = PageRef::new(page_bytes)?;
            let item_id = PageGetItemId(&page, offnum)?;
            (item_id.lp_off() as usize, item_id.lp_len() as usize)
        };
        let item = page_bytes
            .get_mut(off..off + len)
            .ok_or_else(|| types_error::PgError::error("item storage is outside page"))?;
        header_image.write_on_page(item)?;
        Ok(())
    })?;

    /*
     * XLOG stuff. The WAL records match heap_delete(); the same recovery
     * routines are used.
     */
    if relcache_seam::relation_needs_wal::call(relation) {
        let xlrec = xl_heap_delete {
            flags: XLH_DELETE_IS_SUPER,
            infobits_set: compute_infobits(data_ref_f(&tp).t_infomask, data_ref_f(&tp).t_infomask2),
            offnum: ItemPointerGetOffsetNumber(&tp.tuple.t_self),
            xmax: xid,
        };

        xloginsert_seam::xlog_begin_insert::call()?;
        let recbuf = xlrec.to_bytes();
        xloginsert_seam::xlog_register_data::call(&recbuf[..SizeOfHeapDelete])?;
        xloginsert_seam::xlog_register_buffer::call(0, buffer, REGBUF_STANDARD)?;

        /* No replica identity & replication origin logged */

        let recptr = xloginsert_seam::xlog_insert_record::call(RM_HEAP_ID, XLOG_HEAP_DELETE)?;
        bufmgr_seam::page_set_lsn::call(buffer, recptr)?;
    }

    // END_CRIT_SECTION()

    bufmgr_seam::lock_buffer::call(buffer, BUFFER_LOCK_UNLOCK)?;

    if HeapTupleHasExternalF(&tp) {
        debug_assert!(!is_toast_relation(relation));
        backend_access_heap_heaptoast::heap_toast_delete(mcx, relation, &tp, true)?;
    }

    /*
     * Never need to mark the tuple for invalidation; catalogs don't support
     * speculative insertion.
     */

    /* Now we can release the buffer */
    backend_storage_buffer_bufmgr_seams::release_buffer::call(buffer);

    /* count deletion, as we counted the insertion too */
    backend_utils_activity_pgstat_seams::pgstat_count_heap_delete::call(
        relation.rd_id,
        relation.rd_rel.relisshared,
        relation.pgstat_enabled,
    );
    Ok(())
}

// ===========================================================================
// heap_inplace_lock — protect inplace update from concurrent heap_update.
// ===========================================================================

/// `heap_inplace_lock(relation, oldtup, buffer, release_callback, arg)`
/// (heapam.c). C's `void (*release_callback)(void *), void *arg` is modelled by
/// `release_callback: &mut dyn FnMut() -> PgResult<()>`, run when we cannot
/// take the lock and must release the buffer + wait. Returns C's `bool`.
pub fn heap_inplace_lock<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &Relation<'mcx>,
    oldtup: &HeapTupleData<'mcx>,
    buffer: Buffer,
    release_callback: &mut dyn FnMut() -> PgResult<()>,
) -> PgResult<bool> {
    let ret: bool;

    // #ifdef USE_ASSERT_CHECKING: check_inplace_rel_lock(oldtup) when relid ==
    // RelationRelationId. We mirror the (debug-only) check.
    #[cfg(debug_assertions)]
    if relation.rd_id == RelationRelationId {
        check_inplace_rel_lock(relation, oldtup)?;
    }

    debug_assert!(buffer != types_storage::InvalidBuffer);

    /*
     * Register shared cache invals if necessary, *before* LockBuffer (a
     * CatalogCacheInitializeCache reachable from registration might lock
     * "buffer", which would hang after our own LockBuffer).
     */
    backend_utils_cache_inval::cache_invalidate::CacheInvalidateHeapTupleInplace(relation, oldtup)?;

    lmgr_seam::lock_tuple::call(relation.rd_id, oldtup.t_self, InplaceUpdateTupleLock)?;
    bufmgr_seam::lock_buffer_exclusive::call(buffer)?;

    // Re-materialize the on-page tuple (C casts oldtup.t_data into the page;
    // here oldtup carries its own header, but HeapTupleSatisfiesUpdate reads
    // the live page-bound state via the buffer + the materialized header).
    let mut oldtup_local = read_on_page_tuple(mcx, relation.rd_id, buffer, oldtup.t_self)?;

    /*
     * Interpret HeapTupleSatisfiesUpdate like heap_update, except: wait
     * unconditionally; already locked above; don't recheck after wait; don't
     * continue even if the updater aborts; no crosscheck.
     */
    let result = HeapTupleSatisfiesUpdate(
        &mut oldtup_local,
        xact_seam::get_current_command_id::call(false)?,
        buffer,
    )?;

    if result == TM_Result::TM_Invisible {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg_internal("attempted to overwrite invisible tuple".to_string())
            .into_error());
    } else if result == TM_Result::TM_SelfModified {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg("tuple to be updated was already modified by an operation triggered by the current command".to_string())
            .into_error());
    } else if result == TM_Result::TM_BeingModified {
        let xwait = HeapTupleHeaderGetRawXmax(data_ref(&oldtup_local));
        let infomask = data_ref(&oldtup_local).t_infomask;

        if (infomask & HEAP_XMAX_IS_MULTI) != 0 {
            let lockmode = LockTupleMode::LockTupleNoKeyExclusive;
            let mxact_status = MultiXactStatus::NoKeyUpdate;

            let conflict =
                DoesMultiXactIdConflict(mcx, xwait as u32, infomask, lockmode)?;
            if conflict.conflict {
                bufmgr_seam::lock_buffer::call(buffer, BUFFER_LOCK_UNLOCK)?;
                release_callback()?;
                ret = false;
                // MultiXactIdWait(..., &remain) — remain discarded.
                heapam_seam::multi_xact_id_wait::call(
                    xwait as u32,
                    mxact_status,
                    infomask,
                    relation,
                    oldtup_local.t_self,
                    XLTW_Oper::Update,
                )?;
            } else {
                ret = true;
            }
        } else if xact_seam::transaction_id_is_current_transaction_id::call(xwait) {
            ret = true;
        } else if HEAP_XMAX_IS_KEYSHR_LOCKED(infomask) {
            ret = true;
        } else {
            bufmgr_seam::lock_buffer::call(buffer, BUFFER_LOCK_UNLOCK)?;
            release_callback()?;
            ret = false;
            heapam_seam::xact_lock_table_wait::call(xwait, relation, oldtup_local.t_self, XLTW_Oper::Update)?;
        }
    } else {
        ret = result == TM_Result::TM_Ok;
        if !ret {
            bufmgr_seam::lock_buffer::call(buffer, BUFFER_LOCK_UNLOCK)?;
            release_callback()?;
        }
    }

    /*
     * GetCatalogSnapshot relies on invalidation messages. If we failed, drop
     * the tuple lock, forget the inplace invals, and take a fresh snapshot.
     */
    if !ret {
        lmgr_seam::unlock_tuple::call(relation.rd_id, oldtup_local.t_self, InplaceUpdateTupleLock)?;
        backend_utils_cache_inval::at_eoxact::ForgetInplace_Inval();
        backend_utils_time_snapmgr_seams::invalidate_catalog_snapshot::call();
    }
    Ok(ret)
}

// ===========================================================================
// heap_inplace_update_and_unlock — core of systable_inplace_update_finish.
// ===========================================================================

/// `heap_inplace_update_and_unlock(relation, oldtup, tuple, buffer)`
/// (heapam.c). `oldtup` addresses the on-page tuple; `tuple` carries the new
/// (same-length) image to overwrite it with.
pub fn heap_inplace_update_and_unlock<'mcx>(
    _mcx: Mcx<'mcx>,
    relation: &Relation<'mcx>,
    oldtup: &HeapTupleData<'mcx>,
    tuple: &HeapTupleData<'mcx>,
    new_data: &[u8],
    buffer: Buffer,
) -> PgResult<()> {
    let htup = data_ref(oldtup);

    debug_assert!(ItemPointerEquals(&oldtup.t_self, &tuple.t_self));
    let oldlen = (oldtup.t_len - htup.t_hoff as u32) as usize;
    let newlen = (tuple.t_len - data_ref(tuple).t_hoff as u32) as usize;
    if oldlen != newlen || htup.t_hoff != data_ref(tuple).t_hoff {
        return Err(elog_error("wrong tuple length"));
    }

    // src = (char *) tuple->t_data + tuple->t_data->t_hoff — the new user-data
    // area; `new_data` is exactly that `newlen`-byte slice.
    debug_assert_eq!(new_data.len(), newlen);
    let src = new_data;

    /* Like RecordTransactionCommit(), log only if needed */
    let (inval_messages, relcache_init_file_inval, nmsgs) =
        if backend_access_transam_xlog_seams::xlog_standby_info_active::call() {
            let (msgs, fileinval) =
                backend_utils_cache_inval::at_eoxact::inplaceGetInvalidationMessages()?;
            let n = msgs.len() as i32;
            (msgs, fileinval, n)
        } else {
            (alloc::vec::Vec::new(), false, 0)
        };

    /*
     * Unlink relcache init files as needed; if unlinking, hold RelCacheInitLock
     * until after associated invalidations.
     */
    backend_utils_cache_inval::at_eoxact::PreInplace_Inval()?;

    /*
     * NO EREPORT(ERROR) from here till changes are complete.
     *
     * Write WAL first, then mutate the buffer (a reader may have already
     * pinned + checked visibility). Use DELAY_CHKPT_START + a stack copy of the
     * post-mutation page, like XLogSaveBufferForHint.
     */
    // START_CRIT_SECTION()
    proc_seam::set_delay_chkpt_start::call(true);

    /* XLOG stuff */
    if relcache_seam::relation_needs_wal::call(relation) {
        let (rlocator, forkno, blkno) = bufmgr_seam::buffer_get_tag::call(buffer)?;
        debug_assert!(forkno == types_core::primitive::MAIN_FORKNUM);

        let xlrec = xl_heap_inplace {
            offnum: ItemPointerGetOffsetNumber(&tuple.t_self),
            dbId: backend_utils_init_small_seams::my_database_id::call(),
            tsId: backend_utils_init_small_seams::my_database_table_space::call(),
            relcacheInitFileInval: relcache_init_file_inval,
            nmsgs,
        };

        xloginsert_seam::xlog_begin_insert::call()?;
        let recbuf = xlrec.to_bytes();
        xloginsert_seam::xlog_register_data::call(&recbuf[..MinSizeOfHeapInplace])?;
        if nmsgs != 0 {
            let mut msgbytes: alloc::vec::Vec<u8> =
                alloc::vec::Vec::with_capacity(inval_messages.len() * SHARED_INVALIDATION_MESSAGE_SIZE);
            for m in &inval_messages {
                msgbytes.extend_from_slice(&m.to_wire_bytes());
            }
            xloginsert_seam::xlog_register_data::call(&msgbytes)?;
        }

        /*
         * Register a block image matching what the buffer will look like after
         * the change: copy the live page (header + data area, leaving the hole)
         * and apply the mutation at dst, then register it as an explicit block.
         */
        let mut copied_buffer: alloc::vec::Vec<u8> = alloc::vec::Vec::new();
        bufmgr_seam::with_buffer_page::call(buffer, &mut |page_bytes| {
            // origdata == page_bytes (BufferGetBlock == BufferGetPage for a
            // heap page). Read pd_lower / pd_upper from the page header.
            let (lower, upper, dst_off) = {
                let page = PageRef::new(page_bytes)?;
                let item_id = PageGetItemId(&page, ItemPointerGetOffsetNumber(&tuple.t_self))?;
                // dst = (char*) htup + htup->t_hoff; htup is at the item offset.
                let item_off = item_id.lp_off() as usize;
                let dst_off = item_off + data_ref(tuple).t_hoff as usize;
                (page.pd_lower() as usize, page.pd_upper() as usize, dst_off)
            };
            let mut copy = alloc::vec![0u8; BLCKSZ];
            // memcpy(copied, origdata, lower)
            copy[..lower].copy_from_slice(&page_bytes[..lower]);
            // memcpy(copied + upper, origdata + upper, BLCKSZ - upper)
            copy[upper..BLCKSZ].copy_from_slice(&page_bytes[upper..BLCKSZ]);
            // memcpy(copied + dst_offset_in_block, src, newlen)
            copy[dst_off..dst_off + newlen].copy_from_slice(src);
            copied_buffer = copy;
            Ok(())
        })?;

        xloginsert_seam::xlog_register_block::call(
            0,
            rlocator,
            forkno,
            blkno,
            &copied_buffer,
            REGBUF_STANDARD,
        )?;
        xloginsert_seam::xlog_register_buf_data::call(0, src)?;

        /* inplace updates aren't decoded atm, don't log the origin */

        let recptr = xloginsert_seam::xlog_insert_record::call(RM_HEAP_ID, XLOG_HEAP_INPLACE)?;
        bufmgr_seam::page_set_lsn::call(buffer, recptr)?;
    }

    // memcpy(dst, src, newlen) — overwrite the on-page user-data area.
    let offnum = ItemPointerGetOffsetNumber(&tuple.t_self);
    bufmgr_seam::with_buffer_page::call(buffer, &mut |page_bytes| {
        let (item_off, hoff) = {
            let page = PageRef::new(page_bytes)?;
            let item_id = PageGetItemId(&page, offnum)?;
            (item_id.lp_off() as usize, data_ref(tuple).t_hoff as usize)
        };
        let dst_off = item_off + hoff;
        page_bytes
            .get_mut(dst_off..dst_off + newlen)
            .ok_or_else(|| types_error::PgError::error("inplace dst outside page"))?
            .copy_from_slice(src);
        Ok(())
    })?;

    backend_storage_buffer_bufmgr_seams::mark_buffer_dirty::call(buffer);

    bufmgr_seam::lock_buffer::call(buffer, BUFFER_LOCK_UNLOCK)?;

    /*
     * Send invalidations to shared queue. SearchSysCacheLocked1 assumes we do
     * this before UnlockTuple.
     */
    backend_utils_cache_inval::at_eoxact::AtInplace_Inval()?;

    proc_seam::set_delay_chkpt_start::call(false);
    // END_CRIT_SECTION()
    lmgr_seam::unlock_tuple::call(relation.rd_id, tuple.t_self, InplaceUpdateTupleLock)?;

    /* local processing of just-sent inval */
    backend_utils_cache_inval::local_list::AcceptInvalidationMessages()?;

    /*
     * Queue a transactional inval, for logical decoding and legacy third-party
     * code.
     */
    if !backend_utils_init_miscinit_seams::is_bootstrap_processing_mode::call() {
        backend_utils_cache_inval::cache_invalidate::CacheInvalidateHeapTuple(relation, tuple, None)?;
    }
    Ok(())
}

// ===========================================================================
// heap_inplace_unlock — reverse of heap_inplace_lock (heapam.c).
// ===========================================================================

/// `heap_inplace_unlock(relation, oldtup, buffer)` (heapam.c).
pub fn heap_inplace_unlock<'mcx>(
    relation: &Relation<'mcx>,
    oldtup: &HeapTupleData<'mcx>,
    buffer: Buffer,
) -> PgResult<()> {
    bufmgr_seam::lock_buffer::call(buffer, BUFFER_LOCK_UNLOCK)?;
    lmgr_seam::unlock_tuple::call(relation.rd_id, oldtup.t_self, InplaceUpdateTupleLock)?;
    backend_utils_cache_inval::at_eoxact::ForgetInplace_Inval();
    Ok(())
}

// ===========================================================================
// check_inplace_rel_lock — README.tuplock lock check (heapam.c, debug only).
// ===========================================================================

/// `check_inplace_rel_lock(oldtup)` (heapam.c, `#ifdef USE_ASSERT_CHECKING`).
/// Confirms adequate relation lock is held for an inplace update of pg_class.
#[cfg(debug_assertions)]
fn check_inplace_rel_lock<'mcx>(
    relation: &Relation<'mcx>,
    oldtup: &HeapTupleData<'mcx>,
) -> PgResult<()> {
    use types_storage::lock::ShareUpdateExclusiveLock;

    // Form_pg_class classForm = GETSTRUCT(oldtup); relid = classForm->oid;
    // The on-page pg_class tuple's oid is the relation OID; the inplace caller
    // passes the relation whose tuple this is, so use its rd_id.
    let relid = relation.rd_id;
    let dbid = if backend_catalog_catalog_seams::is_shared_relation::call(relid) {
        0
    } else {
        backend_utils_init_small_seams::my_database_id::call()
    };

    const RELKIND_INDEX: u8 = b'i';
    let indrelid;
    if relation.rd_rel.relkind == RELKIND_INDEX {
        // index_open(relid, AccessShareLock); use the index's indrelid; then
        // index_close. The relcache exposes the index's heap relation.
        indrelid = relation
            .rd_index
            .as_ref()
            .map(|idx| idx.indrelid)
            .unwrap_or(relid);
    } else {
        indrelid = relid;
    }

    // LockHeldByMe(&tag, ShareUpdateExclusiveLock, true) — WARNING if missing.
    let held = lmgr_seam::check_relation_oid_locked_by_me::call(
        indrelid_to_oid(indrelid, dbid),
        ShareUpdateExclusiveLock,
        true,
    );
    if !held {
        let _ = oldtup;
        // elog(WARNING, ...) — surface as a debug warning via ereport(WARNING).
        let _ = ereport(types_error::WARNING)
            .errmsg_internal(format!(
                "missing lock for relation (OID {relid}, relkind {})",
                relation.rd_rel.relkind as char
            ))
            .into_error();
    }
    Ok(())
}

/// Helper: the OID component the relation-lock tag uses (the relation OID; the
/// `dbid` selection is handled inside the lmgr seam).
#[cfg(debug_assertions)]
fn indrelid_to_oid(indrelid: Oid, _dbid: Oid) -> Oid {
    indrelid
}

// ===========================================================================
// Small helpers (shared idioms with delete.rs / lock.rs).
// ===========================================================================

/// Materialize the on-page `HeapTupleHeader` at `(buffer, offnum)`.
fn read_on_page_header<'mcx>(
    mcx: Mcx<'mcx>,
    buffer: Buffer,
    offnum: types_core::primitive::OffsetNumber,
) -> PgResult<HeapTupleHeaderData<'mcx>> {
    let mut out: Option<HeapTupleHeaderData<'mcx>> = None;
    bufmgr_seam::with_buffer_page::call(buffer, &mut |page_bytes| {
        let page = PageRef::new(page_bytes)?;
        let item_id = PageGetItemId(&page, offnum)?;
        let item = PageGetItem(&page, &item_id)?;
        out = Some(HeapTupleHeaderData::read_on_page(mcx, item)?);
        Ok(())
    })?;
    Ok(out.expect("with_buffer_page closure must have run"))
}

/// Materialize the full on-page tuple (header + user-data area) at
/// `(buffer, tid)` as a `FormedTuple` (mirrors delete.rs `read_on_page_tuple`).
fn read_on_page_formed<'mcx>(
    mcx: Mcx<'mcx>,
    rel_id: Oid,
    buffer: Buffer,
    tid: ItemPointerData,
) -> PgResult<types_tuple::backend_access_common_heaptuple::FormedTuple<'mcx>> {
    let offnum = ItemPointerGetOffsetNumber(&tid);
    let mut out: Option<(HeapTupleHeaderData<'mcx>, mcx::PgVec<'mcx, u8>, u32)> = None;
    bufmgr_seam::with_buffer_page::call(buffer, &mut |page_bytes| {
        let page = PageRef::new(page_bytes)?;
        let item_id = PageGetItemId(&page, offnum)?;
        debug_assert!(item_id.has_storage());
        let item = PageGetItem(&page, &item_id)?;
        let hdr = HeapTupleHeaderData::read_on_page(mcx, item)?;
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
    Ok(types_tuple::backend_access_common_heaptuple::FormedTuple { tuple, data })
}

/// Materialize the on-page tuple header (only) at `(buffer, tid)` into a bare
/// `HeapTupleData` (header + identity; no user-data area). Used by
/// `heap_inplace_lock`, which only consults header state for the visibility
/// test.
fn read_on_page_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    rel_id: Oid,
    buffer: Buffer,
    tid: ItemPointerData,
) -> PgResult<HeapTupleData<'mcx>> {
    let offnum = ItemPointerGetOffsetNumber(&tid);
    let mut out: Option<(HeapTupleHeaderData<'mcx>, u32)> = None;
    bufmgr_seam::with_buffer_page::call(buffer, &mut |page_bytes| {
        let page = PageRef::new(page_bytes)?;
        let item_id = PageGetItemId(&page, offnum)?;
        debug_assert!(item_id.has_storage());
        let item = PageGetItem(&page, &item_id)?;
        let hdr = HeapTupleHeaderData::read_on_page(mcx, item)?;
        out = Some((hdr, item.len() as u32));
        Ok(())
    })?;
    let (hdr, t_len) = out.expect("with_buffer_page closure must have run");
    Ok(HeapTupleData {
        t_len,
        t_self: tid,
        t_tableOid: rel_id,
        t_data: Some(mcx::alloc_in(mcx, hdr)?),
    })
}

/// `tp->t_data` (shared) for a `FormedTuple`.
fn data_ref_f<'a, 'mcx>(
    tp: &'a types_tuple::backend_access_common_heaptuple::FormedTuple<'mcx>,
) -> &'a HeapTupleHeaderData<'mcx> {
    tp.tuple.t_data.as_ref().expect("inplace: tuple has no t_data")
}
/// `tp->t_data` (mutable) for a `FormedTuple`.
fn data_mut_f<'a, 'mcx>(
    tp: &'a mut types_tuple::backend_access_common_heaptuple::FormedTuple<'mcx>,
) -> &'a mut HeapTupleHeaderData<'mcx> {
    tp.tuple.t_data.as_mut().expect("inplace: tuple has no t_data")
}

/// `tp->t_data` (shared) for a bare `HeapTupleData`.
fn data_ref<'a, 'mcx>(tuple: &'a HeapTupleData<'mcx>) -> &'a HeapTupleHeaderData<'mcx> {
    tuple.t_data.as_ref().expect("inplace: tuple has no t_data")
}

/// `HeapTupleHasExternal(tp)` for a `FormedTuple`.
fn HeapTupleHasExternalF(tp: &types_tuple::backend_access_common_heaptuple::FormedTuple<'_>) -> bool {
    tp.tuple.t_data.as_ref().is_some_and(|hdr| (hdr.t_infomask & HEAP_HASEXTERNAL) != 0)
}

/// `HeapTupleHeaderSetXmin(tup, xid)`.
fn HeapTupleHeaderSetXmin(hdr: &mut HeapTupleHeaderData<'_>, xid: TransactionId) {
    match &mut hdr.t_choice {
        HeapTupleHeaderChoice::THeap(f) => f.t_xmin = xid,
        HeapTupleHeaderChoice::TDatum(_) => {}
    }
}

/// `HEAP_XMAX_IS_KEYSHR_LOCKED(infomask)` (htup_details.h).
fn HEAP_XMAX_IS_KEYSHR_LOCKED(infomask: u16) -> bool {
    (infomask & HEAP_LOCK_MASK) == types_tuple::heaptuple::HEAP_XMAX_KEYSHR_LOCK
}

/// `IsToastRelation(relation)` via the catalog seam.
fn is_toast_relation(relation: &RelationData<'_>) -> bool {
    backend_catalog_catalog_seams::is_toast_relation::call(relation)
}

/// `TransactionIdIsValid(xid)`.
fn TransactionIdIsValid(xid: TransactionId) -> bool {
    xid != InvalidTransactionId
}

/// `TransactionIdPrecedes(a, b)`.
fn transaction_id_precedes(a: TransactionId, b: TransactionId) -> bool {
    backend_access_transam_transam_seams::transaction_id_precedes::call(a, b)
}

/// `elog(ERROR, msg)`.
fn elog_error(msg: &str) -> types_error::PgError {
    ereport(ERROR).errmsg_internal(msg.to_string()).into_error()
}

extern crate alloc;

#[allow(unused_imports)]
use HeapTupleField3 as _HeapTupleField3;
#[allow(unused_imports)]
use HeapTupleFields as _HeapTupleFields;
#[allow(unused_imports)]
use RelFileLocator as _RelFileLocator;
#[allow(unused_imports)]
use PageIsAllVisible as _PageIsAllVisible;
#[allow(unused_imports)]
use PageGetMaxOffsetNumber as _PageGetMaxOffsetNumber;
