//! F2 — heap tuple INSERT (`access/heap/heapam.c`):
//! `heap_insert` / `heap_prepare_insert` / `simple_heap_insert` plus the
//! `heap_multi_insert_pages` page-count helper, with the `XLOG_HEAP_INSERT`
//! WAL emit.
//!
//! The repo carries a heap tuple as the [`FormedTuple`] value (the owned
//! header [`HeapTupleData`] in `tuple` plus the user-data area in `data`) — the
//! faithful analog of C's contiguous `HeapTuple`.  `heap_prepare_insert` stamps
//! the header fields in place (mutating the caller's `FormedTuple`) and returns
//! the data we actually store: the original tuple, or a toasted copy.
//!
//! Page access crosses the `Buffer`-id-through-seams boundary (the
//! freespace.c / visibilitymap precedent): `RelationGetBufferForTuple` /
//! `RelationPutHeapTuple` (hio.c) own the page placement; the buffer-manager
//! and bufpage predicates/mutators are reached through the
//! `bufmgr-seams` / `vacuumlazy-seams` slots; the WAL record body is built and
//! inserted via `xloginsert-seams`.
//!
//! `heap_multi_insert` is ported here over the owned-`FormedTuple` batch model:
//! C takes `TupleTableSlot **slots` and fetches each slot's heap tuple via
//! `ExecFetchSlotHeapTuple`, but the repo carries the heap tuples as owned
//! [`FormedTuple`] values, so the batch crosses by value (the slot fetch is the
//! caller's job) and the stamped heaptuples are returned in input order — the
//! page-at-a-time fill, one `XLOG_HEAP2_MULTI_INSERT` per page, and the t_self
//! writeback all preserved. Its pure page-count helper
//! [`heap_multi_insert_pages`] is ported and tested here.

use mcx::Mcx;
use types_core::primitive::{BlockNumber, OffsetNumber, Size, TransactionId};
use types_core::xact::CommandId;
use types_error::{PgResult, ERRCODE_INVALID_TRANSACTION_STATE, ERROR};
use backend_utils_error::ereport;
use types_rel::{Relation, RelationData};
use types_storage::{Buffer, InvalidBuffer};
use types_tuple::backend_access_common_heaptuple::FormedTuple;
use types_tuple::heaptuple::{
    HeapTupleField3, HeapTupleHeaderChoice, HeapTupleHeaderData, HEAP2_XACT_MASK, HEAP_COMBOCID,
    HEAP_HASEXTERNAL, HEAP_XACT_MASK, HEAP_XMAX_INVALID, HEAP_XMIN_FROZEN,
};

use backend_access_common_heaptuple::heap_tuple_to_disk_image;
use backend_access_heap_hio::{RelationGetBufferForTuple, RelationPutHeapTuple};
use backend_access_heap_heaptoast::heap_toast_insert_or_update;

use backend_access_heap_vacuumlazy_seams as page_seam;
use backend_access_transam_xact_seams as xact_seam;
use backend_access_transam_xloginsert_seams as xloginsert_seam;
use backend_catalog_catalog_seams as catalog_seam;
use backend_storage_buffer_bufmgr_seams as bufmgr_seam;
use backend_storage_lmgr_predicate_seams as predicate_seam;
use backend_utils_activity_pgstat_seams as pgstat_seam;
use backend_utils_cache_relcache_seams as relcache_seam;

use types_storage::bufpage::SizeofHeapTupleHeader;
use types_wal::wal::XLOG_INCLUDE_ORIGIN;
use types_wal::xloginsert::{REGBUF_KEEP_DATA, REGBUF_STANDARD, REGBUF_WILL_INIT};

use backend_rmgrdesc_next::heapdesc::{
    XLOG_HEAP2_MULTI_INSERT, XLOG_HEAP_INIT_PAGE, XLOG_HEAP_INSERT,
};
use types_wal::wal::{RM_HEAP2_ID, RM_HEAP_ID};
use types_xlog_records::heapam_xlog::{
    xl_heap_header, xl_heap_insert, xl_heap_multi_insert, xl_multi_insert_tuple, SizeOfHeapHeader,
    SizeOfHeapInsert, SizeOfHeapMultiInsert, XLH_INSERT_ALL_FROZEN_SET,
    XLH_INSERT_ALL_VISIBLE_CLEARED, XLH_INSERT_CONTAINS_NEW_TUPLE, XLH_INSERT_IS_SPECULATIVE,
    XLH_INSERT_LAST_IN_MULTI, XLH_INSERT_ON_TOAST_RELATION,
};

// ---------------------------------------------------------------------------
// heapam-local vocabulary (heapam.h / htup_details.h / hio.h constants).
// ---------------------------------------------------------------------------

/// `HEAP_INSERT_SKIP_FSM` (access/heapam.h).
pub const HEAP_INSERT_SKIP_FSM: i32 = 0x0002;
/// `HEAP_INSERT_FROZEN` (access/heapam.h).
pub const HEAP_INSERT_FROZEN: i32 = 0x0004;
/// `HEAP_INSERT_SPECULATIVE` (access/heapam.h).
pub const HEAP_INSERT_SPECULATIVE: i32 = 0x0008;
/// `HEAP_INSERT_NO_LOGICAL` (access/heapam.h).
pub const HEAP_INSERT_NO_LOGICAL: i32 = 0x0010;

/// `HEAP_DEFAULT_FILLFACTOR` (utils/rel.h).
pub const HEAP_DEFAULT_FILLFACTOR: i32 = 100;

/// `RELKIND_RELATION` / `RELKIND_MATVIEW` (catalog/pg_class.h).
const RELKIND_RELATION: u8 = b'r';
const RELKIND_MATVIEW: u8 = b'm';

/// `VISIBILITYMAP_VALID_BITS` (access/visibilitymapdefs.h) == ALL_VISIBLE |
/// ALL_FROZEN.
const VISIBILITYMAP_VALID_BITS: u8 = 0x03;

/// `VISIBILITYMAP_ALL_VISIBLE` (access/visibilitymapdefs.h).
const VISIBILITYMAP_ALL_VISIBLE: u8 = 0x01;
/// `VISIBILITYMAP_ALL_FROZEN` (access/visibilitymapdefs.h).
const VISIBILITYMAP_ALL_FROZEN: u8 = 0x02;

/// `FirstOffsetNumber` (storage/off.h).
const FirstOffsetNumber: OffsetNumber = types_tuple::heaptuple::FIRST_OFFSET_NUMBER;

// ===========================================================================
// heap_prepare_insert — stamp the header + toast (heapam.c, static).
// ===========================================================================

/// `heap_prepare_insert(relation, tup, xid, cid, options)` (heapam.c).
///
/// Sets the tuple header fields in place on the caller's `tup` and toasts the
/// tuple if necessary. Returns `None` if the original tuple is to be stored
/// (the caller uses its own `tup`), or `Some(heaptup)` if a toasted copy must
/// be stored instead. The C return of "the original `tup`" is modeled as
/// `None` so the caller never aliases the original behind a second owner.
fn heap_prepare_insert<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &Relation<'_>,
    tup: &mut FormedTuple<'mcx>,
    xid: TransactionId,
    cid: CommandId,
    options: i32,
) -> PgResult<Option<FormedTuple<'mcx>>> {
    /*
     * To allow parallel inserts, we need to ensure that they are safe to be
     * performed in workers. We have the infrastructure to allow parallel
     * inserts in general except for the cases where inserts generate a new
     * CommandId (eg. inserts into a table having a foreign key column).
     */
    if backend_access_transam_parallel_seams::is_parallel_worker::call() {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_TRANSACTION_STATE)
            .errmsg("cannot insert tuples in a parallel worker")
            .into_error());
    }

    {
        let hdr = tup
            .tuple
            .t_data
            .as_mut()
            .expect("heap_prepare_insert: tuple has no t_data header");

        hdr.t_infomask &= !HEAP_XACT_MASK;
        hdr.t_infomask2 &= !HEAP2_XACT_MASK;
        hdr.t_infomask |= HEAP_XMAX_INVALID;
        HeapTupleHeaderSetXmin(hdr, xid);
        if (options & HEAP_INSERT_FROZEN) != 0 {
            HeapTupleHeaderSetXminFrozen(hdr);
        }

        HeapTupleHeaderSetCmin(hdr, cid);
        HeapTupleHeaderSetXmax(hdr, 0); /* for cleanliness */
    }
    tup.tuple.t_tableOid = relation.rd_id;

    /*
     * If the new tuple is too big for storage or contains already toasted
     * out-of-line attributes from some other relation, invoke the toaster.
     */
    let relkind = relation.rd_rel.relkind;
    if relkind != RELKIND_RELATION && relkind != RELKIND_MATVIEW {
        /* toast table entries should never be recursively toasted */
        debug_assert!(!HeapTupleHasExternal(tup));
        Ok(None)
    } else if HeapTupleHasExternal(tup)
        || tup.tuple.t_len as usize
            > backend_access_heap_heaptoast::TOAST_TUPLE_THRESHOLD
    {
        Ok(heap_toast_insert_or_update(mcx, relation, tup, None, options)?)
    } else {
        Ok(None)
    }
}

// ===========================================================================
// heap_insert — insert one tuple into a heap (heapam.c).
// ===========================================================================

/// `heap_insert(relation, tup, cid, options, bistate)` (heapam.c).
///
/// The new tuple is stamped with the current transaction ID and the specified
/// command ID. On return the header fields of `tup` are updated to match the
/// stored tuple — in particular `tup.tuple.t_self` receives the TID where the
/// tuple was stored. (Toasting of fields within the tuple is NOT reflected back
/// into `tup`, matching C.)
pub fn heap_insert<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &Relation<'_>,
    tup: &mut FormedTuple<'mcx>,
    cid: CommandId,
    options: i32,
    mut bistate: Option<&mut crate::BulkInsertState>,
) -> PgResult<()> {
    let xid = xact_seam::get_current_transaction_id::call()?;
    let mut vmbuffer: Buffer = InvalidBuffer;
    let mut vmbuffer_other: Buffer = InvalidBuffer;
    let mut all_visible_cleared = false;

    /* Cheap, simplistic check that the tuple matches the rel's rowtype. */
    debug_assert!(
        {
            let hdr = tup.tuple.t_data.as_ref().expect("heap_insert: no header");
            (types_tuple::heaptuple::HeapTupleHeaderGetNatts(hdr) as i32)
                <= RelationGetNumberOfAttributes(relation)
        }
    );

    /*
     * Fill in tuple header fields and toast the tuple if necessary.
     *
     * Note: below this point, `heaptup` is the data we actually intend to store
     * into the relation; `tup` is the caller's original untoasted data. When
     * `heap_prepare_insert` returns `None`, `heaptup` is the caller's `tup`.
     */
    let mut toasted = heap_prepare_insert(mcx, relation, tup, xid, cid, options)?;

    // `RelationGetBufferForTuple` / the WAL emit operate on whichever tuple we
    // intend to store (`heaptup`): the toasted copy when present, else `tup`.
    let store_toasted = toasted.is_some();
    let heaptup: &mut FormedTuple<'mcx> = match toasted.as_mut() {
        Some(t) => t,
        None => tup,
    };

    /*
     * Find buffer to insert this tuple into.  If the page is all visible, this
     * will also pin the requisite visibility map page.
     */
    let buffer = RelationGetBufferForTuple(
        relation,
        heaptup.tuple.t_len as Size,
        InvalidBuffer,
        options,
        bistate.as_deref_mut(),
        &mut vmbuffer,
        &mut vmbuffer_other,
        0,
    )?;

    /*
     * We're about to do the actual insert -- but check for conflict first, to
     * avoid possibly having to roll back work we've just done.  For a heap
     * insert, we only need to check for table-level SSI locks (no buffer).
     */
    predicate_seam::check_for_serializable_conflict_in::call(relation.rd_id)?;

    /* NO EREPORT(ERROR) from here till changes are logged */
    // START_CRIT_SECTION() — the crit-section bookkeeping lives behind the
    // buffer/WAL substrate; the panic-on-error contract is mirrored by the
    // seam signatures (a seam erroring here is a PANIC-class bug).

    RelationPutHeapTuple(
        relation,
        buffer,
        &mut heaptup.tuple,
        (options & HEAP_INSERT_SPECULATIVE) != 0,
    )?;

    if page_seam::page_is_all_visible::call(buffer)? {
        all_visible_cleared = true;
        page_seam::page_clear_all_visible::call(buffer)?;
        visibilitymap_clear(
            relation,
            heaptup.tuple.t_self.ip_blkid.block_number(),
            vmbuffer,
            VISIBILITYMAP_VALID_BITS,
        )?;
    }

    /*
     * XXX Should we set PageSetPrunable on this page ? (See heap_insert() in C.)
     */

    page_seam::mark_buffer_dirty::call(buffer)?;

    /* XLOG stuff */
    if relcache_seam::relation_needs_wal::call(relation) {
        let mut info: u8 = XLOG_HEAP_INSERT;
        let mut bufflags: u8 = 0;

        /*
         * If this is a catalog, we need to transmit combo CIDs to properly
         * decode, so log that as well.
         */
        if relation_is_accessible_in_logical_decoding(relation) {
            crate::log_heap_new_cid(relation, &heaptup.tuple)?;
        }

        /*
         * If this is the single and first tuple on page, we can reinit the
         * page instead of restoring the whole thing.  Set flag, and hide
         * buffer references from XLogInsert.
         */
        if heaptup.tuple.t_self.ip_posid == FirstOffsetNumber
            && page_seam::page_get_max_offset_number::call(buffer)? == FirstOffsetNumber
        {
            info |= XLOG_HEAP_INIT_PAGE;
            bufflags |= REGBUF_WILL_INIT;
        }

        let mut flags: u8 = 0;
        if all_visible_cleared {
            flags |= XLH_INSERT_ALL_VISIBLE_CLEARED;
        }
        if (options & HEAP_INSERT_SPECULATIVE) != 0 {
            flags |= XLH_INSERT_IS_SPECULATIVE;
        }
        debug_assert_eq!(
            heaptup.tuple.t_self.ip_blkid.block_number(),
            page_seam::buffer_get_block_number::call(buffer)?
        );

        /*
         * For logical decoding, we need the tuple even if we're doing a full
         * page write, so make sure it's included even if we take a full-page
         * image.
         */
        if relation_is_logically_logged(relation) && (options & HEAP_INSERT_NO_LOGICAL) == 0 {
            flags |= XLH_INSERT_CONTAINS_NEW_TUPLE;
            bufflags |= REGBUF_KEEP_DATA;

            if catalog_seam::is_toast_relation::call(relation) {
                flags |= XLH_INSERT_ON_TOAST_RELATION;
            }
        }

        let xlrec = xl_heap_insert {
            offnum: heaptup.tuple.t_self.ip_posid,
            flags,
        };

        let xlhdr = xl_heap_header {
            t_infomask2: heaptup.tuple.t_data.as_ref().unwrap().t_infomask2,
            t_infomask: heaptup.tuple.t_data.as_ref().unwrap().t_infomask,
            t_hoff: heaptup.tuple.t_data.as_ref().unwrap().t_hoff,
        };

        // The contiguous on-disk tuple image; C registers
        // `(char *) t_data + SizeofHeapTupleHeader .. t_len`.
        let img = heap_tuple_to_disk_image(mcx, heaptup)?;

        xloginsert_seam::xlog_begin_insert::call()?;
        let recbuf = xlrec.to_bytes();
        xloginsert_seam::xlog_register_data::call(&recbuf[..SizeOfHeapInsert])?;

        /*
         * note we mark xlhdr as belonging to buffer; if XLogInsert decides to
         * write the whole page to the xlog, we don't need to store
         * xl_heap_header in the xlog.
         */
        xloginsert_seam::xlog_register_buffer::call(0, buffer, REGBUF_STANDARD | bufflags)?;
        let hdrbuf = xlhdr.to_bytes();
        xloginsert_seam::xlog_register_buf_data::call(0, &hdrbuf[..SizeOfHeapHeader])?;
        /* PG73FORMAT: write bitmap [+ padding] [+ oid] + data */
        xloginsert_seam::xlog_register_buf_data::call(0, &img[SizeofHeapTupleHeader..])?;

        /* filtering by origin on a row level is much more efficient */
        xloginsert_seam::xlog_set_record_flags::call(XLOG_INCLUDE_ORIGIN);

        let recptr = xloginsert_seam::xlog_insert_record::call(RM_HEAP_ID, info)?;

        bufmgr_seam::page_set_lsn::call(buffer, recptr)?;
    }

    // END_CRIT_SECTION()

    page_seam::unlock_release_buffer::call(buffer)?;
    if vmbuffer != InvalidBuffer {
        page_seam::release_buffer::call(vmbuffer)?;
    }

    /*
     * If tuple is cachable, mark it for invalidation from the caches in case
     * we abort.
     */
    cache_invalidate_heap_tuple(relation, &heaptup.tuple)?;

    /* Note: speculative insertions are counted too, even if aborted later */
    pgstat_seam::pgstat_count_heap_insert::call(
        relation.rd_id,
        relation.rd_rel.relisshared,
        relation.pgstat_enabled,
        1,
    );

    /*
     * If heaptup is a private copy, copy t_self back to the caller's image.
     * (C also `heap_freetuple`s the copy; the owned `FormedTuple` is dropped at
     * scope end.) Capture the stored TID first to end the `heaptup` borrow
     * before mutating `tup`.
     */
    let stored_self = heaptup.tuple.t_self;
    if store_toasted {
        tup.tuple.t_self = stored_self;
    }

    Ok(())
}

// ===========================================================================
// heap_multi_insert_pages — page-count helper (heapam.c, static).
// ===========================================================================

/// `heap_multi_insert_pages(heaptuples, done, ntuples, saveFreeSpace)`
/// (heapam.c) — the number of entire pages inserting the remaining heaptuples
/// requires, used to size the relation extension.
pub fn heap_multi_insert_pages(
    heaptuples: &[FormedTuple<'_>],
    done: usize,
    ntuples: usize,
    save_free_space: Size,
) -> i32 {
    use types_core::primitive::BLCKSZ;
    let page_header = types_storage::bufpage::SizeOfPageHeaderData;
    let item_id = core::mem::size_of::<types_storage::bufpage::ItemIdData>();

    let mut page_avail: usize = BLCKSZ - page_header - save_free_space;
    let mut npages: i32 = 1;

    for tuple in &heaptuples[done..ntuples] {
        let tup_sz = item_id + maxalign(tuple.tuple.t_len as usize);

        if page_avail < tup_sz {
            npages += 1;
            page_avail = BLCKSZ - page_header - save_free_space;
        }
        page_avail -= tup_sz;
    }

    npages
}

// ===========================================================================
// heap_multi_insert — insert multiple tuples into a heap (heapam.c).
// ===========================================================================

/// `heap_multi_insert(relation, slots, ntuples, cid, options, bistate)`
/// (heapam.c) — insert multiple tuples into a heap in one operation, filling a
/// page at a time and emitting one `XLOG_HEAP2_MULTI_INSERT` WAL record per
/// page (with the page content-locked once).
///
/// C takes `TupleTableSlot **slots`, fetches each slot's heap tuple
/// (`ExecFetchSlotHeapTuple`), stamps `slots[i]->tts_tableOid`, then writes the
/// stored TID back into `slots[i]->tts_tid`. The repo carries the heap tuples
/// as owned [`FormedTuple`] values, so the batch is taken by value (`tuples`)
/// and the inserted tuples — the toasted copy where `heap_prepare_insert`
/// produced one, else the caller's tuple, each with its `t_self`/`t_tableOid`
/// stamped — are returned in input order. That is exactly C's `heaptuples[]`
/// array (the tuples it `CacheInvalidateHeapTuple`s and whose `t_self` it copies
/// back), which is the information the caller
/// (`CatalogTuplesMultiInsertWithInfo`) feeds to `CatalogIndexInsert`.
pub fn heap_multi_insert<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &Relation<'_>,
    tuples: mcx::PgVec<'mcx, FormedTuple<'mcx>>,
    cid: CommandId,
    options: i32,
    mut bistate: Option<&mut crate::BulkInsertState>,
) -> PgResult<mcx::PgVec<'mcx, FormedTuple<'mcx>>> {
    let xid = xact_seam::get_current_transaction_id::call()?;
    let ntuples = tuples.len();
    let mut vmbuffer: Buffer = InvalidBuffer;
    // C passes NULL for vmbuffer_other; the port threads an unused local
    // (RelationGetBufferForTuple takes &mut Buffer, not Option).
    let mut vmbuffer_other: Buffer = InvalidBuffer;
    // need_tuple_data = RelationIsLogicallyLogged(relation);
    let need_tuple_data = relation_is_logically_logged(relation);
    // need_cids = RelationIsAccessibleInLogicalDecoding(relation);
    let need_cids = relation_is_accessible_in_logical_decoding(relation);

    /* currently not needed (thus unsupported) for heap_multi_insert() */
    debug_assert_eq!(options & HEAP_INSERT_NO_LOGICAL, 0);

    // AssertHasSnapshotForToast(relation) — debug-only snapshot assertion; no
    // state to mirror in the owned model.

    // needwal = RelationNeedsWAL(relation);
    let needwal = relcache_seam::relation_needs_wal::call(relation);
    // saveFreeSpace = RelationGetTargetPageFreeSpace(relation,
    //                                                HEAP_DEFAULT_FILLFACTOR);
    let save_free_space: Size = backend_access_heap_hio_seams::relation_get_target_page_free_space::call(
        relation.rd_id,
        HEAP_DEFAULT_FILLFACTOR,
    )?;

    /* Toast and set header data in all the slots */
    // heaptuples = palloc(ntuples * sizeof(HeapTuple));
    // for (i = 0; i < ntuples; i++) {
    //     tuple = ExecFetchSlotHeapTuple(slots[i], true, NULL);
    //     slots[i]->tts_tableOid = RelationGetRelid(relation);
    //     tuple->t_tableOid = slots[i]->tts_tableOid;
    //     heaptuples[i] = heap_prepare_insert(relation, tuple, xid, cid, options);
    // }
    //
    // The owned batch already holds each tuple (the slot fetch is the caller's
    // job); `heap_prepare_insert` stamps the header (and `t_tableOid`) in place
    // and, when it toasts, returns a separate copy. `heaptuples[i]` is that copy
    // when present, else the caller's tuple — so we take ownership of each input
    // tuple and store whichever one `heap_prepare_insert` selects.
    let mut heaptuples: Vec<FormedTuple<'mcx>> = Vec::with_capacity(ntuples);
    for mut tuple in tuples.into_iter() {
        // tuple->t_tableOid = RelationGetRelid(relation); — heap_prepare_insert
        // sets this too, but C also assigns it explicitly via the slot.
        tuple.tuple.t_tableOid = relation.rd_id;
        let toasted = heap_prepare_insert(mcx, relation, &mut tuple, xid, cid, options)?;
        match toasted {
            Some(t) => heaptuples.push(t),
            None => heaptuples.push(tuple),
        }
    }

    /*
     * We're about to do the actual inserts -- but check for conflict first, to
     * minimize the possibility of having to roll back work we've just done.
     *
     * For heap inserts, we only need to check for table-level SSI locks.
     */
    predicate_seam::check_for_serializable_conflict_in::call(relation.rd_id)?;

    let mut ndone: usize = 0;
    let mut starting_with_empty_page = false;
    let mut npages: i32 = 0;
    let mut npages_used: i32 = 0;
    while ndone < ntuples {
        let mut all_visible_cleared = false;
        let mut all_frozen_set = false;

        // CHECK_FOR_INTERRUPTS();
        page_seam::check_for_interrupts::call()?;

        /*
         * Compute number of pages needed to fit the to-be-inserted tuples in
         * the worst case, to size the relation extension. If we filled a prior
         * page from scratch we can update the last computation; if we started
         * with a partially filled page, recompute from scratch.
         */
        if ndone == 0 || !starting_with_empty_page {
            npages = heap_multi_insert_pages(&heaptuples, ndone, ntuples, save_free_space);
            npages_used = 0;
        } else {
            npages_used += 1;
        }

        /*
         * Find buffer where at least the next tuple will fit. If the page is
         * all-visible this also pins the requisite visibility map page (and the
         * COPY FREEZE empty-page case).
         */
        let buffer = RelationGetBufferForTuple(
            relation,
            heaptuples[ndone].tuple.t_len as Size,
            InvalidBuffer,
            options,
            bistate.as_deref_mut(),
            &mut vmbuffer,
            &mut vmbuffer_other,
            npages - npages_used,
        )?;

        // starting_with_empty_page = PageGetMaxOffsetNumber(page) == 0;
        starting_with_empty_page =
            page_seam::page_get_max_offset_number::call(buffer)? == 0;

        if starting_with_empty_page && (options & HEAP_INSERT_FROZEN) != 0 {
            all_frozen_set = true;
        }

        /* NO EREPORT(ERROR) from here till changes are logged */
        // START_CRIT_SECTION();

        /*
         * RelationGetBufferForTuple has ensured the first tuple fits. Put that
         * on the page, then as many others as fit.
         */
        RelationPutHeapTuple(relation, buffer, &mut heaptuples[ndone].tuple, false)?;

        /* For logical decoding we need combo CIDs to properly decode the catalog. */
        if needwal && need_cids {
            crate::log_heap_new_cid(relation, &heaptuples[ndone].tuple)?;
        }

        let mut nthispage: usize = 1;
        while ndone + nthispage < ntuples {
            // if (PageGetHeapFreeSpace(page) < MAXALIGN(heaptup->t_len) +
            //     saveFreeSpace) break;
            let need =
                maxalign(heaptuples[ndone + nthispage].tuple.t_len as usize) + save_free_space;
            if page_seam::page_get_heap_free_space::call(buffer)? < need {
                break;
            }

            RelationPutHeapTuple(
                relation,
                buffer,
                &mut heaptuples[ndone + nthispage].tuple,
                false,
            )?;

            if needwal && need_cids {
                crate::log_heap_new_cid(relation, &heaptuples[ndone + nthispage].tuple)?;
            }

            nthispage += 1;
        }

        /*
         * If the page is all visible, clear that unless we're only adding frozen
         * rows. If we're only adding already frozen rows to a previously empty
         * page, mark it all-visible.
         */
        if page_seam::page_is_all_visible::call(buffer)? && (options & HEAP_INSERT_FROZEN) == 0 {
            all_visible_cleared = true;
            page_seam::page_clear_all_visible::call(buffer)?;
            visibilitymap_clear(
                relation,
                page_seam::buffer_get_block_number::call(buffer)?,
                vmbuffer,
                VISIBILITYMAP_VALID_BITS,
            )?;
        } else if all_frozen_set {
            page_seam::page_set_all_visible::call(buffer)?;
        }

        /*
         * XXX Should we set PageSetPrunable on this page ? (See heap_insert().)
         */

        page_seam::mark_buffer_dirty::call(buffer)?;

        /* XLOG stuff */
        if needwal {
            // info = XLOG_HEAP2_MULTI_INSERT;
            let mut info: u8 = XLOG_HEAP2_MULTI_INSERT;
            let mut bufflags: u8 = 0;

            /*
             * If the page was previously empty, we can reinit the page instead
             * of restoring the whole thing.
             */
            let init = starting_with_empty_page;

            /* check that the mutually exclusive flags are not both set */
            debug_assert!(!(all_visible_cleared && all_frozen_set));

            let mut flags: u8 = 0;
            if all_visible_cleared {
                flags = XLH_INSERT_ALL_VISIBLE_CLEARED;
            }
            if all_frozen_set {
                flags = XLH_INSERT_ALL_FROZEN_SET;
            }

            /*
             * Build the scratch area exactly as C lays it out in scratch.data:
             *   [xl_heap_multi_insert header (SizeOfHeapMultiInsert)]
             *   [offsets[nthispage] (2 bytes each), only when !init]
             *   then per tuple, SHORTALIGN'd at the absolute scratch offset:
             *     [xl_multi_insert_tuple (SizeOfMultiInsertTuple)] [tuple data]
             * `tupledata` marks the start of the per-tuple area. C registers
             * `scratch.data .. tupledata` as XLogRegisterData and
             * `tupledata .. scratchptr` as XLogRegisterBufData.
             */
            let mut scratch: Vec<u8> = Vec::new();

            // xlrec = (xl_heap_multi_insert *) scratchptr; scratchptr += Size...
            // (flags/ntuples are patched into the final header below.)
            scratch.extend_from_slice(&[0u8; SizeOfHeapMultiInsert]);

            // if (!init) scratchptr += nthispage * sizeof(OffsetNumber);
            let offsets_off = scratch.len();
            if !init {
                scratch.resize(scratch.len() + nthispage * 2, 0);
            }

            // tupledata = scratchptr;
            let tupledata_off = scratch.len();

            // Write out an xl_multi_insert_tuple and the tuple data for each.
            for i in 0..nthispage {
                let heaptup = &heaptuples[ndone + i];

                // if (!init) xlrec->offsets[i] = ItemPointerGetOffsetNumber(&t_self);
                if !init {
                    let off = heaptup.tuple.t_self.ip_posid;
                    let pos = offsets_off + i * 2;
                    scratch[pos..pos + 2].copy_from_slice(&off.to_ne_bytes());
                }

                // tuphdr = (xl_multi_insert_tuple *) SHORTALIGN(scratchptr);
                let aligned = shortalign(scratch.len());
                if aligned > scratch.len() {
                    scratch.resize(aligned, 0);
                }

                // datalen = heaptup->t_len - SizeofHeapTupleHeader;
                let hdr = heaptup
                    .tuple
                    .t_data
                    .as_ref()
                    .expect("heap_multi_insert: tuple has no t_data header");
                // The contiguous on-disk image; data = img[SizeofHeapTupleHeader..].
                let img = heap_tuple_to_disk_image(mcx, heaptup)?;
                let data = &img[SizeofHeapTupleHeader..];
                let datalen = data.len();
                debug_assert_eq!(
                    datalen,
                    heaptup.tuple.t_len as usize - SizeofHeapTupleHeader
                );

                let tuphdr = xl_multi_insert_tuple {
                    datalen: datalen as u16,
                    t_infomask2: hdr.t_infomask2,
                    t_infomask: hdr.t_infomask,
                    t_hoff: hdr.t_hoff,
                };
                scratch.extend_from_slice(&tuphdr.to_bytes());
                /* write bitmap [+ padding] [+ oid] + data */
                scratch.extend_from_slice(data);
            }
            // totaldatalen = scratchptr - tupledata;
            let totaldatalen = scratch.len() - tupledata_off;
            debug_assert!(scratch.len() < types_core::primitive::BLCKSZ);

            if need_tuple_data {
                flags |= XLH_INSERT_CONTAINS_NEW_TUPLE;
            }

            /*
             * Signal that this is the last xl_heap_multi_insert record emitted
             * by this call (needed for logical decoding cleanup).
             */
            if ndone + nthispage == ntuples {
                flags |= XLH_INSERT_LAST_IN_MULTI;
            }

            if init {
                info |= XLOG_HEAP_INIT_PAGE;
                bufflags |= REGBUF_WILL_INIT;
            }

            /*
             * For logical decoding, include the new tuple data even if we take a
             * full-page image of the page.
             */
            if need_tuple_data {
                bufflags |= REGBUF_KEEP_DATA;
            }

            // Patch the xl_heap_multi_insert header (flags + ntuples) in place.
            let xlrec = xl_heap_multi_insert {
                flags,
                ntuples: nthispage as u16,
            };
            scratch[..SizeOfHeapMultiInsert].copy_from_slice(&xlrec.to_bytes());

            xloginsert_seam::xlog_begin_insert::call()?;
            // XLogRegisterData(xlrec, tupledata - scratch.data);
            xloginsert_seam::xlog_register_data::call(&scratch[..tupledata_off])?;
            xloginsert_seam::xlog_register_buffer::call(0, buffer, REGBUF_STANDARD | bufflags)?;
            // XLogRegisterBufData(0, tupledata, totaldatalen);
            xloginsert_seam::xlog_register_buf_data::call(
                0,
                &scratch[tupledata_off..tupledata_off + totaldatalen],
            )?;

            /* filtering by origin on a row level is much more efficient */
            xloginsert_seam::xlog_set_record_flags::call(XLOG_INCLUDE_ORIGIN);

            let recptr = xloginsert_seam::xlog_insert_record::call(RM_HEAP2_ID, info)?;

            bufmgr_seam::page_set_lsn::call(buffer, recptr)?;
        }

        // END_CRIT_SECTION();

        /*
         * If we've frozen everything on the page, update the visibilitymap.
         * We're already holding pin on the vmbuffer.
         */
        if all_frozen_set {
            // visibilitymap_set(relation, BufferGetBlockNumber(buffer), buffer,
            //                   InvalidXLogRecPtr, vmbuffer, InvalidTransactionId,
            //                   VISIBILITYMAP_ALL_VISIBLE | VISIBILITYMAP_ALL_FROZEN);
            page_seam::visibilitymap_set::call(types_vacuum::vacuumlazy::VmSetArgs {
                rel: relation.rd_id,
                heap_blk: page_seam::buffer_get_block_number::call(buffer)?,
                heap_buf: buffer,
                rec_ptr: types_core::InvalidXLogRecPtr,
                vm_buf: vmbuffer,
                cutoff_xid: 0, /* InvalidTransactionId */
                flags: VISIBILITYMAP_ALL_VISIBLE | VISIBILITYMAP_ALL_FROZEN,
            })?;
        }

        page_seam::unlock_release_buffer::call(buffer)?;
        ndone += nthispage;

        /*
         * NB: Only release vmbuffer after inserting all tuples — it's likely
         * we'll insert into subsequent heap pages using the same vm page.
         */
    }

    /* We're done with inserting all tuples, so release the last vmbuffer. */
    if vmbuffer != InvalidBuffer {
        page_seam::release_buffer::call(vmbuffer)?;
    }

    /*
     * We're done with the actual inserts. Check for conflicts again, to ensure
     * that all rw-conflicts in to these inserts are detected.
     */
    predicate_seam::check_for_serializable_conflict_in::call(relation.rd_id)?;

    /*
     * If tuples are cachable, mark them for invalidation from the caches in case
     * we abort. OK to do after releasing the buffer (the heaptuples data is all
     * in local memory).
     */
    if catalog_seam::is_catalog_relation::call(relation) {
        for tuple in heaptuples.iter() {
            cache_invalidate_heap_tuple(relation, &tuple.tuple)?;
        }
    }

    /* copy t_self fields back to the caller's slots */
    // for (i = 0; i < ntuples; i++) slots[i]->tts_tid = heaptuples[i]->t_self;
    // The owned model returns the stamped heaptuples directly (in input order),
    // which carry the stored t_self — exactly what the caller would read back
    // from the slots' tts_tid.

    pgstat_seam::pgstat_count_heap_insert::call(
        relation.rd_id,
        relation.rd_rel.relisshared,
        relation.pgstat_enabled,
        ntuples as i64,
    );

    // Return the stamped heaptuples (toasted copy or caller's tuple), in order.
    let mut out: mcx::PgVec<'mcx, FormedTuple<'mcx>> = mcx::PgVec::new_in(mcx);
    out.reserve(heaptuples.len());
    for t in heaptuples {
        out.push(t);
    }
    Ok(out)
}

// ===========================================================================
// simple_heap_insert — insert a tuple with a default command id (heapam.c).
// ===========================================================================

/// `simple_heap_insert(relation, tup)` (heapam.c) — insert a tuple, supplying a
/// default command ID and not allowing the speedup options. Used in most places
/// that modify system catalogs.
pub fn simple_heap_insert<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &Relation<'_>,
    tup: &mut FormedTuple<'mcx>,
) -> PgResult<()> {
    let cid = xact_seam::get_current_command_id::call(true)?;
    heap_insert(mcx, relation, tup, cid, 0, None)
}

// ===========================================================================
// Header-field setters (htup_details.h inline functions).
// ===========================================================================

/// `HeapTupleHeaderSetXmin(tup, xid)` — `tup->t_choice.t_heap.t_xmin = xid`.
fn HeapTupleHeaderSetXmin(hdr: &mut HeapTupleHeaderData<'_>, xid: TransactionId) {
    if let HeapTupleHeaderChoice::THeap(f) = &mut hdr.t_choice {
        f.t_xmin = xid;
    }
}

/// `HeapTupleHeaderSetXmax(tup, xid)` — `tup->t_choice.t_heap.t_xmax = xid`.
fn HeapTupleHeaderSetXmax(hdr: &mut HeapTupleHeaderData<'_>, xid: TransactionId) {
    if let HeapTupleHeaderChoice::THeap(f) = &mut hdr.t_choice {
        f.t_xmax = xid;
    }
}

/// `HeapTupleHeaderSetXminFrozen(tup)` — `tup->t_infomask |= HEAP_XMIN_FROZEN`.
fn HeapTupleHeaderSetXminFrozen(hdr: &mut HeapTupleHeaderData<'_>) {
    hdr.t_infomask |= HEAP_XMIN_FROZEN;
}

/// `HeapTupleHeaderSetCmin(tup, cid)` — `tup->t_choice.t_heap.t_field3.t_cid =
/// cid; tup->t_infomask &= ~HEAP_COMBOCID`. (Asserts `!HEAP_MOVED` in C; a
/// freshly-formed insert tuple never has `HEAP_MOVED`.)
fn HeapTupleHeaderSetCmin(hdr: &mut HeapTupleHeaderData<'_>, cid: CommandId) {
    if let HeapTupleHeaderChoice::THeap(f) = &mut hdr.t_choice {
        f.t_field3 = HeapTupleField3::TCid(cid);
    }
    hdr.t_infomask &= !HEAP_COMBOCID;
}

// ===========================================================================
// Small helpers (htup_details.h / utils/rel.h macros).
// ===========================================================================

/// `HeapTupleHasExternal(tuple)` — `(tuple->t_data->t_infomask &
/// HEAP_HASEXTERNAL) != 0`.
fn HeapTupleHasExternal(tup: &FormedTuple<'_>) -> bool {
    tup.tuple
        .t_data
        .as_ref()
        .is_some_and(|hdr| (hdr.t_infomask & HEAP_HASEXTERNAL) != 0)
}

/// `RelationGetNumberOfAttributes(relation)` — `rel->rd_att->natts`.
fn RelationGetNumberOfAttributes(relation: &RelationData<'_>) -> i32 {
    relation.rd_att.natts
}

/// `MAXALIGN(LEN)` — round up to `MAXIMUM_ALIGNOF` (8).
fn maxalign(len: usize) -> usize {
    const MAXIMUM_ALIGNOF: usize = 8;
    (len.wrapping_add(MAXIMUM_ALIGNOF - 1)) & !(MAXIMUM_ALIGNOF - 1)
}

/// `SHORTALIGN(LEN)` — round up to `ALIGNOF_SHORT` (2).
fn shortalign(len: usize) -> usize {
    const ALIGNOF_SHORT: usize = 2;
    (len.wrapping_add(ALIGNOF_SHORT - 1)) & !(ALIGNOF_SHORT - 1)
}

/// `RelationIsAccessibleInLogicalDecoding(relation)` (utils/rel.h), expanded as
/// the C macro: `XLogLogicalInfoActive() && RelationNeedsWAL(relation) &&
/// (IsCatalogRelation(relation) || RelationIsUsedAsCatalogTable(relation))`.
/// (`RelationIsUsedAsCatalogTable` reads `rd_options->user_catalog_table`,
/// resolved by the relcache; here we conservatively rely on the catalog-rel
/// arm plus the user-catalog-table option carried on the relcache copy.)
fn relation_is_accessible_in_logical_decoding(relation: &RelationData<'_>) -> bool {
    let wal = backend_access_transam_xlog_seams::wal_level::call();
    let xlog_logical_info_active = wal >= types_wal::WalLevel::Logical;
    let used_as_catalog_table = relation_is_used_as_catalog_table(relation);
    xlog_logical_info_active
        && relcache_seam::relation_needs_wal::call(relation)
        && (catalog_seam::is_catalog_relation::call(relation) || used_as_catalog_table)
}

/// `RelationIsLogicallyLogged(relation)` (utils/rel.h): `XLogLogicalInfoActive()
/// && RelationNeedsWAL(relation) && !IsCatalogRelation(relation)`.
fn relation_is_logically_logged(relation: &RelationData<'_>) -> bool {
    let wal = backend_access_transam_xlog_seams::wal_level::call();
    let xlog_logical_info_active = wal >= types_wal::WalLevel::Logical;
    xlog_logical_info_active
        && relcache_seam::relation_needs_wal::call(relation)
        && !catalog_seam::is_catalog_relation::call(relation)
}

/// `RelationIsUsedAsCatalogTable(relation)` (utils/rel.h): true for an ordinary
/// table / matview whose reloptions set `user_catalog_table`.
fn relation_is_used_as_catalog_table(relation: &RelationData<'_>) -> bool {
    let relkind = relation.rd_rel.relkind;
    (relkind == RELKIND_RELATION || relkind == RELKIND_MATVIEW)
        && relation
            .rd_options
            .as_ref()
            .is_some_and(|o| o.user_catalog_table)
}

/// `visibilitymap_clear(rel, heap_blk, vmbuf, flags)` via the page seam (W2
/// owner). C ignores the return; we do too.
fn visibilitymap_clear(
    relation: &RelationData<'_>,
    heap_blk: BlockNumber,
    vmbuf: Buffer,
    flags: u8,
) -> PgResult<()> {
    page_seam::visibilitymap_clear::call(relation.rd_id, heap_blk, vmbuf, flags)?;
    Ok(())
}

/// `CacheInvalidateHeapTuple(relation, tuple, NULL)` — the inval crate is
/// directly callable (no dependency cycle).
fn cache_invalidate_heap_tuple(
    relation: &RelationData<'_>,
    tuple: &types_tuple::heaptuple::HeapTupleData<'_>,
) -> PgResult<()> {
    backend_utils_cache_inval::cache_invalidate::CacheInvalidateHeapTuple(relation, tuple, None)
}


#[cfg(test)]
mod tests {
    use super::*;
    use mcx::MemoryContext;
    use types_tuple::heaptuple::{
        BlockIdData, HeapTupleData, HeapTupleField3, HeapTupleFields, HeapTupleHeaderChoice,
        HeapTupleHeaderData, ItemPointerData,
    };
    use types_xlog_records::heapam_xlog::{
        xl_heap_header, xl_heap_insert, SizeOfHeapHeader, SizeOfHeapInsert,
    };

    /// `xl_heap_insert` encodes `offnum`@0 + `flags`@2 and round-trips.
    #[test]
    fn xl_heap_insert_round_trips() {
        let rec = xl_heap_insert { offnum: 7, flags: 0x05 };
        let bytes = rec.to_bytes();
        assert_eq!(bytes.len(), SizeOfHeapInsert);
        assert_eq!(SizeOfHeapInsert, 3);
        let back = xl_heap_insert::from_bytes(&bytes);
        assert_eq!(back.offnum, 7);
        assert_eq!(back.flags, 0x05);
    }

    /// `xl_heap_header` round-trips its 5-byte body.
    #[test]
    fn xl_heap_header_round_trips() {
        let hdr = xl_heap_header {
            t_infomask2: 0x1234,
            t_infomask: 0x0800,
            t_hoff: 24,
        };
        let bytes = hdr.to_bytes();
        assert_eq!(bytes.len(), SizeOfHeapHeader);
        assert_eq!(SizeOfHeapHeader, 5);
        let back = xl_heap_header::from_bytes(&bytes);
        assert_eq!(back.t_infomask2, 0x1234);
        assert_eq!(back.t_infomask, 0x0800);
        assert_eq!(back.t_hoff, 24);
    }

    /// `heap_prepare_insert`'s header stamping helpers mirror the C macros:
    /// `SetXmin` writes t_heap.t_xmin, `SetCmin` writes t_field3 + clears
    /// HEAP_COMBOCID, `SetXminFrozen` sets the frozen bits, `SetXmax(0)` clears.
    #[test]
    fn header_setters_match_c() {
        let ctx = MemoryContext::new("header_setters");
        let mcx = ctx.mcx();
        let mut hdr = HeapTupleHeaderData {
            t_choice: HeapTupleHeaderChoice::THeap(HeapTupleFields {
                t_xmin: 0,
                t_xmax: 999,
                t_field3: HeapTupleField3::TCid(0),
            }),
            t_ctid: ItemPointerData {
                ip_blkid: BlockIdData::new(0),
                ip_posid: 0,
            },
            t_infomask2: 0,
            t_infomask: HEAP_COMBOCID,
            t_hoff: 24,
            t_bits: mcx::PgVec::new_in(mcx),
        };

        HeapTupleHeaderSetXmin(&mut hdr, 42);
        HeapTupleHeaderSetXmax(&mut hdr, 0);
        HeapTupleHeaderSetCmin(&mut hdr, 7);
        match &hdr.t_choice {
            HeapTupleHeaderChoice::THeap(f) => {
                assert_eq!(f.t_xmin, 42);
                assert_eq!(f.t_xmax, 0);
                assert!(matches!(f.t_field3, HeapTupleField3::TCid(7)));
            }
            _ => panic!("expected THeap"),
        }
        // SetCmin clears HEAP_COMBOCID.
        assert_eq!(hdr.t_infomask & HEAP_COMBOCID, 0);

        HeapTupleHeaderSetXminFrozen(&mut hdr);
        assert_eq!(hdr.t_infomask & HEAP_XMIN_FROZEN, HEAP_XMIN_FROZEN);
    }

    /// `heap_multi_insert_pages` mirrors the C page-fill arithmetic: one page
    /// when everything fits, an extra page when the running available space
    /// can't hold the next `sizeof(ItemIdData) + MAXALIGN(t_len)`.
    #[test]
    fn multi_insert_pages_arithmetic() {
        let ctx = MemoryContext::new("multi_insert_pages");
        let mcx = ctx.mcx();

        // Helper: a FormedTuple of a given on-disk length (only t_len matters).
        let mk = |len: u32| -> FormedTuple<'_> {
            let hdr = HeapTupleHeaderData {
                t_choice: HeapTupleHeaderChoice::THeap(HeapTupleFields {
                    t_xmin: 0,
                    t_xmax: 0,
                    t_field3: HeapTupleField3::TCid(0),
                }),
                t_ctid: ItemPointerData {
                    ip_blkid: BlockIdData::new(0),
                    ip_posid: 0,
                },
                t_infomask2: 0,
                t_infomask: 0,
                t_hoff: 24,
                t_bits: mcx::PgVec::new_in(mcx),
            };
            FormedTuple {
                tuple: mcx::alloc_in(mcx,
                    HeapTupleData {
                        t_len: len,
                        t_self: ItemPointerData {
                            ip_blkid: BlockIdData::new(0),
                            ip_posid: 0,
                        },
                        t_tableOid: 0,
                        t_data: Some(mcx::alloc_in(mcx, hdr).unwrap()),
                    },
                )
                .unwrap(),
                data: mcx::PgVec::new_in(mcx),
            }
        };

        // A handful of tiny tuples fit on one page.
        let small: Vec<FormedTuple<'_>> = (0..4).map(|_| mk(40)).collect();
        assert_eq!(heap_multi_insert_pages(&small, 0, small.len(), 0), 1);

        // Big tuples (~half a page each) force multiple pages.
        let big: Vec<FormedTuple<'_>> = (0..5).map(|_| mk(4096)).collect();
        assert!(heap_multi_insert_pages(&big, 0, big.len(), 0) >= 3);

        // `done` skips already-placed tuples.
        assert_eq!(heap_multi_insert_pages(&small, small.len(), small.len(), 0), 1);
    }
}
