//! Port of `src/backend/access/spgist/spgvacuum.c` (PostgreSQL 18.3): VACUUM
//! support for SP-GiST.
//!
//! Scope: `spgbulkdelete` / `spgvacuumcleanup` and the workers they drive —
//! `spgvacuumscan`, `spgvacuumpage`, `vacuumLeafPage`, `vacuumLeafRoot`,
//! `vacuumRedirectAndPlaceholder`, `spgprocesspending`, plus the pending-TID
//! list helpers (`spgAddPendingTID` / `spgClearPendingList`).
//!
//! ## Memory model
//!
//! The C `spgBulkDeleteState` carries an `IndexVacuumInfo *info` pointer plus a
//! `palloc`'d `pendingList` linked list of `spgVacPendingItem`. Here the state
//! is an owned struct: `info` is borrowed (`&IndexVacuumInfo`), the
//! `IndexBulkDeleteResult` stats are owned and threaded by `&mut`, the callback
//! is a borrowed `&mut dyn FnMut`, and the pending list is an owned `Vec` of
//! [`SpgVacPendingItem`] (the append-only + duplicate-filter behavior is mirrored
//! exactly so a concurrent-insert scan can't loop or miss items).
//!
//! The page-byte work mirrors the C `Page` pointer manipulation against the
//! BLCKSZ buffer bytes via the bufmgr `with_buffer_page` mutate seam; the move
//! step swaps line pointers in place exactly as the C does.
//!
//! ## Read stream
//!
//! C's `spgvacuumscan` drives a `read_stream` over the block range
//! `[SPGIST_METAPAGE_BLKNO+1, num_pages)`, re-checking the relation length and
//! resetting the stream after each pass so leaf pages added mid-scan are still
//! visited. The repo has no usable `read_stream` seam yet, so this port keeps
//! the identical visit set + relength-recheck loop using sequential
//! `ReadBufferExtended` reads (the same simplification `ginvacuum.c`'s port
//! adopted); the externally observable behavior — every page in the final
//! relation length, including ones added after the scan started, vacuumed once —
//! is preserved.

use alloc::vec::Vec;

use mcx::Mcx;

use backend_storage_buffer_bufmgr_seams as bufmgr;
use backend_access_transam_xloginsert_seams as xloginsert;
use backend_utils_cache_relcache_seams as relcache;
use backend_utils_init_miscinit_seams as miscinit;
use backend_storage_freespace_seams as freespace;
use backend_storage_lmgr_lmgr_seams as lmgr;
use backend_storage_ipc_procarray_seams as procarray;
use backend_utils_time_snapmgr_seams as snapmgr;
use backend_access_heap_vacuumlazy_seams as vacuumlazy;
use backend_access_heap_hio_seams as hio;

use backend_storage_page::{
    ItemPointerIsValid as item_pointer_is_valid_opt, ItemPointerSetInvalid, PageGetItemId,
    PageGetMaxOffsetNumber, PageIsEmpty, PageIsNew, PageRef,
};

use types_core::primitive::{
    BlockNumber, InvalidBlockNumber, OffsetNumber, Oid, TransactionId,
};
use types_error::PgResult;
use types_rel::Relation;
use types_storage::buf::{Buffer, BUFFER_LOCK_EXCLUSIVE};
use types_tableam::genam::{IndexBulkDeleteResult, IndexVacuumInfo};
use types_tuple::heaptuple::{
    ItemPointerData, FIRST_OFFSET_NUMBER as FirstOffsetNumber,
    INVALID_OFFSET_NUMBER as InvalidOffsetNumber,
};
use types_wal::xloginsert::REGBUF_STANDARD;
use types_xlog_records::spgxlog::{
    spgxlogVacuumLeaf, spgxlogVacuumRedirect, spgxlogVacuumRoot,
};

use types_spgist::{
    SpGistBlockIsRoot, SpGistState, SPGIST_DEAD, SPGIST_LAST_FIXED_BLKNO, SPGIST_LIVE,
    SPGIST_METAPAGE_BLKNO, SPGIST_PLACEHOLDER, SPGIST_REDIRECT,
};

use crate::spgdoinsert::{
    elog_error, lt_get_next_offset, lt_set_next_offset, lt_tupstate, node_offsets, node_t_tid,
    offsets_to_bytes, opaque_n_redirection, page_index_multi_delete, rel_name,
    set_opaque_n_redirection, store_state, spgPageIndexMultiDelete,
};
use crate::{
    initSpGistState, opaque_n_placeholder, set_opaque_n_placeholder, write_item_pointer,
    SpGistPageIsDeleted, SpGistPageIsLeaf, SpGistSetLastUsedPage, SpGistUpdateMetaPage,
};

/// `ItemPointerIsValid(&ip)` — the backend-storage-page helper takes
/// `Option<&ItemPointerData>`; this wrapper adapts the by-value form.
#[inline]
fn ItemPointerIsValid(ip: &ItemPointerData) -> bool {
    item_pointer_is_valid_opt(Some(ip))
}

// XLOG record types for SPGiST vacuum (spgxlog.h).
const XLOG_SPGIST_VACUUM_LEAF: u8 = 0x60;
const XLOG_SPGIST_VACUUM_ROOT: u8 = 0x70;
const XLOG_SPGIST_VACUUM_REDIRECT: u8 = 0x80;
const RM_SPGIST_ID: types_core::RmgrId = 16;

// ===========================================================================
// spgVacPendingItem (spgvacuum.c:33) + spgBulkDeleteState (spgvacuum.c:41)
// ===========================================================================

/// `spgVacPendingItem` (spgvacuum.c:33) — an entry in the pending-list of TIDs
/// we need to revisit.
struct SpgVacPendingItem {
    /// `ItemPointerData tid` — redirection target to visit.
    tid: ItemPointerData,
    /// `bool done` — have we dealt with this?
    done: bool,
}

/// `IndexBulkDeleteCallback` (genam.h) — returns true iff the heap tuple at
/// `itemptr` is being deleted by this VACUUM.
pub type IndexBulkDeleteCallback<'a> = dyn FnMut(&ItemPointerData) -> PgResult<bool> + 'a;

/// `spgBulkDeleteState` (spgvacuum.c:41) — local state for vacuum operations.
///
/// `info`/`stats`/`callback` borrow the bulkdelete parameters; `spgstate`,
/// `pendingList`, `myXmin` and `lastFilledBlock` are the additional working
/// state set up by `spgvacuumscan`.
struct SpgBulkDeleteState<'a, 'mcx> {
    info: &'a IndexVacuumInfo<'mcx>,
    stats: &'a mut IndexBulkDeleteResult,
    callback: &'a mut IndexBulkDeleteCallback<'a>,
    spgstate: SpGistState<'mcx>,
    pending_list: Vec<SpgVacPendingItem>,
    my_xmin: TransactionId,
    last_filled_block: BlockNumber,
}

// ===========================================================================
// Dead-tuple field accessors against on-disk bytes (SpGistDeadTupleData).
//
// Layout: bits(u32 @0; tupstate = low 2 bits), t_info(u16 @4),
// pointer(ItemPointerData @6, 6 bytes), xid(TransactionId @12).
// ===========================================================================

/// `dt->tupstate` (low 2 bits of `bits` @0) — same encoding as a leaf tuple.
#[inline]
fn dt_tupstate(dt: &[u8]) -> u32 {
    lt_tupstate(dt)
}
/// `dt->tupstate = v`.
#[inline]
fn dt_set_tupstate(dt: &mut [u8], v: u32) {
    let w = u32::from_ne_bytes([dt[0], dt[1], dt[2], dt[3]]);
    let w = (w & !0x3) | (v & 0x3);
    dt[0..4].copy_from_slice(&w.to_ne_bytes());
}
/// `dt->pointer` (ItemPointerData @6).
#[inline]
fn dt_pointer(dt: &[u8]) -> ItemPointerData {
    node_t_tid(&dt[6..])
}
/// `ItemPointerSetInvalid(&dt->pointer)` — write the invalid block/offset image.
#[inline]
fn dt_set_pointer_invalid(dt: &mut [u8]) {
    let mut ip = ItemPointerData::default();
    ItemPointerSetInvalid(&mut ip);
    write_item_pointer(&mut dt[6..12], &ip);
}
/// `dt->xid` (TransactionId @12).
#[inline]
fn dt_xid(dt: &[u8]) -> TransactionId {
    u32::from_ne_bytes([dt[12], dt[13], dt[14], dt[15]])
}

/// `lt->heapPtr` (ItemPointerData @6 of a leaf tuple).
#[inline]
fn lt_heap_ptr(lt: &[u8]) -> ItemPointerData {
    node_t_tid(&lt[6..])
}

// ===========================================================================
// spgAddPendingTID (spgvacuum.c:63) / spgClearPendingList (spgvacuum.c:89)
// ===========================================================================

/// `spgAddPendingTID(bds, tid)` (spgvacuum.c:63) — append `tid` to the pending
/// list unless already present. New items always append at the end so a scan of
/// the list never misses items added during the scan.
fn spg_add_pending_tid(bds: &mut SpgBulkDeleteState<'_, '_>, tid: &ItemPointerData) {
    for pitem in &bds.pending_list {
        if item_pointer_equals(tid, &pitem.tid) {
            return; // already in list, do nothing
        }
    }
    bds.pending_list.push(SpgVacPendingItem {
        tid: *tid,
        done: false,
    });
}

/// `spgClearPendingList(bds)` (spgvacuum.c:89) — clear the pending list. All
/// items should have been dealt with.
fn spg_clear_pending_list(bds: &mut SpgBulkDeleteState<'_, '_>) {
    for pitem in &bds.pending_list {
        debug_assert!(pitem.done, "all pending items should have been dealt with");
    }
    bds.pending_list.clear();
}

/// `ItemPointerEquals(a, b)`.
#[inline]
fn item_pointer_equals(a: &ItemPointerData, b: &ItemPointerData) -> bool {
    a.ip_blkid.bi_hi == b.ip_blkid.bi_hi
        && a.ip_blkid.bi_lo == b.ip_blkid.bi_lo
        && a.ip_posid == b.ip_posid
}

/// `ItemPointerGetBlockNumber(ip)`.
#[inline]
fn item_pointer_get_block_number(ip: &ItemPointerData) -> BlockNumber {
    ((ip.ip_blkid.bi_hi as u32) << 16) | ip.ip_blkid.bi_lo as u32
}
/// `ItemPointerGetOffsetNumber(ip)`.
#[inline]
fn item_pointer_get_offset_number(ip: &ItemPointerData) -> OffsetNumber {
    ip.ip_posid
}

// ===========================================================================
// vacuumLeafPage (spgvacuum.c:125)
// ===========================================================================

/// `vacuumLeafPage(bds, index, buffer, forPending)` (spgvacuum.c:125) — vacuum a
/// regular (non-root) leaf page. Deletes tuples targeted for deletion, but never
/// moves tuples referenced by outside links (chain heads). Concurrently-created
/// REDIRECTs add their targets to the pending list.
fn vacuum_leaf_page<'mcx>(
    mcx: Mcx<'mcx>,
    bds: &mut SpgBulkDeleteState<'_, 'mcx>,
    index: &Relation<'mcx>,
    buffer: Buffer,
    for_pending: bool,
) -> PgResult<()> {
    let max = {
        let page = bufmgr::buffer_get_page::call(mcx, buffer)?;
        PageGetMaxOffsetNumber(&PageRef::new(&page)?)
    };

    // predecessor[] and deletable[] are indexed by 1-based offset (index 0
    // unused); the C arrays are MaxIndexTuplesPerPage+1 long. `max` offsets fit
    // in `max + 1` slots (plus index 0).
    let n_slots = max as usize + 2;
    let mut predecessor: Vec<OffsetNumber> = vec![0; n_slots];
    let mut deletable: Vec<bool> = vec![false; n_slots];
    let mut n_deletable: i32 = 0;

    let block_number = bufmgr::buffer_get_block_number::call(buffer);

    // Scan page, identify tuples to delete, accumulate stats. Add concurrent
    // REDIRECT targets to the pending list as we go.
    {
        let page = bufmgr::buffer_get_page::call(mcx, buffer)?;
        for i in FirstOffsetNumber..=max {
            let it_off = {
                let pr = PageRef::new(&page)?;
                let iid = PageGetItemId(&pr, i)?;
                iid.lp_off() as usize
            };
            let lt = &page[it_off..];
            let st = lt_tupstate(lt);
            if st == SPGIST_LIVE {
                debug_assert!(ItemPointerIsValid(&lt_heap_ptr(lt)));

                if (bds.callback)(&lt_heap_ptr(lt))? {
                    bds.stats.tuples_removed += 1.0;
                    deletable[i as usize] = true;
                    n_deletable += 1;
                } else if !for_pending {
                    bds.stats.num_index_tuples += 1.0;
                }

                // Form predecessor map, too.
                let next = lt_get_next_offset(lt);
                if next != InvalidOffsetNumber {
                    // paranoia about corrupted chain links
                    if next < FirstOffsetNumber
                        || next > max
                        || predecessor[next as usize] != InvalidOffsetNumber
                    {
                        return Err(elog_error(alloc::format!(
                            "inconsistent tuple chain links in page {} of index \"{}\"",
                            block_number,
                            rel_name(index)
                        )));
                    }
                    predecessor[next as usize] = i;
                }
            } else if st == SPGIST_REDIRECT {
                let dt = lt; // a dead tuple shares the leaf layout
                debug_assert_eq!(lt_get_next_offset(dt), InvalidOffsetNumber);
                debug_assert!(ItemPointerIsValid(&dt_pointer(dt)));

                // Add target TID to pending list if the redirection could have
                // happened since VACUUM started.
                if transaction_id_follows_or_equals(dt_xid(dt), bds.my_xmin) {
                    let target = dt_pointer(dt);
                    spg_add_pending_tid(bds, &target);
                }
            } else {
                debug_assert_eq!(lt_get_next_offset(lt), InvalidOffsetNumber);
            }
        }
    }

    if n_deletable == 0 {
        return Ok(()); // nothing more to do
    }

    // Figure out exactly what we have to do — six arrays describing four kinds
    // of operations (see the C comment). We iterate over all tuples to find
    // chain heads, then chase each chain making work-item entries.
    let mut to_dead: Vec<OffsetNumber> = Vec::new();
    let mut to_placeholder: Vec<OffsetNumber> = Vec::new();
    let mut move_src: Vec<OffsetNumber> = Vec::new();
    let mut move_dest: Vec<OffsetNumber> = Vec::new();
    let mut chain_src: Vec<OffsetNumber> = Vec::new();
    let mut chain_dest: Vec<OffsetNumber> = Vec::new();

    {
        let page = bufmgr::buffer_get_page::call(mcx, buffer)?;
        for i in FirstOffsetNumber..=max {
            let head_off = {
                let pr = PageRef::new(&page)?;
                PageGetItemId(&pr, i)?.lp_off() as usize
            };
            let head = &page[head_off..];
            if lt_tupstate(head) != SPGIST_LIVE {
                continue; // can't be a chain member
            }
            if predecessor[i as usize] != 0 {
                continue; // not a chain head
            }

            // initialize ...
            let mut intervening_deletable = false;
            let mut prev_live = if deletable[i as usize] {
                InvalidOffsetNumber
            } else {
                i
            };

            // scan down the chain ...
            let mut j = lt_get_next_offset(head);
            while j != InvalidOffsetNumber {
                let lt_off = {
                    let pr = PageRef::new(&page)?;
                    PageGetItemId(&pr, j)?.lp_off() as usize
                };
                let lt = &page[lt_off..];
                if lt_tupstate(lt) != SPGIST_LIVE {
                    // all tuples in chain should be live
                    return Err(elog_error(alloc::format!(
                        "unexpected SPGiST tuple state: {}",
                        lt_tupstate(lt)
                    )));
                }

                if deletable[j as usize] {
                    // This tuple should be replaced by a placeholder.
                    to_placeholder.push(j);
                    // previous live tuple's chain link will need an update
                    intervening_deletable = true;
                } else if prev_live == InvalidOffsetNumber {
                    // This is the first live tuple in the chain. It has to move
                    // to the head position.
                    move_src.push(j);
                    move_dest.push(i);
                    // Chain updates will be applied after the move
                    prev_live = i;
                    intervening_deletable = false;
                } else {
                    // Second or later live tuple. Arrange to re-chain it to the
                    // previous live one, if there was a gap.
                    if intervening_deletable {
                        chain_src.push(prev_live);
                        chain_dest.push(j);
                    }
                    prev_live = j;
                    intervening_deletable = false;
                }

                j = lt_get_next_offset(lt);
            }

            if prev_live == InvalidOffsetNumber {
                // The chain is entirely removable, so we need a DEAD tuple.
                to_dead.push(i);
            } else if intervening_deletable {
                // One or more deletions at end of chain, so close it off.
                chain_src.push(prev_live);
                chain_dest.push(InvalidOffsetNumber);
            }
        }
    }

    let n_dead = to_dead.len();
    let n_placeholder = to_placeholder.len();
    let n_move = move_src.len();
    let n_chain = chain_src.len();

    // sanity check ...
    if n_deletable as usize != n_dead + n_placeholder + n_move {
        return Err(elog_error("inconsistent counts of deletable tuples".into()));
    }

    // Do the updates.
    miscinit::start_crit_section::call();

    bufmgr::with_buffer_page::call(buffer, &mut |page: &mut [u8]| {
        spgPageIndexMultiDelete(
            &mut bds.spgstate,
            page,
            &to_dead,
            SPGIST_DEAD,
            SPGIST_DEAD,
            InvalidBlockNumber,
            InvalidOffsetNumber,
        )?;

        spgPageIndexMultiDelete(
            &mut bds.spgstate,
            page,
            &to_placeholder,
            SPGIST_PLACEHOLDER,
            SPGIST_PLACEHOLDER,
            InvalidBlockNumber,
            InvalidOffsetNumber,
        )?;

        // Implement the move step by swapping the line pointers of the source
        // and target tuples, then replacing the newly-source tuples with
        // placeholders.
        for k in 0..n_move {
            swap_item_ids(page, move_src[k], move_dest[k])?;
        }

        spgPageIndexMultiDelete(
            &mut bds.spgstate,
            page,
            &move_src,
            SPGIST_PLACEHOLDER,
            SPGIST_PLACEHOLDER,
            InvalidBlockNumber,
            InvalidOffsetNumber,
        )?;

        for k in 0..n_chain {
            let lt_off = {
                let pr = PageRef::new(page)?;
                PageGetItemId(&pr, chain_src[k])?.lp_off() as usize
            };
            debug_assert_eq!(lt_tupstate(&page[lt_off..]), SPGIST_LIVE);
            lt_set_next_offset(&mut page[lt_off..], chain_dest[k]);
        }
        Ok(())
    })?;

    bufmgr::mark_buffer_dirty::call(buffer);

    if relation_needs_wal(index) {
        let xlrec = spgxlogVacuumLeaf {
            nDead: n_dead as u16,
            nPlaceholder: n_placeholder as u16,
            nMove: n_move as u16,
            nChain: n_chain as u16,
        };
        let state_src = store_state(&bds.spgstate);

        xloginsert::xlog_begin_insert::call()?;
        xloginsert::xlog_register_data::call(&xlrec.to_bytes(&state_src))?;
        // sizeof(xlrec) should be a multiple of sizeof(OffsetNumber).
        xloginsert::xlog_register_data::call(&offsets_to_bytes(&to_dead))?;
        xloginsert::xlog_register_data::call(&offsets_to_bytes(&to_placeholder))?;
        xloginsert::xlog_register_data::call(&offsets_to_bytes(&move_src))?;
        xloginsert::xlog_register_data::call(&offsets_to_bytes(&move_dest))?;
        xloginsert::xlog_register_data::call(&offsets_to_bytes(&chain_src))?;
        xloginsert::xlog_register_data::call(&offsets_to_bytes(&chain_dest))?;

        xloginsert::xlog_register_buffer::call(0, buffer, REGBUF_STANDARD)?;

        let recptr =
            xloginsert::xlog_insert_record::call(RM_SPGIST_ID, XLOG_SPGIST_VACUUM_LEAF)?;
        bufmgr::page_set_lsn::call(buffer, recptr)?;
    }

    miscinit::end_crit_section::call();
    Ok(())
}

/// Swap the line pointers (`ItemIdData`, 4 bytes) of two offsets on a page.
fn swap_item_ids(page: &mut [u8], src: OffsetNumber, dest: OffsetNumber) -> PgResult<()> {
    // The item-id array starts right after the page header (SizeOfPageHeaderData
    // = 24); each ItemIdData is 4 bytes, 1-based offsets.
    let id_src = item_id_byte_offset(src);
    let id_dest = item_id_byte_offset(dest);
    // Validate the offsets are addressable.
    if id_src + 4 > page.len() || id_dest + 4 > page.len() {
        return Err(elog_error("SPGiST item id out of range during move".into()));
    }
    let mut tmp = [0u8; 4];
    tmp.copy_from_slice(&page[id_src..id_src + 4]);
    let (a, b) = (
        page[id_dest..id_dest + 4].to_vec(),
        tmp,
    );
    page[id_src..id_src + 4].copy_from_slice(&a);
    page[id_dest..id_dest + 4].copy_from_slice(&b);
    Ok(())
}

/// Byte offset of the `ItemIdData` for `off` (1-based) on a page.
#[inline]
fn item_id_byte_offset(off: OffsetNumber) -> usize {
    // SizeOfPageHeaderData (24) + (off - 1) * sizeof(ItemIdData) (4).
    types_storage::bufpage::SizeOfPageHeaderData as usize + (off as usize - 1) * 4
}

// ===========================================================================
// vacuumLeafRoot (spgvacuum.c:408)
// ===========================================================================

/// `vacuumLeafRoot(bds, index, buffer)` (spgvacuum.c:408) — vacuum a root page
/// when it is also a leaf: just delete dead leaf tuples, no fancy business.
fn vacuum_leaf_root<'mcx>(
    mcx: Mcx<'mcx>,
    bds: &mut SpgBulkDeleteState<'_, 'mcx>,
    index: &Relation<'mcx>,
    buffer: Buffer,
) -> PgResult<()> {
    let max = {
        let page = bufmgr::buffer_get_page::call(mcx, buffer)?;
        PageGetMaxOffsetNumber(&PageRef::new(&page)?)
    };

    let mut to_delete: Vec<OffsetNumber> = Vec::new();

    {
        let page = bufmgr::buffer_get_page::call(mcx, buffer)?;
        for i in FirstOffsetNumber..=max {
            let it_off = {
                let pr = PageRef::new(&page)?;
                PageGetItemId(&pr, i)?.lp_off() as usize
            };
            let lt = &page[it_off..];
            if lt_tupstate(lt) == SPGIST_LIVE {
                debug_assert!(ItemPointerIsValid(&lt_heap_ptr(lt)));
                if (bds.callback)(&lt_heap_ptr(lt))? {
                    bds.stats.tuples_removed += 1.0;
                    to_delete.push(i);
                } else {
                    bds.stats.num_index_tuples += 1.0;
                }
            } else {
                // all tuples on root should be live
                return Err(elog_error(alloc::format!(
                    "unexpected SPGiST tuple state: {}",
                    lt_tupstate(lt)
                )));
            }
        }
    }

    let n_delete = to_delete.len();
    if n_delete == 0 {
        return Ok(()); // nothing more to do
    }

    // Do the update.
    miscinit::start_crit_section::call();

    // The tuple numbers are in order, so we can use PageIndexMultiDelete.
    bufmgr::with_buffer_page::call(buffer, &mut |page: &mut [u8]| {
        page_index_multi_delete(page, &to_delete)
    })?;

    bufmgr::mark_buffer_dirty::call(buffer);

    if relation_needs_wal(index) {
        let xlrec = spgxlogVacuumRoot {
            nDelete: n_delete as u16,
        };
        let state_src = store_state(&bds.spgstate);

        xloginsert::xlog_begin_insert::call()?;
        xloginsert::xlog_register_data::call(&xlrec.to_bytes(&state_src))?;
        // sizeof(xlrec) should be a multiple of sizeof(OffsetNumber).
        xloginsert::xlog_register_data::call(&offsets_to_bytes(&to_delete))?;

        xloginsert::xlog_register_buffer::call(0, buffer, REGBUF_STANDARD)?;

        let recptr =
            xloginsert::xlog_insert_record::call(RM_SPGIST_ID, XLOG_SPGIST_VACUUM_ROOT)?;
        bufmgr::page_set_lsn::call(buffer, recptr)?;
    }

    miscinit::end_crit_section::call();
    Ok(())
}

// ===========================================================================
// vacuumRedirectAndPlaceholder (spgvacuum.c:493)
// ===========================================================================

/// `vacuumRedirectAndPlaceholder(index, heaprel, buffer)` (spgvacuum.c:493) —
/// convert old REDIRECT tuples to PLACEHOLDERs once they're old enough, and
/// remove trailing PLACEHOLDERs that won't change the offsets of non-placeholder
/// tuples. Works on both leaf and inner pages.
fn vacuum_redirect_and_placeholder<'mcx>(
    mcx: Mcx<'mcx>,
    index: &Relation<'mcx>,
    heaprel: &Relation<'mcx>,
    buffer: Buffer,
) -> PgResult<()> {
    let max = {
        let page = bufmgr::buffer_get_page::call(mcx, buffer)?;
        PageGetMaxOffsetNumber(&PageRef::new(&page)?)
    };

    let mut first_placeholder = InvalidOffsetNumber;
    let mut has_non_placeholder = false;
    let mut has_update = false;
    let mut item_to_placeholder: Vec<OffsetNumber> = Vec::new();
    let mut snapshot_conflict_horizon: TransactionId = INVALID_TRANSACTION_ID;
    let is_catalog_rel = relation_is_accessible_in_logical_decoding(heaprel)?;

    let vistest = procarray::global_vis_test_for::call(heaprel.rd_id)?;

    miscinit::start_crit_section::call();

    // Scan backwards converting old redirection tuples to placeholders, and
    // identify the location of the last non-placeholder tuple.
    bufmgr::with_buffer_page::call(buffer, &mut |page: &mut [u8]| {
        let mut i = max;
        while i >= FirstOffsetNumber
            && (opaque_n_redirection(page) > 0 || !has_non_placeholder)
        {
            let dt_off = {
                let pr = PageRef::new(page)?;
                PageGetItemId(&pr, i)?.lp_off() as usize
            };

            let (st, xid) = {
                let dt = &page[dt_off..];
                (dt_tupstate(dt), dt_xid(dt))
            };

            // We can convert a REDIRECT to a PLACEHOLDER if there could no
            // longer be any index scans "in flight" to it.
            if st == SPGIST_REDIRECT
                && (!transaction_id_is_valid(xid)
                    || procarray::global_vis_test_is_removable_xid::call(vistest, xid)?)
            {
                dt_set_tupstate(&mut page[dt_off..], SPGIST_PLACEHOLDER);
                debug_assert!(opaque_n_redirection(page) > 0);
                set_opaque_n_redirection(page, opaque_n_redirection(page) - 1);
                set_opaque_n_placeholder(page, opaque_n_placeholder(page) + 1);

                // remember newest XID among the removed redirects
                if !transaction_id_is_valid(snapshot_conflict_horizon)
                    || transaction_id_precedes(snapshot_conflict_horizon, xid)
                {
                    snapshot_conflict_horizon = xid;
                }

                dt_set_pointer_invalid(&mut page[dt_off..]);

                item_to_placeholder.push(i);

                has_update = true;
            }

            // Re-read tupstate (it may have just changed to PLACEHOLDER).
            let st_now = dt_tupstate(&page[dt_off..]);
            if st_now == SPGIST_PLACEHOLDER {
                if !has_non_placeholder {
                    first_placeholder = i;
                }
            } else {
                has_non_placeholder = true;
            }

            if i == FirstOffsetNumber {
                break;
            }
            i -= 1;
        }

        // Any placeholder tuples at the end of page can safely be removed. We
        // can't remove ones before the last non-placeholder.
        if first_placeholder != InvalidOffsetNumber {
            let mut itemnos: Vec<OffsetNumber> = Vec::new();
            let mut k = first_placeholder;
            while k <= max {
                itemnos.push(k);
                k += 1;
            }

            let n = max - first_placeholder + 1;
            debug_assert!(opaque_n_placeholder(page) >= n);
            set_opaque_n_placeholder(page, opaque_n_placeholder(page) - n);

            // The array is surely sorted, so can use PageIndexMultiDelete.
            page_index_multi_delete(page, &itemnos)?;

            has_update = true;
        }
        Ok(())
    })?;

    if has_update {
        bufmgr::mark_buffer_dirty::call(buffer);
    }

    if has_update && relation_needs_wal(index) {
        let xlrec = spgxlogVacuumRedirect {
            nToPlaceholder: item_to_placeholder.len() as u16,
            firstPlaceholder: first_placeholder,
            snapshotConflictHorizon: snapshot_conflict_horizon,
            isCatalogRel: is_catalog_rel,
        };

        xloginsert::xlog_begin_insert::call()?;
        xloginsert::xlog_register_data::call(&xlrec.to_bytes())?;
        xloginsert::xlog_register_data::call(&offsets_to_bytes(&item_to_placeholder))?;

        xloginsert::xlog_register_buffer::call(0, buffer, REGBUF_STANDARD)?;

        let recptr =
            xloginsert::xlog_insert_record::call(RM_SPGIST_ID, XLOG_SPGIST_VACUUM_REDIRECT)?;
        bufmgr::page_set_lsn::call(buffer, recptr)?;
    }

    miscinit::end_crit_section::call();
    Ok(())
}

// ===========================================================================
// spgvacuumpage (spgvacuum.c:621)
// ===========================================================================

/// `spgvacuumpage(bds, buffer)` (spgvacuum.c:621) — process one page during a
/// bulkdelete scan.
fn spgvacuumpage<'mcx>(
    mcx: Mcx<'mcx>,
    bds: &mut SpgBulkDeleteState<'_, 'mcx>,
    buffer: Buffer,
) -> PgResult<()> {
    let index = bds.info.index.alias();
    let heaprel = bds.info.heaprel.alias();
    let blkno = bufmgr::buffer_get_block_number::call(buffer);

    bufmgr::lock_buffer::call(buffer, BUFFER_LOCK_EXCLUSIVE)?;

    let (is_new, is_empty, is_leaf) = {
        let page = bufmgr::buffer_get_page::call(mcx, buffer)?;
        let pr = PageRef::new(&page)?;
        (PageIsNew(&pr), PageIsEmpty(&pr), SpGistPageIsLeaf(&page))
    };

    if is_new {
        // We found an all-zero page, which could happen if the database crashed
        // just after extending the file. Recycle it.
    } else if is_empty {
        // nothing to do
    } else if is_leaf {
        if SpGistBlockIsRoot(blkno) {
            vacuum_leaf_root(mcx, bds, &index, buffer)?;
            // no need for vacuumRedirectAndPlaceholder
        } else {
            vacuum_leaf_page(mcx, bds, &index, buffer, false)?;
            vacuum_redirect_and_placeholder(mcx, &index, &heaprel, buffer)?;
        }
    } else {
        // inner page
        vacuum_redirect_and_placeholder(mcx, &index, &heaprel, buffer)?;
    }

    // The root pages must never be deleted nor marked available in FSM, because
    // we don't want them returned by a search for a place to put a new tuple.
    if !SpGistBlockIsRoot(blkno) {
        if is_new || is_empty {
            freespace::record_free_index_page::call(&index, blkno)?;
            bds.stats.pages_deleted += 1;
        } else {
            SpGistSetLastUsedPage(mcx, &index, buffer)?;
            bds.last_filled_block = blkno;
        }
    }

    bufmgr::unlock_release_buffer::call(buffer);
    Ok(())
}

// ===========================================================================
// spgprocesspending (spgvacuum.c:687)
// ===========================================================================

/// `spgprocesspending(bds)` (spgvacuum.c:687) — process the pending-TID list
/// between pages of the main scan.
fn spgprocesspending<'mcx>(
    mcx: Mcx<'mcx>,
    bds: &mut SpgBulkDeleteState<'_, 'mcx>,
) -> PgResult<()> {
    let index = bds.info.index.alias();
    let heaprel = bds.info.heaprel.alias();

    // Iterate by index because the pending list grows during the scan (new TIDs
    // can be appended by vacuumLeafPage / the inner-tuple walk below) and items
    // are marked `done` in place.
    let mut idx = 0;
    while idx < bds.pending_list.len() {
        if bds.pending_list[idx].done {
            idx += 1;
            continue; // ignore already-done items
        }

        // call vacuum_delay_point while not holding any buffer lock
        vacuum_delay_point()?;

        // examine the referenced page
        let tid = bds.pending_list[idx].tid;
        let blkno = item_pointer_get_block_number(&tid);
        let buffer = read_buffer_extended(&index, blkno)?;
        bufmgr::lock_buffer::call(buffer, BUFFER_LOCK_EXCLUSIVE)?;

        let (is_new, is_deleted, is_leaf) = {
            let page = bufmgr::buffer_get_page::call(mcx, buffer)?;
            let pr = PageRef::new(&page)?;
            (
                PageIsNew(&pr),
                SpGistPageIsDeleted(&page),
                SpGistPageIsLeaf(&page),
            )
        };

        if is_new || is_deleted {
            // Probably shouldn't happen, but ignore it
        } else if is_leaf {
            if SpGistBlockIsRoot(blkno) {
                // this should definitely not happen
                bufmgr::unlock_release_buffer::call(buffer);
                return Err(elog_error(alloc::format!(
                    "redirection leads to root page of index \"{}\"",
                    rel_name(&index)
                )));
            }

            // deal with any deletable tuples
            vacuum_leaf_page(mcx, bds, &index, buffer, true)?;
            // might as well do this while we are here
            vacuum_redirect_and_placeholder(mcx, &index, &heaprel, buffer)?;

            SpGistSetLastUsedPage(mcx, &index, buffer)?;

            // We can mark as done not only this item, but any later ones
            // pointing at the same page, since we vacuumed the whole page.
            bds.pending_list[idx].done = true;
            for n in (idx + 1)..bds.pending_list.len() {
                if item_pointer_get_block_number(&bds.pending_list[n].tid) == blkno {
                    bds.pending_list[n].done = true;
                }
            }
        } else {
            // On an inner page, visit the referenced inner tuple and add all its
            // downlinks to the pending list. There may be pending items for more
            // than one inner tuple on the same page, so get them all here.
            //
            // Collect first (the page bytes are read-only here), then add TIDs
            // after releasing the page borrow.
            let mut new_targets: Vec<ItemPointerData> = Vec::new();
            let mut to_mark_done: Vec<usize> = Vec::new();
            {
                let page = bufmgr::buffer_get_page::call(mcx, buffer)?;
                for n in idx..bds.pending_list.len() {
                    if bds.pending_list[n].done {
                        continue;
                    }
                    let ntid = bds.pending_list[n].tid;
                    if item_pointer_get_block_number(&ntid) != blkno {
                        continue;
                    }
                    let offset = item_pointer_get_offset_number(&ntid);
                    let inner_off = {
                        let pr = PageRef::new(&page)?;
                        PageGetItemId(&pr, offset)?.lp_off() as usize
                    };
                    let inner_tuple = &page[inner_off..];
                    let st = lt_tupstate(inner_tuple);
                    if st == SPGIST_LIVE {
                        for noff in node_offsets(inner_tuple) {
                            let node = &inner_tuple[noff..];
                            let node_tid = node_t_tid(node);
                            if ItemPointerIsValid(&node_tid) {
                                new_targets.push(node_tid);
                            }
                        }
                    } else if st == SPGIST_REDIRECT {
                        // transfer attention to redirect point
                        new_targets.push(dt_pointer(inner_tuple));
                    } else {
                        bufmgr::unlock_release_buffer::call(buffer);
                        return Err(elog_error(alloc::format!(
                            "unexpected SPGiST tuple state: {st}"
                        )));
                    }

                    to_mark_done.push(n);
                }
            }
            for n in to_mark_done {
                bds.pending_list[n].done = true;
            }
            for t in &new_targets {
                spg_add_pending_tid(bds, t);
            }
        }

        bufmgr::unlock_release_buffer::call(buffer);
        idx += 1;
    }

    spg_clear_pending_list(bds);
    Ok(())
}

// ===========================================================================
// spgvacuumscan (spgvacuum.c:800)
// ===========================================================================

/// `spgvacuumscan(bds)` (spgvacuum.c:800) — perform a bulkdelete scan.
fn spgvacuumscan<'mcx>(
    mcx: Mcx<'mcx>,
    bds: &mut SpgBulkDeleteState<'_, 'mcx>,
) -> PgResult<()> {
    let index = bds.info.index.alias();

    // Finish setting up spgBulkDeleteState.
    bds.spgstate = initSpGistState(mcx, &index)?;
    bds.pending_list.clear();
    bds.my_xmin = active_snapshot_xmin()?;
    bds.last_filled_block = SPGIST_LAST_FIXED_BLKNO;

    // Reset counts that will be incremented during the scan.
    bds.stats.estimated_count = false;
    bds.stats.num_index_tuples = 0.0;
    bds.stats.pages_deleted = 0;

    // We can skip locking for new or temp relations.
    let need_lock = !relation_is_local(index.rd_id)?;

    // The outer loop iterates over all index pages except the metapage, in
    // physical order. It is critical that we visit all leaf pages, including
    // ones added after we start the scan.
    let mut current_blocknum = SPGIST_METAPAGE_BLKNO + 1;
    let mut num_pages;
    loop {
        // Get the current relation length.
        if need_lock {
            lock_relation_for_extension(&index)?;
        }
        num_pages = relation_get_number_of_blocks(&index)?;
        if need_lock {
            unlock_relation_for_extension(index.rd_id)?;
        }

        // Quit if we've scanned the whole relation.
        if current_blocknum >= num_pages {
            break;
        }

        let last_exclusive = num_pages;

        // Iterate over pages, then loop back to recheck length.
        while current_blocknum < last_exclusive {
            // call vacuum_delay_point while not holding any buffer lock
            vacuum_delay_point()?;

            let buf = read_buffer_extended(&index, current_blocknum)?;
            current_blocknum += 1;

            spgvacuumpage(mcx, bds, buf)?;

            // empty the pending-list after each page
            if !bds.pending_list.is_empty() {
                spgprocesspending(mcx, bds)?;
            }
        }
    }

    // Propagate local lastUsedPages cache to metablock.
    SpGistUpdateMetaPage(&index)?;

    // If we found any empty pages (and recorded them in the FSM), then forcibly
    // update the upper-level FSM pages so searchers can find them.
    if bds.stats.pages_deleted > 0 {
        index_free_space_map_vacuum(&index)?;
    }

    // Truncation is disabled (NOT_USED in C) because it's unsafe due to possible
    // concurrent inserts.

    // Report final stats.
    bds.stats.num_pages = num_pages;
    bds.stats.pages_newly_deleted = bds.stats.pages_deleted;
    bds.stats.pages_free = bds.stats.pages_deleted;
    Ok(())
}

// ===========================================================================
// spgbulkdelete (spgvacuum.c:949) / spgvacuumcleanup (spgvacuum.c:980)
// ===========================================================================

/// `spgbulkdelete(info, stats, callback, callback_state)` (spgvacuum.c:949) —
/// bulk deletion of all index entries pointing to a set of heap tuples. The set
/// of target tuples is specified via `callback`.
pub fn spgbulkdelete<'mcx>(
    mcx: Mcx<'mcx>,
    info: &IndexVacuumInfo<'mcx>,
    stats: Option<IndexBulkDeleteResult>,
    callback: &mut IndexBulkDeleteCallback<'_>,
) -> PgResult<IndexBulkDeleteResult> {
    // allocate stats if first time through, else re-use existing struct
    let mut stats = stats.unwrap_or_default();

    // spgstate is set up by spgvacuumscan via initSpGistState; seed it now so the
    // struct is fully initialized (C leaves bds.spgstate uninitialized until
    // spgvacuumscan).
    let spgstate = initSpGistState(mcx, &info.index)?;

    let mut bds = SpgBulkDeleteState {
        info,
        stats: &mut stats,
        callback,
        spgstate,
        pending_list: Vec::new(),
        my_xmin: INVALID_TRANSACTION_ID,
        last_filled_block: InvalidBlockNumber,
    };

    spgvacuumscan(mcx, &mut bds)?;

    Ok(stats)
}

/// `dummy_callback(itemptr, state)` (spgvacuum.c:969) — deletes no tuples during
/// `spgvacuumcleanup`.
fn dummy_callback(_itemptr: &ItemPointerData) -> PgResult<bool> {
    Ok(false)
}

/// `spgvacuumcleanup(info, stats)` (spgvacuum.c:980) — post-VACUUM cleanup.
pub fn spgvacuumcleanup<'mcx>(
    mcx: Mcx<'mcx>,
    info: &IndexVacuumInfo<'mcx>,
    stats: Option<IndexBulkDeleteResult>,
) -> PgResult<Option<IndexBulkDeleteResult>> {
    // No-op in ANALYZE ONLY mode.
    if info.analyze_only {
        return Ok(stats);
    }

    // We don't need to scan the index if there was a preceding bulkdelete pass.
    // Otherwise, make a pass that won't delete any live tuples but might still
    // accomplish redirect/placeholder cleanup and/or FSM housekeeping.
    let mut stats = match stats {
        Some(s) => s,
        None => {
            let mut stats = IndexBulkDeleteResult::default();
            let mut cb = dummy_callback;
            let spgstate = initSpGistState(mcx, &info.index)?;
            let mut bds = SpgBulkDeleteState {
                info,
                stats: &mut stats,
                callback: &mut cb,
                spgstate,
                pending_list: Vec::new(),
                my_xmin: INVALID_TRANSACTION_ID,
                last_filled_block: InvalidBlockNumber,
            };
            spgvacuumscan(mcx, &mut bds)?;
            stats
        }
    };

    // It's possible to be fooled by concurrent tuple moves into double-counting,
    // so disbelieve any total that exceeds the heap's count if we know it.
    if !info.estimated_count && stats.num_index_tuples > info.num_heap_tuples {
        stats.num_index_tuples = info.num_heap_tuples;
    }

    Ok(Some(stats))
}

// ===========================================================================
// Small helpers / substrate seam wrappers.
// ===========================================================================

/// `InvalidTransactionId` (transam.h).
const INVALID_TRANSACTION_ID: TransactionId = 0;

/// `TransactionIdIsValid(xid)` — `xid != InvalidTransactionId`.
#[inline]
fn transaction_id_is_valid(xid: TransactionId) -> bool {
    xid != INVALID_TRANSACTION_ID
}

/// `TransactionIdFollowsOrEquals(a, b)` — wrapping-aware comparison (transam.c).
/// `InvalidTransactionId` (0) precedes every normal xid.
#[inline]
fn transaction_id_follows_or_equals(a: TransactionId, b: TransactionId) -> bool {
    if !transaction_id_is_valid(a) || !transaction_id_is_valid(b) {
        return a >= b;
    }
    let diff = a.wrapping_sub(b) as i32;
    diff >= 0
}

/// `TransactionIdPrecedes(a, b)` — wrapping-aware comparison (transam.c).
#[inline]
fn transaction_id_precedes(a: TransactionId, b: TransactionId) -> bool {
    if !transaction_id_is_valid(a) || !transaction_id_is_valid(b) {
        return a < b;
    }
    let diff = a.wrapping_sub(b) as i32;
    diff < 0
}

/// `RelationNeedsWAL(index)`.
#[inline]
fn relation_needs_wal(index: &Relation<'_>) -> bool {
    relcache::relation_needs_wal::call(index)
}

/// `RelationIsAccessibleInLogicalDecoding(rel)`.
#[inline]
fn relation_is_accessible_in_logical_decoding(rel: &Relation<'_>) -> PgResult<bool> {
    relcache::relation_is_accessible_in_logical_decoding::call(rel)
}

/// `GetActiveSnapshot()->xmin`.
fn active_snapshot_xmin() -> PgResult<TransactionId> {
    match snapmgr::get_active_snapshot::call()? {
        Some(snap) => Ok(snap.xmin),
        None => Err(elog_error(
            "no active snapshot set during SP-GiST vacuum".into(),
        )),
    }
}

/// `vacuum_delay_point(false)`.
#[inline]
fn vacuum_delay_point() -> PgResult<()> {
    vacuumlazy::vacuum_delay_point::call(false)
}

/// `ReadBufferExtended(rel, MAIN_FORKNUM, blkno, RBM_NORMAL, strategy)`.
#[inline]
fn read_buffer_extended<'mcx>(rel: &Relation<'mcx>, blkno: BlockNumber) -> PgResult<Buffer> {
    bufmgr::read_buffer_extended::call(rel, blkno)
}

/// `!RELATION_IS_LOCAL(index)` test helper.
#[inline]
fn relation_is_local(relid: Oid) -> PgResult<bool> {
    hio::relation_is_local::call(relid)
}

/// `LockRelationForExtension(rel, ExclusiveLock)`. The guard releases on drop;
/// the scan brackets the lock explicitly with `UnlockRelationForExtension`, so
/// we leak the guard intentionally (the explicit unlock seam releases).
fn lock_relation_for_extension<'mcx>(rel: &Relation<'mcx>) -> PgResult<()> {
    let guard = lmgr::lock_relation_for_extension::call(rel)?;
    core::mem::forget(guard);
    Ok(())
}
/// `UnlockRelationForExtension(rel, ExclusiveLock)`.
#[inline]
fn unlock_relation_for_extension(relid: Oid) -> PgResult<()> {
    lmgr::unlock_relation_for_extension::call(relid)
}
/// `RelationGetNumberOfBlocks(index)`.
#[inline]
fn relation_get_number_of_blocks<'mcx>(rel: &Relation<'mcx>) -> PgResult<BlockNumber> {
    relcache::relation_get_number_of_blocks::call(rel)
}
/// `IndexFreeSpaceMapVacuum(index)`.
#[inline]
fn index_free_space_map_vacuum<'mcx>(rel: &Relation<'mcx>) -> PgResult<()> {
    freespace::index_free_space_map_vacuum::call(rel)
}
