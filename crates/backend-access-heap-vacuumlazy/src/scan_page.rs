//! Phase I — per-page processing (`vacuumlazy.c`).
//!
//!   * [`lazy_scan_new_or_empty`] (vacuumlazy.c:1809) — handle a `PageIsNew` /
//!     `PageIsEmpty` page; returns `true` if fully handled here.
//!   * [`lazy_scan_prune`] (vacuumlazy.c:1944) — the cleanup-lock path: prune and
//!     freeze tuples, collect LP_DEAD items, accumulate counters, perform the
//!     four-way VM-bit update.
//!   * [`lazy_scan_noprune`] (vacuumlazy.c:2239) — the share-lock path: scan line
//!     pointers without pruning, deciding whether an aggressive VACUUM must fall
//!     back to `lazy_scan_prune`.
//!
//! The on-page tuple/line-pointer substrate (`heap_page_prune_and_freeze`,
//! `HeapTupleSatisfiesVacuum`, `heap_tuple_should_freeze`, the bufpage / VM
//! accessors) is genuinely absent and is reached through
//! [`seams_ub_heaprest::vacuumlazy`]; the page-local accounting + VM-bit decision
//! logic is ported 1:1 in-crate over the owned [`LVRelState`].

use backend_utils_error::ereport;
use types_error::{ErrorLocation, ERROR, WARNING};
use types_core::{BlockNumber, Buffer, BLCKSZ};
use types_error::PgResult;

use crate::consts::{
    offset_number_next, pg_cmp_u16, transaction_id_is_valid, FirstOffsetNumber,
    InvalidOffsetNumber, BUFFER_LOCK_EXCLUSIVE, BUFFER_LOCK_UNLOCK, HEAPTUPLE_DEAD,
    HEAPTUPLE_DELETE_IN_PROGRESS, HEAPTUPLE_INSERT_IN_PROGRESS, HEAPTUPLE_LIVE,
    HEAPTUPLE_RECENTLY_DEAD, HEAP_PAGE_PRUNE_FREEZE, HEAP_PAGE_PRUNE_MARK_UNUSED_NOW,
    InvalidXLogRecPtr, PRUNE_VACUUM_SCAN, VISIBILITYMAP_ALL_FROZEN, VISIBILITYMAP_ALL_VISIBLE,
    VISIBILITYMAP_VALID_BITS,
};
use crate::core::LVRelState;
use crate::dead_items::dead_items_add;
use crate::heap_vacuum::heap_page_is_all_visible;

use backend_access_heap_vacuumlazy_seams as vl;

/// `SizeOfPageHeaderData` (storage/bufpage.h).
const SIZE_OF_PAGE_HEADER_DATA: usize = types_storage::bufpage::SizeOfPageHeaderData;

/// `VM_ALL_FROZEN(r, b, &v)` (access/visibilitymap.h:26).
#[inline]
fn vm_all_frozen(
    rel: types_core::Oid,
    heap_blk: BlockNumber,
    vmbuf: &mut Buffer,
) -> PgResult<bool> {
    let (status, buf) = vl::visibilitymap_get_status::call(rel, heap_blk, *vmbuf)?;
    *vmbuf = buf;
    Ok((status & VISIBILITYMAP_ALL_FROZEN) != 0)
}

fn here(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("vacuumlazy.c", 0, funcname)
}

/// `lazy_scan_new_or_empty()` (vacuumlazy.c:1809) — handle a PageIsNew or
/// PageIsEmpty page; returns `true` if the page was processed here (caller should
/// move on), `false` if it is an ordinary page to be pruned.
pub fn lazy_scan_new_or_empty(
    vacrel: &mut LVRelState,
    buf: Buffer,
    blkno: BlockNumber,
    sharelock: bool,
    vmbuffer: Buffer,
) -> PgResult<bool> {
    if vl::page_is_new::call(buf)? {
        /*
         * All-zeroes pages can be left over by a crashed extend / bulk-extend.
         * Make sure these pages are in the FSM, to ensure they can be reused.
         */
        backend_storage_buffer_bufmgr_seams::unlock_release_buffer::call(buf);

        if vl::get_recorded_free_space::call(vacrel.rel, blkno)? == 0 {
            let freespace = BLCKSZ as usize - SIZE_OF_PAGE_HEADER_DATA;
            vl::record_page_with_free_space::call(vacrel.rel, blkno, freespace)?;
        }

        return Ok(true);
    }

    if vl::page_is_empty::call(buf)? {
        /*
         * Escalate to an exclusive lock if we only have a share lock (still
         * don't need a cleanup lock).
         */
        if sharelock {
            backend_storage_buffer_bufmgr_seams::lock_buffer::call(buf, BUFFER_LOCK_UNLOCK)?;
            backend_storage_buffer_bufmgr_seams::lock_buffer::call(buf, BUFFER_LOCK_EXCLUSIVE)?;

            if !vl::page_is_empty::call(buf)? {
                /* page isn't new or empty -- keep lock and pin for now */
                return Ok(false);
            }
        }

        /* Unlike new pages, empty pages are always set all-visible + all-frozen. */
        if !vl::page_is_all_visible::call(buf)? {
            /* mark buffer dirty before writing a WAL record */
            backend_storage_buffer_bufmgr_seams::mark_buffer_dirty::call(buf);

            /*
             * If the page has not been previously WAL-logged, do so now, to
             * avoid a PANIC during replay.
             */
            if vl::relation_needs_wal::call(vacrel.rel)? && vl::page_lsn_is_invalid::call(buf)? {
                vl::log_newpage_buffer::call(buf, true)?;
            }

            vl::page_set_all_visible::call(buf)?;
            vl::visibilitymap_set::call(types_vacuum::vacuumlazy::VmSetArgs {
                rel: vacrel.rel,
                heap_blk: blkno,
                heap_buf: buf,
                rec_ptr: InvalidXLogRecPtr,
                vm_buf: vmbuffer,
                cutoff_xid: 0,
                flags: VISIBILITYMAP_ALL_VISIBLE | VISIBILITYMAP_ALL_FROZEN,
            })?;

            /* Count the newly all-frozen pages for logging */
            vacrel.vm_new_visible_pages += 1;
            vacrel.vm_new_visible_frozen_pages += 1;
        }

        let freespace = vl::page_get_heap_free_space::call(buf)?;
        backend_storage_buffer_bufmgr_seams::unlock_release_buffer::call(buf);
        vl::record_page_with_free_space::call(vacrel.rel, blkno, freespace)?;
        return Ok(true);
    }

    /* page isn't new or empty -- keep lock and pin */
    Ok(false)
}

/// `lazy_scan_prune()` (vacuumlazy.c:1944) — prune and freeze one page. Caller
/// must hold pin and buffer cleanup lock. Returns `(ndeleted, has_lpdead_items,
/// vm_page_frozen)`.
pub fn lazy_scan_prune(
    vacrel: &mut LVRelState,
    buf: Buffer,
    blkno: BlockNumber,
    mut vmbuffer: Buffer,
    all_visible_according_to_vm: bool,
) -> PgResult<(i32, bool, bool)> {
    let has_lpdead_items;
    let mut vm_page_frozen = false;

    debug_assert!(backend_storage_buffer_bufmgr_seams::buffer_get_block_number::call(buf) == blkno);

    /*
     * Prune all HOT-update chains and potentially freeze tuples on this page.
     * If the relation has no indexes, immediately mark would-be dead items
     * LP_UNUSED.
     */
    let mut prune_options = HEAP_PAGE_PRUNE_FREEZE;
    if vacrel.nindexes == 0 {
        prune_options |= HEAP_PAGE_PRUNE_MARK_UNUSED_NOW;
    }

    let out = vl::heap_page_prune_and_freeze::call(types_vacuum::vacuumlazy::PruneAndFreezeArgs {
        relation: vacrel.rel,
        buffer: buf,
        vistest: vacrel.vistest,
        options: prune_options,
        cutoffs: vacrel.cutoffs,
        reason: PRUNE_VACUUM_SCAN,
        new_relfrozen_xid_in: vacrel.new_relfrozen_xid,
        new_relmin_mxid_in: vacrel.new_relmin_mxid,
        off_loc_in: vacrel.offnum,
    })?;
    let mut presult = out.presult;
    vacrel.new_relfrozen_xid = out.new_relfrozen_xid;
    vacrel.new_relmin_mxid = out.new_relmin_mxid;
    vacrel.offnum = out.off_loc;

    debug_assert!(crate::consts::multi_xact_id_is_valid(vacrel.new_relmin_mxid));
    debug_assert!(transaction_id_is_valid(vacrel.new_relfrozen_xid));

    if presult.nfrozen > 0 {
        /* counts pages with newly frozen tuples (not pages newly all-frozen in VM) */
        vacrel.new_frozen_tuple_pages += 1;
    }

    /*
     * Cross-check with heap_page_is_all_visible() (the second-pass recheck) in
     * assertion builds, to keep the two in agreement.
     */
    if cfg!(debug_assertions) && presult.all_visible {
        debug_assert!(presult.lpdead_items == 0);
        let (ok, debug_cutoff, debug_all_frozen) = heap_page_is_all_visible(vacrel, buf)?;
        debug_assert!(ok);
        debug_assert!(presult.all_frozen == debug_all_frozen);
        debug_assert!(
            !transaction_id_is_valid(debug_cutoff) || debug_cutoff == presult.vm_conflict_horizon
        );
    }

    /* Now save details of the LP_DEAD items from the page in vacrel. */
    if presult.lpdead_items > 0 {
        vacrel.lpdead_item_pages += 1;

        /* dead_items_add requires the offsets sorted ascending. */
        let n = presult.lpdead_items as usize;
        presult.deadoffsets[..n].sort_by(|a, b| pg_cmp_u16(*a, *b).cmp(&0));

        let offsets = presult.deadoffsets[..n].to_vec();
        dead_items_add(vacrel, blkno, offsets)?;
    }

    /* Add page-local counts to whole-VACUUM counts. */
    vacrel.tuples_deleted += presult.ndeleted as i64;
    vacrel.tuples_frozen += presult.nfrozen as i64;
    vacrel.lpdead_items += presult.lpdead_items as i64;
    vacrel.live_tuples += presult.live_tuples as i64;
    vacrel.recently_dead_tuples += presult.recently_dead_tuples as i64;

    /* Can't truncate this page */
    if presult.hastup {
        vacrel.nonempty_pages = blkno + 1;
    }

    has_lpdead_items = presult.lpdead_items > 0;

    debug_assert!(!presult.all_visible || !has_lpdead_items);

    /*
     * Handle setting the VM bit based on info from the VM (as of the last
     * heap_vac_scan_next_block() call) and the all_visible / all_frozen results.
     */
    if !all_visible_according_to_vm && presult.all_visible {
        let mut flags = VISIBILITYMAP_ALL_VISIBLE;

        if presult.all_frozen {
            debug_assert!(!transaction_id_is_valid(presult.vm_conflict_horizon));
            flags |= VISIBILITYMAP_ALL_FROZEN;
        }

        vl::page_set_all_visible::call(buf)?;
        backend_storage_buffer_bufmgr_seams::mark_buffer_dirty::call(buf);
        let old_vmbits = vl::visibilitymap_set::call(types_vacuum::vacuumlazy::VmSetArgs {
            rel: vacrel.rel,
            heap_blk: blkno,
            heap_buf: buf,
            rec_ptr: InvalidXLogRecPtr,
            vm_buf: vmbuffer,
            cutoff_xid: presult.vm_conflict_horizon,
            flags,
        })?;

        if (old_vmbits & VISIBILITYMAP_ALL_VISIBLE) == 0 {
            vacrel.vm_new_visible_pages += 1;
            if presult.all_frozen {
                vacrel.vm_new_visible_frozen_pages += 1;
                vm_page_frozen = true;
            }
        } else if (old_vmbits & VISIBILITYMAP_ALL_FROZEN) == 0 && presult.all_frozen {
            vacrel.vm_new_frozen_pages += 1;
            vm_page_frozen = true;
        }
    } else if all_visible_according_to_vm
        && !vl::page_is_all_visible::call(buf)?
        && {
            let (status, buf2) =
                vl::visibilitymap_get_status::call(vacrel.rel, blkno, vmbuffer)?;
            vmbuffer = buf2;
            status != 0
        }
    {
        ereport(WARNING)
            .errmsg(format!(
                "page is not marked all-visible but visibility map bit is set in relation \"{}\" page {}",
                vacrel.relname, blkno
            ))
            .finish(here("lazy_scan_prune"))
            .ok();
        vl::visibilitymap_clear::call(vacrel.rel, blkno, vmbuffer, VISIBILITYMAP_VALID_BITS)?;
    } else if presult.lpdead_items > 0 && vl::page_is_all_visible::call(buf)? {
        ereport(WARNING)
            .errmsg(format!(
                "page containing LP_DEAD items is marked as all-visible in relation \"{}\" page {}",
                vacrel.relname, blkno
            ))
            .finish(here("lazy_scan_prune"))
            .ok();
        vl::page_clear_all_visible::call(buf)?;
        backend_storage_buffer_bufmgr_seams::mark_buffer_dirty::call(buf);
        vl::visibilitymap_clear::call(vacrel.rel, blkno, vmbuffer, VISIBILITYMAP_VALID_BITS)?;
    } else if all_visible_according_to_vm
        && presult.all_visible
        && presult.all_frozen
        && !vm_all_frozen(vacrel.rel, blkno, &mut vmbuffer)?
    {
        /*
         * Avoid relying on all_visible_according_to_vm as a proxy for the
         * page-level PD_ALL_VISIBLE bit being set, since it might be stale.
         */
        if !vl::page_is_all_visible::call(buf)? {
            vl::page_set_all_visible::call(buf)?;
            backend_storage_buffer_bufmgr_seams::mark_buffer_dirty::call(buf);
        }

        debug_assert!(!transaction_id_is_valid(presult.vm_conflict_horizon));
        let old_vmbits = vl::visibilitymap_set::call(types_vacuum::vacuumlazy::VmSetArgs {
            rel: vacrel.rel,
            heap_blk: blkno,
            heap_buf: buf,
            rec_ptr: InvalidXLogRecPtr,
            vm_buf: vmbuffer,
            cutoff_xid: 0,
            flags: VISIBILITYMAP_ALL_VISIBLE | VISIBILITYMAP_ALL_FROZEN,
        })?;

        if (old_vmbits & VISIBILITYMAP_ALL_VISIBLE) == 0 {
            vacrel.vm_new_visible_pages += 1;
            vacrel.vm_new_visible_frozen_pages += 1;
            vm_page_frozen = true;
        } else {
            vacrel.vm_new_frozen_pages += 1;
            vm_page_frozen = true;
        }
    }

    Ok((presult.ndeleted, has_lpdead_items, vm_page_frozen))
}

/// `lazy_scan_noprune()` (vacuumlazy.c:2239) — process a page without a cleanup
/// lock (cannot prune/freeze). Returns `(processed, has_lpdead_items)` where
/// `processed == false` means an aggressive VACUUM must retry via
/// `lazy_scan_prune`.
pub fn lazy_scan_noprune(
    vacrel: &mut LVRelState,
    buf: Buffer,
    blkno: BlockNumber,
) -> PgResult<(bool, bool)> {
    let mut lpdead_items: i32 = 0;
    let mut live_tuples: i32 = 0;
    let mut recently_dead_tuples: i32 = 0;
    let mut missed_dead_tuples: i32 = 0;
    let mut hastup = false;
    let mut no_freeze_relfrozen_xid = vacrel.new_relfrozen_xid;
    let mut no_freeze_relmin_mxid = vacrel.new_relmin_mxid;
    let mut deadoffsets: Vec<types_core::OffsetNumber> = Vec::new();

    debug_assert!(backend_storage_buffer_bufmgr_seams::buffer_get_block_number::call(buf) == blkno);

    let maxoff = vl::page_get_max_offset_number::call(buf)?;
    let mut offnum = FirstOffsetNumber;
    while offnum <= maxoff {
        vacrel.offnum = offnum;
        let lp = vl::page_item_id_state::call(buf, offnum)?;

        if !lp.is_used {
            offnum = offset_number_next(offnum);
            continue;
        }

        if lp.is_redirected {
            hastup = true;
            offnum = offset_number_next(offnum);
            continue;
        }

        if lp.is_dead {
            /* Deliberately don't set hastup here (see lazy_scan_prune). */
            deadoffsets.push(offnum);
            lpdead_items += 1;
            offnum = offset_number_next(offnum);
            continue;
        }

        hastup = true; /* page prevents rel truncation */

        let (should_freeze, frx, frm) = vl::heap_tuple_should_freeze::call(
            buf,
            offnum,
            vacrel.cutoffs,
            no_freeze_relfrozen_xid,
            no_freeze_relmin_mxid,
        )?;
        no_freeze_relfrozen_xid = frx;
        no_freeze_relmin_mxid = frm;

        if should_freeze {
            /* Tuple with XID < FreezeLimit (or MXID < MultiXactCutoff). */
            if vacrel.aggressive {
                /*
                 * The only safe option is to have caller perform processing of
                 * this page using lazy_scan_prune.
                 */
                vacrel.offnum = InvalidOffsetNumber;
                return Ok((false, false));
            }
            /* Non-aggressive VACUUMs accept an older final relfrozenxid/relminmxid. */
        }

        let htsv = vl::heap_tuple_satisfies_vacuum::call(
            vacrel.rel,
            buf,
            offnum,
            vacrel.cutoffs.OldestXmin,
        )?;
        match htsv {
            x if x == HEAPTUPLE_DELETE_IN_PROGRESS || x == HEAPTUPLE_LIVE => {
                live_tuples += 1;
            }
            x if x == HEAPTUPLE_DEAD => {
                missed_dead_tuples += 1;
            }
            x if x == HEAPTUPLE_RECENTLY_DEAD => {
                recently_dead_tuples += 1;
            }
            x if x == HEAPTUPLE_INSERT_IN_PROGRESS => {
                /* Do not count these rows as live. */
            }
            _ => {
                return Err(ereport(ERROR)
                    .errmsg_internal("unexpected HeapTupleSatisfiesVacuum result")
                    .into_error());
            }
        }

        offnum = offset_number_next(offnum);
    }

    vacrel.offnum = InvalidOffsetNumber;

    /* Remember the no-freeze trackers now (lazy_scan_prune expects a clean slate). */
    vacrel.new_relfrozen_xid = no_freeze_relfrozen_xid;
    vacrel.new_relmin_mxid = no_freeze_relmin_mxid;

    /* Save any LP_DEAD items found on the page in dead_items. */
    if vacrel.nindexes == 0 {
        /* Using one-pass strategy (since table has no indexes). */
        if lpdead_items > 0 {
            /* Count the LP_DEAD items as missed_dead_tuples instead. */
            hastup = true;
            missed_dead_tuples += lpdead_items;
        }
    } else if lpdead_items > 0 {
        vacrel.lpdead_item_pages += 1;
        dead_items_add(vacrel, blkno, deadoffsets)?;
        vacrel.lpdead_items += lpdead_items as i64;
    }

    /* Add page-local counts to whole-VACUUM counts. */
    vacrel.live_tuples += live_tuples as i64;
    vacrel.recently_dead_tuples += recently_dead_tuples as i64;
    vacrel.missed_dead_tuples += missed_dead_tuples as i64;
    if missed_dead_tuples > 0 {
        vacrel.missed_dead_pages += 1;
    }

    /* Can't truncate this page */
    if hastup {
        vacrel.nonempty_pages = blkno + 1;
    }

    Ok((true, lpdead_items > 0))
}
