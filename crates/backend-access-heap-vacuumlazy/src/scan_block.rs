//! Read-stream block-selection callbacks (`vacuumlazy.c`).
//!
//!   * [`heap_vac_scan_next_block`] (vacuumlazy.c:1571) — decide which heap block
//!     the phase-I scan processes next (or "exhausted"), driving the three-state
//!     skip logic and writing the `VAC_BLK_*` flag byte.
//!   * [`find_next_unskippable_block`] (vacuumlazy.c:1676) — walk the VM forward
//!     to the next block that cannot be skipped, managing eager-scan region
//!     boundaries and the unskippable rules.
//!   * [`vacuum_reap_lp_read_stream_next`] (vacuumlazy.c:2682) — phase-III
//!     (second-pass) callback: pull the next block to reap from the TID store.
//!
//! In the owned model the C read-stream callbacks (which carried the `vacrel` /
//! `TidStoreIter *` as a `void *` and ran inside the stream) become plain
//! functions the in-crate scan/reap loops call directly; the chosen block's
//! buffer is then read through the buffer-manager seam. This keeps the *entire*
//! skip/eager-scan and TID-store iteration decision logic in-crate (1:1 with C)
//! and seams only the buffer read.

use types_error::PgResult;
use types_vacuum::vacuumlazy::{ReapBlockInfo, TidStoreIterHandle};

use crate::consts::{
    buffer_is_valid, InvalidBlockNumber, InvalidBuffer, VISIBILITYMAP_ALL_FROZEN,
    VISIBILITYMAP_ALL_VISIBLE,
};
use crate::core::{
    LVRelState, EAGER_SCAN_REGION_SIZE, SKIP_PAGES_THRESHOLD,
    VAC_BLK_ALL_VISIBLE_ACCORDING_TO_VM, VAC_BLK_WAS_EAGER_SCANNED,
};

use backend_access_heap_vacuumlazy_seams as vl;

/// What [`heap_vac_scan_next_block`] yields: the next block to process and its
/// `VAC_BLK_*` flag byte, or `Exhausted` at end of relation.
pub enum NextBlock {
    /// Process `blkno` with the given `VAC_BLK_*` flags.
    Block { blkno: types_core::BlockNumber, blk_info: u8 },
    /// The relation is exhausted (`heap_vac_scan_next_block` returned
    /// `InvalidBlockNumber`).
    Exhausted,
}

/// `heap_vac_scan_next_block()` (vacuumlazy.c:1571) — return the next block for
/// the main vacuum scan, or [`NextBlock::Exhausted`] when finished.
pub fn heap_vac_scan_next_block(vacrel: &mut LVRelState) -> PgResult<NextBlock> {
    let mut blk_info: u8 = 0;

    /* relies on InvalidBlockNumber + 1 overflowing to 0 on first call */
    let next_block = vacrel.current_block.wrapping_add(1);

    /* Have we reached the end of the relation? */
    if next_block >= vacrel.rel_pages {
        if buffer_is_valid(vacrel.next_unskippable_vmbuffer) {
            vl::release_buffer::call(vacrel.next_unskippable_vmbuffer)?;
            vacrel.next_unskippable_vmbuffer = InvalidBuffer;
        }
        return Ok(NextBlock::Exhausted);
    }

    let mut next_block = next_block;
    if next_block > vacrel.next_unskippable_block
        || vacrel.next_unskippable_block == InvalidBlockNumber
    {
        /*
         * 1. We have just processed an unskippable block (or we're at the
         * beginning of the scan).  Find the next unskippable block.
         */
        let mut skipsallvis = false;
        find_next_unskippable_block(vacrel, &mut skipsallvis)?;

        /*
         * Jump ahead only if we can skip at least SKIP_PAGES_THRESHOLD
         * consecutive pages.
         */
        if vacrel.next_unskippable_block - next_block >= SKIP_PAGES_THRESHOLD {
            next_block = vacrel.next_unskippable_block;
            if skipsallvis {
                vacrel.skippedallvis = true;
            }
        }
    }

    /* Now we must be in one of the two remaining states: */
    if next_block < vacrel.next_unskippable_block {
        /*
         * 2. We are processing a range of blocks that we could have skipped
         * but chose not to.  They are all-visible in the VM.
         */
        vacrel.current_block = next_block;
        blk_info |= VAC_BLK_ALL_VISIBLE_ACCORDING_TO_VM;
        Ok(NextBlock::Block {
            blkno: vacrel.current_block,
            blk_info,
        })
    } else {
        /*
         * 3. We reached the next unskippable block.  Process it.  On next
         * iteration, we will be back in state 1.
         */
        debug_assert!(next_block == vacrel.next_unskippable_block);

        vacrel.current_block = next_block;
        if vacrel.next_unskippable_allvis {
            blk_info |= VAC_BLK_ALL_VISIBLE_ACCORDING_TO_VM;
        }
        if vacrel.next_unskippable_eager_scanned {
            blk_info |= VAC_BLK_WAS_EAGER_SCANNED;
        }
        Ok(NextBlock::Block {
            blkno: vacrel.current_block,
            blk_info,
        })
    }
}

/// `find_next_unskippable_block()` (vacuumlazy.c:1676) — advance
/// `vacrel.next_unskippable_*` to the next block we cannot skip via the VM.
/// `*skipsallvis` is set if skipping passed over an all-visible page.
//
// `next_unskippable_allvis` is uninitialized in C; the `for(;;)` body always
// assigns it before the post-loop write-back, but Rust requires an initializer
// for definite-assignment, so the dead initial `false` triggers
// `unused_assignments`.
#[allow(unused_assignments)]
pub fn find_next_unskippable_block(
    vacrel: &mut LVRelState,
    skipsallvis: &mut bool,
) -> PgResult<()> {
    let rel_pages = vacrel.rel_pages;
    let mut next_unskippable_block = vacrel.next_unskippable_block.wrapping_add(1);
    let mut next_unskippable_vmbuffer = vacrel.next_unskippable_vmbuffer;
    let mut next_unskippable_eager_scanned = false;
    let mut next_unskippable_allvis = false;

    *skipsallvis = false;

    loop {
        let (mapbits, vmbuf) = vl::visibilitymap_get_status::call(
            vacrel.rel,
            next_unskippable_block,
            next_unskippable_vmbuffer,
        )?;
        next_unskippable_vmbuffer = vmbuf;

        next_unskippable_allvis = (mapbits & VISIBILITYMAP_ALL_VISIBLE) != 0;

        /*
         * At the start of each eager scan region, normal vacuums with eager
         * scanning enabled reset the failure counter.
         */
        if next_unskippable_block >= vacrel.next_eager_scan_region_start {
            vacrel.eager_scan_remaining_fails = vacrel.eager_scan_max_fails_per_region;
            vacrel.next_eager_scan_region_start = vacrel
                .next_eager_scan_region_start
                .wrapping_add(EAGER_SCAN_REGION_SIZE);
        }

        /* A block is unskippable if it is not all visible according to the VM. */
        if !next_unskippable_allvis {
            debug_assert!((mapbits & VISIBILITYMAP_ALL_FROZEN) == 0);
            break;
        }

        /* Always treat the last block as unsafe to skip. */
        if next_unskippable_block == rel_pages - 1 {
            break;
        }

        /* DISABLE_PAGE_SKIPPING makes all skipping unsafe. */
        if !vacrel.skipwithvm {
            break;
        }

        /* All-frozen pages can be skipped. */
        if (mapbits & VISIBILITYMAP_ALL_FROZEN) != 0 {
            next_unskippable_block = next_unskippable_block.wrapping_add(1);
            continue;
        }

        /* Aggressive vacuums cannot skip all-visible but not all-frozen pages. */
        if vacrel.aggressive {
            break;
        }

        /*
         * Normal vacuums with eager scanning enabled only skip all-visible but
         * not all-frozen pages if they have hit the failure limit.
         */
        if vacrel.eager_scan_remaining_fails > 0 {
            next_unskippable_eager_scanned = true;
            break;
        }

        /* All-visible blocks are safe to skip in a normal vacuum. */
        *skipsallvis = true;

        next_unskippable_block = next_unskippable_block.wrapping_add(1);
    }

    /* write the local variables back to vacrel */
    vacrel.next_unskippable_block = next_unskippable_block;
    vacrel.next_unskippable_allvis = next_unskippable_allvis;
    vacrel.next_unskippable_eager_scanned = next_unskippable_eager_scanned;
    vacrel.next_unskippable_vmbuffer = next_unskippable_vmbuffer;

    Ok(())
}

/// What [`vacuum_reap_lp_read_stream_next`] yields: the next block to reap and
/// the block's `TidStoreIterResult` payload (saved for offset extraction), or
/// `Exhausted` when the TID store is fully iterated.
pub enum ReapNextBlock {
    /// Reap `reap.blkno`; `reap` carries the block's dead offsets.
    Block { reap: ReapBlockInfo },
    /// The TID store is exhausted (`TidStoreIterateNext` returned `NULL` →
    /// `InvalidBlockNumber`).
    Exhausted,
}

/// `vacuum_reap_lp_read_stream_next()` (vacuumlazy.c:2682) — read-stream callback
/// for vacuum's third phase (second pass over the heap). Gets the next block from
/// the TID store and returns it, or [`ReapNextBlock::Exhausted`]
/// (`InvalidBlockNumber`) if there are no further blocks to vacuum.
///
/// In the owned model the C read-stream callback (which carried the
/// `TidStoreIter *` as a `void *callback_private_data` and copied the
/// `TidStoreIterResult` into `per_buffer_data` so the caller could later extract
/// the offsets) becomes a plain function the in-crate reap loop calls directly:
/// it iterates the TID store through the `tidstore_iterate_next` seam and returns
/// the carried [`ReapBlockInfo`] (the C `memcpy` of the result). The chosen
/// block's buffer is then read through the buffer-manager seam, symmetric with
/// the phase-I [`heap_vac_scan_next_block`] callback.
///
/// NB: Assumed to be safe to use with `READ_STREAM_USE_BATCHING`.
pub fn vacuum_reap_lp_read_stream_next(iter: TidStoreIterHandle) -> PgResult<ReapNextBlock> {
    match vl::tidstore_iterate_next::call(iter)? {
        /* The relation is exhausted. */
        None => Ok(ReapNextBlock::Exhausted),
        /*
         * Save the TidStoreIterResult for later, so we can extract the offsets.
         * It is safe to copy the result, according to TidStoreIterateNext().
         */
        Some(reap) => Ok(ReapNextBlock::Block { reap }),
    }
}
