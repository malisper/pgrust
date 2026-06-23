//! Port of `src/backend/access/heap/hio.c` — heap "insertion organization":
//! placing a tuple onto a page ([`RelationPutHeapTuple`]) and finding/extending
//! a page with enough free space ([`RelationGetBufferForTuple`]).
//!
//! Every function in `hio.c` is ported 1:1 here (branch order, message text,
//! severity, and SQLSTATE preserved):
//!
//! * [`RelationPutHeapTuple`]       — public
//! * [`RelationGetBufferForTuple`]  — public
//! * `ReadBufferBI`                 — `static`, ported as a private helper
//! * `GetVisibilityMapPins`         — `static`, ported as a private helper
//! * `RelationAddBlocks`            — `static`, ported as a private helper
//!
//! The control flow that does not require the opaque `Page` / `Relation`
//! layouts — the `MAXALIGN` / fillfactor / target-free-space arithmetic, the
//! page-selection loop, the lock-ordering dance, the `GetVisibilityMapPins`
//! retry loop, the relation-extension bookkeeping, and `RelationPutHeapTuple`'s
//! tuple-pointer updates — is implemented directly here, identically to the C.
//!
//! Everything `hio.c` reaches outside that logic (the buffer manager, the
//! free-space map, the relation-extension lock, the visibility map, the
//! relation cache, and the opaque-`Page` predicates / mutators) crosses the
//! per-owner [`hio_seams`] seams — each a loud-panic slot
//! until its owner installs the real implementation. The relation crosses as
//! its bare `Oid` identity (`RelationGetRelid`); buffers cross as `Buffer`
//! handles. No silent fallback.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]
#![allow(clippy::too_many_arguments)]

use utils_error::ereport;
use types_error::{PgResult, ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERROR, PANIC};

use types_core::{BlockNumber, Oid, OffsetNumber, Size};
use rel::RelationData;
use types_storage::buf::{
    BUFFER_LOCK_EXCLUSIVE, BUFFER_LOCK_UNLOCK, RBM_NORMAL, RBM_ZERO_AND_CLEANUP_LOCK,
    RBM_ZERO_AND_LOCK,
};
use types_storage::bufpage::{
    ItemIdData, MaxHeapTupleSize, MaxHeapTuplesPerPage, SizeOfPageHeaderData,
};
use types_storage::storage::{Buffer, InvalidBuffer};
use types_tableam::BulkInsertStateData;
use types_tuple::heaptuple::{
    HeapTupleData, ItemPointerData, HEAP_XMAX_COMMITTED, HEAP_XMAX_IS_MULTI,
};

use hio_seams as hio_seam;

/// `InvalidBlockNumber` (`storage/block.h`) == `(BlockNumber) 0xFFFFFFFF`.
const InvalidBlockNumber: BlockNumber = 0xFFFF_FFFF;
/// `InvalidOffsetNumber` (`storage/off.h`) == `(OffsetNumber) 0`.
const InvalidOffsetNumber: OffsetNumber = 0;

/// `HEAP_INSERT_SKIP_FSM` (`access/heapam.h`) == `TABLE_INSERT_SKIP_FSM`
/// (`access/tableam.h`, `0x0002`).
const HEAP_INSERT_SKIP_FSM: i32 = 0x0002;
/// `HEAP_INSERT_FROZEN` (`access/heapam.h`) == `TABLE_INSERT_FROZEN`
/// (`access/tableam.h`, `0x0004`).
const HEAP_INSERT_FROZEN: i32 = 0x0004;
/// `HEAP_DEFAULT_FILLFACTOR` (`utils/rel.h`) == `100`.
const HEAP_DEFAULT_FILLFACTOR: i32 = 100;
/// `SpecTokenOffsetNumber` (`storage/itemptr.h`) == `0xfffe`.
const SpecTokenOffsetNumber: OffsetNumber = 0xfffe;

/// `MAX_BUFFERS_TO_EXTEND_BY` (local `#define` in `RelationAddBlocks`).
const MAX_BUFFERS_TO_EXTEND_BY: u32 = 64;

/// `MAXALIGN(LEN)` — round up to `MAXIMUM_ALIGNOF` (8 on supported platforms).
#[inline]
fn maxalign(len: Size) -> Size {
    const MAXIMUM_ALIGNOF: Size = 8;
    (len.wrapping_add(MAXIMUM_ALIGNOF - 1)) & !(MAXIMUM_ALIGNOF - 1)
}

/// The relcache identity of a relation: `RelationGetRelid(rel)` == `rd_id`. The
/// relation crosses the `hio.c` seams as this bare `Oid`; the substrate
/// re-resolves the live relation from the relcache.
#[inline]
fn rel_handle(relation: &RelationData<'_>) -> Oid {
    relation.rd_id
}

/// `IOContextForStrategy(bistate->strategy)` — the pg_stat_io context a
/// bulk-insert's reads/extends are accounted under. The CTAS / COPY-IN path uses
/// a BAS_BULKWRITE strategy → IOCONTEXT_BULKWRITE; a NULL strategy is
/// IOCONTEXT_NORMAL.
fn io_context_for_strategy(
    strategy: &types_storage::buf::BufferAccessStrategy,
) -> types_storage::buf::IOContext {
    use types_storage::buf::{BufferAccessStrategyType as Bas, IOContext};
    match strategy {
        None => IOContext::IOCONTEXT_NORMAL,
        Some(s) => match s.borrow().btype {
            Bas::BasNormal => IOContext::IOCONTEXT_NORMAL,
            Bas::BasBulkread => IOContext::IOCONTEXT_BULKREAD,
            Bas::BasBulkwrite => IOContext::IOCONTEXT_BULKWRITE,
            Bas::BasVacuum => IOContext::IOCONTEXT_VACUUM,
        },
    }
}

/// `BufferIsValid(buffer)` — a buffer number is valid when nonzero
/// (`InvalidBuffer == 0`).
#[inline]
fn BufferIsValid(buffer: Buffer) -> bool {
    buffer != InvalidBuffer
}

/// `tuple->t_data->t_infomask`, or 0 if the tuple has no header data. (The owned
/// `HeapTupleData::t_data` is an `Option<PgBox<HeapTupleHeaderData>>`; a tuple
/// prepared for insertion always has a header.)
#[inline]
fn tuple_infomask(tuple: &HeapTupleData<'_>) -> u16 {
    match &tuple.t_data {
        Some(hdr) => hdr.t_infomask,
        None => 0,
    }
}

/// `HeapTupleHeaderIsSpeculative(tup)` — the tuple's CTID block id holds a
/// speculative token (its offset number is `SpecTokenOffsetNumber`).
#[inline]
fn HeapTupleHeaderIsSpeculative(tuple: &HeapTupleData<'_>) -> bool {
    match &tuple.t_data {
        Some(hdr) => hdr.t_ctid.ip_posid == SpecTokenOffsetNumber,
        None => false,
    }
}

// ---------------------------------------------------------------------------
// RelationPutHeapTuple
// ---------------------------------------------------------------------------

/// `RelationPutHeapTuple` — place tuple at specified page.
///
/// `!!! EREPORT(ERROR) IS DISALLOWED HERE !!!`  Must PANIC on failure.
///
/// Caller must hold `BUFFER_LOCK_EXCLUSIVE` on the buffer.
pub fn RelationPutHeapTuple(
    relation: &RelationData<'_>,
    buffer: Buffer,
    tuple: &mut HeapTupleData<'_>,
    image: &[u8],
    token: bool,
) -> PgResult<()> {
    let _ = relation;

    // A tuple that's being inserted speculatively should already have its token
    // set.
    debug_assert!(!token || HeapTupleHeaderIsSpeculative(tuple));

    // Do not allow tuples with invalid combinations of hint bits to be placed on
    // a page.  This combination is detected as corruption by the contrib/amcheck
    // logic, so if you disable this assertion, make corresponding changes there.
    debug_assert!({
        let infomask = tuple_infomask(tuple);
        !((infomask & HEAP_XMAX_COMMITTED) != 0 && (infomask & HEAP_XMAX_IS_MULTI) != 0)
    });

    // Add the tuple to the page.
    // `offnum = PageAddItem(page, (Item) tuple->t_data, tuple->t_len,
    //                       InvalidOffsetNumber, false, true)`.
    debug_assert_eq!(image.len(), tuple.t_len as usize);
    let offnum = hio_seam::page_add_item::call(buffer, image)?;

    if offnum == InvalidOffsetNumber {
        return Err(ereport(PANIC)
            .errmsg_internal("failed to add tuple to page")
            .into_error());
    }

    // Update tuple->t_self to the actual position where it was stored.
    // `ItemPointerSet(&tuple->t_self, BufferGetBlockNumber(buffer), offnum)`.
    let blkno = hio_seam::buffer_get_block_number::call(buffer)?;
    tuple.t_self = ItemPointerData::new(blkno, offnum);

    // Insert the correct position into CTID of the stored tuple, too (unless this
    // is a speculative insertion, in which case the token is held in the CTID
    // field instead).
    if !token {
        let ctid = tuple.t_self;
        hio_seam::set_stored_tuple_ctid::call(buffer, offnum, ctid)?;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// ReadBufferBI (static)
// ---------------------------------------------------------------------------

/// `ReadBufferBI` — read a block in `mode`, using the bulk-insert strategy if
/// `bistate` isn't NULL.
fn ReadBufferBI(
    relation: &RelationData<'_>,
    target_block: BlockNumber,
    mode: i32,
    bistate: Option<&mut BulkInsertStateData>,
) -> PgResult<Buffer> {
    let rel = rel_handle(relation);

    // If not bulk-insert, exactly like ReadBuffer.
    let bistate = match bistate {
        None => {
            return hio_seam::read_buffer_extended::call(
                rel,
                target_block,
                mode,
                types_storage::buf::IOContext::IOCONTEXT_NORMAL,
            );
        }
        Some(bistate) => bistate,
    };

    // If we have the desired block already pinned, re-pin and return it.
    if bistate.current_buf != InvalidBuffer {
        if hio_seam::buffer_get_block_number::call(bistate.current_buf)? == target_block {
            // Currently the LOCK variants are only used for extending relation,
            // which should never reach this branch.
            debug_assert!(mode != RBM_ZERO_AND_LOCK && mode != RBM_ZERO_AND_CLEANUP_LOCK);

            hio_seam::incr_buffer_ref_count::call(bistate.current_buf)?;
            return Ok(bistate.current_buf);
        }
        // ... else drop the old buffer.
        hio_seam::release_buffer::call(bistate.current_buf)?;
        bistate.current_buf = InvalidBuffer;
    }

    // Perform a read using the buffer strategy.
    let io_context = io_context_for_strategy(&bistate.strategy);
    let buffer = hio_seam::read_buffer_extended::call(rel, target_block, mode, io_context)?;

    // Save the selected block as target for future inserts.
    hio_seam::incr_buffer_ref_count::call(buffer)?;
    bistate.current_buf = buffer;

    Ok(buffer)
}

// ---------------------------------------------------------------------------
// GetVisibilityMapPins (static)
// ---------------------------------------------------------------------------

/// `GetVisibilityMapPins` — for each heap page which is all-visible, acquire a
/// pin on the appropriate visibility map page, if we haven't already got one.
///
/// To avoid complexity in the callers, either `buffer1` or `buffer2` may be
/// `InvalidBuffer` if only one buffer is involved.  For the same reason,
/// `block2` may be smaller than `block1`.
///
/// Returns whether buffer locks were temporarily released.
fn GetVisibilityMapPins(
    relation: &RelationData<'_>,
    mut buffer1: Buffer,
    mut buffer2: Buffer,
    mut block1: BlockNumber,
    mut block2: BlockNumber,
    vmbuffer1: &mut Buffer,
    vmbuffer2: &mut Buffer,
) -> PgResult<bool> {
    let rel = rel_handle(relation);
    let mut released_locks = false;

    // To swap the vm-buffer out-params as the C swaps the pointers, work against
    // locals and copy the (possibly swapped) results back at the end.
    let mut vmbuf1 = *vmbuffer1;
    let mut vmbuf2 = *vmbuffer2;

    // Swap buffers around to handle case of a single block/buffer, and to handle
    // if lock ordering rules require to lock block2 first.
    let mut swapped = false;
    if !BufferIsValid(buffer1) || (BufferIsValid(buffer2) && block1 > block2) {
        core::mem::swap(&mut buffer1, &mut buffer2);
        core::mem::swap(&mut vmbuf1, &mut vmbuf2);
        core::mem::swap(&mut block1, &mut block2);
        swapped = true;
    }

    debug_assert!(BufferIsValid(buffer1));
    debug_assert!(buffer2 == InvalidBuffer || block1 <= block2);

    loop {
        // Figure out which pins we need but don't have.
        let need_to_pin_buffer1 = hio_seam::page_is_all_visible::call(buffer1)?
            && !hio_seam::visibilitymap_pin_ok::call(block1, vmbuf1)?;
        let need_to_pin_buffer2 = buffer2 != InvalidBuffer
            && hio_seam::page_is_all_visible::call(buffer2)?
            && !hio_seam::visibilitymap_pin_ok::call(block2, vmbuf2)?;
        if !need_to_pin_buffer1 && !need_to_pin_buffer2 {
            break;
        }

        // We must unlock both buffers before doing any I/O.
        released_locks = true;
        hio_seam::lock_buffer::call(buffer1, BUFFER_LOCK_UNLOCK)?;
        if buffer2 != InvalidBuffer && buffer2 != buffer1 {
            hio_seam::lock_buffer::call(buffer2, BUFFER_LOCK_UNLOCK)?;
        }

        // Get pins.
        if need_to_pin_buffer1 {
            vmbuf1 = hio_seam::visibilitymap_pin::call(rel, block1, vmbuf1)?;
        }
        if need_to_pin_buffer2 {
            vmbuf2 = hio_seam::visibilitymap_pin::call(rel, block2, vmbuf2)?;
        }

        // Relock buffers.
        hio_seam::lock_buffer::call(buffer1, BUFFER_LOCK_EXCLUSIVE)?;
        if buffer2 != InvalidBuffer && buffer2 != buffer1 {
            hio_seam::lock_buffer::call(buffer2, BUFFER_LOCK_EXCLUSIVE)?;
        }

        // If there are two buffers involved and we pinned just one of them, it's
        // possible that the second one became all-visible while we were busy
        // pinning the first one.  If it looks like that's a possible scenario,
        // we'll need to make a second pass through this loop.
        if buffer2 == InvalidBuffer
            || buffer1 == buffer2
            || (need_to_pin_buffer1 && need_to_pin_buffer2)
        {
            break;
        }
    }

    // Copy the (possibly swapped) vm-buffer results back to the caller's
    // out-parameters, mirroring the C's pointer-swap aliasing.
    if swapped {
        *vmbuffer1 = vmbuf2;
        *vmbuffer2 = vmbuf1;
    } else {
        *vmbuffer1 = vmbuf1;
        *vmbuffer2 = vmbuf2;
    }

    Ok(released_locks)
}

// ---------------------------------------------------------------------------
// RelationAddBlocks (static)
// ---------------------------------------------------------------------------

/// `RelationAddBlocks` — extend the relation by multiple pages, if beneficial.
///
/// Returns a buffer for a newly extended block.  If possible, the buffer is
/// returned exclusively locked.  `*did_unlock` is set to true if the lock had to
/// be released, false otherwise.
fn RelationAddBlocks(
    relation: &RelationData<'_>,
    bistate: Option<&mut BulkInsertStateData>,
    num_pages: i32,
    use_fsm: bool,
    did_unlock: &mut bool,
) -> PgResult<Buffer> {
    let rel = rel_handle(relation);

    // `bistate == NULL` test reused throughout.
    let has_bistate = bistate.is_some();

    // Determine by how many pages to try to extend by.
    let extend_by_pages: u32 = if !has_bistate && !use_fsm {
        // If we have neither bistate, nor can use the FSM, we can't bulk extend -
        // there'd be no way to find the additional pages.
        1
    } else {
        // Try to extend at least by the number of pages the caller needs.  We can
        // remember the additional pages (either via FSM or bistate).
        let mut ebp = num_pages as u32;

        let waitcount: u32 = if !hio_seam::relation_is_local::call(rel)? {
            hio_seam::relation_extension_lock_waiter_count::call(rel)?
        } else {
            0
        };

        // Multiply the number of pages to extend by the number of waiters.  Do
        // this even if we're not using the FSM, as it still relieves contention,
        // by deferring the next time this backend needs to extend.  In that case
        // the extended pages will be found via bistate->next_free.
        ebp = ebp.wrapping_add(ebp.wrapping_mul(waitcount));

        // If we previously extended using the same bistate, it's very likely
        // we'll extend some more.  Try to extend by as many pages as before.
        if let Some(ref bistate) = bistate {
            ebp = ebp.max(bistate.already_extended_by);
        }

        // Can't extend by more than MAX_BUFFERS_TO_EXTEND_BY, we need to pin them
        // all concurrently.
        ebp.min(MAX_BUFFERS_TO_EXTEND_BY)
    };

    // How many of the extended pages should be entered into the FSM?
    //
    // If we have a bistate, only enter pages that we don't need ourselves into
    // the FSM.  Otherwise every other backend will immediately try to use the
    // pages this backend needs for itself, causing unnecessary contention.  If we
    // don't have a bistate, we can't avoid the FSM.
    //
    // Never enter the page returned into the FSM, we'll immediately use it.
    let not_in_fsm_pages: u32 = if num_pages > 1 && !has_bistate {
        1
    } else {
        num_pages as u32
    };

    // prepare to put another buffer into the bistate.
    let mut bistate = bistate;
    if let Some(ref mut bistate) = bistate {
        if bistate.current_buf != InvalidBuffer {
            hio_seam::release_buffer::call(bistate.current_buf)?;
            bistate.current_buf = InvalidBuffer;
        }
    }

    // Extend the relation.  We ask for the first returned page to be locked, so
    // that we are sure that nobody has inserted into the page concurrently.
    let io_context = bistate
        .as_ref()
        .map(|b| io_context_for_strategy(&b.strategy))
        .unwrap_or(types_storage::buf::IOContext::IOCONTEXT_NORMAL);
    let extended = hio_seam::extend_buffered_rel_by::call(rel, io_context, extend_by_pages)?;
    let first_block = extended.first_block;
    let extend_by_pages = extended.extended_by;
    let victim_buffers = extended.victim_buffers;
    let buffer = victim_buffers[0]; // the buffer the function will return
    let last_block = first_block.wrapping_add(extend_by_pages.wrapping_sub(1));
    debug_assert!(first_block == hio_seam::buffer_get_block_number::call(buffer)?);

    // Relation is now extended.  Initialize the page.  We do this here, before
    // potentially releasing the lock on the page, because it allows us to double
    // check that the page contents are empty (this should never happen, but if it
    // does we don't want to risk wiping out valid data).
    if !hio_seam::page_is_new::call(buffer)? {
        return Err(ereport(ERROR)
            .errmsg_internal(format!(
                "page {} of relation \"{}\" should be empty but is not",
                first_block,
                hio_seam::relation_get_relation_name::call(rel)?
            ))
            .into_error());
    }

    hio_seam::page_init::call(buffer)?;
    hio_seam::mark_buffer_dirty::call(buffer)?;

    // If we decided to put pages into the FSM, release the buffer lock (but not
    // pin), we don't want to do IO while holding a buffer lock.  This will
    // necessitate a bit more extensive checking in our caller.
    if use_fsm && not_in_fsm_pages < extend_by_pages {
        hio_seam::lock_buffer::call(buffer, BUFFER_LOCK_UNLOCK)?;
        *did_unlock = true;
    } else {
        *did_unlock = false;
    }

    // Relation is now extended.  Release pins on all buffers, except for the first
    // (which we'll return).  If we decided to put pages into the FSM, we can do
    // that as part of the same loop.
    let mut i: u32 = 1;
    while i < extend_by_pages {
        let cur_block = first_block.wrapping_add(i);

        debug_assert!(
            cur_block == hio_seam::buffer_get_block_number::call(victim_buffers[i as usize])?
        );
        debug_assert!(cur_block != InvalidBlockNumber);

        hio_seam::release_buffer::call(victim_buffers[i as usize])?;

        if use_fsm && i >= not_in_fsm_pages {
            let freespace = hio_seam::buffer_get_page_size::call(victim_buffers[i as usize])?
                .wrapping_sub(SizeOfPageHeaderData);

            hio_seam::record_page_with_free_space::call(rel, cur_block, freespace)?;
        }

        i = i.wrapping_add(1);
    }

    if use_fsm && not_in_fsm_pages < extend_by_pages {
        let first_fsm_block = first_block.wrapping_add(not_in_fsm_pages);

        hio_seam::free_space_map_vacuum_range::call(rel, first_fsm_block, last_block)?;
    }

    if let Some(ref mut bistate) = bistate {
        // Remember the additional pages we extended by, so we later can use them
        // without looking into the FSM.
        if extend_by_pages > 1 {
            bistate.next_free = first_block.wrapping_add(1);
            bistate.last_free = last_block;
        } else {
            bistate.next_free = InvalidBlockNumber;
            bistate.last_free = InvalidBlockNumber;
        }

        // maintain bistate->current_buf.
        hio_seam::incr_buffer_ref_count::call(buffer)?;
        bistate.current_buf = buffer;
        bistate.already_extended_by = bistate.already_extended_by.wrapping_add(extend_by_pages);
    }

    Ok(buffer)
}

// ---------------------------------------------------------------------------
// RelationGetBufferForTuple
// ---------------------------------------------------------------------------

/// `RelationGetBufferForTuple` — return a pinned and exclusive-locked buffer of
/// a page in `relation` with free space `>= len`, extending the relation if
/// necessary.  See the C source for the full contract on `other_buffer`,
/// `bistate`, the visibility-map pins, and the FSM interaction.
///
/// `ereport(ERROR)` is allowed here, so this routine *must* be called before any
/// (unlogged) changes are made in the buffer pool.
pub fn RelationGetBufferForTuple(
    relation: &RelationData<'_>,
    mut len: Size,
    other_buffer: Buffer,
    options: i32,
    mut bistate: Option<&mut BulkInsertStateData>,
    vmbuffer: &mut Buffer,
    vmbuffer_other: &mut Buffer,
    mut num_pages: i32,
) -> PgResult<Buffer> {
    let rel = rel_handle(relation);

    let use_fsm = (options & HEAP_INSERT_SKIP_FSM) == 0;
    #[allow(unused_assignments)]
    let mut buffer: Buffer = InvalidBuffer;
    let mut page_free_space: Size;
    let mut target_block: BlockNumber;
    let mut unlocked_target_buffer: bool;
    let mut recheck_vm_pins: bool;

    len = maxalign(len); // be conservative

    // if the caller doesn't know by how many pages to extend, extend by 1.
    if num_pages <= 0 {
        num_pages = 1;
    }

    // Bulk insert is not supported for updates, only inserts.
    debug_assert!(other_buffer == InvalidBuffer || bistate.is_none());

    // If we're gonna fail for oversize tuple, do it right away.
    if len > MaxHeapTupleSize {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
            .errmsg(format!(
                "row is too big: size {}, maximum size {}",
                len, MaxHeapTupleSize
            ))
            .into_error());
    }

    // Compute desired extra freespace due to fillfactor option.
    let save_free_space: Size =
        hio_seam::relation_get_target_page_free_space::call(rel, HEAP_DEFAULT_FILLFACTOR)?;

    // Since pages without tuples can still have line pointers, we consider pages
    // "empty" when the unavailable space is slight.  This threshold is somewhat
    // arbitrary, but it should prevent most unnecessary relation extensions while
    // inserting large tuples into low-fillfactor tables.
    let nearly_empty_free_space: Size = MaxHeapTupleSize.wrapping_sub(
        (MaxHeapTuplesPerPage / 8).wrapping_mul(core::mem::size_of::<ItemIdData>()),
    );
    let target_free_space: Size = if len.wrapping_add(save_free_space) > nearly_empty_free_space {
        // C: `Max(len, nearlyEmptyFreeSpace)`.
        len.max(nearly_empty_free_space)
    } else {
        len.wrapping_add(save_free_space)
    };

    let other_block: BlockNumber = if other_buffer != InvalidBuffer {
        hio_seam::buffer_get_block_number::call(other_buffer)?
    } else {
        InvalidBlockNumber // just to keep compiler quiet
    };

    // We first try to put the tuple on the same page we last inserted a tuple on,
    // as cached in the BulkInsertState or relcache entry.  If that doesn't work,
    // we ask the Free Space Map to locate a suitable page.  Since the FSM's info
    // might be out of date, we have to be prepared to loop around and retry
    // multiple times.  When use_fsm is false, we either put the tuple onto the
    // existing target page or extend the relation.
    if let Some(ref bistate) = bistate {
        if bistate.current_buf != InvalidBuffer {
            target_block = hio_seam::buffer_get_block_number::call(bistate.current_buf)?;
        } else {
            target_block = hio_seam::relation_get_target_block::call(rel)?;
        }
    } else {
        target_block = hio_seam::relation_get_target_block::call(rel)?;
    }

    if target_block == InvalidBlockNumber && use_fsm {
        // We have no cached target page, so ask the FSM for an initial target.
        target_block = hio_seam::get_page_with_free_space::call(rel, target_free_space)?;
    }

    // If the FSM knows nothing of the rel, try the last page before we give up and
    // extend.  This avoids one-tuple-per-page syndrome during bootstrapping or in
    // a recently-started system.
    if target_block == InvalidBlockNumber {
        let nblocks = hio_seam::relation_get_number_of_blocks::call(rel)?;

        if nblocks > 0 {
            target_block = nblocks - 1;
        }
    }

    // `loop:` label in the C; the inner `while` is the page-selection loop.
    'outer: loop {
        while target_block != InvalidBlockNumber {
            // Read and exclusive-lock the target block, as well as the other block
            // if one was given, taking suitable care with lock ordering and the
            // possibility they are the same block.
            //
            // If the page-level all-visible flag is set, caller will need to clear
            // both that and the corresponding visibility map bit.  We check the
            // bit here before taking the lock, and pin the page if it appears
            // necessary.  Checking without the lock creates a risk of getting the
            // wrong answer, so we'll have to recheck after acquiring the lock.
            if other_buffer == InvalidBuffer {
                // easy case.
                buffer = ReadBufferBI(relation, target_block, RBM_NORMAL, bistate.as_deref_mut())?;
                if hio_seam::page_is_all_visible::call(buffer)? {
                    *vmbuffer = hio_seam::visibilitymap_pin::call(rel, target_block, *vmbuffer)?;
                }

                // If the page is empty, pin vmbuffer to set all_frozen bit later.
                if (options & HEAP_INSERT_FROZEN) != 0
                    && hio_seam::page_get_max_offset_number::call(buffer)? == 0
                {
                    *vmbuffer = hio_seam::visibilitymap_pin::call(rel, target_block, *vmbuffer)?;
                }

                hio_seam::lock_buffer::call(buffer, BUFFER_LOCK_EXCLUSIVE)?;
            } else if other_block == target_block {
                // also easy case.
                buffer = other_buffer;
                if hio_seam::page_is_all_visible::call(buffer)? {
                    *vmbuffer = hio_seam::visibilitymap_pin::call(rel, target_block, *vmbuffer)?;
                }
                hio_seam::lock_buffer::call(buffer, BUFFER_LOCK_EXCLUSIVE)?;
            } else if other_block < target_block {
                // lock other buffer first.
                buffer = hio_seam::read_buffer::call(rel, target_block)?;
                if hio_seam::page_is_all_visible::call(buffer)? {
                    *vmbuffer = hio_seam::visibilitymap_pin::call(rel, target_block, *vmbuffer)?;
                }
                hio_seam::lock_buffer::call(other_buffer, BUFFER_LOCK_EXCLUSIVE)?;
                hio_seam::lock_buffer::call(buffer, BUFFER_LOCK_EXCLUSIVE)?;
            } else {
                // lock target buffer first.
                buffer = hio_seam::read_buffer::call(rel, target_block)?;
                if hio_seam::page_is_all_visible::call(buffer)? {
                    *vmbuffer = hio_seam::visibilitymap_pin::call(rel, target_block, *vmbuffer)?;
                }
                hio_seam::lock_buffer::call(buffer, BUFFER_LOCK_EXCLUSIVE)?;
                hio_seam::lock_buffer::call(other_buffer, BUFFER_LOCK_EXCLUSIVE)?;
            }

            // We now have the target page (and the other buffer, if any) pinned
            // and locked.  Since our initial PageIsAllVisible checks were performed
            // before acquiring the lock, the results might now be out of date.  In
            // that case GetVisibilityMapPins gives up its locks, gets the pin(s) it
            // failed to get earlier, and re-locks.
            GetVisibilityMapPins(
                relation,
                buffer,
                other_buffer,
                target_block,
                other_block,
                vmbuffer,
                vmbuffer_other,
            )?;

            // Now we can check to see if there's enough free space here.  If so,
            // we're done.
            //
            // If necessary initialize page, it'll be used soon.
            if hio_seam::page_is_new::call(buffer)? {
                hio_seam::page_init::call(buffer)?;
                hio_seam::mark_buffer_dirty::call(buffer)?;
            }

            page_free_space = hio_seam::page_get_heap_free_space::call(buffer)?;
            if target_free_space <= page_free_space {
                // use this page as future insert target, too.
                hio_seam::relation_set_target_block::call(rel, target_block)?;
                return Ok(buffer);
            }

            // Not enough space, so we must give up our page locks and pin (if any)
            // and prepare to look elsewhere.  We don't care which order we unlock
            // the two buffers in.
            hio_seam::lock_buffer::call(buffer, BUFFER_LOCK_UNLOCK)?;
            if other_buffer == InvalidBuffer {
                hio_seam::release_buffer::call(buffer)?;
            } else if other_block != target_block {
                hio_seam::lock_buffer::call(other_buffer, BUFFER_LOCK_UNLOCK)?;
                hio_seam::release_buffer::call(buffer)?;
            }

            // Is there an ongoing bulk extension?
            //
            // (`bistate && bistate->next_free != InvalidBlockNumber`.)
            let ongoing_bulk = bistate
                .as_ref()
                .map(|b| b.next_free != InvalidBlockNumber)
                .unwrap_or(false);
            if let (true, Some(bistate)) = (ongoing_bulk, bistate.as_deref_mut()) {
                debug_assert!(bistate.next_free <= bistate.last_free);

                // We bulk extended the relation before, and there are still some
                // unused pages from that extension, so we don't need to look in the
                // FSM for a new page.  But do record the free space from the last
                // page, somebody might insert narrower tuples later.
                if use_fsm {
                    hio_seam::record_page_with_free_space::call(rel, target_block, page_free_space)?;
                }

                target_block = bistate.next_free;
                if bistate.next_free >= bistate.last_free {
                    bistate.next_free = InvalidBlockNumber;
                    bistate.last_free = InvalidBlockNumber;
                } else {
                    bistate.next_free += 1;
                }
            } else if !use_fsm {
                // Without FSM, always fall out of the loop and extend.
                break;
            } else {
                // Update FSM as to condition of this page, and ask for another page
                // to try.
                target_block = hio_seam::record_and_get_page_with_free_space::call(
                    rel,
                    target_block,
                    page_free_space,
                    target_free_space,
                )?;
            }
        }

        // Have to extend the relation.
        unlocked_target_buffer = false;
        buffer = RelationAddBlocks(
            relation,
            bistate.as_deref_mut(),
            num_pages,
            use_fsm,
            &mut unlocked_target_buffer,
        )?;

        target_block = hio_seam::buffer_get_block_number::call(buffer)?;

        // The page is empty, pin vmbuffer to set all_frozen bit.  We don't want to
        // do IO while the buffer is locked, so we unlock the page first if IO is
        // needed (necessitating checks below).
        if (options & HEAP_INSERT_FROZEN) != 0 {
            debug_assert!(hio_seam::page_get_max_offset_number::call(buffer)? == 0);

            if !hio_seam::visibilitymap_pin_ok::call(target_block, *vmbuffer)? {
                if !unlocked_target_buffer {
                    hio_seam::lock_buffer::call(buffer, BUFFER_LOCK_UNLOCK)?;
                }
                unlocked_target_buffer = true;
                *vmbuffer = hio_seam::visibilitymap_pin::call(rel, target_block, *vmbuffer)?;
            }
        }

        // Reacquire locks if necessary.
        //
        // If the target buffer was unlocked above, or is unlocked while
        // reacquiring the lock on otherBuffer below, it's unlikely, but possible,
        // that another backend used space on this page.  We check for that below,
        // and retry if necessary.
        recheck_vm_pins = false;
        if unlocked_target_buffer {
            // released lock on target buffer above.
            if other_buffer != InvalidBuffer {
                hio_seam::lock_buffer::call(other_buffer, BUFFER_LOCK_EXCLUSIVE)?;
            }
            hio_seam::lock_buffer::call(buffer, BUFFER_LOCK_EXCLUSIVE)?;
            recheck_vm_pins = true;
        } else if other_buffer != InvalidBuffer {
            // We did not release the target buffer, and otherBuffer is valid, need
            // to lock the other buffer.  It's guaranteed to be of a lower page
            // number than the new page.  To conform with the deadlock prevent
            // rules, we ought to lock otherBuffer first, but that would give other
            // backends a chance to put tuples on our page.  To reduce the
            // likelihood of that, attempt to lock the other buffer conditionally,
            // that's very likely to work.
            debug_assert!(other_buffer != buffer);
            debug_assert!(target_block > other_block);

            if !hio_seam::conditional_lock_buffer::call(other_buffer)? {
                unlocked_target_buffer = true;
                hio_seam::lock_buffer::call(buffer, BUFFER_LOCK_UNLOCK)?;
                hio_seam::lock_buffer::call(other_buffer, BUFFER_LOCK_EXCLUSIVE)?;
                hio_seam::lock_buffer::call(buffer, BUFFER_LOCK_EXCLUSIVE)?;
            }
            recheck_vm_pins = true;
        }

        // If one of the buffers was unlocked (always the case if otherBuffer is
        // valid), it's possible, although unlikely, that an all-visible flag became
        // set.  We can use GetVisibilityMapPins to deal with that.  It's possible
        // that GetVisibilityMapPins() might need to temporarily release buffer
        // locks, in which case we'll need to check if there's still enough space on
        // the page below.
        if recheck_vm_pins
            && GetVisibilityMapPins(
                relation,
                other_buffer,
                buffer,
                other_block,
                target_block,
                vmbuffer_other,
                vmbuffer,
            )?
        {
            unlocked_target_buffer = true;
        }

        // If the target buffer was temporarily unlocked since the relation
        // extension, it's possible, although unlikely, that all the space on the
        // page was already used.  If so, we just retry from the start.  If we
        // didn't unlock, something has gone wrong if there's not enough space - the
        // test at the top should have prevented reaching this case.
        page_free_space = hio_seam::page_get_heap_free_space::call(buffer)?;
        if len > page_free_space {
            if unlocked_target_buffer {
                if other_buffer != InvalidBuffer {
                    hio_seam::lock_buffer::call(other_buffer, BUFFER_LOCK_UNLOCK)?;
                }
                hio_seam::unlock_release_buffer::call(buffer)?;

                continue 'outer; // goto loop;
            }
            return Err(ereport(PANIC)
                .errmsg_internal(format!("tuple is too big: size {}", len))
                .into_error());
        }

        // Remember the new page as our target for future insertions.
        hio_seam::relation_set_target_block::call(rel, target_block)?;

        return Ok(buffer);
    }
}

// ---------------------------------------------------------------------------
// Buffer / page seam installs
// ---------------------------------------------------------------------------
//
// The `hio.c` buffer/page seams declared in `backend-access-heap-hio-seams`
// are OUTWARD: their real bodies live in the buffer manager (`bufmgr.c`) and
// the opaque-`Page` predicates (`bufpage.c`). The buffer-KEYED slots (those
// crossing only a `Buffer` id, no relation handle) carry no contract
// divergence — they map 1:1 onto either the canonical `bufmgr-seams` body
// (`lock_buffer` / `MarkBufferDirty` / `ReleaseBuffer` / ...) or a `Page`
// predicate read over `with_buffer_page` (`PageGetMaxOffsetNumber` /
// `PageGetHeapFreeSpace` / `PageIsAllVisible` / `BufferGetPageSize`), so this
// crate installs them directly. The bufmgr delegates are wrapped to the
// hio-seam `PgResult` shape (the canonical buffer mutators are infallible).
//
// `page_add_item` is installed here too: the seam was re-signed to cross the
// tuple's full contiguous on-disk byte image (`image: &[u8]`), serialized by
// the caller from its `FormedTuple` — so the owner reconstructs the C `Item`
// from bytes and calls `PageAddItemExtended` over `with_buffer_page` (the prior
// header-only `HeapTupleData` carrier could not carry the user-data area; that
// FormedTuple-carrier keystone is resolved).
//
// Installed by the relcache owner (`backend-utils-cache-relcache/src/seams.rs`,
// not here): the relation-KEYED slots (`read_buffer` / `read_buffer_extended` /
// `extend_buffered_rel_by` / the four freespace slots / `visibilitymap_pin{,_ok}`
// / `relation_get_target_*` / `relation_set_target_block` /
// `relation_extension_lock_waiter_count` / `relation_is_local` /
// `relation_get_relation_name`). These cross the relation as a bare `Oid`; the
// relcache is the OID→`&Relation<'mcx>` owner, so it projects a transient read
// handle (`project_open`, its own `Mcx` arena) off the registry-owned entry and
// delegates to each canonical owner's `&Relation`-keyed seam — the prior
// "no Mcx-free by-Oid resolver" keystone is resolved there, not via a re-sign.

mod wire {
    use bufmgr_seams as bufmgr_seam;
    use page::{
        ItemIdGetOffset, PageAddItemExtended, PageGetHeapFreeSpace, PageGetItemId,
        PageGetMaxOffsetNumber, PageIsAllVisible, PageMut, PageRef,
    };
    use types_core::{BlockNumber, OffsetNumber, Size};
    use types_storage::bufpage::PAI_IS_HEAP;
    use types_tuple::heaptuple::ItemPointerData;
    use types_error::PgResult;
    use types_storage::storage::Buffer;

    pub fn lock_buffer(buffer: Buffer, mode: i32) -> PgResult<()> {
        bufmgr_seam::lock_buffer::call(buffer, mode)
    }

    pub fn conditional_lock_buffer(buffer: Buffer) -> PgResult<bool> {
        bufmgr_seam::conditional_lock_buffer::call(buffer)
    }

    pub fn mark_buffer_dirty(buffer: Buffer) -> PgResult<()> {
        bufmgr_seam::mark_buffer_dirty::call(buffer);
        Ok(())
    }

    pub fn release_buffer(buffer: Buffer) -> PgResult<()> {
        bufmgr_seam::release_buffer::call(buffer);
        Ok(())
    }

    pub fn unlock_release_buffer(buffer: Buffer) -> PgResult<()> {
        bufmgr_seam::unlock_release_buffer::call(buffer);
        Ok(())
    }

    pub fn incr_buffer_ref_count(buffer: Buffer) -> PgResult<()> {
        bufmgr_seam::incr_buffer_ref_count::call(buffer);
        Ok(())
    }

    pub fn buffer_get_block_number(buffer: Buffer) -> PgResult<BlockNumber> {
        Ok(bufmgr_seam::buffer_get_block_number::call(buffer))
    }

    pub fn page_init(buffer: Buffer) -> PgResult<()> {
        bufmgr_seam::page_init::call(buffer)
    }

    pub fn page_is_new(buffer: Buffer) -> PgResult<bool> {
        bufmgr_seam::page_is_new::call(buffer)
    }

    // The `Page` predicate reads: `BufferGetPage(buffer)` then the pure
    // page-format read. `with_buffer_page` hands the live page image; the
    // predicate never mutates it.
    pub fn buffer_get_page_size(buffer: Buffer) -> PgResult<Size> {
        // `BufferGetPageSize` (storage/bufmgr.h): unconditionally `BLCKSZ`. Unlike
        // `PageGetPageSize` (which reads `pd_pagesize_version` off the page header
        // and is only valid on a *formatted* page), this can be called on a raw,
        // not-yet-`PageInit`'d disk block — which is exactly what
        // `RelationAddBlocks` does for freshly-extended victim buffers. Reading the
        // header there would return 0 and underflow the `freespace =
        // BufferGetPageSize - SizeOfPageHeaderData` computation.
        debug_assert!(buffer != super::InvalidBuffer);
        Ok(types_core::primitive::BLCKSZ as Size)
    }

    pub fn page_get_max_offset_number(buffer: Buffer) -> PgResult<OffsetNumber> {
        let mut out: OffsetNumber = 0;
        bufmgr_seam::with_buffer_page::call(buffer, &mut |bytes| {
            out = PageGetMaxOffsetNumber(&PageRef::new(bytes)?);
            Ok(())
        })?;
        Ok(out)
    }

    pub fn page_get_heap_free_space(buffer: Buffer) -> PgResult<Size> {
        let mut out: Size = 0;
        bufmgr_seam::with_buffer_page::call(buffer, &mut |bytes| {
            out = PageGetHeapFreeSpace(&PageRef::new(bytes)?);
            Ok(())
        })?;
        Ok(out)
    }

    pub fn page_is_all_visible(buffer: Buffer) -> PgResult<bool> {
        let mut out = false;
        bufmgr_seam::with_buffer_page::call(buffer, &mut |bytes| {
            out = PageIsAllVisible(&PageRef::new(bytes)?);
            Ok(())
        })?;
        Ok(out)
    }

    /// `PageAddItem(BufferGetPage(buffer), (Item) tuple->t_data, tuple->t_len,
    /// InvalidOffsetNumber, false, true)` (bufpage.c) ==
    /// `PageAddItemExtended(page, item, size, InvalidOffsetNumber, PAI_IS_HEAP)`.
    /// `image` is the full contiguous on-disk tuple image. The owner holds the
    /// exclusive content lock; `with_buffer_page` hands the live mutable page.
    pub fn page_add_item(buffer: Buffer, image: &[u8]) -> PgResult<OffsetNumber> {
        use types_tuple::heaptuple::INVALID_OFFSET_NUMBER;
        let mut out: OffsetNumber = INVALID_OFFSET_NUMBER;
        bufmgr_seam::with_buffer_page::call(buffer, &mut |bytes| {
            let mut page = PageMut::new(bytes)?;
            out = PageAddItemExtended(&mut page, image, INVALID_OFFSET_NUMBER, PAI_IS_HEAP)?;
            Ok(())
        })?;
        Ok(out)
    }

    /// `((HeapTupleHeader) PageGetItem(page, PageGetItemId(page, offnum)))->t_ctid
    /// = ctid` (the `RelationPutHeapTuple` ctid stamp). `t_ctid` is the 6-byte
    /// field at offset 12 of the on-page heap-tuple header.
    pub fn set_stored_tuple_ctid(
        buffer: Buffer,
        offnum: OffsetNumber,
        ctid: ItemPointerData,
    ) -> PgResult<()> {
        // Offset of `t_ctid` within HeapTupleHeaderData: t_choice (12 bytes)
        // precedes it; t_ctid is BlockIdData(bi_hi u16, bi_lo u16) + ip_posid u16.
        const T_CTID_OFF: usize = 12;
        bufmgr_seam::with_buffer_page::call(buffer, &mut |bytes| {
            let item_off = {
                let page = PageRef::new(bytes)?;
                let item_id = PageGetItemId(&page, offnum)?;
                ItemIdGetOffset(&item_id) as usize
            };
            let base = item_off + T_CTID_OFF;
            bytes[base..base + 2].copy_from_slice(&ctid.ip_blkid.bi_hi.to_ne_bytes());
            bytes[base + 2..base + 4].copy_from_slice(&ctid.ip_blkid.bi_lo.to_ne_bytes());
            bytes[base + 4..base + 6].copy_from_slice(&ctid.ip_posid.to_ne_bytes());
            Ok(())
        })
    }
}

/// Install the buffer/page-keyed `hio.c` outward seams whose contract matches
/// their real owner with no divergence (the buffer-manager delegates and the
/// opaque-`Page` predicate reads). The relation-keyed slots and `page_add_item`
/// stay declared-and-panicking pending their re-sign keystones (see `mod wire`).
pub fn init_seams() {
    hio_seam::lock_buffer::set(wire::lock_buffer);
    hio_seam::conditional_lock_buffer::set(wire::conditional_lock_buffer);
    hio_seam::mark_buffer_dirty::set(wire::mark_buffer_dirty);
    hio_seam::release_buffer::set(wire::release_buffer);
    hio_seam::unlock_release_buffer::set(wire::unlock_release_buffer);
    hio_seam::incr_buffer_ref_count::set(wire::incr_buffer_ref_count);
    hio_seam::buffer_get_block_number::set(wire::buffer_get_block_number);
    hio_seam::page_init::set(wire::page_init);
    hio_seam::page_is_new::set(wire::page_is_new);
    hio_seam::buffer_get_page_size::set(wire::buffer_get_page_size);
    hio_seam::page_get_max_offset_number::set(wire::page_get_max_offset_number);
    hio_seam::page_get_heap_free_space::set(wire::page_get_heap_free_space);
    hio_seam::page_is_all_visible::set(wire::page_is_all_visible);
    hio_seam::page_add_item::set(wire::page_add_item);
    hio_seam::set_stored_tuple_ctid::set(wire::set_stored_tuple_ctid);
}
