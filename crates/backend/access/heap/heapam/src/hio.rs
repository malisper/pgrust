// hio.c write lane. Deferred: HEAP_INSERT_FROZEN vm pinning and all-visible
// vm lanes (visibilitymap.c). The target-block cache (C rd_smgr->smgr_targblock)
// lives in a backend-local table until smgr wiring lands.
use std::cell::RefCell;

use ::bufmgr_seams::{BufferPin, BUFFER_LOCK_EXCLUSIVE, BUFFER_LOCK_UNLOCK, EB_LOCK_FIRST};
use ::tableam_vocab::BulkInsertStateData;
use ::types_core::{BlockNumber, ForkNumber, InvalidBlockNumber, Oid, BLCKSZ};
use ::types_error::{PgError, PgResult};
use ::types_rel::RelationData;
use ::types_storage::buf::BufferAccessStrategyType;
use ::types_storage::bufpage::{
    ItemIdData, MaxHeapTupleSize, MaxHeapTuplesPerPage, PageMut, SizeOfPageHeaderData, PAI_IS_HEAP,
};
use ::types_tuple::{HeapTupleData, InvalidOffsetNumber, ItemPointerSet};

use crate::unported;

// Aliases of TABLE_INSERT_* (heapam.h): options pass through tableam unmapped.
pub const HEAP_INSERT_SKIP_FSM: i32 = 0x0002;
pub const HEAP_INSERT_FROZEN: i32 = 0x0004;
pub const HEAP_INSERT_NO_LOGICAL: i32 = 0x0008;
pub const HEAP_INSERT_SPECULATIVE: i32 = 0x0010;

pub const HEAP_DEFAULT_FILLFACTOR: i32 = 100;

const MAXALIGN: usize = 8;
const MAX_BUFFERS_TO_EXTEND_BY: u32 = 64;

pub use ::bufmgr_seams::targblock::{relation_get_target_block, relation_set_target_block};

/// `GetBulkInsertState` (heapam.c); drop is `FreeBulkInsertState` (the pin and
/// the strategy ring release through their own owners).
pub fn GetBulkInsertState() -> BulkInsertStateData {
    BulkInsertStateData {
        strategy: bufmgr_seams::get_access_strategy::call(BufferAccessStrategyType::BasBulkwrite),
        current_buf: None,
        next_free: InvalidBlockNumber,
        last_free: InvalidBlockNumber,
        already_extended_by: 0,
    }
}

/// `ReleaseBulkInsertStatePin` (heapam.c).
pub fn ReleaseBulkInsertStatePin(bistate: &mut BulkInsertStateData) {
    bistate.current_buf = None;
    bistate.next_free = InvalidBlockNumber;
    bistate.last_free = InvalidBlockNumber;
}

// ReadBufferBI (hio.c): with a bistate, re-pin its current buffer when it is
// the target, else read with the bulk-write strategy and cache the pin.
fn ReadBufferBI(
    relation: &RelationData<'_>,
    target_block: BlockNumber,
    bistate: Option<&mut &mut BulkInsertStateData>,
) -> PgResult<BufferPin> {
    let Some(bistate) = bistate else {
        let pin = BufferPin::adopt(bufmgr_seams::read_buffer::call(relation, target_block)?)
            .expect("ReadBuffer returned InvalidBuffer");
        return Ok(pin);
    };
    if let Some(cur) = &bistate.current_buf {
        if cur.block_number() == target_block {
            return Ok(cur.incr_clone());
        }
    }
    bistate.current_buf = None;
    let pin = BufferPin::adopt(bufmgr_seams::read_buffer_strategy::call(
        relation,
        target_block,
        bistate.strategy.clone(),
    )?)
    .expect("ReadBuffer returned InvalidBuffer");
    bistate.current_buf = Some(pin.incr_clone());
    Ok(pin)
}

// RelationAddBlocks (hio.c): bulk extension; returns the first new buffer
// (pinned, exclusive-locked unless did_unlock) plus did_unlock.
fn RelationAddBlocks(
    relation: &RelationData<'_>,
    bistate: &mut Option<&mut BulkInsertStateData>,
    num_pages: i32,
    use_fsm: bool,
) -> PgResult<(BufferPin, bool)> {
    let extend_by_pages = if bistate.is_none() && !use_fsm {
        1
    } else {
        // Single-backend: no extension-lock waiters, so no waitcount term.
        let mut pages = num_pages as u32;
        if let Some(bi) = bistate.as_deref() {
            pages = pages.max(bi.already_extended_by);
        }
        pages.min(MAX_BUFFERS_TO_EXTEND_BY)
    };
    let not_in_fsm_pages: u32 = if num_pages > 1 && bistate.is_none() {
        1
    } else {
        num_pages as u32
    };

    if let Some(bi) = bistate.as_deref_mut() {
        bi.current_buf = None;
    }

    let (buffer, extended_by) = bufmgr_seams::extend_buffered_rel_by::call(
        relation,
        ForkNumber::MAIN_FORKNUM,
        bistate.as_deref().and_then(|bi| bi.strategy.clone()),
        EB_LOCK_FIRST,
        extend_by_pages,
    )?;
    let pin = BufferPin::adopt(buffer).expect("ExtendBufferedRelBy returned InvalidBuffer");
    let first_block = pin.block_number();
    let last_block = first_block + (extended_by - 1);

    // SAFETY: pinned + exclusively locked (EB_LOCK_FIRST).
    let mut page = unsafe { PageMut::from_raw(bufmgr_seams::buffer_get_page::call(pin.buffer())) };
    if !page.as_ref().is_new() {
        panic!("page {first_block} of relation \"{}\" should be empty but is not", relation.name());
    }
    page.init(0);
    bufmgr_seams::mark_buffer_dirty::call(pin.buffer())?;

    let did_unlock = use_fsm && not_in_fsm_pages < extended_by;
    if did_unlock {
        bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_UNLOCK)?;
    }

    // Extra buffers' pins were dropped inside the seam impl; only the FSM
    // recording remains (block numbers suffice).
    for i in 1..extended_by {
        if use_fsm && i >= not_in_fsm_pages {
            freespace_seams::record_page_with_free_space::call(
                relation,
                first_block + i,
                BLCKSZ - SizeOfPageHeaderData,
            )?;
        }
    }
    if use_fsm && not_in_fsm_pages < extended_by {
        freespace_seams::free_space_map_vacuum_range::call(
            relation,
            first_block + not_in_fsm_pages,
            last_block,
        )?;
    }

    if let Some(bi) = bistate.as_deref_mut() {
        if extended_by > 1 {
            bi.next_free = first_block + 1;
            bi.last_free = last_block;
        } else {
            bi.next_free = InvalidBlockNumber;
            bi.last_free = InvalidBlockNumber;
        }
        bi.current_buf = Some(pin.incr_clone());
        bi.already_extended_by += extended_by;
    }

    Ok((pin, did_unlock))
}

/// `RelationPutHeapTuple` (hio.c): caller holds the exclusive content lock.
pub fn RelationPutHeapTuple(
    _relation: &RelationData<'_>,
    pin: &BufferPin,
    tuple: &mut HeapTupleData<'_>,
    token: bool,
) -> PgResult<()> {
    debug_assert!(!token || tuple.t_data().is_speculative());
    debug_assert!(
        !((tuple.t_data().t_infomask & ::types_tuple::HEAP_XMAX_COMMITTED) != 0
            && (tuple.t_data().t_infomask & ::types_tuple::HEAP_XMAX_IS_MULTI) != 0)
    );

    // SAFETY: pinned + exclusively locked by the caller (C contract).
    let mut page = unsafe { PageMut::from_raw(bufmgr_seams::buffer_get_page::call(pin.buffer())) };
    let image = unsafe {
        // SAFETY: tuple image is t_len readable bytes (HeapTupleData contract).
        core::slice::from_raw_parts(tuple.header_ptr(), tuple.t_len as usize)
    };
    let offnum = page
        .add_item(image, InvalidOffsetNumber, PAI_IS_HEAP)
        .unwrap_or_else(|| panic!("failed to add tuple to page"));

    ItemPointerSet(&mut tuple.t_self, pin.block_number(), offnum);

    if !token {
        let r = page.as_ref();
        let id: ItemIdData = r.item_id(offnum);
        let (ptr, len) = r.item_raw(id);
        // SAFETY: stored image just written at offnum; header-sized prefix.
        let mut item = unsafe {
            HeapTupleData::from_raw_parts(ptr, len, tuple.t_self, tuple.t_tableOid)
        };
        item.t_data_mut().t_ctid = tuple.t_self;
    }
    Ok(())
}

#[cold]
#[inline(never)]
fn row_too_big(len: usize) -> Box<PgError> {
    Box::new(
        PgError::error(std::format!(
            "row is too big: size {len}, maximum size {MaxHeapTupleSize}"
        ))
        .with_sqlstate(::types_error::ERRCODE_PROGRAM_LIMIT_EXCEEDED),
    )
}

/// `RelationGetBufferForTuple` (hio.c): returns the pin with the exclusive
/// content lock held; `other_pin` (heap_update's old page) is relocked per
/// C's lower-block-first rule. `num_pages` is the extension hint from
/// heap_multi_insert; <= 0 means 1.
pub fn RelationGetBufferForTuple<'mcx>(
    relation: &RelationData<'mcx>,
    len: usize,
    other_pin: Option<&BufferPin>,
    options: i32,
    mut bistate: Option<&mut BulkInsertStateData>,
    num_pages: i32,
) -> PgResult<BufferPin> {
    let use_fsm = (options & HEAP_INSERT_SKIP_FSM) == 0;
    let len = (len + MAXALIGN - 1) & !(MAXALIGN - 1);
    let num_pages = num_pages.max(1);

    debug_assert!(other_pin.is_none() || bistate.is_none());

    if len > MaxHeapTupleSize {
        return Err(row_too_big(len));
    }
    if (options & HEAP_INSERT_FROZEN) != 0 {
        unported("visibilitymap_pin (HEAP_INSERT_FROZEN lane, visibilitymap.c)");
    }

    let save_free_space = relation.get_target_page_free_space(HEAP_DEFAULT_FILLFACTOR) as usize;
    let nearly_empty_free_space =
        MaxHeapTupleSize - (MaxHeapTuplesPerPage / 8 * core::mem::size_of::<ItemIdData>());
    let target_free_space = if len + save_free_space > nearly_empty_free_space {
        len.max(nearly_empty_free_space)
    } else {
        len + save_free_space
    };

    let other_block = other_pin.map(|p| p.block_number());

    let mut target_block = match bistate.as_deref().and_then(|bi| bi.current_buf.as_ref()) {
        Some(cur) => cur.block_number(),
        None => relation_get_target_block(relation),
    };
    if target_block == InvalidBlockNumber && use_fsm {
        target_block = freespace_seams::get_page_with_free_space::call(relation, target_free_space)?;
    }
    if target_block == InvalidBlockNumber {
        let nblocks = bufmgr_seams::relation_get_number_of_blocks_in_fork::call(
            relation,
            ForkNumber::MAIN_FORKNUM,
        )?;
        if nblocks > 0 {
            target_block = nblocks - 1;
        }
    }

    loop {
        while target_block != InvalidBlockNumber {
            let pin = match other_block {
                None => {
                    let pin = ReadBufferBI(relation, target_block, bistate.as_mut())?;
                    bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_EXCLUSIVE)?;
                    pin
                }
                Some(ob) if ob == target_block => {
                    let pin = other_pin.unwrap().incr_clone();
                    bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_EXCLUSIVE)?;
                    pin
                }
                Some(ob) => {
                    let pin = BufferPin::adopt(bufmgr_seams::read_buffer::call(
                        relation,
                        target_block,
                    )?)
                    .expect("ReadBuffer returned InvalidBuffer");
                    if ob < target_block {
                        bufmgr_seams::lock_buffer::call(
                            other_pin.unwrap().buffer(),
                            BUFFER_LOCK_EXCLUSIVE,
                        )?;
                        bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_EXCLUSIVE)?;
                    } else {
                        bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_EXCLUSIVE)?;
                        bufmgr_seams::lock_buffer::call(
                            other_pin.unwrap().buffer(),
                            BUFFER_LOCK_EXCLUSIVE,
                        )?;
                    }
                    pin
                }
            };

            // SAFETY: pinned + exclusively locked just above.
            let mut page =
                unsafe { PageMut::from_raw(bufmgr_seams::buffer_get_page::call(pin.buffer())) };
            if page.as_ref().is_new() {
                page.init(0);
                bufmgr_seams::mark_buffer_dirty::call(pin.buffer())?;
            }

            let page_free_space = page.as_ref().heap_free_space();
            if target_free_space <= page_free_space {
                relation_set_target_block(relation, target_block);
                return Ok(pin);
            }

            bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_UNLOCK)?;
            match other_block {
                None => pin.release(),
                Some(ob) if ob == target_block => pin.release(),
                Some(_) => {
                    bufmgr_seams::lock_buffer::call(
                        other_pin.unwrap().buffer(),
                        BUFFER_LOCK_UNLOCK,
                    )?;
                    pin.release();
                }
            }

            let next_free = bistate.as_deref().map_or(InvalidBlockNumber, |bi| bi.next_free);
            if next_free != InvalidBlockNumber {
                let bi = bistate.as_deref_mut().unwrap();
                debug_assert!(bi.next_free <= bi.last_free);
                if use_fsm {
                    freespace_seams::record_page_with_free_space::call(
                        relation,
                        target_block,
                        page_free_space,
                    )?;
                }
                target_block = bi.next_free;
                if bi.next_free >= bi.last_free {
                    bi.next_free = InvalidBlockNumber;
                    bi.last_free = InvalidBlockNumber;
                } else {
                    bi.next_free += 1;
                }
            } else if !use_fsm {
                break;
            } else {
                target_block = freespace_seams::record_and_get_page_with_free_space::call(
                    relation,
                    target_block,
                    page_free_space,
                    target_free_space,
                )?;
            }
        }

        let (pin, mut unlocked_target) =
            RelationAddBlocks(relation, &mut bistate, num_pages, use_fsm)?;
        target_block = pin.block_number();

        if unlocked_target {
            if let Some(op) = other_pin {
                bufmgr_seams::lock_buffer::call(op.buffer(), BUFFER_LOCK_EXCLUSIVE)?;
            }
            bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_EXCLUSIVE)?;
        } else if let Some(op) = other_pin {
            debug_assert!(op.block_number() < target_block);
            // C tries ConditionalLockBuffer first to avoid reopening the
            // window; the unconditional order (unlock new, lock old, relock
            // new) is the fallback path and is always deadlock-safe.
            bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_UNLOCK)?;
            bufmgr_seams::lock_buffer::call(op.buffer(), BUFFER_LOCK_EXCLUSIVE)?;
            bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_EXCLUSIVE)?;
            unlocked_target = true;
        }

        let page_free_space = pin.page().heap_free_space();
        if len > page_free_space {
            if unlocked_target {
                bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_UNLOCK)?;
                if let Some(op) = other_pin {
                    bufmgr_seams::lock_buffer::call(op.buffer(), BUFFER_LOCK_UNLOCK)?;
                }
                pin.release();
                continue;
            }
            panic!("tuple is too big: size {len}");
        }

        relation_set_target_block(relation, target_block);
        return Ok(pin);
    }
}
