// hio.c write lane. Deferred whole: BulkInsertState (multi_insert phase 3),
// HEAP_INSERT_FROZEN vm pinning and all-visible vm lanes (visibilitymap.c),
// multi-page bulk extension. The target-block cache (C rd_smgr->smgr_targblock)
// lives in a backend-local table until smgr wiring lands.
use std::cell::RefCell;

use ::bufmgr_seams::{BufferPin, BUFFER_LOCK_EXCLUSIVE, BUFFER_LOCK_UNLOCK, EB_LOCK_FIRST};
use ::types_core::{BlockNumber, ForkNumber, InvalidBlockNumber, Oid};
use ::types_error::{PgError, PgResult};
use ::types_rel::RelationData;
use ::types_storage::bufpage::{
    ItemIdData, MaxHeapTupleSize, MaxHeapTuplesPerPage, PageMut, PAI_IS_HEAP,
};
use ::types_tuple::{HeapTupleData, InvalidOffsetNumber, ItemPointerSet};

use crate::unported;

pub const HEAP_INSERT_SKIP_FSM: i32 = 0x0001;
pub const HEAP_INSERT_FROZEN: i32 = 0x0002;
pub const HEAP_INSERT_NO_LOGICAL: i32 = 0x0004;
pub const HEAP_INSERT_SPECULATIVE: i32 = 0x0010;

pub const HEAP_DEFAULT_FILLFACTOR: i32 = 100;

const MAXALIGN: usize = 8;

std::thread_local! {
    static TARGET_BLOCKS: RefCell<Vec<(Oid, BlockNumber)>> = const { RefCell::new(Vec::new()) };
}

pub fn relation_get_target_block(rel: &RelationData<'_>) -> BlockNumber {
    TARGET_BLOCKS.with(|t| {
        t.borrow()
            .iter()
            .find(|(oid, _)| *oid == rel.rd_id)
            .map_or(InvalidBlockNumber, |(_, b)| *b)
    })
}

pub fn relation_set_target_block(rel: &RelationData<'_>, blk: BlockNumber) {
    TARGET_BLOCKS.with(|t| {
        let mut v = t.borrow_mut();
        match v.iter_mut().find(|(oid, _)| *oid == rel.rd_id) {
            Some(slot) => slot.1 = blk,
            None => v.push((rel.rd_id, blk)),
        }
    })
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
/// C's lower-block-first rule.
pub fn RelationGetBufferForTuple<'mcx>(
    relation: &RelationData<'mcx>,
    len: usize,
    other_pin: Option<&BufferPin>,
    options: i32,
) -> PgResult<BufferPin> {
    let use_fsm = (options & HEAP_INSERT_SKIP_FSM) == 0;
    let len = (len + MAXALIGN - 1) & !(MAXALIGN - 1);

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

    let mut target_block = relation_get_target_block(relation);
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

    while target_block != InvalidBlockNumber {
        let pin = match other_block {
            None => {
                let pin = BufferPin::adopt(bufmgr_seams::read_buffer::call(
                    relation,
                    target_block,
                )?)
                .expect("ReadBuffer returned InvalidBuffer");
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
                bufmgr_seams::lock_buffer::call(other_pin.unwrap().buffer(), BUFFER_LOCK_UNLOCK)?;
                pin.release();
            }
        }

        if !use_fsm {
            break;
        }
        target_block = freespace_seams::record_and_get_page_with_free_space::call(
            relation,
            target_block,
            page_free_space,
            target_free_space,
        )?;
    }

    // Extension arm: single-page ExtendBufferedRelBy, EB_LOCK_FIRST leaves the
    // new buffer exclusively locked; bulk (num_pages > 1) lands with bistate.
    let (buffer, _extended_by) = bufmgr_seams::extend_buffered_rel_by::call(
        relation,
        ForkNumber::MAIN_FORKNUM,
        None,
        EB_LOCK_FIRST,
        1,
    )?;
    let pin = BufferPin::adopt(buffer).expect("ExtendBufferedRelBy returned InvalidBuffer");
    let target_block = pin.block_number();

    if let Some(op) = other_pin {
        debug_assert!(op.block_number() < target_block);
        // C tries ConditionalLockBuffer first to avoid reopening the window;
        // the unconditional order (unlock new, lock old, relock new) is the
        // fallback path and is always deadlock-safe.
        bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_UNLOCK)?;
        bufmgr_seams::lock_buffer::call(op.buffer(), BUFFER_LOCK_EXCLUSIVE)?;
        bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_EXCLUSIVE)?;
    }

    // SAFETY: pinned + exclusively locked.
    let mut page = unsafe { PageMut::from_raw(bufmgr_seams::buffer_get_page::call(pin.buffer())) };
    if page.as_ref().is_new() {
        page.init(0);
        bufmgr_seams::mark_buffer_dirty::call(pin.buffer())?;
    }
    let page_free_space = page.as_ref().heap_free_space();
    if len > page_free_space {
        panic!("tuple is too big: size {len}");
    }

    relation_set_target_block(relation, target_block);
    Ok(pin)
}
