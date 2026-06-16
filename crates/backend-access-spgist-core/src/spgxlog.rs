//! WAL replay logic for SP-GiST (`access/spgist/spgxlog.c`): the `spg_redo`
//! resource-manager callback and its per-op handlers
//! (`spgRedoAddLeaf`/`spgRedoMoveLeafs`/`spgRedoAddNode`/`spgRedoSplitTuple`/
//! `spgRedoPickSplit`/`spgRedoVacuumLeaf`/`spgRedoVacuumRoot`/
//! `spgRedoVacuumRedirect`), plus `spg_xlog_startup`/`spg_xlog_cleanup`/
//! `spg_mask`.
//!
//! Lives in `backend-access-spgist-core` (the unit that owns `spgxlog.c` per
//! the CATALOG row): the per-op handlers reuse the crate's own page/tuple byte
//! primitives (`spgPageIndexMultiDelete`, `spgUpdateNodeLink`,
//! `spgFormDeadTuple`, `SpGistInitBuffer`/`SpGistInitPage`, `swap_item_ids`, the
//! opaque-count accessors, and the leaf/inner/dead header readers), so no logic
//! leaks across a seam.
//!
//! Pages are read through `with_buffer_page` (a copy-in/copy-out byte-mutate
//! closure) and the LSN is stamped through the bufmgr `page_set_lsn` seam after
//! the edit — the same idiom as `spgdoinsert` / `spgvacuum`. `opCtx`
//! (C `static MemoryContext`) is a thread-local recovery context created by
//! `spg_xlog_startup`, reset after each redo, deleted by `spg_xlog_cleanup`.
//!
//! No raw pointers, no `extern "C"`.

extern crate alloc;

use alloc::format;
use alloc::vec::Vec;

use core::cell::RefCell;

use backend_access_common_bufmask_seams::{
    mask_page_hint_bits, mask_page_lsn_and_checksum, mask_unused_space,
};
use backend_access_transam_xlogreader_seams as reader_seam;
use backend_access_transam_xlogutils::{XLogInitBufferForRedo, XLogReadBufferForRedo};
use backend_access_transam_xlogutils_seams as xlogutils_seam;
use backend_storage_buffer_bufmgr_seams as bufmgr;
use backend_storage_buffer_bufmgr_seams::{mark_buffer_dirty, unlock_release_buffer, with_buffer_page};
use backend_storage_ipc_standby_seams as standby;
use backend_storage_page::{
    PageAddItemExtended, PageGetItemId, PageGetMaxOffsetNumber, PageIndexMultiDelete,
    PageIndexTupleDelete, PageMut, PageRef,
};

use mcx::{Mcx, MemoryContext};
use types_core::primitive::{BlockNumber, InvalidBlockNumber, OffsetNumber};
use types_storage::Buffer;
use types_error::error::PANIC;
use types_error::{PgError, PgResult};
use types_spgist::{
    SpGistState, SpGistTypeDesc, spgConfigOut, SPGIST_DEAD, SPGIST_LEAF, SPGIST_LIVE, SPGIST_NULLS,
    SPGIST_PLACEHOLDER, SPGIST_REDIRECT,
};
use types_storage::bufpage::SizeOfPageHeaderData;
use types_wal::rmgr::XLogReaderState;
use types_wal::xlogutils::in_hot_standby;
use types_wal::XLogRedoAction;
use types_xlog_records::spgxlog::{
    spgxlogAddLeaf, spgxlogAddNode, spgxlogMoveLeafs, spgxlogPickSplit, spgxlogSplitTuple,
    spgxlogState, spgxlogVacuumLeaf, spgxlogVacuumRedirect, spgxlogVacuumRoot,
    SIZE_OF_SPGXLOG_ADD_LEAF, SIZE_OF_SPGXLOG_ADD_NODE, SIZE_OF_SPGXLOG_MOVE_LEAFS,
    SIZE_OF_SPGXLOG_PICK_SPLIT, SIZE_OF_SPGXLOG_SPLIT_TUPLE, SIZE_OF_SPGXLOG_VACUUM_LEAF,
    SIZE_OF_SPGXLOG_VACUUM_REDIRECT, SIZE_OF_SPGXLOG_VACUUM_ROOT, SPGXLOG_ADD_NODE_STATE_OFF,
    SPGXLOG_MOVE_LEAFS_STATE_OFF, SPGXLOG_PICK_SPLIT_STATE_OFF, SPGXLOG_VACUUM_LEAF_STATE_OFF,
};

use crate::spgdoinsert::{
    elog_error, it_size, lt_get_next_offset, lt_set_next_offset, lt_size, lt_tupstate,
    opaque_n_redirection, set_opaque_n_redirection, spgPageIndexMultiDelete, spgUpdateNodeLink,
};
use crate::spgvacuum::{dt_set_pointer_invalid, dt_set_tupstate, dt_tupstate, swap_item_ids};
use crate::{
    opaque_n_placeholder, set_opaque_n_placeholder, spgFormDeadTuple, SpGistInitBuffer,
    SpGistInitPage, SGDTSIZE,
};

// ===========================================================================
// `access/spgist_private.h` — XLOG record info codes (the `xl_info` low byte).
// ===========================================================================

const XLOG_SPGIST_ADD_LEAF: u8 = 0x10;
const XLOG_SPGIST_MOVE_LEAFS: u8 = 0x20;
const XLOG_SPGIST_ADD_NODE: u8 = 0x30;
const XLOG_SPGIST_SPLIT_TUPLE: u8 = 0x40;
const XLOG_SPGIST_PICKSPLIT: u8 = 0x50;
const XLOG_SPGIST_VACUUM_LEAF: u8 = 0x60;
const XLOG_SPGIST_VACUUM_ROOT: u8 = 0x70;
const XLOG_SPGIST_VACUUM_REDIRECT: u8 = 0x80;

/// `XLR_INFO_MASK` (`access/xlogrecord.h`) — the framework bits of `xl_info`,
/// masked off to obtain the AM op code.
const XLR_INFO_MASK: u8 = 0x0F;

/// `InvalidOffsetNumber` (`storage/off.h`).
const InvalidOffsetNumber: OffsetNumber = 0;

// ===========================================================================
// `opCtx` — the C `static MemoryContext opCtx` recovery working context.
// ===========================================================================

thread_local! {
    /// `static MemoryContext opCtx` (spgxlog.c) — working memory for redo
    /// operations, created at recovery startup and deleted at cleanup.
    static OP_CTX: RefCell<Option<MemoryContext>> = const { RefCell::new(None) };
}

/// Run `f` with an [`Mcx`] borrowed from `opCtx` (mirrors C's
/// `MemoryContextSwitchTo(opCtx)`). Panics if `opCtx` was never created, which
/// in C would be a NULL-deref — the rmgr only calls redo between
/// `spg_xlog_startup` and `spg_xlog_cleanup`.
fn with_op_ctx<R>(f: impl FnOnce(Mcx<'_>) -> R) -> R {
    OP_CTX.with(|c| {
        let borrow = c.borrow();
        let ctx = borrow
            .as_ref()
            .expect("spg_redo called without spg_xlog_startup (opCtx is NULL)");
        f(ctx.mcx())
    })
}

// ===========================================================================
// Decoded-record accessors (read off `record.record`, owned by xlogreader).
// ===========================================================================

/// `XLogRecGetData(record)` — the record's main data.
fn record_get_data<'a>(record: &'a XLogReaderState<'_>) -> &'a [u8] {
    record.record.as_ref().map(|r| r.data()).unwrap_or(&[])
}

/// `XLogRecGetInfo(record)` — the raw `xl_info` byte.
fn record_get_info(record: &XLogReaderState<'_>) -> u8 {
    record.record.as_ref().map(|r| r.info()).unwrap_or(0)
}

/// `XLogRecGetBlockTag(record, block_id, ..., &blknum)` — the block number a
/// referenced block lives at.
fn block_tag_blkno(record: &XLogReaderState<'_>, block_id: u8) -> PgResult<BlockNumber> {
    match reader_seam::xlog_rec_get_block_tag_extended::call(record, block_id)? {
        Some(tag) => Ok(tag.blkno),
        None => Err(PgError::new(
            PANIC,
            format!("failed to locate backup block with ID {block_id} in WAL record"),
        )),
    }
}

/// `XLogRecHasBlockRef(record, block_id)` (xlogreader.h).
fn has_block_ref(record: &XLogReaderState<'_>, block_id: u8) -> PgResult<bool> {
    Ok(reader_seam::xlog_rec_get_block_tag_extended::call(record, block_id)?.is_some())
}

/// `BufferIsValid(buffer)` — a valid buffer is non-zero (`InvalidBuffer == 0`).
fn buffer_is_valid(buffer: Buffer) -> bool {
    buffer != 0
}

// ===========================================================================
// fillFakeState (spgxlog.c:34)
// ===========================================================================

/// `fillFakeState(state, stateSrc)` (spgxlog.c:34) — prepare a dummy
/// `SpGistState` with just the minimum info needed for replay (enough to
/// support `spgFormDeadTuple`, plus the `isBuild` flag). `memset(0)` plus
/// `palloc0(SGDTSIZE)` for the dead-tuple workspace; `leafTupDesc` stays NULL
/// (`None`).
fn fillFakeState<'mcx>(state_src: spgxlogState) -> SpGistState<'mcx> {
    SpGistState {
        index: 0,
        config: spgConfigOut::default(),
        attType: SpGistTypeDesc::default(),
        attLeafType: SpGistTypeDesc::default(),
        attPrefixType: SpGistTypeDesc::default(),
        attLabelType: SpGistTypeDesc::default(),
        leafTupDesc: None,
        deadTupleStorage: Some(alloc::vec![0u8; SGDTSIZE]),
        redirectXid: state_src.redirectXid,
        isBuild: state_src.isBuild,
    }
}

// ===========================================================================
// addOrReplaceTuple (spgxlog.c:49)
// ===========================================================================

/// `addOrReplaceTuple(page, tuple, size, offset)` (spgxlog.c:49) — add a leaf
/// tuple, or replace an existing placeholder tuple. Used to replay
/// `SpGistPageAddNewItem()` operations.
fn addOrReplaceTuple(page: &mut [u8], tuple: &[u8], offset: OffsetNumber) -> PgResult<()> {
    let max = PageGetMaxOffsetNumber(&PageRef::new(page)?);
    if offset <= max {
        let it_off = {
            let pr = PageRef::new(page)?;
            PageGetItemId(&pr, offset)?.lp_off() as usize
        };
        if dt_tupstate(&page[it_off..]) != SPGIST_PLACEHOLDER {
            return Err(elog_error(
                "SPGiST tuple to be replaced is not a placeholder".into(),
            ));
        }

        debug_assert!(opaque_n_placeholder(page) > 0);
        set_opaque_n_placeholder(page, opaque_n_placeholder(page) - 1);

        let mut pm = PageMut::new(page)?;
        PageIndexTupleDelete(&mut pm, offset)?;
    }

    debug_assert!(offset <= PageGetMaxOffsetNumber(&PageRef::new(page)?) + 1);

    let added = {
        let mut pm = PageMut::new(page)?;
        PageAddItemExtended(&mut pm, tuple, offset, 0)?
    };
    if added != offset {
        return Err(elog_error(format!(
            "failed to add item of size {} to SPGiST index page",
            tuple.len()
        )));
    }
    Ok(())
}

// ===========================================================================
// spgRedoAddLeaf (spgxlog.c:73)
// ===========================================================================

fn spgRedoAddLeaf(record: &XLogReaderState<'_>) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let ptr = record_get_data(record);
    let xldata = spgxlogAddLeaf::from_bytes(ptr);

    let leaf_tuple = &ptr[SIZE_OF_SPGXLOG_ADD_LEAF..];
    let leaf_size = lt_size(leaf_tuple);
    let leaf_tuple = &leaf_tuple[..leaf_size];

    // In WAL replay we may update the leaf page before the parent.
    let (action, buffer) = if xldata.newPage {
        let buffer = XLogInitBufferForRedo(record, 0)?;
        SpGistInitBuffer(
            buffer,
            SPGIST_LEAF | (if xldata.storesNulls { SPGIST_NULLS } else { 0 }),
        )?;
        (XLogRedoAction::BlkNeedsRedo, buffer)
    } else {
        XLogReadBufferForRedo(record, 0)?
    };

    if action == XLogRedoAction::BlkNeedsRedo {
        with_buffer_page::call(buffer, &mut |page: &mut [u8]| {
            if xldata.offnumLeaf != xldata.offnumHeadLeaf {
                addOrReplaceTuple(page, leaf_tuple, xldata.offnumLeaf)?;

                if xldata.offnumHeadLeaf != InvalidOffsetNumber {
                    let head_off = {
                        let pr = PageRef::new(page)?;
                        PageGetItemId(&pr, xldata.offnumHeadLeaf)?.lp_off() as usize
                    };
                    debug_assert_eq!(
                        lt_get_next_offset(&page[head_off..]),
                        lt_get_next_offset(leaf_tuple)
                    );
                    lt_set_next_offset(&mut page[head_off..], xldata.offnumLeaf);
                }
            } else {
                // replacing a DEAD tuple
                {
                    let mut pm = PageMut::new(page)?;
                    PageIndexTupleDelete(&mut pm, xldata.offnumLeaf)?;
                }
                let added = {
                    let mut pm = PageMut::new(page)?;
                    PageAddItemExtended(&mut pm, leaf_tuple, xldata.offnumLeaf, 0)?
                };
                if added != xldata.offnumLeaf {
                    return Err(elog_error(format!(
                        "failed to add item of size {leaf_size} to SPGiST index page"
                    )));
                }
            }
            Ok(())
        })?;
        bufmgr::page_set_lsn::call(buffer, lsn)?;
        mark_buffer_dirty::call(buffer);
    }
    if buffer_is_valid(buffer) {
        unlock_release_buffer::call(buffer);
    }

    // update parent downlink if necessary
    if xldata.offnumParent != InvalidOffsetNumber {
        let (paction, pbuffer) = XLogReadBufferForRedo(record, 1)?;
        if paction == XLogRedoAction::BlkNeedsRedo {
            let blkno_leaf = block_tag_blkno(record, 0)?;
            with_buffer_page::call(pbuffer, &mut |page: &mut [u8]| {
                let it_off = {
                    let pr = PageRef::new(page)?;
                    PageGetItemId(&pr, xldata.offnumParent)?.lp_off() as usize
                };
                spgUpdateNodeLink(
                    &mut page[it_off..],
                    xldata.nodeI as i32,
                    blkno_leaf,
                    xldata.offnumLeaf,
                )
            })?;
            bufmgr::page_set_lsn::call(pbuffer, lsn)?;
            mark_buffer_dirty::call(pbuffer);
        }
        if buffer_is_valid(pbuffer) {
            unlock_release_buffer::call(pbuffer);
        }
    }
    Ok(())
}

// ===========================================================================
// spgRedoMoveLeafs (spgxlog.c:170)
// ===========================================================================

fn spgRedoMoveLeafs(record: &XLogReaderState<'_>) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let ptr = record_get_data(record);
    let xldata = spgxlogMoveLeafs::from_bytes(ptr);

    let blkno_dst = block_tag_blkno(record, 1)?;

    let mut state = fillFakeState(spgxlogState::from_bytes(ptr, SPGXLOG_MOVE_LEAFS_STATE_OFF));

    let n_insert = if xldata.replaceDead {
        1
    } else {
        xldata.nMoves as usize + 1
    };

    let mut off = SIZE_OF_SPGXLOG_MOVE_LEAFS;
    let to_delete = read_offsets(ptr, off, xldata.nMoves as usize);
    off += 2 * xldata.nMoves as usize;
    let to_insert = read_offsets(ptr, off, n_insert);
    off += 2 * n_insert;
    let tuples_off = off; // now ptr points to the list of leaf tuples

    // Insert tuples on the dest page (do first, so redirect is valid).
    let (action, buffer) = if xldata.newPage {
        let buffer = XLogInitBufferForRedo(record, 1)?;
        SpGistInitBuffer(
            buffer,
            SPGIST_LEAF | (if xldata.storesNulls { SPGIST_NULLS } else { 0 }),
        )?;
        (XLogRedoAction::BlkNeedsRedo, buffer)
    } else {
        XLogReadBufferForRedo(record, 1)?
    };

    if action == XLogRedoAction::BlkNeedsRedo {
        with_buffer_page::call(buffer, &mut |page: &mut [u8]| {
            let mut cur = tuples_off;
            for &ins in to_insert.iter() {
                let sz = lt_size(&ptr[cur..]);
                addOrReplaceTuple(page, &ptr[cur..cur + sz], ins)?;
                cur += sz;
            }
            Ok(())
        })?;
        bufmgr::page_set_lsn::call(buffer, lsn)?;
        mark_buffer_dirty::call(buffer);
    }
    if buffer_is_valid(buffer) {
        unlock_release_buffer::call(buffer);
    }

    // Delete tuples from the source page, inserting a redirection pointer.
    let (saction, sbuffer) = XLogReadBufferForRedo(record, 0)?;
    if saction == XLogRedoAction::BlkNeedsRedo {
        let firststate = if state.isBuild {
            SPGIST_PLACEHOLDER
        } else {
            SPGIST_REDIRECT
        };
        with_buffer_page::call(sbuffer, &mut |page: &mut [u8]| {
            spgPageIndexMultiDelete(
                &mut state,
                page,
                &to_delete,
                firststate,
                SPGIST_PLACEHOLDER,
                blkno_dst,
                to_insert[n_insert - 1],
            )
        })?;
        bufmgr::page_set_lsn::call(sbuffer, lsn)?;
        mark_buffer_dirty::call(sbuffer);
    }
    if buffer_is_valid(sbuffer) {
        unlock_release_buffer::call(sbuffer);
    }

    // And update the parent downlink.
    let (paction, pbuffer) = XLogReadBufferForRedo(record, 2)?;
    if paction == XLogRedoAction::BlkNeedsRedo {
        with_buffer_page::call(pbuffer, &mut |page: &mut [u8]| {
            let it_off = {
                let pr = PageRef::new(page)?;
                PageGetItemId(&pr, xldata.offnumParent)?.lp_off() as usize
            };
            spgUpdateNodeLink(
                &mut page[it_off..],
                xldata.nodeI as i32,
                blkno_dst,
                to_insert[n_insert - 1],
            )
        })?;
        bufmgr::page_set_lsn::call(pbuffer, lsn)?;
        mark_buffer_dirty::call(pbuffer);
    }
    if buffer_is_valid(pbuffer) {
        unlock_release_buffer::call(pbuffer);
    }
    Ok(())
}

// ===========================================================================
// spgRedoAddNode (spgxlog.c:283)
// ===========================================================================

fn spgRedoAddNode(record: &XLogReaderState<'_>) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let ptr = record_get_data(record);
    let xldata = spgxlogAddNode::from_bytes(ptr);

    let inner_tuple = &ptr[SIZE_OF_SPGXLOG_ADD_NODE..];
    let inner_size = it_size(inner_tuple);
    let inner_tuple = &inner_tuple[..inner_size];

    let mut state = fillFakeState(spgxlogState::from_bytes(ptr, SPGXLOG_ADD_NODE_STATE_OFF));

    if !has_block_ref(record, 1)? {
        // update in place
        debug_assert_eq!(xldata.parentBlk, -1);
        let (action, buffer) = XLogReadBufferForRedo(record, 0)?;
        if action == XLogRedoAction::BlkNeedsRedo {
            with_buffer_page::call(buffer, &mut |page: &mut [u8]| {
                {
                    let mut pm = PageMut::new(page)?;
                    PageIndexTupleDelete(&mut pm, xldata.offnum)?;
                }
                let added = {
                    let mut pm = PageMut::new(page)?;
                    PageAddItemExtended(&mut pm, inner_tuple, xldata.offnum, 0)?
                };
                if added != xldata.offnum {
                    return Err(elog_error(format!(
                        "failed to add item of size {inner_size} to SPGiST index page"
                    )));
                }
                Ok(())
            })?;
            bufmgr::page_set_lsn::call(buffer, lsn)?;
            mark_buffer_dirty::call(buffer);
        }
        if buffer_is_valid(buffer) {
            unlock_release_buffer::call(buffer);
        }
    } else {
        let blkno_new = block_tag_blkno(record, 1)?;

        // Install new tuple first so redirect is valid.
        let (action, buffer) = if xldata.newPage {
            // AddNode is not used for nulls pages
            let buffer = XLogInitBufferForRedo(record, 1)?;
            SpGistInitBuffer(buffer, 0)?;
            (XLogRedoAction::BlkNeedsRedo, buffer)
        } else {
            XLogReadBufferForRedo(record, 1)?
        };
        if action == XLogRedoAction::BlkNeedsRedo {
            with_buffer_page::call(buffer, &mut |page: &mut [u8]| {
                addOrReplaceTuple(page, inner_tuple, xldata.offnumNew)?;

                if xldata.parentBlk == 1 {
                    let p_off = {
                        let pr = PageRef::new(page)?;
                        PageGetItemId(&pr, xldata.offnumParent)?.lp_off() as usize
                    };
                    spgUpdateNodeLink(
                        &mut page[p_off..],
                        xldata.nodeI as i32,
                        blkno_new,
                        xldata.offnumNew,
                    )?;
                }
                Ok(())
            })?;
            bufmgr::page_set_lsn::call(buffer, lsn)?;
            mark_buffer_dirty::call(buffer);
        }
        if buffer_is_valid(buffer) {
            unlock_release_buffer::call(buffer);
        }

        // Delete old tuple, replacing it with redirect or placeholder tuple.
        let (saction, sbuffer) = XLogReadBufferForRedo(record, 0)?;
        if saction == XLogRedoAction::BlkNeedsRedo {
            if state.isBuild {
                spgFormDeadTuple(
                    &mut state,
                    SPGIST_PLACEHOLDER,
                    InvalidBlockNumber,
                    InvalidOffsetNumber,
                );
            } else {
                spgFormDeadTuple(&mut state, SPGIST_REDIRECT, blkno_new, xldata.offnumNew);
            }
            let dead = state
                .deadTupleStorage
                .as_ref()
                .expect("spgRedoAddNode: deadTupleStorage is NULL")
                .clone();
            let dead_size = lt_size(&dead);
            let dead = &dead[..dead_size];
            let is_build = state.isBuild;
            with_buffer_page::call(sbuffer, &mut |page: &mut [u8]| {
                {
                    let mut pm = PageMut::new(page)?;
                    PageIndexTupleDelete(&mut pm, xldata.offnum)?;
                }
                let added = {
                    let mut pm = PageMut::new(page)?;
                    PageAddItemExtended(&mut pm, dead, xldata.offnum, 0)?
                };
                if added != xldata.offnum {
                    return Err(elog_error(format!(
                        "failed to add item of size {dead_size} to SPGiST index page"
                    )));
                }

                if is_build {
                    set_opaque_n_placeholder(page, opaque_n_placeholder(page) + 1);
                } else {
                    set_opaque_n_redirection(page, opaque_n_redirection(page) + 1);
                }

                if xldata.parentBlk == 0 {
                    let p_off = {
                        let pr = PageRef::new(page)?;
                        PageGetItemId(&pr, xldata.offnumParent)?.lp_off() as usize
                    };
                    spgUpdateNodeLink(
                        &mut page[p_off..],
                        xldata.nodeI as i32,
                        blkno_new,
                        xldata.offnumNew,
                    )?;
                }
                Ok(())
            })?;
            bufmgr::page_set_lsn::call(sbuffer, lsn)?;
            mark_buffer_dirty::call(sbuffer);
        }
        if buffer_is_valid(sbuffer) {
            unlock_release_buffer::call(sbuffer);
        }

        // Update parent downlink, if not already done above.
        if xldata.parentBlk == 2 {
            let (action, buffer) = XLogReadBufferForRedo(record, 2)?;
            if action == XLogRedoAction::BlkNeedsRedo {
                with_buffer_page::call(buffer, &mut |page: &mut [u8]| {
                    let p_off = {
                        let pr = PageRef::new(page)?;
                        PageGetItemId(&pr, xldata.offnumParent)?.lp_off() as usize
                    };
                    spgUpdateNodeLink(
                        &mut page[p_off..],
                        xldata.nodeI as i32,
                        blkno_new,
                        xldata.offnumNew,
                    )
                })?;
                bufmgr::page_set_lsn::call(buffer, lsn)?;
                mark_buffer_dirty::call(buffer);
            }
            if buffer_is_valid(buffer) {
                unlock_release_buffer::call(buffer);
            }
        }
    }
    Ok(())
}

// ===========================================================================
// spgRedoSplitTuple (spgxlog.c:450)
// ===========================================================================

fn spgRedoSplitTuple(record: &XLogReaderState<'_>) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let ptr = record_get_data(record);
    let xldata = spgxlogSplitTuple::from_bytes(ptr);

    let prefix_tuple = &ptr[SIZE_OF_SPGXLOG_SPLIT_TUPLE..];
    let prefix_size = it_size(prefix_tuple);
    let postfix_tuple_all = &prefix_tuple[prefix_size..];
    let postfix_size = it_size(postfix_tuple_all);
    let prefix_tuple = &prefix_tuple[..prefix_size];
    let postfix_tuple = &postfix_tuple_all[..postfix_size];

    // insert postfix tuple first to avoid dangling link
    if !xldata.postfixBlkSame {
        let (action, buffer) = if xldata.newPage {
            let buffer = XLogInitBufferForRedo(record, 1)?;
            // SplitTuple is not used for nulls pages
            SpGistInitBuffer(buffer, 0)?;
            (XLogRedoAction::BlkNeedsRedo, buffer)
        } else {
            XLogReadBufferForRedo(record, 1)?
        };
        if action == XLogRedoAction::BlkNeedsRedo {
            with_buffer_page::call(buffer, &mut |page: &mut [u8]| {
                addOrReplaceTuple(page, postfix_tuple, xldata.offnumPostfix)
            })?;
            bufmgr::page_set_lsn::call(buffer, lsn)?;
            mark_buffer_dirty::call(buffer);
        }
        if buffer_is_valid(buffer) {
            unlock_release_buffer::call(buffer);
        }
    }

    // now handle the original page
    let (action, buffer) = XLogReadBufferForRedo(record, 0)?;
    if action == XLogRedoAction::BlkNeedsRedo {
        with_buffer_page::call(buffer, &mut |page: &mut [u8]| {
            {
                let mut pm = PageMut::new(page)?;
                PageIndexTupleDelete(&mut pm, xldata.offnumPrefix)?;
            }
            let added = {
                let mut pm = PageMut::new(page)?;
                PageAddItemExtended(&mut pm, prefix_tuple, xldata.offnumPrefix, 0)?
            };
            if added != xldata.offnumPrefix {
                return Err(elog_error(format!(
                    "failed to add item of size {prefix_size} to SPGiST index page"
                )));
            }

            if xldata.postfixBlkSame {
                addOrReplaceTuple(page, postfix_tuple, xldata.offnumPostfix)?;
            }
            Ok(())
        })?;
        bufmgr::page_set_lsn::call(buffer, lsn)?;
        mark_buffer_dirty::call(buffer);
    }
    if buffer_is_valid(buffer) {
        unlock_release_buffer::call(buffer);
    }
    Ok(())
}

// ===========================================================================
// spgRedoPickSplit (spgxlog.c:528)
// ===========================================================================

fn spgRedoPickSplit(record: &XLogReaderState<'_>) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let ptr = record_get_data(record);
    let xldata = spgxlogPickSplit::from_bytes(ptr);

    let blkno_inner = block_tag_blkno(record, 2)?;

    let mut state = fillFakeState(spgxlogState::from_bytes(ptr, SPGXLOG_PICK_SPLIT_STATE_OFF));

    let mut off = SIZE_OF_SPGXLOG_PICK_SPLIT;
    let to_delete = read_offsets(ptr, off, xldata.nDelete as usize);
    off += 2 * xldata.nDelete as usize;
    let to_insert = read_offsets(ptr, off, xldata.nInsert as usize);
    off += 2 * xldata.nInsert as usize;
    let leaf_page_select = ptr[off..off + xldata.nInsert as usize].to_vec();
    off += xldata.nInsert as usize;

    let inner_tuple = &ptr[off..];
    let inner_size = it_size(inner_tuple);
    let inner_tuple = &inner_tuple[..inner_size];
    off += inner_size;
    let tuples_off = off; // now ptr points to the list of leaf tuples

    // Per-tuple (start, size, off, to_dest) for the leaf-tuple distribution.
    let mut entries: Vec<(usize, usize, OffsetNumber, bool)> = Vec::new();
    {
        let mut cur = tuples_off;
        for i in 0..xldata.nInsert as usize {
            let sz = lt_size(&ptr[cur..]);
            entries.push((cur, sz, to_insert[i], leaf_page_select[i] != 0));
            cur += sz;
        }
    }

    // --- source page ---
    let (src_buffer, src_present) = if xldata.isRootSplit {
        // when splitting root, we touch it only in the guise of new inner
        (0 as Buffer, false)
    } else if read_init_src(ptr) {
        // just re-init the source page
        let b = XLogInitBufferForRedo(record, 0)?;
        with_buffer_page::call(b, &mut |page: &mut [u8]| {
            SpGistInitPage(
                page,
                SPGIST_LEAF | (if xldata.storesNulls { SPGIST_NULLS } else { 0 }),
            )
        })?;
        (b, true)
    } else {
        let (action, b) = XLogReadBufferForRedo(record, 0)?;
        if action == XLogRedoAction::BlkNeedsRedo {
            // inject the correct redirection tuple now
            let (fs, rs, blk, ofn) = if !state.isBuild {
                (
                    SPGIST_REDIRECT,
                    SPGIST_PLACEHOLDER,
                    blkno_inner,
                    xldata.offnumInner,
                )
            } else {
                (
                    SPGIST_PLACEHOLDER,
                    SPGIST_PLACEHOLDER,
                    InvalidBlockNumber,
                    InvalidOffsetNumber,
                )
            };
            with_buffer_page::call(b, &mut |page: &mut [u8]| {
                spgPageIndexMultiDelete(&mut state, page, &to_delete, fs, rs, blk, ofn)
            })?;
            (b, true)
        } else {
            (b, false)
        }
    };

    // --- dest page ---
    let (dest_buffer, dest_present) = if !has_block_ref(record, 1)? {
        (0 as Buffer, false)
    } else if read_init_dest(ptr) {
        let b = XLogInitBufferForRedo(record, 1)?;
        with_buffer_page::call(b, &mut |page: &mut [u8]| {
            SpGistInitPage(
                page,
                SPGIST_LEAF | (if xldata.storesNulls { SPGIST_NULLS } else { 0 }),
            )
        })?;
        (b, true)
    } else {
        let (action, b) = XLogReadBufferForRedo(record, 1)?;
        if action == XLogRedoAction::BlkNeedsRedo {
            (b, true)
        } else {
            (b, false)
        }
    };

    // restore leaf tuples to src and/or dest page
    if src_present {
        with_buffer_page::call(src_buffer, &mut |page: &mut [u8]| {
            for &(start, sz, ins, to_dest) in &entries {
                if to_dest {
                    continue;
                }
                addOrReplaceTuple(page, &ptr[start..start + sz], ins)?;
            }
            Ok(())
        })?;
    }
    if dest_present {
        with_buffer_page::call(dest_buffer, &mut |page: &mut [u8]| {
            for &(start, sz, ins, to_dest) in &entries {
                if !to_dest {
                    continue;
                }
                addOrReplaceTuple(page, &ptr[start..start + sz], ins)?;
            }
            Ok(())
        })?;
    }

    // Now update src and dest page LSNs if needed.
    if src_present {
        bufmgr::page_set_lsn::call(src_buffer, lsn)?;
        mark_buffer_dirty::call(src_buffer);
    }
    if dest_present {
        bufmgr::page_set_lsn::call(dest_buffer, lsn)?;
        mark_buffer_dirty::call(dest_buffer);
    }

    // restore new inner tuple
    let (action, inner_buffer) = if read_init_inner(ptr) {
        let b = XLogInitBufferForRedo(record, 2)?;
        SpGistInitBuffer(b, if xldata.storesNulls { SPGIST_NULLS } else { 0 })?;
        (XLogRedoAction::BlkNeedsRedo, b)
    } else {
        XLogReadBufferForRedo(record, 2)?
    };
    if action == XLogRedoAction::BlkNeedsRedo {
        with_buffer_page::call(inner_buffer, &mut |page: &mut [u8]| {
            addOrReplaceTuple(page, inner_tuple, xldata.offnumInner)?;

            if xldata.innerIsParent {
                let p_off = {
                    let pr = PageRef::new(page)?;
                    PageGetItemId(&pr, xldata.offnumParent)?.lp_off() as usize
                };
                spgUpdateNodeLink(
                    &mut page[p_off..],
                    xldata.nodeI as i32,
                    blkno_inner,
                    xldata.offnumInner,
                )?;
            }
            Ok(())
        })?;
        bufmgr::page_set_lsn::call(inner_buffer, lsn)?;
        mark_buffer_dirty::call(inner_buffer);
    }
    if buffer_is_valid(inner_buffer) {
        unlock_release_buffer::call(inner_buffer);
    }

    // release the leaf-page locks
    if buffer_is_valid(src_buffer) {
        unlock_release_buffer::call(src_buffer);
    }
    if buffer_is_valid(dest_buffer) {
        unlock_release_buffer::call(dest_buffer);
    }

    // update parent downlink, unless we did it above
    if has_block_ref(record, 3)? {
        let (action, parent_buffer) = XLogReadBufferForRedo(record, 3)?;
        if action == XLogRedoAction::BlkNeedsRedo {
            with_buffer_page::call(parent_buffer, &mut |page: &mut [u8]| {
                let p_off = {
                    let pr = PageRef::new(page)?;
                    PageGetItemId(&pr, xldata.offnumParent)?.lp_off() as usize
                };
                spgUpdateNodeLink(
                    &mut page[p_off..],
                    xldata.nodeI as i32,
                    blkno_inner,
                    xldata.offnumInner,
                )
            })?;
            bufmgr::page_set_lsn::call(parent_buffer, lsn)?;
            mark_buffer_dirty::call(parent_buffer);
        }
        if buffer_is_valid(parent_buffer) {
            unlock_release_buffer::call(parent_buffer);
        }
    } else {
        debug_assert!(xldata.innerIsParent || xldata.isRootSplit);
    }
    Ok(())
}

// ===========================================================================
// spgRedoVacuumLeaf (spgxlog.c:750)
// ===========================================================================

fn spgRedoVacuumLeaf(record: &XLogReaderState<'_>) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let ptr = record_get_data(record);
    let xldata = spgxlogVacuumLeaf::from_bytes(ptr);

    let mut state = fillFakeState(spgxlogState::from_bytes(ptr, SPGXLOG_VACUUM_LEAF_STATE_OFF));

    let mut off = SIZE_OF_SPGXLOG_VACUUM_LEAF;
    let to_dead = read_offsets(ptr, off, xldata.nDead as usize);
    off += 2 * xldata.nDead as usize;
    let to_placeholder = read_offsets(ptr, off, xldata.nPlaceholder as usize);
    off += 2 * xldata.nPlaceholder as usize;
    let move_src = read_offsets(ptr, off, xldata.nMove as usize);
    off += 2 * xldata.nMove as usize;
    let move_dest = read_offsets(ptr, off, xldata.nMove as usize);
    off += 2 * xldata.nMove as usize;
    let chain_src = read_offsets(ptr, off, xldata.nChain as usize);
    off += 2 * xldata.nChain as usize;
    let chain_dest = read_offsets(ptr, off, xldata.nChain as usize);

    let (action, buffer) = XLogReadBufferForRedo(record, 0)?;
    if action == XLogRedoAction::BlkNeedsRedo {
        with_buffer_page::call(buffer, &mut |page: &mut [u8]| {
            spgPageIndexMultiDelete(
                &mut state,
                page,
                &to_dead,
                SPGIST_DEAD,
                SPGIST_DEAD,
                InvalidBlockNumber,
                InvalidOffsetNumber,
            )?;

            spgPageIndexMultiDelete(
                &mut state,
                page,
                &to_placeholder,
                SPGIST_PLACEHOLDER,
                SPGIST_PLACEHOLDER,
                InvalidBlockNumber,
                InvalidOffsetNumber,
            )?;

            // see comments in vacuumLeafPage(): swap the line pointers
            for i in 0..xldata.nMove as usize {
                swap_item_ids(page, move_src[i], move_dest[i])?;
            }

            spgPageIndexMultiDelete(
                &mut state,
                page,
                &move_src,
                SPGIST_PLACEHOLDER,
                SPGIST_PLACEHOLDER,
                InvalidBlockNumber,
                InvalidOffsetNumber,
            )?;

            for i in 0..xldata.nChain as usize {
                let lt_off = {
                    let pr = PageRef::new(page)?;
                    PageGetItemId(&pr, chain_src[i])?.lp_off() as usize
                };
                debug_assert_eq!(lt_tupstate(&page[lt_off..]), SPGIST_LIVE);
                lt_set_next_offset(&mut page[lt_off..], chain_dest[i]);
            }
            Ok(())
        })?;
        bufmgr::page_set_lsn::call(buffer, lsn)?;
        mark_buffer_dirty::call(buffer);
    }
    if buffer_is_valid(buffer) {
        unlock_release_buffer::call(buffer);
    }
    Ok(())
}

// ===========================================================================
// spgRedoVacuumRoot (spgxlog.c:833)
// ===========================================================================

fn spgRedoVacuumRoot(record: &XLogReaderState<'_>) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let ptr = record_get_data(record);
    let xldata = spgxlogVacuumRoot::from_bytes(ptr);

    let to_delete = read_offsets(ptr, SIZE_OF_SPGXLOG_VACUUM_ROOT, xldata.nDelete as usize);

    let (action, buffer) = XLogReadBufferForRedo(record, 0)?;
    if action == XLogRedoAction::BlkNeedsRedo {
        with_buffer_page::call(buffer, &mut |page: &mut [u8]| {
            // The tuple numbers are in order
            let mut pm = PageMut::new(page)?;
            PageIndexMultiDelete(&mut pm, &to_delete)
        })?;
        bufmgr::page_set_lsn::call(buffer, lsn)?;
        mark_buffer_dirty::call(buffer);
    }
    if buffer_is_valid(buffer) {
        unlock_release_buffer::call(buffer);
    }
    Ok(())
}

// ===========================================================================
// spgRedoVacuumRedirect (spgxlog.c:859)
// ===========================================================================

fn spgRedoVacuumRedirect(mcx: Mcx<'_>, record: &XLogReaderState<'_>) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let ptr = record_get_data(record);
    let xldata = spgxlogVacuumRedirect::from_bytes(ptr);

    let item_to_placeholder = read_offsets(
        ptr,
        SIZE_OF_SPGXLOG_VACUUM_REDIRECT,
        xldata.nToPlaceholder as usize,
    );

    // If any redirection tuples are being removed, make sure there are no live
    // Hot Standby transactions that might need to see them.
    if in_hot_standby(xlogutils_seam::standby_state::call()) {
        if let Some(tag) = reader_seam::xlog_rec_get_block_tag_extended::call(record, 0)? {
            standby::resolve_recovery_conflict_with_snapshot::call(
                mcx,
                xldata.snapshotConflictHorizon,
                xldata.isCatalogRel,
                tag.rlocator,
            )?;
        }
    }

    let (action, buffer) = XLogReadBufferForRedo(record, 0)?;
    if action == XLogRedoAction::BlkNeedsRedo {
        with_buffer_page::call(buffer, &mut |page: &mut [u8]| {
            // Convert redirect pointers to plain placeholders
            for i in 0..xldata.nToPlaceholder as usize {
                let dt_off = {
                    let pr = PageRef::new(page)?;
                    PageGetItemId(&pr, item_to_placeholder[i])?.lp_off() as usize
                };
                debug_assert_eq!(dt_tupstate(&page[dt_off..]), SPGIST_REDIRECT);
                dt_set_tupstate(&mut page[dt_off..], SPGIST_PLACEHOLDER);
                dt_set_pointer_invalid(&mut page[dt_off..]);
            }

            debug_assert!(opaque_n_redirection(page) >= xldata.nToPlaceholder);
            set_opaque_n_redirection(page, opaque_n_redirection(page) - xldata.nToPlaceholder);
            set_opaque_n_placeholder(page, opaque_n_placeholder(page) + xldata.nToPlaceholder);

            // Remove placeholder tuples at end of page
            if xldata.firstPlaceholder != InvalidOffsetNumber {
                let max = PageGetMaxOffsetNumber(&PageRef::new(page)?);
                let mut to_delete: Vec<OffsetNumber> = Vec::new();
                let mut i = xldata.firstPlaceholder;
                while i <= max {
                    to_delete.push(i);
                    i += 1;
                }

                let n = (max - xldata.firstPlaceholder + 1) as u16;
                debug_assert!(opaque_n_placeholder(page) >= n);
                set_opaque_n_placeholder(page, opaque_n_placeholder(page) - n);

                // The array is sorted, so can use PageIndexMultiDelete
                let mut pm = PageMut::new(page)?;
                PageIndexMultiDelete(&mut pm, &to_delete)?;
            }
            Ok(())
        })?;
        bufmgr::page_set_lsn::call(buffer, lsn)?;
        mark_buffer_dirty::call(buffer);
    }
    if buffer_is_valid(buffer) {
        unlock_release_buffer::call(buffer);
    }
    Ok(())
}

// ===========================================================================
// spg_redo / spg_xlog_startup / spg_xlog_cleanup / spg_mask (spgxlog.c:934)
// ===========================================================================

/// `spg_redo(record)` (spgxlog.c:934) — WAL redo dispatch for SP-GiST.
pub fn spg_redo(record: &mut XLogReaderState<'_>) -> PgResult<()> {
    let info = record_get_info(record) & !XLR_INFO_MASK;

    // oldCxt = MemoryContextSwitchTo(opCtx); ... MemoryContextReset(opCtx);
    let result = with_op_ctx(|mcx| match info {
        XLOG_SPGIST_ADD_LEAF => spgRedoAddLeaf(record),
        XLOG_SPGIST_MOVE_LEAFS => spgRedoMoveLeafs(record),
        XLOG_SPGIST_ADD_NODE => spgRedoAddNode(record),
        XLOG_SPGIST_SPLIT_TUPLE => spgRedoSplitTuple(record),
        XLOG_SPGIST_PICKSPLIT => spgRedoPickSplit(record),
        XLOG_SPGIST_VACUUM_LEAF => spgRedoVacuumLeaf(record),
        XLOG_SPGIST_VACUUM_ROOT => spgRedoVacuumRoot(record),
        XLOG_SPGIST_VACUUM_REDIRECT => spgRedoVacuumRedirect(mcx, record),
        _ => Err(PgError::new(
            PANIC,
            format!("spg_redo: unknown op code {info}"),
        )),
    });
    result?;

    // MemoryContextReset(opCtx).
    OP_CTX.with(|c| {
        if let Some(ctx) = c.borrow_mut().as_mut() {
            ctx.reset();
        }
    });
    Ok(())
}

/// `spg_xlog_startup()` (spgxlog.c:975) — create the recovery working-memory
/// context (`rm_startup` slot).
pub fn spg_xlog_startup(_parent: Mcx<'_>) -> PgResult<()> {
    OP_CTX.with(|c| {
        *c.borrow_mut() = Some(MemoryContext::new("SP-GiST temporary context"));
    });
    Ok(())
}

/// `spg_xlog_cleanup()` (spgxlog.c:983) — delete the recovery context
/// (`rm_cleanup` slot).
pub fn spg_xlog_cleanup() {
    OP_CTX.with(|c| {
        *c.borrow_mut() = None;
    });
}

/// `spg_mask(pagedata, blkno)` (spgxlog.c:993) — mask an SP-GiST page before
/// consistency checks (`rm_mask` slot).
pub fn spg_mask(page: &mut [u8], _blkno: BlockNumber) -> PgResult<()> {
    mask_page_lsn_and_checksum::call(page);

    mask_page_hint_bits::call(page);

    // Mask the unused space, but only if the page's pd_lower appears to have
    // been set correctly.
    let pd_lower = read_pd_lower(page);
    if pd_lower >= SizeOfPageHeaderData as usize {
        mask_unused_space::call(page)?;
    }
    Ok(())
}

// ===========================================================================
// Local byte helpers.
// ===========================================================================

/// Read `n` `OffsetNumber`s (uint16, native endian) from `buf` at `off`.
fn read_offsets(buf: &[u8], off: usize, n: usize) -> Vec<OffsetNumber> {
    let mut v = Vec::with_capacity(n);
    for i in 0..n {
        let p = off + i * 2;
        v.push(u16::from_ne_bytes([buf[p], buf[p + 1]]));
    }
    v
}

/// `spgxlogPickSplit.initSrc` (write-only bool @6, not on the read struct).
fn read_init_src(rec: &[u8]) -> bool {
    rec[6] != 0
}
/// `spgxlogPickSplit.initDest` (bool @7).
fn read_init_dest(rec: &[u8]) -> bool {
    rec[7] != 0
}
/// `spgxlogPickSplit.initInner` (bool @10).
fn read_init_inner(rec: &[u8]) -> bool {
    rec[10] != 0
}

/// Read the page header's `pd_lower` field (offset 12).
fn read_pd_lower(page: &[u8]) -> usize {
    u16::from_ne_bytes([page[12], page[13]]) as usize
}
