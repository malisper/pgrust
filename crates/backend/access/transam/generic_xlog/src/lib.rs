//! generic_xlog.c: generic WAL records for AMs without a bespoke WAL format.
//! C divergences: the Start/Register/Finish protocol keeps C's shape but the
//! record is emitted through the one-call xloginsert form.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use mcx::{alloc_in, Mcx, PgBox};
use types_core::{BlockNumber, Buffer, InvalidBuffer, OffsetNumber, XLogRecPtr, BLCKSZ};
use types_error::{PgError, PgResult};
use types_rel::Relation;
use xloginsert_seams::{XLogRegBuf, REGBUF_FORCE_IMAGE, REGBUF_STANDARD};
use xlogreader_seams::XLogReaderState;
use xlogutils::{XLogReadBufferForRedo, XLogRedoAction};

#[cfg(test)]
mod tests;

pub const MAX_GENERIC_XLOG_PAGES: usize = 4;
pub const GENERIC_XLOG_FULL_IMAGE: i32 = 0x0001;

const FRAGMENT_HEADER_SIZE: usize = 2 * size_of::<OffsetNumber>();
const MATCH_THRESHOLD: usize = FRAGMENT_HEADER_SIZE;
const MAX_DELTA_SIZE: usize = BLCKSZ + 2 * FRAGMENT_HEADER_SIZE;

const RM_GENERIC_ID: u8 = types_core::RmgrIds::RM_GENERIC_ID as u8;
const InvalidXLogRecPtr: XLogRecPtr = 0;

fn pd_lower(page: &[u8]) -> usize {
    u16::from_ne_bytes([page[12], page[13]]) as usize
}

fn pd_upper(page: &[u8]) -> usize {
    u16::from_ne_bytes([page[14], page[15]]) as usize
}

fn page_set_lsn(page: &mut [u8], lsn: XLogRecPtr) {
    page[0..4].copy_from_slice(&((lsn >> 32) as u32).to_ne_bytes());
    page[4..8].copy_from_slice(&(lsn as u32).to_ne_bytes());
}

// SAFETY contract shared by the buffer views below: the caller holds a pin and
// exclusive content lock on every registered buffer (C GenericXLogFinish/redo contract).
unsafe fn buffer_page<'p>(buffer: Buffer) -> &'p mut [u8] {
    let p = bufmgr_seams::buffer_get_page::call(buffer).as_ptr();
    unsafe { core::slice::from_raw_parts_mut(p, BLCKSZ) }
}

#[repr(C, align(4096))]
struct PageImage([u8; BLCKSZ]);

struct PageData {
    buffer: Buffer,
    flags: i32,
    delta_len: usize,
    delta: [u8; MAX_DELTA_SIZE],
}

// C GenericXLogState is palloc_aligned(PG_IO_ALIGN_SIZE); PgBox drop is the pfree.
pub struct GenericXLogState {
    images: [PageImage; MAX_GENERIC_XLOG_PAGES],
    pages: [PageData; MAX_GENERIC_XLOG_PAGES],
    is_logged: bool,
}

const _: () = assert!(!core::mem::needs_drop::<GenericXLogState>());

// RelationNeedsWAL (rel.h), as nbtree renders it.
fn relation_needs_wal(rel: &Relation<'_>) -> bool {
    rel.is_permanent()
        && (transam_xlog_seams::xlog_standby_info_active::call()
            || (rel.rd_createSubid.get() == types_core::InvalidSubTransactionId
                && rel.rd_firstRelfilelocatorSubid.get() == types_core::InvalidSubTransactionId))
}

pub fn GenericXLogStart<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &Relation<'_>,
) -> PgResult<PgBox<'mcx, GenericXLogState>> {
    alloc_in(
        mcx,
        GenericXLogState {
            images: [const { PageImage([0; BLCKSZ]) }; MAX_GENERIC_XLOG_PAGES],
            pages: [const {
                PageData {
                    buffer: InvalidBuffer,
                    flags: 0,
                    delta_len: 0,
                    delta: [0; MAX_DELTA_SIZE],
                }
            }; MAX_GENERIC_XLOG_PAGES],
            is_logged: relation_needs_wal(relation),
        },
    )
}

impl GenericXLogState {
    pub fn register_buffer(&mut self, buffer: Buffer, flags: i32) -> PgResult<&mut [u8; BLCKSZ]> {
        for block_id in 0..MAX_GENERIC_XLOG_PAGES {
            let pd = &mut self.pages[block_id];
            if pd.buffer == InvalidBuffer {
                pd.buffer = buffer;
                pd.flags = flags;
                // SAFETY: contract at buffer_page.
                self.images[block_id].0.copy_from_slice(unsafe { buffer_page(buffer) });
                return Ok(&mut self.images[block_id].0);
            } else if pd.buffer == buffer {
                return Ok(&mut self.images[block_id].0);
            }
        }
        Err(PgError::error(format!(
            "maximum number {MAX_GENERIC_XLOG_PAGES} of generic xlog buffers is exceeded"
        ))
        .into())
    }

    pub fn page_image_mut(&mut self, block_id: usize) -> &mut [u8; BLCKSZ] {
        &mut self.images[block_id].0
    }

    pub fn is_logged(&self) -> bool {
        self.is_logged
    }
}

pub fn GenericXLogRegisterBuffer<'s>(
    state: &'s mut GenericXLogState,
    buffer: Buffer,
    flags: i32,
) -> PgResult<&'s mut [u8; BLCKSZ]> {
    state.register_buffer(buffer, flags)
}

pub fn GenericXLogFinish(state: PgBox<'_, GenericXLogState>) -> PgResult<XLogRecPtr> {
    let mut state = state;
    let st = &mut *state;
    init_small::globals::StartCriticalSection();
    let res = if st.is_logged {
        finish_logged(st)
    } else {
        finish_unlogged(st)
    };
    init_small::globals::EndCriticalSection();
    res
}

fn finish_logged(st: &mut GenericXLogState) -> PgResult<XLogRecPtr> {
    for i in 0..MAX_GENERIC_XLOG_PAGES {
        let image = &st.images[i].0;
        let pd = &mut st.pages[i];
        if pd.buffer == InvalidBuffer {
            continue;
        }
        // SAFETY: contract at buffer_page.
        let page = unsafe { buffer_page(pd.buffer) };
        if pd.flags & GENERIC_XLOG_FULL_IMAGE == 0 {
            compute_delta(&mut pd.delta, &mut pd.delta_len, page, image);
        }
        // Replay can't reconstruct the hole, so zero it here too.
        let (lower, upper) = (pd_lower(image), pd_upper(image));
        page[..lower].copy_from_slice(&image[..lower]);
        page[lower..upper].fill(0);
        page[upper..].copy_from_slice(&image[upper..]);
        bufmgr_seams::mark_buffer_dirty::call(pd.buffer)?;
    }

    let frags: [&[u8]; MAX_GENERIC_XLOG_PAGES] =
        core::array::from_fn(|i| &st.pages[i].delta[..st.pages[i].delta_len]);
    let mut bufs: [XLogRegBuf<'_>; MAX_GENERIC_XLOG_PAGES] = core::array::from_fn(|i| XLogRegBuf {
        block_id: i as u8,
        buffer: st.pages[i].buffer,
        flags: 0,
        bufdata: &[],
    });
    let mut n = 0;
    for i in 0..MAX_GENERIC_XLOG_PAGES {
        let pd = &st.pages[i];
        if pd.buffer == InvalidBuffer {
            continue;
        }
        bufs[n] = if pd.flags & GENERIC_XLOG_FULL_IMAGE != 0 {
            XLogRegBuf {
                block_id: i as u8,
                buffer: pd.buffer,
                flags: REGBUF_FORCE_IMAGE | REGBUF_STANDARD,
                bufdata: &[],
            }
        } else {
            XLogRegBuf {
                block_id: i as u8,
                buffer: pd.buffer,
                flags: REGBUF_STANDARD,
                bufdata: core::slice::from_ref(&frags[i]),
            }
        };
        n += 1;
    }
    let lsn = xloginsert_seams::xlog_insert_record::call(RM_GENERIC_ID, 0, 0, &[], &bufs[..n])?;

    for pd in &st.pages {
        if pd.buffer == InvalidBuffer {
            continue;
        }
        // SAFETY: contract at buffer_page.
        page_set_lsn(unsafe { buffer_page(pd.buffer) }, lsn);
    }
    Ok(lsn)
}

fn finish_unlogged(st: &mut GenericXLogState) -> PgResult<XLogRecPtr> {
    for i in 0..MAX_GENERIC_XLOG_PAGES {
        let pd = &st.pages[i];
        if pd.buffer == InvalidBuffer {
            continue;
        }
        // SAFETY: contract at buffer_page.
        unsafe { buffer_page(pd.buffer) }.copy_from_slice(&st.images[i].0);
        bufmgr_seams::mark_buffer_dirty::call(pd.buffer)?;
    }
    Ok(InvalidXLogRecPtr)
}

pub fn GenericXLogAbort(state: PgBox<'_, GenericXLogState>) {
    drop(state);
}

fn write_fragment(
    delta: &mut [u8],
    delta_len: &mut usize,
    offset: OffsetNumber,
    length: OffsetNumber,
    data: &[u8],
) {
    let mut ptr = *delta_len;
    debug_assert!(ptr + FRAGMENT_HEADER_SIZE + length as usize <= delta.len());
    delta[ptr..ptr + 2].copy_from_slice(&offset.to_ne_bytes());
    ptr += 2;
    delta[ptr..ptr + 2].copy_from_slice(&length.to_ne_bytes());
    ptr += 2;
    delta[ptr..ptr + length as usize].copy_from_slice(&data[..length as usize]);
    *delta_len = ptr + length as usize;
}

#[allow(clippy::too_many_arguments)]
fn compute_region_delta(
    delta: &mut [u8],
    delta_len: &mut usize,
    curpage: &[u8],
    targetpage: &[u8],
    mut target_start: i32,
    target_end: i32,
    valid_start: i32,
    valid_end: i32,
) {
    let mut fragment_begin: i32 = -1;
    let mut fragment_end: i32 = -1;

    // Bytes of curpage outside valid_start..valid_end are invalid and always
    // overwritten with target data (C computeRegionDelta contract).
    if valid_start > target_start {
        fragment_begin = target_start;
        target_start = valid_start;
    }
    let loop_end = target_end.min(valid_end);

    let mut i = target_start;
    while i < loop_end {
        if curpage[i as usize] != targetpage[i as usize] {
            if fragment_begin < 0 {
                fragment_begin = i;
            }
            fragment_end = -1;
            i += 1;
            while i < loop_end && curpage[i as usize] != targetpage[i as usize] {
                i += 1;
            }
            if i >= loop_end {
                break;
            }
        }

        fragment_end = i;
        i += 1;
        while i < loop_end && curpage[i as usize] == targetpage[i as usize] {
            i += 1;
        }

        // Runs of <= MATCH_THRESHOLD matching bytes merge into the fragment:
        // a fragment header costs as much as the bytes it would save.
        if fragment_begin >= 0 && (i - fragment_end) as usize > MATCH_THRESHOLD {
            write_fragment(
                delta,
                delta_len,
                fragment_begin as OffsetNumber,
                (fragment_end - fragment_begin) as OffsetNumber,
                &targetpage[fragment_begin as usize..],
            );
            fragment_begin = -1;
            fragment_end = -1;
        }
    }

    if loop_end < target_end {
        if fragment_begin < 0 {
            fragment_begin = loop_end;
        }
        fragment_end = target_end;
    }

    if fragment_begin >= 0 {
        if fragment_end < 0 {
            fragment_end = target_end;
        }
        write_fragment(
            delta,
            delta_len,
            fragment_begin as OffsetNumber,
            (fragment_end - fragment_begin) as OffsetNumber,
            &targetpage[fragment_begin as usize..],
        );
    }
}

fn compute_delta(delta: &mut [u8], delta_len: &mut usize, curpage: &[u8], targetpage: &[u8]) {
    *delta_len = 0;
    // Lower and upper page parts diverge independently; the hole between is
    // never logged (zeroed on apply instead).
    compute_region_delta(
        delta,
        delta_len,
        curpage,
        targetpage,
        0,
        pd_lower(targetpage) as i32,
        0,
        pd_lower(curpage) as i32,
    );
    compute_region_delta(
        delta,
        delta_len,
        curpage,
        targetpage,
        pd_upper(targetpage) as i32,
        BLCKSZ as i32,
        pd_upper(curpage) as i32,
        BLCKSZ as i32,
    );
}

fn apply_page_redo(page: &mut [u8], delta: &[u8]) {
    let mut ptr = 0usize;
    while ptr < delta.len() {
        let offset = u16::from_ne_bytes([delta[ptr], delta[ptr + 1]]) as usize;
        let length = u16::from_ne_bytes([delta[ptr + 2], delta[ptr + 3]]) as usize;
        ptr += FRAGMENT_HEADER_SIZE;
        page[offset..offset + length].copy_from_slice(&delta[ptr..ptr + length]);
        ptr += length;
    }
}

/// The BLK_NEEDS_REDO page transform of generic_redo (cross-replay test hook).
pub fn redo_page_transform(page: &mut [u8], delta: &[u8], lsn: XLogRecPtr) {
    apply_page_redo(page, delta);
    let (lower, upper) = (pd_lower(page), pd_upper(page));
    page[lower..upper].fill(0);
    page_set_lsn(page, lsn);
}

pub fn generic_redo(record: &mut XLogReaderState) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let max_block_id = record.record.as_ref().map_or(-1, |r| r.max_block_id);
    debug_assert!((max_block_id as i32) < MAX_GENERIC_XLOG_PAGES as i32);

    let mut buffers = [InvalidBuffer; MAX_GENERIC_XLOG_PAGES];
    let mut block_id: u8 = 0;
    while (block_id as i32) <= max_block_id as i32 {
        if !record.has_block_ref(block_id) {
            block_id += 1;
            continue;
        }
        let (action, buffer) = XLogReadBufferForRedo(record, block_id)?;
        buffers[block_id as usize] = buffer;
        if action == XLogRedoAction::BLK_NEEDS_REDO {
            // SAFETY: points into the reader's decode buffer, valid for the callback.
            let delta = unsafe { record.block(block_id).data_bytes() };
            // SAFETY: contract at buffer_page (redo holds pin + lock).
            redo_page_transform(unsafe { buffer_page(buffer) }, delta, lsn);
            bufmgr_seams::mark_buffer_dirty::call(buffer)?;
        }
        block_id += 1;
    }

    for &buffer in &buffers {
        if buffer != InvalidBuffer {
            bufmgr_seams::lock_buffer::call(buffer, bufmgr_seams::BUFFER_LOCK_UNLOCK)?;
            bufmgr_seams::release_buffer::call(buffer)?;
        }
    }
    Ok(())
}

pub fn generic_mask(pagedata: &mut [u8], _blkno: BlockNumber) -> PgResult<()> {
    bufmask::mask_page_lsn_and_checksum(pagedata);
    bufmask::mask_unused_space(pagedata);
    Ok(())
}

pub fn init_seams() {}
