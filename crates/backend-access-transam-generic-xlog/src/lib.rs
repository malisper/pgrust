//! `backend-access-transam-generic-xlog` — port of
//! `src/backend/access/transam/generic_xlog.c` (PostgreSQL 18.3).
//!
//! The generic WAL API used by index AMs that do not want a bespoke WAL
//! format: [`GenericXLogStart`], [`GenericXLogRegisterBuffer`],
//! [`GenericXLogFinish`], [`GenericXLogAbort`], plus the rmgr callbacks
//! [`generic_redo`] and [`generic_mask`]. It computes a compact delta between
//! the pre- and post-modification page images and replays it.
//!
//! The byte-level delta computation/application is pure and lives in-crate.
//! The genuinely-external operations — the relcache (`RelationNeedsWAL`), the
//! buffer manager, the WAL insert/registration API (`xloginsert.c`), the
//! recovery buffer-for-redo manager (`xlogutils.c`), and the page-masking
//! helpers (`access/common/bufmask.c`) — are reached through their owners'
//! seam crates.

#![allow(non_snake_case)]

use backend_access_common_bufmask_seams::{mask_page_lsn_and_checksum, mask_unused_space};
use backend_access_transam_xloginsert_seams::{
    xlog_begin_insert, xlog_insert_record, xlog_register_buf_data, xlog_register_buffer,
};
use backend_access_transam_xlogutils_seams::xlog_read_buffer_for_redo;
use backend_storage_buffer_bufmgr_seams::{
    mark_buffer_dirty, unlock_release_buffer, with_buffer_page,
};
use backend_utils_cache_relcache_seams::relation_needs_wal;
use mcx::{vec_with_capacity_in, Mcx, PgVec};
use types_core::{BlockNumber, OffsetNumber, BLCKSZ};
use types_error::{PgError, PgResult};
use types_rel::RelationData;
use types_storage::buf::{Buffer, BufferIsInvalid, BufferIsValid, InvalidBuffer};
use types_wal::rmgr::XLogReaderState;
use types_wal::xloginsert::{REGBUF_FORCE_IMAGE, REGBUF_STANDARD};
use types_wal::{XLogRedoAction, RM_GENERIC_ID};

// ---------------------------------------------------------------------------
// generic_xlog.h / generic_xlog.c constants (owned by this unit).
// ---------------------------------------------------------------------------

/// `MAX_GENERIC_XLOG_PAGES == XLR_NORMAL_MAX_BLOCK_ID` (generic_xlog.h /
/// xloginsert.h).
pub const MAX_GENERIC_XLOG_PAGES: usize = 4;

/// `GENERIC_XLOG_FULL_IMAGE 0x0001` (generic_xlog.h) — write a full-page image.
pub const GENERIC_XLOG_FULL_IMAGE: i32 = 0x0001;

/// `FRAGMENT_HEADER_SIZE (2 * sizeof(OffsetNumber))` (generic_xlog.c).
const FRAGMENT_HEADER_SIZE: usize = 2 * core::mem::size_of::<OffsetNumber>();

/// `MATCH_THRESHOLD FRAGMENT_HEADER_SIZE` (generic_xlog.c).
const MATCH_THRESHOLD: usize = FRAGMENT_HEADER_SIZE;

/// `MAX_DELTA_SIZE (BLCKSZ + 2 * FRAGMENT_HEADER_SIZE)` (generic_xlog.c).
const MAX_DELTA_SIZE: usize = BLCKSZ + 2 * FRAGMENT_HEADER_SIZE;

/// Size in bytes of one `OffsetNumber` fragment-header field.
const OFFSET_NUMBER_SIZE: usize = core::mem::size_of::<OffsetNumber>();

// ---------------------------------------------------------------------------
// PageHeaderData field access (storage/bufpage.h).
//
// generic_xlog.c reads pd_lower/pd_upper out of the page image to bound its
// delta/apply loops, and PageSetLSN writes pd_lsn.  In C these are
// `((PageHeader) page)->pd_*`; the fields sit at fixed offsets in
// PageHeaderData.  We read/write them out of the page byte slice directly to
// stay alignment-sound and ABI-faithful.
// ---------------------------------------------------------------------------

/// `offsetof(PageHeaderData, pd_lsn)` — pd_lsn is the first field.
const PD_LSN_OFFSET: usize = 0;
/// `offsetof(PageHeaderData, pd_lower)` — after pd_lsn(8) + pd_checksum(2) +
/// pd_flags(2).
const PD_LOWER_OFFSET: usize = 12;
/// `offsetof(PageHeaderData, pd_upper)`.
const PD_UPPER_OFFSET: usize = 14;

#[inline]
fn page_pd_lower(page: &[u8]) -> u16 {
    u16::from_ne_bytes([page[PD_LOWER_OFFSET], page[PD_LOWER_OFFSET + 1]])
}

#[inline]
fn page_pd_upper(page: &[u8]) -> u16 {
    u16::from_ne_bytes([page[PD_UPPER_OFFSET], page[PD_UPPER_OFFSET + 1]])
}

/// `PageSetLSN(page, lsn)` (bufpage.h) — store the 8-byte `pd_lsn` (split into
/// `xlogid`/`xrecoff` halves; the in-memory layout is the same native-endian
/// 64-bit value, so a single store matches `PageXLogRecPtrSet`).
#[inline]
fn page_set_lsn(page: &mut [u8], lsn: types_core::XLogRecPtr) {
    page[PD_LSN_OFFSET..PD_LSN_OFFSET + 8].copy_from_slice(&lsn.to_ne_bytes());
}

// ---------------------------------------------------------------------------
// Critical section (miscadmin.h START_CRIT_SECTION / END_CRIT_SECTION).
//
// `START_CRIT_SECTION()` is `(CritSectionCount++)`; `END_CRIT_SECTION()` is
// `(CritSectionCount--)`.  A RAII guard increments on construction and
// decrements on Drop so that an early return (Err propagation) does not leak a
// nonzero count.
// ---------------------------------------------------------------------------

struct CritSection;

impl CritSection {
    /// `START_CRIT_SECTION()`.
    fn enter() -> Self {
        let count = backend_utils_error::config::crit_section_count();
        backend_utils_error::config::set_crit_section_count(count + 1);
        CritSection
    }
}

impl Drop for CritSection {
    /// `END_CRIT_SECTION()`.
    fn drop(&mut self) {
        let count = backend_utils_error::config::crit_section_count();
        backend_utils_error::config::set_crit_section_count(count - 1);
    }
}

// ---------------------------------------------------------------------------
// Per-page state (generic_xlog.c `GenericXLogPageData`).
// ---------------------------------------------------------------------------

struct GenericXLogPageData<'mcx> {
    /// `buffer` — registered buffer.
    buffer: Buffer,
    /// `flags` — flags for this buffer.
    flags: i32,
    /// `deltaLen` — space consumed in delta field.
    delta_len: usize,
    /// `image` — copy of page image for modification (BLCKSZ bytes).
    image: PgVec<'mcx, u8>,
    /// `delta` — delta between page images (MAX_DELTA_SIZE bytes).
    delta: PgVec<'mcx, u8>,
}

impl<'mcx> GenericXLogPageData<'mcx> {
    fn new(mcx: Mcx<'mcx>) -> PgResult<Self> {
        let mut image = vec_with_capacity_in(mcx, BLCKSZ)?;
        image.resize(BLCKSZ, 0u8);
        let mut delta = vec_with_capacity_in(mcx, MAX_DELTA_SIZE)?;
        delta.resize(MAX_DELTA_SIZE, 0u8);
        Ok(Self {
            buffer: InvalidBuffer,
            flags: 0,
            delta_len: 0,
            image,
            delta,
        })
    }
}

/// `GenericXLogState` — state of generic xlog record construction. The C
/// `palloc_aligned(sizeof(GenericXLogState), PG_IO_ALIGN_SIZE, 0)` allocation;
/// the I/O alignment requirement is moot for plain owned buffers (the images
/// are not direct-I/O targets here). Dropping the state is the C `pfree`.
pub struct GenericXLogState<'mcx> {
    /// Info about each page.
    pages: PgVec<'mcx, GenericXLogPageData<'mcx>>,
    /// `isLogged`.
    is_logged: bool,
}

// ===========================================================================
// writeFragment  (generic_xlog.c:89-108)
// ===========================================================================

/// Write next fragment into the delta. `delta`/`delta_len` are split out of
/// the page slot so the caller can read the page image immutably while
/// appending to the delta, exactly as the C reads `targetpage` while writing
/// `pageData->delta`.
fn write_fragment(
    delta: &mut [u8],
    delta_len: &mut usize,
    offset: OffsetNumber,
    length: OffsetNumber,
    data: &[u8],
) {
    let mut ptr = *delta_len;

    debug_assert!(
        *delta_len + OFFSET_NUMBER_SIZE + OFFSET_NUMBER_SIZE + length as usize <= delta.len()
    );

    // memcpy(ptr, &offset, sizeof(offset)).
    delta[ptr..ptr + OFFSET_NUMBER_SIZE].copy_from_slice(&offset.to_ne_bytes());
    ptr += OFFSET_NUMBER_SIZE;

    // memcpy(ptr, &length, sizeof(length)).
    delta[ptr..ptr + OFFSET_NUMBER_SIZE].copy_from_slice(&length.to_ne_bytes());
    ptr += OFFSET_NUMBER_SIZE;

    // memcpy(ptr, data, length).
    delta[ptr..ptr + length as usize].copy_from_slice(&data[..length as usize]);
    ptr += length as usize;

    *delta_len = ptr;
}

// ===========================================================================
// computeRegionDelta  (generic_xlog.c:120-221)
// ===========================================================================

/// Compute the XLOG fragments to transform a region of `curpage` into the
/// corresponding region of `targetpage`, appended to the delta. The region
/// runs from `target_start` to `target_end-1`. Bytes in `curpage` outside
/// `valid_start..valid_end-1` are invalid and always overwritten with target
/// data.
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

    // Deal with any invalid start region by including it in first fragment.
    if valid_start > target_start {
        fragment_begin = target_start;
        target_start = valid_start;
    }

    // We'll deal with any invalid end region after the main loop.
    let loop_end = target_end.min(valid_end);

    // Examine all the potentially matchable bytes.
    let mut i = target_start;
    while i < loop_end {
        if curpage[i as usize] != targetpage[i as usize] {
            // On unmatched byte, start new fragment if not already in one.
            if fragment_begin < 0 {
                fragment_begin = i;
            }
            // Mark unmatched-data endpoint as uncertain.
            fragment_end = -1;
            // Extend the fragment as far as possible in a tight loop.
            i += 1;
            while i < loop_end && curpage[i as usize] != targetpage[i as usize] {
                i += 1;
            }
            if i >= loop_end {
                break;
            }
        }

        // Found a matched byte, so remember end of unmatched fragment.
        fragment_end = i;

        // Extend the match as far as possible in a tight loop.
        i += 1;
        while i < loop_end && curpage[i as usize] == targetpage[i as usize] {
            i += 1;
        }

        // Only case 3 (a >MATCH_THRESHOLD run reaching loopEnd) leaves a
        // meaningful fragmentEnd; the unconditional assignment above is OK.
        if fragment_begin >= 0 && (i - fragment_end) as usize > MATCH_THRESHOLD {
            write_fragment(
                delta,
                delta_len,
                fragment_begin as OffsetNumber,
                (fragment_end - fragment_begin) as OffsetNumber,
                &targetpage[fragment_begin as usize..],
            );
            fragment_begin = -1;
            fragment_end = -1; // not really necessary
        }
    }

    // Deal with any invalid end region by including it in final fragment.
    if loop_end < target_end {
        if fragment_begin < 0 {
            fragment_begin = loop_end;
        }
        fragment_end = target_end;
    }

    // Write final fragment if any.
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

// ===========================================================================
// computeDelta  (generic_xlog.c:227-263)
// ===========================================================================

/// Compute the XLOG delta record needed to transform `curpage` into
/// `targetpage`, and store it in `delta` (length tracked by `delta_len`).
fn compute_delta(delta: &mut [u8], delta_len: &mut usize, curpage: &[u8], targetpage: &[u8]) {
    let target_lower = page_pd_lower(targetpage) as i32;
    let target_upper = page_pd_upper(targetpage) as i32;
    let cur_lower = page_pd_lower(curpage) as i32;
    let cur_upper = page_pd_upper(curpage) as i32;

    *delta_len = 0;

    // Compute delta records for lower part of page ...
    compute_region_delta(
        delta, delta_len, curpage, targetpage, 0, target_lower, 0, cur_lower,
    );
    // ... and for upper part, ignoring what's between.
    compute_region_delta(
        delta,
        delta_len,
        curpage,
        targetpage,
        target_upper,
        BLCKSZ as i32,
        cur_upper,
        BLCKSZ as i32,
    );

    // The C #ifdef WAL_DEBUG block (re-applying the delta to verify it
    // reproduces targetpage) is compiled out in the default (non-WAL_DEBUG)
    // build.
}

// ===========================================================================
// GenericXLogStart  (generic_xlog.c:268-286)
// ===========================================================================

/// Start new generic xlog record for modifications to specified relation.
pub fn GenericXLogStart<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &RelationData<'_>,
) -> PgResult<GenericXLogState<'mcx>> {
    let is_logged = relation_needs_wal::call(relation);

    let mut pages = vec_with_capacity_in(mcx, MAX_GENERIC_XLOG_PAGES)?;
    for _ in 0..MAX_GENERIC_XLOG_PAGES {
        // `state->pages[i].image = state->images[i].data;` is implicit — each
        // page owns its image buffer. `page->buffer = InvalidBuffer`.
        pages.push(GenericXLogPageData::new(mcx)?);
    }

    Ok(GenericXLogState { pages, is_logged })
}

// ===========================================================================
// GenericXLogRegisterBuffer  (generic_xlog.c:298-330)
// ===========================================================================

/// Register new buffer for generic xlog record.
///
/// Returns the `block_id` for the page's entry; the caller mutates the page
/// image via [`GenericXLogState::page_image_mut`]. If the buffer is already
/// registered, just return its existing entry (the original flags are kept).
pub fn GenericXLogRegisterBuffer(
    state: &mut GenericXLogState<'_>,
    buffer: Buffer,
    flags: i32,
) -> PgResult<usize> {
    // Search array for existing entry or first unused slot.
    for block_id in 0..MAX_GENERIC_XLOG_PAGES {
        if BufferIsInvalid(state.pages[block_id].buffer) {
            // Empty slot, so use it (there cannot be a match later).
            // memcpy(page->image, BufferGetPage(buffer), BLCKSZ).
            with_buffer_page::call(buffer, &mut |page: &mut [u8]| {
                state.pages[block_id].image.copy_from_slice(&page[..BLCKSZ]);
                Ok(())
            })?;
            let page = &mut state.pages[block_id];
            page.buffer = buffer;
            page.flags = flags;
            return Ok(block_id);
        } else if state.pages[block_id].buffer == buffer {
            // Buffer is already registered.  Just return the image entry,
            // which is already prepared.
            return Ok(block_id);
        }
    }

    Err(PgError::error(format!(
        "maximum number {MAX_GENERIC_XLOG_PAGES} of generic xlog buffers is exceeded"
    )))
}

// ===========================================================================
// GenericXLogFinish  (generic_xlog.c:336-436)
// ===========================================================================

/// Apply changes represented by [`GenericXLogState`] to the actual buffers,
/// and emit a generic xlog record. Consumes the state (C `pfree(state)`).
pub fn GenericXLogFinish(mut state: GenericXLogState<'_>) -> PgResult<types_core::XLogRecPtr> {
    let lsn;

    if state.is_logged {
        // Logged relation: make xlog record in critical section.
        xlog_begin_insert::call()?;

        let crit = CritSection::enter();

        // Compute deltas if necessary, write changes to buffers, mark buffers
        // dirty, and register changes.
        for i in 0..MAX_GENERIC_XLOG_PAGES {
            if BufferIsInvalid(state.pages[i].buffer) {
                continue;
            }

            let buffer = state.pages[i].buffer;
            let flags = state.pages[i].flags;

            // Read the unmodified page (`page = BufferGetPage(...)`), compute
            // the delta against the new image, then apply the image to the
            // shared page — all while we hold the buffer through the callback.
            with_buffer_page::call(buffer, &mut |page: &mut [u8]| {
                let pd = &mut state.pages[i];

                // Compute delta while we still have both the unmodified page
                // and the new image. Not needed if logging the full image.
                if (flags & GENERIC_XLOG_FULL_IMAGE) == 0 {
                    let GenericXLogPageData {
                        delta,
                        delta_len,
                        image,
                        ..
                    } = pd;
                    compute_delta(delta, delta_len, page, image);
                }

                let pd_lower = page_pd_lower(&pd.image) as usize;
                let pd_upper = page_pd_upper(&pd.image) as usize;

                // Apply the image, zeroing the "hole" between pd_lower and
                // pd_upper to avoid divergence between actual page state and
                // what replay would produce.
                //   memcpy(page, pageData->image, pd_lower);
                page[..pd_lower].copy_from_slice(&pd.image[..pd_lower]);
                //   memset(page + pd_lower, 0, pd_upper - pd_lower);
                for b in page.iter_mut().take(pd_upper).skip(pd_lower) {
                    *b = 0;
                }
                //   memcpy(page + pd_upper, image + pd_upper, BLCKSZ - pd_upper);
                page[pd_upper..BLCKSZ].copy_from_slice(&pd.image[pd_upper..BLCKSZ]);
                Ok(())
            })?;

            mark_buffer_dirty::call(buffer);

            if (flags & GENERIC_XLOG_FULL_IMAGE) != 0 {
                xlog_register_buffer::call(
                    i as u8,
                    buffer,
                    REGBUF_FORCE_IMAGE | REGBUF_STANDARD,
                )?;
            } else {
                xlog_register_buffer::call(i as u8, buffer, REGBUF_STANDARD)?;
                let pd = &state.pages[i];
                xlog_register_buf_data::call(i as u8, &pd.delta[..pd.delta_len])?;
            }
        }

        // Insert xlog record.
        lsn = xlog_insert_record::call(RM_GENERIC_ID, 0)?;

        // Set LSN.
        for i in 0..MAX_GENERIC_XLOG_PAGES {
            if BufferIsInvalid(state.pages[i].buffer) {
                continue;
            }
            let buffer = state.pages[i].buffer;
            with_buffer_page::call(buffer, &mut |page: &mut [u8]| {
                page_set_lsn(page, lsn);
                Ok(())
            })?;
        }
        drop(crit); // END_CRIT_SECTION();
    } else {
        // Unlogged relation: skip xlog-related stuff.
        let crit = CritSection::enter();
        for i in 0..MAX_GENERIC_XLOG_PAGES {
            if BufferIsInvalid(state.pages[i].buffer) {
                continue;
            }
            let buffer = state.pages[i].buffer;
            // memcpy(BufferGetPage(buffer), pageData->image, BLCKSZ).
            with_buffer_page::call(buffer, &mut |page: &mut [u8]| {
                page[..BLCKSZ].copy_from_slice(&state.pages[i].image[..BLCKSZ]);
                Ok(())
            })?;
            // We don't worry about zeroing the "hole" in this case.
            mark_buffer_dirty::call(buffer);
        }
        drop(crit); // END_CRIT_SECTION();
        // We don't have a LSN to return, in this case.
        lsn = types_core::xact::InvalidXLogRecPtr;
    }

    // pfree(state) — `state` is consumed by value and dropped here.
    drop(state);

    Ok(lsn)
}

// ===========================================================================
// GenericXLogAbort  (generic_xlog.c:443-447)
// ===========================================================================

/// Abort generic xlog record construction. No changes are applied to buffers.
///
/// Note: caller is responsible for releasing locks/pins on buffers, if needed.
pub fn GenericXLogAbort(state: GenericXLogState<'_>) {
    // pfree(state) — consumed by value and dropped.
    drop(state);
}

// ===========================================================================
// applyPageRedo  (generic_xlog.c:452-472)
// ===========================================================================

/// Apply `delta` to given page image (`delta.len()` bytes of delta).
fn apply_page_redo(page: &mut [u8], delta: &[u8]) {
    let mut ptr = 0usize;
    let end = delta.len();

    while ptr < end {
        // memcpy(&offset, ptr, sizeof(offset)).
        let offset = u16::from_ne_bytes([delta[ptr], delta[ptr + 1]]) as usize;
        ptr += OFFSET_NUMBER_SIZE;
        // memcpy(&length, ptr, sizeof(length)).
        let length = u16::from_ne_bytes([delta[ptr], delta[ptr + 1]]) as usize;
        ptr += OFFSET_NUMBER_SIZE;

        // memcpy(page + offset, ptr, length).
        page[offset..offset + length].copy_from_slice(&delta[ptr..ptr + length]);

        ptr += length;
    }
}

// ===========================================================================
// generic_redo  (generic_xlog.c:477-533)
// ===========================================================================

/// Redo function for generic xlog record.
pub fn generic_redo(record: &mut XLogReaderState<'_>) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let mut buffers: [Buffer; MAX_GENERIC_XLOG_PAGES] = [InvalidBuffer; MAX_GENERIC_XLOG_PAGES];

    let max_block_id = record
        .record
        .as_ref()
        .map(|r| r.max_block_id())
        .unwrap_or(-1);

    // Protect limited size of buffers[] array.
    debug_assert!(max_block_id < MAX_GENERIC_XLOG_PAGES as i32);

    // Iterate over blocks.
    let mut block_id: u8 = 0;
    while (block_id as i32) <= max_block_id {
        let has_block_ref = record
            .record
            .as_ref()
            .is_some_and(|r| r.has_block_ref(block_id as usize));
        if !has_block_ref {
            buffers[block_id as usize] = InvalidBuffer;
            block_id += 1;
            continue;
        }

        let (action, buffer) = xlog_read_buffer_for_redo::call(record, block_id)?;
        buffers[block_id as usize] = buffer;

        // Apply redo to given block if needed.
        if action == XLogRedoAction::BlkNeedsRedo {
            // blockDelta = XLogRecGetBlockData(record, block_id, &blockDeltaSize).
            let block_delta = record
                .record
                .as_ref()
                .and_then(|r| r.block_data(block_id as usize))
                .unwrap_or(&[]);

            with_buffer_page::call(buffer, &mut |page: &mut [u8]| {
                // page = BufferGetPage(buffers[block_id]).
                apply_page_redo(page, block_delta);

                // Since the delta contains no information about what's in the
                // "hole" between pd_lower and pd_upper, set that to zero to
                // ensure we produce the same page state that application of the
                // logged action by GenericXLogFinish did.
                let pd_lower = page_pd_lower(page) as usize;
                let pd_upper = page_pd_upper(page) as usize;
                for b in page.iter_mut().take(pd_upper).skip(pd_lower) {
                    *b = 0;
                }

                page_set_lsn(page, lsn);
                Ok(())
            })?;
            mark_buffer_dirty::call(buffer);
        }

        block_id += 1;
    }

    // Changes are done: unlock and release all buffers.
    let mut block_id: u8 = 0;
    while (block_id as i32) <= max_block_id {
        if BufferIsValid(buffers[block_id as usize]) {
            unlock_release_buffer::call(buffers[block_id as usize]);
        }
        block_id += 1;
    }

    Ok(())
}

// ===========================================================================
// generic_mask  (generic_xlog.c:538-544)
// ===========================================================================

/// Mask a generic page before performing consistency checks on it.
///
/// `blkno` mirrors the rmgr-callback signature; generic_xlog.c does not use it.
pub fn generic_mask(page: &mut [u8], _blkno: BlockNumber) -> PgResult<()> {
    mask_page_lsn_and_checksum::call(page);
    mask_unused_space::call(page)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Public state accessors.
// ---------------------------------------------------------------------------

impl<'mcx> GenericXLogState<'mcx> {
    /// `(Page) page->image` — a mutable view of the page image the caller
    /// modifies after registering a buffer.
    pub fn page_image_mut(&mut self, block_id: usize) -> &mut [u8] {
        &mut self.pages[block_id].image
    }

    /// A shared view of a registered page image.
    pub fn page_image(&self, block_id: usize) -> &[u8] {
        &self.pages[block_id].image
    }

    /// The computed delta bytes for a registered page (valid after
    /// [`GenericXLogFinish`]'s compute step; exposed for tests).
    pub fn page_delta(&self, block_id: usize) -> &[u8] {
        &self.pages[block_id].delta[..self.pages[block_id].delta_len]
    }

    /// `state->isLogged`.
    pub fn is_logged(&self) -> bool {
        self.is_logged
    }
}

// ---------------------------------------------------------------------------
// Seam installation.
// ---------------------------------------------------------------------------

/// Install every seam this crate owns (the `Generic` rmgr-table callbacks).
pub fn init_seams() {
    backend_access_transam_generic_xlog_seams::generic_redo::set(generic_redo);
    backend_access_transam_generic_xlog_seams::generic_mask::set(generic_mask);
}

#[cfg(test)]
mod tests;
