//! Port of `src/backend/access/gist/gistbuildbuffers.c` (PostgreSQL 18.3): node
//! buffer management for the GiST buffering build algorithm.
//!
//! ## Memory / aliasing model
//!
//! A C `GISTNodeBuffer *` is aliased from the `nodeBuffersTab` hash, the
//! `bufferEmptyingQueue`, one of the `buffersOnLevels[level]` lists and the
//! `loadedBuffers` array all at once. The owned model carries each buffer behind
//! [`SharedNodeBuffer`] = `Rc<RefCell<GISTNodeBuffer>>` (the sanctioned shared
//! carrier), so every collection holds a cloned handle and a mutation through
//! one alias is seen by all.
//!
//! ## Page byte model
//!
//! A C [`GISTNodeBufferPage`] is a `BLCKSZ` block: the two-word header
//! (`prev` + `freespace`) at offset 0, then the tuple data filling from the END
//! of the data area towards `BUFFER_PAGE_DATA_OFFSET`. `freespace` counts the
//! free bytes between the header and the first tuple. In the owned model
//! `tupledata` is the [`DATA_SIZE`]-byte data area; a tuple of `MAXALIGN(sz)`
//! bytes occupies `tupledata[freespace .. freespace + MAXALIGN(sz)]` after
//! `freespace` has been decremented (matching the C `data + freespace` write).
//! [`serialize_page`] / [`deserialize_page`] convert to/from the on-temp-file
//! `BLCKSZ` block for [`ReadTempFileBlock`] / [`WriteTempFileBlock`].

use ::mcx::Mcx;

use buffile_seams as buffile;

use ::types_core::primitive::{BlockNumber, InvalidBlockNumber, BLCKSZ};
use ::types_error::PgResult;
use gist::{GISTBuildBuffers, GISTNodeBuffer, GISTNodeBufferPage, SharedNodeBuffer};
use ::rel::Relation;

use alloc::rc::Rc;
use core::cell::RefCell;

extern crate alloc;

/// `MAXALIGN(x)` (c.h) with the 8-byte default alignment.
#[inline]
const fn maxalign(x: usize) -> usize {
    (x + 7) & !7
}

/// `BUFFER_PAGE_DATA_OFFSET = MAXALIGN(offsetof(GISTNodeBufferPage, tupledata))`
/// (gist_private.h): the header size before the flexible `tupledata`. The C
/// struct is `{ BlockNumber prev (4); uint32 freespace (4); char tupledata[] }`,
/// so `offsetof(tupledata) == 8` and `MAXALIGN(8) == 8`.
pub const BUFFER_PAGE_DATA_OFFSET: usize = maxalign(8);

/// Size of the tuple data area of a node buffer page (`BLCKSZ -
/// BUFFER_PAGE_DATA_OFFSET`). The full free space of a fresh page.
pub const DATA_SIZE: usize = BLCKSZ - BUFFER_PAGE_DATA_OFFSET;

/// `IndexTupleSize(itup)` (access/itup.h) over an on-disk byte image: the low
/// 13 bits (`INDEX_SIZE_MASK`) of `t_info`, the `u16` at byte offset 6.
#[inline]
pub fn index_tuple_size(itup: &[u8]) -> usize {
    const INDEX_SIZE_MASK: u16 = 0x1fff;
    let t_info = u16::from_ne_bytes([itup[6], itup[7]]);
    (t_info & INDEX_SIZE_MASK) as usize
}

/// `PAGE_IS_EMPTY(nbp)` (gist_private.h): `freespace == BLCKSZ -
/// BUFFER_PAGE_DATA_OFFSET`.
#[inline]
fn page_is_empty(page: &GISTNodeBufferPage) -> bool {
    page.freespace as usize == DATA_SIZE
}

/// `PAGE_NO_SPACE(nbp, itup)` (gist_private.h): `PAGE_FREE_SPACE(nbp) <
/// MAXALIGN(IndexTupleSize(itup))`.
#[inline]
fn page_no_space(page: &GISTNodeBufferPage, itup: &[u8]) -> bool {
    (page.freespace as usize) < maxalign(index_tuple_size(itup))
}

/// `LEVEL_HAS_BUFFERS(nlevel, gfbb)` (gist_private.h).
#[inline]
pub fn level_has_buffers(nlevel: i32, gfbb: &GISTBuildBuffers<'_>) -> bool {
    nlevel != 0 && nlevel % gfbb.levelStep == 0 && nlevel != gfbb.rootlevel
}

/// `BUFFER_HALF_FILLED(nodeBuffer, gfbb)` (gist_private.h).
#[inline]
pub fn buffer_half_filled(node_buffer: &GISTNodeBuffer, gfbb: &GISTBuildBuffers<'_>) -> bool {
    node_buffer.blocksCount > gfbb.pagesPerBuffer / 2
}

/// `BUFFER_OVERFLOWED(nodeBuffer, gfbb)` (gist_private.h).
#[inline]
pub fn buffer_overflowed(node_buffer: &GISTNodeBuffer, gfbb: &GISTBuildBuffers<'_>) -> bool {
    node_buffer.blocksCount > gfbb.pagesPerBuffer
}

/// Serialize an in-memory [`GISTNodeBufferPage`] to its on-temp-file `BLCKSZ`
/// block: `prev` (4 bytes) + `freespace` (4 bytes) header at offset 0, then the
/// [`DATA_SIZE`]-byte data area. Mirrors writing the C struct's bytes verbatim.
fn serialize_page(page: &GISTNodeBufferPage) -> [u8; BLCKSZ] {
    let mut block = [0u8; BLCKSZ];
    block[0..4].copy_from_slice(&page.prev.to_ne_bytes());
    block[4..8].copy_from_slice(&page.freespace.to_ne_bytes());
    // The data area is the rest of the block.
    let n = page.tupledata.len().min(DATA_SIZE);
    block[BUFFER_PAGE_DATA_OFFSET..BUFFER_PAGE_DATA_OFFSET + n]
        .copy_from_slice(&page.tupledata[..n]);
    block
}

/// Deserialize a `BLCKSZ` block read from the temp file into the in-memory page,
/// overwriting `page` in place (C reads the block straight into the page
/// buffer).
fn deserialize_page(page: &mut GISTNodeBufferPage, block: &[u8]) {
    page.prev = BlockNumber::from_ne_bytes([block[0], block[1], block[2], block[3]]);
    page.freespace = u32::from_ne_bytes([block[4], block[5], block[6], block[7]]);
    page.tupledata.clear();
    page.tupledata
        .extend_from_slice(&block[BUFFER_PAGE_DATA_OFFSET..BUFFER_PAGE_DATA_OFFSET + DATA_SIZE]);
}

// ===========================================================================
// gistInitBuildBuffers (gistbuildbuffers.c:43)
// ===========================================================================

/// `gistInitBuildBuffers(pagesPerBuffer, levelStep, maxLevel)`
/// (gistbuildbuffers.c:43): initialize GiST build buffers.
///
/// The C `BufFileCreateTemp(false)` is created here. C `nodeBuffersTab` /
/// `bufferEmptyingQueue` / `buffersOnLevels` / `loadedBuffers` are the owned
/// collections in [`GISTBuildBuffers`]; their `*Len`/`*Count` bookkeeping is the
/// collections' own length.
pub fn gistInitBuildBuffers<'mcx>(
    mcx: Mcx<'mcx>,
    pages_per_buffer: i32,
    level_step: i32,
    max_level: i32,
) -> PgResult<GISTBuildBuffers<'mcx>> {
    // Create a temporary file to hold buffer pages that are swapped out of
    // memory.
    let pfile = buffile::buf_file_create_temp::call(mcx, false)?;

    Ok(GISTBuildBuffers {
        context: mcx,
        pfile: Some(pfile),
        nFileBlocks: 0,
        // freeBlocks: C reserves 32 slots; the Vec grows as needed.
        freeBlocks: alloc::vec::Vec::with_capacity(32),
        nodeBuffersTab: std::collections::HashMap::new(),
        bufferEmptyingQueue: alloc::vec::Vec::new(),
        levelStep: level_step,
        pagesPerBuffer: pages_per_buffer,
        // buffersOnLevels: one (empty) level to start, like the C
        // buffersOnLevelsLen = 1.
        buffersOnLevels: alloc::vec![alloc::vec::Vec::new()],
        loadedBuffers: alloc::vec::Vec::with_capacity(32),
        rootlevel: max_level,
    })
}

// ===========================================================================
// gistGetNodeBuffer (gistbuildbuffers.c:112)
// ===========================================================================

/// `gistGetNodeBuffer(gfbb, giststate, nodeBlocknum, level)`
/// (gistbuildbuffers.c:112): return the node buffer for `nodeBlocknum`, creating
/// it (empty) if it doesn't exist yet. Returns the shared handle.
pub fn gistGetNodeBuffer<'mcx>(
    gfbb: &mut GISTBuildBuffers<'mcx>,
    node_blocknum: BlockNumber,
    level: i32,
) -> SharedNodeBuffer {
    if let Some(existing) = gfbb.nodeBuffersTab.get(&node_blocknum) {
        return Rc::clone(existing);
    }

    // Node buffer wasn't found. Initialize the new buffer as empty.
    let node_buffer: SharedNodeBuffer = Rc::new(RefCell::new(GISTNodeBuffer {
        nodeBlocknum: node_blocknum,
        blocksCount: 0,
        pageBlocknum: InvalidBlockNumber,
        pageBuffer: None,
        queuedForEmptying: false,
        isTemp: false,
        level,
    }));

    gfbb.nodeBuffersTab
        .insert(node_blocknum, Rc::clone(&node_buffer));

    // Add this buffer to the list of buffers on this level. Enlarge the
    // buffersOnLevels array if needed (initializing the enlarged portion to
    // empty lists).
    if level as usize >= gfbb.buffersOnLevels.len() {
        gfbb.buffersOnLevels
            .resize_with(level as usize + 1, alloc::vec::Vec::new);
    }

    // Prepend the new buffer to the list of buffers on this level (lcons): in
    // the final emptying phase newly split pages are flushed before pre-existing
    // ones (they are likely still in cache).
    gfbb.buffersOnLevels[level as usize].insert(0, Rc::clone(&node_buffer));

    node_buffer
}

// ===========================================================================
// gistAllocateNewPageBuffer (gistbuildbuffers.c:180)
// ===========================================================================

/// `gistAllocateNewPageBuffer(gfbb)` (gistbuildbuffers.c:180): allocate a fresh,
/// empty buffer page (`prev == InvalidBlockNumber`, free space == the full data
/// area).
fn gistAllocateNewPageBuffer() -> GISTNodeBufferPage {
    GISTNodeBufferPage {
        prev: InvalidBlockNumber,
        freespace: DATA_SIZE as u32,
        // C MemoryContextAllocZero(BLCKSZ): the data area is all zero.
        tupledata: alloc::vec![0u8; DATA_SIZE],
    }
}

// ===========================================================================
// gistAddLoadedBuffer (gistbuildbuffers.c:197)
// ===========================================================================

/// `gistAddLoadedBuffer(gfbb, nodeBuffer)` (gistbuildbuffers.c:197): add
/// `node_buffer` to the loaded array (skipping temporary buffers).
fn gistAddLoadedBuffer(gfbb: &mut GISTBuildBuffers<'_>, node_buffer: &SharedNodeBuffer) {
    // Never add a temporary buffer to the array.
    if node_buffer.borrow().isTemp {
        return;
    }
    gfbb.loadedBuffers.push(Rc::clone(node_buffer));
}

// ===========================================================================
// gistLoadNodeBuffer (gistbuildbuffers.c:220)
// ===========================================================================

/// `gistLoadNodeBuffer(gfbb, nodeBuffer)` (gistbuildbuffers.c:220): load the
/// last page of `node_buffer` into main memory from the temp file.
fn gistLoadNodeBuffer(
    gfbb: &mut GISTBuildBuffers<'_>,
    node_buffer: &SharedNodeBuffer,
) -> PgResult<()> {
    // Check if we really should load something.
    let (need_load, page_blocknum) = {
        let nb = node_buffer.borrow();
        (nb.pageBuffer.is_none() && nb.blocksCount > 0, nb.pageBlocknum)
    };
    if need_load {
        // Allocate memory for page and read the block from the temporary file.
        let mut new_page = gistAllocateNewPageBuffer();
        let mut block = [0u8; BLCKSZ];
        ReadTempFileBlock(gfbb, page_blocknum as i64, &mut block)?;
        deserialize_page(&mut new_page, &block);
        node_buffer.borrow_mut().pageBuffer = Some(new_page);

        // Mark file block as free.
        gistBuffersReleaseBlock(gfbb, page_blocknum as i64);

        // Mark node buffer as loaded.
        gistAddLoadedBuffer(gfbb, node_buffer);
        node_buffer.borrow_mut().pageBlocknum = InvalidBlockNumber;
    }
    Ok(())
}

// ===========================================================================
// gistUnloadNodeBuffer (gistbuildbuffers.c:245)
// ===========================================================================

/// `gistUnloadNodeBuffer(gfbb, nodeBuffer)` (gistbuildbuffers.c:245): write the
/// last page of `node_buffer` to disk.
fn gistUnloadNodeBuffer(
    gfbb: &mut GISTBuildBuffers<'_>,
    node_buffer: &SharedNodeBuffer,
) -> PgResult<()> {
    // Check if we have something to write.
    let serialized = {
        let nb = node_buffer.borrow();
        nb.pageBuffer.as_ref().map(serialize_page)
    };
    if let Some(block) = serialized {
        // Get a free file block and write the block to the temporary file.
        let blkno = gistBuffersGetFreeBlock(gfbb);
        WriteTempFileBlock(gfbb, blkno, &block)?;

        // Free memory of that page and save the block number.
        let mut nb = node_buffer.borrow_mut();
        nb.pageBuffer = None;
        nb.pageBlocknum = blkno as BlockNumber;
    }
    Ok(())
}

// ===========================================================================
// gistUnloadNodeBuffers (gistbuildbuffers.c:271)
// ===========================================================================

/// `gistUnloadNodeBuffers(gfbb)` (gistbuildbuffers.c:271): write the last pages
/// of all loaded node buffers to disk, then clear the loaded array.
pub fn gistUnloadNodeBuffers(gfbb: &mut GISTBuildBuffers<'_>) -> PgResult<()> {
    // Unload all the buffers that have a page loaded in memory.
    let loaded: alloc::vec::Vec<SharedNodeBuffer> = gfbb.loadedBuffers.iter().map(Rc::clone).collect();
    for nb in &loaded {
        gistUnloadNodeBuffer(gfbb, nb)?;
    }

    // Now there are no node buffers with loaded last page.
    gfbb.loadedBuffers.clear();
    Ok(())
}

// ===========================================================================
// gistPlaceItupToPage (gistbuildbuffers.c:287)
// ===========================================================================

/// `gistPlaceItupToPage(pageBuffer, itup)` (gistbuildbuffers.c:287): add an
/// index tuple to the buffer page. The tuple is copied to the end of the free
/// space (the C `data + freespace` after decrementing `freespace`).
fn gistPlaceItupToPage(page: &mut GISTNodeBufferPage, itup: &[u8]) {
    let itupsz = index_tuple_size(itup);
    let aligned = maxalign(itupsz);

    // There should be enough of space.
    debug_assert!(page.freespace as usize >= aligned);

    // Reduce free space value of page to reserve a spot for the tuple.
    page.freespace -= aligned as u32;

    // Get pointer to the spot we reserved (ie. end of free space) and copy the
    // index tuple there.
    let off = page.freespace as usize;
    page.tupledata[off..off + itupsz].copy_from_slice(&itup[..itupsz]);
}

// ===========================================================================
// gistGetItupFromPage (gistbuildbuffers.c:310)
// ===========================================================================

/// `gistGetItupFromPage(pageBuffer, itup)` (gistbuildbuffers.c:310): get the
/// last item from the buffer page and remove it; returns a freshly allocated
/// copy of the tuple bytes.
fn gistGetItupFromPage<'mcx>(
    mcx: Mcx<'mcx>,
    page: &mut GISTNodeBufferPage,
) -> PgResult<::mcx::PgVec<'mcx, u8>> {
    // Page shouldn't be empty.
    debug_assert!(!page_is_empty(page));

    // Get pointer to last index tuple (at the start of the occupied region).
    let off = page.freespace as usize;
    let itupsz = index_tuple_size(&page.tupledata[off..]);

    // Make a copy of the tuple.
    let out = ::mcx::slice_in(mcx, &page.tupledata[off..off + itupsz])?;

    // Mark the space used by the tuple as free.
    page.freespace += maxalign(itupsz) as u32;

    Ok(out)
}

// ===========================================================================
// gistPushItupToNodeBuffer (gistbuildbuffers.c:335)
// ===========================================================================

/// `gistPushItupToNodeBuffer(gfbb, nodeBuffer, itup)` (gistbuildbuffers.c:335):
/// push an index tuple onto a node buffer, spilling the previous page to disk
/// and queueing the buffer for emptying if it overflows.
pub fn gistPushItupToNodeBuffer(
    gfbb: &mut GISTBuildBuffers<'_>,
    node_buffer: &SharedNodeBuffer,
    itup: &[u8],
) -> PgResult<()> {
    // If the buffer is currently empty, create the first page.
    let blocks_count = node_buffer.borrow().blocksCount;
    if blocks_count == 0 {
        let mut nb = node_buffer.borrow_mut();
        nb.pageBuffer = Some(gistAllocateNewPageBuffer());
        nb.blocksCount = 1;
        drop(nb);
        gistAddLoadedBuffer(gfbb, node_buffer);
    }

    // Load last page of node buffer if it wasn't in memory already.
    if node_buffer.borrow().pageBuffer.is_none() {
        gistLoadNodeBuffer(gfbb, node_buffer)?;
    }

    // Check if there is enough space on the last page for the tuple.
    let no_space = {
        let nb = node_buffer.borrow();
        let page = nb.pageBuffer.as_ref().expect("pageBuffer loaded");
        page_no_space(page, itup)
    };
    if no_space {
        // Nope. Swap previous block to disk and allocate a new one.
        let block = {
            let nb = node_buffer.borrow();
            serialize_page(nb.pageBuffer.as_ref().expect("pageBuffer loaded"))
        };
        let blkno = gistBuffersGetFreeBlock(gfbb);
        WriteTempFileBlock(gfbb, blkno, &block)?;

        // Reset the in-memory page as empty, and link the previous block to the
        // new page by storing its block number in the prev-link.
        let mut nb = node_buffer.borrow_mut();
        let page = nb.pageBuffer.as_mut().expect("pageBuffer loaded");
        page.freespace = (BLCKSZ - maxalign(8)) as u32;
        page.prev = blkno as BlockNumber;

        // We've just added one more page.
        nb.blocksCount += 1;
    }

    // Place the tuple.
    {
        let mut nb = node_buffer.borrow_mut();
        let page = nb.pageBuffer.as_mut().expect("pageBuffer loaded");
        gistPlaceItupToPage(page, itup);
    }

    // If the buffer just overflowed, add it to the emptying queue.
    let should_queue = {
        let nb = node_buffer.borrow();
        buffer_half_filled(&nb, gfbb) && !nb.queuedForEmptying
    };
    if should_queue {
        gfbb.bufferEmptyingQueue.insert(0, Rc::clone(node_buffer));
        node_buffer.borrow_mut().queuedForEmptying = true;
    }

    Ok(())
}

// ===========================================================================
// gistPopItupFromNodeBuffer (gistbuildbuffers.c:405)
// ===========================================================================

/// `gistPopItupFromNodeBuffer(gfbb, nodeBuffer, itup)` (gistbuildbuffers.c:405):
/// remove one index tuple from a node buffer. Returns the tuple bytes
/// (`Some`) on success, or `None` if the buffer is empty.
pub fn gistPopItupFromNodeBuffer<'mcx>(
    mcx: Mcx<'mcx>,
    gfbb: &mut GISTBuildBuffers<'mcx>,
    node_buffer: &SharedNodeBuffer,
) -> PgResult<Option<::mcx::PgVec<'mcx, u8>>> {
    // If node buffer is empty then return false.
    if node_buffer.borrow().blocksCount <= 0 {
        return Ok(None);
    }

    // Load last page of node buffer if needed.
    if node_buffer.borrow().pageBuffer.is_none() {
        gistLoadNodeBuffer(gfbb, node_buffer)?;
    }

    // Get index tuple from last non-empty page.
    let itup = {
        let mut nb = node_buffer.borrow_mut();
        let page = nb.pageBuffer.as_mut().expect("pageBuffer loaded");
        gistGetItupFromPage(mcx, page)?
    };

    // If we just removed the last tuple from the page, fetch the previous page
    // on this node buffer (if any).
    let now_empty = {
        let nb = node_buffer.borrow();
        page_is_empty(nb.pageBuffer.as_ref().expect("pageBuffer loaded"))
    };
    if now_empty {
        // blocksCount includes the page in pageBuffer, so decrease it now.
        node_buffer.borrow_mut().blocksCount -= 1;

        // If there's more pages, fetch the previous one.
        let prevblkno = node_buffer
            .borrow()
            .pageBuffer
            .as_ref()
            .expect("pageBuffer loaded")
            .prev;
        if prevblkno != InvalidBlockNumber {
            // There is a previous page. Fetch it.
            debug_assert!(node_buffer.borrow().blocksCount > 0);
            let mut block = [0u8; BLCKSZ];
            ReadTempFileBlock(gfbb, prevblkno as i64, &mut block)?;
            {
                let mut nb = node_buffer.borrow_mut();
                let page = nb.pageBuffer.as_mut().expect("pageBuffer loaded");
                deserialize_page(page, &block);
            }

            // Now that we've read the block in memory, we can release its
            // on-disk block for reuse.
            gistBuffersReleaseBlock(gfbb, prevblkno as i64);
        } else {
            // No more pages. Free memory.
            debug_assert_eq!(node_buffer.borrow().blocksCount, 0);
            node_buffer.borrow_mut().pageBuffer = None;
        }
    }

    Ok(Some(itup))
}

// ===========================================================================
// gistBuffersGetFreeBlock / gistBuffersReleaseBlock (gistbuildbuffers.c:467/484)
// ===========================================================================

/// `gistBuffersGetFreeBlock(gfbb)` (gistbuildbuffers.c:467): select a currently
/// unused temp-file block (popping the freelist, else extending the file).
fn gistBuffersGetFreeBlock(gfbb: &mut GISTBuildBuffers<'_>) -> i64 {
    // If there are multiple free blocks, select the one appearing last in
    // freeBlocks[]. If there are none, assign the next block at the end of the
    // file (causing the file to be extended).
    if let Some(blk) = gfbb.freeBlocks.pop() {
        blk
    } else {
        let blk = gfbb.nFileBlocks;
        gfbb.nFileBlocks += 1;
        blk
    }
}

/// `gistBuffersReleaseBlock(gfbb, blocknum)` (gistbuildbuffers.c:484): return a
/// block number to the freelist.
fn gistBuffersReleaseBlock(gfbb: &mut GISTBuildBuffers<'_>, blocknum: i64) {
    gfbb.freeBlocks.push(blocknum);
}

// ===========================================================================
// gistFreeBuildBuffers (gistbuildbuffers.c:506)
// ===========================================================================

/// `gistFreeBuildBuffers(gfbb)` (gistbuildbuffers.c:506): free the buffering
/// build data structure (closing the temp file). All other things are freed
/// when the memory context is released; here `gfbb` is dropped by the caller.
pub fn gistFreeBuildBuffers(gfbb: &mut GISTBuildBuffers<'_>) -> PgResult<()> {
    // Close buffers file.
    if let Some(pfile) = gfbb.pfile.take() {
        buffile::buf_file_close::call(pfile)?;
    }
    Ok(())
}

// ===========================================================================
// gistRelocateBuildBuffersOnSplit (gistbuildbuffers.c:532)
// ===========================================================================

use ::gist_core::gistutil::{gistDeCompressAtt, gistgetadjusted, gistpenalty};
use gist::{GISTENTRY, GISTPageSplitInfo, GISTSTATE};

use bufmgr_seams as bufmgr;

/// Per-page relocation info (`RelocationBufferInfo`, the file-local struct in
/// gistRelocateBuildBuffersOnSplit). `entry`/`isnull` are the decompressed
/// parent index tuple of the page's node buffer; `nodeBuffer` is the (possibly
/// new) node buffer for the page; `split_idx` indexes into `splitinfo`.
struct RelocationBufferInfo<'mcx> {
    entry: alloc::vec::Vec<GISTENTRY<'mcx>>,
    isnull: alloc::vec::Vec<bool>,
    nodeBuffer: SharedNodeBuffer,
    split_idx: usize,
}

/// `gistRelocateBuildBuffersOnSplit(gfbb, giststate, r, level, buffer,
/// splitinfo)` (gistbuildbuffers.c:532): at a page split, distribute tuples from
/// the buffer of the split page to new buffers for the created page halves,
/// also adjusting the `splitinfo` downlinks to include the buffered tuples.
pub fn gistRelocateBuildBuffersOnSplit<'mcx>(
    mcx: Mcx<'mcx>,
    gfbb: &mut GISTBuildBuffers<'mcx>,
    giststate: &GISTSTATE<'mcx>,
    r: &Relation<'mcx>,
    level: i32,
    buffer: types_storage::Buffer,
    splitinfo: &mut [GISTPageSplitInfo<'mcx>],
) -> PgResult<()> {
    // If the split page doesn't have buffers, we have nothing to do.
    if !level_has_buffers(level, gfbb) {
        return Ok(());
    }

    // Get the node buffer of the split page.
    let blocknum = bufmgr::buffer_get_block_number::call(buffer);
    let node_buffer = match gfbb.nodeBuffersTab.get(&blocknum) {
        Some(nb) => Rc::clone(nb),
        // The page has no buffer, so we have nothing to do.
        None => return Ok(()),
    };

    // Make a copy of the old buffer, as we're going to reuse it as the buffer
    // for the new left page (which is on the same block as the old page). The
    // copy is a temporary buffer (isTemp == true), never in the hash table.
    debug_assert!(blocknum != ::gist::GIST_ROOT_BLKNO);
    let old_buf: SharedNodeBuffer = {
        let nb = node_buffer.borrow();
        Rc::new(RefCell::new(GISTNodeBuffer {
            nodeBlocknum: nb.nodeBlocknum,
            blocksCount: nb.blocksCount,
            pageBlocknum: nb.pageBlocknum,
            pageBuffer: nb.pageBuffer.clone(),
            queuedForEmptying: nb.queuedForEmptying,
            isTemp: true,
            level: nb.level,
        }))
    };

    // Reset the old buffer, used for the new left page from now on.
    {
        let mut nb = node_buffer.borrow_mut();
        nb.blocksCount = 0;
        nb.pageBuffer = None;
        nb.pageBlocknum = InvalidBlockNumber;
    }

    // Fill relocation buffers information for node buffers of pages produced by
    // the split.
    let split_pages_count = splitinfo.len();
    let mut relocation_infos: alloc::vec::Vec<RelocationBufferInfo<'mcx>> =
        alloc::vec::Vec::with_capacity(split_pages_count);
    for (i, si) in splitinfo.iter().enumerate() {
        // Decompress parent index tuple of node buffer page.
        let (entry, isnull) =
            gistDeCompressAtt(mcx, giststate, r, &si.downlink, InvalidBlockNumber, 0)?;

        // Create a node buffer for the page. The leftmost half is on the same
        // block as the old page before the split, so for it this returns the
        // original (now empty) buffer.
        let new_node_buffer =
            gistGetNodeBuffer(gfbb, bufmgr::buffer_get_block_number::call(si.buf), level);

        relocation_infos.push(RelocationBufferInfo {
            entry,
            isnull,
            nodeBuffer: new_node_buffer,
            split_idx: i,
        });
    }

    let nkeyatts = r.indnkeyatts() as usize;

    // Loop through all index tuples in the buffer of the page being split,
    // moving them to buffers for the new pages. Each tuple goes to the page with
    // the lowest penalty for the leading column or, on a tie, the lowest penalty
    // for the earliest column that is not tied. (Logic mirrors gistchoose.)
    while let Some(itup) = gistPopItupFromNodeBuffer(mcx, gfbb, &old_buf)? {
        let (entry, isnull) = gistDeCompressAtt(mcx, giststate, r, &itup, InvalidBlockNumber, 0)?;

        // default to using first page (shouldn't matter).
        let mut which = 0usize;

        // best_penalty[j]: best penalty seen so far for column j, -1 when not
        // yet examined. Entries right of the first -1 are undefined.
        let mut best_penalty = alloc::vec![-1.0f32; nkeyatts.max(1)];
        best_penalty[0] = -1.0;

        // Loop over possible target pages.
        for i in 0..split_pages_count {
            let mut zero_penalty = true;

            // Loop over index attributes.
            for j in 0..nkeyatts {
                // Compute penalty for this column.
                let usize_penalty = gistpenalty(
                    mcx,
                    giststate,
                    j,
                    &relocation_infos[i].entry[j],
                    relocation_infos[i].isnull[j],
                    &entry[j],
                    isnull[j],
                )?;
                if usize_penalty > 0.0 {
                    zero_penalty = false;
                }

                if best_penalty[j] < 0.0 || usize_penalty < best_penalty[j] {
                    // New best penalty for column. Tentatively select this page,
                    // record the penalty, and reset the next column's penalty to
                    // "unknown" (and indirectly all the ones to its right).
                    which = i;
                    best_penalty[j] = usize_penalty;

                    if j < nkeyatts - 1 {
                        best_penalty[j + 1] = -1.0;
                    }
                } else if best_penalty[j] == usize_penalty {
                    // Exactly as good for this column as the best seen so far;
                    // the next iteration compares the next column.
                } else {
                    // Worse for this column than the best seen so far. Skip the
                    // remaining columns and move on to the next page, if any.
                    zero_penalty = false; // so outer loop won't exit
                    break;
                }
            }

            // If we find a page with zero penalty for all columns, no need to
            // examine remaining pages.
            if zero_penalty {
                break;
            }
        }

        // "which" is the page index to push the tuple to.
        // Push item to selected node buffer.
        let target_node_buffer = Rc::clone(&relocation_infos[which].nodeBuffer);
        gistPushItupToNodeBuffer(gfbb, &target_node_buffer, &itup)?;

        // Adjust the downlink for this page, if needed.
        let target_split_idx = relocation_infos[which].split_idx;
        let newtup = gistgetadjusted(mcx, r, &splitinfo[target_split_idx].downlink, &itup, giststate)?;
        if let Some(newtup) = newtup {
            let (entry, isnull) =
                gistDeCompressAtt(mcx, giststate, r, &newtup, InvalidBlockNumber, 0)?;
            relocation_infos[which].entry = entry;
            relocation_infos[which].isnull = isnull;
            splitinfo[target_split_idx].downlink = newtup;
        }
    }

    Ok(())
}

// ===========================================================================
// ReadTempFileBlock / WriteTempFileBlock (gistbuildbuffers.c:749/757)
// ===========================================================================

/// `ReadTempFileBlock(file, blknum, ptr)` (gistbuildbuffers.c:749): seek to
/// `blknum` and read a `BLCKSZ` block, reporting errors with ereport.
fn ReadTempFileBlock(gfbb: &mut GISTBuildBuffers<'_>, blknum: i64, buf: &mut [u8]) -> PgResult<()> {
    let pfile = gfbb.pfile.as_mut().expect("gfbb temp file open");
    if buffile::buf_file_seek_block::call(pfile, blknum)? != 0 {
        return Err(::types_error::PgError::error(&alloc::format!(
            "could not seek to block {blknum} in temporary file"
        )));
    }
    buffile::buf_file_read_exact::call(pfile, buf)
}

/// `WriteTempFileBlock(file, blknum, ptr)` (gistbuildbuffers.c:757): seek to
/// `blknum` and write a `BLCKSZ` block, reporting errors with ereport.
fn WriteTempFileBlock(gfbb: &mut GISTBuildBuffers<'_>, blknum: i64, buf: &[u8]) -> PgResult<()> {
    let pfile = gfbb.pfile.as_mut().expect("gfbb temp file open");
    if buffile::buf_file_seek_block::call(pfile, blknum)? != 0 {
        return Err(::types_error::PgError::error(&alloc::format!(
            "could not seek to block {blknum} in temporary file"
        )));
    }
    buffile::buf_file_write::call(pfile, buf)
}
