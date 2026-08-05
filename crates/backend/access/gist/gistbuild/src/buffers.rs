//! gistbuildbuffers.c: node buffer management for the buffered GiST build.

use std::collections::{HashMap, VecDeque};

use ::bufmgr_seams::BufferPin;
use ::fd::buffile::{BufFile, BufFileCreateTemp};
use ::mcx::Mcx;
use ::types_core::{BlockNumber, InvalidBlockNumber, BLCKSZ};
use ::types_error::PgResult;
use ::types_gist::{GISTENTRY, GIST_ROOT_BLKNO};
use ::types_rel::Relation;

use bulkwrite::AlignedPage;
use gist::insert::GISTPageSplitInfo;
use gist::state::GistState;
use gist::util::{gistDeCompressAtt, gistgetadjusted, gistpenalty, index_tuple_size};

// BUFFER_PAGE_DATA_OFFSET: MAXALIGN(offsetof(GISTNodeBufferPage, tupledata)).
const BUFFER_PAGE_DATA_OFFSET: usize = 8;

const fn maxalign(n: usize) -> usize {
    (n + 7) & !7
}

// GISTNodeBufferPage layout kept byte-compatible with C so pages round-trip
// through the temp file in BLCKSZ blocks: prev(u32) | freespace(u32) | data,
// tuples stacked downward from the end of the page.
fn page_prev(p: &AlignedPage) -> BlockNumber {
    u32::from_ne_bytes(p.0[0..4].try_into().expect("4 bytes"))
}

fn page_set_prev(p: &mut AlignedPage, v: BlockNumber) {
    p.0[0..4].copy_from_slice(&v.to_ne_bytes());
}

fn page_freespace(p: &AlignedPage) -> usize {
    u32::from_ne_bytes(p.0[4..8].try_into().expect("4 bytes")) as usize
}

fn page_set_freespace(p: &mut AlignedPage, v: usize) {
    p.0[4..8].copy_from_slice(&(v as u32).to_ne_bytes());
}

fn page_is_empty(p: &AlignedPage) -> bool {
    page_freespace(p) == BLCKSZ - BUFFER_PAGE_DATA_OFFSET
}

/// gistAllocateNewPageBuffer.
fn new_page_buffer() -> Box<AlignedPage> {
    let mut p = Box::new(AlignedPage([0u8; BLCKSZ]));
    page_set_prev(&mut p, InvalidBlockNumber);
    page_set_freespace(&mut p, BLCKSZ - BUFFER_PAGE_DATA_OFFSET);
    p
}

/// gistPlaceItupToPage.
fn place_itup_to_page(p: &mut AlignedPage, itup: &[u8]) {
    let need = maxalign(itup.len());
    debug_assert!(page_freespace(p) >= need);
    let fs = page_freespace(p) - need;
    page_set_freespace(p, fs);
    let off = BUFFER_PAGE_DATA_OFFSET + fs;
    p.0[off..off + itup.len()].copy_from_slice(itup);
}

/// gistGetItupFromPage.
fn get_itup_from_page(p: &mut AlignedPage) -> Vec<u8> {
    debug_assert!(!page_is_empty(p));
    let fs = page_freespace(p);
    let off = BUFFER_PAGE_DATA_OFFSET + fs;
    // SAFETY: tuple image written whole by place_itup_to_page.
    let sz = unsafe { index_tuple_size(p.0[off..].as_ptr()) };
    let itup = p.0[off..off + sz].to_vec();
    page_set_freespace(p, fs + maxalign(sz));
    itup
}

/// GISTNodeBuffer; identified by its index block number (the node_buffers
/// key) instead of C's hash-entry pointer.
pub struct NodeBuffer {
    pub blocks_count: i32,
    page_blocknum: Option<i64>,
    page_buffer: Option<Box<AlignedPage>>,
    pub queued_for_emptying: bool,
    pub level: i32,
}

/// GISTBuildBuffers.
pub struct GistBuildBuffers<'mcx> {
    pfile: BufFile<'mcx>,
    n_file_blocks: i64,
    free_blocks: Vec<i64>,
    pub node_buffers: HashMap<BlockNumber, NodeBuffer>,
    pub buffer_emptying_queue: VecDeque<BlockNumber>,
    pub level_step: i32,
    pub pages_per_buffer: i32,
    pub buffers_on_levels: Vec<VecDeque<BlockNumber>>,
    loaded_buffers: Vec<BlockNumber>,
    pub rootlevel: i32,
}

impl<'mcx> GistBuildBuffers<'mcx> {
    /// gistInitBuildBuffers.
    pub fn new(
        mcx: Mcx<'mcx>,
        pages_per_buffer: i32,
        level_step: i32,
        max_level: i32,
    ) -> PgResult<Self> {
        Ok(GistBuildBuffers {
            pfile: BufFileCreateTemp(mcx, false)?,
            n_file_blocks: 0,
            free_blocks: Vec::with_capacity(32),
            node_buffers: HashMap::new(),
            buffer_emptying_queue: VecDeque::new(),
            level_step,
            pages_per_buffer,
            buffers_on_levels: vec![VecDeque::new()],
            loaded_buffers: Vec::with_capacity(32),
            rootlevel: max_level,
        })
    }

    /// LEVEL_HAS_BUFFERS.
    pub fn level_has_buffers(&self, level: i32) -> bool {
        level != 0 && level % self.level_step == 0 && level != self.rootlevel
    }

    /// gistGetNodeBuffer.
    pub fn get_node_buffer(&mut self, node_blkno: BlockNumber, level: i32) {
        if self.node_buffers.contains_key(&node_blkno) {
            return;
        }
        self.node_buffers.insert(
            node_blkno,
            NodeBuffer {
                blocks_count: 0,
                page_blocknum: None,
                page_buffer: None,
                queued_for_emptying: false,
                level,
            },
        );
        let level = level as usize;
        if level >= self.buffers_on_levels.len() {
            self.buffers_on_levels.resize_with(level + 1, VecDeque::new);
        }
        // Prepend: split-created buffers get flushed first in final emptying.
        self.buffers_on_levels[level].push_front(node_blkno);
    }

    /// gistPushItupToNodeBuffer (queues the buffer for emptying when it
    /// crosses BUFFER_HALF_FILLED).
    pub fn push_itup(&mut self, node_blkno: BlockNumber, itup: &[u8]) -> PgResult<()> {
        let mut nb = self
            .node_buffers
            .remove(&node_blkno)
            .expect("node buffer exists");
        let res = self.push_itup_inner(&mut nb, Some(node_blkno), itup);
        if res.is_ok()
            && nb.blocks_count > self.pages_per_buffer / 2
            && !nb.queued_for_emptying
        {
            self.buffer_emptying_queue.push_front(node_blkno);
            nb.queued_for_emptying = true;
        }
        self.node_buffers.insert(node_blkno, nb);
        res
    }

    fn push_itup_inner(
        &mut self,
        nb: &mut NodeBuffer,
        key: Option<BlockNumber>,
        itup: &[u8],
    ) -> PgResult<()> {
        if nb.blocks_count == 0 {
            nb.page_buffer = Some(new_page_buffer());
            nb.blocks_count = 1;
            if let Some(k) = key {
                self.loaded_buffers.push(k);
            }
        }
        if nb.page_buffer.is_none() {
            self.load_node_buffer(nb, key)?;
        }
        if page_freespace(nb.page_buffer.as_ref().expect("loaded")) < maxalign(itup.len()) {
            let blkno = self.get_free_block();
            self.write_block(blkno, nb.page_buffer.as_ref().expect("loaded"))?;
            let page = nb.page_buffer.as_mut().expect("loaded");
            page_set_freespace(page, BLCKSZ - BUFFER_PAGE_DATA_OFFSET);
            page_set_prev(page, blkno as BlockNumber);
            nb.blocks_count += 1;
        }
        place_itup_to_page(nb.page_buffer.as_mut().expect("loaded"), itup);
        Ok(())
    }

    /// gistPopItupFromNodeBuffer; None when the buffer is empty.
    pub fn pop_itup(&mut self, node_blkno: BlockNumber) -> PgResult<Option<Vec<u8>>> {
        let mut nb = self
            .node_buffers
            .remove(&node_blkno)
            .expect("node buffer exists");
        let res = self.pop_itup_inner(&mut nb, Some(node_blkno));
        self.node_buffers.insert(node_blkno, nb);
        res
    }

    fn pop_itup_inner(
        &mut self,
        nb: &mut NodeBuffer,
        key: Option<BlockNumber>,
    ) -> PgResult<Option<Vec<u8>>> {
        if nb.blocks_count <= 0 {
            return Ok(None);
        }
        if nb.page_buffer.is_none() {
            self.load_node_buffer(nb, key)?;
        }
        let itup = get_itup_from_page(nb.page_buffer.as_mut().expect("loaded"));
        if page_is_empty(nb.page_buffer.as_ref().expect("loaded")) {
            nb.blocks_count -= 1;
            let prevblkno = page_prev(nb.page_buffer.as_ref().expect("loaded"));
            if prevblkno != InvalidBlockNumber {
                debug_assert!(nb.blocks_count > 0);
                let mut page = nb.page_buffer.take().expect("loaded");
                self.read_block(prevblkno as i64, &mut page)?;
                self.release_block(prevblkno as i64);
                nb.page_buffer = Some(page);
            } else {
                debug_assert!(nb.blocks_count == 0);
                nb.page_buffer = None;
            }
        }
        Ok(Some(itup))
    }

    /// gistLoadNodeBuffer; key None = temporary copy (kept out of
    /// loaded_buffers, C's isTemp).
    fn load_node_buffer(&mut self, nb: &mut NodeBuffer, key: Option<BlockNumber>) -> PgResult<()> {
        if nb.page_buffer.is_none() && nb.blocks_count > 0 {
            let mut page = new_page_buffer();
            let blkno = nb.page_blocknum.take().expect("unloaded buffer has a file block");
            self.read_block(blkno, &mut page)?;
            self.release_block(blkno);
            nb.page_buffer = Some(page);
            if let Some(k) = key {
                self.loaded_buffers.push(k);
            }
        }
        Ok(())
    }

    /// gistUnloadNodeBuffers.
    pub fn unload_node_buffers(&mut self) -> PgResult<()> {
        let loaded = std::mem::take(&mut self.loaded_buffers);
        for node_blkno in loaded {
            let nb = self
                .node_buffers
                .get_mut(&node_blkno)
                .expect("node buffer exists");
            if let Some(page) = nb.page_buffer.take() {
                let blkno = self.get_free_block();
                self.write_block(blkno, &page)?;
                self.node_buffers
                    .get_mut(&node_blkno)
                    .expect("node buffer exists")
                    .page_blocknum = Some(blkno);
            }
        }
        Ok(())
    }

    /// gistBuffersGetFreeBlock.
    fn get_free_block(&mut self) -> i64 {
        match self.free_blocks.pop() {
            Some(b) => b,
            None => {
                let b = self.n_file_blocks;
                self.n_file_blocks += 1;
                b
            }
        }
    }

    /// gistBuffersReleaseBlock.
    fn release_block(&mut self, blocknum: i64) {
        self.free_blocks.push(blocknum);
    }

    /// WriteTempFileBlock.
    fn write_block(&mut self, blknum: i64, page: &AlignedPage) -> PgResult<()> {
        if self.pfile.seek_block(blknum)? != 0 {
            panic!("could not seek to block {blknum} in temporary file");
        }
        self.pfile.write(&page.0)
    }

    /// ReadTempFileBlock.
    fn read_block(&mut self, blknum: i64, page: &mut AlignedPage) -> PgResult<()> {
        if self.pfile.seek_block(blknum)? != 0 {
            panic!("could not seek to block {blknum} in temporary file");
        }
        self.pfile.read_exact(&mut page.0)
    }

    /// gistFreeBuildBuffers.
    pub fn free(self) -> PgResult<()> {
        self.pfile.close()
    }
}

/// gistRelocateBuildBuffersOnSplit: redistribute the split page's buffered
/// tuples to the halves' buffers and fold them into the downlinks.
pub fn gistRelocateBuildBuffersOnSplit<'m>(
    mcx: Mcx<'m>,
    gfbb: &mut GistBuildBuffers<'_>,
    giststate: &mut GistState<'_>,
    r: &Relation<'_>,
    level: i32,
    buffer: &BufferPin,
    splitinfo: &mut [GISTPageSplitInfo<'m>],
) -> PgResult<()> {
    const K: usize = ::types_core::fmgr::INDEX_MAX_KEYS as usize;

    if !gfbb.level_has_buffers(level) {
        return Ok(());
    }

    let blocknum = buffer.block_number();
    let Some(nb) = gfbb.node_buffers.get_mut(&blocknum) else {
        return Ok(());
    };

    // The original hash entry becomes the new left page's buffer; the old
    // contents move to a temporary copy that we drain below.
    debug_assert!(blocknum != GIST_ROOT_BLKNO);
    let mut old_buf = NodeBuffer {
        blocks_count: nb.blocks_count,
        page_blocknum: nb.page_blocknum.take(),
        page_buffer: nb.page_buffer.take(),
        queued_for_emptying: nb.queued_for_emptying,
        level: nb.level,
    };
    nb.blocks_count = 0;

    struct RelocationBufferInfo {
        entry: [GISTENTRY; K],
        isnull: [bool; K],
        node_blkno: BlockNumber,
    }

    let nkeyatts = r.indnkeyatts() as usize;
    let mut infos: Vec<RelocationBufferInfo> = Vec::with_capacity(splitinfo.len());
    for si in splitinfo.iter() {
        let mut entry = [GISTENTRY::default(); K];
        let mut isnull = [false; K];
        gistDeCompressAtt(mcx, giststate, r, si.downlink.as_ptr(), 0, &mut entry, &mut isnull)?;
        let si_blkno = ::bufmgr_seams::buffer_get_block_number::call(si.buf);
        gfbb.get_node_buffer(si_blkno, level);
        infos.push(RelocationBufferInfo {
            entry,
            isnull,
            node_blkno: si_blkno,
        });
    }

    while let Some(itup) = gfbb.pop_itup_inner(&mut old_buf, None)? {
        let mut entry = [GISTENTRY::default(); K];
        let mut isnull = [false; K];
        gistDeCompressAtt(mcx, giststate, r, itup.as_ptr(), 0, &mut entry, &mut isnull)?;

        let mut which = 0usize;
        let mut best_penalty = [-1.0f32; K];
        best_penalty[0] = -1.0;

        for (i, split_page_info) in infos.iter().enumerate() {
            let mut zero_penalty = true;
            let mut j = 0usize;
            while j < nkeyatts {
                let usize_ = gistpenalty(
                    mcx,
                    giststate,
                    j,
                    &split_page_info.entry[j],
                    split_page_info.isnull[j],
                    &entry[j],
                    isnull[j],
                )?;
                if usize_ > 0.0 {
                    zero_penalty = false;
                }

                if best_penalty[j] < 0.0 || usize_ < best_penalty[j] {
                    which = i;
                    best_penalty[j] = usize_;
                    if j < nkeyatts - 1 {
                        best_penalty[j + 1] = -1.0;
                    }
                    j += 1;
                } else if best_penalty[j] == usize_ {
                    j += 1;
                } else {
                    zero_penalty = false;
                    break;
                }
            }

            if zero_penalty {
                break;
            }
        }

        gfbb.push_itup(infos[which].node_blkno, &itup)?;

        let newtup = gistgetadjusted(
            mcx,
            r,
            splitinfo[which].downlink.as_ptr(),
            itup.as_ptr(),
            giststate,
        )?;
        if let Some(newtup) = newtup {
            let target = &mut infos[which];
            gistDeCompressAtt(
                mcx,
                giststate,
                r,
                newtup.as_ptr(),
                0,
                &mut target.entry,
                &mut target.isnull,
            )?;
            splitinfo[which].downlink = newtup;
        }
    }

    Ok(())
}
