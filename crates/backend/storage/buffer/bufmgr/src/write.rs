use core::cell::RefCell;
use core::sync::atomic::Ordering;

use init_small::globals;
use lwlock::{LWLockAcquire, LWLockHeldByMe, LWLockRelease, LW_SHARED};
use types_core::{BlockNumber, Buffer, Oid, BLCKSZ, INVALID_PROC_NUMBER};
use types_error::PgResult;
use pgstat::io::{
    pgstat_count_io_op_time, pgstat_prepare_io_time, IOObject, IOOp,
};
use types_storage::buf::{
    buftag, IOContext, BM_CHECKPOINT_NEEDED, BM_DIRTY, BM_IO_ERROR, BM_JUST_DIRTIED, BM_PERMANENT,
    BM_VALID,
};
use types_storage::{RelFileLocator, RelFileLocatorBackend, IO_DIRECT_DATA};

use crate::buf_hdr::{BufferGetBlockPtr, GetBufferDescriptor, LockBufHdr, UnlockBufHdr};
use crate::counters;
use crate::pin::{
    buffer_refcount, buffer_usagecount, BufferIsPinned, PinBuffer_Locked, UnpinBuffer,
};
use crate::privref::ReservePrivateRefCountEntry;
use crate::read::{StartBufferIO, TerminateBufferIO};

const CHECKPOINT_IS_SHUTDOWN: i32 = 0x0001;
const CHECKPOINT_END_OF_RECOVERY: i32 = 0x0002;
const CHECKPOINT_FLUSH_ALL: i32 = 0x0010;

pub(crate) const BUF_WRITTEN: i32 = 0x01;
pub(crate) const BUF_REUSABLE: i32 = 0x02;

const WRITEBACK_MAX_PENDING_FLUSHES: usize = 256;

pub struct WritebackContext {
    max_pending: fn() -> i32,
    nr_pending: usize,
    pending: [buftag; WRITEBACK_MAX_PENDING_FLUSHES],
}

impl WritebackContext {
    pub(crate) fn new(max_pending: fn() -> i32) -> Self {
        WritebackContext {
            max_pending,
            nr_pending: 0,
            pending: [buftag::default(); WRITEBACK_MAX_PENDING_FLUSHES],
        }
    }

    pub fn for_checkpoint() -> Self {
        Self::new(crate::gucs::checkpoint_flush_after)
    }
}

thread_local! {
    // C's BackendWritebackContext (one per backend, backend_flush_after).
    static BACKEND_WB: RefCell<Option<WritebackContext>> = const { RefCell::new(None) };
}

pub(crate) fn schedule_backend_writeback(io_context: IOContext, tag: &buftag) -> PgResult<()> {
    BACKEND_WB.with(|cell| {
        let mut slot = cell.borrow_mut();
        let wb = slot.get_or_insert_with(|| WritebackContext::new(crate::gucs::backend_flush_after));
        ScheduleBufferTagForWriteback(wb, io_context, tag)
    })
}

fn tag_locator(tag: &buftag) -> RelFileLocatorBackend {
    RelFileLocatorBackend {
        locator: RelFileLocator {
            spcOid: tag.spcOid,
            dbOid: tag.dbOid,
            relNumber: tag.relNumber,
        },
        backend: INVALID_PROC_NUMBER,
    }
}

pub fn ScheduleBufferTagForWriteback(
    wb: &mut WritebackContext,
    io_context: IOContext,
    tag: &buftag,
) -> PgResult<()> {
    if fd::io_direct_flags() & IO_DIRECT_DATA != 0 || !globals::enableFsync() {
        return Ok(());
    }
    let max_pending = (wb.max_pending)().max(0) as usize;
    debug_assert!(max_pending <= WRITEBACK_MAX_PENDING_FLUSHES);
    if max_pending > 0 {
        wb.pending[wb.nr_pending] = *tag;
        wb.nr_pending += 1;
    }
    if wb.nr_pending >= max_pending {
        IssuePendingWritebacks(wb, io_context)?;
    }
    Ok(())
}

fn rlocator_cmp(a: &buftag, b: &buftag) -> core::cmp::Ordering {
    (a.relNumber, a.dbOid, a.spcOid).cmp(&(b.relNumber, b.dbOid, b.spcOid))
}

pub fn IssuePendingWritebacks(wb: &mut WritebackContext, io_context: IOContext) -> PgResult<()> {
    if wb.nr_pending == 0 {
        return Ok(());
    }
    let pending = &mut wb.pending[..wb.nr_pending];
    pending.sort_unstable_by(|a, b| {
        rlocator_cmp(a, b)
            .then((a.forkNum as i32).cmp(&(b.forkNum as i32)))
            .then(a.blockNum.cmp(&b.blockNum))
    });

    let io_start = pgstat_prepare_io_time(crate::gucs::track_io_timing());

    let mut i = 0;
    while i < pending.len() {
        let start = pending[i];
        let mut last_block = start.blockNum;
        let mut nblocks: BlockNumber = 1;
        let mut ahead = 0;
        while i + ahead + 1 < pending.len() {
            let next = &pending[i + ahead + 1];
            if rlocator_cmp(&start, next) != core::cmp::Ordering::Equal
                || start.forkNum != next.forkNum
            {
                break;
            }
            if next.blockNum == last_block {
                ahead += 1;
                continue;
            }
            if next.blockNum != last_block + 1 {
                break;
            }
            nblocks += 1;
            last_block = next.blockNum;
            ahead += 1;
        }
        i += ahead + 1;

        smgr_seams::smgr_writeback::call(
            tag_locator(&start),
            start.forkNum,
            start.blockNum,
            nblocks,
        )?;
    }
    pgstat_count_io_op_time(
        IOObject::Relation,
        io_context,
        IOOp::Writeback,
        io_start,
        wb.nr_pending as u32,
        0,
    );
    wb.nr_pending = 0;
    Ok(())
}

pub(crate) mod checksum {
    use types_core::BLCKSZ;

    const N_SUMS: usize = 32;
    const FNV_PRIME: u32 = 16777619;
    const CHECKSUM_BASE_OFFSETS: [u32; N_SUMS] = [
        0x5B1F36E9, 0xB8525960, 0x02AB50AA, 0x1DE66D2A, 0x79FF467A, 0x9BB9F8A3, 0x217E7CD2,
        0x83E13D2C, 0xF8D4474F, 0xE39EB970, 0x42C6AE16, 0x993216FA, 0x7B093B5D, 0x98DAFF3C,
        0xF718902A, 0x0B1C9CDB, 0xE58F764B, 0x187636BC, 0x5D7B3BB1, 0xE73DE7DE, 0x92BEC979,
        0xCCA6C0B2, 0x304A0979, 0x85AA43D4, 0x783125BB, 0x6CA8EAA2, 0xE407EAC6, 0x4B5CFC3E,
        0x9FBF8C76, 0x15CA20BE, 0xF2CA9FD3, 0x959BD756,
    ];

    #[inline(always)]
    fn comp(sum: &mut u32, value: u32) {
        let tmp = *sum ^ value;
        *sum = tmp.wrapping_mul(FNV_PRIME) ^ (tmp >> 17);
    }

    // Caller must have zeroed pd_checksum (bytes 8..10) in `page`.
    pub(super) fn page_checksum(page: &[u8; BLCKSZ], blkno: u32) -> u16 {
        let mut sums = CHECKSUM_BASE_OFFSETS;
        let rows = BLCKSZ / (4 * N_SUMS);
        for row in 0..rows {
            for (lane, sum) in sums.iter_mut().enumerate() {
                let off = (row * N_SUMS + lane) * 4;
                let v = u32::from_ne_bytes(page[off..off + 4].try_into().unwrap());
                comp(sum, v);
            }
        }
        for _ in 0..2 {
            for sum in sums.iter_mut() {
                comp(sum, 0);
            }
        }
        let mut checksum: u32 = sums.into_iter().fold(0, |a, s| a ^ s);
        checksum ^= blkno;
        (checksum % 65535 + 1) as u16
    }

    // pg_checksum_page (checksum_impl.h) over a raw, possibly shared page
    // image: pd_checksum (bytes 8..10) is computed as zero so the stored
    // value doesn't feed itself. C temporarily zeroes the field in place;
    // the verify side here may not own the image exclusively (uring lane),
    // so mask the word instead of mutating.
    //
    // # Safety
    // `page` must be readable for BLCKSZ bytes.
    pub(crate) unsafe fn page_checksum_raw(page: *const u8, blkno: u32) -> u16 {
        let mut sums = CHECKSUM_BASE_OFFSETS;
        let rows = BLCKSZ / (4 * N_SUMS);
        for row in 0..rows {
            for (lane, sum) in sums.iter_mut().enumerate() {
                let off = (row * N_SUMS + lane) * 4;
                // SAFETY: off < BLCKSZ, caller contract (unaligned read:
                // page images are aligned in practice, fixtures may not be).
                let mut v = unsafe { page.add(off).cast::<u32>().read_unaligned() };
                if off == 8 {
                    v &= u32::from_ne_bytes([0, 0, 0xff, 0xff]);
                }
                comp(sum, v);
            }
        }
        for _ in 0..2 {
            for sum in sums.iter_mut() {
                comp(sum, 0);
            }
        }
        let mut checksum: u32 = sums.into_iter().fold(0, |a, s| a ^ s);
        checksum ^= blkno;
        (checksum % 65535 + 1) as u16
    }
}

/// PageSetChecksumInplace (bufpage.c).
pub fn PageSetChecksumInplace(page: &mut [u8; BLCKSZ], blkno: BlockNumber) {
    let is_new = u16::from_ne_bytes([page[14], page[15]]) == 0;
    if is_new || !transam_xlog_seams::data_checksums_enabled::call() {
        return;
    }
    page[8..10].copy_from_slice(&0u16.to_ne_bytes());
    let sum = checksum::page_checksum(page, blkno);
    page[8..10].copy_from_slice(&sum.to_ne_bytes());
}

// PageSetChecksumCopy's static pageCopy (bufpage.c): hint bits may change
// under a share lock, so the checksummed image is built in private storage.
#[repr(align(4096))]
struct PageCopy([u8; BLCKSZ]);

thread_local! {
    static PAGE_COPY: RefCell<PageCopy> = const { RefCell::new(PageCopy([0; BLCKSZ])) };
}

// PageSetChecksumCopy / the no-checksum straight write (bufpage.c), over a
// page image that is LIVE SHARED MEMORY: the flush path holds only a SHARE
// content lock (every FlushBuffer caller here takes LW_SHARED), and
// SetHintBits mutates t_infomask in this same image under that same SHARE
// lock, then calls MarkBufferDirtyHint. So the image must be handled through
// a type that permits a concurrent writer: WriteChunk, whose bytes are read
// only by the kernel via the iovec it lowers to (types_storage::writechunk
// carries the full argument). C's contract is identical and its `char *`
// says nothing, which is why the two arms differ from C only in typing.
pub(crate) fn with_checksummed_page<R>(
    src: *const u8,
    blkno: BlockNumber,
    f: impl FnOnce(types_storage::WriteChunk<'_>) -> R,
) -> R {
    // PageIsNew: pd_upper (offset 14) == 0. This read is NOT part of the
    // concurrent-write problem: the two writers that run under a SHARE
    // content lock touch t_infomask (SetHintBits) and pd_lsn bytes 0..8
    // (MarkBufferDirtyHint, under the buffer header lock), never pd_upper,
    // and every writer of pd_upper holds the content lock EXCLUSIVE, which
    // our SHARE lock excludes.
    // SAFETY: caller holds a pin; the page image is BLCKSZ bytes.
    let is_new = unsafe {
        u16::from_ne_bytes([*src.add(14), *src.add(15)]) == 0
    };
    if is_new || !transam_xlog_seams::data_checksums_enabled::call() {
        // Straight write of the live shared image, as C does: no copy, no
        // checksum to invalidate, and a hint bit landing mid-write is
        // tolerated by design (the page carries no checksum to tear).
        // SAFETY: src is readable for BLCKSZ under the caller's pin and the
        // chunk does not outlive the closure. A concurrent hint-bit writer
        // is permitted: nothing reads these bytes through Rust.
        return f(unsafe { types_storage::WriteChunk::from_shared(src, BLCKSZ) });
    }
    PAGE_COPY.with(|c| {
        let mut copy = c.borrow_mut();
        // SAFETY: distinct buffers; src stays valid under the caller's pin.
        // This copy IS C's memcpy in PageSetChecksumCopy, including its
        // tolerance of a hint bit changing mid-copy: the checksum is computed
        // over whatever snapshot lands, and the shared page is left alone so
        // no torn value is ever published. (This one racing read is the
        // residual the type cannot remove — a formally race-free copy needs
        // the hint-bit *writer* to be atomic too, which is a page-memory
        // typing change well outside this seam.)
        unsafe { core::ptr::copy_nonoverlapping(src, copy.0.as_mut_ptr(), BLCKSZ) };
        copy.0[8..10].copy_from_slice(&0u16.to_ne_bytes());
        let sum = checksum::page_checksum(&copy.0, blkno);
        copy.0[8..10].copy_from_slice(&sum.to_ne_bytes());
        f(types_storage::WriteChunk::from_slice(&copy.0))
    })
}

/// FlushBuffer (bufmgr.c): caller holds a pin and the content lock (any mode).
pub(crate) fn FlushBuffer(
    desc: &crate::buf_hdr::BufferDesc,
    io_context: IOContext,
) -> PgResult<()> {
    if !StartBufferIO(desc, false, false, true)? {
        return Ok(());
    }

    let tag = desc.tag();
    let buf_state = LockBufHdr(desc);
    let block = BufferGetBlockPtr(crate::buf_hdr::BufferDescriptorGetBuffer(desc));
    // Page LSN read under the header lock: the content lock may be shared.
    // SAFETY: pinned page image; pd_lsn is two 4-aligned u32 halves.
    let recptr = unsafe {
        let xlogid = block.cast::<u32>().read();
        let xrecoff = block.add(4).cast::<u32>().read();
        ((xlogid as u64) << 32) | xrecoff as u64
    };
    UnlockBufHdr(desc, buf_state & !BM_JUST_DIRTIED);

    // WAL-before-data; unlogged pages can carry fake LSNs past insert.
    if buf_state & BM_PERMANENT != 0 {
        if let Err(e) = transam_xlog_seams::xlog_flush::call(recptr) {
            TerminateBufferIO(desc, false, BM_IO_ERROR, true, false);
            return Err(e);
        }
    }

    let io_start = pgstat_prepare_io_time(crate::gucs::track_io_timing());
    let write_result = with_checksummed_page(block, tag.blockNum, |page| {
        smgr_seams::smgr_write::call(tag_locator(&tag), tag.forkNum, tag.blockNum, page, false)
    });
    if let Err(e) = write_result {
        TerminateBufferIO(desc, false, BM_IO_ERROR, true, false);
        return Err(e);
    }

    pgstat_count_io_op_time(
        IOObject::Relation,
        io_context,
        IOOp::Write,
        io_start,
        1,
        BLCKSZ as u64,
    );
    counters::written();
    TerminateBufferIO(desc, true, 0, true, false);
    Ok(())
}

/// FlushOneBuffer (bufmgr.c): caller holds pin + content lock.
pub fn FlushOneBuffer(buffer: Buffer) -> PgResult<()> {
    assert!(buffer > 0, "FlushOneBuffer: local buffers unsupported in C too");
    debug_assert!(BufferIsPinned(buffer));
    let desc = GetBufferDescriptor(buffer - 1);
    debug_assert!(LWLockHeldByMe(&desc.content_lock));
    FlushBuffer(desc, IOContext::IOCONTEXT_NORMAL)
}

/// FlushDatabaseBuffers (bufmgr.c): write out all dirty buffers of a database.
pub fn FlushDatabaseBuffers(dbid: types_core::Oid) -> PgResult<()> {
    // Uncollected uring prefetch reads hold thread-owned pins (the startup's
    // recovery prefetcher during dbase_redo -> FlushDatabaseBuffers most of
    // all); PinBuffer_Locked requires no pre-existing private ref. Same
    // drain as DropDatabaseBuffers (032 stall: debug asserts, release
    // corrupts the pin accounting and wedges replay).
    crate::uring::drain_own();
    for i in 0..crate::buf_hdr::NBuffersInited() {
        let desc = GetBufferDescriptor(i);
        // Unlocked precheck, safe as in DropRelationBuffers (C comment).
        if desc.tag().dbOid != dbid {
            continue;
        }

        ReservePrivateRefCountEntry();
        crate::pin::resowner_enlarge_for_pin()?;

        let buf_state = LockBufHdr(desc);
        if desc.tag().dbOid == dbid && buf_state & (BM_VALID | BM_DIRTY) == (BM_VALID | BM_DIRTY) {
            PinBuffer_Locked(desc);
            LWLockAcquire(&desc.content_lock, LW_SHARED, globals::MyProcNumber())?;
            let flush_result = FlushBuffer(desc, IOContext::IOCONTEXT_NORMAL);
            LWLockRelease(&desc.content_lock)?;
            UnpinBuffer(desc);
            flush_result?;
        } else {
            UnlockBufHdr(desc, buf_state);
        }
    }
    Ok(())
}

pub fn FlushRelationsAllBuffers(rels: &[RelFileLocatorBackend]) -> PgResult<()> {
    if rels.is_empty() {
        return Ok(());
    }
    // See FlushDatabaseBuffers: no pre-existing private refs on the swept
    // buffers (uring prefetch pins).
    crate::uring::drain_own();
    for r in rels {
        assert!(
            r.backend == INVALID_PROC_NUMBER,
            "FlushRelationsAllBuffers: temp relations use local buffers (C asserts too)"
        );
    }
    let mut locators: Vec<RelFileLocator> = rels.iter().map(|r| r.locator).collect();
    let use_bsearch = locators.len() > crate::drop_buffers::RELS_BSEARCH_THRESHOLD;
    if use_bsearch {
        locators.sort_by_key(|l| (l.spcOid, l.dbOid, l.relNumber));
    }

    for i in 0..crate::buf_hdr::NBuffersInited() {
        let desc = GetBufferDescriptor(i);
        let tag = desc.tag();
        // Unlocked precheck, safe as in DropRelationBuffers (C comment).
        let rlocator = if use_bsearch {
            locators
                .binary_search_by_key(&(tag.spcOid, tag.dbOid, tag.relNumber), |l| {
                    (l.spcOid, l.dbOid, l.relNumber)
                })
                .ok()
                .map(|idx| locators[idx])
        } else {
            locators
                .iter()
                .find(|l| crate::drop_buffers::tag_matches(&tag, l))
                .copied()
        };
        let Some(rlocator) = rlocator else { continue };

        ReservePrivateRefCountEntry();
        crate::pin::resowner_enlarge_for_pin()?;

        let buf_state = LockBufHdr(desc);
        if crate::drop_buffers::tag_matches(&desc.tag(), &rlocator)
            && buf_state & (BM_VALID | BM_DIRTY) == (BM_VALID | BM_DIRTY)
        {
            PinBuffer_Locked(desc);
            LWLockAcquire(&desc.content_lock, LW_SHARED, globals::MyProcNumber())?;
            let flush_result = FlushBuffer(desc, IOContext::IOCONTEXT_NORMAL);
            LWLockRelease(&desc.content_lock)?;
            UnpinBuffer(desc);
            flush_result?;
        } else {
            UnlockBufHdr(desc, buf_state);
        }
    }
    Ok(())
}

pub(crate) fn SyncOneBuffer(buf_id: i32, skip_recently_used: bool, wb: &mut WritebackContext) -> PgResult<i32> {
    let desc = GetBufferDescriptor(buf_id);
    let mut result = 0;

    ReservePrivateRefCountEntry();
    crate::pin::resowner_enlarge_for_pin()?;

    // Dirty-marking precedes XLogInsert in the AMs, so an unlocked dirty
    // check can only over-write (see C's SyncOneBuffer header comment).
    let buf_state = LockBufHdr(desc);
    if buffer_refcount(buf_state) == 0 && buffer_usagecount(buf_state) == 0 {
        result |= BUF_REUSABLE;
    } else if skip_recently_used {
        UnlockBufHdr(desc, buf_state);
        return Ok(result);
    }
    if buf_state & BM_VALID == 0 || buf_state & BM_DIRTY == 0 {
        UnlockBufHdr(desc, buf_state);
        return Ok(result);
    }

    PinBuffer_Locked(desc);
    LWLockAcquire(&desc.content_lock, LW_SHARED, globals::MyProcNumber())?;
    let flush_result = FlushBuffer(desc, IOContext::IOCONTEXT_NORMAL);
    LWLockRelease(&desc.content_lock)?;
    let tag = desc.tag();
    UnpinBuffer(desc);
    flush_result?;

    ScheduleBufferTagForWriteback(wb, IOContext::IOCONTEXT_NORMAL, &tag)?;
    Ok(result | BUF_WRITTEN)
}

#[derive(Clone, Copy, Default)]
struct CkptSortItem {
    tsId: Oid,
    relNumber: u32,
    forkNum: i32,
    blockNum: BlockNumber,
    buf_id: i32,
}

#[derive(Clone, Copy, Default)]
struct CkptTsStatus {
    tsId: Oid,
    progress: f64,
    progress_slice: f64,
    num_to_scan: i32,
    num_scanned: i32,
    index: i32,
}

struct CkptScratch {
    items: mcx::PgVec<'static, CkptSortItem>,
    per_ts: mcx::PgVec<'static, CkptTsStatus>,
}

thread_local! {
    static CKPT_SCRATCH: RefCell<Option<CkptScratch>> = const { RefCell::new(None) };
}

fn barrier_check() -> PgResult<()> {
    if globals::ProcSignalBarrierPending() {
        procsignal_seams::process_proc_signal_barrier::call()?;
    }
    Ok(())
}

pub fn BufferSync(flags: i32) -> PgResult<()> {
    let mut mask = BM_DIRTY;
    if flags & (CHECKPOINT_IS_SHUTDOWN | CHECKPOINT_END_OF_RECOVERY | CHECKPOINT_FLUSH_ALL) == 0 {
        mask |= BM_PERMANENT;
    }

    CKPT_SCRATCH.with(|cell| -> PgResult<()> {
        let mut slot = cell.borrow_mut();
        let scratch = slot.get_or_insert_with(|| {
            let cx: &'static mcx::MemoryContext =
                mcx::session_root("checkpoint scratch");
            // LIFO: empty the droppy TLS slot before its context is freed.
            mcx::register_session_cleanup(Box::new(|| {
                CKPT_SCRATCH.with(|c| drop(c.borrow_mut().take()));
            }));
            CkptScratch {
                items: mcx::PgVec::new_in(cx.mcx()),
                per_ts: mcx::PgVec::new_in(cx.mcx()),
            }
        });

        let nbuffers = globals::NBuffers();
        scratch.items.clear();
        if scratch
            .items
            .try_reserve(nbuffers.max(0) as usize)
            .is_err()
        {
            panic!("out of memory allocating CkptBufferIds");
        }

        for buf_id in 0..nbuffers {
            let desc = GetBufferDescriptor(buf_id);
            let mut buf_state = LockBufHdr(desc);
            if buf_state & mask == mask {
                buf_state |= BM_CHECKPOINT_NEEDED;
                let tag = desc.tag();
                scratch.items.push(CkptSortItem {
                    tsId: tag.spcOid,
                    relNumber: tag.relNumber,
                    forkNum: tag.forkNum as i32,
                    blockNum: tag.blockNum,
                    buf_id,
                });
            }
            UnlockBufHdr(desc, buf_state);
            barrier_check()?;
        }

        let num_to_scan = scratch.items.len();
        if num_to_scan == 0 {
            return Ok(());
        }

        let mut wb = WritebackContext::for_checkpoint();

        scratch.items.sort_unstable_by(|a, b| {
            (a.tsId, a.relNumber, a.forkNum, a.blockNum)
                .cmp(&(b.tsId, b.relNumber, b.forkNum, b.blockNum))
        });

        scratch.per_ts.clear();
        for (i, item) in scratch.items.iter().enumerate() {
            let need_new = scratch
                .per_ts
                .last()
                .map(|s| s.tsId != item.tsId)
                .unwrap_or(true);
            if need_new {
                if scratch.per_ts.try_reserve(1).is_err() {
                    panic!("out of memory allocating per-tablespace checkpoint status");
                }
                scratch.per_ts.push(CkptTsStatus {
                    tsId: item.tsId,
                    index: i as i32,
                    ..CkptTsStatus::default()
                });
            }
            scratch.per_ts.last_mut().expect("just ensured").num_to_scan += 1;
            barrier_check()?;
        }

        for ts in scratch.per_ts.iter_mut() {
            ts.progress_slice = num_to_scan as f64 / ts.num_to_scan as f64;
        }

        // C balances via a min-heap over per-tablespace progress; tablespace
        // counts are tiny, so a linear min scan gives the same write order.
        let mut num_processed = 0usize;
        let mut live = scratch.per_ts.len();
        while live > 0 {
            let ts_i = scratch
                .per_ts
                .iter()
                .enumerate()
                .filter(|(_, s)| s.num_scanned < s.num_to_scan)
                .min_by(|a, b| a.1.progress.total_cmp(&b.1.progress))
                .map(|(i, _)| i)
                .expect("live > 0");

            let buf_id = scratch.items[scratch.per_ts[ts_i].index as usize].buf_id;
            let desc = GetBufferDescriptor(buf_id);
            num_processed += 1;

            if desc.state.load(Ordering::Acquire) & BM_CHECKPOINT_NEEDED != 0 {
                // bufmgr.c BufferSync: PendingCheckpointerStats.
                // buffers_written++ per buffer the checkpoint wrote (feeds
                // pg_stat_checkpointer.buffers_written).
                if SyncOneBuffer(buf_id, false, &mut wb)? & BUF_WRITTEN != 0 {
                    pgstat::checkpointer::pgstat_count_checkpointer_buffers_written();
                }
            }

            let ts = &mut scratch.per_ts[ts_i];
            ts.progress += ts.progress_slice;
            ts.num_scanned += 1;
            ts.index += 1;
            if ts.num_scanned == ts.num_to_scan {
                live -= 1;
            }

            // CheckpointWriteDelay is the checkpointer's (aux-mains lane);
            // uninstalled = unthrottled, C's CHECKPOINT_IMMEDIATE behavior.
            if checkpointer_seams::checkpoint_write_delay::is_installed() {
                checkpointer_seams::checkpoint_write_delay::call(
                    flags,
                    num_processed as f64 / num_to_scan as f64,
                )?;
            }
        }

        IssuePendingWritebacks(&mut wb, IOContext::IOCONTEXT_NORMAL)?;
        // CheckpointStats (the log-line struct: ckpt_bufs_written and the
        // write=/sync= phase timestamps that also feed
        // pg_stat_checkpointer.write_time/sync_time) pends the stats unit;
        // buffers_written above is counted directly.
        Ok(())
    })
}

pub fn CheckPointBuffers(flags: i32) -> PgResult<()> {
    BufferSync(flags)
}

#[cfg(test)]
pub(crate) fn page_checksum_for_tests(page: &[u8; BLCKSZ], blkno: u32) -> u16 {
    checksum::page_checksum(page, blkno)
}
