//! `localbuf.c` — the local (temp-table) buffer manager.
//!
//! The fast buffer manager for TEMPORARY relations: their pages never need
//! WAL-logging or checkpointing, so the local pool is strictly BACKEND-LOCAL
//! (NOT in shmem). `LocalBufferDescriptors`, the block storage, and
//! `LocalRefCount` are ordinary per-backend arrays, and `LocalBufHash` is a
//! per-backend hash — all owned by the [`LocalBufferManager`] value, with plain
//! `Cell`/`RefCell` interior mutability (the "no `std::sync` for shared state"
//! rule is about cross-backend SHARED state; a single backend owns everything
//! here).
//!
//! The only genuine externals are the temp-relation `smgr` I/O entry points
//! (`smgrread` / `smgrwrite` / `smgrnblocks` / `smgrzeroextend` /
//! `smgrprefetch`), the in-place page checksum, and the two diagnostics helpers
//! (`MyProcNumber` and the relation-path renderer) used to build error strings.
//! AIO is not wired in this substrate, so `bufHdr->io_wref` is always invalid;
//! the `pgaio_wref_valid` branches are the (faithful) always-false path of a
//! backend with synchronous I/O.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;

use types_error::{
    PgError, PgResult, ERRCODE_INSUFFICIENT_RESOURCES, ERRCODE_INVALID_TRANSACTION_STATE,
    ERRCODE_OUT_OF_MEMORY, ERRCODE_PROGRAM_LIMIT_EXCEEDED,
};
use types_storage::buf::{
    buftag, BufferDesc, PgAioWaitRef, BM_DIRTY, BM_IO_ERROR, BM_JUST_DIRTIED, BM_MAX_USAGE_COUNT,
    BM_TAG_VALID, BM_VALID, BUF_FLAG_MASK, BUF_REFCOUNT_ONE, BUF_USAGECOUNT_MASK,
    BUF_USAGECOUNT_ONE, MAX_BLOCK_NUMBER,
};
use types_storage::PrefetchBufferResult;
use types_storage::RelFileLocator;
use types_core::{
    BlockNumber, Buffer, ForkNumber, InvalidBlockNumber, InvalidBuffer, BLCKSZ,
};

use mcx::MAX_ALLOC_SIZE;

use crate::{buf_state_get_refcount, buf_state_get_usagecount};

use backend_storage_buffer_bufmgr_seams as bufmgr_seam;
use backend_storage_smgr_seams as smgr_seam;

/// `GetLocalBufferStorage` chunked-allocation bookkeeping (the C function-local
/// `static`s `cur_block` / `next_buf_in_block` / `num_bufs_in_block` /
/// `total_bufs_allocated`). Block storage is a `Vec<[u8; BLCKSZ]>` grown in
/// doubling chunks (16, 32, ...), exactly the request shape C uses; index `i` of
/// `LocalBufferBlockPointers` becomes `Some(storage_index)` once allocated.
#[derive(Default)]
struct LocalBufferStorage {
    /// All allocated blocks (each BLCKSZ bytes), in allocation order.
    blocks: alloc::vec::Vec<alloc::boxed::Box<[u8; BLCKSZ]>>,
    /// `next_buf_in_block` — index of the next buffer to hand out within the
    /// current chunk; a new chunk is requested once it reaches
    /// `num_bufs_in_block`.
    next_buf_in_block: i32,
    /// `num_bufs_in_block` — size of the most recent chunk request (doubles).
    num_bufs_in_block: i32,
    /// `total_bufs_allocated` — running total of blocks handed out.
    total_bufs_allocated: i32,
}

/// The backend-local temp-buffer pool. Holds the per-backend
/// `LocalBufferDescriptors`, the block storage, `LocalRefCount`, the
/// `LocalBufHash`, the free-buffer cursor (`nextFreeLocalBufId`), and the pinned
/// count (`NLocalPinnedBuffers`). Allocated lazily on first temp access
/// (`InitLocalBuffers`), exactly as localbuf.c does.
pub struct LocalBufferManager {
    /// `num_temp_buffers` GUC — the configured size of the local pool, supplied
    /// at construction (C reads the `num_temp_buffers` global).
    num_temp_buffers: i32,
    /// `NLocBuffer` — number of local buffers (0 until initialized).
    nloc_buffer: Cell<i32>,
    /// `nextFreeLocalBufId` — clock-sweep cursor for `GetLocalVictimBuffer`.
    next_free_local_buf_id: Cell<i32>,
    /// `NLocalPinnedBuffers` — count of local buffers pinned at least once.
    nlocal_pinned_buffers: Cell<i32>,
    /// `IsParallelWorker()` — parallel workers cannot touch temp tables.
    is_parallel_worker: bool,
    /// `LocalBufferDescriptors` — one `BufferDesc` per local buffer.
    descriptors: RefCell<alloc::vec::Vec<BufferDesc>>,
    /// `LocalBufferBlockPointers` — index into `storage.blocks` for buffer `i`,
    /// or `None` until lazily allocated (`GetLocalBufferStorage`).
    block_pointers: RefCell<alloc::vec::Vec<Option<usize>>>,
    /// `LocalRefCount` — per-buffer local pin count.
    local_ref_count: RefCell<alloc::vec::Vec<i32>>,
    /// `LocalBufHash` — tag -> buffer index lookup (`HASH_BLOBS` hash in C).
    local_buf_hash: RefCell<HashMap<buftag, i32>>,
    /// `GetLocalBufferStorage` chunked-allocation state.
    storage: RefCell<LocalBufferStorage>,
}

thread_local! {
    /// THIS backend's ambient local (temp-relation) buffer manager (the
    /// per-backend `LocalBufferDescriptors`/`LocalBufHash` analog), published by
    /// [`LocalBufferManager::register_global`]. Like the shared
    /// `BufferManager::global` posture, this is strictly per-backend — the local
    /// pool is never in shmem — so a `thread_local` `'static` handle mirrors C's
    /// process-global file-static state.
    static BACKEND_LOCAL_MGR: Cell<Option<&'static LocalBufferManager>> =
        const { Cell::new(None) };
}

impl LocalBufferManager {
    /// Publish this manager as THIS backend's ambient local buffer manager,
    /// returning a `'static` reference to it. Calling more than once for the
    /// same backend returns the FIRST-published manager.
    pub fn register_global(self) -> &'static LocalBufferManager {
        BACKEND_LOCAL_MGR.with(|slot| {
            if let Some(existing) = slot.get() {
                return existing;
            }
            let leaked: &'static LocalBufferManager = Box::leak(Box::new(self));
            slot.set(Some(leaked));
            leaked
        })
    }

    /// THIS backend's ambient local buffer manager, or `None` if not yet
    /// published (e.g. a backend that has never touched a temp relation).
    pub fn global() -> Option<&'static LocalBufferManager> {
        BACKEND_LOCAL_MGR.with(|slot| slot.get())
    }

    /// Construct an uninitialized local buffer manager (buffers allocated lazily
    /// by `InitLocalBuffers`, exactly as localbuf.c does on first temp access).
    /// `num_temp_buffers` is the `temp_buffers` GUC value; `is_parallel_worker`
    /// is `IsParallelWorker()`.
    pub fn new(num_temp_buffers: i32, is_parallel_worker: bool) -> Self {
        Self {
            num_temp_buffers,
            nloc_buffer: Cell::new(0),
            next_free_local_buf_id: Cell::new(0),
            nlocal_pinned_buffers: Cell::new(0),
            is_parallel_worker,
            descriptors: RefCell::new(alloc::vec::Vec::new()),
            block_pointers: RefCell::new(alloc::vec::Vec::new()),
            local_ref_count: RefCell::new(alloc::vec::Vec::new()),
            local_buf_hash: RefCell::new(HashMap::new()),
            storage: RefCell::new(LocalBufferStorage::default()),
        }
    }

    /// `BufferDescriptorGetBuffer` for a local descriptor: `buf_id + 1`. For
    /// local buffers `buf_id` is negative (`-i - 2`), so the resulting Buffer is
    /// `-i - 1` (negative), the local buffer handle.
    #[inline]
    fn buffer_for_index(index: i32) -> Buffer {
        -index - 1
    }

    /// `-buffer - 1` — the local buffer index for a (negative) Buffer handle.
    #[inline]
    fn index_for_buffer(buffer: Buffer) -> i32 {
        -buffer - 1
    }

    /// Read the (unlocked) state word of local buffer `index`.
    #[inline]
    fn state(&self, index: i32) -> u32 {
        self.descriptors.borrow()[index as usize].state.read()
    }

    /// `pg_atomic_unlocked_write_u32(&bufHdr->state, v)` for local buffer
    /// `index`.
    #[inline]
    fn set_state(&self, index: i32, v: u32) {
        self.descriptors.borrow()[index as usize]
            .state
            .value
            .store(v, std::sync::atomic::Ordering::Relaxed);
    }

    /// The tag of local buffer `index`.
    #[inline]
    fn tag(&self, index: i32) -> buftag {
        self.descriptors.borrow()[index as usize].tag
    }

    /// `InitBufferTag` — build the lookup key for a temp relation.
    fn make_tag(rlocator: RelFileLocator, forknum: ForkNumber, blocknum: BlockNumber) -> buftag {
        buftag {
            spcOid: rlocator.spcOid,
            dbOid: rlocator.dbOid,
            relNumber: rlocator.relNumber,
            forkNum: forknum,
            blockNum: blocknum,
        }
    }

    /// `BufTagMatchesRelFileLocator`.
    fn tag_matches_rlocator(tag: &buftag, rlocator: &RelFileLocator) -> bool {
        tag.spcOid == rlocator.spcOid
            && tag.dbOid == rlocator.dbOid
            && tag.relNumber == rlocator.relNumber
    }

    /// `PrefetchLocalBuffer` — initiate async read of a block of a temp
    /// relation.
    pub fn PrefetchLocalBuffer(
        &self,
        rlocator: RelFileLocator,
        forknum: ForkNumber,
        blocknum: BlockNumber,
    ) -> PgResult<PrefetchBufferResult> {
        let mut result = PrefetchBufferResult {
            recent_buffer: InvalidBuffer,
            initiated_io: false,
        };
        let new_tag = Self::make_tag(rlocator, forknum, blocknum);

        // Initialize local buffers if first request in this session.
        if self.nloc_buffer.get() == 0 {
            self.InitLocalBuffers()?;
        }

        // See if the desired buffer already exists.
        if let Some(&id) = self.local_buf_hash.borrow().get(&new_tag) {
            // Yes, so nothing to do.
            result.recent_buffer = -id - 1;
        } else {
            // Not in buffers, so initiate prefetch.
            //
            // #ifdef USE_PREFETCH: gated by (io_direct_flags & IO_DIRECT_DATA)
            // == 0 — the IO_DIRECT_DATA test lives at this call site, not inside
            // smgrprefetch (mdprefetch only Asserts it).
            if !bufmgr_seam::io_direct_data::call()
                && smgr_seam::smgr_prefetch::call(
                    rlocator,
                    backend_utils_init_small_seams::my_proc_number::call(),
                    forknum,
                    blocknum,
                )?
            {
                result.initiated_io = true;
            }
        }

        Ok(result)
    }

    /// `LocalBufferAlloc` — find or create a local buffer for the given page of
    /// the given relation. Returns the local Buffer handle (negative) and
    /// `found` (true iff the block was already resident). No locking (all
    /// local); only the default access strategy is supported (usage_count is
    /// always advanced).
    pub fn LocalBufferAlloc(
        &self,
        rlocator: RelFileLocator,
        forknum: ForkNumber,
        blocknum: BlockNumber,
    ) -> PgResult<(Buffer, bool)> {
        let new_tag = Self::make_tag(rlocator, forknum, blocknum);

        // Initialize local buffers if first request in this session.
        if self.nloc_buffer.get() == 0 {
            self.InitLocalBuffers()?;
        }

        // See if the desired buffer already exists.
        let existing = self.local_buf_hash.borrow().get(&new_tag).copied();

        if let Some(bufid) = existing {
            debug_assert!(self.tag(bufid) == new_tag);
            // ResourceOwnerEnlarge(CurrentResourceOwner) (localbuf.c:134) — make
            // room for the pin PinLocalBuffer is about to remember. (The victim
            // branch enlarges inside GetLocalVictimBuffer.)
            backend_storage_buffer_bufmgr_seams::resowner_enlarge::call()?;
            let found = self.PinLocalBuffer(bufid, true)?;
            Ok((Self::buffer_for_index(bufid), found))
        } else {
            let victim_buffer = self.GetLocalVictimBuffer()?;
            let bufid = -victim_buffer - 1;

            // `LocalBufHash` holds at most one entry per local buffer.
            match self.local_buf_hash.borrow_mut().try_reserve(1) {
                Ok(()) => {}
                Err(_) => return Err(out_of_memory()),
            }
            if self
                .local_buf_hash
                .borrow_mut()
                .insert(new_tag, bufid)
                .is_some()
            {
                // shouldn't happen
                return Err(PgError::error("local buffer hash table corrupted"));
            }

            // It's all ours now.
            self.descriptors.borrow_mut()[bufid as usize].tag = new_tag;

            let mut buf_state = self.state(bufid);
            buf_state &= !(BUF_FLAG_MASK | BUF_USAGECOUNT_MASK);
            buf_state |= BM_TAG_VALID | BUF_USAGECOUNT_ONE;
            self.set_state(bufid, buf_state);

            Ok((Self::buffer_for_index(bufid), false))
        }
    }

    /// Like `FlushBuffer()`, just for local buffers. Writes a dirty local buffer
    /// back (`smgrwrite`) and clears `BM_DIRTY`. No WAL flush (temp rels are
    /// never WAL-logged).
    pub fn FlushLocalBuffer(&self, bufid: i32) -> PgResult<()> {
        debug_assert!(self.local_ref_count.borrow()[bufid as usize] > 0);

        // There currently are no reasons for StartLocalBufferIO to return false,
        // so raise an error in that case.
        if !self.StartLocalBufferIO(bufid, false, false)? {
            return Err(PgError::error("failed to start write IO on local buffer"));
        }

        let tag = self.tag(bufid);
        let rlocator = RelFileLocator {
            spcOid: tag.spcOid,
            dbOid: tag.dbOid,
            relNumber: tag.relNumber,
        };

        let block_index = self.block_pointers.borrow()[bufid as usize]
            .ok_or_else(|| PgError::error("FlushLocalBuffer: dirty local buffer has no storage"))?;

        // PageSetChecksumInplace(localpage, bufHdr->tag.blockNum): stamp
        // pd_checksum in place just before the write (no-op when checksums are
        // off).
        {
            let mut storage = self.storage.borrow_mut();
            let mut page = backend_storage_page::PageMut::new(&mut storage.blocks[block_index][..])?;
            backend_storage_page::PageSetChecksumInplace(&mut page, tag.blockNum);
        }

        // FlushLocalBuffer(bufHdr, NULL): when no SMgrRelation was supplied, C
        // does `reln = smgropen(BufTagGetRelFileLocator(&tag), MyProcNumber)`
        // before the write (localbuf.c:196). Ensure the smgr cache entry exists
        // (idempotent) so smgrwrite doesn't hit "md operation on an unopened
        // SMgrRelation" — e.g. the SET TABLESPACE pre-copy flush, where the
        // temp relation may not be open in this backend's smgr cache.
        smgr_seam::smgr_open::call(
            rlocator,
            backend_utils_init_small_seams::my_proc_number::call(),
        )?;

        // And write...
        {
            let storage = self.storage.borrow();
            smgr_seam::smgr_write::call(
                rlocator,
                backend_utils_init_small_seams::my_proc_number::call(),
                tag.forkNum,
                tag.blockNum,
                &storage.blocks[block_index][..],
            )?;
        }

        // pgstat_count_io_op_time(IOOBJECT_TEMP_RELATION, IOCONTEXT_NORMAL,
        // IOOP_WRITE, io_start, 1, BLCKSZ) (localbuf.c:213) — record the
        // temp-relation write into pg_stat_io.
        backend_storage_buffer_bufmgr_seams::count_io_op_temp::call(
            types_pgstat::activity_pgstat::IOOp::IOOP_WRITE,
            1,
            types_core::BLCKSZ as u64,
        );

        // Mark not-dirty.
        self.TerminateLocalBufferIO(bufid, true, 0, false)?;

        Ok(())
    }

    /// `GetLocalVictimBuffer` (static) — clock-sweep the local pool for a usable
    /// buffer, lazily allocating its storage, flushing it if dirty, and dropping
    /// its old hash entry. Returns the (negative) local Buffer handle.
    pub fn GetLocalVictimBuffer(&self) -> PgResult<Buffer> {
        // ResourceOwnerEnlarge(CurrentResourceOwner) (localbuf.c) — make room for
        // the pin PinLocalBuffer takes on the chosen victim.
        backend_storage_buffer_bufmgr_seams::resowner_enlarge::call()?;

        let nloc = self.nloc_buffer.get();
        let mut trycounter = nloc;
        let victim_bufid;
        loop {
            let cur = self.next_free_local_buf_id.get();
            let next = if cur + 1 >= nloc { 0 } else { cur + 1 };
            self.next_free_local_buf_id.set(next);

            if self.local_ref_count.borrow()[cur as usize] == 0 {
                let buf_state = self.state(cur);
                if buf_state_get_usagecount(buf_state) > 0 {
                    self.set_state(cur, buf_state - BUF_USAGECOUNT_ONE);
                    trycounter = nloc;
                } else if buf_state_get_refcount(buf_state) > 0 {
                    // Reachable if the backend initiated AIO for this buffer and
                    // then errored out. (Not exercised with sync I/O.)
                } else {
                    // Found a usable buffer.
                    self.PinLocalBuffer(cur, false)?;
                    victim_bufid = cur;
                    break;
                }
            } else {
                trycounter -= 1;
                if trycounter == 0 {
                    return Err(PgError::error("no empty local buffer available")
                        .with_sqlstate(ERRCODE_INSUFFICIENT_RESOURCES));
                }
            }
        }

        // Lazy memory allocation: allocate space on first use of a buffer.
        if self.block_pointers.borrow()[victim_bufid as usize].is_none() {
            let idx = self.GetLocalBufferStorage()?;
            self.block_pointers.borrow_mut()[victim_bufid as usize] = Some(idx);
        }

        // This buffer is not referenced but it might still be dirty; if so,
        // write it out before reusing it.
        if self.state(victim_bufid) & BM_DIRTY != 0 {
            self.FlushLocalBuffer(victim_bufid)?;
        }

        // Remove the victim buffer from the hashtable and mark as invalid.
        if self.state(victim_bufid) & BM_TAG_VALID != 0 {
            self.InvalidateLocalBuffer(victim_bufid, false)?;

            // pgstat_count_io_op(IOOBJECT_TEMP_RELATION, IOCONTEXT_NORMAL,
            // IOOP_EVICT, 1, 0) (localbuf.c:298) — a valid temp buffer was
            // recycled to make room for another block.
            backend_storage_buffer_bufmgr_seams::count_io_op_temp::call(
                types_pgstat::activity_pgstat::IOOp::IOOP_EVICT,
                1,
                0,
            );
        }

        Ok(Self::buffer_for_index(victim_bufid))
    }

    /// `GetLocalPinLimit` — max additional local pins this backend may take.
    /// Every backend has its own temporary buffers, and can pin them all.
    pub fn GetLocalPinLimit(&self) -> u32 {
        self.num_temp_buffers as u32
    }

    /// `GetAdditionalLocalPinLimit` — pins available beyond those already held.
    pub fn GetAdditionalLocalPinLimit(&self) -> u32 {
        debug_assert!(self.nlocal_pinned_buffers.get() <= self.num_temp_buffers);
        (self.num_temp_buffers - self.nlocal_pinned_buffers.get()) as u32
    }

    /// `LimitAdditionalLocalPins` — clamp a requested pin count to the limit. In
    /// contrast to `LimitAdditionalPins`, other backends don't play a role: we
    /// can allow up to NLocBuffer pins in total, but it might not be initialized
    /// yet so read num_temp_buffers.
    pub fn LimitAdditionalLocalPins(&self, additional_pins: &mut u32) {
        if *additional_pins <= 1 {
            return;
        }
        let max_pins = (self.num_temp_buffers - self.nlocal_pinned_buffers.get()) as u32;
        if *additional_pins >= max_pins {
            *additional_pins = max_pins;
        }
    }

    /// `ExtendBufferedRelLocal` — extend a temp relation by `extend_by` blocks
    /// (clamped to the pin limit). Fills `buffers[0..extended_by]` with the
    /// pinned new buffers and returns the first new block number. `extend_upto`
    /// is `InvalidBlockNumber` for `ExtendBufferedRelBy`, or the cap for
    /// `ExtendBufferedRelTo`.
    #[allow(clippy::too_many_arguments)]
    pub fn ExtendBufferedRelLocal(
        &self,
        rlocator: RelFileLocator,
        fork: ForkNumber,
        _flags: u32,
        mut extend_by: u32,
        extend_upto: BlockNumber,
        buffers: &mut [Buffer],
        extended_by: &mut u32,
    ) -> PgResult<BlockNumber> {
        // Initialize local buffers if first request in this session.
        if self.nloc_buffer.get() == 0 {
            self.InitLocalBuffers()?;
        }

        self.LimitAdditionalLocalPins(&mut extend_by);

        for buf in buffers.iter_mut().take(extend_by as usize) {
            *buf = self.GetLocalVictimBuffer()?;
            let buf_hdr = -*buf - 1;
            // New buffers are zero-filled.
            let block_index =
                self.block_pointers.borrow()[buf_hdr as usize].expect("victim has storage");
            self.storage.borrow_mut().blocks[block_index].fill(0);
        }

        let first_block = smgr_seam::smgrnblocks::call(
            rlocator,
            backend_utils_init_small_seams::my_proc_number::call(),
            fork,
        )?;

        if extend_upto != InvalidBlockNumber {
            // In contrast to shared relations, nothing could change the relation
            // size concurrently, so the assertions always hold.
            debug_assert!(first_block <= extend_upto);
            debug_assert!(first_block as u64 + extend_by as u64 <= extend_upto as u64);
        }

        // Fail if relation is already at maximum possible length.
        if first_block as u64 + extend_by as u64 >= MAX_BLOCK_NUMBER as u64 {
            let relpath = common_relpath_seams::relpathbackend::call(
                rlocator,
                backend_utils_init_small_seams::my_proc_number::call(),
                fork,
            );
            return Err(PgError::error(alloc::format!(
                "cannot extend relation {relpath} beyond {MAX_BLOCK_NUMBER} blocks"
            ))
            .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED));
        }

        for i in 0..extend_by as usize {
            let victim_buf_id = -buffers[i] - 1;

            // in case we need to pin an existing buffer below
            // (ResourceOwnerEnlarge(CurrentResourceOwner), localbuf.c:409).
            backend_storage_buffer_bufmgr_seams::resowner_enlarge::call()?;

            let tag = Self::make_tag(rlocator, fork, first_block + i as u32);

            let found_existing = self.local_buf_hash.borrow().get(&tag).copied();
            if let Some(existing_id) = found_existing {
                self.UnpinLocalBuffer(Self::buffer_for_index(victim_buf_id))?;

                self.PinLocalBuffer(existing_id, false)?;
                buffers[i] = Self::buffer_for_index(existing_id);

                // Clear the BM_VALID bit, do StartLocalBufferIO() and proceed.
                let mut buf_state = self.state(existing_id);
                debug_assert!(buf_state & BM_TAG_VALID != 0);
                debug_assert!(buf_state & BM_DIRTY == 0);
                buf_state &= !BM_VALID;
                self.set_state(existing_id, buf_state);

                // No need to loop for local buffers.
                self.StartLocalBufferIO(existing_id, true, false)?;
            } else {
                let mut buf_state = self.state(victim_buf_id);
                debug_assert!(
                    buf_state & (BM_VALID | BM_TAG_VALID | BM_DIRTY | BM_JUST_DIRTIED) == 0
                );

                self.descriptors.borrow_mut()[victim_buf_id as usize].tag = tag;

                buf_state |= BM_TAG_VALID | BUF_USAGECOUNT_ONE;
                self.set_state(victim_buf_id, buf_state);

                self.local_buf_hash
                    .borrow_mut()
                    .try_reserve(1)
                    .map_err(|_| out_of_memory())?;
                self.local_buf_hash.borrow_mut().insert(tag, victim_buf_id);

                self.StartLocalBufferIO(victim_buf_id, true, false)?;
            }
        }

        // Actually extend relation.
        smgr_seam::smgr_zeroextend::call(
            rlocator,
            backend_utils_init_small_seams::my_proc_number::call(),
            fork,
            first_block,
            extend_by,
            false,
        )?;

        // pgstat_count_io_op_time(IOOBJECT_TEMP_RELATION, IOCONTEXT_NORMAL,
        // IOOP_EXTEND, io_start, extend_by, extend_by * BLCKSZ) (localbuf.c:461).
        backend_storage_buffer_bufmgr_seams::count_io_op_temp::call(
            types_pgstat::activity_pgstat::IOOp::IOOP_EXTEND,
            extend_by as u64,
            extend_by as u64 * types_core::BLCKSZ as u64,
        );

        for buf in buffers.iter().take(extend_by as usize) {
            let buf_hdr = -*buf - 1;
            let buf_state = self.state(buf_hdr) | BM_VALID;
            self.set_state(buf_hdr, buf_state);
        }

        *extended_by = extend_by;

        Ok(first_block)
    }

    /// `MarkLocalBufferDirty` — mark a local buffer dirty.
    pub fn MarkLocalBufferDirty(&self, buffer: Buffer) -> PgResult<()> {
        debug_assert!(Self::buffer_is_local(buffer));

        let bufid = -buffer - 1;

        debug_assert!(self.local_ref_count.borrow()[bufid as usize] > 0);

        // pgBufferUsage.local_blks_dirtied++ when clean -> dirty is per-backend
        // I/O accounting tracked by the bufmgr crate; the BM_DIRTY transition
        // itself is all this function changes.
        let buf_state = self.state(bufid) | BM_DIRTY;
        self.set_state(bufid, buf_state);
        Ok(())
    }

    /// Like `StartBufferIO`, but for local buffers. Returns true if the caller
    /// should proceed with the I/O, false if it was already done. With
    /// synchronous I/O the `io_wref` is always invalid (no AIO wait branch).
    pub fn StartLocalBufferIO(
        &self,
        bufid: i32,
        for_input: bool,
        _nowait: bool,
    ) -> PgResult<bool> {
        // With AIO the buffer could have IO in progress; the io_wref is always
        // invalid in this (synchronous) substrate, so we never wait/return false
        // for that reason.

        // Check if someone else already did the I/O.
        let buf_state = self.state(bufid);
        if for_input {
            if buf_state & BM_VALID != 0 {
                return Ok(false);
            }
        } else if buf_state & BM_DIRTY == 0 {
            return Ok(false);
        }

        // BM_IO_IN_PROGRESS isn't currently used for local buffers.
        // Local buffers don't track IO using resowners.
        Ok(true)
    }

    /// Like `TerminateBufferIO`, but for local buffers. Clears `BM_IO_ERROR`,
    /// optionally `BM_DIRTY`, and ORs in `set_flag_bits`. With synchronous I/O
    /// `release_aio` is never set.
    pub fn TerminateLocalBufferIO(
        &self,
        bufid: i32,
        clear_dirty: bool,
        set_flag_bits: u32,
        release_aio: bool,
    ) -> PgResult<()> {
        // Only need to adjust flags.
        let mut buf_state = self.state(bufid);

        // BM_IO_IN_PROGRESS isn't currently used for local buffers.

        // Clear earlier errors; if this IO failed, it'll be marked again.
        buf_state &= !BM_IO_ERROR;

        if clear_dirty {
            buf_state &= !BM_DIRTY;
        }

        if release_aio {
            // Release pin held by IO subsystem (not exercised with sync I/O).
            debug_assert!(buf_state_get_refcount(buf_state) > 0);
            buf_state -= BUF_REFCOUNT_ONE;
            self.descriptors.borrow_mut()[bufid as usize].io_wref = PgAioWaitRef::default();
        }

        buf_state |= set_flag_bits;
        self.set_state(bufid, buf_state);

        Ok(())
    }

    /// `buffer_stage_common`'s per-buffer `is_temp` body — stage one LOCAL
    /// buffer for an AIO readv/writev: reflect that the buffer is now owned by
    /// the AIO subsystem. The symmetric counterpart of
    /// [`TerminateLocalBufferIO`] with `release_aio = true`.
    pub fn StageLocalBufferIO(
        &self,
        bufid: i32,
        io_ref: PgAioWaitRef,
        is_write: bool,
    ) -> PgResult<()> {
        let mut buf_state = self.state(bufid);

        // verify the buffer is in the expected state
        debug_assert!(buf_state & BM_TAG_VALID != 0);
        if is_write {
            debug_assert!(buf_state & BM_VALID != 0);
            debug_assert!(buf_state & BM_DIRTY != 0);
        } else {
            debug_assert!(buf_state & BM_VALID == 0);
            debug_assert!(buf_state & BM_DIRTY == 0);
        }
        // temp buffers don't use BM_IO_IN_PROGRESS (the !is_temp Assert skipped).
        debug_assert!(buf_state_get_refcount(buf_state) >= 1);

        // Reflect that the buffer is now owned by the AIO subsystem. This pin is
        // released again in TerminateLocalBufferIO(release_aio = true).
        buf_state += BUF_REFCOUNT_ONE;
        self.descriptors.borrow_mut()[bufid as usize].io_wref = io_ref;

        self.set_state(bufid, buf_state);

        Ok(())
    }

    /// `InvalidateLocalBuffer` — mark a local buffer invalid, dropping its tag,
    /// flags, usagecount, and hash entry. If `check_unreferenced`, error when it
    /// is still pinned (locally or by the AIO subsystem).
    pub fn InvalidateLocalBuffer(&self, bufid: i32, check_unreferenced: bool) -> PgResult<()> {
        // It's possible we started IO before aborting; with synchronous I/O the
        // io_wref is always invalid, so there is nothing to wait for here.

        let mut buf_state = self.state(bufid);

        if check_unreferenced
            && (self.local_ref_count.borrow()[bufid as usize] != 0
                || buf_state_get_refcount(buf_state) != 0)
        {
            let tag = self.tag(bufid);
            let rlocator = RelFileLocator {
                spcOid: tag.spcOid,
                dbOid: tag.dbOid,
                relNumber: tag.relNumber,
            };
            let relpath = common_relpath_seams::relpathbackend::call(
                rlocator,
                backend_utils_init_small_seams::my_proc_number::call(),
                tag.forkNum,
            );
            return Err(PgError::error(alloc::format!(
                "block {} of {} is still referenced (local {})",
                tag.blockNum,
                relpath,
                self.local_ref_count.borrow()[bufid as usize]
            )));
        }

        // Remove entry from hashtable.
        let tag = self.tag(bufid);
        if self.local_buf_hash.borrow_mut().remove(&tag).is_none() {
            // shouldn't happen
            return Err(PgError::error("local buffer hash table corrupted"));
        }

        // Mark buffer invalid.
        self.descriptors.borrow_mut()[bufid as usize].tag = buftag::default();
        buf_state &= !BUF_FLAG_MASK;
        buf_state &= !BUF_USAGECOUNT_MASK;
        self.set_state(bufid, buf_state);

        Ok(())
    }

    /// `DropRelationLocalBuffers` — remove from the pool all pages of the
    /// specified relation forks with block numbers `>= firstDelBlock[j]`. Dirty
    /// pages are simply dropped (not written). NOT rollback-able.
    ///
    /// C's `DropRelationLocalBuffers(rlocator, forkNum, firstDelBlock)` takes one
    /// fork + cutoff and full-scans the local pool; its only caller loops once
    /// per fork. We collapse that into one full-pool scan taking the
    /// `forkNum[]`/`firstDelBlock[]` slices: a buffer tag carries exactly ONE
    /// `forkNum`, so any buffer matches at most one slice index `j`; the `break`
    /// after an invalidation stops checking the remaining (non-matching) forks.
    pub fn DropRelationLocalBuffers(
        &self,
        rlocator: RelFileLocator,
        forknum: &[ForkNumber],
        first_del_block: &[BlockNumber],
    ) -> PgResult<()> {
        debug_assert_eq!(forknum.len(), first_del_block.len());
        for i in 0..self.nloc_buffer.get() {
            let buf_state = self.state(i);
            if buf_state & BM_TAG_VALID == 0 {
                continue;
            }
            let tag = self.tag(i);
            if !Self::tag_matches_rlocator(&tag, &rlocator) {
                continue;
            }
            for (j, &fork) in forknum.iter().enumerate() {
                if tag.forkNum == fork && tag.blockNum >= first_del_block[j] {
                    self.InvalidateLocalBuffer(i, true)?;
                    break;
                }
            }
        }
        Ok(())
    }

    /// `DropRelationAllLocalBuffers` — remove from the pool all pages of all
    /// forks of the specified relation.
    pub fn DropRelationAllLocalBuffers(&self, rlocator: RelFileLocator) -> PgResult<()> {
        for i in 0..self.nloc_buffer.get() {
            let buf_state = self.state(i);
            if buf_state & BM_TAG_VALID != 0
                && Self::tag_matches_rlocator(&self.tag(i), &rlocator)
            {
                self.InvalidateLocalBuffer(i, true)?;
            }
        }
        Ok(())
    }

    /// `FlushRelationBuffers`'s `RelationUsesLocalBuffers(rel)` branch — write
    /// out all dirty pages of a temp relation. The local-pool counterpart of the
    /// shared-pool `FlushRelationBuffers` scan.
    pub fn FlushRelationLocalBuffers(&self, rlocator: RelFileLocator) -> PgResult<()> {
        for i in 0..self.nloc_buffer.get() {
            let buf_state = self.state(i);
            if Self::tag_matches_rlocator(&self.tag(i), &rlocator)
                && buf_state & (BM_VALID | BM_DIRTY) == (BM_VALID | BM_DIRTY)
            {
                // Pin/unpin mostly to make valgrind work, but also the right
                // thing.
                // ResourceOwnerEnlarge(CurrentResourceOwner) (bufmgr.c:4963).
                backend_storage_buffer_bufmgr_seams::resowner_enlarge::call()?;
                self.PinLocalBuffer(i, false)?;
                self.FlushLocalBuffer(i)?;
                self.UnpinLocalBuffer(Self::buffer_for_index(i))?;
            }
        }
        Ok(())
    }

    /// `InitLocalBuffers` (static) — init the local buffer cache, lazily on
    /// first temp access. Allocates the buffer headers + auxiliary arrays (block
    /// storage is allocated on demand by `GetLocalBufferStorage`).
    pub fn InitLocalBuffers(&self) -> PgResult<()> {
        let nbufs = self.num_temp_buffers;

        // Parallel workers can't access data in temporary tables.
        if self.is_parallel_worker {
            return Err(PgError::error(
                "cannot access temporary tables during a parallel operation",
            )
            .with_sqlstate(ERRCODE_INVALID_TRANSACTION_STATE));
        }

        // Allocate and zero buffer headers and auxiliary arrays. Reserve
        // fallibly; OOM surfaces as a PgError (C `ereport(FATAL,
        // ERRCODE_OUT_OF_MEMORY)`).
        let n = nbufs.max(0) as usize;
        let mut descriptors: alloc::vec::Vec<BufferDesc> = alloc::vec::Vec::new();
        descriptors.try_reserve(n).map_err(|_| out_of_memory())?;
        for i in 0..nbufs {
            let mut buf = BufferDesc::default();
            // negative to indicate local buffer: start with -2 (so the first
            // buffer id from BufferDescriptorGetBuffer is -1).
            buf.buf_id = -i - 2;
            // pgaio_wref_clear(&buf->io_wref) — default is already cleared.
            // wait_backend_pgprocno = INVALID_PROC_NUMBER.
            buf.wait_backend_pgprocno = types_core::INVALID_PROC_NUMBER;
            descriptors.push(buf);
        }

        let mut block_pointers: alloc::vec::Vec<Option<usize>> = alloc::vec::Vec::new();
        block_pointers.try_reserve(n).map_err(|_| out_of_memory())?;
        block_pointers.resize(n, None);

        let mut local_ref_count: alloc::vec::Vec<i32> = alloc::vec::Vec::new();
        local_ref_count.try_reserve(n).map_err(|_| out_of_memory())?;
        local_ref_count.resize(n, 0);

        *self.descriptors.borrow_mut() = descriptors;
        *self.block_pointers.borrow_mut() = block_pointers;
        *self.local_ref_count.borrow_mut() = local_ref_count;
        *self.local_buf_hash.borrow_mut() = HashMap::new();
        *self.storage.borrow_mut() = LocalBufferStorage::default();

        self.next_free_local_buf_id.set(0);

        // Initialization done, mark buffers allocated.
        self.nloc_buffer.set(nbufs);
        Ok(())
    }

    /// `PinLocalBuffer` — pin a local buffer (bump `LocalRefCount`), optionally
    /// adjusting its usagecount. Returns whether it is `BM_VALID`.
    pub fn PinLocalBuffer(&self, bufid: i32, adjust_usagecount: bool) -> PgResult<bool> {
        let mut buf_state = self.state(bufid);

        if self.local_ref_count.borrow()[bufid as usize] == 0 {
            self.nlocal_pinned_buffers
                .set(self.nlocal_pinned_buffers.get() + 1);
            buf_state += BUF_REFCOUNT_ONE;
            if adjust_usagecount && buf_state_get_usagecount(buf_state) < BM_MAX_USAGE_COUNT {
                buf_state += BUF_USAGECOUNT_ONE;
            }
            self.set_state(bufid, buf_state);
        }
        self.local_ref_count.borrow_mut()[bufid as usize] += 1;
        // ResourceOwnerRememberBuffer(CurrentResourceOwner,
        //                             BufferDescriptorGetBuffer(buf_hdr));
        // (localbuf.c:825). The caller did ResourceOwnerEnlarge already.
        backend_storage_buffer_bufmgr_seams::remember_buffer::call(Self::buffer_for_index(bufid));

        Ok(buf_state & BM_VALID != 0)
    }

    /// `UnpinLocalBuffer` (localbuf.c:832) — drop a local pin with
    /// resource-owner bookkeeping: `UnpinLocalBufferNoOwner` then
    /// `ResourceOwnerForgetBuffer`.
    pub fn UnpinLocalBuffer(&self, buffer: Buffer) -> PgResult<()> {
        self.UnpinLocalBufferNoOwner(buffer)?;
        // ResourceOwnerForgetBuffer(CurrentResourceOwner, buffer) (localbuf.c:835).
        backend_storage_buffer_bufmgr_seams::forget_buffer::call(buffer);
        Ok(())
    }

    /// `UnpinLocalBufferNoOwner` — drop a local pin without resource-owner
    /// bookkeeping.
    pub fn UnpinLocalBufferNoOwner(&self, buffer: Buffer) -> PgResult<()> {
        let buffid = -buffer - 1;
        debug_assert!(Self::buffer_is_local(buffer));
        debug_assert!(self.local_ref_count.borrow()[buffid as usize] > 0);
        debug_assert!(self.nlocal_pinned_buffers.get() > 0);

        self.local_ref_count.borrow_mut()[buffid as usize] -= 1;
        if self.local_ref_count.borrow()[buffid as usize] == 0 {
            self.nlocal_pinned_buffers
                .set(self.nlocal_pinned_buffers.get() - 1);

            let mut buf_state = self.state(buffid);
            debug_assert!(buf_state_get_refcount(buf_state) > 0);
            buf_state -= BUF_REFCOUNT_ONE;
            self.set_state(buffid, buf_state);
        }
        Ok(())
    }

    /// `GetLocalBufferStorage` (static) — allocate memory for a local buffer.
    /// Aggregates requests so memmgr doesn't see lots of small ones: 16-buffer
    /// first request, doubling each time, capped by remaining need. Returns the
    /// index of the newly allocated block in `storage.blocks`.
    fn GetLocalBufferStorage(&self) -> PgResult<usize> {
        let nloc = self.nloc_buffer.get();
        let mut storage = self.storage.borrow_mut();
        debug_assert!(storage.total_bufs_allocated < nloc);

        // Need a new request to memmgr when the current chunk is used up. We
        // track C's explicit `next_buf_in_block >= num_bufs_in_block` counter so
        // the doubling chunk-request sequence (16/32/64/...) matches C's
        // MemoryContextAlloc shape exactly.
        if storage.next_buf_in_block >= storage.num_bufs_in_block {
            // Start with a 16-buffer request; subsequent ones double each time.
            let mut num_bufs = (storage.num_bufs_in_block * 2).max(16);
            // But not more than what we need for all remaining local bufs.
            num_bufs = num_bufs.min(nloc - storage.total_bufs_allocated);
            // And don't overflow MaxAllocSize, either (MaxAllocSize / BLCKSZ).
            num_bufs = num_bufs.min((MAX_ALLOC_SIZE / BLCKSZ) as i32);
            storage.next_buf_in_block = 0;
            storage.num_bufs_in_block = num_bufs;
        }

        // Allocate next buffer in current memory block (zero-filled).
        let idx = storage.blocks.len();
        storage
            .blocks
            .try_reserve(1)
            .map_err(|_| out_of_memory())?;
        storage.blocks.push(alloc::boxed::Box::new([0u8; BLCKSZ]));
        storage.next_buf_in_block += 1;
        storage.total_bufs_allocated += 1;
        Ok(idx)
    }

    /// `CheckForLocalBufferLeaks` — ensure this backend holds no local buffer
    /// pins. Returns the number of leaked pins (0 if clean); like
    /// `CheckForBufferLeaks`, this is a debug cross-check.
    pub fn CheckForLocalBufferLeaks(&self) -> PgResult<i32> {
        let mut errors = 0;
        let refcounts = self.local_ref_count.borrow();
        for (i, &rc) in refcounts
            .iter()
            .enumerate()
            .take(self.nloc_buffer.get() as usize)
        {
            if rc != 0 {
                let _b: Buffer = -(i as i32) - 1;
                errors += 1;
            }
        }
        Ok(errors)
    }

    /// `AtEOXact_LocalBuffers` — clean up at end of transaction (leak check).
    pub fn AtEOXact_LocalBuffers(&self, _is_commit: bool) -> PgResult<()> {
        self.CheckForLocalBufferLeaks()?;
        Ok(())
    }

    /// `AtProcExit_LocalBuffers` — ensure we have dropped pins during backend
    /// exit (leak check).
    pub fn AtProcExit_LocalBuffers(&self) -> PgResult<()> {
        self.CheckForLocalBufferLeaks()?;
        Ok(())
    }

    /// `BufferIsLocal(buffer)` — a buffer handle is local iff it is negative.
    #[inline]
    pub fn buffer_is_local(buffer: Buffer) -> bool {
        buffer < 0
    }

    /// `NLocBuffer` accessor (number of local buffers, 0 until initialized).
    pub fn nloc_buffer(&self) -> i32 {
        self.nloc_buffer.get()
    }

    /// `nextFreeLocalBufId` accessor (the clock-sweep cursor).
    pub fn next_free_local_buf_id(&self) -> i32 {
        self.next_free_local_buf_id.get()
    }

    /// `NLocalPinnedBuffers` accessor.
    pub fn nlocal_pinned_buffers(&self) -> i32 {
        self.nlocal_pinned_buffers.get()
    }

    /// `LocalRefCount[bufid]` accessor (test/inspection).
    pub fn local_ref_count(&self, buffer: Buffer) -> i32 {
        self.local_ref_count.borrow()[(-buffer - 1) as usize]
    }

    /// The (unlocked) state word of a local buffer (test/inspection).
    pub fn buffer_state(&self, buffer: Buffer) -> u32 {
        self.state(Self::index_for_buffer(buffer))
    }

    /// `pg_atomic_read_u32(&GetLocalBufferDescriptor(index)->state)` — the
    /// (unlocked) state word of the local buffer at local index `index`
    /// (0..NLocBuffer). The index form (vs. [`buffer_state`](Self::buffer_state),
    /// which takes a negative Buffer handle) is what bufmgr.c's `BufferIsLocal`
    /// branches use after computing `b = -buffer - 1`.
    #[inline]
    pub fn local_buffer_state(&self, index: i32) -> u32 {
        self.state(index)
    }

    /// `GetLocalBufferDescriptor(index)->tag` — the tag of the local buffer at
    /// local index `index` (0..NLocBuffer).
    #[inline]
    pub fn local_buffer_tag(&self, index: i32) -> buftag {
        self.tag(index)
    }

    /// The block contents of a local buffer (test/inspection). Returns all-zero
    /// if storage was not yet allocated.
    pub fn with_block<R>(&self, buffer: Buffer, f: impl FnOnce(&[u8]) -> R) -> R {
        let idx = Self::index_for_buffer(buffer);
        let block_index = self.block_pointers.borrow()[idx as usize];
        match block_index {
            Some(bi) => f(&self.storage.borrow().blocks[bi][..]),
            None => f(&[0u8; BLCKSZ]),
        }
    }

    /// Mutable access to a local buffer's BLCKSZ block (the
    /// `LocalBufHdrGetBlock(bufHdr)` page used by ReadBufferLocal's read/zero
    /// completion). Storage is allocated lazily by `GetLocalVictimBuffer` before
    /// the page is filled; if it somehow isn't, one chunk is allocated on demand.
    pub fn with_block_mut<R>(&self, buffer: Buffer, f: impl FnOnce(&mut [u8]) -> R) -> R {
        let idx = Self::index_for_buffer(buffer);
        let block_index = match self.block_pointers.borrow()[idx as usize] {
            Some(bi) => bi,
            None => {
                let bi = self
                    .GetLocalBufferStorage()
                    .expect("local buffer storage allocation");
                self.block_pointers.borrow_mut()[idx as usize] = Some(bi);
                bi
            }
        };
        f(&mut self.storage.borrow_mut().blocks[block_index][..])
    }

    /// The block number held by a local buffer (`BufferGetBlockNumber`).
    pub fn block_number(&self, buffer: Buffer) -> BlockNumber {
        self.tag(Self::index_for_buffer(buffer)).blockNum
    }

    /// The full tag held by a local buffer (`BufferGetTag`).
    pub fn buffer_tag(&self, buffer: Buffer) -> buftag {
        self.tag(Self::index_for_buffer(buffer))
    }

    /// `LocalRefCount[-buffer - 1]++` — the local arm of `IncrBufferRefCount`
    /// (bufmgr.c): bump this backend's local pin count on an already-pinned
    /// local buffer. The resource-owner bookkeeping is bufmgr's, not this
    /// function's.
    pub fn IncrLocalBufferRefCount(&self, buffer: Buffer) {
        let bufid = Self::index_for_buffer(buffer);
        debug_assert!(Self::buffer_is_local(buffer));
        debug_assert!(self.local_ref_count.borrow()[bufid as usize] >= 1);
        self.local_ref_count.borrow_mut()[bufid as usize] += 1;
    }
}

/// `ereport(FATAL, errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory"))`
/// from `InitLocalBuffers` / `GetLocalBufferStorage` (here surfaced as a
/// returned `PgError`).
fn out_of_memory() -> PgError {
    PgError::error("out of memory").with_sqlstate(ERRCODE_OUT_OF_MEMORY)
}

/// `check_temp_buffers` (localbuf.c) — the `temp_buffers` GUC check hook. Once
/// local buffers have been initialized (`nloc_buffer != 0`) it's too late to
/// change the value (unless this is only a test call, `is_test`). `nloc_buffer`
/// is the current `NLocBuffer`. Returns Ok(true) if the change is allowed,
/// Ok(false) (with a GUC errdetail in C) if not.
pub fn check_temp_buffers(newval: i32, nloc_buffer: i32, is_test: bool) -> PgResult<bool> {
    if !is_test && nloc_buffer != 0 && nloc_buffer != newval {
        // GUC_check_errdetail("\"temp_buffers\" cannot be changed after any
        // temporary tables have been accessed in the session.")
        return Ok(false);
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{install_test_seams, TestSmgr};

    fn rloc(rel: u32) -> RelFileLocator {
        RelFileLocator {
            spcOid: 1663,
            dbOid: 5,
            relNumber: rel,
        }
    }

    fn mgr(n: i32) -> LocalBufferManager {
        LocalBufferManager::new(n, false)
    }

    const MAIN: ForkNumber = ForkNumber::MAIN_FORKNUM;

    #[test]
    fn init_local_buffers_allocates_lazily() {
        let _g = install_test_seams();
        TestSmgr::reset(0);
        let m = mgr(8);
        assert_eq!(m.nloc_buffer(), 0);
        m.InitLocalBuffers().unwrap();
        assert_eq!(m.nloc_buffer(), 8);
        assert_eq!(m.descriptors.borrow()[0].buf_id, -2);
    }

    #[test]
    fn alloc_miss_then_hit() {
        let _g = install_test_seams();
        TestSmgr::reset(0);
        let m = mgr(8);
        let (b, found) = m.LocalBufferAlloc(rloc(10), MAIN, 3).unwrap();
        assert!(b < 0, "local buffer handle is negative");
        assert!(!found, "first access is a miss");
        assert_eq!(buf_state_get_usagecount(m.buffer_state(b)), 1);
        assert!(m.buffer_state(b) & BM_TAG_VALID != 0);
        assert_eq!(m.local_ref_count(b), 1);
        assert_eq!(m.block_number(b), 3);

        let idx = -b - 1;
        m.set_state(idx, m.buffer_state(b) | BM_VALID);
        m.UnpinLocalBuffer(b).unwrap();
        assert_eq!(m.local_ref_count(b), 0);

        let (b2, found2) = m.LocalBufferAlloc(rloc(10), MAIN, 3).unwrap();
        assert_eq!(b, b2);
        assert!(found2);
        assert_eq!(m.local_ref_count(b), 1);
        assert_eq!(buf_state_get_usagecount(m.buffer_state(b)), 2);
        m.UnpinLocalBuffer(b).unwrap();
        assert_eq!(m.local_ref_count(b), 0);
    }

    #[test]
    fn mark_dirty_then_flush_clears_dirty_and_writes() {
        let _g = install_test_seams();
        TestSmgr::reset(0);
        let m = mgr(4);
        let (b, _) = m.LocalBufferAlloc(rloc(11), MAIN, 0).unwrap();
        let idx = -b - 1;
        m.MarkLocalBufferDirty(b).unwrap();
        assert!(m.buffer_state(b) & BM_DIRTY != 0);
        if m.block_pointers.borrow()[idx as usize].is_none() {
            let s = m.GetLocalBufferStorage().unwrap();
            m.block_pointers.borrow_mut()[idx as usize] = Some(s);
        }
        m.FlushLocalBuffer(idx).unwrap();
        assert!(m.buffer_state(b) & BM_DIRTY == 0, "flush clears BM_DIRTY");
        assert_eq!(TestSmgr::write_count(), 1);
        m.UnpinLocalBuffer(b).unwrap();
    }

    #[test]
    fn victim_clock_sweep_reuses_unpinned_buffer() {
        let _g = install_test_seams();
        TestSmgr::reset(0);
        let m = mgr(2);
        let (b0, _) = m.LocalBufferAlloc(rloc(20), MAIN, 0).unwrap();
        let (b1, _) = m.LocalBufferAlloc(rloc(20), MAIN, 1).unwrap();
        m.UnpinLocalBuffer(b0).unwrap();
        m.UnpinLocalBuffer(b1).unwrap();
        let (b2, found) = m.LocalBufferAlloc(rloc(20), MAIN, 2).unwrap();
        assert!(!found);
        assert_eq!(m.block_number(b2), 2);
        m.UnpinLocalBuffer(b2).unwrap();
    }

    #[test]
    fn stage_local_buffer_io_takes_aio_pin_and_terminate_releases_it() {
        let _g = install_test_seams();
        TestSmgr::reset(0);
        let m = mgr(4);
        let (b, found) = m.LocalBufferAlloc(rloc(40), MAIN, 7).unwrap();
        assert!(!found);
        let idx = -b - 1;
        assert!(m.buffer_state(b) & BM_TAG_VALID != 0);
        assert!(m.buffer_state(b) & BM_VALID == 0);
        assert!(m.buffer_state(b) & BM_DIRTY == 0);
        let refcount_before = buf_state_get_refcount(m.buffer_state(b));
        assert!(refcount_before >= 1);

        let local_ref_before = m.local_ref_count(b);
        m.StageLocalBufferIO(idx, PgAioWaitRef::default(), false)
            .unwrap();
        assert_eq!(
            buf_state_get_refcount(m.buffer_state(b)),
            refcount_before + 1,
            "stage adds exactly one AIO pin to the state word"
        );
        assert_eq!(
            m.local_ref_count(b),
            local_ref_before,
            "stage does not touch LocalRefCount"
        );

        m.TerminateLocalBufferIO(idx, false, BM_VALID, true).unwrap();
        assert_eq!(
            buf_state_get_refcount(m.buffer_state(b)),
            refcount_before,
            "terminate(release_aio) releases the AIO pin it added"
        );
        assert!(m.buffer_state(b) & BM_VALID != 0, "BM_VALID set on completion");

        m.UnpinLocalBuffer(b).unwrap();
    }

    #[test]
    fn no_empty_local_buffer_when_all_pinned() {
        let _g = install_test_seams();
        TestSmgr::reset(0);
        let m = mgr(2);
        let (_b0, _) = m.LocalBufferAlloc(rloc(21), MAIN, 0).unwrap();
        let (_b1, _) = m.LocalBufferAlloc(rloc(21), MAIN, 1).unwrap();
        let e = m.LocalBufferAlloc(rloc(21), MAIN, 2).unwrap_err();
        assert!(alloc::format!("{e:?}").contains("no empty local buffer available"));
        assert_eq!(e.sqlstate(), ERRCODE_INSUFFICIENT_RESOURCES);
    }

    #[test]
    fn extend_buffered_rel_local_grows_and_pins() {
        let _g = install_test_seams();
        TestSmgr::reset(5); // temp file has 5 blocks
        let m = mgr(8);
        let mut buffers = [InvalidBuffer; 3];
        let mut extended_by = 0u32;
        let first = m
            .ExtendBufferedRelLocal(
                rloc(30),
                MAIN,
                0,
                3,
                InvalidBlockNumber,
                &mut buffers,
                &mut extended_by,
            )
            .unwrap();
        assert_eq!(first, 5);
        assert_eq!(extended_by, 3);
        assert_eq!(TestSmgr::zeroextends(), alloc::vec![(5, 3)]);
        for (i, &b) in buffers.iter().enumerate() {
            assert!(b < 0);
            assert_eq!(m.block_number(b), 5 + i as u32);
            assert!(m.buffer_state(b) & BM_VALID != 0);
            m.UnpinLocalBuffer(b).unwrap();
        }
    }

    #[test]
    fn drop_relation_local_buffers_removes_matching() {
        let _g = install_test_seams();
        TestSmgr::reset(0);
        let m = mgr(8);
        let (b0, _) = m.LocalBufferAlloc(rloc(40), MAIN, 0).unwrap();
        let (b1, _) = m.LocalBufferAlloc(rloc(40), MAIN, 5).unwrap();
        let (b2, _) = m.LocalBufferAlloc(rloc(41), MAIN, 0).unwrap();
        m.UnpinLocalBuffer(b0).unwrap();
        m.UnpinLocalBuffer(b1).unwrap();
        m.UnpinLocalBuffer(b2).unwrap();
        m.DropRelationLocalBuffers(rloc(40), &[MAIN], &[5]).unwrap();
        assert!(m.buffer_state(b1) & BM_TAG_VALID == 0);
        assert!(m.buffer_state(b0) & BM_TAG_VALID != 0);
        m.DropRelationAllLocalBuffers(rloc(40)).unwrap();
        assert!(m.buffer_state(b0) & BM_TAG_VALID == 0);
        assert!(m.buffer_state(b2) & BM_TAG_VALID != 0);
    }

    #[test]
    fn flush_relation_local_buffers_writes_only_valid_dirty_matching() {
        let _g = install_test_seams();
        TestSmgr::reset(0);
        let m = mgr(8);
        let (b0, _) = m.LocalBufferAlloc(rloc(80), MAIN, 0).unwrap();
        let (b1, _) = m.LocalBufferAlloc(rloc(80), MAIN, 1).unwrap();
        let (b2, _) = m.LocalBufferAlloc(rloc(80), MAIN, 2).unwrap();
        let (b3, _) = m.LocalBufferAlloc(rloc(81), MAIN, 0).unwrap();

        for &b in &[b0, b1, b2, b3] {
            let idx = -b - 1;
            if m.block_pointers.borrow()[idx as usize].is_none() {
                let s = m.GetLocalBufferStorage().unwrap();
                m.block_pointers.borrow_mut()[idx as usize] = Some(s);
            }
        }
        let mk_valid = |b: Buffer| {
            let idx = -b - 1;
            m.set_state(idx, m.buffer_state(b) | BM_VALID);
        };
        mk_valid(b0);
        mk_valid(b1);
        mk_valid(b3);
        m.MarkLocalBufferDirty(b0).unwrap();
        m.MarkLocalBufferDirty(b2).unwrap();
        m.MarkLocalBufferDirty(b3).unwrap();

        m.FlushRelationLocalBuffers(rloc(80)).unwrap();

        assert_eq!(TestSmgr::write_count(), 1);
        assert_eq!(TestSmgr::first_write_block(), 0);
        assert!(m.buffer_state(b0) & BM_DIRTY == 0, "flushed buffer is clean");
        assert!(m.buffer_state(b2) & BM_DIRTY != 0, "non-valid stays dirty");
        assert!(m.buffer_state(b3) & BM_DIRTY != 0, "non-matching stays dirty");

        for &b in &[b0, b1, b2, b3] {
            m.UnpinLocalBuffer(b).unwrap();
        }
    }

    #[test]
    fn local_buffer_state_and_tag_by_index_match_handle_accessors() {
        let _g = install_test_seams();
        TestSmgr::reset(0);
        let m = mgr(4);
        let (b, _) = m.LocalBufferAlloc(rloc(90), MAIN, 7).unwrap();
        let idx = -b - 1;
        assert_eq!(m.local_buffer_state(idx), m.buffer_state(b));
        assert_eq!(m.local_buffer_tag(idx), m.buffer_tag(b));
        assert_eq!(m.local_buffer_tag(idx).blockNum, 7);
        m.UnpinLocalBuffer(b).unwrap();
    }

    #[test]
    fn pin_limit_tracks_pinned_count() {
        let _g = install_test_seams();
        TestSmgr::reset(0);
        let m = mgr(10);
        assert_eq!(m.GetLocalPinLimit(), 10);
        assert_eq!(m.GetAdditionalLocalPinLimit(), 10);
        let (b, _) = m.LocalBufferAlloc(rloc(50), MAIN, 0).unwrap();
        assert_eq!(m.GetAdditionalLocalPinLimit(), 9);
        let mut want = 100u32;
        m.LimitAdditionalLocalPins(&mut want);
        assert_eq!(want, 9);
        let mut one = 1u32;
        m.LimitAdditionalLocalPins(&mut one);
        assert_eq!(one, 1, "one pin is always allowed");
        m.UnpinLocalBuffer(b).unwrap();
        assert_eq!(m.GetAdditionalLocalPinLimit(), 10);
    }

    #[test]
    fn leak_check_detects_pinned_buffers() {
        let _g = install_test_seams();
        TestSmgr::reset(0);
        let m = mgr(4);
        let (_b, _) = m.LocalBufferAlloc(rloc(60), MAIN, 0).unwrap();
        assert_eq!(m.CheckForLocalBufferLeaks().unwrap(), 1);
    }

    #[test]
    fn check_temp_buffers_gates_after_init() {
        assert!(check_temp_buffers(100, 0, false).unwrap());
        assert!(check_temp_buffers(8, 8, false).unwrap());
        assert!(!check_temp_buffers(16, 8, false).unwrap());
        assert!(check_temp_buffers(16, 8, true).unwrap());
    }

    #[test]
    fn parallel_worker_cannot_init() {
        let _g = install_test_seams();
        let m = LocalBufferManager::new(8, true);
        let e = m.InitLocalBuffers().unwrap_err();
        assert!(alloc::format!("{e:?}").contains("parallel operation"));
    }

    #[test]
    fn prefetch_hit_reports_recent_buffer() {
        let _g = install_test_seams();
        TestSmgr::reset(0);
        let m = mgr(4);
        let (b, _) = m.LocalBufferAlloc(rloc(70), MAIN, 2).unwrap();
        m.UnpinLocalBuffer(b).unwrap();
        let r = m.PrefetchLocalBuffer(rloc(70), MAIN, 2).unwrap();
        assert_eq!(r.recent_buffer, b);
        assert!(!r.initiated_io);
        // Not resident: default test seam returns no prefetch facility.
        let r2 = m.PrefetchLocalBuffer(rloc(70), MAIN, 99).unwrap();
        assert_eq!(r2.recent_buffer, InvalidBuffer);
        assert!(!r2.initiated_io);
    }
}
