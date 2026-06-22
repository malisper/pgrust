//! The buffer read path (bufmgr.c) — the public `ReadBuffer*` entry points and
//! the (possibly async, possibly multi-block) read pipeline.
//!
//! F3 (this stage). Two read paths, exactly as PG18 bufmgr.c:
//!
//!  * The SYNCHRONOUS single-block core ([`BufferManager::read_buffer_common`])
//!    drives the F2 allocate-or-evict ([`crate::mgr::BufferManager::buffer_alloc`]),
//!    starts the buffer I/O, then performs the transfer DIRECTLY through the smgr
//!    vectored read ([`backend_storage_smgr_smgr::smgrreadv`], a landed direct
//!    dep), verifies the page ([`backend_storage_page::PageIsVerified`]), and
//!    marks the buffer valid. This is what `ReadBuffer` / `ReadBufferExtended` /
//!    `ReadBufferWithoutRelcache` / `ReadRecentBuffer` reach — it is fully live
//!    (smgr is ported), so it unblocks read_stream → heap scan → the table-AM
//!    vtable's read path. The C `ReadBuffer_common` routes the
//!    `RBM_NORMAL`/`RBM_ZERO_ON_ERROR` single-block read through
//!    `StartReadBuffer`+`WaitReadBuffers`; the IOMETHOD_SYNC engine performs the
//!    readv inline, so this is behaviour-equivalent to running the synchronous
//!    transfer directly (the same model src-idiomatic's proven port uses), with
//!    the explicit pipeline below faithfully modelled for read_stream.
//!
//!  * The explicit MULTI-BLOCK pipeline ([`BufferManager::StartReadBuffers`] /
//!    [`BufferManager::StartReadBuffer`] / [`BufferManager::WaitReadBuffers`] /
//!    the in-crate `async_read_buffers` / `start_read_buffers_impl`) is the
//!    faithful engine surface read_stream.c consumes. It pins the run, splits at
//!    the first hit / smgr combine limit, and issues the vectored read through
//!    the AIO engine. The actual pgaio handle lifecycle (acquire / register the
//!    buffer-readv completion vtable / `smgrstartreadv` / `pgaio_wref_wait`)
//!    rides the `pgaio_io_acquire` / `pgaio_register_callbacks` /
//!    `start_read_buffers` / `wait_read_buffers` / `wref_check_done` aio-handle
//!    seams, installed by the aio-sync method stage AFTER this F3 layer lands
//!    (panic-until-owner — sanctioned). The buffer-side staging (BM_IO_IN_PROGRESS
//!    interlock, run splitting, partial-read retry) is ported here in full.
//!
//! Model reconciliation to this repo: a relation is named by `&Relation` (the
//! read seams' contract) resolved to `(rlocator, relpersistence)` via [`BmrRead`]
//! (mirroring `BMR_REL(rel)` / `RelationGetSmgr`), exactly as [`crate::extend`]'s
//! `BmrRel`. The per-buffer content lock is a DIRECT lwlock acquire
//! ([`crate::mgr::BufferManager::content_lock`]); no central content-lock seam.
//! TEMP/local buffers live in the backend-local pool (`localbuf.c`), a separate
//! subsystem not modelled by this shared core; the `RELPERSISTENCE_TEMP` /
//! `BufferIsLocal` arms are honest errors, never silent no-ops.

#![allow(dead_code)]

use types_core::primitive::{
    BlockNumber, Buffer, ForkNumber, InvalidBlockNumber, INVALID_PROC_NUMBER, BLCKSZ,
};
use types_error::{PgError, PgResult};
use types_rel::Relation;
use types_storage::buf::{
    buftag, IOContext, PgAioWaitRef, BM_TAG_VALID, BM_VALID, BUFFER_LOCK_EXCLUSIVE,
};
use types_storage::storage::{LWLockMode, ReadBufferMode};
use types_storage::{PrefetchBufferResult, RelFileLocator, RelFileLocatorBackend};
use types_tuple::access::{
    RELPERSISTENCE_PERMANENT, RELPERSISTENCE_TEMP, RELPERSISTENCE_UNLOGGED,
};

use crate::mgr::BufferManager;

use backend_storage_buffer_bufmgr_seams as sb;
use backend_storage_buffer_support::{buf_table_hash_code, buf_table_hash_partition};
use backend_storage_lmgr_lwlock as lwlock;
use backend_storage_page as page;
use backend_storage_smgr_smgr as smgr;
use backend_utils_init_miscinit_seams as misc;

/// `InvalidBuffer` (buf.h).
const INVALID_BUFFER: Buffer = 0;

/// `P_NEW` (bufmgr.h) — the "extend by a new block" sentinel block number
/// (`InvalidBlockNumber`).
const P_NEW: BlockNumber = InvalidBlockNumber;

/// `MAIN_FORKNUM` (common/relpath.h).
const MAIN_FORKNUM: ForkNumber = ForkNumber::MAIN_FORKNUM;

/// `MAX_IO_COMBINE_LIMIT` (bufmgr.h `== PG_IOV_MAX == 32`) — the largest run of
/// blocks a single combined I/O may cover.
const MAX_IO_COMBINE_LIMIT: i32 = 32;

/// `READ_BUFFERS_*` request flags (bufmgr.h). Carried as a plain bitmask in
/// `ReadBuffersOperation.flags`, exactly as the C `int operation->flags`.
const READ_BUFFERS_ZERO_ON_ERROR: u32 = 1 << 0;
#[allow(dead_code)]
const READ_BUFFERS_ISSUE_ADVICE: u32 = 1 << 1;
#[allow(dead_code)]
const READ_BUFFERS_IGNORE_CHECKSUM_FAILURES: u32 = 1 << 2;
const READ_BUFFERS_SYNCHRONOUSLY: u32 = 1 << 3;

/// `EB_SKIP_EXTENSION_LOCK` (bufmgr.h) — used by the `P_NEW` back-compat leg of
/// `ReadBuffer_common`, which routes to `ExtendBufferedRel`.
const EB_SKIP_EXTENSION_LOCK: u32 = 1 << 0;
/// `EB_LOCK_FIRST` (bufmgr.h) — lock the first extended block exclusively.
const EB_LOCK_FIRST: u32 = 1 << 3;

/// `PgAioResultStatus` (aio_types.h) status codes carried over the
/// `wait_read_buffers` seam (the engine writes the completed result there).
const PGAIO_RS_UNKNOWN: u32 = 0;
const PGAIO_RS_OK: u32 = 1;
const PGAIO_RS_PARTIAL: u32 = 2;
const PGAIO_RS_WARNING: u32 = 3;
const PGAIO_RS_ERROR: u32 = 4;

/// `BufferDescriptorGetBuffer(buf)` — the 1-based [`Buffer`] for a 0-based id.
#[inline]
fn buf_id_to_buffer(buf_id: i32) -> Buffer {
    buf_id + 1
}

/// `BufferIsLocal(buffer)` (buf.h) — temp/local buffers carry a NEGATIVE handle.
/// This shared core models only the shared pool; the local arms are honest
/// errors.
#[inline]
fn buffer_is_local(buffer: Buffer) -> bool {
    buffer < 0
}

/// `InitBufferTag(&tag, &rlocator.locator, forknum, blocknum)` — the buffer tag
/// is keyed by the unbacked `RelFileLocator` (the `backend` part of a
/// `RelFileLocatorBackend` is not part of the tag; temp buffers go to the local
/// pool, out of this shared core).
fn make_tag(rlocator: RelFileLocatorBackend, forknum: ForkNumber, blocknum: BlockNumber) -> buftag {
    buftag {
        spcOid: rlocator.locator.spcOid,
        dbOid: rlocator.locator.dbOid,
        relNumber: rlocator.locator.relNumber,
        forkNum: forknum,
        blockNum: blocknum,
    }
}

/// `BufferTagsEqual(a, b)` (buf_internals.h) — full tag equality.
#[inline]
fn tags_equal(a: &buftag, b: &buftag) -> bool {
    a.spcOid == b.spcOid
        && a.dbOid == b.dbOid
        && a.relNumber == b.relNumber
        && a.forkNum == b.forkNum
        && a.blockNum == b.blockNum
}

/// `relpath(rlocator, fork)`-style identifying string for the read-path error
/// messages (the canonical formatter lives in the common path subsystem; this
/// renders the same identifying fields).
fn relpath_str(rlocator: RelFileLocatorBackend, fork: ForkNumber) -> String {
    let loc = rlocator.locator;
    format!("{}/{}/{} (fork {:?})", loc.spcOid, loc.dbOid, loc.relNumber, fork)
}

/// The resolved physical identity of a relation for the read path: the C
/// `ReadBuffer_common` works off `(SMgrRelation, persistence)`. Here a read seam
/// carries `&Relation`, resolved to `(rlocator, relpersistence)` like
/// [`crate::extend`]'s `BmrRel` (`BMR_REL(rel)` after `RelationGetSmgr`).
struct BmrRead {
    rlocator: RelFileLocatorBackend,
    relpersistence: u8,
}

impl BmrRead {
    /// Resolve directly off `&Relation` (`rd_locator` / `rd_backend` /
    /// `rd_rel->relpersistence`).
    ///
    /// `BMR_REL(rel)` in C runs `RelationGetSmgr(rel)`, which lazily `smgropen`s
    /// the relation and caches the handle on `reln->rd_smgr` before any smgr op.
    /// This port's smgr surface is keyed by `RelFileLocatorBackend` (no handle is
    /// threaded), so the SMgrRelation cache entry must already exist in this
    /// backend's thread-local smgr cache before `smgrreadv`/`smgrnblocks` runs —
    /// otherwise the md layer panics ("md operation on an unopened
    /// SMgrRelation"). The single-block `ReadBuffer_common` path opened it
    /// explicitly, but the multi-block read_stream path (`StartReadBuffers` ->
    /// `WaitReadBuffers` -> `smgrreadv`) reached the md layer with no entry on a
    /// cold scan. Doing the `smgropen` here (idempotent) makes every `BMR_REL`
    /// consumer faithful to `RelationGetSmgr`.
    fn new(rel: &Relation) -> PgResult<Self> {
        smgr::smgropen(rel.rd_locator, rel.rd_backend)?;
        Ok(Self {
            rlocator: RelFileLocatorBackend {
                locator: rel.rd_locator,
                backend: rel.rd_backend,
            },
            relpersistence: rel.rd_rel.relpersistence,
        })
    }
}

/// `ReadBuffersOperation` (bufmgr.h) — the in-flight (multi-block) read state.
/// Crate-local: the AIO types model is owned by the (separately-ported) AIO
/// engine; this carries exactly the fields bufmgr.c's read pipeline mutates.
#[derive(Clone)]
struct ReadBuffersOperation {
    /// `smgr` / `rel` collapse to the physical id (`operation->smgr`,
    /// `operation->forknum`, `operation->blocknum`) the pipeline reads.
    rlocator: RelFileLocatorBackend,
    /// `operation->persistence`.
    persistence: u8,
    /// `operation->forknum`.
    forknum: ForkNumber,
    /// `operation->blocknum` — the first block of the run.
    blocknum: BlockNumber,
    /// `operation->flags` — the `READ_BUFFERS_*` bitmask.
    flags: u32,
    /// `operation->buffers` — the run's 0-based buf_ids (this core has no local
    /// buffers, so all are shared ids).
    buffers: Vec<i32>,
    /// `operation->nblocks_done` — blocks already read in by this operation.
    nblocks_done: u32,
    /// `operation->io_wref` — the in-flight AIO wait reference, or the invalid
    /// sentinel when no IO is outstanding.
    io_wref: PgAioWaitRef,
    /// `operation->io_return.result.{result,status}` — the completed AIO result
    /// (actual blocks read + status), valid once an IO has been issued+completed.
    io_result: i32,
    io_status: u32,
    /// `IOContextForStrategy(operation->strategy)` (bufmgr.c:1641/1792) — the
    /// pg_stat_io context this read is accounted under. `IOCONTEXT_NORMAL` for the
    /// default (no strategy ring), or the strategy ring's context
    /// (BULKREAD/BULKWRITE/VACUUM) threaded from the caller. The ring object
    /// itself is still collapsed, but its KIND now reaches the IO-stats.
    io_context: IOContext,
}

/// A cleared / invalid AIO wait reference (`pgaio_wref_clear`):
/// `aio_index == PG_UINT32_MAX`, so `pgaio_wref_valid` reports "no in-flight IO".
fn wref_invalid() -> PgAioWaitRef {
    PgAioWaitRef {
        aio_index: u32::MAX,
        generation_upper: 0,
        generation_lower: 0,
    }
}

/// `pgaio_wref_valid(wref)` (aio.c) — a wait reference is valid iff its index is
/// not the cleared sentinel.
fn wref_valid(wref: PgAioWaitRef) -> bool {
    wref.aio_index != u32::MAX
}

impl BufferManager {
    // -- public read entry points (bufmgr.c) -------------------------------

    /// `ReadBuffer(reln, blockNum)` (bufmgr.c:758) — shorthand for
    /// [`Self::ReadBufferExtended`] reading the main fork with `RBM_NORMAL` and
    /// the default (no) strategy.
    pub fn ReadBuffer(&self, rel: &Relation, block_num: BlockNumber) -> PgResult<Buffer> {
        self.ReadBufferExtended(
            rel,
            MAIN_FORKNUM,
            block_num,
            ReadBufferMode::Normal,
            IOContext::IOCONTEXT_NORMAL,
        )
    }

    /// `ReadBufferExtended(reln, forkNum, blockNum, mode, strategy)`
    /// (bufmgr.c:805) — read a specific fork/mode. Rejects another session's
    /// temp relations, then dispatches to `ReadBuffer_common`.
    pub fn ReadBufferExtended(
        &self,
        rel: &Relation,
        fork_num: ForkNumber,
        block_num: BlockNumber,
        mode: ReadBufferMode,
        io_context: IOContext,
    ) -> PgResult<Buffer> {
        // Reject attempts to read non-local temporary relations directly. We
        // would be likely to get wrong data since we have no visibility into the
        // owning session's local buffers. (RELATION_IS_OTHER_TEMP: a temp
        // relation whose owning backend is not us.)
        let bmr = BmrRead::new(rel)?;
        if bmr.relpersistence == RELPERSISTENCE_TEMP
            && bmr.rlocator.backend != backend_storage_lmgr_proc_seams::my_proc_number::call()
        {
            return Err(PgError::error(
                "cannot access temporary tables of other sessions",
            ));
        }

        // `BmrRead::new` (the `BMR_REL(rel)`/`RelationGetSmgr` analog) has already
        // `smgropen`ed the relation, so the SMgrRelation cache entry exists prior
        // to the `smgrreadv`/`smgrnblocks` inside read_buffer_common.

        // Read the buffer, and update pgstat counters to reflect a cache hit or
        // miss (done inside ReadBuffer_common / PinBufferForBlock).
        self.read_buffer_common(
            Some(bmr.rlocator),
            bmr.relpersistence,
            fork_num,
            block_num,
            mode,
            io_context,
            Some(rel),
        )
    }

    /// `ReadBufferWithoutRelcache(rlocator, forkNum, blockNum, mode, strategy,
    /// permanent)` (bufmgr.c:842) — read a block for a relation identified only
    /// by its `RelFileLocator` (no relcache entry); used by recovery / xlogutils.
    pub fn ReadBufferWithoutRelcache(
        &self,
        rlocator: RelFileLocator,
        permanent: bool,
        forknum: ForkNumber,
        blocknum: BlockNumber,
        mode: ReadBufferMode,
        io_context: IOContext,
    ) -> PgResult<Buffer> {
        let smgr_persistence = if permanent {
            RELPERSISTENCE_PERMANENT
        } else {
            RELPERSISTENCE_UNLOGGED
        };
        let rlocator_backend = RelFileLocatorBackend {
            locator: rlocator,
            backend: INVALID_PROC_NUMBER,
        };
        self.read_buffer_common(
            Some(rlocator_backend),
            smgr_persistence,
            forknum,
            blocknum,
            mode,
            io_context,
            None,
        )
    }

    /// `ReadRecentBuffer(rlocator, forkNum, blockNum, recent_buffer)`
    /// (bufmgr.c:682) — try to re-pin a block in a recently observed buffer,
    /// avoiding a buffer-mapping lookup on success. Returns true iff the buffer
    /// is still valid and holds the expected tag (then pinned, usagecount bumped).
    pub fn ReadRecentBuffer(
        &self,
        rlocator: RelFileLocator,
        fork_num: ForkNumber,
        block_num: BlockNumber,
        recent_buffer: Buffer,
    ) -> PgResult<bool> {
        debug_assert!(self.buffer_is_valid(recent_buffer) || buffer_is_local(recent_buffer));

        // ResourceOwnerEnlarge(CurrentResourceOwner); ReservePrivateRefCountEntry().
        sb::resowner_enlarge::call()?;
        self.private_refcount().ReservePrivateRefCountEntry();

        let tag = make_tag(
            RelFileLocatorBackend {
                locator: rlocator,
                backend: INVALID_PROC_NUMBER,
            },
            fork_num,
            block_num,
        );

        if buffer_is_local(recent_buffer) {
            // The backend-local pool owns the descriptor; reached through the
            // localbuf subsystem, not modelled by this shared core.
            return Err(PgError::error(
                "ReadRecentBuffer: local buffers are handled by the localbuf subsystem (not in this core)",
            ));
        }

        let buf_id = self.buffer_to_buf_id_pub(recent_buffer)?;
        let have_private_ref = self.private_refcount().get(buf_id as i32) > 0;

        // Do we already have this buffer pinned with a private reference?  If so,
        // it must be valid and it is safe to check the tag without locking.  If
        // not, we have to lock the header first and then check.
        let buf_state = if have_private_ref {
            self.read_state(buf_id)
        } else {
            self.lock_buf_hdr(buf_id)
        };

        if (buf_state & BM_VALID) != 0 && tags_equal(&tag, &self.desc_tag(buf_id)) {
            // It's now safe to pin the buffer.  We can't pin first and ask
            // questions later, because it might confuse code paths like
            // InvalidateBuffer() if we pinned a random non-matching buffer.
            if have_private_ref {
                self.pin_buffer(buf_id, false); // bump pin count
            } else {
                self.pin_buffer_locked(buf_id, buf_state); // pin for first time
            }
            // pgBufferUsage.shared_blks_hit++ (instrumentation, deferred).
            return Ok(true);
        }

        // If we locked the header above, now unlock.
        if !have_private_ref {
            self.unlock_buf_hdr(buf_id, buf_state);
        }
        Ok(false)
    }

    // -- prefetch (bufmgr.c) -----------------------------------------------

    /// `PrefetchSharedBuffer(smgr_reln, forkNum, blockNum)` (bufmgr.c:561) —
    /// initiate (or note as unnecessary) a prefetch of a shared buffer. If the
    /// block is already resident, report the buffer it was in (unpinned, must be
    /// rechecked); otherwise issue an asynchronous read-ahead via smgr.
    pub fn PrefetchSharedBuffer(
        &self,
        rlocator: RelFileLocator,
        backend: types_core::primitive::ProcNumber,
        fork_num: ForkNumber,
        block_num: BlockNumber,
    ) -> PgResult<PrefetchBufferResult> {
        debug_assert_ne!(block_num, InvalidBlockNumber);

        let rlocator_backend = RelFileLocatorBackend {
            locator: rlocator,
            backend,
        };
        let new_tag = make_tag(rlocator_backend, fork_num, block_num);
        let new_code = buf_table_hash_code(&new_tag);
        let new_part = buf_table_hash_partition(new_code);

        // See if the block is in the buffer pool already (shared partition lock).
        let guard = self.map_acquire(new_part, LWLockMode::LW_SHARED)?;
        let buf_id = self.buf_table().lookup(&new_tag, new_code);
        guard.release()?;

        let mut result = PrefetchBufferResult {
            recent_buffer: INVALID_BUFFER,
            initiated_io: false,
        };

        if buf_id < 0 {
            // Not in buffers: try to initiate an asynchronous read.  This returns
            // false in recovery if the relation file doesn't exist. Direct I/O
            // disables prefetch (PrefetchLocalBuffer's `io_direct_flags &
            // IO_DIRECT_DATA` guard, reached via the GUC getter seam).
            if !sb::io_direct_data::call()
                && smgr::smgrprefetch(rlocator_backend, fork_num, block_num, 1)?
            {
                result.initiated_io = true;
            }
        } else {
            // Report the buffer it was in at that time.  The caller may be able
            // to avoid a buffer-table lookup, but it's not pinned and it must be
            // rechecked!
            result.recent_buffer = buf_id + 1;
        }
        Ok(result)
    }

    /// `PrefetchBuffer(reln, forkNum, blockNum)` (bufmgr.c:651) — relcache-handle
    /// prefetch wrapper. For a shared relation this is exactly
    /// `PrefetchSharedBuffer`; the local/temp arm lives in localbuf.c.
    pub fn PrefetchBuffer(
        &self,
        rel: &Relation,
        fork_num: ForkNumber,
        block_num: BlockNumber,
    ) -> PgResult<PrefetchBufferResult> {
        debug_assert_ne!(block_num, InvalidBlockNumber);

        let bmr = BmrRead::new(rel)?;
        if bmr.relpersistence == RELPERSISTENCE_TEMP {
            // RelationUsesLocalBuffers(reln): a temp relation's pages live in the
            // backend-local pool. see comments in ReadBufferExtended: a temp
            // relation owned by another session cannot be accessed.
            if bmr.rlocator.backend != backend_storage_lmgr_proc_seams::my_proc_number::call() {
                return Err(PgError::error(
                    "cannot access temporary tables of other sessions",
                ));
            }
            // PrefetchLocalBuffer (bufmgr.c:665) — the temp/local pool lives in
            // localbuf.c (panic-until-owner). Dispatch through the outward seam,
            // mirroring C.
            return sb::prefetch_local_buffer::call(bmr.rlocator, fork_num, block_num);
        }
        // Pass it to the shared buffer version.
        self.PrefetchSharedBuffer(
            bmr.rlocator.locator,
            bmr.rlocator.backend,
            fork_num,
            block_num,
        )
    }

    // -- zero+lock helper (bufmgr.c) ---------------------------------------

    /// `ZeroAndLockBuffer(buffer, mode, already_valid)` (bufmgr.c:1031) — the
    /// `RBM_ZERO_AND_LOCK` / `RBM_ZERO_AND_CLEANUP_LOCK` helper. Take
    /// `BM_IO_IN_PROGRESS` (or discover `BM_VALID` set concurrently); if we got
    /// it, zero the page, grab the content lock before marking it valid, then
    /// terminate the I/O. If it was already valid, just take the lock the caller
    /// expects.
    pub fn ZeroAndLockBuffer(
        &self,
        buffer: Buffer,
        mode: ReadBufferMode,
        already_valid: bool,
    ) -> PgResult<()> {
        debug_assert!(matches!(
            mode,
            ReadBufferMode::ZeroAndLock | ReadBufferMode::ZeroAndCleanupLock
        ));

        if buffer_is_local(buffer) {
            return Err(PgError::error(
                "ZeroAndLockBuffer: local buffers are handled by the localbuf subsystem (not in this core)",
            ));
        }
        let buf_id = self.buffer_to_buf_id_pub(buffer)?;

        let need_to_zero = if already_valid {
            // If the caller already knew the buffer was valid, we can skip some
            // header interaction.  The caller just wants to lock the buffer.
            false
        } else {
            // Take BM_IO_IN_PROGRESS, or discover that BM_VALID has been set
            // concurrently.  Even though we aren't doing I/O, that ensures we
            // don't zero a page that someone else has pinned.
            self.start_buffer_io(buf_id, false, None)?
        };

        if need_to_zero {
            // memset(BufferGetPage(buffer), 0, BLCKSZ).
            self.zero_block(buf_id);

            // Grab the buffer content lock before marking the page as valid, so
            // no other backend sees the zeroed page before the caller has had a
            // chance to initialize it.  Since no-one else can be looking at the
            // page contents yet, there is no difference between an exclusive lock
            // and a cleanup-strength lock.
            lwlock::LWLockAcquire(
                self.content_lock(buf_id),
                LWLockMode::LW_EXCLUSIVE,
                backend_storage_lmgr_proc_seams::my_proc_number::call(),
            )?;

            // Set BM_VALID, terminate IO, and wake up any waiters.
            // TerminateBufferIO(bufHdr, false, BM_VALID, true, false) (bufmgr.c:1089).
            self.terminate_buffer_io(buf_id, false, BM_VALID, true, false)?;
        } else {
            // The buffer is valid, so we can't zero it.  The caller still expects
            // the page to be locked on return.
            if mode == ReadBufferMode::ZeroAndLock {
                self.LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE)?;
            } else {
                self.LockBufferForCleanup(buffer)?;
            }
        }
        Ok(())
    }

    // -- pin-for-block (bufmgr.c:1110) -------------------------------------

    /// `PinBufferForBlock(rel, smgr, persistence, forkNum, blockNum, strategy,
    /// foundPtr)` (bufmgr.c:1110) — pin a buffer for a given block. Returns the
    /// pinned [`Buffer`] and `found = true` iff the block was already present
    /// (else more work is required to read it in or zero it).
    fn PinBufferForBlock(
        &self,
        rlocator: RelFileLocatorBackend,
        persistence: u8,
        fork_num: ForkNumber,
        block_num: BlockNumber,
        io_context: IOContext,
    ) -> PgResult<(Buffer, bool)> {
        debug_assert_ne!(block_num, P_NEW);

        // Persistence should be set before.
        debug_assert!(
            persistence == RELPERSISTENCE_TEMP
                || persistence == RELPERSISTENCE_PERMANENT
                || persistence == RELPERSISTENCE_UNLOGGED
        );

        if persistence == RELPERSISTENCE_TEMP {
            // LocalBufferAlloc (bufmgr.c:1148) — the backend-local temp pool lives
            // in localbuf.c (panic-until-owner). Dispatch through the outward
            // seam, mirroring C.
            return sb::local_buffer_alloc::call(rlocator, fork_num, block_num);
        }

        // BufferAlloc finds or allocates the buffer in the shared pool, counting
        // IOOP_EVICT/REUSE in `io_context` for any victim eviction.
        let (buf_id, found) =
            self.buffer_alloc(rlocator, persistence, fork_num, block_num, io_context)?;
        Ok((buf_id_to_buffer(buf_id as i32), found))
    }

    // -- the synchronous single-block read core (bufmgr.c) -----------------

    /// `ReadBuffer_common(rel, smgr, smgr_persistence, forkNum, blockNum, mode,
    /// strategy)` (bufmgr.c:1193) — the shared read implementation. On a hit
    /// returns the pinned buffer; on a miss allocates a victim, reads the block
    /// (or zeroes it for the `RBM_ZERO_*` modes), verifies and marks it valid.
    ///
    /// The single-block read drives this synchronous core directly: the C path
    /// routes `RBM_NORMAL`/`RBM_ZERO_ON_ERROR` through `StartReadBuffer` +
    /// `WaitReadBuffers` with `READ_BUFFERS_SYNCHRONOUSLY`, which performs the
    /// readv inline (IOMETHOD_SYNC); modelling the inline transfer here is
    /// behaviour-equivalent and stays fully live without the AIO engine.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn read_buffer_common(
        &self,
        rlocator: Option<RelFileLocatorBackend>,
        persistence: u8,
        fork_num: ForkNumber,
        block_num: BlockNumber,
        mode: ReadBufferMode,
        io_context: IOContext,
        rel: Option<&Relation>,
    ) -> PgResult<Buffer> {
        // Backward compatibility path: most code should use ExtendBufferedRel()
        // instead. (bufmgr.c:1206)
        if block_num == P_NEW {
            let rel = rel.ok_or_else(|| {
                PgError::error("ReadBuffer_common: P_NEW requires a relcache entry (rel)")
            })?;
            let mut flags = EB_SKIP_EXTENSION_LOCK;
            // Since no-one else can be looking at the page contents yet, there is
            // no difference between an exclusive lock and a cleanup-strength lock.
            if mode == ReadBufferMode::ZeroAndLock || mode == ReadBufferMode::ZeroAndCleanupLock {
                flags |= EB_LOCK_FIRST;
            }
            return self.ExtendBufferedRel(rel, fork_num, io_context, flags);
        }

        let rlocator = rlocator.ok_or_else(|| {
            PgError::error("ReadBuffer_common: a non-P_NEW read requires a relation locator")
        })?;

        // RBM_ZERO_AND_{LOCK,CLEANUP_LOCK}: pin, then zero+lock (bufmgr.c:1226).
        if mode == ReadBufferMode::ZeroAndCleanupLock || mode == ReadBufferMode::ZeroAndLock {
            let (buffer, found) =
                self.PinBufferForBlock(rlocator, persistence, fork_num, block_num, io_context)?;
            self.ZeroAndLockBuffer(buffer, mode, found)?;
            return Ok(buffer);
        }

        // RBM_NORMAL / RBM_NORMAL_NO_LOG / RBM_ZERO_ON_ERROR: the synchronous
        // read pipeline. C signals READ_BUFFERS_SYNCHRONOUSLY and immediately
        // waits; we perform the inline transfer here.
        let mut flags = READ_BUFFERS_SYNCHRONOUSLY;
        if mode == ReadBufferMode::ZeroOnError {
            flags |= READ_BUFFERS_ZERO_ON_ERROR;
        }

        let (buffer, found) =
            self.PinBufferForBlock(rlocator, persistence, fork_num, block_num, io_context)?;

        // `io_context = IOContextForStrategy(strategy)` for a relation read
        // (bufmgr.c:1647) — threaded from the caller's strategy ring kind (or
        // IOCONTEXT_NORMAL when no strategy). For a temp relation the read-miss
        // path below re-accounts under IOOBJECT_TEMP_RELATION/NORMAL instead.

        // ReadBuffer_common per-relation + IO-object accounting (bufmgr.c:1160).
        // The "read" counter is bumped on every relcache-relation block request
        // (hit or miss); "hit" + IOOP_HIT only on a cache hit. The byte-tracked
        // IOOP_READ is accounted at the smgrreadv below (miss path only).
        if let Some(rel) = rel {
            sb::count_buffer_read::call(rel.rd_id, rel.rd_rel.relisshared, rel.pgstat_enabled);
            if found {
                sb::count_buffer_hit::call(rel.rd_id, rel.rd_rel.relisshared, rel.pgstat_enabled);
            }
        }
        if found {
            sb::count_io_op_hit::call(io_context, 1);
        }

        if found {
            // Already valid and pinned; nothing to read.
            return Ok(buffer);
        }

        // Temp/local relations: the buffer carries a NEGATIVE handle into the
        // backend-local pool, not a shared `buffer - 1` slot. C threads
        // `BufferIsLocal` through `StartReadBuffers`/`WaitReadBuffers`
        // (bufmgr.c:1310/1536/1553), dispatching to `StartLocalBufferIO` /
        // `TerminateLocalBufferIO` and reading into the local page. Mirror that
        // here as the synchronous single-block read; the shared `buf_id =
        // buffer - 1` index would underflow for a local handle.
        if buffer_is_local(buffer) {
            return self.read_local_buffer_miss(rlocator, fork_num, block_num, mode, buffer);
        }

        let buf_id = (buffer - 1) as usize;

        // Miss: we hold a fresh victim. StartBufferIO, then read (synchronously).
        if !self.start_buffer_io(buf_id, false, None)? {
            // Someone else completed it concurrently; it is now valid.
            return Ok(buffer);
        }

        // The buffer is owned by this backend's in-progress I/O
        // (BM_IO_IN_PROGRESS held), so the page bytes are ours to fill. Perform
        // the vectored read of this single block directly through the smgr seam
        // (the inline IOMETHOD_SYNC transfer).
        self.with_block_mut(buf_id, |dst| {
            let mut bufs: [&mut [u8]; 1] = [dst];
            smgr::smgrreadv(rlocator, fork_num, block_num, &mut bufs, 1)
        })?;

        // WaitReadBuffers IOOP_READ accounting (bufmgr.c:1957): one read op of
        // BLCKSZ bytes against the relation object. (The `pgBufferUsage` read
        // counter / per-relation tally for the miss are instrumentation-only here;
        // the per-relation `count_buffer_read` already fired above on the request.)
        sb::count_io_op_read::call(
            io_context,
            1,
            types_core::primitive::BLCKSZ as u64,
        );

        // Verify the just-read page before marking it valid: the synchronous form
        // of buffer_readv_complete_one (bufmgr.c:7088 / the RBM PageIsVerified
        // gate). On a verification failure RBM_ZERO_ON_ERROR zeroes the page and
        // continues with a WARNING (bufmgr.c:7346); any other mode fails the I/O
        // and raises ERROR (bufmgr.c:7338).
        let verified = self.with_block(buf_id, |bytes| {
            let p = page::PageRef::new(bytes)?;
            page::PageIsVerified(&p, block_num, types_storage::bufpage::PIV_LOG_LOG)
        })?;
        if !verified.0 {
            let path = relpath_str(rlocator, fork_num);
            if mode == ReadBufferMode::ZeroOnError {
                // ereport(WARNING): invalid page ... zeroing out page.
                backend_utils_error::emit_error_report_for(
                    &backend_utils_error::ereport(types_error::error::WARNING)
                        .errcode(types_error::error::ERRCODE_DATA_CORRUPTED)
                        .errmsg_internal(format!(
                            "invalid page in block {block_num} of relation \"{path}\"; zeroing out page"
                        ))
                        .into_error(),
                );
                self.zero_block(buf_id);
            } else {
                // Fail the I/O (clear BM_IO_IN_PROGRESS, set BM_IO_ERROR) so a
                // later WaitIO cannot block, then surface the corruption.
                self.terminate_buffer_io(
                    buf_id,
                    false,
                    types_storage::buf::BM_IO_ERROR,
                    true,
                    false,
                )?;
                return Err(PgError::new(
                    types_error::error::ERROR,
                    format!("invalid page in block {block_num} of relation \"{path}\""),
                )
                .with_sqlstate(types_error::error::ERRCODE_DATA_CORRUPTED));
            }
        }

        // Mark valid + terminate the I/O (forget_owner=true, release_aio=false:
        // this backend performed the synchronous read, bufmgr.c:1089).
        self.terminate_buffer_io(buf_id, false, BM_VALID, true, false)?;
        let _ = flags;
        Ok(buffer)
    }

    /// The `BufferIsLocal` arm of the synchronous single-block read miss
    /// (bufmgr.c's `StartReadBuffers`/`WaitReadBuffers` local path): a temp
    /// relation's victim local buffer is filled by reading the block off disk
    /// directly into the backend-local page. `StartLocalBufferIO` / the smgr
    /// readv / `PageIsVerified` / `TerminateLocalBufferIO` mirror the shared
    /// path, but every step routes through the local-buffer subsystem (the
    /// buffer handle is a NEGATIVE local index, never a shared `buffer - 1`
    /// slot). The buffer is already pinned by `PinBufferForBlock`.
    fn read_local_buffer_miss(
        &self,
        rlocator: RelFileLocatorBackend,
        fork_num: ForkNumber,
        block_num: BlockNumber,
        mode: ReadBufferMode,
        buffer: Buffer,
    ) -> PgResult<Buffer> {
        use backend_storage_buffer_support_seams as lb;

        // StartLocalBufferIO(bufHdr, true, false): false iff some path already
        // made the block valid (it cannot under synchronous single-process temp
        // I/O, but mirror C's early-out faithfully).
        if !lb::start_local_buffer_io::call(buffer, true, false)? {
            return Ok(buffer);
        }

        // Read the single block into the backend-local page bytes (the inline
        // IOMETHOD_SYNC transfer; temp relations are never WAL-logged).
        lb::local_buffer_with_page::call(buffer, &mut |dst: &mut [u8]| {
            let mut bufs: [&mut [u8]; 1] = [dst];
            smgr::smgrreadv(rlocator, fork_num, block_num, &mut bufs, 1)
        })?;

        // WaitReadBuffers IOOP_READ accounting for temp relations (bufmgr.c:1641
        // io_object = IOOBJECT_TEMP_RELATION): one read op of BLCKSZ bytes.
        sb::count_io_op_temp::call(
            types_pgstat::activity_pgstat::IOOp::IOOP_READ,
            1,
            types_core::primitive::BLCKSZ as u64,
        );

        // Verify the just-read page (the synchronous form of the RBM
        // PageIsVerified gate). Local buffers are never shared, so no content
        // lock is involved.
        let mut verified = (true, false);
        lb::local_buffer_with_page::call(buffer, &mut |bytes: &mut [u8]| {
            let p = page::PageRef::new(bytes)?;
            verified = page::PageIsVerified(&p, block_num, types_storage::bufpage::PIV_LOG_LOG)?;
            Ok(())
        })?;
        if !verified.0 {
            let path = relpath_str(rlocator, fork_num);
            if mode == ReadBufferMode::ZeroOnError {
                backend_utils_error::emit_error_report_for(
                    &backend_utils_error::ereport(types_error::error::WARNING)
                        .errcode(types_error::error::ERRCODE_DATA_CORRUPTED)
                        .errmsg_internal(format!(
                            "invalid page in block {block_num} of relation \"{path}\"; zeroing out page"
                        ))
                        .into_error(),
                );
                lb::local_buffer_with_page::call(buffer, &mut |bytes: &mut [u8]| {
                    bytes.fill(0);
                    Ok(())
                })?;
            } else {
                // Fail the I/O (set BM_IO_ERROR) so a later WaitIO cannot block,
                // then surface the corruption.
                lb::terminate_local_buffer_io::call(
                    buffer,
                    false,
                    types_storage::buf::BM_IO_ERROR,
                )?;
                return Err(PgError::new(
                    types_error::error::ERROR,
                    format!("invalid page in block {block_num} of relation \"{path}\""),
                )
                .with_sqlstate(types_error::error::ERRCODE_DATA_CORRUPTED));
            }
        }

        // Mark valid + terminate the local I/O (BM_VALID, no dirty-clear).
        lb::terminate_local_buffer_io::call(buffer, false, BM_VALID)?;
        Ok(buffer)
    }

    // -- the (multi-block, possibly async) read pipeline (bufmgr.c) --------

    /// `StartReadBuffers(operation, buffers, blockNum, nblocks, flags)`
    /// (bufmgr.c:1489) — begin reading a range of blocks, expecting forwarded
    /// buffers. `buffers` on entry holds `InvalidBuffer` or buffers forwarded by
    /// an earlier split call. On return `*nblocks` holds the number of blocks
    /// accepted by this operation; returns true iff `WaitReadBuffers` must be
    /// called.
    #[allow(clippy::too_many_arguments)]
    pub fn StartReadBuffers(
        &self,
        rel: &Relation,
        forknum: ForkNumber,
        buffers: &mut [Buffer],
        block_num: BlockNumber,
        nblocks: &mut i32,
        flags: u32,
        io_context: IOContext,
    ) -> PgResult<(ReadOp, bool)> {
        let bmr = BmrRead::new(rel)?;
        let mut operation = ReadBuffersOperation {
            rlocator: bmr.rlocator,
            persistence: bmr.relpersistence,
            forknum,
            blocknum: block_num,
            flags,
            buffers: Vec::new(),
            nblocks_done: 0,
            io_wref: wref_invalid(),
            io_result: 0,
            io_status: PGAIO_RS_UNKNOWN,
            io_context,
        };
        let did_start_io = self.start_read_buffers_impl(
            &mut operation,
            buffers,
            block_num,
            nblocks,
            flags,
            true, /* expect forwarded buffers */
        )?;
        Ok((ReadOp(operation), did_start_io))
    }

    /// `StartReadBuffer(operation, buffer, blocknum, flags)` (bufmgr.c:1508) —
    /// the single-block specialization. Does not support forwarded buffers.
    pub fn StartReadBuffer(
        &self,
        rel: &Relation,
        forknum: ForkNumber,
        buffer: &mut Buffer,
        blocknum: BlockNumber,
        flags: u32,
        io_context: IOContext,
    ) -> PgResult<(ReadOp, bool)> {
        let bmr = BmrRead::new(rel)?;
        let mut operation = ReadBuffersOperation {
            rlocator: bmr.rlocator,
            persistence: bmr.relpersistence,
            forknum,
            blocknum,
            flags,
            buffers: Vec::new(),
            nblocks_done: 0,
            io_wref: wref_invalid(),
            io_result: 0,
            io_status: PGAIO_RS_UNKNOWN,
            io_context,
        };
        let mut nblocks = 1;
        let mut slice = [INVALID_BUFFER];
        let result = self.start_read_buffers_impl(
            &mut operation,
            &mut slice,
            blocknum,
            &mut nblocks,
            flags,
            false, /* single block, no forwarding */
        )?;
        *buffer = slice[0];
        debug_assert_eq!(nblocks, 1, "single block can't be short");
        Ok((ReadOp(operation), result))
    }

    /// `StartReadBuffersImpl(operation, buffers, blockNum, nblocks, flags,
    /// allow_forwarding)` (bufmgr.c:1262) — begin a (multi-)block read. Pins each
    /// block (or accepts a forwarded already-pinned buffer), splits the run at
    /// the first hit / smgr combine limit, then starts the (a)synchronous read.
    /// Returns true iff `WaitReadBuffers` must be called.
    #[allow(clippy::too_many_arguments)]
    fn start_read_buffers_impl(
        &self,
        operation: &mut ReadBuffersOperation,
        buffers: &mut [Buffer],
        block_num: BlockNumber,
        nblocks: &mut i32,
        flags: u32,
        allow_forwarding: bool,
    ) -> PgResult<bool> {
        let mut actual_nblocks = *nblocks;
        let did_start_io;

        debug_assert!(*nblocks == 1 || allow_forwarding);
        debug_assert!(*nblocks > 0);
        debug_assert!(*nblocks <= MAX_IO_COMBINE_LIMIT);

        let rlocator = operation.rlocator;
        let fork_num = operation.forknum;
        let persistence = operation.persistence;
        let io_context = operation.io_context;

        let mut i = 0;
        while i < actual_nblocks {
            let found;

            if allow_forwarding && buffers[i as usize] != INVALID_BUFFER {
                // A buffer pinned by an earlier StartReadBuffers() that couldn't
                // be handled in one operation, forwarded back to us. It might be
                // an already valid buffer (a hit) or a buffer some other backend
                // made valid; either way handle it as a hit now. It is safe to
                // check BM_VALID with a relaxed load, because we got a fresh view
                // of it while pinning it in the previous call.
                let fwd = buffers[i as usize];
                if buffer_is_local(fwd) {
                    return Err(PgError::error(
                        "StartReadBuffers: local buffers are handled by the localbuf subsystem (not in this core)",
                    ));
                }
                debug_assert_eq!(
                    self.BufferGetBlockNumber(fwd)?,
                    block_num.wrapping_add(i as u32)
                );
                let bid = self.buffer_to_buf_id_pub(fwd)?;
                let buf_state = self.read_state(bid);
                debug_assert!(buf_state & BM_TAG_VALID != 0);
                found = buf_state & BM_VALID != 0;
            } else {
                let (buf, f) = self.PinBufferForBlock(
                    rlocator,
                    persistence,
                    fork_num,
                    block_num.wrapping_add(i as u32),
                    io_context,
                )?;
                buffers[i as usize] = buf;
                found = f;
            }

            if found {
                // We have a hit.  If it's the first block in the requested range,
                // we can return it immediately and report that WaitReadBuffers()
                // does not need to be called.
                if i == 0 {
                    *nblocks = 1;
                    // Initialize enough of the operation for the assertions.
                    operation.buffers.clear();
                    operation
                        .buffers
                        .push(self.buffer_to_buf_id_pub(buffers[0])? as i32);
                    operation.blocknum = block_num;
                    operation.nblocks_done = 1;
                    self.check_read_buffers_operation(operation, true);
                    return Ok(false);
                }

                // Otherwise we already have an I/O to perform, but this block
                // can't be included as it is already valid.  Split the I/O here;
                // leave this buffer pinned, forwarding it to the next call.
                actual_nblocks = i;
                break;
            } else {
                // Check how many blocks we can cover with the same IO. The smgr
                // implementation might e.g. be limited due to a segment boundary.
                if i == 0 && actual_nblocks > 1 {
                    let maxcombine =
                        smgr::smgrmaxcombine(rlocator, fork_num, block_num) as i32;
                    if maxcombine < actual_nblocks {
                        // elog(DEBUG2, "limiting nblocks ...").
                        actual_nblocks = maxcombine;
                    }
                }
            }
            i += 1;
        }
        *nblocks = actual_nblocks;

        // Populate the operation from the pinned run.
        operation.buffers.clear();
        for &b in buffers.iter().take(actual_nblocks as usize) {
            operation.buffers.push(self.buffer_to_buf_id_pub(b)? as i32);
        }
        operation.blocknum = block_num;
        operation.flags = flags;
        operation.nblocks_done = 0;
        operation.io_wref = wref_invalid();
        operation.io_status = PGAIO_RS_UNKNOWN;
        operation.io_result = 0;

        // Try to start IO. AsyncReadBuffers drives the read (the aio seam performs
        // the actual transfer). It might not cover the entire requested range,
        // e.g. because an intermediary block has been read in by another backend;
        // that is signalled by decrementing *nblocks AND reducing
        // operation.buffers (the trailing pinned buffers are "forwarded" by
        // read_stream.c to the next call).
        let mut progress = *nblocks;
        did_start_io = self.async_read_buffers(operation, &mut progress)?;
        *nblocks = progress;
        operation.buffers.truncate(*nblocks as usize);

        self.check_read_buffers_operation(operation, !did_start_io);
        Ok(did_start_io)
    }

    /// `CheckReadBuffersOperation(operation, is_complete)` (bufmgr.c:1527) —
    /// sanity checks on the in-flight read (assertions only).
    fn check_read_buffers_operation(&self, operation: &ReadBuffersOperation, is_complete: bool) {
        debug_assert!(operation.nblocks_done as usize <= operation.buffers.len());
        debug_assert!(!is_complete || operation.buffers.len() as u32 == operation.nblocks_done);

        let blocknum = operation.blocknum;
        for (i, &buf_id) in operation.buffers.iter().enumerate() {
            let buffer = buf_id_to_buffer(buf_id);
            debug_assert_eq!(
                self.BufferGetBlockNumber(buffer).unwrap_or(InvalidBlockNumber),
                blocknum.wrapping_add(i as u32)
            );
            debug_assert!(self.read_state(buf_id as usize) & BM_TAG_VALID != 0);
            if (i as u32) < operation.nblocks_done {
                debug_assert!(self.read_state(buf_id as usize) & BM_VALID != 0);
            }
        }
    }

    /// `ReadBuffersCanStartIOOnce(buffer, nowait)` (bufmgr.c:1551) — helper for
    /// [`Self::read_buffers_can_start_io`]: try to take `BM_IO_IN_PROGRESS`.
    fn read_buffers_can_start_io_once(&self, buffer: Buffer, nowait: bool) -> PgResult<bool> {
        if buffer_is_local(buffer) {
            return Err(PgError::error(
                "ReadBuffersCanStartIO: local buffers are handled by the localbuf subsystem (not in this core)",
            ));
        }
        self.start_buffer_io(self.buffer_to_buf_id_pub(buffer)?, nowait, None)
    }

    /// `ReadBuffersCanStartIO(buffer, nowait)` (bufmgr.c:1564) — get the buffer
    /// ready for I/O, submitting any staged AIO first to avoid deadlocks. There
    /// is no separately-staged AIO queue at this layer
    /// (`pgaio_have_staged()`/`pgaio_submit_staged()` reduce to constant no-ops
    /// in the synchronous engine), so this is exactly
    /// [`Self::read_buffers_can_start_io_once`].
    fn read_buffers_can_start_io(&self, buffer: Buffer, nowait: bool) -> PgResult<bool> {
        // if (!nowait && pgaio_have_staged()) pgaio_submit_staged();
        self.read_buffers_can_start_io_once(buffer, nowait)
    }

    /// `ProcessReadBuffersResult(operation)` (bufmgr.c:1593) — post-completion
    /// bookkeeping for a readv. Reads the ACTUAL number of blocks the AIO
    /// completion recorded (NOT assuming `io_buffers_len`), reporting/raising on
    /// ERROR/WARNING and emitting a debug message on a PARTIAL read (which
    /// `WaitReadBuffers` retries), then advances `nblocks_done` by that count.
    fn process_read_buffers_result(&self, operation: &mut ReadBuffersOperation) -> PgResult<()> {
        let rs = operation.io_status;
        let mut newly_read_blocks: i32 = 0;

        // Assert(pgaio_wref_valid(&operation->io_wref)).
        debug_assert!(wref_valid(operation.io_wref));
        // Assert(aio_ret->result.status != PGAIO_RS_UNKNOWN).
        debug_assert_ne!(rs, PGAIO_RS_UNKNOWN);

        // SMGR reports the number of blocks successfully read as the IO result.
        if rs != PGAIO_RS_ERROR {
            newly_read_blocks = operation.io_result;
        }

        if rs == PGAIO_RS_ERROR || rs == PGAIO_RS_WARNING {
            // pgaio_result_report(...): the completion callback already reported
            // through the engine; the WARNING/ERROR is surfaced by the aio seam's
            // Err on the wait. For ERROR the read raises; for WARNING it logged
            // already (handled inside the engine). No extra work here.
            if rs == PGAIO_RS_ERROR {
                return Err(PgError::error(format!(
                    "could not read blocks {}..{} in relation \"{}\"",
                    operation.blocknum,
                    operation.blocknum + operation.buffers.len() as u32 - 1,
                    relpath_str(operation.rlocator, operation.forknum)
                )));
            }
        }
        // PARTIAL: we'll retry; the completion path already advanced what it read.

        debug_assert!(newly_read_blocks > 0);
        debug_assert!(newly_read_blocks <= MAX_IO_COMBINE_LIMIT);

        operation.nblocks_done += newly_read_blocks as u32;
        debug_assert!(operation.nblocks_done as usize <= operation.buffers.len());
        Ok(())
    }

    /// `WaitReadBuffers(operation)` (bufmgr.c:1632) — block until a started read
    /// finishes. Re-issues I/O until every block of the run is done (handling
    /// partial reads and blocks concurrently read in by other backends).
    pub fn WaitReadBuffers(&self, op: &mut ReadOp) -> PgResult<()> {
        let operation = &mut op.0;
        // To handle partial reads, and IOMETHOD_SYNC, we re-issue IO until we're
        // done. We may need multiple retries, not just for multiple partial
        // reads, but also because some remaining to-be-read buffers may have been
        // read in by other backends, limiting the IO size.
        loop {
            let mut ignored_nblocks_progress = 0;

            self.check_read_buffers_operation(operation, false);

            // If there is an IO associated with the operation, we may need to
            // wait for it.
            if wref_valid(operation.io_wref) {
                // Track the time spent waiting for the IO to complete. As tracking
                // a wait even if we don't actually need to wait is not cheap, we
                // first check if the IO is already complete.
                if operation.io_status == PGAIO_RS_UNKNOWN {
                    // pgaio_wref_wait(&operation->io_wref): block until done and
                    // refresh the issuer-owned io_return slot with the result.
                    //
                    // C only takes the wait-time instrumentation branch when the
                    // IO is not yet complete (`!pgaio_wref_check_done`), because
                    // there the completion has already written the result through
                    // the issuer's `&operation->io_return` pointer. In the
                    // value-typed engine the completed result is published into a
                    // backend-local slot that this seam reads back, so we must
                    // fetch it here whether or not the IO already finished
                    // (`pgaio_wref_wait` on an already-complete IO returns
                    // immediately). The `wref_check_done` probe is retained only
                    // to gate the (deferred) wait-time accounting.
                    let _already_done = sb::wref_check_done::call(operation.io_wref)?;
                    let (result, status) = sb::wait_read_buffers::call(operation.io_wref)?;
                    operation.io_result = result;
                    operation.io_status = status;
                    // pgstat_count_io_op_time(... wait-time ...): cnt 0, bytes 0,
                    // deferred (instrumentation only).
                }

                // We now are sure the IO completed. Check the results (reports on
                // errors if any) and advance nblocks_done.
                self.process_read_buffers_result(operation)?;
            }

            // Most of the time the one IO we already started reads in everything.
            // But we need to deal with partial reads and buffers not needing IO
            // anymore.
            if operation.nblocks_done as usize == operation.buffers.len() {
                break;
            }

            backend_tcop_postgres_seams::check_for_interrupts::call()?;

            // This may only complete the IO partially. NB: unlike after
            // AsyncReadBuffers() in StartReadBuffers(), we do *not* reduce
            // operation.buffers here; callers expect the full operation to be
            // completed at this point (as more operations may have been queued).
            self.async_read_buffers(operation, &mut ignored_nblocks_progress)?;
            let _ = ignored_nblocks_progress;
        }

        self.check_read_buffers_operation(operation, true);
        Ok(())
    }

    /// `AsyncReadBuffers(operation, nblocks_progress)` (bufmgr.c:1764) — initiate
    /// I/O for the [`ReadBuffersOperation`]. Starts a single I/O at a time; the
    /// size may be limited below the to-be-read blocks if one was concurrently
    /// read in. If the first to-be-read buffer is already valid, no I/O is issued
    /// (`*nblocks_progress = 1`, `nblocks_done` incremented). Returns true iff
    /// I/O was initiated. To support retries after partial reads, the first
    /// `operation.nblocks_done` buffers are skipped.
    fn async_read_buffers(
        &self,
        operation: &mut ReadBuffersOperation,
        nblocks_progress: &mut i32,
    ) -> PgResult<bool> {
        let blocknum = operation.blocknum;
        let fork_num = operation.forknum;
        let rlocator = operation.rlocator;
        let nblocks_done = operation.nblocks_done as i32;
        let total = operation.buffers.len() as i32;
        let did_start_io;

        // PGAIO_HF_SYNCHRONOUS / READ_BUFFERS_SYNCHRONOUSLY flags are carried to
        // the aio seam so the engine sets the handle flags. (The flag promotion
        // of zero_damaged_pages / ignore_checksum_failure is session GUC state
        // consulted by the page verify at completion time; no-op at this layer.)
        let _ = operation.flags & READ_BUFFERS_ZERO_ON_ERROR;

        let head = operation.buffers[nblocks_done as usize];
        let head_buffer = buf_id_to_buffer(head);

        // Check if we can start IO on the first to-be-read buffer.  If an I/O is
        // already in progress in another backend, we want to wait for the outcome.
        if !self.read_buffers_can_start_io(head_buffer, false)? {
            // Someone else has already completed this block, we're done.  When IO
            // is necessary, ->nblocks_done is updated in
            // ProcessReadBuffersResult(); that is not called when no IO is
            // necessary, so update here.
            operation.nblocks_done += 1;
            *nblocks_progress = 1;

            // pgaio_io_release(ioh); pgaio_wref_clear(&operation->io_wref).
            operation.io_wref = wref_invalid();
            did_start_io = false;
            // Report this as a 'hit' (pgstat, deferred).
        } else {
            // We found a buffer that we need to read in. Acquire an AIO handle for
            // this operation's read.
            let io_wref = sb::pgaio_io_acquire::call()?;
            operation.io_wref = io_wref;

            // Build the scatter-read run: the head plus as many consecutive
            // neighbours as we can start IO on without blocking. The AIO layer
            // (`buffer_stage_common` / `pgaio_perform_io_syscall` /
            // `buffer_readv_complete`) keys the run on `Buffer` values (1-based
            // for shared, negative for local), matching C's
            // `ReadBuffersOperation.buffers[]`; `operation.buffers` stores
            // 0-based buf_ids, so convert each entry through `buf_id_to_buffer`.
            let mut io_buffers: Vec<i32> = vec![head_buffer];
            let mut io_buffers_len = 1i32;
            let mut idx = nblocks_done + 1;
            while idx < total {
                let b = buf_id_to_buffer(operation.buffers[idx as usize]);
                if !self.read_buffers_can_start_io(b, true)? {
                    break;
                }
                // Must be consecutive block numbers.
                debug_assert_eq!(
                    self.BufferGetBlockNumber(buf_id_to_buffer(
                        operation.buffers[(idx - 1) as usize]
                    ))?,
                    self.BufferGetBlockNumber(b)? - 1
                );
                io_buffers.push(b);
                io_buffers_len += 1;
                idx += 1;
            }

            // Register the buffer-readv completion callbacks + the run on the
            // handle, then submit the vectored read. In IOMETHOD_SYNC the readv
            // happens inline and the shared completion callback runs before the
            // submit returns (it writes the io_return slot the wait reads).
            let synchronous = operation.flags & READ_BUFFERS_SYNCHRONOUSLY != 0;
            let is_temp = operation.persistence == RELPERSISTENCE_TEMP;
            sb::pgaio_register_callbacks::call(
                io_wref,
                &io_buffers[..],
                operation.flags as u8,
                synchronous,
                is_temp,
            )?;
            sb::start_read_buffers::call(
                io_wref,
                rlocator,
                fork_num,
                blocknum.wrapping_add(nblocks_done as u32),
                io_buffers_len,
            )?;
            // pgstat_count_io_op_time(IOOBJECT_RELATION, ..., IOOP_READ, 1,
            //   io_buffers_len * BLCKSZ): the timed counter, deferred (pgstat).
            let _ = (io_buffers_len as u64) * BLCKSZ as u64;

            // NB: ->nblocks_done is NOT advanced here. The actual blocks-read
            // count comes from the completion path (ProcessReadBuffersResult,
            // called by WaitReadBuffers).
            *nblocks_progress = io_buffers_len;
            did_start_io = true;
        }
        Ok(did_start_io)
    }

    /// `RelationCopyStorageUsingBuffer(srclocator, dstlocator, forkNum,
    /// permanent)` (bufmgr.c:5126) — copy one fork's data using the buffer
    /// manager. Same as `RelationCopyStorage` but uses the bufmgr read/extend
    /// APIs instead of raw `smgrread`/`smgrextend`.
    ///
    /// The C uses a `ReadStream` over the source for prefetch; this model has
    /// no read-stream, so the source blocks are read directly with
    /// `ReadBufferWithoutRelcache` in the per-block loop (behaviour-identical:
    /// the read stream only affects prefetch scheduling, not the bytes copied),
    /// exactly as `src-idiomatic`'s proven port collapses it.
    pub fn RelationCopyStorageUsingBuffer(
        &self,
        srclocator: RelFileLocator,
        dstlocator: RelFileLocator,
        fork_num: ForkNumber,
        permanent: bool,
    ) -> PgResult<()> {
        use types_storage::buf::BUFFER_LOCK_SHARE;

        // In general, we want to write WAL whenever wal_level > 'minimal', but
        // we can skip it when copying any fork of an unlogged relation other
        // than the init fork.
        let use_wal = backend_access_transam_xlog_seams::wal_level::call()
            >= types_wal::xlog_consts::WAL_LEVEL_REPLICA
            && (permanent || fork_num == ForkNumber::INIT_FORKNUM);

        // Get number of blocks in the source relation.
        let src_key = RelFileLocatorBackend {
            locator: srclocator,
            backend: INVALID_PROC_NUMBER,
        };
        let nblocks = smgr::smgrnblocks(src_key, fork_num)?;

        // Nothing to copy; just return.
        if nblocks == 0 {
            return Ok(());
        }

        // Bulk extend the destination relation to the same size as the source
        // relation before starting to copy block by block. memset(buf.data, 0,
        // BLCKSZ); smgrextend(smgropen(dstlocator), forkNum, nblocks - 1, buf,
        // true).
        let dst_key = RelFileLocatorBackend {
            locator: dstlocator,
            backend: INVALID_PROC_NUMBER,
        };
        let zero_page = [0u8; BLCKSZ as usize];
        smgr::smgrextend(dst_key, fork_num, nblocks - 1, &zero_page, true)?;

        // This is a bulk operation, so use buffer access strategies: the source
        // reads use a BAS_BULKREAD ring (IOCONTEXT_BULKREAD), the destination
        // writes a BAS_BULKWRITE ring (IOCONTEXT_BULKWRITE). The ring objects are
        // collapsed in this core, but their context KINDs reach pg_stat_io.
        // Iterate over each block of the source relation file.
        for blkno in 0..nblocks {
            // CHECK_FOR_INTERRUPTS().
            backend_tcop_postgres_seams::check_for_interrupts::call()?;

            // Read block from source relation (C: read_stream_next_buffer).
            let src_buf = self.ReadBufferWithoutRelcache(
                srclocator,
                permanent,
                fork_num,
                blkno,
                ReadBufferMode::Normal,
                IOContext::IOCONTEXT_BULKREAD,
            )?;
            self.LockBuffer(src_buf, BUFFER_LOCK_SHARE)?;

            // dstBuf = ReadBufferWithoutRelcache(dstlocator, forkNum, blkno,
            //   RBM_ZERO_AND_LOCK, bstrategy_dst, permanent).
            let dst_buf = self.ReadBufferWithoutRelcache(
                dstlocator,
                permanent,
                fork_num,
                blkno,
                ReadBufferMode::ZeroAndLock,
                IOContext::IOCONTEXT_BULKWRITE,
            )?;

            // START_CRIT_SECTION().
            misc::start_crit_section::call();
            // Copy page data from the source to the destination
            // (memcpy(dstPage, srcPage, BLCKSZ)) and mark the destination dirty.
            let mut page_image = [0u8; BLCKSZ as usize];
            self.with_page_bytes(src_buf, |src| {
                page_image.copy_from_slice(&src[..BLCKSZ as usize]);
            })?;
            self.with_page_bytes_mut(dst_buf, &mut |dst| {
                dst[..BLCKSZ as usize].copy_from_slice(&page_image);
                Ok(())
            })?;
            self.MarkBufferDirty(dst_buf)?;

            // WAL-log the copied page.
            if use_wal {
                backend_access_transam_xloginsert_seams::log_newpage_buffer::call(
                    dst_buf, true,
                )?;
            }
            // END_CRIT_SECTION().
            misc::end_crit_section::call();

            self.UnlockReleaseBuffer(dst_buf)?;
            self.UnlockReleaseBuffer(src_buf)?;
        }

        Ok(())
    }
}

/// An opaque, owned handle to an in-flight read operation, returned by
/// `StartReadBuffers`/`StartReadBuffer` and consumed by `WaitReadBuffers`. Wraps
/// the crate-local [`ReadBuffersOperation`] (the C `ReadBuffersOperation` is an
/// on-stack caller-owned value; the consumer here owns it across the
/// Start/Wait boundary exactly the same way).
pub struct ReadOp(ReadBuffersOperation);
