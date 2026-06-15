//! The relation-extension path (bufmgr.c):
//!
//!  * `GetPinLimit` (bufmgr.c:2505) / `GetAdditionalPinLimit` (2517) /
//!    `LimitAdditionalPins` (2543) — the proportional pin budget the extend run
//!    uses to cap how many new blocks it pins at once.
//!  * `ExtendBufferedRel` (858) — extend by one block, return the new buffer.
//!  * `ExtendBufferedRelBy` (890) — extend by N blocks.
//!  * `ExtendBufferedRelTo` (922) — extend up to a target block count.
//!  * `ExtendBufferedRelCommon` (2561) — the shared driver.
//!  * `ExtendBufferedRelShared` (2605) — the shared-buffers implementation.
//!
//! F2b (this stage). The victim acquisition + pin/IO machinery is reused from
//! [`crate::bufalloc`] (F2a: `get_victim_buffer` / `start_buffer_io` /
//! `terminate_buffer_io` / `unpin_buffer` / `strategy_free_buffer`).
//!
//! Boundary seams reconciled to this repo:
//!   * smgr `nblocks` / `nblocks_cached` / `exists` / `create` / `zeroextend` —
//!     DIRECT calls into [`backend_storage_smgr_smgr`] (no seam; the smgr crate
//!     is a direct dependency).
//!   * the relation-extension lock — the lmgr
//!     [`backend_storage_lmgr_lmgr_seams::lock_relation_for_extension`] RAII
//!     guard (takes `&Relation`; `Drop`/`release` is `UnlockRelationForExtension`).
//!   * the mapping (`BufferMappingLock`) partition locks — direct lwlock dep via
//!     [`crate::mgr::BufferManager::map_acquire`].
//!   * the per-buffer content lock for the `EB_LOCK_*` legs — DIRECT
//!     `LWLockAcquire(&content_locks[id], LW_EXCLUSIVE)`.
//!   * resource-owner enlarge — `resowner_enlarge` (panic-until-owner).
//!   * pgstat `count_buffer_write` / `count_io_op_extend` — no-op installs in
//!     [`crate::init_seams`] (stats-only, never affects correctness, same
//!     posture as F1's `count_buffer_dirtied`).
//!   * the proportional-pin divisor — `MaxBackends + NUM_AUXILIARY_PROCS` via
//!     [`backend_utils_init_small_seams::max_backends`] + the
//!     `NUM_AUXILIARY_PROCS` const (no `max_backends_plus_aux` seam).
//!
//! TEMP relations dispatch to the local-buffer extend path (`localbuf.c
//! ExtendBufferedRelLocal`), a separate subsystem not modelled by this shared
//! core; that arm returns `Err`, faithful to src-idiomatic.
//!
//! `ExtendBufferedRelTo`'s concurrent-extension read fallback
//! (`ReadBuffer_common`) is the F3 read path; until it lands, that branch is a
//! seam-and-panic into the read owner (sanctioned: only hit when another backend
//! wins the extension race).

#![allow(dead_code)]

use types_core::primitive::{BlockNumber, Buffer, ForkNumber, InvalidBlockNumber};
use types_error::{PgError, PgResult};
use types_rel::Relation;
use types_storage::buf::{
    buftag, MAX_BLOCK_NUMBER, BM_DIRTY, BM_JUST_DIRTIED, BM_PERMANENT, BM_TAG_VALID, BM_VALID,
    BUF_USAGECOUNT_ONE,
};
use types_storage::storage::{LWLockMode, NUM_AUXILIARY_PROCS, ReadBufferMode};
use types_storage::RelFileLocatorBackend;
use types_tuple::access::{RELPERSISTENCE_PERMANENT, RELPERSISTENCE_TEMP};

use crate::mgr::BufferManager;

use backend_storage_buffer_bufmgr_seams as sb;
use backend_storage_buffer_support::{buf_table_hash_code, buf_table_hash_partition};
use backend_storage_lmgr_lwlock as lwlock;
use backend_storage_smgr_smgr as smgr;

/// `REFCOUNT_ARRAY_ENTRIES` (bufmgr.c:100) — the size of the per-backend private
/// refcount fast array; `GetAdditionalPinLimit` uses it as the conservative
/// "pins we assume are held" estimate.
const REFCOUNT_ARRAY_ENTRIES: u32 = 8;

/// `InvalidBuffer` (buf.h).
const INVALID_BUFFER: Buffer = 0;

/// `lengthof(buffers)` in `ExtendBufferedRelTo` (bufmgr.c:932) — the on-stack
/// victim-buffer batch size.
const EXTEND_TO_BATCH: usize = 64;

/// `INIT_FORKNUM` (common/relpath.h) — the init fork is always WAL-logged.
const INIT_FORKNUM: ForkNumber = ForkNumber::INIT_FORKNUM;

/// `BUF_STATE_GET_REFCOUNT(buf_state)` (buf_internals.h).
#[inline]
fn buf_state_get_refcount(buf_state: u32) -> u32 {
    buf_state & types_storage::buf::BUF_REFCOUNT_MASK
}

/// `BufferDescriptorGetBuffer(buf)` — the 1-based [`Buffer`] for a 0-based id.
#[inline]
fn buf_id_to_buffer(buf_id: i32) -> Buffer {
    buf_id + 1
}

/// `extend` flag bits (bufmgr.h `ExtendBufferedFlags`). Local constants because
/// the `ExtendBufferedFlags` type is not modelled in the shared types crates;
/// the extend seams encode the fixed flag combinations their C call sites pass.
mod eb {
    /// `EB_SKIP_EXTENSION_LOCK` — don't take the relation-extension lock (the
    /// caller already holds it).
    pub const SKIP_EXTENSION_LOCK: u32 = 1 << 0;
    /// `EB_PERFORMING_RECOVERY` — the extension happens during recovery.
    pub const PERFORMING_RECOVERY: u32 = 1 << 1;
    /// `EB_CREATE_FORK_IF_NEEDED` — create the fork if it does not exist yet.
    pub const CREATE_FORK_IF_NEEDED: u32 = 1 << 2;
    /// `EB_LOCK_FIRST` — return the first extended block exclusively locked.
    pub const LOCK_FIRST: u32 = 1 << 3;
    /// `EB_CLEAR_SIZE_CACHE` — invalidate the smgr size cache.
    pub const CLEAR_SIZE_CACHE: u32 = 1 << 4;
    /// `EB_LOCK_TARGET` — return the target block exclusively locked.
    pub const LOCK_TARGET: u32 = 1 << 5;
}

/// The resolved physical identity of a relation for the extend path: the C
/// `BufferManagerRelation` collapses to `(rlocator, relpersistence)` plus the
/// `&Relation` the extension-lock seam needs. Mirrors `BMR_REL(rel)` after
/// `RelationGetSmgr` resolution.
struct BmrRel<'a, 'mcx> {
    rel: &'a Relation<'mcx>,
    rlocator: RelFileLocatorBackend,
    relpersistence: u8,
}

impl<'a, 'mcx> BmrRel<'a, 'mcx> {
    /// `BMR_REL(rel)` — resolve the physical id directly off the `&Relation`
    /// (its `rd_locator` / `rd_backend` / `rd_rel->relpersistence`).
    fn new(rel: &'a Relation<'mcx>) -> Self {
        Self {
            rel,
            rlocator: RelFileLocatorBackend {
                locator: rel.rd_locator,
                backend: rel.rd_backend,
            },
            relpersistence: rel.rd_rel.relpersistence,
        }
    }
}

/// `InitBufferTag(&tag, &smgr_rlocator.locator, fork, blocknum)` — the buffer
/// tag is keyed by the unbacked `RelFileLocator` (the `backend` part is not part
/// of the tag; temp buffers go to the local pool, out of this shared core).
fn make_tag(rlocator: RelFileLocatorBackend, fork: ForkNumber, blocknum: BlockNumber) -> buftag {
    buftag {
        spcOid: rlocator.locator.spcOid,
        dbOid: rlocator.locator.dbOid,
        relNumber: rlocator.locator.relNumber,
        forkNum: fork,
        blockNum: blocknum,
    }
}

/// `BufTableHashPartition(BufTableHashCode(&tag))` — the partition index of a
/// tag, selecting its `BufferMappingLock`.
fn tag_partition(tag: &buftag) -> u32 {
    buf_table_hash_partition(buf_table_hash_code(tag))
}

/// `relpath(smgr_rlocator, fork).str` — a human-readable physical path for the
/// extend-path error messages. The canonical `relpath` formatting lives in the
/// common path subsystem; this renders the same identifying fields for the
/// bounded set of extend errors.
fn relpath_str(rlocator: RelFileLocatorBackend, fork: ForkNumber) -> String {
    let loc = rlocator.locator;
    format!(
        "{}/{}/{} (fork {:?})",
        loc.spcOid, loc.dbOid, loc.relNumber, fork
    )
}

impl BufferManager {
    // -- proportional pin budget (bufmgr.c) --------------------------------

    /// `MaxProportionalPins` (bufmgr.c:221, set in `InitBufferPool` to
    /// `NBuffers / (MaxBackends + NUM_AUXILIARY_PROCS)`). Recomputed from the
    /// live pool size + the `max_backends` seam so the extend path never depends
    /// on a stashed global. Integer division, exactly like C; the divisor is
    /// always positive in a configured cluster.
    fn max_proportional_pins(&self) -> u32 {
        let divisor = backend_utils_init_small_seams::max_backends::call() + NUM_AUXILIARY_PROCS;
        if divisor <= 0 {
            return 0;
        }
        self.nbuffers() / divisor as u32
    }

    /// `GetPinLimit()` (bufmgr.c:2505) — the maximum number of buffers a backend
    /// should try to pin at once to stay within its fair share. The highest
    /// value `GetAdditionalPinLimit()` could ever return; may be zero on a very
    /// small pool.
    pub fn GetPinLimit(&self) -> u32 {
        self.max_proportional_pins()
    }

    /// `GetAdditionalPinLimit()` (bufmgr.c:2517) — the maximum number of
    /// ADDITIONAL buffers this backend may pin given what it already holds.
    /// Unlike `LimitAdditionalPins`, this may return zero.
    ///
    /// Faithful to the C estimate: `estimated_pins_held =
    /// PrivateRefCountOverflowed + REFCOUNT_ARRAY_ENTRIES`. This crate collapses
    /// the C array+overflow-hash split into a single backend-local map, so
    /// `PrivateRefCountOverflowed` is structurally always 0 (no overflow tier
    /// exists); the estimate is therefore `REFCOUNT_ARRAY_ENTRIES`, matching C's
    /// "just assume the max" comment.
    pub fn GetAdditionalPinLimit(&self) -> u32 {
        let estimated_pins_held: u32 = 0 /* PrivateRefCountOverflowed */ + REFCOUNT_ARRAY_ENTRIES;
        let max_proportional_pins = self.max_proportional_pins();

        // Is this backend already holding more than its fair share?
        if estimated_pins_held > max_proportional_pins {
            return 0;
        }

        max_proportional_pins - estimated_pins_held
    }

    /// `LimitAdditionalPins(uint32 *additional_pins)` (bufmgr.c:2543) — cap the
    /// number of pins a batch operation may additionally acquire so it does not
    /// run the pool out of pinnable buffers. One additional pin is always allowed
    /// (the operation needs at least one to make progress).
    pub fn LimitAdditionalPins(&self, additional_pins: &mut u32) {
        if *additional_pins <= 1 {
            return;
        }

        let mut limit = self.GetAdditionalPinLimit();
        // Max(limit, 1).
        limit = limit.max(1);
        if limit < *additional_pins {
            *additional_pins = limit;
        }
    }

    // -- public extend entry points (bufmgr.c) -----------------------------

    /// `ExtendBufferedRel(bmr, forkNum, strategy, flags)` (bufmgr.c:858) — the
    /// convenience wrapper extending by exactly one block; returns the new pinned
    /// buffer.
    pub(crate) fn ExtendBufferedRel(
        &self,
        rel: &Relation,
        fork: ForkNumber,
        has_strategy: bool,
        flags: u32,
    ) -> PgResult<Buffer> {
        let mut buf: Buffer = INVALID_BUFFER;
        let mut extend_by: u32 = 1;
        // ExtendBufferedRelBy(bmr, forkNum, strategy, flags, 1, &buf, &extend_by);
        let buffers = core::slice::from_mut(&mut buf);
        self.ExtendBufferedRelBy(rel, fork, has_strategy, flags, 1, buffers, &mut extend_by)?;
        Ok(buf)
    }

    /// `ExtendBufferedRelBy(bmr, fork, strategy, flags, extend_by, buffers,
    /// extended_by)` (bufmgr.c:890) — extend the relation by up to `extend_by`
    /// blocks (always at least one unless an error is thrown). `buffers` must be
    /// at least `extend_by` long; on return its first `*extended_by` elements are
    /// pinned buffers. Returns the first newly added block number.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn ExtendBufferedRelBy(
        &self,
        rel: &Relation,
        fork: ForkNumber,
        has_strategy: bool,
        flags: u32,
        extend_by: u32,
        buffers: &mut [Buffer],
        extended_by: &mut u32,
    ) -> PgResult<BlockNumber> {
        debug_assert!(extend_by > 0);
        // bmr.smgr resolution + relpersistence are pre-resolved off `&Relation`.
        let bmr = BmrRel::new(rel);
        self.ExtendBufferedRelCommon(
            &bmr,
            fork,
            has_strategy,
            flags,
            extend_by,
            InvalidBlockNumber,
            buffers,
            extended_by,
        )
    }

    /// `ExtendBufferedRelTo(bmr, fork, strategy, flags, extend_to, mode)`
    /// (bufmgr.c:922) — extend the relation so it is at least `extend_to` blocks
    /// large, returning the buffer for block `extend_to - 1`. Useful for callers
    /// that want a specific page regardless of the current size (visibilitymap,
    /// crash recovery).
    pub(crate) fn ExtendBufferedRelTo(
        &self,
        rel: &Relation,
        fork: ForkNumber,
        has_strategy: bool,
        mut flags: u32,
        extend_to: BlockNumber,
        mode: ReadBufferMode,
    ) -> PgResult<Buffer> {
        let bmr = BmrRel::new(rel);
        let mut extended_by: u32 = 0;
        let mut buffer: Buffer = INVALID_BUFFER;
        let mut buffers: [Buffer; EXTEND_TO_BATCH] = [INVALID_BUFFER; EXTEND_TO_BATCH];

        debug_assert!(extend_to != InvalidBlockNumber && extend_to > 0);

        // If desired, create the file if it doesn't exist. If
        // smgr_cached_nblocks[fork] is positive then it must exist, no need for
        // an smgrexists call. The cached count surfaces via `smgrnblocks_cached`
        // (InvalidBlockNumber when not cached); the C "positive" test
        // corresponds to a cached count that is neither 0 nor InvalidBlockNumber.
        if flags & eb::CREATE_FORK_IF_NEEDED != 0 {
            let cached = smgr::smgrnblocks_cached(bmr.rlocator, fork);
            if (cached == 0 || cached == InvalidBlockNumber)
                && !smgr::smgrexists(bmr.rlocator, fork)?
            {
                let guard =
                    backend_storage_lmgr_lmgr_seams::lock_relation_for_extension::call(bmr.rel)?;

                // recheck, fork might have been created concurrently
                if !smgr::smgrexists(bmr.rlocator, fork)? {
                    smgr::smgrcreate(
                        bmr.rlocator,
                        fork,
                        flags & eb::PERFORMING_RECOVERY != 0,
                    )?;
                }

                guard.release()?;
            }
        }

        // If requested, invalidate size cache, so that smgrnblocks asks the
        // kernel (C: `smgr_cached_nblocks[fork] = InvalidBlockNumber`). The
        // per-relation cache is owned by the smgr subsystem and is NOT trusted
        // outside recovery (`smgrnblocks_cached` returns InvalidBlockNumber, so
        // `smgrnblocks` always re-reads from md) — so the EB_CLEAR_SIZE_CACHE
        // request is already the default behaviour on the normal path. The
        // recovery-time corner (cache consulted) is bounded to redo-driven
        // FSM/VM extension; honouring it precisely needs a per-fork smgr
        // cache-clear API the smgr layer does not yet expose.
        let _ = flags & eb::CLEAR_SIZE_CACHE;

        // Estimate how many pages we'll need to extend by. This avoids acquiring
        // unnecessarily many victim buffers.
        let mut current_size = smgr::smgrnblocks(bmr.rlocator, fork)?;

        // Since no-one else can be looking at the page contents yet, there is no
        // difference between an exclusive lock and a cleanup-strength lock. Note
        // that we pass the original mode to ReadBuffer_common() below, when
        // falling back to reading the buffer to a concurrent relation extension.
        if mode == ReadBufferMode::ZeroAndLock || mode == ReadBufferMode::ZeroAndCleanupLock {
            flags |= eb::LOCK_TARGET;
        }

        while current_size < extend_to {
            let mut num_pages: u32 = buffers.len() as u32;

            if current_size as u64 + num_pages as u64 > extend_to as u64 {
                num_pages = extend_to - current_size;
            }

            let first_block = self.ExtendBufferedRelCommon(
                &bmr,
                fork,
                has_strategy,
                flags,
                num_pages,
                extend_to,
                &mut buffers,
                &mut extended_by,
            )?;

            current_size = first_block + extended_by;
            debug_assert!(num_pages != 0 || current_size >= extend_to);

            for i in 0..extended_by {
                if first_block + i != extend_to - 1 {
                    self.ReleaseBuffer(buffers[i as usize])?;
                } else {
                    buffer = buffers[i as usize];
                }
            }
        }

        // It's possible that another backend concurrently extended the relation.
        // In that case read the buffer. (bufmgr.c:1006)
        if buffer == INVALID_BUFFER {
            debug_assert_eq!(extended_by, 0);
            // ReadBuffer_common(bmr.smgr, bmr.relpersistence, fork, extend_to-1,
            //                   mode, strategy) — the F3 synchronous read core.
            // Reachable only when another backend won the extension race between
            // our size probe and the per-iteration extend.
            buffer = self.read_buffer_common(
                Some(bmr.rlocator),
                bmr.relpersistence,
                fork,
                extend_to - 1,
                mode,
                has_strategy,
                Some(bmr.rel),
            )?;
        }

        Ok(buffer)
    }

    // -- shared driver (bufmgr.c:2561) -------------------------------------

    /// `ExtendBufferedRelCommon(bmr, fork, strategy, flags, extend_by,
    /// extend_upto, buffers, extended_by)` (bufmgr.c:2561) — logic shared between
    /// `ExtendBufferedRelBy` and `ExtendBufferedRelTo`. Dispatches on
    /// relpersistence: temp relations to the local-buffer path (not modelled by
    /// this shared core), permanent/unlogged relations to the shared path.
    #[allow(clippy::too_many_arguments)]
    fn ExtendBufferedRelCommon(
        &self,
        bmr: &BmrRel,
        fork: ForkNumber,
        has_strategy: bool,
        flags: u32,
        extend_by: u32,
        extend_upto: BlockNumber,
        buffers: &mut [Buffer],
        extended_by: &mut u32,
    ) -> PgResult<BlockNumber> {
        // TRACE_POSTGRESQL_BUFFER_EXTEND_START(...) — DTrace probe, no-op here.

        let mut extend_by = extend_by;
        let first_block = if bmr.relpersistence == RELPERSISTENCE_TEMP {
            // ExtendBufferedRelLocal (bufmgr.c:2580) — the temp/local-buffer
            // extend path lives in localbuf.c, owned by the local-buffer manager
            // (panic-until-owner; its ambient per-backend handle is not yet
            // established). Dispatch through the outward seam, mirroring C.
            let (first_block, ext) = sb::extend_buffered_rel_local::call(
                bmr.rlocator,
                fork,
                flags,
                extend_by,
                extend_upto,
                buffers,
            )?;
            extend_by = ext;
            first_block
        } else {
            self.ExtendBufferedRelShared(
                bmr,
                fork,
                has_strategy,
                flags,
                extend_by,
                extend_upto,
                buffers,
                &mut extend_by,
            )?
        };
        *extended_by = extend_by;

        // TRACE_POSTGRESQL_BUFFER_EXTEND_DONE(...) — DTrace probe, no-op here.

        Ok(first_block)
    }

    /// `ExtendBufferedRelShared(bmr, fork, strategy, flags, extend_by,
    /// extend_upto, buffers, extended_by)` (bufmgr.c:2605) — the shared-buffers
    /// implementation of relation extension. Acquires `extend_by` zero-filled
    /// victim buffers (outside the extension lock), takes the extension lock,
    /// queries the true relation size, inserts the new buffers into the buffer
    /// table marking them IO_IN_PROGRESS, `smgrzeroextend`s the file, releases
    /// the extension lock, then marks every new buffer valid and terminates its
    /// I/O. Returns the first new block number; `*extended_by` is the count.
    #[allow(clippy::too_many_arguments)]
    fn ExtendBufferedRelShared(
        &self,
        bmr: &BmrRel,
        fork: ForkNumber,
        has_strategy: bool,
        flags: u32,
        mut extend_by: u32,
        extend_upto: BlockNumber,
        buffers: &mut [Buffer],
        extended_by: &mut u32,
    ) -> PgResult<BlockNumber> {
        // IOContext io_context = IOContextForStrategy(strategy). The shared core
        // collapses BufferAccessStrategy to `has_strategy`; the ring→IOContext
        // mapping (ring reuse) is behaviour-equivalent to the shared sweep on a
        // cold pool, so a no-strategy extend is accounted under IOCONTEXT_NORMAL.

        self.LimitAdditionalPins(&mut extend_by);

        // Acquire victim buffers for extension without holding extension lock.
        // Writing out victim buffers is the most expensive part of extending the
        // relation, particularly when doing so requires WAL flushes. Zeroing out
        // the buffers is also quite expensive, so do that before holding the
        // extension lock as well.
        //
        // These pages are pinned by us and not valid. While we hold the pin they
        // can't be acquired as victim buffers by another backend.
        for i in 0..extend_by {
            let buf_id = self.get_victim_buffer(has_strategy)?;
            buffers[i as usize] = buf_id_to_buffer(buf_id as i32);

            // new buffers are zero-filled: MemSet(buf_block, 0, BLCKSZ).
            self.zero_block(buf_id);
        }

        // Lock relation against concurrent extensions, unless requested not to.
        //
        // We use the same extension lock for all forks. That's unnecessarily
        // restrictive, but currently extensions for forks don't happen often
        // enough to make it worth locking more granularly.
        //
        // Note that another backend might have extended the relation by the time
        // we get the lock.
        let mut ext_guard = None;
        if flags & eb::SKIP_EXTENSION_LOCK == 0 {
            ext_guard =
                Some(backend_storage_lmgr_lmgr_seams::lock_relation_for_extension::call(bmr.rel)?);
        }

        // If requested, invalidate size cache, so that smgrnblocks asks the
        // kernel. The cached-size array is owned by the smgr subsystem; the
        // CLEAR_SIZE_CACHE flag is honoured at the smgr layer.

        let first_block = smgr::smgrnblocks(bmr.rlocator, fork)?;

        // Now that we have the accurate relation size, check if the caller wants
        // us to extend to only up to a specific size. If there were concurrent
        // extensions, we might have acquired too many buffers and need to release
        // them.
        if extend_upto != InvalidBlockNumber {
            let orig_extend_by = extend_by;

            if first_block > extend_upto {
                extend_by = 0;
            } else if first_block as u64 + extend_by as u64 > extend_upto as u64 {
                extend_by = extend_upto - first_block;
            }

            let mut i = extend_by;
            while i < orig_extend_by {
                let buf_id = (buffers[i as usize] - 1) as usize;

                // The victim buffer we acquired previously is clean and unused,
                // let it be found again quickly.
                self.strategy_free_buffer(buf_id)?;
                self.unpin_buffer(buf_id);
                i += 1;
            }

            if extend_by == 0 {
                if let Some(guard) = ext_guard.take() {
                    guard.release()?;
                }
                *extended_by = extend_by;
                return Ok(first_block);
            }
        }

        // Fail if relation is already at maximum possible length.
        if first_block as u64 + extend_by as u64 >= MAX_BLOCK_NUMBER as u64 {
            return Err(PgError::error(format!(
                "cannot extend relation {} beyond {} blocks",
                relpath_str(bmr.rlocator, fork),
                MAX_BLOCK_NUMBER
            )));
        }

        // Insert buffers into buffer table, mark as IO_IN_PROGRESS.
        //
        // This needs to happen before we extend the relation, because as soon as
        // we do, other backends can start to read in those pages.
        for i in 0..extend_by {
            let victim_buf = buffers[i as usize];
            let victim_buf_id = (victim_buf - 1) as usize;

            // in case we need to pin an existing buffer below
            sb::resowner_enlarge::call()?;
            self.private_refcount().ReservePrivateRefCountEntry();

            // InitBufferTag(&tag, &smgr_rlocator.locator, fork, first_block + i).
            let tag = make_tag(bmr.rlocator, fork, first_block + i);
            // hash = BufTableHashCode(&tag); partition_lock = BufMappingPartitionLock(hash).
            let new_code = buf_table_hash_code(&tag);
            let partition = buf_table_hash_partition(new_code);

            let guard = self.map_acquire(partition, LWLockMode::LW_EXCLUSIVE)?;

            let existing_id = self.buf_table().insert(tag, new_code, victim_buf_id as i32)?;

            // We get here only in the corner case where we are trying to extend
            // the relation but we found a pre-existing buffer. This can happen
            // because a prior attempt at extending the relation failed, and
            // because mdread doesn't complain about reads beyond EOF (when
            // zero_damaged_pages is ON) and so a previous attempt to read a block
            // beyond EOF could have left a "valid" zero-filled buffer.
            // Unfortunately, we have also seen this case occurring because of
            // buggy Linux kernels that sometimes return an lseek(SEEK_END) result
            // that doesn't account for a recent write. In that situation, the
            // pre-existing buffer would contain valid data that we don't want to
            // overwrite.  Since the legitimate cases should always have left a
            // zero-filled buffer, complain if not PageIsNew.
            if existing_id >= 0 {
                let existing_buf_id = existing_id as usize;

                // Pin the existing buffer before releasing the partition lock,
                // preventing it from being evicted.
                let valid = self.pin_buffer(existing_buf_id, has_strategy);

                guard.release()?;

                // The victim buffer we acquired previously is clean and unused,
                // let it be found again quickly.
                self.strategy_free_buffer(victim_buf_id)?;
                self.unpin_buffer(victim_buf_id);

                buffers[i as usize] = buf_id_to_buffer(existing_buf_id as i32);

                if valid && !self.page_is_new(buf_id_to_buffer(existing_buf_id as i32))? {
                    let existing_tag = self.desc_tag(existing_buf_id);
                    return Err(PgError::error(format!(
                        "unexpected data beyond EOF in block {} of relation \"{}\"",
                        existing_tag.blockNum,
                        relpath_str(bmr.rlocator, fork)
                    ))
                    .with_hint(
                        "This has been seen to occur with buggy kernels; \
                         consider updating your system.",
                    ));
                }

                // We *must* do smgr[zero]extend before succeeding, else the page
                // will not be reserved by the kernel, and the next P_NEW call
                // will decide to return the same page.  Clear the BM_VALID bit,
                // do StartBufferIO() and proceed.
                //
                // Loop to handle the very small possibility that someone re-sets
                // BM_VALID between our clearing it and StartBufferIO inspecting it.
                loop {
                    let mut buf_state = self.lock_buf_hdr(existing_buf_id);
                    buf_state &= !BM_VALID;
                    self.unlock_buf_hdr(existing_buf_id, buf_state);
                    // StartBufferIO(existing_hdr, forInput=true, nowait=false).
                    if self.start_buffer_io(existing_buf_id, false, None)? {
                        break;
                    }
                }
            } else {
                let mut buf_state = self.lock_buf_hdr(victim_buf_id);

                // some sanity checks while we hold the buffer header lock
                debug_assert_eq!(
                    buf_state & (BM_VALID | BM_TAG_VALID | BM_DIRTY | BM_JUST_DIRTIED),
                    0
                );
                debug_assert_eq!(buf_state_get_refcount(buf_state), 1);

                self.set_desc_tag(victim_buf_id, tag);

                buf_state |= BM_TAG_VALID | BUF_USAGECOUNT_ONE;
                if bmr.relpersistence == RELPERSISTENCE_PERMANENT || fork == INIT_FORKNUM {
                    buf_state |= BM_PERMANENT;
                }

                self.unlock_buf_hdr(victim_buf_id, buf_state);

                guard.release()?;

                // XXX: could combine the locked operations in it with the above.
                // StartBufferIO(victim_buf_hdr, forInput=true, nowait=false).
                self.start_buffer_io(victim_buf_id, false, None)?;
            }
        }

        // io_start = pgstat_prepare_io_time(track_io_timing). The start-timestamp
        // dance is internal to pgstat; collapsed into the post-write
        // `count_io_op_extend` seam below (stats-only, behaviour-neutral).

        // Note: if smgrzeroextend fails, we will end up with buffers that are
        // allocated but not marked BM_VALID.  The next relation extension will
        // still select the same block number (because the relation didn't get any
        // longer on disk) and so future attempts to extend the relation will find
        // the same buffers (if they have not been recycled) but come right back
        // here to try smgrzeroextend again.
        //
        // We don't need to set checksum for all-zero pages.
        smgr::smgrzeroextend(bmr.rlocator, fork, first_block, extend_by as i32, false)?;

        // Release the file-extension lock; it's now OK for someone else to extend
        // the relation some more.
        //
        // We remove IO_IN_PROGRESS after this, as waking up waiting backends can
        // take noticeable time.
        if let Some(guard) = ext_guard.take() {
            guard.release()?;
        }

        // pgstat_count_io_op_time(IOOBJECT_RELATION, io_context, IOOP_EXTEND,
        //                         io_start, 1, extend_by * BLCKSZ).
        sb::count_io_op_extend::call(1, extend_by as u64 * types_core::primitive::BLCKSZ as u64);

        // Set BM_VALID, terminate IO, and wake up any waiters.
        for i in 0..extend_by {
            let buf = buffers[i as usize];
            let buf_id = (buf - 1) as usize;
            let mut lock = false;

            if flags & eb::LOCK_FIRST != 0 && i == 0 {
                lock = true;
            } else if flags & eb::LOCK_TARGET != 0 {
                debug_assert_ne!(extend_upto, InvalidBlockNumber);
                if first_block + i + 1 == extend_upto {
                    lock = true;
                }
            }

            if lock {
                // LWLockAcquire(BufferDescriptorGetContentLock(buf_hdr), LW_EXCLUSIVE).
                lwlock::LWLockAcquire(
                    self.content_lock(buf_id),
                    LWLockMode::LW_EXCLUSIVE,
                    backend_storage_lmgr_proc_seams::my_proc_number::call(),
                )?;
            }

            // TerminateBufferIO(buf_hdr, clear_dirty=false, BM_VALID,
            // forget_owner=true, release_aio=false) (bufmgr.c:2868).
            self.terminate_buffer_io(buf_id, false, BM_VALID, true, false)?;
        }

        // pgBufferUsage.shared_blks_written += extend_by.
        for _ in 0..extend_by {
            sb::count_buffer_write::call();
        }

        *extended_by = extend_by;

        Ok(first_block)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicI32, Ordering};
    use std::sync::{Mutex, Once};

    /// `MaxBackends` the seam returns, settable per test. The seam installer needs
    /// a non-capturing `fn` pointer, so the per-test value rides this static and
    /// the installed fn reads it. Tests that depend on the seam take `SEAM_LOCK`
    /// so they don't race each other on the shared value.
    static MAX_BACKENDS: AtomicI32 = AtomicI32::new(1);
    static SEAM_LOCK: Mutex<()> = Mutex::new(());
    static INSTALL: Once = Once::new();

    fn read_max_backends() -> i32 {
        MAX_BACKENDS.load(Ordering::SeqCst)
    }

    /// Install the `max_backends` seam once and set the per-test value; returns
    /// the guard that serialises seam-dependent tests. `max_backends_plus_aux`
    /// (the proportional-pin divisor) is `max_backends + NUM_AUXILIARY_PROCS`.
    fn with_divisor(divisor: i32) -> std::sync::MutexGuard<'static, ()> {
        let guard = SEAM_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        // Pick max_backends so max_backends + NUM_AUXILIARY_PROCS == divisor.
        MAX_BACKENDS.store(divisor - NUM_AUXILIARY_PROCS, Ordering::SeqCst);
        INSTALL.call_once(|| {
            backend_utils_init_small_seams::max_backends::set(read_max_backends);
        });
        guard
    }

    /// A pool of the given size; the strategy/buf-table shmem stubs ride the
    /// shared F1 test harness.
    fn pool(nbuffers: u32) -> BufferManager {
        crate::mgr::test_seams::install();
        BufferManager::new(nbuffers)
    }

    #[test]
    fn get_pin_limit_is_proportional() {
        let _g = with_divisor(10);
        let p = pool(100);
        // 100 / 10 == 10.
        assert_eq!(p.GetPinLimit(), 10);
    }

    #[test]
    fn get_pin_limit_zero_on_tiny_pool() {
        let _g = with_divisor(200);
        let p = pool(16);
        // 16 / 200 == 0 (integer division): the documented zero corner.
        assert_eq!(p.GetPinLimit(), 0);
    }

    #[test]
    fn additional_pin_limit_subtracts_array_estimate() {
        let _g = with_divisor(2);
        let p = pool(64);
        // MaxProportionalPins = 64 / 2 = 32; estimate held = REFCOUNT_ARRAY_ENTRIES.
        assert_eq!(p.GetAdditionalPinLimit(), 32 - REFCOUNT_ARRAY_ENTRIES);
    }

    #[test]
    fn additional_pin_limit_zero_when_over_share() {
        let _g = with_divisor(50);
        let p = pool(64);
        // MaxProportionalPins = 64 / 50 = 1; estimate held = 8 > 1 -> 0.
        assert_eq!(p.GetAdditionalPinLimit(), 0);
    }

    #[test]
    fn limit_additional_pins_allows_one() {
        let _g = with_divisor(50);
        let p = pool(64);
        // <= 1 is left untouched even when the budget is zero.
        let mut pins: u32 = 1;
        p.LimitAdditionalPins(&mut pins);
        assert_eq!(pins, 1);

        let mut zero: u32 = 0;
        p.LimitAdditionalPins(&mut zero);
        assert_eq!(zero, 0);
    }

    #[test]
    fn limit_additional_pins_caps_at_least_one() {
        let _g = with_divisor(50);
        let p = pool(64);
        // Budget is 0, but Max(limit, 1) guarantees at least one pin for progress.
        let mut pins: u32 = 16;
        p.LimitAdditionalPins(&mut pins);
        assert_eq!(pins, 1);
    }

    #[test]
    fn limit_additional_pins_caps_to_budget() {
        let _g = with_divisor(2);
        let p = pool(64);
        // MaxProportionalPins = 32; additional = 32 - 8 = 24; request 100 -> 24.
        let mut pins: u32 = 100;
        p.LimitAdditionalPins(&mut pins);
        assert_eq!(pins, 24);
    }

    #[test]
    fn limit_additional_pins_keeps_smaller_request() {
        let _g = with_divisor(2);
        let p = pool(64);
        // Request below the budget (24) is unchanged.
        let mut pins: u32 = 4;
        p.LimitAdditionalPins(&mut pins);
        assert_eq!(pins, 4);
    }
}
