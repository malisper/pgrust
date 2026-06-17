//! Backend-startup / end-of-transaction / process-exit buffer bookkeeping
//! (bufmgr.c) and the relation-size accessor:
//!
//!  * `InitBufferManagerAccess` (bufmgr.c:4007) — initialise this backend's
//!    private pin map and register the process-exit cleanup callback.
//!  * `AtEOXact_Buffers` (bufmgr.c:3991) — end-of-transaction pin leak check.
//!  * `AtProcExit_Buffers` (bufmgr.c:4041) — the registered process-exit
//!    callback (UnlockBuffers + leak check + the local-buffer leg).
//!  * `CheckForBufferLeaks` (bufmgr.c:4059) — the USE_ASSERT_CHECKING shared-pin
//!    leak scan.
//!  * `RelationGetNumberOfBlocksInFork` (bufmgr.c:4423) — the current block
//!    count of a relation fork.
//!
//! The local-buffer legs (`AtEOXact_LocalBuffers` / `AtProcExit_LocalBuffers`)
//! dispatch through the bufmgr-OUTWARD seams installed by the local-buffer owner
//! when its ambient per-backend handle lands (panic-until-owner — sanctioned,
//! same posture as the F1c local-buffer pin dispatch).

use types_core::primitive::{BlockNumber, ForkNumber};
use types_error::PgResult;
use types_rel::Relation;
use types_storage::RelFileLocatorBackend;

use crate::mgr::BufferManager;

use backend_storage_buffer_bufmgr_seams as sb;
use backend_storage_smgr_smgr as smgr;

impl BufferManager {
    /// `InitBufferManagerAccess()` (bufmgr.c:4007) — set up this backend's local
    /// buffer-manager structures and register the process-exit cleanup. The C
    /// path computes `MaxProportionalPins`, zeroes the private-refcount fast
    /// array, creates the overflow hash, and `on_shmem_exit(AtProcExit_Buffers,
    /// 0)`. This crate's map-backed private-refcount substrate needs no array/
    /// hash split, so the access setup is a `clear()`; the cleanup registration
    /// is the same `on_shmem_exit` call.
    pub fn InitBufferManagerAccess(&self) -> PgResult<()> {
        // memset(&PrivateRefCountArray, 0, ...); PrivateRefCountHash =
        // hash_create(...). The map substrate collapses both onto one clear.
        self.private_refcount().clear();

        // Assert(MyProc != NULL);
        // on_shmem_exit(AtProcExit_Buffers, 0).
        backend_storage_ipc_dsm_core_seams::on_shmem_exit::call(
            at_proc_exit_buffers,
            types_tuple::Datum::from_i32(0),
        )
    }

    /// `AtEOXact_Buffers(isCommit)` (bufmgr.c:3991) — sanity-check that no buffer
    /// pins survive end of transaction, then run the local-buffer leg.
    pub fn AtEOXact_Buffers(&self, is_commit: bool) -> PgResult<()> {
        self.CheckForBufferLeaks()?;
        // AtEOXact_LocalBuffers(isCommit) — the temp-pool leg.
        sb::at_eoxact_local_buffers::call(is_commit)?;
        // Assert(PrivateRefCountOverflowed == 0) — structurally always 0 here
        // (the map substrate has no overflow tier).
        Ok(())
    }

    /// `AtProcExit_Buffers(code, arg)` (bufmgr.c:4041) — the process-exit
    /// callback: release any in-progress PIN_COUNT request, leak-check, then run
    /// the local-buffer leg.
    pub fn AtProcExit_Buffers(&self) -> PgResult<()> {
        self.UnlockBuffers();
        self.CheckForBufferLeaks()?;
        // AtProcExit_LocalBuffers() — the temp-pool leg.
        sb::at_proc_exit_local_buffers::call()
    }

    /// `CheckForBufferLeaks()` (bufmgr.c:4059) — under `USE_ASSERT_CHECKING`,
    /// warn about and count any shared-buffer pin this backend still holds. In a
    /// production (assertions-off) build the C body is empty; mirrored here as a
    /// `debug_assertions`-gated scan so a leak `debug_assert!`-fails in debug
    /// builds and the function is a no-op otherwise.
    pub fn CheckForBufferLeaks(&self) -> PgResult<()> {
        #[cfg(debug_assertions)]
        {
            let mut ref_count_errors = 0u32;
            self.private_refcount().for_each_present(|_buf_id, count| {
                if count != 0 {
                    ref_count_errors += 1;
                }
            });
            debug_assert_eq!(
                ref_count_errors, 0,
                "buffer refcount leak: {ref_count_errors} buffer(s) still pinned"
            );
        }
        Ok(())
    }

    /// `RelationGetNumberOfBlocksInFork(relation, forkNum)` (bufmgr.c:4423) — the
    /// current number of blocks in the relation fork. For a table-AM relation C
    /// uses `table_relation_size(rel, fork) / BLCKSZ` (rounded up); the default
    /// (heap) table AM computes that as `smgrnblocks(...) * BLCKSZ`, so the value
    /// equals `smgrnblocks` for every relation with storage. The table-AM vtable
    /// lives above the buffer manager (a direct call would cycle), so this core
    /// resolves the physical id off the `&Relation` and calls `smgrnblocks`
    /// directly — behaviour-identical to the C for the default AM and for every
    /// non-table storage relation (index/sequence/toast), which is all this seam
    /// is consumed for.
    pub fn RelationGetNumberOfBlocksInFork(
        &self,
        relation: &Relation<'_>,
        fork_num: ForkNumber,
    ) -> PgResult<BlockNumber> {
        let rlocator = RelFileLocatorBackend {
            locator: relation.rd_locator,
            backend: relation.rd_backend,
        };
        // `RelationGetSmgr(relation)` — the C inline lazily `smgropen`s the
        // relation and caches it on `relation->rd_smgr` before any smgr op.
        // `smgropen`/`cache_open` is idempotent, so call it to guarantee the
        // SMgrRelation cache entry exists prior to `smgrnblocks`.
        smgr::smgropen(relation.rd_locator, relation.rd_backend)?;
        smgr::smgrnblocks(rlocator, fork_num)
    }
}

/// The `on_shmem_exit`-registered `AtProcExit_Buffers(code, arg)` trampoline
/// (bufmgr.c:4041). Matches the `on_shmem_exit` callback signature.
fn at_proc_exit_buffers(
    _code: i32,
    _arg: types_tuple::Datum<'static>,
) -> PgResult<()> {
    BufferManager::global_expect().AtProcExit_Buffers()
}
