//! Seam declarations for the bulk-write substrate
//! (`storage/smgr/bulk_write.c`): the `smgr_bulk_*` API a from-scratch relation
//! build (nbtsort.c `_bt_load`, spgist/gist builds, `RelationCopyStorage`)
//! uses to stream freshly-built pages to a fork's smgr without going through
//! shared buffers.
//!
//! The owning unit (`backend-storage-smgr-bulkwrite`) is not yet ported, so
//! every call panics loudly until that unit lands and installs these from its
//! `init_seams()`. There is no silent fallback.
//!
//! ## Owned model
//! C's opaque `BulkWriteState *` is carried as [`BulkWriteState`], a
//! type-erased handle the bulk-write owner downcasts (mirroring
//! `types_nodes::Tuplesortstate`). C's `BulkWriteBuffer` (a pointer to a
//! writer-owned `BLCKSZ` page the caller fills in place) is the safe owned page
//! workspace [`mcx::PgVec<u8>`]: `smgr_bulk_get_buf` hands back a zeroed page,
//! the caller fills it, and `smgr_bulk_write` takes ownership back.

#![allow(non_snake_case)]

use core::any::Any;

use mcx::{Mcx, PgBox, PgVec};
use types_core::primitive::{BlockNumber, ForkNumber};
use types_error::PgResult;
use types_rel::Relation;

/// `BulkWriteState *` (`storage/bulk_write.h`) — opaque to every consumer. The
/// owned model type-erases the real bulk-write engine state; only the
/// bulk-write owner downcasts it.
pub struct BulkWriteState<'mcx> {
    /// The real owned state, type-erased and context-allocated (C: the
    /// `BulkWriteState` palloc'd in a private memory context); `None` for a
    /// default-constructed carrier (the C `NULL`).
    state: Option<PgBox<'mcx, dyn Any>>,
}

impl<'mcx> BulkWriteState<'mcx> {
    /// `smgr_bulk_start_*`-shaped construction: allocate the concrete engine
    /// state in `mcx` and type-erase it. Only the bulk-write owner (or a test
    /// mock) calls this. Fallible: allocating.
    pub fn new<T: Any>(mcx: Mcx<'mcx>, state: T) -> PgResult<Self> {
        let boxed = mcx::alloc_in(mcx, state)?;
        let (ptr, alloc) = PgBox::into_raw_with_allocator(boxed);
        // SAFETY: `ptr` came from `into_raw_with_allocator` with `alloc`; the
        // cast only attaches the `dyn Any` vtable (no `CoerceUnsized` on stable).
        let erased: PgBox<'mcx, dyn Any> =
            unsafe { PgBox::from_raw_in(ptr as *mut dyn Any, alloc) };
        Ok(BulkWriteState {
            state: Some(erased),
        })
    }

    /// The type-erased engine state (the bulk-write owner downcasts).
    pub fn downcast_ref<T: Any>(&self) -> Option<&T> {
        self.state.as_ref().and_then(|b| (**b).downcast_ref::<T>())
    }

    /// The type-erased engine state, mutably.
    pub fn downcast_mut<T: Any>(&mut self) -> Option<&mut T> {
        self.state.as_mut().and_then(|b| (**b).downcast_mut::<T>())
    }
}

seam_core::seam!(
    /// `smgr_bulk_start_rel(rel, forknum)` (bulk_write.c): start a bulk write
    /// to relation `rel`'s `forknum` fork, returning the bulk-write handle.
    /// Allocates the engine state in `mcx`; fallible on OOM.
    pub fn smgr_bulk_start_rel<'mcx>(
        mcx: Mcx<'mcx>,
        rel: &Relation<'mcx>,
        forknum: ForkNumber,
    ) -> PgResult<BulkWriteState<'mcx>>
);

seam_core::seam!(
    /// `smgr_bulk_start_smgr(smgr, forknum, use_wal)` (bulk_write.c): start a
    /// bulk write to a fork named by its `(RelFileLocator, ProcNumber)` smgr
    /// key, without a relcache entry (the `RelationCopyStorage` path). The
    /// caller supplies `use_wal` directly. Allocates in `mcx`; fallible on OOM.
    pub fn smgr_bulk_start_smgr<'mcx>(
        mcx: Mcx<'mcx>,
        smgr_rlocator: types_storage::relfilelocator::RelFileLocatorBackend,
        forknum: ForkNumber,
        use_wal: bool,
    ) -> PgResult<BulkWriteState<'mcx>>
);

seam_core::seam!(
    /// `smgr_bulk_get_buf(bulkstate)` (bulk_write.c): obtain a fresh, zeroed
    /// `BLCKSZ` page workspace for the next block. C hands back a pointer into
    /// writer-owned memory; the owned model returns an owned zeroed page the
    /// caller fills and then returns via `smgr_bulk_write`. Fallible on OOM.
    pub fn smgr_bulk_get_buf<'mcx>(
        mcx: Mcx<'mcx>,
        bulkstate: &mut BulkWriteState<'mcx>,
    ) -> PgResult<PgVec<'mcx, u8>>
);

seam_core::seam!(
    /// `smgr_bulk_write(bulkstate, blocknum, buf, page_std)` (bulk_write.c):
    /// queue `buf` to be written at `blocknum`. Takes ownership of the page
    /// workspace `buf` (C reuses the writer-owned buffer). `page_std` selects
    /// the standard-page (hole-skipping) WAL/checksum path. Can `ereport` on
    /// write errors, hence fallible.
    pub fn smgr_bulk_write<'mcx>(
        bulkstate: &mut BulkWriteState<'mcx>,
        blocknum: BlockNumber,
        buf: PgVec<'mcx, u8>,
        page_std: bool,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `smgr_bulk_finish(bulkstate)` (bulk_write.c): flush all queued pages and
    /// release the bulk-write state. Consumes the handle. `Err` carries the
    /// smgr write / `log_newpage` ereports.
    pub fn smgr_bulk_finish<'mcx>(bulkstate: BulkWriteState<'mcx>) -> PgResult<()>
);
