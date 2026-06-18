//! Seam declarations for the local-buffer (`localbuf.c`) dispatch entry points
//! that the shared buffer manager (`bufmgr.c`) calls on its `BufferIsLocal`
//! branches.
//!
//! `bufmgr.c`'s `ReleaseBuffer` / `IncrBufferRefCount` / `MarkBufferDirty` /
//! `BufferGetBlockNumber` / `BufferGetTag` / `BufferGetLSNAtomic` each test
//! `BufferIsLocal(buffer)` (a negative buffer id) and dispatch to the
//! local-buffer manager. The local pool is owned by
//! `backend-storage-buffer-support` (`localbuf.c`), which installs these from
//! its `init_seams()`; a direct dep from `bufmgr` to `support` would cycle (the
//! support crate already consumes `bufmgr`'s header seams), so these cross
//! through this crate. Until the owner installs them, a call panics loudly.

seam_core::seam!(
    /// `MarkLocalBufferDirty(buffer)` (localbuf.c) — mark a local (temp)
    /// buffer's contents dirty (the `BufferIsLocal` arm of `MarkBufferDirty`).
    pub fn mark_local_buffer_dirty(
        buffer: types_core::primitive::Buffer,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `UnpinLocalBuffer(buffer)` (localbuf.c) — drop one local pin (the
    /// `BufferIsLocal` arm of `ReleaseBuffer`).
    pub fn unpin_local_buffer(
        buffer: types_core::primitive::Buffer,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `UnpinLocalBufferNoOwner(buffer)` (localbuf.c) — drop one local pin
    /// without resource-owner bookkeeping (the `BufferIsLocal` arm of
    /// `ResOwnerReleaseBufferPin`, where the owner is already mid-release).
    pub fn unpin_local_buffer_no_owner(
        buffer: types_core::primitive::Buffer,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `LocalRefCount[-buffer - 1]++` (localbuf.c, the inline `IncrBufferRefCount`
    /// local arm) — bump this backend's local pin count on an already-pinned
    /// local buffer.
    pub fn incr_local_buffer_ref_count(
        buffer: types_core::primitive::Buffer,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `GetLocalBufferDescriptor(-buffer - 1)->tag.blockNum` (localbuf.c) — the
    /// block number a local buffer currently holds (the `BufferIsLocal` arm of
    /// `BufferGetBlockNumber`).
    pub fn local_buffer_block_number(
        buffer: types_core::primitive::Buffer,
    ) -> types_error::PgResult<types_core::primitive::BlockNumber>
);

seam_core::seam!(
    /// `BufferGetTag(buffer, ...)` local arm — the relation/fork/block this
    /// local buffer holds, returned as one owned triple.
    pub fn local_buffer_get_tag(
        buffer: types_core::primitive::Buffer,
    ) -> types_error::PgResult<(
        types_storage::RelFileLocator,
        types_core::primitive::ForkNumber,
        types_core::primitive::BlockNumber,
    )>
);

seam_core::seam!(
    /// `PageGetLSN(BufferGetPage(buffer))` local arm (`BufferGetLSNAtomic`) —
    /// the page LSN of a local buffer (no header spinlock needed: local buffers
    /// are never shared).
    pub fn local_buffer_get_lsn(
        buffer: types_core::primitive::Buffer,
    ) -> types_error::PgResult<types_core::primitive::XLogRecPtr>
);

seam_core::seam!(
    /// `BufferGetPage(buffer)` local arm — run `f` over a local (temp) buffer's
    /// live page bytes (`BLCKSZ`) for in-place read/write. Local buffers are
    /// never shared, so no content lock is involved; the closure operates on the
    /// backend-local page directly. `f`'s `Err` propagates.
    pub fn local_buffer_with_page(
        buffer: types_core::primitive::Buffer,
        f: &mut dyn FnMut(&mut [u8]) -> types_error::PgResult<()>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `BufferGetPage(buffer)` local arm materialised as an owned snapshot copy
    /// of a local (temp) buffer's page image in `mcx` (the consumer reads
    /// page-format fields off it). `Err` carries OOM.
    pub fn local_buffer_page_owned<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        buffer: types_core::primitive::Buffer,
    ) -> types_error::PgResult<mcx::PgVec<'mcx, u8>>
);
