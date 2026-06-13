//! Seam declarations for the `backend-storage-aio-core` unit
//! (`storage/aio/aio.c` et al.). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.


seam_core::seam!(
    /// `AtEOXact_Aio(isCommit)` — error out about leaked AIO handles at
    /// commit (Assert-side checks).
    pub fn at_eoxact_aio(is_commit: bool)
);

seam_core::seam!(
    /// `pgaio_error_cleanup()` — release AIO state on the abort path.
    pub fn pgaio_error_cleanup()
);

seam_core::seam!(
    /// `pgaio_closing_fd(fd)` (`storage/aio/aio.c`) — called just before a
    /// kernel file descriptor is closed so the AIO subsystem can wait out any
    /// in-flight IOs that still reference it. `fd` is the raw kernel
    /// descriptor about to be closed.
    pub fn pgaio_closing_fd(fd: i32)
);

// === read_stream (read_stream.c) ===========================================

/// Opaque token standing in for C's `ReadStream *` while the read-stream
/// runtime (read_stream.c) owns the live stream state. Valid from
/// `read_stream_begin_relation` until `read_stream_end`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReadStreamHandle(pub u64);

seam_core::seam!(
    /// `read_stream_begin_relation(READ_STREAM_MAINTENANCE | READ_STREAM_FULL
    /// | READ_STREAM_USE_BATCHING, info->strategy, rel, MAIN_FORKNUM,
    /// block_range_read_stream_cb, &p, 0)` (read_stream.c): begin a physical
    /// block-range scan starting at `first_block`. `Err` carries setup
    /// ereports/OOM.
    pub fn read_stream_begin<'mcx>(
        rel: &types_rel::Relation<'mcx>,
        first_block: types_core::primitive::BlockNumber,
    ) -> types_error::PgResult<ReadStreamHandle>
);

seam_core::seam!(
    /// `p.last_exclusive = num_pages` — set the stream's exclusive upper block
    /// bound before iterating.
    pub fn read_stream_set_last_exclusive(
        stream: ReadStreamHandle,
        num_pages: types_core::primitive::BlockNumber,
    )
);

seam_core::seam!(
    /// `p.current_blocknum` — the stream's current (next-to-return) block.
    pub fn read_stream_current_blocknum(
        stream: ReadStreamHandle,
    ) -> types_core::primitive::BlockNumber
);

seam_core::seam!(
    /// `read_stream_next_buffer(stream, NULL)` (read_stream.c): the next
    /// pinned buffer, or `InvalidBuffer` at the end of the current range.
    /// `Err` carries the smgr read ereports.
    pub fn read_stream_next_buffer(
        stream: ReadStreamHandle,
    ) -> types_error::PgResult<types_storage::storage::Buffer>
);

seam_core::seam!(
    /// `read_stream_reset(stream)` (read_stream.c): rewind so the callback is
    /// invoked again after a full range was consumed.
    pub fn read_stream_reset(stream: ReadStreamHandle)
);

seam_core::seam!(
    /// `read_stream_end(stream)` (read_stream.c): finish and free the stream.
    pub fn read_stream_end(stream: ReadStreamHandle)
);

// --- backend-utils-init-postinit consumer (aio_init.c) ---

seam_core::seam!(
    /// `pgaio_init_backend()` (aio_init.c): initialize this backend's AIO
    /// subsystem. `Err` carries its `ereport` surface.
    pub fn pgaio_init_backend() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `AioShmemSize()` (ipci.c `CalculateShmemSize` accumulator) — shared-memory
    /// bytes this subsystem needs. `Err` carries the `add_size`/`mul_size`
    /// overflow `ereport(ERROR)`. Owner unported; scaffolded slot.
    pub fn aio_shmem_size() -> types_error::PgResult<types_core::Size>
);

seam_core::seam!(
    /// `AioShmemInit()` (ipci.c `CreateOrAttachShmemStructs`) — allocate-or-attach
    /// this subsystem's shared-memory structures. `Err` carries the C
    /// out-of-shared-memory `ereport(ERROR)`. Owner unported; scaffolded slot.
    pub fn aio_shmem_init() -> types_error::PgResult<()>
);
