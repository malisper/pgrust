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
//
// read_stream.c is now ported in the `backend-storage-aio-read-stream` owner
// crate. It sits directly above the buffer manager (it drives StartReadBuffers/
// WaitReadBuffers + the buffer accessors), so its consumers (e.g. nbtree's
// `btvacuumscan`) depend on it directly and own a real `ReadStream<'mcx>` value
// with a real `ReadStreamBlockNumberCB` callback. There is no seam and no
// `ReadStreamHandle` stand-in: those were removed when the owner landed.

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
