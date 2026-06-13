//! Seam declarations for the `backend-storage-aio-aio` unit
//! (`storage/aio/aio.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

seam_core::seam!(
    /// `pgaio_error_cleanup()` (aio.c) — reclaim in-flight AIO handles during
    /// error recovery (the aux-process `sigsetjmp` cleanup calls it).
    /// Infallible; runs with interrupts held.
    pub fn pgaio_error_cleanup()
);

seam_core::seam!(
    /// `pgaio_closing_fd(int fd)` (aio.c) — wait for and tear down any AIO
    /// referencing this kernel fd just before the VFD layer closes it.
    /// Infallible.
    pub fn pgaio_closing_fd(fd: i32)
);

seam_core::seam!(
    /// `pgaio_io_start_readv(PgAioHandle *ioh, int fd, int iovcnt, uint64 offset)`
    /// (aio_io.c) — stage a vectored read on `ioh` against `fd`. The iovec is
    /// the handle's pre-set scatter/gather array (`pgaio_io_get_iovec`), so the
    /// VFD layer passes only `fd`/`iovcnt`/`offset`; the handle is threaded on
    /// the AIO side. Infallible.
    pub fn pgaio_io_start_readv(fd: i32, iovcnt: i32, offset: u64)
);
