seam_core::seam!(
    // `pgaio_closing_fd(fd)` (storage/aio/aio.c) — drain in-flight AIO that
    // references this kernel fd before it is closed.
    pub fn pgaio_closing_fd(fd: i32)
);

seam_core::seam!(
    // `pgaio_io_start_readv(ioh, fd, iovcnt, offset)` (storage/aio/aio_io.c).
    // The PgAioHandle and its iovec live on the AIO side of the seam; only the
    // fd/iovcnt/offset triple crosses (fd.c:2241).
    pub fn pgaio_io_start_readv(fd: i32, iovcnt: i32, offset: i64)
);

seam_core::seam!(
    pub fn at_eoxact_aio(is_commit: bool)
);

seam_core::seam!(
    pub fn pgaio_error_cleanup()
);
