pub mod buffer;
pub mod lmgr;
pub mod page;
pub mod smgr;

/// POSIX fsync — flushes kernel buffer cache to disk without flushing the
/// hardware write cache.  Matches PostgreSQL's default `wal_sync_method = fdatasync`
/// on Linux and `wal_sync_method = fsync` on macOS.
///
/// Rust's `File::sync_data()` / `File::sync_all()` use macOS `F_FULLFSYNC`
/// which also flushes the disk's hardware write cache — much slower.
#[cfg(unix)]
pub fn fsync_file(file: &std::fs::File) -> std::io::Result<()> {
    use std::os::unix::io::AsRawFd;
    let ret = unsafe { libc::fsync(file.as_raw_fd()) };
    if ret == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}
