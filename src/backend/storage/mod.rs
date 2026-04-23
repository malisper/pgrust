pub mod buffer;
pub mod fsm;
pub mod lmgr;
pub mod page;
pub mod smgr;
pub mod sync;

/// POSIX fsync — flushes kernel buffer cache to disk without flushing the
/// hardware write cache.  Matches PostgreSQL's default `wal_sync_method = fdatasync`
/// on Linux and `wal_sync_method = fsync` on macOS.
///
/// Rust's `File::sync_data()` / `File::sync_all()` use macOS `F_FULLFSYNC`
/// which also flushes the disk's hardware write cache — much slower.
#[cfg(all(unix, not(test)))]
pub fn fsync_file(file: &std::fs::File) -> std::io::Result<()> {
    use std::os::unix::io::AsRawFd;
    let ret = unsafe { libc::fsync(file.as_raw_fd()) };
    if ret == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(any(not(unix), test))]
pub fn fsync_file(_file: &std::fs::File) -> std::io::Result<()> {
    Ok(())
}

#[cfg(not(test))]
pub fn sync_file_data(file: &std::fs::File) -> std::io::Result<()> {
    file.sync_data()
}

#[cfg(test)]
pub fn sync_file_data(_file: &std::fs::File) -> std::io::Result<()> {
    Ok(())
}
