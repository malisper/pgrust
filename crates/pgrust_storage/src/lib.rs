pub mod buffer;
pub mod fsm;
pub mod include;
pub mod lmgr;
pub mod page;
pub mod smgr;
pub mod sync;
pub mod wal;

pub use buf_internals::{
    BufferId, BufferTag, BufferUsageStats, ClientId, FlushResult, Page, RequestPageResult,
};
pub use buffer::*;
pub use include::storage::buf_internals;
pub use smgr::{BLCKSZ, ForkNumber, RelFileLocator};

#[cfg(not(target_arch = "wasm32"))]
pub(crate) use std::time::Instant;
#[cfg(target_arch = "wasm32")]
pub(crate) use web_time::Instant;

pub(crate) fn now_timestamptz() -> pgrust_nodes::datetime::TimestampTzADT {
    use pgrust_nodes::datetime::{TimestampTzADT, USECS_PER_DAY, USECS_PER_SEC};

    const UNIX_EPOCH_TO_POSTGRES_EPOCH_DAYS: i64 = 10_957;

    #[cfg(not(target_arch = "wasm32"))]
    use std::time::{SystemTime, UNIX_EPOCH};
    #[cfg(target_arch = "wasm32")]
    use web_time::{SystemTime, UNIX_EPOCH};

    let unix_usecs = match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_secs() as i64 * USECS_PER_SEC + duration.subsec_micros() as i64,
        Err(err) => {
            let duration = err.duration();
            -(duration.as_secs() as i64 * USECS_PER_SEC + duration.subsec_micros() as i64)
        }
    };

    TimestampTzADT(unix_usecs - UNIX_EPOCH_TO_POSTGRES_EPOCH_DAYS * USECS_PER_DAY)
}

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

#[cfg(all(unix, not(test)))]
pub fn fsync_dir(path: &std::path::Path) -> std::io::Result<()> {
    let dir = std::fs::File::open(path)?;
    fsync_file(&dir)
}

#[cfg(any(not(unix), test))]
pub fn fsync_dir(_path: &std::path::Path) -> std::io::Result<()> {
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
