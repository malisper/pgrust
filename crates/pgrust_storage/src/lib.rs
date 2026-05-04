pub mod buffer;
pub mod fsm;
pub mod include;
pub mod lmgr;
pub mod page;
pub mod smgr;
pub mod sync;
pub mod wal;

pub use buffer::*;
pub use include::storage::buf_internals::{
    BufferId, BufferTag, BufferUsageStats, ClientId, FlushResult, Page, RequestPageResult,
};
pub use smgr::{BLCKSZ, ForkNumber, RelFileLocator};

/// :HACK: Compatibility namespace for mechanically moved storage files. The
/// long-term shape is direct `pgrust_storage::*` imports inside this crate.
pub mod backend {
    pub mod access {
        pub mod transam {
            pub mod xact {
                pub use pgrust_core::{
                    CommandId, FIRST_NORMAL_TRANSACTION_ID, FROZEN_TRANSACTION_ID,
                    INVALID_TRANSACTION_ID, Snapshot, TransactionId,
                };
            }
        }
    }

    pub mod storage {
        pub use crate::{
            buffer, fsm, fsync_dir, fsync_file, lmgr, page, smgr, sync, sync_file_data,
        };
    }

    pub mod utils {
        pub mod activity {
            use pgrust_nodes::datetime::{TimestampTzADT, USECS_PER_DAY, USECS_PER_SEC};

            const UNIX_EPOCH_TO_POSTGRES_EPOCH_DAYS: i64 = 10_957;

            pub fn now_timestamptz() -> TimestampTzADT {
                TimestampTzADT(current_postgres_timestamp_usecs())
            }

            fn current_postgres_timestamp_usecs() -> i64 {
                match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
                    Ok(duration) => {
                        let unix_usecs = duration.as_secs() as i64 * USECS_PER_SEC
                            + duration.subsec_micros() as i64;
                        unix_usecs - UNIX_EPOCH_TO_POSTGRES_EPOCH_DAYS * USECS_PER_DAY
                    }
                    Err(err) => {
                        let duration = err.duration();
                        let unix_usecs = duration.as_secs() as i64 * USECS_PER_SEC
                            + duration.subsec_micros() as i64;
                        -unix_usecs - UNIX_EPOCH_TO_POSTGRES_EPOCH_DAYS * USECS_PER_DAY
                    }
                }
            }
        }

        pub mod misc {
            pub mod interrupts {
                pub use pgrust_core::{InterruptReason, InterruptState, check_for_interrupts};
            }
        }

        pub mod time {
            pub mod instant {
                #[cfg(not(target_arch = "wasm32"))]
                pub use std::time::Instant;
                #[cfg(target_arch = "wasm32")]
                pub use web_time::Instant;
            }
        }
    }
}

/// :HACK: Compatibility namespace for storage include files moved before the
/// root `include` module is fully removed.
pub mod compat_include {
    pub mod storage {
        pub use crate::include::storage::*;
    }
}

pub mod include_compat_nodes {
    pub mod datetime {
        pub use pgrust_nodes::datetime::*;
    }
    pub mod parsenodes {
        pub use pgrust_nodes::parsenodes::*;
    }
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
