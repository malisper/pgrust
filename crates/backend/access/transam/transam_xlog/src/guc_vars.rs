use std::cell::Cell;

use guc_tables::{GucVarAccessors, vars};

// xlog.c-owned GUC globals: per-backend thread_local cells (C file-scope
// globals, one copy per backend), boot values matching guc_tables.
macro_rules! xlog_guc {
    ($($cell:ident, $var:ident, $ty:ty, $init:expr;)+) => {
        $(
            thread_local! {
                static $cell: Cell<$ty> = const {
                    assert!(!core::mem::needs_drop::<$ty>());
                    Cell::new($init)
                };
            }
        )+
        pub(crate) fn install() {
            $(
                vars::$var.install(GucVarAccessors {
                    get: || $cell.get(),
                    set: |v| $cell.set(v),
                });
            )+
        }
    };
}

xlog_guc! {
    MAX_WAL_SIZE_MB, max_wal_size_mb, i32, 1024;
    MIN_WAL_SIZE_MB, min_wal_size_mb, i32, 80;
    WAL_KEEP_SIZE_MB, wal_keep_size_mb, i32, 0;
    XLOG_BUFFERS, XLOGbuffers, i32, -1;
    XLOG_ARCHIVE_TIMEOUT, XLogArchiveTimeout, i32, 0;
    XLOG_ARCHIVE_MODE, XLogArchiveMode, i32, 0;
    ENABLE_HOT_STANDBY, EnableHotStandby, bool, true;
    FULL_PAGE_WRITES, fullPageWrites, bool, true;
    WAL_LOG_HINTS, wal_log_hints, bool, false;
    WAL_COMPRESSION, wal_compression, i32, 0;
    WAL_INIT_ZERO, wal_init_zero, bool, true;
    WAL_RECYCLE, wal_recycle, bool, true;
    LOG_CHECKPOINTS, log_checkpoints, bool, true;
    WAL_SYNC_METHOD, wal_sync_method, i32, crate::DEFAULT_WAL_SYNC_METHOD;
    WAL_LEVEL, wal_level, i32, crate::WAL_LEVEL_REPLICA;
    COMMIT_DELAY, CommitDelay, i32, 0;
    COMMIT_SIBLINGS, CommitSiblings, i32, 5;
    WAL_RETRIEVE_RETRY_INTERVAL, wal_retrieve_retry_interval, i32, 5000;
    MAX_SLOT_WAL_KEEP_SIZE_MB, max_slot_wal_keep_size_mb, i32, -1;
    WAL_DECODE_BUFFER_SIZE, wal_decode_buffer_size, i32, 512 * 1024;
    TRACK_WAL_IO_TIMING, track_wal_io_timing, bool, false;
}

// wal_consistency_checking_string (xlog.c): stored as a leaked &'static str
// so the cell stays !needs_drop; changes are boot-rare.
thread_local! {
    static WAL_CONSISTENCY_CHECKING_STRING: Cell<Option<&'static str>> = const { Cell::new(None) };
}

// XLogArchiveCommand (xlog.c): same leaked-&'static-str shape; SIGHUP
// reloads are boot-rare.
thread_local! {
    static XLOG_ARCHIVE_COMMAND: Cell<Option<&'static str>> = const { Cell::new(Some("")) };
}

pub(crate) fn install_xlog_archive_command() {
    vars::XLogArchiveCommand.install(GucVarAccessors {
        get: || XLOG_ARCHIVE_COMMAND.get().map(str::to_string),
        set: |v| XLOG_ARCHIVE_COMMAND.set(v.map(|s| &*s.leak())),
    });
    guc_tables::hooks::show_archive_command.install(show_archive_command);
}

// show_archive_command (xlog.c).
fn show_archive_command() -> String {
    if crate::XLogArchivingActive() {
        XLOG_ARCHIVE_COMMAND.get().unwrap_or("").to_string()
    } else {
        "(disabled)".to_string()
    }
}

pub(crate) fn install_wal_consistency_checking_string() {
    vars::wal_consistency_checking_string.install(GucVarAccessors {
        get: || WAL_CONSISTENCY_CHECKING_STRING.get().map(str::to_string),
        set: |v| WAL_CONSISTENCY_CHECKING_STRING.set(v.map(|s| &*s.leak())),
    });
}

// C owner is checkpointer.c, but CalculateCheckpointSegments reads it at
// boot before the checkpointer unit initializes, so the backing stays here
// (the checkpointer crate documents the same layering).
thread_local! {
    static CHECKPOINT_COMPLETION_TARGET: Cell<f64> = const { Cell::new(0.9) };
}

pub(crate) fn install_checkpoint_completion_target() {
    vars::CheckPointCompletionTarget.install(GucVarAccessors {
        get: || CHECKPOINT_COMPLETION_TARGET.get(),
        set: |v| CHECKPOINT_COMPLETION_TARGET.set(v),
    });
}

pub(crate) fn install_wal_segment_size() {
    vars::wal_segment_size.install(GucVarAccessors {
        get: crate::wal_segment_size,
        set: crate::set_wal_segment_size,
    });
}

// wal_sync_method_options[] / archive_mode_options[] (xlog.c).
pub(crate) const WAL_SYNC_METHOD_OPTIONS: &[types_guc::config_enum_entry] = &[
    types_guc::config_enum_entry { name: "fsync", val: crate::WAL_SYNC_METHOD_FSYNC, hidden: false },
    types_guc::config_enum_entry { name: "fsync_writethrough", val: crate::WAL_SYNC_METHOD_FSYNC_WRITETHROUGH, hidden: false },
    types_guc::config_enum_entry { name: "fdatasync", val: crate::WAL_SYNC_METHOD_FDATASYNC, hidden: false },
    types_guc::config_enum_entry { name: "open_sync", val: crate::WAL_SYNC_METHOD_OPEN, hidden: false },
    types_guc::config_enum_entry { name: "open_datasync", val: crate::WAL_SYNC_METHOD_OPEN_DSYNC, hidden: false },
];

pub(crate) const ARCHIVE_MODE_OPTIONS: &[types_guc::config_enum_entry] = &[
    types_guc::config_enum_entry { name: "always", val: 2, hidden: false },
    types_guc::config_enum_entry { name: "on", val: 1, hidden: false },
    types_guc::config_enum_entry { name: "off", val: 0, hidden: false },
    types_guc::config_enum_entry { name: "true", val: 1, hidden: true },
    types_guc::config_enum_entry { name: "false", val: 0, hidden: true },
    types_guc::config_enum_entry { name: "yes", val: 1, hidden: true },
    types_guc::config_enum_entry { name: "no", val: 0, hidden: true },
    types_guc::config_enum_entry { name: "1", val: 1, hidden: true },
    types_guc::config_enum_entry { name: "0", val: 0, hidden: true },
];
