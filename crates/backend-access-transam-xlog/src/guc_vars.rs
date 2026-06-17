//! xlog.c-owned GUC variable storage and accessor installation.
//!
//! Mirrors the file-scope GUC globals in xlog.c (and a few xlog.h externs)
//! that back the WAL settings whose `config_*` entries in guc_tables.c point
//! their `&variable` at an xlog.c symbol. Each value is read by C straight
//! from its GUC slot (`*conf->variable`) — none of these is sourced from the
//! ControlFile at runtime (the only ControlFile-seeded one here is
//! `wal_segment_size`, which still has its own GUC slot whose backing store is
//! the same `WAL_SEGMENT_SIZE` cell `ReadControlFile` writes).
//!
//! The boot defaults mirror the `boot_val` column of each guc_tables.c entry.
//! The GUC machinery seeds the live value from `boot_val` during
//! `InitializeGUCOptions`; until then these cells hold the same default a C
//! backend would see before the GUC subsystem runs.
//!
//! `max_wal_size`/`min_wal_size`/`hot_standby` are installed by
//! [`crate::guc_state`] (they participate in the CheckPointSegments recompute /
//! were wired earlier); this module installs the rest.

extern crate std;

use core::cell::Cell;

use backend_utils_misc_guc_tables::{vars, GucVarAccessors};

std::thread_local! {
    /// `bool fullPageWrites = true` (xlog.c). `full_page_writes` GUC.
    static FULL_PAGE_WRITES: Cell<bool> = const { Cell::new(true) };
    /// `bool wal_log_hints = false` (xlog.c). `wal_log_hints` GUC.
    static WAL_LOG_HINTS: Cell<bool> = const { Cell::new(false) };
    /// `bool wal_init_zero = true` (xlog.c). `wal_init_zero` GUC.
    static WAL_INIT_ZERO: Cell<bool> = const { Cell::new(true) };
    /// `bool wal_recycle = true` (xlog.c). `wal_recycle` GUC.
    static WAL_RECYCLE: Cell<bool> = const { Cell::new(true) };
    /// `bool log_checkpoints = true` (xlog.c). `log_checkpoints` GUC.
    static LOG_CHECKPOINTS: Cell<bool> = const { Cell::new(true) };
    /// `bool track_wal_io_timing = false` (xlog.c). `track_wal_io_timing` GUC.
    static TRACK_WAL_IO_TIMING: Cell<bool> = const { Cell::new(false) };

    /// `int XLogArchiveTimeout = 0` (xlog.c). `archive_timeout` GUC (seconds).
    static XLOG_ARCHIVE_TIMEOUT: Cell<i32> = const { Cell::new(0) };
    /// `int wal_decode_buffer_size = 512 * 1024` (xlog.c). `wal_decode_buffer_size`.
    static WAL_DECODE_BUFFER_SIZE: Cell<i32> = const { Cell::new(512 * 1024) };
    /// `int wal_keep_size_mb = 0` (xlog.c). `wal_keep_size` GUC.
    static WAL_KEEP_SIZE_MB: Cell<i32> = const { Cell::new(0) };
    /// `int max_slot_wal_keep_size_mb = -1` (xlog.c). `max_slot_wal_keep_size`.
    static MAX_SLOT_WAL_KEEP_SIZE_MB: Cell<i32> = const { Cell::new(-1) };
    /// `int CommitDelay = 0` (xlog.c). `commit_delay` GUC (microseconds).
    static COMMIT_DELAY: Cell<i32> = const { Cell::new(0) };
    /// `int CommitSiblings = 5` (xlog.c). `commit_siblings` GUC.
    static COMMIT_SIBLINGS: Cell<i32> = const { Cell::new(5) };
    /// `int wal_retrieve_retry_interval = 5000` (xlog.c). `wal_retrieve_retry_interval`.
    static WAL_RETRIEVE_RETRY_INTERVAL: Cell<i32> = const { Cell::new(5000) };

    /// `int XLogArchiveMode = ARCHIVE_MODE_OFF` (xlog.c). `archive_mode` enum GUC.
    /// ARCHIVE_MODE_OFF == 0.
    static XLOG_ARCHIVE_MODE: Cell<i32> = const { Cell::new(0) };
    /// `int wal_compression = WAL_COMPRESSION_NONE` (xlog.c). `wal_compression`
    /// enum GUC. WAL_COMPRESSION_NONE == 0.
    static WAL_COMPRESSION: Cell<i32> = const { Cell::new(0) };
    /// `int wal_level = WAL_LEVEL_REPLICA` (xlog.c). `wal_level` enum GUC.
    /// WAL_LEVEL_MINIMAL=0, WAL_LEVEL_REPLICA=1, WAL_LEVEL_LOGICAL=2.
    static WAL_LEVEL: Cell<i32> = const { Cell::new(1) };
    /// `int wal_sync_method = DEFAULT_WAL_SYNC_METHOD` (xlog.c). `wal_sync_method`
    /// enum GUC. On this (darwin) build the platform default is
    /// WAL_SYNC_METHOD_OPEN_DSYNC == 4 (xlogdefs.h fallback).
    static WAL_SYNC_METHOD: Cell<i32> = const { Cell::new(4) };
}

std::thread_local! {
    /// `char *XLogArchiveCommand = ""` (xlog.c). `archive_command` string GUC.
    static XLOG_ARCHIVE_COMMAND: std::cell::RefCell<Option<std::string::String>> =
        std::cell::RefCell::new(Some(std::string::String::new()));
    /// `char *wal_consistency_checking_string = ""` (xlog.c).
    /// `wal_consistency_checking` string GUC (the comma-list input string; the
    /// per-rmgr bool array is built by `assign_wal_consistency_checking`).
    static WAL_CONSISTENCY_CHECKING_STRING: std::cell::RefCell<Option<std::string::String>> =
        std::cell::RefCell::new(Some(std::string::String::new()));
}

/// Install the xlog.c-owned GUC variable accessors (`conf->variable`) for the
/// WAL settings backed by xlog.c globals. Called from [`crate::init_seams`].
pub fn install() {
    // --- bool GUCs ---------------------------------------------------------
    vars::fullPageWrites.install(GucVarAccessors {
        get: || FULL_PAGE_WRITES.with(Cell::get),
        set: |v| FULL_PAGE_WRITES.with(|c| c.set(v)),
    });
    vars::wal_log_hints.install(GucVarAccessors {
        get: || WAL_LOG_HINTS.with(Cell::get),
        set: |v| WAL_LOG_HINTS.with(|c| c.set(v)),
    });
    vars::wal_init_zero.install(GucVarAccessors {
        get: || WAL_INIT_ZERO.with(Cell::get),
        set: |v| WAL_INIT_ZERO.with(|c| c.set(v)),
    });
    vars::wal_recycle.install(GucVarAccessors {
        get: || WAL_RECYCLE.with(Cell::get),
        set: |v| WAL_RECYCLE.with(|c| c.set(v)),
    });
    vars::log_checkpoints.install(GucVarAccessors {
        get: || LOG_CHECKPOINTS.with(Cell::get),
        set: |v| LOG_CHECKPOINTS.with(|c| c.set(v)),
    });
    vars::track_wal_io_timing.install(GucVarAccessors {
        get: || TRACK_WAL_IO_TIMING.with(Cell::get),
        set: |v| TRACK_WAL_IO_TIMING.with(|c| c.set(v)),
    });

    // --- int GUCs ----------------------------------------------------------
    vars::XLogArchiveTimeout.install(GucVarAccessors {
        get: || XLOG_ARCHIVE_TIMEOUT.with(Cell::get),
        set: |v| XLOG_ARCHIVE_TIMEOUT.with(|c| c.set(v)),
    });
    vars::wal_decode_buffer_size.install(GucVarAccessors {
        get: || WAL_DECODE_BUFFER_SIZE.with(Cell::get),
        set: |v| WAL_DECODE_BUFFER_SIZE.with(|c| c.set(v)),
    });
    vars::wal_keep_size_mb.install(GucVarAccessors {
        get: || WAL_KEEP_SIZE_MB.with(Cell::get),
        set: |v| WAL_KEEP_SIZE_MB.with(|c| c.set(v)),
    });
    vars::max_slot_wal_keep_size_mb.install(GucVarAccessors {
        get: || MAX_SLOT_WAL_KEEP_SIZE_MB.with(Cell::get),
        set: |v| MAX_SLOT_WAL_KEEP_SIZE_MB.with(|c| c.set(v)),
    });
    vars::CommitDelay.install(GucVarAccessors {
        get: || COMMIT_DELAY.with(Cell::get),
        set: |v| COMMIT_DELAY.with(|c| c.set(v)),
    });
    vars::CommitSiblings.install(GucVarAccessors {
        get: || COMMIT_SIBLINGS.with(Cell::get),
        set: |v| COMMIT_SIBLINGS.with(|c| c.set(v)),
    });
    vars::wal_retrieve_retry_interval.install(GucVarAccessors {
        get: || WAL_RETRIEVE_RETRY_INTERVAL.with(Cell::get),
        set: |v| WAL_RETRIEVE_RETRY_INTERVAL.with(|c| c.set(v)),
    });
    // `XLOGbuffers` + `wal_segment_size` reuse the existing xlog.c-global cells
    // in `crate::shmem` (the resolved buffer count / control-file segment size).
    vars::XLOGbuffers.install(GucVarAccessors {
        get: crate::shmem::xlog_buffers,
        set: crate::shmem::set_xlog_buffers,
    });
    vars::wal_segment_size.install(GucVarAccessors {
        get: crate::shmem::wal_segment_size,
        set: crate::shmem::set_wal_segment_size,
    });

    // --- enum GUCs (stored as the int ordinal, like C `int` enum vars) -----
    vars::XLogArchiveMode.install(GucVarAccessors {
        get: || XLOG_ARCHIVE_MODE.with(Cell::get),
        set: |v| XLOG_ARCHIVE_MODE.with(|c| c.set(v)),
    });
    vars::wal_compression.install(GucVarAccessors {
        get: || WAL_COMPRESSION.with(Cell::get),
        set: |v| WAL_COMPRESSION.with(|c| c.set(v)),
    });
    vars::wal_level.install(GucVarAccessors {
        get: || WAL_LEVEL.with(Cell::get),
        set: |v| WAL_LEVEL.with(|c| c.set(v)),
    });
    vars::wal_sync_method.install(GucVarAccessors {
        get: || WAL_SYNC_METHOD.with(Cell::get),
        set: |v| WAL_SYNC_METHOD.with(|c| c.set(v)),
    });

    // --- string GUCs (`char **variable`; NULL stays distinct from empty) ---
    vars::XLogArchiveCommand.install(GucVarAccessors {
        get: || XLOG_ARCHIVE_COMMAND.with(|c| c.borrow().clone()),
        set: |v| XLOG_ARCHIVE_COMMAND.with(|c| *c.borrow_mut() = v),
    });
    vars::wal_consistency_checking_string.install(GucVarAccessors {
        get: || WAL_CONSISTENCY_CHECKING_STRING.with(|c| c.borrow().clone()),
        set: |v| WAL_CONSISTENCY_CHECKING_STRING.with(|c| *c.borrow_mut() = v),
    });
}
