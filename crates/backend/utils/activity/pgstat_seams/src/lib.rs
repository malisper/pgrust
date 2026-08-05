seam_core::seam!(
    pub fn pgstat_set_session_end_cause_fatal()
);

seam_core::seam!(
    pub fn pgstat_get_slru_index(name: &str) -> i32
);

seam_core::seam!(
    pub fn pgstat_count_slru_page_zeroed(slru_idx: i32)
);

seam_core::seam!(
    pub fn pgstat_count_slru_page_hit(slru_idx: i32)
);

seam_core::seam!(
    pub fn pgstat_count_slru_page_read(slru_idx: i32)
);

seam_core::seam!(
    pub fn pgstat_count_slru_page_written(slru_idx: i32)
);

seam_core::seam!(
    pub fn pgstat_count_slru_page_exists(slru_idx: i32)
);

seam_core::seam!(
    pub fn pgstat_count_slru_flush(slru_idx: i32)
);

seam_core::seam!(
    pub fn pgstat_count_slru_truncate(slru_idx: i32)
);

seam_core::seam!(
    pub fn pgstat_count_checkpointer_slru_written()
);

// PendingCheckpointerStats counter bumps from CheckpointerMain / ShutdownXLOG
// (checkpointer.c writes the extern struct directly).
seam_core::seam!(
    pub fn pgstat_count_checkpointer_num_timed()
);

seam_core::seam!(
    pub fn pgstat_count_checkpointer_num_requested()
);

seam_core::seam!(
    pub fn pgstat_count_checkpointer_num_performed()
);

seam_core::seam!(
    pub fn pgstat_count_checkpointer_restartpoints_timed()
);

seam_core::seam!(
    pub fn pgstat_count_checkpointer_restartpoints_requested()
);

seam_core::seam!(
    pub fn pgstat_count_checkpointer_restartpoints_performed()
);

// LogCheckpointEnd (xlog.c): PendingCheckpointerStats.write_time +=
// write_msecs / .sync_time += sync_msecs — unconditional, not gated on
// log_checkpoints.
seam_core::seam!(
    pub fn pgstat_count_checkpointer_write_time(msecs: i64)
);

seam_core::seam!(
    pub fn pgstat_count_checkpointer_sync_time(msecs: i64)
);

// Returns C's rel->pgstat_enabled; pgstat keys pgstat_info by relid.
seam_core::seam!(
    pub fn pgstat_init_relation(relid: types_core::Oid, relkind: u8) -> bool
);

seam_core::seam!(
    // `pgstat_report_tempfile(filesize)` (utils/activity/pgstat_database.c).
    pub fn pgstat_report_tempfile(file_size: u64)
);

seam_core::seam!(
    // pgstat_initialize (pgstat.c).
    pub fn pgstat_initialize() -> types_error::PgResult<()>
);

seam_core::seam!(
    // Retention claim (wretain): re-arm the shutdown hook + WAL baseline for
    // a pooled thread whose pgstat TLS survived a park.
    pub fn pgstat_reattach_retained_backend() -> types_error::PgResult<()>
);

seam_core::seam!(
    // pgstat_before_server_shutdown(code, arg) (pgstat.c), before_shmem_exit shape.
    pub fn pgstat_before_server_shutdown(code: i32) -> types_error::PgResult<()>
);

seam_core::seam!(
    // pgstat_restore_stats() (pgstat.c).
    pub fn pgstat_restore_stats() -> types_error::PgResult<()>
);

seam_core::seam!(
    // pgstat_discard_stats() (pgstat.c).
    pub fn pgstat_discard_stats() -> types_error::PgResult<()>
);

seam_core::seam!(
    // pgstat_report_checkpointer() (pgstat_checkpointer.c).
    pub fn pgstat_report_checkpointer()
);

seam_core::seam!(
    // pgstat_report_bgwriter() (pgstat_bgwriter.c).
    pub fn pgstat_report_bgwriter()
);

seam_core::seam!(
    // pgstat_report_wal(force) (pgstat_wal.c).
    pub fn pgstat_report_wal(force: bool)
);

seam_core::seam!(
    // pgstat_report_fixed = true (pgstat.c); the xlog insert path's arm.
    pub fn pgstat_report_fixed_set()
);

// Discriminants of pgstat's IOObject/IOOp and types_storage's IOContext; the
// io seams take them raw so callers need no pgstat type dependency.
pub const IOOBJECT_WAL: u32 = 2;
pub const IOCONTEXT_INIT: u32 = 2;
pub const IOCONTEXT_NORMAL: u32 = 3;
pub const IOOP_FSYNC: u32 = 1;
pub const IOOP_READ: u32 = 6;
pub const IOOP_WRITE: u32 = 7;

seam_core::seam!(
    // pgstat_count_io_op (pgstat_io.c); IOObject/IOContext/IOOp as their enum
    // discriminants.
    pub fn pgstat_count_io_op(io_object: u32, io_context: u32, io_op: u32, cnt: u32, bytes: u64)
);

seam_core::seam!(
    // pgstat_prepare_io_time (pgstat_io.c); returns ns start, 0 = disabled.
    pub fn pgstat_prepare_io_time(track_io_guc: bool) -> i64
);

seam_core::seam!(
    // pgstat_count_io_op_time (pgstat_io.c).
    pub fn pgstat_count_io_op_time(
        io_object: u32,
        io_context: u32,
        io_op: u32,
        start_ns: i64,
        cnt: u32,
        bytes: u64,
    )
);
