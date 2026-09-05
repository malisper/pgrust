use types_core::{TimeLineID, XLogRecPtr};
use types_error::PgResult;
use xlogreader_seams::XLogReaderState;

seam_core::seam!(
    // xlog_redo (xlog.c) — the XLOG rmgr rm_redo callback; rmgr's table row
    // delegates here (a direct rmgr -> transam_xlog dep would cycle through
    // xlogreader).
    pub fn xlog_redo(record: &mut XLogReaderState) -> PgResult<()>
);

seam_core::seam!(
    // GetRedoRecPtr() (xlog.c).
    pub fn get_redo_rec_ptr() -> XLogRecPtr
);

seam_core::seam!(
    // XLogInsertRecord(rdata, fpw_lsn, flags, num_fpi, topxid_included)
    // (xlog.c). `rechdr` is the 24-byte XLogRecord header (xl_prev/xl_crc
    // filled in by the callee); `rdatas` is the rest of the rdata chain.
    pub fn xlog_insert_record<'a>(
        rechdr: &'a mut [u8; 24],
        rdatas: &'a [&'a [u8]],
        fpw_lsn: XLogRecPtr,
        flags: u8,
        num_fpi: i32,
        topxid_included: bool,
    ) -> PgResult<XLogRecPtr>
);

seam_core::seam!(
    // XLogInsertAllowed() (xlog.c).
    pub fn xlog_insert_allowed() -> bool
);

seam_core::seam!(
    // GetFullPageWriteInfo(&RedoRecPtr, &doPageWrites) (xlog.c).
    pub fn get_full_page_write_info() -> (XLogRecPtr, bool)
);

seam_core::seam!(
    pub fn xlog_flush(record: XLogRecPtr) -> PgResult<()>
);

seam_core::seam!(
    // GL-FLUSHPIPE-1: XLogFlush for the sync-commit durability wait —
    // xact.c:1502's `XLogFlush(XactLastRecEnd)` call site ONLY (every
    // other flush caller uses xlog_flush above). Behaviorally identical to
    // xlog_flush unless PGRUST_FLUSH_PIPELINE is armed, in which case the
    // contended wait rides the pending-flush queue instead of the
    // WALWriteLock convoy (transam_xlog::flushpipe).
    pub fn xlog_flush_commit(record: XLogRecPtr) -> PgResult<()>
);

seam_core::seam!(
    pub fn count_ckpt_slru_written()
);

seam_core::seam!(
    // CheckpointStats.ckpt_bufs_written += num_written (bufmgr.c:3626,
    // BufferSync) — the LogCheckpointEnd "wrote %d buffers" field.
    pub fn count_ckpt_bufs_written(num_written: i32)
);

seam_core::seam!(
    // CheckpointStats.ckpt_sync_rels / ckpt_longest_sync /
    // ckpt_agg_sync_time (sync.c:184-186, ProcessSyncRequests; the times
    // are microseconds) — the LogCheckpointEnd "sync files=" fields.
    pub fn record_ckpt_sync_stats(rels: i32, longest_us: u64, agg_us: u64)
);

seam_core::seam!(
    // XLogLogicalInfoActive() (xlog.h): wal_level >= logical.
    pub fn xlog_logical_info_active() -> bool
);

seam_core::seam!(
    // RecoveryInProgress() (xlog.c).
    pub fn recovery_in_progress() -> bool
);

seam_core::seam!(
    // pgWalUsage.wal_fpi reader (xlog.c increments it per inserted record).
    pub fn wal_usage_fpi() -> i64
);

seam_core::seam!(
    // pgWalUsage reader (instrument.h global, maintained by the xlog insert
    // and AdvanceXLInsertBuffer paths).
    pub fn wal_usage() -> types_core::instrument::WalUsage
);

seam_core::seam!(
    // GetFlushRecPtr(&insertTLI) (xlog.c): (flush ptr, insert TLI).
    pub fn get_flush_rec_ptr() -> (XLogRecPtr, TimeLineID)
);

seam_core::seam!(
    // GetWALInsertionTimeLineIfSet() (xlog.c): XLogCtl->InsertTimeLineID under
    // info_lck, 0 until end-of-recovery has set it. xlogutils reaches xlog
    // only through this crate (transam_xlog depends on xlogutils).
    // upstream 4bff3aa51c19 (18.6): Fix second race with timeline selection during promotion
    pub fn get_wal_insertion_time_line_if_set() -> TimeLineID
);

seam_core::seam!(
    // wal_segment_size (xlog.c global).
    pub fn wal_segment_size() -> i32
);

seam_core::seam!(
    // XLogStandbyInfoActive() (xlog.h): wal_level >= replica.
    pub fn xlog_standby_info_active() -> bool
);

seam_core::seam!(
    // XactLastRecEnd (xlog.c global).
    pub fn xact_last_rec_end() -> XLogRecPtr
);

seam_core::seam!(
    pub fn set_xact_last_rec_end(lsn: XLogRecPtr)
);

seam_core::seam!(
    // XactLastCommitEnd = lsn (xlog.c global).
    pub fn set_xact_last_commit_end(lsn: XLogRecPtr)
);

seam_core::seam!(
    // XactLastCommitEnd reader (worker.c store_flush_position).
    pub fn xact_last_commit_end() -> XLogRecPtr
);

seam_core::seam!(
    pub fn xlog_set_async_xact_lsn(lsn: XLogRecPtr)
);

seam_core::seam!(
    // XLogNeedsFlush(record) (xlog.c): no ereport path.
    pub fn xlog_needs_flush(record: XLogRecPtr) -> bool
);

seam_core::seam!(
    // StartupXLOG (xlog.c) — bootstrap/standalone WAL startup.
    pub fn startup_xlog() -> PgResult<()>
);

seam_core::seam!(
    // ShutdownXLOG(code, arg) (xlog.c), before_shmem_exit shape; body reads neither.
    pub fn shutdown_xlog() -> PgResult<()>
);

seam_core::seam!(
    // XLogPutNextOid (xlog.c); OID-prefetch WAL record.
    pub fn xlog_put_next_oid(next_oid: types_core::Oid) -> PgResult<()>
);

seam_core::seam!(
    // InitializeWalConsistencyChecking (xlog.c).
    pub fn initialize_wal_consistency_checking() -> PgResult<()>
);

seam_core::seam!(
    // The maskable resource managers (rmgr.c): (rm_name, rmid) for every
    // builtin rmgr whose rm_mask != NULL. check_wal_consistency_checking
    // (xlog.c) matches its list tokens (or "all") against exactly these.
    // Sourced from RmgrTable in the rmgr crate; transam_xlog cannot depend on
    // rmgr directly (it would cycle through xloginsert/xlogreader).
    pub fn wal_consistency_maskable_rmgrs() -> Vec<(&'static str, u8)>
);

seam_core::seam!(
    // DataChecksumsEnabled() (xlog.c): ControlFile->data_checksum_version > 0.
    pub fn data_checksums_enabled() -> bool
);
