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
    pub fn count_ckpt_slru_written()
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
    // DataChecksumsEnabled() (xlog.c): ControlFile->data_checksum_version > 0.
    pub fn data_checksums_enabled() -> bool
);
