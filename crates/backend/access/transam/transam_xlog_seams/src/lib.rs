use types_core::{TimeLineID, XLogRecPtr};
use types_error::PgResult;

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
    pub fn xlog_set_async_xact_lsn(lsn: XLogRecPtr)
);
