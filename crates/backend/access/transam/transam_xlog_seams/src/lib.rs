use types_core::XLogRecPtr;
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
