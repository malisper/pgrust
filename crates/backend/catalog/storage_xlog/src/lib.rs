// storage_xlog.h + storage.c smgr_redo (XLOG_SMGR_TRUNCATE arm unported).
use types_core::{ForkNumber, INVALID_PROC_NUMBER};
use types_error::PgResult;
use types_storage::{RelFileLocator, RelFileLocatorBackend};
use xlogreader_seams::XLogReaderState;

pub const XLOG_SMGR_CREATE: u8 = 0x10;
pub const XLOG_SMGR_TRUNCATE: u8 = 0x20;

pub fn smgr_redo(record: &mut XLogReaderState) -> PgResult<()> {
    const XLR_INFO_MASK: u8 = 0x0F;
    let rec = record.record.as_ref().expect("smgr redo with no decoded record");
    let info = rec.xl_info & !XLR_INFO_MASK;
    // SAFETY: points into the reader's decode buffer, valid for this callback.
    let xlrec = unsafe { rec.main_data_bytes() };
    if info == XLOG_SMGR_CREATE {
        let locator = RelFileLocator::new(
            u32::from_ne_bytes(xlrec[0..4].try_into().unwrap()),
            u32::from_ne_bytes(xlrec[4..8].try_into().unwrap()),
            u32::from_ne_bytes(xlrec[8..12].try_into().unwrap()),
        );
        let fork_num =
            ForkNumber::from_i32(i32::from_ne_bytes(xlrec[12..16].try_into().unwrap()))
                .expect("invalid forknum in XLOG_SMGR_CREATE");
        let key = RelFileLocatorBackend { locator, backend: INVALID_PROC_NUMBER };
        smgr::smgropen(locator, INVALID_PROC_NUMBER)?;
        smgr::smgrcreate(key, fork_num, true)?;
        Ok(())
    } else {
        panic!("smgr_redo (storage.c): XLOG_SMGR_TRUNCATE unported (info {info:02X})");
    }
}
