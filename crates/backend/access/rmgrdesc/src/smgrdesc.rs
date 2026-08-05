use crate::{appendf, rec_data, rec_info, Rec, XLR_INFO_MASK};
use storage_xlog::{XLOG_SMGR_CREATE, XLOG_SMGR_TRUNCATE};
use stringinfo::StringInfo;
use types_core::ForkNumber;
use types_error::PgResult;
use types_storage::RelFileLocator;
use xlogreader_seams::XLogReaderState;

pub fn smgr_desc(buf: &mut StringInfo<'_>, record: &XLogReaderState) -> PgResult<()> {
    let rec = Rec(rec_data(record));
    let info = rec_info(record) & !XLR_INFO_MASK;

    if info == XLOG_SMGR_CREATE {
        // xl_smgr_create: rlocator 0..12, forkNum 12.
        let what = "xl_smgr_create";
        let rlocator =
            RelFileLocator::new(rec.u32(0, what)?, rec.u32(4, what)?, rec.u32(8, what)?);
        let forknum = ForkNumber::from_i32(rec.i32(12, what)?)
            .unwrap_or(ForkNumber::MAIN_FORKNUM);
        buf.append_str(&relpath_seams::relpathperm::call(rlocator, forknum))?;
    } else if info == XLOG_SMGR_TRUNCATE {
        // xl_smgr_truncate: blkno 0, rlocator 4..16, flags 16.
        let what = "xl_smgr_truncate";
        let rlocator =
            RelFileLocator::new(rec.u32(4, what)?, rec.u32(8, what)?, rec.u32(12, what)?);
        appendf!(
            buf,
            "{} to {} blocks flags {}",
            relpath_seams::relpathperm::call(rlocator, ForkNumber::MAIN_FORKNUM),
            rec.u32(0, what)?,
            rec.i32(16, what)?
        )?;
    }
    Ok(())
}

pub fn smgr_identify(info: u8) -> Option<&'static str> {
    match info & !XLR_INFO_MASK {
        XLOG_SMGR_CREATE => Some("CREATE"),
        XLOG_SMGR_TRUNCATE => Some("TRUNCATE"),
        _ => None,
    }
}
