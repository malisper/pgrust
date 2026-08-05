use crate::{appendf, rec_data, rec_info, Rec, XLR_INFO_MASK};
use relmapper::XLOG_RELMAP_UPDATE;
use stringinfo::StringInfo;
use types_error::PgResult;
use xlogreader_seams::XLogReaderState;

pub fn relmap_desc(buf: &mut StringInfo<'_>, record: &XLogReaderState) -> PgResult<()> {
    let rec = Rec(rec_data(record));
    let info = rec_info(record) & !XLR_INFO_MASK;

    if info == XLOG_RELMAP_UPDATE {
        // xl_relmap_update: dbid 0, tsid 4, nbytes 8.
        appendf!(
            buf,
            "database {} tablespace {} size {}",
            rec.u32(0, "xl_relmap_update")?,
            rec.u32(4, "xl_relmap_update")?,
            rec.i32(8, "xl_relmap_update")?
        )?;
    }
    Ok(())
}

pub fn relmap_identify(info: u8) -> Option<&'static str> {
    match info & !XLR_INFO_MASK {
        XLOG_RELMAP_UPDATE => Some("UPDATE"),
        _ => None,
    }
}
