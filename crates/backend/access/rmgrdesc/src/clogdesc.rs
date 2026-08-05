use crate::{appendf, rec_data, rec_info, Rec, XLR_INFO_MASK};
use clog::{CLOG_TRUNCATE, CLOG_ZEROPAGE};
use stringinfo::StringInfo;
use types_error::PgResult;
use xlogreader_seams::XLogReaderState;

pub fn clog_desc(buf: &mut StringInfo<'_>, record: &XLogReaderState) -> PgResult<()> {
    let rec = Rec(rec_data(record));
    let info = rec_info(record) & !XLR_INFO_MASK;

    if info == CLOG_ZEROPAGE {
        appendf!(buf, "page {}", rec.i64(0, "CLOG_ZEROPAGE")?)?;
    } else if info == CLOG_TRUNCATE {
        // xl_clog_truncate: pageno 0, oldestXact 8, oldestXactDb 12.
        appendf!(
            buf,
            "page {}; oldestXact {}",
            rec.i64(0, "xl_clog_truncate")?,
            rec.u32(8, "xl_clog_truncate")?
        )?;
    }
    Ok(())
}

pub fn clog_identify(info: u8) -> Option<&'static str> {
    match info & !XLR_INFO_MASK {
        CLOG_ZEROPAGE => Some("ZEROPAGE"),
        CLOG_TRUNCATE => Some("TRUNCATE"),
        _ => None,
    }
}
