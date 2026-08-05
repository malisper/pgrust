use crate::{appendf, rec_data, rec_info, Rec, XLR_INFO_MASK};
use commit_ts::{COMMIT_TS_TRUNCATE, COMMIT_TS_ZEROPAGE};
use stringinfo::StringInfo;
use types_error::PgResult;
use xlogreader_seams::XLogReaderState;

pub fn commit_ts_desc(buf: &mut StringInfo<'_>, record: &XLogReaderState) -> PgResult<()> {
    let rec = Rec(rec_data(record));
    let info = rec_info(record) & !XLR_INFO_MASK;

    if info == COMMIT_TS_ZEROPAGE {
        appendf!(buf, "{}", rec.i64(0, "COMMIT_TS_ZEROPAGE")?)?;
    } else if info == COMMIT_TS_TRUNCATE {
        // xl_commit_ts_truncate: pageno 0, oldestXid 8.
        appendf!(
            buf,
            "pageno {}, oldestXid {}",
            rec.i64(0, "xl_commit_ts_truncate")?,
            rec.u32(8, "xl_commit_ts_truncate")?
        )?;
    }
    Ok(())
}

pub fn commit_ts_identify(info: u8) -> Option<&'static str> {
    match info {
        COMMIT_TS_ZEROPAGE => Some("ZEROPAGE"),
        COMMIT_TS_TRUNCATE => Some("TRUNCATE"),
        _ => None,
    }
}
