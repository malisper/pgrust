use crate::{appendf, rec_data, rec_info, Rec, XLR_INFO_MASK};
use multixact::{
    XLOG_MULTIXACT_CREATE_ID, XLOG_MULTIXACT_TRUNCATE_ID, XLOG_MULTIXACT_ZERO_MEM_PAGE,
    XLOG_MULTIXACT_ZERO_OFF_PAGE,
};
use stringinfo::StringInfo;
use types_error::PgResult;
use xlogreader_seams::XLogReaderState;

fn out_member(buf: &mut StringInfo<'_>, xid: u32, status: u32) -> PgResult<()> {
    appendf!(buf, "{xid} ")?;
    // MultiXactStatus values 0..=5 (multixact.h); the default arm mirrors C.
    buf.append_str(match status {
        0 => "(keysh) ",
        1 => "(sh) ",
        2 => "(fornokeyupd) ",
        3 => "(forupd) ",
        4 => "(nokeyupd) ",
        5 => "(upd) ",
        _ => "(unk) ",
    })
}

pub fn multixact_desc(buf: &mut StringInfo<'_>, record: &XLogReaderState) -> PgResult<()> {
    let rec = Rec(rec_data(record));
    let info = rec_info(record) & !XLR_INFO_MASK;

    if info == XLOG_MULTIXACT_ZERO_OFF_PAGE || info == XLOG_MULTIXACT_ZERO_MEM_PAGE {
        appendf!(buf, "{}", rec.i64(0, "multixact zero page")?)?;
    } else if info == XLOG_MULTIXACT_CREATE_ID {
        // xl_multixact_create: mid 0, moff 4, nmembers 8, members[] 12
        // (MultiXactMember is { xid, status }: 8 bytes).
        let what = "xl_multixact_create";
        let nmembers = rec.i32(8, what)?.max(0) as usize;
        appendf!(
            buf,
            "{} offset {} nmembers {}: ",
            rec.u32(0, what)?,
            rec.u32(4, what)?,
            nmembers
        )?;
        for i in 0..nmembers {
            out_member(buf, rec.u32(12 + 8 * i, what)?, rec.u32(16 + 8 * i, what)?)?;
        }
    } else if info == XLOG_MULTIXACT_TRUNCATE_ID {
        // xl_multixact_truncate: oldestMultiDB 0, startTruncOff 4,
        // endTruncOff 8, startTruncMemb 12, endTruncMemb 16.
        let what = "xl_multixact_truncate";
        appendf!(
            buf,
            "offsets [{}, {}), members [{}, {})",
            rec.u32(4, what)?,
            rec.u32(8, what)?,
            rec.u32(12, what)?,
            rec.u32(16, what)?
        )?;
    }
    Ok(())
}

pub fn multixact_identify(info: u8) -> Option<&'static str> {
    match info & !XLR_INFO_MASK {
        XLOG_MULTIXACT_ZERO_OFF_PAGE => Some("ZERO_OFF_PAGE"),
        XLOG_MULTIXACT_ZERO_MEM_PAGE => Some("ZERO_MEM_PAGE"),
        XLOG_MULTIXACT_CREATE_ID => Some("CREATE_ID"),
        XLOG_MULTIXACT_TRUNCATE_ID => Some("TRUNCATE_ID"),
        _ => None,
    }
}
