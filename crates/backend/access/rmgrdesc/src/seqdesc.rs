use crate::{appendf, rec_data, rec_info, Rec, XLR_INFO_MASK};
use stringinfo::StringInfo;
use types_error::PgResult;
use xlogreader_seams::XLogReaderState;

// commands/sequence.h; owning unit (backend-commands-sequence) not ported.
pub const XLOG_SEQ_LOG: u8 = 0x00;

pub fn seq_desc(buf: &mut StringInfo<'_>, record: &XLogReaderState) -> PgResult<()> {
    let rec = Rec(rec_data(record));
    let info = rec_info(record) & !XLR_INFO_MASK;

    if info == XLOG_SEQ_LOG {
        // xl_seq_rec: locator { spcOid 0, dbOid 4, relNumber 8 }.
        appendf!(
            buf,
            "rel {}/{}/{}",
            rec.u32(0, "xl_seq_rec")?,
            rec.u32(4, "xl_seq_rec")?,
            rec.u32(8, "xl_seq_rec")?
        )?;
    }
    Ok(())
}

pub fn seq_identify(info: u8) -> Option<&'static str> {
    match info & !XLR_INFO_MASK {
        XLOG_SEQ_LOG => Some("LOG"),
        _ => None,
    }
}
