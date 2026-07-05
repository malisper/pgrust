use crate::{appendf, rec_data, rec_info, Rec, XLR_INFO_MASK};
use stringinfo::StringInfo;
use types_error::PgResult;
use xlogreader_seams::XLogReaderState;

// xl_logical_message: dbId 0, transactional 4, prefix_size 8 (u64), message_size 16
// (u64), payload 24 (prefix incl. trailing NUL, then message bytes).
const XLOG_LOGICAL_MESSAGE: u8 = 0x00;

pub fn logicalmsg_desc(buf: &mut StringInfo<'_>, record: &XLogReaderState) -> PgResult<()> {
    let rec = Rec(rec_data(record));
    let info = rec_info(record) & !XLR_INFO_MASK;

    if info == XLOG_LOGICAL_MESSAGE {
        let transactional = rec.u8(4, "xl_logical_message")? != 0;
        let prefix_size = rec.u64(8, "xl_logical_message")? as usize;
        let message_size = rec.u64(16, "xl_logical_message")? as usize;
        let prefix = &rec.0[24..24 + prefix_size];
        let message = &rec.0[24 + prefix_size..24 + prefix_size + message_size];
        let prefix_str = core::str::from_utf8(&prefix[..prefix_size - 1])
            .map_err(|_| crate::record_truncated("xl_logical_message prefix"))?;

        appendf!(
            buf,
            "{}, prefix \"{prefix_str}\"; payload ({message_size} bytes): ",
            if transactional { "transactional" } else { "non-transactional" }
        )?;
        let mut sep = "";
        for &byte in message {
            appendf!(buf, "{sep}{byte:02X}")?;
            sep = " ";
        }
    }
    Ok(())
}

pub fn logicalmsg_identify(info: u8) -> Option<&'static str> {
    if info & !XLR_INFO_MASK == XLOG_LOGICAL_MESSAGE {
        Some("MESSAGE")
    } else {
        None
    }
}
