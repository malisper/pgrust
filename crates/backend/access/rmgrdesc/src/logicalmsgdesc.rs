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
        // The attacker controls prefix_size/message_size (64-bit fields read
        // straight from the record). Validate them against the actual payload
        // length with overflow-checked arithmetic before any slicing, and
        // require prefix_size >= 1 so the trailing-NUL trim (prefix_size - 1)
        // cannot underflow. On violation surface record_truncated instead of
        // panicking on an out-of-bounds slice.
        const HEADER: usize = 24;
        let payload_end = HEADER
            .checked_add(prefix_size)
            .and_then(|end| end.checked_add(message_size))
            .filter(|&end| prefix_size >= 1 && end <= rec.0.len())
            .ok_or_else(|| crate::record_truncated("xl_logical_message"))?;
        let prefix = rec
            .0
            .get(HEADER..HEADER + prefix_size)
            .ok_or_else(|| crate::record_truncated("xl_logical_message"))?;
        let message = rec
            .0
            .get(HEADER + prefix_size..payload_end)
            .ok_or_else(|| crate::record_truncated("xl_logical_message"))?;
        let prefix_str = prefix
            .get(..prefix_size - 1)
            .ok_or_else(|| crate::record_truncated("xl_logical_message prefix"))
            .and_then(|s| {
                core::str::from_utf8(s)
                    .map_err(|_| crate::record_truncated("xl_logical_message prefix"))
            })?;

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
