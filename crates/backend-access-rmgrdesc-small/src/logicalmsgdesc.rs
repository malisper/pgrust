//! `logicalmsgdesc.c` — rmgr descriptor routines for
//! `replication/logical/message.c`.

use mcx::PgString;
use types_core::uint8;
use types_error::PgResult;
use types_wal::{xl_logical_message, DecodedXLogRecord, XLR_INFO_MASK};

use crate::util::{append_lossy, appendf, record_truncated};

/// `XLOG_LOGICAL_MESSAGE` (replication/message.h).
pub const XLOG_LOGICAL_MESSAGE: uint8 = 0x00;

/// `logicalmsg_desc`. The payload is an [`xl_logical_message`]: the
/// NUL-terminated prefix (`prefix_size` bytes including the NUL) followed by
/// `message_size` payload bytes, written as hex.
pub fn logicalmsg_desc(buf: &mut PgString<'_>, record: &DecodedXLogRecord<'_>) -> PgResult<()> {
    let data = record.main_data();
    let info = record.info() & !XLR_INFO_MASK;

    if info == XLOG_LOGICAL_MESSAGE {
        let xlrec = xl_logical_message::from_bytes(data)
            .ok_or_else(|| record_truncated("xl_logical_message"))?;
        let prefix = xlrec.prefix();

        // Assert(prefix[xlrec->prefix_size - 1] == '\0')
        debug_assert!(prefix.last() == Some(&0), "prefix must be NUL-terminated");
        // %s prints up to the first NUL (guaranteed within prefix_size).
        let nul = prefix.iter().position(|&b| b == 0).unwrap_or(prefix.len());

        appendf!(
            buf,
            "{}, prefix \"",
            if xlrec.transactional() { "transactional" } else { "non-transactional" }
        )?;
        append_lossy(buf, &prefix[..nul])?;
        appendf!(buf, "\"; payload ({} bytes): ", xlrec.message_size())?;

        // Write message payload as a series of hex bytes.
        let mut sep = "";
        for byte in xlrec.payload() {
            appendf!(buf, "{sep}{byte:02X}")?;
            sep = " ";
        }
    }

    Ok(())
}

/// `logicalmsg_identify`.
pub fn logicalmsg_identify(info: uint8) -> Option<&'static str> {
    if (info & !XLR_INFO_MASK) == XLOG_LOGICAL_MESSAGE {
        return Some("MESSAGE");
    }
    None
}

/// Adapter installed into the rmgr-table `logicalmsg_desc` seam: extracts the decoded
/// record from the dispatcher's `XLogReaderState` (C's `record->record`) and
/// renders it. The reader is always positioned on a decoded record when the
/// rmgr table invokes `rm_desc`.
pub fn logicalmsg_desc_seam(
    buf: &mut PgString<'_>,
    record: &types_wal::rmgr::XLogReaderState<'_>,
) -> PgResult<()> {
    let record = record
        .record
        .as_ref()
        .expect("logicalmsg_desc called without a decoded record");
    logicalmsg_desc(buf, record)
}
