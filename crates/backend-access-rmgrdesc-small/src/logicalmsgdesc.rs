//! `logicalmsgdesc.c` — rmgr descriptor routines for
//! `replication/logical/message.c`.

use mcx::PgString;
use types_core::{uint8, PgResult};
use types_wal::XLR_INFO_MASK;

use crate::util::{appendf, read_size, read_u8, record_truncated};

/// `XLOG_LOGICAL_MESSAGE` (replication/message.h).
pub const XLOG_LOGICAL_MESSAGE: uint8 = 0x00;

// xl_logical_message (replication/message.h):
//   { Oid dbId; bool transactional; Size prefix_size; Size message_size;
//     char message[FLEXIBLE_ARRAY_MEMBER]; }
// repr(C) offsets: dbId@0, transactional@4, padding to Size alignment,
// prefix_size@8, message_size@8+sizeof(Size), message@8+2*sizeof(Size).
const TRANSACTIONAL_OFF: usize = 4;
const PREFIX_SIZE_OFF: usize = 8;
const MESSAGE_SIZE_OFF: usize = PREFIX_SIZE_OFF + core::mem::size_of::<usize>();
const MESSAGE_OFF: usize = MESSAGE_SIZE_OFF + core::mem::size_of::<usize>();

/// `logicalmsg_desc`. The payload's flexible `message` array holds the
/// NUL-terminated prefix (`prefix_size` bytes including the NUL) followed by
/// `message_size` payload bytes, written as hex.
pub fn logicalmsg_desc(buf: &mut PgString<'_>, info: uint8, data: &[u8]) -> PgResult<()> {
    let info = info & !XLR_INFO_MASK;

    if info == XLOG_LOGICAL_MESSAGE {
        let transactional =
            read_u8(data, TRANSACTIONAL_OFF, "xl_logical_message.transactional")? != 0;
        let prefix_size = read_size(data, PREFIX_SIZE_OFF, "xl_logical_message.prefix_size")?;
        let message_size = read_size(data, MESSAGE_SIZE_OFF, "xl_logical_message.message_size")?;

        let prefix_bytes = data
            .get(MESSAGE_OFF..MESSAGE_OFF.saturating_add(prefix_size))
            .ok_or_else(|| record_truncated("xl_logical_message prefix"))?;
        // Assert(prefix[xlrec->prefix_size - 1] == '\0')
        debug_assert!(prefix_bytes.last() == Some(&0), "prefix must be NUL-terminated");
        // %s prints up to the first NUL (guaranteed within prefix_size).
        let nul = prefix_bytes
            .iter()
            .position(|&b| b == 0)
            .unwrap_or(prefix_bytes.len());
        let prefix = String::from_utf8_lossy(&prefix_bytes[..nul]);

        appendf!(
            buf,
            "{}, prefix \"{}\"; payload ({} bytes): ",
            if transactional { "transactional" } else { "non-transactional" },
            prefix,
            message_size
        )?;

        let message_start = MESSAGE_OFF + prefix_size;
        let message = data
            .get(message_start..message_start.saturating_add(message_size))
            .ok_or_else(|| record_truncated("xl_logical_message payload"))?;

        // Write message payload as a series of hex bytes.
        let mut sep = "";
        for byte in message {
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
