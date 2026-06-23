//! `src/backend/replication/logical/message.c` (PostgreSQL 18.3) — generic
//! logical messages.
//!
//! Generic logical messages allow XLOG logging of arbitrary binary blobs that
//! get passed to the logical decoding plugin. In normal XLOG processing they
//! are the same as NOOP.
//!
//! These messages can be either transactional or non-transactional.
//! Transactional messages are part of the current transaction and will be sent
//! to the decoding plugin in the same way as DML operations. Non-transactional
//! messages are sent to the plugin at the time when the logical decoding reads
//! them from XLOG; this also means that transactional messages won't be
//! delivered if the transaction was rolled back but the non-transactional one
//! will always be delivered.
//!
//! Every message carries a prefix to avoid conflicts between different decoding
//! plugins.
//!
//! The two functions of `message.c` (`LogLogicalMessage`, `logicalmsg_redo`)
//! are ported here. The companion rmgr-descriptor routines (`logicalmsg_desc`,
//! `logicalmsg_identify`) live in `logicalmsgdesc.c`
//! (`backend-access-rmgrdesc-small`), and the SQL-callable
//! `pg_logical_emit_message_*` backends live in `logicalfuncs.c`.

#![no_std]
#![allow(non_snake_case)]

extern crate alloc;

use alloc::format;
use alloc::vec::Vec;

use ::utils_error::{ereport, PgError, PgResult};
use ::types_core::primitive::{Size, XLogRecPtr};
use ::types_error::error::PANIC;
use ::wal::rmgr::XLogReaderState;
use ::wal::wal::{RM_LOGICALMSG_ID, XLOG_INCLUDE_ORIGIN, XLOG_LOGICAL_MESSAGE, XLR_INFO_MASK};

/// `SizeOfLogicalMessage` (replication/message.h):
/// `offsetof(xl_logical_message, message)`.
///
/// On the LP64 target PostgreSQL builds for, the fixed header is laid out as
/// `Oid dbId` (4) + `bool transactional` (1) + 3 padding bytes + `Size
/// prefix_size` (8) + `Size message_size` (8) = 24 bytes, and the flexible
/// array member `message[]` follows at offset 24. [`serialize_header`] emits
/// exactly these bytes.
const SIZE_OF_LOGICAL_MESSAGE: usize = 24;

/// `LogLogicalMessage` (message.c:42): write a logical decoding message into
/// XLog. Returns the LSN at the end of the inserted record.
///
/// `prefix` is the decoding-plugin prefix (the C `const char *prefix`, a
/// NUL-terminated C string — here the bytes *before* the terminator); the first
/// `size` bytes of `message` are logged.
pub fn LogLogicalMessage(
    prefix: &[u8],
    message: &[u8],
    size: Size,
    transactional: bool,
    flush: bool,
) -> PgResult<XLogRecPtr> {
    let lsn: XLogRecPtr;

    /*
     * Force xid to be allocated if we're emitting a transactional message.
     */
    if transactional {
        debug_assert!(transam_xact::IsTransactionState());
        transam_xact::GetCurrentTransactionId()?;
    }

    let db_id = init_small::globals::MyDatabaseId();
    /* trailing zero is critical; see logicalmsg_desc */
    let prefix_size = prefix.len() + 1;
    let message_size = size;

    xloginsert::XLogBeginInsert()?;
    xloginsert::XLogRegisterData(&serialize_header(
        db_id,
        transactional,
        prefix_size,
        message_size,
    ))?;
    xloginsert::XLogRegisterData(&prefix_with_nul(prefix))?;
    xloginsert::XLogRegisterData(&message[..size])?;

    /* allow origin filtering */
    xloginsert::XLogSetRecordFlags(XLOG_INCLUDE_ORIGIN);

    lsn = xloginsert::XLogInsert(RM_LOGICALMSG_ID, XLOG_LOGICAL_MESSAGE)?;

    /*
     * Make sure that the message hits disk before leaving if emitting a
     * non-transactional message when flush is requested.
     */
    if !transactional && flush {
        transam_xlog::XLogFlush(lsn)?;
    }
    Ok(lsn)
}

/// `logicalmsg_redo` (message.c:87): redo is basically just a noop for logical
/// decoding messages.
pub fn logicalmsg_redo(record: &mut XLogReaderState<'_>) -> PgResult<()> {
    let info: u8 = record_get_info(record) & !XLR_INFO_MASK;

    if info != XLOG_LOGICAL_MESSAGE {
        return Err(panic_unknown_opcode(info));
    }

    /* This is only interesting for logical decoding, see decode.c. */
    Ok(())
}

/// `XLogRecGetInfo(record)` — the raw `xl_info` byte.
fn record_get_info(record: &XLogReaderState<'_>) -> u8 {
    record.record.as_ref().map(|r| r.info()).unwrap_or(0)
}

/// `elog(PANIC, "logicalmsg_redo: unknown op code %u", info)`.
fn panic_unknown_opcode(info: u8) -> PgError {
    ereport(PANIC)
        .errmsg_internal(format!("logicalmsg_redo: unknown op code {info}"))
        .into_error()
}

/// Serialize the fixed `xl_logical_message` header into the exact C-on-LP64
/// wire bytes that `XLogRegisterData(&xlrec, SizeOfLogicalMessage)` copies:
/// `dbId` (u32 LE), `transactional` (u8), 3 alignment-padding bytes,
/// `prefix_size` (u64 LE), `message_size` (u64 LE). Total length
/// [`SIZE_OF_LOGICAL_MESSAGE`].
fn serialize_header(
    db_id: ::types_core::primitive::Oid,
    transactional: bool,
    prefix_size: Size,
    message_size: Size,
) -> Vec<u8> {
    let mut buf = Vec::with_capacity(SIZE_OF_LOGICAL_MESSAGE);
    buf.extend_from_slice(&db_id.to_le_bytes()); //                     Oid dbId @0..4
    buf.push(transactional as u8); //                                   bool      @4
    buf.extend_from_slice(&[0u8; 3]); //                                padding   @5..8
    buf.extend_from_slice(&(prefix_size as u64).to_le_bytes()); //      Size      @8..16
    buf.extend_from_slice(&(message_size as u64).to_le_bytes()); //     Size      @16..24
    debug_assert_eq!(buf.len(), SIZE_OF_LOGICAL_MESSAGE);
    buf
}

/// `prefix` plus the trailing NUL byte, of total length `prefix_size`
/// (`strlen(prefix) + 1`), matching `XLogRegisterData(prefix, xlrec.prefix_size)`.
fn prefix_with_nul(prefix: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(prefix.len() + 1);
    buf.extend_from_slice(prefix);
    buf.push(0);
    buf
}

#[cfg(test)]
mod tests {
    extern crate std;
    use super::*;

    /// The header wire format is a silent-corruption hazard: the decoder
    /// (`logicalmsg_desc` / `decode.c`) reads `dbId`@0, `transactional`@4,
    /// `prefix_size`@8, `message_size`@16 back out of these exact 24 bytes.
    #[test]
    fn header_matches_c_on_lp64_wire_layout() {
        let h = serialize_header(0x1234_5678, true, 6, 11);
        assert_eq!(h.len(), SIZE_OF_LOGICAL_MESSAGE);
        assert_eq!(&h[0..4], &0x1234_5678u32.to_le_bytes());
        assert_eq!(h[4], 1);
        assert_eq!(&h[5..8], &[0, 0, 0]);
        assert_eq!(&h[8..16], &6u64.to_le_bytes());
        assert_eq!(&h[16..24], &11u64.to_le_bytes());

        let h = serialize_header(0, false, 1, 0);
        assert_eq!(h[4], 0);
        assert_eq!(&h[8..16], &1u64.to_le_bytes());
        assert_eq!(&h[16..24], &0u64.to_le_bytes());
    }

    #[test]
    fn prefix_gets_trailing_nul() {
        assert_eq!(&prefix_with_nul(b"myext")[..], b"myext\0");
        assert_eq!(&prefix_with_nul(b"")[..], b"\0");
    }
}

/// Install the rmgr-table callback this unit owns (`logicalmsg_redo`).
pub fn init_seams() {
    message_seams::logicalmsg_redo::set(logicalmsg_redo);
}
