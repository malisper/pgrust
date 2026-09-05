//! message.c: LogLogicalMessage + the LOGICALMSG WAL record. Decode-side
//! consumption (decode.c/logical decoding output) is out of scope here —
//! only emit + redo are ported.
//!
//! xl_logical_message layout (byte-identical to C, must not drift):
//! dbId Oid @0 (4B), transactional bool @4 (1B, 3B pad), prefix_size Size
//! @8 (8B), message_size Size @16 (8B), payload @24: prefix bytes then a
//! trailing NUL then message bytes.

#![allow(non_snake_case)]

use types_core::{Oid, XLogRecPtr};
use types_error::{PgError, PgResult, PANIC};
use xlogreader_seams::XLogReaderState;

pub const XLOG_LOGICAL_MESSAGE: u8 = 0x00;
const RM_LOGICALMSG_ID: u8 = types_core::RmgrIds::RM_LOGICALMSG_ID as u8;
const SIZE_OF_LOGICAL_MESSAGE: usize = 24;
const XLR_INFO_MASK: u8 = 0x0F;

/// `LogLogicalMessage` (message.c). `prefix`/`message` are the raw
/// VARDATA_ANY payloads of the SQL-level arguments; the caller is
/// responsible for C's `strlen`-on-a-cstring truncation semantics for an
/// embedded NUL in prefix (this fn trusts `prefix` to already be that
/// truncated slice).
pub fn LogLogicalMessage(
    prefix: &[u8],
    message: &[u8],
    transactional: bool,
    flush: bool,
) -> PgResult<XLogRecPtr> {
    if transactional {
        debug_assert!(xact::IsTransactionState());
        xact::GetCurrentTransactionId()?;
    }

    let db_id: Oid = init_small::globals::MyDatabaseId();
    let prefix_size: u64 = prefix.len() as u64 + 1;
    let message_size: u64 = message.len() as u64;

    let mut hdr = [0u8; SIZE_OF_LOGICAL_MESSAGE];
    hdr[0..4].copy_from_slice(&db_id.to_ne_bytes());
    hdr[4] = transactional as u8;
    hdr[8..16].copy_from_slice(&prefix_size.to_ne_bytes());
    hdr[16..24].copy_from_slice(&message_size.to_ne_bytes());

    let nul = [0u8];
    let lsn = xloginsert::insert_record(
        RM_LOGICALMSG_ID,
        XLOG_LOGICAL_MESSAGE,
        transam_xlog::XLOG_INCLUDE_ORIGIN,
        &[&hdr, prefix, &nul, message],
        &[],
    )?;

    if !transactional && flush {
        transam_xlog::XLogFlush(lsn)?;
    }
    Ok(lsn)
}

/// `logicalmsg_redo` (message.c): a no-op for WAL replay — the record only
/// matters to logical decoding (decode.c), which reads it directly off the
/// WAL stream rather than through redo. An unknown opcode is
/// elog(PANIC, ...) (message.c:92), a structured PANIC-level error.
pub fn logicalmsg_redo(record: &mut XLogReaderState) -> PgResult<()> {
    let rec = record.record.as_ref().expect("logicalmsg_redo with no decoded record");
    let info = rec.xl_info & !XLR_INFO_MASK;
    if info != XLOG_LOGICAL_MESSAGE {
        return Err(Box::new(PgError::new(
            PANIC,
            format!("logicalmsg_redo: unknown op code {info}"),
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // message.c:92: an unknown opcode in a LOGICALMSG record is
    // elog(PANIC, "logicalmsg_redo: unknown op code %u") — a structured
    // PANIC-level error, never an unwinding Rust panic (row
    // a186-candidate-fp-logical-b1-173132cfe5ffd02992b8-1).
    #[test]
    fn redo_unknown_opcode_is_a_panic_level_error_not_a_rust_panic() {
        let mut rec = xlogreader_seams::DecodedXLogRecord::default();
        rec.xl_info = 0x10;
        let mut record = XLogReaderState {
            record: Some(rec),
            ..Default::default()
        };
        let err = logicalmsg_redo(&mut record).expect_err("unknown opcode must not redo silently");
        assert_eq!(err.level(), types_error::PANIC);
        assert_eq!(err.message(), "logicalmsg_redo: unknown op code 16");

        // The one valid opcode redoes as a no-op.
        let mut rec = xlogreader_seams::DecodedXLogRecord::default();
        rec.xl_info = XLOG_LOGICAL_MESSAGE;
        let mut record = XLogReaderState {
            record: Some(rec),
            ..Default::default()
        };
        logicalmsg_redo(&mut record).unwrap();
    }
}
