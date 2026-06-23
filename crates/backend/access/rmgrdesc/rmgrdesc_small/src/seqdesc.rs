//! `seqdesc.c` — rmgr descriptor routines for `commands/sequence.c`.

use ::mcx::PgString;
use ::types_core::uint8;
use ::types_error::PgResult;
use ::wal::{xl_seq_rec, DecodedXLogRecord, XLR_INFO_MASK};

use crate::util::{appendf, record_truncated};

/// `XLOG_SEQ_LOG` (commands/sequence.h).
pub const XLOG_SEQ_LOG: uint8 = 0x00;

/// `seq_desc`. The payload is an [`xl_seq_rec`]; only its `RelFileLocator`
/// is printed.
pub fn seq_desc(buf: &mut PgString<'_>, record: &DecodedXLogRecord<'_>) -> PgResult<()> {
    let data = record.main_data();
    let info = record.info() & !XLR_INFO_MASK;

    if info == XLOG_SEQ_LOG {
        let xlrec = xl_seq_rec::from_bytes(data).ok_or_else(|| record_truncated("xl_seq_rec"))?;
        let locator = xlrec.locator();
        appendf!(
            buf,
            "rel {}/{}/{}",
            locator.spc_oid(),
            locator.db_oid(),
            locator.rel_number()
        )?;
    }

    Ok(())
}

/// `seq_identify`.
pub fn seq_identify(info: uint8) -> Option<&'static str> {
    match info & !XLR_INFO_MASK {
        XLOG_SEQ_LOG => Some("LOG"),
        _ => None,
    }
}

/// Adapter installed into the rmgr-table `seq_desc` seam: extracts the decoded
/// record from the dispatcher's `XLogReaderState` (C's `record->record`) and
/// renders it. The reader is always positioned on a decoded record when the
/// rmgr table invokes `rm_desc`.
pub fn seq_desc_seam(
    buf: &mut PgString<'_>,
    record: &::wal::rmgr::XLogReaderState<'_>,
) -> PgResult<()> {
    let record = record
        .record
        .as_ref()
        .expect("seq_desc called without a decoded record");
    seq_desc(buf, record)
}
