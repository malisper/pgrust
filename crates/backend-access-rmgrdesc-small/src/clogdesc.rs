//! `clogdesc.c` — rmgr descriptor routines for `access/transam/clog.c`.

use mcx::PgString;
use types_core::uint8;
use types_error::PgResult;
use types_wal::{xl_clog_truncate, DecodedXLogRecord, XLR_INFO_MASK};

use crate::util::{appendf, read_i64, record_truncated};

/// `CLOG_ZEROPAGE` (access/clog.h).
pub const CLOG_ZEROPAGE: uint8 = 0x00;
/// `CLOG_TRUNCATE` (access/clog.h).
pub const CLOG_TRUNCATE: uint8 = 0x10;

/// `clog_desc`. `CLOG_ZEROPAGE` carries a bare `int64 pageno`;
/// `CLOG_TRUNCATE` carries an [`xl_clog_truncate`] (only `pageno` and
/// `oldestXact` are printed).
pub fn clog_desc(buf: &mut PgString<'_>, record: &DecodedXLogRecord<'_>) -> PgResult<()> {
    let data = record.main_data();
    let info = record.info() & !XLR_INFO_MASK;

    if info == CLOG_ZEROPAGE {
        let pageno = read_i64(data, 0, "clog zeropage pageno")?;
        appendf!(buf, "page {pageno}")?;
    } else if info == CLOG_TRUNCATE {
        let xlrec =
            xl_clog_truncate::from_bytes(data).ok_or_else(|| record_truncated("xl_clog_truncate"))?;
        appendf!(buf, "page {}; oldestXact {}", xlrec.pageno(), xlrec.oldest_xact())?;
    }

    Ok(())
}

/// `clog_identify`.
pub fn clog_identify(info: uint8) -> Option<&'static str> {
    match info & !XLR_INFO_MASK {
        CLOG_ZEROPAGE => Some("ZEROPAGE"),
        CLOG_TRUNCATE => Some("TRUNCATE"),
        _ => None,
    }
}

/// Adapter installed into the rmgr-table `clog_desc` seam: extracts the decoded
/// record from the dispatcher's `XLogReaderState` (C's `record->record`) and
/// renders it. The reader is always positioned on a decoded record when the
/// rmgr table invokes `rm_desc`.
pub fn clog_desc_seam(
    buf: &mut PgString<'_>,
    record: &types_wal::rmgr::XLogReaderState<'_>,
) -> PgResult<()> {
    let record = record
        .record
        .as_ref()
        .expect("clog_desc called without a decoded record");
    clog_desc(buf, record)
}
