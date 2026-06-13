//! `committsdesc.c` — rmgr descriptor routines for
//! `access/transam/commit_ts.c`.

use mcx::PgString;
use types_core::uint8;
use types_error::PgResult;
use types_wal::{xl_commit_ts_truncate, DecodedXLogRecord, XLR_INFO_MASK};

use crate::util::{appendf, read_i64, record_truncated};

/// `COMMIT_TS_ZEROPAGE` (access/commit_ts.h).
pub const COMMIT_TS_ZEROPAGE: uint8 = 0x00;
/// `COMMIT_TS_TRUNCATE` (access/commit_ts.h).
pub const COMMIT_TS_TRUNCATE: uint8 = 0x10;

/// `commit_ts_desc`. `COMMIT_TS_ZEROPAGE` carries a bare `int64 pageno`;
/// `COMMIT_TS_TRUNCATE` carries an [`xl_commit_ts_truncate`].
pub fn commit_ts_desc(buf: &mut PgString<'_>, record: &DecodedXLogRecord<'_>) -> PgResult<()> {
    let data = record.main_data();
    let info = record.info() & !XLR_INFO_MASK;

    if info == COMMIT_TS_ZEROPAGE {
        let pageno = read_i64(data, 0, "commit_ts zeropage pageno")?;
        appendf!(buf, "{pageno}")?;
    } else if info == COMMIT_TS_TRUNCATE {
        let trunc = xl_commit_ts_truncate::from_bytes(data)
            .ok_or_else(|| record_truncated("xl_commit_ts_truncate"))?;
        appendf!(buf, "pageno {}, oldestXid {}", trunc.pageno(), trunc.oldest_xid())?;
    }

    Ok(())
}

/// `commit_ts_identify`. NB: unlike its siblings, the C switches on the raw
/// `info` byte without masking `XLR_INFO_MASK`.
pub fn commit_ts_identify(info: uint8) -> Option<&'static str> {
    match info {
        COMMIT_TS_ZEROPAGE => Some("ZEROPAGE"),
        COMMIT_TS_TRUNCATE => Some("TRUNCATE"),
        _ => None,
    }
}

/// Adapter installed into the rmgr-table `commit_ts_desc` seam: extracts the decoded
/// record from the dispatcher's `XLogReaderState` (C's `record->record`) and
/// renders it. The reader is always positioned on a decoded record when the
/// rmgr table invokes `rm_desc`.
pub fn commit_ts_desc_seam(
    buf: &mut PgString<'_>,
    record: &types_wal::rmgr::XLogReaderState<'_>,
) -> PgResult<()> {
    let record = record
        .record
        .as_ref()
        .expect("commit_ts_desc called without a decoded record");
    commit_ts_desc(buf, record)
}
