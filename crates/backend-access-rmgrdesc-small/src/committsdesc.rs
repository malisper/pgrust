//! `committsdesc.c` — rmgr descriptor routines for
//! `access/transam/commit_ts.c`.

use mcx::PgString;
use types_core::{uint8, PgResult};
use types_wal::XLR_INFO_MASK;

use crate::util::{appendf, read_i64, read_u32};

/// `COMMIT_TS_ZEROPAGE` (access/commit_ts.h).
pub const COMMIT_TS_ZEROPAGE: uint8 = 0x00;
/// `COMMIT_TS_TRUNCATE` (access/commit_ts.h).
pub const COMMIT_TS_TRUNCATE: uint8 = 0x10;

/// `commit_ts_desc`. Payloads: `COMMIT_TS_ZEROPAGE` carries a bare
/// `int64 pageno`; `COMMIT_TS_TRUNCATE` carries `xl_commit_ts_truncate
/// { int64 pageno; TransactionId oldestXid; }`.
pub fn commit_ts_desc(buf: &mut PgString<'_>, info: uint8, data: &[u8]) -> PgResult<()> {
    let info = info & !XLR_INFO_MASK;

    if info == COMMIT_TS_ZEROPAGE {
        let pageno = read_i64(data, 0, "commit_ts zeropage pageno")?;
        appendf!(buf, "{pageno}")?;
    } else if info == COMMIT_TS_TRUNCATE {
        let pageno = read_i64(data, 0, "xl_commit_ts_truncate.pageno")?;
        let oldest_xid = read_u32(data, 8, "xl_commit_ts_truncate.oldestXid")?;
        appendf!(buf, "pageno {pageno}, oldestXid {oldest_xid}")?;
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
