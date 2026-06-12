//! `clogdesc.c` — rmgr descriptor routines for `access/transam/clog.c`.

use mcx::PgString;
use types_core::{uint8, PgResult};
use types_wal::XLR_INFO_MASK;

use crate::util::{appendf, read_i64, read_u32};

/// `CLOG_ZEROPAGE` (access/clog.h).
pub const CLOG_ZEROPAGE: uint8 = 0x00;
/// `CLOG_TRUNCATE` (access/clog.h).
pub const CLOG_TRUNCATE: uint8 = 0x10;

/// `clog_desc`. Payloads: `CLOG_ZEROPAGE` carries a bare `int64 pageno`;
/// `CLOG_TRUNCATE` carries `xl_clog_truncate { int64 pageno;
/// TransactionId oldestXact; Oid oldestXactDb; }` (only the first two fields
/// are printed).
pub fn clog_desc(buf: &mut PgString<'_>, info: uint8, data: &[u8]) -> PgResult<()> {
    let info = info & !XLR_INFO_MASK;

    if info == CLOG_ZEROPAGE {
        let pageno = read_i64(data, 0, "clog zeropage pageno")?;
        appendf!(buf, "page {pageno}")?;
    } else if info == CLOG_TRUNCATE {
        let pageno = read_i64(data, 0, "xl_clog_truncate.pageno")?;
        let oldest_xact = read_u32(data, 8, "xl_clog_truncate.oldestXact")?;
        appendf!(buf, "page {pageno}; oldestXact {oldest_xact}")?;
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
