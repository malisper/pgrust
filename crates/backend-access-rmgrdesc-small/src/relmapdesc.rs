//! `relmapdesc.c` — rmgr descriptor routines for `utils/cache/relmapper.c`.

use mcx::PgString;
use types_core::{uint8, PgResult};
use types_wal::XLR_INFO_MASK;

use crate::util::{appendf, read_i32, read_u32};

/// `XLOG_RELMAP_UPDATE` (utils/relmapper.h).
pub const XLOG_RELMAP_UPDATE: uint8 = 0x00;

/// `relmap_desc`. Payload: `xl_relmap_update { Oid dbid; Oid tsid;
/// int32 nbytes; char data[]; }` (the map image is not printed).
pub fn relmap_desc(buf: &mut PgString<'_>, info: uint8, data: &[u8]) -> PgResult<()> {
    let info = info & !XLR_INFO_MASK;

    if info == XLOG_RELMAP_UPDATE {
        let dbid = read_u32(data, 0, "xl_relmap_update.dbid")?;
        let tsid = read_u32(data, 4, "xl_relmap_update.tsid")?;
        let nbytes = read_i32(data, 8, "xl_relmap_update.nbytes")?;
        appendf!(buf, "database {dbid} tablespace {tsid} size {nbytes}")?;
    }

    Ok(())
}

/// `relmap_identify`.
pub fn relmap_identify(info: uint8) -> Option<&'static str> {
    match info & !XLR_INFO_MASK {
        XLOG_RELMAP_UPDATE => Some("UPDATE"),
        _ => None,
    }
}
