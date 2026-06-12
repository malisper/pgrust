//! `tblspcdesc.c` — rmgr descriptor routines for `commands/tablespace.c`.

use mcx::PgString;
use types_core::{uint8, PgResult};
use types_wal::XLR_INFO_MASK;

use crate::util::{appendf, read_u32, record_truncated};

/// `XLOG_TBLSPC_CREATE` (commands/tablespace.h).
pub const XLOG_TBLSPC_CREATE: uint8 = 0x00;
/// `XLOG_TBLSPC_DROP` (commands/tablespace.h).
pub const XLOG_TBLSPC_DROP: uint8 = 0x10;

/// `tblspc_desc`. Payloads: `xl_tblspc_create_rec { Oid ts_id;
/// char ts_path[]; }` (NUL-terminated path) and `xl_tblspc_drop_rec
/// { Oid ts_id; }`.
pub fn tblspc_desc(buf: &mut PgString<'_>, info: uint8, data: &[u8]) -> PgResult<()> {
    let info = info & !XLR_INFO_MASK;

    if info == XLOG_TBLSPC_CREATE {
        let ts_id = read_u32(data, 0, "xl_tblspc_create_rec.ts_id")?;
        // %s on the flexible char array: bytes from offset 4 up to the NUL.
        let path_bytes = data
            .get(4..)
            .ok_or_else(|| record_truncated("xl_tblspc_create_rec.ts_path"))?;
        let nul = path_bytes
            .iter()
            .position(|&b| b == 0)
            .ok_or_else(|| record_truncated("xl_tblspc_create_rec.ts_path NUL"))?;
        let ts_path = String::from_utf8_lossy(&path_bytes[..nul]);
        appendf!(buf, "{ts_id} \"{ts_path}\"")?;
    } else if info == XLOG_TBLSPC_DROP {
        let ts_id = read_u32(data, 0, "xl_tblspc_drop_rec.ts_id")?;
        appendf!(buf, "{ts_id}")?;
    }

    Ok(())
}

/// `tblspc_identify`.
pub fn tblspc_identify(info: uint8) -> Option<&'static str> {
    match info & !XLR_INFO_MASK {
        XLOG_TBLSPC_CREATE => Some("CREATE"),
        XLOG_TBLSPC_DROP => Some("DROP"),
        _ => None,
    }
}
