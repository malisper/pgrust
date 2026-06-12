//! `seqdesc.c` — rmgr descriptor routines for `commands/sequence.c`.

use mcx::PgString;
use types_core::{uint8, PgResult};
use types_wal::XLR_INFO_MASK;

use crate::util::{appendf, read_u32};

/// `XLOG_SEQ_LOG` (commands/sequence.h).
pub const XLOG_SEQ_LOG: uint8 = 0x00;

/// `seq_desc`. Payload: `xl_seq_rec { RelFileLocator locator; ... }` with
/// `RelFileLocator { Oid spcOid; Oid dbOid; RelFileNumber relNumber; }`.
pub fn seq_desc(buf: &mut PgString<'_>, info: uint8, data: &[u8]) -> PgResult<()> {
    let info = info & !XLR_INFO_MASK;

    if info == XLOG_SEQ_LOG {
        let spc_oid = read_u32(data, 0, "xl_seq_rec.locator.spcOid")?;
        let db_oid = read_u32(data, 4, "xl_seq_rec.locator.dbOid")?;
        let rel_number = read_u32(data, 8, "xl_seq_rec.locator.relNumber")?;
        appendf!(buf, "rel {spc_oid}/{db_oid}/{rel_number}")?;
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
