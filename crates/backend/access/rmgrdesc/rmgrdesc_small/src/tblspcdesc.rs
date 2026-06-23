//! `tblspcdesc.c` — rmgr descriptor routines for `commands/tablespace.c`.

use ::mcx::PgString;
use ::types_core::uint8;
use ::types_error::PgResult;
use ::wal::{xl_tblspc_create_rec, xl_tblspc_drop_rec, DecodedXLogRecord, XLR_INFO_MASK};

use crate::util::{append_lossy, appendf, record_truncated};

/// `XLOG_TBLSPC_CREATE` (commands/tablespace.h).
pub const XLOG_TBLSPC_CREATE: uint8 = 0x00;
/// `XLOG_TBLSPC_DROP` (commands/tablespace.h).
pub const XLOG_TBLSPC_DROP: uint8 = 0x10;

/// `tblspc_desc`. Payloads are [`xl_tblspc_create_rec`] (NUL-terminated
/// path) and [`xl_tblspc_drop_rec`].
pub fn tblspc_desc(buf: &mut PgString<'_>, record: &DecodedXLogRecord<'_>) -> PgResult<()> {
    let data = record.main_data();
    let info = record.info() & !XLR_INFO_MASK;

    if info == XLOG_TBLSPC_CREATE {
        let xlrec = xl_tblspc_create_rec::from_bytes(data)
            .ok_or_else(|| record_truncated("xl_tblspc_create_rec"))?;
        appendf!(buf, "{} \"", xlrec.ts_id())?;
        append_lossy(buf, xlrec.ts_path())?;
        buf.try_push('"')?;
    } else if info == XLOG_TBLSPC_DROP {
        let xlrec = xl_tblspc_drop_rec::from_bytes(data)
            .ok_or_else(|| record_truncated("xl_tblspc_drop_rec"))?;
        appendf!(buf, "{}", xlrec.ts_id())?;
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

/// Adapter installed into the rmgr-table `tblspc_desc` seam: extracts the decoded
/// record from the dispatcher's `XLogReaderState` (C's `record->record`) and
/// renders it. The reader is always positioned on a decoded record when the
/// rmgr table invokes `rm_desc`.
pub fn tblspc_desc_seam(
    buf: &mut PgString<'_>,
    record: &::wal::rmgr::XLogReaderState<'_>,
) -> PgResult<()> {
    let record = record
        .record
        .as_ref()
        .expect("tblspc_desc called without a decoded record");
    tblspc_desc(buf, record)
}
