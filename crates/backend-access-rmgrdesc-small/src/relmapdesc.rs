//! `relmapdesc.c` — rmgr descriptor routines for `utils/cache/relmapper.c`.

use mcx::PgString;
use types_core::uint8;
use types_error::PgResult;
use types_wal::{xl_relmap_update, DecodedXLogRecord, XLR_INFO_MASK};

use crate::util::{appendf, record_truncated};

/// `XLOG_RELMAP_UPDATE` (utils/relmapper.h).
pub const XLOG_RELMAP_UPDATE: uint8 = 0x00;

/// `relmap_desc`. The payload is an [`xl_relmap_update`] (the map image that
/// follows the header is not printed).
pub fn relmap_desc(buf: &mut PgString<'_>, record: &DecodedXLogRecord<'_>) -> PgResult<()> {
    let data = record.main_data();
    let info = record.info() & !XLR_INFO_MASK;

    if info == XLOG_RELMAP_UPDATE {
        let xlrec = xl_relmap_update::from_bytes(data)
            .ok_or_else(|| record_truncated("xl_relmap_update"))?;
        appendf!(
            buf,
            "database {} tablespace {} size {}",
            xlrec.dbid(),
            xlrec.tsid(),
            xlrec.nbytes()
        )?;
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

/// Adapter installed into the rmgr-table `relmap_desc` seam: extracts the decoded
/// record from the dispatcher's `XLogReaderState` (C's `record->record`) and
/// renders it. The reader is always positioned on a decoded record when the
/// rmgr table invokes `rm_desc`.
pub fn relmap_desc_seam(
    buf: &mut PgString<'_>,
    record: &types_wal::rmgr::XLogReaderState<'_>,
) -> PgResult<()> {
    let record = record
        .record
        .as_ref()
        .expect("relmap_desc called without a decoded record");
    relmap_desc(buf, record)
}
