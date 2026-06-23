//! `dbasedesc.c` — rmgr descriptor routines for `commands/dbcommands.c`.

use ::mcx::PgString;
use ::types_core::uint8;
use ::types_error::PgResult;
use wal::{
    xl_dbase_create_file_copy_rec, xl_dbase_create_wal_log_rec, xl_dbase_drop_rec,
    DecodedXLogRecord, XLR_INFO_MASK,
};

use crate::util::{appendf, record_truncated};

/// `XLOG_DBASE_CREATE_FILE_COPY` (commands/dbcommands_xlog.h).
pub const XLOG_DBASE_CREATE_FILE_COPY: uint8 = 0x00;
/// `XLOG_DBASE_CREATE_WAL_LOG` (commands/dbcommands_xlog.h).
pub const XLOG_DBASE_CREATE_WAL_LOG: uint8 = 0x10;
/// `XLOG_DBASE_DROP` (commands/dbcommands_xlog.h).
pub const XLOG_DBASE_DROP: uint8 = 0x20;

/// `dbase_desc`. Payloads are [`xl_dbase_create_file_copy_rec`],
/// [`xl_dbase_create_wal_log_rec`], and [`xl_dbase_drop_rec`].
pub fn dbase_desc(buf: &mut PgString<'_>, record: &DecodedXLogRecord<'_>) -> PgResult<()> {
    let data = record.main_data();
    let info = record.info() & !XLR_INFO_MASK;

    if info == XLOG_DBASE_CREATE_FILE_COPY {
        let xlrec = xl_dbase_create_file_copy_rec::from_bytes(data)
            .ok_or_else(|| record_truncated("xl_dbase_create_file_copy_rec"))?;
        appendf!(
            buf,
            "copy dir {}/{} to {}/{}",
            xlrec.src_tablespace_id(),
            xlrec.src_db_id(),
            xlrec.tablespace_id(),
            xlrec.db_id()
        )?;
    } else if info == XLOG_DBASE_CREATE_WAL_LOG {
        let xlrec = xl_dbase_create_wal_log_rec::from_bytes(data)
            .ok_or_else(|| record_truncated("xl_dbase_create_wal_log_rec"))?;
        appendf!(buf, "create dir {}/{}", xlrec.tablespace_id(), xlrec.db_id())?;
    } else if info == XLOG_DBASE_DROP {
        let xlrec = xl_dbase_drop_rec::from_bytes(data)
            .ok_or_else(|| record_truncated("xl_dbase_drop_rec"))?;
        buf.try_push_str("dir")?;
        for ts in xlrec.tablespace_ids() {
            appendf!(buf, " {ts}/{}", xlrec.db_id())?;
        }
    }

    Ok(())
}

/// `dbase_identify`.
pub fn dbase_identify(info: uint8) -> Option<&'static str> {
    match info & !XLR_INFO_MASK {
        XLOG_DBASE_CREATE_FILE_COPY => Some("CREATE_FILE_COPY"),
        XLOG_DBASE_CREATE_WAL_LOG => Some("CREATE_WAL_LOG"),
        XLOG_DBASE_DROP => Some("DROP"),
        _ => None,
    }
}

/// Adapter installed into the rmgr-table `dbase_desc` seam: extracts the decoded
/// record from the dispatcher's `XLogReaderState` (C's `record->record`) and
/// renders it. The reader is always positioned on a decoded record when the
/// rmgr table invokes `rm_desc`.
pub fn dbase_desc_seam(
    buf: &mut PgString<'_>,
    record: &::wal::rmgr::XLogReaderState<'_>,
) -> PgResult<()> {
    let record = record
        .record
        .as_ref()
        .expect("dbase_desc called without a decoded record");
    dbase_desc(buf, record)
}
