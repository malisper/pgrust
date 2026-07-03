use crate::{appendf, rec_data, rec_info, Rec, XLR_INFO_MASK};
use stringinfo::StringInfo;
use types_error::PgResult;
use xlogreader_seams::XLogReaderState;

// dbcommands_xlog.h; owning unit (backend-commands-dbcommands) not ported.
pub const XLOG_DBASE_CREATE_FILE_COPY: u8 = 0x00;
pub const XLOG_DBASE_CREATE_WAL_LOG: u8 = 0x10;
pub const XLOG_DBASE_DROP: u8 = 0x20;

pub fn dbase_desc(buf: &mut StringInfo<'_>, record: &XLogReaderState) -> PgResult<()> {
    let rec = Rec(rec_data(record));
    let info = rec_info(record) & !XLR_INFO_MASK;

    if info == XLOG_DBASE_CREATE_FILE_COPY {
        // xl_dbase_create_file_copy_rec: db_id 0, tablespace_id 4,
        // src_db_id 8, src_tablespace_id 12.
        let what = "xl_dbase_create_file_copy_rec";
        appendf!(
            buf,
            "copy dir {}/{} to {}/{}",
            rec.u32(12, what)?,
            rec.u32(8, what)?,
            rec.u32(4, what)?,
            rec.u32(0, what)?
        )?;
    } else if info == XLOG_DBASE_CREATE_WAL_LOG {
        // xl_dbase_create_wal_log_rec: db_id 0, tablespace_id 4.
        let what = "xl_dbase_create_wal_log_rec";
        appendf!(buf, "create dir {}/{}", rec.u32(4, what)?, rec.u32(0, what)?)?;
    } else if info == XLOG_DBASE_DROP {
        // xl_dbase_drop_rec: db_id 0, ntablespaces 4, tablespace_ids[] 8.
        let what = "xl_dbase_drop_rec";
        let db_id = rec.u32(0, what)?;
        let ntablespaces = rec.i32(4, what)?.max(0) as usize;
        buf.append_str("dir")?;
        for i in 0..ntablespaces {
            appendf!(buf, " {}/{}", rec.u32(8 + 4 * i, what)?, db_id)?;
        }
    }
    Ok(())
}

pub fn dbase_identify(info: u8) -> Option<&'static str> {
    match info & !XLR_INFO_MASK {
        XLOG_DBASE_CREATE_FILE_COPY => Some("CREATE_FILE_COPY"),
        XLOG_DBASE_CREATE_WAL_LOG => Some("CREATE_WAL_LOG"),
        XLOG_DBASE_DROP => Some("DROP"),
        _ => None,
    }
}
