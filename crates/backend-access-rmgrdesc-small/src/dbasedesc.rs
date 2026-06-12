//! `dbasedesc.c` — rmgr descriptor routines for `commands/dbcommands.c`.

use mcx::PgString;
use types_core::{uint8, PgResult};
use types_wal::XLR_INFO_MASK;

use crate::util::{appendf, read_i32, read_u32};

/// `XLOG_DBASE_CREATE_FILE_COPY` (commands/dbcommands_xlog.h).
pub const XLOG_DBASE_CREATE_FILE_COPY: uint8 = 0x00;
/// `XLOG_DBASE_CREATE_WAL_LOG` (commands/dbcommands_xlog.h).
pub const XLOG_DBASE_CREATE_WAL_LOG: uint8 = 0x10;
/// `XLOG_DBASE_DROP` (commands/dbcommands_xlog.h).
pub const XLOG_DBASE_DROP: uint8 = 0x20;

/// `dbase_desc`. Payload layouts (commands/dbcommands_xlog.h):
/// - `xl_dbase_create_file_copy_rec { Oid db_id; Oid tablespace_id;
///   Oid src_db_id; Oid src_tablespace_id; }`
/// - `xl_dbase_create_wal_log_rec { Oid db_id; Oid tablespace_id; }`
/// - `xl_dbase_drop_rec { Oid db_id; int ntablespaces; Oid tablespace_ids[]; }`
pub fn dbase_desc(buf: &mut PgString<'_>, info: uint8, data: &[u8]) -> PgResult<()> {
    let info = info & !XLR_INFO_MASK;

    if info == XLOG_DBASE_CREATE_FILE_COPY {
        let db_id = read_u32(data, 0, "xl_dbase_create_file_copy_rec.db_id")?;
        let tablespace_id = read_u32(data, 4, "xl_dbase_create_file_copy_rec.tablespace_id")?;
        let src_db_id = read_u32(data, 8, "xl_dbase_create_file_copy_rec.src_db_id")?;
        let src_tablespace_id =
            read_u32(data, 12, "xl_dbase_create_file_copy_rec.src_tablespace_id")?;
        appendf!(
            buf,
            "copy dir {src_tablespace_id}/{src_db_id} to {tablespace_id}/{db_id}"
        )?;
    } else if info == XLOG_DBASE_CREATE_WAL_LOG {
        let db_id = read_u32(data, 0, "xl_dbase_create_wal_log_rec.db_id")?;
        let tablespace_id = read_u32(data, 4, "xl_dbase_create_wal_log_rec.tablespace_id")?;
        appendf!(buf, "create dir {tablespace_id}/{db_id}")?;
    } else if info == XLOG_DBASE_DROP {
        let db_id = read_u32(data, 0, "xl_dbase_drop_rec.db_id")?;
        let ntablespaces = read_i32(data, 4, "xl_dbase_drop_rec.ntablespaces")?;
        buf.try_push_str("dir")?;
        for i in 0..ntablespaces {
            let ts = read_u32(
                data,
                8 + (i as usize) * 4,
                "xl_dbase_drop_rec.tablespace_ids[i]",
            )?;
            appendf!(buf, " {ts}/{db_id}")?;
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
