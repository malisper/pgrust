use crate::{appendf, rec_data, rec_info, Rec, XLR_INFO_MASK};
use stringinfo::StringInfo;
use types_error::PgResult;
use xlogreader_seams::XLogReaderState;

// commands/tablespace.h; owning unit (backend-commands-tablespace) not ported.
pub const XLOG_TBLSPC_CREATE: u8 = 0x00;
pub const XLOG_TBLSPC_DROP: u8 = 0x10;

pub fn tblspc_desc(buf: &mut StringInfo<'_>, record: &XLogReaderState) -> PgResult<()> {
    let rec = Rec(rec_data(record));
    let info = rec_info(record) & !XLR_INFO_MASK;

    if info == XLOG_TBLSPC_CREATE {
        // xl_tblspc_create_rec: ts_id 0, ts_path (NUL-terminated) 4.
        let ts_id = rec.u32(0, "xl_tblspc_create_rec")?;
        let path = rec.0.get(4..).unwrap_or(&[]);
        let path = &path[..path.iter().position(|&b| b == 0).unwrap_or(path.len())];
        appendf!(buf, "{ts_id} \"")?;
        buf.append_bytes(path)?;
        buf.append_byte(b'"')?;
    } else if info == XLOG_TBLSPC_DROP {
        appendf!(buf, "{}", rec.u32(0, "xl_tblspc_drop_rec")?)?;
    }
    Ok(())
}

pub fn tblspc_identify(info: u8) -> Option<&'static str> {
    match info & !XLR_INFO_MASK {
        XLOG_TBLSPC_CREATE => Some("CREATE"),
        XLOG_TBLSPC_DROP => Some("DROP"),
        _ => None,
    }
}
