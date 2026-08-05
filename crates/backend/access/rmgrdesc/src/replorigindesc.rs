use crate::{appendf, rec_data, rec_info, Rec, XLR_INFO_MASK};
use stringinfo::StringInfo;
use types_error::PgResult;
use xlogreader_seams::XLogReaderState;

const XLOG_REPLORIGIN_SET: u8 = 0x00;
const XLOG_REPLORIGIN_DROP: u8 = 0x10;

pub fn replorigin_desc(buf: &mut StringInfo<'_>, record: &XLogReaderState) -> PgResult<()> {
    let rec = Rec(rec_data(record));
    let info = rec_info(record) & !XLR_INFO_MASK;

    match info {
        XLOG_REPLORIGIN_SET => {
            // xl_replorigin_set: remote_lsn 0 (u64), node_id 8, force 10.
            let remote_lsn = rec.u64(0, "xl_replorigin_set")?;
            appendf!(
                buf,
                "set {}; lsn {:X}/{:X}; force: {}",
                rec.u16(8, "xl_replorigin_set")?,
                (remote_lsn >> 32) as u32,
                remote_lsn as u32,
                rec.u8(10, "xl_replorigin_set")?
            )?;
        }
        XLOG_REPLORIGIN_DROP => {
            appendf!(buf, "drop {}", rec.u16(0, "xl_replorigin_drop")?)?;
        }
        _ => {}
    }
    Ok(())
}

// C divergence from every other *_identify: replorigin_identify switches on
// the raw info byte, not `info & ~XLR_INFO_MASK` (replorigindesc.c verbatim).
pub fn replorigin_identify(info: u8) -> Option<&'static str> {
    match info {
        XLOG_REPLORIGIN_SET => Some("SET"),
        XLOG_REPLORIGIN_DROP => Some("DROP"),
        _ => None,
    }
}
