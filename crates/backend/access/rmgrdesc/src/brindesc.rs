use crate::{appendf, rec_data, rec_info, Rec, XLR_INFO_MASK};
use stringinfo::StringInfo;
use types_brin::{
    XLOG_BRIN_CREATE_INDEX, XLOG_BRIN_DESUMMARIZE, XLOG_BRIN_INIT_PAGE, XLOG_BRIN_INSERT,
    XLOG_BRIN_OPMASK, XLOG_BRIN_REVMAP_EXTEND, XLOG_BRIN_SAMEPAGE_UPDATE, XLOG_BRIN_UPDATE,
};
use types_error::PgResult;
use xlogreader_seams::XLogReaderState;

pub fn brin_desc(buf: &mut StringInfo<'_>, record: &XLogReaderState) -> PgResult<()> {
    let rec = Rec(rec_data(record));
    let info = (rec_info(record) & !XLR_INFO_MASK) & XLOG_BRIN_OPMASK;

    match info {
        XLOG_BRIN_CREATE_INDEX => {
            appendf!(
                buf,
                "v{} pagesPerRange {}",
                rec.u16(4, "xl_brin_createidx")?,
                rec.u32(0, "xl_brin_createidx")?
            )?;
        }
        XLOG_BRIN_INSERT => {
            appendf!(
                buf,
                "heapBlk {} pagesPerRange {} offnum {}",
                rec.u32(0, "xl_brin_insert")?,
                rec.u32(4, "xl_brin_insert")?,
                rec.u16(8, "xl_brin_insert")?
            )?;
        }
        XLOG_BRIN_UPDATE => {
            // oldOffnum 0; embedded xl_brin_insert at 4.
            appendf!(
                buf,
                "heapBlk {} pagesPerRange {} old offnum {}, new offnum {}",
                rec.u32(4, "xl_brin_update")?,
                rec.u32(8, "xl_brin_update")?,
                rec.u16(0, "xl_brin_update")?,
                rec.u16(12, "xl_brin_update")?
            )?;
        }
        XLOG_BRIN_SAMEPAGE_UPDATE => {
            appendf!(buf, "offnum {}", rec.u16(0, "xl_brin_samepage_update")?)?;
        }
        XLOG_BRIN_REVMAP_EXTEND => {
            appendf!(buf, "targetBlk {}", rec.u32(0, "xl_brin_revmap_extend")?)?;
        }
        XLOG_BRIN_DESUMMARIZE => {
            appendf!(
                buf,
                "pagesPerRange {}, heapBlk {}, page offset {}",
                rec.u32(0, "xl_brin_desummarize")?,
                rec.u32(4, "xl_brin_desummarize")?,
                rec.u16(8, "xl_brin_desummarize")?
            )?;
        }
        _ => {}
    }
    Ok(())
}

const INSERT_INIT: u8 = XLOG_BRIN_INSERT | XLOG_BRIN_INIT_PAGE;
const UPDATE_INIT: u8 = XLOG_BRIN_UPDATE | XLOG_BRIN_INIT_PAGE;

pub fn brin_identify(info: u8) -> Option<&'static str> {
    match info & !XLR_INFO_MASK {
        XLOG_BRIN_CREATE_INDEX => Some("CREATE_INDEX"),
        XLOG_BRIN_INSERT => Some("INSERT"),
        INSERT_INIT => Some("INSERT+INIT"),
        XLOG_BRIN_UPDATE => Some("UPDATE"),
        UPDATE_INIT => Some("UPDATE+INIT"),
        XLOG_BRIN_SAMEPAGE_UPDATE => Some("SAMEPAGE_UPDATE"),
        XLOG_BRIN_REVMAP_EXTEND => Some("REVMAP_EXTEND"),
        XLOG_BRIN_DESUMMARIZE => Some("DESUMMARIZE"),
        _ => None,
    }
}
