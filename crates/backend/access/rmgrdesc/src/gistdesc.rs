use crate::{appendf, rec_data, rec_info, Rec, XLR_INFO_MASK};
use stringinfo::StringInfo;
use types_error::PgResult;
use types_gist::{
    XLOG_GIST_ASSIGN_LSN, XLOG_GIST_DELETE, XLOG_GIST_PAGE_DELETE, XLOG_GIST_PAGE_REUSE,
    XLOG_GIST_PAGE_SPLIT, XLOG_GIST_PAGE_UPDATE,
};
use xlogreader_seams::XLogReaderState;

pub fn gist_desc(buf: &mut StringInfo<'_>, record: &XLogReaderState) -> PgResult<()> {
    let rec = Rec(rec_data(record));
    let info = rec_info(record) & !XLR_INFO_MASK;

    match info {
        XLOG_GIST_PAGE_UPDATE => {}
        XLOG_GIST_PAGE_REUSE => {
            // gistxlogPageReuse: locator 0..12, block 12, snapshotConflictHorizon 16 (u64), isCatalogRel 24.
            let horizon = rec.u64(16, "gistxlogPageReuse")?;
            appendf!(
                buf,
                "rel {}/{}/{}; blk {}; snapshotConflictHorizon {}:{}, isCatalogRel {}",
                rec.u32(0, "gistxlogPageReuse")?,
                rec.u32(4, "gistxlogPageReuse")?,
                rec.u32(8, "gistxlogPageReuse")?,
                rec.u32(12, "gistxlogPageReuse")?,
                (horizon >> 32) as u32,
                horizon as u32,
                if rec.u8(24, "gistxlogPageReuse")? != 0 { 'T' } else { 'F' }
            )?;
        }
        XLOG_GIST_DELETE => {
            // gistxlogDelete: snapshotConflictHorizon 0, ntodelete 4, isCatalogRel 6.
            appendf!(
                buf,
                "delete: snapshotConflictHorizon {}, nitems: {}, isCatalogRel {}",
                rec.u32(0, "gistxlogDelete")?,
                rec.u16(4, "gistxlogDelete")?,
                if rec.u8(6, "gistxlogDelete")? != 0 { 'T' } else { 'F' }
            )?;
        }
        XLOG_GIST_PAGE_SPLIT => {
            // gistxlogPageSplit: origrlink 0, orignsn 8, origleaf 16, npage 18, markfollowright 20.
            appendf!(buf, "page_split: splits to {} pages", rec.u16(18, "gistxlogPageSplit")?)?;
        }
        XLOG_GIST_PAGE_DELETE => {
            // gistxlogPageDelete: deleteXid 0 (FullTransactionId, u64), downlinkOffset 8.
            let delete_xid = rec.u64(0, "gistxlogPageDelete")?;
            appendf!(
                buf,
                "deleteXid {}:{}; downlink {}",
                (delete_xid >> 32) as u32,
                delete_xid as u32,
                rec.u16(8, "gistxlogPageDelete")?
            )?;
        }
        XLOG_GIST_ASSIGN_LSN => {}
        _ => {}
    }
    Ok(())
}

pub fn gist_identify(info: u8) -> Option<&'static str> {
    match info & !XLR_INFO_MASK {
        XLOG_GIST_PAGE_UPDATE => Some("PAGE_UPDATE"),
        XLOG_GIST_DELETE => Some("DELETE"),
        XLOG_GIST_PAGE_REUSE => Some("PAGE_REUSE"),
        XLOG_GIST_PAGE_SPLIT => Some("PAGE_SPLIT"),
        XLOG_GIST_PAGE_DELETE => Some("PAGE_DELETE"),
        XLOG_GIST_ASSIGN_LSN => Some("ASSIGN_LSN"),
        _ => None,
    }
}
