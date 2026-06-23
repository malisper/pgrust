//! `access/rmgrdesc/gistdesc.c` — rmgr descriptor routines for GiST indexes.

use crate::appendf;
use mcx::PgString;
use types_error::PgResult;
use wal::{DecodedXLogRecord, XLR_INFO_MASK};
use xlog_records::gistxlog::{gistxlogDelete, gistxlogPageDelete, gistxlogPageReuse,
                                   gistxlogPageSplit};

// access/gistxlog.h
pub const XLOG_GIST_PAGE_UPDATE: u8 = 0x00;
pub const XLOG_GIST_DELETE: u8 = 0x10;
pub const XLOG_GIST_PAGE_REUSE: u8 = 0x20;
pub const XLOG_GIST_PAGE_SPLIT: u8 = 0x30;
pub const XLOG_GIST_PAGE_DELETE: u8 = 0x60;
pub const XLOG_GIST_ASSIGN_LSN: u8 = 0x70;

/// `out_gistxlogPageUpdate` — intentionally empty in C.
fn out_gistxlog_page_update(_buf: &mut PgString<'_>, _rec: &[u8]) -> PgResult<()> {
    Ok(())
}

/// `out_gistxlogPageReuse`.
fn out_gistxlog_page_reuse(buf: &mut PgString<'_>, rec: &[u8]) -> PgResult<()> {
    let xlrec = gistxlogPageReuse::from_bytes(rec);
    appendf!(
        buf,
        "rel {}/{}/{}; blk {}; snapshotConflictHorizon {}:{}, isCatalogRel {}",
        xlrec.locator.spcOid,
        xlrec.locator.dbOid,
        xlrec.locator.relNumber,
        xlrec.block,
        xlrec.snapshotConflictHorizon.epoch(),
        xlrec.snapshotConflictHorizon.xid(),
        if xlrec.isCatalogRel { 'T' } else { 'F' }
    );
    Ok(())
}

/// `out_gistxlogDelete`.
fn out_gistxlog_delete(buf: &mut PgString<'_>, rec: &[u8]) -> PgResult<()> {
    let xlrec = gistxlogDelete::from_bytes(rec);
    appendf!(
        buf,
        "delete: snapshotConflictHorizon {}, nitems: {}, isCatalogRel {}",
        xlrec.snapshotConflictHorizon,
        xlrec.ntodelete,
        if xlrec.isCatalogRel { 'T' } else { 'F' }
    );
    Ok(())
}

/// `out_gistxlogPageSplit`.
fn out_gistxlog_page_split(buf: &mut PgString<'_>, rec: &[u8]) -> PgResult<()> {
    let xlrec = gistxlogPageSplit::from_bytes(rec);
    appendf!(buf, "page_split: splits to {} pages", xlrec.npage);
    Ok(())
}

/// `out_gistxlogPageDelete`.
fn out_gistxlog_page_delete(buf: &mut PgString<'_>, rec: &[u8]) -> PgResult<()> {
    let xlrec = gistxlogPageDelete::from_bytes(rec);
    appendf!(
        buf,
        "deleteXid {}:{}; downlink {}",
        xlrec.deleteXid.epoch(),
        xlrec.deleteXid.xid(),
        xlrec.downlinkOffset
    );
    Ok(())
}

/// `gist_desc(StringInfo buf, XLogReaderState *record)`.
pub fn gist_desc(buf: &mut PgString<'_>, record: &DecodedXLogRecord<'_>) -> PgResult<()> {
    let rec = record.data();
    let info = record.info() & !XLR_INFO_MASK;

    match info {
        XLOG_GIST_PAGE_UPDATE => out_gistxlog_page_update(buf, rec)?,
        XLOG_GIST_PAGE_REUSE => out_gistxlog_page_reuse(buf, rec)?,
        XLOG_GIST_DELETE => out_gistxlog_delete(buf, rec)?,
        XLOG_GIST_PAGE_SPLIT => out_gistxlog_page_split(buf, rec)?,
        XLOG_GIST_PAGE_DELETE => out_gistxlog_page_delete(buf, rec)?,
        XLOG_GIST_ASSIGN_LSN => { /* No details to write out */ }
        _ => {}
    }
    Ok(())
}

/// `gist_identify(uint8 info)` — `None` where C returns NULL.
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::record;
    use mcx::MemoryContext;

    fn desc(info: u8, data: &[u8]) -> String {
        let ctx = MemoryContext::new("test");
        let mut buf = PgString::new_in(ctx.mcx());
        let record = record(ctx.mcx(), info, data, &[]);
        gist_desc(&mut buf, &record).unwrap();
        buf.as_str().to_string()
    }

    #[test]
    fn formats_page_reuse() {
        let mut rec = vec![0u8; 25];
        rec[0..4].copy_from_slice(&1u32.to_ne_bytes());
        rec[4..8].copy_from_slice(&2u32.to_ne_bytes());
        rec[8..12].copy_from_slice(&3u32.to_ne_bytes());
        rec[12..16].copy_from_slice(&7u32.to_ne_bytes());
        rec[16..24].copy_from_slice(&((5u64 << 32) | 42).to_ne_bytes());
        rec[24] = 1;
        assert_eq!(
            desc(XLOG_GIST_PAGE_REUSE, &rec),
            "rel 1/2/3; blk 7; snapshotConflictHorizon 5:42, isCatalogRel T"
        );
    }

    #[test]
    fn formats_page_delete_and_split() {
        let mut rec = vec![0u8; 10];
        rec[0..8].copy_from_slice(&((1u64 << 32) | 9).to_ne_bytes());
        rec[8..10].copy_from_slice(&4u16.to_ne_bytes());
        assert_eq!(desc(XLOG_GIST_PAGE_DELETE, &rec), "deleteXid 1:9; downlink 4");

        let mut rec = vec![0u8; 21];
        rec[18..20].copy_from_slice(&3u16.to_ne_bytes());
        assert_eq!(desc(XLOG_GIST_PAGE_SPLIT, &rec), "page_split: splits to 3 pages");

        assert_eq!(desc(XLOG_GIST_PAGE_UPDATE, &[]), "");
        assert_eq!(desc(XLOG_GIST_ASSIGN_LSN, &[]), "");
    }

    #[test]
    fn identifies() {
        assert_eq!(gist_identify(XLOG_GIST_PAGE_UPDATE), Some("PAGE_UPDATE"));
        assert_eq!(gist_identify(XLOG_GIST_ASSIGN_LSN), Some("ASSIGN_LSN"));
        assert_eq!(gist_identify(0x40), None);
    }
}
