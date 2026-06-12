//! `access/rmgrdesc/gindesc.c` — rmgr descriptor routines for GIN indexes.

use crate::{appendf, block_id_at, bool_at, i32_at, u16_at, u8_at};
use mcx::PgString;
use types_error::PgResult;
use types_wal::{XLogRecordView, XLR_INFO_MASK};

// access/ginxlog.h
pub const XLOG_GIN_CREATE_PTREE: u8 = 0x10;
pub const XLOG_GIN_INSERT: u8 = 0x20;
pub const XLOG_GIN_SPLIT: u8 = 0x30;
pub const XLOG_GIN_VACUUM_PAGE: u8 = 0x40;
pub const XLOG_GIN_DELETE_PAGE: u8 = 0x50;
pub const XLOG_GIN_UPDATE_META_PAGE: u8 = 0x60;
pub const XLOG_GIN_INSERT_LISTPAGE: u8 = 0x70;
pub const XLOG_GIN_DELETE_LISTPAGE: u8 = 0x80;
pub const XLOG_GIN_VACUUM_DATA_LEAF_PAGE: u8 = 0x90;

pub const GIN_SEGMENT_UNMODIFIED: u8 = 0;
pub const GIN_SEGMENT_DELETE: u8 = 1;
pub const GIN_SEGMENT_INSERT: u8 = 2;
pub const GIN_SEGMENT_REPLACE: u8 = 3;
pub const GIN_SEGMENT_ADDITEMS: u8 = 4;

pub const GIN_INSERT_ISDATA: u16 = 0x01;
pub const GIN_INSERT_ISLEAF: u16 = 0x02;
pub const GIN_SPLIT_ROOT: u16 = 0x04;

/// `sizeof(ginxlogInsert)` — `uint16 flags` only.
const SIZEOF_GINXLOG_INSERT: usize = 2;
/// `sizeof(BlockIdData)` — `{uint16 bi_hi; uint16 bi_lo;}`.
const SIZEOF_BLOCK_ID_DATA: usize = 4;
/// `sizeof(ginxlogRecompressDataLeaf)` — `uint16 nactions` only.
const SIZEOF_GINXLOG_RECOMPRESS_DATA_LEAF: usize = 2;
/// `sizeof(ItemPointerData)` (6 bytes, 2-aligned).
const SIZEOF_ITEM_POINTER_DATA: usize = 6;
/// `offsetof(GinPostingList, bytes)` — `first` (6) + `nbytes` u16 (2).
const OFFSETOF_GIN_POSTING_LIST_BYTES: usize = 8;
/// `sizeof(GinMetaPageData)` (gin_private.h; natural layout, 8-aligned).
const SIZEOF_GIN_META_PAGE_DATA: usize = 56;

/// `SHORTALIGN`.
const fn shortalign(n: usize) -> usize {
    (n + 1) & !1
}

/// `desc_recompress_leaf(StringInfo buf, ginxlogRecompressDataLeaf *insertData)`.
/// `insert_data` starts at the `ginxlogRecompressDataLeaf` struct.
fn desc_recompress_leaf(buf: &mut PgString<'_>, insert_data: &[u8]) -> PgResult<()> {
    let nactions = u16_at(insert_data, 0);
    let mut walbuf = &insert_data[SIZEOF_GINXLOG_RECOMPRESS_DATA_LEAF..];

    appendf!(buf, " {} segments:", nactions as i32);

    for _ in 0..nactions {
        let a_segno = u8_at(walbuf, 0);
        let a_action = u8_at(walbuf, 1);
        walbuf = &walbuf[2..];
        let mut nitems: u16 = 0;

        if a_action == GIN_SEGMENT_INSERT || a_action == GIN_SEGMENT_REPLACE {
            // SizeOfGinPostingList: offsetof(bytes) + SHORTALIGN(nbytes)
            let nbytes = u16_at(walbuf, 6) as usize;
            let newsegsize = OFFSETOF_GIN_POSTING_LIST_BYTES + shortalign(nbytes);
            walbuf = &walbuf[shortalign(newsegsize)..];
        }

        if a_action == GIN_SEGMENT_ADDITEMS {
            nitems = u16_at(walbuf, 0);
            walbuf = &walbuf[2 + nitems as usize * SIZEOF_ITEM_POINTER_DATA..];
        }

        match a_action {
            GIN_SEGMENT_ADDITEMS => appendf!(buf, " {} (add {} items)", a_segno, nitems),
            GIN_SEGMENT_DELETE => appendf!(buf, " {} (delete)", a_segno),
            GIN_SEGMENT_INSERT => appendf!(buf, " {} (insert)", a_segno),
            GIN_SEGMENT_REPLACE => appendf!(buf, " {} (replace)", a_segno),
            _ => {
                appendf!(buf, " {} unknown action {} ???", a_segno, a_action);
                // cannot decode unrecognized actions further
                return Ok(());
            }
        }
    }
    Ok(())
}

/// `gin_desc(StringInfo buf, XLogReaderState *record)`.
pub fn gin_desc(buf: &mut PgString<'_>, record: &XLogRecordView<'_>) -> PgResult<()> {
    let rec = record.data();
    let info = record.info() & !XLR_INFO_MASK;

    match info {
        XLOG_GIN_CREATE_PTREE => { /* no further information */ }
        XLOG_GIN_INSERT => {
            let flags = u16_at(rec, 0); // ginxlogInsert.flags
            appendf!(
                buf,
                "isdata: {} isleaf: {}",
                if flags & GIN_INSERT_ISDATA != 0 { 'T' } else { 'F' },
                if flags & GIN_INSERT_ISLEAF != 0 { 'T' } else { 'F' }
            );
            if flags & GIN_INSERT_ISLEAF == 0 {
                let payload = &rec[SIZEOF_GINXLOG_INSERT..];
                let left_child_blkno = block_id_at(payload, 0);
                let right_child_blkno = block_id_at(payload, SIZEOF_BLOCK_ID_DATA);
                appendf!(buf, " children: {}/{}", left_child_blkno, right_child_blkno);
            }
            if record.has_block_image(0) {
                if record.block_image_apply(0) {
                    buf.try_push_str(" (full page image)")?;
                } else {
                    buf.try_push_str(" (full page image, for WAL verification)")?;
                }
            } else {
                let payload = record
                    .block_data(0)
                    .expect("XLOG_GIN_INSERT without FPI carries block 0 data");

                if flags & GIN_INSERT_ISDATA == 0 {
                    // ginxlogInsertEntry.isDelete: bool @2
                    appendf!(
                        buf,
                        " isdelete: {}",
                        if bool_at(payload, 2) { 'T' } else { 'F' }
                    );
                } else if flags & GIN_INSERT_ISLEAF != 0 {
                    desc_recompress_leaf(buf, payload)?;
                } else {
                    // ginxlogInsertDataInternal: offset u16 @0, newitem PostingItem @2
                    // PostingItem: child_blkno BlockIdData @+0, key ItemPointerData @+4
                    appendf!(
                        buf,
                        " pitem: {}-{}/{}",
                        block_id_at(payload, 2),
                        block_id_at(payload, 6),
                        u16_at(payload, 10)
                    );
                }
            }
        }
        XLOG_GIN_SPLIT => {
            // ginxlogSplit.flags: u16 @24 (locator 12 + rrlink 4 + children 8)
            let flags = u16_at(rec, 24);
            appendf!(
                buf,
                "isrootsplit: {}",
                if flags & GIN_SPLIT_ROOT != 0 { 'T' } else { 'F' }
            );
            appendf!(
                buf,
                " isdata: {} isleaf: {}",
                if flags & GIN_INSERT_ISDATA != 0 { 'T' } else { 'F' },
                if flags & GIN_INSERT_ISLEAF != 0 { 'T' } else { 'F' }
            );
        }
        XLOG_GIN_VACUUM_PAGE => { /* no further information */ }
        XLOG_GIN_VACUUM_DATA_LEAF_PAGE => {
            if record.has_block_image(0) {
                if record.block_image_apply(0) {
                    buf.try_push_str(" (full page image)")?;
                } else {
                    buf.try_push_str(" (full page image, for WAL verification)")?;
                }
            } else {
                // ginxlogVacuumDataLeafPage.data is a ginxlogRecompressDataLeaf @0
                let payload = record
                    .block_data(0)
                    .expect("XLOG_GIN_VACUUM_DATA_LEAF_PAGE without FPI carries block 0 data");
                desc_recompress_leaf(buf, payload)?;
            }
        }
        XLOG_GIN_DELETE_PAGE => { /* no further information */ }
        XLOG_GIN_UPDATE_META_PAGE => { /* no further information */ }
        XLOG_GIN_INSERT_LISTPAGE => { /* no further information */ }
        XLOG_GIN_DELETE_LISTPAGE => {
            // ginxlogDeleteListPages.ndeleted: i32 after GinMetaPageData
            appendf!(buf, "ndeleted: {}", i32_at(rec, SIZEOF_GIN_META_PAGE_DATA));
        }
        _ => {}
    }
    Ok(())
}

/// `gin_identify(uint8 info)` — `None` where C returns NULL.
pub fn gin_identify(info: u8) -> Option<&'static str> {
    match info & !XLR_INFO_MASK {
        XLOG_GIN_CREATE_PTREE => Some("CREATE_PTREE"),
        XLOG_GIN_INSERT => Some("INSERT"),
        XLOG_GIN_SPLIT => Some("SPLIT"),
        XLOG_GIN_VACUUM_PAGE => Some("VACUUM_PAGE"),
        XLOG_GIN_VACUUM_DATA_LEAF_PAGE => Some("VACUUM_DATA_LEAF_PAGE"),
        XLOG_GIN_DELETE_PAGE => Some("DELETE_PAGE"),
        XLOG_GIN_UPDATE_META_PAGE => Some("UPDATE_META_PAGE"),
        XLOG_GIN_INSERT_LISTPAGE => Some("INSERT_LISTPAGE"),
        XLOG_GIN_DELETE_LISTPAGE => Some("DELETE_LISTPAGE"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mcx::MemoryContext;
    use types_wal::XLogRecordBlockView;

    #[test]
    fn insert_internal_with_children() {
        let ctx = MemoryContext::new("test");
        let mut buf = PgString::new_in(ctx.mcx());

        // ginxlogInsert{flags=ISDATA} + BlockIdData[2] children
        let mut rec = Vec::new();
        rec.extend_from_slice(&GIN_INSERT_ISDATA.to_ne_bytes());
        // left child block 0x00010002 -> hi 1, lo 2
        rec.extend_from_slice(&1u16.to_ne_bytes());
        rec.extend_from_slice(&2u16.to_ne_bytes());
        // right child block 3
        rec.extend_from_slice(&0u16.to_ne_bytes());
        rec.extend_from_slice(&3u16.to_ne_bytes());

        let blocks = [XLogRecordBlockView::new(true, true, true, None)];
        let record = XLogRecordView::new(XLOG_GIN_INSERT, &rec, &blocks);
        gin_desc(&mut buf, &record).unwrap();
        assert_eq!(
            buf.as_str(),
            "isdata: T isleaf: F children: 65538/3 (full page image)"
        );
    }

    #[test]
    fn recompress_leaf_actions() {
        let ctx = MemoryContext::new("test");
        let mut buf = PgString::new_in(ctx.mcx());

        // nactions=2: segno 1 delete, segno 2 additems(2 items)
        let mut data = Vec::new();
        data.extend_from_slice(&2u16.to_ne_bytes());
        data.push(1);
        data.push(GIN_SEGMENT_DELETE);
        data.push(2);
        data.push(GIN_SEGMENT_ADDITEMS);
        data.extend_from_slice(&2u16.to_ne_bytes());
        data.extend_from_slice(&[0u8; 12]); // 2 ItemPointerData
        desc_recompress_leaf(&mut buf, &data).unwrap();
        assert_eq!(buf.as_str(), " 2 segments: 1 (delete) 2 (add 2 items)");
    }

    #[test]
    fn delete_listpage() {
        let ctx = MemoryContext::new("test");
        let mut buf = PgString::new_in(ctx.mcx());
        let mut rec = vec![0u8; 60];
        rec[56..60].copy_from_slice(&13i32.to_ne_bytes());
        let record = XLogRecordView::new(XLOG_GIN_DELETE_LISTPAGE, &rec, &[]);
        gin_desc(&mut buf, &record).unwrap();
        assert_eq!(buf.as_str(), "ndeleted: 13");
    }

    #[test]
    fn identifies() {
        assert_eq!(gin_identify(XLOG_GIN_CREATE_PTREE), Some("CREATE_PTREE"));
        assert_eq!(gin_identify(XLOG_GIN_VACUUM_DATA_LEAF_PAGE), Some("VACUUM_DATA_LEAF_PAGE"));
        assert_eq!(gin_identify(0xA0), None);
    }
}
