use crate::{appendf, block_data, has_block_data, rec_data, rec_info, Rec, XLR_INFO_MASK};
use gin_vocab::{
    size_of_gin_posting_list, SHORTALIGN, GIN_INSERT_ISDATA, GIN_INSERT_ISLEAF, GIN_SEGMENT_ADDITEMS,
    GIN_SEGMENT_DELETE, GIN_SEGMENT_INSERT, GIN_SEGMENT_REPLACE, GinMetaPageData, XLOG_GIN_CREATE_PTREE,
    XLOG_GIN_DELETE_LISTPAGE, XLOG_GIN_DELETE_PAGE, XLOG_GIN_INSERT, XLOG_GIN_INSERT_LISTPAGE,
    XLOG_GIN_SPLIT, XLOG_GIN_UPDATE_META_PAGE, XLOG_GIN_VACUUM_DATA_LEAF_PAGE, XLOG_GIN_VACUUM_PAGE,
};
use stringinfo::StringInfo;
use types_error::PgResult;
use xlogreader_seams::XLogReaderState;

const GIN_META_PAGE_SIZE: usize = core::mem::size_of::<GinMetaPageData>();
const ITEM_POINTER_SIZE: usize = 6;

fn desc_recompress_leaf(buf: &mut StringInfo<'_>, insert_data: &[u8]) -> PgResult<()> {
    // ginxlogRecompressDataLeaf: nactions (uint16) followed by the walbuf stream.
    let rec = Rec(insert_data);
    let nactions = rec.u16(0, "ginxlogRecompressDataLeaf")? as usize;
    // Cursor into insert_data, mirroring C's advancing `walbuf` pointer.
    let mut pos = 2usize;

    appendf!(buf, " {nactions} segments:")?;

    for _ in 0..nactions {
        let a_segno = rec.u8(pos, "ginxlogRecompressDataLeaf")?;
        let a_action = rec.u8(pos + 1, "ginxlogRecompressDataLeaf")?;
        pos += 2;

        if a_action == GIN_SEGMENT_INSERT || a_action == GIN_SEGMENT_REPLACE {
            // GinPostingList: nbytes is a uint16 at offset 6 of the posting list.
            let nbytes = rec.u16(pos + 6, "ginxlogRecompressDataLeaf")? as usize;
            pos += SHORTALIGN(size_of_gin_posting_list(nbytes));
        }

        let mut nitems = 0u16;
        if a_action == GIN_SEGMENT_ADDITEMS {
            nitems = rec.u16(pos, "ginxlogRecompressDataLeaf")?;
            pos += 2 + nitems as usize * ITEM_POINTER_SIZE;
        }

        match a_action {
            GIN_SEGMENT_ADDITEMS => appendf!(buf, " {a_segno} (add {nitems} items)")?,
            GIN_SEGMENT_DELETE => appendf!(buf, " {a_segno} (delete)")?,
            GIN_SEGMENT_INSERT => appendf!(buf, " {a_segno} (insert)")?,
            GIN_SEGMENT_REPLACE => appendf!(buf, " {a_segno} (replace)")?,
            other => {
                appendf!(buf, " {a_segno} unknown action {other} ???")?;
                return Ok(());
            }
        }
    }
    Ok(())
}

pub fn gin_desc(buf: &mut StringInfo<'_>, record: &XLogReaderState) -> PgResult<()> {
    let rec = Rec(rec_data(record));
    let info = rec_info(record) & !XLR_INFO_MASK;

    match info {
        XLOG_GIN_CREATE_PTREE => {}
        XLOG_GIN_INSERT => {
            let flags = rec.u16(0, "ginxlogInsert")?;
            appendf!(
                buf,
                "isdata: {} isleaf: {}",
                if flags & GIN_INSERT_ISDATA != 0 { 'T' } else { 'F' },
                if flags & GIN_INSERT_ISLEAF != 0 { 'T' } else { 'F' }
            )?;
            if flags & GIN_INSERT_ISLEAF == 0 {
                // ginxlogInsert (2 bytes) followed by BlockIdData[2] (both
                // read via the hi/lo split, matching C's BlockIdGetBlockNumber).
                let left_hi = rec.u16(2, "ginxlogInsert")? as u32;
                let left_lo = rec.u16(4, "ginxlogInsert")? as u32;
                let right_hi = rec.u16(6, "ginxlogInsert")? as u32;
                let right_lo = rec.u16(8, "ginxlogInsert")? as u32;
                appendf!(
                    buf,
                    " children: {}/{}",
                    (left_hi << 16) | left_lo,
                    (right_hi << 16) | right_lo
                )?;
            }
            if record.has_block_image(0) {
                if record.block_image_apply(0) {
                    buf.append_str(" (full page image)")?;
                } else {
                    buf.append_str(" (full page image, for WAL verification)")?;
                }
            } else if has_block_data(record, 0) {
                let payload = block_data(record, 0);
                if flags & GIN_INSERT_ISDATA == 0 {
                    // ginxlogInsertEntry: offset 0, isDelete 2.
                    let is_delete = Rec(payload).u8(2, "ginxlogInsertEntry")?;
                    appendf!(
                        buf,
                        " isdelete: {}",
                        if is_delete != 0 { 'T' } else { 'F' }
                    )?;
                } else if flags & GIN_INSERT_ISLEAF != 0 {
                    desc_recompress_leaf(buf, payload)?;
                } else {
                    // ginxlogInsertDataInternal: offset 0, newitem (PostingItem) 2.
                    // PostingItem { BlockIdData child_blkno; ItemPointerData key }.
                    let d = Rec(payload);
                    let child_hi = d.u16(2, "ginxlogInsertDataInternal")? as u32;
                    let child_lo = d.u16(4, "ginxlogInsertDataInternal")? as u32;
                    let key_hi = d.u16(6, "ginxlogInsertDataInternal")? as u32;
                    let key_lo = d.u16(8, "ginxlogInsertDataInternal")? as u32;
                    let key_off = d.u16(10, "ginxlogInsertDataInternal")?;
                    appendf!(
                        buf,
                        " pitem: {}-{}/{}",
                        (child_hi << 16) | child_lo,
                        (key_hi << 16) | key_lo,
                        key_off
                    )?;
                }
            }
        }
        XLOG_GIN_SPLIT => {
            // ginxlogSplit: locator 0..12, rrlink 12, leftChildBlkno 16, rightChildBlkno 20, flags 24.
            let flags = rec.u16(24, "ginxlogSplit")?;
            appendf!(
                buf,
                "isrootsplit: {}",
                if flags & gin_vocab::GIN_SPLIT_ROOT != 0 { 'T' } else { 'F' }
            )?;
            appendf!(
                buf,
                " isdata: {} isleaf: {}",
                if flags & GIN_INSERT_ISDATA != 0 { 'T' } else { 'F' },
                if flags & GIN_INSERT_ISLEAF != 0 { 'T' } else { 'F' }
            )?;
        }
        XLOG_GIN_VACUUM_PAGE => {}
        XLOG_GIN_VACUUM_DATA_LEAF_PAGE => {
            if record.has_block_image(0) {
                if record.block_image_apply(0) {
                    buf.append_str(" (full page image)")?;
                } else {
                    buf.append_str(" (full page image, for WAL verification)")?;
                }
            } else if has_block_data(record, 0) {
                desc_recompress_leaf(buf, block_data(record, 0))?;
            }
        }
        XLOG_GIN_DELETE_PAGE => {}
        XLOG_GIN_UPDATE_META_PAGE => {}
        XLOG_GIN_INSERT_LISTPAGE => {}
        XLOG_GIN_DELETE_LISTPAGE => {
            appendf!(
                buf,
                "ndeleted: {}",
                rec.i32(GIN_META_PAGE_SIZE, "ginxlogDeleteListPages")?
            )?;
        }
        _ => {}
    }
    Ok(())
}

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
