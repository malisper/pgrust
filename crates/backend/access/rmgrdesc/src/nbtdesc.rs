use crate::{appendf, array_desc, block_data, has_block_data, rec_data, rec_info, Rec, XLR_INFO_MASK};
use stringinfo::StringInfo;
use types_error::PgResult;
use types_nbtree::xlog::{
    XLOG_BTREE_DEDUP, XLOG_BTREE_DELETE, XLOG_BTREE_INSERT_LEAF, XLOG_BTREE_INSERT_META,
    XLOG_BTREE_INSERT_POST, XLOG_BTREE_INSERT_UPPER, XLOG_BTREE_MARK_PAGE_HALFDEAD,
    XLOG_BTREE_META_CLEANUP, XLOG_BTREE_NEWROOT, XLOG_BTREE_REUSE_PAGE, XLOG_BTREE_SPLIT_L,
    XLOG_BTREE_SPLIT_R, XLOG_BTREE_UNLINK_PAGE, XLOG_BTREE_UNLINK_PAGE_META, XLOG_BTREE_VACUUM,
};
use xlogreader_seams::XLogReaderState;

// SizeOfBtreeUpdate: xl_btree_update is { uint16 ndeletedtids; }.
const SizeOfBtreeUpdate: usize = 2;

pub fn btree_desc(buf: &mut StringInfo<'_>, record: &XLogReaderState) -> PgResult<()> {
    let rec = Rec(rec_data(record));
    let info = rec_info(record) & !XLR_INFO_MASK;

    match info {
        XLOG_BTREE_INSERT_LEAF | XLOG_BTREE_INSERT_UPPER | XLOG_BTREE_INSERT_META
        | XLOG_BTREE_INSERT_POST => {
            appendf!(buf, "off: {}", rec.u16(0, "xl_btree_insert")?)?;
        }
        XLOG_BTREE_SPLIT_L | XLOG_BTREE_SPLIT_R => {
            // level 0, firstrightoff 4, newitemoff 6, postingoff 8.
            appendf!(
                buf,
                "level: {}, firstrightoff: {}, newitemoff: {}, postingoff: {}",
                rec.u32(0, "xl_btree_split")?,
                rec.u16(4, "xl_btree_split")?,
                rec.u16(6, "xl_btree_split")?,
                rec.u16(8, "xl_btree_split")?
            )?;
        }
        XLOG_BTREE_DEDUP => {
            appendf!(buf, "nintervals: {}", rec.u16(0, "xl_btree_dedup")?)?;
        }
        XLOG_BTREE_VACUUM => {
            // xl_btree_vacuum: ndeleted 0, nupdated 2.
            let ndeleted = rec.u16(0, "xl_btree_vacuum")?;
            let nupdated = rec.u16(2, "xl_btree_vacuum")?;
            appendf!(buf, "ndeleted: {ndeleted}, nupdated: {nupdated}")?;
            if has_block_data(record, 0) {
                delvacuum_desc(buf, block_data(record, 0), ndeleted, nupdated)?;
            }
        }
        XLOG_BTREE_DELETE => {
            // horizon 0, ndeleted 4, nupdated 6, isCatalogRel 8.
            let ndeleted = rec.u16(4, "xl_btree_delete")?;
            let nupdated = rec.u16(6, "xl_btree_delete")?;
            appendf!(
                buf,
                "snapshotConflictHorizon: {}, ndeleted: {ndeleted}, nupdated: {nupdated}, isCatalogRel: {}",
                rec.u32(0, "xl_btree_delete")?,
                if rec.u8(8, "xl_btree_delete")? != 0 { 'T' } else { 'F' }
            )?;
            if has_block_data(record, 0) {
                delvacuum_desc(buf, block_data(record, 0), ndeleted, nupdated)?;
            }
        }
        XLOG_BTREE_MARK_PAGE_HALFDEAD => {
            // poffset 0, leafblk 4, leftblk 8, rightblk 12, topparent 16.
            appendf!(
                buf,
                "topparent: {}, leaf: {}, left: {}, right: {}",
                rec.u32(16, "xl_btree_mark_page_halfdead")?,
                rec.u32(4, "xl_btree_mark_page_halfdead")?,
                rec.u32(8, "xl_btree_mark_page_halfdead")?,
                rec.u32(12, "xl_btree_mark_page_halfdead")?
            )?;
        }
        XLOG_BTREE_UNLINK_PAGE_META | XLOG_BTREE_UNLINK_PAGE => {
            // leftsib 0, rightsib 4, level 8, safexid 16 (u64-aligned),
            // leafleftsib 24, leafrightsib 28, leaftopparent 32.
            let safexid = rec.u64(16, "xl_btree_unlink_page")?;
            appendf!(
                buf,
                "left: {}, right: {}, level: {}, safexid: {}:{}, ",
                rec.u32(0, "xl_btree_unlink_page")?,
                rec.u32(4, "xl_btree_unlink_page")?,
                rec.u32(8, "xl_btree_unlink_page")?,
                (safexid >> 32) as u32,
                safexid as u32
            )?;
            appendf!(
                buf,
                "leafleft: {}, leafright: {}, leaftopparent: {}",
                rec.u32(24, "xl_btree_unlink_page")?,
                rec.u32(28, "xl_btree_unlink_page")?,
                rec.u32(32, "xl_btree_unlink_page")?
            )?;
        }
        XLOG_BTREE_NEWROOT => {
            // xl_btree_newroot: rootblk 0, level 4.
            appendf!(buf, "level: {}", rec.u32(4, "xl_btree_newroot")?)?;
        }
        XLOG_BTREE_REUSE_PAGE => {
            // locator 0..12, block 12, horizon 16 (u64-aligned), isCatalogRel 24.
            let horizon = rec.u64(16, "xl_btree_reuse_page")?;
            appendf!(
                buf,
                "rel: {}/{}/{}, snapshotConflictHorizon: {}:{}, isCatalogRel: {}",
                rec.u32(0, "xl_btree_reuse_page")?,
                rec.u32(4, "xl_btree_reuse_page")?,
                rec.u32(8, "xl_btree_reuse_page")?,
                (horizon >> 32) as u32,
                horizon as u32,
                if rec.u8(24, "xl_btree_reuse_page")? != 0 { 'T' } else { 'F' }
            )?;
        }
        XLOG_BTREE_META_CLEANUP => {
            // xl_btree_metadata in block 0: last_cleanup_num_delpages at 20.
            let meta = Rec(block_data(record, 0));
            appendf!(
                buf,
                "last_cleanup_num_delpages: {}",
                meta.u32(20, "xl_btree_metadata")?
            )?;
        }
        _ => {}
    }
    Ok(())
}

pub fn btree_identify(info: u8) -> Option<&'static str> {
    match info & !XLR_INFO_MASK {
        XLOG_BTREE_INSERT_LEAF => Some("INSERT_LEAF"),
        XLOG_BTREE_INSERT_UPPER => Some("INSERT_UPPER"),
        XLOG_BTREE_INSERT_META => Some("INSERT_META"),
        XLOG_BTREE_SPLIT_L => Some("SPLIT_L"),
        XLOG_BTREE_SPLIT_R => Some("SPLIT_R"),
        XLOG_BTREE_INSERT_POST => Some("INSERT_POST"),
        XLOG_BTREE_DEDUP => Some("DEDUP"),
        XLOG_BTREE_VACUUM => Some("VACUUM"),
        XLOG_BTREE_DELETE => Some("DELETE"),
        XLOG_BTREE_MARK_PAGE_HALFDEAD => Some("MARK_PAGE_HALFDEAD"),
        XLOG_BTREE_UNLINK_PAGE => Some("UNLINK_PAGE"),
        XLOG_BTREE_UNLINK_PAGE_META => Some("UNLINK_PAGE_META"),
        XLOG_BTREE_NEWROOT => Some("NEWROOT"),
        XLOG_BTREE_REUSE_PAGE => Some("REUSE_PAGE"),
        XLOG_BTREE_META_CLEANUP => Some("META_CLEANUP"),
        _ => None,
    }
}

fn delvacuum_desc(
    buf: &mut StringInfo<'_>,
    block_data: &[u8],
    ndeleted: u16,
    nupdated: u16,
) -> PgResult<()> {
    let what = "xl_btree_update stream";
    let d = Rec(block_data);

    buf.append_str(", deleted:")?;
    array_desc(buf, ndeleted as usize, |buf, i| {
        appendf!(buf, "{}", d.u16(2 * i, what)?)
    })?;

    // One object per updated offset (readability over layout, as in C).
    buf.append_str(", updated: [")?;
    let updatedoffsets = ndeleted as usize * 2;
    let mut update = updatedoffsets + nupdated as usize * 2;
    for i in 0..nupdated as usize {
        let off = d.u16(updatedoffsets + 2 * i, what)?;
        let ndeletedtids = d.u16(update, what)?;
        appendf!(buf, "{{ off: {off}, nptids: {ndeletedtids}, ptids: [")?;
        for p in 0..ndeletedtids as usize {
            appendf!(buf, "{}", d.u16(update + SizeOfBtreeUpdate + 2 * p, what)?)?;
            if p < ndeletedtids as usize - 1 {
                buf.append_str(", ")?;
            }
        }
        buf.append_str("] }")?;
        if i < nupdated as usize - 1 {
            buf.append_str(", ")?;
        }
        update += SizeOfBtreeUpdate + ndeletedtids as usize * 2;
    }
    buf.append_byte(b']')
}
