//! `access/rmgrdesc/nbtdesc.c` — rmgr descriptor routines for btree indexes.

use crate::appendf;
use rmgrdesc_small_seams::{array_desc, offset_elem_desc};
use mcx::PgString;
use types_error::PgResult;
use wal::{DecodedXLogRecord, XLR_INFO_MASK};
use xlog_records::arrays::{OffsetNumbers, SIZEOF_OFFSET_NUMBER};
use xlog_records::nbtxlog::{xl_btree_delete, xl_btree_dedup, xl_btree_insert,
                                  xl_btree_mark_page_halfdead, xl_btree_metadata,
                                  xl_btree_newroot, xl_btree_reuse_page, xl_btree_split,
                                  xl_btree_unlink_page, xl_btree_update, xl_btree_vacuum,
                                  SIZE_OF_BTREE_UPDATE};

// access/nbtxlog.h
pub const XLOG_BTREE_INSERT_LEAF: u8 = 0x00;
pub const XLOG_BTREE_INSERT_UPPER: u8 = 0x10;
pub const XLOG_BTREE_INSERT_META: u8 = 0x20;
pub const XLOG_BTREE_SPLIT_L: u8 = 0x30;
pub const XLOG_BTREE_SPLIT_R: u8 = 0x40;
pub const XLOG_BTREE_INSERT_POST: u8 = 0x50;
pub const XLOG_BTREE_DEDUP: u8 = 0x60;
pub const XLOG_BTREE_DELETE: u8 = 0x70;
pub const XLOG_BTREE_UNLINK_PAGE: u8 = 0x80;
pub const XLOG_BTREE_UNLINK_PAGE_META: u8 = 0x90;
pub const XLOG_BTREE_NEWROOT: u8 = 0xA0;
pub const XLOG_BTREE_MARK_PAGE_HALFDEAD: u8 = 0xB0;
pub const XLOG_BTREE_VACUUM: u8 = 0xC0;
pub const XLOG_BTREE_REUSE_PAGE: u8 = 0xD0;
pub const XLOG_BTREE_META_CLEANUP: u8 = 0xE0;

/// `delvacuum_desc(StringInfo buf, char *block_data, uint16 ndeleted,
/// uint16 nupdated)`.
fn delvacuum_desc(
    buf: &mut PgString<'_>,
    block_data: &[u8],
    ndeleted: u16,
    nupdated: u16,
) -> PgResult<()> {
    // Output deleted page offset number array
    let deleted = OffsetNumbers::from_bytes(block_data);
    buf.try_push_str(", deleted:")?;
    array_desc::call(
        buf,
        deleted.bytes_of(ndeleted as usize),
        SIZEOF_OFFSET_NUMBER,
        ndeleted as i32,
        &mut |buf, elem| offset_elem_desc::call(buf, OffsetNumbers::from_bytes(elem).get(0)),
    )?;

    // Output updates as an array of "update objects", where each element
    // contains a page offset number from updated array.  (This is not the
    // most literal representation of the underlying physical data structure
    // that we could use.  Readability seems more important here.)
    buf.try_push_str(", updated: [")?;
    let updatedoffsets = deleted.skip(ndeleted as usize);
    // the xl_btree_update items start right after the updated offsets
    let mut updates =
        &block_data[(ndeleted as usize + nupdated as usize) * SIZEOF_OFFSET_NUMBER..];
    for i in 0..nupdated as usize {
        let off = updatedoffsets.get(i);
        let update = xl_btree_update::from_bytes(updates);
        let ndeletedtids = update.ndeletedtids;

        // "ptid" is the symbol name used when building each xl_btree_update's
        // array of offsets into a posting list tuple's ItemPointerData array.
        // xl_btree_update describes a subset of the existing TIDs to delete.
        appendf!(buf, "{{ off: {}, nptids: {}, ptids: [", off, ndeletedtids);
        let ptids = xl_btree_update::ptids(updates);
        for p in 0..ndeletedtids as usize {
            appendf!(buf, "{}", ptids.get(p));
            if p + 1 < ndeletedtids as usize {
                buf.try_push_str(", ")?;
            }
        }
        buf.try_push_str("] }")?;
        if i + 1 < nupdated as usize {
            buf.try_push_str(", ")?;
        }

        updates = &updates[SIZE_OF_BTREE_UPDATE + ndeletedtids as usize * 2..];
    }
    buf.try_push_str("]")?;
    Ok(())
}

/// `btree_desc(StringInfo buf, XLogReaderState *record)`.
pub fn btree_desc(buf: &mut PgString<'_>, record: &DecodedXLogRecord<'_>) -> PgResult<()> {
    let rec = record.data();
    let info = record.info() & !XLR_INFO_MASK;

    match info {
        XLOG_BTREE_INSERT_LEAF | XLOG_BTREE_INSERT_UPPER | XLOG_BTREE_INSERT_META
        | XLOG_BTREE_INSERT_POST => {
            let xlrec = xl_btree_insert::from_bytes(rec);
            appendf!(buf, "off: {}", xlrec.offnum);
        }
        XLOG_BTREE_SPLIT_L | XLOG_BTREE_SPLIT_R => {
            let xlrec = xl_btree_split::from_bytes(rec);
            appendf!(
                buf,
                "level: {}, firstrightoff: {}, newitemoff: {}, postingoff: {}",
                xlrec.level,
                xlrec.firstrightoff,
                xlrec.newitemoff,
                xlrec.postingoff
            );
        }
        XLOG_BTREE_DEDUP => {
            let xlrec = xl_btree_dedup::from_bytes(rec);
            appendf!(buf, "nintervals: {}", xlrec.nintervals);
        }
        XLOG_BTREE_VACUUM => {
            let xlrec = xl_btree_vacuum::from_bytes(rec);
            appendf!(buf, "ndeleted: {}, nupdated: {}", xlrec.ndeleted, xlrec.nupdated);

            if record.has_block_data(0) {
                delvacuum_desc(
                    buf,
                    record.block_data(0).expect("checked has_block_data"),
                    xlrec.ndeleted,
                    xlrec.nupdated,
                )?;
            }
        }
        XLOG_BTREE_DELETE => {
            let xlrec = xl_btree_delete::from_bytes(rec);
            appendf!(
                buf,
                "snapshotConflictHorizon: {}, ndeleted: {}, nupdated: {}, isCatalogRel: {}",
                xlrec.snapshotConflictHorizon,
                xlrec.ndeleted,
                xlrec.nupdated,
                if xlrec.isCatalogRel { 'T' } else { 'F' }
            );

            if record.has_block_data(0) {
                delvacuum_desc(
                    buf,
                    record.block_data(0).expect("checked has_block_data"),
                    xlrec.ndeleted,
                    xlrec.nupdated,
                )?;
            }
        }
        XLOG_BTREE_MARK_PAGE_HALFDEAD => {
            let xlrec = xl_btree_mark_page_halfdead::from_bytes(rec);
            appendf!(
                buf,
                "topparent: {}, leaf: {}, left: {}, right: {}",
                xlrec.topparent,
                xlrec.leafblk,
                xlrec.leftblk,
                xlrec.rightblk
            );
        }
        XLOG_BTREE_UNLINK_PAGE_META | XLOG_BTREE_UNLINK_PAGE => {
            let xlrec = xl_btree_unlink_page::from_bytes(rec);
            appendf!(
                buf,
                "left: {}, right: {}, level: {}, safexid: {}:{}, ",
                xlrec.leftsib,
                xlrec.rightsib,
                xlrec.level,
                xlrec.safexid.epoch(),
                xlrec.safexid.xid()
            );
            appendf!(
                buf,
                "leafleft: {}, leafright: {}, leaftopparent: {}",
                xlrec.leafleftsib,
                xlrec.leafrightsib,
                xlrec.leaftopparent
            );
        }
        XLOG_BTREE_NEWROOT => {
            let xlrec = xl_btree_newroot::from_bytes(rec);
            appendf!(buf, "level: {}", xlrec.level);
        }
        XLOG_BTREE_REUSE_PAGE => {
            let xlrec = xl_btree_reuse_page::from_bytes(rec);
            appendf!(
                buf,
                "rel: {}/{}/{}, snapshotConflictHorizon: {}:{}, isCatalogRel: {}",
                xlrec.locator.spcOid,
                xlrec.locator.dbOid,
                xlrec.locator.relNumber,
                xlrec.snapshotConflictHorizon.epoch(),
                xlrec.snapshotConflictHorizon.xid(),
                if xlrec.isCatalogRel { 'T' } else { 'F' }
            );
        }
        XLOG_BTREE_META_CLEANUP => {
            let xlrec = xl_btree_metadata::from_bytes(
                record
                    .block_data(0)
                    .expect("XLOG_BTREE_META_CLEANUP carries block 0 data"),
            );
            appendf!(buf, "last_cleanup_num_delpages: {}", xlrec.last_cleanup_num_delpages);
        }
        _ => {}
    }
    Ok(())
}

/// `btree_identify(uint8 info)` — `None` where C returns NULL.
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::record;
    use mcx::MemoryContext;

    fn desc(info: u8, data: &[u8]) -> String {
        let ctx = MemoryContext::new("test");
        let mut buf = PgString::new_in(ctx.mcx());
        let record = record(ctx.mcx(), info, data, &[]);
        btree_desc(&mut buf, &record).unwrap();
        buf.as_str().to_string()
    }

    #[test]
    fn formats_records() {
        assert_eq!(desc(XLOG_BTREE_INSERT_LEAF, &7u16.to_ne_bytes()), "off: 7");

        let mut rec = vec![0u8; 36];
        rec[0..4].copy_from_slice(&1u32.to_ne_bytes()); // leftsib
        rec[4..8].copy_from_slice(&2u32.to_ne_bytes()); // rightsib
        rec[8..12].copy_from_slice(&3u32.to_ne_bytes()); // level
        rec[16..24].copy_from_slice(&((4u64 << 32) | 5).to_ne_bytes()); // safexid
        rec[24..28].copy_from_slice(&6u32.to_ne_bytes());
        rec[28..32].copy_from_slice(&7u32.to_ne_bytes());
        rec[32..36].copy_from_slice(&8u32.to_ne_bytes());
        assert_eq!(
            desc(XLOG_BTREE_UNLINK_PAGE, &rec),
            "left: 1, right: 2, level: 3, safexid: 4:5, leafleft: 6, leafright: 7, leaftopparent: 8"
        );
    }

    #[test]
    fn identifies() {
        assert_eq!(btree_identify(XLOG_BTREE_META_CLEANUP), Some("META_CLEANUP"));
        assert_eq!(btree_identify(0xF0), None);
    }
}
