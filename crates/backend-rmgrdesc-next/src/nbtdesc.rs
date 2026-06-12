//! `access/rmgrdesc/nbtdesc.c` — rmgr descriptor routines for btree indexes.

use crate::{appendf, bool_at, full_xid_parts, u16_at, u32_at, u64_at};
use backend_rmgrdesc_small_seams::{array_desc, offset_elem_desc};
use mcx::PgString;
use types_error::PgResult;
use types_wal::{XLogRecordView, XLR_INFO_MASK};

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

/// `sizeof(OffsetNumber)`.
const SIZEOF_OFFSET_NUMBER: usize = 2;
/// `SizeOfBtreeUpdate` — `offsetof(xl_btree_update, ndeletedtids) + sizeof(uint16)`.
const SIZEOF_BTREE_UPDATE: usize = 2;

/// `delvacuum_desc(StringInfo buf, char *block_data, uint16 ndeleted,
/// uint16 nupdated)`.
fn delvacuum_desc(
    buf: &mut PgString<'_>,
    block_data: &[u8],
    ndeleted: u16,
    nupdated: u16,
) -> PgResult<()> {
    // Output deleted page offset number array
    buf.try_push_str(", deleted:")?;
    array_desc::call(
        buf,
        &block_data[..ndeleted as usize * SIZEOF_OFFSET_NUMBER],
        SIZEOF_OFFSET_NUMBER,
        ndeleted as i32,
        &mut |buf, elem| offset_elem_desc::call(buf, elem),
    )?;

    // Output updates as an array of "update objects", where each element
    // contains a page offset number from updated array.  (This is not the
    // most literal representation of the underlying physical data structure
    // that we could use.  Readability seems more important here.)
    buf.try_push_str(", updated: [")?;
    let updatedoffsets = &block_data[ndeleted as usize * SIZEOF_OFFSET_NUMBER..];
    let mut updates = &updatedoffsets[nupdated as usize * SIZEOF_OFFSET_NUMBER..];
    for i in 0..nupdated as usize {
        let off = u16_at(updatedoffsets, i * SIZEOF_OFFSET_NUMBER);
        let ndeletedtids = u16_at(updates, 0);

        // "ptid" is the symbol name used when building each xl_btree_update's
        // array of offsets into a posting list tuple's ItemPointerData array.
        // xl_btree_update describes a subset of the existing TIDs to delete.
        appendf!(buf, "{{ off: {}, nptids: {}, ptids: [", off, ndeletedtids);
        for p in 0..ndeletedtids as usize {
            appendf!(buf, "{}", u16_at(updates, SIZEOF_BTREE_UPDATE + p * 2));
            if p + 1 < ndeletedtids as usize {
                buf.try_push_str(", ")?;
            }
        }
        buf.try_push_str("] }")?;
        if i + 1 < nupdated as usize {
            buf.try_push_str(", ")?;
        }

        updates = &updates[SIZEOF_BTREE_UPDATE + ndeletedtids as usize * 2..];
    }
    buf.try_push_str("]")?;
    Ok(())
}

/// `btree_desc(StringInfo buf, XLogReaderState *record)`.
pub fn btree_desc(buf: &mut PgString<'_>, record: &XLogRecordView<'_>) -> PgResult<()> {
    let rec = record.data();
    let info = record.info() & !XLR_INFO_MASK;

    match info {
        XLOG_BTREE_INSERT_LEAF | XLOG_BTREE_INSERT_UPPER | XLOG_BTREE_INSERT_META
        | XLOG_BTREE_INSERT_POST => {
            // xl_btree_insert: offnum u16 @0
            appendf!(buf, "off: {}", u16_at(rec, 0));
        }
        XLOG_BTREE_SPLIT_L | XLOG_BTREE_SPLIT_R => {
            // xl_btree_split: level u32 @0, firstrightoff u16 @4, newitemoff u16 @6,
            // postingoff u16 @8
            appendf!(
                buf,
                "level: {}, firstrightoff: {}, newitemoff: {}, postingoff: {}",
                u32_at(rec, 0),
                u16_at(rec, 4),
                u16_at(rec, 6),
                u16_at(rec, 8)
            );
        }
        XLOG_BTREE_DEDUP => {
            // xl_btree_dedup: nintervals u16 @0
            appendf!(buf, "nintervals: {}", u16_at(rec, 0));
        }
        XLOG_BTREE_VACUUM => {
            // xl_btree_vacuum: ndeleted u16 @0, nupdated u16 @2
            let ndeleted = u16_at(rec, 0);
            let nupdated = u16_at(rec, 2);
            appendf!(buf, "ndeleted: {}, nupdated: {}", ndeleted, nupdated);

            if record.has_block_data(0) {
                delvacuum_desc(
                    buf,
                    record.block_data(0).expect("checked has_block_data"),
                    ndeleted,
                    nupdated,
                )?;
            }
        }
        XLOG_BTREE_DELETE => {
            // xl_btree_delete: snapshotConflictHorizon u32 @0, ndeleted u16 @4,
            // nupdated u16 @6, isCatalogRel bool @8
            let ndeleted = u16_at(rec, 4);
            let nupdated = u16_at(rec, 6);
            appendf!(
                buf,
                "snapshotConflictHorizon: {}, ndeleted: {}, nupdated: {}, isCatalogRel: {}",
                u32_at(rec, 0),
                ndeleted,
                nupdated,
                if bool_at(rec, 8) { 'T' } else { 'F' }
            );

            if record.has_block_data(0) {
                delvacuum_desc(
                    buf,
                    record.block_data(0).expect("checked has_block_data"),
                    ndeleted,
                    nupdated,
                )?;
            }
        }
        XLOG_BTREE_MARK_PAGE_HALFDEAD => {
            // xl_btree_mark_page_halfdead: poffset u16 @0, leafblk u32 @4,
            // leftblk u32 @8, rightblk u32 @12, topparent u32 @16
            appendf!(
                buf,
                "topparent: {}, leaf: {}, left: {}, right: {}",
                u32_at(rec, 16),
                u32_at(rec, 4),
                u32_at(rec, 8),
                u32_at(rec, 12)
            );
        }
        XLOG_BTREE_UNLINK_PAGE_META | XLOG_BTREE_UNLINK_PAGE => {
            // xl_btree_unlink_page: leftsib u32 @0, rightsib u32 @4, level u32 @8,
            // safexid u64 @16 (8-aligned), leafleftsib u32 @24, leafrightsib u32 @28,
            // leaftopparent u32 @32
            let (epoch, xid) = full_xid_parts(u64_at(rec, 16));
            appendf!(
                buf,
                "left: {}, right: {}, level: {}, safexid: {}:{}, ",
                u32_at(rec, 0),
                u32_at(rec, 4),
                u32_at(rec, 8),
                epoch,
                xid
            );
            appendf!(
                buf,
                "leafleft: {}, leafright: {}, leaftopparent: {}",
                u32_at(rec, 24),
                u32_at(rec, 28),
                u32_at(rec, 32)
            );
        }
        XLOG_BTREE_NEWROOT => {
            // xl_btree_newroot: rootblk u32 @0, level u32 @4
            appendf!(buf, "level: {}", u32_at(rec, 4));
        }
        XLOG_BTREE_REUSE_PAGE => {
            // xl_btree_reuse_page: locator u32x3 @0, block u32 @12,
            // snapshotConflictHorizon u64 @16 (8-aligned), isCatalogRel bool @24
            let (epoch, xid) = full_xid_parts(u64_at(rec, 16));
            appendf!(
                buf,
                "rel: {}/{}/{}, snapshotConflictHorizon: {}:{}, isCatalogRel: {}",
                u32_at(rec, 0),
                u32_at(rec, 4),
                u32_at(rec, 8),
                epoch,
                xid,
                if bool_at(rec, 24) { 'T' } else { 'F' }
            );
        }
        XLOG_BTREE_META_CLEANUP => {
            // xl_btree_metadata in block 0 data: version u32 @0, root u32 @4,
            // level u32 @8, fastroot u32 @12, fastlevel u32 @16,
            // last_cleanup_num_delpages u32 @20, allequalimage bool @24
            let xlrec = record
                .block_data(0)
                .expect("XLOG_BTREE_META_CLEANUP carries block 0 data");
            appendf!(buf, "last_cleanup_num_delpages: {}", u32_at(xlrec, 20));
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
    use mcx::MemoryContext;

    fn desc(info: u8, data: &[u8]) -> String {
        let ctx = MemoryContext::new("test");
        let mut buf = PgString::new_in(ctx.mcx());
        let record = XLogRecordView::new(info, data, &[]);
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
