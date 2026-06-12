//! `access/rmgrdesc/spgdesc.c` — rmgr descriptor routines for SP-GiST indexes.

use crate::{appendf, bool_at, i8_at, u16_at, u32_at};
use mcx::PgString;
use types_error::PgResult;
use types_wal::{XLogRecordView, XLR_INFO_MASK};

// access/spgxlog.h
pub const XLOG_SPGIST_ADD_LEAF: u8 = 0x10;
pub const XLOG_SPGIST_MOVE_LEAFS: u8 = 0x20;
pub const XLOG_SPGIST_ADD_NODE: u8 = 0x30;
pub const XLOG_SPGIST_SPLIT_TUPLE: u8 = 0x40;
pub const XLOG_SPGIST_PICKSPLIT: u8 = 0x50;
pub const XLOG_SPGIST_VACUUM_LEAF: u8 = 0x60;
pub const XLOG_SPGIST_VACUUM_ROOT: u8 = 0x70;
pub const XLOG_SPGIST_VACUUM_REDIRECT: u8 = 0x80;

/// `spg_desc(StringInfo buf, XLogReaderState *record)`.
pub fn spg_desc(buf: &mut PgString<'_>, record: &XLogRecordView<'_>) -> PgResult<()> {
    let rec = record.data();
    let info = record.info() & !XLR_INFO_MASK;

    match info {
        XLOG_SPGIST_ADD_LEAF => {
            // spgxlogAddLeaf: newPage bool @0, storesNulls bool @1, offnumLeaf u16 @2,
            // offnumHeadLeaf u16 @4, offnumParent u16 @6, nodeI u16 @8
            appendf!(
                buf,
                "off: {}, headoff: {}, parentoff: {}, nodeI: {}",
                u16_at(rec, 2),
                u16_at(rec, 4),
                u16_at(rec, 6),
                u16_at(rec, 8)
            );
            if bool_at(rec, 0) {
                buf.try_push_str(" (newpage)")?;
            }
            if bool_at(rec, 1) {
                buf.try_push_str(" (nulls)")?;
            }
        }
        XLOG_SPGIST_MOVE_LEAFS => {
            // spgxlogMoveLeafs: nMoves u16 @0, newPage bool @2, replaceDead bool @3,
            // storesNulls bool @4, offnumParent u16 @6, nodeI u16 @8
            appendf!(
                buf,
                "nmoves: {}, parentoff: {}, nodeI: {}",
                u16_at(rec, 0),
                u16_at(rec, 6),
                u16_at(rec, 8)
            );
            if bool_at(rec, 2) {
                buf.try_push_str(" (newpage)")?;
            }
            if bool_at(rec, 3) {
                buf.try_push_str(" (replacedead)")?;
            }
            if bool_at(rec, 4) {
                buf.try_push_str(" (nulls)")?;
            }
        }
        XLOG_SPGIST_ADD_NODE => {
            // spgxlogAddNode: offnum u16 @0, offnumNew u16 @2, newPage bool @4,
            // parentBlk int8 @5, offnumParent u16 @6, nodeI u16 @8
            appendf!(
                buf,
                "off: {}, newoff: {}, parentBlk: {}, parentoff: {}, nodeI: {}",
                u16_at(rec, 0),
                u16_at(rec, 2),
                i8_at(rec, 5),
                u16_at(rec, 6),
                u16_at(rec, 8)
            );
            if bool_at(rec, 4) {
                buf.try_push_str(" (newpage)")?;
            }
        }
        XLOG_SPGIST_SPLIT_TUPLE => {
            // spgxlogSplitTuple: offnumPrefix u16 @0, offnumPostfix u16 @2,
            // newPage bool @4, postfixBlkSame bool @5
            appendf!(
                buf,
                "prefixoff: {}, postfixoff: {}",
                u16_at(rec, 0),
                u16_at(rec, 2)
            );
            if bool_at(rec, 4) {
                buf.try_push_str(" (newpage)")?;
            }
            if bool_at(rec, 5) {
                buf.try_push_str(" (same)")?;
            }
        }
        XLOG_SPGIST_PICKSPLIT => {
            // spgxlogPickSplit: isRootSplit bool @0, nDelete u16 @2, nInsert u16 @4,
            // initSrc bool @6, initDest bool @7, offnumInner u16 @8, initInner bool @10,
            // storesNulls bool @11, innerIsParent bool @12, offnumParent u16 @14,
            // nodeI u16 @16
            appendf!(
                buf,
                "ndelete: {}, ninsert: {}, inneroff: {}, parentoff: {}, nodeI: {}",
                u16_at(rec, 2),
                u16_at(rec, 4),
                u16_at(rec, 8),
                u16_at(rec, 14),
                u16_at(rec, 16)
            );
            if bool_at(rec, 12) {
                buf.try_push_str(" (innerIsParent)")?;
            }
            if bool_at(rec, 11) {
                buf.try_push_str(" (nulls)")?;
            }
            if bool_at(rec, 0) {
                buf.try_push_str(" (isRootSplit)")?;
            }
        }
        XLOG_SPGIST_VACUUM_LEAF => {
            // spgxlogVacuumLeaf: nDead u16 @0, nPlaceholder u16 @2, nMove u16 @4,
            // nChain u16 @6
            appendf!(
                buf,
                "ndead: {}, nplaceholder: {}, nmove: {}, nchain: {}",
                u16_at(rec, 0),
                u16_at(rec, 2),
                u16_at(rec, 4),
                u16_at(rec, 6)
            );
        }
        XLOG_SPGIST_VACUUM_ROOT => {
            // spgxlogVacuumRoot: nDelete u16 @0
            appendf!(buf, "ndelete: {}", u16_at(rec, 0));
        }
        XLOG_SPGIST_VACUUM_REDIRECT => {
            // spgxlogVacuumRedirect: nToPlaceholder u16 @0, firstPlaceholder u16 @2,
            // snapshotConflictHorizon u32 @4, isCatalogRel bool @8
            appendf!(
                buf,
                "ntoplaceholder: {}, firstplaceholder: {}, snapshotConflictHorizon: {}, isCatalogRel: {}",
                u16_at(rec, 0),
                u16_at(rec, 2),
                u32_at(rec, 4),
                if bool_at(rec, 8) { 'T' } else { 'F' }
            );
        }
        _ => {}
    }
    Ok(())
}

/// `spg_identify(uint8 info)` — `None` where C returns NULL.
pub fn spg_identify(info: u8) -> Option<&'static str> {
    match info & !XLR_INFO_MASK {
        XLOG_SPGIST_ADD_LEAF => Some("ADD_LEAF"),
        XLOG_SPGIST_MOVE_LEAFS => Some("MOVE_LEAFS"),
        XLOG_SPGIST_ADD_NODE => Some("ADD_NODE"),
        XLOG_SPGIST_SPLIT_TUPLE => Some("SPLIT_TUPLE"),
        XLOG_SPGIST_PICKSPLIT => Some("PICKSPLIT"),
        XLOG_SPGIST_VACUUM_LEAF => Some("VACUUM_LEAF"),
        XLOG_SPGIST_VACUUM_ROOT => Some("VACUUM_ROOT"),
        XLOG_SPGIST_VACUUM_REDIRECT => Some("VACUUM_REDIRECT"),
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
        spg_desc(&mut buf, &record).unwrap();
        buf.as_str().to_string()
    }

    #[test]
    fn formats_add_leaf() {
        let mut rec = vec![0u8; 10];
        rec[0] = 1; // newPage
        rec[1] = 1; // storesNulls
        rec[2..4].copy_from_slice(&3u16.to_ne_bytes());
        rec[4..6].copy_from_slice(&4u16.to_ne_bytes());
        rec[6..8].copy_from_slice(&5u16.to_ne_bytes());
        rec[8..10].copy_from_slice(&6u16.to_ne_bytes());
        assert_eq!(
            desc(XLOG_SPGIST_ADD_LEAF, &rec),
            "off: 3, headoff: 4, parentoff: 5, nodeI: 6 (newpage) (nulls)"
        );
    }

    #[test]
    fn formats_vacuum_redirect() {
        let mut rec = vec![0u8; 9];
        rec[0..2].copy_from_slice(&2u16.to_ne_bytes());
        rec[2..4].copy_from_slice(&3u16.to_ne_bytes());
        rec[4..8].copy_from_slice(&44u32.to_ne_bytes());
        rec[8] = 0;
        assert_eq!(
            desc(XLOG_SPGIST_VACUUM_REDIRECT, &rec),
            "ntoplaceholder: 2, firstplaceholder: 3, snapshotConflictHorizon: 44, isCatalogRel: F"
        );
    }

    #[test]
    fn identifies() {
        assert_eq!(spg_identify(XLOG_SPGIST_PICKSPLIT), Some("PICKSPLIT"));
        assert_eq!(spg_identify(0x90), None);
    }
}
