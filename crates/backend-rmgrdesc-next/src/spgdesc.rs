//! `access/rmgrdesc/spgdesc.c` — rmgr descriptor routines for SP-GiST indexes.

use crate::appendf;
use mcx::PgString;
use types_error::PgResult;
use types_wal::{DecodedXLogRecord, XLR_INFO_MASK};
use types_xlog_records::spgxlog::{spgxlogAddLeaf, spgxlogAddNode, spgxlogMoveLeafs,
                                  spgxlogPickSplit, spgxlogSplitTuple, spgxlogVacuumLeaf,
                                  spgxlogVacuumRedirect, spgxlogVacuumRoot};

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
pub fn spg_desc(buf: &mut PgString<'_>, record: &DecodedXLogRecord<'_>) -> PgResult<()> {
    let rec = record.data();
    let info = record.info() & !XLR_INFO_MASK;

    match info {
        XLOG_SPGIST_ADD_LEAF => {
            let xlrec = spgxlogAddLeaf::from_bytes(rec);
            appendf!(
                buf,
                "off: {}, headoff: {}, parentoff: {}, nodeI: {}",
                xlrec.offnumLeaf,
                xlrec.offnumHeadLeaf,
                xlrec.offnumParent,
                xlrec.nodeI
            );
            if xlrec.newPage {
                buf.try_push_str(" (newpage)")?;
            }
            if xlrec.storesNulls {
                buf.try_push_str(" (nulls)")?;
            }
        }
        XLOG_SPGIST_MOVE_LEAFS => {
            let xlrec = spgxlogMoveLeafs::from_bytes(rec);
            appendf!(
                buf,
                "nmoves: {}, parentoff: {}, nodeI: {}",
                xlrec.nMoves,
                xlrec.offnumParent,
                xlrec.nodeI
            );
            if xlrec.newPage {
                buf.try_push_str(" (newpage)")?;
            }
            if xlrec.replaceDead {
                buf.try_push_str(" (replacedead)")?;
            }
            if xlrec.storesNulls {
                buf.try_push_str(" (nulls)")?;
            }
        }
        XLOG_SPGIST_ADD_NODE => {
            let xlrec = spgxlogAddNode::from_bytes(rec);
            appendf!(
                buf,
                "off: {}, newoff: {}, parentBlk: {}, parentoff: {}, nodeI: {}",
                xlrec.offnum,
                xlrec.offnumNew,
                xlrec.parentBlk,
                xlrec.offnumParent,
                xlrec.nodeI
            );
            if xlrec.newPage {
                buf.try_push_str(" (newpage)")?;
            }
        }
        XLOG_SPGIST_SPLIT_TUPLE => {
            let xlrec = spgxlogSplitTuple::from_bytes(rec);
            appendf!(
                buf,
                "prefixoff: {}, postfixoff: {}",
                xlrec.offnumPrefix,
                xlrec.offnumPostfix
            );
            if xlrec.newPage {
                buf.try_push_str(" (newpage)")?;
            }
            if xlrec.postfixBlkSame {
                buf.try_push_str(" (same)")?;
            }
        }
        XLOG_SPGIST_PICKSPLIT => {
            let xlrec = spgxlogPickSplit::from_bytes(rec);
            appendf!(
                buf,
                "ndelete: {}, ninsert: {}, inneroff: {}, parentoff: {}, nodeI: {}",
                xlrec.nDelete,
                xlrec.nInsert,
                xlrec.offnumInner,
                xlrec.offnumParent,
                xlrec.nodeI
            );
            if xlrec.innerIsParent {
                buf.try_push_str(" (innerIsParent)")?;
            }
            if xlrec.storesNulls {
                buf.try_push_str(" (nulls)")?;
            }
            if xlrec.isRootSplit {
                buf.try_push_str(" (isRootSplit)")?;
            }
        }
        XLOG_SPGIST_VACUUM_LEAF => {
            let xlrec = spgxlogVacuumLeaf::from_bytes(rec);
            appendf!(
                buf,
                "ndead: {}, nplaceholder: {}, nmove: {}, nchain: {}",
                xlrec.nDead,
                xlrec.nPlaceholder,
                xlrec.nMove,
                xlrec.nChain
            );
        }
        XLOG_SPGIST_VACUUM_ROOT => {
            let xlrec = spgxlogVacuumRoot::from_bytes(rec);
            appendf!(buf, "ndelete: {}", xlrec.nDelete);
        }
        XLOG_SPGIST_VACUUM_REDIRECT => {
            let xlrec = spgxlogVacuumRedirect::from_bytes(rec);
            appendf!(
                buf,
                "ntoplaceholder: {}, firstplaceholder: {}, snapshotConflictHorizon: {}, isCatalogRel: {}",
                xlrec.nToPlaceholder,
                xlrec.firstPlaceholder,
                xlrec.snapshotConflictHorizon,
                if xlrec.isCatalogRel { 'T' } else { 'F' }
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
    use crate::test_support::record;
    use mcx::MemoryContext;

    fn desc(info: u8, data: &[u8]) -> String {
        let ctx = MemoryContext::new("test");
        let mut buf = PgString::new_in(ctx.mcx());
        let record = record(ctx.mcx(), info, data, &[]);
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
