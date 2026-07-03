use crate::{appendf, rec_data, rec_info, XLR_INFO_MASK};
use stringinfo::StringInfo;
use types_error::PgResult;
use types_spgist::xlog::*;
use xlogreader_seams::XLogReaderState;

pub fn spg_desc(buf: &mut StringInfo<'_>, record: &XLogReaderState) -> PgResult<()> {
    let rec = rec_data(record);
    let info = rec_info(record) & !XLR_INFO_MASK;

    match info {
        XLOG_SPGIST_ADD_LEAF => {
            let x = spgxlogAddLeaf::decode(rec);
            appendf!(
                buf,
                "off: {}, headoff: {}, parentoff: {}, nodeI: {}",
                x.offnumLeaf,
                x.offnumHeadLeaf,
                x.offnumParent,
                x.nodeI
            )?;
            if x.newPage {
                buf.append_str(" (newpage)")?;
            }
            if x.storesNulls {
                buf.append_str(" (nulls)")?;
            }
        }
        XLOG_SPGIST_MOVE_LEAFS => {
            let x = spgxlogMoveLeafs::decode(rec);
            appendf!(
                buf,
                "nmoves: {}, parentoff: {}, nodeI: {}",
                x.nMoves,
                x.offnumParent,
                x.nodeI
            )?;
            if x.newPage {
                buf.append_str(" (newpage)")?;
            }
            if x.replaceDead {
                buf.append_str(" (replacedead)")?;
            }
            if x.storesNulls {
                buf.append_str(" (nulls)")?;
            }
        }
        XLOG_SPGIST_ADD_NODE => {
            let x = spgxlogAddNode::decode(rec);
            appendf!(
                buf,
                "off: {}, newoff: {}, parentBlk: {}, parentoff: {}, nodeI: {}",
                x.offnum,
                x.offnumNew,
                x.parentBlk,
                x.offnumParent,
                x.nodeI
            )?;
            if x.newPage {
                buf.append_str(" (newpage)")?;
            }
        }
        XLOG_SPGIST_SPLIT_TUPLE => {
            let x = spgxlogSplitTuple::decode(rec);
            appendf!(
                buf,
                "prefixoff: {}, postfixoff: {}",
                x.offnumPrefix,
                x.offnumPostfix
            )?;
            if x.newPage {
                buf.append_str(" (newpage)")?;
            }
            if x.postfixBlkSame {
                buf.append_str(" (same)")?;
            }
        }
        XLOG_SPGIST_PICKSPLIT => {
            let x = spgxlogPickSplit::decode(rec);
            appendf!(
                buf,
                "ndelete: {}, ninsert: {}, inneroff: {}, parentoff: {}, nodeI: {}",
                x.nDelete,
                x.nInsert,
                x.offnumInner,
                x.offnumParent,
                x.nodeI
            )?;
            if x.innerIsParent {
                buf.append_str(" (innerIsParent)")?;
            }
            if x.storesNulls {
                buf.append_str(" (nulls)")?;
            }
            if x.isRootSplit {
                buf.append_str(" (isRootSplit)")?;
            }
        }
        XLOG_SPGIST_VACUUM_LEAF => {
            let x = spgxlogVacuumLeaf::decode(rec);
            appendf!(
                buf,
                "ndead: {}, nplaceholder: {}, nmove: {}, nchain: {}",
                x.nDead,
                x.nPlaceholder,
                x.nMove,
                x.nChain
            )?;
        }
        XLOG_SPGIST_VACUUM_ROOT => {
            let x = spgxlogVacuumRoot::decode(rec);
            appendf!(buf, "ndelete: {}", x.nDelete)?;
        }
        XLOG_SPGIST_VACUUM_REDIRECT => {
            let x = spgxlogVacuumRedirect::decode(rec);
            appendf!(
                buf,
                "ntoplaceholder: {}, firstplaceholder: {}, snapshotConflictHorizon: {}, isCatalogRel: {}",
                x.nToPlaceholder,
                x.firstPlaceholder,
                x.snapshotConflictHorizon,
                if x.isCatalogRel { 'T' } else { 'F' }
            )?;
        }
        _ => {}
    }
    Ok(())
}

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
