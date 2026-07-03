//! C rd_smgr->smgr_targblock (RelationGet/SetTargetBlock), kept in a
//! backend-local table until smgr wiring lands; shared by heapam's hio and
//! brin_pageops.
use core::cell::RefCell;

use types_core::{BlockNumber, InvalidBlockNumber, Oid};
use types_rel::RelationData;

std::thread_local! {
    static TARGET_BLOCKS: RefCell<Vec<(Oid, BlockNumber)>> = const { RefCell::new(Vec::new()) };
}

pub fn relation_get_target_block(rel: &RelationData<'_>) -> BlockNumber {
    TARGET_BLOCKS.with(|t| {
        t.borrow()
            .iter()
            .find(|(oid, _)| *oid == rel.rd_id)
            .map_or(InvalidBlockNumber, |(_, b)| *b)
    })
}

pub fn relation_set_target_block(rel: &RelationData<'_>, blk: BlockNumber) {
    TARGET_BLOCKS.with(|t| {
        let mut v = t.borrow_mut();
        match v.iter_mut().find(|(oid, _)| *oid == rel.rd_id) {
            Some(slot) => slot.1 = blk,
            None => v.push((rel.rd_id, blk)),
        }
    })
}
