use crate::backend::access::transam::xact::{
    FROZEN_TRANSACTION_ID, TransactionId, TransactionManager, TransactionStatus,
    transaction_id_is_normal,
};
use crate::backend::storage::buffer::Page;
use crate::backend::storage::page::bufpage::{
    ItemIdFlags, page_get_item, page_get_item_id, page_get_max_offset_number,
};
use crate::include::access::htup::{
    HEAP_XMAX_INVALID, HEAP_XMIN_COMMITTED, HEAP_XMIN_INVALID, HeapTuple, TupleError,
    heap_page_replace_tuple,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PruneTupleState {
    Live,
    DeadRemovable,
    DeadNotRemovable,
    AbortedInsertGarbage,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrunePageResult {
    pub removable_offsets: Vec<u16>,
    pub freeze_offsets: Vec<u16>,
    pub all_visible: bool,
    pub all_frozen: bool,
    pub relfrozenxid_candidate: Option<TransactionId>,
}

pub fn classify_page_for_prune(
    page: &Page,
    txns: &TransactionManager,
    oldest_xmin: TransactionId,
    freeze_cutoff_xid: TransactionId,
) -> Result<PrunePageResult, TupleError> {
    let max_offset = page_get_max_offset_number(page)?;
    let mut removable_offsets = Vec::new();
    let mut freeze_offsets = Vec::new();
    let mut all_visible = true;
    let mut all_frozen = true;
    let mut relfrozenxid_candidate = None;

    for off in 1..=max_offset {
        let item_id = page_get_item_id(page, off)?;
        if item_id.lp_flags == ItemIdFlags::Unused || !item_id.has_storage() {
            continue;
        }
        if item_id.lp_flags == ItemIdFlags::Dead {
            all_visible = false;
            all_frozen = false;
            removable_offsets.push(off);
            continue;
        }
        let tuple = HeapTuple::parse(page_get_item(page, off)?)?;
        match classify_tuple(&tuple, txns, oldest_xmin) {
            PruneTupleState::Live => {
                if tuple.header.xmin == FROZEN_TRANSACTION_ID {
                    continue;
                }
                if !tuple_xmin_committed(&tuple, txns) {
                    all_visible = false;
                    all_frozen = false;
                    continue;
                }
                if transaction_id_is_normal(tuple.header.xmin)
                    && tuple.header.xmin < freeze_cutoff_xid
                {
                    freeze_offsets.push(off);
                    all_frozen = false;
                    continue;
                }
                if transaction_id_is_normal(tuple.header.xmin) {
                    relfrozenxid_candidate = Some(
                        relfrozenxid_candidate
                            .map(|current: TransactionId| current.min(tuple.header.xmin))
                            .unwrap_or(tuple.header.xmin),
                    );
                    all_frozen = false;
                }
            }
            PruneTupleState::DeadRemovable => {
                all_visible = false;
                all_frozen = false;
                removable_offsets.push(off);
            }
            PruneTupleState::DeadNotRemovable | PruneTupleState::AbortedInsertGarbage => {
                all_visible = false;
                all_frozen = false;
            }
        }
    }

    Ok(PrunePageResult {
        removable_offsets,
        freeze_offsets,
        all_visible,
        all_frozen: all_visible && all_frozen,
        relfrozenxid_candidate,
    })
}

pub fn freeze_page_tuples(
    page: &mut Page,
    txns: &TransactionManager,
    oldest_xmin: TransactionId,
    freeze_cutoff_xid: TransactionId,
) -> Result<bool, TupleError> {
    let max_offset = page_get_max_offset_number(page)?;
    let mut changed = false;

    for off in 1..=max_offset {
        let item_id = page_get_item_id(page, off)?;
        if item_id.lp_flags != ItemIdFlags::Normal || !item_id.has_storage() {
            continue;
        }
        let mut tuple = HeapTuple::parse(page_get_item(page, off)?)?;
        if classify_tuple(&tuple, txns, oldest_xmin) != PruneTupleState::Live {
            continue;
        }
        if !tuple_xmin_committed(&tuple, txns)
            || !transaction_id_is_normal(tuple.header.xmin)
            || tuple.header.xmin >= freeze_cutoff_xid
        {
            continue;
        }
        tuple.header.xmin = FROZEN_TRANSACTION_ID;
        tuple.header.infomask |= HEAP_XMIN_COMMITTED;
        tuple.header.infomask &= !HEAP_XMIN_INVALID;
        heap_page_replace_tuple(page, off, &tuple)?;
        changed = true;
    }

    Ok(changed)
}

pub fn classify_tuple(
    tuple: &HeapTuple,
    txns: &TransactionManager,
    oldest_xmin: TransactionId,
) -> PruneTupleState {
    if tuple_xmin_aborted(tuple, txns) {
        return PruneTupleState::AbortedInsertGarbage;
    }
    if !tuple_xmin_committed(tuple, txns) {
        return PruneTupleState::DeadNotRemovable;
    }
    if tuple_xmax_invalid(tuple, txns) {
        return PruneTupleState::Live;
    }
    match txns.status(tuple.header.xmax) {
        Some(TransactionStatus::Committed) if tuple.header.xmax < oldest_xmin => {
            PruneTupleState::DeadRemovable
        }
        _ => PruneTupleState::DeadNotRemovable,
    }
}

fn tuple_xmin_committed(tuple: &HeapTuple, txns: &TransactionManager) -> bool {
    tuple.header.xmin == FROZEN_TRANSACTION_ID
        || tuple.header.infomask & HEAP_XMIN_COMMITTED != 0
        || matches!(
            txns.status(tuple.header.xmin),
            Some(TransactionStatus::Committed)
        )
}

fn tuple_xmin_aborted(tuple: &HeapTuple, txns: &TransactionManager) -> bool {
    tuple.header.infomask & HEAP_XMIN_INVALID != 0
        || matches!(txns.status(tuple.header.xmin), Some(TransactionStatus::Aborted))
}

fn tuple_xmax_invalid(tuple: &HeapTuple, txns: &TransactionManager) -> bool {
    tuple.header.xmax == 0
        || tuple.header.infomask & HEAP_XMAX_INVALID != 0
        || matches!(txns.status(tuple.header.xmax), Some(TransactionStatus::Aborted))
}
