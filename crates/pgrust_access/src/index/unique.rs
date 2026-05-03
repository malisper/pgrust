use std::collections::BTreeSet;

use pgrust_core::{INVALID_TRANSACTION_ID, RelFileLocator, Snapshot, TransactionId};
use pgrust_nodes::datum::Value;

use crate::access::htup::HeapTuple;
use crate::access::itemptr::ItemPointerData;
use crate::{AccessHeapServices, AccessResult, AccessTransactionServices};

#[derive(Debug, Clone)]
pub struct UniqueCandidateContext {
    pub snapshot: Snapshot,
    pub heap_relation: RelFileLocator,
    pub heap_tid: ItemPointerData,
    pub old_heap_tid: Option<ItemPointerData>,
}

#[derive(Debug, Clone)]
pub enum UniqueCandidateResult {
    NoConflict,
    Conflict(HeapTuple),
    WaitFor(TransactionId),
}

pub fn keys_contain_null(values: &[Value]) -> bool {
    values.iter().any(|value| matches!(value, Value::Null))
}

pub fn classify_unique_candidate(
    ctx: &UniqueCandidateContext,
    tid: ItemPointerData,
    services: &(impl AccessHeapServices + AccessTransactionServices),
) -> AccessResult<UniqueCandidateResult> {
    if tid == ctx.heap_tid || ctx.old_heap_tid == Some(tid) {
        return Ok(UniqueCandidateResult::NoConflict);
    }
    let tuple = services.fetch_heap_tuple(ctx.heap_relation, tid)?;
    let xmin = tuple.header.xmin;
    let xmax = tuple.header.xmax;

    if xmin == INVALID_TRANSACTION_ID {
        return Ok(UniqueCandidateResult::NoConflict);
    }
    if ctx.snapshot.transaction_is_own(xmin)
        && tid_reachable_from_same_transaction_update_chain(ctx, tid, services)?
    {
        return Ok(UniqueCandidateResult::NoConflict);
    }
    if !ctx.snapshot.transaction_is_own(xmin) {
        match services.transaction_status(xmin) {
            Some(pgrust_core::TransactionStatus::Committed) => {}
            Some(pgrust_core::TransactionStatus::Aborted) => {
                return Ok(UniqueCandidateResult::NoConflict);
            }
            Some(pgrust_core::TransactionStatus::InProgress) | None => {
                return Ok(UniqueCandidateResult::WaitFor(xmin));
            }
        }
    }

    if xmax == INVALID_TRANSACTION_ID {
        return Ok(UniqueCandidateResult::Conflict(tuple));
    }
    if ctx.snapshot.transaction_is_own(xmax) {
        return Ok(UniqueCandidateResult::NoConflict);
    }
    match services.transaction_status(xmax) {
        Some(pgrust_core::TransactionStatus::Committed) => Ok(UniqueCandidateResult::NoConflict),
        Some(pgrust_core::TransactionStatus::Aborted) => Ok(UniqueCandidateResult::Conflict(tuple)),
        Some(pgrust_core::TransactionStatus::InProgress) | None => {
            Ok(UniqueCandidateResult::WaitFor(xmax))
        }
    }
}

fn tid_reachable_from_same_transaction_update_chain(
    ctx: &UniqueCandidateContext,
    target_tid: ItemPointerData,
    heap: &impl AccessHeapServices,
) -> AccessResult<bool> {
    if !item_pointer_is_valid(ctx.heap_tid) || !item_pointer_is_valid(target_tid) {
        return Ok(false);
    }

    let mut current_tid = ctx.heap_tid;
    let mut seen = BTreeSet::new();
    for _ in 0..1024 {
        if !item_pointer_is_valid(current_tid) {
            return Ok(false);
        }
        if !seen.insert(current_tid) {
            return Ok(false);
        }
        let tuple = heap.fetch_heap_tuple(ctx.heap_relation, current_tid)?;
        if !ctx.snapshot.transaction_is_own(tuple.header.xmax) || tuple.header.ctid == current_tid {
            return Ok(false);
        }
        if tuple.header.ctid == target_tid {
            return Ok(true);
        }
        current_tid = tuple.header.ctid;
    }
    Ok(false)
}

fn item_pointer_is_valid(tid: ItemPointerData) -> bool {
    tid.offset_number != 0 && tid.block_number != u32::MAX
}
