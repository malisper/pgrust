use std::collections::BTreeSet;

use pgrust_core::{INVALID_TRANSACTION_ID, RelFileLocator, Snapshot, TransactionId};
use pgrust_nodes::datum::Value;

use crate::access::amapi::{IndexBeginScanContext, IndexInsertContext, IndexUniqueCheck};
use crate::access::htup::HeapTuple;
use crate::access::itemptr::ItemPointerData;
use crate::access::relscan::ScanDirection;
use crate::access::scankey::ScanKeyData;
use crate::{AccessError, AccessScalarServices};
use crate::{AccessHeapServices, AccessResult, AccessTransactionServices};

use super::indexam;

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

#[derive(Debug, Clone)]
pub struct UniqueProbeConflict {
    pub tid: ItemPointerData,
    pub tuple: HeapTuple,
}

pub fn keys_contain_null(values: &[Value]) -> bool {
    values.iter().any(|value| matches!(value, Value::Null))
}

pub fn probe_unique_conflict<R>(
    ctx: &IndexInsertContext,
    key_values: &[Value],
    runtime: &R,
    scalar: &dyn AccessScalarServices,
) -> AccessResult<Option<UniqueProbeConflict>>
where
    R: AccessHeapServices + AccessTransactionServices + ?Sized,
{
    let key_count = usize::try_from(ctx.index_meta.indnkeyatts.max(0))
        .unwrap_or_default()
        .min(key_values.len());
    let key_values = &key_values[..key_count];
    if !matches!(ctx.unique_check, IndexUniqueCheck::Yes)
        || (!ctx.index_meta.indnullsnotdistinct && keys_contain_null(key_values))
    {
        return Ok(None);
    }

    loop {
        let begin = IndexBeginScanContext {
            pool: ctx.pool.clone(),
            client_id: ctx.client_id,
            snapshot: ctx.snapshot.clone(),
            heap_relation: ctx.heap_relation,
            index_relation: ctx.index_relation,
            index_desc: ctx.index_desc.clone(),
            index_meta: ctx.index_meta.clone(),
            key_data: key_values
                .iter()
                .enumerate()
                .map(|(idx, value)| ScanKeyData {
                    attribute_number: idx as i16 + 1,
                    strategy: 3,
                    argument: value.clone(),
                })
                .collect(),
            order_by_data: Vec::new(),
            direction: ScanDirection::Forward,
            want_itup: false,
        };
        let mut scan = indexam::index_beginscan(&begin, ctx.index_meta.am_oid, scalar)?;
        let mut wait_for_xid = None;
        while indexam::index_getnext(&mut scan, ctx.index_meta.am_oid, scalar)? {
            let tid = scan
                .xs_heaptid
                .ok_or(AccessError::Corrupt("index scan tuple missing heap tid"))?;
            match classify_unique_candidate_for_insert(ctx, tid, runtime)? {
                UniqueCandidateResult::NoConflict => {}
                UniqueCandidateResult::Conflict(tuple) => {
                    let _ = indexam::index_endscan(scan, ctx.index_meta.am_oid);
                    return Ok(Some(UniqueProbeConflict { tid, tuple }));
                }
                UniqueCandidateResult::WaitFor(xid) => {
                    wait_for_xid = Some(xid);
                    break;
                }
            }
        }
        indexam::index_endscan(scan, ctx.index_meta.am_oid)?;
        let Some(xid) = wait_for_xid else {
            return Ok(None);
        };
        runtime.wait_for_transaction(xid)?;
    }
}

pub fn classify_unique_candidate_for_insert(
    ctx: &IndexInsertContext,
    tid: ItemPointerData,
    services: &(impl AccessHeapServices + AccessTransactionServices + ?Sized),
) -> AccessResult<UniqueCandidateResult> {
    let access_ctx = UniqueCandidateContext {
        snapshot: ctx.snapshot.clone(),
        heap_relation: ctx.heap_relation,
        heap_tid: ctx.heap_tid,
        old_heap_tid: ctx.old_heap_tid,
    };
    classify_unique_candidate(&access_ctx, tid, services)
}

pub fn classify_unique_candidate(
    ctx: &UniqueCandidateContext,
    tid: ItemPointerData,
    services: &(impl AccessHeapServices + AccessTransactionServices + ?Sized),
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
    heap: &(impl AccessHeapServices + ?Sized),
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
