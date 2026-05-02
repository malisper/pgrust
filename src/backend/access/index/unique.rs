use crate::backend::access::heap::heapam::heap_fetch;
use crate::backend::access::index::indexam;
use crate::backend::access::transam::xact::{
    INVALID_TRANSACTION_ID, TransactionId, TransactionStatus,
};
use crate::backend::catalog::CatalogError;
use crate::include::access::amapi::{IndexInsertContext, IndexUniqueCheck};
use crate::include::access::htup::HeapTuple;
use crate::include::access::itemptr::ItemPointerData;
use crate::include::access::relscan::ScanDirection;
use crate::include::access::scankey::ScanKeyData;
use crate::include::nodes::datum::Value;

#[derive(Debug, Clone)]
pub struct UniqueProbeConflict {
    pub tid: ItemPointerData,
    pub tuple: HeapTuple,
}

pub fn probe_unique_conflict(
    ctx: &IndexInsertContext,
    key_values: &[Value],
) -> Result<Option<UniqueProbeConflict>, CatalogError> {
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
        let begin = crate::include::access::amapi::IndexBeginScanContext {
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
        let mut scan = indexam::index_beginscan(&begin, ctx.index_meta.am_oid)?;
        let mut wait_for_xid = None;
        while indexam::index_getnext(&mut scan, ctx.index_meta.am_oid)? {
            let tid = scan
                .xs_heaptid
                .ok_or(CatalogError::Corrupt("index scan tuple missing heap tid"))?;
            match classify_unique_candidate(ctx, tid)? {
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
        let waiter = ctx.txn_waiter.as_ref().ok_or_else(|| {
            CatalogError::Io("btree unique check missing transaction waiter".into())
        })?;
        match waiter.wait_for(&ctx.txns, xid, ctx.client_id, ctx.interrupts.as_ref()) {
            crate::backend::storage::lmgr::WaitOutcome::Completed => {}
            crate::backend::storage::lmgr::WaitOutcome::DeadlockTimeout => {
                return Err(CatalogError::Io(format!(
                    "btree unique check timed out waiting for transaction {xid}"
                )));
            }
            crate::backend::storage::lmgr::WaitOutcome::Interrupted(reason) => {
                return Err(CatalogError::Interrupted(reason));
            }
        }
    }
}

pub(crate) fn keys_contain_null(values: &[Value]) -> bool {
    values.iter().any(|value| matches!(value, Value::Null))
}

pub(crate) enum UniqueCandidateResult {
    NoConflict,
    Conflict(HeapTuple),
    WaitFor(TransactionId),
}

fn tid_reachable_from_same_transaction_update_chain(
    ctx: &IndexInsertContext,
    target_tid: ItemPointerData,
) -> Result<bool, CatalogError> {
    if !item_pointer_is_valid(ctx.heap_tid) || !item_pointer_is_valid(target_tid) {
        return Ok(false);
    }

    let mut current_tid = ctx.heap_tid;
    let mut seen = std::collections::BTreeSet::new();
    for _ in 0..1024 {
        if !item_pointer_is_valid(current_tid) {
            return Ok(false);
        }
        if !seen.insert(current_tid) {
            return Ok(false);
        }
        let tuple = heap_fetch(&ctx.pool, ctx.client_id, ctx.heap_relation, current_tid)
            .map_err(|err| CatalogError::Io(format!("heap unique chain probe failed: {err:?}")))?;
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

pub(crate) fn classify_unique_candidate(
    ctx: &IndexInsertContext,
    tid: ItemPointerData,
) -> Result<UniqueCandidateResult, CatalogError> {
    if tid == ctx.heap_tid || ctx.old_heap_tid == Some(tid) {
        return Ok(UniqueCandidateResult::NoConflict);
    }
    let tuple = heap_fetch(&ctx.pool, ctx.client_id, ctx.heap_relation, tid)
        .map_err(|err| CatalogError::Io(format!("heap unique probe failed: {err:?}")))?;
    let txns = ctx.txns.read();
    let xmin = tuple.header.xmin;
    let xmax = tuple.header.xmax;

    if xmin == INVALID_TRANSACTION_ID {
        return Ok(UniqueCandidateResult::NoConflict);
    }
    if ctx.snapshot.transaction_is_own(xmin)
        && tid_reachable_from_same_transaction_update_chain(ctx, tid)?
    {
        return Ok(UniqueCandidateResult::NoConflict);
    }
    if !ctx.snapshot.transaction_is_own(xmin) {
        match txns.status(xmin) {
            Some(TransactionStatus::Committed) => {}
            Some(TransactionStatus::Aborted) => return Ok(UniqueCandidateResult::NoConflict),
            Some(TransactionStatus::InProgress) | None => {
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
    match txns.status(xmax) {
        Some(TransactionStatus::Committed) => Ok(UniqueCandidateResult::NoConflict),
        Some(TransactionStatus::Aborted) => Ok(UniqueCandidateResult::Conflict(tuple)),
        Some(TransactionStatus::InProgress) | None => Ok(UniqueCandidateResult::WaitFor(xmax)),
    }
}
