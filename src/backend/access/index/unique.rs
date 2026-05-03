use pgrust_access::AccessTransactionServices;
use pgrust_access::index::unique as access_unique;

use crate::backend::access::RootAccessRuntime;
use crate::backend::access::index::buildkeys::map_access_error;
use crate::backend::access::index::indexam;
use crate::backend::catalog::CatalogError;
use crate::include::access::amapi::{IndexInsertContext, IndexUniqueCheck};
use crate::include::access::htup::HeapTuple;
use crate::include::access::itemptr::ItemPointerData;
use crate::include::access::relscan::ScanDirection;
use crate::include::access::scankey::ScanKeyData;
use crate::include::nodes::datum::Value;

pub(crate) use access_unique::UniqueCandidateResult;

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
        || (!ctx.index_meta.indnullsnotdistinct && access_unique::keys_contain_null(key_values))
    {
        return Ok(None);
    }
    let access_runtime = RootAccessRuntime {
        pool: Some(&ctx.pool),
        txns: Some(&ctx.txns),
        txn_waiter: ctx.txn_waiter.as_deref(),
        interrupts: Some(ctx.interrupts.as_ref()),
        client_id: ctx.client_id,
    };
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
            match classify_unique_candidate(ctx, tid, &access_runtime)? {
                access_unique::UniqueCandidateResult::NoConflict => {}
                access_unique::UniqueCandidateResult::Conflict(tuple) => {
                    let _ = indexam::index_endscan(scan, ctx.index_meta.am_oid);
                    return Ok(Some(UniqueProbeConflict { tid, tuple }));
                }
                access_unique::UniqueCandidateResult::WaitFor(xid) => {
                    wait_for_xid = Some(xid);
                    break;
                }
            }
        }
        indexam::index_endscan(scan, ctx.index_meta.am_oid)?;
        let Some(xid) = wait_for_xid else {
            return Ok(None);
        };
        access_runtime
            .wait_for_transaction(xid)
            .map_err(map_access_error)?;
    }
}

pub(crate) fn classify_unique_candidate(
    ctx: &IndexInsertContext,
    tid: ItemPointerData,
    services: &RootAccessRuntime<'_>,
) -> Result<access_unique::UniqueCandidateResult, CatalogError> {
    let access_ctx = access_unique::UniqueCandidateContext {
        snapshot: ctx.snapshot.clone(),
        heap_relation: ctx.heap_relation,
        heap_tid: ctx.heap_tid,
        old_heap_tid: ctx.old_heap_tid,
    };
    access_unique::classify_unique_candidate(&access_ctx, tid, services).map_err(map_access_error)
}
