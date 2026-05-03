use parking_lot::RwLock;

use crate::backend::access::services::RootAccessRuntime;
use crate::backend::access::transam::xact::TransactionManager;
use crate::backend::storage::buffer::storage_backend::SmgrStorageBackend;
use crate::pgrust::database::TransactionWaiter;
use pgrust_access::heap::heapam as access_heapam;
use pgrust_core::{ClientId, CommandId, RelFileLocator, Snapshot, TransactionId};

pub use pgrust_access::access::htup::{HeapTuple, ItemPointerData};
pub use pgrust_access::heap::HeapWalPolicy;
pub use pgrust_access::heap::heapam::{
    HeapError, HeapModifyResult, HeapScan, VisibleHeapScan, heap_fetch, heap_flush, heap_insert,
    heap_insert_mvcc, heap_insert_mvcc_with_cid, heap_insert_mvcc_with_cid_and_fillfactor,
    heap_scan_begin, heap_scan_begin_visible, heap_scan_end, heap_scan_next,
    heap_scan_page_next_tuple,
};

type BufferPool = crate::BufferPool<SmgrStorageBackend>;
type WaiterArgs<'a> = Option<(
    &'a RwLock<TransactionManager>,
    &'a TransactionWaiter,
    &'a pgrust_core::InterruptState,
)>;

// :HACK: Compatibility wrapper preserving the old root transaction-manager
// argument while the heap runtime lives in `pgrust_access`.
pub fn heap_scan_next_visible(
    pool: &BufferPool,
    client_id: ClientId,
    txns: &TransactionManager,
    scan: &mut VisibleHeapScan,
) -> Result<Option<(ItemPointerData, HeapTuple)>, HeapError> {
    access_heapam::heap_scan_next_visible(pool, client_id, txns, scan)
}

// :HACK: Compatibility wrapper preserving the old root transaction-manager
// argument while the heap runtime lives in `pgrust_access`.
pub fn heap_scan_next_visible_raw<T, E: From<HeapError>>(
    pool: &BufferPool,
    client_id: ClientId,
    txns: &std::sync::Arc<RwLock<TransactionManager>>,
    scan: &mut VisibleHeapScan,
    process: impl FnMut(ItemPointerData, &[u8]) -> Result<T, E>,
) -> Result<Option<T>, E> {
    let txns_guard = txns.read();
    access_heapam::heap_scan_next_visible_raw(pool, client_id, &*txns_guard, scan, process)
}

// :HACK: Compatibility wrapper preserving the old root transaction-manager
// argument while the heap runtime lives in `pgrust_access`.
pub fn heap_scan_prepare_next_page<E: From<HeapError>>(
    pool: &BufferPool,
    client_id: ClientId,
    txns: &std::sync::Arc<RwLock<TransactionManager>>,
    scan: &mut VisibleHeapScan,
) -> Result<Option<usize>, E> {
    let txns_guard = txns.read();
    access_heapam::heap_scan_prepare_next_page(pool, client_id, &*txns_guard, scan)
}

// :HACK: Compatibility wrapper preserving the old root transaction-manager
// argument while the heap runtime lives in `pgrust_access`.
pub fn heap_scan_prepare_page_at<E: From<HeapError>>(
    pool: &BufferPool,
    client_id: ClientId,
    txns: &std::sync::Arc<RwLock<TransactionManager>>,
    scan: &mut VisibleHeapScan,
    block: u32,
) -> Result<Option<usize>, E> {
    let txns_guard = txns.read();
    access_heapam::heap_scan_prepare_page_at(pool, client_id, &*txns_guard, scan, block)
}

// :HACK: Compatibility wrapper preserving the old root transaction-manager
// argument while the heap runtime lives in `pgrust_access`.
pub fn heap_scan_all_visible_raw<E: From<HeapError>>(
    pool: &BufferPool,
    client_id: ClientId,
    txns: &std::sync::Arc<RwLock<TransactionManager>>,
    scan: &mut VisibleHeapScan,
    process: impl FnMut(&[u8]) -> Result<(), E>,
) -> Result<usize, E> {
    let txns_guard = txns.read();
    access_heapam::heap_scan_all_visible_raw(pool, client_id, &*txns_guard, scan, process)
}

// :HACK: Compatibility wrapper preserving the old root transaction-manager
// argument while the heap runtime lives in `pgrust_access`.
pub fn heap_fetch_visible(
    pool: &BufferPool,
    client_id: ClientId,
    rel: RelFileLocator,
    tid: ItemPointerData,
    txns: &TransactionManager,
    snapshot: &Snapshot,
) -> Result<Option<HeapTuple>, HeapError> {
    access_heapam::heap_fetch_visible(pool, client_id, rel, tid, txns, snapshot)
}

// :HACK: Compatibility wrapper preserving the old root transaction-manager
// argument while the heap runtime lives in `pgrust_access`.
pub fn heap_fetch_visible_with_txns(
    pool: &BufferPool,
    client_id: ClientId,
    rel: RelFileLocator,
    tid: ItemPointerData,
    txns: &RwLock<TransactionManager>,
    snapshot: &Snapshot,
) -> Result<Option<HeapTuple>, HeapError> {
    let txns_guard = txns.read();
    access_heapam::heap_fetch_visible(pool, client_id, rel, tid, &*txns_guard, snapshot)
}

// :HACK: Compatibility wrapper preserving the old root transaction-manager
// argument while the heap runtime lives in `pgrust_access`.
pub fn heap_delete(
    pool: &BufferPool,
    client_id: ClientId,
    rel: RelFileLocator,
    txns: &TransactionManager,
    xid: TransactionId,
    tid: ItemPointerData,
) -> Result<(), HeapError> {
    access_heapam::heap_delete(pool, client_id, rel, txns, xid, tid)
}

// :HACK: Compatibility wrapper preserving the old root transaction-manager
// argument while the heap runtime lives in `pgrust_access`.
pub fn heap_update(
    pool: &BufferPool,
    client_id: ClientId,
    rel: RelFileLocator,
    txns: &TransactionManager,
    xid: TransactionId,
    tid: ItemPointerData,
    replacement: &HeapTuple,
) -> Result<ItemPointerData, HeapError> {
    access_heapam::heap_update(pool, client_id, rel, txns, xid, tid, replacement)
}

// :HACK: Compatibility wrapper preserving the old root transaction-manager
// argument while the heap runtime lives in `pgrust_access`.
pub fn heap_update_with_cid(
    pool: &BufferPool,
    client_id: ClientId,
    rel: RelFileLocator,
    txns: &TransactionManager,
    xid: TransactionId,
    cid: CommandId,
    tid: ItemPointerData,
    replacement: &HeapTuple,
) -> Result<ItemPointerData, HeapError> {
    access_heapam::heap_update_with_cid(pool, client_id, rel, txns, xid, cid, tid, replacement)
}

// :HACK: Compatibility wrapper preserving the old root waiter tuple while the
// heap runtime lives in `pgrust_access`.
pub fn heap_delete_with_waiter(
    pool: &BufferPool,
    client_id: ClientId,
    rel: RelFileLocator,
    txns: &RwLock<TransactionManager>,
    xid: TransactionId,
    tid: ItemPointerData,
    snapshot: &Snapshot,
    waiter: WaiterArgs<'_>,
) -> Result<(), HeapError> {
    heap_delete_with_waiter_with_wal_policy(
        pool,
        client_id,
        rel,
        txns,
        xid,
        tid,
        snapshot,
        waiter,
        HeapWalPolicy::Wal,
    )
}

// :HACK: Compatibility wrapper preserving the old root waiter tuple while the
// heap runtime lives in `pgrust_access`.
pub fn heap_delete_with_waiter_with_wal_policy(
    pool: &BufferPool,
    client_id: ClientId,
    rel: RelFileLocator,
    txns: &RwLock<TransactionManager>,
    xid: TransactionId,
    tid: ItemPointerData,
    snapshot: &Snapshot,
    waiter: WaiterArgs<'_>,
    wal_policy: HeapWalPolicy,
) -> Result<(), HeapError> {
    let txns_runtime = RootAccessRuntime::transaction_only(txns, None, None, client_id);
    let runtime;
    let waiter_services: Option<&dyn pgrust_access::AccessTransactionServices> =
        if let Some((txns_lock, txn_waiter, interrupts)) = waiter {
            runtime = RootAccessRuntime::transaction_only(
                txns_lock,
                Some(txn_waiter),
                Some(interrupts),
                client_id,
            );
            Some(&runtime)
        } else {
            None
        };
    access_heapam::heap_delete_with_waiter_with_wal_policy(
        pool,
        client_id,
        rel,
        &txns_runtime,
        xid,
        tid,
        snapshot,
        waiter_services,
        wal_policy,
    )
}

// :HACK: Compatibility wrapper preserving the old root waiter tuple while the
// heap runtime lives in `pgrust_access`.
pub fn heap_update_with_waiter(
    pool: &BufferPool,
    client_id: ClientId,
    rel: RelFileLocator,
    txns: &RwLock<TransactionManager>,
    xid: TransactionId,
    cid: CommandId,
    tid: ItemPointerData,
    replacement: &HeapTuple,
    waiter: WaiterArgs<'_>,
) -> Result<ItemPointerData, HeapError> {
    let snapshot = txns.read().snapshot_for_command(xid, cid)?;
    heap_update_with_waiter_with_snapshot(
        pool,
        client_id,
        rel,
        txns,
        xid,
        cid,
        tid,
        replacement,
        &snapshot,
        waiter,
    )
}

// :HACK: Compatibility wrapper preserving the old root waiter tuple while the
// heap runtime lives in `pgrust_access`.
pub fn heap_update_with_waiter_with_snapshot(
    pool: &BufferPool,
    client_id: ClientId,
    rel: RelFileLocator,
    txns: &RwLock<TransactionManager>,
    xid: TransactionId,
    cid: CommandId,
    tid: ItemPointerData,
    replacement: &HeapTuple,
    snapshot: &Snapshot,
    waiter: WaiterArgs<'_>,
) -> Result<ItemPointerData, HeapError> {
    let txns_runtime = RootAccessRuntime::transaction_only(txns, None, None, client_id);
    let runtime;
    let waiter_services: Option<&dyn pgrust_access::AccessTransactionServices> =
        if let Some((txns_lock, txn_waiter, interrupts)) = waiter {
            runtime = RootAccessRuntime::transaction_only(
                txns_lock,
                Some(txn_waiter),
                Some(interrupts),
                client_id,
            );
            Some(&runtime)
        } else {
            None
        };
    access_heapam::heap_update_with_waiter_with_snapshot(
        pool,
        client_id,
        rel,
        &txns_runtime,
        xid,
        cid,
        tid,
        replacement,
        snapshot,
        waiter_services,
    )
}
