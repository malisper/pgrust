use parking_lot::RwLock;

use crate::backend::access::transam::xact::{TransactionId, TransactionManager};
use crate::backend::storage::buffer::storage_backend::SmgrStorageBackend;
use pgrust_core::{ClientId, RelFileLocator};
use pgrust_storage::BufferPool;

pub use pgrust_access::heap::vacuumlazy::{VacuumRelationStats, VacuumScanState};

type RootBufferPool = BufferPool<SmgrStorageBackend>;

// :HACK: Compatibility wrapper preserving the old root transaction-manager
// argument while vacuum runtime lives in `pgrust_access`.
pub fn vacuum_relation_scan(
    pool: &RootBufferPool,
    client_id: ClientId,
    rel: RelFileLocator,
    txns: &RwLock<TransactionManager>,
) -> Result<VacuumScanState, crate::backend::access::heap::heapam::HeapError> {
    let txns_guard = txns.read();
    pgrust_access::heap::vacuumlazy::vacuum_relation_scan(pool, client_id, rel, &*txns_guard)
}

// :HACK: Compatibility wrapper preserving the old root transaction-manager
// argument while vacuum runtime lives in `pgrust_access`.
pub fn vacuum_relation_pages(
    pool: &RootBufferPool,
    client_id: ClientId,
    rel: RelFileLocator,
    relation_oid: u32,
    txns: &RwLock<TransactionManager>,
    scan: &VacuumScanState,
    previous_relfrozenxid: Option<TransactionId>,
    truncate: bool,
) -> Result<VacuumRelationStats, crate::backend::access::heap::heapam::HeapError> {
    let txns_guard = txns.read();
    pgrust_access::heap::vacuumlazy::vacuum_relation_pages(
        pool,
        client_id,
        rel,
        relation_oid,
        &*txns_guard,
        scan,
        previous_relfrozenxid,
        truncate,
    )
}

// :HACK: Compatibility wrapper preserving the old root transaction-manager
// argument while vacuum runtime lives in `pgrust_access`.
pub fn vacuum_relation(
    pool: &RootBufferPool,
    client_id: ClientId,
    rel: RelFileLocator,
    relation_oid: u32,
    txns: &RwLock<TransactionManager>,
    previous_relfrozenxid: Option<TransactionId>,
) -> Result<(VacuumScanState, VacuumRelationStats), crate::backend::access::heap::heapam::HeapError>
{
    let scan = vacuum_relation_scan(pool, client_id, rel, txns)?;
    let stats = vacuum_relation_pages(
        pool,
        client_id,
        rel,
        relation_oid,
        txns,
        &scan,
        previous_relfrozenxid,
        true,
    )?;
    Ok((scan, stats))
}
