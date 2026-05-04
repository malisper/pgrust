use pgrust_access::ItemPointerData;
use pgrust_access::transam::xact::{Snapshot, TransactionId};
use pgrust_catalog_store::CatalogMutationEffect;
use pgrust_nodes::Value;
use pgrust_storage::RelFileLocator;
use pgrust_storage::lmgr::{RowLockMode, RowLockOwner, SerializableXactId};

pub trait LockStatusProvider: Send + Sync {
    fn pg_lock_status_rows(&self, current_client_id: u32) -> Vec<Vec<Value>>;
    fn pg_blocking_pids(&self, blocked_pid: u32) -> Vec<u32>;
}

pub trait ExecutorTransactionServices {
    type Error;

    fn transaction_xid(&self) -> Option<TransactionId>;
    fn write_snapshot(&self) -> Snapshot;
    fn uses_transaction_snapshot(&self) -> bool;
    fn ensure_write_xid(&mut self) -> Result<TransactionId, Self::Error>;
}

pub trait ExecutorRowLockServices {
    type Error;

    fn row_lock_owner(&self) -> RowLockOwner;
    fn acquire_row_lock(
        &self,
        relation_oid: u32,
        tid: ItemPointerData,
        mode: RowLockMode,
    ) -> Result<(), Self::Error>;
    fn try_acquire_row_lock(
        &self,
        relation_oid: u32,
        tid: ItemPointerData,
        mode: RowLockMode,
    ) -> bool;
}

pub trait ExecutorPredicateLockServices {
    type Error;

    fn serializable_xact_id(&self) -> Option<SerializableXactId>;
    fn predicate_lock_relation(&self, relation_oid: u32) -> Result<(), Self::Error>;
    fn predicate_lock_page(&self, relation_oid: u32, block_number: u32) -> Result<(), Self::Error>;
    fn predicate_lock_tuple(
        &self,
        relation_oid: u32,
        tid: ItemPointerData,
    ) -> Result<(), Self::Error>;
    fn check_serializable_visible_tuple_xmax(
        &self,
        xmax: Option<TransactionId>,
    ) -> Result<(), Self::Error>;
    fn check_serializable_write_relation(&self, relation_oid: u32) -> Result<(), Self::Error>;
    fn check_serializable_write_tuple(
        &self,
        relation_oid: u32,
        tid: ItemPointerData,
    ) -> Result<(), Self::Error>;
}

pub trait ExecutorMutationSink {
    fn record_catalog_effect(&mut self, effect: CatalogMutationEffect);
    fn record_table_lock(&mut self, rel: RelFileLocator);
}
