use std::sync::Arc;

use pgrust_access::transam::xact::{CommandId, Snapshot, TransactionId};
use pgrust_storage::lmgr::SerializableXactId;

#[derive(Debug)]
pub struct ExecutorTransactionState {
    pub xid: Option<TransactionId>,
    pub cid: CommandId,
    pub transaction_snapshot: Option<Snapshot>,
    pub serializable_xact: Option<SerializableXactId>,
}

pub type SharedExecutorTransactionState = Arc<parking_lot::Mutex<ExecutorTransactionState>>;
