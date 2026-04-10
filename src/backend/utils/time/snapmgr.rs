use std::collections::BTreeSet;

use crate::backend::access::transam::xact::{CommandId, TransactionId, INVALID_TRANSACTION_ID};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    pub current_xid: TransactionId,
    pub current_cid: CommandId,
    pub xmin: TransactionId,
    pub xmax: TransactionId,
    pub(crate) in_progress: BTreeSet<TransactionId>,
}

impl Snapshot {
    pub fn bootstrap() -> Self {
        Self {
            current_xid: INVALID_TRANSACTION_ID,
            current_cid: CommandId::MAX,
            xmin: 1,
            xmax: 1,
            in_progress: BTreeSet::new(),
        }
    }

    pub fn transaction_active_in_snapshot(&self, xid: TransactionId) -> bool {
        xid != INVALID_TRANSACTION_ID
            && xid != self.current_xid
            && xid >= self.xmin
            && xid < self.xmax
            && self.in_progress.contains(&xid)
    }
}
