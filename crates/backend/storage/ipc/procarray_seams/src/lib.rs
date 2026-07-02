use types_core::{ProcNumber, TransactionId};
use types_error::PgResult;

seam_core::seam!(
    pub fn proc_array_add(procno: ProcNumber) -> PgResult<()>
);

seam_core::seam!(
    pub fn proc_array_remove(procno: ProcNumber, latest_xid: TransactionId) -> PgResult<()>
);

seam_core::seam!(
    pub fn transaction_id_is_in_progress(xid: TransactionId) -> PgResult<bool>
);

seam_core::seam!(
    pub fn proc_array_clear_transaction() -> types_error::PgResult<()>
);

seam_core::seam!(
    pub fn xid_cache_remove_running_xids<'a>(
        xid: types_core::TransactionId,
        children: &'a [types_core::TransactionId],
        latest_xid: types_core::TransactionId,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    pub fn record_known_assigned_transaction_ids(xid: types_core::TransactionId) -> types_error::PgResult<()>
);

seam_core::seam!(
    pub fn expire_tree_known_assigned_transaction_ids<'a>(
        xid: types_core::TransactionId,
        subxids: &'a [types_core::TransactionId],
        max_xid: types_core::TransactionId,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    pub fn proc_array_apply_xid_assignment<'a>(
        top_xid: types_core::TransactionId,
        subxids: &'a [types_core::TransactionId],
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    // GlobalVisTestFor(rel) (procarray.c).
    pub fn global_vis_test_for<'a, 'mcx>(
        rel: &'a types_rel::RelationData<'mcx>,
    ) -> types_core::GlobalVisStateHandle
);

seam_core::seam!(
    // ProcArrayEndTransaction(MyProc, latestXid) (procarray.c).
    pub fn proc_array_end_transaction(procno: ProcNumber, latest_xid: TransactionId) -> PgResult<()>
);

seam_core::seam!(
    // GlobalVisTestIsRemovableXid(vistest, xid) (procarray.c).
    pub fn global_vis_test_is_removable_xid(
        vistest: types_core::GlobalVisStateHandle,
        xid: TransactionId,
    ) -> PgResult<bool>
);
