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
    pub fn global_vis_test_for<'a, 'mcx>(
        rel: &'a types_rel::RelationData<'mcx>,
    ) -> types_core::GlobalVisStateHandle
);

seam_core::seam!(
    pub fn proc_array_end_transaction(procno: ProcNumber, latest_xid: TransactionId) -> PgResult<()>
);

seam_core::seam!(
    pub fn global_vis_test_is_removable_xid(
        vistest: types_core::GlobalVisStateHandle,
        xid: TransactionId,
    ) -> PgResult<bool>
);

seam_core::seam!(
    pub fn global_vis_check_removable_full_xid<'a, 'mcx>(
        rel: &'a types_rel::RelationData<'mcx>,
        fxid: types_core::xact::FullTransactionId,
    ) -> PgResult<bool>
);

seam_core::seam!(
    pub fn get_oldest_non_removable_transaction_id<'a, 'mcx>(
        rel: &'a types_rel::RelationData<'mcx>,
    ) -> PgResult<TransactionId>
);

seam_core::seam!(
    pub fn count_db_connections(databaseid: types_core::Oid) -> PgResult<i32>
);

seam_core::seam!(
    pub fn count_user_backends(roleid: types_core::Oid) -> PgResult<i32>
);

seam_core::seam!(
    // GetVirtualXIDsDelayingChkpt + HaveVirtualXIDsDelayingChkpt wait loop
    // inputs (procarray.c): snapshot the vxids holding `type` delay flags.
    // Empty result = nothing to wait for.
    pub fn have_virtual_xids_delaying_chkpt(delay_type: i32) -> bool
);

seam_core::seam!(
    pub fn get_oldest_active_transaction_id() -> TransactionId
);

seam_core::seam!(
    pub fn get_oldest_transaction_id_considered_running() -> TransactionId
);

seam_core::seam!(
    pub fn minimum_active_backends(min: i32) -> bool
);

seam_core::seam!(
    pub fn expire_all_known_assigned_transaction_ids() -> PgResult<()>
);

seam_core::seam!(
    pub fn expire_old_known_assigned_transaction_ids(xid: TransactionId) -> PgResult<()>
);

seam_core::seam!(
    pub fn known_assigned_transaction_ids_idle_maintenance()
);

seam_core::seam!(
    // StandbyReleaseOldLocks (standby.c): procarray-outward, installed by the
    // standby crate, consumed by ProcArrayApplyRecoveryInfo.
    pub fn standby_release_old_locks(oldest_xid: TransactionId) -> PgResult<()>
);

seam_core::seam!(
    pub fn proc_array_init_recovery(initialized_upto_xid: TransactionId)
);

seam_core::seam!(
    pub fn proc_array_apply_recovery_info<'a, 'mcx>(
        running: &'a types_storage::storage::RunningTransactionsData<'mcx>,
    ) -> PgResult<()>
);
