#![allow(non_snake_case)]

mod codec;
mod core;
mod files;
mod finish;
mod recovery;
mod srf;
mod state;

#[cfg(test)]
mod tests;

pub use crate::core::{
    AtAbort_Twophase, IsTwoPhaseTransactionGidForSubid, LookupGXactBySubid, MarkAsPreparing,
    PostPrepare_Twophase, RegisterTwoPhaseRecord, StandbyTransactionIdIsPrepared,
    TwoPhaseGetDummyProcNumber, TwoPhaseGetXidByVirtualXID, TwoPhaseTransactionGid,
};
pub use codec::{TwoPhaseFileHeader, TwoPhaseRecordOnDisk, TWOPHASE_MAGIC};
pub use finish::{FinishPreparedTransaction, LookupGXact};
pub use recovery::{
    restoreTwoPhaseData, CheckPointTwoPhase, PrescanPreparedTransactions,
    RecoverPreparedTransactions, StandbyRecoverPreparedTransactions,
};
pub use state::{TwoPhaseShmemInit, TwoPhaseShmemSize, TwoPhaseStateResetAfterCrash, GIDSIZE};

fn here(function: &'static str) -> types_error::ErrorLocation {
    types_error::ErrorLocation::new(file!(), line!() as i32, function)
}

pub fn init_seams() {
    use lwlock::LW_EXCLUSIVE;
    use twophase_seams as seams;

    seams::mark_as_preparing::set(|xid, gid, prepared_at, owner, databaseid| {
        MarkAsPreparing(xid, gid, prepared_at, owner, databaseid).map(|_| ())
    });
    seams::start_prepare::set(crate::core::start_prepare);
    seams::end_prepare::set(crate::core::end_prepare);
    seams::register_two_phase_record::set(RegisterTwoPhaseRecord);
    seams::post_prepare_twophase::set(PostPrepare_Twophase);
    seams::at_abort_twophase::set(AtAbort_Twophase);
    seams::two_phase_get_dummy_proc_number::set(TwoPhaseGetDummyProcNumber);
    seams::standby_transaction_id_is_prepared::set(StandbyTransactionIdIsPrepared);

    // C's xact_redo holds TwoPhaseStateLock around these callees; the redo
    // arms here delegate the lock to the installed impl (redo.rs contract).
    seams::prepare_redo_add::set(|data, start_lsn, end_lsn, origin_id| {
        crate::state::lock_twophase_state(LW_EXCLUSIVE);
        let r = crate::recovery::prepare_redo_add_locked(data, start_lsn, end_lsn, origin_id);
        crate::state::unlock_twophase_state();
        r
    });
    seams::prepare_redo_remove::set(|xid, give_warning| {
        crate::state::lock_twophase_state(LW_EXCLUSIVE);
        let r = crate::recovery::prepare_redo_remove_locked(xid, give_warning);
        crate::state::unlock_twophase_state();
        r
    });

    seams::restore_two_phase_data::set(restoreTwoPhaseData);
    seams::prescan_prepared_transactions::set(PrescanPreparedTransactions);
    seams::prescan_prepared_transactions_xids::set(recovery::PrescanPreparedTransactionsXids);
    seams::standby_recover_prepared_transactions::set(recovery::StandbyRecoverPreparedTransactions);
    seams::two_phase_get_xid_by_virtual_xid::set(recovery::TwoPhaseGetXidByVirtualXID);
    seams::recover_prepared_transactions::set(RecoverPreparedTransactions);
    seams::check_point_two_phase::set(CheckPointTwoPhase);
    seams::finish_prepared_transaction::set(FinishPreparedTransaction);

    srf::register_builtins();
}
