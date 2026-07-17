use types_core::{Oid, ProcNumber, RepOriginId, TimestampTz, TransactionId, XLogRecPtr};
use types_error::PgResult;

// StartPrepare's inputs, pre-serialized to the 2PC state-file segment images
// (cold path; std collections are the marshal buffers, not engine state).
#[derive(Clone, Debug, Default)]
pub struct StartPrepareArgs {
    pub xid: TransactionId,
    pub gid: String,
    pub prepared_at: TimestampTz,
    pub owner: Oid,
    pub databaseid: Oid,
    pub children: Vec<TransactionId>,
    pub ncommitrels: i32,
    pub commitrels: Vec<u8>,
    pub nabortrels: i32,
    pub abortrels: Vec<u8>,
    pub ncommitstats: i32,
    pub commitstats: Vec<u8>,
    pub nabortstats: i32,
    pub abortstats: Vec<u8>,
    pub ninvalmsgs: i32,
    pub invalmsgs: Vec<u8>,
    pub initfileinval: bool,
}

seam_core::seam!(
    pub fn mark_as_preparing(
        xid: TransactionId,
        gid: &str,
        prepared_at: TimestampTz,
        owner: Oid,
        databaseid: Oid,
    ) -> PgResult<()>
);

seam_core::seam!(
    pub fn start_prepare<'a>(args: &'a StartPrepareArgs) -> PgResult<()>
);

seam_core::seam!(
    pub fn end_prepare() -> PgResult<()>
);

seam_core::seam!(
    pub fn post_prepare_twophase()
);

seam_core::seam!(
    pub fn at_abort_twophase()
);

seam_core::seam!(
    pub fn prepare_redo_add<'a>(
        data: &'a [u8],
        start_lsn: XLogRecPtr,
        end_lsn: XLogRecPtr,
        origin_id: RepOriginId,
    ) -> PgResult<()>
);

seam_core::seam!(
    pub fn prepare_redo_remove(xid: TransactionId, give_warning: bool) -> PgResult<()>
);

seam_core::seam!(
    // restoreTwoPhaseData() (twophase.c).
    pub fn restore_two_phase_data() -> PgResult<()>
);

seam_core::seam!(
    // PrescanPreparedTransactions(NULL, NULL) (twophase.c): oldestActiveXid.
    pub fn prescan_prepared_transactions() -> PgResult<TransactionId>
);

seam_core::seam!(
    // PrescanPreparedTransactions(&xids, &nxids) (twophase.c): oldestActiveXid
    // plus the valid prepared-xact XIDs, for the hot-standby fake running-xacts
    // snapshot (StartupXLOG / xlog_redo shutdown-checkpoint arms).
    pub fn prescan_prepared_transactions_xids() -> PgResult<(TransactionId, Vec<TransactionId>)>
);

seam_core::seam!(
    // StandbyRecoverPreparedTransactions() (twophase.c): pg_subtrans entries
    // for prepared transactions during hot-standby init.
    pub fn standby_recover_prepared_transactions() -> PgResult<()>
);

seam_core::seam!(
    // RecoverPreparedTransactions() (twophase.c).
    pub fn recover_prepared_transactions() -> PgResult<()>
);

seam_core::seam!(
    // CheckPointTwoPhase(redo_horizon) (twophase.c).
    pub fn check_point_two_phase(redo_horizon: XLogRecPtr) -> PgResult<()>
);

seam_core::seam!(
    // FinishPreparedTransaction(gid, isCommit) (twophase.c).
    pub fn finish_prepared_transaction<'a>(gid: &'a str, is_commit: bool) -> PgResult<()>
);

seam_core::seam!(
    pub fn register_two_phase_record<'a>(rmid: u8, info: u16, data: &'a [u8]) -> PgResult<()>
);

seam_core::seam!(
    pub fn two_phase_get_dummy_proc_number(
        xid: TransactionId,
        lock_held: bool,
    ) -> PgResult<ProcNumber>
);

seam_core::seam!(
    // StandbyTransactionIdIsPrepared(xid) (twophase.c); standby's
    // StandbyReleaseOldLocks is the cyclic caller (twophase -> xlogreader ->
    // rmgr -> rmgrdesc -> standby).
    pub fn standby_transaction_id_is_prepared(xid: TransactionId) -> PgResult<bool>
);

seam_core::seam!(
    // TwoPhaseGetXidByVirtualXID(vxid, &have_more) (twophase.c:852): the XID
    // of a valid prepared xact whose backend-time vxid matches, plus C's
    // have_more (a second match exists). Args are (procNumber, lxid).
    pub fn two_phase_get_xid_by_virtual_xid(
        proc_number: ProcNumber,
        lxid: u32,
    ) -> PgResult<(TransactionId, bool)>
);
