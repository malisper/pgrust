//! Seam declarations for the `backend-access-transam-twophase` unit
//! (`access/transam/twophase.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

use ::types_core::{Oid, RepOriginId, Size, TimestampTz, TransactionId, XLogRecPtr};
use ::types_error::PgResult;
pub use ::wal::xact_records::StartPrepareArgs;

seam_core::seam!(
    /// `StandbyTransactionIdIsPrepared(xid)` — true if `xid` is a prepared
    /// transaction known to this standby.
    pub fn standby_transaction_id_is_prepared(xid: TransactionId) -> PgResult<bool>
);

seam_core::seam!(
    /// `RegisterTwoPhaseRecord(rmid, info, data, len)` — append a resource
    /// manager's 2PC record (header + optional payload) to the in-flight
    /// prepare builder set up by `StartPrepare`. Called by the per-RM
    /// `AtPrepare_*` hooks (e.g. `AtPrepare_MultiXact`) between `StartPrepare`
    /// and `EndPrepare`.
    pub fn register_two_phase_record(rmid: u8, info: u16, data: &[u8]) -> PgResult<()>
);

seam_core::seam!(
    /// `TwoPhaseGetDummyProcNumber(xid, lock_held)` — the dummy `PGPROC`'s
    /// `ProcNumber` standing in for a prepared transaction `xid`. `lock_held`
    /// is true when the caller already holds `TwoPhaseStateLock`.
    pub fn two_phase_get_dummy_proc_number(
        xid: TransactionId,
        lock_held: bool,
    ) -> PgResult<::types_core::ProcNumber>
);

seam_core::seam!(
    /// `TwoPhaseGetXidByVirtualXID(vxid, *have_more)` — find a prepared xact by
    /// its dummy proc's virtual transaction id `(procNumber, localTransactionId)`,
    /// returning `(xid, have_more)`. `xid` is `InvalidTransactionId` when no
    /// prepared xact matches; `have_more` is set when more than one prepared
    /// xact shares the vxid (caller re-invokes to lock them all). Consumed by
    /// lock.c's `XactLockForVirtualXact`.
    pub fn two_phase_get_xid_by_virtual_xid(
        vxid: (::types_core::ProcNumber, u32),
    ) -> PgResult<(TransactionId, bool)>
);

seam_core::seam!(
    /// `MarkAsPreparing(xid, gid, prepared_at, owner, databaseid)` — reserve
    /// the GID; fails if invalid or already in use.
    pub fn mark_as_preparing(
        xid: TransactionId,
        gid: &str,
        prepared_at: TimestampTz,
        owner: Oid,
        databaseid: Oid,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `StartPrepare(gxact)` — begin collecting the 2PC state file data. C
    /// reads the file payload (committed children, pending commit/abort rels,
    /// transactional drop stats, committed inval messages, `proc->databaseId`)
    /// from the current backend transaction; the consumer gathers it and hands
    /// it here in [`StartPrepareArgs`]. The owner stashes the in-flight builder
    /// in its backend-private slot for the matching `end_prepare`.
    pub fn start_prepare(args: &StartPrepareArgs) -> PgResult<()>
);

seam_core::seam!(
    /// `EndPrepare(gxact)` — write the prepare record; the durable prepare.
    /// Operates on the builder + slot the matching `start_prepare`/
    /// `mark_as_preparing` left in the owner's backend-private state, and reads
    /// the ambient replication-origin session via the origin seams.
    pub fn end_prepare() -> PgResult<()>
);

seam_core::seam!(
    /// `PostPrepare_Twophase()` — detach the prepared transaction from this
    /// backend.
    pub fn post_prepare_twophase()
);

seam_core::seam!(
    /// `AtAbort_Twophase()` — clean up a partially-prepared gxact on abort.
    pub fn at_abort_twophase()
);

seam_core::seam!(
    /// `PrepareRedoAdd(...)` under `TwoPhaseStateLock` (the xact_redo caller
    /// acquires/releases the lock in C; the installed impl carries it until
    /// the lwlock surface is ported).
    pub fn prepare_redo_add(
        data: &[u8],
        start_lsn: XLogRecPtr,
        end_lsn: XLogRecPtr,
        origin_id: RepOriginId,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `PrepareRedoRemove(xid, giveWarning)` under `TwoPhaseStateLock` (see
    /// `prepare_redo_add`).
    pub fn prepare_redo_remove(xid: TransactionId, give_warning: bool) -> PgResult<()>
);

seam_core::seam!(
    /// `TwoPhaseShmemSize(void)` — bytes the 2PC shared `TwoPhaseStateData` /
    /// `GXACT` array needs. Mirrors C's `TwoPhaseShmemSize(void)`: takes no
    /// argument and reads the `max_prepared_xacts` GUC itself (the owner's
    /// per-backend value), returning `Size`. Infallible (the offsetof/sizeof
    /// arithmetic does not `ereport`; the outer `add_size` in the ipci
    /// accumulator carries any overflow error).
    pub fn two_phase_shmem_size() -> Size
);

seam_core::seam!(
    /// `TwoPhaseShmemInit()` — allocate-or-attach the global `TwoPhaseState`
    /// in main shared memory (via `ShmemInitStruct`) and, on the
    /// non-`IsUnderPostmaster` path, build the GXACT freelist over the
    /// preallocated dummy PGPROCs. `Err` carries the out-of-shared-memory
    /// `ereport(ERROR)` from `ShmemInitStruct`.
    pub fn two_phase_shmem_init() -> PgResult<()>

);

seam_core::seam!(
    /// `restoreTwoPhaseData()` (twophase.c) — scan `pg_twophase/` and load each
    /// prepared-transaction state file into shared memory at the end of recovery.
    /// Called from `StartupXLOG` (xlog.c:5731). The owner wraps its private
    /// `TwoPhaseStateData`; `orig_next_xid` is `TransamVariables->nextXid` and
    /// `transaction_xmin` is `TransactionXmin`, which the WAL-startup driver
    /// reads from their owners and threads in (the values are globals in C).
    pub fn restore_two_phase_data(
        orig_next_xid: TransactionId,
        transaction_xmin: TransactionId,
        reached_consistency: bool,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `PrescanPreparedTransactions(xids_p, nxids_p)` (twophase.c) — return the
    /// oldest XID having an unfinished prepared transaction (or `nextXid` if none)
    /// so `StartupSUBTRANS` knows how far back to zero, plus the list of prepared
    /// XIDs. Called from `StartupXLOG` (xlog.c:5857, 5988). The clean path
    /// (xlog.c:5988, C `xids_p == NULL`) drops the list; the hot-standby
    /// shutdown-checkpoint path (xlog.c:5857) consumes it to build the
    /// running-xacts snapshot. The seam always collects the list (the C
    /// `xids_p == NULL` case is just `running.xids` going unused) — the caller
    /// keeps or drops it.
    pub fn prescan_prepared_transactions(
        orig_next_xid: TransactionId,
        transaction_xmin: TransactionId,
    ) -> PgResult<(TransactionId, ::std::vec::Vec<TransactionId>)>
);

seam_core::seam!(
    /// `StandbyRecoverPreparedTransactions()` (twophase.c) — re-acquire locks for
    /// prepared transactions during hot-standby startup. Called from
    /// `StartupXLOG` (xlog.c:5884) on the standby path. Owner wraps its private
    /// state; fallible.
    pub fn standby_recover_prepared_transactions(
        orig_next_xid: TransactionId,
        transaction_xmin: TransactionId,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `RecoverPreparedTransactions()` (twophase.c) — reconstruct full in-memory
    /// state for each prepared transaction at the end of recovery. Called from
    /// `StartupXLOG` (xlog.c:6168). Owner wraps its private state + the
    /// per-backend `my_locked_gxact` slot; fallible.
    pub fn recover_prepared_transactions(
        orig_next_xid: TransactionId,
        transaction_xmin: TransactionId,
        in_hot_standby: bool,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `CheckPointTwoPhase(redo_horizon)` (twophase.c) — fsync every valid /
    /// in-redo prepared-xact whose PREPARE end-LSN ≤ `redo_horizon` to a
    /// `pg_twophase/` state file, so it survives a crash that loses the WAL
    /// before the new checkpoint redo point. Called from `CreateCheckPoint` via
    /// `CheckPointGuts` (xlog.c:7600), deliberately last in the checkpoint.
    pub fn check_point_two_phase(redo_horizon: XLogRecPtr) -> PgResult<()>
);
