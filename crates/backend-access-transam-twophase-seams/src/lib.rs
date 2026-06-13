//! Seam declarations for the `backend-access-transam-twophase` unit
//! (`access/transam/twophase.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

use types_core::{Oid, RepOriginId, Size, TimestampTz, TransactionId, XLogRecPtr};
use types_error::PgResult;
pub use types_wal::xact_records::StartPrepareArgs;

seam_core::seam!(
    /// `StandbyTransactionIdIsPrepared(xid)` — true if `xid` is a prepared
    /// transaction known to this standby.
    pub fn standby_transaction_id_is_prepared(xid: TransactionId) -> PgResult<bool>
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
