//! Seam declarations for the `backend-access-transam-varsup` unit
//! (`access/transam/varsup.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

use types_core::{FullTransactionId, Oid, TransactionId};
use types_error::PgResult;

seam_core::seam!(
    /// `ReadNextFullTransactionId()` — the next full xid to be assigned.
    pub fn read_next_full_transaction_id() -> FullTransactionId
);

seam_core::seam!(
    /// `GetNewTransactionId(isSubXact)` — allocate the next FullTransactionId,
    /// record it in PGPROC and pg_subtrans. `ereport(ERROR)`s during recovery,
    /// in parallel mode, and near XID wraparound.
    pub fn get_new_transaction_id(is_subxact: bool) -> PgResult<FullTransactionId>
);

seam_core::seam!(
    /// `ReadNextTransactionId()` (`access/transam.h`) — read
    /// `TransamVariables->nextXid` (the xid part).
    pub fn read_next_transaction_id() -> TransactionId
);

seam_core::seam!(
    /// `AdvanceNextFullTransactionIdPastXid(xid)` — used during redo to keep
    /// nextXid beyond any XID mentioned in WAL.
    pub fn advance_next_full_transaction_id_past_xid(xid: TransactionId)
);

seam_core::seam!(
    /// `AdvanceNextFullTransactionIdPastXid(xid)` (varsup.c): bump
    /// `TransamVariables->nextXid` past `xid` if it is not already, so a
    /// recovered prepared transaction's subxids don't collide with future
    /// assignments. Takes `XidGenLock`; the SLRU extension it triggers can
    /// `ereport(ERROR)`, carried on `Err`.
    pub fn advance_next_full_xid_past_xid(xid: TransactionId) -> PgResult<()>
);

seam_core::seam!(
    /// `TransamVariables->oldestCommitTsXid` (`access/transam.h`) — the oldest
    /// XID for which a commit timestamp can be consulted. Read under
    /// `CommitTsLock` by commit_ts.c.
    pub fn get_oldest_commit_ts_xid() -> TransactionId
);

seam_core::seam!(
    /// `TransamVariables->newestCommitTsXid` (`access/transam.h`) — the newest
    /// XID for which a commit timestamp endpoint is tracked. Read under
    /// `CommitTsLock` by commit_ts.c.
    pub fn get_newest_commit_ts_xid() -> TransactionId
);

seam_core::seam!(
    /// `TransamVariables->oldestCommitTsXid = xid` — store the oldest
    /// consultable commit-ts XID (commit_ts.c, under `CommitTsLock`).
    pub fn set_oldest_commit_ts_xid(xid: TransactionId)
);

seam_core::seam!(
    /// `TransamVariables->newestCommitTsXid = xid` — store the newest tracked
    /// commit-ts XID endpoint (commit_ts.c, under `CommitTsLock`).
    pub fn set_newest_commit_ts_xid(xid: TransactionId)
);

seam_core::seam!(
    /// `VarsupShmemSize()` (ipci.c `CalculateShmemSize` accumulator) — shared-memory
    /// bytes this subsystem needs. `Err` carries the `add_size`/`mul_size`
    /// overflow `ereport(ERROR)`. Owner unported; scaffolded slot.
    pub fn varsup_shmem_size() -> types_error::PgResult<types_core::Size>
);

seam_core::seam!(
    /// `VarsupShmemInit()` (ipci.c `CreateOrAttachShmemStructs`) — allocate-or-attach
    /// this subsystem's shared-memory structures. `Err` carries the C
    /// out-of-shared-memory `ereport(ERROR)`. Owner unported; scaffolded slot.
    pub fn varsup_shmem_init() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `GetNewObjectId()` (varsup.c): allocate the next system-wide OID,
    /// skipping the pinned range on wraparound. Takes `OidGenLock`; can
    /// `ereport(ERROR)` if pinned-object generation has been stopped.
    pub fn get_new_object_id() -> PgResult<Oid>
);

seam_core::seam!(
    /// `StopGeneratingPinnedObjectIds()` (varsup.c): the initdb-only call that
    /// advances the OID counter past the pinned range so no further objects are
    /// pinned. Takes `OidGenLock`.
    pub fn stop_generating_pinned_object_ids() -> PgResult<()>
);
