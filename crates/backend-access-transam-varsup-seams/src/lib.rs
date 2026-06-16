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
    /// `AdvanceOldestClogXid(oldest_datfrozenxid)` (varsup.c) — advance
    /// `TransamVariables->oldestClogXid` (under `XidGenLock`) so concurrent xact
    /// status lookups never reach truncated-away clog. Called from clog's
    /// `TruncateCLOG` / `clog_redo`. Plain shared-memory store; cannot
    /// `ereport`, but kept fallible to match the shared-state-mutation channel.
    pub fn advance_oldest_clog_xid(oldest_xact: TransactionId) -> PgResult<()>
);

seam_core::seam!(
    /// `TransamVariables->oldestClogXid` (`access/transam.h`) — the oldest XID
    /// whose clog entry is guaranteed to still exist. Read under
    /// `XactTruncationLock` by xid8funcs.c's `TransactionIdInRecentPast` to
    /// decide whether a recent-past XID can still be looked up in clog. Owned in
    /// varsup (the `TransamVariables` singleton); plain shared-memory read.
    pub fn get_oldest_clog_xid() -> TransactionId
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

seam_core::seam!(
    /// `TransamVariables->xactCompletionCount = 1;` performed by
    /// `ProcArrayShmemInit()` (procarray.c) on first shared-memory
    /// initialization. `xactCompletionCount` is a `ProcArrayLock`-protected
    /// field of the `TransamVariables` shared singleton, owned here in varsup;
    /// procarray reaches it through this owner seam.
    pub fn init_xact_completion_count()
);

seam_core::seam!(
    /// `TransamVariables->latestCompletedXid` (`access/transam.h`) — the newest
    /// `FullTransactionId` of any transaction that has completed. Read under
    /// `ProcArrayLock` by procarray.c's `MaintainLatestCompletedXid*` and the
    /// snapshot/horizon scans. Owned in varsup (the `TransamVariables`
    /// singleton); plain shared-memory read.
    pub fn get_latest_completed_xid() -> FullTransactionId
);

seam_core::seam!(
    /// `TransamVariables->latestCompletedXid = fxid` — store the newest
    /// completed `FullTransactionId` (procarray.c's `MaintainLatestCompletedXid*`,
    /// under `ProcArrayLock`). Owned in varsup; plain shared-memory store.
    pub fn set_latest_completed_xid(fxid: FullTransactionId)
);

seam_core::seam!(
    /// `TransamVariables->nextXid = checkPoint.nextXid; nextOid = ...; oidCount = 0`
    /// (xlog.c:5631-5634 in `StartupXLOG`) — seed the cluster-wide XID/OID
    /// counters from the starting checkpoint record at WAL startup. The C code
    /// writes these `TransamVariables` fields directly (no lock; no other process
    /// is up yet); varsup owns the singleton, so the WAL-startup driver reaches
    /// them through this owner seam. Plain shared-memory store.
    pub fn set_transam_variables_at_startup(next_xid: FullTransactionId, next_oid: Oid)
);

seam_core::seam!(
    /// `TransamVariables->xactCompletionCount++` — bump the completed-transaction
    /// generation counter (procarray.c's end-of-xact / clear-transaction paths,
    /// under `ProcArrayLock`). Owned in varsup; plain shared-memory increment.
    pub fn increment_xact_completion_count()
);

seam_core::seam!(
    /// `TransamVariables->xactCompletionCount` (`access/transam.h`) — the current
    /// completed-transaction generation counter, snapshotted by procarray.c's
    /// `GetSnapshotData`/`GetSnapshotDataReuse` under `ProcArrayLock`. Owned in
    /// varsup; plain shared-memory read.
    pub fn get_xact_completion_count() -> u64
);

seam_core::seam!(
    /// `TransamVariables->oldestXid` (`access/transam.h`) — the cluster-wide
    /// oldest xid before which all data is frozen, read by procarray.c's
    /// `GetSnapshotData` (to seed the GlobalVis lower bounds). Owned in varsup;
    /// plain shared-memory read.
    pub fn get_oldest_xid() -> TransactionId
);
