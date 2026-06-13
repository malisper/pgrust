//! Seam declarations for the `backend-utils-time-snapmgr` unit
//! (`utils/time/snapmgr.c`): snapshot registration, serialization, and the
//! catalog snapshot.
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly. Snapshots cross as trimmed owned
//! `SnapshotData` values.

use types_core::CommandId;
use types_error::PgResult;

seam_core::seam!(
    /// `GetCatalogSnapshot(relid)` (snapmgr.c): an MVCC snapshot capable of
    /// reading the catalog (refreshed if invalidations arrived). Can
    /// `ereport(ERROR)` (snapshot import/allocation paths), carried on
    /// `Err`.
    pub fn get_catalog_snapshot(
        relid: types_core::primitive::Oid,
    ) -> types_error::PgResult<types_snapshot::SnapshotData>
);

seam_core::seam!(
    /// `RegisterSnapshot(snapshot)` (snapmgr.c): register the snapshot on
    /// the current resource owner so it stays valid. Allocates (registered
    /// snapshots are copied), so fallible on OOM.
    pub fn register_snapshot(
        snapshot: types_snapshot::SnapshotData,
    ) -> types_error::PgResult<types_snapshot::SnapshotData>
);

seam_core::seam!(
    /// `UnregisterSnapshot(snapshot)` (snapmgr.c): drop the resource-owner
    /// registration taken by [`register_snapshot`], freeing the snapshot when
    /// its last registration goes away. The owned `SnapshotData` is consumed.
    /// Cannot `ereport` in C; modeled infallible bare.
    pub fn unregister_snapshot(snapshot: types_snapshot::SnapshotData)
);

seam_core::seam!(
    /// `EstimateSnapshotSpace(snapshot)` (snapmgr.c): bytes needed to
    /// serialize the snapshot. Pure size computation; cannot `ereport`.
    pub fn estimate_snapshot_space(snapshot: &types_snapshot::SnapshotData) -> usize
);

seam_core::seam!(
    /// `SerializeSnapshot(snapshot, start_address)` (snapmgr.c): serialize
    /// the snapshot. C writes into caller-provided (shared) memory; the
    /// owned model returns the bytes. Fallible on OOM for the buffer.
    pub fn serialize_snapshot(
        snapshot: &types_snapshot::SnapshotData,
    ) -> types_error::PgResult<std::vec::Vec<u8>>
);

seam_core::seam!(
    /// `RestoreSnapshot(start_address)` (snapmgr.c): rebuild a snapshot from
    /// its serialized form. C pallocs the snapshot, so fallible on OOM.
    pub fn restore_snapshot(
        bytes: &[u8],
    ) -> types_error::PgResult<types_snapshot::SnapshotData>
);

seam_core::seam!(
    /// Run `f` with a transaction snapshot active — the C
    /// `PushActiveSnapshot(GetTransactionSnapshot()); ...; PopActiveSnapshot()`
    /// bracket of `RemoveTempRelationsCallback`, owned by snapmgr as one
    /// scope. Snapshot acquisition allocates and can `ereport(ERROR)`, and
    /// `f`'s error propagates; both carried on `Err`.
    pub fn with_transaction_snapshot(
        f: &mut dyn FnMut() -> PgResult<()>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `SnapshotSetCommandId(curcid)` — propagate the new command id into the
    /// static snapshots. Pure field updates; cannot `ereport`.
    pub fn snapshot_set_command_id(curcid: CommandId)
);

seam_core::seam!(
    /// `AtEOXact_Snapshot(isCommit, resetXmin)` — snapshot cleanup at
    /// transaction end (WARNs about leaks at commit; can `ereport(ERROR)` on
    /// exported-snapshot file cleanup).
    pub fn at_eoxact_snapshot(is_commit: bool, reset_xmin: bool) -> PgResult<()>
);

seam_core::seam!(
    /// `AtSubCommit_Snapshot(level)`.
    pub fn at_subcommit_snapshot(level: i32)
);

seam_core::seam!(
    /// `AtSubAbort_Snapshot(level)`.
    pub fn at_subabort_snapshot(level: i32) -> PgResult<()>
);

seam_core::seam!(
    /// `XactHasExportedSnapshots()` — true after `pg_export_snapshot`, which
    /// forbids PREPARE.
    pub fn xact_has_exported_snapshots() -> bool
);

seam_core::seam!(
    /// `PushCopiedSnapshot(GetActiveSnapshot())` (copyto.c:830): push a copy of
    /// the current active snapshot onto the active-snapshot stack, so a fresh
    /// command id can be set without disturbing the caller's snapshot. `Err`
    /// carries the `there is no active snapshot` `ereport`.
    pub fn push_copied_active_snapshot() -> PgResult<()>
);

seam_core::seam!(
    /// `UpdateActiveSnapshotCommandId()` (copyto.c:831): bump the active
    /// snapshot's command id so this query sees the results of previously
    /// executed commands. `Err` carries snapmgr `ereport(ERROR)`s.
    pub fn update_active_snapshot_command_id() -> PgResult<()>
);

seam_core::seam!(
    /// `GetActiveSnapshot()` (snapmgr.c) — the topmost active snapshot, or
    /// `None` (the C may return NULL when no snapshot is active). Snapshots
    /// cross as a shared `Rc<SnapshotData>` (the C `Snapshot` is a shared
    /// pointer the snapshot stack and callers alias).
    pub fn get_active_snapshot() -> PgResult<Option<std::rc::Rc<types_snapshot::SnapshotData>>>
);

seam_core::seam!(
    /// `PushActiveSnapshot(snap)` (snapmgr.c) — make `snap` the active
    /// snapshot (copies it onto the active-snapshot stack). Allocates; can
    /// `ereport(ERROR)`.
    pub fn push_active_snapshot(
        snapshot: std::rc::Rc<types_snapshot::SnapshotData>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `PopActiveSnapshot()` (snapmgr.c) — pop the topmost active snapshot.
    /// Used by COPY-(query)-TO teardown (copyto.c:1013) to pop the snapshot
    /// pushed by [`push_copied_active_snapshot`].
    pub fn pop_active_snapshot() -> PgResult<()>
);

seam_core::seam!(
    /// `GetLatestSnapshot()` (snapmgr.c): a fresh MVCC snapshot reflecting all
    /// committed transactions as of now. Snapshot acquisition can
    /// `ereport(ERROR)` (e.g. too-old-snapshot / xmin), carried on `Err`.
    pub fn get_latest_snapshot() -> PgResult<types_snapshot::SnapshotData>
);

seam_core::seam!(
    /// `GetTransactionSnapshot()` (snapmgr.c): the transaction snapshot
    /// (serializable: the registered xact snapshot; otherwise a fresh one).
    /// Can `ereport(ERROR)`, carried on `Err`.
    pub fn get_transaction_snapshot() -> PgResult<types_snapshot::SnapshotData>
);
