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
    /// `UnregisterSnapshotFromOwner(snapshot, owner)` — drop the portal's
    /// hold-snapshot registration against its resource owner. portalmem holds
    /// the `Snapshot` / `ResourceOwner` only as long-lived identity tokens
    /// (they outlive any `Mcx` borrow), so they cross as handles.
    pub fn unregister_snapshot_from_owner(
        snapshot: types_portal::SnapshotHandle,
        owner: types_portal::ResourceOwnerHandle,
    )
);

seam_core::seam!(
    /// `ActiveSnapshotSet()` — true if the active-snapshot stack is non-empty.
    pub fn active_snapshot_set() -> bool
);

seam_core::seam!(
    /// `PopActiveSnapshot()` — pop one entry off the active-snapshot stack.
    pub fn pop_active_snapshot()
);
