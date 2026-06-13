//! Seam declarations for the `backend-utils-time-snapmgr` unit
//! (`utils/time/snapmgr.c`): snapshot registration, serialization, and the
//! catalog snapshot.
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly. Snapshots cross as trimmed owned
//! `SnapshotData` values.

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
