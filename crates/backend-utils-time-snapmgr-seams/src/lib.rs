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
    /// `HistoricSnapshotActive()` (snapmgr.c): true when a historic MVCC
    /// snapshot (logical decoding) is installed. Pure read of the file-static
    /// `HistoricSnapshot != NULL`. Cannot `ereport`.
    pub fn historic_snapshot_active() -> bool
);

seam_core::seam!(
    /// `HaveRegisteredOrActiveSnapshot()` (snapmgr.c): true when there is any
    /// registered or active snapshot besides the catalog snapshot. snapbuild.c
    /// asserts this is false before building an initial slot snapshot.
    pub fn have_registered_or_active_snapshot() -> bool
);

seam_core::seam!(
    /// `ExportSnapshot(snapshot)` (snapmgr.c): make `snapshot` active and write
    /// it to a shared snapshot file for `SET TRANSACTION SNAPSHOT`, returning
    /// the snapshot's file name. snapbuild.c hands it the plain MVCC snapshot
    /// built by `SnapBuildInitialSnapshot`. Fallible (file/allocation).
    pub fn export_snapshot(
        snapshot: types_snapshot::SnapshotData,
    ) -> types_error::PgResult<std::string::String>
);

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
    /// `UnregisterSnapshotFromOwner(snapshot, owner)` — drop the portal's
    /// hold-snapshot registration against its resource owner. The snapshot
    /// crosses as the shared `Rc<SnapshotData>` the stack/owner alias (the C
    /// `Snapshot` is a shared pointer); the owner as the shared
    /// `types_portal::ResourceOwner` handle.
    pub fn unregister_snapshot_from_owner(
        snapshot: std::rc::Rc<types_snapshot::SnapshotData>,
        owner: types_portal::ResourceOwner,
    )
);

seam_core::seam!(
    /// `ActiveSnapshotSet()` — true if the active-snapshot stack is non-empty.
    pub fn active_snapshot_set() -> bool
);

seam_core::seam!(
    /// `XidInMVCCSnapshot(xid, snapshot)` (snapmgr.c): is `xid` still seen as
    /// in-progress by the given MVCC snapshot? Can `ereport(ERROR)` on the
    /// subtransaction-overflow recheck (`SubTransGetTopmostTransaction` ->
    /// `TransactionIdDidCommit`), carried on `Err`.
    pub fn xid_in_mvcc_snapshot(
        xid: types_core::TransactionId,
        snapshot: &types_snapshot::SnapshotData,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `InvalidateCatalogSnapshot()` (snapmgr.c): drop the backend's cached
    /// catalog snapshot so the next catalog read takes a fresh one — driven by
    /// most arms of `LocalExecuteInvalidationMessage`. Pure global reset;
    /// infallible.
    pub fn invalidate_catalog_snapshot()
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
    /// `PushActiveSnapshot(GetTransactionSnapshot())` (snapmgr.c): take and
    /// push the transaction snapshot.
    pub fn push_active_snapshot_transaction() -> PgResult<()>
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

seam_core::seam!(
    /// `PushActiveSnapshotWithLevel(snapshot, snap_level)` (snapmgr.c:693):
    /// push `snapshot` onto the active-snapshot stack, recording it as
    /// belonging to transaction nesting level `snap_level` so it is popped at
    /// the right subxact boundary. `PortalRunUtility` / `PortalRunMulti` /
    /// `EnsurePortalSnapshotExists` use this to tie a portal's snapshot to its
    /// `createLevel`. The snapshot copy can `ereport(ERROR)`.
    pub fn push_active_snapshot_with_level(
        snapshot: std::rc::Rc<types_snapshot::SnapshotData>,
        snap_level: i32,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `SetupHistoricSnapshot(historic_snapshot, tuplecids)` (snapmgr.c:1666):
    /// install a historic (logical-decoding) catalog snapshot plus its
    /// `(relfilelocator, ctid) -> (cmin, cmax)` lookup map. The map is the
    /// owned value behind C's `static HTAB *tuplecid_data` (`None` == the C
    /// `NULL`); reorderbuffer builds it via `ReorderBufferBuildTupleCidHash`
    /// and hands it across. Cannot `ereport`.
    pub fn setup_historic_snapshot(
        historic_snapshot: types_snapshot::SnapshotData,
        tuplecids: Option<types_logical::TupleCidHash>,
    )
);

seam_core::seam!(
    /// `TeardownHistoricSnapshot(is_error)` (snapmgr.c:1682): clear the
    /// historic snapshot and its tuplecid map. Cannot `ereport`.
    pub fn teardown_historic_snapshot(is_error: bool)
);

seam_core::seam!(
    /// `HistoricSnapshotGetTupleCids()` (snapmgr.c:1695): the active
    /// `(relfilelocator, ctid) -> (cmin, cmax)` map (`None` == C `NULL`),
    /// read by `ResolveCminCmaxDuringDecoding`. Cannot `ereport`.
    pub fn historic_snapshot_get_tuple_cids() -> Option<types_logical::TupleCidHash>
);

// ---------------------------------------------------------------------------
// Re-homed from consumer-local seam crates (true C owner is snapmgr.c).
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `ImportSnapshot(idstr)` (snapmgr.c): adopt the snapshot named by the
    /// `SET TRANSACTION SNAPSHOT '...'` argument. Can `ereport(ERROR)`.
    /// Re-homed from `backend-utils-misc-guc-funcs-seams`.
    pub fn import_snapshot(idstr: std::string::String) -> PgResult<()>
);

seam_core::seam!(
    /// `PushCopiedSnapshot(GetActiveSnapshot()); UpdateActiveSnapshotCommandId();`
    /// (snapmgr.c) — the matview-refresh "copy the active snapshot and bump its
    /// command id" composite. Re-homed from `backend-commands-matview-deps-seams`.
    pub fn push_copied_snapshot_and_bump() -> PgResult<()>
);

seam_core::seam!(
    /// `RegisterSnapshotOnOwner(snapshot, TopTransactionResourceOwner)`
    /// (snapmgr.c) — pin a large-object's snapshot to the top-transaction
    /// resource owner so it stays alive for the FD's lifetime. Consumes the
    /// descriptor's owned snapshot and returns the (possibly copied) registered
    /// snapshot. Re-homed from `backend-libpq-be-fsstubs-seams`. The repo port
    /// is the `NoOwner` core (resowner lifecycle is RAII), matching the existing
    /// `unregister_snapshot_from_owner` install.
    pub fn register_snapshot_on_top_owner(
        snapshot: std::rc::Rc<types_snapshot::SnapshotData>,
    ) -> PgResult<std::rc::Rc<types_snapshot::SnapshotData>>
);

seam_core::seam!(
    /// `UnregisterSnapshotFromOwner(snapshot, TopTransactionResourceOwner)`
    /// (snapmgr.c) — release a large-object's snapshot from the top-transaction
    /// resource owner. Consumes the descriptor's owned snapshot. Re-homed from
    /// `backend-libpq-be-fsstubs-seams`.
    pub fn unregister_snapshot_from_top_owner(
        snapshot: std::rc::Rc<types_snapshot::SnapshotData>,
    ) -> PgResult<()>
);
