seam_core::seam!(
    // InvalidateCatalogSnapshot (snapmgr.c): unregister/flag only, no ereport.
    pub fn invalidate_catalog_snapshot()
);

seam_core::seam!(
    // SnapshotSetCommandId (snapmgr.c).
    pub fn snapshot_set_command_id(cid: types_core::CommandId)
);

seam_core::seam!(
    pub fn at_eoxact_snapshot(is_commit: bool, reset_session: bool) -> types_error::PgResult<()>
);

seam_core::seam!(
    pub fn at_subcommit_snapshot(level: i32)
);

seam_core::seam!(
    pub fn at_subabort_snapshot(level: i32) -> types_error::PgResult<()>
);

seam_core::seam!(
    pub fn xact_has_exported_snapshots() -> bool
);

seam_core::seam!(
    // Startup-time cleanup of pg_snapshots/ leftovers (crash recovery).
    pub fn delete_all_exported_snapshot_files()
);

seam_core::seam!(
    // The snapmgr.c TransactionXmin global (cold: serializable-only callers).
    pub fn transaction_xmin() -> types_core::TransactionId
);

seam_core::seam!(
    // GetActiveSnapshot()->xmin (spgvacuum.c: newly-added-redirect detection).
    pub fn active_snapshot_xmin() -> types_core::TransactionId
);

// Snapshots are snapmgr-owned, backend-lifetime storage (C: statics +
// TopTransactionContext copies); C's signatures carry no allocator.
seam_core::seam!(
    pub fn get_catalog_snapshot(
        relid: types_core::Oid,
    ) -> types_error::PgResult<std::rc::Rc<types_snapshot::SnapshotData<'static>>>
);

seam_core::seam!(
    // RegisterSnapshot: returns the registered (possibly copied) snapshot.
    pub fn register_snapshot(
        snapshot: std::rc::Rc<types_snapshot::SnapshotData<'static>>,
    ) -> types_error::PgResult<std::rc::Rc<types_snapshot::SnapshotData<'static>>>
);

seam_core::seam!(
    pub fn unregister_snapshot(snapshot: std::rc::Rc<types_snapshot::SnapshotData<'static>>)
);

seam_core::seam!(
    // UnregisterSnapshotNoOwner (snapmgr.c): the ResOwnerReleaseSnapshot
    // target — must not touch the resource owner mid-release.
    pub fn unregister_snapshot_no_owner(
        snapshot: std::rc::Rc<types_snapshot::SnapshotData<'static>>,
    )
);

seam_core::seam!(
    // ImportSnapshot (snapmgr.c): SET TRANSACTION SNAPSHOT 'id'.
    pub fn import_snapshot(idstr: &str) -> types_error::PgResult<()>
);

seam_core::seam!(
    // GetLatestSnapshot (snapmgr.c): the currtid family's snapshot source.
    pub fn get_latest_snapshot(
    ) -> types_error::PgResult<std::rc::Rc<types_snapshot::SnapshotData<'static>>>
);
