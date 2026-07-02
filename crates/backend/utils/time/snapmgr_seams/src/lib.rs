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
    // The snapmgr.c TransactionXmin global; a getter seam sanctioned as the
    // global's accessor until snapmgr ports (serializable-only cold path).
    pub fn transaction_xmin() -> types_core::TransactionId
);
