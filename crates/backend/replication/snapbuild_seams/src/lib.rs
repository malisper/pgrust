seam_core::seam!(
    pub fn snap_build_reset_exported_snapshot_state()
);

seam_core::seam!(
    // CheckPointSnapBuild() (snapbuild.c).
    pub fn check_point_snap_build() -> types_error::PgResult<()>
);
