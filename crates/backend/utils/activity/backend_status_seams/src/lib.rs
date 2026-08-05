use types_core::TimestampTz;

seam_core::seam!(
    pub fn pgstat_report_xact_timestamp(ts: TimestampTz)
);

seam_core::seam!(
    // BackendStatusShmemSize (backend_status.c); ipci's CalculateShmemSize leg.
    pub fn backend_status_shmem_size() -> types_error::PgResult<usize>
);

seam_core::seam!(
    // BackendStatusShmemInit (backend_status.c); ipci's CreateOrAttachShmemStructs leg.
    pub fn backend_status_shmem_init() -> types_error::PgResult<()>
);

seam_core::seam!(
    // Crash-cycle in-place reset (notes/crash-restart-design.md row 11).
    pub fn backend_status_shmem_reset_after_crash()
);

seam_core::seam!(
    // pgstat_beinit (backend_status.c).
    pub fn pgstat_beinit() -> types_error::PgResult<()>
);

seam_core::seam!(
    // pgstat_bestart_initial (backend_status.c).
    pub fn pgstat_bestart_initial() -> types_error::PgResult<()>
);

seam_core::seam!(
    // pgstat_bestart_security (backend_status.c).
    pub fn pgstat_bestart_security() -> types_error::PgResult<()>
);

seam_core::seam!(
    // pgstat_bestart_final (backend_status.c).
    pub fn pgstat_bestart_final() -> types_error::PgResult<()>
);

// BackendState (utils/backend_status.h).
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum BackendState {
    STATE_UNDEFINED = 0,
    STATE_IDLE,
    STATE_RUNNING,
    STATE_IDLEINTRANSACTION,
    STATE_FASTPATH,
    STATE_IDLEINTRANSACTION_ABORTED,
    STATE_DISABLED,
    STATE_STARTING,
}

seam_core::seam!(
    // pgstat_report_activity(state, cmd_str) (backend_status.c).
    pub fn pgstat_report_activity(state: BackendState, cmd_str: Option<&str>)
);

seam_core::seam!(
    // pgstat_report_query_id(query_id, force) (backend_status.c).
    pub fn pgstat_report_query_id(query_id: i64, force: bool)
);

seam_core::seam!(
    // pgstat_report_plan_id(plan_id, force) (backend_status.c).
    pub fn pgstat_report_plan_id(plan_id: i64, force: bool)
);

seam_core::seam!(
    // pgstat_clear_backend_activity_snapshot (backend_status.c).
    pub fn pgstat_clear_backend_status_snapshot()
);
