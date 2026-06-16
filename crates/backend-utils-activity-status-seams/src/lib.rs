//! Seam declarations for the `backend-utils-activity-status` unit
//! (`utils/activity/backend_status.c`): the process-global backend status
//! entry (`MyBEEntry`) and the `pgstat_track_activities` GUC.
//!
//! `backend_progress.c` writes the `st_progress_*` fields of its own backend
//! entry between `PGSTAT_BEGIN_WRITE_ACTIVITY` / `PGSTAT_END_WRITE_ACTIVITY`;
//! the entry itself is owned by `backend_status.c`, so it is reached through
//! the [`with_my_beentry`] callback slot (a callback rather than a returned
//! `&'static mut`: aliasable mutable statics are unsound in Rust). The
//! bracketing and field writes — the logic — stay in the consumer. The owning
//! unit installs these from its `init_seams()` when it lands; until then a
//! call panics loudly.

extern crate alloc;

use types_pgstat::backend_status::PgBackendStatus;

seam_core::seam!(
    /// `MyBEEntry != NULL` — is the backend status entry initialized?
    pub fn my_be_entry_present() -> bool
);

seam_core::seam!(
    /// `pgstat_get_backend_current_activity(pid, checkUser)` (backend_status.c)
    /// — the current query string of backend `pid`, for the server-log deadlock
    /// detail. `check_user` is C's `checkUser` (redact for permission); the
    /// deadlock detector passes `false`. Returns the activity string (the C
    /// pointer into the backend's status entry).
    pub fn backend_current_activity(pid: i32, check_user: bool) -> alloc::string::String
);

seam_core::seam!(
    /// The `pgstat_track_activities` GUC (`backend_status.c`).
    pub fn track_activities() -> bool
);

seam_core::seam!(
    /// Run `f` on this backend's live `*MyBEEntry` (`backend_status.c`).
    /// Callers must only call this after [`my_be_entry_present`] returns true.
    pub fn with_my_beentry(f: &mut dyn FnMut(&mut PgBackendStatus))
);

seam_core::seam!(
    /// `pgstat_report_query_id(query_id, force)` (`backend_status.c`): advertise
    /// the running query's jumble id on the backend status entry.
    /// `exec_simple_query` resets it to `0` (`force = true`) per parsetree.
    /// Owner unported; scaffolded slot.
    pub fn pgstat_report_query_id(query_id: u64, force: bool)
);

seam_core::seam!(
    /// `pgstat_report_plan_id(plan_id, force)` (`backend_status.c`): advertise
    /// the running query's plan id on the backend status entry.
    /// `exec_simple_query` resets it to `0` (`force = true`) per parsetree.
    /// Owner unported; scaffolded slot.
    pub fn pgstat_report_plan_id(plan_id: u64, force: bool)
);

seam_core::seam!(
    /// `pgstat_report_activity(STATE_IDLE, NULL)` (`backend_status.c`): mark
    /// this backend idle and clear the current activity string. Infallible.
    pub fn pgstat_report_activity_idle()
);

seam_core::seam!(
    /// `pgstat_report_activity(STATE_RUNNING, cmd_str)` (`backend_status.c`):
    /// mark this backend as actively running the given query string and stamp
    /// the activity start timestamp. `exec_simple_query` /
    /// `exec_execute_message` (postgres.c) call this at the start of command
    /// processing. The `BackendState` discriminant is baked into the seam name
    /// (the `BackendState` enum is not yet modeled), matching the existing
    /// [`pgstat_report_activity_idle`] convention. Infallible.
    pub fn pgstat_report_activity_running(cmd_str: alloc::string::String)
);

seam_core::seam!(
    /// `pgstat_report_xact_timestamp(tstamp)` (backend_status.c).
    pub fn pgstat_report_xact_timestamp(tstamp: types_core::TimestampTz)
);

// --- backend-utils-init-postinit consumers (backend_status.c) ---

seam_core::seam!(
    /// `pgstat_beinit()` (backend_status.c): initialize backend status
    /// reporting (pick the MyBEEntry slot). `Err` carries its `ereport` surface.
    pub fn pgstat_beinit() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `pgstat_bestart_initial()` (backend_status.c): begin filling the
    /// PgBackendStatus entry (the pre-auth portion).
    pub fn pgstat_bestart_initial() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `pgstat_bestart_security()` (backend_status.c): record SSL/GSS details.
    pub fn pgstat_bestart_security() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `pgstat_bestart_final()` (backend_status.c): finish the PgBackendStatus
    /// entry (database/role/activity).
    pub fn pgstat_bestart_final() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `conn_timing.auth_start = tstamp` (backend_status.c): record the
    /// authentication start timestamp for logging.
    pub fn set_conn_timing_auth_start(tstamp: types_core::TimestampTz)
);

seam_core::seam!(
    /// `conn_timing.auth_end = tstamp` (backend_status.c).
    pub fn set_conn_timing_auth_end(tstamp: types_core::TimestampTz)
);

seam_core::seam!(
    /// `BackendStatusShmemSize()` (backend_status.c) — shared-memory bytes for
    /// the per-backend status array (`PgBackendStatus` entries, activity
    /// buffers, app-name and client-host buffers); summed by ipci.c
    /// `CalculateShmemSize`. `Err` carries the `add_size`/`mul_size` overflow
    /// `ereport(ERROR)`. Owner unported; scaffolded slot.
    pub fn backend_status_shmem_size() -> types_error::PgResult<types_core::Size>
);

seam_core::seam!(
    /// `BackendStatusShmemInit()` (backend_status.c) — allocate-or-attach the
    /// per-backend status array in shared memory (called from ipci.c
    /// `CreateOrAttachShmemStructs`). `Err` carries the out-of-shmem
    /// `ereport(ERROR)`. Owner unported; scaffolded slot.
    pub fn backend_status_shmem_init() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `pgstat_get_backend_type_by_proc_number(procNumber)` (backend_status.c):
    /// the `BackendType` advertised by the backend status entry at `procNumber`.
    /// Used by `signalfuncs.c` to recognize autovacuum workers (which do not
    /// advertise a role). Pure read of the shared status array; cannot `ereport`.
    pub fn pgstat_get_backend_type_by_proc_number(
        proc_number: types_core::ProcNumber,
    ) -> types_core::init::BackendType
);
