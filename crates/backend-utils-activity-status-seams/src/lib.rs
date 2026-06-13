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
    /// `pgstat_report_xact_timestamp(tstamp)` (backend_status.c).
    pub fn pgstat_report_xact_timestamp(tstamp: types_core::TimestampTz)
);
