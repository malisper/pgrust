//! Thin marshal-and-delegate wrappers for the `backend-utils-activity-status`
//! seams. All real logic lives in `lib.rs`; these only adapt the seam contract
//! types (the trimmed `::types_pgstat::backend_status::PgBackendStatus` view, the
//! `String`-carrying activity reporters) to the internal functions.

use core::sync::atomic::Ordering;

use ::types_core::Size;
use ::types_error::PgResult;
use ::types_pgstat::backend_status::PgBackendStatus as BeView;

use crate::{
    pgstat_get_backend_current_activity, pgstat_report_activity, MyBEEntry, PgBackendStatusEntry,
    STATE_IDLE, STATE_RUNNING,
};

pub(crate) fn backend_status_shmem_size() -> PgResult<Size> {
    crate::BackendStatusShmemSize()
}

pub(crate) fn backend_status_shmem_init() -> PgResult<()> {
    crate::BackendStatusShmemInit()
}

/// `MyBEEntry != NULL`.
pub(crate) fn my_be_entry_present() -> bool {
    !MyBEEntry().is_null()
}

/// Run `f` on this backend's live `*MyBEEntry`, exposed as the trimmed
/// `::types_pgstat::backend_status::PgBackendStatus` view (changecount + progress
/// fields). Callers (`backend_progress.c`) write the progress fields and run
/// their own `PGSTAT_BEGIN/END_WRITE_ACTIVITY` bracketing on the view's
/// `AtomicU32` changecount; we copy the four fields out of the in-segment entry
/// into the view, run the callback, then copy them back. Sound because the
/// entry is only written by this backend, synchronously within the callback.
pub(crate) fn with_my_beentry(f: &mut dyn FnMut(&mut BeView)) {
    let beentry: *mut PgBackendStatusEntry = MyBEEntry();
    debug_assert!(!beentry.is_null());

    // SAFETY: callers must only invoke this after `my_be_entry_present()` is
    // true, so `beentry` is the live, shmem-resident MyBEEntry.
    let mut view = BeView::default();
    unsafe {
        view.st_changecount
            .store((*beentry).st_changecount as u32, Ordering::Relaxed);
        view.st_progress_command = (*beentry).st_progress_command;
        view.st_progress_command_target = (*beentry).st_progress_command_target;
        view.st_progress_param = (*beentry).st_progress_param;
    }

    f(&mut view);

    // SAFETY: as above.
    unsafe {
        (*beentry).st_changecount = view.st_changecount.load(Ordering::Relaxed) as i32;
        (*beentry).st_progress_command = view.st_progress_command;
        (*beentry).st_progress_command_target = view.st_progress_command_target;
        (*beentry).st_progress_param = view.st_progress_param;
    }
}

/// `pgstat_get_backend_current_activity(pid, check_user)` as a UTF-8 `String`.
/// The activity bytes are server-encoded; the special-case messages are ASCII.
/// `from_utf8_lossy` keeps the deadlock-log path infallible (the seam contract
/// is `String`, not bytes).
pub(crate) fn backend_current_activity(pid: i32, check_user: bool) -> String {
    let bytes = pgstat_get_backend_current_activity(pid, check_user);
    String::from_utf8_lossy(&bytes).into_owned()
}

/// `pgstat_report_activity(STATE_IDLE, NULL)`.
pub(crate) fn pgstat_report_activity_idle() {
    pgstat_report_activity(STATE_IDLE, None);
}

/// `pgstat_report_activity(STATE_RUNNING, cmd_str)`.
pub(crate) fn pgstat_report_activity_running(cmd_str: String) {
    pgstat_report_activity(STATE_RUNNING, Some(cmd_str.as_bytes()));
}
