//! Port of `src/backend/postmaster/postmaster.c`: the postmaster supervisor.
//!
//! ## Scope of this crate (incremental)
//!
//! postmaster.c is the ~4600-line server supervisor: `PostmasterMain`, the
//! `ServerLoop`, the `pmState` machine (`PM_INIT` … `PM_NO_CHILDREN`), the
//! SIGCHLD `reaper`/`process_pm_child_exit`/`HandleChildCrash` path, child
//! launch (`BackendStartup`/`StartChildProcess`/`StartBackgroundWorker`),
//! `SignalChildren`/`CountChildren`, and `DetermineSleepTime`/
//! `maybe_start_bgworkers`. That full spine is a multi-session campaign (62
//! file-static globals + an 11-state machine + a large new fork/exec/socket/
//! signal seam surface) and is **not** ported here yet.
//!
//! What *is* ported here is the self-contained, already-consumed slice that the
//! rest of the tree depends on today:
//!
//!   * [`PostmasterMarkPIDForWorkerNotify`] — installed as the
//!     `postmaster_mark_pid_for_worker_notify` seam, which `bgworker.c`'s
//!     `BackgroundWorkerStateChange` already calls. Until now nothing installed
//!     it, so that call panicked; this crate makes it real.
//!
//! The function is faithful 100%-C logic over pmchild's `ActiveChildList`
//! access primitive (pmchild owns the list + the `PMChild` slab; postmaster.c
//! iterates and mutates entries through them).

#![allow(non_snake_case)]

// ---------------------------------------------------------------------------
// PostmasterMarkPIDForWorkerNotify
// ---------------------------------------------------------------------------

/// `PostmasterMarkPIDForWorkerNotify(int pid)` (postmaster.c) — record that the
/// backend with the given `pid` wants background-worker start/stop
/// notifications, by setting `bgworker_notify` on its `PMChild` entry. Returns
/// whether such a backend was found.
///
/// ```c
/// dlist_foreach(iter, &ActiveChildList) {
///     bp = dlist_container(PMChild, elem, iter.cur);
///     if (bp->pid == pid) { bp->bgworker_notify = true; return true; }
/// }
/// return false;
/// ```
///
/// `ActiveChildList` and the `PMChild` slab are owned by the pmchild unit here,
/// so the find-by-pid + in-place `bgworker_notify = true` runs through pmchild's
/// [`MarkActiveChildBgworkerNotify`](backend_postmaster_pmchild::MarkActiveChildBgworkerNotify)
/// primitive (same semantics, list owned privately).
pub fn PostmasterMarkPIDForWorkerNotify(pid: i32) -> bool {
    backend_postmaster_pmchild::MarkActiveChildBgworkerNotify(pid)
}

// ---------------------------------------------------------------------------
// Seam installation
// ---------------------------------------------------------------------------

/// Install this crate's seam implementations.
pub fn init_seams() {
    backend_postmaster_postmaster_seams::postmaster_mark_pid_for_worker_notify::set(
        PostmasterMarkPIDForWorkerNotify,
    );
}

#[cfg(test)]
mod tests;
