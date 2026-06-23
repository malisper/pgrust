//! Inward seam + GUC-hook installation for `backend-replication-syncrep`.
//!
//! `init_seams()` installs every seam this crate owns
//! (`backend-replication-syncrep-seams`) and the three `synchronous_*` GUC
//! hooks (the slots in `backend-utils-misc-guc-tables`).  The adapters here are
//! thin marshal + delegate into the real functions in [`crate`].

use alloc::boxed::Box;
use alloc::string::String;
use alloc::vec::Vec;

use ::types_core::primitive::XLogRecPtr;
use ::types_error::PgResult;

use syncrep_seams as own;
use ::guc_tables::hooks;
use ::guc_tables::vars;
use ::guc_tables::GucVarAccessors;
use ::guc_tables::GucHookExtra;
use ::types_guc::GucSource;

use crate::{
    assign_synchronous_commit, assign_synchronous_standby_names, check_synchronous_standby_names,
    sync_rep_method, CheckResult, SyncRepCleanupAtProcExit, SyncRepConfig, SyncRepGetCandidateStandbys,
    SyncRepInitConfig, SyncRepUpdateSyncStandbysDefined, SyncRepWaitForLSN, SYNC_REP_PRIORITY,
};

// ---- inward seams (backend-replication-syncrep-seams) ----

fn sync_rep_wait_for_lsn(lsn: XLogRecPtr, commit: bool) -> PgResult<()> {
    SyncRepWaitForLSN(lsn, commit)
}

fn sync_rep_cleanup_at_proc_exit() {
    // C `SyncRepCleanupAtProcExit` is void; the only fallible step is the
    // SyncRepLock acquire (the "too many LWLocks" elog), which cannot fire on
    // the backend-exit path.
    SyncRepCleanupAtProcExit().expect("SyncRepCleanupAtProcExit");
}

fn sync_rep_update_sync_standbys_defined() -> PgResult<()> {
    SyncRepUpdateSyncStandbysDefined()
}

fn sync_rep_init_config() {
    // The walsender startup / SIGHUP path; the only fallible step is the DEBUG1
    // ereport, which never raises ERROR at this elevel.
    SyncRepInitConfig().expect("SyncRepInitConfig");
}

/// `SyncRepGetCandidateStandbys(&standbys)` projected to the
/// `(walsnd_index, pid)` pairs `pg_stat_get_wal_senders` matches on.
fn sync_rep_get_candidate_standbys() -> Vec<(i32, i32)> {
    let config = match current_config() {
        Some(c) => c,
        // C: `if (SyncRepConfig == NULL) return 0;`.
        None => return Vec::new(),
    };
    SyncRepGetCandidateStandbys(&config)
        .into_iter()
        .map(|s| (s.walsnd_index, s.pid))
        .collect()
}

fn sync_rep_config_is_priority() -> bool {
    // C `SyncRepConfigIsPriority()`: `SyncRepConfig->syncrep_method ==
    // SYNC_REP_PRIORITY`. `sync_rep_method()` returns that, defaulting to
    // SYNC_REP_PRIORITY when unset (only reached when configured).
    sync_rep_method() == SYNC_REP_PRIORITY
}

/// Read the parsed `SyncRepConfig` thread-local (the candidate seam needs it,
/// matching the C `SyncRepConfig` global check at the top of
/// `SyncRepGetCandidateStandbys`).
fn current_config() -> Option<SyncRepConfig> {
    crate::SYNC_REP_CONFIG.with(|cell| cell.borrow().clone())
}

// ---- GUC hooks (backend-utils-misc-guc-tables slots) ----

/// `check_synchronous_standby_names(char **newval, void **extra, GucSource)`.
fn check_hook(
    newval: &mut Option<String>,
    extra: &mut Option<GucHookExtra>,
    _source: GucSource,
) -> PgResult<bool> {
    match check_synchronous_standby_names(newval.as_deref())? {
        CheckResult::Ok(config) => {
            *extra = config.map(|c| Box::new(c) as GucHookExtra);
            Ok(true)
        }
        CheckResult::Reject => Ok(false),
    }
}

/// `assign_synchronous_standby_names(const char *newval, void *extra)`.
fn assign_standby_names_hook(newval: Option<&str>, extra: Option<&GucHookExtra>) {
    // The GUC machinery sets `SyncRepStandbyNames = newval` (its `conf->variable`)
    // before invoking the assign hook; mirror that store write here so the var
    // accessor reads the live value, then stash the parsed config like C does.
    crate::set_sync_rep_standby_names(newval.map(String::from));
    let config = extra.and_then(|e| (**e).downcast_ref::<SyncRepConfig>().cloned());
    assign_synchronous_standby_names(config);
}

/// `assign_synchronous_commit(int newval, void *extra)`.
fn assign_synchronous_commit_hook(newval: i32, _extra: Option<&GucHookExtra>) {
    assign_synchronous_commit(newval);
}

pub fn init_seams() {
    own::sync_rep_wait_for_lsn::set(sync_rep_wait_for_lsn);
    own::sync_rep_cleanup_at_proc_exit::set(sync_rep_cleanup_at_proc_exit);
    own::sync_rep_update_sync_standbys_defined::set(sync_rep_update_sync_standbys_defined);
    own::sync_rep_init_config::set(sync_rep_init_config);
    own::sync_rep_get_candidate_standbys::set(sync_rep_get_candidate_standbys);
    own::sync_rep_config_is_priority::set(sync_rep_config_is_priority);

    // GUC var accessor for `synchronous_standby_names` (`char *SyncRepStandbyNames`,
    // syncrep.c) — `conf->variable` read/written via this crate's backing store.
    vars::SyncRepStandbyNames.install(GucVarAccessors {
        get: crate::sync_rep_standby_names,
        set: crate::set_sync_rep_standby_names,
    });

    hooks::check_synchronous_standby_names.install(check_hook);
    hooks::assign_synchronous_standby_names.install(assign_standby_names_hook);
    hooks::assign_synchronous_commit.install(assign_synchronous_commit_hook);
}
