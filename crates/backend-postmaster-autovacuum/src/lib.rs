#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::needless_range_loop)]

//! `backend/postmaster/autovacuum.c` — the PostgreSQL Integrated Autovacuum
//! Daemon, ported from PostgreSQL 18.3.
//!
//! The autovacuum system is two kinds of process: the always-running
//! **launcher** (which schedules workers per-database) and the **workers**
//! (forked by the postmaster, which examine the catalogs to pick tables to
//! vacuum/analyze). Both halves share an autovacuum shared-memory area and a
//! set of GUC globals.
//!
//! Every scheduling/scoring/threshold/balancing algorithm of `autovacuum.c` is
//! ported here, with the C control flow / branch order / arithmetic kept 1:1.
//! The cross-subsystem runtime this daemon drives (the postmaster fork/signal
//! path, the catalog/relcache seqscans, the vacuum executor, pgstat, the shmem
//! allocator, the latch, transactions/snapshots) is not yet ported, so every
//! such boundary routes through a loud-panic ext-seam
//! ([`backend_postmaster_autovacuum_ext_seams`]).
//!
//! The `AutoVacuumShmem` struct and the `WorkerInfo` array live in real
//! PostgreSQL shared memory (the substrate owns the layout); this crate never
//! holds a `&mut WorkerInfoData` — it addresses worker slots / work items by
//! index through the index-keyed accessor ext-seams. The launcher's
//! `DatabaseList` is process-local (a private memory context, not shmem) and is
//! ported as a per-backend owned `Vec<AvlDbase>`.

extern crate alloc;

pub mod core;
pub mod cost_balance;
pub mod launcher;
pub mod schedule;
pub mod shmem;
pub mod substrate;
pub mod worker;

#[cfg(test)]
mod tests;

pub use cost_balance::*;
pub use launcher::*;
pub use schedule::*;
pub use shmem::*;
pub use worker::*;

use types_startup::StartupData;

/// `-> !` adapter for the launcher entry-point seam. The per-backend lifecycle
/// setup (`InitProcess`/`BaseInit`/`InitPostgres`, signal handlers, the
/// `sigsetjmp` error-recovery frame) is owned by the child-launch machinery
/// and not yet ported; this adapter runs the post-setup loop body and, on the
/// `pg_noreturn` C contract, exits the process either via the body's own
/// `proc_exit` (the Ok path never returns) or via `proc_exit(1)` after an
/// uncaught error.
fn auto_vac_launcher_main_entry(_startup_data: &StartupData) -> ! {
    match launcher::AutoVacLauncherMain() {
        Ok(()) => backend_postmaster_autovacuum_ext_seams::proc_exit::call(0),
        Err(_) => backend_postmaster_autovacuum_ext_seams::proc_exit::call(1),
    }
}

/// `-> !` adapter for the worker entry-point seam (see the launcher adapter).
fn auto_vac_worker_main_entry(_startup_data: &StartupData) -> ! {
    match worker::AutoVacWorkerMain() {
        Ok(()) => backend_postmaster_autovacuum_ext_seams::proc_exit::call(0),
        Err(_) => backend_postmaster_autovacuum_ext_seams::proc_exit::call(1),
    }
}

/// `AmAutoVacuumLauncherProcess()` (miscadmin.h) — `MyBackendType ==
/// B_AUTOVAC_LAUNCHER`. A pure macro over the `MyBackendType` global read
/// through the init-small owner's seam (mirrors proc seam.rs).
pub fn am_autovacuum_launcher_process() -> bool {
    backend_utils_init_small_seams::my_backend_type::call()
        == types_core::init::BackendType::AutovacLauncher
}

/// `AmAutoVacuumWorkerProcess()` (miscadmin.h) — `MyBackendType ==
/// B_AUTOVAC_WORKER`.
pub fn am_autovacuum_worker_process() -> bool {
    backend_utils_init_small_seams::my_backend_type::call()
        == types_core::init::BackendType::AutovacWorker
}

/// Install this crate's implementations into its seam crate.
pub fn init_seams() {
    // `VacuumUpdateCosts()` (autovacuum.c) — the cost-delay parameter refresh
    // both autovacuum workers and foreground VACUUM/ANALYZE call during setup.
    backend_commands_vacuum_seams::vacuum_update_costs::set(cost_balance::VacuumUpdateCosts);
    backend_postmaster_autovacuum_seams::auto_vac_launcher_main::set(auto_vac_launcher_main_entry);
    backend_postmaster_autovacuum_seams::auto_vac_worker_main::set(auto_vac_worker_main_entry);
    backend_postmaster_autovacuum_seams::am_autovacuum_launcher_process::set(
        am_autovacuum_launcher_process,
    );
    backend_postmaster_autovacuum_seams::am_autovacuum_worker_process::set(
        am_autovacuum_worker_process,
    );
    // Pure-wiring installs (assemble/seam-wiring-guard): owner bodies match.
    backend_postmaster_autovacuum_seams::autovacuum_worker_slots::set(core::autovacuum_worker_slots);
    backend_postmaster_autovacuum_seams::auto_vacuum_shmem_init::set(shmem::AutoVacuumShmemInit);
    // Contract-reconciled install (assemble/seam-contract-reconciles): the seam
    // is now the infallible `-> Size` shape, matching the C `Size` return.
    backend_postmaster_autovacuum_seams::auto_vacuum_shmem_size::set(shmem::AutoVacuumShmemSize);
    backend_postmaster_autovacuum_seams::auto_vacuuming_active::set(shmem::AutoVacuumingActive);

    // The postmaster (PostmasterMain + the maybe_start_autovac_launcher
    // scheduler) calls `autovac_init()` once at startup and reads
    // `AutoVacuumingActive()`. C returns void from autovac_init (the
    // misconfiguration WARNING is logged, not propagated), so the PgResult is
    // discarded, matching the C call site.
    backend_postmaster_postmaster_seams::autovac_init::set(|| {
        let _ = shmem::autovac_init();
    });
    backend_postmaster_postmaster_seams::autovacuuming_active::set(shmem::AutoVacuumingActive);
    // `AutoVacWorkerFailed()` (autovacuum.c) — the postmaster's
    // StartAutovacuumWorker calls this through its own seam when a worker fork
    // fails; the body is owned here.
    backend_postmaster_postmaster_seams::autovac_worker_failed::set(
        launcher::AutoVacWorkerFailed,
    );

    // Install the GUC var accessors for the autovacuum knobs whose
    // `conf->variable` backing (the per-backend `core::*` thread-locals) is
    // owned here, exactly as `guc_tables.c` binds each entry's variable
    // pointer (e.g. `&autovacuum_start_daemon`, `&autovacuum_vac_cost_limit`).
    // All are plain runtime GUCs read through the GUC slot — none come from
    // the ControlFile.
    {
        use backend_utils_misc_guc_tables::{vars, GucVarAccessors};
        // `autovacuum` → `&autovacuum_start_daemon` (bool).
        vars::autovacuum_start_daemon.install(GucVarAccessors {
            get: core::autovacuum_start_daemon,
            set: core::set_autovacuum_start_daemon,
        });
        vars::autovacuum_worker_slots.install(GucVarAccessors {
            get: core::autovacuum_worker_slots,
            set: core::set_autovacuum_worker_slots,
        });
        vars::autovacuum_max_workers.install(GucVarAccessors {
            get: core::autovacuum_max_workers,
            set: core::set_autovacuum_max_workers,
        });
        vars::autovacuum_work_mem.install(GucVarAccessors {
            get: core::autovacuum_work_mem,
            set: core::set_autovacuum_work_mem,
        });
        vars::autovacuum_naptime.install(GucVarAccessors {
            get: core::autovacuum_naptime,
            set: core::set_autovacuum_naptime,
        });
        vars::autovacuum_vac_thresh.install(GucVarAccessors {
            get: core::autovacuum_vac_thresh,
            set: core::set_autovacuum_vac_thresh,
        });
        vars::autovacuum_vac_max_thresh.install(GucVarAccessors {
            get: core::autovacuum_vac_max_thresh,
            set: core::set_autovacuum_vac_max_thresh,
        });
        vars::autovacuum_vac_scale.install(GucVarAccessors {
            get: core::autovacuum_vac_scale,
            set: core::set_autovacuum_vac_scale,
        });
        vars::autovacuum_vac_ins_thresh.install(GucVarAccessors {
            get: core::autovacuum_vac_ins_thresh,
            set: core::set_autovacuum_vac_ins_thresh,
        });
        vars::autovacuum_vac_ins_scale.install(GucVarAccessors {
            get: core::autovacuum_vac_ins_scale,
            set: core::set_autovacuum_vac_ins_scale,
        });
        vars::autovacuum_anl_thresh.install(GucVarAccessors {
            get: core::autovacuum_anl_thresh,
            set: core::set_autovacuum_anl_thresh,
        });
        vars::autovacuum_anl_scale.install(GucVarAccessors {
            get: core::autovacuum_anl_scale,
            set: core::set_autovacuum_anl_scale,
        });
        vars::autovacuum_freeze_max_age.install(GucVarAccessors {
            get: core::autovacuum_freeze_max_age,
            set: core::set_autovacuum_freeze_max_age,
        });
        vars::autovacuum_multixact_freeze_max_age.install(GucVarAccessors {
            get: core::autovacuum_multixact_freeze_max_age,
            set: core::set_autovacuum_multixact_freeze_max_age,
        });
        vars::autovacuum_vac_cost_delay.install(GucVarAccessors {
            get: core::autovacuum_vac_cost_delay,
            set: core::set_autovacuum_vac_cost_delay,
        });
        vars::autovacuum_vac_cost_limit.install(GucVarAccessors {
            get: core::autovacuum_vac_cost_limit,
            set: core::set_autovacuum_vac_cost_limit,
        });
        vars::Log_autovacuum_min_duration.install(GucVarAccessors {
            get: core::Log_autovacuum_min_duration,
            set: core::set_Log_autovacuum_min_duration,
        });
    }

    // The AutoVacuumShmem layout + accessor seams (the shmem substrate this
    // crate owns; autovacuum.c:231-417). These operate purely on the
    // AutoVacuumShmemStruct / WorkerInfoData[] this crate models.
    use backend_postmaster_autovacuum_ext_seams as ext;
    ext::autovacuum_shmem_init::set(substrate::auto_vacuum_shmem_init);
    ext::get_launcher_pid::set(substrate::get_launcher_pid);
    ext::set_launcher_pid::set(substrate::set_launcher_pid);
    ext::get_av_signal::set(substrate::get_av_signal);
    ext::set_av_signal::set(substrate::set_av_signal);
    ext::free_workers_count::set(substrate::free_workers_count);
    ext::free_workers_pop_head::set(substrate::free_workers_pop_head);
    ext::free_workers_push_head::set(substrate::free_workers_push_head);
    ext::running_workers_push_head::set(substrate::running_workers_push_head);
    ext::worker_links_delete::set(substrate::worker_links_delete);
    ext::running_workers_slots::set(substrate::running_workers_slots);
    ext::worker_get_dboid::set(substrate::worker_get_dboid);
    ext::worker_set_dboid::set(substrate::worker_set_dboid);
    ext::worker_get_tableoid::set(substrate::worker_get_tableoid);
    ext::worker_set_tableoid::set(substrate::worker_set_tableoid);
    ext::worker_get_sharedrel::set(substrate::worker_get_sharedrel);
    ext::worker_set_sharedrel::set(substrate::worker_set_sharedrel);
    ext::worker_get_launchtime::set(substrate::worker_get_launchtime);
    ext::worker_set_launchtime::set(substrate::worker_set_launchtime);
    ext::worker_proc_is_set::set(substrate::worker_proc_is_set);
    ext::worker_set_proc::set(substrate::worker_set_proc);
    ext::worker_dobalance_unlocked_test::set(substrate::worker_dobalance_unlocked_test);
    ext::worker_dobalance_test_set::set(substrate::worker_dobalance_test_set);
    ext::worker_dobalance_clear::set(substrate::worker_dobalance_clear);
    ext::starting_worker_slot::set(substrate::starting_worker_slot);
    ext::set_starting_worker_slot::set(substrate::set_starting_worker_slot);
    ext::nworkers_for_balance_read::set(substrate::nworkers_for_balance_read);
    ext::nworkers_for_balance_write::set(substrate::nworkers_for_balance_write);
    ext::workitem_get_used::set(substrate::workitem_get_used);
    ext::workitem_get_active::set(substrate::workitem_get_active);
    ext::workitem_set_active::set(substrate::workitem_set_active);
    ext::workitem_set_used::set(substrate::workitem_set_used);
    ext::workitem_get_database::set(substrate::workitem_get_database);
    ext::workitem_get_type::set(substrate::workitem_get_type);
    ext::workitem_get_relation::set(substrate::workitem_get_relation);
    ext::workitem_get_block_number::set(substrate::workitem_get_block_number);
    ext::workitem_fill::set(substrate::workitem_fill);
}
