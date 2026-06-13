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

/// Install this crate's implementations into its seam crate.
pub fn init_seams() {
    backend_postmaster_autovacuum_seams::auto_vac_launcher_main::set(auto_vac_launcher_main_entry);
    backend_postmaster_autovacuum_seams::auto_vac_worker_main::set(auto_vac_worker_main_entry);
}
