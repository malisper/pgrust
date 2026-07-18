//! autovacuum.c: GUC homes, AutoVacuumingActive, the launcher (launcher.rs),
//! the worker + do_autovacuum (worker.rs), cost balancing (cost.rs), and the
//! thread-native AutoVacuumShmem (shmem.rs).

#![allow(non_snake_case)]

use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, Ordering};

use guc_tables::{vars, GucVarAccessors};
use types_error::{ErrorLocation, ERRCODE_INVALID_PARAMETER_VALUE, WARNING};

mod cost;
mod launcher;
mod shmem;
mod worker;
pub use cost::{AutoVacuumUpdateCostLimit, VacuumUpdateCosts};
pub use launcher::{AutoVacLauncherMain, AutoVacWorkerFailed};
pub use worker::{do_autovacuum, AutoVacWorkerMain, AutoVacuumRequestWork};

const AUTOVACUUM_C: &str = "src/backend/postmaster/autovacuum.c";

macro_rules! av_int {
    ($($cell:ident, $var:ident, $getter:ident, $boot:expr;)+) => {
        $(
            static $cell: AtomicI32 = AtomicI32::new($boot);
            pub fn $getter() -> i32 {
                $cell.load(Ordering::Relaxed)
            }
        )+
        fn install_ints() {
            $(
                vars::$var.install(GucVarAccessors {
                    get: $getter,
                    set: |v| $cell.store(v, Ordering::Relaxed),
                });
            )+
        }
    };
}

macro_rules! av_real {
    ($($cell:ident, $var:ident, $getter:ident, $boot:expr;)+) => {
        $(
            static $cell: AtomicU64 = AtomicU64::new(($boot as f64).to_bits());
            pub fn $getter() -> f64 {
                f64::from_bits($cell.load(Ordering::Relaxed))
            }
        )+
        fn install_reals() {
            $(
                vars::$var.install(GucVarAccessors {
                    get: $getter,
                    set: |v| $cell.store(v.to_bits(), Ordering::Relaxed),
                });
            )+
        }
    };
}

av_int! {
    AV_WORKER_SLOTS, autovacuum_worker_slots, autovacuum_worker_slots, 16;
    AV_MAX_WORKERS, autovacuum_max_workers, autovacuum_max_workers, 3;
    AV_WORK_MEM, autovacuum_work_mem, autovacuum_work_mem, -1;
    AV_NAPTIME, autovacuum_naptime, autovacuum_naptime, 60;
    AV_VAC_THRESH, autovacuum_vac_thresh, autovacuum_vac_thresh, 50;
    AV_VAC_MAX_THRESH, autovacuum_vac_max_thresh, autovacuum_vac_max_thresh, 100000000;
    AV_VAC_INS_THRESH, autovacuum_vac_ins_thresh, autovacuum_vac_ins_thresh, 1000;
    AV_ANL_THRESH, autovacuum_anl_thresh, autovacuum_anl_thresh, 50;
    AV_MXID_FREEZE_MAX_AGE, autovacuum_multixact_freeze_max_age, autovacuum_multixact_freeze_max_age, 400000000;
    AV_VAC_COST_LIMIT, autovacuum_vac_cost_limit, autovacuum_vac_cost_limit, -1;
    LOG_AV_MIN_DURATION, Log_autovacuum_min_duration, Log_autovacuum_min_duration, 600000;
}

av_real! {
    AV_VAC_SCALE, autovacuum_vac_scale, autovacuum_vac_scale, 0.2;
    AV_VAC_INS_SCALE, autovacuum_vac_ins_scale, autovacuum_vac_ins_scale, 0.2;
    AV_ANL_SCALE, autovacuum_anl_scale, autovacuum_anl_scale, 0.1;
    AV_VAC_COST_DELAY, autovacuum_vac_cost_delay, autovacuum_vac_cost_delay, 2.0;
}

static AV_START_DAEMON: AtomicBool = AtomicBool::new(true);

pub fn autovacuum_start_daemon() -> bool {
    AV_START_DAEMON.load(Ordering::Relaxed)
}

pub fn AutoVacuumingActive() -> bool {
    autovacuum_start_daemon() && guc_tables::vars::pgstat_track_counts.read()
}

pub fn autovac_init() {
    if !autovacuum_start_daemon() {
        return;
    }
    if !guc_tables::vars::pgstat_track_counts.read() {
        let _ = elog::ereport(WARNING)
            .errmsg("autovacuum not started because of misconfiguration")
            .errhint("Enable the \"track_counts\" option.")
            .finish(loc(3347, "autovac_init"));
    } else {
        check_av_worker_gucs();
    }
}

fn check_av_worker_gucs() {
    let slots = autovacuum_worker_slots();
    let max_workers = autovacuum_max_workers();
    if slots < max_workers {
        let _ = elog::ereport(WARNING)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!(
                "\"autovacuum_max_workers\" ({max_workers}) should be less than or equal to \"autovacuum_worker_slots\" ({slots})"
            ))
            .errdetail(format!(
                "The server will only start up to \"autovacuum_worker_slots\" ({slots}) autovacuum workers at a given time."
            ))
            .finish(loc(3470, "check_av_worker_gucs"));
    }
}

fn loc(lineno: i32, funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new(AUTOVACUUM_C, lineno, funcname)
}

pub fn init_seams() {
    install_ints();
    install_reals();
    vars::autovacuum_start_daemon.install(GucVarAccessors {
        get: autovacuum_start_daemon,
        set: |v| AV_START_DAEMON.store(v, Ordering::Relaxed),
    });
    autovacuum_seams::autovac_init::set(autovac_init);
    autovacuum_seams::autovacuuming_active::set(AutoVacuumingActive);
    autovacuum_seams::vacuum_update_costs::set(cost::VacuumUpdateCosts);
    autovacuum_seams::auto_vacuum_update_cost_limit::set(cost::AutoVacuumUpdateCostLimit);
    // Fixture tests pre-install a no-op wake (no launcher there); keep it.
    if !autovacuum_seams::wake_autovacuum_launcher::is_installed() {
        autovacuum_seams::wake_autovacuum_launcher::set(wake_autovacuum_launcher);
    }
    autovacuum_seams::autovac_worker_failed::set(launcher::AutoVacWorkerFailed);
}

// ProcKill's kill(AutovacuumLauncherPid, SIGUSR2): only autovac workers carry
// a nonzero saved launcher pid (set by FreeWorkerInfo).
fn wake_autovacuum_launcher() {
    let pid = shmem::AUTOVACUUM_LAUNCHER_PID.get();
    if pid != 0 {
        let _ = procsignal::SendThreadSignal(pid, procsignal::signums::SIGUSR2);
    }
}
