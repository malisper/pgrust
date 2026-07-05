// backend_progress.c over backend_status's PgBackendStatus fields.
#![allow(non_upper_case_globals)]
#![allow(non_snake_case)]

use backend_status::{
    begin_write_activity, end_write_activity, MyBEEntry, PgBackendStatus,
    PGSTAT_NUM_PROGRESS_PARAM, PROGRESS_COMMAND_INVALID,
};
use types_core::{InvalidOid, Oid};

// ProgressCommandType (backend_progress.h); INVALID lives in backend_status.
pub const PROGRESS_COMMAND_VACUUM: i32 = 1;
pub const PROGRESS_COMMAND_ANALYZE: i32 = 2;
pub const PROGRESS_COMMAND_CLUSTER: i32 = 3;
pub const PROGRESS_COMMAND_CREATE_INDEX: i32 = 4;
pub const PROGRESS_COMMAND_BASEBACKUP: i32 = 5;
pub const PROGRESS_COMMAND_COPY: i32 = 6;

pub mod progress;

fn with_write_bracket(f: impl FnOnce(&'static PgBackendStatus)) {
    let Some(beentry) = MyBEEntry() else {
        return;
    };
    if !backend_status::pgstat_track_activities() {
        return;
    }
    begin_write_activity(beentry);
    f(beentry);
    end_write_activity(beentry);
}

pub fn pgstat_progress_start_command(cmdtype: i32, relid: Oid) {
    with_write_bracket(|be| {
        be.st_progress_command.set(cmdtype);
        be.st_progress_command_target.set(relid);
        for p in &be.st_progress_param {
            p.set(0);
        }
    });
}

pub fn pgstat_progress_update_param(index: usize, val: i64) {
    debug_assert!(index < PGSTAT_NUM_PROGRESS_PARAM);
    with_write_bracket(|be| be.st_progress_param[index].set(val));
}

pub fn pgstat_progress_incr_param(index: usize, incr: i64) {
    debug_assert!(index < PGSTAT_NUM_PROGRESS_PARAM);
    with_write_bracket(|be| {
        let p = &be.st_progress_param[index];
        p.set(p.get() + incr);
    });
}

pub fn pgstat_progress_parallel_incr_param(index: usize, incr: i64) {
    if parallel_seams::is_parallel_worker::call() {
        parallel_seams::parallel_worker_report_progress::call(index as i32, incr);
    } else {
        pgstat_progress_incr_param(index, incr);
    }
}

pub fn pgstat_progress_update_multi_param(indices: &[usize], vals: &[i64]) {
    debug_assert_eq!(indices.len(), vals.len());
    if indices.is_empty() {
        return;
    }
    with_write_bracket(|be| {
        for (&i, &v) in indices.iter().zip(vals) {
            debug_assert!(i < PGSTAT_NUM_PROGRESS_PARAM);
            be.st_progress_param[i].set(v);
        }
    });
}

pub fn pgstat_progress_end_command() {
    let Some(beentry) = MyBEEntry() else {
        return;
    };
    if !backend_status::pgstat_track_activities() {
        return;
    }
    if beentry.st_progress_command.get() == PROGRESS_COMMAND_INVALID {
        return;
    }
    begin_write_activity(beentry);
    beentry.st_progress_command.set(PROGRESS_COMMAND_INVALID);
    beentry.st_progress_command_target.set(InvalidOid);
    end_write_activity(beentry);
}

pub fn init_seams() {
    backend_progress_seams::pgstat_progress_end_command::set(pgstat_progress_end_command);
}
