//! signalfuncs.c: pg_cancel_backend / pg_terminate_backend. Backends are
//! threads here: kill() renders as procsignal::SendThreadSignal, and the
//! kill(pid, 0) liveness probe as a procarray membership check.

use core::sync::atomic::Ordering::Relaxed;

use ::datum::Datum;
use ::types_core::{BackendType, InvalidOid, Oid};
use ::types_error::{
    PgResult, ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE, WARNING,
};
use ::types_fmgr::{FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};
use ::types_storage::waiteventset::{WL_EXIT_ON_PM_DEATH, WL_LATCH_SET, WL_TIMEOUT};

#[cfg(test)]
mod tests;

const SIGNAL_BACKEND_SUCCESS: i32 = 0;
const SIGNAL_BACKEND_ERROR: i32 = 1;
const SIGNAL_BACKEND_NOPERMISSION: i32 = 2;
const SIGNAL_BACKEND_NOSUPERUSER: i32 = 3;
const SIGNAL_BACKEND_NOAUTOVAC: i32 = 4;

const ROLE_PG_SIGNAL_BACKEND: Oid = 4200;
const ROLE_PG_SIGNAL_AUTOVACUUM_WORKER: Oid = 6392;

const PG_WAIT_IPC: u32 = 0x0800_0000;
const WAIT_EVENT_BACKEND_TERMINATION: u32 = PG_WAIT_IPC + 3;

fn warn(msg: String) -> PgResult<()> {
    elog::elog(WARNING, msg)
}

fn pg_signal_backend(pid: i32, sig: i32) -> PgResult<i32> {
    let Some(proc) = procarray::BackendPidGetProc(pid) else {
        warn(format!("PID {pid} is not a PostgreSQL backend process"))?;
        return Ok(SIGNAL_BACKEND_ERROR);
    };

    let role_id = proc.roleId.load(Relaxed);
    let user_id = miscinit_seams::get_user_id::call();
    if role_id == InvalidOid || superuser_seams::superuser_arg::call(role_id)? {
        let procno = get_number_from_pgproc(proc);
        let backend_type = backend_status::pgstat_get_backend_type_by_proc_number(procno);
        if backend_type == BackendType::AutovacWorker {
            if !acl_seams::has_privs_of_role::call(user_id, ROLE_PG_SIGNAL_AUTOVACUUM_WORKER)? {
                return Ok(SIGNAL_BACKEND_NOAUTOVAC);
            }
        } else if !superuser_seams::superuser::call()? {
            return Ok(SIGNAL_BACKEND_NOSUPERUSER);
        }
    } else if !acl_seams::has_privs_of_role::call(user_id, role_id)?
        && !acl_seams::has_privs_of_role::call(user_id, ROLE_PG_SIGNAL_BACKEND)?
    {
        return Ok(SIGNAL_BACKEND_NOPERMISSION);
    }

    if procsignal::SendThreadSignal(pid, sig) != 0 {
        warn(format!("could not send signal to process {pid}: No such process"))?;
        return Ok(SIGNAL_BACKEND_ERROR);
    }
    Ok(SIGNAL_BACKEND_SUCCESS)
}

// C GetNumberFromPGProc: index of the entry within ProcGlobal->allProcs.
fn get_number_from_pgproc(proc: &types_storage::storage::PGPROC) -> i32 {
    let procs = lmgr_proc::ProcGlobal().allProcs;
    ((core::ptr::from_ref(proc) as usize - procs.as_ptr() as usize)
        / core::mem::size_of::<types_storage::storage::PGPROC>()) as i32
}

// wasm32: the wasi libc crate exposes no SIG* names; 2/15 are SIGINT/SIGTERM
// in the thread-signal emulation's Linux-numbered space (procsignal wasm arm).
#[cfg(not(target_family = "wasm"))]
const SIGINT: i32 = libc::SIGINT;
#[cfg(not(target_family = "wasm"))]
const SIGTERM: i32 = libc::SIGTERM;
#[cfg(target_family = "wasm")]
const SIGINT: i32 = 2;
#[cfg(target_family = "wasm")]
const SIGTERM: i32 = 15;

#[cold]
fn signal_denied(errmsg: &str, errdetail: &str) -> Box<types_error::PgError> {
    Box::new(
        elog::ereport(types_error::ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg(errmsg.to_string())
            .errdetail(errdetail.to_string())
            .into_error(),
    )
}

pub fn fc_pg_cancel_backend(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let r = pg_signal_backend(fcinfo.arg_i32(0), SIGINT)?;
    let detail = match r {
        SIGNAL_BACKEND_NOSUPERUSER => {
            "Only roles with the SUPERUSER attribute may cancel queries of roles with the \
             SUPERUSER attribute."
        }
        SIGNAL_BACKEND_NOAUTOVAC => {
            "Only roles with privileges of the \"pg_signal_autovacuum_worker\" role may cancel \
             autovacuum workers."
        }
        SIGNAL_BACKEND_NOPERMISSION => {
            "Only roles with privileges of the role whose query is being canceled or with \
             privileges of the \"pg_signal_backend\" role may cancel this query."
        }
        _ => return Ok(Datum::from_bool(r == SIGNAL_BACKEND_SUCCESS)),
    };
    Err(signal_denied("permission denied to cancel query", detail).into())
}

// pg_wait_until_termination: the kill(pid, 0)/ESRCH probe is a procarray
// membership check in the thread model (the slot clears at backend exit).
fn pg_wait_until_termination(pid: i32, timeout: i64) -> PgResult<bool> {
    let mut waittime: i64 = 100;
    let mut remainingtime = timeout;
    loop {
        if remainingtime < waittime {
            waittime = remainingtime;
        }
        if procarray::BackendPidGetProc(pid).is_none() {
            return Ok(true);
        }
        if init_small::globals::InterruptPending() {
            postgres_seams::check_for_interrupts::call()?;
        }
        latch::WaitLatch(
            init_small::globals::MyLatch(),
            WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
            waittime,
            WAIT_EVENT_BACKEND_TERMINATION,
        )?;
        if let Some(l) = init_small::globals::MyLatch() {
            latch::ResetLatch(l);
        }
        remainingtime -= waittime;
        if remainingtime <= 0 {
            break;
        }
    }
    let unit = if timeout == 1 { "millisecond" } else { "milliseconds" };
    warn(format!("backend with PID {pid} did not terminate within {timeout} {unit}"))?;
    Ok(false)
}

pub fn fc_pg_terminate_backend(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let pid = fcinfo.arg_i32(0);
    let timeout = fcinfo.arg_i64(1);

    if timeout < 0 {
        return Err(elog::ereport(types_error::ERROR)
            .errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
            .errmsg("\"timeout\" must not be negative".to_string())
            .into_error()
            .into());
    }

    let r = pg_signal_backend(pid, SIGTERM)?;
    let detail = match r {
        SIGNAL_BACKEND_NOSUPERUSER => Some(
            "Only roles with the SUPERUSER attribute may terminate processes of roles with \
             the SUPERUSER attribute.",
        ),
        SIGNAL_BACKEND_NOAUTOVAC => Some(
            "Only roles with privileges of the \"pg_signal_autovacuum_worker\" role may \
             terminate autovacuum workers.",
        ),
        SIGNAL_BACKEND_NOPERMISSION => Some(
            "Only roles with privileges of the role whose process is being terminated or with \
             privileges of the \"pg_signal_backend\" role may terminate this process.",
        ),
        _ => None,
    };
    if let Some(detail) = detail {
        return Err(signal_denied("permission denied to terminate process", detail).into());
    }

    if r == SIGNAL_BACKEND_SUCCESS && timeout > 0 {
        Ok(Datum::from_bool(pg_wait_until_termination(pid, timeout)?))
    } else {
        Ok(Datum::from_bool(r == SIGNAL_BACKEND_SUCCESS))
    }
}

pub fn fc_pg_reload_conf(_flinfo: Option<&mut FmgrInfo>, _fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // kill(PostmasterPid, SIGHUP)'s thread rendering cannot fail, so C's
    // false-with-WARNING arm is unreachable.
    postmaster_seams::signal_postmaster_sighup::call();
    Ok(Datum::from_bool(true))
}

pub fn fc_pg_rotate_logfile(
    _flinfo: Option<&mut FmgrInfo>,
    _fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    if !guc_tables::vars::Logging_collector.read() {
        warn("rotation not possible because log collection not active".to_string())?;
        return Ok(Datum::from_bool(false));
    }
    pmsignal::SendPostmasterSignal(pmsignal::PMSignalReason::PMSIGNAL_ROTATE_LOGFILE);
    Ok(Datum::from_bool(true))
}

const fn b(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset: false, func }
}

pub const SIGNALFUNCS_BUILTINS: &[FmgrBuiltin] = &[
    b(2096, "pg_terminate_backend", 2, fc_pg_terminate_backend),
    b(2171, "pg_cancel_backend", 1, fc_pg_cancel_backend),
    b(2621, "pg_reload_conf", 0, fc_pg_reload_conf),
    b(2622, "pg_rotate_logfile", 0, fc_pg_rotate_logfile),
];
