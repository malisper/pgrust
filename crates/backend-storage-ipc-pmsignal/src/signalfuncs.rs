//! Port of `src/backend/storage/ipc/signalfuncs.c`: SQL-callable
//! backend-signaling functions.
//!
//! [`pg_cancel_backend`] (deliver `SIGINT`), [`pg_terminate_backend`] (deliver
//! `SIGTERM`, optionally waiting for the process to exit), [`pg_reload_conf`]
//! (`SIGHUP` the postmaster) and [`pg_rotate_logfile`] (ask the syslogger to
//! rotate). The shared decision core is [`pg_signal_backend`], which validates
//! the target pid against the proc array and runs the privilege ladder before
//! delivering the signal.
//!
//! Branch order, the `SIGNAL_BACKEND_*` return ladder, cancel/terminate error
//! texts + their `ERRCODE_INSUFFICIENT_PRIVILEGE` SQLSTATE, the `"timeout" must
//! not be negative` check, and the [`pg_wait_until_termination`] poll loop are
//! kept 1:1 with C. `BackendPidGetProc`, the superuser/role predicates, the
//! `Logging_collector` GUC, the `SendPostmasterSignal` rotation request, and
//! the latch substrate are reached through seams. `kill(2)` is the OS boundary
//! (`libc`).

use backend_utils_error::ereport;
use types_core::init::BackendType;
use types_error::{
    ErrorLocation, PgError, PgResult, ERRCODE_INSUFFICIENT_PRIVILEGE,
    ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE, ERROR, WARNING,
};
use types_pgstat::wait_event::WAIT_EVENT_BACKEND_TERMINATION;
use types_storage::waiteventset::{WL_EXIT_ON_PM_DEATH, WL_LATCH_SET, WL_TIMEOUT};

use crate::pmsignal::{PMSignalReason, SendPostmasterSignal};

const FILE: &str = "signalfuncs.c";

// pg_signal_backend return codes (C #defines).
/// `pg_signal_backend` succeeded: the signal was delivered.
pub const SIGNAL_BACKEND_SUCCESS: i32 = 0;
/// General failure (no such backend, or `kill` failed). A `WARNING` has already
/// been emitted; the caller does not raise an error.
pub const SIGNAL_BACKEND_ERROR: i32 = 1;
/// The caller lacks the privileges of the target backend's role.
pub const SIGNAL_BACKEND_NOPERMISSION: i32 = 2;
/// The caller must be a superuser to signal this (superuser-owned) backend.
pub const SIGNAL_BACKEND_NOSUPERUSER: i32 = 3;
/// The caller must hold `pg_signal_autovacuum_worker` to signal this autovacuum
/// worker.
pub const SIGNAL_BACKEND_NOAUTOVAC: i32 = 4;

/// `ROLE_PG_SIGNAL_BACKEND` (`catalog/pg_authid.h`).
const ROLE_PG_SIGNAL_BACKEND: types_core::Oid = 4200;
/// `ROLE_PG_SIGNAL_AUTOVACUUM_WORKER` (`catalog/pg_authid.h`).
const ROLE_PG_SIGNAL_AUTOVACUUM_WORKER: types_core::Oid = 6392;

fn loc(lineno: i32, funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new(FILE, lineno, funcname)
}

// ---------------------------------------------------------------------------
// pg_signal_backend
// ---------------------------------------------------------------------------

/// Send a signal to another backend.
///
/// The signal is delivered if the user is either a superuser or the same role
/// as the backend being signaled. For "dangerous" signals, an explicit check
/// for superuser needs to be done prior to calling this function.
///
/// Returns one of the `SIGNAL_BACKEND_*` codes. In the event of a general
/// failure ([`SIGNAL_BACKEND_ERROR`]), a warning has already been emitted;
/// for permission errors, raising one is the caller's responsibility.
///
/// ```c
/// static int pg_signal_backend(int pid, int sig)
/// ```
pub fn pg_signal_backend(pid: i32, sig: i32) -> PgResult<i32> {
    // PGPROC *proc = BackendPidGetProc(pid);
    //
    // BackendPidGetProc returns NULL if the pid isn't valid; by the time we
    // reach kill(), a process for which we get a valid proc here might have
    // terminated on its own. proc is also NULL for an auxiliary process or the
    // postmaster (neither of which can be signaled via pg_signal_backend()).
    let Some((role_id, proc_number)) =
        backend_storage_ipc_procarray_seams::backend_pid_get_proc_role::call(pid)
    else {
        // This is just a warning so a loop-through-resultset will not abort if
        // one backend terminated on its own during the run.
        ereport(WARNING)
            .errmsg(format!("PID {pid} is not a PostgreSQL backend process"))
            .finish(loc(74, "pg_signal_backend"))?;
        return Ok(SIGNAL_BACKEND_ERROR);
    };

    // Only allow superusers to signal superuser-owned backends. Any process not
    // advertising a role might have the importance of a superuser-owned
    // backend, so treat it that way. As an exception, we allow roles with
    // privileges of pg_signal_autovacuum_worker to signal autovacuum workers
    // (which do not advertise a role). Otherwise, users can signal backends for
    // roles they have privileges of.
    //
    // C: if (!OidIsValid(proc->roleId) || superuser_arg(proc->roleId))
    if role_id == types_core::InvalidOid
        || backend_utils_misc_superuser_seams::superuser_arg::call(role_id)?
    {
        // C: backendType = pgstat_get_backend_type_by_proc_number(procNumber);
        let backend_type =
            backend_utils_activity_status_seams::pgstat_get_backend_type_by_proc_number::call(
                proc_number,
            );

        if backend_type == BackendType::AutovacWorker {
            // C: !has_privs_of_role(GetUserId(), ROLE_PG_SIGNAL_AUTOVACUUM_WORKER)
            if !current_user_has_privs_of_role(ROLE_PG_SIGNAL_AUTOVACUUM_WORKER)? {
                return Ok(SIGNAL_BACKEND_NOAUTOVAC);
            }
        } else if !backend_utils_misc_superuser_seams::superuser::call()? {
            return Ok(SIGNAL_BACKEND_NOSUPERUSER);
        }
    } else if !current_user_has_privs_of_role(role_id)?
        && !current_user_has_privs_of_role(ROLE_PG_SIGNAL_BACKEND)?
    {
        return Ok(SIGNAL_BACKEND_NOPERMISSION);
    }

    // If we have setsid(), signal the backend's whole process group:
    //   #ifdef HAVE_SETSID   if (kill(-pid, sig))
    //   #else                if (kill(pid, sig))
    // setsid() is used by the backend startup, so deliver to the whole group.
    let killed = unsafe { libc::kill(-pid, sig) };
    if killed != 0 {
        // Again, just a warning to allow loops. The C "%m" expands to the errno
        // string; we report the failure without the raw errno text.
        ereport(WARNING)
            .errmsg(format!("could not send signal to process {pid}"))
            .finish(loc(123, "pg_signal_backend"))?;
        return Ok(SIGNAL_BACKEND_ERROR);
    }

    Ok(SIGNAL_BACKEND_SUCCESS)
}

/// `has_privs_of_role(GetUserId(), role)`.
#[inline]
fn current_user_has_privs_of_role(role: types_core::Oid) -> PgResult<bool> {
    let user = backend_utils_init_miscinit_seams::get_user_id::call();
    backend_utils_adt_acl_seams::has_privs_of_role::call(user, role)
}

// ---------------------------------------------------------------------------
// pg_cancel_backend
// ---------------------------------------------------------------------------

/// Signal to cancel a backend process (deliver `SIGINT`). Allowed if you are a
/// member of the role whose process is being canceled. Only superusers can
/// signal superuser-owned processes.
///
/// ```c
/// Datum pg_cancel_backend(PG_FUNCTION_ARGS)
/// ```
pub fn pg_cancel_backend(pid: i32) -> PgResult<bool> {
    let r = pg_signal_backend(pid, libc::SIGINT)?;

    if r == SIGNAL_BACKEND_NOSUPERUSER {
        return Err(privilege_error(
            "permission denied to cancel query",
            "Only roles with the SUPERUSER attribute may cancel queries of roles with the SUPERUSER attribute.",
        ));
    }

    if r == SIGNAL_BACKEND_NOAUTOVAC {
        return Err(privilege_error(
            "permission denied to cancel query",
            "Only roles with privileges of the \"pg_signal_autovacuum_worker\" role may cancel autovacuum workers.",
        ));
    }

    if r == SIGNAL_BACKEND_NOPERMISSION {
        return Err(privilege_error(
            "permission denied to cancel query",
            "Only roles with privileges of the role whose query is being canceled or with privileges of the \"pg_signal_backend\" role may cancel this query.",
        ));
    }

    Ok(r == SIGNAL_BACKEND_SUCCESS)
}

// ---------------------------------------------------------------------------
// pg_wait_until_termination
// ---------------------------------------------------------------------------

/// Wait until there is no backend process with the given PID and return `true`.
/// On timeout, a warning is emitted and `false` is returned.
///
/// ```c
/// static bool pg_wait_until_termination(int pid, int64 timeout)
/// ```
fn pg_wait_until_termination(pid: i32, timeout: i64) -> PgResult<bool> {
    // Wait in steps of waittime milliseconds until this function exits or
    // timeout.
    let mut waittime: i64 = 100;

    // Initially remaining time is the entire timeout specified by the user.
    let mut remainingtime: i64 = timeout;

    // Check existence of the backend. If the backend still exists, wait for
    // waittime ms, again check existence. Repeat until timeout, an error, or a
    // pending interrupt such as query cancel gets processed.
    //
    // C: do { ... } while (remainingtime > 0);
    loop {
        if remainingtime < waittime {
            waittime = remainingtime;
        }

        // C: if (kill(pid, 0) == -1) { if (errno == ESRCH) return true; else
        //        ereport(ERROR, "could not check the existence of the backend
        //        with PID %d: %m"); }
        if unsafe { libc::kill(pid, 0) } == -1 {
            let errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
            if errno == libc::ESRCH {
                return Ok(true);
            }
            return Err(ereport(ERROR)
                .errcode(types_error::ERRCODE_INTERNAL_ERROR)
                .errmsg(format!(
                    "could not check the existence of the backend with PID {pid}"
                ))
                .into_error());
        }

        // Process interrupts, if any, before waiting.
        backend_tcop_postgres_seams::check_for_interrupts::call()?;

        // (void) WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT |
        //                   WL_EXIT_ON_PM_DEATH, waittime,
        //                   WAIT_EVENT_BACKEND_TERMINATION);
        let _ = backend_storage_ipc_latch_seams::wait_latch_my_latch::call(
            WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
            waittime,
            WAIT_EVENT_BACKEND_TERMINATION,
        )?;

        // ResetLatch(MyLatch);
        backend_storage_ipc_latch_seams::reset_latch_my_latch::call();

        remainingtime -= waittime;

        if remainingtime <= 0 {
            break;
        }
    }

    ereport(WARNING)
        .errmsg_plural(
            format!("backend with PID {pid} did not terminate within {timeout} millisecond"),
            format!("backend with PID {pid} did not terminate within {timeout} milliseconds"),
            timeout as u64,
        )
        .finish(loc(221, "pg_wait_until_termination"))?;

    Ok(false)
}

// ---------------------------------------------------------------------------
// pg_terminate_backend
// ---------------------------------------------------------------------------

/// Send a signal to terminate a backend process (deliver `SIGTERM`). Allowed if
/// you are a member of the role whose process is being terminated. If `timeout`
/// is `0`, this just signals the backend and returns `true`. If `timeout` is
/// nonzero, it waits until no process has the given PID; `true` if it ends
/// within the timeout, else a warning is emitted and `false` is returned.
///
/// ```c
/// Datum pg_terminate_backend(PG_FUNCTION_ARGS)
/// ```
pub fn pg_terminate_backend(pid: i32, timeout: i64) -> PgResult<bool> {
    if timeout < 0 {
        return Err(PgError::new(ERROR, "\"timeout\" must not be negative")
            .with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE));
    }

    let r = pg_signal_backend(pid, libc::SIGTERM)?;

    if r == SIGNAL_BACKEND_NOSUPERUSER {
        return Err(privilege_error(
            "permission denied to terminate process",
            "Only roles with the SUPERUSER attribute may terminate processes of roles with the SUPERUSER attribute.",
        ));
    }

    if r == SIGNAL_BACKEND_NOAUTOVAC {
        return Err(privilege_error(
            "permission denied to terminate process",
            "Only roles with privileges of the \"pg_signal_autovacuum_worker\" role may terminate autovacuum workers.",
        ));
    }

    if r == SIGNAL_BACKEND_NOPERMISSION {
        return Err(privilege_error(
            "permission denied to terminate process",
            "Only roles with privileges of the role whose process is being terminated or with privileges of the \"pg_signal_backend\" role may terminate this process.",
        ));
    }

    // Wait only on success and if actually requested.
    if r == SIGNAL_BACKEND_SUCCESS && timeout > 0 {
        pg_wait_until_termination(pid, timeout)
    } else {
        Ok(r == SIGNAL_BACKEND_SUCCESS)
    }
}

// ---------------------------------------------------------------------------
// pg_reload_conf
// ---------------------------------------------------------------------------

/// Signal to reload the database configuration (`SIGHUP` the postmaster).
/// Permission checking is managed through the normal GRANT system.
///
/// ```c
/// Datum pg_reload_conf(PG_FUNCTION_ARGS)
/// ```
pub fn pg_reload_conf() -> PgResult<bool> {
    let postmaster_pid = backend_utils_init_small_seams::postmaster_pid::call();
    // C: if (kill(PostmasterPid, SIGHUP)) { ereport(WARNING, ...); RETURN false; }
    if unsafe { libc::kill(postmaster_pid, libc::SIGHUP) } != 0 {
        ereport(WARNING)
            .errmsg("failed to send signal to postmaster")
            .finish(loc(293, "pg_reload_conf"))?;
        return Ok(false);
    }

    Ok(true)
}

// ---------------------------------------------------------------------------
// pg_rotate_logfile
// ---------------------------------------------------------------------------

/// Rotate log file (ask the syslogger to start a new log file). Permission
/// checking is managed through the normal GRANT system.
///
/// ```c
/// Datum pg_rotate_logfile(PG_FUNCTION_ARGS)
/// ```
pub fn pg_rotate_logfile() -> PgResult<bool> {
    // C: if (!Logging_collector) { ereport(WARNING, ...); RETURN false; }
    if !backend_postmaster_syslogger_seams::logging_collector::call() {
        ereport(WARNING)
            .errmsg("rotation not possible because log collection not active")
            .finish(loc(313, "pg_rotate_logfile"))?;
        return Ok(false);
    }

    // SendPostmasterSignal(PMSIGNAL_ROTATE_LOGFILE);
    SendPostmasterSignal(PMSignalReason::PMSIGNAL_ROTATE_LOGFILE);
    Ok(true)
}

// ---------------------------------------------------------------------------
// Helpers.
// ---------------------------------------------------------------------------

/// Build the shared `ereport(ERROR, errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
/// errmsg(...), errdetail(...))` of the cancel/terminate permission paths.
fn privilege_error(message: &'static str, detail: &'static str) -> PgError {
    PgError::new(ERROR, message)
        .with_sqlstate(ERRCODE_INSUFFICIENT_PRIVILEGE)
        .with_detail(detail)
}
