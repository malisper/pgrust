//! Family F5 — logging / duration / errdetail helpers and the resource-usage
//! reporting of `tcop/postgres.c`.
//!
//! Reproduced here:
//!   * `check_log_duration`  (postgres.c:2422)
//!   * `errdetail_recovery_conflict` (postgres.c:2552) — in [`crate::interrupt`]
//!   * `errdetail_abort`     (postgres.c:2538)         — in [`crate::interrupt`]
//!   * `ResetUsage`          (postgres.c:5051)
//!   * `ShowUsage`           (postgres.c:5058)
//!   * `log_disconnections`  (postgres.c:5167)
//!   * `log_statement_is_all` — the `log_statement == LOGSTMT_ALL` read used by
//!     fastpath.c (a postgres.c-owned GUC read)
//!   * `log_executor_stats`   — the GUC read used by pquery.c
//!   * `enable_statement_timeout`/`disable_statement_timeout` (postgres.c:5203/5225)
//!
//! `check_log_statement` (postgres.c:2384), `errdetail_execute` (postgres.c:2486)
//! and `errdetail_params` (postgres.c:2519) are coupled to the simple/extended
//! query pipeline (Families F1/F2): they take the parser `List *` of `RawStmt`,
//! the bind `ParamListInfo`, and look up `PreparedStatement`s — none of which
//! exists until the planner-gated query pipeline lands. They are ported
//! alongside their only callers (`exec_simple_query`/`exec_parse_message`) in
//! F1/F2, not here.

#![allow(non_snake_case)]

#[cfg(target_family = "wasm")]
#[allow(unused_imports)]
use wasm_libc_shim as libc;
use ::utils_error::{errdetail_internal, errfinish, errmsg_internal, errstart};
use ::mcx::{Mcx, PgString};
use ::types_error::{PgResult, LOG};
use ::types_timeout::TimeoutId;

use ::init_small::globals as g;

// `__FILE__` / `__LINE__` / `__func__` for `errfinish`.
macro_rules! here {
    ($func:expr) => {
        (Some(file!()), line!() as i32, Some($func))
    };
}

// ---- GUC reads (postgres.c-owned logging GUCs) ----

/// `log_statement == LOGSTMT_ALL` (the `log_statement` GUC). fastpath.c reads
/// this via the `log_statement_is_all` seam.
pub fn log_statement_is_all() -> bool {
    log_statement() == guc_tables::consts::LOGSTMT_ALL
}

fn log_statement() -> i32 {
    guc_tables::vars::log_statement.read()
}

/// `log_executor_stats` (GUC). pquery.c reads this via the `log_executor_stats`
/// seam to gate `ResetUsage`/`ShowUsage`.
pub fn log_executor_stats() -> bool {
    guc_tables::vars::log_executor_stats.read()
}

fn log_duration() -> bool {
    guc_tables::vars::log_duration.read()
}

fn log_min_duration_sample() -> i32 {
    guc_tables::vars::log_min_duration_sample.read()
}

fn log_min_duration_statement() -> i32 {
    guc_tables::vars::log_min_duration_statement.read()
}

fn log_statement_sample_rate() -> f64 {
    guc_tables::vars::log_statement_sample_rate.read()
}

fn statement_timeout() -> i32 {
    guc_tables::vars::StatementTimeout.read()
}

fn transaction_timeout() -> i32 {
    guc_tables::vars::TransactionTimeout.read()
}

/// `check_log_duration(msec_str, was_logged)` (postgres.c:2422) — determine
/// whether the current command's duration should be logged.
///
/// Returns `(code, msec_str)` where `code` is `0` (no logging), `1` (log the
/// duration only), or `2` (log the duration and the query). The formatted
/// milliseconds string mirrors the C `msec_str[32]` buffer and is meaningful
/// only for a nonzero `code` (empty otherwise).
pub fn check_log_duration<'mcx>(mcx: Mcx<'mcx>, was_logged: bool) -> PgResult<(i32, PgString<'mcx>)> {
    let xact_is_sampled = transam_xact::xact_is_sampled();

    if log_duration()
        || log_min_duration_sample() >= 0
        || log_min_duration_statement() >= 0
        || xact_is_sampled
    {
        // TimestampDifference(GetCurrentStatementStartTimestamp(),
        //                     GetCurrentTimestamp(), &secs, &usecs);
        let start = transam_xact_seams::get_current_statement_start_timestamp::call();
        let stop = timestamp_seams::get_current_timestamp::call();
        let (secs, usecs) = timestamp_seams::timestamp_difference::call(start, stop);
        let msecs = usecs / 1000;

        // This odd-looking test for log_min_duration_* being exceeded is
        // designed to avoid integer overflow with very long durations.
        let lmds = log_min_duration_statement();
        let exceeded_duration = lmds == 0
            || (lmds > 0
                && (secs > (lmds / 1000) as i64
                    || secs * 1000 + msecs as i64 >= lmds as i64));

        let lmsamp = log_min_duration_sample();
        let exceeded_sample_duration = lmsamp == 0
            || (lmsamp > 0
                && (secs > (lmsamp / 1000) as i64
                    || secs * 1000 + msecs as i64 >= lmsamp as i64));

        // Do not log if log_statement_sample_rate = 0. Log a sample if
        // log_statement_sample_rate <= 1 and avoid unnecessary PRNG call if
        // log_statement_sample_rate = 1.
        let mut in_sample = false;
        if exceeded_sample_duration {
            let rate = log_statement_sample_rate();
            in_sample = rate != 0.0 && (rate == 1.0 || pg_prng_double_global() <= rate);
        }

        if exceeded_duration || in_sample || log_duration() || xact_is_sampled {
            // snprintf(msec_str, 32, "%ld.%03d", secs*1000 + msecs, usecs%1000)
            let msec_str = PgString::from_str_in(
                &alloc::format!("{}.{:03}", secs * 1000 + msecs as i64, usecs % 1000),
                mcx,
            )?;
            if (exceeded_duration || in_sample || xact_is_sampled) && !was_logged {
                return Ok((2, msec_str));
            } else {
                return Ok((1, msec_str));
            }
        }
    }

    Ok((0, PgString::from_str_in("", mcx)?))
}

/// `pg_prng_double(&pg_global_prng_state)` — a uniform double in `[0, 1)` from
/// the backend's global PRNG state.
fn pg_prng_double_global() -> f64 {
    prng::global_prng(|p| p.next_f64())
}

// ---- resource usage (ResetUsage / ShowUsage) ----

#[cfg(unix)]
thread_local! {
    /// `static struct rusage Save_r;` (postgres.c:5048).
    static SAVE_R: std::cell::Cell<libc::rusage> = std::cell::Cell::new(zero_rusage());
    /// `static struct timeval Save_t;` (postgres.c:5049).
    static SAVE_T: std::cell::Cell<libc::timeval> = std::cell::Cell::new(zero_timeval());
}

#[cfg(unix)]
const fn zero_timeval() -> libc::timeval {
    libc::timeval {
        tv_sec: 0,
        tv_usec: 0,
    }
}

#[cfg(unix)]
fn zero_rusage() -> libc::rusage {
    // SAFETY: `rusage` is plain old data; an all-zero value is a valid initial
    // state (it is only read after `getrusage` populates it via `ResetUsage`).
    unsafe { core::mem::zeroed() }
}

#[cfg(unix)]
fn getrusage_self() -> libc::rusage {
    let mut r = zero_rusage();
    // SAFETY: `getrusage` fills a valid `rusage` for RUSAGE_SELF.
    unsafe {
        libc::getrusage(libc::RUSAGE_SELF, &mut r as *mut libc::rusage);
    }
    r
}

#[cfg(unix)]
fn gettimeofday_now() -> libc::timeval {
    use std::time::{SystemTime, UNIX_EPOCH};
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => libc::timeval {
            tv_sec: d.as_secs() as libc::time_t,
            tv_usec: d.subsec_micros() as libc::suseconds_t,
        },
        Err(_) => zero_timeval(),
    }
}

/// `ResetUsage(void)` (postgres.c:5051): snapshot the current resource usage as
/// the baseline for the next `ShowUsage`.
#[cfg(unix)]
pub fn ResetUsage() {
    SAVE_R.set(getrusage_self());
    SAVE_T.set(gettimeofday_now());
}

/// `ShowUsage(title)` (postgres.c:5058): log the resource-usage delta since the
/// last `ResetUsage` under `title`.
#[cfg(unix)]
pub fn ShowUsage(title: &str) -> PgResult<()> {
    let mut r = getrusage_self();
    let mut elapse_t = gettimeofday_now();
    let user = r.ru_utime;
    let sys = r.ru_stime;
    let save_r = SAVE_R.get();
    let save_t = SAVE_T.get();

    if elapse_t.tv_usec < save_t.tv_usec {
        elapse_t.tv_sec -= 1;
        elapse_t.tv_usec += 1_000_000;
    }
    if r.ru_utime.tv_usec < save_r.ru_utime.tv_usec {
        r.ru_utime.tv_sec -= 1;
        r.ru_utime.tv_usec += 1_000_000;
    }
    if r.ru_stime.tv_usec < save_r.ru_stime.tv_usec {
        r.ru_stime.tv_sec -= 1;
        r.ru_stime.tv_usec += 1_000_000;
    }

    // The only stats we don't show here are ixrss, idrss, isrss.
    let mut str = alloc::string::String::new();
    str.push_str("! system usage stats:\n");
    str.push_str(&alloc::format!(
        "!\t{}.{:06} s user, {}.{:06} s system, {}.{:06} s elapsed\n",
        (r.ru_utime.tv_sec - save_r.ru_utime.tv_sec) as i64,
        (r.ru_utime.tv_usec - save_r.ru_utime.tv_usec) as i64,
        (r.ru_stime.tv_sec - save_r.ru_stime.tv_sec) as i64,
        (r.ru_stime.tv_usec - save_r.ru_stime.tv_usec) as i64,
        (elapse_t.tv_sec - save_t.tv_sec) as i64,
        (elapse_t.tv_usec - save_t.tv_usec) as i64,
    ));
    str.push_str(&alloc::format!(
        "!\t[{}.{:06} s user, {}.{:06} s system total]\n",
        user.tv_sec as i64,
        user.tv_usec as i64,
        sys.tv_sec as i64,
        sys.tv_usec as i64,
    ));

    // The following rusage fields are not defined by POSIX but present on all
    // current Unix-like systems (#ifndef WIN32 in C).
    let maxrss = if cfg!(target_os = "macos") {
        // in bytes on macOS
        (r.ru_maxrss / 1024) as i64
    } else {
        // in kilobytes on most other platforms
        r.ru_maxrss as i64
    };
    str.push_str(&alloc::format!("!\t{maxrss} kB max resident size\n"));
    str.push_str(&alloc::format!(
        "!\t{}/{} [{}/{}] filesystem blocks in/out\n",
        (r.ru_inblock - save_r.ru_inblock) as i64,
        (r.ru_oublock - save_r.ru_oublock) as i64,
        r.ru_inblock as i64,
        r.ru_oublock as i64,
    ));
    str.push_str(&alloc::format!(
        "!\t{}/{} [{}/{}] page faults/reclaims, {} [{}] swaps\n",
        (r.ru_majflt - save_r.ru_majflt) as i64,
        (r.ru_minflt - save_r.ru_minflt) as i64,
        r.ru_majflt as i64,
        r.ru_minflt as i64,
        (r.ru_nswap - save_r.ru_nswap) as i64,
        r.ru_nswap as i64,
    ));
    str.push_str(&alloc::format!(
        "!\t{} [{}] signals rcvd, {}/{} [{}/{}] messages rcvd/sent\n",
        (r.ru_nsignals - save_r.ru_nsignals) as i64,
        r.ru_nsignals as i64,
        (r.ru_msgrcv - save_r.ru_msgrcv) as i64,
        (r.ru_msgsnd - save_r.ru_msgsnd) as i64,
        r.ru_msgrcv as i64,
        r.ru_msgsnd as i64,
    ));
    str.push_str(&alloc::format!(
        "!\t{}/{} [{}/{}] voluntary/involuntary context switches\n",
        (r.ru_nvcsw - save_r.ru_nvcsw) as i64,
        (r.ru_nivcsw - save_r.ru_nivcsw) as i64,
        r.ru_nvcsw as i64,
        r.ru_nivcsw as i64,
    ));

    // remove trailing newline
    if str.ends_with('\n') {
        str.pop();
    }

    if errstart(LOG, None) {
        errmsg_internal(title)?;
        errdetail_internal(&str)?;
        let (f, l, fc) = here!("ShowUsage");
        errfinish(f, l, fc)?;
    }
    Ok(())
}

// wasm64 has no `getrusage`/`rusage`; the `log_statement_stats` resource-usage
// report is a diagnostic only, so single-user wasm gets no-op stand-ins (the
// `! system usage stats` lines are simply omitted).
#[cfg(all(not(unix), target_family = "wasm"))]
pub fn ResetUsage() {}
#[cfg(all(not(unix), target_family = "wasm"))]
pub fn ShowUsage(_title: &str) -> PgResult<()> {
    Ok(())
}

/// `log_disconnections(int code, Datum arg)` (postgres.c:5167) — the
/// `on_proc_exit` handler to log end of session.
pub fn log_disconnections() -> PgResult<()> {
    // TimestampDifference(MyStartTimestamp, GetCurrentTimestamp(), &secs, &usecs)
    let start = g::MyStartTimestamp();
    let stop = timestamp_seams::get_current_timestamp::call();
    let (mut secs, usecs) = timestamp_seams::timestamp_difference::call(start, stop);
    let msecs = usecs / 1000;

    const SECS_PER_HOUR: i64 = 3600;
    const SECS_PER_MINUTE: i64 = 60;
    let hours = secs / SECS_PER_HOUR;
    secs %= SECS_PER_HOUR;
    let minutes = secs / SECS_PER_MINUTE;
    let seconds = secs % SECS_PER_MINUTE;

    // Read the per-connection Port fields (MyProcPort->...). A NULL MyProcPort
    // is a programming error here, surfaced via the owner's accessor.
    let (user_name, database_name, remote_host, remote_port) = port_fields();

    if errstart(LOG, None) {
        let port_clause = if remote_port.is_empty() {
            alloc::string::String::new()
        } else {
            alloc::format!(" port={remote_port}")
        };
        ::utils_error::errmsg(&alloc::format!(
            "disconnection: session time: {hours}:{minutes:02}:{seconds:02}.{msecs:03} \
             user={user_name} database={database_name} host={remote_host}{port_clause}"
        ))?;
        let (f, l, fc) = here!("log_disconnections");
        errfinish(f, l, fc)?;
    }
    Ok(())
}

/// Read `(user_name, database_name, remote_host, remote_port)` from
/// `MyProcPort`. C dereferences the `Port *` directly; the owned model reads
/// the live `MyProcPort` value.
fn port_fields() -> (
    alloc::string::String,
    alloc::string::String,
    alloc::string::String,
    alloc::string::String,
) {
    g::WithMyProcPort(|p| {
        (
            p.user_name.clone().unwrap_or_default(),
            p.database_name.clone().unwrap_or_default(),
            p.remote_host.clone().unwrap_or_default(),
            p.remote_port.clone().unwrap_or_default(),
        )
    })
    .unwrap_or_default()
}

// ---- statement-timeout enable/disable (postgres.c:5203/5225) ----

/// `enable_statement_timeout(void)` (postgres.c:5203) — start the statement
/// timeout timer, if enabled. If one is already running, don't restart it.
pub fn enable_statement_timeout() -> PgResult<()> {
    // must be within an xact
    debug_assert!(crate::globals::xact_started());

    let stmt = statement_timeout();
    let xact = transaction_timeout();
    if stmt > 0 && (stmt < xact || xact == 0) {
        if !misc_timeout::get_timeout_active(TimeoutId::STATEMENT_TIMEOUT) {
            misc_timeout::enable_timeout_after(TimeoutId::STATEMENT_TIMEOUT, stmt)?;
        }
    } else if misc_timeout::get_timeout_active(TimeoutId::STATEMENT_TIMEOUT) {
        misc_timeout::disable_timeout(TimeoutId::STATEMENT_TIMEOUT, false);
    }
    Ok(())
}

/// `disable_statement_timeout(void)` (postgres.c:5225) — disable the statement
/// timeout, if active.
pub fn disable_statement_timeout() {
    if misc_timeout::get_timeout_active(TimeoutId::STATEMENT_TIMEOUT) {
        misc_timeout::disable_timeout(TimeoutId::STATEMENT_TIMEOUT, false);
    }
}
