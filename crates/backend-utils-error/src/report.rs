//! Report formatting and output routing: log_line_prefix, the server-log and
//! frontend writers, timestamps, and the pre-ereport stderr helpers.

#![allow(non_snake_case)]

use std::cell::RefCell;
use std::io::Write;
use std::time::{SystemTime, UNIX_EPOCH};

use types_dest::CommandDest;
use types_error::{
    unpack_sqlstate, ErrorLevel, PGErrorVerbosity, PgError, PgResult, SqlState, DEBUG1, DEBUG2,
    DEBUG3, DEBUG4, DEBUG5, ERROR, FATAL, INFO, LOG, LOG_DESTINATION_CSVLOG,
    LOG_DESTINATION_JSONLOG, LOG_DESTINATION_STDERR, LOG_DESTINATION_SYSLOG, LOG_SERVER_ONLY,
    NOTICE, PANIC, WARNING, WARNING_CLIENT_ONLY,
};

use crate::{config, errno, policy, sink, stack, syslog};

const FORMATTED_TS_LEN: usize = 128;
const _: () = assert!(FORMATTED_TS_LEN > 0); // keep the C constant on record

// Syslog priority levels and `PIPE_BUF`. On wasm `libc` exports neither; pin
// the standard Linux numeric values (sys/syslog.h: LOG_CRIT=2 .. LOG_DEBUG=7;
// PIPE_BUF=4096) so the elevel->priority mapping and the pipe-chunk size are
// identical to the native build. Single-user wasm sends syslog to stderr and
// never uses the syslogger pipe, so these are effectively inert there.
#[cfg(not(target_family = "wasm"))]
mod sysconst {
    pub const LOG_CRIT: i32 = libc::LOG_CRIT;
    pub const LOG_ERR: i32 = libc::LOG_ERR;
    pub const LOG_WARNING: i32 = libc::LOG_WARNING;
    pub const LOG_NOTICE: i32 = libc::LOG_NOTICE;
    pub const LOG_INFO: i32 = libc::LOG_INFO;
    pub const LOG_DEBUG: i32 = libc::LOG_DEBUG;
    pub const PIPE_BUF: usize = libc::PIPE_BUF;
}
#[cfg(target_family = "wasm")]
mod sysconst {
    pub const LOG_CRIT: i32 = 2;
    pub const LOG_ERR: i32 = 3;
    pub const LOG_WARNING: i32 = 4;
    pub const LOG_NOTICE: i32 = 5;
    pub const LOG_INFO: i32 = 6;
    pub const LOG_DEBUG: i32 = 7;
    pub const PIPE_BUF: usize = 4096;
}

/// elog.c's per-backend file statics (`saved_timeval`, `formatted_log_time`,
/// `formatted_start_time`, `log_line_number`, `save_format_errnumber`,
/// `save_format_domain`). Per AGENTS.md "Backend-global state" these are
/// `thread_local!`. C's `log_my_pid` pid-change reset existed only because
/// the statics were fork-inherited from the postmaster; a thread_local starts
/// fresh per backend thread, so it has no counterpart.
#[derive(Default)]
struct LogState {
    /// `saved_timeval` + `saved_timeval_set`: (seconds, microseconds).
    saved_timeval: Option<(i64, u32)>,
    /// `formatted_log_time` ([0] == '\0' <=> None).
    formatted_log_time: Option<String>,
    /// `formatted_start_time`.
    formatted_start_time: Option<String>,
    /// `log_line_number` (static in log_status_format).
    log_line_number: i64,
    /// `save_format_errnumber` / `save_format_domain` (format_elog_string).
    save_format_errnumber: i32,
    save_format_domain: Option<String>,
}

thread_local! {
    static LOG_STATE: RefCell<LogState> = RefCell::new(LogState::default());
}

fn with_log_state<R>(f: impl FnOnce(&mut LogState) -> R) -> R {
    LOG_STATE.with(|state| f(&mut state.borrow_mut()))
}

/// EmitErrorReport's reset of the formatted timestamp fields
/// (`saved_timeval_set = false; formatted_log_time[0] = '\0'`).
pub(crate) fn reset_formatted_log_time() {
    with_log_state(|state| {
        state.saved_timeval = None;
        state.formatted_log_time = None;
    });
}

// ---------------------------------------------------------------------------
// Timestamps
// ---------------------------------------------------------------------------

fn now_timeval() -> (i64, u32) {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => (d.as_secs() as i64, d.subsec_micros()),
        Err(_) => (0, 0),
    }
}

fn civil_from_days(days_since_epoch: i64) -> (i64, u32, u32) {
    let z = days_since_epoch + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    let year = y + if month <= 2 { 1 } else { 0 };
    (year, month as u32, day as u32)
}

/// `pg_strftime(.., "%Y-%m-%d %H:%M:%S %Z", pg_localtime(secs, log_timezone))`
/// with the boot-default `log_timezone` of GMT (the timezone GUC machinery is
/// a separate unit; guc.c guarantees log_timezone is at least GMT before any
/// of these formats can be requested).
fn format_timestamp_seconds(secs: i64) -> String {
    let days = secs.div_euclid(86_400);
    let sod = secs.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02} GMT",
        year,
        month,
        day,
        sod / 3_600,
        (sod % 3_600) / 60,
        sod % 60
    )
}

fn format_timestamp_millis(secs: i64, micros: u32) -> String {
    let days = secs.div_euclid(86_400);
    let sod = secs.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:03} GMT",
        year,
        month,
        day,
        sod / 3_600,
        (sod % 3_600) / 60,
        sod % 60,
        micros / 1_000
    )
}

/// `get_formatted_log_time` — compute (once per report) and return the log
/// timestamp, millisecond precision, consistent across all destinations.
pub fn get_formatted_log_time() -> String {
    with_log_state(|state| {
        if let Some(formatted) = &state.formatted_log_time {
            return formatted.clone();
        }
        let (secs, micros) = match state.saved_timeval {
            Some(tv) => tv,
            None => {
                let tv = now_timeval();
                state.saved_timeval = Some(tv);
                tv
            }
        };
        let formatted = format_timestamp_millis(secs, micros);
        state.formatted_log_time = Some(formatted.clone());
        formatted
    })
}

/// `reset_formatted_start_time`.
pub fn reset_formatted_start_time() {
    with_log_state(|state| state.formatted_start_time = None);
}

/// `get_formatted_start_time` — `MyStartTime` formatted, cached.
pub fn get_formatted_start_time() -> String {
    let start = sink::backend_log_context().map_or(0, |c| c.session_start_time());
    with_log_state(|state| {
        if let Some(formatted) = &state.formatted_start_time {
            return formatted.clone();
        }
        let formatted = format_timestamp_seconds(start);
        state.formatted_start_time = Some(formatted.clone());
        formatted
    })
}

// ---------------------------------------------------------------------------
// Misc helpers
// ---------------------------------------------------------------------------

/// `check_log_of_query` — should this report's STATEMENT be logged?
pub fn check_log_of_query(edata: &PgError) -> bool {
    // log required?
    if !policy::is_log_level_output(edata.level, config::log_min_error_statement()) {
        return false;
    }
    // query log wanted?
    if edata.hide_statement {
        return false;
    }
    // query string available?
    current_query_string().is_some()
}

/// `debug_query_string`, honoring the recursion-trouble suppression.
fn current_query_string() -> Option<String> {
    if stack::statement_suppressed() {
        return None;
    }
    sink::backend_log_context().and_then(|c| c.query_string().map(str::to_owned))
}

/// `get_backend_type_for_log` — backend type for log entries.
/// `GetBackendTypeDesc(B_INVALID)` is "not initialized", the state before
/// miscinit installs a context provider.
pub fn get_backend_type_for_log() -> String {
    sink::backend_log_context()
        .and_then(|c| c.backend_type())
        .unwrap_or("not initialized")
        .to_owned()
}

/// `unpack_sql_state` — render a MAKE_SQLSTATE code as its five-char text.
pub fn unpack_sql_state(sql_state: SqlState) -> String {
    String::from_utf8_lossy(&unpack_sqlstate(sql_state)).into_owned()
}

/// `error_severity` — string representing elevel.
pub fn error_severity(elevel: ErrorLevel) -> &'static str {
    match elevel {
        DEBUG1 | DEBUG2 | DEBUG3 | DEBUG4 | DEBUG5 => "DEBUG",
        LOG | LOG_SERVER_ONLY => "LOG",
        INFO => "INFO",
        NOTICE => "NOTICE",
        WARNING | WARNING_CLIENT_ONLY => "WARNING",
        ERROR => "ERROR",
        FATAL => "FATAL",
        PANIC => "PANIC",
        _ => "???",
    }
}

/// `append_with_tabs` — append, inserting a tab after any newline.
pub fn append_with_tabs(buf: &mut String, str_: &str) {
    for ch in str_.chars() {
        buf.push(ch);
        if ch == '\n' {
            buf.push('\t');
        }
    }
}

/// `set_backtrace` — compute backtrace data into the ErrorData. The C version
/// uses backtrace_symbols with `num_skip` inner frames dropped; the Rust
/// standard backtrace renders its own frame list, so `num_skip` is advisory.
pub fn set_backtrace(edata: &mut PgError, _num_skip: i32) {
    let bt = std::backtrace::Backtrace::force_capture();
    let text = match bt.status() {
        std::backtrace::BacktraceStatus::Captured => format!("\n{}", bt),
        _ => "backtrace generation is not supported by this installation".to_owned(),
    };
    edata.backtrace = Some(text);
}

// ---------------------------------------------------------------------------
// format_elog_string (GUC check-hook message construction)
// ---------------------------------------------------------------------------

/// `pre_format_elog_string` — save errno and text domain before the argument
/// expressions of `format_elog_string` can change them.
pub fn pre_format_elog_string(errnumber: i32, domain: Option<&str>) {
    with_log_state(|state| {
        state.save_format_errnumber = errnumber;
        state.save_format_domain = domain.map(str::to_owned);
    });
}

/// `format_elog_string` — format a message (caller pre-formats everything but
/// `%m`) against the saved errno/domain.
pub fn format_elog_string(fmt: &str) -> String {
    let errnumber = with_log_state(|state| state.save_format_errnumber);
    errno::replace_percent_m(fmt, errnumber)
}

// ---------------------------------------------------------------------------
// log_line_prefix
// ---------------------------------------------------------------------------

/// `process_log_prefix_padding` — parse the optional `-`/digits padding after
/// a `%`. Returns the index of the option character, or `None` if the format
/// is invalid (ends in the padding).
fn process_log_prefix_padding(chars: &[char], mut p: usize, padding: &mut i32) -> Option<usize> {
    let mut paddingsign = 1;
    let mut pad = 0i64;

    if chars.get(p) == Some(&'-') {
        p += 1;
        if p >= chars.len() {
            return None; // buf ended in %-
        }
        paddingsign = -1;
    }

    while let Some(&c) = chars.get(p) {
        if !c.is_ascii_digit() {
            break;
        }
        pad = (pad * 10 + (c as i64 - '0' as i64)).min(i32::MAX as i64);
        p += 1;
    }

    // format is invalid if it ends with the padding number
    if p >= chars.len() {
        return None;
    }

    *padding = pad as i32 * paddingsign;
    Some(p)
}

/// printf `%*s` semantics: right-justify to `padding` width (left-justify when
/// negative); never truncate. Width is in bytes, as in C.
fn append_padded(buf: &mut String, value: &str, padding: i32) {
    if padding == 0 {
        buf.push_str(value);
        return;
    }
    let width = padding.unsigned_abs() as usize;
    let len = value.len();
    if len >= width {
        buf.push_str(value);
        return;
    }
    let spaces = width - len;
    if padding > 0 {
        buf.extend(std::iter::repeat_n(' ', spaces));
        buf.push_str(value);
    } else {
        buf.push_str(value);
        buf.extend(std::iter::repeat_n(' ', spaces));
    }
}

/// `appendStringInfoSpaces(buf, padding > 0 ? padding : -padding)`.
fn append_spaces(buf: &mut String, padding: i32) {
    buf.extend(std::iter::repeat_n(' ', padding.unsigned_abs() as usize));
}

/// `log_line_prefix` — format log status information using `Log_line_prefix`.
pub fn log_line_prefix(buf: &mut String, edata: &PgError) {
    log_status_format(buf, config::log_line_prefix_format().as_deref(), edata);
}

/// `log_status_format` — format log status info, appending to `buf`.
/// `format == None` mirrors the C NULL check (guc hasn't run yet).
pub fn log_status_format(buf: &mut String, format: Option<&str>, edata: &PgError) {
    let context = sink::backend_log_context();
    let my_pid = context.map_or_else(sink::current_pid, |c| c.process_id());
    let has_port = context.is_some_and(|c| c.has_client_port());

    // (C resets log_line_number/formatted_start_time when log_my_pid changes,
    // because its statics are fork-inherited from the postmaster; the
    // thread_local state starts fresh per backend thread.)
    with_log_state(|state| state.log_line_number += 1);

    let Some(format) = format else {
        return; // in case guc hasn't run yet
    };

    let chars: Vec<char> = format.chars().collect();
    let mut p = 0;
    while p < chars.len() {
        if chars[p] != '%' {
            // literal char, just copy
            buf.push(chars[p]);
            p += 1;
            continue;
        }

        // skip the '%'
        p += 1;
        if p >= chars.len() {
            break; // format error - ignore it
        }
        if chars[p] == '%' {
            buf.push('%');
            p += 1;
            continue;
        }

        // Process any padding before the option character.
        let mut padding = 0;
        if chars[p] <= '9' {
            match process_log_prefix_padding(&chars, p, &mut padding) {
                Some(next) => p = next,
                None => break,
            }
        }

        match chars[p] {
            'a' => {
                if has_port {
                    let appname = context
                        .and_then(|c| c.application_name())
                        .filter(|s| !s.is_empty())
                        .unwrap_or("[unknown]");
                    append_padded(buf, appname, padding);
                } else if padding != 0 {
                    append_spaces(buf, padding);
                }
            }
            'b' => {
                let backend_type = get_backend_type_for_log();
                append_padded(buf, &backend_type, padding);
            }
            'u' => {
                if has_port {
                    let username = context
                        .and_then(|c| c.user_name())
                        .filter(|s| !s.is_empty())
                        .unwrap_or("[unknown]");
                    append_padded(buf, username, padding);
                } else if padding != 0 {
                    append_spaces(buf, padding);
                }
            }
            'd' => {
                if has_port {
                    let dbname = context
                        .and_then(|c| c.database_name())
                        .filter(|s| !s.is_empty())
                        .unwrap_or("[unknown]");
                    append_padded(buf, dbname, padding);
                } else if padding != 0 {
                    append_spaces(buf, padding);
                }
            }
            'c' => {
                // session id: MyStartTime.MyProcPid in hex
                let start = context.map_or(0, |c| c.session_start_time());
                let value = format!("{:x}.{:x}", start, my_pid);
                append_padded(buf, &value, padding);
            }
            'p' => {
                append_padded(buf, &my_pid.to_string(), padding);
            }
            'P' => {
                // Show the leader only for active parallel workers.
                match context.and_then(|c| c.lock_group_leader_pid()) {
                    Some(leader_pid) if leader_pid != my_pid => {
                        append_padded(buf, &leader_pid.to_string(), padding);
                    }
                    _ => append_spaces(buf, padding),
                }
            }
            'l' => {
                let n = with_log_state(|state| state.log_line_number);
                append_padded(buf, &n.to_string(), padding);
            }
            'm' => {
                // force a log timestamp reset
                with_log_state(|state| state.formatted_log_time = None);
                let ts = get_formatted_log_time();
                append_padded(buf, &ts, padding);
            }
            't' => {
                let (secs, _) = now_timeval();
                let ts = format_timestamp_seconds(secs);
                append_padded(buf, &ts, padding);
            }
            'n' => {
                let (secs, micros) = with_log_state(|state| match state.saved_timeval {
                    Some(tv) => tv,
                    None => {
                        let tv = now_timeval();
                        state.saved_timeval = Some(tv);
                        tv
                    }
                });
                let value = format!("{}.{:03}", secs, micros / 1_000);
                append_padded(buf, &value, padding);
            }
            's' => {
                let start_time = get_formatted_start_time();
                append_padded(buf, &start_time, padding);
            }
            'i' => {
                if has_port {
                    let psdisp = context.and_then(|c| c.ps_display()).unwrap_or("");
                    append_padded(buf, psdisp, padding);
                } else if padding != 0 {
                    append_spaces(buf, padding);
                }
            }
            'L' => {
                let local_host = context
                    .filter(|c| c.has_client_port())
                    .and_then(|c| c.local_host())
                    // Background process, or connection not yet made
                    .unwrap_or("[none]");
                append_padded(buf, local_host, padding);
            }
            'r' => {
                match context.filter(|c| c.has_client_port()).and_then(|c| c.remote_host()) {
                    Some(remote_host) => {
                        let remote_port = context.and_then(|c| c.remote_port()).unwrap_or("");
                        if !remote_port.is_empty() {
                            let hostport = format!("{}({})", remote_host, remote_port);
                            append_padded(buf, &hostport, padding);
                        } else {
                            append_padded(buf, remote_host, padding);
                        }
                    }
                    None => {
                        if padding != 0 {
                            append_spaces(buf, padding);
                        }
                    }
                }
            }
            'h' => {
                match context.filter(|c| c.has_client_port()).and_then(|c| c.remote_host()) {
                    Some(remote_host) => append_padded(buf, remote_host, padding),
                    None => {
                        if padding != 0 {
                            append_spaces(buf, padding);
                        }
                    }
                }
            }
            'q' => {
                // in postmaster and friends, stop if %q is seen; in a backend, ignore
                if !has_port {
                    return;
                }
            }
            'v' => {
                // keep VXID format in sync with lockfuncs.c
                match context.and_then(|c| c.virtual_transaction_id()) {
                    Some((proc_number, lxid)) => {
                        let value = format!("{}/{}", proc_number, lxid);
                        append_padded(buf, &value, padding);
                    }
                    None => {
                        if padding != 0 {
                            append_spaces(buf, padding);
                        }
                    }
                }
            }
            'x' => {
                let xid = context.map_or(0, |c| c.top_transaction_id());
                append_padded(buf, &xid.to_string(), padding);
            }
            'e' => {
                let state = unpack_sql_state(edata.sqlstate);
                append_padded(buf, &state, padding);
            }
            'Q' => {
                let query_id = context.map_or(0, |c| c.query_id());
                append_padded(buf, &query_id.to_string(), padding);
            }
            _ => {
                // format error - ignore it
            }
        }
        p += 1;
    }
}

// ---------------------------------------------------------------------------
// Server log writer
// ---------------------------------------------------------------------------

/// `send_message_to_server_log` — write the error report to the server's log
/// (stderr / syslog / csvlog / jsonlog / syslogger pipe, per Log_destination).
pub fn send_message_to_server_log(edata: &PgError) {
    let mut buf = String::new();
    let mut fallback_to_stderr = false;

    log_line_prefix(&mut buf, edata);
    buf.push_str(error_severity(edata.level));
    buf.push_str(":  ");

    if config::log_error_verbosity() >= PGErrorVerbosity::Verbose {
        buf.push_str(&unpack_sql_state(edata.sqlstate));
        buf.push_str(": ");
    }

    if !edata.message.is_empty() {
        append_with_tabs(&mut buf, &edata.message);
    } else {
        append_with_tabs(&mut buf, "missing error text");
    }

    if edata.cursor_position.unwrap_or(0) > 0 {
        buf.push_str(&format!(" at character {}", edata.cursor_position.unwrap()));
    } else if edata.internal_position.unwrap_or(0) > 0 {
        buf.push_str(&format!(" at character {}", edata.internal_position.unwrap()));
    }

    buf.push('\n');

    if config::log_error_verbosity() >= PGErrorVerbosity::Default {
        if let Some(detail_log) = &edata.detail_log {
            log_line_prefix(&mut buf, edata);
            buf.push_str("DETAIL:  ");
            append_with_tabs(&mut buf, detail_log);
            buf.push('\n');
        } else if let Some(detail) = &edata.detail {
            log_line_prefix(&mut buf, edata);
            buf.push_str("DETAIL:  ");
            append_with_tabs(&mut buf, detail);
            buf.push('\n');
        }
        if let Some(hint) = &edata.hint {
            log_line_prefix(&mut buf, edata);
            buf.push_str("HINT:  ");
            append_with_tabs(&mut buf, hint);
            buf.push('\n');
        }
        if let Some(internalquery) = &edata.internal_query {
            log_line_prefix(&mut buf, edata);
            buf.push_str("QUERY:  ");
            append_with_tabs(&mut buf, internalquery);
            buf.push('\n');
        }
        if let Some(context) = &edata.context {
            if !edata.hide_context {
                log_line_prefix(&mut buf, edata);
                buf.push_str("CONTEXT:  ");
                append_with_tabs(&mut buf, context);
                buf.push('\n');
            }
        }
        if config::log_error_verbosity() >= PGErrorVerbosity::Verbose {
            // assume no newlines in funcname or filename...
            let location = edata.location.as_ref();
            let funcname = location.and_then(|l| l.funcname.as_deref());
            let filename = location.and_then(|l| l.filename.as_deref());
            let lineno = location.map_or(0, |l| l.lineno);
            if let (Some(funcname), Some(filename)) = (funcname, filename) {
                log_line_prefix(&mut buf, edata);
                buf.push_str(&format!("LOCATION:  {}, {}:{}\n", funcname, filename, lineno));
            } else if let Some(filename) = filename {
                log_line_prefix(&mut buf, edata);
                buf.push_str(&format!("LOCATION:  {}:{}\n", filename, lineno));
            }
        }
        if let Some(backtrace) = &edata.backtrace {
            log_line_prefix(&mut buf, edata);
            buf.push_str("BACKTRACE:  ");
            append_with_tabs(&mut buf, backtrace);
            buf.push('\n');
        }
    }

    // If the user wants the query that generated this error logged, do it.
    if check_log_of_query(edata) {
        if let Some(query) = current_query_string() {
            log_line_prefix(&mut buf, edata);
            buf.push_str("STATEMENT:  ");
            append_with_tabs(&mut buf, &query);
            buf.push('\n');
        }
    }

    // Write to syslog, if enabled
    if config::log_destination() & LOG_DESTINATION_SYSLOG != 0 {
        let syslog_level = match edata.level {
            DEBUG5 | DEBUG4 | DEBUG3 | DEBUG2 | DEBUG1 => sysconst::LOG_DEBUG,
            LOG | LOG_SERVER_ONLY | INFO => sysconst::LOG_INFO,
            NOTICE | WARNING | WARNING_CLIENT_ONLY => sysconst::LOG_NOTICE,
            ERROR => sysconst::LOG_WARNING,
            FATAL => sysconst::LOG_ERR,
            _ => sysconst::LOG_CRIT, // PANIC and default
        };
        syslog::write_syslog(syslog_level, &buf);
    }

    // (win32 eventlog destination intentionally not ported.)

    // Write to csvlog, if enabled; only safe if the syslogger doesn't need
    // the pipe, else fall back to an entry written to stderr.
    if config::log_destination() & LOG_DESTINATION_CSVLOG != 0 {
        if config::redirection_done() || config::am_syslogger() {
            backend_utils_error_small_seams::write_csvlog::call(edata);
        } else {
            fallback_to_stderr = true;
        }
    }

    // Write to JSON log, if enabled — same safety rule.
    if config::log_destination() & LOG_DESTINATION_JSONLOG != 0 {
        if config::redirection_done() || config::am_syslogger() {
            backend_utils_error_small_seams::write_jsonlog::call(edata);
        } else {
            fallback_to_stderr = true;
        }
    }

    // Write to stderr, if enabled or required by a previous limitation.
    if config::log_destination() & LOG_DESTINATION_STDERR != 0
        || config::where_to_send_output() == CommandDest::Debug
        || fallback_to_stderr
    {
        // Use the chunking protocol if the syslogger should be catching
        // stderr output and we are not ourselves the syslogger.
        if config::redirection_done() && !config::am_syslogger() {
            write_pipe_chunks(buf.as_bytes(), LOG_DESTINATION_STDERR);
        } else {
            write_console(buf.as_bytes());
        }
    }

    // If in the syslogger process, try to write messages direct to file
    if config::am_syslogger() {
        backend_postmaster_syslogger_seams::write_syslogger_file::call(
            buf.as_bytes(),
            LOG_DESTINATION_STDERR,
        );
    }
}

/// `write_console` — vanilla write to stderr, errors deliberately ignored
/// (no useful way to report them). The win32 UTF-16 console path is not
/// ported.
pub fn write_console(line: &[u8]) {
    #[cfg(not(target_family = "wasm"))]
    let _ = std::io::stderr().write_all(line);
    // std stderr is a no-op on wasm64-unknown-unknown; route to the host.
    #[cfg(target_family = "wasm")]
    wasm_libc_shim::stderr_write(line);
}

// ---------------------------------------------------------------------------
// Syslogger pipe chunk protocol (write_pipe_chunks)
// ---------------------------------------------------------------------------

/// `PIPE_CHUNK_SIZE` (`postmaster/syslogger.h`): the OS `PIPE_BUF` clamped
/// to 64K, so both sides of the pipe protocol share the OS constant (the
/// syslogger crate derives its copy the same way).
const PIPE_CHUNK_SIZE: usize = if sysconst::PIPE_BUF > 65536 {
    65536
} else {
    sysconst::PIPE_BUF
};

/// `PIPE_HEADER_SIZE = offsetof(PipeProtoHeader, data)`:
/// `nuls[2]` (2) + `uint16 len` (2) + `int32 pid` (4) + `bits8 flags` (1).
const PIPE_HEADER_SIZE: usize = 9;
/// `PIPE_MAX_PAYLOAD`.
const PIPE_MAX_PAYLOAD: usize = PIPE_CHUNK_SIZE - PIPE_HEADER_SIZE;

/// `PIPE_PROTO_IS_LAST` — last chunk of message?
const PIPE_PROTO_IS_LAST: u8 = 0x01;
/// `PIPE_PROTO_DEST_STDERR`.
const PIPE_PROTO_DEST_STDERR: u8 = 0x10;
/// `PIPE_PROTO_DEST_CSVLOG`.
const PIPE_PROTO_DEST_CSVLOG: u8 = 0x20;
/// `PIPE_PROTO_DEST_JSONLOG`.
const PIPE_PROTO_DEST_JSONLOG: u8 = 0x40;

/// One `PipeProtoHeader` rendered to bytes (native-endian `len`/`pid`, as the
/// C struct is read back by the syslogger on the same host).
fn pipe_proto_header(len: u16, pid: i32, flags: u8) -> [u8; PIPE_HEADER_SIZE] {
    let mut header = [0u8; PIPE_HEADER_SIZE];
    // nuls[0] = nuls[1] = '\0' (already zeroed)
    header[2..4].copy_from_slice(&len.to_ne_bytes());
    header[4..8].copy_from_slice(&pid.to_ne_bytes());
    header[8] = flags;
    header
}

/// `write_pipe_chunks` — send data to the syslogger over the stderr pipe using
/// the chunked protocol: each write is one atomic `PipeProtoChunk` no larger
/// than PIPE_BUF, so concurrent writers cannot interleave. Write failures are
/// deliberately ignored (there is nowhere to report them).
pub fn write_pipe_chunks(data: &[u8], dest: i32) {
    // Assert(len > 0)
    debug_assert!(!data.is_empty());

    let pid = sink::backend_log_context().map_or_else(sink::current_pid, |c| c.process_id()) as i32;
    let mut flags: u8 = 0;
    if dest == LOG_DESTINATION_STDERR {
        flags |= PIPE_PROTO_DEST_STDERR;
    } else if dest == LOG_DESTINATION_CSVLOG {
        flags |= PIPE_PROTO_DEST_CSVLOG;
    } else if dest == LOG_DESTINATION_JSONLOG {
        flags |= PIPE_PROTO_DEST_JSONLOG;
    }

    let write_chunk = |payload: &[u8], flags: u8| {
        let mut chunk = Vec::with_capacity(PIPE_HEADER_SIZE + payload.len());
        chunk.extend_from_slice(&pipe_proto_header(payload.len() as u16, pid, flags));
        chunk.extend_from_slice(payload);
        // write(fileno(stderr), &p, ...); (void) rc;  — one write per chunk,
        // result ignored. Must be a single write for pipe atomicity, so use
        // the raw fd rather than the buffering Stdio handle.
        write_fd2(&chunk);
    };

    // write all but the last chunk (no PIPE_PROTO_IS_LAST yet)
    let mut rest = data;
    while rest.len() > PIPE_MAX_PAYLOAD {
        write_chunk(&rest[..PIPE_MAX_PAYLOAD], flags);
        rest = &rest[PIPE_MAX_PAYLOAD..];
    }

    // write the last chunk
    write_chunk(rest, flags | PIPE_PROTO_IS_LAST);
}

/// Atomic single write to fd 2 (stderr) for the pipe-chunk protocol. Native
/// uses the raw `write(2)` syscall (single write = pipe atomicity); wasm has no
/// syslogger pipe, so it writes through std's stderr handle.
#[cfg(not(target_family = "wasm"))]
fn write_fd2(chunk: &[u8]) {
    unsafe {
        let _ = libc::write(2, chunk.as_ptr().cast(), chunk.len());
    }
}
#[cfg(target_family = "wasm")]
fn write_fd2(chunk: &[u8]) {
    // std stderr is a no-op on wasm64-unknown-unknown; route to the host.
    wasm_libc_shim::stderr_write(chunk);
}

// ---------------------------------------------------------------------------
// Frontend writer
// ---------------------------------------------------------------------------

/// `err_sendstring` — append a NUL-terminated string field to the protocol
/// message. (The C version skips encoding conversion when in error-recursion
/// trouble; the owned model sends the UTF-8 bytes either way, so the two
/// paths coincide.)
pub fn err_sendstring(buf: &mut Vec<u8>, s: &str) {
    buf.extend_from_slice(s.as_bytes());
    buf.push(0);
}

fn send_field(buf: &mut Vec<u8>, code: types_error::ErrorField, value: &str) {
    buf.push(code.0 as u8);
    err_sendstring(buf, value);
}

/// `send_message_to_frontend` — write the error report to the client over the
/// FE/BE protocol ('N' for nonfatal conditions, 'E' for errors).
pub fn send_message_to_frontend(edata: &PgError) {
    use types_error::{
        PG_DIAG_COLUMN_NAME, PG_DIAG_CONSTRAINT_NAME, PG_DIAG_CONTEXT, PG_DIAG_DATATYPE_NAME,
        PG_DIAG_INTERNAL_POSITION, PG_DIAG_INTERNAL_QUERY, PG_DIAG_MESSAGE_DETAIL,
        PG_DIAG_MESSAGE_HINT, PG_DIAG_MESSAGE_PRIMARY, PG_DIAG_SCHEMA_NAME, PG_DIAG_SEVERITY,
        PG_DIAG_SEVERITY_NONLOCALIZED, PG_DIAG_SOURCE_FILE, PG_DIAG_SOURCE_FUNCTION,
        PG_DIAG_SOURCE_LINE, PG_DIAG_SQLSTATE, PG_DIAG_STATEMENT_POSITION, PG_DIAG_TABLE_NAME,
    };

    let proto = config::frontend_protocol();
    let msgtype = if edata.level < ERROR { b'N' } else { b'E' };

    // Pre-3.0 protocol is supported only here, so a too-old client still gets
    // a comprehensible "protocol not supported" error. Protocol not yet set
    // (early startup) is assumed modern.
    if (proto >> 16) >= 3 || proto == 0 {
        // New style with separate fields
        let mut body: Vec<u8> = Vec::new();
        let sev = error_severity(edata.level);

        send_field(&mut body, PG_DIAG_SEVERITY, sev);
        send_field(&mut body, PG_DIAG_SEVERITY_NONLOCALIZED, sev);
        send_field(&mut body, PG_DIAG_SQLSTATE, &unpack_sql_state(edata.sqlstate));

        // M field is required per protocol, so always send something
        if !edata.message.is_empty() {
            send_field(&mut body, PG_DIAG_MESSAGE_PRIMARY, &edata.message);
        } else {
            send_field(&mut body, PG_DIAG_MESSAGE_PRIMARY, "missing error text");
        }

        if let Some(detail) = &edata.detail {
            send_field(&mut body, PG_DIAG_MESSAGE_DETAIL, detail);
        }
        // detail_log is intentionally not used here
        if let Some(hint) = &edata.hint {
            send_field(&mut body, PG_DIAG_MESSAGE_HINT, hint);
        }
        if let Some(context) = &edata.context {
            send_field(&mut body, PG_DIAG_CONTEXT, context);
        }
        if let Some(schema_name) = &edata.schema_name {
            send_field(&mut body, PG_DIAG_SCHEMA_NAME, schema_name);
        }
        if let Some(table_name) = &edata.table_name {
            send_field(&mut body, PG_DIAG_TABLE_NAME, table_name);
        }
        if let Some(column_name) = &edata.column_name {
            send_field(&mut body, PG_DIAG_COLUMN_NAME, column_name);
        }
        if let Some(datatype_name) = &edata.datatype_name {
            send_field(&mut body, PG_DIAG_DATATYPE_NAME, datatype_name);
        }
        if let Some(constraint_name) = &edata.constraint_name {
            send_field(&mut body, PG_DIAG_CONSTRAINT_NAME, constraint_name);
        }
        if edata.cursor_position.unwrap_or(0) > 0 {
            send_field(
                &mut body,
                PG_DIAG_STATEMENT_POSITION,
                &edata.cursor_position.unwrap().to_string(),
            );
        }
        if edata.internal_position.unwrap_or(0) > 0 {
            send_field(
                &mut body,
                PG_DIAG_INTERNAL_POSITION,
                &edata.internal_position.unwrap().to_string(),
            );
        }
        if let Some(internal_query) = &edata.internal_query {
            send_field(&mut body, PG_DIAG_INTERNAL_QUERY, internal_query);
        }
        let location = edata.location.as_ref();
        if let Some(filename) = location.and_then(|l| l.filename.as_deref()) {
            send_field(&mut body, PG_DIAG_SOURCE_FILE, filename);
        }
        if location.map_or(0, |l| l.lineno) > 0 {
            send_field(
                &mut body,
                PG_DIAG_SOURCE_LINE,
                &location.unwrap().lineno.to_string(),
            );
        }
        if let Some(funcname) = location.and_then(|l| l.funcname.as_deref()) {
            send_field(&mut body, PG_DIAG_SOURCE_FUNCTION, funcname);
        }

        body.push(0); // terminator

        let _ = backend_libpq_pqcomm_seams::pq_putmessage::call(msgtype, &body);
    } else {
        // Old style --- gin up a backwards-compatible message
        let mut buf = String::new();
        buf.push_str(error_severity(edata.level));
        buf.push_str(":  ");
        if !edata.message.is_empty() {
            buf.push_str(&edata.message);
        } else {
            buf.push_str("missing error text");
        }
        buf.push('\n');

        // pq_putmessage_v2(.., buf.data, buf.len + 1): the body includes the
        // terminating NUL.
        let mut body = buf.into_bytes();
        body.push(0);
        let _ = backend_libpq_pqcomm_seams::pq_putmessage_v2::call(msgtype, &body);
    }

    // Flush so the client has some clue what happened if the backend dies
    // before getting back to the main loop.
    let _ = backend_libpq_pqcomm_seams::pq_flush::call();
}

// ---------------------------------------------------------------------------
// Pre-ereport stderr output
// ---------------------------------------------------------------------------

/// `write_stderr` — write errors to stderr; usable before ereport/elog is
/// safe. The caller pre-formats (the C variadic surface).
pub fn write_stderr(message: &str) {
    vwrite_stderr(message);
}

/// `vwrite_stderr` — va_list flavor; same preformatted-string adaptation.
pub fn vwrite_stderr(message: &str) {
    #[cfg(not(target_family = "wasm"))]
    {
        let mut stderr = std::io::stderr().lock();
        let _ = stderr.write_all(message.as_bytes());
        let _ = stderr.flush();
    }
    // std stderr is a no-op on wasm64-unknown-unknown; route to the host.
    #[cfg(target_family = "wasm")]
    wasm_libc_shim::stderr_write(message.as_bytes());
}

/// `DebugFileOpen` — redirect stderr (and possibly stdout) into the debug
/// output file named by `OutputFileName`. The C freopen calls are realized as
/// open + dup2 onto the standard descriptors.
///
/// wasm (single-process) has no `dup2`/fd-redirection and runs without the
/// `-o OutputFileName` debug-redirect mode, so this is a no-op there.
#[cfg(target_family = "wasm")]
pub fn DebugFileOpen() -> PgResult<()> {
    Ok(())
}

#[cfg(not(target_family = "wasm"))]
pub fn DebugFileOpen() -> PgResult<()> {
    let Some(name) = config::output_file_name().filter(|n| !n.is_empty()) else {
        return Ok(());
    };

    let cpath = std::ffi::CString::new(name.as_str())
        .map_err(|_| PgError::error("invalid OutputFileName"))?;

    // Make sure we can write the file, and find out if it's a tty.
    let (fd, istty) = unsafe {
        let fd = libc::open(
            cpath.as_ptr(),
            libc::O_CREAT | libc::O_APPEND | libc::O_WRONLY,
            0o666,
        );
        if fd < 0 {
            let errnum = errno::current_errno();
            return fatal_file_error(
                &format!("could not open file \"{}\": {}", name, errno::strerror(errnum)),
                errnum,
            );
        }
        let istty = libc::isatty(fd) != 0;
        (fd, istty)
    };

    // Redirect our stderr to the debug output file.
    unsafe {
        if libc::dup2(fd, 2) < 0 {
            let errnum = errno::current_errno();
            libc::close(fd);
            return fatal_file_error(
                &format!(
                    "could not reopen file \"{}\" as stderr: {}",
                    name,
                    errno::strerror(errnum)
                ),
                errnum,
            );
        }
    }

    // If the file is a tty and we're running under the postmaster, send
    // stdout there as well.
    if istty && config::is_under_postmaster() {
        unsafe {
            if libc::dup2(fd, 1) < 0 {
                let errnum = errno::current_errno();
                libc::close(fd);
                return fatal_file_error(
                    &format!(
                        "could not reopen file \"{}\" as stdout: {}",
                        name,
                        errno::strerror(errnum)
                    ),
                    errnum,
                );
            }
        }
    }

    unsafe {
        libc::close(fd);
    }
    Ok(())
}

fn fatal_file_error(message: &str, errnum: i32) -> PgResult<()> {
    let error = PgError::new(FATAL, message)
        .with_saved_errno(errnum)
        .with_sqlstate(errno::sqlstate_for_file_access(errnum));
    stack::ThrowErrorData(error)
}
