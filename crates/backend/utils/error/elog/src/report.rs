//! Report formatting and output routing: log_line_prefix, the server-log and
//! frontend writers, timestamps, and the pre-ereport stderr helpers.

#![allow(non_snake_case)]

use std::cell::RefCell;
use std::io::Write;
use std::time::{SystemTime, UNIX_EPOCH};

use ::types_dest::CommandDest;
use ::types_error::{
    unpack_sqlstate, ErrorLevel, PGErrorVerbosity, PgError, PgResult, SqlState, DEBUG1, DEBUG2,
    DEBUG3, DEBUG4, DEBUG5, ERROR, FATAL, INFO, LOG, LOG_DESTINATION_CSVLOG,
    LOG_DESTINATION_JSONLOG, LOG_DESTINATION_STDERR, LOG_DESTINATION_SYSLOG, LOG_SERVER_ONLY,
    NOTICE, PANIC, WARNING, WARNING_CLIENT_ONLY,
};

use crate::{config, errno, policy, sink, stack, syslog};

#[derive(Default)]
struct LogState {
    saved_timeval: Option<(i64, u32)>,
    formatted_log_time: Option<String>,
    formatted_start_time: Option<String>,
    log_line_number: i64,
    save_format_errnumber: i32,
    save_format_domain: Option<String>,
}

thread_local! {
    static LOG_STATE: RefCell<LogState> = RefCell::new(LogState::default());
}

fn with_log_state<R>(f: impl FnOnce(&mut LogState) -> R) -> R {
    LOG_STATE.with(|state| f(&mut state.borrow_mut()))
}

pub(crate) fn reset_formatted_log_time() {
    with_log_state(|state| {
        state.saved_timeval = None;
        state.formatted_log_time = None;
    });
}

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

pub fn reset_formatted_start_time() {
    with_log_state(|state| state.formatted_start_time = None);
}

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

pub fn check_log_of_query(edata: &PgError) -> bool {
    if !policy::is_log_level_output(edata.level, config::log_min_error_statement()) {
        return false;
    }
    if edata.hide_statement {
        return false;
    }
    current_query_string().is_some()
}

fn current_query_string() -> Option<String> {
    if stack::statement_suppressed() {
        return None;
    }
    sink::backend_log_context().and_then(|c| c.query_string().map(str::to_owned))
}

pub fn get_backend_type_for_log() -> String {
    sink::backend_log_context()
        .and_then(|c| c.backend_type())
        .unwrap_or("not initialized")
        .to_owned()
}

pub fn unpack_sql_state(sql_state: SqlState) -> String {
    String::from_utf8_lossy(&unpack_sqlstate(sql_state)).into_owned()
}

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

pub fn append_with_tabs(buf: &mut String, str_: &str) {
    for ch in str_.chars() {
        buf.push(ch);
        if ch == '\n' {
            buf.push('\t');
        }
    }
}

#[cold]
#[inline(never)]
pub fn set_backtrace(edata: &mut PgError, _num_skip: i32) {
    let bt = std::backtrace::Backtrace::force_capture();
    let text = match bt.status() {
        std::backtrace::BacktraceStatus::Captured => format!("\n{}", bt),
        _ => "backtrace generation is not supported by this installation".to_owned(),
    };
    edata.backtrace = Some(text);
}

pub fn pre_format_elog_string(errnumber: i32, domain: Option<&str>) {
    with_log_state(|state| {
        state.save_format_errnumber = errnumber;
        state.save_format_domain = domain.map(str::to_owned);
    });
}

#[cold]
#[inline(never)]
pub fn format_elog_string(fmt: &str) -> String {
    let errnumber = with_log_state(|state| state.save_format_errnumber);
    errno::replace_percent_m(fmt, errnumber)
}

fn process_log_prefix_padding(chars: &[char], mut p: usize, padding: &mut i32) -> Option<usize> {
    let mut paddingsign = 1;
    let mut pad = 0i64;

    if chars.get(p) == Some(&'-') {
        p += 1;
        if p >= chars.len() {
            return None;
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

    if p >= chars.len() {
        return None;
    }

    *padding = pad as i32 * paddingsign;
    Some(p)
}

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

fn append_spaces(buf: &mut String, padding: i32) {
    buf.extend(std::iter::repeat_n(' ', padding.unsigned_abs() as usize));
}

pub fn log_line_prefix(buf: &mut String, edata: &PgError) {
    log_status_format(buf, config::log_line_prefix_format().as_deref(), edata);
}

pub fn log_status_format(buf: &mut String, format: Option<&str>, edata: &PgError) {
    let context = sink::backend_log_context();
    let my_pid = context.map_or_else(sink::current_pid, |c| c.process_id());
    let has_port = context.is_some_and(|c| c.has_client_port());

    with_log_state(|state| state.log_line_number += 1);

    let Some(format) = format else {
        return; // in case guc hasn't run yet
    };

    let chars: Vec<char> = format.chars().collect();
    let mut p = 0;
    while p < chars.len() {
        if chars[p] != '%' {
            buf.push(chars[p]);
            p += 1;
            continue;
        }

        p += 1;
        if p >= chars.len() {
            break; // format error - ignore it
        }
        if chars[p] == '%' {
            buf.push('%');
            p += 1;
            continue;
        }

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
                let start = context.map_or(0, |c| c.session_start_time());
                let value = format!("{:x}.{:x}", start, my_pid);
                append_padded(buf, &value, padding);
            }
            'p' => {
                append_padded(buf, &my_pid.to_string(), padding);
            }
            'P' => {
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

#[cold]
#[inline(never)]
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

    if check_log_of_query(edata) {
        if let Some(query) = current_query_string() {
            log_line_prefix(&mut buf, edata);
            buf.push_str("STATEMENT:  ");
            append_with_tabs(&mut buf, &query);
            buf.push('\n');
        }
    }

    if config::log_destination() & LOG_DESTINATION_SYSLOG != 0 {
        let syslog_level = match edata.level {
            DEBUG5 | DEBUG4 | DEBUG3 | DEBUG2 | DEBUG1 => libc::LOG_DEBUG,
            LOG | LOG_SERVER_ONLY | INFO => libc::LOG_INFO,
            NOTICE | WARNING | WARNING_CLIENT_ONLY => libc::LOG_NOTICE,
            ERROR => libc::LOG_WARNING,
            FATAL => libc::LOG_ERR,
            _ => libc::LOG_CRIT, // PANIC and default
        };
        syslog::write_syslog(syslog_level, &buf);
    }

    // (win32 eventlog destination intentionally not ported.)

    if config::log_destination() & LOG_DESTINATION_CSVLOG != 0 {
        if config::redirection_done() || config::am_syslogger() {
            error_small_seams::write_csvlog::call(edata);
        } else {
            fallback_to_stderr = true;
        }
    }

    if config::log_destination() & LOG_DESTINATION_JSONLOG != 0 {
        if config::redirection_done() || config::am_syslogger() {
            error_small_seams::write_jsonlog::call(edata);
        } else {
            fallback_to_stderr = true;
        }
    }

    if config::log_destination() & LOG_DESTINATION_STDERR != 0
        || config::where_to_send_output() == CommandDest::Debug
        || fallback_to_stderr
    {
        if config::redirection_done() && !config::am_syslogger() {
            write_pipe_chunks(buf.as_bytes(), LOG_DESTINATION_STDERR);
        } else {
            write_console(buf.as_bytes());
        }
    }

    if config::am_syslogger() {
        syslogger_seams::write_syslogger_file::call(buf.as_bytes(), LOG_DESTINATION_STDERR);
    }
}

pub fn write_console(line: &[u8]) {
    let _ = std::io::stderr().write_all(line);
}

const PIPE_CHUNK_SIZE: usize = if libc::PIPE_BUF > 65536 {
    65536
} else {
    libc::PIPE_BUF
};

const PIPE_HEADER_SIZE: usize = 9;
const PIPE_MAX_PAYLOAD: usize = PIPE_CHUNK_SIZE - PIPE_HEADER_SIZE;

const PIPE_PROTO_IS_LAST: u8 = 0x01;
const PIPE_PROTO_DEST_STDERR: u8 = 0x10;
const PIPE_PROTO_DEST_CSVLOG: u8 = 0x20;
const PIPE_PROTO_DEST_JSONLOG: u8 = 0x40;

fn pipe_proto_header(len: u16, pid: i32, flags: u8) -> [u8; PIPE_HEADER_SIZE] {
    let mut header = [0u8; PIPE_HEADER_SIZE];
    header[2..4].copy_from_slice(&len.to_ne_bytes());
    header[4..8].copy_from_slice(&pid.to_ne_bytes());
    header[8] = flags;
    header
}

#[cold]
#[inline(never)]
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
        write_fd2(&chunk);
    };

    let mut rest = data;
    while rest.len() > PIPE_MAX_PAYLOAD {
        write_chunk(&rest[..PIPE_MAX_PAYLOAD], flags);
        rest = &rest[PIPE_MAX_PAYLOAD..];
    }

    write_chunk(rest, flags | PIPE_PROTO_IS_LAST);
}

fn write_fd2(chunk: &[u8]) {
    // SAFETY: the pointer/length pair describes a live buffer; write(2) has
    // no other preconditions and the result is deliberately ignored.
    unsafe {
        let _ = libc::write(2, chunk.as_ptr().cast(), chunk.len());
    }
}

std::thread_local! {
    // C ErrorContext: backend-lifetime scratch for converting outgoing error
    // fields to the client encoding; reset after every field.
    static ERR_CONVERT_CX: RefCell<std::mem::ManuallyDrop<::mcx::MemoryContext>> =
        RefCell::new(std::mem::ManuallyDrop::new(::mcx::MemoryContext::new(
            "error conversion",
        )));
}

// C err_sendstring: pq_sendstring (client-encoding conversion) unless already
// in error recursion. Divergences: an uninstalled seam (unit tests, early
// startup) and a conversion failure both fall back to the raw
// server-encoding bytes instead of re-entering ereport.
pub fn err_sendstring(buf: &mut Vec<u8>, s: &str) {
    let converted = !stack::in_error_recursion_trouble()
        && ::mbutils_seams::pg_server_to_client::is_installed()
        && ERR_CONVERT_CX.with(|cell| {
            let Ok(mut cx) = cell.try_borrow_mut() else {
                return false;
            };
            let done = {
                match ::mbutils_seams::pg_server_to_client::call(cx.mcx(), s.as_bytes()) {
                    Ok(Some(conv)) => {
                        buf.extend_from_slice(&conv);
                        true
                    }
                    Ok(None) | Err(_) => false,
                }
            };
            cx.reset();
            done
        });
    if !converted {
        buf.extend_from_slice(s.as_bytes());
    }
    buf.push(0);
}

fn send_field(buf: &mut Vec<u8>, code: ::types_error::ErrorField, value: &str) {
    buf.push(code.0 as u8);
    err_sendstring(buf, value);
}

#[cold]
#[inline(never)]
pub fn send_message_to_frontend(edata: &PgError) {
    use ::types_error::{
        PG_DIAG_COLUMN_NAME, PG_DIAG_CONSTRAINT_NAME, PG_DIAG_CONTEXT, PG_DIAG_DATATYPE_NAME,
        PG_DIAG_INTERNAL_POSITION, PG_DIAG_INTERNAL_QUERY, PG_DIAG_MESSAGE_DETAIL,
        PG_DIAG_MESSAGE_HINT, PG_DIAG_MESSAGE_PRIMARY, PG_DIAG_SCHEMA_NAME, PG_DIAG_SEVERITY,
        PG_DIAG_SEVERITY_NONLOCALIZED, PG_DIAG_SOURCE_FILE, PG_DIAG_SOURCE_FUNCTION,
        PG_DIAG_SOURCE_LINE, PG_DIAG_SQLSTATE, PG_DIAG_STATEMENT_POSITION, PG_DIAG_TABLE_NAME,
    };

    let proto = config::frontend_protocol();
    let msgtype = if edata.level < ERROR { b'N' } else { b'E' };

    // Pre-3.0 protocol is supported only here, so a too-old client still gets
    // a comprehensible error. Protocol 0 (early startup) is assumed modern.
    if (proto >> 16) >= 3 || proto == 0 {
        let mut body: Vec<u8> = Vec::new();
        let sev = error_severity(edata.level);

        send_field(&mut body, PG_DIAG_SEVERITY, sev);
        send_field(&mut body, PG_DIAG_SEVERITY_NONLOCALIZED, sev);
        send_field(&mut body, PG_DIAG_SQLSTATE, &unpack_sql_state(edata.sqlstate));

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

        let _ = pqcomm_seams::pq_putmessage::call(msgtype, &body);
    } else {
        let mut buf = String::new();
        buf.push_str(error_severity(edata.level));
        buf.push_str(":  ");
        if !edata.message.is_empty() {
            buf.push_str(&edata.message);
        } else {
            buf.push_str("missing error text");
        }
        buf.push('\n');

        let mut body = buf.into_bytes();
        body.push(0);
        let _ = pqcomm_seams::pq_putmessage_v2::call(msgtype, &body);
    }

    let _ = pqcomm_seams::pq_flush::call();
}

pub fn write_stderr(message: &str) {
    vwrite_stderr(message);
}

pub fn vwrite_stderr(message: &str) {
    let mut stderr = std::io::stderr().lock();
    let _ = stderr.write_all(message.as_bytes());
    let _ = stderr.flush();
}

pub fn DebugFileOpen() -> PgResult<()> {
    let Some(name) = config::output_file_name().filter(|n| !n.is_empty()) else {
        return Ok(());
    };

    let cpath = std::ffi::CString::new(name.as_str())
        .map_err(|_| PgError::error("invalid OutputFileName"))?;

    // SAFETY: cpath is a live NUL-terminated path; open/isatty have no other
    // preconditions.
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

    // SAFETY: fd is a valid open descriptor; dup2 onto the standard
    // descriptors has no other preconditions.
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

    if istty && config::is_under_postmaster() {
        // SAFETY: as above.
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

    // SAFETY: fd is a valid open descriptor owned by this function.
    unsafe {
        libc::close(fd);
    }
    Ok(())
}

#[cold]
#[inline(never)]
fn fatal_file_error(message: &str, errnum: i32) -> PgResult<()> {
    let error = PgError::new(FATAL, message)
        .with_saved_errno(errnum)
        .with_sqlstate(errno::sqlstate_for_file_access(errnum));
    stack::ThrowErrorData(error)
}
