#![allow(non_snake_case)]

use std::io::{self, Write};
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

// wasm: shadow the extern `libc` with the crate-local errno shim (used for the
// `EIO` fallback below).
#[cfg(target_family = "wasm")]
use crate::libc_wasm as libc;

use ::pg_ffi_fgram::{unpack_sqlstate, ErrorLevel, SqlState};

use crate::{
    backend_log_context, emit_error_report, errcode_for_file_access, severity, write_pipe_chunks,
    CopyErrorData, LogDestination, PgError, PgResult,
};

static LOG_FORMAT_STATE: Mutex<LogFormatState> = Mutex::new(LogFormatState {
    formatted_log_time: None,
    formatted_start_time: None,
    saved_log_time: None,
    format_saved_errno: None,
    format_domain: None,
    log_line_number: 0,
    log_process_id: None,
});

#[derive(Clone, Debug)]
struct LogFormatState {
    formatted_log_time: Option<String>,
    formatted_start_time: Option<String>,
    saved_log_time: Option<SystemTime>,
    format_saved_errno: Option<i32>,
    format_domain: Option<String>,
    log_line_number: u64,
    log_process_id: Option<u32>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DebugFileOpenResult {
    pub path: String,
    pub stderr_reopened: bool,
    pub stdout_reopened: bool,
    pub is_tty: bool,
}

pub fn EmitErrorReport() -> PgResult<()> {
    let error = CopyErrorData()?;
    let formatted = format_error_report(error.error());
    emit_error_report(error.error(), &formatted)?;
    Ok(())
}

pub fn GetErrorContextStack() -> PgResult<String> {
    let error = CopyErrorData()?;
    Ok(error.error().context().unwrap_or_default().to_owned())
}

pub fn error_severity(level: ErrorLevel) -> &'static str {
    severity(level)
}

pub fn unpack_sql_state(sqlstate: SqlState) -> String {
    String::from_utf8_lossy(&unpack_sqlstate(sqlstate)).into_owned()
}

pub fn get_formatted_log_time() -> String {
    let mut state = LOG_FORMAT_STATE
        .lock()
        .expect("log format state lock poisoned");
    if let Some(formatted) = &state.formatted_log_time {
        return formatted.clone();
    }

    let time = state.saved_log_time.unwrap_or_else(SystemTime::now);
    state.saved_log_time = Some(time);
    let formatted = format_log_timestamp_millis(time);
    state.formatted_log_time = Some(formatted.clone());
    formatted
}

pub fn reset_formatted_start_time() {
    LOG_FORMAT_STATE
        .lock()
        .expect("log format state lock poisoned")
        .formatted_start_time = None;
}

pub fn get_formatted_start_time() -> String {
    let mut state = LOG_FORMAT_STATE
        .lock()
        .expect("log format state lock poisoned");
    if let Some(formatted) = &state.formatted_start_time {
        return formatted.clone();
    }

    let time = backend_log_context()
        .and_then(|context| context.session_start_time())
        .unwrap_or_else(SystemTime::now);
    let formatted = format_log_timestamp_seconds(time);
    state.formatted_start_time = Some(formatted.clone());
    formatted
}

pub fn get_backend_type_for_log() -> String {
    backend_log_context()
        .and_then(|context| context.backend_type())
        .unwrap_or("[unknown]")
        .to_owned()
}

pub fn check_log_of_query(error: &PgError) -> bool {
    let Some(context) = backend_log_context() else {
        return false;
    };

    if error.level() < context.log_min_error_statement() {
        return false;
    }

    if error.hide_statement() {
        return false;
    }

    context.query_string().is_some()
}

pub fn log_line_prefix(format: &str, error: &PgError) -> String {
    log_status_format(format, error)
}

pub fn log_status_format(format: &str, error: &PgError) -> String {
    update_log_line_state();

    let mut output = String::new();
    let mut chars = format.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch != '%' {
            output.push(ch);
            continue;
        }

        let Some(next) = chars.next() else {
            break;
        };
        if next == '%' {
            output.push('%');
            continue;
        }

        let (padding, code) = parse_padding(next, &mut chars);
        let Some(code) = code else {
            break;
        };
        if code == 'q' && backend_log_context().is_none() {
            return output;
        }

        if let Some(value) = log_status_value(code, error) {
            append_padded(&mut output, &value, padding);
        } else if padding != 0 {
            append_spaces(&mut output, padding.unsigned_abs() as usize);
        }
    }

    output
}

pub fn pre_format_elog_string(errnumber: i32, domain: Option<&str>) {
    let mut state = LOG_FORMAT_STATE
        .lock()
        .expect("log format state lock poisoned");
    state.format_saved_errno = Some(errnumber);
    state.format_domain = domain.map(str::to_owned);
}

pub fn format_elog_string(format: &str) -> String {
    let saved_errno = LOG_FORMAT_STATE
        .lock()
        .expect("log format state lock poisoned")
        .format_saved_errno;

    match saved_errno {
        Some(saved_errno) => replace_percent_m(format, saved_errno),
        None => format.to_owned(),
    }
}

pub fn send_message_to_server_log(error: &PgError, prefix_format: &str) -> PgResult<String> {
    let mut message = log_line_prefix(prefix_format, error);
    message.push_str(&format_error_report(error));
    message.push('\n');
    emit_error_report(error, &message)?;
    Ok(message)
}

pub fn append_with_tabs(text: &str) -> String {
    let mut output = String::with_capacity(text.len());
    for ch in text.chars() {
        output.push(ch);
        if ch == '\n' {
            output.push('\t');
        }
    }
    output
}

pub fn write_console(line: &str) -> io::Result<()> {
    write_stderr(line)
}

pub fn write_syslog(level: ErrorLevel, line: &str) -> PgResult<()> {
    let error = PgError::new(level, line);
    emit_error_report(&error, line)
}

pub fn send_message_to_frontend(error: &PgError) -> PgResult<()> {
    emit_error_report(error, "")
}

pub fn err_sendstring(buffer: &mut String, value: &str) {
    buffer.push_str(value);
}

pub fn vwrite_stderr(message: impl AsRef<str>) -> io::Result<()> {
    write_stderr(message)
}

pub fn DebugFileOpen(
    path: impl AsRef<str>,
    under_postmaster: bool,
) -> PgResult<DebugFileOpenResult> {
    let path = path.as_ref();
    let file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|error| {
            PgError::new(
                crate::FATAL,
                format!("could not open file \"{}\": {}", path, error),
            )
            .with_sqlstate(errcode_for_file_access(
                error.raw_os_error().unwrap_or(libc::EIO),
            ))
        })?;
    let is_tty = file
        .metadata()
        .map(|metadata| metadata.is_dir())
        .unwrap_or(false);

    Ok(DebugFileOpenResult {
        path: path.to_owned(),
        stderr_reopened: true,
        stdout_reopened: under_postmaster && is_tty,
        is_tty,
    })
}

pub fn write_pipe_chunks_for_text(text: &str, destination: LogDestination) -> PgResult<()> {
    write_pipe_chunks(text.as_bytes(), destination)
}

pub fn format_error_report(error: &PgError) -> String {
    let mut report = format!("{}:  {}", error_severity(error.level()), error.message());

    if let Some(detail) = error.detail() {
        report.push('\n');
        report.push_str("DETAIL:  ");
        report.push_str(detail);
    }

    if let Some(hint) = error.hint() {
        report.push('\n');
        report.push_str("HINT:  ");
        report.push_str(hint);
    }

    if !error.hide_context() {
        if let Some(context) = error.context() {
            report.push('\n');
            report.push_str("CONTEXT:  ");
            report.push_str(context);
        }
    }

    report
}

pub fn write_stderr(message: impl AsRef<str>) -> io::Result<()> {
    let mut stderr = io::stderr().lock();
    stderr.write_all(message.as_ref().as_bytes())?;
    stderr.flush()
}

fn update_log_line_state() {
    let process_id = backend_log_context().and_then(|context| context.process_id());
    let mut state = LOG_FORMAT_STATE
        .lock()
        .expect("log format state lock poisoned");

    if state.log_process_id != process_id {
        state.log_line_number = 0;
        state.log_process_id = process_id;
        state.formatted_start_time = None;
    }
    state.log_line_number += 1;
}

fn current_log_line_number() -> u64 {
    LOG_FORMAT_STATE
        .lock()
        .expect("log format state lock poisoned")
        .log_line_number
}

fn reset_formatted_log_time() {
    LOG_FORMAT_STATE
        .lock()
        .expect("log format state lock poisoned")
        .formatted_log_time = None;
}

fn log_status_value(code: char, error: &PgError) -> Option<String> {
    let context = backend_log_context();
    match code {
        'a' => context.map(|context| unknown_if_empty(context.application_name())),
        'b' => Some(get_backend_type_for_log()),
        'u' => context.map(|context| unknown_if_empty(context.user_name())),
        'd' => context.map(|context| unknown_if_empty(context.database_name())),
        'c' => Some(session_id(context)),
        'p' => Some(
            context
                .and_then(|context| context.process_id())
                .unwrap_or(0)
                .to_string(),
        ),
        'P' => context.and_then(|context| {
            context
                .parallel_leader_process_id()
                .map(|pid| pid.to_string())
        }),
        'l' => Some(current_log_line_number().to_string()),
        'm' => {
            reset_formatted_log_time();
            Some(get_formatted_log_time())
        }
        't' => Some(format_log_timestamp_seconds(SystemTime::now())),
        'n' => Some(format_unix_millis(saved_or_current_log_time())),
        's' => Some(get_formatted_start_time()),
        'i' => context.and_then(|context| context.ps_display().map(str::to_owned)),
        'L' => Some(
            context
                .and_then(|context| context.local_host())
                .unwrap_or("[none]")
                .to_owned(),
        ),
        'r' => context.and_then(remote_host_and_port),
        'h' => context.and_then(|context| context.remote_host().map(str::to_owned)),
        'q' => None,
        'v' => context.and_then(|context| {
            context
                .virtual_transaction_id()
                .map(|(proc_number, lxid)| format!("{}/{}", proc_number, lxid))
        }),
        'x' => Some(
            context
                .and_then(|context| context.top_transaction_id())
                .unwrap_or(0)
                .to_string(),
        ),
        'e' => Some(unpack_sql_state(error.sqlstate())),
        'Q' => Some(
            context
                .and_then(|context| context.query_id())
                .unwrap_or(0)
                .to_string(),
        ),
        _ => None,
    }
}

fn parse_padding(
    first: char,
    chars: &mut std::iter::Peekable<std::str::Chars<'_>>,
) -> (isize, Option<char>) {
    let mut sign = 1;
    let mut ch = first;

    if ch == '-' {
        sign = -1;
        let Some(next) = chars.next() else {
            return (0, None);
        };
        ch = next;
    }

    if ch.is_ascii_digit() {
        let mut padding = ch.to_digit(10).expect("checked digit") as isize;
        while let Some(next) = chars.peek().copied() {
            if !next.is_ascii_digit() {
                break;
            }
            chars.next();
            padding = padding * 10 + next.to_digit(10).expect("checked digit") as isize;
        }
        let Some(code) = chars.next() else {
            return (0, None);
        };
        (padding * sign, Some(code))
    } else {
        (0, Some(ch))
    }
}

fn append_padded(output: &mut String, value: &str, padding: isize) {
    if padding == 0 {
        output.push_str(value);
        return;
    }

    let width = padding.unsigned_abs();
    let len = value.chars().count();
    if len >= width {
        output.push_str(value);
        return;
    }

    let spaces = width - len;
    if padding > 0 {
        append_spaces(output, spaces);
        output.push_str(value);
    } else {
        output.push_str(value);
        append_spaces(output, spaces);
    }
}

fn append_spaces(output: &mut String, count: usize) {
    output.extend(std::iter::repeat_n(' ', count));
}

fn unknown_if_empty(value: Option<&str>) -> String {
    match value {
        Some(value) if !value.is_empty() => value.to_owned(),
        _ => "[unknown]".to_owned(),
    }
}

fn remote_host_and_port(context: &dyn crate::BackendLogContext) -> Option<String> {
    let host = context.remote_host()?;
    match context.remote_port() {
        Some(port) if !port.is_empty() => Some(format!("{}({})", host, port)),
        _ => Some(host.to_owned()),
    }
}

fn session_id(context: Option<&dyn crate::BackendLogContext>) -> String {
    let pid = context
        .and_then(|context| context.process_id())
        .unwrap_or(0);
    let start = context
        .and_then(|context| context.session_start_time())
        .unwrap_or(UNIX_EPOCH);
    let secs = start
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs();
    format!("{:x}.{:x}", secs, pid)
}

fn saved_or_current_log_time() -> SystemTime {
    let mut state = LOG_FORMAT_STATE
        .lock()
        .expect("log format state lock poisoned");
    let time = state.saved_log_time.unwrap_or_else(SystemTime::now);
    state.saved_log_time = Some(time);
    time
}

fn format_unix_millis(time: SystemTime) -> String {
    let duration = time.duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO);
    format!("{}.{:03}", duration.as_secs(), duration.subsec_millis())
}

fn replace_percent_m(format: &str, saved_errno: i32) -> String {
    if !format.contains("%m") {
        return format.to_owned();
    }
    format.replace(
        "%m",
        &std::io::Error::from_raw_os_error(saved_errno).to_string(),
    )
}

fn format_log_timestamp_millis(time: SystemTime) -> String {
    let duration = time.duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO);
    let (year, month, day, hour, minute, second) = unix_seconds_to_utc(duration.as_secs());
    format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:03} UTC",
        year,
        month,
        day,
        hour,
        minute,
        second,
        duration.subsec_millis()
    )
}

fn format_log_timestamp_seconds(time: SystemTime) -> String {
    let duration = time.duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO);
    let (year, month, day, hour, minute, second) = unix_seconds_to_utc(duration.as_secs());
    format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02} UTC",
        year, month, day, hour, minute, second
    )
}

fn unix_seconds_to_utc(seconds: u64) -> (i32, u32, u32, u32, u32, u32) {
    let days = (seconds / 86_400) as i64;
    let seconds_of_day = seconds % 86_400;
    let (year, month, day) = civil_from_days(days);
    let hour = (seconds_of_day / 3_600) as u32;
    let minute = ((seconds_of_day % 3_600) / 60) as u32;
    let second = (seconds_of_day % 60) as u32;
    (year, month, day, hour, minute, second)
}

fn civil_from_days(days_since_epoch: i64) -> (i32, u32, u32) {
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
    (year as i32, month as u32, day as u32)
}
