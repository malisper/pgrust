//! `utils/error/jsonlog.c` — JSON server-log line formatter.

use std::cell::RefCell;

use backend_postmaster_syslogger_seams::write_syslogger_file;
use backend_utils_error::{
    backend_log_context, check_log_of_query, config, error_severity, get_backend_type_for_log,
    get_formatted_log_time, get_formatted_start_time, unpack_sql_state, write_pipe_chunks, PgError,
};
use types_error::LOG_DESTINATION_JSONLOG;

use crate::{my_proc_pid, my_start_time, verbose_location, LogLineCounter};

thread_local! {
    // C: two file-static locals (`log_line_number`, `log_my_pid`) in
    // write_jsonlog, distinct from write_csvlog's.
    static COUNTER: RefCell<LogLineCounter> = RefCell::new(LogLineCounter::default());
}

/// `escape_json(buf, str)` (json.c) — produce a JSON string literal from a
/// cstring. Mirrored in-crate (the json crate's escaper requires the Mcx/PgVec
/// machinery, and the seam declaration sanctions a precise in-crate copy).
/// The C loop stops at the first NUL; Rust `&str` has no embedded NUL convention
/// so we escape the whole slice, but honor the stop-at-NUL rule for fidelity.
fn escape_json(buf: &mut Vec<u8>, s: &str) {
    buf.push(b'"');
    for &c in s.as_bytes() {
        if c == 0 {
            break;
        }
        escape_json_char(buf, c);
    }
    buf.push(b'"');
}

/// `escape_json_char(buf, c)` (json.c).
fn escape_json_char(buf: &mut Vec<u8>, c: u8) {
    match c {
        0x08 => buf.extend_from_slice(b"\\b"),
        0x0c => buf.extend_from_slice(b"\\f"),
        b'\n' => buf.extend_from_slice(b"\\n"),
        b'\r' => buf.extend_from_slice(b"\\r"),
        b'\t' => buf.extend_from_slice(b"\\t"),
        b'"' => buf.extend_from_slice(b"\\\""),
        b'\\' => buf.extend_from_slice(b"\\\\"),
        _ => {
            if c < b' ' {
                const HEX: &[u8; 16] = b"0123456789abcdef";
                buf.extend_from_slice(&[
                    b'\\',
                    b'u',
                    b'0',
                    b'0',
                    HEX[((c >> 4) & 0xF) as usize],
                    HEX[(c & 0xF) as usize],
                ]);
            } else {
                buf.push(c);
            }
        }
    }
}

/// `appendJSONKeyValue(buf, key, value, escape_value)` — append a comma
/// followed by a JSON key and a value. The key is always escaped. The value is
/// escaped optionally. If `value` is `None` (C `NULL`), append nothing.
fn append_json_key_value(buf: &mut Vec<u8>, key: &str, value: Option<&str>, escape_value: bool) {
    let Some(value) = value else {
        return;
    };
    buf.push(b',');
    escape_json(buf, key);
    buf.push(b':');
    if escape_value {
        escape_json(buf, value);
    } else {
        buf.extend_from_slice(value.as_bytes());
    }
}

/// `appendJSONKeyValueFmt(buf, key, escape_key, fmt, ...)` — format the value
/// then emit it via [`append_json_key_value`]. Here the formatting happens at
/// the call site (a `String`), so this is the `escape_key`-aware wrapper.
fn append_json_key_value_fmt(buf: &mut Vec<u8>, key: &str, escape_value: bool, value: String) {
    append_json_key_value(buf, key, Some(value.as_str()), escape_value);
}

/// `write_jsonlog(edata)` — write logs in JSON format. The field set and order
/// are format-critical and match the C exactly.
pub fn write_jsonlog(edata: &PgError) {
    let log_line_number = COUNTER.with(|c| c.borrow_mut().next());

    let ctx = backend_log_context();
    let mut buf: Vec<u8> = Vec::new();

    // Initialize string
    buf.push(b'{');

    // timestamp with milliseconds — first property does not use
    // appendJSONKeyValue as it does not have a comma prefix.
    let log_time = get_formatted_log_time();
    escape_json(&mut buf, "timestamp");
    buf.push(b':');
    escape_json(&mut buf, &log_time);

    // username
    if ctx.is_some_and(|c| c.has_client_port()) {
        append_json_key_value(&mut buf, "user", ctx.and_then(|c| c.user_name()), true);
    }

    // database name
    if ctx.is_some_and(|c| c.has_client_port()) {
        append_json_key_value(&mut buf, "dbname", ctx.and_then(|c| c.database_name()), true);
    }

    // Process ID
    let pid = my_proc_pid();
    if pid != 0 {
        append_json_key_value_fmt(&mut buf, "pid", false, pid.to_string());
    }

    // Remote host and port
    if let Some(remote_host) = ctx.filter(|c| c.has_client_port()).and_then(|c| c.remote_host()) {
        append_json_key_value(&mut buf, "remote_host", Some(remote_host), true);
        if let Some(remote_port) = ctx.and_then(|c| c.remote_port()) {
            if !remote_port.is_empty() {
                append_json_key_value(&mut buf, "remote_port", Some(remote_port), false);
            }
        }
    }

    // Session id
    append_json_key_value_fmt(
        &mut buf,
        "session_id",
        true,
        format!("{:x}.{:x}", my_start_time(), pid),
    );

    // Line number
    append_json_key_value_fmt(&mut buf, "line_num", false, log_line_number.to_string());

    // PS display
    if let Some(c) = ctx.filter(|c| c.has_client_port()) {
        let psdisp = c.ps_display().unwrap_or("");
        append_json_key_value(&mut buf, "ps", Some(psdisp), true);
    }

    // session start timestamp
    append_json_key_value(&mut buf, "session_start", Some(&get_formatted_start_time()), true);

    // Virtual transaction id (keep VXID format in sync with lockfuncs.c)
    if let Some((proc_number, lxid)) = ctx.and_then(|c| c.virtual_transaction_id()) {
        append_json_key_value_fmt(&mut buf, "vxid", true, format!("{proc_number}/{lxid}"));
    }

    // Transaction id
    let top_xid = ctx.map_or(0, |c| c.top_transaction_id());
    append_json_key_value_fmt(&mut buf, "txid", false, top_xid.to_string());

    // Error severity
    append_json_key_value(&mut buf, "error_severity", Some(error_severity(edata.level)), true);

    // SQL state code
    append_json_key_value(&mut buf, "state_code", Some(&unpack_sql_state(edata.sqlstate)), true);

    // errmessage
    append_json_key_value(&mut buf, "message", Some(edata.message.as_str()), true);

    // errdetail or error_detail log
    if edata.detail_log.is_some() {
        append_json_key_value(&mut buf, "detail", edata.detail_log.as_deref(), true);
    } else {
        append_json_key_value(&mut buf, "detail", edata.detail.as_deref(), true);
    }

    // errhint
    if edata.hint.is_some() {
        append_json_key_value(&mut buf, "hint", edata.hint.as_deref(), true);
    }

    // internal query
    if edata.internal_query.is_some() {
        append_json_key_value(&mut buf, "internal_query", edata.internal_query.as_deref(), true);
    }

    // if printed internal query, print internal pos too
    let internalpos = edata.internal_position.unwrap_or(0);
    if internalpos > 0 && edata.internal_query.is_some() {
        append_json_key_value_fmt(&mut buf, "internal_position", false, internalpos.to_string());
    }

    // errcontext
    if edata.context.is_some() && !edata.hide_context {
        append_json_key_value(&mut buf, "context", edata.context.as_deref(), true);
    }

    // user query --- only reported if not disabled by the caller
    if check_log_of_query(edata) {
        append_json_key_value(&mut buf, "statement", ctx.and_then(|c| c.query_string()), true);
        let cursorpos = edata.cursor_position.unwrap_or(0);
        if cursorpos > 0 {
            append_json_key_value_fmt(&mut buf, "cursor_position", false, cursorpos.to_string());
        }
    }

    // file error location
    if verbose_location() {
        if let Some(loc) = &edata.location {
            if loc.funcname.is_some() {
                append_json_key_value(&mut buf, "func_name", loc.funcname.as_deref(), true);
            }
            if loc.filename.is_some() {
                append_json_key_value(&mut buf, "file_name", loc.filename.as_deref(), true);
                append_json_key_value_fmt(&mut buf, "file_line_num", false, loc.lineno.to_string());
            }
        }
    }

    // Application name
    if let Some(app) = ctx.and_then(|c| c.application_name()) {
        if !app.is_empty() {
            append_json_key_value(&mut buf, "application_name", Some(app), true);
        }
    }

    // backend type
    append_json_key_value(&mut buf, "backend_type", Some(&get_backend_type_for_log()), true);

    // leader PID — show the leader only for active parallel workers.
    if let Some(leader_pid) = ctx.and_then(|c| c.lock_group_leader_pid()) {
        if leader_pid != pid {
            append_json_key_value_fmt(&mut buf, "leader_pid", false, leader_pid.to_string());
        }
    }

    // query id
    let query_id = ctx.map_or(0, |c| c.query_id());
    append_json_key_value_fmt(&mut buf, "query_id", false, query_id.to_string());

    // Finish string
    buf.push(b'}');
    buf.push(b'\n');

    // If in the syslogger process, try to write messages direct to file
    if config::am_syslogger() {
        write_syslogger_file::call(&buf, LOG_DESTINATION_JSONLOG);
    } else {
        write_pipe_chunks(&buf, LOG_DESTINATION_JSONLOG);
    }
}
