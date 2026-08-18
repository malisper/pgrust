//! write_csvlog (csvlog.c) + write_jsonlog (jsonlog.c): the structured
//! server-log writers behind `log_destination = csvlog | jsonlog`. Hosted
//! here (not in elog) so the syslogger-thread arm can write straight to the
//! collector's file; backends route through the log pipe as in C. Installed
//! into `error_small_seams` alongside the syslogger seams.

use elog::sink::backend_log_context;
use elog::{
    check_log_of_query, current_query_string, error_severity, get_formatted_log_time,
    get_formatted_start_time, unpack_sql_state, write_pipe_chunks,
};
use std::cell::Cell;
use types_error::{PGErrorVerbosity, PgError, LOG_DESTINATION_CSVLOG, LOG_DESTINATION_JSONLOG};

use crate::write_syslogger_file;

// C keeps one `static long log_line_number` per writer, reset when MyProcPid
// changes (a fork-inheritance artifact). Thread-per-backend makes a fresh
// thread-local the exact analog: each backend starts at 0.
thread_local! {
    static CSV_LOG_LINE_NUMBER: Cell<i64> = const { Cell::new(0) };
    static JSON_LOG_LINE_NUMBER: Cell<i64> = const { Cell::new(0) };
}

// appendCSVLiteral (csvlog.c): PostgreSQL CSV defaults, quote = escape = '"';
// NULL appends nothing (an empty string still gets quotes).
fn append_csv_literal(buf: &mut String, data: Option<&str>) {
    let Some(data) = data else { return };
    buf.push('"');
    for c in data.chars() {
        if c == '"' {
            buf.push('"');
        }
        buf.push(c);
    }
    buf.push('"');
}

// escape_json (src/common/jsonapi.c): backslash escapes for the JSON control
// set, \uXXXX for remaining chars < 0x20, everything else verbatim.
fn append_escaped_json(buf: &mut String, s: &str) {
    buf.push('"');
    for c in s.chars() {
        match c {
            '\x08' => buf.push_str("\\b"),
            '\x0c' => buf.push_str("\\f"),
            '\n' => buf.push_str("\\n"),
            '\r' => buf.push_str("\\r"),
            '\t' => buf.push_str("\\t"),
            '"' => buf.push_str("\\\""),
            '\\' => buf.push_str("\\\\"),
            c if (c as u32) < 0x20 => {
                buf.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => buf.push(c),
        }
    }
    buf.push('"');
}

// appendJSONKeyValue (jsonlog.c): comma, escaped key, ':', value (escaped or
// verbatim); a missing value appends nothing at all.
fn append_json_key_value(buf: &mut String, key: &str, value: Option<&str>, escape_value: bool) {
    let Some(value) = value else { return };
    buf.push(',');
    append_escaped_json(buf, key);
    buf.push(':');
    if escape_value {
        append_escaped_json(buf, value);
    } else {
        buf.push_str(value);
    }
}

fn route(buf: &str, destination: i32) {
    // C: if (MyBackendType == B_LOGGER) write direct; else down the log pipe.
    if elog::config::am_syslogger() {
        write_syslogger_file(buf.as_bytes(), destination);
    } else {
        write_pipe_chunks(buf.as_bytes(), destination);
    }
}

/// write_csvlog (csvlog.c) — column order is documented in config.sgml and
/// load-bearing for anyone ingesting the .csv files; keep it byte-exact.
pub fn write_csvlog(edata: &PgError) {
    let context = backend_log_context();
    let my_pid = context.map_or_else(init_small::globals::process_id, |c| c.process_id());
    let has_port = context.is_some_and(|c| c.has_client_port());

    let line_number = CSV_LOG_LINE_NUMBER.with(|c| {
        c.set(c.get() + 1);
        c.get()
    });

    let mut buf = String::new();

    // timestamp with milliseconds
    buf.push_str(&get_formatted_log_time());
    buf.push(',');

    // username
    if has_port {
        append_csv_literal(&mut buf, context.and_then(|c| c.user_name()));
    }
    buf.push(',');

    // database name
    if has_port {
        append_csv_literal(&mut buf, context.and_then(|c| c.database_name()));
    }
    buf.push(',');

    // process id
    if my_pid != 0 {
        buf.push_str(&my_pid.to_string());
    }
    buf.push(',');

    // remote host and port
    if let Some(remote_host) = context.filter(|c| c.has_client_port()).and_then(|c| c.remote_host())
    {
        buf.push('"');
        buf.push_str(remote_host);
        if let Some(remote_port) = context.and_then(|c| c.remote_port()) {
            if !remote_port.is_empty() {
                buf.push(':');
                buf.push_str(remote_port);
            }
        }
        buf.push('"');
    }
    buf.push(',');

    // session id (MyStartTime.MyProcPid, both hex)
    let start = context.map_or(0, |c| c.session_start_time());
    buf.push_str(&format!("{:x}.{:x}", start, my_pid));
    buf.push(',');

    // line number
    buf.push_str(&line_number.to_string());
    buf.push(',');

    // PS display
    if has_port {
        append_csv_literal(&mut buf, Some(context.and_then(|c| c.ps_display()).unwrap_or("")));
    }
    buf.push(',');

    // session start timestamp
    buf.push_str(&get_formatted_start_time());
    buf.push(',');

    // virtual transaction id — keep VXID format in sync with lockfuncs.c
    if let Some((proc_number, lxid)) = context.and_then(|c| c.virtual_transaction_id()) {
        buf.push_str(&format!("{}/{}", proc_number, lxid));
    }
    buf.push(',');

    // transaction id
    buf.push_str(&context.map_or(0, |c| c.top_transaction_id()).to_string());
    buf.push(',');

    // error severity
    buf.push_str(error_severity(edata.level));
    buf.push(',');

    // SQL state code
    buf.push_str(&unpack_sql_state(edata.sqlstate));
    buf.push(',');

    // errmessage
    append_csv_literal(&mut buf, Some(edata.message.as_str()));
    buf.push(',');

    // errdetail or errdetail_log
    append_csv_literal(&mut buf, edata.detail_log.as_deref().or(edata.detail.as_deref()));
    buf.push(',');

    // errhint
    append_csv_literal(&mut buf, edata.hint.as_deref());
    buf.push(',');

    // internal query
    append_csv_literal(&mut buf, edata.internal_query.as_deref());
    buf.push(',');

    // if printed internal query, print internal pos too
    if let (Some(pos), Some(_)) = (edata.internal_position, &edata.internal_query) {
        if pos > 0 {
            buf.push_str(&pos.to_string());
        }
    }
    buf.push(',');

    // errcontext
    if !edata.hide_context {
        append_csv_literal(&mut buf, edata.context.as_deref());
    }
    buf.push(',');

    // user query — only reported if not disabled by the caller
    let print_stmt = check_log_of_query(edata);
    let query = if print_stmt { current_query_string() } else { None };
    append_csv_literal(&mut buf, query.as_deref());
    buf.push(',');
    if query.is_some() {
        if let Some(pos) = edata.cursor_position {
            if pos > 0 {
                buf.push_str(&pos.to_string());
            }
        }
    }
    buf.push(',');

    // file error location
    if elog::config::log_error_verbosity() >= PGErrorVerbosity::Verbose {
        let mut msgbuf = String::new();
        if let Some(loc) = &edata.location {
            match (&loc.funcname, &loc.filename) {
                (Some(func), Some(file)) => {
                    msgbuf = format!("{}, {}:{}", func, file, loc.lineno);
                }
                (None, Some(file)) => {
                    msgbuf = format!("{}:{}", file, loc.lineno);
                }
                _ => {}
            }
        }
        append_csv_literal(&mut buf, Some(&msgbuf));
    }
    buf.push(',');

    // application name
    if let Some(appname) = context.and_then(|c| c.application_name()) {
        append_csv_literal(&mut buf, Some(appname));
    }
    buf.push(',');

    // backend type
    append_csv_literal(&mut buf, Some(&elog::get_backend_type_for_log()));
    buf.push(',');

    // leader PID — only for active parallel workers
    if let Some(leader_pid) = context.and_then(|c| c.lock_group_leader_pid()) {
        if leader_pid != my_pid {
            buf.push_str(&leader_pid.to_string());
        }
    }
    buf.push(',');

    // query id
    buf.push_str(&context.map_or(0, |c| c.query_id()).to_string());

    buf.push('\n');

    route(&buf, LOG_DESTINATION_CSVLOG);
}

/// write_jsonlog (jsonlog.c) — one JSON object per line; keys match C's.
pub fn write_jsonlog(edata: &PgError) {
    let context = backend_log_context();
    let my_pid = context.map_or_else(init_small::globals::process_id, |c| c.process_id());
    let has_port = context.is_some_and(|c| c.has_client_port());

    let line_number = JSON_LOG_LINE_NUMBER.with(|c| {
        c.set(c.get() + 1);
        c.get()
    });

    let mut buf = String::new();
    buf.push('{');

    // timestamp with milliseconds — first property, no comma prefix
    append_escaped_json(&mut buf, "timestamp");
    buf.push(':');
    append_escaped_json(&mut buf, &get_formatted_log_time());

    if has_port {
        append_json_key_value(&mut buf, "user", context.and_then(|c| c.user_name()), true);
        append_json_key_value(&mut buf, "dbname", context.and_then(|c| c.database_name()), true);
    }

    if my_pid != 0 {
        append_json_key_value(&mut buf, "pid", Some(&my_pid.to_string()), false);
    }

    if let Some(remote_host) = context.filter(|c| c.has_client_port()).and_then(|c| c.remote_host())
    {
        append_json_key_value(&mut buf, "remote_host", Some(remote_host), true);
        if let Some(remote_port) = context.and_then(|c| c.remote_port()) {
            if !remote_port.is_empty() {
                append_json_key_value(&mut buf, "remote_port", Some(remote_port), false);
            }
        }
    }

    let start = context.map_or(0, |c| c.session_start_time());
    append_json_key_value(&mut buf, "session_id", Some(&format!("{:x}.{:x}", start, my_pid)), true);
    append_json_key_value(&mut buf, "line_num", Some(&line_number.to_string()), false);

    if has_port {
        append_json_key_value(
            &mut buf,
            "ps",
            Some(context.and_then(|c| c.ps_display()).unwrap_or("")),
            true,
        );
    }

    append_json_key_value(&mut buf, "session_start", Some(&get_formatted_start_time()), true);

    if let Some((proc_number, lxid)) = context.and_then(|c| c.virtual_transaction_id()) {
        append_json_key_value(&mut buf, "vxid", Some(&format!("{}/{}", proc_number, lxid)), true);
    }

    append_json_key_value(
        &mut buf,
        "txid",
        Some(&context.map_or(0, |c| c.top_transaction_id()).to_string()),
        false,
    );

    append_json_key_value(&mut buf, "error_severity", Some(error_severity(edata.level)), true);
    append_json_key_value(&mut buf, "state_code", Some(&unpack_sql_state(edata.sqlstate)), true);
    append_json_key_value(&mut buf, "message", Some(edata.message.as_str()), true);
    append_json_key_value(
        &mut buf,
        "detail",
        edata.detail_log.as_deref().or(edata.detail.as_deref()),
        true,
    );
    append_json_key_value(&mut buf, "hint", edata.hint.as_deref(), true);
    append_json_key_value(&mut buf, "internal_query", edata.internal_query.as_deref(), true);
    if let (Some(pos), Some(_)) = (edata.internal_position, &edata.internal_query) {
        if pos > 0 {
            append_json_key_value(&mut buf, "internal_position", Some(&pos.to_string()), false);
        }
    }
    if !edata.hide_context {
        append_json_key_value(&mut buf, "context", edata.context.as_deref(), true);
    }

    if check_log_of_query(edata) {
        if let Some(query) = current_query_string() {
            append_json_key_value(&mut buf, "statement", Some(&query), true);
            if let Some(pos) = edata.cursor_position {
                if pos > 0 {
                    append_json_key_value(&mut buf, "cursor_position", Some(&pos.to_string()), false);
                }
            }
        }
    }

    if elog::config::log_error_verbosity() >= PGErrorVerbosity::Verbose {
        if let Some(loc) = &edata.location {
            append_json_key_value(&mut buf, "func_name", loc.funcname.as_deref(), true);
            if let Some(file) = &loc.filename {
                append_json_key_value(&mut buf, "file_name", Some(file), true);
                append_json_key_value(&mut buf, "file_line_num", Some(&loc.lineno.to_string()), false);
            }
        }
    }

    if let Some(appname) = context.and_then(|c| c.application_name()) {
        if !appname.is_empty() {
            append_json_key_value(&mut buf, "application_name", Some(appname), true);
        }
    }

    append_json_key_value(&mut buf, "backend_type", Some(&elog::get_backend_type_for_log()), true);

    if let Some(leader_pid) = context.and_then(|c| c.lock_group_leader_pid()) {
        if leader_pid != my_pid {
            append_json_key_value(&mut buf, "leader_pid", Some(&leader_pid.to_string()), false);
        }
    }

    append_json_key_value(
        &mut buf,
        "query_id",
        Some(&context.map_or(0, |c| c.query_id()).to_string()),
        false,
    );

    buf.push('}');
    buf.push('\n');

    route(&buf, LOG_DESTINATION_JSONLOG);
}
