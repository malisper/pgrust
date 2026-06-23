//! `utils/error/csvlog.c` — CSV server-log line formatter.

use std::cell::RefCell;

use ::syslogger_seams::write_syslogger_file;
use ::utils_error::{
    backend_log_context, check_log_of_query, config, error_severity, get_backend_type_for_log,
    get_formatted_log_time, get_formatted_start_time, unpack_sql_state, write_pipe_chunks, PgError,
};
use ::types_error::LOG_DESTINATION_CSVLOG;

use crate::{my_proc_pid, my_start_time, verbose_location, LogLineCounter};

thread_local! {
    // C: two file-static locals (`log_line_number`, `log_my_pid`) in
    // write_csvlog. Per-backend == per-thread here.
    static COUNTER: RefCell<LogLineCounter> = RefCell::new(LogLineCounter::default());
}

/// `appendCSVLiteral(buf, data)` — append a CSV-quoted version of a string.
///
/// We use the PostgreSQL defaults for CSV, i.e. quote = escape = `"`. If it's
/// `None` (the C `NULL`), append nothing — this avoids confusing an empty
/// string with NULL.
fn append_csv_literal(buf: &mut Vec<u8>, data: Option<&str>) {
    let Some(data) = data else {
        return;
    };
    buf.push(b'"');
    for &c in data.as_bytes() {
        if c == b'"' {
            buf.push(b'"');
        }
        buf.push(c);
    }
    buf.push(b'"');
}

/// Like [`append_csv_literal`] but for raw bytes (the PS display, which the C
/// builds via `appendBinaryStringInfo`).
fn append_csv_literal_bytes(buf: &mut Vec<u8>, data: &[u8]) {
    buf.push(b'"');
    for &c in data {
        if c == b'"' {
            buf.push(b'"');
        }
        buf.push(c);
    }
    buf.push(b'"');
}

fn append_str(buf: &mut Vec<u8>, s: &str) {
    buf.extend_from_slice(s.as_bytes());
}

/// `write_csvlog(edata)` — generate and write a CSV log entry.
///
/// Constructs the error message, depending on the `ErrorData` it gets, in a CSV
/// format which is described in doc/src/sgml/config.sgml. The column order is
/// format-critical and matches the C exactly.
pub fn write_csvlog(edata: &PgError) {
    let log_line_number = COUNTER.with(|c| c.borrow_mut().next());

    let ctx = backend_log_context();
    let mut buf: Vec<u8> = Vec::new();

    // timestamp with milliseconds
    append_str(&mut buf, &get_formatted_log_time());
    buf.push(b',');

    // username
    if ctx.is_some_and(|c| c.has_client_port()) {
        append_csv_literal(&mut buf, ctx.and_then(|c| c.user_name()));
    }
    buf.push(b',');

    // database name
    if ctx.is_some_and(|c| c.has_client_port()) {
        append_csv_literal(&mut buf, ctx.and_then(|c| c.database_name()));
    }
    buf.push(b',');

    // Process id
    let pid = my_proc_pid();
    if pid != 0 {
        append_str(&mut buf, &pid.to_string());
    }
    buf.push(b',');

    // Remote host and port
    if let Some(remote_host) = ctx.filter(|c| c.has_client_port()).and_then(|c| c.remote_host()) {
        buf.push(b'"');
        append_str(&mut buf, remote_host);
        if let Some(remote_port) = ctx.and_then(|c| c.remote_port()) {
            if !remote_port.is_empty() {
                buf.push(b':');
                append_str(&mut buf, remote_port);
            }
        }
        buf.push(b'"');
    }
    buf.push(b',');

    // session id
    append_str(&mut buf, &format!("{:x}.{:x}", my_start_time(), pid));
    buf.push(b',');

    // Line number
    append_str(&mut buf, &log_line_number.to_string());
    buf.push(b',');

    // PS display
    if let Some(c) = ctx.filter(|c| c.has_client_port()) {
        let psdisp = c.ps_display().unwrap_or("");
        append_csv_literal_bytes(&mut buf, psdisp.as_bytes());
    }
    buf.push(b',');

    // session start timestamp
    append_str(&mut buf, &get_formatted_start_time());
    buf.push(b',');

    // Virtual transaction id (keep VXID format in sync with lockfuncs.c)
    if let Some((proc_number, lxid)) = ctx.and_then(|c| c.virtual_transaction_id()) {
        append_str(&mut buf, &format!("{proc_number}/{lxid}"));
    }
    buf.push(b',');

    // Transaction id
    let top_xid = ctx.map_or(0, |c| c.top_transaction_id());
    append_str(&mut buf, &top_xid.to_string());
    buf.push(b',');

    // Error severity
    append_str(&mut buf, error_severity(edata.level));
    buf.push(b',');

    // SQL state code
    append_str(&mut buf, &unpack_sql_state(edata.sqlstate));
    buf.push(b',');

    // errmessage
    append_csv_literal(&mut buf, Some(edata.message.as_str()));
    buf.push(b',');

    // errdetail or errdetail_log
    if edata.detail_log.is_some() {
        append_csv_literal(&mut buf, edata.detail_log.as_deref());
    } else {
        append_csv_literal(&mut buf, edata.detail.as_deref());
    }
    buf.push(b',');

    // errhint
    append_csv_literal(&mut buf, edata.hint.as_deref());
    buf.push(b',');

    // internal query
    append_csv_literal(&mut buf, edata.internal_query.as_deref());
    buf.push(b',');

    // if printed internal query, print internal pos too
    let internalpos = edata.internal_position.unwrap_or(0);
    if internalpos > 0 && edata.internal_query.is_some() {
        append_str(&mut buf, &internalpos.to_string());
    }
    buf.push(b',');

    // errcontext
    if !edata.hide_context {
        append_csv_literal(&mut buf, edata.context.as_deref());
    }
    buf.push(b',');

    // user query --- only reported if not disabled by the caller
    let print_stmt = check_log_of_query(edata);
    if print_stmt {
        append_csv_literal(&mut buf, ctx.and_then(|c| c.query_string()));
    }
    buf.push(b',');
    let cursorpos = edata.cursor_position.unwrap_or(0);
    if print_stmt && cursorpos > 0 {
        append_str(&mut buf, &cursorpos.to_string());
    }
    buf.push(b',');

    // file error location
    if verbose_location() {
        let mut msgbuf = String::new();
        if let Some(loc) = &edata.location {
            match (&loc.funcname, &loc.filename) {
                (Some(funcname), Some(filename)) => {
                    msgbuf = format!("{funcname}, {filename}:{}", loc.lineno);
                }
                (_, Some(filename)) => {
                    msgbuf = format!("{filename}:{}", loc.lineno);
                }
                _ => {}
            }
        }
        append_csv_literal(&mut buf, Some(msgbuf.as_str()));
    }
    buf.push(b',');

    // application name
    if let Some(app) = ctx.and_then(|c| c.application_name()) {
        append_csv_literal(&mut buf, Some(app));
    }
    buf.push(b',');

    // backend type
    append_csv_literal(&mut buf, Some(get_backend_type_for_log().as_str()));
    buf.push(b',');

    // leader PID — show the leader only for active parallel workers (leaves out
    // the leader of a parallel group).
    if let Some(leader_pid) = ctx.and_then(|c| c.lock_group_leader_pid()) {
        if leader_pid != pid {
            append_str(&mut buf, &leader_pid.to_string());
        }
    }
    buf.push(b',');

    // query id
    let query_id = ctx.map_or(0, |c| c.query_id());
    append_str(&mut buf, &query_id.to_string());

    buf.push(b'\n');

    // If in the syslogger process, try to write messages direct to file
    if config::am_syslogger() {
        write_syslogger_file::call(&buf, LOG_DESTINATION_CSVLOG);
    } else {
        write_pipe_chunks(&buf, LOG_DESTINATION_CSVLOG);
    }
}
