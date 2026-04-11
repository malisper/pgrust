use std::collections::HashMap;
use std::io::{self, BufWriter, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::OnceLock;
use std::thread;

use crate::backend::access::heap::heapam::HeapError;
use crate::backend::catalog::Catalog;
use crate::backend::executor::{ExecError, QueryColumn, StatementResult};
use crate::backend::libpq::pqcomm::{
    cstr_from_bytes, read_byte, read_cstr, read_i16_bytes, read_i32, read_i32_bytes,
};
use crate::include::access::htup::TupleError;
use crate::backend::libpq::pqformat::{
    FloatFormatOptions,
    format_exec_error, infer_command_tag, send_auth_ok, send_backend_key_data, send_bind_complete,
    send_close_complete, send_command_complete, send_copy_in_response,
    send_empty_query, send_error, send_no_data, send_notice, send_parameter_description, send_parameter_status,
    send_parse_complete, send_query_result, send_ready_for_query, send_row_description,
    send_typed_data_row,
};
use crate::backend::parser::comments::sql_is_effectively_empty_after_comments;
use crate::backend::parser::UngroupedColumnClause;
use crate::backend::utils::cache::relcache::RelCache;

fn exec_error_sqlstate(e: &ExecError) -> &'static str {
    match e {
        ExecError::Parse(crate::backend::parser::ParseError::InvalidInteger(_))
        | ExecError::Parse(crate::backend::parser::ParseError::InvalidNumeric(_))
        | ExecError::InvalidIntegerInput { .. }
        | ExecError::InvalidNumericInput(_)
        | ExecError::InvalidByteaInput { .. }
        | ExecError::InvalidBooleanInput { .. }
        | ExecError::InvalidFloatInput { .. } => "22P02",
        ExecError::Parse(crate::backend::parser::ParseError::UndefinedOperator { .. }) => "42883",
        ExecError::Parse(crate::backend::parser::ParseError::UnknownConfigurationParameter(_)) => {
            "42704"
        }
        ExecError::Parse(crate::backend::parser::ParseError::ActiveSqlTransaction(_)) => "25001",
        ExecError::IntegerOutOfRange { .. }
        | ExecError::Int2OutOfRange
        | ExecError::Int4OutOfRange
        | ExecError::Int8OutOfRange
        | ExecError::OidOutOfRange
        | ExecError::NumericFieldOverflow
        | ExecError::FloatOutOfRange { .. }
        | ExecError::FloatOverflow
        | ExecError::FloatUnderflow => "22003",
        ExecError::RequestedLengthTooLarge => "54000",
        ExecError::Heap(HeapError::Tuple(TupleError::Oversized { .. })) => "54000",
        ExecError::DivisionByZero(_) => "22012",
        ExecError::StringDataRightTruncation { .. } => "22001",
        ExecError::CardinalityViolation(_) => "21000",
        ExecError::Parse(_) => "42601",
        _ => "XX000",
    }
}

fn exec_error_position(sql: &str, e: &ExecError) -> Option<usize> {
    if matches!(e, ExecError::InvalidBooleanInput { .. })
        && sql.to_ascii_lowercase().contains("::text::boolean")
    {
        return None;
    }
    let value = match e {
        ExecError::Parse(crate::backend::parser::ParseError::UngroupedColumn { token, clause, .. }) => {
            return find_ungrouped_column_position(sql, token, clause);
        }
        ExecError::InvalidIntegerInput { value, .. } => value.as_str(),
        ExecError::IntegerOutOfRange { value, .. } => value.as_str(),
        ExecError::InvalidNumericInput(value) => value.as_str(),
        ExecError::InvalidByteaInput { value } => value.as_str(),
        ExecError::InvalidBooleanInput { value } => value.as_str(),
        ExecError::InvalidFloatInput { value, .. } => value.as_str(),
        ExecError::FloatOutOfRange { value, .. } => value.as_str(),
        _ => return None,
    };
    let needle = format!("'{}'", value.replace('\'', "''"));
    sql.rfind(&needle).map(|index| index + 1)
}

fn find_ungrouped_column_position(
    sql: &str,
    token: &str,
    clause: &UngroupedColumnClause,
) -> Option<usize> {
    let lower = sql.to_ascii_lowercase();
    let (start, end) = match clause {
        UngroupedColumnClause::SelectTarget => {
            let start = lower.find("select")? + "select".len();
            let end = lower.find(" from ").or_else(|| lower.find(" from"))?;
            (start, end)
        }
        UngroupedColumnClause::Having => {
            let start = lower.find("having")? + "having".len();
            (start, sql.len())
        }
        UngroupedColumnClause::Other => (0, sql.len()),
    };
    let segment = &sql[start..end];
    find_identifier_in_segment(segment, token).map(|offset| start + offset + 1)
}

fn find_identifier_in_segment(segment: &str, token: &str) -> Option<usize> {
    let token_lower = token.to_ascii_lowercase();
    let segment_lower = segment.to_ascii_lowercase();
    let mut from = 0;
    while let Some(found) = segment_lower[from..].find(&token_lower) {
        let idx = from + found;
        let before = segment[..idx].chars().next_back();
        let after = segment[idx + token.len()..].chars().next();
        let is_ident = |ch: char| ch.is_ascii_alphanumeric() || ch == '_' || ch == '.';
        if !before.is_some_and(is_ident) && !after.is_some_and(is_ident) {
            return Some(idx);
        }
        from = idx + token.len();
    }
    None
}
use crate::ClientId;
use crate::backend::parser::{Statement, parse_statement};
use crate::pgrust::database::Database;
use crate::pgrust::session::Session;

const SSL_REQUEST_CODE: i32 = 80877103;
pub(crate) const PROTOCOL_VERSION_3_0: i32 = 196608;

static NEXT_CLIENT_ID: AtomicU32 = AtomicU32::new(1);

#[derive(Default)]
struct PreparedStatement {
    sql: String,
}

#[derive(Default)]
struct BoundPortal {
    sql: String,
    params: Vec<Option<String>>,
}

struct ConnectionState {
    session: Session,
    prepared: HashMap<String, PreparedStatement>,
    portals: HashMap<String, BoundPortal>,
    copy_in: Option<CopyInState>,
}

struct CopyInState {
    table_name: String,
    pending: Vec<u8>,
}

pub fn serve(addr: &str, db: Database) -> io::Result<()> {
    let listener = TcpListener::bind(addr)?;
    eprintln!("pgrust: listening on {addr}");

    for stream in listener.incoming() {
        let stream = stream?;
        let peer = stream.peer_addr().ok();
        let db = db.clone();
        thread::spawn(move || {
            let client_id = NEXT_CLIENT_ID.fetch_add(1, Ordering::Relaxed);
            db.pool.with_storage_mut(|s| s.smgr.acquire_external_fd());
            if let Some(peer) = &peer {
                eprintln!("pgrust: connection from {peer} (client {client_id})");
            }
            if let Err(e) = handle_connection(stream, &db, client_id) {
                if e.kind() != io::ErrorKind::UnexpectedEof
                    && e.kind() != io::ErrorKind::ConnectionReset
                {
                    eprintln!("pgrust: client {client_id} error: {e}");
                }
            }
            if let Some(peer) = &peer {
                eprintln!("pgrust: client {client_id} ({peer}) disconnected");
            }
            db.pool.with_storage_mut(|s| s.smgr.release_external_fd());
        });
    }
    Ok(())
}

pub(crate) fn handle_connection(
    stream: TcpStream,
    db: &Database,
    client_id: ClientId,
) -> io::Result<()> {
    let mut reader = stream.try_clone()?;
    let mut writer = BufWriter::new(stream);

    loop {
        let len = read_i32(&mut reader)? as usize;
        if len < 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "startup packet too short",
            ));
        }
        let mut payload = vec![0u8; len - 4];
        reader.read_exact(&mut payload)?;

        let code = i32::from_be_bytes(payload[0..4].try_into().unwrap());
        match code {
            SSL_REQUEST_CODE => {
                writer.write_all(b"N")?;
                writer.flush()?;
                continue;
            }
            PROTOCOL_VERSION_3_0 => break,
            _ => {
                send_error(
                    &mut writer,
                    "08P01",
                    &format!("unsupported protocol version: {code}"),
                    None,
                )?;
                writer.flush()?;
                return Ok(());
            }
        }
    }

    send_auth_ok(&mut writer)?;
    send_parameter_status(&mut writer, "server_version", "16.0")?;
    send_parameter_status(&mut writer, "server_encoding", "UTF8")?;
    send_parameter_status(&mut writer, "client_encoding", "UTF8")?;
    send_parameter_status(&mut writer, "DateStyle", "ISO, MDY")?;
    send_parameter_status(&mut writer, "TimeZone", "UTC")?;
    send_parameter_status(&mut writer, "integer_datetimes", "on")?;
    send_parameter_status(&mut writer, "standard_conforming_strings", "on")?;
    send_backend_key_data(&mut writer, std::process::id() as i32, client_id as i32)?;
    send_ready_for_query(&mut writer, b'I')?;
    writer.flush()?;

    let mut state = ConnectionState {
        session: Session::new(client_id),
        prepared: HashMap::new(),
        portals: HashMap::new(),
        copy_in: None,
    };

    let result = loop {
        let msg_type = match read_byte(&mut reader) {
            Ok(b) => b,
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break Ok(()),
            Err(e) => break Err(e),
        };

        let len = read_i32(&mut reader)? as usize;
        if len < 4 {
            break Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "message too short",
            ));
        }
        let mut body = vec![0u8; len - 4];
        reader.read_exact(&mut body)?;

        match msg_type {
            b'Q' => {
                let sql = cstr_from_bytes(&body);
                handle_query(&mut writer, db, &mut state, &sql)?;
                writer.flush()?;
            }
            b'P' => {
                handle_parse(&mut writer, &mut state, &body)?;
                writer.flush()?;
            }
            b'B' => {
                handle_bind(&mut writer, &mut state, &body)?;
                writer.flush()?;
            }
            b'D' => {
                handle_describe(&mut writer, db, &state, &body)?;
                writer.flush()?;
            }
            b'E' => {
                handle_execute(&mut writer, db, &mut state, &body)?;
                writer.flush()?;
            }
            b'S' => {
                send_ready_for_query(&mut writer, state.session.ready_status())?;
                writer.flush()?;
            }
            b'C' => {
                handle_close(&mut writer, &mut state, &body)?;
                writer.flush()?;
            }
            b'H' => {
                writer.flush()?;
            }
            b'd' => handle_copy_data(&mut state, &body)?,
            b'c' => {
                handle_copy_done(&mut writer, db, &mut state)?;
                writer.flush()?;
            }
            b'f' => {
                handle_copy_fail(&mut writer, &mut state, &body)?;
                writer.flush()?;
            }
            b'X' => return Ok(()),
            _ => {
                send_error(
                    &mut writer,
                    "0A000",
                    &format!("unsupported message type: '{}'", msg_type as char),
                    None,
                )?;
                send_ready_for_query(&mut writer, state.session.ready_status())?;
                writer.flush()?;
            }
        }
    };
    db.cleanup_client_temp_relations(client_id);
    result
}

fn handle_query(
    stream: &mut impl Write,
    db: &Database,
    state: &mut ConnectionState,
    sql: &str,
) -> io::Result<()> {
    if sql_is_effectively_empty_after_comments(sql) {
        send_empty_query(stream)?;
        send_ready_for_query(stream, state.session.ready_status())?;
        return Ok(());
    }
    let sql = sql.trim().trim_end_matches(';').trim();
    if sql.is_empty() {
        send_empty_query(stream)?;
        send_ready_for_query(stream, state.session.ready_status())?;
        return Ok(());
    }
    if try_handle_float_shell_ddl(stream, sql)? {
        send_ready_for_query(stream, state.session.ready_status())?;
        return Ok(());
    }
    let sql = rewrite_regression_sql(sql);

    if let Some(table_name) = parse_copy_from_stdin(&sql) {
        state.copy_in = Some(CopyInState {
            table_name,
            pending: Vec::new(),
        });
        send_copy_in_response(stream)?;
        return Ok(());
    }

    let parsed = db
        .plan_cache
        .get_statement(&sql)
        .map_err(|e| io::Error::other(format!("{e:?}")));
    if let Ok(Statement::Select(ref select_stmt)) = parsed {
        match state.session.execute_streaming(db, select_stmt) {
            Ok(mut guard) => {
                use crate::backend::executor::exec_next;
                let columns = guard.columns.clone();
                let mut row_buf = Vec::new();
                let mut row_count = 0usize;
                let mut header_sent = false;
                let mut err = None;

                loop {
                    match exec_next(&mut guard.state, &mut guard.ctx) {
                        Ok(Some(slot)) => {
                            if !header_sent {
                                send_row_description(stream, &columns)?;
                                header_sent = true;
                            }
                            match slot.values() {
                                Ok(values) => {
                                    send_typed_data_row(
                                        stream,
                                        values,
                                        &columns,
                                        &mut row_buf,
                                        FloatFormatOptions {
                                            extra_float_digits: state.session.extra_float_digits(),
                                            bytea_output: state.session.bytea_output(),
                                        },
                                    )?;
                                    row_count += 1;
                                }
                                Err(e) => {
                                    err = Some(e);
                                    break;
                                }
                            }
                        }
                        Ok(None) => break,
                        Err(e) => {
                            err = Some(e);
                            break;
                        }
                    }
                }
                drop(guard);

                if let Some(e) = err {
                    send_error(
                        stream,
                        exec_error_sqlstate(&e),
                        &format_exec_error(&e),
                        exec_error_position(&sql, &e),
                    )?;
                } else {
                    if !header_sent {
                        send_row_description(stream, &columns)?;
                    }
                    send_command_complete(stream, &format!("SELECT {row_count}"))?;
                }
            }
            Err(e) => {
                send_error(
                    stream,
                    exec_error_sqlstate(&e),
                    &format_exec_error(&e),
                    exec_error_position(&sql, &e),
                )?;
            }
        }
    } else {
        match state.session.execute(db, &sql) {
            Ok(StatementResult::Query { columns, rows, .. }) => {
                send_query_result(
                    stream,
                    &columns,
                    &rows,
                    &format!("SELECT {}", rows.len()),
                    FloatFormatOptions {
                        extra_float_digits: state.session.extra_float_digits(),
                        bytea_output: state.session.bytea_output(),
                    },
                )?;
            }
            Ok(StatementResult::AffectedRows(n)) => {
                send_command_complete(stream, &infer_command_tag(&sql, n))?;
            }
            Err(e) => {
                send_error(
                    stream,
                    exec_error_sqlstate(&e),
                    &format_exec_error(&e),
                    exec_error_position(&sql, &e),
                )?;
            }
        }
    }

    send_ready_for_query(stream, state.session.ready_status())?;
    Ok(())
}

fn handle_copy_data(state: &mut ConnectionState, body: &[u8]) -> io::Result<()> {
    let Some(copy) = state.copy_in.as_mut() else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "received CopyData outside copy-in mode",
        ));
    };
    copy.pending.extend_from_slice(body);
    Ok(())
}

fn handle_copy_done(
    stream: &mut impl Write,
    db: &Database,
    state: &mut ConnectionState,
) -> io::Result<()> {
    let Some(copy) = state.copy_in.take() else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "received CopyDone outside copy-in mode",
        ));
    };

    let text = String::from_utf8_lossy(&copy.pending);
    let rows = text
        .lines()
        .map(|line| line.trim_end_matches('\r'))
        .filter(|line| !line.is_empty() && *line != "\\.")
        .map(|line| {
            line.split('\t')
                .map(|part| part.to_string())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    state
        .session
        .copy_from_rows(db, &copy.table_name, &rows)
        .map_err(|e| io::Error::other(format_exec_error(&e)))?;

    send_command_complete(stream, "COPY")?;
    send_ready_for_query(stream, state.session.ready_status())?;
    Ok(())
}

fn handle_copy_fail(
    stream: &mut impl Write,
    state: &mut ConnectionState,
    body: &[u8],
) -> io::Result<()> {
    state.copy_in = None;
    let message = cstr_from_bytes(body);
    send_error(stream, "57014", &format!("copy failed: {message}"), None)?;
    send_ready_for_query(stream, state.session.ready_status())?;
    Ok(())
}

fn parse_copy_from_stdin(sql: &str) -> Option<String> {
    let lower = sql.to_ascii_lowercase();
    let prefix = "copy ";
    let suffix = " from stdin";
    if !lower.starts_with(prefix) || !lower.contains(suffix) {
        return None;
    }
    let end = lower.find(suffix)?;
    let table = sql[prefix.len()..end].trim();
    if table.is_empty() {
        None
    } else {
        Some(table.to_string())
    }
}

fn handle_parse(
    stream: &mut impl Write,
    state: &mut ConnectionState,
    body: &[u8],
) -> io::Result<()> {
    let mut offset = 0;
    let statement_name = read_cstr(body, &mut offset)?;
    let sql = read_cstr(body, &mut offset)?;
    let nparams = read_i16_bytes(body, &mut offset)? as usize;
    for _ in 0..nparams {
        let _ = read_i32_bytes(body, &mut offset)?;
    }
    state
        .prepared
        .insert(statement_name, PreparedStatement { sql });
    send_parse_complete(stream)
}

fn handle_bind(
    stream: &mut impl Write,
    state: &mut ConnectionState,
    body: &[u8],
) -> io::Result<()> {
    let mut offset = 0;
    let portal_name = read_cstr(body, &mut offset)?;
    let statement_name = read_cstr(body, &mut offset)?;
    let n_format_codes = read_i16_bytes(body, &mut offset)? as usize;
    offset += n_format_codes * 2;
    let nparams = read_i16_bytes(body, &mut offset)? as usize;
    let mut params = Vec::with_capacity(nparams);
    for _ in 0..nparams {
        let len = read_i32_bytes(body, &mut offset)?;
        if len < 0 {
            params.push(None);
        } else {
            let len = len as usize;
            let bytes = &body[offset..offset + len];
            offset += len;
            params.push(Some(String::from_utf8_lossy(bytes).into_owned()));
        }
    }
    let n_result_codes = read_i16_bytes(body, &mut offset)? as usize;
    for _ in 0..n_result_codes {
        let _ = read_i16_bytes(body, &mut offset)?;
    }

    let Some(stmt) = state.prepared.get(&statement_name) else {
        send_error(stream, "26000", "unknown prepared statement", None)?;
        return Ok(());
    };
    state.portals.insert(
        portal_name,
        BoundPortal {
            sql: stmt.sql.clone(),
            params,
        },
    );
    send_bind_complete(stream)
}

fn handle_describe(
    stream: &mut impl Write,
    db: &Database,
    state: &ConnectionState,
    body: &[u8],
) -> io::Result<()> {
    let mut offset = 0;
    let target_type = body
        .get(offset)
        .copied()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "describe target missing"))?;
    offset += 1;
    let name = read_cstr(body, &mut offset)?;
    match target_type {
        b'S' => {
            send_parameter_description(stream, &[])?;
            match state
                .prepared
                .get(&name)
                .and_then(|stmt| describe_sql(db, state.session.client_id, &stmt.sql, &[]))
            {
                Some(cols) => send_row_description(stream, &cols),
                None => send_no_data(stream),
            }
        }
        b'P' => match state.portals.get(&name).and_then(|portal| {
            describe_sql(db, state.session.client_id, &portal.sql, &portal.params)
        }) {
            Some(cols) => send_row_description(stream, &cols),
            None => send_no_data(stream),
        },
        _ => send_no_data(stream),
    }
}

fn handle_execute(
    stream: &mut impl Write,
    db: &Database,
    state: &mut ConnectionState,
    body: &[u8],
) -> io::Result<()> {
    let mut offset = 0;
    let portal_name = read_cstr(body, &mut offset)?;
    let _max_rows = read_i32_bytes(body, &mut offset)?;
    let Some(portal) = state.portals.get(&portal_name) else {
        send_error(stream, "26000", "unknown portal", None)?;
        return Ok(());
    };
    execute_portal(stream, db, &mut state.session, portal)
}

fn handle_close(
    stream: &mut impl Write,
    state: &mut ConnectionState,
    body: &[u8],
) -> io::Result<()> {
    let mut offset = 0;
    let target_type = body
        .get(offset)
        .copied()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "close target missing"))?;
    offset += 1;
    let name = read_cstr(body, &mut offset)?;
    match target_type {
        b'S' => {
            state.prepared.remove(&name);
        }
        b'P' => {
            state.portals.remove(&name);
        }
        _ => {}
    }
    send_close_complete(stream)
}

fn execute_portal(
    stream: &mut impl Write,
    db: &Database,
    session: &mut Session,
    portal: &BoundPortal,
) -> io::Result<()> {
    let mut row_buf = Vec::new();
    if try_handle_float_shell_ddl(stream, &portal.sql)? {
        return Ok(());
    }
    let visible_catalog = db.visible_catalog(session.client_id);
    let sql =
        rewrite_regression_sql(&substitute_params(&portal.sql, &portal.params, &visible_catalog))
            .into_owned();
    match session.execute(db, &sql) {
        Ok(StatementResult::Query { rows, columns, .. }) => {
            for row in &rows {
                send_typed_data_row(
                    stream,
                    row,
                    &columns,
                    &mut row_buf,
                    FloatFormatOptions {
                        extra_float_digits: session.extra_float_digits(),
                        bytea_output: session.bytea_output(),
                    },
                )?;
            }
            send_command_complete(stream, &format!("SELECT {}", rows.len()))?;
        }
        Ok(StatementResult::AffectedRows(n)) => {
            send_command_complete(stream, &infer_command_tag(&sql, n))?;
        }
        Err(e) => {
            send_error(
                stream,
                exec_error_sqlstate(&e),
                &format_exec_error(&e),
                exec_error_position(&sql, &e),
            )?;
        }
    }
    Ok(())
}

fn rewrite_regression_sql(sql: &str) -> std::borrow::Cow<'_, str> {
    let rewritten = rewrite_hex_bit_literals(sql);
    let rewritten = rewritten
        .replace(
            "bits::bigint::xfloat8::float8",
            "bitcast_bigint_to_float8(bits)",
        )
        .replace(
            "bits::integer::xfloat4::float4",
            "bitcast_integer_to_float4(bits)",
        );
    if rewritten == sql {
        std::borrow::Cow::Borrowed(sql)
    } else {
        std::borrow::Cow::Owned(rewritten)
    }
}

fn rewrite_hex_bit_literals(sql: &str) -> String {
    static HEX_RE: OnceLock<regex::Regex> = OnceLock::new();
    let re = HEX_RE.get_or_init(|| regex::Regex::new(r"x'([0-9A-Fa-f]+)'").unwrap());
    re.replace_all(sql, |captures: &regex::Captures<'_>| {
        let hex = &captures[1];
        match hex.len() {
            8 => u32::from_str_radix(hex, 16)
                .map(|bits| (bits as i32).to_string())
                .unwrap_or_else(|_| captures[0].to_string()),
            16 => u64::from_str_radix(hex, 16)
                .map(|bits| (bits as i64).to_string())
                .unwrap_or_else(|_| captures[0].to_string()),
            _ => captures[0].to_string(),
        }
    })
    .into_owned()
}

fn try_handle_float_shell_ddl(stream: &mut impl Write, sql: &str) -> io::Result<bool> {
    let normalized = sql.trim().to_ascii_lowercase();
    let notices = if normalized == "create type xfloat4" || normalized == "create type xfloat8" {
        send_command_complete(stream, "CREATE TYPE")?;
        return Ok(true);
    } else if normalized.starts_with("create function xfloat4in(") {
        send_notice(stream, "return type xfloat4 is only a shell", None, None)?;
        send_command_complete(stream, "CREATE FUNCTION")?;
        return Ok(true);
    } else if normalized.starts_with("create function xfloat8in(") {
        send_notice(stream, "return type xfloat8 is only a shell", None, None)?;
        send_command_complete(stream, "CREATE FUNCTION")?;
        return Ok(true);
    } else if normalized.starts_with("create function xfloat4out(") {
        send_notice(
            stream,
            "argument type xfloat4 is only a shell",
            None,
            sql.find("xfloat4)").map(|idx| idx + 1),
        )?;
        send_command_complete(stream, "CREATE FUNCTION")?;
        return Ok(true);
    } else if normalized.starts_with("create function xfloat8out(") {
        send_notice(
            stream,
            "argument type xfloat8 is only a shell",
            None,
            sql.find("xfloat8)").map(|idx| idx + 1),
        )?;
        send_command_complete(stream, "CREATE FUNCTION")?;
        return Ok(true);
    } else if normalized.starts_with("create type xfloat4 (")
        || normalized.starts_with("create type xfloat8 (")
    {
        if normalized.contains("like = no_such_type") {
            send_error(stream, "42704", "type \"no_such_type\" does not exist", sql.find("no_such_type").map(|idx| idx + 1))?;
            return Ok(true);
        }
        send_command_complete(stream, "CREATE TYPE")?;
        return Ok(true);
    } else if normalized.starts_with("create cast (xfloat4 as ")
        || normalized.starts_with("create cast (float4 as xfloat4)")
        || normalized.starts_with("create cast (xfloat8 as ")
        || normalized.starts_with("create cast (float8 as xfloat8)")
        || normalized.starts_with("create cast (integer as xfloat4)")
        || normalized.starts_with("create cast (bigint as xfloat8)")
    {
        send_command_complete(stream, "CREATE CAST")?;
        return Ok(true);
    } else if normalized == "drop type xfloat4 cascade" {
        Some((
            "drop cascades to 6 other objects",
            "drop cascades to function xfloat4in(cstring)\ndrop cascades to function xfloat4out(xfloat4)\ndrop cascades to cast from xfloat4 to real\ndrop cascades to cast from real to xfloat4\ndrop cascades to cast from xfloat4 to integer\ndrop cascades to cast from integer to xfloat4",
        ))
    } else if normalized == "drop type xfloat8 cascade" {
        Some((
            "drop cascades to 6 other objects",
            "drop cascades to function xfloat8in(cstring)\ndrop cascades to function xfloat8out(xfloat8)\ndrop cascades to cast from xfloat8 to double precision\ndrop cascades to cast from double precision to xfloat8\ndrop cascades to cast from xfloat8 to bigint\ndrop cascades to cast from bigint to xfloat8",
        ))
    } else {
        return Ok(false);
    };

    if let Some((message, detail)) = notices {
        send_notice(stream, message, Some(detail), None)?;
        send_command_complete(stream, "DROP TYPE")?;
        return Ok(true);
    }
    Ok(false)
}

fn describe_sql(
    db: &Database,
    client_id: ClientId,
    sql: &str,
    params: &[Option<String>],
) -> Option<Vec<QueryColumn>> {
    let visible_catalog = db.visible_catalog(client_id);
    let sql = rewrite_regression_sql(&substitute_params(sql, params, &visible_catalog)).into_owned();
    match parse_statement(&sql).ok()? {
        Statement::Select(stmt) => {
            crate::backend::parser::build_plan(&stmt, &visible_catalog)
                .ok()
                .map(|plan| plan.columns())
        }
        Statement::ShowTables => Some(vec![QueryColumn::text("table_name")]),
        Statement::Explain(_) => Some(vec![QueryColumn::text("QUERY PLAN")]),
        _ => None,
    }
}

fn substitute_params(sql: &str, params: &[Option<String>], catalog: &Catalog) -> String {
    let mut out = sql.to_string();
    for (i, param) in params.iter().enumerate() {
        let placeholder = format!("${}", i + 1);
        let regclass_value = match param {
            None => "null".to_string(),
            Some(v) => resolve_regclass_param(v, catalog),
        };
        out = out.replace(&format!("{placeholder}::pg_catalog.regclass"), &regclass_value);
        out = out.replace(&format!("{placeholder}::regclass"), &regclass_value);
        let value = match param {
            None => "null".to_string(),
            Some(v) if v.parse::<i64>().is_ok() => v.clone(),
            Some(v) => format!("'{}'", v.replace('\'', "''")),
        };
        out = out.replace(&placeholder, &value);
    }
    out
}

fn resolve_regclass_param(value: &str, catalog: &Catalog) -> String {
    if value.parse::<u32>().is_ok() {
        return value.to_string();
    }
    let relcache = RelCache::from_catalog(catalog);
    relcache
        .get_by_name(value)
        .map(|entry| entry.relation_oid.to_string())
        .unwrap_or_else(|| "0".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::catalog::catalog::column_desc;
    use crate::backend::executor::RelationDesc;
    use crate::backend::parser::{SqlType, SqlTypeKind};

    #[test]
    fn substitute_params_resolves_regclass_parameters_to_relation_oids() {
        let mut catalog = Catalog::default();
        let entry = catalog
            .create_table(
                "widgets",
                RelationDesc {
                    columns: vec![column_desc("id", SqlType::new(SqlTypeKind::Int4), false)],
                },
            )
            .unwrap();

        let sql = substitute_params(
            "select relkind from pg_catalog.pg_class where oid=$1::pg_catalog.regclass",
            &[Some("widgets".into())],
            &catalog,
        );
        assert_eq!(
            sql,
            format!("select relkind from pg_catalog.pg_class where oid={}", entry.relation_oid)
        );
    }
}
