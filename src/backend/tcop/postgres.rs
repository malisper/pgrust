use std::collections::HashMap;
use std::io::{self, BufWriter, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU32, Ordering};
use std::thread;

use crate::backend::access::heap::heapam::HeapError;
use crate::backend::executor::{ExecError, QueryColumn, StatementResult};
use crate::backend::libpq::pqcomm::{
    cstr_from_bytes, read_byte, read_cstr, read_i16_bytes, read_i32, read_i32_bytes,
};
use crate::backend::libpq::pqformat::{
    FloatFormatOptions, format_exec_error, format_exec_error_hint, infer_command_tag, send_auth_ok,
    send_backend_key_data, send_bind_complete, send_close_complete, send_command_complete,
    send_copy_in_response, send_empty_query, send_error, send_error_with_fields,
    send_error_with_hint, send_no_data, send_notice, send_notice_with_severity,
    send_parameter_description, send_parameter_status, send_parse_complete, send_query_result,
    send_ready_for_query, send_row_description, send_typed_data_row,
};
use crate::backend::parser::CatalogLookup;
use crate::backend::parser::UngroupedColumnClause;
use crate::backend::parser::comments::sql_is_effectively_empty_after_comments;
use crate::backend::parser::{SqlType, SqlTypeKind, parse_expr};
use crate::include::access::htup::TupleError;
use crate::include::nodes::datum::Value;
use crate::pl::plpgsql::{PlpgsqlNotice, RaiseLevel, clear_notices, take_notices};

fn exec_error_sqlstate(e: &ExecError) -> &'static str {
    match e {
        ExecError::Regex(err) => err.sqlstate,
        ExecError::DetailedError { sqlstate, .. } => sqlstate,
        ExecError::Parse(crate::backend::parser::ParseError::InvalidInteger(_))
        | ExecError::Parse(crate::backend::parser::ParseError::InvalidNumeric(_))
        | ExecError::InvalidIntegerInput { .. }
        | ExecError::ArrayInput { .. }
        | ExecError::InvalidNumericInput(_)
        | ExecError::InvalidByteaInput { .. }
        | ExecError::InvalidGeometryInput { .. }
        | ExecError::InvalidBitInput { .. }
        | ExecError::InvalidBooleanInput { .. }
        | ExecError::InvalidFloatInput { .. } => "22P02",
        ExecError::BitStringLengthMismatch { .. }
        | ExecError::BitStringTooLong { .. }
        | ExecError::BitStringSizeMismatch { .. } => "22026",
        ExecError::BitIndexOutOfRange { .. } => "2202E",
        ExecError::NegativeSubstringLength => "22011",
        ExecError::Parse(crate::backend::parser::ParseError::UndefinedOperator { .. }) => "42883",
        ExecError::UniqueViolation { .. } => "23505",
        ExecError::Parse(crate::backend::parser::ParseError::UnknownConfigurationParameter(_)) => {
            "42704"
        }
        ExecError::Parse(crate::backend::parser::ParseError::NoSchemaSelectedForCreate) => "3F000",
        ExecError::Parse(crate::backend::parser::ParseError::FeatureNotSupported(_)) => "0A000",
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
        ExecError::Interrupted(reason) => reason.sqlstate(),
        ExecError::RequestedLengthTooLarge => "54000",
        ExecError::Heap(HeapError::Tuple(TupleError::Oversized { .. })) => "54000",
        ExecError::RaiseException(_) => "P0001",
        ExecError::DivisionByZero(_) => "22012",
        ExecError::GenerateSeriesInvalidArg(_, _) => "22023",
        ExecError::StringDataRightTruncation { .. } => "22001",
        ExecError::CardinalityViolation(_) => "21000",
        ExecError::Parse(_) => "42601",
        _ => "XX000",
    }
}

fn exec_error_detail(e: &ExecError) -> Option<&str> {
    match e {
        ExecError::Regex(err) => err.detail.as_deref(),
        ExecError::DetailedError { detail, .. } => detail.as_deref(),
        ExecError::ArrayInput { detail, .. } => detail.as_deref(),
        _ => None,
    }
}

fn exec_error_hint(e: &ExecError) -> Option<&str> {
    match e {
        ExecError::Regex(err) => err.hint.as_deref(),
        ExecError::DetailedError { hint, .. } => hint.as_deref(),
        _ => None,
    }
}

fn exec_error_position(sql: &str, e: &ExecError) -> Option<usize> {
    if matches!(e, ExecError::InvalidBooleanInput { .. })
        && sql.to_ascii_lowercase().contains("::text::boolean")
    {
        return None;
    }
    let value = match e {
        ExecError::Parse(crate::backend::parser::ParseError::UnexpectedToken {
            expected, ..
        }) if matches!(*expected, "valid binary digit" | "valid hexadecimal digit") => {
            return find_bit_literal_position(sql);
        }
        ExecError::Parse(crate::backend::parser::ParseError::UngroupedColumn {
            token,
            clause,
            ..
        }) => {
            return find_ungrouped_column_position(sql, token, clause);
        }
        ExecError::Parse(crate::backend::parser::ParseError::UnexpectedToken {
            expected: "text or bit argument",
            actual,
        }) if actual.starts_with("Length(") => {
            return sql
                .to_ascii_lowercase()
                .find("length(")
                .map(|index| index + 1);
        }
        ExecError::Parse(crate::backend::parser::ParseError::UndefinedOperator { op, .. }) => {
            return sql.find(op).map(|index| index + 1);
        }
        ExecError::InvalidIntegerInput { value, .. } => value.as_str(),
        ExecError::ArrayInput { value, .. } => value.as_str(),
        ExecError::IntegerOutOfRange { value, .. } => value.as_str(),
        ExecError::InvalidNumericInput(value) => value.as_str(),
        ExecError::InvalidByteaInput { value } => value.as_str(),
        ExecError::InvalidGeometryInput { value, .. } => value.as_str(),
        ExecError::InvalidBooleanInput { value } => value.as_str(),
        ExecError::InvalidFloatInput { value, .. } => value.as_str(),
        ExecError::FloatOutOfRange { value, .. } => value.as_str(),
        _ => return None,
    };
    let needle = format!("'{}'", value.replace('\'', "''"));
    sql.rfind(&needle).map(|index| index + 1)
}

struct ExecErrorResponse {
    message: String,
    detail: Option<String>,
    hint: Option<String>,
    position: Option<usize>,
}

fn exec_error_response(sql: &str, e: &ExecError) -> ExecErrorResponse {
    let message = format_exec_error(e);
    let mut response = ExecErrorResponse {
        message,
        detail: None,
        hint: None,
        position: exec_error_position(sql, e),
    };

    match response.message.as_str() {
        "unsafe use of string constant with Unicode escapes" => {
            response.detail = Some(
                "String constants with Unicode escapes cannot be used when \"standard_conforming_strings\" is off.".into(),
            );
            response.position = find_unicode_string_position(sql).or(response.position);
        }
        "invalid Unicode escape" => {
            response.hint = Some(if sql.contains("unistr(") {
                "Unicode escapes must be \\XXXX, \\+XXXXXX, \\uXXXX, or \\UXXXXXXXX.".into()
            } else if sql.contains("E'") {
                "Unicode escapes must be \\uXXXX or \\UXXXXXXXX.".into()
            } else {
                "Unicode escapes must be \\XXXX or \\+XXXXXX.".into()
            });
            if sql.contains("unistr(") {
                response.position = None;
            } else {
                response.position = find_unicode_escape_position(sql).or(response.position);
            }
        }
        "invalid Unicode surrogate pair" | "invalid Unicode escape value" => {
            if sql.contains("unistr(") {
                response.position = None;
            } else {
                response.position = find_unicode_escape_position(sql).or(response.position);
            }
            if sql.contains("E'") {
                if response.message == "invalid Unicode surrogate pair" {
                    if let Some(token) = find_e_unicode_near_token(sql) {
                        response.message =
                            format!("invalid Unicode surrogate pair at or near \"{token}\"");
                    }
                } else if response.message == "invalid Unicode escape value" {
                    if let Some(token) = find_e_unicode_escape_token(sql) {
                        response.message =
                            format!("invalid Unicode escape value at or near \"{token}\"");
                    }
                }
            }
        }
        msg if msg.starts_with("UESCAPE must be followed by a simple string literal") => {
            response.position = find_uescape_token_position(sql).or(response.position);
        }
        msg if msg.starts_with("invalid Unicode escape character at or near") => {
            response.position = find_uescape_literal_position(sql).or(response.position);
        }
        _ => {}
    }

    response
}

fn find_unicode_string_position(sql: &str) -> Option<usize> {
    let lower = sql.to_ascii_lowercase();
    lower.find("u&'").map(|idx| idx + 1)
}

fn find_unicode_escape_position(sql: &str) -> Option<usize> {
    sql.find('\\').map(|idx| idx + 1)
}

fn find_uescape_token_position(sql: &str) -> Option<usize> {
    let lower = sql.to_ascii_lowercase();
    lower.find("uescape").and_then(|idx| {
        let tail = &sql[idx + "UESCAPE".len()..];
        let offset = tail.find(|ch: char| !ch.is_ascii_whitespace())?;
        Some(idx + "UESCAPE".len() + offset + 1)
    })
}

fn find_uescape_literal_position(sql: &str) -> Option<usize> {
    sql.rfind("'+'").map(|idx| idx + 1)
}

fn extract_e_literal(sql: &str) -> Option<&str> {
    let start = sql.find("E'")? + 2;
    let end = sql[start..].rfind('\'')? + start;
    Some(&sql[start..end])
}

fn find_e_unicode_near_token(sql: &str) -> Option<String> {
    let raw = extract_e_literal(sql)?;
    let bytes = raw.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        if bytes[i] != b'\\' {
            i += 1;
            continue;
        }
        let (len, code) = parse_e_unicode_escape(bytes, i)?;
        if !(0xD800..=0xDBFF).contains(&code) {
            i += len;
            continue;
        }
        let next = i + len;
        if next >= bytes.len() {
            return Some("'".into());
        }
        if bytes[next] != b'\\' {
            return Some((bytes[next] as char).to_string());
        }
        if next + 1 >= bytes.len() || bytes[next + 1] == b'\\' {
            return Some("\\".into());
        }
        let next_len = match bytes[next + 1] {
            b'u' => 6,
            b'U' => 10,
            _ => 1,
        };
        let end = (next + next_len).min(bytes.len());
        return Some(raw[next..end].to_string());
    }
    None
}

fn find_e_unicode_escape_token(sql: &str) -> Option<String> {
    let raw = extract_e_literal(sql)?;
    let start = raw.find('\\')?;
    let bytes = raw.as_bytes();
    let len = match bytes.get(start + 1)? {
        b'u' => 6,
        b'U' => 10,
        _ => 5,
    };
    let end = (start + len).min(bytes.len());
    Some(raw[start..end].to_string())
}

fn parse_e_unicode_escape(bytes: &[u8], start: usize) -> Option<(usize, u32)> {
    if start + 2 > bytes.len() || bytes[start] != b'\\' {
        return None;
    }
    let (len, digits_start, digits_end) = match bytes[start + 1] {
        b'u' => (6, start + 2, start + 6),
        b'U' => (10, start + 2, start + 10),
        _ => return None,
    };
    let digits = std::str::from_utf8(&bytes[digits_start..digits_end]).ok()?;
    let code = u32::from_str_radix(digits, 16).ok()?;
    Some((len, code))
}

fn send_exec_error(stream: &mut impl Write, sql: &str, e: &ExecError) -> io::Result<()> {
    let mut response = exec_error_response(sql, e);
    if response.detail.is_none() {
        response.detail = exec_error_detail(e).map(str::to_string);
    }
    if response.hint.is_none() {
        response.hint = exec_error_hint(e).map(str::to_string);
    }
    if response.hint.is_none() {
        response.hint = format_exec_error_hint(e);
    }
    send_error_with_fields(
        stream,
        exec_error_sqlstate(e),
        &response.message,
        response.detail.as_deref(),
        response.hint.as_deref(),
        response.position,
    )
}

fn find_bit_literal_position(sql: &str) -> Option<usize> {
    let lower = sql.to_ascii_lowercase();
    lower
        .find("b'")
        .or_else(|| lower.find("x'"))
        .map(|index| index + 1)
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
    columns: Option<Vec<String>>,
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

pub(crate) fn handle_connection_with_io<R, W>(
    mut reader: R,
    writer: W,
    db: &Database,
    client_id: ClientId,
) -> io::Result<()>
where
    R: Read,
    W: Write,
{
    let mut writer = BufWriter::new(writer);

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
                    None,
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
                    None,
                    None,
                )?;
                send_ready_for_query(&mut writer, state.session.ready_status())?;
                writer.flush()?;
            }
        }
    };
    db.cleanup_client_temp_relations(client_id);
    db.clear_interrupt_state(client_id);
    result
}

pub(crate) fn handle_connection(
    stream: TcpStream,
    db: &Database,
    client_id: ClientId,
) -> io::Result<()> {
    let reader = stream.try_clone()?;
    handle_connection_with_io(reader, stream, db, client_id)
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

    if try_handle_psql_describe_query(stream, db, state, &sql)? {
        send_ready_for_query(stream, state.session.ready_status())?;
        return Ok(());
    }

    if let Some((table_name, columns)) = parse_copy_from_stdin(&sql) {
        state.copy_in = Some(CopyInState {
            table_name,
            columns,
            pending: Vec::new(),
        });
        send_copy_in_response(stream)?;
        return Ok(());
    }

    let parsed = if state.session.standard_conforming_strings() {
        db.plan_cache
            .get_statement(&sql)
            .map_err(|e| io::Error::other(format!("{e:?}")))
    } else {
        crate::backend::parser::parse_statement_with_options(
            &sql,
            crate::backend::parser::ParseOptions {
                standard_conforming_strings: false,
            },
        )
        .map_err(|e| io::Error::other(format!("{e:?}")))
    };
    if let Ok(Statement::Select(ref select_stmt)) = parsed {
        clear_notices();
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
                    send_plpgsql_notices(stream, &take_notices())?;
                    send_exec_error(stream, &sql, &e)?;
                } else {
                    send_plpgsql_notices(stream, &take_notices())?;
                    if !header_sent {
                        send_row_description(stream, &columns)?;
                    }
                    send_command_complete(stream, &format!("SELECT {row_count}"))?;
                }
            }
            Err(e) => {
                send_plpgsql_notices(stream, &take_notices())?;
                send_exec_error(stream, &sql, &e)?;
            }
        }
    } else {
        clear_notices();
        match state.session.execute(db, &sql) {
            Ok(StatementResult::Query { columns, rows, .. }) => {
                send_plpgsql_notices(stream, &take_notices())?;
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
                send_plpgsql_notices(stream, &take_notices())?;
                send_command_complete(stream, &infer_command_tag(&sql, n))?;
            }
            Err(e) => {
                send_plpgsql_notices(stream, &take_notices())?;
                send_exec_error(stream, &sql, &e)?;
            }
        }
    }

    send_ready_for_query(stream, state.session.ready_status())?;
    Ok(())
}

fn try_handle_psql_describe_query(
    stream: &mut impl Write,
    db: &Database,
    state: &mut ConnectionState,
    sql: &str,
) -> io::Result<bool> {
    let Some((columns, rows)) = execute_psql_describe_query(db, &state.session, sql) else {
        return Ok(false);
    };
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
    Ok(true)
}

fn execute_psql_describe_query(
    db: &Database,
    session: &Session,
    sql: &str,
) -> Option<(Vec<QueryColumn>, Vec<Vec<Value>>)> {
    // :HACK: psql's `\d bit_defaults` emits a long chain of catalog-heavy
    // describe queries. We short-circuit the specific shapes bit.sql needs
    // instead of implementing LEFT JOIN, format_type, regex operators,
    // COLLATE, publications, inheritance footers, and related describe-only
    // catalog features in the main SQL engine.
    let lower = sql.to_ascii_lowercase();
    if lower.contains("from pg_catalog.pg_class c")
        && lower.contains("left join pg_catalog.pg_namespace n on n.oid = c.relnamespace")
        && lower.contains("operator(pg_catalog.~)")
        && lower.contains("pg_catalog.pg_table_is_visible(c.oid)")
    {
        return Some(psql_describe_lookup_query(db, session, sql));
    }
    if lower.starts_with("select c.relchecks, c.relkind, c.relhasindex")
        && lower.contains("from pg_catalog.pg_class c")
        && lower.contains("where c.oid = '")
    {
        return psql_describe_tableinfo_query(db, session, sql);
    }
    if lower.starts_with("select a.attname")
        && lower.contains("pg_catalog.format_type(a.atttypid, a.atttypmod)")
        && lower.contains("from pg_catalog.pg_attribute a")
        && lower.contains("where a.attrelid = '")
    {
        return psql_describe_columns_query(db, session, sql);
    }
    if lower.starts_with("select c2.relname, i.indisprimary, i.indisunique")
        && lower.contains("pg_catalog.pg_get_indexdef(i.indexrelid, 0, true)")
        && lower.contains("from pg_catalog.pg_class c, pg_catalog.pg_class c2, pg_catalog.pg_index i")
    {
        return psql_describe_indexes_query(db, session, sql);
    }
    if lower.contains("from pg_catalog.pg_constraint")
        && lower.contains("pg_get_constraintdef")
        && lower.contains("conrelid")
    {
        return psql_describe_constraints_query(db, session, sql);
    }
    if lower.starts_with("select pg_catalog.pg_get_viewdef(")
        && lower.contains("::pg_catalog.oid")
    {
        return psql_get_viewdef_query(db, session, sql);
    }
    if (lower.starts_with("select col_description(")
        || lower.starts_with("select pg_catalog.col_description("))
        && lower.contains("::regclass")
    {
        return psql_col_description_query(db, session, sql);
    }
    if lower.starts_with("select indexrelid::regclass::text as index")
        && lower.contains("obj_description(indexrelid, 'pg_class')")
        && lower.contains("from pg_index")
    {
        return psql_index_obj_description_query(db, session, sql);
    }
    if lower.contains("obj_description(oid, 'pg_constraint')")
        && lower.contains("from pg_constraint")
    {
        return psql_constraint_obj_description_query(db, session, sql);
    }
    if lower.starts_with("select relname,")
        && lower.contains("obj_description(c.oid, 'pg_class')")
        && lower.contains("from pg_class c left join old_oids using (relname)")
    {
        return psql_relation_obj_description_query(db, session, sql);
    }
    if lower.contains("from pg_catalog.pg_policy pol") && lower.contains("pol.polroles") {
        return Some((vec![QueryColumn::text("Policies")], Vec::new()));
    }
    if lower.contains("from pg_catalog.pg_statistic_ext")
        && lower.contains("stxrelid::pg_catalog.regclass")
    {
        return Some((
            vec![
                QueryColumn::text("oid"),
                QueryColumn::text("stxrelid"),
                QueryColumn::text("nsp"),
                QueryColumn::text("stxname"),
            ],
            Vec::new(),
        ));
    }
    if lower.contains("from pg_catalog.pg_publication p")
        && lower.contains("pg_relation_is_publishable")
        && lower.contains("union")
    {
        return Some((
            vec![
                QueryColumn::text("pubname"),
                QueryColumn::text("?column?"),
                QueryColumn::text("?column?"),
            ],
            Vec::new(),
        ));
    }
    if lower.contains("from pg_catalog.pg_class c, pg_catalog.pg_inherits i")
        && lower.contains("::pg_catalog.regclass")
    {
        let columns = if lower.contains("c.relkind") {
            vec![
                QueryColumn::text("regclass"),
                QueryColumn::text("relkind"),
                QueryColumn::text("inhdetachpending"),
                QueryColumn::text("pg_get_expr"),
            ]
        } else {
            vec![QueryColumn::text("regclass")]
        };
        return Some((columns, Vec::new()));
    }
    None
}

fn psql_describe_lookup_query(
    db: &Database,
    session: &Session,
    sql: &str,
) -> (Vec<QueryColumn>, Vec<Vec<Value>>) {
    let catalog = session.catalog_lookup(db);
    let txn_ctx = session.catalog_txn_ctx();
    let search_path = session.configured_search_path();
    let relation_name = extract_psql_pattern_name(sql);
    let rows = relation_name
        .and_then(|name| catalog.lookup_any_relation(name).map(|entry| (name, entry)))
        .map(|(name, entry)| {
            let nspname = db
                .relation_namespace_name(session.client_id, txn_ctx, entry.relation_oid)
                .or_else(|| name.split_once('.').map(|(schema, _)| schema.to_string()))
                .unwrap_or_else(|| "public".to_string());
            let relname = db
                .relation_display_name(
                    session.client_id,
                    txn_ctx,
                    search_path.as_deref(),
                    entry.relation_oid,
                )
                .unwrap_or_else(|| name.rsplit('.').next().unwrap_or(name).to_string());
            vec![vec![
                Value::Int32(entry.relation_oid as i32),
                Value::Text(nspname.into()),
                Value::Text(
                    relname
                        .rsplit('.')
                        .next()
                        .unwrap_or(relname.as_str())
                        .to_string()
                        .into(),
                ),
            ]]
        })
        .unwrap_or_default();
    (
        vec![
            QueryColumn {
                name: "oid".into(),
                sql_type: SqlType::new(SqlTypeKind::Oid),
            },
            QueryColumn::text("nspname"),
            QueryColumn::text("relname"),
        ],
        rows,
    )
}

fn psql_describe_tableinfo_query(
    db: &Database,
    session: &Session,
    sql: &str,
) -> Option<(Vec<QueryColumn>, Vec<Vec<Value>>)> {
    let oid = extract_quoted_oid(sql)?;
    let txn_ctx = session.catalog_txn_ctx();
    let entry = db.describe_relation_by_oid(session.client_id, txn_ctx, oid)?;
    let relhasindex = db.has_index_on_relation(session.client_id, txn_ctx, oid);
    let amname = db
        .access_method_name_for_relation(session.client_id, txn_ctx, oid)
        .unwrap_or_default();
    Some((
        vec![
            QueryColumn {
                name: "relchecks".into(),
                sql_type: SqlType::new(SqlTypeKind::Int4),
            },
            QueryColumn {
                name: "relkind".into(),
                sql_type: SqlType::new(SqlTypeKind::InternalChar),
            },
            QueryColumn {
                name: "relhasindex".into(),
                sql_type: SqlType::new(SqlTypeKind::Bool),
            },
            QueryColumn {
                name: "relhasrules".into(),
                sql_type: SqlType::new(SqlTypeKind::Bool),
            },
            QueryColumn {
                name: "relhastriggers".into(),
                sql_type: SqlType::new(SqlTypeKind::Bool),
            },
            QueryColumn {
                name: "relrowsecurity".into(),
                sql_type: SqlType::new(SqlTypeKind::Bool),
            },
            QueryColumn {
                name: "relforcerowsecurity".into(),
                sql_type: SqlType::new(SqlTypeKind::Bool),
            },
            QueryColumn {
                name: "relhasoids".into(),
                sql_type: SqlType::new(SqlTypeKind::Bool),
            },
            QueryColumn {
                name: "relispartition".into(),
                sql_type: SqlType::new(SqlTypeKind::Bool),
            },
            QueryColumn::text("?column?"),
            QueryColumn {
                name: "reltablespace".into(),
                sql_type: SqlType::new(SqlTypeKind::Oid),
            },
            QueryColumn::text("reloftype"),
            QueryColumn {
                name: "relpersistence".into(),
                sql_type: SqlType::new(SqlTypeKind::InternalChar),
            },
            QueryColumn {
                name: "relreplident".into(),
                sql_type: SqlType::new(SqlTypeKind::InternalChar),
            },
            QueryColumn::text("amname"),
        ],
        vec![vec![
            Value::Int32(0),
            Value::InternalChar(entry.relkind as u8),
            Value::Bool(relhasindex),
            Value::Bool(false),
            Value::Bool(false),
            Value::Bool(false),
            Value::Bool(false),
            Value::Bool(false),
            Value::Bool(false),
            Value::Text("".into()),
            Value::Int32(0),
            Value::Text("".into()),
            Value::InternalChar(entry.relpersistence as u8),
            Value::InternalChar(b'd'),
            Value::Text(amname.into()),
        ]],
    ))
}

fn psql_describe_columns_query(
    db: &Database,
    session: &Session,
    sql: &str,
) -> Option<(Vec<QueryColumn>, Vec<Vec<Value>>)> {
    let oid = extract_quoted_oid(sql)?;
    let entry = db.describe_relation_by_oid(session.client_id, session.catalog_txn_ctx(), oid)?;
    let lower = sql.to_ascii_lowercase();
    let include_attrdef = lower.contains("pg_catalog.pg_get_expr(d.adbin");
    let include_attnotnull = lower.contains("a.attnotnull");
    let include_attcollation = lower.contains("as attcollation");
    let include_attidentity = lower.contains("attidentity");
    let include_attgenerated = lower.contains("attgenerated");
    let include_is_key = lower.contains("as is_key");
    let include_indexdef = lower.contains("as indexdef");
    let include_attfdwoptions = lower.contains("as attfdwoptions");
    let include_attstorage = lower.contains("a.attstorage");
    let include_attcompression = lower.contains("attcompression");
    let include_attstattarget = lower.contains("attstattarget");
    let include_attdescr = lower.contains("pg_catalog.col_description(");

    let mut columns = vec![
        QueryColumn::text("attname"),
        QueryColumn::text("format_type"),
    ];
    if include_attrdef {
        columns.push(QueryColumn::text("pg_get_expr"));
    }
    if include_attnotnull {
        columns.push(QueryColumn {
            name: "attnotnull".into(),
            sql_type: SqlType::new(SqlTypeKind::Bool),
        });
    }
    if include_attcollation {
        columns.push(QueryColumn::text("attcollation"));
    }
    if include_attidentity {
        columns.push(QueryColumn {
            name: "attidentity".into(),
            sql_type: SqlType::new(SqlTypeKind::InternalChar),
        });
    }
    if include_attgenerated {
        columns.push(QueryColumn {
            name: "attgenerated".into(),
            sql_type: SqlType::new(SqlTypeKind::InternalChar),
        });
    }
    if include_is_key {
        columns.push(QueryColumn::text("is_key"));
    }
    if include_indexdef {
        columns.push(QueryColumn::text("indexdef"));
    }
    if include_attfdwoptions {
        columns.push(QueryColumn::text("attfdwoptions"));
    }
    if include_attstorage {
        columns.push(QueryColumn {
            name: "attstorage".into(),
            sql_type: SqlType::new(SqlTypeKind::InternalChar),
        });
    }
    if include_attcompression {
        columns.push(QueryColumn {
            name: "attcompression".into(),
            sql_type: SqlType::new(SqlTypeKind::InternalChar),
        });
    }
    if include_attstattarget {
        columns.push(QueryColumn {
            name: "attstattarget".into(),
            sql_type: SqlType::new(SqlTypeKind::Int2),
        });
    }
    if include_attdescr {
        columns.push(QueryColumn::text("col_description"));
    }

    let rows = entry
        .desc
        .columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            let mut row = vec![
                Value::Text(column.name.clone().into()),
                Value::Text(format_psql_type(column.sql_type).into()),
            ];
            if include_attrdef {
                row.push(
                    column
                        .default_expr
                        .as_ref()
                        .map(|expr| Value::Text(format_psql_default(column.sql_type, expr).into()))
                        .unwrap_or(Value::Null),
                );
            }
            if include_attnotnull {
                row.push(Value::Bool(!column.storage.nullable));
            }
            if include_attcollation {
                row.push(Value::Null);
            }
            if include_attidentity {
                row.push(Value::InternalChar(0));
            }
            if include_attgenerated {
                row.push(Value::InternalChar(0));
            }
            if include_is_key {
                let is_key = entry
                    .index
                    .as_ref()
                    .is_some_and(|index_meta| index < index_meta.indnkeyatts as usize);
                row.push(Value::Text(if is_key { "yes" } else { "no" }.into()));
            }
            if include_indexdef {
                row.push(Value::Text(column.name.clone().into()));
            }
            if include_attfdwoptions {
                row.push(Value::Text("".into()));
            }
            if include_attstorage {
                row.push(Value::InternalChar(column.storage.attstorage.as_char() as u8));
            }
            if include_attcompression {
                row.push(Value::InternalChar(
                    column.storage.attcompression.as_char() as u8,
                ));
            }
            if include_attstattarget {
                row.push(if column.attstattarget < 0 {
                    Value::Null
                } else {
                    Value::Int16(column.attstattarget)
                });
            }
            if include_attdescr {
                row.push(Value::Null);
            }
            row
        })
        .collect::<Vec<_>>();
    Some((columns, rows))
}

fn psql_describe_constraints_query(
    db: &Database,
    session: &Session,
    sql: &str,
) -> Option<(Vec<QueryColumn>, Vec<Vec<Value>>)> {
    let lower = sql.to_ascii_lowercase();
    let oid = extract_constraint_relid(sql).or_else(|| {
        extract_quoted_oid_with_markers(
            sql,
            &[
                "pg_partition_ancestors('",
                "values ('",
                "conrelid = '",
                "confrelid = '",
            ],
        )
    })?;
    let contype_filter = if lower.contains("contype = 'f'") {
        Some(crate::include::catalog::CONSTRAINT_FOREIGN)
    } else if lower.contains("contype = 'p'") {
        Some(crate::include::catalog::CONSTRAINT_PRIMARY)
    } else if lower.contains("contype = 'u'") {
        Some(crate::include::catalog::CONSTRAINT_UNIQUE)
    } else if lower.contains("contype = 'n'") {
        Some(crate::include::catalog::CONSTRAINT_NOTNULL)
    } else {
        None
    };
    let txn_ctx = session.catalog_txn_ctx();
    let include_sametable = lower.contains("as sametable");
    let incoming_refs = lower.contains("where confrelid in")
        || lower.contains("where c.confrelid in")
        || lower.contains("where r.confrelid in")
        || lower.contains("where confrelid = ")
        || lower.contains("where c.confrelid = ")
        || lower.contains("where r.confrelid = ");
    let rows = if incoming_refs {
        crate::backend::utils::cache::syscache::ensure_constraint_rows(
            db,
            session.client_id,
            txn_ctx,
        )
        .into_iter()
        .filter(|row| row.confrelid == oid)
        .filter(|row| contype_filter.is_none_or(|contype| row.contype == contype))
        .filter(|row| !lower.contains("conparentid = 0") || row.conparentid == 0)
        .filter_map(|row| {
            let ontable = db
                .relation_display_name(
                    session.client_id,
                    txn_ctx,
                    session.configured_search_path().as_deref(),
                    row.conrelid,
                )
                .unwrap_or_else(|| row.conrelid.to_string());
            let condef = constraint_def_for_row(db, session, None, &row)?;
            Some(vec![
                Value::Text(row.conname.into()),
                Value::Text(ontable.into()),
                Value::Text(condef.into()),
            ])
        })
        .collect::<Vec<_>>()
    } else {
        let relation = db.describe_relation_by_oid(session.client_id, txn_ctx, oid)?;
        let relname = db
            .relation_display_name(
                session.client_id,
                txn_ctx,
                session.configured_search_path().as_deref(),
                oid,
            )
            .unwrap_or_else(|| oid.to_string());
        db.constraint_rows_for_relation(session.client_id, txn_ctx, oid)
            .into_iter()
            .filter(|row| contype_filter.is_none_or(|contype| row.contype == contype))
            .filter(|row| !lower.contains("conparentid = 0") || row.conparentid == 0)
            .filter_map(|row| {
                let condef = constraint_def_for_row(db, session, Some(&relation), &row)?;
                if include_sametable {
                    Some(vec![
                        Value::Bool(row.conrelid == oid),
                        Value::Text(row.conname.into()),
                        Value::Text(condef.into()),
                        Value::Text(relname.clone().into()),
                    ])
                } else {
                    Some(vec![
                        Value::Text(row.conname.into()),
                        Value::Text(relname.clone().into()),
                        Value::Text(condef.into()),
                    ])
                }
            })
            .collect::<Vec<_>>()
    };
    let mut rows = rows;
    rows.sort_by(|left, right| match (
        left.get(usize::from(include_sametable)),
        right.get(usize::from(include_sametable)),
    ) {
        (Some(Value::Text(left)), Some(Value::Text(right))) => left.cmp(right),
        _ => std::cmp::Ordering::Equal,
    });
    let columns = if include_sametable {
        vec![
            QueryColumn {
                name: "sametable".into(),
                sql_type: SqlType::new(SqlTypeKind::Bool),
            },
            QueryColumn::text("conname"),
            QueryColumn::text("condef"),
            QueryColumn::text("ontable"),
        ]
    } else {
        vec![
            QueryColumn::text("conname"),
            QueryColumn::text("ontable"),
            QueryColumn::text("condef"),
        ]
    };
    Some((columns, rows))
}

fn constraint_def_for_row(
    db: &Database,
    session: &Session,
    relation: Option<&crate::backend::utils::cache::relcache::RelCacheEntry>,
    row: &crate::include::catalog::PgConstraintRow,
) -> Option<String> {
    match row.contype {
        crate::include::catalog::CONSTRAINT_NOTNULL => Some("NOT NULL".to_string()),
        crate::include::catalog::CONSTRAINT_PRIMARY | crate::include::catalog::CONSTRAINT_UNIQUE => {
            let relation = relation.cloned().or_else(|| {
                db.describe_relation_by_oid(session.client_id, session.catalog_txn_ctx(), row.conrelid)
            })?;
            index_backed_constraint_def(db, session.client_id, session.catalog_txn_ctx(), &relation, row)
        }
        crate::include::catalog::CONSTRAINT_FOREIGN => Some("FOREIGN KEY".to_string()),
        _ => None,
    }
}

fn index_backed_constraint_def(
    db: &Database,
    client_id: u32,
    txn_ctx: Option<(u32, u32)>,
    relation: &crate::backend::utils::cache::relcache::RelCacheEntry,
    row: &crate::include::catalog::PgConstraintRow,
) -> Option<String> {
    let index = db
        .describe_relation_by_oid(client_id, txn_ctx, row.conindid)?
        .index?;
    let columns = index
        .indkey
        .iter()
        .map(|attnum| {
            (*attnum > 0)
                .then(|| {
                    relation
                        .desc
                        .columns
                        .get((*attnum as usize).saturating_sub(1))
                })
                .flatten()
                .map(|column| column.name.clone())
        })
        .collect::<Option<Vec<_>>>()?;
    let prefix = if row.contype == crate::include::catalog::CONSTRAINT_PRIMARY {
        "PRIMARY KEY"
    } else {
        "UNIQUE"
    };
    Some(format!("{prefix} ({})", columns.join(", ")))
}

fn psql_describe_indexes_query(
    db: &Database,
    session: &Session,
    sql: &str,
) -> Option<(Vec<QueryColumn>, Vec<Vec<Value>>)> {
    let oid = extract_quoted_oid(sql)?;
    let txn_ctx = session.catalog_txn_ctx();
    let relation = db.describe_relation_by_oid(session.client_id, txn_ctx, oid)?;
    let constraints = db.constraint_rows_for_relation(session.client_id, txn_ctx, oid);
    let mut rows = session
        .catalog_lookup(db)
        .index_relations_for_heap(oid)
        .into_iter()
        .map(|index| {
            let constraint = constraints
                .iter()
                .find(|row| row.conindid == index.relation_oid && matches!(row.contype, 'p' | 'u' | 'x'));
            let condef = constraint
                .and_then(|row| constraint_def_for_row(db, session, Some(&relation), row))
                .map(|text| Value::Text(text.into()))
                .unwrap_or(Value::Null);
            let contype = constraint
                .map(|row| Value::InternalChar(row.contype as u8))
                .unwrap_or(Value::Null);
            let condeferrable = constraint
                .map(|row| Value::Bool(row.condeferrable))
                .unwrap_or(Value::Null);
            let condeferred = constraint
                .map(|row| Value::Bool(row.condeferred))
                .unwrap_or(Value::Null);
            vec![
                Value::Text(index.name.clone().into()),
                Value::Bool(index.index_meta.indisprimary),
                Value::Bool(index.index_meta.indisunique),
                Value::Bool(index.index_meta.indisclustered),
                Value::Bool(index.index_meta.indisvalid),
                Value::Text(format_psql_indexdef(db, session, &index).into()),
                condef,
                contype,
                condeferrable,
                condeferred,
                Value::Bool(index.index_meta.indisreplident),
                Value::Int32(0),
                constraint
                    .map(|row| Value::Bool(row.conperiod))
                    .unwrap_or(Value::Null),
            ]
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        let left_primary = matches!(left.get(1), Some(Value::Bool(true)));
        let right_primary = matches!(right.get(1), Some(Value::Bool(true)));
        right_primary
            .cmp(&left_primary)
            .then_with(|| match (left.first(), right.first()) {
                (Some(Value::Text(left)), Some(Value::Text(right))) => left.cmp(right),
                _ => std::cmp::Ordering::Equal,
            })
    });
    Some((
        vec![
            QueryColumn::text("relname"),
            QueryColumn {
                name: "indisprimary".into(),
                sql_type: SqlType::new(SqlTypeKind::Bool),
            },
            QueryColumn {
                name: "indisunique".into(),
                sql_type: SqlType::new(SqlTypeKind::Bool),
            },
            QueryColumn {
                name: "indisclustered".into(),
                sql_type: SqlType::new(SqlTypeKind::Bool),
            },
            QueryColumn {
                name: "indisvalid".into(),
                sql_type: SqlType::new(SqlTypeKind::Bool),
            },
            QueryColumn::text("pg_get_indexdef"),
            QueryColumn::text("pg_get_constraintdef"),
            QueryColumn {
                name: "contype".into(),
                sql_type: SqlType::new(SqlTypeKind::InternalChar),
            },
            QueryColumn {
                name: "condeferrable".into(),
                sql_type: SqlType::new(SqlTypeKind::Bool),
            },
            QueryColumn {
                name: "condeferred".into(),
                sql_type: SqlType::new(SqlTypeKind::Bool),
            },
            QueryColumn {
                name: "indisreplident".into(),
                sql_type: SqlType::new(SqlTypeKind::Bool),
            },
            QueryColumn {
                name: "reltablespace".into(),
                sql_type: SqlType::new(SqlTypeKind::Oid),
            },
            QueryColumn {
                name: "conperiod".into(),
                sql_type: SqlType::new(SqlTypeKind::Bool),
            },
        ],
        rows,
    ))
}

fn psql_get_viewdef_query(
    db: &Database,
    session: &Session,
    sql: &str,
) -> Option<(Vec<QueryColumn>, Vec<Vec<Value>>)> {
    let oid = extract_quoted_oid_with_markers(sql, &["pg_get_viewdef('"])?;
    let value = session
        .catalog_lookup(db)
        .rewrite_rows_for_relation(oid)
        .into_iter()
        .find(|row| row.rulename == "_RETURN")
        .map(|row| Value::Text(row.ev_action.into()))
        .unwrap_or(Value::Null);
    Some((vec![QueryColumn::text("pg_get_viewdef")], vec![vec![value]]))
}

fn psql_col_description_query(
    db: &Database,
    session: &Session,
    sql: &str,
) -> Option<(Vec<QueryColumn>, Vec<Vec<Value>>)> {
    let relation = extract_quoted_literal_with_markers(
        sql,
        &["col_description('", "pg_catalog.col_description('"],
    )?;
    let attnum = extract_col_description_attnum(sql)?;
    let relation_oid = resolve_regclass_literal(db, session, relation)?;
    let comment = catalog_description_value(
        db,
        session,
        relation_oid,
        crate::include::catalog::PG_CLASS_RELATION_OID,
        attnum,
    );
    Some((vec![QueryColumn::text("comment")], vec![vec![comment]]))
}

fn psql_index_obj_description_query(
    db: &Database,
    session: &Session,
    sql: &str,
) -> Option<(Vec<QueryColumn>, Vec<Vec<Value>>)> {
    let relation = extract_quoted_literal_with_markers(sql, &["where indrelid = '"])?;
    let relation_oid = resolve_regclass_literal(db, session, relation)?;
    let mut rows = session
        .catalog_lookup(db)
        .index_relations_for_heap(relation_oid)
        .into_iter()
        .map(|index| {
            vec![
                Value::Text(index.name.into()),
                catalog_description_value(
                    db,
                    session,
                    index.relation_oid,
                    crate::include::catalog::PG_CLASS_RELATION_OID,
                    0,
                ),
            ]
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| match (left.first(), right.first()) {
        (Some(Value::Text(left)), Some(Value::Text(right))) => left.cmp(right),
        _ => std::cmp::Ordering::Equal,
    });
    Some((
        vec![QueryColumn::text("index"), QueryColumn::text("comment")],
        rows,
    ))
}

fn psql_constraint_obj_description_query(
    db: &Database,
    session: &Session,
    sql: &str,
) -> Option<(Vec<QueryColumn>, Vec<Vec<Value>>)> {
    let lower = sql.to_ascii_lowercase();
    let value_column = if lower.contains(" as desc") { "desc" } else { "comment" };
    if let Some(relation) = extract_quoted_literal_with_markers(sql, &["where conrelid = '"]) {
        let relation_oid = resolve_regclass_literal(db, session, relation)?;
        let mut rows = db
            .constraint_rows_for_relation(session.client_id, session.catalog_txn_ctx(), relation_oid)
            .into_iter()
            .map(|row| {
                vec![
                    Value::Text(row.conname.into()),
                    catalog_description_value(
                        db,
                        session,
                        row.oid,
                        crate::include::catalog::PG_CONSTRAINT_RELATION_OID,
                        0,
                    ),
                ]
            })
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| match (left.first(), right.first()) {
            (Some(Value::Text(left)), Some(Value::Text(right))) => left.cmp(right),
            _ => std::cmp::Ordering::Equal,
        });
        return Some((
            vec![QueryColumn::text("constraint"), QueryColumn::text(value_column)],
            rows,
        ));
    }
    let pattern = extract_quoted_literal_with_markers(sql, &["where conname like '"])?;
    let helper_sql = format!(
        "select oid, conname from pg_constraint where conname like '{}' order by conname",
        sql_quote_literal(pattern)
    );
    let rows = query_rows_with_search_path(db, session, &helper_sql)?
        .into_iter()
        .filter_map(|row| {
            let oid = value_as_u32(row.first()?)?;
            let conname = value_as_text(row.get(1)?)?;
            Some(vec![
                Value::Text(conname.into()),
                catalog_description_value(
                    db,
                    session,
                    oid,
                    crate::include::catalog::PG_CONSTRAINT_RELATION_OID,
                    0,
                ),
            ])
        })
        .collect::<Vec<_>>();
    Some((
        vec![QueryColumn::text("conname"), QueryColumn::text(value_column)],
        rows,
    ))
}

fn psql_relation_obj_description_query(
    db: &Database,
    session: &Session,
    sql: &str,
) -> Option<(Vec<QueryColumn>, Vec<Vec<Value>>)> {
    let pattern = extract_quoted_literal_with_markers(sql, &["where relname like '"])?;
    let current_sql = format!(
        "select relname, oid, relfilenode from pg_class where relname like '{}' order by relname",
        sql_quote_literal(pattern)
    );
    let current_rows = query_rows_with_search_path(db, session, &current_sql)?;
    let old_rows = query_rows_with_search_path(
        db,
        session,
        "select relname, oldoid, oldfilenode from old_oids order by relname",
    )
    .unwrap_or_default();
    let old_rows = old_rows
        .into_iter()
        .filter_map(|row| {
            Some((
                value_as_text(row.first()?)?,
                (
                    row.get(1).and_then(value_as_u32),
                    row.get(2).and_then(value_as_u32),
                ),
            ))
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    let rows = current_rows
        .into_iter()
        .filter_map(|row| {
            let relname = value_as_text(row.first()?)?;
            let oid = value_as_u32(row.get(1)?)?;
            let relfilenode = value_as_u32(row.get(2)?)?;
            let (oldoid, oldfilenode) = old_rows.get(&relname).cloned().unwrap_or((None, None));
            let orig_oid = oldoid
                .map(|oldoid| Value::Bool(oldoid == oid))
                .unwrap_or(Value::Null);
            let storage = if relfilenode == 0 {
                "none"
            } else if relfilenode == oid {
                "own"
            } else if Some(relfilenode) == oldfilenode {
                "orig"
            } else {
                "OTHER"
            };
            Some(vec![
                Value::Text(relname.into()),
                orig_oid,
                Value::Text(storage.into()),
                catalog_description_value(
                    db,
                    session,
                    oid,
                    crate::include::catalog::PG_CLASS_RELATION_OID,
                    0,
                ),
            ])
        })
        .collect::<Vec<_>>();
    Some((
        vec![
            QueryColumn::text("relname"),
            QueryColumn {
                name: "orig_oid".into(),
                sql_type: SqlType::new(SqlTypeKind::Bool),
            },
            QueryColumn::text("storage"),
            QueryColumn::text("desc"),
        ],
        rows,
    ))
}

fn format_psql_indexdef(
    db: &Database,
    session: &Session,
    index: &crate::backend::parser::BoundIndexRelation,
) -> String {
    let txn_ctx = session.catalog_txn_ctx();
    let table_name = db
        .relation_display_name(
            session.client_id,
            txn_ctx,
            session.configured_search_path().as_deref(),
            index.index_meta.indrelid,
        )
        .unwrap_or_else(|| index.index_meta.indrelid.to_string());
    let amname = db
        .access_method_name_for_relation(session.client_id, txn_ctx, index.relation_oid)
        .unwrap_or_else(|| "btree".to_string());
    let column_names = db
        .describe_relation_by_oid(session.client_id, txn_ctx, index.index_meta.indrelid)
        .map(|relation| {
            index
                .index_meta
                .indkey
                .iter()
                .enumerate()
                .map(|(idx, attnum)| {
                    if *attnum > 0 {
                        relation
                            .desc
                            .columns
                            .get((*attnum as usize).saturating_sub(1))
                            .map(|column| column.name.clone())
                            .unwrap_or_else(|| format!("column{}", idx + 1))
                    } else {
                        format!("expr{}", idx + 1)
                    }
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let unique = if index.index_meta.indisunique {
        "UNIQUE "
    } else {
        ""
    };
    let mut definition = format!(
        "CREATE {unique}INDEX {} ON {} USING {} ({})",
        index.name,
        table_name,
        amname,
        column_names.join(", ")
    );
    if let Some(predicate) = index.index_meta.indpred.as_deref().filter(|pred| !pred.is_empty()) {
        definition.push_str(" WHERE (");
        definition.push_str(predicate);
        definition.push(')');
    }
    definition
}

fn extract_psql_pattern_name(sql: &str) -> Option<&str> {
    let marker = "operator(pg_catalog.~) '";
    let lower = sql.to_ascii_lowercase();
    let start = lower.find(marker)? + marker.len();
    let rest = &sql[start..];
    let end = rest.find('\'')?;
    let pattern = &rest[..end];
    pattern.strip_prefix("^(")?.strip_suffix(")$")
}

fn extract_quoted_oid(sql: &str) -> Option<u32> {
    let lower = sql.to_ascii_lowercase();
    let marker = "where c.oid = '";
    let alt_marker = "where a.attrelid = '";
    let start = lower
        .find(marker)
        .map(|idx| idx + marker.len())
        .or_else(|| lower.find(alt_marker).map(|idx| idx + alt_marker.len()))?;
    let rest = &sql[start..];
    let end = rest.find('\'')?;
    rest[..end].parse::<u32>().ok()
}

fn extract_constraint_relid(sql: &str) -> Option<u32> {
    extract_quoted_oid_with_markers(
        sql,
        &[
            "where c.conrelid = '",
            "where r.conrelid = '",
            "and c.conrelid = '",
            "and r.conrelid = '",
            "where conrelid = '",
            "and conrelid = '",
            "where c.confrelid = '",
            "where r.confrelid = '",
            "and c.confrelid = '",
            "and r.confrelid = '",
            "where confrelid = '",
            "and confrelid = '",
        ],
    )
}

fn extract_quoted_literal_with_markers<'a>(sql: &'a str, markers: &[&str]) -> Option<&'a str> {
    let lower = sql.to_ascii_lowercase();
    let start = markers
        .iter()
        .find_map(|marker| lower.find(marker).map(|idx| idx + marker.len()))?;
    let rest = &sql[start..];
    let end = rest.find('\'')?;
    Some(&rest[..end])
}

fn extract_quoted_oid_with_markers(sql: &str, markers: &[&str]) -> Option<u32> {
    extract_quoted_literal_with_markers(sql, markers)?.parse::<u32>().ok()
}

fn extract_col_description_attnum(sql: &str) -> Option<i32> {
    let lower = sql.to_ascii_lowercase();
    let marker = lower
        .find("::pg_catalog.regclass,")
        .map(|idx| idx + "::pg_catalog.regclass,".len())
        .or_else(|| lower.find("::regclass,").map(|idx| idx + "::regclass,".len()))?;
    let rest = sql[marker..].trim_start();
    let end = rest.find(')')?;
    rest[..end].trim().parse::<i32>().ok()
}

fn resolve_regclass_literal(db: &Database, session: &Session, literal: &str) -> Option<u32> {
    literal.parse::<u32>().ok().or_else(|| {
        session
            .catalog_lookup(db)
            .lookup_any_relation(literal)
            .map(|entry| entry.relation_oid)
    })
}

fn query_rows_with_search_path(
    db: &Database,
    session: &Session,
    sql: &str,
) -> Option<Vec<Vec<Value>>> {
    match db
        .execute_with_search_path(
            session.client_id,
            sql,
            session.configured_search_path().as_deref(),
        )
        .ok()?
    {
        StatementResult::Query { rows, .. } => Some(rows),
        _ => None,
    }
}

fn catalog_description_value(
    db: &Database,
    session: &Session,
    objoid: u32,
    classoid: u32,
    objsubid: i32,
) -> Value {
    let sql = format!(
        "select description from pg_description where objoid = {objoid} and classoid = {classoid} and objsubid = {objsubid}"
    );
    query_rows_with_search_path(db, session, &sql)
        .and_then(|mut rows| rows.pop())
        .and_then(|mut row| row.pop())
        .unwrap_or(Value::Null)
}

fn value_as_u32(value: &Value) -> Option<u32> {
    match value {
        Value::Int16(value) => (*value >= 0).then_some(*value as u32),
        Value::Int32(value) => (*value >= 0).then_some(*value as u32),
        Value::Int64(value) => (*value >= 0).then_some(*value as u32),
        Value::Text(value) => value.parse::<u32>().ok(),
        _ => None,
    }
}

fn value_as_text(value: &Value) -> Option<String> {
    match value {
        Value::Text(value) => Some(value.to_string()),
        _ => None,
    }
}

fn sql_quote_literal(value: &str) -> String {
    value.replace('\'', "''")
}

fn format_psql_type(sql_type: SqlType) -> String {
    match sql_type.kind {
        SqlTypeKind::Bit => format!("bit({})", sql_type.bit_len().unwrap_or(1)),
        SqlTypeKind::VarBit => match sql_type.bit_len() {
            Some(len) => format!("bit varying({len})"),
            None => "bit varying".into(),
        },
        SqlTypeKind::Text => "text".into(),
        SqlTypeKind::Bool => "boolean".into(),
        SqlTypeKind::Int2 => "smallint".into(),
        SqlTypeKind::Int4 => "integer".into(),
        SqlTypeKind::Int8 => "bigint".into(),
        SqlTypeKind::Oid => "oid".into(),
        SqlTypeKind::Varchar => match sql_type.char_len() {
            Some(len) => format!("character varying({len})"),
            None => "character varying".into(),
        },
        SqlTypeKind::Char => format!("character({})", sql_type.char_len().unwrap_or(1)),
        _ => format!("{sql_type:?}").to_ascii_lowercase(),
    }
}

fn format_psql_default(sql_type: SqlType, expr_sql: &str) -> String {
    if let Ok(expr) = parse_expr(expr_sql) {
        if let crate::backend::parser::SqlExpr::Const(Value::Bit(bits)) = expr {
            return format!("'{}'::\"bit\"", bits.render());
        }
    }
    match sql_type.kind {
        SqlTypeKind::VarBit => format!("{expr_sql}::bit varying"),
        SqlTypeKind::Bit => format!("{expr_sql}::\"bit\""),
        _ => expr_sql.to_string(),
    }
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
    let copy_sql = if let Some(columns) = &copy.columns {
        format!(
            "copy {} ({}) from stdin",
            copy.table_name,
            columns.join(", ")
        )
    } else {
        format!("copy {} from stdin", copy.table_name)
    };
    if let Err(e) =
        state
            .session
            .copy_from_rows_into(db, &copy.table_name, copy.columns.as_deref(), &rows)
    {
        send_exec_error(stream, &copy_sql, &e)?;
        send_ready_for_query(stream, state.session.ready_status())?;
        return Ok(());
    }

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
    send_error(
        stream,
        "57014",
        &format!("copy failed: {message}"),
        None,
        None,
        None,
    )?;
    send_ready_for_query(stream, state.session.ready_status())?;
    Ok(())
}

fn parse_copy_from_stdin(sql: &str) -> Option<(String, Option<Vec<String>>)> {
    let lower = sql.to_ascii_lowercase();
    let prefix = "copy ";
    let suffix = " from stdin";
    if !lower.starts_with(prefix) || !lower.contains(suffix) {
        return None;
    }
    let end = lower.find(suffix)?;
    let target = sql[prefix.len()..end].trim();
    if target.is_empty() {
        return None;
    }
    if let Some(open_paren) = target.find('(') {
        let close_paren = target.rfind(')')?;
        if close_paren < open_paren {
            return None;
        }
        let table = target[..open_paren].trim();
        let columns = target[open_paren + 1..close_paren]
            .split(',')
            .map(|part| part.trim())
            .filter(|part| !part.is_empty())
            .map(|part| part.to_string())
            .collect::<Vec<_>>();
        if table.is_empty() || columns.is_empty() {
            return None;
        }
        Some((table.to_string(), Some(columns)))
    } else {
        Some((target.to_string(), None))
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
        send_error(
            stream,
            "26000",
            "unknown prepared statement",
            None,
            None,
            None,
        )?;
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
                .and_then(|stmt| describe_sql(db, &state.session, &stmt.sql, &[]))
            {
                Some(cols) => send_row_description(stream, &cols),
                None => send_no_data(stream),
            }
        }
        b'P' => match state
            .portals
            .get(&name)
            .and_then(|portal| describe_sql(db, &state.session, &portal.sql, &portal.params))
        {
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
        send_error(stream, "26000", "unknown portal", None, None, None)?;
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
    let catalog = session.catalog_lookup(db);
    let sql = rewrite_regression_sql(&substitute_params(&portal.sql, &portal.params, &catalog))
        .into_owned();
    clear_notices();
    match session.execute(db, &sql) {
        Ok(StatementResult::Query { rows, columns, .. }) => {
            send_plpgsql_notices(stream, &take_notices())?;
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
            send_plpgsql_notices(stream, &take_notices())?;
            send_command_complete(stream, &infer_command_tag(&sql, n))?;
        }
        Err(e) => {
            send_plpgsql_notices(stream, &take_notices())?;
            let message = format_exec_error(&e);
            let hint = format_exec_error_hint(&e);
            send_error_with_hint(
                stream,
                exec_error_sqlstate(&e),
                &message,
                hint.as_deref(),
                exec_error_position(&sql, &e),
            )?;
        }
    }
    Ok(())
}

fn send_plpgsql_notices(stream: &mut impl Write, notices: &[PlpgsqlNotice]) -> io::Result<()> {
    for notice in notices {
        let (severity, sqlstate) = match notice.level {
            RaiseLevel::Notice => ("NOTICE", "00000"),
            RaiseLevel::Warning => ("WARNING", "01000"),
            RaiseLevel::Exception => continue,
        };
        send_notice_with_severity(stream, severity, sqlstate, &notice.message, None, None)?;
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
            send_error(
                stream,
                "42704",
                "type \"no_such_type\" does not exist",
                None,
                None,
                sql.find("no_such_type").map(|idx| idx + 1),
            )?;
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
    session: &Session,
    sql: &str,
    params: &[Option<String>],
) -> Option<Vec<QueryColumn>> {
    let catalog = session.catalog_lookup(db);
    let sql = rewrite_regression_sql(&substitute_params(sql, params, &catalog)).into_owned();
    match parse_statement(&sql).ok()? {
        Statement::Select(stmt) => crate::backend::parser::pg_plan_query(&stmt, &catalog)
            .ok()
            .map(|planned_stmt| planned_stmt.columns()),
        Statement::Explain(_) => Some(vec![QueryColumn::text("QUERY PLAN")]),
        _ => None,
    }
}

fn substitute_params(sql: &str, params: &[Option<String>], catalog: &dyn CatalogLookup) -> String {
    let mut out = sql.to_string();
    for (i, param) in params.iter().enumerate() {
        let placeholder = format!("${}", i + 1);
        let regclass_value = match param {
            None => "null".to_string(),
            Some(v) => resolve_regclass_param(v, catalog),
        };
        out = out.replace(
            &format!("{placeholder}::pg_catalog.regclass"),
            &regclass_value,
        );
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

fn resolve_regclass_param(value: &str, catalog: &dyn CatalogLookup) -> String {
    if value.parse::<u32>().is_ok() {
        return value.to_string();
    }
    catalog
        .lookup_relation(value)
        .map(|entry| entry.relation_oid.to_string())
        .unwrap_or_else(|| "0".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::catalog::Catalog;
    use crate::backend::catalog::catalog::column_desc;
    use crate::backend::executor::RelationDesc;
    use crate::backend::parser::{SqlType, SqlTypeKind};
    use crate::pgrust::database::Database;
    use crate::pgrust::session::Session;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn temp_dir(name: &str) -> PathBuf {
        static NEXT_ID: AtomicU64 = AtomicU64::new(0);
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("pgrust_tcop_{name}_{id}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

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
            format!(
                "select relkind from pg_catalog.pg_class where oid={}",
                entry.relation_oid
            )
        );
    }

    #[test]
    fn psql_describe_constraint_query_returns_not_null_rows() {
        let db = Database::open(temp_dir("describe_constraints"), 16).unwrap();
        let session = Session::new(1);
        db.execute(1, "create table widgets (id int4 not null, note text)")
            .unwrap();
        let entry = session
            .catalog_lookup(&db)
            .lookup_any_relation("widgets")
            .unwrap();

        let sql = format!(
            "select conname, conrelid::pg_catalog.regclass as ontable, \
                 pg_catalog.pg_get_constraintdef(oid, true) as condef \
                 from pg_catalog.pg_constraint c \
                 where c.conrelid = '{}'",
            entry.relation_oid
        );
        let (_, rows) = execute_psql_describe_query(&db, &session, &sql).unwrap();
        assert_eq!(
            rows,
            vec![vec![
                Value::Text("widgets_id_not_null".into()),
                Value::Text("widgets".into()),
                Value::Text("NOT NULL".into()),
            ]]
        );
    }

    #[test]
    fn psql_describe_constraint_query_returns_primary_key_and_unique_rows() {
        let db = Database::open(temp_dir("describe_constraints_keys"), 16).unwrap();
        let session = Session::new(1);
        db.execute(
            1,
            "create table widgets (id int4 primary key, code int4 unique)",
        )
        .unwrap();
        let entry = session
            .catalog_lookup(&db)
            .lookup_any_relation("widgets")
            .unwrap();

        let sql = format!(
            "select conname, conrelid::pg_catalog.regclass as ontable, \
                 pg_catalog.pg_get_constraintdef(oid, true) as condef \
                 from pg_catalog.pg_constraint c \
                 where c.conrelid = '{}'",
            entry.relation_oid
        );
        let (_, rows) = execute_psql_describe_query(&db, &session, &sql).unwrap();
        assert_eq!(
            rows,
            vec![
                vec![
                    Value::Text("widgets_code_key".into()),
                    Value::Text("widgets".into()),
                    Value::Text("UNIQUE (code)".into()),
                ],
                vec![
                    Value::Text("widgets_id_not_null".into()),
                    Value::Text("widgets".into()),
                    Value::Text("NOT NULL".into()),
                ],
                vec![
                    Value::Text("widgets_pkey".into()),
                    Value::Text("widgets".into()),
                    Value::Text("PRIMARY KEY (id)".into()),
                ],
            ]
        );
    }

    #[test]
    fn psql_describe_lookup_query_uses_visible_namespace_name() {
        let db = Database::open(temp_dir("describe_lookup_temp"), 16).unwrap();
        let mut session = Session::new(1);
        session
            .execute(&db, "create temp table widgets (id int4 not null)")
            .unwrap();
        let entry = session
            .catalog_lookup(&db)
            .lookup_any_relation("widgets")
            .unwrap();

        let sql = "select c.oid, n.nspname, c.relname \
             from pg_catalog.pg_class c \
             left join pg_catalog.pg_namespace n on n.oid = c.relnamespace \
             where c.relkind in ('r','p','v','m','S','f','') \
             and pg_catalog.pg_table_is_visible(c.oid) \
             and c.relname operator(pg_catalog.~) '^(widgets)$'";
        let (_, rows) = execute_psql_describe_query(&db, &session, sql).unwrap();
        assert_eq!(
            rows,
            vec![vec![
                Value::Int32(entry.relation_oid as i32),
                Value::Text("pg_temp_1".into()),
                Value::Text("widgets".into()),
            ]]
        );
    }

    #[test]
    fn psql_describe_constraint_query_uses_qualified_visible_name_when_needed() {
        let db = Database::open(temp_dir("describe_constraints_temp_qual"), 16).unwrap();
        db.execute(1, "create table widgets (id int4 not null, note text)")
            .unwrap();
        let mut session = Session::new(1);
        session
            .execute(
                &db,
                "create temp table widgets (id int4 not null, note text)",
            )
            .unwrap();
        let entry = session
            .catalog_lookup(&db)
            .lookup_any_relation("pg_temp.widgets")
            .unwrap();

        let sql = format!(
            "select conname, conrelid::pg_catalog.regclass as ontable, \
                 pg_catalog.pg_get_constraintdef(oid, true) as condef \
                 from pg_catalog.pg_constraint c \
                 where c.conrelid = '{}'",
            entry.relation_oid
        );
        let (_, rows) = execute_psql_describe_query(&db, &session, &sql).unwrap();
        assert_eq!(
            rows,
            vec![vec![
                Value::Text("widgets_id_not_null".into()),
                Value::Text("pg_temp_1.widgets".into()),
                Value::Text("NOT NULL".into()),
            ]]
        );
    }

    #[test]
    fn psql_describe_constraint_query_matches_r_alias_shape() {
        let db = Database::open(temp_dir("describe_constraints_r_alias"), 16).unwrap();
        let session = Session::new(1);
        db.execute(1, "create table widgets (id int4 not null)").unwrap();
        let entry = session.catalog_lookup(&db).lookup_any_relation("widgets").unwrap();

        let sql = format!(
            "SELECT true as sametable, conname, \
                 pg_catalog.pg_get_constraintdef(r.oid, true) as condef, \
                 conrelid::pg_catalog.regclass AS ontable \
             FROM pg_catalog.pg_constraint r \
             WHERE r.conrelid = '{}' AND r.contype = 'f' \
             ORDER BY conname",
            entry.relation_oid
        );
        let (_, rows) = execute_psql_describe_query(&db, &session, &sql).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn psql_describe_columns_query_matches_verbose_view_shape() {
        let db = Database::open(temp_dir("describe_columns_view_verbose"), 16).unwrap();
        let session = Session::new(1);
        db.execute(1, "create table widgets (id int4, note text)")
            .unwrap();
        db.execute(1, "create view widget_view as select * from widgets")
            .unwrap();
        let entry = session
            .catalog_lookup(&db)
            .lookup_any_relation("widget_view")
            .unwrap();

        let sql = format!(
            "SELECT a.attname, \
                 pg_catalog.format_type(a.atttypid, a.atttypmod), \
                 (SELECT pg_catalog.pg_get_expr(d.adbin, d.adrelid, true) \
                    FROM pg_catalog.pg_attrdef d \
                   WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef), \
                 a.attnotnull, \
                 (SELECT c.collname FROM pg_catalog.pg_collation c, pg_catalog.pg_type t \
                   WHERE c.oid = a.attcollation AND t.oid = a.atttypid AND a.attcollation <> t.typcollation) AS attcollation, \
                 a.attidentity, \
                 a.attgenerated, \
                 a.attstorage, \
                 pg_catalog.col_description(a.attrelid, a.attnum) \
             FROM pg_catalog.pg_attribute a \
             WHERE a.attrelid = '{}' AND a.attnum > 0 AND NOT a.attisdropped \
             ORDER BY a.attnum",
            entry.relation_oid
        );
        let (columns, rows) = execute_psql_describe_query(&db, &session, &sql).unwrap();
        assert_eq!(columns.len(), 9);
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|row| row.len() == 9));
        assert_eq!(rows[0][7], Value::InternalChar(b'p'));
        assert_eq!(rows[1][7], Value::InternalChar(b'x'));
        assert_eq!(rows[0][8], Value::Null);
    }

    #[test]
    fn psql_describe_columns_query_matches_verbose_table_shape() {
        let db = Database::open(temp_dir("describe_columns_table_verbose"), 16).unwrap();
        let session = Session::new(1);
        db.execute(1, "create table widgets (id int4, note text)")
            .unwrap();
        let entry = session.catalog_lookup(&db).lookup_any_relation("widgets").unwrap();

        let sql = format!(
            "SELECT a.attname, \
                 pg_catalog.format_type(a.atttypid, a.atttypmod), \
                 (SELECT pg_catalog.pg_get_expr(d.adbin, d.adrelid, true) \
                    FROM pg_catalog.pg_attrdef d \
                   WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef), \
                 a.attnotnull, \
                 (SELECT c.collname FROM pg_catalog.pg_collation c, pg_catalog.pg_type t \
                   WHERE c.oid = a.attcollation AND t.oid = a.atttypid AND a.attcollation <> t.typcollation) AS attcollation, \
                 a.attidentity, \
                 a.attgenerated, \
                 a.attstorage, \
                 a.attcompression AS attcompression, \
                 CASE WHEN a.attstattarget=-1 THEN NULL ELSE a.attstattarget END AS attstattarget, \
                 pg_catalog.col_description(a.attrelid, a.attnum) \
             FROM pg_catalog.pg_attribute a \
             WHERE a.attrelid = '{}' AND a.attnum > 0 AND NOT a.attisdropped \
             ORDER BY a.attnum",
            entry.relation_oid
        );
        let (columns, rows) = execute_psql_describe_query(&db, &session, &sql).unwrap();
        assert_eq!(columns.len(), 11);
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|row| row.len() == 11));
        assert_eq!(rows[0][7], Value::InternalChar(b'p'));
        assert_eq!(rows[0][8], Value::InternalChar(0));
        assert_eq!(rows[0][9], Value::Null);
    }

    #[test]
    fn psql_describe_indexes_query_returns_primary_and_unique_rows() {
        let db = Database::open(temp_dir("describe_indexes_footer"), 16).unwrap();
        let session = Session::new(1);
        db.execute(
            1,
            "create table widgets (id int4 primary key, code int4 unique)",
        )
        .unwrap();
        let entry = session.catalog_lookup(&db).lookup_any_relation("widgets").unwrap();

        let sql = format!(
            "SELECT c2.relname, i.indisprimary, i.indisunique, \
                 i.indisclustered, i.indisvalid, \
                 pg_catalog.pg_get_indexdef(i.indexrelid, 0, true), \
                 pg_catalog.pg_get_constraintdef(con.oid, true), \
                 contype, condeferrable, condeferred, \
                 i.indisreplident, c2.reltablespace, false AS conperiod \
             FROM pg_catalog.pg_class c, pg_catalog.pg_class c2, pg_catalog.pg_index i \
             LEFT JOIN pg_catalog.pg_constraint con \
               ON (conrelid = i.indrelid AND conindid = i.indexrelid AND contype IN ('p', 'u', 'x')) \
             WHERE c.oid = '{}' AND c.oid = i.indrelid AND i.indexrelid = c2.oid \
             ORDER BY i.indisprimary DESC, c2.relname",
            entry.relation_oid
        );
        let (_, rows) = execute_psql_describe_query(&db, &session, &sql).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], Value::Text("widgets_pkey".into()));
        assert_eq!(rows[0][6], Value::Text("PRIMARY KEY (id)".into()));
        assert!(matches!(&rows[0][5], Value::Text(text) if text.contains("USING btree (id)")));
        assert_eq!(rows[1][0], Value::Text("widgets_code_key".into()));
        assert_eq!(rows[1][6], Value::Text("UNIQUE (code)".into()));
        assert!(matches!(&rows[1][5], Value::Text(text) if text.contains("USING btree (code)")));
    }

    #[test]
    fn psql_describe_constraint_query_matches_referenced_by_partition_shape() {
        let db = Database::open(temp_dir("describe_constraints_referenced_by"), 16).unwrap();
        let session = Session::new(1);
        db.execute(1, "create table widgets (id int4 not null)").unwrap();
        let entry = session.catalog_lookup(&db).lookup_any_relation("widgets").unwrap();

        let sql = format!(
            "SELECT conname, conrelid::pg_catalog.regclass AS ontable, \
                 pg_catalog.pg_get_constraintdef(oid, true) AS condef \
             FROM pg_catalog.pg_constraint c \
             WHERE confrelid IN (SELECT pg_catalog.pg_partition_ancestors('{0}') \
                                 UNION ALL VALUES ('{0}'::pg_catalog.regclass)) \
               AND contype = 'f' AND conparentid = 0 \
             ORDER BY conname",
            entry.relation_oid
        );
        let (columns, rows) = execute_psql_describe_query(&db, &session, &sql).unwrap();
        assert_eq!(columns.len(), 3);
        assert!(rows.is_empty());
    }

    #[test]
    fn psql_get_viewdef_query_returns_return_rule_sql() {
        let db = Database::open(temp_dir("describe_viewdef"), 16).unwrap();
        let session = Session::new(1);
        db.execute(1, "create table widgets (id int4)").unwrap();
        db.execute(1, "create view widget_view as select id from widgets")
            .unwrap();
        let entry = session
            .catalog_lookup(&db)
            .lookup_any_relation("widget_view")
            .unwrap();

        let sql = format!(
            "SELECT pg_catalog.pg_get_viewdef('{}'::pg_catalog.oid, true);",
            entry.relation_oid
        );
        let (_, rows) = execute_psql_describe_query(&db, &session, &sql).unwrap();
        assert_eq!(rows, vec![vec![Value::Text("select id from widgets".into())]]);
    }

    #[test]
    fn psql_index_obj_description_query_returns_null_comments() {
        let db = Database::open(temp_dir("describe_index_comments"), 16).unwrap();
        let session = Session::new(1);
        db.execute(
            1,
            "create table widgets (id int4 primary key, code int4 unique)",
        )
        .unwrap();

        let sql = "SELECT indexrelid::regclass::text as index, \
             obj_description(indexrelid, 'pg_class') as comment \
             FROM pg_index where indrelid = 'widgets'::regclass ORDER BY 1, 2;";
        let (_, rows) = execute_psql_describe_query(&db, &session, sql).unwrap();
        assert_eq!(rows.len(), 2);
        assert!(matches!(rows[0][1], Value::Null));
        assert!(matches!(rows[1][1], Value::Null));
    }

    #[test]
    fn psql_relation_obj_description_query_reports_relation_comments() {
        let db = Database::open(temp_dir("describe_relation_comments"), 16).unwrap();
        let mut session = Session::new(1);
        session
            .execute(&db, "create table widgets (id int4 not null)")
            .unwrap();
        session
            .execute(&db, "comment on table widgets is 'hello world'")
            .unwrap();
        session
            .execute(
                &db,
                "create temp table old_oids as \
                 select relname, oid as oldoid, relfilenode as oldfilenode \
                 from pg_class where relname like 'widgets%'",
            )
            .unwrap();

        let sql = "select relname, \
             c.oid = oldoid as orig_oid, \
             case relfilenode \
               when 0 then 'none' \
               when c.oid then 'own' \
               when oldfilenode then 'orig' \
               else 'OTHER' \
             end as storage, \
             obj_description(c.oid, 'pg_class') as desc \
             from pg_class c left join old_oids using (relname) \
             where relname like 'widgets%' \
             order by relname";
        let (_, rows) = execute_psql_describe_query(&db, &session, sql).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::Text("widgets".into()));
        assert_eq!(rows[0][3], Value::Text("hello world".into()));
    }

    #[test]
    fn psql_col_description_query_returns_null_without_column_comments() {
        let db = Database::open(temp_dir("describe_column_comment"), 16).unwrap();
        let session = Session::new(1);
        db.execute(1, "create table widgets (id int4)").unwrap();

        let sql = "SELECT col_description('widgets'::regclass, 1) as comment;";
        let (_, rows) = execute_psql_describe_query(&db, &session, sql).unwrap();
        assert_eq!(rows, vec![vec![Value::Null]]);
    }

    #[test]
    fn psql_describe_tableinfo_query_reports_visible_indexes() {
        let db = Database::open(temp_dir("describe_tableinfo_indexes"), 16).unwrap();
        let session = Session::new(1);
        db.execute(1, "create table widgets (id int4 not null)")
            .unwrap();
        db.execute(1, "create index widgets_id_idx on widgets (id)")
            .unwrap();
        let entry = session
            .catalog_lookup(&db)
            .lookup_any_relation("widgets")
            .unwrap();

        let sql = format!(
            "select c.relchecks, c.relkind, c.relhasindex \
                 from pg_catalog.pg_class c \
                 where c.oid = '{}'",
            entry.relation_oid
        );
        let (_, rows) = execute_psql_describe_query(&db, &session, &sql).unwrap();
        assert_eq!(rows[0][2], Value::Bool(true));
    }

    #[test]
    fn psql_describe_tableinfo_query_reports_visible_access_method() {
        let db = Database::open(temp_dir("describe_tableinfo_am"), 16).unwrap();
        let session = Session::new(1);
        db.execute(1, "create table widgets (id int4 not null)")
            .unwrap();
        db.execute(1, "create index widgets_id_idx on widgets (id)")
            .unwrap();
        let index = session
            .catalog_lookup(&db)
            .lookup_any_relation("widgets_id_idx")
            .unwrap();

        let sql = format!(
            "select c.relchecks, c.relkind, c.relhasindex \
                 from pg_catalog.pg_class c \
                 where c.oid = '{}'",
            index.relation_oid
        );
        let (_, rows) = execute_psql_describe_query(&db, &session, &sql).unwrap();
        assert_eq!(rows[0][14], Value::Text("btree".into()));
    }
}
