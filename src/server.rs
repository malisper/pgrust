use std::io::{self, BufWriter, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::thread;

use crate::database::{Database, Session};
use crate::executor::{ExecError, StatementResult, Value};
use crate::parser::{parse_statement, Statement};
use crate::ClientId;

const SSL_REQUEST_CODE: i32 = 80877103;
const PROTOCOL_VERSION_3_0: i32 = 196608;

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

/// Starts a PostgreSQL-compatible TCP server on the given address.
///
/// Each connection is handled in a dedicated thread using the simple query
/// protocol. The server runs until the process is terminated.
pub fn serve(addr: &str, db: Database) -> io::Result<()> {
    let listener = TcpListener::bind(addr)?;
    eprintln!("pgrust: listening on {addr}");

    for stream in listener.incoming() {
        let stream = stream?;
        let peer = stream.peer_addr().ok();
        let db = db.clone();
        thread::spawn(move || {
            let client_id = NEXT_CLIENT_ID.fetch_add(1, Ordering::Relaxed);
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
        });
    }
    Ok(())
}

fn handle_connection(
    stream: TcpStream,
    db: &Database,
    client_id: ClientId,
) -> io::Result<()> {
    // Use a BufWriter for the write side so multiple small write_all calls
    // (e.g. message tag + length + body) are batched into a single sendto
    // syscall per flush. Reads go through the raw stream reference.
    let mut reader = stream.try_clone()?;
    let mut writer = BufWriter::new(stream);

    // Phase 1: SSL / startup negotiation.
    // The first message has no type byte — just length + payload.
    loop {
        let len = read_i32(&mut reader)? as usize;
        if len < 4 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "startup packet too short"));
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
            PROTOCOL_VERSION_3_0 => {
                break;
            }
            _ => {
                let msg = format!("unsupported protocol version: {code}");
                send_error(&mut writer, "08P01", &msg)?;
                writer.flush()?;
                return Ok(());
            }
        }
    }

    // Phase 2: send authentication OK + startup parameters.
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

    // Phase 3: query loop.
    let mut state = ConnectionState {
        session: Session::new(client_id),
        prepared: HashMap::new(),
        portals: HashMap::new(),
        copy_in: None,
    };

    loop {
        let msg_type = match read_byte(&mut reader) {
            Ok(b) => b,
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(e) => return Err(e),
        };

        let len = read_i32(&mut reader)? as usize;
        if len < 4 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "message too short"));
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
            b'd' => {
                handle_copy_data(&mut state, &body)?;
            }
            b'c' => {
                handle_copy_done(&mut writer, db, &mut state)?;
                writer.flush()?;
            }
            b'f' => {
                handle_copy_fail(&mut writer, &mut state, &body)?;
                writer.flush()?;
            }
            b'X' => {
                return Ok(());
            }
            _ => {
                send_error(
                    &mut writer,
                    "0A000",
                    &format!("unsupported message type: '{}'", msg_type as char),
                )?;
                send_ready_for_query(&mut writer, state.session.ready_status())?;
                writer.flush()?;
            }
        }
    }
}

fn handle_query(
    stream: &mut impl Write,
    db: &Database,
    state: &mut ConnectionState,
    sql: &str,
) -> io::Result<()> {
    let sql = sql.trim().trim_end_matches(';').trim();
    if sql.is_empty() {
        send_empty_query(stream)?;
        send_ready_for_query(stream, state.session.ready_status())?;
        return Ok(());
    }

    if let Some(table_name) = parse_copy_from_stdin(sql) {
        state.copy_in = Some(CopyInState {
            table_name,
            pending: Vec::new(),
        });
        send_copy_in_response(stream)?;
        return Ok(());
    }

    // Parse once, then use the streaming path for SELECT queries — avoids
    // materializing the entire result set before sending any rows.
    let parsed = crate::parser::parse_statement(sql);
    if let Ok(crate::parser::Statement::Select(ref select_stmt)) = parsed {
        match state.session.execute_streaming(db, select_stmt) {
            Ok(mut guard) => {
                use crate::executor::exec_next;
                let column_names = guard.column_names.clone();
                let mut row_buf = Vec::new();
                let mut row_count = 0usize;
                let mut header_sent = false;
                let mut err = None;

                loop {
                    match exec_next(&mut guard.state, &mut guard.ctx) {
                        Ok(Some(mut slot)) => {
                            if !header_sent {
                                send_row_description(stream, &column_names)?;
                                header_sent = true;
                            }
                            match slot.values() {
                                Ok(values) => {
                                    send_data_row(stream, values, &mut row_buf)?;
                                    row_count += 1;
                                }
                                Err(e) => { err = Some(e); break; }
                            }
                        }
                        Ok(None) => break,
                        Err(e) => { err = Some(e); break; }
                    }
                }
                drop(guard); // release table lock

                if let Some(e) = err {
                    let msg = format_exec_error(&e);
                    send_error(stream, "XX000", &msg)?;
                } else {
                    if !header_sent {
                        send_row_description(stream, &column_names)?;
                    }
                    send_command_complete(stream, &format!("SELECT {row_count}"))?;
                }
            }
            Err(e) => {
                let msg = format_exec_error(&e);
                let sqlstate = match &e {
                    ExecError::Parse(_) => "42601",
                    _ => "XX000",
                };
                send_error(stream, sqlstate, &msg)?;
            }
        }
    } else {
        match state.session.execute(db, sql) {
            Ok(StatementResult::Query { column_names, rows }) => {
                send_query_result(stream, &column_names, &rows, &format!("SELECT {}", rows.len()))?;
            }
            Ok(StatementResult::AffectedRows(n)) => {
                let tag = infer_command_tag(sql, n);
                send_command_complete(stream, &tag)?;
            }
            Err(e) => {
                let msg = format_exec_error(&e);
                let sqlstate = match &e {
                    ExecError::Parse(_) => "42601",
                    _ => "XX000",
                };
                send_error(stream, sqlstate, &msg)?;
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
        .map(|line| line.split('\t').map(|part| part.to_string()).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    state
        .session
        .copy_from_rows(db, &copy.table_name, &rows)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format_exec_error(&e)))?;

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
    send_error(stream, "57014", &format!("copy failed: {message}"))?;
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
        send_error(stream, "26000", "unknown prepared statement")?;
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
    let target_type = body.get(offset).copied().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidData, "describe target missing")
    })?;
    offset += 1;
    let name = read_cstr(body, &mut offset)?;
    match target_type {
        b'S' => {
            send_parameter_description(stream, &[])?;
            match state.prepared.get(&name).and_then(|stmt| describe_sql(db, &stmt.sql, &[])) {
                Some(cols) => send_row_description(stream, &cols),
                None => send_no_data(stream),
            }
        }
        b'P' => match state
            .portals
            .get(&name)
            .and_then(|portal| describe_sql(db, &portal.sql, &portal.params))
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
        send_error(stream, "26000", "unknown portal")?;
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
    let target_type = body.get(offset).copied().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidData, "close target missing")
    })?;
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
    if let Some((_columns, rows, tag)) = execute_special_query(db, &portal.sql, &portal.params) {
        // Extended protocol: Execute does not resend RowDescription (Describe already did).
        for row in &rows {
            send_data_row(stream, row, &mut row_buf)?;
        }
        send_command_complete(stream, &tag)?;
        return Ok(());
    }

    let sql = substitute_params(&portal.sql, &portal.params);
    match session.execute(db, &sql) {
        Ok(StatementResult::Query { column_names: _, rows }) => {
            // Extended protocol: Execute sends DataRows + CommandComplete only.
            for row in &rows {
                send_data_row(stream, row, &mut row_buf)?;
            }
            send_command_complete(stream, &format!("SELECT {}", rows.len()))?;
        }
        Ok(StatementResult::AffectedRows(n)) => {
            let tag = infer_command_tag(&sql, n);
            send_command_complete(stream, &tag)?;
        }
        Err(e) => {
            let msg = format_exec_error(&e);
            let sqlstate = match &e {
                ExecError::Parse(_) => "42601",
                _ => "XX000",
            };
            send_error(stream, sqlstate, &msg)?;
        }
    }
    Ok(())
}

fn describe_sql(db: &Database, sql: &str, params: &[Option<String>]) -> Option<Vec<String>> {
    // Match special queries by SQL pattern first — params may not be bound yet at
    // Describe-Statement time, so we cannot call execute_special_query here.
    let normalized = sql.trim().to_ascii_lowercase();
    if normalized == "select relkind from pg_catalog.pg_class where oid=$1::pg_catalog.regclass" {
        return Some(vec!["relkind".to_string()]);
    }

    if execute_special_query(db, sql, params).is_some() {
        return Some(vec!["relkind".to_string()]);
    }
    let sql = substitute_params(sql, params);
    match parse_statement(&sql).ok()? {
        Statement::Select(stmt) => Some(stmt.targets.iter().map(|t| t.output_name.clone()).collect()),
        Statement::ShowTables => Some(vec!["table_name".to_string()]),
        Statement::Explain(_) => Some(vec!["QUERY PLAN".to_string()]),
        _ => None,
    }
}

fn execute_special_query(
    db: &Database,
    sql: &str,
    params: &[Option<String>],
) -> Option<(Vec<String>, Vec<Vec<Value>>, String)> {
    let normalized = sql.trim().to_ascii_lowercase();
    if normalized == "select relkind from pg_catalog.pg_class where oid=$1::pg_catalog.regclass" {
        let table_name = params.first()?.as_ref()?.to_ascii_lowercase();
        let exists = db.catalog.read().catalog().get(&table_name).is_some();
        let rows = if exists {
            vec![vec![Value::Text("r".into())]]
        } else {
            Vec::new()
        };
        return Some((vec!["relkind".to_string()], rows.clone(), format!("SELECT {}", rows.len())));
    }
    None
}

fn substitute_params(sql: &str, params: &[Option<String>]) -> String {
    let mut out = sql.to_string();
    for (i, param) in params.iter().enumerate() {
        let placeholder = format!("${}", i + 1);
        let value = match param {
            None => "null".to_string(),
            Some(v) if v.parse::<i64>().is_ok() => v.clone(),
            Some(v) => format!("'{}'", v.replace('\'', "''")),
        };
        out = out.replace(&placeholder, &value);
    }
    out
        .replace("::pg_catalog.regclass", "")
        .replace("::regclass", "")
}

fn send_query_result(
    stream: &mut impl Write,
    column_names: &[String],
    rows: &[Vec<Value>],
    tag: &str,
) -> io::Result<()> {
    send_row_description(stream, column_names)?;
    let mut row_buf = Vec::new();
    for row in rows {
        send_data_row(stream, row, &mut row_buf)?;
    }
    send_command_complete(stream, tag)
}

fn infer_command_tag(sql: &str, affected: usize) -> String {
    let first_word = sql
        .split_ascii_whitespace()
        .next()
        .unwrap_or("")
        .to_ascii_uppercase();
    match first_word.as_str() {
        "INSERT" => format!("INSERT 0 {affected}"),
        "UPDATE" => format!("UPDATE {affected}"),
        "DELETE" => format!("DELETE {affected}"),
        "CREATE" => "CREATE TABLE".to_string(),
        "DROP" => "DROP TABLE".to_string(),
        "BEGIN" | "START" => "BEGIN".to_string(),
        "COMMIT" | "END" => "COMMIT".to_string(),
        "ROLLBACK" => "ROLLBACK".to_string(),
        _ => format!("SELECT {affected}"),
    }
}

fn format_exec_error(e: &ExecError) -> String {
    match e {
        ExecError::Parse(p) => p.to_string(),
        other => format!("{other:?}"),
    }
}

// ---- Wire protocol message writers ----

fn send_auth_ok(w: &mut impl Write) -> io::Result<()> {
    w.write_all(&[b'R'])?;
    w.write_all(&8_i32.to_be_bytes())?; // length (includes self)
    w.write_all(&0_i32.to_be_bytes())?; // auth type 0 = OK
    Ok(())
}

fn send_parameter_status(w: &mut impl Write, name: &str, value: &str) -> io::Result<()> {
    let len = 4 + name.len() + 1 + value.len() + 1;
    w.write_all(&[b'S'])?;
    w.write_all(&(len as i32).to_be_bytes())?;
    w.write_all(name.as_bytes())?;
    w.write_all(&[0])?;
    w.write_all(value.as_bytes())?;
    w.write_all(&[0])?;
    Ok(())
}

fn send_backend_key_data(w: &mut impl Write, pid: i32, key: i32) -> io::Result<()> {
    w.write_all(&[b'K'])?;
    w.write_all(&12_i32.to_be_bytes())?;
    w.write_all(&pid.to_be_bytes())?;
    w.write_all(&key.to_be_bytes())?;
    Ok(())
}

fn send_ready_for_query(w: &mut impl Write, status: u8) -> io::Result<()> {
    w.write_all(&[b'Z'])?;
    w.write_all(&5_i32.to_be_bytes())?;
    w.write_all(&[status])?;
    Ok(())
}

fn send_row_description(w: &mut impl Write, columns: &[String]) -> io::Result<()> {
    let mut body = Vec::new();
    body.extend_from_slice(&(columns.len() as i16).to_be_bytes());
    for col in columns {
        body.extend_from_slice(col.as_bytes());
        body.push(0); // name NUL
        body.extend_from_slice(&0_i32.to_be_bytes()); // table OID
        body.extend_from_slice(&0_i16.to_be_bytes()); // column attr number
        body.extend_from_slice(&25_i32.to_be_bytes()); // type OID (TEXT=25)
        body.extend_from_slice(&(-1_i16).to_be_bytes()); // type size (-1 = varlena)
        body.extend_from_slice(&(-1_i32).to_be_bytes()); // type modifier
        body.extend_from_slice(&0_i16.to_be_bytes()); // format code (text)
    }

    w.write_all(&[b'T'])?;
    w.write_all(&((body.len() + 4) as i32).to_be_bytes())?;
    w.write_all(&body)?;
    Ok(())
}

fn send_data_row(w: &mut impl Write, values: &[Value], buf: &mut Vec<u8>) -> io::Result<()> {
    buf.clear();
    buf.extend_from_slice(&(values.len() as i16).to_be_bytes());
    for val in values {
        match val {
            Value::Null => {
                buf.extend_from_slice(&(-1_i32).to_be_bytes());
            }
            Value::Int32(v) => {
                let start = buf.len();
                buf.extend_from_slice(&0_i32.to_be_bytes()); // length placeholder
                let mut itoa_buf = itoa::Buffer::new();
                let written = itoa_buf.format(*v);
                buf.extend_from_slice(written.as_bytes());
                let text_len = (buf.len() - start - 4) as i32;
                buf[start..start + 4].copy_from_slice(&text_len.to_be_bytes());
            }
            Value::Float64(v) => {
                let start = buf.len();
                buf.extend_from_slice(&0_i32.to_be_bytes());
                use std::io::Write as _;
                write!(buf, "{v}").unwrap();
                let text_len = (buf.len() - start - 4) as i32;
                buf[start..start + 4].copy_from_slice(&text_len.to_be_bytes());
            }
            Value::Text(v) => {
                buf.extend_from_slice(&(v.len() as i32).to_be_bytes());
                buf.extend_from_slice(v.as_bytes());
            }
            Value::Bool(true) => {
                buf.extend_from_slice(&1_i32.to_be_bytes());
                buf.push(b't');
            }
            Value::Bool(false) => {
                buf.extend_from_slice(&1_i32.to_be_bytes());
                buf.push(b'f');
            }
        }
    }

    w.write_all(&[b'D'])?;
    w.write_all(&((buf.len() + 4) as i32).to_be_bytes())?;
    w.write_all(buf)?;
    Ok(())
}

fn send_command_complete(w: &mut impl Write, tag: &str) -> io::Result<()> {
    let len = 4 + tag.len() + 1;
    w.write_all(&[b'C'])?;
    w.write_all(&(len as i32).to_be_bytes())?;
    w.write_all(tag.as_bytes())?;
    w.write_all(&[0])?;
    Ok(())
}

fn send_parse_complete(w: &mut impl Write) -> io::Result<()> {
    w.write_all(&[b'1'])?;
    w.write_all(&4_i32.to_be_bytes())?;
    Ok(())
}

fn send_bind_complete(w: &mut impl Write) -> io::Result<()> {
    w.write_all(&[b'2'])?;
    w.write_all(&4_i32.to_be_bytes())?;
    Ok(())
}

fn send_close_complete(w: &mut impl Write) -> io::Result<()> {
    w.write_all(&[b'3'])?;
    w.write_all(&4_i32.to_be_bytes())?;
    Ok(())
}

fn send_no_data(w: &mut impl Write) -> io::Result<()> {
    w.write_all(&[b'n'])?;
    w.write_all(&4_i32.to_be_bytes())?;
    Ok(())
}

fn send_parameter_description(w: &mut impl Write, type_oids: &[i32]) -> io::Result<()> {
    let len = 4 + 2 + type_oids.len() * 4;
    w.write_all(&[b't'])?;
    w.write_all(&(len as i32).to_be_bytes())?;
    w.write_all(&(type_oids.len() as i16).to_be_bytes())?;
    for oid in type_oids {
        w.write_all(&oid.to_be_bytes())?;
    }
    Ok(())
}

fn send_copy_in_response(w: &mut impl Write) -> io::Result<()> {
    w.write_all(&[b'G'])?;
    w.write_all(&7_i32.to_be_bytes())?;
    w.write_all(&[0])?; // overall text format
    w.write_all(&0_i16.to_be_bytes())?; // no per-column format codes
    Ok(())
}

fn send_empty_query(w: &mut impl Write) -> io::Result<()> {
    w.write_all(&[b'I'])?;
    w.write_all(&4_i32.to_be_bytes())?;
    Ok(())
}

fn send_error(w: &mut impl Write, sqlstate: &str, message: &str) -> io::Result<()> {
    let mut body = Vec::new();
    // Severity
    body.push(b'S');
    body.extend_from_slice(b"ERROR\0");
    // Non-localized severity
    body.push(b'V');
    body.extend_from_slice(b"ERROR\0");
    // SQLSTATE
    body.push(b'C');
    body.extend_from_slice(sqlstate.as_bytes());
    body.push(0);
    // Message
    body.push(b'M');
    body.extend_from_slice(message.as_bytes());
    body.push(0);
    // Terminator
    body.push(0);

    w.write_all(&[b'E'])?;
    w.write_all(&((body.len() + 4) as i32).to_be_bytes())?;
    w.write_all(&body)?;
    Ok(())
}


// ---- Wire protocol readers ----

fn read_byte(r: &mut impl Read) -> io::Result<u8> {
    let mut buf = [0u8; 1];
    r.read_exact(&mut buf)?;
    Ok(buf[0])
}

fn read_i32(r: &mut impl Read) -> io::Result<i32> {
    let mut buf = [0u8; 4];
    r.read_exact(&mut buf)?;
    Ok(i32::from_be_bytes(buf))
}

fn read_i16_bytes(bytes: &[u8], offset: &mut usize) -> io::Result<i16> {
    let end = *offset + 2;
    let slice = bytes
        .get(*offset..end)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "short i16 field"))?;
    *offset = end;
    Ok(i16::from_be_bytes(slice.try_into().unwrap()))
}

fn read_i32_bytes(bytes: &[u8], offset: &mut usize) -> io::Result<i32> {
    let end = *offset + 4;
    let slice = bytes
        .get(*offset..end)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "short i32 field"))?;
    *offset = end;
    Ok(i32::from_be_bytes(slice.try_into().unwrap()))
}

fn read_cstr(bytes: &[u8], offset: &mut usize) -> io::Result<String> {
    let start = *offset;
    let rel_end = bytes[start..]
        .iter()
        .position(|&b| b == 0)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "unterminated cstring"))?;
    let end = start + rel_end;
    *offset = end + 1;
    Ok(String::from_utf8_lossy(&bytes[start..end]).into_owned())
}

fn cstr_from_bytes(bytes: &[u8]) -> String {
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    String::from_utf8_lossy(&bytes[..end]).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::{Shutdown, TcpListener, TcpStream};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::thread;
    use std::time::Duration;

    static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(1);

    fn temp_dir(label: &str) -> std::path::PathBuf {
        let id = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("pgrust_server_test_{label}_{id}"));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    fn start_test_connection() -> (TcpStream, thread::JoinHandle<()>) {
        let db = Database::open(temp_dir("wire_copy"), 16).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            handle_connection(stream, &db, 1).unwrap();
        });

        let stream = TcpStream::connect(addr).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        (stream, server)
    }

    fn send_startup(stream: &mut TcpStream) {
        let mut body = Vec::new();
        body.extend_from_slice(&PROTOCOL_VERSION_3_0.to_be_bytes());
        body.extend_from_slice(b"user\0postgres\0database\0postgres\0\0");
        stream
            .write_all(&((body.len() + 4) as i32).to_be_bytes())
            .unwrap();
        stream.write_all(&body).unwrap();
        stream.flush().unwrap();
    }

    fn send_typed_message(stream: &mut TcpStream, kind: u8, body: &[u8]) {
        stream.write_all(&[kind]).unwrap();
        stream
            .write_all(&((body.len() + 4) as i32).to_be_bytes())
            .unwrap();
        stream.write_all(body).unwrap();
        stream.flush().unwrap();
    }

    fn send_query(stream: &mut TcpStream, sql: &str) {
        let mut body = sql.as_bytes().to_vec();
        body.push(0);
        send_typed_message(stream, b'Q', &body);
    }

    fn send_copy_data(stream: &mut TcpStream, data: &[u8]) {
        send_typed_message(stream, b'd', data);
    }

    fn send_copy_done(stream: &mut TcpStream) {
        send_typed_message(stream, b'c', &[]);
    }

    fn read_message(stream: &mut TcpStream, label: &str) -> (u8, Vec<u8>) {
        let mut kind = [0u8; 1];
        stream
            .read_exact(&mut kind)
            .unwrap_or_else(|e| panic!("{label}: failed reading kind: {e}"));
        let mut len = [0u8; 4];
        stream
            .read_exact(&mut len)
            .unwrap_or_else(|e| panic!("{label}: failed reading length: {e}"));
        let len = i32::from_be_bytes(len) as usize;
        let mut body = vec![0u8; len - 4];
        stream
            .read_exact(&mut body)
            .unwrap_or_else(|e| panic!("{label}: failed reading body for message '{}' len {len}: {e}", kind[0] as char));
        (kind[0], body)
    }

    fn read_until_ready(stream: &mut TcpStream, label: &str) -> Vec<(u8, Vec<u8>)> {
        let mut messages = Vec::new();
        loop {
            let msg = read_message(stream, label);
            let done = msg.0 == b'Z';
            messages.push(msg);
            if done {
                return messages;
            }
        }
    }

    fn command_tag(body: &[u8]) -> String {
        cstr_from_bytes(body)
    }

    fn data_row_values(body: &[u8]) -> Vec<Option<String>> {
        let mut offset = 0;
        let ncols = read_i16_bytes(body, &mut offset).unwrap() as usize;
        let mut values = Vec::with_capacity(ncols);
        for _ in 0..ncols {
            let len = read_i32_bytes(body, &mut offset).unwrap();
            if len < 0 {
                values.push(None);
            } else {
                let end = offset + len as usize;
                values.push(Some(String::from_utf8_lossy(&body[offset..end]).into_owned()));
                offset = end;
            }
        }
        values
    }

    #[test]
    fn copy_from_stdin_round_trips_over_wire_protocol() {
        let (mut stream, server) = start_test_connection();

        send_startup(&mut stream);
        let startup = read_until_ready(&mut stream, "startup");
        assert!(startup.iter().any(|(kind, _)| *kind == b'R'));
        assert!(matches!(startup.last(), Some((b'Z', _))));

        send_query(&mut stream, "create table t (id int, name text)");
        let create = read_until_ready(&mut stream, "create");
        assert_eq!(
            create
                .iter()
                .find(|(kind, _)| *kind == b'C')
                .map(|(_, body)| command_tag(body)),
            Some("CREATE TABLE".to_string())
        );

        send_query(&mut stream, "copy t from stdin");
        let copy_start = read_message(&mut stream, "copy_start");
        assert_eq!(copy_start.0, b'G');

        send_copy_data(&mut stream, b"1\talice\n");
        send_copy_done(&mut stream);
        let copy_finish = read_until_ready(&mut stream, "copy_finish");
        assert_eq!(
            copy_finish
                .iter()
                .find(|(kind, _)| *kind == b'C')
                .map(|(_, body)| command_tag(body)),
            Some("COPY".to_string())
        );

        send_query(&mut stream, "select id, name from t");
        let select = read_until_ready(&mut stream, "select");
        let rows = select
            .iter()
            .filter(|(kind, _)| *kind == b'D')
            .map(|(_, body)| data_row_values(body))
            .collect::<Vec<_>>();
        assert_eq!(rows, vec![vec![Some("1".into()), Some("alice".into())]]);
        assert_eq!(
            select
                .iter()
                .find(|(kind, _)| *kind == b'C')
                .map(|(_, body)| command_tag(body)),
            Some("SELECT 1".to_string())
        );

        stream.shutdown(Shutdown::Both).unwrap();
        server.join().unwrap();
    }

    #[test]
    fn copy_from_stdin_accepts_legacy_end_marker_before_copy_done() {
        let (mut stream, server) = start_test_connection();

        send_startup(&mut stream);
        let _ = read_until_ready(&mut stream, "startup");

        send_query(&mut stream, "create table t (id int, name text)");
        let _ = read_until_ready(&mut stream, "create");

        send_query(&mut stream, "copy t from stdin");
        let copy_start = read_message(&mut stream, "copy_start");
        assert_eq!(copy_start.0, b'G');

        send_copy_data(&mut stream, b"1\talice\n");
        send_copy_data(&mut stream, b"\\.\n");
        send_copy_done(&mut stream);
        let copy_finish = read_until_ready(&mut stream, "copy_finish");
        assert_eq!(
            copy_finish
                .iter()
                .find(|(kind, _)| *kind == b'C')
                .map(|(_, body)| command_tag(body)),
            Some("COPY".to_string())
        );

        send_query(&mut stream, "select id, name from t");
        let select = read_until_ready(&mut stream, "select");
        let rows = select
            .iter()
            .filter(|(kind, _)| *kind == b'D')
            .map(|(_, body)| data_row_values(body))
            .collect::<Vec<_>>();
        assert_eq!(rows, vec![vec![Some("1".into()), Some("alice".into())]]);

        stream.shutdown(Shutdown::Both).unwrap();
        server.join().unwrap();
    }
}
