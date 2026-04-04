use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicU32, Ordering};
use std::thread;

use crate::database::Database;
use crate::executor::{ExecError, StatementResult, Value};
use crate::ClientId;

const SSL_REQUEST_CODE: i32 = 80877103;
const PROTOCOL_VERSION_3_0: i32 = 196608;

static NEXT_CLIENT_ID: AtomicU32 = AtomicU32::new(1);

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
    mut stream: TcpStream,
    db: &Database,
    client_id: ClientId,
) -> io::Result<()> {
    // Phase 1: SSL / startup negotiation.
    // The first message has no type byte — just length + payload.
    loop {
        let len = read_i32(&mut stream)? as usize;
        if len < 4 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "startup packet too short"));
        }
        let mut payload = vec![0u8; len - 4];
        stream.read_exact(&mut payload)?;

        let code = i32::from_be_bytes(payload[0..4].try_into().unwrap());
        match code {
            SSL_REQUEST_CODE => {
                stream.write_all(b"N")?;
                stream.flush()?;
                continue;
            }
            PROTOCOL_VERSION_3_0 => {
                break;
            }
            _ => {
                let msg = format!("unsupported protocol version: {code}");
                send_error(&mut stream, "08P01", &msg)?;
                return Ok(());
            }
        }
    }

    // Phase 2: send authentication OK + startup parameters.
    send_auth_ok(&mut stream)?;
    send_parameter_status(&mut stream, "server_version", "16.0")?;
    send_parameter_status(&mut stream, "server_encoding", "UTF8")?;
    send_parameter_status(&mut stream, "client_encoding", "UTF8")?;
    send_parameter_status(&mut stream, "DateStyle", "ISO, MDY")?;
    send_parameter_status(&mut stream, "TimeZone", "UTC")?;
    send_parameter_status(&mut stream, "integer_datetimes", "on")?;
    send_parameter_status(&mut stream, "standard_conforming_strings", "on")?;
    send_backend_key_data(&mut stream, std::process::id() as i32, client_id as i32)?;
    send_ready_for_query(&mut stream, b'I')?;
    stream.flush()?;

    // Phase 3: query loop.
    loop {
        let msg_type = match read_byte(&mut stream) {
            Ok(b) => b,
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(e) => return Err(e),
        };

        let len = read_i32(&mut stream)? as usize;
        if len < 4 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "message too short"));
        }
        let mut body = vec![0u8; len - 4];
        stream.read_exact(&mut body)?;

        match msg_type {
            b'Q' => {
                let sql = cstr_from_bytes(&body);
                handle_query(&mut stream, db, client_id, &sql)?;
                stream.flush()?;
            }
            b'X' => {
                return Ok(());
            }
            _ => {
                send_error(
                    &mut stream,
                    "0A000",
                    &format!("unsupported message type: '{}'", msg_type as char),
                )?;
                send_ready_for_query(&mut stream, b'I')?;
                stream.flush()?;
            }
        }
    }
}

fn handle_query(
    stream: &mut TcpStream,
    db: &Database,
    client_id: ClientId,
    sql: &str,
) -> io::Result<()> {
    let sql = sql.trim().trim_end_matches(';').trim();
    if sql.is_empty() {
        send_empty_query(stream)?;
        send_ready_for_query(stream, b'I')?;
        return Ok(());
    }

    match db.execute(client_id, sql) {
        Ok(StatementResult::Query { column_names, rows }) => {
            send_row_description(stream, &column_names)?;
            for row in &rows {
                send_data_row(stream, row)?;
            }
            send_command_complete(stream, &format!("SELECT {}", rows.len()))?;
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

    send_ready_for_query(stream, b'I')?;
    Ok(())
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

fn send_data_row(w: &mut impl Write, values: &[Value]) -> io::Result<()> {
    let mut body = Vec::new();
    body.extend_from_slice(&(values.len() as i16).to_be_bytes());
    for val in values {
        match val {
            Value::Null => {
                body.extend_from_slice(&(-1_i32).to_be_bytes());
            }
            _ => {
                let text = render_value(val);
                body.extend_from_slice(&(text.len() as i32).to_be_bytes());
                body.extend_from_slice(text.as_bytes());
            }
        }
    }

    w.write_all(&[b'D'])?;
    w.write_all(&((body.len() + 4) as i32).to_be_bytes())?;
    w.write_all(&body)?;
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

fn render_value(val: &Value) -> String {
    match val {
        Value::Int32(v) => v.to_string(),
        Value::Text(v) => v.clone(),
        Value::Bool(true) => "t".to_string(),
        Value::Bool(false) => "f".to_string(),
        Value::Null => String::new(),
    }
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

fn cstr_from_bytes(bytes: &[u8]) -> String {
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    String::from_utf8_lossy(&bytes[..end]).into_owned()
}
