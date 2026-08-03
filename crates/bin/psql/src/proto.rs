//! PostgreSQL v3 wire-protocol client for psql: startup, auth (trust /
//! cleartext / md5 / SCRAM-SHA-256), simple query, extended query
//! (Parse/Bind/Describe/Execute/Sync), COPY framing, async messages
//! (ParameterStatus / NotificationResponse / NoticeResponse).
//!
//! Transports live behind the [`Transport`] trait: TCP and unix-domain
//! sockets natively; a raw-fd pair (host-provided pipes) for wasm32-wasip1
//! where WASI p1 has no sockets (increment 2 of the psql plan).

use std::collections::VecDeque;
use std::io::{Read, Write};

pub const PG_PROTOCOL_3_0: u32 = 3 << 16;

// Largest message length we accept from the server (the wire length is a
// SIGNED i32 that includes its own 4 bytes). PostgreSQL never sends a
// message anywhere near 2^30 (values are capped at 1GB), so anything
// larger — and anything below the 4-byte self-inclusive minimum, negative
// values included — is framing loss, not a big row. Mirrors the pgclient
// crate's hardening (fix/pgclient-frame-hardening).
const MAX_MESSAGE_LEN: i32 = 0x3FFF_FFFF;

// ---------------------------------------------------------------- transport

/// Blocking byte transport carrying the wire protocol. Implementations:
/// [`TcpTransport`], [`UnixTransport`] (native), [`FdTransport`] (raw fd
/// pair; the wasm arm where the host cross-connects psql to the server).
pub trait Transport {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize>;
    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()>;
}

pub struct TcpTransport(pub std::net::TcpStream);

impl Transport for TcpTransport {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.0.read(buf)
    }
    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        self.0.write_all(buf)
    }
}

#[cfg(unix)]
pub struct UnixTransport(pub std::os::unix::net::UnixStream);

#[cfg(unix)]
impl Transport for UnixTransport {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.0.read(buf)
    }
    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        self.0.write_all(buf)
    }
}

/// Raw-fd transport: reads from `rfd`, writes to `wfd`. On wasm32-wasip1 the
/// host provides these as preopened pipe fds (psql convention: fd 3 = read
/// from server, fd 4 = write to server); the server side runs
/// `postgres --stdio-wire` on its own stdin/stdout.
pub struct FdTransport {
    rfd: i32,
    wfd: i32,
}

impl FdTransport {
    pub fn new(rfd: i32, wfd: i32) -> Self {
        FdTransport { rfd, wfd }
    }
}

impl Transport for FdTransport {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        loop {
            let n = unsafe { libc::read(self.rfd, buf.as_mut_ptr().cast(), buf.len()) };
            if n >= 0 {
                return Ok(n as usize);
            }
            let e = std::io::Error::last_os_error();
            if e.kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            return Err(e);
        }
    }
    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        let mut off = 0;
        while off < buf.len() {
            let n = unsafe {
                libc::write(self.wfd, buf[off..].as_ptr().cast(), buf.len() - off)
            };
            if n >= 0 {
                off += n as usize;
                continue;
            }
            let e = std::io::Error::last_os_error();
            if e.kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            return Err(e);
        }
        Ok(())
    }
}

// ------------------------------------------------------------ wire plumbing

pub(crate) fn msg(t: u8, body: &[u8]) -> Vec<u8> {
    let mut m = Vec::with_capacity(5 + body.len());
    m.push(t);
    m.extend_from_slice(&((body.len() as u32 + 4).to_be_bytes()));
    m.extend_from_slice(body);
    m
}

pub(crate) fn be_i32(b: &[u8]) -> i32 {
    i32::from_be_bytes([b[0], b[1], b[2], b[3]])
}

pub(crate) fn be_i16(b: &[u8]) -> i16 {
    i16::from_be_bytes([b[0], b[1]])
}

pub(crate) fn cstr_at(b: &[u8], pos: usize) -> (String, usize) {
    // Server-controlled offsets: a body whose previous field ran to the end
    // without a NUL hands us pos == b.len() + 1; clamp instead of slicing
    // out of range.
    let pos = pos.min(b.len());
    let end = b[pos..].iter().position(|&c| c == 0).map(|e| pos + e).unwrap_or(b.len());
    (String::from_utf8_lossy(&b[pos..end]).into_owned(), end + 1)
}

// --------------------------------------------------------------- error diag

/// ErrorResponse / NoticeResponse fields (the PG_DIAG_* surface).
#[derive(Default, Clone)]
pub struct ErrorFields {
    pub severity: String,
    pub sqlstate: String,
    pub primary: String,
    pub detail: String,
    pub hint: String,
    pub position: String,
    pub internal_position: String,
    pub internal_query: String,
    pub context: String,
}

pub(crate) fn parse_diag(body: &[u8]) -> ErrorFields {
    let mut f = ErrorFields { severity: "ERROR".into(), ..Default::default() };
    let mut i = 0;
    while i < body.len() && body[i] != 0 {
        let code = body[i];
        let (val, next) = cstr_at(body, i + 1);
        match code {
            b'S' => f.severity = val,
            b'C' => f.sqlstate = val,
            b'M' => f.primary = val,
            b'D' => f.detail = val,
            b'H' => f.hint = val,
            b'P' => f.position = val,
            b'p' => f.internal_position = val,
            b'q' => f.internal_query = val,
            b'W' => f.context = val,
            _ => {}
        }
        i = next;
    }
    f
}

// ------------------------------------------------------------------ results

#[derive(Clone)]
pub struct Field {
    pub name: String,
    pub type_oid: u32,
    pub typmod: i32,
    pub format: i16,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ExecStatus {
    CommandOk,
    TuplesOk,
    CopyIn,
    CopyOut,
    Empty,
    Error,
}

pub struct QueryResult {
    pub status: ExecStatus,
    pub fields: Vec<Field>,
    pub rows: Vec<Vec<Option<Vec<u8>>>>,
    /// CommandComplete tag, e.g. "INSERT 0 1".
    pub cmd_tag: String,
    pub diag: Option<ErrorFields>,
    /// Connection-level error text when diag is absent.
    pub conn_err: String,
}

impl QueryResult {
    fn status_only(status: ExecStatus) -> Self {
        QueryResult {
            status,
            fields: Vec::new(),
            rows: Vec::new(),
            cmd_tag: String::new(),
            diag: None,
            conn_err: String::new(),
        }
    }

    pub fn conn_error(err: String) -> Self {
        QueryResult { conn_err: err, ..QueryResult::status_only(ExecStatus::Error) }
    }
}

pub struct Notify {
    pub channel: String,
    pub be_pid: i32,
    pub extra: String,
}

// --------------------------------------------------------------- connection

pub struct Conn {
    t: Box<dyn Transport>,
    inbuf: Vec<u8>,
    inpos: usize,
    dead: bool,
    pub params: Vec<(String, String)>,
    pub be_pid: i32,
    pub be_key: Vec<u8>,
    /// Last ReadyForQuery status: b'I' / b'T' / b'E'.
    pub txn_status: u8,
    pub notifies: VecDeque<Notify>,
    /// NoticeResponse sink (psql prints to stderr as they arrive).
    pub notice_hook: Box<dyn FnMut(&ErrorFields)>,
    /// PQconnectionUsedPassword.
    pub used_password: bool,
    /// True once a query is sent and ReadyForQuery not yet consumed.
    busy: bool,
    /// Inside a COPY (either direction).
    in_copy: bool,
}

pub enum CopyMsg {
    Data(Vec<u8>),
    Done,
    Error(ErrorFields),
}

impl Conn {
    pub fn new(t: Box<dyn Transport>) -> Self {
        Conn {
            t,
            inbuf: Vec::new(),
            inpos: 0,
            dead: false,
            params: Vec::new(),
            be_pid: 0,
            be_key: Vec::new(),
            txn_status: b'I',
            notifies: VecDeque::new(),
            notice_hook: Box::new(|_| {}),
            used_password: false,
            busy: false,
            in_copy: false,
        }
    }

    pub fn is_dead(&self) -> bool {
        self.dead
    }

    pub fn parameter_status(&self, name: &str) -> Option<&str> {
        self.params.iter().find(|(k, _)| k == name).map(|(_, v)| v.as_str())
    }

    pub fn server_version_num(&self) -> i32 {
        // Derive a version number from server_version ("18.3", "18beta1", ...).
        let v = self.parameter_status("server_version").unwrap_or("");
        let mut it = v.split(|c: char| !c.is_ascii_digit()).filter(|s| !s.is_empty());
        let major: i32 = it.next().and_then(|s| s.parse().ok()).unwrap_or(0);
        let minor: i32 = it.next().and_then(|s| s.parse().ok()).unwrap_or(0);
        major * 10000 + minor
    }

    pub(crate) fn send_raw(&mut self, buf: &[u8]) -> Result<(), String> {
        self.send_all(buf)
    }

    fn send_all(&mut self, buf: &[u8]) -> Result<(), String> {
        if self.dead {
            return Err("no connection to the server".into());
        }
        self.t.write_all(buf).map_err(|e| {
            self.dead = true;
            format!("could not send data to server: {e}")
        })
    }

    fn fill(&mut self) -> Result<(), String> {
        let mut buf = [0u8; 16384];
        let n = self.t.read(&mut buf).map_err(|e| {
            self.dead = true;
            format!("could not receive data from server: {e}")
        })?;
        if n == 0 {
            self.dead = true;
            return Err("server closed the connection unexpectedly\n\tThis probably means the server terminated abnormally\n\tbefore or while processing the request.".into());
        }
        self.inbuf.extend_from_slice(&buf[..n]);
        Ok(())
    }

    // Ok(None) = need more bytes; Err = the frame header is malformed. The
    // wire length is a SIGNED i32 the server controls: validate it BEFORE
    // it becomes a usize, or a negative/short value turns into an enormous
    // index and a panic. Once framing is lost there is no way to find the
    // next message boundary, so the connection is marked dead (libpq's
    // handleSyncLoss discipline).
    fn next_buffered(&mut self) -> Result<Option<(u8, Vec<u8>)>, String> {
        let avail = self.inbuf.len() - self.inpos;
        if avail < 5 {
            return Ok(None);
        }
        let p = self.inpos;
        let t = self.inbuf[p];
        let wire_len = be_i32(&self.inbuf[p + 1..p + 5]);
        if !(4..=MAX_MESSAGE_LEN).contains(&wire_len) {
            self.dead = true;
            return Err(format!(
                "lost synchronization with server: got message type \"{}\", length {wire_len}",
                t as char
            ));
        }
        let len = wire_len as usize;
        if avail < 1 + len {
            return Ok(None);
        }
        let body = self.inbuf[p + 5..p + 1 + len].to_vec();
        self.inpos = p + 1 + len;
        if self.inpos == self.inbuf.len() {
            self.inbuf.clear();
            self.inpos = 0;
        } else if self.inpos > 65536 {
            self.inbuf.drain(..self.inpos);
            self.inpos = 0;
        }
        Ok(Some((t, body)))
    }

    pub(crate) fn read_message(&mut self) -> Result<(u8, Vec<u8>), String> {
        loop {
            if let Some(m) = self.next_buffered()? {
                return Ok(m);
            }
            self.fill()?;
        }
    }

    /// A malformed message body (bad DataRow, short RowDescription) poisons
    /// the connection: after it, nothing on the wire can be trusted.
    fn proto_error(&mut self, e: String) -> String {
        self.dead = true;
        e
    }

    /// Async-message bookkeeping shared by every read loop.
    pub(crate) fn note_async(&mut self, t: u8, body: &[u8]) {
        match t {
            b'S' => {
                let (name, next) = cstr_at(body, 0);
                let (value, _) = cstr_at(body, next);
                if let Some(slot) = self.params.iter_mut().find(|(k, _)| *k == name) {
                    slot.1 = value;
                } else {
                    self.params.push((name, value));
                }
            }
            b'A' => {
                // NotificationResponse needs at least the 4-byte pid; a
                // shorter body is malformed — drop it rather than index
                // past the frame (losing a corrupt notify is harmless).
                let Some(pid_bytes) = body.get(0..4) else {
                    return;
                };
                let be_pid = be_i32(pid_bytes);
                let (channel, next) = cstr_at(body, 4);
                let (extra, _) = cstr_at(body, next);
                self.notifies.push_back(Notify { channel, be_pid, extra });
            }
            b'K' => {
                // BackendKeyData shorter than its 4-byte pid is malformed;
                // keep the zero pid/key (cancel then simply can't target).
                let Some(pid_bytes) = body.get(0..4) else {
                    return;
                };
                self.be_pid = be_i32(pid_bytes);
                self.be_key = body[4..].to_vec();
            }
            b'N' => {
                let diag = parse_diag(body);
                (self.notice_hook)(&diag);
            }
            _ => {}
        }
    }

    // ------------------------------------------------------------- startup

    /// Send StartupMessage. `params` = (user, database, application_name,
    /// client_encoding, ...).
    pub fn startup(
        &mut self,
        params: &[(&str, &str)],
        user: &str,
        password: Option<&str>,
    ) -> Result<(), String> {
        let mut body = Vec::new();
        body.extend_from_slice(&PG_PROTOCOL_3_0.to_be_bytes());
        for (k, v) in params {
            body.extend_from_slice(k.as_bytes());
            body.push(0);
            body.extend_from_slice(v.as_bytes());
            body.push(0);
        }
        body.push(0);
        let mut pkt = Vec::with_capacity(4 + body.len());
        pkt.extend_from_slice(&((body.len() as u32 + 4).to_be_bytes()));
        pkt.extend_from_slice(&body);
        self.send_all(&pkt)?;
        crate::auth::handshake(self, user, password)
    }

    // -------------------------------------------------------- simple query

    pub fn send_query(&mut self, query: &str) -> Result<(), String> {
        let mut body = query.as_bytes().to_vec();
        body.push(0);
        self.send_all(&msg(b'Q', &body))?;
        self.busy = true;
        Ok(())
    }

    /// PQgetResult-alike: one result per call, None once ReadyForQuery is
    /// consumed. CopyIn/CopyOut results leave the stream open; the caller
    /// drives it with copy_get_data / copy_put_data + copy_done.
    pub fn get_result(&mut self) -> Result<Option<QueryResult>, String> {
        if !self.busy && !self.in_copy {
            return Ok(None);
        }
        let mut fields: Vec<Field> = Vec::new();
        let mut rows: Vec<Vec<Option<Vec<u8>>>> = Vec::new();
        let mut got_desc = false;
        loop {
            let (t, body) = self.read_message()?;
            match t {
                b'T' => {
                    fields = match parse_row_description(&body) {
                        Ok(f) => f,
                        Err(e) => return Err(self.proto_error(e)),
                    };
                    got_desc = true;
                    rows.clear();
                }
                b'D' => match parse_data_row(&body) {
                    Ok(r) => rows.push(r),
                    Err(e) => return Err(self.proto_error(e)),
                },
                b'C' | b's' => {
                    let tag = if t == b'C' { cstr_at(&body, 0).0 } else { String::new() };
                    // A zero-column SELECT is still a tuples result: key off
                    // RowDescription, not the field count.
                    let status = if got_desc {
                        ExecStatus::TuplesOk
                    } else {
                        ExecStatus::CommandOk
                    };
                    return Ok(Some(QueryResult {
                        status,
                        fields,
                        rows,
                        cmd_tag: tag,
                        diag: None,
                        conn_err: String::new(),
                    }));
                }
                b'I' => return Ok(Some(QueryResult::status_only(ExecStatus::Empty))),
                b'E' => {
                    let diag = parse_diag(&body);
                    let mut r = QueryResult::status_only(ExecStatus::Error);
                    r.diag = Some(diag);
                    return Ok(Some(r));
                }
                b'G' => {
                    self.in_copy = true;
                    return Ok(Some(QueryResult::status_only(ExecStatus::CopyIn)));
                }
                b'H' => {
                    self.in_copy = true;
                    return Ok(Some(QueryResult::status_only(ExecStatus::CopyOut)));
                }
                b'W' => {
                    // CopyBoth: replication-only; treat as CopyOut for psql.
                    self.in_copy = true;
                    return Ok(Some(QueryResult::status_only(ExecStatus::CopyOut)));
                }
                b'd' => {} // stray CopyData outside copy driving: ignore
                b'c' => {}
                b'1' | b'2' | b'3' | b'n' | b't' => {}
                b'S' | b'N' | b'A' | b'K' => self.note_async(t, &body),
                b'Z' => {
                    self.txn_status = body.first().copied().unwrap_or(b'I');
                    self.busy = false;
                    self.in_copy = false;
                    return Ok(None);
                }
                other => {
                    self.dead = true;
                    return Err(format!(
                        "unexpected message type \"{}\" from server",
                        other as char
                    ));
                }
            }
        }
    }

    // ---------------------------------------------------------------- COPY

    /// During COPY OUT: next CopyData / CopyDone / ErrorResponse.
    pub fn copy_get_data(&mut self) -> Result<CopyMsg, String> {
        loop {
            let (t, body) = self.read_message()?;
            match t {
                b'd' => return Ok(CopyMsg::Data(body)),
                b'c' => {
                    self.in_copy = false;
                    return Ok(CopyMsg::Done);
                }
                b'E' => {
                    self.in_copy = false;
                    return Ok(CopyMsg::Error(parse_diag(&body)));
                }
                b'S' | b'N' | b'A' => self.note_async(t, &body),
                other => {
                    self.dead = true;
                    return Err(format!(
                        "unexpected message type \"{}\" during COPY",
                        other as char
                    ));
                }
            }
        }
    }

    /// During COPY IN.
    pub fn copy_put_data(&mut self, data: &[u8]) -> Result<(), String> {
        self.send_all(&msg(b'd', data))
    }

    pub fn copy_put_done(&mut self) -> Result<(), String> {
        self.in_copy = false;
        self.send_all(&msg(b'c', &[]))
    }

    pub fn copy_put_fail(&mut self, err: &str) -> Result<(), String> {
        self.in_copy = false;
        let mut b = err.as_bytes().to_vec();
        b.push(0);
        self.send_all(&msg(b'f', &b))
    }

    // ------------------------------------------------------ extended query

    /// PQsendQueryParams shape: Parse("") + Bind + Describe(portal) +
    /// Execute + Sync. Text params and text results.
    pub fn send_query_params(
        &mut self,
        query: &str,
        params: &[Option<&str>],
    ) -> Result<(), String> {
        let mut pkt = msg(b'P', &parse_body("", query, params.len()));
        pkt.extend_from_slice(&msg(b'B', &bind_body("", "", params)));
        pkt.extend_from_slice(&msg(b'D', b"P\0"));
        pkt.extend_from_slice(&msg(b'E', &execute_body("", 0)));
        pkt.extend_from_slice(&msg(b'S', &[]));
        self.send_all(&pkt)?;
        self.busy = true;
        Ok(())
    }

    /// Drain everything through ReadyForQuery (error-path cleanup).
    pub fn drain(&mut self) {
        if !self.busy || self.dead {
            return;
        }
        loop {
            match self.read_message() {
                Ok((b'Z', body)) => {
                    self.txn_status = body.first().copied().unwrap_or(b'I');
                    self.busy = false;
                    self.in_copy = false;
                    return;
                }
                Ok((t, body)) => self.note_async(t, &body),
                Err(_) => return,
            }
        }
    }

    pub fn terminate(&mut self) {
        let _ = self.send_all(&msg(b'X', &[]));
    }
}

// RowDescription ('T') body: the 2-byte field-count header and every
// per-field fixed block are server-controlled — bounds-check them all.
fn parse_row_description(body: &[u8]) -> Result<Vec<Field>, String> {
    const MALFORMED: &str = "insufficient data in \"T\" message";
    if body.len() < 2 {
        return Err(MALFORMED.to_string());
    }
    let nfields = u16::from_be_bytes([body[0], body[1]]) as usize;
    let mut fields = Vec::with_capacity(nfields.min((body.len() - 2) / 19 + 1));
    let mut p = 2;
    for _ in 0..nfields {
        if p >= body.len() {
            return Err(MALFORMED.to_string());
        }
        let (name, next) = cstr_at(body, p);
        p = next;
        let Some(fixed) = body.get(p..p + 18) else {
            return Err(MALFORMED.to_string());
        };
        let _table_oid = be_i32(&fixed[0..4]);
        let _col = be_i16(&fixed[4..6]);
        let type_oid = be_i32(&fixed[6..10]) as u32;
        let _typlen = be_i16(&fixed[10..12]);
        let typmod = be_i32(&fixed[12..16]);
        let format = be_i16(&fixed[16..18]);
        p += 18;
        fields.push(Field { name, type_oid, typmod, format });
    }
    Ok(fields)
}

// DataRow ('D') body: every count and length in it is server-controlled.
// Enforced here (matching the pgclient crate's hardening): the 2-byte
// column-count header is present; each column has its 4-byte length; a
// negative length is the NULL marker -1 and nothing else; a column never
// runs past the frame; and the frame has no trailing bytes after the last
// declared column (leftover data means we and the server disagree about
// the row shape — reject, don't guess).
pub(crate) fn parse_data_row(body: &[u8]) -> Result<Vec<Option<Vec<u8>>>, String> {
    const MALFORMED: &str = "insufficient data in \"D\" message";
    if body.len() < 2 {
        return Err(MALFORMED.to_string());
    }
    let ncols = u16::from_be_bytes([body[0], body[1]]) as usize;
    let mut cols = Vec::with_capacity(ncols.min((body.len() - 2) / 4 + 1));
    let mut p = 2usize;
    for _ in 0..ncols {
        let Some(hdr) = body.get(p..p + 4) else {
            return Err(MALFORMED.to_string());
        };
        let len = be_i32(hdr);
        p += 4;
        if len == -1 {
            cols.push(None);
            continue;
        }
        if len < 0 {
            return Err(format!("invalid column length {len} in \"D\" message"));
        }
        // len <= i32::MAX and p <= body.len(): p + len cannot overflow usize.
        let Some(val) = body.get(p..p + len as usize) else {
            return Err(MALFORMED.to_string());
        };
        cols.push(Some(val.to_vec()));
        p += len as usize;
    }
    if p != body.len() {
        return Err("extraneous data in \"D\" message".to_string());
    }
    Ok(cols)
}

// Parse ('P') body: unnamed statement, no explicit param types (server infers).
fn parse_body(stmt_name: &str, query: &str, nparams: usize) -> Vec<u8> {
    let mut b = Vec::with_capacity(stmt_name.len() + query.len() + 4 + 4 * nparams);
    b.extend_from_slice(stmt_name.as_bytes());
    b.push(0);
    b.extend_from_slice(query.as_bytes());
    b.push(0);
    b.extend_from_slice(&(nparams as u16).to_be_bytes());
    for _ in 0..nparams {
        b.extend_from_slice(&0u32.to_be_bytes());
    }
    b
}

fn bind_body(portal: &str, stmt_name: &str, params: &[Option<&str>]) -> Vec<u8> {
    let mut b = Vec::new();
    b.extend_from_slice(portal.as_bytes());
    b.push(0);
    b.extend_from_slice(stmt_name.as_bytes());
    b.push(0);
    b.extend_from_slice(&0u16.to_be_bytes()); // param format codes: none = all text
    b.extend_from_slice(&(params.len() as u16).to_be_bytes());
    for p in params {
        match p {
            None => b.extend_from_slice(&(-1i32).to_be_bytes()),
            Some(v) => {
                b.extend_from_slice(&(v.len() as u32).to_be_bytes());
                b.extend_from_slice(v.as_bytes());
            }
        }
    }
    b.extend_from_slice(&1u16.to_be_bytes()); // one result-format code...
    b.extend_from_slice(&0u16.to_be_bytes()); // ...text
    b
}

fn execute_body(portal: &str, maxrows: u32) -> Vec<u8> {
    let mut b = Vec::with_capacity(portal.len() + 5);
    b.extend_from_slice(portal.as_bytes());
    b.push(0);
    b.extend_from_slice(&maxrows.to_be_bytes());
    b
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- hostile-server framing / DataRow decoding ----
    //
    // Every byte below is something a broken or malicious server could put
    // on the wire; none may panic, all must surface as connection errors
    // (or be safely dropped where the message is advisory).

    /// In-memory transport: serves a canned server byte stream, then EOF.
    struct MockTransport {
        data: Vec<u8>,
        pos: usize,
    }

    impl Transport for MockTransport {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            let n = (self.data.len() - self.pos).min(buf.len());
            buf[..n].copy_from_slice(&self.data[self.pos..self.pos + n]);
            self.pos += n;
            Ok(n)
        }
        fn write_all(&mut self, _buf: &[u8]) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn conn_with(stream: Vec<u8>) -> Conn {
        Conn::new(Box::new(MockTransport { data: stream, pos: 0 }))
    }

    /// Wire message: type byte + self-inclusive length + body.
    fn wire(t: u8, body: &[u8]) -> Vec<u8> {
        msg(t, body)
    }

    // -------------------------------------------------- parse_data_row unit

    #[test]
    fn data_row_valid_roundtrip() {
        // 3 cols: "ab", NULL, "" (empty non-null).
        let mut body = vec![0, 3];
        body.extend_from_slice(&2i32.to_be_bytes());
        body.extend_from_slice(b"ab");
        body.extend_from_slice(&(-1i32).to_be_bytes());
        body.extend_from_slice(&0i32.to_be_bytes());
        let cols = parse_data_row(&body).unwrap();
        assert_eq!(cols, vec![Some(b"ab".to_vec()), None, Some(Vec::new())]);
    }

    #[test]
    fn data_row_zero_columns() {
        assert_eq!(parse_data_row(&[0, 0]).unwrap(), Vec::<Option<Vec<u8>>>::new());
    }

    #[test]
    fn data_row_missing_count_header() {
        for body in [&[][..], &[0][..]] {
            let e = parse_data_row(body).unwrap_err();
            assert!(e.contains("insufficient data"), "{e}");
        }
    }

    #[test]
    fn data_row_column_count_lies() {
        // Claims 3 columns, carries 1.
        let mut body = vec![0, 3];
        body.extend_from_slice(&1i32.to_be_bytes());
        body.push(b'x');
        let e = parse_data_row(&body).unwrap_err();
        assert!(e.contains("insufficient data"), "{e}");
    }

    #[test]
    fn data_row_column_length_exceeds_remaining() {
        // 1 column claiming 100 bytes; only 2 present.
        let mut body = vec![0, 1];
        body.extend_from_slice(&100i32.to_be_bytes());
        body.extend_from_slice(b"hi");
        let e = parse_data_row(&body).unwrap_err();
        assert!(e.contains("insufficient data"), "{e}");
    }

    #[test]
    fn data_row_huge_column_length_no_panic() {
        // i32::MAX length must not panic on the slice math or overflow.
        let mut body = vec![0, 1];
        body.extend_from_slice(&i32::MAX.to_be_bytes());
        let e = parse_data_row(&body).unwrap_err();
        assert!(e.contains("insufficient data"), "{e}");
    }

    #[test]
    fn data_row_negative_length_other_than_null_marker() {
        // -1 is NULL; any other negative is malformed, not a huge usize.
        let mut body = vec![0, 1];
        body.extend_from_slice(&(-2i32).to_be_bytes());
        let e = parse_data_row(&body).unwrap_err();
        assert!(e.contains("invalid column length"), "{e}");
    }

    #[test]
    fn data_row_truncated_length_word() {
        // Second column's 4-byte length word is cut short.
        let mut body = vec![0, 2];
        body.extend_from_slice(&1i32.to_be_bytes());
        body.push(b'x');
        body.extend_from_slice(&[0, 0]); // half a length word
        let e = parse_data_row(&body).unwrap_err();
        assert!(e.contains("insufficient data"), "{e}");
    }

    #[test]
    fn data_row_trailing_garbage() {
        // Correct single column followed by extra bytes.
        let mut body = vec![0, 1];
        body.extend_from_slice(&1i32.to_be_bytes());
        body.push(b'x');
        body.extend_from_slice(b"junk");
        let e = parse_data_row(&body).unwrap_err();
        assert!(e.contains("extraneous data"), "{e}");
    }

    // ------------------------------------------- parse_row_description unit

    #[test]
    fn row_description_short_header() {
        for body in [&[][..], &[0][..]] {
            let e = parse_row_description(body).err().expect("expected error");
            assert!(e.contains("insufficient data"), "{e}");
        }
    }

    #[test]
    fn row_description_truncated_fixed_block() {
        // One field: name + only 4 of the 18 fixed bytes.
        let mut body = vec![0, 1];
        body.extend_from_slice(b"col\0");
        body.extend_from_slice(&[0, 0, 0, 0]);
        let e = parse_row_description(&body).err().expect("expected error");
        assert!(e.contains("insufficient data"), "{e}");
    }

    #[test]
    fn row_description_field_count_lies() {
        // Claims 2 fields, carries 0 bytes of them.
        let e = parse_row_description(&[0, 2]).err().expect("expected error");
        assert!(e.contains("insufficient data"), "{e}");
    }

    #[test]
    fn row_description_valid() {
        let mut body = vec![0, 1];
        body.extend_from_slice(b"a\0");
        body.extend_from_slice(&0i32.to_be_bytes()); // table oid
        body.extend_from_slice(&0i16.to_be_bytes()); // col
        body.extend_from_slice(&25i32.to_be_bytes()); // type oid (text)
        body.extend_from_slice(&(-1i16).to_be_bytes()); // typlen
        body.extend_from_slice(&(-1i32).to_be_bytes()); // typmod
        body.extend_from_slice(&0i16.to_be_bytes()); // format
        let f = parse_row_description(&body).unwrap();
        assert_eq!(f.len(), 1);
        assert_eq!(f[0].name, "a");
        assert_eq!(f[0].type_oid, 25);
    }

    // ----------------------------------------------------- framing (Conn)

    #[test]
    fn framing_negative_length_is_sync_loss() {
        let mut stream = vec![b'D'];
        stream.extend_from_slice(&(-5i32).to_be_bytes());
        let mut c = conn_with(stream);
        let e = c.read_message().unwrap_err();
        assert!(e.contains("lost synchronization"), "{e}");
        assert!(c.is_dead());
    }

    #[test]
    fn framing_below_minimum_length_is_sync_loss() {
        // Wire length 3 < the 4-byte self-inclusive minimum.
        let mut stream = vec![b'Z'];
        stream.extend_from_slice(&3i32.to_be_bytes());
        let mut c = conn_with(stream);
        let e = c.read_message().unwrap_err();
        assert!(e.contains("lost synchronization"), "{e}");
        assert!(c.is_dead());
    }

    #[test]
    fn framing_huge_length_is_sync_loss() {
        let mut stream = vec![b'D'];
        stream.extend_from_slice(&i32::MAX.to_be_bytes());
        let mut c = conn_with(stream);
        let e = c.read_message().unwrap_err();
        assert!(e.contains("lost synchronization"), "{e}");
        assert!(c.is_dead());
    }

    #[test]
    fn framing_truncated_stream_is_connection_error() {
        // Valid header claiming 100 bytes; stream ends first (EOF).
        let mut stream = vec![b'D'];
        stream.extend_from_slice(&104i32.to_be_bytes());
        stream.extend_from_slice(b"partial");
        let mut c = conn_with(stream);
        let e = c.read_message().unwrap_err();
        assert!(e.contains("server closed the connection"), "{e}");
        assert!(c.is_dead());
    }

    // --------------------------------------------- get_result end-to-end

    /// RowDescription body with `n` text columns named c0..cn.
    fn row_desc_body(n: u16) -> Vec<u8> {
        let mut body = (n).to_be_bytes().to_vec();
        for i in 0..n {
            body.extend_from_slice(format!("c{i}\0").as_bytes());
            body.extend_from_slice(&0i32.to_be_bytes());
            body.extend_from_slice(&0i16.to_be_bytes());
            body.extend_from_slice(&25i32.to_be_bytes());
            body.extend_from_slice(&(-1i16).to_be_bytes());
            body.extend_from_slice(&(-1i32).to_be_bytes());
            body.extend_from_slice(&0i16.to_be_bytes());
        }
        body
    }

    #[test]
    fn get_result_malformed_data_row_is_error_not_panic() {
        // T (1 col) then a D whose column length lies past the frame.
        let mut bad_row = vec![0, 1];
        bad_row.extend_from_slice(&1000i32.to_be_bytes());
        bad_row.extend_from_slice(b"tiny");
        let mut stream = wire(b'T', &row_desc_body(1));
        stream.extend_from_slice(&wire(b'D', &bad_row));
        let mut c = conn_with(stream);
        c.send_query("SELECT 1").unwrap();
        let e = c.get_result().err().expect("expected error");
        assert!(e.contains("insufficient data in \"D\" message"), "{e}");
        assert!(c.is_dead());
    }

    #[test]
    fn get_result_short_row_description_is_error_not_panic() {
        let stream = wire(b'T', &[7]); // 1-byte body: no field count
        let mut c = conn_with(stream);
        c.send_query("SELECT 1").unwrap();
        let e = c.get_result().err().expect("expected error");
        assert!(e.contains("insufficient data in \"T\" message"), "{e}");
        assert!(c.is_dead());
    }

    #[test]
    fn get_result_well_formed_rows_still_parse() {
        let mut row = vec![0, 1];
        row.extend_from_slice(&2i32.to_be_bytes());
        row.extend_from_slice(b"42");
        let mut stream = wire(b'T', &row_desc_body(1));
        stream.extend_from_slice(&wire(b'D', &row));
        stream.extend_from_slice(&wire(b'C', b"SELECT 1\0"));
        stream.extend_from_slice(&wire(b'Z', b"I"));
        let mut c = conn_with(stream);
        c.send_query("SELECT 1").unwrap();
        let r = c.get_result().unwrap().unwrap();
        assert_eq!(r.status, ExecStatus::TuplesOk);
        assert_eq!(r.rows, vec![vec![Some(b"42".to_vec())]]);
        assert!(c.get_result().unwrap().is_none());
        assert!(!c.is_dead());
    }

    // -------------------------------------------------- async-message guards

    #[test]
    fn short_async_bodies_are_dropped_not_panicking() {
        let mut c = conn_with(Vec::new());
        c.note_async(b'A', &[1, 2]); // NotificationResponse: pid cut short
        assert!(c.notifies.is_empty());
        c.note_async(b'K', &[9]); // BackendKeyData: pid cut short
        assert_eq!(c.be_pid, 0);
        c.note_async(b'A', &[0, 0, 0, 7]); // pid only, no channel/extra
        assert_eq!(c.notifies.len(), 1);
        assert_eq!(c.notifies[0].be_pid, 7);
        assert_eq!(c.notifies[0].channel, "");
    }

    #[test]
    fn cstr_at_out_of_range_offset_clamps() {
        let b = b"abc"; // no NUL: cstr_at reports end + 1 == len + 1
        let (s, next) = cstr_at(b, 0);
        assert_eq!(s, "abc");
        let (s2, _) = cstr_at(b, next); // next == 4 > len: must not panic
        assert_eq!(s2, "");
    }

    #[test]
    fn parse_diag_truncated_field_no_panic() {
        // 'M' code then text with no NUL terminator, then EOF.
        let f = parse_diag(b"Mboom");
        assert_eq!(f.primary, "boom");
        assert_eq!(f.severity, "ERROR");
    }
}
