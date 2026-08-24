//! Minimal frontend-protocol-v3 client for the differential runner: startup
//! (trust auth only — harness servers are local), simple query, extended
//! query (Parse/Bind/Describe/Execute/Sync with text- or binary-format
//! parameters, text- or binary-format results, and portal
//! suspension/resume), COPY TO STDOUT capture + COPY FROM STDIN feeding
//! (the X2 COPY BINARY surface), and the per-statement capture the differ
//! needs (rows, column type OIDs, command tag, error SQLSTATE + message).
//! Blocking std::net::TcpStream, zero external deps. Modeled on
//! crates/bin/psql/src/proto.rs, which is not importable (bin-only crate).
//!
//! Binary-format results (X2): when Bind requests all-binary results, the
//! DataRow cells arrive as the server's *_send output — the differential
//! surface itself. Cells are decoded to strings ONLY as a transport into
//! the existing text-cell compare machinery, identically on both sides:
//!   - float4/float8 decode to the value's shortest-roundtrip text so the
//!     ruled float-ulp comparator (B1) still applies — a bit-level float
//!     difference within ulp tolerance stays ruled, exactly as in text
//!     mode, instead of flooding findings;
//!   - text-family types (text/varchar/bpchar/name/cstring) decode as
//!     UTF-8 — their binary wire form IS the text bytes, so string
//!     equality remains byte equality, and EXPLAIN counter masking keeps
//!     working on binary-mode EXPLAIN output;
//!   - every other type renders as `\x`-hex: cell equality is byte-for-byte
//!     *_send equality, any byte diff = divergence.
//!
//! The client is generic over its transport so the extended-protocol
//! exchange logic is unit-testable against canned byte streams; production
//! use is `Client<TcpStream>` via `connect`.

use std::io::{Read, Write};
use std::net::TcpStream;

// Wire length is a SIGNED i32 including its own 4 bytes; anything outside
// [4, 2^30) is framing loss, not a big row (psql/pgclient hardening).
const MAX_MESSAGE_LEN: i32 = 0x3FFF_FFFF;

/// One bind parameter on the wire: raw value bytes (None = NULL) plus its
/// format (false = text, true = binary). Encoding from typed values into
/// bytes lives in crate::xproto (pure, seeded); the client only frames.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WireParam {
    pub bytes: Option<Vec<u8>>,
    pub binary: bool,
}

impl WireParam {
    /// Text-format parameter from a string (None = NULL).
    pub fn text(v: Option<&str>) -> WireParam {
        WireParam { bytes: v.map(|s| s.as_bytes().to_vec()), binary: false }
    }
}

/// One resultset (or error) out of a query exchange.
#[derive(Clone, Debug)]
pub struct RawResult {
    /// Column type OIDs from RowDescription; empty for command results.
    pub col_oids: Vec<u32>,
    /// Cells as compare-ready strings; None = NULL. Text-format cells are
    /// the server text verbatim; binary-format cells are decoded per the
    /// module policy above.
    pub rows: Vec<Vec<Option<String>>>,
    /// CommandComplete tag, e.g. "SELECT 3", "UPDATE 2", "COPY 5".
    pub cmd_tag: String,
    /// ErrorResponse SQLSTATE + primary message, when this result is an error.
    pub error: Option<(String, String)>,
    /// Concatenated CopyData payload of a COPY TO STDOUT exchange.
    pub copy_out: Vec<u8>,
    /// True when this result came from a COPY data transfer (out or in):
    /// distinguishes an empty COPY payload from a plain command result.
    pub was_copy: bool,
}

impl RawResult {
    fn new() -> RawResult {
        RawResult {
            col_oids: Vec::new(),
            rows: Vec::new(),
            cmd_tag: String::new(),
            error: None,
            copy_out: Vec::new(),
            was_copy: false,
        }
    }
}

/// The connection died (I/O error, EOF, framing loss). Distinct from a
/// server ErrorResponse, which is a normal RawResult.
#[derive(Clone, Debug)]
pub struct ConnLost(pub String);

pub struct Client<S: Read + Write = TcpStream> {
    stream: S,
    buf: Vec<u8>,
    pos: usize,
    dead: Option<String>,
    /// Latest RowDescription: per-column (type oid, format code), so
    /// DataRow decoding knows how to render each cell.
    desc: Vec<(u32, i16)>,
    /// Latest ErrorResponse (SQLSTATE, primary message). A FATAL closes
    /// the connection before ReadyForQuery, so the error result itself
    /// is discarded — this copy lets the ConnLost detail name WHY the
    /// session died (round-7 FP-1: triage and the invalid-database
    /// residue ruling both need the FATAL message on the wire record).
    last_error: Option<(String, String)>,
}

fn be_i32(b: &[u8]) -> i32 {
    i32::from_be_bytes([b[0], b[1], b[2], b[3]])
}

fn be_u32(b: &[u8]) -> u32 {
    u32::from_be_bytes([b[0], b[1], b[2], b[3]])
}

fn be_i16(b: &[u8]) -> i16 {
    i16::from_be_bytes([b[0], b[1]])
}

fn msg(t: u8, body: &[u8]) -> Vec<u8> {
    let mut m = Vec::with_capacity(5 + body.len());
    m.push(t);
    m.extend_from_slice(&((body.len() as u32 + 4).to_be_bytes()));
    m.extend_from_slice(body);
    m
}

fn cstr_at(b: &[u8], pos: usize) -> (String, usize) {
    let pos = pos.min(b.len());
    let end = b[pos..].iter().position(|&c| c == 0).map(|e| pos + e).unwrap_or(b.len());
    (String::from_utf8_lossy(&b[pos..end]).into_owned(), end + 1)
}

/// ErrorResponse body -> (SQLSTATE, primary message).
fn parse_error(body: &[u8]) -> (String, String) {
    let mut sqlstate = String::new();
    let mut message = String::new();
    let mut i = 0;
    while i < body.len() && body[i] != 0 {
        let code = body[i];
        let (val, next) = cstr_at(body, i + 1);
        match code {
            b'C' => sqlstate = val,
            b'M' => message = val,
            _ => {}
        }
        i = next;
    }
    (sqlstate, message)
}

// ---------------------------------------------------------------------
// Binary-cell decoding (see module docs for the policy).
// ---------------------------------------------------------------------

/// Postgres-parseable text for a float value (crate::diff::parse_pg_float
/// accepts these spellings), shortest-roundtrip for finite values.
fn pg_float_text(neg_inf: bool, pos_inf: bool, nan: bool, finite: String) -> String {
    if nan {
        "NaN".to_string()
    } else if pos_inf {
        "Infinity".to_string()
    } else if neg_inf {
        "-Infinity".to_string()
    } else {
        finite
    }
}

fn hex_of(b: &[u8]) -> String {
    let mut s = String::with_capacity(2 + 2 * b.len());
    s.push_str("\\x");
    for byte in b {
        s.push_str(&format!("{byte:02x}"));
    }
    s
}

/// Decode one binary-format cell to its compare string.
pub fn decode_binary_cell(oid: u32, b: &[u8]) -> String {
    match oid {
        // float4: 4-byte IEEE big-endian. Shortest-roundtrip f32 text so
        // the f64-based ulp comparator sees exactly the value.
        700 if b.len() == 4 => {
            let f = f32::from_be_bytes([b[0], b[1], b[2], b[3]]);
            pg_float_text(
                f == f32::NEG_INFINITY,
                f == f32::INFINITY,
                f.is_nan(),
                format!("{f}"),
            )
        }
        // float8: 8-byte IEEE big-endian.
        701 if b.len() == 8 => {
            let f = f64::from_be_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]);
            pg_float_text(
                f == f64::NEG_INFINITY,
                f == f64::INFINITY,
                f.is_nan(),
                format!("{f}"),
            )
        }
        // Text family: binary form IS the text bytes (textsend/json_send).
        // Decoding as a string keeps byte equality AND keeps EXPLAIN
        // counter masking alive — including EXPLAIN (FORMAT JSON), whose
        // result column is json (114). 25 text, 1043 varchar, 1042 bpchar,
        // 19 name, 2275 cstring, 114 json, 142 xml.
        25 | 1043 | 1042 | 19 | 2275 | 114 | 142 => String::from_utf8_lossy(b).into_owned(),
        // Everything else: byte-for-byte *_send output as hex.
        _ => hex_of(b),
    }
}

// ---------------------------------------------------------------------
// Extended-protocol message encoders (pure; unit-tested below).
// ---------------------------------------------------------------------

/// Parse: unnamed statement, no pre-declared parameter type oids (the
/// server infers them from context, which is the surface we want).
fn parse_msg(sql: &str) -> Vec<u8> {
    let mut body = Vec::with_capacity(sql.len() + 4);
    body.push(0); // unnamed statement ""
    body.extend_from_slice(sql.as_bytes());
    body.push(0);
    body.extend_from_slice(&0i16.to_be_bytes()); // 0 parameter type oids
    msg(b'P', &body)
}

/// Bind: unnamed portal from the unnamed statement. Parameter format codes
/// are per-parameter when any is binary (else the 0-codes = all-text
/// shorthand, byte-identical to the X1 encoder); the result format is one
/// code — 1 = every column binary (the *_send surface), 0-codes = all text.
fn bind_msg(params: &[WireParam], result_binary: bool) -> Vec<u8> {
    let mut body = Vec::new();
    body.push(0); // unnamed portal ""
    body.push(0); // unnamed statement ""
    if params.iter().any(|p| p.binary) {
        body.extend_from_slice(&(params.len() as i16).to_be_bytes());
        for p in params {
            body.extend_from_slice(&(i16::from(p.binary)).to_be_bytes());
        }
    } else {
        body.extend_from_slice(&0i16.to_be_bytes()); // all-text shorthand
    }
    body.extend_from_slice(&(params.len() as i16).to_be_bytes());
    for p in params {
        match &p.bytes {
            None => body.extend_from_slice(&(-1i32).to_be_bytes()),
            Some(v) => {
                body.extend_from_slice(&(v.len() as i32).to_be_bytes());
                body.extend_from_slice(v);
            }
        }
    }
    if result_binary {
        body.extend_from_slice(&1i16.to_be_bytes());
        body.extend_from_slice(&1i16.to_be_bytes()); // all columns binary
    } else {
        body.extend_from_slice(&0i16.to_be_bytes()); // all-text results
    }
    msg(b'B', &body)
}

/// Describe the unnamed portal (yields RowDescription or NoData).
fn describe_portal_msg() -> Vec<u8> {
    msg(b'D', &[b'P', 0])
}

/// Execute the unnamed portal; `limit` 0 = run to completion.
fn execute_msg(limit: u32) -> Vec<u8> {
    let mut body = Vec::with_capacity(5);
    body.push(0); // unnamed portal ""
    body.extend_from_slice(&(limit as i32).to_be_bytes());
    msg(b'E', &body)
}

impl Client<TcpStream> {
    /// Dial + startup + trust auth, through the first ReadyForQuery. Any
    /// authentication demand beyond AuthenticationOk is refused — harness
    /// servers run trust.
    pub fn connect(host: &str, port: u16, db: &str, user: &str) -> Result<Client, ConnLost> {
        let stream = TcpStream::connect((host, port))
            .map_err(|e| ConnLost(format!("connect {host}:{port}: {e}")))?;
        let _ = stream.set_nodelay(true);
        let mut c = Client { stream, buf: Vec::new(), pos: 0, dead: None, desc: Vec::new(), last_error: None };

        let mut body = Vec::new();
        body.extend_from_slice(&(3u32 << 16).to_be_bytes());
        for (k, v) in [("user", user), ("database", db)] {
            body.extend_from_slice(k.as_bytes());
            body.push(0);
            body.extend_from_slice(v.as_bytes());
            body.push(0);
        }
        body.push(0);
        let mut pkt = Vec::with_capacity(4 + body.len());
        pkt.extend_from_slice(&((body.len() as u32 + 4).to_be_bytes()));
        pkt.extend_from_slice(&body);
        c.send(&pkt)?;

        loop {
            let (t, mbody) = c.read_message()?;
            match t {
                b'R' => {
                    let code = if mbody.len() >= 4 { be_i32(&mbody[0..4]) } else { -1 };
                    if code != 0 {
                        return Err(ConnLost(format!(
                            "server demanded authentication (code {code}); trust auth only"
                        )));
                    }
                }
                b'E' => {
                    let (state, message) = parse_error(&mbody);
                    return Err(ConnLost(format!("startup failed: {state} {message}")));
                }
                b'S' | b'K' | b'N' => {}
                b'Z' => return Ok(c),
                other => {
                    return Err(ConnLost(format!(
                        "unexpected message type \"{}\" during startup",
                        other as char
                    )))
                }
            }
        }
    }
}

impl<S: Read + Write> Client<S> {
    /// Wrap an already-authenticated transport (tests: canned byte streams).
    #[cfg(test)]
    fn from_stream(stream: S) -> Client<S> {
        Client { stream, buf: Vec::new(), pos: 0, dead: None, desc: Vec::new(), last_error: None }
    }

    /// Simple query: send one 'Q', collect every resultset through
    /// ReadyForQuery. Errors from the server come back as RawResults with
    /// `error` set; Err means the connection itself is gone. A COPY TO
    /// STDOUT exchange lands in the result's `copy_out` bytes; CopyIn is
    /// failed (use `copy_in` to feed data).
    pub fn simple_query(&mut self, sql: &str) -> Result<Vec<RawResult>, ConnLost> {
        self.simple_query_impl(sql, None)
    }

    /// Simple query that feeds `data` when the server enters COPY FROM
    /// STDIN mode (CopyData + CopyDone). The X2 COPY BINARY round-trip
    /// path. Statements that never enter CopyIn behave as `simple_query`.
    pub fn copy_in(&mut self, sql: &str, data: &[u8]) -> Result<Vec<RawResult>, ConnLost> {
        self.simple_query_impl(sql, Some(data))
    }

    fn simple_query_impl(
        &mut self,
        sql: &str,
        copy_payload: Option<&[u8]>,
    ) -> Result<Vec<RawResult>, ConnLost> {
        if let Some(d) = &self.dead {
            return Err(ConnLost(d.clone()));
        }
        // Per-exchange: only an error from THIS statement may annotate a
        // subsequent connection death.
        self.last_error = None;
        let mut qbody = sql.as_bytes().to_vec();
        qbody.push(0);
        self.send(&msg(b'Q', &qbody))?;

        let mut results = Vec::new();
        let mut cur = RawResult::new();
        loop {
            let (t, body) = self.read_message()?;
            match t {
                b'T' => {
                    cur = RawResult::new();
                    let desc = parse_row_description(&body).map_err(|e| self.poison(e))?;
                    cur.col_oids = desc.iter().map(|(oid, _)| *oid).collect();
                    self.desc = desc;
                }
                b'D' => {
                    let desc = self.desc.clone();
                    let row = parse_data_row(&body, &desc).map_err(|e| self.poison(e))?;
                    cur.rows.push(row);
                }
                b'C' => {
                    let (tag, _) = cstr_at(&body, 0);
                    cur.cmd_tag = tag;
                    results.push(std::mem::replace(&mut cur, RawResult::new()));
                }
                b'I' => {
                    results.push(std::mem::replace(&mut cur, RawResult::new()));
                }
                b'E' => {
                    let e = parse_error(&body);
                    self.last_error = Some(e.clone());
                    let mut r = RawResult::new();
                    r.error = Some(e);
                    results.push(r);
                    cur = RawResult::new();
                }
                b'G' => {
                    // CopyInResponse: feed the payload when we have one,
                    // else fail the copy so the exchange completes.
                    match copy_payload {
                        Some(data) => {
                            cur.was_copy = true;
                            let mut out = msg(b'd', data);
                            out.extend_from_slice(&msg(b'c', &[]));
                            self.send(&out)?;
                        }
                        None => {
                            let mut fbody =
                                b"diffrunner does not support COPY".to_vec();
                            fbody.push(0);
                            self.send(&msg(b'f', &fbody))?;
                        }
                    }
                }
                b'H' => {
                    // CopyOutResponse: a COPY TO STDOUT begins.
                    cur.was_copy = true;
                }
                b'd' => {
                    cur.was_copy = true;
                    cur.copy_out.extend_from_slice(&body);
                }
                b'c' | b'S' | b'A' | b'N' | b'K' => {}
                b'Z' => return Ok(results),
                other => {
                    return Err(self.poison(format!(
                        "unexpected message type \"{}\" from server",
                        other as char
                    )));
                }
            }
        }
    }

    /// Extended query: Parse/Bind/Describe(portal)/Execute over the unnamed
    /// statement + portal. Parameters carry per-parameter formats;
    /// `result_binary` asks Bind for all-binary results (decoded per the
    /// module policy — the *_send differential surface).
    ///
    /// `row_limit` > 0 asks Execute for at most that many rows; on
    /// PortalSuspended the portal is resumed with Execute(0) (the portal
    /// resume surface), so the returned rowset is always the complete one
    /// and compares identically to the simple-query path. Flush ('H') after
    /// every Execute forces the server to reveal suspension before we
    /// commit to Sync. Server errors come back as a RawResult with `error`
    /// set (the exchange is Sync'd to ReadyForQuery either way); Err means
    /// the connection itself is gone.
    pub fn extended_query(
        &mut self,
        sql: &str,
        params: &[WireParam],
        row_limit: u32,
        result_binary: bool,
    ) -> Result<Vec<RawResult>, ConnLost> {
        if let Some(d) = &self.dead {
            return Err(ConnLost(d.clone()));
        }
        self.last_error = None;
        let mut batch = parse_msg(sql);
        batch.extend_from_slice(&bind_msg(params, result_binary));
        batch.extend_from_slice(&describe_portal_msg());
        batch.extend_from_slice(&execute_msg(row_limit));
        batch.extend_from_slice(&msg(b'H', &[])); // Flush
        self.send(&batch)?;

        let mut cur = RawResult::new();
        let mut synced = false;
        loop {
            let (t, body) = self.read_message()?;
            match t {
                // ParseComplete / BindComplete / NoData / ParameterDescription
                // / ParameterStatus / NotificationResponse / NoticeResponse /
                // BackendKeyData: shape-only, nothing to capture.
                b'1' | b'2' | b'n' | b't' | b'S' | b'A' | b'N' | b'K' => {}
                b'T' => {
                    let desc = parse_row_description(&body).map_err(|e| self.poison(e))?;
                    cur.col_oids = desc.iter().map(|(oid, _)| *oid).collect();
                    self.desc = desc;
                }
                b'D' => {
                    let desc = self.desc.clone();
                    let row = parse_data_row(&body, &desc).map_err(|e| self.poison(e))?;
                    cur.rows.push(row);
                }
                b's' => {
                    // PortalSuspended: resume to completion (rows accumulate
                    // into the same logical result).
                    let mut cont = execute_msg(0);
                    cont.extend_from_slice(&msg(b'H', &[]));
                    self.send(&cont)?;
                }
                b'C' => {
                    let (tag, _) = cstr_at(&body, 0);
                    cur.cmd_tag = tag;
                    if !synced {
                        self.send(&msg(b'S', &[]))?;
                        synced = true;
                    }
                }
                b'I' => {
                    if !synced {
                        self.send(&msg(b'S', &[]))?;
                        synced = true;
                    }
                }
                b'E' => {
                    let e = parse_error(&body);
                    self.last_error = Some(e.clone());
                    if cur.error.is_none() {
                        cur.error = Some(e);
                    }
                    if !synced {
                        self.send(&msg(b'S', &[]))?;
                        synced = true;
                    }
                }
                b'G' => {
                    // CopyInResponse: never feed COPY data on the extended
                    // path; fail the copy (the resulting ErrorResponse
                    // closes the exchange).
                    let mut fbody = b"diffrunner does not support COPY".to_vec();
                    fbody.push(0);
                    self.send(&msg(b'f', &fbody))?;
                }
                b'H' => {
                    // CopyOutResponse (backend->frontend 'H' is CopyOut,
                    // distinct from the frontend Flush we send).
                    cur.was_copy = true;
                }
                b'd' => {
                    cur.was_copy = true;
                    cur.copy_out.extend_from_slice(&body);
                }
                b'c' => {}
                b'Z' => return Ok(vec![cur]),
                other => {
                    return Err(self.poison(format!(
                        "unexpected message type \"{}\" in extended exchange",
                        other as char
                    )));
                }
            }
        }
    }

    fn poison(&mut self, e: String) -> ConnLost {
        // A FATAL ErrorResponse closes the connection before ReadyForQuery,
        // so the death surfaces here as an EOF/read failure and the error
        // result is never returned. Stamp the last server error onto the
        // detail so classification and triage see WHY the session died
        // (round-7 FP-1 invalid-database residue ruling matches on it).
        let e = match &self.last_error {
            Some((state, message))
                if e.starts_with("server closed") || e.starts_with("could not read") =>
            {
                format!("{e} after server error {state}: {message}")
            }
            _ => e,
        };
        self.dead = Some(e.clone());
        ConnLost(e)
    }

    fn send(&mut self, buf: &[u8]) -> Result<(), ConnLost> {
        self.stream
            .write_all(buf)
            .map_err(|e| self.poison(format!("could not send to server: {e}")))
    }

    fn read_message(&mut self) -> Result<(u8, Vec<u8>), ConnLost> {
        loop {
            let avail = self.buf.len() - self.pos;
            if avail >= 5 {
                let p = self.pos;
                let t = self.buf[p];
                let wire_len = be_i32(&self.buf[p + 1..p + 5]);
                if !(4..=MAX_MESSAGE_LEN).contains(&wire_len) {
                    return Err(self.poison(format!(
                        "lost synchronization with server: message type \"{}\", length {wire_len}",
                        t as char
                    )));
                }
                let len = wire_len as usize;
                if avail > len {
                    let body = self.buf[p + 5..p + 1 + len].to_vec();
                    self.pos = p + 1 + len;
                    if self.pos == self.buf.len() {
                        self.buf.clear();
                        self.pos = 0;
                    } else if self.pos > 65536 {
                        self.buf.drain(..self.pos);
                        self.pos = 0;
                    }
                    return Ok((t, body));
                }
            }
            let mut chunk = [0u8; 16384];
            let n = self
                .stream
                .read(&mut chunk)
                .map_err(|e| self.poison(format!("could not read from server: {e}")))?;
            if n == 0 {
                return Err(self.poison("server closed the connection unexpectedly".to_string()));
            }
            self.buf.extend_from_slice(&chunk[..n]);
        }
    }
}

/// RowDescription -> per-column (type oid, format code). Format is 0 for
/// simple-query and text-Bind results, 1 for binary-Bind results.
fn parse_row_description(body: &[u8]) -> Result<Vec<(u32, i16)>, String> {
    if body.len() < 2 {
        return Err("short RowDescription".to_string());
    }
    let nfields = be_i16(&body[0..2]);
    if nfields < 0 {
        return Err("negative field count in RowDescription".to_string());
    }
    let mut cols = Vec::with_capacity(nfields as usize);
    let mut i = 2;
    for _ in 0..nfields {
        let (_name, next) = cstr_at(body, i);
        // table oid(4) + attnum(2) + type oid(4) + typlen(2) + typmod(4) +
        // format(2) = 18 fixed bytes after the name.
        if next + 18 > body.len() {
            return Err("short RowDescription field".to_string());
        }
        cols.push((be_u32(&body[next + 6..next + 10]), be_i16(&body[next + 16..next + 18])));
        i = next + 18;
    }
    Ok(cols)
}

/// DataRow -> compare-ready cells. `desc` supplies per-column (oid, format)
/// from the preceding RowDescription; columns beyond the description (which
/// a conforming server never sends) decode as text.
fn parse_data_row(
    body: &[u8],
    desc: &[(u32, i16)],
) -> Result<Vec<Option<String>>, String> {
    if body.len() < 2 {
        return Err("short DataRow".to_string());
    }
    let ncols = be_i16(&body[0..2]);
    if ncols < 0 {
        return Err("negative column count in DataRow".to_string());
    }
    let mut row = Vec::with_capacity(ncols as usize);
    let mut i = 2;
    for c in 0..ncols {
        if i + 4 > body.len() {
            return Err("short DataRow column header".to_string());
        }
        let len = be_i32(&body[i..i + 4]);
        i += 4;
        if len < 0 {
            row.push(None);
            continue;
        }
        let len = len as usize;
        if i + len > body.len() {
            return Err("DataRow column overruns frame".to_string());
        }
        let cell = &body[i..i + len];
        let (oid, fmt) = desc.get(c as usize).copied().unwrap_or((0, 0));
        row.push(Some(if fmt == 1 {
            decode_binary_cell(oid, cell)
        } else {
            String::from_utf8_lossy(cell).into_owned()
        }));
        i += len;
    }
    Ok(row)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_description_extracts_type_oids_and_formats() {
        // Two columns: "a" int4 (oid 23) text, "b" float8 (oid 701) binary.
        let mut body = vec![0, 2];
        for (name, oid, fmt) in [("a", 23u32, 0u16), ("b", 701u32, 1u16)] {
            body.extend_from_slice(name.as_bytes());
            body.push(0);
            body.extend_from_slice(&0u32.to_be_bytes()); // table oid
            body.extend_from_slice(&0u16.to_be_bytes()); // attnum
            body.extend_from_slice(&oid.to_be_bytes()); // type oid
            body.extend_from_slice(&8u16.to_be_bytes()); // typlen
            body.extend_from_slice(&u32::MAX.to_be_bytes()); // typmod
            body.extend_from_slice(&fmt.to_be_bytes()); // format
        }
        assert_eq!(parse_row_description(&body).unwrap(), vec![(23, 0), (701, 1)]);
    }

    #[test]
    fn data_row_nulls_and_text() {
        let mut body = vec![0, 3];
        body.extend_from_slice(&2u32.to_be_bytes());
        body.extend_from_slice(b"42");
        body.extend_from_slice(&(-1i32).to_be_bytes());
        body.extend_from_slice(&0u32.to_be_bytes());
        let desc = vec![(23u32, 0i16), (23, 0), (25, 0)];
        assert_eq!(
            parse_data_row(&body, &desc).unwrap(),
            vec![Some("42".to_string()), None, Some(String::new())]
        );
    }

    #[test]
    fn binary_cells_decode_per_policy() {
        // int4 -> hex (byte-for-byte compare surface).
        assert_eq!(decode_binary_cell(23, &42i32.to_be_bytes()), "\\x0000002a");
        // float8 -> shortest-roundtrip text (ruled ulp compare applies).
        assert_eq!(decode_binary_cell(701, &1.5f64.to_be_bytes()), "1.5");
        assert_eq!(
            decode_binary_cell(701, &(0.1f64 + 0.2f64).to_be_bytes()),
            "0.30000000000000004"
        );
        assert_eq!(decode_binary_cell(701, &f64::INFINITY.to_be_bytes()), "Infinity");
        assert_eq!(
            decode_binary_cell(701, &f64::NEG_INFINITY.to_be_bytes()),
            "-Infinity"
        );
        assert_eq!(decode_binary_cell(701, &f64::NAN.to_be_bytes()), "NaN");
        // float4.
        assert_eq!(decode_binary_cell(700, &1.25f32.to_be_bytes()), "1.25");
        assert_eq!(decode_binary_cell(700, &f32::NAN.to_be_bytes()), "NaN");
        // text family -> verbatim string (binary form IS the text bytes).
        assert_eq!(decode_binary_cell(25, b"alpha"), "alpha");
        assert_eq!(decode_binary_cell(1043, b""), "");
        // Unknown/other types -> hex.
        assert_eq!(decode_binary_cell(1700, &[0, 1, 0, 0, 0, 0, 0, 1, 0, 5]), "\\x00010000000000010005");
        // Malformed float width falls back to hex, not a panic.
        assert_eq!(decode_binary_cell(701, &[1, 2]), "\\x0102");
    }

    #[test]
    fn error_body_yields_sqlstate_and_message() {
        let body = b"SERROR\0C22012\0Mdivision by zero\0\0";
        let (state, message) = parse_error(body);
        assert_eq!(state, "22012");
        assert_eq!(message, "division by zero");
    }

    #[test]
    fn malformed_frames_rejected() {
        assert!(parse_data_row(&[0, 1, 0, 0], &[]).is_err());
        assert!(parse_row_description(&[0]).is_err());
    }

    // ------------------------------------------------------------------
    // Protocol exchanges against canned byte streams.
    // ------------------------------------------------------------------

    /// Fake transport: reads from a pre-canned server byte stream, records
    /// everything the client writes.
    struct FakeStream {
        input: std::io::Cursor<Vec<u8>>,
        written: Vec<u8>,
    }

    impl Read for FakeStream {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            self.input.read(buf)
        }
    }

    impl Write for FakeStream {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.written.extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn row_desc_body(cols: &[(u32, u16)]) -> Vec<u8> {
        let mut body = (cols.len() as i16).to_be_bytes().to_vec();
        for (i, (oid, fmt)) in cols.iter().enumerate() {
            body.extend_from_slice(format!("c{i}").as_bytes());
            body.push(0);
            body.extend_from_slice(&0u32.to_be_bytes()); // table oid
            body.extend_from_slice(&0u16.to_be_bytes()); // attnum
            body.extend_from_slice(&oid.to_be_bytes()); // type oid
            body.extend_from_slice(&4u16.to_be_bytes()); // typlen
            body.extend_from_slice(&u32::MAX.to_be_bytes()); // typmod
            body.extend_from_slice(&fmt.to_be_bytes()); // format
        }
        body
    }

    fn data_row_body(vals: &[&[u8]]) -> Vec<u8> {
        let mut body = (vals.len() as i16).to_be_bytes().to_vec();
        for v in vals {
            body.extend_from_slice(&(v.len() as u32).to_be_bytes());
            body.extend_from_slice(v);
        }
        body
    }

    fn ready() -> Vec<u8> {
        msg(b'Z', b"I")
    }

    fn complete(tag: &str) -> Vec<u8> {
        let mut cbody = tag.as_bytes().to_vec();
        cbody.push(0);
        msg(b'C', &cbody)
    }

    /// Count frontend messages of one type in the written byte stream.
    fn count_sent(written: &[u8], ty: u8) -> usize {
        let mut n = 0;
        let mut i = 0;
        while i + 5 <= written.len() {
            if written[i] == ty {
                n += 1;
            }
            let len = be_i32(&written[i + 1..i + 5]) as usize;
            i += 1 + len;
        }
        n
    }

    #[test]
    fn extended_query_merges_suspended_portal_batches() {
        // Server script: ParseComplete, BindComplete, RowDescription,
        // one row, PortalSuspended — then (after the resume Execute) the
        // second row, CommandComplete, ReadyForQuery.
        let mut input = Vec::new();
        input.extend_from_slice(&msg(b'1', &[]));
        input.extend_from_slice(&msg(b'2', &[]));
        input.extend_from_slice(&msg(b'T', &row_desc_body(&[(23, 0)])));
        input.extend_from_slice(&msg(b'D', &data_row_body(&[b"1"])));
        input.extend_from_slice(&msg(b's', &[]));
        input.extend_from_slice(&msg(b'D', &data_row_body(&[b"2"])));
        input.extend_from_slice(&complete("SELECT 1"));
        input.extend_from_slice(&ready());

        let mut c = Client::from_stream(FakeStream {
            input: std::io::Cursor::new(input),
            written: Vec::new(),
        });
        let results = c
            .extended_query("SELECT k FROM t;", &[WireParam::text(Some("7"))], 1, false)
            .unwrap();
        assert_eq!(results.len(), 1);
        let r = &results[0];
        assert!(r.error.is_none());
        assert_eq!(r.col_oids, vec![23]);
        assert_eq!(
            r.rows,
            vec![vec![Some("1".to_string())], vec![Some("2".to_string())]]
        );
        assert_eq!(r.cmd_tag, "SELECT 1");
        let w = &c.stream.written;
        assert_eq!(count_sent(w, b'P'), 1, "one Parse");
        assert_eq!(count_sent(w, b'B'), 1, "one Bind");
        assert_eq!(count_sent(w, b'E'), 2, "Execute + resume Execute");
        assert_eq!(count_sent(w, b'H'), 2, "Flush after each Execute");
        assert_eq!(count_sent(w, b'S'), 1, "one Sync");
    }

    #[test]
    fn extended_query_binary_results_decode() {
        // Binary result exchange: RowDescription reports format 1; cells
        // decode per policy (int4 hex, float8 text, text verbatim).
        let mut input = Vec::new();
        input.extend_from_slice(&msg(b'1', &[]));
        input.extend_from_slice(&msg(b'2', &[]));
        input.extend_from_slice(&msg(
            b'T',
            &row_desc_body(&[(23, 1), (701, 1), (25, 1)]),
        ));
        input.extend_from_slice(&msg(
            b'D',
            &data_row_body(&[&7i32.to_be_bytes(), &2.25f64.to_be_bytes(), b"hi"]),
        ));
        input.extend_from_slice(&complete("SELECT 1"));
        input.extend_from_slice(&ready());
        let mut c = Client::from_stream(FakeStream {
            input: std::io::Cursor::new(input),
            written: Vec::new(),
        });
        let results = c.extended_query("SELECT ...", &[], 0, true).unwrap();
        assert_eq!(
            results[0].rows,
            vec![vec![
                Some("\\x00000007".to_string()),
                Some("2.25".to_string()),
                Some("hi".to_string())
            ]]
        );
        // The Bind we sent requested one result-format code = 1.
        let w = &c.stream.written;
        let bind = bind_msg(&[], true);
        assert!(
            w.windows(bind.len()).any(|win| win == &bind[..]),
            "all-binary Bind not on the wire"
        );
    }

    #[test]
    fn extended_query_error_is_captured_and_synced() {
        let mut input = Vec::new();
        input.extend_from_slice(&msg(b'1', &[]));
        input.extend_from_slice(&msg(b'E', b"SERROR\0C22012\0Mdivision by zero\0\0"));
        input.extend_from_slice(&ready());
        let mut c = Client::from_stream(FakeStream {
            input: std::io::Cursor::new(input),
            written: Vec::new(),
        });
        let results = c.extended_query("SELECT 1/0;", &[], 0, false).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].error,
            Some(("22012".to_string(), "division by zero".to_string()))
        );
        assert_eq!(count_sent(&c.stream.written, b'S'), 1);
        // Only the initial Execute — no resume after an error.
        assert_eq!(count_sent(&c.stream.written, b'E'), 1);
    }

    #[test]
    fn extended_message_encoders_frame_correctly() {
        // Parse: 'P' | len | "" | sql | i16 0.
        let p = parse_msg("SELECT 1;");
        assert_eq!(p[0], b'P');
        assert_eq!(be_i32(&p[1..5]) as usize, p.len() - 1);
        assert_eq!(&p[5..], b"\0SELECT 1;\0\0\0");

        // Bind with one text param and one NULL: X1-identical framing
        // (0 format codes = all text) when nothing is binary.
        let b = bind_msg(
            &[WireParam::text(Some("42")), WireParam::text(None)],
            false,
        );
        assert_eq!(b[0], b'B');
        let body = &b[5..];
        // portal "" + stmt "" + 0 format codes + 2 params.
        assert_eq!(&body[0..2], &[0, 0]);
        assert_eq!(be_i16(&body[2..4]), 0);
        assert_eq!(be_i16(&body[4..6]), 2);
        assert_eq!(be_i32(&body[6..10]), 2);
        assert_eq!(&body[10..12], b"42");
        assert_eq!(be_i32(&body[12..16]), -1);
        assert_eq!(be_i16(&body[16..18]), 0); // all-text results

        // Mixed formats: per-parameter codes appear.
        let b = bind_msg(
            &[
                WireParam { bytes: Some(7i32.to_be_bytes().to_vec()), binary: true },
                WireParam::text(Some("x")),
            ],
            true,
        );
        let body = &b[5..];
        assert_eq!(&body[0..2], &[0, 0]);
        assert_eq!(be_i16(&body[2..4]), 2); // two format codes
        assert_eq!(be_i16(&body[4..6]), 1); // binary
        assert_eq!(be_i16(&body[6..8]), 0); // text
        assert_eq!(be_i16(&body[8..10]), 2); // two params
        assert_eq!(be_i32(&body[10..14]), 4);
        assert_eq!(&body[14..18], &7i32.to_be_bytes());
        assert_eq!(be_i32(&body[18..22]), 1);
        assert_eq!(&body[22..23], b"x");
        // Result formats: one code, binary.
        assert_eq!(be_i16(&body[23..25]), 1);
        assert_eq!(be_i16(&body[25..27]), 1);

        // Execute carries the row limit.
        let e = execute_msg(5);
        assert_eq!(e[0], b'E');
        assert_eq!(&e[5..], &[0, 0, 0, 0, 5]);

        // Describe targets the unnamed portal.
        assert_eq!(&describe_portal_msg()[5..], &[b'P', 0]);
    }

    #[test]
    fn simple_query_still_works_over_generic_stream() {
        let mut input = Vec::new();
        input.extend_from_slice(&msg(b'T', &row_desc_body(&[(23, 0)])));
        input.extend_from_slice(&msg(b'D', &data_row_body(&[b"9"])));
        input.extend_from_slice(&complete("SELECT 1"));
        input.extend_from_slice(&ready());
        let mut c = Client::from_stream(FakeStream {
            input: std::io::Cursor::new(input),
            written: Vec::new(),
        });
        let results = c.simple_query("SELECT 9;").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].rows, vec![vec![Some("9".to_string())]]);
        assert!(!results[0].was_copy);
        assert_eq!(count_sent(&c.stream.written, b'Q'), 1);
    }

    #[test]
    fn copy_out_payload_is_captured() {
        // COPY t TO STDOUT: CopyOutResponse, two CopyData chunks, CopyDone,
        // CommandComplete COPY 2, ReadyForQuery.
        let mut input = Vec::new();
        input.extend_from_slice(&msg(b'H', &[0, 0, 0])); // format 0, 0 cols
        input.extend_from_slice(&msg(b'd', b"PGCOPY\n"));
        input.extend_from_slice(&msg(b'd', b"\xff\x0d\x0a\x00"));
        input.extend_from_slice(&msg(b'c', &[]));
        input.extend_from_slice(&complete("COPY 2"));
        input.extend_from_slice(&ready());
        let mut c = Client::from_stream(FakeStream {
            input: std::io::Cursor::new(input),
            written: Vec::new(),
        });
        let results = c.simple_query("COPY t TO STDOUT (FORMAT binary);").unwrap();
        assert_eq!(results.len(), 1);
        let r = &results[0];
        assert!(r.was_copy);
        assert_eq!(r.copy_out, b"PGCOPY\n\xff\x0d\x0a\x00");
        assert_eq!(r.cmd_tag, "COPY 2");
        assert!(r.error.is_none());
    }

    #[test]
    fn copy_in_feeds_payload_and_completes() {
        // COPY t FROM STDIN: CopyInResponse, then (after our CopyData +
        // CopyDone) CommandComplete COPY 3, ReadyForQuery.
        let mut input = Vec::new();
        input.extend_from_slice(&msg(b'G', &[0, 0, 0]));
        input.extend_from_slice(&complete("COPY 3"));
        input.extend_from_slice(&ready());
        let mut c = Client::from_stream(FakeStream {
            input: std::io::Cursor::new(input),
            written: Vec::new(),
        });
        let payload = b"PGCOPY\n\xff\x0d\x0a\x00binarybytes";
        let results = c
            .copy_in("COPY t FROM STDIN (FORMAT binary);", payload)
            .unwrap();
        assert_eq!(results.len(), 1);
        assert!(results[0].was_copy);
        assert_eq!(results[0].cmd_tag, "COPY 3");
        let w = &c.stream.written;
        assert_eq!(count_sent(w, b'd'), 1, "one CopyData sent");
        assert_eq!(count_sent(w, b'c'), 1, "CopyDone sent");
        // The CopyData frame carries the payload verbatim.
        let want = msg(b'd', payload);
        assert!(w.windows(want.len()).any(|win| win == &want[..]));
    }

    #[test]
    fn copy_in_without_payload_is_failed() {
        let mut input = Vec::new();
        input.extend_from_slice(&msg(b'G', &[0, 0, 0]));
        input.extend_from_slice(&msg(b'E', b"SERROR\0C57014\0MCOPY failed\0\0"));
        input.extend_from_slice(&ready());
        let mut c = Client::from_stream(FakeStream {
            input: std::io::Cursor::new(input),
            written: Vec::new(),
        });
        let results = c.simple_query("COPY t FROM STDIN;").unwrap();
        assert_eq!(count_sent(&c.stream.written, b'f'), 1, "CopyFail sent");
        assert!(results.iter().any(|r| r.error.is_some()));
    }
}
