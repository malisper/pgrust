//! Full-field frontend-protocol-v3 client for the sitediff differential
//! (plan v2 §3.1 row `client.rs`, lane L0.1).
//!
//! Every backend message is decoded and RETAINED as a
//! `contracts::WireMsg` — the ObservationRecord `wire[]` input. Nothing is
//! dropped and nothing is stringified: ErrorResponse/NoticeResponse keep
//! every field code (S V C M D H P p q W s t c d n F L R) as raw bytes,
//! RowDescription keeps name/tableoid/attnum/typoid/typlen/typmod/fmt,
//! DataRow cells are bytes, CommandComplete is the full tag text,
//! ParameterStatus / NotificationResponse / BackendKeyData /
//! ParameterDescription / NegotiateProtocolVersion / COPY frames all
//! survive, and anything the decoder does not model structurally (or a
//! structurally malformed frame) lands in `WireMsg::Raw` verbatim.
//!
//! Surface:
//!   - StartupMessage with arbitrary extra keys (`options`,
//!     `application_name`, `client_encoding`, `replication`, `_pq_.*`) and
//!     protocol version 3.0 or 3.2 (NegotiateProtocolVersion observed and
//!     recorded; the negotiated minor is kept on the client).
//!   - Auth ladder: trust, cleartext password, md5, SCRAM-SHA-256 (RFC 5802
//!     / 7677, no channel binding — the psql client implementation,
//!     re-hosted on the workspace `pg_sha2` + `pg_md5` ports). SASLprep is
//!     not applied: harness passwords are ASCII, on which SASLprep is the
//!     identity; a non-ASCII password is sent as its UTF-8 bytes.
//!   - Named statements and portals, Describe S/P, pipelining (a batch of
//!     frames then a single Sync), COPY both ways, CancelRequest over a
//!     second socket, Terminate, and a raw-frame send for the protocol
//!     mutator.
//!   - `set_read_timeout` / `set_write_timeout` on every socket; a deadline
//!     hit is reported as `Fault::Hang` (never a block-forever) and the
//!     messages received so far are kept.
//!
//! Every exchange returns an `Exchange { wire, fault, ms }`. The pre-L0.1
//! `RawResult` adapters (`simple_query`, `copy_in`, `extended_query`,
//! `connect`) are thin folds over the same decode so runner.rs, xproto.rs,
//! ssi.rs, diffrunner and covapply compile unchanged; L0.3 switches the
//! consumers to `Exchange`. The adapters are the ONLY place bytes become
//! strings (the old compare machinery is string-typed); the binary-cell
//! policy they apply (float → shortest-roundtrip text, text family →
//! UTF-8, else `\x`-hex) is documented on `decode_binary_cell`.
//!
//! The client is generic over its transport so every exchange, the whole
//! startup handshake and every auth rung are unit-tested against canned
//! byte streams; production use is `Client<TcpStream>` via `connect_with`.

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::{Duration, Instant};

pub use crate::contracts::{Bytes, ColDesc, ErrFields, WireMsg};

// Wire length is a SIGNED i32 including its own 4 bytes; anything outside
// [4, 2^30) is framing loss, not a big row (psql/pgclient hardening).
const MAX_MESSAGE_LEN: i32 = 0x3FFF_FFFF;

/// CancelRequest pseudo-version (80877102).
pub const CANCEL_REQUEST_CODE: u32 = (1234 << 16) | 5678;

// ---------------------------------------------------------------------
// Byte helpers
// ---------------------------------------------------------------------

fn be_i32(b: &[u8]) -> i32 {
    i32::from_be_bytes([b[0], b[1], b[2], b[3]])
}

fn be_u32(b: &[u8]) -> u32 {
    u32::from_be_bytes([b[0], b[1], b[2], b[3]])
}

fn be_i16(b: &[u8]) -> i16 {
    i16::from_be_bytes([b[0], b[1]])
}

/// Frame one typed message: type byte, i32 length (self-inclusive), body.
pub fn msg(t: u8, body: &[u8]) -> Vec<u8> {
    let mut m = Vec::with_capacity(5 + body.len());
    m.push(t);
    m.extend_from_slice(&((body.len() as u32 + 4).to_be_bytes()));
    m.extend_from_slice(body);
    m
}

/// NUL-terminated byte string at `pos`: (bytes without the NUL, index past
/// the NUL). An unterminated tail yields the tail and `len + 1`.
fn cstr_bytes(b: &[u8], pos: usize) -> (&[u8], usize) {
    let pos = pos.min(b.len());
    let end = b[pos..].iter().position(|&c| c == 0).map(|e| pos + e).unwrap_or(b.len());
    (&b[pos..end], end + 1)
}

fn push_cstr(out: &mut Vec<u8>, s: &[u8]) {
    out.extend_from_slice(s);
    out.push(0);
}

fn lossy(b: &[u8]) -> String {
    String::from_utf8_lossy(b).into_owned()
}

// ---------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------

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

/// One resultset (or error) out of a query exchange — the pre-L0.1
/// string-typed shape the old callers consume (see `fold_simple`).
#[derive(Clone, Debug)]
pub struct RawResult {
    /// Column type OIDs from RowDescription; empty for command results.
    pub col_oids: Vec<u32>,
    /// Cells as compare-ready strings; None = NULL. Text-format cells are
    /// the server text verbatim; binary-format cells are decoded per the
    /// `decode_binary_cell` policy.
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

/// The connection died (I/O error, EOF, framing loss, or — through the
/// adapters — a read deadline). Distinct from a server ErrorResponse,
/// which is a normal wire message.
#[derive(Clone, Debug)]
pub struct ConnLost(pub String);

/// Why an exchange ended before its ReadyForQuery.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Fault {
    /// I/O error, EOF, or framing loss; the connection is dead afterwards.
    Lost(String),
    /// A read or write deadline hit. The connection is NOT poisoned: the
    /// caller may `cancel()` and then `drain()` to a ReadyForQuery, or
    /// `terminate()`.
    Hang { timeout_ms: u64 },
}

impl Fault {
    /// The old ConnLost detail string for a fault.
    pub fn detail(&self) -> String {
        match self {
            Fault::Lost(s) => s.clone(),
            Fault::Hang { timeout_ms } => format!("read timed out after {timeout_ms} ms (hang)"),
        }
    }
}

/// One exchange's observation: every backend message in order (the
/// ObservationRecord `wire[]`), the fault that ended it early (if any),
/// and the wall time.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Exchange {
    pub wire: Vec<WireMsg>,
    pub fault: Option<Fault>,
    pub ms: u64,
}

impl Exchange {
    /// Every ErrorResponse in the exchange, in order.
    pub fn errors(&self) -> Vec<&ErrFields> {
        self.wire
            .iter()
            .filter_map(|m| match m {
                WireMsg::ErrorResponse(f) => Some(f),
                _ => None,
            })
            .collect()
    }

    /// Every NoticeResponse in the exchange, in order.
    pub fn notices(&self) -> Vec<&ErrFields> {
        self.wire
            .iter()
            .filter_map(|m| match m {
                WireMsg::NoticeResponse(f) => Some(f),
                _ => None,
            })
            .collect()
    }

    /// The first ErrorResponse's SQLSTATE (field C) as text, if any.
    pub fn first_sqlstate(&self) -> Option<String> {
        self.errors().first().and_then(|f| f.get(&'C')).map(|b| lossy(&b.0))
    }

    /// True when the exchange reached ReadyForQuery with no fault.
    pub fn completed(&self) -> bool {
        self.fault.is_none()
    }
}

/// Frontend protocol version carried in the StartupMessage.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Protocol {
    V3_0,
    V3_2,
}

impl Protocol {
    pub fn minor(self) -> u32 {
        match self {
            Protocol::V3_0 => 0,
            Protocol::V3_2 => 2,
        }
    }
    pub fn code(self) -> u32 {
        (3 << 16) | self.minor()
    }
}

/// Everything a `@connect` step may vary (plan §5.6 / §6 hba axis).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConnectOpts {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub database: String,
    /// Required for the password/md5/scram rungs; a demand without one
    /// fails the connect with the demand recorded in the wire.
    pub password: Option<String>,
    pub protocol: Protocol,
    /// Extra StartupMessage keys in send order (`options`,
    /// `application_name`, `client_encoding`, `replication`, `_pq_.*`,
    /// ...). `user` and `database` always go first.
    pub extra: Vec<(String, String)>,
    pub connect_timeout: Option<Duration>,
    pub read_timeout: Option<Duration>,
    pub write_timeout: Option<Duration>,
    /// Test hook: a fixed SCRAM client nonce (base64 text) instead of a
    /// random one, so a canned SCRAM transcript can be byte-exact.
    pub scram_nonce: Option<String>,
}

impl ConnectOpts {
    pub fn new(host: &str, port: u16, database: &str, user: &str) -> ConnectOpts {
        ConnectOpts {
            host: host.to_string(),
            port,
            user: user.to_string(),
            database: database.to_string(),
            password: None,
            protocol: Protocol::V3_0,
            extra: Vec::new(),
            connect_timeout: None,
            read_timeout: None,
            write_timeout: None,
            scram_nonce: None,
        }
    }

    pub fn password(mut self, pw: &str) -> ConnectOpts {
        self.password = Some(pw.to_string());
        self
    }

    pub fn protocol(mut self, p: Protocol) -> ConnectOpts {
        self.protocol = p;
        self
    }

    pub fn param(mut self, key: &str, value: &str) -> ConnectOpts {
        self.extra.push((key.to_string(), value.to_string()));
        self
    }

    pub fn timeouts(mut self, connect: Option<Duration>, read: Option<Duration>, write: Option<Duration>) -> ConnectOpts {
        self.connect_timeout = connect;
        self.read_timeout = read;
        self.write_timeout = write;
        self
    }
}

/// A failed connect: the detail plus every message received before the
/// failure (the first ErrorResponse before AuthenticationOk is itself a
/// differential surface, plan §5.6) and whether a deadline was the cause.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConnectError {
    pub detail: String,
    pub wire: Vec<WireMsg>,
    pub hang: bool,
}

impl ConnectError {
    fn from_fault(f: Fault, wire: Vec<WireMsg>) -> ConnectError {
        ConnectError { detail: f.detail(), hang: matches!(f, Fault::Hang { .. }), wire }
    }
}

/// Describe target.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Describe {
    Statement,
    Portal,
}

impl Describe {
    fn byte(self) -> u8 {
        match self {
            Describe::Statement => b'S',
            Describe::Portal => b'P',
        }
    }
}

/// One frontend frame. `encode` is pure; `Client::pipeline` sends a batch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Frame {
    /// 'Q'
    Query(String),
    /// 'P' — statement name ("" = unnamed), SQL, pre-declared parameter
    /// type oids (0 = let the server infer).
    Parse { stmt: String, sql: String, param_oids: Vec<u32> },
    /// 'B' — portal, statement, parameters (per-parameter formats), result
    /// format codes (`[]` = all text, `[1]` = all binary, else per column).
    Bind { portal: String, stmt: String, params: Vec<WireParam>, result_fmts: Vec<i16> },
    /// 'D'
    Describe { kind: Describe, name: String },
    /// 'E' — `limit` 0 = run to completion.
    Execute { portal: String, limit: u32 },
    /// 'C'
    Close { kind: Describe, name: String },
    /// 'H'
    Flush,
    /// 'S'
    Sync,
    /// 'd'
    CopyData(Vec<u8>),
    /// 'c'
    CopyDone,
    /// 'f'
    CopyFail(String),
    /// 'X'
    Terminate,
    /// Bytes sent verbatim (the protocol mutator's malformed frames).
    Raw(Vec<u8>),
}

impl Frame {
    pub fn encode(&self) -> Vec<u8> {
        match self {
            Frame::Query(sql) => {
                let mut body = Vec::with_capacity(sql.len() + 1);
                push_cstr(&mut body, sql.as_bytes());
                msg(b'Q', &body)
            }
            Frame::Parse { stmt, sql, param_oids } => {
                let mut body = Vec::with_capacity(stmt.len() + sql.len() + 4 + 4 * param_oids.len());
                push_cstr(&mut body, stmt.as_bytes());
                push_cstr(&mut body, sql.as_bytes());
                body.extend_from_slice(&(param_oids.len() as i16).to_be_bytes());
                for oid in param_oids {
                    body.extend_from_slice(&oid.to_be_bytes());
                }
                msg(b'P', &body)
            }
            Frame::Bind { portal, stmt, params, result_fmts } => {
                let mut body = Vec::new();
                push_cstr(&mut body, portal.as_bytes());
                push_cstr(&mut body, stmt.as_bytes());
                // Parameter format codes: per-parameter when any is binary,
                // else the 0-codes = all-text shorthand (byte-identical to
                // the X1 encoder).
                if params.iter().any(|p| p.binary) {
                    body.extend_from_slice(&(params.len() as i16).to_be_bytes());
                    for p in params {
                        body.extend_from_slice(&(i16::from(p.binary)).to_be_bytes());
                    }
                } else {
                    body.extend_from_slice(&0i16.to_be_bytes());
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
                body.extend_from_slice(&(result_fmts.len() as i16).to_be_bytes());
                for f in result_fmts {
                    body.extend_from_slice(&f.to_be_bytes());
                }
                msg(b'B', &body)
            }
            Frame::Describe { kind, name } => {
                let mut body = vec![kind.byte()];
                push_cstr(&mut body, name.as_bytes());
                msg(b'D', &body)
            }
            Frame::Execute { portal, limit } => {
                let mut body = Vec::with_capacity(portal.len() + 5);
                push_cstr(&mut body, portal.as_bytes());
                body.extend_from_slice(&(*limit as i32).to_be_bytes());
                msg(b'E', &body)
            }
            Frame::Close { kind, name } => {
                let mut body = vec![kind.byte()];
                push_cstr(&mut body, name.as_bytes());
                msg(b'C', &body)
            }
            Frame::Flush => msg(b'H', &[]),
            Frame::Sync => msg(b'S', &[]),
            Frame::CopyData(d) => msg(b'd', d),
            Frame::CopyDone => msg(b'c', &[]),
            Frame::CopyFail(reason) => {
                let mut body = Vec::with_capacity(reason.len() + 1);
                push_cstr(&mut body, reason.as_bytes());
                msg(b'f', &body)
            }
            Frame::Terminate => msg(b'X', &[]),
            Frame::Raw(b) => b.clone(),
        }
    }

    /// True for frames after which the server emits one ReadyForQuery.
    fn yields_ready(&self) -> bool {
        matches!(self, Frame::Query(_) | Frame::Sync)
    }
}

/// One extended-protocol step (StepRecord `xproto {mode, params, stmt,
/// portal, limit, describe}`), driven to its ReadyForQuery by
/// `Client::extended`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExtendedStep {
    pub sql: String,
    /// Statement name ("" = unnamed).
    pub stmt: String,
    /// Portal name ("" = unnamed).
    pub portal: String,
    /// Pre-declared parameter type oids for Parse (0 = infer).
    pub param_oids: Vec<u32>,
    pub params: Vec<WireParam>,
    /// Bind result format codes (`[]` all text, `[1]` all binary, else
    /// per column).
    pub result_fmts: Vec<i16>,
    /// Describe S (sent between Parse and Bind), Describe P (after Bind),
    /// or none.
    pub describe: Option<Describe>,
    /// Execute row limit; 0 = run to completion.
    pub limit: u32,
    /// On PortalSuspended, resume with Execute(limit 0) so the rowset is
    /// complete; false leaves the portal suspended and Syncs.
    pub resume: bool,
    /// Skip Parse — the named statement was prepared by an earlier step.
    pub skip_parse: bool,
    /// Payload to feed when the server answers CopyInResponse; None fails
    /// the copy.
    pub copy_in: Option<Vec<u8>>,
}

impl ExtendedStep {
    /// Unnamed statement + portal, Describe P, run to completion.
    pub fn new(sql: &str) -> ExtendedStep {
        ExtendedStep {
            sql: sql.to_string(),
            stmt: String::new(),
            portal: String::new(),
            param_oids: Vec::new(),
            params: Vec::new(),
            result_fmts: Vec::new(),
            describe: Some(Describe::Portal),
            limit: 0,
            resume: true,
            skip_parse: false,
            copy_in: None,
        }
    }

    fn frames(&self) -> Vec<Frame> {
        let mut out = Vec::with_capacity(6);
        if !self.skip_parse {
            out.push(Frame::Parse {
                stmt: self.stmt.clone(),
                sql: self.sql.clone(),
                param_oids: self.param_oids.clone(),
            });
        }
        if self.describe == Some(Describe::Statement) {
            out.push(Frame::Describe { kind: Describe::Statement, name: self.stmt.clone() });
        }
        out.push(Frame::Bind {
            portal: self.portal.clone(),
            stmt: self.stmt.clone(),
            params: self.params.clone(),
            result_fmts: self.result_fmts.clone(),
        });
        if self.describe == Some(Describe::Portal) {
            out.push(Frame::Describe { kind: Describe::Portal, name: self.portal.clone() });
        }
        out.push(Frame::Execute { portal: self.portal.clone(), limit: self.limit });
        out.push(Frame::Flush);
        out
    }
}

// ---------------------------------------------------------------------
// Startup / cancel packet encoders (pure)
// ---------------------------------------------------------------------

/// StartupMessage: i32 length, i32 protocol code, then `user`, `database`
/// and every extra key as NUL-terminated key/value pairs, then a NUL.
pub fn encode_startup(protocol: Protocol, user: &str, database: &str, extra: &[(String, String)]) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&protocol.code().to_be_bytes());
    push_cstr(&mut body, b"user");
    push_cstr(&mut body, user.as_bytes());
    push_cstr(&mut body, b"database");
    push_cstr(&mut body, database.as_bytes());
    for (k, v) in extra {
        push_cstr(&mut body, k.as_bytes());
        push_cstr(&mut body, v.as_bytes());
    }
    body.push(0);
    let mut pkt = Vec::with_capacity(4 + body.len());
    pkt.extend_from_slice(&((body.len() as u32 + 4).to_be_bytes()));
    pkt.extend_from_slice(&body);
    pkt
}

/// CancelRequest: i32 length, i32 80877102, i32 pid, key bytes (4 under
/// protocol 3.0, variable-length under 3.2).
pub fn encode_cancel_request(pid: u32, key: &[u8]) -> Vec<u8> {
    let mut pkt = Vec::with_capacity(12 + key.len());
    pkt.extend_from_slice(&((12 + key.len()) as u32).to_be_bytes());
    pkt.extend_from_slice(&CANCEL_REQUEST_CODE.to_be_bytes());
    pkt.extend_from_slice(&pid.to_be_bytes());
    pkt.extend_from_slice(key);
    pkt
}

/// Open a second socket to `host:port`, send a CancelRequest for
/// (`pid`, `key`), and close it. `timeout` bounds the dial and the write.
pub fn send_cancel(host: &str, port: u16, pid: u32, key: &[u8], timeout: Option<Duration>) -> Result<(), String> {
    let mut s = dial(host, port, timeout).map_err(|e| e.detail)?;
    if let Some(t) = timeout {
        let _ = s.set_write_timeout(Some(t));
    }
    s.write_all(&encode_cancel_request(pid, key)).map_err(|e| format!("could not send CancelRequest: {e}"))?;
    let _ = s.shutdown(std::net::Shutdown::Both);
    Ok(())
}

fn dial(host: &str, port: u16, timeout: Option<Duration>) -> Result<TcpStream, ConnectError> {
    let fail = |e: std::io::Error| ConnectError {
        detail: format!("connect {host}:{port}: {e}"),
        wire: Vec::new(),
        hang: matches!(e.kind(), std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock),
    };
    let stream = match timeout {
        None => TcpStream::connect((host, port)).map_err(fail)?,
        Some(t) => {
            let mut addrs = (host, port).to_socket_addrs().map_err(fail)?;
            let addr = addrs.next().ok_or_else(|| ConnectError {
                detail: format!("connect {host}:{port}: no address"),
                wire: Vec::new(),
                hang: false,
            })?;
            TcpStream::connect_timeout(&addr, t).map_err(fail)?
        }
    };
    let _ = stream.set_nodelay(true);
    Ok(stream)
}

// ---------------------------------------------------------------------
// Backend message decode (pure, lossless)
// ---------------------------------------------------------------------

/// Decode one backend frame. Structurally malformed bodies fall back to
/// `WireMsg::Raw` (never dropped, never a panic) — a server that emits a
/// malformed frame is itself a finding.
pub fn decode_frame(t: u8, body: &[u8]) -> WireMsg {
    match decode_structured(t, body) {
        Some(m) => m,
        None => WireMsg::Raw { code: t as char, data: Bytes(body.to_vec()) },
    }
}

fn decode_structured(t: u8, body: &[u8]) -> Option<WireMsg> {
    Some(match t {
        b'E' => WireMsg::ErrorResponse(parse_err_fields(body)),
        b'N' => WireMsg::NoticeResponse(parse_err_fields(body)),
        b'T' => WireMsg::RowDescription(parse_row_description(body).ok()?),
        b'D' => WireMsg::DataRow(parse_data_row(body).ok()?),
        b'C' => WireMsg::CommandComplete(Bytes(cstr_bytes(body, 0).0.to_vec())),
        b'S' => {
            let (name, next) = cstr_bytes(body, 0);
            let (value, _) = cstr_bytes(body, next);
            WireMsg::ParameterStatus { name: Bytes(name.to_vec()), value: Bytes(value.to_vec()) }
        }
        b'A' => {
            if body.len() < 4 {
                return None;
            }
            let pid = be_u32(&body[0..4]);
            let (channel, next) = cstr_bytes(body, 4);
            let (payload, _) = cstr_bytes(body, next);
            WireMsg::NotificationResponse {
                pid,
                channel: Bytes(channel.to_vec()),
                payload: Bytes(payload.to_vec()),
            }
        }
        b'K' => {
            if body.len() < 4 {
                return None;
            }
            WireMsg::BackendKeyData { pid: be_u32(&body[0..4]), key: Bytes(body[4..].to_vec()) }
        }
        b'Z' => {
            if body.len() != 1 {
                return None;
            }
            WireMsg::ReadyForQuery { status: body[0] as char }
        }
        b'I' => WireMsg::EmptyQueryResponse,
        b'1' => WireMsg::ParseComplete,
        b'2' => WireMsg::BindComplete,
        b'3' => WireMsg::CloseComplete,
        b'n' => WireMsg::NoData,
        b's' => WireMsg::PortalSuspended,
        b'c' => WireMsg::CopyDone,
        b't' => {
            if body.len() < 2 {
                return None;
            }
            let n = be_i16(&body[0..2]);
            if n < 0 || body.len() != 2 + 4 * n as usize {
                return None;
            }
            WireMsg::ParameterDescription((0..n as usize).map(|i| be_u32(&body[2 + 4 * i..6 + 4 * i])).collect())
        }
        b'G' | b'H' => {
            if body.len() < 3 {
                return None;
            }
            let fmt = body[0] as i8;
            let n = be_i16(&body[1..3]);
            if n < 0 || body.len() != 3 + 2 * n as usize {
                return None;
            }
            let col_fmts = (0..n as usize).map(|i| be_i16(&body[3 + 2 * i..5 + 2 * i])).collect();
            if t == b'G' {
                WireMsg::CopyInResponse { fmt, col_fmts }
            } else {
                WireMsg::CopyOutResponse { fmt, col_fmts }
            }
        }
        b'd' => WireMsg::CopyData(Bytes(body.to_vec())),
        b'R' => {
            if body.len() < 4 {
                return None;
            }
            WireMsg::Authentication { kind: be_i32(&body[0..4]), data: Bytes(body[4..].to_vec()) }
        }
        b'v' => {
            if body.len() < 8 {
                return None;
            }
            let minor = be_i32(&body[0..4]);
            let n = be_i32(&body[4..8]);
            if n < 0 {
                return None;
            }
            let mut opts = Vec::with_capacity(n as usize);
            let mut i = 8;
            for _ in 0..n {
                if i >= body.len() {
                    return None;
                }
                let (o, next) = cstr_bytes(body, i);
                opts.push(Bytes(o.to_vec()));
                i = next;
            }
            WireMsg::NegotiateProtocolVersion { minor, unknown_options: opts }
        }
        _ => return None,
    })
}

/// ErrorResponse / NoticeResponse body: (code byte, cstr) pairs to a NUL.
pub fn parse_err_fields(body: &[u8]) -> ErrFields {
    let mut f = ErrFields::new();
    let mut i = 0;
    while i < body.len() && body[i] != 0 {
        let code = body[i] as char;
        let (val, next) = cstr_bytes(body, i + 1);
        f.insert(code, Bytes(val.to_vec()));
        i = next;
    }
    f
}

/// RowDescription body: i16 n, then per column cstr name, i32 tableoid,
/// i16 attnum, i32 typoid, i16 typlen, i32 typmod, i16 fmt.
pub fn parse_row_description(body: &[u8]) -> Result<Vec<ColDesc>, String> {
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
        if i >= body.len() {
            return Err("short RowDescription field".to_string());
        }
        let (name, next) = cstr_bytes(body, i);
        if next + 18 > body.len() {
            return Err("short RowDescription field".to_string());
        }
        cols.push(ColDesc {
            name: Bytes(name.to_vec()),
            tableoid: be_u32(&body[next..next + 4]),
            attnum: be_i16(&body[next + 4..next + 6]),
            typoid: be_u32(&body[next + 6..next + 10]),
            typlen: be_i16(&body[next + 10..next + 12]),
            typmod: be_i32(&body[next + 12..next + 16]),
            fmt: be_i16(&body[next + 16..next + 18]),
        });
        i = next + 18;
    }
    if i != body.len() {
        return Err("RowDescription has trailing bytes".to_string());
    }
    Ok(cols)
}

/// DataRow body: i16 n, then per cell i32 length (-1 = NULL) and bytes.
pub fn parse_data_row(body: &[u8]) -> Result<Vec<Option<Bytes>>, String> {
    if body.len() < 2 {
        return Err("short DataRow".to_string());
    }
    let ncols = be_i16(&body[0..2]);
    if ncols < 0 {
        return Err("negative column count in DataRow".to_string());
    }
    let mut row = Vec::with_capacity(ncols as usize);
    let mut i = 2;
    for _ in 0..ncols {
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
        row.push(Some(Bytes(body[i..i + len].to_vec())));
        i += len;
    }
    if i != body.len() {
        return Err("DataRow has trailing bytes".to_string());
    }
    Ok(row)
}

// ---------------------------------------------------------------------
// Auth primitives: base64, HMAC-SHA-256, PBKDF2, SCRAM-SHA-256, md5
// ---------------------------------------------------------------------

const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

pub fn b64_encode(data: &[u8]) -> String {
    let mut out = String::with_capacity((data.len() + 2) / 3 * 4);
    for chunk in data.chunks(3) {
        let b = [chunk[0], *chunk.get(1).unwrap_or(&0), *chunk.get(2).unwrap_or(&0)];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        out.push(B64[(n >> 18) as usize & 63] as char);
        out.push(B64[(n >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 { B64[(n >> 6) as usize & 63] as char } else { '=' });
        out.push(if chunk.len() > 2 { B64[n as usize & 63] as char } else { '=' });
    }
    out
}

pub fn b64_decode(s: &str) -> Result<Vec<u8>, String> {
    let mut out = Vec::with_capacity(s.len() / 4 * 3);
    let mut acc: u32 = 0;
    let mut bits = 0;
    let mut pad = 0;
    for c in s.bytes() {
        if c == b'=' {
            pad += 1;
            continue;
        }
        if pad > 0 {
            return Err("malformed base64 in SCRAM message".to_string());
        }
        let v = match c {
            b'A'..=b'Z' => c - b'A',
            b'a'..=b'z' => c - b'a' + 26,
            b'0'..=b'9' => c - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            _ => return Err("malformed base64 in SCRAM message".to_string()),
        };
        acc = (acc << 6) | u32::from(v);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
            acc &= (1 << bits) - 1;
        }
    }
    if pad > 2 || (s.len() % 4 != 0) {
        return Err("malformed base64 in SCRAM message".to_string());
    }
    Ok(out)
}

/// HMAC-SHA-256 (RFC 2104) over the workspace sha2 port.
pub fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    const BLOCK: usize = pg_sha2::PG_SHA256_BLOCK_LENGTH;
    let mut k = [0u8; BLOCK];
    if key.len() > BLOCK {
        k[..32].copy_from_slice(&pg_sha2::sha256(key));
    } else {
        k[..key.len()].copy_from_slice(key);
    }
    let mut inner = Vec::with_capacity(BLOCK + data.len());
    inner.extend(k.iter().map(|b| b ^ 0x36));
    inner.extend_from_slice(data);
    let ih = pg_sha2::sha256(&inner);
    let mut outer = Vec::with_capacity(BLOCK + 32);
    outer.extend(k.iter().map(|b| b ^ 0x5c));
    outer.extend_from_slice(&ih);
    pg_sha2::sha256(&outer)
}

/// PBKDF2-HMAC-SHA-256 with a 32-byte output (SCRAM SaltedPassword,
/// RFC 5802 Hi()).
pub fn scram_salted_password(password: &[u8], salt: &[u8], iterations: u32) -> [u8; 32] {
    let mut salt1 = salt.to_vec();
    salt1.extend_from_slice(&1u32.to_be_bytes());
    let mut u = hmac_sha256(password, &salt1);
    let mut result = u;
    for _ in 1..iterations {
        u = hmac_sha256(password, &u);
        for (r, x) in result.iter_mut().zip(u.iter()) {
            *r ^= x;
        }
    }
    result
}

/// `attr=value` lookup in a comma-split SCRAM message.
fn scram_attr<'a>(fields: &'a [&'a str], name: char) -> Result<&'a str, String> {
    fields
        .iter()
        .find(|f| f.starts_with(name) && f.as_bytes().get(1) == Some(&b'='))
        .map(|f| &f[2..])
        .ok_or_else(|| format!("malformed SCRAM message (missing \"{name}\" attribute)"))
}

/// SCRAM-SHA-256 client state (RFC 5802 / 7677), no channel binding
/// (gs2 header `n,,`, `c=biws`). Pure: nonce injected, transcript-testable.
#[derive(Clone, Debug)]
pub struct ScramClient {
    client_nonce: String,
    client_first_bare: String,
    expected_server_sig: Option<String>,
}

impl ScramClient {
    /// `user` is the SCRAM username attribute; libpq/psql send it empty
    /// (the user is already in the StartupMessage).
    pub fn new(user: &str, client_nonce: &str) -> ScramClient {
        ScramClient {
            client_nonce: client_nonce.to_string(),
            client_first_bare: format!("n={user},r={client_nonce}"),
            expected_server_sig: None,
        }
    }

    /// Fresh 18-byte random nonce, base64.
    pub fn random_nonce() -> Result<String, String> {
        let mut raw = [0u8; 18];
        let mut f = std::fs::File::open("/dev/urandom").map_err(|e| format!("could not open /dev/urandom: {e}"))?;
        f.read_exact(&mut raw).map_err(|_| "could not generate nonce".to_string())?;
        Ok(b64_encode(&raw))
    }

    /// The client-first-message (with gs2 header).
    pub fn client_first(&self) -> String {
        format!("n,,{}", self.client_first_bare)
    }

    /// SASLInitialResponse body: mechanism cstr, i32 length, initial data.
    pub fn sasl_initial_response(&self) -> Vec<u8> {
        let initial = self.client_first();
        let mut body = Vec::new();
        push_cstr(&mut body, b"SCRAM-SHA-256");
        body.extend_from_slice(&(initial.len() as u32).to_be_bytes());
        body.extend_from_slice(initial.as_bytes());
        body
    }

    /// Consume the server-first-message and produce the
    /// client-final-message; remembers the server signature to expect.
    pub fn client_final(&mut self, password: &[u8], server_first: &str) -> Result<String, String> {
        let fields: Vec<&str> = server_first.split(',').collect();
        let server_nonce = scram_attr(&fields, 'r')?.to_string();
        if !server_nonce.starts_with(&self.client_nonce) || server_nonce.len() == self.client_nonce.len() {
            return Err("invalid SCRAM response (nonce mismatch)".to_string());
        }
        let salt = scram_attr(&fields, 's').and_then(b64_decode)?;
        let iterations: u32 = match scram_attr(&fields, 'i').map(|s| s.parse::<u32>()) {
            Ok(Ok(v)) if v > 0 => v,
            _ => return Err("malformed SCRAM message (invalid iteration count)".to_string()),
        };
        let salted = scram_salted_password(password, &salt, iterations);
        let client_key = hmac_sha256(&salted, b"Client Key");
        let stored_key = pg_sha2::sha256(&client_key);
        let client_final_wo_proof = format!("c=biws,r={server_nonce}");
        let auth_message = format!("{},{},{}", self.client_first_bare, server_first, client_final_wo_proof);
        let client_sig = hmac_sha256(&stored_key, auth_message.as_bytes());
        let mut proof = client_key;
        for (p, s) in proof.iter_mut().zip(client_sig.iter()) {
            *p ^= s;
        }
        let server_key = hmac_sha256(&salted, b"Server Key");
        self.expected_server_sig = Some(b64_encode(&hmac_sha256(&server_key, auth_message.as_bytes())));
        Ok(format!("{client_final_wo_proof},p={}", b64_encode(&proof)))
    }

    /// Verify the server-final-message (`v=` signature, or `e=` error).
    pub fn verify_server_final(&self, server_final: &str) -> Result<(), String> {
        let fields: Vec<&str> = server_final.split(',').collect();
        if let Ok(e) = scram_attr(&fields, 'e') {
            return Err(format!("SCRAM server error: {e}"));
        }
        let v = scram_attr(&fields, 'v')?;
        let Some(expected) = &self.expected_server_sig else {
            return Err("SCRAM server-final before client-final".to_string());
        };
        // Constant-time compare (upstream d93ef413174d: timingsafe_bcmp in
        // auth paths).
        let a = v.as_bytes();
        let b = expected.as_bytes();
        let mut diff = a.len() ^ b.len();
        for (x, y) in a.iter().zip(b.iter()) {
            diff |= usize::from(x ^ y);
        }
        if diff != 0 {
            return Err("incorrect server signature in SCRAM exchange".to_string());
        }
        Ok(())
    }
}

/// PasswordMessage body for AuthenticationMD5Password: `"md5" +
/// md5hex(md5hex(password + user) + salt)`.
pub fn md5_password_response(user: &str, password: &str, salt: &[u8]) -> String {
    let stage1 = pg_md5::pg_md5_encrypt(password.as_bytes(), user.as_bytes());
    let stage2 = pg_md5::pg_md5_encrypt(&stage1[3..], salt);
    lossy(&stage2)
}

// ---------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------

pub struct Client<S: Read + Write = TcpStream> {
    stream: S,
    buf: Vec<u8>,
    pos: usize,
    dead: Option<String>,
    /// Latest ErrorResponse (SQLSTATE, primary message) of the current
    /// exchange. A FATAL closes the connection before ReadyForQuery, so
    /// the death detail names WHY the session died (round-7 FP-1: triage
    /// and the invalid-database residue ruling both need it).
    last_error: Option<(String, String)>,
    /// Every message of the startup exchange (the `@connect` step's wire).
    startup_wire: Vec<WireMsg>,
    backend_pid: u32,
    cancel_key: Vec<u8>,
    parameters: BTreeMap<Vec<u8>, Vec<u8>>,
    negotiated_minor: u32,
    txn_status: char,
    read_timeout_ms: Option<u64>,
    write_timeout_ms: Option<u64>,
    peer: Option<(String, u16)>,
}

impl<S: Read + Write> std::fmt::Debug for Client<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("backend_pid", &self.backend_pid)
            .field("negotiated_minor", &self.negotiated_minor)
            .field("txn_status", &self.txn_status)
            .field("dead", &self.dead)
            .field("peer", &self.peer)
            .finish_non_exhaustive()
    }
}

impl Client<TcpStream> {
    /// Dial + startup + auth ladder, through the first ReadyForQuery.
    /// Every message of the handshake is retained (`connect_wire`), also
    /// on failure (`ConnectError::wire`).
    pub fn connect_with(opts: &ConnectOpts) -> Result<Client, ConnectError> {
        let stream = dial(&opts.host, opts.port, opts.connect_timeout)?;
        let mut c = Client::over(stream);
        c.peer = Some((opts.host.clone(), opts.port));
        if let Err(e) = c.set_read_timeout(opts.read_timeout) {
            return Err(ConnectError { detail: format!("set_read_timeout: {e}"), wire: Vec::new(), hang: false });
        }
        if let Err(e) = c.set_write_timeout(opts.write_timeout) {
            return Err(ConnectError { detail: format!("set_write_timeout: {e}"), wire: Vec::new(), hang: false });
        }
        c.handshake(opts)?;
        Ok(c)
    }

    /// Dial + startup + trust auth, protocol 3.0, no timeouts (the
    /// pre-L0.1 adapter; every message is still retained).
    pub fn connect(host: &str, port: u16, db: &str, user: &str) -> Result<Client, ConnLost> {
        Client::connect_with(&ConnectOpts::new(host, port, db, user)).map_err(|e| ConnLost(e.detail))
    }

    /// Read deadline; a hit surfaces as `Fault::Hang` instead of blocking.
    pub fn set_read_timeout(&mut self, d: Option<Duration>) -> std::io::Result<()> {
        self.stream.set_read_timeout(d)?;
        self.read_timeout_ms = d.map(|d| d.as_millis() as u64);
        Ok(())
    }

    /// Write deadline; a hit surfaces as `Fault::Hang`.
    pub fn set_write_timeout(&mut self, d: Option<Duration>) -> std::io::Result<()> {
        self.stream.set_write_timeout(d)?;
        self.write_timeout_ms = d.map(|d| d.as_millis() as u64);
        Ok(())
    }

    /// CancelRequest over a second socket for this backend's pid + key.
    pub fn cancel(&self) -> Result<(), String> {
        let Some((host, port)) = &self.peer else {
            return Err("cancel: peer address unknown".to_string());
        };
        if self.cancel_key.is_empty() {
            return Err("cancel: no BackendKeyData received".to_string());
        }
        let t = self.write_timeout_ms.or(self.read_timeout_ms).map(Duration::from_millis);
        send_cancel(host, *port, self.backend_pid, &self.cancel_key, t)
    }
}

/// What an exchange loop does with server-driven turns.
struct Drive<'a> {
    /// Payload for CopyInResponse (None = CopyFail).
    copy_in: Option<&'a [u8]>,
    /// Resume PortalSuspended with Execute(portal, 0) + Flush.
    resume_portal: Option<&'a str>,
    /// Send Sync once when the exchange ends (C / I / E, or an unresumed
    /// PortalSuspended) — the extended-protocol driver.
    sync_on_end: bool,
    /// Stop after this many ReadyForQuery.
    ready_needed: usize,
}

impl<S: Read + Write> Client<S> {
    /// Wrap an already-open transport without a handshake (tests: canned
    /// byte streams; also a pre-authenticated stream from elsewhere).
    pub fn over(stream: S) -> Client<S> {
        Client {
            stream,
            buf: Vec::new(),
            pos: 0,
            dead: None,
            last_error: None,
            startup_wire: Vec::new(),
            backend_pid: 0,
            cancel_key: Vec::new(),
            parameters: BTreeMap::new(),
            negotiated_minor: 0,
            txn_status: 'I',
            read_timeout_ms: None,
            write_timeout_ms: None,
            peer: None,
        }
    }

    /// Report a read deadline (ms) for `Fault::Hang` on transports whose
    /// timeout is configured outside the client (`over` + a pre-set
    /// stream).
    pub fn note_read_timeout(&mut self, d: Option<Duration>) {
        self.read_timeout_ms = d.map(|d| d.as_millis() as u64);
    }

    /// Run the startup handshake (StartupMessage + auth ladder) over the
    /// wrapped transport, through ReadyForQuery.
    pub fn handshake(&mut self, opts: &ConnectOpts) -> Result<(), ConnectError> {
        let pkt = encode_startup(opts.protocol, &opts.user, &opts.database, &opts.extra);
        let mut wire: Vec<WireMsg> = Vec::new();
        if let Err(f) = self.send(&pkt) {
            return Err(ConnectError::from_fault(f, wire));
        }
        let fail = |detail: String, wire: Vec<WireMsg>| Err(ConnectError { detail, wire, hang: false });
        let mut scram: Option<ScramClient> = None;
        loop {
            let (t, body) = match self.read_message() {
                Ok(x) => x,
                Err(f) => return Err(ConnectError::from_fault(f, wire)),
            };
            let m = decode_frame(t, &body);
            wire.push(m.clone());
            match m {
                WireMsg::Authentication { kind, data } => {
                    let password = || -> Result<&[u8], String> {
                        opts.password
                            .as_deref()
                            .map(str::as_bytes)
                            .ok_or_else(|| format!("no password supplied for authentication method {kind}"))
                    };
                    let reply: Option<Vec<u8>> = match kind {
                        0 => None,
                        3 => match password() {
                            Ok(pw) => {
                                let mut b = pw.to_vec();
                                b.push(0);
                                Some(msg(b'p', &b))
                            }
                            Err(e) => return fail(e, wire),
                        },
                        5 => match password() {
                            Ok(pw) => {
                                if data.0.len() < 4 {
                                    return fail("received malformed MD5 authentication request".to_string(), wire);
                                }
                                let r = md5_password_response(&opts.user, &lossy(pw), &data.0[..4]);
                                let mut b = r.into_bytes();
                                b.push(0);
                                Some(msg(b'p', &b))
                            }
                            Err(e) => return fail(e, wire),
                        },
                        10 => {
                            if let Err(e) = password() {
                                return fail(e, wire);
                            }
                            let mut mechs: Vec<String> = Vec::new();
                            let mut p = 0;
                            while p < data.0.len() && data.0[p] != 0 {
                                let (mech, next) = cstr_bytes(&data.0, p);
                                mechs.push(lossy(mech));
                                p = next;
                            }
                            if !mechs.iter().any(|m| m == "SCRAM-SHA-256") {
                                return fail(
                                    format!(
                                        "none of the server's SASL authentication mechanisms are supported (offered: {})",
                                        mechs.join(", ")
                                    ),
                                    wire,
                                );
                            }
                            let nonce = match &opts.scram_nonce {
                                Some(n) => n.clone(),
                                None => match ScramClient::random_nonce() {
                                    Ok(n) => n,
                                    Err(e) => return fail(e, wire),
                                },
                            };
                            let sc = ScramClient::new("", &nonce);
                            let body = sc.sasl_initial_response();
                            scram = Some(sc);
                            Some(msg(b'p', &body))
                        }
                        11 => {
                            let Some(sc) = scram.as_mut() else {
                                return fail("SASL continue without an initial response".to_string(), wire);
                            };
                            let pw = match password() {
                                Ok(pw) => pw,
                                Err(e) => return fail(e, wire),
                            };
                            match sc.client_final(pw, &lossy(&data.0)) {
                                Ok(cf) => Some(msg(b'p', cf.as_bytes())),
                                Err(e) => return fail(e, wire),
                            }
                        }
                        12 => {
                            let Some(sc) = scram.as_ref() else {
                                return fail("SASL final without an exchange".to_string(), wire);
                            };
                            if let Err(e) = sc.verify_server_final(&lossy(&data.0)) {
                                return fail(e, wire);
                            }
                            None
                        }
                        other => return fail(format!("authentication method {other} not supported"), wire),
                    };
                    if let Some(r) = reply {
                        if let Err(f) = self.send(&r) {
                            return Err(ConnectError::from_fault(f, wire));
                        }
                    }
                }
                WireMsg::ErrorResponse(f) => {
                    let state = f.get(&'C').map(|b| lossy(&b.0)).unwrap_or_default();
                    let message = f.get(&'M').map(|b| lossy(&b.0)).unwrap_or_default();
                    return fail(format!("startup failed: {state} {message}"), wire);
                }
                WireMsg::ParameterStatus { name, value } => {
                    self.parameters.insert(name.0, value.0);
                }
                WireMsg::BackendKeyData { pid, key } => {
                    self.backend_pid = pid;
                    self.cancel_key = key.0;
                }
                WireMsg::NegotiateProtocolVersion { minor, .. } => {
                    self.negotiated_minor = minor.max(0) as u32;
                }
                WireMsg::NoticeResponse(_) => {}
                WireMsg::ReadyForQuery { status } => {
                    self.txn_status = status;
                    self.negotiated_minor = if self.negotiated_minor == 0 && wire.iter().all(|m| !matches!(m, WireMsg::NegotiateProtocolVersion { .. })) {
                        opts.protocol.minor()
                    } else {
                        self.negotiated_minor
                    };
                    self.startup_wire = wire;
                    return Ok(());
                }
                other => {
                    return fail(format!("unexpected message type \"{}\" during startup", other.code()), wire);
                }
            }
        }
    }

    /// The startup exchange's messages (empty for `over`).
    pub fn connect_wire(&self) -> &[WireMsg] {
        &self.startup_wire
    }

    /// Backend pid from BackendKeyData (0 if none).
    pub fn backend_pid(&self) -> u32 {
        self.backend_pid
    }

    /// Cancel key bytes from BackendKeyData (4 bytes under 3.0, variable
    /// under 3.2).
    pub fn cancel_key(&self) -> &[u8] {
        &self.cancel_key
    }

    /// Latest ParameterStatus value for `name`.
    pub fn parameter(&self, name: &str) -> Option<&[u8]> {
        self.parameters.get(name.as_bytes()).map(Vec::as_slice)
    }

    /// Every ParameterStatus key/value seen so far.
    pub fn parameters(&self) -> &BTreeMap<Vec<u8>, Vec<u8>> {
        &self.parameters
    }

    /// Protocol minor the server settled on (0 or 2).
    pub fn negotiated_minor(&self) -> u32 {
        self.negotiated_minor
    }

    /// Last ReadyForQuery status byte ('I' | 'T' | 'E').
    pub fn txn_status(&self) -> char {
        self.txn_status
    }

    /// Why the connection is dead, if it is.
    pub fn dead(&self) -> Option<&str> {
        self.dead.as_deref()
    }

    // ---- exchanges --------------------------------------------------

    /// Simple query: one 'Q' through ReadyForQuery. CopyInResponse is
    /// failed (use `simple_with_copy` to feed data).
    pub fn simple(&mut self, sql: &str) -> Exchange {
        self.simple_with_copy(sql, None)
    }

    /// Simple query that feeds `copy_in` (CopyData + CopyDone) when the
    /// server enters COPY FROM STDIN; COPY TO STDOUT data is retained as
    /// CopyData messages.
    pub fn simple_with_copy(&mut self, sql: &str, copy_in: Option<&[u8]>) -> Exchange {
        let frames = [Frame::Query(sql.to_string())];
        self.drive(&frames, Drive { copy_in, resume_portal: None, sync_on_end: false, ready_needed: 1 })
    }

    /// Extended-protocol step: Parse / [Describe S] / Bind / [Describe P] /
    /// Execute / Flush, then Sync once the server has answered (or a
    /// resumed portal has run out), through ReadyForQuery.
    pub fn extended(&mut self, step: &ExtendedStep) -> Exchange {
        let frames = step.frames();
        self.drive(
            &frames,
            Drive {
                copy_in: step.copy_in.as_deref(),
                resume_portal: if step.resume { Some(step.portal.as_str()) } else { None },
                sync_on_end: true,
                ready_needed: 1,
            },
        )
    }

    /// Pipelining: send every frame back-to-back, then one Sync if the
    /// batch carries none, and read until one ReadyForQuery per Sync (and
    /// per 'Q'). CopyInResponse is fed from `copy_in` (None = CopyFail).
    pub fn pipeline(&mut self, frames: &[Frame], copy_in: Option<&[u8]>) -> Exchange {
        let ready = frames.iter().filter(|f| f.yields_ready()).count();
        if ready == 0 {
            let mut with_sync = frames.to_vec();
            with_sync.push(Frame::Sync);
            return self.drive(&with_sync, Drive { copy_in, resume_portal: None, sync_on_end: false, ready_needed: 1 });
        }
        self.drive(frames, Drive { copy_in, resume_portal: None, sync_on_end: false, ready_needed: ready })
    }

    /// Raw bytes on the wire (the protocol mutator), then read until
    /// `ready_needed` ReadyForQuery messages — or the fault (FATAL + close,
    /// or the read deadline) that a malformed frame provokes.
    pub fn raw(&mut self, bytes: &[u8], ready_needed: usize) -> Exchange {
        let frames = [Frame::Raw(bytes.to_vec())];
        self.drive(&frames, Drive { copy_in: None, resume_portal: None, sync_on_end: false, ready_needed })
    }

    /// Read (sending nothing) until one ReadyForQuery: after a `Hang` +
    /// `cancel()`, or to collect asynchronous NotificationResponse traffic
    /// behind a Sync sent separately.
    pub fn drain(&mut self) -> Exchange {
        self.drive(&[], Drive { copy_in: None, resume_portal: None, sync_on_end: false, ready_needed: 1 })
    }

    /// Send Terminate and mark the connection closed.
    pub fn terminate(&mut self) {
        let _ = self.send(&Frame::Terminate.encode());
        self.dead = Some("terminated by client".to_string());
    }

    fn drive(&mut self, frames: &[Frame], d: Drive<'_>) -> Exchange {
        let start = Instant::now();
        let mut wire: Vec<WireMsg> = Vec::new();
        let finish = |wire: Vec<WireMsg>, fault: Option<Fault>| Exchange {
            wire,
            fault,
            ms: start.elapsed().as_millis() as u64,
        };
        if let Some(dead) = &self.dead {
            return finish(wire, Some(Fault::Lost(dead.clone())));
        }
        // Per-exchange: only an error from THIS exchange may annotate a
        // subsequent connection death.
        self.last_error = None;
        let mut batch = Vec::new();
        for f in frames {
            batch.extend_from_slice(&f.encode());
        }
        if !batch.is_empty() {
            if let Err(f) = self.send(&batch) {
                return finish(wire, Some(f));
            }
        }
        let mut synced = !d.sync_on_end;
        let mut ready = 0;
        loop {
            let (t, body) = match self.read_message() {
                Ok(x) => x,
                Err(f) => return finish(wire, Some(f)),
            };
            let m = decode_frame(t, &body);
            let mut reply: Vec<u8> = Vec::new();
            match &m {
                WireMsg::ErrorResponse(f) => {
                    let state = f.get(&'C').map(|b| lossy(&b.0)).unwrap_or_default();
                    let message = f.get(&'M').map(|b| lossy(&b.0)).unwrap_or_default();
                    self.last_error = Some((state, message));
                    if !synced {
                        reply.extend_from_slice(&Frame::Sync.encode());
                        synced = true;
                    }
                }
                WireMsg::CommandComplete(_) | WireMsg::EmptyQueryResponse => {
                    if !synced {
                        reply.extend_from_slice(&Frame::Sync.encode());
                        synced = true;
                    }
                }
                WireMsg::PortalSuspended => match d.resume_portal {
                    Some(portal) => {
                        reply.extend_from_slice(&Frame::Execute { portal: portal.to_string(), limit: 0 }.encode());
                        reply.extend_from_slice(&Frame::Flush.encode());
                    }
                    None => {
                        if !synced {
                            reply.extend_from_slice(&Frame::Sync.encode());
                            synced = true;
                        }
                    }
                },
                WireMsg::CopyInResponse { .. } => match d.copy_in {
                    Some(data) => {
                        reply.extend_from_slice(&Frame::CopyData(data.to_vec()).encode());
                        reply.extend_from_slice(&Frame::CopyDone.encode());
                    }
                    None => {
                        reply.extend_from_slice(&Frame::CopyFail("client has no COPY payload".to_string()).encode());
                    }
                },
                WireMsg::ParameterStatus { name, value } => {
                    self.parameters.insert(name.0.clone(), value.0.clone());
                }
                WireMsg::BackendKeyData { pid, key } => {
                    self.backend_pid = *pid;
                    self.cancel_key = key.0.clone();
                }
                WireMsg::ReadyForQuery { status } => {
                    self.txn_status = *status;
                    ready += 1;
                }
                _ => {}
            }
            wire.push(m);
            if !reply.is_empty() {
                if let Err(f) = self.send(&reply) {
                    return finish(wire, Some(f));
                }
            }
            if ready >= d.ready_needed {
                return finish(wire, None);
            }
        }
    }

    // ---- adapters (pre-L0.1 API; L0.3 switches consumers) ------------

    /// Simple query: every resultset through ReadyForQuery. Errors from the
    /// server come back as RawResults with `error` set; Err means the
    /// connection itself is gone (or hung past the read deadline).
    pub fn simple_query(&mut self, sql: &str) -> Result<Vec<RawResult>, ConnLost> {
        let x = self.simple(sql);
        self.adapt(x, fold_simple)
    }

    /// Simple query that feeds `data` when the server enters COPY FROM
    /// STDIN mode (CopyData + CopyDone). The X2 COPY BINARY round-trip
    /// path. Statements that never enter CopyIn behave as `simple_query`.
    pub fn copy_in(&mut self, sql: &str, data: &[u8]) -> Result<Vec<RawResult>, ConnLost> {
        let x = self.simple_with_copy(sql, Some(data));
        self.adapt(x, fold_simple)
    }

    /// Extended query: Parse/Bind/Describe(portal)/Execute over the unnamed
    /// statement + portal; `row_limit` > 0 with resume on PortalSuspended
    /// so the rowset is always complete; `result_binary` asks Bind for
    /// all-binary results (decoded per `decode_binary_cell`).
    pub fn extended_query(
        &mut self,
        sql: &str,
        params: &[WireParam],
        row_limit: u32,
        result_binary: bool,
    ) -> Result<Vec<RawResult>, ConnLost> {
        let mut step = ExtendedStep::new(sql);
        step.params = params.to_vec();
        step.limit = row_limit;
        step.result_fmts = if result_binary { vec![1] } else { Vec::new() };
        let x = self.extended(&step);
        self.adapt(x, |wire| vec![fold_extended(wire)])
    }

    fn adapt(&mut self, x: Exchange, fold: impl Fn(&[WireMsg]) -> Vec<RawResult>) -> Result<Vec<RawResult>, ConnLost> {
        match x.fault {
            None => Ok(fold(&x.wire)),
            Some(f) => Err(ConnLost(f.detail())),
        }
    }

    // ---- transport ----------------------------------------------------

    fn poison(&mut self, e: String) -> Fault {
        // A FATAL ErrorResponse closes the connection before ReadyForQuery,
        // so the death surfaces here as an EOF/read failure. Stamp the last
        // server error onto the detail so classification and triage see
        // WHY the session died.
        let e = match &self.last_error {
            Some((state, message)) if e.starts_with("server closed") || e.starts_with("could not read") => {
                format!("{e} after server error {state}: {message}")
            }
            _ => e,
        };
        self.dead = Some(e.clone());
        Fault::Lost(e)
    }

    fn is_timeout(e: &std::io::Error) -> bool {
        matches!(e.kind(), std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut)
    }

    fn send(&mut self, buf: &[u8]) -> Result<(), Fault> {
        match self.stream.write_all(buf) {
            Ok(()) => Ok(()),
            Err(e) if Self::is_timeout(&e) => Err(Fault::Hang { timeout_ms: self.write_timeout_ms.unwrap_or(0) }),
            Err(e) => Err(self.poison(format!("could not send to server: {e}"))),
        }
    }

    fn read_message(&mut self) -> Result<(u8, Vec<u8>), Fault> {
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
            let n = match self.stream.read(&mut chunk) {
                Ok(n) => n,
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) if Self::is_timeout(&e) => {
                    return Err(Fault::Hang { timeout_ms: self.read_timeout_ms.unwrap_or(0) });
                }
                Err(e) => return Err(self.poison(format!("could not read from server: {e}"))),
            };
            if n == 0 {
                return Err(self.poison("server closed the connection unexpectedly".to_string()));
            }
            self.buf.extend_from_slice(&chunk[..n]);
        }
    }
}

// ---------------------------------------------------------------------
// Folds: wire -> the pre-L0.1 RawResult shape (strings)
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

/// Decode one binary-format cell to its compare string (X2 policy):
///   - float4/float8 decode to the value's shortest-roundtrip text so the
///     ruled float-ulp comparator (B1) still applies;
///   - text-family types (text/varchar/bpchar/name/cstring/json/xml)
///     decode as UTF-8 — their binary wire form IS the text bytes;
///   - every other type renders as `\x`-hex: cell equality is byte-for-byte
///     *_send equality.
pub fn decode_binary_cell(oid: u32, b: &[u8]) -> String {
    match oid {
        700 if b.len() == 4 => {
            let f = f32::from_be_bytes([b[0], b[1], b[2], b[3]]);
            pg_float_text(f == f32::NEG_INFINITY, f == f32::INFINITY, f.is_nan(), format!("{f}"))
        }
        701 if b.len() == 8 => {
            let f = f64::from_be_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]);
            pg_float_text(f == f64::NEG_INFINITY, f == f64::INFINITY, f.is_nan(), format!("{f}"))
        }
        // 25 text, 1043 varchar, 1042 bpchar, 19 name, 2275 cstring, 114
        // json, 142 xml.
        25 | 1043 | 1042 | 19 | 2275 | 114 | 142 => lossy(b),
        _ => hex_of(b),
    }
}

fn cell_text(desc: &[ColDesc], col: usize, cell: &Option<Bytes>) -> Option<String> {
    let b = cell.as_ref()?;
    let (oid, fmt) = desc.get(col).map(|c| (c.typoid, c.fmt)).unwrap_or((0, 0));
    Some(if fmt == 1 { decode_binary_cell(oid, &b.0) } else { lossy(&b.0) })
}

fn error_pair(f: &ErrFields) -> (String, String) {
    (
        f.get(&'C').map(|b| lossy(&b.0)).unwrap_or_default(),
        f.get(&'M').map(|b| lossy(&b.0)).unwrap_or_default(),
    )
}

/// Fold a simple-query wire into results: one per CommandComplete /
/// EmptyQueryResponse, plus one error result per ErrorResponse.
pub fn fold_simple(wire: &[WireMsg]) -> Vec<RawResult> {
    let mut results = Vec::new();
    let mut cur = RawResult::new();
    let mut desc: Vec<ColDesc> = Vec::new();
    for m in wire {
        match m {
            WireMsg::RowDescription(cols) => {
                cur = RawResult::new();
                cur.col_oids = cols.iter().map(|c| c.typoid).collect();
                desc = cols.clone();
            }
            WireMsg::DataRow(cells) => {
                cur.rows.push(cells.iter().enumerate().map(|(i, c)| cell_text(&desc, i, c)).collect());
            }
            WireMsg::CommandComplete(tag) => {
                cur.cmd_tag = lossy(&tag.0);
                results.push(std::mem::replace(&mut cur, RawResult::new()));
            }
            WireMsg::EmptyQueryResponse => {
                results.push(std::mem::replace(&mut cur, RawResult::new()));
            }
            WireMsg::ErrorResponse(f) => {
                let mut r = RawResult::new();
                r.error = Some(error_pair(f));
                results.push(r);
                cur = RawResult::new();
            }
            WireMsg::CopyInResponse { .. } | WireMsg::CopyOutResponse { .. } => {
                cur.was_copy = true;
            }
            WireMsg::CopyData(d) => {
                cur.was_copy = true;
                cur.copy_out.extend_from_slice(&d.0);
            }
            _ => {}
        }
    }
    results
}

/// Fold an extended-protocol wire into the one result the old callers
/// expect: rows accumulate across resumed portal batches, the first
/// ErrorResponse wins.
pub fn fold_extended(wire: &[WireMsg]) -> RawResult {
    let mut cur = RawResult::new();
    let mut desc: Vec<ColDesc> = Vec::new();
    for m in wire {
        match m {
            WireMsg::RowDescription(cols) => {
                cur.col_oids = cols.iter().map(|c| c.typoid).collect();
                desc = cols.clone();
            }
            WireMsg::DataRow(cells) => {
                cur.rows.push(cells.iter().enumerate().map(|(i, c)| cell_text(&desc, i, c)).collect());
            }
            WireMsg::CommandComplete(tag) => cur.cmd_tag = lossy(&tag.0),
            WireMsg::ErrorResponse(f) => {
                if cur.error.is_none() {
                    cur.error = Some(error_pair(f));
                }
            }
            WireMsg::CopyInResponse { .. } | WireMsg::CopyOutResponse { .. } => cur.was_copy = true,
            WireMsg::CopyData(d) => {
                cur.was_copy = true;
                cur.copy_out.extend_from_slice(&d.0);
            }
            _ => {}
        }
    }
    cur
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contracts::{json, ObservationRecord};
    use std::path::PathBuf;

    // ------------------------------------------------------------------
    // Fixtures and encoders
    // ------------------------------------------------------------------

    fn fixture(name: &str) -> String {
        let p = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures/contracts").join(name);
        std::fs::read_to_string(&p).unwrap_or_else(|e| panic!("{}: {e}", p.display()))
    }

    fn err_body(fields: &ErrFields) -> Vec<u8> {
        let mut b = Vec::new();
        for (k, v) in fields {
            b.push(*k as u8);
            push_cstr(&mut b, &v.0);
        }
        b.push(0);
        b
    }

    fn row_desc_body(cols: &[ColDesc]) -> Vec<u8> {
        let mut body = (cols.len() as i16).to_be_bytes().to_vec();
        for c in cols {
            push_cstr(&mut body, &c.name.0);
            body.extend_from_slice(&c.tableoid.to_be_bytes());
            body.extend_from_slice(&c.attnum.to_be_bytes());
            body.extend_from_slice(&c.typoid.to_be_bytes());
            body.extend_from_slice(&c.typlen.to_be_bytes());
            body.extend_from_slice(&c.typmod.to_be_bytes());
            body.extend_from_slice(&c.fmt.to_be_bytes());
        }
        body
    }

    fn simple_desc(cols: &[(u32, i16)]) -> Vec<ColDesc> {
        cols.iter()
            .enumerate()
            .map(|(i, (oid, fmt))| ColDesc {
                name: Bytes::text(&format!("c{i}")),
                tableoid: 0,
                attnum: 0,
                typoid: *oid,
                typlen: 4,
                typmod: -1,
                fmt: *fmt,
            })
            .collect()
    }

    fn data_row_body(cells: &[Option<&[u8]>]) -> Vec<u8> {
        let mut body = (cells.len() as i16).to_be_bytes().to_vec();
        for c in cells {
            match c {
                None => body.extend_from_slice(&(-1i32).to_be_bytes()),
                Some(v) => {
                    body.extend_from_slice(&(v.len() as u32).to_be_bytes());
                    body.extend_from_slice(v);
                }
            }
        }
        body
    }

    /// Encode one WireMsg back to its backend frame body (test-side
    /// encoder: the decoder must invert it).
    fn encode_wire(m: &WireMsg) -> Vec<u8> {
        let t = m.code() as u8;
        let body: Vec<u8> = match m {
            WireMsg::ErrorResponse(f) | WireMsg::NoticeResponse(f) => err_body(f),
            WireMsg::RowDescription(cols) => row_desc_body(cols),
            WireMsg::DataRow(cells) => {
                let refs: Vec<Option<&[u8]>> = cells.iter().map(|c| c.as_ref().map(|b| b.0.as_slice())).collect();
                data_row_body(&refs)
            }
            WireMsg::CommandComplete(tag) => {
                let mut b = Vec::new();
                push_cstr(&mut b, &tag.0);
                b
            }
            WireMsg::ParameterStatus { name, value } => {
                let mut b = Vec::new();
                push_cstr(&mut b, &name.0);
                push_cstr(&mut b, &value.0);
                b
            }
            WireMsg::NotificationResponse { pid, channel, payload } => {
                let mut b = pid.to_be_bytes().to_vec();
                push_cstr(&mut b, &channel.0);
                push_cstr(&mut b, &payload.0);
                b
            }
            WireMsg::BackendKeyData { pid, key } => {
                let mut b = pid.to_be_bytes().to_vec();
                b.extend_from_slice(&key.0);
                b
            }
            WireMsg::ReadyForQuery { status } => vec![*status as u8],
            WireMsg::EmptyQueryResponse
            | WireMsg::ParseComplete
            | WireMsg::BindComplete
            | WireMsg::CloseComplete
            | WireMsg::NoData
            | WireMsg::PortalSuspended
            | WireMsg::CopyDone => Vec::new(),
            WireMsg::ParameterDescription(oids) => {
                let mut b = (oids.len() as i16).to_be_bytes().to_vec();
                for o in oids {
                    b.extend_from_slice(&o.to_be_bytes());
                }
                b
            }
            WireMsg::CopyInResponse { fmt, col_fmts } | WireMsg::CopyOutResponse { fmt, col_fmts } => {
                let mut b = vec![*fmt as u8];
                b.extend_from_slice(&(col_fmts.len() as i16).to_be_bytes());
                for f in col_fmts {
                    b.extend_from_slice(&f.to_be_bytes());
                }
                b
            }
            WireMsg::CopyData(d) => d.0.clone(),
            WireMsg::Authentication { kind, data } => {
                let mut b = kind.to_be_bytes().to_vec();
                b.extend_from_slice(&data.0);
                b
            }
            WireMsg::NegotiateProtocolVersion { minor, unknown_options } => {
                let mut b = minor.to_be_bytes().to_vec();
                b.extend_from_slice(&(unknown_options.len() as i32).to_be_bytes());
                for o in unknown_options {
                    push_cstr(&mut b, &o.0);
                }
                b
            }
            WireMsg::Raw { data, .. } => data.0.clone(),
        };
        msg(t, &body)
    }

    fn split_frames(stream: &[u8]) -> Vec<(u8, Vec<u8>)> {
        let mut out = Vec::new();
        let mut i = 0;
        while i + 5 <= stream.len() {
            let t = stream[i];
            let len = be_i32(&stream[i + 1..i + 5]) as usize;
            out.push((t, stream[i + 5..i + 1 + len].to_vec()));
            i += 1 + len;
        }
        assert_eq!(i, stream.len(), "trailing bytes in frame stream");
        out
    }

    fn fields(pairs: &[(char, &str)]) -> ErrFields {
        pairs.iter().map(|(k, v)| (*k, Bytes::text(v))).collect()
    }

    #[test]
    fn every_message_type_round_trips_through_decode() {
        let msgs = vec![
            WireMsg::ErrorResponse(fields(&[
                ('S', "ERROR"),
                ('V', "ERROR"),
                ('C', "23502"),
                ('M', "null value"),
                ('D', "Failing row contains (null, 1)."),
                ('H', "Supply a value."),
                ('P', "1"),
                ('p', "13"),
                ('q', "INSERT INTO t VALUES (NULL, 1)"),
                ('W', "PL/pgSQL function f() line 3 at EXECUTE"),
                ('s', "public"),
                ('t', "t"),
                ('c', "t_a_not_null"),
                ('d', "integer"),
                ('n', "a"),
                ('F', "execMain.c"),
                ('L', "1975"),
                ('R', "ExecConstraints"),
            ])),
            WireMsg::NoticeResponse(fields(&[('S', "INFO"), ('V', "INFO"), ('C', "00000"), ('M', "analyzing \"public.t\"")])),
            WireMsg::RowDescription(vec![
                ColDesc { name: Bytes::text("a"), tableoid: 16401, attnum: 1, typoid: 23, typlen: 4, typmod: -1, fmt: 0 },
                ColDesc { name: Bytes::text("?column?"), tableoid: 0, attnum: 0, typoid: 1043, typlen: -1, typmod: 36, fmt: 1 },
            ]),
            WireMsg::DataRow(vec![Some(Bytes::text("1")), None, Some(Bytes(vec![0xff, 0x00, 0x41])), Some(Bytes(vec![]))]),
            WireMsg::CommandComplete(Bytes::text("INSERT 0 3")),
            WireMsg::ParameterStatus { name: Bytes::text("client_encoding"), value: Bytes::text("UTF8") },
            WireMsg::NotificationResponse { pid: 41233, channel: Bytes::text("fz_chan"), payload: Bytes::text("hello") },
            WireMsg::BackendKeyData { pid: 41233, key: Bytes(vec![0x1f, 0x9a, 0x00, 0x7c]) },
            // Protocol 3.2: variable-length key.
            WireMsg::BackendKeyData { pid: 7, key: Bytes((0..32).collect()) },
            WireMsg::ReadyForQuery { status: 'T' },
            WireMsg::EmptyQueryResponse,
            WireMsg::ParseComplete,
            WireMsg::BindComplete,
            WireMsg::CloseComplete,
            WireMsg::NoData,
            WireMsg::PortalSuspended,
            WireMsg::CopyDone,
            WireMsg::ParameterDescription(vec![23, 25, 1700]),
            WireMsg::ParameterDescription(vec![]),
            WireMsg::CopyInResponse { fmt: 1, col_fmts: vec![1, 1] },
            WireMsg::CopyOutResponse { fmt: 0, col_fmts: vec![0, 0, 0] },
            WireMsg::CopyData(Bytes(b"PGCOPY\n\xff\r\n\0".to_vec())),
            WireMsg::Authentication { kind: 10, data: Bytes(b"SCRAM-SHA-256\0\0".to_vec()) },
            WireMsg::Authentication { kind: 5, data: Bytes(vec![1, 2, 3, 4]) },
            WireMsg::Authentication { kind: 0, data: Bytes(vec![]) },
            WireMsg::NegotiateProtocolVersion { minor: 0, unknown_options: vec![Bytes::text("_pq_.foo"), Bytes::text("_pq_.bar")] },
            WireMsg::Raw { code: 'V', data: Bytes(vec![0, 0, 0, 1]) },
        ];
        for m in &msgs {
            let frame = encode_wire(m);
            let (t, body) = &split_frames(&frame)[0];
            assert_eq!(&decode_frame(*t, body), m, "round trip of {:?}", m.code());
        }
    }

    #[test]
    fn contracts_fixture_error_and_notice_bytes_decode_to_the_fixture() {
        // observation-error.json: an ErrorResponse with every field code.
        for name in ["observation-error.json", "observation-notice.json", "observation-crash.json"] {
            let rec = ObservationRecord::from_json(&json::parse(&fixture(name)).unwrap()).unwrap();
            assert!(!rec.wire.is_empty());
            for m in &rec.wire {
                let frame = encode_wire(m);
                let (t, body) = &split_frames(&frame)[0];
                assert_eq!(&decode_frame(*t, body), m, "{name}: {:?}", m.code());
            }
        }
        let rec = ObservationRecord::from_json(&json::parse(&fixture("observation-error.json")).unwrap()).unwrap();
        let WireMsg::ErrorResponse(f) = &rec.wire[0] else { panic!("fixture wire[0] is not E") };
        let codes: String = f.keys().collect();
        assert_eq!(codes, "CDFHLMPRSVWcdnpqst");
        assert_eq!(f.len(), 18, "all 18 field codes present");
        let decoded = parse_err_fields(&err_body(f));
        assert_eq!(&decoded, f);
        assert_eq!(decoded[&'q'], Bytes::text("INSERT INTO t VALUES (NULL, 1)"));
        assert_eq!(decoded[&'W'], Bytes::text("PL/pgSQL function f() line 3 at EXECUTE"));
    }

    #[test]
    fn non_utf8_bytes_survive_decode_verbatim() {
        let body = b"SERROR\0C22021\0Minvalid byte \xff\xfe here\0\0";
        let f = parse_err_fields(body);
        assert_eq!(f[&'M'].0, b"invalid byte \xff\xfe here".to_vec());
        let row = parse_data_row(&data_row_body(&[Some(b"\xc3\x28"), Some(b"")])).unwrap();
        assert_eq!(row, vec![Some(Bytes(vec![0xc3, 0x28])), Some(Bytes(vec![]))]);
        let WireMsg::CommandComplete(tag) = decode_frame(b'C', b"COPY \xff 3\0") else { panic!() };
        assert_eq!(tag.0, b"COPY \xff 3".to_vec());
    }

    #[test]
    fn malformed_frames_become_raw_not_dropped() {
        assert!(parse_data_row(&[0, 1, 0, 0]).is_err());
        assert!(parse_row_description(&[0]).is_err());
        assert!(matches!(decode_frame(b'D', &[0, 1, 0, 0]), WireMsg::Raw { code: 'D', .. }));
        assert!(matches!(decode_frame(b'T', &[0]), WireMsg::Raw { code: 'T', .. }));
        assert!(matches!(decode_frame(b'Z', &[b'I', b'I']), WireMsg::Raw { code: 'Z', .. }));
        assert!(matches!(decode_frame(b't', &[0, 2, 0]), WireMsg::Raw { code: 't', .. }));
        assert!(matches!(decode_frame(b'G', &[0, 0, 1]), WireMsg::Raw { code: 'G', .. }));
        assert!(matches!(decode_frame(b'v', &[0, 0, 0, 0]), WireMsg::Raw { code: 'v', .. }));
        assert!(matches!(decode_frame(b'K', &[1, 2]), WireMsg::Raw { code: 'K', .. }));
        assert!(matches!(decode_frame(b'R', &[0, 0]), WireMsg::Raw { code: 'R', .. }));
        assert_eq!(decode_frame(b'W', b"xyz"), WireMsg::Raw { code: 'W', data: Bytes(b"xyz".to_vec()) });
        // Unterminated fields still decode (tail taken as the value).
        let f = parse_err_fields(b"SERROR\0C22012");
        assert_eq!(f[&'C'], Bytes::text("22012"));
    }

    #[test]
    fn startup_packet_encodes_both_versions_with_extra_keys() {
        let extra = vec![
            ("options".to_string(), "-c work_mem=8MB --foo=bar".to_string()),
            ("application_name".to_string(), "fuzz".to_string()),
            ("client_encoding".to_string(), "LATIN1".to_string()),
            ("replication".to_string(), "database".to_string()),
            ("_pq_.protocol_managed_params".to_string(), "x".to_string()),
        ];
        let p30 = encode_startup(Protocol::V3_0, "u", "d", &extra);
        let p32 = encode_startup(Protocol::V3_2, "u", "d", &extra);
        assert_eq!(be_u32(&p30[0..4]) as usize, p30.len());
        assert_eq!(be_u32(&p32[0..4]) as usize, p32.len());
        assert_eq!(&p30[4..8], &[0, 3, 0, 0]);
        assert_eq!(&p32[4..8], &[0, 3, 0, 2]);
        let expect = b"user\0u\0database\0d\0options\0-c work_mem=8MB --foo=bar\0application_name\0fuzz\0client_encoding\0LATIN1\0replication\0database\0_pq_.protocol_managed_params\0x\0\0";
        assert_eq!(&p30[8..], &expect[..]);
        assert_eq!(&p32[8..], &expect[..]);
        // No extras: user + database + terminator only.
        let bare = encode_startup(Protocol::V3_0, "fuzz", "postgres", &[]);
        assert_eq!(&bare[8..], b"user\0fuzz\0database\0postgres\0\0");
        assert_eq!(Protocol::V3_2.code(), 196610);
    }

    #[test]
    fn cancel_request_encodes_pid_and_variable_key() {
        let p = encode_cancel_request(41233, &[0x1f, 0x9a, 0x00, 0x7c]);
        assert_eq!(p.len(), 16);
        assert_eq!(be_u32(&p[0..4]), 16);
        assert_eq!(be_u32(&p[4..8]), 80877102);
        assert_eq!(be_u32(&p[8..12]), 41233);
        assert_eq!(&p[12..], &[0x1f, 0x9a, 0x00, 0x7c]);
        let long: Vec<u8> = (0..32).collect();
        let p = encode_cancel_request(1, &long);
        assert_eq!(p.len(), 44);
        assert_eq!(be_u32(&p[0..4]), 44);
        assert_eq!(&p[12..], &long[..]);
    }

    #[test]
    fn frame_encoders_frame_correctly() {
        let p = Frame::Parse { stmt: "ps1".into(), sql: "SELECT $1;".into(), param_oids: vec![23, 0] }.encode();
        assert_eq!(p[0], b'P');
        assert_eq!(be_i32(&p[1..5]) as usize, p.len() - 1);
        assert_eq!(&p[5..], b"ps1\0SELECT $1;\0\0\x02\0\0\0\x17\0\0\0\0");

        // Bind with one text param and one NULL: 0 format codes = all
        // text when nothing is binary; `[]` result formats = all text.
        let b = Frame::Bind {
            portal: String::new(),
            stmt: String::new(),
            params: vec![WireParam::text(Some("42")), WireParam::text(None)],
            result_fmts: vec![],
        }
        .encode();
        assert_eq!(b[0], b'B');
        let body = &b[5..];
        assert_eq!(&body[0..2], &[0, 0]);
        assert_eq!(be_i16(&body[2..4]), 0);
        assert_eq!(be_i16(&body[4..6]), 2);
        assert_eq!(be_i32(&body[6..10]), 2);
        assert_eq!(&body[10..12], b"42");
        assert_eq!(be_i32(&body[12..16]), -1);
        assert_eq!(be_i16(&body[16..18]), 0);
        assert_eq!(body.len(), 18);

        // Mixed formats, named portal + statement, per-column results.
        let b = Frame::Bind {
            portal: "p1".into(),
            stmt: "ps1".into(),
            params: vec![WireParam { bytes: Some(7i32.to_be_bytes().to_vec()), binary: true }, WireParam::text(Some("x"))],
            result_fmts: vec![1, 0],
        }
        .encode();
        let body = &b[5..];
        assert_eq!(&body[0..7], b"p1\0ps1\0");
        let body = &body[7..];
        assert_eq!(be_i16(&body[0..2]), 2);
        assert_eq!(be_i16(&body[2..4]), 1);
        assert_eq!(be_i16(&body[4..6]), 0);
        assert_eq!(be_i16(&body[6..8]), 2);
        assert_eq!(be_i32(&body[8..12]), 4);
        assert_eq!(&body[12..16], &7i32.to_be_bytes());
        assert_eq!(be_i32(&body[16..20]), 1);
        assert_eq!(&body[20..21], b"x");
        assert_eq!(be_i16(&body[21..23]), 2);
        assert_eq!(be_i16(&body[23..25]), 1);
        assert_eq!(be_i16(&body[25..27]), 0);

        assert_eq!(&Frame::Execute { portal: "p1".into(), limit: 5 }.encode()[5..], b"p1\0\0\0\0\x05");
        assert_eq!(&Frame::Describe { kind: Describe::Portal, name: String::new() }.encode()[5..], b"P\0");
        assert_eq!(&Frame::Describe { kind: Describe::Statement, name: "ps1".into() }.encode()[5..], b"Sps1\0");
        assert_eq!(&Frame::Close { kind: Describe::Statement, name: "ps1".into() }.encode()[5..], b"Sps1\0");
        assert_eq!(Frame::Flush.encode(), vec![b'H', 0, 0, 0, 4]);
        assert_eq!(Frame::Sync.encode(), vec![b'S', 0, 0, 0, 4]);
        assert_eq!(Frame::Terminate.encode(), vec![b'X', 0, 0, 0, 4]);
        assert_eq!(Frame::CopyDone.encode(), vec![b'c', 0, 0, 0, 4]);
        assert_eq!(&Frame::CopyData(vec![1, 2]).encode()[5..], &[1, 2]);
        assert_eq!(&Frame::CopyFail("no".into()).encode()[5..], b"no\0");
        assert_eq!(&Frame::Query("SELECT 1".into()).encode()[5..], b"SELECT 1\0");
        assert_eq!(Frame::Raw(vec![9, 9]).encode(), vec![9, 9]);
    }

    // ------------------------------------------------------------------
    // Auth primitives and vectors
    // ------------------------------------------------------------------

    #[test]
    fn base64_round_trips() {
        for (raw, enc) in [(&b""[..], ""), (b"f", "Zg=="), (b"fo", "Zm8="), (b"foo", "Zm9v"), (b"foob", "Zm9vYg=="), (b"foobar", "Zm9vYmFy")] {
            assert_eq!(b64_encode(raw), enc);
            assert_eq!(b64_decode(enc).unwrap(), raw);
        }
        assert_eq!(b64_decode("W22ZaJ0SNY7soEsUEjb6gQ==").unwrap().len(), 16);
        assert!(b64_decode("Zm9=v").is_err());
        assert!(b64_decode("Zm9").is_err());
    }

    #[test]
    fn hmac_sha256_rfc4231_vector() {
        // RFC 4231 test case 1.
        let mac = hmac_sha256(&[0x0b; 20], b"Hi There");
        assert_eq!(crate::contracts::hex(&mac), "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7");
        // Test case 2: key "Jefe".
        let mac = hmac_sha256(b"Jefe", b"what do ya want for nothing?");
        assert_eq!(crate::contracts::hex(&mac), "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843");
    }

    #[test]
    fn scram_sha256_rfc7677_vector() {
        // RFC 7677 §3: user "user", password "pencil".
        let mut sc = ScramClient::new("user", "rOprNGfwEbeRWgbNEkqO");
        assert_eq!(sc.client_first(), "n,,n=user,r=rOprNGfwEbeRWgbNEkqO");
        let server_first = "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";
        let client_final = sc.client_final(b"pencil", server_first).unwrap();
        assert_eq!(
            client_final,
            "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ="
        );
        assert!(sc.verify_server_final("v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=").is_ok());
        assert!(sc.verify_server_final("v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G5=").is_err());
        assert!(sc.verify_server_final("e=invalid-proof").is_err());
        // Nonce tampering is refused.
        let mut sc2 = ScramClient::new("user", "rOprNGfwEbeRWgbNEkqO");
        assert!(sc2.client_final(b"pencil", "r=XXprNGfwEbeRWgbNEkqO%hv,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096").is_err());
        assert!(sc2.client_final(b"pencil", "r=rOprNGfwEbeRWgbNEkqO%hv,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=0").is_err());
        // The SASLInitialResponse body frames mechanism + length + data.
        let body = sc.sasl_initial_response();
        assert_eq!(&body[..14], b"SCRAM-SHA-256\0");
        assert_eq!(be_u32(&body[14..18]) as usize, "n,,n=user,r=rOprNGfwEbeRWgbNEkqO".len());
        assert_eq!(&body[18..], b"n,,n=user,r=rOprNGfwEbeRWgbNEkqO");
    }

    #[test]
    fn scram_salted_password_matches_rfc7677_derivation() {
        // SaltedPassword for the RFC 7677 vector: ClientKey = HMAC(salted,
        // "Client Key"); StoredKey = H(ClientKey) must equal the value the
        // server would compute for the documented client proof.
        let salt = b64_decode("W22ZaJ0SNY7soEsUEjb6gQ==").unwrap();
        let salted = scram_salted_password(b"pencil", &salt, 4096);
        assert_eq!(
            crate::contracts::hex(&salted),
            "c4a49510323ab4f952cac1fa99441939e78ea74d6be81ddf7096e87513dc615d"
        );
    }

    #[test]
    fn md5_password_response_vector() {
        // md5(md5("secret" + "fuzz") + salt 01020304), python hashlib.
        assert_eq!(md5_password_response("fuzz", "secret", &[1, 2, 3, 4]), "md5ce113c52f282067a4b65b4fbb18c534e");
        assert_eq!(md5_password_response("fuzz", "secret", &[1, 2, 3, 4]).len(), 35);
    }

    // ------------------------------------------------------------------
    // Fake transport
    // ------------------------------------------------------------------

    /// Fake transport: reads from a pre-canned server byte stream, records
    /// everything the client writes.
    struct FakeStream {
        input: std::io::Cursor<Vec<u8>>,
        written: Vec<u8>,
    }

    impl FakeStream {
        fn new(input: Vec<u8>) -> FakeStream {
            FakeStream { input: std::io::Cursor::new(input), written: Vec::new() }
        }
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

    fn server(msgs: &[WireMsg]) -> Vec<u8> {
        msgs.iter().flat_map(encode_wire).collect()
    }

    fn ready(status: char) -> WireMsg {
        WireMsg::ReadyForQuery { status }
    }

    fn complete(tag: &str) -> WireMsg {
        WireMsg::CommandComplete(Bytes::text(tag))
    }

    fn count_sent(written: &[u8], ty: u8) -> usize {
        split_frames(written).iter().filter(|(t, _)| *t == ty).count()
    }

    fn sent_types(written: &[u8]) -> String {
        split_frames(written).iter().map(|(t, _)| *t as char).collect()
    }

    // ------------------------------------------------------------------
    // Handshake over canned streams: trust, cleartext, md5, scram, 3.2
    // ------------------------------------------------------------------

    fn startup_tail() -> Vec<WireMsg> {
        vec![
            WireMsg::Authentication { kind: 0, data: Bytes(vec![]) },
            WireMsg::ParameterStatus { name: Bytes::text("server_version"), value: Bytes::text("18.6") },
            WireMsg::ParameterStatus { name: Bytes::text("client_encoding"), value: Bytes::text("UTF8") },
            WireMsg::BackendKeyData { pid: 4242, key: Bytes(vec![9, 8, 7, 6]) },
            WireMsg::NoticeResponse(fields(&[('S', "WARNING"), ('M', "hello")])),
            ready('I'),
        ]
    }

    #[test]
    fn handshake_trust_retains_every_startup_message() {
        let mut c = Client::over(FakeStream::new(server(&startup_tail())));
        let opts = ConnectOpts::new("h", 1, "db", "fuzz").param("application_name", "sitediff");
        c.handshake(&opts).unwrap();
        assert_eq!(c.connect_wire(), &startup_tail()[..]);
        assert_eq!(c.backend_pid(), 4242);
        assert_eq!(c.cancel_key(), &[9, 8, 7, 6]);
        assert_eq!(c.parameter("server_version"), Some(&b"18.6"[..]));
        assert_eq!(c.negotiated_minor(), 0);
        assert_eq!(c.txn_status(), 'I');
        // Exactly one packet written: the startup message with the extra key.
        let w = &c.stream.written;
        assert_eq!(w, &encode_startup(Protocol::V3_0, "fuzz", "db", &[("application_name".into(), "sitediff".into())]));
    }

    #[test]
    fn handshake_cleartext_password() {
        let mut script = vec![WireMsg::Authentication { kind: 3, data: Bytes(vec![]) }];
        script.extend(startup_tail());
        let mut c = Client::over(FakeStream::new(server(&script)));
        c.handshake(&ConnectOpts::new("h", 1, "db", "fuzz").password("s3cret")).unwrap();
        let frames = split_frames(&c.stream.written[encode_startup(Protocol::V3_0, "fuzz", "db", &[]).len()..]);
        assert_eq!(frames, vec![(b'p', b"s3cret\0".to_vec())]);
        assert_eq!(c.connect_wire().len(), 7);
    }

    #[test]
    fn handshake_md5_password() {
        let mut script = vec![WireMsg::Authentication { kind: 5, data: Bytes(vec![1, 2, 3, 4]) }];
        script.extend(startup_tail());
        let mut c = Client::over(FakeStream::new(server(&script)));
        c.handshake(&ConnectOpts::new("h", 1, "db", "fuzz").password("secret")).unwrap();
        let frames = split_frames(&c.stream.written[encode_startup(Protocol::V3_0, "fuzz", "db", &[]).len()..]);
        assert_eq!(frames, vec![(b'p', b"md5ce113c52f282067a4b65b4fbb18c534e\0".to_vec())]);
    }

    #[test]
    fn handshake_scram_full_exchange() {
        // Server side computed with the same primitives (the primitives
        // themselves are pinned by the RFC 7677 vector above); the
        // handshake test proves the plumbing: mechanism selection, nonce
        // injection, client-final framing, server-signature verification.
        let nonce = "rOprNGfwEbeRWgbNEkqO";
        let server_nonce = format!("{nonce}%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0");
        let server_first = format!("r={server_nonce},s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096");
        let salt = b64_decode("W22ZaJ0SNY7soEsUEjb6gQ==").unwrap();
        let salted = scram_salted_password(b"pencil", &salt, 4096);
        let auth_message = format!("n=,r={nonce},{server_first},c=biws,r={server_nonce}");
        let server_key = hmac_sha256(&salted, b"Server Key");
        let server_sig = b64_encode(&hmac_sha256(&server_key, auth_message.as_bytes()));
        let client_key = hmac_sha256(&salted, b"Client Key");
        let client_sig = hmac_sha256(&pg_sha2::sha256(&client_key), auth_message.as_bytes());
        let proof: Vec<u8> = client_key.iter().zip(client_sig.iter()).map(|(a, b)| a ^ b).collect();

        let mut script = vec![
            WireMsg::Authentication { kind: 10, data: Bytes(b"SCRAM-SHA-256-PLUS\0SCRAM-SHA-256\0\0".to_vec()) },
            WireMsg::Authentication { kind: 11, data: Bytes::text(&server_first) },
            WireMsg::Authentication { kind: 12, data: Bytes::text(&format!("v={server_sig}")) },
        ];
        script.extend(startup_tail());
        let mut c = Client::over(FakeStream::new(server(&script)));
        let mut opts = ConnectOpts::new("h", 1, "db", "fuzz").password("pencil");
        opts.scram_nonce = Some(nonce.to_string());
        c.handshake(&opts).unwrap();
        let frames = split_frames(&c.stream.written[encode_startup(Protocol::V3_0, "fuzz", "db", &[]).len()..]);
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].0, b'p');
        let mut init = b"SCRAM-SHA-256\0".to_vec();
        let first = format!("n,,n=,r={nonce}");
        init.extend_from_slice(&(first.len() as u32).to_be_bytes());
        init.extend_from_slice(first.as_bytes());
        assert_eq!(frames[0].1, init);
        assert_eq!(frames[1].0, b'p');
        assert_eq!(frames[1].1, format!("c=biws,r={server_nonce},p={}", b64_encode(&proof)).into_bytes());
        // All three R messages retained in order.
        let kinds: Vec<i32> = c
            .connect_wire()
            .iter()
            .filter_map(|m| match m {
                WireMsg::Authentication { kind, .. } => Some(*kind),
                _ => None,
            })
            .collect();
        assert_eq!(kinds, vec![10, 11, 12, 0]);

        // A bad server signature is refused, wire retained through the R 12.
        let mut bad = script.clone();
        bad[2] = WireMsg::Authentication { kind: 12, data: Bytes::text("v=AAAA") };
        let mut c = Client::over(FakeStream::new(server(&bad)));
        let e = c.handshake(&opts).unwrap_err();
        assert!(e.detail.contains("incorrect server signature"), "{}", e.detail);
        assert_eq!(e.wire.len(), 3);
        assert!(!e.hang);
    }

    #[test]
    fn handshake_refuses_unknown_mechanisms_and_missing_password() {
        let script = vec![WireMsg::Authentication { kind: 10, data: Bytes(b"GSSAPI\0\0".to_vec()) }];
        let mut c = Client::over(FakeStream::new(server(&script)));
        let e = c.handshake(&ConnectOpts::new("h", 1, "db", "fuzz").password("x")).unwrap_err();
        assert!(e.detail.contains("none of the server's SASL"), "{}", e.detail);
        assert_eq!(e.wire.len(), 1);

        let script = vec![WireMsg::Authentication { kind: 5, data: Bytes(vec![1, 2, 3, 4]) }];
        let mut c = Client::over(FakeStream::new(server(&script)));
        let e = c.handshake(&ConnectOpts::new("h", 1, "db", "fuzz")).unwrap_err();
        assert!(e.detail.contains("no password supplied for authentication method 5"), "{}", e.detail);

        let script = vec![WireMsg::Authentication { kind: 7, data: Bytes(vec![]) }];
        let mut c = Client::over(FakeStream::new(server(&script)));
        let e = c.handshake(&ConnectOpts::new("h", 1, "db", "fuzz")).unwrap_err();
        assert!(e.detail.contains("authentication method 7 not supported"), "{}", e.detail);
    }

    #[test]
    fn handshake_records_fatal_before_auth_ok() {
        // The first ErrorResponse severity before AuthenticationOk is a
        // differential surface (plan §5.6): it is retained in the error.
        let script = vec![WireMsg::ErrorResponse(fields(&[
            ('S', "FATAL"),
            ('V', "FATAL"),
            ('C', "3D000"),
            ('M', "database \"nope\" does not exist"),
        ]))];
        let mut c = Client::over(FakeStream::new(server(&script)));
        let e = c.handshake(&ConnectOpts::new("h", 1, "nope", "fuzz")).unwrap_err();
        assert_eq!(e.detail, "startup failed: 3D000 database \"nope\" does not exist");
        assert_eq!(e.wire, script);
        // Server closing without a word: Lost, empty wire.
        let mut c = Client::over(FakeStream::new(Vec::new()));
        let e = c.handshake(&ConnectOpts::new("h", 1, "db", "fuzz")).unwrap_err();
        assert!(e.detail.starts_with("server closed"), "{}", e.detail);
        assert!(e.wire.is_empty());
    }

    #[test]
    fn handshake_protocol_32_negotiation_and_long_key() {
        let mut script = vec![WireMsg::NegotiateProtocolVersion {
            minor: 0,
            unknown_options: vec![Bytes::text("_pq_.nope")],
        }];
        script.push(WireMsg::Authentication { kind: 0, data: Bytes(vec![]) });
        script.push(WireMsg::BackendKeyData { pid: 1, key: Bytes((0..32).collect()) });
        script.push(ready('I'));
        let mut c = Client::over(FakeStream::new(server(&script)));
        let opts = ConnectOpts::new("h", 1, "db", "fuzz").protocol(Protocol::V3_2).param("_pq_.nope", "1");
        c.handshake(&opts).unwrap();
        assert_eq!(&c.stream.written[4..8], &[0, 3, 0, 2]);
        assert_eq!(c.negotiated_minor(), 0, "server negotiated down to 3.0");
        assert_eq!(c.cancel_key().len(), 32);
        assert!(matches!(c.connect_wire()[0], WireMsg::NegotiateProtocolVersion { .. }));
        assert_eq!(encode_cancel_request(c.backend_pid(), c.cancel_key()).len(), 44);

        // No negotiation under 3.2: the requested minor stands.
        let mut c = Client::over(FakeStream::new(server(&startup_tail())));
        c.handshake(&ConnectOpts::new("h", 1, "db", "fuzz").protocol(Protocol::V3_2)).unwrap();
        assert_eq!(c.negotiated_minor(), 2);
    }

    // ------------------------------------------------------------------
    // Exchanges over canned streams
    // ------------------------------------------------------------------

    #[test]
    fn simple_exchange_retains_everything_in_order() {
        let desc = vec![
            ColDesc { name: Bytes::text("a"), tableoid: 16401, attnum: 1, typoid: 23, typlen: 4, typmod: -1, fmt: 0 },
            ColDesc { name: Bytes::text("?column?"), tableoid: 0, attnum: 0, typoid: 25, typlen: -1, typmod: -1, fmt: 0 },
        ];
        let script = vec![
            WireMsg::ParameterStatus { name: Bytes::text("client_encoding"), value: Bytes::text("UTF8") },
            WireMsg::NoticeResponse(fields(&[('S', "INFO"), ('V', "INFO"), ('C', "00000"), ('M', "analyzing \"public.t\"")])),
            complete("ANALYZE"),
            WireMsg::RowDescription(desc),
            WireMsg::DataRow(vec![Some(Bytes::text("1")), None]),
            WireMsg::DataRow(vec![Some(Bytes::text("2")), Some(Bytes(vec![0xff, 0x00, 0x41]))]),
            complete("SELECT 2"),
            WireMsg::NotificationResponse { pid: 41233, channel: Bytes::text("fz_chan"), payload: Bytes::text("hello") },
            WireMsg::EmptyQueryResponse,
            ready('T'),
        ];
        let mut c = Client::over(FakeStream::new(server(&script)));
        let x = c.simple("ANALYZE t; SELECT a, '\\xff0041'::bytea::text FROM t; NOTIFY fz_chan; ;");
        assert_eq!(x.fault, None);
        assert_eq!(x.wire, script);
        assert_eq!(c.txn_status(), 'T');
        assert_eq!(c.parameter("client_encoding"), Some(&b"UTF8"[..]));
        assert_eq!(sent_types(&c.stream.written), "Q");
        assert!(x.completed());
        assert_eq!(x.notices().len(), 1);

        // The adapter fold reproduces the old RawResult split.
        let rs = fold_simple(&x.wire);
        assert_eq!(rs.len(), 3);
        assert_eq!(rs[0].cmd_tag, "ANALYZE");
        assert_eq!(rs[1].col_oids, vec![23, 25]);
        assert_eq!(rs[1].rows, vec![vec![Some("1".into()), None], vec![Some("2".into()), Some("\u{fffd}\0A".into())]]);
        assert_eq!(rs[1].cmd_tag, "SELECT 2");
        assert_eq!(rs[2].cmd_tag, "");
    }

    #[test]
    fn simple_error_exchange_keeps_all_fields_and_ready_status() {
        let rec = ObservationRecord::from_json(&json::parse(&fixture("observation-error.json")).unwrap()).unwrap();
        let mut c = Client::over(FakeStream::new(server(&rec.wire)));
        let x = c.simple("INSERT INTO t VALUES (NULL, 1)");
        assert_eq!(x.wire, rec.wire);
        assert_eq!(x.first_sqlstate().as_deref(), Some("23502"));
        assert_eq!(c.txn_status(), 'E');
        let rs = c.simple_query("SELECT 1").unwrap_err();
        assert!(rs.0.starts_with("server closed"), "{}", rs.0);
        // Old adapter shape on the same wire.
        let rs = fold_simple(&x.wire);
        assert_eq!(rs.len(), 1);
        assert_eq!(
            rs[0].error,
            Some(("23502".into(), "null value in column \"a\" of relation \"t\" violates not-null constraint".into()))
        );
    }

    #[test]
    fn fatal_then_close_annotates_the_loss_with_the_error() {
        let script = vec![WireMsg::ErrorResponse(fields(&[('S', "FATAL"), ('C', "57P01"), ('M', "terminating connection")]))];
        let mut c = Client::over(FakeStream::new(server(&script)));
        let x = c.simple("SELECT pg_sleep(1)");
        assert_eq!(x.wire, script);
        let Some(Fault::Lost(d)) = &x.fault else { panic!("{:?}", x.fault) };
        assert_eq!(d, "server closed the connection unexpectedly after server error 57P01: terminating connection");
        assert_eq!(c.dead(), Some(d.as_str()));
        // Sticky: the next exchange fails immediately with the same detail.
        let x2 = c.simple("SELECT 1");
        assert_eq!(x2.fault, Some(Fault::Lost(d.clone())));
        assert!(x2.wire.is_empty());
        assert_eq!(c.simple_query("SELECT 1").unwrap_err().0, *d);
    }

    #[test]
    fn framing_loss_poisons() {
        let mut input = vec![b'Z'];
        input.extend_from_slice(&(-5i32).to_be_bytes());
        let mut c = Client::over(FakeStream::new(input));
        let x = c.simple("SELECT 1");
        let Some(Fault::Lost(d)) = x.fault else { panic!() };
        assert!(d.starts_with("lost synchronization"), "{d}");
    }

    #[test]
    fn extended_step_named_statement_portal_describe_and_resume() {
        let script = vec![
            WireMsg::ParseComplete,
            WireMsg::ParameterDescription(vec![23]),
            WireMsg::RowDescription(simple_desc(&[(23, 0)])),
            WireMsg::BindComplete,
            WireMsg::DataRow(vec![Some(Bytes::text("1"))]),
            WireMsg::PortalSuspended,
            WireMsg::DataRow(vec![Some(Bytes::text("2"))]),
            complete("SELECT 1"),
            ready('I'),
        ];
        let mut c = Client::over(FakeStream::new(server(&script)));
        let mut step = ExtendedStep::new("SELECT a FROM t WHERE b = $1 ORDER BY a");
        step.stmt = "ps1".into();
        step.portal = "p1".into();
        step.param_oids = vec![23];
        step.params = vec![WireParam::text(Some("2"))];
        step.describe = Some(Describe::Statement);
        step.limit = 1;
        let x = c.extended(&step);
        assert_eq!(x.fault, None);
        assert_eq!(x.wire, script);
        // Parse, Describe S, Bind, Execute, Flush; resume Execute + Flush; Sync.
        assert_eq!(sent_types(&c.stream.written), "PDBEHEHS");
        let frames = split_frames(&c.stream.written);
        assert_eq!(frames[1].1, b"Sps1\0");
        assert_eq!(&frames[3].1, b"p1\0\0\0\0\x01");
        assert_eq!(&frames[5].1, b"p1\0\0\0\0\0", "resume names the same portal, limit 0");
        let r = fold_extended(&x.wire);
        assert_eq!(r.rows, vec![vec![Some("1".into())], vec![Some("2".into())]]);
        assert_eq!(r.cmd_tag, "SELECT 1");
    }

    #[test]
    fn extended_query_adapter_merges_suspended_portal_batches() {
        let script = vec![
            WireMsg::ParseComplete,
            WireMsg::BindComplete,
            WireMsg::RowDescription(simple_desc(&[(23, 0)])),
            WireMsg::DataRow(vec![Some(Bytes::text("1"))]),
            WireMsg::PortalSuspended,
            WireMsg::DataRow(vec![Some(Bytes::text("2"))]),
            complete("SELECT 1"),
            ready('I'),
        ];
        let mut c = Client::over(FakeStream::new(server(&script)));
        let results = c.extended_query("SELECT k FROM t;", &[WireParam::text(Some("7"))], 1, false).unwrap();
        assert_eq!(results.len(), 1);
        let r = &results[0];
        assert!(r.error.is_none());
        assert_eq!(r.col_oids, vec![23]);
        assert_eq!(r.rows, vec![vec![Some("1".to_string())], vec![Some("2".to_string())]]);
        assert_eq!(r.cmd_tag, "SELECT 1");
        let w = &c.stream.written;
        assert_eq!(count_sent(w, b'P'), 1, "one Parse");
        assert_eq!(count_sent(w, b'B'), 1, "one Bind");
        assert_eq!(count_sent(w, b'E'), 2, "Execute + resume Execute");
        assert_eq!(count_sent(w, b'H'), 2, "Flush after each Execute");
        assert_eq!(count_sent(w, b'S'), 1, "one Sync");
        assert_eq!(count_sent(w, b'D'), 1, "Describe P");
    }

    #[test]
    fn extended_unresumed_suspension_syncs() {
        let script = vec![
            WireMsg::ParseComplete,
            WireMsg::BindComplete,
            WireMsg::NoData,
            WireMsg::DataRow(vec![Some(Bytes::text("1"))]),
            WireMsg::PortalSuspended,
            ready('I'),
        ];
        let mut c = Client::over(FakeStream::new(server(&script)));
        let mut step = ExtendedStep::new("SELECT 1");
        step.limit = 1;
        step.resume = false;
        let x = c.extended(&step);
        assert_eq!(x.fault, None);
        assert_eq!(sent_types(&c.stream.written), "PBDEHS");
    }

    #[test]
    fn extended_query_binary_results_decode() {
        let script = vec![
            WireMsg::ParseComplete,
            WireMsg::BindComplete,
            WireMsg::RowDescription(simple_desc(&[(23, 1), (701, 1), (25, 1)])),
            WireMsg::DataRow(vec![
                Some(Bytes(7i32.to_be_bytes().to_vec())),
                Some(Bytes(2.25f64.to_be_bytes().to_vec())),
                Some(Bytes::text("hi")),
            ]),
            complete("SELECT 1"),
            ready('I'),
        ];
        let mut c = Client::over(FakeStream::new(server(&script)));
        let results = c.extended_query("SELECT ...", &[], 0, true).unwrap();
        assert_eq!(
            results[0].rows,
            vec![vec![Some("\\x00000007".to_string()), Some("2.25".to_string()), Some("hi".to_string())]]
        );
        let bind = Frame::Bind { portal: String::new(), stmt: String::new(), params: vec![], result_fmts: vec![1] }.encode();
        let w = &c.stream.written;
        assert!(w.windows(bind.len()).any(|win| win == &bind[..]), "all-binary Bind not on the wire");
    }

    #[test]
    fn binary_cells_decode_per_policy() {
        assert_eq!(decode_binary_cell(23, &42i32.to_be_bytes()), "\\x0000002a");
        assert_eq!(decode_binary_cell(701, &1.5f64.to_be_bytes()), "1.5");
        assert_eq!(decode_binary_cell(701, &(0.1f64 + 0.2f64).to_be_bytes()), "0.30000000000000004");
        assert_eq!(decode_binary_cell(701, &f64::INFINITY.to_be_bytes()), "Infinity");
        assert_eq!(decode_binary_cell(701, &f64::NEG_INFINITY.to_be_bytes()), "-Infinity");
        assert_eq!(decode_binary_cell(701, &f64::NAN.to_be_bytes()), "NaN");
        assert_eq!(decode_binary_cell(700, &1.25f32.to_be_bytes()), "1.25");
        assert_eq!(decode_binary_cell(700, &f32::NAN.to_be_bytes()), "NaN");
        assert_eq!(decode_binary_cell(25, b"alpha"), "alpha");
        assert_eq!(decode_binary_cell(1043, b""), "");
        assert_eq!(decode_binary_cell(1700, &[0, 1, 0, 0, 0, 0, 0, 1, 0, 5]), "\\x00010000000000010005");
        assert_eq!(decode_binary_cell(701, &[1, 2]), "\\x0102");
    }

    #[test]
    fn extended_query_error_is_captured_and_synced() {
        let script = vec![
            WireMsg::ParseComplete,
            WireMsg::ErrorResponse(fields(&[('S', "ERROR"), ('C', "22012"), ('M', "division by zero")])),
            ready('I'),
        ];
        let mut c = Client::over(FakeStream::new(server(&script)));
        let results = c.extended_query("SELECT 1/0;", &[], 0, false).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].error, Some(("22012".to_string(), "division by zero".to_string())));
        assert_eq!(count_sent(&c.stream.written, b'S'), 1);
        assert_eq!(count_sent(&c.stream.written, b'E'), 1, "no resume after an error");
    }

    #[test]
    fn pipeline_batches_frames_then_one_sync_and_counts_readies() {
        // Two statements pipelined: Parse ps1, Bind, Execute, Parse ps2,
        // Bind, Execute — one Sync appended by the client, one Z expected.
        let script = vec![
            WireMsg::ParseComplete,
            WireMsg::BindComplete,
            complete("INSERT 0 1"),
            WireMsg::ParseComplete,
            WireMsg::BindComplete,
            WireMsg::DataRow(vec![Some(Bytes::text("1"))]),
            complete("SELECT 1"),
            WireMsg::CloseComplete,
            ready('I'),
        ];
        let mut c = Client::over(FakeStream::new(server(&script)));
        let frames = vec![
            Frame::Parse { stmt: "ps1".into(), sql: "INSERT INTO t VALUES ($1)".into(), param_oids: vec![] },
            Frame::Bind { portal: "".into(), stmt: "ps1".into(), params: vec![WireParam::text(Some("1"))], result_fmts: vec![] },
            Frame::Execute { portal: "".into(), limit: 0 },
            Frame::Parse { stmt: "ps2".into(), sql: "SELECT 1".into(), param_oids: vec![] },
            Frame::Bind { portal: "".into(), stmt: "ps2".into(), params: vec![], result_fmts: vec![] },
            Frame::Execute { portal: "".into(), limit: 0 },
            Frame::Close { kind: Describe::Statement, name: "ps1".into() },
        ];
        let x = c.pipeline(&frames, None);
        assert_eq!(x.fault, None);
        assert_eq!(x.wire, script);
        assert_eq!(sent_types(&c.stream.written), "PBEPBECS");

        // Explicit Syncs: two of them, two ReadyForQuery, no extra Sync.
        let script = vec![WireMsg::ParseComplete, ready('I'), WireMsg::ErrorResponse(fields(&[('C', "42601"), ('M', "syntax error")])), ready('I')];
        let mut c = Client::over(FakeStream::new(server(&script)));
        let frames = vec![
            Frame::Parse { stmt: "".into(), sql: "SELECT 1".into(), param_oids: vec![] },
            Frame::Sync,
            Frame::Parse { stmt: "".into(), sql: "SELEC".into(), param_oids: vec![] },
            Frame::Sync,
        ];
        let x = c.pipeline(&frames, None);
        assert_eq!(x.fault, None);
        assert_eq!(x.wire, script);
        assert_eq!(sent_types(&c.stream.written), "PSPS");
        assert_eq!(x.errors().len(), 1);
    }

    #[test]
    fn copy_out_payload_is_retained_and_folded() {
        let script = vec![
            WireMsg::CopyOutResponse { fmt: 1, col_fmts: vec![1, 1] },
            WireMsg::CopyData(Bytes(b"PGCOPY\n".to_vec())),
            WireMsg::CopyData(Bytes(b"\xff\x0d\x0a\x00".to_vec())),
            WireMsg::CopyDone,
            complete("COPY 2"),
            ready('I'),
        ];
        let mut c = Client::over(FakeStream::new(server(&script)));
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
        let script = vec![WireMsg::CopyInResponse { fmt: 1, col_fmts: vec![1] }, complete("COPY 3"), ready('I')];
        let mut c = Client::over(FakeStream::new(server(&script)));
        let payload = b"PGCOPY\n\xff\x0d\x0a\x00binarybytes";
        let results = c.copy_in("COPY t FROM STDIN (FORMAT binary);", payload).unwrap();
        assert_eq!(results.len(), 1);
        assert!(results[0].was_copy);
        assert_eq!(results[0].cmd_tag, "COPY 3");
        assert_eq!(sent_types(&c.stream.written), "Qdc");
        let want = Frame::CopyData(payload.to_vec()).encode();
        assert!(c.stream.written.windows(want.len()).any(|win| win == &want[..]));

        // Extended path: CopyIn fed from the step, Sync after CommandComplete.
        let mut c = Client::over(FakeStream::new(server(&[
            WireMsg::ParseComplete,
            WireMsg::BindComplete,
            WireMsg::NoData,
            WireMsg::CopyInResponse { fmt: 0, col_fmts: vec![0] },
            complete("COPY 1"),
            ready('I'),
        ])));
        let mut step = ExtendedStep::new("COPY t FROM STDIN");
        step.copy_in = Some(b"1\n".to_vec());
        let x = c.extended(&step);
        assert_eq!(x.fault, None);
        assert_eq!(sent_types(&c.stream.written), "PBDEHdcS");
    }

    #[test]
    fn copy_in_without_payload_is_failed() {
        let script = vec![
            WireMsg::CopyInResponse { fmt: 0, col_fmts: vec![0] },
            WireMsg::ErrorResponse(fields(&[('S', "ERROR"), ('C', "57014"), ('M', "COPY failed")])),
            ready('I'),
        ];
        let mut c = Client::over(FakeStream::new(server(&script)));
        let results = c.simple_query("COPY t FROM STDIN;").unwrap();
        assert_eq!(count_sent(&c.stream.written, b'f'), 1, "CopyFail sent");
        assert!(results.iter().any(|r| r.error.is_some()));
    }

    #[test]
    fn raw_frames_go_out_verbatim_and_read_to_ready() {
        let script = vec![WireMsg::ErrorResponse(fields(&[('S', "FATAL"), ('C', "08P01"), ('M', "invalid frontend message type 7")]))];
        let mut c = Client::over(FakeStream::new(server(&script)));
        let junk = [7u8, 0, 0, 0, 9, 1, 2, 3, 4, 5];
        let x = c.raw(&junk, 1);
        assert_eq!(c.stream.written, junk.to_vec());
        assert_eq!(x.wire, script);
        let Some(Fault::Lost(d)) = x.fault else { panic!() };
        assert!(d.contains("after server error 08P01"), "{d}");

        // Terminate sends 'X' and closes the client side.
        let mut c = Client::over(FakeStream::new(Vec::new()));
        c.terminate();
        assert_eq!(c.stream.written, Frame::Terminate.encode());
        assert_eq!(c.dead(), Some("terminated by client"));
    }

    #[test]
    fn drain_collects_async_traffic_to_ready() {
        let script = vec![
            WireMsg::NotificationResponse { pid: 1, channel: Bytes::text("c"), payload: Bytes(vec![]) },
            WireMsg::ParameterStatus { name: Bytes::text("TimeZone"), value: Bytes::text("UTC") },
            ready('I'),
        ];
        let mut c = Client::over(FakeStream::new(server(&script)));
        let x = c.drain();
        assert_eq!(x.wire, script);
        assert!(c.stream.written.is_empty());
        assert_eq!(c.parameter("TimeZone"), Some(&b"UTC"[..]));
    }

    // ------------------------------------------------------------------
    // Deadlines against a real socket that never answers
    // ------------------------------------------------------------------

    #[test]
    fn read_timeout_maps_to_hang_not_a_block() {
        // A bound listener accepts the TCP handshake in the kernel backlog
        // and never speaks — the classic hung backend.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let opts = ConnectOpts::new("127.0.0.1", port, "db", "fuzz").timeouts(
            Some(Duration::from_secs(5)),
            Some(Duration::from_millis(150)),
            Some(Duration::from_secs(5)),
        );
        let started = Instant::now();
        let e = Client::connect_with(&opts).unwrap_err();
        assert!(e.hang, "{}", e.detail);
        assert!(e.wire.is_empty());
        assert!(e.detail.contains("150 ms"), "{}", e.detail);
        assert!(started.elapsed() < Duration::from_secs(4), "did not honor the deadline");

        // An established client (as if authenticated) hangs on an exchange
        // the same way, keeps what it received, and is not poisoned.
        let stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
        let mut c = Client::over(stream);
        c.set_read_timeout(Some(Duration::from_millis(120))).unwrap();
        c.set_write_timeout(Some(Duration::from_secs(5))).unwrap();
        let x = c.simple("SELECT pg_sleep(1000)");
        assert_eq!(x.fault, Some(Fault::Hang { timeout_ms: 120 }));
        assert!(x.wire.is_empty());
        assert!(c.dead().is_none(), "a hang must leave the connection usable for cancel/drain");
        assert_eq!(c.simple_query("SELECT 1").unwrap_err().0, "read timed out after 120 ms (hang)");
        assert!(c.cancel().is_err(), "no BackendKeyData yet");
        // A CancelRequest to the silent listener still goes out (the
        // kernel accepts; the packet is written), proving the second
        // socket path.
        send_cancel("127.0.0.1", port, 1, &[1, 2, 3, 4], Some(Duration::from_secs(5))).unwrap();
        drop(listener);
    }
}
