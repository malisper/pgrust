// In-server postgres-wire CLIENT (fast carries no libpq): conninfo, startup +
// auth (trust, password, md5, SCRAM-SHA-256), simple query (buffered and
// row-streamed), async send/poll, cancel protocol, COPY framing. Consumers:
// walreceiver, dblink; designed for postgres_fdw (docs/design/pgclient-seam.md).
//
// Blocking discipline (invariant): every wait is WaitLatchOrSocket(latch |
// socket) -> ResetLatch -> CHECK_FOR_INTERRUPTS -> nonblocking pump; no
// blocking syscall outside the EAGAIN wait (libpq-be-fe-helpers.h contract).
use std::os::fd::RawFd;

use types_core::{pgsocket, Oid};
use types_error::PgResult;
use types_storage::waiteventset::{
    WL_EXIT_ON_PM_DEATH, WL_LATCH_SET, WL_SOCKET_READABLE, WL_SOCKET_WRITEABLE, WL_TIMEOUT,
};

pub mod conninfo;
pub use conninfo::{opt, parse_conninfo, resolve_conninfo, ConnOption, CONNINFO_OPTIONS};

mod auth;

pub const PG_PROTOCOL_3_0: u32 = 3 << 16;
const CANCEL_REQUEST_CODE: u32 = (1234 << 16) | 5678;

#[derive(Clone, Copy)]
pub struct WaitEvents {
    pub connect: u32,
    pub receive: u32,
}

enum Stream {
    Tcp(std::net::TcpStream),
    // wasm32: std::os::unix is absent on wasi (no AF_UNIX on WASI p1); the
    // unix-socket conninfo arm refuses at connect instead.
    #[cfg(not(target_family = "wasm"))]
    Unix(std::os::unix::net::UnixStream),
}

#[derive(Clone)]
enum DialTarget {
    Tcp(String, u16),
    Unix(String),
}

pub struct PgConn {
    _stream: Stream,
    fd: RawFd,
    target: DialTarget,
    inbuf: Vec<u8>,
    inpos: usize,
    conn_ok: bool,
    in_copy: bool,
    copy_server_done: bool,
    copy_client_done: bool,
    // A query was sent and ReadyForQuery not yet consumed (libpq asyncStatus
    // != IDLE); get_result on an idle connection returns None immediately.
    pending_results: bool,
    // libpq conn->xactStatus: the status byte of the last ReadyForQuery
    // ('I' idle / 'T' in transaction / 'E' failed transaction).
    txn_status: u8,
    err: String,
    opts: Vec<(String, String)>,
    display_host: String,
    display_port: i32,
    be_pid: i32,
    be_key: Vec<u8>,
    server_version: i32,
    used_password: bool,
    params: Vec<(String, String)>,
    notifies: std::collections::VecDeque<Notify>,
    we: WaitEvents,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ExecStatus {
    CommandOk,
    TuplesOk,
    CopyBoth,
    CopyIn,
    CopyOut,
    Empty,
    Error,
}

// ErrorResponse diag fields (PQresultErrorField's PG_DIAG_* surface).
#[derive(Default, Clone)]
pub struct ErrorFields {
    pub severity: String,
    pub sqlstate: String,
    pub primary: String,
    pub detail: String,
    pub hint: String,
    pub context: String,
}

pub struct QueryResult {
    pub status: ExecStatus,
    pub nfields: usize,
    pub rows: Vec<Vec<Option<Vec<u8>>>>,
    // CommandComplete tag (PQcmdStatus), e.g. "INSERT 0 1".
    pub cmd_tag: String,
    pub diag: Option<ErrorFields>,
    pub err: String,
}

impl QueryResult {
    /// A connection-level failure as a result value (no server diag) —
    /// callers shaping libpq's "PGresult == NULL" arms (pgfdw_report_error
    /// falls back to PQerrorMessage) build these.
    pub fn conn_error(err: String) -> Self {
        Self::error(err)
    }

    fn error(err: String) -> Self {
        QueryResult {
            status: ExecStatus::Error,
            nfields: 0,
            rows: Vec::new(),
            cmd_tag: String::new(),
            diag: None,
            err,
        }
    }

    fn status_only(status: ExecStatus) -> Self {
        QueryResult {
            status,
            nfields: 0,
            rows: Vec::new(),
            cmd_tag: String::new(),
            diag: None,
            err: String::new(),
        }
    }
}

pub struct Notify {
    pub channel: String,
    pub be_pid: i32,
    pub extra: String,
}

pub enum CopyData {
    Msg(Vec<u8>),
    Block,
    End,
}

// Sink for row-streamed execution (libpq single-row mode's shape): one
// result_start per resultset (dblink recreates its tuplestore there), one
// row per DataRow. result_start sees the connection read-only at
// first-row-in-hand — after any ParameterStatus messages, which dblink's
// remote-GUC mirroring depends on. Errors unwind through exec_streaming;
// the caller is expected to drain() the connection on the error path.
pub trait RowSink {
    fn result_start(&mut self, conn: &PgConn, nfields: usize) -> PgResult<()>;
    fn row(&mut self, cols: &[Option<&[u8]>]) -> PgResult<()>;
}

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

pub(crate) fn cstr_at(b: &[u8], pos: usize) -> (String, usize) {
    let end = b[pos..].iter().position(|&c| c == 0).map(|e| pos + e).unwrap_or(b.len());
    (String::from_utf8_lossy(&b[pos..end]).into_owned(), end + 1)
}

fn parse_diag(body: &[u8]) -> ErrorFields {
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
            b'W' => f.context = val,
            _ => {}
        }
        i = next;
    }
    f
}

// ErrorResponse/NoticeResponse body -> libpq default-verbosity message
// (pqBuildErrorMessage3): "SEVERITY:  message" plus DETAIL/HINT/CONTEXT
// lines. The walreceiver relays the whole thing into the local log —
// 019_replslot_limit greps the standby log for the primary's DETAIL line.
pub(crate) fn parse_error_fields(body: &[u8]) -> String {
    let f = parse_diag(body);
    let mut out = format!("{}:  {}", f.severity, f.primary);
    if !f.detail.is_empty() {
        out.push_str(&format!("\nDETAIL:  {}", f.detail));
    }
    if !f.hint.is_empty() {
        out.push_str(&format!("\nHINT:  {}", f.hint));
    }
    if !f.context.is_empty() {
        out.push_str(&format!("\nCONTEXT:  {}", f.context));
    }
    out
}

pub(crate) fn wait_socket(fd: RawFd, events: u32, wait_event_info: u32) -> PgResult<()> {
    loop {
        let rc = latch::WaitLatchOrSocket(
            init_small::globals::MyLatch(),
            WL_LATCH_SET | WL_EXIT_ON_PM_DEATH | events,
            fd,
            -1,
            wait_event_info,
        )?;
        if rc & WL_LATCH_SET != 0 {
            if let Some(l) = init_small::globals::MyLatch() {
                latch::ResetLatch(l);
            }
            postgres_seams::check_for_interrupts::call()?;
        }
        if rc & events != 0 {
            return Ok(());
        }
    }
}

#[cfg(not(target_family = "wasm"))]
pub fn os_user_name() -> String {
    if let Ok(u) = std::env::var("PGUSER") {
        if !u.is_empty() {
            return u;
        }
    }
    // SAFETY: getpwuid returns a static struct; read name under it.
    unsafe {
        let pw = libc::getpwuid(libc::geteuid());
        if !pw.is_null() && !(*pw).pw_name.is_null() {
            return std::ffi::CStr::from_ptr((*pw).pw_name).to_string_lossy().into_owned();
        }
    }
    std::env::var("USER").unwrap_or_default()
}

// wasm32: no uids and no passwd db on WASI; identity comes through the
// environment (wasmtime --env USER=<name>), the main_main synthetic-identity
// arm's convention.
#[cfg(target_family = "wasm")]
pub fn os_user_name() -> String {
    if let Ok(u) = std::env::var("PGUSER") {
        if !u.is_empty() {
            return u;
        }
    }
    std::env::var("USER").unwrap_or_default()
}

pub fn resolve_user(opts: &[(String, String)]) -> String {
    opt(opts, "user")
        .filter(|u| !u.is_empty())
        .map(|s| s.to_string())
        .unwrap_or_else(os_user_name)
}

pub fn resolve_password(opts: &[(String, String)]) -> Option<String> {
    opt(opts, "password")
        .filter(|p| !p.is_empty())
        .map(|s| s.to_string())
        .or_else(|| std::env::var("PGPASSWORD").ok().filter(|p| !p.is_empty()))
}

// libpq's unix-socket connect leg.
#[cfg(not(target_family = "wasm"))]
fn unix_connect(path: &str) -> Result<(Stream, RawFd), String> {
    use std::os::fd::AsRawFd;
    match std::os::unix::net::UnixStream::connect(path) {
        Ok(s) => {
            let fd = s.as_raw_fd();
            Ok((Stream::Unix(s), fd))
        }
        Err(e) => Err(format!("connection to server on socket \"{path}\" failed: {e}")),
    }
}

// wasm32: no AF_UNIX on WASI p1 — refuse with the C error shape
// (52 = WASI ENOSYS), matching the tablespace wasm arm.
#[cfg(target_family = "wasm")]
fn unix_connect(path: &str) -> Result<(Stream, RawFd), String> {
    Err(format!(
        "connection to server on socket \"{path}\" failed: {}",
        std::io::Error::from_raw_os_error(52)
    ))
}

pub fn validate_port(opts: &[(String, String)]) -> Result<u16, String> {
    // PQconnectPoll's try-next-host arm (fe-connect.c): the port option is
    // validated BEFORE address resolution or any socket attempt. Regress
    // relies on this ordering — CREATE SUBSCRIPTION ... 'port=-1' must fail
    // "invalid port number: \"-1\"" WITHOUT a connection ever being tried.
    let port_s = opt(opts, "port").unwrap_or("").to_string();
    if port_s.is_empty() {
        return Ok(5432); // DEF_PGPORT
    }
    if port_s.contains(',') {
        // unported: multi-host conninfo port list (recorded divergence).
        return Err("connecting to multiple hosts is not supported yet".to_string());
    }
    // pqParseIntParam: strtol with surrounding whitespace allowed, no
    // trailing garbage, overflow-checked into int.
    match port_s.trim_matches(|c: char| c.is_ascii_whitespace()).parse::<i32>() {
        Err(_) => Err(format!(
            "invalid integer value \"{port_s}\" for connection option \"port\""
        )),
        Ok(n) if !(1..=65535).contains(&n) => Err(format!("invalid port number: \"{port_s}\"")),
        Ok(n) => Ok(n as u16),
    }
}

// Dial + startup + auth, stopping at ReadyForQuery. `startup_params` is the
// full parameter list for the startup packet (caller decides user/database/
// replication/...). Ok(Err(msg)) is libpq's CONNECTION_BAD-with-errorMessage
// shape; the caller wraps it into its own ereport.
pub fn connect(
    opts: Vec<(String, String)>,
    startup_params: &[(&str, &str)],
    we: WaitEvents,
) -> PgResult<Result<PgConn, String>> {
    let port = match validate_port(&opts) {
        Ok(p) => p,
        Err(e) => return Ok(Err(e)),
    };
    let host = opt(&opts, "host").unwrap_or("").to_string();
    let hostaddr = opt(&opts, "hostaddr").unwrap_or("").to_string();
    // md5/SCRAM hash the user the server was told about — the startup
    // packet's, not a re-derivation from the options.
    let user = startup_params
        .iter()
        .find(|(k, _)| *k == "user")
        .map(|(_, v)| v.to_string())
        .unwrap_or_else(|| resolve_user(&opts));
    let password = resolve_password(&opts);

    let (stream, fd, target, display_host) = if !host.is_empty() && host.starts_with('/') {
        let path = format!("{host}/.s.PGSQL.{port}");
        match unix_connect(&path) {
            Ok((s, fd)) => (s, fd, DialTarget::Unix(path), host.clone()),
            Err(e) => return Ok(Err(e)),
        }
    } else {
        let target = if !hostaddr.is_empty() { hostaddr.clone() } else { host.clone() };
        let target = if target.is_empty() { "localhost".to_string() } else { target };
        match std::net::TcpStream::connect((target.as_str(), port)) {
            Ok(s) => {
                use std::os::fd::AsRawFd;
                let _ = s.set_nodelay(true);
                let fd = s.as_raw_fd();
                (Stream::Tcp(s), fd, DialTarget::Tcp(target.clone(), port), target)
            }
            Err(e) => {
                return Ok(Err(format!(
                    "connection to server at \"{target}\", port {port} failed: {e}"
                )))
            }
        }
    };

    match &stream {
        Stream::Tcp(s) => s.set_nonblocking(true).expect("set_nonblocking"),
        #[cfg(not(target_family = "wasm"))]
        Stream::Unix(s) => s.set_nonblocking(true).expect("set_nonblocking"),
    }

    // libpq's emitHostIdentityInfo (fe-connect.c), emitted SPECULATIVELY into
    // conn->errorMessage the moment the target address is identified
    // (PQconnectPoll, "all errors ... should be prefixed with host-identity
    // information"): every later failure of this attempt — the startup-packet
    // send, a server-sent FATAL during auth (role/database does not exist),
    // fe_sendauth, a SCRAM failure — carries the prefix. Dial failures above
    // already build it inline.
    let host_identity = match &target {
        DialTarget::Unix(path) => {
            format!("connection to server on socket \"{path}\" failed: ")
        }
        DialTarget::Tcp(disp_host, disp_port) => {
            // C: a CHT_HOST_ADDRESS target (hostaddr=) displays bare; a host
            // NAME whose looked-up address differs textually displays
            // "name" (addr) (emitHostIdentityInfo's strcmp arm).
            let peer_ip = match &stream {
                Stream::Tcp(s) => {
                    s.peer_addr().map(|a| a.ip().to_string()).unwrap_or_default()
                }
                #[cfg(not(target_family = "wasm"))]
                Stream::Unix(_) => String::new(),
            };
            if !hostaddr.is_empty() || peer_ip.is_empty() || peer_ip == *disp_host {
                format!("connection to server at \"{disp_host}\", port {disp_port} failed: ")
            } else {
                format!(
                    "connection to server at \"{disp_host}\" ({peer_ip}), port {disp_port} failed: "
                )
            }
        }
    };

    let mut conn = PgConn {
        _stream: stream,
        fd,
        target,
        inbuf: Vec::new(),
        inpos: 0,
        conn_ok: true,
        in_copy: false,
        copy_server_done: false,
        copy_client_done: false,
        pending_results: false,
        txn_status: b'I',
        err: String::new(),
        opts,
        display_host,
        display_port: port as i32,
        be_pid: 0,
        be_key: Vec::new(),
        server_version: 0,
        used_password: false,
        params: Vec::new(),
        notifies: std::collections::VecDeque::new(),
        we,
    };

    let mut body = Vec::new();
    body.extend_from_slice(&PG_PROTOCOL_3_0.to_be_bytes());
    for (k, v) in startup_params {
        body.extend_from_slice(k.as_bytes());
        body.push(0);
        body.extend_from_slice(v.as_bytes());
        body.push(0);
    }
    body.push(0);
    let mut pkt = Vec::with_capacity(4 + body.len());
    pkt.extend_from_slice(&((body.len() as u32 + 4).to_be_bytes()));
    pkt.extend_from_slice(&body);
    if let Err(e) = conn.send_all(&pkt) {
        return Ok(Err(format!("{host_identity}{e}")));
    }

    match auth::handshake(&mut conn, &user, password.as_deref())? {
        Ok(()) => Ok(Ok(conn)),
        Err(e) => Ok(Err(format!("{host_identity}{e}"))),
    }
}

// Standard SQL connection (libpq PQconnectdb shape): resolve conninfo, build
// the usual startup params, force client_encoding to `encoding` when given
// (dblink pins the remote to the local database encoding). Ok(Err(msg)) is
// libpq's CONNECTION_BAD-with-errorMessage arm.
pub fn connect_db(
    conninfo: &str,
    encoding: Option<&str>,
    we: WaitEvents,
) -> PgResult<Result<PgConn, String>> {
    let opts = match resolve_conninfo(conninfo) {
        Ok(o) => o,
        Err(e) => return Ok(Err(e)),
    };
    let user = resolve_user(&opts);
    let dbname = opt(&opts, "dbname").filter(|s| !s.is_empty()).unwrap_or(&user).to_string();
    let appname = opt(&opts, "application_name").unwrap_or("").to_string();
    let options = opt(&opts, "options").unwrap_or("").to_string();
    let enc = encoding
        .map(|s| s.to_string())
        .or_else(|| opt(&opts, "client_encoding").map(|s| s.to_string()));

    let mut params: Vec<(&str, &str)> = vec![("user", user.as_str()), ("database", dbname.as_str())];
    if !appname.is_empty() {
        params.push(("application_name", appname.as_str()));
    }
    if let Some(e) = enc.as_deref() {
        if !e.is_empty() {
            params.push(("client_encoding", e));
        }
    }
    if !options.is_empty() {
        params.push(("options", options.as_str()));
    }
    connect(opts, &params, we)
}

/// libpq PGTransactionStatusType (PQtransactionStatus).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransactionStatus {
    /// 'I' — connection idle, no transaction open.
    Idle,
    /// A command is in flight (libpq: asyncStatus != PGASYNC_IDLE).
    Active,
    /// 'T' — idle, inside a transaction block.
    InTrans,
    /// 'E' — idle, inside a FAILED transaction block.
    InError,
    /// Bad connection or unrecognized status byte.
    Unknown,
}

impl PgConn {
    pub fn error_message(&self) -> String {
        self.err.clone()
    }

    // PQtransactionStatus (fe-exec.c): UNKNOWN on a bad connection, ACTIVE
    // while a query is pending, else the last ReadyForQuery status byte.
    pub fn transaction_status(&self) -> TransactionStatus {
        if !self.conn_ok {
            return TransactionStatus::Unknown;
        }
        if self.pending_results {
            return TransactionStatus::Active;
        }
        match self.txn_status {
            b'I' => TransactionStatus::Idle,
            b'T' => TransactionStatus::InTrans,
            b'E' => TransactionStatus::InError,
            _ => TransactionStatus::Unknown,
        }
    }

    pub fn connection_bad(&self) -> bool {
        !self.conn_ok
    }

    pub fn socket(&self) -> pgsocket {
        self.fd as pgsocket
    }

    pub fn backend_pid(&self) -> i32 {
        self.be_pid
    }

    pub fn server_version(&self) -> i32 {
        self.server_version
    }

    /// PQconnectionUsedPassword.
    pub fn used_password(&self) -> bool {
        self.used_password
    }

    /// PQparameterStatus.
    pub fn parameter_status(&self, name: &str) -> Option<&str> {
        self.params.iter().find(|(k, _)| k == name).map(|(_, v)| v.as_str())
    }

    /// PQclientEncoding, as the encoding NAME the server last reported.
    pub fn client_encoding(&self) -> Option<&str> {
        self.parameter_status("client_encoding")
    }

    pub fn conn_options(&self) -> &[(String, String)] {
        &self.opts
    }

    pub fn display_conninfo(&self) -> String {
        let mut out = String::new();
        for (k, v) in &self.opts {
            if v.is_empty() {
                continue;
            }
            if !out.is_empty() {
                out.push(' ');
            }
            out.push_str(k);
            out.push('=');
            out.push_str(if k == "password" { "********" } else { v });
        }
        out
    }

    pub fn senderinfo(&self) -> (String, i32) {
        (self.display_host.clone(), self.display_port)
    }

    /// PQnotifies.
    pub fn next_notify(&mut self) -> Option<Notify> {
        self.notifies.pop_front()
    }

    // wasm32: the wasi libc crate exposes no send/recv (WASI p1 has no
    // socket-open surface, so connect() can never have succeeded and these
    // are unreachable); the stubs keep the crate compiling and fail like a
    // dead connection if ever reached (the ip::sys stub convention).
    #[cfg(target_family = "wasm")]
    pub(crate) fn send_all(&mut self, _buf: &[u8]) -> Result<(), String> {
        self.conn_ok = false;
        self.err = format!(
            "could not send data to server: {}",
            std::io::Error::from_raw_os_error(52) // WASI ENOSYS
        );
        Err(self.err.clone())
    }

    #[cfg(not(target_family = "wasm"))]
    pub(crate) fn send_all(&mut self, buf: &[u8]) -> Result<(), String> {
        let mut off = 0;
        while off < buf.len() {
            let n = unsafe {
                libc::send(
                    self.fd,
                    buf[off..].as_ptr().cast(),
                    buf.len() - off,
                    libc::MSG_NOSIGNAL,
                )
            };
            if n > 0 {
                off += n as usize;
                continue;
            }
            let e = std::io::Error::last_os_error();
            match e.raw_os_error() {
                Some(libc::EINTR) => continue,
                Some(libc::EAGAIN) => {
                    if let Err(pe) = wait_socket(self.fd, WL_SOCKET_WRITEABLE, self.we.receive) {
                        self.conn_ok = false;
                        return Err(format!("{pe:?}"));
                    }
                }
                _ => {
                    self.conn_ok = false;
                    self.err = format!("could not send data to server: {e}");
                    return Err(self.err.clone());
                }
            }
        }
        Ok(())
    }

    // PQconsumeInput: drain whatever is readable right now. false == the
    // connection died (self.err set).
    // wasm32: no recv in the wasi libc crate; unreachable stub (see send_all).
    #[cfg(target_family = "wasm")]
    pub fn consume_input(&mut self) -> bool {
        self.conn_ok = false;
        self.err = format!(
            "could not receive data from server: {}",
            std::io::Error::from_raw_os_error(52) // WASI ENOSYS
        );
        false
    }

    #[cfg(not(target_family = "wasm"))]
    pub fn consume_input(&mut self) -> bool {
        if !self.conn_ok {
            return false;
        }
        let mut buf = [0u8; 16384];
        loop {
            let n = unsafe { libc::recv(self.fd, buf.as_mut_ptr().cast(), buf.len(), 0) };
            if n > 0 {
                self.inbuf.extend_from_slice(&buf[..n as usize]);
                if (n as usize) < buf.len() {
                    return true;
                }
                continue;
            }
            if n == 0 {
                self.conn_ok = false;
                self.err = "server closed the connection unexpectedly".into();
                return false;
            }
            let e = std::io::Error::last_os_error();
            return match e.raw_os_error() {
                Some(libc::EINTR) => continue,
                Some(libc::EAGAIN) => true,
                _ => {
                    self.conn_ok = false;
                    self.err = format!("could not receive data from server: {e}");
                    false
                }
            };
        }
    }

    pub(crate) fn next_message(&mut self) -> Option<(u8, Vec<u8>)> {
        let avail = self.inbuf.len() - self.inpos;
        if avail < 5 {
            return None;
        }
        let p = self.inpos;
        let len = be_i32(&self.inbuf[p + 1..p + 5]) as usize;
        if avail < 1 + len {
            return None;
        }
        let t = self.inbuf[p];
        let body = self.inbuf[p + 5..p + 1 + len].to_vec();
        self.inpos = p + 1 + len;
        if self.inpos == self.inbuf.len() {
            self.inbuf.clear();
            self.inpos = 0;
        } else if self.inpos > 65536 {
            self.inbuf.drain(..self.inpos);
            self.inpos = 0;
        }
        Some((t, body))
    }

    pub(crate) fn read_message(
        &mut self,
        wait_event_info: u32,
    ) -> PgResult<Result<(u8, Vec<u8>), String>> {
        loop {
            if let Some(m) = self.next_message() {
                return Ok(Ok(m));
            }
            wait_socket(self.fd, WL_SOCKET_READABLE, wait_event_info)?;
            if !self.consume_input() {
                return Ok(Err(self.err.clone()));
            }
        }
    }

    // Async-message bookkeeping shared by every read loop.
    pub(crate) fn note_async(&mut self, t: u8, body: &[u8]) {
        match t {
            b'S' => {
                let (name, next) = cstr_at(body, 0);
                let (value, _) = cstr_at(body, next);
                if name == "server_version" {
                    let major: i32 = value
                        .split(|c: char| !c.is_ascii_digit())
                        .next()
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0);
                    self.server_version = major * 10000;
                }
                if let Some(slot) = self.params.iter_mut().find(|(k, _)| *k == name) {
                    slot.1 = value;
                } else {
                    self.params.push((name, value));
                }
            }
            b'A' => {
                let be_pid = be_i32(&body[0..4]);
                let (channel, next) = cstr_at(body, 4);
                let (extra, _) = cstr_at(body, next);
                self.notifies.push_back(Notify { channel, be_pid, extra });
            }
            b'K' => {
                self.be_pid = be_i32(&body[0..4]);
                self.be_key = body[4..].to_vec();
            }
            _ => {}
        }
    }

    // PQexec (simple query): send, then collect until ReadyForQuery keeping
    // the last resultset. CopyBothResponse/CopyOutResponse return immediately
    // with the stream open.
    pub fn exec(&mut self, query: &str) -> PgResult<QueryResult> {
        // PQexecStart (fe-exec.c): "Silently discard any prior query result"
        // — a local error can abandon an earlier result stream mid-flight
        // (dblink's storeRow unwind), and PQexec is what makes the connection
        // usable again for the next command.
        if self.pending_results {
            self.drain();
        }
        if !self.send_query_raw(query) {
            return Ok(QueryResult::error(self.err.clone()));
        }

        let mut nfields = 0usize;
        let mut rows: Vec<Vec<Option<Vec<u8>>>> = Vec::new();
        let mut result = QueryResult::status_only(ExecStatus::CommandOk);
        let mut error: Option<(String, ErrorFields)> = None;
        loop {
            let (t, mbody) = match self.read_message(self.we.receive)? {
                Ok(m) => m,
                Err(e) => return Ok(QueryResult::error(e)),
            };
            match t {
                b'T' => {
                    nfields = u16::from_be_bytes([mbody[0], mbody[1]]) as usize;
                    rows.clear();
                }
                b'D' => rows.push(parse_data_row(&mbody)),
                b'C' => {
                    let (tag, _) = cstr_at(&mbody, 0);
                    result = QueryResult {
                        status: if nfields > 0 { ExecStatus::TuplesOk } else { ExecStatus::CommandOk },
                        nfields,
                        rows: std::mem::take(&mut rows),
                        cmd_tag: tag,
                        diag: None,
                        err: String::new(),
                    };
                    nfields = 0;
                }
                b'I' => result = QueryResult::status_only(ExecStatus::Empty),
                b'E' => {
                    let diag = parse_diag(&mbody);
                    self.err = parse_error_fields(&mbody);
                    error = Some((self.err.clone(), diag));
                }
                b'W' => {
                    self.in_copy = true;
                    self.copy_server_done = false;
                    self.copy_client_done = false;
                    return Ok(QueryResult::status_only(ExecStatus::CopyBoth));
                }
                b'H' => {
                    // CopyOutResponse: the caller drains with get_copy_data
                    // until CopyData::End, then get_result for the tail.
                    self.in_copy = true;
                    self.copy_server_done = false;
                    self.copy_client_done = true; // nothing to send on COPY OUT
                    return Ok(QueryResult::status_only(ExecStatus::CopyOut));
                }
                b'S' | b'N' | b'A' => self.note_async(t, &mbody),
                b'Z' => {
                    self.txn_status = mbody.first().copied().unwrap_or(b'I');
                    self.pending_results = false;
                    break;
                }
                other => {
                    self.conn_ok = false;
                    return Ok(QueryResult::error(format!(
                        "unexpected message type \"{}\" from server",
                        other as char
                    )));
                }
            }
        }
        if let Some((e, diag)) = error {
            let mut r = QueryResult::error(e);
            r.diag = Some(diag);
            return Ok(r);
        }
        Ok(result)
    }

    fn send_query_raw(&mut self, query: &str) -> bool {
        let mut body = query.as_bytes().to_vec();
        body.push(0);
        if let Err(e) = self.send_all(&msg(b'Q', &body)) {
            self.err = e;
            return false;
        }
        self.pending_results = true;
        true
    }

    /// PQsendQuery: dispatch without waiting. false == send failed (err set).
    pub fn send_query(&mut self, query: &str) -> bool {
        self.send_query_raw(query)
    }

    // ---- extended query protocol (Parse/Bind/Describe/Execute/Sync) ----
    //
    // The wire shapes mirror libpq fe-exec.c: pqSendQueryGuts (PQsendQueryParams
    // / PQsendQueryPrepared), PQsendPrepare. Params and results travel as TEXT
    // (postgres_fdw transmits params textually and needs no binary results).

    /// PQsendQueryParams: Parse(unnamed) + Bind + Describe(portal) +
    /// Execute + Sync in one flush. `param_types` is either empty (server
    /// infers every type, libpq's paramTypes==NULL) or one Oid per param
    /// (0 = infer). `params` are text values, None = NULL.
    pub fn send_query_params(
        &mut self,
        query: &str,
        param_types: &[Oid],
        params: &[Option<&str>],
    ) -> bool {
        debug_assert!(param_types.is_empty() || param_types.len() == params.len());
        let mut pkt = msg(b'P', &parse_body("", query, param_types, params.len()));
        pkt.extend_from_slice(&msg(b'B', &bind_body("", "", params)));
        pkt.extend_from_slice(&msg(b'D', b"P\0"));
        pkt.extend_from_slice(&msg(b'E', &execute_body("", 0)));
        pkt.extend_from_slice(&msg(b'S', &[]));
        if let Err(e) = self.send_all(&pkt) {
            self.err = e;
            return false;
        }
        self.pending_results = true;
        true
    }

    /// PQsendPrepare: Parse(name) + Sync (no Describe, exactly libpq).
    pub fn send_prepare(&mut self, stmt_name: &str, query: &str, param_types: &[Oid]) -> bool {
        let mut pkt = msg(b'P', &parse_body(stmt_name, query, param_types, param_types.len()));
        pkt.extend_from_slice(&msg(b'S', &[]));
        if let Err(e) = self.send_all(&pkt) {
            self.err = e;
            return false;
        }
        self.pending_results = true;
        true
    }

    /// PQsendQueryPrepared: Bind(stmt) + Describe(portal) + Execute + Sync.
    pub fn send_query_prepared(&mut self, stmt_name: &str, params: &[Option<&str>]) -> bool {
        let mut pkt = msg(b'B', &bind_body("", stmt_name, params));
        pkt.extend_from_slice(&msg(b'D', b"P\0"));
        pkt.extend_from_slice(&msg(b'E', &execute_body("", 0)));
        pkt.extend_from_slice(&msg(b'S', &[]));
        if let Err(e) = self.send_all(&pkt) {
            self.err = e;
            return false;
        }
        self.pending_results = true;
        true
    }

    /// PQexecParams: parameterized query, text params/results, keep the last
    /// resultset (PQexecStart discard discipline included).
    pub fn exec_params(
        &mut self,
        query: &str,
        param_types: &[Oid],
        params: &[Option<&str>],
    ) -> PgResult<QueryResult> {
        if self.pending_results {
            self.drain();
        }
        if !self.send_query_params(query, param_types, params) {
            return Ok(QueryResult::error(self.err.clone()));
        }
        self.collect_last_result()
    }

    /// PQprepare: create a named prepared statement; CommandOk on success.
    pub fn prepare(
        &mut self,
        stmt_name: &str,
        query: &str,
        param_types: &[Oid],
    ) -> PgResult<QueryResult> {
        if self.pending_results {
            self.drain();
        }
        if !self.send_prepare(stmt_name, query, param_types) {
            return Ok(QueryResult::error(self.err.clone()));
        }
        self.collect_last_result()
    }

    /// PQexecPrepared: execute a named prepared statement with text params.
    pub fn exec_prepared(
        &mut self,
        stmt_name: &str,
        params: &[Option<&str>],
    ) -> PgResult<QueryResult> {
        if self.pending_results {
            self.drain();
        }
        if !self.send_query_prepared(stmt_name, params) {
            return Ok(QueryResult::error(self.err.clone()));
        }
        self.collect_last_result()
    }

    // PQexec's collect loop, shared by the extended-protocol entry points:
    // gather results until ReadyForQuery, keep the last resultset. Handles
    // the extended-protocol acknowledgements ('1' ParseComplete, '2'
    // BindComplete, '3' CloseComplete, 'n' NoData, 't' ParameterDescription)
    // and PortalSuspended ('s', treated as result-complete like libpq).
    fn collect_last_result(&mut self) -> PgResult<QueryResult> {
        let mut nfields = 0usize;
        let mut rows: Vec<Vec<Option<Vec<u8>>>> = Vec::new();
        let mut result = QueryResult::status_only(ExecStatus::CommandOk);
        let mut error: Option<(String, ErrorFields)> = None;
        loop {
            let (t, mbody) = match self.read_message(self.we.receive)? {
                Ok(m) => m,
                Err(e) => return Ok(QueryResult::error(e)),
            };
            match t {
                b'T' => {
                    nfields = u16::from_be_bytes([mbody[0], mbody[1]]) as usize;
                    rows.clear();
                }
                b'D' => rows.push(parse_data_row(&mbody)),
                b'C' | b's' => {
                    let tag =
                        if t == b'C' { cstr_at(&mbody, 0).0 } else { String::new() };
                    result = QueryResult {
                        status: if nfields > 0 {
                            ExecStatus::TuplesOk
                        } else {
                            ExecStatus::CommandOk
                        },
                        nfields,
                        rows: std::mem::take(&mut rows),
                        cmd_tag: tag,
                        diag: None,
                        err: String::new(),
                    };
                    nfields = 0;
                }
                b'I' => result = QueryResult::status_only(ExecStatus::Empty),
                b'E' => {
                    let diag = parse_diag(&mbody);
                    self.err = parse_error_fields(&mbody);
                    error = Some((self.err.clone(), diag));
                }
                b'1' | b'2' | b'3' | b'n' | b't' => {}
                b'S' | b'N' | b'A' => self.note_async(t, &mbody),
                b'Z' => {
                    self.txn_status = mbody.first().copied().unwrap_or(b'I');
                    self.pending_results = false;
                    break;
                }
                other => {
                    self.conn_ok = false;
                    return Ok(QueryResult::error(format!(
                        "unexpected message type \"{}\" from server",
                        other as char
                    )));
                }
            }
        }
        if let Some((e, diag)) = error {
            let mut r = QueryResult::error(e);
            r.diag = Some(diag);
            return Ok(r);
        }
        Ok(result)
    }

    // Deadline-bounded readable wait (postgres_fdw's abort-cleanup
    // discipline pumps with timeouts). Ok(true) = readable; Ok(false) =
    // timed out. Latch wakeups run CHECK_FOR_INTERRUPTS and keep waiting.
    pub fn wait_readable_timeout(&self, timeout_ms: i64) -> PgResult<bool> {
        let deadline = std::time::Instant::now()
            + std::time::Duration::from_millis(timeout_ms.max(0) as u64);
        loop {
            let left = deadline.saturating_duration_since(std::time::Instant::now());
            if left.is_zero() {
                return Ok(false);
            }
            let rc = latch::WaitLatchOrSocket(
                init_small::globals::MyLatch(),
                WL_LATCH_SET | WL_EXIT_ON_PM_DEATH | WL_SOCKET_READABLE | WL_TIMEOUT,
                self.fd,
                left.as_millis().min(i64::MAX as u128) as i64,
                self.we.receive,
            )?;
            if rc & WL_LATCH_SET != 0 {
                if let Some(l) = init_small::globals::MyLatch() {
                    latch::ResetLatch(l);
                }
                postgres_seams::check_for_interrupts::call()?;
            }
            if rc & WL_SOCKET_READABLE != 0 {
                return Ok(true);
            }
            if rc & WL_TIMEOUT != 0 {
                return Ok(false);
            }
        }
    }

    // PQisBusy (after a consume_input): would get_result block? Not busy once
    // a whole result's terminator (or ReadyForQuery) is buffered, or when no
    // query is pending at all.
    pub fn is_busy(&self) -> bool {
        if !self.pending_results || !self.conn_ok {
            return false;
        }
        let mut p = self.inpos;
        loop {
            if self.inbuf.len() - p < 5 {
                return true;
            }
            let len = be_i32(&self.inbuf[p + 1..p + 5]) as usize;
            if self.inbuf.len() - p < 1 + len {
                return true;
            }
            match self.inbuf[p] {
                b'C' | b'I' | b'E' | b'Z' | b'W' | b'H' | b'G' => return false,
                _ => {}
            }
            p += 1 + len;
        }
    }

    // libpqrcv_receive's wait arm: block until the socket is readable (or a
    // latch interrupt); the caller then calls consume_input + get_copy_data.
    pub fn wait_readable(&mut self) -> PgResult<()> {
        wait_socket(self.fd, WL_SOCKET_READABLE, self.we.receive)
    }

    // PQgetCopyData(async=true).
    pub fn get_copy_data(&mut self) -> Result<CopyData, String> {
        loop {
            let Some((t, body)) = self.next_message() else {
                return Ok(CopyData::Block);
            };
            match t {
                b'd' => return Ok(CopyData::Msg(body)),
                b'c' => {
                    self.copy_server_done = true;
                    return Ok(CopyData::End);
                }
                b'E' => {
                    self.err = parse_error_fields(&body);
                    return Err(self.err.clone());
                }
                b'S' | b'N' | b'A' => self.note_async(t, &body),
                other => {
                    self.conn_ok = false;
                    self.err = format!("unexpected message type \"{}\" during COPY", other as char);
                    return Err(self.err.clone());
                }
            }
        }
    }

    // PQgetResult: one result per call; None once ReadyForQuery is consumed
    // (or immediately when no query is pending — libpq's IDLE arm).
    // A half-closed CopyBoth (server CopyDone, ours unsent) reports COPY_IN
    // without reading, exactly as libpq does — the walreceiver may still send.
    pub fn get_result(&mut self) -> PgResult<Option<QueryResult>> {
        if self.in_copy && self.copy_server_done && !self.copy_client_done {
            return Ok(Some(QueryResult::status_only(ExecStatus::CopyIn)));
        }
        if !self.pending_results && !self.in_copy {
            return Ok(None);
        }
        let mut nfields = 0usize;
        let mut rows: Vec<Vec<Option<Vec<u8>>>> = Vec::new();
        loop {
            let (t, body) = match self.read_message(self.we.receive)? {
                Ok(m) => m,
                Err(e) => return Ok(Some(QueryResult::error(e))),
            };
            match t {
                b'T' => {
                    nfields = u16::from_be_bytes([body[0], body[1]]) as usize;
                    rows.clear();
                }
                b'D' => rows.push(parse_data_row(&body)),
                b'C' | b's' => {
                    let tag = if t == b'C' { cstr_at(&body, 0).0 } else { String::new() };
                    let status =
                        if nfields > 0 { ExecStatus::TuplesOk } else { ExecStatus::CommandOk };
                    return Ok(Some(QueryResult {
                        status,
                        nfields,
                        rows,
                        cmd_tag: tag,
                        diag: None,
                        err: String::new(),
                    }));
                }
                b'1' | b'2' | b'3' | b'n' | b't' => {}
                b'I' => return Ok(Some(QueryResult::status_only(ExecStatus::Empty))),
                b'E' => {
                    let diag = parse_diag(&body);
                    self.err = parse_error_fields(&body);
                    let mut r = QueryResult::error(self.err.clone());
                    r.diag = Some(diag);
                    return Ok(Some(r));
                }
                b'W' => {
                    self.in_copy = true;
                    self.copy_server_done = false;
                    self.copy_client_done = false;
                    return Ok(Some(QueryResult::status_only(ExecStatus::CopyBoth)));
                }
                b'd' => {}
                b'c' => self.copy_server_done = true,
                b'S' | b'N' | b'A' => self.note_async(t, &body),
                b'Z' => {
                    self.txn_status = body.first().copied().unwrap_or(b'I');
                    self.in_copy = false;
                    self.pending_results = false;
                    return Ok(None);
                }
                other => {
                    self.conn_ok = false;
                    self.err = format!("unexpected message type \"{}\" from server", other as char);
                    return Ok(Some(QueryResult::error(self.err.clone())));
                }
            }
        }
    }

    // dblink's storeQueryResult shape (libpq single-row mode): rows stream
    // into `sink` as they arrive, the LAST resultset's terminal status is
    // returned (PQexec's keep-the-last rule), and CHECK_FOR_INTERRUPTS runs
    // per row. On sink error the connection still holds unread results — the
    // caller drains in its error path (C's PG_CATCH does the same).
    pub fn exec_streaming(
        &mut self,
        query: &str,
        sink: &mut dyn RowSink,
    ) -> PgResult<QueryResult> {
        if !self.send_query_raw(query) {
            return Ok(QueryResult::error(self.err.clone()));
        }
        let mut nfields = 0usize;
        let mut started = false;
        let mut result = QueryResult::status_only(ExecStatus::CommandOk);
        let mut error: Option<(String, ErrorFields)> = None;
        loop {
            let (t, body) = match self.read_message(self.we.receive)? {
                Ok(m) => m,
                Err(e) => return Ok(QueryResult::error(e)),
            };
            match t {
                b'T' => {
                    nfields = u16::from_be_bytes([body[0], body[1]]) as usize;
                    started = false;
                }
                b'D' => {
                    postgres_seams::check_for_interrupts::call()?;
                    if !started {
                        sink.result_start(&*self, nfields)?;
                        started = true;
                    }
                    let mut cols: Vec<Option<&[u8]>> = Vec::new();
                    parse_data_row_borrowed(&body, &mut cols);
                    sink.row(&cols)?;
                }
                b'C' => {
                    let (tag, _) = cstr_at(&body, 0);
                    if nfields > 0 && !started {
                        // Empty resultset still announces its shape (C's
                        // "if empty resultset, fill tuplestore header").
                        sink.result_start(&*self, nfields)?;
                    }
                    result = QueryResult {
                        status: if nfields > 0 { ExecStatus::TuplesOk } else { ExecStatus::CommandOk },
                        nfields,
                        rows: Vec::new(),
                        cmd_tag: tag,
                        diag: None,
                        err: String::new(),
                    };
                    nfields = 0;
                    started = false;
                }
                b'I' => result = QueryResult::status_only(ExecStatus::Empty),
                b'E' => {
                    let diag = parse_diag(&body);
                    self.err = parse_error_fields(&body);
                    error = Some((self.err.clone(), diag));
                }
                b'1' | b'2' | b'3' | b'n' | b't' => {}
                b'S' | b'N' | b'A' => self.note_async(t, &body),
                b'Z' => {
                    self.txn_status = body.first().copied().unwrap_or(b'I');
                    self.pending_results = false;
                    break;
                }
                other => {
                    self.conn_ok = false;
                    return Ok(QueryResult::error(format!(
                        "unexpected message type \"{}\" from server",
                        other as char
                    )));
                }
            }
        }
        if let Some((e, diag)) = error {
            let mut r = QueryResult::error(e);
            r.diag = Some(diag);
            return Ok(r);
        }
        Ok(result)
    }

    // Error-path cleanup: swallow everything through ReadyForQuery so the
    // connection is reusable (C's PG_CATCH PQgetResult-until-NULL loop).
    pub fn drain(&mut self) {
        if !self.pending_results || !self.conn_ok {
            return;
        }
        loop {
            match self.read_message(self.we.receive) {
                Ok(Ok((b'Z', body))) => {
                    self.txn_status = body.first().copied().unwrap_or(b'I');
                    self.pending_results = false;
                    self.in_copy = false;
                    return;
                }
                Ok(Ok((t, body))) => self.note_async(t, &body),
                Ok(Err(_)) | Err(_) => return,
            }
        }
    }

    // libpqsrv_cancel: dial the same endpoint, send CancelRequest with the
    // saved BackendKeyData, wait (interruptibly, deadline-bounded) for the
    // server to close. None = accepted; Some(msg) = failure text.
    pub fn cancel(&self, timeout_ms: i64) -> PgResult<Option<String>> {
        let sock = match &self.target {
            DialTarget::Tcp(host, port) => {
                match std::net::TcpStream::connect((host.as_str(), *port)) {
                    Ok(s) => CancelSock::Tcp(s),
                    Err(e) => return Ok(Some(format!(
                        "connection to server at \"{host}\", port {port} failed: {e}"
                    ))),
                }
            }
            #[cfg(not(target_family = "wasm"))]
            DialTarget::Unix(path) => match std::os::unix::net::UnixStream::connect(path) {
                Ok(s) => CancelSock::Unix(s),
                Err(e) => {
                    return Ok(Some(format!(
                        "connection to server on socket \"{path}\" failed: {e}"
                    )))
                }
            },
            #[cfg(target_family = "wasm")]
            DialTarget::Unix(path) => {
                return Ok(Some(format!(
                    "connection to server on socket \"{path}\" failed: {}",
                    std::io::Error::from_raw_os_error(52)
                )))
            }
        };
        let mut key = self.be_key.clone();
        if key.len() != 4 {
            key.resize(4, 0);
        }
        let mut pkt = Vec::with_capacity(12 + key.len());
        pkt.extend_from_slice(&((12 + key.len() as u32).to_be_bytes()));
        pkt.extend_from_slice(&CANCEL_REQUEST_CODE.to_be_bytes());
        pkt.extend_from_slice(&self.be_pid.to_be_bytes());
        pkt.extend_from_slice(&key);
        if let Err(e) = sock.write_all(&pkt) {
            return Ok(Some(format!("could not send cancel request: {e}")));
        }
        sock.set_nonblocking();
        // Await EOF: the server closes the cancel connection once processed.
        let fd = sock.raw_fd();
        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(timeout_ms.max(0) as u64);
        loop {
            let mut buf = [0u8; 16];
            let n = unsafe { libc::recv(fd, buf.as_mut_ptr().cast(), buf.len(), 0) };
            if n == 0 {
                return Ok(None);
            }
            if n > 0 {
                continue;
            }
            let e = std::io::Error::last_os_error();
            match e.raw_os_error() {
                Some(libc::EINTR) => continue,
                Some(libc::EAGAIN) => {
                    let left = deadline.saturating_duration_since(std::time::Instant::now());
                    if left.is_zero() {
                        return Ok(Some("cancel request timed out".into()));
                    }
                    let rc = latch::WaitLatchOrSocket(
                        init_small::globals::MyLatch(),
                        WL_LATCH_SET | WL_EXIT_ON_PM_DEATH | WL_SOCKET_READABLE | WL_TIMEOUT,
                        fd,
                        left.as_millis().min(i64::MAX as u128) as i64,
                        self.we.receive,
                    )?;
                    if rc & WL_LATCH_SET != 0 {
                        if let Some(l) = init_small::globals::MyLatch() {
                            latch::ResetLatch(l);
                        }
                        postgres_seams::check_for_interrupts::call()?;
                    }
                }
                _ => return Ok(Some(format!("could not receive cancel response: {e}"))),
            }
        }
    }

    pub fn put_copy_data(&mut self, data: &[u8]) -> Result<(), String> {
        self.send_all(&msg(b'd', data))
    }

    pub fn put_copy_end(&mut self) -> Result<(), String> {
        self.copy_client_done = true;
        self.send_all(&msg(b'c', &[]))
    }

    pub fn terminate(&mut self) {
        let _ = self.send_all(&msg(b'X', &[]));
    }
}

enum CancelSock {
    Tcp(std::net::TcpStream),
    #[cfg(not(target_family = "wasm"))]
    Unix(std::os::unix::net::UnixStream),
}

impl CancelSock {
    fn write_all(&self, buf: &[u8]) -> std::io::Result<()> {
        use std::io::Write;
        match self {
            CancelSock::Tcp(s) => (&mut &*s).write_all(buf),
            #[cfg(not(target_family = "wasm"))]
            CancelSock::Unix(s) => (&mut &*s).write_all(buf),
        }
    }

    fn set_nonblocking(&self) {
        match self {
            CancelSock::Tcp(s) => {
                let _ = s.set_nonblocking(true);
            }
            #[cfg(not(target_family = "wasm"))]
            CancelSock::Unix(s) => {
                let _ = s.set_nonblocking(true);
            }
        }
    }

    fn raw_fd(&self) -> RawFd {
        use std::os::fd::AsRawFd;
        match self {
            CancelSock::Tcp(s) => s.as_raw_fd(),
            #[cfg(not(target_family = "wasm"))]
            CancelSock::Unix(s) => s.as_raw_fd(),
        }
    }
}

// Parse ('P') message body: stmt name, query, param-type OIDs (0 = infer;
// zeros are sent for every param when the caller supplied no types, matching
// libpq's paramTypes==NULL arm).
fn parse_body(stmt_name: &str, query: &str, param_types: &[Oid], nparams: usize) -> Vec<u8> {
    let mut b = Vec::with_capacity(stmt_name.len() + query.len() + 4 + 4 * nparams);
    b.extend_from_slice(stmt_name.as_bytes());
    b.push(0);
    b.extend_from_slice(query.as_bytes());
    b.push(0);
    b.extend_from_slice(&(nparams as u16).to_be_bytes());
    for i in 0..nparams {
        let oid = param_types.get(i).copied().map(u32::from).unwrap_or(0);
        b.extend_from_slice(&oid.to_be_bytes());
    }
    b
}

// Bind ('B') message body: all params text (0 param-format codes, libpq's
// paramFormats==NULL), one result-format code 0 (text), exactly
// pqSendQueryGuts with resultFormat=0.
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

// Execute ('E') message body.
fn execute_body(portal: &str, maxrows: u32) -> Vec<u8> {
    let mut b = Vec::with_capacity(portal.len() + 5);
    b.extend_from_slice(portal.as_bytes());
    b.push(0);
    b.extend_from_slice(&maxrows.to_be_bytes());
    b
}

pub(crate) fn parse_data_row(body: &[u8]) -> Vec<Option<Vec<u8>>> {
    let ncols = u16::from_be_bytes([body[0], body[1]]) as usize;
    let mut cols = Vec::with_capacity(ncols);
    let mut p = 2;
    for _ in 0..ncols {
        let len = be_i32(&body[p..p + 4]);
        p += 4;
        if len < 0 {
            cols.push(None);
        } else {
            cols.push(Some(body[p..p + len as usize].to_vec()));
            p += len as usize;
        }
    }
    cols
}

fn parse_data_row_borrowed<'a>(body: &'a [u8], cols: &mut Vec<Option<&'a [u8]>>) {
    let ncols = u16::from_be_bytes([body[0], body[1]]) as usize;
    let mut p = 2;
    for _ in 0..ncols {
        let len = be_i32(&body[p..p + 4]);
        p += 4;
        if len < 0 {
            cols.push(None);
        } else {
            cols.push(Some(&body[p..p + len as usize]));
            p += len as usize;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conninfo_parse_basic() {
        let opts =
            parse_conninfo("host=/tmp/x port=5433 user=al application_name='my app'").unwrap();
        assert_eq!(opt(&opts, "host"), Some("/tmp/x"));
        assert_eq!(opt(&opts, "port"), Some("5433"));
        assert_eq!(opt(&opts, "application_name"), Some("my app"));
    }

    #[test]
    fn conninfo_parse_quoting() {
        let opts = parse_conninfo(r"password='a\'b' dbname = rep").unwrap();
        assert_eq!(opt(&opts, "password"), Some("a'b"));
        assert_eq!(opt(&opts, "dbname"), Some("rep"));
    }

    #[test]
    fn conninfo_parse_last_dup_wins() {
        let opts = parse_conninfo("host=a host=b").unwrap();
        assert_eq!(opt(&opts, "host"), Some("b"));
    }

    #[test]
    fn conninfo_parse_errors() {
        assert!(parse_conninfo("hostonly").unwrap_err().contains("missing \"=\""));
        assert!(parse_conninfo("host='x").unwrap_err().contains("unterminated"));
    }

    #[test]
    fn conninfo_rejects_unknown_keyword() {
        assert_eq!(
            resolve_conninfo("dbname=x frobnicate=1").unwrap_err(),
            "invalid connection option \"frobnicate\""
        );
    }

    #[test]
    fn port_validation() {
        let parse = |s: &str| validate_port(&parse_conninfo(s).unwrap());
        assert_eq!(parse("port=-1").unwrap_err(), "invalid port number: \"-1\"");
        assert_eq!(parse("port=70000").unwrap_err(), "invalid port number: \"70000\"");
        assert_eq!(parse("port=0").unwrap_err(), "invalid port number: \"0\"");
        assert_eq!(
            parse("port=1foo").unwrap_err(),
            "invalid integer value \"1foo\" for connection option \"port\""
        );
        assert_eq!(parse("").unwrap(), 5432);
        assert_eq!(parse("port= 5433 ").unwrap(), 5433);
    }

    #[test]
    fn msg_framing() {
        let m = msg(b'Q', b"SELECT 1\0");
        assert_eq!(m[0], b'Q');
        assert_eq!(be_i32(&m[1..5]) as usize, m.len() - 1);
    }

    #[test]
    fn parse_body_shape() {
        // Unnamed statement, 2 params, no explicit types -> two zero OIDs
        // (libpq paramTypes==NULL).
        let b = parse_body("", "SELECT $1, $2", &[], 2);
        assert_eq!(&b[..2], b"\0S");
        let ntypes = u16::from_be_bytes([b[b.len() - 10], b[b.len() - 9]]);
        assert_eq!(ntypes, 2);
        assert_eq!(&b[b.len() - 8..], &[0, 0, 0, 0, 0, 0, 0, 0]);
        // Named + explicit types (PQprepare).
        let b = parse_body("st1", "SELECT $1", &[Oid::from(23u32)], 1);
        assert!(b.starts_with(b"st1\0SELECT $1\0"));
        assert_eq!(&b[b.len() - 6..], &[0, 1, 0, 0, 0, 23]);
    }

    #[test]
    fn bind_body_shape() {
        let b = bind_body("", "st1", &[Some("42"), None]);
        // portal "" NUL, stmt "st1" NUL, 0 param-format codes, 2 params:
        // "42" (len 2), NULL (-1); then 1 result-format code = 0 (text).
        let mut want: Vec<u8> = Vec::new();
        want.extend_from_slice(b"\0st1\0");
        want.extend_from_slice(&0u16.to_be_bytes());
        want.extend_from_slice(&2u16.to_be_bytes());
        want.extend_from_slice(&2u32.to_be_bytes());
        want.extend_from_slice(b"42");
        want.extend_from_slice(&(-1i32).to_be_bytes());
        want.extend_from_slice(&1u16.to_be_bytes());
        want.extend_from_slice(&0u16.to_be_bytes());
        assert_eq!(b, want);
    }

    #[test]
    fn execute_body_shape() {
        assert_eq!(execute_body("", 0), vec![0, 0, 0, 0, 0]);
    }

    #[test]
    fn error_fields() {
        let mut body = Vec::new();
        body.extend_from_slice(b"SFATAL\0");
        body.extend_from_slice(b"C57P01\0");
        body.extend_from_slice(b"Mgoodbye\0");
        body.push(0);
        assert_eq!(parse_error_fields(&body), "FATAL:  goodbye");
        let d = parse_diag(&body);
        assert_eq!(d.sqlstate, "57P01");
        assert_eq!(d.severity, "FATAL");
        assert_eq!(d.primary, "goodbye");
    }

    #[test]
    fn service_file_syntax_error() {
        let dir = std::env::temp_dir().join(format!("pgclient-svc-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let f = dir.join("pg_service.conf");
        std::fs::write(&f, "# c\n\n[test_ldap]\nldap://127.0.0.1:9/base?attribute?one?filter\n")
            .unwrap();
        let mut opts = Vec::new();
        let mut found = false;
        let err = super::conninfo::parse_service_file(
            f.to_str().unwrap(),
            "test_ldap",
            &mut opts,
            &mut found,
        )
        .unwrap_err();
        assert_eq!(
            err,
            format!("syntax error in service file \"{}\", line 4", f.to_str().unwrap())
        );
        std::fs::write(&f, "[svc]\nhost=h1\nport=5455\n[other]\nhost=h2\n").unwrap();
        let mut opts = vec![("port".to_string(), "1111".to_string())];
        let mut found = false;
        super::conninfo::parse_service_file(f.to_str().unwrap(), "svc", &mut opts, &mut found)
            .unwrap();
        assert!(found);
        assert_eq!(opt(&opts, "host"), Some("h1"));
        assert_eq!(opt(&opts, "port"), Some("1111"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn dispchar_table_shape() {
        assert_eq!(lookup("password").unwrap().dispchar, "*");
        assert_eq!(lookup("sslpassword").unwrap().dispchar, "*");
        assert_eq!(lookup("oauth_client_secret").unwrap().dispchar, "*");
        assert_eq!(lookup("replication").unwrap().dispchar, "D");
        assert_eq!(lookup("scram_client_key").unwrap().dispchar, "D");
        assert_eq!(lookup("sslkeylogfile").unwrap().dispchar, "D");
        assert!(lookup("server").is_none());
        fn lookup(k: &str) -> Option<&'static ConnOption> {
            conninfo::lookup_option(k)
        }
    }
}
