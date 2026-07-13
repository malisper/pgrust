// libpqwalreceiver.c over a minimal in-process replication-protocol client
// (fast carries no libpq): conninfo parsing, startup/auth handshake (trust,
// password, md5, SCRAM-SHA-256), simple query, and CopyBoth framing. Only the
// physical-replication surface walreceiver.c uses is implemented.
use std::os::fd::{AsRawFd, RawFd};

use elog::ereport;
use types_core::{pgsocket, TimeLineID, XLogRecPtr, PGINVALID_SOCKET};
use types_error::{
    ErrorLocation, PgResult, ERRCODE_CONNECTION_FAILURE, ERRCODE_PROTOCOL_VIOLATION,
    ERRCODE_SYNTAX_ERROR, ERROR,
};
use types_storage::waiteventset::{
    WL_EXIT_ON_PM_DEATH, WL_LATCH_SET, WL_SOCKET_READABLE, WL_SOCKET_WRITEABLE,
};

const WAIT_EVENT_LIBPQWALRECEIVER_CONNECT: u32 = 0x0600_0000 | 3;
const WAIT_EVENT_LIBPQWALRECEIVER_RECEIVE: u32 = 0x0600_0000 | 4;
const PG_PROTOCOL_3_0: u32 = 3 << 16;

fn loc(func: &'static str) -> ErrorLocation {
    ErrorLocation::new("src/backend/replication/libpqwalreceiver/libpqwalreceiver.c", 0, func)
}

fn pchomp(s: &str) -> String {
    s.trim_end_matches('\n').to_string()
}

// ereport(ERROR)'s .finish() is typed PgResult<()>; rethrow it as any T.
fn throw<T>(r: PgResult<()>) -> PgResult<T> {
    match r {
        Ok(()) => unreachable!("throw called with non-error report"),
        Err(e) => Err(e),
    }
}

fn lsn_fmt(lsn: XLogRecPtr) -> String {
    format!("{:X}/{:X}", (lsn >> 32) as u32, lsn as u32)
}

// ---------------------------------------------------------------------------
// conninfo parsing (PQconninfoParse's key=value scanner).
// ---------------------------------------------------------------------------

pub fn parse_conninfo(s: &str) -> Result<Vec<(String, String)>, String> {
    let b = s.as_bytes();
    let mut i = 0;
    let mut opts: Vec<(String, String)> = Vec::new();
    loop {
        while i < b.len() && (b[i] as char).is_ascii_whitespace() {
            i += 1;
        }
        if i >= b.len() {
            return Ok(opts);
        }
        let kstart = i;
        while i < b.len() && b[i] != b'=' && !(b[i] as char).is_ascii_whitespace() {
            i += 1;
        }
        let key = s[kstart..i].to_string();
        while i < b.len() && (b[i] as char).is_ascii_whitespace() {
            i += 1;
        }
        if i >= b.len() || b[i] != b'=' {
            return Err(format!(
                "missing \"=\" after \"{key}\" in connection info string"
            ));
        }
        i += 1;
        while i < b.len() && (b[i] as char).is_ascii_whitespace() {
            i += 1;
        }
        let mut val = Vec::new();
        if i < b.len() && b[i] == b'\'' {
            i += 1;
            loop {
                if i >= b.len() {
                    return Err("unterminated quoted string in connection info string".into());
                }
                match b[i] {
                    b'\'' => {
                        i += 1;
                        break;
                    }
                    b'\\' if i + 1 < b.len() => {
                        val.push(b[i + 1]);
                        i += 2;
                    }
                    c => {
                        val.push(c);
                        i += 1;
                    }
                }
            }
        } else {
            while i < b.len() && !(b[i] as char).is_ascii_whitespace() {
                if b[i] == b'\\' && i + 1 < b.len() {
                    val.push(b[i + 1]);
                    i += 2;
                } else {
                    val.push(b[i]);
                    i += 1;
                }
            }
        }
        let val = String::from_utf8_lossy(&val).into_owned();
        opts.retain(|(k, _)| *k != key);
        opts.push((key, val));
    }
}

fn opt<'a>(opts: &'a [(String, String)], key: &str) -> Option<&'a str> {
    opts.iter().find(|(k, _)| k == key).map(|(_, v)| v.as_str())
}

// ---------------------------------------------------------------------------
// Connection object.
// ---------------------------------------------------------------------------

enum Stream {
    Tcp(std::net::TcpStream),
    Unix(std::os::unix::net::UnixStream),
}

pub struct PgConn {
    _stream: Stream,
    fd: RawFd,
    inbuf: Vec<u8>,
    inpos: usize,
    conn_ok: bool,
    in_copy: bool,
    copy_server_done: bool,
    copy_client_done: bool,
    err: String,
    opts: Vec<(String, String)>,
    display_host: String,
    display_port: i32,
    be_pid: i32,
    server_version: i32,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ExecStatus {
    CommandOk,
    TuplesOk,
    CopyBoth,
    CopyIn,
    Empty,
    Error,
}

pub struct QueryResult {
    pub status: ExecStatus,
    pub nfields: usize,
    pub rows: Vec<Vec<Option<Vec<u8>>>>,
    pub err: String,
}

pub enum CopyData {
    Msg(Vec<u8>),
    Block,
    End,
}

fn msg(t: u8, body: &[u8]) -> Vec<u8> {
    let mut m = Vec::with_capacity(5 + body.len());
    m.push(t);
    m.extend_from_slice(&((body.len() as u32 + 4).to_be_bytes()));
    m.extend_from_slice(body);
    m
}

fn be_i32(b: &[u8]) -> i32 {
    i32::from_be_bytes([b[0], b[1], b[2], b[3]])
}

fn cstr_at(b: &[u8], pos: usize) -> (String, usize) {
    let end = b[pos..].iter().position(|&c| c == 0).map(|e| pos + e).unwrap_or(b.len());
    (String::from_utf8_lossy(&b[pos..end]).into_owned(), end + 1)
}

// ErrorResponse/NoticeResponse body -> libpq default-verbosity message
// (pqBuildErrorMessage3): "SEVERITY:  message" plus DETAIL/HINT/CONTEXT
// lines. Callers (e.g. the walreceiver's "could not start WAL streaming")
// relay the whole thing into the local log — 019_replslot_limit greps the
// standby log for the primary's DETAIL line.
fn parse_error_fields(body: &[u8]) -> String {
    let (mut severity, mut message) = (String::from("ERROR"), String::new());
    let (mut detail, mut hint, mut context) = (String::new(), String::new(), String::new());
    let mut i = 0;
    while i < body.len() && body[i] != 0 {
        let code = body[i];
        let (val, next) = cstr_at(body, i + 1);
        match code {
            b'S' => severity = val,
            b'M' => message = val,
            b'D' => detail = val,
            b'H' => hint = val,
            b'W' => context = val,
            _ => {}
        }
        i = next;
    }
    let mut out = format!("{severity}:  {message}");
    if !detail.is_empty() {
        out.push_str(&format!("\nDETAIL:  {detail}"));
    }
    if !hint.is_empty() {
        out.push_str(&format!("\nHINT:  {hint}"));
    }
    if !context.is_empty() {
        out.push_str(&format!("\nCONTEXT:  {context}"));
    }
    out
}

fn wait_socket(fd: RawFd, events: u32, wait_event_info: u32) -> PgResult<()> {
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

impl PgConn {
    pub fn error_message(&self) -> String {
        self.err.clone()
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

    fn send_all(&mut self, buf: &[u8]) -> Result<(), String> {
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
                    if let Err(pe) =
                        wait_socket(self.fd, WL_SOCKET_WRITEABLE, WAIT_EVENT_LIBPQWALRECEIVER_RECEIVE)
                    {
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

    fn next_message(&mut self) -> Option<(u8, Vec<u8>)> {
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

    fn read_message(&mut self, wait_event_info: u32) -> PgResult<Result<(u8, Vec<u8>), String>> {
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

    // PQexec (simple query), restricted to single-statement replication
    // commands. CopyBothResponse returns immediately with the stream open.
    pub fn exec(&mut self, query: &str) -> PgResult<QueryResult> {
        let mut body = query.as_bytes().to_vec();
        body.push(0);
        if let Err(e) = self.send_all(&msg(b'Q', &body)) {
            return Ok(QueryResult { status: ExecStatus::Error, nfields: 0, rows: Vec::new(), err: e });
        }

        let mut nfields = 0usize;
        let mut rows: Vec<Vec<Option<Vec<u8>>>> = Vec::new();
        let mut result_nfields = 0usize;
        let mut result_rows: Vec<Vec<Option<Vec<u8>>>> = Vec::new();
        let mut last = ExecStatus::CommandOk;
        let mut error: Option<String> = None;
        let mut got_result = false;
        loop {
            let (t, mbody) = match self.read_message(WAIT_EVENT_LIBPQWALRECEIVER_RECEIVE)? {
                Ok(m) => m,
                Err(e) => {
                    return Ok(QueryResult {
                        status: ExecStatus::Error,
                        nfields: 0,
                        rows: Vec::new(),
                        err: e,
                    })
                }
            };
            match t {
                b'T' => {
                    nfields = u16::from_be_bytes([mbody[0], mbody[1]]) as usize;
                    rows.clear();
                }
                b'D' => rows.push(parse_data_row(&mbody)),
                b'C' => {
                    last = if nfields > 0 { ExecStatus::TuplesOk } else { ExecStatus::CommandOk };
                    got_result = true;
                    result_nfields = nfields;
                    result_rows = std::mem::take(&mut rows);
                    nfields = 0;
                }
                b'I' => {
                    last = ExecStatus::Empty;
                    got_result = true;
                }
                b'E' => {
                    self.err = parse_error_fields(&mbody);
                    error = Some(self.err.clone());
                }
                b'W' => {
                    self.in_copy = true;
                    self.copy_server_done = false;
                    self.copy_client_done = false;
                    return Ok(QueryResult {
                        status: ExecStatus::CopyBoth,
                        nfields: 0,
                        rows: Vec::new(),
                        err: String::new(),
                    });
                }
                b'S' | b'N' | b'A' => {}
                b'Z' => break,
                other => {
                    self.conn_ok = false;
                    return Ok(QueryResult {
                        status: ExecStatus::Error,
                        nfields: 0,
                        rows: Vec::new(),
                        err: format!("unexpected message type \"{}\" from server", other as char),
                    });
                }
            }
        }
        if let Some(e) = error {
            return Ok(QueryResult { status: ExecStatus::Error, nfields: 0, rows: Vec::new(), err: e });
        }
        let _ = got_result;
        Ok(QueryResult { status: last, nfields: result_nfields, rows: result_rows, err: String::new() })
    }

    // libpqrcv_receive's wait arm: block until the socket is readable (or a
    // latch interrupt); the caller then calls consume_input + get_copy_data.
    pub fn wait_readable(&mut self) -> PgResult<()> {
        wait_socket(self.fd, WL_SOCKET_READABLE, WAIT_EVENT_LIBPQWALRECEIVER_RECEIVE)
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
                b'S' | b'N' | b'A' => continue,
                other => {
                    self.conn_ok = false;
                    self.err = format!("unexpected message type \"{}\" during COPY", other as char);
                    return Err(self.err.clone());
                }
            }
        }
    }

    // PQgetResult: one result per call; None once ReadyForQuery is consumed.
    // A half-closed CopyBoth (server CopyDone, ours unsent) reports COPY_IN
    // without reading, exactly as libpq does — the walreceiver may still send.
    pub fn get_result(&mut self) -> PgResult<Option<QueryResult>> {
        if self.in_copy && self.copy_server_done && !self.copy_client_done {
            return Ok(Some(QueryResult {
                status: ExecStatus::CopyIn,
                nfields: 0,
                rows: Vec::new(),
                err: String::new(),
            }));
        }
        let mut nfields = 0usize;
        let mut rows: Vec<Vec<Option<Vec<u8>>>> = Vec::new();
        loop {
            let (t, body) = match self.read_message(WAIT_EVENT_LIBPQWALRECEIVER_RECEIVE)? {
                Ok(m) => m,
                Err(e) => {
                    return Ok(Some(QueryResult {
                        status: ExecStatus::Error,
                        nfields: 0,
                        rows: Vec::new(),
                        err: e,
                    }))
                }
            };
            match t {
                b'T' => {
                    nfields = u16::from_be_bytes([body[0], body[1]]) as usize;
                    rows.clear();
                }
                b'D' => rows.push(parse_data_row(&body)),
                b'C' => {
                    let status =
                        if nfields > 0 { ExecStatus::TuplesOk } else { ExecStatus::CommandOk };
                    return Ok(Some(QueryResult { status, nfields, rows, err: String::new() }));
                }
                b'I' => {
                    return Ok(Some(QueryResult {
                        status: ExecStatus::Empty,
                        nfields: 0,
                        rows: Vec::new(),
                        err: String::new(),
                    }))
                }
                b'E' => {
                    self.err = parse_error_fields(&body);
                    return Ok(Some(QueryResult {
                        status: ExecStatus::Error,
                        nfields: 0,
                        rows: Vec::new(),
                        err: self.err.clone(),
                    }));
                }
                b'W' => {
                    self.in_copy = true;
                    self.copy_server_done = false;
                    self.copy_client_done = false;
                    return Ok(Some(QueryResult {
                        status: ExecStatus::CopyBoth,
                        nfields: 0,
                        rows: Vec::new(),
                        err: String::new(),
                    }));
                }
                b'd' => {}
                b'c' => self.copy_server_done = true,
                b'S' | b'N' | b'A' => {}
                b'Z' => {
                    self.in_copy = false;
                    return Ok(None);
                }
                other => {
                    self.conn_ok = false;
                    self.err = format!("unexpected message type \"{}\" from server", other as char);
                    return Ok(Some(QueryResult {
                        status: ExecStatus::Error,
                        nfields: 0,
                        rows: Vec::new(),
                        err: self.err.clone(),
                    }));
                }
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

fn parse_data_row(body: &[u8]) -> Vec<Option<Vec<u8>>> {
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

// ---------------------------------------------------------------------------
// Connect + authentication.
// ---------------------------------------------------------------------------

fn os_user_name() -> String {
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

// libpqrcv_connect (replication=true, logical=false): the physical
// walreceiver's connection. Ok(Err(msg)) is C's NULL-return-with-*err path;
// the caller wraps it into its own ereport.
pub fn connect(conninfo: &str, appname: &str) -> PgResult<Result<PgConn, String>> {
    connect_extended(conninfo, true, false, false, appname)
}

// libpqrcv_connect (libpqwalreceiver.c), full parameter surface: physical
// replication (replication=true, database "replication"), logical replication
// (replication=database + the subscriber's dbname + forced GUC options, as
// pg_dump forces them), or a plain SQL connection (tablesync catalog reads).
// must_use_password renders libpqrcv_check_conninfo's recheck: the conninfo
// itself must carry a password (recorded divergence: C additionally checks
// PQconnectionUsedPassword after connecting — server-side auth-method
// awareness this client does not track).
pub fn connect_extended(
    conninfo: &str,
    replication: bool,
    logical: bool,
    must_use_password: bool,
    appname: &str,
) -> PgResult<Result<PgConn, String>> {
    debug_assert!(replication || !logical);
    let opts = check_conninfo(conninfo)?;

    if must_use_password && opt(&opts, "password").map_or(true, |p| p.is_empty()) {
        return throw(
            ereport(ERROR)
                .errcode(types_error::ERRCODE_S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED)
                .errmsg("password is required")
                .errdetail("Non-superusers must provide a password in the connection string.")
                .finish(loc("libpqrcv_connect")),
        );
    }

    let host = opt(&opts, "host").unwrap_or("").to_string();
    let hostaddr = opt(&opts, "hostaddr").unwrap_or("").to_string();
    let port_s = opt(&opts, "port").unwrap_or("5432").to_string();
    let port: u16 = port_s.trim().parse().unwrap_or(5432);
    let user = opt(&opts, "user").map(|s| s.to_string()).unwrap_or_else(os_user_name);
    let password = opt(&opts, "password")
        .map(|s| s.to_string())
        .or_else(|| std::env::var("PGPASSWORD").ok().filter(|p| !p.is_empty()));
    let appname = opt(&opts, "application_name").unwrap_or(appname).to_string();
    let options = opt(&opts, "options").unwrap_or("").to_string();

    let (stream, fd, display_host) = if !host.is_empty() && host.starts_with('/') {
        let path = format!("{host}/.s.PGSQL.{port}");
        match std::os::unix::net::UnixStream::connect(&path) {
            Ok(s) => {
                let fd = s.as_raw_fd();
                (Stream::Unix(s), fd, host.clone())
            }
            Err(e) => {
                return Ok(Err(format!(
                    "connection to server on socket \"{path}\" failed: {e}"
                )))
            }
        }
    } else {
        let target = if !hostaddr.is_empty() { hostaddr.clone() } else { host.clone() };
        let target = if target.is_empty() { "localhost".to_string() } else { target };
        match std::net::TcpStream::connect((target.as_str(), port)) {
            Ok(s) => {
                let _ = s.set_nodelay(true);
                let fd = s.as_raw_fd();
                (Stream::Tcp(s), fd, target)
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
        Stream::Unix(s) => s.set_nonblocking(true).expect("set_nonblocking"),
    }

    let mut conn = PgConn {
        _stream: stream,
        fd,
        inbuf: Vec::new(),
        inpos: 0,
        conn_ok: true,
        in_copy: false,
        copy_server_done: false,
        copy_client_done: false,
        err: String::new(),
        opts,
        display_host,
        display_port: port as i32,
        be_pid: 0,
        server_version: 0,
    };

    // Startup packet: protocol 3.0. Parameter set per libpqrcv_connect.
    let dbname = opt(&conn.opts, "dbname").unwrap_or("").to_string();
    let database: &str = if replication && !logical { "replication" } else { dbname.as_str() };
    let forced_options;
    let options_val: &str = if logical {
        // Force unambiguous output formats on the publisher (matches pg_dump).
        forced_options = format!(
            "{options} -c datestyle=ISO -c intervalstyle=postgres -c extra_float_digits=3"
        );
        forced_options.as_str()
    } else {
        options.as_str()
    };
    let mut params: Vec<(&str, &str)> = vec![("user", user.as_str()), ("database", database)];
    if replication {
        params.push(("replication", if logical { "database" } else { "true" }));
    }
    let encoding;
    if logical {
        // Tell the publisher to translate to our encoding.
        encoding = mbutils_seams::get_database_encoding_name::call();
        params.push(("client_encoding", encoding));
    }
    params.push(("application_name", appname.as_str()));
    if !options_val.is_empty() {
        params.push(("options", options_val));
    }
    let mut body = Vec::new();
    body.extend_from_slice(&PG_PROTOCOL_3_0.to_be_bytes());
    for (k, v) in &params {
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
        return Ok(Err(e));
    }

    loop {
        let (t, mbody) = match conn.read_message(WAIT_EVENT_LIBPQWALRECEIVER_CONNECT)? {
            Ok(m) => m,
            Err(e) => return Ok(Err(e)),
        };
        match t {
            b'R' => {
                let authtype = be_i32(&mbody[0..4]);
                match authtype {
                    0 => {}
                    3 => {
                        let Some(pw) = password.as_deref() else {
                            return Ok(Err("fe_sendauth: no password supplied".into()));
                        };
                        let mut b = pw.as_bytes().to_vec();
                        b.push(0);
                        if let Err(e) = conn.send_all(&msg(b'p', &b)) {
                            return Ok(Err(e));
                        }
                    }
                    5 => {
                        let Some(pw) = password.as_deref() else {
                            return Ok(Err("fe_sendauth: no password supplied".into()));
                        };
                        let salt = &mbody[4..8];
                        let stage1 = pg_md5::pg_md5_encrypt(pw.as_bytes(), user.as_bytes());
                        let hex = &stage1[3..];
                        let stage2 = pg_md5::pg_md5_encrypt(hex, salt);
                        let mut b = stage2.to_vec();
                        b.push(0);
                        if let Err(e) = conn.send_all(&msg(b'p', &b)) {
                            return Ok(Err(e));
                        }
                    }
                    10 => {
                        let Some(pw) = password.as_deref() else {
                            return Ok(Err("fe_sendauth: no password supplied".into()));
                        };
                        let mut mechs = Vec::new();
                        let mut p = 4;
                        while p < mbody.len() && mbody[p] != 0 {
                            let (m, next) = cstr_at(&mbody, p);
                            mechs.push(m);
                            p = next;
                        }
                        if !mechs.iter().any(|m| m == scram_common::SCRAM_SHA_256_NAME) {
                            return Ok(Err(format!(
                                "none of the server's SASL authentication mechanisms are supported (offered: {})",
                                mechs.join(", ")
                            )));
                        }
                        if let Err(e) = scram_exchange(&mut conn, pw)? {
                            return Ok(Err(e));
                        }
                    }
                    other => {
                        return Ok(Err(format!(
                            "authentication method {other} not supported by the pgrust replication client"
                        )))
                    }
                }
            }
            b'S' => {
                let (name, next) = cstr_at(&mbody, 0);
                let (value, _) = cstr_at(&mbody, next);
                if name == "server_version" {
                    let major: i32 = value
                        .split(|c: char| !c.is_ascii_digit())
                        .next()
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0);
                    conn.server_version = major * 10000;
                }
            }
            b'K' => {
                conn.be_pid = be_i32(&mbody[0..4]);
            }
            b'N' => {}
            b'E' => return Ok(Err(parse_error_fields(&mbody))),
            b'Z' => break,
            other => {
                return Ok(Err(format!(
                    "unexpected message type \"{}\" during connection startup",
                    other as char
                )))
            }
        }
    }

    Ok(Ok(conn))
}

fn b64(data: &[u8]) -> String {
    let mut dst = vec![0u8; pg_b64::pg_b64_enc_len(data.len() as i32) as usize];
    let dstlen = dst.len() as i32;
    let n = pg_b64::pg_b64_encode(data, data.len() as i32, &mut dst, dstlen);
    assert!(n >= 0, "base64 encode failed");
    String::from_utf8_lossy(&dst[..n as usize]).into_owned()
}

fn b64_decode(s: &str) -> Result<Vec<u8>, String> {
    let mut dst = vec![0u8; pg_b64::pg_b64_dec_len(s.len() as i32) as usize];
    let dstlen = dst.len() as i32;
    let n = pg_b64::pg_b64_decode(s.as_bytes(), s.len() as i32, &mut dst, dstlen);
    if n < 0 {
        return Err("malformed base64 in SCRAM message".into());
    }
    dst.truncate(n as usize);
    Ok(dst)
}

fn scram_attr<'a>(fields: &'a [&'a str], name: char) -> Result<&'a str, String> {
    fields
        .iter()
        .find(|f| f.starts_with(name) && f.as_bytes().get(1) == Some(&b'='))
        .map(|f| &f[2..])
        .ok_or_else(|| format!("malformed SCRAM message (missing \"{name}\" attribute)"))
}

// SCRAM-SHA-256 client exchange, no channel binding (gs2 = "n,,").
fn scram_exchange(conn: &mut PgConn, password: &str) -> PgResult<Result<(), String>> {
    let scratch = mcx::MemoryContext::new("walrcv scram");
    let prep = saslprep::pg_saslprep(scratch.mcx(), password.as_bytes())
        .ok()
        .flatten()
        .map(|v| v.as_slice().to_vec())
        .unwrap_or_else(|| password.as_bytes().to_vec());

    let mut raw_nonce = [0u8; scram_common::SCRAM_RAW_NONCE_LEN];
    if std::io::Read::read_exact(
        &mut std::fs::File::open("/dev/urandom").expect("open /dev/urandom"),
        &mut raw_nonce,
    )
    .is_err()
    {
        return Ok(Err("could not generate nonce".into()));
    }
    let client_nonce = b64(&raw_nonce);
    let client_first_bare = format!("n=,r={client_nonce}");

    let mut body = Vec::new();
    body.extend_from_slice(scram_common::SCRAM_SHA_256_NAME.as_bytes());
    body.push(0);
    let initial = format!("n,,{client_first_bare}");
    body.extend_from_slice(&((initial.len() as u32).to_be_bytes()));
    body.extend_from_slice(initial.as_bytes());
    if let Err(e) = conn.send_all(&msg(b'p', &body)) {
        return Ok(Err(e));
    }

    let (t, mbody) = match conn.read_message(WAIT_EVENT_LIBPQWALRECEIVER_CONNECT)? {
        Ok(m) => m,
        Err(e) => return Ok(Err(e)),
    };
    if t == b'E' {
        return Ok(Err(parse_error_fields(&mbody)));
    }
    if t != b'R' || be_i32(&mbody[0..4]) != 11 {
        return Ok(Err("expected SASL continue message from server".into()));
    }
    let server_first = String::from_utf8_lossy(&mbody[4..]).into_owned();
    let fields: Vec<&str> = server_first.split(',').collect();
    let server_nonce = match scram_attr(&fields, 'r') {
        Ok(v) => v.to_string(),
        Err(e) => return Ok(Err(e)),
    };
    if !server_nonce.starts_with(&client_nonce) {
        return Ok(Err("invalid SCRAM response (nonce mismatch)".into()));
    }
    let salt = match scram_attr(&fields, 's').and_then(|s| b64_decode(s)) {
        Ok(v) => v,
        Err(e) => return Ok(Err(e)),
    };
    let iterations: i32 = match scram_attr(&fields, 'i').map(|s| s.parse::<i32>()) {
        Ok(Ok(v)) => v,
        _ => return Ok(Err("malformed SCRAM message (invalid iteration count)".into())),
    };

    let salted = scram_common::scram_salted_password(&prep, &salt, iterations)?;
    let client_key = scram_common::scram_client_key(&salted);
    let stored_key = scram_common::scram_h(&client_key);

    let client_final_wo_proof = format!("c=biws,r={server_nonce}");
    let auth_message = format!("{client_first_bare},{server_first},{client_final_wo_proof}");
    let client_sig = pg_hmac_sha256(&stored_key, auth_message.as_bytes());
    let mut proof = client_key;
    for (p, s) in proof.iter_mut().zip(client_sig.iter()) {
        *p ^= s;
    }
    let client_final = format!("{client_final_wo_proof},p={}", b64(&proof));
    if let Err(e) = conn.send_all(&msg(b'p', client_final.as_bytes())) {
        return Ok(Err(e));
    }

    let (t, mbody) = match conn.read_message(WAIT_EVENT_LIBPQWALRECEIVER_CONNECT)? {
        Ok(m) => m,
        Err(e) => return Ok(Err(e)),
    };
    if t == b'E' {
        return Ok(Err(parse_error_fields(&mbody)));
    }
    if t != b'R' || be_i32(&mbody[0..4]) != 12 {
        return Ok(Err("expected SASL final message from server".into()));
    }
    let server_final = String::from_utf8_lossy(&mbody[4..]).into_owned();
    let ffields: Vec<&str> = server_final.split(',').collect();
    let server_sig_b64 = match scram_attr(&ffields, 'v') {
        Ok(v) => v.to_string(),
        Err(e) => return Ok(Err(e)),
    };
    let server_key = scram_common::scram_server_key(&salted);
    let expected = b64(&pg_hmac_sha256(&server_key, auth_message.as_bytes()));
    if server_sig_b64 != expected {
        return Ok(Err("incorrect server signature in SCRAM exchange".into()));
    }
    Ok(Ok(()))
}

fn pg_hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    pg_hmac::hmac_sha256(key, data)
}

// ---------------------------------------------------------------------------
// libpqrcv_* surface consumed by walreceiver.c.
// ---------------------------------------------------------------------------

/// libpqrcv_check_conninfo.
pub fn check_conninfo(conninfo: &str) -> PgResult<Vec<(String, String)>> {
    match parse_conninfo(conninfo) {
        Ok(opts) => Ok(opts),
        Err(e) => throw(
            ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg(format!("invalid connection string syntax: {e}"))
                .finish(loc("libpqrcv_check_conninfo")),
        ),
    }
}

/// libpqrcv_identify_system.
pub fn identify_system(conn: &mut PgConn) -> PgResult<(String, TimeLineID)> {
    let res = conn.exec("IDENTIFY_SYSTEM")?;
    if res.status != ExecStatus::TuplesOk {
        return throw(ereport(ERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg(format!(
                "could not receive database system identifier and timeline ID from the primary server: {}",
                pchomp(&res.err)
            ))
            .finish(loc("libpqrcv_identify_system")));
    }
    if res.nfields < 3 || res.rows.len() != 1 {
        return throw(ereport(ERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg("invalid response from primary server")
            .errdetail(format!(
                "Could not identify system: got {} rows and {} fields, expected {} rows and {} or more fields.",
                res.rows.len(),
                res.nfields,
                1,
                3
            ))
            .finish(loc("libpqrcv_identify_system")));
    }
    let sysid = text_col(&res, 0, 0);
    let tli: TimeLineID = text_col(&res, 0, 1).trim().parse().map_err(|_| {
        ereport(ERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg("invalid response from primary server")
            .errdetail("Could not parse the primary timeline ID.")
            .into_error()
    })?;
    Ok((sysid, tli))
}

fn text_col(res: &QueryResult, row: usize, col: usize) -> String {
    match res.rows.get(row).and_then(|r| r.get(col)) {
        Some(Some(v)) => String::from_utf8_lossy(v).into_owned(),
        _ => String::new(),
    }
}

/// libpqrcv_startstreaming (physical).
pub fn start_streaming(
    conn: &mut PgConn,
    slotname: Option<&str>,
    startpoint: XLogRecPtr,
    tli: TimeLineID,
) -> PgResult<bool> {
    let mut cmd = String::from("START_REPLICATION");
    if let Some(slot) = slotname {
        cmd.push_str(&format!(" SLOT \"{slot}\""));
    }
    cmd.push_str(&format!(" {} TIMELINE {tli}", lsn_fmt(startpoint)));

    let res = conn.exec(&cmd)?;
    match res.status {
        ExecStatus::CommandOk => Ok(false),
        ExecStatus::CopyBoth => Ok(true),
        _ => throw(ereport(ERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg(format!("could not start WAL streaming: {}", pchomp(&res.err)))
            .finish(loc("libpqrcv_startstreaming"))),
    }
}

/// libpqrcv_endstreaming. Returns the next timeline ID (0 if not reported).
pub fn end_streaming(conn: &mut PgConn) -> PgResult<TimeLineID> {
    if conn.put_copy_end().is_err() {
        return throw(ereport(ERROR)
            .errcode(ERRCODE_CONNECTION_FAILURE)
            .errmsg(format!(
                "could not send end-of-streaming message to primary: {}",
                pchomp(&conn.error_message())
            ))
            .finish(loc("libpqrcv_endstreaming")));
    }

    let mut next_tli: TimeLineID = 0;
    let mut res = conn.get_result()?;
    if let Some(r) = &res {
        if r.status == ExecStatus::TuplesOk {
            if r.nfields < 2 || r.rows.len() != 1 {
                return throw(ereport(ERROR)
                    .errcode(ERRCODE_PROTOCOL_VIOLATION)
                    .errmsg("unexpected result set after end-of-streaming")
                    .finish(loc("libpqrcv_endstreaming")));
            }
            next_tli = text_col(r, 0, 0).trim().parse().unwrap_or(0);
            res = conn.get_result()?;
        }
    }
    match &res {
        Some(r) if r.status == ExecStatus::CommandOk => {}
        _ => {
            return throw(ereport(ERROR)
                .errcode(ERRCODE_PROTOCOL_VIOLATION)
                .errmsg(format!(
                    "error reading result of streaming command: {}",
                    pchomp(&conn.error_message())
                ))
                .finish(loc("libpqrcv_endstreaming")));
        }
    }
    if conn.get_result()?.is_some() {
        return throw(ereport(ERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg(format!(
                "unexpected result after CommandComplete: {}",
                pchomp(&conn.error_message())
            ))
            .finish(loc("libpqrcv_endstreaming")));
    }
    Ok(next_tli)
}

/// libpqrcv_readtimelinehistoryfile.
pub fn read_timeline_history_file(
    conn: &mut PgConn,
    tli: TimeLineID,
) -> PgResult<(String, Vec<u8>)> {
    let res = conn.exec(&format!("TIMELINE_HISTORY {tli}"))?;
    if res.status != ExecStatus::TuplesOk {
        return throw(ereport(ERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg(format!(
                "could not receive timeline history file from the primary server: {}",
                pchomp(&res.err)
            ))
            .finish(loc("libpqrcv_readtimelinehistoryfile")));
    }
    if res.nfields != 2 || res.rows.len() != 1 {
        return throw(ereport(ERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg("invalid response from primary server")
            .errdetail(format!(
                "Expected 1 tuple with 2 fields, got {} tuples with {} fields.",
                res.rows.len(),
                res.nfields
            ))
            .finish(loc("libpqrcv_readtimelinehistoryfile")));
    }
    let fname = text_col(&res, 0, 0);
    let content = res.rows[0][1].clone().unwrap_or_default();
    Ok((fname, content))
}

/// libpqrcv_receive: (len > 0, buf) = data; (0, wait socket) = try again;
/// (-1, _) = end of COPY stream.
pub fn receive(conn: &mut PgConn) -> PgResult<(i32, Vec<u8>, pgsocket)> {
    let first = match conn.get_copy_data() {
        Ok(d) => d,
        Err(e) => return receive_stream_error(&e),
    };
    let data = match first {
        CopyData::Block => {
            if !conn.consume_input() {
                return receive_stream_error(&conn.error_message());
            }
            match conn.get_copy_data() {
                Ok(CopyData::Block) => return Ok((0, Vec::new(), conn.socket())),
                Ok(d) => d,
                Err(e) => return receive_stream_error(&e),
            }
        }
        d => d,
    };
    match data {
        CopyData::Msg(buf) => Ok((buf.len() as i32, buf, PGINVALID_SOCKET)),
        CopyData::End => {
            let res = conn.get_result()?;
            match &res {
                Some(r) if r.status == ExecStatus::CommandOk => {
                    if conn.get_result()?.is_some() {
                        if conn.connection_bad() {
                            return Ok((-1, Vec::new(), PGINVALID_SOCKET));
                        }
                        return throw(ereport(ERROR)
                            .errcode(ERRCODE_PROTOCOL_VIOLATION)
                            .errmsg(format!(
                                "unexpected result after CommandComplete: {}",
                                conn.error_message()
                            ))
                            .finish(loc("libpqrcv_receive")));
                    }
                    Ok((-1, Vec::new(), PGINVALID_SOCKET))
                }
                Some(r) if r.status == ExecStatus::CopyIn => Ok((-1, Vec::new(), PGINVALID_SOCKET)),
                _ => receive_stream_error(&conn.error_message()),
            }
        }
        CopyData::Block => unreachable!(),
    }
}

fn receive_stream_error(err: &str) -> PgResult<(i32, Vec<u8>, pgsocket)> {
    throw(
        ereport(ERROR)
            .errcode(ERRCODE_CONNECTION_FAILURE)
            .errmsg(format!("could not receive data from WAL stream: {}", pchomp(err)))
            .finish(loc("libpqrcv_receive")),
    )
}

/// libpqrcv_send.
pub fn send(conn: &mut PgConn, buffer: &[u8]) -> PgResult<()> {
    if conn.put_copy_data(buffer).is_err() {
        return ereport(ERROR)
            .errcode(ERRCODE_CONNECTION_FAILURE)
            .errmsg(format!(
                "could not send data to WAL stream: {}",
                pchomp(&conn.error_message())
            ))
            .finish(loc("libpqrcv_send"));
    }
    Ok(())
}

/// libpqrcv_disconnect.
pub fn disconnect(mut conn: PgConn) {
    conn.terminate();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conninfo_parse_basic() {
        let opts = parse_conninfo("host=/tmp/x port=5433 user=al application_name='my app'").unwrap();
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
    fn msg_framing() {
        let m = msg(b'Q', b"SELECT 1\0");
        assert_eq!(m[0], b'Q');
        assert_eq!(be_i32(&m[1..5]) as usize, m.len() - 1);
    }

    #[test]
    fn error_fields() {
        let mut body = Vec::new();
        body.extend_from_slice(b"SFATAL\0");
        body.extend_from_slice(b"C57P01\0");
        body.extend_from_slice(b"Mgoodbye\0");
        body.push(0);
        assert_eq!(parse_error_fields(&body), "FATAL:  goodbye");
    }

    #[test]
    fn b64_roundtrip() {
        let data = [7u8; 18];
        assert_eq!(b64_decode(&b64(&data)).unwrap(), data);
    }

    #[test]
    fn scram_attr_lookup() {
        let f: Vec<&str> = "r=abc,s=c2FsdA==,i=4096".split(',').collect();
        assert_eq!(scram_attr(&f, 'r').unwrap(), "abc");
        assert_eq!(scram_attr(&f, 'i').unwrap(), "4096");
        assert!(scram_attr(&f, 'v').is_err());
    }
}
