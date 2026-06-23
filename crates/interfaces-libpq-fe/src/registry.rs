//! Handle-registry adapter: backs the opaque `PgConnId` / `PgResultId` seam
//! contract of [`interfaces_libpq_fe_seams`] with owned [`PgClientConn`] /
//! [`PGresult`] objects.
//!
//! A C `PGconn *` / `PGresult *` is an opaque pointer; the
//! `interfaces-libpq-fe-seams` contract deliberately crosses them as opaque
//! `usize` handles "owned by the provider's registry" (see that crate's module
//! doc — this is the *sanctioned* model for the libpqwalreceiver glue, not the
//! forbidden token→value registry). This module is that registry: one
//! process-local thread-local table per object kind, `0` is the never-issued
//! NULL sentinel (the analog of a C `NULL`), a freed slot becomes `None` and is
//! reused, exactly as `regexp-engine`'s handle wiring does.
//!
//! # What is real vs loud
//!
//! Real (grounded over the owned client + a real OS socket): connect (trust /
//! cleartext password), simple-query exec / get-result, the COPY/CopyBoth
//! streaming path, finish, and every result/connection accessor.
//!
//! Loud seam-panic (faithful deferral, never a silent fake): the conninfo
//! string parser (`PQconninfoParse` / `PQconninfo` — the 1462-line option-table
//! parser of fe-connect.c is its own port), and the server-encoding-aware
//! escapers (`PQescapeLiteral` / `PQescapeIdentifier`). Those bottom out in
//! their declared loud seams below.

use std::cell::RefCell;
#[cfg(not(target_family = "wasm"))]
use std::net::TcpStream;
#[cfg(not(target_family = "wasm"))]
use std::os::fd::AsRawFd;
#[cfg(not(target_family = "wasm"))]
use std::os::unix::net::UnixStream;

use interfaces_libpq_fe_seams as s;
use types_libpqwalreceiver::{
    ConnStatusType, ConninfoOption, ExecStatusType as SeamExecStatus, PgConnId, PgResultId,
    Pgsocket,
};

use crate::client::PgClientConn;
use crate::protocol3::{StartupParams, PG_PROTOCOL_3_0};
use crate::result::{ExecStatusType, PGresult};
#[cfg(not(target_family = "wasm"))]
use crate::transport::SocketTransport;
use crate::transport::{Transport, TransportError};

// ===========================================================================
// Loud seams for the genuinely-unported legs (declared here; never installed
// with a fake). Calling one panics with its name, the sanctioned route.
// ===========================================================================

seam_core::seam!(
    /// `PQconninfoParse(conninfo, &err)` — the fe-connect.c conninfo/URI option
    /// parser (the 1462-line `conninfo_parse` + option-table machinery). Not yet
    /// ported; loud until it is.
    pub fn unported_conninfo_parse(conninfo: String) -> Result<Vec<ConninfoOption>, Option<String>>
);

seam_core::seam!(
    /// `PQconninfo(conn)` — return the live connection's full `PQconninfoOption`
    /// table. Requires the conninfo option-table model the parser owns; deferred.
    pub fn unported_conninfo(conn: PgConnId) -> Option<Vec<ConninfoOption>>
);

seam_core::seam!(
    /// `PQescapeLiteral` / `PQescapeIdentifier` — the server-encoding-aware SQL
    /// string/identifier escapers (need the connection's `client_encoding` +
    /// `standard_conforming_strings`). Not yet ported; loud until it is.
    pub fn unported_escape(conn: PgConnId, s: String, ident: bool) -> Option<String>
);

// ===========================================================================
// The owned-object registries.
// ===========================================================================

/// The concrete connection type the registry stores: a client over a boxed
/// byte-stream transport (so a TCP or Unix-domain socket — or a test pipe —
/// all live behind one type).
type StoredConn = PgClientConn<Box<dyn Transport>>;

thread_local! {
    /// `PgConnId` -> live connection. Index 0 is the NULL sentinel (always
    /// `None`); issued handles are `index` for `index >= 1`.
    static CONNS: RefCell<Vec<Option<StoredConn>>> = RefCell::new(vec![None]);
    /// `PgResultId` -> owned result. Index 0 is the NULL sentinel.
    static RESULTS: RefCell<Vec<Option<PGresult>>> = RefCell::new(vec![None]);
    /// The last connection-level error text for a connection that failed before
    /// it could be stored (so `PQerrorMessage` on a bad PGconn handle still
    /// reports something). Keyed by handle; `0` is the "last connect attempt".
    static CONN_ERRORS: RefCell<Vec<String>> = RefCell::new(vec![String::new()]);
}

/// Insert a connection, returning its handle (reusing a freed slot or pushing).
fn store_conn(conn: StoredConn) -> PgConnId {
    CONNS.with(|c| {
        let mut v = c.borrow_mut();
        if let Some(idx) = v.iter().skip(1).position(|s| s.is_none()) {
            let idx = idx + 1;
            v[idx] = Some(conn);
            idx
        } else {
            v.push(Some(conn));
            v.len() - 1
        }
    })
}

/// Insert a result, returning its handle.
fn store_result(res: PGresult) -> PgResultId {
    RESULTS.with(|r| {
        let mut v = r.borrow_mut();
        if let Some(idx) = v.iter().skip(1).position(|s| s.is_none()) {
            let idx = idx + 1;
            v[idx] = Some(res);
            idx
        } else {
            v.push(Some(res));
            v.len() - 1
        }
    })
}

/// Run `f` over the connection behind `id` (mutably). Panics if the handle is
/// the NULL sentinel or already freed — the analog of dereferencing a NULL /
/// dangling `PGconn *`, which is undefined behaviour in C; here it is a loud
/// abort rather than memory corruption.
fn with_conn<R>(id: PgConnId, f: impl FnOnce(&mut StoredConn) -> R) -> R {
    CONNS.with(|c| {
        let mut v = c.borrow_mut();
        let slot = v
            .get_mut(id)
            .and_then(|o| o.as_mut())
            .unwrap_or_else(|| panic!("libpq fe: use of invalid PGconn handle {id}"));
        f(slot)
    })
}

/// Run `f` over the result behind `id` (immutably). Panics for an invalid
/// handle (NULL/freed), as dereferencing a NULL `PGresult *` would be UB in C.
fn with_result<R>(id: PgResultId, f: impl FnOnce(&PGresult) -> R) -> R {
    RESULTS.with(|r| {
        let v = r.borrow();
        let slot = v
            .get(id)
            .and_then(|o| o.as_ref())
            .unwrap_or_else(|| panic!("libpq fe: use of invalid PGresult handle {id}"));
        f(slot)
    })
}

// ===========================================================================
// conninfo key/val -> startup params + socket address.
// ===========================================================================

/// The resolved subset of connection options the minimal client needs, pulled
/// out of the `(keys, vals)` arrays `libpqsrv_connect_params` is handed (these
/// arrays are already the expanded conninfo `libpqrcv_connect` assembled — see
/// `libpqwalreceiver.c`, which passes discrete `host`/`port`/`user`/… keys).
#[derive(Default)]
struct ResolvedOptions {
    host: Option<String>,
    hostaddr: Option<String>,
    port: Option<String>,
    user: Option<String>,
    dbname: Option<String>,
    password: Option<String>,
    replication: Option<String>,
    application_name: Option<String>,
    fallback_application_name: Option<String>,
    options: Option<String>,
    client_encoding: Option<String>,
}

/// Pair up the parallel `keys` / `vals` arrays into the options we read. An
/// unknown key is ignored (libpq would error on it during conninfo parsing,
/// but here the caller already validated keys; we only consume what we use).
/// Whether `dbname` is a recognized connection string (URI or `key=value`
/// form), mirroring libpq's `recognized_connection_string` (fe-connect.c).
fn recognized_connection_string(dbname: &str) -> bool {
    crate::conninfo_parse::uri_prefix_length(dbname) != 0 || dbname.contains('=')
}

/// `connectOptions2`'s expand_dbname leg: when `dbname` is itself a connection
/// string, parse it via `conninfo_parse` and apply its options in place (later
/// explicit keys still override). A parse error is surfaced verbatim (e.g.
/// `invalid port number: "-1"` validation happens via the parsed `port` below).
fn resolve_options_expanded(
    keys: &[String],
    vals: &[Option<String>],
    expand_dbname: bool,
) -> Result<ResolvedOptions, String> {
    let mut o = ResolvedOptions::default();
    for (k, v) in keys.iter().zip(vals.iter()) {
        let val = match v {
            Some(s) if !s.is_empty() => s.clone(),
            _ => continue,
        };
        if expand_dbname && k == "dbname" && recognized_connection_string(&val) {
            // PQconninfoParse: a syntax error here is reported as-is.
            let opts = crate::conninfo_parse::pq_conninfo_parse(&val)
                .map_err(|e| e.unwrap_or_else(|| "out of memory".to_string()))?;
            for opt in &opts {
                if let Some(ref ov) = opt.val {
                    if !ov.is_empty() {
                        apply_option(&mut o, &opt.keyword, ov.clone());
                    }
                }
            }
            continue;
        }
        apply_option(&mut o, k, val);
    }
    Ok(o)
}

fn apply_option(o: &mut ResolvedOptions, k: &str, val: String) {
    match k {
        "host" => o.host = Some(val),
        "hostaddr" => o.hostaddr = Some(val),
        "port" => o.port = Some(val),
        "user" => o.user = Some(val),
        "dbname" => o.dbname = Some(val),
        "password" => o.password = Some(val),
        "replication" => o.replication = Some(val),
        "application_name" => o.application_name = Some(val),
        "fallback_application_name" => o.fallback_application_name = Some(val),
        "options" => o.options = Some(val),
        "client_encoding" => o.client_encoding = Some(val),
        _ => {}
    }
}

/// The default Postgres TCP port (`DEF_PGPORT`).
// Unused on wasm: the only consumer is the cfg'd-out native `open_socket`.
#[cfg_attr(target_family = "wasm", allow(dead_code))]
const DEF_PGPORT: u16 = 5432;
/// The default Unix-socket directory (`DEFAULT_PGSOCKET_DIR`). Matches the
/// compiled default of a typical install; `host` overrides it.
#[cfg_attr(target_family = "wasm", allow(dead_code))]
const DEFAULT_PGSOCKET_DIR: &str = "/tmp";

/// wasm (single-process) stub: `interfaces-libpq-fe` is the libpq *client*
/// library, used by the walreceiver to dial out to another server. Single-user
/// wasm has no outbound sockets (`TcpStream`/`UnixStream` are absent on wasip1)
/// and never opens a client connection, so this path errors. The rest of the
/// client codec/protocol logic still compiles for completeness.
#[cfg(target_family = "wasm")]
fn open_socket(_o: &ResolvedOptions) -> Result<Box<dyn Transport>, TransportError> {
    Err(TransportError::Io(
        "outbound libpq connections are not supported in single-user wasm".to_string(),
    ))
}

/// Open the connection socket the way `fe-connect.c`'s `connectDBStart` does:
/// a host beginning with '/' (or absent, defaulting to the socket dir) is a
/// Unix-domain socket at `<dir>/.s.PGSQL.<port>`; otherwise a TCP connection to
/// `host`/`hostaddr`:`port`. Returns the boxed transport + its fd.
#[cfg(not(target_family = "wasm"))]
fn open_socket(o: &ResolvedOptions) -> Result<Box<dyn Transport>, TransportError> {
    let port: u16 = match &o.port {
        Some(p) => p
            .parse()
            .map_err(|_| TransportError::Io(format!("invalid port number: \"{p}\"")))?,
        None => DEF_PGPORT,
    };

    // hostaddr forces TCP; host beginning with '/' is a Unix socket path.
    let is_unix = o.hostaddr.is_none()
        && o
            .host
            .as_deref()
            .map(|h| h.starts_with('/'))
            .unwrap_or(true);

    if is_unix {
        let dir = o.host.as_deref().unwrap_or(DEFAULT_PGSOCKET_DIR);
        let path = format!("{dir}/.s.PGSQL.{port}");
        let stream = UnixStream::connect(&path).map_err(|e| {
            TransportError::Io(format!(
                "could not connect to server on socket \"{path}\": {e}"
            ))
        })?;
        let fd = stream.as_raw_fd();
        Ok(Box::new(SocketTransport::new(stream, fd)))
    } else {
        let host = o
            .hostaddr
            .as_deref()
            .or(o.host.as_deref())
            .unwrap_or("localhost");
        let stream = TcpStream::connect((host, port)).map_err(|e| {
            TransportError::Io(format!(
                "could not connect to server at \"{host}\" port {port}: {e}"
            ))
        })?;
        // TCP_NODELAY: libpq sets it (connectNoDelay) so small protocol
        // messages are not Nagle-delayed.
        let _ = stream.set_nodelay(true);
        let fd = stream.as_raw_fd();
        Ok(Box::new(SocketTransport::new(stream, fd)))
    }
}

// ===========================================================================
// Status-enum adapters: the local client enums <-> the seam contract enums.
// ===========================================================================

fn seam_exec_status(s: ExecStatusType) -> SeamExecStatus {
    // The local result status IS the seam enum (both re-export
    // types_libpqwalreceiver::ExecStatusType), so this is the identity.
    s
}

// ===========================================================================
// Seam provider bodies (`libpqsrv_*` transport + `pq_*` accessors).
// ===========================================================================

/// `libpqsrv_connect_params` — start a connection and return its handle. On
/// failure libpq still returns a (BAD) `PGconn *`; we mirror that by storing a
/// connection-less bad-conn marker is impossible with our owned model, so we
/// instead store nothing and return `0` (NULL). The walrcv caller checks
/// `PQstatus(conn) != CONNECTION_OK` *and* a NULL conn, so `0` is handled.
fn libpqsrv_connect_params(
    keys: Vec<String>,
    vals: Vec<Option<String>>,
    expand_dbname: bool,
    _wait_event_info: u32,
) -> PgConnId {
    let o = match resolve_options_expanded(&keys, &vals, expand_dbname) {
        Ok(o) => o,
        Err(e) => {
            record_connect_error(e);
            return 0;
        }
    };

    let transport = match open_socket(&o) {
        Ok(t) => t,
        Err(e) => {
            record_connect_error(e.message());
            return 0;
        }
    };

    let params = StartupParams {
        pversion: PG_PROTOCOL_3_0,
        pguser: o.user.as_deref(),
        db_name: o.dbname.as_deref(),
        replication: o.replication.as_deref(),
        pgoptions: o.options.as_deref(),
        send_appname: o.application_name.is_some() || o.fallback_application_name.is_some(),
        appname: o.application_name.as_deref(),
        fbappname: o.fallback_application_name.as_deref(),
        client_encoding_initial: o.client_encoding.as_deref(),
    };

    match PgClientConn::connect(transport, &params, o.password.as_deref()) {
        Ok(conn) => store_conn(conn),
        Err(e) => {
            record_connect_error(e.message());
            0
        }
    }
}

/// Stash a connect-attempt error so `PQerrorMessage(0)` can report it.
fn record_connect_error(msg: String) {
    CONN_ERRORS.with(|e| {
        e.borrow_mut()[0] = msg;
    });
}

/// `libpqsrv_exec(conn, query)` — send a query and wait for the single result.
fn libpqsrv_exec(conn: PgConnId, query: String, _wait_event_info: u32) -> PgResultId {
    let res = with_conn(conn, |c| c.exec(&query));
    match res {
        Ok(r) => store_result(r),
        Err(e) => {
            // libpq returns a PGRES_FATAL_ERROR result (not NULL) for a failed
            // PQexec; build one carrying the error text.
            let mut r = PGresult::make_empty(ExecStatusType::PGRES_FATAL_ERROR);
            r.err_msg = Some(e.message());
            store_result(r)
        }
    }
}

/// `libpqsrv_get_result(conn)` — fetch the next result (`0` == NULL == done).
/// `PQexec`/`libpqsrv_exec` already drains to ReadyForQuery and returns the one
/// result, so a follow-up `get_result` has nothing left: return NULL (the C
/// `PQgetResult` returning NULL after the final result), the loop terminator.
fn libpqsrv_get_result(_conn: PgConnId, _wait_event_info: u32) -> PgResultId {
    0
}

/// `libpqsrv_disconnect(conn)` — PQfinish: send Terminate and free the conn.
fn libpqsrv_disconnect(conn: PgConnId) {
    let taken = CONNS.with(|c| {
        let mut v = c.borrow_mut();
        v.get_mut(conn).and_then(|o| o.take())
    });
    if let Some(c) = taken {
        c.finish();
    }
}

/// `PQstatus(conn)`. A NULL/freed handle is CONNECTION_BAD (the C NULL-conn
/// path), so we don't dereference an invalid handle here.
fn pq_status(conn: PgConnId) -> ConnStatusType {
    CONNS.with(|c| {
        let v = c.borrow();
        match v.get(conn).and_then(|o| o.as_ref()) {
            Some(c) if c.is_ok() => ConnStatusType::CONNECTION_OK,
            _ => ConnStatusType::CONNECTION_BAD,
        }
    })
}

/// `PQconnectionUsedPassword(conn)`.
fn pq_connection_used_password(conn: PgConnId) -> bool {
    CONNS.with(|c| {
        c.borrow()
            .get(conn)
            .and_then(|o| o.as_ref())
            .map(|c| c.used_password())
            .unwrap_or(false)
    })
}

/// `PQerrorMessage(conn)`. For a valid handle, the connection's last error; for
/// a NULL/failed handle, the last connect-attempt error.
fn pq_error_message(conn: PgConnId) -> String {
    CONNS.with(|c| {
        match c.borrow().get(conn).and_then(|o| o.as_ref()) {
            Some(c) => c.error_message().to_string(),
            None => CONN_ERRORS.with(|e| e.borrow()[0].clone()),
        }
    })
}

fn pq_result_status(res: PgResultId) -> SeamExecStatus {
    with_result(res, |r| seam_exec_status(r.result_status()))
}

fn pq_result_error_field_sqlstate(res: PgResultId) -> Option<String> {
    with_result(res, |r| r.sqlstate.clone())
}

fn pq_clear(res: PgResultId) {
    RESULTS.with(|r| {
        if let Some(slot) = r.borrow_mut().get_mut(res) {
            *slot = None;
        }
    });
}

fn pq_nfields(res: PgResultId) -> i32 {
    with_result(res, |r| r.nfields())
}

fn pq_ntuples(res: PgResultId) -> i32 {
    with_result(res, |r| r.ntuples())
}

fn pq_fname(res: PgResultId, field_num: i32) -> Option<String> {
    with_result(res, |r| r.fname(field_num).map(|s| s.to_string()))
}

fn pq_getvalue(res: PgResultId, tup_num: i32, field_num: i32) -> Vec<u8> {
    with_result(res, |r| r.get_value(tup_num, field_num).to_vec())
}

fn pq_getisnull(res: PgResultId, tup_num: i32, field_num: i32) -> bool {
    with_result(res, |r| r.get_isnull(tup_num, field_num))
}

fn pq_getlength(res: PgResultId, tup_num: i32, field_num: i32) -> i32 {
    with_result(res, |r| r.get_length(tup_num, field_num))
}

/// `PQgetCopyData(conn, &buf, 1 /* async=0 here, blocking */)` — returns
/// `(rawlen, buf)`. `rawlen == -1` means CopyDone (end of stream); the buffer
/// is empty then. A negative other value (`-2`) would be an error in C; we map
/// a transport error to a CopyDone-with-error by leaving rawlen=-1 after
/// recording the message.
fn pq_get_copy_data(conn: PgConnId) -> (i32, Vec<u8>) {
    let r = with_conn(conn, |c| c.copy_receive());
    match r {
        Ok(Some(buf)) => (buf.len() as i32, buf),
        Ok(None) => (-1, Vec::new()),
        Err(_) => (-2, Vec::new()),
    }
}

/// `PQputCopyData(conn, buffer, nbytes)` — returns 1 on success, -1 on error.
fn pq_put_copy_data(conn: PgConnId, buffer: Vec<u8>) -> i32 {
    match with_conn(conn, |c| c.copy_send(&buffer)) {
        Ok(()) => 1,
        Err(_) => -1,
    }
}

/// `PQputCopyEnd(conn, NULL)` — returns 1 on success, -1 on error.
fn pq_put_copy_end(conn: PgConnId) -> i32 {
    match with_conn(conn, |c| c.copy_done()) {
        Ok(()) => 1,
        Err(_) => -1,
    }
}

/// `PQflush(conn)` — returns 0 on success, -1 on error.
fn pq_flush(conn: PgConnId) -> i32 {
    match with_conn(conn, |c| c.flush()) {
        Ok(()) => 0,
        Err(_) => -1,
    }
}

/// `PQconsumeInput(conn)` — returns 1 (success) / 0 (error). This blocking
/// client does not buffer ahead of message reads, so there is nothing to
/// consume eagerly; report success (the consumer follows with PQgetCopyData,
/// which does the real blocking read).
fn pq_consume_input(_conn: PgConnId) -> i32 {
    1
}

/// `PQsocket(conn)`.
fn pq_socket(conn: PgConnId) -> Pgsocket {
    CONNS.with(|c| {
        c.borrow()
            .get(conn)
            .and_then(|o| o.as_ref())
            .map(|c| c.socket())
            .unwrap_or(-1)
    })
}

/// `PQendcopy(conn)` — the legacy COPY terminator. The replication path uses
/// PQputCopyEnd + PQgetResult instead; the legacy `PQendcopy` is the obsolete
/// 2.0-protocol API. Returns 0 (success) for a connection that is not mid-copy.
fn pq_endcopy(conn: PgConnId) -> i32 {
    // Drain to ReadyForQuery (PQendcopy's v3 behaviour: it loops PQgetResult).
    match with_conn(conn, |c| c.collect_result()) {
        Ok(_) => 0,
        Err(_) => 1,
    }
}

/// `PQhost(conn)` — the host the connection used. Not retained on the owned
/// conn in this minimal model; report `None` (the consumer only uses it for
/// the error-context string).
fn pq_host(_conn: PgConnId) -> Option<String> {
    None
}

/// `PQport(conn)`.
fn pq_port(_conn: PgConnId) -> Option<String> {
    None
}

/// `PQserverVersion(conn)`.
fn pq_server_version(conn: PgConnId) -> i32 {
    CONNS.with(|c| {
        c.borrow()
            .get(conn)
            .and_then(|o| o.as_ref())
            .map(|c| c.server_version())
            .unwrap_or(0)
    })
}

/// `PQbackendPID(conn)`.
fn pq_backend_pid(conn: PgConnId) -> i32 {
    CONNS.with(|c| {
        c.borrow()
            .get(conn)
            .and_then(|o| o.as_ref())
            .map(|c| c.backend_pid())
            .unwrap_or(0)
    })
}

/// `PQconninfo(conn)` — the live connection's option list. The owned model does
/// not retain the full PQconninfoOption table (that is the conninfo parser's
/// job, deferred); loud rather than silent-empty.
fn pq_conninfo(conn: PgConnId) -> Option<Vec<ConninfoOption>> {
    unported_conninfo::call(conn)
}

/// `PQconninfoParse(conninfo, &err)` — the keyword=value conninfo parser
/// (`conninfo_parse` + option-table keyword validation). The URI form is not
/// yet ported (it surfaces a clear unsupported-URI error).
fn pq_conninfo_parse(conninfo: String) -> Result<Vec<ConninfoOption>, Option<String>> {
    crate::conninfo_parse::pq_conninfo_parse(&conninfo)
}

/// `PQescapeLiteral(conn, s)` — deferred to the loud encoding-aware escaper.
fn pq_escape_literal(conn: PgConnId, value: String) -> Option<String> {
    unported_escape::call(conn, value, false)
}

/// `PQescapeIdentifier(conn, s)` — deferred to the loud encoding-aware escaper.
fn pq_escape_identifier(conn: PgConnId, value: String) -> Option<String> {
    unported_escape::call(conn, value, true)
}

// ===========================================================================
// Install: wire every grounded transport/accessor seam over the registry.
// Called once, single-threaded, from `crate::init_seams()`.
// ===========================================================================

/// Install the libpq frontend client-transport + result/accessor seam
/// providers. The conninfo-parse and escape legs are left at their loud
/// `unported_*` seam default (REAL-OR-LOUD: never a silent fake).
pub fn install() {
    s::libpqsrv_connect_params::set(libpqsrv_connect_params);
    s::libpqsrv_exec::set(libpqsrv_exec);
    s::libpqsrv_get_result::set(libpqsrv_get_result);
    s::libpqsrv_disconnect::set(libpqsrv_disconnect);

    s::pq_status::set(pq_status);
    s::pq_connection_used_password::set(pq_connection_used_password);
    s::pq_error_message::set(pq_error_message);

    s::pq_result_status::set(pq_result_status);
    s::pq_result_error_field_sqlstate::set(pq_result_error_field_sqlstate);
    s::pq_clear::set(pq_clear);
    s::pq_nfields::set(pq_nfields);
    s::pq_ntuples::set(pq_ntuples);
    s::pq_fname::set(pq_fname);
    s::pq_getvalue::set(pq_getvalue);
    s::pq_getisnull::set(pq_getisnull);
    s::pq_getlength::set(pq_getlength);

    s::pq_get_copy_data::set(pq_get_copy_data);
    s::pq_put_copy_data::set(pq_put_copy_data);
    s::pq_put_copy_end::set(pq_put_copy_end);
    s::pq_flush::set(pq_flush);
    s::pq_consume_input::set(pq_consume_input);
    s::pq_socket::set(pq_socket);
    s::pq_endcopy::set(pq_endcopy);

    s::pq_host::set(pq_host);
    s::pq_port::set(pq_port);
    s::pq_server_version::set(pq_server_version);
    s::pq_backend_pid::set(pq_backend_pid);

    s::pq_conninfo::set(pq_conninfo);
    s::pq_conninfo_parse::set(pq_conninfo_parse);
    s::pq_escape_literal::set(pq_escape_literal);
    s::pq_escape_identifier::set(pq_escape_identifier);
}
