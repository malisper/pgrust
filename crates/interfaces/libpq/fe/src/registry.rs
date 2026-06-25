//! Handle-registry adapter: backs the opaque `PgConnId` / `PgResultId` seam
//! contract of [`fe_seams`] with owned [`PgClientConn`] /
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

#[cfg(target_family = "wasm")]
#[allow(unused_imports)]
use wasm_libc_shim as libc;

use std::cell::RefCell;
#[cfg(not(target_family = "wasm"))]
use std::net::TcpStream;
#[cfg(not(target_family = "wasm"))]
use std::os::fd::AsRawFd;
#[cfg(not(target_family = "wasm"))]
use std::os::unix::net::UnixStream;

use fe_seams as s;
use ::types_libpqwalreceiver::{
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
    /// `PgConnId` -> the explicit conninfo options the connection was made with
    /// (the `PQconninfo(conn)` table, the slice `libpqrcv_get_conninfo` walks).
    /// Parallel to `CONNS`; index 0 is the NULL sentinel.
    static CONN_OPTIONS: RefCell<Vec<Option<Vec<ConninfoOption>>>> = RefCell::new(vec![None]);
}

/// `dispchar` values libpq assigns the security-sensitive keywords (the `*`
/// fields `PQconninfoOptions` marks secret, so `libpqrcv_get_conninfo`
/// obfuscates them). Mirrors the `"*"` dispchar entries of `PQconninfoOptions`.
fn conninfo_dispchar(keyword: &str) -> &'static str {
    match keyword {
        "password" => "*",
        _ => "",
    }
}

/// Record (parallel to `CONNS`) the option list `PQconninfo(conn)` returns: the
/// explicit `(keyword, value)` pairs the connection was opened with, each tagged
/// with its `dispchar`. Empty / `None` values are kept (the reader skips them).
fn store_conn_options(id: PgConnId, keys: &[String], vals: &[Option<String>]) {
    let opts: Vec<ConninfoOption> = keys
        .iter()
        .zip(vals.iter())
        .map(|(k, v)| ConninfoOption {
            keyword: k.clone(),
            val: v.clone(),
            dispchar: conninfo_dispchar(k).to_string(),
        })
        .collect();
    CONN_OPTIONS.with(|c| {
        let mut v = c.borrow_mut();
        if v.len() <= id {
            v.resize_with(id + 1, || None);
        }
        v[id] = Some(opts);
    });
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
    apply_conninfo_defaults(&mut o);
    Ok(o)
}

/// `conninfo_add_defaults` (fe-connect.c), the subset that matters for an
/// accepted startup packet: fill in option fallbacks not given explicitly nor
/// by a service entry. The decisive one is `user`, which the server requires
/// in the startup packet — C defaults it (in precedence order) to the `PGUSER`
/// environment variable, then to `pg_fe_getauthname()` (the OS login name via
/// `getpwuid(geteuid())`). Without this, an apply-worker/walreceiver conninfo
/// that omits `user=` would send no user name and the server rejects it with
/// `FATAL: no PostgreSQL user name specified in startup packet`.
fn apply_conninfo_defaults(o: &mut ResolvedOptions) {
    if o.user.as_deref().is_none_or(str::is_empty) {
        // envvar leg: PGUSER (conninfo_add_defaults applies it before the
        // compiled/special-case fallback).
        if let Some(env_user) = std::env::var("PGUSER").ok().filter(|s| !s.is_empty()) {
            o.user = Some(env_user);
        } else if let Some(name) = fe_getauthname() {
            // Special "user" handling: pg_fe_getauthname(). C leaves it NULL on
            // failure (only a problem if the caller truly gave no user); we
            // mirror that — a lookup failure leaves `user` unset.
            o.user = Some(name);
        }
    }
}

/// `pg_fe_getauthname(NULL)` (fe-auth.c) → `pg_fe_getusername(geteuid())`:
/// the effective OS user's login name via `getpwuid(geteuid())->pw_name`.
/// Returns `None` on any lookup failure (C returns NULL and leaves `user`
/// unset, which is not itself an error).
fn fe_getauthname() -> Option<String> {
    // SAFETY: geteuid never fails; getpwuid returns a pointer into a static
    // libc buffer (we are single-threaded at connect setup), copied out at
    // once below before any other libc call can clobber it.
    let uid = unsafe { libc::geteuid() };
    let pw = unsafe { libc::getpwuid(uid) };
    if pw.is_null() {
        return None;
    }
    let name_ptr = unsafe { (*pw).pw_name };
    if name_ptr.is_null() {
        return None;
    }
    let name = unsafe { std::ffi::CStr::from_ptr(name_ptr) };
    Some(name.to_string_lossy().into_owned())
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
    // ::types_libpqwalreceiver::ExecStatusType), so this is the identity.
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
        Ok(conn) => {
            let id = store_conn(conn);
            // Record the conninfo the connection was opened with. Mirror C's
            // `PQconninfo`, which reports defaults filled in (notably `user`,
            // resolved to the OS login name when omitted) — so the conninfo
            // `libpqrcv_get_conninfo` rebuilds carries the resolved user.
            let (keys, vals) = with_resolved_user(&keys, &vals, o.user.as_deref());
            store_conn_options(id, &keys, &vals);
            id
        }
        Err(e) => {
            record_connect_error(e.message());
            0
        }
    }
}

/// Return `(keys, vals)` for `store_conn_options` with the resolved `user`
/// folded in: if the caller supplied a non-empty `user` key its value is
/// overwritten with the resolved one (identical when explicit); if no `user`
/// key was present, one is appended. This makes the recorded conninfo match
/// what `build_startup_packet` actually sent (C's `PQconninfo` defaults-filled
/// table), so a downstream `libpqrcv_get_conninfo` rebuild keeps the user.
fn with_resolved_user(
    keys: &[String],
    vals: &[Option<String>],
    resolved_user: Option<&str>,
) -> (Vec<String>, Vec<Option<String>>) {
    let mut keys: Vec<String> = keys.to_vec();
    let mut vals: Vec<Option<String>> = vals.to_vec();
    let resolved_user = match resolved_user {
        Some(u) if !u.is_empty() => u.to_string(),
        _ => return (keys, vals),
    };
    if let Some(idx) = keys.iter().position(|k| k == "user") {
        vals[idx] = Some(resolved_user);
    } else {
        keys.push("user".to_string());
        vals.push(Some(resolved_user));
    }
    (keys, vals)
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
///
/// `libpqrcv_receive` calls this after `PQgetCopyData` returns -1 (the server
/// ended the CopyBoth stream, e.g. at a timeline switch): C's `PQgetResult`
/// then yields the trailing `PGRES_COMMAND_OK` (drained through
/// `ReadyForQuery`), and a second `PQgetResult` returns NULL. We mirror that via
/// the connection's just-ended-COPY state: the first call collects and returns
/// the trailing result; subsequent calls (and calls outside an end-of-COPY,
/// where `PQexec` already drained to ReadyForQuery) return NULL.
fn libpqsrv_get_result(conn: PgConnId, _wait_event_info: u32) -> PgResultId {
    match with_conn(conn, |c| c.get_result_after_copy()) {
        Ok(Some(r)) => store_result(r),
        Ok(None) => 0,
        Err(e) => {
            // A wire error while collecting the trailing result: surface it as a
            // PGRES_FATAL_ERROR result (libpq builds a fatal result, not NULL),
            // so the caller reports it rather than treating it as a clean end.
            let mut r = PGresult::make_empty(ExecStatusType::PGRES_FATAL_ERROR);
            r.err_msg = Some(e.message());
            store_result(r)
        }
    }
}

/// `libpqsrv_disconnect(conn)` — PQfinish: send Terminate and free the conn.
fn libpqsrv_disconnect(conn: PgConnId) {
    let taken = CONNS.with(|c| {
        let mut v = c.borrow_mut();
        v.get_mut(conn).and_then(|o| o.take())
    });
    CONN_OPTIONS.with(|c| {
        let mut v = c.borrow_mut();
        if let Some(slot) = v.get_mut(conn) {
            *slot = None;
        }
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
    // `PQresultStatus(NULL)` returns `PGRES_FATAL_ERROR` (fe-exec.c) — the C
    // callers (e.g. `libpqrcv_receive`'s end-of-stream `PQgetResult` ->
    // `PQresultStatus` after a peer disconnect, where `PQgetResult` yields NULL)
    // rely on this NULL-tolerance rather than dereferencing. Index 0 is our NULL
    // sentinel, so mirror C here instead of treating it as an invalid handle.
    if res == 0 {
        return ExecStatusType::PGRES_FATAL_ERROR;
    }
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
    use crate::client::CopyRecv;
    let r = with_conn(conn, |c| c.copy_receive());
    match r {
        // async PQgetCopyData: data ready, no data yet (0), or CopyDone (-1).
        Ok(CopyRecv::Data(buf)) => (buf.len() as i32, buf),
        Ok(CopyRecv::WouldBlock) => (0, Vec::new()),
        Ok(CopyRecv::Done) => (-1, Vec::new()),
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
    match with_conn(conn, |c| c.end_copy()) {
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

/// `PQconninfo(conn)` — the live connection's option list. C returns the full
/// `PQconninfoOption` table (every keyword, defaults filled in); here we return
/// the explicit options the connection was opened with, recorded at connect.
/// That is exactly the slice `libpqrcv_get_conninfo` walks to rebuild the
/// user-visible connection string (it skips empty/`D` options regardless).
fn pq_conninfo(conn: PgConnId) -> Option<Vec<ConninfoOption>> {
    CONN_OPTIONS.with(|c| c.borrow().get(conn).and_then(|o| o.clone()))
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

#[cfg(test)]
mod default_user_tests {
    use super::*;
    use crate::protocol3::{build_startup_packet3, StartupParams, PG_PROTOCOL_3_0};

    /// The OS login name the way C's `pg_fe_getauthname` would compute it.
    fn os_login() -> String {
        let uid = unsafe { libc::geteuid() };
        let pw = unsafe { libc::getpwuid(uid) };
        assert!(!pw.is_null(), "test host has no passwd entry for euid");
        let name = unsafe { std::ffi::CStr::from_ptr((*pw).pw_name) };
        name.to_string_lossy().into_owned()
    }

    fn parse(user: Option<&str>) -> ResolvedOptions {
        let mut keys = vec!["dbname".to_string()];
        let mut vals = vec![Some("postgres".to_string())];
        if let Some(u) = user {
            keys.push("user".to_string());
            vals.push(Some(u.to_string()));
        }
        resolve_options_expanded(&keys, &vals, true).unwrap()
    }

    fn startup_user(o: &ResolvedOptions) -> Option<String> {
        let params = StartupParams {
            pversion: PG_PROTOCOL_3_0,
            pguser: o.user.as_deref(),
            db_name: o.dbname.as_deref(),
            ..Default::default()
        };
        let pkt = build_startup_packet3(&params, &[]).unwrap();
        // The packet is: 4-byte version, then NUL-terminated key/value pairs.
        let body = &pkt[4..];
        let mut it = body.split(|&b| b == 0).filter(|s| !s.is_empty());
        while let (Some(k), Some(v)) = (it.next(), it.next()) {
            if k == b"user" {
                return Some(String::from_utf8_lossy(v).into_owned());
            }
        }
        None
    }

    #[test]
    fn omitted_user_defaults_to_os_login_and_reaches_startup_packet() {
        // No PGUSER in the env for this assertion.
        let saved = std::env::var("PGUSER").ok();
        unsafe { std::env::remove_var("PGUSER") };

        let o = parse(None);
        assert_eq!(o.user.as_deref(), Some(os_login().as_str()));
        // The decisive bug: the startup packet now carries a user name.
        assert_eq!(startup_user(&o).as_deref(), Some(os_login().as_str()));

        if let Some(v) = saved {
            unsafe { std::env::set_var("PGUSER", v) };
        }
    }

    #[test]
    fn explicit_user_wins() {
        let o = parse(Some("alice"));
        assert_eq!(o.user.as_deref(), Some("alice"));
        assert_eq!(startup_user(&o).as_deref(), Some("alice"));
    }

    #[test]
    fn pguser_env_used_when_user_omitted() {
        let saved = std::env::var("PGUSER").ok();
        unsafe { std::env::set_var("PGUSER", "bob_from_env") };

        let o = parse(None);
        assert_eq!(o.user.as_deref(), Some("bob_from_env"));
        assert_eq!(startup_user(&o).as_deref(), Some("bob_from_env"));

        match saved {
            Some(v) => unsafe { std::env::set_var("PGUSER", v) },
            None => unsafe { std::env::remove_var("PGUSER") },
        }
    }

    #[test]
    fn with_resolved_user_appends_when_absent() {
        let (keys, vals) =
            with_resolved_user(&["dbname".to_string()], &[Some("db".to_string())], Some("zoe"));
        let idx = keys.iter().position(|k| k == "user").unwrap();
        assert_eq!(vals[idx].as_deref(), Some("zoe"));
    }
}
