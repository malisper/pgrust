// connection.c — per-user-mapping connection cache over pgclient, remote
// transaction management (xact/subxact callbacks), and the abort-cleanup
// discipline (cancel + bounded-deadline pumps).
//
// C's ConnectionHash is a process global; one backend = one thread here, so
// the cache is a thread_local keyed by user-mapping OID exactly like C
// (comment at connection.c:36 — umid keying makes scans under one mapping
// share one connection and one remote snapshot).
//
// RefCell discipline (the reentrancy rule): catalog lookups can fire
// pgfdw_inval_callback, which borrows the map. So the map is only borrowed
// for plain access; entries are TAKEN OUT around catalog-touching work
// (make_new_connection), and remote I/O closures (`with_entry`) must not
// touch catalogs. C is reentrancy-safe here by construction; this is our
// equivalent.
//
// Deliberate divergences (recorded in notes/contrib-pgfdw-p2.md):
// - parallel_commit / parallel_abort server options are parsed but the
//   commit/abort paths always run C's serial arms.
// - GSSAPI delegation + SCRAM passthrough are dormant (same as dblink).
// - postgres_fdw.application_name rides the placeholder custom-GUC store
//   (SET works; no DefineCustomStringVariable metadata).
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::time::{Duration, Instant};

use rustc_hash::FxBuildHasher;

use datum::Datum;
use elog::ereport;
use pgclient::{PgConn, QueryResult, TransactionStatus, WaitEvents};
use types_core::xact::{
    SubXactEvent, XactEvent, SUBXACT_EVENT_ABORT_SUB, SUBXACT_EVENT_PRE_COMMIT_SUB,
    XACT_EVENT_ABORT, XACT_EVENT_COMMIT, XACT_EVENT_PARALLEL_ABORT, XACT_EVENT_PARALLEL_COMMIT,
    XACT_EVENT_PARALLEL_PRE_COMMIT, XACT_EVENT_PREPARE, XACT_EVENT_PRE_COMMIT,
    XACT_EVENT_PRE_PREPARE,
};
use types_core::Oid;
use types_error::{
    make_sqlstate, PgError, PgResult, WARNING, ERRCODE_CONNECTION_EXCEPTION,
    ERRCODE_CONNECTION_FAILURE, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION,
    ERRCODE_S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED,
};

use foreigncmds::foreign::{ForeignServer, UserMapping};

use crate::loc;

// Timeout to cancel an in-progress query or execute a cleanup query; if it
// expires the connection is assumed dead (connection.c:97).
const CONNECTION_CLEANUP_TIMEOUT_MS: i64 = 30_000;
// Delay before a cancel request is re-issued (connection.c:104).
const RETRY_CANCEL_TIMEOUT_MS: i64 = 1_000;

pub(crate) struct ConnCacheEntry {
    pub(crate) conn: Option<PgConn>,
    // 0 = no xact open, 1 = main xact, 2 = one level of subxact, ...
    pub(crate) xact_depth: i32,
    pub(crate) have_prep_stmt: bool,
    pub(crate) have_error: bool,
    pub(crate) changing_xact_state: bool,
    pub(crate) invalidated: bool,
    pub(crate) keep_connections: bool,
    // Parsed for option fidelity; the serial paths always run (divergence).
    pub(crate) parallel_commit: bool,
    pub(crate) parallel_abort: bool,
    pub(crate) serverid: Oid,
    pub(crate) server_hashvalue: u32,
    pub(crate) mapping_hashvalue: u32,
}

impl ConnCacheEntry {
    fn new() -> ConnCacheEntry {
        ConnCacheEntry {
            conn: None,
            xact_depth: 0,
            have_prep_stmt: false,
            have_error: false,
            changing_xact_state: false,
            invalidated: false,
            keep_connections: true,
            parallel_commit: false,
            parallel_abort: false,
            serverid: types_core::InvalidOid,
            server_hashvalue: 0,
            mapping_hashvalue: 0,
        }
    }
}

type ConnMap = HashMap<Oid, ConnCacheEntry, FxBuildHasher>;

thread_local! {
    static CONNECTIONS: RefCell<ConnMap> = RefCell::new(HashMap::with_hasher(FxBuildHasher));
    static CALLBACKS_REGISTERED: Cell<bool> = const { Cell::new(false) };
    static XACT_GOT_CONNECTION: Cell<bool> = const { Cell::new(false) };
    static CURSOR_NUMBER: Cell<u32> = const { Cell::new(0) };
    static PREP_STMT_NUMBER: Cell<u32> = const { Cell::new(0) };
    static WE_CONNECT: Cell<u32> = const { Cell::new(0) };
    static WE_GET_RESULT: Cell<u32> = const { Cell::new(0) };
    static WE_CLEANUP_RESULT: Cell<u32> = const { Cell::new(0) };
}

fn we_lazy(cell: &'static std::thread::LocalKey<Cell<u32>>, name: &str) -> PgResult<u32> {
    let v = cell.with(Cell::get);
    if v != 0 {
        return Ok(v);
    }
    let id = waitevent::custom::WaitEventExtensionNew(name)?;
    cell.with(|c| c.set(id));
    Ok(id)
}

pub(crate) fn wait_events() -> PgResult<WaitEvents> {
    Ok(WaitEvents {
        connect: we_lazy(&WE_CONNECT, "PostgresFdwConnect")?,
        receive: we_lazy(&WE_GET_RESULT, "PostgresFdwGetResult")?,
    })
}

// ---------- error reporting (pgfdw_report_error) ----------

// pgfdw_report_error(ERROR, ...): remote diag -> local PgError with the
// remote SQLSTATE (fallback ERRCODE_CONNECTION_FAILURE), the remote CONTEXT
// preserved and "remote SQL command: %s" appended.
#[cold]
pub(crate) fn remote_error(res: &QueryResult, sql: Option<&str>) -> Box<PgError> {
    let diag = res.diag.clone().unwrap_or_default();
    let sqlstate = if diag.sqlstate.len() == 5 {
        let b = diag.sqlstate.as_bytes();
        make_sqlstate([b[0], b[1], b[2], b[3], b[4]])
    } else {
        ERRCODE_CONNECTION_FAILURE
    };
    let mut primary = diag.primary.clone();
    if primary.is_empty() {
        primary = pchomp(&res.err);
    }
    let mut e = if !primary.is_empty() {
        PgError::error(primary)
    } else {
        PgError::error("could not obtain message string for remote error")
    };
    e = e.with_sqlstate(sqlstate);
    if !diag.detail.is_empty() {
        e = e.with_detail(diag.detail.clone());
    }
    if !diag.hint.is_empty() {
        e = e.with_hint(diag.hint.clone());
    }
    let mut ctx = String::new();
    if !diag.context.is_empty() {
        ctx.push_str(&diag.context);
    }
    if let Some(sql) = sql {
        if !ctx.is_empty() {
            ctx.push('\n');
        }
        ctx.push_str(&format!("remote SQL command: {sql}"));
    }
    if !ctx.is_empty() {
        e = e.with_context(ctx);
    }
    Box::new(e)
}

// pgfdw_report_error(WARNING, ...): emit and continue (cleanup paths).
fn remote_warning(res: &QueryResult, sql: Option<&str>) {
    let e = remote_error(res, sql);
    let mut b = ereport(WARNING).errcode(e.sqlstate).errmsg_internal(e.message.clone());
    if let Some(d) = &e.detail {
        b = b.errdetail_internal(d.clone());
    }
    if let Some(h) = &e.hint {
        b = b.errhint(h.clone());
    }
    if let Some(c) = &e.context {
        b = b.errcontext_msg(c.clone());
    }
    let _ = b.finish(loc("pgfdw_report_error"));
}

fn pchomp(s: &str) -> String {
    s.trim_end_matches('\n').to_string()
}

// ---------- entry access ----------

fn take_entry(key: Oid) -> ConnCacheEntry {
    CONNECTIONS.with(|c| c.borrow_mut().remove(&key)).unwrap_or_else(ConnCacheEntry::new)
}

fn put_entry(key: Oid, entry: ConnCacheEntry) {
    CONNECTIONS.with(|c| {
        c.borrow_mut().insert(key, entry);
    });
}

// Remote-I/O access to a cached connection. The closure MUST NOT touch
// catalogs (see the RefCell discipline note at the top).
pub(crate) fn with_entry<R>(
    key: Oid,
    f: impl FnOnce(&mut ConnCacheEntry) -> R,
) -> PgResult<R> {
    CONNECTIONS.with(|c| {
        let mut map = c.borrow_mut();
        let Some(entry) = map.get_mut(&key) else {
            return Err(Box::new(PgError::error(format!(
                "postgres_fdw: no cached connection for user mapping {key}"
            ))));
        };
        Ok(f(entry))
    })
}

// pgfdw_exec_query (sync arm): PQsendQuery + collect-last. The closure runs
// no catalog code; conversion happens outside the borrow.
pub(crate) fn exec_query(key: Oid, sql: &str) -> PgResult<QueryResult> {
    with_entry(key, |e| {
        let conn = e.conn.as_mut().expect("live connection");
        conn.exec(sql)
    })?
}

// create_cursor's PQsendQueryParams + pgfdw_get_result pair (text params).
pub(crate) fn exec_query_params(
    key: Oid,
    sql: &str,
    params: &[Option<&str>],
) -> PgResult<QueryResult> {
    with_entry(key, |e| {
        let conn = e.conn.as_mut().expect("live connection");
        conn.exec_params(sql, &[], params)
    })?
}

#[allow(dead_code)] // phase 3: MOVE BACKWARD gate for pre-15 remotes
pub(crate) fn server_version(key: Oid) -> PgResult<i32> {
    with_entry(key, |e| e.conn.as_ref().expect("live connection").server_version())
}

// ---------- GetConnection ----------

fn ensure_callbacks_registered() -> PgResult<()> {
    if CALLBACKS_REGISTERED.with(Cell::get) {
        return Ok(());
    }
    we_lazy(&WE_GET_RESULT, "PostgresFdwGetResult")?;
    xact::RegisterXactCallback(pgfdw_xact_callback, Datum::null());
    xact::RegisterSubXactCallback(pgfdw_subxact_callback, Datum::null());
    inval::invalidate::CacheRegisterSyscacheCallback(
        cache_syscache::cacheinfo::FOREIGNSERVEROID,
        pgfdw_inval_callback,
        Datum::null(),
    )?;
    inval::invalidate::CacheRegisterSyscacheCallback(
        cache_syscache::cacheinfo::USERMAPPINGOID,
        pgfdw_inval_callback,
        Datum::null(),
    )?;
    CALLBACKS_REGISTERED.with(|c| c.set(true));
    Ok(())
}

/// C GetConnection: cached (or fresh) connection for the user mapping, with
/// the remote transaction opened to the current local nesting level. Returns
/// the cache key (the user-mapping OID) — remote I/O goes through
/// `exec_query`/`with_entry` under that key.
pub(crate) fn get_connection<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    user: &UserMapping<'mcx>,
    will_prep_stmt: bool,
) -> PgResult<Oid> {
    ensure_callbacks_registered()?;
    XACT_GOT_CONNECTION.with(|c| c.set(true));

    let key = user.umid;
    let mut entry = take_entry(key);

    // Reject connections whose xact state changes were interrupted mid-flight
    // (pgfdw_reject_incomplete_xact_state_change).
    if entry.conn.is_some() && entry.changing_xact_state {
        let serverid = entry.serverid;
        disconnect_entry(&mut entry);
        put_entry(key, entry);
        let server = foreigncmds::foreign::GetForeignServer(mcx, serverid)?;
        return Err(Box::new(
            PgError::error(format!("connection to server \"{}\" was lost", server.servername))
                .with_sqlstate(ERRCODE_CONNECTION_EXCEPTION),
        ));
    }

    // Invalidated + out of transaction: drop so options take effect.
    if entry.conn.is_some() && entry.invalidated && entry.xact_depth == 0 {
        disconnect_entry(&mut entry);
    }

    if entry.conn.is_none() {
        if let Err(e) = make_new_connection(mcx, &mut entry, user) {
            put_entry(key, entry);
            return Err(e);
        }
    }

    // begin_remote_xact with C's retry-once arm: only when the error is
    // connection-failure-class, the connection is bad, and we are not inside
    // a remote transaction (GetConnection's PG_CATCH, connection.c:297).
    match begin_remote_xact(&mut entry) {
        Ok(()) => {}
        Err(e) => {
            let retryable = e.sqlstate == ERRCODE_CONNECTION_FAILURE
                && entry.conn.as_ref().is_some_and(|c| c.connection_bad())
                && entry.xact_depth == 0;
            if !retryable {
                put_entry(key, entry);
                return Err(e);
            }
            disconnect_entry(&mut entry);
            if let Err(e2) = make_new_connection(mcx, &mut entry, user) {
                put_entry(key, entry);
                return Err(e2);
            }
            if let Err(e2) = begin_remote_xact(&mut entry) {
                put_entry(key, entry);
                return Err(e2);
            }
        }
    }

    entry.have_prep_stmt |= will_prep_stmt;
    put_entry(key, entry);
    Ok(key)
}

/// C ReleaseConnection: a no-op — cleanup happens per-(sub)transaction.
pub(crate) fn release_connection(_key: Oid) {}

/// GetCursorNumber: quasi-unique per transaction (reset by the xact callback).
pub(crate) fn get_cursor_number() -> u32 {
    CURSOR_NUMBER.with(|c| {
        let v = c.get() + 1;
        c.set(v);
        v
    })
}

/// GetPrepStmtNumber: never reset within a session (connection.c:917).
#[allow(dead_code)]
pub(crate) fn get_prep_stmt_number() -> u32 {
    PREP_STMT_NUMBER.with(|c| {
        let v = c.get() + 1;
        c.set(v);
        v
    })
}

// ---------- connection establishment ----------

fn make_new_connection<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    entry: &mut ConnCacheEntry,
    user: &UserMapping<'mcx>,
) -> PgResult<()> {
    let server = foreigncmds::foreign::GetForeignServer(mcx, user.serverid)?;
    debug_assert!(entry.conn.is_none());

    entry.xact_depth = 0;
    entry.have_prep_stmt = false;
    entry.have_error = false;
    entry.changing_xact_state = false;
    entry.invalidated = false;
    entry.serverid = server.serverid;
    entry.server_hashvalue = cache_syscache::GetSysCacheHashValue(
        cache_syscache::cacheinfo::FOREIGNSERVEROID,
        cache_syscache::SysCacheKey::Value(Datum::from_oid(server.serverid)),
        cache_syscache::SysCacheKey::UNUSED,
        cache_syscache::SysCacheKey::UNUSED,
        cache_syscache::SysCacheKey::UNUSED,
    )?;
    entry.mapping_hashvalue = cache_syscache::GetSysCacheHashValue(
        cache_syscache::cacheinfo::USERMAPPINGOID,
        cache_syscache::SysCacheKey::Value(Datum::from_oid(user.umid)),
        cache_syscache::SysCacheKey::UNUSED,
        cache_syscache::SysCacheKey::UNUSED,
        cache_syscache::SysCacheKey::UNUSED,
    )?;

    // Server-option-driven policies, computed at connect time only
    // (connection.c:388: changing them forces close+remake via inval).
    entry.keep_connections = true;
    entry.parallel_commit = false;
    entry.parallel_abort = false;
    for opt in server.options.iter() {
        match opt.name {
            "keep_connections" => entry.keep_connections = def_get_boolean(opt.value),
            "parallel_commit" => entry.parallel_commit = def_get_boolean(opt.value),
            "parallel_abort" => entry.parallel_abort = def_get_boolean(opt.value),
            _ => {}
        }
    }

    entry.conn = Some(connect_pg_server(&server, user)?);
    Ok(())
}

fn def_get_boolean(value: Option<&str>) -> bool {
    // C defGetBoolean: a DefElem with a NULL arg means true.
    match value {
        None => true,
        Some(v) => matches!(v.to_ascii_lowercase().as_str(), "true" | "t" | "on" | "1" | "yes" | "y"),
    }
}

// connect_pg_server: assemble libpq options (server options, then
// user-mapping options — last wins), application_name precedence
// (option < postgres_fdw.application_name GUC), process_pgfdw_appname
// escapes, fallback application_name "postgres_fdw", client_encoding pinned
// to the local database encoding.
fn connect_pg_server<'mcx>(
    server: &ForeignServer<'mcx>,
    user: &UserMapping<'mcx>,
) -> PgResult<PgConn> {
    let mut opts: Vec<(String, String)> = Vec::new();
    for opt in server.options.iter() {
        if crate::option::is_libpq_option(opt.name) {
            opts.push((opt.name.to_string(), opt.require_value()?.to_string()));
        }
    }
    for opt in user.options.iter() {
        if crate::option::is_libpq_option(opt.name) {
            opts.push((opt.name.to_string(), opt.require_value()?.to_string()));
        }
    }
    // postgres_fdw.application_name GUC overrides any connection option.
    if let Ok(Some(v)) = guc::GetConfigOption("postgres_fdw.application_name", true, false) {
        if !v.is_empty() {
            opts.push(("application_name".to_string(), v));
        }
    }
    // Search backwards for the last non-empty application_name and expand
    // its escape sequences; empty expansions unset the slot and keep looking.
    let mut i = opts.len();
    while i > 0 {
        i -= 1;
        if opts[i].0 == "application_name" && !opts[i].1.is_empty() {
            let expanded = crate::option::process_pgfdw_appname(&opts[i].1)?;
            if !expanded.is_empty() {
                opts[i].1 = expanded;
                break;
            }
            opts.remove(i);
        }
    }
    // fallback_application_name "postgres_fdw" (libpq applies the fallback
    // only when no application_name won; emulated here).
    if !opts.iter().any(|(k, v)| k == "application_name" && !v.is_empty()) {
        opts.push(("application_name".to_string(), "postgres_fdw".to_string()));
    }

    check_conn_params(&opts, user)?;

    // Build a conninfo string; pgclient's resolver applies libpq's defaults
    // ladder exactly as PQconnectdbParams would.
    let mut conninfo = String::new();
    for (k, v) in &opts {
        conninfo.push_str(k);
        conninfo.push_str("='");
        conninfo.push_str(&escape_param_str(v));
        conninfo.push_str("' ");
    }

    we_lazy(&WE_CONNECT, "PostgresFdwConnect")?;
    let conn = match pgclient::connect_db(
        &conninfo,
        Some(mbutils::GetDatabaseEncodingName()),
        wait_events()?,
    )? {
        Ok(conn) => conn,
        Err(msg) => {
            return Err(Box::new(
                PgError::error(format!(
                    "could not connect to server \"{}\"",
                    server.servername
                ))
                .with_sqlstate(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION)
                .with_detail(pchomp(&msg)),
            ));
        }
    };

    if let Err(e) = pgfdw_security_check(&opts, user, &conn) {
        let mut conn = conn;
        conn.terminate();
        return Err(e);
    }

    let mut conn = conn;
    if let Err(e) = configure_remote_session(&mut conn) {
        conn.terminate();
        return Err(e);
    }
    Ok(conn)
}

fn escape_param_str(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        if c == '\\' || c == '\'' {
            out.push('\\');
        }
        out.push(c);
    }
    out
}

fn user_mapping_password_required(user: &UserMapping<'_>) -> bool {
    for opt in user.options.iter() {
        if opt.name == "password_required" {
            return def_get_boolean(opt.value);
        }
    }
    true
}

fn opts_have_password(opts: &[(String, String)]) -> bool {
    opts.iter().any(|(k, v)| k == "password" && !v.is_empty())
}

// check_conn_params: pre-connect. GSS delegation and SCRAM passthrough are
// dormant (no delegated creds / has_scram_keys on this server).
fn check_conn_params(opts: &[(String, String)], user: &UserMapping<'_>) -> PgResult<()> {
    if superuser::superuser_arg(user.userid)? {
        return Ok(());
    }
    if opts_have_password(opts) {
        return Ok(());
    }
    if !user_mapping_password_required(user) {
        return Ok(());
    }
    Err(Box::new(
        PgError::error("password or GSSAPI delegated credentials required")
            .with_sqlstate(ERRCODE_S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED)
            .with_detail(
                "Non-superusers must delegate GSSAPI credentials, provide a password, or enable SCRAM pass-through in user mapping.",
            ),
    ))
}

// pgfdw_security_check: post-connect — the password must actually have been
// used by the server's auth exchange.
fn pgfdw_security_check(
    opts: &[(String, String)],
    user: &UserMapping<'_>,
    conn: &PgConn,
) -> PgResult<()> {
    if superuser::superuser_arg(user.userid)? {
        return Ok(());
    }
    if !user_mapping_password_required(user) {
        return Ok(());
    }
    if conn.used_password() && opts_have_password(opts) {
        return Ok(());
    }
    Err(Box::new(
        PgError::error("password or GSSAPI delegated credentials required")
            .with_sqlstate(ERRCODE_S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED)
            .with_detail(
                "Non-superuser cannot connect if the server does not request a password or use GSSAPI with delegated credentials.",
            )
            .with_hint(
                "Target server's authentication method must be changed or password_required=false set in the user mapping attributes.",
            ),
    ))
}

// configure_remote_session: force settings the deparser and row conversion
// rely on (connection.c:766); version-gated exactly as C.
fn configure_remote_session(conn: &mut PgConn) -> PgResult<()> {
    let remoteversion = conn.server_version();
    do_sql_command(conn, "SET search_path = pg_catalog")?;
    do_sql_command(conn, "SET timezone = 'GMT'")?;
    do_sql_command(conn, "SET datestyle = ISO")?;
    if remoteversion >= 80400 {
        do_sql_command(conn, "SET intervalstyle = postgres")?;
    }
    if remoteversion >= 90000 {
        do_sql_command(conn, "SET extra_float_digits = 3")?;
    } else {
        do_sql_command(conn, "SET extra_float_digits = 2")?;
    }
    Ok(())
}

pub(crate) fn do_sql_command(conn: &mut PgConn, sql: &str) -> PgResult<()> {
    let res = conn.exec(sql)?;
    if res.status != pgclient::ExecStatus::CommandOk {
        return Err(remote_error(&res, Some(sql)));
    }
    Ok(())
}

// begin_remote_xact: open the remote main transaction (>= REPEATABLE READ;
// SERIALIZABLE mirrors the local level) and stack savepoints lazily up to the
// current local nesting level.
fn begin_remote_xact(entry: &mut ConnCacheEntry) -> PgResult<()> {
    let curlevel = xact::GetCurrentTransactionNestLevel();
    if entry.xact_depth <= 0 {
        let sql = if xact::IsolationIsSerializable() {
            "START TRANSACTION ISOLATION LEVEL SERIALIZABLE"
        } else {
            "START TRANSACTION ISOLATION LEVEL REPEATABLE READ"
        };
        entry.changing_xact_state = true;
        do_sql_command(entry.conn.as_mut().expect("live connection"), sql)?;
        entry.xact_depth = 1;
        entry.changing_xact_state = false;
    }
    while entry.xact_depth < curlevel {
        let sql = format!("SAVEPOINT s{}", entry.xact_depth + 1);
        entry.changing_xact_state = true;
        do_sql_command(entry.conn.as_mut().expect("live connection"), &sql)?;
        entry.xact_depth += 1;
        entry.changing_xact_state = false;
    }
    Ok(())
}

fn disconnect_entry(entry: &mut ConnCacheEntry) {
    if let Some(mut conn) = entry.conn.take() {
        conn.terminate();
    }
}

// ---------- transaction callbacks ----------

// pgfdw_xact_callback. The serial paths only (parallel_commit/parallel_abort
// recorded divergence).
fn pgfdw_xact_callback(event: XactEvent, _arg: Datum) -> PgResult<()> {
    if !XACT_GOT_CONNECTION.with(Cell::get) {
        return Ok(());
    }
    // Take the whole map for the scan; reinstall in every path.
    let mut map = CONNECTIONS.with(|c| std::mem::take(&mut *c.borrow_mut()));
    let r = (|| -> PgResult<()> {
        for entry in map.values_mut() {
            if entry.conn.is_none() {
                continue;
            }
            if entry.xact_depth > 0 {
                match event {
                    XACT_EVENT_PARALLEL_PRE_COMMIT | XACT_EVENT_PRE_COMMIT => {
                        // Interrupted-state connections cannot commit.
                        if entry.changing_xact_state {
                            let serverid = entry.serverid;
                            disconnect_entry(entry);
                            return Err(connection_lost_error(serverid));
                        }
                        entry.changing_xact_state = true;
                        do_sql_command(
                            entry.conn.as_mut().expect("live connection"),
                            "COMMIT TRANSACTION",
                        )?;
                        entry.changing_xact_state = false;
                        if entry.have_prep_stmt && entry.have_error {
                            // Errors deliberately ignored (DEALLOCATE ALL).
                            let _ = entry
                                .conn
                                .as_mut()
                                .expect("live connection")
                                .exec("DEALLOCATE ALL");
                        }
                        entry.have_prep_stmt = false;
                        entry.have_error = false;
                    }
                    XACT_EVENT_PRE_PREPARE => {
                        return Err(Box::new(
                            PgError::error(
                                "cannot PREPARE a transaction that has operated on postgres_fdw foreign tables",
                            )
                            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
                        ));
                    }
                    XACT_EVENT_PARALLEL_COMMIT | XACT_EVENT_COMMIT | XACT_EVENT_PREPARE => {
                        panic!("missed cleaning up connection during pre-commit");
                    }
                    XACT_EVENT_PARALLEL_ABORT | XACT_EVENT_ABORT => {
                        pgfdw_abort_cleanup(entry, true)?;
                    }
                }
            }
            pgfdw_reset_xact_state(entry, true);
        }
        Ok(())
    })();
    CONNECTIONS.with(|c| *c.borrow_mut() = map);
    // Regardless of the event outcome we are now out of the transaction
    // (matches C: these reset at the bottom of every non-quick-exit call).
    XACT_GOT_CONNECTION.with(|c| c.set(false));
    CURSOR_NUMBER.with(|c| c.set(0));
    r
}

#[track_caller]
#[cold]
fn connection_lost_error(serverid: Oid) -> Box<PgError> {
    let scratch = mcx::MemoryContext::new("postgres_fdw connection error");
    let name = foreigncmds::foreign::GetForeignServer(scratch.mcx(), serverid)
        .map(|s| s.servername.to_string())
        .unwrap_or_else(|_| format!("{serverid}"));
    Box::new(
        PgError::error(format!("connection to server \"{name}\" was lost"))
            .with_sqlstate(ERRCODE_CONNECTION_EXCEPTION),
    )
}

// pgfdw_subxact_callback (serial paths).
fn pgfdw_subxact_callback(
    event: SubXactEvent,
    _my_subid: types_core::xact::SubTransactionId,
    _parent_subid: types_core::xact::SubTransactionId,
    _arg: Datum,
) -> PgResult<()> {
    if !matches!(event, SUBXACT_EVENT_PRE_COMMIT_SUB | SUBXACT_EVENT_ABORT_SUB) {
        return Ok(());
    }
    if !XACT_GOT_CONNECTION.with(Cell::get) {
        return Ok(());
    }
    let curlevel = xact::GetCurrentTransactionNestLevel();
    let mut map = CONNECTIONS.with(|c| std::mem::take(&mut *c.borrow_mut()));
    let r = (|| -> PgResult<()> {
        for entry in map.values_mut() {
            if entry.conn.is_none() || entry.xact_depth < curlevel {
                continue;
            }
            if entry.xact_depth > curlevel {
                panic!(
                    "missed cleaning up remote subtransaction at level {}",
                    entry.xact_depth
                );
            }
            if event == SUBXACT_EVENT_PRE_COMMIT_SUB {
                if entry.changing_xact_state {
                    let serverid = entry.serverid;
                    disconnect_entry(entry);
                    return Err(connection_lost_error(serverid));
                }
                let sql = format!("RELEASE SAVEPOINT s{curlevel}");
                entry.changing_xact_state = true;
                do_sql_command(entry.conn.as_mut().expect("live connection"), &sql)?;
                entry.changing_xact_state = false;
            } else {
                pgfdw_abort_cleanup(entry, false)?;
            }
            pgfdw_reset_xact_state(entry, false);
        }
        Ok(())
    })();
    CONNECTIONS.with(|c| *c.borrow_mut() = map);
    r
}

// pgfdw_inval_callback: connection depends on its server + user mapping
// syscache entries; drop now (idle) or mark for reconnect (in use).
fn pgfdw_inval_callback(_arg: Datum, cacheid: i32, hashvalue: u32) {
    debug_assert!(
        cacheid == cache_syscache::cacheinfo::FOREIGNSERVEROID
            || cacheid == cache_syscache::cacheinfo::USERMAPPINGOID
    );
    CONNECTIONS.with(|c| {
        for entry in c.borrow_mut().values_mut() {
            if entry.conn.is_none() {
                continue;
            }
            let matches = hashvalue == 0
                || (cacheid == cache_syscache::cacheinfo::FOREIGNSERVEROID
                    && entry.server_hashvalue == hashvalue)
                || (cacheid == cache_syscache::cacheinfo::USERMAPPINGOID
                    && entry.mapping_hashvalue == hashvalue);
            if matches {
                if entry.xact_depth == 0 {
                    disconnect_entry(entry);
                } else {
                    entry.invalidated = true;
                }
            }
        }
    });
}

// pgfdw_reset_xact_state: end-of-(sub)xact bookkeeping + the discard rules.
fn pgfdw_reset_xact_state(entry: &mut ConnCacheEntry, toplevel: bool) {
    if toplevel {
        entry.xact_depth = 0;
        let unhealthy = match entry.conn.as_ref() {
            None => false,
            Some(conn) => {
                conn.connection_bad()
                    || conn.transaction_status() != TransactionStatus::Idle
            }
        };
        if entry.conn.is_some()
            && (unhealthy
                || entry.changing_xact_state
                || entry.invalidated
                || !entry.keep_connections)
        {
            disconnect_entry(entry);
        }
    } else {
        entry.xact_depth -= 1;
    }
}

// ---------- abort cleanup (cancel + bounded-deadline pumps) ----------

// pgfdw_abort_cleanup: abort the remote (sub)transaction. On any failure the
// entry keeps changing_xact_state=true, which makes GetConnection /
// pgfdw_reset_xact_state discard the connection — C's exact rule.
fn pgfdw_abort_cleanup(entry: &mut ConnCacheEntry, toplevel: bool) -> PgResult<()> {
    if elog::in_error_recursion_trouble() {
        entry.changing_xact_state = true;
    }
    if entry.changing_xact_state {
        return Ok(());
    }
    entry.changing_xact_state = true;
    entry.have_error = true;

    let conn = entry.conn.as_mut().expect("live connection");
    if conn.transaction_status() == TransactionStatus::Active && !pgfdw_cancel_query(conn)? {
        return Ok(()); // unsalvageable; stays changing_xact_state=true
    }
    let sql = if toplevel {
        "ABORT TRANSACTION".to_string()
    } else {
        format!(
            "ROLLBACK TO SAVEPOINT s{0}; RELEASE SAVEPOINT s{0}",
            entry.xact_depth
        )
    };
    if !pgfdw_exec_cleanup_query(conn, &sql, false)? {
        return Ok(());
    }
    if toplevel {
        if entry.have_prep_stmt
            && entry.have_error
            && !pgfdw_exec_cleanup_query(
                entry.conn.as_mut().expect("live connection"),
                "DEALLOCATE ALL",
                true,
            )?
        {
            return Ok(());
        }
        entry.have_prep_stmt = false;
        entry.have_error = false;
    }
    entry.changing_xact_state = false;
    Ok(())
}

// pgfdw_cancel_query: request cancel of whatever is in progress, then swallow
// its result within the cleanup deadline.
fn pgfdw_cancel_query(conn: &mut PgConn) -> PgResult<bool> {
    let endtime = Instant::now() + Duration::from_millis(CONNECTION_CLEANUP_TIMEOUT_MS as u64);
    let retrycanceltime =
        Instant::now() + Duration::from_millis(RETRY_CANCEL_TIMEOUT_MS as u64);
    // pgfdw_cancel_query_begin: send the cancel request.
    if let Some(msg) = conn.cancel(CONNECTION_CLEANUP_TIMEOUT_MS)? {
        let _ = ereport(WARNING)
            .errcode(ERRCODE_CONNECTION_FAILURE)
            .errmsg(format!("could not send cancel request: {msg}"))
            .finish(loc("pgfdw_cancel_query_begin"));
        return Ok(false);
    }
    // pgfdw_cancel_query_end: swallow the canceled query's result.
    match pgfdw_get_cleanup_result(conn, endtime, retrycanceltime)? {
        CleanupResult::TimedOut => {
            let _ = ereport(WARNING)
                .errmsg("could not get result of cancel request due to timeout")
                .finish(loc("pgfdw_cancel_query_end"));
            Ok(false)
        }
        CleanupResult::Failed => {
            let _ = ereport(WARNING)
                .errcode(ERRCODE_CONNECTION_FAILURE)
                .errmsg(format!(
                    "could not get result of cancel request: {}",
                    pchomp(&conn.error_message())
                ))
                .finish(loc("pgfdw_cancel_query_end"));
            Ok(false)
        }
        CleanupResult::Done(_) => Ok(true),
    }
}

// pgfdw_exec_cleanup_query: run a cleanup command with the 30s deadline.
// false = the connection is unsalvageable (warnings already emitted).
fn pgfdw_exec_cleanup_query(
    conn: &mut PgConn,
    query: &str,
    ignore_errors: bool,
) -> PgResult<bool> {
    let endtime = Instant::now() + Duration::from_millis(CONNECTION_CLEANUP_TIMEOUT_MS as u64);
    if !conn.send_query(query) {
        remote_warning(&QueryResult::conn_error(conn.error_message()), Some(query));
        return Ok(false);
    }
    match pgfdw_get_cleanup_result(conn, endtime, endtime)? {
        CleanupResult::TimedOut => {
            let _ = ereport(WARNING)
                .errmsg("could not get query result due to timeout")
                .errcontext_msg(format!("remote SQL command: {query}"))
                .finish(loc("pgfdw_exec_cleanup_query_end"));
            Ok(false)
        }
        CleanupResult::Failed => {
            remote_warning(&QueryResult::conn_error(conn.error_message()), Some(query));
            Ok(false)
        }
        CleanupResult::Done(res) => {
            if res.as_ref().is_some_and(|r| r.status == pgclient::ExecStatus::CommandOk) {
                Ok(true)
            } else {
                if let Some(r) = res {
                    remote_warning(&r, Some(query));
                }
                Ok(ignore_errors)
            }
        }
    }
}

// ---------- SQL introspection functions (connection.c:2134-2460) ----------

// Snapshot of one cache entry, taken under the map borrow; the catalog
// lookups (server/user names) run after release (RefCell discipline).
struct ConnRow {
    umid: Oid,
    serverid: Oid,
    invalidated: bool,
    xact_depth: i32,
    be_pid: i32,
}

fn server_name_missing_ok(serverid: Oid) -> PgResult<Option<String>> {
    let Some(tp) = cache_syscache::SearchSysCache1(
        cache_syscache::cacheinfo::FOREIGNSERVEROID,
        cache_syscache::SysCacheKey::Value(Datum::from_oid(serverid)),
    )?
    else {
        return Ok(None);
    };
    let d = cache_syscache::SysCacheGetAttrNotNull(
        cache_syscache::cacheinfo::FOREIGNSERVEROID,
        &tp,
        foreigncmds::foreign::Anum_pg_foreign_server_srvname,
    )?;
    // SAFETY: name attribute — NUL-terminated NameData image.
    let s = unsafe { core::ffi::CStr::from_ptr(d.as_usize() as *const core::ffi::c_char) }
        .to_string_lossy()
        .into_owned();
    cache_syscache::ReleaseSysCache(tp);
    Ok(Some(s))
}

// MappingUserName over the mapping row (NULL when the mapping was dropped).
fn mapping_user_name(umid: Oid) -> PgResult<Option<String>> {
    let Some(tp) = cache_syscache::SearchSysCache1(
        cache_syscache::cacheinfo::USERMAPPINGOID,
        cache_syscache::SysCacheKey::Value(Datum::from_oid(umid)),
    )?
    else {
        return Ok(None);
    };
    let umuser = cache_syscache::SysCacheGetAttrNotNull(
        cache_syscache::cacheinfo::USERMAPPINGOID,
        &tp,
        foreigncmds::foreign::Anum_pg_user_mapping_umuser,
    )?
    .as_oid();
    cache_syscache::ReleaseSysCache(tp);
    if umuser == types_core::InvalidOid {
        return Ok(Some("public".to_string()));
    }
    let scratch = mcx::MemoryContext::new("pgfdw user name");
    let name = {
        let m = scratch.mcx();
        miscinit::GetUserNameFromId(m, umuser, false)?.map(|n| n.as_str().to_string())
    };
    Ok(name)
}

fn get_connections_internal(
    flinfo: &mut types_fmgr::FmgrInfo,
    fcinfo: &mut types_fmgr::FunctionCallInfoBaseData,
    v1_2: bool,
) -> PgResult<Datum> {
    let fcinfo_ptr: *mut types_fmgr::FunctionCallInfoBaseData = fcinfo;
    // SAFETY: single-threaded fmgr call; the result context outlives it
    // (materialize.rs TupleSink precedent for the aliasing shape).
    let mcx = unsafe { &*fcinfo_ptr }.result_mcx();
    let mut srf = funcapi::InitMaterializedSRF(mcx, flinfo, fcinfo, 0)?;
    let expected = if v1_2 { 6 } else { 2 };
    if srf.tupdesc.natts != expected {
        return Err(Box::new(PgError::error("incorrect number of output arguments")));
    }
    let rows: Vec<ConnRow> = CONNECTIONS.with(|c| {
        c.borrow()
            .iter()
            .filter(|(_, e)| e.conn.is_some())
            .map(|(&umid, e)| ConnRow {
                umid,
                serverid: e.serverid,
                invalidated: e.invalidated,
                xact_depth: e.xact_depth,
                be_pid: e.conn.as_ref().map(|c| c.backend_pid()).unwrap_or(0),
            })
            .collect()
    });
    for row in rows {
        let server_name = server_name_missing_ok(row.serverid)?;
        let mut values = vec![Datum::null(); expected as usize];
        let mut nulls = vec![true; expected as usize];
        if let Some(n) = &server_name {
            values[0] = types_fmgr::varlena_result(varlena::cstring_to_text(mcx, n.as_bytes())?);
            nulls[0] = false;
        }
        if v1_2 {
            if let Some(u) = mapping_user_name(row.umid)? {
                values[1] =
                    types_fmgr::varlena_result(varlena::cstring_to_text(mcx, u.as_bytes())?);
                nulls[1] = false;
            }
            values[2] = Datum::from_bool(!row.invalidated);
            nulls[2] = false;
            values[3] = Datum::from_bool(row.xact_depth > 0);
            nulls[3] = false;
            // closed: C answers only when check_conn && pgfdw_conn_checkable()
            // (POLLRDHUP); the probe is unported — always NULL (divergence:
            // check_conn=true would answer t/f on C/Linux).
            values[5] = Datum::from_i32(row.be_pid);
            nulls[5] = false;
        } else {
            values[1] = Datum::from_bool(!row.invalidated);
            nulls[1] = false;
        }
        srf.putvalues(&values, &nulls)?;
    }
    // SAFETY: fcinfo_ptr is the same borrow InitMaterializedSRF released.
    Ok(srf.finish(unsafe { &mut *fcinfo_ptr }))
}

pub(crate) fn fc_postgres_fdw_get_connections(
    flinfo: Option<&mut types_fmgr::FmgrInfo>,
    fcinfo: &mut types_fmgr::FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    get_connections_internal(flinfo.expect("srf needs flinfo"), fcinfo, false)
}

pub(crate) fn fc_postgres_fdw_get_connections_1_2(
    flinfo: Option<&mut types_fmgr::FmgrInfo>,
    fcinfo: &mut types_fmgr::FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    get_connections_internal(flinfo.expect("srf needs flinfo"), fcinfo, true)
}

// disconnect_cached_connections: close idle cached connections (to one
// server, or all); in-use connections get C's WARNINGs and stay.
fn disconnect_cached_connections(server: Option<Oid>) -> PgResult<bool> {
    // (key decisions under the borrow; warnings + name lookups after.)
    let mut closed_any = false;
    let mut in_use: Vec<Oid> = Vec::new(); // serverids to warn about
    let mut in_use_dropped = 0usize;
    CONNECTIONS.with(|c| {
        for entry in c.borrow_mut().values_mut() {
            if entry.conn.is_none() {
                continue;
            }
            if let Some(sid) = server {
                if entry.serverid != sid {
                    continue;
                }
            }
            if entry.xact_depth > 0 {
                in_use.push(entry.serverid);
            } else {
                disconnect_entry(entry);
                closed_any = true;
            }
        }
    });
    for sid in in_use {
        match server_name_missing_ok(sid)? {
            Some(name) => {
                let _ = ereport(WARNING)
                    .errmsg(format!(
                        "cannot close connection for server \"{name}\" because it is still in use"
                    ))
                    .finish(loc("disconnect_cached_connections"));
            }
            None => in_use_dropped += 1,
        }
    }
    for _ in 0..in_use_dropped {
        let _ = ereport(WARNING)
            .errmsg("cannot close dropped server connection because it is still in use")
            .finish(loc("disconnect_cached_connections"));
    }
    Ok(closed_any)
}

pub(crate) fn fc_postgres_fdw_disconnect(
    _flinfo: Option<&mut types_fmgr::FmgrInfo>,
    fcinfo: &mut types_fmgr::FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    // SAFETY: strict text arg.
    let name = {
        let v = unsafe { fcinfo.arg_varlena_packed(0)? };
        String::from_utf8_lossy(v.data()).into_owned()
    };
    let serverid = foreigncmds::foreign::get_foreign_server_oid(&name, false)?;
    Ok(Datum::from_bool(disconnect_cached_connections(Some(serverid))?))
}

pub(crate) fn fc_postgres_fdw_disconnect_all(
    _flinfo: Option<&mut types_fmgr::FmgrInfo>,
    fcinfo: &mut types_fmgr::FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let _ = fcinfo;
    Ok(Datum::from_bool(disconnect_cached_connections(None)?))
}

enum CleanupResult {
    TimedOut,
    Failed,
    Done(Option<QueryResult>),
}

// pgfdw_get_cleanup_result: pump for the last result with a hard deadline;
// re-issue cancel requests with exponential backoff while waiting (the
// remote may have been idle when the first cancel arrived).
fn pgfdw_get_cleanup_result(
    conn: &mut PgConn,
    endtime: Instant,
    mut retrycanceltime: Instant,
) -> PgResult<CleanupResult> {
    let mut canceldelta = Duration::from_millis((RETRY_CANCEL_TIMEOUT_MS * 2) as u64);
    let mut last: Option<QueryResult> = None;
    loop {
        while conn.is_busy() {
            let now = Instant::now();
            if now >= endtime {
                return Ok(CleanupResult::TimedOut);
            }
            if now >= retrycanceltime {
                let left = endtime.saturating_duration_since(now).as_millis() as i64;
                let _ = conn.cancel(left.max(1))?;
                retrycanceltime = Instant::now() + canceldelta;
                canceldelta *= 2;
            }
            let wait_until = endtime.min(retrycanceltime);
            let ms = wait_until
                .saturating_duration_since(Instant::now())
                .as_millis()
                .min(i64::MAX as u128) as i64;
            if ms <= 0 {
                continue;
            }
            if !conn.wait_readable_timeout(ms)? {
                continue; // timeout: loop re-checks the deadlines
            }
            if !conn.consume_input() {
                return Ok(CleanupResult::Failed);
            }
        }
        // Not busy: a whole result (or ReadyForQuery) is buffered.
        match conn.get_result()? {
            None => break,
            Some(r) => last = Some(r),
        }
    }
    Ok(CleanupResult::Done(last))
}
