//! `replication/libpqwalreceiver/libpqwalreceiver.c` — the libpq-specific parts
//! of walreceiver.
//!
//! In C this is a dynamically-loaded module that installs the
//! `WalReceiverFunctionsType` vtable into the global `WalReceiverFunctions`
//! slot, so the rest of the backend can call libpq exclusively through that
//! vtable (avoiding linking the server binary against libpq).  Apart from
//! walreceiver, these routines are also used by logical-replication workers and
//! slot synchronization.
//!
//! Everything this module reaches OUTSIDE itself is a libpq *client* call (the
//! `PQ*` / `libpqsrv_*` surface) or one of a few backend leaves (encoding name,
//! `pg_lsn_in`, `quote_identifier`, tuplestore / tuple-descriptor / memory
//! context machinery, `MyDatabaseId` / `work_mem` / `CHECK_FOR_INTERRUPTS`).
//! There is no in-process libpq client in this tree, so the entire transport
//! surface routes through [`fe_seams`] (aliased `rt`), each
//! seam a loud panic until a provider lands.  This module's OWN logic — the
//! conninfo key/val assembly, the command builders, every result-status
//! dispatch and error-path selection, the COPY byte framing, the
//! [`WalRcvExecResult`] population — is ported 1:1 with C.
//!
//! Across the inward [`libpqwalreceiver_seams`] boundary the
//! `WalReceiverConn` / `WalRcvExecResult` / `WalRcvResultTupslot` objects are
//! opaque integer handles (`types_walreceiver::*`); the live owned objects are
//! parked in this crate's registries ([`conn_registry`]) and resolved by the
//! vtable wrappers in [`walrcv_table`].

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(clippy::result_large_err)]

use ::utils_error::ereport;
use ::types_error::{
    make_sqlstate, ErrorLocation, PgError, PgResult, SqlState, ERROR, ERRCODE_CONNECTION_FAILURE,
    ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERRCODE_OUT_OF_MEMORY, ERRCODE_PROTOCOL_VIOLATION,
    ERRCODE_S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED, ERRCODE_SYNTAX_ERROR,
};

use ::types_core::{InvalidOid, Oid, TimeLineID, XLogRecPtr};
use ::types_libpqwalreceiver::{
    ConnStatusType, ExecStatusType, PgConnId, PgResultId, Pgsocket, TupleDescId, TuplestoreId,
};

use fe_seams as rt;

pub mod conn_registry;
pub mod walrcv_table;

// ===========================================================================
// Constants (from generated PostgreSQL headers).
// ===========================================================================

/// `ALWAYS_SECURE_SEARCH_PATH_SQL` (common/connect.h).
pub const ALWAYS_SECURE_SEARCH_PATH_SQL: &str =
    "SELECT pg_catalog.set_config('search_path', '', false);";

/// `MaxTupleAttributeNumber` (access/htup_details.h).
pub const MaxTupleAttributeNumber: usize = 1664;

/// `WAIT_EVENT_LIBPQWALRECEIVER_CONNECT` — CLIENT-class wait event; alphabetical
/// id 3 within the `WaitEventClient` section of `wait_event_names.txt`.
pub const WAIT_EVENT_LIBPQWALRECEIVER_CONNECT: u32 = types_pgstat::wait_event::PG_WAIT_CLIENT | 3;

/// `WAIT_EVENT_LIBPQWALRECEIVER_RECEIVE` — CLIENT-class wait event; alphabetical
/// id 4 within the `WaitEventClient` section.
pub const WAIT_EVENT_LIBPQWALRECEIVER_RECEIVE: u32 = types_pgstat::wait_event::PG_WAIT_CLIENT | 4;

// ===========================================================================
// Shared subsystem enums (replication/walsender.h, replication/walreceiver.h).
// ===========================================================================

/// `CRSSnapshotAction` (replication/walsender.h).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum CRSSnapshotAction {
    CRS_EXPORT_SNAPSHOT,
    CRS_NOEXPORT_SNAPSHOT,
    CRS_USE_SNAPSHOT,
}

/// `WalRcvExecStatus` (replication/walreceiver.h).  Mirrors
/// [`::types_walreceiver::WalRcvExecStatus`]; kept locally too for the owned
/// [`WalRcvExecResult`] struct.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum WalRcvExecStatus {
    WALRCV_ERROR,
    WALRCV_OK_COMMAND,
    WALRCV_OK_TUPLES,
    WALRCV_OK_COPY_IN,
    WALRCV_OK_COPY_OUT,
    WALRCV_OK_COPY_BOTH,
}

impl WalRcvExecStatus {
    /// Project onto the seam-boundary [`::types_walreceiver::WalRcvExecStatus`].
    pub fn to_types(self) -> ::types_walreceiver::WalRcvExecStatus {
        use ::types_walreceiver::WalRcvExecStatus as T;
        match self {
            WalRcvExecStatus::WALRCV_ERROR => T::WALRCV_ERROR,
            WalRcvExecStatus::WALRCV_OK_COMMAND => T::WALRCV_OK_COMMAND,
            WalRcvExecStatus::WALRCV_OK_TUPLES => T::WALRCV_OK_TUPLES,
            WalRcvExecStatus::WALRCV_OK_COPY_IN => T::WALRCV_OK_COPY_IN,
            WalRcvExecStatus::WALRCV_OK_COPY_OUT => T::WALRCV_OK_COPY_OUT,
            WalRcvExecStatus::WALRCV_OK_COPY_BOTH => T::WALRCV_OK_COPY_BOTH,
        }
    }
}

/// Logical-replication arm of `WalRcvStreamOptions.proto`
/// (replication/walreceiver.h).
#[derive(Clone, Debug, Default)]
pub struct WalRcvStreamOptionsLogical {
    /// `uint32 proto_version` — logical protocol version.
    pub proto_version: u32,
    /// `List *publication_names` — string list of publications.
    pub publication_names: Vec<String>,
    /// `bool binary` — ask the publisher to use binary.
    pub binary: bool,
    /// `char *streaming_str` — streaming of large transactions.
    pub streaming_str: Option<String>,
    /// `bool twophase` — streaming of two-phase transactions at prepare time.
    pub twophase: bool,
    /// `char *origin` — only publish data originating from the given origin.
    pub origin: Option<String>,
}

/// Physical-replication arm of `WalRcvStreamOptions.proto`.
#[derive(Clone, Debug, Default)]
pub struct WalRcvStreamOptionsPhysical {
    /// `TimeLineID startpointTLI` — starting timeline.
    pub startpointTLI: TimeLineID,
}

/// The `proto` union of [`WalRcvStreamOptions`]. C uses a `union`; the active
/// arm is selected by `WalRcvStreamOptions::logical`.
#[derive(Clone, Debug)]
pub enum WalRcvStreamOptionsProto {
    Physical(WalRcvStreamOptionsPhysical),
    Logical(WalRcvStreamOptionsLogical),
}

/// `WalRcvStreamOptions` (replication/walreceiver.h) — the full physical/logical
/// form used internally.  The inward seam carries the physical-only
/// [`::types_walreceiver::WalRcvStreamOptions`]; [`walrcv_table`] adapts it.
#[derive(Clone, Debug)]
pub struct WalRcvStreamOptions {
    /// `bool logical` — true if logical replication, false if physical.
    pub logical: bool,
    /// `char *slotname` — name of the replication slot or `None`.
    pub slotname: Option<String>,
    /// `XLogRecPtr startpoint` — LSN of the starting point.
    pub startpoint: XLogRecPtr,
    /// `union { ... } proto` — physical/logical protocol options.
    pub proto: WalRcvStreamOptionsProto,
}

/// `WalRcvExecResult` (replication/walreceiver.h) — the live, owned result of a
/// `walrcv_exec` query.  Parked in the result registry; the seam boundary sees
/// the opaque [`::types_walreceiver::WalRcvExecResult`] handle.
///
/// `tuplestore` / `tupledesc` are external handles owned by the tuplestore and
/// tuple-descriptor subsystems; they are populated through the seam by
/// [`libpqrcv_processTuples`].
#[derive(Clone, Debug)]
pub struct WalRcvExecResult {
    /// `WalRcvExecStatus status`.
    pub status: WalRcvExecStatus,
    /// `int sqlstate`.
    pub sqlstate: i32,
    /// `char *err`.
    pub err: Option<String>,
    /// `Tuplestorestate *tuplestore` — opaque handle (0 == none).
    pub tuplestore: TuplestoreId,
    /// `TupleDesc tupledesc` — opaque handle (0 == none).
    pub tupledesc: TupleDescId,
}

impl WalRcvExecResult {
    /// `palloc0(sizeof(WalRcvExecResult))` — zeroed result struct.
    fn palloc0() -> WalRcvExecResult {
        WalRcvExecResult {
            status: WalRcvExecStatus::WALRCV_ERROR,
            sqlstate: 0,
            err: None,
            tuplestore: 0,
            tupledesc: 0,
        }
    }
}

// ===========================================================================
// WalReceiverConn — the per-connection state owned by THIS file.
// ===========================================================================

/// `struct WalReceiverConn` (libpqwalreceiver.c:45-53). Owned by this module;
/// the rest of the backend treats it as an opaque handle.
#[derive(Clone, Debug)]
pub struct WalReceiverConn {
    /// `PGconn *streamConn` — current connection to the primary (0 == none).
    pub streamConn: PgConnId,
    /// `bool logical` — remember if the connection is logical or physical.
    pub logical: bool,
    /// `char *recvBuf` — buffer for currently read records. In C this is a
    /// libpq-owned pointer (freed with `PQfreemem`); here it is an owned byte
    /// buffer.
    pub recvBuf: Vec<u8>,
}

/// Result of [`libpqrcv_connect`]: either the new connection, or — on a normal
/// failure — `None` with the palloc'd error message that C returns via `*err`.
#[derive(Debug)]
pub struct ConnectResult {
    /// The new connection, or `None` on failure.
    pub conn: Option<WalReceiverConn>,
    /// The error message set into `*err` on failure (`None` on success).
    pub err: Option<String>,
}

// ===========================================================================
// Helpers.
// ===========================================================================

fn here(funcname: &'static str) -> ErrorLocation {
    ErrorLocation {
        filename: Some(
            "src/backend/replication/libpqwalreceiver/libpqwalreceiver.c".to_string(),
        ),
        lineno: 0,
        funcname: Some(funcname.to_string()),
    }
}

/// `pchomp(str)` (utils/mb/mbutils.c) — copy with trailing newline characters
/// removed.
fn pchomp(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut n = bytes.len();
    while n > 0 && bytes[n - 1] == b'\n' {
        n -= 1;
    }
    String::from_utf8_lossy(&bytes[..n]).into_owned()
}

/// `LSN_FORMAT_ARGS` + `"%X/%X"` (xlogdefs.h) — uppercase-hex high/low halves.
fn lsn_format(lsn: XLogRecPtr) -> String {
    format!("{:X}/{:X}", (lsn >> 32) as u32, lsn as u32)
}

/// Decode libpq-returned (textual) field bytes into an owned `String`, mirroring
/// `pstrdup(PQgetvalue(...))` for the text path.
fn bytes_to_string(bytes: Vec<u8>) -> String {
    String::from_utf8_lossy(&bytes).into_owned()
}

/// C `atoi` semantics: parse a leading optional-sign decimal integer, ignoring
/// trailing non-digits, returning 0 on no leading digits.
fn atoi(s: &str) -> i32 {
    let bytes = s.as_bytes();
    let mut idx = 0;
    while idx < bytes.len() && (bytes[idx] == b' ' || bytes[idx] == b'\t') {
        idx += 1;
    }
    let mut sign: i64 = 1;
    if idx < bytes.len() && (bytes[idx] == b'+' || bytes[idx] == b'-') {
        if bytes[idx] == b'-' {
            sign = -1;
        }
        idx += 1;
    }
    let mut acc: i64 = 0;
    while idx < bytes.len() && bytes[idx].is_ascii_digit() {
        acc = acc * 10 + (bytes[idx] - b'0') as i64;
        idx += 1;
    }
    (sign * acc) as i32
}

// ===========================================================================
// _PG_init (libpqwalreceiver.c:124-130).
// ===========================================================================

use std::sync::atomic::{AtomicBool, Ordering};

/// Tracks whether `WalReceiverFunctions` has been set, standing in for the C
/// global `WalReceiverFunctionsType *WalReceiverFunctions` (initially NULL).
static WAL_RECEIVER_FUNCTIONS_LOADED: AtomicBool = AtomicBool::new(false);

/// `WalReceiverFunctionsType` (replication/walreceiver.h) — the set of
/// libpqwalreceiver hooks installed into `WalReceiverFunctions`. In C this is a
/// function-pointer struct; here the `libpqrcv_*` free functions ARE the
/// entries (installed as the inward seams), so the marker carries no fields.
pub struct WalReceiverFunctionsType;

/// `_PG_init` (libpqwalreceiver.c:124-130) — module initialization. Errors if
/// the module is loaded twice; otherwise marks the vtable installed.
pub fn _PG_init() -> PgResult<()> {
    if WAL_RECEIVER_FUNCTIONS_LOADED.load(Ordering::SeqCst) {
        // elog(ERROR, "libpqwalreceiver already loaded")
        ereport(ERROR)
            .errmsg_internal("libpqwalreceiver already loaded")
            .finish(here("_PG_init"))?;
    }
    WAL_RECEIVER_FUNCTIONS_LOADED.store(true, Ordering::SeqCst);
    Ok(())
}

// ===========================================================================
// libpqrcv_connect (libpqwalreceiver.c:147-282).
// ===========================================================================

/// `libpqrcv_connect` (libpqwalreceiver.c:147-282) — establish the connection to
/// the primary server. Returns the new connection, or — on a normal failure —
/// `None` with the error message C sets via `*err`. `ereport(ERROR)` when
/// `must_use_password` is true but no password was used.
pub fn libpqrcv_connect(
    conninfo: &str,
    replication: bool,
    logical: bool,
    must_use_password: bool,
    appname: Option<&str>,
) -> PgResult<ConnectResult> {
    // const char *keys[6]; const char *vals[6];
    let mut keys: Vec<String> = Vec::new();
    let mut vals: Vec<Option<String>> = Vec::new();

    // Re-validate connection string.
    libpqrcv_check_conninfo(conninfo, must_use_password)?;

    // expand_dbname processing: keys[i] = "dbname"; vals[i] = conninfo;
    keys.push("dbname".to_string());
    vals.push(Some(conninfo.to_string()));

    // Assert(replication || !logical);
    debug_assert!(replication || !logical);

    if replication {
        keys.push("replication".to_string());
        vals.push(Some(
            if logical { "database" } else { "true" }.to_string(),
        ));

        if logical {
            // Tell the publisher to translate to our encoding.
            keys.push("client_encoding".to_string());
            vals.push(Some(rt::get_database_encoding_name::call()));

            // Force assorted GUC parameters (must match pg_dump).
            let opt = libpqrcv_get_option_from_conninfo(conninfo, "options")?;
            let options_val = format!(
                "{} -c datestyle=ISO -c intervalstyle=postgres -c extra_float_digits=3",
                opt.as_deref().unwrap_or("")
            );
            keys.push("options".to_string());
            vals.push(Some(options_val));
            // (opt dropped here, mirroring pfree(opt) when opt != NULL.)
            drop(opt);
        } else {
            // The database name is ignored by the server in replication mode,
            // but specify "replication" for .pgpass lookup.
            keys.push("dbname".to_string());
            vals.push(Some("replication".to_string()));
        }
    }

    keys.push("fallback_application_name".to_string());
    vals.push(appname.map(|s| s.to_string()));

    // keys[++i] = NULL; vals[i] = NULL; — terminator implicit in Vec length.
    // Assert(i < lengthof(keys));
    debug_assert!(keys.len() <= 6);

    let mut conn = WalReceiverConn {
        streamConn: rt::libpqsrv_connect_params::call(
            keys,
            vals,
            /* expand_dbname = */ true,
            WAIT_EVENT_LIBPQWALRECEIVER_CONNECT,
        ),
        logical: false,
        recvBuf: Vec::new(),
    };

    // pfree(options_val) — owned copy already dropped.

    if rt::pq_status::call(conn.streamConn) != ConnStatusType::CONNECTION_OK {
        // bad_connection_errmsg: *err = pchomp(PQerrorMessage(conn->streamConn));
        let msg = rt::pq_error_message::call(conn.streamConn);
        let err = Some(pchomp(&msg));
        // bad_connection (fall-through): libpqsrv_disconnect(...); pfree(conn);
        rt::libpqsrv_disconnect::call(conn.streamConn);
        return Ok(ConnectResult { conn: None, err });
    }

    if must_use_password && !rt::pq_connection_used_password::call(conn.streamConn) {
        rt::libpqsrv_disconnect::call(conn.streamConn);
        // pfree(conn) is implicit (conn dropped on early return).

        ereport(ERROR)
            .errcode(ERRCODE_S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED)
            .errmsg("password is required")
            .errdetail("Non-superuser cannot connect if the server does not request a password.")
            .errhint("Target server's authentication method must be changed, or set password_required=false in the subscription parameters.")
            .finish(here("libpqrcv_connect"))?;
    }

    // Set always-secure search path for SQL-query connections.
    if !replication || logical {
        let res = rt::libpqsrv_exec::call(
            conn.streamConn,
            ALWAYS_SECURE_SEARCH_PATH_SQL.to_string(),
            WAIT_EVENT_LIBPQWALRECEIVER_RECEIVE,
        );
        if rt::pq_result_status::call(res) != ExecStatusType::PGRES_TUPLES_OK {
            rt::pq_clear::call(res);
            // _("could not clear search path: %s")
            let msg = rt::pq_error_message::call(conn.streamConn);
            let err = Some(format!("could not clear search path: {}", pchomp(&msg)));
            // bad_connection
            rt::libpqsrv_disconnect::call(conn.streamConn);
            return Ok(ConnectResult { conn: None, err });
        }
        rt::pq_clear::call(res);
    }

    conn.logical = logical;

    Ok(ConnectResult {
        conn: Some(conn),
        err: None,
    })
}

// ===========================================================================
// libpqrcv_check_conninfo (libpqwalreceiver.c:292-341).
// ===========================================================================

/// `libpqrcv_check_conninfo` (libpqwalreceiver.c:292-341) — validate the
/// connection-info string, optionally requiring a password.
pub fn libpqrcv_check_conninfo(conninfo: &str, must_use_password: bool) -> PgResult<()> {
    let opts = match rt::pq_conninfo_parse::call(conninfo.to_string()) {
        Ok(opts) => opts,
        Err(err) => {
            // The error string is malloc'd, so we must free it explicitly.
            let errcopy = err.unwrap_or_else(|| "out of memory".to_string());
            ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg(format!("invalid connection string syntax: {errcopy}"))
                .finish(here("libpqrcv_check_conninfo"))?;
            unreachable!()
        }
    };

    if must_use_password {
        let mut uses_password = false;

        for opt in &opts {
            // Ignore connection options that are not present.
            let Some(val) = opt.val.as_deref() else {
                continue;
            };

            if opt.keyword == "password" && !val.is_empty() {
                uses_password = true;
                break;
            }
        }

        if !uses_password {
            // PQconninfoFree(opts) is implicit (opts dropped).
            ereport(ERROR)
                .errcode(ERRCODE_S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED)
                .errmsg("password is required")
                .errdetail("Non-superusers must provide a password in the connection string.")
                .finish(here("libpqrcv_check_conninfo"))?;
        }
    }

    // PQconninfoFree(opts) — opts dropped here.
    Ok(())
}

// ===========================================================================
// libpqrcv_get_conninfo (libpqwalreceiver.c:347-391).
// ===========================================================================

/// `libpqrcv_get_conninfo` (libpqwalreceiver.c:347-391) — build a
/// user-displayable conninfo string with security-sensitive fields obfuscated.
pub fn libpqrcv_get_conninfo(conn: &WalReceiverConn) -> PgResult<Option<String>> {
    debug_assert!(conn.streamConn != 0);

    // initPQExpBuffer(&buf) — a Rust String. The "broken" state can't occur for
    // a String, so retval is always Some(buf).
    let mut buf = String::new();

    let conn_opts = match rt::pq_conninfo::call(conn.streamConn) {
        Some(opts) => opts,
        None => {
            ereport(ERROR)
                .errcode(ERRCODE_OUT_OF_MEMORY)
                // errmsg("could not parse connection string: %s", _("out of memory"))
                .errmsg(format!(
                    "could not parse connection string: {}",
                    "out of memory"
                ))
                .finish(here("libpqrcv_get_conninfo"))?;
            unreachable!()
        }
    };

    // build a clean connection string from pieces
    for conn_opt in &conn_opts {
        // Skip debug and empty options.
        if conn_opt.dispchar.contains('D')
            || conn_opt.val.is_none()
            || conn_opt.val.as_deref() == Some("")
        {
            continue;
        }

        // Obfuscate security-sensitive options.
        let obfuscate = conn_opt.dispchar.contains('*');

        // appendPQExpBuffer(&buf, "%s%s=%s", buf.len == 0 ? "" : " ", keyword, val)
        let sep = if buf.is_empty() { "" } else { " " };
        let value = if obfuscate {
            "********"
        } else {
            conn_opt.val.as_deref().ok_or_else(|| {
                PgError::error("libpqrcv_get_conninfo: connection option has no value")
            })?
        };
        buf.push_str(sep);
        buf.push_str(&conn_opt.keyword);
        buf.push('=');
        buf.push_str(value);
    }

    // PQconninfoFree(conn_opts) — dropped here.
    Ok(Some(buf))
}

// ===========================================================================
// libpqrcv_get_senderinfo (libpqwalreceiver.c:396-414).
// ===========================================================================

/// `libpqrcv_get_senderinfo` (libpqwalreceiver.c:396-414) — provide info on the
/// sender this WAL receiver is connected to. Returns `(sender_host, sender_port)`
/// (the C `*sender_host` / `*sender_port` out-params).
pub fn libpqrcv_get_senderinfo(conn: &WalReceiverConn) -> (Option<String>, i32) {
    let mut sender_host: Option<String> = None;
    let mut sender_port: i32 = 0;

    debug_assert!(conn.streamConn != 0);

    let ret = rt::pq_host::call(conn.streamConn);
    if let Some(host) = ret {
        if !host.is_empty() {
            sender_host = Some(host);
        }
    }

    let ret = rt::pq_port::call(conn.streamConn);
    if let Some(port) = ret {
        if !port.is_empty() {
            // atoi(ret)
            sender_port = atoi(&port);
        }
    }

    (sender_host, sender_port)
}

// ===========================================================================
// libpqrcv_identify_system (libpqwalreceiver.c:420-464).
// ===========================================================================

/// `libpqrcv_identify_system` (libpqwalreceiver.c:420-464) — verify the
/// primary's system identifier and fetch its current timeline ID. Returns
/// `(primary_sysid, primary_tli)` (the C return + `*primary_tli` out-param).
pub fn libpqrcv_identify_system(conn: &WalReceiverConn) -> PgResult<(String, TimeLineID)> {
    let res = rt::libpqsrv_exec::call(
        conn.streamConn,
        "IDENTIFY_SYSTEM".to_string(),
        WAIT_EVENT_LIBPQWALRECEIVER_RECEIVE,
    );
    if rt::pq_result_status::call(res) != ExecStatusType::PGRES_TUPLES_OK {
        rt::pq_clear::call(res);
        let msg = rt::pq_error_message::call(conn.streamConn);
        ereport(ERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg(format!(
                "could not receive database system identifier and timeline ID from the primary server: {}",
                pchomp(&msg)
            ))
            .finish(here("libpqrcv_identify_system"))?;
    }

    // IDENTIFY_SYSTEM returns 3 columns in 9.3 and earlier, 4 in 9.4+.
    if rt::pq_nfields::call(res) < 3 || rt::pq_ntuples::call(res) != 1 {
        let ntuples = rt::pq_ntuples::call(res);
        let nfields = rt::pq_nfields::call(res);

        rt::pq_clear::call(res);
        ereport(ERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg("invalid response from primary server")
            .errdetail(format!(
                "Could not identify system: got {} rows and {} fields, expected {} rows and {} or more fields.",
                ntuples, nfields, 1, 3
            ))
            .finish(here("libpqrcv_identify_system"))?;
    }
    let primary_sysid = bytes_to_string(rt::pq_getvalue::call(res, 0, 0));
    let tli_str = bytes_to_string(rt::pq_getvalue::call(res, 0, 1));
    let primary_tli = rt::pg_strtoint32::call(tli_str)? as TimeLineID;
    rt::pq_clear::call(res);

    Ok((primary_sysid, primary_tli))
}

// ===========================================================================
// libpqrcv_server_version (libpqwalreceiver.c:469-473).
// ===========================================================================

/// `libpqrcv_server_version` (libpqwalreceiver.c:469-473) — thin wrapper to
/// obtain the server version.
pub fn libpqrcv_server_version(conn: &WalReceiverConn) -> i32 {
    rt::pq_server_version::call(conn.streamConn)
}

// ===========================================================================
// libpqrcv_get_dbname_from_conninfo (libpqwalreceiver.c:480-484).
// ===========================================================================

/// `libpqrcv_get_dbname_from_conninfo` (libpqwalreceiver.c:480-484) — get the
/// database name from the primary's conninfo, or `None`.
pub fn libpqrcv_get_dbname_from_conninfo(conn_info: &str) -> PgResult<Option<String>> {
    libpqrcv_get_option_from_conninfo(conn_info, "dbname")
}

// ===========================================================================
// libpqrcv_get_option_from_conninfo (libpqwalreceiver.c:492-529).
// ===========================================================================

/// `libpqrcv_get_option_from_conninfo` (libpqwalreceiver.c:492-529) — get the
/// value of the option with the given keyword from the conninfo, or `None`.
pub fn libpqrcv_get_option_from_conninfo(
    conn_info: &str,
    keyword: &str,
) -> PgResult<Option<String>> {
    let opts = match rt::pq_conninfo_parse::call(conn_info.to_string()) {
        Ok(opts) => opts,
        Err(err) => {
            let errcopy = err.unwrap_or_else(|| "out of memory".to_string());
            ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg(format!("invalid connection string syntax: {errcopy}"))
                .finish(here("libpqrcv_get_option_from_conninfo"))?;
            unreachable!()
        }
    };

    let mut option: Option<String> = None;

    for opt in &opts {
        // If the same option appears multiple times, the last one is returned.
        if opt.keyword == keyword {
            if let Some(val) = opt.val.as_deref() {
                if !val.is_empty() {
                    // pfree(option) if previously set is implicit (overwrite).
                    option = Some(val.to_string());
                }
            }
        }
    }

    // PQconninfoFree(opts) — dropped here.
    Ok(option)
}

// ===========================================================================
// libpqrcv_startstreaming (libpqwalreceiver.c:541-641).
// ===========================================================================

/// `libpqrcv_startstreaming` (libpqwalreceiver.c:541-641) — start streaming WAL
/// from the given options. Returns `true` if we switched to copy-both mode;
/// `false` if the server executed the command but did not switch to copy mode.
pub fn libpqrcv_startstreaming(
    conn: &WalReceiverConn,
    options: &WalRcvStreamOptions,
) -> PgResult<bool> {
    debug_assert!(options.logical == conn.logical);
    debug_assert!(options.slotname.is_some() || !options.logical);

    // initStringInfo(&cmd) — a Rust String.
    let mut cmd = String::new();

    // Build the command.
    cmd.push_str("START_REPLICATION");
    if let Some(slotname) = options.slotname.as_deref() {
        cmd.push_str(&format!(" SLOT \"{slotname}\""));
    }

    if options.logical {
        cmd.push_str(" LOGICAL");
    }

    cmd.push_str(&format!(" {}", lsn_format(options.startpoint)));

    // Options differ for logical vs physical.
    if options.logical {
        let lo = match &options.proto {
            WalRcvStreamOptionsProto::Logical(lo) => lo,
            WalRcvStreamOptionsProto::Physical(_) => unreachable_logical_proto()?,
        };

        cmd.push_str(" (");

        cmd.push_str(&format!("proto_version '{}'", lo.proto_version));

        if let Some(streaming_str) = lo.streaming_str.as_deref() {
            cmd.push_str(&format!(", streaming '{streaming_str}'"));
        }

        if lo.twophase && rt::pq_server_version::call(conn.streamConn) >= 150000 {
            cmd.push_str(", two_phase 'on'");
        }

        if let Some(origin) = lo.origin.as_deref() {
            if rt::pq_server_version::call(conn.streamConn) >= 160000 {
                cmd.push_str(&format!(", origin '{origin}'"));
            }
        }

        let pubnames = &lo.publication_names;
        let pubnames_str = stringlist_to_identifierstr(conn.streamConn, pubnames);
        let pubnames_str = match pubnames_str {
            Some(s) => s,
            None => {
                let msg = rt::pq_error_message::call(conn.streamConn);
                ereport(ERROR)
                    .errcode(ERRCODE_OUT_OF_MEMORY) // likely guess
                    .errmsg(format!("could not start WAL streaming: {}", pchomp(&msg)))
                    .finish(here("libpqrcv_startstreaming"))?;
                unreachable!()
            }
        };
        let pubnames_literal = rt::pq_escape_literal::call(conn.streamConn, pubnames_str);
        let pubnames_literal = match pubnames_literal {
            Some(s) => s,
            None => {
                let msg = rt::pq_error_message::call(conn.streamConn);
                ereport(ERROR)
                    .errcode(ERRCODE_OUT_OF_MEMORY) // likely guess
                    .errmsg(format!("could not start WAL streaming: {}", pchomp(&msg)))
                    .finish(here("libpqrcv_startstreaming"))?;
                unreachable!()
            }
        };
        cmd.push_str(&format!(", publication_names {pubnames_literal}"));
        // PQfreemem(pubnames_literal); pfree(pubnames_str); — owned copies drop.

        if lo.binary && rt::pq_server_version::call(conn.streamConn) >= 140000 {
            cmd.push_str(", binary 'true'");
        }

        cmd.push(')');
    } else {
        let po = match &options.proto {
            WalRcvStreamOptionsProto::Physical(po) => po,
            WalRcvStreamOptionsProto::Logical(_) => unreachable_logical_proto()?,
        };
        cmd.push_str(&format!(" TIMELINE {}", po.startpointTLI));
    }

    // Start streaming.
    let res = rt::libpqsrv_exec::call(conn.streamConn, cmd, WAIT_EVENT_LIBPQWALRECEIVER_RECEIVE);
    // pfree(cmd.data) — cmd consumed by the call.

    let status = rt::pq_result_status::call(res);
    if status == ExecStatusType::PGRES_COMMAND_OK {
        rt::pq_clear::call(res);
        return Ok(false);
    } else if status != ExecStatusType::PGRES_COPY_BOTH {
        rt::pq_clear::call(res);
        let msg = rt::pq_error_message::call(conn.streamConn);
        ereport(ERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg(format!("could not start WAL streaming: {}", pchomp(&msg)))
            .finish(here("libpqrcv_startstreaming"))?;
    }
    rt::pq_clear::call(res);
    Ok(true)
}

/// Mirror of C accessing the logical union arm when `logical` is set; in C the
/// union access is unconditional, so a mismatched arm is a programming error.
fn unreachable_logical_proto<T>() -> PgResult<T> {
    ereport(ERROR)
        .errmsg_internal("WalRcvStreamOptions proto arm does not match options.logical")
        .finish(here("libpqrcv_startstreaming"))?;
    unreachable!()
}

// ===========================================================================
// libpqrcv_endstreaming (libpqwalreceiver.c:647-723).
// ===========================================================================

/// `libpqrcv_endstreaming` (libpqwalreceiver.c:647-723) — stop streaming WAL.
/// Returns the next timeline's ID (the C `*next_tli` out-param), or 0 if the
/// server did not report it.
pub fn libpqrcv_endstreaming(conn: &WalReceiverConn) -> PgResult<TimeLineID> {
    // Send copy-end message.
    if rt::pq_put_copy_end::call(conn.streamConn) <= 0 || rt::pq_flush::call(conn.streamConn) != 0 {
        let msg = rt::pq_error_message::call(conn.streamConn);
        ereport(ERROR)
            .errcode(ERRCODE_CONNECTION_FAILURE)
            .errmsg(format!(
                "could not send end-of-streaming message to primary: {}",
                pchomp(&msg)
            ))
            .finish(here("libpqrcv_endstreaming"))?;
    }

    let mut next_tli: TimeLineID = 0;

    let mut res =
        rt::libpqsrv_get_result::call(conn.streamConn, WAIT_EVENT_LIBPQWALRECEIVER_RECEIVE);
    let status = rt::pq_result_status::call(res);
    if status == ExecStatusType::PGRES_TUPLES_OK {
        // Read the next timeline's ID; the starting point is ignored.
        if rt::pq_nfields::call(res) < 2 || rt::pq_ntuples::call(res) != 1 {
            ereport(ERROR)
                .errcode(ERRCODE_PROTOCOL_VIOLATION)
                .errmsg("unexpected result set after end-of-streaming")
                .finish(here("libpqrcv_endstreaming"))?;
        }
        let tli_str = bytes_to_string(rt::pq_getvalue::call(res, 0, 0));
        next_tli = rt::pg_strtoint32::call(tli_str)? as TimeLineID;
        rt::pq_clear::call(res);

        // the result set should be followed by CommandComplete
        res = rt::libpqsrv_get_result::call(conn.streamConn, WAIT_EVENT_LIBPQWALRECEIVER_RECEIVE);
    } else if status == ExecStatusType::PGRES_COPY_OUT {
        rt::pq_clear::call(res);

        // End the copy.
        if rt::pq_endcopy::call(conn.streamConn) != 0 {
            let msg = rt::pq_error_message::call(conn.streamConn);
            ereport(ERROR)
                .errcode(ERRCODE_CONNECTION_FAILURE)
                .errmsg(format!(
                    "error while shutting down streaming COPY: {}",
                    pchomp(&msg)
                ))
                .finish(here("libpqrcv_endstreaming"))?;
        }

        // CommandComplete should follow.
        res = rt::libpqsrv_get_result::call(conn.streamConn, WAIT_EVENT_LIBPQWALRECEIVER_RECEIVE);
    }

    if rt::pq_result_status::call(res) != ExecStatusType::PGRES_COMMAND_OK {
        let msg = rt::pq_error_message::call(conn.streamConn);
        ereport(ERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg(format!(
                "error reading result of streaming command: {}",
                pchomp(&msg)
            ))
            .finish(here("libpqrcv_endstreaming"))?;
    }
    rt::pq_clear::call(res);

    // Verify that there are no more results.
    let res = rt::libpqsrv_get_result::call(conn.streamConn, WAIT_EVENT_LIBPQWALRECEIVER_RECEIVE);
    if res != 0 {
        let msg = rt::pq_error_message::call(conn.streamConn);
        ereport(ERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg(format!(
                "unexpected result after CommandComplete: {}",
                pchomp(&msg)
            ))
            .finish(here("libpqrcv_endstreaming"))?;
    }

    Ok(next_tli)
}

// ===========================================================================
// libpqrcv_readtimelinehistoryfile (libpqwalreceiver.c:728-772).
// ===========================================================================

/// `libpqrcv_readtimelinehistoryfile` (libpqwalreceiver.c:728-772) — fetch the
/// timeline history file for `tli` from the primary. Returns `(filename,
/// content)` (the C `*filename` / `*content` / `*len` out-params; `len` is
/// `content.len()`).
pub fn libpqrcv_readtimelinehistoryfile(
    conn: &WalReceiverConn,
    tli: TimeLineID,
) -> PgResult<(String, Vec<u8>)> {
    debug_assert!(!conn.logical);

    // snprintf(cmd, sizeof(cmd), "TIMELINE_HISTORY %u", tli)
    let cmd = format!("TIMELINE_HISTORY {tli}");
    let res = rt::libpqsrv_exec::call(conn.streamConn, cmd, WAIT_EVENT_LIBPQWALRECEIVER_RECEIVE);
    if rt::pq_result_status::call(res) != ExecStatusType::PGRES_TUPLES_OK {
        rt::pq_clear::call(res);
        let msg = rt::pq_error_message::call(conn.streamConn);
        ereport(ERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg(format!(
                "could not receive timeline history file from the primary server: {}",
                pchomp(&msg)
            ))
            .finish(here("libpqrcv_readtimelinehistoryfile"))?;
    }
    if rt::pq_nfields::call(res) != 2 || rt::pq_ntuples::call(res) != 1 {
        let ntuples = rt::pq_ntuples::call(res);
        let nfields = rt::pq_nfields::call(res);

        rt::pq_clear::call(res);
        ereport(ERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg("invalid response from primary server")
            .errdetail(format!(
                "Expected 1 tuple with 2 fields, got {ntuples} tuples with {nfields} fields."
            ))
            .finish(here("libpqrcv_readtimelinehistoryfile"))?;
    }
    let filename = bytes_to_string(rt::pq_getvalue::call(res, 0, 0));

    // *len = PQgetlength(res, 0, 1); *content = palloc(*len);
    // memcpy(*content, PQgetvalue(res, 0, 1), *len);
    let len = rt::pq_getlength::call(res, 0, 1);
    let mut content = rt::pq_getvalue::call(res, 0, 1);
    content.truncate(len as usize);
    rt::pq_clear::call(res);

    Ok((filename, content))
}

// ===========================================================================
// libpqrcv_disconnect (libpqwalreceiver.c:777-783).
// ===========================================================================

/// `libpqrcv_disconnect` (libpqwalreceiver.c:777-783) — disconnect from the
/// primary, if connected, and free the connection state.
pub fn libpqrcv_disconnect(conn: WalReceiverConn) {
    rt::libpqsrv_disconnect::call(conn.streamConn);
    // PQfreemem(conn->recvBuf) — recvBuf owned; dropped with conn.
    // pfree(conn) — conn dropped here.
    drop(conn);
}

// ===========================================================================
// libpqrcv_receive (libpqwalreceiver.c:801-886).
// ===========================================================================

/// `libpqrcv_receive` (libpqwalreceiver.c:801-886) — receive a message from the
/// XLOG stream. Returns the data length and:
///   * `len > 0` — the received bytes are in `conn.recvBuf`;
///   * `len == 0` — no data yet; the socket to wait on is the `Pgsocket`;
///   * `len == -1` — the server ended the COPY.
/// ereports on error.
pub fn libpqrcv_receive(conn: &mut WalReceiverConn) -> PgResult<(i32, Pgsocket)> {
    // PQfreemem(conn->recvBuf); conn->recvBuf = NULL;
    conn.recvBuf = Vec::new();

    // Try to receive a CopyData message.
    let (mut rawlen, recv_buf) = rt::pq_get_copy_data::call(conn.streamConn);
    conn.recvBuf = recv_buf;
    if rawlen == 0 {
        // Try consuming some data.
        if rt::pq_consume_input::call(conn.streamConn) == 0 {
            let msg = rt::pq_error_message::call(conn.streamConn);
            ereport(ERROR)
                .errcode(ERRCODE_CONNECTION_FAILURE)
                .errmsg(format!(
                    "could not receive data from WAL stream: {}",
                    pchomp(&msg)
                ))
                .finish(here("libpqrcv_receive"))?;
        }

        // Now that we've consumed some input, try again.
        let (again, recv_buf) = rt::pq_get_copy_data::call(conn.streamConn);
        rawlen = again;
        conn.recvBuf = recv_buf;
        if rawlen == 0 {
            // Tell caller to try again when our socket is ready.
            let wait_fd = rt::pq_socket::call(conn.streamConn);
            return Ok((0, wait_fd));
        }
    }
    if rawlen == -1 {
        // end-of-streaming or error
        let res =
            rt::libpqsrv_get_result::call(conn.streamConn, WAIT_EVENT_LIBPQWALRECEIVER_RECEIVE);
        let status = rt::pq_result_status::call(res);
        if status == ExecStatusType::PGRES_COMMAND_OK {
            rt::pq_clear::call(res);

            // Verify that there are no more results.
            let res =
                rt::libpqsrv_get_result::call(conn.streamConn, WAIT_EVENT_LIBPQWALRECEIVER_RECEIVE);
            if res != 0 {
                rt::pq_clear::call(res);

                // Orderly close: don't report an error, let callers deal.
                if rt::pq_status::call(conn.streamConn) == ConnStatusType::CONNECTION_BAD {
                    return Ok((-1, 0));
                }

                let msg = rt::pq_error_message::call(conn.streamConn);
                ereport(ERROR)
                    .errcode(ERRCODE_PROTOCOL_VIOLATION)
                    .errmsg(format!("unexpected result after CommandComplete: {msg}"))
                    .finish(here("libpqrcv_receive"))?;
            }

            return Ok((-1, 0));
        } else if status == ExecStatusType::PGRES_COPY_IN {
            rt::pq_clear::call(res);
            return Ok((-1, 0));
        } else {
            rt::pq_clear::call(res);
            let msg = rt::pq_error_message::call(conn.streamConn);
            ereport(ERROR)
                .errcode(ERRCODE_PROTOCOL_VIOLATION)
                .errmsg(format!(
                    "could not receive data from WAL stream: {}",
                    pchomp(&msg)
                ))
                .finish(here("libpqrcv_receive"))?;
        }
    }
    if rawlen < -1 {
        let msg = rt::pq_error_message::call(conn.streamConn);
        ereport(ERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg(format!(
                "could not receive data from WAL stream: {}",
                pchomp(&msg)
            ))
            .finish(here("libpqrcv_receive"))?;
    }

    // Return received messages to caller (*buffer = conn->recvBuf).
    Ok((rawlen, 0))
}

// ===========================================================================
// libpqrcv_send (libpqwalreceiver.c:893-902).
// ===========================================================================

/// `libpqrcv_send` (libpqwalreceiver.c:893-902) — send a message to the XLOG
/// stream. ereports on error.
pub fn libpqrcv_send(conn: &WalReceiverConn, buffer: &[u8]) -> PgResult<()> {
    if rt::pq_put_copy_data::call(conn.streamConn, buffer.to_vec()) <= 0
        || rt::pq_flush::call(conn.streamConn) != 0
    {
        let msg = rt::pq_error_message::call(conn.streamConn);
        ereport(ERROR)
            .errcode(ERRCODE_CONNECTION_FAILURE)
            .errmsg(format!("could not send data to WAL stream: {}", pchomp(&msg)))
            .finish(here("libpqrcv_send"))?;
    }
    Ok(())
}

// ===========================================================================
// libpqrcv_create_slot (libpqwalreceiver.c:909-1019).
// ===========================================================================

/// `libpqrcv_create_slot` (libpqwalreceiver.c:909-1019) — create a new
/// replication slot. Returns `(snapshot, lsn)`: the exported snapshot name for a
/// logical slot (`None` for a physical slot), and — when the caller passed a
/// non-NULL `*lsn` (`want_lsn`) — the slot's confirmed LSN.
pub fn libpqrcv_create_slot(
    conn: &WalReceiverConn,
    slotname: &str,
    temporary: bool,
    two_phase: bool,
    failover: bool,
    snapshot_action: CRSSnapshotAction,
    want_lsn: bool,
) -> PgResult<(Option<String>, Option<XLogRecPtr>)> {
    let use_new_options_syntax = rt::pq_server_version::call(conn.streamConn) >= 150000;

    // initStringInfo(&cmd) — Rust String.
    let mut cmd = String::new();

    cmd.push_str(&format!("CREATE_REPLICATION_SLOT \"{slotname}\""));

    if temporary {
        cmd.push_str(" TEMPORARY");
    }

    if conn.logical {
        cmd.push_str(" LOGICAL pgoutput ");
        if use_new_options_syntax {
            cmd.push('(');
        }
        if two_phase {
            cmd.push_str("TWO_PHASE");
            if use_new_options_syntax {
                cmd.push_str(", ");
            } else {
                cmd.push(' ');
            }
        }

        if failover {
            cmd.push_str("FAILOVER");
            if use_new_options_syntax {
                cmd.push_str(", ");
            } else {
                cmd.push(' ');
            }
        }

        if use_new_options_syntax {
            match snapshot_action {
                CRSSnapshotAction::CRS_EXPORT_SNAPSHOT => cmd.push_str("SNAPSHOT 'export'"),
                CRSSnapshotAction::CRS_NOEXPORT_SNAPSHOT => cmd.push_str("SNAPSHOT 'nothing'"),
                CRSSnapshotAction::CRS_USE_SNAPSHOT => cmd.push_str("SNAPSHOT 'use'"),
            }
        } else {
            match snapshot_action {
                CRSSnapshotAction::CRS_EXPORT_SNAPSHOT => cmd.push_str("EXPORT_SNAPSHOT"),
                CRSSnapshotAction::CRS_NOEXPORT_SNAPSHOT => cmd.push_str("NOEXPORT_SNAPSHOT"),
                CRSSnapshotAction::CRS_USE_SNAPSHOT => cmd.push_str("USE_SNAPSHOT"),
            }
        }

        if use_new_options_syntax {
            cmd.push(')');
        }
    } else if use_new_options_syntax {
        cmd.push_str(" PHYSICAL (RESERVE_WAL)");
    } else {
        cmd.push_str(" PHYSICAL RESERVE_WAL");
    }

    let res = rt::libpqsrv_exec::call(conn.streamConn, cmd, WAIT_EVENT_LIBPQWALRECEIVER_RECEIVE);
    // pfree(cmd.data) — cmd consumed by the call.

    if rt::pq_result_status::call(res) != ExecStatusType::PGRES_TUPLES_OK {
        rt::pq_clear::call(res);
        let msg = rt::pq_error_message::call(conn.streamConn);
        ereport(ERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg(format!(
                "could not create replication slot \"{}\": {}",
                slotname,
                pchomp(&msg)
            ))
            .finish(here("libpqrcv_create_slot"))?;
    }

    let lsn = if want_lsn {
        let value = rt::pq_getvalue::call(res, 0, 1);
        Some(rt::pg_lsn_in::call(value)?)
    } else {
        None
    };

    let snapshot = if !rt::pq_getisnull::call(res, 0, 2) {
        Some(bytes_to_string(rt::pq_getvalue::call(res, 0, 2)))
    } else {
        None
    };

    rt::pq_clear::call(res);

    Ok((snapshot, lsn))
}

// ===========================================================================
// libpqrcv_alter_slot (libpqwalreceiver.c:1024-1059).
// ===========================================================================

/// `libpqrcv_alter_slot` (libpqwalreceiver.c:1024-1059) — change the definition
/// of a replication slot.
pub fn libpqrcv_alter_slot(
    conn: &WalReceiverConn,
    slotname: &str,
    failover: Option<bool>,
    two_phase: Option<bool>,
) -> PgResult<()> {
    // initStringInfo(&cmd) — Rust String.
    let mut cmd = String::new();
    cmd.push_str(&format!(
        "ALTER_REPLICATION_SLOT {} ( ",
        rt::quote_identifier::call(slotname.to_string())
    ));

    if let Some(failover) = failover {
        cmd.push_str(&format!(
            "FAILOVER {}",
            if failover { "true" } else { "false" }
        ));
    }

    if failover.is_some() && two_phase.is_some() {
        cmd.push_str(", ");
    }

    if let Some(two_phase) = two_phase {
        cmd.push_str(&format!(
            "TWO_PHASE {}",
            if two_phase { "true" } else { "false" }
        ));
    }

    cmd.push_str(" );");

    let res = rt::libpqsrv_exec::call(conn.streamConn, cmd, WAIT_EVENT_LIBPQWALRECEIVER_RECEIVE);
    // pfree(cmd.data) — cmd consumed by the call.

    if rt::pq_result_status::call(res) != ExecStatusType::PGRES_COMMAND_OK {
        let msg = rt::pq_error_message::call(conn.streamConn);
        ereport(ERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg(format!(
                "could not alter replication slot \"{}\": {}",
                slotname,
                pchomp(&msg)
            ))
            .finish(here("libpqrcv_alter_slot"))?;
    }

    rt::pq_clear::call(res);
    Ok(())
}

// ===========================================================================
// libpqrcv_get_backend_pid (libpqwalreceiver.c:1064-1068).
// ===========================================================================

/// `libpqrcv_get_backend_pid` (libpqwalreceiver.c:1064-1068) — return the PID of
/// the remote backend process.
pub fn libpqrcv_get_backend_pid(conn: &WalReceiverConn) -> i32 {
    rt::pq_backend_pid::call(conn.streamConn)
}

// ===========================================================================
// libpqrcv_processTuples (libpqwalreceiver.c:1073-1142).
// ===========================================================================

/// `libpqrcv_processTuples` (libpqwalreceiver.c:1073-1142) — convert a tuple
/// query result into a tuplestore on `walres`.
pub fn libpqrcv_processTuples(
    pgres: PgResultId,
    walres: &mut WalRcvExecResult,
    nRetTypes: i32,
    retTypes: &[Oid],
) -> PgResult<()> {
    let nfields = rt::pq_nfields::call(pgres);

    // Make sure we got the expected number of fields.
    if nfields != nRetTypes {
        ereport(ERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg("invalid query response")
            .errdetail(format!("Expected {nRetTypes} fields, got {nfields} fields."))
            .finish(here("libpqrcv_processTuples"))?;
    }

    let work_mem = rt::work_mem::call();
    walres.tuplestore = rt::tuplestore_begin_heap::call(true, false, work_mem);

    // Create tuple descriptor corresponding to expected result.
    walres.tupledesc = rt::create_template_tuple_desc::call(nRetTypes);
    for coln in 0..nRetTypes {
        let fname = rt::pq_fname::call(pgres, coln);
        rt::tuple_desc_init_entry::call(
            walres.tupledesc,
            (coln + 1) as i16,
            fname,
            retTypes[coln as usize],
            -1,
            0,
        );
    }
    let attinmeta = rt::tuple_desc_get_att_in_metadata::call(walres.tupledesc);

    // No point doing more if there were no tuples returned.
    if rt::pq_ntuples::call(pgres) == 0 {
        return Ok(());
    }

    // Create temporary context for local allocations.
    let rowcontext =
        rt::alloc_set_context_create_default::call("libpqrcv query result context".to_string());

    // Process returned rows.
    let ntuples = rt::pq_ntuples::call(pgres);
    for tupn in 0..ntuples {
        // char *cstrs[MaxTupleAttributeNumber]; — only the first `nfields` are
        // filled and consumed by BuildTupleFromCStrings.
        let mut cstrs: Vec<Option<Vec<u8>>> = Vec::new();

        rt::check_for_interrupts::call()?;

        // Do the allocations in temporary context.
        let oldcontext = rt::memory_context_switch_to::call(rowcontext);

        // Fill cstrs with the column values (None == SQL NULL).
        for coln in 0..nfields {
            if rt::pq_getisnull::call(pgres, tupn, coln) {
                cstrs.push(None);
            } else {
                cstrs.push(Some(rt::pq_getvalue::call(pgres, tupn, coln)));
            }
        }
        debug_assert!(cstrs.len() <= MaxTupleAttributeNumber);

        // Convert row to a tuple, and add it to the tuplestore.
        let tuple = rt::build_tuple_from_c_strings::call(attinmeta, cstrs);
        rt::tuplestore_puttuple::call(walres.tuplestore, tuple);

        // Clean up.
        rt::memory_context_switch_to::call(oldcontext);
        rt::memory_context_reset::call(rowcontext);
    }

    rt::memory_context_delete::call(rowcontext);
    Ok(())
}

// ===========================================================================
// libpqrcv_exec (libpqwalreceiver.c:1149-1221).
// ===========================================================================

/// `libpqrcv_exec` (libpqwalreceiver.c:1149-1221) — public interface for sending
/// generic queries (and commands). Only callable from a process connected to a
/// database.
pub fn libpqrcv_exec(
    conn: &WalReceiverConn,
    query: &str,
    nRetTypes: i32,
    retTypes: &[Oid],
) -> PgResult<WalRcvExecResult> {
    let mut walres = WalRcvExecResult::palloc0();

    if rt::my_database_id::call() == InvalidOid {
        ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg("the query interface requires a database connection")
            .finish(here("libpqrcv_exec"))?;
    }

    let pgres = rt::libpqsrv_exec::call(
        conn.streamConn,
        query.to_string(),
        WAIT_EVENT_LIBPQWALRECEIVER_RECEIVE,
    );

    match rt::pq_result_status::call(pgres) {
        ExecStatusType::PGRES_TUPLES_OK
        | ExecStatusType::PGRES_SINGLE_TUPLE
        | ExecStatusType::PGRES_TUPLES_CHUNK => {
            walres.status = WalRcvExecStatus::WALRCV_OK_TUPLES;
            libpqrcv_processTuples(pgres, &mut walres, nRetTypes, retTypes)?;
        }

        ExecStatusType::PGRES_COPY_IN => {
            walres.status = WalRcvExecStatus::WALRCV_OK_COPY_IN;
        }

        ExecStatusType::PGRES_COPY_OUT => {
            walres.status = WalRcvExecStatus::WALRCV_OK_COPY_OUT;
        }

        ExecStatusType::PGRES_COPY_BOTH => {
            walres.status = WalRcvExecStatus::WALRCV_OK_COPY_BOTH;
        }

        ExecStatusType::PGRES_COMMAND_OK => {
            walres.status = WalRcvExecStatus::WALRCV_OK_COMMAND;
        }

        // Empty query is considered an error.
        ExecStatusType::PGRES_EMPTY_QUERY => {
            walres.status = WalRcvExecStatus::WALRCV_ERROR;
            walres.err = Some("empty query".to_string());
        }

        ExecStatusType::PGRES_PIPELINE_SYNC | ExecStatusType::PGRES_PIPELINE_ABORTED => {
            walres.status = WalRcvExecStatus::WALRCV_ERROR;
            walres.err = Some("unexpected pipeline mode".to_string());
        }

        ExecStatusType::PGRES_NONFATAL_ERROR
        | ExecStatusType::PGRES_FATAL_ERROR
        | ExecStatusType::PGRES_BAD_RESPONSE => {
            walres.status = WalRcvExecStatus::WALRCV_ERROR;
            let msg = rt::pq_error_message::call(conn.streamConn);
            walres.err = Some(pchomp(&msg));
            let diag_sqlstate = rt::pq_result_error_field_sqlstate::call(pgres);
            if let Some(diag_sqlstate) = diag_sqlstate {
                let b = diag_sqlstate.as_bytes();
                // MAKE_SQLSTATE(ch1..ch5)
                walres.sqlstate = make_sqlstate([
                    b.first().copied().unwrap_or(0),
                    b.get(1).copied().unwrap_or(0),
                    b.get(2).copied().unwrap_or(0),
                    b.get(3).copied().unwrap_or(0),
                    b.get(4).copied().unwrap_or(0),
                ])
                .0;
            }
        }
    }

    rt::pq_clear::call(pgres);

    Ok(walres)
}

// ===========================================================================
// stringlist_to_identifierstr (libpqwalreceiver.c:1231-1261).
// ===========================================================================

/// `stringlist_to_identifierstr` (libpqwalreceiver.c:1231-1261) — given a List of
/// strings, return a single comma-separated string, quoting identifiers as
/// needed (the reverse of `SplitIdentifierString`). Returns `None` on escape
/// failure. The caller frees the result.
///
/// In C the list holds `String` value nodes accessed via `strVal(lfirst(lc))`;
/// here the publication names arrive pre-resolved as a `&[String]`.
pub fn stringlist_to_identifierstr(conn: PgConnId, strings: &[String]) -> Option<String> {
    // initStringInfo(&res) — Rust String.
    let mut res = String::new();
    let mut first = true;

    for val in strings {
        if first {
            first = false;
        } else {
            res.push(',');
        }

        let val_escaped = rt::pq_escape_identifier::call(conn, val.clone());
        let val_escaped = match val_escaped {
            Some(v) => v,
            None => {
                // free(res.data); return NULL;
                return None;
            }
        };
        res.push_str(&val_escaped);
        // PQfreemem(val_escaped) — owned copy drops.
    }

    Some(res)
}

/// Mirror of the `SqlState`-typed errcode constant the C errcodes table assigns
/// `2F003`; re-exported for tests/consumers that assert on it.
pub const ERRCODE_S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED_LOCAL: SqlState =
    make_sqlstate(*b"2F003");

/// Install every inward seam this crate owns
/// (`libpqwalreceiver_seams`).
pub fn init_seams() {
    walrcv_table::init_seams();
}

#[cfg(test)]
mod tests;
