// libpqwalreceiver.c over the shared in-process wire client (crate pgclient;
// fast carries no libpq). This module keeps the libpqrcv_* verb surface and
// the replication-specific connect policy; the protocol lives in pgclient.
use elog::ereport;
use types_core::{pgsocket, TimeLineID, XLogRecPtr, PGINVALID_SOCKET};
use types_error::{
    ErrorLocation, PgResult, ERRCODE_CONNECTION_FAILURE, ERRCODE_PROTOCOL_VIOLATION,
    ERRCODE_SYNTAX_ERROR, ERROR,
};

pub use pgclient::{parse_conninfo, CopyData, ExecStatus, PgConn, QueryResult};
use pgclient::{opt, os_user_name, WaitEvents};

const WAIT_EVENT_LIBPQWALRECEIVER_CONNECT: u32 = 0x0600_0000 | 3;
const WAIT_EVENT_LIBPQWALRECEIVER_RECEIVE: u32 = 0x0600_0000 | 4;

const WE: WaitEvents = WaitEvents {
    connect: WAIT_EVENT_LIBPQWALRECEIVER_CONNECT,
    receive: WAIT_EVENT_LIBPQWALRECEIVER_RECEIVE,
};

#[track_caller]
fn loc(func: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, func)
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

    let user = opt(&opts, "user").map(|s| s.to_string()).unwrap_or_else(os_user_name);
    let appname = opt(&opts, "application_name").unwrap_or(appname).to_string();
    let options = opt(&opts, "options").unwrap_or("").to_string();
    let dbname = opt(&opts, "dbname").unwrap_or("").to_string();

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

    let mut conn = match pgclient::connect(opts, &params, WE)? {
        Ok(c) => c,
        Err(e) => return Ok(Err(e)),
    };

    // Set always-secure search path for connections that run SQL queries, so
    // malicious users can't redirect user code, e.g. operators
    // (libpqrcv_connect after commit b3f6b14cf48; ALWAYS_SECURE_SEARCH_PATH_SQL).
    // recovery/040's poisoned-'=' scenario catches its absence.
    if !replication || logical {
        match conn.exec("SELECT pg_catalog.set_config('search_path', '', false);") {
            Ok(res) if res.status == ExecStatus::TuplesOk => {}
            Ok(res) => {
                return Ok(Err(format!("could not clear \"search_path\": {}", res.err)));
            }
            Err(e) => return Ok(Err(format!("could not clear \"search_path\": {e}"))),
        }
    }

    Ok(Ok(conn))
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

    // Ok(Err(msg)) is libpqrcv_connect's NULL-return-with-*err arm.
    fn conn_err(conninfo: &str) -> String {
        match connect_extended(conninfo, true, false, false, "t") {
            Ok(Err(e)) => e,
            Ok(Ok(_)) => panic!("unexpected successful connection for {conninfo:?}"),
            Err(e) => panic!("unexpected ereport for {conninfo:?}: {e}"),
        }
    }

    #[test]
    fn connect_rejects_bad_port_without_connecting() {
        // PQconnectPoll's try-next-host arm validates the port option before
        // any socket is opened — these never touch the network (regress:
        // CREATE SUBSCRIPTION ... 'port=-1' "does so without connecting").
        assert_eq!(conn_err("port=-1"), "invalid port number: \"-1\"");
        assert_eq!(conn_err("port=70000"), "invalid port number: \"70000\"");
        assert_eq!(conn_err("port=0"), "invalid port number: \"0\"");
        // pqParseIntParam arm: trailing garbage is an integer-value error.
        assert_eq!(
            conn_err("port=1foo"),
            "invalid integer value \"1foo\" for connection option \"port\""
        );
        // must_use_password recheck still precedes everything (C checks it
        // post-connect; recorded divergence — see connect_extended).
        match connect_extended("port=-1", true, false, true, "t") {
            Err(e) => assert!(e.message().contains("password is required")),
            Ok(_) => panic!("expected password-required ereport"),
        }
    }
}
