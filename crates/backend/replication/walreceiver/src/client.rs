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
// must_use_password renders libpqrcv_check_conninfo's recheck (the conninfo
// itself must carry a password) AND, like C, verifies post-connect via
// PQconnectionUsedPassword that the server actually demanded the credential,
// so a non-superuser subscription can't ride ambient trust/peer/ident auth.
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

    // PQconnectStartParams (conninfo_array_parse): service file, environment
    // and compiled defaults fill in what the conninfo leaves out.
    let opts = match pgclient::resolve_conninfo(conninfo) {
        Ok(o) => o,
        Err(e) => return Ok(Err(e)),
    };
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

    // The conninfo-syntax recheck above cannot tell whether the server
    // actually demanded the password: a trust/peer/ident HBA line lets the
    // connection succeed without ever consuming the supplied credential. So
    // when must_use_password is set, verify post-connect that the password was
    // really used (PQconnectionUsedPassword); otherwise a non-superuser-owned
    // subscription could ride the server's ambient authentication. Close the
    // connection and ereport (matching libpqrcv_connect).
    if must_use_password && !conn.used_password() {
        conn.terminate();
        return throw(
            ereport(ERROR)
                .errcode(types_error::ERRCODE_S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED)
                .errmsg("password is required")
                .errdetail("Non-superuser cannot connect if the server does not request a password.")
                .errhint(
                    "Target server's authentication method must be changed, or set password_required=false in the subscription parameters.",
                )
                .finish(loc("libpqrcv_connect")),
        );
    }

    // Set always-secure search path for connections that run SQL queries, so
    // malicious users can't redirect user code, e.g. operators
    // (libpqrcv_connect after commit b3f6b14cf48; ALWAYS_SECURE_SEARCH_PATH_SQL).
    // recovery/040's poisoned-'=' scenario catches its absence. The failure
    // text is C's "could not clear search path: %s" (libpqwalreceiver.c:262).
    if !replication || logical {
        // libpqsrv_exec's own ereports (interrupts) propagate, as in C.
        let res = conn.exec("SELECT pg_catalog.set_config('search_path', '', false);")?;
        if res.status != ExecStatus::TuplesOk {
            return Ok(Err(format!("could not clear search path: {}", pchomp(&res.err))));
        }
    }

    Ok(Ok(conn))
}

/// libpqrcv_identify_system. Returns (sysid, primary tli, flush position).
// upstream 33101632235a (18.6): Fix cascading standby reconnect failure after archive fallback
// The xlogpos column (C: WalRcvIdentifySystemLsn, a global for ABI reasons)
// rides the return value here.
pub fn identify_system(conn: &mut PgConn) -> PgResult<(String, TimeLineID, XLogRecPtr)> {
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
    // *primary_tli = pg_strtoint32(PQgetvalue(res, 0, 1)) (libpqwalreceiver.c:460):
    // numutils' grammar and its 22P02/22003 ereports, the int32 assigned to
    // the unsigned TimeLineID as C does.
    let tli = numutils::pg_strtoint32(&text_col(&res, 0, 1))? as TimeLineID;
    // Column 2 is the server's current WAL flush position.
    let xlogpos = text_col(&res, 0, 2);
    let Some(flush) = sscanf_lsn(&xlogpos) else {
        return throw(ereport(ERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg(format!("could not parse WAL location \"{xlogpos}\""))
            .finish(loc("libpqrcv_identify_system")));
    };
    Ok((sysid, tli, flush))
}

// sscanf(s, "%X/%X", &hi, &lo) == 2: each %X skips leading whitespace, takes
// an optional 0x prefix and at least one hex digit (wrapping at 32 bits);
// trailing bytes are ignored.
fn sscanf_lsn(s: &str) -> Option<XLogRecPtr> {
    fn scan_x(b: &[u8]) -> Option<(u32, usize)> {
        let mut i = 0;
        // C-locale isspace: the six ASCII blanks.
        while i < b.len() && matches!(b[i], b' ' | b'\t' | b'\n' | b'\x0b' | b'\x0c' | b'\r') {
            i += 1;
        }
        if b.len() >= i + 2 && b[i] == b'0' && (b[i + 1] == b'x' || b[i + 1] == b'X') {
            i += 2;
        }
        let start = i;
        let mut v: u32 = 0;
        while i < b.len() && b[i].is_ascii_hexdigit() {
            v = v.wrapping_mul(16).wrapping_add((b[i] as char).to_digit(16)?);
            i += 1;
        }
        (i > start).then_some((v, i))
    }
    let b = s.as_bytes();
    let (hi, n) = scan_x(b)?;
    if b.get(n) != Some(&b'/') {
        return None;
    }
    let (lo, _) = scan_x(&b[n + 1..])?;
    Some(((hi as u64) << 32) | lo as u64)
}

fn text_col(res: &QueryResult, row: usize, col: usize) -> String {
    match res.rows.get(row).and_then(|r| r.get(col)) {
        Some(Some(v)) => String::from_utf8_lossy(v).into_owned(),
        _ => String::new(),
    }
}

// upstream abb5825550a8 (18.5): Clean up quoting of variable strings within replication commands.
// appendQuotedIdentifier: replication-grammar quoting (doubled quotes), not SQL's.
fn append_quoted_identifier(buf: &mut String, s: &str) {
    buf.push('"');
    for c in s.chars() {
        if c == '"' {
            buf.push('"');
        }
        buf.push(c);
    }
    buf.push('"');
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
        // upstream abb5825550a8 (18.5): quote the slot name.
        cmd.push_str(" SLOT ");
        append_quoted_identifier(&mut cmd, slot);
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

// libpqrcv_create_slot's physical command text (new options syntax; the
// publisher is same-version).
pub(crate) fn create_slot_physical_cmd(slotname: &str, temporary: bool) -> String {
    // upstream abb5825550a8 (18.5): quote the slot name.
    let mut cmd = String::from("CREATE_REPLICATION_SLOT ");
    append_quoted_identifier(&mut cmd, slotname);
    if temporary {
        cmd.push_str(" TEMPORARY");
    }
    cmd.push_str(" PHYSICAL (RESERVE_WAL)");
    cmd
}

// upstream a6a2eb9f6024 (18.6): Check CREATE_REPLICATION_SLOT response shape in libpqwalreceiver
/// CREATE_REPLICATION_SLOT returns a single row with four columns; any other
/// shape is a protocol violation (a zero-row result crashed C's LSN parse).
/// Shared by every CREATE_REPLICATION_SLOT arm (the logical ones live in
/// subscriptioncmds and tablesync).
pub fn check_create_slot_result(res: &QueryResult, slotname: &str) -> PgResult<()> {
    if res.nfields != 4 || res.rows.len() != 1 {
        return ereport(ERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg("invalid response from primary server")
            .errdetail(format!(
                "Could not create replication slot \"{slotname}\": got {} rows and {} fields, expected {} rows and {} fields.",
                res.rows.len(),
                res.nfields,
                1,
                4
            ))
            .finish(loc("libpqrcv_create_slot"));
    }
    Ok(())
}

/// libpqrcv_create_slot, physical arm (the walreceiver's temporary slot;
/// walreceiver.c:362 passes lsn = NULL, so the reserved LSN is not returned).
pub fn create_slot_physical(conn: &mut PgConn, slotname: &str, temporary: bool) -> PgResult<()> {
    let cmd = create_slot_physical_cmd(slotname, temporary);
    let res = conn.exec(&cmd)?;
    if res.status != ExecStatus::TuplesOk {
        return throw(ereport(ERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg(format!(
                "could not create replication slot \"{slotname}\": {}",
                pchomp(&res.err)
            ))
            .finish(loc("libpqrcv_create_slot")));
    }
    // upstream a6a2eb9f6024 (18.6): Check CREATE_REPLICATION_SLOT response shape in libpqwalreceiver
    check_create_slot_result(&res, slotname)?;
    Ok(())
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
            // *next_tli = pg_strtoint32(PQgetvalue(res, 0, 0)) (libpqwalreceiver.c:718):
            // garbage is numutils' 22P02 ereport, not "no timeline reported".
            next_tli = numutils::pg_strtoint32(&text_col(r, 0, 0))? as TimeLineID;
            res = conn.get_result()?;
        } else if r.status == ExecStatus::CopyOut {
            // C: if CopyDone hadn't been received from the backend yet (copy
            // aborted mid-stream), PQendcopy drains the copy; a drain failure
            // is 08006 (libpqwalreceiver.c:693-701).
            loop {
                match conn.get_copy_data() {
                    Ok(CopyData::End) => break,
                    Ok(CopyData::Msg(_)) => {}
                    Ok(CopyData::Block) => {
                        if !conn.consume_input() {
                            return throw(ereport(ERROR)
                                .errcode(ERRCODE_CONNECTION_FAILURE)
                                .errmsg(format!(
                                    "error while shutting down streaming COPY: {}",
                                    pchomp(&conn.error_message())
                                ))
                                .finish(loc("libpqrcv_endstreaming")));
                        }
                    }
                    Err(e) => {
                        return throw(ereport(ERROR)
                            .errcode(ERRCODE_CONNECTION_FAILURE)
                            .errmsg(format!(
                                "error while shutting down streaming COPY: {}",
                                pchomp(&e)
                            ))
                            .finish(loc("libpqrcv_endstreaming")));
                    }
                }
            }
            // CommandComplete should follow
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
///
/// SQLSTATEs follow libpqwalreceiver.c: only a failed PQconsumeInput (:850)
/// is ERRCODE_CONNECTION_FAILURE; a COPY ended by anything but
/// CommandComplete/CopyIn (:905) and a PQgetCopyData error (:912) are
/// ERRCODE_PROTOCOL_VIOLATION. get_copy_data's Err is libpq's rawlen == -1
/// with an error result (the server's ErrorResponse) or rawlen == -2.
pub fn receive(conn: &mut PgConn) -> PgResult<(i32, Vec<u8>, pgsocket)> {
    let first = match conn.get_copy_data() {
        Ok(d) => d,
        Err(e) => return receive_stream_error(ERRCODE_PROTOCOL_VIOLATION, &e),
    };
    let data = match first {
        CopyData::Block => {
            if !conn.consume_input() {
                return receive_stream_error(ERRCODE_CONNECTION_FAILURE, &conn.error_message());
            }
            match conn.get_copy_data() {
                Ok(CopyData::Block) => return Ok((0, Vec::new(), conn.socket())),
                Ok(d) => d,
                Err(e) => return receive_stream_error(ERRCODE_PROTOCOL_VIOLATION, &e),
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
                _ => receive_stream_error(ERRCODE_PROTOCOL_VIOLATION, &conn.error_message()),
            }
        }
        CopyData::Block => unreachable!(),
    }
}

fn receive_stream_error(
    sqlstate: types_error::SqlState,
    err: &str,
) -> PgResult<(i32, Vec<u8>, pgsocket)> {
    throw(
        ereport(ERROR)
            .errcode(sqlstate)
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
        // must_use_password's conninfo recheck still precedes everything (the
        // PQconnectionUsedPassword post-connect check happens after connecting).
        match connect_extended("port=-1", true, false, true, "t") {
            Err(e) => assert!(e.message().contains("password is required")),
            Ok(_) => panic!("expected password-required ereport"),
        }
    }

    // walrcv_create_slot for wal_receiver_create_temp_slot (walreceiver.c:355):
    // the physical CREATE_REPLICATION_SLOT command and the generated name.
    #[test]
    fn temp_slot_command_text() {
        assert_eq!(
            super::create_slot_physical_cmd("pg_walreceiver_12345", true),
            "CREATE_REPLICATION_SLOT \"pg_walreceiver_12345\" TEMPORARY PHYSICAL (RESERVE_WAL)"
        );
        assert_eq!(
            super::create_slot_physical_cmd("s", false),
            "CREATE_REPLICATION_SLOT \"s\" PHYSICAL (RESERVE_WAL)"
        );
        // snprintf "pg_walreceiver_%lld" over the backend pid (walreceiver.c:359).
        assert_eq!(format!("pg_walreceiver_{}", 42i32 as i64), "pg_walreceiver_42");
    }

    // upstream abb5825550a8 (18.5): embedded double quotes are doubled.
    #[test]
    fn replication_command_identifiers_are_quote_doubled() {
        assert_eq!(
            super::create_slot_physical_cmd("odd\"na\"\"me", true),
            "CREATE_REPLICATION_SLOT \"odd\"\"na\"\"\"\"me\" TEMPORARY PHYSICAL (RESERVE_WAL)"
        );
    }

    // ---- a scripted replication server over a real socket ----
    //
    // The real client stack runs end to end (pgclient connect/exec, the
    // waiteventset socket wait); only the far end is canned: one accepted
    // connection, the startup packet answered with AuthenticationOk +
    // ReadyForQuery, then each Query answered with the next reply.
    fn wire(t: u8, body: &[u8]) -> Vec<u8> {
        let mut m = vec![t];
        m.extend_from_slice(&(body.len() as u32 + 4).to_be_bytes());
        m.extend_from_slice(body);
        m
    }

    // RowDescription + DataRows + CommandComplete, no ReadyForQuery (the
    // end-of-streaming result set precedes the COPY's own CommandComplete).
    fn tuples_no_ready(fields: &[&str], rows: &[&[&str]]) -> Vec<u8> {
        let mut desc = (fields.len() as i16).to_be_bytes().to_vec();
        for f in fields {
            desc.extend_from_slice(f.as_bytes());
            desc.push(0);
            desc.extend_from_slice(&0i32.to_be_bytes()); // table oid
            desc.extend_from_slice(&0i16.to_be_bytes()); // attnum
            desc.extend_from_slice(&25i32.to_be_bytes()); // text
            desc.extend_from_slice(&(-1i16).to_be_bytes()); // typlen
            desc.extend_from_slice(&(-1i32).to_be_bytes()); // typmod
            desc.extend_from_slice(&0i16.to_be_bytes()); // format
        }
        let mut out = wire(b'T', &desc);
        for row in rows {
            let mut body = (row.len() as i16).to_be_bytes().to_vec();
            for col in *row {
                body.extend_from_slice(&(col.len() as i32).to_be_bytes());
                body.extend_from_slice(col.as_bytes());
            }
            out.extend_from_slice(&wire(b'D', &body));
        }
        out.extend_from_slice(&wire(b'C', format!("SELECT {}\0", rows.len()).as_bytes()));
        out
    }

    fn tuples(fields: &[&str], rows: &[&[&str]]) -> Vec<u8> {
        let mut out = tuples_no_ready(fields, rows);
        out.extend_from_slice(&wire(b'Z', b"I"));
        out
    }

    // ErrorResponse (severity ERROR) with the given SQLSTATE and message.
    fn error_response(sqlstate: &str, message: &str) -> Vec<u8> {
        let mut body = Vec::new();
        for (code, val) in [(b'S', "ERROR"), (b'V', "ERROR"), (b'C', sqlstate), (b'M', message)] {
            body.push(code);
            body.extend_from_slice(val.as_bytes());
            body.push(0);
        }
        body.push(0);
        wire(b'E', &body)
    }

    // CopyBothResponse: text format, no columns.
    fn copy_both() -> Vec<u8> {
        wire(b'W', &[0, 0, 0])
    }

    // Each reply answers the next client frame: a Query ('Q', recorded) or
    // the CopyDone ('c') libpqrcv_endstreaming sends.
    fn scripted_server(replies: Vec<Vec<u8>>) -> (u16, std::thread::JoinHandle<Vec<String>>) {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let handle = std::thread::spawn(move || {
            let (mut s, _) = listener.accept().unwrap();
            let mut len = [0u8; 4];
            s.read_exact(&mut len).unwrap();
            let mut startup = vec![0u8; u32::from_be_bytes(len) as usize - 4];
            s.read_exact(&mut startup).unwrap();
            let mut hello = wire(b'R', &0i32.to_be_bytes());
            hello.extend_from_slice(&wire(b'Z', b"I"));
            s.write_all(&hello).unwrap();
            let mut queries = Vec::new();
            for reply in replies {
                let mut t = [0u8; 1];
                if s.read_exact(&mut t).is_err() {
                    break;
                }
                assert!(t[0] == b'Q' || t[0] == b'c', "unexpected client frame {}", t[0] as char);
                s.read_exact(&mut len).unwrap();
                let mut q = vec![0u8; u32::from_be_bytes(len) as usize - 4];
                s.read_exact(&mut q).unwrap();
                if t[0] == b'Q' {
                    queries.push(String::from_utf8_lossy(&q[..q.len() - 1]).into_owned());
                }
                s.write_all(&reply).unwrap();
            }
            queries
        });
        (port, handle)
    }

    // The client's socket waits ride WaitLatchOrSocket: the real
    // waiteventset/latch stack, one owned latch per test thread (the
    // waiteventset crate's own test recipe).
    fn client_env() {
        static ENV: std::sync::Once = std::sync::Once::new();
        static NEXT_PID: std::sync::atomic::AtomicI32 = std::sync::atomic::AtomicI32::new(7300);
        ENV.call_once(|| {
            if !waitevent_seams::pgstat_report_wait_start::is_installed() {
                waitevent_seams::pgstat_report_wait_start::set(|_| {});
                waitevent_seams::pgstat_report_wait_end::set(|| {});
            }
            if !waiteventset_seams::create_wait_event_set_current_owner::is_installed() {
                waiteventset::init_seams();
            }
            if !latch_seams::set_latch::is_installed() {
                latch::init_seams();
            }
            if !postgres_seams::check_for_interrupts::is_installed() {
                postgres_seams::check_for_interrupts::set(|| Ok(()));
            }
        });
        if init_small::globals::MyLatch().is_none() {
            init_small::globals::SetMyProcPid(
                NEXT_PID.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
            );
            fd::vfd::set_max_safe_fds_value(1000);
            waiteventset::InitializeWaitEventSupport().unwrap();
            let h = latch::allocate_local_latch();
            latch::InitLatch(h);
            init_small::globals::SetMyLatch(Some(h));
        }
    }

    fn connect_scripted(port: u16) -> PgConn {
        client_env();
        match connect(&format!("host=127.0.0.1 port={port} user=walrcv"), "t") {
            Ok(Ok(c)) => c,
            Ok(Err(e)) => panic!("scripted server refused: {e}"),
            Err(e) => panic!("ereport connecting: {e}"),
        }
    }

    // libpqrcv_connect connects through PQconnectStartParams
    // (libpqwalreceiver.c:230): a service= conninfo takes host, port and
    // user from the service file.
    #[test]
    fn connect_resolves_the_service_file() {
        let (port, server) = scripted_server(vec![]);
        let dir = std::env::temp_dir().join(format!("walrcv-svc-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let f = dir.join("pg_service.conf");
        std::fs::write(&f, format!("[walrcv_test]\nhost=127.0.0.1\nport={port}\nuser=walrcv\n"))
            .unwrap();
        std::env::set_var("PGSERVICEFILE", &f);
        client_env();
        let conn = match connect("service=walrcv_test", "t") {
            Ok(Ok(c)) => c,
            Ok(Err(e)) => panic!("service file not resolved: {e}"),
            Err(e) => panic!("ereport connecting: {e}"),
        };
        drop(conn);
        server.join().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    const CREATE_SLOT_FIELDS: [&str; 4] =
        ["slot_name", "consistent_point", "snapshot_name", "output_plugin"];

    // upstream a6a2eb9f6024 (18.6): Check CREATE_REPLICATION_SLOT response shape in libpqwalreceiver
    // A TuplesOk reply without exactly one row of four fields is a protocol
    // violation (C read PQgetvalue of a missing row and crashed in pg_lsn_in).
    #[test]
    fn create_slot_rejects_a_zero_row_reply() {
        let (port, server) = scripted_server(vec![tuples(&CREATE_SLOT_FIELDS, &[])]);
        let mut conn = connect_scripted(port);
        let err = create_slot_physical(&mut conn, "pg_walreceiver_7", true).unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_PROTOCOL_VIOLATION);
        assert_eq!(err.message(), "invalid response from primary server");
        assert_eq!(
            err.detail(),
            Some("Could not create replication slot \"pg_walreceiver_7\": got 0 rows and 4 fields, expected 1 rows and 4 fields.")
        );
        drop(conn);
        assert_eq!(
            server.join().unwrap(),
            vec!["CREATE_REPLICATION_SLOT \"pg_walreceiver_7\" TEMPORARY PHYSICAL (RESERVE_WAL)"]
        );
    }

    #[test]
    fn create_slot_rejects_a_three_field_reply() {
        let (port, server) =
            scripted_server(vec![tuples(&CREATE_SLOT_FIELDS[..3], &[&["s", "0/1", ""]])]);
        let mut conn = connect_scripted(port);
        let err = create_slot_physical(&mut conn, "s", false).unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_PROTOCOL_VIOLATION);
        assert_eq!(
            err.detail(),
            Some("Could not create replication slot \"s\": got 1 rows and 3 fields, expected 1 rows and 4 fields.")
        );
        drop(conn);
        server.join().unwrap();
    }

    #[test]
    fn create_slot_accepts_one_row_of_four_fields() {
        let (port, server) = scripted_server(vec![tuples(
            &CREATE_SLOT_FIELDS,
            &[&["s", "0/1A2B3C", "", ""]],
        )]);
        let mut conn = connect_scripted(port);
        create_slot_physical(&mut conn, "s", false).unwrap();
        drop(conn);
        server.join().unwrap();
    }

    // The shared shape check the logical arms (subscriptioncmds, tablesync)
    // call on their own results.
    #[test]
    fn create_slot_shape_check_counts_rows_and_fields() {
        let mut res = QueryResult {
            status: ExecStatus::TuplesOk,
            nfields: 4,
            rows: vec![vec![None; 4], vec![None; 4]],
            cmd_tag: String::new(),
            diag: None,
            err: String::new(),
        };
        let err = check_create_slot_result(&res, "two").unwrap_err();
        assert_eq!(
            err.detail(),
            Some("Could not create replication slot \"two\": got 2 rows and 4 fields, expected 1 rows and 4 fields.")
        );
        res.rows.truncate(1);
        check_create_slot_result(&res, "one").unwrap();
    }

    // upstream 33101632235a (18.6): Fix cascading standby reconnect failure after archive fallback
    // IDENTIFY_SYSTEM's third column (xlogpos) is the upstream's flush
    // position; libpqrcv_identify_system used to discard it.
    #[test]
    fn identify_system_returns_the_upstream_flush_position() {
        let (port, server) = scripted_server(vec![tuples(
            &["systemid", "timeline", "xlogpos", "dbname"],
            &[&["7000000000000000001", "3", "1/2A3B4C5D", ""]],
        )]);
        let mut conn = connect_scripted(port);
        let (sysid, tli, flush) = identify_system(&mut conn).unwrap();
        assert_eq!(sysid, "7000000000000000001");
        assert_eq!(tli, 3);
        assert_eq!(flush, 0x0000_0001_2A3B_4C5D);
        drop(conn);
        assert_eq!(server.join().unwrap(), vec!["IDENTIFY_SYSTEM"]);
    }

    #[test]
    fn identify_system_rejects_an_unparseable_flush_position() {
        let (port, server) = scripted_server(vec![tuples(
            &["systemid", "timeline", "xlogpos", "dbname"],
            &[&["1", "1", "nope", ""]],
        )]);
        let mut conn = connect_scripted(port);
        let err = identify_system(&mut conn).unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_PROTOCOL_VIOLATION);
        assert_eq!(err.message(), "could not parse WAL location \"nope\"");
        drop(conn);
        server.join().unwrap();
    }

    // libpqrcv_identify_system (libpqwalreceiver.c:460) parses the timeline
    // with pg_strtoint32: a non-integer column is numutils' own 22P02
    // "invalid input syntax for type integer", not a protocol violation
    // (audit-18.6 b064 row 4d5ef930).
    #[test]
    fn identify_system_rejects_a_non_integer_timeline_like_pg_strtoint32() {
        let (port, server) = scripted_server(vec![tuples(
            &["systemid", "timeline", "xlogpos", "dbname"],
            &[&["1", "abc", "0/1", ""]],
        )]);
        let mut conn = connect_scripted(port);
        let err = identify_system(&mut conn).unwrap_err();
        assert_eq!(err.sqlstate(), types_error::ERRCODE_INVALID_TEXT_REPRESENTATION);
        assert_eq!(err.message(), "invalid input syntax for type integer: \"abc\"");
        drop(conn);
        server.join().unwrap();
    }

    // libpqrcv_endstreaming (libpqwalreceiver.c:718): the next timeline is
    // pg_strtoint32(PQgetvalue(res, 0, 0)) — garbage ereports 22P02 instead
    // of silently reading as "no timeline reported" (0)
    // (audit-18.6 b064 row d4ddc322).
    fn end_of_streaming_reply(next_tli: &str) -> Vec<u8> {
        // walsender after CopyDone: the result set, then the COPY's own
        // CommandComplete, then ReadyForQuery.
        let mut reply = wire(b'c', &[]);
        reply.extend_from_slice(&tuples_no_ready(
            &["next_tli", "next_tli_startpos"],
            &[&[next_tli, "0/3000000"]],
        ));
        reply.extend_from_slice(&wire(b'C', b"START_STREAMING\0"));
        reply.extend_from_slice(&wire(b'Z', b"I"));
        reply
    }

    #[test]
    fn end_streaming_reads_the_next_timeline() {
        let (port, server) = scripted_server(vec![copy_both(), end_of_streaming_reply("7")]);
        let mut conn = connect_scripted(port);
        assert!(start_streaming(&mut conn, None, 0x3000000, 1).unwrap());
        assert_eq!(end_streaming(&mut conn).unwrap(), 7);
        drop(conn);
        assert_eq!(server.join().unwrap(), vec!["START_REPLICATION 0/3000000 TIMELINE 1"]);
    }

    #[test]
    fn end_streaming_rejects_a_non_integer_next_timeline_like_pg_strtoint32() {
        let (port, server) = scripted_server(vec![copy_both(), end_of_streaming_reply("abc")]);
        let mut conn = connect_scripted(port);
        assert!(start_streaming(&mut conn, None, 0x3000000, 1).unwrap());
        let err = end_streaming(&mut conn).unwrap_err();
        assert_eq!(err.sqlstate(), types_error::ERRCODE_INVALID_TEXT_REPRESENTATION);
        assert_eq!(err.message(), "invalid input syntax for type integer: \"abc\"");
        drop(conn);
        server.join().unwrap();
    }

    // libpqrcv_receive (libpqwalreceiver.c:905-914): a COPY that ends in
    // anything but CommandComplete/CopyIn — here the walsender's ERROR mid
    // stream, libpq's rawlen == -1 with a PGRES_FATAL_ERROR result — is
    // ERRCODE_PROTOCOL_VIOLATION; 08006 is reserved for PQconsumeInput
    // failing (:850) (audit-18.6 b064 row cf0e96eb).
    #[test]
    fn receive_reports_a_mid_stream_error_as_protocol_violation() {
        let mut reply = copy_both();
        reply.extend_from_slice(&error_response(
            "58P01",
            "requested WAL segment 000000010000000000000003 has already been removed",
        ));
        reply.extend_from_slice(&wire(b'Z', b"I"));
        let (port, server) = scripted_server(vec![reply]);
        let mut conn = connect_scripted(port);
        assert!(start_streaming(&mut conn, None, 0x3000000, 1).unwrap());
        let err = receive(&mut conn).unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_PROTOCOL_VIOLATION);
        assert_eq!(
            err.message(),
            "could not receive data from WAL stream: ERROR:  requested WAL segment 000000010000000000000003 has already been removed"
        );
        drop(conn);
        server.join().unwrap();
    }

    // libpqrcv_connect (libpqwalreceiver.c:262): a failed
    // ALWAYS_SECURE_SEARCH_PATH_SQL is "could not clear search path: %s" —
    // no quotes around the GUC name (audit-18.6 b064 row a5522518).
    #[test]
    fn clear_search_path_failure_uses_the_c_message() {
        let mut reply = error_response("42501", "permission denied to set parameter");
        reply.extend_from_slice(&wire(b'Z', b"I"));
        let (port, server) = scripted_server(vec![reply]);
        client_env();
        let r = connect_extended(
            &format!("host=127.0.0.1 port={port} user=walrcv"),
            false,
            false,
            false,
            "t",
        );
        match r {
            Ok(Err(msg)) => assert_eq!(
                msg,
                "could not clear search path: ERROR:  permission denied to set parameter"
            ),
            Ok(Ok(_)) => panic!("connection must fail when search_path cannot be cleared"),
            Err(e) => panic!("unexpected ereport: {e}"),
        }
        assert_eq!(
            server.join().unwrap(),
            vec!["SELECT pg_catalog.set_config('search_path', '', false);"]
        );
    }

    // sscanf("%X/%X") acceptance: leading blanks and 0x prefixes per
    // conversion, trailing junk ignored, anything short of two numbers fails.
    #[test]
    fn sscanf_lsn_matches_the_c_scan() {
        assert_eq!(sscanf_lsn("0/0"), Some(0));
        assert_eq!(sscanf_lsn("1/2A3B4C5D"), Some(0x1_2A3B_4C5D));
        assert_eq!(sscanf_lsn(" 1/ 2"), Some(0x1_0000_0002));
        assert_eq!(sscanf_lsn("0x1A/0X2b"), Some(0x1A_0000_002B));
        assert_eq!(sscanf_lsn("FFFFFFFF/FFFFFFFF"), Some(u64::MAX));
        assert_eq!(sscanf_lsn("1/2junk"), Some(0x1_0000_0002));
        assert_eq!(sscanf_lsn(""), None);
        assert_eq!(sscanf_lsn("1"), None);
        assert_eq!(sscanf_lsn("1/"), None);
        assert_eq!(sscanf_lsn("/1"), None);
        assert_eq!(sscanf_lsn("g/1"), None);
    }
}
