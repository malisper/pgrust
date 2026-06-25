//! `PgClientConn` — the live frontend connection over a byte-stream transport.
//!
//! The structural port of the `PQconnectPoll` (`fe-connect.c`) connection state
//! machine tail, the `PQexec` simple-query path (`fe-exec.c`), and the
//! `pqParseInput3` / `pqGetCopyData3` message readers (`fe-protocol3.c`), scoped
//! to what the `walrcv_*` plugin (walreceiver / logical apply / slotsync) and
//! the ecpg runtime actually call.
//!
//! # What is implemented (1:1 with the C control flow)
//!
//!  * **connect** ([`PgClientConn::connect`]) — send the startup packet, then
//!    run the auth/parameter loop: handle `AuthenticationRequest` (the
//!    `AUTH_REQ_OK` *trust* path is grounded; the cleartext-password path sends
//!    a `PasswordMessage`; MD5/SASL/GSS are loud "unsupported" errors, not
//!    silent), collect every `ParameterStatus`, record `BackendKeyData`, and
//!    finish on `ReadyForQuery`.
//!
//!  * **simple query** ([`PgClientConn::exec`]) — send a `Query` message, then
//!    pump messages until `ReadyForQuery`, building a [`PGresult`].
//!
//!  * **finish** ([`PgClientConn::finish`]) — send a `Terminate` ('X') message.
//!
//!  * **CopyBoth streaming** ([`PgClientConn::start_replication`] /
//!    [`PgClientConn::copy_receive`] / [`PgClientConn::copy_send`] /
//!    [`PgClientConn::copy_done`] / [`PgClientConn::end_copy`]).
//!
//! # What is deliberately NOT here
//!
//!  * the async `PQconnectPoll` cursor (returning `PGRES_POLLING_READING`): the
//!    blocking driver is the faithful behaviour for the synchronous
//!    `libpqsrv_exec` / `PQexec` entry points the consumers use;
//!  * TLS/GSS negotiation: this is the `--without-ssl --without-gssapi` build;
//!  * MD5 / SCRAM / SASL auth response loops — surfaced as a loud
//!    [`TransportError::AuthFailed`] rather than a stub.

use crate::codec::{self, BackendMessage, MsgReader};
use crate::protocol3;

/// Outcome of an async [`PgClientConn::copy_receive`], mirroring
/// `PQgetCopyData(conn, &buf, async=1)`: a CopyData payload, the end of the
/// COPY (`CopyDone`), or "no data ready yet" (the C `0` return).
pub enum CopyRecv {
    /// A CopyData ('d') payload.
    Data(Vec<u8>),
    /// CopyDone ('c') — the server ended the COPY.
    Done,
    /// No complete message is available without blocking (`PQgetCopyData`
    /// returning 0); the caller should wait on the socket.
    WouldBlock,
}
use crate::result::{ExecStatusType, PGresult, PgResAttDesc, PgResAttValue, PgTransactionStatusType};
use crate::transport::{Transport, TransportError};

/// The async COPY sub-state of a connection, mirroring the COPY-related values
/// of libpq's `conn->asyncStatus` (`PGASYNC_COPY_OUT` / `PGASYNC_COPY_IN` /
/// `PGASYNC_COPY_BOTH`). This drives the same control flow `getCopyDataMessage`
/// + `PQgetResult` use to end a COPY without blocking on the wire: in
/// particular, when a CopyBoth stream sees the server's `CopyDone` it transitions
/// to `In` (C's `asyncStatus = PGASYNC_COPY_IN`) and a subsequent `PQgetResult`
/// must synthesize a `PGRES_COPY_IN` result *without reading the socket* — the
/// trailing `CommandComplete`/`ReadyForQuery` are not sent by the peer until we
/// reply with our own `CopyDone` (`PQputCopyEnd`).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum CopyState {
    /// Not in a COPY (`asyncStatus` is none of the COPY values).
    None,
    /// `PGASYNC_COPY_OUT`: a COPY-out stream is in progress.
    Out,
    /// `PGASYNC_COPY_BOTH`: a CopyBoth (replication) stream is in progress.
    Both,
    /// `PGASYNC_COPY_IN`: reached either as a genuine COPY-in, or — the case
    /// that matters here — after the server ended a CopyBoth stream with
    /// `CopyDone` (we owe it our own `CopyDone` before the trailing result is
    /// sent). `PQgetResult` here yields a `PGRES_COPY_IN` result with no wire I/O.
    In,
    /// A COPY-out stream's `CopyDone` was seen; the trailing
    /// `CommandComplete`/`ReadyForQuery` ARE already on the wire, so the next
    /// `PQgetResult` collects the queued trailing result (the C `PGASYNC_BUSY`
    /// transition that `getCopyDataMessage` performs for a plain COPY-out).
    OutEnded,
    /// We have sent our own `CopyDone` ([`Self::copy_done`] / `PQputCopyEnd`),
    /// so the stream is now `PGASYNC_BUSY` and the server will send the trailing
    /// result(s): for a historic-timeline switch a single-row `PGRES_TUPLES_OK`
    /// (the next timeline) followed by `CommandComplete`, otherwise just
    /// `CommandComplete`, then `ReadyForQuery`. `PQgetResult` here reads ONE
    /// result per call off the wire (delivering `TUPLES_OK`, then `COMMAND_OK`,
    /// then `NULL`), matching libpq's `PQgetResult` exactly — `libpqrcv_endstreaming`
    /// depends on that one-at-a-time delivery.
    Draining,
}

/// A live frontend connection. Owns the byte-stream transport plus the
/// observable connection state libpq exposes (`PQstatus` / `PQerrorMessage` /
/// `PQtransactionStatus` / `PQparameterStatus` / `PQbackendPID`). The C
/// `PGconn` in/out buffers are replaced by the blocking [`Transport`] (we read
/// whole framed messages on demand rather than buffering partial input).
pub struct PgClientConn<T: Transport> {
    /// The byte stream to the backend.
    transport: T,
    /// `conn->status`: whether the connection is usable (`CONNECTION_OK`).
    ok: bool,
    /// `conn->errorMessage`: the last connection-level error text.
    error_message: String,
    /// `conn->xactStatus` (`PQtransactionStatus`).
    xact_status: PgTransactionStatusType,
    /// `conn->pstatus` list (`PQparameterStatus`): server `(name, value)` pairs.
    parameter_status: Vec<(String, String)>,
    /// `conn->be_pid` (`PQbackendPID`).
    be_pid: i32,
    /// `conn->be_key` — the backend cancel key (from BackendKeyData).
    be_key: i32,
    /// Whether a password was actually sent during connect
    /// (`PQconnectionUsedPassword`).
    used_password: bool,
    /// The async COPY sub-state (`conn->asyncStatus`'s COPY values). Set to
    /// [`CopyState::Both`]/[`CopyState::Out`] when a Copy{Both,Out}Response
    /// starts the stream, advanced by [`Self::copy_receive`] when the server's
    /// `CopyDone` ends it, and cleared once the trailing result is drained.
    /// Distinguishing CopyBoth from CopyOut at end-of-stream is what lets us
    /// mirror C's non-blocking `PGRES_COPY_IN` result (see [`CopyState`]).
    copy_state: CopyState,
}

impl<T: Transport> PgClientConn<T> {
    /// `PQstatus(conn) == CONNECTION_OK`.
    pub fn is_ok(&self) -> bool {
        self.ok
    }

    /// Test-only access to the underlying transport, used by the loopback tests
    /// to push additional inbound bytes mid-exchange (modelling a backend that
    /// sends its trailing result only after receiving the client's CopyDone).
    #[cfg(test)]
    pub(crate) fn transport_mut(&mut self) -> &mut T {
        &mut self.transport
    }

    /// `PQerrorMessage(conn)`.
    pub fn error_message(&self) -> &str {
        &self.error_message
    }

    /// `PQtransactionStatus(conn)`.
    pub fn transaction_status(&self) -> PgTransactionStatusType {
        self.xact_status
    }

    /// `PQbackendPID(conn)`.
    pub fn backend_pid(&self) -> i32 {
        self.be_pid
    }

    /// `conn->be_key` — the backend cancel key.
    pub fn backend_key(&self) -> i32 {
        self.be_key
    }

    /// `PQconnectionUsedPassword(conn)`.
    pub fn used_password(&self) -> bool {
        self.used_password
    }

    /// `PQsocket(conn)` — the underlying socket fd (or -1).
    pub fn socket(&self) -> i32 {
        self.transport.raw_fd()
    }

    /// `PQparameterStatus(conn, name)` — look up a reported server parameter.
    pub fn parameter_status(&self, name: &str) -> Option<&str> {
        self.parameter_status
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }

    /// All reported `(name, value)` parameter statuses.
    pub fn parameter_statuses(&self) -> &[(String, String)] {
        &self.parameter_status
    }

    /// `PQserverVersion(conn)` — parse the `server_version` parameter into the
    /// numeric form (major * 10000 + minor) the C `PQserverVersion` returns. A
    /// missing/unparseable value yields 0.
    pub fn server_version(&self) -> i32 {
        let Some(s) = self.parameter_status("server_version") else {
            return 0;
        };
        // The version string is e.g. "18.3" (modern, two-part) or "9.6.24"
        // (legacy three-part). pqSaveParameterStatus computes sversion the same
        // way: cnt = sscanf(value, "%d.%d", &vmaj, &vmin). For >= 10, only the
        // major part is significant and minor is the second component / 0.
        let mut parts = s.split(|c: char| !c.is_ascii_digit());
        let vmaj: i32 = parts.next().and_then(|p| p.parse().ok()).unwrap_or(0);
        if vmaj == 0 {
            return 0;
        }
        if vmaj >= 10 {
            // Two-part scheme: major * 10000 + (minor or 0).
            // C: scanf("%d.%d") -> if cnt==1, vmin=0. Then sversion = vmaj*10000+vmin.
            let vmin: i32 = parts.next().and_then(|p| p.parse().ok()).unwrap_or(0);
            return vmaj * 10000 + vmin;
        }
        // Legacy three-part scheme (major.minor.rev): vmaj*10000 + vmin*100 + rev.
        let vmin: i32 = parts.next().and_then(|p| p.parse().ok()).unwrap_or(0);
        let rev: i32 = parts.next().and_then(|p| p.parse().ok()).unwrap_or(0);
        vmaj * 10000 + vmin * 100 + rev
    }

    // -----------------------------------------------------------------------
    // connect: startup packet + auth/parameter loop (PQconnectPoll tail).
    // -----------------------------------------------------------------------

    /// Establish a connection over `transport`: send the startup packet, then
    /// run the auth/parameter loop to `ReadyForQuery`.
    ///
    /// `params` are the already-resolved startup string fields. `password` is
    /// used only if the backend asks for a cleartext password
    /// (`AUTH_REQ_PASSWORD`); the *trust* path (`AUTH_REQ_OK`) needs none.
    ///
    /// Mirrors `pqBuildStartupPacket3` (assemble) + `pqPacketSend(conn, 0, ...)`
    /// (send, no type byte) + the `CONNECTION_AWAITING_RESPONSE` /
    /// `CONNECTION_AUTH_OK` message handling in `PQconnectPoll`.
    pub fn connect(
        mut transport: T,
        params: &protocol3::StartupParams<'_>,
        password: Option<&str>,
    ) -> Result<Self, TransportError> {
        // Assemble the startup packet body (protocol version word + option
        // pairs + trailing NUL) via the ported assembler.
        let packet = protocol3::build_startup_packet3(params, protocol3::ENVIRONMENT_OPTIONS)
            .map_err(|e| match e {
                protocol3::StartupPacketError::OutOfMemory => TransportError::OutOfMemory,
                protocol3::StartupPacketError::SizeOverflow
                | protocol3::StartupPacketError::TooLong => {
                    TransportError::Io("startup packet too long".to_string())
                }
            })?;

        // pqPacketSend(conn, 0, startpacket, packetlen): no type byte, 4-byte
        // self-inclusive length, then the body.
        let framed = codec::build_startup_message(&packet)?;
        transport.write_all(&framed)?;
        transport.flush()?;

        let mut conn = PgClientConn {
            transport,
            ok: false,
            error_message: String::new(),
            xact_status: PgTransactionStatusType::Unknown,
            parameter_status: Vec::new(),
            be_pid: 0,
            be_key: 0,
            used_password: false,
            copy_state: CopyState::None,
        };

        // The auth / parameter / ready loop: pump messages until ReadyForQuery
        // or a fatal error. This is the CONNECTION_AWAITING_RESPONSE ->
        // CONNECTION_AUTH_OK -> CONNECTION_OK arc of PQconnectPoll, blocking.
        loop {
            let msg = conn.read_message()?;
            match msg.kind {
                codec::B_AUTH => {
                    let mut r = MsgReader::new(&msg.body);
                    let areq = r.get_i32()?;
                    match areq {
                        codec::AUTH_REQ_OK => {
                            // CONNECTION_AUTH_OK: authentication accepted.
                        }
                        codec::AUTH_REQ_PASSWORD => {
                            // Cleartext password requested: send a
                            // PasswordMessage ('p') with the NUL-terminated
                            // password (pg_password_sendauth's plaintext leg).
                            let pw = password.ok_or_else(|| {
                                TransportError::AuthFailed(
                                    "server requested a password but none was provided".to_string(),
                                )
                            })?;
                            let mut body = Vec::new();
                            body.try_reserve(pw.len() + 1)
                                .map_err(|_| TransportError::OutOfMemory)?;
                            body.extend_from_slice(pw.as_bytes());
                            body.push(0);
                            let framed = codec::build_message(codec::F_PASSWORD_MESSAGE, &body)?;
                            conn.transport.write_all(&framed)?;
                            conn.transport.flush()?;
                            conn.used_password = true;
                        }
                        codec::AUTH_REQ_MD5
                        | codec::AUTH_REQ_SASL
                        | codec::AUTH_REQ_SASL_CONT
                        | codec::AUTH_REQ_SASL_FIN
                        | codec::AUTH_REQ_GSS
                        | codec::AUTH_REQ_GSS_CONT
                        | codec::AUTH_REQ_SSPI
                        | codec::AUTH_REQ_KRB4
                        | codec::AUTH_REQ_KRB5 => {
                            // These need the fe-auth.c / fe-auth-scram.c crypto
                            // response loops, deferred behind their loud
                            // unported-auth seam (see registry::request_auth).
                            return Err(TransportError::AuthFailed(format!(
                                "authentication method {areq} not supported by this minimal client \
                                 (only trust / cleartext-password)"
                            )));
                        }
                        other => {
                            return Err(TransportError::AuthFailed(format!(
                                "unknown authentication request {other}"
                            )));
                        }
                    }
                }
                codec::B_PARAMETER_STATUS => {
                    conn.handle_parameter_status(&msg.body)?;
                }
                codec::B_BACKEND_KEY_DATA => {
                    // getBackendKeyData: pid then secret key (both int32).
                    let mut r = MsgReader::new(&msg.body);
                    conn.be_pid = r.get_i32()?;
                    conn.be_key = r.get_i32()?;
                }
                codec::B_NOTICE_RESPONSE => {
                    // A notice during startup: process (consume) and continue.
                    let _ = conn.parse_error_notice(&msg.body, false)?;
                }
                codec::B_READY_FOR_QUERY => {
                    // getReadyForQuery: one byte of xact status. Connection up.
                    conn.handle_ready_for_query(&msg.body)?;
                    conn.ok = true;
                    return Ok(conn);
                }
                codec::B_ERROR_RESPONSE => {
                    // A fatal error during startup (e.g. auth rejected, bad db).
                    let (_sqlstate, message) = conn.parse_error_notice(&msg.body, true)?;
                    conn.ok = false;
                    conn.error_message = message.clone();
                    return Err(TransportError::AuthFailed(message));
                }
                codec::B_NEGOTIATE_PROTOCOL_VERSION => {
                    // pqGetNegotiateProtocolVersion3: the server offers a lower
                    // protocol / unsupported options. We only speak 3.0 with no
                    // protocol extensions, so any negotiation is a hard failure
                    // here (loud, not silent).
                    return Err(TransportError::ProtocolViolation);
                }
                other => {
                    return Err(TransportError::Io(format!(
                        "unexpected message type '{}' during startup",
                        other as char
                    )));
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // simple query (PQexec): send Query, pump to ReadyForQuery.
    // -----------------------------------------------------------------------

    /// `PQexec(conn, query)` — run a simple query and return the (last) result.
    ///
    /// Sends a `Query` ('Q') message with the NUL-terminated SQL text, then
    /// pumps backend messages until `ReadyForQuery`, building a [`PGresult`].
    /// A simple query may produce multiple results (multi-statement strings);
    /// libpq's `PQexec` returns the LAST one (and an error result if any
    /// statement failed). The CopyBoth result (replication) returns
    /// immediately with `PGRES_COPY_BOTH`.
    pub fn exec(&mut self, query: &str) -> Result<PGresult, TransportError> {
        self.send_query(query)?;
        self.collect_result()
    }

    /// Send a `Query` ('Q') message (`PQsendQuery` core).
    pub fn send_query(&mut self, query: &str) -> Result<(), TransportError> {
        // A new command starts: any prior COPY's trailing result is moot.
        self.copy_state = CopyState::None;
        let mut body = Vec::new();
        body.try_reserve(query.len() + 1)
            .map_err(|_| TransportError::OutOfMemory)?;
        body.extend_from_slice(query.as_bytes());
        body.push(0);
        let framed = codec::build_message(codec::F_QUERY, &body)?;
        self.transport.write_all(&framed)?;
        self.transport.flush()?;
        Ok(())
    }

    /// `PQgetResult(conn)` looped to NULL (`PQexecFinish`): pump messages,
    /// building results, until `ReadyForQuery`; return the last result. A
    /// COPY-start (`G`/`H`/`W`) returns immediately so the caller can run the
    /// COPY loop.
    pub fn collect_result(&mut self) -> Result<PGresult, TransportError> {
        let mut current: Option<PGresult> = None;
        let mut last: Option<PGresult> = None;

        loop {
            let msg = self.read_message()?;
            match msg.kind {
                codec::B_ROW_DESCRIPTION => {
                    // getRowDescriptions: start a TUPLES_OK result.
                    let res = self.parse_row_description(&msg.body)?;
                    current = Some(res);
                }
                codec::B_DATA_ROW => {
                    // getAnotherTuple: append a row to the in-progress result.
                    let res = current.as_mut().ok_or(TransportError::ProtocolViolation)?;
                    Self::parse_data_row(&msg.body, res)?;
                }
                codec::B_COMMAND_COMPLETE => {
                    // CommandComplete: finalize the current result. With no
                    // preceding RowDescription it's a COMMAND_OK result.
                    let mut res = current
                        .take()
                        .unwrap_or_else(|| PGresult::make_empty(ExecStatusType::PGRES_COMMAND_OK));
                    let tag = MsgReader::new(&msg.body).get_cstr()?;
                    res.cmd_status = String::from_utf8_lossy(&tag).into_owned();
                    last = Some(res);
                }
                codec::B_EMPTY_QUERY => {
                    // EmptyQueryResponse: an empty query string.
                    let _ = current.take();
                    last = Some(PGresult::make_empty(ExecStatusType::PGRES_EMPTY_QUERY));
                }
                codec::B_ERROR_RESPONSE => {
                    // pqGetErrorNotice3 (isError): build a FATAL_ERROR result.
                    let (sqlstate, message) = self.parse_error_notice(&msg.body, true)?;
                    let mut res = PGresult::make_empty(ExecStatusType::PGRES_FATAL_ERROR);
                    res.err_msg = Some(message.clone());
                    res.sqlstate = sqlstate;
                    self.error_message = message;
                    current = None;
                    last = Some(res);
                }
                codec::B_NOTICE_RESPONSE => {
                    // Processed inline at any point; does not affect results.
                    let _ = self.parse_error_notice(&msg.body, false)?;
                }
                codec::B_NOTIFICATION_RESPONSE => {
                    // getNotify: consumed (LISTEN/NOTIFY); not surfaced in this
                    // minimal client.
                    let _ = self.parse_notification(&msg.body)?;
                }
                codec::B_PARAMETER_STATUS => {
                    // ParameterStatus can arrive mid-stream (e.g. SET).
                    self.handle_parameter_status(&msg.body)?;
                }
                codec::B_COPY_OUT_RESPONSE => {
                    return self.parse_copy_start(&msg.body, ExecStatusType::PGRES_COPY_OUT);
                }
                codec::B_COPY_IN_RESPONSE => {
                    return self.parse_copy_start(&msg.body, ExecStatusType::PGRES_COPY_IN);
                }
                codec::B_COPY_BOTH_RESPONSE => {
                    return self.parse_copy_start(&msg.body, ExecStatusType::PGRES_COPY_BOTH);
                }
                codec::B_READY_FOR_QUERY => {
                    self.handle_ready_for_query(&msg.body)?;
                    // Return the last result, or an empty COMMAND_OK if none was
                    // produced (matching PQexec returning a result object).
                    return Ok(last
                        .unwrap_or_else(|| PGresult::make_empty(ExecStatusType::PGRES_COMMAND_OK)));
                }
                other => {
                    return Err(TransportError::Io(format!(
                        "unexpected message type '{}' during query",
                        other as char
                    )));
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // CopyBoth streaming (replication): START_REPLICATION + CopyData exchange.
    // -----------------------------------------------------------------------

    /// Run a `START_REPLICATION` (or any COPY-producing) command and expect a
    /// CopyBoth handshake. Returns the `PGRES_COPY_BOTH` result on success;
    /// otherwise the command's result, exactly as `libpqrcv_startstreaming`
    /// inspects the status.
    pub fn start_replication(&mut self, command: &str) -> Result<PGresult, TransportError> {
        self.exec(command)
    }

    /// `PQgetCopyData(conn, &buffer, async=1)` for the replication COPY-out
    /// stream. Reads the next `CopyData` ('d') frame and returns its payload,
    /// without blocking when no complete message is buffered.
    ///
    /// Returns [`CopyRecv::Data`] for a CopyData payload, [`CopyRecv::Done`] for
    /// `CopyDone` (the server ended the COPY — collect the trailing result via
    /// [`Self::end_copy`]), or [`CopyRecv::WouldBlock`] when the socket has no
    /// data ready (the C async `PQgetCopyData` returning 0). Mirrors
    /// `getCopyDataMessage`'s dispatch over `pqReadReady`.
    pub fn copy_receive(&mut self) -> Result<CopyRecv, TransportError> {
        loop {
            // Async semantics: if nothing is ready to read, don't block — tell
            // the caller to wait on the socket (the walreceiver's WaitLatch).
            // Once a message header is available the rest of that message is
            // read to completion (the peer frames whole messages).
            if !self.transport.read_ready() {
                return Ok(CopyRecv::WouldBlock);
            }
            let msg = self.read_message()?;
            match msg.kind {
                codec::B_COPY_DATA => {
                    // The whole body is the CopyData payload.
                    return Ok(CopyRecv::Data(msg.body));
                }
                codec::B_COPY_DONE => {
                    // The server ended the COPY. Mirror `getCopyDataMessage`'s
                    // CopyDone transition (fe-protocol3.c):
                    //
                    //   * in COPY_BOTH (replication) we move to COPY_IN — we now
                    //     owe the peer our own CopyDone before it sends the
                    //     trailing CommandComplete/ReadyForQuery, so the next
                    //     `PQgetResult` must yield a `PGRES_COPY_IN` result with
                    //     NO wire read (otherwise we deadlock: the server is
                    //     waiting on our CopyDone while we wait on its result —
                    //     e.g. an end-of-timeline switch);
                    //   * in plain COPY_OUT we move to "busy" with the trailing
                    //     result already on the wire, so the next `PQgetResult`
                    //     collects it.
                    self.copy_state = match self.copy_state {
                        CopyState::Both => CopyState::In,
                        _ => CopyState::OutEnded,
                    };
                    return Ok(CopyRecv::Done);
                }
                codec::B_NOTICE_RESPONSE => {
                    // getCopyDataMessage processes async notices inline.
                    let _ = self.parse_error_notice(&msg.body, false)?;
                }
                codec::B_NOTIFICATION_RESPONSE => {
                    let _ = self.parse_notification(&msg.body)?;
                }
                codec::B_PARAMETER_STATUS => {
                    self.handle_parameter_status(&msg.body)?;
                }
                codec::B_ERROR_RESPONSE => {
                    let (_sqlstate, message) = self.parse_error_notice(&msg.body, true)?;
                    self.error_message = message.clone();
                    return Err(TransportError::Io(message));
                }
                other => {
                    return Err(TransportError::Io(format!(
                        "unexpected message type '{}' during COPY",
                        other as char
                    )));
                }
            }
        }
    }

    /// `PQputCopyData(conn, buffer, nbytes)` — send a `CopyData` ('d') frame.
    pub fn copy_send(&mut self, payload: &[u8]) -> Result<(), TransportError> {
        let framed = codec::build_message(codec::F_COPY_DATA, payload)?;
        self.transport.write_all(&framed)?;
        self.transport.flush()?;
        Ok(())
    }

    /// `PQputCopyEnd(conn, NULL)` — send a `CopyDone` ('c') frame ending our
    /// side of the COPY stream. As in libpq's `PQputCopyEnd`, sending our
    /// CopyDone moves the async status out of the COPY states to "busy": the
    /// server will now send the trailing result(s), which the next
    /// `PQgetResult` reads off the wire (see [`CopyState::Draining`]).
    pub fn copy_done(&mut self) -> Result<(), TransportError> {
        let framed = codec::build_message(codec::F_COPY_DONE, &[])?;
        self.transport.write_all(&framed)?;
        self.transport.flush()?;
        // Only a stream we were actively in transitions to draining; a stray
        // copy_done outside a COPY leaves the state untouched.
        if matches!(
            self.copy_state,
            CopyState::In | CopyState::Both | CopyState::Out | CopyState::OutEnded
        ) {
            self.copy_state = CopyState::Draining;
        }
        Ok(())
    }

    /// `PQflush(conn)` — flush buffered writes.
    pub fn flush(&mut self) -> Result<(), TransportError> {
        self.transport.flush()
    }

    /// After receiving `CopyDone` from the server (`copy_receive` returned
    /// `Done`), collect the trailing result up to `ReadyForQuery` (the trailing
    /// `CommandComplete` then `ReadyForQuery`). This is the convenience form used
    /// where the trailing result is already on the wire; the replication path
    /// instead sends its own `CopyDone` ([`Self::copy_done`]) and then reads the
    /// results one at a time via [`Self::get_result_after_copy`].
    pub fn end_copy(&mut self) -> Result<PGresult, TransportError> {
        self.copy_state = CopyState::None;
        self.collect_result()
    }

    /// `PQgetResult(conn)` as the WAL receiver's end-of-COPY path uses it
    /// (`libpqrcv_receive` after `PQgetCopyData` returns -1, and
    /// `libpqrcv_endstreaming`). Mirrors `PQgetResult`'s dispatch over the COPY
    /// `asyncStatus` values:
    ///
    ///   * [`CopyState::In`] (reached after the server's CopyDone ended a
    ///     CopyBoth stream): return a `PGRES_COPY_IN` result **without any wire
    ///     I/O**, exactly as C's `getCopyResult(conn, PGRES_COPY_IN)`. The
    ///     trailing `CommandComplete`/`ReadyForQuery` are NOT sent until we reply
    ///     with our own `CopyDone` ([`Self::copy_done`]), so reading here would
    ///     deadlock. We stay in `In` until `copy_done` is sent (after which
    ///     `collect_result`/`end_copy` drains the real trailing results).
    ///   * [`CopyState::OutEnded`] (a plain COPY-out ended): the trailing result
    ///     is already on the wire — collect and return it, then drop to
    ///     [`CopyState::None`].
    ///   * otherwise: nothing is queued, return `None` (C's `PQgetResult`
    ///     returning NULL once the result queue is drained).
    pub fn get_result_after_copy(&mut self) -> Result<Option<PGresult>, TransportError> {
        match self.copy_state {
            CopyState::In => {
                // getCopyResult(conn, PGRES_COPY_IN): a synthesized result, no
                // socket read. asyncStatus stays COPY_IN.
                Ok(Some(PGresult::make_empty(ExecStatusType::PGRES_COPY_IN)))
            }
            CopyState::OutEnded => {
                self.copy_state = CopyState::None;
                Ok(Some(self.collect_result()?))
            }
            CopyState::Draining => {
                // Post-PQputCopyEnd: deliver the trailing results one at a time,
                // exactly as C's PQgetResult does. `None` (the terminating NULL,
                // at ReadyForQuery) drops us back to the no-COPY state.
                let r = self.get_one_result()?;
                if r.is_none() {
                    self.copy_state = CopyState::None;
                }
                Ok(r)
            }
            CopyState::None | CopyState::Out | CopyState::Both => Ok(None),
        }
    }

    /// `PQgetResult(conn)` for a single result: pump messages, building exactly
    /// ONE result, and return it; return `None` once `ReadyForQuery` is reached
    /// with no further result (the C `PQgetResult` returning NULL). This is the
    /// one-result-at-a-time form `libpqrcv_endstreaming` relies on (it reads the
    /// next-timeline `PGRES_TUPLES_OK`, then the `CommandComplete`, then the
    /// terminating NULL as separate calls), as opposed to [`Self::collect_result`]
    /// which drains the whole command sequence and returns only the last result.
    fn get_one_result(&mut self) -> Result<Option<PGresult>, TransportError> {
        let mut current: Option<PGresult> = None;
        loop {
            let msg = self.read_message()?;
            match msg.kind {
                codec::B_ROW_DESCRIPTION => {
                    current = Some(self.parse_row_description(&msg.body)?);
                }
                codec::B_DATA_ROW => {
                    let res = current.as_mut().ok_or(TransportError::ProtocolViolation)?;
                    Self::parse_data_row(&msg.body, res)?;
                }
                codec::B_COMMAND_COMPLETE => {
                    // CommandComplete completes the in-progress result (tagging a
                    // TUPLES_OK), or, with none pending, is itself a COMMAND_OK
                    // result. Either way it ends ONE result — return it.
                    let mut res = current
                        .take()
                        .unwrap_or_else(|| PGresult::make_empty(ExecStatusType::PGRES_COMMAND_OK));
                    let tag = MsgReader::new(&msg.body).get_cstr()?;
                    res.cmd_status = String::from_utf8_lossy(&tag).into_owned();
                    return Ok(Some(res));
                }
                codec::B_EMPTY_QUERY => {
                    return Ok(Some(PGresult::make_empty(ExecStatusType::PGRES_EMPTY_QUERY)));
                }
                codec::B_ERROR_RESPONSE => {
                    let (sqlstate, message) = self.parse_error_notice(&msg.body, true)?;
                    let mut res = PGresult::make_empty(ExecStatusType::PGRES_FATAL_ERROR);
                    res.err_msg = Some(message.clone());
                    res.sqlstate = sqlstate;
                    self.error_message = message;
                    return Ok(Some(res));
                }
                codec::B_READY_FOR_QUERY => {
                    // End of the command sequence. If a result was somehow still
                    // in progress (a RowDescription with no terminating
                    // CommandComplete — the backend does not actually do this)
                    // deliver it; otherwise this is the terminating NULL.
                    self.handle_ready_for_query(&msg.body)?;
                    return Ok(current.take());
                }
                codec::B_NOTICE_RESPONSE => {
                    let _ = self.parse_error_notice(&msg.body, false)?;
                }
                codec::B_NOTIFICATION_RESPONSE => {
                    let _ = self.parse_notification(&msg.body)?;
                }
                codec::B_PARAMETER_STATUS => {
                    self.handle_parameter_status(&msg.body)?;
                }
                codec::B_COPY_OUT_RESPONSE => {
                    return Ok(Some(
                        self.parse_copy_start(&msg.body, ExecStatusType::PGRES_COPY_OUT)?,
                    ));
                }
                codec::B_COPY_IN_RESPONSE => {
                    return Ok(Some(
                        self.parse_copy_start(&msg.body, ExecStatusType::PGRES_COPY_IN)?,
                    ));
                }
                codec::B_COPY_BOTH_RESPONSE => {
                    return Ok(Some(
                        self.parse_copy_start(&msg.body, ExecStatusType::PGRES_COPY_BOTH)?,
                    ));
                }
                other => {
                    return Err(TransportError::Io(format!(
                        "unexpected message type '{}' during result", other as char
                    )));
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // finish (sendTerminateConn).
    // -----------------------------------------------------------------------

    /// `PQfinish(conn)` — send a `Terminate` ('X') message and consume the
    /// connection (the transport is dropped). Errors sending the terminate are
    /// ignored (the C path closes the socket regardless).
    pub fn finish(mut self) {
        if let Ok(framed) = codec::build_message(codec::F_TERMINATE, &[]) {
            let _ = self.transport.write_all(&framed);
            let _ = self.transport.flush();
        }
        // `self` (and its transport) dropped here.
    }

    // -----------------------------------------------------------------------
    // Message framing read: one complete backend message off the wire.
    // -----------------------------------------------------------------------

    /// Read one complete framed backend message: 1 type byte, a 4-byte BE
    /// length (`>= 4`, covers the length field but not the type byte), then
    /// `length - 4` body bytes. Mirrors the `pqGetc(id)` + `pqGetInt(msgLength,
    /// 4)` + body-availability check at the top of `pqParseInput3`, but blocking.
    fn read_message(&mut self) -> Result<BackendMessage, TransportError> {
        let mut header = [0u8; 5];
        self.transport.read_exact(&mut header)?;
        let kind = header[0];
        let msg_length = i32::from_be_bytes([header[1], header[2], header[3], header[4]]);
        // A length < 4 is definitely broken (handleSyncLoss).
        if msg_length < 4 {
            return Err(TransportError::ProtocolViolation);
        }
        let body_len = (msg_length - 4) as usize;
        let mut body = Vec::new();
        body.try_reserve(body_len)
            .map_err(|_| TransportError::OutOfMemory)?;
        body.resize(body_len, 0u8);
        self.transport.read_exact(&mut body)?;
        Ok(BackendMessage { kind, body })
    }

    // -----------------------------------------------------------------------
    // Per-message-type readers (the fe-protocol3.c get* subroutines).
    // -----------------------------------------------------------------------

    /// `getRowDescriptions` — build a TUPLES_OK result with `nfields` attribute
    /// descriptors read from a 'T' message.
    fn parse_row_description(&mut self, body: &[u8]) -> Result<PGresult, TransportError> {
        let mut r = MsgReader::new(body);
        let nfields = r.get_u16()? as usize;
        let mut att_descs: Vec<PgResAttDesc> = Vec::new();
        att_descs
            .try_reserve(nfields)
            .map_err(|_| TransportError::OutOfMemory)?;
        // result->binary set true only if ALL columns are binary.
        let mut all_binary = nfields > 0;
        for _ in 0..nfields {
            let name = r.get_cstr()?;
            let tableid = r.get_u32()?;
            let columnid = r.get_i16()?;
            let typid = r.get_u32()?;
            let typlen = r.get_i16()?;
            let atttypmod = r.get_i32()?;
            let format = r.get_i16()?;
            if format != 1 {
                all_binary = false;
            }
            att_descs.push(PgResAttDesc {
                name: String::from_utf8_lossy(&name).into_owned(),
                tableid,
                columnid,
                format,
                typid,
                typlen,
                atttypmod,
            });
        }
        let mut res = PGresult::make_empty(ExecStatusType::PGRES_TUPLES_OK);
        res.att_descs = att_descs;
        res.binary = if all_binary { 1 } else { 0 };
        Ok(res)
    }

    /// `getAnotherTuple` — read one 'D' DataRow and append it to `res`. Each
    /// field is a 4-byte length (`-1` == SQL NULL) then that many value bytes.
    fn parse_data_row(body: &[u8], res: &mut PGresult) -> Result<(), TransportError> {
        let mut r = MsgReader::new(body);
        let nfields = res.att_descs.len();
        let tupnfields = r.get_u16()? as usize;
        // C: unexpected field count in "D" message -> error.
        if tupnfields != nfields {
            return Err(TransportError::ProtocolViolation);
        }
        let mut row: Vec<PgResAttValue> = Vec::new();
        row.try_reserve(nfields)
            .map_err(|_| TransportError::OutOfMemory)?;
        for _ in 0..nfields {
            let vlen = r.get_i32()?;
            if vlen < 0 {
                // NULL_LEN: SQL NULL -> None.
                row.push(PgResAttValue { value: None });
            } else {
                let bytes = r.get_nbytes(vlen as usize)?;
                row.push(PgResAttValue { value: Some(bytes) });
            }
        }
        res.tuples
            .try_reserve(1)
            .map_err(|_| TransportError::OutOfMemory)?;
        res.tuples.push(row);
        Ok(())
    }

    /// `getCopyStart` — read a 'G'/'H'/'W' Copy{In,Out,Both}Response into a
    /// result of the matching status. Body: `copy_is_binary` (1 byte), `nfields`
    /// (i16), then `nfields` per-column format codes (i16 each).
    fn parse_copy_start(
        &mut self,
        body: &[u8],
        copytype: ExecStatusType,
    ) -> Result<PGresult, TransportError> {
        let mut r = MsgReader::new(body);
        let copy_is_binary = r.get_u8()?;
        let nfields = r.get_u16()? as usize;
        let mut att_descs: Vec<PgResAttDesc> = Vec::new();
        att_descs
            .try_reserve(nfields)
            .map_err(|_| TransportError::OutOfMemory)?;
        for _ in 0..nfields {
            let format = r.get_i16()?;
            att_descs.push(PgResAttDesc {
                format,
                ..Default::default()
            });
        }
        // Enter the matching async COPY sub-state, the way libpq sets
        // `conn->asyncStatus` from a Copy{Out,In,Both}Response. This is what lets
        // `copy_receive`'s end-of-stream handling know whether a `CopyDone`
        // leaves us owing the peer a `CopyDone` (CopyBoth -> COPY_IN) or whether
        // the trailing result is already queued (plain CopyOut).
        self.copy_state = match copytype {
            ExecStatusType::PGRES_COPY_OUT => CopyState::Out,
            ExecStatusType::PGRES_COPY_BOTH => CopyState::Both,
            ExecStatusType::PGRES_COPY_IN => CopyState::In,
            _ => CopyState::None,
        };
        let mut res = PGresult::make_empty(copytype);
        res.att_descs = att_descs;
        res.binary = copy_is_binary as i32;
        Ok(res)
    }

    /// `pqGetErrorNotice3` — read an 'E'/'N' message's `(code, value)` field
    /// pairs (terminated by a `\0` code byte). Returns `(sqlstate, primary
    /// message)`. `_is_error` mirrors the C parameter (notice vs error); the
    /// field parsing is identical.
    fn parse_error_notice(
        &mut self,
        body: &[u8],
        _is_error: bool,
    ) -> Result<(Option<String>, String), TransportError> {
        let mut r = MsgReader::new(body);
        let mut sqlstate: Option<String> = None;
        let mut primary: Option<String> = None;
        let mut severity: Option<String> = None;
        loop {
            let code = r.get_u8()?;
            if code == 0 {
                break; // terminator
            }
            let value = r.get_cstr()?;
            let value = String::from_utf8_lossy(&value).into_owned();
            match code {
                codec::PG_DIAG_SQLSTATE => sqlstate = Some(value),
                codec::PG_DIAG_MESSAGE_PRIMARY => primary = Some(value),
                codec::PG_DIAG_SEVERITY => severity = Some(value),
                _ => {} // other diag fields not surfaced in this minimal client
            }
        }
        // Build a "severity: message" line, the gist of pqBuildErrorMessage3's
        // default verbosity output (severity + primary).
        let message = match (severity, primary) {
            (Some(sev), Some(msg)) => format!("{sev}:  {msg}"),
            (None, Some(msg)) => msg,
            (Some(sev), None) => sev,
            (None, None) => String::new(),
        };
        Ok((sqlstate, message))
    }

    /// `getNotify` — read an 'A' NotificationResponse: be_pid (i32), relname
    /// (cstr), extra (cstr). Consumed but not surfaced in this minimal client.
    fn parse_notification(&mut self, body: &[u8]) -> Result<(), TransportError> {
        let mut r = MsgReader::new(body);
        let _be_pid = r.get_i32()?;
        let _relname = r.get_cstr()?;
        let _extra = r.get_cstr()?;
        Ok(())
    }

    /// `getParameterStatus` — read an 'S' ParameterStatus: name (cstr), value
    /// (cstr); record into `parameter_status` (overwriting an existing key, as
    /// `pqSaveParameterStatus` does).
    fn handle_parameter_status(&mut self, body: &[u8]) -> Result<(), TransportError> {
        let mut r = MsgReader::new(body);
        let name = String::from_utf8_lossy(&r.get_cstr()?).into_owned();
        let value = String::from_utf8_lossy(&r.get_cstr()?).into_owned();
        if let Some(slot) = self.parameter_status.iter_mut().find(|(k, _)| *k == name) {
            slot.1 = value;
        } else {
            self.parameter_status
                .try_reserve(1)
                .map_err(|_| TransportError::OutOfMemory)?;
            self.parameter_status.push((name, value));
        }
        Ok(())
    }

    /// `getReadyForQuery` — read a 'Z' ReadyForQuery: one byte of transaction
    /// status ('I'/'T'/'E'); set `xactStatus`.
    fn handle_ready_for_query(&mut self, body: &[u8]) -> Result<(), TransportError> {
        let mut r = MsgReader::new(body);
        let xact = r.get_u8()?;
        self.xact_status = match xact {
            b'I' => PgTransactionStatusType::Idle,
            b'T' => PgTransactionStatusType::Intrans,
            b'E' => PgTransactionStatusType::Inerror,
            _ => PgTransactionStatusType::Unknown,
        };
        Ok(())
    }
}
