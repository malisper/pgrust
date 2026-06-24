//! In-process loopback tests: drive the real [`PgClientConn`] state machine
//! against a scripted mock backend (a queue of pre-framed backend messages),
//! plus a real-OS `socketpair` round-trip exercising the [`SocketTransport`].

use crate::client::PgClientConn;
use crate::codec::{self, build_message};
use crate::protocol3::{StartupParams, PG_PROTOCOL_3_0};
use crate::result::{ExecStatusType, PgTransactionStatusType};
use crate::transport::{Transport, TransportError};

// ---------------------------------------------------------------------------
// A scripted mock backend transport: reads come from a queue of bytes the test
// pre-fills with framed backend messages; writes are captured for assertions.
// ---------------------------------------------------------------------------

struct MockBackend {
    inbound: Vec<u8>, // bytes the client will read (server->client)
    read_pos: usize,
    outbound: Vec<u8>, // bytes the client wrote (client->server)
}

impl MockBackend {
    fn new(inbound: Vec<u8>) -> Self {
        MockBackend {
            inbound,
            read_pos: 0,
            outbound: Vec::new(),
        }
    }
}

impl Transport for MockBackend {
    fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), TransportError> {
        let end = self.read_pos + buf.len();
        if end > self.inbound.len() {
            return Err(TransportError::Io("mock backend: EOF".to_string()));
        }
        buf.copy_from_slice(&self.inbound[self.read_pos..end]);
        self.read_pos = end;
        Ok(())
    }
    fn write_all(&mut self, buf: &[u8]) -> Result<(), TransportError> {
        self.outbound.extend_from_slice(buf);
        Ok(())
    }
    fn flush(&mut self) -> Result<(), TransportError> {
        Ok(())
    }
}

// Helpers to build framed backend messages.
fn msg(kind: u8, body: &[u8]) -> Vec<u8> {
    build_message(kind, body).unwrap()
}

fn auth_ok() -> Vec<u8> {
    msg(codec::B_AUTH, &codec::AUTH_REQ_OK.to_be_bytes())
}

fn ready(xact: u8) -> Vec<u8> {
    msg(codec::B_READY_FOR_QUERY, &[xact])
}

fn param_status(name: &str, value: &str) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(name.as_bytes());
    body.push(0);
    body.extend_from_slice(value.as_bytes());
    body.push(0);
    msg(codec::B_PARAMETER_STATUS, &body)
}

fn backend_key(pid: i32, key: i32) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&pid.to_be_bytes());
    body.extend_from_slice(&key.to_be_bytes());
    msg(codec::B_BACKEND_KEY_DATA, &body)
}

fn row_desc_one(name: &str, typid: u32) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&1u16.to_be_bytes()); // nfields
    body.extend_from_slice(name.as_bytes());
    body.push(0);
    body.extend_from_slice(&0u32.to_be_bytes()); // tableid
    body.extend_from_slice(&0u16.to_be_bytes()); // columnid
    body.extend_from_slice(&typid.to_be_bytes()); // typid
    body.extend_from_slice(&(-1i16).to_be_bytes()); // typlen
    body.extend_from_slice(&(-1i32).to_be_bytes()); // atttypmod
    body.extend_from_slice(&0u16.to_be_bytes()); // format = text
    msg(codec::B_ROW_DESCRIPTION, &body)
}

fn data_row_one(value: Option<&[u8]>) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&1u16.to_be_bytes()); // nfields
    match value {
        Some(v) => {
            body.extend_from_slice(&(v.len() as i32).to_be_bytes());
            body.extend_from_slice(v);
        }
        None => body.extend_from_slice(&(-1i32).to_be_bytes()),
    }
    msg(codec::B_DATA_ROW, &body)
}

fn command_complete(tag: &str) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(tag.as_bytes());
    body.push(0);
    msg(codec::B_COMMAND_COMPLETE, &body)
}

fn error_response(severity: &str, sqlstate: &str, message: &str) -> Vec<u8> {
    let mut body = Vec::new();
    body.push(codec::PG_DIAG_SEVERITY);
    body.extend_from_slice(severity.as_bytes());
    body.push(0);
    body.push(codec::PG_DIAG_SQLSTATE);
    body.extend_from_slice(sqlstate.as_bytes());
    body.push(0);
    body.push(codec::PG_DIAG_MESSAGE_PRIMARY);
    body.extend_from_slice(message.as_bytes());
    body.push(0);
    body.push(0); // terminator
    msg(codec::B_ERROR_RESPONSE, &body)
}

fn copy_both(nfields: u16) -> Vec<u8> {
    let mut body = Vec::new();
    body.push(0); // copy_is_binary = text
    body.extend_from_slice(&nfields.to_be_bytes());
    for _ in 0..nfields {
        body.extend_from_slice(&0u16.to_be_bytes()); // format text
    }
    msg(codec::B_COPY_BOTH_RESPONSE, &body)
}

fn copy_data(payload: &[u8]) -> Vec<u8> {
    msg(codec::B_COPY_DATA, payload)
}

fn copy_done() -> Vec<u8> {
    msg(codec::B_COPY_DONE, &[])
}

// ---------------------------------------------------------------------------
// connect tests.
// ---------------------------------------------------------------------------

fn default_params() -> StartupParams<'static> {
    StartupParams {
        pversion: PG_PROTOCOL_3_0,
        pguser: Some("postgres"),
        db_name: Some("postgres"),
        ..Default::default()
    }
}

#[test]
fn connect_trust_path() {
    let mut inbound = Vec::new();
    inbound.extend(auth_ok());
    inbound.extend(param_status("server_version", "18.3"));
    inbound.extend(backend_key(4242, 99));
    inbound.extend(ready(b'I'));

    let backend = MockBackend::new(inbound);
    let conn = PgClientConn::connect(backend, &default_params(), None).unwrap();

    assert!(conn.is_ok());
    assert_eq!(conn.backend_pid(), 4242);
    assert_eq!(conn.transaction_status(), PgTransactionStatusType::Idle);
    assert_eq!(conn.parameter_status("server_version"), Some("18.3"));
    assert_eq!(conn.server_version(), 180003);
    assert!(!conn.used_password());
}

#[test]
fn connect_cleartext_password() {
    let mut inbound = Vec::new();
    inbound.extend(msg(codec::B_AUTH, &codec::AUTH_REQ_PASSWORD.to_be_bytes()));
    inbound.extend(auth_ok());
    inbound.extend(ready(b'I'));

    let backend = MockBackend::new(inbound);
    let conn = PgClientConn::connect(backend, &default_params(), Some("secret")).unwrap();
    assert!(conn.is_ok());
    assert!(conn.used_password());
}

#[test]
fn connect_md5_is_loud_not_silent() {
    let inbound = msg(codec::B_AUTH, &codec::AUTH_REQ_MD5.to_be_bytes());
    let backend = MockBackend::new(inbound);
    match PgClientConn::connect(backend, &default_params(), Some("pw")) {
        Err(TransportError::AuthFailed(_)) => {}
        Err(other) => panic!("expected AuthFailed, got {other:?}"),
        Ok(_) => panic!("expected MD5 auth to fail loudly"),
    }
}

#[test]
fn connect_error_response_fails() {
    let mut inbound = Vec::new();
    inbound.extend(error_response(
        "FATAL",
        "28P01",
        "password authentication failed",
    ));
    let backend = MockBackend::new(inbound);
    match PgClientConn::connect(backend, &default_params(), None) {
        Err(TransportError::AuthFailed(m)) => {
            assert!(m.contains("password authentication failed"))
        }
        Err(other) => panic!("expected AuthFailed, got {other:?}"),
        Ok(_) => panic!("expected error response to fail connect"),
    }
}

// ---------------------------------------------------------------------------
// simple-query tests.
// ---------------------------------------------------------------------------

fn connected_backend(query_replies: Vec<u8>) -> PgClientConn<MockBackend> {
    let mut inbound = Vec::new();
    inbound.extend(auth_ok());
    inbound.extend(ready(b'I'));
    inbound.extend(query_replies);
    let backend = MockBackend::new(inbound);
    PgClientConn::connect(backend, &default_params(), None).unwrap()
}

#[test]
fn exec_select_one_row() {
    let mut replies = Vec::new();
    replies.extend(row_desc_one("relname", 19));
    replies.extend(data_row_one(Some(b"pg_class")));
    replies.extend(command_complete("SELECT 1"));
    replies.extend(ready(b'I'));

    let mut conn = connected_backend(replies);
    let res = conn.exec("SELECT relname FROM pg_class LIMIT 1").unwrap();

    assert_eq!(res.result_status(), ExecStatusType::PGRES_TUPLES_OK);
    assert_eq!(res.nfields(), 1);
    assert_eq!(res.ntuples(), 1);
    assert_eq!(res.fname(0), Some("relname"));
    assert_eq!(res.get_value(0, 0), b"pg_class");
    assert!(!res.get_isnull(0, 0));
    assert_eq!(res.cmd_status, "SELECT 1");
}

#[test]
fn exec_null_value() {
    let mut replies = Vec::new();
    replies.extend(row_desc_one("x", 23));
    replies.extend(data_row_one(None));
    replies.extend(command_complete("SELECT 1"));
    replies.extend(ready(b'I'));

    let mut conn = connected_backend(replies);
    let res = conn.exec("SELECT NULL").unwrap();
    assert!(res.get_isnull(0, 0));
    assert_eq!(res.get_length(0, 0), 0);
}

#[test]
fn exec_command_ok() {
    let mut replies = Vec::new();
    replies.extend(command_complete("CREATE TABLE"));
    replies.extend(ready(b'I'));

    let mut conn = connected_backend(replies);
    let res = conn.exec("CREATE TABLE t (x int)").unwrap();
    assert_eq!(res.result_status(), ExecStatusType::PGRES_COMMAND_OK);
    assert_eq!(res.cmd_status, "CREATE TABLE");
}

#[test]
fn exec_error_result_carries_sqlstate() {
    let mut replies = Vec::new();
    replies.extend(error_response("ERROR", "42P01", "relation \"nope\" does not exist"));
    replies.extend(ready(b'E'));

    let mut conn = connected_backend(replies);
    let res = conn.exec("SELECT * FROM nope").unwrap();
    assert_eq!(res.result_status(), ExecStatusType::PGRES_FATAL_ERROR);
    assert_eq!(res.sqlstate.as_deref(), Some("42P01"));
    assert!(res.err_msg.as_deref().unwrap().contains("does not exist"));
}

// ---------------------------------------------------------------------------
// CopyBoth (replication) streaming.
// ---------------------------------------------------------------------------

#[test]
fn copy_both_stream() {
    let mut replies = Vec::new();
    replies.extend(copy_both(0));
    let mut conn = connected_backend(replies.clone());

    let start = conn.start_replication("START_REPLICATION 0/0").unwrap();
    assert_eq!(start.result_status(), ExecStatusType::PGRES_COPY_BOTH);

    // Now feed CopyData frames + CopyDone + trailing CommandComplete/Ready by
    // building a fresh connected backend whose query replies are the copy
    // stream (the connect handshake already consumed).
    let mut stream = Vec::new();
    stream.extend(copy_both(0));
    stream.extend(copy_data(b"WAL-CHUNK-1"));
    stream.extend(copy_data(b"WAL-CHUNK-2"));
    stream.extend(copy_done());
    stream.extend(command_complete("COPY 0"));
    stream.extend(ready(b'I'));

    let mut conn = connected_backend(stream);
    let start = conn.start_replication("START_REPLICATION 0/0").unwrap();
    assert_eq!(start.result_status(), ExecStatusType::PGRES_COPY_BOTH);

    match conn.copy_receive().unwrap() {
        crate::client::CopyRecv::Data(d) => assert_eq!(&d[..], &b"WAL-CHUNK-1"[..]),
        _ => panic!("expected CopyData WAL-CHUNK-1"),
    }
    match conn.copy_receive().unwrap() {
        crate::client::CopyRecv::Data(d) => assert_eq!(&d[..], &b"WAL-CHUNK-2"[..]),
        _ => panic!("expected CopyData WAL-CHUNK-2"),
    }
    assert!(matches!(conn.copy_receive().unwrap(), crate::client::CopyRecv::Done)); // CopyDone
    let trailing = conn.end_copy().unwrap();
    assert_eq!(trailing.result_status(), ExecStatusType::PGRES_COMMAND_OK);
}

// ---------------------------------------------------------------------------
// Real-OS socketpair: SocketTransport over a connected stream pair.
// ---------------------------------------------------------------------------

#[test]
fn socketpair_connect_and_select() {
    use crate::transport::SocketTransport;
    use std::io::{Read, Write};
    use std::os::fd::AsRawFd;
    use std::os::unix::net::UnixStream;
    use std::thread;

    let (client_sock, mut server_sock) = UnixStream::pair().unwrap();
    let client_fd = client_sock.as_raw_fd();

    // Mock backend thread: read the startup packet, reply with the connect
    // handshake, then read a Query and reply with one row + CommandComplete.
    let server = thread::spawn(move || {
        // Read startup: 4-byte length (self-inclusive) then body.
        let mut len_buf = [0u8; 4];
        server_sock.read_exact(&mut len_buf).unwrap();
        let total = i32::from_be_bytes(len_buf) as usize;
        let mut body = vec![0u8; total - 4];
        server_sock.read_exact(&mut body).unwrap();

        // Connect handshake.
        let mut out = Vec::new();
        out.extend(auth_ok());
        out.extend(param_status("server_version", "18.3"));
        out.extend(ready(b'I'));
        server_sock.write_all(&out).unwrap();
        server_sock.flush().unwrap();

        // Read the Query message: 'Q' + len + body.
        let mut hdr = [0u8; 5];
        server_sock.read_exact(&mut hdr).unwrap();
        assert_eq!(hdr[0], codec::F_QUERY);
        let qlen = i32::from_be_bytes([hdr[1], hdr[2], hdr[3], hdr[4]]) as usize;
        let mut qbody = vec![0u8; qlen - 4];
        server_sock.read_exact(&mut qbody).unwrap();

        // Reply with one row.
        let mut reply = Vec::new();
        reply.extend(row_desc_one("answer", 23));
        reply.extend(data_row_one(Some(b"42")));
        reply.extend(command_complete("SELECT 1"));
        reply.extend(ready(b'I'));
        server_sock.write_all(&reply).unwrap();
        server_sock.flush().unwrap();
    });

    let transport = SocketTransport::new(client_sock, client_fd);
    let mut conn = PgClientConn::connect(transport, &default_params(), None).unwrap();
    assert!(conn.is_ok());
    assert_eq!(conn.socket(), client_fd);

    let res = conn.exec("SELECT 42").unwrap();
    assert_eq!(res.result_status(), ExecStatusType::PGRES_TUPLES_OK);
    assert_eq!(res.get_value(0, 0), b"42");

    server.join().unwrap();
}
