use std::cell::RefCell;
use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Mutex, Once};

use init_small::globals as g;
use ip::SockAddr;
use types_core::PGINVALID_SOCKET;
use types_error::{make_sqlstate, PgError, FATAL};
use types_startup::ClientSocket;

use crate::*;

static GUC_LOCK: Mutex<()> = Mutex::new(());

thread_local! {
    static CAPTURED: RefCell<Vec<PgError>> = const { RefCell::new(Vec::new()) };
}

fn capture_hook(error: &PgError, _output_to_server: &mut bool) {
    CAPTURED.with(|c| c.borrow_mut().push(error.clone()));
}

fn install() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        waitevent_seams::pgstat_report_wait_start::set(|_| {});
        waitevent_seams::pgstat_report_wait_end::set(|| {});
        pgstat_seams::pgstat_set_session_end_cause_fatal::set(|| {});
        ipc_seams::proc_exit::set(|code, _pid| panic!("proc_exit({code})"));
        ipc_seams::on_proc_exit::set(|_callback, _arg| {});
        miscinit_seams::create_socket_lock_file::set(|_, _, _| Ok(()));
        postgres_seams::process_client_read_interrupt::set(|_| Ok(()));
        postgres_seams::process_client_write_interrupt::set(|_| Ok(()));
        postgres_seams::check_for_interrupts::set(|| Ok(()));
        acl_seams::get_role_oid::set(|_, _| Ok(0));
        // elog::init_seams also claims the ExitOnAnyError GUC slot that
        // init_small installs; ereport works unseamed here.
        guc_tables::init_seams();
        init_small::init_seams();
        waiteventset::init_seams();
        latch::init_seams();
        pqcomm::init_seams();
        pqcomm::init_socket_seams();
        be_secure::init_seams();
        hba::init_seams();
        crate::init_seams();
        transam_xlog::control_file_mark_read_for_tests();
        guc_tables::vars::Password_encryption.install(guc_tables::GucVarAccessors {
            get: || 2, // PASSWORD_TYPE_SCRAM_SHA_256
            set: |_| {},
        });
        syscache_seams::lookup_authid_rolpassword::set(|mcx, rolname| {
            let secret = match rolname {
                "scramuser" | "passuser" => Some(RFC7677_SECRET.to_string()),
                "md5user" => Some(
                    String::from_utf8(pg_md5::pg_md5_encrypt(b"md5pw", b"md5user").to_vec())
                        .unwrap(),
                ),
                "nopass" => None,
                _ => return Ok(None),
            };
            let rolpassword = match secret {
                Some(sec) => Some(mcx::PgString::from_str_in(&sec, mcx)?),
                None => None,
            };
            Ok(Some(syscache_seams::AuthIdPasswordShape {
                rolpassword,
                rolvaliduntil: None,
            }))
        });
    });
}

// Password "pencil" (RFC 7677 salt/iterations).
const RFC7677_SECRET: &str = "SCRAM-SHA-256$4096:W22ZaJ0SNY7soEsUEjb6gQ==$\
WG5d8oPm3OtcPnkdi4Uo7BkeZkBFzpcXkuLmtbsT4qY=:wfPLwcE6nTWhTAmQ7tl2KeoiWGPlZqQxSrmfPwDl2dU=";

fn setup_backend(pid: i32) {
    install();
    g::SetMyProcPid(pid);
    fd::vfd::set_max_safe_fds_value(1000);
    waiteventset::InitializeWaitEventSupport().unwrap();
    let latch = latch::allocate_local_latch();
    latch::InitLatch(latch);
    g::SetMyLatch(Some(latch));
}

// hba lines are process-global; every test holds GUC_LOCK across load + use.
fn load_hba_content_locked(name: &str, content: &str) {
    install();
    let dir = std::env::temp_dir().join(format!("pgrust_auth_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(name);
    std::fs::write(&path, content).unwrap();
    guc_tables::vars::HbaFileName.write(Some(path.to_string_lossy().into_owned()));
    assert!(hba_seams::load_hba::call());
}

fn unix_port(user: &str, db: &str) -> Port {
    let mut raddr = SockAddr::zeroed();
    // SAFETY: writing an aligned sockaddr_un prefix into the storage buffer.
    unsafe {
        let mut sun: libc::sockaddr_un = core::mem::MaybeUninit::zeroed().assume_init();
        sun.sun_family = libc::AF_UNIX as libc::sa_family_t;
        core::ptr::copy_nonoverlapping(
            core::ptr::from_ref(&sun).cast::<u8>(),
            raddr.addr.as_mut_ptr(),
            core::mem::size_of::<libc::sockaddr_un>(),
        );
    }
    raddr.salen = core::mem::size_of::<libc::sockaddr_un>() as u32;
    let mut port = Port::new(&ClientSocket { sock: -1, raddr });
    port.user_name = Some(user.to_string());
    port.database_name = Some(db.to_string());
    port
}

fn expect_fatal(f: impl FnOnce()) -> PgError {
    CAPTURED.with(|c| c.borrow_mut().clear());
    let prev = elog::set_emit_log_hook(Some(capture_hook));
    let result = catch_unwind(AssertUnwindSafe(f));
    elog::set_emit_log_hook(prev);
    let panic_msg = payload_str(&result.expect_err("expected FATAL proc_exit"));
    assert_eq!(panic_msg, "proc_exit(1)");
    let err = CAPTURED
        .with(|c| c.borrow().last().cloned())
        .expect("FATAL report was emitted");
    assert_eq!(err.level(), FATAL);
    err
}

fn payload_str(payload: &Box<dyn std::any::Any + Send>) -> String {
    payload
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string()))
        .unwrap_or_default()
}

const INITDB_DEFAULT_HBA: &str = concat!(
    "local   all             all                                     trust\n",
    "host    all             all             127.0.0.1/32            trust\n",
    "host    all             all             ::1/128                 trust\n",
    "local   replication     all                                     trust\n",
    "host    replication     all             127.0.0.1/32            trust\n",
    "host    replication     all             ::1/128                 trust\n",
);

// The M1 gate: ClientAuthentication(trust) for a unix-socket Port, with the
// client receiving AuthenticationOk on the wire.
#[test]
fn trust_auth_unix_socket_end_to_end() {
    setup_backend(4243);
    let _g = GUC_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    load_hba_content_locked("pg_hba.conf", INITDB_DEFAULT_HBA);

    let dir = std::env::temp_dir().join(format!("pgrust_auth_sock_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let dir_s = dir.to_str().unwrap().to_owned();
    let port_number: u16 = 45455;
    let sock_path = format!("{dir_s}/.s.PGSQL.{port_number}");
    let _ = std::fs::remove_file(&sock_path);

    let mut listen_sockets: Vec<i32> = Vec::new();
    let status = pqcomm::ListenServerPort(
        libc::AF_UNIX,
        None,
        port_number,
        Some(&dir_s),
        &mut listen_sockets,
        64,
    )
    .unwrap();
    assert_eq!(status, 0);

    let client_path = sock_path.clone();
    let client = std::thread::spawn(move || {
        let mut stream = UnixStream::connect(client_path).unwrap();
        // AuthenticationOk: 'R' + int32 len 8 + int32 code 0.
        let mut reply = [0u8; 9];
        stream.read_exact(&mut reply).unwrap();
        assert_eq!(reply, [b'R', 0, 0, 0, 8, 0, 0, 0, 0]);
        stream.write_all(b"x").unwrap();
    });

    let mut client_sock = ClientSocket {
        sock: PGINVALID_SOCKET,
        raddr: SockAddr::zeroed(),
    };
    while pqcomm::AcceptConnection(listen_sockets[0], &mut client_sock) != 0 {}
    let mut port = pqcomm_seams::pq_init::call(&client_sock).unwrap();
    port.user_name = Some("malisper".to_string());
    port.database_name = Some("postgres".to_string());
    g::SetMyProcPort(port);

    auth_seams::client_authentication::call().unwrap();

    g::WithMyProcPort(|port| {
        let hba = port.hba.as_ref().expect("check_hba set port->hba");
        assert_eq!(hba.auth_method, types_core::init::uaTrust);
        assert_eq!(hba.conntype, types_startup::ctLocal);
        assert_eq!(hba.linenumber, 1);
    });
    assert!(miscinit::client_connection_info().0.is_none());

    // AUTH_REQ_OK is not flushed by sendAuthRequest; flush now.
    assert_eq!(pqcomm::pq_flush().unwrap(), 0);
    client.join().unwrap();

    pqcomm::RemoveSocketFiles();
    let _ = std::fs::remove_dir_all(&dir);
}

// Regression: a FATAL raised while ClientAuthentication holds the MyProcPort
// borrow (auth_seams entry) must still send to the client — the transport
// reads pqcomm's socket cells, never re-borrowing the Port RefCell.
#[test]
fn auth_fatal_under_port_borrow_reaches_client() {
    setup_backend(4244);
    let _g = GUC_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    load_hba_content_locked("reject_e2e.conf", "local all all reject\n");

    let dir = std::env::temp_dir().join(format!("pgrust_auth_fatal_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let dir_s = dir.to_str().unwrap().to_owned();
    let port_number: u16 = 45456;
    let sock_path = format!("{dir_s}/.s.PGSQL.{port_number}");
    let _ = std::fs::remove_file(&sock_path);

    let mut listen_sockets: Vec<i32> = Vec::new();
    let status = pqcomm::ListenServerPort(
        libc::AF_UNIX,
        None,
        port_number,
        Some(&dir_s),
        &mut listen_sockets,
        64,
    )
    .unwrap();
    assert_eq!(status, 0);

    let client_path = sock_path.clone();
    let client = std::thread::spawn(move || {
        let mut stream = UnixStream::connect(client_path).unwrap();
        let mut header = [0u8; 5];
        stream.read_exact(&mut header).unwrap();
        assert_eq!(header[0], b'E');
        let len = u32::from_be_bytes(header[1..5].try_into().unwrap()) as usize;
        let mut body = vec![0u8; len - 4];
        stream.read_exact(&mut body).unwrap();
        let body = String::from_utf8_lossy(&body).into_owned();
        assert!(body.contains("28000"), "no SQLSTATE in: {body}");
        assert!(body.contains("rejects connection"), "wrong message: {body}");
    });

    let mut client_sock = ClientSocket {
        sock: PGINVALID_SOCKET,
        raddr: SockAddr::zeroed(),
    };
    while pqcomm::AcceptConnection(listen_sockets[0], &mut client_sock) != 0 {}
    let mut port = pqcomm_seams::pq_init::call(&client_sock).unwrap();
    port.user_name = Some("alice".to_string());
    port.database_name = Some("postgres".to_string());
    g::SetMyProcPort(port);
    elog::config::set_where_to_send_output(types_dest::CommandDest::Remote);

    let result = catch_unwind(AssertUnwindSafe(|| {
        let _ = auth_seams::client_authentication::call();
    }));
    elog::config::set_where_to_send_output(types_dest::CommandDest::Debug);
    let msg = payload_str(&result.expect_err("expected FATAL proc_exit"));
    assert_eq!(msg, "proc_exit(1)", "FATAL send re-entered MyProcPort");

    client.join().unwrap();
    pqcomm::RemoveSocketFiles();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn explicit_reject_is_fatal_28000() {
    std::thread::spawn(|| {
        install();
        let _g = GUC_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        load_hba_content_locked("reject.conf", "local all all reject\n");
        let mut port = unix_port("alice", "postgres");
        let err = expect_fatal(|| {
            let _ = ClientAuthentication(&mut port);
        });
        assert_eq!(err.sqlstate(), make_sqlstate(*b"28000"));
        assert_eq!(
            err.message(),
            "pg_hba.conf rejects connection for host \"[local]\", user \"alice\", database \"postgres\", no encryption"
        );
    })
    .join()
    .unwrap();
}

#[test]
fn implicit_reject_is_fatal_28000() {
    std::thread::spawn(|| {
        install();
        let _g = GUC_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        load_hba_content_locked("hostonly.conf", "host all all 127.0.0.1/32 trust\n");
        let mut port = unix_port("alice", "postgres");
        let err = expect_fatal(|| {
            let _ = ClientAuthentication(&mut port);
        });
        assert_eq!(err.sqlstate(), make_sqlstate(*b"28000"));
        assert_eq!(
            err.message(),
            "no pg_hba.conf entry for host \"[local]\", user \"alice\", database \"postgres\", no encryption"
        );
    })
    .join()
    .unwrap();
}

#[test]
fn auth_failed_surfaces_exact_28000() {
    std::thread::spawn(|| {
        install();
        let _g = GUC_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        load_hba_content_locked("trust2.conf", "local all all trust\n");
        let mut port = unix_port("alice", "postgres");
        hba::hba_getauthmethod(&mut port).unwrap();

        let err = expect_fatal(|| {
            let _ = auth_failed(&port, STATUS_ERROR, None);
        });
        assert_eq!(err.sqlstate(), make_sqlstate(*b"28000"));
        assert_eq!(
            err.message(),
            "\"trust\" authentication failed for user \"alice\""
        );
        let detail = err.detail_log().unwrap();
        assert!(detail.starts_with("Connection matched file "));
        assert!(detail.ends_with("line 1: \"local all all trust\""));

        let err = expect_fatal(|| {
            let _ = auth_failed(&port, STATUS_ERROR, Some("extra detail"));
        });
        assert!(err
            .detail_log()
            .unwrap()
            .starts_with("extra detail\nConnection matched file"));
    })
    .join()
    .unwrap();
}

#[test]
fn password_failed_is_28P01() {
    std::thread::spawn(|| {
        install();
        let _g = GUC_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        load_hba_content_locked("scram2.conf", "local all all scram-sha-256\n");
        let mut port = unix_port("alice", "postgres");
        hba::hba_getauthmethod(&mut port).unwrap();
        let err = expect_fatal(|| {
            let _ = auth_failed(&port, STATUS_ERROR, None);
        });
        assert_eq!(err.sqlstate(), make_sqlstate(*b"28P01"));
        assert_eq!(
            err.message(),
            "password authentication failed for user \"alice\""
        );
    })
    .join()
    .unwrap();
}


#[test]
fn eof_status_exits_quietly() {
    let result = std::thread::spawn(|| {
        install();
        let _g = GUC_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        load_hba_content_locked("trust3.conf", "local all all trust\n");
        let mut port = unix_port("alice", "postgres");
        hba::hba_getauthmethod(&mut port).unwrap();
        let _ = auth_failed(&port, STATUS_EOF, None);
    })
    .join();
    // STATUS_EOF: proc_exit(0), no message to client.
    assert_eq!(payload_str(&result.unwrap_err()), "proc_exit(0)");
}

// ---- password-family end-to-end over a real unix socket ----

use pg_b64::{pg_b64_dec_len, pg_b64_decode, pg_b64_enc_len, pg_b64_encode};
use pg_hmac::{PgHmacCtx, Sha256};
use scram_common::{scram_client_key, scram_h, scram_salted_password, scram_server_key};

fn b64e(src: &[u8]) -> String {
    let cap = pg_b64_enc_len(src.len() as i32);
    let mut dst = vec![0u8; cap as usize];
    let n = pg_b64_encode(src, src.len() as i32, &mut dst, cap);
    assert!(n >= 0);
    dst.truncate(n as usize);
    String::from_utf8(dst).unwrap()
}

fn b64d(src: &str) -> Vec<u8> {
    let cap = pg_b64_dec_len(src.len() as i32);
    let mut dst = vec![0u8; cap as usize];
    let n = pg_b64_decode(src.as_bytes(), src.len() as i32, &mut dst, cap);
    assert!(n >= 0);
    dst.truncate(n as usize);
    dst
}

// Reads one server message; ('R', auth code, payload) or ('E', 0, body).
fn read_server_msg(stream: &mut UnixStream) -> (u8, u32, Vec<u8>) {
    let mut header = [0u8; 5];
    stream.read_exact(&mut header).unwrap();
    let len = u32::from_be_bytes(header[1..5].try_into().unwrap()) as usize;
    let mut body = vec![0u8; len - 4];
    stream.read_exact(&mut body).unwrap();
    if header[0] == b'R' {
        let code = u32::from_be_bytes(body[..4].try_into().unwrap());
        (b'R', code, body[4..].to_vec())
    } else {
        (header[0], 0, body)
    }
}

fn send_password_msg(stream: &mut UnixStream, body: &[u8]) {
    let mut pkt = Vec::with_capacity(5 + body.len());
    pkt.push(b'p');
    pkt.extend_from_slice(&((4 + body.len()) as u32).to_be_bytes());
    pkt.extend_from_slice(body);
    stream.write_all(&pkt).unwrap();
}

// Client-side SCRAM-SHA-256; returns the final server message tuple.
fn scram_client(stream: &mut UnixStream, password: &str, correct: bool) -> (u8, u32, Vec<u8>) {
    let (t, code, payload) = read_server_msg(stream);
    assert_eq!((t, code), (b'R', AUTH_REQ_SASL));
    let mechs = String::from_utf8(payload).unwrap();
    assert!(mechs.contains("SCRAM-SHA-256\0"));
    assert!(!mechs.contains("SCRAM-SHA-256-PLUS"), "PLUS without SSL: {mechs}");

    let client_first_bare = "n=,r=clientnonce0123456789";
    let mut body = b"SCRAM-SHA-256\0".to_vec();
    let initial = format!("n,,{client_first_bare}");
    body.extend_from_slice(&(initial.len() as u32).to_be_bytes());
    body.extend_from_slice(initial.as_bytes());
    send_password_msg(stream, &body);

    let (t, code, payload) = read_server_msg(stream);
    assert_eq!((t, code), (b'R', AUTH_REQ_SASL_CONT));
    let server_first = String::from_utf8(payload).unwrap();
    let mut parts = server_first.split(',');
    let full_nonce = parts.next().unwrap().strip_prefix("r=").unwrap().to_string();
    assert!(full_nonce.starts_with("clientnonce0123456789"));
    let salt = b64d(parts.next().unwrap().strip_prefix("s=").unwrap());
    let iterations: i32 = parts.next().unwrap().strip_prefix("i=").unwrap().parse().unwrap();

    let salted = scram_salted_password(password.as_bytes(), &salt, iterations).unwrap();
    let client_key = scram_client_key(&salted);
    let stored_key = scram_h(&client_key);
    let without_proof = format!("c=biws,r={full_nonce}");
    let auth_message = format!("{client_first_bare},{server_first},{without_proof}");
    let mut ctx = PgHmacCtx::<Sha256>::init(&stored_key);
    ctx.update(auth_message.as_bytes());
    let signature = ctx.finalize();
    let mut proof = [0u8; 32];
    for i in 0..32 {
        proof[i] = client_key[i] ^ signature[i];
    }
    if !correct {
        proof[0] ^= 0xff;
    }
    send_password_msg(stream, format!("{without_proof},p={}", b64e(&proof)).as_bytes());

    let (t, code, payload) = read_server_msg(stream);
    if t == b'R' && code == AUTH_REQ_SASL_FIN {
        let server_key = scram_server_key(&salted);
        let mut ctx = PgHmacCtx::<Sha256>::init(&server_key);
        ctx.update(auth_message.as_bytes());
        let expected = format!("v={}", b64e(&ctx.finalize()));
        assert_eq!(String::from_utf8(payload.clone()).unwrap(), expected);
        read_server_msg(stream)
    } else {
        (t, code, payload)
    }
}

struct SocketAuth {
    listen_sockets: Vec<i32>,
    dir: std::path::PathBuf,
}

impl SocketAuth {
    fn listen(tag: &str, port_number: u16) -> (Self, String) {
        let dir = std::env::temp_dir().join(format!("pgrust_auth_{tag}_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let dir_s = dir.to_str().unwrap().to_owned();
        let sock_path = format!("{dir_s}/.s.PGSQL.{port_number}");
        let _ = std::fs::remove_file(&sock_path);
        let mut listen_sockets: Vec<i32> = Vec::new();
        let status = pqcomm::ListenServerPort(
            libc::AF_UNIX,
            None,
            port_number,
            Some(&dir_s),
            &mut listen_sockets,
            64,
        )
        .unwrap();
        assert_eq!(status, 0);
        (Self { listen_sockets, dir }, sock_path)
    }

    fn accept_port(&self, user: &str) -> Port {
        let mut client_sock = ClientSocket {
            sock: PGINVALID_SOCKET,
            raddr: SockAddr::zeroed(),
        };
        while pqcomm::AcceptConnection(self.listen_sockets[0], &mut client_sock) != 0 {}
        let mut port = pqcomm_seams::pq_init::call(&client_sock).unwrap();
        port.user_name = Some(user.to_string());
        port.database_name = Some("postgres".to_string());
        port
    }

    fn cleanup(self) {
        pqcomm::RemoveSocketFiles();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn expect_client_auth_fatal(port: &mut Port) -> PgError {
    CAPTURED.with(|c| c.borrow_mut().clear());
    let prev = elog::set_emit_log_hook(Some(capture_hook));
    elog::config::set_where_to_send_output(types_dest::CommandDest::Remote);
    let result = catch_unwind(AssertUnwindSafe(|| {
        let _ = ClientAuthentication(port);
    }));
    elog::config::set_where_to_send_output(types_dest::CommandDest::Debug);
    elog::set_emit_log_hook(prev);
    assert_eq!(payload_str(&result.expect_err("expected FATAL")), "proc_exit(1)");
    let err = CAPTURED
        .with(|c| c.borrow().last().cloned())
        .expect("FATAL report was emitted");
    assert_eq!(err.level(), FATAL);
    err
}

#[test]
fn scram_auth_end_to_end() {
    setup_backend(4245);
    let _g = GUC_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    load_hba_content_locked("scram_ok.conf", "local all all scram-sha-256\n");
    let (sa, sock_path) = SocketAuth::listen("scram_ok", 45457);

    let client = std::thread::spawn(move || {
        let mut stream = UnixStream::connect(sock_path).unwrap();
        let (t, code, _) = scram_client(&mut stream, "pencil", true);
        assert_eq!((t, code), (b'R', AUTH_REQ_OK));
    });

    let mut port = sa.accept_port("scramuser");
    ClientAuthentication(&mut port).unwrap();
    assert_eq!(pqcomm::pq_flush().unwrap(), 0);
    assert_eq!(miscinit::client_connection_info().0, Some("scramuser"));
    client.join().unwrap();
    sa.cleanup();
}

#[test]
fn scram_auth_wrong_password_is_28P01() {
    setup_backend(4246);
    let _g = GUC_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    load_hba_content_locked("scram_bad.conf", "local all all scram-sha-256\n");
    let (sa, sock_path) = SocketAuth::listen("scram_bad", 45458);

    let client = std::thread::spawn(move || {
        let mut stream = UnixStream::connect(sock_path).unwrap();
        let (t, _code, body) = scram_client(&mut stream, "pencil", false);
        assert_eq!(t, b'E');
        let body = String::from_utf8_lossy(&body).into_owned();
        assert!(body.contains("28P01"), "{body}");
        assert!(
            body.contains("password authentication failed for user \"scramuser\""),
            "{body}"
        );
    });

    let mut port = sa.accept_port("scramuser");
    let err = expect_client_auth_fatal(&mut port);
    assert_eq!(err.sqlstate(), make_sqlstate(*b"28P01"));
    assert_eq!(
        err.message(),
        "password authentication failed for user \"scramuser\""
    );
    client.join().unwrap();
    sa.cleanup();
}

// Nonexistent user: the mock exchange runs to completion (plausible
// server-first) and fails exactly like a wrong password.
#[test]
fn scram_auth_nonexistent_user_mock() {
    setup_backend(4247);
    let _g = GUC_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    load_hba_content_locked("scram_ghost.conf", "local all all scram-sha-256\n");
    let (sa, sock_path) = SocketAuth::listen("scram_ghost", 45459);

    let client = std::thread::spawn(move || {
        let mut stream = UnixStream::connect(sock_path).unwrap();
        let (t, _code, body) = scram_client(&mut stream, "pencil", true);
        assert_eq!(t, b'E');
        let body = String::from_utf8_lossy(&body).into_owned();
        assert!(body.contains("28P01"), "{body}");
        assert!(
            body.contains("password authentication failed for user \"ghost\""),
            "{body}"
        );
    });

    let mut port = sa.accept_port("ghost");
    let err = expect_client_auth_fatal(&mut port);
    assert_eq!(err.sqlstate(), make_sqlstate(*b"28P01"));
    assert_eq!(
        err.message(),
        "password authentication failed for user \"ghost\""
    );
    assert!(err
        .detail_log()
        .unwrap()
        .starts_with("Role \"ghost\" does not exist."));
    client.join().unwrap();
    sa.cleanup();
}

#[test]
fn md5_auth_end_to_end() {
    setup_backend(4248);
    let _g = GUC_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    load_hba_content_locked("md5_ok.conf", "local all all md5\n");
    let (sa, sock_path) = SocketAuth::listen("md5_ok", 45460);

    let client = std::thread::spawn(move || {
        let mut stream = UnixStream::connect(sock_path).unwrap();
        let (t, code, payload) = read_server_msg(&mut stream);
        assert_eq!((t, code), (b'R', AUTH_REQ_MD5));
        assert_eq!(payload.len(), 4);
        let inner = pg_md5::pg_md5_encrypt(b"md5pw", b"md5user");
        let response = pg_md5::pg_md5_encrypt(&inner[3..], &payload);
        let mut body = response.to_vec();
        body.push(0);
        send_password_msg(&mut stream, &body);
        let (t, code, _) = read_server_msg(&mut stream);
        assert_eq!((t, code), (b'R', AUTH_REQ_OK));
    });

    let mut port = sa.accept_port("md5user");
    ClientAuthentication(&mut port).unwrap();
    assert_eq!(pqcomm::pq_flush().unwrap(), 0);
    assert_eq!(miscinit::client_connection_info().0, Some("md5user"));
    client.join().unwrap();
    sa.cleanup();
}

// C 18 CheckPWChallengeAuth: an md5 hba line with a SCRAM secret runs SCRAM.
#[test]
fn md5_hba_with_scram_secret_runs_scram() {
    setup_backend(4249);
    let _g = GUC_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    load_hba_content_locked("md5_scram.conf", "local all all md5\n");
    let (sa, sock_path) = SocketAuth::listen("md5_scram", 45461);

    let client = std::thread::spawn(move || {
        let mut stream = UnixStream::connect(sock_path).unwrap();
        let (t, code, _) = scram_client(&mut stream, "pencil", true);
        assert_eq!((t, code), (b'R', AUTH_REQ_OK));
    });

    let mut port = sa.accept_port("scramuser");
    ClientAuthentication(&mut port).unwrap();
    assert_eq!(pqcomm::pq_flush().unwrap(), 0);
    client.join().unwrap();
    sa.cleanup();
}

#[test]
fn password_auth_end_to_end() {
    setup_backend(4250);
    let _g = GUC_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    load_hba_content_locked("pass_ok.conf", "local all all password\n");
    let (sa, sock_path) = SocketAuth::listen("pass_ok", 45462);

    let client = std::thread::spawn(move || {
        let mut stream = UnixStream::connect(sock_path).unwrap();
        let (t, code, _) = read_server_msg(&mut stream);
        assert_eq!((t, code), (b'R', AUTH_REQ_PASSWORD));
        send_password_msg(&mut stream, b"pencil\0");
        let (t, code, _) = read_server_msg(&mut stream);
        assert_eq!((t, code), (b'R', AUTH_REQ_OK));
    });

    let mut port = sa.accept_port("passuser");
    ClientAuthentication(&mut port).unwrap();
    assert_eq!(pqcomm::pq_flush().unwrap(), 0);
    assert_eq!(miscinit::client_connection_info().0, Some("passuser"));
    client.join().unwrap();
    sa.cleanup();
}

#[test]
fn interpret_ident_response_cases() {
    // RFC 1413 USERID happy path (the RFC's own example).
    assert_eq!(
        interpret_ident_response(b"6193, 23 : USERID : UNIX : stjohns\r\n").as_deref(),
        Some("stjohns")
    );
    // No blanks around the separators.
    assert_eq!(
        interpret_ident_response(b"123,456:USERID:OTHER:foo\r\n").as_deref(),
        Some("foo")
    );
    // User names keep interior blanks.
    assert_eq!(
        interpret_ident_response(b"123,456:USERID:UNIX:foo bar\r\n").as_deref(),
        Some("foo bar")
    );
    // ERROR responses carry no user name.
    assert_eq!(
        interpret_ident_response(b"6195, 23 : ERROR : NO-USER\r\n"),
        None
    );
    // Not terminated with CRLF.
    assert_eq!(
        interpret_ident_response(b"6193, 23 : USERID : UNIX : stjohns"),
        None
    );
    // Too short / degenerate.
    assert_eq!(interpret_ident_response(b""), None);
    assert_eq!(interpret_ident_response(b"x"), None);
    assert_eq!(interpret_ident_response(b"\r\n"), None);
    // No colon before the final CR.
    assert_eq!(interpret_ident_response(b"garbage\r\n"), None);
    // Missing the OS-field colon.
    assert_eq!(interpret_ident_response(b"123,456:USERID:UNIX\r\n"), None);
    // A NUL truncates the scan like C's strlen (no CRLF before it -> None).
    assert_eq!(
        interpret_ident_response(b"123,456:USERID:UNIX:foo\0trailing\r\n"),
        None
    );
    // User name is capped at IDENT_USERNAME_MAX bytes.
    let mut long = b"1,2:USERID:UNIX:".to_vec();
    long.extend(std::iter::repeat(b'a').take(IDENT_USERNAME_MAX + 50));
    long.extend(b"\r\n");
    let got = interpret_ident_response(&long).unwrap();
    assert_eq!(got.len(), IDENT_USERNAME_MAX);
    assert!(got.bytes().all(|b| b == b'a'));
}
