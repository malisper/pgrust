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
                // Verifier over a NON-UTF-8 password byte string (b046).
                "rawbytes" => Some(
                    String::from_utf8(
                        pg_md5::pg_md5_encrypt(b"pencil\xe9", b"rawbytes").to_vec(),
                    )
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
fn read_server_msg(stream: &mut impl Read) -> (u8, u32, Vec<u8>) {
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

fn send_password_msg(stream: &mut impl Write, body: &[u8]) {
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

// audit-18.6 b046 (auth.c:771-775): recv_password_packet returns the client's
// bytes verbatim — C does no encoding conversion because the client encoding
// is not known yet. A password carrying non-UTF-8 bytes must verify against a
// verifier computed over exactly those bytes (a lossy UTF-8 decode turns
// 0xE9 into U+FFFD and the plaintext arm fails where C succeeds).
#[test]
fn password_auth_raw_bytes_end_to_end() {
    setup_backend(4251);
    let _g = GUC_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    load_hba_content_locked("pass_raw.conf", "local all all password\n");
    let (sa, sock_path) = SocketAuth::listen("pass_raw", 45463);

    let client = std::thread::spawn(move || {
        let mut stream = UnixStream::connect(sock_path).unwrap();
        let (t, code, _) = read_server_msg(&mut stream);
        assert_eq!((t, code), (b'R', AUTH_REQ_PASSWORD));
        send_password_msg(&mut stream, b"pencil\xe9\0");
        let (t, code, body) = read_server_msg(&mut stream);
        assert_eq!(
            (t, code),
            (b'R', AUTH_REQ_OK),
            "server answered {:?}",
            String::from_utf8_lossy(&body)
        );
    });

    let mut port = sa.accept_port("rawbytes");
    ClientAuthentication(&mut port).unwrap();
    assert_eq!(pqcomm::pq_flush().unwrap(), 0);
    assert_eq!(miscinit::client_connection_info().0, Some("rawbytes"));
    client.join().unwrap();
    sa.cleanup();
}

// TCP twin of SocketAuth for methods hba refuses on local sockets (gss).
struct TcpAuth {
    listen_sockets: Vec<i32>,
}

impl TcpAuth {
    fn listen(port_number: u16) -> Self {
        let mut listen_sockets: Vec<i32> = Vec::new();
        let status = pqcomm::ListenServerPort(
            libc::AF_INET,
            Some("127.0.0.1"),
            port_number,
            None,
            &mut listen_sockets,
            64,
        )
        .unwrap();
        assert_eq!(status, 0);
        Self { listen_sockets }
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
        for s in self.listen_sockets {
            // SAFETY: fds we opened via ListenServerPort.
            unsafe { libc::close(s) };
        }
    }
}

// audit-18.6 b046 (auth.c:939-1005): with pg_krb_server_keyfile set, C only
// points libkrb5 at the keytab (setenv KRB5_KTNAME) and lets
// gss_accept_sec_context discover an unusable keytab — AFTER the client's
// first GSS token has been read — reported as "accepting GSS security
// context failed". The acceptor credential must not be acquired (and fail)
// before that read: message and socket read state would both diverge.
#[test]
fn gss_bad_keytab_fails_after_reading_client_token() {
    setup_backend(4252);
    let _g = GUC_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    if let Err(e) = crate::gss_ffi::try_gss() {
        eprintln!("SKIP gss_bad_keytab_fails_after_reading_client_token: {e}");
        return;
    }
    load_hba_content_locked("gss_keytab.conf", "host all all 127.0.0.1/32 gss\n");
    let prev_keyfile = guc_tables::vars::pg_krb_server_keyfile.read();
    guc_tables::vars::pg_krb_server_keyfile
        .write(Some("/nonexistent/b046-audit.keytab".to_string()));
    let ta = TcpAuth::listen(45471);

    let client = std::thread::spawn(move || {
        let mut stream = std::net::TcpStream::connect(("127.0.0.1", 45471)).unwrap();
        let (t, code, _) = read_server_msg(&mut stream);
        assert_eq!((t, code), (b'R', AUTH_REQ_GSS));
        // First (garbage) AP-REQ token; C reads it before anything can fail.
        send_password_msg(&mut stream, b"\x60\x10not-a-gss-token");
        let (t, _, body) = read_server_msg(&mut stream);
        assert_eq!(t, b'E', "expected FATAL, got {:?}", String::from_utf8_lossy(&body));
    });

    let mut port = ta.accept_port("gssuser");
    let err = expect_client_auth_fatal(&mut port);
    guc_tables::vars::pg_krb_server_keyfile.write(prev_keyfile);
    assert_eq!(err.message(), "GSSAPI authentication failed for user \"gssuser\"");
    let logged: Vec<String> =
        CAPTURED.with(|c| c.borrow().iter().map(|e| e.message().to_string()).collect());
    assert!(
        logged.iter().any(|m| m == "accepting GSS security context failed"),
        "keytab failure must surface from the context-accept step (C auth.c:1049): {logged:?}"
    );
    assert!(
        !logged.iter().any(|m| m.starts_with("gss_acquire_cred_from")),
        "acceptor credential was acquired (and failed) before the client token was read: {logged:?}"
    );
    client.join().unwrap();
    ta.cleanup();
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

// ---------------- PAM (mock libpam FFI layer) ----------------
//
// A real PAM service needs root-owned /etc/pam.d entries, so the
// conversation and CheckPAMAuth flow are unit-tested against a mock PamApi;
// the e2e script exercises hba acceptance and the real-libpam failure path
// (nonexistent service -> pam_deny via the "other" policy).

use crate::pam_ffi::{self, pam_conv, pam_message, pam_response};
use core::ffi::{c_char, c_int, c_void};

thread_local! {
    static MOCK_CONV: RefCell<usize> = const { RefCell::new(0) };
    static MOCK_AUTH_RESULT: RefCell<c_int> = const { RefCell::new(pam_ffi::PAM_SUCCESS) };
    static MOCK_SEEN_PASSWORD: RefCell<Option<String>> = const { RefCell::new(None) };
}

unsafe extern "C" fn mock_pam_start(
    _service: *const c_char,
    _user: *const c_char,
    conv: *const pam_conv,
    pamh: *mut *mut pam_ffi::pam_handle_t,
) -> c_int {
    MOCK_CONV.with(|c| *c.borrow_mut() = conv as usize);
    *pamh = 0x1 as *mut pam_ffi::pam_handle_t;
    pam_ffi::PAM_SUCCESS
}

unsafe extern "C" fn mock_pam_set_item(
    _pamh: *mut pam_ffi::pam_handle_t,
    item_type: c_int,
    item: *const c_void,
) -> c_int {
    if item_type == pam_ffi::PAM_CONV {
        MOCK_CONV.with(|c| *c.borrow_mut() = item as usize);
    }
    pam_ffi::PAM_SUCCESS
}

// Drives the conversation like a PAM module asking for a password.
unsafe extern "C" fn mock_pam_authenticate(
    _pamh: *mut pam_ffi::pam_handle_t,
    _flags: c_int,
) -> c_int {
    let scripted = MOCK_AUTH_RESULT.with(|r| *r.borrow());
    if scripted != pam_ffi::PAM_SUCCESS {
        return scripted;
    }
    let conv = MOCK_CONV.with(|c| *c.borrow()) as *const pam_conv;
    assert!(!conv.is_null(), "mock: no conversation registered");
    let msg = pam_message {
        msg_style: pam_ffi::PAM_PROMPT_ECHO_OFF,
        msg: c"Password:".as_ptr(),
    };
    let mut msgs: [*const pam_message; 1] = [&msg];
    let mut resp: *mut pam_response = core::ptr::null_mut();
    let rc = ((*conv).conv)(1, msgs.as_mut_ptr(), &mut resp, (*conv).appdata_ptr);
    if rc != pam_ffi::PAM_SUCCESS {
        return rc;
    }
    assert!(!resp.is_null());
    let got = std::ffi::CStr::from_ptr((*resp).resp).to_string_lossy().into_owned();
    libc::free((*resp).resp as *mut c_void);
    libc::free(resp as *mut c_void);
    MOCK_SEEN_PASSWORD.with(|p| *p.borrow_mut() = Some(got));
    pam_ffi::PAM_SUCCESS
}

unsafe extern "C" fn mock_pam_acct_mgmt(
    _pamh: *mut pam_ffi::pam_handle_t,
    _flags: c_int,
) -> c_int {
    pam_ffi::PAM_SUCCESS
}

unsafe extern "C" fn mock_pam_end(_pamh: *mut pam_ffi::pam_handle_t, _status: c_int) -> c_int {
    pam_ffi::PAM_SUCCESS
}

unsafe extern "C" fn mock_pam_strerror(
    _pamh: *mut pam_ffi::pam_handle_t,
    _errnum: c_int,
) -> *const c_char {
    c"mock PAM error".as_ptr()
}

fn install_pam_mock() {
    pam_ffi::install_mock_for_tests(pam_ffi::PamApi {
        pam_start: mock_pam_start,
        pam_set_item: mock_pam_set_item,
        pam_authenticate: mock_pam_authenticate,
        pam_acct_mgmt: mock_pam_acct_mgmt,
        pam_end: mock_pam_end,
        pam_strerror: mock_pam_strerror,
    });
}

fn pam_port(user: &str) -> Port {
    let mut port = unix_port(user, "postgres");
    let mut hba = types_startup::HbaLine::new_zeroed();
    hba.auth_method = types_core::init::uaPAM;
    hba.conntype = types_startup::ctLocal;
    hba.sourcefile = "test_hba".to_string();
    hba.linenumber = 1;
    hba.rawline = "local all all pam".to_string();
    port.hba = Some(hba);
    port
}

// CheckPAMAuth full flow against the mock: pam_start -> set_item(USER/CONV)
// -> authenticate (conversation returns the password) -> acct_mgmt -> end,
// then set_authn_id records the identity.
#[test]
fn pam_mock_flow_succeeds_and_sets_authn_id() {
    setup_backend(4271);
    install_pam_mock();
    MOCK_AUTH_RESULT.with(|r| *r.borrow_mut() = pam_ffi::PAM_SUCCESS);
    let port = pam_port("pamuser");
    // Non-empty password: the conversation answers from appdata without
    // needing a client socket (the empty-password client round trip shares
    // sendAuthRequest/recv_password_packet with the tested password arms).
    let status = crate::pam::CheckPAMAuth(&port, "pamuser", b"sekrit").unwrap();
    assert_eq!(status, STATUS_OK);
    assert_eq!(
        MOCK_SEEN_PASSWORD.with(|p| p.borrow().clone()).as_deref(),
        Some("sekrit")
    );
    let (authn_id, method) = miscinit::client_connection_info();
    assert_eq!(authn_id, Some("pamuser"));
    assert_eq!(method, types_core::init::uaPAM);
}

#[test]
fn pam_mock_authenticate_failure_is_status_error() {
    setup_backend(4272);
    install_pam_mock();
    MOCK_AUTH_RESULT.with(|r| *r.borrow_mut() = 9); // e.g. PAM_AUTH_ERR
    let port = pam_port("pamuser");
    let status = crate::pam::CheckPAMAuth(&port, "pamuser", b"sekrit").unwrap();
    assert_eq!(status, STATUS_ERROR);
    assert_eq!(miscinit::client_connection_info().0, None);
}

// The conversation proc directly: TEXT_INFO / ERROR_MSG replies, unsupported
// styles, and bad num_msg.
#[test]
fn pam_conv_proc_message_styles() {
    install();
    let passwd = std::ffi::CString::new("pw").unwrap();
    let m1 = pam_message { msg_style: pam_ffi::PAM_PROMPT_ECHO_OFF, msg: c"Password:".as_ptr() };
    let m2 = pam_message { msg_style: pam_ffi::PAM_TEXT_INFO, msg: c"info".as_ptr() };
    let m3 = pam_message { msg_style: pam_ffi::PAM_ERROR_MSG, msg: c"boom".as_ptr() };
    let mut msgs: [*const pam_message; 3] = [&m1, &m2, &m3];
    let mut resp: *mut pam_response = core::ptr::null_mut();
    // SAFETY: valid message array and out-pointer.
    let rc = unsafe {
        crate::pam::pam_passwd_conv_proc(
            3,
            msgs.as_mut_ptr(),
            &mut resp,
            passwd.as_ptr() as *mut c_void,
        )
    };
    assert_eq!(rc, pam_ffi::PAM_SUCCESS);
    assert!(!resp.is_null());
    // SAFETY: conv allocated 3 responses.
    unsafe {
        let r0 = std::ffi::CStr::from_ptr((*resp).resp).to_str().unwrap();
        assert_eq!(r0, "pw");
        assert_eq!((*resp).resp_retcode, pam_ffi::PAM_SUCCESS);
        let r1 = std::ffi::CStr::from_ptr((*resp.add(1)).resp).to_str().unwrap();
        assert_eq!(r1, "");
        let r2 = std::ffi::CStr::from_ptr((*resp.add(2)).resp).to_str().unwrap();
        assert_eq!(r2, "");
        for i in 0..3 {
            libc::free((*resp.add(i)).resp as *mut c_void);
        }
        libc::free(resp as *mut c_void);
    }

    // Unsupported style fails the whole conversation.
    let bad = pam_message { msg_style: 99, msg: core::ptr::null() };
    let mut msgs: [*const pam_message; 1] = [&bad];
    let mut resp: *mut pam_response = core::ptr::null_mut();
    // SAFETY: valid message array and out-pointer.
    let rc = unsafe {
        crate::pam::pam_passwd_conv_proc(
            1,
            msgs.as_mut_ptr(),
            &mut resp,
            passwd.as_ptr() as *mut c_void,
        )
    };
    assert_eq!(rc, pam_ffi::PAM_CONV_ERR);
    assert!(resp.is_null());

    // num_msg out of range.
    let mut resp: *mut pam_response = core::ptr::null_mut();
    // SAFETY: num_msg is rejected before the message array is read.
    let rc = unsafe {
        crate::pam::pam_passwd_conv_proc(
            0,
            core::ptr::null_mut(),
            &mut resp,
            passwd.as_ptr() as *mut c_void,
        )
    };
    assert_eq!(rc, pam_ffi::PAM_CONV_ERR);
    // SAFETY: as above; PAM_MAX_NUM_MSG+1 messages claimed but rejected.
    let rc = unsafe {
        crate::pam::pam_passwd_conv_proc(
            pam_ffi::PAM_MAX_NUM_MSG + 1,
            core::ptr::null_mut(),
            &mut resp,
            passwd.as_ptr() as *mut c_void,
        )
    };
    assert_eq!(rc, pam_ffi::PAM_CONV_ERR);
}

// ===========================================================================
// audit-18.6 w2-022 — LDAP DNS SRV discovery (auth.c:2255-2290:
// ldap_dn2domain / ldap_domain2hostlist) and LDAP TLS (auth.c:2296-2385:
// `ldaps` at ldap_initialize, `ldaptls` via ldap_start_tls_s), witnessed
// against in-process fakes: a DNS server answering `_ldap._tcp.<domain>`
// SRV queries (UDP, TC -> TCP) and an LDAPv3 server (bind / search /
// StartTLS / ldaps) built on ldapber's own BER helpers. The expected
// outcomes are what an OpenLDAP-built C 18.6 produces against the same
// fakes (message text: auth.c + libldap 2.5 tls2.c / tls_o.c).
// ===========================================================================
#[cfg(not(target_family = "wasm"))]
pub(crate) mod ldap_fakes {
    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpListener, TcpStream, UdpSocket};
    use std::path::PathBuf;
    use std::sync::Arc;

    use openssl::asn1::Asn1Time;
    use openssl::bn::BigNum;
    use openssl::hash::MessageDigest;
    use openssl::pkey::{PKey, Private};
    use openssl::rsa::Rsa;
    use openssl::ssl::{SslAcceptor, SslMethod, SslStream};
    use openssl::x509::extension::{BasicConstraints, SubjectAlternativeName};
    use openssl::x509::{X509NameBuilder, X509};

    use crate::ldapber::{
        ber_int, decode_int, tlv, BerReader, TAG_BIND_REQUEST, TAG_BIND_RESPONSE,
        TAG_ENUMERATED, TAG_INTEGER, TAG_OCTET_STRING, TAG_SEARCH_DONE, TAG_SEARCH_ENTRY,
        TAG_SEARCH_REQUEST, TAG_SEQUENCE, TAG_UNBIND_REQUEST,
    };

    pub const STARTTLS_OID: &[u8] = b"1.3.6.1.4.1.1466.20037";
    const TAG_EXTENDED_REQUEST: u8 = 0x77;
    const TAG_EXTENDED_RESPONSE: u8 = 0x78;

    // ---------------- DNS ----------------

    pub struct SrvRec {
        pub priority: u16,
        pub weight: u16,
        pub port: u16,
        pub target: &'static str,
    }

    /// SRV-only authoritative fake on 127.0.0.1: one UDP socket and one TCP
    /// listener on the same port. `truncate_udp` answers UDP with TC and no
    /// records, so only the TCP retry carries the answer.
    pub struct FakeDns {
        pub addr: SocketAddr,
    }

    fn encode_name(name: &str) -> Vec<u8> {
        let mut v = Vec::new();
        for label in name.trim_end_matches('.').split('.') {
            v.push(label.len() as u8);
            v.extend_from_slice(label.as_bytes());
        }
        v.push(0);
        v
    }

    fn dns_response(query: &[u8], recs: &[SrvRec], truncate: bool) -> Vec<u8> {
        let mut p = 12;
        while query[p] != 0 {
            p += 1 + query[p] as usize;
        }
        let question = &query[12..p + 1 + 4];
        let mut out = Vec::new();
        out.extend_from_slice(&query[0..2]);
        let flags: u16 = if truncate { 0x8380 } else { 0x8180 };
        out.extend_from_slice(&flags.to_be_bytes());
        out.extend_from_slice(&1u16.to_be_bytes());
        let an: u16 = if truncate { 0 } else { recs.len() as u16 };
        out.extend_from_slice(&an.to_be_bytes());
        out.extend_from_slice(&0u16.to_be_bytes());
        out.extend_from_slice(&0u16.to_be_bytes());
        out.extend_from_slice(question);
        if !truncate {
            for r in recs {
                out.extend_from_slice(&[0xc0, 0x0c]); // name: pointer to the question
                out.extend_from_slice(&33u16.to_be_bytes()); // SRV
                out.extend_from_slice(&1u16.to_be_bytes()); // IN
                out.extend_from_slice(&60u32.to_be_bytes());
                let target = encode_name(r.target);
                out.extend_from_slice(&((6 + target.len()) as u16).to_be_bytes());
                out.extend_from_slice(&r.priority.to_be_bytes());
                out.extend_from_slice(&r.weight.to_be_bytes());
                out.extend_from_slice(&r.port.to_be_bytes());
                out.extend_from_slice(&target);
            }
        }
        out
    }

    impl FakeDns {
        pub fn start(records: Vec<SrvRec>, truncate_udp: bool) -> FakeDns {
            // The same port number on both transports: bind TCP on an
            // ephemeral port and retry until UDP can take the same one.
            let (udp, tcp, addr) = loop {
                let tcp = TcpListener::bind("127.0.0.1:0").unwrap();
                let addr = tcp.local_addr().unwrap();
                if let Ok(udp) = UdpSocket::bind(addr) {
                    break (udp, tcp, addr);
                }
            };
            let recs = Arc::new(records);
            let recs_udp = Arc::clone(&recs);
            std::thread::spawn(move || {
                let mut buf = [0u8; 1024];
                while let Ok((n, peer)) = udp.recv_from(&mut buf) {
                    let resp = dns_response(&buf[..n], &recs_udp, truncate_udp);
                    let _ = udp.send_to(&resp, peer);
                }
            });
            std::thread::spawn(move || {
                for s in tcp.incoming() {
                    let Ok(mut s) = s else { continue };
                    let mut l = [0u8; 2];
                    if s.read_exact(&mut l).is_err() {
                        continue;
                    }
                    let mut q = vec![0u8; u16::from_be_bytes(l) as usize];
                    if s.read_exact(&mut q).is_err() {
                        continue;
                    }
                    let resp = dns_response(&q, &recs, false);
                    let _ = s.write_all(&(resp.len() as u16).to_be_bytes());
                    let _ = s.write_all(&resp);
                }
            });
            FakeDns { addr }
        }
    }

    // ---------------- certificate ----------------

    pub struct TestCert {
        pub cert_pem: PathBuf,
        pub cert: X509,
        pub key: PKey<Private>,
    }

    /// Self-signed certificate (CN = `cn`, the given DNS / IP SANs), written
    /// as PEM into a fresh per-process directory; usable both as the fake
    /// server's certificate and as the client's trust anchor.
    pub fn test_cert(tag: &str, cn: &str, dns: &[&str], ips: &[&str]) -> TestCert {
        let key = PKey::from_rsa(Rsa::generate(2048).unwrap()).unwrap();
        let mut name = X509NameBuilder::new().unwrap();
        name.append_entry_by_text("CN", cn).unwrap();
        let name = name.build();
        let mut b = X509::builder().unwrap();
        b.set_version(2).unwrap();
        b.set_serial_number(&BigNum::from_u32(1).unwrap().to_asn1_integer().unwrap())
            .unwrap();
        b.set_subject_name(&name).unwrap();
        b.set_issuer_name(&name).unwrap();
        b.set_pubkey(&key).unwrap();
        b.set_not_before(&Asn1Time::days_from_now(0).unwrap()).unwrap();
        b.set_not_after(&Asn1Time::days_from_now(30).unwrap()).unwrap();
        b.append_extension(BasicConstraints::new().critical().ca().build().unwrap())
            .unwrap();
        if !dns.is_empty() || !ips.is_empty() {
            let mut san = SubjectAlternativeName::new();
            for d in dns {
                san.dns(d);
            }
            for ip in ips {
                san.ip(ip);
            }
            let ext = san.build(&b.x509v3_context(None, None)).unwrap();
            b.append_extension(ext).unwrap();
        }
        b.sign(&key, MessageDigest::sha256()).unwrap();
        let cert = b.build();
        let dir = std::env::temp_dir().join(format!("pgrust_auth_ldap_{tag}_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let cert_pem = dir.join("cert.pem");
        std::fs::write(&cert_pem, cert.to_pem().unwrap()).unwrap();
        TestCert {
            cert_pem,
            cert,
            key,
        }
    }

    // ---------------- LDAP ----------------

    #[derive(Clone, Copy, PartialEq, Eq)]
    pub enum LdapMode {
        /// Plain LDAP; StartTLS is honored (needs a cert).
        Plain,
        /// Plain LDAP; StartTLS answered `unwillingToPerform` "TLS not supported".
        NoTls,
        /// TLS from the first byte (ldaps).
        Ldaps,
    }

    pub struct FakeLdap {
        pub port: u16,
    }

    enum FakeStream {
        Plain(TcpStream),
        Tls(SslStream<TcpStream>),
        Gone,
    }

    impl Read for FakeStream {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            match self {
                FakeStream::Plain(s) => s.read(buf),
                FakeStream::Tls(s) => s.read(buf),
                FakeStream::Gone => Ok(0),
            }
        }
    }
    impl Write for FakeStream {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            match self {
                FakeStream::Plain(s) => s.write(buf),
                FakeStream::Tls(s) => s.write(buf),
                FakeStream::Gone => Ok(0),
            }
        }
        fn flush(&mut self) -> std::io::Result<()> {
            match self {
                FakeStream::Plain(s) => s.flush(),
                FakeStream::Tls(s) => s.flush(),
                FakeStream::Gone => Ok(()),
            }
        }
    }

    fn read_ber_message(r: &mut impl Read) -> Option<Vec<u8>> {
        let mut header = [0u8; 2];
        r.read_exact(&mut header).ok()?;
        let mut msg = header.to_vec();
        let len = if header[1] < 0x80 {
            header[1] as usize
        } else {
            let n = (header[1] & 0x7f) as usize;
            let mut lb = vec![0u8; n];
            r.read_exact(&mut lb).ok()?;
            msg.extend_from_slice(&lb);
            lb.iter().fold(0usize, |v, &b| (v << 8) | b as usize)
        };
        let at = msg.len();
        msg.resize(at + len, 0);
        r.read_exact(&mut msg[at..]).ok()?;
        Some(msg)
    }

    fn envelope(msgid: i64, op: &[u8]) -> Vec<u8> {
        let mut c = ber_int(TAG_INTEGER, msgid);
        c.extend_from_slice(op);
        tlv(TAG_SEQUENCE, &c)
    }

    fn ldap_result(tag: u8, rc: i64, diag: &str, extra: &[u8]) -> Vec<u8> {
        let mut body = ber_int(TAG_ENUMERATED, rc);
        body.extend_from_slice(&tlv(TAG_OCTET_STRING, b""));
        body.extend_from_slice(&tlv(TAG_OCTET_STRING, diag.as_bytes()));
        body.extend_from_slice(extra);
        tlv(tag, &body)
    }

    fn contains(hay: &[u8], needle: &[u8]) -> bool {
        !needle.is_empty() && hay.windows(needle.len()).any(|w| w == needle)
    }

    struct Users {
        entries: Vec<(String, String)>,
        anonymous_ok: bool,
    }

    fn serve(tcp: TcpStream, mode: LdapMode, users: Arc<Users>, acceptor: Option<Arc<SslAcceptor>>) {
        let mut s = if mode == LdapMode::Ldaps {
            match acceptor.as_ref().unwrap().accept(tcp) {
                Ok(t) => FakeStream::Tls(t),
                Err(_) => return, // client refused our certificate
            }
        } else {
            FakeStream::Plain(tcp)
        };
        loop {
            let Some(msg) = read_ber_message(&mut s) else { return };
            let mut r = BerReader::new(&msg);
            let Ok((_, content)) = r.read_tlv() else { return };
            let mut r = BerReader::new(content);
            let Ok((_, id)) = r.read_tlv() else { return };
            let Ok(msgid) = decode_int(id) else { return };
            let Ok((tag, op)) = r.read_tlv() else { return };
            match tag {
                TAG_BIND_REQUEST => {
                    let mut b = BerReader::new(op);
                    let (Ok((_, _ver)), Ok((_, name)), Ok((_, pw))) =
                        (b.read_tlv(), b.read_tlv(), b.read_tlv())
                    else {
                        return;
                    };
                    let ok = if name.is_empty() && pw.is_empty() {
                        users.anonymous_ok
                    } else {
                        users
                            .entries
                            .iter()
                            .any(|(dn, p)| dn.as_bytes() == name && p.as_bytes() == pw)
                    };
                    let rc = if ok { 0 } else { 49 };
                    let _ = s.write_all(&envelope(msgid, &ldap_result(TAG_BIND_RESPONSE, rc, "", &[])));
                }
                TAG_SEARCH_REQUEST => {
                    for (dn, _) in &users.entries {
                        let uid = dn.split(',').next().and_then(|a| a.strip_prefix("uid="));
                        if let Some(uid) = uid {
                            if contains(op, uid.as_bytes()) {
                                let mut e = tlv(TAG_OCTET_STRING, dn.as_bytes());
                                e.extend_from_slice(&tlv(TAG_SEQUENCE, &[]));
                                let _ = s.write_all(&envelope(msgid, &tlv(TAG_SEARCH_ENTRY, &e)));
                            }
                        }
                    }
                    let _ = s.write_all(&envelope(msgid, &ldap_result(TAG_SEARCH_DONE, 0, "", &[])));
                }
                TAG_EXTENDED_REQUEST => {
                    let mut b = BerReader::new(op);
                    let Ok((t, oid)) = b.read_tlv() else { return };
                    let is_tls = matches!(s, FakeStream::Tls(_));
                    if t != 0x80 || oid != STARTTLS_OID {
                        let _ = s.write_all(&envelope(
                            msgid,
                            &ldap_result(TAG_EXTENDED_RESPONSE, 2, "unsupported extended operation", &[]),
                        ));
                    } else if is_tls {
                        let _ = s.write_all(&envelope(
                            msgid,
                            &ldap_result(TAG_EXTENDED_RESPONSE, 1, "TLS already started", &[]),
                        ));
                    } else if mode == LdapMode::NoTls {
                        let _ = s.write_all(&envelope(
                            msgid,
                            &ldap_result(TAG_EXTENDED_RESPONSE, 53, "TLS not supported", &[]),
                        ));
                    } else {
                        let _ = s.write_all(&envelope(
                            msgid,
                            &ldap_result(TAG_EXTENDED_RESPONSE, 0, "", &tlv(0x8a, STARTTLS_OID)),
                        ));
                        let FakeStream::Plain(tcp) = std::mem::replace(&mut s, FakeStream::Gone) else {
                            return;
                        };
                        match acceptor.as_ref().unwrap().accept(tcp) {
                            Ok(t) => s = FakeStream::Tls(t),
                            Err(_) => return,
                        }
                    }
                }
                TAG_UNBIND_REQUEST => return,
                _ => return,
            }
        }
    }

    impl FakeLdap {
        /// `users` = (bind DN, password); `anonymous_ok` = whether the empty
        /// simple bind auth.c's search+bind mode issues first succeeds.
        pub fn start(
            mode: LdapMode,
            users: Vec<(&str, &str)>,
            anonymous_ok: bool,
            cert: Option<&TestCert>,
        ) -> FakeLdap {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let port = listener.local_addr().unwrap().port();
            let acceptor = cert.map(|c| {
                let mut b = SslAcceptor::mozilla_intermediate_v5(SslMethod::tls_server()).unwrap();
                b.set_private_key(&c.key).unwrap();
                b.set_certificate(&c.cert).unwrap();
                Arc::new(b.build())
            });
            let users = Arc::new(Users {
                entries: users
                    .into_iter()
                    .map(|(d, p)| (d.to_string(), p.to_string()))
                    .collect(),
                anonymous_ok,
            });
            std::thread::spawn(move || {
                for conn in listener.incoming() {
                    let Ok(conn) = conn else { continue };
                    let users = Arc::clone(&users);
                    let acceptor = acceptor.clone();
                    std::thread::spawn(move || serve(conn, mode, users, acceptor));
                }
            });
            FakeLdap { port }
        }
    }
}

#[cfg(not(target_family = "wasm"))]
mod ldap_witness {
    use super::ldap_fakes::{test_cert, FakeDns, FakeLdap, LdapMode, SrvRec};
    use super::*;

    const ALICE_DN: &str = "uid=alice,dc=example,dc=test";

    // Every LDAP test holds GUC_LOCK (hba is process-global) and owns the
    // libldap-style environment while it runs: LDAPCONF names the one
    // ldap.conf the test wants (OpenLDAP init.c reads it after the system
    // file, so its TLS_* lines win), nothing else is inherited.
    fn ldap_conf(tag: &str, body: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("pgrust_auth_ldapconf_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(format!("{tag}.conf"));
        std::fs::write(&path, body).unwrap();
        path
    }

    fn set_ldap_env(conf: &std::path::Path) {
        for k in [
            "LDAPNOINIT",
            "LDAPRC",
            "LDAPTLS_CACERT",
            "LDAPTLS_CACERTDIR",
            "LDAPTLS_REQCERT",
            "LDAPTLS_REQSAN",
            "LDAPTLS_CERT",
            "LDAPTLS_KEY",
            "LDAPTLS_CIPHER_SUITE",
            "LDAPTLS_PROTOCOL_MIN",
            "LDAPTLS_PROTOCOL_MAX",
            "LDAPTLS_PEERKEY_HASH",
            "LDAPTLS_CRLCHECK",
        ] {
            std::env::remove_var(k);
        }
        std::env::set_var("LDAPCONF", conf);
    }

    fn describe(captured: &[PgError]) -> Vec<String> {
        captured
            .iter()
            .map(|e| match e.detail() {
                Some(d) => format!("{} | DETAIL: {d}", e.message()),
                None => e.message().to_string(),
            })
            .collect()
    }

    // Runs ClientAuthentication on `port` with a client thread that answers
    // the password request with `password`; returns (outcome, server-side
    // reports, the client's final message type/code).
    fn run_ldap_auth(
        tag: &str,
        unix_port: u16,
        port_user: &str,
        password: &'static [u8],
    ) -> (Result<(), String>, Vec<PgError>, (u8, u32)) {
        let (sa, sock_path) = SocketAuth::listen(tag, unix_port);
        let client = std::thread::spawn(move || {
            let mut stream = UnixStream::connect(sock_path).unwrap();
            let (t, code, _) = read_server_msg(&mut stream);
            assert_eq!((t, code), (b'R', AUTH_REQ_PASSWORD));
            let mut body = password.to_vec();
            body.push(0);
            send_password_msg(&mut stream, &body);
            let (t, code, _) = read_server_msg(&mut stream);
            (t, code)
        });
        let mut port = sa.accept_port(port_user);
        CAPTURED.with(|c| c.borrow_mut().clear());
        let prev = elog::set_emit_log_hook(Some(capture_hook));
        elog::config::set_where_to_send_output(types_dest::CommandDest::Remote);
        let result = catch_unwind(AssertUnwindSafe(|| ClientAuthentication(&mut port)));
        elog::config::set_where_to_send_output(types_dest::CommandDest::Debug);
        elog::set_emit_log_hook(prev);
        let _ = pqcomm::pq_flush();
        let captured = CAPTURED.with(|c| c.borrow().clone());
        let outcome = match result {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(format!("error: {}", e.message())),
            Err(p) => Err(payload_str(&p)),
        };
        let client_final = client.join().unwrap();
        drop(port);
        sa.cleanup();
        (outcome, captured, client_final)
    }

    fn assert_ok(outcome: &Result<(), String>, captured: &[PgError], client_final: (u8, u32)) {
        assert!(
            outcome.is_ok(),
            "authentication failed ({:?}); server reports: {:#?}",
            outcome,
            describe(captured)
        );
        assert_eq!(client_final, (b'R', AUTH_REQ_OK));
        assert_eq!(miscinit::client_connection_info().0, Some(ALICE_DN));
    }

    fn assert_failed_with(
        outcome: &Result<(), String>,
        captured: &[PgError],
        message: &str,
        detail: Option<&str>,
    ) {
        assert_eq!(
            outcome.as_ref().unwrap_err(),
            "proc_exit(1)",
            "expected the FATAL auth failure; reports: {:#?}",
            describe(captured)
        );
        let hit = captured.iter().find(|e| e.message() == message);
        assert!(
            hit.is_some(),
            "expected server report {message:?}; got {:#?}",
            describe(captured)
        );
        assert_eq!(hit.unwrap().detail(), detail, "DETAIL of {message:?}");
    }

    // auth.c:2255-2290 — no ldapserver: the base DN's trailing DC components
    // name the domain (ldap_dn2domain: "ou=people,dc=example,dc=test" ->
    // example.test), `_ldap._tcp.example.test` SRV records name the servers,
    // and ldap_domain2hostlist orders them by priority: the priority-0 server
    // (which knows alice) must be tried before the priority-10 one (which
    // refuses every bind, so a wrong order fails the initial bind). The UDP
    // answer is truncated, so the lookup must complete over TCP as libresolv
    // does. Before the port: "LDAP authentication could not find DNS SRV
    // records for \"example.test\"" and STATUS_ERROR.
    #[test]
    fn ldap_srv_discovery_orders_hosts_by_priority() {
        setup_backend(4261);
        let _g = GUC_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        set_ldap_env(&ldap_conf("srv", "# nothing\n"));
        let good = FakeLdap::start(LdapMode::Plain, vec![(ALICE_DN, "secret")], true, None);
        let bad = FakeLdap::start(LdapMode::Plain, vec![], false, None);
        let dns = FakeDns::start(
            vec![
                SrvRec { priority: 10, weight: 0, port: bad.port, target: "localhost" },
                SrvRec { priority: 0, weight: 0, port: good.port, target: "localhost" },
            ],
            true,
        );
        *pgsync::lock(&crate::ldap::SRV_NAMESERVERS) = Some(vec![dns.addr]);
        load_hba_content_locked(
            "ldap_srv.conf",
            "local all all ldap ldapbasedn=\"ou=people,dc=example,dc=test\"\n",
        );
        let (outcome, captured, fin) = run_ldap_auth("ldap_srv", 45471, "alice", b"secret");
        *pgsync::lock(&crate::ldap::SRV_NAMESERVERS) = None;
        assert_ok(&outcome, &captured, fin);
    }

    // auth.c:2296-2330 — ldapscheme=ldaps: ldap_initialize("ldaps://...")
    // and the TLS handshake at the first operation, the peer verified against
    // the configured TLS_CACERT (ldap.conf via LDAPCONF), the host name
    // checked against the certificate's IP SAN (tls_o.c tlso_session_chkhost).
    // Before the port: "could not initialize LDAP: Not Supported".
    #[test]
    fn ldaps_with_configured_ca_authenticates() {
        setup_backend(4262);
        let _g = GUC_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let cert = test_cert("ldaps_ok", "ldap.example.test", &["ldap.example.test"], &["127.0.0.1"]);
        set_ldap_env(&ldap_conf(
            "ldaps_ok",
            &format!("TLS_CACERT {}\n", cert.cert_pem.display()),
        ));
        let srv = FakeLdap::start(LdapMode::Ldaps, vec![(ALICE_DN, "secret")], true, Some(&cert));
        load_hba_content_locked(
            "ldaps_ok.conf",
            &format!(
                "local all all ldap ldapserver=127.0.0.1 ldapport={} ldapscheme=ldaps ldapprefix=\"uid=\" ldapsuffix=\",dc=example,dc=test\"\n",
                srv.port
            ),
        );
        let (outcome, captured, fin) = run_ldap_auth("ldaps_ok", 45472, "alice", b"secret");
        assert_ok(&outcome, &captured, fin);
    }

    // Same, search+bind mode over ldaps (the anonymous initial bind, the
    // search, and the user bind all ride the one TLS session).
    #[test]
    fn ldaps_search_bind_authenticates() {
        setup_backend(4263);
        let _g = GUC_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let cert = test_cert("ldaps_sb", "ldap.example.test", &[], &["127.0.0.1"]);
        set_ldap_env(&ldap_conf("ldaps_sb", &format!("TLS_CACERT {}\n", cert.cert_pem.display())));
        let srv = FakeLdap::start(LdapMode::Ldaps, vec![(ALICE_DN, "secret")], true, Some(&cert));
        load_hba_content_locked(
            "ldaps_sb.conf",
            &format!(
                "local all all ldap ldapserver=127.0.0.1 ldapport={} ldapscheme=ldaps ldapbasedn=\"dc=example,dc=test\" ldapsearchattribute=uid\n",
                srv.port
            ),
        );
        let (outcome, captured, fin) = run_ldap_auth("ldaps_sb", 45473, "alice", b"secret");
        assert_ok(&outcome, &captured, fin);
    }

    // TLS_REQCERT demand (the OpenLDAP default) with no trust anchor for the
    // fake's self-signed certificate: tlsg_session_accept's post-handshake
    // verification fails (-1), ldap_new_connection reports LDAP_SERVER_DOWN
    // at the bind, and ld_error carries tlsg_session_errmsg's text —
    // gnutls_strerror(-1) = "(unknown error code)", as the Debian C 18.6
    // pair logs it.
    #[test]
    fn ldaps_untrusted_certificate_is_cant_contact_with_gnutls_diagnostics() {
        setup_backend(4264);
        let _g = GUC_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let cert = test_cert("ldaps_untrusted", "ldap.example.test", &[], &["127.0.0.1"]);
        set_ldap_env(&ldap_conf("ldaps_untrusted", "TLS_REQCERT demand\n"));
        let srv = FakeLdap::start(LdapMode::Ldaps, vec![(ALICE_DN, "secret")], true, Some(&cert));
        load_hba_content_locked(
            "ldaps_untrusted.conf",
            &format!(
                "local all all ldap ldapserver=127.0.0.1 ldapport={} ldapscheme=ldaps ldapprefix=\"uid=\" ldapsuffix=\",dc=example,dc=test\"\n",
                srv.port
            ),
        );
        let (outcome, captured, _) = run_ldap_auth("ldaps_untrusted", 45474, "alice", b"secret");
        assert_failed_with(
            &outcome,
            &captured,
            "LDAP login failed for user \"uid=alice,dc=example,dc=test\" on server \"127.0.0.1\": Can't contact LDAP server",
            Some("LDAP diagnostics: (unknown error code)"),
        );
    }

    // TLS_REQCERT never: no verification, no host-name check
    // (tls2.c:544-551), the untrusted certificate is accepted.
    #[test]
    fn ldaps_reqcert_never_skips_verification() {
        setup_backend(4265);
        let _g = GUC_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let cert = test_cert("ldaps_never", "other.example.test", &[], &[]);
        set_ldap_env(&ldap_conf("ldaps_never", "TLS_REQCERT never\n"));
        let srv = FakeLdap::start(LdapMode::Ldaps, vec![(ALICE_DN, "secret")], true, Some(&cert));
        load_hba_content_locked(
            "ldaps_never.conf",
            &format!(
                "local all all ldap ldapserver=127.0.0.1 ldapport={} ldapscheme=ldaps ldapprefix=\"uid=\" ldapsuffix=\",dc=example,dc=test\"\n",
                srv.port
            ),
        );
        let (outcome, captured, fin) = run_ldap_auth("ldaps_never", 45475, "alice", b"secret");
        assert_ok(&outcome, &captured, fin);
    }

    // ldapserver=localhost: libldap checks the certificate against the local
    // FQDN (ldap_int_hostname, init.c:695 / tls_g.c:579-585), never against
    // the literal "localhost"; a certificate naming neither fails the CN
    // check (LDAP_CONNECT_ERROR), and ldap_int_tls_connect renders that code
    // through gnutls_strerror: "(unknown error code)".
    #[test]
    fn ldaps_localhost_checks_the_fqdn_against_the_certificate() {
        setup_backend(4266);
        let _g = GUC_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let cert = test_cert("ldaps_host", "ldap.example.test", &["ldap.example.test"], &["127.0.0.1"]);
        set_ldap_env(&ldap_conf("ldaps_host", &format!("TLS_CACERT {}\n", cert.cert_pem.display())));
        let srv = FakeLdap::start(LdapMode::Ldaps, vec![(ALICE_DN, "secret")], true, Some(&cert));
        load_hba_content_locked(
            "ldaps_host.conf",
            &format!(
                "local all all ldap ldapserver=localhost ldapport={} ldapscheme=ldaps ldapprefix=\"uid=\" ldapsuffix=\",dc=example,dc=test\"\n",
                srv.port
            ),
        );
        let (outcome, captured, _) = run_ldap_auth("ldaps_host", 45476, "alice", b"secret");
        assert_failed_with(
            &outcome,
            &captured,
            "LDAP login failed for user \"uid=alice,dc=example,dc=test\" on server \"localhost\": Can't contact LDAP server",
            Some("LDAP diagnostics: (unknown error code)"),
        );
    }

    // auth.c:2367-2385 — ldaptls=1: ldap_start_tls_s sends the StartTLS
    // extended operation (1.3.6.1.4.1.1466.20037) on the plain connection and
    // upgrades it. Before the port: "could not start LDAP TLS session: Not
    // Supported".
    #[test]
    fn ldap_starttls_with_configured_ca_authenticates() {
        setup_backend(4267);
        let _g = GUC_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let cert = test_cert("starttls_ok", "ldap.example.test", &[], &["127.0.0.1"]);
        set_ldap_env(&ldap_conf("starttls_ok", &format!("TLS_CACERT {}\n", cert.cert_pem.display())));
        let srv = FakeLdap::start(LdapMode::Plain, vec![(ALICE_DN, "secret")], true, Some(&cert));
        load_hba_content_locked(
            "starttls_ok.conf",
            &format!(
                "local all all ldap ldapserver=127.0.0.1 ldapport={} ldaptls=1 ldapprefix=\"uid=\" ldapsuffix=\",dc=example,dc=test\"\n",
                srv.port
            ),
        );
        let (outcome, captured, fin) = run_ldap_auth("starttls_ok", 45477, "alice", b"secret");
        assert_ok(&outcome, &captured, fin);
    }

    // The server declines StartTLS: the extended operation's result code and
    // diagnosticMessage come back through ldap_err2string / errdetail_for_ldap.
    #[test]
    fn ldap_starttls_refused_by_server_reports_the_result() {
        setup_backend(4268);
        let _g = GUC_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        set_ldap_env(&ldap_conf("starttls_refused", "# nothing\n"));
        let srv = FakeLdap::start(LdapMode::NoTls, vec![(ALICE_DN, "secret")], true, None);
        load_hba_content_locked(
            "starttls_refused.conf",
            &format!(
                "local all all ldap ldapserver=127.0.0.1 ldapport={} ldaptls=1 ldapprefix=\"uid=\" ldapsuffix=\",dc=example,dc=test\"\n",
                srv.port
            ),
        );
        let (outcome, captured, _) = run_ldap_auth("starttls_refused", 45478, "alice", b"secret");
        assert_failed_with(
            &outcome,
            &captured,
            "could not start LDAP TLS session: Server is unwilling to perform",
            Some("LDAP diagnostics: TLS not supported"),
        );
    }
}
