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
    });
}

fn setup_backend(pid: i32) {
    install();
    g::SetMyProcPid(pid);
    fd::vfd::set_max_safe_fds_value(1000);
    waiteventset::InitializeWaitEventSupport().unwrap();
    let latch = latch::allocate_local_latch();
    latch::InitLatch(latch);
    g::SetMyLatch(Some(latch));
}

fn load_hba_content(name: &str, content: &str) {
    install();
    let _g = GUC_LOCK.lock().unwrap();
    load_hba_content_locked(name, content);
}

// hba lines are process-global; callers needing a stable view across their
// whole body hold GUC_LOCK themselves.
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
    load_hba_content("pg_hba.conf", INITDB_DEFAULT_HBA);

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
    let _g = GUC_LOCK.lock().unwrap();
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
        load_hba_content("reject.conf", "local all all reject\n");
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
        load_hba_content("hostonly.conf", "host all all 127.0.0.1/32 trust\n");
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
        load_hba_content("trust2.conf", "local all all trust\n");
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
        load_hba_content("scram2.conf", "local all all scram-sha-256\n");
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
fn scram_arm_is_loud() {
    let result = std::thread::spawn(|| {
        install();
        load_hba_content("scram.conf", "local all all scram-sha-256\n");
        let mut port = unix_port("alice", "postgres");
        let _ = ClientAuthentication(&mut port);
    })
    .join();
    let msg = payload_str(&result.unwrap_err());
    assert!(
        msg.contains("\"scram-sha-256\" arm deferred") && msg.contains("backend-libpq-crypt"),
        "unexpected panic: {msg}"
    );
}

#[test]
fn eof_status_exits_quietly() {
    let result = std::thread::spawn(|| {
        install();
        load_hba_content("trust3.conf", "local all all trust\n");
        let mut port = unix_port("alice", "postgres");
        hba::hba_getauthmethod(&mut port).unwrap();
        let _ = auth_failed(&port, STATUS_EOF, None);
    })
    .join();
    // STATUS_EOF: proc_exit(0), no message to client.
    assert_eq!(payload_str(&result.unwrap_err()), "proc_exit(0)");
}
