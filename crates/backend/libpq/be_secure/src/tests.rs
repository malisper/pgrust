use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::sync::Once;

use init_small::globals as g;
use ip::SockAddr;
use types_core::{PGINVALID_SOCKET, STATUS_OK};
use types_startup::ClientSocket;

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
        init_small::init_seams();
        waiteventset::init_seams();
        latch::init_seams();
        pqcomm::init_seams();
        pqcomm::init_socket_seams();
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

fn frame(msgtype: u8, body: &[u8]) -> Vec<u8> {
    let mut f = vec![msgtype];
    f.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
    f.extend_from_slice(body);
    f
}

// Boot-readiness proof: listen/accept/pq_init over a real AF_UNIX socket,
// pqcomm framing above the installed seams; the delayed client send blocks
// the first read through the FeBeWaitSet wait loop.
#[test]
fn af_unix_listen_accept_roundtrip() {
    setup_backend(4242);

    let dir = std::env::temp_dir().join(format!("pgrust_be_secure_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let dir_s = dir.to_str().unwrap().to_owned();
    let port_number: u16 = 45454;
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
    assert_eq!(status, STATUS_OK);
    assert_eq!(listen_sockets.len(), 1);

    let body = b"SELECT 'roundtrip';".to_vec();
    let request = frame(b'Q', &body);
    let expected_reply = frame(b'Z', &body);
    let client_path = sock_path.clone();
    let client = std::thread::spawn(move || {
        let mut stream = UnixStream::connect(client_path).unwrap();
        // Delay so the server's first read blocks through FeBeWaitSet.
        std::thread::sleep(std::time::Duration::from_millis(100));
        stream.write_all(&request).unwrap();
        let mut reply = vec![0u8; expected_reply.len()];
        stream.read_exact(&mut reply).unwrap();
        assert_eq!(reply, expected_reply);
    });

    let mut client_sock = ClientSocket {
        sock: PGINVALID_SOCKET,
        raddr: SockAddr::zeroed(),
    };
    assert_eq!(
        pqcomm::AcceptConnection(listen_sockets[0], &mut client_sock),
        STATUS_OK
    );
    let port = pqcomm_seams::pq_init::call(&client_sock).unwrap();
    assert!(!port.noblock);
    g::SetMyProcPort(port);

    pqcomm::pq_startmsgread().unwrap();
    assert_eq!(pqcomm::pq_getbyte().unwrap(), i32::from(b'Q'));
    let ctx = mcx::MemoryContext::new("be_secure test");
    let mut s = stringinfo::StringInfo::new_in(ctx.mcx()).unwrap();
    assert_eq!(pqcomm::pq_getmessage(&mut s, 10000).unwrap(), 0);
    assert_eq!(s.as_bytes(), &body[..]);
    assert!(!pqcomm::pq_is_reading_msg());

    assert_eq!(pqcomm::pq_putmessage(b'Z', s.as_bytes()).unwrap(), 0);
    assert_eq!(pqcomm::pq_flush().unwrap(), 0);

    client.join().unwrap();

    pqcomm::RemoveSocketFiles();
    assert!(!std::path::Path::new(&sock_path).exists());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn set_port_noblock_without_port_reports_no_connection() {
    // Own thread: MyProcPort is thread-local and must be absent here.
    std::thread::spawn(|| {
        install();
        assert!(!be_secure_seams::set_port_noblock::call(true));
    })
    .join()
    .unwrap();
}
