use super::*;
use std::cell::Cell;
use std::sync::mpsc::{channel, Sender};
use std::sync::{Mutex, Once};
use std::time::Duration;

use ip::SockAddr;
use types_startup::{BackendStartupData, CacState};

#[derive(Debug)]
struct ChildSnapshot {
    my_proc_pid: i32,
    is_under_postmaster: bool,
    work_mem: i32,
    max_safe_fds: i32,
    postmaster_pid: i32,
    data_dir: Option<&'static str>,
    my_latch_set: bool,
    my_start_timestamp: i64,
    socket_create: i64,
    fork_start: i64,
    fork_end: i64,
    pm_child_slot: i32,
    client_sock: Option<i32>,
    sigterm_blocked: bool,
    sigquit_in_blocksig: bool,
}

static SNAPSHOT_TX: Mutex<Option<Sender<ChildSnapshot>>> = Mutex::new(None);

thread_local! {
    static WES_POS: Cell<i32> = const { Cell::new(0) };
}

fn install() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        timestamp_seams::get_current_timestamp::set(|| 777);
        guc_tables::init_seams();
        // Child threads run InitializeGUCOptions; its check hooks reach xact.
        xact_seams::is_in_parallel_mode::set(|| false);
        scalar_seams::parse_bool::set(|v| match v {
            "on" | "true" | "yes" | "1" => Some(true),
            "off" | "false" | "no" | "0" => Some(false),
            _ => None,
        });
        init_small::init_seams();
        waiteventset_seams::create_wait_event_set::set(|_| {
            Ok(types_storage::waiteventset::WaitEventSetHandle::new(1))
        });
        waiteventset_seams::add_wait_event_to_set::set(|_, _, _, _, _| {
            let pos = WES_POS.get();
            WES_POS.set(pos + 1);
            Ok(pos)
        });
        // Child's first cross-unit call in backend_initialize: capture here.
        pqcomm_seams::pq_init::set(|client_sock| {
            let timing = backend_startup::conn_timing::get();
            let masks = libpq_pqsignal::signal_masks();
            let mut cur: libc::sigset_t = unsafe { core::mem::zeroed() };
            // SAFETY: null set with a valid oldset out-param reads the mask.
            unsafe { libc::sigprocmask(libc::SIG_SETMASK, core::ptr::null(), &mut cur) };
            let snap = ChildSnapshot {
                my_proc_pid: init_small::globals::MyProcPid(),
                is_under_postmaster: init_small::globals::IsUnderPostmaster(),
                work_mem: init_small::globals::work_mem(),
                max_safe_fds: fd::max_safe_fds(),
                postmaster_pid: init_small::globals::PostmasterPid(),
                data_dir: init_small::globals::DataDir(),
                my_latch_set: init_small::globals::MyLatch().is_some(),
                my_start_timestamp: init_small::globals::MyStartTimestamp(),
                socket_create: timing.socket_create,
                fork_start: timing.fork_start,
                fork_end: timing.fork_end,
                pm_child_slot: init_small::globals::MyPMChildSlot(),
                client_sock: init_small::globals::MyClientSocket().map(|c| c.sock),
                sigterm_blocked: unsafe { libc::sigismember(&cur, libc::SIGTERM) == 1 },
                sigquit_in_blocksig: masks.block_sig_contains(libc::SIGQUIT),
            };
            SNAPSHOT_TX.lock().unwrap().as_ref().unwrap().send(snap).unwrap();
            panic!("test capture complete");
        });
    });
}

#[test]
fn child_names_match_c_table() {
    let expected = [
        "invalid",
        "backend",
        "dead-end backend",
        "autovacuum launcher",
        "autovacuum worker",
        "bgworker",
        "wal sender",
        "slot sync worker",
        "standalone backend",
        "archiver",
        "bgwriter",
        "checkpointer",
        "io_worker",
        "startup",
        "wal_receiver",
        "wal_summarizer",
        "wal_writer",
        "syslogger",
    ];
    for (i, bt) in BackendType::ALL.iter().enumerate() {
        assert_eq!(postmaster_child_name(*bt), expected[i], "{bt:?}");
    }
}

#[test]
fn shmem_attach_matches_c_table() {
    for bt in BackendType::ALL {
        let expect = !matches!(
            bt,
            BackendType::Invalid | BackendType::StandaloneBackend | BackendType::Logger
        );
        assert_eq!(CHILD_PROCESS_KINDS[bt as usize].shmem_attach, expect, "{bt:?}");
    }
}

#[test]
fn launch_backend_thread_runs_child_init_in_order() {
    install();
    let (tx, rx) = channel();
    *SNAPSHOT_TX.lock().unwrap() = Some(tx);

    init_small::globals::SetIsPostmasterEnvironment(true);
    init_small::globals::SetIsUnderPostmaster(false);
    guc::initialize_guc_options().unwrap();
    guc::SetConfigOption(
        "work_mem",
        Some("4321"),
        types_guc::PGC_POSTMASTER,
        types_guc::PGC_S_ARGV,
    )
    .unwrap();
    init_small::globals::SetPostmasterPid(42);
    init_small::globals::SetDataDir("/tmp/pg-launch-test");
    // PostmasterMain's set_max_safe_fds() result must cross into the child
    // thread (C: fork inheritance); a stand-in value proves the wire.
    fd::vfd::set_max_safe_fds_value(987);

    let startup = StartupData::Backend(BackendStartupData {
        can_accept_connections: CacState::Ok,
        socket_created: 111,
        fork_started: 0,
    });
    let pid = postmaster_child_launch(
        BackendType::Backend,
        7,
        startup,
        Some(ClientSocket { sock: 33, raddr: SockAddr::zeroed() }),
    );
    assert!(pid >= 1000, "synthetic pid, got {pid}");

    let snap = rx.recv_timeout(Duration::from_secs(10)).expect("child snapshot");
    assert_eq!(snap.my_proc_pid, pid);
    assert!(snap.is_under_postmaster);
    assert_eq!(snap.work_mem, 4321);
    assert_eq!(snap.max_safe_fds, 987, "max_safe_fds must inherit into child threads");
    assert_eq!(snap.postmaster_pid, 42);
    assert_eq!(snap.data_dir, Some("/tmp/pg-launch-test"));
    assert!(snap.my_latch_set);
    assert_eq!(snap.my_start_timestamp, 777);
    assert_eq!(snap.socket_create, 111);
    assert_eq!(snap.fork_start, 777);
    assert_eq!(snap.fork_end, 777);
    assert_eq!(snap.pm_child_slot, 7);
    assert_eq!(snap.client_sock, Some(33));
    assert!(snap.sigterm_blocked);
    assert!(!snap.sigquit_in_blocksig);
}

// The Main::Unported class is empty since GL-AIO-1 ported IoWorkerMain (its
// loud-panic arms stay for future kinds); the None-kind test below keeps the
// loud-refusal class pinned.
#[test]
#[should_panic(expected = "no main_fn")]
fn null_main_fn_kind_panics_loudly() {
    install();
    init_small::globals::SetIsPostmasterEnvironment(true);
    init_small::globals::SetIsUnderPostmaster(false);
    postmaster_child_launch(BackendType::StandaloneBackend, 1, StartupData::None, None);
}

// ---------------------------------------------------------------------------
// GL-FDLIMIT-1: a connection whose child thread cannot start must be REFUSED
// on the wire and its socket closed. Before this, child-init failure panicked
// the thread: the accepted socket stayed open with no owner, so the client sat
// waiting forever (it deadlocked a benchmark driver for 55 minutes).
// ---------------------------------------------------------------------------
#[test]
fn startup_failure_reaches_the_client_and_closes_the_socket() {
    let mut sv = [0i32; 2];
    // SAFETY: socketpair writes two fds into the array.
    let rc = unsafe { libc::socketpair(libc::AF_UNIX, libc::SOCK_STREAM, 0, sv.as_mut_ptr()) };
    assert_eq!(rc, 0, "socketpair failed");
    let (server, client) = (sv[0], sv[1]);

    report_startup_failure_to_client(server, "waiter wake pipe creation failed: errno 24");

    // The client sees the error text...
    let mut buf = [0u8; 256];
    // SAFETY: read into a local buffer on the peer end we own.
    let n = unsafe { libc::read(client, buf.as_mut_ptr().cast(), buf.len()) };
    assert!(n > 0, "client got no error message (read returned {n})");
    let got = &buf[..n as usize];
    assert_eq!(got[0], b'E', "message must be an error frame: {got:?}");
    let text = String::from_utf8_lossy(&got[1..]);
    assert!(text.starts_with("waiter wake pipe creation failed: errno 24"), "{text}");
    assert_eq!(got[got.len() - 1], 0, "message must be NUL-terminated");

    // ...and then EOF, because the server closed the socket. Without the
    // close this read blocks forever, which is the hang being fixed.
    // SAFETY: same peer fd; a closed peer returns 0.
    let n = unsafe { libc::read(client, buf.as_mut_ptr().cast(), buf.len()) };
    assert_eq!(n, 0, "socket was left open: the client would wait forever");

    // SAFETY: closing the peer end we own.
    unsafe { libc::close(client) };
}

// An invalid socket (no client — an auxiliary child) is a no-op, not a crash.
#[test]
fn startup_failure_with_no_client_socket_is_inert() {
    report_startup_failure_to_client(types_core::PGINVALID_SOCKET, "no client here");
}

// --- Lifecycle test design: I-1 firewall status-map seam + I-2 P0 guards ---

/// I-1 [INVARIANT-only] (P0): a backend PANIC resolves through the
/// `catch_unwind` seam into a postmaster-visible crash status rather than
/// escaping out of band, and it does NOT take down the process — the spawned
/// backend thread crashes while this test thread lives on. Exercises the B1
/// crash primitives against the REAL production status map
/// (`panic_payload_to_exit_status`, the sole seam `run_child_task` uses).
#[test]
fn i1_backend_crash_resolves_through_firewall_seam() {
    // (a) raw backend panic -> SIGABRT, the generic crash class the reaper
    //     treats as a crash (drives D-1 terminate-all + reinit).
    let status = std::thread::spawn(|| {
        let payload = std::panic::catch_unwind(|| {
            crash_primitive::pgrust_test_backend_panic()
        })
        .expect_err("the panic primitive must unwind, not return");
        panic_payload_to_exit_status(payload.as_ref())
    })
    .join()
    .expect("the crash must stay contained in its own thread (firewall), not abort the process");
    assert_eq!(
        status,
        procsignal::signums::SIGABRT,
        "a raw backend panic must map to the SIGABRT crash class"
    );

    // (b) the SIGSEGV kill variant -> WTERMSIG(SIGSEGV): the hard-signal
    //     (crash_signals) class, still contained to the one thread.
    let status = std::thread::spawn(|| {
        let payload = std::panic::catch_unwind(|| {
            crash_primitive::pgrust_test_backend_kill_sigsegv()
        })
        .expect_err("the SIGSEGV primitive must unwind, not return");
        panic_payload_to_exit_status(payload.as_ref())
    })
    .join()
    .expect("the SIGSEGV crash must stay contained in its own thread");
    assert_eq!(
        status, libc::SIGSEGV,
        "the SIGSEGV crash primitive must map to WTERMSIG(SIGSEGV)"
    );

    // (c) negative controls — a clean exit and a FATAL exit(1) are NOT
    //     crashes: the reaper's status0/status1 arm reclaims the slot and
    //     keeps the server running (this is what makes D-5 distinct from D-1).
    assert_eq!(
        panic_payload_to_exit_status(&ipc::ProcExitThread { code: 0 }),
        0,
        "a clean proc_exit(0) is WIFEXITED status 0, not a crash"
    );
    assert_eq!(
        panic_payload_to_exit_status(&ipc::ProcExitThread { code: 1 }),
        1 << 8,
        "a FATAL exit(1) is WIFEXITED status 1 (slot reclaimed, no crash cycle)"
    );
}

/// I-2 [INVARIANT-only] (P0) hazard #1: `panic = "abort"` silently converts
/// the `catch_unwind` firewall into a whole-process `abort()` on the first
/// backend panic — every session dies at once, with none of C's crash
/// restart. The firewall only exists if the build unwinds.
#[test]
fn i2_build_is_panic_unwind_not_abort() {
    assert!(
        cfg!(panic = "unwind"),
        "the backend crash firewall requires panic=unwind"
    );
    assert!(
        !cfg!(panic = "abort"),
        "panic=abort breaks the I-1 backend crash firewall (I-2 P0 hazard #1)"
    );
}
