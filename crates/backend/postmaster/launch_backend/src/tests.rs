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
    init_small::globals::set_work_mem(4321);
    init_small::globals::SetPostmasterPid(42);
    init_small::globals::SetDataDir("/tmp/pg-launch-test");

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

#[test]
#[should_panic(expected = "CheckpointerMain (backend-postmaster-checkpointer) unported")]
fn unported_child_kind_panics_loudly() {
    install();
    init_small::globals::SetIsPostmasterEnvironment(true);
    init_small::globals::SetIsUnderPostmaster(false);
    postmaster_child_launch(BackendType::Checkpointer, 1, StartupData::None, None);
}

#[test]
#[should_panic(expected = "no main_fn")]
fn null_main_fn_kind_panics_loudly() {
    install();
    init_small::globals::SetIsPostmasterEnvironment(true);
    init_small::globals::SetIsUnderPostmaster(false);
    postmaster_child_launch(BackendType::StandaloneBackend, 1, StartupData::None, None);
}
