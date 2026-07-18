use super::*;
use init_small::globals as g;
use std::sync::{Mutex, Once};
use types_core::{BackendType, ProcNumber};

const MAX_CONNECTIONS: i32 = 16;
const MAX_WORKER_PROCESSES: i32 = 2;
const NUM_SPECIAL: i32 = 2;
const MAX_BACKENDS: i32 = MAX_CONNECTIONS + 3 + MAX_WORKER_PROCESSES + 2 + NUM_SPECIAL;

thread_local! {
    static ISO_USES_XACT_SNAPSHOT: Cell<bool> = const { Cell::new(false) };
    static CURCID: Cell<CommandId> = const { Cell::new(0) };
    static REMEMBERED: Cell<i32> = const { Cell::new(0) };
    static FORGOTTEN: Cell<i32> = const { Cell::new(0) };
    static THREAD_PROC: Cell<bool> = const { Cell::new(false) };
}

fn test_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: Mutex<()> = Mutex::new(());
    LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

fn setup() {
    static SETUP: Once = Once::new();
    SETUP.call_once(|| {
        g::SetMaxConnections(MAX_CONNECTIONS);
        g::set_max_worker_processes(MAX_WORKER_PROCESSES);
        g::SetMaxBackends(MAX_BACKENDS);
        g::SetMyProcPid(777);

        pg_sema_seams::pg_semaphore_create::set(|_| {});
        pg_sema_seams::pg_semaphore_reset::set(|_| {});
        pg_sema_seams::pg_semaphore_lock::set(|_| {});
        pg_sema_seams::pg_semaphore_unlock::set(|_| {});
        s_lock_seams::perform_spin_delay::set(|_| std::thread::yield_now());
        s_lock_seams::finish_spin_delay::set(|_| {});
        s_lock_seams::set_spins_per_delay::set(|_| {});
        s_lock_seams::update_spins_per_delay::set(|v| v);
        latch_seams::own_latch::set(|_| {});
        latch_seams::disown_latch::set(|_| {});
        latch_seams::set_latch::set(|_| {});
        latch_seams::set_latch_my_latch::set(|| {});
        latch_seams::wait_latch_my_latch::set(|_, _, _| 0);
        latch_seams::reset_latch_my_latch::set(|| {});
        miscinit_seams::switch_to_shared_latch::set(|| {});
        miscinit_seams::switch_back_to_local_latch::set(|| {});
        waitevent_seams::pgstat_set_wait_event_storage::set(|_| {});
        waitevent_seams::pgstat_report_wait_start::set(|_| {});
        waitevent_seams::pgstat_report_wait_end::set(|| {});
        waitevent_seams::pgstat_reset_wait_event_storage::set(|| {});
        ipc_seams::on_shmem_exit::set(|_, _| {});
        deadlock_seams::init_dead_lock_checking::set(|| Ok(()));
        pmsignal_seams::register_postmaster_child_active::set(|| {});
        syncrep_seams::sync_rep_cleanup_at_proc_exit::set(|| {});
        condition_variable_seams::condition_variable_cancel_sleep::set(|| false);
        autovacuum_seams::wake_autovacuum_launcher::set(|| {});
        lock_seams::abort_strong_lock_acquire::set(|| {});
        lock_seams::get_awaited_lock_hashcode::set(|| None);
        lock_seams::lock_release_all::set(|_, _| Ok(()));
        timeout_seams::disable_timeouts::set(|_| {});
        shmem_seams::add_size::set(|a, b| Ok(a.checked_add(b).expect("size overflow")));
        shmem_seams::mul_size::set(|a, b| Ok(a.checked_mul(b).expect("size overflow")));
        shmem_seams::shmem_alloc::set(|size| {
            Ok(Box::leak(vec![0u8; size].into_boxed_slice()).as_mut_ptr())
        });

        xact_seams::get_current_command_id::set(|_| Ok(CURCID.get()));
        xact_seams::get_current_transaction_nest_level::set(|| 1);
        xact_seams::is_in_parallel_mode::set(|| false);
        // Export/Import surface (the inc-5 fd-reroute tests).
        xact_seams::get_top_transaction_id_if_any::set(|| InvalidTransactionId);
        xact_seams::is_sub_transaction::set(|| false);
        xact_seams::get_current_sub_transaction_id::set(|| 1);
        xact_seams::xact_get_committed_children::set(|| Ok(Vec::new()));
        xact_seams::get_xact_iso_level::set(|| 1);
        xact_seams::xact_read_only::set(|| false);
        g::SetMyDatabaseId(5);
        fd::init_seams();
        fd::InitFileAccess();
        xact_seams::isolation_uses_xact_snapshot::set(|| ISO_USES_XACT_SNAPSHOT.get());
        xact_seams::isolation_is_serializable::set(|| false);
        xact_seams::transaction_id_is_current_transaction_id::set(|_| false);
        transam_xlog_seams::recovery_in_progress::set(|| false);
        subtrans_seams::sub_trans_get_topmost_transaction::set(Ok);
        syscache_seams::relation_invalidates_snapshots_only::set(|_| false);
        syscache_seams::relation_has_sys_cache::set(|_| true);
        resowner_seams::current_resource_owner::set(|| ResourceOwner::NULL);
        resowner_seams::resource_owner_enlarge::set(|_| Ok(()));
        resowner_seams::resource_owner_remember_snapshot::set(|_, _| {
            REMEMBERED.set(REMEMBERED.get() + 1)
        });
        resowner_seams::resource_owner_forget_snapshot::set(|_, _| {
            FORGOTTEN.set(FORGOTTEN.get() + 1)
        });

        lwlock::CreateLWLocks(false).unwrap();
        lmgr_proc::init_seams();
        lmgr_proc::InitProcGlobal(&lmgr_proc::ProcGlobalConfig {
            autovacuum_worker_slots: 3,
            max_wal_senders: 2,
            max_prepared_xacts: 2,
            fastpath_lock_groups_per_backend: 1,
        });
        procarray::init_seams();
        varsup::VarsupShmemInit();
        procarray::ProcArrayShmemInit();
        init_seams();
    });
}

fn my_backend() -> ProcNumber {
    setup();
    if !THREAD_PROC.get() {
        g::SetMyProcPid(777);
        lmgr_proc::InitProcess(BackendType::Backend).expect("InitProcess");
        procarray::ProcArrayAdd(lmgr_proc::MyProc().unwrap()).expect("ProcArrayAdd");
        THREAD_PROC.set(true);
    }
    lmgr_proc::MyProc().unwrap()
}

fn end_xact() {
    AtEOXact_Snapshot(true, true).expect("AtEOXact_Snapshot");
}

#[test]
fn statement_flow_hits_reuse_fastpath() {
    let _g = test_lock();
    my_backend();

    let builds0 = procarray::snapshot_full_builds();
    let hits0 = procarray::snapshot_reuse_hits();
    let repl0 = static_snapshot_replacements();

    // Statement 1: acquire, push active (copies), drop the returned handle.
    let snap = GetTransactionSnapshot().unwrap();
    PushActiveSnapshot(&snap).unwrap();
    drop(snap);
    PopActiveSnapshot().unwrap();

    // Statement 2 in the same transaction: same static struct, no xact
    // completed => procarray's GetSnapshotDataReuse must be CALLED and hit.
    let snap = GetTransactionSnapshot().unwrap();
    drop(snap);

    assert_eq!(procarray::snapshot_full_builds(), builds0 + 1);
    assert_eq!(
        procarray::snapshot_reuse_hits(),
        hits0 + 1,
        "snapmgr did not route the second acquisition through the reuse fastpath"
    );
    assert_eq!(
        static_snapshot_replacements(),
        repl0,
        "static snapshot struct was replaced; array reuse defeated"
    );

    end_xact();
}

#[test]
fn first_snapshot_sets_and_eoxact_clears_xmin() {
    let _g = test_lock();
    let me = my_backend();

    let snap = GetTransactionSnapshot().unwrap();
    assert!(FirstSnapshotSet());
    assert_eq!(lmgr_proc::GetPGProcByNumber(me).xmin.read(), snap.xmin);
    assert_eq!(TransactionXmin(), snap.xmin);
    assert_eq!(RecentXmin(), snap.xmin);
    drop(snap);

    end_xact();
    assert!(!FirstSnapshotSet());
    assert_eq!(lmgr_proc::GetPGProcByNumber(me).xmin.read(), InvalidTransactionId);
    assert_eq!(TransactionXmin(), InvalidTransactionId);
}

#[test]
fn active_stack_copies_static_and_tracks_counts() {
    let _g = test_lock();
    my_backend();

    let snap = GetTransactionSnapshot().unwrap();
    assert!(!ActiveSnapshotSet());
    PushActiveSnapshot(&snap).unwrap();
    assert!(ActiveSnapshotSet());

    let active = GetActiveSnapshot();
    // The static must have been copied on push.
    assert!(!Rc::ptr_eq(&active, &snap));
    assert!(active.copied);
    assert_eq!(active.active_count.get(), 1);

    CURCID.set(5);
    UpdateActiveSnapshotCommandId().unwrap();
    assert_eq!(active.curcid.get(), 5);
    CURCID.set(0);

    PopActiveSnapshot().unwrap();
    assert!(!ActiveSnapshotSet());
    assert!(PopActiveSnapshot().is_err());

    drop((snap, active));
    end_xact();
}

#[test]
fn register_unregister_lifecycle() {
    let _g = test_lock();
    let me = my_backend();

    let snap = GetTransactionSnapshot().unwrap();
    let r0 = REMEMBERED.get();
    let reg = RegisterSnapshot(Some(&snap)).unwrap().unwrap();
    assert!(!Rc::ptr_eq(&reg, &snap));
    assert_eq!(reg.regd_count.get(), 1);
    assert_eq!(REMEMBERED.get(), r0 + 1);
    assert!(!ThereAreNoPriorRegisteredSnapshots() || with_state(|s| s.registered.len() <= 1));
    assert!(HaveRegisteredOrActiveSnapshot());
    drop(snap);

    // The registration pins MyProc->xmin while it lives.
    let pinned = lmgr_proc::GetPGProcByNumber(me).xmin.read();
    assert_ne!(pinned, InvalidTransactionId);

    let f0 = FORGOTTEN.get();
    UnregisterSnapshot(Some(&reg));
    assert_eq!(reg.regd_count.get(), 0);
    assert_eq!(FORGOTTEN.get(), f0 + 1);
    drop(reg);
    assert_eq!(lmgr_proc::GetPGProcByNumber(me).xmin.read(), InvalidTransactionId);
    assert!(!HaveRegisteredOrActiveSnapshot());

    end_xact();
}

#[test]
fn catalog_snapshot_registers_and_invalidates() {
    let _g = test_lock();
    my_backend();

    let cat = GetCatalogSnapshot(1259).unwrap();
    assert!(with_state(|s| s.catalog_valid));
    assert!(with_state(|s| s.registered.len() == 1));
    // Catalog-only registration doesn't count for HaveRegisteredOrActive.
    assert!(!HaveRegisteredOrActiveSnapshot());
    drop(cat);

    InvalidateCatalogSnapshot();
    assert!(!with_state(|s| s.catalog_valid));
    assert!(with_state(|s| s.registered.is_empty()));

    let _ = GetCatalogSnapshot(1259).unwrap();
    InvalidateCatalogSnapshotConditionally();
    assert!(!with_state(|s| s.catalog_valid));

    end_xact();
}

#[test]
fn repeatable_read_returns_registered_copy() {
    let _g = test_lock();
    my_backend();
    ISO_USES_XACT_SNAPSHOT.set(true);

    let first = GetTransactionSnapshot().unwrap();
    assert!(first.copied);
    assert_eq!(first.regd_count.get(), 1);

    let second = GetTransactionSnapshot().unwrap();
    assert!(Rc::ptr_eq(&first, &second));

    ISO_USES_XACT_SNAPSHOT.set(false);
    drop((first, second));
    end_xact();
}

#[test]
fn snapshot_set_command_id_reaches_current() {
    let _g = test_lock();
    my_backend();

    let snap = GetTransactionSnapshot().unwrap();
    SnapshotSetCommandId(9);
    assert_eq!(snap.curcid.get(), 9);
    drop(snap);
    end_xact();
}

#[test]
fn subxact_stack_relabels_and_aborts() {
    let _g = test_lock();
    my_backend();

    let snap = GetTransactionSnapshot().unwrap();
    PushActiveSnapshotWithLevel(&snap, 1).unwrap();
    PushActiveSnapshotWithLevel(&snap, 2).unwrap();
    AtSubCommit_Snapshot(2);
    assert!(with_state(|s| s.active.iter().all(|e| e.as_level == 1)));

    PushActiveSnapshotWithLevel(&snap, 2).unwrap();
    AtSubAbort_Snapshot(2).unwrap();
    assert!(with_state(|s| s.active.len() == 2));

    PopActiveSnapshot().unwrap();
    PopActiveSnapshot().unwrap();
    drop(snap);
    end_xact();
}

#[test]
fn xid_in_mvcc_snapshot_matches_c_shape() {
    let _g = test_lock();
    my_backend();
    let mcx = with_state(|s| s.mcx);

    let mut snap = SnapshotData::sentinel(mcx, types_snapshot::SnapshotType::SNAPSHOT_MVCC);
    snap.xmin = 10;
    snap.xmax = 20;
    snap.xip.extend_from_slice(&[12, 15]);
    snap.xcnt = 2;
    snap.subxip.extend_from_slice(&[16]);
    snap.subxcnt = 1;

    assert!(!XidInMVCCSnapshot(5, &snap).unwrap()); // < xmin
    assert!(XidInMVCCSnapshot(25, &snap).unwrap()); // >= xmax
    assert!(XidInMVCCSnapshot(12, &snap).unwrap()); // in xip
    assert!(XidInMVCCSnapshot(16, &snap).unwrap()); // in subxip
    assert!(!XidInMVCCSnapshot(13, &snap).unwrap());

    // Overflowed: subxip is ignored; xid maps to itself via subtrans stub.
    snap.suboverflowed = true;
    assert!(!XidInMVCCSnapshot(16, &snap).unwrap());
    assert!(XidInMVCCSnapshot(15, &snap).unwrap());
}

#[test]
fn serialize_restore_roundtrip_across_thread() {
    let _g = test_lock();
    my_backend();
    let mcx = with_state(|s| s.mcx);

    let mut d = SnapshotData::sentinel(mcx, types_snapshot::SnapshotType::SNAPSHOT_MVCC);
    d.xmin = 100;
    d.xmax = 200;
    d.xip.extend_from_slice(&[110, 120, 150]);
    d.xcnt = 3;
    d.subxip.extend_from_slice(&[130, 140]);
    d.subxcnt = 2;
    d.takenDuringRecovery = false;
    d.curcid.set(7);
    d.vistest = types_core::GlobalVisStateHandle::new(3);
    let src: Snapshot = Rc::new(d);

    let ser = SerializeSnapshot(&src);
    assert_eq!(ser.xmin, 100);
    assert_eq!(ser.xmax, 200);
    assert_eq!(ser.xip, vec![110, 120, 150]);
    assert_eq!(ser.subxip, vec![130, 140]);
    assert_eq!(ser.curcid, 7);

    std::thread::spawn(move || {
        let restored = RestoreSnapshot(&ser);
        assert_eq!(restored.snapshot_type, types_snapshot::SnapshotType::SNAPSHOT_MVCC);
        assert_eq!(restored.xmin, 100);
        assert_eq!(restored.xmax, 200);
        assert_eq!(restored.xip[..], [110, 120, 150]);
        assert_eq!(restored.xcnt, 3);
        assert_eq!(restored.subxip[..], [130, 140]);
        assert_eq!(restored.subxcnt, 2);
        assert!(!restored.suboverflowed);
        assert!(!restored.takenDuringRecovery);
        assert!(restored.copied);
        assert_eq!(restored.curcid.get(), 7);
        assert_eq!(restored.vistest, types_core::GlobalVisStateHandle::new(3));
        assert_eq!(restored.active_count.get(), 0);
        assert_eq!(restored.regd_count.get(), 0);
        assert_eq!(restored.snapXactCompletionCount, 0);
    })
    .join()
    .unwrap();
}

#[test]
fn serialize_drops_overflowed_subxip_unless_recovery() {
    let _g = test_lock();
    my_backend();
    let mcx = with_state(|s| s.mcx);

    let mut d = SnapshotData::sentinel(mcx, types_snapshot::SnapshotType::SNAPSHOT_MVCC);
    d.xmin = 10;
    d.xmax = 20;
    d.subxip.extend_from_slice(&[12, 15]);
    d.subxcnt = 2;
    d.suboverflowed = true;
    let mut src: Snapshot = Rc::new(d);

    let ser = SerializeSnapshot(&src);
    assert!(ser.suboverflowed);
    assert!(ser.subxip.is_empty());

    Rc::get_mut(&mut src).unwrap().takenDuringRecovery = true;
    let ser = SerializeSnapshot(&src);
    assert_eq!(ser.subxip, vec![12, 15]);
    let restored = RestoreSnapshot(&ser);
    assert_eq!(restored.subxip[..], [12, 15]);
    assert!(restored.suboverflowed);
    assert!(restored.takenDuringRecovery);
}

#[test]
fn historic_snapshot_short_circuits_acquisition() {
    let _g = test_lock();
    my_backend();

    let snap = GetTransactionSnapshot().unwrap();
    let historic = CopySnapshot(&snap);
    drop(snap);
    end_xact();

    SetupHistoricSnapshot(historic.clone(), None);
    assert!(HistoricSnapshotActive());
    let got = GetTransactionSnapshot().unwrap();
    assert!(Rc::ptr_eq(&got, &historic));
    let got = GetCatalogSnapshot(1259).unwrap();
    assert!(Rc::ptr_eq(&got, &historic));
    TeardownHistoricSnapshot(false);
    assert!(!HistoricSnapshotActive());
    drop((got, historic));
}

#[test]
fn panic_inside_with_state_does_not_poison_the_session() {
    let _g = test_lock();
    my_backend();

    // Wedge regression (d1a86f62f): a converted-panic ERROR raised inside the
    // with_state closure (the GetSafeSnapshot loud shape) unwinds to the
    // main-loop catch; every later snapmgr call must still work, or the
    // session errors "with_state re-entered" forever.
    let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        with_state(|_| -> () { panic!("injected loud inside with_state") })
    }));
    assert!(unwound.is_err());

    let snap = GetTransactionSnapshot().unwrap();
    PushActiveSnapshot(&snap).unwrap();
    drop(snap);
    PopActiveSnapshot().unwrap();

    AtEOXact_Snapshot(false, true).expect("abort-path AtEOXact_Snapshot");
    assert!(with_state(|s| s.active.is_empty() && s.registered.is_empty()));
    assert!(!with_state(|s| s.first_snapshot_set));
}

// ---------------------------------------------------------------------------
// pg_snapshots fs surface (the P4 inc-5 fd-reroute, closing the inc-4
// ledger's "ImportSnapshot read surface" allowlist rows): the export/import
// files are DATADIR-RELATIVE and must resolve through the fd -> vfs choke
// (the relmapper/initfile dataplane precedent) — PosixVfs in the entered
// real cwd under the default cfg, the SimVfs namespace (which has no cwd:
// relative paths resolve at the sim root) under --cfg pgrust_sim.
// ---------------------------------------------------------------------------

fn cpath(path: &str) -> std::ffi::CString {
    std::ffi::CString::new(path).unwrap()
}

fn vfs_mkdir(dir: &str) {
    let rc = vfs::mkdir(&cpath(dir), 0o700);
    assert!(
        rc == 0 || vfs::get_errno() == libc::EEXIST,
        "vfs_mkdir({dir}): errno {}",
        vfs::get_errno()
    );
}

fn vfs_read_file(path: &str) -> Vec<u8> {
    let fd = vfs::open(&cpath(path), libc::O_RDONLY, 0);
    assert!(fd >= 0, "vfs_read_file open({path}): errno {}", vfs::get_errno());
    let mut out = Vec::new();
    let mut buf = [0u8; 4096];
    let mut off = 0i64;
    loop {
        let n = vfs::pread(fd, &mut buf, off);
        assert!(n >= 0, "vfs_read_file pread({path}): errno {}", vfs::get_errno());
        if n == 0 {
            break;
        }
        out.extend_from_slice(&buf[..n as usize]);
        off += n as i64;
    }
    assert_eq!(vfs::close(fd), 0);
    out
}

static CWD: Mutex<()> = Mutex::new(());

fn enter_dir(dir: &str) -> std::sync::MutexGuard<'static, ()> {
    let guard = CWD.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    std::env::set_current_dir(dir).unwrap();
    guard
}

fn scratch_dir(tag: &str) -> String {
    let dir = std::env::temp_dir().join(format!("pgrust_snapmgr_{}_{tag}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir.to_str().unwrap().to_owned()
}

#[test]
fn export_snapshot_and_eoxact_unlink_are_fd_routed() {
    let _g = test_lock();
    my_backend();
    let root = scratch_dir("snapexport");
    let _cwd = enter_dir(&root);
    // The vfs domain's mirror of the relative export dir (EEXIST no-op in
    // the entered real cwd under the default cfg; minted at the sim root
    // under --cfg pgrust_sim).
    vfs_mkdir("pg_snapshots");

    let snap = GetTransactionSnapshot().unwrap();
    let name = ExportSnapshot(&snap).unwrap();
    drop(snap);
    let path = format!("pg_snapshots/{name}");
    let content = vfs_read_file(&path);
    assert!(
        content.starts_with(b"vxid:"),
        "export file must exist in the vfs domain with the C layout"
    );
    // The tmp staging file was renamed away (fd::pg_rename, not std::fs).
    assert!(
        vfs::open(&cpath(&format!("{path}.tmp")), libc::O_RDONLY, 0) < 0,
        "the .tmp staging file must not survive the rename"
    );

    // AtEOXact_Snapshot(commit): the export file unlinks through fd::pg_unlink.
    end_xact();
    assert!(
        vfs::open(&cpath(&path), libc::O_RDONLY, 0) < 0,
        "export file must be unlinked from the vfs domain at EOXact"
    );
}

#[test]
fn import_snapshot_missing_file_is_fd_routed() {
    let _g = test_lock();
    my_backend();
    let root = scratch_dir("snapimport");
    let _cwd = enter_dir(&root);
    vfs_mkdir("pg_snapshots");
    end_xact(); // FirstSnapshotSet must be false for the precondition gate

    ISO_USES_XACT_SNAPSHOT.set(true);
    let err = ImportSnapshot("00000000-00000000-1").unwrap_err();
    ISO_USES_XACT_SNAPSHOT.set(false);
    // The fd-routed open reported ENOENT: C's "snapshot does not exist" arm.
    assert!(
        err.message.contains("does not exist"),
        "unexpected ImportSnapshot error: {}",
        err.message
    );
}
