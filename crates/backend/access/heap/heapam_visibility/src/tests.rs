use super::*;
use ::mcx::MemoryContext;
use ::types_snapshot::SnapshotType;
use ::types_tuple::ItemPointerData;
use init_small::globals as g;
use std::cell::Cell;
use std::sync::{Mutex, Once};
use types_core::xact::FirstNormalTransactionId;
use types_core::BackendType;

// one PGPROC per test thread (libtest never reuses threads); keep headroom
const MAX_CONNECTIONS: i32 = 40;
const MAX_BACKENDS: i32 = MAX_CONNECTIONS + 3 + 2 + 2 + 2;
const BUF: Buffer = 1;

thread_local! {
    static DID_COMMIT: Cell<bool> = const { Cell::new(false) };
    static COMMIT_LSN: Cell<u64> = const { Cell::new(0) };
    static PERMANENT: Cell<bool> = const { Cell::new(false) };
    static NEEDS_FLUSH: Cell<bool> = const { Cell::new(false) };
    static PAGE_LSN: Cell<u64> = const { Cell::new(0) };
    static DIRTY_CALLS: Cell<u32> = const { Cell::new(0) };
    static REMOVABLE: Cell<bool> = const { Cell::new(false) };
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
        g::set_max_worker_processes(2);
        g::SetMaxBackends(MAX_BACKENDS);
        g::SetMyProcPid(778);

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
        xact_seams::transaction_id_is_current_transaction_id::set(
            ::xact::TransactionIdIsCurrentTransactionId,
        );
        transam_xlog_seams::recovery_in_progress::set(|| false);
        subtrans_seams::sub_trans_get_topmost_transaction::set(Ok);

        transam_seams::transaction_id_did_commit::set(|_| Ok(DID_COMMIT.get()));
        transam_seams::transaction_id_get_commit_lsn::set(|_| Ok(COMMIT_LSN.get()));
        transam_xlog_seams::xlog_needs_flush::set(|_| NEEDS_FLUSH.get());
        bufmgr_seams::buffer_is_permanent::set(|_| PERMANENT.get());
        bufmgr_seams::buffer_get_lsn_atomic::set(|_| PAGE_LSN.get());
        bufmgr_seams::mark_buffer_dirty_hint::set(|_, _| {
            DIRTY_CALLS.set(DIRTY_CALLS.get() + 1);
            Ok(())
        });
        combocid_seams::heap_tuple_header_get_cmin::set(|t| t.raw_command_id());
        combocid_seams::heap_tuple_header_get_cmax::set(|t| t.raw_command_id());
        multixact_seams::multi_xact_id_is_running::set(|_, _| Ok(false));
        procarray_seams::global_vis_test_is_removable_xid::set(|_, _| Ok(REMOVABLE.get()));

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

fn boot() {
    setup();
    if !THREAD_PROC.get() {
        g::SetMyProcPid(778);
        lmgr_proc::InitProcess(BackendType::Backend).expect("InitProcess");
        procarray::ProcArrayAdd(lmgr_proc::MyProc().unwrap()).expect("ProcArrayAdd");
        THREAD_PROC.set(true);
    }
    DID_COMMIT.set(false);
    COMMIT_LSN.set(0);
    PERMANENT.set(false);
    NEEDS_FLUSH.set(false);
    PAGE_LSN.set(0);
    DIRTY_CALLS.set(0);
    REMOVABLE.set(false);
}

// xid == latestCompletedXid (FirstNormal): procarray answers not-in-progress.
const XID_DONE: TransactionId = FirstNormalTransactionId;
// xid > latestCompletedXid: procarray answers in-progress.
const XID_RUNNING: TransactionId = FirstNormalTransactionId + 7;

#[repr(align(8))]
struct Image([u8; 32]);

struct TestTuple(Box<Image>);

impl TestTuple {
    fn new(xmin: TransactionId, xmax: TransactionId, infomask: u16) -> Self {
        let mut t = TestTuple(Box::new(Image([0; 32])));
        let hdr = t.hdr_mut();
        hdr.set_xmin(xmin);
        hdr.set_xmax(xmax);
        hdr.t_infomask = infomask;
        hdr.t_hoff = 24;
        hdr.t_ctid = ItemPointerData::new(0, 1);
        t
    }

    fn hdr_mut(&mut self) -> &mut HeapTupleHeaderData {
        // SAFETY: 32-byte 8-aligned zero-init image, exclusively owned.
        unsafe { &mut *self.0 .0.as_mut_ptr().cast::<HeapTupleHeaderData>() }
    }

    fn htup(&mut self) -> HeapTupleData<'_> {
        // SAFETY: live owned image, t_len 24 >= header size; as_mut_ptr keeps
        // write provenance for t_data_mut.
        unsafe {
            HeapTupleData::from_raw_parts(
                self.0 .0.as_mut_ptr(),
                24,
                ItemPointerData::new(0, 1),
                1000,
            )
        }
    }

    fn infomask(&mut self) -> u16 {
        self.hdr_mut().t_infomask
    }
}

fn mvcc_snapshot<'m>(mcx: ::mcx::Mcx<'m>, xmin: TransactionId, xmax: TransactionId) -> SnapshotData<'m> {
    let mut s = SnapshotData::sentinel(mcx, SnapshotType::SNAPSHOT_MVCC);
    s.xmin = xmin;
    s.xmax = xmax;
    s.regd_count.set(1);
    s
}

#[test]
fn mvcc_hinted_committed_is_visible_without_clog() {
    let _g = test_lock();
    boot();
    let cx = MemoryContext::new("test");
    let snap = mvcc_snapshot(cx.mcx(), 10, 20);

    let mut t = TestTuple::new(5, 0, HEAP_XMIN_COMMITTED | HEAP_XMAX_INVALID);
    assert!(HeapTupleSatisfiesMVCC(&mut t.htup(), &snap, BUF).unwrap());
    assert_eq!(t.infomask(), HEAP_XMIN_COMMITTED | HEAP_XMAX_INVALID);
    assert_eq!(DIRTY_CALLS.get(), 0);
}

#[test]
fn mvcc_hinted_xmin_in_snapshot_is_invisible() {
    let _g = test_lock();
    boot();
    let cx = MemoryContext::new("test");
    let mut snap = mvcc_snapshot(cx.mcx(), 50, 200);
    snap.xip.push(100);
    snap.xcnt = 1;

    let mut t = TestTuple::new(100, 0, HEAP_XMIN_COMMITTED | HEAP_XMAX_INVALID);
    assert!(!HeapTupleSatisfiesMVCC(&mut t.htup(), &snap, BUF).unwrap());

    // >= snapshot xmax is in-progress too.
    let mut t = TestTuple::new(300, 0, HEAP_XMIN_COMMITTED | HEAP_XMAX_INVALID);
    assert!(!HeapTupleSatisfiesMVCC(&mut t.htup(), &snap, BUF).unwrap());
}

#[test]
fn mvcc_derives_xmin_committed_hint_through_page_image() {
    let _g = test_lock();
    boot();
    DID_COMMIT.set(true);
    let cx = MemoryContext::new("test");
    let snap = mvcc_snapshot(cx.mcx(), 10, 20);

    let mut t = TestTuple::new(XID_DONE, 0, HEAP_XMAX_INVALID);
    assert!(HeapTupleSatisfiesMVCC(&mut t.htup(), &snap, BUF).unwrap());
    assert_eq!(t.infomask() & HEAP_XMIN_COMMITTED, HEAP_XMIN_COMMITTED);
    assert_eq!(DIRTY_CALLS.get(), 1);
}

#[test]
fn mvcc_derives_xmin_invalid_hint_on_abort() {
    let _g = test_lock();
    boot();
    DID_COMMIT.set(false);
    let cx = MemoryContext::new("test");
    let snap = mvcc_snapshot(cx.mcx(), 10, 20);

    let mut t = TestTuple::new(XID_DONE, 0, HEAP_XMAX_INVALID);
    assert!(!HeapTupleSatisfiesMVCC(&mut t.htup(), &snap, BUF).unwrap());
    assert_eq!(t.infomask() & HEAP_XMIN_INVALID, HEAP_XMIN_INVALID);
}

#[test]
fn set_hint_bits_honors_lsn_interlock() {
    let _g = test_lock();
    boot();
    PERMANENT.set(true);
    NEEDS_FLUSH.set(true);
    COMMIT_LSN.set(0x2000);
    PAGE_LSN.set(0x1000);

    let mut t = TestTuple::new(XID_DONE, 0, 0);
    HeapTupleSetHintBits(t.hdr_mut(), BUF, HEAP_XMIN_COMMITTED, XID_DONE).unwrap();
    assert_eq!(t.infomask(), 0);
    assert_eq!(DIRTY_CALLS.get(), 0);

    PAGE_LSN.set(0x2000);
    HeapTupleSetHintBits(t.hdr_mut(), BUF, HEAP_XMIN_COMMITTED, XID_DONE).unwrap();
    assert_eq!(t.infomask(), HEAP_XMIN_COMMITTED);
    assert_eq!(DIRTY_CALLS.get(), 1);
}

#[test]
fn vacuum_reports_live_dead_and_horizon() {
    let _g = test_lock();
    boot();

    let mut t = TestTuple::new(5, 0, HEAP_XMIN_COMMITTED | HEAP_XMAX_INVALID);
    assert_eq!(
        HeapTupleSatisfiesVacuum(&mut t.htup(), XID_DONE, BUF).unwrap(),
        HEAPTUPLE_LIVE
    );

    let mut t = TestTuple::new(5, 0, HEAP_XMIN_INVALID);
    assert_eq!(
        HeapTupleSatisfiesVacuum(&mut t.htup(), XID_DONE, BUF).unwrap(),
        HEAPTUPLE_DEAD
    );

    let mask = HEAP_XMIN_COMMITTED | HEAP_XMAX_COMMITTED;
    let mut t = TestTuple::new(5, XID_DONE, mask);
    assert_eq!(
        HeapTupleSatisfiesVacuum(&mut t.htup(), XID_DONE, BUF).unwrap(),
        HEAPTUPLE_RECENTLY_DEAD
    );
    let mut t = TestTuple::new(5, XID_DONE, mask);
    assert_eq!(
        HeapTupleSatisfiesVacuum(&mut t.htup(), XID_DONE + 1, BUF).unwrap(),
        HEAPTUPLE_DEAD
    );
}

#[test]
fn vacuum_unhinted_xmin_consults_procarray() {
    let _g = test_lock();
    boot();

    let mut t = TestTuple::new(XID_RUNNING, 0, HEAP_XMAX_INVALID);
    assert_eq!(
        HeapTupleSatisfiesVacuum(&mut t.htup(), XID_DONE, BUF).unwrap(),
        HEAPTUPLE_INSERT_IN_PROGRESS
    );

    DID_COMMIT.set(true);
    let mut t = TestTuple::new(XID_DONE, 0, HEAP_XMAX_INVALID);
    assert_eq!(
        HeapTupleSatisfiesVacuum(&mut t.htup(), XID_DONE, BUF).unwrap(),
        HEAPTUPLE_LIVE
    );
    assert_eq!(t.infomask() & HEAP_XMIN_COMMITTED, HEAP_XMIN_COMMITTED);
}

#[test]
fn self_snapshot_clears_aborted_xmax() {
    let _g = test_lock();
    boot();
    DID_COMMIT.set(false);

    let mut t = TestTuple::new(5, XID_DONE, HEAP_XMIN_COMMITTED);
    assert!(HeapTupleSatisfiesSelf(&mut t.htup(), BUF).unwrap());
    assert_eq!(t.infomask() & HEAP_XMAX_INVALID, HEAP_XMAX_INVALID);
}

#[test]
fn dirty_reports_in_progress_inserter() {
    let _g = test_lock();
    boot();
    let cx = MemoryContext::new("test");
    let mut snap = SnapshotData::sentinel(cx.mcx(), SnapshotType::SNAPSHOT_DIRTY);

    let mut t = TestTuple::new(XID_RUNNING, 0, HEAP_XMAX_INVALID);
    assert!(HeapTupleSatisfiesDirty(&mut t.htup(), &mut snap, BUF).unwrap());
    assert_eq!(snap.xmin, XID_RUNNING);
    assert_eq!(snap.xmax, InvalidTransactionId);
}

#[test]
fn surely_dead_requires_hinted_committed_deleter() {
    let _g = test_lock();
    boot();
    REMOVABLE.set(true);
    let vt = GlobalVisStateHandle::new(1);

    let mut t = TestTuple::new(5, XID_DONE, HEAP_XMIN_COMMITTED | HEAP_XMAX_COMMITTED);
    assert!(HeapTupleIsSurelyDead(&t.htup(), vt).unwrap());

    let mut t = TestTuple::new(5, XID_DONE, HEAP_XMIN_COMMITTED);
    assert!(!HeapTupleIsSurelyDead(&t.htup(), vt).unwrap());

    let mut t = TestTuple::new(5, XID_DONE, HEAP_XMIN_INVALID);
    assert!(HeapTupleIsSurelyDead(&t.htup(), vt).unwrap());

    let mut t = TestTuple::new(5, XID_DONE, 0);
    assert!(!HeapTupleIsSurelyDead(&t.htup(), vt).unwrap());
}

#[test]
fn only_locked_infomask_paths() {
    let _g = test_lock();
    boot();

    let mut t = TestTuple::new(5, XID_DONE, HEAP_XMAX_INVALID);
    assert!(HeapTupleHeaderIsOnlyLocked(t.hdr_mut()).unwrap());

    let mut t = TestTuple::new(5, XID_DONE, HEAP_XMAX_LOCK_ONLY);
    assert!(HeapTupleHeaderIsOnlyLocked(t.hdr_mut()).unwrap());

    let mut t = TestTuple::new(5, InvalidTransactionId, 0);
    assert!(HeapTupleHeaderIsOnlyLocked(t.hdr_mut()).unwrap());

    let mut t = TestTuple::new(5, XID_DONE, 0);
    assert!(!HeapTupleHeaderIsOnlyLocked(t.hdr_mut()).unwrap());
}

#[test]
fn update_reports_deleted_vs_updated_by_ctid() {
    let _g = test_lock();
    boot();

    let mask = HEAP_XMIN_COMMITTED | HEAP_XMAX_INVALID;
    let mut t = TestTuple::new(5, 0, mask);
    assert_eq!(
        HeapTupleSatisfiesUpdate(&mut t.htup(), 1, BUF).unwrap(),
        TM_Ok
    );

    let mask = HEAP_XMIN_COMMITTED | HEAP_XMAX_COMMITTED;
    let mut t = TestTuple::new(5, XID_DONE, mask);
    assert_eq!(
        HeapTupleSatisfiesUpdate(&mut t.htup(), 1, BUF).unwrap(),
        TM_Deleted
    );

    let mut t = TestTuple::new(5, XID_DONE, mask);
    t.hdr_mut().t_ctid = ItemPointerData::new(0, 2);
    assert_eq!(
        HeapTupleSatisfiesUpdate(&mut t.htup(), 1, BUF).unwrap(),
        TM_Updated
    );
}

#[test]
fn seam_dispatch_covers_read_lane() {
    let _g = test_lock();
    boot();
    let cx = MemoryContext::new("test");
    let snap = mvcc_snapshot(cx.mcx(), 10, 20);

    let mut t = TestTuple::new(5, 0, HEAP_XMIN_COMMITTED | HEAP_XMAX_INVALID);
    assert!(heapam_visibility_seams::heap_tuple_satisfies_visibility::call(
        &mut t.htup(),
        &snap,
        BUF
    )
    .unwrap());

    let any = SnapshotData::sentinel(cx.mcx(), SnapshotType::SNAPSHOT_ANY);
    let mut t = TestTuple::new(5, 0, 0);
    assert!(heapam_visibility_seams::heap_tuple_satisfies_visibility::call(
        &mut t.htup(),
        &any,
        BUF
    )
    .unwrap());

    let mut t = TestTuple::new(5, XID_DONE, HEAP_XMIN_COMMITTED | HEAP_XMAX_COMMITTED);
    assert_eq!(
        heapam_visibility_seams::heap_tuple_satisfies_vacuum::call(&mut t.htup(), XID_DONE, BUF)
            .unwrap(),
        HEAPTUPLE_RECENTLY_DEAD
    );
}

#[test]
fn toast_rejects_super_deleted_xmin() {
    let _g = test_lock();
    boot();

    let mut t = TestTuple::new(InvalidTransactionId, 0, 0);
    assert!(!HeapTupleSatisfiesToast(&mut t.htup(), BUF).unwrap());

    let mut t = TestTuple::new(5, 0, HEAP_XMIN_COMMITTED);
    assert!(HeapTupleSatisfiesToast(&mut t.htup(), BUF).unwrap());
}

pub(crate) fn test_historic_rlocator() -> types_storage::RelFileLocator {
    types_storage::RelFileLocator::new(1663, 5, 16384)
}

fn historic_snapshot<'m>(
    mcx: ::mcx::Mcx<'m>,
    xmin: TransactionId,
    xmax: TransactionId,
    xip: &[TransactionId],
    subxip: &[TransactionId],
    curcid: CommandId,
) -> SnapshotData<'m> {
    let mut s = SnapshotData::sentinel(mcx, SnapshotType::SNAPSHOT_HISTORIC_MVCC);
    s.xmin = xmin;
    s.xmax = xmax;
    s.xip.extend_from_slice(xip);
    s.xcnt = xip.len() as u32;
    s.subxip.extend_from_slice(subxip);
    s.subxcnt = subxip.len() as i32;
    s.curcid.set(curcid);
    s
}

fn historic_boot() {
    boot();
    TEST_HISTORIC_RLOCATOR.with(|c| c.set(Some(test_historic_rlocator())));
    ::snapmgr::TeardownHistoricSnapshot(false);
}

#[test]
fn historic_xmin_states_without_cid_resolution() {
    let _g = test_lock();
    historic_boot();
    let cx = MemoryContext::new("test");

    let snap = historic_snapshot(cx.mcx(), 50, 100, &[], &[], 10);
    let mut t = TestTuple::new(60, 0, HEAP_XMIN_INVALID);
    assert!(!HeapTupleSatisfiesHistoricMVCC(&mut t.htup(), &snap, BUF).unwrap());

    let mut t = TestTuple::new(40, 0, HEAP_XMIN_COMMITTED | HEAP_XMAX_INVALID);
    DID_COMMIT.set(true);
    assert!(HeapTupleSatisfiesHistoricMVCC(&mut t.htup(), &snap, BUF).unwrap());

    DID_COMMIT.set(false);
    let mut t = TestTuple::new(40, 0, HEAP_XMAX_INVALID);
    assert!(!HeapTupleSatisfiesHistoricMVCC(&mut t.htup(), &snap, BUF).unwrap());

    DID_COMMIT.set(true);
    let mut t = TestTuple::new(40, 0, HEAP_XMAX_INVALID);
    assert!(HeapTupleSatisfiesHistoricMVCC(&mut t.htup(), &snap, BUF).unwrap());

    let mut t = TestTuple::new(100, 0, HEAP_XMAX_INVALID);
    assert!(!HeapTupleSatisfiesHistoricMVCC(&mut t.htup(), &snap, BUF).unwrap());

    let snap = historic_snapshot(cx.mcx(), 50, 100, &[60], &[], 10);
    let mut t = TestTuple::new(60, 0, HEAP_XMAX_INVALID);
    assert!(HeapTupleSatisfiesHistoricMVCC(&mut t.htup(), &snap, BUF).unwrap());

    let mut t = TestTuple::new(61, 0, HEAP_XMAX_INVALID);
    assert!(!HeapTupleSatisfiesHistoricMVCC(&mut t.htup(), &snap, BUF).unwrap());
}

#[test]
fn historic_xmax_states_without_cid_resolution() {
    let _g = test_lock();
    historic_boot();
    let cx = MemoryContext::new("test");
    let snap = historic_snapshot(cx.mcx(), 50, 100, &[60, 70], &[], 10);

    let mut t = TestTuple::new(60, 90, HEAP_XMIN_COMMITTED | HEAP_XMAX_LOCK_ONLY);
    assert!(HeapTupleSatisfiesHistoricMVCC(&mut t.htup(), &snap, BUF).unwrap());

    let mut t = TestTuple::new(60, 40, HEAP_XMIN_COMMITTED | HEAP_XMAX_COMMITTED);
    DID_COMMIT.set(true);
    assert!(!HeapTupleSatisfiesHistoricMVCC(&mut t.htup(), &snap, BUF).unwrap());

    let mut t = TestTuple::new(60, 40, 0);
    DID_COMMIT.set(true);
    assert!(!HeapTupleSatisfiesHistoricMVCC(&mut t.htup(), &snap, BUF).unwrap());
    DID_COMMIT.set(false);
    let mut t = TestTuple::new(60, 40, 0);
    assert!(HeapTupleSatisfiesHistoricMVCC(&mut t.htup(), &snap, BUF).unwrap());

    let mut t = TestTuple::new(60, 100, 0);
    assert!(HeapTupleSatisfiesHistoricMVCC(&mut t.htup(), &snap, BUF).unwrap());

    let mut t = TestTuple::new(60, 70, 0);
    assert!(!HeapTupleSatisfiesHistoricMVCC(&mut t.htup(), &snap, BUF).unwrap());

    let mut t = TestTuple::new(60, 71, 0);
    assert!(HeapTupleSatisfiesHistoricMVCC(&mut t.htup(), &snap, BUF).unwrap());
}

fn install_tuplecids(cmin: CommandId, cmax: CommandId) {
    use ::reorderbuffer::{ReorderBufferTupleCidEnt, ReorderBufferTupleCidKey, TupleCidHash};
    use std::cell::RefCell;
    use std::rc::Rc;

    let cx: &'static ::mcx::MemoryContext =
        Box::leak(Box::new(::mcx::MemoryContext::new("historic test")));
    let mut hash: TupleCidHash =
        ::mcx::PgFxHashMap::with_hasher_in(Default::default(), cx.mcx());
    hash.insert(
        ReorderBufferTupleCidKey {
            rlocator: test_historic_rlocator(),
            tid: ::types_tuple::ItemPointerData::new(0, 1),
        },
        ReorderBufferTupleCidEnt { cmin, cmax, combocid: InvalidCommandId },
    );
    let dummy = Rc::new(SnapshotData::sentinel(
        cx.mcx(),
        SnapshotType::SNAPSHOT_HISTORIC_MVCC,
    ));
    ::snapmgr::SetupHistoricSnapshot(dummy, Some(Rc::new(RefCell::new(hash))));
}

#[test]
fn historic_own_transaction_cid_resolution() {
    let _g = test_lock();
    historic_boot();
    let cx = MemoryContext::new("test");
    let snap = historic_snapshot(cx.mcx(), 50, 100, &[], &[80], 10);

    let mut t = TestTuple::new(80, 0, HEAP_XMAX_INVALID);
    assert!(!HeapTupleSatisfiesHistoricMVCC(&mut t.htup(), &snap, BUF).unwrap());

    install_tuplecids(5, InvalidCommandId);
    let mut t = TestTuple::new(80, 0, HEAP_XMAX_INVALID);
    assert!(HeapTupleSatisfiesHistoricMVCC(&mut t.htup(), &snap, BUF).unwrap());
    ::snapmgr::TeardownHistoricSnapshot(false);

    install_tuplecids(10, InvalidCommandId);
    let mut t = TestTuple::new(80, 0, HEAP_XMAX_INVALID);
    assert!(!HeapTupleSatisfiesHistoricMVCC(&mut t.htup(), &snap, BUF).unwrap());
    ::snapmgr::TeardownHistoricSnapshot(false);
}

#[test]
fn historic_own_transaction_cmax_resolution() {
    let _g = test_lock();
    historic_boot();
    let cx = MemoryContext::new("test");
    let snap = historic_snapshot(cx.mcx(), 50, 100, &[60], &[80], 10);

    let mut t = TestTuple::new(60, 80, HEAP_XMIN_COMMITTED);
    assert!(HeapTupleSatisfiesHistoricMVCC(&mut t.htup(), &snap, BUF).unwrap());

    install_tuplecids(5, InvalidCommandId);
    let mut t = TestTuple::new(60, 80, HEAP_XMIN_COMMITTED);
    assert!(HeapTupleSatisfiesHistoricMVCC(&mut t.htup(), &snap, BUF).unwrap());
    ::snapmgr::TeardownHistoricSnapshot(false);

    install_tuplecids(5, 10);
    let mut t = TestTuple::new(60, 80, HEAP_XMIN_COMMITTED);
    assert!(HeapTupleSatisfiesHistoricMVCC(&mut t.htup(), &snap, BUF).unwrap());
    ::snapmgr::TeardownHistoricSnapshot(false);

    install_tuplecids(5, 9);
    let mut t = TestTuple::new(60, 80, HEAP_XMIN_COMMITTED);
    assert!(!HeapTupleSatisfiesHistoricMVCC(&mut t.htup(), &snap, BUF).unwrap());
    ::snapmgr::TeardownHistoricSnapshot(false);
}

#[test]
fn historic_dispatch_through_satisfies_visibility() {
    let _g = test_lock();
    historic_boot();
    let cx = MemoryContext::new("test");
    let mut snap = historic_snapshot(cx.mcx(), 50, 100, &[60], &[], 10);
    let mut t = TestTuple::new(60, 0, HEAP_XMAX_INVALID);
    assert!(HeapTupleSatisfiesVisibility(&mut t.htup(), &mut snap, BUF).unwrap());
}
