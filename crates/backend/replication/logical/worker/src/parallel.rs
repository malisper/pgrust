// applyparallelworker.c: parallel apply of streamed transactions. The leader
// assigns a streaming transaction to a parallel apply (PA) worker at its first
// STREAM START and forwards every chunk over a message queue; the PA worker
// applies the changes as they arrive and the leader waits at the transaction
// finish command so commit order is preserved. When the queue backs up
// (pa_send_data timeout) the leader switches the transaction to
// PARTIAL_SERIALIZE: the rest is spooled to the shared fileset and the PA
// worker replays it once the leader marks the fileset SERIALIZE_DONE. The
// LA<->PA deadlock-detection protocol (stream lock + transaction lock, both
// lmgr session locks on LOCKTAG_APPLY_TRANSACTION) is kept exactly.
//
// Thread-model renderings (per lane convention):
// - The DSM segment is an Arc'd ParallelApplyWorkerShared + Arc'd shm_mq ring
//   handed through a process-global handle registry; the handle rides
//   bgw_extra where C puts the dsm_handle.
// - The 16kB error shm_mq + pqmq redirection collapse into a one-slot error
//   mailbox in the shared state (the parallel-query WorkerMessage precedent):
//   the dying PA worker parks its PgError there and pokes the leader with
//   PROCSIG_PARALLEL_APPLY_MESSAGE; ProcessParallelApplyMessages rethrows
//   C's "exited due to error" ERROR.
// - The queue carries the stripped logical-rep message (action byte first,
//   the shape apply_dispatch takes), not C's full 'w' envelope; the PA loop
//   therefore has no 'w'/stats skipping to do.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;

use elog::ereport;
use fd::FileSet;
use init_small::globals as g;
use mcx::Mcx;
use pgsync::Mutex;
use shm_mq::{shm_mq_attach, shm_mq_create, ShmMq, ShmMqHandle, ShmMqRecv, ShmMqResult};
use types_core::{
    InvalidTransactionId, InvalidXLogRecPtr, Oid, TimestampTz, TransactionId, XLogRecPtr,
};
use types_error::{
    PgError, PgResult, DEBUG1, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERROR, LOG,
};
use types_rel::{AccessExclusiveLock, AccessShareLock};
use types_storage::storage::ProcSignalReason;
use types_storage::waiteventset::{WL_EXIT_ON_PM_DEATH, WL_LATCH_SET, WL_TIMEOUT};

use crate::{loc, my_sub, ApplyErrorContextFrame};

// DSM_QUEUE_SIZE (applyparallelworker.c:187).
const DSM_QUEUE_SIZE: usize = 16 * 1024 * 1024;

// PARALLEL_APPLY_LOCK_STREAM / PARALLEL_APPLY_LOCK_XACT.
const PARALLEL_APPLY_LOCK_STREAM: u16 = 0;
const PARALLEL_APPLY_LOCK_XACT: u16 = 1;

const PG_WAIT_ACTIVITY: u32 = 0x0500_0000;
const PG_WAIT_IPC: u32 = 0x0800_0000;
const WAIT_EVENT_LOGICAL_PARALLEL_APPLY_MAIN: u32 = PG_WAIT_ACTIVITY | 9;
const WAIT_EVENT_LOGICAL_APPLY_SEND_DATA: u32 = PG_WAIT_IPC | 29;
const WAIT_EVENT_LOGICAL_PARALLEL_APPLY_STATE_CHANGE: u32 = PG_WAIT_IPC | 30;

// ParallelTransState (worker_internal.h).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Debug)]
pub(crate) enum ParallelTransState {
    Unknown = 0,
    Started = 1,
    Finished = 2,
}

// PartialFileSetState (worker_internal.h).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum PartialFileSetState {
    Empty,
    SerializeInProgress,
    SerializeDone,
    Ready,
}

struct PaSharedInner {
    xid: TransactionId,
    xact_state: ParallelTransState,
    fileset_state: PartialFileSetState,
    // C copies the FileSet by value into the DSM at SERIALIZE_DONE; an Arc
    // clone of the leader's stream fileset here.
    fileset: Option<Arc<FileSet>>,
    logicalrep_worker_generation: u16,
    logicalrep_worker_slot_no: usize,
    // Thread-model error queue: the PA worker's fatal PgError, and whether
    // the worker has left (C: shm_mq_receive on the error queue returns
    // SHM_MQ_DETACHED).
    error: Option<Box<PgError>>,
    error_mq_detached: bool,
}

// ParallelApplyWorkerShared (worker_internal.h).
pub(crate) struct ParallelApplyWorkerShared {
    inner: Mutex<PaSharedInner>,
    pending_stream_count: AtomicU32,
    // XactLastCommitEnd of the PA worker's commit (read by the leader for
    // store_flush_position).
    last_commit_end: AtomicU64,
}

impl ParallelApplyWorkerShared {
    fn lock(&self) -> impl std::ops::DerefMut<Target = PaSharedInner> + '_ {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub(crate) fn lock_xid(&self) -> TransactionId {
        self.lock().xid
    }
}

// ParallelApplyWorkerInfo (worker_internal.h), leader side.
pub(crate) struct ParallelApplyWorkerInfo {
    mq_handle: ShmMqHandle,
    // C error_mq_handle != NULL: whether the leader still reads this
    // worker's error mailbox.
    error_mq_attached: bool,
    dsm_handle: u64,
    pub(crate) shared: Arc<ParallelApplyWorkerShared>,
    in_use: bool,
    pub(crate) serialize_changes: bool,
}

pub(crate) type Winfo = Rc<RefCell<ParallelApplyWorkerInfo>>;

// The "DSM segment" registry: handle -> (shared, mq ring).
struct PaDsm {
    shared: Arc<ParallelApplyWorkerShared>,
    mq: Arc<ShmMq>,
}

pgsync::process_global! {
    static PA_DSM_REGISTRY: Mutex<Vec<(u64, PaDsm)>> = Mutex::new(Vec::new());
}
static NEXT_PA_DSM_HANDLE: AtomicU64 = AtomicU64::new(1);

thread_local! {
    // ParallelApplyWorkerPool + ParallelApplyTxnHash + stream_apply_worker
    // (leader apply worker file-statics).
    static WORKER_POOL: RefCell<Vec<Winfo>> = const { RefCell::new(Vec::new()) };
    static TXN_HASH: RefCell<HashMap<TransactionId, Winfo>> = RefCell::new(HashMap::new());
    static STREAM_APPLY_WORKER: RefCell<Option<Winfo>> = const { RefCell::new(None) };
    // MyParallelShared (PA worker side).
    static MY_PARALLEL_SHARED: RefCell<Option<Arc<ParallelApplyWorkerShared>>> =
        const { RefCell::new(None) };
    // subxactlist (PA worker side).
    static SUBXACTLIST: RefCell<Vec<TransactionId>> = const { RefCell::new(Vec::new()) };
    // parallel_stream_nchanges (worker.c).
    pub(crate) static PARALLEL_STREAM_NCHANGES: Cell<u32> = const { Cell::new(0) };
}

pub(crate) fn am_parallel_apply_worker() -> bool {
    MY_PARALLEL_SHARED.with(|s| s.borrow().is_some())
}

pub(crate) fn my_shared() -> Arc<ParallelApplyWorkerShared> {
    MY_PARALLEL_SHARED
        .with(|s| s.borrow().clone())
        .expect("MyParallelShared set in parallel apply worker")
}
use my_shared as my_parallel_shared;

fn subid() -> Oid {
    my_sub(|s| s.oid)
}

// ---- session locks (applyparallelworker.c:1546-1591) -----------------------

pub(crate) fn pa_lock_stream(xid: TransactionId, lockmode: i32) -> PgResult<()> {
    lmgr::LockApplyTransactionForSession(subid(), xid, PARALLEL_APPLY_LOCK_STREAM, lockmode)
}

pub(crate) fn pa_unlock_stream(xid: TransactionId, lockmode: i32) -> PgResult<()> {
    lmgr::UnlockApplyTransactionForSession(subid(), xid, PARALLEL_APPLY_LOCK_STREAM, lockmode)
}

pub(crate) fn pa_lock_transaction(xid: TransactionId, lockmode: i32) -> PgResult<()> {
    lmgr::LockApplyTransactionForSession(subid(), xid, PARALLEL_APPLY_LOCK_XACT, lockmode)
}

pub(crate) fn pa_unlock_transaction(xid: TransactionId, lockmode: i32) -> PgResult<()> {
    lmgr::UnlockApplyTransactionForSession(subid(), xid, PARALLEL_APPLY_LOCK_XACT, lockmode)
}

// ---- shared-state accessors ------------------------------------------------

pub(crate) fn pa_set_xact_state(
    shared: &ParallelApplyWorkerShared,
    xact_state: ParallelTransState,
) {
    shared.lock().xact_state = xact_state;
}

fn pa_get_xact_state(shared: &ParallelApplyWorkerShared) -> ParallelTransState {
    shared.lock().xact_state
}

pub(crate) fn pa_set_fileset_state(
    shared: &ParallelApplyWorkerShared,
    fileset_state: PartialFileSetState,
) {
    let mut inner = shared.lock();
    inner.fileset_state = fileset_state;
    if fileset_state == PartialFileSetState::SerializeDone {
        debug_assert!(!am_parallel_apply_worker());
        inner.fileset = crate::stream_apply::stream_fileset_arc().ok();
    }
}

fn pa_get_fileset_state() -> PartialFileSetState {
    debug_assert!(am_parallel_apply_worker());
    my_parallel_shared().lock().fileset_state
}

pub(crate) fn pa_set_last_commit_end(lsn: XLogRecPtr) {
    my_parallel_shared().last_commit_end.store(lsn, Ordering::Relaxed);
}

pub(crate) fn my_parallel_shared_xid() -> TransactionId {
    my_parallel_shared().lock().xid
}

// ---- leader: worker pool ---------------------------------------------------

// pa_can_start (applyparallelworker.c:264).
fn pa_can_start(mcx: Mcx<'static>) -> PgResult<bool> {
    // Only leader apply workers start parallel apply workers.
    if am_parallel_apply_worker()
        || crate::tablesync::AM_TABLESYNC_WORKER.with(Cell::get)
    {
        return Ok(false);
    }

    // Refresh the subscription so a constant stream of parallel transactions
    // still notices parameter changes.
    crate::maybe_reread_subscription(mcx)?;
    if crate::apply_worker_exit_requested() {
        return Ok(false);
    }

    let slot = launcher::my_worker_slot().expect("attached");
    let w = launcher::worker_snapshot(slot).expect("worker slot");
    if !w.parallel_apply {
        return Ok(false);
    }

    // A set skiplsn forces the serialize path (we must know the last LSN of
    // the transaction before applying to judge whether to skip).
    if my_sub(|s| s.skiplsn) != InvalidXLogRecPtr {
        return Ok(false);
    }

    // Non-READY tables cannot be decided without remote_final_lsn.
    if !crate::tablesync::all_tablesyncs_ready(mcx)? {
        return Ok(false);
    }

    Ok(true)
}

// pa_setup_dsm (applyparallelworker.c:326).
fn pa_setup_dsm() -> (u64, Winfo) {
    let shared = Arc::new(ParallelApplyWorkerShared {
        inner: Mutex::new(PaSharedInner {
            xid: InvalidTransactionId,
            xact_state: ParallelTransState::Unknown,
            fileset_state: PartialFileSetState::Empty,
            fileset: None,
            logicalrep_worker_generation: 0,
            logicalrep_worker_slot_no: 0,
            error: None,
            error_mq_detached: false,
        }),
        pending_stream_count: AtomicU32::new(0),
        last_commit_end: AtomicU64::new(InvalidXLogRecPtr),
    });
    let mq = shm_mq_create(DSM_QUEUE_SIZE);
    mq.set_sender(g::MyProcNumber());
    let mq_handle = shm_mq_attach(Arc::clone(&mq));

    let handle = NEXT_PA_DSM_HANDLE.fetch_add(1, Ordering::Relaxed);
    PA_DSM_REGISTRY
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .push((handle, PaDsm { shared: Arc::clone(&shared), mq }));

    let winfo = Rc::new(RefCell::new(ParallelApplyWorkerInfo {
        mq_handle,
        error_mq_attached: true,
        dsm_handle: handle,
        shared,
        in_use: false,
        serialize_changes: false,
    }));
    (handle, winfo)
}

// pa_launch_parallel_worker (applyparallelworker.c:403).
fn pa_launch_parallel_worker() -> PgResult<Option<Winfo>> {
    // Reuse an available pool worker.
    let reusable = WORKER_POOL.with(|p| {
        p.borrow().iter().find(|w| !w.borrow().in_use).cloned()
    });
    if let Some(w) = reusable {
        return Ok(Some(w));
    }

    let (handle, winfo) = pa_setup_dsm();
    let slot = launcher::my_worker_slot().expect("attached");
    let w = launcher::worker_snapshot(slot).expect("worker slot");
    let launched = launcher::logicalrep_worker_launch(
        launcher::LogicalRepWorkerType::ParallelApply,
        w.dbid,
        my_sub(|s| s.oid),
        &my_sub(|s| s.name.clone()),
        w.userid,
        types_core::InvalidOid,
        handle,
    )?;

    if launched {
        WORKER_POOL.with(|p| p.borrow_mut().push(Rc::clone(&winfo)));
        Ok(Some(winfo))
    } else {
        pa_free_worker_info(&winfo)?;
        Ok(None)
    }
}

// pa_allocate_worker (applyparallelworker.c:469).
pub(crate) fn pa_allocate_worker(mcx: Mcx<'static>, xid: TransactionId) -> PgResult<()> {
    if !pa_can_start(mcx)? {
        return Ok(());
    }
    let Some(winfo) = pa_launch_parallel_worker()? else {
        return Ok(());
    };

    {
        let mut w = winfo.borrow_mut();
        let mut inner = w.shared.lock();
        inner.xact_state = ParallelTransState::Unknown;
        inner.xid = xid;
        drop(inner);
        w.in_use = true;
        w.serialize_changes = false;
    }
    let dup = TXN_HASH.with(|h| h.borrow_mut().insert(xid, winfo).is_some());
    if dup {
        return elog::elog(ERROR, "hash table corrupted".to_string());
    }
    Ok(())
}

// pa_find_worker (applyparallelworker.c:517).
pub(crate) fn pa_find_worker(xid: TransactionId) -> Option<Winfo> {
    if xid == InvalidTransactionId {
        return None;
    }
    if let Some(w) = STREAM_APPLY_WORKER.with(|s| s.borrow().clone()) {
        return Some(w);
    }
    TXN_HASH.with(|h| {
        let w = h.borrow().get(&xid).cloned();
        if let Some(w) = &w {
            debug_assert!(w.borrow().in_use);
        }
        w
    })
}

// pa_free_worker (applyparallelworker.c:555).
fn pa_free_worker(winfo: &Winfo) -> PgResult<()> {
    debug_assert!(!am_parallel_apply_worker());
    debug_assert!(winfo.borrow().in_use);
    debug_assert!(
        pa_get_xact_state(&winfo.borrow().shared) == ParallelTransState::Finished
    );

    let xid = winfo.borrow().shared.lock().xid;
    if TXN_HASH.with(|h| h.borrow_mut().remove(&xid)).is_none() {
        return elog::elog(ERROR, "hash table corrupted".to_string());
    }

    // Stop the worker on partial serialization (the queue may hold a partly
    // written message) or when the pool is over half the per-subscription max.
    let pool_len = WORKER_POOL.with(|p| p.borrow().len()) as i32;
    let max = guc_tables::vars::max_parallel_apply_workers_per_subscription.read();
    if winfo.borrow().serialize_changes || pool_len > max / 2 {
        logicalrep_pa_worker_stop(winfo)?;
        pa_free_worker_info(winfo)?;
        return Ok(());
    }

    let mut w = winfo.borrow_mut();
    w.in_use = false;
    w.serialize_changes = false;
    Ok(())
}

// logicalrep_pa_worker_stop (launcher.c:643) leader half: detach the error
// mailbox first so the leader doesn't report the termination it caused.
fn logicalrep_pa_worker_stop(winfo: &Winfo) -> PgResult<()> {
    let (slot, generation) = {
        let mut w = winfo.borrow_mut();
        w.error_mq_attached = false;
        let inner = w.shared.lock();
        (inner.logicalrep_worker_slot_no, inner.logicalrep_worker_generation)
    };
    launcher::logicalrep_pa_worker_stop(slot, generation)
}

// pa_free_worker_info (applyparallelworker.c:594).
fn pa_free_worker_info(winfo: &Winfo) -> PgResult<()> {
    let (dsm_handle, serialize_changes, xid) = {
        let mut w = winfo.borrow_mut();
        w.mq_handle.detach();
        w.error_mq_attached = false;
        let xid = w.shared.lock_xid();
        (w.dsm_handle, w.serialize_changes, xid)
    };

    // Unlink the files with serialized changes.
    if serialize_changes {
        crate::stream_apply::stream_cleanup_files(subid(), xid)?;
    }

    PA_DSM_REGISTRY
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .retain(|(h, _)| *h != dsm_handle);

    WORKER_POOL.with(|p| {
        p.borrow_mut().retain(|w| !Rc::ptr_eq(w, winfo));
    });
    Ok(())
}

// The leader's dsm_backend_shutdown half: every pool worker's "segment" is
// released when the leader exits (the workers hold their own Arcs until they
// exit), so a retained pool never outlives its leader.
pub(crate) fn pa_release_pool_dsm() {
    let handles: Vec<u64> = WORKER_POOL.with(|p| {
        let pool = std::mem::take(&mut *p.borrow_mut());
        pool.iter().map(|w| w.borrow().dsm_handle).collect()
    });
    TXN_HASH.with(|h| h.borrow_mut().clear());
    STREAM_APPLY_WORKER.with(|s| *s.borrow_mut() = None);
    if handles.is_empty() {
        return;
    }
    PA_DSM_REGISTRY
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .retain(|(h, _)| !handles.contains(h));
}

#[cfg(test)]
pub(crate) fn test_pool_worker() -> Winfo {
    let (_, winfo) = pa_setup_dsm();
    WORKER_POOL.with(|p| p.borrow_mut().push(Rc::clone(&winfo)));
    winfo
}

#[cfg(test)]
pub(crate) fn test_worker_exited(winfo: &Winfo) {
    winfo.borrow().shared.lock().error_mq_detached = true;
}

#[cfg(test)]
pub(crate) fn test_registry_len() -> usize {
    PA_DSM_REGISTRY.lock().unwrap_or_else(|e| e.into_inner()).len()
}

// pa_detach_all_error_mq (applyparallelworker.c:621).
pub(crate) fn pa_detach_all_error_mq() {
    WORKER_POOL.with(|p| {
        for w in p.borrow().iter() {
            w.borrow_mut().error_mq_attached = false;
        }
    });
}

// pa_set_stream_apply_worker (applyparallelworker.c:1340).
pub(crate) fn pa_set_stream_apply_worker(winfo: Option<Winfo>) {
    STREAM_APPLY_WORKER.with(|s| *s.borrow_mut() = winfo);
}

// ---- leader: sending -------------------------------------------------------

// pa_send_data (applyparallelworker.c:1152). Returns false on timeout (the
// caller switches to partial serialization).
pub(crate) fn pa_send_data(winfo: &Winfo, data: &[u8]) -> PgResult<bool> {
    debug_assert!(!xact::IsTransactionState());
    debug_assert!(!winfo.borrow().serialize_changes);

    // debug_logical_replication_streaming = immediate: never send, so every
    // parallel transaction exercises the partial-serialize path (testing).
    if guc_tables::vars::debug_logical_replication_streaming.read()
        == guc_tables::consts::DEBUG_LOGICAL_REP_STREAMING_IMMEDIATE
    {
        return Ok(false);
    }

    const SHM_SEND_RETRY_INTERVAL_MS: i64 = 1000;
    const SHM_SEND_TIMEOUT_MS: i64 = 10000 - SHM_SEND_RETRY_INTERVAL_MS;

    let mut start_time: TimestampTz = 0;
    loop {
        let result = winfo.borrow_mut().mq_handle.send(data, true, true)?;
        match result {
            ShmMqResult::Success => return Ok(true),
            ShmMqResult::Detached => {
                ereport(ERROR)
                    .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                    .errmsg("could not send data to shared-memory queue")
                    .finish(loc("pa_send_data"))?;
                unreachable!();
            }
            ShmMqResult::WouldBlock => {}
        }

        let rc = latch::WaitLatch(
            g::MyLatch(),
            WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
            SHM_SEND_RETRY_INTERVAL_MS,
            WAIT_EVENT_LOGICAL_APPLY_SEND_DATA,
        )?;
        if rc & WL_LATCH_SET != 0 {
            if let Some(l) = g::MyLatch() {
                latch::ResetLatch(l);
            }
            postgres_seams::check_for_interrupts::call()?;
        }

        if start_time == 0 {
            start_time = crate::get_ts();
        } else if adt_timestamp::TimestampDifferenceExceeds(
            start_time,
            crate::get_ts(),
            SHM_SEND_TIMEOUT_MS as i32,
        ) {
            return Ok(false);
        }
    }
}

// pa_switch_to_partial_serialize (applyparallelworker.c:1217).
pub(crate) fn pa_switch_to_partial_serialize(
    mcx: Mcx<'static>,
    winfo: &Winfo,
    stream_locked: bool,
) -> PgResult<()> {
    let xid = winfo.borrow().shared.lock().xid;
    let _ = elog::elog(
        LOG,
        format!(
            "logical replication apply worker will serialize the remaining changes of remote transaction {xid} to a file"
        ),
    );

    winfo.borrow_mut().serialize_changes = true;
    crate::stream_apply::stream_start_internal(mcx, xid, true)?;

    if !stream_locked {
        pa_lock_stream(xid, AccessExclusiveLock)?;
    }

    pa_set_fileset_state(&winfo.borrow().shared, PartialFileSetState::SerializeInProgress);
    Ok(())
}

// ---- leader: waiting for the PA worker -------------------------------------

// pa_wait_for_xact_state (applyparallelworker.c:1250).
fn pa_wait_for_xact_state(
    winfo: &Winfo,
    xact_state: ParallelTransState,
) -> PgResult<()> {
    loop {
        if pa_get_xact_state(&winfo.borrow().shared) >= xact_state {
            return Ok(());
        }
        let rc = latch::WaitLatch(
            g::MyLatch(),
            WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
            10,
            WAIT_EVENT_LOGICAL_PARALLEL_APPLY_STATE_CHANGE,
        )?;
        if rc & WL_LATCH_SET != 0 {
            if let Some(l) = g::MyLatch() {
                latch::ResetLatch(l);
            }
        }
        postgres_seams::check_for_interrupts::call()?;
    }
}

// pa_wait_for_xact_finish (applyparallelworker.c:1280).
fn pa_wait_for_xact_finish(winfo: &Winfo) -> PgResult<()> {
    pa_wait_for_xact_state(winfo, ParallelTransState::Started)?;

    let xid = winfo.borrow().shared.lock().xid;
    pa_lock_transaction(xid, AccessShareLock)?;
    pa_unlock_transaction(xid, AccessShareLock)?;

    if pa_get_xact_state(&winfo.borrow().shared) != ParallelTransState::Finished {
        ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg("lost connection to the logical replication parallel apply worker")
            .finish(loc("pa_wait_for_xact_finish"))?;
    }
    Ok(())
}

// pa_xact_finish (applyparallelworker.c:1624).
pub(crate) fn pa_xact_finish(winfo: &Winfo, remote_lsn: XLogRecPtr) -> PgResult<()> {
    debug_assert!(!am_parallel_apply_worker());
    let xid = winfo.borrow().shared.lock().xid;
    pa_unlock_stream(xid, AccessExclusiveLock)?;

    // Wait for the worker: maintains commit order.
    pa_wait_for_xact_finish(winfo)?;

    if remote_lsn != InvalidXLogRecPtr {
        let local_end = winfo.borrow().shared.last_commit_end.load(Ordering::Relaxed);
        crate::store_flush_position(remote_lsn, local_end);
    }

    pa_free_worker(winfo)
}

// pending_stream_count increment (apply_handle_stream_start / stream_abort
// leader arms).
pub(crate) fn pa_incr_pending_stream_count(winfo: &Winfo) {
    winfo
        .borrow()
        .shared
        .pending_stream_count
        .fetch_add(1, Ordering::SeqCst);
}

// ---- leader: error mailbox -------------------------------------------------

// HandleParallelApplyMessageInterrupt (applyparallelworker.c:995); runs on
// the leader thread from the procsignal arm.
pub fn HandleParallelApplyMessageInterrupt() {
    g::SetInterruptPending(true);
    logical_worker_seams::parallel_apply_message::set(true);
    if let Some(l) = g::MyLatch() {
        latch::SetLatch(l);
    }
}

// ProcessParallelApplyMessages (applyparallelworker.c:1069).
pub fn ProcessParallelApplyMessages() -> PgResult<()> {
    logical_worker_seams::parallel_apply_message::set(false);

    let workers = WORKER_POOL.with(|p| p.borrow().clone());
    for winfo in workers {
        // The leader detaches before stopping a worker; skip those.
        if !winfo.borrow().error_mq_attached {
            continue;
        }
        let (err, detached) = {
            let shared = winfo.borrow().shared.clone();
            let mut inner = shared.lock();
            (inner.error.take(), inner.error_mq_detached)
        };
        if let Some(e) = err {
            // C parses the worker's ErrorResponse and rethrows with an added
            // context line (applyparallelworker.c:1039); the original error
            // was already logged by the worker's own exit path.
            ereport(ERROR)
                .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg("logical replication parallel apply worker exited due to error")
                .errcontext_msg(parallel_apply_worker_context(&e))
                .finish(loc("ProcessParallelApplyMessages"))?;
        }
        if detached {
            ereport(ERROR)
                .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg("lost connection to the logical replication parallel apply worker")
                .finish(loc("ProcessParallelApplyMessages"))?;
        }
    }
    Ok(())
}

// applyparallelworker.c:1045: the worker error's own CONTEXT (if any) plus a
// line showing the message was propagated from a parallel apply worker; the
// primary message is never part of the context.
pub(crate) fn parallel_apply_worker_context(e: &PgError) -> String {
    match e.context() {
        Some(c) => format!("{c}\nlogical replication parallel apply worker"),
        None => "logical replication parallel apply worker".to_string(),
    }
}

// ---- PA worker: subtransactions --------------------------------------------

// pa_savepoint_name (applyparallelworker.c:1354).
fn pa_savepoint_name(suboid: Oid, xid: TransactionId) -> String {
    format!("pg_sp_{suboid}_{xid}")
}

// pa_start_subtrans (applyparallelworker.c:1368).
pub(crate) fn pa_start_subtrans(
    current_xid: TransactionId,
    top_xid: TransactionId,
) -> PgResult<()> {
    if current_xid == top_xid
        || SUBXACTLIST.with(|l| l.borrow().contains(&current_xid))
    {
        return Ok(());
    }

    let spname = pa_savepoint_name(subid(), current_xid);
    let _ = elog::elog(
        DEBUG1,
        format!("defining savepoint {spname} in logical replication parallel apply worker"),
    );

    // We must be in a transaction block to define the SAVEPOINT.
    if !xact::IsTransactionBlock() {
        if !xact::IsTransactionState() {
            xact::StartTransactionCommand()?;
        }
        xact::BeginTransactionBlock()?;
        xact::CommitTransactionCommand()?;
    }

    xact::DefineSavepoint(Some(&spname))?;
    // Start the subtransaction (StartSubTransaction happens at
    // CommitTransactionCommand after DefineSavepoint).
    xact::CommitTransactionCommand()?;

    SUBXACTLIST.with(|l| l.borrow_mut().push(current_xid));
    Ok(())
}

// pa_reset_subtrans (applyparallelworker.c:1408).
pub(crate) fn pa_reset_subtrans() {
    SUBXACTLIST.with(|l| l.borrow_mut().clear());
}

// pa_stream_abort (applyparallelworker.c:1422).
pub(crate) fn pa_stream_abort(abort: &logicalproto::LogicalRepStreamAbortData) -> PgResult<()> {
    let xid = abort.xid;
    let subxid = abort.subxid;

    // Update origin state so we can restart streaming from the correct
    // position in case of crash.
    origin::set_replorigin_session_origin_lsn(abort.abort_lsn);
    origin::set_replorigin_session_origin_timestamp(abort.abort_time);

    if subxid == xid {
        // Toplevel abort.
        pa_set_xact_state(&my_parallel_shared(), ParallelTransState::Finished);

        // Release the lock first: an empty streaming transaction has no
        // rollback to release it, and an aborted transaction stays aborted
        // even if this worker dies right here.
        pa_unlock_transaction(xid, AccessExclusiveLock)?;

        xact::AbortCurrentTransaction()?;
        if xact::IsTransactionBlock() {
            let _ = xact::EndTransactionBlock(false)?;
            xact::CommitTransactionCommand()?;
        }
        pa_reset_subtrans();

        crate::apply::report_activity(crate::apply::BackendState::STATE_IDLE);
    } else {
        // Rollback to the subxact's savepoint.
        let spname = pa_savepoint_name(subid(), subxid);
        let _ = elog::elog(
            DEBUG1,
            format!(
                "rolling back to savepoint {spname} in logical replication parallel apply worker"
            ),
        );
        let pos = SUBXACTLIST.with(|l| l.borrow().iter().rposition(|&x| x == subxid));
        // An empty sub-transaction won't be found here.
        if let Some(i) = pos {
            xact::RollbackToSavepoint(&spname)?;
            xact::CommitTransactionCommand()?;
            SUBXACTLIST.with(|l| l.borrow_mut().truncate(i));
        }
    }
    Ok(())
}

// pa_decr_and_wait_stream_block (applyparallelworker.c:1597).
pub(crate) fn pa_decr_and_wait_stream_block() -> PgResult<()> {
    debug_assert!(am_parallel_apply_worker());
    let shared = my_parallel_shared();

    if shared.pending_stream_count.load(Ordering::SeqCst) == 0 {
        // Only possible while applying spooled messages.
        if pa_has_spooled_message_pending() {
            return Ok(());
        }
        return elog::elog(ERROR, "invalid pending streaming chunk 0".to_string());
    }

    if shared.pending_stream_count.fetch_sub(1, Ordering::SeqCst) == 1 {
        let xid = shared.lock().xid;
        pa_lock_stream(xid, AccessShareLock)?;
        pa_unlock_stream(xid, AccessShareLock)?;
    }
    Ok(())
}

// ---- PA worker: spooled-message fallback -----------------------------------

fn pa_has_spooled_message_pending() -> bool {
    pa_get_fileset_state() != PartialFileSetState::Empty
}

// pa_process_spooled_messages_if_required (applyparallelworker.c:657).
fn pa_process_spooled_messages_if_required(mcx: Mcx<'static>) -> PgResult<bool> {
    let mut fileset_state = pa_get_fileset_state();
    if fileset_state == PartialFileSetState::Empty {
        return Ok(false);
    }

    // While the leader is still serializing, wait on the stream lock so the
    // deadlock detector can see this edge.
    if fileset_state == PartialFileSetState::SerializeInProgress {
        let xid = my_parallel_shared().lock().xid;
        pa_lock_stream(xid, AccessShareLock)?;
        pa_unlock_stream(xid, AccessShareLock)?;
        fileset_state = pa_get_fileset_state();
    }

    // One extra pass between SERIALIZE_DONE and READY drains the memory
    // queue before the file is replayed.
    if fileset_state == PartialFileSetState::SerializeDone {
        pa_set_fileset_state(&my_parallel_shared(), PartialFileSetState::Ready);
    } else if fileset_state == PartialFileSetState::Ready {
        let shared = my_parallel_shared();
        let (xid, fileset) = {
            let inner = shared.lock();
            (inner.xid, inner.fileset.clone().expect("fileset set at SERIALIZE_DONE"))
        };
        crate::stream_apply::apply_spooled_messages(
            mcx,
            None,
            &fileset,
            xid,
            InvalidXLogRecPtr,
        )?;
        pa_set_fileset_state(&my_parallel_shared(), PartialFileSetState::Empty);
    }
    Ok(true)
}

// ---- PA worker: main -------------------------------------------------------

// ProcessParallelApplyInterrupts (applyparallelworker.c:711). Ok(false) =
// shutdown requested (clean exit).
fn process_parallel_apply_interrupts() -> PgResult<bool> {
    postgres_seams::check_for_interrupts::call()?;

    if interrupt::ShutdownRequestPending() {
        let name = my_sub(|s| s.name.clone());
        let _ = elog::elog(
            LOG,
            format!(
                "logical replication parallel apply worker for subscription \"{name}\" has finished"
            ),
        );
        return Ok(false);
    }

    if interrupt::ConfigReloadPending() {
        interrupt::SetConfigReloadPending(false);
        guc_file::ProcessConfigFile(types_guc::GucContext::PGC_SIGHUP)?;
    }
    Ok(true)
}

// LogicalParallelApplyLoop (applyparallelworker.c:733). The apply error
// context callback is pushed for the loop's duration
// (applyparallelworker.c:749-754) and popped on every exit (:839).
fn logical_parallel_apply_loop(mqh: &mut ShmMqHandle) -> PgResult<()> {
    let frame = ApplyErrorContextFrame::push();
    frame.attach(logical_parallel_apply_loop_guts(mqh))
}

fn logical_parallel_apply_loop_guts(mqh: &mut ShmMqHandle) -> PgResult<()> {
    // The ApplyMessageContext we clean up after each replication protocol
    // message (applyparallelworker.c:741-746); a bump arena released
    // wholesale at every reset, as the leader's (see apply_loop_guts).
    let mut top = mcx::MemoryContext::new_bump("ApplyMessageContext");

    // Reused across iterations; the ring borrow ends before dispatch.
    let mut buf: Vec<u8> = Vec::new();
    loop {
        if !process_parallel_apply_interrupts()? {
            return Ok(());
        }
        // Subscription removed (maybe_reread_subscription): C proc_exit(0)s.
        if crate::apply_worker_exit_requested() {
            return Ok(());
        }

        // MemoryContextReset(ApplyMessageContext) at the end of every
        // iteration (applyparallelworker.c:834), placed at the head here so
        // the handle below is always derived from a reset context.
        top.reset();
        // SAFETY: `top` outlives the iteration; the handle is re-derived
        // after every reset and never stored past it.
        let mcx: Mcx<'static> = unsafe { std::mem::transmute(top.mcx()) };

        let received = match mqh.receive(true)? {
            ShmMqRecv::Success(data) => {
                if data.is_empty() {
                    return elog::elog(ERROR, "invalid message length".to_string());
                }
                buf.clear();
                buf.extend_from_slice(data);
                true
            }
            ShmMqRecv::WouldBlock => false,
            ShmMqRecv::Detached => {
                ereport(ERROR)
                    .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                    .errmsg("lost connection to the logical replication apply worker")
                    .finish(loc("LogicalParallelApplyLoop"))?;
                unreachable!();
            }
        };

        if received {
            crate::apply::apply_dispatch(mcx, None, &buf)?;
        } else if !pa_process_spooled_messages_if_required(mcx)? {
            let rc = latch::WaitLatch(
                g::MyLatch(),
                WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                1000,
                WAIT_EVENT_LOGICAL_PARALLEL_APPLY_MAIN,
            )?;
            if rc & WL_LATCH_SET != 0 {
                if let Some(l) = g::MyLatch() {
                    latch::ResetLatch(l);
                }
            }

            // upstream 44c8dc280178 (18.4): Flush statistics during idle periods in parallel apply worker.
            // The idle gap before the leader assigns the next transaction is
            // the only chance to report the stats of the one just applied.
            if rc & WL_TIMEOUT != 0 && !xact::IsTransactionState() {
                pgstat::pending::pgstat_report_stat(true);
            }
        }
    }
}

// pa_shutdown (applyparallelworker.c:843): poke the leader so it reads our
// error mailbox even when we exit without a clean ErrorResponse.
fn pa_shutdown(leader_pid: i32) {
    let _ = procsignal::SendProcSignal(
        leader_pid,
        ProcSignalReason::PROCSIG_PARALLEL_APPLY_MESSAGE,
        types_core::INVALID_PROC_NUMBER,
    );
}

// ParallelApplyWorkerMain (applyparallelworker.c:856).
pub fn ParallelApplyWorkerMain(main_arg: u64) -> PgResult<()> {
    let worker_slot = main_arg as usize;
    // InitializingApplyWorker = true (applyparallelworker.c:879).
    launcher::set_initializing_apply_worker(true);

    // Signals: SIGHUP config reload; SIGTERM die; SIGUSR2 = graceful shutdown
    // requested by the leader (differentiates it from an abort-and-exit).
    procsignal::pqsignal_thread(
        procsignal::signums::SIGHUP,
        procsignal::ThreadSignalHandler::Simple(interrupt::SignalHandlerForConfigReload),
    );
    procsignal::pqsignal_thread(
        procsignal::signums::SIGTERM,
        procsignal::ThreadSignalHandler::Fallible(crate::logicalrep_worker_die),
    );
    procsignal::pqsignal_thread(
        procsignal::signums::SIGUSR2,
        procsignal::ThreadSignalHandler::Simple(interrupt::SignalHandlerForShutdownRequest),
    );

    // "Attach to the DSM segment": resolve the handle from bgw_extra.
    let entry = bgworker::MyBgworkerEntry().expect("MyBgworkerEntry is not set");
    let handle = u64::from_ne_bytes(entry.bgw_extra[..8].try_into().expect("8 bytes"));
    let dsm = {
        let reg = PA_DSM_REGISTRY.lock().unwrap_or_else(|e| e.into_inner());
        reg.iter()
            .find(|(h, _)| *h == handle)
            .map(|(_, d)| (Arc::clone(&d.shared), Arc::clone(&d.mq)))
    };
    let Some((shared, mq)) = dsm else {
        ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg("could not map dynamic shared memory segment")
            .finish(loc("ParallelApplyWorkerMain"))?;
        unreachable!();
    };
    MY_PARALLEL_SHARED.with(|s| *s.borrow_mut() = Some(Arc::clone(&shared)));

    // Attach to the message queue.
    mq.set_receiver(g::MyProcNumber());
    let mut mqh = shm_mq_attach(mq);

    // Attach to our worker slot only after the queue is ready for the leader.
    // logicalrep_worker_onexit's stream fileset removal (launcher.c:836-838,
    // a NULL check in a parallel apply worker) registered ahead of it, as in
    // ApplyWorkerMain.
    ipc::before_shmem_exit(
        crate::stream_apply::stream_fileset_delete_on_exit,
        datum::Datum::null(),
    )?;
    launcher::logicalrep_worker_attach(worker_slot)?;
    let w = launcher::worker_snapshot(worker_slot).expect("attached worker slot");
    {
        let mut inner = shared.lock();
        inner.logicalrep_worker_generation = w.generation;
        inner.logicalrep_worker_slot_no = worker_slot;
    }

    let result = pa_worker_body(&shared, &mut mqh, &w);

    // pa_shutdown + dsm_detach on every exit path: park a failure in the
    // error mailbox, detach the queue so the leader's sends see Detached,
    // clear the slot, and poke the leader.
    {
        let mut inner = shared.lock();
        if let Err(e) = &result {
            inner.error = Some(e.clone());
        }
        inner.error_mq_detached = true;
    }
    mqh.detach();
    // No replorigin_session_reset here: the origin is the LEADER's
    // acquisition (acquired_by = leader pid); ReplicationOriginExitCleanup
    // (registered at session setup) skips it for this pid, as in C.
    launcher::logicalrep_worker_detach();
    pa_shutdown(w.leader_pid);

    // The clean-shutdown arm exits the loop with Ok; errors are reported by
    // the bgworker harness after this returns.
    result
}

fn pa_worker_body(
    shared: &Arc<ParallelApplyWorkerShared>,
    mqh: &mut ShmMqHandle,
    w: &launcher::LogicalRepWorker,
) -> PgResult<()> {
    // InitializeLogRepWorker (shared with the leader path).
    let top = mcx::MemoryContext::new("ApplyContext");
    // SAFETY: `top` outlives the worker body (see apply_worker_body).
    let mcx: Mcx<'static> = unsafe { std::mem::transmute(top.mcx()) };
    let Some(_subname) = crate::initialize_logrep_worker(mcx, w)? else {
        return Ok(()); // subscription removed/disabled during startup
    };
    // applyparallelworker.c:960.
    launcher::set_initializing_apply_worker(false);
    let _ = shared;

    // Origin: reuse the origin the leader already acquired.
    let originname = format!("pg_{}", my_sub(|s| s.oid));
    xact::StartTransactionCommand()?;
    let originid = origin::replorigin_by_name(&originname, false)?;
    origin::replorigin_session_setup(originid, w.leader_pid)?;
    origin::set_replorigin_session_origin(originid);
    xact::CommitTransactionCommand()?;

    // applyparallelworker.c:984.
    crate::set_apply_error_context_origin(&originname);

    inval::invalidate::CacheRegisterSyscacheCallback(
        cache_syscache::cacheinfo::SUBSCRIPTIONRELMAP,
        crate::tablesync::invalidate_table_states_cb,
        datum::Datum::null(),
    )?;

    crate::apply::logicalrep_relmap_prepare()?;

    logical_parallel_apply_loop(mqh)
}

pub fn init_seams() {
    logical_worker_seams::parallel_apply_worker_main::set(ParallelApplyWorkerMain);
    logical_worker_seams::handle_parallel_apply_message_interrupt::set(
        HandleParallelApplyMessageInterrupt,
    );
    logical_worker_seams::process_parallel_apply_messages::set(ProcessParallelApplyMessages);
    // IsLogicalParallelApplyWorker (worker.c:4905): IsLogicalWorker() &&
    // am_parallel_apply_worker(); MyParallelShared is only ever set in a PA
    // worker, so the second conjunct implies the first.
    logical_worker_seams::is_logical_parallel_apply_worker::set(am_parallel_apply_worker);
}
