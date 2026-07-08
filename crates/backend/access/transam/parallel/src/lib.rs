#![allow(non_snake_case)]

use std::any::Any;
use std::cell::{Cell, RefCell};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::SeqCst};
use std::sync::mpsc::{Receiver, SyncSender, TryRecvError};
use std::sync::{Arc, Mutex};

use elog::ereport;
use init_small::globals as g;
use types_core::{
    CommandId, InvalidOid, Oid, ProcNumber, SubTransactionId, TimestampTz, XLogRecPtr,
};
use types_error::{
    ErrorLocation, PgError, PgResult, ERRCODE_ADMIN_SHUTDOWN, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERROR, FATAL, WARNING,
};
use types_storage::waiteventset::{WL_EXIT_ON_PM_DEATH, WL_LATCH_SET};
use types_storage::RelFileLocator;

#[cfg(test)]
mod tests;

const SRC: &str = "src/backend/access/transam/parallel.c";

fn loc(line: i32, func: &'static str) -> ErrorLocation {
    ErrorLocation::new(SRC, line, func)
}

// C's error rings are PARALLEL_ERROR_QUEUE_SIZE (16384) bytes; the typed
// channel bounds by message count instead.
const PARALLEL_ERROR_QUEUE_MSGS: usize = 64;

pub type ParallelWorkerEntry = fn(&ParallelShared) -> PgResult<()>;

pub enum WorkerMessage {
    Error(Box<PgError>),
    Notice(Box<PgError>),
    Progress { index: i32, incr: i64 },
    Terminate,
}

pub struct ParallelShared {
    pub database_id: Oid,
    pub authenticated_user_id: Oid,
    pub session_user_id: Oid,
    pub outer_user_id: Oid,
    pub current_user_id: Oid,
    pub sec_context: i32,
    pub session_user_is_superuser: bool,
    pub role_is_superuser: bool,
    pub parallel_leader_pid: i32,
    pub parallel_leader_proc_number: ProcNumber,
    pub xact_ts: TimestampTz,
    pub stmt_ts: TimestampTz,
    pub temp_namespace_id: Oid,
    pub temp_toast_namespace_id: Oid,
    pub last_xlog_end: AtomicU64,
    // ShareSerializableXact handle (SERIALIZABLEXACT* in shared memory as a
    // usize, 0 = invalid); workers adopt it via AttachSerializableXact so SSI
    // conflict tracking spans the whole parallel query.
    serializable_xact_handle: usize,
    // Retention (wretain): the leader's transaction holds invalidation
    // messages not yet broadcast (uncommitted DDL); a retained worker's
    // sinval drain cannot see them, so it must fall back to C's
    // fresh-process InvalidateSystemCaches.
    leader_pending_invals: bool,
    guc_state: Vec<guc::store::NondefaultGuc>,
    // §3.4 P-guc: typed leader capture; when session_guc_bind_enabled() this
    // carries the GUC transfer and guc_state stays empty (and vice versa).
    guc_bind: Vec<guc::store::CapturedGuc>,
    tstate: Vec<u8>,
    combocid: Arc<[(CommandId, CommandId)]>,
    pending_syncs: Vec<(RelFileLocator, bool)>,
    reindex: types_rel::reindex::SerializedReindexState,
    active_snapshot: snapmgr::SerializedSnapshot,
    transaction_snapshot: Option<snapmgr::SerializedSnapshot>,
    clientconninfo: Vec<u8>,
    relmap: relmapper::SerializedActiveRelMaps,
    // SharedRecordTypmodRegistry (typcache.c/session.c): unlike the rest of
    // session.c's DSM (skipped — threads share the address space), the
    // record-type registry is thread_local in TypCacheState and so still
    // needs an explicit handle so workers see the leader's registrations.
    record_registry: typcache_seams::RecordRegistryHandle,
    library_name: String,
    function_name: String,
    error_senders: Vec<Mutex<Option<SyncSender<WorkerMessage>>>>,
    worker_attached: Vec<AtomicBool>,
    private: Mutex<Option<Arc<dyn Any + Send + Sync>>>,
}

const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<ParallelShared>();
};

impl ParallelShared {
    pub fn private(&self) -> Option<Arc<dyn Any + Send + Sync>> {
        self.private.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
}

struct ParallelWorkerInfo {
    bgwhandle: Option<bgworker::BackgroundWorkerHandle>,
    error_receiver: Option<Receiver<WorkerMessage>>,
}

pub struct ParallelContext {
    id: u64,
    subid: SubTransactionId,
    nworkers: i32,
    nworkers_to_launch: i32,
    nworkers_launched: i32,
    library_name: String,
    function_name: String,
    workers: Vec<ParallelWorkerInfo>,
    known_attached_workers: Vec<bool>,
    nknown_attached_workers: i32,
    shared: Option<Arc<ParallelShared>>,
    shared_key: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ParallelContextId(u64);

thread_local! {
    // Set only in ParallelWorkerMain; -1 in the leader and regular backends.
    static PARALLEL_WORKER_NUMBER: Cell<i32> = const { Cell::new(-1) };
    static INITIALIZING_PARALLEL_WORKER: Cell<bool> = const { Cell::new(false) };
    static PCXT_LIST: RefCell<Vec<ParallelContext>> = const { RefCell::new(Vec::new()) };
    // Every-commit AtEOXact_Parallel must pay C's dlist_is_empty, not a
    // RefCell borrow (M1 gate).
    static PCXT_COUNT: Cell<usize> = const { Cell::new(0) };
    static NEXT_PCXT_ID: Cell<u64> = const { Cell::new(1) };
    static MY_WORKER_SHARED: RefCell<Option<Arc<ParallelShared>>> =
        const { RefCell::new(None) };
}

// The dsm-handle analog: bgw_main_arg keys the leader's Arc for the worker.
static SHARED_REGISTRY: Mutex<Vec<(u64, Arc<ParallelShared>)>> = Mutex::new(Vec::new());
static NEXT_SHARED_KEY: AtomicU64 = AtomicU64::new(1);

static REGISTERED_ENTRYPOINTS: Mutex<Vec<(&'static str, ParallelWorkerEntry)>> =
    Mutex::new(Vec::new());

const UNPORTED_INTERNAL_WORKERS: &[&str] = &[
    "ParallelQueryMain",
    "_bt_parallel_build_main",
    "_brin_parallel_build_main",
    "_gin_parallel_build_main",
];

pub fn ParallelWorkerNumber() -> i32 {
    PARALLEL_WORKER_NUMBER.with(|c| c.get())
}

// Gather launch-path phase timestamps, PGRUST_GATHER_TRACE-gated (§2 fixed-cost
// attribution); off the launch path this is never called.
pub fn gtrace(phase: &str) {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    if !*ON.get_or_init(|| std::env::var_os("PGRUST_GATHER_TRACE").is_some()) {
        return;
    }
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros())
        .unwrap_or(0);
    eprintln!("GTRACE {phase} w={} t_us={t}", ParallelWorkerNumber());
}

pub fn IsParallelWorker() -> bool {
    ParallelWorkerNumber() >= 0
}

pub fn InitializingParallelWorker() -> bool {
    INITIALIZING_PARALLEL_WORKER.with(|c| c.get())
}

pub fn register_parallel_worker_entrypoint(name: &'static str, f: ParallelWorkerEntry) {
    let mut table = REGISTERED_ENTRYPOINTS.lock().unwrap_or_else(|e| e.into_inner());
    if !table.iter().any(|(n, _)| *n == name) {
        table.push((name, f));
    }
}

fn LookupParallelWorkerFunction(library_name: &str, function_name: &str) -> PgResult<ParallelWorkerEntry> {
    if library_name != "postgres" {
        panic!(
            "LookupParallelWorkerFunction: external library \"{library_name}\" (no dynamic loading; internal table only)"
        );
    }
    if let Some((_, f)) = REGISTERED_ENTRYPOINTS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .iter()
        .find(|(n, _)| *n == function_name)
    {
        return Ok(*f);
    }
    if UNPORTED_INTERNAL_WORKERS.contains(&function_name) {
        panic!("LookupParallelWorkerFunction: internal worker \"{function_name}\" unported (its owner registers it when its lane lands)");
    }
    Err(ereport(ERROR)
        .errmsg(format!("internal function \"{function_name}\" not found"))
        .into_error()
        .with_error_location(loc(1668, "LookupParallelWorkerFunction"))
        .into())
}

fn with_pcxt<R>(id: ParallelContextId, f: impl FnOnce(&mut ParallelContext) -> R) -> R {
    PCXT_LIST.with(|l| {
        let mut list = l.borrow_mut();
        let pcxt = list
            .iter_mut()
            .find(|p| p.id == id.0)
            .unwrap_or_else(|| panic!("ParallelContext {} not in pcxt_list", id.0));
        f(pcxt)
    })
}

pub fn CreateParallelContext(
    library_name: &str,
    function_name: &str,
    nworkers: i32,
) -> PgResult<ParallelContextId> {
    assert!(xact::IsInParallelMode());
    assert!(nworkers >= 0);

    let id = NEXT_PCXT_ID.with(|c| {
        let id = c.get();
        c.set(id + 1);
        id
    });
    // C dlist_push_head: the head is the newest context, so AtEOSubXact's
    // front-of-list subid scan sees inner-subxact contexts first.
    PCXT_LIST.with(|l| {
        l.borrow_mut().insert(0, ParallelContext {
            id,
            subid: xact::GetCurrentSubTransactionId(),
            nworkers,
            nworkers_to_launch: nworkers,
            nworkers_launched: 0,
            library_name: library_name.to_string(),
            function_name: function_name.to_string(),
            workers: Vec::new(),
            known_attached_workers: Vec::new(),
            nknown_attached_workers: 0,
            shared: None,
            shared_key: None,
        })
    });
    PCXT_COUNT.with(|c| c.set(c.get() + 1));
    Ok(ParallelContextId(id))
}

pub fn InitializeParallelDSM(id: ParallelContextId) -> PgResult<()> {
    gtrace("l.dsm.begin");
    let mut nworkers = with_pcxt(id, |p| p.nworkers);

    if g::InterruptHoldoffCount() != 0 || g::CritSectionCount() != 0 {
        nworkers = 0;
    }
    // Session DSM (C GetSessionDsmHandle nworkers=0 arm): threads share the
    // address space; not transferred (docs/parallel-query-design.md).

    // Unported C arm (SerializeUncommittedEnums, catalog/pg_enum.c). A clean
    // ERROR — not a panic — so the transaction aborts and the session stays
    // usable (the panic-leaves-session-wedged hazard class).
    if nworkers > 0 && pg_enum::HasUncommittedEnums() {
        return ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(
                "cannot start parallel workers with uncommitted enum values: SerializeUncommittedEnums (catalog/pg_enum.c) unported",
            )
            .finish(loc(0, "InitializeParallelDSM"));
    }

    let (current_user_id, sec_context) = miscinit::GetUserIdAndSecContext();
    let (temp_ns, temp_toast_ns) = catalog_namespace::GetTempNamespaceState();

    let tstate = {
        let mut buf = vec![0u8; xact::EstimateTransactionStateSpace()];
        let n = xact::SerializeTransactionState(&mut buf)?;
        buf.truncate(n);
        buf
    };
    let clientconninfo = {
        let mut buf = vec![0u8; miscinit::EstimateClientConnectionInfoSpace()];
        miscinit::SerializeClientConnectionInfo(&mut buf);
        buf
    };
    let active_snapshot = snapmgr::SerializeSnapshot(&snapmgr::GetActiveSnapshot());
    let transaction_snapshot = if xact::IsolationUsesXactSnapshot() {
        Some(snapmgr::SerializeSnapshot(&xact_get_transaction_snapshot()?))
    } else {
        None
    };

    let mut error_senders = Vec::with_capacity(nworkers.max(0) as usize);
    let mut receivers = Vec::with_capacity(nworkers.max(0) as usize);
    let mut worker_attached = Vec::with_capacity(nworkers.max(0) as usize);
    for _ in 0..nworkers {
        let (tx, rx) = std::sync::mpsc::sync_channel(PARALLEL_ERROR_QUEUE_MSGS);
        error_senders.push(Mutex::new(Some(tx)));
        receivers.push(rx);
        worker_attached.push(AtomicBool::new(false));
    }

    let (library_name, function_name) =
        with_pcxt(id, |p| (p.library_name.clone(), p.function_name.clone()));

    let shared = Arc::new(ParallelShared {
        database_id: g::MyDatabaseId(),
        authenticated_user_id: miscinit::GetAuthenticatedUserId(),
        session_user_id: miscinit::GetSessionUserId(),
        outer_user_id: miscinit::GetCurrentRoleId(),
        current_user_id,
        sec_context,
        session_user_is_superuser: miscinit::GetSessionUserIsSuperuser(),
        role_is_superuser: guc_tables::vars::current_role_is_superuser.read(),
        parallel_leader_pid: g::MyProcPid(),
        parallel_leader_proc_number: g::MyProcNumber(),
        xact_ts: xact::GetCurrentTransactionStartTimestamp(),
        stmt_ts: xact::GetCurrentStatementStartTimestamp(),
        temp_namespace_id: temp_ns,
        temp_toast_namespace_id: temp_toast_ns,
        last_xlog_end: AtomicU64::new(0),
        serializable_xact_handle: predicate_seams::share_serializable_xact::call(),
        leader_pending_invals: inval::TransactionHasPendingInvalidationMessages(),
        guc_state: if guc::store::session_guc_bind_enabled() {
            Vec::new()
        } else {
            guc::store::capture_nondefault_variables()
        },
        guc_bind: if guc::store::session_guc_bind_enabled() {
            guc::store::capture_session_gucs()
        } else {
            Vec::new()
        },
        tstate,
        combocid: combocid::SerializeComboCIDState(),
        pending_syncs: catalog_storage::SerializePendingSyncs(),
        reindex: types_rel::reindex::serialize_reindex_state(),
        active_snapshot,
        transaction_snapshot,
        clientconninfo,
        relmap: relmapper::SerializeRelationMap(),
        record_registry: typcache_seams::record_registry_handle::call(),
        library_name,
        function_name,
        error_senders,
        worker_attached,
        private: Mutex::new(None),
    });

    with_pcxt(id, |p| {
        p.nworkers = nworkers;
        p.nworkers_to_launch = nworkers;
        p.workers = receivers
            .into_iter()
            .map(|rx| ParallelWorkerInfo { bgwhandle: None, error_receiver: Some(rx) })
            .collect();
        p.shared = Some(shared);
    });
    gtrace("l.dsm.end");
    Ok(())
}

fn xact_get_transaction_snapshot() -> PgResult<snapmgr::Snapshot> {
    snapmgr::GetTransactionSnapshot()
}

pub fn shared_for(id: ParallelContextId) -> Arc<ParallelShared> {
    with_pcxt(id, |p| p.shared.clone().expect("InitializeParallelDSM not run"))
}

pub fn set_private(id: ParallelContextId, private: Arc<dyn Any + Send + Sync>) {
    let shared = shared_for(id);
    *shared.private.lock().unwrap_or_else(|e| e.into_inner()) = Some(private);
}

pub fn nworkers_launched(id: ParallelContextId) -> i32 {
    with_pcxt(id, |p| p.nworkers_launched)
}

pub fn nworkers(id: ParallelContextId) -> i32 {
    with_pcxt(id, |p| p.nworkers)
}

pub fn nworkers_to_launch(id: ParallelContextId) -> i32 {
    with_pcxt(id, |p| p.nworkers_to_launch)
}

// pcxt->worker[i].bgwhandle (execParallel.c:904 shm_mq_set_handle wiring).
pub fn worker_bgwhandle(
    id: ParallelContextId,
    i: usize,
) -> Option<bgworker::BackgroundWorkerHandle> {
    with_pcxt(id, |p| p.workers.get(i).and_then(|w| w.bgwhandle))
}

pub fn ReinitializeParallelDSM(id: ParallelContextId) -> PgResult<()> {
    WaitForParallelWorkersToFinish(id)?;
    // The handles come out under a short borrow: the shutdown wait services
    // interrupts, and ProcessParallelMessages walks pcxt_list — C's
    // WaitForParallelWorkersToExit holds no lock here (parallel.c:904).
    let handles: Vec<_> = with_pcxt(id, |p| {
        p.workers.iter_mut().filter_map(|w| w.bgwhandle.take()).collect()
    });
    wait_for_workers_to_exit(handles)?;

    with_pcxt(id, |p| {
        let shared = p.shared.as_ref().expect("InitializeParallelDSM not run");
        shared.last_xlog_end.store(0, SeqCst);
        p.nworkers_launched = 0;
        p.known_attached_workers.clear();
        p.nknown_attached_workers = 0;
        for (i, w) in p.workers.iter_mut().enumerate() {
            let (tx, rx) = std::sync::mpsc::sync_channel(PARALLEL_ERROR_QUEUE_MSGS);
            *shared.error_senders[i].lock().unwrap_or_else(|e| e.into_inner()) = Some(tx);
            shared.worker_attached[i].store(false, SeqCst);
            w.bgwhandle = None;
            w.error_receiver = Some(rx);
        }
    });
    Ok(())
}

pub fn ReinitializeParallelWorkers(id: ParallelContextId, nworkers_to_launch: i32) {
    with_pcxt(id, |p| {
        debug_assert!(p.nworkers_launched == 0);
        p.nworkers_to_launch = p.nworkers.min(nworkers_to_launch);
    });
}

pub fn LaunchParallelWorkers(id: ParallelContextId) -> PgResult<i32> {
    let (nworkers_to_launch, shared) =
        with_pcxt(id, |p| (p.nworkers_to_launch, p.shared.clone()));
    if nworkers_to_launch == 0 {
        return Ok(0);
    }
    let shared = shared.expect("InitializeParallelDSM not run");
    gtrace("l.launch.begin");

    lmgr_proc::BecomeLockGroupLeader()?;

    let key = with_pcxt(id, |p| p.shared_key) .unwrap_or_else(|| {
        let key = NEXT_SHARED_KEY.fetch_add(1, SeqCst);
        SHARED_REGISTRY
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push((key, Arc::clone(&shared)));
        with_pcxt(id, |p| p.shared_key = Some(key));
        key
    });

    let leader_pid = g::MyProcPid();
    let mut any_registrations_failed = false;
    let mut launched = 0;
    for i in 0..nworkers_to_launch {
        // C 618-649: after one failure, stop registering; the caller must
        // tolerate fewer workers than requested.
        if any_registrations_failed {
            with_pcxt(id, |p| p.workers[i as usize].error_receiver = None);
            continue;
        }
        let mut bgw_extra = [0u8; bgworker::BGW_EXTRALEN];
        bgw_extra[0..4].copy_from_slice(&i.to_ne_bytes());
        let worker = bgworker::BackgroundWorker {
            bgw_name: format!("parallel worker for PID {leader_pid}"),
            bgw_type: "parallel worker".to_string(),
            bgw_flags: bgworker::BGWORKER_SHMEM_ACCESS
                | bgworker::BGWORKER_BACKEND_DATABASE_CONNECTION
                | bgworker::BGWORKER_CLASS_PARALLEL,
            bgw_start_time: bgworker::BgWorkerStartTime::ConsistentState,
            bgw_restart_time: bgworker::BGW_NEVER_RESTART,
            bgw_main: parallel_worker_main_thunk,
            bgw_main_arg: key,
            bgw_extra,
            bgw_notify_pid: leader_pid,
        };
        match bgworker::RegisterDynamicBackgroundWorker(worker)? {
            Some(handle) => {
                launched += 1;
                with_pcxt(id, |p| p.workers[i as usize].bgwhandle = Some(handle));
            }
            None => {
                any_registrations_failed = true;
                with_pcxt(id, |p| p.workers[i as usize].error_receiver = None);
            }
        }
    }
    with_pcxt(id, |p| {
        p.nworkers_launched = launched;
        p.known_attached_workers = vec![false; p.nworkers_to_launch as usize];
        p.nknown_attached_workers = 0;
    });
    gtrace("l.launch.end");
    Ok(launched)
}

fn worker_failed_to_init<T>(func: &'static str, line: i32) -> PgResult<T> {
    Err(ereport(ERROR)
        .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
        .errmsg("parallel worker failed to initialize")
        .errhint("More details may be available in the server log.")
        .into_error()
        .with_error_location(loc(line, func))
        .into())
}

pub fn WaitForParallelWorkersToAttach(id: ParallelContextId) -> PgResult<()> {
    if with_pcxt(id, |p| p.nworkers_launched == 0) {
        return Ok(());
    }
    loop {
        postgres_seams::check_for_interrupts::call()?;
        ProcessParallelMessages()?;

        let mut all_known = true;
        let n = with_pcxt(id, |p| p.workers.len());
        for i in 0..n {
            let (known, has_receiver, bgwhandle) = with_pcxt(id, |p| {
                (
                    p.known_attached_workers.get(i).copied().unwrap_or(true),
                    p.workers[i].error_receiver.is_some(),
                    p.workers[i].bgwhandle,
                )
            });
            if known || !has_receiver {
                continue;
            }
            let Some(handle) = bgwhandle else { continue };
            // Status BEFORE the attached flag: a worker can attach+exit between
            // the two reads, and stale not-attached + STOPPED is a false
            // init-failure (C reads shm_mq_get_sender after BGWH_STOPPED).
            let status = bgworker::GetBackgroundWorkerPid(&handle).0;
            let attached = with_pcxt(id, |p| {
                p.shared.as_ref().is_some_and(|s| s.worker_attached[i].load(SeqCst))
            });
            match status {
                bgworker::BgwHandleStatus::BGWH_STARTED if attached => {
                    mark_known_attached(id, i);
                }
                bgworker::BgwHandleStatus::BGWH_STOPPED
                | bgworker::BgwHandleStatus::BGWH_POSTMASTER_DIED => {
                    if !attached {
                        return worker_failed_to_init("WaitForParallelWorkersToAttach", 757);
                    }
                    mark_known_attached(id, i);
                }
                _ => all_known = false,
            }
        }
        if all_known {
            return Ok(());
        }
        wait_on_my_latch(WAIT_EVENT_BGWORKER_STARTUP);
    }
}

const PG_WAIT_IPC: u32 = 0x0800_0000;
const WAIT_EVENT_BGWORKER_STARTUP: u32 = PG_WAIT_IPC + 6;
const WAIT_EVENT_PARALLEL_FINISH: u32 = PG_WAIT_IPC + 32;

fn wait_on_my_latch(wait_event: u32) {
    let latch = g::MyLatch().expect("parallel leader without MyLatch");
    let _ = latch::WaitLatch(Some(latch), WL_LATCH_SET | WL_EXIT_ON_PM_DEATH, 0, wait_event);
    latch::ResetLatch(latch);
}

fn mark_known_attached(id: ParallelContextId, i: usize) {
    with_pcxt(id, |p| {
        if !p.known_attached_workers[i] {
            p.known_attached_workers[i] = true;
            p.nknown_attached_workers += 1;
        }
    });
}

pub fn WaitForParallelWorkersToFinish(id: ParallelContextId) -> PgResult<()> {
    loop {
        postgres_seams::check_for_interrupts::call()?;
        ProcessParallelMessages()?;

        let (nfinished, launched) = with_pcxt(id, |p| {
            let done = p
                .workers
                .iter()
                .take(p.nworkers_launched.max(0) as usize)
                .filter(|w| w.error_receiver.is_none())
                .count() as i32;
            (done, p.nworkers_launched)
        });
        if nfinished >= launched {
            break;
        }

        // C 858-885: nobody known-attached alive would deadlock the wait; a
        // stopped worker that never attached is an initialization failure.
        if with_pcxt(id, |p| p.nknown_attached_workers < p.nworkers_launched) {
            let n = with_pcxt(id, |p| p.workers.len());
            for i in 0..n {
                let (known, has_receiver, bgwhandle) = with_pcxt(id, |p| {
                    (
                        p.known_attached_workers.get(i).copied().unwrap_or(true),
                        p.workers[i].error_receiver.is_some(),
                        p.workers[i].bgwhandle,
                    )
                });
                if known || !has_receiver {
                    continue;
                }
                let Some(handle) = bgwhandle else { continue };
                // Status first (see WaitForParallelWorkersToAttach).
                let status = bgworker::GetBackgroundWorkerPid(&handle).0;
                let attached = with_pcxt(id, |p| {
                    p.shared.as_ref().is_some_and(|s| s.worker_attached[i].load(SeqCst))
                });
                if matches!(
                    status,
                    bgworker::BgwHandleStatus::BGWH_STOPPED
                        | bgworker::BgwHandleStatus::BGWH_POSTMASTER_DIED
                ) && !attached
                {
                    return worker_failed_to_init("WaitForParallelWorkersToFinish", 878);
                }
                if attached {
                    mark_known_attached(id, i);
                }
            }
        }

        wait_on_my_latch(WAIT_EVENT_PARALLEL_FINISH);
    }

    if let Some(shared) = with_pcxt(id, |p| p.shared.clone()) {
        let end = shared.last_xlog_end.load(SeqCst) as XLogRecPtr;
        if end > transam_xlog_seams::xact_last_rec_end::call() {
            transam_xlog_seams::set_xact_last_rec_end::call(end);
        }
    }
    Ok(())
}

// Callers must NOT hold the PCXT_LIST borrow: the shutdown wait services
// interrupts, which re-enter pcxt_list via ProcessParallelMessages.
fn wait_for_workers_to_exit(handles: Vec<bgworker::BackgroundWorkerHandle>) -> PgResult<()> {
    for handle in handles {
        match bgworker::WaitForBackgroundWorkerShutdown(&handle)? {
            bgworker::BgwHandleStatus::BGWH_POSTMASTER_DIED => {
                return ereport(FATAL)
                    .errcode(ERRCODE_ADMIN_SHUTDOWN)
                    .errmsg("postmaster exited during a parallel transaction")
                    .finish(loc(939, "WaitForParallelWorkersToExit"));
            }
            _ => {}
        }
    }
    Ok(())
}

pub fn DestroyParallelContext(id: ParallelContextId) -> PgResult<()> {
    // Unlinked first so error paths cannot re-enter (C 968-975).
    let mut pcxt = PCXT_LIST.with(|l| {
        let mut list = l.borrow_mut();
        let idx = list
            .iter()
            .position(|p| p.id == id.0)
            .unwrap_or_else(|| panic!("ParallelContext {} not in pcxt_list", id.0));
        list.remove(idx)
    });
    PCXT_COUNT.with(|c| c.set(c.get() - 1));

    for w in pcxt.workers.iter_mut() {
        if w.error_receiver.is_some() {
            if let Some(handle) = w.bgwhandle {
                bgworker::TerminateBackgroundWorker(&handle);
            }
            w.error_receiver = None;
        }
    }
    if let Some(key) = pcxt.shared_key.take() {
        SHARED_REGISTRY
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|(k, _)| *k != key);
    }

    let handles: Vec<_> = pcxt.workers.iter_mut().filter_map(|w| w.bgwhandle.take()).collect();
    g::HoldInterrupts();
    let result = wait_for_workers_to_exit(handles);
    g::ResumeInterrupts();
    result
}

pub fn ParallelContextActive() -> bool {
    PCXT_COUNT.with(|c| c.get() != 0)
}

pub fn AtEOXact_Parallel(is_commit: bool) -> PgResult<()> {
    if !ParallelContextActive() {
        return Ok(());
    }
    while let Some(id) = PCXT_LIST.with(|l| l.borrow().first().map(|p| ParallelContextId(p.id))) {
        if is_commit {
            let _ = elog::elog(WARNING, "leaked parallel context");
        }
        DestroyParallelContext(id)?;
    }
    Ok(())
}

pub fn AtEOSubXact_Parallel(is_commit: bool, my_subid: SubTransactionId) -> PgResult<()> {
    if !ParallelContextActive() {
        return Ok(());
    }
    loop {
        let front = PCXT_LIST.with(|l| {
            l.borrow()
                .first()
                .filter(|p| p.subid == my_subid)
                .map(|p| ParallelContextId(p.id))
        });
        let Some(id) = front else { return Ok(()) };
        if is_commit {
            let _ = elog::elog(WARNING, "leaked parallel context");
        }
        DestroyParallelContext(id)?;
    }
}

pub fn HandleParallelMessageInterrupt() {
    g::SetInterruptPending(true);
    g::SetParallelMessagePending(true);
    if let Some(latch) = g::MyLatch() {
        latch::SetLatch(latch);
    }
}

pub fn ProcessParallelMessages() -> PgResult<()> {
    g::SetParallelMessagePending(false);
    g::HoldInterrupts();
    let result = process_parallel_messages_guts();
    g::ResumeInterrupts();
    result
}

fn process_parallel_messages_guts() -> PgResult<()> {
    let ids: Vec<ParallelContextId> =
        PCXT_LIST.with(|l| l.borrow().iter().map(|p| ParallelContextId(p.id)).collect());
    for id in ids {
        let n = PCXT_LIST.with(|l| {
            l.borrow().iter().find(|p| p.id == id.0).map(|p| p.workers.len()).unwrap_or(0)
        });
        for i in 0..n {
            loop {
                let msg = with_pcxt(id, |p| {
                    p.workers[i].error_receiver.as_ref().map(|rx| rx.try_recv())
                });
                match msg {
                    None => break,
                    Some(Ok(m)) => {
                        mark_known_attached(id, i);
                        match m {
                            WorkerMessage::Error(mut e) => {
                                if e.level > ERROR {
                                    // Death of a worker isn't enough
                                    // justification for suicide (C 1167).
                                    e.level = ERROR;
                                }
                                append_parallel_worker_context(&mut e);
                                return Err(e);
                            }
                            WorkerMessage::Notice(mut e) => {
                                append_parallel_worker_context(&mut e);
                                elog::emit_error_report_for(&e);
                            }
                            WorkerMessage::Progress { index, incr } => {
                                backend_progress::pgstat_progress_incr_param(
                                    index as usize,
                                    incr,
                                );
                            }
                            WorkerMessage::Terminate => {
                                with_pcxt(id, |p| p.workers[i].error_receiver = None);
                                break;
                            }
                        }
                    }
                    Some(Err(TryRecvError::Empty)) => break,
                    Some(Err(TryRecvError::Disconnected)) => {
                        return ereport(ERROR)
                            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                            .errmsg("lost connection to parallel worker")
                            .finish(loc(1126, "ProcessParallelMessages"));
                    }
                }
            }
        }
    }
    Ok(())
}

const DEBUG_PARALLEL_REGRESS: i32 = 2;

fn append_parallel_worker_context(e: &mut PgError) {
    if guc_tables::vars::debug_parallel_query.read() != DEBUG_PARALLEL_REGRESS {
        e.add_context_line("parallel worker");
    }
}

pub fn ParallelWorkerReportLastRecEnd(last_rec_end: XLogRecPtr) -> PgResult<()> {
    MY_WORKER_SHARED.with(|s| {
        let shared = s.borrow();
        let shared = shared
            .as_ref()
            .unwrap_or_else(|| panic!("ParallelWorkerReportLastRecEnd outside a parallel worker"));
        shared.last_xlog_end.fetch_max(last_rec_end as u64, SeqCst);
    });
    Ok(())
}

thread_local! {
    static MY_PROGRESS_SENDER: RefCell<Option<SyncSender<WorkerMessage>>> =
        const { RefCell::new(None) };
}

// pgstat_progress_parallel_incr_param's worker leg (C sends PqMsg_Progress on
// the redirected pq channel; the error mq is that channel here).
pub fn parallel_worker_report_progress(index: i32, incr: i64) {
    let sent = MY_PROGRESS_SENDER.with(|c| {
        let slot = c.borrow();
        let Some(sender) = slot.as_ref() else {
            return None;
        };
        let _ = sender.send(WorkerMessage::Progress { index, incr });
        MY_WORKER_SHARED.with(|s| {
            s.borrow().as_ref().map(|sh| {
                (sh.parallel_leader_pid, sh.parallel_leader_proc_number)
            })
        })
    });
    let Some((leader_pid, leader_proc)) = sent else {
        panic!("parallel_worker_report_progress outside a parallel worker");
    };
    procsignal::SendProcSignal(
        leader_pid,
        types_storage::storage::ProcSignalReason::PROCSIG_PARALLEL_MESSAGE,
        leader_proc,
    );
}

fn take_my_error_sender(shared: &ParallelShared, worker_number: i32) -> SyncSender<WorkerMessage> {
    let mut slot = shared.error_senders[worker_number as usize]
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    slot.take().expect("parallel worker error sender already taken")
}

fn parallel_worker_main_thunk(main_arg: u64) -> PgResult<()> {
    ParallelWorkerMain(main_arg)
}

pub fn ParallelWorkerMain(main_arg: u64) -> PgResult<()> {
    INITIALIZING_PARALLEL_WORKER.with(|c| c.set(true));

    let entry = bgworker::MyBgworkerEntry().expect("ParallelWorkerMain without bgworker entry");
    let worker_number = i32::from_ne_bytes(entry.bgw_extra[0..4].try_into().unwrap());
    debug_assert!(worker_number >= 0);
    PARALLEL_WORKER_NUMBER.with(|c| c.set(worker_number));
    gtrace("w.main.enter");

    let shared = SHARED_REGISTRY
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .iter()
        .find(|(k, _)| *k == main_arg)
        .map(|(_, s)| Arc::clone(s));
    let Some(shared) = shared else {
        return ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg("could not map dynamic shared memory segment")
            .finish(loc(1347, "ParallelWorkerMain"));
    };
    MY_WORKER_SHARED.with(|s| *s.borrow_mut() = Some(Arc::clone(&shared)));

    let sender = take_my_error_sender(&shared, worker_number);
    MY_PROGRESS_SENDER.with(|c| *c.borrow_mut() = Some(sender.clone()));
    shared.worker_attached[worker_number as usize].store(true, SeqCst);
    // C shm_mq_set_sender wakes the leader's attach wait.
    latch::SetLatch(types_storage::latch::LatchHandle::proc(
        shared.parallel_leader_proc_number,
    ));
    gtrace("w.attached");

    // C pq_redirect_to_shm_mq: sub-ERROR client-bound reports become 'N'
    // messages; ERROR+ is forwarded exactly once from the unwind payload.
    let notice_sender = sender.clone();
    let (leader_pid, leader_proc) =
        (shared.parallel_leader_pid, shared.parallel_leader_proc_number);
    let prev_redirect = elog::set_frontend_redirect(Some(Box::new(move |e: &PgError| {
        if e.level >= ERROR {
            return;
        }
        let _ = notice_sender.send(WorkerMessage::Notice(Box::new(e.clone())));
        procsignal::SendProcSignal(
            leader_pid,
            types_storage::storage::ProcSignalReason::PROCSIG_PARALLEL_MESSAGE,
            leader_proc,
        );
    })));
    let prev_dest = elog::config::where_to_send_output();
    elog::config::set_where_to_send_output(types_dest::CommandDest::Remote);

    let body = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        parallel_worker_body(&shared, worker_number)
    }));

    elog::config::set_where_to_send_output(prev_dest);
    elog::set_frontend_redirect(prev_redirect);
    let result = match body {
        Ok(r) => r,
        Err(payload) => match types_error::pg_error_from_panic(payload) {
            Ok(e) => Err(Box::new(e)),
            Err(payload) => {
                let msg = payload
                    .downcast_ref::<&str>()
                    .map(|s| s.to_string())
                    .or_else(|| payload.downcast_ref::<String>().cloned())
                    .unwrap_or_else(|| "parallel worker panicked".to_string());
                Err(Box::new(PgError::error(msg)))
            }
        },
    };

    // ParallelWorkerShutdown's guarantee: the leader always hears from us.
    let outcome = match result {
        Ok(()) => {
            let _ = sender.send(WorkerMessage::Terminate);
            Ok(())
        }
        Err(e) => {
            let _ = sender.send(WorkerMessage::Error(Box::new((*e).clone())));
            Err(e)
        }
    };
    MY_PROGRESS_SENDER.with(|c| *c.borrow_mut() = None);
    drop(sender);
    procsignal::SendProcSignal(
        shared.parallel_leader_pid,
        types_storage::storage::ProcSignalReason::PROCSIG_PARALLEL_MESSAGE,
        shared.parallel_leader_proc_number,
    );
    MY_WORKER_SHARED.with(|s| *s.borrow_mut() = None);
    outcome
}

fn parallel_worker_body(shared: &Arc<ParallelShared>, _worker_number: i32) -> PgResult<()> {
    // C 1400-1402: leader already gone — exit quietly (Terminate still sent).
    if !lmgr_proc::BecomeLockGroupMember(
        shared.parallel_leader_proc_number,
        shared.parallel_leader_pid,
    )? {
        return Ok(());
    }

    xact::SetParallelStartTimestamps(shared.xact_ts, shared.stmt_ts);

    let entrypt = LookupParallelWorkerFunction(&shared.library_name, &shared.function_name)?;

    miscinit::SetAuthenticatedUserId(shared.authenticated_user_id);
    miscinit::SetSessionAuthorization(shared.session_user_id, shared.session_user_is_superuser)?;
    miscinit::SetCurrentRoleId(shared.outer_user_id, shared.role_is_superuser)?;

    // C's BackgroundWorkerInitializeConnectionByOid(InvalidOid) still runs
    // InitPostgres for a database-less worker; our InitPostgres has no such
    // arm yet, so InvalidOid (the substrate e2e) skips the connect step.
    if shared.database_id != InvalidOid {
        gtrace("w.conn.begin");
        bgworker::BackgroundWorkerInitializeConnectionByOid(
            shared.database_id,
            shared.authenticated_user_id,
            bgworker::BGWORKER_BYPASS_ALLOWCONN | bgworker::BGWORKER_BYPASS_ROLELOGINCHECK,
        )?;
        mbutils::SetClientEncoding(mbutils::GetDatabaseEncoding())?;
        gtrace("w.conn.end");
    }

    xact::StartParallelWorkerTransaction(&shared.tstate)?;
    gtrace("w.txn.started");

    catalog_storage::RestorePendingSyncs(&shared.pending_syncs);
    relmapper::RestoreRelationMap(&shared.relmap)?;
    types_rel::reindex::restore_reindex_state(
        &shared.reindex,
        xact::GetCurrentTransactionNestLevel(),
    );
    combocid::RestoreComboCIDState(&shared.combocid);
    // Session attach: skipped (docs/parallel-query-design.md) except for the
    // record-type registry, which — unlike the rest of session.c's DSM state —
    // is not otherwise visible across threads (TypCacheState is thread_local).
    typcache_seams::install_record_registry::call(std::sync::Arc::clone(&shared.record_registry));

    let asnapshot = snapmgr::RestoreSnapshot(&shared.active_snapshot);
    let tsource = shared.transaction_snapshot.as_ref().unwrap_or(&shared.active_snapshot);
    snapmgr::RestoreTransactionSnapshot(tsource, shared.parallel_leader_proc_number)?;
    snapmgr::PushActiveSnapshot(&asnapshot)?;

    if init_small::wretain::warm_claim() && !shared.leader_pending_invals {
        // Retention warm claim: caches were drained against the shared queue
        // at InitPostgres (postinit warm arm); a second cheap drain here
        // covers messages that arrived since. C's blanket invalidation is
        // only needed for a fresh process's incidentally-mistimed cache
        // loads — or for the leader's own uncommitted DDL, which forces the
        // fallback arm below.
        gtrace("w.retain.inval.begin");
        inval::local::AcceptInvalidationMessages()?;
        gtrace("w.retain.inval.drained");
    } else {
        gtrace("w.cold.inval.begin");
        inval::local::InvalidateSystemCaches()?;
    }
    gtrace("w.inval.done");

    // A retained thread keeps its previous task's session GUCs (a C worker
    // is a fresh process; RestoreGUCState overlays postmaster state only);
    // the transfer below only SETs, so a variable the new leader has at
    // default would silently keep the old task's value — RESET ALL semantics
    // (guc.c:2003) rolls them back first. Shipped instance: matview
    // datafill's RestrictSearchPath search_path='' surviving into later
    // tasks, breaking worker-side function name lookup.
    if init_small::wretain::warm_claim() {
        guc::ResetAllOptions();
    }
    let _guc_binding = if guc::store::session_guc_bind_enabled() {
        Some(guc::store::bind_session_gucs(&shared.guc_bind)?)
    } else {
        guc::store::restore_nondefault_variables(&shared.guc_state)?;
        None
    };
    gtrace("w.guc.done");

    miscinit::SetUserIdAndSecContext(shared.current_user_id, shared.sec_context);

    catalog_namespace::SetTempNamespaceState(
        shared.temp_namespace_id,
        shared.temp_toast_namespace_id,
    );

    miscinit::RestoreClientConnectionInfo(&shared.clientconninfo)?;
    if miscinit::client_connection_info().0.is_some() {
        panic!("ParallelWorkerMain: InitializeSystemUser (SYSTEM_USER for authenticated identity) unported");
    }

    predicate_seams::attach_serializable_xact::call(shared.serializable_xact_handle)?;

    INITIALIZING_PARALLEL_WORKER.with(|c| c.set(false));
    xact::EnterParallelMode();

    gtrace("w.entry.begin");
    entrypt(shared)?;
    gtrace("w.entry.end");

    xact::ExitParallelMode();
    snapmgr::PopActiveSnapshot()?;
    xact::EndParallelWorkerTransaction()?;
    // A clean task parks this thread (wretain); C's worker process would die
    // here, taking the temp-namespace TLS with it. Errored tasks rotate the
    // thread out, so the success path is the only park that needs this.
    catalog_namespace::ResetTempNamespaceStateForRetainedPark();
    Ok(())
}

pub fn init_seams() {
    parallel_seams::is_parallel_worker::set(IsParallelWorker);
    parallel_seams::initializing_parallel_worker::set(InitializingParallelWorker);
    parallel_seams::at_eoxact_parallel::set(AtEOXact_Parallel);
    parallel_seams::at_eosubxact_parallel::set(AtEOSubXact_Parallel);
    parallel_seams::parallel_worker_report_last_rec_end::set(ParallelWorkerReportLastRecEnd);
    parallel_seams::handle_parallel_message_interrupt::set(HandleParallelMessageInterrupt);
    parallel_seams::parallel_worker_report_progress::set(parallel_worker_report_progress);
    parallel_seams::process_parallel_messages::set(ProcessParallelMessages);
}
