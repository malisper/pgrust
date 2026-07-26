#![allow(non_snake_case)]

use std::any::Any;
use std::cell::{Cell, RefCell};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering::SeqCst};
use std::sync::Arc;

// permit-s4 row 7: the per-worker error queues ride pgsync::mailbox
// (bounded 64). Leader-detach turns worker sends into drop-and-continue
// structurally: dropping the receiver closes the mailbox and send returns
// Err — C's detached-mq send failure, with no bespoke flag to maintain.
use pgsync::{Mutex, MailboxReceiver, MailboxSender, TryRecv};

use elog::ereport;
use init_small::globals as g;
use types_core::{
    CommandId, InvalidOid, Oid, ProcNumber, SubTransactionId, TimestampTz, XLogRecPtr,
};
use types_error::{
    ErrorLocation, PgError, PgResult, ERRCODE_ADMIN_SHUTDOWN, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERROR, FATAL, WARNING,
};
use types_storage::RelFileLocator;

mod query_task_guard;
pub mod standing;

pub use query_task_guard::QueryTaskBindingGuard;
// Ceremony-v2 (notes/runtime-ceremony2.md): deferred first-touch binding +
// sticky session-affine retention for standing runtime executors.
pub use query_task_guard::{
    lazy_bind_enabled, sticky_bind_enabled, sticky_parked, DeferredQueryTaskBinding,
};

#[cfg(debug_assertions)]
pub use query_task_guard::{
    set_query_task_fault, QueryTaskFaultAction, QueryTaskFaultPoint,
};

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

#[derive(Clone, Copy, Debug, Default)]
pub struct QueryTaskBindingPolicy {
    pub has_params: bool,
    pub temp_state: bool,
    pub serializable: bool,
    pub pending_invalidations: bool,
    /// M4.2 (utility DDL on the pool): the installer DECLARES the target
    /// transaction carries unbroadcast (uncommitted-DDL) invalidation
    /// messages and requests the launched-substrate fallback semantics on
    /// every bind: blanket `InvalidateSystemCaches` (C fresh-process
    /// parity — cache entries must be rebuilt under the bound snapshot/xid
    /// so the leader's uncommitted catalog rows are seen) plus an EAGER
    /// caches taint (`wretain::note_caches_tainted`: if the bound
    /// transaction aborts, no sinval traffic ever corrects entries built
    /// during it — the next adoption on this thread re-blankets instead of
    /// trusting the cheap drain; the exact law of the launched path in
    /// lib.rs `leader_pending_invals`). Without this flag a pending-invals
    /// target keeps today's fail-closed refusal.
    pub invals_flush: bool,
}

const QUERY_TASK_INSTALLED: u8 = 1 << 0;
const QUERY_TASK_PARAMS: u8 = 1 << 1;
const QUERY_TASK_TEMP: u8 = 1 << 2;
const QUERY_TASK_SERIALIZABLE: u8 = 1 << 3;
const QUERY_TASK_PENDING_INVALS: u8 = 1 << 4;
const QUERY_TASK_INVALS_FLUSH: u8 = 1 << 5;

// Post-task park hooks (harvested with the query-task binder from
// morsel/query-task-binder-20260710 @ 1b3cba43f; entrypoint-table precedent).
// POST_TASK_PARK runs on a worker thread after its task fully ended and
// Terminate was sent — the leader's finish wait is already satisfied, so
// parking there cannot deadlock it. PRIVATE_SHUTDOWN runs in
// DestroyParallelContext before it waits for worker exit: it must release
// anything a worker could still be parked on. Inert unless registered; on
// this tree only the binder substrate e2e registers them (the runtime
// pool/scheduler lane owns the production park).
// MULTI-REGISTRANT (M2 reconciliation of both lanes' independent fixes):
// several runtime arms coexist (M1 runtime-scan, the M2 agg + distinct sink
// arms), each with its own private-payload type — a single OnceLock slot
// silently dropped the second arm's hook (its helpers would park as no-ops
// and wedge the leader's wait). Every hook downcasts the context's private
// payload and no-ops on foreign types, so calling every registrant in
// registration order is correct by construction. Registration is append-only
// and idempotent (fn-pointer dedup via fn_addr_eq); the lists are tiny,
// written once per arm per process, read per worker task.
pgsync::process_global! {
    static POST_TASK_PARK: Mutex<Vec<fn(&ParallelShared)>> = Mutex::new(Vec::new());
    static PRIVATE_SHUTDOWN: Mutex<Vec<fn(&(dyn Any + Send + Sync))>> = Mutex::new(Vec::new());
}

pub fn register_parallel_post_task_park(f: fn(&ParallelShared)) {
    let mut v = POST_TASK_PARK.lock().unwrap_or_else(|p| p.into_inner());
    if !v.iter().any(|&h| core::ptr::fn_addr_eq(h, f)) {
        v.push(f);
    }
}

pub fn register_parallel_private_shutdown(f: fn(&(dyn Any + Send + Sync))) {
    let mut v = PRIVATE_SHUTDOWN.lock().unwrap_or_else(|p| p.into_inner());
    if !v.iter().any(|&h| core::ptr::fn_addr_eq(h, f)) {
        v.push(f);
    }
}

fn post_task_park_hooks() -> Vec<fn(&ParallelShared)> {
    POST_TASK_PARK.lock().unwrap_or_else(|p| p.into_inner()).clone()
}

fn private_shutdown_hooks() -> Vec<fn(&(dyn Any + Send + Sync))> {
    PRIVATE_SHUTDOWN.lock().unwrap_or_else(|p| p.into_inner()).clone()
}

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
    // §3.4 P-guc via the layered-snapshot query pin (guc::layers): the
    // leader's per-statement composed capture, shared by Arc — one capture
    // per statement window regardless of worker count, and the worker adopts
    // the leader's base (started-with parity). When session_guc_bind_enabled()
    // this carries the GUC transfer and guc_state stays empty (and vice
    // versa: PGRUST_NO_GUC_BIND reverts to the string restore path).
    guc_pin: Option<Arc<guc::layers::GucQuerySnapshot>>,
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
    error_senders: Vec<Mutex<Option<MailboxSender<WorkerMessage>>>>,
    worker_attached: Vec<AtomicBool>,
    private: Mutex<Option<Arc<dyn Any + Send + Sync>>>,
    // Per-arm standing-gang driver (M2 inc-1): the engagement's driver
    // rides its shared state so several runtime arms can use the standing
    // channel without a process-global driver slot. Set alongside
    // `private` (set_standing_driver), read by standing::serve_ticket.
    standing_driver: Mutex<Option<standing::StandingDriver>>,
    query_task_binding: AtomicU8,
}

const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<ParallelShared>();
};

impl ParallelShared {
    pub fn private(&self) -> Option<Arc<dyn Any + Send + Sync>> {
        self.private.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    /// This engagement's standing-gang driver (M2 inc-1 per-arm dispatch).
    pub fn standing_driver(&self) -> Option<standing::StandingDriver> {
        *self
            .standing_driver
            .lock()
            .unwrap_or_else(|e| e.into_inner())
    }
}

struct ParallelWorkerInfo {
    bgwhandle: Option<bgworker::BackgroundWorkerHandle>,
    error_receiver: Option<MailboxReceiver<WorkerMessage>>,
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
    // GL-SYNCWEDGE-1: the leader park loop's stall clock, kept across quanta
    // (see wait_on_my_latch). NON-SESSION TLS, and declared inside this block
    // rather than as its own so the TLS census gains no new row — the
    // eoxact_parts.rs/lanetable precedent recorded in session/src/tests.rs.
    // It is a pure diagnostic timer over THIS THREAD's current park episode:
    // a session cannot migrate off a thread while that thread is parked, it
    // carries no session identity, an envelope would have nothing to capture,
    // and the worst consequence of a stale value is one spurious or one
    // missed LOG line. Taken out of the cell for the duration of the sleep so
    // a re-entrant park cannot double-borrow.
    static PARK_STALL: RefCell<Option<shm_mq::stall::ParkStallClock>> =
        const { RefCell::new(None) };
}

// The dsm-handle analog: bgw_main_arg keys the leader's Arc for the worker.
pgsync::process_global! {
    static SHARED_REGISTRY: Mutex<Vec<(u64, Arc<ParallelShared>)>> = Mutex::new(Vec::new());
}
static NEXT_SHARED_KEY: AtomicU64 = AtomicU64::new(1);

pgsync::process_global! {
    static REGISTERED_ENTRYPOINTS: Mutex<Vec<(&'static str, ParallelWorkerEntry)>> =
        Mutex::new(Vec::new());
}

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
        let (tx, rx) = pgsync::mailbox(Some(PARALLEL_ERROR_QUEUE_MSGS));
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
        guc_pin: if guc::store::session_guc_bind_enabled() {
            Some(guc::layers::current_query_pin())
        } else {
            None
        },
        tstate,
        combocid: combocid::SerializeComboCIDState(),
        pending_syncs: catalog_storage::SerializePendingSyncs(),
        reindex: types_rel::reindex::serialize_reindex_state(),
        active_snapshot,
        transaction_snapshot,
        clientconninfo,
        relmap: relmapper::SerializeRelationMap(),
        // Guarded for typcache-less rigs (substrate/gather e2e harnesses).
        record_registry: if typcache_seams::record_registry_handle::is_installed() {
            typcache_seams::record_registry_handle::call()
        } else {
            Default::default()
        },
        library_name,
        function_name,
        error_senders,
        worker_attached,
        private: Mutex::new(None),
        standing_driver: Mutex::new(None),
        query_task_binding: AtomicU8::new(0),
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

/// GL-STMTTASK-2 change 2 (pointer-passing, not DSM ritual): build the
/// query-task binder target for ONE dop-1 statement task WITHOUT the
/// parallel-context ceremony — no pcxt-list entry, no error mailboxes, no
/// worker bookkeeping, no SHARED_REGISTRY key, no DestroyParallelContext
/// walk. Everything the worker-side binder actually consumes is captured
/// exactly as `InitializeParallelDSM` captures it (same sources, same
/// per-statement re-arm set: transaction state, snapshots, timestamps,
/// combocid, pending syncs, reindex, relmap, invalidation posture) — the
/// re-arm resets exactly what C resets between statements. Session-stable
/// state travels by POINTER (Arc): the GUC query pin (statement-window
/// cached — its Arc identity is the sticky-resume key), the record
/// registry, the combocid share. The DSM-shaped path stays untouched for
/// REAL parallel engagements.
///
/// `Ok(None)` = refuse-by-name (interrupt-holdoff / critical section, or
/// uncommitted enum values — the launched path's C-parity error gate,
/// rendered fail-closed): the caller keeps the incumbent serial loop.
/// The binding policy is installed at construction (the
/// InstallQueryTaskBinding fold); the CALLER owns the standing-board join
/// (`standing::close_and_await`) before its arena unwinds — with no pcxt
/// there is no private-shutdown hook, so the caller must bracket the
/// engagement in its own RAII guard.
pub fn statement_task_shared(
    policy: QueryTaskBindingPolicy,
) -> PgResult<Option<Arc<ParallelShared>>> {
    if g::InterruptHoldoffCount() != 0 || g::CritSectionCount() != 0 {
        return Ok(None);
    }
    // Unported C arm (SerializeUncommittedEnums): the launched path raises
    // a clean ERROR; the statement task simply refuses — the incumbent
    // loop serves the statement. (In practice unreachable: uncommitted
    // enums co-occur with pending invalidations, which the arm's binder
    // policy gate already refused.)
    if pg_enum::HasUncommittedEnums() {
        return Ok(None);
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

    let encoded = QUERY_TASK_INSTALLED
        | u8::from(policy.has_params) * QUERY_TASK_PARAMS
        | u8::from(policy.temp_state) * QUERY_TASK_TEMP
        | u8::from(policy.serializable) * QUERY_TASK_SERIALIZABLE
        | u8::from(policy.pending_invalidations) * QUERY_TASK_PENDING_INVALS
        | u8::from(policy.invals_flush) * QUERY_TASK_INVALS_FLUSH;

    Ok(Some(Arc::new(ParallelShared {
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
        guc_pin: if guc::store::session_guc_bind_enabled() {
            Some(guc::layers::current_query_pin())
        } else {
            None
        },
        tstate,
        combocid: combocid::SerializeComboCIDState(),
        pending_syncs: catalog_storage::SerializePendingSyncs(),
        reindex: types_rel::reindex::serialize_reindex_state(),
        active_snapshot,
        transaction_snapshot,
        clientconninfo,
        relmap: relmapper::SerializeRelationMap(),
        record_registry: if typcache_seams::record_registry_handle::is_installed() {
            typcache_seams::record_registry_handle::call()
        } else {
            Default::default()
        },
        library_name: String::new(),
        function_name: String::new(),
        error_senders: Vec::new(),
        worker_attached: Vec::new(),
        private: Mutex::new(None),
        standing_driver: Mutex::new(None),
        query_task_binding: AtomicU8::new(encoded),
    })))
}

pub fn set_private(id: ParallelContextId, private: Arc<dyn Any + Send + Sync>) {
    let shared = shared_for(id);
    *shared.private.lock().unwrap_or_else(|e| e.into_inner()) = Some(private);
}

/// [`set_private`] for a pcxt-less shared ([`statement_task_shared`]).
pub fn set_private_shared(shared: &Arc<ParallelShared>, private: Arc<dyn Any + Send + Sync>) {
    *shared.private.lock().unwrap_or_else(|e| e.into_inner()) = Some(private);
}

/// GL-SIMPLEWEDGE-1: sever the engagement's back-edges at teardown. The
/// arm payload (`set_private*`) holds the shared by Arc and the shared's
/// `private` holds the payload back — a strong cycle that outlives the
/// engagement (C has no analog: the DSM segment dies at
/// DestroyParallelContext). Callers: DestroyParallelContext (after the
/// workers exited — C-exact lifetime) and the pcxt-less fast ceremony's
/// RAII join guard (its Destroy-equivalent). Idempotent.
pub fn clear_engagement_refs(shared: &Arc<ParallelShared>) {
    *shared.private.lock().unwrap_or_else(|e| e.into_inner()) = None;
    *shared
        .standing_driver
        .lock()
        .unwrap_or_else(|e| e.into_inner()) = None;
}

/// [`set_standing_driver`] for a pcxt-less shared ([`statement_task_shared`]).
pub fn set_standing_driver_shared(shared: &Arc<ParallelShared>, driver: standing::StandingDriver) {
    *shared
        .standing_driver
        .lock()
        .unwrap_or_else(|e| e.into_inner()) = Some(driver);
}

/// Install the standing-gang driver for this context's engagement (M2
/// inc-1 per-arm dispatch): `standing::try_engage` refuses an engagement
/// without one, and the gang's serve dispatches through it. Set after
/// InitializeParallelDSM, before the standing publish — the same window as
/// `set_private`.
pub fn set_standing_driver(id: ParallelContextId, driver: standing::StandingDriver) {
    let shared = shared_for(id);
    *shared
        .standing_driver
        .lock()
        .unwrap_or_else(|e| e.into_inner()) = Some(driver);
}

pub fn InstallQueryTaskBinding(
    id: ParallelContextId,
    policy: QueryTaskBindingPolicy,
) -> PgResult<()> {
    let shared = shared_for(id);
    let encoded = QUERY_TASK_INSTALLED
        | u8::from(policy.has_params) * QUERY_TASK_PARAMS
        | u8::from(policy.temp_state) * QUERY_TASK_TEMP
        | u8::from(policy.serializable) * QUERY_TASK_SERIALIZABLE
        | u8::from(policy.pending_invalidations) * QUERY_TASK_PENDING_INVALS
        | u8::from(policy.invals_flush) * QUERY_TASK_INVALS_FLUSH;
    shared
        .query_task_binding
        .compare_exchange(0, encoded, SeqCst, SeqCst)
        .map(|_| ())
        .map_err(|_| {
            Box::new(
                PgError::new(ERROR, "parallel query-task binding was installed twice")
                    .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            )
        })
}

pub fn with_query_task_binding<T>(
    shared: &Arc<ParallelShared>,
    body: impl FnOnce() -> PgResult<T>,
) -> PgResult<T> {
    query_task_guard::with_query_task_binding(shared, body)
}

/// The binder's SESSION-state policy inputs, probed from the same sources
/// `InitializeParallelDSM` serializes (temp namespace, serializable xact,
/// pending invalidations) — the M1 runtime-scan leader's fail-closed
/// admission reads this BEFORE creating a context: any set flag refuses
/// engagement, because `validate()` (query_task_guard.rs) would refuse the
/// bind on every helper anyway. `has_params` stays the caller's: params are
/// executor state the leader already knows.
pub fn query_task_policy_probe() -> QueryTaskBindingPolicy {
    let (temp_ns, temp_toast_ns) = catalog_namespace::GetTempNamespaceState();
    QueryTaskBindingPolicy {
        has_params: false,
        temp_state: temp_ns != InvalidOid || temp_toast_ns != InvalidOid,
        serializable: xact::IsolationUsesXactSnapshot()
            || predicate_seams::share_serializable_xact::call() != 0,
        pending_invalidations: inval::TransactionHasPendingInvalidationMessages(),
        invals_flush: false,
    }
}

/// One bounded leader latch wait + reset (the WaitForParallelWorkersToFinish
/// wait quantum, recheck-cadence-bounded): the M1 runtime-scan leader's
/// submit-and-park loop parks here between its completion/message/interrupt
/// re-polls. An Err is a RAISED cancel disposition (statement_timeout /
/// pg_cancel_backend delivered at the latch sleep, the thread model's signal
/// delivery point) — the caller must abort + drain its RG and propagate
/// (F1 chaos finding: swallowing it made every leader park loop
/// uncancellable).
pub fn wait_parallel_finish_quantum() -> PgResult<()> {
    wait_on_my_latch(WAIT_EVENT_PARALLEL_FINISH)
}

/// `wait_parallel_finish_quantum` with the sleep additionally bounded by
/// `max_ms`: the standing channels' PRE-CLAIM phase has no guaranteed wake
/// source (an absent or fully-mismatched worker set may never touch the
/// board), so the leader's park must not outlive the claim deadline its
/// loop enforces — an unbounded quantum turned the deadline into "one full
/// MQ-recheck period" (GL-ZSTALL-1). Same Err propagation contract as the
/// unbounded quantum (a raised cancel disposition surfaces from the latch
/// sleep and must abort the ceremony).
pub fn wait_parallel_finish_quantum_bounded(max_ms: i64) -> PgResult<()> {
    let latch = g::MyLatch().expect("parallel leader without MyLatch");
    let mut d = shm_mq::stall::StallDetector::new_capped(max_ms);
    shm_mq::stall::wait_latch_reporting(latch, WAIT_EVENT_PARALLEL_FINISH, &mut d, &mut |_| {})?;
    latch::ResetLatch(latch);
    Ok(())
}

/// True when every launched worker's underlying bgworker task has ENDED
/// (thread exited, died, or parked back to the pool). The M1 runtime-scan
/// leader's liveness probe: all stopped while the pinned RG is incomplete
/// means nobody will ever finish the submitted work (helpers died pre-hook
/// — e.g. an init-path panic-to-ERROR — leaves no channel message after
/// Terminate and no refusal count). During normal hook driving the tasks
/// are still BGWH_STARTED, so this cannot false-positive mid-drive.
pub fn parallel_workers_all_stopped(id: ParallelContextId) -> bool {
    let n = with_pcxt(id, |p| p.workers.len());
    for i in 0..n {
        let handle = with_pcxt(id, |p| p.workers.get(i).and_then(|w| w.bgwhandle));
        let Some(handle) = handle else { continue };
        match bgworker::GetBackgroundWorkerPid(&handle).0 {
            bgworker::BgwHandleStatus::BGWH_STOPPED
            | bgworker::BgwHandleStatus::BGWH_POSTMASTER_DIED => {}
            _ => return false,
        }
    }
    true
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
            let (tx, rx) = pgsync::mailbox(Some(PARALLEL_ERROR_QUEUE_MSGS));
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
        wait_on_my_latch(WAIT_EVENT_BGWORKER_STARTUP)?;
    }
}

const PG_WAIT_IPC: u32 = 0x0800_0000;
const WAIT_EVENT_BGWORKER_STARTUP: u32 = PG_WAIT_IPC + 6;
// PARALLEL_FINISH's index in wait_event_names.txt's IPC section. It is 40:
// index 32 is LOGICAL_SYNC_STATE_CHANGE, and every leader parked here used to
// publish itself in pg_stat_activity as a logical-replication sync wait
// (GL-SYNCWEDGE-1). scripts/lint-waitevent-tags.sh pins the whole family.
pub const WAIT_EVENT_PARALLEL_FINISH: u32 = PG_WAIT_IPC + 40;

/// One LOG line describing why this leader is still parked: the same evidence
/// the Gather leader's report carries, for the parallel-finish/attach loops.
/// The 12m53s production wedge (GL-SYNCWEDGE-1) produced none of this.
fn report_park_stall(wait_event: u32, waited_ms: i64) {
    let mut msg = format!(
        "parallel leader park stall self-report: wait_event={} waited_ms={waited_ms} \
         my_pid={} my_procno={}",
        if wait_event == WAIT_EVENT_PARALLEL_FINISH { "ParallelFinish" } else { "BgworkerStartup" },
        g::MyProcPid(),
        g::MyProcNumber(),
    );
    // PCXT_LIST is never borrowed across a park (every caller drops the
    // borrow before waiting), so this walk is safe here.
    PCXT_LIST.with(|l| {
        for p in l.borrow().iter() {
            msg.push_str(&format!(
                " pcxt[{}]={{launched={} known_attached={}",
                p.id, p.nworkers_launched, p.nknown_attached_workers
            ));
            for (i, w) in p.workers.iter().enumerate() {
                let status = match w.bgwhandle {
                    Some(h) => format!("{:?}", bgworker::GetBackgroundWorkerPid(&h).0),
                    None => "no-handle".to_string(),
                };
                let attached = p
                    .shared
                    .as_ref()
                    .map(|s| s.worker_attached[i].load(SeqCst))
                    .unwrap_or(false);
                msg.push_str(&format!(
                    " w[{i}]={{live_queue={} attached={attached} bgw={status}}}",
                    w.error_receiver.is_some()
                ));
            }
            msg.push('}');
        }
    });
    shm_mq::stall::log_stall_report(msg);
}

fn wait_on_my_latch(wait_event: u32) -> PgResult<()> {
    let latch = g::MyLatch().expect("parallel leader without MyLatch");
    // Both callers are recheck loops (worker attach/finish state), and the
    // wakes they rely on are the same cross-thread SetLatch delivery the
    // shm_mq stall class loses intermittently — bound the sleep with the
    // shared recheck cadence so a lost wake costs one period, not forever
    // (shm_mq stall.rs rationale; a timeout return is a legal spurious wake
    // because the caller re-polls before re-blocking).
    //
    // PROPAGATE the wait result (F1 chaos finding, defect layer 2b): in the
    // thread model the latch sleep is the signal delivery point — WaitLatch
    // runs drain_thread_signals(), so a raised statement-timeout/cancel
    // disposition surfaces HERE as an Err. Discarding it (`let _ =`)
    // CONSUMED the one-shot cancel and threw it away; the subsequent
    // check_for_interrupts saw nothing and every arm's leader park loop
    // became uncancellable. On Err the latch is deliberately NOT reset: the
    // raise aborts the ceremony, and a leftover set latch only costs one
    // spurious wake on the next wait.
    //
    // GL-SYNCWEDGE-1: the clock is kept ACROSS quanta and the report closure
    // actually says something. Building a fresh StallDetector here re-armed
    // its 60 s deadline once per 1000 ms recheck, so the deadline could never
    // mature; the closure was `|_| {}`, so nothing would have been printed if
    // it had. Both holes together are why a 12m53s production park left zero
    // log evidence.
    let mut clock = PARK_STALL
        .with(|c| c.borrow_mut().take())
        .unwrap_or_else(shm_mq::stall::ParkStallClock::new);
    clock.enter(shm_mq::stall::park_now_ms());
    let waited = shm_mq::stall::wait_latch_reporting(
        latch,
        wait_event,
        clock.detector(),
        &mut |waited_ms| report_park_stall(wait_event, waited_ms),
    );
    PARK_STALL.with(|c| *c.borrow_mut() = Some(clock));
    waited?;
    latch::ResetLatch(latch);
    Ok(())
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

        wait_on_my_latch(WAIT_EVENT_PARALLEL_FINISH)?;
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

    // Release parked helpers BEFORE waiting for worker exit below. Every
    // registered hook runs; each no-ops on foreign payload types.
    if let Some(p) = pcxt.shared.as_ref().and_then(|s| s.private()) {
        for f in private_shutdown_hooks() {
            f(&*p);
        }
    }

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
    // GL-SIMPLEWEDGE-1: the arm payload stored by set_private holds this
    // shared by Arc while `private` holds the payload back — sever the
    // cycle now that every worker has exited (C-exact lifetime: the DSM
    // segment dies at DestroyParallelContext).
    if let Some(shared) = pcxt.shared.as_ref() {
        clear_engagement_refs(shared);
    }
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
                    Some(TryRecv::Msg(m)) => {
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
                    Some(TryRecv::Empty) => break,
                    Some(TryRecv::Disconnected) => {
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
    static MY_PROGRESS_SENDER: RefCell<Option<MailboxSender<WorkerMessage>>> =
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

fn take_my_error_sender(shared: &ParallelShared, worker_number: i32) -> MailboxSender<WorkerMessage> {
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
    // messages; plain ERROR is forwarded exactly once from the Err return
    // below (errfinish never emits it toward the client). FATAL+ IS emitted
    // at the raise point and proc_exits without returning through the body,
    // so it is forwarded here — the worker's dying words; the leader's
    // ProcessParallelMessages downgrades it to ERROR (C
    // HandleParallelMessages parity).
    let notice_sender = sender.clone();
    let (leader_pid, leader_proc) =
        (shared.parallel_leader_pid, shared.parallel_leader_proc_number);
    let prev_redirect = elog::set_frontend_redirect(Some(Box::new(move |e: &PgError| {
        if e.level == ERROR {
            return;
        }
        let msg = if e.level > ERROR {
            WorkerMessage::Error(Box::new(e.clone()))
        } else {
            WorkerMessage::Notice(Box::new(e.clone()))
        };
        let _ = notice_sender.send(msg);
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
        // A FATAL inside the body proc_exits; converting its unwind to an
        // ERROR would resurrect the exit this thread is committed to (the
        // deferred callback drain runs at the thread top). The FATAL report
        // already reached the leader through the redirect above; tell the
        // leader we are gone and keep unwinding.
        Err(payload)
            if payload.is::<ipc::ProcExitThread>()
                || payload.is::<types_error::PanicExitThread>() =>
        {
            MY_PROGRESS_SENDER.with(|c| *c.borrow_mut() = None);
            drop(sender);
            procsignal::SendProcSignal(
                shared.parallel_leader_pid,
                types_storage::storage::ProcSignalReason::PROCSIG_PARALLEL_MESSAGE,
                shared.parallel_leader_proc_number,
            );
            MY_WORKER_SHARED.with(|s| *s.borrow_mut() = None);
            std::panic::resume_unwind(payload);
        }
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
    // Successful tasks may park on the query's ready work instead of
    // returning to the pool at once; the hook returns when the leader
    // releases it (DestroyParallelContext at the latest). A hook panic must
    // not corrupt the already-sent outcome.
    if outcome.is_ok() {
        for f in post_task_park_hooks() {
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f(&shared)));
        }
    }
    outcome
}

/// Test-only fault injection (default-off, dead unless the env var is set at
/// server start): `PGRUST_TEST_WARM_WINDOW_FAIL=<n>` fails the first `n`
/// WARM-claimed (wretain) pooled tasks at the top of the worker body — the
/// exact geometry of the organic pre-connect failures (leader destroyed the
/// parallel context before this worker attached: "could not map dynamic
/// shared memory segment" / lock-group join refusal) that exposed the
/// sinval-slot leak window between ReattachRetainedProc and the sinval
/// reattach. scripts/sinval-slot-e2e.sh is the standing battery over this
/// knob: pre-fix, each injected failure leaked its retained sinval slot
/// (procno freed by ProcKill, slot procPid still set) and poisoned every
/// later claimant of the procno.
fn test_warm_window_fail() -> PgResult<()> {
    use std::sync::atomic::AtomicUsize;
    static BUDGET: std::sync::OnceLock<Option<AtomicUsize>> = std::sync::OnceLock::new();
    let Some(budget) = BUDGET.get_or_init(|| {
        std::env::var("PGRUST_TEST_WARM_WINDOW_FAIL")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .filter(|n| *n > 0)
            .map(AtomicUsize::new)
    }) else {
        return Ok(());
    };
    if !init_small::wretain::warm_claim() {
        return Ok(());
    }
    if budget
        .fetch_update(SeqCst, SeqCst, |n| n.checked_sub(1))
        .is_ok()
    {
        return ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg("pgrust: test warm-window fail injection")
            .finish(loc(0, "test_warm_window_fail"));
    }
    Ok(())
}

fn parallel_worker_body(shared: &Arc<ParallelShared>, _worker_number: i32) -> PgResult<()> {
    test_warm_window_fail()?;
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
    if typcache_seams::install_record_registry::is_installed() {
        typcache_seams::install_record_registry::call(std::sync::Arc::clone(
            &shared.record_registry,
        ));
    }

    let asnapshot = snapmgr::RestoreSnapshot(&shared.active_snapshot);
    let tsource = shared.transaction_snapshot.as_ref().unwrap_or(&shared.active_snapshot);
    snapmgr::RestoreTransactionSnapshot(tsource, shared.parallel_leader_proc_number)?;
    snapmgr::PushActiveSnapshot(&asnapshot)?;

    if init_small::wretain::warm_claim()
        && !shared.leader_pending_invals
        && !init_small::wretain::caches_tainted()
    {
        // Retention warm claim: caches were drained against the shared queue
        // at InitPostgres (postinit warm arm); a second cheap drain here
        // covers messages that arrived since. C's blanket invalidation is
        // only needed for a fresh process's incidentally-mistimed cache
        // loads — or for the leader's own uncommitted DDL, which forces the
        // fallback arm below (both for THIS task's binding and, via the
        // taint, for a PREVIOUS task's: see note_caches_tainted below).
        gtrace("w.retain.inval.begin");
        inval::local::AcceptInvalidationMessages()?;
        gtrace("w.retain.inval.drained");
    } else {
        gtrace("w.cold.inval.begin");
        inval::local::InvalidateSystemCaches()?;
        init_small::wretain::clear_caches_taint();
    }
    gtrace("w.inval.done");
    if shared.leader_pending_invals {
        // This task binds a transaction with unbroadcast (uncommitted-DDL)
        // invalidation messages: cache entries built during it hold
        // uncommitted catalog state. If that transaction ABORTS, no sinval
        // traffic ever corrects them — C's per-query worker process dies
        // here, and parallel.c:1513 blankets every fresh start — so a
        // retained thread must re-run the blanket at its next claim instead
        // of trusting the drain. Without this, a rolled-back TRUNCATE left a
        // parked worker's relcache pointing at the aborted relfilelocator,
        // tripping table_beginscan_parallel's locator assert (tableam.c:172
        // parity) on the next parallel scan of that table.
        init_small::wretain::note_caches_tainted();
    }

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
    let _guc_binding = if let Some(pin) = shared.guc_pin.as_ref() {
        // Pin bind: leader-validated values + extras, assign hooks fire,
        // check hooks don't; also adopts the leader's base (started-with
        // parity across the whole parallel query).
        Some(guc::layers::bind_query_pin(pin)?)
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
    // C: InitializeSystemUser once MyClientConnectionInfo is restored (only
    // when authn_id is set, so auth_method is valid).
    let (authn_id, auth_method) = miscinit::client_connection_info();
    if let Some(authn_id) = authn_id {
        miscinit::InitializeSystemUser(authn_id, hba_seams::hba_authname::call(auth_method));
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
    parallel_seams::parallel_worker_number::set(ParallelWorkerNumber);
    parallel_seams::initializing_parallel_worker::set(InitializingParallelWorker);
    parallel_seams::at_eoxact_parallel::set(AtEOXact_Parallel);
    parallel_seams::at_eosubxact_parallel::set(AtEOSubXact_Parallel);
    parallel_seams::parallel_worker_report_last_rec_end::set(ParallelWorkerReportLastRecEnd);
    parallel_seams::handle_parallel_message_interrupt::set(HandleParallelMessageInterrupt);
    parallel_seams::parallel_worker_report_progress::set(parallel_worker_report_progress);
    parallel_seams::process_parallel_messages::set(ProcessParallelMessages);
}
