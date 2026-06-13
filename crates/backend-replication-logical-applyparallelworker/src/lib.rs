//! `replication/logical/applyparallelworker.c` — the *coordinator* for logical
//! replication **parallel apply**.
//!
//! Launches, pools, and tears down parallel apply workers; sets up the DSM
//! segment carrying the leader→worker change queue, the worker→leader error
//! queue, and the shared `ParallelApplyWorkerShared` header; runs the
//! parallel-apply worker main loop; and sequences the lmgr **session locks** (a
//! *stream* lock and a *transaction* lock) that let the lock manager detect
//! deadlocks between the leader apply (LA) worker and the parallel apply (PA)
//! workers. See the long comment atop the C file for the deadlock analysis.
//!
//! # The leader/worker process boundary
//!
//! `ParallelApplyWorkerShared` is owned by **this** file in C (`MyParallelShared`
//! is defined here, C line 239); it is a fixed header (a spinlock plus scalars
//! and a `FileSet`) that merely *lives in* the DSM segment so the leader and the
//! parallel apply worker can both reach it. The spinlock-protected field
//! accessors — the commit-ordering / deadlock-detection state machine — are this
//! file's native logic, so the header is modeled here as a real synchronized
//! type ([`ParallelApplyWorkerShared`]) with an in-crate [`Mutex`] (the C
//! `slock_t mutex`) and an [`AtomicU32`] (`pending_stream_count`), shared across
//! the leader/worker threads via [`Arc`]. The two `shm_mq`s and the
//! `dsm_segment` are still owned by the DSM/shm_mq subsystems and are carried as
//! opaque `u64` handles the worker seams resolve. The worker **pool** is an
//! owned `Vec<ParallelApplyWorkerInfo>` and workers are addressed by **index** (a
//! [`WorkerHandle`]) rather than by raw pointer, mirroring C's
//! List-of-pointers + HTAB-of-pointers model.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU32, Ordering::SeqCst};
use std::sync::{Arc, Mutex};

use backend_utils_error::{ereport, PgError, PgResult};
use types_applyparallel::{
    DsmSetupResult, FileSet, ParallelTransState, PartialFileSetState, ShmMqResult,
};
use types_core::{
    InvalidTransactionId, InvalidXLogRecPtr, Oid, Size, TimestampTz, TransactionId, XLogRecPtr,
    INVALID_PROC_NUMBER,
};
use types_error::{ErrorLocation, DEBUG1, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, LOG};
use types_guc::PGC_SIGHUP;
use types_pgstat::wait_event::{
    WAIT_EVENT_LOGICAL_APPLY_SEND_DATA, WAIT_EVENT_LOGICAL_PARALLEL_APPLY_MAIN,
    WAIT_EVENT_LOGICAL_PARALLEL_APPLY_STATE_CHANGE,
};
use types_storage::lock::{AccessExclusiveLock, AccessShareLock, LOCKMODE};
use types_storage::ProcSignalReason;

use backend_replication_logical_worker_seams as worker;

pub use backend_storage_ipc_procsignal::SendProcSignal;

// ---------------------------------------------------------------------------
// File-private constants (match the C #defines exactly).
// ---------------------------------------------------------------------------

/// `PG_LOGICAL_APPLY_SHM_MAGIC` (C line 175).
pub const PG_LOGICAL_APPLY_SHM_MAGIC: u32 = 0x787c_a067;

/// DSM TOC keys (C lines 182-184).
pub const PARALLEL_APPLY_KEY_SHARED: u64 = 1;
pub const PARALLEL_APPLY_KEY_MQ: u64 = 2;
pub const PARALLEL_APPLY_KEY_ERROR_QUEUE: u64 = 3;

/// Queue size of DSM, 16 MB (C line 187).
pub const DSM_QUEUE_SIZE: Size = 16 * 1024 * 1024;

/// Error queue size of DSM, 16 KB (C line 195).
pub const DSM_ERROR_QUEUE_SIZE: Size = 16 * 1024;

/// `SIZE_STATS_MESSAGE` = 2 * sizeof(XLogRecPtr) + sizeof(TimestampTz) (C line 203).
pub const SIZE_STATS_MESSAGE: usize =
    2 * core::mem::size_of::<XLogRecPtr>() + core::mem::size_of::<TimestampTz>();

/// Session-lock object ids for `locktag_field4` (C lines 209-210).
pub const PARALLEL_APPLY_LOCK_STREAM: u16 = 0;
pub const PARALLEL_APPLY_LOCK_XACT: u16 = 1;

/// `pa_send_data` retry/timeout (C lines 1175-1176).
pub const SHM_SEND_RETRY_INTERVAL_MS: i64 = 1000;
pub const SHM_SEND_TIMEOUT_MS: i32 = 10000 - SHM_SEND_RETRY_INTERVAL_MS as i32;

/// Wake-event flags for `WaitLatch` (`storage/latch.h`).
const WL_LATCH_SET: u32 = 1 << 0;
const WL_TIMEOUT: u32 = 1 << 3;
const WL_EXIT_ON_PM_DEATH: u32 = 1 << 5;

/// `NAMEDATALEN` — the savepoint-name buffer size (`char spname[NAMEDATALEN]`).
const NAMEDATALEN: Size = 64;

// ---------------------------------------------------------------------------
// `ParallelApplyWorkerShared` (worker_internal.h lines 135-180) — owned here.
// ---------------------------------------------------------------------------

/// The fields of [`ParallelApplyWorkerShared`] protected by `slock_t mutex`.
/// (`pending_stream_count` is independently atomic and lives outside the lock,
/// exactly as in C.)
#[derive(Debug)]
struct SharedLocked {
    /// `TransactionId xid`.
    xid: TransactionId,
    /// `ParallelTransState xact_state` — the commit-ordering state.
    xact_state: ParallelTransState,
    /// `uint16 logicalrep_worker_generation`.
    logicalrep_worker_generation: u16,
    /// `int logicalrep_worker_slot_no`.
    logicalrep_worker_slot_no: i32,
    /// `XLogRecPtr last_commit_end`.
    last_commit_end: XLogRecPtr,
    /// `PartialFileSetState fileset_state`.
    fileset_state: PartialFileSetState,
    /// `FileSet fileset`.
    fileset: FileSet,
}

/// `ParallelApplyWorkerShared` (`worker_internal.h`). This file owns the struct
/// (`MyParallelShared` is defined in applyparallelworker.c); the header lives in
/// the DSM segment in C only so both the leader and the parallel apply worker
/// reach it. Here the `slock_t mutex` is a host [`Mutex`] guarding exactly the
/// fields C documents it as protecting; `pending_stream_count` is the
/// independent `pg_atomic_uint32`. The handle is shared across the
/// leader/worker threads via [`Arc`].
#[derive(Debug)]
pub struct ParallelApplyWorkerShared {
    /// `slock_t mutex` + the fields it protects.
    locked: Mutex<SharedLocked>,
    /// `pg_atomic_uint32 pending_stream_count`.
    pending_stream_count: AtomicU32,
}

impl ParallelApplyWorkerShared {
    /// A fresh header (`palloc0`-equivalent: zero/initial values).
    fn new() -> Self {
        ParallelApplyWorkerShared {
            locked: Mutex::new(SharedLocked {
                xid: InvalidTransactionId,
                xact_state: ParallelTransState::PARALLEL_TRANS_UNKNOWN,
                logicalrep_worker_generation: 0,
                logicalrep_worker_slot_no: 0,
                last_commit_end: InvalidXLogRecPtr,
                fileset_state: PartialFileSetState::FS_EMPTY,
                fileset: FileSet::default(),
            }),
            pending_stream_count: AtomicU32::new(0),
        }
    }
}

/// An opaque token the worker seam uses to carry a shared-header [`Arc`] across
/// the DSM TOC: the leader registers an `Arc<ParallelApplyWorkerShared>` under a
/// token in [`Globals::shared_registry`] before `setup_dsm`; the worker recovers
/// the same token from `worker_attach_dsm` and binds `MyParallelShared`.
type SharedToken = u64;

// ---------------------------------------------------------------------------
// `ParallelApplyWorkerInfo` (worker_internal.h lines 188-218).
// ---------------------------------------------------------------------------

/// A pool slot's identity. In C this is a `ParallelApplyWorkerInfo *`; here it
/// is the owned index of the entry in [`Globals::pool`]. Stable for the entry's
/// lifetime (the pool never compacts a still-live entry; `pa_free_worker_info`
/// blanks the slot, so cached/hashed handles stay valid until the caller drops
/// them).
pub type WorkerHandle = usize;

/// `ParallelApplyWorkerInfo` (worker_internal.h lines 188-218). The `shm_mq`
/// handles and the `dsm_segment` live in / point into the DSM segment and are
/// owned by the worker seams, so they are carried here as opaque `u64` handle
/// slots; the `shared` header is owned in-crate ([`Arc`]); the
/// `serialize_changes`/`in_use` booleans and the pool policy are this file's own
/// state.
#[derive(Clone, Debug, Default)]
pub struct ParallelApplyWorkerInfo {
    /// Opaque handle to `winfo->mq_handle` (leader→worker change queue), or 0.
    pub mq_handle: u64,
    /// Opaque handle to `winfo->error_mq_handle` (worker→leader error queue), or 0.
    pub error_mq_handle: u64,
    /// Opaque handle to `winfo->dsm_seg`, or 0.
    pub dsm_seg: u64,
    /// `winfo->serialize_changes`.
    pub serialize_changes: bool,
    /// `winfo->in_use`.
    pub in_use: bool,
    /// `winfo->shared` (`ParallelApplyWorkerShared *`) — the in-crate header,
    /// shared with the worker thread via [`Arc`]. `None` until `pa_setup_dsm`.
    pub shared: Option<Arc<ParallelApplyWorkerShared>>,
}

/// `ParallelApplyWorkerEntry` (C lines 215-219): xid → winfo hash entry. The C
/// `winfo` pointer is the pool index here.
#[derive(Clone, Copy, Debug)]
struct ParallelApplyWorkerEntry {
    /// `ParallelApplyWorkerInfo *winfo` — the pool slot index.
    winfo: WorkerHandle,
}

// ---------------------------------------------------------------------------
// Per-backend state (mirrors the C file-scope globals). A logical-replication
// apply backend maps to one thread; per AGENTS.md these per-backend C globals
// are `thread_local!`.
// ---------------------------------------------------------------------------

struct Globals {
    /// `ParallelApplyTxnHash` (NULL until first use): xid → winfo index.
    txn_hash: Option<BTreeMap<TransactionId, ParallelApplyWorkerEntry>>,
    /// `ParallelApplyWorkerPool` (NIL): owning storage for the winfo entries.
    /// Slots are `None` once removed (`list_delete_ptr`) so live handles stay
    /// stable.
    pool: Vec<Option<ParallelApplyWorkerInfo>>,
    /// `stream_apply_worker` cache (NULL) — a pool index when set.
    stream_apply_worker: Option<WorkerHandle>,
    /// `ParallelApplyWorkerShared *MyParallelShared` (C line 239) — set on the
    /// worker side by `ParallelApplyWorkerMain` after attaching to the segment.
    my_parallel_shared: Option<Arc<ParallelApplyWorkerShared>>,
    /// Maps a [`SharedToken`] to the leader-created `Arc` so the worker can
    /// recover the same header from `worker_attach_dsm`. The next token to hand
    /// out is `next_shared_token`.
    shared_registry: BTreeMap<SharedToken, Arc<ParallelApplyWorkerShared>>,
    next_shared_token: SharedToken,
    /// `volatile sig_atomic_t ParallelApplyMessagePending` (C line 245).
    parallel_apply_message_pending: bool,
    /// `static List *subxactlist = NIL;` (C line 255) — a list of subtransaction
    /// xids maintained by this file. In C it is allocated in
    /// `TopTransactionContext` and freed at transaction end; here it is a plain
    /// `Vec<TransactionId>` reset by `pa_reset_subtrans`.
    subxactlist: Vec<TransactionId>,
}

impl Globals {
    fn seed() -> Self {
        Globals {
            txn_hash: None,
            pool: Vec::new(),
            stream_apply_worker: None,
            my_parallel_shared: None,
            shared_registry: BTreeMap::new(),
            next_shared_token: 1,
            parallel_apply_message_pending: false,
            subxactlist: Vec::new(),
        }
    }
}

thread_local! {
    static GLOBALS: RefCell<Globals> = RefCell::new(Globals::seed());
}

#[inline]
fn with_globals<R>(f: impl FnOnce(&mut Globals) -> R) -> R {
    GLOBALS.with(|g| f(&mut g.borrow_mut()))
}

/// `winfo->serialize_changes` — read through the pool handle. Loud-panics on a
/// freed/never-allocated handle (C would deref a dangling `winfo`).
pub fn pa_winfo_serialize_changes(winfo: WorkerHandle) -> bool {
    with_globals(|g| {
        g.pool
            .get(winfo)
            .and_then(|s| s.as_ref())
            .unwrap_or_else(|| panic!("pa_winfo_serialize_changes: stale winfo handle {winfo}"))
            .serialize_changes
    })
}

/// Read `ParallelApplyMessagePending`.
pub fn parallel_apply_message_pending() -> bool {
    with_globals(|g| g.parallel_apply_message_pending)
}

fn set_parallel_apply_message_pending(v: bool) {
    with_globals(|g| g.parallel_apply_message_pending = v);
}

/// Whether `MyParallelShared` has been set (worker side).
pub fn my_parallel_shared_is_set() -> bool {
    with_globals(|g| g.my_parallel_shared.is_some())
}

/// `MyParallelShared` — the worker-side shared header. Panics if read before
/// `ParallelApplyWorkerMain` bound it (the C `MyParallelShared` NULL deref).
fn my_parallel_shared() -> Arc<ParallelApplyWorkerShared> {
    with_globals(|g| g.my_parallel_shared.clone())
        .expect("MyParallelShared dereferenced before ParallelApplyWorkerMain attached")
}

// ---------------------------------------------------------------------------
// `ParallelApplyWorkerShared` accessors — the spinlock-protected state machine
// this file owns (C 1313-1335, 1504-1536) plus the `pending_stream_count`
// atomics (C 1606/1614). All field access goes through the in-crate `Mutex`
// (the `slock_t mutex`) so the leader and worker threads stay coordinated.
// ---------------------------------------------------------------------------

/// `winfo->shared->xid` (read under the mutex).
fn shared_xid(shared: &Arc<ParallelApplyWorkerShared>) -> TransactionId {
    shared.locked.lock().unwrap().xid
}

/// `winfo->shared->last_commit_end` (read under the mutex).
fn shared_last_commit_end(shared: &Arc<ParallelApplyWorkerShared>) -> XLogRecPtr {
    shared.locked.lock().unwrap().last_commit_end
}

// Helpers mirroring the trivial C inline checks.

#[inline]
fn TransactionIdIsValid(xid: TransactionId) -> bool {
    xid != InvalidTransactionId
}

#[inline]
fn XLogRecPtrIsInvalid(p: XLogRecPtr) -> bool {
    p == InvalidXLogRecPtr
}

// ===========================================================================
// 1. pa_can_start (C 264-316)
// ===========================================================================

/// `static bool pa_can_start(void)` — returns true if it is OK to start a
/// parallel apply worker.
fn pa_can_start() -> PgResult<bool> {
    /* Only leader apply workers can start parallel apply workers. */
    if !worker::am_leader_apply_worker::call()? {
        return Ok(false);
    }

    /*
     * Check for any change in the subscription parameter so the latest values
     * get used for the checks below.
     */
    worker::maybe_reread_subscription::call()?;

    /*
     * Don't start a new parallel apply worker if the subscription is not using
     * parallel streaming mode, or if the publisher does not support parallel
     * apply.
     */
    if !worker::my_worker_parallel_apply::call() {
        return Ok(false);
    }

    /*
     * Don't start a new parallel worker if user has set skiplsn as it's
     * possible that they want to skip the streaming transaction.
     */
    if !XLogRecPtrIsInvalid(worker::my_subscription_skiplsn::call()) {
        return Ok(false);
    }

    /*
     * For streaming transactions being applied using a parallel apply worker,
     * we cannot decide whether to apply the change for a relation that is not
     * in the READY state, so don't start the new parallel apply worker.
     */
    if !backend_replication_logical_tablesync_seams::all_tablesyncs_ready::call()? {
        return Ok(false);
    }

    Ok(true)
}

// ===========================================================================
// 2. pa_setup_dsm (C 326-397)
// ===========================================================================

/// `static bool pa_setup_dsm(ParallelApplyWorkerInfo *winfo)`.
///
/// The DSM sizing / TOC build / queue creation / endpoint attach are owned by
/// the DSM/shm_toc/shm_mq machinery, so the worker seam performs them in one
/// call; the **shared header** is owned here, so this function `palloc0`s the
/// `ParallelApplyWorkerShared` (C 357-359 `shm_toc_allocate` + the field inits
/// at C 360-372), registers it under a token the worker recovers on attach, and
/// hands the token to `setup_dsm` to place in the TOC. On success it writes the
/// `winfo->{dsm_seg,shared,mq_handle,error_mq_handle}` assignments and returns
/// true; C returns false when `dsm_create` failed.
fn pa_setup_dsm(winfo_index: WorkerHandle, winfo: &mut ParallelApplyWorkerInfo) -> PgResult<bool> {
    /* Create the shared header (palloc0 + SpinLockInit + field inits). */
    let shared = Arc::new(ParallelApplyWorkerShared::new());
    let token = with_globals(|g| {
        let token = g.next_shared_token;
        g.next_shared_token += 1;
        g.shared_registry.insert(token, Arc::clone(&shared));
        token
    });

    match worker::setup_dsm::call(winfo_index as u64, token)? {
        None => {
            /* dsm_create failed: drop the just-registered header. */
            with_globals(|g| {
                g.shared_registry.remove(&token);
            });
            Ok(false)
        }
        Some(DsmSetupResult {
            dsm_seg,
            mq_handle,
            error_mq_handle,
        }) => {
            winfo.dsm_seg = dsm_seg;
            winfo.shared = Some(shared);
            winfo.mq_handle = mq_handle;
            winfo.error_mq_handle = error_mq_handle;
            Ok(true)
        }
    }
}

// ===========================================================================
// 3. pa_launch_parallel_worker (C 403-459)
// ===========================================================================

/// `static ParallelApplyWorkerInfo *pa_launch_parallel_worker(void)`.
///
/// Returns the pool index of the chosen/created winfo, or `None`.
fn pa_launch_parallel_worker() -> PgResult<Option<WorkerHandle>> {
    /* Try to get an available parallel apply worker from the worker pool. */
    if let Some(idx) = with_globals(|g| {
        for (i, slot) in g.pool.iter().enumerate() {
            if let Some(winfo) = slot {
                if !winfo.in_use {
                    return Some(i);
                }
            }
        }
        None
    }) {
        return Ok(Some(idx));
    }

    /*
     * Start a new parallel apply worker.
     *
     * The worker info can be used for the lifetime of the worker process, so
     * create it in a permanent context (ApplyContext) — modeled here by a
     * permanent pool slot.
     */

    /* palloc0(sizeof(ParallelApplyWorkerInfo)) — install a fresh pool slot. */
    let winfo_index = with_globals(|g| {
        let idx = g.pool.len();
        g.pool.push(Some(ParallelApplyWorkerInfo::default()));
        idx
    });

    let mut winfo = with_globals(|g| g.pool[winfo_index].clone())
        .expect("pa_launch_parallel_worker: just-installed pool slot must be present");

    /* Setup shared memory. */
    let ok = match pa_setup_dsm(winfo_index, &mut winfo) {
        Ok(ok) => ok,
        Err(e) => {
            /* pfree(winfo) — drop the just-installed slot. */
            with_globals(|g| g.pool[winfo_index] = None);
            return Err(e);
        }
    };
    if !ok {
        /* pfree(winfo) — drop the just-installed slot. */
        with_globals(|g| g.pool[winfo_index] = None);
        return Ok(None);
    }
    let dsm_seg = winfo.dsm_seg;
    /* Persist the handles written by pa_setup_dsm. */
    with_globals(|g| g.pool[winfo_index] = Some(winfo));

    let handle = worker::dsm_segment_handle::call(dsm_seg)?;

    let launched = worker::logicalrep_worker_launch_parallel_apply::call(
        worker::my_worker_dbid::call(),
        worker::my_subscription_oid::call(),
        &worker::my_subscription_name::call(),
        worker::my_worker_userid::call(),
        handle,
    )?;

    let result;
    if launched {
        /*
         * ParallelApplyWorkerPool = lappend(ParallelApplyWorkerPool, winfo);
         * The slot is already in the pool; it is now a permanent pool member.
         */
        result = Some(winfo_index);
    } else {
        /* pa_free_worker_info(winfo); winfo = NULL; */
        pa_free_worker_info(winfo_index)?;
        result = None;
    }

    Ok(result)
}

// ===========================================================================
// 4. pa_allocate_worker (C 469-512)
// ===========================================================================

/// `void pa_allocate_worker(TransactionId xid)`.
pub fn pa_allocate_worker(xid: TransactionId) -> PgResult<()> {
    if !pa_can_start()? {
        return Ok(());
    }

    let winfo_index = match pa_launch_parallel_worker()? {
        Some(i) => i,
        None => return Ok(()),
    };

    /* First time through, initialize parallel apply worker state hashtable. */
    with_globals(|g| {
        if g.txn_hash.is_none() {
            g.txn_hash = Some(BTreeMap::new());
        }

        /* Create an entry for the requested transaction. */
        let hash = g.txn_hash.as_mut().unwrap();
        if hash.contains_key(&xid) {
            /* HASH_ENTER + found => elog(ERROR, "hash table corrupted") */
            return Err(elog_error("hash table corrupted"));
        }
        hash.insert(xid, ParallelApplyWorkerEntry { winfo: winfo_index });
        Ok(())
    })?;

    /* Update the transaction information in shared memory. (C 504-507) */
    let shared = winfo_shared_or_panic(winfo_index);
    {
        /* SpinLockAcquire(&winfo->shared->mutex); ... SpinLockRelease(...) */
        let mut s = shared.locked.lock().unwrap();
        s.xact_state = ParallelTransState::PARALLEL_TRANS_UNKNOWN;
        s.xid = xid;
    }

    with_globals(|g| {
        let winfo = g.pool[winfo_index].as_mut().unwrap();
        winfo.in_use = true;
        winfo.serialize_changes = false;
    });

    Ok(())
}

// ===========================================================================
// 5. pa_find_worker (C 517-543)
// ===========================================================================

/// `ParallelApplyWorkerInfo *pa_find_worker(TransactionId xid)`.
pub fn pa_find_worker(xid: TransactionId) -> Option<WorkerHandle> {
    if !TransactionIdIsValid(xid) {
        return None;
    }

    with_globals(|g| {
        if g.txn_hash.is_none() {
            return None;
        }

        /* Return the cached parallel apply worker if valid. */
        if let Some(idx) = g.stream_apply_worker {
            return Some(idx);
        }

        /* Find an entry for the requested transaction. */
        if let Some(entry) = g.txn_hash.as_ref().unwrap().get(&xid) {
            let winfo = entry.winfo;
            /* The worker must not have exited. (Assert(entry->winfo->in_use)) */
            debug_assert!(g.pool[winfo].as_ref().map(|w| w.in_use).unwrap_or(false));
            return Some(winfo);
        }

        None
    })
}

// ===========================================================================
// 6. pa_free_worker (C 555-588)
// ===========================================================================

/// `static void pa_free_worker(ParallelApplyWorkerInfo *winfo)`.
fn pa_free_worker(winfo_index: WorkerHandle) -> PgResult<()> {
    debug_assert!(!worker::am_parallel_apply_worker::call());

    let (shared, serialize_changes) = {
        let w = winfo_or_err(winfo_index, "pa_free_worker")?;
        (
            w.shared.expect("pa_free_worker: winfo->shared is NULL"),
            w.serialize_changes,
        )
    };

    debug_assert!(with_globals(|g| g.pool[winfo_index].as_ref().unwrap().in_use));
    debug_assert_eq!(
        pa_get_xact_state(&shared),
        ParallelTransState::PARALLEL_TRANS_FINISHED
    );

    let xid = shared_xid(&shared);

    /* hash_search(..., HASH_REMOVE, NULL) — error if not found. */
    with_globals(|g| {
        let removed = g.txn_hash.as_mut().and_then(|h| h.remove(&xid)).is_some();
        if !removed {
            return Err(elog_error("hash table corrupted"));
        }
        Ok(())
    })?;

    /*
     * Stop the worker if there are enough workers in the pool, or if the leader
     * serialized part of the transaction. (C 576-584)
     */
    let pool_len = pool_length();
    if serialize_changes
        || pool_len > (worker::max_parallel_apply_workers_per_subscription::call() / 2)
    {
        /*
         * logicalrep_pa_worker_stop reads winfo->shared->{generation,slot_no}
         * (under the in-crate mutex), detaches and NULLs winfo->error_mq_handle
         * (launcher.c 661-665), then runs the launcher-owned LWLock stop
         * sequence (LWLock + generation/proc check + SIGUSR2).
         */
        let (generation, slot_no) = {
            let s = shared.locked.lock().unwrap();
            (s.logicalrep_worker_generation, s.logicalrep_worker_slot_no)
        };
        let error_mq_handle = winfo_or_err(winfo_index, "pa_free_worker")?.error_mq_handle;
        if error_mq_handle != 0 {
            worker::shm_mq_detach_error::call(error_mq_handle)?;
            with_globals(|g| {
                if let Some(w) = g.pool[winfo_index].as_mut() {
                    w.error_mq_handle = 0;
                }
            });
        }
        worker::logicalrep_pa_worker_stop::call(generation, slot_no)?;
        pa_free_worker_info(winfo_index)?;
        return Ok(());
    }

    with_globals(|g| {
        let w = g.pool[winfo_index].as_mut().unwrap();
        w.in_use = false;
        w.serialize_changes = false;
    });
    Ok(())
}

// ===========================================================================
// 7. pa_free_worker_info (C 594-616)
// ===========================================================================

/// `static void pa_free_worker_info(ParallelApplyWorkerInfo *winfo)`.
///
/// Detaches the queues, optionally unlinks serialized files, detaches the DSM
/// segment, removes the entry from the pool (`list_delete_ptr`) and drops it
/// (`pfree`). Removal blanks the slot so still-live handles stay stable.
fn pa_free_worker_info(winfo_index: WorkerHandle) -> PgResult<()> {
    let winfo = winfo_or_err(winfo_index, "pa_free_worker_info")?;

    if winfo.mq_handle != 0 {
        worker::shm_mq_detach_data::call(winfo.mq_handle)?;
    }
    if winfo.error_mq_handle != 0 {
        worker::shm_mq_detach_error::call(winfo.error_mq_handle)?;
    }

    /* Unlink the files with serialized changes. */
    if winfo.serialize_changes {
        let xid = winfo
            .shared
            .as_ref()
            .map(shared_xid)
            .expect("pa_free_worker_info: winfo->shared is NULL");
        worker::stream_cleanup_files::call(worker::my_worker_subid::call(), xid)?;
    }

    if winfo.dsm_seg != 0 {
        worker::dsm_detach_winfo::call(winfo.dsm_seg)?;
    }

    /* Remove from the worker pool (list_delete_ptr) and pfree (slot blank). */
    with_globals(|g| g.pool[winfo_index] = None);
    Ok(())
}

// ===========================================================================
// 8. pa_detach_all_error_mq (C 621-636)
// ===========================================================================

/// `void pa_detach_all_error_mq(void)`.
pub fn pa_detach_all_error_mq() -> PgResult<()> {
    /*
     * Snapshot the (index, error_mq_handle) pairs so we don't hold the globals
     * borrow across the seam call; then clear the handle.
     */
    let targets: Vec<(WorkerHandle, u64)> = with_globals(|g| {
        g.pool
            .iter()
            .enumerate()
            .filter_map(|(i, slot)| {
                slot.as_ref()
                    .and_then(|w| (w.error_mq_handle != 0).then_some((i, w.error_mq_handle)))
            })
            .collect()
    });

    for (i, error_mq_handle) in targets {
        worker::shm_mq_detach_error::call(error_mq_handle)?;
        with_globals(|g| {
            if let Some(w) = g.pool[i].as_mut() {
                w.error_mq_handle = 0;
            }
        });
    }
    Ok(())
}

// ===========================================================================
// 9. pa_has_spooled_message_pending (C 641-649)
// ===========================================================================

/// `static bool pa_has_spooled_message_pending()`.
fn pa_has_spooled_message_pending() -> bool {
    let fileset_state = pa_get_fileset_state();
    fileset_state != PartialFileSetState::FS_EMPTY
}

// ===========================================================================
// 10. pa_process_spooled_messages_if_required (C 657-706)
// ===========================================================================

/// `static bool pa_process_spooled_messages_if_required(void)`.
fn pa_process_spooled_messages_if_required() -> PgResult<bool> {
    let mut fileset_state = pa_get_fileset_state();

    if fileset_state == PartialFileSetState::FS_EMPTY {
        return Ok(false);
    }

    /*
     * If the leader apply worker is busy serializing the partial changes then
     * acquire the stream lock now and wait for the leader worker to finish.
     */
    if fileset_state == PartialFileSetState::FS_SERIALIZE_IN_PROGRESS {
        let xid = shared_xid(&my_parallel_shared());
        pa_lock_stream(xid, AccessShareLock)?;
        pa_unlock_stream(xid, AccessShareLock)?;

        fileset_state = pa_get_fileset_state();
    }

    /*
     * We cannot read the file immediately after the leader has serialized all
     * changes to the file because there may still be messages in the memory
     * queue.
     */
    if fileset_state == PartialFileSetState::FS_SERIALIZE_DONE {
        pa_set_fileset_state_my(PartialFileSetState::FS_READY)?;
    } else if fileset_state == PartialFileSetState::FS_READY {
        worker::apply_spooled_messages::call(InvalidXLogRecPtr)?;
        pa_set_fileset_state_my(PartialFileSetState::FS_EMPTY)?;
    }

    Ok(true)
}

// ===========================================================================
// 11. ProcessParallelApplyInterrupts (C 711-730)
// ===========================================================================

/// `static void ProcessParallelApplyInterrupts(void)`.
fn ProcessParallelApplyInterrupts() -> PgResult<()> {
    backend_tcop_postgres_seams::check_for_interrupts::call()?;

    if backend_postmaster_interrupt::ShutdownRequestPending() {
        ereport(LOG)
            .errmsg(format!(
                "logical replication parallel apply worker for subscription \"{}\" has finished",
                worker::my_subscription_name::call()
            ))
            .finish(errloc("ProcessParallelApplyInterrupts"))?;

        backend_storage_ipc_seams::proc_exit::call(0, backend_utils_init_small_seams::my_proc_pid::call());
    }

    if backend_postmaster_interrupt::ConfigReloadPending() {
        backend_postmaster_interrupt::SetConfigReloadPending(false);
        backend_utils_misc_guc_file_seams::process_config_file::call(PGC_SIGHUP)?;
    }

    Ok(())
}

// ===========================================================================
// 12. LogicalParallelApplyLoop (C 733-833)
// ===========================================================================

/// `static void LogicalParallelApplyLoop(shm_mq_handle *mqh)`.
///
/// The change queue handle is owned by the runtime (set up in
/// `ParallelApplyWorkerMain`); the receive is routed through
/// `shm_mq_receive_main`.
fn LogicalParallelApplyLoop() -> PgResult<()> {
    /*
     * Init the ApplyMessageContext + push the apply error context callback.
     * (Both owned by the apply/worker + memory-context subsystems.)
     */
    worker::push_apply_error_callback::call()?;

    let loop_result: PgResult<()> = (|| {
        loop {
            ProcessParallelApplyInterrupts()?;

            let recv = worker::shm_mq_receive_main::call()?;

            match recv.result {
                ShmMqResult::SHM_MQ_SUCCESS => {
                    let data = recv.data;
                    let len = data.len();

                    if len == 0 {
                        return Err(elog_error("invalid message length"));
                    }

                    /*
                     * The first byte of messages sent from leader apply worker
                     * to parallel apply workers can only be 'w'.
                     */
                    let c = data[0];
                    if c != b'w' {
                        return Err(elog_error(format!("unexpected message \"{}\"", c as char)));
                    }

                    /*
                     * Ignore statistics fields (start_lsn, end_lsn, send_time)
                     * already updated by the leader: skip 'w' + SIZE_STATS_MESSAGE.
                     */
                    let cursor = 1 + SIZE_STATS_MESSAGE;
                    let dispatch_slice = &data[cursor..];
                    worker::apply_dispatch::call(dispatch_slice)?;
                }
                ShmMqResult::SHM_MQ_WOULD_BLOCK => {
                    /* Replay the changes from the file, if any. */
                    if !pa_process_spooled_messages_if_required()? {
                        /* Wait for more work. */
                        let rc = backend_storage_ipc_latch_seams::wait_latch_my_latch::call(
                            WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                            1000,
                            WAIT_EVENT_LOGICAL_PARALLEL_APPLY_MAIN,
                        )?;

                        if rc & WL_LATCH_SET != 0 {
                            backend_storage_ipc_latch_seams::reset_latch_my_latch::call();
                        }
                    }
                }
                ShmMqResult::SHM_MQ_DETACHED => {
                    return Err(ereport_oops_lost_connection());
                }
            }
        }
    })();

    /* Pop the error context stack on the way out (C 829-832). */
    worker::pop_apply_error_callback::call();

    loop_result
}

// ===========================================================================
// 13. pa_shutdown (C 843-851)
// ===========================================================================

/// `static void pa_shutdown(int code, Datum arg)`.
///
/// `arg` is the `dsm_segment *` (carried as the opaque dsm_handle the Datum
/// wraps). Registered as the `before_shmem_exit` callback by the runtime during
/// the worker attach phase (C line 928).
pub fn pa_shutdown(_code: i32, seg: u32) -> PgResult<()> {
    /*
     * SendProcSignal(MyLogicalRepWorker->leader_pid,
     *                PROCSIG_PARALLEL_APPLY_MESSAGE, INVALID_PROC_NUMBER);
     */
    SendProcSignal(
        worker::my_worker_leader_pid::call(),
        ProcSignalReason::PROCSIG_PARALLEL_APPLY_MESSAGE,
        INVALID_PROC_NUMBER,
    );

    /* dsm_detach((dsm_segment *) DatumGetPointer(arg)); */
    worker::dsm_detach_handle::call(seg)
}

// ===========================================================================
// 14. ParallelApplyWorkerMain (C 856-986)
// ===========================================================================

/// `void ParallelApplyWorkerMain(Datum main_arg)`.
///
/// `main_arg` is the worker-slot number (`DatumGetInt32`). Every attach step is
/// performed by `worker_attach_dsm` (signal setup → dsm_attach → toc lookups →
/// queue attach → slot attach → before_shmem_exit → error-queue redirect →
/// InitializeLogRepWorker → replication-origin setup → syscache callback); this
/// function keeps the exact phase *ordering* and then enters the worker loop.
pub fn ParallelApplyWorkerMain(main_arg: i32) -> PgResult<()> {
    let worker_slot = main_arg;

    /*
     * Attach to the segment (signal setup, dsm_attach, toc lookups, queue
     * attach, slot attach, before_shmem_exit, error-queue redirect,
     * InitializeLogRepWorker, origin setup, syscache callback) and recover the
     * in-crate shared-header token from the TOC. Binding MyParallelShared is
     * `MyParallelShared = shared;` (C 907); the spinlock write of generation /
     * slot_no (C 930-933) follows.
     */
    let token = worker::worker_attach_dsm::call(worker_slot)?;
    let shared = with_globals(|g| g.shared_registry.remove(&token))
        .expect("ParallelApplyWorkerMain: shared-header token not found in registry");
    {
        let mut s = shared.locked.lock().unwrap();
        s.logicalrep_worker_generation = worker::my_worker_generation::call();
        s.logicalrep_worker_slot_no = worker_slot;
    }
    with_globals(|g| g.my_parallel_shared = Some(shared));

    LogicalParallelApplyLoop()?;

    /* The parallel apply worker must not get here ... (C 979-985). */
    debug_assert!(false, "parallel apply worker returned from main loop");
    Ok(())
}

// ===========================================================================
// 15. HandleParallelApplyMessageInterrupt (C 995-1001)
// ===========================================================================

/// `void HandleParallelApplyMessageInterrupt(void)` — runs in a signal handler.
pub fn HandleParallelApplyMessageInterrupt() {
    backend_utils_init_small_seams::set_interrupt_pending::call(true); /* InterruptPending = true; */
    set_parallel_apply_message_pending(true);
    backend_storage_ipc_latch_seams::set_latch_my_latch::call(); /* SetLatch(MyLatch); */
}

// ===========================================================================
// 16. ProcessParallelApplyMessage (C 1007-1064)
// ===========================================================================

/// `static void ProcessParallelApplyMessage(StringInfo msg)`.
///
/// `msg` is the raw error-queue payload; the first byte is the message type.
fn ProcessParallelApplyMessage(msg: &[u8]) -> PgResult<()> {
    let msgtype = msg[0]; /* pq_getmsgbyte(msg) */

    match msgtype {
        b'E' => {
            /* ErrorResponse */
            /*
             * In C the leading message-type byte ('E') was already consumed by
             * the pq_getmsgbyte(msg) above (it advanced msg->cursor), so
             * pq_parse_errornotice begins reading at the byte *after* the type.
             */
            let edata = backend_libpq_pqmq_seams::pq_parse_errornotice::call(&msg[1..])?;

            /*
             * If desired, add a context line to show that this is a message
             * propagated from a parallel apply worker. (C 1029-1033)
             */
            let pa_label = "logical replication parallel apply worker";
            let context = match edata.context {
                Some(ctx) => format!("{ctx}\n{pa_label}"), /* psprintf("%s\n%s", ...) */
                None => pa_label.to_string(),               /* pstrdup(_()) */
            };

            /*
             * Context beyond that should use the error context callbacks that
             * were in effect in LogicalRepApplyLoop().
             */
            worker::restore_apply_error_context_stack::call();

            /* The actual error must have been reported by the PA worker. */
            Err(PgError::error(
                "logical replication parallel apply worker exited due to error",
            )
            .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .with_context(context))
        }

        /* Don't need to do anything about NoticeResponse and NotifyResponse. */
        b'N' | b'A' => Ok(()),

        _ => Err(elog_error(format!(
            "unrecognized message type received from logical replication parallel apply worker: {} (message length {} bytes)",
            msgtype as char,
            msg.len()
        ))),
    }
}

// ===========================================================================
// 17. ProcessParallelApplyMessages (C 1069-1143)
// ===========================================================================

/// `void ProcessParallelApplyMessages(void)`.
pub fn ProcessParallelApplyMessages() -> PgResult<()> {
    /*
     * Block interrupts until done (C 1084), and do work in a private,
     * reset-on-each-use context (C 1091-1098).
     */
    backend_utils_init_small_seams::hold_interrupts::call();

    let result: PgResult<()> = (|| {
        worker::enter_hpam_context::call()?;

        set_parallel_apply_message_pending(false);

        /*
         * Walk the pool; pull at most one message from each worker's error
         * queue. Snapshot the (index, error_mq_handle) pairs up front so we
         * don't hold the globals borrow across the (re-entrant) processing.
         */
        let winfos: Vec<(WorkerHandle, u64)> = with_globals(|g| {
            g.pool
                .iter()
                .enumerate()
                .filter_map(|(i, slot)| slot.as_ref().map(|w| (i, w.error_mq_handle)))
                .collect()
        });

        for (_i, error_mq_handle) in winfos {
            /*
             * The leader detaches and NULLs the error queue before stopping all
             * parallel apply workers, so skip a detached one.
             */
            if error_mq_handle == 0 {
                continue;
            }

            let recv = worker::shm_mq_receive_error::call(error_mq_handle)?;

            match recv.result {
                ShmMqResult::SHM_MQ_WOULD_BLOCK => continue,
                ShmMqResult::SHM_MQ_SUCCESS => {
                    ProcessParallelApplyMessage(&recv.data)?;
                }
                ShmMqResult::SHM_MQ_DETACHED => {
                    /* C 1132-1134: "parallel apply worker" wording. */
                    return Err(ereport_oops_lost_connection_parallel());
                }
            }
        }

        Ok(())
    })();

    /* MemoryContextSwitchTo(oldcontext); reset hpam_context (C 1137-1140). */
    worker::leave_hpam_context::call();

    backend_utils_init_small_seams::resume_interrupts::call();

    result
}

// ===========================================================================
// 18. pa_send_data (C 1152-1209)
// ===========================================================================

/// `bool pa_send_data(ParallelApplyWorkerInfo *winfo, Size nbytes, const void *data)`.
pub fn pa_send_data(winfo_index: WorkerHandle, data: &[u8]) -> PgResult<bool> {
    let (mq_handle, serialize_changes) = {
        let w = winfo_or_err(winfo_index, "pa_send_data")?;
        (w.mq_handle, w.serialize_changes)
    };

    debug_assert!(!backend_access_transam_xact_seams::is_transaction_state::call());
    debug_assert!(!serialize_changes);
    let _ = serialize_changes;

    /* We don't try to send data to parallel worker for 'immediate' mode. */
    if worker::debug_streaming_is_immediate::call() {
        return Ok(false);
    }

    let mut start_time: TimestampTz = 0;

    loop {
        let result = worker::shm_mq_send_data::call(mq_handle, data)?;

        if result == ShmMqResult::SHM_MQ_SUCCESS {
            return Ok(true);
        } else if result == ShmMqResult::SHM_MQ_DETACHED {
            return Err(PgError::error("could not send data to shared-memory queue")
                .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE));
        }

        debug_assert_eq!(result, ShmMqResult::SHM_MQ_WOULD_BLOCK);

        /* Wait before retrying. */
        let rc = backend_storage_ipc_latch_seams::wait_latch_my_latch::call(
            WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
            SHM_SEND_RETRY_INTERVAL_MS,
            WAIT_EVENT_LOGICAL_APPLY_SEND_DATA,
        )?;

        if rc & WL_LATCH_SET != 0 {
            backend_storage_ipc_latch_seams::reset_latch_my_latch::call();
            backend_tcop_postgres_seams::check_for_interrupts::call()?;
        }

        if start_time == 0 {
            start_time = backend_utils_adt_timestamp_seams::get_current_timestamp::call();
        } else if backend_utils_adt_timestamp_seams::timestamp_difference_exceeds::call(
            start_time,
            backend_utils_adt_timestamp_seams::get_current_timestamp::call(),
            SHM_SEND_TIMEOUT_MS,
        ) {
            return Ok(false);
        }
    }
}

// ===========================================================================
// 19. pa_switch_to_partial_serialize (C 1217-1244)
// ===========================================================================

/// `void pa_switch_to_partial_serialize(ParallelApplyWorkerInfo *winfo, bool stream_locked)`.
pub fn pa_switch_to_partial_serialize(
    winfo_index: WorkerHandle,
    stream_locked: bool,
) -> PgResult<()> {
    let shared = winfo_shared_or_panic(winfo_index);
    let xid = shared_xid(&shared);

    ereport(LOG)
        .errmsg(format!(
            "logical replication apply worker will serialize the remaining changes of remote transaction {xid} to a file"
        ))
        .finish(errloc("pa_switch_to_partial_serialize"))?;

    /*
     * Stop trying to send data directly to the (possibly stuck) worker and
     * start serializing data to the file instead.
     */
    with_globals(|g| g.pool[winfo_index].as_mut().unwrap().serialize_changes = true);

    /* Initialize the stream fileset. */
    worker::stream_start_internal::call(xid, true)?;

    /*
     * Acquire the stream lock if not already, so the parallel apply worker will
     * wait for the leader to release it until the end of the transaction.
     */
    if !stream_locked {
        pa_lock_stream(xid, AccessExclusiveLock)?;
    }

    /* pa_set_fileset_state(winfo->shared, FS_SERIALIZE_IN_PROGRESS) */
    pa_set_fileset_state_handle(&shared, PartialFileSetState::FS_SERIALIZE_IN_PROGRESS)
}

// ===========================================================================
// 20. pa_wait_for_xact_state (C 1250-1275)
// ===========================================================================

/// `static void pa_wait_for_xact_state(ParallelApplyWorkerInfo *winfo, ParallelTransState xact_state)`.
fn pa_wait_for_xact_state(
    shared: &Arc<ParallelApplyWorkerShared>,
    xact_state: ParallelTransState,
) -> PgResult<()> {
    loop {
        /* Stop if the transaction state has reached or exceeded xact_state. */
        if pa_get_xact_state(shared) >= xact_state {
            break;
        }

        /* Wait to be signalled. */
        backend_storage_ipc_latch_seams::wait_latch_my_latch::call(
            WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
            10,
            WAIT_EVENT_LOGICAL_PARALLEL_APPLY_STATE_CHANGE,
        )?;

        /* Reset the latch so we don't spin. */
        backend_storage_ipc_latch_seams::reset_latch_my_latch::call();

        /* An interrupt may have occurred while we were waiting. */
        backend_tcop_postgres_seams::check_for_interrupts::call()?;
    }
    Ok(())
}

// ===========================================================================
// 21. pa_wait_for_xact_finish (C 1280-1308)
// ===========================================================================

/// `static void pa_wait_for_xact_finish(ParallelApplyWorkerInfo *winfo)`.
fn pa_wait_for_xact_finish(shared: &Arc<ParallelApplyWorkerShared>) -> PgResult<()> {
    /*
     * Wait until the PA worker set the state to PARALLEL_TRANS_STARTED, meaning
     * it has acquired the transaction lock.
     */
    pa_wait_for_xact_state(shared, ParallelTransState::PARALLEL_TRANS_STARTED)?;

    /* Wait for the transaction lock to be released. (deadlock detection) */
    let xid = shared_xid(shared);
    pa_lock_transaction(xid, AccessShareLock)?;
    pa_unlock_transaction(xid, AccessShareLock)?;

    /*
     * Check if the state became PARALLEL_TRANS_FINISHED in case the PA worker
     * failed while applying changes causing the lock to be released.
     */
    if pa_get_xact_state(shared) != ParallelTransState::PARALLEL_TRANS_FINISHED {
        /* C 1305-1307: "parallel apply worker" wording. */
        return Err(ereport_oops_lost_connection_parallel());
    }
    Ok(())
}

// ===========================================================================
// 22. pa_set_xact_state (C 1313-1320)
// ===========================================================================

/// `void pa_set_xact_state(ParallelApplyWorkerShared *wshared, ParallelTransState xact_state)`
/// (C 1313-1320): `SpinLockAcquire; wshared->xact_state = x; SpinLockRelease`.
///
/// The C callers pass either `winfo->shared` (the leader, addressed here by the
/// pool handle, [`pa_set_xact_state_handle`]) or `MyParallelShared` (the worker,
/// [`pa_set_xact_state_my`]). This public entry takes the pool handle so cross-
/// crate callers (e.g. the worker) can drive the leader-side header.
pub fn pa_set_xact_state(winfo_index: WorkerHandle, xact_state: ParallelTransState) {
    pa_set_xact_state_handle(&winfo_shared_or_panic(winfo_index), xact_state);
}

/// `pa_set_xact_state(wshared, ...)` against a resolved header.
fn pa_set_xact_state_handle(shared: &Arc<ParallelApplyWorkerShared>, xact_state: ParallelTransState) {
    shared.locked.lock().unwrap().xact_state = xact_state;
}

// ===========================================================================
// 23. pa_get_xact_state (C 1325-1335)
// ===========================================================================

/// `static ParallelTransState pa_get_xact_state(ParallelApplyWorkerShared *wshared)`
/// (C 1325-1335): `SpinLockAcquire; x = wshared->xact_state; SpinLockRelease`.
fn pa_get_xact_state(shared: &Arc<ParallelApplyWorkerShared>) -> ParallelTransState {
    shared.locked.lock().unwrap().xact_state
}

// ===========================================================================
// 24. pa_set_stream_apply_worker (C 1340-1344)
// ===========================================================================

/// `void pa_set_stream_apply_worker(ParallelApplyWorkerInfo *winfo)`.
pub fn pa_set_stream_apply_worker(winfo_index: Option<WorkerHandle>) {
    with_globals(|g| g.stream_apply_worker = winfo_index);
}

// ===========================================================================
// 25. pa_savepoint_name (C 1354-1358)
// ===========================================================================

/// `static void pa_savepoint_name(Oid suboid, TransactionId xid, char *spname, Size szsp)`.
///
/// Returns the formatted name (`snprintf(spname, szsp, "pg_sp_%u_%u", ...)`),
/// truncated to `szsp` bytes including the NUL, exactly as `snprintf` would.
fn pa_savepoint_name(suboid: Oid, xid: TransactionId, szsp: Size) -> String {
    let s = format!("pg_sp_{suboid}_{xid}");
    /* snprintf truncates to szsp-1 chars + NUL. */
    if szsp == 0 {
        return String::new();
    }
    let max = szsp - 1;
    if s.len() > max {
        s[..max].to_string()
    } else {
        s
    }
}

// ===========================================================================
// 26. pa_start_subtrans (C 1368-1405)
// ===========================================================================

/// `void pa_start_subtrans(TransactionId current_xid, TransactionId top_xid)`.
pub fn pa_start_subtrans(current_xid: TransactionId, top_xid: TransactionId) -> PgResult<()> {
    if current_xid != top_xid && !with_globals(|g| g.subxactlist.contains(&current_xid)) {
        let spname =
            pa_savepoint_name(worker::my_subscription_oid::call(), current_xid, NAMEDATALEN);

        ereport(DEBUG1)
            .errmsg_internal(format!(
                "defining savepoint {spname} in logical replication parallel apply worker"
            ))
            .finish(errloc("pa_start_subtrans"))?;

        /* We must be in transaction block to define the SAVEPOINT. */
        if !backend_access_transam_xact_seams::is_transaction_block::call() {
            if !backend_access_transam_xact_seams::is_transaction_state::call() {
                backend_access_transam_xact_seams::start_transaction_command::call()?;
            }

            backend_access_transam_xact_seams::begin_transaction_block::call()?;
            backend_access_transam_xact_seams::commit_transaction_command::call()?;
        }

        backend_access_transam_xact_seams::define_savepoint::call(&spname)?;

        /*
         * CommitTransactionCommand is needed to start a subtransaction after
         * issuing a SAVEPOINT inside a transaction block.
         */
        backend_access_transam_xact_seams::commit_transaction_command::call()?;

        /* subxactlist = lappend_xid(subxactlist, current_xid); (C 1402) */
        with_globals(|g| g.subxactlist.push(current_xid));
    }
    Ok(())
}

// ===========================================================================
// 27. pa_reset_subtrans (C 1408-1416)
// ===========================================================================

/// `void pa_reset_subtrans(void)`.
pub fn pa_reset_subtrans() {
    /*
     * We don't need to free this explicitly as the allocated memory will be
     * freed at the transaction end.
     */
    with_globals(|g| g.subxactlist.clear()); /* subxactlist = NIL; */
}

// ===========================================================================
// 28. pa_stream_abort (C 1422-1497)
// ===========================================================================

/// The `LogicalRepStreamAbortData` field set this function reads. The full
/// parse of the abort message belongs to the apply-protocol code; this carries
/// only the four fields `pa_stream_abort` touches.
pub struct LogicalRepStreamAbortData {
    pub xid: TransactionId,
    pub subxid: TransactionId,
    pub abort_lsn: XLogRecPtr,
    pub abort_time: TimestampTz,
}

/// `void pa_stream_abort(LogicalRepStreamAbortData *abort_data)`.
pub fn pa_stream_abort(abort_data: &LogicalRepStreamAbortData) -> PgResult<()> {
    let xid = abort_data.xid;
    let subxid = abort_data.subxid;

    /*
     * Update origin state so we can restart streaming from correct position in
     * case of crash.
     */
    backend_replication_logical_origin_seams::set_replorigin_session_origin_lsn::call(
        abort_data.abort_lsn,
    );
    backend_replication_logical_origin_seams::set_replorigin_session_origin_timestamp::call(
        abort_data.abort_time,
    );

    /*
     * If the two XIDs are the same, it's in fact abort of toplevel xact, so
     * just free the subxactlist.
     */
    if subxid == xid {
        /* pa_set_xact_state(MyParallelShared, PARALLEL_TRANS_FINISHED) (C 1441) */
        pa_set_xact_state_my(ParallelTransState::PARALLEL_TRANS_FINISHED);

        /*
         * Release the lock as we might be processing an empty streaming
         * transaction.
         */
        pa_unlock_transaction(xid, AccessExclusiveLock)?;

        backend_access_transam_xact_seams::abort_current_transaction::call()?;

        if backend_access_transam_xact_seams::is_transaction_block::call() {
            backend_access_transam_xact_seams::end_transaction_block::call(false)?;
            backend_access_transam_xact_seams::commit_transaction_command::call()?;
        }

        pa_reset_subtrans();

        backend_utils_activity_status_seams::pgstat_report_activity_idle::call();
    } else {
        /* OK, so it's a subxact. Rollback to the savepoint. */
        let spname = pa_savepoint_name(worker::my_subscription_oid::call(), subxid, NAMEDATALEN);

        ereport(DEBUG1)
            .errmsg_internal(format!(
                "rolling back to savepoint {spname} in logical replication parallel apply worker"
            ))
            .finish(errloc("pa_stream_abort"))?;

        /*
         * Search the subxactlist, determine the offset tracked for the subxact,
         * and truncate the list.
         */
        let mut i = with_globals(|g| g.subxactlist.len() as i32) - 1;
        while i >= 0 {
            let xid_tmp = with_globals(|g| g.subxactlist[i as usize]);

            if xid_tmp == subxid {
                backend_access_transam_xact_seams::rollback_to_savepoint::call(&spname)?;
                backend_access_transam_xact_seams::commit_transaction_command::call()?;
                /* subxactlist = list_truncate(subxactlist, i); keeps first i. */
                with_globals(|g| g.subxactlist.truncate(i as usize));
                break;
            }
            i -= 1;
        }
    }
    Ok(())
}

/// `pa_set_xact_state(MyParallelShared, ...)` (worker side).
fn pa_set_xact_state_my(xact_state: ParallelTransState) {
    pa_set_xact_state_handle(&my_parallel_shared(), xact_state);
}

// ===========================================================================
// 29. pa_set_fileset_state (C 1504-1519)
// ===========================================================================

/// `void pa_set_fileset_state(ParallelApplyWorkerShared *wshared, PartialFileSetState fileset_state)`
/// (C 1504-1519). The leader entry takes the pool handle.
pub fn pa_set_fileset_state(
    winfo_index: WorkerHandle,
    fileset_state: PartialFileSetState,
) -> PgResult<()> {
    pa_set_fileset_state_handle(&winfo_shared_or_panic(winfo_index), fileset_state)
}

/// `pa_set_fileset_state(wshared, ...)` against a resolved header (C 1504-1519):
///
/// ```c
/// SpinLockAcquire(&wshared->mutex);
/// wshared->fileset_state = fileset_state;
/// if (fileset_state == FS_SERIALIZE_DONE) {
///     Assert(am_leader_apply_worker());
///     Assert(MyLogicalRepWorker->stream_fileset);
///     wshared->fileset = *MyLogicalRepWorker->stream_fileset;
/// }
/// SpinLockRelease(&wshared->mutex);
/// ```
fn pa_set_fileset_state_handle(
    shared: &Arc<ParallelApplyWorkerShared>,
    fileset_state: PartialFileSetState,
) -> PgResult<()> {
    /*
     * The `stream_fileset` source read crosses an ownership boundary (it is
     * `MyLogicalRepWorker->stream_fileset`, owned by the worker), so read its
     * value before taking the mutex. The C reads it inside the spinlock, but
     * the value is stable for the duration and the mutex only protects the
     * destination `wshared` fields.
     */
    let stream_fileset = if fileset_state == PartialFileSetState::FS_SERIALIZE_DONE {
        debug_assert!(worker::am_leader_apply_worker::call()?);
        Some(
            worker::my_worker_stream_fileset::call()
                .expect("pa_set_fileset_state: MyLogicalRepWorker->stream_fileset is NULL"),
        )
    } else {
        None
    };

    let mut s = shared.locked.lock().unwrap();
    s.fileset_state = fileset_state;
    if fileset_state == PartialFileSetState::FS_SERIALIZE_DONE {
        s.fileset = stream_fileset.unwrap();
    }
    Ok(())
}

/// `pa_set_fileset_state(MyParallelShared, ...)` — the worker-side wrapper used
/// by `pa_process_spooled_messages_if_required`.
fn pa_set_fileset_state_my(fileset_state: PartialFileSetState) -> PgResult<()> {
    pa_set_fileset_state_handle(&my_parallel_shared(), fileset_state)
}

// ===========================================================================
// 30. pa_get_fileset_state (C 1524-1536)
// ===========================================================================

/// `static PartialFileSetState pa_get_fileset_state(void)` (C 1524-1536):
/// `Assert(am_parallel_apply_worker()); SpinLockAcquire; x =
/// MyParallelShared->fileset_state; SpinLockRelease`.
fn pa_get_fileset_state() -> PartialFileSetState {
    debug_assert!(worker::am_parallel_apply_worker::call());
    my_parallel_shared().locked.lock().unwrap().fileset_state
}

// ===========================================================================
// 31-34. pa_lock_stream / pa_unlock_stream / pa_lock_transaction / pa_unlock_transaction
// (C 1546-1591)
// ===========================================================================

/// `void pa_lock_stream(TransactionId xid, LOCKMODE lockmode)`.
pub fn pa_lock_stream(xid: TransactionId, lockmode: LOCKMODE) -> PgResult<()> {
    backend_storage_lmgr_lmgr_seams::lock_apply_transaction_for_session::call(
        worker::my_worker_subid::call(),
        xid,
        PARALLEL_APPLY_LOCK_STREAM,
        lockmode,
    )
}

/// `void pa_unlock_stream(TransactionId xid, LOCKMODE lockmode)`.
pub fn pa_unlock_stream(xid: TransactionId, lockmode: LOCKMODE) -> PgResult<()> {
    backend_storage_lmgr_lmgr_seams::unlock_apply_transaction_for_session::call(
        worker::my_worker_subid::call(),
        xid,
        PARALLEL_APPLY_LOCK_STREAM,
        lockmode,
    )
}

/// `void pa_lock_transaction(TransactionId xid, LOCKMODE lockmode)`.
pub fn pa_lock_transaction(xid: TransactionId, lockmode: LOCKMODE) -> PgResult<()> {
    backend_storage_lmgr_lmgr_seams::lock_apply_transaction_for_session::call(
        worker::my_worker_subid::call(),
        xid,
        PARALLEL_APPLY_LOCK_XACT,
        lockmode,
    )
}

/// `void pa_unlock_transaction(TransactionId xid, LOCKMODE lockmode)`.
pub fn pa_unlock_transaction(xid: TransactionId, lockmode: LOCKMODE) -> PgResult<()> {
    backend_storage_lmgr_lmgr_seams::unlock_apply_transaction_for_session::call(
        worker::my_worker_subid::call(),
        xid,
        PARALLEL_APPLY_LOCK_XACT,
        lockmode,
    )
}

// ===========================================================================
// 35. pa_decr_and_wait_stream_block (C 1597-1619)
// ===========================================================================

/// `void pa_decr_and_wait_stream_block(void)`.
pub fn pa_decr_and_wait_stream_block() -> PgResult<()> {
    debug_assert!(worker::am_parallel_apply_worker::call());

    let shared = my_parallel_shared();

    /*
     * It is only possible to not have any pending stream chunks when we are
     * applying spooled messages.
     */
    /* pg_atomic_read_u32(&MyParallelShared->pending_stream_count) (C 1606) */
    if shared.pending_stream_count.load(SeqCst) == 0 {
        if pa_has_spooled_message_pending() {
            return Ok(());
        }

        return Err(elog_error("invalid pending streaming chunk 0"));
    }

    /* pg_atomic_sub_fetch_u32(&MyParallelShared->pending_stream_count, 1) (C 1614) */
    if shared.pending_stream_count.fetch_sub(1, SeqCst) - 1 == 0 {
        let xid = shared_xid(&shared);
        pa_lock_stream(xid, AccessShareLock)?;
        pa_unlock_stream(xid, AccessShareLock)?;
    }
    Ok(())
}

// ===========================================================================
// 36. pa_xact_finish (C 1624-1646)
// ===========================================================================

/// `void pa_xact_finish(ParallelApplyWorkerInfo *winfo, XLogRecPtr remote_lsn)`.
pub fn pa_xact_finish(winfo_index: WorkerHandle, remote_lsn: XLogRecPtr) -> PgResult<()> {
    debug_assert!(worker::am_leader_apply_worker::call()?);

    let shared = winfo_shared_or_panic(winfo_index);
    let xid = shared_xid(&shared);

    /*
     * Unlock the shared object lock so that the parallel apply worker can
     * continue to receive and apply changes.
     */
    pa_unlock_stream(xid, AccessExclusiveLock)?;

    /* Wait for that worker to finish (preserves commit order). */
    pa_wait_for_xact_finish(&shared)?;

    if !XLogRecPtrIsInvalid(remote_lsn) {
        let last_commit_end = shared_last_commit_end(&shared);
        worker::store_flush_position::call(remote_lsn, last_commit_end)?;
    }

    pa_free_worker(winfo_index)
}

// ---------------------------------------------------------------------------
// Small shared helpers.
// ---------------------------------------------------------------------------

/// `list_length(ParallelApplyWorkerPool)` — count of live (non-blanked) slots.
fn pool_length() -> i32 {
    with_globals(|g| g.pool.iter().filter(|s| s.is_some()).count() as i32)
}

/// Clone a live winfo or surface the C "dangling winfo" deref as an error.
fn winfo_or_err(winfo_index: WorkerHandle, who: &str) -> PgResult<ParallelApplyWorkerInfo> {
    with_globals(|g| g.pool.get(winfo_index).cloned().flatten())
        .ok_or_else(|| PgError::error(format!("{who}: worker pool slot is empty")))
}

/// `winfo->shared` — the in-crate shared header. A live winfo always has it; a
/// stale handle (or a winfo before `pa_setup_dsm`) is the C dangling/NULL deref,
/// so panic loudly.
fn winfo_shared_or_panic(winfo_index: WorkerHandle) -> Arc<ParallelApplyWorkerShared> {
    with_globals(|g| g.pool.get(winfo_index).cloned().flatten())
        .and_then(|w| w.shared)
        .unwrap_or_else(|| panic!("stale winfo handle {winfo_index}"))
}

/// `elog(ERROR, ...)` — an internal error (elog uses ERRCODE_INTERNAL_ERROR,
/// which `PgError::error` defaults to).
fn elog_error(msg: impl Into<String>) -> PgError {
    PgError::error(msg)
}

/// `ereport(ERROR, errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
/// errmsg("lost connection to the logical replication apply worker"))` —
/// `LogicalParallelApplyLoop` (C 820-822). This site says *apply worker*, not
/// *parallel apply worker* (see the PARALLEL variant below).
fn ereport_oops_lost_connection() -> PgError {
    PgError::error("lost connection to the logical replication apply worker")
        .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
}

/// The *parallel* apply worker wording used in `ProcessParallelApplyMessages`
/// (C 1132-1134) and `pa_wait_for_xact_finish` (C 1305-1307).
fn ereport_oops_lost_connection_parallel() -> PgError {
    PgError::error("lost connection to the logical replication parallel apply worker")
        .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
}

/// `ErrorLocation` for an `ereport(...).finish(...)` call.
fn errloc(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new(
        "src/backend/replication/logical/applyparallelworker.c",
        0,
        funcname,
    )
}

/// Install every seam this crate owns. Called once at startup by `seams-init`.
pub fn init_seams() {
    backend_replication_logical_applyparallelworker_seams::handle_parallel_apply_message_interrupt::set(
        HandleParallelApplyMessageInterrupt,
    );
}

#[cfg(test)]
mod tests;
