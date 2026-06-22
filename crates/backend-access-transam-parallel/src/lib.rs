//! Port of `src/backend/access/transam/parallel.c` (PostgreSQL 18.3) — the
//! infrastructure for launching parallel workers.
//!
//! This file is pure *orchestration* of the parallel-worker lifecycle: the
//! estimate/serialize phases of `InitializeParallelDSM`, the launch loop of
//! `LaunchParallelWorkers`, the attach/finish/exit wait loops, the
//! message-dispatch switch in `ProcessParallelMessage`, the
//! `InternalParallelWorkers` lookup in `LookupParallelWorkerFunction`, the
//! end-of-(sub)xact teardown, and the long fixed setup sequence of
//! `ParallelWorkerMain`.
//!
//! ## Identity model
//!
//! The live `ParallelContext` is named across the seam by
//! [`types_execparallel::ParallelContextHandle`] (execParallel.c consumes it);
//! the contents the parallel subsystem maintains behind that handle live in a
//! per-backend registry here. C's `pcxt_list` `dlist` of live contexts becomes
//! an ordered list of handles over a slab of contexts.
//!
//! The DSM segment is a *real* `dsm-core` segment: `pcxt.seg` carries the real
//! [`DsmSegmentId`] and the context owns the [`DsmSegment`] RAII guard (or, for
//! the no-worker fallback, a `TopMemoryContext`-allocated private buffer). The
//! `shm_toc` estimate/allocate/insert/lookup helpers delegate to a real
//! [`ShmToc`] built over the segment base. A [`SerializeCursor`] is the *real
//! chunk address* the `ShmToc` hands back (a raw pointer reinterpreted as
//! `usize`), and the typed `*Handle` newtypes are thin views carrying that same
//! address. The `repr(C)` chunk payloads
//! ([`FixedParallelExecutorState`]/[`SharedExecutorInstrumentation`]/the JIT
//! header) and the `"library\0function\0"` entrypoint bytes are written and
//! read back IN PLACE at the real address — no side tables (family
//! `shm-toc-address`).
//!
//! Everything genuinely external (DSM creation, `shm_mq`, background workers,
//! the latch/wait layer, the transaction/snapshot/GUC/namespace/relmapper/
//! combocid/reindex/enum/clientconninfo serializers, pgstat, libpq message
//! parsing, the misc backend accessors) is reached through
//! [`backend_access_transam_parallel_rt_seams`] and panics loudly until each
//! owner lands.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]
#![allow(clippy::too_many_arguments)]

use std::alloc::Layout;
use std::cell::Cell;
use std::cell::RefCell;
use std::ptr::NonNull;

use backend_storage_ipc_dsm_core::dsm::{
    dsm_create, dsm_segment_address, dsm_segment_handle as dsm_seg_handle, DsmSegment, DsmSegmentId,
    DSM_CREATE_NULL_IF_MAXSEGMENTS,
};
use backend_storage_ipc_shm_toc::{shm_toc_estimate, ShmToc};
use backend_utils_error::{elog, ereport, PgResult};
use mcx::{Allocator, Mcx};
use types_core::{pid_t, ProcNumber, Size, SubTransactionId, XLogRecPtr, INVALID_PROC_NUMBER};
use types_tuple::Datum;
use types_error::{
    ERRCODE_ADMIN_SHUTDOWN, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERROR, FATAL, WARNING,
};
use types_execparallel::{
    BackgroundWorkerHandle, DsmSegmentHandle as ExecDsmSeg, FixedParallelExecutorState,
    FixedStateHandle, InstrumentationHandle, JitInstrumentationHandle, ParallelContextHandle,
    ParallelWorkerContextHandle, SerializeCursor, SharedExecutorInstrumentation,
    ShmMqAttachHandle, ShmTocEstimatorHandle, ShmTocHandle as ExecShmToc,
};
use types_parallel::{
    dsm_handle, BgwHandleStatus, DsmSegmentHandle, FixedParallelState, ParallelWorkerMainFn,
    ShmMqHandleHandle, ShmMqResult,
};

use backend_storage_ipc_shm_mq_seams as shmmq;
use types_storage::storage::shm_toc_estimator;

use backend_access_transam_parallel_rt_seams as rt;

pub mod shared_dsm_object;
mod fps_driver;

// ===========================================================================
// Constants (parallel.c:48-79).
// ===========================================================================

/// Error-queue size; small because most of the time it processes only a handful
/// of small messages (parallel.c:55).
pub const PARALLEL_ERROR_QUEUE_SIZE: Size = 16384;

/// Magic number for parallel context TOC (parallel.c:58).
pub const PARALLEL_MAGIC: u64 = 0x5047_7c7c;

// Magic numbers for per-context parallel state sharing (parallel.c:65-79).
pub const PARALLEL_KEY_FIXED: u64 = 0xFFFF_FFFF_FFFF_0001;
pub const PARALLEL_KEY_ERROR_QUEUE: u64 = 0xFFFF_FFFF_FFFF_0002;
pub const PARALLEL_KEY_LIBRARY: u64 = 0xFFFF_FFFF_FFFF_0003;
pub const PARALLEL_KEY_GUC: u64 = 0xFFFF_FFFF_FFFF_0004;
pub const PARALLEL_KEY_COMBO_CID: u64 = 0xFFFF_FFFF_FFFF_0005;
pub const PARALLEL_KEY_TRANSACTION_SNAPSHOT: u64 = 0xFFFF_FFFF_FFFF_0006;
pub const PARALLEL_KEY_ACTIVE_SNAPSHOT: u64 = 0xFFFF_FFFF_FFFF_0007;
pub const PARALLEL_KEY_TRANSACTION_STATE: u64 = 0xFFFF_FFFF_FFFF_0008;
pub const PARALLEL_KEY_ENTRYPOINT: u64 = 0xFFFF_FFFF_FFFF_0009;
pub const PARALLEL_KEY_SESSION_DSM: u64 = 0xFFFF_FFFF_FFFF_000A;
pub const PARALLEL_KEY_PENDING_SYNCS: u64 = 0xFFFF_FFFF_FFFF_000B;
pub const PARALLEL_KEY_REINDEX_STATE: u64 = 0xFFFF_FFFF_FFFF_000C;
pub const PARALLEL_KEY_RELMAPPER_STATE: u64 = 0xFFFF_FFFF_FFFF_000D;
pub const PARALLEL_KEY_UNCOMMITTEDENUMS: u64 = 0xFFFF_FFFF_FFFF_000E;
pub const PARALLEL_KEY_CLIENTCONNINFO: u64 = 0xFFFF_FFFF_FFFF_000F;

/// `DSM_HANDLE_INVALID` (storage/dsm_impl.h:58).
pub const DSM_HANDLE_INVALID: dsm_handle = 0;

/// `PG_WAIT_IPC` (build-time wait-event generator).
const PG_WAIT_IPC: u32 = 0x0800_0000;
/// `WAIT_EVENT_BGWORKER_STARTUP` — `PG_WAIT_IPC | 3`.
pub const WAIT_EVENT_BGWORKER_STARTUP: u32 = PG_WAIT_IPC | 3;
/// `WAIT_EVENT_PARALLEL_FINISH` — `PG_WAIT_IPC | 40`.
pub const WAIT_EVENT_PARALLEL_FINISH: u32 = PG_WAIT_IPC | 40;

/// `WL_LATCH_SET` (storage/latch.h).
pub const WL_LATCH_SET: i32 = 1 << 0;

/// `BGWORKER_BYPASS_ALLOWCONN` (postmaster/bgworker.h:156).
pub const BGWORKER_BYPASS_ALLOWCONN: u32 = 0x0001;
/// `BGWORKER_BYPASS_ROLELOGINCHECK` (postmaster/bgworker.h:157).
pub const BGWORKER_BYPASS_ROLELOGINCHECK: u32 = 0x0002;

/// `DEBUG_PARALLEL_REGRESS` (optimizer/optimizer.h:107).
pub const DEBUG_PARALLEL_REGRESS: i32 = 2;

// libpq protocol message-type bytes (libpq/protocol.h).
const PqMsg_ErrorResponse: u8 = b'E';
const PqMsg_NoticeResponse: u8 = b'N';
const PqMsg_NotificationResponse: u8 = b'A';
const PqMsg_Progress: u8 = b'P';
const PqMsg_Terminate: u8 = b'X';

/// Names of the internal parallel-worker entry points, in C order
/// (parallel.c:136-158 `InternalParallelWorkers`).
static INTERNAL_PARALLEL_WORKERS: [&str; 5] = [
    "ParallelQueryMain",
    "_bt_parallel_build_main",
    "_brin_parallel_build_main",
    "_gin_parallel_build_main",
    "parallel_vacuum_main",
];

// ===========================================================================
// Registry (parallel.c file-scope globals + the DSM byte buffer the subsystem
// owns).
// ===========================================================================

/// `shm_mq_handle *error_mqh` — the OPTION (i) registry id the `shm-mq` owner
/// hands back from `shm_mq_attach`, or [`ERROR_MQH_NULL`] for the C NULL
/// `shm_mq_handle *`. (execParallel's `ShmMqAttachHandle` has no NULL sentinel
/// of its own; `0` is the registry's NULL because ids are 1-based.)
const ERROR_MQH_NULL: ShmMqAttachHandle = ShmMqAttachHandle(0);

/// `error_mqh == NULL`.
fn error_mqh_is_null(h: ShmMqAttachHandle) -> bool {
    h.0 == 0
}

/// Per-worker leader-side state (`ParallelWorkerInfo`, access/parallel.h:24-28).
#[derive(Clone, Copy, Debug)]
struct ParallelWorkerInfo {
    /// `BackgroundWorkerHandle *bgwhandle` — the REAL value handle
    /// (`{slot, generation}`); `None` is the C `NULL` pointer (no live worker
    /// registered for this slot).
    bgwhandle: Option<BackgroundWorkerHandle>,
    /// `shm_mq_handle *error_mqh`.
    error_mqh: ShmMqAttachHandle,
}

impl ParallelWorkerInfo {
    const fn new() -> Self {
        Self {
            bgwhandle: None,
            error_mqh: ERROR_MQH_NULL,
        }
    }
}

/// A `TopMemoryContext`-allocated private buffer backing the `shm_toc` when no
/// workers are budgeted (C: `pcxt->private_memory = MemoryContextAlloc(...)`).
/// Holds the raw allocation so `DestroyParallelContext`'s `pfree` can return it.
struct PrivateSeg {
    ptr: NonNull<u8>,
    layout: Layout,
}

// SAFETY: the parallel registry is thread-local (the `G` thread_local), so the
// pointer never crosses threads; `NonNull` is `!Send`/`!Sync` only out of
// caution. The buffer is a backend-private `MemoryContextAlloc` allocation.
unsafe impl Send for PrivateSeg {}

/// A parallel execution context (`ParallelContext`, access/parallel.h:30-46)
/// plus the real DSM segment / private buffer this subsystem owns for it.
struct ParallelContext {
    subid: SubTransactionId,
    nworkers: i32,
    nworkers_to_launch: i32,
    nworkers_launched: i32,
    library_name: String,
    function_name: String,
    /// `ErrorContextCallback *error_context_stack` — opaque pointer handle.
    error_context_stack: usize,
    /// `shm_toc_estimator estimator` (`storage/shm_toc.h`).
    estimator: shm_toc_estimator,
    /// `dsm_segment *seg` (NULL when running in private memory). Carries the
    /// real [`DsmSegmentId`] (opacity-inherited: the handle value *is* the id).
    seg: DsmSegmentHandle,
    /// The real `DsmSegment` RAII guard — owns the segment's mapping until the
    /// context is destroyed (C: `pcxt->seg`).
    seg_guard: Option<DsmSegment>,
    /// `void *private_memory` — the no-worker fallback backing buffer.
    private_memory: Option<PrivateSeg>,
    /// The real `shm_toc` over the segment (or private) base, plus that base
    /// address (so the offset<->address helpers can relativize chunks).
    toc: Option<ShmToc>,
    toc_base: usize,
    /// `ParallelWorkerInfo *worker` — empty until `InitializeParallelDSM`.
    worker: Vec<ParallelWorkerInfo>,
    nknown_attached_workers: i32,
    known_attached_workers: Vec<bool>,
}

impl ParallelContext {
    fn worker_is_null(&self) -> bool {
        self.worker.is_empty()
    }
}

struct ParallelGlobals {
    /// Slab of live contexts; slot index is the `ParallelContextHandle` payload.
    slots: Vec<Option<ParallelContext>>,
    /// `pcxt_list` order (front == dlist head).
    list: Vec<ParallelContextHandle>,
    /// `int ParallelWorkerNumber = -1;`
    parallel_worker_number: i32,
    /// `bool InitializingParallelWorker = false;`
    initializing_parallel_worker: bool,
    /// `static FixedParallelState *MyFixedParallelState;` — base handle (0=NULL).
    my_fixed_parallel_state: usize,
    /// `static pid_t ParallelLeaderPid;`
    parallel_leader_pid: pid_t,
    /// `ProcNumber ParallelLeaderProcNumber = INVALID_PROC_NUMBER;` (globals.c).
    /// A parallel worker sets this from its leader's `FixedParallelState` in
    /// `ParallelWorkerMain`; in any other backend it stays `INVALID_PROC_NUMBER`.
    parallel_leader_proc_number: ProcNumber,
}

impl ParallelGlobals {
    const fn new() -> Self {
        Self {
            slots: Vec::new(),
            list: Vec::new(),
            parallel_worker_number: -1,
            initializing_parallel_worker: false,
            my_fixed_parallel_state: 0,
            parallel_leader_pid: 0,
            parallel_leader_proc_number: INVALID_PROC_NUMBER,
        }
    }

    /// `dlist_push_head(&pcxt_list, &pcxt->node)` after `palloc0`.
    /// Stand-in for C's `palloc0(sizeof(ParallelContext))` +
    /// `dlist_push_head`. Fallible: a failed slab/list grow converts to the
    /// caller-context OOM error (`mcx.oom`), mirroring the `ereport(ERROR,
    /// ERRCODE_OUT_OF_MEMORY)` every `palloc` can raise.
    fn push_head(
        &mut self,
        mcx: Mcx<'_>,
        pcxt: ParallelContext,
    ) -> PgResult<ParallelContextHandle> {
        let slot = match self.slots.iter().position(Option::is_none) {
            Some(i) => {
                self.slots[i] = Some(pcxt);
                i
            }
            None => {
                self.slots
                    .try_reserve(1)
                    .map_err(|_| mcx.oom(core::mem::size_of::<ParallelContext>()))?;
                self.slots.push(Some(pcxt));
                self.slots.len() - 1
            }
        };
        let h = ParallelContextHandle(slot);
        self.list
            .try_reserve(1)
            .map_err(|_| mcx.oom(core::mem::size_of::<ParallelContextHandle>()))?;
        self.list.insert(0, h);
        Ok(h)
    }

    fn list_is_empty(&self) -> bool {
        self.list.is_empty()
    }

    /// `dlist_head_element(ParallelContext, node, &pcxt_list)`.
    fn head(&self) -> Option<ParallelContextHandle> {
        self.list.first().copied()
    }

    fn get(&self, h: ParallelContextHandle) -> &ParallelContext {
        self.slots[h.0].as_ref().expect("live ParallelContext")
    }

    fn get_mut(&mut self, h: ParallelContextHandle) -> &mut ParallelContext {
        self.slots[h.0].as_mut().expect("live ParallelContext")
    }
}

thread_local! {
    static G: RefCell<ParallelGlobals> = const { RefCell::new(ParallelGlobals::new()) };

    /// `volatile sig_atomic_t ParallelMessagePending = false;` (parallel.c).
    ///
    /// In C this is a standalone process-global atomic flag, NOT part of any
    /// struct guarded by a lock — `HandleParallelMessageInterrupt()` runs in a
    /// signal handler and sets it directly. It is deliberately kept OUT of the
    /// `RefCell`-guarded [`ParallelGlobals`]: the signal handler can fire while
    /// mainline code (e.g. parallel teardown in
    /// `wait_for_parallel_workers_to_finish` / `ExecParallelFinish`, or any
    /// `with_globals` block that crosses a `CHECK_FOR_INTERRUPTS`) already holds
    /// a `G.borrow_mut()`. Re-entering that borrow from the handler would panic
    /// `already borrowed: BorrowMutError`. A plain [`Cell`] is reentrancy-safe
    /// (no borrow tracking), matching C's `volatile sig_atomic_t` semantics.
    static PARALLEL_MESSAGE_PENDING: Cell<bool> = const { Cell::new(false) };
}

fn with_globals<R>(f: impl FnOnce(&mut ParallelGlobals) -> R) -> R {
    G.with(|g| f(&mut g.borrow_mut()))
}

// ===========================================================================
// Worker-side DSM/shm_toc attach registry (family `worker-attach`).
//
// `ParallelWorkerMain` (parallel.c:1351-1360) genuinely crosses into a segment
// the *leader* created: `dsm_attach` then `shm_toc_attach(PARALLEL_MAGIC,
// dsm_segment_address(seg))`. Unlike the leader, the worker has no
// `ParallelContext` to own these — and the C comment at parallel.c:1346-1349 is
// explicit that the worker keeps no ResourceOwner, so its DSM mapping "survives
// until process exit". We mirror that by holding the real [`DsmSegment`] RAII
// guard and the real attached [`ShmToc`] in this process-global registry, keyed
// by the segment's real base address (the value the worker threads onward as its
// `ExecShmToc`). The guard is never dropped on the success path — that is the C
// "survives until process exit" semantics, not a leak.
// ===========================================================================

/// One worker-attached segment: the real RAII guard (keeps the mapping live) and
/// the real `ShmToc` built over its base by `ShmToc::attach`.
struct WorkerAttached {
    /// Real base address of the mapped segment (`dsm_segment_address(seg)`); the
    /// registry key and the worker's `ExecShmToc` payload.
    base: usize,
    /// The real `DsmSegment` — held so the mapping outlives the worker call
    /// (C: no ResourceOwner, mapping survives to process exit).
    _seg_guard: DsmSegment,
    /// The real `shm_toc` attached over `base` (`shm_toc_attach`).
    toc: ShmToc,
}

thread_local! {
    /// Live worker-attached segments for this process. A worker normally has one;
    /// the test harness drives several sequentially, so this is a small `Vec`.
    static WORKER_ATTACHED: RefCell<Vec<WorkerAttached>> = const { RefCell::new(Vec::new()) };
}

/// `shm_toc_lookup(toc, key, no_error)` on the worker-attached segment. Resolves
/// the registered real `ShmToc` for `base` and performs a real in-segment lookup,
/// returning the chunk's real address (`0` when absent and `no_error`). Panics if
/// `base` is not a worker-attached segment (a programming error: the worker only
/// ever looks up the segment it just attached).
fn worker_with_toc<R>(base: usize, f: impl FnOnce(&ShmToc) -> R) -> R {
    WORKER_ATTACHED.with(|w| {
        let w = w.borrow();
        let entry = w
            .iter()
            .find(|e| e.base == base)
            .expect("worker shm_toc lookup on a segment that was not attached");
        f(&entry.toc)
    })
}

/// Whether `base` is the real DSM base address of a worker-attached segment.
/// Used by the shared `shm_toc_*` surface ([`with_toc`]) to dispatch worker-side
/// TOC access (toc == real DSM address) vs leader-side (toc == small
/// context-slot index) without a separate worker code path. Leader slot indices
/// are small integers and never collide with a mapped segment's base pointer.
fn is_worker_attached_base(base: usize) -> bool {
    WORKER_ATTACHED.with(|w| w.borrow().iter().any(|e| e.base == base))
}

// ===========================================================================
// Cursor / address model (family `shm-toc-address`): a `SerializeCursor` is now
// the *real* chunk address — the raw pointer `shm_toc_allocate`/`shm_toc_lookup`
// hands back, reinterpreted as `usize`. The typed handles (FixedStateHandle,
// InstrumentationHandle, JitInstrumentationHandle) are thin views carrying that
// same real address; the `repr(C)` payloads are written/read in place at it.
// ===========================================================================

/// shm_toc handle encodes the owning context slot directly (the execParallel
/// contract threads the same handle through every call so the leader-side
/// allocate/insert/lookup can find the real `ShmToc`).
fn toc_handle(slot: usize) -> ExecShmToc {
    ExecShmToc(slot)
}
fn toc_slot(toc: ExecShmToc) -> usize {
    toc.0
}

/// The leader-side `DsmSegmentHandle` carries the real [`DsmSegmentId`]: the
/// handle value *is* `DsmSegmentId::as_u64()` (opacity-inherited), and `0`
/// remains the NULL sentinel because `dsm-core` never hands out id `0`
/// (`DSM_NEXT_ID` starts at 1).
fn seg_handle_of(id: DsmSegmentId) -> DsmSegmentHandle {
    DsmSegmentHandle(id.as_u64() as usize)
}
// Used by the runtime test (and the worker-side follow-up family); the inverse
// of `seg_handle_of`, recovering the real id from the opacity-inherited handle.
#[cfg_attr(not(test), allow(dead_code))]
fn seg_id_of(seg: DsmSegmentHandle) -> DsmSegmentId {
    DsmSegmentId::from_u64(seg.0 as u64)
}

/// `shm_toc_lookup(toc, PARALLEL_KEY_FIXED, false)` missing — the corruption
/// path C handles with `elog(ERROR)`.
fn missing_fixed_key() -> types_error::PgError {
    ereport(ERROR)
        .errmsg("could not find fixed parallel state in shm TOC")
        .into_error()
}

/// `shm_toc_lookup(toc, PARALLEL_KEY_ERROR_QUEUE, false)` missing.
fn missing_error_queue_key() -> types_error::PgError {
    ereport(ERROR)
        .errmsg("could not find parallel error queue in shm TOC")
        .into_error()
}

/// `mul_size(s1, s2)` — checked multiplication (utils/shmem).
fn mul_size(s1: Size, s2: Size) -> PgResult<Size> {
    s1.checked_mul(s2).ok_or_else(|| {
        ereport(ERROR)
            .errmsg("requested shared memory size overflows size_t")
            .into_error()
    })
}

// ===========================================================================
// shm_toc estimate/allocate/insert/lookup (storage/ipc/shm_toc.c), operating on
// the context-owned buffer. These satisfy the execParallel seam contract.
// ===========================================================================

fn estimator_slot_of(_e: ShmTocEstimatorHandle) -> usize {
    _e.0
}

/// `shm_toc_estimate_chunk(&pcxt->estimator, sz)` — delegates to the real
/// `shm-toc` estimator inline. `space_for_chunks += BUFFERALIGN(sz)`; an
/// `add_size` overflow `ereport(ERROR)`s in C. The execParallel contract is
/// infallible here and the requests are bounded, so an overflow is a
/// programming error: panic loudly (matches the `ereport(ERROR)` non-return).
pub fn shm_toc_estimate_chunk(e: ShmTocEstimatorHandle, sz: Size) {
    with_globals(|g| {
        let c = g.get_mut(ParallelContextHandle(estimator_slot_of(e)));
        backend_storage_ipc_shm_toc::shm_toc_estimate_chunk(&mut c.estimator, sz)
            .expect("shm_toc_estimate_chunk overflow");
    });
}

/// `shm_toc_estimate_keys(&pcxt->estimator, nkeys)`.
pub fn shm_toc_estimate_keys(e: ShmTocEstimatorHandle, nkeys: i32) {
    with_globals(|g| {
        let c = g.get_mut(ParallelContextHandle(estimator_slot_of(e)));
        backend_storage_ipc_shm_toc::shm_toc_estimate_keys(&mut c.estimator, nkeys as Size)
            .expect("shm_toc_estimate_keys overflow");
    });
}

/// `shm_toc_estimate_chunk(&pcxt->estimator, size)` keyed directly by the
/// live `ParallelContext` — the execParallel-support contract surface used by
/// the per-node `Exec*Estimate` hooks. Fallible (`add_size` overflow
/// `ereport(ERROR)`s in C).
pub fn pcxt_estimate_chunk(pcxt: ParallelContextHandle, size: Size) -> PgResult<()> {
    with_globals(|g| {
        let c = g.get_mut(pcxt);
        backend_storage_ipc_shm_toc::shm_toc_estimate_chunk(&mut c.estimator, size)
    })
}

/// `shm_toc_estimate_keys(&pcxt->estimator, keys)` keyed directly by the live
/// `ParallelContext`.
pub fn pcxt_estimate_keys(pcxt: ParallelContextHandle, keys: Size) -> PgResult<()> {
    with_globals(|g| {
        let c = g.get_mut(pcxt);
        backend_storage_ipc_shm_toc::shm_toc_estimate_keys(&mut c.estimator, keys)
    })
}

/// Run `f` on the live real `ShmToc` and segment base for the context owning
/// `toc`. Panics on a `toc` for a context whose DSM has not been created
/// (a programming error — `shm_toc_*` is never called before the segment is
/// established, mirroring C's `shm_toc_allocate(pcxt->toc, ...)` after
/// `shm_toc_create`).
fn with_toc<R>(toc: ExecShmToc, f: impl FnOnce(&ShmToc, usize) -> R) -> R {
    // A worker holds no `ParallelContext`: its `ExecShmToc.0` is the real DSM
    // segment base address it attached to (registered in `WORKER_ATTACHED`), not a
    // leader-side context slot index. The shared execParallel code calls this same
    // `shm_toc_*` surface in both processes, so resolve the worker-attached
    // segment first (keyed by the exact base); leader slot indices are small and
    // never collide with a mapped segment's base pointer.
    if is_worker_attached_base(toc.0) {
        return worker_with_toc(toc.0, |real| f(real, toc.0));
    }
    with_globals(|g| {
        let c = g.get(ParallelContextHandle(toc_slot(toc)));
        let real = c.toc.expect("shm_toc not yet created for parallel context");
        f(&real, c.toc_base)
    })
}

/// `shm_toc_allocate(toc, nbytes)` — delegate to the real `ShmToc`; return the
/// chunk's *real address* as a `SerializeCursor` (the raw pointer reinterpreted
/// as `usize`).
///
/// The real `shm_toc_allocate` `ereport(ERROR)`s ("out of shared memory") on
/// exhaustion; the segment is sized exactly by `shm_toc_estimate`, so this
/// cannot happen in correct operation. The execParallel contract is infallible,
/// so an allocation failure is a programming error: panic loudly (matches the
/// `ereport(ERROR)` non-return).
pub fn shm_toc_allocate(toc: ExecShmToc, nbytes: Size) -> SerializeCursor {
    with_toc(toc, |real, _base| {
        let ptr = real
            .allocate(nbytes)
            .expect("shm_toc_allocate out of shared memory");
        SerializeCursor(ptr.as_ptr() as usize)
    })
}

/// `shm_toc_insert(toc, key, address)` — register the chunk's real address in
/// the real in-segment entry table.
pub fn shm_toc_insert(toc: ExecShmToc, key: u64, address: SerializeCursor) {
    with_toc(toc, |real, _base| {
        let addr =
            NonNull::new(address.0 as *mut u8).expect("shm_toc chunk address is non-null");
        // SAFETY: `addr` is a chunk previously handed out by `real.allocate`,
        // so it lies within the segment, strictly past the TOC start.
        unsafe { real.insert(key, addr) }.expect("shm_toc_insert out of shared memory");
    });
}

/// `shm_toc_lookup(toc, key, noError)` — `None` when `noError` and absent. The
/// chunk's real address is returned as a `SerializeCursor`.
pub fn shm_toc_lookup(toc: ExecShmToc, key: u64, no_error: bool) -> Option<SerializeCursor> {
    with_toc(toc, |real, _base| {
        // shm_toc_lookup elog(ERROR) on a missing required key. The execParallel
        // contract is infallible, so a missing required key is a programming
        // error: the `?`-less `expect` matches elog(ERROR) which never returns.
        let found = real
            .lookup(key, no_error)
            .expect("shm_toc_lookup on missing required key");
        found.map(|ptr| SerializeCursor(ptr.as_ptr() as usize))
    })
}

// ===========================================================================
// Typed DSM chunk stores/loads. The chunk address IS the real in-segment
// pointer (a `SerializeCursor`/`*Handle` carries the raw address): the `repr(C)`
// payloads are written and read back IN PLACE at it, exactly as C dereferences
// `(FixedParallelExecutorState *) shm_toc_allocate(...)`. No side tables.
// ===========================================================================

// SAFETY contract shared by every accessor below: the address inside a
// `SerializeCursor`/`FixedStateHandle`/`InstrumentationHandle`/
// `JitInstrumentationHandle` is a chunk previously handed out by the real
// `shm_toc_allocate` (or recovered via `shm_toc_lookup`), so it points at
// `>= sizeof(payload)` writable, suitably-aligned bytes inside the mapped DSM
// (or private-memory) segment, live for as long as the owning `ParallelContext`
// holds the segment. The execParallel contract never resurrects a handle past
// `DestroyParallelContext`. Chunks come from `BUFFERALIGN`ed `shm_toc_allocate`,
// which over-aligns relative to these structs' natural alignment.

/// `fpes = shm_toc_allocate(...); *fpes = state;` — write the `repr(C)`
/// `FixedParallelExecutorState` in place at the chunk address.
pub fn store_fixed_state(chunk: SerializeCursor, state: FixedParallelExecutorState) -> FixedStateHandle {
    let p = chunk.0 as *mut FixedParallelExecutorState;
    // SAFETY: see the module SAFETY contract above.
    unsafe { p.write_unaligned(state) };
    FixedStateHandle(chunk.0)
}

/// `fpes = (FixedParallelExecutorState *) chunk` — reinterpret an existing chunk.
pub fn fixed_state_from_chunk(chunk: SerializeCursor) -> FixedStateHandle {
    FixedStateHandle(chunk.0)
}

fn read_fixed_state(fpes: FixedStateHandle) -> FixedParallelExecutorState {
    let p = fpes.0 as *const FixedParallelExecutorState;
    // SAFETY: see the module SAFETY contract above.
    unsafe { p.read_unaligned() }
}

pub fn set_fixed_param_exec(fpes: FixedStateHandle, dp: u64) {
    let mut st = read_fixed_state(fpes);
    st.param_exec = dp;
    let p = fpes.0 as *mut FixedParallelExecutorState;
    // SAFETY: see the module SAFETY contract above.
    unsafe { p.write_unaligned(st) };
}
pub fn fixed_param_exec(fpes: FixedStateHandle) -> u64 {
    read_fixed_state(fpes).param_exec
}
pub fn fixed_eflags(fpes: FixedStateHandle) -> i32 {
    read_fixed_state(fpes).eflags
}
pub fn fixed_jit_flags(fpes: FixedStateHandle) -> i32 {
    read_fixed_state(fpes).jit_flags
}
pub fn fixed_tuples_needed(fpes: FixedStateHandle) -> i64 {
    read_fixed_state(fpes).tuples_needed
}

/// `memcpy(chunk, value, strlen(value) + 1)` — copy the NUL-terminated string
/// into the chunk. The chunk was sized `value.len() + 1` by the caller.
pub fn store_cstring(chunk: SerializeCursor, value: String) {
    let bytes = value.as_bytes();
    let dst = chunk.0 as *mut u8;
    // SAFETY: see the module SAFETY contract above; the chunk was allocated with
    // `value.len() + 1` bytes so the body plus the trailing NUL fit.
    unsafe {
        core::ptr::copy_nonoverlapping(bytes.as_ptr(), dst, bytes.len());
        dst.add(bytes.len()).write(0);
    }
}

/// Read the NUL-terminated string back out of a chunk (`pstrdup(chunk)`).
pub fn cursor_cstring(chunk: SerializeCursor) -> PgResult<String> {
    let p = chunk.0 as *const u8;
    // SAFETY: see the module SAFETY contract above; `store_cstring` (or the C
    // leader) wrote a NUL-terminated string here.
    let mut len = 0usize;
    unsafe {
        while p.add(len).read() != 0 {
            len += 1;
        }
        let slice = core::slice::from_raw_parts(p, len);
        String::from_utf8(slice.to_vec()).map_err(|_| {
            ereport(ERROR)
                .errmsg("invalid UTF-8 string stored at parallel DSM chunk")
                .into_error()
        })
    }
}

/// Write `"library\0function\0"` into the entrypoint chunk in place. Mirrors
/// `strcpy(entrypointstate, library_name); strcpy(entrypointstate + lnamelen +
/// 1, function_name)` (parallel.c:488-492). The chunk was sized
/// `library.len() + function.len() + 2`.
fn write_entrypoint(chunk: SerializeCursor, library: &str, function: &str) {
    let dst = chunk.0 as *mut u8;
    let lname = library.as_bytes();
    let fname = function.as_bytes();
    // SAFETY: see the module SAFETY contract above; the chunk was allocated with
    // `lname.len() + fname.len() + 2` bytes so both strings plus their NULs fit.
    unsafe {
        core::ptr::copy_nonoverlapping(lname.as_ptr(), dst, lname.len());
        dst.add(lname.len()).write(0);
        let f = dst.add(lname.len() + 1);
        core::ptr::copy_nonoverlapping(fname.as_ptr(), f, fname.len());
        f.add(fname.len()).write(0);
    }
}

/// Read back the two NUL-terminated strings from an entrypoint chunk. Mirrors
/// `library_name = entrypointstate; function_name = entrypointstate +
/// strlen(library_name) + 1` (parallel.c:1416-1418).
fn read_entrypoint(chunk: SerializeCursor) -> PgResult<(String, String)> {
    let library = cursor_cstring(chunk)?;
    let function = cursor_cstring(SerializeCursor(chunk.0 + library.len() + 1))?;
    Ok((library, function))
}

/// Offset of the `plan_node_id` flexible array past the
/// `SharedExecutorInstrumentation` header (the four leading `int`s).
const SEI_PLAN_NODE_ID_OFFSET: usize =
    types_execparallel::SHARED_EXEC_INSTRUMENTATION_HEADER_SIZE;

/// `instrumentation = shm_toc_allocate(...); instrumentation->{...} = ...` —
/// write the `repr(C)` `SharedExecutorInstrumentation` header in place. The
/// trailing `plan_node_id` array and the `Instrumentation` slots are written
/// separately into the same chunk (`set_sei_plan_node_id` / the `instr_*`
/// support seams), matching C's writes past the header.
pub fn store_instrumentation_header(
    chunk: SerializeCursor,
    header: SharedExecutorInstrumentation,
) -> InstrumentationHandle {
    let p = chunk.0 as *mut SharedExecutorInstrumentation;
    // SAFETY: see the module SAFETY contract above.
    unsafe { p.write_unaligned(header) };
    InstrumentationHandle(chunk.0)
}

/// `instrumentation = (SharedExecutorInstrumentation *) chunk`.
pub fn instrumentation_from_chunk(chunk: SerializeCursor) -> InstrumentationHandle {
    InstrumentationHandle(chunk.0)
}

fn read_sei_header(sei: InstrumentationHandle) -> SharedExecutorInstrumentation {
    let p = sei.0 as *const SharedExecutorInstrumentation;
    // SAFETY: see the module SAFETY contract above.
    unsafe { p.read_unaligned() }
}

pub fn sei_instrument_options(sei: InstrumentationHandle) -> i32 {
    read_sei_header(sei).instrument_options
}
pub fn sei_num_workers(sei: InstrumentationHandle) -> i32 {
    read_sei_header(sei).num_workers
}
pub fn sei_num_plan_nodes(sei: InstrumentationHandle) -> i32 {
    read_sei_header(sei).num_plan_nodes
}
/// `sei->plan_node_id[index]` — the flexible array immediately follows the header.
pub fn sei_plan_node_id(sei: InstrumentationHandle, index: i32) -> i32 {
    let p = (sei.0 + SEI_PLAN_NODE_ID_OFFSET) as *const i32;
    // SAFETY: see the module SAFETY contract above; `index < num_plan_nodes`,
    // and the chunk was sized `header + num_plan_nodes * sizeof(int) + ...`.
    unsafe { p.add(index as usize).read_unaligned() }
}
/// `sei->plan_node_id[index] = value`.
pub fn set_sei_plan_node_id(sei: InstrumentationHandle, index: i32, value: i32) {
    let p = (sei.0 + SEI_PLAN_NODE_ID_OFFSET) as *mut i32;
    // SAFETY: see the module SAFETY contract above; `index < num_plan_nodes`.
    unsafe { p.add(index as usize).write_unaligned(value) };
}

/// `jit_instrumentation = shm_toc_allocate(...);
/// jit_instrumentation->num_workers = num_workers; memset(jit_instr, 0, ...)`.
/// The `SharedJitInstrumentation` header is a single leading `int num_workers`,
/// followed (at `offsetof(.., jit_instr)`) by `num_workers` zeroed
/// `JitInstrumentation` objects. The caller sized the chunk for both; the
/// `jit_instr` array is left zeroed (chunk space is not pre-zeroed, so zero it).
pub fn store_jit_instrumentation_header(
    chunk: SerializeCursor,
    num_workers: i32,
) -> JitInstrumentationHandle {
    // Write num_workers, then zero the jit_instr array region. The header is one
    // int; the JitInstrumentation array starts at MAXALIGN(sizeof(int)).
    let p = chunk.0 as *mut i32;
    // SAFETY: see the module SAFETY contract above.
    unsafe { p.write_unaligned(num_workers) };
    let arr_off = jit_instr_offset();
    let arr = (chunk.0 + arr_off) as *mut u8;
    let arr_len = (num_workers as usize)
        .saturating_mul(core::mem::size_of::<types_execparallel::JitInstrumentation>());
    // SAFETY: the chunk was sized `arr_off + num_workers * sizeof(JitInstrumentation)`.
    unsafe { core::ptr::write_bytes(arr, 0, arr_len) };
    JitInstrumentationHandle(chunk.0)
}

/// `offsetof(SharedJitInstrumentation, jit_instr)` — one leading `int`,
/// MAXALIGNed (jit/jit.h: `int num_workers; JitInstrumentation jit_instr[];`).
fn jit_instr_offset() -> usize {
    const MAXIMUM_ALIGNOF: usize = 8;
    (core::mem::size_of::<i32>() + (MAXIMUM_ALIGNOF - 1)) & !(MAXIMUM_ALIGNOF - 1)
}

/// `jit_instrumentation = (SharedJitInstrumentation *) chunk`.
pub fn jit_instrumentation_from_chunk(chunk: SerializeCursor) -> JitInstrumentationHandle {
    JitInstrumentationHandle(chunk.0)
}

/// `shared_jit->num_workers`.
pub fn shared_jit_num_workers(shared_jit: JitInstrumentationHandle) -> i32 {
    let p = shared_jit.0 as *const i32;
    // SAFETY: see the module SAFETY contract above.
    unsafe { p.read_unaligned() }
}

// ===========================================================================
// Accessor seams on the live ParallelContext (execParallel reads pcxt->field).
// ===========================================================================

pub fn pcxt_nworkers(pcxt: ParallelContextHandle) -> i32 {
    with_globals(|g| g.get(pcxt).nworkers)
}
pub fn pcxt_nworkers_launched(pcxt: ParallelContextHandle) -> i32 {
    with_globals(|g| g.get(pcxt).nworkers_launched)
}
pub fn pcxt_nworkers_to_launch(pcxt: ParallelContextHandle) -> i32 {
    with_globals(|g| g.get(pcxt).nworkers_to_launch)
}
pub fn pcxt_estimator(pcxt: ParallelContextHandle) -> ShmTocEstimatorHandle {
    // The estimator is part of the context; address it by the same slot.
    ShmTocEstimatorHandle(pcxt.0)
}
pub fn pcxt_toc(pcxt: ParallelContextHandle) -> ExecShmToc {
    toc_handle(pcxt.0)
}
pub fn pcxt_seg(pcxt: ParallelContextHandle) -> Option<ExecDsmSeg> {
    with_globals(|g| {
        let c = g.get(pcxt);
        if c.seg.is_null() {
            None
        } else {
            // The execParallel-visible `DsmSegmentHandle` carries the same real
            // id value (no contract change; both are the segment's identity).
            Some(ExecDsmSeg(c.seg.0))
        }
    })
}
/// `pcxt->worker[i].bgwhandle` for a *launched* worker (`i <
/// nworkers_launched`), which always has a live handle (C reads it
/// unconditionally in `ExecParallelCreateReaders`). Returns the real value
/// handle.
pub fn pcxt_worker_bgwhandle(pcxt: ParallelContextHandle, i: i32) -> BackgroundWorkerHandle {
    with_globals(|g| {
        g.get(pcxt).worker[i as usize]
            .bgwhandle
            .expect("launched parallel worker has a live bgwhandle")
    })
}
pub fn make_parallel_worker_context(seg: ExecDsmSeg, toc: ExecShmToc) -> ParallelWorkerContextHandle {
    // {seg, toc} pair handed to per-node Exec*InitializeWorker hooks; encode the
    // toc slot (both share the context identity).
    let _ = seg;
    ParallelWorkerContextHandle(toc.0)
}
pub fn pwcxt_toc(pwcxt: ParallelWorkerContextHandle) -> ExecShmToc {
    // `make_parallel_worker_context` encoded the worker context as the toc slot
    // (`ParallelWorkerContextHandle(toc.0)`); recover it symmetrically. Mirrors
    // C `pwcxt->toc`.
    toc_handle(pwcxt.0)
}
pub fn pwcxt_seg(pwcxt: ParallelWorkerContextHandle) -> ExecDsmSeg {
    // The worker context shares the parallel context's slot identity (its toc,
    // seg and estimator are all addressed by the same slot — see
    // `make_parallel_worker_context`), so the segment is the context's `seg`.
    // Mirrors C `pwcxt->seg`.
    with_globals(|g| ExecDsmSeg(g.get(ParallelContextHandle(pwcxt.0)).seg.0))
}
pub fn parallel_worker_number() -> i32 {
    with_globals(|g| g.parallel_worker_number)
}

/// Accumulate a finishing parallel worker's local index-scan search count into
/// its slot of the shared `SharedIndexScanInstrumentation`. Mirrors the
/// `ExecEndIndex(Only)Scan` parallel-worker path:
/// `winstrument[ParallelWorkerNumber].nsearches += nsearches`. The slot is
/// picked here because `ParallelWorkerNumber` is parallel.c's per-backend
/// global (owned by this crate).
pub fn accumulate_shared_index_searches(
    shared_info: &mut types_nodes::SharedIndexScanInstrumentation,
    nsearches: u64,
) {
    let worker = with_globals(|g| g.parallel_worker_number);
    shared_info.winstrument[worker as usize].nsearches += nsearches;
}

// ===========================================================================
// IsParallelWorker / ParallelMessagePending / signal interrupt (the small seams
// declared in backend-access-transam-parallel-seams).
// ===========================================================================

/// `IsParallelWorker()` (access/parallel.h:60) — `ParallelWorkerNumber >= 0`.
pub fn is_parallel_worker() -> bool {
    with_globals(|g| g.parallel_worker_number) >= 0
}

/// `InitializingParallelWorker` (access/parallel.c) — true while a parallel
/// worker is in `ParallelWorkerMain` initialization.
pub fn initializing_parallel_worker() -> bool {
    with_globals(|g| g.initializing_parallel_worker)
}

/// Assign `ParallelWorkerNumber`.
fn set_parallel_worker_number(value: i32) {
    with_globals(|g| g.parallel_worker_number = value);
}

fn set_parallel_message_pending(value: bool) {
    PARALLEL_MESSAGE_PENDING.with(|f| f.set(value));
}

/// Read `ParallelMessagePending` (parallel.c) — set in
/// `HandleParallelMessageInterrupt` and read by `ProcessInterrupts`
/// (tcop/postgres.c) to gate the `ProcessParallelMessages()` call.
pub fn parallel_message_pending() -> bool {
    PARALLEL_MESSAGE_PENDING.with(|f| f.get())
}

/// `HandleParallelMessageInterrupt()` (parallel.c:1043-1049). In C this runs in
/// a signal handler: `InterruptPending = true; ParallelMessagePending = true;
/// SetLatch(MyLatch);`. The interrupt-pending flip and latch set go through
/// their seams; the local flag is set directly.
pub fn handle_parallel_message_interrupt() {
    // set_interrupt_pending / set_my_latch are infallible flag/latch operations
    // here; the C does not error. Errors would be a wiring bug, so surface them.
    rt::set_interrupt_pending::call().expect("set InterruptPending");
    set_parallel_message_pending(true);
    rt::set_my_latch::call().expect("SetLatch(MyLatch)");
}

// ===========================================================================
// CreateParallelContext (parallel.c:172-203).
// ===========================================================================

/// Establish a new parallel context, returning its handle (C's `palloc0`'d
/// `ParallelContext *`). Linked into `pcxt_list`.
pub fn create_parallel_context<'mcx>(
    mcx: Mcx<'mcx>,
    library_name: String,
    function_name: String,
    nworkers: i32,
) -> PgResult<ParallelContextHandle> {
    // It is unsafe to create a parallel context if not in parallel mode.
    debug_assert!(nworkers >= 0);

    // C: pcxt = palloc0(sizeof(ParallelContext)), in TopTransactionContext.
    // Account the allocation against the caller's context so OOM carries
    // ERRCODE_OUT_OF_MEMORY and the context name (mcx.oom), like every other
    // allocating function (AGENTS "Memory allocation").
    mcx::check_alloc_size(core::mem::size_of::<ParallelContext>())?;

    // We might be running in a short-lived memory context.
    let oldcontext = rt::switch_to_top_transaction_context::call()?;

    let pcxt = ParallelContext {
        subid: rt::get_current_subtransaction_id::call()?,
        nworkers,
        nworkers_to_launch: nworkers,
        nworkers_launched: 0,
        library_name,
        function_name,
        error_context_stack: rt::error_context_stack::call()?,
        estimator: shm_toc_estimator::default(),
        seg: DsmSegmentHandle::NULL,
        seg_guard: None,
        private_memory: None,
        toc: None,
        toc_base: 0,
        worker: Vec::new(),
        nknown_attached_workers: 0,
        known_attached_workers: Vec::new(),
    };

    let h = with_globals(|g| g.push_head(mcx, pcxt))?;

    // Restore previous memory context.
    rt::memory_context_switch_back::call(oldcontext)?;

    Ok(h)
}

// ===========================================================================
// InitializeParallelDSM (parallel.c:210-501).
// ===========================================================================

/// Alignment for the no-worker private buffer: it backs an `InSegmentShmToc`
/// header (laid out at the buffer start by `ShmToc::create`), so it must be at
/// least as aligned as that header. The atomic-safety note in `shm_toc.c` wants
/// `BUFFERALIGN` (`ALIGNOF_BUFFER`, 32); a real `dsm_segment_address` is page
/// aligned, so we match that floor for the private fallback too.
const PRIVATE_SEG_ALIGN: usize = 64;

/// Create the DSM segment (or no-worker private buffer) backing `pcxt`'s
/// `shm_toc`, build the real [`ShmToc`] over its base, and record the segment
/// id / toc / base in the context. Mirrors the
/// `segsize = shm_toc_estimate(...)` ... `shm_toc_create(PARALLEL_MAGIC, base,
/// segsize)` block of `InitializeParallelDSM` (parallel.c:325-339).
///
/// `segsize` is the `shm_toc_estimate` total. When the context budgets workers
/// we try the real `dsm_create(segsize, DSM_CREATE_NULL_IF_MAXSEGMENTS)`; if it
/// returns `None` (max segments) we fall back to backend-private memory and
/// plan for no workers, exactly like the C.
fn establish_parallel_segment(
    pcxt: ParallelContextHandle,
    segsize: Size,
) -> PgResult<()> {
    // dsm_create allocates its descriptor in TopMemoryContext (the C global);
    // the descriptor outlives this (possibly short-lived) caller context.
    let top = backend_utils_mmgr_mcxt_seams::top_memory_context::call();

    let seg: Option<DsmSegment> = if pcxt_nworkers(pcxt) > 0 {
        dsm_create(segsize, DSM_CREATE_NULL_IF_MAXSEGMENTS, top)?
    } else {
        None
    };

    match seg {
        Some(seg) => {
            // The TOC lives in the real DSM segment.
            let id = seg.id();
            let base = dsm_segment_address(id);
            debug_assert!(!base.is_null(), "dsm_segment_address returned NULL");
            let base_nn = NonNull::new(base).expect("dsm segment base is non-null");
            // SAFETY: `base` addresses `>= segsize` writable, page-aligned bytes
            // of the freshly created segment, which outlives the toc handle
            // (held in the same context as the `DsmSegment` guard).
            let toc = unsafe { ShmToc::create(PARALLEL_MAGIC, base_nn, segsize) };
            with_globals(|g| {
                let c = g.get_mut(pcxt);
                c.seg = seg_handle_of(id);
                c.seg_guard = Some(seg);
                c.toc = Some(toc);
                c.toc_base = base as usize;
            });
        }
        None => {
            // No workers (or max segments hit): use backend-private memory.
            // C: pcxt->private_memory = MemoryContextAlloc(TopMemoryContext,
            // segsize). Fallible: OOM on the caller-controlled segsize carries
            // ERRCODE_OUT_OF_MEMORY (the context's oom), not a process abort.
            mcx::check_alloc_size(segsize)?;
            let layout = Layout::from_size_align(segsize, PRIVATE_SEG_ALIGN)
                .expect("valid private-segment layout");
            let ptr = top
                .allocate(layout)
                .map_err(|_| top.oom(segsize))?
                .cast::<u8>();
            // MemoryContextAlloc does not zero; shm_toc_create writes the header
            // and the chunks are filled before use, matching the C.
            let base_nn = ptr;
            // SAFETY: `ptr` is a fresh `segsize`-byte allocation aligned to
            // PRIVATE_SEG_ALIGN (>= the header alignment), live until the
            // context frees it in DestroyParallelContext.
            let toc = unsafe { ShmToc::create(PARALLEL_MAGIC, base_nn, segsize) };
            with_globals(|g| {
                let c = g.get_mut(pcxt);
                c.nworkers = 0;
                c.private_memory = Some(PrivateSeg { ptr, layout });
                c.toc = Some(toc);
                c.toc_base = ptr.as_ptr() as usize;
            });
        }
    }
    Ok(())
}

/// Collect the leader's session state for the DSM `FixedParallelState`
/// (`InitializeParallelDSM`, parallel.c:343-358). Every field a worker restores
/// in `ParallelWorkerMain` (`RestoreFixedParallelState`'s reads) is gathered
/// here off the leader's backend globals, each through its owning subsystem's
/// accessor seam. `parallel_leader_pgproc` is a process-local pointer with no
/// cross-process meaning in this repo, so it stays 0 (parallel.c uses
/// `MyProc`); the leader's identity travels in `parallel_leader_proc_number`.
/// `last_xlog_end` and the spinlock are initialized by `fps_init`, not here.
fn collect_fixed_parallel_state() -> PgResult<FixedParallelState> {
    let (current_user_id, sec_context) =
        backend_commands_matview_deps_seams::get_user_id_and_sec_context::call()?;
    let (temp_namespace_id, temp_toast_namespace_id) =
        backend_catalog_namespace_seams::get_temp_namespace_state::call();
    Ok(FixedParallelState {
        database_id: backend_utils_init_small_seams::my_database_id::call(),
        authenticated_user_id: backend_commands_variable_seams::get_authenticated_user_id::call(),
        session_user_id: backend_commands_user_seams::get_session_user_id::call()?,
        outer_user_id: backend_commands_variable_seams::get_current_role_id::call(),
        current_user_id,
        temp_namespace_id,
        temp_toast_namespace_id,
        sec_context,
        session_user_is_superuser:
            backend_commands_variable_seams::get_session_user_is_superuser::call(),
        role_is_superuser: backend_commands_variable_seams::current_role_is_superuser::call(),
        parallel_leader_pgproc: 0,
        parallel_leader_pid: backend_utils_init_small_seams::my_proc_pid::call(),
        parallel_leader_proc_number: backend_storage_lmgr_proc_seams::my_proc_number::call(),
        xact_ts: backend_access_transam_xact_seams::get_current_transaction_start_timestamp::call(),
        stmt_ts: backend_access_transam_xact_seams::get_current_statement_start_timestamp::call(),
        serializable_xact_handle:
            backend_storage_lmgr_predicate_seams::share_serializable_xact::call(),
        last_xlog_end: 0,
    })
}

/// Establish the dynamic shared memory segment for a parallel context and copy
/// state needed by parallel workers into it.
pub fn initialize_parallel_dsm<'mcx>(mcx: Mcx<'mcx>, pcxt: ParallelContextHandle) -> PgResult<()> {
    let mut library_len: Size = 0;
    let mut guc_len: Size = 0;
    let mut combocidlen: Size = 0;
    let mut tsnaplen: Size = 0;
    let mut asnaplen: Size = 0;
    let mut tstatelen: Size = 0;
    let mut pendingsyncslen: Size = 0;
    let mut reindexlen: Size = 0;
    let mut relmapperlen: Size = 0;
    let mut uncommittedenumslen: Size = 0;
    let mut clientconninfolen: Size = 0;
    let mut session_dsm_handle: dsm_handle = DSM_HANDLE_INVALID;
    let transaction_snapshot = rt::get_transaction_snapshot::call()?;
    let active_snapshot = rt::get_active_snapshot::call()?;
    // Borrowed for estimate/serialize below — the owned `SnapshotData` values
    // live until the DSM serialization is complete.

    // We might be running in a very short-lived memory context.
    let oldcontext = rt::switch_to_top_transaction_context::call()?;

    let est = pcxt_estimator(pcxt);

    // Allow space to store the fixed-size parallel state.
    shm_toc_estimate_chunk(est, core::mem::size_of::<FixedParallelState>());
    shm_toc_estimate_keys(est, 1);

    // If non-interruptible, it's unsafe to launch workers; pretend none.
    if !rt::interrupts_can_be_processed::call() {
        with_globals(|g| g.get_mut(pcxt).nworkers = 0);
    }

    // Normally the user requested at least one worker.
    if pcxt_nworkers(pcxt) > 0 {
        session_dsm_handle = backend_access_common_session_seams::get_session_dsm_handle::call()?;
        if session_dsm_handle == DSM_HANDLE_INVALID {
            with_globals(|g| g.get_mut(pcxt).nworkers = 0);
        }
    }

    if pcxt_nworkers(pcxt) > 0 {
        // Estimate space for various kinds of state sharing.
        library_len = rt::estimate_library_state_space::call()?;
        shm_toc_estimate_chunk(est, library_len);
        guc_len = rt::estimate_guc_state_space::call()?;
        shm_toc_estimate_chunk(est, guc_len);
        combocidlen = rt::estimate_combocid_state_space::call()?;
        shm_toc_estimate_chunk(est, combocidlen);
        if rt::isolation_uses_xact_snapshot::call() {
            tsnaplen = rt::estimate_snapshot_space::call(&transaction_snapshot)?;
            shm_toc_estimate_chunk(est, tsnaplen);
        }
        asnaplen = rt::estimate_snapshot_space::call(&active_snapshot)?;
        shm_toc_estimate_chunk(est, asnaplen);
        tstatelen = rt::estimate_transaction_state_space::call()?;
        shm_toc_estimate_chunk(est, tstatelen);
        shm_toc_estimate_chunk(est, core::mem::size_of::<dsm_handle>());
        pendingsyncslen = rt::estimate_pending_syncs_space::call()?;
        shm_toc_estimate_chunk(est, pendingsyncslen);
        reindexlen = rt::estimate_reindex_state_space::call()?;
        shm_toc_estimate_chunk(est, reindexlen);
        relmapperlen = rt::estimate_relation_map_space::call()?;
        shm_toc_estimate_chunk(est, relmapperlen);
        uncommittedenumslen = rt::estimate_uncommitted_enums_space::call()?;
        shm_toc_estimate_chunk(est, uncommittedenumslen);
        clientconninfolen = rt::estimate_client_connection_info_space::call()?;
        shm_toc_estimate_chunk(est, clientconninfolen);
        // If you add more chunks here, you probably need to add keys.
        shm_toc_estimate_keys(est, 12);

        // Estimate space for error queues.
        let nworkers = pcxt_nworkers(pcxt);
        shm_toc_estimate_chunk(est, mul_size(PARALLEL_ERROR_QUEUE_SIZE, nworkers as Size)?);
        shm_toc_estimate_keys(est, 1);

        // Estimate space for entrypoint info ("library\0function\0").
        let (lname_len, fname_len) =
            with_globals(|g| (g.get(pcxt).library_name.len(), g.get(pcxt).function_name.len()));
        shm_toc_estimate_chunk(est, lname_len + fname_len + 2);
        shm_toc_estimate_keys(est, 1);
    }

    // Create DSM and initialize with a new table of contents. But if the user
    // didn't request any workers, just use backend-private memory; also fall
    // back to private memory (and no workers) if dsm_create hits the
    // max-segments limit. segsize is the full shm_toc_estimate total (TOC
    // header + entry array + chunk space), not just the chunk space.
    let segsize = with_globals(|g| shm_toc_estimate(&g.get(pcxt).estimator))?;
    establish_parallel_segment(pcxt, segsize)?;

    let toc = pcxt_toc(pcxt);

    // Initialize fixed-size state in shared memory.
    let fps = shm_toc_allocate(toc, core::mem::size_of::<FixedParallelState>());
    {
        let init = collect_fixed_parallel_state()?;
        rt::fps_init::call(fps.0, init)?;
    }
    shm_toc_insert(toc, PARALLEL_KEY_FIXED, fps);

    // Skip the rest if not budgeting for workers.
    if pcxt_nworkers(pcxt) > 0 {
        // Serialize shared libraries we have loaded.
        let libraryspace = shm_toc_allocate(toc, library_len);
        rt::serialize_library_state::call(library_len, libraryspace.0)?;
        shm_toc_insert(toc, PARALLEL_KEY_LIBRARY, libraryspace);

        // Serialize GUC settings.
        let gucspace = shm_toc_allocate(toc, guc_len);
        rt::serialize_guc_state::call(guc_len, gucspace.0)?;
        shm_toc_insert(toc, PARALLEL_KEY_GUC, gucspace);

        // Serialize combo CID state.
        let combocidspace = shm_toc_allocate(toc, combocidlen);
        rt::serialize_combocid_state::call(combocidlen, combocidspace.0)?;
        shm_toc_insert(toc, PARALLEL_KEY_COMBO_CID, combocidspace);

        // Serialize the transaction snapshot if the isolation level uses one.
        if rt::isolation_uses_xact_snapshot::call() {
            let tsnapspace = shm_toc_allocate(toc, tsnaplen);
            rt::serialize_snapshot::call(&transaction_snapshot, tsnapspace.0)?;
            shm_toc_insert(toc, PARALLEL_KEY_TRANSACTION_SNAPSHOT, tsnapspace);
        }

        // Serialize the active snapshot.
        let asnapspace = shm_toc_allocate(toc, asnaplen);
        rt::serialize_snapshot::call(&active_snapshot, asnapspace.0)?;
        shm_toc_insert(toc, PARALLEL_KEY_ACTIVE_SNAPSHOT, asnapspace);

        // Provide the handle for per-session segment.
        let session_dsm_handle_space = shm_toc_allocate(toc, core::mem::size_of::<dsm_handle>());
        rt::write_dsm_handle::call(session_dsm_handle_space.0, session_dsm_handle)?;
        shm_toc_insert(toc, PARALLEL_KEY_SESSION_DSM, session_dsm_handle_space);

        // Serialize transaction state.
        let tstatespace = shm_toc_allocate(toc, tstatelen);
        rt::serialize_transaction_state::call(tstatelen, tstatespace.0)?;
        shm_toc_insert(toc, PARALLEL_KEY_TRANSACTION_STATE, tstatespace);

        // Serialize pending syncs.
        let pendingsyncsspace = shm_toc_allocate(toc, pendingsyncslen);
        rt::serialize_pending_syncs::call(pendingsyncslen, pendingsyncsspace.0)?;
        shm_toc_insert(toc, PARALLEL_KEY_PENDING_SYNCS, pendingsyncsspace);

        // Serialize reindex state.
        let reindexspace = shm_toc_allocate(toc, reindexlen);
        rt::serialize_reindex_state::call(reindexlen, reindexspace.0)?;
        shm_toc_insert(toc, PARALLEL_KEY_REINDEX_STATE, reindexspace);

        // Serialize relmapper state.
        let relmapperspace = shm_toc_allocate(toc, relmapperlen);
        rt::serialize_relation_map::call(relmapperlen, relmapperspace.0)?;
        shm_toc_insert(toc, PARALLEL_KEY_RELMAPPER_STATE, relmapperspace);

        // Serialize uncommitted enum state.
        let uncommittedenumsspace = shm_toc_allocate(toc, uncommittedenumslen);
        rt::serialize_uncommitted_enums::call(uncommittedenumsspace.0, uncommittedenumslen)?;
        shm_toc_insert(toc, PARALLEL_KEY_UNCOMMITTEDENUMS, uncommittedenumsspace);

        // Serialize our ClientConnectionInfo.
        let clientconninfospace = shm_toc_allocate(toc, clientconninfolen);
        rt::serialize_client_connection_info::call(clientconninfolen, clientconninfospace.0)?;
        shm_toc_insert(toc, PARALLEL_KEY_CLIENTCONNINFO, clientconninfospace);

        // Allocate space for worker information (palloc0). Fallible: OOM goes
        // through the context's mcx.oom (ERRCODE_OUT_OF_MEMORY + context name),
        // not a hand-rolled "out of memory" ereport that drops the SQLSTATE.
        let nworkers = pcxt_nworkers(pcxt);
        let seg = with_globals(|g| g.get(pcxt).seg);
        let request = (nworkers as usize)
            .saturating_mul(core::mem::size_of::<ParallelWorkerInfo>());
        mcx::check_alloc_size(request)?;
        let mut workers: Vec<ParallelWorkerInfo> = Vec::new();
        workers
            .try_reserve(nworkers as usize)
            .map_err(|_| mcx.oom(request))?;
        for _ in 0..nworkers {
            workers.push(ParallelWorkerInfo::new());
        }

        // Establish error queues in dynamic shared memory.
        let error_queue_space =
            shm_toc_allocate(toc, mul_size(PARALLEL_ERROR_QUEUE_SIZE, nworkers as Size)?);
        let mut i = 0;
        while i < nworkers {
            // C: mq = shm_mq_create(error_queue_space + i*SIZE, SIZE);
            //    shm_mq_set_receiver(mq, MyProc);
            //    pcxt->worker[i].error_mqh = shm_mq_attach(mq, pcxt->seg, NULL);
            let mq = shmmq::shm_mq_create_at::call(error_queue_space, i, PARALLEL_ERROR_QUEUE_SIZE);
            shmmq::shm_mq_set_receiver_to_myproc::call(mq);
            workers[i as usize].error_mqh = shmmq::shm_mq_attach::call(mq, seg_to_exec(seg))?;
            i += 1;
        }
        with_globals(|g| g.get_mut(pcxt).worker = workers);
        shm_toc_insert(toc, PARALLEL_KEY_ERROR_QUEUE, error_queue_space);

        // Serialize entrypoint information. "library\0function\0".
        // C: strcpy(entrypointstate, library_name);
        //    strcpy(entrypointstate + lnamelen + 1, function_name);
        let (library_name, function_name) =
            with_globals(|g| (g.get(pcxt).library_name.clone(), g.get(pcxt).function_name.clone()));
        let entrypointstate = shm_toc_allocate(toc, library_name.len() + function_name.len() + 2);
        write_entrypoint(entrypointstate, &library_name, &function_name);
        shm_toc_insert(toc, PARALLEL_KEY_ENTRYPOINT, entrypointstate);
    }

    // Update nworkers_to_launch, in case we changed nworkers above.
    with_globals(|g| {
        let c = g.get_mut(pcxt);
        c.nworkers_to_launch = c.nworkers;
    });

    // Restore previous memory context.
    rt::memory_context_switch_back::call(oldcontext)?;
    Ok(())
}

/// The `pcxt.seg` handle as the execParallel `DsmSegmentHandle` the `shm-mq`
/// seam consumes (both carry the real `DsmSegmentId`; `NULL`/`0` -> `None`, so
/// the error queue gets no `on_dsm_detach` auto-detach in the private-memory
/// no-DSM fallback — exactly C's `shm_mq_attach(mq, NULL, NULL)`).
fn seg_to_exec(seg: DsmSegmentHandle) -> Option<ExecDsmSeg> {
    if seg.is_null() {
        None
    } else {
        Some(ExecDsmSeg(seg.0))
    }
}

// ===========================================================================
// ReinitializeParallelDSM (parallel.c:507-556).
// ===========================================================================

/// Reinitialize the DSM segment so we can launch workers for it again.
pub fn reinitialize_parallel_dsm(pcxt: ParallelContextHandle) -> PgResult<()> {
    // We might be running in a very short-lived memory context.
    let oldcontext = rt::switch_to_top_transaction_context::call()?;

    // Wait for any old workers to exit.
    if with_globals(|g| g.get(pcxt).nworkers_launched) > 0 {
        wait_for_parallel_workers_to_finish(pcxt)?;
        wait_for_parallel_workers_to_exit(pcxt)?;
        with_globals(|g| {
            let c = g.get_mut(pcxt);
            c.nworkers_launched = 0;
            if !c.known_attached_workers.is_empty() {
                c.known_attached_workers = Vec::new();
                c.nknown_attached_workers = 0;
            }
        });
    }

    // Reset a few bits of fixed parallel state to a clean state.
    let toc = pcxt_toc(pcxt);
    let fps = shm_toc_lookup(toc, PARALLEL_KEY_FIXED, false).ok_or_else(missing_fixed_key)?;
    rt::fps_reset_last_xlog_end::call(fps.0)?;

    // Recreate error queues (if they exist).
    if pcxt_nworkers(pcxt) > 0 {
        let error_queue_space =
            shm_toc_lookup(toc, PARALLEL_KEY_ERROR_QUEUE, false).ok_or_else(missing_error_queue_key)?;
        let nworkers = pcxt_nworkers(pcxt);
        let seg = with_globals(|g| g.get(pcxt).seg);
        let mut i = 0;
        while i < nworkers {
            // C: mq = shm_mq_create(error_queue_space + i*SIZE, SIZE);
            //    shm_mq_set_receiver(mq, MyProc);
            //    pcxt->worker[i].error_mqh = shm_mq_attach(mq, pcxt->seg, NULL);
            let mq = shmmq::shm_mq_create_at::call(error_queue_space, i, PARALLEL_ERROR_QUEUE_SIZE);
            shmmq::shm_mq_set_receiver_to_myproc::call(mq);
            let mqh = shmmq::shm_mq_attach::call(mq, seg_to_exec(seg))?;
            with_globals(|g| g.get_mut(pcxt).worker[i as usize].error_mqh = mqh);
            i += 1;
        }
    }

    // Restore previous memory context.
    rt::memory_context_switch_back::call(oldcontext)?;
    Ok(())
}

// ===========================================================================
// ReinitializeParallelWorkers (parallel.c:564-574).
// ===========================================================================

/// Reinitialize parallel workers so we could launch a different number of them.
pub fn reinitialize_parallel_workers(pcxt: ParallelContextHandle, nworkers_to_launch: i32) {
    // nworkers_to_launch must be <= nworkers the context was initialized with.
    with_globals(|g| {
        let c = g.get_mut(pcxt);
        c.nworkers_to_launch = c.nworkers.min(nworkers_to_launch);
    });
}

// ===========================================================================
// LaunchParallelWorkers (parallel.c:579-665).
// ===========================================================================

/// Launch parallel workers.
pub fn launch_parallel_workers(pcxt: ParallelContextHandle) -> PgResult<()> {
    let mut any_registrations_failed = false;

    // Skip this if we have no workers.
    let (nworkers, nworkers_to_launch) =
        with_globals(|g| (g.get(pcxt).nworkers, g.get(pcxt).nworkers_to_launch));
    if nworkers == 0 || nworkers_to_launch == 0 {
        return Ok(());
    }

    // We need to be a lock group leader.
    rt::become_lock_group_leader::call()?;

    // If we do have workers, we'd better have a DSM segment.
    debug_assert!(!with_globals(|g| g.get(pcxt).seg).is_null());

    // We might be running in a short-lived memory context.
    let oldcontext = rt::switch_to_top_transaction_context::call()?;

    // The BackgroundWorker struct assembly — memset/snprintf, bgw_extra memcpy
    // of `i`, `bgw_main_arg = UInt32GetDatum(dsm_segment_handle(seg))` — is
    // performed by the bgworker owner inside `register_dynamic_background_worker`;
    // we resolve the DSM segment's machine name (`dsm_segment_handle(pcxt->seg)`)
    // here, since only the parallel subsystem can resolve its private segment id.
    let seg = with_globals(|g| g.get(pcxt).seg);
    let seg_handle = dsm_seg_handle(seg_id_of(seg));
    let mut i = 0;
    while i < nworkers_to_launch {
        let bgwhandle = if !any_registrations_failed {
            rt::register_dynamic_background_worker::call(seg_handle, i)?
        } else {
            None
        };

        if let Some(bgwhandle) = bgwhandle {
            let error_mqh = with_globals(|g| {
                let c = g.get_mut(pcxt);
                c.worker[i as usize].bgwhandle = Some(bgwhandle);
                c.worker[i as usize].error_mqh
            });
            // C: shm_mq_set_handle(pcxt->worker[i].error_mqh, bgwhandle);
            shmmq::shm_mq_set_handle::call(error_mqh, bgwhandle);
            with_globals(|g| g.get_mut(pcxt).nworkers_launched += 1);
        } else {
            // We've hit the max_worker_processes limit; future registrations
            // will probably fail too, so skip them. But still forget about the
            // error queues we budgeted for these workers.
            any_registrations_failed = true;
            let error_mqh = with_globals(|g| {
                let c = g.get_mut(pcxt);
                c.worker[i as usize].bgwhandle = None;
                c.worker[i as usize].error_mqh
            });
            shmmq::shm_mq_detach::call(error_mqh);
            with_globals(|g| g.get_mut(pcxt).worker[i as usize].error_mqh = ERROR_MQH_NULL);
        }
        i += 1;
    }

    // Now that nworkers_launched is final, initialize known_attached_workers.
    let nworkers_launched = with_globals(|g| g.get(pcxt).nworkers_launched);
    if nworkers_launched > 0 {
        with_globals(|g| {
            let c = g.get_mut(pcxt);
            c.known_attached_workers = vec![false; nworkers_launched as usize];
            c.nknown_attached_workers = 0;
        });
    }

    // Restore previous memory context.
    rt::memory_context_switch_back::call(oldcontext)?;
    Ok(())
}

// ===========================================================================
// WaitForParallelWorkersToAttach (parallel.c:699-789).
// ===========================================================================

/// Wait for all workers to attach to their error queues, throwing if any fails.
pub fn wait_for_parallel_workers_to_attach(pcxt: ParallelContextHandle) -> PgResult<()> {
    let nworkers_launched = with_globals(|g| g.get(pcxt).nworkers_launched);
    if nworkers_launched == 0 {
        return Ok(());
    }

    loop {
        // Process pending parallel messages; may throw an error from a worker.
        rt::check_for_interrupts::call()?;

        let mut i = 0;
        while i < nworkers_launched {
            if with_globals(|g| g.get(pcxt).known_attached_workers[i as usize]) {
                i += 1;
                continue;
            }

            // If error_mqh is NULL, the worker has already exited cleanly.
            let error_mqh = with_globals(|g| g.get(pcxt).worker[i as usize].error_mqh);
            if error_mqh_is_null(error_mqh) {
                with_globals(|g| {
                    let c = g.get_mut(pcxt);
                    c.known_attached_workers[i as usize] = true;
                    c.nknown_attached_workers += 1;
                });
                i += 1;
                continue;
            }

            // The worker has a live error queue here, so it was launched and its
            // `bgwhandle` is set (C reads the non-NULL handle directly).
            let bgwhandle = with_globals(|g| g.get(pcxt).worker[i as usize].bgwhandle)
                .expect("attached parallel worker has a live bgwhandle");
            let (status, _pid) = rt::get_background_worker_pid::call(bgwhandle)?;
            if status == BgwHandleStatus::Started {
                // Has the worker attached to the error queue?
                let mq = shmmq::shm_mq_get_queue::call(error_mqh);
                if shmmq::shm_mq_get_sender::call(mq).is_some() {
                    with_globals(|g| {
                        let c = g.get_mut(pcxt);
                        c.known_attached_workers[i as usize] = true;
                        c.nknown_attached_workers += 1;
                    });
                }
            } else if status == BgwHandleStatus::Stopped {
                // If the worker stopped without attaching, throw an error.
                let mq = shmmq::shm_mq_get_queue::call(error_mqh);
                if shmmq::shm_mq_get_sender::call(mq).is_none() {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                        .errmsg("parallel worker failed to initialize")
                        .errhint("More details may be available in the server log.")
                        .into_error());
                }

                with_globals(|g| {
                    let c = g.get_mut(pcxt);
                    c.known_attached_workers[i as usize] = true;
                    c.nknown_attached_workers += 1;
                });
            } else {
                // Worker not yet started; wait for the postmaster to notify us.
                let rc = rt::wait_latch::call(WAIT_EVENT_BGWORKER_STARTUP)?;
                if rc & WL_LATCH_SET != 0 {
                    rt::reset_latch::call()?;
                }
            }
            i += 1;
        }

        // If all workers are known to have started, we're done.
        let nknown = with_globals(|g| g.get(pcxt).nknown_attached_workers);
        if nknown >= nworkers_launched {
            debug_assert_eq!(nknown, nworkers_launched);
            break;
        }
    }
    Ok(())
}

// ===========================================================================
// WaitForParallelWorkersToFinish (parallel.c:802-906).
// ===========================================================================

/// Wait for all workers to finish computing.
pub fn wait_for_parallel_workers_to_finish(pcxt: ParallelContextHandle) -> PgResult<()> {
    loop {
        let mut anyone_alive = false;
        let mut nfinished = 0;

        // Process pending parallel messages; may throw an error from a worker.
        rt::check_for_interrupts::call()?;

        let nworkers_launched = with_globals(|g| g.get(pcxt).nworkers_launched);
        let mut i = 0;
        while i < nworkers_launched {
            // If error_mqh is NULL, the worker exited cleanly. If we received a
            // message through error_mqh, it started cleanly and we'll be notified
            // when it exits.
            let (error_mqh_null, known) = with_globals(|g| {
                let c = g.get(pcxt);
                (
                    error_mqh_is_null(c.worker[i as usize].error_mqh),
                    c.known_attached_workers[i as usize],
                )
            });
            if error_mqh_null {
                nfinished += 1;
            } else if known {
                anyone_alive = true;
                break;
            }
            i += 1;
        }

        if !anyone_alive {
            // If all workers are known to have finished, we're done.
            if nfinished >= nworkers_launched {
                debug_assert_eq!(nfinished, nworkers_launched);
                break;
            }

            // We didn't detect any living workers, but not all are known to have
            // exited cleanly. Investigate.
            let mut i = 0;
            while i < nworkers_launched {
                let (error_mqh, bgwhandle) = with_globals(|g| {
                    let c = g.get(pcxt);
                    (c.worker[i as usize].error_mqh, c.worker[i as usize].bgwhandle)
                });
                // C: error_mqh == NULL || bgwhandle == NULL ||
                //    GetBackgroundWorkerPid(bgwhandle, &pid) != BGWH_STOPPED
                // (short-circuits before the pid call when there is no handle).
                let stopped = match bgwhandle {
                    Some(h) => {
                        rt::get_background_worker_pid::call(h)?.0 == BgwHandleStatus::Stopped
                    }
                    None => false,
                };
                if error_mqh_is_null(error_mqh) || !stopped {
                    i += 1;
                    continue;
                }

                // Check whether the worker stopped without ever attaching to the
                // error queue. If so, throw an error.
                let mq = shmmq::shm_mq_get_queue::call(error_mqh);
                if shmmq::shm_mq_get_sender::call(mq).is_none() {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                        .errmsg("parallel worker failed to initialize")
                        .errhint("More details may be available in the server log.")
                        .into_error());
                }

                // Stopped but attached: our latch should have been set; the right
                // things will happen on the next pass.
                i += 1;
            }
        }

        rt::wait_latch::call(WAIT_EVENT_PARALLEL_FINISH)?;
        rt::reset_latch::call()?;
    }

    let toc = pcxt_toc(pcxt);
    if with_globals(|g| g.get(pcxt).toc.is_some()) {
        let fps = shm_toc_lookup(toc, PARALLEL_KEY_FIXED, false).ok_or_else(missing_fixed_key)?;
        let last = rt::fps_get_last_xlog_end::call(fps.0)?;
        if last > rt::xact_last_rec_end::call()? {
            rt::set_xact_last_rec_end::call(last)?;
        }
    }
    Ok(())
}

// ===========================================================================
// WaitForParallelWorkersToExit (parallel.c:916-946).
// ===========================================================================

/// Wait for all workers to exit (complete shutdown).
fn wait_for_parallel_workers_to_exit(pcxt: ParallelContextHandle) -> PgResult<()> {
    let nworkers_launched = with_globals(|g| g.get(pcxt).nworkers_launched);
    let mut i = 0;
    while i < nworkers_launched {
        let (worker_null, bgwhandle) = with_globals(|g| {
            let c = g.get(pcxt);
            (c.worker_is_null(), c.worker[i as usize].bgwhandle)
        });
        // C: pcxt->worker == NULL || pcxt->worker[i].bgwhandle == NULL
        let Some(bgwhandle) = bgwhandle.filter(|_| !worker_null) else {
            i += 1;
            continue;
        };

        let status = rt::wait_for_background_worker_shutdown::call(bgwhandle)?;

        // If the postmaster died, we have no chance of cleaning up safely.
        if status == BgwHandleStatus::PostmasterDied {
            return Err(ereport(FATAL)
                .errcode(ERRCODE_ADMIN_SHUTDOWN)
                .errmsg("postmaster exited during a parallel transaction")
                .into_error());
        }

        // Release memory (pfree(bgwhandle)).
        rt::terminate_background_worker_handle_free::call(bgwhandle)?;
        with_globals(|g| g.get_mut(pcxt).worker[i as usize].bgwhandle = None);
        i += 1;
    }
    Ok(())
}

// ===========================================================================
// DestroyParallelContext (parallel.c:956-1025).
// ===========================================================================

/// Destroy a parallel context. After this call the handle is dangling.
pub fn destroy_parallel_context(pcxt: ParallelContextHandle) -> PgResult<()> {
    // Remove from the list before anything else; an error in a later step could
    // otherwise try to nuke it again.
    with_globals(|g| {
        if let Some(pos) = g.list.iter().position(|&p| p == pcxt) {
            g.list.remove(pos);
        }
    });

    // Kill each worker in turn, and forget their error queues.
    if !with_globals(|g| g.get(pcxt).worker_is_null()) {
        let nworkers_launched = with_globals(|g| g.get(pcxt).nworkers_launched);
        let mut i = 0;
        while i < nworkers_launched {
            let (error_mqh, bgwhandle) = with_globals(|g| {
                let c = g.get(pcxt);
                (c.worker[i as usize].error_mqh, c.worker[i as usize].bgwhandle)
            });
            if !error_mqh_is_null(error_mqh) {
                // A live error queue means the worker was launched, so its
                // `bgwhandle` is set (C passes the non-NULL handle directly).
                let bgwhandle =
                    bgwhandle.expect("launched parallel worker has a live bgwhandle");
                rt::terminate_background_worker::call(bgwhandle)?;
                shmmq::shm_mq_detach::call(error_mqh);
                with_globals(|g| g.get_mut(pcxt).worker[i as usize].error_mqh = ERROR_MQH_NULL);
            }
            i += 1;
        }
    }

    // If we allocated a shared memory segment, detach it (C: dsm_detach(seg);
    // seg = NULL). Dropping the real `DsmSegment` guard runs `dsm_detach`.
    let seg_guard = with_globals(|g| {
        let c = g.get_mut(pcxt);
        c.seg = DsmSegmentHandle::NULL;
        c.toc = None;
        c.toc_base = 0;
        c.seg_guard.take()
    });
    drop(seg_guard);

    // If this context is in backend-private memory, free that instead (C:
    // pfree(private_memory); private_memory = NULL). The buffer was allocated in
    // TopMemoryContext, so it is freed back there.
    let private_memory = with_globals(|g| {
        let c = g.get_mut(pcxt);
        c.toc = None;
        c.toc_base = 0;
        c.private_memory.take()
    });
    if let Some(pm) = private_memory {
        let top = backend_utils_mmgr_mcxt_seams::top_memory_context::call();
        // SAFETY: `pm.ptr`/`pm.layout` are exactly the allocation made from this
        // same TopMemoryContext in `establish_parallel_segment`, freed once.
        unsafe { top.deallocate(pm.ptr, pm.layout) };
    }

    // We can't finish transaction commit/abort until all workers have exited; in
    // particular, we can't respond to interrupts at this stage.
    rt::hold_interrupts::call()?;
    wait_for_parallel_workers_to_exit(pcxt)?;
    rt::resume_interrupts::call()?;

    // Free the worker array.
    if !with_globals(|g| g.get(pcxt).worker_is_null()) {
        with_globals(|g| g.get_mut(pcxt).worker = Vec::new());
    }

    // Free memory (pfree library_name/function_name/pcxt; the registry slot drop
    // frees the context and its owned Strings).
    with_globals(|g| {
        if let Some(slot) = g.slots.get_mut(pcxt.0) {
            *slot = None;
        }
    });
    Ok(())
}

// ===========================================================================
// ParallelContextActive (parallel.c:1030-1034).
// ===========================================================================

/// Are there any parallel contexts currently active?
pub fn parallel_context_active() -> bool {
    !with_globals(|g| g.list_is_empty())
}

// ===========================================================================
// ProcessParallelMessages (parallel.c:1054-1138).
// ===========================================================================

/// Process any queued protocol messages received from parallel workers.
pub fn process_parallel_messages() -> PgResult<()> {
    // Block interrupts until done.
    rt::hold_interrupts::call()?;

    // Do our work in a private context we can reset on each use.
    let oldcontext = rt::enter_hpm_context::call()?;

    // OK to process messages; reset the flag saying there are more to do.
    set_parallel_message_pending(false);

    // dlist_foreach over the live contexts.
    let ids: Vec<ParallelContextHandle> = with_globals(|g| g.list.clone());
    for pcxt in ids {
        if with_globals(|g| g.get(pcxt).worker_is_null()) {
            continue;
        }

        let nworkers_launched = with_globals(|g| g.get(pcxt).nworkers_launched);
        let mut i = 0;
        while i < nworkers_launched {
            // Read as many messages as we can from each worker, but stop when
            // (1) the error queue goes away, or (2) no more messages can be read
            // without blocking.
            loop {
                let error_mqh = with_globals(|g| g.get(pcxt).worker[i as usize].error_mqh);
                if error_mqh_is_null(error_mqh) {
                    break;
                }
                let (res, data) = shmmq::shm_mq_receive::call(error_mqh)?;
                match res {
                    Some(ShmMqResult::WouldBlock) => break,
                    Some(ShmMqResult::Success) => {
                        process_parallel_message(pcxt, i, &data)?;
                    }
                    Some(ShmMqResult::Detached) => {
                        return Err(ereport(ERROR)
                            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                            .errmsg("lost connection to parallel worker")
                            .into_error());
                    }
                    None => break,
                }
            }
            i += 1;
        }
    }

    // MemoryContextSwitchTo(oldcontext) + MemoryContextReset(hpm_context).
    rt::leave_hpm_context::call(oldcontext)?;

    rt::resume_interrupts::call()?;
    Ok(())
}

// ===========================================================================
// ProcessParallelMessage (parallel.c:1143-1252).
// ===========================================================================

/// Process a single protocol message received from a single parallel worker.
fn process_parallel_message(pcxt: ParallelContextHandle, i: i32, msg: &[u8]) -> PgResult<()> {
    with_globals(|g| {
        let c = g.get_mut(pcxt);
        if !c.known_attached_workers.is_empty() && !c.known_attached_workers[i as usize] {
            c.known_attached_workers[i as usize] = true;
            c.nknown_attached_workers += 1;
        }
    });

    // msgtype = pq_getmsgbyte(msg); subsequent reads begin at &msg[1..].
    let msgtype = msg[0];
    let body = &msg[1..];

    match msgtype {
        m if m == PqMsg_ErrorResponse || m == PqMsg_NoticeResponse => {
            // Parse the ErrorResponse/NoticeResponse, cap elevel at ERROR (death
            // of a worker isn't enough justification for suicide), optionally add
            // a "parallel worker" context line, and rethrow the error / print the
            // notice — preserving the worker's full ErrorData (message, SQLSTATE,
            // detail, …). The owner (`libpq/pqmq.c`) keeps the rebuilt `ErrorData`
            // local and never projects it lossily across the seam.
            //
            // The "parallel worker" context line is skipped in
            // DEBUG_PARALLEL_REGRESS mode (it causes test-result instability
            // depending on whether a worker is actually used); we own
            // `debug_parallel_query`, so we compute that decision here.
            let append_parallel_worker_context =
                rt::debug_parallel_query::call() != DEBUG_PARALLEL_REGRESS;

            // Context beyond that should use the error context callbacks in
            // effect when the ParallelContext was created.
            let pcxt_stack = with_globals(|g| g.get(pcxt).error_context_stack);
            rt::throw_parallel_error_data::call(
                body,
                append_parallel_worker_context,
                pcxt_stack,
            )?;
        }

        m if m == PqMsg_NotificationResponse => {
            // Propagate NotifyResponse.
            let (pid, channel, payload) = rt::parse_notification_response::call(body)?;
            rt::notify_my_front_end::call(&channel, &payload, pid)?;
        }

        m if m == PqMsg_Progress => {
            // Only incremental progress reporting is currently supported.
            let (index, incr) = rt::parse_progress::call(body)?;
            rt::pgstat_progress_incr_param::call(index, incr)?;
        }

        m if m == PqMsg_Terminate => {
            let error_mqh = with_globals(|g| g.get(pcxt).worker[i as usize].error_mqh);
            shmmq::shm_mq_detach::call(error_mqh);
            with_globals(|g| g.get_mut(pcxt).worker[i as usize].error_mqh = ERROR_MQH_NULL);
        }

        _ => {
            return Err(ereport(ERROR)
                .errmsg(format!(
                    "unrecognized message type received from parallel worker: {} (message length {} bytes)",
                    msgtype as char,
                    msg.len()
                ))
                .into_error());
        }
    }
    Ok(())
}

// ===========================================================================
// AtEOSubXact_Parallel (parallel.c:1260-1274).
// ===========================================================================

/// End-of-subtransaction cleanup; removes only contexts initiated within the
/// current subtransaction.
pub fn at_eosubxact_parallel(is_commit: bool, my_sub_id: SubTransactionId) -> PgResult<()> {
    while !with_globals(|g| g.list_is_empty()) {
        let pcxt = with_globals(|g| g.head()).expect("non-empty pcxt_list");
        if with_globals(|g| g.get(pcxt).subid) != my_sub_id {
            break;
        }
        if is_commit {
            elog(WARNING, "leaked parallel context")?;
        }
        destroy_parallel_context(pcxt)?;
    }
    Ok(())
}

// ===========================================================================
// AtEOXact_Parallel (parallel.c:1282-1293).
// ===========================================================================

/// End-of-transaction cleanup; nukes all remaining contexts.
pub fn at_eoxact_parallel(is_commit: bool) -> PgResult<()> {
    while !with_globals(|g| g.list_is_empty()) {
        let pcxt = with_globals(|g| g.head()).expect("non-empty pcxt_list");
        if is_commit {
            elog(WARNING, "leaked parallel context")?;
        }
        destroy_parallel_context(pcxt)?;
    }
    Ok(())
}

// ===========================================================================
// ParallelWorkerMain (parallel.c:1298-1586).
// ===========================================================================

/// Main entrypoint for parallel workers. `main_arg` is the `Datum` carrying the
/// DSM segment handle (`UInt32GetDatum(dsm_segment_handle(seg))`).
pub fn parallel_worker_main(main_arg: Datum<'static>) -> PgResult<()> {
    // Set flag to indicate we're initializing a parallel worker.
    rt::set_initializing_parallel_worker::call(true)?;
    with_globals(|g| g.initializing_parallel_worker = true);

    // Establish signal handlers.
    rt::worker_install_signal_handlers::call()?;

    // Determine and set our parallel worker number.
    debug_assert_eq!(with_globals(|g| g.parallel_worker_number), -1);
    let worker_number = rt::worker_number_from_bgw_extra::call()?;
    set_parallel_worker_number(worker_number);

    // Set up a memory context to work in.
    rt::worker_create_memory_context::call()?;

    // Attach to the dynamic shared memory segment for the parallel query, and
    // find its table of contents. The worker side genuinely crosses processes
    // into the segment the leader created (the hard core): real `dsm_attach`
    // returns a real `DsmSegment`, and a real `ShmToc::attach(PARALLEL_MAGIC,
    // base)` reads the in-segment header the leader wrote.
    //
    // Note: at this point we have not created any ResourceOwner in this process,
    // so our DSM mapping survives until process exit (C: parallel.c:1346-1349).
    // We model that by parking the `DsmSegment` guard in the process-global
    // worker registry and never dropping it on the success path.
    let seg = worker_attach_segment(datum_as_u32(main_arg) as dsm_handle)?;
    // C: seg == NULL -> "could not map dynamic shared memory segment".
    let seg = match seg {
        Some(seg) => seg,
        None => {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg("could not map dynamic shared memory segment")
                .into_error());
        }
    };
    let id = seg.id();
    let base = dsm_segment_address(id);
    debug_assert!(!base.is_null(), "dsm_segment_address returned NULL");
    let base_nn = NonNull::new(base).expect("dsm segment base is non-null");
    // C: toc = shm_toc_attach(PARALLEL_MAGIC, dsm_segment_address(seg));
    //    toc == NULL (magic mismatch) -> "invalid magic number ...".
    // SAFETY: `base_nn` points at the live, mapped segment header; the segment
    // outlives the toc because we hold the `DsmSegment` guard for process life.
    let real_toc = match unsafe { ShmToc::attach(PARALLEL_MAGIC, base_nn) } {
        Some(t) => t,
        None => {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg("invalid magic number in dynamic shared memory segment")
                .into_error());
        }
    };
    let toc_base = base as usize;
    // Park the real guard + real toc so the mapping survives the call (C: no
    // ResourceOwner; mapping survives to process exit) and the worker lookups can
    // recover the real `ShmToc` by base.
    WORKER_ATTACHED.with(|w| {
        w.borrow_mut().push(WorkerAttached {
            base: toc_base,
            _seg_guard: seg,
            toc: real_toc,
        })
    });
    // The execParallel-visible segment handle carries the real `DsmSegmentId`
    // (opacity-inherited), exactly like the leader side.
    let seg = seg_handle_of(id);
    // The worker-side toc lookups address the attached segment buffer; the worker
    // threads the segment's real base as its toc handle (so `invoke_entrypoint`
    // hands the entrypoint the real `shm_toc` base).
    let toc = ExecShmToc(toc_base);

    // Look up fixed parallel state.
    let fps_base = worker_lookup(toc, PARALLEL_KEY_FIXED)?;
    rt::set_my_fixed_parallel_state::call(fps_base)?;
    with_globals(|g| g.my_fixed_parallel_state = fps_base);
    let fps = rt::fps_read::call(fps_base)?;

    // Arrange to signal the leader if we exit.
    with_globals(|g| g.parallel_leader_pid = fps.parallel_leader_pid);
    rt::set_parallel_leader_proc_number::call(fps.parallel_leader_proc_number)?;
    rt::register_parallel_worker_shutdown::call(seg)?;

    // Find and attach to the error queue provided for us.
    // C: error_queue_space = shm_toc_lookup(toc, PARALLEL_KEY_ERROR_QUEUE, false);
    //    mq = (shm_mq *) (error_queue_space + ParallelWorkerNumber*SIZE);
    //    shm_mq_set_sender(mq, MyProc);
    //    mqh = shm_mq_attach(mq, seg, NULL);
    //    pq_redirect_to_shm_mq(seg, mqh);
    // The error_queue_space chunk is a real in-segment address; the worker just
    // *casts* `chunk + ParallelWorkerNumber*SIZE` to the already-created queue
    // (only the leader runs `shm_mq_create`; re-creating here would wipe the
    // leader's `mq_set_receiver`). `seg` carries the real `DsmSegmentId`, so the
    // queue auto-detaches with the segment.
    let error_queue_space = SerializeCursor(worker_lookup(toc, PARALLEL_KEY_ERROR_QUEUE)?);
    let worker_number = with_globals(|g| g.parallel_worker_number);
    let mq = shmmq::shm_mq_at::call(error_queue_space, worker_number, PARALLEL_ERROR_QUEUE_SIZE);
    shmmq::shm_mq_set_sender_to_myproc::call(mq);
    let mqh = shmmq::shm_mq_attach::call(mq, seg_to_exec(seg))?;
    rt::pq_redirect_to_shm_mq::call(seg, ShmMqHandleHandle(mqh.0))?;
    rt::pq_set_parallel_leader::call(fps.parallel_leader_pid, fps.parallel_leader_proc_number)?;

    // Join locking group. If we can't, the leader has gone away, so exit quietly.
    if !rt::become_lock_group_member::call(fps.parallel_leader_proc_number, fps.parallel_leader_pid)? {
        return Ok(());
    }

    // Restore transaction and statement start-time timestamps.
    rt::set_parallel_start_timestamps::call(fps.xact_ts, fps.stmt_ts)?;

    // Identify the entry point to be called.
    // C: library_name = entrypointstate;
    //    function_name = entrypointstate + strlen(library_name) + 1;
    let entrypointstate = worker_lookup(toc, PARALLEL_KEY_ENTRYPOINT)?;
    let (library_name, function_name) = read_entrypoint(SerializeCursor(entrypointstate))?;
    let entrypt = lookup_parallel_worker_function(&library_name, &function_name)?;

    // Restore current session authorization and role id. No verification.
    rt::set_authenticated_user_id::call(fps.authenticated_user_id)?;
    rt::set_session_authorization::call(fps.session_user_id, fps.session_user_is_superuser)?;
    rt::set_current_role_id::call(fps.outer_user_id, fps.role_is_superuser)?;

    // Restore database connection. We skip connection authorization checks.
    rt::background_worker_initialize_connection_by_oid::call(
        fps.database_id,
        fps.authenticated_user_id,
        BGWORKER_BYPASS_ALLOWCONN | BGWORKER_BYPASS_ROLELOGINCHECK,
    )?;

    // Set the client encoding to the database encoding.
    let enc = rt::get_database_encoding::call()?;
    if rt::set_client_encoding::call(enc)? < 0 {
        return Err(ereport(ERROR)
            .errmsg(format!("SetClientEncoding({enc}) failed"))
            .into_error());
    }

    // Load libraries the original backend loaded, before restoring GUCs.
    let libraryspace = worker_lookup(toc, PARALLEL_KEY_LIBRARY)?;
    rt::start_transaction_command::call()?;
    rt::restore_library_state::call(libraryspace)?;
    rt::commit_transaction_command::call()?;

    // Crank up a transaction state appropriate to a parallel worker.
    let tstatespace = worker_lookup(toc, PARALLEL_KEY_TRANSACTION_STATE)?;
    rt::start_parallel_worker_transaction::call(tstatespace)?;

    // Restore state that affects catalog access.
    let pendingsyncsspace = worker_lookup(toc, PARALLEL_KEY_PENDING_SYNCS)?;
    rt::restore_pending_syncs::call(pendingsyncsspace)?;
    let relmapperspace = worker_lookup(toc, PARALLEL_KEY_RELMAPPER_STATE)?;
    rt::restore_relation_map::call(relmapperspace)?;
    let reindexspace = worker_lookup(toc, PARALLEL_KEY_REINDEX_STATE)?;
    rt::restore_reindex_state::call(reindexspace)?;
    let combocidspace = worker_lookup(toc, PARALLEL_KEY_COMBO_CID)?;
    rt::restore_combocid_state::call(combocidspace)?;

    // Attach to the per-session DSM segment and contained objects.
    let session_dsm_handle_space = worker_lookup(toc, PARALLEL_KEY_SESSION_DSM)?;
    rt::attach_session::call(rt::read_dsm_handle::call(session_dsm_handle_space)?)?;

    // Restore the active snapshot (and the transaction snapshot if present).
    let asnapspace = worker_lookup(toc, PARALLEL_KEY_ACTIVE_SNAPSHOT)?;
    let tsnapspace = worker_lookup_opt(toc, PARALLEL_KEY_TRANSACTION_SNAPSHOT);
    let asnapshot = rt::restore_snapshot::call(asnapspace)?;
    let tsnapshot = if tsnapspace != 0 {
        rt::restore_snapshot::call(tsnapspace)?
    } else {
        // C aliases `tsnapshot = asnapshot` (same `Snapshot` pointer); the owned
        // value model takes a distinct copy, then pushes the active one below.
        asnapshot.clone()
    };
    rt::restore_transaction_snapshot::call(tsnapshot, fps.parallel_leader_proc_number)?;
    rt::push_active_snapshot::call(asnapshot)?;

    // We've changed which tuples we can see; invalidate system caches.
    rt::invalidate_system_caches::call()?;

    // Restore GUC values from launching backend.
    let gucspace = worker_lookup(toc, PARALLEL_KEY_GUC)?;
    rt::restore_guc_state::call(gucspace)?;

    // Restore current user ID and security context (after restoring GUCs).
    rt::set_user_id_and_sec_context::call(fps.current_user_id, fps.sec_context)?;

    // Restore temp-namespace state.
    rt::set_temp_namespace_state::call(fps.temp_namespace_id, fps.temp_toast_namespace_id)?;

    // Restore uncommitted enums.
    let uncommittedenumsspace = worker_lookup(toc, PARALLEL_KEY_UNCOMMITTEDENUMS)?;
    rt::restore_uncommitted_enums::call(uncommittedenumsspace)?;

    // Restore the ClientConnectionInfo.
    let clientconninfospace = worker_lookup(toc, PARALLEL_KEY_CLIENTCONNINFO)?;
    rt::restore_client_connection_info::call(clientconninfospace)?;

    // Initialize SystemUser now that MyClientConnectionInfo is restored.
    rt::maybe_initialize_system_user::call()?;

    // Attach to the leader's serializable transaction, if SERIALIZABLE.
    rt::attach_serializable_xact::call(fps.serializable_xact_handle)?;

    // State is initialized; nothing should change hereafter.
    with_globals(|g| g.initializing_parallel_worker = false);
    rt::set_initializing_parallel_worker::call(false)?;
    rt::enter_parallel_mode::call()?;

    // Invoke the caller-supplied code.
    rt::invoke_entrypoint::call(entrypt, seg, toc.0)?;

    // Must exit parallel mode to pop active snapshot.
    rt::exit_parallel_mode::call()?;

    // Must pop active snapshot so snapmgr.c doesn't complain.
    rt::pop_active_snapshot::call()?;

    // Shut down the parallel-worker transaction.
    rt::end_parallel_worker_transaction::call()?;

    // Detach from the per-session DSM segment.
    rt::detach_session::call()?;

    // Report success.
    rt::pq_put_terminate::call()?;
    Ok(())
}

/// `dsm_attach(handle)` on the worker side — the real `dsm-core` attach.
/// `Ok(None)` when the segment is unknown (everyone, including the creator,
/// detached before we got here): C returns NULL and the caller `ereport(ERROR)`s.
/// The descriptor is allocated in `TopMemoryContext` (C global), matching the
/// leader's `dsm_create` so the mapping outlives the (short-lived) caller.
fn worker_attach_segment(handle: dsm_handle) -> PgResult<Option<DsmSegment>> {
    let top = backend_utils_mmgr_mcxt_seams::top_memory_context::call();
    backend_storage_ipc_dsm_core::dsm::dsm_attach(handle, top)
}

/// `shm_toc_lookup(toc, key, false)` on the worker-attached segment — a real
/// in-segment lookup against the leader-written TOC. The chunk's *real address*
/// is what the worker threads onward (family `shm-toc-address`). `shm_toc_lookup`
/// `elog(ERROR)`s on a missing required key; the leader writes every required key
/// before launching, so a miss is corruption — propagate the error.
fn worker_lookup(toc: ExecShmToc, key: u64) -> PgResult<usize> {
    worker_with_toc(toc.0, |real| {
        let found = real.lookup(key, false)?;
        Ok(found
            .expect("shm_toc_lookup on missing required key")
            .as_ptr() as usize)
    })
}

/// `shm_toc_lookup(toc, key, true)` — 0 when absent (real in-segment lookup).
fn worker_lookup_opt(toc: ExecShmToc, key: u64) -> usize {
    worker_with_toc(toc.0, |real| {
        real.lookup(key, true)
            .ok()
            .flatten()
            .map_or(0, |p| p.as_ptr() as usize)
    })
}

// Convert a `Datum` carrying a `uint32` back to that u32 (UInt32GetDatum inverse).
fn datum_as_u32(d: Datum<'_>) -> u32 {
    d.as_u32()
}

// ===========================================================================
// ParallelWorkerReportLastRecEnd (parallel.c:1592-1602).
// ===========================================================================

/// Update shared memory with the ending location of the last WAL record we
/// wrote, if greater than the value stored there.
pub fn parallel_worker_report_last_rec_end(last_rec_end: XLogRecPtr) -> PgResult<()> {
    let fps = with_globals(|g| g.my_fixed_parallel_state);
    debug_assert!(fps != 0);
    // SpinLockAcquire / compare-and-set / SpinLockRelease — the spinlock is
    // genuinely cross-process, performed in the seam.
    rt::fps_report_last_rec_end::call(fps, last_rec_end)?;
    Ok(())
}

// ===========================================================================
// ParallelWorkerShutdown (parallel.c:1620-1628).
// ===========================================================================

/// `on_dsm_detach`-registered callback: make the leader read once more from our
/// error queue, then detach from the dsm segment. `arg` is the `Datum` carrying
/// the segment.
pub fn parallel_worker_shutdown(_code: i32, arg: Datum<'static>) -> PgResult<()> {
    rt::send_parallel_message_signal::call(
        with_globals(|g| g.parallel_leader_pid),
        rt::parallel_leader_proc_number::call(),
    )?;
    rt::dsm_detach_handle::call(rt::dsm_segment_from_datum::call(arg)?)?;
    Ok(())
}

// ===========================================================================
// LookupParallelWorkerFunction (parallel.c:1648-1672).
// ===========================================================================

/// Look up (and possibly load) a parallel worker entry-point function.
fn lookup_parallel_worker_function(libraryname: &str, funcname: &str) -> PgResult<usize> {
    // If the function is to be loaded from postgres itself, search the
    // InternalParallelWorkers array.
    if libraryname == "postgres" {
        for &name in INTERNAL_PARALLEL_WORKERS.iter() {
            if name == funcname {
                return rt::resolve_internal_parallel_worker::call(funcname);
            }
        }

        // Reachable only by programming error; C elog(ERROR) never returns.
        return Err(ereport(ERROR)
            .errmsg(format!("internal function \"{funcname}\" not found"))
            .into_error());
    }

    // Otherwise load from external library.
    rt::load_external_function::call(libraryname, funcname)
}

// ===========================================================================
// Seam installation.
// ===========================================================================

/// Install the DSM-helper and `FixedParallelState`-driver runtime seams this
/// crate owns. These were collected in `backend-access-transam-parallel-rt-seams`
/// (the design-debt home) but their bodies live here — the DSM-segment-handle
/// bridge (`DsmSegmentHandle <-> DsmSegmentId`), the in-segment fixed-state
/// byte layout, and the genuine cross-process `FixedParallelState.mutex`
/// spinlock are all owned by the parallel subsystem.
pub fn init_seams() {
    install_dsm_helper_seams();
    install_fps_driver_seams();
    install_execparallel_support_pcxt_seams();
    install_mmgr_context_seams();
    install_worker_state_seams();

    // planner.c:373 reads `IsParallelWorker()` in the cheap-test gate for
    // `glob->parallelModeOK`; expose this crate's `is_parallel_worker()` to the
    // planner through the planner seam (avoids a planner→parallel-executor dep).
    backend_optimizer_plan_planner_seams::is_parallel_worker::set(is_parallel_worker);

    // `IsParallelWorker()` (access/parallel.h) is also read by parallel_vacuum.c
    // (`parallel_vacuum_*` assert `!IsParallelWorker()` on the leader) and by
    // vacuum.c's cost-delay accounting. `ParallelWorkerNumber` is this crate's
    // per-backend state, so install the vacuum-seams slot from here too.
    backend_commands_vacuum_seams::is_parallel_worker::set(|| Ok(is_parallel_worker()));

    // `IsParallelWorker()` is also read by execUtils.c's
    // `ExecGetRangeTableRelation`: a parallel worker takes its OWN local lock on
    // the scan relation (`table_open(relid, rellockmode)`) rather than relying on
    // the leader's lock (`NoLock` + a `CheckRelationLockedByMe` Assert). Install
    // the execUtils-seams slot from here for the same reason as the planner slot.
    backend_executor_execUtils_seams::is_parallel_worker::set(is_parallel_worker);
}

/// Install the worker-side restore-sequence and leader message-handling seams
/// `ParallelWorkerMain` / `ProcessParallelMessages` (parallel.c) drive. Most of
/// these address this crate's own per-backend parallel state (the worker number,
/// the `InitializingParallelWorker` flag, the `MyFixedParallelState` base); the
/// rest delegate to the owner subsystem that owns the body (init/interrupts,
/// mmgr, dfmgr, ipc shmem-exit, the execParallel worker entry point).
fn install_worker_state_seams() {
    // --- leader: ProcessParallelMessages private memory context ------------
    // C: oldcontext = MemoryContextSwitchTo(hpm_context) over a reset-per-call
    // private `AllocSetContext`. There is no ambient `CurrentMemoryContext` in
    // this tree's mcx model (docs/mctx-design.md), so — exactly like
    // `switch_to_top_transaction_context` above — the switch has no observable
    // effect: return a nominal saved handle, and the restore (which in C also
    // resets the context) is a no-op (the port uses owned `Vec`s freed on drop).
    rt::enter_hpm_context::set(|| Ok(0));
    rt::leave_hpm_context::set(|_saved| Ok(()));

    // C: `pfree(msg.data)` after each ProcessParallelMessage. The port frees the
    // message buffer natively (owned `Vec` dropped); nothing to free here.
    rt::pfree::set(|_ptr| Ok(()));

    // --- leader: HandleParallelMessageInterrupt sets InterruptPending -------
    // C macro `InterruptPending = true;`. globals.c owns the flag; delegate to
    // the init-small accessor seam.
    rt::set_interrupt_pending::set(|| {
        backend_utils_init_small_seams::set_interrupt_pending::call(true);
        Ok(())
    });

    // NOTE: `throw_parallel_error_data` (rethrow a worker's ErrorResponse /
    // print its NoticeResponse via `ThrowErrorData`) is installed by the error
    // subsystem owner (`backend-libpq-pqmq`, which owns `pq_parse_errornotice`
    // and the full `ErrorData` the rethrow needs); it is not this crate's body.

    // --- worker: InitializingParallelWorker flag (miscadmin-style global) ---
    // C: `InitializingParallelWorker = value;`. This crate owns the flag.
    rt::set_initializing_parallel_worker::set(|value| {
        with_globals(|g| g.initializing_parallel_worker = value);
        Ok(())
    });

    // --- worker: establish signal handlers ---------------------------------
    // C: `pqsignal(SIGTERM, die); BackgroundWorkerUnblockSignals();`.
    rt::worker_install_signal_handlers::set(|| {
        interfaces_libpq_legacy_pqsignal::pqsignal(
            libc::SIGTERM,
            types_signal::SigHandler::Handler(
                backend_tcop_postgres_seams::die_signal_handler::call(),
            ),
        );
        backend_postmaster_bgworker_seams::background_worker_unblock_signals::call();
        Ok(())
    });

    // --- worker: private "Parallel worker" memory context ------------------
    // C: `CurrentMemoryContext = AllocSetContextCreate(TopMemoryContext,
    // "Parallel worker", ...)` — set up a working context "just for cleanliness".
    // No ambient `CurrentMemoryContext` exists in this tree's mcx model (every
    // allocation threads an owned `Mcx`), so there is nothing to flip; no-op.
    rt::worker_create_memory_context::set(|| Ok(()));

    // --- worker: MyFixedParallelState base ---------------------------------
    // C: `MyFixedParallelState = fps;` — the in-segment base of the fixed state.
    rt::set_my_fixed_parallel_state::set(|base| {
        with_globals(|g| g.my_fixed_parallel_state = base);
        Ok(())
    });

    // --- worker: before_shmem_exit(ParallelWorkerShutdown, PointerGetDatum(seg)) -
    // Arrange to signal the leader (one last error-queue read) and detach the DSM
    // segment at process exit. The `arg` Datum carries the segment handle word
    // (PointerGetDatum(seg)); `dsm_segment_from_datum` recovers it on the way out.
    rt::register_parallel_worker_shutdown::set(|seg| {
        backend_storage_ipc_dsm_core_seams::before_shmem_exit::call(
            parallel_worker_shutdown,
            Datum::from_usize(seg.0),
        )
    });

    // --- worker: invoke the resolved entry point ---------------------------
    // C: `entrypt(seg, toc);`. `LookupParallelWorkerFunction` resolved a function
    // pointer; this tree carries it as a small `ParallelWorkerMainFn` token
    // (`resolve_internal_parallel_worker` below assigns the tokens). Dispatch the
    // token to the real entry-point seam over the worker's real in-segment toc.
    rt::invoke_entrypoint::set(|entrypt, seg, toc| {
        match entrypt {
            ENTRYPT_PARALLEL_QUERY_MAIN => {
                let mcx = backend_utils_mmgr_mcxt_seams::top_memory_context::call();
                backend_executor_execParallel_seams::ParallelQueryMain::call(
                    mcx,
                    types_execparallel::DsmSegmentHandle(seg.0),
                    types_execparallel::ShmTocHandle(toc),
                )
            }
            other => Err(ereport(ERROR)
                .errmsg(format!(
                    "unrecognized internal parallel worker entry point token: {other}"
                ))
                .into_error()),
        }
    });

    // --- worker: resolve an internal entry point to its token --------------
    // C: searches the `InternalParallelWorkers[]` table and returns the function
    // pointer. Here each internal name maps to a stable token `invoke_entrypoint`
    // dispatches. `ParallelQueryMain` is the only entry point compiled into this
    // build (the executor's parallel-query worker main).
    rt::resolve_internal_parallel_worker::set(|funcname| match funcname {
        "ParallelQueryMain" => Ok(ENTRYPT_PARALLEL_QUERY_MAIN),
        other => Err(ereport(ERROR)
            .errmsg(format!("internal function \"{other}\" not found"))
            .into_error()),
    });

    // NOTE: `load_external_function` (resolve a non-"postgres" library's parallel
    // worker entry point) is installed by its owner, `backend-utils-fmgr-dfmgr`,
    // which owns the dynamic loader. No in-core parallel plan loads an external
    // library, so this path is exercised only by extension parallel workers.
}

/// Stable token for the in-core `ParallelQueryMain` entry point (execParallel.c),
/// assigned by `resolve_internal_parallel_worker` and dispatched by
/// `invoke_entrypoint`. (The C `parallel_worker_main_type` function pointer has no
/// portable token here, so we enumerate the internal entry points.)
const ENTRYPT_PARALLEL_QUERY_MAIN: ParallelWorkerMainFn = 1;

/// Install the `MemoryContextSwitchTo(TopTransactionContext)` / restore pair
/// that `CreateParallelContext` (parallel.c:172-203) brackets its `palloc0` of
/// the `ParallelContext` with, so it survives a short-lived caller context.
///
/// In this tree's `mcx` model there is **no ambient `CurrentMemoryContext`** to
/// flip (docs/mctx-design.md): every allocation threads an owned `Mcx` and the
/// `ParallelContext` itself is held in the parallel subsystem's thread-local
/// registry (`with_globals` / `push_head`), not in the switched-to arena. So,
/// exactly like the `switch_to_top_memory_context` seam
/// (`backend-utils-mmgr-portalmem`'s `top_context.rs`, the
/// `MemoryContextSwitchTo(TopMemoryContext)` analog), the C switch has no
/// observable effect to reproduce: the switch returns a nominal saved handle
/// and the restore is a no-op. Both are infallible here.
fn install_mmgr_context_seams() {
    // C: oldcontext = MemoryContextSwitchTo(TopTransactionContext). No ambient
    // current context exists in this model, so there is nothing to flip and no
    // real old context to capture; return a nominal saved handle.
    rt::switch_to_top_transaction_context::set(|| Ok(0));
    // C: MemoryContextSwitchTo(oldcontext). The saved handle is nominal (no
    // ambient context was changed), so restoring it is a no-op.
    rt::memory_context_switch_back::set(|_saved| Ok(()));

    // C: pcxt->subid = GetCurrentSubTransactionId() (parallel.c:191). Delegate
    // to the xact-owned accessor seam (the real `GetCurrentSubTransactionId`,
    // installed in production by `backend-access-transam-xact`).
    rt::get_current_subtransaction_id::set(|| {
        Ok(backend_access_transam_xact_seams::get_current_sub_transaction_id::call())
    });

    // C: pcxt->error_context_stack = error_context_stack (parallel.c:193) — saves
    // the live `ErrorContextCallback *` chain head into the context so the worker
    // can re-raise errors under the leader's callback chain. That `error_context_stack`
    // global is RETIRED in this tree (backend-utils-error: context attaches on
    // propagation, not via a saved chain pointer — docs/query-lifecycle-raii.md),
    // so the faithful saved value is the NULL handle (`0`), exactly as the
    // parallel subsystem's own test path encodes it (`error_context_stack: 0`).
    rt::error_context_stack::set(|| Ok(0));

    // Interrupt-management macros (miscadmin.h), owned by globals.c and installed
    // in production by `backend-utils-init-small`. Delegate to those accessor
    // seams.
    //
    // C: INTERRUPTS_CAN_BE_PROCESSED() — read by `InitializeParallelDSM` to
    // decide whether it is safe to launch workers (else it pretends none).
    rt::interrupts_can_be_processed::set(|| {
        backend_utils_init_small_seams::interrupts_can_be_processed::call()
    });
    // C: HOLD_INTERRUPTS() / RESUME_INTERRUPTS() — bracket `ProcessParallelMessages`
    // and `LaunchParallelWorkers`.
    rt::hold_interrupts::set(|| {
        backend_utils_init_small_seams::hold_interrupts::call();
        Ok(())
    });
    rt::resume_interrupts::set(|| {
        backend_utils_init_small_seams::resume_interrupts::call();
        Ok(())
    });
    // NOTE: `rt::check_for_interrupts` is already installed in production by
    // `backend-utils-init-miscinit` (lib.rs:1067, delegating to the tcop-owned
    // accessor seam); do not install it here (seam slots panic on a second
    // `set`).
}

/// Install the orthogonal `ParallelContext`/`shm_toc` estimator accessors that
/// the parallel-aware executor nodes' `Exec*Estimate` hooks reach across the
/// `execParallel-support` seam. These address the DSM-owned live
/// `ParallelContext` this crate owns.
fn install_execparallel_support_pcxt_seams() {
    use backend_executor_execParallel_support_seams as sup;
    sup::pcxt_nworkers::set(pcxt_nworkers);
    sup::pcxt_estimate_chunk::set(pcxt_estimate_chunk);
    sup::pcxt_estimate_keys::set(pcxt_estimate_keys);

    // `InstrInit(&GetInstrumentationArray(sei)[i], opts)`: this crate owns the
    // `SharedExecutorInstrumentation` DSM header (and thus `instrument_offset`),
    // so it resolves the array base; `InstrInit` itself is the instrument crate's
    // body. `GetInstrumentationArray(sei) == (char*)sei + sei->instrument_offset`.
    sup::instr_init_slot::set(|sei, i, instrument_options| {
        let array_base = sei.0 + read_sei_header(sei).instrument_offset as usize;
        // SAFETY: the leader sized the chunk for `num_workers * num_plan_nodes`
        // `Instrumentation` slots starting at `instrument_offset`; `i` is in range.
        unsafe {
            backend_executor_instrument::instr_init_slot_at(array_base, i, instrument_options)
        }
    });
}

/// Install the thin DSM helpers that bridge the `DsmSegmentHandle` carrier (its
/// value *is* `DsmSegmentId::as_u64()`, opacity-inherited) into the merged
/// `dsm-core` segment API, plus the per-session-handle byte read/write into the
/// real DSM chunk.
fn install_dsm_helper_seams() {
    use backend_storage_ipc_dsm_core::dsm as dsm_core;

    // `dsm_detach((dsm_segment *) seg)` — drop the segment's mapping and run its
    // on-detach callbacks. Mirrors the leader/worker explicit detach.
    rt::dsm_detach::set(|seg| dsm_core::dsm_detach(seg_id_of(seg)));
    // Same body, distinct slot name (the worker-shutdown path uses the
    // `_handle` flavor reached via `dsm_segment_from_datum`).
    rt::dsm_detach_handle::set(|seg| dsm_core::dsm_detach(seg_id_of(seg)));

    // `dsm_segment_handle(seg)` — the integer `dsm_handle` name of a segment.
    rt::dsm_segment_handle::set(|seg| Ok(dsm_core::dsm_segment_handle(seg_id_of(seg))));

    // `(dsm_segment *) DatumGetPointer(arg)` — the on-detach callback `arg` is
    // the segment handle word (opacity-inherited: handle value == id == the
    // machine word the C `Datum` carried). Recover the `DsmSegmentHandle`.
    rt::dsm_segment_from_datum::set(|arg| Ok(DsmSegmentHandle(arg.as_usize())));

    // `*(dsm_handle *) space = handle` — write the per-session DSM handle into
    // its `shm_toc_allocate`'d chunk. SAFETY: `base` is the start of a
    // `sizeof(dsm_handle)` chunk the leader reserved; it is writable and
    // suitably aligned for a `u32`.
    rt::write_dsm_handle::set(|base, value| {
        unsafe {
            core::ptr::write(base as *mut dsm_handle, value);
        }
        Ok(())
    });
    // `*(dsm_handle *) space` — read it back in the worker. SAFETY: as above;
    // the leader wrote a `dsm_handle` here before publishing the chunk.
    rt::read_dsm_handle::set(|base| {
        let v = unsafe { core::ptr::read(base as *const dsm_handle) };
        Ok(v)
    });
}

/// Install the `FixedParallelState` DSM driver — the in-segment byte layout and
/// its genuine cross-process spinlock (see `fps_driver`).
fn install_fps_driver_seams() {
    rt::fps_init::set(|base, state| {
        fps_driver::fps_init(base, state);
        Ok(())
    });
    rt::fps_read::set(|base| Ok(fps_driver::fps_read(base)));
    rt::fps_reset_last_xlog_end::set(|base| {
        fps_driver::fps_reset_last_xlog_end(base);
        Ok(())
    });
    rt::fps_get_last_xlog_end::set(|base| Ok(fps_driver::fps_get_last_xlog_end(base)));
    rt::fps_report_last_rec_end::set(|base, last_xlog_end| {
        fps_driver::fps_report_last_rec_end(base, last_xlog_end);
        Ok(())
    });

    // `ParallelLeaderProcNumber` (globals.c): read by `GetProcNumberForTempRelations`
    // and the bgworker-state serializers, written only by `ParallelWorkerMain`.
    // Defaults to `INVALID_PROC_NUMBER` in any non-parallel-worker backend.
    rt::parallel_leader_proc_number::set(|| with_globals(|g| g.parallel_leader_proc_number));
    rt::set_parallel_leader_proc_number::set(|procno| {
        with_globals(|g| g.parallel_leader_proc_number = procno);
        Ok(())
    });
}

// ===========================================================================
// Runtime test: the DSM-init core over a REAL dsm-core segment.
// ===========================================================================

#[cfg(test)]
mod dsm_substrate_tests {
    //! Drive `InitializeParallelDSM`'s DSM-init core
    //! ([`establish_parallel_segment`] + the `shm_toc` allocate/insert/lookup
    //! helpers) over a *real* `dsm-core` segment, using the merged
    //! `dsm_test_bringup()` harness (feature `test-bringup`). This proves the
    //! conversion off the `Vec<u8>` emulation actually creates a real DSM
    //! segment and that the TOC chunks resolve to real, in-segment addresses —
    //! the whole point of family `dsm-substrate-convert`.

    use std::sync::{Mutex, Once};

    use backend_storage_ipc_dsm_core::dsm::{dsm_segment_address, dsm_segment_map_length};
    use backend_storage_ipc_dsm_core::test_bringup::dsm_test_bringup;

    use super::*;

    static TEST_LOCK: Mutex<()> = Mutex::new(());
    static INSTALL_TOP_MCX: Once = Once::new();

    fn guard() -> std::sync::MutexGuard<'static, ()> {
        TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// The `top_memory_context()` seam body for the test: the bring-up's
    /// `TopMemoryContext` stand-in. `dsm_test_bringup()` is idempotent per
    /// thread and returns that same thread-local `Mcx<'static>`, so the seam
    /// (a plain `fn` pointer, no captures) can recover it here.
    fn test_top_mcx() -> Mcx<'static> {
        dsm_test_bringup()
    }

    /// Install the `top_memory_context` seam once for the whole test process
    /// (seam slots are process-global; a second `set` panics "installed twice").
    fn install_top_mcx_once() {
        INSTALL_TOP_MCX
            .call_once(|| backend_utils_mmgr_mcxt_seams::top_memory_context::set(test_top_mcx));
    }

    /// Push a bare context into the registry directly (the seam-driven
    /// `create_parallel_context` would need the transaction/error-stack seams,
    /// which are out of family A's scope). Mirrors the post-`palloc0`
    /// `ParallelContext` with `nworkers` set.
    fn new_test_context(nworkers: i32) -> ParallelContextHandle {
        with_globals(|g| {
            let slot = g.slots.len();
            g.slots.push(Some(ParallelContext {
                subid: 0,
                nworkers,
                nworkers_to_launch: nworkers,
                nworkers_launched: 0,
                library_name: "postgres".to_string(),
                function_name: "ParallelQueryMain".to_string(),
                error_context_stack: 0,
                estimator: shm_toc_estimator::default(),
                seg: DsmSegmentHandle::NULL,
                seg_guard: None,
                private_memory: None,
                toc: None,
                toc_base: 0,
                worker: Vec::new(),
                nknown_attached_workers: 0,
                known_attached_workers: Vec::new(),
            }));
            let h = ParallelContextHandle(slot);
            g.list.insert(0, h);
            h
        })
    }

    /// Tear a test context back down (the registry slot drop + segment detach /
    /// private-memory free), mirroring `DestroyParallelContext`'s resource
    /// release without the worker/bgworker seams.
    fn drop_test_context(pcxt: ParallelContextHandle, top: Mcx<'static>) {
        with_globals(|g| {
            if let Some(pos) = g.list.iter().position(|&p| p == pcxt) {
                g.list.remove(pos);
            }
        });
        let (seg_guard, private_memory) = with_globals(|g| {
            let c = g.get_mut(pcxt);
            c.seg = DsmSegmentHandle::NULL;
            c.toc = None;
            c.toc_base = 0;
            (c.seg_guard.take(), c.private_memory.take())
        });
        drop(seg_guard);
        if let Some(pm) = private_memory {
            // SAFETY: pm was allocated from this same TopMemoryContext.
            unsafe { top.deallocate(pm.ptr, pm.layout) };
        }
        with_globals(|g| {
            if let Some(slot) = g.slots.get_mut(pcxt.0) {
                *slot = None;
            }
        });
    }

    /// With workers budgeted, the DSM-init core creates a REAL dsm-core segment,
    /// builds a real `shm_toc` over its base, and the allocate/insert/lookup
    /// helpers round-trip chunks to real, in-segment addresses.
    #[test]
    fn dsm_init_core_creates_real_segment_and_resolves_chunks() {
        let _g = guard();
        let top = dsm_test_bringup();
        // The DSM-init core reaches TopMemoryContext through this seam; point it
        // at the bring-up's TopMemoryContext stand-in.
        install_top_mcx_once();

        let pcxt = new_test_context(/* nworkers */ 2);

        // Estimate: fixed state + one error queue + an entrypoint string, plus
        // their keys — enough to size a non-trivial segment.
        let est = pcxt_estimator(pcxt);
        shm_toc_estimate_chunk(est, core::mem::size_of::<FixedParallelState>());
        shm_toc_estimate_keys(est, 1);
        shm_toc_estimate_chunk(est, PARALLEL_ERROR_QUEUE_SIZE * 2);
        shm_toc_estimate_keys(est, 1);
        shm_toc_estimate_chunk(est, 32);
        shm_toc_estimate_keys(est, 1);

        let segsize = with_globals(|g| shm_toc_estimate(&g.get(pcxt).estimator)).unwrap();
        assert!(segsize >= PARALLEL_ERROR_QUEUE_SIZE * 2);

        // The DSM-init core: real dsm_create + ShmToc::create.
        establish_parallel_segment(pcxt, segsize).expect("establish_parallel_segment");

        // A real segment was created (no private-memory fallback), and its id is
        // the real dsm-core id carried by the handle.
        let (seg_handle, base_recorded, has_guard, no_private) = with_globals(|g| {
            let c = g.get(pcxt);
            (c.seg, c.toc_base, c.seg_guard.is_some(), c.private_memory.is_none())
        });
        assert!(!seg_handle.is_null(), "expected a real DSM segment");
        assert!(has_guard, "expected the real DsmSegment guard to be held");
        assert!(no_private, "expected no private-memory fallback with workers");

        // The recorded base equals the real dsm_segment_address for the id the
        // handle carries (opacity-inherited: handle value == DsmSegmentId).
        let id = seg_id_of(seg_handle);
        let real_base = dsm_segment_address(id);
        assert!(!real_base.is_null());
        assert_eq!(base_recorded, real_base as usize);
        assert!(dsm_segment_map_length(id) >= segsize);

        // Allocate + insert + look up chunks through the real shm_toc; every
        // resolved chunk must be a real address inside the mapped segment.
        let toc = pcxt_toc(pcxt);
        let fps = shm_toc_allocate(toc, core::mem::size_of::<FixedParallelState>());
        shm_toc_insert(toc, PARALLEL_KEY_FIXED, fps);
        let eq = shm_toc_allocate(toc, PARALLEL_ERROR_QUEUE_SIZE * 2);
        shm_toc_insert(toc, PARALLEL_KEY_ERROR_QUEUE, eq);

        let seg_lo = real_base as usize;
        let seg_hi = seg_lo + segsize;
        for key in [PARALLEL_KEY_FIXED, PARALLEL_KEY_ERROR_QUEUE] {
            let found = shm_toc_lookup(toc, key, false).expect("key present");
            // The cursor IS the real chunk address (family shm-toc-address).
            let addr = found.0;
            let _ = base_recorded;
            assert!(
                addr >= seg_lo && addr < seg_hi,
                "chunk for key {key:#x} resolves to a real in-segment address"
            );
            // The chunk is genuinely writable shared memory.
            unsafe {
                let p = addr as *mut u8;
                p.write(0xAB);
                assert_eq!(p.read(), 0xAB);
            }
        }

        // A missing key with no_error returns None (real shm_toc_lookup).
        assert!(shm_toc_lookup(toc, PARALLEL_KEY_GUC, true).is_none());

        drop_test_context(pcxt, top);
    }

    /// The typed chunk store/load contract (family `shm-toc-address`): the
    /// `repr(C)` payloads are written and read back IN PLACE at the real chunk
    /// address — no side tables. Proves `store_fixed_state`/`fixed_*`,
    /// `store_cstring`/`cursor_cstring`, `store_instrumentation_header`/`sei_*`/
    /// `set_sei_plan_node_id`, `store_jit_instrumentation_header`/
    /// `shared_jit_num_workers`, and `write_entrypoint`/`read_entrypoint`
    /// round-trip through real in-segment memory.
    #[test]
    fn typed_chunks_round_trip_in_place_at_real_address() {
        let _g = guard();
        let top = dsm_test_bringup();
        install_top_mcx_once();

        let pcxt = new_test_context(/* nworkers */ 2);

        // Size a segment big enough for everything below.
        let est = pcxt_estimator(pcxt);
        shm_toc_estimate_chunk(est, 4096);
        shm_toc_estimate_keys(est, 6);
        let segsize = with_globals(|g| shm_toc_estimate(&g.get(pcxt).estimator)).unwrap();
        establish_parallel_segment(pcxt, segsize).expect("establish_parallel_segment");
        let toc = pcxt_toc(pcxt);
        let base = with_globals(|g| g.get(pcxt).toc_base);
        let seg_hi = base + segsize;
        let in_seg = |c: SerializeCursor| c.0 >= base && c.0 < seg_hi;

        // FixedParallelExecutorState: write in place, read each field back.
        let fps_chunk =
            shm_toc_allocate(toc, core::mem::size_of::<FixedParallelExecutorState>());
        assert!(in_seg(fps_chunk));
        let fpes = store_fixed_state(
            fps_chunk,
            FixedParallelExecutorState {
                tuples_needed: 42,
                param_exec: 7,
                eflags: 3,
                jit_flags: 5,
            },
        );
        assert_eq!(fpes.0, fps_chunk.0, "handle is a thin view over the address");
        assert_eq!(fixed_tuples_needed(fpes), 42);
        assert_eq!(fixed_param_exec(fpes), 7);
        assert_eq!(fixed_eflags(fpes), 3);
        assert_eq!(fixed_jit_flags(fpes), 5);
        set_fixed_param_exec(fpes, 99);
        assert_eq!(fixed_param_exec(fpes), 99);
        // Reinterpreting the same chunk recovers the same data.
        assert_eq!(fixed_eflags(fixed_state_from_chunk(fps_chunk)), 3);

        // cstring: store + read back NUL-terminated.
        let s_chunk = shm_toc_allocate(toc, 16);
        store_cstring(s_chunk, "hello".to_string());
        assert_eq!(cursor_cstring(s_chunk).unwrap(), "hello");

        // SharedExecutorInstrumentation header + plan_node_id flexible array.
        let num_plan_nodes = 3;
        let num_workers = 2;
        let instr_len = SEI_PLAN_NODE_ID_OFFSET + 4 * (num_plan_nodes as usize);
        let i_chunk = shm_toc_allocate(toc, instr_len);
        let sei = store_instrumentation_header(
            i_chunk,
            SharedExecutorInstrumentation {
                instrument_options: 11,
                instrument_offset: instr_len as i32,
                num_workers,
                num_plan_nodes,
            },
        );
        assert_eq!(sei_instrument_options(sei), 11);
        assert_eq!(sei_num_workers(sei), num_workers);
        assert_eq!(sei_num_plan_nodes(sei), num_plan_nodes);
        for i in 0..num_plan_nodes {
            set_sei_plan_node_id(sei, i, 100 + i);
        }
        for i in 0..num_plan_nodes {
            assert_eq!(sei_plan_node_id(sei, i), 100 + i);
        }
        // The header reinterpreted from the chunk reads identically.
        assert_eq!(sei_num_plan_nodes(instrumentation_from_chunk(i_chunk)), num_plan_nodes);

        // SharedJitInstrumentation header.
        let jit_chunk = shm_toc_allocate(toc, 256);
        let jit = store_jit_instrumentation_header(jit_chunk, 4);
        assert_eq!(shared_jit_num_workers(jit), 4);
        assert_eq!(shared_jit_num_workers(jit_instrumentation_from_chunk(jit_chunk)), 4);

        // Entrypoint "library\0function\0" bytes round-trip.
        let e_chunk = shm_toc_allocate(toc, "postgres".len() + "ParallelQueryMain".len() + 2);
        write_entrypoint(e_chunk, "postgres", "ParallelQueryMain");
        let (lib, func) = read_entrypoint(e_chunk).unwrap();
        assert_eq!(lib, "postgres");
        assert_eq!(func, "ParallelQueryMain");

        drop_test_context(pcxt, top);
    }

    /// The typed-shared-DSM-object primitive (`shared_dsm_object`): the leader
    /// `place_and_init`s a `repr(C)` object — with a launch-once scalar, an
    /// in-segment `Spinlock`, and a `pg_atomic_uint64` — at a real
    /// `shm_toc_allocate`'d chunk, the worker `attach`es to it via the SAME real
    /// `shm_toc_lookup` address, and the concurrently-mutated fields round-trip
    /// through the interior-mutable accessors. This is the cross-process
    /// aliasing model `ParallelTableScanDescData` (phs_relid scalar + phs_mutex
    /// spinlock + phs_nallocated atomic) rides on.
    #[test]
    fn shared_dsm_object_place_init_and_worker_attach_over_real_dsm() {
        use core::sync::atomic::Ordering;
        use shared_dsm_object::{attach, estimate, place_and_init, SharedDsmObject, SharedView};
        use types_storage::storage::{pg_atomic_uint64, Spinlock};

        // A `repr(C)` per-node shared object shaped like `ParallelTableScanDescData`:
        // a launch-once leader-write scalar, an in-segment spinlock, and a shared
        // atomic counter. Every concurrently-mutated field is interior-mutable.
        #[repr(C)]
        struct DemoShared {
            /// launch-once scalar (leader writes pre-launch, workers read).
            relid: u32,
            /// `slock_t` protecting the allocation cursor.
            mutex: Spinlock,
            /// `pg_atomic_uint64` block distributor.
            nallocated: pg_atomic_uint64,
        }
        // SAFETY (audited for the test): repr(C); the scalar is launch-once, the
        // spinlock and atomic are interior-mutable; the initializer below fully
        // initializes every field.
        unsafe impl SharedDsmObject for DemoShared {}

        let _g = guard();
        let top = dsm_test_bringup();
        install_top_mcx_once();

        let pcxt = new_test_context(/* nworkers */ 2);
        let est = pcxt_estimator(pcxt);
        shm_toc_estimate_chunk(est, estimate::<DemoShared>());
        shm_toc_estimate_keys(est, 1);
        let segsize = with_globals(|g| shm_toc_estimate(&g.get(pcxt).estimator)).unwrap();
        establish_parallel_segment(pcxt, segsize).expect("establish_parallel_segment");

        let toc = pcxt_toc(pcxt);
        // The execParallel-visible `dsm_segment *` handle the primitive ties its
        // borrow to — exactly what a per-node `Exec*InitializeDSM`/`Worker` hook
        // receives from the `pcxt_seg`/`pwcxt_seg` seam.
        let seg = pcxt_seg(pcxt).expect("segment established");

        // --- leader: allocate a real chunk, place + init the object in place. ---
        let chunk = shm_toc_allocate(toc, estimate::<DemoShared>());
        let leader_ref = place_and_init::<DemoShared>(seg, chunk, |view: &SharedView<DemoShared>| {
            // SAFETY: pre-launch the leader is the sole writer; `view.as_ptr()`
            // addresses >= size_of writable suitably-aligned in-segment bytes.
            let p = unsafe { view.as_ptr() };
            unsafe {
                // launch-once scalar by plain write; the atomic/spinlock by
                // their in-place constructors (mirrors SpinLockInit /
                // pg_atomic_init_u64).
                (*p).relid = 12345;
                core::ptr::write(&mut (*p).mutex, Spinlock::new());
                core::ptr::write(&mut (*p).nallocated, pg_atomic_uint64::new(0));
            }
        });
        shm_toc_insert(toc, PARALLEL_KEY_FIXED, chunk);

        // Leader bumps the shared atomic before workers launch.
        leader_ref.get().nallocated.value.fetch_add(7, Ordering::Relaxed);

        // --- worker: shm_toc_lookup returns the SAME real address; attach. ---
        let found = shm_toc_lookup(toc, PARALLEL_KEY_FIXED, false).expect("key present");
        assert_eq!(found.0, chunk.0, "worker lookup resolves the leader's chunk");
        let worker_ref = attach::<DemoShared>(seg, found);

        // Launch-once scalar reads back by copy.
        assert_eq!(worker_ref.get().relid, 12345);
        // The atomic is genuinely shared: the worker sees the leader's bump and
        // its own fetch_add is visible back to the leader (same physical bytes).
        assert_eq!(worker_ref.get().nallocated.value.load(Ordering::Relaxed), 7);
        worker_ref.get().nallocated.value.fetch_add(35, Ordering::Relaxed);
        assert_eq!(leader_ref.get().nallocated.value.load(Ordering::Relaxed), 42);

        // The spinlock guards a critical section across the shared mapping.
        {
            use backend_storage_lmgr_s_lock::{s_lock_macro, s_unlock};
            let lock = &worker_ref.get().mutex;
            s_lock_macro(lock, Some(file!()), line!() as i32, Some("test"));
            // ... critical section ...
            s_unlock(lock);
        }

        drop_test_context(pcxt, top);
    }

    /// With no workers budgeted, the DSM-init core takes the backend-private
    /// memory fallback: no real DSM segment, a private buffer, and the shm_toc
    /// still resolves chunks to real addresses inside that buffer.
    #[test]
    fn dsm_init_core_no_workers_uses_private_memory() {
        let _g = guard();
        let top = dsm_test_bringup();
        install_top_mcx_once();

        let pcxt = new_test_context(/* nworkers */ 0);
        let est = pcxt_estimator(pcxt);
        shm_toc_estimate_chunk(est, core::mem::size_of::<FixedParallelState>());
        shm_toc_estimate_keys(est, 1);

        let segsize = with_globals(|g| shm_toc_estimate(&g.get(pcxt).estimator)).unwrap();
        establish_parallel_segment(pcxt, segsize).expect("establish_parallel_segment");

        let (seg_null, has_private, base) = with_globals(|g| {
            let c = g.get(pcxt);
            (c.seg.is_null(), c.private_memory.is_some(), c.toc_base)
        });
        assert!(seg_null, "no DSM segment when nworkers == 0");
        assert!(has_private, "expected the private-memory fallback");
        assert!(base != 0);

        let toc = pcxt_toc(pcxt);
        let fps = shm_toc_allocate(toc, core::mem::size_of::<FixedParallelState>());
        shm_toc_insert(toc, PARALLEL_KEY_FIXED, fps);
        let found = shm_toc_lookup(toc, PARALLEL_KEY_FIXED, false).expect("fixed key present");
        // The cursor IS the real chunk address (family shm-toc-address).
        let addr = found.0;
        assert!(addr >= base && addr < base + segsize);
        unsafe {
            let p = addr as *mut u8;
            p.write(0xCD);
            assert_eq!(p.read(), 0xCD);
        }

        drop_test_context(pcxt, top);
    }

    /// Family `worker-attach`: a worker genuinely attaches to a segment a
    /// *leader* created and resolves a real chunk address through the worker-side
    /// `ShmToc`. This drives the converted `ParallelWorkerMain` attach core
    /// (`worker_attach_segment` -> real `dsm_attach`; `ShmToc::attach` over
    /// `dsm_segment_address`; `worker_lookup`/`worker_lookup_opt` -> real
    /// in-segment lookups) — no emulation tokens, no fabricated ids.
    #[test]
    fn worker_attaches_to_leader_segment_and_resolves_real_chunk() {
        use backend_storage_ipc_dsm_core::dsm::{
            dsm_create, dsm_detach, dsm_pin_segment, dsm_segment_handle, dsm_unpin_segment,
        };

        let _g = guard();
        let top = dsm_test_bringup();
        install_top_mcx_once();

        // --- LEADER: create a real DSM segment, lay a real shm_toc over it,
        // allocate + insert a chunk, and write a recognizable value into it. ---
        const SIZE: usize = 8192;
        let leader_seg = dsm_create(SIZE, 0, top)
            .expect("dsm_create errored")
            .expect("dsm_create returned None");
        let id = leader_seg.id();
        let handle = dsm_segment_handle(id);
        let leader_base = dsm_segment_address(id);
        assert!(!leader_base.is_null());
        let leader_base_nn = NonNull::new(leader_base).unwrap();
        // SAFETY: freshly created, page-aligned, SIZE-byte writable segment.
        let leader_toc = unsafe { ShmToc::create(PARALLEL_MAGIC, leader_base_nn, SIZE) };
        const TEST_KEY: u64 = PARALLEL_KEY_FIXED;
        const SENTINEL: u8 = 0xC7;
        let chunk = leader_toc.allocate(64).expect("leader allocate");
        // SAFETY: chunk is a real in-segment address from this segment.
        unsafe { chunk.as_ptr().write(SENTINEL) };
        let chunk_off = chunk.as_ptr() as usize - leader_base as usize;
        // SAFETY: chunk addresses live shared memory in the segment.
        unsafe { leader_toc.insert(TEST_KEY, chunk).expect("leader insert") };

        // Pin the segment so it survives with no mapping, then detach the leader's
        // mapping — exactly the state a freshly-started worker backend sees.
        dsm_pin_segment(id).expect("dsm_pin_segment");
        dsm_detach(leader_seg.into_id()).expect("dsm_detach (leader)");

        // --- WORKER: the converted attach core. ---
        let seg = worker_attach_segment(handle)
            .expect("worker_attach_segment errored")
            .expect("worker_attach_segment returned None for a live (pinned) segment");
        let wid = seg.id();
        let wbase = dsm_segment_address(wid);
        assert!(!wbase.is_null());
        let wbase_nn = NonNull::new(wbase).unwrap();
        // Real shm_toc_attach over the worker's mapping: the magic the leader
        // wrote must match.
        // SAFETY: wbase addresses the worker's live mapping of the same segment.
        let real_toc = unsafe { ShmToc::attach(PARALLEL_MAGIC, wbase_nn) }
            .expect("ShmToc::attach: magic must match the leader's");
        let toc_base = wbase as usize;
        WORKER_ATTACHED.with(|w| {
            w.borrow_mut().push(WorkerAttached {
                base: toc_base,
                _seg_guard: seg,
                toc: real_toc,
            })
        });
        let toc = ExecShmToc(toc_base);

        // The worker resolves the leader's chunk to a REAL in-segment address
        // (not the leader's address — a different mapping of the same object) and
        // reads back the leader's sentinel through it.
        let found = worker_lookup(toc, TEST_KEY).expect("worker_lookup");
        assert!(
            found >= toc_base && found < toc_base + SIZE,
            "resolved chunk must be a real address inside the worker's mapping"
        );
        assert_eq!(
            found - toc_base,
            chunk_off,
            "the worker resolves the same chunk offset the leader inserted"
        );
        // SAFETY: `found` is a real, mapped, in-segment address.
        assert_eq!(
            unsafe { (found as *const u8).read() },
            SENTINEL,
            "the worker reads the leader's bytes back across the unmap/remap"
        );

        // A missing key with no_error returns 0 (real shm_toc_lookup, noError).
        assert_eq!(worker_lookup_opt(toc, PARALLEL_KEY_GUC), 0);

        // --- teardown: drop the worker guard (detach), unpin the segment. ---
        let guard_seg = WORKER_ATTACHED.with(|w| {
            let mut w = w.borrow_mut();
            let pos = w.iter().position(|e| e.base == toc_base).unwrap();
            w.remove(pos)
        });
        dsm_detach(guard_seg._seg_guard.into_id()).expect("dsm_detach (worker)");
        dsm_unpin_segment(handle).expect("dsm_unpin_segment");
        let _ = top;
    }

    // -----------------------------------------------------------------------
    // Family C2 (worker-shm-mq): leader/worker error-queue round-trip over a
    // real DSM segment.
    //
    // This drives the exact worker error-queue attach call sites from
    // `parallel_worker_main` — `shm_mq_at` / `shm_mq_set_sender_to_myproc`
    // / `shm_mq_attach(seg_to_exec(seg))` (the real OPTION (i) shm_mq seam layer)
    // — over the *real* chunk the leader laid in a real `dsm_create` segment, and
    // verifies an actual byte round-trip through `shm_mq_send` / `shm_mq_receive`.
    // The attach threads the real `DsmSegmentId` (so the queue gets a real
    // `on_dsm_detach`) and the real `my_latch()`; nothing is emulated.
    // -----------------------------------------------------------------------

    use std::sync::Once as ShmMqOnce;

    static INSTALL_SHM_MQ: ShmMqOnce = ShmMqOnce::new();

    /// Install the OPTION (i) shm_mq seam layer plus the few harness seams the
    /// send/receive/attach paths reach that `dsm_test_bringup` does not already
    /// provide (it sets `my_proc_number` to 0 and the interrupt flags). Latches
    /// are single-process no-ops: `wait_latch` returns immediately so any
    /// blocking loop makes one extra non-blocking trip and re-examines the queue,
    /// exactly as the shm_mq unit-test harness does.
    fn install_shm_mq_seam_layer_once() {
        INSTALL_SHM_MQ.call_once(|| {
            use types_storage::latch::LatchHandle;
            use types_storage::waiteventset::WL_LATCH_SET;

            // The real OPTION (i) registry-backed seam layer.
            backend_storage_ipc_shm_mq::init_seams();

            // shm_mq allocates the handle + on_dsm_detach record in the
            // TopMemoryContext; the bring-up's thread-local stand-in is installed
            // by `install_top_mcx_once`, which every caller of this also calls.

            // Process-latch machinery (single-process cooperating stand-ins).
            backend_storage_lmgr_proc_seams::proc_latch::set(|procno| {
                LatchHandle::new(procno as usize + 1)
            });
            backend_storage_ipc_latch_seams::my_latch::set(|| LatchHandle::new(1));
            backend_storage_ipc_latch_seams::set_latch::set(|_latch| {});
            backend_storage_ipc_latch_seams::reset_latch::set(|_latch| {});
            backend_storage_ipc_latch_seams::wait_latch::set(|_latch, _events, _timeout, _wei| {
                Ok(WL_LATCH_SET as i32)
            });

            // CHECK_FOR_INTERRUPTS in the send/receive loops: nothing pending.
            backend_tcop_postgres_seams::check_for_interrupts::set(|| Ok(()));
        });
    }

    #[test]
    fn leader_worker_error_queue_roundtrip_over_real_dsm() -> PgResult<()> {
        use backend_storage_ipc_dsm_core::dsm::{
            dsm_create, dsm_detach, dsm_pin_segment, dsm_segment_handle, dsm_unpin_segment,
        };
        use backend_storage_ipc_shm_mq::{
            shm_mq_attach as real_attach, shm_mq_create as real_create,
            shm_mq_receive as real_receive, shm_mq_send as real_send,
            shm_mq_set_receiver as real_set_receiver, ShmMq, SHM_MQ_SUCCESS,
        };

        let _g = guard();
        let top = dsm_test_bringup();
        install_top_mcx_once();
        install_shm_mq_seam_layer_once();

        // --- LEADER: a real DSM segment + real shm_toc + a real error-queue
        // region (one PARALLEL_ERROR_QUEUE_SIZE slot for worker 0), inserted
        // under PARALLEL_KEY_ERROR_QUEUE exactly as initialize_parallel_dsm. ---
        const SIZE: usize = 1 << 16;
        let leader_seg = dsm_create(SIZE, 0, top)
            .expect("dsm_create errored")
            .expect("dsm_create returned None");
        let id = leader_seg.id();
        let handle = dsm_segment_handle(id);
        let leader_base = dsm_segment_address(id);
        assert!(!leader_base.is_null());
        let leader_base_nn = NonNull::new(leader_base).unwrap();
        // SAFETY: freshly created, page-aligned, SIZE-byte writable segment.
        let leader_toc = unsafe { ShmToc::create(PARALLEL_MAGIC, leader_base_nn, SIZE) };
        let eq_chunk = leader_toc
            .allocate(PARALLEL_ERROR_QUEUE_SIZE)
            .expect("leader allocate error-queue region");
        // SAFETY: chunk is a real in-segment address from this segment.
        unsafe {
            leader_toc
                .insert(PARALLEL_KEY_ERROR_QUEUE, eq_chunk)
                .expect("leader insert");
        }
        let eq_off = eq_chunk.as_ptr() as usize - leader_base as usize;

        // LEADER receiver side: create the real shm_mq over the chunk, set
        // ourselves (MyProc == 0 under the bring-up) as receiver, and attach
        // without a segment (NULL) — the receiver outlives the queue.
        // SAFETY: `eq_chunk` is a real, MAXALIGN'd, in-segment region.
        let leader_mq = unsafe { real_create(eq_chunk, PARALLEL_ERROR_QUEUE_SIZE) };
        real_set_receiver(leader_mq, 0);
        let mut leader_h = real_attach(
            leader_mq,
            top,
            None,
            None,
            backend_storage_ipc_latch_seams::my_latch::call(),
        )
        .expect("leader shm_mq_attach");

        // Pin + detach the leader's mapping: the state a fresh worker sees.
        dsm_pin_segment(id).expect("dsm_pin_segment");
        dsm_detach(leader_seg.into_id()).expect("dsm_detach (leader)");

        // --- WORKER: the converted attach core resolves the real chunk and
        // drives the *exact* parallel_worker_main error-queue seam calls. ---
        let seg = worker_attach_segment(handle)
            .expect("worker_attach_segment errored")
            .expect("worker_attach_segment returned None for a live (pinned) segment");
        let wid = seg.id();
        let wbase = dsm_segment_address(wid);
        assert!(!wbase.is_null());
        let wbase_nn = NonNull::new(wbase).unwrap();
        // SAFETY: wbase addresses the worker's live mapping of the same segment.
        let real_toc = unsafe { ShmToc::attach(PARALLEL_MAGIC, wbase_nn) }
            .expect("ShmToc::attach: magic must match the leader's");
        let toc_base = wbase as usize;
        WORKER_ATTACHED.with(|w| {
            w.borrow_mut().push(WorkerAttached {
                base: toc_base,
                _seg_guard: seg,
                toc: real_toc,
            })
        });
        let toc = ExecShmToc(toc_base);
        let worker_seg = seg_handle_of(wid);

        // C: error_queue_space = shm_toc_lookup(...); mq = (shm_mq *)(space +
        // ParallelWorkerNumber*SIZE); shm_mq_set_sender(mq, MyProc);
        // mqh = shm_mq_attach(mq, seg, NULL); — through the real seam layer. The
        // worker *casts* the leader-created queue (shm_mq_at), it does not
        // re-create it (that would wipe the leader's receiver).
        let error_queue_space = SerializeCursor(worker_lookup(toc, PARALLEL_KEY_ERROR_QUEUE)?);
        assert_eq!(
            error_queue_space.0 - toc_base,
            eq_off,
            "worker resolves the same error-queue offset the leader inserted"
        );
        let worker_mq = shmmq::shm_mq_at::call(
            error_queue_space,
            /* ParallelWorkerNumber */ 0,
            PARALLEL_ERROR_QUEUE_SIZE,
        );
        shmmq::shm_mq_set_sender_to_myproc::call(worker_mq);
        let worker_mqh = shmmq::shm_mq_attach::call(worker_mq, seg_to_exec(worker_seg))
            .expect("worker shm_mq_attach over real DsmSegmentId");

        // The receiver-side queue now observes the worker as the sender (this is
        // exactly wait_for_parallel_workers_to_attach's probe).
        let probe = shmmq::shm_mq_get_queue::call(worker_mqh);
        assert!(
            shmmq::shm_mq_get_sender::call(probe).is_some(),
            "leader must see the worker attached as sender on the real chunk"
        );

        // --- DATA ROUND-TRIP: the worker sends a message; the leader receives
        // it, proving real bytes flow worker -> leader through the real shm_mq
        // ring over the real DSM chunk. ---
        // SAFETY: the worker-side queue base is the real in-segment chunk address.
        let worker_send_mq =
            unsafe { ShmMq::from_base(NonNull::new(worker_mq.0 as *mut u8).unwrap()) };
        let mut worker_send_h = real_attach(
            worker_send_mq,
            top,
            None,
            None,
            backend_storage_ipc_latch_seams::my_latch::call(),
        )
        .expect("worker sender shm_mq_attach");

        let msg = b"worker error frame";
        // SAFETY: both handles wrap the same live, attached in-segment queue.
        let sres = unsafe { real_send(&mut worker_send_h, msg, false, true) }.expect("shm_mq_send");
        assert_eq!(sres, SHM_MQ_SUCCESS);
        // SAFETY: leader_h wraps the live attached queue; non-blocking receive.
        let (rres, payload) =
            unsafe { real_receive(&mut leader_h, false) }.expect("shm_mq_receive");
        assert_eq!(rres, SHM_MQ_SUCCESS);
        assert_eq!(payload, msg, "leader reads back the worker's bytes");

        // --- teardown: detach worker handles, drop the worker guard, unpin. ---
        shmmq::shm_mq_detach::call(worker_mqh);
        let guard_seg = WORKER_ATTACHED.with(|w| {
            let mut w = w.borrow_mut();
            let pos = w.iter().position(|e| e.base == toc_base).unwrap();
            w.remove(pos)
        });
        dsm_detach(guard_seg._seg_guard.into_id()).expect("dsm_detach (worker)");
        dsm_unpin_segment(handle).expect("dsm_unpin_segment");
        let _ = top;
        Ok(())
    }

    /// Family `tqueue-substrate-check` (integration): drive execParallel's
    /// *tuple-queue* path — `ExecParallelSetupTupleQueues` (leader) +
    /// `ExecParallelGetReceiver` (worker) — through the exact same real seam
    /// layer, proving the tuple queues are real shm_mq rings over real in-segment
    /// chunk addresses, with the real `DsmSegmentId` + `Mcx<'static>` (the handle
    /// is allocated in `top_memory_context()`) + `MyLatch` threaded into
    /// `shm_mq_attach`. Mirrors the error-queue round-trip but over a
    /// multi-worker tqueue region keyed by `PARALLEL_KEY_TUPLE_QUEUE`, with the
    /// leader as receiver and the worker as sender (the C tuple-flow direction).
    #[test]
    fn tuple_queue_roundtrip_over_real_dsm() -> PgResult<()> {
        use backend_storage_ipc_dsm_core::dsm::{dsm_create, dsm_detach};
        use backend_storage_ipc_shm_mq::{
            shm_mq_attach as real_attach, shm_mq_send as real_send, ShmMq, SHM_MQ_SUCCESS,
        };

        // execParallel.c constants (execParallel.c:69, the TUPLE_QUEUE TOC key).
        const PARALLEL_KEY_TUPLE_QUEUE: u64 = 0xE000000000000005;
        const PARALLEL_TUPLE_QUEUE_SIZE: usize = 65536;
        // Two workers — exercises the `i*SIZE` chunk arithmetic in both
        // `shm_mq_create_at` and the worker's `shm_mq_at` cast.
        const NWORKERS: i32 = 2;
        // The worker we round-trip through (the C `ParallelWorkerNumber`).
        const WORKER: i32 = 1;

        let _g = guard();
        let top = dsm_test_bringup();
        install_top_mcx_once();
        install_shm_mq_seam_layer_once();

        // --- LEADER: real DSM segment + real shm_toc; allocate the whole
        // `mul_size(SIZE, nworkers)` tuple-queue region and create+attach one
        // queue per worker, exactly as ExecParallelSetupTupleQueues. ---
        let segsize: usize = mul_size(PARALLEL_TUPLE_QUEUE_SIZE, NWORKERS as usize)? + (1 << 16);
        let leader_seg = dsm_create(segsize, 0, top)
            .expect("dsm_create errored")
            .expect("dsm_create returned None");
        let id = leader_seg.id();
        let leader_base = dsm_segment_address(id);
        assert!(!leader_base.is_null());
        let leader_base_nn = NonNull::new(leader_base).unwrap();
        // SAFETY: freshly created, page-aligned, segsize-byte writable segment.
        let leader_toc = unsafe { ShmToc::create(PARALLEL_MAGIC, leader_base_nn, segsize) };
        let tq_region = leader_toc
            .allocate(mul_size(PARALLEL_TUPLE_QUEUE_SIZE, NWORKERS as usize)?)
            .expect("leader allocate tuple-queue region");
        // SAFETY: chunk is a real in-segment address from this segment.
        unsafe {
            leader_toc
                .insert(PARALLEL_KEY_TUPLE_QUEUE, tq_region)
                .expect("leader insert tuple-queue region");
        }
        let tq_off = tq_region.as_ptr() as usize - leader_base as usize;
        let tqueuespace = SerializeCursor(tq_region.as_ptr() as usize);

        // Per-worker: create the real shm_mq at `region + i*SIZE`, set ourselves
        // (the leader, MyProc==0) as receiver, attach over the real seg. This is
        // the verbatim ExecParallelSetupTupleQueues loop body.
        let mut leader_handles = Vec::new();
        for i in 0..NWORKERS {
            let mq = shmmq::shm_mq_create_at::call(tqueuespace, i, PARALLEL_TUPLE_QUEUE_SIZE);
            shmmq::shm_mq_set_receiver_to_myproc::call(mq);
            leader_handles.push(shmmq::shm_mq_attach::call(mq, seg_to_exec(seg_handle_of(id)))?);
        }
        assert_eq!(leader_handles.len(), NWORKERS as usize);

        // --- WORKER (same process): unlike the error-queue round-trip we keep
        // the leader's mapping LIVE rather than detach+re-attach, because the
        // faithful `ExecParallelSetupTupleQueues` attaches the leader's receiver
        // handles with `Some(seg)` (the real C call), so detaching the leader's
        // mapping in a single process would fire their `on_dsm_detach` and mark
        // the queues detached. The leader's `toc`/base IS the shared segment
        // here; the worker resolves the same tuple-queue region over it and casts
        // + attaches its own queue (ExecParallelGetReceiver: shm_mq_at,
        // set_sender, attach over the real `seg`). ---
        let real_toc = unsafe { ShmToc::attach(PARALLEL_MAGIC, leader_base_nn) }
            .expect("ShmToc::attach: magic must match the leader's");
        let toc_base = leader_base as usize;
        WORKER_ATTACHED.with(|w| {
            w.borrow_mut().push(WorkerAttached {
                base: toc_base,
                _seg_guard: leader_seg,
                toc: real_toc,
            })
        });
        let toc = ExecShmToc(toc_base);
        let worker_seg = seg_handle_of(id);

        let mqspace = SerializeCursor(worker_lookup(toc, PARALLEL_KEY_TUPLE_QUEUE)?);
        assert_eq!(
            mqspace.0 - toc_base,
            tq_off,
            "worker resolves the same tuple-queue offset the leader inserted"
        );
        // C: mq = (shm_mq *)(mqspace + ParallelWorkerNumber*SIZE) — a cast, not a
        // re-create (re-creating would wipe the leader's mq_set_receiver).
        let worker_mq =
            shmmq::shm_mq_at::call(mqspace, WORKER, PARALLEL_TUPLE_QUEUE_SIZE);
        shmmq::shm_mq_set_sender_to_myproc::call(worker_mq);
        let worker_mqh = shmmq::shm_mq_attach::call(worker_mq, seg_to_exec(worker_seg))
            .expect("worker tuple-queue shm_mq_attach over real DsmSegmentId");

        // The leader's receiver queue for WORKER now sees the worker as sender —
        // the wait_for_parallel_workers_to_attach probe.
        let probe = shmmq::shm_mq_get_queue::call(worker_mqh);
        assert!(
            shmmq::shm_mq_get_sender::call(probe).is_some(),
            "leader must see the worker attached as sender on the real tuple chunk"
        );

        // --- DATA ROUND-TRIP: worker sends a tuple frame; leader receives it on
        // the matching per-worker queue, proving real bytes flow over the real
        // DSM tuple chunk. ---
        // SAFETY: the worker-side queue base is the real in-segment chunk address.
        let worker_send_mq =
            unsafe { ShmMq::from_base(NonNull::new(worker_mq.0 as *mut u8).unwrap()) };
        let mut worker_send_h = real_attach(
            worker_send_mq,
            top,
            None,
            None,
            backend_storage_ipc_latch_seams::my_latch::call(),
        )
        .expect("worker tuple sender shm_mq_attach");

        let frame = b"parallel tuple frame";
        // SAFETY: both handles wrap the same live, attached in-segment queue.
        let sres =
            unsafe { real_send(&mut worker_send_h, frame, false, true) }.expect("shm_mq_send");
        assert_eq!(sres, SHM_MQ_SUCCESS);

        // The leader receives through the SAME registry handle it attached as
        // receiver for WORKER (`leader_handles[WORKER]`) — no second attach
        // (that would reset the ring). This is exactly the seam the tuple-queue
        // readers (`TupleQueueReceiver`) use: `shm_mq_receive::call`.
        let (rres, payload) = shmmq::shm_mq_receive::call(leader_handles[WORKER as usize])?;
        assert_eq!(rres, Some(ShmMqResult::Success));
        assert_eq!(payload, frame, "leader reads back the worker's tuple bytes");

        // --- teardown: detach the seam handles (cancels their on_dsm_detach),
        // then release the single shared mapping. ---
        shmmq::shm_mq_detach::call(worker_mqh);
        for h in leader_handles {
            shmmq::shm_mq_detach::call(h);
        }
        let entry = WORKER_ATTACHED.with(|w| {
            let mut w = w.borrow_mut();
            let pos = w.iter().position(|e| e.base == toc_base).unwrap();
            w.remove(pos)
        });
        dsm_detach(entry._seg_guard.into_id()).expect("dsm_detach (segment)");
        let _ = (top, frame, worker_send_h);
        Ok(())
    }
}
