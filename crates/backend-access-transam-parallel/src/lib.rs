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
//! The DSM segment is owned here as a byte buffer; `shm_toc` estimate/allocate/
//! insert/lookup and the typed chunk store/load helpers operate on it. A
//! [`SerializeCursor`] is `(ctx_slot << 32) | offset` into that buffer.
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

use std::cell::RefCell;

use backend_utils_error::{elog, ereport, PgResult};
use mcx::Mcx;
use types_core::{pid_t, Size, SubTransactionId, XLogRecPtr};
use types_datum::Datum;
use types_error::{
    ERRCODE_ADMIN_SHUTDOWN, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERROR, FATAL, WARNING,
};
use types_execparallel::{
    BackgroundWorkerHandle, DsmSegmentHandle as ExecDsmSeg, FixedParallelExecutorState,
    FixedStateHandle, InstrumentationHandle, JitInstrumentationHandle, ParallelContextHandle,
    ParallelWorkerContextHandle, SerializeCursor, SharedExecutorInstrumentation,
    ShmTocEstimatorHandle, ShmTocHandle as ExecShmToc,
};
use types_parallel::{
    dsm_handle, BgwHandle, BgwHandleStatus, DsmSegmentHandle, FixedParallelState, ShmMqHandleHandle,
    ShmMqResult,
};

use backend_access_transam_parallel_rt_seams as rt;

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

/// `ERROR` elevel as a raw int, for `Min(edata.elevel, ERROR)` (elog.h: 21).
const ERROR_ELEVEL: i32 = 21;

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

/// Per-worker leader-side state (`ParallelWorkerInfo`, access/parallel.h:24-28).
#[derive(Clone, Copy, Debug)]
struct ParallelWorkerInfo {
    /// `BackgroundWorkerHandle *bgwhandle`.
    bgwhandle: BgwHandle,
    /// `shm_mq_handle *error_mqh`.
    error_mqh: ShmMqHandleHandle,
}

impl ParallelWorkerInfo {
    const fn new() -> Self {
        Self {
            bgwhandle: BgwHandle::NULL,
            error_mqh: ShmMqHandleHandle::NULL,
        }
    }
}

/// A live `shm_toc` over a context's DSM buffer: a magic + the key→chunk table.
#[derive(Clone, Debug, Default)]
struct ShmToc {
    /// `(key, offset, nbytes)` of each inserted chunk.
    entries: Vec<(u64, usize, usize)>,
    /// Next free offset in the buffer (chunks allocated bump-style, BUFFERALIGN'd).
    alloc_cursor: usize,
}

/// `shm_toc_estimator` (storage/shm_toc.h:38-42) — accumulated estimate.
#[derive(Clone, Copy, Debug, Default)]
struct Estimator {
    space_for_chunks: Size,
    number_of_keys: Size,
}

/// A parallel execution context (`ParallelContext`, access/parallel.h:30-46)
/// plus the DSM buffer this subsystem owns for it.
#[derive(Clone, Debug)]
struct ParallelContext {
    subid: SubTransactionId,
    nworkers: i32,
    nworkers_to_launch: i32,
    nworkers_launched: i32,
    library_name: String,
    function_name: String,
    /// `ErrorContextCallback *error_context_stack` — opaque pointer handle.
    error_context_stack: usize,
    estimator: Estimator,
    /// `dsm_segment *seg` (NULL when running in private memory).
    seg: DsmSegmentHandle,
    /// `void *private_memory` base handle (0 == NULL).
    private_memory: usize,
    /// The DSM (or private) byte buffer backing the `shm_toc`.
    buffer: Vec<u8>,
    toc: ShmToc,
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
    /// `volatile sig_atomic_t ParallelMessagePending = false;`
    parallel_message_pending: bool,
    /// `bool InitializingParallelWorker = false;`
    initializing_parallel_worker: bool,
    /// `static FixedParallelState *MyFixedParallelState;` — base handle (0=NULL).
    my_fixed_parallel_state: usize,
    /// `static pid_t ParallelLeaderPid;`
    parallel_leader_pid: pid_t,
}

impl ParallelGlobals {
    const fn new() -> Self {
        Self {
            slots: Vec::new(),
            list: Vec::new(),
            parallel_worker_number: -1,
            parallel_message_pending: false,
            initializing_parallel_worker: false,
            my_fixed_parallel_state: 0,
            parallel_leader_pid: 0,
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
}

fn with_globals<R>(f: impl FnOnce(&mut ParallelGlobals) -> R) -> R {
    G.with(|g| f(&mut g.borrow_mut()))
}

// ===========================================================================
// Cursor codec: SerializeCursor encodes (ctx_slot << 32) | offset. The high bit
// distinguishes a context-owned chunk from a freestanding pointer-sized one.
// ===========================================================================

const CURSOR_OFFSET_BITS: usize = 32;
const CURSOR_OFFSET_MASK: usize = (1usize << CURSOR_OFFSET_BITS) - 1;

fn make_cursor(slot: usize, offset: usize) -> SerializeCursor {
    SerializeCursor((slot << CURSOR_OFFSET_BITS) | (offset & CURSOR_OFFSET_MASK))
}

fn cursor_parts(c: SerializeCursor) -> (usize, usize) {
    (c.0 >> CURSOR_OFFSET_BITS, c.0 & CURSOR_OFFSET_MASK)
}

/// shm_toc handle and dsm-seg handle encode the owning context slot directly
/// (the execParallel contract threads the same handle through every call).
fn toc_handle(slot: usize) -> ExecShmToc {
    ExecShmToc(slot)
}
fn toc_slot(toc: ExecShmToc) -> usize {
    toc.0
}

/// `BUFFERALIGN` (c.h) — align to `MAXIMUM_ALIGNOF`-padded 8 KB buffer line; the
/// shm_toc rounds chunk sizes up to `BUFFERALIGN`.
const BUFFER_ALIGNMENT: usize = 64;
fn buffer_align(n: usize) -> usize {
    (n + BUFFER_ALIGNMENT - 1) & !(BUFFER_ALIGNMENT - 1)
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

fn shm_toc_estimate_chunk(e: ShmTocEstimatorHandle, sz: Size) {
    with_globals(|g| {
        let c = g.get_mut(ParallelContextHandle(estimator_slot_of(e)));
        // shm_toc_estimate: space_for_chunks += BUFFERALIGN(nbytes).
        c.estimator.space_for_chunks += buffer_align(sz);
    });
}

fn shm_toc_estimate_keys(e: ShmTocEstimatorHandle, nkeys: i32) {
    with_globals(|g| {
        let c = g.get_mut(ParallelContextHandle(estimator_slot_of(e)));
        c.estimator.number_of_keys += nkeys as Size;
    });
}

/// `shm_toc_allocate(toc, nbytes)` — bump-allocate a BUFFERALIGN'd chunk.
fn shm_toc_allocate(toc: ExecShmToc, nbytes: Size) -> SerializeCursor {
    with_globals(|g| {
        let slot = toc_slot(toc);
        let c = g.get_mut(ParallelContextHandle(slot));
        let offset = c.toc.alloc_cursor;
        let nalloc = buffer_align(nbytes);
        let end = offset + nalloc;
        if end > c.buffer.len() {
            c.buffer.resize(end, 0);
        }
        c.toc.alloc_cursor = end;
        make_cursor(slot, offset)
    })
}

fn shm_toc_insert(toc: ExecShmToc, key: u64, address: SerializeCursor) {
    with_globals(|g| {
        let slot = toc_slot(toc);
        let (_aslot, offset) = cursor_parts(address);
        let c = g.get_mut(ParallelContextHandle(slot));
        c.toc.entries.push((key, offset, 0));
    });
}

/// `shm_toc_lookup(toc, key, noError)` — `None` when `noError` and absent.
fn shm_toc_lookup(toc: ExecShmToc, key: u64, no_error: bool) -> Option<SerializeCursor> {
    with_globals(|g| {
        let slot = toc_slot(toc);
        let c = g.get(ParallelContextHandle(slot));
        match c.toc.entries.iter().find(|(k, _, _)| *k == key) {
            Some((_, offset, _)) => Some(make_cursor(slot, *offset)),
            None => {
                if no_error {
                    None
                } else {
                    // shm_toc_lookup elog(ERROR) on missing key. The execParallel
                    // contract is infallible here, so a missing required key is a
                    // programming error: panic loudly (matches elog(ERROR) which
                    // never returns).
                    panic!("could not find key {key:#x} in shm TOC");
                }
            }
        }
    })
}

// ===========================================================================
// Typed DSM chunk stores/loads. The chunk addresses index into the context
// buffer; the typed payloads are stored in a side table keyed by (slot,offset)
// because the execParallel structs are richer than raw bytes here. The store
// helpers satisfy execParallel's serialization seam contract.
// ===========================================================================

thread_local! {
    static FIXED_STATES: RefCell<Vec<(SerializeCursor, FixedParallelExecutorState)>> =
        const { RefCell::new(Vec::new()) };
    static CSTRINGS: RefCell<Vec<(SerializeCursor, String)>> = const { RefCell::new(Vec::new()) };
    static INSTRUMENTATIONS: RefCell<Vec<(SerializeCursor, SharedExecutorInstrumentation)>> =
        const { RefCell::new(Vec::new()) };
    static JIT_HEADERS: RefCell<Vec<(SerializeCursor, i32)>> = const { RefCell::new(Vec::new()) };
}

fn store_fixed_state(chunk: SerializeCursor, state: FixedParallelExecutorState) -> FixedStateHandle {
    FIXED_STATES.with(|s| {
        let mut v = s.borrow_mut();
        if let Some(slot) = v.iter_mut().find(|(c, _)| *c == chunk) {
            slot.1 = state;
        } else {
            v.push((chunk, state));
        }
    });
    FixedStateHandle(chunk.0)
}

fn fixed_state_from_chunk(chunk: SerializeCursor) -> FixedStateHandle {
    FixedStateHandle(chunk.0)
}

fn with_fixed_state<R>(fpes: FixedStateHandle, f: impl FnOnce(&mut FixedParallelExecutorState) -> R) -> R {
    let chunk = SerializeCursor(fpes.0);
    FIXED_STATES.with(|s| {
        let mut v = s.borrow_mut();
        match v.iter_mut().find(|(c, _)| *c == chunk) {
            Some((_, st)) => f(st),
            None => {
                v.push((chunk, FixedParallelExecutorState::default()));
                let n = v.len() - 1;
                f(&mut v[n].1)
            }
        }
    })
}

fn set_fixed_param_exec(fpes: FixedStateHandle, dp: u64) {
    with_fixed_state(fpes, |st| st.param_exec = dp);
}
fn fixed_param_exec(fpes: FixedStateHandle) -> u64 {
    with_fixed_state(fpes, |st| st.param_exec)
}
fn fixed_eflags(fpes: FixedStateHandle) -> i32 {
    with_fixed_state(fpes, |st| st.eflags)
}
fn fixed_jit_flags(fpes: FixedStateHandle) -> i32 {
    with_fixed_state(fpes, |st| st.jit_flags)
}
fn fixed_tuples_needed(fpes: FixedStateHandle) -> i64 {
    with_fixed_state(fpes, |st| st.tuples_needed)
}

fn store_cstring(chunk: SerializeCursor, value: String) {
    CSTRINGS.with(|s| {
        let mut v = s.borrow_mut();
        if let Some(slot) = v.iter_mut().find(|(c, _)| *c == chunk) {
            slot.1 = value;
        } else {
            v.push((chunk, value));
        }
    });
}

fn cursor_cstring(chunk: SerializeCursor) -> PgResult<String> {
    CSTRINGS.with(|s| {
        s.borrow()
            .iter()
            .find(|(c, _)| *c == chunk)
            .map(|(_, val)| val.clone())
            .ok_or_else(|| {
                ereport(ERROR)
                    .errmsg("no string stored at parallel DSM chunk")
                    .into_error()
            })
    })
}

fn store_instrumentation_header(
    chunk: SerializeCursor,
    header: SharedExecutorInstrumentation,
) -> InstrumentationHandle {
    INSTRUMENTATIONS.with(|s| {
        let mut v = s.borrow_mut();
        if let Some(slot) = v.iter_mut().find(|(c, _)| *c == chunk) {
            slot.1 = header;
        } else {
            v.push((chunk, header));
        }
    });
    InstrumentationHandle(chunk.0)
}

fn instrumentation_from_chunk(chunk: SerializeCursor) -> InstrumentationHandle {
    InstrumentationHandle(chunk.0)
}

fn with_instrumentation<R>(
    sei: InstrumentationHandle,
    f: impl FnOnce(&mut SharedExecutorInstrumentation) -> R,
) -> R {
    let chunk = SerializeCursor(sei.0);
    INSTRUMENTATIONS.with(|s| {
        let mut v = s.borrow_mut();
        match v.iter_mut().find(|(c, _)| *c == chunk) {
            Some((_, st)) => f(st),
            None => {
                v.push((chunk, SharedExecutorInstrumentation::default()));
                let n = v.len() - 1;
                f(&mut v[n].1)
            }
        }
    })
}

fn sei_instrument_options(sei: InstrumentationHandle) -> i32 {
    with_instrumentation(sei, |s| s.instrument_options)
}
fn sei_num_workers(sei: InstrumentationHandle) -> i32 {
    with_instrumentation(sei, |s| s.num_workers)
}
fn sei_num_plan_nodes(sei: InstrumentationHandle) -> i32 {
    with_instrumentation(sei, |s| s.num_plan_nodes)
}
fn sei_plan_node_id(sei: InstrumentationHandle, index: i32) -> i32 {
    with_instrumentation(sei, |s| s.plan_node_id[index as usize])
}
fn set_sei_plan_node_id(sei: InstrumentationHandle, index: i32, value: i32) {
    with_instrumentation(sei, |s| {
        let idx = index as usize;
        if s.plan_node_id.len() <= idx {
            s.plan_node_id.resize(idx + 1, 0);
        }
        s.plan_node_id[idx] = value;
    });
}

fn store_jit_instrumentation_header(chunk: SerializeCursor, num_workers: i32) -> JitInstrumentationHandle {
    JIT_HEADERS.with(|s| {
        let mut v = s.borrow_mut();
        if let Some(slot) = v.iter_mut().find(|(c, _)| *c == chunk) {
            slot.1 = num_workers;
        } else {
            v.push((chunk, num_workers));
        }
    });
    JitInstrumentationHandle(chunk.0)
}

fn jit_instrumentation_from_chunk(chunk: SerializeCursor) -> JitInstrumentationHandle {
    JitInstrumentationHandle(chunk.0)
}

fn shared_jit_num_workers(shared_jit: JitInstrumentationHandle) -> i32 {
    let chunk = SerializeCursor(shared_jit.0);
    JIT_HEADERS.with(|s| {
        s.borrow()
            .iter()
            .find(|(c, _)| *c == chunk)
            .map(|(_, n)| *n)
            .unwrap_or(0)
    })
}

// ===========================================================================
// Accessor seams on the live ParallelContext (execParallel reads pcxt->field).
// ===========================================================================

fn pcxt_nworkers(pcxt: ParallelContextHandle) -> i32 {
    with_globals(|g| g.get(pcxt).nworkers)
}
fn pcxt_nworkers_launched(pcxt: ParallelContextHandle) -> i32 {
    with_globals(|g| g.get(pcxt).nworkers_launched)
}
fn pcxt_estimator(pcxt: ParallelContextHandle) -> ShmTocEstimatorHandle {
    // The estimator is part of the context; address it by the same slot.
    ShmTocEstimatorHandle(pcxt.0)
}
fn pcxt_toc(pcxt: ParallelContextHandle) -> ExecShmToc {
    toc_handle(pcxt.0)
}
fn pcxt_seg(pcxt: ParallelContextHandle) -> Option<ExecDsmSeg> {
    with_globals(|g| {
        let c = g.get(pcxt);
        if c.seg.is_null() {
            None
        } else {
            Some(ExecDsmSeg(c.seg.0))
        }
    })
}
fn pcxt_worker_bgwhandle(pcxt: ParallelContextHandle, i: i32) -> BackgroundWorkerHandle {
    with_globals(|g| BackgroundWorkerHandle(g.get(pcxt).worker[i as usize].bgwhandle.0))
}
fn make_parallel_worker_context(seg: ExecDsmSeg, toc: ExecShmToc) -> ParallelWorkerContextHandle {
    // {seg, toc} pair handed to per-node Exec*InitializeWorker hooks; encode the
    // toc slot (both share the context identity).
    let _ = seg;
    ParallelWorkerContextHandle(toc.0)
}
fn parallel_worker_number() -> i32 {
    with_globals(|g| g.parallel_worker_number)
}

// ===========================================================================
// IsParallelWorker / ParallelMessagePending / signal interrupt (the small seams
// declared in backend-access-transam-parallel-seams).
// ===========================================================================

/// `IsParallelWorker()` (access/parallel.h:60) — `ParallelWorkerNumber >= 0`.
pub fn is_parallel_worker() -> bool {
    with_globals(|g| g.parallel_worker_number) >= 0
}

/// Assign `ParallelWorkerNumber`.
fn set_parallel_worker_number(value: i32) {
    with_globals(|g| g.parallel_worker_number = value);
}

fn set_parallel_message_pending(value: bool) {
    with_globals(|g| g.parallel_message_pending = value);
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
        estimator: Estimator::default(),
        seg: DsmSegmentHandle::NULL,
        private_memory: 0,
        buffer: Vec::new(),
        toc: ShmToc::default(),
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

/// Fallibly allocate the zeroed `segsize`-byte buffer that backs the TOC.
///
/// This stands in for the segment storage (`dsm_create` on the worker path,
/// `MemoryContextAlloc(TopMemoryContext, segsize)` on the no-worker fallback)
/// until a real cross-process DSM layout lands. `segsize` is caller-controlled
/// (the estimator total), so the allocation must be fallible: OOM converts to
/// the context's `mcx.oom(segsize)` (`ERRCODE_OUT_OF_MEMORY` + context name)
/// rather than aborting the process via `vec![0u8; segsize]`.
fn alloc_zeroed_buffer(mcx: Mcx<'_>, segsize: usize) -> PgResult<Vec<u8>> {
    mcx::check_alloc_size(segsize)?;
    let mut buffer: Vec<u8> = Vec::new();
    buffer.try_reserve_exact(segsize).map_err(|_| mcx.oom(segsize))?;
    buffer.resize(segsize, 0);
    Ok(buffer)
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
        session_dsm_handle = rt::get_session_dsm_handle::call()?;
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
            tsnaplen = rt::estimate_snapshot_space::call(transaction_snapshot)?;
            shm_toc_estimate_chunk(est, tsnaplen);
        }
        asnaplen = rt::estimate_snapshot_space::call(active_snapshot)?;
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

    // Create DSM and initialize TOC. If no workers, use backend-private memory.
    // Also fall back if dsm_create hits the max-segments limit.
    let segsize = with_globals(|g| {
        let c = g.get(pcxt);
        c.estimator.space_for_chunks
    });
    if pcxt_nworkers(pcxt) > 0 {
        let seg = rt::dsm_create_null_if_maxsegments::call(segsize)?;
        with_globals(|g| g.get_mut(pcxt).seg = DsmSegmentHandle(seg.0));
    }
    if !with_globals(|g| g.get(pcxt).seg).is_null() {
        // The TOC lives in the DSM segment; back it with a context buffer.
        // C: dsm_create(segsize). Fallible alloc: OOM on the caller-controlled
        // segsize must carry ERRCODE_OUT_OF_MEMORY (mcx.oom), not abort.
        let buffer = alloc_zeroed_buffer(mcx, segsize)?;
        with_globals(|g| {
            let c = g.get_mut(pcxt);
            c.buffer = buffer;
            c.toc = ShmToc::default();
        });
    } else {
        let pm = rt::top_memory_context_alloc::call(segsize)?;
        // C: MemoryContextAlloc(TopMemoryContext, segsize). Same fallible rule.
        let buffer = alloc_zeroed_buffer(mcx, segsize)?;
        with_globals(|g| {
            let c = g.get_mut(pcxt);
            c.nworkers = 0;
            c.private_memory = pm;
            c.buffer = buffer;
            c.toc = ShmToc::default();
        });
    }

    let toc = pcxt_toc(pcxt);

    // Initialize fixed-size state in shared memory.
    let fps = shm_toc_allocate(toc, core::mem::size_of::<FixedParallelState>());
    {
        let init = rt::collect_fixed_parallel_state::call()?;
        let (_slot, offset) = cursor_parts(fps);
        rt::fps_init::call(offset, init)?;
    }
    shm_toc_insert(toc, PARALLEL_KEY_FIXED, fps);

    // Skip the rest if not budgeting for workers.
    if pcxt_nworkers(pcxt) > 0 {
        // Serialize shared libraries we have loaded.
        let libraryspace = shm_toc_allocate(toc, library_len);
        rt::serialize_library_state::call(library_len, cursor_parts(libraryspace).1)?;
        shm_toc_insert(toc, PARALLEL_KEY_LIBRARY, libraryspace);

        // Serialize GUC settings.
        let gucspace = shm_toc_allocate(toc, guc_len);
        rt::serialize_guc_state::call(guc_len, cursor_parts(gucspace).1)?;
        shm_toc_insert(toc, PARALLEL_KEY_GUC, gucspace);

        // Serialize combo CID state.
        let combocidspace = shm_toc_allocate(toc, combocidlen);
        rt::serialize_combocid_state::call(combocidlen, cursor_parts(combocidspace).1)?;
        shm_toc_insert(toc, PARALLEL_KEY_COMBO_CID, combocidspace);

        // Serialize the transaction snapshot if the isolation level uses one.
        if rt::isolation_uses_xact_snapshot::call() {
            let tsnapspace = shm_toc_allocate(toc, tsnaplen);
            rt::serialize_snapshot::call(transaction_snapshot, cursor_parts(tsnapspace).1)?;
            shm_toc_insert(toc, PARALLEL_KEY_TRANSACTION_SNAPSHOT, tsnapspace);
        }

        // Serialize the active snapshot.
        let asnapspace = shm_toc_allocate(toc, asnaplen);
        rt::serialize_snapshot::call(active_snapshot, cursor_parts(asnapspace).1)?;
        shm_toc_insert(toc, PARALLEL_KEY_ACTIVE_SNAPSHOT, asnapspace);

        // Provide the handle for per-session segment.
        let session_dsm_handle_space = shm_toc_allocate(toc, core::mem::size_of::<dsm_handle>());
        rt::write_dsm_handle::call(cursor_parts(session_dsm_handle_space).1, session_dsm_handle)?;
        shm_toc_insert(toc, PARALLEL_KEY_SESSION_DSM, session_dsm_handle_space);

        // Serialize transaction state.
        let tstatespace = shm_toc_allocate(toc, tstatelen);
        rt::serialize_transaction_state::call(tstatelen, cursor_parts(tstatespace).1)?;
        shm_toc_insert(toc, PARALLEL_KEY_TRANSACTION_STATE, tstatespace);

        // Serialize pending syncs.
        let pendingsyncsspace = shm_toc_allocate(toc, pendingsyncslen);
        rt::serialize_pending_syncs::call(pendingsyncslen, cursor_parts(pendingsyncsspace).1)?;
        shm_toc_insert(toc, PARALLEL_KEY_PENDING_SYNCS, pendingsyncsspace);

        // Serialize reindex state.
        let reindexspace = shm_toc_allocate(toc, reindexlen);
        rt::serialize_reindex_state::call(reindexlen, cursor_parts(reindexspace).1)?;
        shm_toc_insert(toc, PARALLEL_KEY_REINDEX_STATE, reindexspace);

        // Serialize relmapper state.
        let relmapperspace = shm_toc_allocate(toc, relmapperlen);
        rt::serialize_relation_map::call(relmapperlen, cursor_parts(relmapperspace).1)?;
        shm_toc_insert(toc, PARALLEL_KEY_RELMAPPER_STATE, relmapperspace);

        // Serialize uncommitted enum state.
        let uncommittedenumsspace = shm_toc_allocate(toc, uncommittedenumslen);
        rt::serialize_uncommitted_enums::call(cursor_parts(uncommittedenumsspace).1, uncommittedenumslen)?;
        shm_toc_insert(toc, PARALLEL_KEY_UNCOMMITTEDENUMS, uncommittedenumsspace);

        // Serialize our ClientConnectionInfo.
        let clientconninfospace = shm_toc_allocate(toc, clientconninfolen);
        rt::serialize_client_connection_info::call(clientconninfolen, cursor_parts(clientconninfospace).1)?;
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
        let eq_base = cursor_parts(error_queue_space).1;
        let mut i = 0;
        while i < nworkers {
            let start = eq_base + (i as Size) * PARALLEL_ERROR_QUEUE_SIZE;
            let mq = rt::shm_mq_create::call(start, PARALLEL_ERROR_QUEUE_SIZE)?;
            rt::shm_mq_set_receiver_to_myproc::call(mq)?;
            workers[i as usize].error_mqh = rt::shm_mq_attach::call(mq, seg_to_parallel(seg), BgwHandle::NULL)?;
            i += 1;
        }
        with_globals(|g| g.get_mut(pcxt).worker = workers);
        shm_toc_insert(toc, PARALLEL_KEY_ERROR_QUEUE, error_queue_space);

        // Serialize entrypoint information. "library\0function\0".
        let (library_name, function_name) =
            with_globals(|g| (g.get(pcxt).library_name.clone(), g.get(pcxt).function_name.clone()));
        let entrypointstate = shm_toc_allocate(toc, library_name.len() + function_name.len() + 2);
        rt::write_entrypoint::call(cursor_parts(entrypointstate).1, &library_name, &function_name)?;
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

/// Convert the leader-side execParallel DSM-seg handle to the parallel-subsystem
/// handle the rt seams use.
fn seg_to_parallel(seg: DsmSegmentHandle) -> DsmSegmentHandle {
    seg
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
    rt::fps_reset_last_xlog_end::call(cursor_parts(fps).1)?;

    // Recreate error queues (if they exist).
    if pcxt_nworkers(pcxt) > 0 {
        let error_queue_space =
            shm_toc_lookup(toc, PARALLEL_KEY_ERROR_QUEUE, false).ok_or_else(missing_error_queue_key)?;
        let eq_base = cursor_parts(error_queue_space).1;
        let nworkers = pcxt_nworkers(pcxt);
        let seg = with_globals(|g| g.get(pcxt).seg);
        let mut i = 0;
        while i < nworkers {
            let start = eq_base + (i as Size) * PARALLEL_ERROR_QUEUE_SIZE;
            let mq = rt::shm_mq_create::call(start, PARALLEL_ERROR_QUEUE_SIZE)?;
            rt::shm_mq_set_receiver_to_myproc::call(mq)?;
            let mqh = rt::shm_mq_attach::call(mq, seg_to_parallel(seg), BgwHandle::NULL)?;
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

    // (The BackgroundWorker struct assembly — memset/snprintf, bgw_extra memcpy
    // of `i`, bgw_main_arg = dsm_segment_handle(seg) — is performed by the
    // runtime in register_dynamic_background_worker.)
    let seg = with_globals(|g| g.get(pcxt).seg);
    let mut i = 0;
    while i < nworkers_to_launch {
        let mut bgwhandle = BgwHandle::NULL;
        let registered = if !any_registrations_failed {
            bgwhandle = rt::register_dynamic_background_worker::call(seg_to_parallel(seg), i)?;
            !bgwhandle.is_null()
        } else {
            false
        };

        if registered {
            let error_mqh = with_globals(|g| {
                let c = g.get_mut(pcxt);
                c.worker[i as usize].bgwhandle = bgwhandle;
                c.worker[i as usize].error_mqh
            });
            rt::shm_mq_set_handle::call(error_mqh, bgwhandle)?;
            with_globals(|g| g.get_mut(pcxt).nworkers_launched += 1);
        } else {
            // We've hit the max_worker_processes limit; future registrations
            // will probably fail too, so skip them. But still forget about the
            // error queues we budgeted for these workers.
            any_registrations_failed = true;
            let error_mqh = with_globals(|g| {
                let c = g.get_mut(pcxt);
                c.worker[i as usize].bgwhandle = BgwHandle::NULL;
                c.worker[i as usize].error_mqh
            });
            rt::shm_mq_detach::call(error_mqh)?;
            with_globals(|g| g.get_mut(pcxt).worker[i as usize].error_mqh = ShmMqHandleHandle::NULL);
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
            if error_mqh.is_null() {
                with_globals(|g| {
                    let c = g.get_mut(pcxt);
                    c.known_attached_workers[i as usize] = true;
                    c.nknown_attached_workers += 1;
                });
                i += 1;
                continue;
            }

            let bgwhandle = with_globals(|g| g.get(pcxt).worker[i as usize].bgwhandle);
            let (status, _pid) = rt::get_background_worker_pid::call(bgwhandle)?;
            if status == BgwHandleStatus::Started {
                // Has the worker attached to the error queue?
                let mq = rt::shm_mq_get_queue::call(error_mqh)?;
                if !rt::shm_mq_get_sender::call(mq)?.is_null() {
                    with_globals(|g| {
                        let c = g.get_mut(pcxt);
                        c.known_attached_workers[i as usize] = true;
                        c.nknown_attached_workers += 1;
                    });
                }
            } else if status == BgwHandleStatus::Stopped {
                // If the worker stopped without attaching, throw an error.
                let mq = rt::shm_mq_get_queue::call(error_mqh)?;
                if rt::shm_mq_get_sender::call(mq)?.is_null() {
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
                    c.worker[i as usize].error_mqh.is_null(),
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
                if error_mqh.is_null()
                    || bgwhandle.is_null()
                    || rt::get_background_worker_pid::call(bgwhandle)?.0 != BgwHandleStatus::Stopped
                {
                    i += 1;
                    continue;
                }

                // Check whether the worker stopped without ever attaching to the
                // error queue. If so, throw an error.
                let mq = rt::shm_mq_get_queue::call(error_mqh)?;
                if rt::shm_mq_get_sender::call(mq)?.is_null() {
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
    if !with_globals(|g| g.get(pcxt).buffer.is_empty()) {
        let fps = shm_toc_lookup(toc, PARALLEL_KEY_FIXED, false).ok_or_else(missing_fixed_key)?;
        let last = rt::fps_get_last_xlog_end::call(cursor_parts(fps).1)?;
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
        if worker_null || bgwhandle.is_null() {
            i += 1;
            continue;
        }

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
        with_globals(|g| g.get_mut(pcxt).worker[i as usize].bgwhandle = BgwHandle::NULL);
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
            if !error_mqh.is_null() {
                rt::terminate_background_worker::call(bgwhandle)?;
                rt::shm_mq_detach::call(error_mqh)?;
                with_globals(|g| g.get_mut(pcxt).worker[i as usize].error_mqh = ShmMqHandleHandle::NULL);
            }
            i += 1;
        }
    }

    // If we allocated a shared memory segment, detach it.
    let seg = with_globals(|g| g.get(pcxt).seg);
    if !seg.is_null() {
        rt::dsm_detach::call(seg_to_parallel(seg))?;
        with_globals(|g| g.get_mut(pcxt).seg = DsmSegmentHandle::NULL);
    }

    // If this context is in backend-private memory, free that instead.
    let private_memory = with_globals(|g| g.get(pcxt).private_memory);
    if private_memory != 0 {
        rt::pfree::call(private_memory)?;
        with_globals(|g| g.get_mut(pcxt).private_memory = 0);
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
                if error_mqh.is_null() {
                    break;
                }
                let (res, data) = rt::shm_mq_receive::call(error_mqh)?;
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
            // Parse ErrorResponse or NoticeResponse.
            let edata = rt::pq_parse_errornotice::call(body)?;

            // Death of a worker isn't enough justification for suicide.
            let elevel = edata.elevel.min(ERROR_ELEVEL);

            // Add a context line to show this is from a parallel worker (skip in
            // DEBUG_PARALLEL_REGRESS for test stability).
            let mut context = edata.context;
            if rt::debug_parallel_query::call() != DEBUG_PARALLEL_REGRESS {
                context = Some(match context {
                    Some(ctx) => format!("{ctx}\nparallel worker"),
                    None => "parallel worker".to_string(),
                });
            }

            // Context beyond that should use the error context callbacks in
            // effect when the ParallelContext was created.
            let pcxt_stack = with_globals(|g| g.get(pcxt).error_context_stack);
            rt::throw_parallel_error_data::call(elevel, context.as_deref(), pcxt_stack)?;
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
            rt::shm_mq_detach::call(error_mqh)?;
            with_globals(|g| g.get_mut(pcxt).worker[i as usize].error_mqh = ShmMqHandleHandle::NULL);
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
pub fn parallel_worker_main(main_arg: Datum) -> PgResult<()> {
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

    // Attach to the DSM segment and find its TOC. The worker side genuinely
    // crosses processes (the hard core); the DSM/shm_toc attach lives in the
    // owning subsystem behind the rt seams.
    let seg = rt::dsm_attach::call(datum_as_u32(main_arg) as dsm_handle)?;
    if seg.is_null() {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg("could not map dynamic shared memory segment")
            .into_error());
    }
    let toc_base = rt::shm_toc_attach::call(rt::dsm_segment_address::call(seg)?)?;
    if toc_base == 0 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg("invalid magic number in dynamic shared memory segment")
            .into_error());
    }
    // The worker-side toc lookups address the attached segment buffer; the
    // worker uses a context-less toc handle derived from the attach base.
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
    let error_queue_space = worker_lookup(toc, PARALLEL_KEY_ERROR_QUEUE)?;
    let mq_addr =
        error_queue_space + (with_globals(|g| g.parallel_worker_number) as Size) * PARALLEL_ERROR_QUEUE_SIZE;
    let mq = rt::shm_mq_create::call(mq_addr, PARALLEL_ERROR_QUEUE_SIZE)?;
    rt::shm_mq_set_sender_to_myproc::call(mq)?;
    let mqh = rt::shm_mq_attach::call(mq, seg, BgwHandle::NULL)?;
    rt::pq_redirect_to_shm_mq::call(seg, mqh)?;
    rt::pq_set_parallel_leader::call(fps.parallel_leader_pid, fps.parallel_leader_proc_number)?;

    // Join locking group. If we can't, the leader has gone away, so exit quietly.
    if !rt::become_lock_group_member::call(fps.parallel_leader_pgproc, fps.parallel_leader_pid)? {
        return Ok(());
    }

    // Restore transaction and statement start-time timestamps.
    rt::set_parallel_start_timestamps::call(fps.xact_ts, fps.stmt_ts)?;

    // Identify the entry point to be called.
    let entrypointstate = worker_lookup(toc, PARALLEL_KEY_ENTRYPOINT)?;
    let (library_name, function_name) = rt::read_entrypoint::call(entrypointstate)?;
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
        asnapshot
    };
    rt::restore_transaction_snapshot::call(tsnapshot, fps.parallel_leader_pgproc)?;
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

/// `shm_toc_lookup(toc, key, false)` on the worker-attached segment. The base
/// address of the chunk is what the worker threads onward. The worker side
/// genuinely crosses into the attached DSM segment, so the lookup is a seam.
fn worker_lookup(toc: ExecShmToc, key: u64) -> PgResult<usize> {
    rt::worker_toc_lookup::call(toc.0, key, false)
}

/// `shm_toc_lookup(toc, key, true)` — 0 when absent.
fn worker_lookup_opt(toc: ExecShmToc, key: u64) -> usize {
    rt::worker_toc_lookup::call(toc.0, key, true).unwrap_or(0)
}

// Convert a `Datum` carrying a `uint32` back to that u32 (UInt32GetDatum inverse).
fn datum_as_u32(d: Datum) -> u32 {
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
pub fn parallel_worker_shutdown(_code: i32, arg: Datum) -> PgResult<()> {
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

/// Install every seam this crate owns.
pub fn init_seams() {
    use backend_access_transam_parallel_seams as seams;

    seams::is_parallel_worker::set(is_parallel_worker);
    seams::handle_parallel_message_interrupt::set(handle_parallel_message_interrupt);
    seams::at_eoxact_parallel::set(at_eoxact_parallel);
    seams::at_eosubxact_parallel::set(at_eosubxact_parallel);
    seams::parallel_worker_report_last_rec_end::set(parallel_worker_report_last_rec_end);

    seams::create_parallel_context::set(create_parallel_context);
    seams::initialize_parallel_dsm::set(initialize_parallel_dsm);
    seams::reinitialize_parallel_dsm::set(reinitialize_parallel_dsm);
    seams::wait_for_parallel_workers_to_finish::set(wait_for_parallel_workers_to_finish);
    seams::destroy_parallel_context::set(destroy_parallel_context);
    seams::pcxt_nworkers::set(pcxt_nworkers);
    seams::pcxt_nworkers_launched::set(pcxt_nworkers_launched);
    seams::pcxt_estimator::set(pcxt_estimator);
    seams::pcxt_toc::set(pcxt_toc);
    seams::pcxt_seg::set(pcxt_seg);
    seams::pcxt_worker_bgwhandle::set(pcxt_worker_bgwhandle);
    seams::make_parallel_worker_context::set(make_parallel_worker_context);
    seams::parallel_worker_number::set(parallel_worker_number);

    seams::shm_toc_estimate_chunk::set(shm_toc_estimate_chunk);
    seams::shm_toc_estimate_keys::set(shm_toc_estimate_keys);
    seams::shm_toc_allocate::set(shm_toc_allocate);
    seams::shm_toc_insert::set(shm_toc_insert);
    seams::shm_toc_lookup::set(shm_toc_lookup);

    seams::store_fixed_state::set(store_fixed_state);
    seams::set_fixed_param_exec::set(set_fixed_param_exec);
    seams::fixed_param_exec::set(fixed_param_exec);
    seams::fixed_eflags::set(fixed_eflags);
    seams::fixed_jit_flags::set(fixed_jit_flags);
    seams::fixed_tuples_needed::set(fixed_tuples_needed);
    seams::fixed_state_from_chunk::set(fixed_state_from_chunk);

    seams::store_cstring::set(store_cstring);
    seams::cursor_cstring::set(cursor_cstring);

    seams::store_instrumentation_header::set(store_instrumentation_header);
    seams::instrumentation_from_chunk::set(instrumentation_from_chunk);
    seams::sei_instrument_options::set(sei_instrument_options);
    seams::sei_num_workers::set(sei_num_workers);
    seams::sei_num_plan_nodes::set(sei_num_plan_nodes);
    seams::sei_plan_node_id::set(sei_plan_node_id);
    seams::set_sei_plan_node_id::set(set_sei_plan_node_id);

    seams::store_jit_instrumentation_header::set(store_jit_instrumentation_header);
    seams::jit_instrumentation_from_chunk::set(jit_instrumentation_from_chunk);
    seams::shared_jit_num_workers::set(shared_jit_num_workers);
}
