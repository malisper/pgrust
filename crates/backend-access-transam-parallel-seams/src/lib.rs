//! Seam declarations for the `backend-access-transam-parallel` unit
//! (`access/transam/parallel.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(unused_doc_comments)]
#![allow(non_snake_case)]

seam_core::seam!(
    /// `IsParallelWorker()` (`access/parallel.h`):
    /// `(ParallelWorkerNumber >= 0)`; `ParallelWorkerNumber` is owned by
    /// `parallel.c`.
    pub fn is_parallel_worker() -> bool
);

seam_core::seam!(
    /// `HandleParallelMessageInterrupt()` (parallel.c) — the
    /// PROCSIG_PARALLEL_MESSAGE arm of `procsignal_sigusr1_handler`.
    /// Signal-handler-safe flag flipping; infallible.
    pub fn handle_parallel_message_interrupt()
);

seam_core::seam!(
    /// `AtEOXact_Parallel(isCommit)` — clean up unfinished parallel workers
    /// at top-level transaction end (warning about leaks on commit).
    pub fn at_eoxact_parallel(is_commit: bool) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `AtEOSubXact_Parallel(isCommit, mySubId)`.
    pub fn at_eosubxact_parallel(
        is_commit: bool,
        my_sub_id: types_core::SubTransactionId,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ParallelWorkerReportLastRecEnd(XactLastRecEnd)` — tell the leader
    /// about WAL this worker wrote.
    pub fn parallel_worker_report_last_rec_end(
        last_rec_end: types_core::XLogRecPtr,
    ) -> types_error::PgResult<()>
);
// ===========================================================================
// ParallelContext machinery (access/parallel.c) used by execParallel.c.
// ===========================================================================

use types_error::PgResult;
use types_execparallel::{
    BackgroundWorkerHandle, DsmSegmentHandle, FixedParallelExecutorState, FixedStateHandle,
    InstrumentationHandle, JitInstrumentationHandle, ParallelContextHandle,
    ParallelWorkerContextHandle, SerializeCursor, SharedExecutorInstrumentation, ShmTocEstimatorHandle,
    ShmTocHandle, Size,
};

/// `CreateParallelContext(library_name, function_name, nworkers)`. C does its
/// allocation in `TopTransactionContext` (it switches to it internally); the
/// caller hands that context's `Mcx` so the `palloc0` of the context and the
/// `pstrdup`'d names go through the fallible allocator with the owning context's
/// OOM error, rather than an ambient global crossing the seam.
seam_core::seam!(pub fn create_parallel_context<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    library_name: String,
    function_name: String,
    nworkers: i32,
) -> PgResult<ParallelContextHandle>);
/// `InitializeParallelDSM(pcxt)`. The worker array (`palloc0` in
/// `TopTransactionContext`) and the segment-backing buffer
/// (`MemoryContextAlloc(TopMemoryContext, segsize)` / `dsm_create`) allocate
/// fallibly through the caller-supplied `Mcx`.
seam_core::seam!(pub fn initialize_parallel_dsm<'mcx>(mcx: mcx::Mcx<'mcx>, pcxt: ParallelContextHandle) -> PgResult<()>);
/// `ReinitializeParallelDSM(pcxt)`.
seam_core::seam!(pub fn reinitialize_parallel_dsm(pcxt: ParallelContextHandle) -> PgResult<()>);
/// `WaitForParallelWorkersToFinish(pcxt)`.
seam_core::seam!(pub fn wait_for_parallel_workers_to_finish(pcxt: ParallelContextHandle) -> PgResult<()>);
/// `DestroyParallelContext(pcxt)`.
seam_core::seam!(pub fn destroy_parallel_context(pcxt: ParallelContextHandle) -> PgResult<()>);
/// `pcxt->nworkers`.
seam_core::seam!(pub fn pcxt_nworkers(pcxt: ParallelContextHandle) -> i32);
/// `pcxt->nworkers_launched`.
seam_core::seam!(pub fn pcxt_nworkers_launched(pcxt: ParallelContextHandle) -> i32);
/// `&pcxt->estimator`.
seam_core::seam!(pub fn pcxt_estimator(pcxt: ParallelContextHandle) -> ShmTocEstimatorHandle);
/// `pcxt->toc`.
seam_core::seam!(pub fn pcxt_toc(pcxt: ParallelContextHandle) -> ShmTocHandle);
/// `pcxt->seg` (`None` when running in private memory, i.e. `seg == NULL`).
seam_core::seam!(pub fn pcxt_seg(pcxt: ParallelContextHandle) -> Option<DsmSegmentHandle>);
/// `pcxt->worker[i].bgwhandle`.
seam_core::seam!(pub fn pcxt_worker_bgwhandle(pcxt: ParallelContextHandle, i: i32) -> BackgroundWorkerHandle);
/// Build the worker's `ParallelWorkerContext { seg, toc }` and return its handle.
seam_core::seam!(pub fn make_parallel_worker_context(
    seg: DsmSegmentHandle,
    toc: ShmTocHandle,
) -> ParallelWorkerContextHandle);
/// `ParallelWorkerNumber`.
seam_core::seam!(pub fn parallel_worker_number() -> i32);

// ===========================================================================
// shm_toc estimate/allocate/insert/lookup (storage/ipc/shm_toc.c).
// ===========================================================================

seam_core::seam!(pub fn shm_toc_estimate_chunk(e: ShmTocEstimatorHandle, sz: Size));
seam_core::seam!(pub fn shm_toc_estimate_keys(e: ShmTocEstimatorHandle, nkeys: i32));
/// `shm_toc_allocate(toc, nbytes)` — allocate a chunk, returning a cursor over
/// its DSM bytes.
seam_core::seam!(pub fn shm_toc_allocate(toc: ShmTocHandle, nbytes: Size) -> SerializeCursor);
seam_core::seam!(pub fn shm_toc_insert(toc: ShmTocHandle, key: u64, address: SerializeCursor));
/// `shm_toc_lookup(toc, key, noError)` — `None` when `noError` and absent.
seam_core::seam!(pub fn shm_toc_lookup(toc: ShmTocHandle, key: u64, no_error: bool) -> Option<SerializeCursor>);

// ===========================================================================
// Typed DSM chunk stores/loads (the orchestration writes typed values into a
// freshly-allocated chunk; the DSM allocation lives in the parallel subsystem).
// ===========================================================================

/// Store a `FixedParallelExecutorState` into a DSM chunk and return its handle.
seam_core::seam!(pub fn store_fixed_state(
    chunk: SerializeCursor,
    state: FixedParallelExecutorState,
) -> FixedStateHandle);
/// `fpes->param_exec = dp`.
seam_core::seam!(pub fn set_fixed_param_exec(fpes: FixedStateHandle, dp: u64));
/// `fpes->param_exec`.
seam_core::seam!(pub fn fixed_param_exec(fpes: FixedStateHandle) -> u64);
/// `fpes->eflags`.
seam_core::seam!(pub fn fixed_eflags(fpes: FixedStateHandle) -> i32);
/// `fpes->jit_flags`.
seam_core::seam!(pub fn fixed_jit_flags(fpes: FixedStateHandle) -> i32);
/// `fpes->tuples_needed`.
seam_core::seam!(pub fn fixed_tuples_needed(fpes: FixedStateHandle) -> i64);
/// Reinterpret an existing DSM chunk as a `FixedParallelExecutorState` handle.
seam_core::seam!(pub fn fixed_state_from_chunk(chunk: SerializeCursor) -> FixedStateHandle);

/// Copy a NUL-terminated string into a DSM chunk (`memcpy`).
seam_core::seam!(pub fn store_cstring(chunk: SerializeCursor, value: String));
/// Read a NUL-terminated string back out of a DSM chunk.
seam_core::seam!(pub fn cursor_cstring(chunk: SerializeCursor) -> PgResult<String>);

/// Initialize a `SharedExecutorInstrumentation` header into a DSM chunk.
seam_core::seam!(pub fn store_instrumentation_header(
    chunk: SerializeCursor,
    header: SharedExecutorInstrumentation,
) -> InstrumentationHandle);
/// Reinterpret an existing DSM chunk as a `SharedExecutorInstrumentation` handle.
seam_core::seam!(pub fn instrumentation_from_chunk(chunk: SerializeCursor) -> InstrumentationHandle);
/// `sei->instrument_options`.
seam_core::seam!(pub fn sei_instrument_options(sei: InstrumentationHandle) -> i32);
/// `sei->num_workers`.
seam_core::seam!(pub fn sei_num_workers(sei: InstrumentationHandle) -> i32);
/// `sei->num_plan_nodes`.
seam_core::seam!(pub fn sei_num_plan_nodes(sei: InstrumentationHandle) -> i32);
/// `sei->plan_node_id[index]`.
seam_core::seam!(pub fn sei_plan_node_id(sei: InstrumentationHandle, index: i32) -> i32);
/// `sei->plan_node_id[index] = value`.
seam_core::seam!(pub fn set_sei_plan_node_id(sei: InstrumentationHandle, index: i32, value: i32));

/// Initialize a `SharedJitInstrumentation { num_workers, jit_instr: [zeroed] }`
/// into a DSM chunk.
seam_core::seam!(pub fn store_jit_instrumentation_header(
    chunk: SerializeCursor,
    num_workers: i32,
) -> JitInstrumentationHandle);
/// Reinterpret an existing DSM chunk as a `SharedJitInstrumentation` handle.
seam_core::seam!(pub fn jit_instrumentation_from_chunk(chunk: SerializeCursor) -> JitInstrumentationHandle);
/// `shared_jit->num_workers`.
seam_core::seam!(pub fn shared_jit_num_workers(shared_jit: JitInstrumentationHandle) -> i32);
