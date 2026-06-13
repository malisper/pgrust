//! Types for `execParallel.c` — the executor's parallel-query infrastructure
//! (PostgreSQL 18.3).
//!
//! Two kinds of types live here:
//!
//! 1. The genuine **DSM ABI** structs that the parallel executor lays out into
//!    the dynamic shared-memory segment and reads back from a worker process —
//!    `FixedParallelExecutorState`, `SharedExecutorInstrumentation`, and the
//!    JIT instrumentation structs from `jit/jit.h`. Because they cross the
//!    leader/worker process boundary they are real shared-memory layouts.
//!
//! 2. `Copy` **handle** newtypes naming the live objects owned by sibling
//!    subsystems that are not ported yet (the `PlanState` tree, the `EState`,
//!    the `ParallelContext`, the DSM segment, the `shm_toc`, the DSA area, the
//!    tuple queues, the serialized `PlannedStmt`/`ParamListInfo`, the
//!    `QueryDesc`, the `DestReceiver`). The orchestration threads each handle
//!    from one seam call to the next, exactly as the C threads a pointer; the
//!    owning subsystem hands the identity back across the seam. This is the
//!    sanctioned "function owned by a not-yet-ported neighbor" form — when the
//!    owner lands, these collapse onto the owner's real type.

#![no_std]
#![allow(non_camel_case_types)]

extern crate alloc;

use alloc::vec::Vec;

use types_core::instrument::instr_time;
use types_datum::Datum;

/// `int16` — the C signed 16-bit integer (`c.h`).
pub type int16 = i16;
/// `int64` — re-export for tuple-bound signatures.
pub type int64 = i64;
/// `Size` — re-export (`c.h`).
pub type Size = usize;

/// `dsa_pointer` (`utils/dsa.h`) — a relative offset into a DSA area; 64-bit on
/// the supported platforms.
pub type DsaPointer = u64;

/// `InvalidDsaPointer` (`utils/dsa.h`).
pub const INVALID_DSA_POINTER: DsaPointer = 0;

/// `DsaPointerIsValid(x)` (`utils/dsa.h`) — `(x) != InvalidDsaPointer`.
#[inline]
pub const fn dsa_pointer_is_valid(x: DsaPointer) -> bool {
    x != INVALID_DSA_POINTER
}

/// `DSA_ALLOC_HUGE` (`utils/dsa.h:73`) — allow huge allocation (> 1 GB).
pub const DSA_ALLOC_HUGE: i32 = 0x01;
/// `DSA_ALLOC_NO_OOM` (`utils/dsa.h:74`) — no failure if out-of-memory.
pub const DSA_ALLOC_NO_OOM: i32 = 0x02;
/// `DSA_ALLOC_ZERO` (`utils/dsa.h:75`) — zero allocated memory.
pub const DSA_ALLOC_ZERO: i32 = 0x04;

/// Tuple-bound value (`int64 tuples_needed`).
pub type TuplesNeeded = int64;

// ===========================================================================
// JIT instrumentation (jit/jit.h). These cross into the DSM, so they are real
// layouts, not handles.
// ===========================================================================

/// `PGJIT_NONE` (`jit/jit.h`).
pub const PGJIT_NONE: i32 = 0;
/// `PGJIT_PERFORM` (`jit/jit.h`).
pub const PGJIT_PERFORM: i32 = 1 << 0;
/// `PGJIT_OPT3` (`jit/jit.h`).
pub const PGJIT_OPT3: i32 = 1 << 1;
/// `PGJIT_INLINE` (`jit/jit.h`).
pub const PGJIT_INLINE: i32 = 1 << 2;
/// `PGJIT_EXPR` (`jit/jit.h`).
pub const PGJIT_EXPR: i32 = 1 << 3;
/// `PGJIT_DEFORM` (`jit/jit.h`).
pub const PGJIT_DEFORM: i32 = 1 << 4;

/// `struct JitInstrumentation` (`jit/jit.h`) — per-context JIT timing/counters.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct JitInstrumentation {
    /// `size_t created_functions` — number of emitted functions.
    pub created_functions: usize,
    /// `instr_time generation_counter` — accumulated code-generation time.
    pub generation_counter: instr_time,
    /// `instr_time deform_counter` — accumulated tuple-deform time.
    pub deform_counter: instr_time,
    /// `instr_time inlining_counter` — accumulated inlining time.
    pub inlining_counter: instr_time,
    /// `instr_time optimization_counter` — accumulated optimization time.
    pub optimization_counter: instr_time,
    /// `instr_time emission_counter` — accumulated emission time.
    pub emission_counter: instr_time,
}

// ===========================================================================
// DSM ABI structs (execParallel.c).
// ===========================================================================

/// `struct FixedParallelExecutorState` (execParallel.c) — fixed-size random
/// state passed to parallel workers, stored under `PARALLEL_KEY_EXECUTOR_FIXED`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FixedParallelExecutorState {
    /// `int64 tuples_needed` — tuple bound (see `ExecSetTupleBound`).
    pub tuples_needed: int64,
    /// `dsa_pointer param_exec` — DSA handle of the serialized PARAM_EXEC
    /// parameters, or `InvalidDsaPointer`.
    pub param_exec: DsaPointer,
    /// `int eflags` — executor eflags to pass to the worker.
    pub eflags: i32,
    /// `int jit_flags` — JIT flags to pass to the worker.
    pub jit_flags: i32,
}

/// `struct SharedExecutorInstrumentation` (execParallel.c) — DSM structure for
/// accumulating per-`PlanState` instrumentation, stored under
/// `PARALLEL_KEY_INSTRUMENTATION`. `plan_node_id` is the C flexible array
/// member, modeled as an owned `Vec`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SharedExecutorInstrumentation {
    /// `int instrument_options` — same meaning as in instrument.c.
    pub instrument_options: i32,
    /// `int instrument_offset` — byte offset of the first `Instrumentation`.
    pub instrument_offset: i32,
    /// `int num_workers`.
    pub num_workers: i32,
    /// `int num_plan_nodes`.
    pub num_plan_nodes: i32,
    /// `int plan_node_id[FLEXIBLE_ARRAY_MEMBER]`.
    pub plan_node_id: Vec<i32>,
}

// ===========================================================================
// Handle newtypes for live objects owned by not-yet-ported subsystems.
// Each is a distinct type so they can never be confused.
// ===========================================================================

macro_rules! handle {
    ($(#[$m:meta])* $name:ident) => {
        $(#[$m])*
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(pub usize);
    };
}

handle!(
    /// Live `PlanState *` (`nodes/execnodes.h`) — owned by the executor.
    PlanStateHandle);
handle!(
    /// Live `EState *` (`nodes/execnodes.h`) — per-query executor state.
    EStateHandle);
handle!(
    /// Live `ParallelContext *` (`access/parallel.h`).
    ParallelContextHandle);
handle!(
    /// Live `ParallelWorkerContext *` (`access/parallel.h`) — the `{seg, toc}`
    /// pair handed to the per-node `Exec*InitializeWorker` hooks in the worker.
    ParallelWorkerContextHandle);
handle!(
    /// Live `dsm_segment *` (`storage/ipc/dsm.h`).
    DsmSegmentHandle);
handle!(
    /// Live `shm_toc *` (`storage/ipc/shm_toc.h`).
    ShmTocHandle);
handle!(
    /// Live `shm_toc_estimator *` (`storage/ipc/shm_toc.h`).
    ShmTocEstimatorHandle);
handle!(
    /// Live `dsa_area *` (`utils/dsa.h`).
    DsaAreaHandle);
handle!(
    /// Live `shm_mq *` (`storage/ipc/shm_mq.h`).
    ShmMqHandle);
handle!(
    /// Live `shm_mq_handle *` (`storage/ipc/shm_mq.h`).
    ShmMqAttachHandle);
handle!(
    /// Live `TupleQueueReader *` (`executor/tqueue.h`).
    TupleQueueReaderHandle);
handle!(
    /// Live `BackgroundWorkerHandle *` (`postmaster/bgworker.h`).
    BackgroundWorkerHandle);
handle!(
    /// Live `DestReceiver *` (`tcop/dest.h`).
    DestReceiverHandle);
handle!(
    /// Live `QueryDesc *` (`executor/execdesc.h`).
    QueryDescHandle);
handle!(
    /// Live `Plan *` (`nodes/plannodes.h`).
    PlanHandle);
handle!(
    /// Live serialized `PlannedStmt *` (`nodes/plannodes.h`).
    PlannedStmtHandle);
handle!(
    /// Live `ParamListInfo` (`nodes/params.h`).
    ParamListInfoHandle);
handle!(
    /// Live `ExprContext *` (`nodes/execnodes.h`).
    ExprContextHandle);
handle!(
    /// In-DSM `FixedParallelExecutorState` allocation.
    FixedStateHandle);
handle!(
    /// In-DSM `SharedExecutorInstrumentation` allocation.
    InstrumentationHandle);
handle!(
    /// In-DSM `SharedJitInstrumentation` allocation.
    JitInstrumentationHandle);
handle!(
    /// Live `Snapshot` (`utils/snapshot.h`) — the active snapshot, threaded by
    /// identity. `None` models C's `InvalidSnapshot`/NULL.
    SnapshotHandle);
handle!(
    /// Live `BufFile *` (`storage/buffile.h`) — a buffered virtual temp file,
    /// owned by `storage/file/buffile.c`. The hash table holds one per batch
    /// in its `innerBatchFile`/`outerBatchFile` arrays.
    BufFileHandle);
handle!(
    /// Live `SharedTuplestoreAccessor *` (`utils/sharedtuplestore.h`) — a
    /// backend's accessor onto a shared tuplestore, owned by
    /// `utils/sort/sharedtuplestore.c`.
    SharedTuplestoreAccessorHandle);
handle!(
    /// In-DSM `SharedTuplestore` (`utils/sharedtuplestore.h`) — the shared
    /// state object, placed in shmem following a `ParallelHashJoinBatch`.
    SharedTuplestoreHandle);
handle!(
    /// In-DSM `SharedFileSet *` (`storage/sharedfileset.h`) — names a group of
    /// shared temp files, owned by `storage/file/sharedfileset.c`.
    SharedFileSetHandle);
handle!(
    /// `FileSet *` (`storage/fileset.h`) — names a group of temporary files
    /// shared by a set of backends, owned by `storage/file/fileset.c`. A
    /// fileset-backed `BufFile` borrows this pointer (never owns the body), so
    /// it is an inherited-opacity handle here.
    FileSetHandle);

/// Cursor over a serialized buffer in DSA/DSM storage that the
/// (de)serialization helpers advance as they read/write
/// (`utils/adt/datum.c` / `nodes/params.c` `start_address`).
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SerializeCursor(pub usize);

/// One PARAM_EXEC parameter's serializable value (`ParamExecData` value/isnull
/// plus the resolved type metadata), read from `es_param_exec_vals` for
/// serialization.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ParamExecValue {
    /// `ParamExecData.value`.
    pub value: Datum,
    /// `ParamExecData.isnull`.
    pub isnull: bool,
    /// Resolved `typByVal` (or `true` when the param has no type OID).
    pub typ_byval: bool,
    /// Resolved `typLen` (or `sizeof(Datum)` when the param has no type OID).
    pub typ_len: int16,
}

/// A restored PARAM_EXEC parameter, written back into the worker's
/// `es_param_exec_vals[paramid]` by `RestoreParamExecParams`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RestoredParam {
    /// `prm->value`.
    pub value: Datum,
    /// `prm->isnull`.
    pub isnull: bool,
}

/// `struct ParallelExecutorInfo` (execParallel.h:22-37) — the leader's handle
/// onto a running parallel subplan. The `tqueue`/`reader` arrays are
/// `palloc`ed into the per-query context, modeled here as `PgVec<'mcx, _>`.
pub struct ParallelExecutorInfo<'mcx> {
    /// `PlanState *planstate`.
    pub planstate: PlanStateHandle,
    /// `ParallelContext *pcxt` (`None` after cleanup).
    pub pcxt: Option<ParallelContextHandle>,
    /// `BufferUsage *buffer_usage` — points into the DSM bufusage area.
    pub buffer_usage: SerializeCursor,
    /// `WalUsage *wal_usage` — points into the DSM walusage area.
    pub wal_usage: SerializeCursor,
    /// `SharedExecutorInstrumentation *instrumentation` — optional.
    pub instrumentation: Option<InstrumentationHandle>,
    /// `struct SharedJitInstrumentation *jit_instrumentation` — optional.
    pub jit_instrumentation: Option<JitInstrumentationHandle>,
    /// `dsa_area *area` (`None` after cleanup or when in private memory).
    pub area: Option<DsaAreaHandle>,
    /// `dsa_pointer param_exec`.
    pub param_exec: DsaPointer,
    /// `bool finished`.
    pub finished: bool,
    /// `shm_mq_handle **tqueue`.
    pub tqueue: mcx::PgVec<'mcx, ShmMqAttachHandle>,
    /// `struct TupleQueueReader **reader`.
    pub reader: mcx::PgVec<'mcx, TupleQueueReaderHandle>,
}

/// `struct ExecParallelEstimateContext` (execParallel.c:111-116).
#[derive(Clone, Copy, Debug)]
pub struct ExecParallelEstimateContext {
    /// `ParallelContext *pcxt`.
    pub pcxt: ParallelContextHandle,
    /// `int nnodes`.
    pub nnodes: i32,
}

/// `struct ExecParallelInitializeDSMContext` (execParallel.c:118-124).
#[derive(Clone, Copy, Debug)]
pub struct ExecParallelInitializeDSMContext {
    /// `ParallelContext *pcxt`.
    pub pcxt: ParallelContextHandle,
    /// `SharedExecutorInstrumentation *instrumentation` — optional.
    pub instrumentation: Option<InstrumentationHandle>,
    /// `int nnodes`.
    pub nnodes: i32,
}
