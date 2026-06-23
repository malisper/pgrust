//! `#[repr(C)]` ABI for `execParallel.c` — the two file-private structs that
//! the parallel executor lays out *into the DSM segment* and reads back from a
//! worker process. Because they cross the leader/worker process boundary they
//! are genuine shared-memory ABI (not opaque sibling-subsystem types), so their
//! layout must match PostgreSQL 18.3 byte-for-byte.
//!
//! The two transient *walk-context* structs (`ExecParallelEstimateContext`,
//! `ExecParallelInitializeDSMContext`) are pure backend-local file-private
//! bookkeeping and live in the crate itself, not here.
//!
//! `dsa_pointer` is the crate's [`crate::dsa_pointer`] (64-bit) alias; the
//! magic DSM keys (`PARALLEL_KEY_*`) and `PARALLEL_TUPLE_QUEUE_SIZE` are
//! crate-local `#define`s in the port and are kept with the logic, not here.

#![allow(non_camel_case_types)]

use core::ffi::c_int;

use crate::{dsa_pointer, int64, Instrumentation};

/// `struct FixedParallelExecutorState` (execParallel.c) — fixed-size random
/// stuff that we need to pass to parallel workers, stored in the DSM under
/// `PARALLEL_KEY_EXECUTOR_FIXED`.
///
/// ```c
/// typedef struct FixedParallelExecutorState
/// {
///     int64       tuples_needed;  /* tuple bound, see ExecSetTupleBound */
///     dsa_pointer param_exec;
///     int         eflags;
///     int         jit_flags;
/// } FixedParallelExecutorState;
/// ```
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FixedParallelExecutorState {
    /// `int64 tuples_needed` — tuple bound (see `ExecSetTupleBound`).
    pub tuples_needed: int64,
    /// `dsa_pointer param_exec` — DSA handle of the serialized PARAM_EXEC
    /// parameters, or `InvalidDsaPointer`.
    pub param_exec: dsa_pointer,
    /// `int eflags` — executor eflags to pass to the worker.
    pub eflags: c_int,
    /// `int jit_flags` — JIT flags to pass to the worker.
    pub jit_flags: c_int,
}

/// `struct SharedExecutorInstrumentation` (execParallel.c) — DSM structure for
/// accumulating per-`PlanState` instrumentation, stored under
/// `PARALLEL_KEY_INSTRUMENTATION`.
///
/// ```c
/// struct SharedExecutorInstrumentation
/// {
///     int         instrument_options;
///     int         instrument_offset;
///     int         num_workers;
///     int         num_plan_nodes;
///     int         plan_node_id[FLEXIBLE_ARRAY_MEMBER];
///     /* array of num_plan_nodes * num_workers Instrumentation objects follows */
/// };
/// ```
///
/// `plan_node_id` is the C flexible array member, modeled as a zero-length
/// array; the `Instrumentation` objects that follow are reached through
/// [`get_instrumentation_array`] at `instrument_offset` bytes from the start.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct SharedExecutorInstrumentation {
    /// `int instrument_options` — same meaning as in instrument.c.
    pub instrument_options: c_int,
    /// `int instrument_offset` — byte offset, relative to the start of this
    /// struct, of the first `Instrumentation` object.
    pub instrument_offset: c_int,
    /// `int num_workers` — number of workers.
    pub num_workers: c_int,
    /// `int num_plan_nodes` — number of plan nodes.
    pub num_plan_nodes: c_int,
    /// `int plan_node_id[FLEXIBLE_ARRAY_MEMBER]` — plan-node ids being gathered.
    pub plan_node_id: [c_int; 0],
}

/// `GetInstrumentationArray(sei)` (execParallel.c) — the `Instrumentation`
/// array begins `sei->instrument_offset` bytes from the start of the struct.
///
/// # Safety
/// `sei` must point at a live `SharedExecutorInstrumentation` whose
/// `instrument_offset` was set by the leader.
#[inline]
pub unsafe fn get_instrumentation_array(
    sei: *mut SharedExecutorInstrumentation,
) -> *mut Instrumentation {
    let offset = (*sei).instrument_offset as usize;
    (sei as *mut u8).add(offset).cast::<Instrumentation>()
}
