//! `#[repr(C)]` ABI for `nodeGather.c` (the Gather executor node).
//!
//! The Gather node is ported in-crate (`backend-executor-nodeGather`), so its
//! state node is a complete, address-stable `#[repr(C)]` struct laid out exactly
//! like the C `GatherState` (execnodes.h). The `Gather` plan node it navigates is
//! spelled out here too.
//!
//! The DSM/parallel-shm machinery that *creates*, *launches*, and *destroys* the
//! parallel context (`ExecInitParallelPlan`, `LaunchParallelWorkers`,
//! `ExecParallelFinish`, …) is genuinely external and reached through the node
//! crate's runtime seam. The state-machine logic in `ExecGather`/`gather_getnext`
//! however *navigates* the already-built `ParallelExecutorInfo` / `ParallelContext`
//! (reading `pei->pcxt`, `pei->area`, `pei->reader`, `pcxt->nworkers_launched`,
//! `pcxt->nworkers_to_launch`), so faithful repr(C) views of those structs are
//! provided here.
//!
//! The embedded `PlanState` head reuses the shared [`crate::PlanStateData`]
//! layout defined in `execnodes`.

use core::ffi::{c_int, c_void};

use crate::{int64, Bitmapset, PlanNode, PlanState, TupleTableSlot};

/// `dsa_area` — opaque per-query dynamic shared memory area (utils/dsa.h). The
/// Gather node only stores/clears its address in `EState.es_query_dsa`.
pub type DsaArea = c_void;

/// `TupleQueueReader` — opaque tuple-queue reader handle (executor/tqueue.h).
/// The Gather node holds an array of pointers to these and consults the seam to
/// read from them; it never lays one out by value.
pub type TupleQueueReader = c_void;

/// `Gather` plan node (plannodes.h):
///
/// ```c
/// typedef struct Gather {
///     Plan        plan;
///     int         num_workers;
///     int         rescan_param;
///     bool        single_copy;
///     bool        invisible;
///     Bitmapset  *initParam;
/// } Gather;
/// ```
///
/// The leading `plan` is the abstract [`PlanNode`] base (its first field is the
/// `NodeTag`), so a `*mut GatherPlan` is also a valid `Node *` / `Plan *`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct GatherPlan {
    /// `Plan plan` — the abstract plan-node base.
    pub plan: PlanNode,
    /// `int num_workers` — planned number of worker processes.
    pub num_workers: c_int,
    /// `int rescan_param` — ID of the `Param` that signals a rescan, or -1.
    pub rescan_param: c_int,
    /// `bool single_copy` — don't execute the plan more than once.
    pub single_copy: bool,
    /// `bool invisible` — suppress EXPLAIN display (for testing)?
    pub invisible: bool,
    /// `Bitmapset *initParam` — param IDs of initplans referenced at the gather
    /// or one of its child nodes.
    pub initParam: *mut Bitmapset,
}

/// `GatherState` (execnodes.h):
///
/// ```c
/// typedef struct GatherState {
///     PlanState   ps;                 /* its first field is NodeTag */
///     bool        initialized;        /* workers launched? */
///     bool        need_to_scan_locally;   /* need to read from local plan? */
///     int64       tuples_needed;      /* tuple bound, see ExecSetTupleBound */
///     /* these fields are set up once: */
///     TupleTableSlot *funnel_slot;
///     struct ParallelExecutorInfo *pei;
///     /* all remaining fields are reinitialized during a rescan: */
///     int         nworkers_launched;  /* original number of workers */
///     int         nreaders;           /* number of still-active workers */
///     int         nextreader;         /* next one to try to read from */
///     struct TupleQueueReader **reader;   /* array with nreaders active entries */
/// } GatherState;
/// ```
///
/// The leading [`crate::PlanStateData`] head's first member is a `NodeTag`, so a
/// `*mut GatherStateData` is also a valid `Node *` / `PlanState *`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct GatherStateData {
    /// `PlanState ps` — its first field is `NodeTag`.
    pub ps: crate::PlanStateData,
    /// `bool initialized` — have the workers been launched?
    pub initialized: bool,
    /// `bool need_to_scan_locally` — must the leader also read from the local
    /// copy of the plan?
    pub need_to_scan_locally: bool,
    /// `int64 tuples_needed` — tuple bound, see `ExecSetTupleBound`.
    pub tuples_needed: int64,
    /// `TupleTableSlot *funnel_slot` — slot the worker tuples are funneled into.
    pub funnel_slot: *mut TupleTableSlot,
    /// `struct ParallelExecutorInfo *pei` — shared state for the parallel run.
    pub pei: *mut ParallelExecutorInfo,
    /// `int nworkers_launched` — original number of workers (for EXPLAIN).
    pub nworkers_launched: c_int,
    /// `int nreaders` — number of still-active workers.
    pub nreaders: c_int,
    /// `int nextreader` — next reader to try to read from (round-robin).
    pub nextreader: c_int,
    /// `struct TupleQueueReader **reader` — working array of `nreaders` active
    /// readers.
    pub reader: *mut *mut TupleQueueReader,
}

/// `ParallelExecutorInfo` (executor/execParallel.h) — the shared state for a
/// parallel plan run. Built/torn down through the runtime seam; the node only
/// navigates `pcxt`, `area`, and `reader`.
///
/// ```c
/// typedef struct ParallelExecutorInfo {
///     PlanState  *planstate;
///     ParallelContext *pcxt;
///     BufferUsage *buffer_usage;
///     WalUsage   *wal_usage;
///     SharedExecutorInstrumentation *instrumentation;
///     struct SharedJitInstrumentation *jit_instrumentation;
///     dsa_area   *area;
///     dsa_pointer param_exec;
///     bool        finished;
///     shm_mq_handle **tqueue;
///     struct TupleQueueReader **reader;
/// } ParallelExecutorInfo;
/// ```
///
/// `dsa_pointer` is 64-bit on the supported (64-bit) platforms.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct ParallelExecutorInfo {
    /// `PlanState *planstate` — plan subtree we're running in parallel.
    pub planstate: *mut PlanState,
    /// `ParallelContext *pcxt` — parallel context we're using. Navigated as the
    /// typed [`ParallelContextHead`] for the worker-count fields.
    pub pcxt: *mut ParallelContextHead,
    /// `BufferUsage *buffer_usage` — points to the bufusage area in DSM.
    pub buffer_usage: *mut c_void,
    /// `WalUsage *wal_usage` — walusage area in DSM.
    pub wal_usage: *mut c_void,
    /// `SharedExecutorInstrumentation *instrumentation` — optional.
    pub instrumentation: *mut c_void,
    /// `struct SharedJitInstrumentation *jit_instrumentation` — optional.
    pub jit_instrumentation: *mut c_void,
    /// `dsa_area *area` — points to the DSA area in DSM.
    pub area: *mut DsaArea,
    /// `dsa_pointer param_exec` — serialized PARAM_EXEC parameters (64-bit).
    pub param_exec: u64,
    /// `bool finished` — set true by `ExecParallelFinish`.
    pub finished: bool,
    /// `shm_mq_handle **tqueue` — tuple queues for worker output.
    pub tqueue: *mut *mut c_void,
    /// `struct TupleQueueReader **reader` — tuple reader/writer support.
    pub reader: *mut *mut TupleQueueReader,
}

/// `ParallelContext` (access/parallel.h) — head fields only. The Gather node
/// reads `nworkers_to_launch` and `nworkers_launched`; the trailing fields
/// (`library_name`, the `shm_toc_estimator`, …) are owned by the parallel
/// subsystem and never laid out by value here. A `*mut ParallelContextHead` is
/// always obtained through `pei->pcxt`, so only the reachable head is modeled.
/// (Named `…Head` to avoid clashing with the crate's opaque
/// [`crate::ParallelContext`] alias, which other subsystems depend on.)
///
/// ```c
/// typedef struct ParallelContext {
///     dlist_node  node;               /* 2 pointers */
///     SubTransactionId subid;         /* uint32 */
///     int         nworkers;
///     int         nworkers_to_launch;
///     int         nworkers_launched;
///     ...
/// } ParallelContext;
/// ```
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct ParallelContextHead {
    /// `dlist_node node` — `{ dlist_node *prev; dlist_node *next; }`.
    pub node_prev: *mut c_void,
    /// second word of the embedded `dlist_node`.
    pub node_next: *mut c_void,
    /// `SubTransactionId subid` — `uint32`.
    pub subid: u32,
    /// `int nworkers` — maximum number of workers to launch.
    pub nworkers: c_int,
    /// `int nworkers_to_launch` — actual number of workers to launch.
    pub nworkers_to_launch: c_int,
    /// `int nworkers_launched` — number of workers actually launched.
    pub nworkers_launched: c_int,
}

// ===========================================================================
// Layout asserts: the embedded heads must keep their C offsets so a
// `*mut GatherStateData` can be navigated as the C `GatherState *`, and a
// `*mut GatherPlan` as the C `Gather *`.
// ===========================================================================
const _: () = {
    // GatherPlan { Plan plan; int num_workers; ... }
    assert!(core::mem::offset_of!(GatherPlan, plan) == 0);
    assert!(core::mem::offset_of!(PlanNode, type_) == 0);
    assert!(core::mem::offset_of!(GatherPlan, num_workers) == core::mem::size_of::<PlanNode>());

    // GatherState { PlanState ps; ... }
    assert!(core::mem::offset_of!(GatherStateData, ps) == 0);
    assert!(core::mem::offset_of!(crate::PlanStateData, type_) == 0);
    assert!(
        core::mem::offset_of!(GatherStateData, initialized)
            == core::mem::size_of::<crate::PlanStateData>()
    );

    // ParallelContextHead: the two int fields the node reads sit right after
    // dlist_node (16 bytes), subid (uint32, 4) and nworkers (int, 4).
    assert!(core::mem::offset_of!(ParallelContextHead, nworkers_to_launch) == 24);
    assert!(core::mem::offset_of!(ParallelContextHead, nworkers_launched) == 28);

    // ParallelExecutorInfo: pcxt is the 2nd pointer; area follows 6 pointers;
    // reader is the last field after param_exec (8) + finished (padded) + tqueue.
    assert!(core::mem::offset_of!(ParallelExecutorInfo, pcxt) == 8);
    assert!(core::mem::offset_of!(ParallelExecutorInfo, area) == 48);
};

impl crate::EState {
    /// `EState.es_use_parallel_mode` — can we use parallel workers? (`bool`,
    /// byte offset 280). Read by `ExecGather` to decide whether to fire up
    /// workers.
    pub fn es_use_parallel_mode(&self) -> bool {
        unsafe {
            core::ptr::read_unaligned(
                (self as *const crate::EState as *const u8)
                    .add(280)
                    .cast::<bool>(),
            )
        }
    }

    /// `EState.es_parallel_workers_to_launch` — number of workers to launch
    /// (`int`, byte offset 284). Accumulated by `ExecGather` for EXPLAIN.
    pub fn es_parallel_workers_to_launch(&self) -> c_int {
        unsafe {
            core::ptr::read_unaligned(
                (self as *const crate::EState as *const u8)
                    .add(284)
                    .cast::<c_int>(),
            )
        }
    }

    /// Set `EState.es_parallel_workers_to_launch` (byte offset 284). Mirrors
    /// `estate->es_parallel_workers_to_launch += pcxt->nworkers_to_launch`.
    pub fn set_es_parallel_workers_to_launch(&mut self, value: c_int) {
        unsafe {
            core::ptr::write_unaligned(
                (self as *mut crate::EState as *mut u8)
                    .add(284)
                    .cast::<c_int>(),
                value,
            );
        }
    }

    /// `EState.es_parallel_workers_launched` — number of workers actually
    /// launched (`int`, byte offset 288).
    pub fn es_parallel_workers_launched(&self) -> c_int {
        unsafe {
            core::ptr::read_unaligned(
                (self as *const crate::EState as *const u8)
                    .add(288)
                    .cast::<c_int>(),
            )
        }
    }

    /// Set `EState.es_parallel_workers_launched` (byte offset 288). Mirrors
    /// `estate->es_parallel_workers_launched += pcxt->nworkers_launched`.
    pub fn set_es_parallel_workers_launched(&mut self, value: c_int) {
        unsafe {
            core::ptr::write_unaligned(
                (self as *mut crate::EState as *mut u8)
                    .add(288)
                    .cast::<c_int>(),
                value,
            );
        }
    }

    /// `EState.es_query_dsa` — the per-query shared memory area used for
    /// parallel execution (`struct dsa_area *`, byte offset 296). `ExecGather`
    /// installs the gather's DSA area here while executing the local plan and
    /// clears it afterward.
    pub fn es_query_dsa(&self) -> *mut DsaArea {
        unsafe {
            core::ptr::read_unaligned(
                (self as *const crate::EState as *const u8)
                    .add(296)
                    .cast::<*mut DsaArea>(),
            )
        }
    }

    /// Set `EState.es_query_dsa` (byte offset 296). Mirrors
    /// `estate->es_query_dsa = gatherstate->pei ? gatherstate->pei->area : NULL`.
    // `value` is stored as an opaque pointer value, never dereferenced here.
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    pub fn set_es_query_dsa(&mut self, value: *mut DsaArea) {
        unsafe {
            core::ptr::write_unaligned(
                (self as *mut crate::EState as *mut u8)
                    .add(296)
                    .cast::<*mut DsaArea>(),
                value,
            );
        }
    }
}
