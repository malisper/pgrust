//! `#[repr(C)]` ABI for `nodeGatherMerge.c` (the Gather Merge executor node).
//!
//! The Gather Merge node is ported in-crate (`backend-executor-nodeGatherMerge`),
//! so its state node is a complete, address-stable `#[repr(C)]` struct laid out
//! exactly like the C `GatherMergeState` (execnodes.h). The `GatherMerge` plan
//! node it navigates is spelled out here too.
//!
//! The DSM/parallel-shm machinery that *creates*, *launches*, and *destroys* the
//! parallel context (`ExecInitParallelPlan`, `LaunchParallelWorkers`,
//! `ExecParallelFinish`, …) is genuinely external and reached through the node
//! crate's runtime seam. The state-machine logic in
//! `ExecGatherMerge`/`gather_merge_readnext` however *navigates* the already-built
//! `ParallelExecutorInfo` / `ParallelContext` (reading `pei->pcxt`, `pei->area`,
//! `pei->reader`, `pcxt->nworkers_launched`, `pcxt->nworkers_to_launch`), so the
//! faithful repr(C) views of those structs (shared with the Gather node) are
//! reused from [`crate::nodegather_abi`].
//!
//! The embedded `PlanState` head reuses the shared [`crate::PlanStateData`]
//! layout defined in `execnodes`; the `TupleQueueReader`/`ParallelExecutorInfo`/
//! `DsaArea` types are shared with `nodeGather.c` via [`crate::nodegather_abi`].

use core::ffi::c_int;
use core::ffi::c_void;

use crate::heaptuple::{MinimalTuple, TupleDesc};
use crate::nodegather_abi::{ParallelExecutorInfo, TupleQueueReader};
use crate::sortsupport::SortSupportData;
use crate::{int64, AttrNumber, Bitmapset, Oid, PlanNode, TupleTableSlot};

/// `GatherMerge` plan node (plannodes.h):
///
/// ```c
/// typedef struct GatherMerge {
///     Plan        plan;
///     int         num_workers;
///     int         rescan_param;
///     int         numCols;
///     AttrNumber *sortColIdx;
///     Oid        *sortOperators;
///     Oid        *collations;
///     bool       *nullsFirst;
///     Bitmapset  *initParam;
/// } GatherMerge;
/// ```
///
/// The leading `plan` is the abstract [`PlanNode`] base (its first field is the
/// `NodeTag`), so a `*mut GatherMergePlan` is also a valid `Node *` / `Plan *`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct GatherMergePlan {
    /// `Plan plan` — the abstract plan-node base.
    pub plan: PlanNode,
    /// `int num_workers` — planned number of worker processes.
    pub num_workers: c_int,
    /// `int rescan_param` — ID of the `Param` that signals a rescan, or -1.
    pub rescan_param: c_int,
    /// `int numCols` — number of sort-key columns.
    pub numCols: c_int,
    /// `AttrNumber *sortColIdx` — their indexes in the target list.
    pub sortColIdx: *mut AttrNumber,
    /// `Oid *sortOperators` — OIDs of operators to sort them by.
    pub sortOperators: *mut Oid,
    /// `Oid *collations` — OIDs of collations.
    pub collations: *mut Oid,
    /// `bool *nullsFirst` — NULLS FIRST/LAST directions.
    pub nullsFirst: *mut bool,
    /// `Bitmapset *initParam` — param IDs of initplans referenced at the gather
    /// merge or one of its child nodes.
    pub initParam: *mut Bitmapset,
}

/// `GMReaderTupleBuffer` (nodeGatherMerge.c) — pending-tuple array for each
/// worker. Holds additional tuples we were able to fetch but can't process yet,
/// plus the "done" flag indicating the worker is known exhausted.
///
/// ```c
/// typedef struct GMReaderTupleBuffer {
///     MinimalTuple *tuple;    /* array of length MAX_TUPLE_STORE */
///     int          nTuples;   /* number of tuples currently stored */
///     int          readCounter; /* index of next tuple to extract */
///     bool         done;      /* true if reader is known exhausted */
/// } GMReaderTupleBuffer;
/// ```
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct GMReaderTupleBuffer {
    /// `MinimalTuple *tuple` — array of length `MAX_TUPLE_STORE`.
    pub tuple: *mut MinimalTuple,
    /// `int nTuples` — number of tuples currently stored.
    pub nTuples: c_int,
    /// `int readCounter` — index of the next tuple to extract.
    pub readCounter: c_int,
    /// `bool done` — true if the reader is known exhausted.
    pub done: bool,
}

/// `GatherMergeState` (execnodes.h):
///
/// ```c
/// typedef struct GatherMergeState {
///     PlanState   ps;                 /* its first field is NodeTag */
///     bool        initialized;        /* workers launched? */
///     bool        gm_initialized;     /* gather_merge_init() done? */
///     bool        need_to_scan_locally;   /* need to read from local plan? */
///     int64       tuples_needed;      /* tuple bound, see ExecSetTupleBound */
///     /* these fields are set up once: */
///     TupleDesc   tupDesc;            /* descriptor for subplan result tuples */
///     int         gm_nkeys;           /* number of sort columns */
///     SortSupport gm_sortkeys;        /* array of length gm_nkeys */
///     struct ParallelExecutorInfo *pei;
///     /* all remaining fields are reinitialized during a rescan */
///     int         nworkers_launched;  /* original number of workers */
///     int         nreaders;           /* number of active workers */
///     TupleTableSlot **gm_slots;      /* array with nreaders+1 entries */
///     struct TupleQueueReader **reader;   /* array with nreaders active entries */
///     struct GMReaderTupleBuffer *gm_tuple_buffers;   /* nreaders tuple buffers */
///     struct binaryheap *gm_heap;     /* binary heap of slot indices */
/// } GatherMergeState;
/// ```
///
/// The leading [`crate::PlanStateData`] head's first member is a `NodeTag`, so a
/// `*mut GatherMergeStateData` is also a valid `Node *` / `PlanState *`. The
/// `gm_heap` binary heap is owned by `lib/binaryheap.c`; the node crate models its
/// reachable head (`bh_size`) internally and reaches it as an opaque pointer here.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct GatherMergeStateData {
    /// `PlanState ps` — its first field is `NodeTag`.
    pub ps: crate::PlanStateData,
    /// `bool initialized` — have the workers been launched?
    pub initialized: bool,
    /// `bool gm_initialized` — has `gather_merge_init()` run?
    pub gm_initialized: bool,
    /// `bool need_to_scan_locally` — must the leader also read from the local
    /// copy of the plan?
    pub need_to_scan_locally: bool,
    /// `int64 tuples_needed` — tuple bound, see `ExecSetTupleBound`.
    pub tuples_needed: int64,
    /// `TupleDesc tupDesc` — descriptor for the subplan's result tuples.
    pub tupDesc: TupleDesc,
    /// `int gm_nkeys` — number of sort columns.
    pub gm_nkeys: c_int,
    /// `SortSupport gm_sortkeys` — array of length `gm_nkeys`.
    pub gm_sortkeys: *mut SortSupportData,
    /// `struct ParallelExecutorInfo *pei` — shared state for the parallel run.
    pub pei: *mut ParallelExecutorInfo,
    /// `int nworkers_launched` — original number of workers (for EXPLAIN).
    pub nworkers_launched: c_int,
    /// `int nreaders` — number of active workers.
    pub nreaders: c_int,
    /// `TupleTableSlot **gm_slots` — array with `nreaders + 1` entries (index 0 is
    /// the leader).
    pub gm_slots: *mut *mut TupleTableSlot,
    /// `struct TupleQueueReader **reader` — array with `nreaders` active readers.
    pub reader: *mut *mut TupleQueueReader,
    /// `struct GMReaderTupleBuffer *gm_tuple_buffers` — `nreaders` tuple buffers
    /// (indexed 0..nreaders-1; no entry for the leader).
    pub gm_tuple_buffers: *mut GMReaderTupleBuffer,
    /// `struct binaryheap *gm_heap` — binary heap of slot indices (opaque pointer;
    /// the node crate navigates its `bh_size` head).
    pub gm_heap: *mut c_void,
}

// ===========================================================================
// Layout asserts: the embedded heads must keep their C offsets so a
// `*mut GatherMergeStateData` can be navigated as the C `GatherMergeState *`, and
// a `*mut GatherMergePlan` as the C `GatherMerge *`. Offsets verified against
// PostgreSQL 18.3 (`offsetof`).
// ===========================================================================
const _: () = {
    // GatherMergePlan { Plan plan; int num_workers; int rescan_param; int numCols;
    //   AttrNumber *sortColIdx; Oid *sortOperators; Oid *collations;
    //   bool *nullsFirst; Bitmapset *initParam; }
    assert!(core::mem::offset_of!(GatherMergePlan, plan) == 0);
    assert!(core::mem::offset_of!(PlanNode, type_) == 0);
    assert!(core::mem::offset_of!(GatherMergePlan, num_workers) == 104);
    assert!(core::mem::offset_of!(GatherMergePlan, rescan_param) == 108);
    assert!(core::mem::offset_of!(GatherMergePlan, numCols) == 112);
    assert!(core::mem::offset_of!(GatherMergePlan, sortColIdx) == 120);
    assert!(core::mem::offset_of!(GatherMergePlan, sortOperators) == 128);
    assert!(core::mem::offset_of!(GatherMergePlan, collations) == 136);
    assert!(core::mem::offset_of!(GatherMergePlan, nullsFirst) == 144);
    assert!(core::mem::offset_of!(GatherMergePlan, initParam) == 152);
    assert!(core::mem::size_of::<GatherMergePlan>() == 160);

    // GMReaderTupleBuffer { MinimalTuple *tuple; int nTuples; int readCounter;
    //   bool done; }
    assert!(core::mem::offset_of!(GMReaderTupleBuffer, tuple) == 0);
    assert!(core::mem::offset_of!(GMReaderTupleBuffer, nTuples) == 8);
    assert!(core::mem::offset_of!(GMReaderTupleBuffer, readCounter) == 12);
    assert!(core::mem::offset_of!(GMReaderTupleBuffer, done) == 16);
    assert!(core::mem::size_of::<GMReaderTupleBuffer>() == 24);

    // GatherMergeState { PlanState ps; bool initialized; bool gm_initialized;
    //   bool need_to_scan_locally; int64 tuples_needed; TupleDesc tupDesc;
    //   int gm_nkeys; SortSupport gm_sortkeys; ParallelExecutorInfo *pei;
    //   int nworkers_launched; int nreaders; TupleTableSlot **gm_slots;
    //   TupleQueueReader **reader; GMReaderTupleBuffer *gm_tuple_buffers;
    //   binaryheap *gm_heap; }
    assert!(core::mem::offset_of!(GatherMergeStateData, ps) == 0);
    assert!(core::mem::offset_of!(crate::PlanStateData, type_) == 0);
    assert!(core::mem::offset_of!(GatherMergeStateData, initialized) == 200);
    assert!(core::mem::offset_of!(GatherMergeStateData, gm_initialized) == 201);
    assert!(core::mem::offset_of!(GatherMergeStateData, need_to_scan_locally) == 202);
    assert!(core::mem::offset_of!(GatherMergeStateData, tuples_needed) == 208);
    assert!(core::mem::offset_of!(GatherMergeStateData, tupDesc) == 216);
    assert!(core::mem::offset_of!(GatherMergeStateData, gm_nkeys) == 224);
    assert!(core::mem::offset_of!(GatherMergeStateData, gm_sortkeys) == 232);
    assert!(core::mem::offset_of!(GatherMergeStateData, pei) == 240);
    assert!(core::mem::offset_of!(GatherMergeStateData, nworkers_launched) == 248);
    assert!(core::mem::offset_of!(GatherMergeStateData, nreaders) == 252);
    assert!(core::mem::offset_of!(GatherMergeStateData, gm_slots) == 256);
    assert!(core::mem::offset_of!(GatherMergeStateData, reader) == 264);
    assert!(core::mem::offset_of!(GatherMergeStateData, gm_tuple_buffers) == 272);
    assert!(core::mem::offset_of!(GatherMergeStateData, gm_heap) == 280);
    assert!(core::mem::size_of::<GatherMergeStateData>() == 288);
    assert!(core::mem::align_of::<GatherMergeStateData>() == 8);
};
