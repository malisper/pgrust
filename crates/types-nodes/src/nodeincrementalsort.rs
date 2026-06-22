//! IncrementalSort plan-node / executor-state vocabulary
//! (`nodes/plannodes.h`, `executor/execnodes.h`), trimmed to what
//! `nodeIncrementalSort.c` consumes.
//!
//! Incremental sort is an optimized variant of multikey sort for cases when the
//! input is already sorted by a prefix of the sort keys.

use mcx::{Mcx, PgBox, PgVec};
use types_core::{AttrNumber, Oid};
use types_error::PgResult;
use types_execparallel::SerializeCursor;
use types_slot::SlotData;

use crate::execnodes::{PlanStateData, ScanStateData};
use crate::execstate_tags::T_IncrementalSortState;
use crate::nodes::NodeTag;
use crate::nodesort::{Sort, Tuplesortstate, TuplesortMethod};

// ===========================================================================
// IncrementalSort plan node (nodes/plannodes.h).
// ===========================================================================

/// `T_IncrementalSort` (nodetags.h) — the IncrementalSort plan-node tag.
pub const T_IncrementalSort: NodeTag = NodeTag(363);

/// `IncrementalSort` plan node (nodes/plannodes.h):
///
/// ```c
/// typedef struct IncrementalSort {
///     Sort        sort;
///     int         nPresortedCols;
/// } IncrementalSort;
/// ```
#[derive(Debug)]
pub struct IncrementalSort<'mcx> {
    /// `Sort sort` — the sort-plan base (embeds `Plan plan`).
    pub sort: Sort<'mcx>,
    /// `int nPresortedCols` — number of presorted columns.
    pub nPresortedCols: i32,
}

impl IncrementalSort<'_> {
    /// Deep copy into `mcx` (C: `copyObject` shape). Fallible: copying allocates.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<IncrementalSort<'b>> {
        Ok(IncrementalSort {
            sort: self.sort.clone_in(mcx)?,
            nPresortedCols: self.nPresortedCols,
        })
    }
}

// ===========================================================================
// PresortedKeyData (executor/execnodes.h).
// ===========================================================================

/// `PresortedKeyData` (execnodes.h):
///
/// ```c
/// typedef struct PresortedKeyData {
///     FmgrInfo     flinfo;     /* comparison function info */
///     FunctionCallInfo fcinfo; /* comparison function call info */
///     OffsetNumber attno;      /* attribute number in tuple */
/// } PresortedKeyData;
/// ```
///
/// The C caches the resolved equality function's `FmgrInfo`/`FunctionCallInfo`
/// for speed. In the owned model `FunctionCall2Coll` re-resolves by OID at call
/// time (the established repo pattern), so the cached call info collapses to the
/// equality function's OID plus the per-key collation; `attno` is preserved.
#[derive(Clone, Copy, Debug, Default)]
pub struct PresortedKeyData {
    /// `flinfo.fn_oid` — the equality comparison function's OID
    /// (`get_opcode(get_equality_op_for_ordering_op(sortOperators[i]))`).
    pub eq_func: Oid,
    /// `fcinfo->fncollation` — the collation the comparison runs under
    /// (`plannode->sort.collations[i]`).
    pub collation: Oid,
    /// `OffsetNumber attno` — the attribute number in the tuple
    /// (`plannode->sort.sortColIdx[i]`).
    pub attno: AttrNumber,
}

// ===========================================================================
// IncrementalSort instrumentation (executor/execnodes.h).
// ===========================================================================

/// `IncrementalSortGroupInfo` (execnodes.h):
///
/// ```c
/// typedef struct IncrementalSortGroupInfo {
///     int64  groupCount;
///     int64  maxDiskSpaceUsed;
///     int64  totalDiskSpaceUsed;
///     int64  maxMemorySpaceUsed;
///     int64  totalMemorySpaceUsed;
///     bits32 sortMethods;     /* bitmask of TuplesortMethod */
/// } IncrementalSortGroupInfo;
/// ```
///
/// `#[repr(C)]` because it is a sub-aggregate of [`IncrementalSortInfo`], which
/// is the element type of the `SharedIncrementalSortInfo` flexible-array member
/// living DIRECTLY in the parallel-query DSM segment.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct IncrementalSortGroupInfo {
    /// `int64 groupCount`.
    pub groupCount: i64,
    /// `int64 maxDiskSpaceUsed`.
    pub maxDiskSpaceUsed: i64,
    /// `int64 totalDiskSpaceUsed`.
    pub totalDiskSpaceUsed: i64,
    /// `int64 maxMemorySpaceUsed`.
    pub maxMemorySpaceUsed: i64,
    /// `int64 totalMemorySpaceUsed`.
    pub totalMemorySpaceUsed: i64,
    /// `bits32 sortMethods` — bitmask of [`TuplesortMethod`].
    pub sortMethods: u32,
}

/// `IncrementalSortInfo` (execnodes.h):
///
/// ```c
/// typedef struct IncrementalSortInfo {
///     IncrementalSortGroupInfo fullsortGroupInfo;
///     IncrementalSortGroupInfo prefixsortGroupInfo;
/// } IncrementalSortInfo;
/// ```
///
/// `#[repr(C)]` because it is the element type of the
/// `SharedIncrementalSortInfo` flexible-array member that lives DIRECTLY in the
/// parallel-query DSM segment (`ExecIncrementalSortInitializeDSM`
/// `shm_toc_allocate`s the chunk and each worker folds its own
/// `sinfo[ParallelWorkerNumber]` slot). Placed/attached through the typed
/// shared-DSM-object flex primitive (`shared_dsm_object::place_flex` /
/// `attach_flex`).
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct IncrementalSortInfo {
    /// `IncrementalSortGroupInfo fullsortGroupInfo`.
    pub fullsortGroupInfo: IncrementalSortGroupInfo,
    /// `IncrementalSortGroupInfo prefixsortGroupInfo`.
    pub prefixsortGroupInfo: IncrementalSortGroupInfo,
}

// SAFETY (audited per the `SharedDsmObject` contract): `IncrementalSortInfo` is
//   1. `#[repr(C)]` and matches `execnodes.h` field-for-field (two
//      `IncrementalSortGroupInfo`s, each five `int64`s + one `bits32`, all POD).
//   2. Each parallel worker folds ONLY its own `sinfo[ParallelWorkerNumber]`
//      slot (in `INSTRUMENT_SORT_GROUP`), and the leader reads the whole array
//      only in `ExecIncrementalSortRetrieveInstrumentation` after the workers
//      have detached; element bytes are never aliased-and-mutated concurrently.
//   3. The leader's placement initializer zero-fills every element before any
//      worker attaches (`place_flex` writes `IncrementalSortInfo::default()`).
//   4. A shared `&IncrementalSortInfo` aliasing another process's mapping of the
//      SAME element is never created concurrently with a write (clause 2).
unsafe impl types_parallel::SharedDsmObject for IncrementalSortInfo {}

/// `offsetof(SharedIncrementalSortInfo, sinfo)`-bearing header of
/// `SharedIncrementalSortInfo` (execnodes.h): `{ int num_workers;
/// IncrementalSortInfo sinfo[]; }`. The `H` of the `place_flex`/`attach_flex`
/// flexible-array placement; the `sinfo[]` tail is the `E = IncrementalSortInfo`
/// slice.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct SharedIncrementalSortInfoHeader {
    /// `int num_workers`.
    pub num_workers: i32,
}

// SAFETY: `#[repr(C)]` POD header written once by the leader
// (`ExecIncrementalSortInitializeDSM`) before any worker attaches, read-only
// thereafter; no concurrent mutation. Matches the C header field-for-field.
unsafe impl types_parallel::SharedDsmObject for SharedIncrementalSortInfoHeader {}

/// `SharedIncrementalSortInfo` (execnodes.h) — shared-memory container for
/// per-worker incremental-sort information:
///
/// ```c
/// typedef struct SharedIncrementalSortInfo {
///     int num_workers;
///     IncrementalSortInfo sinfo[FLEXIBLE_ARRAY_MEMBER];
/// } SharedIncrementalSortInfo;
/// ```
///
/// In C this is a single `SharedIncrementalSortInfo *` pointer that is FIRST the
/// DSM-resident shared area (set in `ExecIncrementalSortInitializeDSM` /
/// inherited by workers via `shm_toc_lookup`) and is LATER REPLACED, in
/// `ExecIncrementalSortRetrieveInstrumentation`, by a backend-local `palloc`'d
/// copy. Each worker folds its own `sinfo[ParallelWorkerNumber]` slot in the DSM
/// array directly (in `INSTRUMENT_SORT_GROUP`). The two states have different
/// ownership (cross-process DSM view vs. owned backend-local array), so they are
/// modelled as the two arms — mirroring `SharedSortInfo`.
#[derive(Debug)]
pub enum SharedIncrementalSortInfo<'mcx> {
    /// The DSM-resident shared area: a cursor to the `shm_toc`-allocated chunk
    /// (`{ SharedIncrementalSortInfoHeader; IncrementalSortInfo[num_workers] }`)
    /// plus the worker count needed to recover the flex length.
    Dsm {
        /// Real in-segment chunk address (the
        /// `shm_toc_allocate`/`shm_toc_lookup` return value).
        chunk: SerializeCursor,
        /// The DSM segment the chunk lives in, so the retrieve path can
        /// `attach_flex` the array and the worker fold can `with_mut` its slot
        /// before detach.
        seg: types_execparallel::DsmSegmentHandle,
        /// `shared_info->num_workers`.
        num_workers: i32,
    },
    /// The backend-local copy `ExecIncrementalSortRetrieveInstrumentation` makes
    /// before the DSM segment is detached.
    Local {
        /// `shared_info->num_workers`.
        num_workers: i32,
        /// `IncrementalSortInfo sinfo[]` copied out of DSM.
        sinfo: PgVec<'mcx, IncrementalSortInfo>,
    },
}

impl<'mcx> SharedIncrementalSortInfo<'mcx> {
    /// `shared_info->num_workers` — the number of per-worker slots, regardless of
    /// arm.
    pub fn num_workers(&self) -> i32 {
        match self {
            SharedIncrementalSortInfo::Dsm { num_workers, .. } => *num_workers,
            SharedIncrementalSortInfo::Local { num_workers, .. } => *num_workers,
        }
    }
}

// ===========================================================================
// IncrementalSortExecutionStatus (executor/execnodes.h).
// ===========================================================================

/// `IncrementalSortExecutionStatus` (execnodes.h) — which mode of the two-mode
/// hybrid algorithm the node is in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(i32)]
pub enum IncrementalSortExecutionStatus {
    /// `INCSORT_LOADFULLSORT` — accumulating tuples into the full sort state.
    INCSORT_LOADFULLSORT = 0,
    /// `INCSORT_LOADPREFIXSORT` — accumulating tuples into the prefix sort state.
    INCSORT_LOADPREFIXSORT = 1,
    /// `INCSORT_READFULLSORT` — reading sorted tuples from the full sort state.
    INCSORT_READFULLSORT = 2,
    /// `INCSORT_READPREFIXSORT` — reading sorted tuples from the prefix state.
    INCSORT_READPREFIXSORT = 3,
}

pub use IncrementalSortExecutionStatus::{
    INCSORT_LOADFULLSORT, INCSORT_LOADPREFIXSORT, INCSORT_READFULLSORT, INCSORT_READPREFIXSORT,
};

// ===========================================================================
// IncrementalSortState executor node (executor/execnodes.h).
// ===========================================================================

/// `IncrementalSortState` (execnodes.h) — owned-tree form of the incremental
/// sort executor node:
///
/// ```c
/// typedef struct IncrementalSortState {
///     ScanState   ss;
///     bool        bounded;
///     int64       bound;
///     bool        outerNodeDone;
///     int64       bound_Done;
///     IncrementalSortExecutionStatus execution_status;
///     int64       n_fullsort_remaining;
///     Tuplesortstate *fullsort_state;
///     Tuplesortstate *prefixsort_state;
///     PresortedKeyData *presorted_keys;
///     IncrementalSortInfo incsort_info;
///     TupleTableSlot *group_pivot;
///     TupleTableSlot *transfer_tuple;
///     bool        am_worker;
///     SharedIncrementalSortInfo *shared_info;
/// } IncrementalSortState;
/// ```
#[derive(Debug, Default)]
pub struct IncrementalSortStateData<'mcx> {
    /// `ScanState ss` — its first field is `NodeTag`.
    pub ss: ScanStateData<'mcx>,
    /// `bool bounded` — is the result set bounded?
    pub bounded: bool,
    /// `int64 bound` — if bounded, how many tuples are needed.
    pub bound: i64,
    /// `bool outerNodeDone` — finished fetching tuples from outer node?
    pub outerNodeDone: bool,
    /// `int64 bound_Done` — value of bound we did the sort with.
    pub bound_Done: i64,
    /// `IncrementalSortExecutionStatus execution_status`.
    pub execution_status: IncrementalSortExecutionStatus,
    /// `int64 n_fullsort_remaining`.
    pub n_fullsort_remaining: i64,
    /// `Tuplesortstate *fullsort_state` — private state of tuplesort.c. `None`
    /// is the C `NULL`.
    pub fullsort_state: Option<PgBox<'mcx, Tuplesortstate<'mcx>>>,
    /// `Tuplesortstate *prefixsort_state`. `None` is the C `NULL`.
    pub prefixsort_state: Option<PgBox<'mcx, Tuplesortstate<'mcx>>>,
    /// `PresortedKeyData *presorted_keys` — the keys by which the input path is
    /// already sorted (`nPresortedCols` long). `None` is the C `NULL`
    /// (uninitialized until `preparePresortedCols`).
    pub presorted_keys: Option<PgVec<'mcx, PresortedKeyData>>,
    /// `IncrementalSortInfo incsort_info`.
    pub incsort_info: IncrementalSortInfo,
    /// `TupleTableSlot *group_pivot` — slot for the pivot tuple defining the
    /// presorted-key values within a group. `None` is the C `NULL`. A standalone
    /// slot (`MakeSingleTupleTableSlot`), not in `es_tupleTable`.
    pub group_pivot: Option<PgBox<'mcx, SlotData<'mcx>>>,
    /// `TupleTableSlot *transfer_tuple` — carry-over slot between batches.
    /// `None` is the C `NULL`. Standalone slot.
    pub transfer_tuple: Option<PgBox<'mcx, SlotData<'mcx>>>,
    /// `bool am_worker` — are we a worker?
    pub am_worker: bool,
    /// `SharedIncrementalSortInfo *shared_info` — one entry per worker. `None`
    /// is the C `NULL`. Either the DSM-resident shared area (leader after
    /// `ExecIncrementalSortInitializeDSM` / worker after
    /// `ExecIncrementalSortInitializeWorker`) or the backend-local copy (leader
    /// after `ExecIncrementalSortRetrieveInstrumentation`).
    pub shared_info: Option<SharedIncrementalSortInfo<'mcx>>,
}

impl Default for IncrementalSortExecutionStatus {
    fn default() -> Self {
        IncrementalSortExecutionStatus::INCSORT_LOADFULLSORT
    }
}

impl<'mcx> IncrementalSortStateData<'mcx> {
    /// `&node->ss.ps` — the embedded `PlanState` head.
    #[inline]
    pub fn ps(&self) -> &PlanStateData<'mcx> {
        &self.ss.ps
    }

    /// `&mut node->ss.ps`.
    #[inline]
    pub fn ps_mut(&mut self) -> &mut PlanStateData<'mcx> {
        &mut self.ss.ps
    }
}

/// `nodeTag(IncrementalSortState)`.
pub const fn incremental_sort_state_tag() -> NodeTag {
    T_IncrementalSortState
}

/// `(int) sortMethod` for OR-ing a [`TuplesortInstrumentation`]'s method into a
/// group-info `sortMethods` bitmask (the C `groupInfo->sortMethods |=
/// sort_instr.sortMethod`).
#[inline]
pub fn tuplesort_method_bits(method: TuplesortMethod) -> u32 {
    method as i32 as u32
}

