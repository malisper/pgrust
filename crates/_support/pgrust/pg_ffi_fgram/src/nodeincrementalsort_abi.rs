//! `#[repr(C)]` ABI for `nodeIncrementalSort.c` (the incremental-sort executor
//! node).
//!
//! The incremental-sort node is ported in-crate
//! (`backend-executor-nodeIncrementalSort`), so its state node is a complete,
//! address-stable `#[repr(C)]` struct laid out exactly like the C
//! `IncrementalSortState` (execnodes.h). The `Sort`/`IncrementalSort` plan nodes
//! it navigates, the `PresortedKeyData` comparison cache, the per-group/per-info
//! instrumentation containers, and the `TuplesortInstrumentation` value that
//! `tuplesort_get_stats` fills in are all spelled out here for layout fidelity.
//!
//! The embedded `ScanState`/`PlanState` heads reuse the shared
//! [`crate::ScanStateData`] / [`crate::PlanStateData`] layouts from `execnodes`;
//! the plan-node `Plan` head reuses [`crate::PlanNode`]. `Tuplesortstate` is an
//! opaque tuplesort-private type, modelled as `c_void`.

use core::ffi::{c_int, c_void};

use crate::{
    bits32, AttrNumber, FmgrInfo, FunctionCallInfo, NodeTag, OffsetNumber, Oid, PlanNode,
    ScanStateData, TupleTableSlot,
};

/// NodeTag for `IncrementalSort` (the plan node). Matches `T_IncrementalSort`.
pub const T_IncrementalSort: NodeTag = 363;
/// NodeTag for `IncrementalSortState` (the executor state node). Matches
/// `T_IncrementalSortState`.
pub const T_IncrementalSortState: NodeTag = 427;

/// `Tuplesortstate` — opaque tuplesort-private state (`tuplesort.c`). Only its
/// address is ever held; the node never inspects its contents.
pub type Tuplesortstate = c_void;

// ===========================================================================
// Plan nodes (plannodes.h).
// ===========================================================================

/// `Sort` plan node (plannodes.h):
///
/// ```c
/// typedef struct Sort {
///     Plan        plan;
///     int         numCols;
///     AttrNumber *sortColIdx;
///     Oid        *sortOperators;
///     Oid        *collations;
///     bool       *nullsFirst;
/// } Sort;
/// ```
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct Sort {
    /// `Plan plan` — the plan-node base (its first field is `NodeTag`).
    pub plan: PlanNode,
    /// `int numCols` — number of sort-key columns.
    pub numCols: c_int,
    /// `AttrNumber *sortColIdx` — their indexes in the target list (length `numCols`).
    pub sortColIdx: *mut AttrNumber,
    /// `Oid *sortOperators` — OIDs of operators to sort them by (length `numCols`).
    pub sortOperators: *mut Oid,
    /// `Oid *collations` — OIDs of collations (length `numCols`).
    pub collations: *mut Oid,
    /// `bool *nullsFirst` — NULLS FIRST/LAST directions (length `numCols`).
    pub nullsFirst: *mut bool,
}

/// `IncrementalSort` plan node (plannodes.h):
///
/// ```c
/// typedef struct IncrementalSort {
///     Sort        sort;
///     int         nPresortedCols;
/// } IncrementalSort;
/// ```
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct IncrementalSort {
    /// `Sort sort` — the sort-plan base (embeds `Plan plan`).
    pub sort: Sort,
    /// `int nPresortedCols` — number of presorted columns.
    pub nPresortedCols: c_int,
}

// ===========================================================================
// tuplesort instrumentation (utils/tuplesort.h).
// ===========================================================================

/// `TuplesortMethod` — sort algorithm used (`tuplesort.h`). A bitmask value; the
/// per-group `sortMethods` field OR's these together.
pub type TuplesortMethod = c_int;
/// `SORT_TYPE_STILL_IN_PROGRESS`.
pub const SORT_TYPE_STILL_IN_PROGRESS: TuplesortMethod = 0;
/// `SORT_TYPE_TOP_N_HEAPSORT`.
pub const SORT_TYPE_TOP_N_HEAPSORT: TuplesortMethod = 1 << 0;
/// `SORT_TYPE_QUICKSORT`.
pub const SORT_TYPE_QUICKSORT: TuplesortMethod = 1 << 1;
/// `SORT_TYPE_EXTERNAL_SORT`.
pub const SORT_TYPE_EXTERNAL_SORT: TuplesortMethod = 1 << 2;
/// `SORT_TYPE_EXTERNAL_MERGE`.
pub const SORT_TYPE_EXTERNAL_MERGE: TuplesortMethod = 1 << 3;

/// `TuplesortSpaceType` — type of space `spaceUsed` represents (`tuplesort.h`).
pub type TuplesortSpaceType = c_int;
/// `SORT_SPACE_TYPE_DISK`.
pub const SORT_SPACE_TYPE_DISK: TuplesortSpaceType = 0;
/// `SORT_SPACE_TYPE_MEMORY`.
pub const SORT_SPACE_TYPE_MEMORY: TuplesortSpaceType = 1;

/// `TUPLESORT_NONE` (tuplesort.h) — no tuplesort option flags.
pub const TUPLESORT_NONE: c_int = 0;
/// `TUPLESORT_RANDOMACCESS` — non-sequential access to the result required.
pub const TUPLESORT_RANDOMACCESS: c_int = 1 << 0;
/// `TUPLESORT_ALLOWBOUNDED` — the tuplesort can support bounded sorts.
pub const TUPLESORT_ALLOWBOUNDED: c_int = 1 << 1;

/// `TuplesortInstrumentation` (tuplesort.h) — sort statistics filled in by
/// `tuplesort_get_stats`.
///
/// ```c
/// typedef struct TuplesortInstrumentation {
///     TuplesortMethod sortMethod;
///     TuplesortSpaceType spaceType;
///     int64 spaceUsed;
/// } TuplesortInstrumentation;
/// ```
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TuplesortInstrumentation {
    /// `TuplesortMethod sortMethod` — sort algorithm used.
    pub sortMethod: TuplesortMethod,
    /// `TuplesortSpaceType spaceType` — type of space `spaceUsed` represents.
    pub spaceType: TuplesortSpaceType,
    /// `int64 spaceUsed` — space consumption, in kB.
    pub spaceUsed: i64,
}

// ===========================================================================
// Executor state node (execnodes.h).
// ===========================================================================

/// `PresortedKeyData` (execnodes.h) — information about one presorted key,
/// caching its equality-comparison function so `isCurrentGroup` can call it
/// without re-resolving the operator each time.
///
/// ```c
/// typedef struct PresortedKeyData {
///     FmgrInfo    flinfo;
///     FunctionCallInfo fcinfo;
///     OffsetNumber attno;
/// } PresortedKeyData;
/// ```
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct PresortedKeyData {
    /// `FmgrInfo flinfo` — comparison function info.
    pub flinfo: FmgrInfo,
    /// `FunctionCallInfo fcinfo` — comparison function call info.
    pub fcinfo: FunctionCallInfo,
    /// `OffsetNumber attno` — attribute number in tuple.
    pub attno: OffsetNumber,
}

/// `IncrementalSortGroupInfo` (execnodes.h) — accumulated tuplesort statistics
/// for one sort kind (full sort or prefix sort), used by EXPLAIN ANALYZE.
///
/// ```c
/// typedef struct IncrementalSortGroupInfo {
///     int64   groupCount;
///     int64   maxDiskSpaceUsed;
///     int64   totalDiskSpaceUsed;
///     int64   maxMemorySpaceUsed;
///     int64   totalMemorySpaceUsed;
///     bits32  sortMethods;
/// } IncrementalSortGroupInfo;
/// ```
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
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
    /// `bits32 sortMethods` — bitmask of `TuplesortMethod`.
    pub sortMethods: bits32,
}

/// `IncrementalSortInfo` (execnodes.h) — full-sort and prefix-sort group infos.
///
/// ```c
/// typedef struct IncrementalSortInfo {
///     IncrementalSortGroupInfo fullsortGroupInfo;
///     IncrementalSortGroupInfo prefixsortGroupInfo;
/// } IncrementalSortInfo;
/// ```
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct IncrementalSortInfo {
    /// `IncrementalSortGroupInfo fullsortGroupInfo`.
    pub fullsortGroupInfo: IncrementalSortGroupInfo,
    /// `IncrementalSortGroupInfo prefixsortGroupInfo`.
    pub prefixsortGroupInfo: IncrementalSortGroupInfo,
}

/// `SharedIncrementalSortInfo` (execnodes.h) — shared-memory container for
/// per-worker incremental sort information.
///
/// ```c
/// typedef struct SharedIncrementalSortInfo {
///     int num_workers;
///     IncrementalSortInfo sinfo[FLEXIBLE_ARRAY_MEMBER];
/// } SharedIncrementalSortInfo;
/// ```
///
/// The flexible array member `sinfo` is modelled as a zero-length array; the
/// node indexes into it via the trailing-storage helpers.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct SharedIncrementalSortInfo {
    /// `int num_workers`.
    pub num_workers: c_int,
    /// `IncrementalSortInfo sinfo[FLEXIBLE_ARRAY_MEMBER]`.
    pub sinfo: [IncrementalSortInfo; 0],
}

/// `IncrementalSortExecutionStatus` (execnodes.h) — the executor state machine's
/// current mode.
///
/// ```c
/// typedef enum {
///     INCSORT_LOADFULLSORT,
///     INCSORT_LOADPREFIXSORT,
///     INCSORT_READFULLSORT,
///     INCSORT_READPREFIXSORT,
/// } IncrementalSortExecutionStatus;
/// ```
pub type IncrementalSortExecutionStatus = c_int;
/// `INCSORT_LOADFULLSORT`.
pub const INCSORT_LOADFULLSORT: IncrementalSortExecutionStatus = 0;
/// `INCSORT_LOADPREFIXSORT`.
pub const INCSORT_LOADPREFIXSORT: IncrementalSortExecutionStatus = 1;
/// `INCSORT_READFULLSORT`.
pub const INCSORT_READFULLSORT: IncrementalSortExecutionStatus = 2;
/// `INCSORT_READPREFIXSORT`.
pub const INCSORT_READPREFIXSORT: IncrementalSortExecutionStatus = 3;

/// `IncrementalSortState` (execnodes.h) — faithful `#[repr(C)]` ABI for the
/// incremental-sort executor node.
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
///
/// The leading [`ScanStateData`] head's first field is a `NodeTag`, so a
/// `*mut IncrementalSortStateData` is also a valid `Node *` / `PlanState *` and
/// the opaque public `IncrementalSortState *` handle.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct IncrementalSortStateData {
    /// `ScanState ss` — its first field is `NodeTag`.
    pub ss: ScanStateData,
    /// `bool bounded` — is the result set bounded?
    pub bounded: bool,
    /// `int64 bound` — if bounded, how many tuples are needed.
    pub bound: i64,
    /// `bool outerNodeDone` — finished fetching tuples from outer node.
    pub outerNodeDone: bool,
    /// `int64 bound_Done` — value of bound we did the sort with.
    pub bound_Done: i64,
    /// `IncrementalSortExecutionStatus execution_status`.
    pub execution_status: IncrementalSortExecutionStatus,
    /// `int64 n_fullsort_remaining`.
    pub n_fullsort_remaining: i64,
    /// `Tuplesortstate *fullsort_state` — private state of tuplesort.c.
    pub fullsort_state: *mut Tuplesortstate,
    /// `Tuplesortstate *prefixsort_state` — private state of tuplesort.c.
    pub prefixsort_state: *mut Tuplesortstate,
    /// `PresortedKeyData *presorted_keys` — the keys by which the input path is
    /// already sorted.
    pub presorted_keys: *mut PresortedKeyData,
    /// `IncrementalSortInfo incsort_info`.
    pub incsort_info: IncrementalSortInfo,
    /// `TupleTableSlot *group_pivot` — slot for pivot tuple defining values of
    /// presorted keys within group.
    pub group_pivot: *mut TupleTableSlot,
    /// `TupleTableSlot *transfer_tuple`.
    pub transfer_tuple: *mut TupleTableSlot,
    /// `bool am_worker` — are we a worker?
    pub am_worker: bool,
    /// `SharedIncrementalSortInfo *shared_info` — one entry per worker.
    pub shared_info: *mut SharedIncrementalSortInfo,
}

// Layout asserts: the embedded heads keep their C offsets so a
// `*mut IncrementalSortStateData` can be navigated as the C
// `IncrementalSortState *`, and the plan-node heads match.
const _: () = {
    assert!(core::mem::offset_of!(IncrementalSortStateData, ss) == 0);
    assert!(core::mem::offset_of!(ScanStateData, ps) == 0);
    assert!(core::mem::offset_of!(crate::PlanStateData, type_) == 0);
    assert!(core::mem::offset_of!(IncrementalSort, sort) == 0);
    assert!(core::mem::offset_of!(Sort, plan) == 0);
    assert!(core::mem::offset_of!(PlanNode, type_) == 0);
    // `nPresortedCols` immediately follows the embedded `Sort`.
    assert!(core::mem::offset_of!(IncrementalSort, nPresortedCols) == core::mem::size_of::<Sort>());
    // `sinfo` flexible array begins right after `num_workers` (with padding to
    // the 8-byte alignment of `IncrementalSortInfo`).
    assert!(core::mem::offset_of!(SharedIncrementalSortInfo, sinfo) == 8);
};
