//! IncrementalSort plan-node / executor-state vocabulary
//! (`nodes/plannodes.h`, `executor/execnodes.h`), trimmed to what
//! `nodeIncrementalSort.c` consumes.
//!
//! Incremental sort is an optimized variant of multikey sort for cases when the
//! input is already sorted by a prefix of the sort keys.

use mcx::{alloc_in, vec_with_capacity_in, Mcx, PgBox, PgVec};
use types_core::{AttrNumber, Oid};
use types_error::PgResult;
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
#[derive(Clone, Copy, Debug, Default)]
pub struct IncrementalSortInfo {
    /// `IncrementalSortGroupInfo fullsortGroupInfo`.
    pub fullsortGroupInfo: IncrementalSortGroupInfo,
    /// `IncrementalSortGroupInfo prefixsortGroupInfo`.
    pub prefixsortGroupInfo: IncrementalSortGroupInfo,
}

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
/// The flexible array member is modelled as an owned vector.
#[derive(Clone, Debug)]
pub struct SharedIncrementalSortInfo<'mcx> {
    /// `int num_workers`.
    pub num_workers: i32,
    /// `IncrementalSortInfo sinfo[FLEXIBLE_ARRAY_MEMBER]`.
    pub sinfo: PgVec<'mcx, IncrementalSortInfo>,
}

impl<'mcx> SharedIncrementalSortInfo<'mcx> {
    /// A freshly allocated container with the flexible array empty (the C
    /// `shm_toc_allocate` + `memset(0)` before any worker fills a slot).
    pub fn new_in(mcx: Mcx<'mcx>) -> Self {
        SharedIncrementalSortInfo {
            num_workers: 0,
            sinfo: PgVec::new_in(mcx),
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
    /// is the C `NULL`.
    pub shared_info: Option<PgBox<'mcx, SharedIncrementalSortInfo<'mcx>>>,
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

/// Internal helper mirroring the per-element clone used by the parallel
/// retrieve-instrumentation copy.
#[allow(dead_code)]
fn copy_sinfo<'b>(
    mcx: Mcx<'b>,
    src: &PgVec<'_, IncrementalSortInfo>,
) -> PgResult<PgVec<'b, IncrementalSortInfo>> {
    let mut out = vec_with_capacity_in(mcx, src.len())?;
    for &v in src.iter() {
        out.push(v);
    }
    Ok(out)
}

/// Allocate a fresh [`SharedIncrementalSortInfo`] carrier in `mcx`.
#[allow(dead_code)]
pub fn alloc_shared_incremental_sort_info<'mcx>(
    mcx: Mcx<'mcx>,
) -> PgResult<PgBox<'mcx, SharedIncrementalSortInfo<'mcx>>> {
    alloc_in(mcx, SharedIncrementalSortInfo::new_in(mcx))
}
