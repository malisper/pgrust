//! GatherMerge node vocabulary (`nodes/plannodes.h` `GatherMerge`,
//! `executor/execnodes.h` `GatherMergeState`, and the node-private
//! `GMReaderTupleBuffer` from `nodeGatherMerge.c`).
//!
//! The embedded `PlanState` head reuses [`PlanStateData`], the leading `Plan`
//! base reuses [`crate::nodeindexscan::Plan`], the sort-support array reuses
//! [`types_sortsupport::SortSupportData`], the binary heap reuses
//! [`crate::nodemergeappend::BinaryHeap`] (the same `lib/binaryheap.c`
//! slot-index heap), and the executor-pool aliases follow the owned model
//! ([`SlotId`] for `TupleTableSlot *`). The leader's parallel handle and the
//! per-worker tuple-queue readers reuse the execParallel vocabulary
//! ([`ParallelExecutorInfo`] / [`TupleQueueReaderHandle`]).

use mcx::{Mcx, PgBox, PgVec};
use types_core::primitive::{AttrNumber, Oid};
use types_error::PgResult;
use execparallel::{ParallelExecutorInfo, TupleQueueReaderHandle};
use types_sortsupport::SortSupportData;
use types_tuple::heaptuple::FormedMinimalTuple;
use types_tuple::heaptuple::TupleDesc;

use crate::bitmapset::Bitmapset;
use crate::execnodes::{PlanStateData, SlotId};
use crate::nodeindexscan::Plan;
use crate::nodemergeappend::BinaryHeap;
use crate::nodes::NodeTag;

/// `T_GatherMerge` (nodes/nodetags.h) — the plan-node tag for a GatherMerge.
/// Value verified against the PostgreSQL 18.3 generated `nodetags.h`
/// (`T_GatherMerge = 369`).
pub const T_GatherMerge: NodeTag = NodeTag(369);
/// `T_GatherMergeState` (nodes/nodetags.h) — the executor-state node tag.
/// Value verified against the PostgreSQL 18.3 generated `nodetags.h`
/// (`T_GatherMergeState = 433`).
pub const T_GatherMergeState: NodeTag = NodeTag(433);

/// `MAX_TUPLE_STORE` (nodeGatherMerge.c) — when reading tuples from workers it
/// is efficient to read several at once (minimizing context-switch overhead),
/// but reading too many wastes memory; we read up to `MAX_TUPLE_STORE` tuples
/// in addition to the first one.
pub const MAX_TUPLE_STORE: i32 = 10;

/// `GatherMerge` plan node (plannodes.h):
///
/// ```c
/// typedef struct GatherMerge
/// {
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
#[derive(Debug, Default)]
pub struct GatherMerge<'mcx> {
    /// `Plan plan` — its first field (a `NodeTag`) makes this a `Node`.
    pub plan: Plan<'mcx>,
    /// `int num_workers` — planned number of worker processes.
    pub num_workers: i32,
    /// `int rescan_param` — ID of Param that signals a rescan, or -1.
    pub rescan_param: i32,
    /// `int numCols` — number of sort-key columns.
    pub numCols: i32,
    /// `AttrNumber *sortColIdx` — their indices in the target list.
    pub sortColIdx: alloc::vec::Vec<AttrNumber>,
    /// `Oid *sortOperators` — OIDs of the operators to sort them by.
    pub sortOperators: alloc::vec::Vec<Oid>,
    /// `Oid *collations` — OIDs of the collations.
    pub collations: alloc::vec::Vec<Oid>,
    /// `bool *nullsFirst` — NULLS FIRST/LAST directions.
    pub nullsFirst: alloc::vec::Vec<bool>,
    /// `Bitmapset *initParam` — param ids of initplans referred at this gather
    /// merge or one of its child nodes.
    pub initParam: Option<PgBox<'mcx, Bitmapset<'mcx>>>,
}

impl GatherMerge<'_> {
    /// Deep copy into `mcx` (C: `copyObject` shape). Fallible: copying the
    /// embedded plan subtree and the dense arrays allocates.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<GatherMerge<'b>> {
        Ok(GatherMerge {
            plan: self.plan.clone_in(mcx)?,
            num_workers: self.num_workers,
            rescan_param: self.rescan_param,
            numCols: self.numCols,
            sortColIdx: self.sortColIdx.clone(),
            sortOperators: self.sortOperators.clone(),
            collations: self.collations.clone(),
            nullsFirst: self.nullsFirst.clone(),
            initParam: match &self.initParam {
                Some(b) => Some(mcx::alloc_in(mcx, b.clone_in(mcx)?)?),
                None => None,
            },
        })
    }
}

/// `GMReaderTupleBuffer` (nodeGatherMerge.c, private) — pending-tuple array for
/// each worker, holding tuples we fetched but can't process yet, plus the
/// "done" flag indicating the worker is known to have no more tuples. (Not used
/// for the leader.)
///
/// ```c
/// typedef struct GMReaderTupleBuffer
/// {
///     MinimalTuple *tuple;        /* array of length MAX_TUPLE_STORE */
///     int          nTuples;       /* number of tuples currently stored */
///     int          readCounter;   /* index of next tuple to extract */
///     bool         done;          /* true if reader is known exhausted */
/// } GMReaderTupleBuffer;
/// ```
#[derive(Debug)]
pub struct GMReaderTupleBuffer<'mcx> {
    /// `MinimalTuple *tuple` — array of length `MAX_TUPLE_STORE`; `None` slots
    /// are unoccupied entries (the C `palloc0` NULL pointers). Each occupied
    /// entry is the payload-bearing [`FormedMinimalTuple`] carrier.
    pub tuple: PgVec<'mcx, Option<FormedMinimalTuple<'mcx>>>,
    /// `int nTuples` — number of tuples currently stored.
    pub nTuples: i32,
    /// `int readCounter` — index of next tuple to extract.
    pub readCounter: i32,
    /// `bool done` — true if reader is known exhausted.
    pub done: bool,
}

/// `GatherMergeState` (execnodes.h) — the per-node execution state of a
/// GatherMerge.
///
/// ```c
/// typedef struct GatherMergeState
/// {
///     PlanState   ps;
///     bool        initialized;
///     bool        gm_initialized;
///     bool        need_to_scan_locally;
///     int64       tuples_needed;
///     TupleDesc   tupDesc;
///     int         gm_nkeys;
///     SortSupport gm_sortkeys;
///     struct ParallelExecutorInfo *pei;
///     int         nworkers_launched;
///     int         nreaders;
///     TupleTableSlot **gm_slots;
///     struct TupleQueueReader **reader;
///     struct GMReaderTupleBuffer *gm_tuple_buffers;
///     struct binaryheap *gm_heap;
/// } GatherMergeState;
/// ```
#[derive(Debug)]
pub struct GatherMergeStateData<'mcx> {
    /// `PlanState ps` — its first field is `NodeTag`.
    pub ps: PlanStateData<'mcx>,
    /// `bool initialized` — workers launched?
    pub initialized: bool,
    /// `bool gm_initialized` — `gather_merge_init()` done?
    pub gm_initialized: bool,
    /// `bool need_to_scan_locally` — need to read from the local plan?
    pub need_to_scan_locally: bool,
    /// `int64 tuples_needed` — tuple bound (see `ExecSetTupleBound`).
    pub tuples_needed: i64,
    /// `TupleDesc tupDesc` — descriptor for subplan result tuples.
    pub tupDesc: TupleDesc<'mcx>,
    /// `int gm_nkeys` — number of sort columns.
    pub gm_nkeys: i32,
    /// `SortSupport gm_sortkeys` — array of length `gm_nkeys`.
    pub gm_sortkeys: PgVec<'mcx, SortSupportData<'mcx>>,
    /// `struct ParallelExecutorInfo *pei` — the leader's handle on the running
    /// parallel subplan, or `None` before launch / after cleanup.
    pub pei: Option<PgBox<'mcx, ParallelExecutorInfo<'mcx>>>,
    /// `int nworkers_launched` — original number of workers.
    pub nworkers_launched: i32,
    /// `int nreaders` — number of active workers.
    pub nreaders: i32,
    /// `TupleTableSlot **gm_slots` — array with `nreaders+1` entries (index 0 =
    /// leader). Ids into `es_tupleTable`; `None` = the C `NULL`.
    pub gm_slots: PgVec<'mcx, Option<SlotId>>,
    /// `struct TupleQueueReader **reader` — array with `nreaders` active
    /// entries (a local copy of `pei->reader`).
    pub reader: PgVec<'mcx, TupleQueueReaderHandle>,
    /// `struct GMReaderTupleBuffer *gm_tuple_buffers` — `nreaders` tuple
    /// buffers (one per worker; no leader entry).
    pub gm_tuple_buffers: PgVec<'mcx, GMReaderTupleBuffer<'mcx>>,
    /// `struct binaryheap *gm_heap` — binary heap of slot indices.
    pub gm_heap: Option<PgBox<'mcx, BinaryHeap<'mcx>>>,
}

impl<'mcx> GatherMergeStateData<'mcx> {
    /// `&node->ps` — the embedded `PlanState` head.
    #[inline]
    pub fn ps(&self) -> &PlanStateData<'mcx> {
        &self.ps
    }

    /// `&mut node->ps`.
    #[inline]
    pub fn ps_mut(&mut self) -> &mut PlanStateData<'mcx> {
        &mut self.ps
    }
}
