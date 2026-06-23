//! Gather node vocabulary (`nodes/plannodes.h` `Gather` and
//! `executor/execnodes.h` `GatherState`).
//!
//! The embedded `PlanState` head reuses [`PlanStateData`], the leading `Plan`
//! base reuses [`crate::nodeindexscan::Plan`], and the executor-pool aliases
//! follow the owned model ([`SlotId`] for `TupleTableSlot *`). The leader's
//! parallel handle and the per-worker tuple-queue readers reuse the
//! execParallel vocabulary ([`ParallelExecutorInfo`] /
//! [`TupleQueueReaderHandle`]). Mirrors the sibling
//! [`crate::nodegathermerge`].

use mcx::{Mcx, PgBox, PgVec};
use types_error::PgResult;
use execparallel::{ParallelExecutorInfo, TupleQueueReaderHandle};

use crate::bitmapset::Bitmapset;
use crate::execnodes::{PlanStateData, SlotId};
use crate::nodeindexscan::Plan;
use crate::nodes::NodeTag;

/// `T_Gather` (nodes/nodetags.h) — the plan-node tag for a Gather. Value
/// verified against the PostgreSQL 18.3 generated `nodetags.h`
/// (`T_Gather = 368`).
pub const T_Gather: NodeTag = NodeTag(368);
/// `T_GatherState` (nodes/nodetags.h) — the executor-state node tag. Value
/// verified against the PostgreSQL 18.3 generated `nodetags.h`
/// (`T_GatherState = 432`).
pub const T_GatherState: NodeTag = NodeTag(432);

/// `Gather` plan node (plannodes.h):
///
/// ```c
/// typedef struct Gather
/// {
///     Plan        plan;
///     int         num_workers;
///     int         rescan_param;
///     bool        single_copy;
///     bool        invisible;
///     Bitmapset  *initParam;
/// } Gather;
/// ```
#[derive(Debug, Default)]
pub struct Gather<'mcx> {
    /// `Plan plan` — its first field (a `NodeTag`) makes this a `Node`.
    pub plan: Plan<'mcx>,
    /// `int num_workers` — planned number of worker processes.
    pub num_workers: i32,
    /// `int rescan_param` — ID of Param that signals a rescan, or -1.
    pub rescan_param: i32,
    /// `bool single_copy` — don't execute plan more than once.
    pub single_copy: bool,
    /// `bool invisible` — suppress EXPLAIN display (for testing)?
    pub invisible: bool,
    /// `Bitmapset *initParam` — param ids of initplans referred at this gather
    /// or one of its child nodes.
    pub initParam: Option<PgBox<'mcx, Bitmapset<'mcx>>>,
}

impl Gather<'_> {
    /// Deep copy into `mcx` (C: `copyObject` shape). Fallible: copying the
    /// embedded plan subtree allocates.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<Gather<'b>> {
        Ok(Gather {
            plan: self.plan.clone_in(mcx)?,
            num_workers: self.num_workers,
            rescan_param: self.rescan_param,
            single_copy: self.single_copy,
            invisible: self.invisible,
            initParam: match &self.initParam {
                Some(b) => Some(mcx::alloc_in(mcx, b.clone_in(mcx)?)?),
                None => None,
            },
        })
    }
}

/// `GatherState` (execnodes.h) — the per-node execution state of a Gather.
///
/// ```c
/// typedef struct GatherState
/// {
///     PlanState   ps;
///     bool        initialized;
///     bool        need_to_scan_locally;
///     int64       tuples_needed;
///     TupleTableSlot *funnel_slot;
///     struct ParallelExecutorInfo *pei;
///     int         nworkers_launched;
///     int         nreaders;
///     int         nextreader;
///     struct TupleQueueReader **reader;
/// } GatherState;
/// ```
#[derive(Debug)]
pub struct GatherStateData<'mcx> {
    /// `PlanState ps` — its first field is `NodeTag`.
    pub ps: PlanStateData<'mcx>,
    /// `bool initialized` — workers launched?
    pub initialized: bool,
    /// `bool need_to_scan_locally` — need to read from the local plan?
    pub need_to_scan_locally: bool,
    /// `int64 tuples_needed` — tuple bound (see `ExecSetTupleBound`).
    pub tuples_needed: i64,
    /// `TupleTableSlot *funnel_slot` — the slot the worker tuples are funnelled
    /// into. An id into `es_tupleTable`; `None` = the C `NULL`.
    pub funnel_slot: Option<SlotId>,
    /// `struct ParallelExecutorInfo *pei` — the leader's handle on the running
    /// parallel subplan, or `None` before launch / after cleanup.
    pub pei: Option<PgBox<'mcx, ParallelExecutorInfo<'mcx>>>,
    /// `int nworkers_launched` — original number of workers.
    pub nworkers_launched: i32,
    /// `int nreaders` — number of still-active workers.
    pub nreaders: i32,
    /// `int nextreader` — next one to try to read from.
    pub nextreader: i32,
    /// `struct TupleQueueReader **reader` — array with `nreaders` active
    /// entries (a local copy of `pei->reader`).
    pub reader: PgVec<'mcx, TupleQueueReaderHandle>,
}

impl<'mcx> GatherStateData<'mcx> {
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
