//! Gather node vocabulary (`nodes/plannodes.h` `Gather`, `executor/execnodes.h`
//! `GatherState`).
//!
//! The leading `Plan` plan base reuses [`crate::nodeindexscan::Plan`], the
//! embedded `PlanState` head reuses [`PlanStateData`], and the executor-pool /
//! parallel handles follow the owned model: [`SlotId`] for `TupleTableSlot *`,
//! the leader's [`types_execparallel::ParallelExecutorInfo`] for
//! `struct ParallelExecutorInfo *pei`, and a working array of
//! [`types_execparallel::TupleQueueReaderHandle`] for the `node->reader`
//! `palloc`/`memcpy`/`memmove`/`pfree`d block.

use mcx::{Mcx, PgBox, PgVec};
use types_error::PgResult;
use types_execparallel::{ParallelExecutorInfo, TupleQueueReaderHandle};

use crate::execnodes::{PlanStateData, SlotId};
use crate::nodeindexscan::Plan;
use crate::nodes::NodeTag;

/// `T_Gather` (nodes/nodetags.h) — the plan-node tag for a Gather.
pub const T_Gather: NodeTag = NodeTag(368);
/// `T_GatherState` (nodes/nodetags.h) — the executor-state node tag.
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
    /// `Plan plan` — its first field starts with the `NodeTag`.
    pub plan: Plan<'mcx>,
    /// `int num_workers` — planned number of worker processes.
    pub num_workers: i32,
    /// `int rescan_param` — ID of the Param that signals a rescan, or -1.
    pub rescan_param: i32,
    /// `bool single_copy` — don't execute the plan more than once.
    pub single_copy: bool,
    /// `bool invisible` — suppress EXPLAIN display (for testing)?
    pub invisible: bool,
    /// `Bitmapset *initParam` — param ids of initplans referred to at the
    /// gather node or one of its children. `None` is the C NULL.
    pub initParam: Option<PgBox<'mcx, crate::bitmapset::Bitmapset<'mcx>>>,
}

/// `GatherState` (execnodes.h) — the per-node execution state of a Gather:
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
    /// `TupleTableSlot *funnel_slot` — id into `es_tupleTable`. Set up once.
    pub funnel_slot: Option<SlotId>,
    /// `struct ParallelExecutorInfo *pei` — the leader's parallel-subplan
    /// handle (`None` before first launch / after cleanup). Set up once.
    pub pei: Option<PgBox<'mcx, ParallelExecutorInfo<'mcx>>>,
    /// `int nworkers_launched` — original number of workers (for EXPLAIN).
    /// Reinitialized during a rescan.
    pub nworkers_launched: i32,
    /// `int nreaders` — number of still-active workers.
    pub nreaders: i32,
    /// `int nextreader` — next one to try to read from.
    pub nextreader: i32,
    /// `struct TupleQueueReader **reader` — working array with `nreaders`
    /// active entries (C: `palloc`ed and `memcpy`d from `pei->reader`,
    /// shrunk in place with `memmove`, `pfree`d at shutdown). Empty is the C
    /// NULL.
    pub reader: PgVec<'mcx, TupleQueueReaderHandle>,
}

impl<'mcx> GatherStateData<'mcx> {
    /// `makeNode(GatherState)` — a zeroed Gather state with its `reader`
    /// working array empty (`PgVec::new_in(mcx)`), matching the C `palloc0`.
    pub fn new(mcx: Mcx<'mcx>) -> Self {
        GatherStateData {
            ps: PlanStateData::default(),
            initialized: false,
            need_to_scan_locally: false,
            tuples_needed: 0,
            funnel_slot: None,
            pei: None,
            nworkers_launched: 0,
            nreaders: 0,
            nextreader: 0,
            reader: PgVec::new_in(mcx),
        }
    }
}

impl Gather<'_> {
    /// Deep copy into `mcx` (C: `copyObject` shape). Fallible: copying the
    /// embedded plan subtree allocates.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<Gather<'b>> {
        let initParam = match &self.initParam {
            Some(bms) => Some(mcx::alloc_in(mcx, bms.clone_in(mcx)?)?),
            None => None,
        };
        Ok(Gather {
            plan: self.plan.clone_in(mcx)?,
            num_workers: self.num_workers,
            rescan_param: self.rescan_param,
            single_copy: self.single_copy,
            invisible: self.invisible,
            initParam,
        })
    }
}
