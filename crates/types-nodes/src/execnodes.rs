//! Executor node vocabulary (executor/execnodes.h plus the `sdir.h` scan
//! direction), trimmed.
//!
//! In the owned-tree model each `<Node>StateData` layout carries its fields as
//! owned children (`Option<PgBox<'mcx, T>>` for a single nullable pointee,
//! `PgVec<'mcx, T>` for a counted array), allocated in the per-query memory
//! context whose `'mcx` the tree carries. `TupleTableSlot *` fields are
//! [`SlotId`] indexes into the owning [`EStateData::es_tupleTable`] slot pool,
//! exactly as C's slot pointers point into the `es_tupleTable`-owned objects.
//! The C `PlanState.state` back-pointer to the `EState` is not carried: the
//! owned model threads `&mut EStateData` explicitly through the executor entry
//! points instead.

use mcx::{Mcx, PgBox, PgVec};
use types_core::PgResult;

use crate::bitmapset::Bitmapset;
use crate::execexpr::{ProjectionInfo, SubPlanState};
use crate::executor::TupleTableSlot;
use crate::instrument::Instrumentation;
use crate::planstate::PlanStateNode;
use types_core::NodeTag;

/// `T_MaterialState` (nodes/nodetags.h) — the executor-state node tag for a
/// Material node.
pub const T_MaterialState: NodeTag = 424;

/// `ExprContext` (execnodes.h) — per-node expression-evaluation context.
/// Trimmed to a presence marker: ports so far only test `ps_ExprContext` for
/// NULL-ness and hand it across the `ReScanExprContext` seam; the working
/// fields arrive with the expression-machinery owners.
#[derive(Debug, Default)]
pub struct ExprContext;

/// `ScanDirection` (access/sdir.h). Kept as the raw C scale so direction
/// comparisons read like the original.
pub type ScanDirection = i32;

pub const BackwardScanDirection: ScanDirection = -1;
pub const NoMovementScanDirection: ScanDirection = 0;
pub const ForwardScanDirection: ScanDirection = 1;

/// `ScanDirectionIsForward(direction)` (sdir.h).
pub const fn ScanDirectionIsForward(direction: ScanDirection) -> bool {
    direction == ForwardScanDirection
}

/// `ScanDirectionIsBackward(direction)` (sdir.h).
pub const fn ScanDirectionIsBackward(direction: ScanDirection) -> bool {
    direction == BackwardScanDirection
}

/// `TupleTableSlot *` in the owned model: a `Copy` index into the owning
/// [`EStateData::es_tupleTable`] slot pool.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SlotId(pub u32);

/// `ExecProcNodeMtd` — the per-node execution callback stored in
/// `PlanState.ExecProcNode`. The cross-node recursion `ExecProcNode(child)`
/// dispatches through this pointer (installed at node init). Returns the
/// `SlotId` of the produced tuple's slot, or `None` for the C `NULL` return.
/// The callback is tied to the state tree's allocator lifetime: any memory it
/// needs (C: `palloc` while executing) comes from
/// [`EStateData::es_query_cxt`].
pub type ExecProcNodeMtd<'mcx> = Option<
    fn(
        pstate: &mut PlanStateNode<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<SlotId>>,
>;

/// `PlanState` head (execnodes.h), trimmed to the fields ports consume.
#[derive(Debug, Default)]
pub struct PlanStateData<'mcx> {
    /// `Plan *plan` — associated plan node.
    pub plan: Option<PgBox<'mcx, crate::nodes::Node<'mcx>>>,
    /// `ExecProcNodeMtd ExecProcNode` — function to return next tuple.
    pub ExecProcNode: ExecProcNodeMtd<'mcx>,
    /// `Instrumentation *instrument` — optional runtime stats for this node.
    pub instrument: Option<PgBox<'mcx, Instrumentation>>,
    /// `struct PlanState *lefttree` — input plan tree (`outerPlanState`).
    pub lefttree: Option<PgBox<'mcx, PlanStateNode<'mcx>>>,
    /// `struct PlanState *righttree` — `innerPlanState(node)`.
    pub righttree: Option<PgBox<'mcx, PlanStateNode<'mcx>>>,
    /// `List *initPlan` — `SubPlanState` nodes for my init-plans (un-correlated
    /// expression subselects). `None` is the C `NIL`.
    pub initPlan: Option<PgVec<'mcx, SubPlanState<'mcx>>>,
    /// `List *subPlan` — `SubPlanState` nodes in my expressions. `None` is the
    /// C `NIL`.
    pub subPlan: Option<PgVec<'mcx, SubPlanState<'mcx>>>,
    /// `Bitmapset *chgParam` — set of IDs of changed Params.
    pub chgParam: Option<PgBox<'mcx, Bitmapset<'mcx>>>,
    /// `ExprContext *ps_ExprContext` — node's expression-evaluation context.
    pub ps_ExprContext: Option<PgBox<'mcx, ExprContext>>,
    /// `TupleTableSlot *ps_ResultTupleSlot` — slot for my result tuples (id
    /// into `es_tupleTable`).
    pub ps_ResultTupleSlot: Option<SlotId>,
    /// `ProjectionInfo *ps_ProjInfo` — info for doing tuple projection.
    pub ps_ProjInfo: Option<PgBox<'mcx, ProjectionInfo>>,
}

/// `ScanState` head (execnodes.h), trimmed.
#[derive(Debug, Default)]
pub struct ScanStateData<'mcx> {
    /// `PlanState ps` — its first field is `NodeTag`.
    pub ps: PlanStateData<'mcx>,
    /// `TupleTableSlot *ss_ScanTupleSlot` — id into `es_tupleTable`.
    pub ss_ScanTupleSlot: Option<SlotId>,
}

/// `EState` (execnodes.h) — working storage for one Executor invocation,
/// trimmed to the fields ports consume.
#[derive(Debug)]
pub struct EStateData<'mcx> {
    /// `ScanDirection es_direction` — current scan direction.
    pub es_direction: ScanDirection,
    /// `MemoryContext es_query_cxt` — the per-query context the executor
    /// allocates in (C: the context `CreateExecutorState` made the `EState`
    /// in, current while nodes init and run).
    pub es_query_cxt: Mcx<'mcx>,
    /// `List *es_tupleTable` — the executor slot pool. Slots are addressed by
    /// [`SlotId`] (the owned-model `TupleTableSlot *`).
    pub es_tupleTable: PgVec<'mcx, TupleTableSlot>,
}

impl<'mcx> EStateData<'mcx> {
    /// `CreateExecutorState()`-shaped construction: an empty executor state
    /// whose allocations live in (and are accounted to) `mcx`.
    pub fn new_in(mcx: Mcx<'mcx>) -> Self {
        EStateData {
            es_direction: ForwardScanDirection,
            es_query_cxt: mcx,
            es_tupleTable: PgVec::new_in(mcx),
        }
    }

    /// `ExecAllocTableSlot` — append a slot to the per-query pool
    /// (`es_tupleTable`) and return its id (C: the pointer). Fallible: the
    /// pool grows by `palloc` (OOM is `ereport(ERROR)` in C).
    pub fn make_slot(&mut self, slot: TupleTableSlot) -> PgResult<SlotId> {
        let mcx = *self.es_tupleTable.allocator();
        self.es_tupleTable
            .try_reserve(1)
            .map_err(|_| mcx.oom(core::mem::size_of::<TupleTableSlot>()))?;
        let id = SlotId(self.es_tupleTable.len() as u32);
        self.es_tupleTable.push(slot);
        Ok(id)
    }

    /// Resolve a slot id to the live slot (C: dereference the pointer).
    pub fn slot(&self, id: SlotId) -> &TupleTableSlot {
        &self.es_tupleTable[id.0 as usize]
    }

    /// Resolve a slot id mutably (C: dereference the pointer).
    pub fn slot_mut(&mut self, id: SlotId) -> &mut TupleTableSlot {
        &mut self.es_tupleTable[id.0 as usize]
    }

    /// Two DISTINCT slots mutably at once (e.g. copy one slot's tuple into
    /// another). Panics if `a == b` — the slots play distinct roles by
    /// construction in the C executor too.
    pub fn slot_pair_mut(
        &mut self,
        a: SlotId,
        b: SlotId,
    ) -> (&mut TupleTableSlot, &mut TupleTableSlot) {
        assert_ne!(a, b, "slot_pair_mut: the two slots must be distinct");
        let (ai, bi) = (a.0 as usize, b.0 as usize);
        if ai < bi {
            let (lo, hi) = self.es_tupleTable.split_at_mut(bi);
            (&mut lo[ai], &mut hi[0])
        } else {
            let (lo, hi) = self.es_tupleTable.split_at_mut(ai);
            (&mut hi[0], &mut lo[bi])
        }
    }
}
