//! Executor node vocabulary (executor/execnodes.h plus the `sdir.h` scan
//! direction), trimmed.
//!
//! In the owned-tree model each `<Node>StateData` layout carries its fields as
//! owned children (`Option<Box<T>>` for a single nullable pointee, `Vec<T>`
//! for a counted array). `TupleTableSlot *` fields are [`SlotId`] indexes into
//! the owning [`EStateData::es_tupleTable`] slot pool, exactly as C's slot
//! pointers point into the `es_tupleTable`-owned objects. The C
//! `PlanState.state` back-pointer to the `EState` is not carried: the owned
//! model threads `&mut EStateData` explicitly through the executor entry
//! points instead.

use alloc::boxed::Box;
use alloc::vec::Vec;

use crate::bitmapset::Bitmapset;
use crate::execexpr::ProjectionInfo;
use crate::executor::TupleTableSlot;
use crate::planstate::PlanStateNode;

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
pub type ExecProcNodeMtd = Option<
    fn(pstate: &mut PlanStateNode, estate: &mut EStateData) -> types_core::PgResult<Option<SlotId>>,
>;

/// `PlanState` head (execnodes.h), trimmed to the fields ports consume.
#[derive(Debug, Default)]
pub struct PlanStateData {
    /// `Plan *plan` — associated plan node.
    pub plan: Option<Box<crate::nodes::Node>>,
    /// `ExecProcNodeMtd ExecProcNode` — function to return next tuple.
    pub ExecProcNode: ExecProcNodeMtd,
    /// `struct PlanState *lefttree` — input plan tree (`outerPlanState`).
    pub lefttree: Option<Box<PlanStateNode>>,
    /// `Bitmapset *chgParam` — set of IDs of changed Params.
    pub chgParam: Option<Box<Bitmapset>>,
    /// `TupleTableSlot *ps_ResultTupleSlot` — slot for my result tuples (id
    /// into `es_tupleTable`).
    pub ps_ResultTupleSlot: Option<SlotId>,
    /// `ProjectionInfo *ps_ProjInfo` — info for doing tuple projection.
    pub ps_ProjInfo: Option<Box<ProjectionInfo>>,
}

/// `ScanState` head (execnodes.h), trimmed.
#[derive(Debug, Default)]
pub struct ScanStateData {
    /// `PlanState ps` — its first field is `NodeTag`.
    pub ps: PlanStateData,
    /// `TupleTableSlot *ss_ScanTupleSlot` — id into `es_tupleTable`.
    pub ss_ScanTupleSlot: Option<SlotId>,
}

/// `EState` (execnodes.h) — working storage for one Executor invocation,
/// trimmed to the fields ports consume.
#[derive(Debug, Default)]
pub struct EStateData {
    /// `ScanDirection es_direction` — current scan direction.
    pub es_direction: ScanDirection,
    /// `List *es_tupleTable` — the executor slot pool. Slots are addressed by
    /// [`SlotId`] (the owned-model `TupleTableSlot *`).
    pub es_tupleTable: Vec<TupleTableSlot>,
}

impl EStateData {
    /// `ExecAllocTableSlot` — append a slot to the per-query pool
    /// (`es_tupleTable`) and return its id (C: the pointer).
    pub fn make_slot(&mut self, slot: TupleTableSlot) -> SlotId {
        let id = SlotId(self.es_tupleTable.len() as u32);
        self.es_tupleTable.push(slot);
        id
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
