//! MergeAppend node vocabulary (`nodes/plannodes.h` `MergeAppend`,
//! `executor/execnodes.h` `MergeAppendState`), the node-local binary heap
//! (`lib/binaryheap.h`, specialized to the merge node's slot-index entries),
//! and the partition-pruning state head (`executor/execPartition.h`
//! `PartitionPruneState`, trimmed to the fields the MergeAppend node reads).
//!
//! The embedded `PlanState` head reuses [`PlanStateData`], the leading `Plan`
//! base reuses [`crate::nodeindexscan::Plan`], the sort-support array reuses
//! [`types_sortsupport::SortSupportData`], and the executor-pool aliases follow
//! the owned model ([`SlotId`] for `TupleTableSlot *`).

use alloc::vec::Vec;

use mcx::{Mcx, PgBox, PgVec};
use types_core::primitive::{AttrNumber, Oid};
use types_tuple::backend_access_common_heaptuple::Datum;
use types_error::PgResult;
use types_sortsupport::SortSupportData;

use crate::bitmapset::Bitmapset;
use crate::execnodes::{PlanStateData, SlotId};
use crate::nodeindexscan::Plan;
use crate::nodes::NodeTag;

/// `T_MergeAppend` (nodes/nodetags.h) — the plan-node tag for a MergeAppend.
pub const T_MergeAppend: NodeTag = NodeTag(335);
/// `T_MergeAppendState` (nodes/nodetags.h) — the executor-state node tag.
/// Value verified against the PostgreSQL 18.3 generated `nodetags.h`
/// (`T_MergeAppendState = 398`).
pub const T_MergeAppendState: NodeTag = NodeTag(398);

/// `MergeAppend` plan node (plannodes.h):
///
/// ```c
/// typedef struct MergeAppend
/// {
///     Plan        plan;
///     Bitmapset  *apprelids;
///     List       *mergeplans;
///     int         numCols;
///     AttrNumber *sortColIdx;
///     Oid        *sortOperators;
///     Oid        *collations;
///     bool       *nullsFirst;
///     int         part_prune_index;
/// } MergeAppend;
/// ```
#[derive(Debug, Default)]
pub struct MergeAppend<'mcx> {
    /// `Plan plan` — its first field (a `NodeTag`) makes this a `Node`.
    pub plan: Plan<'mcx>,
    /// `Bitmapset *apprelids` — RT indices of appendrel(s) formed by this node.
    pub apprelids: Option<PgBox<'mcx, Bitmapset<'mcx>>>,
    /// `List *mergeplans` — the child `Plan` nodes whose sorted outputs merge.
    pub mergeplans: Vec<crate::nodes::Node<'mcx>>,
    /// `int numCols` — number of sort-key columns.
    pub numCols: i32,
    /// `AttrNumber *sortColIdx` — their indices in the target list.
    pub sortColIdx: Vec<AttrNumber>,
    /// `Oid *sortOperators` — OIDs of the operators to sort them by.
    pub sortOperators: Vec<Oid>,
    /// `Oid *collations` — OIDs of the collations.
    pub collations: Vec<Oid>,
    /// `bool *nullsFirst` — NULLS FIRST/LAST directions.
    pub nullsFirst: Vec<bool>,
    /// `int part_prune_index` — index into `PlannedStmt.partPruneInfos`, or -1
    /// if run-time partition pruning is not in use.
    pub part_prune_index: i32,
}

impl MergeAppend<'_> {
    /// Deep copy into `mcx` (C: `copyObject` shape). Fallible: copying the
    /// embedded plan subtree and the dense arrays allocates.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<MergeAppend<'b>> {
        let mut mergeplans = Vec::with_capacity(self.mergeplans.len());
        for child in self.mergeplans.iter() {
            mergeplans.push(child.clone_in(mcx)?);
        }
        Ok(MergeAppend {
            plan: self.plan.clone_in(mcx)?,
            apprelids: match &self.apprelids {
                Some(b) => Some(mcx::alloc_in(mcx, b.clone_in(mcx)?)?),
                None => None,
            },
            mergeplans,
            numCols: self.numCols,
            sortColIdx: self.sortColIdx.clone(),
            sortOperators: self.sortOperators.clone(),
            collations: self.collations.clone(),
            nullsFirst: self.nullsFirst.clone(),
            part_prune_index: self.part_prune_index,
        })
    }
}

/// `PartitionPruneState` (executor/execPartition.h) — the MergeAppend node
/// embeds the full run-time partition-pruning state defined in
/// [`crate::partition::PartitionPruneState`]; re-exported here so the
/// `MergeAppendState` field type stays unqualified.
pub use crate::partition::PartitionPruneState;

/// `binaryheap` (lib/binaryheap.h), specialized to the MergeAppend node's
/// slot-index entries.
///
/// ```c
/// typedef struct binaryheap
/// {
///     int         bh_space;
///     int         bh_size;
///     bool        bh_has_heap_property;
///     binaryheap_comparator bh_compare;
///     void       *bh_arg;
///     bh_node_type bh_nodes[FLEXIBLE_ARRAY_MEMBER];
/// } binaryheap;
/// ```
///
/// The comparator (`heap_compare_slots`) and its `arg` (`MergeAppendState *`)
/// are node-local in the owned model — the heap operations are driven by the
/// owning node crate with the comparator threaded in — so `bh_compare`/`bh_arg`
/// are not carried. The node store is the merge node's working scratch
/// (`binaryheap`'s `palloc`'d slot array), context-allocated, so it carries the
/// allocator lifetime.
#[derive(Debug)]
pub struct BinaryHeap<'mcx> {
    /// `int bh_space` — how many nodes can be stored.
    pub bh_space: i32,
    /// `int bh_size` — current number of valid nodes.
    pub bh_size: i32,
    /// `bool bh_has_heap_property` — debugging cross-check: true while the heap
    /// property holds (false between `add_unordered` and `build`).
    pub bh_has_heap_property: bool,
    /// `bh_node_type bh_nodes[]` — the slot-index entries (`Datum`s holding
    /// `int32` slot numbers).
    pub bh_nodes: PgVec<'mcx, Datum<'mcx>>,
}

impl<'mcx> BinaryHeap<'mcx> {
    /// An empty heap able to hold `capacity` entries, with its backing store
    /// reserved in `mcx` (C: `binaryheap_allocate` `palloc`s the struct with a
    /// trailing `bh_nodes[capacity]`). Fallible: the reservation allocates.
    pub fn allocate(mcx: Mcx<'mcx>, capacity: usize) -> PgResult<Self> {
        Ok(BinaryHeap {
            bh_space: capacity as i32,
            bh_size: 0,
            bh_has_heap_property: true,
            bh_nodes: mcx::vec_with_capacity_in(mcx, capacity)?,
        })
    }
}

/// `MergeAppendState` (execnodes.h) — the per-node execution state of a
/// MergeAppend.
///
/// ```c
/// typedef struct MergeAppendState
/// {
///     PlanState   ps;
///     PlanState **mergeplans;
///     int         ms_nplans;
///     int         ms_nkeys;
///     SortSupport ms_sortkeys;
///     TupleTableSlot **ms_slots;
///     struct binaryheap *ms_heap;
///     bool        ms_initialized;
///     struct PartitionPruneState *ms_prune_state;
///     Bitmapset  *ms_valid_subplans;
/// } MergeAppendState;
/// ```
#[derive(Debug)]
pub struct MergeAppendStateData<'mcx> {
    /// `PlanState ps` — its first field is `NodeTag`.
    pub ps: PlanStateData<'mcx>,
    /// `PlanState **mergeplans` — array of child plan-state nodes (length
    /// `ms_nplans`). `None` slots never occur post-init; the option mirrors the
    /// generic `Node` child slots elsewhere.
    pub mergeplans: PgVec<'mcx, Option<PgBox<'mcx, crate::planstate::PlanStateNode<'mcx>>>>,
    /// `int ms_nplans` — number of subplans actually initialized.
    pub ms_nplans: i32,
    /// `int ms_nkeys` — number of sort-key columns.
    pub ms_nkeys: i32,
    /// `SortSupport ms_sortkeys` — array of length `ms_nkeys`.
    pub ms_sortkeys: PgVec<'mcx, SortSupportData<'mcx>>,
    /// `TupleTableSlot **ms_slots` — the current head slot of each subplan (ids
    /// into `es_tupleTable`). `None` = the C `NULL` (no/exhausted tuple).
    pub ms_slots: PgVec<'mcx, Option<SlotId>>,
    /// `struct binaryheap *ms_heap` — heap of subplan indices keyed on sort
    /// columns.
    pub ms_heap: Option<PgBox<'mcx, BinaryHeap<'mcx>>>,
    /// `bool ms_initialized` — are subplans started?
    pub ms_initialized: bool,
    /// `struct PartitionPruneState *ms_prune_state` — run-time pruning state, or
    /// `None` when pruning is not in use.
    pub ms_prune_state: Option<PgBox<'mcx, PartitionPruneState<'mcx>>>,
    /// `Bitmapset *ms_valid_subplans` — the set of subplans that survived
    /// pruning, or `None` until determined.
    pub ms_valid_subplans: Option<PgBox<'mcx, Bitmapset<'mcx>>>,
}

impl<'mcx> MergeAppendStateData<'mcx> {
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
