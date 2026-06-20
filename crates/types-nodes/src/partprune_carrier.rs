//! `PartitionPruneInfo` / `PartitionedRelPruneInfo` / `PartitionPruneStep`
//! family (`nodes/plannodes.h`) — the plan-data carriers the planner
//! (`partprune.c` `make_partition_pruneinfo`) produces and the executor
//! (`execPartition.c`) consumes.
//!
//! In the owned model these are plain `'static` plan-data structs (every field
//! is owned by value — no arena lifetime), so they can be:
//!   * appended to `PlannerInfo.partPruneInfos` / `PlannerGlobal.part_prune_infos`
//!     and carried on the owned `PlannedStmt.partPruneInfos`, and
//!   * stored as the type-erased payload of an `EState.es_part_prune_infos`
//!     `Opaque` (`Box<dyn Any + 'static>`) and the
//!     `PartitionedRelPruningData.{initial,exec}_pruning_steps` `Opaque`.
//!
//! Carried unchanged through `set_plan_references`'
//! `register_partpruneinfo`, which only mutates the RT-index/relid fields in
//! place. The `Bitmapset *` fields are carried as raw `bitmapword[]` (see
//! [`RawBms`]) so the struct satisfies `dyn Any`'s `'static` bound; readers wrap
//! the words into a transient `Bitmapset` to call the `bms_*` owner seams.

extern crate alloc;

use alloc::vec::Vec;

use crate::bitmapset::bitmapword;
use crate::primnodes::Expr;
use types_core::primitive::{Index, Oid};

/// A `Bitmapset *` carried as raw `bitmapword[]` plan data; `None` is the C
/// NULL set.
pub type RawBms = Option<Vec<bitmapword>>;

/// `PartitionPruneInfo` (nodes/plannodes.h). Built by `make_partition_pruneinfo`
/// and appended to the planner's `partPruneInfos`; the executor reads it from
/// `EState.es_part_prune_infos`.
#[derive(Clone, Debug)]
pub struct PartitionPruneInfo {
    /// `Bitmapset *relids` — relids of the Append/MergeAppend node.
    pub relids: RawBms,
    /// `List *prune_infos` — list of lists of `PartitionedRelPruneInfo` (one
    /// inner list per partition hierarchy).
    pub prune_infos: Vec<Vec<PartitionedRelPruneInfo>>,
    /// `Bitmapset *other_subplans` — subplans not covered by any prune_info.
    pub other_subplans: RawBms,
}

/// `PartitionedRelPruneInfo` (nodes/plannodes.h) — pruning info for one
/// partitioned table within a hierarchy.
#[derive(Clone, Debug)]
pub struct PartitionedRelPruneInfo {
    /// `Index rtindex` — RT index of partition rel for this level.
    pub rtindex: Index,
    /// `Bitmapset *present_parts` — partition indexes with subplans/subparts.
    pub present_parts: RawBms,
    /// `int nparts` — length of the following arrays.
    pub nparts: i32,
    /// `int *subplan_map` — subplan index by partition index, or -1.
    pub subplan_map: Vec<i32>,
    /// `int *subpart_map` — subpart index by partition index, or -1.
    pub subpart_map: Vec<i32>,
    /// `int *leafpart_rti_map` — RT index by partition index, or 0.
    pub leafpart_rti_map: Vec<i32>,
    /// `Oid *relid_map` — relation OID by partition index, or 0.
    pub relid_map: Vec<Oid>,
    /// `List *initial_pruning_steps` — startup pruning steps (NIL if none).
    pub initial_pruning_steps: Vec<PartitionPruneStep>,
    /// `List *exec_pruning_steps` — per-scan pruning steps (NIL if none).
    pub exec_pruning_steps: Vec<PartitionPruneStep>,
    /// `Bitmapset *execparamids` — all PARAM_EXEC Param IDs in
    /// `exec_pruning_steps`.
    pub execparamids: RawBms,
}

/// `PartitionPruneStep` (nodes/plannodes.h) — abstract base with two concrete
/// variants. `step_id` is the base field; each variant carries it here.
#[derive(Clone, Debug)]
pub enum PartitionPruneStep {
    /// `PartitionPruneStepOp`.
    Op(PartitionPruneStepOp),
    /// `PartitionPruneStepCombine`.
    Combine(PartitionPruneStepCombine),
}

impl PartitionPruneStep {
    /// `step->step.step_id`.
    pub fn step_id(&self) -> i32 {
        match self {
            PartitionPruneStep::Op(op) => op.step_id,
            PartitionPruneStep::Combine(c) => c.step_id,
        }
    }
}

/// `PartitionPruneCombineOp` (nodes/plannodes.h).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PartitionPruneCombineOp {
    /// `PARTPRUNE_COMBINE_UNION`.
    Union,
    /// `PARTPRUNE_COMBINE_INTERSECT`.
    Intersect,
}

/// `PartitionPruneStepOp` (nodes/plannodes.h) — prune using a set of mutually
/// ANDed OpExpr clauses. Carries every field both the planner builder writes and
/// the runtime kernel (`get_matching_partitions`) reads.
#[derive(Clone, Debug)]
pub struct PartitionPruneStepOp {
    /// `step.step_id`.
    pub step_id: i32,
    /// `StrategyNumber opstrategy` — strategy of the operator matched to the
    /// last partition key.
    pub opstrategy: i32,
    /// `List *exprs` — lookup-key expressions (up to partnatts items).
    pub exprs: Vec<Expr>,
    /// `List *cmpfns` — comparison-function OIDs, parallel to `exprs`.
    pub cmpfns: Vec<Oid>,
    /// `Bitmapset *nullkeys` — partition-key offsets matched to IS NULL
    /// (`None`/empty is the C NULL set).
    pub nullkeys: RawBms,
}

/// `PartitionPruneStepCombine` (nodes/plannodes.h) — combine the partition sets
/// of several argument steps via a BoolExpr.
#[derive(Clone, Debug)]
pub struct PartitionPruneStepCombine {
    /// `step.step_id`.
    pub step_id: i32,
    /// `PartitionPruneCombineOp combineOp`.
    pub combine_op: PartitionPruneCombineOp,
    /// `List *source_stepids` — step ids whose results are combined.
    pub source_stepids: Vec<i32>,
}
