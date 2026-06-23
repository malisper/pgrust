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
use ::types_core::primitive::{Index, Oid};
use ::mcx::Mcx;
use ::types_error::PgResult;

impl<'mcx> PartitionPruneInfo<'mcx> {
    /// Deep copy into `mcx`, recursing through the pruning-step expressions so the
    /// result is tied to the mcx lifetime `'b` (the `Expr`-`'mcx` campaign).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<PartitionPruneInfo<'b>> {
        let mut prune_infos = Vec::with_capacity(self.prune_infos.len());
        for inner in self.prune_infos.iter() {
            let mut v = Vec::with_capacity(inner.len());
            for pri in inner.iter() {
                v.push(pri.clone_in(mcx)?);
            }
            prune_infos.push(v);
        }
        Ok(PartitionPruneInfo {
            relids: self.relids.clone(),
            prune_infos,
            other_subplans: self.other_subplans.clone(),
        })
    }
}

impl<'mcx> PartitionedRelPruneInfo<'mcx> {
    /// Deep copy into `mcx` (recurses through pruning steps).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<PartitionedRelPruneInfo<'b>> {
        let mut initial_pruning_steps = Vec::with_capacity(self.initial_pruning_steps.len());
        for s in self.initial_pruning_steps.iter() {
            initial_pruning_steps.push(s.clone_in(mcx)?);
        }
        let mut exec_pruning_steps = Vec::with_capacity(self.exec_pruning_steps.len());
        for s in self.exec_pruning_steps.iter() {
            exec_pruning_steps.push(s.clone_in(mcx)?);
        }
        Ok(PartitionedRelPruneInfo {
            rtindex: self.rtindex,
            present_parts: self.present_parts.clone(),
            nparts: self.nparts,
            subplan_map: self.subplan_map.clone(),
            subpart_map: self.subpart_map.clone(),
            leafpart_rti_map: self.leafpart_rti_map.clone(),
            relid_map: self.relid_map.clone(),
            initial_pruning_steps,
            exec_pruning_steps,
            execparamids: self.execparamids.clone(),
        })
    }
}

impl<'mcx> PartitionPruneStep<'mcx> {
    /// Deep copy into `mcx`.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<PartitionPruneStep<'b>> {
        Ok(match self {
            PartitionPruneStep::Op(op) => PartitionPruneStep::Op(op.clone_in(mcx)?),
            PartitionPruneStep::Combine(c) => PartitionPruneStep::Combine(c.clone()),
        })
    }
}

impl<'mcx> PartitionPruneStepOp<'mcx> {
    /// Deep copy into `mcx`, deep-cloning each lookup-key `Expr`.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<PartitionPruneStepOp<'b>> {
        let mut exprs = Vec::with_capacity(self.exprs.len());
        for e in self.exprs.iter() {
            exprs.push(e.clone_in(mcx)?);
        }
        Ok(PartitionPruneStepOp {
            step_id: self.step_id,
            opstrategy: self.opstrategy,
            exprs,
            cmpfns: self.cmpfns.clone(),
            nullkeys: self.nullkeys.clone(),
        })
    }
}

/// A `Bitmapset *` carried as raw `bitmapword[]` plan data; `None` is the C
/// NULL set.
pub type RawBms = Option<Vec<bitmapword>>;

/// `PartitionPruneInfo` (nodes/plannodes.h). Built by `make_partition_pruneinfo`
/// and appended to the planner's `partPruneInfos`; the executor reads it from
/// `EState.es_part_prune_infos`.
#[derive(Clone, Debug)]
pub struct PartitionPruneInfo<'mcx> {
    /// `Bitmapset *relids` — relids of the Append/MergeAppend node.
    pub relids: RawBms,
    /// `List *prune_infos` — list of lists of `PartitionedRelPruneInfo` (one
    /// inner list per partition hierarchy).
    pub prune_infos: Vec<Vec<PartitionedRelPruneInfo<'mcx>>>,
    /// `Bitmapset *other_subplans` — subplans not covered by any prune_info.
    pub other_subplans: RawBms,
}

/// Erase a [`PartitionPruneInfo`]'s lifetime to the planner arena's notional
/// `'static` (sibling of `primnodes::placeholdervar_into_static`). The contained
/// pruning-step `Expr`s are fully owned (moved in); this is a lifetime-parameter-
/// only erase, used when the planner appends the pruneinfo into its backend-
/// lifetime `partPruneInfos` list (which, not Rust's borrow tracker, governs its
/// validity).
pub fn partpruneinfo_into_static(p: PartitionPruneInfo<'_>) -> PartitionPruneInfo<'static> {
    // SAFETY: `p`'s children are fully owned; lifetime-parameter-only erase to the
    // Expr tree's 'static notional lifetime (cf. placeholdervar_into_static).
    unsafe { core::mem::transmute(p) }
}

/// `PartitionedRelPruneInfo` (nodes/plannodes.h) — pruning info for one
/// partitioned table within a hierarchy.
#[derive(Clone, Debug)]
pub struct PartitionedRelPruneInfo<'mcx> {
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
    pub initial_pruning_steps: Vec<PartitionPruneStep<'mcx>>,
    /// `List *exec_pruning_steps` — per-scan pruning steps (NIL if none).
    pub exec_pruning_steps: Vec<PartitionPruneStep<'mcx>>,
    /// `Bitmapset *execparamids` — all PARAM_EXEC Param IDs in
    /// `exec_pruning_steps`.
    pub execparamids: RawBms,
}

/// `PartitionPruneStep` (nodes/plannodes.h) — abstract base with two concrete
/// variants. `step_id` is the base field; each variant carries it here.
#[derive(Clone, Debug)]
pub enum PartitionPruneStep<'mcx> {
    /// `PartitionPruneStepOp`.
    Op(PartitionPruneStepOp<'mcx>),
    /// `PartitionPruneStepCombine`.
    Combine(PartitionPruneStepCombine),
}

impl<'mcx> PartitionPruneStep<'mcx> {
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
pub struct PartitionPruneStepOp<'mcx> {
    /// `step.step_id`.
    pub step_id: i32,
    /// `StrategyNumber opstrategy` — strategy of the operator matched to the
    /// last partition key.
    pub opstrategy: i32,
    /// `List *exprs` — lookup-key expressions (up to partnatts items).
    pub exprs: Vec<Expr<'mcx>>,
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
