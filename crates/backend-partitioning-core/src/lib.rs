//! `partitioning/partprune.c` — plan-time partition pruning.
//!
//! This crate ports the planner-facing entry point `prune_append_rel_partitions`
//! and the machinery it drives: pruning-step generation
//! (`gen_partprune_steps` → `gen_partprune_steps_internal` →
//! `match_clause_to_partition_key` → `gen_prune_steps_from_opexps` /
//! `get_steps_using_prefix*`) and the shared pruning kernel
//! (`get_matching_partitions` → `perform_pruning_{base,combine}_step` →
//! `get_matching_{hash,list,range}_bounds`).
//!
//! Scope (this lane): the PARTTARGET_PLANNER path, which compares the partition
//! key against `Const` quals only. That is what static query pruning needs and
//! is fully self-contained — the comparison values are plain constants, so the
//! `partkey_datum_from_expr` executor leg (ExprState evaluation) is never
//! reached and the cross-seam run-time `get_matching_partitions` entry is not
//! needed. The pruning steps are a plain owned Rust enum local to this crate
//! (they are plan-data only, never copyObject/equal/out-walked, so no Node
//! registration is required).
//!
//! Run-time pruning planner leg (this lane): `make_partition_pruneinfo` /
//! `make_partitionedrel_pruneinfo` build the `PartitionPruneInfo` plan-data
//! carrier (`types_nodes::partprune_carrier`) for Append/MergeAppend, generating
//! INITIAL/EXEC pruning steps (`gen_partprune_steps` now honors all three
//! `PartClauseTarget` values, with `pull_exec_paramids` /
//! `get_partkey_exec_paramids`). The carrier is appended to
//! `root.partPruneInfos`; `set_plan_references`' `register_partpruneinfo` moves
//! it onto `glob.part_prune_infos` and the `PlannedStmt`.
//!
//! Deferred to a follow-on lane: the executor-side run-time kernel evaluation
//! (`get_matching_partitions` seam body over a live `ExprContext`, the
//! `partkey_datum_from_expr` `ExprState` leg) — i.e. actually *executing* the
//! exec/initial steps at scan time.

extern crate alloc;

use alloc::boxed::Box;
use alloc::vec::Vec;

use mcx::Mcx;
use types_core::primitive::Oid;
use types_error::{PgError, PgResult};
use types_nodes::partition::{PartitionBoundInfoData, PartitionKeyData, PartitionRangeDatumKind};
use types_nodes::primnodes::{
    BoolExpr, BoolExprType, BoolTestType, Const, Expr, NullTest, NullTestType, ScalarArrayOpExpr,
};
use types_pathnodes::planner_run::PlannerRun;
use types_pathnodes::{PlannerInfo, RelId, Relids};
use types_tuple::backend_access_common_heaptuple::Datum;

use backend_partitioning_partprune_seams as partprune_seams;

// --- catalog / strategy constants (mirrored from the C headers) ----------------

use types_core::catalog::{BOOLOID, BOOL_BTREE_FAM_OID, BOOL_HASH_FAM_OID};
use types_hash::hash::HASHEXTENDED_PROC;
use types_partition::{
    BTORDER_PROC, PARTITION_STRATEGY_HASH, PARTITION_STRATEGY_LIST, PARTITION_STRATEGY_RANGE,
};
use types_scan::scankey::{
    BTEqualStrategyNumber, BTGreaterEqualStrategyNumber, BTGreaterStrategyNumber,
    BTLessEqualStrategyNumber, BTLessStrategyNumber, InvalidStrategy,
};

/// `BTMaxStrategyNumber` (`access/stratnum.h`).
const BT_MAX_STRATEGY_NUMBER: i32 = 5;
/// `HTEqualStrategyNumber` (`access/stratnum.h`).
const HT_EQUAL_STRATEGY_NUMBER: i32 = 1;
/// `BooleanEqualOperator` (pg_operator.dat: `=` for `bool`, OID 91).
const BOOLEAN_EQUAL_OPERATOR: Oid = 91;
/// `PROVOLATILE_IMMUTABLE` (`catalog/pg_proc.h`).
const PROVOLATILE_IMMUTABLE: u8 = b'i';

/// `IsBuiltinBooleanOpfamily(opfamily)` (`catalog/pg_opfamily.h` macro).
#[inline]
fn is_builtin_boolean_opfamily(opfamily: Oid) -> bool {
    opfamily == BOOL_BTREE_FAM_OID || opfamily == BOOL_HASH_FAM_OID
}

#[inline]
fn oid_is_valid(oid: Oid) -> bool {
    oid != 0
}

/// `PartCollMatchesExprColl(partcoll, exprcoll)` (partprune.c macro).
#[inline]
fn part_coll_matches_expr_coll(partcoll: Oid, exprcoll: Oid) -> bool {
    partcoll == 0 || partcoll == exprcoll
}

// =============================================================================
// Local plan-data types (PartClauseInfo / PartClauseMatchStatus / pruning steps)
// =============================================================================

/// `PartClauseInfo` (partprune.c): a clause matched with a partition key.
#[derive(Clone, Debug)]
struct PartClauseInfo<'mcx> {
    /// Partition key number (0 to partnatts - 1).
    keyno: i32,
    /// Operator used to compare partkey to expr.
    opno: Oid,
    /// Is the clause's original operator `<>`?
    op_is_ne: bool,
    /// The expr the partition key is compared to.
    expr: Expr<'mcx>,
    /// Oid of the function to compare `expr` to the partition key.
    cmpfn: Oid,
    /// btree strategy identifying the operator.
    op_strategy: i32,
}

/// `PartClauseMatchStatus` (partprune.c): result of
/// `match_clause_to_partition_key`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PartClauseMatchStatus {
    NoMatch,
    MatchClause,
    MatchNullness,
    MatchSteps,
    MatchContradict,
    Unsupported,
}

/// `PartClauseTarget` (partprune.c) — what kind of pruning steps to generate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PartClauseTarget {
    /// `PARTTARGET_PLANNER` — prune during planning (immutable clauses only).
    Planner,
    /// `PARTTARGET_INITIAL` — executor startup pruning (any allowable clause
    /// except ones containing PARAM_EXEC Params).
    Initial,
    /// `PARTTARGET_EXEC` — executor per-scan pruning (any allowable clause).
    Exec,
}

/// `PartitionPruneCombineOp` (plannodes.h).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PartitionPruneCombineOp {
    Union,
    Intersect,
}

/// `PartitionPruneStepOp` (plannodes.h), trimmed to plan-time pruning fields.
#[derive(Clone, Debug)]
struct PruneStepOp<'mcx> {
    step_id: i32,
    opstrategy: i32,
    /// Lookup-key expressions (up to partnatts items), parallel to `cmpfns`.
    exprs: Vec<Expr<'mcx>>,
    /// Comparison/hash support function OIDs, parallel to `exprs`.
    cmpfns: Vec<Oid>,
    /// Partition-key offsets matched to IS NULL (empty == C NULL set).
    nullkeys: Vec<i32>,
}

/// `PartitionPruneStepCombine` (plannodes.h).
#[derive(Clone, Debug)]
struct PruneStepCombine {
    step_id: i32,
    combine_op: PartitionPruneCombineOp,
    source_stepids: Vec<i32>,
}

/// `PartitionPruneStep` (plannodes.h) — base with the two concrete variants.
#[derive(Clone, Debug)]
enum PartitionPruneStep<'mcx> {
    Op(PruneStepOp<'mcx>),
    Combine(PruneStepCombine),
}

impl<'mcx> PartitionPruneStep<'mcx> {
    fn step_id(&self) -> i32 {
        match self {
            PartitionPruneStep::Op(s) => s.step_id,
            PartitionPruneStep::Combine(s) => s.step_id,
        }
    }
}

/// `PruneStepResult` (partprune.c): the result of performing one pruning step.
#[derive(Clone, Debug, Default)]
struct PruneStepResult {
    /// Offsets of bounds (in a table's boundinfo) selected by the step.
    bound_offsets: Bitmapset,
    scan_default: bool,
    scan_null: bool,
}

/// `GeneratePruningStepsContext` (partprune.c).
///
/// `target` / `has_mutable_arg` / `has_exec_param` are the run-time-target
/// bookkeeping the C struct carries; they are written but not yet read on the
/// PLANNER-only path this lane lands (the PARTTARGET_INITIAL/EXEC reader is the
/// follow-on run-time lane).
#[allow(dead_code)]
struct GeneratePruningStepsContext<'a, 'mcx> {
    /// The arena the per-element `Const` nodes deconstructed from a constant
    /// SAOP array allocate into (`deconstruct_const_array` / `makeConst`).
    mcx: Mcx<'mcx>,
    /// The partition scheme (column metadata) of the partitioned relation.
    part_scheme: &'a PartitionScheme,
    /// The partition key expressions (`rel->partexprs[i]` — first per key).
    partexprs: &'a [Expr<'mcx>],
    /// `rel->boundinfo` presence — whether a default partition can exist.
    has_default: bool,
    /// `rel->partition_qual` (NIL for a top-level partitioned table).
    has_partition_qual: bool,
    /// `rel->partition_qual` — the partition constraint clauses (implicit-AND
    /// list of bare Exprs), used by the default-partition refutation check.
    partition_qual: &'a [Expr<'mcx>],
    target: PartClauseTarget,
    /// Result: the list of pruning steps.
    steps: Vec<PartitionPruneStep<'mcx>>,
    has_mutable_op: bool,
    has_mutable_arg: bool,
    has_exec_param: bool,
    contradictory: bool,
    next_step_id: i32,
}

/// A snapshot of the partition scheme's per-column metadata needed by step
/// generation and the kernel — the `PartitionSchemeData` fields plus the
/// partition support functions, copied out of the relcache partition key.
struct PartitionScheme {
    strategy: i8,
    partnatts: i32,
    partopfamily: Vec<Oid>,
    partopcintype: Vec<Oid>,
    partcollation: Vec<Oid>,
    /// `FmgrInfo.fn_oid` for each key's cached support function.
    partsupfunc_oid: Vec<Oid>,
}

// A tiny owned bitmapset over partition-bound offsets / partition indexes.
// The C kernel manipulates `Bitmapset *` of small non-negative integers; an
// owned `Vec<bool>`-free sorted set keeps the algorithm faithful without
// pulling in the bms seams (these sets never leave the crate).
type Bitmapset = alloc::collections::BTreeSet<i32>;

fn bms_add_range(set: &mut Bitmapset, lo: i32, hi: i32) {
    for i in lo..=hi {
        set.insert(i);
    }
}

/// `pull_exec_paramids(expr)` (partprune.c:2620) — collect the `paramid`s of all
/// `PARAM_EXEC` Params anywhere in `expr`. Faithfully walks the whole expression
/// tree via `expression_tree_walker` over a transient `Node` wrapper (same model
/// as var.c's `pull_varnos`).
fn pull_exec_paramids(expr: &Expr) -> Bitmapset {
    use backend_nodes_core::node_walker::{expression_tree_walker, node_expr_wrapper};
    use types_nodes::nodes::Node;

    let scratch = mcx::MemoryContext::new("pull_exec_paramids");
    let mut result = Bitmapset::new();
    let wrapped = node_expr_wrapper(expr, scratch.mcx());
    pull_exec_paramids_walker(&wrapped, &mut result);
    return result;

    /// `pull_exec_paramids_walker` (partprune.c:2633).
    fn pull_exec_paramids_walker(node: &Node, context: &mut Bitmapset) -> bool {
        if let Some(Expr::Param(param)) = node.as_expr() {
            if param.paramkind == types_nodes::primnodes::PARAM_EXEC {
                context.insert(param.paramid);
            }
            return false;
        }
        expression_tree_walker(node, &mut |n: &Node| pull_exec_paramids_walker(n, context))
    }
}

// =============================================================================
// make_partition_pruneinfo / make_partitionedrel_pruneinfo (partprune.c:224)
// =============================================================================

use types_nodes::partprune_carrier::{
    PartitionPruneCombineOp as CarrierCombineOp, PartitionPruneInfo as CarrierPruneInfo,
    PartitionPruneStep as CarrierStep, PartitionPruneStepCombine as CarrierStepCombine,
    PartitionPruneStepOp as CarrierStepOp, PartitionedRelPruneInfo as CarrierRelPruneInfo, RawBms,
};

use backend_optimizer_util_relnode::find_base_rel;
use backend_optimizer_util_appendinfo::adjust_appendrel_attrs_multilevel;
use backend_optimizer_util_appendinfo_seams::find_appinfos_by_relids;

/// `IS_PARTITIONED_REL(rel)` (pathnodes.h macro): `rel->part_scheme != NULL`.
#[inline]
fn rel_is_partitioned(root: &PlannerInfo, rel: RelId) -> bool {
    root.rel(rel).part_scheme.is_some()
}

/// Convert a partprune-core local pruning step to the plan-data carrier step.
fn step_to_carrier<'mcx>(step: &PartitionPruneStep<'mcx>) -> CarrierStep<'mcx> {
    match step {
        PartitionPruneStep::Op(op) => CarrierStep::Op(CarrierStepOp {
            step_id: op.step_id,
            opstrategy: op.opstrategy,
            exprs: op.exprs.clone(),
            cmpfns: op.cmpfns.clone(),
            nullkeys: ints_to_rawbms(&op.nullkeys),
        }),
        PartitionPruneStep::Combine(c) => CarrierStep::Combine(CarrierStepCombine {
            step_id: c.step_id,
            combine_op: match c.combine_op {
                PartitionPruneCombineOp::Union => CarrierCombineOp::Union,
                PartitionPruneCombineOp::Intersect => CarrierCombineOp::Intersect,
            },
            source_stepids: c.source_stepids.clone(),
        }),
    }
}

/// Pack a list of small non-negative ints into a `bitmapword[]` `RawBms`
/// (`None`/empty == the C NULL set).
fn ints_to_rawbms(ints: &[i32]) -> RawBms {
    if ints.is_empty() {
        return None;
    }
    let maxbit = *ints.iter().max().unwrap();
    let bits_per_word = (core::mem::size_of::<types_nodes::bitmapset::bitmapword>() * 8) as i32;
    let nwords = (maxbit / bits_per_word + 1) as usize;
    let mut words = alloc::vec![0 as types_nodes::bitmapset::bitmapword; nwords];
    for &i in ints {
        let w = (i / bits_per_word) as usize;
        let b = i % bits_per_word;
        words[w] |= (1 as types_nodes::bitmapset::bitmapword) << b;
    }
    Some(words)
}

/// Pack a `Bitmapset` (BTreeSet) into a `RawBms`.
fn bms_to_rawbms(set: &Bitmapset) -> RawBms {
    let v: Vec<i32> = set.iter().copied().collect();
    ints_to_rawbms(&v)
}

/// `add_part_relids(allpartrelids, partrelids)` (partprune.c:398). Add a
/// newly-found partition hierarchy's RT-index set to the appropriate member of
/// `allpartrelids`, keyed by the lowest set bit (the topmost parent).
fn add_part_relids(allpartrelids: &mut Vec<Bitmapset>, partrelids: Bitmapset) {
    // Lowest set bit = topmost parent.
    let targetpart = *partrelids.iter().next().expect("add_part_relids: empty set");
    for curr in allpartrelids.iter_mut() {
        let currtarget = *curr.iter().next().expect("empty hierarchy set");
        if targetpart == currtarget {
            for m in partrelids.iter() {
                curr.insert(*m);
            }
            return;
        }
    }
    allpartrelids.push(partrelids);
}

/// `make_partition_pruneinfo(root, parentrel, subpaths, prunequal)`
/// (partprune.c:224). Returns the 0-based index of the appended
/// `PartitionPruneInfo` in `root.partPruneInfos`, or -1 if nothing was added.
fn make_partition_pruneinfo<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    parentrel: RelId,
    subpaths: &[types_pathnodes::PathId],
    prunequal_ids: &[types_pathnodes::NodeId],
) -> PgResult<i32> {
    let mcx = run.mcx();

    // Deref the prunequal clauses to owned Exprs once. The prunequal is the
    // parent's restriction clauses, which may carry context-allocated children
    // (e.g. an AlternativeSubPlan / SubPlan inside an `... OR exists(...)`
    // qual) that have no faithful derived `.clone()`; deep-copy through
    // `Expr::clone_in` (`copyObject` shape).
    let prunequal: Vec<Expr<'mcx>> = prunequal_ids
        .iter()
        .map(|id| root.node(*id).clone_in(mcx))
        .collect::<PgResult<Vec<Expr<'mcx>>>>()?;

    let simple_rel_array_size = root.simple_rel_array_size;

    // Scan subpaths to identify partition-child scans and their parents, and
    // build relid_subplan_map (1-based; 0 = unfilled).
    let mut allpartrelids: Vec<Bitmapset> = Vec::new();
    let mut relid_subplan_map: Vec<i32> = alloc::vec![0; simple_rel_array_size as usize];

    let mut i: i32 = 1;
    for &pathid in subpaths {
        let pathrel: RelId = root.path(pathid).base().parent;
        // We don't consider partitioned joins here.
        if root.rel(pathrel).reloptkind == types_pathnodes::RELOPT_OTHER_MEMBER_REL {
            let mut prel = pathrel;
            let mut partrelids: Bitmapset = Bitmapset::new();
            // Traverse up to the topmost partitioned parent (stop at parentrel).
            loop {
                let prel_relid = root.rel(prel).relid;
                debug_assert!((prel_relid as i32) < simple_rel_array_size);
                let parent_relid = match &root.append_rel_array[prel_relid as usize] {
                    Some(appinfo) => appinfo.parent_relid,
                    None => break,
                };
                prel = find_base_rel(root, parent_relid as i32);
                if !rel_is_partitioned(root, prel) {
                    break; // reached a non-partitioned parent
                }
                partrelids.insert(root.rel(prel).relid as i32);
                if prel == parentrel {
                    break; // don't traverse above parentrel
                }
                if root.rel(prel).reloptkind != types_pathnodes::RELOPT_OTHER_MEMBER_REL {
                    break;
                }
            }

            if !partrelids.is_empty() {
                add_part_relids(&mut allpartrelids, partrelids);
                let pr = root.rel(pathrel).relid as usize;
                debug_assert!(relid_subplan_map[pr] == 0); // no duplicates
                relid_subplan_map[pr] = i;
            }
        }
        i += 1;
    }

    // Build a PartitionedRelPruneInfo list for each topmost partitioned rel.
    let mut prunerelinfos: Vec<Vec<CarrierRelPruneInfo>> = Vec::new();
    let mut allmatchedsubplans: Bitmapset = Bitmapset::new();

    for partrelids in &allpartrelids {
        let mut matchedsubplans: Bitmapset = Bitmapset::new();
        let pinfolist = make_partitionedrel_pruneinfo(
            run,
            root,
            parentrel,
            &prunequal,
            partrelids,
            &relid_subplan_map,
            &mut matchedsubplans,
            mcx,
        )?;
        if let Some(list) = pinfolist {
            prunerelinfos.push(list);
            for m in matchedsubplans.iter() {
                allmatchedsubplans.insert(*m);
            }
        }
    }

    // If no hierarchy had useful run-time pruning quals, skip run-time pruning.
    if prunerelinfos.is_empty() {
        return Ok(-1);
    }

    // Build the result PartitionPruneInfo.
    let relids = bms_to_rawbms(&relids_to_bitmapset(&root.rel(parentrel).relids));

    // Subplans not matched to any hierarchy must never be pruned.
    let other_subplans: RawBms = if (allmatchedsubplans.len() as usize) < subpaths.len() {
        let mut other: Bitmapset = Bitmapset::new();
        bms_add_range(&mut other, 0, subpaths.len() as i32 - 1);
        for m in allmatchedsubplans.iter() {
            other.remove(m);
        }
        bms_to_rawbms(&other)
    } else {
        None
    };

    let pruneinfo = CarrierPruneInfo {
        relids,
        prune_infos: prunerelinfos,
        other_subplans,
    };

    // Interned into the planner's backend-lifetime `partPruneInfos` list; erase
    // to the arena's notional 'static at this sanctioned intern boundary.
    root.partPruneInfos
        .push(types_nodes::partprune_carrier::partpruneinfo_into_static(pruneinfo));
    Ok(root.partPruneInfos.len() as i32 - 1)
}

/// `make_partitionedrel_pruneinfo(...)` (partprune.c:445). Build the list of
/// `PartitionedRelPruneInfo`s for one partition hierarchy, or `None` if no
/// useful run-time pruning steps exist.
#[allow(clippy::too_many_arguments)]
fn make_partitionedrel_pruneinfo<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    parentrel: RelId,
    prunequal: &[Expr],
    partrelids: &Bitmapset,
    relid_subplan_map: &[i32],
    matchedsubplans: &mut Bitmapset,
    mcx: Mcx<'mcx>,
) -> PgResult<Option<Vec<CarrierRelPruneInfo<'mcx>>>> {
    let simple_rel_array_size = root.simple_rel_array_size;
    let mut relid_subpart_map: Vec<i32> = alloc::vec![0; simple_rel_array_size as usize];

    // First pass: per partitioned rel, generate INITIAL/EXEC steps and discover
    // whether any run-time pruning is needed.
    struct PinfoBuild<'mcx> {
        rtindex: u32,
        initial_pruning_steps: Vec<CarrierStep<'mcx>>,
        exec_pruning_steps: Vec<CarrierStep<'mcx>>,
        execparamids: RawBms,
    }
    let mut pinfo_builds: Vec<PinfoBuild<'mcx>> = Vec::new();
    let mut doruntimeprune = false;
    let mut targetpart: Option<RelId> = None;
    // The prunequal may be translated parent->child as we descend; carry it.
    // Deep-copy through `Expr::clone_in` — a prunequal clause may hold a SubPlan
    // / AlternativeSubPlan with no faithful derived `.clone()`.
    let mut cur_prunequal: Vec<Expr<'mcx>> = prunequal
        .iter()
        .map(|e| e.clone_in(mcx))
        .collect::<PgResult<Vec<Expr<'mcx>>>>()?;

    let mut i: i32 = 1;
    let rtis: Vec<i32> = partrelids.iter().copied().collect();
    for &rti in &rtis {
        let subpart = find_base_rel(root, rti);
        debug_assert!(rti < simple_rel_array_size);
        relid_subpart_map[rti as usize] = i;
        i += 1;

        // Translate the pruning qual for this partition.
        let partprunequal: Vec<Expr<'mcx>> = match targetpart {
            None => {
                targetpart = Some(subpart);
                // The prunequal is presented for 'parentrel'. If targetpart is a
                // different rel, translate parent->target and update cur_prunequal.
                let parent_relids = relids_to_bitmapset(&root.rel(parentrel).relids);
                let sub_relids = relids_to_bitmapset(&root.rel(subpart).relids);
                if parent_relids != sub_relids {
                    let sub_rel_set = root.rel(subpart).relids.clone();
                    let appinfos = find_appinfos_by_relids::call(root, &sub_rel_set)?;
                    let mut translated: Vec<Expr<'mcx>> = Vec::with_capacity(cur_prunequal.len());
                    for cl in core::mem::take(&mut cur_prunequal) {
                        translated.push(
                            backend_optimizer_util_appendinfo::adjust_appendrel_attrs(
                                root, cl, &appinfos,
                            )?,
                        );
                    }
                    cur_prunequal = translated;
                }
                cur_prunequal
                    .iter()
                    .map(|e| e.clone_in(mcx))
                    .collect::<PgResult<Vec<Expr<'mcx>>>>()?
            }
            Some(tp) => {
                // Sub-partitioned: translate from the target down to this child.
                let mut translated: Vec<Expr<'mcx>> = Vec::with_capacity(cur_prunequal.len());
                for cl in cur_prunequal.iter() {
                    translated.push(adjust_appendrel_attrs_multilevel(
                        root,
                        cl.clone_in(mcx)?,
                        subpart,
                        tp,
                    )?);
                }
                translated
            }
        };

        // gen_partprune_steps with PARTTARGET_INITIAL.
        let inputs_initial = collect_prune_inputs_with_clauses(
            run,
            root,
            subpart,
            mcx,
            Some(
                partprunequal
                    .iter()
                    .map(|e| e.clone_in(mcx))
                    .collect::<PgResult<Vec<Expr<'mcx>>>>()?,
            ),
        )?;
        let gctx_initial = gen_partprune_steps(&inputs_initial, PartClauseTarget::Initial)?;
        if gctx_initial.contradictory {
            // Shouldn't normally happen; disable run-time pruning to be safe.
            return Ok(None);
        }

        // Startup steps only matter if there's a mutable op/arg.
        let initial_pruning_steps: Vec<CarrierStep<'mcx>> =
            if gctx_initial.has_mutable_op || gctx_initial.has_mutable_arg {
                gctx_initial.steps.iter().map(step_to_carrier).collect()
            } else {
                Vec::new()
            };

        // exec pruning only if exec Params appear.
        let mut exec_pruning_steps: Vec<CarrierStep<'mcx>> = Vec::new();
        let mut execparamids: RawBms = None;
        if gctx_initial.has_exec_param {
            let inputs_exec = collect_prune_inputs_with_clauses(
                run,
                root,
                subpart,
                mcx,
                Some(
                    partprunequal
                        .iter()
                        .map(|e| e.clone_in(mcx))
                        .collect::<PgResult<Vec<Expr<'mcx>>>>()?,
                ),
            )?;
            let gctx_exec = gen_partprune_steps(&inputs_exec, PartClauseTarget::Exec)?;
            if gctx_exec.contradictory {
                return Ok(None);
            }
            // Detect which exec Params actually got used.
            let paramids = get_partkey_exec_paramids(&gctx_exec.steps);
            if !paramids.is_empty() {
                exec_pruning_steps = gctx_exec.steps.iter().map(step_to_carrier).collect();
                execparamids = bms_to_rawbms(&paramids);
            }
        }

        if !initial_pruning_steps.is_empty() || !exec_pruning_steps.is_empty() {
            doruntimeprune = true;
        }

        pinfo_builds.push(PinfoBuild {
            rtindex: rti as u32,
            initial_pruning_steps,
            exec_pruning_steps,
            execparamids,
        });
    }

    if !doruntimeprune {
        return Ok(None);
    }

    // Second pass: build the subplan/subpart/relid/leafpart maps.
    let mut subplansfound: Bitmapset = Bitmapset::new();
    let mut result: Vec<CarrierRelPruneInfo> = Vec::with_capacity(pinfo_builds.len());

    for build in pinfo_builds {
        let subpart = find_base_rel(root, build.rtindex as i32);
        let nparts = root.rel(subpart).nparts;
        let mut subplan_map: Vec<i32> = alloc::vec![-1; nparts as usize];
        let mut subpart_map: Vec<i32> = alloc::vec![-1; nparts as usize];
        let mut relid_map: Vec<Oid> = alloc::vec![0 as Oid; nparts as usize];
        let mut leafpart_rti_map: Vec<i32> = alloc::vec![0; nparts as usize];
        let mut present_parts: Bitmapset = Bitmapset::new();

        let live_parts: Vec<i32> =
            relids_to_bitmapset(&root.rel(subpart).live_parts).iter().copied().collect();
        for p in live_parts {
            let partrel = root.rel(subpart).part_rels[p as usize]
                .expect("live part has no part_rel");
            let partrel_relid = root.rel(partrel).relid as usize;
            let subplanidx = relid_subplan_map[partrel_relid] - 1;
            let subpartidx = relid_subpart_map[partrel_relid] - 1;
            subplan_map[p as usize] = subplanidx;
            subpart_map[p as usize] = subpartidx;
            relid_map[p as usize] =
                planner_rt_fetch(run, root, root.rel(partrel).relid).relid;

            if subplanidx >= 0 {
                present_parts.insert(p);
                // Track leaf partitions (nparts == -1) for prunableRelids.
                if root.rel(partrel).nparts == -1 {
                    leafpart_rti_map[p as usize] = root.rel(partrel).relid as i32;
                }
                subplansfound.insert(subplanidx);
            } else if subpartidx >= 0 {
                present_parts.insert(p);
            }
        }

        debug_assert!(!present_parts.is_empty());

        result.push(CarrierRelPruneInfo {
            rtindex: build.rtindex,
            present_parts: bms_to_rawbms(&present_parts),
            nparts,
            subplan_map,
            subpart_map,
            leafpart_rti_map,
            relid_map,
            initial_pruning_steps: build.initial_pruning_steps,
            exec_pruning_steps: build.exec_pruning_steps,
            execparamids: build.execparamids,
        });
    }

    *matchedsubplans = subplansfound;
    Ok(Some(result))
}

/// `get_partkey_exec_paramids(steps)` (partprune.c:2654): collect the exec Param
/// ids used in the Op steps' non-Const exprs.
fn get_partkey_exec_paramids(steps: &[PartitionPruneStep]) -> Bitmapset {
    let mut execparamids = Bitmapset::new();
    for step in steps {
        if let PartitionPruneStep::Op(op) = step {
            for expr in &op.exprs {
                if !matches!(expr, Expr::Const(_)) {
                    for id in pull_exec_paramids(expr) {
                        execparamids.insert(id);
                    }
                }
            }
        }
    }
    execparamids
}

/// Convert a `Relids` (planner bitmapset, `Option<Box<Bitmapset { words }>>`) to
/// the crate-local `Bitmapset` (sorted set of set-bit indexes).
fn relids_to_bitmapset(relids: &Relids) -> Bitmapset {
    let mut set = Bitmapset::new();
    if let Some(bms) = relids {
        for (wi, &word) in bms.words.iter().enumerate() {
            let mut w = word;
            while w != 0 {
                let b = w.trailing_zeros() as i32;
                set.insert(wi as i32 * 64 + b);
                w &= w - 1;
            }
        }
    }
    set
}

// =============================================================================
// init_seams — install prune_append_rel_partitions + make_partition_pruneinfo
// =============================================================================

/// Install this unit's `partprune.c` seams.
pub fn init_seams() {
    partprune_seams::prune_append_rel_partitions::set(prune_append_rel_partitions_seam);
    partprune_seams::make_partition_pruneinfo::set(make_partition_pruneinfo_seam);
    partprune_seams::get_matching_partitions::set(get_matching_partitions_seam);
}

/// Seam adapter: `get_matching_partitions(mcx, context, pruning_steps, estate)`
/// — the run-time (executor) pruning kernel entry.
fn get_matching_partitions_seam<'mcx>(
    mcx: Mcx<'mcx>,
    context: &mut types_nodes::partition::PartitionPruneContext<'mcx>,
    pruning_steps: &[types_nodes::partprune_carrier::PartitionPruneStep<'mcx>],
    estate: &mut types_nodes::EStateData<'mcx>,
) -> PgResult<Option<mcx::PgBox<'mcx, types_nodes::Bitmapset<'mcx>>>> {
    get_matching_partitions_exec(mcx, context, pruning_steps, estate)
}

/// Seam adapter: `make_partition_pruneinfo(run, root, parentrel, subpaths,
/// prunequal)`.
fn make_partition_pruneinfo_seam<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    parentrel: RelId,
    subpaths: &[types_pathnodes::PathId],
    prunequal: &[types_pathnodes::NodeId],
) -> PgResult<i32> {
    make_partition_pruneinfo(run, root, parentrel, subpaths, prunequal)
}

/// Seam adapter: `prune_append_rel_partitions(run, root, rel)`.
fn prune_append_rel_partitions_seam<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    rel: RelId,
) -> PgResult<Relids> {
    prune_append_rel_partitions(run, root, rel)
}

// =============================================================================
// prune_append_rel_partitions — plan-time entry (partprune.c:779)
// =============================================================================

/// `prune_append_rel_partitions(rel)` (partprune.c:779). Process `rel`'s
/// `baserestrictinfo` and use the quals evaluable at planning time to determine
/// the minimum set of partitions that must be scanned. Returns the matching
/// partitions as a `Relids` (Bitmapset of part_rels indexes).
fn prune_append_rel_partitions<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    rel: RelId,
) -> PgResult<Relids> {
    let mcx = run.mcx();
    // Gather the inputs off the RelOptInfo and (for the real boundinfo) the
    // relcache. Done up front so we no longer borrow `root` while pruning.
    let inputs = collect_prune_inputs(run, root, rel, mcx)?;

    let nparts = inputs.nparts;

    // If there are no partitions, return the empty set.
    if nparts == 0 {
        return Ok(bms_to_relids(&Bitmapset::new()));
    }

    // If pruning is disabled or there are no clauses, return all partitions.
    if !enable_partition_pruning() || inputs.clauses.is_empty() {
        let mut all = Bitmapset::new();
        bms_add_range(&mut all, 0, nparts - 1);
        return Ok(bms_to_relids(&all));
    }

    // Process clauses to extract plan-time pruning steps. Contradictory clauses
    // mean the empty set.
    let mut gcontext = gen_partprune_steps(&inputs, PartClauseTarget::Planner)?;
    if gcontext.contradictory {
        return Ok(bms_to_relids(&Bitmapset::new()));
    }
    let pruning_steps = core::mem::take(&mut gcontext.steps);

    // Nothing usable -> all partitions.
    if pruning_steps.is_empty() {
        let mut all = Bitmapset::new();
        bms_add_range(&mut all, 0, nparts - 1);
        return Ok(bms_to_relids(&all));
    }

    // Set up the pruning context and run the kernel.
    let mut context = PruneContext {
        strategy: inputs.part_scheme.strategy,
        partnatts: inputs.part_scheme.partnatts,
        nparts,
        boundinfo: &inputs.boundinfo,
        partcollation: &inputs.part_scheme.partcollation,
        partkey: &inputs.partkey,
        // stepcmpfuncs lazily resolved by step_id*partnatts+keyno.
        stepcmpfuncs: alloc::vec![0 as Oid; (inputs.part_scheme.partnatts as usize) * pruning_steps.len()],
        mcx: inputs.mcx,
    };

    let result = get_matching_partitions(&mut context, &pruning_steps)?;
    Ok(bms_to_relids(&result))
}

/// `enable_partition_pruning` GUC (cost.c). Defaults true; the gate is honored
/// here. (Reading the live GUC requires the cost.c knob; until that seam is
/// available this returns the C default — pruning enabled — which is the
/// behavior every regression file that exercises pruning expects.)
fn enable_partition_pruning() -> bool {
    true
}

/// Inputs gathered for pruning, decoupled from the `&mut PlannerInfo` borrow.
struct PruneInputs<'mcx> {
    mcx: Mcx<'mcx>,
    part_scheme: PartitionScheme,
    partexprs: Vec<Expr<'mcx>>,
    partkey: PartitionKeyData<'mcx>,
    boundinfo: Box<PartitionBoundInfoData<'mcx>>,
    nparts: i32,
    has_default: bool,
    has_partition_qual: bool,
    /// The baserestrictinfo clauses (already deref'd to owned Exprs).
    clauses: Vec<Expr<'mcx>>,
    /// `rel->partition_qual` — the partition constraint (implicit-AND list of
    /// Exprs), deref'd to owned Exprs. Empty for a top-level partitioned table.
    partition_qual: Vec<Expr<'mcx>>,
}

/// Collect the partition metadata + restriction clauses needed to prune `rel`.
///
/// `part_scheme`, `partexprs`, `nparts`, and `partition_qual` come off the
/// RelOptInfo (populated by `set_relation_partition_info`). The bound *data*
/// (`boundinfo`) is a presence-only stub on the planner RelOptInfo, so the real
/// bound algebra is read from the relcache partdesc, exactly as the executor
/// does — open the relation and look up its `PartitionDesc`.
fn collect_prune_inputs<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    rel: RelId,
    mcx: Mcx<'mcx>,
) -> PgResult<PruneInputs<'mcx>> {
    collect_prune_inputs_with_clauses(run, root, rel, mcx, None)
}

/// Like [`collect_prune_inputs`], but if `override_clauses` is `Some`, use those
/// owned clauses instead of the rel's `baserestrictinfo`. The run-time-pruning
/// path (`make_partitionedrel_pruneinfo`) passes the per-partition-translated
/// `partprunequal` here.
fn collect_prune_inputs_with_clauses<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    rel: RelId,
    mcx: Mcx<'mcx>,
    override_clauses: Option<Vec<Expr<'mcx>>>,
) -> PgResult<PruneInputs<'mcx>> {
    // Read the RelOptInfo fields we need (immutable borrow, then drop it).
    let (relid_index, nparts, has_default, has_partition_qual, partexpr_ids, restrict_ids, partqual_ids) = {
        let r = root.rel(rel);
        // rel->relid -> simple_rte_array[relid] -> RTE relid Oid.
        let relid_index = r.relid;
        let nparts = r.nparts;
        let has_default = r.boundinfo.is_some();
        let has_partition_qual = !r.partition_qual.is_empty();
        // First partexpr per key column (linitial(rel->partexprs[i])).
        let partexpr_ids: Vec<types_pathnodes::NodeId> =
            r.partexprs.iter().map(|v| v[0]).collect();
        let restrict_ids: Vec<types_pathnodes::RinfoId> = r.baserestrictinfo.clone();
        let partqual_ids: Vec<types_pathnodes::NodeId> = r.partition_qual.clone();
        (relid_index, nparts, has_default, has_partition_qual, partexpr_ids, restrict_ids, partqual_ids)
    };

    // Deref the partexprs and restriction clauses out of the planner arenas to
    // owned Exprs.
    let partexprs: Vec<Expr<'mcx>> = partexpr_ids
        .iter()
        .map(|id| root.node(*id).clone_in(mcx))
        .collect::<PgResult<Vec<Expr<'mcx>>>>()?;
    // rel->partition_qual is a list of plain Exprs (implicit-AND form, not
    // RestrictInfos); deref each out of the planner arena.
    let partition_qual: Vec<Expr<'mcx>> = partqual_ids
        .iter()
        .map(|id| root.node(*id).clone_in(mcx))
        .collect::<PgResult<Vec<Expr<'mcx>>>>()?;
    // The restriction clauses may carry context-allocated children (a SubPlan /
    // AlternativeSubPlan inside an `... OR exists(...)` qual) with no faithful
    // derived `.clone()`; deep-copy through `Expr::clone_in` (`copyObject`).
    let clauses: Vec<Expr<'mcx>> = match override_clauses {
        Some(c) => c,
        None => restrict_ids
            .iter()
            .map(|rid| {
                let clause_id = root.rinfo(*rid).clause;
                root.node(clause_id).clone_in(mcx)
            })
            .collect::<PgResult<Vec<Expr<'mcx>>>>()?,
    };

    // Resolve the relation Oid from the RTE.
    let reloid_oid = planner_rt_fetch(run, root, relid_index).relid;

    // Open the relation and read its partition key + descriptor. The relation is
    // already locked (planner held the lock); a NoLock open is a fresh pin.
    let relation = backend_access_table_table_seams::table_open::call(
        mcx,
        reloid_oid,
        types_storage::lock::NoLock,
    )?;

    let partkey =
        backend_utils_cache_partcache_seams::relation_get_partition_key::call(mcx, relation.alias())?
            .ok_or_else(|| {
                PgError::error("prune_append_rel_partitions: partitioned table has no partition key")
            })?;

    let mut partdesc =
        backend_partitioning_partdesc::RelationGetPartitionDesc(mcx, &relation, false)?;

    let boundinfo = partdesc
        .boundinfo
        .take()
        .ok_or_else(|| PgError::error("prune_append_rel_partitions: partdesc has no boundinfo"))?;

    relation.close(types_storage::lock::NoLock)?;

    // Build the PartitionScheme snapshot from the partition key.
    let partnatts = partkey.partnatts as i32;
    let pn = partnatts as usize;
    let part_scheme = PartitionScheme {
        strategy: partkey.strategy as i8,
        partnatts,
        partopfamily: partkey.partopfamily.as_slice()[..pn].to_vec(),
        partopcintype: partkey.partopcintype.as_slice()[..pn].to_vec(),
        partcollation: partkey.partcollation.as_slice()[..pn].to_vec(),
        partsupfunc_oid: partkey.partsupfunc.as_slice()[..pn]
            .iter()
            .map(|f| f.fn_oid)
            .collect(),
    };

    Ok(PruneInputs {
        mcx,
        part_scheme,
        partexprs,
        partkey: mcx::PgBox::into_inner(partkey),
        boundinfo: Box::new(mcx::PgBox::into_inner(boundinfo)),
        nparts,
        has_default,
        has_partition_qual,
        clauses,
        partition_qual,
    })
}

// =============================================================================
// get_matching_partitions — kernel entry (partprune.c:846)
// =============================================================================

/// Runtime/plan-time pruning context (a trimmed `PartitionPruneContext`).
struct PruneContext<'a, 'mcx> {
    strategy: i8,
    partnatts: i32,
    nparts: i32,
    boundinfo: &'a PartitionBoundInfoData<'mcx>,
    partcollation: &'a [Oid],
    partkey: &'a PartitionKeyData<'mcx>,
    /// per-step, per-key comparison/hash function OIDs (lazily filled).
    stepcmpfuncs: Vec<Oid>,
    mcx: Mcx<'mcx>,
}

#[inline]
fn prune_cxt_state_idx(partnatts: i32, step_id: i32, keyno: i32) -> usize {
    (partnatts * step_id + keyno) as usize
}

/// `get_matching_partitions(context, pruning_steps)` (partprune.c:846).
fn get_matching_partitions<'mcx>(
    context: &mut PruneContext,
    pruning_steps: &[PartitionPruneStep<'mcx>],
) -> PgResult<Bitmapset> {
    let num_steps = pruning_steps.len();

    // No pruning steps -> all partitions match.
    if num_steps == 0 {
        let mut all = Bitmapset::new();
        bms_add_range(&mut all, 0, context.nparts - 1);
        return Ok(all);
    }

    // Evaluate each step in step-id order, storing its result.
    let mut results: Vec<Option<PruneStepResult>> = alloc::vec![None; num_steps];
    for step in pruning_steps {
        match step {
            PartitionPruneStep::Op(op) => {
                let r = perform_pruning_base_step(context, op)?;
                results[op.step_id as usize] = Some(r);
            }
            PartitionPruneStep::Combine(c) => {
                let r = perform_pruning_combine_step(context, c, &results)?;
                results[c.step_id as usize] = Some(r);
            }
        }
    }

    let final_result = results[num_steps - 1]
        .as_ref()
        .expect("get_matching_partitions: final step result missing");

    let mut result = Bitmapset::new();
    let mut scan_default = final_result.scan_default;
    for &i in final_result.bound_offsets.iter() {
        let partindex = context.boundinfo.indexes.as_slice()[i as usize];
        if partindex < 0 {
            // Uncovered key space — mark the default partition if one exists.
            scan_default |= partition_bound_has_default(context.boundinfo);
            continue;
        }
        result.insert(partindex);
    }

    if final_result.scan_null {
        result.insert(context.boundinfo.null_index);
    }
    if scan_default {
        result.insert(context.boundinfo.default_index);
    }

    Ok(result)
}

#[inline]
fn partition_bound_has_default(bi: &PartitionBoundInfoData) -> bool {
    bi.default_index != -1
}

#[inline]
fn partition_bound_accepts_nulls(bi: &PartitionBoundInfoData) -> bool {
    bi.null_index != -1
}

// =============================================================================
// gen_partprune_steps / gen_partprune_steps_internal (partprune.c:743, :990)
// =============================================================================

/// `gen_partprune_steps(rel, clauses, target, context)` (partprune.c:743).
fn gen_partprune_steps<'a, 'mcx>(
    inputs: &'a PruneInputs<'mcx>,
    target: PartClauseTarget,
) -> PgResult<GeneratePruningStepsContext<'a, 'mcx>> {
    let mut context = GeneratePruningStepsContext {
        mcx: inputs.mcx,
        part_scheme: &inputs.part_scheme,
        partexprs: &inputs.partexprs,
        has_default: inputs.has_default,
        has_partition_qual: inputs.has_partition_qual,
        partition_qual: &inputs.partition_qual,
        target,
        steps: Vec::new(),
        has_mutable_op: false,
        has_mutable_arg: false,
        has_exec_param: false,
        contradictory: false,
        next_step_id: 0,
    };

    // If this partitioned table has a default partition and is itself a
    // partition (partition_qual is not NIL), include the partition constraint in
    // the clauses so the default partition can be pruned using the parent's
    // bound (partprune.c gen_partprune_steps:
    //   if (partition_bound_has_default(rel->boundinfo) && rel->partition_qual)
    //       clauses = list_concat_copy(clauses, rel->partition_qual); ).
    // partition_qual is empty for a top-level partitioned table, so the common
    // path uses the baserestrictinfo clauses unchanged.
    if context.has_default && context.has_partition_qual {
        let mut combined: Vec<Expr<'mcx>> =
            Vec::with_capacity(inputs.clauses.len() + inputs.partition_qual.len());
        for c in &inputs.clauses {
            combined.push(c.clone_in(inputs.mcx)?);
        }
        for c in &inputs.partition_qual {
            combined.push(c.clone_in(inputs.mcx)?);
        }
        gen_partprune_steps_internal(&mut context, &combined)?;
        return Ok(context);
    }

    gen_partprune_steps_internal(&mut context, &inputs.clauses)?;
    Ok(context)
}

/// `gen_partprune_steps_internal(context, clauses)` (partprune.c:990).
fn gen_partprune_steps_internal<'mcx>(
    context: &mut GeneratePruningStepsContext<'_, 'mcx>,
    clauses: &[Expr<'mcx>],
) -> PgResult<Vec<PartitionPruneStep<'mcx>>> {
    let partnatts = context.part_scheme.partnatts;
    let strategy = context.part_scheme.strategy;

    // keyclauses[i] holds PartClauseInfos that matched partition key i.
    let mut keyclauses: Vec<Vec<PartClauseInfo<'mcx>>> = alloc::vec![Vec::new(); partnatts as usize];
    let mut nullkeys: Bitmapset = Bitmapset::new();
    let mut notnullkeys: Bitmapset = Bitmapset::new();
    let mut generate_opsteps = false;
    let mut result: Vec<PartitionPruneStep<'mcx>> = Vec::new();

    // Default-vs-partition-constraint contradiction check (partprune.c:1013):
    //   if (partition_bound_has_default(rel->boundinfo) &&
    //       predicate_refuted_by(rel->partition_qual, clauses, false))
    //   { context->contradictory = true; return NIL; }
    // Only fires when the rel is itself a partition (partition_qual set) with a
    // default partition. Detecting that the query clauses refute this partition's
    // own constraint lets the default sub-partition be pruned, and (via the
    // per-OR-arm recursion) drops contradictory OR arms.
    if context.has_default && context.has_partition_qual {
        if backend_optimizer_util_predtest_seams::predicate_refuted_by_exprs::call(
            context.mcx,
            context.partition_qual,
            clauses,
            false,
        )? {
            context.contradictory = true;
            return Ok(Vec::new());
        }
    }

    for clause in clauses {
        let clause = strip_restrictinfo(clause);

        // Constant-false-or-null is contradictory.
        if let Expr::Const(con) = clause {
            if con.constisnull || !datum_get_bool(con) {
                context.contradictory = true;
                return Ok(Vec::new());
            }
        }

        // Handle BoolExpr (AND/OR) by recursion.
        if let Expr::BoolExpr(boolexpr) = clause {
            match boolexpr.boolop {
                BoolExprType::OR_EXPR => {
                    let mut arg_stepids: Vec<i32> = Vec::new();
                    let mut all_args_contradictory = true;
                    for arg in &boolexpr.args {
                        let argsteps =
                            gen_partprune_steps_internal(context, core::slice::from_ref(arg))?;
                        let arg_contradictory = context.contradictory;
                        context.contradictory = false;
                        if arg_contradictory {
                            continue;
                        }
                        all_args_contradictory = false;
                        if !argsteps.is_empty() {
                            let last = argsteps.last().unwrap();
                            arg_stepids.push(last.step_id());
                        } else {
                            let orstep = gen_prune_step_combine(
                                context,
                                Vec::new(),
                                PartitionPruneCombineOp::Union,
                            );
                            arg_stepids.push(orstep);
                        }
                    }
                    if all_args_contradictory {
                        context.contradictory = true;
                        return Ok(Vec::new());
                    }
                    if !arg_stepids.is_empty() {
                        let step = gen_prune_step_combine(
                            context,
                            arg_stepids,
                            PartitionPruneCombineOp::Union,
                        );
                        result.push(step_ref(context, step));
                    }
                    continue;
                }
                BoolExprType::AND_EXPR => {
                    let argsteps = gen_partprune_steps_internal(context, &boolexpr.args)?;
                    if context.contradictory {
                        return Ok(Vec::new());
                    }
                    if !argsteps.is_empty() {
                        result.push(argsteps.last().unwrap().clone());
                    }
                    continue;
                }
                BoolExprType::NOT_EXPR => {
                    // Fall through to match_clause_to_partition_key (handles
                    // Boolean-test-shaped NOT clauses).
                }
            }
        }

        // Try to match this clause to any partition key.
        for i in 0..partnatts {
            let partkey = &context.partexprs[i as usize];
            let mut clause_is_not_null = false;
            let mut pc: Option<PartClauseInfo<'mcx>> = None;
            let mut clause_steps: Vec<PartitionPruneStep<'mcx>> = Vec::new();

            let status = match_clause_to_partition_key(
                context,
                clause,
                partkey,
                i,
                &mut clause_is_not_null,
                &mut pc,
                &mut clause_steps,
            )?;

            match status {
                PartClauseMatchStatus::MatchClause => {
                    let pc = pc.expect("MATCH_CLAUSE without PartClauseInfo");
                    if nullkeys.contains(&i) {
                        context.contradictory = true;
                        return Ok(Vec::new());
                    }
                    generate_opsteps = true;
                    keyclauses[i as usize].push(pc);
                    break;
                }
                PartClauseMatchStatus::MatchNullness => {
                    if !clause_is_not_null {
                        if notnullkeys.contains(&i) || !keyclauses[i as usize].is_empty() {
                            context.contradictory = true;
                            return Ok(Vec::new());
                        }
                        nullkeys.insert(i);
                    } else {
                        if nullkeys.contains(&i) {
                            context.contradictory = true;
                            return Ok(Vec::new());
                        }
                        notnullkeys.insert(i);
                    }
                    break;
                }
                PartClauseMatchStatus::MatchSteps => {
                    result.extend(clause_steps);
                    break;
                }
                PartClauseMatchStatus::MatchContradict => {
                    context.contradictory = true;
                    return Ok(Vec::new());
                }
                PartClauseMatchStatus::NoMatch => {
                    continue;
                }
                PartClauseMatchStatus::Unsupported => {
                    break;
                }
            }
        }
    }

    // Strategy 1/2/3 (IS NULL / OpExprs / IS NOT NULL all-keys).
    let nullkeys_count = nullkeys.len() as i32;
    if !nullkeys.is_empty()
        && (strategy == PARTITION_STRATEGY_LIST
            || strategy == PARTITION_STRATEGY_RANGE
            || (strategy == PARTITION_STRATEGY_HASH && nullkeys_count == partnatts))
    {
        let nk = bms_to_vec(&nullkeys);
        let step = gen_prune_step_op(context, InvalidStrategy as i32, false, Vec::new(), Vec::new(), nk);
        result.push(step_ref(context, step));
    } else if generate_opsteps {
        let opsteps = gen_prune_steps_from_opexps(context, &keyclauses, &nullkeys)?;
        result.extend(opsteps);
    } else if notnullkeys.len() as i32 == partnatts {
        let step = gen_prune_step_op(context, InvalidStrategy as i32, false, Vec::new(), Vec::new(), Vec::new());
        result.push(step_ref(context, step));
    }

    // Multiple steps under an AND -> add a final INTERSECT combine.
    if result.len() > 1 {
        let step_ids: Vec<i32> = result.iter().map(|s| s.step_id()).collect();
        let final_id =
            gen_prune_step_combine(context, step_ids, PartitionPruneCombineOp::Intersect);
        result.push(step_ref(context, final_id));
    }

    Ok(result)
}

/// Return the just-generated step (looked up by id from context.steps), cloned
/// for inclusion in a result list. The C code returns the step pointer; here we
/// clone the owned value out of the steps store.
fn step_ref<'mcx>(context: &GeneratePruningStepsContext<'_, 'mcx>, step_id: i32) -> PartitionPruneStep<'mcx> {
    context
        .steps
        .iter()
        .find(|s| s.step_id() == step_id)
        .expect("step_ref: step id not found")
        .clone()
}

/// `gen_prune_step_op(...)` (partprune.c:1342). Appends a step to context.steps
/// and returns its step_id.
fn gen_prune_step_op<'mcx>(
    context: &mut GeneratePruningStepsContext<'_, 'mcx>,
    opstrategy: i32,
    op_is_ne: bool,
    exprs: Vec<Expr<'mcx>>,
    cmpfns: Vec<Oid>,
    nullkeys: Vec<i32>,
) -> i32 {
    let step_id = context.next_step_id;
    context.next_step_id += 1;
    let opstrategy = if op_is_ne { InvalidStrategy as i32 } else { opstrategy };
    debug_assert_eq!(exprs.len(), cmpfns.len());
    context.steps.push(PartitionPruneStep::Op(PruneStepOp {
        step_id,
        opstrategy,
        exprs,
        cmpfns,
        nullkeys,
    }));
    step_id
}

/// `gen_prune_step_combine(...)` (partprune.c:1375).
fn gen_prune_step_combine<'mcx>(
    context: &mut GeneratePruningStepsContext<'_, 'mcx>,
    source_stepids: Vec<i32>,
    combine_op: PartitionPruneCombineOp,
) -> i32 {
    let step_id = context.next_step_id;
    context.next_step_id += 1;
    context.steps.push(PartitionPruneStep::Combine(PruneStepCombine {
        step_id,
        combine_op,
        source_stepids,
    }));
    step_id
}

// =============================================================================
// gen_prune_steps_from_opexps (partprune.c:1412)
// =============================================================================

/// `gen_prune_steps_from_opexps(context, keyclauses, nullkeys)`
/// (partprune.c:1412).
fn gen_prune_steps_from_opexps<'mcx>(
    context: &mut GeneratePruningStepsContext<'_, 'mcx>,
    keyclauses: &[Vec<PartClauseInfo<'mcx>>],
    nullkeys: &Bitmapset,
) -> PgResult<Vec<PartitionPruneStep<'mcx>>> {
    let strategy = context.part_scheme.strategy;
    let partnatts = context.part_scheme.partnatts;
    let mut opsteps: Vec<PartitionPruneStep<'mcx>> = Vec::new();

    // btree_clauses indexed by op_strategy (1..=BTMaxStrategyNumber);
    // hash_clauses indexed by HTEqualStrategyNumber.
    let mut btree_clauses: Vec<Vec<PartClauseInfo<'mcx>>> =
        alloc::vec![Vec::new(); (BT_MAX_STRATEGY_NUMBER + 1) as usize];
    let mut hash_clauses: Vec<Vec<PartClauseInfo<'mcx>>> =
        alloc::vec![Vec::new(); (HT_EQUAL_STRATEGY_NUMBER + 1) as usize];

    for i in 0..partnatts {
        let clauselist = &keyclauses[i as usize];
        let mut consider_next_key = true;

        if strategy == PARTITION_STRATEGY_RANGE && clauselist.is_empty() {
            break;
        }
        if strategy == PARTITION_STRATEGY_HASH && clauselist.is_empty() && !nullkeys.contains(&i) {
            return Ok(Vec::new());
        }

        for pc in clauselist {
            let mut pc = pc.clone();
            if pc.op_strategy == InvalidStrategy as i32 {
                let (op_strategy, _lt, _rt) = get_op_opfamily_properties(
                    pc.opno,
                    context.part_scheme.partopfamily[i as usize],
                    false,
                )?;
                pc.op_strategy = op_strategy;
            }
            match strategy {
                PARTITION_STRATEGY_LIST | PARTITION_STRATEGY_RANGE => {
                    btree_clauses[pc.op_strategy as usize].push(pc.clone());
                    if pc.op_strategy == BTLessStrategyNumber as i32
                        || pc.op_strategy == BTGreaterStrategyNumber as i32
                    {
                        consider_next_key = false;
                    }
                }
                PARTITION_STRATEGY_HASH => {
                    if pc.op_strategy != HT_EQUAL_STRATEGY_NUMBER {
                        return Err(PgError::error("invalid clause for hash partitioning"));
                    }
                    hash_clauses[pc.op_strategy as usize].push(pc.clone());
                }
                _ => return Err(PgError::error("invalid partition strategy")),
            }
        }

        if !consider_next_key {
            break;
        }
    }

    match strategy {
        PARTITION_STRATEGY_LIST | PARTITION_STRATEGY_RANGE => {
            let eq_clauses = btree_clauses[BTEqualStrategyNumber as usize].clone();
            let le_clauses = btree_clauses[BTLessEqualStrategyNumber as usize].clone();
            let ge_clauses = btree_clauses[BTGreaterEqualStrategyNumber as usize].clone();

            for strat in 1..=BT_MAX_STRATEGY_NUMBER {
                let strat_clauses = btree_clauses[strat as usize].clone();
                for pc in &strat_clauses {
                    if pc.keyno == 0 {
                        let pc_steps = get_steps_using_prefix(
                            context,
                            strat,
                            pc.op_is_ne,
                            pc.expr.clone(),
                            pc.cmpfn,
                            &[],
                            &[],
                        )?;
                        opsteps.extend(pc_steps);
                        continue;
                    }

                    // Build the prefix of inclusive clauses from earlier keys.
                    let mut prefix: Vec<PartClauseInfo<'mcx>> = Vec::new();
                    let mut prefix_valid = true;
                    let mut eq_idx = 0usize;
                    let mut le_idx = 0usize;
                    let mut ge_idx = 0usize;

                    for keyno in 0..pc.keyno {
                        let mut pk_has_clauses = false;

                        while eq_idx < eq_clauses.len() {
                            let eqpc = &eq_clauses[eq_idx];
                            if eqpc.keyno == keyno {
                                prefix.push(eqpc.clone());
                                pk_has_clauses = true;
                                eq_idx += 1;
                            } else {
                                break;
                            }
                        }

                        if strat == BTLessStrategyNumber as i32
                            || strat == BTLessEqualStrategyNumber as i32
                        {
                            while le_idx < le_clauses.len() {
                                let lepc = &le_clauses[le_idx];
                                if lepc.keyno == keyno {
                                    prefix.push(lepc.clone());
                                    pk_has_clauses = true;
                                    le_idx += 1;
                                } else {
                                    break;
                                }
                            }
                        }

                        if strat == BTGreaterStrategyNumber as i32
                            || strat == BTGreaterEqualStrategyNumber as i32
                        {
                            while ge_idx < ge_clauses.len() {
                                let gepc = &ge_clauses[ge_idx];
                                if gepc.keyno == keyno {
                                    prefix.push(gepc.clone());
                                    pk_has_clauses = true;
                                    ge_idx += 1;
                                } else {
                                    break;
                                }
                            }
                        }

                        if !pk_has_clauses {
                            prefix_valid = false;
                            break;
                        }
                    }

                    if prefix_valid {
                        let pc_steps = get_steps_using_prefix(
                            context,
                            strat,
                            pc.op_is_ne,
                            pc.expr.clone(),
                            pc.cmpfn,
                            &[],
                            &prefix,
                        )?;
                        opsteps.extend(pc_steps);
                    } else {
                        break;
                    }
                }
            }
        }
        PARTITION_STRATEGY_HASH => {
            let eq_clauses = hash_clauses[HT_EQUAL_STRATEGY_NUMBER as usize].clone();
            if !eq_clauses.is_empty() {
                // Locate the clause for the greatest column.
                let last_keyno = eq_clauses.last().unwrap().keyno;
                // Add all clauses before the first one for last_keyno to prefix.
                let mut prefix: Vec<PartClauseInfo<'mcx>> = Vec::new();
                let mut first_last_idx = eq_clauses.len();
                for (idx, pc) in eq_clauses.iter().enumerate() {
                    if pc.keyno == last_keyno {
                        first_last_idx = idx;
                        break;
                    }
                    prefix.push(pc.clone());
                }
                let nk = bms_to_vec(nullkeys);
                for pc in &eq_clauses[first_last_idx..] {
                    let pc_steps = get_steps_using_prefix(
                        context,
                        HT_EQUAL_STRATEGY_NUMBER,
                        false,
                        pc.expr.clone(),
                        pc.cmpfn,
                        &nk,
                        &prefix,
                    )?;
                    opsteps.extend(pc_steps);
                }
            }
        }
        _ => return Err(PgError::error("invalid partition strategy")),
    }

    Ok(opsteps)
}

// =============================================================================
// get_steps_using_prefix[_recurse] (partprune.c:2467, :2525)
// =============================================================================

/// `get_steps_using_prefix(...)` (partprune.c:2467).
#[allow(clippy::too_many_arguments)]
fn get_steps_using_prefix<'mcx>(
    context: &mut GeneratePruningStepsContext<'_, 'mcx>,
    step_opstrategy: i32,
    step_op_is_ne: bool,
    step_lastexpr: Expr<'mcx>,
    step_lastcmpfn: Oid,
    step_nullkeys: &[i32],
    prefix: &[PartClauseInfo<'mcx>],
) -> PgResult<Vec<PartitionPruneStep<'mcx>>> {
    if prefix.is_empty() {
        let step = gen_prune_step_op(
            context,
            step_opstrategy,
            step_op_is_ne,
            alloc::vec![step_lastexpr],
            alloc::vec![step_lastcmpfn],
            step_nullkeys.to_vec(),
        );
        return Ok(alloc::vec![step_ref(context, step)]);
    }

    get_steps_using_prefix_recurse(
        context,
        step_opstrategy,
        step_op_is_ne,
        &step_lastexpr,
        step_lastcmpfn,
        step_nullkeys,
        prefix,
        0,
        &[],
        &[],
    )
}

/// `get_steps_using_prefix_recurse(...)` (partprune.c:2525).
#[allow(clippy::too_many_arguments)]
fn get_steps_using_prefix_recurse<'mcx>(
    context: &mut GeneratePruningStepsContext<'_, 'mcx>,
    step_opstrategy: i32,
    step_op_is_ne: bool,
    step_lastexpr: &Expr<'mcx>,
    step_lastcmpfn: Oid,
    step_nullkeys: &[i32],
    prefix: &[PartClauseInfo<'mcx>],
    start: usize,
    step_exprs: &[Expr<'mcx>],
    step_cmpfns: &[Oid],
) -> PgResult<Vec<PartitionPruneStep<'mcx>>> {
    let mut result: Vec<PartitionPruneStep<'mcx>> = Vec::new();

    let cur_keyno = prefix[start].keyno;
    let final_keyno = prefix[prefix.len() - 1].keyno;

    if cur_keyno < final_keyno {
        // Find where the next partition key's clauses begin.
        let mut next_start = start;
        for idx in start..prefix.len() {
            if prefix[idx].keyno > cur_keyno {
                next_start = idx;
                break;
            }
            next_start = idx + 1;
        }

        let mut idx = start;
        while idx < prefix.len() {
            let pc = &prefix[idx];
            if pc.keyno != cur_keyno {
                break;
            }
            let mut step_exprs1 = step_exprs.to_vec();
            step_exprs1.push(pc.expr.clone());
            let mut step_cmpfns1 = step_cmpfns.to_vec();
            step_cmpfns1.push(pc.cmpfn);

            let moresteps = get_steps_using_prefix_recurse(
                context,
                step_opstrategy,
                step_op_is_ne,
                step_lastexpr,
                step_lastcmpfn,
                step_nullkeys,
                prefix,
                next_start,
                &step_exprs1,
                &step_cmpfns1,
            )?;
            result.extend(moresteps);
            idx += 1;
        }
    } else {
        // Generate one step per clause with cur_keyno (from `start` onward).
        let mut idx = start;
        while idx < prefix.len() {
            let pc = &prefix[idx];
            let mut step_exprs1 = step_exprs.to_vec();
            step_exprs1.push(pc.expr.clone());
            step_exprs1.push(step_lastexpr.clone());
            let mut step_cmpfns1 = step_cmpfns.to_vec();
            step_cmpfns1.push(pc.cmpfn);
            step_cmpfns1.push(step_lastcmpfn);

            let step = gen_prune_step_op(
                context,
                step_opstrategy,
                step_op_is_ne,
                step_exprs1,
                step_cmpfns1,
                step_nullkeys.to_vec(),
            );
            result.push(step_ref(context, step));
            idx += 1;
        }
    }

    Ok(result)
}

// =============================================================================
// match_clause_to_partition_key (partprune.c:1819)
// =============================================================================

/// `match_clause_to_partition_key(...)` (partprune.c:1819).
#[allow(clippy::too_many_arguments)]
fn match_clause_to_partition_key<'mcx>(
    context: &mut GeneratePruningStepsContext<'_, 'mcx>,
    clause: &Expr<'mcx>,
    partkey: &Expr<'mcx>,
    partkeyidx: i32,
    clause_is_not_null: &mut bool,
    pc: &mut Option<PartClauseInfo<'mcx>>,
    clause_steps: &mut Vec<PartitionPruneStep<'mcx>>,
) -> PgResult<PartClauseMatchStatus> {
    let partopfamily = context.part_scheme.partopfamily[partkeyidx as usize];
    let partcoll = context.part_scheme.partcollation[partkeyidx as usize];

    // Recognize specially shaped Boolean-partition-key clauses.
    let mut notclause = false;
    let mut outconst: Option<Expr> = None;
    let boolmatchstatus =
        match_boolean_partition_clause(partopfamily, clause, partkey, &mut outconst, &mut notclause)?;

    if boolmatchstatus == PartClauseMatchStatus::MatchClause {
        if notclause {
            // "partkey IS NOT true" -> "partkey IS false OR partkey IS NULL".
            let Expr::BooleanTest(btest) = clause else {
                return Err(PgError::error("notclause set for non-BooleanTest"));
            };
            let new_booltesttype = match btest.booltesttype {
                BoolTestType::IS_NOT_TRUE => BoolTestType::IS_FALSE,
                BoolTestType::IS_NOT_FALSE => BoolTestType::IS_TRUE,
                _ => return Err(PgError::error("unexpected booltesttype for notclause")),
            };
            let mut new_booltest = btest.clone();
            new_booltest.booltesttype = new_booltesttype;

            let nulltest = NullTest {
                arg: Some(Box::new(partkey.clone())),
                nulltesttype: NullTestType::IS_NULL,
                argisrow: false,
                location: -1,
            };

            let or_expr = Expr::BoolExpr(BoolExpr {
                boolop: BoolExprType::OR_EXPR,
                args: alloc::vec![Expr::BooleanTest(new_booltest), Expr::NullTest(nulltest)],
                location: -1,
            });

            *clause_steps = gen_partprune_steps_internal(context, &[or_expr])?;
            if context.contradictory {
                return Ok(PartClauseMatchStatus::MatchContradict);
            } else if clause_steps.is_empty() {
                return Ok(PartClauseMatchStatus::Unsupported);
            }
            return Ok(PartClauseMatchStatus::MatchSteps);
        }

        let expr = outconst.expect("boolmatch MATCH_CLAUSE without outconst");
        *pc = Some(PartClauseInfo {
            keyno: partkeyidx,
            opno: BOOLEAN_EQUAL_OPERATOR,
            op_is_ne: false,
            expr,
            cmpfn: context.part_scheme.partsupfunc_oid[partkeyidx as usize],
            op_strategy: InvalidStrategy as i32,
        });
        return Ok(PartClauseMatchStatus::MatchClause);
    } else if boolmatchstatus == PartClauseMatchStatus::MatchNullness {
        *clause_is_not_null = notclause;
        return Ok(PartClauseMatchStatus::MatchNullness);
    }

    // OpExpr with two args.
    if let Expr::OpExpr(opclause) = clause {
        if opclause.args.len() == 2 {
            return match_opexpr_to_partition_key(
                context, opclause, partkey, partkeyidx, partopfamily, partcoll, pc,
            );
        }
    }

    // ScalarArrayOpExpr.
    if let Expr::ScalarArrayOpExpr(saop) = clause {
        return match_saop_to_partition_key(
            context, saop, partkey, partkeyidx, partopfamily, partcoll, clause_steps,
        );
    }

    // NullTest.
    if let Expr::NullTest(nulltest) = clause {
        let arg = strip_relabel(nulltest.arg.as_deref().expect("NullTest with NULL arg"));
        if !node_equal(arg, partkey)? {
            return Ok(PartClauseMatchStatus::NoMatch);
        }
        *clause_is_not_null = nulltest.nulltesttype == NullTestType::IS_NOT_NULL;
        return Ok(PartClauseMatchStatus::MatchNullness);
    }

    Ok(boolmatchstatus)
}

/// The OpExpr branch of `match_clause_to_partition_key`.
#[allow(clippy::too_many_arguments)]
fn match_opexpr_to_partition_key<'mcx>(
    context: &mut GeneratePruningStepsContext<'_, 'mcx>,
    opclause: &types_nodes::primnodes::OpExpr<'mcx>,
    partkey: &Expr<'mcx>,
    partkeyidx: i32,
    partopfamily: Oid,
    partcoll: Oid,
    pc: &mut Option<PartClauseInfo<'mcx>>,
) -> PgResult<PartClauseMatchStatus> {
    let strategy = context.part_scheme.strategy;
    let leftop = strip_relabel(&opclause.args[0]);
    let rightop = strip_relabel(&opclause.args[1]);
    let mut opno = opclause.opno;
    let expr: Expr;

    if node_equal(leftop, partkey)? {
        expr = rightop.clone();
    } else if node_equal(rightop, partkey)? {
        opno = get_commutator(opno)?;
        if !oid_is_valid(opno) {
            return Ok(PartClauseMatchStatus::Unsupported);
        }
        expr = leftop.clone();
    } else {
        return Ok(PartClauseMatchStatus::NoMatch);
    }

    if !part_coll_matches_expr_coll(partcoll, opclause.inputcollid) {
        return Ok(PartClauseMatchStatus::NoMatch);
    }

    let mut negator: Oid = 0;
    let mut is_opne_listp = false;
    let op_strategy;
    let op_righttype;

    if op_in_opfamily(opno, partopfamily)? {
        let (s, _lt, rt) = get_op_opfamily_properties(opno, partopfamily, false)?;
        op_strategy = s;
        op_righttype = rt;
    } else {
        if strategy != PARTITION_STRATEGY_LIST {
            return Ok(PartClauseMatchStatus::Unsupported);
        }
        negator = get_negator(opno)?;
        if oid_is_valid(negator) && op_in_opfamily(negator, partopfamily)? {
            let (s, _lt, rt) = get_op_opfamily_properties(negator, partopfamily, false)?;
            if s == BTEqualStrategyNumber as i32 {
                is_opne_listp = true;
                op_strategy = s;
                op_righttype = rt;
            } else {
                return Ok(PartClauseMatchStatus::NoMatch);
            }
        } else {
            return Ok(PartClauseMatchStatus::NoMatch);
        }
    }

    if !op_strict(opno)? {
        return Ok(PartClauseMatchStatus::Unsupported);
    }

    // Examine the other argument. We postpone these tests until after matching
    // the partkey and operator (partprune.c:2030).
    //
    // First, check for non-Const argument. (Immutable subexpressions are
    // assumed already folded to a Const.)
    if !matches!(expr, Expr::Const(_)) {
        // When pruning in the planner, only comparisons to constants are
        // supported; has_mutable_arg/has_exec_param do not get set for PLANNER.
        if context.target == PartClauseTarget::Planner {
            return Ok(PartClauseMatchStatus::Unsupported);
        }
        // We can never prune using an expression that contains Vars.
        if backend_optimizer_util_var_seams::contain_var_clause::call(&expr) {
            return Ok(PartClauseMatchStatus::Unsupported);
        }
        // Reject anything containing a volatile function (stable is OK).
        if backend_optimizer_path_small_seams::contain_volatile_functions_expr::call(&expr) {
            return Ok(PartClauseMatchStatus::Unsupported);
        }
        // See if there are any exec Params. If so, usable only at per-scan time.
        let paramids = pull_exec_paramids(&expr);
        if !paramids.is_empty() {
            context.has_exec_param = true;
            if context.target != PartClauseTarget::Exec {
                return Ok(PartClauseMatchStatus::Unsupported);
            }
        } else {
            // It's potentially usable, but mutable.
            context.has_mutable_arg = true;
        }
    }

    // Operator immutability (partprune.c:2087).
    if op_volatile(opno)? != PROVOLATILE_IMMUTABLE {
        context.has_mutable_op = true;
        // When pruning in the planner, we cannot prune with mutable operators.
        if context.target == PartClauseTarget::Planner {
            return Ok(PartClauseMatchStatus::Unsupported);
        }
    }

    // Resolve the comparison/hash support function.
    let cmpfn;
    if op_righttype == context.part_scheme.partopcintype[partkeyidx as usize] {
        cmpfn = context.part_scheme.partsupfunc_oid[partkeyidx as usize];
    } else {
        let f = match strategy {
            PARTITION_STRATEGY_LIST | PARTITION_STRATEGY_RANGE => get_opfamily_proc(
                context.part_scheme.partopfamily[partkeyidx as usize],
                context.part_scheme.partopcintype[partkeyidx as usize],
                op_righttype,
                BTORDER_PROC,
            )?,
            PARTITION_STRATEGY_HASH => get_opfamily_proc(
                context.part_scheme.partopfamily[partkeyidx as usize],
                op_righttype,
                op_righttype,
                HASHEXTENDED_PROC as i16,
            )?,
            _ => return Err(PgError::error("invalid partition strategy")),
        };
        if !oid_is_valid(f) {
            return Ok(PartClauseMatchStatus::NoMatch);
        }
        cmpfn = f;
    }

    let partclause = if is_opne_listp {
        PartClauseInfo {
            keyno: partkeyidx,
            opno: negator,
            op_is_ne: true,
            op_strategy: InvalidStrategy as i32,
            expr,
            cmpfn,
        }
    } else {
        PartClauseInfo {
            keyno: partkeyidx,
            opno,
            op_is_ne: false,
            op_strategy,
            expr,
            cmpfn,
        }
    };
    *pc = Some(partclause);
    Ok(PartClauseMatchStatus::MatchClause)
}

/// The ScalarArrayOpExpr branch of `match_clause_to_partition_key`.
#[allow(clippy::too_many_arguments)]
fn match_saop_to_partition_key<'mcx>(
    context: &mut GeneratePruningStepsContext<'_, 'mcx>,
    saop: &ScalarArrayOpExpr<'mcx>,
    partkey: &Expr<'mcx>,
    _partkeyidx: i32,
    partopfamily: Oid,
    partcoll: Oid,
    clause_steps: &mut Vec<PartitionPruneStep<'mcx>>,
) -> PgResult<PartClauseMatchStatus> {
    let strategy = context.part_scheme.strategy;
    let saop_op = saop.opno;
    let saop_coll = saop.inputcollid;
    let leftop = strip_relabel(&saop.args[0]);
    let rightop = &saop.args[1];

    if !node_equal(leftop, partkey)? || !part_coll_matches_expr_coll(partcoll, saop.inputcollid) {
        return Ok(PartClauseMatchStatus::NoMatch);
    }

    if !op_in_opfamily(saop_op, partopfamily)? {
        if strategy != PARTITION_STRATEGY_LIST {
            return Ok(PartClauseMatchStatus::NoMatch);
        }
        let negator = get_negator(saop_op)?;
        if oid_is_valid(negator) && op_in_opfamily(negator, partopfamily)? {
            let (s, _lt, _rt) = get_op_opfamily_properties(negator, partopfamily, false)?;
            if s != BTEqualStrategyNumber as i32 {
                return Ok(PartClauseMatchStatus::NoMatch);
            }
        } else {
            return Ok(PartClauseMatchStatus::NoMatch);
        }
    }

    if !op_strict(saop_op)? {
        return Ok(PartClauseMatchStatus::Unsupported);
    }

    // Examine the array argument to see if it's usable for pruning. This is
    // identical to the logic for a plain OpExpr (partprune.c:2236).
    if !matches!(rightop, Expr::Const(_)) {
        // PLANNER only supports comparisons to constants; has_mutable_arg /
        // has_exec_param do not get set for PLANNER.
        if context.target == PartClauseTarget::Planner {
            return Ok(PartClauseMatchStatus::Unsupported);
        }
        // We can never prune using an expression that contains Vars.
        if backend_optimizer_util_var_seams::contain_var_clause::call(rightop) {
            return Ok(PartClauseMatchStatus::Unsupported);
        }
        // Reject anything containing a volatile function (stable is OK).
        if backend_optimizer_path_small_seams::contain_volatile_functions_expr::call(rightop) {
            return Ok(PartClauseMatchStatus::Unsupported);
        }
        // See if there are any exec Params. If so, usable only at per-scan time.
        let paramids = pull_exec_paramids(rightop);
        if !paramids.is_empty() {
            context.has_exec_param = true;
            if context.target != PartClauseTarget::Exec {
                return Ok(PartClauseMatchStatus::Unsupported);
            }
        } else {
            context.has_mutable_arg = true;
        }
    }

    // Operator immutability (partprune.c:2285).
    if op_volatile(saop_op)? != PROVOLATILE_IMMUTABLE {
        context.has_mutable_op = true;
        if context.target == PartClauseTarget::Planner {
            return Ok(PartClauseMatchStatus::Unsupported);
        }
    }

    // Examine the contents of the array argument (partprune.c:2297).
    let elem_exprs: Vec<Expr<'mcx>> = match rightop {
        Expr::Const(arr) => {
            // For a constant array, convert the elements to per-element Const
            // nodes (excepting nulls).
            if arr.constisnull {
                return Ok(PartClauseMatchStatus::MatchContradict);
            }
            match deconstruct_const_array(context.mcx, arr, saop.useOr)? {
                Some(e) => e,
                None => return Ok(PartClauseMatchStatus::MatchContradict),
            }
        }
        Expr::ArrayExpr(arrexpr) => {
            // For a nested ArrayExpr we don't know how to flatten; give up.
            if arrexpr.multidims {
                return Ok(PartClauseMatchStatus::Unsupported);
            }
            arrexpr.elements.clone()
        }
        _ => {
            // Give up on any other clause types.
            return Ok(PartClauseMatchStatus::Unsupported);
        }
    };

    // Build one OpExpr per element: leftop saop_op elem.
    let mut elem_clauses: Vec<Expr<'mcx>> = Vec::with_capacity(elem_exprs.len());
    for elem in elem_exprs {
        let opclause = make_opclause(saop_op, BOOLOID, false, leftop.clone(), elem, 0, saop_coll);
        elem_clauses.push(opclause);
    }

    let elem_clauses = if saop.useOr && elem_clauses.len() > 1 {
        alloc::vec![Expr::BoolExpr(BoolExpr {
            boolop: BoolExprType::OR_EXPR,
            args: elem_clauses,
            location: -1,
        })]
    } else {
        elem_clauses
    };

    *clause_steps = gen_partprune_steps_internal(context, &elem_clauses)?;
    if context.contradictory {
        Ok(PartClauseMatchStatus::MatchContradict)
    } else if clause_steps.is_empty() {
        Ok(PartClauseMatchStatus::Unsupported)
    } else {
        Ok(PartClauseMatchStatus::MatchSteps)
    }
}


/// `match_boolean_partition_clause(...)` (partprune.c:3700).
fn match_boolean_partition_clause<'mcx>(
    partopfamily: Oid,
    clause: &Expr<'mcx>,
    partkey: &Expr<'mcx>,
    outconst: &mut Option<Expr>,
    notclause: &mut bool,
) -> PgResult<PartClauseMatchStatus> {
    *outconst = None;
    *notclause = false;

    if !is_builtin_boolean_opfamily(partopfamily) {
        return Ok(PartClauseMatchStatus::Unsupported);
    }

    if let Expr::BooleanTest(btest) = clause {
        let leftop = strip_relabel(btest.arg.as_deref().expect("BooleanTest with NULL arg"));
        if node_equal(leftop, partkey)? {
            match btest.booltesttype {
                BoolTestType::IS_NOT_TRUE => {
                    *notclause = true;
                    *outconst = Some(make_bool_const(true, false));
                    Ok(PartClauseMatchStatus::MatchClause)
                }
                BoolTestType::IS_TRUE => {
                    *outconst = Some(make_bool_const(true, false));
                    Ok(PartClauseMatchStatus::MatchClause)
                }
                BoolTestType::IS_NOT_FALSE => {
                    *notclause = true;
                    *outconst = Some(make_bool_const(false, false));
                    Ok(PartClauseMatchStatus::MatchClause)
                }
                BoolTestType::IS_FALSE => {
                    *outconst = Some(make_bool_const(false, false));
                    Ok(PartClauseMatchStatus::MatchClause)
                }
                BoolTestType::IS_NOT_UNKNOWN => {
                    *notclause = true;
                    Ok(PartClauseMatchStatus::MatchNullness)
                }
                BoolTestType::IS_UNKNOWN => Ok(PartClauseMatchStatus::MatchNullness),
            }
        } else {
            Ok(PartClauseMatchStatus::NoMatch)
        }
    } else {
        let is_not_clause = is_notclause(clause);
        let inner = if is_not_clause { get_notclausearg(clause) } else { clause };
        let leftop = strip_relabel(inner);

        if node_equal(leftop, partkey)? {
            *outconst = Some(make_bool_const(!is_not_clause, false));
        } else if node_equal(&negate_clause(leftop)?, partkey)? {
            *outconst = Some(make_bool_const(is_not_clause, false));
        } else {
            return Ok(PartClauseMatchStatus::NoMatch);
        }
        Ok(PartClauseMatchStatus::MatchClause)
    }
}

// =============================================================================
// perform_pruning_base_step / perform_pruning_combine_step (partprune.c:3444, :3592)
// =============================================================================

/// `perform_pruning_base_step(context, opstep)` (partprune.c:3444).
fn perform_pruning_base_step<'mcx>(
    context: &mut PruneContext,
    opstep: &PruneStepOp<'mcx>,
) -> PgResult<PruneStepResult> {
    let partnatts = context.partnatts;
    let mut values: Vec<Datum> = alloc::vec![Datum::null(); partnatts as usize];
    let mut nvalues = 0i32;
    let mut lc = 0usize; // index into opstep.exprs/cmpfns

    for keyno in 0..partnatts {
        if opstep.nullkeys.contains(&keyno) {
            continue;
        }
        if keyno > nvalues && context.strategy == PARTITION_STRATEGY_RANGE {
            break;
        }
        if lc < opstep.exprs.len() {
            let expr = &opstep.exprs[lc];
            // partkey_datum_from_expr: plan-time only reads Const.constvalue.
            let (datum, isnull) = partkey_datum_from_expr(expr)?;
            if isnull {
                // Strict operators: a null comparison value matches nothing.
                return Ok(PruneStepResult::default());
            }
            let cmpfn = opstep.cmpfns[lc];
            debug_assert!(oid_is_valid(cmpfn));
            let stateidx = prune_cxt_state_idx(partnatts, opstep.step_id, keyno);
            context.stepcmpfuncs[stateidx] = cmpfn;
            values[keyno as usize] = datum;
            nvalues += 1;
            lc += 1;
        }
    }

    let base_stateidx = prune_cxt_state_idx(partnatts, opstep.step_id, 0);

    match context.strategy {
        PARTITION_STRATEGY_HASH => {
            get_matching_hash_bounds(context, opstep.opstrategy, &values, nvalues, base_stateidx, &opstep.nullkeys)
        }
        PARTITION_STRATEGY_LIST => {
            get_matching_list_bounds(context, opstep.opstrategy, values[0].clone(), nvalues, base_stateidx, &opstep.nullkeys)
        }
        PARTITION_STRATEGY_RANGE => {
            get_matching_range_bounds(context, opstep.opstrategy, &values, nvalues, base_stateidx, &opstep.nullkeys)
        }
        _ => Err(PgError::error("unexpected partition strategy")),
    }
}

/// `perform_pruning_combine_step(context, cstep, step_results)`
/// (partprune.c:3592).
fn perform_pruning_combine_step(
    context: &mut PruneContext,
    cstep: &PruneStepCombine,
    step_results: &[Option<PruneStepResult>],
) -> PgResult<PruneStepResult> {
    let mut result = PruneStepResult::default();

    if cstep.source_stepids.is_empty() {
        let bi = context.boundinfo;
        bms_add_range(&mut result.bound_offsets, 0, bi.nindexes - 1);
        result.scan_default = partition_bound_has_default(bi);
        result.scan_null = partition_bound_accepts_nulls(bi);
        return Ok(result);
    }

    match cstep.combine_op {
        PartitionPruneCombineOp::Union => {
            for &step_id in &cstep.source_stepids {
                if step_id >= cstep.step_id {
                    return Err(PgError::error("invalid pruning combine step argument"));
                }
                let sr = step_results[step_id as usize]
                    .as_ref()
                    .expect("combine: source step result missing");
                for &off in sr.bound_offsets.iter() {
                    result.bound_offsets.insert(off);
                }
                if !result.scan_null {
                    result.scan_null = sr.scan_null;
                }
                if !result.scan_default {
                    result.scan_default = sr.scan_default;
                }
            }
        }
        PartitionPruneCombineOp::Intersect => {
            let mut firststep = true;
            for &step_id in &cstep.source_stepids {
                if step_id >= cstep.step_id {
                    return Err(PgError::error("invalid pruning combine step argument"));
                }
                let sr = step_results[step_id as usize]
                    .as_ref()
                    .expect("combine: source step result missing");
                if firststep {
                    result.bound_offsets = sr.bound_offsets.clone();
                    result.scan_null = sr.scan_null;
                    result.scan_default = sr.scan_default;
                    firststep = false;
                } else {
                    result.bound_offsets =
                        result.bound_offsets.intersection(&sr.bound_offsets).copied().collect();
                    if result.scan_null {
                        result.scan_null = sr.scan_null;
                    }
                    if result.scan_default {
                        result.scan_default = sr.scan_default;
                    }
                }
            }
        }
    }

    Ok(result)
}

/// `partkey_datum_from_expr` (partprune.c:3787) — plan-time Const branch.
fn partkey_datum_from_expr<'mcx>(expr: &Expr<'mcx>) -> PgResult<(Datum<'mcx>, bool)> {
    match expr {
        Expr::Const(con) => Ok((con.constvalue.clone(), con.constisnull)),
        _ => Err(PgError::error(
            "partkey_datum_from_expr: non-Const at plan time (run-time pruning unported)",
        )),
    }
}

// =============================================================================
// Run-time (executor) pruning kernel — get_matching_partitions over the
// executor's PartitionPruneContext (partprune.c execution side).
//
// The plan-time entry above runs the steps over a trimmed `PruneContext` built
// from the relcache partition key (PARTTARGET_PLANNER, Const comparisons only).
// At execution the very same step-evaluation kernel runs, except the comparison
// values are produced by `partkey_datum_from_expr`'s ExprState leg — evaluating
// each step's exec-Param / stable-function expression through the context's
// pre-compiled `exprstates[]` over its `exprcontext`. The bound-math matchers
// (`get_matching_{list,range,hash}_bounds` + the bsearch helpers) are shared
// verbatim; only the datum source differs.
// =============================================================================

/// Run-time `partkey_datum_from_expr(context, expr, stateidx, &datum, &isnull)`
/// (partprune.c:3787) — the executor leg. A `Const` yields its cached value;
/// any other expression is evaluated through the pre-compiled `ExprState` at
/// `exprstates[stateidx]` in the context's `exprcontext`
/// (`ExecEvalExprSwitchContext`). The context must carry a valid `exprcontext`
/// whenever a non-Const expression is reached (C: `Assert(exprcontext != NULL)`).
fn partkey_datum_from_expr_exec<'mcx>(
    context: &mut types_nodes::partition::PartitionPruneContext<'mcx>,
    estate: &mut types_nodes::EStateData<'mcx>,
    expr: &Expr<'mcx>,
    stateidx: usize,
) -> PgResult<(Datum<'mcx>, bool)> {
    if let Expr::Const(con) = expr {
        // We can always determine the value of a constant.
        return Ok((con.constvalue.clone(), con.constisnull));
    }

    // We should never see a non-Const in a step unless the caller has passed a
    // valid ExprContext.
    let ectx = context.exprcontext.ok_or_else(|| {
        PgError::error("partkey_datum_from_expr: non-Const expr without exprcontext")
    })?;

    // exprstate = context->exprstates[stateidx];
    let exprstate = context
        .exprstates
        .as_mut_slice()
        .get_mut(stateidx)
        .and_then(|s| s.as_mut())
        .ok_or_else(|| {
            PgError::error("partkey_datum_from_expr: missing compiled ExprState for step")
        })?;

    // *value = ExecEvalExprSwitchContext(exprstate, ectx, isnull);
    backend_executor_execExpr_seams::exec_eval_expr_switch_context::call(exprstate, ectx, estate)
}

/// `get_matching_partitions(context, pruning_steps)` (partprune.c:846) — the
/// run-time (executor) entry. Mirrors the plan-time kernel exactly, but the
/// per-step comparison values come from `partkey_datum_from_expr_exec` (the
/// ExprState leg) so exec-Param / stable-function pruning steps evaluate at
/// execution. Returns the matching partition indexes as the executor's
/// `Bitmapset` (`None` is the C NULL/empty set).
fn get_matching_partitions_exec<'mcx>(
    mcx: Mcx<'mcx>,
    context: &mut types_nodes::partition::PartitionPruneContext<'mcx>,
    pruning_steps: &[types_nodes::partprune_carrier::PartitionPruneStep<'mcx>],
    estate: &mut types_nodes::EStateData<'mcx>,
) -> PgResult<Option<mcx::PgBox<'mcx, types_nodes::Bitmapset<'mcx>>>> {
    let num_steps = pruning_steps.len();
    let nparts = context.nparts;

    // No pruning steps -> all partitions match.
    if num_steps == 0 {
        let mut all = Bitmapset::new();
        if nparts > 0 {
            bms_add_range(&mut all, 0, nparts - 1);
        }
        return Ok(owned_bms_to_exec(mcx, &all)?);
    }

    // Evaluate each step in step-id order, storing its result.
    let mut results: Vec<Option<PruneStepResult>> = alloc::vec![None; num_steps];
    for step in pruning_steps {
        match step {
            types_nodes::partprune_carrier::PartitionPruneStep::Op(op) => {
                let r = perform_pruning_base_step_exec(mcx, context, estate, op)?;
                results[op.step_id as usize] = Some(r);
            }
            types_nodes::partprune_carrier::PartitionPruneStep::Combine(c) => {
                let r = perform_pruning_combine_step_exec(mcx, context, c, &results)?;
                results[c.step_id as usize] = Some(r);
            }
        }
    }

    let final_result = results[num_steps - 1]
        .as_ref()
        .expect("get_matching_partitions: final step result missing");

    let bi = exec_boundinfo(context);
    let mut result = Bitmapset::new();
    let mut scan_default = final_result.scan_default;
    for &i in final_result.bound_offsets.iter() {
        let partindex = bi.indexes.as_slice()[i as usize];
        if partindex < 0 {
            scan_default |= partition_bound_has_default(bi);
            continue;
        }
        result.insert(partindex);
    }

    if final_result.scan_null {
        result.insert(bi.null_index);
    }
    if scan_default {
        result.insert(bi.default_index);
    }

    owned_bms_to_exec(mcx, &result)
}

/// `&context->boundinfo` (the executor context aliases the relcache PartitionDesc
/// boundinfo, moved into the owned context). All bound-math reads go through here.
fn exec_boundinfo<'a, 'mcx>(
    context: &'a types_nodes::partition::PartitionPruneContext<'mcx>,
) -> &'a PartitionBoundInfoData<'mcx> {
    context
        .boundinfo
        .as_deref()
        .expect("get_matching_partitions: PartitionPruneContext has no boundinfo")
}

/// Materialize the executor's `partsupfunc` (the relcache partition key's cached
/// `FmgrInfo`s, carried opaque) so the hash kernel can read the support-func OIDs
/// and collations — a minimal `PartitionKeyData` carrying only the fields
/// `compute_partition_hash_value` reads (`partnatts`, `partsupfunc`,
/// `partcollation`).
fn exec_build_hash_key<'mcx>(
    mcx: Mcx<'mcx>,
    context: &types_nodes::partition::PartitionPruneContext<'mcx>,
) -> PgResult<PartitionKeyData<'mcx>> {
    let partnatts = context.partnatts;
    let supfuncs: &Vec<types_core::fmgr::FmgrInfo> = context
        .partsupfunc
        .0
        .as_ref()
        .and_then(|b| b.downcast_ref::<Vec<types_core::fmgr::FmgrInfo>>())
        .ok_or_else(|| {
            PgError::error("get_matching_partitions: hash partkey support funcs unavailable")
        })?;

    let mut key = PartitionKeyData {
        strategy: context.strategy,
        partnatts: partnatts as i16,
        partattrs: mcx::slice_in(mcx, &[])?,
        partexprs: mcx::slice_in(mcx, &[])?,
        partopfamily: mcx::slice_in(mcx, &[])?,
        partopcintype: mcx::slice_in(mcx, &[])?,
        partsupfunc: mcx::slice_in(mcx, supfuncs.as_slice())?,
        partcollation: mcx::slice_in(mcx, context.partcollation.as_slice())?,
        parttypid: mcx::slice_in(mcx, &[])?,
        parttypmod: mcx::slice_in(mcx, &[])?,
        parttyplen: mcx::slice_in(mcx, &[])?,
        parttypbyval: mcx::slice_in(mcx, &[])?,
        parttypalign: mcx::slice_in(mcx, &[])?,
        parttypcoll: mcx::slice_in(mcx, &[])?,
    };
    // Silence "field never read" lints in the minimal key; only the three fields
    // above are read by compute_partition_hash_value.
    let _ = &mut key.partattrs;
    Ok(key)
}

/// `perform_pruning_base_step(context, opstep)` (partprune.c:3444) — executor
/// leg. Builds the lookup key by evaluating each non-null step expression
/// (`partkey_datum_from_expr_exec`), records the per-key comparison-function OID
/// into the trimmed `PruneContext`, then dispatches to the shared
/// `get_matching_{hash,list,range}_bounds`.
fn perform_pruning_base_step_exec<'mcx>(
    mcx: Mcx<'mcx>,
    context: &mut types_nodes::partition::PartitionPruneContext<'mcx>,
    estate: &mut types_nodes::EStateData<'mcx>,
    opstep: &types_nodes::partprune_carrier::PartitionPruneStepOp<'mcx>,
) -> PgResult<PruneStepResult> {
    let partnatts = context.partnatts;
    let strategy = context.strategy as i8;
    let nullkeys = raw_bms_to_vec(&opstep.nullkeys);

    let mut values: Vec<Datum> = alloc::vec![Datum::null(); partnatts as usize];
    let mut nvalues = 0i32;
    // Per-key comparison-function OIDs for *this* step, indexed by `keyno`
    // (`base_stateidx == 0` below). The trimmed kernel calls the comparison/hash
    // funcs by OID, so we record OIDs (C: `fmgr_info` into `stepcmpfuncs[stateidx]`).
    let mut stepcmpfuncs: Vec<Oid> = alloc::vec![0 as Oid; partnatts as usize];
    let mut lc = 0usize;

    for keyno in 0..partnatts {
        if nullkeys.contains(&keyno) {
            continue;
        }
        if keyno > nvalues && strategy == PARTITION_STRATEGY_RANGE {
            break;
        }
        if lc < opstep.exprs.len() {
            let expr = &opstep.exprs[lc];
            // ExprState slot index into the executor context's exprstates[] array
            // (palloc0'd at partnatts*step_id+keyno by InitPartitionPruneContext).
            let stateidx = prune_cxt_state_idx(partnatts, opstep.step_id, keyno);
            let (datum, isnull) = partkey_datum_from_expr_exec(context, estate, expr, stateidx)?;
            // Strict operators: a null comparison value matches nothing.
            if isnull {
                return Ok(PruneStepResult::default());
            }
            let cmpfn = opstep.cmpfns[lc];
            debug_assert!(oid_is_valid(cmpfn));
            stepcmpfuncs[keyno as usize] = cmpfn;
            values[keyno as usize] = datum;
            nvalues += 1;
            lc += 1;
        }
    }

    // The shared matchers read `stepcmpfuncs[base_stateidx + k]`; this step's
    // funcs sit at the front of the per-step array (base 0).
    let base_stateidx = 0usize;

    // Build the trimmed PruneContext the shared matchers operate over. For hash
    // we also need a minimal PartitionKeyData carrying the support funcs.
    let bi = exec_boundinfo(context).clone_in(mcx)?;
    let partcollation: Vec<Oid> = context.partcollation.as_slice().to_vec();

    match strategy {
        PARTITION_STRATEGY_HASH => {
            let key = exec_build_hash_key(mcx, context)?;
            let mut pc = PruneContext {
                strategy,
                partnatts,
                nparts: context.nparts,
                boundinfo: &bi,
                partcollation: &partcollation,
                partkey: &key,
                stepcmpfuncs,
                mcx,
            };
            get_matching_hash_bounds(
                &mut pc,
                opstep.opstrategy,
                &values,
                nvalues,
                base_stateidx,
                &nullkeys,
            )
        }
        PARTITION_STRATEGY_LIST => {
            let key = empty_partkey(mcx)?;
            let mut pc = PruneContext {
                strategy,
                partnatts,
                nparts: context.nparts,
                boundinfo: &bi,
                partcollation: &partcollation,
                partkey: &key,
                stepcmpfuncs,
                mcx,
            };
            get_matching_list_bounds(
                &mut pc,
                opstep.opstrategy,
                values[0].clone(),
                nvalues,
                base_stateidx,
                &nullkeys,
            )
        }
        PARTITION_STRATEGY_RANGE => {
            let key = empty_partkey(mcx)?;
            let mut pc = PruneContext {
                strategy,
                partnatts,
                nparts: context.nparts,
                boundinfo: &bi,
                partcollation: &partcollation,
                partkey: &key,
                stepcmpfuncs,
                mcx,
            };
            get_matching_range_bounds(
                &mut pc,
                opstep.opstrategy,
                &values,
                nvalues,
                base_stateidx,
                &nullkeys,
            )
        }
        _ => Err(PgError::error("unexpected partition strategy")),
    }
}

/// `perform_pruning_combine_step(context, cstep, step_results)`
/// (partprune.c:3592) — executor leg (identical to the plan-time version, but
/// reading the boundinfo through the executor context).
fn perform_pruning_combine_step_exec<'mcx>(
    _mcx: Mcx<'mcx>,
    context: &types_nodes::partition::PartitionPruneContext<'mcx>,
    cstep: &types_nodes::partprune_carrier::PartitionPruneStepCombine,
    step_results: &[Option<PruneStepResult>],
) -> PgResult<PruneStepResult> {
    let mut result = PruneStepResult::default();
    // Map the carrier combine op onto the crate-local one used in the match below.
    let combine_op = match cstep.combine_op {
        types_nodes::partprune_carrier::PartitionPruneCombineOp::Union => {
            PartitionPruneCombineOp::Union
        }
        types_nodes::partprune_carrier::PartitionPruneCombineOp::Intersect => {
            PartitionPruneCombineOp::Intersect
        }
    };

    if cstep.source_stepids.is_empty() {
        let bi = exec_boundinfo(context);
        bms_add_range(&mut result.bound_offsets, 0, bi.nindexes - 1);
        result.scan_default = partition_bound_has_default(bi);
        result.scan_null = partition_bound_accepts_nulls(bi);
        return Ok(result);
    }

    match combine_op {
        PartitionPruneCombineOp::Union => {
            for &step_id in &cstep.source_stepids {
                if step_id >= cstep.step_id {
                    return Err(PgError::error("invalid pruning combine step argument"));
                }
                let sr = step_results[step_id as usize]
                    .as_ref()
                    .expect("combine: source step result missing");
                for &off in sr.bound_offsets.iter() {
                    result.bound_offsets.insert(off);
                }
                if !result.scan_null {
                    result.scan_null = sr.scan_null;
                }
                if !result.scan_default {
                    result.scan_default = sr.scan_default;
                }
            }
        }
        PartitionPruneCombineOp::Intersect => {
            let mut firststep = true;
            for &step_id in &cstep.source_stepids {
                if step_id >= cstep.step_id {
                    return Err(PgError::error("invalid pruning combine step argument"));
                }
                let sr = step_results[step_id as usize]
                    .as_ref()
                    .expect("combine: source step result missing");
                if firststep {
                    result.bound_offsets = sr.bound_offsets.clone();
                    result.scan_null = sr.scan_null;
                    result.scan_default = sr.scan_default;
                    firststep = false;
                } else {
                    result.bound_offsets = result
                        .bound_offsets
                        .intersection(&sr.bound_offsets)
                        .copied()
                        .collect();
                    if result.scan_null {
                        result.scan_null = sr.scan_null;
                    }
                    if result.scan_default {
                        result.scan_default = sr.scan_default;
                    }
                }
            }
        }
    }

    Ok(result)
}

/// `bms_to_vec` over a `RawBms` carrier (the carrier step's `nullkeys`).
fn raw_bms_to_vec(raw: &types_nodes::partprune_carrier::RawBms) -> Vec<i32> {
    match raw {
        Some(words) => {
            let mut out = Vec::new();
            for (wordnum, &w) in words.iter().enumerate() {
                let mut bits = w;
                let mut bit = 0;
                while bits != 0 {
                    if bits & 1 != 0 {
                        out.push((wordnum * 64 + bit) as i32);
                    }
                    bits >>= 1;
                    bit += 1;
                }
            }
            out
        }
        None => Vec::new(),
    }
}

/// Convert a crate-internal `Bitmapset` (BTreeSet of partition indexes) into the
/// executor's `types_nodes::Bitmapset` allocated in `mcx`. `None` for the empty
/// set (the C NULL).
fn owned_bms_to_exec<'mcx>(
    mcx: Mcx<'mcx>,
    set: &Bitmapset,
) -> PgResult<Option<mcx::PgBox<'mcx, types_nodes::Bitmapset<'mcx>>>> {
    let mut result: Option<mcx::PgBox<'mcx, types_nodes::Bitmapset<'mcx>>> = None;
    for &member in set.iter() {
        let cur = result.take();
        result = Some(backend_nodes_core_seams::bms_add_member::call(
            mcx, cur, member,
        )?);
    }
    Ok(result)
}

/// An empty `PartitionKeyData` (list/range pruning never reads the partkey; only
/// the hash kernel uses `compute_partition_hash_value`).
fn empty_partkey<'mcx>(mcx: Mcx<'mcx>) -> PgResult<PartitionKeyData<'mcx>> {
    Ok(PartitionKeyData {
        strategy: types_nodes::partition::PartitionStrategy::List,
        partnatts: 0,
        partattrs: mcx::slice_in(mcx, &[])?,
        partexprs: mcx::slice_in(mcx, &[])?,
        partopfamily: mcx::slice_in(mcx, &[])?,
        partopcintype: mcx::slice_in(mcx, &[])?,
        partsupfunc: mcx::slice_in(mcx, &[])?,
        partcollation: mcx::slice_in(mcx, &[])?,
        parttypid: mcx::slice_in(mcx, &[])?,
        parttypmod: mcx::slice_in(mcx, &[])?,
        parttyplen: mcx::slice_in(mcx, &[])?,
        parttypbyval: mcx::slice_in(mcx, &[])?,
        parttypalign: mcx::slice_in(mcx, &[])?,
        parttypcoll: mcx::slice_in(mcx, &[])?,
    })
}

// =============================================================================
// get_matching_{hash,list,range}_bounds (partprune.c:2692, :2769, :2980)
// =============================================================================

/// `get_matching_hash_bounds(...)` (partprune.c:2692).
fn get_matching_hash_bounds(
    context: &mut PruneContext,
    opstrategy: i32,
    values: &[Datum],
    nvalues: i32,
    base_stateidx: usize,
    nullkeys: &[i32],
) -> PgResult<PruneStepResult> {
    let mut result = PruneStepResult::default();
    let bi = context.boundinfo;
    let partnatts = context.partnatts;

    if nvalues + nullkeys.len() as i32 == partnatts {
        debug_assert!(opstrategy == HT_EQUAL_STRATEGY_NUMBER || nvalues == 0);
        let mut isnull = alloc::vec![false; partnatts as usize];
        for (i, slot) in isnull.iter_mut().enumerate() {
            *slot = nullkeys.contains(&(i as i32));
        }
        // compute_partition_hash_value over the step's hash functions. The
        // partkey carries the support funcs; for cross-type the resolved cmpfn
        // OID lives in stepcmpfuncs but compute_partition_hash_value uses the
        // partkey's cached FmgrInfos (single-type hash key — the common case).
        let _ = base_stateidx;
        let row_hash = backend_partitioning_partbounds_seams::compute_partition_hash_value::call(
            context.partkey,
            values,
            &isnull,
        )?;
        let greatest_modulus = bi.nindexes as u64;
        let off = (row_hash % greatest_modulus) as i32;
        if bi.indexes.as_slice()[off as usize] >= 0 {
            result.bound_offsets.insert(off);
        }
    } else {
        bms_add_range(&mut result.bound_offsets, 0, bi.nindexes - 1);
    }

    Ok(result)
}

/// `get_matching_list_bounds(...)` (partprune.c:2769).
#[allow(unused_assignments)]
fn get_matching_list_bounds(
    context: &mut PruneContext,
    opstrategy: i32,
    value: Datum,
    nvalues: i32,
    base_stateidx: usize,
    nullkeys: &[i32],
) -> PgResult<PruneStepResult> {
    let mut result = PruneStepResult::default();
    let bi = context.boundinfo;

    if !nullkeys.is_empty() {
        if partition_bound_accepts_nulls(bi) {
            result.scan_null = true;
        } else {
            result.scan_default = partition_bound_has_default(bi);
        }
        return Ok(result);
    }

    if bi.ndatums == 0 {
        result.scan_default = partition_bound_has_default(bi);
        return Ok(result);
    }

    let mut minoff = 0i32;
    let mut maxoff = bi.ndatums - 1;

    if nvalues == 0 {
        bms_add_range(&mut result.bound_offsets, 0, bi.ndatums - 1);
        result.scan_default = partition_bound_has_default(bi);
        return Ok(result);
    }

    let cmpfn = context.stepcmpfuncs[base_stateidx];
    let coll = context.partcollation[0];

    if opstrategy == InvalidStrategy as i32 {
        bms_add_range(&mut result.bound_offsets, 0, bi.ndatums - 1);
        let (off, is_equal) = list_bsearch(context.mcx, cmpfn, coll, bi, &value)?;
        if off >= 0 && is_equal {
            result.bound_offsets.remove(&off);
        }
        result.scan_default = partition_bound_has_default(bi);
        return Ok(result);
    }

    if opstrategy != BTEqualStrategyNumber as i32 {
        result.scan_default = partition_bound_has_default(bi);
    }

    let mut inclusive = false;
    match opstrategy {
        x if x == BTEqualStrategyNumber as i32 => {
            let (off, is_equal) = list_bsearch(context.mcx, cmpfn, coll, bi, &value)?;
            if off >= 0 && is_equal {
                result.bound_offsets.insert(off);
            } else {
                result.scan_default = partition_bound_has_default(bi);
            }
            return Ok(result);
        }
        x if x == BTGreaterEqualStrategyNumber as i32 || x == BTGreaterStrategyNumber as i32 => {
            inclusive = x == BTGreaterEqualStrategyNumber as i32;
            let (mut off, is_equal) = list_bsearch(context.mcx, cmpfn, coll, bi, &value)?;
            if off >= 0 {
                if !is_equal || !inclusive {
                    off += 1;
                }
            } else {
                off = 0;
            }
            if off > bi.ndatums - 1 {
                return Ok(result);
            }
            minoff = off;
        }
        x if x == BTLessEqualStrategyNumber as i32 || x == BTLessStrategyNumber as i32 => {
            inclusive = x == BTLessEqualStrategyNumber as i32;
            let (mut off, is_equal) = list_bsearch(context.mcx, cmpfn, coll, bi, &value)?;
            if off >= 0 && is_equal && !inclusive {
                off -= 1;
            }
            if off < 0 {
                return Ok(result);
            }
            maxoff = off;
        }
        _ => return Err(PgError::error("invalid strategy number")),
    }

    bms_add_range(&mut result.bound_offsets, minoff, maxoff);
    Ok(result)
}

/// `get_matching_range_bounds(...)` (partprune.c:2980).
#[allow(unused_assignments)]
fn get_matching_range_bounds(
    context: &mut PruneContext,
    opstrategy: i32,
    values: &[Datum],
    nvalues: i32,
    base_stateidx: usize,
    nullkeys: &[i32],
) -> PgResult<PruneStepResult> {
    let mut result = PruneStepResult::default();
    let bi = context.boundinfo;
    let partnatts = context.partnatts;
    let partindices = bi.indexes.as_slice();

    if bi.ndatums == 0 || !nullkeys.is_empty() {
        result.scan_default = partition_bound_has_default(bi);
        return Ok(result);
    }

    let mut minoff = 0i32;
    let mut maxoff = bi.ndatums;

    if nvalues == 0 {
        if partindices[minoff as usize] < 0 {
            minoff += 1;
        }
        if partindices[maxoff as usize] < 0 {
            maxoff -= 1;
        }
        result.scan_default = partition_bound_has_default(bi);
        bms_add_range(&mut result.bound_offsets, minoff, maxoff);
        return Ok(result);
    }

    if nvalues < partnatts {
        result.scan_default = partition_bound_has_default(bi);
    }

    // The step's comparison functions live consecutively from base_stateidx.
    let cmpfns: Vec<Oid> = (0..nvalues)
        .map(|k| context.stepcmpfuncs[base_stateidx + k as usize])
        .collect();

    let mut inclusive = false;
    match opstrategy {
        x if x == BTEqualStrategyNumber as i32 => {
            let (off, is_equal) =
                range_datum_bsearch(context.mcx, &cmpfns, context.partcollation, bi, nvalues, values)?;
            if off >= 0 && is_equal {
                if nvalues == partnatts {
                    result.bound_offsets.insert(off + 1);
                    return Ok(result);
                } else {
                    let saved_off = off;
                    let mut off = off;
                    while off >= 1 {
                        let cmpval = rbound_datum_cmp(context.mcx, &cmpfns, context.partcollation, bi, (off - 1) as usize, values, nvalues)?;
                        if cmpval != 0 {
                            break;
                        }
                        off -= 1;
                    }
                    if range_kind_at(bi, off as usize, nvalues as usize) == PartitionRangeDatumKind::MinValue {
                        off += 1;
                    }
                    minoff = off;

                    let mut off = saved_off;
                    while off < bi.ndatums - 1 {
                        let cmpval = rbound_datum_cmp(context.mcx, &cmpfns, context.partcollation, bi, (off + 1) as usize, values, nvalues)?;
                        if cmpval != 0 {
                            break;
                        }
                        off += 1;
                    }
                    maxoff = off + 1;
                }
                bms_add_range(&mut result.bound_offsets, minoff, maxoff);
            } else {
                result.bound_offsets.insert(off + 1);
            }
            return Ok(result);
        }
        x if x == BTGreaterEqualStrategyNumber as i32 || x == BTGreaterStrategyNumber as i32 => {
            inclusive = x == BTGreaterEqualStrategyNumber as i32;
            let (off, is_equal) =
                range_datum_bsearch(context.mcx, &cmpfns, context.partcollation, bi, nvalues, values)?;
            if off < 0 {
                minoff = 0;
            } else if is_equal && nvalues < partnatts {
                let mut off = off;
                while off >= 1 && off < bi.ndatums - 1 {
                    let nextoff = if inclusive { off - 1 } else { off + 1 };
                    let cmpval = rbound_datum_cmp(context.mcx, &cmpfns, context.partcollation, bi, nextoff as usize, values, nvalues)?;
                    if cmpval != 0 {
                        break;
                    }
                    off = nextoff;
                }
                minoff = if inclusive { off } else { off + 1 };
            } else {
                minoff = off + 1;
            }
        }
        x if x == BTLessEqualStrategyNumber as i32 || x == BTLessStrategyNumber as i32 => {
            inclusive = x == BTLessEqualStrategyNumber as i32;
            let (off, is_equal) =
                range_datum_bsearch(context.mcx, &cmpfns, context.partcollation, bi, nvalues, values)?;
            if off >= 0 {
                if is_equal && nvalues < partnatts {
                    let mut off = off;
                    while off >= 1 && off < bi.ndatums - 1 {
                        let nextoff = if inclusive { off + 1 } else { off - 1 };
                        let cmpval = rbound_datum_cmp(context.mcx, &cmpfns, context.partcollation, bi, nextoff as usize, values, nvalues)?;
                        if cmpval != 0 {
                            break;
                        }
                        off = nextoff;
                    }
                    maxoff = if inclusive { off + 1 } else { off };
                } else if !is_equal || inclusive {
                    maxoff = off + 1;
                } else {
                    maxoff = off;
                }
            } else {
                maxoff = off + 1;
            }
        }
        _ => return Err(PgError::error("invalid strategy number")),
    }

    // MINVALUE/MAXVALUE adjustments so we don't scan the default partition.
    if minoff < bi.ndatums && partindices[minoff as usize] < 0 {
        let lastkey = (nvalues - 1) as usize;
        if range_kind_at(bi, minoff as usize, lastkey) == PartitionRangeDatumKind::MinValue {
            minoff += 1;
        }
    }
    if maxoff >= 1 && (maxoff as usize) < partindices.len() && partindices[maxoff as usize] < 0 {
        let lastkey = (nvalues - 1) as usize;
        if range_kind_at(bi, (maxoff - 1) as usize, lastkey) == PartitionRangeDatumKind::MaxValue {
            maxoff -= 1;
        }
    }

    if minoff <= maxoff {
        bms_add_range(&mut result.bound_offsets, minoff, maxoff);
    }
    Ok(result)
}

// =============================================================================
// bsearch helpers — local, taking the step's (cross-type) support function OID.
// =============================================================================

/// `partition_list_bsearch(partsupfunc, partcollation, boundinfo, value)`
/// (partbounds.c:3607) using the step's comparison function OID.
fn list_bsearch(
    mcx: Mcx,
    cmpfn: Oid,
    collation: Oid,
    bi: &PartitionBoundInfoData,
    value: &Datum,
) -> PgResult<(i32, bool)> {
    let mut lo = -1i32;
    let mut hi = bi.ndatums - 1;
    let mut is_equal = false;
    while lo < hi {
        let mid = (lo + hi + 1) / 2;
        let bound = &bi.datums.as_slice()[mid as usize].as_slice()[0];
        let cmpval = call_cmp(mcx, cmpfn, collation, bound.clone(), value.clone())?;
        if cmpval <= 0 {
            lo = mid;
            is_equal = cmpval == 0;
            if is_equal {
                break;
            }
        } else {
            hi = mid - 1;
        }
    }
    Ok((lo, is_equal))
}

/// `partition_range_datum_bsearch(...)` (partbounds.c:3695) using the step's
/// per-key comparison function OIDs.
fn range_datum_bsearch(
    mcx: Mcx,
    cmpfns: &[Oid],
    partcollation: &[Oid],
    bi: &PartitionBoundInfoData,
    nvalues: i32,
    values: &[Datum],
) -> PgResult<(i32, bool)> {
    let mut lo = -1i32;
    let mut hi = bi.ndatums - 1;
    let mut is_equal = false;
    while lo < hi {
        let mid = (lo + hi + 1) / 2;
        let cmpval = rbound_datum_cmp(mcx, cmpfns, partcollation, bi, mid as usize, values, nvalues)?;
        if cmpval <= 0 {
            lo = mid;
            is_equal = cmpval == 0;
            if is_equal {
                break;
            }
        } else {
            hi = mid - 1;
        }
    }
    Ok((lo, is_equal))
}

/// `partition_rbound_datum_cmp(...)` (partbounds.c) using the step's per-key
/// comparison functions, comparing boundinfo->datums[off] against the tuple
/// values.
fn rbound_datum_cmp(
    mcx: Mcx,
    cmpfns: &[Oid],
    partcollation: &[Oid],
    bi: &PartitionBoundInfoData,
    off: usize,
    tuple_datums: &[Datum],
    n_tuple_datums: i32,
) -> PgResult<i32> {
    let rb_datums = bi.datums.as_slice()[off].as_slice();
    let mut cmpval = -1i32;
    for i in 0..n_tuple_datums as usize {
        match range_kind_at(bi, off, i) {
            PartitionRangeDatumKind::MinValue => return Ok(-1),
            PartitionRangeDatumKind::MaxValue => return Ok(1),
            PartitionRangeDatumKind::Value => {}
        }
        cmpval = call_cmp(
            mcx,
            cmpfns[i],
            partcollation[i],
            rb_datums[i].clone(),
            tuple_datums[i].clone(),
        )?;
        if cmpval != 0 {
            break;
        }
    }
    Ok(cmpval)
}

/// boundinfo->kind[off][key], or VALUE if kind is NULL (hash/list).
fn range_kind_at(bi: &PartitionBoundInfoData, off: usize, key: usize) -> PartitionRangeDatumKind {
    match &bi.kind {
        Some(kind) => kind.as_slice()[off].as_slice()[key],
        None => PartitionRangeDatumKind::Value,
    }
}

/// `DatumGetInt32(FunctionCall2Coll(cmpfn, collation, a1, a2))`.
fn call_cmp(
    mcx: Mcx,
    cmpfn: Oid,
    collation: Oid,
    arg1: Datum,
    arg2: Datum,
) -> PgResult<i32> {
    let r = backend_utils_fmgr_fmgr_seams::function_call2_coll_datum::call(
        mcx, cmpfn, collation, arg1, arg2,
    )?;
    Ok(r.as_i32())
}

// =============================================================================
// Small node helpers
// =============================================================================

/// Strip a top-level RestrictInfo wrapper (`IsA(clause, RestrictInfo)`), if any.
/// In the owned model baserestrictinfo clauses are already deref'd to their
/// inner Expr, so this is a no-op pass-through but mirrors the C guard.
fn strip_restrictinfo<'a, 'mcx>(clause: &'a Expr<'mcx>) -> &'a Expr<'mcx> {
    clause
}

/// `if (IsA(x, RelabelType)) x = ((RelabelType *) x)->arg`.
fn strip_relabel<'a, 'mcx>(expr: &'a Expr<'mcx>) -> &'a Expr<'mcx> {
    match expr {
        Expr::RelabelType(r) => r.arg.as_deref().expect("RelabelType with NULL arg"),
        other => other,
    }
}

/// `DatumGetBool(con->constvalue)`.
fn datum_get_bool(con: &Const) -> bool {
    !con.constisnull && con.constvalue.as_bool()
}

fn bms_to_vec(set: &Bitmapset) -> Vec<i32> {
    set.iter().copied().collect()
}

fn bms_to_relids(set: &Bitmapset) -> Relids {
    if set.is_empty() {
        return None;
    }
    let maxbit = *set.iter().next_back().unwrap();
    let nwords = (maxbit as usize / 64) + 1;
    let mut words = alloc::vec![0u64; nwords];
    for &m in set.iter() {
        words[m as usize / 64] |= 1u64 << (m as usize % 64);
    }
    Some(Box::new(types_pathnodes::Bitmapset { words }))
}

// --- thin seam wrappers (lsyscache / nodeFuncs / makefuncs) --------------------

fn get_commutator(opno: Oid) -> PgResult<Oid> {
    backend_utils_cache_lsyscache_seams::get_commutator::call(opno)
}
fn get_negator(opno: Oid) -> PgResult<Oid> {
    backend_utils_cache_lsyscache_seams::get_negator::call(opno)
}
fn op_in_opfamily(opno: Oid, opfamily: Oid) -> PgResult<bool> {
    backend_utils_cache_lsyscache_seams::op_in_opfamily::call(opno, opfamily)
}
fn op_strict(opno: Oid) -> PgResult<bool> {
    backend_utils_cache_lsyscache_seams::op_strict::call(opno)
}
fn op_volatile(opno: Oid) -> PgResult<u8> {
    backend_utils_cache_lsyscache_seams::op_volatile::call(opno)
}
fn get_opfamily_proc(opfamily: Oid, lefttype: Oid, righttype: Oid, procnum: i16) -> PgResult<Oid> {
    backend_utils_cache_lsyscache_seams::get_opfamily_proc::call(opfamily, lefttype, righttype, procnum)
}
/// `get_op_opfamily_properties(opno, opfamily, ordering_op, &strategy, &lefttype,
/// &righttype)` — returns (strategy, lefttype, righttype). `missing_ok=false`.
fn get_op_opfamily_properties(opno: Oid, opfamily: Oid, ordering_op: bool) -> PgResult<(i32, Oid, Oid)> {
    backend_utils_cache_lsyscache_seams::get_op_opfamily_properties::call(opno, opfamily, ordering_op, false)?
        .ok_or_else(|| PgError::error("operator is not a member of opfamily"))
}

fn node_equal(a: &Expr, b: &Expr) -> PgResult<bool> {
    Ok(backend_nodes_nodeFuncs_seams::equal::call(a, b))
}
fn is_notclause(clause: &Expr) -> bool {
    backend_nodes_nodeFuncs_seams::is_notclause::call(clause)
}
fn get_notclausearg<'a, 'mcx>(clause: &'a Expr<'mcx>) -> &'a Expr<'mcx> {
    backend_nodes_nodeFuncs_seams::get_notclausearg::call(clause)
}
fn negate_clause<'mcx>(expr: &Expr<'mcx>) -> PgResult<Expr<'mcx>> {
    backend_optimizer_prep_prepqual_seams::negate_clause::call(expr.clone())
}
fn make_bool_const<'mcx>(value: bool, isnull: bool) -> Expr<'mcx> {
    Expr::Const(backend_nodes_core::makefuncs::make_bool_const(value, isnull))
}
fn make_opclause<'mcx>(
    opno: Oid,
    opresulttype: Oid,
    opretset: bool,
    leftop: Expr<'mcx>,
    rightop: Expr<'mcx>,
    opcollid: Oid,
    inputcollid: Oid,
) -> Expr<'mcx> {
    backend_nodes_core::makefuncs::make_opclause(
        opno, opresulttype, opretset, leftop, Some(rightop), opcollid, inputcollid,
    )
}

/// Deconstruct a constant array into per-element non-null `Const` exprs
/// (partprune.c:2301-2348, the `IsA(rightop, Const)` arm of
/// `match_clause_to_partition_key`'s SAOP handling).
///
/// `arrval = DatumGetArrayTypeP(arr->constvalue)`,
/// `get_typlenbyvalalign(ARR_ELEMTYPE(arrval), ...)`, `deconstruct_array(...)`,
/// then one `makeConst(ARR_ELEMTYPE, -1, arr->constcollid, elemlen,
/// elem_values[i], false, elembyval)` per element. A null element makes the
/// strict `saop_op` return null: it is skipped under `useOr`, but otherwise
/// implies self-contradiction — signalled here by returning `Ok(None)` (the
/// caller maps it to `PARTCLAUSE_MATCH_CONTRADICT`). The caller has already
/// excluded the `arr->constisnull` case.
fn deconstruct_const_array<'mcx>(
    mcx: Mcx<'mcx>,
    arr: &Const<'mcx>,
    use_or: bool,
) -> PgResult<Option<Vec<Expr<'mcx>>>> {
    // ARR_ELEMTYPE(arrval): for a base-element constant array the element type
    // is `get_element_type(arr->consttype)`.
    let elemtype =
        match backend_utils_cache_lsyscache_seams::get_element_type::call(arr.consttype)? {
            Some(t) => t,
            // Not an array type — the C path always has an array Const here, but
            // be defensive rather than fabricate.
            None => return Ok(Some(Vec::new())),
        };

    // get_typlenbyvalalign(ARR_ELEMTYPE(arrval), &elemlen, &elembyval, &elemalign);
    let s = backend_utils_cache_lsyscache_seams::get_typlenbyvalalign::call(elemtype)?;

    // deconstruct_array(DatumGetArrayTypeP(arr->constvalue), ARR_ELEMTYPE,
    //                   elemlen, elembyval, elemalign,
    //                   &elem_values, &elem_nulls, &num_elems);
    let deconstructed = backend_utils_adt_arrayfuncs_seams::deconstruct_array_v::call(
        mcx,
        arr.constvalue.clone(),
        elemtype,
        s.typlen,
        s.typbyval,
        s.typalign as core::ffi::c_char,
    )?;

    let mut elem_exprs: Vec<Expr<'mcx>> = Vec::with_capacity(deconstructed.len());
    for (elem_value, elem_isnull) in deconstructed.iter() {
        // A null array element must lead to a null comparison result, since
        // saop_op is known strict. We can ignore it in the useOr case, but
        // otherwise it implies self-contradiction.
        if *elem_isnull {
            if use_or {
                continue;
            }
            return Ok(None);
        }

        let elem_expr = backend_nodes_core::makefuncs::make_const(
            mcx,
            elemtype,
            -1,
            arr.constcollid,
            s.typlen as i32,
            elem_value.clone(),
            false,
            s.typbyval,
        )?;
        elem_exprs.push(Expr::Const(elem_expr));
    }

    Ok(Some(elem_exprs))
}

use types_pathnodes::planner_run::planner_rt_fetch;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bms_add_range_inclusive() {
        let mut s = Bitmapset::new();
        bms_add_range(&mut s, 2, 5);
        assert_eq!(bms_to_vec(&s), alloc::vec![2, 3, 4, 5]);
    }

    #[test]
    fn bms_to_relids_empty_is_none() {
        assert!(bms_to_relids(&Bitmapset::new()).is_none());
    }

    #[test]
    fn bms_to_relids_words_packed() {
        let mut s = Bitmapset::new();
        s.insert(0);
        s.insert(63);
        s.insert(64);
        let r = bms_to_relids(&s).expect("non-empty");
        assert_eq!(r.words.len(), 2);
        assert_eq!(r.words[0], (1u64 << 0) | (1u64 << 63));
        assert_eq!(r.words[1], 1u64 << 0);
    }
}
