//! Run-time partition pruning family: `ExecDoInitialPruning`,
//! `ExecInitPartitionExecPruning`, `CreatePartitionPruneState`,
//! `InitPartitionPruneContext`, `InitExecPartitionPruneContexts`,
//! `ExecFindMatchingSubPlans`, `find_matching_subplans_recurse`.

use mcx::{Mcx, PgBox, PgVec};
use types_error::PgResult;
use types_nodes::partition::{
    PartitionDescData, PartitionKeyData, PartitionPruneContext, PartitionPruneState,
    PartitionPruningData, PartitionedRelPruningData,
};
use types_nodes::{Bitmapset, EStateData, EcxtId, PlanStateData};

/// `ExecDoInitialPruning(estate)` — perform runtime "initial" pruning for every
/// `PartitionPruneInfo` in `estate->es_part_prune_infos`, building each
/// `PartitionPruneState` (appended to `es_part_prune_states`) and storing the
/// surviving-subplan bitmapset (or `None`) in `es_part_prune_results`; also
/// accumulates the surviving leaf RT indexes into `es_unpruned_relids`.
/// Fallible (pruning evaluation, OOM).
pub fn ExecDoInitialPruning<'mcx>(
    mcx: Mcx<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let _ = (mcx, estate);
    todo!("decomp")
}

/// `ExecInitPartitionExecPruning(planstate, n_total_subplans, part_prune_index,
/// relids, &initially_valid_subplans)` — initialize the data needed for "exec"
/// pruning and return the matching `PartitionPruneState` (id into
/// `es_part_prune_states`) along with the initial-pruning result. Validates the
/// pruneinfo relids against the plan node's (`elog(ERROR)` on mismatch).
/// Fallible (context init, OOM).
pub fn ExecInitPartitionExecPruning<'mcx>(
    mcx: Mcx<'mcx>,
    planstate: &mut PlanStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    n_total_subplans: i32,
    part_prune_index: i32,
    relids: Option<&Bitmapset<'_>>,
) -> PgResult<(usize, Option<PgBox<'mcx, Bitmapset<'mcx>>>)> {
    let _ = (mcx, planstate, estate, n_total_subplans, part_prune_index, relids);
    todo!("decomp")
}

/// `CreatePartitionPruneState(estate, pruneinfo, &all_leafpart_rtis)` — build
/// the `PartitionPruneState` for one `PartitionPruneInfo` (the per-hierarchy
/// `PartitionPruningData` / `PartitionedRelPruningData` tree, subplan/subpart
/// maps, and the initial pruning contexts). `pruneinfo` is addressed by its
/// index into `estate->es_part_prune_infos`. On the EXPLAIN-GENERIC skip path,
/// `all_leafpart_rtis` collects every leaf RT index. Fallible.
pub(crate) fn CreatePartitionPruneState<'mcx>(
    mcx: Mcx<'mcx>,
    estate: &mut EStateData<'mcx>,
    pruneinfo_index: usize,
    all_leafpart_rtis: &mut Option<PgBox<'mcx, Bitmapset<'mcx>>>,
) -> PgResult<PartitionPruneState<'mcx>> {
    let _ = (mcx, estate, pruneinfo_index, all_leafpart_rtis);
    todo!("decomp")
}

/// `InitPartitionPruneContext(context, pruning_steps, partdesc, partkey,
/// planstate, econtext)` — initialize a `PartitionPruneContext` for a list of
/// pruning steps: copy the strategy/bounds, allocate the per-step comparison
/// and `ExprState` arrays, and compile the non-Const lookup expressions
/// (through the planstate when available, else with the econtext's params).
/// `econtext` is an id into the EState pool. Fallible (expression compile, OOM).
pub(crate) fn InitPartitionPruneContext<'mcx>(
    mcx: Mcx<'mcx>,
    estate: &mut EStateData<'mcx>,
    context: &mut PartitionPruneContext<'mcx>,
    pruning_steps: &types_nodes::Opaque,
    partdesc: &PartitionDescData<'mcx>,
    partkey: &PartitionKeyData<'mcx>,
    planstate: Option<&mut PlanStateData<'mcx>>,
    econtext: EcxtId,
) -> PgResult<()> {
    let _ = (mcx, estate, context, pruning_steps, partdesc, partkey, planstate, econtext);
    todo!("decomp")
}

/// `InitExecPartitionPruneContexts(prunestate, parent_plan,
/// initially_valid_subplans, n_total_subplans)` — initialize the deferred
/// exec-pruning contexts of a `PartitionPruneState` (those needing the parent
/// plan's `PlanState`) and re-sequence the subplan/present-part maps to account
/// for subplans removed during initial pruning. `prunestate` is addressed by
/// its index into `es_part_prune_states`. Fallible (context init, OOM).
pub(crate) fn InitExecPartitionPruneContexts<'mcx>(
    mcx: Mcx<'mcx>,
    estate: &mut EStateData<'mcx>,
    prunestate_index: usize,
    parent_plan: &mut PlanStateData<'mcx>,
    initially_valid_subplans: Option<&Bitmapset<'_>>,
    n_total_subplans: i32,
) -> PgResult<()> {
    let _ = (mcx, estate, prunestate_index, parent_plan, initially_valid_subplans, n_total_subplans);
    todo!("decomp")
}

/// `ExecFindMatchingSubPlans(prunestate, initial_prune, &validsubplan_rtis)` —
/// determine which subplans match the pruning steps for the current comparison
/// values. `prunestate` is addressed by its index into `es_part_prune_states`.
/// `validsubplan_rtis` must be `Some` during initial pruning (collects the leaf
/// RT indexes whose subnodes will run). Fallible (pruning evaluation, OOM).
pub fn ExecFindMatchingSubPlans<'mcx>(
    mcx: Mcx<'mcx>,
    estate: &mut EStateData<'mcx>,
    prunestate_index: usize,
    initial_prune: bool,
    validsubplan_rtis: Option<&mut Option<PgBox<'mcx, Bitmapset<'mcx>>>>,
) -> PgResult<Option<PgBox<'mcx, Bitmapset<'mcx>>>> {
    let _ = (mcx, estate, prunestate_index, initial_prune, validsubplan_rtis);
    todo!("decomp")
}

/// `find_matching_subplans_recurse(prunedata, pprune, initial_prune,
/// &validsubplans, &validsubplan_rtis)` — recursive worker for
/// `ExecFindMatchingSubPlans`: add valid (non-prunable) subplan IDs (and, when
/// requested, leaf RT indexes) to the accumulators, recursing into
/// sub-partitioned levels. Guards against overly deep hierarchies via
/// `check_stack_depth` (the partprune seam owner / stack-depth seam), so
/// fallible.
#[allow(clippy::too_many_arguments)]
pub(crate) fn find_matching_subplans_recurse<'mcx>(
    mcx: Mcx<'mcx>,
    estate: &mut EStateData<'mcx>,
    prunedata: &mut PartitionPruningData<'mcx>,
    pprune_index: usize,
    initial_prune: bool,
    validsubplans: &mut Option<PgBox<'mcx, Bitmapset<'mcx>>>,
    validsubplan_rtis: Option<&mut Option<PgBox<'mcx, Bitmapset<'mcx>>>>,
) -> PgResult<()> {
    let _ = (mcx, estate, prunedata, pprune_index, initial_prune, validsubplans, validsubplan_rtis);
    let _: Option<&PartitionedRelPruningData> = None;
    todo!("decomp")
}
