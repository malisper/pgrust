//! `executor/execPartition.c` — support routines for partitioning.
//!
//! Two responsibilities, split across family modules:
//!   * tuple routing — `routing_setup` (build/teardown of the
//!     `PartitionTupleRouting`/`PartitionDispatch` structures) and
//!     `routing_find` (`ExecFindPartition` and the per-tuple key extraction +
//!     partition search), with `colnos` holding the UPDATE target-column
//!     remapping helpers.
//!   * run-time partition pruning — `pruning` (initial/exec pruning state
//!     setup and the `ExecFindMatchingSubPlans` evaluator).
//!
//! The `PartitionTupleRouting` and `PartitionDispatchData` structs are the
//! shared carrier types (held by `ModifyTableState.mt_partition_tuple_routing`
//! and named in this unit's seam declarations), so they are homed canonically
//! in `types-nodes` and re-exported here; only the routing *logic* lives in
//! this translation unit.

#![allow(non_snake_case)]

use mcx::Mcx;
use types_core::primitive::AttrNumber;
use nodes::{EcxtId, ResultRelInfo, RriId, SlotId};

// Canonical carrier types homed in `types-nodes` (so the owner, the seam crate,
// and the nodeModifyTable consumers share one definition); re-export at the
// crate root so the family modules continue to reach them via `crate::`.
pub use nodes::{PartitionDispatchData, PartitionDispatchId, PartitionTupleRouting};

pub mod colnos;
pub mod pruning;
pub mod routing_find;
pub mod routing_init_info;
pub mod routing_setup;

/// `PARTITION_MAX_KEYS` (`pg_config_manual.h`): max columns in a partition key.
pub const PARTITION_MAX_KEYS: usize = 32;

/// `PARTITION_CACHED_FIND_THRESHOLD` (execPartition.c): number of consecutive
/// same-partition finds before `get_partition_for_tuple` switches from a binary
/// search to a cached last-found check. Must be above 0.
pub const PARTITION_CACHED_FIND_THRESHOLD: i32 = 16;

/// Install this unit's seams.
///
/// The two run-time-pruning entry points are reached across a dependency cycle
/// by the Append/MergeAppend executor nodes (which cannot take a direct
/// dependency on the executor's own pruning unit). The three tuple-routing
/// entry points are reached by nodeModifyTable, which routes through the seam
/// crate (rather than a direct dependency) so that the carrier type contract is
/// owned by `types-nodes`. All five are installed here, their canonical home.
pub fn init_seams() {
    execPartition_seams::exec_init_partition_exec_pruning::set(
        pruning::ExecInitPartitionExecPruning,
    );
    execPartition_seams::exec_find_matching_subplans::set(
        pruning::ExecFindMatchingSubPlans,
    );
    execPartition_seams::exec_setup_partition_tuple_routing::set(
        seam_exec_setup_partition_tuple_routing,
    );
    execPartition_seams::exec_find_partition::set(seam_exec_find_partition);
    execPartition_seams::exec_cleanup_tuple_routing::set(
        routing_setup::ExecCleanupTupleRouting,
    );
    execPartition_seams::exec_do_initial_pruning::set(
        pruning::ExecDoInitialPruning,
    );
    execPartition_seams::adjust_partition_colnos::set(
        colnos::adjust_partition_colnos,
    );
    execPartition_seams::adjust_partition_colnos_using_map::set(
        colnos::adjust_partition_colnos_using_attnums,
    );
}

/// Seam adapter for `exec_setup_partition_tuple_routing`: the owner returns the
/// routing struct by value (C `palloc0` + return-by-pointer); the seam contract
/// hands it back boxed in `mcx` so `ModifyTableState` can hold the
/// `Option<PgBox<…>>` the C `mt_partition_tuple_routing` pointer maps to.
fn seam_exec_setup_partition_tuple_routing<'mcx>(
    mcx: Mcx<'mcx>,
    estate: &mut ::nodes::EStateData<'mcx>,
    rel: rel::Relation<'mcx>,
) -> types_error::PgResult<mcx::PgBox<'mcx, PartitionTupleRouting<'mcx>>> {
    let proute = routing_setup::ExecSetupPartitionTupleRouting(mcx, estate, rel)?;
    mcx::alloc_in(mcx, proute)
}

/// Seam adapter for `exec_find_partition`: thread the `mcx` into the by-value
/// owner signature (`ExecFindPartition(mcx, mtstate, root, proute, slot,
/// estate)`).
fn seam_exec_find_partition<'mcx>(
    mcx: Mcx<'mcx>,
    mtstate: &mut ::nodes::ModifyTableState<'mcx>,
    root_result_rel_info: RriId,
    proute: &mut PartitionTupleRouting<'mcx>,
    slot: SlotId,
    estate: &mut ::nodes::EStateData<'mcx>,
) -> types_error::PgResult<RriId> {
    routing_find::ExecFindPartition(mcx, mtstate, root_result_rel_info, proute, slot, estate)
}

// Re-export the family entry points so consumers depend on the crate root.
pub use pruning::{
    ExecDoInitialPruning, ExecFindMatchingSubPlans, ExecInitPartitionExecPruning,
};
pub use routing_find::ExecFindPartition;
pub use routing_setup::{ExecCleanupTupleRouting, ExecSetupPartitionTupleRouting};

// Anchor a few imports the family modules share so the root stays the single
// definition site; silenced until the body phase consumes them everywhere.
#[allow(dead_code)]
fn _vocab_anchor<'mcx>(
    _mcx: Mcx<'mcx>,
    _a: AttrNumber,
    _e: EcxtId,
    _s: SlotId,
    _r: &ResultRelInfo<'mcx>,
) {
}
