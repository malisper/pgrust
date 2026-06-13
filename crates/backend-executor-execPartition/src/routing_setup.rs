//! Tuple-routing structure build/teardown family:
//! `ExecSetupPartitionTupleRouting`, `ExecInitPartitionDispatchInfo`,
//! `ExecInitPartitionInfo`, `ExecInitRoutingInfo`, `ExecCleanupTupleRouting`.

use mcx::Mcx;
use types_core::primitive::Oid;
use types_error::PgResult;
use types_nodes::{EStateData, ModifyTableState, RriId};
use types_rel::Relation;

use crate::{PartitionDispatchId, PartitionTupleRouting};

/// `ExecSetupPartitionTupleRouting(estate, rel)` — set up the information
/// needed during tuple routing for a partitioned table and return it.
///
/// Allocates the routing struct and all subsidiary structs in `mcx` (C: the
/// current context, typically `estate->es_query_cxt`); fallible on OOM and on
/// the relcache reads `ExecInitPartitionDispatchInfo` performs.
pub fn ExecSetupPartitionTupleRouting<'mcx>(
    mcx: Mcx<'mcx>,
    estate: &mut EStateData<'mcx>,
    rel: Relation<'mcx>,
) -> PgResult<PartitionTupleRouting<'mcx>> {
    let _ = (mcx, estate, rel);
    todo!("decomp")
}

/// `ExecInitPartitionInfo(mtstate, estate, proute, dispatch, rootResultRelInfo,
/// partidx)` — lock the partition, build its `ResultRelInfo`, and store it in
/// the next free slot of `proute->partitions`. Returns the new `ResultRelInfo`
/// id. Fallible (table open, index open, expression compilation, OOM).
pub(crate) fn ExecInitPartitionInfo<'mcx>(
    mcx: Mcx<'mcx>,
    mtstate: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    proute: &mut PartitionTupleRouting<'mcx>,
    dispatch: PartitionDispatchId,
    root_result_rel_info: RriId,
    partidx: i32,
) -> PgResult<RriId> {
    let _ = (mcx, mtstate, estate, proute, dispatch, root_result_rel_info, partidx);
    todo!("decomp")
}

/// `ExecInitRoutingInfo(mtstate, estate, proute, dispatch, partRelInfo,
/// partidx, is_borrowed_rel)` — set up tuple-conversion info for a partition
/// and track it in `proute`. Fallible (slot/array allocation, FDW init).
pub(crate) fn ExecInitRoutingInfo<'mcx>(
    mcx: Mcx<'mcx>,
    mtstate: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    proute: &mut PartitionTupleRouting<'mcx>,
    dispatch: PartitionDispatchId,
    part_rel_info: RriId,
    partidx: i32,
    is_borrowed_rel: bool,
) -> PgResult<()> {
    let _ = (mcx, mtstate, estate, proute, dispatch, part_rel_info, partidx, is_borrowed_rel);
    todo!("decomp")
}

/// `ExecInitPartitionDispatchInfo(estate, proute, partoid, parent_pd, partidx,
/// rootResultRelInfo)` — lock the partitioned table (if not already), build its
/// `PartitionDispatch`, store it in `proute->partition_dispatch_info`, and
/// record the parent downlink. Returns the new dispatch's id. Fallible.
pub(crate) fn ExecInitPartitionDispatchInfo<'mcx>(
    mcx: Mcx<'mcx>,
    estate: &mut EStateData<'mcx>,
    proute: &mut PartitionTupleRouting<'mcx>,
    partoid: Oid,
    parent_pd: Option<PartitionDispatchId>,
    partidx: i32,
    root_result_rel_info: Option<RriId>,
) -> PgResult<PartitionDispatchId> {
    let _ = (mcx, estate, proute, partoid, parent_pd, partidx, root_result_rel_info);
    todo!("decomp")
}

/// `ExecCleanupTupleRouting(mtstate, proute)` — close all partitioned tables,
/// leaf partitions, and their indices set up for routing. Fallible (table/index
/// close, FDW shutdown can `elog(ERROR)`).
pub fn ExecCleanupTupleRouting<'mcx>(
    mtstate: &mut ModifyTableState<'mcx>,
    proute: &mut PartitionTupleRouting<'mcx>,
) -> PgResult<()> {
    let _ = (mtstate, proute);
    todo!("decomp")
}
