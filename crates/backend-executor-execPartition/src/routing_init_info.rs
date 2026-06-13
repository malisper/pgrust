//! `ExecInitPartitionInfo` — split out of the `routing_setup` family because
//! the C function (execPartition.c L507-985, ~479 lines) is large enough to own
//! its own module: it locks the partition, builds its `ResultRelInfo`, validates
//! it as an INSERT/UPDATE target, opens partition indices, and builds the WITH
//! CHECK OPTION / RETURNING / ON CONFLICT / MERGE state before storing it in the
//! next free slot of `proute->partitions`.

use mcx::Mcx;
use types_error::PgResult;
use types_nodes::{EStateData, ModifyTableState, ResultRelInfo, RriId};
use types_storage::lock::RowExclusiveLock;

use crate::{PartitionDispatchId, PartitionTupleRouting};

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
    // Oid partOid = dispatch->partdesc->oids[partidx];
    let part_oid = {
        let pd = &proute.partition_dispatch_info[dispatch];
        let partdesc = pd
            .partdesc
            .as_ref()
            .expect("PartitionDispatch.partdesc set");
        partdesc.oids[partidx as usize]
    };

    // partrel = table_open(partOid, RowExclusiveLock);
    let partrel =
        backend_access_common_relation_seams::relation_open::call(mcx, part_oid, RowExclusiveLock)?;

    // leaf_part_rri = makeNode(ResultRelInfo);
    let mut leaf_part_rri = ResultRelInfo::default();

    // InitResultRelInfo(leaf_part_rri, partrel, 0, rootResultRelInfo,
    //                   estate->es_instrument);
    let instrument = estate.es_instrument;
    backend_executor_execMain_seams::init_result_rel_info::call(
        mcx,
        &mut leaf_part_rri,
        partrel.alias(),
        0,
        Some(root_result_rel_info),
        instrument,
    )?;

    // The following per-partition setup steps — verifying the result rel is a
    // valid INSERT target (CheckValidResultRel), opening partition indices
    // (ExecOpenIndices), building the WITH CHECK OPTION / RETURNING / ON
    // CONFLICT / MERGE state, and storing the per-partition FdwRoutine/batch
    // bookkeeping — all read or write `ResultRelInfo` fields that the trimmed
    // executor type (and their owning -seams crates) do not yet carry.  Until
    // those land, the C body past InitResultRelInfo cannot be expressed; a loud
    // panic here beats silently dropping that logic.
    let _ = (mtstate, partidx, &partrel, &mut leaf_part_rri);
    panic!(
        "ExecInitPartitionInfo: CheckValidResultRel / ExecOpenIndices / WCO / \
         RETURNING / ON CONFLICT / MERGE setup not yet portable — trimmed \
         ResultRelInfo and its owners (execMain index/WCO, execExpr update \
         projection, nodeModifyTable merge) have not landed"
    );
}
