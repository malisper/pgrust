//! Per-tuple routing family: `ExecFindPartition`, `FormPartitionKeyDatum`,
//! `get_partition_for_tuple`, `ExecBuildSlotPartitionKeyDescription`.

use mcx::{Mcx, PgString};
use types_datum::Datum;
use types_error::PgResult;
use types_nodes::{EStateData, ModifyTableState, RriId, SlotId};
use types_rel::Relation;

use crate::{PartitionDispatchId, PartitionTupleRouting};

/// `ExecFindPartition(mtstate, rootResultRelInfo, proute, slot, estate)` —
/// return the `ResultRelInfo` (id) of the leaf partition the tuple in `slot`
/// belongs to, building or reusing the partition's `ResultRelInfo` on first
/// use. Errors out (`ERRCODE_CHECK_VIOLATION`) when no leaf partition matches;
/// also fallible on the partition-info init it triggers.
pub fn ExecFindPartition<'mcx>(
    mcx: Mcx<'mcx>,
    mtstate: &mut ModifyTableState<'mcx>,
    root_result_rel_info: RriId,
    proute: &mut PartitionTupleRouting<'mcx>,
    slot: SlotId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<RriId> {
    let _ = (mcx, mtstate, root_result_rel_info, proute, slot, estate);
    todo!("decomp")
}

/// `FormPartitionKeyDatum(pd, slot, estate, values, isnull)` — fill the
/// `values[]`/`isnull[]` arrays with the partition key of the tuple in `slot`,
/// compiling the key's expression state on first use. The `ecxt_scantuple` of
/// `estate`'s per-tuple expr context must already point at `slot`. Fallible
/// (expression compile/eval, OOM); `elog(ERROR)` on a key-expression count
/// mismatch.
pub(crate) fn FormPartitionKeyDatum<'mcx>(
    mcx: Mcx<'mcx>,
    dispatch: PartitionDispatchId,
    slot: SlotId,
    estate: &mut EStateData<'mcx>,
    proute: &mut PartitionTupleRouting<'mcx>,
    values: &mut [Datum],
    isnull: &mut [bool],
) -> PgResult<()> {
    let _ = (mcx, dispatch, slot, estate, proute, values, isnull);
    todo!("decomp")
}

/// `get_partition_for_tuple(pd, values, isnull)` — find the partition (index in
/// `0..partdesc->nparts`) accepting the given partition-key values, or -1 if
/// none. Verified MATCH against the C control flow (HASH/LIST/RANGE strategies
/// with the last-found caching path). Fallible: the support/comparison
/// functions and bound searches can `ereport(ERROR)`.
pub(crate) fn get_partition_for_tuple<'mcx>(
    dispatch: &mut crate::PartitionDispatchData<'mcx>,
    values: &[Datum],
    isnull: &[bool],
) -> PgResult<i32> {
    let _ = (dispatch, values, isnull);
    todo!("decomp")
}

/// `ExecBuildSlotPartitionKeyDescription(rel, values, isnull, maxfieldlen)` —
/// build a `"(col, ...) = (val, ...)"` description of the failing partition key
/// for the "no partition found" error message, limited to columns the current
/// user has SELECT rights on. `Ok(None)` when RLS is enabled or permissions
/// allow no column (the C `NULL`). Allocated in `mcx`; out-functions can
/// `ereport(ERROR)`.
pub(crate) fn ExecBuildSlotPartitionKeyDescription<'mcx>(
    mcx: Mcx<'mcx>,
    rel: Relation<'mcx>,
    values: &[Datum],
    isnull: &[bool],
    maxfieldlen: i32,
) -> PgResult<Option<PgString<'mcx>>> {
    let _ = (mcx, rel, values, isnull, maxfieldlen);
    todo!("decomp")
}
