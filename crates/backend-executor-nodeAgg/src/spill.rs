//! Spill family: the hash-agg disk-spill machinery — memory limits and the
//! decision to spill, per-batch spill files (logtape), metrics, batch
//! creation and reading, and the reset of all spill state.

use mcx::Mcx;
use types_error::PgResult;
use types_nodes::nodeagg::{
    AggStateData, HashAggBatch, HashAggSpill, LogicalTapeHandle, LogicalTapeSetHandle,
};
use types_nodes::SlotId;

/// `hash_agg_set_limits(hashentrysize, input_groups, used_bits, &mem_limit,
/// &ngroups_limit, &num_partitions)` — compute the memory and group-count
/// limits and the planned partition count for the first pass. Returns
/// `(mem_limit, ngroups_limit, num_partitions)`.
pub fn hash_agg_set_limits(
    hashentrysize: f64,
    input_groups: f64,
    used_bits: i32,
) -> (usize, u64, i32) {
    todo!("decomp")
}

/// `hash_agg_check_limits(aggstate)` — check the current hash-table memory and
/// group count against the limits, entering spill mode when exceeded.
pub fn hash_agg_check_limits<'mcx>(aggstate: &mut AggStateData<'mcx>) -> PgResult<()> {
    todo!("decomp")
}

/// `hash_agg_enter_spill_mode(aggstate)` — switch the current batch to spill
/// mode: create the spill files for every grouping set so new groups go to
/// disk instead of memory.
pub fn hash_agg_enter_spill_mode<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    mcx: Mcx<'mcx>,
) -> PgResult<()> {
    todo!("decomp")
}

/// `hash_agg_update_metrics(aggstate, from_tape, npartitions)` — update the
/// peak-memory / disk-usage / batch-count metrics after processing a batch.
pub fn hash_agg_update_metrics<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    from_tape: bool,
    npartitions: i32,
) -> PgResult<()> {
    todo!("decomp")
}

/// `hashagg_finish_initial_spills(aggstate)` — at the end of the first pass,
/// finalize every grouping set's initial spill files into read batches.
pub fn hashagg_finish_initial_spills<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    mcx: Mcx<'mcx>,
) -> PgResult<()> {
    todo!("decomp")
}

/// `hashagg_reset_spill_state(aggstate)` — release the tape set and all
/// pending batches, returning the node to a non-spilling state.
pub fn hashagg_reset_spill_state<'mcx>(aggstate: &mut AggStateData<'mcx>) -> PgResult<()> {
    todo!("decomp")
}

/// `hashagg_batch_new(input_tape, setno, input_tuples, input_card, used_bits)`
/// — allocate a `HashAggBatch` describing one spill partition to refill from.
pub fn hashagg_batch_new<'mcx>(
    input_tape: LogicalTapeHandle,
    setno: i32,
    input_tuples: i64,
    input_card: f64,
    used_bits: i32,
    mcx: Mcx<'mcx>,
) -> PgResult<mcx::PgBox<'mcx, HashAggBatch>> {
    todo!("decomp")
}

/// `hashagg_batch_read(batch, &hashp)` — read the next spilled minimal tuple
/// from a batch's input tape, returning its bytes and the stored hash, or
/// `None` at end of tape.
pub fn hashagg_batch_read<'mcx>(
    batch: &mut HashAggBatch,
    mcx: Mcx<'mcx>,
) -> PgResult<Option<(mcx::PgVec<'mcx, u8>, u32)>> {
    todo!("decomp")
}

/// `hashagg_spill_init(spill, tapeset, used_bits, input_groups,
/// hashentrysize)` — initialize a `HashAggSpill`: choose the partition count
/// and create one output tape per partition.
pub fn hashagg_spill_init<'mcx>(
    spill: &mut HashAggSpill<'mcx>,
    tapeset: LogicalTapeSetHandle,
    used_bits: i32,
    input_groups: f64,
    hashentrysize: f64,
    mcx: Mcx<'mcx>,
) -> PgResult<()> {
    todo!("decomp")
}

/// `hashagg_spill_tuple(aggstate, spill, inputslot, hash)` — write one input
/// tuple to the spill partition selected by its hash, returning the tuple's
/// on-disk size.
pub fn hashagg_spill_tuple<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    spill: &mut HashAggSpill<'mcx>,
    inputslot: SlotId,
    hash: u32,
) -> PgResult<usize> {
    todo!("decomp")
}

/// `hashagg_spill_finish(aggstate, spill, setno)` — close a spill's output
/// tapes and turn each partition into a pending read batch.
pub fn hashagg_spill_finish<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    spill: &mut HashAggSpill<'mcx>,
    setno: i32,
    mcx: Mcx<'mcx>,
) -> PgResult<()> {
    todo!("decomp")
}
