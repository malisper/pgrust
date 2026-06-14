//! Spill family: the hash-agg disk-spill machinery — memory limits and the
//! decision to spill, per-batch spill files (logtape), metrics, batch
//! creation and reading, and the reset of all spill state.

use mcx::Mcx;
use types_error::PgResult;
use types_nodes::nodeagg::{
    AggStateData, HashAggBatch, HashAggSpill, LogicalTapeHandle, LogicalTapeSetHandle, AGG_HASHED,
    AGG_MIXED,
};
use types_nodes::{EStateData, SlotId};

use backend_lib_hyperloglog_seams as hll_seams;
use backend_executor_nodeHash_seams as nodeHash_seams;
use backend_utils_sort_storage_seams as tape_seams;

use crate::hash_grouping::hash_choose_num_partitions;
use crate::{
    HASHAGG_HLL_BIT_WIDTH, HASHAGG_READ_BUFFER_SIZE, HASHAGG_WRITE_BUFFER_SIZE,
};

/// `get_hash_memory_limit()` (nodeHash.c) — the per-hash-operation memory
/// budget in bytes (`hash_mem_multiplier * work_mem * 1024`). Owned by the
/// now-ported `backend-executor-nodeHash` unit; called through its seam crate.
fn get_hash_memory_limit() -> usize {
    // nodeHash owns the real `hash_table::get_hash_memory_limit` (reads the
    // `work_mem` / `hash_mem_multiplier` GUCs) and installs this seam. The C
    // function is infallible in practice; the `PgResult` only mirrors its
    // `ereport`-capable surface, so unwrap it here.
    nodeHash_seams::get_hash_memory_limit::call()
        .expect("get_hash_memory_limit (nodeHash.c) does not ereport") as usize
}

/// `TupleHashEntrySize()` (execGrouping.c) — the fixed per-entry overhead used
/// to refresh the running `hashentrysize` estimate. Owned by the
/// not-yet-ported `backend-executor-execGrouping` unit.
fn tuple_hash_entry_size() -> f64 {
    panic!(
        "seam not installed: backend-executor-execGrouping::TupleHashEntrySize \
         (execGrouping.c) — needed by the hash-agg metrics update"
    )
}

/// `hash_agg_set_limits(hashentrysize, input_groups, used_bits, &mem_limit,
/// &ngroups_limit, &num_partitions)` — compute the memory and group-count
/// limits and the planned partition count for the first pass. Returns
/// `(mem_limit, ngroups_limit, num_partitions)`.
pub fn hash_agg_set_limits(
    hashentrysize: f64,
    input_groups: f64,
    used_bits: i32,
) -> (usize, u64, i32) {
    let hash_mem_limit = get_hash_memory_limit();

    // if not expected to spill, use all of hash_mem
    if input_groups * hashentrysize <= hash_mem_limit as f64 {
        let num_partitions = 0;
        let mem_limit = hash_mem_limit;
        let ngroups_limit = (hash_mem_limit as f64 / hashentrysize) as u64;
        return (mem_limit, ngroups_limit, num_partitions);
    }

    // Calculate expected memory requirements for spilling, which is the size
    // of the buffers needed for all the tapes that need to be open at once.
    // Then, subtract that from the memory available for holding hash tables.
    let (npartitions, _partition_bits) =
        hash_choose_num_partitions(input_groups, hashentrysize, used_bits);
    let num_partitions = npartitions;

    let partition_mem =
        HASHAGG_READ_BUFFER_SIZE + HASHAGG_WRITE_BUFFER_SIZE * npartitions as usize;

    // Don't set the limit below 3/4 of hash_mem. In that case, we are at the
    // minimum number of partitions, so we aren't going to dramatically exceed
    // work mem anyway.
    let mem_limit = if hash_mem_limit > 4 * partition_mem {
        hash_mem_limit - partition_mem
    } else {
        (hash_mem_limit as f64 * 0.75) as usize
    };

    let ngroups_limit = if mem_limit as f64 > hashentrysize {
        (mem_limit as f64 / hashentrysize) as u64
    } else {
        1
    };

    (mem_limit, ngroups_limit, num_partitions)
}

/// `hash_agg_check_limits(aggstate)` — check the current hash-table memory and
/// group count against the limits, entering spill mode when exceeded.
pub fn hash_agg_check_limits<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    mcx: Mcx<'mcx>,
) -> PgResult<()> {
    let ngroups = aggstate.hash_ngroups_current;
    let meta_mem = aggstate
        .hash_metacxt
        .as_ref()
        .map(|c| c.subtree_used())
        .unwrap_or(0);
    let entry_mem = aggstate
        .hash_tablecxt
        .as_ref()
        .map(|c| c.subtree_used())
        .unwrap_or(0);
    let tval_mem = aggstate
        .hashcontext
        .as_ref()
        .map(|c| c.ecxt_per_tuple_memory.subtree_used())
        .unwrap_or(0);
    let total_mem = meta_mem + entry_mem + tval_mem;
    let mut do_spill = false;

    // (USE_INJECTION_POINTS test-only spill triggers are omitted: injection
    // points are a build-time test facility, not production logic.)

    // Don't spill unless there's at least one group in the hash table so we
    // can be sure to make progress even in edge cases.
    if aggstate.hash_ngroups_current > 0
        && (total_mem > aggstate.hash_mem_limit || ngroups > aggstate.hash_ngroups_limit)
    {
        do_spill = true;
    }

    if do_spill {
        hash_agg_enter_spill_mode(aggstate, estate, mcx)?;
    }

    Ok(())
}

/// `hash_agg_enter_spill_mode(aggstate)` — switch the current batch to spill
/// mode: create the spill files for every grouping set so new groups go to
/// disk instead of memory.
pub fn hash_agg_enter_spill_mode<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    mcx: Mcx<'mcx>,
) -> PgResult<()> {
    aggstate.hash_spill_mode = true;
    crate::hash_grouping::hashagg_recompile_expressions(
        aggstate,
        aggstate.table_filled,
        true,
        estate,
    )?;

    if !aggstate.hash_ever_spilled {
        debug_assert!(aggstate.hash_tapeset.is_none());
        debug_assert!(aggstate.hash_spills.is_none());

        aggstate.hash_ever_spilled = true;

        let tapeset = tape_seams::logical_tape_set_create::call(mcx, true, -1)?;
        aggstate.hash_tapeset = Some(tapeset);

        let num_hashes = aggstate.num_hashes;
        let mut spills = mcx::vec_with_capacity_in::<HashAggSpill>(mcx, num_hashes as usize)?;
        for _ in 0..num_hashes {
            spills.push(HashAggSpill::default());
        }

        for setno in 0..num_hashes as usize {
            let num_groups = aggstate
                .perhash
                .as_ref()
                .and_then(|p| p.get(setno))
                .and_then(|ph| ph.aggnode.as_ref())
                .map(|n| n.num_groups)
                .unwrap_or(0);
            let hashentrysize = aggstate.hashentrysize;
            hashagg_spill_init(
                &mut spills[setno],
                tapeset,
                0,
                num_groups as f64,
                hashentrysize,
                mcx,
            )?;
        }

        aggstate.hash_spills = Some(spills);
    }

    Ok(())
}

/// `hash_agg_update_metrics(aggstate, from_tape, npartitions)` — update the
/// peak-memory / disk-usage / batch-count metrics after processing a batch.
pub fn hash_agg_update_metrics<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    from_tape: bool,
    npartitions: i32,
) -> PgResult<()> {
    if aggstate.aggstrategy != AGG_MIXED && aggstate.aggstrategy != AGG_HASHED {
        return Ok(());
    }

    // memory for the hash table itself
    let meta_mem = aggstate
        .hash_metacxt
        .as_ref()
        .map(|c| c.subtree_used())
        .unwrap_or(0);

    // memory for hash entries
    let entry_mem = aggstate
        .hash_tablecxt
        .as_ref()
        .map(|c| c.subtree_used())
        .unwrap_or(0);

    // memory for byref transition states
    let hashkey_mem = aggstate
        .hashcontext
        .as_ref()
        .map(|c| c.ecxt_per_tuple_memory.subtree_used())
        .unwrap_or(0);

    // memory for read/write tape buffers, if spilled
    let mut buffer_mem = npartitions as usize * HASHAGG_WRITE_BUFFER_SIZE;
    if from_tape {
        buffer_mem += HASHAGG_READ_BUFFER_SIZE;
    }

    // update peak mem
    let total_mem = meta_mem + entry_mem + hashkey_mem + buffer_mem;
    if total_mem > aggstate.hash_mem_peak {
        aggstate.hash_mem_peak = total_mem;
    }

    // update disk usage
    if let Some(tapeset) = aggstate.hash_tapeset {
        let blocks = tape_seams::logical_tape_set_blocks::call(tapeset);
        // disk_used = blocks * (BLCKSZ / 1024)
        let disk_used = blocks as u64 * (types_core::BLCKSZ as u64 / 1024);
        if aggstate.hash_disk_used < disk_used {
            aggstate.hash_disk_used = disk_used;
        }
    }

    // update hashentrysize estimate based on contents
    if aggstate.hash_ngroups_current > 0 {
        aggstate.hashentrysize =
            tuple_hash_entry_size() + (hashkey_mem as f64 / aggstate.hash_ngroups_current as f64);
    }

    Ok(())
}

/// `hashagg_finish_initial_spills(aggstate)` — at the end of the first pass,
/// finalize every grouping set's initial spill files into read batches.
pub fn hashagg_finish_initial_spills<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    mcx: Mcx<'mcx>,
) -> PgResult<()> {
    let mut total_npartitions = 0;

    if let Some(mut spills) = aggstate.hash_spills.take() {
        let num_hashes = aggstate.num_hashes as usize;
        for setno in 0..num_hashes {
            total_npartitions += spills[setno].npartitions;
            hashagg_spill_finish(aggstate, &mut spills[setno], setno as i32, mcx)?;
        }

        // We're not processing tuples from outer plan any more; only
        // processing batches of spilled tuples. The initial spill structures
        // are no longer needed. (Dropping `spills` here is C's pfree.)
        drop(spills);
    }

    hash_agg_update_metrics(aggstate, false, total_npartitions)?;
    aggstate.hash_spill_mode = false;

    Ok(())
}

/// `hashagg_reset_spill_state(aggstate)` — release the tape set and all
/// pending batches, returning the node to a non-spilling state.
pub fn hashagg_reset_spill_state<'mcx>(aggstate: &mut AggStateData<'mcx>) -> PgResult<()> {
    // free spills from initial pass
    if let Some(spills) = aggstate.hash_spills.take() {
        // The C frees spill->ntuples and spill->partitions for each set; here
        // dropping the owned vec releases the same allocations.
        drop(spills);
    }

    // free batches (C: list_free_deep + NIL)
    aggstate.hash_batches = None;

    // close tape set
    if let Some(tapeset) = aggstate.hash_tapeset.take() {
        tape_seams::logical_tape_set_close::call(tapeset);
    }

    Ok(())
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
    let batch = HashAggBatch {
        setno,
        used_bits,
        input_tape: Some(input_tape),
        input_tuples,
        input_card,
    };
    mcx::alloc_in(mcx, batch)
}

/// `hashagg_batch_read(batch, &hashp)` — read the next spilled minimal tuple
/// from a batch's input tape, returning its bytes and the stored hash, or
/// `None` at end of tape.
pub fn hashagg_batch_read<'mcx>(
    batch: &mut HashAggBatch,
    mcx: Mcx<'mcx>,
) -> PgResult<Option<(mcx::PgVec<'mcx, u8>, u32)>> {
    let tape = batch
        .input_tape
        .expect("hashagg_batch_read: batch has no input tape");

    // Read the stored 32-bit hash.
    let mut hash_buf = [0u8; 4];
    let nread = tape_seams::logical_tape_read::call(tape, &mut hash_buf)?;
    if nread == 0 {
        return Ok(None);
    }
    if nread != core::mem::size_of::<u32>() {
        return Err(types_error::PgError::error(format!(
            "unexpected EOF for tape {:?}: requested {} bytes, read {} bytes",
            tape,
            core::mem::size_of::<u32>(),
            nread
        )));
    }
    let hash = u32::from_ne_bytes(hash_buf);

    // Read the minimal tuple length (the leading uint32 of MinimalTupleData).
    let mut tlen_buf = [0u8; 4];
    let nread = tape_seams::logical_tape_read::call(tape, &mut tlen_buf)?;
    if nread != core::mem::size_of::<u32>() {
        return Err(types_error::PgError::error(format!(
            "unexpected EOF for tape {:?}: requested {} bytes, read {} bytes",
            tape,
            core::mem::size_of::<u32>(),
            nread
        )));
    }
    let t_len = u32::from_ne_bytes(tlen_buf) as usize;

    // palloc(t_len); the leading uint32 is t_len itself, then read the rest.
    let mut tuple = mcx::vec_with_capacity_in::<u8>(mcx, t_len)?;
    tuple.extend_from_slice(&tlen_buf);
    tuple.resize(t_len, 0);

    let rest = t_len - core::mem::size_of::<u32>();
    let nread = tape_seams::logical_tape_read::call(tape, &mut tuple[core::mem::size_of::<u32>()..])?;
    if nread != rest {
        return Err(types_error::PgError::error(format!(
            "unexpected EOF for tape {:?}: requested {} bytes, read {} bytes",
            tape, rest, nread
        )));
    }

    Ok(Some((tuple, hash)))
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
    let (npartitions, partition_bits) =
        hash_choose_num_partitions(input_groups, hashentrysize, used_bits);

    // (USE_INJECTION_POINTS single-partition override omitted — test facility.)

    let mut partitions =
        mcx::vec_with_capacity_in::<Option<LogicalTapeHandle>>(mcx, npartitions as usize)?;
    let mut ntuples = mcx::vec_with_capacity_in::<i64>(mcx, npartitions as usize)?;
    let mut hll_card = mcx::vec_with_capacity_in::<usize>(mcx, npartitions as usize)?;
    for _ in 0..npartitions {
        ntuples.push(0);
        hll_card.push(0);
        partitions.push(None);
    }

    for i in 0..npartitions as usize {
        partitions[i] = Some(tape_seams::logical_tape_create::call(mcx, tapeset)?);
    }

    spill.shift = 32 - used_bits - partition_bits;
    if spill.shift < 32 {
        spill.mask = ((npartitions - 1) as u32) << spill.shift;
    } else {
        spill.mask = 0;
    }
    spill.npartitions = npartitions;

    for i in 0..npartitions as usize {
        hll_card[i] = hll_seams::init_hyper_log_log::call(HASHAGG_HLL_BIT_WIDTH);
    }

    spill.partitions = Some(partitions);
    spill.ntuples = Some(ntuples);
    spill.hll_card = Some(hll_card);

    Ok(())
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
    // The C body fetches a MinimalTuple from the (possibly projected) input
    // slot via slot_getsomeattrs / ExecStoreVirtualTuple /
    // ExecFetchSlotMinimalTuple, all owned by the not-yet-ported
    // executor/execTuples unit (and the trimmed TupleTableSlot here carries no
    // payload yet). It also reads `colnos_needed` via bms_is_member (owned by
    // nodes/bitmapset.c) and rehashes via hash_bytes_uint32 (owned by
    // access/hash/hashfn.c). None of those owners are ported, so this path
    // must fail loudly rather than spill a fabricated tuple.
    let _ = (aggstate, spill, inputslot, hash);
    panic!(
        "seam not installed: executor/execTuples (ExecFetchSlotMinimalTuple, \
         slot_getsomeattrs, ExecStoreVirtualTuple), nodes/bitmapset \
         (bms_is_member), access/hash/hashfn (hash_bytes_uint32) — needed by \
         hashagg_spill_tuple; ports land with the slot payload model"
    )
}

/// `hashagg_spill_finish(aggstate, spill, setno)` — close a spill's output
/// tapes and turn each partition into a pending read batch.
pub fn hashagg_spill_finish<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    spill: &mut HashAggSpill<'mcx>,
    setno: i32,
    mcx: Mcx<'mcx>,
) -> PgResult<()> {
    let used_bits = 32 - spill.shift;

    if spill.npartitions == 0 {
        return Ok(()); // didn't spill
    }

    for i in 0..spill.npartitions as usize {
        let tape = spill
            .partitions
            .as_ref()
            .and_then(|p| p[i])
            .expect("hashagg_spill_finish: partition tape missing");

        // if the partition is empty, don't create a new batch of work
        let ntuples_i = spill
            .ntuples
            .as_ref()
            .map(|n| n[i])
            .expect("hashagg_spill_finish: ntuples missing");
        if ntuples_i == 0 {
            continue;
        }

        let hll_handle = spill
            .hll_card
            .as_ref()
            .map(|h| h[i])
            .expect("hashagg_spill_finish: hll_card missing");
        let cardinality = hll_seams::estimate_hyper_log_log::call(hll_handle);
        hll_seams::free_hyper_log_log::call(hll_handle);

        // rewinding frees the buffer while not in use
        tape_seams::logical_tape_rewind_for_read::call(tape, HASHAGG_READ_BUFFER_SIZE)?;

        let new_batch =
            hashagg_batch_new(tape, setno, ntuples_i, cardinality, used_bits, mcx)?;

        let batches = aggstate
            .hash_batches
            .get_or_insert_with(|| mcx::PgVec::new_in(mcx));
        batches
            .try_reserve(1)
            .map_err(|_| mcx.oom(core::mem::size_of::<mcx::PgBox<'mcx, HashAggBatch>>()))?;
        batches.push(new_batch);
        aggstate.hash_batches_used += 1;
    }

    // pfree(spill->ntuples / hll_card / partitions): drop the owned vecs.
    spill.ntuples = None;
    spill.hll_card = None;
    spill.partitions = None;

    Ok(())
}
