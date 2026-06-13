//! Hash-grouping family: building and probing the per-grouping-set tuple hash
//! tables, the in-memory and refill retrieve paths, the recompiled transition
//! expressions for hashed input, and the bucket/partition sizing helpers.

use mcx::Mcx;
use types_error::PgResult;
use types_nodes::nodeagg::{
    do_aggsplit_skipfinal, AggStatePerGroupData, AggStateData, AggStrategy, TupleHashEntryHandle,
    TupleHashTableHandle,
};
use types_nodes::{EStateData, SlotId};

use crate::{
    CHUNKHDRSZ, HASHAGG_MAX_PARTITIONS, HASHAGG_MIN_PARTITIONS, HASHAGG_PARTITION_FACTOR,
    HASHAGG_READ_BUFFER_SIZE, HASHAGG_WRITE_BUFFER_SIZE,
};

/// `my_log2(num)` — ceil(log2(num)); returns 0 for num <= 1 (matches the C
/// `my_log2` used by `hash_choose_num_partitions`).
fn my_log2(num: i64) -> i32 {
    let mut limit: i64 = 1;
    let mut exp: i32 = 0;
    while limit < num {
        limit <<= 1;
        exp += 1;
    }
    exp
}

/// `pg_nextpower2_size_t(num)` — smallest power of two >= num.
fn pg_nextpower2_size_t(num: usize) -> usize {
    if num <= 1 {
        return num;
    }
    let mut p: usize = 1;
    while p < num {
        p <<= 1;
    }
    p
}

/// `prepare_hash_slot(perhash, inputslot, hashslot)` — load the hash slot's
/// grouping columns from the input slot for hash-table probing.
pub fn prepare_hash_slot<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    perhash_idx: i32,
    inputslot: SlotId,
    hashslot: SlotId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // C:
    //   slot_getsomeattrs(inputslot, perhash->largestGrpColIdx);
    //   ExecClearTuple(hashslot);
    //   for (i = 0; i < perhash->numhashGrpCols; i++) {
    //       int varNumber = perhash->hashGrpColIdxInput[i] - 1;
    //       hashslot->tts_values[i] = inputslot->tts_values[varNumber];
    //       hashslot->tts_isnull[i] = inputslot->tts_isnull[varNumber];
    //   }
    //   ExecStoreVirtualTuple(hashslot);
    //
    // This pokes the slots' tts_values/tts_isnull arrays and calls
    // slot_getsomeattrs / ExecStoreVirtualTuple, owned by the unported
    // execTuples unit, for which the trimmed shared TupleTableSlot vocabulary
    // carries no value arrays and no seam is declared. Loud panic until that
    // surface lands.
    let _ = (aggstate, perhash_idx, inputslot, hashslot, estate);
    panic!(
        "backend-executor-execTuples: slot_getsomeattrs / ExecStoreVirtualTuple \
         not yet ported (prepare_hash_slot)"
    );
}

/// `build_hash_tables(aggstate)` — (re)create the tuple hash table for every
/// grouping set, sizing buckets from the planned group counts and memory.
pub fn build_hash_tables<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let num_hashes = aggstate.num_hashes;

    for setno in 0..num_hashes {
        let (existing, num_groups) = {
            let perhash = &aggstate.perhash.as_ref().expect("perhash array")[setno as usize];
            let num_groups = perhash
                .aggnode
                .as_ref()
                .expect("perhash->aggnode")
                .num_groups;
            (perhash.hashtable, num_groups)
        };

        if let Some(hashtable) = existing {
            // ResetTupleHashTable(perhash->hashtable);
            backend_executor_execGrouping_seams::reset_tuple_hash_table::call(hashtable)?;
            continue;
        }

        // Assert(perhash->aggnode->numGroups > 0);
        debug_assert!(num_groups > 0);

        // memory = aggstate->hash_mem_limit / aggstate->num_hashes;
        let memory = aggstate.hash_mem_limit / num_hashes as usize;

        // nbuckets = hash_choose_num_buckets(hashentrysize, numGroups, memory);
        let nbuckets = hash_choose_num_buckets(aggstate.hashentrysize, num_groups, memory);

        // (USE_INJECTION_POINTS oversize-table branch is debug-only; omitted.)

        build_hash_table(aggstate, setno, nbuckets, estate)?;
    }

    aggstate.hash_ngroups_current = 0;
    Ok(())
}

/// `build_hash_table(aggstate, setno, nbuckets)` — create one grouping set's
/// hash table via `BuildTupleHashTable`.
pub fn build_hash_table<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    setno: i32,
    nbuckets: i64,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // Assert(aggstrategy == AGG_HASHED || aggstrategy == AGG_MIXED);
    debug_assert!(
        aggstate.aggstrategy == AggStrategy::AggHashed
            || aggstate.aggstrategy == AggStrategy::AggMixed
    );

    // additionalsize = aggstate->numtrans * sizeof(AggStatePerGroupData);
    let _additionalsize =
        aggstate.numtrans as usize * core::mem::size_of::<AggStatePerGroupData>();

    // use_variable_hash_iv = DO_AGGSPLIT_SKIPFINAL(aggstate->aggsplit);
    let _use_variable_hash_iv = do_aggsplit_skipfinal(aggstate.aggsplit);

    // BuildTupleHashTable needs the hashslot's tuple descriptor and ops
    // (perhash->hashslot->tts_tupleDescriptor / tts_ops) plus the per-key
    // descriptors and the meta/table/tmp contexts. The slot descriptor/ops are
    // owned by the unported execTuples slot machinery and are not carried in
    // the trimmed TupleTableSlot vocabulary, so the execGrouping seam cannot be
    // marshaled here. Loud panic until that surface lands.
    let _ = (setno, nbuckets, estate);
    panic!(
        "backend-executor-execTuples: hashslot tts_tupleDescriptor/tts_ops not yet \
         available to marshal BuildTupleHashTable (build_hash_table)"
    );
}

/// `hashagg_recompile_expressions(aggstate, minslot, nullcheck)` — recompile
/// the per-phase transition expressions for hashed input, selecting the
/// outer-ops vs minimal-tuple and null-check cached variants.
pub fn hashagg_recompile_expressions<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    minslot: bool,
    nullcheck: bool,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // int i = minslot ? 1 : 0;  int j = nullcheck ? 1 : 0;
    let i = if minslot { 1usize } else { 0usize };
    let j = if nullcheck { 1usize } else { 0usize };

    // Assert(aggstrategy == AGG_HASHED || aggstrategy == AGG_MIXED);
    debug_assert!(
        aggstate.aggstrategy == AggStrategy::AggHashed
            || aggstate.aggstrategy == AggStrategy::AggMixed
    );

    // phase = (AGG_HASHED) ? &phases[0] : &phases[1]   /* AGG_MIXED */
    let phase_idx = if aggstate.aggstrategy == AggStrategy::AggHashed {
        0usize
    } else {
        1usize
    };

    let cache_empty = aggstate.phases.as_ref().expect("phases")[phase_idx].evaltrans_cache[i][j]
        .is_none();

    if cache_empty {
        // dohash = true; dosort = (AGG_MIXED && !minslot);
        let _dohash = true;
        let _dosort = aggstate.aggstrategy == AggStrategy::AggMixed && !minslot;

        // Builds a fresh aggregate-transition expression via ExecBuildAggTrans(),
        // temporarily swapping ss.ps.outerops to &TTSOpsMinimalTuple when
        // minslot. ExecBuildAggTrans is owned by the unported execExpr unit and
        // has no seam declaration. Loud panic.
        let _ = estate;
        panic!(
            "backend-executor-execExpr: ExecBuildAggTrans not yet ported \
             (hashagg_recompile_expressions)"
        );
    }

    // phase->evaltrans = phase->evaltrans_cache[i][j];
    let phase = &mut aggstate.phases.as_mut().expect("phases")[phase_idx];
    phase.evaltrans = phase.evaltrans_cache[i][j].take();
    Ok(())
}

/// `hash_create_memory(aggstate)` — create the `hash_metacxt` / `hash_tablecxt`
/// memory contexts that hold the hash tables and their entries.
pub fn hash_create_memory<'mcx>(aggstate: &mut AggStateData<'mcx>) -> PgResult<()> {
    // C:
    //   aggstate->hashcontext = CreateWorkExprContext(es->state);
    //   aggstate->hash_metacxt = AllocSetContextCreate(es_query_cxt, "HashAgg meta context", ...);
    //   maxBlockSize = pg_prevpower2_size_t(work_mem * 1024 / 16);
    //   maxBlockSize = Min(maxBlockSize, ALLOCSET_DEFAULT_MAXSIZE);
    //   maxBlockSize = Max(maxBlockSize, ALLOCSET_DEFAULT_INITSIZE);
    //   aggstate->hash_tablecxt = BumpContextCreate(es_query_cxt, "HashAgg table context", ...);
    //
    // CreateWorkExprContext (execUtils) has no seam, and the work_mem GUC that
    // sizes the bump block is not a dependency of this unit. Loud panic until
    // they are reachable.
    let _ = aggstate;
    panic!(
        "backend-executor-execUtils: CreateWorkExprContext / work_mem sizing not yet \
         available (hash_create_memory)"
    );
}

/// `hash_choose_num_buckets(hashentrysize, ngroups, memory)` — choose a bucket
/// count that keeps the estimated table within the memory budget.
pub fn hash_choose_num_buckets(hashentrysize: f64, ngroups: i64, memory: usize) -> i64 {
    // long nbuckets = ngroups;
    let mut nbuckets: i64 = ngroups;

    // max_nbuckets = memory / hashentrysize;
    let mut max_nbuckets: i64 = (memory as f64 / hashentrysize) as i64;

    // Underestimating is better than overestimating: max_nbuckets >>= 1;
    max_nbuckets >>= 1;

    if nbuckets > max_nbuckets {
        nbuckets = max_nbuckets;
    }

    // return Max(nbuckets, 1);
    nbuckets.max(1)
}

/// `hash_choose_num_partitions(input_groups, hashentrysize, used_bits,
/// &log2_npartitions)` — choose the number of spill partitions (a power of
/// two) and report its log2.
pub fn hash_choose_num_partitions(
    input_groups: f64,
    hashentrysize: f64,
    used_bits: i32,
) -> (i32, i32) {
    // Size hash_mem_limit = get_hash_memory_limit();
    //
    // get_hash_memory_limit() reads the work_mem / hash_mem_multiplier GUCs
    // (utils/misc/guc), which this unit does not depend on and for which no seam
    // is declared. The arithmetic below is faithful, but cannot run without that
    // value. Loud panic until the GUC surface is reachable.
    //
    // Faithful body (for reference, runs once hash_mem_limit is available):
    //   partition_limit = (hash_mem_limit * 0.25 - HASHAGG_READ_BUFFER_SIZE)
    //                     / HASHAGG_WRITE_BUFFER_SIZE;
    //   mem_wanted = HASHAGG_PARTITION_FACTOR * input_groups * hashentrysize;
    //   dpartitions = 1 + (mem_wanted / hash_mem_limit);
    //   if (dpartitions > partition_limit) dpartitions = partition_limit;
    //   if (dpartitions < HASHAGG_MIN_PARTITIONS) dpartitions = HASHAGG_MIN_PARTITIONS;
    //   if (dpartitions > HASHAGG_MAX_PARTITIONS) dpartitions = HASHAGG_MAX_PARTITIONS;
    //   npartitions = (int) dpartitions;
    //   partition_bits = my_log2(npartitions);
    //   if (partition_bits + used_bits >= 32) partition_bits = 32 - used_bits;
    //   npartitions = 1 << partition_bits;
    //   return (npartitions, partition_bits);
    let _ = (
        input_groups,
        hashentrysize,
        used_bits,
        HASHAGG_PARTITION_FACTOR,
        HASHAGG_MIN_PARTITIONS,
        HASHAGG_MAX_PARTITIONS,
        HASHAGG_READ_BUFFER_SIZE,
        HASHAGG_WRITE_BUFFER_SIZE,
    );
    let _ = my_log2 as fn(i64) -> i32;
    panic!(
        "backend-utils-misc-guc: get_hash_memory_limit (work_mem/hash_mem_multiplier) \
         not yet reachable (hash_choose_num_partitions)"
    );
}

/// `initialize_hash_entry(aggstate, hashtable, entry)` — initialize a freshly
/// created hash entry's per-group transition values.
pub fn initialize_hash_entry<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    hashtable: TupleHashTableHandle,
    entry: TupleHashEntryHandle,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let mcx = estate_mcx(estate);

    // aggstate->hash_ngroups_current++;
    aggstate.hash_ngroups_current += 1;

    // hash_agg_check_limits(aggstate);
    crate::spill::hash_agg_check_limits(aggstate, estate, mcx)?;

    // no need to allocate or initialize per-group state
    if aggstate.numtrans == 0 {
        return Ok(());
    }

    // pergroup = (AggStatePerGroup) TupleHashEntryGetAdditional(hashtable, entry);
    // for (transno = 0; transno < numtrans; transno++)
    //     initialize_aggregate(aggstate, &pertrans[transno], &pergroup[transno]);
    //
    // The per-group AggStatePerGroupData array lives in the entry's additional
    // space, owned by the unported execGrouping unit, which the seam exposes as
    // raw &mut [u8] via a callback (no &'static mut). Driving initialize_aggregate
    // over that aliased storage — and holding &mut pertrans simultaneously —
    // requires execGrouping's concrete entry layout to be a real type. Loud
    // panic until execGrouping lands.
    let _ = (hashtable, entry, mcx);
    panic!(
        "backend-executor-execGrouping: TupleHashEntryGetAdditional additional layout \
         not yet a real type (initialize_hash_entry)"
    );
}

/// `lookup_hash_entries(aggstate)` — probe every grouping set's hash table for
/// the current input tuple, creating entries as needed (or routing the tuple
/// to spill when in spill mode), and stash the per-group pointers in
/// `hash_pergroup`.
pub fn lookup_hash_entries<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // TupleTableSlot *outerslot = aggstate->tmpcontext->ecxt_outertuple;
    let outerslot = aggstate
        .tmpcontext
        .as_ref()
        .expect("tmpcontext")
        .ecxt_outertuple
        .expect("ecxt_outertuple");

    let num_hashes = aggstate.num_hashes;

    for setno in 0..num_hashes {
        let (hashtable, hashslot) = {
            let perhash = &aggstate.perhash.as_ref().expect("perhash")[setno as usize];
            (
                perhash.hashtable.expect("perhash->hashtable"),
                perhash.hashslot.expect("perhash->hashslot"),
            )
        };

        // p_isnew = aggstate->hash_spill_mode ? NULL : &isnew;
        let want_new = !aggstate.hash_spill_mode;

        // select_current_set(aggstate, setno, true);
        crate::node_lifecycle::select_current_set(aggstate, setno, true);

        // prepare_hash_slot(perhash, outerslot, hashslot);
        prepare_hash_slot(aggstate, setno, outerslot, hashslot, estate)?;

        if want_new {
            // entry = LookupTupleHashEntry(hashtable, hashslot, &isnew, &hash);
            let (entry, isnew, _hash) =
                backend_executor_execGrouping_seams::lookup_tuple_hash_entry::call(
                    hashtable, hashslot,
                )?;
            // creation allowed → entry is always non-NULL
            if isnew {
                initialize_hash_entry(aggstate, hashtable, entry, estate)?;
            }
            // pergroup[setno] = TupleHashEntryGetAdditional(hashtable, entry);
            //
            // Stashing the per-group pointer caches a view of the
            // execGrouping-owned additional bytes into hash_pergroup[setno];
            // that aliasing needs the real entry type. Loud panic.
            let _ = entry;
            panic!(
                "backend-executor-execGrouping: hash_pergroup pointer into entry \
                 additional space needs the real entry type (lookup_hash_entries)"
            );
        } else {
            // Spill mode: LookupTupleHashEntry with p_isnew == NULL (no create);
            // a miss spills the tuple via hashagg_spill_tuple. The execGrouping
            // seam has no no-create lookup variant, and the spill path operates
            // on slot value images that execTuples owns. Loud panic until both
            // land.
            let _ = hashtable;
            panic!(
                "backend-executor-execGrouping: no-create LookupTupleHashEntry \
                 (spill mode) not yet declared (lookup_hash_entries)"
            );
        }
    }

    Ok(())
}

/// `agg_fill_hash_table(aggstate)` — first pass over the input that fills the
/// hash tables (spilling when the memory limit is hit).
pub fn agg_fill_hash_table<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // for (;;) { outerslot = fetch_input_tuple(aggstate); if (TupIsNull) break; ... }
    loop {
        let outerslot = crate::node_lifecycle::fetch_input_tuple(aggstate, estate)?;
        let outerslot = match outerslot {
            Some(s) => s,
            None => break,
        };

        // tmpcontext->ecxt_outertuple = outerslot;
        aggstate
            .tmpcontext
            .as_mut()
            .expect("tmpcontext")
            .ecxt_outertuple = Some(outerslot);

        // Find or build hashtable entries
        lookup_hash_entries(aggstate, estate)?;

        // Advance the aggregates (or combine functions)
        crate::transition::advance_aggregates(aggstate, estate)?;

        // ResetExprContext(aggstate->tmpcontext);
        reset_tmpcontext(aggstate);
    }

    // finalize spills, if any
    crate::spill::hashagg_finish_initial_spills(aggstate, estate_mcx(estate))?;

    aggstate.table_filled = true;

    // select_current_set(aggstate, 0, true);
    crate::node_lifecycle::select_current_set(aggstate, 0, true);

    // ResetTupleHashIterator(perhash[0].hashtable, &perhash[0].hashiter);
    let table0 = aggstate.perhash.as_ref().expect("perhash")[0]
        .hashtable
        .expect("perhash[0]->hashtable");
    let iter = backend_executor_execGrouping_seams::init_tuple_hash_iterator::call(table0);
    aggstate.perhash.as_mut().expect("perhash")[0].hashiter = iter;

    Ok(())
}

/// `agg_refill_hash_table(aggstate)` — process one spilled batch: rebuild the
/// hash table from a spill tape, re-spilling if it again overflows. Returns
/// false when there are no more batches.
pub fn agg_refill_hash_table<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    // if (aggstate->hash_batches == NIL) return false;
    let has_batches = aggstate
        .hash_batches
        .as_ref()
        .map(|b| !b.is_empty())
        .unwrap_or(false);
    if !has_batches {
        return Ok(false);
    }

    // batch = llast(hash_batches); hash_batches = list_delete_last(hash_batches);
    let batch = {
        let batches = aggstate.hash_batches.as_mut().expect("hash_batches");
        let b = batches.pop().expect("non-empty");
        *b // HashAggBatch is Copy
    };

    // hash_agg_set_limits(hashentrysize, input_card, used_bits,
    //                     &hash_mem_limit, &hash_ngroups_limit, NULL);
    let (mem_limit, ngroups_limit, _np) =
        crate::spill::hash_agg_set_limits(aggstate.hashentrysize, batch.input_card, batch.used_bits);
    aggstate.hash_mem_limit = mem_limit;
    aggstate.hash_ngroups_limit = ngroups_limit;

    // MemSet(hash_pergroup, 0, sizeof(AggStatePerGroup) * num_hashes);
    if let Some(hp) = aggstate.hash_pergroup.as_mut() {
        for slot in hp.iter_mut() {
            *slot = None;
        }
    }

    // ReScanExprContext(hashcontext); MemoryContextReset(hash_tablecxt);
    rescan_hashcontext(aggstate);
    if let Some(tablecxt) = aggstate.hash_tablecxt.as_mut() {
        tablecxt.reset();
    }
    // for setno: ResetTupleHashTable(perhash[setno].hashtable);
    let num_hashes = aggstate.num_hashes;
    for setno in 0..num_hashes {
        let table = aggstate.perhash.as_ref().expect("perhash")[setno as usize]
            .hashtable
            .expect("hashtable");
        backend_executor_execGrouping_seams::reset_tuple_hash_table::call(table)?;
    }

    aggstate.hash_ngroups_current = 0;

    // Assert(current_phase == 0);
    debug_assert_eq!(aggstate.current_phase, 0);

    // AGG_MIXED: switch to phase 1 while processing the batch.
    let is_mixed = {
        aggstate.phases.as_ref().expect("phases")[aggstate.phase as usize].aggstrategy
            == AggStrategy::AggMixed
    };
    if is_mixed {
        aggstate.current_phase = 1;
        aggstate.phase = aggstate.current_phase;
    }

    // select_current_set(aggstate, batch->setno, true);
    crate::node_lifecycle::select_current_set(aggstate, batch.setno, true);

    // perhash = &aggstate->perhash[aggstate->current_set];
    // hashagg_recompile_expressions(aggstate, true, true);
    hashagg_recompile_expressions(aggstate, true, true, estate)?;

    // The per-tuple refill loop reads spilled MinimalTuples
    // (hashagg_batch_read), stores them with ExecStoreMinimalTuple, prepares
    // the hash slot, looks them up by precomputed hash, and either advances the
    // aggregates or re-spills; then closes the input tape, runs
    // hashagg_spill_finish / hash_agg_update_metrics, clears hash_spill_mode,
    // and resets the iterator. ExecStoreMinimalTuple is owned by the unported
    // execTuples unit (no seam; slot value arrays absent from the shared
    // vocabulary). Loud panic until that surface lands.
    let _ = (mem_limit, ngroups_limit);
    panic!(
        "backend-executor-execTuples: ExecStoreMinimalTuple for spilled batch not yet \
         ported (agg_refill_hash_table)"
    );
}

/// `agg_retrieve_hash_table(aggstate)` — the hashed-grouping driver: emit
/// results from the in-memory tables, then refill and emit from spilled
/// batches until exhausted. Returns `None` at end.
pub fn agg_retrieve_hash_table<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    // TupleTableSlot *result = NULL;
    let mut result: Option<SlotId> = None;

    // while (result == NULL) { ... }
    while result.is_none() {
        result = agg_retrieve_hash_table_in_memory(aggstate, estate)?;
        if result.is_none() {
            if !agg_refill_hash_table(aggstate, estate)? {
                aggstate.agg_done = true;
                break;
            }
        }
    }

    Ok(result)
}

/// `agg_retrieve_hash_table_in_memory(aggstate)` — iterate the current
/// in-memory hash tables, finalizing and projecting each group's result.
pub fn agg_retrieve_hash_table_in_memory<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    // for (;;) {
    //   CHECK_FOR_INTERRUPTS();
    //   entry = ScanTupleHashTable(hashtable, &perhash->hashiter);
    //   if (entry == NULL) { switch to next set & restart, or return NULL; }
    //   ResetExprContext(econtext);
    //   ExecStoreMinimalTuple(TupleHashEntryGetTuple(entry), hashslot, false);
    //   slot_getallattrs(hashslot);
    //   ExecClearTuple(firstSlot);
    //   memset(firstSlot->tts_isnull, true, natts);
    //   for (i ...) firstSlot->tts_values/isnull[varNumber] = hashslot[i];
    //   ExecStoreVirtualTuple(firstSlot);
    //   pergroup = TupleHashEntryGetAdditional(hashtable, entry);
    //   econtext->ecxt_outertuple = firstSlot;
    //   prepare_projection_slot(aggstate, firstSlot, current_set);
    //   finalize_aggregates(aggstate, peragg, pergroup);
    //   result = project_aggregates(aggstate);
    //   if (result) return result;
    // }
    //
    // The interrupt check and ScanTupleHashTable are available as seams, but
    // the body reconstructs the representative tuple by poking
    // firstSlot/hashslot tts_values/tts_isnull arrays (execTuples-owned, absent
    // from the shared vocabulary) and reinterprets the entry's additional bytes
    // as the per-group array (execGrouping-owned real entry type). Both are
    // unported with no seam. Loud panic until they land.
    backend_tcop_postgres_seams::check_for_interrupts::call()?;
    let _ = (aggstate, estate);
    panic!(
        "backend-executor-execTuples/execGrouping: representative-tuple slot poking + \
         entry additional layout not yet ported (agg_retrieve_hash_table_in_memory)"
    );
}

/// `hash_agg_entry_size(numTrans, tupleWidth, transitionSpace)` — estimate the
/// per-group hash-entry size, used by the planner and `build_hash_tables`.
pub fn hash_agg_entry_size(num_trans: i32, tuple_width: usize, transition_space: usize) -> usize {
    // tupleSize = MAXALIGN(SizeofMinimalTupleHeader) + tupleWidth;
    // pergroupSize = numTrans * sizeof(AggStatePerGroupData);
    // tupleChunkSize = MAXALIGN(tupleSize);
    // pergroupChunkSize = pergroupSize;
    // transitionChunkSize = (transitionSpace > 0)
    //     ? CHUNKHDRSZ + pg_nextpower2_size_t(transitionSpace) : 0;
    // return TupleHashEntrySize() + tupleChunkSize + pergroupChunkSize + transitionChunkSize;
    //
    // TupleHashEntrySize() is an execGrouping inline (sizeof TupleHashEntryData)
    // with no seam and no real type to size; the rest is faithful arithmetic.
    // Loud panic until execGrouping lands.
    let _pergroup_size = num_trans as usize * core::mem::size_of::<AggStatePerGroupData>();
    let _transition_chunk = if transition_space > 0 {
        CHUNKHDRSZ + pg_nextpower2_size_t(transition_space)
    } else {
        0
    };
    let _ = tuple_width;
    panic!(
        "backend-executor-execGrouping: TupleHashEntrySize not yet a real type \
         (hash_agg_entry_size)"
    );
}

// ---------------------------------------------------------------------------
// Local helpers for the owned-context operations C inlines as macros.
// ---------------------------------------------------------------------------

/// `ResetExprContext(aggstate->tmpcontext)` — reset the per-input-tuple memory
/// of the temp context.
fn reset_tmpcontext(aggstate: &mut AggStateData<'_>) {
    if let Some(tmp) = aggstate.tmpcontext.as_mut() {
        tmp.ecxt_per_tuple_memory.reset();
    }
}

/// `ReScanExprContext(aggstate->hashcontext)` — reset the hashcontext's
/// per-tuple memory (the byref-transvalue arena).
fn rescan_hashcontext(aggstate: &mut AggStateData<'_>) {
    if let Some(hc) = aggstate.hashcontext.as_mut() {
        hc.ecxt_per_tuple_memory.reset();
    }
}

/// The per-query context handle, target for transient allocations the
/// hash-grouping path hands to `mcx`-taking siblings/seams.
fn estate_mcx<'mcx>(estate: &EStateData<'mcx>) -> Mcx<'mcx> {
    estate.es_query_cxt
}
