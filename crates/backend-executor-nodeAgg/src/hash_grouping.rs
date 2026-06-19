//! Hash-grouping family: building and probing the per-grouping-set tuple hash
//! tables, the in-memory and refill retrieve paths, the recompiled transition
//! expressions for hashed input, and the bucket/partition sizing helpers.

use backend_executor_nodeHash_seams as nodeHash_seams;
use mcx::Mcx;
use types_error::PgResult;
use types_nodes::nodeagg::{do_aggsplit_skipfinal, AggStrategy, TupleHashEntryData};
use crate::aggstate::{AggStateData, AggStatePerGroupData};
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
    // Read each needed input column (slot_getsomeattr deforms up to that attr)
    // and assemble the hash slot's numhashGrpCols-wide virtual tuple via
    // store_virtual_values (ExecClearTuple + per-column fill + StoreVirtual).
    let (num_cols, idx_input) = {
        let perhash = &aggstate.perhash.as_ref().expect("perhash")[perhash_idx as usize];
        let num_cols = perhash.numhash_grp_cols;
        let src = perhash
            .hash_grp_col_idx_input
            .as_ref()
            .expect("perhash->hashGrpColIdxInput");
        let mut idx_input = mcx::vec_with_capacity_in(estate.es_query_cxt, src.len())?;
        for &v in src.iter() {
            idx_input.push(v);
        }
        (num_cols, idx_input)
    };

    let mut values =
        mcx::vec_with_capacity_in(estate.es_query_cxt, num_cols.max(0) as usize)?;
    let mut isnull =
        mcx::vec_with_capacity_in(estate.es_query_cxt, num_cols.max(0) as usize)?;
    for i in 0..num_cols as usize {
        // varNumber = perhash->hashGrpColIdxInput[i] - 1; (1-based attr = +1)
        let attno = idx_input[i] as i32;
        let (val, null) =
            backend_executor_execTuples_seams::slot_getsomeattr::call(estate, inputslot, attno)?;
        values.push(val);
        isnull.push(null);
    }

    backend_executor_execTuples_seams::store_virtual_values::call(
        estate,
        hashslot,
        values.as_slice(),
        isnull.as_slice(),
    )?;
    Ok(())
}

/// `build_hash_tables(aggstate)` — (re)create the tuple hash table for every
/// grouping set, sizing buckets from the planned group counts and memory.
pub fn build_hash_tables<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let num_hashes = aggstate.num_hashes;

    for setno in 0..num_hashes {
        let (has_existing, num_groups) = {
            let perhash = &aggstate.perhash.as_ref().expect("perhash array")[setno as usize];
            let num_groups = perhash
                .aggnode
                .as_ref()
                .expect("perhash->aggnode")
                .num_groups;
            (perhash.hashtable.is_some(), num_groups)
        };

        if has_existing {
            // ResetTupleHashTable(perhash->hashtable);
            let hashtable = aggstate.perhash.as_mut().expect("perhash array")[setno as usize]
                .hashtable
                .as_mut()
                .expect("perhash->hashtable");
            backend_executor_execGrouping_seams::reset_tuple_hash_table::call(&mut **hashtable)?;
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
    let additionalsize =
        aggstate.numtrans as usize * core::mem::size_of::<AggStatePerGroupData<'_>>();

    // use_variable_hash_iv = DO_AGGSPLIT_SKIPFINAL(aggstate->aggsplit);
    let use_variable_hash_iv = do_aggsplit_skipfinal(aggstate.aggsplit);

    let mcx = estate.es_query_cxt;

    // Read the per-hash key descriptors and the hashslot's tuple descriptor.
    let (num_cols, hashslot, idx_hash, eqfuncoids, hashfunctions, grp_collations) = {
        let perhash = &aggstate.perhash.as_ref().expect("perhash")[setno as usize];
        let hashslot = perhash.hashslot.expect("perhash->hashslot");
        let mut idx_hash = mcx::vec_with_capacity_in(
            mcx,
            perhash
                .hash_grp_col_idx_hash
                .as_ref()
                .map(|v| v.len())
                .unwrap_or(0),
        )?;
        for &v in perhash
            .hash_grp_col_idx_hash
            .as_ref()
            .expect("perhash->hashGrpColIdxHash")
            .iter()
        {
            idx_hash.push(v);
        }
        let mut eqfuncoids =
            mcx::vec_with_capacity_in(mcx, perhash.eqfuncoids.as_ref().map(|v| v.len()).unwrap_or(0))?;
        for &o in perhash.eqfuncoids.as_ref().expect("perhash->eqfuncoids").iter() {
            eqfuncoids.push(o);
        }
        let mut hashfunctions = mcx::vec_with_capacity_in(
            mcx,
            perhash.hashfunctions.as_ref().map(|v| v.len()).unwrap_or(0),
        )?;
        for f in perhash.hashfunctions.as_ref().expect("perhash->hashfunctions").iter() {
            hashfunctions.push(f.clone());
        }
        let mut grp_collations = {
            let aggnode = perhash.aggnode.as_ref().expect("perhash->aggnode");
            let src = aggnode
                .grp_collations
                .as_ref()
                .expect("perhash->aggnode->grpCollations");
            let mut v = mcx::vec_with_capacity_in(mcx, src.len())?;
            for &c in src.iter() {
                v.push(c);
            }
            v
        };
        let _ = &mut grp_collations;
        (
            perhash.num_cols,
            hashslot,
            idx_hash,
            eqfuncoids,
            hashfunctions,
            grp_collations,
        )
    };

    // perhash->hashslot->tts_tupleDescriptor / tts_ops (MinimalTuple slot).
    let hash_desc =
        backend_executor_execTuples_seams::exec_slot_descriptor::call(mcx, estate, hashslot)?;

    // tmpcxt = aggstate->tmpcontext->ecxt_per_tuple_memory; — the per-tuple
    // context of the node's ExprContext (an EcxtId in the EState pool).
    let tmpcontext = aggstate.tmpcontext.expect("tmpcontext");

    // The three contexts (metacxt = hash_metacxt, tablecxt = hash_tablecxt,
    // tmpcxt) are caller-owned; the table borrows them.
    let table = {
        let tmpcxt: &mcx::MemoryContext = &estate.ecxt(tmpcontext).ecxt_per_tuple_memory;
        let metacxt = aggstate
            .hash_metacxt
            .as_ref()
            .expect("aggstate->hash_metacxt");
        let tablecxt = aggstate
            .hash_tablecxt
            .as_ref()
            .expect("aggstate->hash_tablecxt");
        backend_executor_execGrouping_seams::build_tuple_hash_table::call(
            mcx,
            None,
            hash_desc,
            types_nodes::TupleSlotKind::MinimalTuple,
            num_cols,
            idx_hash.as_slice(),
            eqfuncoids.as_slice(),
            hashfunctions.as_slice(),
            grp_collations.as_slice(),
            nbuckets,
            additionalsize,
            metacxt,
            tablecxt,
            tmpcxt,
            use_variable_hash_iv,
        )?
    };

    aggstate.perhash.as_mut().expect("perhash")[setno as usize].hashtable = Some(table);
    Ok(())
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
        // minslot. ExecBuildAggTrans IS ported (execExpr_domain_agg::
        // exec_build_agg_trans), but it takes `&mut AggStateData<'mcx>` — and
        // AggStateData lives in this nodeAgg crate, ABOVE types-nodes, so the
        // backend-executor-execExpr-seams crate (which nodeAgg depends on) cannot
        // name it without re-introducing the cycle the seam breaks. The
        // type-erased bridge (`PlanStateNode::as_agg_state`) is also unavailable:
        // there is no `PlanStateNode::Agg` variant yet (planstate.rs returns None
        // — the T_Agg keystone). So the owner body exists but is unreachable from
        // here. Blocked on the same T_Agg/PlanStateNode::Agg carrier keystone.
        let _ = estate;
        panic!(
            "backend-executor-execExpr::ExecBuildAggTrans: owner is ported but unreachable — \
             takes &mut AggStateData (above types-nodes, can't cross execExpr-seams) and \
             PlanStateNode has no Agg variant (T_Agg keystone). (hashagg_recompile_expressions)"
        );
    }

    // phase->evaltrans = phase->evaltrans_cache[i][j];
    let phase = &mut aggstate.phases.as_mut().expect("phases")[phase_idx];
    phase.evaltrans = phase.evaltrans_cache[i][j].take();
    Ok(())
}

/// `hash_create_memory(aggstate)` — create the `hash_metacxt` / `hash_tablecxt`
/// memory contexts that hold the hash tables and their entries.
pub fn hash_create_memory<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // C:
    //   aggstate->hashcontext = CreateWorkExprContext(es->state);
    //   aggstate->hash_metacxt = AllocSetContextCreate(es_query_cxt, "HashAgg meta context", ...);
    //   maxBlockSize = pg_prevpower2_size_t(work_mem * 1024 / 16);
    //   maxBlockSize = Min(maxBlockSize, ALLOCSET_DEFAULT_MAXSIZE);
    //   maxBlockSize = Max(maxBlockSize, ALLOCSET_DEFAULT_INITSIZE);
    //   aggstate->hash_tablecxt = BumpContextCreate(es_query_cxt, "HashAgg table context", ...);
    //
    // aggstate->hashcontext = CreateWorkExprContext(es->state): the work-sized
    // ExprContext is registered in the EState pool, and (#165 P0) hashcontext is
    // now an EcxtId, so the assignment is the faithful id store.
    let work_mem_kb = backend_utils_init_small_seams::work_mem::call();
    aggstate.hashcontext = Some(
        backend_executor_execUtils_seams::create_work_expr_context::call(estate, work_mem_kb)?,
    );

    // aggstate->hash_metacxt = AllocSetContextCreate(es_query_cxt,
    //                                                "HashAgg meta context",
    //                                                ALLOCSET_DEFAULT_SIZES);
    //
    // The meta context holds the bucket array(s) of TupleHashEntryData; it
    // doubles as the table grows and frees the old array, so an AllocSet
    // (malloc-backed) child of the per-query context is the faithful backend.
    let query_cxt = estate.es_query_cxt.context();
    aggstate.hash_metacxt = Some(query_cxt.new_child("HashAgg meta context"));

    // aggstate->hash_tablecxt = BumpContextCreate(es_query_cxt,
    //                                             "HashAgg table context", ...);
    //
    // The hash entries (grouping key firstTuple + pergroup data) live in the
    // table context. The bump allocator is used because entries are not freed
    // until the whole table is reset, so a bump-arena child of the per-query
    // context is the faithful backend. The C sizes the bump's maxBlockSize off
    // work_mem (pg_prevpower2_size_t(work_mem*1024/16), clamped to
    // [ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE]); block sizing is
    // internal to bumpalo in this model, so the budget is not a parameter here.
    aggstate.hash_tablecxt = Some(query_cxt.new_child_bump("HashAgg table context"));

    Ok(())
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
    let hash_mem_limit = nodeHash_seams::get_hash_memory_limit::call()
        .expect("get_hash_memory_limit (nodeHash.c) does not ereport")
        as f64;

    // Avoid creating so many partitions that the memory requirements of the
    // open partition files are greater than 1/4 of hash_mem.
    //   partition_limit = (hash_mem_limit * 0.25 - HASHAGG_READ_BUFFER_SIZE)
    //                     / HASHAGG_WRITE_BUFFER_SIZE;
    let partition_limit = (hash_mem_limit * 0.25 - HASHAGG_READ_BUFFER_SIZE as f64)
        / HASHAGG_WRITE_BUFFER_SIZE as f64;

    // mem_wanted = HASHAGG_PARTITION_FACTOR * input_groups * hashentrysize;
    let mem_wanted = HASHAGG_PARTITION_FACTOR * input_groups * hashentrysize;

    // make enough partitions so that each one is likely to fit in memory
    //   dpartitions = 1 + (mem_wanted / hash_mem_limit);
    let mut dpartitions = 1.0 + (mem_wanted / hash_mem_limit);

    if dpartitions > partition_limit {
        dpartitions = partition_limit;
    }

    if dpartitions < HASHAGG_MIN_PARTITIONS as f64 {
        dpartitions = HASHAGG_MIN_PARTITIONS as f64;
    }
    if dpartitions > HASHAGG_MAX_PARTITIONS as f64 {
        dpartitions = HASHAGG_MAX_PARTITIONS as f64;
    }

    // HASHAGG_MAX_PARTITIONS limit makes this safe
    let mut npartitions = dpartitions as i32;

    // ceil(log2(npartitions))
    let mut partition_bits = my_log2(npartitions as i64);

    // make sure that we don't exhaust the hash bits
    if partition_bits + used_bits >= 32 {
        partition_bits = 32 - used_bits;
    }

    // number of partitions will be a power of two
    npartitions = 1 << partition_bits;

    (npartitions, partition_bits)
}

/// `initialize_hash_entry(aggstate, hashtable, entry)` — initialize a freshly
/// created hash entry's per-group transition values.
pub fn initialize_hash_entry<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    entry: &mut TupleHashEntryData<'mcx>,
    additional: &mut [u8],
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
    let _ = (entry, additional, mcx);
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
    // tmpcontext is an EcxtId; resolve it through the EState pool.
    let tmpcontext = aggstate.tmpcontext.expect("tmpcontext");
    let outerslot = estate
        .ecxt(tmpcontext)
        .ecxt_outertuple
        .expect("ecxt_outertuple");

    let num_hashes = aggstate.num_hashes;

    for setno in 0..num_hashes {
        let hashslot = aggstate.perhash.as_ref().expect("perhash")[setno as usize]
            .hashslot
            .expect("perhash->hashslot");

        // p_isnew = aggstate->hash_spill_mode ? NULL : &isnew;
        let want_new = !aggstate.hash_spill_mode;

        // select_current_set(aggstate, setno, true);
        crate::node_lifecycle::select_current_set(aggstate, setno, true);

        // prepare_hash_slot(perhash, outerslot, hashslot);
        prepare_hash_slot(aggstate, setno, outerslot, hashslot, estate)?;

        if want_new {
            // entry = LookupTupleHashEntry(hashtable, hashslot, &isnew, &hash);
            //
            // The seam finds/creates the entry and reports whether it is new.
            // When new the C runs initialize_hash_entry; then it caches the
            // per-group pointer (TupleHashEntryGetAdditional) in
            // hash_pergroup[setno]. In the owned model the entry's additional
            // bytes are execGrouping-owned and cannot be aliased into a typed
            // `AggStatePerGroup` cache held alongside a live table borrow. For
            // numtrans == 0 (hashed DISTINCT / set-op dedup / DISTINCT-only
            // grouping) there is no per-group transition state, so the cache is
            // an empty array and the divergence is inert: the lookup itself does
            // the dedup. For numtrans > 0 the typed-additional aliasing is a
            // genuine keystone (advance_aggregates mutates per-group state in
            // place inside the entry); loud-panic there.
            let isnew = {
                let hashtable = aggstate.perhash.as_mut().expect("perhash")[setno as usize]
                    .hashtable
                    .as_mut()
                    .expect("perhash->hashtable");
                let (isnew, _hash) =
                    backend_executor_execGrouping_seams::lookup_tuple_hash_entry::call(
                        &mut **hashtable,
                        hashslot,
                        estate,
                        &mut |_entry, _additional| {},
                    )?;
                isnew
            };

            if aggstate.numtrans == 0 {
                // No per-group state: initialize_hash_entry only bumps the group
                // counter / checks limits, and the per-group pointer is unused.
                if isnew {
                    aggstate.hash_ngroups_current += 1;
                    crate::spill::hash_agg_check_limits(aggstate, estate, estate_mcx(estate))?;
                }
                if let Some(hp) = aggstate.hash_pergroup.as_mut() {
                    hp[setno as usize] = Some(mcx::PgVec::new_in(estate.es_query_cxt));
                }
            } else {
                let _ = isnew;
                panic!(
                    "backend-executor-execGrouping: hash_pergroup pointer into entry \
                     additional space (typed AggStatePerGroup aliasing) needs in-place \
                     per-group state for numtrans > 0 (lookup_hash_entries)"
                );
            }
        } else {
            // Spill mode: LookupTupleHashEntryHash with create == false (the C
            // passes p_isnew == NULL); a miss spills the tuple via
            // hashagg_spill_tuple. The spill path operates on slot value images
            // that execTuples owns. Loud panic until that surface lands.
            let _ = setno;
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
        let tmpcontext = aggstate.tmpcontext.expect("tmpcontext");
        estate.ecxt_mut(tmpcontext).ecxt_outertuple = Some(outerslot);

        // Find or build hashtable entries
        lookup_hash_entries(aggstate, estate)?;

        // Advance the aggregates (or combine functions)
        crate::transition::advance_aggregates(aggstate, estate)?;

        // ResetExprContext(aggstate->tmpcontext);
        reset_tmpcontext(aggstate, estate)?;
    }

    // finalize spills, if any
    crate::spill::hashagg_finish_initial_spills(aggstate, estate, estate_mcx(estate))?;

    aggstate.table_filled = true;

    // select_current_set(aggstate, 0, true);
    crate::node_lifecycle::select_current_set(aggstate, 0, true);

    // ResetTupleHashIterator(perhash[0].hashtable, &perhash[0].hashiter);
    let iter = {
        let table0 = aggstate.perhash.as_mut().expect("perhash")[0]
            .hashtable
            .as_mut()
            .expect("perhash[0]->hashtable");
        backend_executor_execGrouping_seams::init_tuple_hash_iterator::call(&mut **table0)
    };
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
    rescan_hashcontext(aggstate, estate)?;
    if let Some(tablecxt) = aggstate.hash_tablecxt.as_mut() {
        tablecxt.reset();
    }
    // for setno: ResetTupleHashTable(perhash[setno].hashtable);
    let num_hashes = aggstate.num_hashes;
    for setno in 0..num_hashes {
        let table = aggstate.perhash.as_mut().expect("perhash")[setno as usize]
            .hashtable
            .as_mut()
            .expect("hashtable");
        backend_executor_execGrouping_seams::reset_tuple_hash_table::call(&mut **table)?;
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
    // econtext = aggstate->ss.ps.ps_ExprContext;
    let econtext = aggstate
        .ss
        .ps
        .ps_ExprContext
        .expect("agg_retrieve_hash_table_in_memory: ps_ExprContext is NULL");
    // firstSlot = aggstate->ss.ss_ScanTupleSlot;
    let first_slot = aggstate
        .ss
        .ss_ScanTupleSlot
        .expect("agg_retrieve_hash_table_in_memory: ss_ScanTupleSlot is NULL");

    // firstSlot descriptor natts (for the all-null base then grouped-col fill).
    let first_natts = crate::exec_init_agg::scan_tuple_desc(aggstate, estate)?
        .as_deref()
        .map(|d| d.natts)
        .expect("agg_retrieve_hash_table_in_memory: scanDesc is NULL");

    loop {
        // CHECK_FOR_INTERRUPTS();
        backend_tcop_postgres_seams::check_for_interrupts::call()?;

        let setno = aggstate.current_set;
        let hashslot = aggstate.perhash.as_ref().expect("perhash")[setno as usize]
            .hashslot
            .expect("perhash->hashslot");

        // entry = ScanTupleHashTable(hashtable, &perhash->hashiter);
        let mut entry_tuple: Option<
            types_tuple::backend_access_common_heaptuple::FormedMinimalTuple<'mcx>,
        > = None;
        let found = {
            let mcx = estate.es_query_cxt;
            let mut hashiter = aggstate.perhash.as_ref().expect("perhash")[setno as usize].hashiter;
            let hashtable = aggstate.perhash.as_mut().expect("perhash")[setno as usize]
                .hashtable
                .as_mut()
                .expect("perhash->hashtable");
            let found = backend_executor_execGrouping_seams::scan_tuple_hash_table::call(
                &mut **hashtable,
                &mut hashiter,
                estate,
                &mut |entry, _additional| {
                    // TupleHashEntryGetTuple(entry) — group's first tuple. For
                    // numtrans == 0 there is no per-group state in `additional`.
                    entry_tuple = entry
                        .firstTuple
                        .as_ref()
                        .map(|m| m.clone_in(mcx).expect("clone hash entry tuple"));
                },
            )?;
            aggstate.perhash.as_mut().expect("perhash")[setno as usize].hashiter = hashiter;
            found
        };

        if !found {
            // Switch to the next grouping set, or finish.
            let nextset = aggstate.current_set + 1;
            if nextset < aggstate.num_hashes {
                crate::node_lifecycle::select_current_set(aggstate, nextset, true);
                let s = aggstate.current_set as usize;
                let iter = {
                    let table = aggstate.perhash.as_mut().expect("perhash")[s]
                        .hashtable
                        .as_mut()
                        .expect("perhash->hashtable");
                    backend_executor_execGrouping_seams::init_tuple_hash_iterator::call(
                        &mut **table,
                    )
                };
                aggstate.perhash.as_mut().expect("perhash")[s].hashiter = iter;
                continue;
            } else {
                return Ok(None);
            }
        }

        // ResetExprContext(econtext);
        backend_executor_execUtils_seams::reset_expr_context::call(estate, econtext)?;

        // Transform representative tuple back into one with the right columns:
        //   ExecStoreMinimalTuple(TupleHashEntryGetTuple(entry), hashslot, false);
        //   slot_getallattrs(hashslot);
        let mtup = entry_tuple.expect("scan callback captured the entry tuple");
        backend_executor_execTuples_seams::exec_store_minimal_tuple::call(
            estate, mtup, hashslot, false,
        )?;
        let hash_cols =
            backend_executor_execTuples_seams::slot_getallattrs_by_id::call(estate, hashslot)?;

        // ExecClearTuple(firstSlot);
        // memset(firstSlot->tts_isnull, true, natts);
        // for i in numhashGrpCols: firstSlot[varNumber] = hashslot[i];
        // ExecStoreVirtualTuple(firstSlot);
        let (num_hash_cols, idx_input) = {
            let perhash = &aggstate.perhash.as_ref().expect("perhash")[setno as usize];
            let n = perhash.numhash_grp_cols;
            let src = perhash
                .hash_grp_col_idx_input
                .as_ref()
                .expect("perhash->hashGrpColIdxInput");
            let mut idx = mcx::vec_with_capacity_in(estate.es_query_cxt, src.len())?;
            for &v in src.iter() {
                idx.push(v);
            }
            (n, idx)
        };
        let mut values: mcx::PgVec<'mcx, types_tuple::backend_access_common_heaptuple::Datum<'mcx>> =
            mcx::vec_with_capacity_in(estate.es_query_cxt, first_natts.max(0) as usize)?;
        let mut isnull = mcx::vec_with_capacity_in(estate.es_query_cxt, first_natts.max(0) as usize)?;
        for _ in 0..first_natts {
            values.push(types_tuple::backend_access_common_heaptuple::Datum::null());
            isnull.push(true);
        }
        for i in 0..num_hash_cols as usize {
            let var_number = idx_input[i] as usize - 1;
            values[var_number] = hash_cols[i].0.clone();
            isnull[var_number] = hash_cols[i].1;
        }
        backend_executor_execTuples_seams::store_virtual_values::call(
            estate,
            first_slot,
            values.as_slice(),
            isnull.as_slice(),
        )?;

        // econtext->ecxt_outertuple = firstSlot;
        estate.ecxt_mut(econtext).ecxt_outertuple = Some(first_slot);

        // prepare_projection_slot(aggstate, firstSlot, current_set);
        let current_set = aggstate.current_set;
        crate::finalize::prepare_projection_slot(aggstate, first_slot, current_set, estate)?;

        // finalize_aggregates(aggstate, peragg, pergroup);
        //
        // For numtrans > 0 the per-group transition values live in the entry's
        // additional bytes (typed-additional aliasing keystone); finalize reads
        // them. For numtrans == 0 (hashed DISTINCT / set-op dedup) there are no
        // aggregates to finalize, so this is a no-op and the projection emits the
        // grouping columns directly.
        if aggstate.numtrans == 0 {
            // result = project_aggregates(aggstate);
            if let Some(result) = crate::finalize::project_aggregates(aggstate, estate)? {
                return Ok(Some(result));
            }
        } else {
            panic!(
                "backend-executor-nodeAgg: finalize_aggregates over the entry's per-group \
                 additional bytes (typed AggStatePerGroup aliasing) for numtrans > 0 \
                 (agg_retrieve_hash_table_in_memory)"
            );
        }
    }
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
    // tupleSize = MAXALIGN(SizeofMinimalTupleHeader) + tupleWidth.
    let tuple_size = SizeofMinimalTupleHeader + tuple_width;
    let pergroup_size = num_trans as usize * core::mem::size_of::<AggStatePerGroupData<'_>>();

    // Entries use the Bump allocator, so chunk sizes equal requested sizes.
    let tuple_chunk_size = maxalign(tuple_size);
    let pergroup_chunk_size = pergroup_size;

    // Transition values use AllocSet: chunk header + power-of-two allocation.
    let transition_chunk_size = if transition_space > 0 {
        CHUNKHDRSZ + pg_nextpower2_size_t(transition_space)
    } else {
        0
    };

    // TupleHashEntrySize() == sizeof(TupleHashEntryData) (executor.h:165).
    core::mem::size_of::<TupleHashEntryData>()
        + tuple_chunk_size
        + pergroup_chunk_size
        + transition_chunk_size
}

/// `SizeofMinimalTupleHeader` == `offsetof(MinimalTupleData, t_bits)`
/// (`access/htup_details.h`) == `SizeofHeapTupleHeader - MINIMAL_TUPLE_OFFSET`.
const SizeofMinimalTupleHeader: usize =
    types_tuple::heap::SizeofHeapTupleHeader - types_tuple::heaptuple::MINIMAL_TUPLE_OFFSET;

/// `MAXALIGN(len)` — round up to the platform max alignment (8).
#[inline]
const fn maxalign(len: usize) -> usize {
    (len + 7) & !7
}

// ---------------------------------------------------------------------------
// Local helpers for the owned-context operations C inlines as macros.
// ---------------------------------------------------------------------------

/// `ResetExprContext(aggstate->tmpcontext)` — reset the per-input-tuple memory
/// of the temp context.
fn reset_tmpcontext<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // tmpcontext is an EcxtId into the EState pool; ResetExprContext resets its
    // per-tuple memory.
    if let Some(ecxt) = aggstate.tmpcontext {
        backend_executor_execUtils_seams::reset_expr_context::call(estate, ecxt)?;
    }
    Ok(())
}

/// `ReScanExprContext(aggstate->hashcontext)` — reset the hashcontext's
/// per-tuple memory (the byref-transvalue arena).
fn rescan_hashcontext<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    if let Some(ecxt) = aggstate.hashcontext {
        backend_executor_execUtils_seams::re_scan_expr_context::call(estate, ecxt)?;
    }
    Ok(())
}

/// The per-query context handle, target for transient allocations the
/// hash-grouping path hands to `mcx`-taking siblings/seams.
fn estate_mcx<'mcx>(estate: &EStateData<'mcx>) -> Mcx<'mcx> {
    estate.es_query_cxt
}
