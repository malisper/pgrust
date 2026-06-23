//! Hash-grouping family: building and probing the per-grouping-set tuple hash
//! tables, the in-memory and refill retrieve paths, the recompiled transition
//! expressions for hashed input, and the bucket/partition sizing helpers.

use nodeHash_seams as nodeHash_seams;
use ::mcx::Mcx;
use ::types_error::PgResult;
use ::nodes::nodeagg::{do_aggsplit_skipfinal, AggStrategy};
use crate::aggstate::{AggStateData, AggStatePerGroupData};
use ::nodes::{EStateData, SlotId};

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
        let mut idx_input = ::mcx::vec_with_capacity_in(estate.es_query_cxt, src.len())?;
        for &v in src.iter() {
            idx_input.push(v);
        }
        (num_cols, idx_input)
    };

    let mut values =
        ::mcx::vec_with_capacity_in(estate.es_query_cxt, num_cols.max(0) as usize)?;
    let mut isnull =
        ::mcx::vec_with_capacity_in(estate.es_query_cxt, num_cols.max(0) as usize)?;
    for i in 0..num_cols as usize {
        // varNumber = perhash->hashGrpColIdxInput[i] - 1; (1-based attr = +1)
        let attno = idx_input[i] as i32;
        let (val, null) =
            execTuples_seams::slot_getsomeattr::call(estate, inputslot, attno)?;
        values.push(val);
        isnull.push(null);
    }

    execTuples_seams::store_virtual_values::call(
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
            execGrouping_seams::reset_tuple_hash_table::call(&mut **hashtable)?;
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
    // Use C's ABI struct size (16B), not Rust's owned-`Datum`-enum `size_of`, so
    // the per-entry `additional` region and the resulting runtime hashentrysize /
    // spill-threshold memory accounting match C exactly. (The Rust port only
    // stores a 4-byte side-table index in this region; the size governs the
    // entry's MAXALIGN'd footprint, which must mirror C for memory accounting.)
    let additionalsize = aggstate.numtrans as usize * SIZEOF_AGGSTATEPERGROUPDATA;

    // use_variable_hash_iv = DO_AGGSPLIT_SKIPFINAL(aggstate->aggsplit);
    let use_variable_hash_iv = do_aggsplit_skipfinal(aggstate.aggsplit);

    let mcx = estate.es_query_cxt;

    // Read the per-hash key descriptors and the hashslot's tuple descriptor.
    let (num_cols, hashslot, idx_hash, eqfuncoids, hashfunctions, grp_collations) = {
        let perhash = &aggstate.perhash.as_ref().expect("perhash")[setno as usize];
        let hashslot = perhash.hashslot.expect("perhash->hashslot");
        let mut idx_hash = ::mcx::vec_with_capacity_in(
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
            ::mcx::vec_with_capacity_in(mcx, perhash.eqfuncoids.as_ref().map(|v| v.len()).unwrap_or(0))?;
        for &o in perhash.eqfuncoids.as_ref().expect("perhash->eqfuncoids").iter() {
            eqfuncoids.push(o);
        }
        let mut hashfunctions = ::mcx::vec_with_capacity_in(
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
            let mut v = ::mcx::vec_with_capacity_in(mcx, src.len())?;
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
        execTuples_seams::exec_slot_descriptor::call(mcx, estate, hashslot)?;

    // tmpcxt = aggstate->tmpcontext->ecxt_per_tuple_memory; — the per-tuple
    // context of the node's ExprContext (an EcxtId in the EState pool).
    let tmpcontext = aggstate.tmpcontext.expect("tmpcontext");

    // The three contexts (metacxt = hash_metacxt, tablecxt = hash_tablecxt,
    // tmpcxt) are caller-owned; the table borrows them.
    let table = {
        let tmpcxt: &::mcx::MemoryContext = &estate.ecxt(tmpcontext).ecxt_per_tuple_memory;
        let metacxt = aggstate
            .hash_metacxt
            .as_ref()
            .expect("aggstate->hash_metacxt");
        let tablecxt = aggstate
            .hash_tablecxt
            .as_ref()
            .expect("aggstate->hash_tablecxt");
        execGrouping_seams::build_tuple_hash_table::call(
            mcx,
            None,
            hash_desc,
            ::nodes::TupleSlotKind::MinimalTuple,
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

/// Recover the `ExprState.parent` back-link (the address-stable enclosing
/// `PlanStateNode::Agg`) from any already-stamped evaltrans on this AggState.
///
/// The owned model stamps `parent` onto each phase's evaltrans (and the cache
/// variants present at init) in execProcnode's `stamp_agg_evaltrans_parents`,
/// once the enum wrapper is address-stable. A fresh evaltrans built later (this
/// recompile) cannot reach that enum from the inner `&mut AggStateData`, so it
/// inherits the link from a sibling that already carries it. Returns `None`
/// only before any evaltrans has been stamped (i.e. before ExecInitNode wired
/// the back-links), in which case execProcnode's init stamp covers it.
fn existing_evaltrans_parent<'mcx>(
    aggstate: &AggStateData<'mcx>,
) -> Option<::nodes::planstate::PlanStateLink> {
    let phases = aggstate.phases.as_ref()?;
    for phase in phases.iter() {
        if let Some(es) = phase.evaltrans.as_ref() {
            if let Some(link) = es.parent {
                return Some(link);
            }
        }
        for row in phase.evaltrans_cache.iter() {
            for cached in row.iter() {
                if let Some(es) = cached.as_ref() {
                    if let Some(link) = es.parent {
                        return Some(link);
                    }
                }
            }
        }
    }
    None
}

/// `hashagg_recompile_expressions(aggstate, minslot, nullcheck)` — recompile
/// the per-phase transition expressions for hashed input, selecting the
/// outer-ops vs minimal-tuple and null-check cached variants.
pub fn hashagg_recompile_expressions<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    minslot: bool,
    nullcheck: bool,
    estate: &mut EStateData<'mcx>,
    mcx: Mcx<'mcx>,
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
        let dohash = true;
        let dosort = aggstate.aggstrategy == AggStrategy::AggMixed && !minslot;

        // C's ExecBuildAggTrans sets `state->parent = &aggstate->ss.ps` at build
        // time, so every (re)compiled evaltrans carries the AggState back-link the
        // EEOP_AGG_PLAIN_TRANS_* / NULLCHECK interpreter steps recover through
        // `state->parent`. The owned model cannot reach the enclosing,
        // address-stable `PlanStateNode::Agg` enum from here (we hold only the
        // inner `&mut AggStateData`), so the build seam leaves `parent` unset and
        // execProcnode's `stamp_agg_evaltrans_parents` stamps it once at init.
        // That init stamp does not cover evaltrans variants compiled *later* (this
        // recompile, fired e.g. on a LATERAL rescan), so carry the back-link from
        // an already-stamped sibling evaltrans onto the fresh one below. Every Agg
        // has at least the init-built phase evaltrans stamped, so this link is
        // always present once execution has begun.
        let parent_link = existing_evaltrans_parent(aggstate);

        // C temporarily swaps `ss.ps.outerops` to `&TTSOpsMinimalTuple` (and sets
        // `outeropsfixed`) while compiling, so the EEOP_OUTER_FETCHSOME deform
        // step is specialized to a minimal-tuple slot when reading a spilled
        // batch. The owned model carries no `outerops`/`outeropsfixed` field on
        // PlanState: the FETCHSOME deform step is left non-fixed (see
        // `push_setup_steps` in execExpr_domain_agg) and resolves the slot type
        // dynamically at runtime, so a single compiled program works for both the
        // outer-plan heap slot and the minimal-tuple tape slot. The swap is
        // therefore a no-op here and `minslot` only selects the cache slot `i`.
        //
        // ExecBuildAggTrans(aggstate, phase, dosort, dohash, nullcheck) is reached
        // through the execExpr seam (the same path build_phase_eval_trans uses for
        // the initial compile); it returns the freshly built ExprState which we
        // cache in evaltrans_cache[i][j].
        let phaseno = phase_idx as i32;
        let mut evaltrans = execExpr_seams::exec_build_agg_trans::call(
            mcx, aggstate, phaseno, dosort, dohash, nullcheck, estate,
        )?;
        // Re-stamp the AggState back-link the build seam left unset (mirrors
        // execProcnode's `stamp_agg_evaltrans_parents` for late recompiles).
        evaltrans.parent = parent_link;
        aggstate.phases.as_mut().expect("phases")[phase_idx].evaltrans_cache[i][j] =
            Some(evaltrans);
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
    let work_mem_kb = init_small_seams::work_mem::call();
    aggstate.hashcontext = Some(
        execUtils_seams::create_work_expr_context::call(estate, work_mem_kb)?,
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
    setno: i32,
    index: usize,
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
    //
    // In C `pergroup` aliases the entry's additional bytes; here the entry's
    // per-group `AggStatePerGroupData[]` lives in the perhash side-table slot
    // `index` (its id is stashed in the entry's `additional` bytes). Allocate the
    // `numtrans`-long `palloc0`-equivalent slot, then drive
    // `initialize_aggregate` over each transition.
    //
    //   for (transno = 0; transno < aggstate->numtrans; transno++) {
    //       AggStatePerTrans pertrans = &aggstate->pertrans[transno];
    //       AggStatePerGroup pergroupstate = &pergroup[transno];
    //       initialize_aggregate(aggstate, pertrans, pergroupstate);
    //   }
    let num_trans = aggstate.numtrans as usize;

    // Allocate the side-table slot (palloc0(sizeof(AggStatePerGroupData)*numtrans)).
    {
        let perhash =
            &mut aggstate.perhash.as_mut().expect("perhash")[setno as usize];
        debug_assert_eq!(perhash.pergroup_sidetable.len(), index);
        let mut pg: ::mcx::PgVec<'mcx, AggStatePerGroupData<'mcx>> =
            ::mcx::vec_with_capacity_in(mcx, num_trans)?;
        for _ in 0..num_trans {
            pg.push(AggStatePerGroupData::default());
        }
        perhash.pergroup_sidetable.push(Some(pg));
    }

    // Take the slot and the pertrans array out so both can be borrowed mutably
    // alongside &mut aggstate (mirrors the two raw pointers in C).
    let mut pg = aggstate.perhash.as_mut().expect("perhash")[setno as usize]
        .pergroup_sidetable[index]
        .take()
        .expect("initialize_hash_entry: side-table slot just pushed");
    let mut transstates = aggstate
        .pertrans
        .take()
        .expect("initialize_hash_entry: pertrans not built");

    let mut init_result = Ok(());
    for transno in 0..num_trans {
        if let Err(e) = crate::transition::initialize_aggregate(
            aggstate,
            &mut transstates[transno],
            &mut pg[transno],
            mcx,
        ) {
            init_result = Err(e);
            break;
        }
    }

    aggstate.pertrans = Some(transstates);
    aggstate.perhash.as_mut().expect("perhash")[setno as usize].pergroup_sidetable[index] =
        Some(pg);
    init_result
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

    // hash_pergroup aliases the tail of all_pergroups (the last num_hashes
    // slots): C sets `hash_pergroup = all_pergroups + numGroupingSets`, and the
    // compiled transition expr indexes `all_pergroups[setoff][transno]` where for
    // the hashed phase setoff == hash_setoff_base + setno. So the per-group state
    // for grouping set `setno` is all_pergroups[hash_setoff_base + setno].
    let hash_setoff_base = hash_setoff_base(aggstate);

    for setno in 0..num_hashes {
        // Reset this set's borrowed-entry record for the current tuple.
        if (setno as usize) < aggstate.hash_cur_entry_index.len() {
            aggstate.hash_cur_entry_index[setno as usize] = None;
        }

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
            // The seam finds/creates the entry and reports whether it is new,
            // lending its additional bytes to the callback. C then runs
            // initialize_hash_entry for a new entry and caches the per-group
            // pointer `hash_pergroup[setno] = TupleHashEntryGetAdditional(entry)`.
            // Here the per-group AggStatePerGroupData[] lives in the perhash
            // side-table, and the entry's additional bytes carry only its slot id
            // (pergroup_index_{read,write}). For a brand-new entry we assign the
            // next side-table slot id (== current side-table length) and stamp it
            // into the entry; for an existing entry we read its id back.
            let next_index = aggstate.perhash.as_ref().expect("perhash")[setno as usize]
                .pergroup_sidetable
                .len();
            let aggstate_numtrans_gt0 = aggstate.numtrans != 0;
            let mut captured_index: Option<usize> = None;
            let isnew = {
                let hashtable = aggstate.perhash.as_mut().expect("perhash")[setno as usize]
                    .hashtable
                    .as_mut()
                    .expect("perhash->hashtable");
                let (isnew, _hash) =
                    execGrouping_seams::lookup_tuple_hash_entry::call(
                        &mut **hashtable,
                        hashslot,
                        estate,
                        &mut |_entry, additional| {
                            if aggstate_numtrans_gt0 {
                                match pergroup_index_read(additional) {
                                    Some(idx) => captured_index = Some(idx),
                                    None => {
                                        // Fresh (zeroed) entry: stamp the next id.
                                        pergroup_index_write(additional, next_index);
                                        captured_index = Some(next_index);
                                    }
                                }
                            }
                        },
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
                    hp[setno as usize] = Some(::mcx::PgVec::new_in(estate.es_query_cxt));
                }
            } else {
                let index =
                    captured_index.expect("lookup_hash_entries: callback set the side-table id");

                // if (isnew) initialize_hash_entry(aggstate, hashtable, entry);
                if isnew {
                    initialize_hash_entry(aggstate, setno, index, estate)?;
                }

                // pergroup[setno] = TupleHashEntryGetAdditional(hashtable, entry);
                //
                // C repoints hash_pergroup[setno] (== all_pergroups[setoff]) at
                // the entry's additional storage so the transition mutates it in
                // place. Here we move the entry's per-group PgVec out of the
                // side-table into all_pergroups[setoff]; store_hash_pergroups_back
                // returns it after advance_aggregates.
                let setoff = hash_setoff_base + setno as usize;
                let pg = aggstate.perhash.as_mut().expect("perhash")[setno as usize]
                    .pergroup_sidetable[index]
                    .take()
                    .expect("lookup_hash_entries: side-table slot for entry");
                if let Some(all) = aggstate.all_pergroups.as_mut() {
                    all[setoff] = Some(pg);
                }
                aggstate.hash_cur_entry_index[setno as usize] = Some(index);
            }
        } else {
            // Spill mode: C calls LookupTupleHashEntry(hashtable, hashslot,
            // NULL, &hash) — p_isnew == NULL means no new entry is created on a
            // miss. The internally-computed hash is returned and used to route a
            // missing tuple to its spill partition. Here we compute the hash
            // explicitly (tuple_hash_table_hash, exactly what
            // LookupTupleHashEntry does internally) then probe with
            // create == false; a hit caches the per-group pointer, a miss spills.
            let hash = {
                let hashtable = aggstate.perhash.as_mut().expect("perhash")[setno as usize]
                    .hashtable
                    .as_mut()
                    .expect("perhash->hashtable");
                execGrouping_seams::tuple_hash_table_hash::call(
                    &mut **hashtable,
                    hashslot,
                    estate,
                )?
            };

            let aggstate_numtrans_gt0 = aggstate.numtrans != 0;
            let next_index = aggstate.perhash.as_ref().expect("perhash")[setno as usize]
                .pergroup_sidetable
                .len();
            let mut captured_index: Option<usize> = None;
            let (found, _isnew) = {
                let hashtable = aggstate.perhash.as_mut().expect("perhash")[setno as usize]
                    .hashtable
                    .as_mut()
                    .expect("perhash->hashtable");
                execGrouping_seams::lookup_tuple_hash_entry_hash::call(
                    &mut **hashtable,
                    hashslot,
                    hash,
                    false, // create == false (p_isnew == NULL)
                    estate,
                    &mut |opt| {
                        if let Some((_entry, additional)) = opt {
                            if aggstate_numtrans_gt0 {
                                match pergroup_index_read(additional) {
                                    Some(idx) => captured_index = Some(idx),
                                    None => {
                                        pergroup_index_write(additional, next_index);
                                        captured_index = Some(next_index);
                                    }
                                }
                            }
                        }
                    },
                )?
            };

            if found {
                // entry != NULL: isnew is always false here (create == false),
                // so no initialize_hash_entry. pergroup[setno] =
                // TupleHashEntryGetAdditional(entry).
                if aggstate.numtrans == 0 {
                    if let Some(hp) = aggstate.hash_pergroup.as_mut() {
                        hp[setno as usize] = Some(::mcx::PgVec::new_in(estate.es_query_cxt));
                    }
                } else {
                    let index = captured_index
                        .expect("lookup_hash_entries: callback set the side-table id");
                    let setoff = hash_setoff_base + setno as usize;
                    let pg = aggstate.perhash.as_mut().expect("perhash")[setno as usize]
                        .pergroup_sidetable[index]
                        .take()
                        .expect("lookup_hash_entries: side-table slot for entry");
                    if let Some(all) = aggstate.all_pergroups.as_mut() {
                        all[setoff] = Some(pg);
                    }
                    aggstate.hash_cur_entry_index[setno as usize] = Some(index);
                }
            } else {
                // entry == NULL: no room for a new group, spill the tuple.
                //   HashAggSpill *spill = &aggstate->hash_spills[setno];
                //   if (spill->partitions == NULL)
                //       hashagg_spill_init(spill, hash_tapeset, 0,
                //                          perhash->aggnode->numGroups,
                //                          hashentrysize);
                //   hashagg_spill_tuple(aggstate, spill, slot, hash);
                //   pergroup[setno] = NULL;
                let needs_init = aggstate
                    .hash_spills
                    .as_ref()
                    .map(|s| s[setno as usize].partitions.is_none())
                    .unwrap_or(true);

                if needs_init {
                    let num_groups = aggstate
                        .perhash
                        .as_ref()
                        .and_then(|p| p.get(setno as usize))
                        .and_then(|ph| ph.aggnode.as_ref())
                        .map(|n| n.num_groups)
                        .unwrap_or(0) as f64;
                    let hashentrysize = aggstate.hashentrysize;
                    let mcx = estate.es_query_cxt;
                    // Borrow the tapeset and the target spill disjointly.
                    let mut spills = aggstate
                        .hash_spills
                        .take()
                        .expect("lookup_hash_entries: hash_spills present in spill mode");
                    let tapeset = aggstate
                        .hash_tapeset
                        .as_mut()
                        .expect("lookup_hash_entries: hash_tapeset present in spill mode");
                    crate::spill::hashagg_spill_init(
                        &mut spills[setno as usize],
                        tapeset,
                        0,
                        num_groups,
                        hashentrysize,
                        mcx,
                    )?;
                    aggstate.hash_spills = Some(spills);
                }

                // hashagg_spill_tuple borrows aggstate (tapeset) + a single
                // spill mutably; take the spills vec out for the call.
                let mut spills = aggstate
                    .hash_spills
                    .take()
                    .expect("lookup_hash_entries: hash_spills present in spill mode");
                let res = crate::spill::hashagg_spill_tuple(
                    aggstate,
                    estate,
                    &mut spills[setno as usize],
                    outerslot,
                    hash,
                );
                aggstate.hash_spills = Some(spills);
                res?;

                // pergroup[setno] = NULL;
                if let Some(hp) = aggstate.hash_pergroup.as_mut() {
                    hp[setno as usize] = None;
                }
            }
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
        // C: `if (TupIsNull(outerslot)) break;` — TupIsNull is true for both a
        // NULL slot *and* a non-NULL but TTS_EMPTY slot. `fetch_input_tuple`
        // faithfully returns the child's slot as-is (like C), so at end-of-scan a
        // projecting child (e.g. a SeqScan with a computed-expr target) hands back
        // a non-NULL but *empty* virtual result slot. Breaking only on `None`
        // would then fall through to `lookup_hash_entries`/`prepare_hash_slot`,
        // whose `slot_getsomeattr` deform of the empty virtual slot raises
        // "getsomeattrs is not required to be called on a virtual tuple table
        // slot". Mirror C by treating an empty slot as end-of-input too.
        let outerslot = match outerslot {
            Some(s) if !estate.slot(s).is_empty() => s,
            _ => break,
        };

        // tmpcontext->ecxt_outertuple = outerslot;
        let tmpcontext = aggstate.tmpcontext.expect("tmpcontext");
        estate.ecxt_mut(tmpcontext).ecxt_outertuple = Some(outerslot);

        // Find or build hashtable entries
        lookup_hash_entries(aggstate, estate)?;

        // Advance the aggregates (or combine functions)
        crate::transition::advance_aggregates(aggstate, estate)?;

        // Owned-model write-back: return each entry's per-group PgVec (mutated in
        // place by advance_aggregates inside all_pergroups[setoff]) to its
        // side-table slot. C needs none of this — its hash_pergroup[setno] aliases
        // the entry's additional bytes directly.
        store_hash_pergroups_back(aggstate);

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
        execGrouping_seams::init_tuple_hash_iterator::call(&mut **table0)
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
    let mut batch = {
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
        execGrouping_seams::reset_tuple_hash_table::call(&mut **table)?;
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
    let mcx = estate.es_query_cxt;
    hashagg_recompile_expressions(aggstate, true, true, estate, mcx)?;

    let _ = (mem_limit, ngroups_limit);

    let setno = batch.setno;
    let hash_setoff_base = hash_setoff_base(aggstate);

    // A local HashAggSpill for any re-spilling this batch triggers; created
    // lazily on first miss (C: spill_initialized).
    let mut spill: Option<crate::aggstate::HashAggSpill<'mcx>> = None;

    let spillslot = aggstate
        .hash_spill_rslot
        .expect("agg_refill_hash_table: hash_spill_rslot");

    let tmpcontext = aggstate.tmpcontext.expect("tmpcontext");

    loop {
        // CHECK_FOR_INTERRUPTS();
        postgres_seams::check_for_interrupts::call()?;

        // tuple = hashagg_batch_read(batch, &hash);  if (tuple == NULL) break;
        let read = {
            let tapeset = aggstate
                .hash_tapeset
                .as_mut()
                .expect("agg_refill_hash_table: hash_tapeset");
            crate::spill::hashagg_batch_read(tapeset, &mut batch, mcx)?
        };
        let (blob, hash) = match read {
            Some(t) => t,
            None => break,
        };

        // ExecStoreMinimalTuple(tuple, spillslot, true);
        let mtup = heaptuple::flat::minimal_tuple_from_flat(mcx, &blob)
            .map_err(crate::spill::flat_err)?;
        execTuples_seams::exec_store_minimal_tuple::call(
            estate, mtup, spillslot, true,
        )?;

        // aggstate->tmpcontext->ecxt_outertuple = spillslot;
        estate.ecxt_mut(tmpcontext).ecxt_outertuple = Some(spillslot);

        // prepare_hash_slot(perhash, outerslot, hashslot);
        let hashslot = aggstate.perhash.as_ref().expect("perhash")[setno as usize]
            .hashslot
            .expect("perhash->hashslot");
        prepare_hash_slot(aggstate, setno, spillslot, hashslot, estate)?;

        // p_isnew = aggstate->hash_spill_mode ? NULL : &isnew;
        let want_new = !aggstate.hash_spill_mode;

        // entry = LookupTupleHashEntryHash(hashtable, hashslot, p_isnew, hash);
        aggstate.hash_cur_entry_index[setno as usize] = None;
        let next_index = aggstate.perhash.as_ref().expect("perhash")[setno as usize]
            .pergroup_sidetable
            .len();
        let aggstate_numtrans_gt0 = aggstate.numtrans != 0;
        let mut captured_index: Option<usize> = None;
        // entry = LookupTupleHashEntryHash(hashtable, hashslot, p_isnew, hash);
        // Always use the precomputed tape `hash` (C's LookupTupleHashEntryHash):
        // the spill partitioning routed equal keys to the same partition by that
        // exact hash, so the refill MUST insert/probe by it (recomputing the hash
        // would risk a different value and split a group). `create == want_new`
        // (p_isnew == NULL in spill mode); the seam returns (found, isnew).
        let (found, isnew) = {
            let hashtable = aggstate.perhash.as_mut().expect("perhash")[setno as usize]
                .hashtable
                .as_mut()
                .expect("perhash->hashtable");
            execGrouping_seams::lookup_tuple_hash_entry_hash::call(
                &mut **hashtable,
                hashslot,
                hash,
                want_new,
                estate,
                &mut |opt| {
                    if let Some((_entry, additional)) = opt {
                        if aggstate_numtrans_gt0 {
                            match pergroup_index_read(additional) {
                                Some(idx) => captured_index = Some(idx),
                                None => {
                                    pergroup_index_write(additional, next_index);
                                    captured_index = Some(next_index);
                                }
                            }
                        }
                    }
                },
            )?
        };

        if found {
            // if (isnew) initialize_hash_entry(...);
            // aggstate->hash_pergroup[setno] = TupleHashEntryGetAdditional(entry);
            // advance_aggregates(aggstate);
            if aggstate.numtrans == 0 {
                if isnew {
                    aggstate.hash_ngroups_current += 1;
                    crate::spill::hash_agg_check_limits(aggstate, estate, mcx)?;
                }
                if let Some(hp) = aggstate.hash_pergroup.as_mut() {
                    hp[setno as usize] = Some(::mcx::PgVec::new_in(mcx));
                }
            } else {
                let index = captured_index
                    .expect("agg_refill_hash_table: callback set the side-table id");
                if isnew {
                    initialize_hash_entry(aggstate, setno, index, estate)?;
                }
                let setoff = hash_setoff_base + setno as usize;
                let pg = aggstate.perhash.as_mut().expect("perhash")[setno as usize]
                    .pergroup_sidetable[index]
                    .take()
                    .expect("agg_refill_hash_table: side-table slot for entry");
                if let Some(all) = aggstate.all_pergroups.as_mut() {
                    all[setoff] = Some(pg);
                }
                aggstate.hash_cur_entry_index[setno as usize] = Some(index);
            }

            crate::transition::advance_aggregates(aggstate, estate)?;
            store_hash_pergroups_back(aggstate);
        } else {
            // no memory for a new group, spill
            if spill.is_none() {
                // hashagg_spill_init(&spill, tapeset, batch->used_bits,
                //                    batch->input_card, aggstate->hashentrysize);
                let mut s = crate::aggstate::HashAggSpill::default();
                let hashentrysize = aggstate.hashentrysize;
                let used_bits = batch.used_bits;
                let input_card = batch.input_card;
                let tapeset = aggstate
                    .hash_tapeset
                    .as_mut()
                    .expect("agg_refill_hash_table: hash_tapeset");
                crate::spill::hashagg_spill_init(
                    &mut s, tapeset, used_bits, input_card, hashentrysize, mcx,
                )?;
                spill = Some(s);
            }

            let s = spill.as_mut().expect("spill just initialized");
            crate::spill::hashagg_spill_tuple(aggstate, estate, s, spillslot, hash)?;

            if let Some(hp) = aggstate.hash_pergroup.as_mut() {
                hp[setno as usize] = None;
            }
        }

        // ResetExprContext(aggstate->tmpcontext);
        reset_tmpcontext(aggstate, estate)?;
    }

    // LogicalTapeClose(batch->input_tape);
    if let Some(tape) = batch.input_tape {
        let tapeset = aggstate
            .hash_tapeset
            .as_mut()
            .expect("agg_refill_hash_table: hash_tapeset for close");
        sort_storage_seams::logical_tape_close::call(tapeset, tape);
    }

    // change back to phase 0
    aggstate.current_phase = 0;
    aggstate.phase = aggstate.current_phase;

    if let Some(mut s) = spill {
        // hashagg_spill_finish(aggstate, &spill, batch->setno);
        let np = s.npartitions;
        crate::spill::hashagg_spill_finish(aggstate, &mut s, setno, mcx)?;
        // hash_agg_update_metrics(aggstate, true, spill.npartitions);
        crate::spill::hash_agg_update_metrics(aggstate, estate, true, np)?;
    } else {
        // hash_agg_update_metrics(aggstate, true, 0);
        crate::spill::hash_agg_update_metrics(aggstate, estate, true, 0)?;
    }

    aggstate.hash_spill_mode = false;

    // prepare to walk the first hash table
    // select_current_set(aggstate, batch->setno, true);
    crate::node_lifecycle::select_current_set(aggstate, setno, true);

    // ResetTupleHashIterator(perhash[batch->setno].hashtable, &perhash[...].hashiter);
    let iter = {
        let table = aggstate.perhash.as_mut().expect("perhash")[setno as usize]
            .hashtable
            .as_mut()
            .expect("perhash->hashtable");
        execGrouping_seams::init_tuple_hash_iterator::call(&mut **table)
    };
    aggstate.perhash.as_mut().expect("perhash")[setno as usize].hashiter = iter;

    // pfree(batch); — `batch` is a Copy struct on the stack; nothing to free.

    Ok(true)
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
        postgres_seams::check_for_interrupts::call()?;

        let setno = aggstate.current_set;
        let hashslot = aggstate.perhash.as_ref().expect("perhash")[setno as usize]
            .hashslot
            .expect("perhash->hashslot");

        // entry = ScanTupleHashTable(hashtable, &perhash->hashiter);
        let mut entry_tuple: Option<
            types_tuple::heaptuple::FormedMinimalTuple<'mcx>,
        > = None;
        let mut entry_pergroup_index: Option<usize> = None;
        let want_pergroup = aggstate.numtrans != 0;
        let found = {
            let mcx = estate.es_query_cxt;
            let mut hashiter = aggstate.perhash.as_ref().expect("perhash")[setno as usize].hashiter;
            let hashtable = aggstate.perhash.as_mut().expect("perhash")[setno as usize]
                .hashtable
                .as_mut()
                .expect("perhash->hashtable");
            let found = execGrouping_seams::scan_tuple_hash_table::call(
                &mut **hashtable,
                &mut hashiter,
                estate,
                &mut |entry, additional| {
                    // TupleHashEntryGetTuple(entry) — group's first tuple.
                    entry_tuple = entry
                        .firstTuple
                        .as_ref()
                        .map(|m| m.clone_in(mcx).expect("clone hash entry tuple"));
                    // For numtrans > 0 the entry's additional bytes carry the
                    // per-group side-table slot id (pergroup_index codec).
                    if want_pergroup {
                        entry_pergroup_index = pergroup_index_read(additional);
                    }
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
                    execGrouping_seams::init_tuple_hash_iterator::call(
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
        execUtils_seams::reset_expr_context::call(estate, econtext)?;

        // Transform representative tuple back into one with the right columns:
        //   ExecStoreMinimalTuple(TupleHashEntryGetTuple(entry), hashslot, false);
        //   slot_getallattrs(hashslot);
        let mtup = entry_tuple.expect("scan callback captured the entry tuple");
        execTuples_seams::exec_store_minimal_tuple::call(
            estate, mtup, hashslot, false,
        )?;
        let hash_cols =
            execTuples_seams::slot_getallattrs_by_id::call(estate, hashslot)?;

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
            let mut idx = ::mcx::vec_with_capacity_in(estate.es_query_cxt, src.len())?;
            for &v in src.iter() {
                idx.push(v);
            }
            (n, idx)
        };
        let mut values: ::mcx::PgVec<'mcx, types_tuple::heaptuple::Datum<'mcx>> =
            ::mcx::vec_with_capacity_in(estate.es_query_cxt, first_natts.max(0) as usize)?;
        let mut isnull = ::mcx::vec_with_capacity_in(estate.es_query_cxt, first_natts.max(0) as usize)?;
        for _ in 0..first_natts {
            values.push(types_tuple::heaptuple::Datum::null());
            isnull.push(true);
        }
        for i in 0..num_hash_cols as usize {
            let var_number = idx_input[i] as usize - 1;
            values[var_number] = hash_cols[i].0.clone();
            isnull[var_number] = hash_cols[i].1;
        }
        execTuples_seams::store_virtual_values::call(
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
        // C: pergroup = TupleHashEntryGetAdditional(hashtable, entry). Here the
        // entry's per-group state is in the perhash side-table slot whose id was
        // read from the entry's additional bytes during the scan. For numtrans ==
        // 0 (hashed DISTINCT / set-op dedup) there are no aggregates to finalize,
        // so this is a no-op and the projection emits the grouping columns
        // directly.
        if aggstate.numtrans != 0 {
            let index = entry_pergroup_index
                .expect("agg_retrieve_hash_table_in_memory: entry had no per-group side-table id");
            // Take the slot out so finalize_aggregates can borrow it mutably
            // alongside &mut aggstate (C aliases the entry storage in place).
            let mut pg = aggstate.perhash.as_mut().expect("perhash")[setno as usize]
                .pergroup_sidetable[index]
                .take()
                .expect("agg_retrieve_hash_table_in_memory: side-table slot for entry");
            let fin = crate::finalize::finalize_aggregates(aggstate, pg.as_mut_slice(), estate);
            aggstate.perhash.as_mut().expect("perhash")[setno as usize].pergroup_sidetable[index] =
                Some(pg);
            fin?;
        }

        // result = project_aggregates(aggstate);
        if let Some(result) = crate::finalize::project_aggregates(aggstate, estate)? {
            return Ok(Some(result));
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
    // tupleSize = MAXALIGN(SizeofMinimalTupleHeader) + tupleWidth (C exactly:
    // the header is MAXALIGNed BEFORE adding the width).
    let tuple_size = maxalign(SizeofMinimalTupleHeader) + tuple_width;
    // sizeof(AggStatePerGroupData): C's ABI struct is { Datum (8B word); bool;
    // bool } == 16 bytes after padding. Rust's `AggStatePerGroupData` carries an
    // owned-heap `Datum<'mcx>` enum whose `size_of` is unrelated to C's on-the-
    // wire layout, so this estimate must use the C ABI size, NOT `size_of`.
    let pergroup_size = num_trans as usize * SIZEOF_AGGSTATEPERGROUPDATA;

    // Entries use the Bump allocator, so chunk sizes equal requested sizes.
    let tuple_chunk_size = maxalign(tuple_size);
    let pergroup_chunk_size = pergroup_size;

    // Transition values use AllocSet: chunk header + power-of-two allocation.
    let transition_chunk_size = if transition_space > 0 {
        CHUNKHDRSZ + pg_nextpower2_size_t(transition_space)
    } else {
        0
    };

    // TupleHashEntrySize() == sizeof(TupleHashEntryData) (executor.h:165). C's
    // struct is { MinimalTuple ptr (8B); uint32 status; uint32 hash } == 16B.
    // The Rust `TupleHashEntryData` carries an owned `FormedMinimalTuple` +
    // `PgVec`, so again use the C ABI size, NOT `size_of`.
    SIZEOF_TUPLEHASHENTRYDATA + tuple_chunk_size + pergroup_chunk_size + transition_chunk_size
}

/// `sizeof(AggStatePerGroupData)` in C: `{ Datum transValue; bool
/// transValueIsNull; bool noTransValue; }` == 16 bytes (8-byte Datum + two
/// bools, padded to 8-byte alignment).
const SIZEOF_AGGSTATEPERGROUPDATA: usize = 16;

/// `sizeof(TupleHashEntryData)` in C: `{ MinimalTuple firstTuple; uint32
/// status; uint32 hash; }` == 16 bytes (8-byte pointer + two uint32s).
const SIZEOF_TUPLEHASHENTRYDATA: usize = 16;

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
        execUtils_seams::reset_expr_context::call(estate, ecxt)?;
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
        execUtils_seams::re_scan_expr_context::call(estate, ecxt)?;
    }
    Ok(())
}

/// The per-query context handle, target for transient allocations the
/// hash-grouping path hands to `mcx`-taking siblings/seams.
fn estate_mcx<'mcx>(estate: &EStateData<'mcx>) -> Mcx<'mcx> {
    estate.es_query_cxt
}

// ---------------------------------------------------------------------------
// Per-group side-table index codec (owned-model rendering of the C
// `TupleHashEntryGetAdditional` per-entry per-group storage).
//
// C carves a `numtrans * sizeof(AggStatePerGroupData)` MAXALIGN'd region into
// each entry's `additional` bytes and stores the `AggStatePerGroupData[]` there
// in place. A typed `AggStatePerGroupData<'mcx>` (owned `Datum<'mcx>` enum) is
// not reinterpretable from raw bytes, so the real per-group `PgVec` lives in
// `perhash.pergroup_sidetable`, and the entry's first 4 `additional` bytes carry
// a `u32` index into that table. The stored value is `index + 1` so that a
// freshly-zeroed (just-inserted, index-not-yet-written) entry reads back as the
// "unassigned" sentinel `0` and is distinguishable from a valid index `0`.
// `additionalsize` is always `>= 4` here because this codec is only used when
// `numtrans > 0` (so `numtrans * sizeof(AggStatePerGroupData) >= 24`).
// ---------------------------------------------------------------------------

/// The index into `all_pergroups` at which the hashed grouping-set region
/// begins (`hash_pergroup == all_pergroups + offset`). C's `ExecInitAgg`
/// advances the pergroups pointer by `numGroupingSets` only on the non-hashed
/// (AGG_SORTED/AGG_MIXED) path; for pure AGG_HASHED the hash region is at the
/// front (offset 0). The compiled hashed-phase transition expr is built with the
/// matching `setoff` base (`execExpr.c`: `setoff = (aggstrategy != AGG_HASHED) ?
/// maxsets : 0`), so this offset is exactly `pergroup_offset` from
/// `assign_pergroup_regions`.
fn hash_setoff_base(aggstate: &AggStateData<'_>) -> usize {
    if aggstate.aggstrategy == AggStrategy::AggHashed {
        0
    } else {
        aggstate.maxsets.max(0) as usize
    }
}

/// Return each grouping set's borrowed per-group `PgVec` (mutated in place by
/// `advance_aggregates` inside `all_pergroups[hash_setoff_base + setno]`) to its
/// `perhash[setno].pergroup_sidetable[index]` slot, clearing the transient
/// borrow record. No-op for sets that spilled / had no entry this tuple. C has no
/// analogue: it aliases the entry's additional bytes, so the transition mutates
/// the entry storage directly and nothing is written back.
pub(crate) fn store_hash_pergroups_back(aggstate: &mut AggStateData<'_>) {
    let num_hashes = aggstate.num_hashes as usize;
    let hash_setoff_base = hash_setoff_base(aggstate);

    for setno in 0..num_hashes {
        let index = match aggstate.hash_cur_entry_index.get(setno).copied().flatten() {
            Some(i) => i,
            None => continue,
        };
        let setoff = hash_setoff_base + setno;
        let pg = match aggstate.all_pergroups.as_mut() {
            Some(all) => all[setoff].take(),
            None => None,
        };
        if let Some(pg) = pg {
            aggstate.perhash.as_mut().expect("perhash")[setno].pergroup_sidetable[index] = Some(pg);
        }
        aggstate.hash_cur_entry_index[setno] = None;
    }
}

/// Write the `index`-th side-table slot id into the entry's `additional` bytes.
fn pergroup_index_write(additional: &mut [u8], index: usize) {
    let stored = (index as u32) + 1;
    additional[0..4].copy_from_slice(&stored.to_ne_bytes());
}

/// Read the side-table slot id out of the entry's `additional` bytes; returns
/// `None` for the zeroed "unassigned" sentinel.
fn pergroup_index_read(additional: &[u8]) -> Option<usize> {
    let mut buf = [0u8; 4];
    buf.copy_from_slice(&additional[0..4]);
    let stored = u32::from_ne_bytes(buf);
    if stored == 0 {
        None
    } else {
        Some((stored - 1) as usize)
    }
}
