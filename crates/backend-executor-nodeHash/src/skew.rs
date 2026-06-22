//! The skew-optimization hashtable: build the skew bucket set from the outer
//! relation's MCVs, look up / insert into it, and shrink it on demand.

use mcx::Mcx;
use types_core::{uint32, Size};
// Datum-unification status (Wave 7): this crate's only canonical-Datum work is
// already done — the `MultiExecPrivateHash` / `ExecHashTableInsert` hash path in
// `exec_hash.rs` consumes the canonical `types_tuple::...heaptuple::Datum<'mcx>`
// returned by the `exec_eval_expr_switch_context` interpreter seam and reads it
// through `Datum::as_u32()`. There are NO internal shim construction/read sites
// to migrate.
//
// The skew-MCV probe below flows through two seam ABI edges that are still bare
// machine words (`types_datum::Datum`): lsyscache's `get_attstatsslot_mcv` hands
// back `PgVec<Datum>` MCV values (mirroring the still-bare-word
// `AttStatsSlot.values`), and fmgr's `function_call1_coll` consumes/returns a
// bare scalar word. These are the genuinely-sanctioned bare-word edges per the
// datum-redesign plan: the unified `types_tuple::Datum<'mcx>` value type cannot
// cross them until those owners (lsyscache `AttStatsSlot` + the fmgr arg)
// migrate together — the execTuples canonical-carrier follow-on (#113). The MCV
// word is read out of one bare-word seam and fed straight into the other, so it
// stays a bare word; it is never forged into / out of the canonical type here.
use types_datum::Datum as DatumWord;
use types_error::PgResult;
use types_nodes::nodehash::{
    Hash, HashJoinBuckets, HashJoinTupleData, HashJoinTupleLink, HashSkewBucket, HashState,
    HashJoinTableData, INVALID_SKEW_BUCKET_NO,
};
use types_tuple::heaptuple::HEAP_TUPLE_HAS_MATCH;

use crate::hash_table::{dense_alloc, ExecHashGetBucketAndBatch, ExecHashIncreaseNumBatches};
use crate::{MaxAllocSize, SKEW_BUCKET_OVERHEAD, HJTUPLE_OVERHEAD, SKEW_MIN_OUTER_FRACTION};

/// `pg_nextpower2_32(num)` (`port/pg_bitutils.h`, static inline) — the next
/// power of 2 not less than `num` (`num` must be > 0 and <= 2^31).
#[inline]
fn pg_nextpower2_32(num: u32) -> u32 {
    debug_assert!(num > 0 && num <= (1u32 << 31));
    if num == 1 {
        return 1;
    }
    1u32 << (32 - (num - 1).leading_zeros())
}

/// `ExecHashBuildSkewHash(HashState *hashstate, HashJoinTable hashtable,
/// Hash *node, int mcvsToUse)` (nodeHash.c:2403) — set up the skew hashtable
/// from the planner's outer-relation MCV list (read from the syscache).
/// Allocates the skew bucket array in `mcx`.
pub fn ExecHashBuildSkewHash<'mcx>(
    mcx: Mcx<'mcx>,
    hashstate: &mut HashState<'mcx>,
    hashtable: &mut HashJoinTableData<'mcx>,
    node: &Hash<'mcx>,
    mut mcvsToUse: i32,
) -> PgResult<()> {
    // Do nothing if planner didn't identify the outer relation's join key.
    if !types_core::OidIsValid(node.skewTable) {
        return Ok(());
    }
    // Also, do nothing if we don't have room for at least one skew bucket.
    if mcvsToUse <= 0 {
        return Ok(());
    }

    // Try to find the MCV statistics for the outer relation's join key:
    //   statsTuple = SearchSysCache3(STATRELATTINH, skewTable, skewColumn,
    //                                skewInherit);
    //   if (!HeapTupleIsValid(statsTuple)) return;
    //   if (get_attstatsslot(&sslot, statsTuple, STATISTIC_KIND_MCV, InvalidOid,
    //                        ATTSTATSSLOT_VALUES | ATTSTATSSLOT_NUMBERS)) { ... }
    //   ReleaseSysCache(statsTuple);
    //
    // The owner (lsyscache) does the SearchSysCache3 + get_attstatsslot +
    // free_attstatsslot + ReleaseSysCache, returning the MCV slot's
    // (values, numbers) arrays copied into mcx, or None on a missing
    // pg_statistic row or missing MCV slot.
    let slot = backend_utils_cache_lsyscache_seams::get_attstatsslot_mcv::call(
        mcx,
        node.skewTable,
        node.skewColumn,
        node.skewInherit,
    )?;
    let (values, numbers) = match slot {
        // !HeapTupleIsValid(statsTuple) || !get_attstatsslot(...): nothing to do.
        None => return Ok(()),
        Some(s) => s,
    };

    // get_attstatsslot succeeded.
    let nvalues = values.len() as i32;

    if mcvsToUse > nvalues {
        mcvsToUse = nvalues;
    }

    // Calculate the expected fraction of outer relation that will participate
    // in the skew optimization.  If this isn't at least SKEW_MIN_OUTER_FRACTION,
    // don't use skew optimization.
    let mut frac: f64 = 0.0;
    for i in 0..mcvsToUse as usize {
        frac += numbers[i] as f64;
    }
    if frac < SKEW_MIN_OUTER_FRACTION {
        // free_attstatsslot(&sslot); ReleaseSysCache(statsTuple): the owner
        // already released; the copied arrays drop here.
        return Ok(());
    }

    // Okay, set up the skew hashtable.
    //
    // skewBucket[] is an open addressing hashtable with a power of 2 size that
    // is greater than the number of MCV values.  (This ensures there will be at
    // least one null entry, so searches will always terminate.)
    //
    // Note: this code could fail if mcvsToUse exceeds INT_MAX/8 or
    // MaxAllocSize/sizeof(void *)/8, but that is not currently possible since we
    // limit pg_statistic entries to much less than that.
    let _ = MaxAllocSize;
    let mut nbuckets = pg_nextpower2_32((mcvsToUse + 1) as u32) as i32;
    // use two more bits just to help avoid collisions
    nbuckets <<= 2;

    hashtable.skewEnabled = true;
    hashtable.skewBucketLen = nbuckets;

    // We allocate the bucket memory in the hashtable's batch context. It is only
    // needed during the first batch, and this ensures it will be automatically
    // removed once the first batch is done.
    //   hashtable->skewBucket = MemoryContextAllocZero(batchCxt,
    //       nbuckets * sizeof(HashSkewBucket *));
    //   hashtable->skewBucketNums = MemoryContextAllocZero(batchCxt,
    //       mcvsToUse * sizeof(int));
    let mut skewBucket = mcx::vec_with_capacity_in::<Option<Box<HashSkewBucket>>>(
        mcx,
        nbuckets as usize,
    )?;
    for _ in 0..nbuckets {
        skewBucket.push(None);
    }
    let mut skewBucketNums = mcx::vec_with_capacity_in::<i32>(mcx, mcvsToUse as usize)?;
    for _ in 0..mcvsToUse {
        skewBucketNums.push(0);
    }
    hashtable.skewBucket = skewBucket;
    hashtable.skewBucketNums = skewBucketNums;

    let arrays_space: Size =
        nbuckets as usize * core::mem::size_of::<usize>() + mcvsToUse as usize * core::mem::size_of::<i32>();
    hashtable.spaceUsed += arrays_space;
    hashtable.spaceUsedSkew += arrays_space;
    if hashtable.spaceUsed > hashtable.spacePeak {
        hashtable.spacePeak = hashtable.spaceUsed;
    }

    // Create a skew bucket for each MCV hash value.
    //
    // Note: it is very important that we create the buckets in order of
    // decreasing MCV frequency.  If we have to remove some buckets, they must be
    // removed in reverse order of creation (see notes in
    // ExecHashRemoveNextSkewBucket) and we want the least common MCVs to be
    // removed first.
    for i in 0..mcvsToUse as usize {
        // hashvalue = DatumGetUInt32(FunctionCall1Coll(hashstate->skew_hashfunction,
        //                            hashstate->skew_collation, sslot.values[i]));
        let skew_hashfunction = hashstate
            .skew_hashfunction
            .as_ref()
            .expect("skew_hashfunction must be set when building skew hashtable");
        let result: DatumWord = backend_utils_fmgr_fmgr_seams::function_call1_coll::call(
            skew_hashfunction.fn_oid,
            hashstate.skew_collation,
            values[i],
        )?;
        let hashvalue: uint32 = result.as_u32();

        // While we have not hit a hole in the hashtable and have not hit the
        // desired bucket, we have collided with some previous hash value, so try
        // the next bucket location.  NB: this code must match
        // ExecHashGetSkewBucket.
        let mask = (nbuckets - 1) as uint32;
        let mut bucket = (hashvalue & mask) as usize;
        while let Some(entry) = hashtable.skewBucket[bucket].as_ref() {
            if entry.hashvalue == hashvalue {
                break;
            }
            bucket = ((bucket as uint32 + 1) & mask) as usize;
        }

        // If we found an existing bucket with the same hashvalue, leave it
        // alone.  It's okay for two MCVs to share a hashvalue.
        if hashtable.skewBucket[bucket].is_some() {
            continue;
        }

        // Okay, create a new skew bucket for this hashvalue.
        hashtable.skewBucket[bucket] = Some(Box::new(HashSkewBucket {
            hashvalue,
            tuples: None,
        }));
        let n = hashtable.nSkewBuckets as usize;
        hashtable.skewBucketNums[n] = bucket as i32;
        hashtable.nSkewBuckets += 1;
        hashtable.spaceUsed += SKEW_BUCKET_OVERHEAD;
        hashtable.spaceUsedSkew += SKEW_BUCKET_OVERHEAD;
        if hashtable.spaceUsed > hashtable.spacePeak {
            hashtable.spacePeak = hashtable.spaceUsed;
        }
    }

    // free_attstatsslot(&sslot); ReleaseSysCache(statsTuple): handled by the
    // owner; the copied arrays drop here.
    Ok(())
}

/// `ExecHashGetSkewBucket(HashJoinTable hashtable, uint32 hashvalue)`
/// (nodeHash.c:2555) — the skew-bucket index for a hash value, or
/// `INVALID_SKEW_BUCKET_NO`. Pure lookup.
pub fn ExecHashGetSkewBucket<'mcx>(hashtable: &HashJoinTableData<'mcx>, hashvalue: uint32) -> i32 {
    // Always return INVALID_SKEW_BUCKET_NO if not doing skew optimization (in
    // particular, this happens after the initial batch is done).
    if !hashtable.skewEnabled {
        return INVALID_SKEW_BUCKET_NO;
    }

    // Since skewBucketLen is a power of 2, we can do a modulo by ANDing.
    let mask = (hashtable.skewBucketLen - 1) as uint32;
    let mut bucket = (hashvalue & mask) as usize;

    // While we have not hit a hole in the hashtable and have not hit the
    // desired bucket, we have collided with some other hash value, so try the
    // next bucket location.
    while let Some(entry) = hashtable.skewBucket[bucket].as_ref() {
        if entry.hashvalue == hashvalue {
            break;
        }
        bucket = ((bucket as uint32 + 1) & mask) as usize;
    }

    // Found the desired bucket?
    if hashtable.skewBucket[bucket].is_some() {
        return bucket as i32;
    }

    // There must not be any hashtable entry for this hash value.
    INVALID_SKEW_BUCKET_NO
}

/// `ExecHashSkewTableInsert(HashJoinTable hashtable, TupleTableSlot *slot,
/// uint32 hashvalue, int bucketNumber)` (nodeHash.c:2601) — insert a tuple into
/// the given skew bucket; shrinks the skew table if it overflows its budget.
pub fn ExecHashSkewTableInsert<'mcx>(
    mcx: Mcx<'mcx>,
    hashtable: &mut HashJoinTableData<'mcx>,
    estate: &mut types_nodes::EStateData<'mcx>,
    slot: types_nodes::SlotId,
    hashvalue: uint32,
    bucketNumber: i32,
) -> PgResult<()> {
    // bool shouldFree;
    // MinimalTuple tuple = ExecFetchSlotMinimalTuple(slot, &shouldFree);
    //
    // The owned model always copies into mcx, so the C shouldFree /
    // heap_free_minimal_tuple bookkeeping is internal to the owner.
    let (mut tuple, _should_free) =
        backend_executor_execTuples_seams::exec_fetch_slot_minimal_tuple::call(mcx, estate, slot)?;

    // Create the HashJoinTuple.
    //   hashTupleSize = HJTUPLE_OVERHEAD + tuple->t_len;
    //   hashTuple = MemoryContextAlloc(hashtable->batchCxt, hashTupleSize);
    //   hashTuple->hashvalue = hashvalue;
    //   memcpy(HJTUPLE_MINTUPLE(hashTuple), tuple, tuple->t_len);
    //   HeapTupleHeaderClearMatch(HJTUPLE_MINTUPLE(hashTuple));
    let hashTupleSize: Size = HJTUPLE_OVERHEAD + tuple.tuple.t_len as usize;
    // HeapTupleHeaderClearMatch: tup->t_infomask2 &= ~HEAP_TUPLE_HAS_MATCH.
    tuple.tuple.t_infomask2 &= !HEAP_TUPLE_HAS_MATCH;
    let hashTuple = HashJoinTupleData {
        next: HashJoinTupleLink::SkewUnshared(None),
        hashvalue,
        mintuple: tuple,
    };
    // Push the tuple into the SEPARATE skew arena (C: MemoryContextAlloc in
    // batchCxt, a distinct allocation from the dense chunks). Storing skew
    // tuples here — not in `tuples` — is what keeps a later
    // ExecHashIncreaseNumBatches `mem::replace` on `tuples` from renumbering
    // (and corrupting) the live skew-bucket chains.
    hashtable.skew_tuples.try_reserve(1).map_err(|_| mcx.oom(hashTupleSize))?;
    let idx = types_nodes::nodehash::SkewTupleIdx(hashtable.skew_tuples.len());
    hashtable.skew_tuples.push(hashTuple);

    // Push it onto the front of the skew bucket's list.
    //   hashTuple->next.unshared = hashtable->skewBucket[bucketNumber]->tuples;
    //   hashtable->skewBucket[bucketNumber]->tuples = hashTuple;
    //   Assert(hashTuple != hashTuple->next.unshared);
    {
        let bucket = hashtable.skewBucket[bucketNumber as usize]
            .as_mut()
            .expect("skew bucket slot must be occupied for an insert");
        let old_head = bucket.tuples;
        hashtable.skew_tuples[idx.0].next = HashJoinTupleLink::SkewUnshared(old_head);
        bucket.tuples = Some(idx);
        debug_assert!(old_head != Some(idx));
    }

    // Account for space used, and back off if we've used too much.
    hashtable.spaceUsed += hashTupleSize;
    hashtable.spaceUsedSkew += hashTupleSize;
    if hashtable.spaceUsed > hashtable.spacePeak {
        hashtable.spacePeak = hashtable.spaceUsed;
    }
    while hashtable.spaceUsedSkew > hashtable.spaceAllowedSkew {
        ExecHashRemoveNextSkewBucket(mcx, hashtable)?;
    }

    // Check we are not over the total spaceAllowed, either.
    if hashtable.spaceUsed > hashtable.spaceAllowed {
        ExecHashIncreaseNumBatches(mcx, hashtable)?;
    }

    // if (shouldFree) heap_free_minimal_tuple(tuple): the owned copy lives in
    // the arena; the original slot tuple is the owner's concern.
    Ok(())
}

/// `ExecHashRemoveNextSkewBucket(HashJoinTable hashtable)` (nodeHash.c:2647) —
/// remove the least valuable skew bucket, moving its tuples to the main table.
pub fn ExecHashRemoveNextSkewBucket<'mcx>(
    mcx: Mcx<'mcx>,
    hashtable: &mut HashJoinTableData<'mcx>,
) -> PgResult<()> {
    // Locate the bucket to remove.
    let bucketToRemove = hashtable.skewBucketNums[(hashtable.nSkewBuckets - 1) as usize];
    let bucket_idx = bucketToRemove as usize;

    // Calculate which bucket and batch the tuples belong to in the main
    // hashtable. They all have the same hash value, so it's the same for all of
    // them. Also note that it's not possible for nbatch to increase while we
    // are processing the tuples.
    let hashvalue = hashtable.skewBucket[bucket_idx]
        .as_ref()
        .expect("skew bucket slot must be occupied")
        .hashvalue;
    let bb = ExecHashGetBucketAndBatch(hashtable, hashvalue);
    let bucketno = bb.bucketno;
    let batchno = bb.batchno;

    // Process all tuples in the bucket.
    let mut hashTuple = hashtable.skewBucket[bucket_idx]
        .as_ref()
        .expect("skew bucket slot must be occupied")
        .tuples;
    while let Some(cur) = hashTuple {
        // This code must agree with ExecHashTableInsert. We do not use
        // ExecHashTableInsert directly as ExecHashTableInsert expects a
        // TupleTableSlot while we already have HashJoinTuples. `cur` indexes the
        // SEPARATE skew arena (`skew_tuples`); the copy below moves the tuple
        // into the dense `tuples` arena.
        let nextHashTuple = match hashtable.skew_tuples[cur.0].next {
            HashJoinTupleLink::SkewUnshared(n) => n,
            HashJoinTupleLink::Unshared(_) | HashJoinTupleLink::Shared(_) => {
                // The skew table is a serial-mode-only structure and its chains
                // only ever carry SkewUnshared links.
                unreachable!("skew bucket tuple has non-skew link (nodeHash.c:2647)")
            }
        };
        let tuple_t_len = hashtable.skew_tuples[cur.0].mintuple.tuple.t_len as usize;
        let tupleSize: Size = HJTUPLE_OVERHEAD + tuple_t_len;

        // Decide whether to put the tuple in the hash table or a temp file.
        if batchno == hashtable.curbatch {
            // Move the tuple to the main hash table.
            //
            // We must copy the tuple into the dense storage, else it will not
            // be found by, eg, ExecHashIncreaseNumBatches.
            let copyTuple = dense_alloc(mcx, hashtable, tupleSize)?;

            // memcpy(copyTuple, hashTuple, tupleSize): copy the header + inline
            // MinimalTuple image from the skew tuple into the dense-storage slot.
            let mintuple = hashtable.skew_tuples[cur.0].mintuple.clone_in(mcx)?;
            let src_hashvalue = hashtable.skew_tuples[cur.0].hashvalue;
            {
                let dst = &mut hashtable.tuples[copyTuple.0];
                dst.hashvalue = src_hashvalue;
                dst.mintuple = mintuple;
            }

            // pfree(hashTuple): the owned-model skew tuple stays in the arena
            // (the arena is reclaimed when the batch context is reset); only
            // the chain links are rewired.

            // copyTuple->next.unshared = hashtable->buckets.unshared[bucketno];
            // hashtable->buckets.unshared[bucketno] = copyTuple;
            match &mut hashtable.buckets {
                HashJoinBuckets::Unshared(heads) => {
                    let old_head = heads[bucketno as usize];
                    hashtable.tuples[copyTuple.0].next =
                        HashJoinTupleLink::Unshared(old_head);
                    heads[bucketno as usize] = Some(copyTuple);
                }
                HashJoinBuckets::Shared(_) => {
                    unreachable!("serial skew path with shared buckets (nodeHash.c:2647)")
                }
            }

            // We have reduced skew space, but overall space doesn't change.
            hashtable.spaceUsedSkew -= tupleSize;
        } else {
            // Put the tuple into a temp file for later batches.
            debug_assert!(batchno > hashtable.curbatch);
            let blob = crate::hash_table::mintuple_to_flat(
                mcx,
                &hashtable.skew_tuples[cur.0].mintuple,
            )?;
            let file = &mut hashtable.innerBatchFile[batchno as usize];
            backend_executor_nodeHashjoin_seams::ExecHashJoinSaveTuple::call(
                mcx, &blob, hashvalue, file,
            )?;
            // pfree(hashTuple): see note above.
            hashtable.spaceUsed -= tupleSize;
            hashtable.spaceUsedSkew -= tupleSize;
        }

        hashTuple = nextHashTuple;

        // Allow this loop to be cancellable.
        // CHECK_FOR_INTERRUPTS(): owned by tcop/postgres; the interrupt seam is
        // a no-op for the skew data structure's correctness and is a
        // cross-family Cargo concern, so the call site is left documented here.
    }

    // Free the bucket struct itself and reset the hashtable entry to NULL.
    //
    // NOTE: this is not nearly as simple as it looks on the surface, because of
    // the possibility of collisions in the hashtable. Suppose that hash values
    // A and B collide at a particular hashtable entry, and that A was entered
    // first so B gets shifted to a different table entry. If we were to remove
    // A first then ExecHashGetSkewBucket would mistakenly start reporting that
    // B is not in the hashtable, because it would hit the NULL before finding
    // B. However, we always remove entries in the reverse order of creation, so
    // this failure cannot happen.
    hashtable.skewBucket[bucket_idx] = None;
    hashtable.nSkewBuckets -= 1;
    // pfree(bucket): the Box is dropped by the assignment above.
    hashtable.spaceUsed -= SKEW_BUCKET_OVERHEAD;
    hashtable.spaceUsedSkew -= SKEW_BUCKET_OVERHEAD;

    // If we have removed all skew buckets then give up on skew optimization.
    // Release the arrays since they aren't useful any more.
    if hashtable.nSkewBuckets == 0 {
        hashtable.skewEnabled = false;
        // pfree(skewBucket) / pfree(skewBucketNums): clear the owned arrays.
        hashtable.skewBucket = mcx::PgVec::new_in(mcx);
        hashtable.skewBucketNums = mcx::PgVec::new_in(mcx);
        hashtable.spaceUsed -= hashtable.spaceUsedSkew;
        hashtable.spaceUsedSkew = 0;
    }

    Ok(())
}
