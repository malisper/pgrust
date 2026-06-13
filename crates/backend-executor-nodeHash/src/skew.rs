//! The skew-optimization hashtable: build the skew bucket set from the outer
//! relation's MCVs, look up / insert into it, and shrink it on demand.

use mcx::Mcx;
use types_core::{uint32, Size};
use types_error::PgResult;
use types_nodes::nodehash::{
    Hash, HashJoinBuckets, HashJoinTupleLink, HashState, HashJoinTableData,
    INVALID_SKEW_BUCKET_NO,
};

use crate::hash_table::{dense_alloc, ExecHashGetBucketAndBatch};
use crate::{SKEW_BUCKET_OVERHEAD, HJTUPLE_OVERHEAD};

/// `ExecHashBuildSkewHash(HashState *hashstate, HashJoinTable hashtable,
/// Hash *node, int mcvsToUse)` (nodeHash.c:2403) — set up the skew hashtable
/// from the planner's outer-relation MCV list (read from the syscache).
/// Allocates the skew bucket array in `mcx`.
pub fn ExecHashBuildSkewHash<'mcx>(
    mcx: Mcx<'mcx>,
    hashstate: &mut HashState<'mcx>,
    hashtable: &mut HashJoinTableData<'mcx>,
    node: &Hash<'mcx>,
    mcvsToUse: i32,
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
    //                        ATTSTATSSLOT_VALUES | ATTSTATSSLOT_NUMBERS)) {
    //       ... build the skewBucket[] open-addressing table, hashing each MCV
    //       through FunctionCall1Coll(hashstate->skew_hashfunction, ...) ...
    //   }
    //   ReleaseSysCache(statsTuple);
    //
    // The pg_statistic MCV lookup (SearchSysCache3(STATRELATTINH) /
    // get_attstatsslot / free_attstatsslot / ReleaseSysCache) and the per-MCV
    // skew hash function (FunctionCall1Coll on hashstate->skew_hashfunction)
    // are owned by backend-utils-cache-syscache and backend-utils-fmgr-fmgr.
    // Neither owner declares the STATRELATTINH MCV-slot projection nor the
    // collation-aware fmgr call in its seam crate yet, so the build path is a
    // loud seam-boundary panic until they land. The two guards above are the
    // real C logic that precedes the lookup.
    let _ = (mcx, hashstate, hashtable);
    panic!(
        "backend-utils-cache-syscache: SearchSysCache3(STATRELATTINH) + \
         get_attstatsslot(STATISTIC_KIND_MCV), and backend-utils-fmgr-fmgr: \
         FunctionCall1Coll(hashstate.skew_hashfunction) not yet ported \
         (nodeHash.c:2403 ExecHashBuildSkewHash)"
    );
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
    slot: &types_nodes::TupleTableSlot,
    hashvalue: uint32,
    bucketNumber: i32,
) -> PgResult<()> {
    // bool shouldFree;
    // MinimalTuple tuple = ExecFetchSlotMinimalTuple(slot, &shouldFree);
    //
    // ExecFetchSlotMinimalTuple is owned by backend-executor-execTuples, which
    // does not yet declare it in its seam crate. The remaining logic below is
    // the real C body; it panics at the seam boundary until that owner lands.
    let _ = (mcx, hashtable, slot, hashvalue, bucketNumber);
    panic!(
        "backend-executor-execTuples: ExecFetchSlotMinimalTuple / \
         heap_free_minimal_tuple not yet ported (nodeHash.c:2601 \
         ExecHashSkewTableInsert)"
    );
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
        // TupleTableSlot while we already have HashJoinTuples.
        let nextHashTuple = match hashtable.tuples[cur.0].next {
            HashJoinTupleLink::Unshared(n) => n,
            HashJoinTupleLink::Shared(_) => {
                // The skew table is a serial-mode-only structure.
                unreachable!("skew bucket tuple has shared link (nodeHash.c:2647)")
            }
        };
        let tuple_t_len = hashtable.tuples[cur.0].mintuple.t_len as usize;
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
            let mintuple = hashtable.tuples[cur.0].mintuple.clone_in(mcx)?;
            let src_hashvalue = hashtable.tuples[cur.0].hashvalue;
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
            let mintuple = hashtable.tuples[cur.0].mintuple.clone_in(mcx)?;
            let file = &mut hashtable.innerBatchFile[batchno as usize];
            backend_executor_nodeHashjoin_seams::ExecHashJoinSaveTuple::call(
                mcx, &mintuple, hashvalue, file,
            )?;
            // pfree(hashTuple): see note above.
            hashtable.spaceUsed -= tupleSize;
            hashtable.spaceUsedSkew -= tupleSize;
        }

        hashTuple = nextHashTuple;

        // Allow this loop to be cancellable.
        // CHECK_FOR_INTERRUPTS(): owned by the signal/interrupt subsystem;
        // a no-op stand-in here would silently drop cancellation, so this
        // call point is left as the documented C site without a fake check.
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
