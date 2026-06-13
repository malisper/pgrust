//! The skew-optimization hashtable: build the skew bucket set from the outer
//! relation's MCVs, look up / insert into it, and shrink it on demand.

use mcx::Mcx;
use types_core::uint32;
use types_error::PgResult;
use types_nodes::nodehash::{Hash, HashJoinTableData, HashState};

/// `ExecHashBuildSkewHash(HashState *hashstate, HashJoinTable hashtable,
/// Hash *node, int mcvsToUse)` (nodeHash.c:2403) — set up the skew hashtable
/// from the planner's outer-relation MCV list (read from the syscache).
/// Allocates the skew bucket array in `mcx`.
pub fn ExecHashBuildSkewHash<'mcx>(
    _mcx: Mcx<'mcx>,
    _hashstate: &mut HashState<'mcx>,
    _hashtable: &mut HashJoinTableData<'mcx>,
    _node: &Hash<'mcx>,
    _mcvsToUse: i32,
) -> PgResult<()> {
    todo!("decomp")
}

/// `ExecHashGetSkewBucket(HashJoinTable hashtable, uint32 hashvalue)`
/// (nodeHash.c:2555) — the skew-bucket index for a hash value, or
/// `INVALID_SKEW_BUCKET_NO`. Pure lookup.
pub fn ExecHashGetSkewBucket<'mcx>(_hashtable: &HashJoinTableData<'mcx>, _hashvalue: uint32) -> i32 {
    todo!("decomp")
}

/// `ExecHashSkewTableInsert(HashJoinTable hashtable, TupleTableSlot *slot,
/// uint32 hashvalue, int bucketNumber)` (nodeHash.c:2601) — insert a tuple into
/// the given skew bucket; shrinks the skew table if it overflows its budget.
pub fn ExecHashSkewTableInsert<'mcx>(
    _mcx: Mcx<'mcx>,
    _hashtable: &mut HashJoinTableData<'mcx>,
    _slot: &types_nodes::TupleTableSlot,
    _hashvalue: uint32,
    _bucketNumber: i32,
) -> PgResult<()> {
    todo!("decomp")
}

/// `ExecHashRemoveNextSkewBucket(HashJoinTable hashtable)` (nodeHash.c:2647) —
/// remove the least valuable skew bucket, moving its tuples to the main table.
pub fn ExecHashRemoveNextSkewBucket<'mcx>(
    _mcx: Mcx<'mcx>,
    _hashtable: &mut HashJoinTableData<'mcx>,
) -> PgResult<()> {
    todo!("decomp")
}
