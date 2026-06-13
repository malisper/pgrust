//! The serial in-memory hash table: create / size / build / probe / grow /
//! reset, plus the dense allocator and the memory-limit helper.

use mcx::{Mcx, PgBox};
use types_core::{Size, uint32};
use types_error::PgResult;
use types_nodes::nodehash::{
    BucketAndBatch, HashJoinState, HashJoinTableData, HashState, HashTupleIdx,
};

/// Out-parameters of `ExecChooseHashTableSize` (nodeHash.c:658) — C writes
/// through `space_allowed` / `numbuckets` / `numbatches` / `num_skew_mcvs`.
#[derive(Clone, Copy, Debug)]
pub struct HashTableSize {
    /// `*space_allowed`.
    pub space_allowed: Size,
    /// `*numbuckets`.
    pub numbuckets: i32,
    /// `*numbatches`.
    pub numbatches: i32,
    /// `*num_skew_mcvs`.
    pub num_skew_mcvs: i32,
}

/// `ExecHashTableCreate(HashState *state)` (nodeHash.c:446) — create an empty
/// hashtable data structure for hashjoin. Allocates the table and its child
/// contexts in `mcx`.
pub fn ExecHashTableCreate<'mcx>(
    _mcx: Mcx<'mcx>,
    _state: &mut HashState<'mcx>,
) -> PgResult<PgBox<'mcx, HashJoinTableData<'mcx>>> {
    todo!("decomp")
}

/// `ExecChooseHashTableSize(ntuples, tupwidth, useskew, try_combined_hash_mem,
/// parallel_workers, ...)` (nodeHash.c:658) — compute the bucket/batch/skew
/// dimensions for the planned input. Pure arithmetic (no allocation).
pub fn ExecChooseHashTableSize(
    _ntuples: f64,
    _tupwidth: i32,
    _useskew: bool,
    _try_combined_hash_mem: bool,
    _parallel_workers: i32,
) -> HashTableSize {
    todo!("decomp")
}

/// `ExecHashTableDestroy(HashJoinTable hashtable)` (nodeHash.c:956) — destroy a
/// hash table, closing its batch temp files. Fallible (`BufFileClose` I/O).
pub fn ExecHashTableDestroy<'mcx>(_hashtable: PgBox<'mcx, HashJoinTableData<'mcx>>) -> PgResult<()> {
    todo!("decomp")
}

/// `ExecHashIncreaseBatchSize(HashJoinTable hashtable)` (nodeHash.c:998) — if
/// the optimal bucket count grew, grow `nbuckets_optimal` accordingly.
pub fn ExecHashIncreaseBatchSize<'mcx>(_hashtable: &mut HashJoinTableData<'mcx>) {
    todo!("decomp")
}

/// `ExecHashIncreaseNumBatches(HashJoinTable hashtable)` (nodeHash.c:1030) —
/// double the number of batches when the in-memory table grew too large,
/// rescanning and dumping moved tuples to their batch files.
pub fn ExecHashIncreaseNumBatches<'mcx>(
    _mcx: Mcx<'mcx>,
    _hashtable: &mut HashJoinTableData<'mcx>,
) -> PgResult<()> {
    todo!("decomp")
}

/// `ExecHashIncreaseNumBuckets(HashJoinTable hashtable)` (nodeHash.c:1587) —
/// double the bucket count and reinsert every in-memory tuple.
pub fn ExecHashIncreaseNumBuckets<'mcx>(
    _mcx: Mcx<'mcx>,
    _hashtable: &mut HashJoinTableData<'mcx>,
) -> PgResult<()> {
    todo!("decomp")
}

/// `ExecHashTableInsert(HashJoinTable hashtable, TupleTableSlot *slot,
/// uint32 hashvalue)` (nodeHash.c:1749) — insert a tuple into the appropriate
/// bucket of the (serial) hash table, spilling to batch files past batch 0.
pub fn ExecHashTableInsert<'mcx>(
    _mcx: Mcx<'mcx>,
    _hashtable: &mut HashJoinTableData<'mcx>,
    _slot: &types_nodes::TupleTableSlot,
    _hashvalue: uint32,
) -> PgResult<()> {
    todo!("decomp")
}

/// `ExecHashGetBucketAndBatch(HashJoinTable hashtable, uint32 hashvalue,
/// int *bucketno, int *batchno)` (nodeHash.c:1960) — split a hash value into
/// its bucket and batch numbers. Pure arithmetic.
pub fn ExecHashGetBucketAndBatch<'mcx>(
    _hashtable: &HashJoinTableData<'mcx>,
    _hashvalue: uint32,
) -> BucketAndBatch {
    todo!("decomp")
}

/// `ExecScanHashBucket(HashJoinState *hjstate, ExprContext *econtext)`
/// (nodeHash.c:1992) — scan a (serial) hash bucket for matches to the current
/// outer tuple; returns `true` when a match was found.
pub fn ExecScanHashBucket<'mcx>(
    _hjstate: &mut HashJoinState<'mcx>,
    _econtext: &mut types_nodes::ExprContext<'mcx>,
) -> PgResult<bool> {
    todo!("decomp")
}

/// `ExecPrepHashTableForUnmatched(HashJoinState *hjstate)` (nodeHash.c:2104) —
/// set up state to scan the (serial) hash table for unmatched inner tuples.
pub fn ExecPrepHashTableForUnmatched<'mcx>(_hjstate: &mut HashJoinState<'mcx>) {
    todo!("decomp")
}

/// `ExecScanHashTableForUnmatched(HashJoinState *hjstate, ExprContext *econtext)`
/// (nodeHash.c:2190) — return the next unmatched inner tuple (serial path).
pub fn ExecScanHashTableForUnmatched<'mcx>(
    _hjstate: &mut HashJoinState<'mcx>,
    _econtext: &mut types_nodes::ExprContext<'mcx>,
) -> PgResult<bool> {
    todo!("decomp")
}

/// `ExecHashTableReset(HashJoinTable hashtable)` (nodeHash.c:2327) — reset the
/// hashtable for a new batch (reset `batchCxt`, zero the bucket array).
pub fn ExecHashTableReset<'mcx>(
    _mcx: Mcx<'mcx>,
    _hashtable: &mut HashJoinTableData<'mcx>,
) -> PgResult<()> {
    todo!("decomp")
}

/// `ExecHashTableResetMatchFlags(HashJoinTable hashtable)` (nodeHash.c:2355) —
/// clear the `HEAP_TUPLE_HAS_MATCH` flag on every in-memory and skew tuple
/// (for right/full joins rescanned per batch).
pub fn ExecHashTableResetMatchFlags<'mcx>(_hashtable: &mut HashJoinTableData<'mcx>) {
    todo!("decomp")
}

/// `dense_alloc(HashJoinTable hashtable, Size size)` (nodeHash.c:2896) —
/// allocate space for `size` bytes of tuple data within the dense-allocation
/// chunks; returns the staged tuple's arena index. Allocates in `mcx`.
pub fn dense_alloc<'mcx>(
    _mcx: Mcx<'mcx>,
    _hashtable: &mut HashJoinTableData<'mcx>,
    _size: Size,
) -> PgResult<HashTupleIdx> {
    todo!("decomp")
}

/// `get_hash_memory_limit(void)` (nodeHash.c:3622) — the per-hash memory budget
/// in bytes (`work_mem * hash_mem_multiplier`, capped at `SIZE_MAX`). Reads the
/// backend GUCs; passed explicitly until the GUC owner lands.
pub fn get_hash_memory_limit(_work_mem: i32, _hash_mem_multiplier: f64) -> Size {
    todo!("decomp")
}
