//! `nodeHash.c` / `executor/hashjoin.h` type vocabulary: the hash-join hash
//! table machinery and the `Hash` plan / `HashState` / `HashJoinState`
//! executor nodes (`nodes/plannodes.h`, `nodes/execnodes.h`).
//!
//! Trimmed, owned-tree port mirroring PostgreSQL 18.3 field-for-field. C
//! unions over `unshared`/`shared` links become Rust enums; counted arrays
//! become `PgVec`; the dense-allocation chunk byte buffers and per-tuple
//! `MinimalTuple` images that C carves out of palloc'd chunks become owned
//! arenas. Sibling-subsystem objects C reaches by pointer
//! (`BufFile`/`SharedTuplestoreAccessor`/`dsa_area`) are the opacity-inherited
//! handle newtypes from `types-execparallel`; the genuinely-shared DSM ABI
//! structs carry their real `Barrier`/`LWLock`/`SharedFileSet`/atomic fields.

use alloc::boxed::Box;
use mcx::{Mcx, PgBox, PgVec};

use types_condvar::Barrier;
use types_core::{uint32, AttrNumber, Cardinality, Oid, Size};
use types_core::FmgrInfo;
use types_execparallel::{
    BufFileHandle, DsaAreaHandle, DsaPointer, SharedTuplestoreAccessorHandle,
};
use types_storage::storage::{pg_atomic_uint32, pg_atomic_uint64, LWLock};
use types_storage::fileset::SharedFileSet;
use types_tuple::heaptuple::MinimalTupleData;

use crate::execexpr::ExprState;
use crate::execnodes::PlanStateData;
use crate::jointype::JoinStateData;
use crate::nodeindexscan::Plan;
use crate::primnodes::Expr;
use crate::TupleTableSlot;

// ===========================================================================
//                          Constants (hashjoin.h)
// ===========================================================================

/// `INVALID_SKEW_BUCKET_NO` (hashjoin.h).
pub const INVALID_SKEW_BUCKET_NO: i32 = -1;
/// `SKEW_HASH_MEM_PERCENT` (hashjoin.h).
pub const SKEW_HASH_MEM_PERCENT: i32 = 2;
/// `SKEW_MIN_OUTER_FRACTION` (hashjoin.h).
pub const SKEW_MIN_OUTER_FRACTION: f64 = 0.01;

/// `HASH_CHUNK_SIZE` (hashjoin.h) — `(Size) (32 * 1024)`.
pub const HASH_CHUNK_SIZE: Size = 32 * 1024;
/// `HASH_CHUNK_THRESHOLD` (hashjoin.h) — `HASH_CHUNK_SIZE / 4`.
pub const HASH_CHUNK_THRESHOLD: Size = HASH_CHUNK_SIZE / 4;

// The build-barrier phases (hashjoin.h), used by `build_barrier`.
pub const PHJ_BUILD_ELECT: i32 = 0;
pub const PHJ_BUILD_ALLOCATE: i32 = 1;
pub const PHJ_BUILD_HASH_INNER: i32 = 2;
pub const PHJ_BUILD_HASH_OUTER: i32 = 3;
pub const PHJ_BUILD_RUN: i32 = 4;
pub const PHJ_BUILD_FREE: i32 = 5;

// The batch-barrier phases (hashjoin.h), used by `batch_barrier`.
pub const PHJ_BATCH_ELECT: i32 = 0;
pub const PHJ_BATCH_ALLOCATE: i32 = 1;
pub const PHJ_BATCH_LOAD: i32 = 2;
pub const PHJ_BATCH_PROBE: i32 = 3;
pub const PHJ_BATCH_SCAN: i32 = 4;
pub const PHJ_BATCH_FREE: i32 = 5;

// The batch-growth phases (hashjoin.h), used by `grow_batches_barrier`.
pub const PHJ_GROW_BATCHES_ELECT: i32 = 0;
pub const PHJ_GROW_BATCHES_REALLOCATE: i32 = 1;
pub const PHJ_GROW_BATCHES_REPARTITION: i32 = 2;
pub const PHJ_GROW_BATCHES_DECIDE: i32 = 3;
pub const PHJ_GROW_BATCHES_FINISH: i32 = 4;

/// `PHJ_GROW_BATCHES_PHASE(n)` — circular phases, `(n) % 5`.
#[inline]
pub const fn PHJ_GROW_BATCHES_PHASE(n: i32) -> i32 {
    n % 5
}

// The bucket-growth phases (hashjoin.h), used by `grow_buckets_barrier`.
pub const PHJ_GROW_BUCKETS_ELECT: i32 = 0;
pub const PHJ_GROW_BUCKETS_REALLOCATE: i32 = 1;
pub const PHJ_GROW_BUCKETS_REINSERT: i32 = 2;

/// `PHJ_GROW_BUCKETS_PHASE(n)` — circular phases, `(n) % 3`.
#[inline]
pub const fn PHJ_GROW_BUCKETS_PHASE(n: i32) -> i32 {
    n % 3
}

// ===========================================================================
//                    Owned-tree arena indices for chains
// ===========================================================================

/// Index of a [`HashJoinTupleData`] in [`HashJoinTableData::tuples`]. The
/// owned model stores each tuple once and links chains by index, since the C
/// `HashJoinTuple` pointers point into the dense-allocation chunk buffers and
/// cannot be modeled as a self-referential graph of `Box`es.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct HashTupleIdx(pub usize);

/// Index of a [`HashMemoryChunkData`] in [`HashJoinTableData::chunk_arena`].
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct HashChunkIdx(pub usize);

/// `union { HashJoinTuple unshared; dsa_pointer shared; } next` — the per-tuple
/// bucket-chain link.
#[derive(Clone, Copy, Debug)]
pub enum HashJoinTupleLink {
    /// `HashJoinTuple unshared` — next tuple in same bucket (serial mode);
    /// `None` is the C `NULL`.
    Unshared(Option<HashTupleIdx>),
    /// `dsa_pointer shared` — next tuple in same bucket (parallel mode).
    Shared(DsaPointer),
}

/// `union { HashMemoryChunkData *unshared; dsa_pointer shared; } next`.
#[derive(Clone, Copy, Debug)]
pub enum HashMemoryChunkLink {
    /// `HashMemoryChunk unshared` — next chunk (serial); `None` is C `NULL`.
    Unshared(Option<HashChunkIdx>),
    /// `dsa_pointer shared` — next chunk (parallel mode).
    Shared(DsaPointer),
}

/// `union { HashJoinTupleData **unshared; dsa_pointer_atomic *shared; } buckets`
/// — the per-batch in-memory bucket-head array.
pub enum HashJoinBuckets<'mcx> {
    /// `struct HashJoinTupleData **unshared` — per-batch storage; each entry is
    /// the head index of the bucket's chain (`None` = empty bucket).
    Unshared(PgVec<'mcx, Option<HashTupleIdx>>),
    /// `dsa_pointer_atomic *shared` — per-query DSA-area bucket heads.
    Shared(PgVec<'mcx, pg_atomic_uint64>),
}

// ===========================================================================
//                     Hash-table structures (hashjoin.h)
// ===========================================================================

/// `HashJoinTupleData` (hashjoin.h). C stores the `MinimalTuple` inline after
/// the header on a MAXALIGN boundary; here it is an owned image.
pub struct HashJoinTupleData<'mcx> {
    /// `union next` — link to next tuple in same bucket.
    pub next: HashJoinTupleLink,
    /// `uint32 hashvalue` — tuple's hash code.
    pub hashvalue: uint32,
    /// the inline `MinimalTuple` payload.
    pub mintuple: MinimalTupleData<'mcx>,
}

/// `HashSkewBucket` (hashjoin.h).
pub struct HashSkewBucket {
    /// `uint32 hashvalue` — common hash value.
    pub hashvalue: uint32,
    /// `HashJoinTuple tuples` — linked list of inner-relation tuples (head
    /// index into [`HashJoinTableData::tuples`]); `None` = empty.
    pub tuples: Option<HashTupleIdx>,
}

/// `HashMemoryChunkData` (hashjoin.h) — a dense-allocation chunk header. The C
/// chunk's tuple buffer follows the header inline; the owned model carries only
/// the byte-accounting fields used by `spaceUsed`.
pub struct HashMemoryChunkData {
    /// `int ntuples` — number of tuples stored in this chunk.
    pub ntuples: i32,
    /// `size_t maxlen` — size of the chunk's tuple buffer.
    pub maxlen: Size,
    /// `size_t used` — number of buffer bytes already used.
    pub used: Size,
    /// `union next` — pointer to the next chunk (linked list).
    pub next: HashMemoryChunkLink,
}

/// `ParallelHashGrowth` (hashjoin.h) — the growth flag participants set while
/// hashing the inner relation.
#[repr(i32)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ParallelHashGrowth {
    /// The current dimensions are sufficient.
    PHJ_GROWTH_OK = 0,
    /// The load factor is too high, so we need to add buckets.
    PHJ_GROWTH_NEED_MORE_BUCKETS = 1,
    /// The memory budget would be exhausted, so we need to repartition.
    PHJ_GROWTH_NEED_MORE_BATCHES = 2,
    /// Repartitioning didn't help last time, so don't try again.
    PHJ_GROWTH_DISABLED = 3,
}

/// `ParallelHashJoinBatch` (hashjoin.h) — a DSM-resident per-batch coordination
/// object. In C it is immediately followed in memory by variable-sized
/// `SharedTuplestore` objects; here those are reached through the
/// shared-tuplestore owner's accessors (see [`ParallelHashJoinBatchAccessor`]).
#[repr(C)]
pub struct ParallelHashJoinBatch {
    /// `dsa_pointer buckets` — array of hash table buckets.
    pub buckets: DsaPointer,
    /// `Barrier batch_barrier` — synchronization for joining this batch.
    pub batch_barrier: Barrier,
    /// `dsa_pointer chunks` — chunks of tuples loaded.
    pub chunks: DsaPointer,
    /// `size_t size` — size of buckets + chunks in memory.
    pub size: Size,
    /// `size_t estimated_size` — size of buckets + chunks while writing.
    pub estimated_size: Size,
    /// `size_t ntuples` — number of tuples loaded.
    pub ntuples: Size,
    /// `size_t old_ntuples` — number of tuples before repartitioning.
    pub old_ntuples: Size,
    /// `bool space_exhausted`.
    pub space_exhausted: bool,
    /// `bool skip_unmatched` — whether to abandon unmatched scan.
    pub skip_unmatched: bool,
}

/// `ParallelHashJoinBatchAccessor` (hashjoin.h) — each backend's per-batch
/// state for interacting with a [`ParallelHashJoinBatch`].
pub struct ParallelHashJoinBatchAccessor {
    /// `ParallelHashJoinBatch *shared` — the shared batch this accessor wraps
    /// (resolved from the DSA area via the batches base pointer).
    pub shared: DsaPointer,
    /// `size_t preallocated` — pre-allocated space for this backend.
    pub preallocated: Size,
    /// `size_t ntuples` — number of tuples.
    pub ntuples: Size,
    /// `size_t size` — size of partition in memory.
    pub size: Size,
    /// `size_t estimated_size` — size of partition on disk.
    pub estimated_size: Size,
    /// `size_t old_ntuples` — how many tuples before repartitioning.
    pub old_ntuples: Size,
    /// `bool at_least_one_chunk` — has this backend allocated a chunk?
    pub at_least_one_chunk: bool,
    /// `bool outer_eof` — has this process hit end of batch?
    pub outer_eof: bool,
    /// `bool done` — flag to remember that a batch is done.
    pub done: bool,
    /// `SharedTuplestoreAccessor *inner_tuples`.
    pub inner_tuples: Option<SharedTuplestoreAccessorHandle>,
    /// `SharedTuplestoreAccessor *outer_tuples`.
    pub outer_tuples: Option<SharedTuplestoreAccessorHandle>,
}

/// `ParallelHashJoinState` (hashjoin.h) — the shared state coordinating a
/// Parallel Hash Join, stored in the DSM segment. Carries real
/// `Barrier`/`LWLock`/`SharedFileSet`/atomic fields (genuinely shared).
#[repr(C)]
pub struct ParallelHashJoinState {
    /// `dsa_pointer batches` — array of [`ParallelHashJoinBatch`].
    pub batches: DsaPointer,
    /// `dsa_pointer old_batches` — previous generation during repartition.
    pub old_batches: DsaPointer,
    /// `int nbatch` — number of batches now.
    pub nbatch: i32,
    /// `int old_nbatch` — previous number of batches.
    pub old_nbatch: i32,
    /// `int nbuckets` — number of buckets.
    pub nbuckets: i32,
    /// `ParallelHashGrowth growth` — control batch/bucket growth.
    pub growth: ParallelHashGrowth,
    /// `dsa_pointer chunk_work_queue` — chunk work queue.
    pub chunk_work_queue: DsaPointer,
    /// `int nparticipants`.
    pub nparticipants: i32,
    /// `size_t space_allowed`.
    pub space_allowed: Size,
    /// `size_t total_tuples` — total number of inner tuples.
    pub total_tuples: Size,
    /// `LWLock lock` — lock protecting the above.
    pub lock: LWLock,
    /// `Barrier build_barrier` — synchronization for the build phases.
    pub build_barrier: Barrier,
    /// `Barrier grow_batches_barrier`.
    pub grow_batches_barrier: Barrier,
    /// `Barrier grow_buckets_barrier`.
    pub grow_buckets_barrier: Barrier,
    /// `pg_atomic_uint32 distributor` — counter for load balancing.
    pub distributor: pg_atomic_uint32,
    /// `SharedFileSet fileset` — space for shared temporary files.
    pub fileset: SharedFileSet,
}

/// `HashJoinTableData` (hashjoin.h) — the per-hashjoin hash table, palloc'd in
/// the executor's per-query context.
pub struct HashJoinTableData<'mcx> {
    /// `int nbuckets` — # buckets in the in-memory hash table.
    pub nbuckets: i32,
    /// `int log2_nbuckets` — its log2 (nbuckets must be a power of 2).
    pub log2_nbuckets: i32,
    /// `int nbuckets_original` — # buckets when starting the first hash.
    pub nbuckets_original: i32,
    /// `int nbuckets_optimal` — optimal # buckets (per batch).
    pub nbuckets_optimal: i32,
    /// `int log2_nbuckets_optimal`.
    pub log2_nbuckets_optimal: i32,
    /// `union buckets` — bucket-head array.
    pub buckets: HashJoinBuckets<'mcx>,
    /// `bool skewEnabled` — are we using skew optimization?
    pub skewEnabled: bool,
    /// `HashSkewBucket **skewBucket` — hashtable of skew buckets (`None` = the
    /// C `NULL` slot).
    pub skewBucket: PgVec<'mcx, Option<Box<HashSkewBucket>>>,
    /// `int skewBucketLen` — size of skewBucket array (a power of 2).
    pub skewBucketLen: i32,
    /// `int nSkewBuckets` — number of active skew buckets.
    pub nSkewBuckets: i32,
    /// `int *skewBucketNums` — array indexes of active skew buckets.
    pub skewBucketNums: PgVec<'mcx, i32>,
    /// `int nbatch` — number of batches.
    pub nbatch: i32,
    /// `int curbatch` — current batch #; 0 during 1st pass.
    pub curbatch: i32,
    /// `int nbatch_original` — nbatch when we started inner scan.
    pub nbatch_original: i32,
    /// `int nbatch_outstart` — nbatch when we started outer scan.
    pub nbatch_outstart: i32,
    /// `bool growEnabled` — flag to shut off nbatch increases.
    pub growEnabled: bool,
    /// `double totalTuples` — # tuples obtained from inner plan.
    pub totalTuples: f64,
    /// `double partialTuples` — # tuples obtained from inner plan by me.
    pub partialTuples: f64,
    /// `double skewTuples` — # tuples inserted into skew tuples.
    pub skewTuples: f64,
    /// `BufFile **innerBatchFile` — temp file per batch (`None` until first
    /// write opens it).
    pub innerBatchFile: PgVec<'mcx, Option<BufFileHandle>>,
    /// `BufFile **outerBatchFile` — temp file per batch.
    pub outerBatchFile: PgVec<'mcx, Option<BufFileHandle>>,
    /// `Size spaceUsed` — memory space currently used by tuples.
    pub spaceUsed: Size,
    /// `Size spaceAllowed` — upper limit for space used.
    pub spaceAllowed: Size,
    /// `Size spacePeak` — peak space used.
    pub spacePeak: Size,
    /// `Size spaceUsedSkew` — skew hash table's current space usage.
    pub spaceUsedSkew: Size,
    /// `Size spaceAllowedSkew` — upper limit for skew hashtable.
    pub spaceAllowedSkew: Size,
    /// `HashMemoryChunk chunks` — dense-allocation chunk list head (index into
    /// [`Self::chunk_arena`]); `None` = empty.
    pub chunks: Option<HashChunkIdx>,
    /// `HashMemoryChunk current_chunk` — this backend's current chunk.
    pub current_chunk: Option<HashChunkIdx>,
    /// `dsa_area *area` — DSA area to allocate from (`None` in serial mode).
    pub area: Option<DsaAreaHandle>,
    /// `ParallelHashJoinState *parallel_state` — `None` in serial mode.
    pub parallel_state: Option<DsaPointer>,
    /// `ParallelHashJoinBatchAccessor *batches`.
    pub batches: PgVec<'mcx, ParallelHashJoinBatchAccessor>,
    /// `dsa_pointer current_chunk_shared`.
    pub current_chunk_shared: DsaPointer,
    /// OWNED-MODEL arena: every in-memory current-batch [`HashJoinTupleData`].
    /// C carves these out of the dense-allocation chunk byte buffers; the owned
    /// model stores them here once and indexes them with [`HashTupleIdx`].
    pub tuples: PgVec<'mcx, HashJoinTupleData<'mcx>>,
    /// OWNED-MODEL arena: the dense-allocation chunk headers, linked by
    /// [`HashChunkIdx`].
    pub chunk_arena: PgVec<'mcx, HashMemoryChunkData>,
}

// ===========================================================================
//                Per-crate helper vocabulary the bodies consume
// ===========================================================================

/// The `(bucketno, batchno)` pair `ExecHashGetBucketAndBatch` computes (C
/// writes through two out-pointers).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BucketAndBatch {
    pub bucketno: i32,
    pub batchno: i32,
}

/// `AttStatsSlot` (`utils/lsyscache.h`) — the values/numbers pulled from a
/// `pg_statistic` MCV slot by `get_attstatsslot`, consumed by
/// `ExecHashBuildSkewHash`. Trimmed to the fields the skew build reads.
pub struct AttStatsSlot<'mcx> {
    /// `Oid staop` — operator OID.
    pub staop: Oid,
    /// `Oid stacoll` — collation OID.
    pub stacoll: Oid,
    /// `int nvalues` — number of MCV `values`.
    pub nvalues: i32,
    /// `Datum *values` — the MCV datums.
    pub values: PgVec<'mcx, types_datum::Datum>,
    /// `int nnumbers` — number of `numbers`.
    pub nnumbers: i32,
    /// `float4 *numbers` — the MCV frequencies.
    pub numbers: PgVec<'mcx, f32>,
}

// ===========================================================================
//                   Plan / executor node structs
// ===========================================================================

/// `Hash` plan node (`nodes/plannodes.h`).
pub struct Hash<'mcx> {
    /// `Plan plan`.
    pub plan: Plan<'mcx>,
    /// `List *hashkeys` — hash keys for the hashjoin condition.
    pub hashkeys: Option<PgVec<'mcx, Expr>>,
    /// `Oid skewTable` — outer join key's table OID, or `InvalidOid`.
    pub skewTable: Oid,
    /// `AttrNumber skewColumn` — outer join key's column #, or zero.
    pub skewColumn: AttrNumber,
    /// `bool skewInherit` — is outer join rel an inheritance tree?
    pub skewInherit: bool,
    /// `Cardinality rows_total` — estimate total rows if parallel_aware.
    pub rows_total: Cardinality,
}

/// `HashInstrumentation` (`nodes/execnodes.h`) — per-process hash-build stats.
#[derive(Clone, Copy, Debug, Default)]
pub struct HashInstrumentation {
    /// `int nbuckets` — number of buckets at end of execution.
    pub nbuckets: i32,
    /// `int nbuckets_original` — planned number of buckets.
    pub nbuckets_original: i32,
    /// `int nbatch` — number of batches at end of execution.
    pub nbatch: i32,
    /// `int nbatch_original` — planned number of batches.
    pub nbatch_original: i32,
    /// `Size space_peak` — peak memory usage in bytes.
    pub space_peak: Size,
}

/// `SharedHashInfo` (`nodes/execnodes.h`) — DSM-resident array of per-worker
/// [`HashInstrumentation`] (C uses a `FLEXIBLE_ARRAY_MEMBER`).
pub struct SharedHashInfo<'mcx> {
    /// `int num_workers`.
    pub num_workers: i32,
    /// `HashInstrumentation hinstrument[]`.
    pub hinstrument: PgVec<'mcx, HashInstrumentation>,
}

/// `HashState` (`nodes/execnodes.h`) — the Hash executor node.
pub struct HashState<'mcx> {
    /// `PlanState ps` — its first field is `NodeTag`.
    pub ps: PlanStateData<'mcx>,
    /// `HashJoinTable hashtable` — hash table for the hashjoin (`None` = NULL).
    pub hashtable: Option<PgBox<'mcx, HashJoinTableData<'mcx>>>,
    /// `ExprState *hash_expr` — ExprState to get hash value.
    pub hash_expr: Option<PgBox<'mcx, ExprState>>,
    /// `FmgrInfo *skew_hashfunction` — lookup data for skew hash function.
    pub skew_hashfunction: Option<PgBox<'mcx, FmgrInfo>>,
    /// `Oid skew_collation` — collation to call skew_hashfunction with.
    pub skew_collation: Oid,
    /// `SharedHashInfo *shared_info` — leader's pointer to the shared stats
    /// area; `None` in workers / non-parallel joins.
    pub shared_info: Option<PgBox<'mcx, SharedHashInfo<'mcx>>>,
    /// `HashInstrumentation *hinstrument` — this process's stats collection
    /// area (local or shared); `None` when not collecting.
    pub hinstrument: Option<PgBox<'mcx, HashInstrumentation>>,
    /// `struct ParallelHashJoinState *parallel_state` — `None` in serial mode.
    pub parallel_state: Option<DsaPointer>,
}

/// `HashJoinState` (`nodes/execnodes.h`) — the HashJoin executor node. Defined
/// here (rather than in a future `nodeHashjoin` types module) because
/// `nodeHash.c`'s probe routines (`ExecScanHashBucket`, …) operate on it.
pub struct HashJoinState<'mcx> {
    /// `JoinState js` — its first field is `NodeTag`.
    pub js: JoinStateData<'mcx>,
    /// `ExprState *hashclauses`.
    pub hashclauses: Option<PgBox<'mcx, ExprState>>,
    /// `ExprState *hj_OuterHash`.
    pub hj_OuterHash: Option<PgBox<'mcx, ExprState>>,
    /// `HashJoinTable hj_HashTable`.
    pub hj_HashTable: Option<PgBox<'mcx, HashJoinTableData<'mcx>>>,
    /// `uint32 hj_CurHashValue`.
    pub hj_CurHashValue: uint32,
    /// `int hj_CurBucketNo`.
    pub hj_CurBucketNo: i32,
    /// `int hj_CurSkewBucketNo`.
    pub hj_CurSkewBucketNo: i32,
    /// `HashJoinTuple hj_CurTuple` — current tuple in the scan (index into the
    /// hash table's tuple arena); `None` = NULL.
    pub hj_CurTuple: Option<HashTupleIdx>,
    /// `TupleTableSlot *hj_OuterTupleSlot`.
    pub hj_OuterTupleSlot: Option<PgBox<'mcx, TupleTableSlot>>,
    /// `TupleTableSlot *hj_HashTupleSlot`.
    pub hj_HashTupleSlot: Option<PgBox<'mcx, TupleTableSlot>>,
    /// `TupleTableSlot *hj_NullOuterTupleSlot`.
    pub hj_NullOuterTupleSlot: Option<PgBox<'mcx, TupleTableSlot>>,
    /// `TupleTableSlot *hj_NullInnerTupleSlot`.
    pub hj_NullInnerTupleSlot: Option<PgBox<'mcx, TupleTableSlot>>,
    /// `TupleTableSlot *hj_FirstOuterTupleSlot`.
    pub hj_FirstOuterTupleSlot: Option<PgBox<'mcx, TupleTableSlot>>,
    /// `int hj_JoinState`.
    pub hj_JoinState: i32,
    /// `bool hj_MatchedOuter`.
    pub hj_MatchedOuter: bool,
    /// `bool hj_OuterNotEmpty`.
    pub hj_OuterNotEmpty: bool,
}

/// Build a zeroed [`HashInstrumentation`] in `mcx` is unnecessary (it is
/// `Copy`/`Default`); marker kept for parity with C's palloc0 sites is omitted.
#[allow(dead_code)]
const _: () = ();

/// Silence unused-import lints in the scaffold (the bodies consume `Mcx`).
#[allow(dead_code)]
fn _uses_mcx(_m: Mcx<'_>) {}
