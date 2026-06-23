//! `nodeHash.c` ABI — faithful `#[repr(C)]` layouts for the hash-join hash
//! table machinery (`executor/hashjoin.h`) and the `Hash` plan / `HashState`
//! executor nodes (`nodes/plannodes.h`, `nodes/execnodes.h`).
//!
//! These mirror PostgreSQL 18.3 field-for-field so `backend-executor-nodeHash`
//! can manipulate the hash table, tuple chains, dense-allocation chunks, skew
//! buckets and the parallel-hash shared state exactly as the C node does. Only
//! the addresses cross the ABI boundary; pointer-typed fields are opaque raw
//! pointers / DSA pointers to this crate.

#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

use core::ffi::c_void;

use crate::execnodes::dsa_pointer;
use crate::jit::PlanState;
use crate::nodeindexscan::Plan;
use crate::storage::{pg_atomic_uint32, LWLock, SharedFileSet};
use crate::types::{uint32, AttrNumber, Cardinality, Oid, Size};
use crate::{Barrier, FmgrInfo, MemoryContext};

/// `dsa_pointer_atomic` (utils/dsa.h) — an atomically-updated DSA pointer slot.
/// On 64-bit builds this wraps a `pg_atomic_uint64`; we mirror its 8-byte
/// payload so bucket arrays and tuple `next.shared` links lay out correctly.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct dsa_pointer_atomic {
    pub value: u64,
}

/// `union { HashJoinTuple unshared; dsa_pointer shared; }` — the per-tuple and
/// per-chunk "next" link. Both arms are pointer-width (8 bytes).
#[repr(C)]
#[derive(Clone, Copy)]
pub union HashJoinTupleLink {
    pub unshared: *mut HashJoinTupleData,
    pub shared: dsa_pointer,
}

/// `HashJoinTupleData` (executor/hashjoin.h) — header of a tuple stored in the
/// hash table; the `MinimalTuple` data follows at `HJTUPLE_OVERHEAD`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct HashJoinTupleData {
    /// link to next tuple in same bucket
    pub next: HashJoinTupleLink,
    /// tuple's hash code
    pub hashvalue: uint32,
    /* MinimalTuple data follows on a MAXALIGN boundary */
}

/// `HashJoinTuple` typedef (`HashJoinTupleData *`).
pub type HashJoinTuple = *mut HashJoinTupleData;

/// `HashSkewBucket` (executor/hashjoin.h) — one skew-optimization bucket.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct HashSkewBucket {
    /// common hash value
    pub hashvalue: uint32,
    /// linked list of inner-relation tuples
    pub tuples: HashJoinTuple,
}

/// `union { HashMemoryChunk unshared; dsa_pointer shared; }` — chunk "next"
/// link.
#[repr(C)]
#[derive(Clone, Copy)]
pub union HashMemoryChunkLink {
    pub unshared: *mut HashMemoryChunkData,
    pub shared: dsa_pointer,
}

/// `HashMemoryChunkData` (executor/hashjoin.h) — a dense-allocation chunk; the
/// tuple buffer follows at `HASH_CHUNK_HEADER_SIZE`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct HashMemoryChunkData {
    /// number of tuples stored in this chunk
    pub ntuples: c_int,
    /// size of the chunk's tuple buffer
    pub maxlen: Size,
    /// number of buffer bytes already used
    pub used: Size,
    /// pointer to the next chunk (linked list)
    pub next: HashMemoryChunkLink,
}

/// `HashMemoryChunk` typedef (`HashMemoryChunkData *`).
pub type HashMemoryChunk = *mut HashMemoryChunkData;

/// `ParallelHashGrowth` (executor/hashjoin.h).
pub type ParallelHashGrowth = c_int;
/// The current dimensions are sufficient.
pub const PHJ_GROWTH_OK: ParallelHashGrowth = 0;
/// The load factor is too high, so we need to add buckets.
pub const PHJ_GROWTH_NEED_MORE_BUCKETS: ParallelHashGrowth = 1;
/// The memory budget would be exhausted, so we need to repartition.
pub const PHJ_GROWTH_NEED_MORE_BATCHES: ParallelHashGrowth = 2;
/// Repartitioning didn't help last time, so don't try again.
pub const PHJ_GROWTH_DISABLED: ParallelHashGrowth = 3;

/// `ParallelHashJoinBatch` (executor/hashjoin.h) — shared per-batch state in
/// the DSM segment; followed by variable-sized `SharedTuplestore` objects.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ParallelHashJoinBatch {
    /// array of hash table buckets
    pub buckets: dsa_pointer,
    /// synchronization for joining this batch
    pub batch_barrier: Barrier,
    /// chunks of tuples loaded
    pub chunks: dsa_pointer,
    /// size of buckets + chunks in memory
    pub size: Size,
    /// size of buckets + chunks while writing
    pub estimated_size: Size,
    /// number of tuples loaded
    pub ntuples: Size,
    /// number of tuples before repartitioning
    pub old_ntuples: Size,
    pub space_exhausted: bool,
    /// whether to abandon unmatched scan
    pub skip_unmatched: bool,
}

/// `ParallelHashJoinBatchAccessor` (executor/hashjoin.h) — each backend's
/// per-batch state for interacting with a `ParallelHashJoinBatch`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ParallelHashJoinBatchAccessor {
    /// pointer to shared state
    pub shared: *mut ParallelHashJoinBatch,
    /// pre-allocated space for this backend
    pub preallocated: Size,
    /// number of tuples
    pub ntuples: Size,
    /// size of partition in memory
    pub size: Size,
    /// size of partition on disk
    pub estimated_size: Size,
    /// how many tuples before repartitioning?
    pub old_ntuples: Size,
    /// has this backend allocated a chunk?
    pub at_least_one_chunk: bool,
    /// has this process hit end of batch?
    pub outer_eof: bool,
    /// flag to remember that a batch is done
    pub done: bool,
    /// `SharedTuplestoreAccessor *`
    pub inner_tuples: *mut c_void,
    /// `SharedTuplestoreAccessor *`
    pub outer_tuples: *mut c_void,
}

/// `ParallelHashJoinState` (executor/hashjoin.h) — shared state coordinating a
/// Parallel Hash Join, stored in the DSM segment.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ParallelHashJoinState {
    /// array of ParallelHashJoinBatch
    pub batches: dsa_pointer,
    /// previous generation during repartition
    pub old_batches: dsa_pointer,
    /// number of batches now
    pub nbatch: c_int,
    /// previous number of batches
    pub old_nbatch: c_int,
    /// number of buckets
    pub nbuckets: c_int,
    /// control batch/bucket growth
    pub growth: ParallelHashGrowth,
    /// chunk work queue
    pub chunk_work_queue: dsa_pointer,
    pub nparticipants: c_int,
    pub space_allowed: Size,
    /// total number of inner tuples
    pub total_tuples: Size,
    /// lock protecting the above
    pub lock: LWLock,
    /// synchronization for the build phases
    pub build_barrier: Barrier,
    pub grow_batches_barrier: Barrier,
    pub grow_buckets_barrier: Barrier,
    /// counter for load balancing
    pub distributor: pg_atomic_uint32,
    /// space for shared temporary files
    pub fileset: SharedFileSet,
}

/// `union { HashJoinTupleData **unshared; dsa_pointer_atomic *shared; }` — the
/// hashtable bucket array head.
#[repr(C)]
#[derive(Clone, Copy)]
pub union HashJoinBuckets {
    pub unshared: *mut *mut HashJoinTupleData,
    pub shared: *mut dsa_pointer_atomic,
}

/// `HashJoinTableData` (executor/hashjoin.h) — the per-hashjoin hash table.
#[repr(C)]
pub struct HashJoinTableData {
    /// # buckets in the in-memory hash table
    pub nbuckets: c_int,
    /// its log2 (nbuckets must be a power of 2)
    pub log2_nbuckets: c_int,
    /// # buckets when starting the first hash
    pub nbuckets_original: c_int,
    /// optimal # buckets (per batch)
    pub nbuckets_optimal: c_int,
    /// log2(nbuckets_optimal)
    pub log2_nbuckets_optimal: c_int,
    /// buckets[i] is head of list of tuples in i'th in-memory bucket
    pub buckets: HashJoinBuckets,
    /// are we using skew optimization?
    pub skewEnabled: bool,
    /// hashtable of skew buckets
    pub skewBucket: *mut *mut HashSkewBucket,
    /// size of skewBucket array (a power of 2!)
    pub skewBucketLen: c_int,
    /// number of active skew buckets
    pub nSkewBuckets: c_int,
    /// array indexes of active skew buckets
    pub skewBucketNums: *mut c_int,
    /// number of batches
    pub nbatch: c_int,
    /// current batch #; 0 during 1st pass
    pub curbatch: c_int,
    /// nbatch when we started inner scan
    pub nbatch_original: c_int,
    /// nbatch when we started outer scan
    pub nbatch_outstart: c_int,
    /// flag to shut off nbatch increases
    pub growEnabled: bool,
    /// # tuples obtained from inner plan
    pub totalTuples: f64,
    /// # tuples obtained from inner plan by me
    pub partialTuples: f64,
    /// # tuples inserted into skew tuples
    pub skewTuples: f64,
    /// buffered virtual temp file per batch (`BufFile **`)
    pub innerBatchFile: *mut *mut c_void,
    /// buffered virtual temp file per batch (`BufFile **`)
    pub outerBatchFile: *mut *mut c_void,
    /// memory space currently used by tuples
    pub spaceUsed: Size,
    /// upper limit for space used
    pub spaceAllowed: Size,
    /// peak space used
    pub spacePeak: Size,
    /// skew hash table's current space usage
    pub spaceUsedSkew: Size,
    /// upper limit for skew hashtable
    pub spaceAllowedSkew: Size,
    /// context for whole-hash-join storage
    pub hashCxt: MemoryContext,
    /// context for this-batch-only storage
    pub batchCxt: MemoryContext,
    /// context for spilling to temp files
    pub spillCxt: MemoryContext,
    /// one list for the whole batch (dense allocation)
    pub chunks: HashMemoryChunk,
    /// this backend's current chunk
    pub current_chunk: HashMemoryChunk,
    /// DSA area to allocate memory from (`dsa_area *`)
    pub area: *mut c_void,
    pub parallel_state: *mut ParallelHashJoinState,
    pub batches: *mut ParallelHashJoinBatchAccessor,
    pub current_chunk_shared: dsa_pointer,
}

/// `HashJoinTable` typedef (`HashJoinTableData *`).
pub type HashJoinTable = *mut HashJoinTableData;

/// `Hash` (nodes/plannodes.h) — the Hash plan node.
#[repr(C)]
pub struct Hash {
    pub plan: Plan,
    /// hash keys for the hashjoin condition (`List *`)
    pub hashkeys: *mut c_void,
    /// outer join key's table OID, or InvalidOid
    pub skewTable: Oid,
    /// outer join key's column #, or zero
    pub skewColumn: AttrNumber,
    /// is outer join rel an inheritance tree?
    pub skewInherit: bool,
    /// estimate total rows if parallel_aware
    pub rows_total: Cardinality,
}

/// `HashInstrumentation` (nodes/execnodes.h) — per-process hash stats for
/// EXPLAIN.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct HashInstrumentation {
    /// number of buckets at end of execution
    pub nbuckets: c_int,
    /// planned number of buckets
    pub nbuckets_original: c_int,
    /// number of batches at end of execution
    pub nbatch: c_int,
    /// planned number of batches
    pub nbatch_original: c_int,
    /// peak memory usage in bytes
    pub space_peak: Size,
}

/// `SharedHashInfo` (nodes/execnodes.h) — shared-memory hash stats; the
/// `hinstrument[]` flexible array follows `num_workers`.
#[repr(C)]
pub struct SharedHashInfo {
    pub num_workers: c_int,
    /// `HashInstrumentation hinstrument[FLEXIBLE_ARRAY_MEMBER]`
    pub hinstrument: [HashInstrumentation; 0],
}

/// `HashState` (nodes/execnodes.h) — the Hash node's executor state.
#[repr(C)]
pub struct HashState {
    /// its first field is NodeTag
    pub ps: PlanState,
    /// hash table for the hashjoin
    pub hashtable: HashJoinTable,
    /// ExprState to get hash value (`ExprState *`)
    pub hash_expr: *mut c_void,
    /// lookup data for skew hash function
    pub skew_hashfunction: *mut FmgrInfo,
    /// collation to call skew_hashfunction with
    pub skew_collation: Oid,
    /// leader's pointer to the shared-memory stats area
    pub shared_info: *mut SharedHashInfo,
    /// per-process collection area (local or shared)
    pub hinstrument: *mut HashInstrumentation,
    /// parallel hash state
    pub parallel_state: *mut ParallelHashJoinState,
}

use core::ffi::c_int;
