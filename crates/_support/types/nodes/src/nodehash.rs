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

use ::condvar::Barrier;
use types_core::{uint32, Oid, Size};
use ::types_core::FmgrInfo;
use execparallel::{DsaAreaHandle, DsaPointer, FileSetHandle, SerializeCursor};
use ::types_storage::storage::{pg_atomic_uint32, pg_atomic_uint64, LWLock};
use ::types_storage::fileset::SharedFileSet;
use ::types_storage::file::{File, PGAlignedBlock};
use ::types_storage::lock::ResourceOwnerHandle;
use types_tuple::heaptuple::FormedMinimalTuple;

use crate::execexpr::ExprState;
use crate::execnodes::{Opaque, PlanStateData, SlotId};
use crate::jointype::JoinStateData;

// ===========================================================================
//          Inherited-opacity sibling-subsystem handles (consolidated)
// ===========================================================================

/// `struct BufFile` (storage/file/buffile.c) — a buffered virtual temp file
/// consisting of one or more `MAX_PHYSICAL_FILESIZE`-byte physical segments,
/// each a VFD [`File`] managed by fd.c. Owned by `storage/file/buffile.c`,
/// which holds the buffered-I/O behaviour; the hash join only stores the value
/// (in a `PgBox`) and passes it back to the buffile seams.
///
/// This is a backend-local handle, NOT a shmem-resident ABI struct, so the
/// fields carry no layout invariant. The C `File *files` palloc'd array and the
/// `pstrdup`'d `const char *name` become owned `Vec`/`String`: the canonical
/// type is lifetime-free (every consumer stores it as `PgBox<'mcx, BufFile>`
/// and the buffile seams are typed `&mut BufFile`), so it cannot carry the
/// `'mcx` an in-arena `PgVec`/`PgString` would require.
#[derive(Debug)]
pub struct BufFile {
    /// `int numFiles` — number of physical files in the set. Kept equal to
    /// `files.len()` at every push/pop/append/truncate (C's `numFiles` is the
    /// segment count).
    pub numFiles: i32,
    /// `File *files` — the physical segments. All but the last have length
    /// exactly `MAX_PHYSICAL_FILESIZE`.
    pub files: alloc::vec::Vec<File>,
    /// `bool isInterXact` — keep open over transactions?
    pub isInterXact: bool,
    /// `bool dirty` — does the buffer need to be written?
    pub dirty: bool,
    /// `bool readOnly` — has the file been set to read only?
    pub readOnly: bool,
    /// `FileSet *fileset` — the fileset backing the segment files, or `None`
    /// for a standalone temp file. Borrowed (the body is owned by fileset.c).
    pub fileset: Option<FileSetHandle>,
    /// `const char *name` — name of a fileset-based BufFile (`None` otherwise).
    pub name: Option<alloc::string::String>,
    /// `ResourceOwner resowner` — the resource owner for the underlying temp
    /// files, captured at creation (`CurrentResourceOwner`).
    pub resowner: Option<ResourceOwnerHandle>,
    /// `int curFile` — file index (0..n) part of the current position.
    pub curFile: i32,
    /// `off_t curOffset` — offset part of the current position (start of buffer
    /// within the logical file).
    pub curOffset: i64,
    /// `int pos` — next read/write position in the buffer.
    pub pos: i32,
    /// `int nbytes` — total number of valid bytes in the buffer.
    pub nbytes: i32,
    /// `PGAlignedBlock buffer` — the `BLCKSZ` I/O buffer.
    pub buffer: PGAlignedBlock,
}

/// `SharedTuplestoreAccessor` (utils/sharedtuplestore.h) — a backend's handle
/// to a shared tuplestore partition. Owned by the (unported) sharedtuplestore
/// subsystem; inherited opacity here.
#[derive(Debug, Default)]
pub struct SharedTuplestoreAccessor(pub Opaque);

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

/// Index of a [`HashJoinTupleData`] in [`HashJoinTableData::tuples`] (the dense
/// main-batch arena) — or, on the parallel probe path, a raw backend-local DSA
/// address of an on-segment `HashJoinTupleData`. The owned model stores each
/// tuple once and links chains by index, since the C `HashJoinTuple` pointers
/// point into the dense-allocation chunk buffers and cannot be modeled as a
/// self-referential graph of `Box`es.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct HashTupleIdx(pub usize);

/// Index of a [`HashJoinTupleData`] in [`HashJoinTableData::skew_tuples`] — the
/// SEPARATE skew-tuple arena. C allocates skew tuples in their own
/// `MemoryContextAlloc(batchCxt)` storage, distinct from the dense-allocation
/// chunks the main batch lives in; `ExecHashIncreaseNumBatches` renumbers ONLY
/// the dense storage, so skew chains must index a separate arena to survive a
/// rebatch's `mem::replace`. Keeping skew indices in their own newtype makes
/// that separation type-enforced.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct SkewTupleIdx(pub usize);

/// A tagged locator distinguishing a tuple in the dense main-batch arena from
/// one in the separate skew arena. The serial scan cursor (`hj_CurTuple`) and
/// the scan loops walk both the dense bucket chains and the skew bucket chains,
/// so the "current tuple" cursor must carry which arena it indexes. (In the
/// parallel probe path the locator is always [`HashTupleRef::Dense`] carrying a
/// raw DSA address — parallel hash joins never use the skew optimization.)
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum HashTupleRef {
    /// A tuple in [`HashJoinTableData::tuples`] (or a raw DSA address in the
    /// parallel path).
    Dense(HashTupleIdx),
    /// A tuple in [`HashJoinTableData::skew_tuples`].
    Skew(SkewTupleIdx),
}

/// Index of a [`HashMemoryChunkData`] in [`HashJoinTableData::chunk_arena`].
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct HashChunkIdx(pub usize);

/// `union { HashJoinTuple unshared; dsa_pointer shared; } next` — the per-tuple
/// bucket-chain link.
#[derive(Clone, Copy, Debug)]
pub enum HashJoinTupleLink {
    /// `HashJoinTuple unshared` — next tuple in same DENSE main-batch bucket
    /// (serial mode); index into [`HashJoinTableData::tuples`]. `None` is the C
    /// `NULL`.
    Unshared(Option<HashTupleIdx>),
    /// `HashJoinTuple unshared` for a SKEW-arena tuple — next tuple in the same
    /// skew bucket chain; index into [`HashJoinTableData::skew_tuples`]. `None`
    /// is the C `NULL`. Distinct from [`Self::Unshared`] only by which arena it
    /// indexes (C uses a single pointer; the owned two-arena model needs the
    /// tag).
    SkewUnshared(Option<SkewTupleIdx>),
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
    /// the inline `MinimalTuple` payload (header + user-data area).
    pub mintuple: FormedMinimalTuple<'mcx>,
}

/// `HashSkewBucket` (hashjoin.h).
pub struct HashSkewBucket {
    /// `uint32 hashvalue` — common hash value.
    pub hashvalue: uint32,
    /// `HashJoinTuple tuples` — linked list of inner-relation tuples (head
    /// index into the SEPARATE [`HashJoinTableData::skew_tuples`] arena);
    /// `None` = empty. Skew tuples live apart from the dense main-batch arena
    /// so a rebatch's `mem::replace`/renumber on `tuples` cannot invalidate
    /// these chains. A skew tuple's own `next` link
    /// ([`HashJoinTupleData::next`] = `Unshared(Some(SkewTupleIdx(i)))`) also
    /// indexes `skew_tuples`.
    pub tuples: Option<SkewTupleIdx>,
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
pub struct ParallelHashJoinBatchAccessor<'mcx> {
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
    pub inner_tuples: Option<PgBox<'mcx, SharedTuplestoreAccessor>>,
    /// `SharedTuplestoreAccessor *outer_tuples`.
    pub outer_tuples: Option<PgBox<'mcx, SharedTuplestoreAccessor>>,
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

// SAFETY (audited per the `SharedDsmObject` contract):
//   1. `ParallelHashJoinState` is `#[repr(C)]` and matches `hashjoin.h`'s
//      `ParallelHashJoinState` field-for-field (the order above is the C order).
//   2. Every field the C mutates concurrently after the build barrier releases
//      is interior-mutable: `lock` is the in-segment `LWLock`, `build_barrier` /
//      `grow_batches_barrier` / `grow_buckets_barrier` are real `Barrier`s
//      (in-segment `Spinlock` + `ConditionVariable`), `distributor` is a
//      `pg_atomic_uint32`, and `fileset` carries its own in-segment `Spinlock`.
//      The remaining scalars (`batches`/`old_batches`/`nbatch`/`old_nbatch`/
//      `nbuckets`/`growth`/`chunk_work_queue`/`nparticipants`/`space_allowed`/
//      `total_tuples`) are mutated only under `lock` or by the single elected
//      participant inside a build-barrier phase, never racily — exactly as in C,
//      where they are plain ints/dsa_pointers behind the same LWLock + barrier
//      discipline (the parallel-hash protocol in nodeHash drives the locking).
//   3. The leader's placement initializer (`ExecHashJoinInitializeDSM`) writes
//      every field before any worker attaches.
//   4. A shared `&ParallelHashJoinState` aliasing another process's shared
//      `&ParallelHashJoinState` is sound: all writes go through the LWLock /
//      Barrier / atomic / fileset-spinlock interior-mutable accessors.
unsafe impl types_parallel::SharedDsmObject for ParallelHashJoinState {}

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
    pub innerBatchFile: PgVec<'mcx, Option<PgBox<'mcx, BufFile>>>,
    /// `BufFile **outerBatchFile` — temp file per batch.
    pub outerBatchFile: PgVec<'mcx, Option<PgBox<'mcx, BufFile>>>,
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
    pub batches: PgVec<'mcx, ParallelHashJoinBatchAccessor<'mcx>>,
    /// `dsa_pointer current_chunk_shared`.
    pub current_chunk_shared: DsaPointer,
    /// `MemoryContext spillCxt` — child context of `hashCxt` used by
    /// `ExecHashJoinSaveTuple` for the batch temp-file buffers. Carried as the
    /// per-query allocator handle the spill files are charged to.
    pub spillCxt: Mcx<'mcx>,
    /// OWNED-MODEL arena: every in-memory current-batch [`HashJoinTupleData`].
    /// C carves these out of the dense-allocation chunk byte buffers; the owned
    /// model stores them here once and indexes them with [`HashTupleIdx`].
    pub tuples: PgVec<'mcx, HashJoinTupleData<'mcx>>,
    /// OWNED-MODEL arena: every in-memory SKEW-bucket [`HashJoinTupleData`],
    /// stored SEPARATELY from the dense `tuples` arena. C allocates skew tuples
    /// in their own `batchCxt` storage, untouched by the dense rebatch; the
    /// owned model mirrors that with a distinct Vec indexed by [`SkewTupleIdx`]
    /// so `ExecHashIncreaseNumBatches`'s `mem::replace` on `tuples` never
    /// renumbers (and thus never corrupts) the live skew chains.
    pub skew_tuples: PgVec<'mcx, HashJoinTupleData<'mcx>>,
    /// OWNED-MODEL arena: the dense-allocation chunk headers, linked by
    /// [`HashChunkIdx`].
    pub chunk_arena: PgVec<'mcx, HashMemoryChunkData>,
}

impl<'mcx> core::fmt::Debug for HashJoinTableData<'mcx> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("HashJoinTableData")
            .field("nbuckets", &self.nbuckets)
            .field("nbatch", &self.nbatch)
            .field("curbatch", &self.curbatch)
            .field("spaceUsed", &self.spaceUsed)
            .finish_non_exhaustive()
    }
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
    pub values: PgVec<'mcx, types_tuple::heaptuple::Datum<'mcx>>,
    /// `int nnumbers` — number of `numbers`.
    pub nnumbers: i32,
    /// `float4 *numbers` — the MCV frequencies.
    pub numbers: PgVec<'mcx, f32>,
}

// ===========================================================================
//                   Plan / executor node structs
// ===========================================================================

// `Hash` plan node (`nodes/plannodes.h`) is the single canonical
// `crate::nodehashjoin::Hash` (re-exported below); the `Node` enum and the
// nodeHash/nodeHashjoin bodies all reference that one type.
pub use crate::nodehashjoin::Hash;

/// `HashInstrumentation` (`nodes/execnodes.h`) — per-process hash-build stats.
///
/// `#[repr(C)]` because it is the element type of the `SharedHashInfo`
/// flexible-array member that lives DIRECTLY in the parallel-query DSM segment
/// (`ExecHashInitializeDSM` `shm_toc_allocate`s the chunk and the workers
/// `shm_toc_lookup` + index into it). Placed/attached through the typed
/// shared-DSM-object flex primitive (`shared_dsm_object::place_flex` /
/// `attach_flex`).
#[repr(C)]
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

// SAFETY (audited per the `SharedDsmObject` contract):
//   1. `HashInstrumentation` is `#[repr(C)]` and matches `execnodes.h`
//      field-for-field (five scalars in C order: nbuckets, nbuckets_original,
//      nbatch, nbatch_original, space_peak).
//   2. There is NO concurrent mutation of any single element across processes:
//      each parallel worker writes ONLY its own `hinstrument[ParallelWorkerNumber]`
//      slot (set up in `ExecHashInitializeWorker`), and the leader reads the
//      whole array only in `ExecHashRetrieveInstrumentation`, which the C runs
//      after the workers have detached from the DSM segment. The element bytes
//      are therefore never aliased-and-mutated concurrently, so plain (non
//      interior-mutable) POD scalars satisfy clause 2 by partition.
//   3. The leader's placement initializer (`ExecHashInitializeDSM`) zero-fills
//      every element before any worker attaches (`place_flex` writes
//      `HashInstrumentation::default()` into each slot).
//   4. A shared `&HashInstrumentation` aliasing another process's mapping of
//      the SAME element is never created concurrently with a write (clause 2),
//      so such a shared borrow is sound.
unsafe impl types_parallel::SharedDsmObject for HashInstrumentation {}

/// `offsetof(SharedHashInfo, num_workers)`-bearing header of `SharedHashInfo`
/// (`nodes/execnodes.h`): `{ int num_workers; HashInstrumentation hinstrument[]; }`.
/// This is the `H` of the `place_flex`/`attach_flex` flexible-array placement;
/// the `hinstrument[]` tail is the `E = HashInstrumentation` slice.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct SharedHashInfoHeader {
    /// `int num_workers`.
    pub num_workers: i32,
}

// SAFETY: `#[repr(C)]` POD header written once by the leader
// (`ExecHashInitializeDSM`) before any worker attaches, read-only thereafter
// (workers only read `num_workers`); no concurrent mutation. Matches the C
// `SharedHashInfo` header field-for-field.
unsafe impl types_parallel::SharedDsmObject for SharedHashInfoHeader {}

/// `SharedHashInfo *` (`nodes/execnodes.h`) — `node->shared_info`. In C this is
/// a single `SharedHashInfo *` pointer that is FIRST the DSM-resident shared
/// area (set in `ExecHashInitializeDSM` / inherited by workers) and is LATER
/// REPLACED, in `ExecHashRetrieveInstrumentation`, by a backend-local `palloc`'d
/// copy. The two states have different ownership (cross-process DSM view vs.
/// owned backend-local array), so they are modelled as the two arms here.
#[derive(Debug)]
pub enum SharedHashInfo<'mcx> {
    /// The DSM-resident shared area: a cursor to the `shm_toc`-allocated chunk
    /// (`{ SharedHashInfoHeader; HashInstrumentation[num_workers] }`) plus the
    /// worker count needed to recover the flex length. Mirrors the leader's
    /// `node->shared_info = shm_toc_allocate(...)`.
    Dsm {
        /// Real in-segment chunk address (the `shm_toc_allocate`/`shm_toc_lookup`
        /// return value).
        chunk: SerializeCursor,
        /// The DSM segment the chunk lives in (the leader's `pcxt->seg`), so the
        /// retrieve path can `attach_flex` the array before detach.
        seg: ::execparallel::DsmSegmentHandle,
        /// `shared_info->num_workers`.
        num_workers: i32,
    },
    /// The backend-local copy `ExecHashRetrieveInstrumentation` makes before the
    /// DSM segment is detached (`node->shared_info = palloc(size); memcpy(...)`).
    Local {
        /// `shared_info->num_workers`.
        num_workers: i32,
        /// `HashInstrumentation hinstrument[]` copied out of DSM.
        hinstrument: PgVec<'mcx, HashInstrumentation>,
    },
}

/// `HashInstrumentation *` (`nodes/execnodes.h`) — `node->hinstrument`, this
/// process's stats-collection slot. Like the C pointer it is either a
/// backend-local `palloc0_object(HashInstrumentation)` (serial / leader without
/// workers) or an alias INTO the leader's DSM `SharedHashInfo` array at this
/// worker's index (`&shared_info->hinstrument[ParallelWorkerNumber]`).
#[derive(Debug)]
pub enum HashInstrumentSlot<'mcx> {
    /// Backend-local `palloc0`'d `HashInstrumentation`.
    Local(PgBox<'mcx, HashInstrumentation>),
    /// Alias into the DSM `SharedHashInfo` flex array: the header chunk cursor
    /// plus this worker's element index (`ParallelWorkerNumber`).
    Dsm {
        /// Cursor to the `SharedHashInfo` chunk header (the array starts at
        /// `flex_tail_offset::<SharedHashInfoHeader, HashInstrumentation>()`).
        chunk: SerializeCursor,
        /// The DSM segment the chunk lives in (the worker's `pwcxt->seg`), so
        /// the slot can be mutated via `shared_dsm_object::with_mut`.
        seg: ::execparallel::DsmSegmentHandle,
        /// `ParallelWorkerNumber` — this worker's slot index in the array.
        worker_index: i32,
    },
}

/// `HashState` (`nodes/execnodes.h`) — the Hash executor node.
#[derive(Debug)]
pub struct HashState<'mcx> {
    /// `PlanState ps` — its first field is `NodeTag`.
    pub ps: PlanStateData<'mcx>,
    /// `HashJoinTable hashtable` — hash table for the hashjoin (`None` = NULL).
    pub hashtable: Option<PgBox<'mcx, HashJoinTableData<'mcx>>>,
    /// `ExprState *hash_expr` — ExprState to get hash value.
    pub hash_expr: Option<PgBox<'mcx, ExprState<'mcx>>>,
    /// `FmgrInfo *skew_hashfunction` — lookup data for skew hash function.
    pub skew_hashfunction: Option<PgBox<'mcx, FmgrInfo>>,
    /// `Oid skew_collation` — collation to call skew_hashfunction with.
    pub skew_collation: Oid,
    /// `SharedHashInfo *shared_info` — leader's pointer to the shared stats
    /// area; `None` in workers / non-parallel joins.
    pub shared_info: Option<SharedHashInfo<'mcx>>,
    /// `HashInstrumentation *hinstrument` — this process's stats collection
    /// area (local or shared); `None` when not collecting.
    pub hinstrument: Option<HashInstrumentSlot<'mcx>>,
    /// `struct ParallelHashJoinState *parallel_state` — `None` in serial mode.
    pub parallel_state: Option<DsaPointer>,
}

impl<'mcx> HashState<'mcx> {
    /// The `HashInstrumentation` collection that `show_hash_info`
    /// (commands/explain.c:3375) merges before emitting the
    /// `Buckets: ... Batches: ... Memory Usage: ...` line. Returns `None` when
    /// the node never collected stats (`node->hinstrument == NULL` and
    /// `node->shared_info == NULL`).
    ///
    /// Mirrors `show_hash_info`'s merge exactly: start from this process's
    /// `hinstrument` (`memcpy(&hinstrument, hashstate->hinstrument, ...)`), then
    /// fold each worker's slot in `shared_info->hinstrument[i]` via element-wise
    /// `Max` (the parallel-aware case). At EXPLAIN time, in the leader, both the
    /// local `hinstrument` and the merged `shared_info` are the backend-local
    /// (`Local`) arms (`ExecShutdownHash` palloc0's the local slot and
    /// `ExecHashRetrieveInstrumentation` snapshots the DSM array into a
    /// backend-local copy before detach), so this read needs no DSM mapping.
    pub fn collect_hash_instrumentation(&self) -> Option<HashInstrumentation> {
        if self.hinstrument.is_none() && self.shared_info.is_none() {
            return None;
        }

        // HashInstrumentation hinstrument = {0};
        // if (hashstate->hinstrument)
        //     memcpy(&hinstrument, hashstate->hinstrument, sizeof(...));
        let mut hinstrument = match &self.hinstrument {
            Some(HashInstrumentSlot::Local(b)) => **b,
            // The DSM arm only exists inside a parallel worker; the leader
            // (the EXPLAIN reader) always holds the `Local` arm here.
            _ => HashInstrumentation::default(),
        };

        // if (hashstate->shared_info)
        //     for (i = 0; i < num_workers; ++i)
        //         hinstrument.X = Max(hinstrument.X, worker_hi->X);
        if let Some(SharedHashInfo::Local { hinstrument: workers, .. }) = &self.shared_info {
            for w in workers.iter() {
                hinstrument.nbuckets = hinstrument.nbuckets.max(w.nbuckets);
                hinstrument.nbuckets_original =
                    hinstrument.nbuckets_original.max(w.nbuckets_original);
                hinstrument.nbatch = hinstrument.nbatch.max(w.nbatch);
                hinstrument.nbatch_original = hinstrument.nbatch_original.max(w.nbatch_original);
                hinstrument.space_peak = hinstrument.space_peak.max(w.space_peak);
            }
        }

        Some(hinstrument)
    }
}

/// `HashJoinState` (`nodes/execnodes.h`) — the HashJoin executor node. Defined
/// here (rather than in a future `nodeHashjoin` types module) because
/// `nodeHash.c`'s probe routines (`ExecScanHashBucket`, …) operate on it.
#[derive(Debug)]
pub struct HashJoinState<'mcx> {
    /// `JoinState js` — its first field is `NodeTag`.
    pub js: JoinStateData<'mcx>,
    /// `ExprState *hashclauses`.
    pub hashclauses: Option<PgBox<'mcx, ExprState<'mcx>>>,
    /// `ExprState *hj_OuterHash`.
    pub hj_OuterHash: Option<PgBox<'mcx, ExprState<'mcx>>>,
    /// `HashJoinTable hj_HashTable`.
    pub hj_HashTable: Option<PgBox<'mcx, HashJoinTableData<'mcx>>>,
    /// `uint32 hj_CurHashValue`.
    pub hj_CurHashValue: uint32,
    /// `int hj_CurBucketNo`.
    pub hj_CurBucketNo: i32,
    /// `int hj_CurSkewBucketNo`.
    pub hj_CurSkewBucketNo: i32,
    /// `HashJoinTuple hj_CurTuple` — current tuple in the scan (a tagged
    /// locator into either the dense `tuples` arena or the separate
    /// `skew_tuples` arena); `None` = NULL.
    pub hj_CurTuple: Option<HashTupleRef>,
    /// `TupleTableSlot *hj_OuterTupleSlot` — id into `es_tupleTable`.
    pub hj_OuterTupleSlot: Option<SlotId>,
    /// `TupleTableSlot *hj_HashTupleSlot` — id into `es_tupleTable`.
    pub hj_HashTupleSlot: Option<SlotId>,
    /// `TupleTableSlot *hj_NullOuterTupleSlot` — id into `es_tupleTable`.
    pub hj_NullOuterTupleSlot: Option<SlotId>,
    /// `TupleTableSlot *hj_NullInnerTupleSlot` — id into `es_tupleTable`.
    pub hj_NullInnerTupleSlot: Option<SlotId>,
    /// `TupleTableSlot *hj_FirstOuterTupleSlot` — id into `es_tupleTable`.
    pub hj_FirstOuterTupleSlot: Option<SlotId>,
    /// `int hj_JoinState`.
    pub hj_JoinState: i32,
    /// `bool hj_MatchedOuter`.
    pub hj_MatchedOuter: bool,
    /// `bool hj_OuterNotEmpty`.
    pub hj_OuterNotEmpty: bool,
}

impl<'mcx> Default for HashJoinState<'mcx> {
    fn default() -> Self {
        HashJoinState {
            js: JoinStateData::default(),
            hashclauses: None,
            hj_OuterHash: None,
            hj_HashTable: None,
            hj_CurHashValue: 0,
            hj_CurBucketNo: 0,
            hj_CurSkewBucketNo: -1,
            hj_CurTuple: None,
            hj_OuterTupleSlot: None,
            hj_HashTupleSlot: None,
            hj_NullOuterTupleSlot: None,
            hj_NullInnerTupleSlot: None,
            hj_FirstOuterTupleSlot: None,
            hj_JoinState: 0,
            hj_MatchedOuter: false,
            hj_OuterNotEmpty: false,
        }
    }
}

/// Silence unused-import lints in the scaffold (the bodies consume `Mcx`).
#[allow(dead_code)]
fn _uses_mcx(_m: Mcx<'_>) {}
