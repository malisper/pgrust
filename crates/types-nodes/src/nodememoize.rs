//! Memoize node vocabulary (nodes/plannodes.h `Memoize` + executor/execnodes.h
//! `MemoizeState` / `MemoizeInstrumentation` / `SharedMemoizeInfo`), trimmed.
//!
//! Unlike the C node (`nodeMemoize.c`), which threads `palloc`'d `MemoizeEntry`
//! / `MemoizeKey` / `MemoizeTuple` records through a `simplehash.h` table and an
//! intrusive `lib/ilist.h` LRU list, this owned port models the cache as Rust
//! collections:
//!
//! - [`MemoizeCache`] holds its entries in a slot vector with an explicit
//!   free-list, an LRU order (front = least recently used), and a hash index
//!   from the cached hash value to candidate slot ids;
//! - each [`CacheEntry`] owns its parameter key tuple and its chain of cached
//!   result tuples directly (`Vec` rather than a `next`-pointer list).
//!
//! The state-machine logic, the memory accounting (`mem_used` / `mem_limit`,
//! peak tracking, eviction) and the cache statistics are preserved faithfully
//! from PostgreSQL 18.3: the per-entry/per-tuple memory charge uses the fixed
//! LP64 `sizeof` values for the C records so the `mem_peak` EXPLAIN reports
//! matches the C node.

extern crate alloc;

use alloc::boxed::Box;
use alloc::collections::{BTreeMap, VecDeque};
use alloc::vec::Vec;

use mcx::{PgBox, PgString, PgVec};
use types_core::primitive::Oid;

use crate::bitmapset::Bitmapset;
use crate::execnodes::ScanStateData;
use crate::nodeindexscan::Plan;
use crate::nodes::NodeTag;
use crate::primnodes::Expr;
use types_tuple::heaptuple::MinimalTupleData;

// `Expr` is not lifetime-generic in this crate.

/// `T_Memoize` (nodes/nodetags.h).
pub const T_Memoize: NodeTag = NodeTag(361);
/// `T_MemoizeState` (nodes/nodetags.h).
pub const T_MemoizeState: NodeTag = NodeTag(425);

/// `sizeof(MemoizeEntry)` on LP64: `MemoizeKey* key`, `MemoizeTuple* tuplehead`,
/// `uint32 hash`, `char status`, `bool complete` -> 8 + 8 + 4 + 1 + 1 padded to
/// 24 bytes.
pub const SIZEOF_MEMOIZE_ENTRY: u64 = 24;
/// `sizeof(MemoizeKey)` on LP64: `MinimalTuple params` (8) + `dlist_node
/// lru_node` (two pointers, 16) -> 24 bytes.
pub const SIZEOF_MEMOIZE_KEY: u64 = 24;
/// `sizeof(MemoizeTuple)` on LP64: `MinimalTuple mintuple` (8) + `MemoizeTuple*
/// next` (8) -> 16 bytes.
pub const SIZEOF_MEMOIZE_TUPLE: u64 = 16;

// ===========================================================================
// Memoize plan node (plannodes.h).
// ===========================================================================

/// `Memoize` plan node (plannodes.h):
///
/// ```c
/// typedef struct Memoize {
///     Plan        plan;
///     int         numKeys;
///     Oid        *hashOperators;   /* array_size(numKeys) */
///     Oid        *collations;      /* array_size(numKeys) */
///     List       *param_exprs;
///     bool        singlerow;
///     bool        binary_mode;
///     uint32      est_entries;
///     Bitmapset  *keyparamids;
/// } Memoize;
/// ```
#[derive(Debug)]
pub struct Memoize<'mcx> {
    /// `Plan plan` — the abstract plan-node base.
    pub plan: Plan<'mcx>,
    /// `int plan_node_id` — `plan.plan_node_id`, used as the DSM key for
    /// per-worker instrumentation.
    pub plan_node_id: i32,
    /// `int numKeys` — size of the two `Oid` arrays below.
    pub numKeys: i32,
    /// `Oid *hashOperators` — hash operators for each key.
    pub hashOperators: PgVec<'mcx, Oid>,
    /// `Oid *collations` — collations for each key.
    pub collations: PgVec<'mcx, Oid>,
    /// `List *param_exprs` — cache keys as exprs containing parameters.
    pub param_exprs: PgVec<'mcx, Expr>,
    /// `bool singlerow` — mark the cache entry complete after the first tuple.
    pub singlerow: bool,
    /// `bool binary_mode` — compare cache keys bit-by-bit vs hash equality ops.
    pub binary_mode: bool,
    /// `uint32 est_entries` — planner estimate of entries fitting in the cache,
    /// 0 if unknown.
    pub est_entries: u32,
    /// `Bitmapset *keyparamids` — paramids from `param_exprs`.
    pub keyparamids: Option<PgBox<'mcx, Bitmapset<'mcx>>>,
}

impl Memoize<'_> {
    /// Deep copy into `mcx` (C: `copyObject` shape). Fallible: copying
    /// allocates.
    pub fn clone_in<'b>(&self, mcx: mcx::Mcx<'b>) -> types_error::PgResult<Memoize<'b>> {
        let mut hash_operators = mcx::vec_with_capacity_in(mcx, self.hashOperators.len())?;
        for op in self.hashOperators.iter() {
            hash_operators.push(*op);
        }
        let mut collations = mcx::vec_with_capacity_in(mcx, self.collations.len())?;
        for c in self.collations.iter() {
            collations.push(*c);
        }
        let mut param_exprs = mcx::vec_with_capacity_in(mcx, self.param_exprs.len())?;
        for e in self.param_exprs.iter() {
            param_exprs.push(e.clone());
        }
        Ok(Memoize {
            plan: self.plan.clone_in(mcx)?,
            plan_node_id: self.plan_node_id,
            numKeys: self.numKeys,
            hashOperators: hash_operators,
            collations,
            param_exprs,
            singlerow: self.singlerow,
            binary_mode: self.binary_mode,
            est_entries: self.est_entries,
            keyparamids: match &self.keyparamids {
                Some(b) => Some(mcx::alloc_in(mcx, b.clone_in(mcx)?)?),
                None => None,
            },
        })
    }
}

// ===========================================================================
// State-machine states (nodeMemoize.c `MEMO_*` defines).
// ===========================================================================

/// States of the `ExecMemoize` state machine.
///
/// ```c
/// #define MEMO_CACHE_LOOKUP            1
/// #define MEMO_CACHE_FETCH_NEXT_TUPLE  2
/// #define MEMO_FILLING_CACHE           3
/// #define MEMO_CACHE_BYPASS_MODE       4
/// #define MEMO_END_OF_SCAN             5
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MemoStatus {
    /// `MEMO_CACHE_LOOKUP` — attempt to perform a cache lookup.
    CacheLookup,
    /// `MEMO_CACHE_FETCH_NEXT_TUPLE` — get another tuple from the cache.
    CacheFetchNextTuple,
    /// `MEMO_FILLING_CACHE` — read the outer node to fill the cache.
    FillingCache,
    /// `MEMO_CACHE_BYPASS_MODE` — bypass: read the subplan, cache nothing.
    CacheBypassMode,
    /// `MEMO_END_OF_SCAN` — ready for rescan.
    EndOfScan,
}

/// `MemoizeInstrumentation` (execnodes.h) — per-node execution statistics.
///
/// ```c
/// typedef struct MemoizeInstrumentation {
///     uint64  cache_hits;
///     uint64  cache_misses;
///     uint64  cache_evictions;
///     uint64  cache_overflows;
///     uint64  mem_peak;
/// } MemoizeInstrumentation;
/// ```
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MemoizeInstrumentation {
    /// `uint64 cache_hits` — rescans where the parameter values were cached.
    pub cache_hits: u64,
    /// `uint64 cache_misses` — rescans where the parameter values were not cached.
    pub cache_misses: u64,
    /// `uint64 cache_evictions` — cache entries removed to free memory.
    pub cache_evictions: u64,
    /// `uint64 cache_overflows` — times we bypassed the cache because we could
    /// not free enough space.
    pub cache_overflows: u64,
    /// `uint64 mem_peak` — peak memory usage in bytes.
    pub mem_peak: u64,
}

/// `SharedMemoizeInfo` (execnodes.h) — shared-memory container for per-worker
/// memoize statistics; the C `sinstrument` flexible-array member is a counted
/// vector here.
///
/// ```c
/// typedef struct SharedMemoizeInfo {
///     int num_workers;
///     MemoizeInstrumentation sinstrument[FLEXIBLE_ARRAY_MEMBER];
/// } SharedMemoizeInfo;
/// ```
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SharedMemoizeInfo {
    /// `int num_workers`
    pub num_workers: i32,
    /// `MemoizeInstrumentation sinstrument[FLEXIBLE_ARRAY_MEMBER]`
    pub sinstrument: Vec<MemoizeInstrumentation>,
}

// ===========================================================================
// Owned cache representation (the idiomatic replacement for the simplehash
// table, the intrusive LRU list, and the palloc'd MemoizeEntry/Key/Tuple).
// ===========================================================================

/// One cached result tuple (`MemoizeTuple` in C, sans the `next` pointer — the
/// chain is held by the owning [`CacheEntry`] as a `Vec`).
///
/// ```c
/// typedef struct MemoizeTuple {
///     MinimalTuple mintuple;
///     struct MemoizeTuple *next;
/// } MemoizeTuple;
/// ```
#[derive(Debug)]
pub struct CachedTuple<'mcx> {
    /// `MinimalTuple mintuple` — the cached tuple's owned payload (palloc'd into
    /// the cache's `tableContext` in C).
    pub mintuple: MinimalTupleData<'mcx>,
}

/// One cache entry — the merge of the C `MemoizeEntry` (the simplehash element)
/// and its `MemoizeKey` (the hash key plus the LRU list link). The LRU link and
/// the bucket placement are handled by [`MemoizeCache`] instead.
///
/// ```c
/// typedef struct MemoizeKey {
///     MinimalTuple params;
///     dlist_node  lru_node;
/// } MemoizeKey;
/// typedef struct MemoizeEntry {
///     MemoizeKey *key;
///     MemoizeTuple *tuplehead;
///     uint32      hash;
///     char        status;
///     bool        complete;
/// } MemoizeEntry;
/// ```
#[derive(Debug)]
pub struct CacheEntry<'mcx> {
    /// `key->params` — the cached parameter values that identify this entry.
    pub params: MinimalTupleData<'mcx>,
    /// `tuplehead` chain — the cached result tuples for these parameters, in
    /// scan order.
    pub tuples: Vec<CachedTuple<'mcx>>,
    /// `uint32 hash` — cached hash value (`SH_STORE_HASH`).
    pub hash: u32,
    /// `bool complete` — did we read the outer plan to completion for this entry?
    pub complete: bool,
}

/// The Memoize cache: the owned replacement for the `simplehash` table plus the
/// intrusive LRU list. Entries live in [`slots`](Self::slots); a `None` slot is
/// free. [`lru`](Self::lru) orders live slot ids least-recently-used first (the
/// front bubbles to the top, as the C `dlist` LRU does), and
/// [`index`](Self::index) maps a cached hash value to candidate slot ids.
#[derive(Debug, Default)]
pub struct MemoizeCache<'mcx> {
    /// Entry storage; `slots[id]` is `Some` when slot `id` is in use.
    pub slots: Vec<Option<CacheEntry<'mcx>>>,
    /// Reusable free slot ids.
    pub free_slots: Vec<usize>,
    /// LRU order of live slot ids; front = least recently used.
    pub lru: VecDeque<usize>,
    /// Hash value -> candidate live slot ids (collision chain).
    pub index: BTreeMap<u32, Vec<usize>>,
    /// Number of live entries (`tb->members`).
    pub members: u32,
}

impl<'mcx> MemoizeCache<'mcx> {
    /// Create an empty cache.
    pub fn new() -> Self {
        Self::default()
    }
}

// ===========================================================================
// Node state (idiomatic `MemoizeState`).
// ===========================================================================

/// `MemoizeState` (execnodes.h), owned form.
///
/// The leading [`ScanStateData`] head carries the embedded `ScanState` /
/// `PlanState` heads. The cache (`hashtable`, `lru_list`, `last_tuple`,
/// `entry`) is modeled by the owned [`MemoizeCache`] plus the cursor fields.
///
/// ```c
/// typedef struct MemoizeState {
///     ScanState   ss;
///     int         mstatus;
///     int         nkeys;
///     struct memoize_hash *hashtable;
///     TupleDesc   hashkeydesc;
///     TupleTableSlot *tableslot;
///     TupleTableSlot *probeslot;
///     ExprState  *cache_eq_expr;
///     ExprState **param_exprs;
///     FmgrInfo   *hashfunctions;
///     Oid        *collations;
///     uint64      mem_used;
///     uint64      mem_limit;
///     MemoryContext tableContext;
///     dlist_head  lru_list;
///     struct MemoizeTuple *last_tuple;
///     struct MemoizeEntry *entry;
///     bool        singlerow;
///     bool        binary_mode;
///     MemoizeInstrumentation stats;
///     SharedMemoizeInfo *shared_info;
///     Bitmapset  *keyparamids;
/// } MemoizeState;
/// ```
#[derive(Debug)]
pub struct MemoizeScanState<'mcx> {
    /// `ScanState ss` — its first field is `NodeTag`.
    pub ss: ScanStateData<'mcx>,
    /// `int mstatus` — the `ExecMemoize` state-machine state.
    pub mstatus: MemoStatus,
    /// `int nkeys` — number of cache keys.
    pub nkeys: i32,
    /// `struct memoize_hash *hashtable` — the cache. `None` until the first call
    /// (the C node delays building the hash table until executor run) and after
    /// `cache_purge_all`.
    pub hashtable: Option<MemoizeCache<'mcx>>,
    /// `uint32 est_entries` — planner estimate of entries fitting in the cache;
    /// used to size the table on the first call (`build_hash_table`).
    pub est_entries: u32,
    /// `Oid *collations` — collation per key (`nkeys` long); copied from the
    /// plan data.
    pub collations: PgVec<'mcx, Oid>,
    /// `uint64 mem_used` — bytes of memory used by the cache.
    pub mem_used: u64,
    /// `uint64 mem_limit` — memory limit in bytes for the cache.
    pub mem_limit: u64,
    /// Working cursor — the cache slot id of the entry currently being read or
    /// filled (`mstate->entry`). `None` == NULL.
    pub entry: Option<usize>,
    /// Working cursor — index into the current entry's `tuples` of the last
    /// tuple returned or stored (`mstate->last_tuple`). `None` == NULL.
    pub last_tuple: Option<usize>,
    /// `bool singlerow` — mark the cache entry complete after the first tuple.
    pub singlerow: bool,
    /// `bool binary_mode` — compare cache keys bit-by-bit vs hash equality ops.
    pub binary_mode: bool,
    /// `MemoizeInstrumentation stats` — execution statistics.
    pub stats: MemoizeInstrumentation,
    /// `SharedMemoizeInfo *shared_info` — statistics for parallel workers.
    pub shared_info: Option<Box<SharedMemoizeInfo>>,
    /// `Bitmapset *keyparamids` — `Param->paramids` of the `param_exprs`.
    pub keyparamids: Option<PgBox<'mcx, Bitmapset<'mcx>>>,
    /// `int plan_node_id` — `node->ss.ps.plan->plan_node_id`, the DSM key.
    pub plan_node_id: i32,
    /// Diagnostic label, mirrors the C `"MemoizeHashTable"` context name.
    pub table_context_name: Option<PgString<'mcx>>,
}

impl<'mcx> MemoizeScanState<'mcx> {
    /// `EMPTY_ENTRY_MEMORY_BYTES(e)` — `sizeof(MemoizeEntry) +
    /// sizeof(MemoizeKey) + e->key->params->t_len`. The struct sizes are the
    /// fixed PostgreSQL 18.3 (LP64) `sizeof` values so the memory accounting
    /// (and `mem_peak`) matches the C node.
    pub fn empty_entry_memory_bytes(params_len: u32) -> u64 {
        SIZEOF_MEMOIZE_ENTRY + SIZEOF_MEMOIZE_KEY + params_len as u64
    }

    /// `CACHE_TUPLE_BYTES(t)` — `sizeof(MemoizeTuple) + t->mintuple->t_len`.
    pub fn cache_tuple_bytes(mintuple_len: u32) -> u64 {
        SIZEOF_MEMOIZE_TUPLE + mintuple_len as u64
    }
}

/// `ExecEstimateCacheEntryOverheadBytes(ntuples)` (nodeMemoize.c) — planner
/// helper estimating the memory required to store a single cache entry. Kept in
/// the vocabulary crate so the planner consumes it without depending on the
/// executor node crate.
pub fn exec_estimate_cache_entry_overhead_bytes(ntuples: f64) -> f64 {
    SIZEOF_MEMOIZE_ENTRY as f64 + SIZEOF_MEMOIZE_KEY as f64 + SIZEOF_MEMOIZE_TUPLE as f64 * ntuples
}
