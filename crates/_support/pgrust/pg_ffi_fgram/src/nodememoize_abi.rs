//! `#[repr(C)]` ABI for `nodeMemoize.c` (the Memoize executor node).
//!
//! The Memoize node is ported in-crate (`backend-executor-nodeMemoize`), so its
//! state node is a complete, address-stable `#[repr(C)]` struct laid out exactly
//! like the C `MemoizeState` (execnodes.h). The `Memoize` plan node it navigates
//! is spelled out here too.
//!
//! The cache-internal structs (`memoize_hash`, `MemoizeEntry`, `MemoizeKey`,
//! `MemoizeTuple`) never cross the crate boundary — the node crate owns them and
//! navigates `MemoizeStateData.hashtable` / `.last_tuple` / `.entry` as opaque
//! `*mut c_void` here, casting to its in-crate types. The embedded `ScanState`
//! head reuses the shared [`crate::ScanStateData`] layout defined in `execnodes`.

use core::ffi::c_void;

use crate::execnodes::{PlanNode, ScanStateData};
use crate::guc::dlist_head;
use crate::heaptuple::TupleDesc;
use crate::memory::MemoryContext;
use crate::{Bitmapset, ExprState, FmgrInfo, List, Oid, TupleTableSlot};

/// `MemoizeInstrumentation` (execnodes.h) — per-node execution statistics.
/// Plain POD (only `uint64` counters), so it can be `memcpy`'d into and out of
/// shared memory during parallel scans.
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
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MemoizeInstrumentation {
    /// `uint64 cache_hits` — rescans where the scan parameter values were cached.
    pub cache_hits: u64,
    /// `uint64 cache_misses` — rescans where the parameter values were not cached.
    pub cache_misses: u64,
    /// `uint64 cache_evictions` — cache entries removed to free memory.
    pub cache_evictions: u64,
    /// `uint64 cache_overflows` — times we had to bypass the cache when filling
    /// it due to not being able to free enough space.
    pub cache_overflows: u64,
    /// `uint64 mem_peak` — peak memory usage in bytes.
    pub mem_peak: u64,
}

/// `SharedMemoizeInfo` (execnodes.h) — shared-memory container for per-worker
/// memoize statistics. `sinstrument` is a flexible-array member, modeled as a
/// zero-length array.
///
/// ```c
/// typedef struct SharedMemoizeInfo {
///     int num_workers;
///     MemoizeInstrumentation sinstrument[FLEXIBLE_ARRAY_MEMBER];
/// } SharedMemoizeInfo;
/// ```
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct SharedMemoizeInfo {
    /// `int num_workers`
    pub num_workers: core::ffi::c_int,
    /// `MemoizeInstrumentation sinstrument[FLEXIBLE_ARRAY_MEMBER]`
    pub sinstrument: [MemoizeInstrumentation; 0],
}

/// `Memoize` plan node (plannodes.h):
///
/// ```c
/// typedef struct Memoize {
///     Plan        plan;
///     int         numKeys;
///     Oid        *hashOperators;
///     Oid        *collations;
///     List       *param_exprs;
///     bool        singlerow;
///     bool        binary_mode;
///     uint32      est_entries;
///     Bitmapset  *keyparamids;
/// } Memoize;
/// ```
///
/// The leading `plan` is the abstract [`PlanNode`] base (its first field is the
/// `NodeTag`), so a `*mut MemoizePlan` is also a valid `Node *` / `Plan *`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct MemoizePlan {
    /// `Plan plan` — the abstract plan-node base.
    pub plan: PlanNode,
    /// `int numKeys` — size of the two arrays below.
    pub numKeys: core::ffi::c_int,
    /// `Oid *hashOperators` — hash operators for each key (`numKeys` long).
    pub hashOperators: *mut Oid,
    /// `Oid *collations` — collations for each key (`numKeys` long).
    pub collations: *mut Oid,
    /// `List *param_exprs` — cache keys as exprs containing parameters.
    pub param_exprs: *mut List,
    /// `bool singlerow` — mark cache entry complete after the first tuple.
    pub singlerow: bool,
    /// `bool binary_mode` — compare cache keys bit-by-bit vs. hash equality ops.
    pub binary_mode: bool,
    /// `uint32 est_entries` — planner estimate of entries fitting in the cache.
    pub est_entries: u32,
    /// `Bitmapset *keyparamids` — paramids from `param_exprs`.
    pub keyparamids: *mut Bitmapset,
}

/// `MemoizeState` (execnodes.h):
///
/// ```c
/// typedef struct MemoizeState {
///     ScanState   ss;             /* its first field is NodeTag */
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
///
/// The leading [`crate::ScanStateData`] head's first member is a `NodeTag`, so a
/// `*mut MemoizeStateData` is also a valid `Node *` / `PlanState *`. The
/// cache-internal `hashtable`/`last_tuple`/`entry` pointers are opaque here; the
/// node crate casts them to its in-crate `memoize_hash`/`MemoizeTuple`/
/// `MemoizeEntry`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct MemoizeStateData {
    /// `ScanState ss` — its first field is `NodeTag`.
    pub ss: ScanStateData,
    /// `int mstatus` — value of the `ExecMemoize` state machine.
    pub mstatus: core::ffi::c_int,
    /// `int nkeys` — number of cache keys.
    pub nkeys: core::ffi::c_int,
    /// `struct memoize_hash *hashtable` — cache-entry hash table (opaque here).
    pub hashtable: *mut c_void,
    /// `TupleDesc hashkeydesc` — tuple descriptor for cache keys.
    pub hashkeydesc: TupleDesc,
    /// `TupleTableSlot *tableslot` — min tuple slot for existing cache entries.
    pub tableslot: *mut TupleTableSlot,
    /// `TupleTableSlot *probeslot` — virtual slot used for hash lookups.
    pub probeslot: *mut TupleTableSlot,
    /// `ExprState *cache_eq_expr` — compare exec params to hash key.
    pub cache_eq_expr: *mut ExprState,
    /// `ExprState **param_exprs` — exprs containing the parameters to this node.
    pub param_exprs: *mut *mut ExprState,
    /// `FmgrInfo *hashfunctions` — lookup data for hash funcs (`nkeys` long).
    pub hashfunctions: *mut FmgrInfo,
    /// `Oid *collations` — collation for comparisons (`nkeys` long).
    pub collations: *mut Oid,
    /// `uint64 mem_used` — bytes of memory used by the cache.
    pub mem_used: u64,
    /// `uint64 mem_limit` — memory limit in bytes for the cache.
    pub mem_limit: u64,
    /// `MemoryContext tableContext` — memory context to store cache data.
    pub tableContext: MemoryContext,
    /// `dlist_head lru_list` — least recently used entry list.
    pub lru_list: dlist_head,
    /// `struct MemoizeTuple *last_tuple` — last tuple returned/stored (opaque).
    pub last_tuple: *mut c_void,
    /// `struct MemoizeEntry *entry` — the entry `last_tuple` belongs to (opaque).
    pub entry: *mut c_void,
    /// `bool singlerow` — mark the cache entry complete after the first tuple.
    pub singlerow: bool,
    /// `bool binary_mode` — compare cache key bit-by-bit vs. hash equality ops.
    pub binary_mode: bool,
    /// `MemoizeInstrumentation stats` — execution statistics.
    pub stats: MemoizeInstrumentation,
    /// `SharedMemoizeInfo *shared_info` — statistics for parallel workers.
    pub shared_info: *mut SharedMemoizeInfo,
    /// `Bitmapset *keyparamids` — `Param->paramids` of the `param_exprs`.
    pub keyparamids: *mut Bitmapset,
}

// ===========================================================================
// Layout asserts: the embedded heads must keep their C offsets so a
// `*mut MemoizeStateData` can be navigated as the C `MemoizeState *`, and a
// `*mut MemoizePlan` as the C `Memoize *`.
// ===========================================================================
const _: () = {
    // MemoizePlan { Plan plan; int numKeys; ... }
    assert!(core::mem::offset_of!(MemoizePlan, plan) == 0);
    assert!(core::mem::offset_of!(PlanNode, type_) == 0);
    assert!(core::mem::offset_of!(MemoizePlan, numKeys) == core::mem::size_of::<PlanNode>());

    // MemoizeState { ScanState ss; int mstatus; ... }
    assert!(core::mem::offset_of!(MemoizeStateData, ss) == 0);
    assert!(core::mem::offset_of!(ScanStateData, ps) == 0);
    assert!(
        core::mem::offset_of!(MemoizeStateData, mstatus) == core::mem::size_of::<ScanStateData>()
    );

    // MemoizeInstrumentation is a plain block of five uint64 counters.
    assert!(core::mem::size_of::<MemoizeInstrumentation>() == 40);
};
