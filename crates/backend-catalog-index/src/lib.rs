// The reindex file-static globals (`currentlyReindexedHeap`,
// `currentlyReindexedIndex`, `pendingReindexedIndexes`, `reindexingNestLevel`)
// are backend-local process statics in C; here they live in a `thread_local!`
// (a `std`-only macro), so this crate is `std`, matching the idiomatic catalog
// crates that model backend-local process statics (`namespace`,
// `objectaccess`). The public functions keep their PostgreSQL C names
// (`IndexGetRelation`, `ReindexIsProcessingIndex`, …) as the stable API surface.
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
// The file-static reindex-state mutators `SetReindexProcessing`,
// `ResetReindexProcessing`, `SetReindexPending`, `RemoveReindexPending`, and
// `ReindexIsCurrentlyProcessingIndex` are exercised only by `reindex_index` /
// `reindex_relation` (the not-yet-landed catalog-write/build drivers that live
// in this same crate; see the crate header for their keystone blockers). They
// are faithful, complete parts of the index.c reindexing state machine kept
// here so the machine lands whole, not stubbed; intra-crate callers arrive when
// those drivers do. (`RemoveReindexPending` is also called by
// `SetReindexProcessing`, which is itself driver-only.)
#![allow(dead_code)]

//! Partial port of `backend/catalog/index.c` — code to create and destroy
//! POSTGRES index relations.
//!
//! # What this pass lands
//!
//! Two faithful, fully-grounded slices of `index.c`:
//!
//! * `IndexGetRelation(indexId, missing_ok)` — given an index's OID, return the
//!   OID of the table it indexes, via the `INDEXRELID` syscache.
//!
//! * The **system-index reindexing support** backend-local state machine:
//!   `ReindexIsProcessingHeap`, `ReindexIsCurrentlyProcessingIndex`,
//!   `ReindexIsProcessingIndex`, `SetReindexProcessing`,
//!   `ResetReindexProcessing`, `SetReindexPending`, `RemoveReindexPending`,
//!   `ResetReindexState`, plus the parallel-worker transfer
//!   (`EstimateReindexStateSpace` / `SerializeReindexState` /
//!   `RestoreReindexState`). The four file-static globals are modeled as
//!   backend-local thread-local state, mirroring the C process statics.
//!
//! These install the inward seams real consumers already wire to:
//! `index_get_relation` (brin scan/insert-vacuum, cluster),
//! `reindex_is_processing_index` (indexam, relcache), `reset_reindex_state`
//! (xact abort), and the three parallel reindex-state transfer seams
//! (parallel.c).
//!
//! # What is NOT landed in this pass (precise STOP boundaries)
//!
//! The catalog-write / build / drop core of `index.c` (`index_create`,
//! `BuildIndexInfo`, `index_build`, `validate_index`, `index_drop`, the
//! `index_concurrently_*` family, `index_set_state_flags`, `index_update_stats`,
//! `index_check_primary_key`, `CompareIndexInfo`, …) is blocked on two
//! genuinely-unported keystones, contrary to the task premise:
//!
//! 1. **The pg_index INSERT carrier keystone.** `UpdateIndexRelation` (the
//!    `index_create` core) and `BuildIndexInfo` need a full `FormData_pg_index`
//!    /`PgIndexInsertRow` carrier (all 23 columns incl. the `int2vector indkey`
//!    / `oidvector indcollation,indclass` / `int2vector indoption` /
//!    `pg_node_tree indexprs,indpred` fields) plus a `catalog_tuple_insert_pg_index`
//!    producer. The repo's `types_rel::FormData_pg_index` is a 7-field relcache
//!    projection, and only `catalog_tuple_update_pg_index` exists (no INSERT
//!    producer, no INSERT row type). This is a K1-style keystone in its own
//!    right that also ripples into relcache's `rd_index` build. `BuildIndexInfo`
//!    additionally needs `RelationGetIndexExpressions` / `RelationGetIndexPredicate`
//!    / `RelationGetExclusionInfo` relcache seams (none exist; they decode
//!    `pg_node_tree` via the unported node-tree string reader).
//!
//! 2. **The ambuild vtable keystone.** `index_build` dispatches through
//!    `amroutine->ambuild`, but `types_tableam::amapi::IndexAmRoutine` has no
//!    `ambuild` (nor `ambuildempty` / `amoptions` / `amgettuple` / …) slot — the
//!    landed vtable carries only scan / insert / vacuum callbacks. Adding the
//!    `ambuild` slot and populating it in every AM handler (nbtree / hash / gist
//!    / gin / spgist / brin) is a cross-cutting vtable keystone.
//!
//! `index_set_state_flags` is additionally blocked on widening `PgIndexForm`
//! (which currently exposes only `indisclustered` / `indisvalid`, not the
//! `indislive` / `indisready` / `indisreplident` it mutates) and on a
//! `table_open(pg_index)` path. The `index_concurrently_*` family and
//! `index_drop` further depend on `WaitForLockers` / snapshot push/pop /
//! relcache rebuild substrate with no owned-tree representation yet.
//!
//! The owner `index_create` / `build_index_info` / `index_build` /
//! `reindex_relation` / `index_drop` inward seams (declared in
//! `backend-catalog-index-seams`) therefore stay UNINSTALLED in this pass: a
//! call panics loudly (mirror-PG-and-panic), exactly as before this crate
//! existed.

extern crate alloc;

use alloc::vec::Vec;

use types_core::primitive::{InvalidOid, OidIsValid};
use types_core::primitive::{Oid, Size};
use types_error::{PgError, PgResult};

use backend_access_transam_xact_seams as xact;
use backend_catalog_index_seams as index_seam;
use backend_access_transam_parallel_rt_seams as rt;
use backend_utils_cache_syscache_seams as syscache;

/* ===========================================================================
 * Backend-local reindexing state.
 *
 * Mirrors index.c's file-static globals:
 *
 *     static Oid   currentlyReindexedHeap = InvalidOid;
 *     static Oid   currentlyReindexedIndex = InvalidOid;
 *     static List *pendingReindexedIndexes = NIL;
 *     static int   reindexingNestLevel = 0;
 * ========================================================================= */

struct ReindexState {
    currently_reindexed_heap: Oid,
    currently_reindexed_index: Oid,
    pending_reindexed_indexes: Vec<Oid>,
    reindexing_nest_level: i32,
}

impl ReindexState {
    const fn new() -> Self {
        ReindexState {
            currently_reindexed_heap: InvalidOid,
            currently_reindexed_index: InvalidOid,
            pending_reindexed_indexes: Vec::new(),
            reindexing_nest_level: 0,
        }
    }
}

thread_local! {
    static REINDEX: core::cell::RefCell<ReindexState> =
        const { core::cell::RefCell::new(ReindexState::new()) };
}

/// `elog(ERROR, ...)` — internal error (`ERRCODE_INTERNAL_ERROR` is the elog
/// default).
fn elog_error<T>(message: alloc::string::String) -> PgResult<T> {
    Err(PgError::error(message))
}

/// `list_member_oid(list, oid)`.
fn list_member_oid(list: &[Oid], oid: Oid) -> bool {
    list.contains(&oid)
}

/* ===========================================================================
 * IndexGetRelation
 * ========================================================================= */

/// `IndexGetRelation(indexId, missing_ok)` (catalog/index.c): given an index's
/// relation OID, get the OID of the relation it is an index on. Uses the system
/// cache.
///
/// ```c
/// Oid
/// IndexGetRelation(Oid indexId, bool missing_ok)
/// {
///     HeapTuple   tuple;
///     Form_pg_index index;
///     Oid         result;
///
///     tuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(indexId));
///     if (!HeapTupleIsValid(tuple))
///     {
///         if (missing_ok)
///             return InvalidOid;
///         elog(ERROR, "cache lookup failed for index %u", indexId);
///     }
///     index = (Form_pg_index) GETSTRUCT(tuple);
///     Assert(index->indexrelid == indexId);
///
///     result = index->indrelid;
///     ReleaseSysCache(tuple);
///     return result;
/// }
/// ```
///
/// The C reads two scalars off the `INDEXRELID` syscache tuple — `indexrelid`
/// (only for the `Assert`) and `indrelid` (the result) — and allocates nothing.
/// The `index_get_relid` syscache seam projects exactly `indrelid` by value
/// (`Ok(None)` on the cache miss); the `Assert(index->indexrelid == indexId)` is
/// inside the syscache lookup by construction (it keys on `indexId`).
pub fn IndexGetRelation(indexId: Oid, missing_ok: bool) -> PgResult<Oid> {
    match syscache::index_get_relid::call(indexId)? {
        Some(indrelid) => Ok(indrelid),
        None => {
            if missing_ok {
                return Ok(InvalidOid);
            }
            elog_error(alloc::format!("cache lookup failed for index {indexId}"))
        }
    }
}

/* ===========================================================================
 * System index reindexing support
 * ========================================================================= */

/// `ReindexIsProcessingHeap(heapOid)` — true if the heap specified by OID is
/// currently being reindexed.
pub fn ReindexIsProcessingHeap(heapOid: Oid) -> bool {
    REINDEX.with(|s| heapOid == s.borrow().currently_reindexed_heap)
}

/// `ReindexIsCurrentlyProcessingIndex(indexOid)` — true if the index specified
/// by OID is currently being reindexed. (File-static in C.)
fn ReindexIsCurrentlyProcessingIndex(indexOid: Oid) -> bool {
    REINDEX.with(|s| indexOid == s.borrow().currently_reindexed_index)
}

/// `ReindexIsProcessingIndex(indexOid)` — true if the index specified by OID is
/// currently being reindexed, or should be treated as invalid because it is
/// awaiting reindex.
pub fn ReindexIsProcessingIndex(indexOid: Oid) -> bool {
    REINDEX.with(|s| {
        let s = s.borrow();
        indexOid == s.currently_reindexed_index
            || list_member_oid(&s.pending_reindexed_indexes, indexOid)
    })
}

/// `SetReindexProcessing(heapOid, indexOid)` — set flag that specified
/// heap/index are being reindexed. (File-static in C.)
fn SetReindexProcessing(heapOid: Oid, indexOid: Oid) -> PgResult<()> {
    debug_assert!(OidIsValid(heapOid) && OidIsValid(indexOid));
    // Reindexing is not re-entrant.
    let already = REINDEX.with(|s| OidIsValid(s.borrow().currently_reindexed_heap));
    if already {
        return elog_error("cannot reindex while reindexing".into());
    }
    REINDEX.with(|s| {
        let mut s = s.borrow_mut();
        s.currently_reindexed_heap = heapOid;
        s.currently_reindexed_index = indexOid;
    });
    // Index is no longer "pending" reindex.
    RemoveReindexPending(indexOid)?;
    // This may have been set already, but in case it isn't, do so now.
    let nest = xact::get_current_transaction_nest_level::call();
    REINDEX.with(|s| s.borrow_mut().reindexing_nest_level = nest);
    Ok(())
}

/// `ResetReindexProcessing()` — unset reindexing status. (File-static in C.)
fn ResetReindexProcessing() {
    REINDEX.with(|s| {
        let mut s = s.borrow_mut();
        s.currently_reindexed_heap = InvalidOid;
        s.currently_reindexed_index = InvalidOid;
        // reindexingNestLevel remains set till end of (sub)transaction
    });
}

/// `SetReindexPending(indexes)` — mark the given indexes as pending reindex.
/// (File-static in C.)
///
/// NB: we assume that the current memory context stays valid throughout.
fn SetReindexPending(indexes: &[Oid]) -> PgResult<()> {
    // Reindexing is not re-entrant.
    let pending_nonempty = REINDEX.with(|s| !s.borrow().pending_reindexed_indexes.is_empty());
    if pending_nonempty {
        return elog_error("cannot reindex while reindexing".into());
    }
    if xact::is_in_parallel_mode::call() {
        return elog_error("cannot modify reindex state during a parallel operation".into());
    }
    let nest = xact::get_current_transaction_nest_level::call();
    REINDEX.with(|s| {
        let mut s = s.borrow_mut();
        s.pending_reindexed_indexes = indexes.to_vec(); // list_copy
        s.reindexing_nest_level = nest;
    });
    Ok(())
}

/// `RemoveReindexPending(indexOid)` — remove the given index from the pending
/// list. (File-static in C.)
fn RemoveReindexPending(indexOid: Oid) -> PgResult<()> {
    if xact::is_in_parallel_mode::call() {
        return elog_error("cannot modify reindex state during a parallel operation".into());
    }
    REINDEX.with(|s| {
        // list_delete_oid: remove all occurrences of indexOid.
        s.borrow_mut()
            .pending_reindexed_indexes
            .retain(|&oid| oid != indexOid)
    });
    Ok(())
}

/// `ResetReindexState(nestLevel)` — clear all reindexing state during
/// (sub)transaction abort.
///
/// Because reindexing is not re-entrant, we don't need to cope with nested
/// reindexing states. We just need to avoid messing up the outer-level state in
/// case a subtransaction fails within a REINDEX. So checking the current nest
/// level against that of the reindex operation is sufficient.
pub fn ResetReindexState(nestLevel: i32) {
    REINDEX.with(|s| {
        let mut s = s.borrow_mut();
        if s.reindexing_nest_level >= nestLevel {
            s.currently_reindexed_heap = InvalidOid;
            s.currently_reindexed_index = InvalidOid;

            // We needn't try to release the contents of
            // pendingReindexedIndexes; that list should be in a
            // transaction-lifespan context, so it will go away automatically.
            s.pending_reindexed_indexes = Vec::new();

            s.reindexing_nest_level = 0;
        }
    });
}

/* ---------------------------------------------------------------------------
 * Parallel-worker transfer of the reindex state.
 *
 * C `SerializedReindexState`:
 *
 *     typedef struct
 *     {
 *         Oid  currentlyReindexedHeap;
 *         Oid  currentlyReindexedIndex;
 *         int  numPendingReindexedIndexes;
 *         Oid  pendingReindexedIndexes[FLEXIBLE_ARRAY_MEMBER];
 *     } SerializedReindexState;
 *
 * On every supported platform Oid and int are 4-byte, 4-aligned, so the
 * flexible array starts at offset 12 with no trailing padding. We mirror that
 * exact layout for the DSM bytes.
 * ------------------------------------------------------------------------- */

/// `offsetof(SerializedReindexState, pendingReindexedIndexes)` = 2*sizeof(Oid)
/// + sizeof(int) = 12 bytes.
const SERIALIZED_REINDEX_HEADER: usize =
    2 * core::mem::size_of::<Oid>() + core::mem::size_of::<i32>();

/// `mul_size(s1, s2)` (memutils): multiply two sizes, raising on overflow.
fn mul_size(s1: Size, s2: Size) -> PgResult<Size> {
    s1.checked_mul(s2)
        .ok_or_else(|| PgError::error(alloc::string::String::from("requested shared memory size overflows size_t")))
}

/// `EstimateReindexStateSpace()` — estimate space needed to pass reindex state
/// to parallel workers.
///
/// ```c
/// return offsetof(SerializedReindexState, pendingReindexedIndexes)
///     + mul_size(sizeof(Oid), list_length(pendingReindexedIndexes));
/// ```
pub fn EstimateReindexStateSpace() -> PgResult<Size> {
    let n = REINDEX.with(|s| s.borrow().pending_reindexed_indexes.len());
    Ok(SERIALIZED_REINDEX_HEADER + mul_size(core::mem::size_of::<Oid>(), n)?)
}

/// `SerializeReindexState(maxsize, start_address)` — serialize reindex state for
/// parallel workers. `start_address` is the DSM chunk the leader reserved
/// (`EstimateReindexStateSpace` sized it).
///
/// # Safety
///
/// `start_address` must point at a writable chunk of at least
/// `EstimateReindexStateSpace()` bytes, as reserved by the parallel-DSM leader.
unsafe fn serialize_reindex_state(_maxsize: Size, start_address: usize) {
    REINDEX.with(|s| {
        let s = s.borrow();
        let n = s.pending_reindexed_indexes.len();
        // sistate->currentlyReindexedHeap
        let heap = start_address as *mut Oid;
        // sistate->currentlyReindexedIndex
        let index = (start_address + core::mem::size_of::<Oid>()) as *mut Oid;
        // sistate->numPendingReindexedIndexes
        let numptr = (start_address + 2 * core::mem::size_of::<Oid>()) as *mut i32;
        unsafe {
            heap.write_unaligned(s.currently_reindexed_heap);
            index.write_unaligned(s.currently_reindexed_index);
            numptr.write_unaligned(n as i32);
            // sistate->pendingReindexedIndexes[c++] = lfirst_oid(lc);
            let arr = (start_address + SERIALIZED_REINDEX_HEADER) as *mut Oid;
            for (c, &oid) in s.pending_reindexed_indexes.iter().enumerate() {
                arr.add(c).write_unaligned(oid);
            }
        }
    });
}

/// `RestoreReindexState(reindexstate)` — restore reindex state in a parallel
/// worker.
///
/// # Safety
///
/// `reindexstate` must point at the chunk a leader serialized with
/// `serialize_reindex_state`.
unsafe fn restore_reindex_state(reindexstate: usize) {
    let heap = reindexstate as *const Oid;
    let index = (reindexstate + core::mem::size_of::<Oid>()) as *const Oid;
    let numptr = (reindexstate + 2 * core::mem::size_of::<Oid>()) as *const i32;
    let (currently_heap, currently_index, num) = unsafe {
        (
            heap.read_unaligned(),
            index.read_unaligned(),
            numptr.read_unaligned(),
        )
    };

    REINDEX.with(|s| {
        let mut s = s.borrow_mut();
        s.currently_reindexed_heap = currently_heap;
        s.currently_reindexed_index = currently_index;

        debug_assert!(s.pending_reindexed_indexes.is_empty());
        // The C switches to TopMemoryContext while lappend'ing; the owned Vec
        // carries the same lifetime as the backend-local state here.
        let arr = (reindexstate + SERIALIZED_REINDEX_HEADER) as *const Oid;
        for c in 0..(num as usize) {
            let oid = unsafe { arr.add(c).read_unaligned() };
            s.pending_reindexed_indexes.push(oid); // lappend_oid
        }
    });

    // Note the worker has its own transaction nesting level
    let nest = xact::get_current_transaction_nest_level::call();
    REINDEX.with(|s| s.borrow_mut().reindexing_nest_level = nest);
}

/* ===========================================================================
 * Seam installation
 * ========================================================================= */

/// Install this unit's inward seams. Mirror-PG-and-panic: only the two
/// fully-grounded slices are installed here; the catalog-write / build / drop
/// core (`index_create`, `build_index_info`, `index_build`, `reindex_relation`,
/// `index_drop`) stays uninstalled until its keystones land (see the crate
/// header), so calling one still panics loudly.
pub fn init_seams() {
    // IndexGetRelation.
    index_seam::index_get_relation::set(IndexGetRelation);

    // Reindexing-support state machine.
    index_seam::reindex_is_processing_index::set(ReindexIsProcessingIndex);
    index_seam::reset_reindex_state::set(ResetReindexState);

    // Parallel-worker transfer of the reindex state (owned by index.c, the
    // bodies installed here; the seam decls live in parallel-rt-seams).
    rt::estimate_reindex_state_space::set(EstimateReindexStateSpace);
    rt::serialize_reindex_state::set(|len, space| {
        // SAFETY: `space` is the start of a `len`-byte chunk shm_toc_allocate
        // reserved for the reindex state (EstimateReindexStateSpace sized it);
        // the leader writes the whole chunk here. The audited DSM-pointer
        // primitive (cf. backend-utils-misc-guc serialize_guc_state).
        unsafe { serialize_reindex_state(len, space) };
        Ok(())
    });
    rt::restore_reindex_state::set(|space| {
        // SAFETY: `space` points at the reindex-state chunk the leader
        // serialized; the embedded numPendingReindexedIndexes bounds the read.
        unsafe { restore_reindex_state(space) };
        Ok(())
    });
}
