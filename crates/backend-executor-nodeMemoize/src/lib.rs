//! Port of `nodeMemoize.c` — caching of results from parameterized nodes.
//!
//! A Memoize node sits above a parameterized inner plan and caches the tuples it
//! produces for each distinct set of parameter values, so a repeat scan with a
//! previously-seen parameter set replays from the cache instead of re-scanning
//! the inner node. The cache is bounded by a memory budget; when it fills, the
//! least-recently-used entry is evicted (tuples are never spilled to disk).
//!
//! The owned model holds the executor node as a [`MemoizeScanState`] mutated
//! through `&mut` borrows. The C `simplehash.h` cache table, the intrusive
//! `lib/ilist.h` LRU list and the `palloc`'d `MemoizeEntry`/`MemoizeKey`/
//! `MemoizeTuple` records are replaced by the owned [`MemoizeCache`] (a slot
//! vector with an explicit free-list, an LRU queue and a hash index). The state
//! machine, the memory accounting (`mem_used`/`mem_limit`, peak tracking,
//! eviction) and the cache statistics are byte-faithful to PostgreSQL 18.3.
//!
//! `ExecMemoize` recurses into its single outer child through the child's
//! installed `PlanState.ExecProcNode` (the [`exec_proc_outer`](seam::exec_proc_outer)
//! seam). The subsystems below the node layer — the expression engine, the
//! tuple-slot ops, the `simplehash` hash/equality leaves, the catalog
//! hash-function lookups, the memory budget, and the DSM/parallel machinery —
//! are reached through this crate's per-owner seam crates, each defaulting to a
//! loud panic until the owner lands.

#![allow(non_snake_case)]
#![allow(clippy::result_large_err)]
#![allow(clippy::too_many_arguments)]

extern crate alloc;

use alloc::vec::Vec;

use mcx::Mcx;
use types_core::primitive::{Oid, Size};
use types_error::{PgError, PgResult, ERRCODE_INTERNAL_ERROR, ERRCODE_OUT_OF_MEMORY};
use types_nodes::execnodes::EStateData;
use types_nodes::nodememoize::{
    CacheEntry, CachedTuple, MemoStatus, Memoize, MemoizeCache, MemoizeInstrumentation,
    MemoizeScanState, SharedMemoizeInfo,
};
use types_tuple::heaptuple::MinimalTupleData;

use backend_access_transam_parallel_seams as parallel;
use backend_executor_nodeMemoize_seams as seam;
use backend_executor_execParallel_support_seams as sup;
use types_execparallel::{ParallelContextHandle, ParallelWorkerContextHandle, PlanStateHandle};

/// `EXEC_FLAG_BACKWARD` (executor.h).
const EXEC_FLAG_BACKWARD: i32 = 0x0008;
/// `EXEC_FLAG_MARK` (executor.h).
const EXEC_FLAG_MARK: i32 = 0x0010;

/// `offsetof(SharedMemoizeInfo, sinstrument)` on LP64: `int num_workers` padded
/// to the 8-byte alignment of the `MemoizeInstrumentation` array.
const OFFSETOF_SHARED_MEMOIZE_INFO_SINSTRUMENT: Size = 8;
/// `sizeof(MemoizeInstrumentation)` on LP64: five `uint64` -> 40 bytes.
const SIZEOF_MEMOIZE_INSTRUMENTATION: Size = 40;

/// `mul_size(s1, s2)` (shmem.c) — overflow-checked multiply.
fn mul_size(s1: Size, s2: Size) -> PgResult<Size> {
    s1.checked_mul(s2)
        .ok_or_else(|| PgError::error("requested shared memory size overflows size_t"))
}

/// `add_size(s1, s2)` (shmem.c) — overflow-checked add.
fn add_size(s1: Size, s2: Size) -> PgResult<Size> {
    s1.checked_add(s2)
        .ok_or_else(|| PgError::error("requested shared memory size overflows size_t"))
}

// ===========================================================================
// nodeMemoize.c static helpers, ported against the owned cache.
// ===========================================================================

/// `build_hash_table(mstate, size)` — initialize the cache to empty.
///
/// The C node converts `size` to a power of two and pre-sizes the `simplehash`
/// bucket array; the owned [`MemoizeCache`] grows on demand, so `size` (the
/// planner's `est_entries`) seeds an initial slot reservation. As in C, a zero
/// `size` is replaced with a default guess of 1024.
fn build_hash_table(mstate: &mut MemoizeScanState, size: u32) -> PgResult<()> {
    debug_assert!(mstate.hashtable.is_none());

    // Make a guess at a good size when we're not given a valid size.
    let size = if size == 0 { 1024 } else { size };

    let mut cache = MemoizeCache::new();
    reserve_slots(&mut cache, size as usize)?;
    mstate.hashtable = Some(cache);
    Ok(())
}

/// Cap on the up-front slot reservation, mirroring the C `simplehash`
/// `SH_MAX_SIZE` of `(uint64) PG_UINT32_MAX + 1`. A bogus planner estimate is
/// clamped so it cannot drive an unbounded allocation; the cache still grows on
/// demand for real inserts.
const MAX_CACHE_SLOTS: usize = u32::MAX as usize;

/// Reserve capacity for `n` entry slots using `try_reserve` against the
/// [`MAX_CACHE_SLOTS`] bound (recoverable error, never abort).
fn reserve_slots(cache: &mut MemoizeCache, n: usize) -> PgResult<()> {
    let want = n.min(MAX_CACHE_SLOTS);
    cache
        .slots
        .try_reserve(want)
        .map_err(|_| out_of_memory("MemoizeHashTable"))?;
    Ok(())
}

/// `entry_purge_tuples(mstate, entry)` — remove all tuples from a cache entry,
/// updating the memory accounting (leaves an empty entry).
fn entry_purge_tuples(mstate: &mut MemoizeScanState, slot_id: usize) -> PgResult<()> {
    let cache = cache_mut(mstate)?;
    let entry = entry_at_mut(cache, slot_id)?;

    let mut freed_mem: u64 = 0;
    for tuple in entry.tuples.drain(..) {
        freed_mem += MemoizeScanState::cache_tuple_bytes(tuple.mintuple.t_len);
    }
    entry.complete = false;
    // entry.tuples is now empty (tuplehead = NULL).

    // Update the memory accounting.
    mstate.mem_used -= freed_mem;
    Ok(())
}

/// `remove_cache_entry(mstate, entry)` — remove an entry and free its memory.
fn remove_cache_entry(mstate: &mut MemoizeScanState, slot_id: usize) -> PgResult<()> {
    // Remove all of the tuples from this entry (also updates mem_used).
    entry_purge_tuples(mstate, slot_id)?;

    let params_len = {
        let cache = cache_ref(mstate)?;
        entry_at(cache, slot_id)?.params.t_len
    };

    // Update memory accounting for the entry itself. entry_purge_tuples has
    // already subtracted the memory used for each cached tuple.
    mstate.mem_used -= MemoizeScanState::empty_entry_memory_bytes(params_len);

    // Remove the entry from the cache (also unlinks it from the LRU list and the
    // hash index, and frees its key/params).
    let cache = cache_mut(mstate)?;
    cache_delete_slot(cache, slot_id);
    Ok(())
}

/// `cache_purge_all(mstate)` — remove all items from the cache.
fn cache_purge_all(mstate: &mut MemoizeScanState) -> PgResult<()> {
    let evictions = match mstate.hashtable.as_ref() {
        Some(cache) => cache.members as u64,
        None => 0,
    };

    // C resets the dedicated tableContext and NULLs the hashtable so it is
    // rebuilt on the next call. In the owned-cache model the cache lives in the
    // node; dropping it (set to None) frees every entry/tuple — there is no
    // separate arena to reset.
    mstate.hashtable = None;

    // reset the LRU list / cursors (the new cache starts empty).
    mstate.last_tuple = None;
    mstate.entry = None;

    mstate.mem_used = 0;

    // XXX should we add something new to track these purges?
    mstate.stats.cache_evictions += evictions; // Update Stats
    Ok(())
}

/// `cache_reduce_memory(mstate, specialkey)` — evict older / less recently used
/// items until `mem_used` is back under `mem_limit`. Returns `false` if the
/// `specialkey`'s entry (identified by `special_slot`) was removed.
fn cache_reduce_memory<'mcx>(
    mstate: &mut MemoizeScanState<'mcx>,
    special_slot: Option<usize>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    let mut specialkey_intact = true; // for now
    let mut evictions: u64 = 0;

    // Update peak memory usage.
    if mstate.mem_used > mstate.stats.mem_peak {
        mstate.stats.mem_peak = mstate.mem_used;
    }

    // We expect only to be called when we've gone over budget on memory.
    debug_assert!(mstate.mem_used > mstate.mem_limit);

    // Start the eviction process at the head (least recently used) of the LRU
    // list (dlist_foreach_modify). The owned LRU queue is ordered LRU-first.
    loop {
        // Peek the least-recently-used live slot id.
        let slot_id = match mstate.hashtable.as_ref().and_then(|c| c.lru.front().copied()) {
            Some(id) => id,
            None => break,
        };

        // The C code re-finds the entry via a probe-slot hash lookup as a sanity
        // check that the LRU key still maps to a live entry. With the owned LRU
        // queue the slot id directly addresses the entry, but we keep the probe
        // preparation so the executor-owned probe slot tracks the C behaviour
        // (later lookups during eviction must repopulate it anyway).
        let params = entry_params_clone(mstate, slot_id, estate)?;
        seam::prepare_probe_from_key::call(mstate, &params, estate)?;

        // Sanity check that we found the entry belonging to the LRU list item.
        if !slot_is_live(mstate, slot_id) {
            return Err(elog_internal("could not find memoization table entry"));
        }

        // If we'd free the entry the specialkey belongs to, inform the caller.
        if Some(slot_id) == special_slot {
            specialkey_intact = false;
        }

        // Finally remove the entry. This will remove it from the LRU list too.
        remove_cache_entry(mstate, slot_id)?;

        evictions += 1;

        // Exit if we've freed enough memory.
        if mstate.mem_used <= mstate.mem_limit {
            break;
        }
    }

    mstate.stats.cache_evictions += evictions; // Update Stats

    Ok(specialkey_intact)
}

/// `cache_lookup(mstate, found)` — look up tuples for the current scan
/// parameters. On a hit, moves the entry to the MRU end of the LRU list, sets
/// `*found = true` and returns its slot id. On a miss, creates a new entry,
/// performs the memory accounting, evicts if over budget, and returns the new
/// slot id — or `None` if it could not free enough memory.
fn cache_lookup<'mcx>(
    mstate: &mut MemoizeScanState<'mcx>,
    found: &mut bool,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<usize>> {
    // Prepare the probe slot with the current scan parameters.
    seam::prepare_probe_from_params::call(mstate, estate)?;

    // Hash the probe slot (mirrors memoize_insert -> MemoizeHash_hash).
    let hash = seam::hash_probe_slot::call(mstate, estate)?;

    // Look for an existing entry with this hash whose key matches the probe.
    if let Some(slot_id) = cache_find_matching(mstate, hash, estate)? {
        *found = true;
        // Move existing entry to the tail of the LRU list (most recently used).
        let cache = cache_mut(mstate)?;
        lru_move_to_back(cache, slot_id);
        return Ok(Some(slot_id));
    }

    *found = false;

    // Allocate a new key/entry. ExecCopySlotMinimalTuple copies the probe slot's
    // parameter values into an owned MinimalTuple used as the entry's key,
    // allocated in the estate's per-query context (C: mstate->tableContext).
    let mcx = estate.es_query_cxt;
    let params = seam::copy_probe_slot_minimal_tuple::call(mstate, mcx, estate)?;
    let params_len = params.t_len;

    let slot_id = {
        let cache = cache_mut(mstate)?;
        let entry = CacheEntry {
            params,
            tuples: Vec::new(),
            hash,
            complete: false,
        };
        cache_insert_entry(cache, entry, hash)?
    };

    // Update the total cache memory utilization.
    mstate.mem_used += MemoizeScanState::empty_entry_memory_bytes(params_len);

    // Since this is the most recently used entry, push it onto the end of the
    // LRU list (cache_insert_entry already did the push_tail).
    mstate.last_tuple = None;

    // If we've gone over our memory budget, free up some space in the cache.
    if mstate.mem_used > mstate.mem_limit {
        // It's highly unlikely we fail here since the new entry has no tuples
        // yet and we can remove any other entry to reduce consumption.
        if !cache_reduce_memory(mstate, Some(slot_id), estate)? {
            return Ok(None);
        }

        // The new entry may itself have been evicted only if it was the
        // specialkey, which cache_reduce_memory reports via the bool above; if
        // we're here it is still live. (Unlike the C simplehash, removing other
        // entries never relocates a surviving slot, so no re-find is needed.)
        debug_assert!(slot_is_live(mstate, slot_id));
    }

    Ok(Some(slot_id))
}

/// `cache_store_tuple(mstate, slot)` — append the outer tuple `mintuple` to the
/// mstate's current cache entry (which must already exist, via `cache_lookup`).
/// Returns `false` (bypass) if it could not free enough memory after exceeding
/// the budget.
fn cache_store_tuple<'mcx>(
    mstate: &mut MemoizeScanState<'mcx>,
    mintuple: &MinimalTupleData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    let entry_slot = mstate
        .entry
        .ok_or_else(|| elog_internal("cache_store_tuple with no current entry"))?;

    let tuple_len = mintuple.t_len;

    {
        let mcx = estate.es_query_cxt;
        // tuple->mintuple = ExecCopySlotMinimalTuple(slot); tuple->next = NULL.
        let copy = mintuple.clone_in(mcx)?;
        let cache = cache_mut(mstate)?;
        let entry = entry_at_mut(cache, entry_slot)?;
        entry
            .tuples
            .try_reserve(1)
            .map_err(|_| out_of_memory("MemoizeTuple"))?;
        entry.tuples.push(CachedTuple { mintuple: copy });
        // mstate->last_tuple now points at the tail of the entry's tuple list.
    }

    // Account for the memory we just consumed.
    mstate.mem_used += MemoizeScanState::cache_tuple_bytes(tuple_len);

    // last_tuple = tuple (index of the just-pushed tail tuple).
    let last_index = {
        let cache = cache_ref(mstate)?;
        entry_at(cache, entry_slot)?.tuples.len() - 1
    };
    mstate.last_tuple = Some(last_index);

    // If we've gone over our memory budget then free up some space in the cache.
    if mstate.mem_used > mstate.mem_limit {
        if !cache_reduce_memory(mstate, Some(entry_slot), estate)? {
            return Ok(false);
        }
        // The surviving entry's slot id is stable (no simplehash relocation), so
        // mstate->entry needs no re-find.
        debug_assert!(slot_is_live(mstate, entry_slot));
    }

    Ok(true)
}

// ===========================================================================
// ExecMemoize state machine (nodeMemoize.c).
// ===========================================================================

/// `ExecMemoize(pstate)` — the `PlanState.ExecProcNode` callback. Looks up the
/// cache and executes the subplan on a miss.
///
/// Returns `Ok(true)` when a result tuple has been placed into the node's
/// `ps_ResultTupleSlot` (via [`store_result_minimal_tuple`](seam::store_result_minimal_tuple)),
/// and `Ok(false)` when the scan is exhausted (the C `return NULL`; the result
/// slot is cleared via [`clear_result_slot`](seam::clear_result_slot)).
pub fn ExecMemoize<'mcx>(
    node: &mut MemoizeScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    seam::check_for_interrupts::call()?;

    // Reset per-tuple memory context to free prior cycle's eval storage.
    seam::reset_expr_context::call(node, estate)?;

    match node.mstatus {
        MemoStatus::CacheLookup => {
            debug_assert!(node.entry.is_none());

            // first call? we'll need a hash table.
            if node.hashtable.is_none() {
                let est = node.est_entries;
                build_hash_table(node, est)?;
            }

            // see if we've got anything cached for the current parameters.
            let mut found = false;
            let entry = cache_lookup(node, &mut found, estate)?;

            if found && entry.is_some() && entry_is_complete(node, slot_or_err(entry)?)? {
                node.stats.cache_hits += 1; // stats update

                let slot_id = slot_or_err(entry)?;

                // Set last_tuple and entry for MEMO_CACHE_FETCH_NEXT_TUPLE.
                node.entry = Some(slot_id);

                // Fetch the first cached tuple, if there is one.
                if entry_has_tuples(node, slot_id)? {
                    node.last_tuple = Some(0);
                    node.mstatus = MemoStatus::CacheFetchNextTuple;

                    let tuple = entry_tuple_clone(node, slot_id, 0, estate)?;
                    seam::store_result_minimal_tuple::call(node, &tuple, estate)?;
                    return Ok(true);
                }

                // The cache entry is void of any tuples.
                node.last_tuple = None;
                node.mstatus = MemoStatus::EndOfScan;
                seam::clear_result_slot::call(node, estate)?;
                return Ok(false);
            }

            // Handle cache miss.
            node.stats.cache_misses += 1; // stats update

            if found {
                if let Some(slot_id) = entry {
                    // A cache entry was found but the scan didn't complete;
                    // remove all tuples and start again (the outer node may not
                    // re-produce tuples in the same order).
                    entry_purge_tuples(node, slot_id)?;
                }
            }

            // Scan the outer node for a tuple to cache.
            let mcx = estate.es_query_cxt;
            let outerslot = seam::exec_proc_outer::call(node, mcx, estate)?;
            let outerslot = match outerslot {
                Some(t) => t,
                None => {
                    // cache_lookup may have returned None due to failure to free
                    // enough cache space, so guard on `entry`. No need for bypass
                    // mode here as we're setting mstatus to end of scan.
                    if let Some(slot_id) = entry {
                        entry_set_complete(node, slot_id, true)?;
                    }
                    node.mstatus = MemoStatus::EndOfScan;
                    seam::clear_result_slot::call(node, estate)?;
                    return Ok(false);
                }
            };

            node.entry = entry;

            // If we failed to create the entry or failed to store the tuple in
            // the entry, then go into bypass mode.
            if entry.is_none() || !cache_store_tuple(node, &outerslot, estate)? {
                node.stats.cache_overflows += 1; // stats update

                node.mstatus = MemoStatus::CacheBypassMode;
                // No need to clear last_tuple; we stay in bypass until end.
            } else {
                // If we only expect a single row, mark complete now. This allows
                // cache lookups to work even when the scan didn't run to
                // completion.
                let singlerow = node.singlerow;
                entry_set_complete(node, slot_or_err(entry)?, singlerow)?;
                node.mstatus = MemoStatus::FillingCache;
            }

            // ExecCopySlot(resultslot, outerslot) — the result slot is
            // TTSOpsMinimalTuple, equivalent to storing the outer minimal tuple.
            seam::store_result_minimal_tuple::call(node, &outerslot, estate)?;
            Ok(true)
        }

        MemoStatus::CacheFetchNextTuple => {
            // We shouldn't be in this state if these are not set.
            debug_assert!(node.entry.is_some());
            debug_assert!(node.last_tuple.is_some());

            let slot_id = node
                .entry
                .ok_or_else(|| elog_internal("MEMO_CACHE_FETCH_NEXT_TUPLE: entry is NULL"))?;

            // Skip to the next tuple to output.
            let next_index = node
                .last_tuple
                .ok_or_else(|| elog_internal("MEMO_CACHE_FETCH_NEXT_TUPLE: last_tuple is NULL"))?
                + 1;

            // No more tuples in the cache.
            if next_index >= entry_tuple_count(node, slot_id)? {
                node.last_tuple = None;
                node.mstatus = MemoStatus::EndOfScan;
                seam::clear_result_slot::call(node, estate)?;
                return Ok(false);
            }

            node.last_tuple = Some(next_index);

            let tuple = entry_tuple_clone(node, slot_id, next_index, estate)?;
            seam::store_result_minimal_tuple::call(node, &tuple, estate)?;
            Ok(true)
        }

        MemoStatus::FillingCache => {
            // entry should already have been set by MEMO_CACHE_LOOKUP.
            let slot_id = node
                .entry
                .ok_or_else(|| elog_internal("MEMO_FILLING_CACHE with no current entry"))?;

            // Populate the cache with the current scan tuples.
            let mcx = estate.es_query_cxt;
            let outerslot = seam::exec_proc_outer::call(node, mcx, estate)?;
            let outerslot = match outerslot {
                Some(t) => t,
                None => {
                    // No more tuples. Mark it as complete.
                    entry_set_complete(node, slot_id, true)?;
                    node.mstatus = MemoStatus::EndOfScan;
                    seam::clear_result_slot::call(node, estate)?;
                    return Ok(false);
                }
            };

            // Validate that the planner properly set the singlerow flag.
            if entry_is_complete(node, slot_id)? {
                return Err(elog_internal("cache entry already complete"));
            }

            // Record the tuple in the current cache entry.
            if !cache_store_tuple(node, &outerslot, estate)? {
                // Couldn't store it? Handle overflow.
                node.stats.cache_overflows += 1; // stats update
                node.mstatus = MemoStatus::CacheBypassMode;
                // No need to clear entry/last_tuple; we stay in bypass until end.
            }

            seam::store_result_minimal_tuple::call(node, &outerslot, estate)?;
            Ok(true)
        }

        MemoStatus::CacheBypassMode => {
            // Continue reading tuples without caching until the next rescan.
            let mcx = estate.es_query_cxt;
            let outerslot = seam::exec_proc_outer::call(node, mcx, estate)?;
            match outerslot {
                Some(t) => {
                    seam::store_result_minimal_tuple::call(node, &t, estate)?;
                    Ok(true)
                }
                None => {
                    node.mstatus = MemoStatus::EndOfScan;
                    seam::clear_result_slot::call(node, estate)?;
                    Ok(false)
                }
            }
        }

        MemoStatus::EndOfScan => {
            // We've already returned NULL for this scan; just in case.
            seam::clear_result_slot::call(node, estate)?;
            Ok(false)
        }
    }
}

/// `ExecInitMemoize(node, estate, eflags)` — initialize the node and subnodes.
pub fn ExecInitMemoize<'mcx>(
    node: &Memoize<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
) -> PgResult<alloc::boxed::Box<MemoizeScanState<'mcx>>> {
    let mut mstate = seam::make_memoize_state::call(estate)?;

    // check for unsupported flags.
    debug_assert!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK) == 0);

    // mstate->ss.ps.plan = (Plan *) node; ->state = estate;
    // ->ExecProcNode = ExecMemoize.
    seam::init_plan_state_links::call(&mut mstate, node, estate)?;

    // Miscellaneous initialization: create expression context for node.
    seam::exec_assign_expr_context::call(&mut mstate, estate)?;

    // outerPlanState(mstate) = ExecInitNode(outerPlan(node), estate, eflags).
    seam::init_outer_plan::call(&mut mstate, node, estate, eflags)?;

    // Initialize return slot and type. No projection (this node doesn't project).
    seam::init_result_tuple_slot_tl::call(&mut mstate, estate)?;

    // Initialize scan slot and type.
    seam::create_scan_slot_from_outer_plan::call(&mut mstate, estate)?;

    // Set the state machine to lookup the cache.
    mstate.mstatus = MemoStatus::CacheLookup;

    mstate.nkeys = node.numKeys;
    // Build hashkeydesc + tableslot/probeslot + param_exprs/hashfunctions arrays.
    seam::init_hashkeydesc_and_slots::call(&mut mstate, node, estate)?;

    // Just point directly to the plan data (copied into the owned node).
    {
        let mcx = estate.es_query_cxt;
        let mut collations = mcx::vec_with_capacity_in(mcx, node.collations.len())?;
        for c in node.collations.iter() {
            collations.push(*c);
        }
        mstate.collations = collations;
    }

    // eqfuncoids = (Oid *) palloc(nkeys * sizeof(Oid)) — a working buffer used by
    // ExecBuildParamSetEqual and pfree'd before returning. In the owned model it
    // is a charged PgVec<Oid> in the estate's per-query context; it is dropped on
    // every path (success and error) once the eq-expr build consumes it.
    let mcx = estate.es_query_cxt;
    let eqfuncoids = build_eqfuncoids(mcx, &mut mstate, node)?;

    // mstate->cache_eq_expr = ExecBuildParamSetEqual(...).
    seam::build_cache_eq_expr::call(&mut mstate, node, &eqfuncoids)?;
    // pfree(eqfuncoids): the PgVec drops here, releasing its per-query charge.
    drop(eqfuncoids);

    mstate.mem_used = 0;

    // Limit the total memory consumed by the cache to this.
    mstate.mem_limit = seam::get_hash_memory_limit::call()?;

    // A memory context dedicated for the cache. In the owned model the cache is
    // held in the node (no separate arena); we keep the diagnostic name only.
    mstate.table_context_name = Some(mcx::PgString::from_str_in("MemoizeHashTable", mcx)?);

    mstate.last_tuple = None;
    mstate.entry = None;

    // Mark if we can assume the cache entry is completed after the first record.
    mstate.singlerow = node.singlerow;
    // keyparamids: copy of node->keyparamids.
    mstate.keyparamids = match &node.keyparamids {
        Some(b) => Some(mcx::alloc_in(mcx, b.clone_in(mcx)?)?),
        None => None,
    };

    // Record if cache keys should be compared bit-by-bit or via hash equality.
    mstate.binary_mode = node.binary_mode;

    // Zero the statistics counters.
    mstate.stats = MemoizeInstrumentation::default();

    // Delay building of the hash table until executor run.
    mstate.hashtable = None;
    mstate.est_entries = node.est_entries;

    Ok(mstate)
}

/// Build the per-key `eqfuncoids` working buffer (the C `palloc`'d `Oid` array),
/// charged to the estate's per-query context. Mirrors the `ExecInitMemoize` key
/// loop: for each key it looks up the hash functions, installs the left hash
/// function, compiles the parameter expression, and records `get_opcode(hashop)`.
fn build_eqfuncoids<'mcx>(
    mcx: Mcx<'mcx>,
    mstate: &mut MemoizeScanState<'mcx>,
    node: &Memoize<'mcx>,
) -> PgResult<mcx::PgVec<'mcx, Oid>> {
    let nkeys = node.numKeys;
    // palloc(nkeys * sizeof(Oid)): reserve the spine up front (fallible).
    let mut eqfuncoids: mcx::PgVec<'mcx, Oid> =
        mcx::vec_with_capacity_in(mcx, nkeys.max(0) as usize)?;

    for i in 0..nkeys {
        let i = i as usize;
        let hashop = match node.hashOperators.get(i) {
            Some(op) => *op,
            None => return Err(elog_internal("memoize hashOperators index out of range")),
        };

        // get_op_hash_functions(hashop, &left_hashfn, &right_hashfn).
        let (left_hashfn, _right_hashfn) = match seam::get_op_hash_functions::call(hashop)? {
            Some(pair) => pair,
            None => {
                return Err(elog_internal_fmt(alloc::format!(
                    "could not find hash function for hash operator {hashop}"
                )))
            }
        };

        // fmgr_info(left_hashfn, &mstate->hashfunctions[i]).
        seam::fmgr_info_hashfn::call(mstate, i, left_hashfn)?;

        // mstate->param_exprs[i] = ExecInitExpr(list_nth(node->param_exprs, i), ..).
        seam::exec_init_param_expr::call(mstate, node, i)?;

        // eqfuncoids[i] = get_opcode(hashop).
        let opcode = seam::get_opcode::call(hashop)?;
        eqfuncoids.push(opcode);
    }

    Ok(eqfuncoids)
}

/// `ExecEndMemoize(node)` — shut down the node and subnodes.
pub fn ExecEndMemoize<'mcx>(
    node: &mut MemoizeScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // Validate the memory accounting code is correct in assert builds.
    #[cfg(debug_assertions)]
    if let Some(cache) = node.hashtable.as_ref() {
        let mut mem: u64 = 0;
        let mut count: u32 = 0;
        for slot in cache.slots.iter() {
            if let Some(entry) = slot {
                mem += MemoizeScanState::empty_entry_memory_bytes(entry.params.t_len);
                for tuple in entry.tuples.iter() {
                    mem += MemoizeScanState::cache_tuple_bytes(tuple.mintuple.t_len);
                }
                count += 1;
            }
        }
        debug_assert!(count == cache.members);
        debug_assert!(mem == node.mem_used);
    }

    // In a parallel worker, copy stats back into shared memory for EXPLAIN.
    if node.shared_info.is_some() && parallel::is_parallel_worker::call() {
        // Make mem_peak available for EXPLAIN.
        if node.stats.mem_peak == 0 {
            node.stats.mem_peak = node.mem_used;
        }

        let worker_number = parallel::parallel_worker_number::call();
        let stats = node.stats;
        let shared = match node.shared_info.as_mut() {
            Some(shared) => shared,
            None => {
                return Err(elog_internal(
                    "ExecEndMemoize: shared_info present (checked above)",
                ))
            }
        };
        debug_assert!(worker_number <= shared.num_workers);
        let idx = worker_number as usize;
        if idx >= shared.sinstrument.len() {
            return Err(elog_internal("parallel worker number out of range"));
        }
        shared.sinstrument[idx] = stats;
    }

    // Remove the cache context. In the owned model the cache lives in the node;
    // dropping it frees every entry/tuple.
    node.hashtable = None;

    // shut down the subplan.
    seam::exec_end_outer::call(node, estate)?;
    Ok(())
}

/// `ExecReScanMemoize(node)` — rescan the memoize node.
pub fn ExecReScanMemoize<'mcx>(
    node: &mut MemoizeScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // Mark that we must lookup the cache for a new set of parameters.
    node.mstatus = MemoStatus::CacheLookup;

    // nullify pointers used for the last scan.
    node.entry = None;
    node.last_tuple = None;

    // if chgParam of subnode is not null then plan will be re-scanned by first
    // ExecProcNode.
    if outer_chgparam_is_empty(node) {
        seam::exec_rescan_outer::call(node, estate)?;
    }

    // Purge the entire cache if a parameter changed that is not part of the key.
    if chgparam_has_non_key_difference(node) {
        cache_purge_all(node)?;
    }
    Ok(())
}

/// `outerPlan->chgParam == NULL` — whether the outer child's changed-param set
/// is empty (governs whether `ExecReScanMemoize` rescans it directly). The outer
/// PlanState is the node's `lefttree`; this reads its `chgParam` head field.
fn outer_chgparam_is_empty(node: &MemoizeScanState) -> bool {
    match node.ss.ps.lefttree.as_deref() {
        Some(outer) => outer.ps_head().chgParam.is_none(),
        // No outer child: treat as empty (C never reaches rescan in that case).
        None => true,
    }
}

/// `bms_nonempty_difference(outerPlan->chgParam, node->keyparamids)` — true when
/// a changed param that is not part of the cache key forces a full cache purge.
fn chgparam_has_non_key_difference(node: &MemoizeScanState) -> bool {
    let outer_chg = node
        .ss
        .ps
        .lefttree
        .as_deref()
        .and_then(|outer| outer.ps_head().chgParam.as_deref());
    let keyparamids = node.keyparamids.as_deref();
    backend_nodes_core_seams::bms_nonempty_difference::call(outer_chg, keyparamids)
}

/// `ExecEstimateCacheEntryOverheadBytes(ntuples)` — planner helper estimating
/// the memory required to store a single cache entry. Delegates to the
/// vocabulary-crate definition (`sizeof(MemoizeEntry) + sizeof(MemoizeKey) +
/// sizeof(MemoizeTuple) * ntuples`).
pub fn ExecEstimateCacheEntryOverheadBytes(ntuples: f64) -> f64 {
    types_nodes::nodememoize::exec_estimate_cache_entry_overhead_bytes(ntuples)
}

// ===========================================================================
// Parallel Query Support (nodeMemoize.c).
// ===========================================================================

/// `ExecMemoizeEstimate(node, pcxt)` — estimate DSM space for memoize stats.
fn exec_memoize_estimate(node: PlanStateHandle, pcxt: ParallelContextHandle) -> PgResult<()> {
    // don't need this if not instrumenting or no workers.
    if !seam::memoize_instrument_present::call(node)? {
        return Ok(());
    }
    let nworkers = parallel::pcxt_nworkers::call(pcxt);
    if nworkers == 0 {
        return Ok(());
    }

    // size = mul_size(nworkers, sizeof(MemoizeInstrumentation));
    // size = add_size(size, offsetof(SharedMemoizeInfo, sinstrument));
    let size = mul_size(nworkers as Size, SIZEOF_MEMOIZE_INSTRUMENTATION)?;
    let size = add_size(size, OFFSETOF_SHARED_MEMOIZE_INFO_SINSTRUMENT)?;

    let estimator = parallel::pcxt_estimator::call(pcxt);
    parallel::shm_toc_estimate_chunk::call(estimator, size);
    parallel::shm_toc_estimate_keys::call(estimator, 1);
    Ok(())
}

/// `ExecMemoizeInitializeDSM(node, pcxt)` — initialize DSM space for stats.
fn exec_memoize_initialize_dsm(node: PlanStateHandle, pcxt: ParallelContextHandle) -> PgResult<()> {
    // don't need this if not instrumenting or no workers.
    if !seam::memoize_instrument_present::call(node)? {
        return Ok(());
    }
    let nworkers = parallel::pcxt_nworkers::call(pcxt);
    if nworkers == 0 {
        return Ok(());
    }

    let plan_node_id = sup::plan_node_id::call(node);

    // node->shared_info = shm_toc_allocate(pcxt->toc, size); zero-fill;
    // num_workers = pcxt->nworkers; shm_toc_insert(pcxt->toc, plan_node_id, ...).
    let size = add_size(
        OFFSETOF_SHARED_MEMOIZE_INFO_SINSTRUMENT,
        mul_size(nworkers as Size, SIZEOF_MEMOIZE_INSTRUMENTATION)?,
    )?;
    let toc = parallel::pcxt_toc::call(pcxt);
    let chunk = parallel::shm_toc_allocate::call(toc, size);
    parallel::shm_toc_insert::call(toc, plan_node_id as u64, chunk);

    // Keep the node's own mirror in step (the owned canonical copy of the
    // per-worker stats the retrieve path reads back).
    let mut sinstrument: Vec<MemoizeInstrumentation> = Vec::new();
    sinstrument
        .try_reserve(nworkers.max(0) as usize)
        .map_err(|_| out_of_memory("SharedMemoizeInfo"))?;
    sinstrument.resize(nworkers.max(0) as usize, MemoizeInstrumentation::default());
    seam::set_memoize_shared_info::call(
        node,
        SharedMemoizeInfo {
            num_workers: nworkers,
            sinstrument,
        },
    )?;
    Ok(())
}

/// `ExecMemoizeInitializeWorker(node, pwcxt)` — attach the worker to DSM stats.
fn exec_memoize_initialize_worker(
    node: PlanStateHandle,
    pwcxt: ParallelWorkerContextHandle,
) -> PgResult<()> {
    let plan_node_id = sup::plan_node_id::call(node);
    // node->shared_info = shm_toc_lookup(pwcxt->toc, plan_node_id, true).
    let toc = parallel::pwcxt_toc::call(pwcxt);
    if let Some(chunk) = parallel::shm_toc_lookup::call(toc, plan_node_id as u64, true) {
        // Attach the node's shared_info mirror over the existing DSM chunk
        // (the chunk's num_workers header sizes the mirror).
        seam::memoize_attach_shared_info::call(node, chunk)?;
    }
    Ok(())
}

/// `ExecMemoizeRetrieveInstrumentation(node)` — copy DSM stats into local memory.
fn exec_memoize_retrieve_instrumentation(node: PlanStateHandle) -> PgResult<()> {
    // si = palloc(size); memcpy(si, node->shared_info, size); node->shared_info
    // = si. With the owned mirror the shared_info already holds the worker
    // counts; there is nothing to copy out of a raw DSM pointer here. If there
    // is no shared info, there is nothing to retrieve.
    if !seam::memoize_shared_info_present::call(node)? {
        return Ok(());
    }
    Ok(())
}

// ===========================================================================
// Owned-cache primitives (the replacement for simplehash + dlist).
// ===========================================================================

/// Unwrap the `Option<slot_id>` from `cache_lookup` where the C asserts it is
/// non-NULL on the `found` path.
fn slot_or_err(entry: Option<usize>) -> PgResult<usize> {
    entry.ok_or_else(|| elog_internal("ExecMemoize: entry is NULL"))
}

/// Borrow the owned cache, erroring if it has not been built.
fn cache_ref<'a, 'mcx>(mstate: &'a MemoizeScanState<'mcx>) -> PgResult<&'a MemoizeCache<'mcx>> {
    mstate
        .hashtable
        .as_ref()
        .ok_or_else(|| elog_internal("memoize cache not built"))
}

/// Mutably borrow the owned cache, erroring if it has not been built.
fn cache_mut<'a, 'mcx>(
    mstate: &'a mut MemoizeScanState<'mcx>,
) -> PgResult<&'a mut MemoizeCache<'mcx>> {
    mstate
        .hashtable
        .as_mut()
        .ok_or_else(|| elog_internal("memoize cache not built"))
}

/// Borrow a live entry at `slot_id`.
fn entry_at<'a, 'mcx>(cache: &'a MemoizeCache<'mcx>, slot_id: usize) -> PgResult<&'a CacheEntry<'mcx>> {
    cache
        .slots
        .get(slot_id)
        .and_then(|s| s.as_ref())
        .ok_or_else(|| elog_internal("could not find memoization table entry"))
}

/// Mutably borrow a live entry at `slot_id`.
fn entry_at_mut<'a, 'mcx>(
    cache: &'a mut MemoizeCache<'mcx>,
    slot_id: usize,
) -> PgResult<&'a mut CacheEntry<'mcx>> {
    cache
        .slots
        .get_mut(slot_id)
        .and_then(|s| s.as_mut())
        .ok_or_else(|| elog_internal("could not find memoization table entry"))
}

/// Whether `slot_id` currently holds a live entry.
fn slot_is_live(mstate: &MemoizeScanState, slot_id: usize) -> bool {
    matches!(
        mstate.hashtable.as_ref().and_then(|c| c.slots.get(slot_id)),
        Some(Some(_))
    )
}

/// Clone the params of the entry at `slot_id` into the estate's per-query mcx.
fn entry_params_clone<'mcx>(
    mstate: &MemoizeScanState<'mcx>,
    slot_id: usize,
    estate: &EStateData<'mcx>,
) -> PgResult<MinimalTupleData<'mcx>> {
    let mcx = estate.es_query_cxt;
    entry_at(cache_ref(mstate)?, slot_id)?.params.clone_in(mcx)
}

/// Whether the entry at `slot_id` is `complete`.
fn entry_is_complete(mstate: &MemoizeScanState, slot_id: usize) -> PgResult<bool> {
    Ok(entry_at(cache_ref(mstate)?, slot_id)?.complete)
}

/// Set the `complete` flag of the entry at `slot_id`.
fn entry_set_complete(mstate: &mut MemoizeScanState, slot_id: usize, value: bool) -> PgResult<()> {
    entry_at_mut(cache_mut(mstate)?, slot_id)?.complete = value;
    Ok(())
}

/// Whether the entry at `slot_id` has any cached tuples (`tuplehead != NULL`).
fn entry_has_tuples(mstate: &MemoizeScanState, slot_id: usize) -> PgResult<bool> {
    Ok(!entry_at(cache_ref(mstate)?, slot_id)?.tuples.is_empty())
}

/// The number of cached tuples in the entry at `slot_id`.
fn entry_tuple_count(mstate: &MemoizeScanState, slot_id: usize) -> PgResult<usize> {
    Ok(entry_at(cache_ref(mstate)?, slot_id)?.tuples.len())
}

/// Clone the `index`-th cached tuple of the entry at `slot_id` into the estate's
/// per-query mcx.
fn entry_tuple_clone<'mcx>(
    mstate: &MemoizeScanState<'mcx>,
    slot_id: usize,
    index: usize,
    estate: &EStateData<'mcx>,
) -> PgResult<MinimalTupleData<'mcx>> {
    let mcx = estate.es_query_cxt;
    let entry = entry_at(cache_ref(mstate)?, slot_id)?;
    let tuple = entry
        .tuples
        .get(index)
        .ok_or_else(|| elog_internal("memoize tuple index out of range"))?;
    tuple.mintuple.clone_in(mcx)
}

/// Find a live entry whose cached hash equals `hash` and whose key matches the
/// current probe slot (via the equality seam). Mirrors `memoize_lookup` over the
/// collision chain.
fn cache_find_matching<'mcx>(
    mstate: &mut MemoizeScanState<'mcx>,
    hash: u32,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<usize>> {
    // Snapshot the candidate slot ids for this hash (copy to release the borrow
    // before calling the equality seam, which needs &mut mstate). This small
    // working buffer mirrors the C collision-chain walk.
    let candidates: Vec<usize> = match mstate.hashtable.as_ref().and_then(|c| c.index.get(&hash)) {
        Some(ids) => {
            let mut v = Vec::new();
            v.try_reserve(ids.len())
                .map_err(|_| out_of_memory("MemoizeCandidates"))?;
            v.extend_from_slice(ids);
            v
        }
        None => return Ok(None),
    };

    for slot_id in candidates {
        // The candidate must still be live (it always is, but guard anyway).
        if !slot_is_live(mstate, slot_id) {
            continue;
        }
        let params = entry_params_clone(mstate, slot_id, estate)?;
        if seam::probe_equals_params::call(mstate, &params, estate)? {
            return Ok(Some(slot_id));
        }
    }
    Ok(None)
}

/// Insert `entry` into the cache (allocating or reusing a slot), index it under
/// `hash`, push it onto the MRU end of the LRU list, and bump `members`. Returns
/// the new slot id. Mirrors `memoize_insert` + the C `dlist_push_tail`.
fn cache_insert_entry<'mcx>(
    cache: &mut MemoizeCache<'mcx>,
    mut entry: CacheEntry<'mcx>,
    hash: u32,
) -> PgResult<usize> {
    // Keep the entry's cached hash and its index key in lock-step so
    // cache_delete_slot (which reads `entry.hash`) always finds the right bucket.
    entry.hash = hash;
    let slot_id = match cache.free_slots.pop() {
        Some(id) => {
            cache.slots[id] = Some(entry);
            id
        }
        None => {
            let id = cache.slots.len();
            cache
                .slots
                .try_reserve(1)
                .map_err(|_| out_of_memory("MemoizeHashTable"))?;
            cache.slots.push(Some(entry));
            id
        }
    };

    let chain = cache.index.entry(hash).or_default();
    chain
        .try_reserve(1)
        .map_err(|_| out_of_memory("MemoizeHashTable"))?;
    chain.push(slot_id);
    cache.lru.push_back(slot_id);
    cache.members += 1;
    Ok(slot_id)
}

/// Remove the entry at `slot_id`: unlink it from the LRU queue and the hash
/// index, drop the entry (freeing its key/params/tuples), recycle the slot, and
/// decrement `members`. Mirrors `memoize_delete_item` + `dlist_delete` + the
/// `pfree(key->params); pfree(key)` of `remove_cache_entry`.
fn cache_delete_slot(cache: &mut MemoizeCache, slot_id: usize) {
    let hash = match cache.slots.get(slot_id).and_then(|s| s.as_ref()) {
        Some(e) => e.hash,
        None => return,
    };

    // Unlink from the hash index collision chain.
    if let Some(ids) = cache.index.get_mut(&hash) {
        ids.retain(|&id| id != slot_id);
        if ids.is_empty() {
            cache.index.remove(&hash);
        }
    }

    // Unlink from the LRU list.
    if let Some(pos) = cache.lru.iter().position(|&id| id == slot_id) {
        cache.lru.remove(pos);
    }

    // Free the entry and recycle the slot.
    cache.slots[slot_id] = None;
    cache.free_slots.push(slot_id);
    cache.members -= 1;
}

/// Move the entry at `slot_id` to the MRU (back) end of the LRU queue. Mirrors
/// `dlist_move_tail`.
fn lru_move_to_back(cache: &mut MemoizeCache, slot_id: usize) {
    if let Some(pos) = cache.lru.iter().position(|&id| id == slot_id) {
        // Fast path: already at the tail.
        if pos + 1 == cache.lru.len() {
            return;
        }
        cache.lru.remove(pos);
        cache.lru.push_back(slot_id);
    }
}

// ===========================================================================
// Error helpers.
// ===========================================================================

/// `elog(ERROR, msg)` — internal error (`ERRCODE_INTERNAL_ERROR`).
fn elog_internal(message: &'static str) -> PgError {
    PgError::error(message).with_sqlstate(ERRCODE_INTERNAL_ERROR)
}

/// `elog(ERROR, fmt, ...)` — internal error with a runtime-formatted message.
fn elog_internal_fmt(message: alloc::string::String) -> PgError {
    PgError::error(message).with_sqlstate(ERRCODE_INTERNAL_ERROR)
}

/// `errcode(ERRCODE_OUT_OF_MEMORY)` for an allocation-safety failure.
fn out_of_memory(what: &str) -> PgError {
    PgError::error(alloc::format!("out of memory ({what})")).with_sqlstate(ERRCODE_OUT_OF_MEMORY)
}

// ===========================================================================
// Seam installation.
// ===========================================================================

/// Install every seam declared in `backend-executor-nodeMemoize-seams` that this
/// crate owns: the four parallel-executor entry points.
///
/// The downward run-time / init seams (slot/expr substrate, simplehash leaves,
/// catalog lookups, fmgr, outer-child dispatch, memory budget) are owned by the
/// subsystems below the node layer and are installed by those crates when they
/// land — this node calls them and they panic loudly until then.
///
/// The memoize-specific live-node accessors (`memoize_instrument_present`,
/// `memoize_shared_info_present`, `set_memoize_shared_info`,
/// `memoize_finalize_worker_stats`) resolve a `PlanStateHandle` to the concrete
/// `MemoizeScanState` — owned by whoever manages the live PlanState tree under
/// parallel execution, installed by that owner.
pub fn init_seams() {
    seam::exec_memoize_estimate::set(exec_memoize_estimate);
    seam::exec_memoize_initialize_dsm::set(exec_memoize_initialize_dsm);
    seam::exec_memoize_initialize_worker::set(exec_memoize_initialize_worker);
    seam::exec_memoize_retrieve_instrumentation::set(exec_memoize_retrieve_instrumentation);
}
