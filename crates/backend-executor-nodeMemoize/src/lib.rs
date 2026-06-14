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
//! installed `PlanState.ExecProcNode` (via the `execProcnode` owner seam
//! `exec_proc_node`). The subsystems below the node layer — the expression engine, the
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
use types_nodes::execnodes::{EStateData, ScanStateData, SlotId};
use types_nodes::nodememoize::{
    CacheEntry, CachedTuple, MemoStatus, Memoize, MemoizeCache, MemoizeInstrumentation,
    MemoizeKeyAttr, MemoizeScanState,
};
use types_nodes::TupleSlotKind;
use types_tuple::heaptuple::MinimalTupleData;
// Datum-unification migration target: the canonical unified value enum. The
// binary-mode hash/equality leaves consume it by reference (`datum_image_*_v`).
// The bare slot words deformed through the still-unmigrated execTuples /
// execExpr slot seams arrive as `types_datum::Datum`; at that ABI edge they are
// the by-value scalar word, wrapped as `Datum::ByVal` for the `_v` contract.
use types_tuple::backend_access_common_heaptuple::Datum as DatumV;

use backend_access_transam_parallel_seams as parallel;
use backend_executor_nodeMemoize_seams as seam;
use backend_executor_execParallel_support_seams as sup;
use types_execparallel::{ParallelContextHandle, ParallelWorkerContextHandle, PlanStateHandle};

// Owner `-seams` crates this node calls outward through, with the node-side
// marshaling living in this crate.
use backend_tcop_postgres_seams as tcop_postgres;
use backend_utils_cache_lsyscache_seams as lsyscache;
use backend_utils_adt_datum_seams as datum;
use backend_utils_fmgr_fmgr_seams as fmgr;
use backend_executor_nodeHash_seams as nodeHash;
use backend_executor_execExpr_seams as execExpr;
use backend_executor_execProcnode_seams as execProcnode;
use backend_executor_execUtils_seams as execUtils;
use backend_executor_execTuples_seams as execTuples;
use backend_executor_execAmi_seams as execAmi;

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

/// `pg_rotate_left32(word, n)` (pg_bitutils.h).
#[inline]
fn pg_rotate_left32(word: u32, n: u32) -> u32 {
    (word << n) | (word >> (32 - n))
}

/// `murmurhash32(data)` (hashfn.h) — simple inline 32-bit finalizer.
#[inline]
fn murmurhash32(data: u32) -> u32 {
    let mut h = data;
    h ^= h >> 16;
    h = h.wrapping_mul(0x85eb_ca6b);
    h ^= h >> 13;
    h = h.wrapping_mul(0xc2b2_ae35);
    h ^= h >> 16;
    h
}

// ===========================================================================
// nodeMemoize.c static helpers, ported against the owned cache.
// ===========================================================================

/// `prepare_probe_slot(mstate, key)` — populate `mstate`'s probe slot
/// (`probe_values`/`probe_isnull`) with the lookup key. When `key` is `None`,
/// evaluate `mstate->param_exprs` against the current scan parameters (the
/// `ExecEvalExpr` leaf, in the per-tuple context); when `key` is `Some(params)`,
/// deform the cached entry's `params` (the `slot_getallattrs` leaf) and copy its
/// values/nulls into the probe slot. This control flow is nodeMemoize.c's own.
fn prepare_probe_slot<'mcx>(
    mstate: &mut MemoizeScanState<'mcx>,
    key: Option<&MinimalTupleData<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let num_keys = mstate.nkeys as usize;

    // ExecClearTuple(pslot): reset the probe slot's owned values/nulls.
    mstate.probe_values.clear();
    mstate.probe_isnull.clear();

    match key {
        None => {
            // oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);
            // for (i) pslot->tts_values[i] = ExecEvalExpr(param_exprs[i], econtext,
            //                                             &pslot->tts_isnull[i]);
            // (the per-tuple context switch is handled inside the eval leaf, which
            // evaluates in the node's ps_ExprContext->ecxt_per_tuple_memory.)
            // econtext = mstate->ss.ps.ps_ExprContext; oldcontext =
            // MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory).
            let econtext = mstate
                .ss
                .ps
                .ps_ExprContext
                .ok_or_else(|| elog_internal("Memoize node has no ps_ExprContext"))?;
            for i in 0..num_keys {
                // pslot->tts_values[i] = ExecEvalExpr(mstate->param_exprs[i],
                //                                     econtext, &pslot->tts_isnull[i]);
                let state = mstate.param_exprs[i].as_mut();
                let (value, isnull) =
                    execExpr::exec_eval_expr_switch_context::call(state, econtext, estate)?;
                // The eval leaf hands back a bare scalar word at the
                // `types_datum::Datum` ABI edge; it crosses into the canonical
                // value type's by-value arm (`pslot->tts_values[i] = ...`).
                mstate.probe_values.push(DatumV::ByVal(value));
                mstate.probe_isnull.push(isnull);
            }
        }
        Some(params) => {
            // ExecStoreMinimalTuple(key->params, tslot, false); slot_getallattrs(tslot);
            // memcpy(pslot->tts_values, tslot->tts_values, sizeof(Datum) * numKeys);
            // memcpy(pslot->tts_isnull, tslot->tts_isnull, sizeof(bool) * numKeys);
            let (values, isnull) = deform_key_params(mstate, params, num_keys, estate)?;
            // tableslot also holds the deformed values (used by MemoizeHash_equal
            // in non-binary mode via the cache_eq_expr ecxt_innertuple).
            mstate.table_values.clear();
            mstate.table_isnull.clear();
            for i in 0..num_keys {
                // The deformed slot words cross into the canonical value type's
                // by-value arm (`memcpy(pslot->tts_values, tslot->tts_values)`).
                mstate.table_values.push(DatumV::ByVal(values[i]));
                mstate.table_isnull.push(isnull[i]);
                mstate.probe_values.push(DatumV::ByVal(values[i]));
                mstate.probe_isnull.push(isnull[i]);
            }
        }
    }

    // ExecStoreVirtualTuple(pslot): the probe slot now holds num_keys virtual
    // attributes (the owned values/nulls vectors are the materialized slot).
    Ok(())
}

/// `MemoizeHash_hash(tb, NULL)` — hash the current probe slot. The probe slot
/// must already have been populated by [`prepare_probe_slot`]. In binary mode
/// each non-null key is `datum_image_hash`ed; otherwise each key's hash function
/// is invoked via `FunctionCall1Coll`. Successive keys are combined by rotating
/// left one bit and XORing; the accumulator is finalized with `murmurhash32`.
fn memoize_hash_hash<'mcx>(mstate: &mut MemoizeScanState<'mcx>) -> PgResult<u32> {
    let numkeys = mstate.nkeys as usize;
    let mut hashkey: u32 = 0;

    // oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory): the
    // datum-image hash / fmgr leaves run their transient allocations in the
    // node's per-tuple context; the leaves perform that switch themselves.

    if mstate.binary_mode {
        for i in 0..numkeys {
            // combine successive hashkeys by rotating
            hashkey = pg_rotate_left32(hashkey, 1);

            if !mstate.probe_isnull[i] {
                // treat nulls as having hash key 0
                let attr = mstate.key_attrs[i];
                // The probe value is the canonical unified value type; the
                // binary-mode hash leaf consumes it by reference.
                let value = &mstate.probe_values[i];
                let hkey = datum::datum_image_hash_v::call(value, attr.attbyval, attr.attlen)?;
                hashkey ^= hkey;
            }
        }
    } else {
        for i in 0..numkeys {
            // combine successive hashkeys by rotating
            hashkey = pg_rotate_left32(hashkey, 1);

            if !mstate.probe_isnull[i] {
                // hkey = DatumGetUInt32(FunctionCall1Coll(&hashfunctions[i],
                //                          collations[i], pslot->tts_values[i]));
                // The owned `FmgrInfo` only carries `fn_oid`; read it in-crate and
                // call the fmgr owner's leaf, then apply DatumGetUInt32 in-crate.
                let fn_oid = mstate.hashfunctions[i].fn_oid;
                let collation = mstate.collations[i];
                // The fmgr leaf takes a bare scalar word; the probe value is the
                // canonical unified value type, so unwrap its by-value arm (a
                // hash key column is always pass-by-value-or-pointer scalar — a C
                // `tts_values[i]` word).
                let value = byval_word(&mstate.probe_values[i]);
                let result = fmgr::function_call1_coll::call(fn_oid, collation, value)?;
                let hkey = result.as_u32(); // DatumGetUInt32
                hashkey ^= hkey;
            }
        }
    }

    Ok(murmurhash32(hashkey))
}

/// `MemoizeHash_equal(tb, key1, NULL)` — confirm the cached entry whose key is
/// `params` matches the current probe slot. The probe slot must already have
/// been populated. In binary mode this deforms `params` into the table slot and
/// compares each attribute with `datum_image_eq` (mismatched null-ness or datum
/// breaks the match); otherwise it runs `ExecQual(cache_eq_expr)` with the table
/// slot as inner and the probe slot as outer tuple. This decision logic is
/// nodeMemoize.c's own.
fn memoize_hash_equal<'mcx>(
    mstate: &mut MemoizeScanState<'mcx>,
    params: &MinimalTupleData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    // probeslot should have already been prepared by prepare_probe_slot().
    // ExecStoreMinimalTuple(key1->params, tslot, false): deform params into the
    // owned table slot.
    let numkeys = mstate.nkeys as usize;
    {
        let (values, isnull) = deform_key_params(mstate, params, numkeys, estate)?;
        mstate.table_values.clear();
        mstate.table_isnull.clear();
        for i in 0..numkeys {
            // The deformed slot word crosses into the canonical value type's
            // by-value arm.
            mstate.table_values.push(DatumV::ByVal(values[i]));
            mstate.table_isnull.push(isnull[i]);
        }
    }

    if mstate.binary_mode {
        // oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);
        // slot_getallattrs(tslot); slot_getallattrs(pslot): both slots are
        // already fully deformed in the owned model.
        let mut is_match = true;

        for i in 0..numkeys {
            if mstate.table_isnull[i] != mstate.probe_isnull[i] {
                is_match = false;
                break;
            }

            // both NULL? they're equal
            if mstate.table_isnull[i] {
                continue;
            }

            // perform binary comparison on the two datums
            let attr = mstate.key_attrs[i];
            // Both operands are the canonical unified value type held in the
            // table/probe slots; the binary-mode equality leaf consumes them by
            // reference.
            let table_value = &mstate.table_values[i];
            let probe_value = &mstate.probe_values[i];
            if !datum::datum_image_eq_v::call(
                table_value,
                probe_value,
                attr.attbyval,
                attr.attlen,
            )? {
                is_match = false;
                break;
            }
        }

        Ok(is_match)
    } else {
        // econtext->ecxt_innertuple = tslot; econtext->ecxt_outertuple = pslot;
        // return ExecQual(mstate->cache_eq_expr, econtext);
        //
        // The table slot (the cached entry's deformed `params`) is the inner
        // tuple and the probe slot is the outer tuple; the owned model holds
        // those deformed values in `table_values`/`probe_values`. The expression
        // engine (the owner leaf) reads them through the node's per-node
        // ExprContext, which the cache-eq expression was compiled against.
        let econtext = mstate
            .ss
            .ps
            .ps_ExprContext
            .ok_or_else(|| elog_internal("Memoize node has no ps_ExprContext"))?;
        let cache_eq_expr = mstate
            .cache_eq_expr
            .as_mut()
            .ok_or_else(|| elog_internal("Memoize node has no cache_eq_expr"))?
            .as_mut();
        execExpr::exec_qual::call(cache_eq_expr, econtext, estate)
    }
}

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
        prepare_probe_slot(mstate, Some(&params), estate)?;

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
    prepare_probe_slot(mstate, None, estate)?;

    // Hash the probe slot (mirrors memoize_insert -> MemoizeHash_hash).
    let hash = memoize_hash_hash(mstate)?;

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
    let params = copy_probe_slot_minimal_tuple(mstate, mcx, estate)?;
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
/// `ps_ResultTupleSlot` (via the `execTuples` owner seam
/// `exec_force_store_minimal_tuple`), and `Ok(false)` when the scan is exhausted
/// (the C `return NULL`; the result slot is cleared via the `execTuples` owner
/// seam `exec_clear_tuple`).
pub fn ExecMemoize<'mcx>(
    node: &mut MemoizeScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    tcop_postgres::check_for_interrupts::call()?;

    // Reset per-tuple memory context to free prior cycle's eval storage.
    //   ResetExprContext(node->ss.ps.ps_ExprContext);
    reset_expr_context(node, estate)?;

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
                    store_result_minimal_tuple(node, &tuple, estate)?;
                    return Ok(true);
                }

                // The cache entry is void of any tuples.
                node.last_tuple = None;
                node.mstatus = MemoStatus::EndOfScan;
                clear_result_slot(node, estate)?;
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
            let outerslot = exec_proc_outer(node, mcx, estate)?;
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
                    clear_result_slot(node, estate)?;
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
            store_result_minimal_tuple(node, &outerslot, estate)?;
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
                clear_result_slot(node, estate)?;
                return Ok(false);
            }

            node.last_tuple = Some(next_index);

            let tuple = entry_tuple_clone(node, slot_id, next_index, estate)?;
            store_result_minimal_tuple(node, &tuple, estate)?;
            Ok(true)
        }

        MemoStatus::FillingCache => {
            // entry should already have been set by MEMO_CACHE_LOOKUP.
            let slot_id = node
                .entry
                .ok_or_else(|| elog_internal("MEMO_FILLING_CACHE with no current entry"))?;

            // Populate the cache with the current scan tuples.
            let mcx = estate.es_query_cxt;
            let outerslot = exec_proc_outer(node, mcx, estate)?;
            let outerslot = match outerslot {
                Some(t) => t,
                None => {
                    // No more tuples. Mark it as complete.
                    entry_set_complete(node, slot_id, true)?;
                    node.mstatus = MemoStatus::EndOfScan;
                    clear_result_slot(node, estate)?;
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

            store_result_minimal_tuple(node, &outerslot, estate)?;
            Ok(true)
        }

        MemoStatus::CacheBypassMode => {
            // Continue reading tuples without caching until the next rescan.
            let mcx = estate.es_query_cxt;
            let outerslot = exec_proc_outer(node, mcx, estate)?;
            match outerslot {
                Some(t) => {
                    store_result_minimal_tuple(node, &t, estate)?;
                    Ok(true)
                }
                None => {
                    node.mstatus = MemoStatus::EndOfScan;
                    clear_result_slot(node, estate)?;
                    Ok(false)
                }
            }
        }

        MemoStatus::EndOfScan => {
            // We've already returned NULL for this scan; just in case.
            clear_result_slot(node, estate)?;
            Ok(false)
        }
    }
}

/// `ExecInitMemoize(node, estate, eflags)` — initialize the node and subnodes.
pub fn ExecInitMemoize<'mcx>(
    plan_node: &'mcx types_nodes::nodes::Node<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
) -> PgResult<alloc::boxed::Box<MemoizeScanState<'mcx>>> {
    // MemoizeState *mstate = makeNode(MemoizeState);
    let node: &'mcx Memoize<'mcx> = match plan_node {
        types_nodes::nodes::Node::Memoize(m) => m,
        other => panic!("castNode(Memoize, node) failed: {other:?}"),
    };

    let mut mstate = make_memoize_state(estate)?;

    // check for unsupported flags.
    debug_assert!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK) == 0);

    // mstate->ss.ps.plan = (Plan *) node; ->state = estate;
    // ->ExecProcNode = ExecMemoize.
    init_plan_state_links(&mut mstate, plan_node, estate)?;

    // Miscellaneous initialization: create expression context for node.
    //   ExecAssignExprContext(estate, &mstate->ss.ps);
    execUtils::exec_assign_expr_context::call(estate, &mut mstate.ss.ps)?;

    // outerPlanState(mstate) = ExecInitNode(outerPlan(node), estate, eflags).
    init_outer_plan(&mut mstate, node, estate, eflags)?;

    // Initialize return slot and type. No projection (this node doesn't project).
    //   ExecInitResultTupleSlotTL(&mstate->ss.ps, &TTSOpsMinimalTuple);
    //   mstate->ss.ps.ps_ProjInfo = NULL;
    execTuples::exec_init_result_tuple_slot_tl::call(
        &mut mstate.ss.ps,
        estate,
        types_nodes::TupleSlotKind::MinimalTuple,
    )?;
    mstate.ss.ps.ps_ProjInfo = None;

    // Initialize scan slot and type.
    //   ExecCreateScanSlotFromOuterPlan(estate, &mstate->ss, &TTSOpsMinimalTuple);
    execUtils::exec_create_scan_slot_from_outer_plan::call(
        estate,
        &mut mstate.ss,
        types_nodes::TupleSlotKind::MinimalTuple,
    )?;

    // Set the state machine to lookup the cache.
    mstate.mstatus = MemoStatus::CacheLookup;

    mstate.nkeys = node.numKeys;
    // Build hashkeydesc + tableslot/probeslot + param_exprs/hashfunctions arrays.
    init_hashkeydesc_and_slots(&mut mstate, node, estate)?;

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
    let eqfuncoids = build_eqfuncoids(&mut mstate, node, estate)?;

    // mstate->cache_eq_expr = ExecBuildParamSetEqual(...).
    build_cache_eq_expr(&mut mstate, node, &eqfuncoids, estate)?;
    // pfree(eqfuncoids): the PgVec drops here, releasing its per-query charge.
    drop(eqfuncoids);

    mstate.mem_used = 0;

    // Limit the total memory consumed by the cache to this.
    mstate.mem_limit = nodeHash::get_hash_memory_limit::call()?;

    // A memory context dedicated for the cache. In the owned model the cache is
    // held in the node (no separate arena); we keep the diagnostic name only.
    let mcx = estate.es_query_cxt;
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
    mstate: &mut MemoizeScanState<'mcx>,
    node: &Memoize<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<mcx::PgVec<'mcx, Oid>> {
    let mcx = estate.es_query_cxt;
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
        let (left_hashfn, _right_hashfn) = match lsyscache::get_op_hash_functions::call(hashop)? {
            Some(pair) => pair,
            None => {
                return Err(elog_internal_fmt(alloc::format!(
                    "could not find hash function for hash operator {hashop}"
                )))
            }
        };

        // fmgr_info(left_hashfn, &mstate->hashfunctions[i]). The owned `FmgrInfo`
        // only carries the resolved `fn_oid`, so this is a trivial field write
        // (the real fmgr lookup is deferred to the call site in MemoizeHash_hash).
        mstate.hashfunctions[i].fn_oid = left_hashfn;

        // mstate->param_exprs[i] = ExecInitExpr(list_nth(node->param_exprs, i),
        //                                       (PlanState *) mstate).
        let expr = node
            .param_exprs
            .get(i)
            .ok_or_else(|| elog_internal("memoize param_exprs index out of range"))?;
        let state = execExpr::exec_init_expr::call(expr, &mut mstate.ss.ps, estate)?;
        mstate.param_exprs[i] = state;

        // eqfuncoids[i] = get_opcode(hashop).
        let opcode = lsyscache::get_opcode::call(hashop)?;
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
    exec_end_outer(node, estate)?;
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
        exec_rescan_outer(node, estate)?;
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
///
/// nodeMemoize owns the C control flow (the instrument/nworkers guards, the
/// chunk sizing); the handle-addressed reads of the live `MemoizeState` and the
/// `ParallelContext`/`shm_toc` go through the parallel-executor support seams.
fn exec_memoize_estimate(node: PlanStateHandle, pcxt: ParallelContextHandle) -> PgResult<()> {
    // don't need this if not instrumenting or no workers.
    //   if (!node->ss.ps.instrument || pcxt->nworkers == 0) return;
    if !sup::memoize_instrument_present::call(node) || sup::pcxt_nworkers::call(pcxt) == 0 {
        return Ok(());
    }

    // size = mul_size(nworkers, sizeof(MemoizeInstrumentation));
    // size = add_size(size, offsetof(SharedMemoizeInfo, sinstrument));
    let nworkers = sup::pcxt_nworkers::call(pcxt);
    let size = add_size(
        mul_size(nworkers as Size, SIZEOF_MEMOIZE_INSTRUMENTATION)?,
        OFFSETOF_SHARED_MEMOIZE_INFO_SINSTRUMENT,
    )?;

    //   shm_toc_estimate_chunk(&pcxt->estimator, size);
    //   shm_toc_estimate_keys(&pcxt->estimator, 1);
    sup::pcxt_estimate_chunk::call(pcxt, size)?;
    sup::pcxt_estimate_keys::call(pcxt, 1)?;
    Ok(())
}

/// `ExecMemoizeInitializeDSM(node, pcxt)` — initialize DSM space for stats.
fn exec_memoize_initialize_dsm(node: PlanStateHandle, pcxt: ParallelContextHandle) -> PgResult<()> {
    // don't need this if not instrumenting or no workers.
    //   if (!node->ss.ps.instrument || pcxt->nworkers == 0) return;
    if !sup::memoize_instrument_present::call(node) || sup::pcxt_nworkers::call(pcxt) == 0 {
        return Ok(());
    }

    // size = offsetof(SharedMemoizeInfo, sinstrument)
    //        + pcxt->nworkers * sizeof(MemoizeInstrumentation);
    let nworkers = sup::pcxt_nworkers::call(pcxt);
    let size = add_size(
        OFFSETOF_SHARED_MEMOIZE_INFO_SINSTRUMENT,
        mul_size(nworkers as Size, SIZEOF_MEMOIZE_INSTRUMENTATION)?,
    )?;
    let plan_node_id = sup::plan_node_id::call(node);

    // node->shared_info = shm_toc_allocate(pcxt->toc, size);
    // MemSet(node->shared_info, 0, size);
    // node->shared_info->num_workers = pcxt->nworkers;
    // shm_toc_insert(pcxt->toc, plan_node_id, node->shared_info).
    sup::memoize_initialize_dsm_shared_info::call(node, pcxt, nworkers, plan_node_id, size)
}

/// `ExecMemoizeInitializeWorker(node, pwcxt)` — attach the worker to DSM stats.
fn exec_memoize_initialize_worker(
    node: PlanStateHandle,
    pwcxt: ParallelWorkerContextHandle,
) -> PgResult<()> {
    // node->shared_info = shm_toc_lookup(pwcxt->toc, plan_node_id, true).
    let plan_node_id = sup::plan_node_id::call(node);
    sup::memoize_initialize_worker_shared_info::call(node, pwcxt, plan_node_id)
}

/// `ExecMemoizeRetrieveInstrumentation(node)` — copy DSM stats into local memory.
fn exec_memoize_retrieve_instrumentation(node: PlanStateHandle) -> PgResult<()> {
    // SharedMemoizeInfo *si;
    // if (node->shared_info == NULL) return;
    if !sup::memoize_shared_info_present::call(node) {
        return Ok(());
    }

    // size = offsetof(SharedMemoizeInfo, sinstrument)
    //        + node->shared_info->num_workers * sizeof(MemoizeInstrumentation);
    // si = palloc(size); memcpy(si, node->shared_info, size); node->shared_info
    // = si — copy the per-worker stats out of the DSM chunk into local memory so
    // they survive the parallel context teardown for EXPLAIN.
    let num_workers = sup::memoize_shared_info_num_workers::call(node);
    let size = add_size(
        OFFSETOF_SHARED_MEMOIZE_INFO_SINSTRUMENT,
        mul_size(num_workers as Size, SIZEOF_MEMOIZE_INSTRUMENTATION)?,
    )?;
    sup::memoize_retrieve_shared_info::call(node, size)
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
        if memoize_hash_equal(mstate, &params, estate)? {
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
// Node-side marshaling over owner-subsystem leaves, and the genuinely
// Memoize-owned operations that have no owner-type leaf matching this port's
// owned-vector data model. These were previously per-call seams in the
// nodeMemoize-seams crate; per the seam-ownership convention they are now
// either thin in-crate wrappers over an owner `-seams` leaf, or in-crate plain
// functions for the Memoize-owned operations.
// ===========================================================================

/// `ResetExprContext(node->ss.ps.ps_ExprContext)` (executor.h) — reset the
/// node's per-tuple memory context. Thin marshaling over the execUtils owner
/// leaf: resolve the node's `ps_ExprContext` id and reset it. The C resets the
/// context unconditionally on every `ExecMemoize` entry; when the node has no
/// ExprContext yet there is nothing to reset.
fn reset_expr_context<'mcx>(
    mstate: &mut MemoizeScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    if let Some(econtext) = mstate.ss.ps.ps_ExprContext {
        execUtils::reset_expr_context::call(estate, econtext)?;
    }
    Ok(())
}

/// `outerslot = ExecProcNode(outerPlanState(node))` then
/// `ExecCopySlotMinimalTuple(outerslot)` into `mcx`. Returns `Some(mintuple)`
/// when a tuple is produced, `None` when `TupIsNull(outerslot)`. Thin marshaling
/// over the execProcnode / execTuples owner leaves and the node's `lefttree`.
fn exec_proc_outer<'mcx>(
    mstate: &mut MemoizeScanState<'mcx>,
    mcx: Mcx<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<MinimalTupleData<'mcx>>> {
    let outer = mstate
        .ss
        .ps
        .lefttree
        .as_deref_mut()
        .ok_or_else(|| elog_internal("Memoize node has no outer plan state"))?;
    let slot_id = match execProcnode::exec_proc_node::call(outer, estate)? {
        Some(id) if !estate.slot(id).is_empty() => id,
        _ => return Ok(None),
    };
    // ExecCopySlotMinimalTuple(outerslot): materialize as a MinimalTuple in mcx.
    let (mtup, _should_free) =
        execTuples::exec_fetch_slot_minimal_tuple::call(mcx, estate.slot_mut(slot_id))?;
    Ok(Some(mtup.clone_in(mcx)?))
}

/// `ExecEndNode(outerPlanState(node))` — shut down the outer child. Thin
/// marshaling over the execProcnode owner leaf and the node's `lefttree`.
fn exec_end_outer<'mcx>(
    mstate: &mut MemoizeScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let outer = mstate
        .ss
        .ps
        .lefttree
        .as_deref_mut()
        .ok_or_else(|| elog_internal("Memoize node has no outer plan state"))?;
    execProcnode::exec_end_node::call(outer, estate)
}

/// `ExecReScan(outerPlan)` — rescan the outer child. Thin marshaling over the
/// execAmi owner leaf and the node's `lefttree`.
fn exec_rescan_outer<'mcx>(
    mstate: &mut MemoizeScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let outer = mstate
        .ss
        .ps
        .lefttree
        .as_deref_mut()
        .ok_or_else(|| elog_internal("Memoize node has no outer plan state"))?;
    execAmi::exec_re_scan::call(outer, estate)
}

/// `ExecStoreMinimalTuple(tuple, node->ss.ps.ps_ResultTupleSlot, false)` —
/// place the given cached/outer minimal tuple into the result slot (the result
/// slot uses `TTSOpsMinimalTuple`, so this is equivalent to the C `ExecCopySlot`
/// from the minimal-tuple outer slot). Thin marshaling over the execTuples owner
/// leaf and the node's `ps_ResultTupleSlot`.
fn store_result_minimal_tuple<'mcx>(
    mstate: &mut MemoizeScanState<'mcx>,
    tuple: &MinimalTupleData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let slot = mstate
        .ss
        .ps
        .ps_ResultTupleSlot
        .ok_or_else(|| elog_internal("Memoize node result slot not initialized"))?;
    let mcx = estate.es_query_cxt;
    let mtup = mcx::alloc_in(mcx, tuple.clone_in(mcx)?)?;
    execTuples::exec_force_store_minimal_tuple::call(slot, mtup, false, estate)
}

/// `ExecClearTuple(node->ss.ps.ps_ResultTupleSlot)` — clear the result slot,
/// mirroring the C return of `NULL` from `ExecMemoize`. Thin marshaling over the
/// execTuples owner leaf and the node's `ps_ResultTupleSlot`.
fn clear_result_slot<'mcx>(
    mstate: &mut MemoizeScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let slot = mstate
        .ss
        .ps
        .ps_ResultTupleSlot
        .ok_or_else(|| elog_internal("Memoize node result slot not initialized"))?;
    execTuples::exec_clear_tuple::call(estate.slot_mut(slot))
}

/// `outerPlanState(mstate) = ExecInitNode(outerPlan(node), estate, eflags)` —
/// initialize the single outer child plan. Thin marshaling over the execProcnode
/// owner leaf and the `Memoize` plan's `lefttree`, storing the resulting
/// PlanState into the node's `lefttree`.
fn init_outer_plan<'mcx>(
    mstate: &mut MemoizeScanState<'mcx>,
    node: &'mcx Memoize<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    let outer_plan = node.plan.lefttree.as_deref();
    mstate.ss.ps.lefttree = execProcnode::exec_init_node::call(mcx, outer_plan, estate, eflags)?;
    Ok(())
}

// --- ExecInitMemoize builders ----------------------------------------------
//
// These mirror the `makeNode(MemoizeState)` / PlanState wiring / hashkeydesc +
// slot setup / `ExecBuildParamSetEqual` / minimal-tuple deform-form of
// `ExecInitMemoize` and `prepare_probe_slot`. The owned model carries the real
// `hashkeydesc` `TupleDesc` and the `tableslot`/`probeslot` as ids in the
// EState slot pool (created by `MakeSingleTupleTableSlot`), so the deform/form
// run through the execTuples owner seams exactly as the C does
// (`ExecStoreMinimalTuple` + `slot_getattr`, virtual store + `ExecCopySlotMinimalTuple`).
// The per-key `attbyval`/`attlen` the binary-mode hash/equal loops need are
// distilled from `hashkeydesc` into `key_attrs` at init time.

/// The `PlanState.ExecProcNode` callback installed by [`ExecInitMemoize`]:
/// `castNode(MemoizeState, pstate)` then run [`ExecMemoize`], returning the
/// result slot's id (the C `return slot`) or `None`.
fn exec_memoize_node<'mcx>(
    pstate: &mut types_nodes::planstate::PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    let node = match pstate {
        types_nodes::planstate::PlanStateNode::Memoize(node) => node,
        other => panic!("castNode(MemoizeState, pstate) failed: {other:?}"),
    };
    if ExecMemoize(node, estate)? {
        Ok(node.ss.ps.ps_ResultTupleSlot)
    } else {
        Ok(None)
    }
}

/// `makeNode(MemoizeState)` — allocate and zero the executor-state node. C
/// `makeNode` palloc0s the state in `CurrentMemoryContext` (the per-query
/// context); here the owned `MemoizeScanState` is boxed with its embedded
/// `ScanState`/`PlanState` heads default-initialized and the owned value
/// vectors empty (sized later by `init_hashkeydesc_and_slots`). Fallible on OOM.
fn make_memoize_state<'mcx>(
    estate: &mut EStateData<'mcx>,
) -> PgResult<alloc::boxed::Box<MemoizeScanState<'mcx>>> {
    let mcx = estate.es_query_cxt;
    let state = MemoizeScanState {
        ss: ScanStateData::default(),
        mstatus: MemoStatus::CacheLookup,
        nkeys: 0,
        hashkeydesc: None,
        key_attrs: mcx::vec_with_capacity_in(mcx, 0)?,
        tableslot: None,
        probeslot: None,
        table_values: mcx::vec_with_capacity_in(mcx, 0)?,
        table_isnull: mcx::vec_with_capacity_in(mcx, 0)?,
        probe_values: mcx::vec_with_capacity_in(mcx, 0)?,
        probe_isnull: mcx::vec_with_capacity_in(mcx, 0)?,
        cache_eq_expr: None,
        param_exprs: mcx::vec_with_capacity_in(mcx, 0)?,
        hashfunctions: mcx::vec_with_capacity_in(mcx, 0)?,
        hashtable: None,
        est_entries: 0,
        collations: mcx::vec_with_capacity_in(mcx, 0)?,
        mem_used: 0,
        mem_limit: 0,
        entry: None,
        last_tuple: None,
        singlerow: false,
        binary_mode: false,
        stats: MemoizeInstrumentation::default(),
        shared_info: None,
        keyparamids: None,
        plan_node_id: 0,
        table_context_name: None,
    };
    Ok(alloc::boxed::Box::new(state))
}

/// `mstate->ss.ps.plan = (Plan *) node; mstate->ss.ps.state = estate;
/// mstate->ss.ps.ExecProcNode = ExecMemoize` — wire the PlanState back-link to
/// the shared plan node and install the execution callback. (`state` is the
/// `estate` the node init threads explicitly in the owned model; the
/// `plan_node_id` DSM key is copied off the plan node.)
fn init_plan_state_links<'mcx>(
    mstate: &mut MemoizeScanState<'mcx>,
    plan_node: &'mcx types_nodes::nodes::Node<'mcx>,
    _estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    mstate.ss.ps.plan = Some(plan_node);
    mstate.ss.ps.ExecProcNode = Some(exec_memoize_node);
    mstate.plan_node_id = plan_node.plan_head().plan_node_id;
    Ok(())
}

/// `mstate->hashkeydesc = ExecTypeFromExprList(node->param_exprs)` then
/// `mstate->tableslot = MakeSingleTupleTableSlot(hashkeydesc, &TTSOpsMinimalTuple)`
/// and `mstate->probeslot = MakeSingleTupleTableSlot(hashkeydesc, &TTSOpsVirtual)`,
/// plus the `param_exprs`/`hashfunctions` array allocations. The owned model
/// also distills each key column's `attbyval`/`attlen` from `hashkeydesc` into
/// `key_attrs` (the `TupleDescCompactAttr(hashkeydesc, i)` reads the binary-mode
/// hash/equal loops perform) and presizes the `tts_values`/`tts_isnull` mirrors
/// of the two slots. Fallible on OOM / type-lookup `ereport(ERROR)`.
fn init_hashkeydesc_and_slots<'mcx>(
    mstate: &mut MemoizeScanState<'mcx>,
    node: &Memoize<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    let nkeys = node.numKeys as usize;

    // mstate->hashkeydesc = ExecTypeFromExprList(node->param_exprs);
    let hashkeydesc =
        execTuples::exec_type_from_expr_list::call(mcx, node.param_exprs.as_slice())?;

    // Distill the per-key attbyval/attlen the binary-mode hash/equal loops read
    // via TupleDescCompactAttr(hashkeydesc, i).
    let mut key_attrs = mcx::vec_with_capacity_in(mcx, nkeys)?;
    {
        let desc = hashkeydesc
            .as_ref()
            .ok_or_else(|| elog_internal("Memoize hashkeydesc is NULL"))?;
        for i in 0..nkeys {
            let attr = desc.compact_attr(i);
            key_attrs.push(MemoizeKeyAttr {
                attbyval: attr.attbyval,
                attlen: attr.attlen,
            });
        }
    }
    mstate.key_attrs = key_attrs;

    // mstate->tableslot = MakeSingleTupleTableSlot(hashkeydesc, &TTSOpsMinimalTuple);
    let tableslot = {
        let desc_copy = clone_hashkeydesc(&hashkeydesc, mcx)?;
        let slot =
            execTuples::make_single_tuple_table_slot::call(mcx, desc_copy, TupleSlotKind::MinimalTuple)?;
        estate.make_slot(slot)?
    };
    // mstate->probeslot = MakeSingleTupleTableSlot(hashkeydesc, &TTSOpsVirtual);
    let probeslot = {
        let desc_copy = clone_hashkeydesc(&hashkeydesc, mcx)?;
        let slot =
            execTuples::make_single_tuple_table_slot::call(mcx, desc_copy, TupleSlotKind::Virtual)?;
        estate.make_slot(slot)?
    };
    mstate.tableslot = Some(tableslot);
    mstate.probeslot = Some(probeslot);
    mstate.hashkeydesc = hashkeydesc;

    // mstate->param_exprs = (ExprState **) palloc(nkeys * sizeof(ExprState *));
    // mstate->hashfunctions = (FmgrInfo *) palloc(nkeys * sizeof(FmgrInfo));
    // build_eqfuncoids fills these per key; here we presize the spines so the
    // per-key writes (`mstate->param_exprs[i] = ...`) can index in place.
    let mut param_exprs = mcx::vec_with_capacity_in(mcx, nkeys)?;
    for _ in 0..nkeys {
        // Placeholder ExprState; overwritten in build_eqfuncoids's per-key loop.
        param_exprs.push(mcx::alloc_in(mcx, types_nodes::execexpr::ExprState::default())?);
    }
    mstate.param_exprs = param_exprs;

    let mut hashfunctions = mcx::vec_with_capacity_in(mcx, nkeys)?;
    hashfunctions.resize(nkeys, types_core::fmgr::FmgrInfo::default());
    mstate.hashfunctions = hashfunctions;

    // Presize the tts_values/tts_isnull mirrors of the two slots.
    let mut tv = mcx::vec_with_capacity_in(mcx, nkeys)?;
    tv.resize(nkeys, DatumV::null());
    mstate.table_values = tv;
    let mut ti = mcx::vec_with_capacity_in(mcx, nkeys)?;
    ti.resize(nkeys, false);
    mstate.table_isnull = ti;
    let mut pv = mcx::vec_with_capacity_in(mcx, nkeys)?;
    pv.resize(nkeys, DatumV::null());
    mstate.probe_values = pv;
    let mut pi = mcx::vec_with_capacity_in(mcx, nkeys)?;
    pi.resize(nkeys, false);
    mstate.probe_isnull = pi;

    Ok(())
}

/// `CreateTupleDescCopy(hashkeydesc)` — a fresh owned copy of the cache-key row
/// type for a slot to take ownership of (each `MakeSingleTupleTableSlot` fixes
/// the slot to its own descriptor; the C node shares one `TupleDesc *`, the
/// owned model gives each slot its own copy in `mcx`).
fn clone_hashkeydesc<'mcx>(
    hashkeydesc: &types_tuple::heaptuple::TupleDesc<'mcx>,
    mcx: Mcx<'mcx>,
) -> PgResult<types_tuple::heaptuple::TupleDesc<'mcx>> {
    match hashkeydesc {
        Some(desc) => Ok(Some(mcx::alloc_in(mcx, desc.clone_in(mcx)?)?)),
        None => Ok(None),
    }
}

/// `mstate->cache_eq_expr = ExecBuildParamSetEqual(mstate->hashkeydesc,
/// &TTSOpsMinimalTuple, &TTSOpsVirtual, eqfuncoids, node->collations,
/// node->param_exprs, (PlanState *) mstate)` — compile the non-binary
/// key-equality expression. Routed through the execExpr owner seam.
fn build_cache_eq_expr<'mcx>(
    mstate: &mut MemoizeScanState<'mcx>,
    node: &Memoize<'mcx>,
    eqfuncoids: &[Oid],
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let desc_box = {
        let desc = mstate
            .hashkeydesc
            .as_ref()
            .ok_or_else(|| elog_internal("Memoize hashkeydesc is NULL"))?;
        // The seam borrows the descriptor; clone the data out of the PgBox into a
        // local owned copy whose borrow we lend (the C passes the live TupleDesc).
        desc.clone_in(estate.es_query_cxt)?
    };
    let collations: Vec<Oid> = node.collations.iter().copied().collect();
    let state = execExpr::exec_build_param_set_equal::call(
        &desc_box,
        TupleSlotKind::MinimalTuple,
        TupleSlotKind::Virtual,
        eqfuncoids,
        &collations,
        node.param_exprs.as_slice(),
        &mut mstate.ss.ps,
        estate,
    )?;
    mstate.cache_eq_expr = Some(state);
    Ok(())
}

/// `ExecStoreMinimalTuple(params, mstate->tableslot, false); slot_getallattrs(
/// mstate->tableslot)` then read the first `numkeys` `tts_values`/`tts_isnull` —
/// deform a cached entry's key `params` into `numkeys` `(value, isnull)` pairs.
/// Routed through the execTuples owner seams against the real `tableslot`.
fn deform_key_params<'mcx>(
    mstate: &mut MemoizeScanState<'mcx>,
    params: &MinimalTupleData<'mcx>,
    numkeys: usize,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Vec<types_datum::Datum>, Vec<bool>)> {
    let tableslot = mstate
        .tableslot
        .ok_or_else(|| elog_internal("Memoize tableslot not initialized"))?;
    let mcx = estate.es_query_cxt;

    // ExecStoreMinimalTuple(key->params, tslot, false): store a borrowed copy.
    let mtup = mcx::alloc_in(mcx, params.clone_in(mcx)?)?;
    execTuples::exec_force_store_minimal_tuple::call(tableslot, mtup, false, estate)?;

    // slot_getallattrs(tslot); read tts_values[i]/tts_isnull[i] for the keys.
    let mut values = Vec::with_capacity(numkeys);
    let mut isnull = Vec::with_capacity(numkeys);
    for i in 0..numkeys {
        let attr = execTuples::slot_getattr_by_id::call(estate, tableslot, (i + 1) as i16)?;
        values.push(attr.value);
        isnull.push(attr.isnull);
    }
    Ok((values, isnull))
}

/// `ExecStoreVirtualTuple(mstate->probeslot); ExecCopySlotMinimalTuple(
/// mstate->probeslot)` — materialize the prepared probe slot's `numkeys`
/// parameter values into a fresh owned `MinimalTuple` used as a cache entry's
/// key. Routed through the execTuples owner seams against the real `probeslot`.
fn copy_probe_slot_minimal_tuple<'mcx>(
    mstate: &mut MemoizeScanState<'mcx>,
    mcx: Mcx<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<MinimalTupleData<'mcx>> {
    let probeslot = mstate
        .probeslot
        .ok_or_else(|| elog_internal("Memoize probeslot not initialized"))?;

    // The probe slot's tts_values/tts_isnull were filled by prepare_probe_slot;
    // mirror them into the real slot and ExecStoreVirtualTuple. The store seam
    // takes bare scalar words (the still-bare-word execTuples ABI edge), so
    // unwrap each canonical value's by-value arm (a virtual-tuple key column is
    // a C `tts_values[i]` scalar word).
    let values: Vec<types_datum::Datum> =
        mstate.probe_values.iter().map(byval_word).collect();
    let isnull: Vec<bool> = mstate.probe_isnull.iter().copied().collect();
    execTuples::store_virtual_values::call(estate, probeslot, &values, &isnull)?;

    // ExecCopySlotMinimalTuple(probeslot): materialize as an owned MinimalTuple.
    let (mtup, _should_free) =
        execTuples::exec_fetch_slot_minimal_tuple::call(mcx, estate.slot_mut(probeslot))?;
    mtup.clone_in(mcx)
}

// ===========================================================================
// Value-type ABI helpers.
// ===========================================================================

/// Unwrap a canonical unified value's by-value arm into the bare scalar word.
///
/// The slot key columns Memoize handles are hash-key columns — always
/// pass-by-value scalars or by-reference *pointers*, i.e. a C `tts_values[i]`
/// machine word. This is the projection across the still-bare-word
/// (`types_datum::Datum`) execTuples / fmgr seam ABI edges, used until those
/// owners migrate to the unified value type. A by-reference image here would be
/// a caller bug (C would equally read garbage treating it as a scalar word).
fn byval_word(value: &DatumV<'_>) -> types_datum::Datum {
    match value {
        DatumV::ByVal(word) => *word,
        DatumV::ByRef(_) => {
            panic!("Memoize: scalar slot word expected, found a by-reference value")
        }
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
/// Every other operation the node performs is an outward call to an owner
/// subsystem's `-seams` crate (the expression engine `execExpr` — incl.
/// `ExecBuildParamSetEqual`; the tuple-slot ops `execTuples` — incl.
/// `ExecTypeFromExprList`, `MakeSingleTupleTableSlot`, the minimal-tuple
/// store/fetch and the virtual-store/`slot_getattr` deform-form the node's
/// `tableslot`/`probeslot` round-trip its cache keys through; the context
/// substrate `execUtils`; the outer-child dispatch `execProcnode`/`execAmi`;
/// the `datum.c` hash/equality leaves; the `lsyscache` catalog lookups; the
/// `fmgr` invocation; the `nodeHash` memory budget; the `tcop/postgres`
/// interrupt check; and the handle-addressed parallel-instrumentation accessors
/// in the execParallel support seams), with the node-side marshaling (the C
/// `makeNode(MemoizeState)`, the PlanState back-link/`ExecProcNode` install, and
/// the `ExecInitMemoize` builders `init_hashkeydesc_and_slots`/`build_cache_eq_expr`
/// /`deform_key_params`/`copy_probe_slot_minimal_tuple`) living in this crate as
/// plain functions over the owned `MemoizeScanState`.
pub fn init_seams() {
    seam::exec_memoize_estimate::set(exec_memoize_estimate);
    seam::exec_memoize_initialize_dsm::set(exec_memoize_initialize_dsm);
    seam::exec_memoize_initialize_worker::set(exec_memoize_initialize_worker);
    seam::exec_memoize_retrieve_instrumentation::set(exec_memoize_retrieve_instrumentation);
}
