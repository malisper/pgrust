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
//! # The build / introspection core (now landed — keystones #340–#344)
//!
//! With the four walls down — #340 (`ambuild`/`amoptions` vtable slots), #341
//! (pg_index INSERT carrier), #342 ([`IndexInfoCarrier`] for the build dispatch
//! edge), #343 (relcache `rd_index` reads) and #344 (heap_create / pg_class
//! seams) — this pass adds:
//!
//! * [`BuildIndexInfo`] — construct the `IndexInfo` describing an open index
//!   relation. The scalar fields come from the #343 relcache `rd_index` reads;
//!   the `amsummarizing` flag from the index-AM vtable (`relation_rd_indam`);
//!   the expression / predicate / exclusion legs delegate to the relcache's
//!   `pg_node_tree`-decoding `RelationGetIndex{Expressions,Predicate}` /
//!   `RelationGetExclusionInfo` seams. Installs `build_index_info` (brin
//!   insert-vacuum, amcheck).
//!
//! * [`index_build`] — dispatch the heap scan + fill through the AM's `ambuild`
//!   vtable slot via the #342 [`IndexInfoCarrier`], wrapping it in the
//!   userid/security-context + GUC-nest-level frame and the CREATE INDEX
//!   progress report, then `index_update_stats` on heap and index. Installs
//!   `index_build` (bootstrap `build_indices`).
//!
//! # What is NOT landed in this pass (precise STOP boundaries)
//!
//! The full catalog-*write* drivers (`index_create`, `index_constraint_create`,
//! the `index_concurrently_*` family, `validate_index`, `index_drop`,
//! `index_set_state_flags`, `reindex_index`/`reindex_relation`) need a much
//! larger outward producer surface (ConstructTupleDescriptor /
//! InitializeAttributeOids / AppendAttributeTuples / UpdateIndexRelation /
//! recordDependency* / WaitForLockers / snapshot push-pop / relcache rebuild)
//! that is not yet ported, so their inward seams (`index_create`,
//! `reindex_relation`, `index_drop`) stay UNINSTALLED here: a call panics
//! loudly (mirror-PG-and-panic).
//!
//! Within the landed `index_build`, four sub-legs are reached only under
//! specific conditions and route to precise inward seams owned by the layers
//! that hold their substrate (each panics until that owner installs it):
//! `plan_create_index_workers` (parallel build, planner), the unlogged-index
//! init-fork emit (`build_index_init_fork_if_needed`, catalog/storage),
//! `set_index_indcheckxmin` (broken-HOT-chain pg_index update, catalog-indexing)
//! and `index_check_exclusion` (exclusion-constraint second pass, executor).
//! `index_update_stats` (the pg_class field-level in-place stats write) is
//! likewise a precise seam owned by the catalog-indexing (pg_class write)
//! layer.

extern crate alloc;

use alloc::vec::Vec;

use types_core::fmgr::INDEX_MAX_KEYS;
use types_core::primitive::{InvalidOid, OidIsValid};
use types_core::primitive::{Oid, Size};
use types_error::{PgError, PgResult};

use mcx::Mcx;
use types_nodes::execnodes::IndexInfo;
use types_rel::Relation;
use types_tableam::index_info_carrier::IndexInfoCarrier;

use backend_access_transam_xact_seams as xact;
use backend_catalog_index_seams as index_seam;
use backend_access_transam_parallel_rt_seams as rt;
use backend_utils_cache_syscache_seams as syscache;
use backend_utils_cache_relcache_seams as relcache;
use backend_utils_init_miscinit_seams as miscinit;
use backend_commands_matview_deps_seams as matview;
use backend_utils_misc_guc_seams as guc;
use backend_utils_activity_backend_progress_seams as progress;

/* progress.h CREATE INDEX phase constants (duplicated here, as in the AM
 * crates, since `commands/progress.h` has no owned crate). */
const PROGRESS_CREATEIDX_PHASE: i32 = 9;
const PROGRESS_CREATEIDX_SUBPHASE: i32 = 10;
const PROGRESS_CREATEIDX_TUPLES_TOTAL: i32 = 11;
const PROGRESS_CREATEIDX_TUPLES_DONE: i32 = 12;
const PROGRESS_SCAN_BLOCKS_TOTAL: i32 = 15;
const PROGRESS_SCAN_BLOCKS_DONE: i32 = 16;
const PROGRESS_CREATEIDX_PHASE_BUILD: i64 = 2;
const PROGRESS_CREATEIDX_SUBPHASE_INITIALIZE: i64 = 1;

/// `RELPERSISTENCE_UNLOGGED` (pg_class.h).
const RELPERSISTENCE_UNLOGGED: i8 = b'u' as i8;

/// `SECURITY_RESTRICTED_OPERATION` (miscadmin.h).
const SECURITY_RESTRICTED_OPERATION: i32 = 1 << 1;

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
 * BuildIndexInfo
 * ========================================================================= */

/// `BuildIndexInfo(index)` (catalog/index.c): construct an `IndexInfo` record
/// describing the open index relation `index`, fetching any expression /
/// predicate / exclusion-constraint info it carries.
///
/// ```c
/// IndexInfo *
/// BuildIndexInfo(Relation index)
/// {
///     IndexInfo  *ii;
///     Form_pg_index indexStruct = index->rd_index;
///     int         i;
///     int         numAtts;
///
///     numAtts = indexStruct->indnatts;
///     if (numAtts < 1 || numAtts > INDEX_MAX_KEYS)
///         elog(ERROR, "invalid indnatts %d for index %u",
///              numAtts, RelationGetRelid(index));
///
///     ii = makeIndexInfo(indexStruct->indnatts,
///                        indexStruct->indnkeyatts,
///                        index->rd_rel->relam,
///                        RelationGetIndexExpressions(index),
///                        RelationGetIndexPredicate(index),
///                        indexStruct->indisunique,
///                        indexStruct->indnullsnotdistinct,
///                        indexStruct->indisready,
///                        false,
///                        index->rd_indam->amsummarizing,
///                        indexStruct->indisexclusion && indexStruct->indisunique);
///
///     for (i = 0; i < numAtts; i++)
///         ii->ii_IndexAttrNumbers[i] = indexStruct->indkey.values[i];
///
///     if (indexStruct->indisexclusion)
///         RelationGetExclusionInfo(index,
///                                  &ii->ii_ExclusionOps,
///                                  &ii->ii_ExclusionProcs,
///                                  &ii->ii_ExclusionStrats);
///     return ii;
/// }
/// ```
///
/// The `index->rd_index` field scalars are read through the #343 relcache
/// `rd_index_*` seams (`rd_index == NULL` — a non-index relation — surfaces as
/// the `None`/`false` those seams return, mirroring the C `Form_pg_index`
/// deref crashing on a NULL `rd_index`, which only a programming error
/// produces). `index->rd_indam->amsummarizing` is read off the AM vtable
/// (`relation_rd_indam`); the expression / predicate / exclusion legs decode
/// `pg_node_tree`/`pg_constraint` through the relcache's
/// `RelationGetIndex{Expressions,Predicate}` / `RelationGetExclusionInfo`
/// seams. `makeIndexInfo`'s body is inlined here so the expression / predicate
/// lists (which the standalone `make_index_info` helper drops) ride into the
/// constructed record.
pub fn BuildIndexInfo<'mcx>(
    mcx: Mcx<'mcx>,
    index: &Relation<'mcx>,
) -> PgResult<IndexInfo<'mcx>> {
    /* check the number of keys, and copy attr numbers into the IndexInfo */
    let numAtts = relcache::rd_index_indnatts::call(index)?.unwrap_or(0) as i32;
    if numAtts < 1 || numAtts > INDEX_MAX_KEYS as i32 {
        return elog_error(alloc::format!(
            "invalid indnatts {numAtts} for index {}",
            index.rd_id
        ));
    }

    let indnkeyatts = relcache::rd_index_indnkeyatts::call(index)?.unwrap_or(0) as i32;
    let relam = relcache::rd_rel_relam::call(index)?;
    let indisunique = relcache::rd_index_indisunique::call(index)?;
    let indnullsnotdistinct = relcache::rd_index_indnullsnotdistinct::call(index)?;
    let indisready = relcache::rd_index_indisready::call(index)?;
    let indisexclusion = relcache::rd_index_indisexclusion::call(index)?;

    /* `index->rd_indam->amsummarizing` — read off the AM vtable. */
    let amsummarizing = match relcache::relation_rd_indam::call(index.rd_id) {
        Some(amroutine) => amroutine.amsummarizing,
        None => false,
    };

    /*
     * Create the node, fetching any expressions needed for expressional indexes
     * and index predicate if any. (makeIndexInfo, inlined so the expression /
     * predicate lists are carried in.)
     */
    let ii_Expressions = relcache::relation_get_index_expressions::call(mcx, index)?;
    let ii_Predicate = relcache::relation_get_index_predicate::call(mcx, index)?;

    /* makeIndexInfo asserts (numkeyatts != 0, numkeyatts <= numatts). */
    debug_assert!(indnkeyatts != 0);
    debug_assert!(indnkeyatts <= numAtts);

    let mut ii = IndexInfo {
        ii_NumIndexAttrs: numAtts,
        ii_NumIndexKeyAttrs: indnkeyatts,
        ii_Am: relam,
        ii_Expressions,
        ii_Predicate,
        ii_Unique: indisunique,
        ii_NullsNotDistinct: indnullsnotdistinct,
        ii_ReadyForInserts: indisready,
        ii_Concurrent: false,
        ii_Summarizing: amsummarizing,
        ii_WithoutOverlaps: indisexclusion && indisunique,
        ii_IndexAttrNumbers: Default::default(),
        ..Default::default()
    };

    /* fill in attribute numbers (indexStruct->indkey.values[0..numAtts]) */
    let indkey = relcache::rd_index_indkey::call(index)?.unwrap_or_default();
    for i in 0..numAtts as usize {
        ii.ii_IndexAttrNumbers[i] = indkey[i];
    }

    /* fetch exclusion constraint info if any */
    if indisexclusion {
        let (ops, procs, strats) = relcache::relation_get_exclusion_info::call(mcx, index)?;
        ii.ii_ExclusionOps = Some(ops);
        ii.ii_ExclusionProcs = Some(procs);
        ii.ii_ExclusionStrats = Some(strats);
    }

    Ok(ii)
}

/* ===========================================================================
 * index_build
 * ========================================================================= */

/// `index_build(heapRelation, indexRelation, indexInfo, isreindex=false,
/// parallel=false)` (catalog/index.c): invoke the index AM's build routine,
/// then update the heap's and the index's `pg_class` stats rows. This is the
/// shape the bootstrap `build_indices` driver calls (`isreindex` /`parallel`
/// both `false`; the C default-build path).
///
/// The AM build is dispatched through the `rd_indam->ambuild` vtable slot
/// (#340) via the lifetime-preserving [`IndexInfoCarrier`] (#342). The
/// userid/security-context switch + GUC-nest-level frame + RestrictSearchPath
/// wrap the build exactly as the C does (so index functions run as the table
/// owner under SECURITY_RESTRICTED_OPERATION).
pub fn index_build<'mcx>(
    mcx: Mcx<'mcx>,
    heap_relation: &Relation<'mcx>,
    index_relation: &Relation<'mcx>,
    index_info: &mut IndexInfo<'mcx>,
) -> PgResult<()> {
    /*
     * sanity checks — the C asserts rd_indam / ambuild / ambuildempty are
     * present. The vtable is resolved from the relcache; a missing one is the
     * C NULL-pointer crash (a programming error).
     */
    let amroutine = relcache::relation_rd_indam::call(index_relation.rd_id)
        .unwrap_or_else(|| panic!("index {} has no rd_indam vtable", index_relation.rd_id));

    /*
     * Determine worker process details for parallel CREATE INDEX. Currently,
     * only btree, GIN, and BRIN have support for parallel builds.
     */
    if miscinit::is_normal_processing_mode::call() && amroutine.amcanbuildparallel {
        // parallel == true at this call shape only via index_create; the
        // bootstrap driver runs in non-normal mode so this branch is dead
        // there. The planner owns plan_create_index_workers.
        index_info.ii_ParallelWorkers = index_seam::plan_create_index_workers::call(
            heap_relation.rd_id,
            index_relation.rd_id,
        )?;
    }

    /* (the DEBUG1 serial/parallel build line is internal-elog only) */

    /*
     * Switch to the table owner's userid, so that any index functions are run
     * as that user. Also lock down security-restricted operations and arrange
     * to make GUC variable changes local to this command.
     */
    let (save_userid, save_sec_context) = matview::get_user_id_and_sec_context::call()?;
    rt::set_user_id_and_sec_context::call(
        relcache::rd_rel_relowner::call(heap_relation)?,
        save_sec_context | SECURITY_RESTRICTED_OPERATION,
    )?;
    let save_nestlevel = guc::new_guc_nest_level::call();
    guc::restrict_search_path::call()?;

    /* Set up initial progress report status */
    {
        let progress_index = [
            PROGRESS_CREATEIDX_PHASE,
            PROGRESS_CREATEIDX_SUBPHASE,
            PROGRESS_CREATEIDX_TUPLES_DONE,
            PROGRESS_CREATEIDX_TUPLES_TOTAL,
            PROGRESS_SCAN_BLOCKS_DONE,
            PROGRESS_SCAN_BLOCKS_TOTAL,
        ];
        let progress_vals = [
            PROGRESS_CREATEIDX_PHASE_BUILD,
            PROGRESS_CREATEIDX_SUBPHASE_INITIALIZE,
            0,
            0,
            0,
            0,
        ];
        progress::pgstat_progress_update_multi_param::call(&progress_index, &progress_vals);
    }

    /*
     * Call the access method's build procedure (through the IndexInfoCarrier;
     * the AM adapter downcasts it back to the concrete IndexInfo<'mcx>).
     */
    let stats = {
        let mut carrier = IndexInfoCarrier::new(index_info);
        (amroutine.ambuild)(mcx, heap_relation, index_relation, &mut carrier)?
    };

    /*
     * If this is an unlogged index, we may need to write out an init fork for
     * it -- but we must first check whether one already exists. (smgr-create +
     * WAL + ambuildempty live in the catalog/storage layer's seam.)
     */
    if relcache::rd_rel_relpersistence::call(index_relation)? == RELPERSISTENCE_UNLOGGED {
        index_seam::build_index_init_fork_if_needed::call(mcx, index_relation)?;
    }

    /*
     * If we found any potentially broken HOT chains, mark the index as not
     * being usable until the current transaction is below the event horizon.
     * This code path can only be taken during non-concurrent CREATE INDEX
     * (isreindex == false, ii_Concurrent == false here).
     */
    if index_info.ii_BrokenHotChain && !index_info.ii_Concurrent {
        index_seam::set_index_indcheckxmin::call(index_relation.rd_id)?;
    }

    /*
     * Update heap and index pg_class rows.
     */
    index_seam::index_update_stats::call(heap_relation, true, stats.heap_tuples)?;
    index_seam::index_update_stats::call(index_relation, false, stats.index_tuples)?;

    /* Make the updated catalog row versions visible */
    xact::command_counter_increment::call()?;

    /*
     * If it's for an exclusion constraint, make a second pass over the heap to
     * verify that the constraint is satisfied.
     */
    if index_info.ii_ExclusionOps.is_some() {
        index_seam::index_check_exclusion::call(mcx, heap_relation, index_relation, index_info)?;
    }

    /* Roll back any GUC changes executed by index functions */
    guc::at_eoxact_guc::call(false, save_nestlevel)?;

    /* Restore userid and security context */
    rt::set_user_id_and_sec_context::call(save_userid, save_sec_context)?;

    Ok(())
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

    // BuildIndexInfo (brin insert-vacuum, amcheck) and index_build (bootstrap
    // build_indices) — the build / introspection core (keystones #340–#344).
    index_seam::build_index_info::set(BuildIndexInfo);
    index_seam::index_build::set(index_build);

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
