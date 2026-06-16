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
//! # Also landed in this pass
//!
//! * [`BuildSpeculativeIndexInfo`] — fill a unique `IndexInfo`'s `ii_Unique*`
//!   arrays (equality operators / support procs / strategy numbers) from the
//!   index opclasses, for speculative insertion (`INSERT ... ON CONFLICT`) and
//!   logical-replication conflict detection. Reads the relcache `rd_opfamily` /
//!   `rd_opcintype` / `rd_index_indnkeyatts`, the amapi
//!   `IndexAmTranslateCompareType`, and the lsyscache
//!   `get_opfamily_member` / `get_opcode`. Installs `build_speculative_index_info`
//!   (`ExecOpenIndices(..., speculative)`).
//!
//! * The **IndexInfo introspection / index-create catalog-build cluster**:
//!   `index_check_primary_key` + `relationHasPrimaryKey` (pre-PRIMARY-KEY
//!   checks over the relcache index list + the `index_get_indisprimary` /
//!   `att_get_attnotnull` syscache projections), `ConstructTupleDescriptor` +
//!   `AppendAttributeTuples` (the index tuple-descriptor build + pg_attribute
//!   insert legs of `index_create`, over the new amapi
//!   `GetIndexAmRoutineByAmId`, relcache `relation_get_descr` /
//!   `rd_rel_relnatts`, heap `check_attribute_type`, the `pg_type_form` /
//!   `pg_opclass_keytype` syscache reads, `get_base_element_type`, and the
//!   catalog-indexing `append_attribute_tuples` writer), `BuildDummyIndexInfo`
//!   (the user-code-free `IndexInfo` over relcache
//!   `relation_get_dummy_index_expressions`), and `CompareIndexInfo` (over the
//!   rewriteManip `map_variable_attnos_expr_list` + equalfuncs
//!   `equal_expr_list` seams). These are exposed as `pub fn` API surface (as in
//!   the C, they are called directly by `indexcmds` / partition / amcheck once
//!   those land); `ConstructTupleDescriptor` / `AppendAttributeTuples` are the
//!   private helpers `index_create` will call.
//!
//! * `UpdateIndexRelation` — the file-static helper of `index_create` that
//!   inserts the `pg_index` row. Lands over the pg_index INSERT carrier
//!   (keystone #341) + the catalog-indexing `catalog_tuple_insert_pg_index`
//!   writer + `table_open`/`table_close` of pg_index + the `nodeToString`
//!   (outfuncs) image of the index expressions / `make_ands_explicit`
//!   predicate. (Reachable only once its caller `index_create` lands, hence
//!   kept private; see the STOP boundary below.)
//!
//! # What is NOT landed in this pass (precise STOP boundaries)
//!
//! The full catalog-*write* drivers — `index_create`, `index_constraint_create`,
//! the `index_concurrently_*` family, `validate_index`, `index_drop`,
//! `index_set_state_flags`, `reindex_index`/`reindex_relation` — need a much
//! larger outward producer surface that is not yet ported, so their inward
//! seams (`index_create`, `reindex_relation`, `index_drop`) stay UNINSTALLED
//! here: a call panics loudly (mirror-PG-and-panic). Concretely:
//!
//! * `index_create` still needs (and so stays seam-and-panic via the
//!   `index_create` inward seam): `GetNewRelFileNumber`, `LockRelation`,
//!   `get_collation_isdeterministic`, the live-relcache `rd_rel` scribbles
//!   (`relowner`/`relam`/`relispartition` setters) on the `heap_create`d index
//!   entry, `RelationInitIndexAccessInfo`, `index_opclass_options`,
//!   `index_register` (bootstrap), `StoreSingleInheritance` /
//!   `SetRelationHasSubclass` for partitions, and the
//!   `IsSystemRelation`/`IsCatalogRelation`/`get_relname_relid`/
//!   `ConstraintNameIsUsed` precondition checks — a ~20-callee outward surface
//!   across unported owners. Its tupdesc-build / pg_attribute-insert /
//!   pg_index-write halves (`ConstructTupleDescriptor` /
//!   `AppendAttributeTuples` / `UpdateIndexRelation`) and its
//!   dependency-recording calls ([`record_object_address_dependencies`] /
//!   [`record_dependency_on`] / [`record_dependency_on_single_rel_expr`]) are
//!   now all available — only the relation-create + live-relcache-mutate spine
//!   remains.
//! * `index_set_state_flags` and the `reindex_index` revalidation leg need a
//!   *full* pg_index `Form` read-modify-write (`SearchSysCacheCopy1(INDEXRELID)`
//!   → set `indisready`/`indislive`/`indisvalid`/`indisreplident`/`indcheckxmin`
//!   → `CatalogTupleUpdate`). The only available pg_index `Form` carrier
//!   ([`types_cluster::PgIndexForm`]) is the cluster-mark *trimmed* carrier
//!   (only `indisclustered`/`indisvalid`); writing the other state flags through
//!   it would silently drop them (a contract divergence). Needs a full
//!   pg_index-`Form` update carrier first.
//! * `index_drop` / `index_concurrently_*` / `validate_index` /
//!   `IndexCheckExclusion` need transaction commit/start, snapshot push/pop,
//!   `WaitForLockers`, `RelationDropStorage`, `performDeletion`, executor
//!   table-scan + `check_exclusion_constraint`, and `tuplesort`-based TID merge —
//!   all unported here.
//! * `index_constraint_create` needs `CreateConstraintEntry` /
//!   `CreateTrigger` / `deleteDependencyRecordsForClass`, none reachable as a
//!   callable producer.
//! * `FormIndexDatum` is NOT landed here: the `form_index_datum` inward seam's
//!   result-array contract is the word-model `types_datum::Datum`, but the
//!   executor eval/slot seams it must route through (`exec_prepare_expr_list` /
//!   `slot_getattr` / `slot_getsysattr` / `exec_eval_expr_switch_context`)
//!   yield the canonical `types_tuple::Datum`; reconciling that element-type
//!   divergence is owned by the genam-side migration the seam doc names, so it
//!   stays uninstalled (mirror-pg-and-panic) until then.
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
use backend_access_table_table_seams as table_am;
use backend_catalog_indexing_seams as indexing;
use backend_nodes_core_seams as nodes_seam;
use backend_utils_cache_lsyscache_seams as lsyscache;
use backend_access_index_amapi_seams as amapi;
use backend_catalog_heap_seams as heap;
use backend_nodes_nodeFuncs_seams as nodefuncs;
use backend_nodes_equalfuncs_seams as equalfuncs;
use backend_rewrite_rewritemanip_seams as rewritemanip;

use types_core::primitive::AttrNumber;

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
 * UpdateIndexRelation
 * ========================================================================= */

/// `IndexRelationId` — pg_index's OID (`catalog/pg_index.h`).
const INDEX_RELATION_ID: Oid = 2610;

/// `RowExclusiveLock` (`storage/lockdefs.h`).
const ROW_EXCLUSIVE_LOCK: i32 = 3;

/// `DEFAULT_COLLATION_OID` (`catalog/pg_collation_d.h`).
const DEFAULT_COLLATION_OID: Oid = 100;

/// `UpdateIndexRelation(indexoid, heapoid, parentIndexId, indexInfo,
/// collationOids, opclassOids, coloptions, primary, isexclusion, immediate,
/// isvalid, isready)` (catalog/index.c, a file-static helper of
/// `index_create`): construct and insert a new entry in the `pg_index`
/// catalog.
///
/// ```c
/// static void
/// UpdateIndexRelation(Oid indexoid, Oid heapoid, Oid parentIndexId,
///                     const IndexInfo *indexInfo,
///                     const Oid *collationOids, const Oid *opclassOids,
///                     const int16 *coloptions,
///                     bool primary, bool isexclusion, bool immediate,
///                     bool isvalid, bool isready)
/// {
///     int2vector *indkey; oidvector *indcollation; oidvector *indclass;
///     int2vector *indoption; Datum exprsDatum; Datum predDatum;
///     Datum values[Natts_pg_index]; bool nulls[Natts_pg_index] = {0};
///     Relation pg_index; HeapTuple tuple; int i;
///
///     indkey = buildint2vector(NULL, indexInfo->ii_NumIndexAttrs);
///     for (i = 0; i < indexInfo->ii_NumIndexAttrs; i++)
///         indkey->values[i] = indexInfo->ii_IndexAttrNumbers[i];
///     indcollation = buildoidvector(collationOids, indexInfo->ii_NumIndexKeyAttrs);
///     indclass = buildoidvector(opclassOids, indexInfo->ii_NumIndexKeyAttrs);
///     indoption = buildint2vector(coloptions, indexInfo->ii_NumIndexKeyAttrs);
///
///     if (indexInfo->ii_Expressions != NIL) {
///         char *exprsString = nodeToString(indexInfo->ii_Expressions);
///         exprsDatum = CStringGetTextDatum(exprsString); pfree(exprsString);
///     } else exprsDatum = (Datum) 0;
///     if (indexInfo->ii_Predicate != NIL) {
///         char *predString = nodeToString(make_ands_explicit(indexInfo->ii_Predicate));
///         predDatum = CStringGetTextDatum(predString); pfree(predString);
///     } else predDatum = (Datum) 0;
///
///     pg_index = table_open(IndexRelationId, RowExclusiveLock);
///     values[Anum_pg_index_indexrelid - 1] = ObjectIdGetDatum(indexoid);
///     ... (the full values[]/nulls[] array, see the C) ...
///     tuple = heap_form_tuple(RelationGetDescr(pg_index), values, nulls);
///     CatalogTupleInsert(pg_index, tuple);
///     table_close(pg_index, RowExclusiveLock);
///     heap_freetuple(tuple);
/// }
/// ```
///
/// The pg_index INSERT carrier ([`types_catalog::pg_index::PgIndexInsertRow`],
/// keystone #341) maps 1:1 to the C `values[]` array; the seam
/// `catalog_tuple_insert_pg_index` (catalog-indexing) does the
/// `buildint2vector`/`buildoidvector`, `heap_form_tuple`, and
/// `CatalogTupleInsert` from that row, so this body just assembles the row and
/// brackets it with the `table_open`/`table_close` of `pg_index` the C does.
///
/// The index expressions / predicate cross as their `nodeToString` images (the
/// carrier's `Option<String>` fields, `None` == the C `(Datum) 0` SQL NULL):
/// `ii_Expressions` is `nodeToString`'d as a `List` node (each `Expr` wrapped
/// in a `Node::Expr` cell, the whole wrapped in `Node::List`), and the
/// predicate is `nodeToString(make_ands_explicit(ii_Predicate))` — exactly the
/// C's implicit-AND→explicit-AND storage form.
#[allow(clippy::too_many_arguments)]
fn UpdateIndexRelation<'mcx>(
    mcx: Mcx<'mcx>,
    indexoid: Oid,
    heapoid: Oid,
    index_info: &IndexInfo<'mcx>,
    collation_oids: &[Oid],
    opclass_oids: &[Oid],
    coloptions: &[i16],
    primary: bool,
    isexclusion: bool,
    immediate: bool,
    isvalid: bool,
    isready: bool,
) -> PgResult<()> {
    use types_nodes::nodes::Node;

    let numatts = index_info.ii_NumIndexAttrs as usize;
    let numkeyatts = index_info.ii_NumIndexKeyAttrs as usize;

    /*
     * Copy the index key, opclass, and indoption info into arrays.
     *
     *     indkey = buildint2vector(NULL, indexInfo->ii_NumIndexAttrs);
     *     for (i = 0; i < indexInfo->ii_NumIndexAttrs; i++)
     *         indkey->values[i] = indexInfo->ii_IndexAttrNumbers[i];
     *     indcollation = buildoidvector(collationOids, indexInfo->ii_NumIndexKeyAttrs);
     *     indclass = buildoidvector(opclassOids, indexInfo->ii_NumIndexKeyAttrs);
     *     indoption = buildint2vector(coloptions, indexInfo->ii_NumIndexKeyAttrs);
     *
     * (buildint2vector/buildoidvector run inside catalog_tuple_insert_pg_index;
     * here we just gather the values the carrier holds.)
     */
    let indkey: Vec<types_core::primitive::AttrNumber> =
        index_info.ii_IndexAttrNumbers[..numatts].to_vec();
    let indcollation: Vec<Oid> = collation_oids[..numkeyatts].to_vec();
    let indclass: Vec<Oid> = opclass_oids[..numkeyatts].to_vec();
    let indoption: Vec<i16> = coloptions[..numkeyatts].to_vec();

    /*
     * Convert the index expressions (if any) to a text datum.
     *
     *     if (indexInfo->ii_Expressions != NIL)
     *         exprsDatum = CStringGetTextDatum(nodeToString(indexInfo->ii_Expressions));
     *     else exprsDatum = (Datum) 0;
     */
    let indexprs: Option<alloc::string::String> = match &index_info.ii_Expressions {
        Some(exprs) if !exprs.is_empty() => {
            // nodeToString of the List of Exprs.
            let mut cells = mcx::vec_with_capacity_in(mcx, exprs.len())?;
            for e in exprs.iter() {
                cells.push(mcx::alloc_in(mcx, Node::Expr(e.clone_in(mcx)?))?);
            }
            let list = Node::List(cells);
            let s = nodes_seam::node_to_string_with_locations::call(mcx, &list)?;
            Some(s.as_str().into())
        }
        _ => None,
    };

    /*
     * Convert the index predicate (if any) to a text datum.  Note we convert
     * implicit-AND format to normal explicit-AND for storage.
     *
     *     if (indexInfo->ii_Predicate != NIL)
     *         predDatum = CStringGetTextDatum(
     *             nodeToString(make_ands_explicit(indexInfo->ii_Predicate)));
     *     else predDatum = (Datum) 0;
     */
    let indpred: Option<alloc::string::String> = match &index_info.ii_Predicate {
        Some(pred) if !pred.is_empty() => {
            let mut clauses = alloc::vec::Vec::with_capacity(pred.len());
            for e in pred.iter() {
                clauses.push(e.clone_in(mcx)?);
            }
            let anded = backend_nodes_core::makefuncs::make_ands_explicit(clauses);
            let node = Node::Expr(anded);
            let s = nodes_seam::node_to_string_with_locations::call(mcx, &node)?;
            Some(s.as_str().into())
        }
        _ => None,
    };

    /*
     * Build a pg_index tuple (the carrier maps 1:1 to the C `values[]`).
     *
     *     values[Anum_pg_index_indexrelid - 1] = ObjectIdGetDatum(indexoid);
     *     values[Anum_pg_index_indrelid - 1] = ObjectIdGetDatum(heapoid);
     *     ... indisclustered = false, indcheckxmin = false, indislive = true,
     *         indisreplident = false ...
     */
    let row = types_catalog::pg_index::PgIndexInsertRow {
        indexrelid: indexoid,
        indrelid: heapoid,
        indnatts: index_info.ii_NumIndexAttrs as i16,
        indnkeyatts: index_info.ii_NumIndexKeyAttrs as i16,
        indisunique: index_info.ii_Unique,
        indnullsnotdistinct: index_info.ii_NullsNotDistinct,
        indisprimary: primary,
        indisexclusion: isexclusion,
        indimmediate: immediate,
        indisclustered: false,
        indisvalid: isvalid,
        indcheckxmin: false,
        indisready: isready,
        indislive: true,
        indisreplident: false,
        indkey,
        indcollation,
        indclass,
        indoption,
        indexprs,
        indpred,
    };

    /*
     * open the system catalog index relation, insert the tuple, and close.
     *
     *     pg_index = table_open(IndexRelationId, RowExclusiveLock);
     *     CatalogTupleInsert(pg_index, tuple);
     *     table_close(pg_index, RowExclusiveLock);
     */
    let pg_index = table_am::table_open::call(mcx, INDEX_RELATION_ID, ROW_EXCLUSIVE_LOCK)?;
    indexing::catalog_tuple_insert_pg_index::call(mcx, &pg_index, &row)?;
    // table_close(pg_index, RowExclusiveLock) — `table_close` is `relation_close`.
    table_am::relation_close::call(pg_index.rd_id, ROW_EXCLUSIVE_LOCK)?;

    Ok(())
}

/* ===========================================================================
 * BuildSpeculativeIndexInfo
 * ========================================================================= */

/// `BuildSpeculativeIndexInfo(index, ii)` (catalog/index.c): add extra state to
/// a unique-index `IndexInfo` record that speculative insertion (INSERT ... ON
/// CONFLICT) and logical-replication conflict detection need — the per-key
/// equality operators (`ii_UniqueOps`), their support function OIDs
/// (`ii_UniqueProcs`), and the opclass strategy numbers (`ii_UniqueStrats`).
///
/// ```c
/// void
/// BuildSpeculativeIndexInfo(Relation index, IndexInfo *ii)
/// {
///     int indnkeyatts = IndexRelationGetNumberOfKeyAttributes(index);
///     int i;
///     Assert(ii->ii_Unique);
///     ii->ii_UniqueOps = (Oid *) palloc(sizeof(Oid) * indnkeyatts);
///     ii->ii_UniqueProcs = (Oid *) palloc(sizeof(Oid) * indnkeyatts);
///     ii->ii_UniqueStrats = (uint16 *) palloc(sizeof(uint16) * indnkeyatts);
///     for (i = 0; i < indnkeyatts; i++) {
///         ii->ii_UniqueStrats[i] =
///             IndexAmTranslateCompareType(COMPARE_EQ, index->rd_rel->relam,
///                                         index->rd_opfamily[i], false);
///         ii->ii_UniqueOps[i] =
///             get_opfamily_member(index->rd_opfamily[i], index->rd_opcintype[i],
///                                 index->rd_opcintype[i], ii->ii_UniqueStrats[i]);
///         if (!OidIsValid(ii->ii_UniqueOps[i]))
///             elog(ERROR, "missing operator %d(%u,%u) in opfamily %u", ...);
///         ii->ii_UniqueProcs[i] = get_opcode(ii->ii_UniqueOps[i]);
///     }
/// }
/// ```
///
/// `IndexRelationGetNumberOfKeyAttributes(index)` reads `rd_index->indnkeyatts`
/// (relcache seam). `index->rd_opfamily[i]` / `index->rd_opcintype[i]` are the
/// relcache `rd_opfamily` / `rd_opcintype` reads (1-based `attno = i + 1`).
/// `IndexAmTranslateCompareType` is the amapi seam (`COMPARE_EQ` == 3),
/// `get_opfamily_member` / `get_opcode` are the lsyscache seams. The result
/// arrays land in `mcx`-backed `PgVec`s in the three `ii_Unique*` fields.
pub fn BuildSpeculativeIndexInfo<'mcx>(
    index: &Relation<'mcx>,
    index_info: &mut IndexInfo<'mcx>,
) -> PgResult<()> {
    /* indnkeyatts = IndexRelationGetNumberOfKeyAttributes(index); */
    let indnkeyatts = relcache::rd_index_indnkeyatts::call(index)?.unwrap_or(0) as usize;

    /* fetch info for checking unique indexes — Assert(ii->ii_Unique); */
    debug_assert!(index_info.ii_Unique);

    let mcx = index_info
        .ii_Context
        .expect("BuildSpeculativeIndexInfo: IndexInfo has no owning context");

    let mut unique_ops: mcx::PgVec<'mcx, Oid> = mcx::vec_with_capacity_in(mcx, indnkeyatts)?;
    let mut unique_procs: mcx::PgVec<'mcx, Oid> = mcx::vec_with_capacity_in(mcx, indnkeyatts)?;
    let mut unique_strats: mcx::PgVec<'mcx, u16> = mcx::vec_with_capacity_in(mcx, indnkeyatts)?;

    let relam = relcache::rd_rel_relam::call(index)?;

    /*
     * We have to look up the operator's strategy number.  This provides a
     * cross-check that the operator does match the index.
     */
    for i in 0..indnkeyatts {
        let attno = (i + 1) as types_core::primitive::AttrNumber;
        let opfamily = relcache::rd_opfamily::call(index, attno)?;
        let opcintype = relcache::rd_opcintype::call(index, attno)?;

        /* COMPARE_EQ == 3 (access/cmptype.h). */
        let strat = amapi::index_am_translate_cmptype::call(3, relam, opfamily, false)?;
        unique_strats.push(strat as u16);

        let op = lsyscache::get_opfamily_member::call(opfamily, opcintype, opcintype, strat)?;
        if !OidIsValid(op) {
            return elog_error(alloc::format!(
                "missing operator {strat}({opcintype},{opcintype}) in opfamily {opfamily}"
            ));
        }
        unique_ops.push(op);
        unique_procs.push(lsyscache::get_opcode::call(op)?);
    }

    index_info.ii_UniqueOps = Some(unique_ops);
    index_info.ii_UniqueProcs = Some(unique_procs);
    index_info.ii_UniqueStrats = Some(unique_strats);

    Ok(())
}

/* ===========================================================================
 * relationHasPrimaryKey / index_check_primary_key
 * ========================================================================= */

/// `InvalidCompressionMethod` (`access/toast_compression.h`) — `'\0'`.
const INVALID_COMPRESSION_METHOD: i8 = 0;
/// `ANYELEMENTOID` / `ANYARRAYOID` (pg_type.h).
const ANYELEMENTOID: Oid = 2283;
const ANYARRAYOID: Oid = 2277;
/// `ATTRIBUTE_FIXED_PART_SIZE` is the fixed C struct size; in the safe port a
/// fresh `FormData_pg_attribute` is `Default`-zeroed, which is the `MemSet(to,
/// 0, ATTRIBUTE_FIXED_PART_SIZE)` the C does.

/// `relationHasPrimaryKey(rel)` (catalog/index.c, file-static): does the
/// relation already have a primary-key index?
///
/// ```c
/// static bool
/// relationHasPrimaryKey(Relation rel)
/// {
///     bool result = false;
///     List *indexoidlist = RelationGetIndexList(rel);
///     ListCell *indexoidscan;
///     foreach(indexoidscan, indexoidlist) {
///         Oid indexoid = lfirst_oid(indexoidscan);
///         HeapTuple indexTuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(indexoid));
///         if (!HeapTupleIsValid(indexTuple)) elog(ERROR, "cache lookup failed for index %u", indexoid);
///         result = ((Form_pg_index) GETSTRUCT(indexTuple))->indisprimary;
///         ReleaseSysCache(indexTuple);
///         if (result) break;
///     }
///     list_free(indexoidlist);
///     return result;
/// }
/// ```
///
/// `RelationGetIndexList` is the relcache seam; each index's `indisprimary` is
/// the `index_get_indisprimary` syscache projection (`Ok(None)` is the C
/// "should not happen" cache miss → the `elog(ERROR)`).
fn relationHasPrimaryKey<'mcx>(mcx: Mcx<'mcx>, rel: &Relation<'mcx>) -> PgResult<bool> {
    let indexoidlist = relcache::relation_get_index_list::call(mcx, rel)?;

    for &indexoid in indexoidlist.iter() {
        let result = match syscache::index_get_indisprimary::call(indexoid)? {
            Some(b) => b,
            None => {
                return elog_error(alloc::format!(
                    "cache lookup failed for index {indexoid}"
                ))
            }
        };
        if result {
            return Ok(true);
        }
    }

    Ok(false)
}

/// `index_check_primary_key(heapRel, indexInfo, is_alter_table, stmt)`
/// (catalog/index.c): apply the special checks before creating a PRIMARY KEY
/// index — no pre-existing primary key (for ALTER TABLE / partition-of), no
/// NULLS NOT DISTINCT, and every key column marked NOT NULL.
///
/// The `const IndexStmt *stmt` argument is unused by the function body (the C
/// keeps it only for symmetry with the historical signature), so it is omitted.
/// `relispartition` is the relcache `rd_rel_relispartition` read; the per-column
/// `attnotnull` (and `attname` for the error) is the `att_get_attnotnull` ATTNUM
/// syscache projection.
pub fn index_check_primary_key<'mcx>(
    mcx: Mcx<'mcx>,
    heap_rel: &Relation<'mcx>,
    index_info: &IndexInfo<'mcx>,
    is_alter_table: bool,
) -> PgResult<()> {
    /*
     * If ALTER TABLE or CREATE TABLE .. PARTITION OF, check that there isn't
     * already a PRIMARY KEY.
     */
    if (is_alter_table || relcache::rd_rel_relispartition::call(heap_rel)?)
        && relationHasPrimaryKey(mcx, heap_rel)?
    {
        return Err(PgError::error(alloc::format!(
            "multiple primary keys for table \"{}\" are not allowed",
            heap_rel.name()
        )));
    }

    /*
     * Indexes created with NULLS NOT DISTINCT cannot be used for primary key
     * constraints.
     */
    if index_info.ii_NullsNotDistinct {
        return Err(PgError::error(alloc::string::String::from(
            "primary keys cannot use NULLS NOT DISTINCT indexes",
        )));
    }

    /*
     * Check that all of the attributes in a primary key are marked as not null.
     */
    for i in 0..index_info.ii_NumIndexKeyAttrs as usize {
        let attnum = index_info.ii_IndexAttrNumbers[i];

        if attnum == 0 {
            return Err(PgError::error(alloc::string::String::from(
                "primary keys cannot be expressions",
            )));
        }

        /* System attributes are never null, so no need to check */
        if attnum < 0 {
            continue;
        }

        match syscache::att_get_attnotnull::call(mcx, heap_rel.rd_id, attnum)? {
            None => {
                return elog_error(alloc::format!(
                    "cache lookup failed for attribute {attnum} of relation {}",
                    heap_rel.rd_id
                ))
            }
            Some((attnotnull, attname)) => {
                if !attnotnull {
                    return Err(PgError::error(alloc::format!(
                        "primary key column \"{}\" is not marked NOT NULL",
                        attname.as_str()
                    )));
                }
            }
        }
    }

    Ok(())
}

/* ===========================================================================
 * ConstructTupleDescriptor / InitializeAttributeOids / AppendAttributeTuples
 * ========================================================================= */

/// `ConstructTupleDescriptor(heapRelation, indexInfo, indexColNames,
/// accessMethodId, collationIds, opclassIds)` (catalog/index.c, file-static):
/// build the tuple descriptor for a new index's tuples.
///
/// Faithful to the C: per index column, copy the pg_attribute fields from the
/// parent heap column (simple columns) or look up the expression result type
/// (expression columns), then override the type with the opclass/AM keytype if
/// either provides one. `GetIndexAmRoutineByAmId` is the amapi seam (for
/// `amkeytype`); the heap descriptor is the relcache `relation_get_descr`
/// read; `exprType`/`exprTypmod` are the nodeFuncs `expr_type_info` seam;
/// `CheckAttributeType` and the pg_type/pg_opclass lookups +
/// `get_base_element_type` are their respective seams. `CreateTemplateTupleDesc`
/// / `populate_compact_attribute` are the common-tupdesc helpers.
#[allow(clippy::too_many_arguments)]
fn ConstructTupleDescriptor<'mcx>(
    mcx: Mcx<'mcx>,
    heap_relation: &Relation<'mcx>,
    index_info: &IndexInfo<'mcx>,
    index_col_names: &[alloc::string::String],
    access_method_id: Oid,
    collation_ids: &[Oid],
    opclass_ids: &[Oid],
) -> PgResult<types_tuple::heaptuple::TupleDescData<'mcx>> {
    let numatts = index_info.ii_NumIndexAttrs as usize;
    let numkeyatts = index_info.ii_NumIndexKeyAttrs as usize;

    /* We need access to the index AM's API struct */
    let amroutine = amapi::get_index_am_routine_by_amid::call(access_method_id)?;

    /* ... and to the table's tuple descriptor */
    let heap_tup_desc = relcache::relation_get_descr::call(mcx, heap_relation)?;
    let natts = relcache::rd_rel_relnatts::call(heap_relation)? as i32;

    /* allocate the new tuple descriptor */
    let mut index_tup_desc = backend_access_common_tupdesc::CreateTemplateTupleDesc(mcx, numatts as i32)?;

    /* the expression list is walked in parallel with the columns */
    let exprs_empty: alloc::vec::Vec<types_nodes::primnodes::Expr> = alloc::vec::Vec::new();
    let exprs: &[types_nodes::primnodes::Expr] = match &index_info.ii_Expressions {
        Some(v) => v.as_slice(),
        None => &exprs_empty,
    };
    let mut indexpr_idx: usize = 0;
    let mut colnames_idx: usize = 0;

    for i in 0..numatts {
        let atnum = index_info.ii_IndexAttrNumbers[i];

        // MemSet(to, 0, ATTRIBUTE_FIXED_PART_SIZE) — start from a zeroed attr.
        let mut to = types_tuple::heaptuple::FormData_pg_attribute::default();
        to.attnum = (i + 1) as i16;
        to.attislocal = true;
        to.attcollation = if i < numkeyatts { collation_ids[i] } else { InvalidOid };

        /* Set the attribute name as specified by caller. */
        if colnames_idx >= index_col_names.len() {
            return elog_error("too few entries in colnames list".into());
        }
        let this_colname = index_col_names[colnames_idx].clone();
        to.attname.namestrcpy(this_colname.as_str());
        colnames_idx += 1;

        /*
         * For simple index columns, copy some pg_attribute fields from the
         * parent relation. For expressions we look at the expression result.
         */
        if atnum != 0 {
            /* Simple index column */
            // Assert(atnum > 0)
            if atnum > natts as i16 {
                return elog_error(alloc::format!("invalid column number {atnum}"));
            }
            let from = heap_tup_desc.attr((atnum - 1) as usize);

            to.atttypid = from.atttypid;
            to.attlen = from.attlen;
            to.attndims = from.attndims;
            to.atttypmod = from.atttypmod;
            to.attbyval = from.attbyval;
            to.attalign = from.attalign;
            to.attstorage = from.attstorage;
            to.attcompression = from.attcompression;
        } else {
            /* Expressional index */
            if indexpr_idx >= exprs.len() {
                return elog_error("too few entries in indexprs list".into());
            }
            let indexkey = &exprs[indexpr_idx];
            indexpr_idx += 1;

            /* Lookup the expression type in pg_type for the type length etc. */
            let key_type_info = nodefuncs::expr_type_info::call(indexkey)?;
            let key_type = key_type_info.typid;
            let type_tup = match syscache::pg_type_form::call(key_type)? {
                Some(t) => t,
                None => return elog_error(alloc::format!("cache lookup failed for type {key_type}")),
            };

            to.atttypid = key_type;
            to.attlen = type_tup.typlen;
            to.atttypmod = key_type_info.typmod;
            to.attbyval = type_tup.typbyval;
            to.attalign = type_tup.typalign;
            to.attstorage = type_tup.typstorage;

            /* For expression columns, set attcompression invalid. */
            to.attcompression = INVALID_COMPRESSION_METHOD;

            /*
             * Make sure the expression yields a type that's safe to store in
             * an index.
             */
            heap::check_attribute_type::call(
                this_colname.as_str(),
                to.atttypid,
                to.attcollation,
            )?;
        }

        /*
         * We do not yet have the correct relation OID for the index, so just
         * set it invalid for now. InitializeAttributeOids() will fix it later.
         */
        to.attrelid = InvalidOid;

        /*
         * Check the opclass and index AM to see if either provides a keytype
         * (overriding the attribute type). Opclass (if exists) takes precedence.
         */
        let mut key_type = amroutine.amkeytype;

        if i < numkeyatts {
            let (opckeytype, opcintype, _opcname) =
                match syscache::pg_opclass_keytype::call(mcx, opclass_ids[i])? {
                    Some(t) => t,
                    None => {
                        return elog_error(alloc::format!(
                            "cache lookup failed for opclass {}",
                            opclass_ids[i]
                        ))
                    }
                };
            if OidIsValid(opckeytype) {
                key_type = opckeytype;
            }

            /*
             * If keytype is specified as ANYELEMENT, and opcintype is ANYARRAY,
             * then the attribute type must be an array; use its element type.
             */
            if key_type == ANYELEMENTOID && opcintype == ANYARRAYOID {
                key_type = lsyscache::get_base_element_type::call(to.atttypid)?;
                if !OidIsValid(key_type) {
                    return elog_error(alloc::format!(
                        "could not get element type of array type {}",
                        to.atttypid
                    ));
                }
            }
        }

        /*
         * If a key type different from the heap value is specified, update the
         * type-related fields in the index tupdesc.
         */
        if OidIsValid(key_type) && key_type != to.atttypid {
            let type_tup = match syscache::pg_type_form::call(key_type)? {
                Some(t) => t,
                None => return elog_error(alloc::format!("cache lookup failed for type {key_type}")),
            };

            to.atttypid = key_type;
            to.atttypmod = -1;
            to.attlen = type_tup.typlen;
            to.attbyval = type_tup.typbyval;
            to.attalign = type_tup.typalign;
            to.attstorage = type_tup.typstorage;
            /* As above, use the default compression method in this case */
            to.attcompression = INVALID_COMPRESSION_METHOD;
        }

        *index_tup_desc.attr_mut(i) = to;
        backend_access_common_tupdesc::populate_compact_attribute(&mut index_tup_desc, i)?;
    }

    Ok(index_tup_desc)
}

/// `AppendAttributeTuples(indexRelation, attopts, stattargets)`
/// (catalog/index.c): insert one `pg_attribute` row per index column. This is
/// the catalog-write leg; it routes to the catalog-indexing `append_attribute_-
/// tuples` seam (which owns `InsertPgAttributeTuples` and the index's stored
/// `RelationGetDescr`). `InitializeAttributeOids` runs inside that seam (it
/// scribbles `attrelid` on the index's relcache descriptor before the insert);
/// here at the `index_create` call site `attopts`/`stattargets` are both NULL.
fn AppendAttributeTuples<'mcx>(
    mcx: Mcx<'mcx>,
    index_relation: &Relation<'mcx>,
) -> PgResult<()> {
    indexing::append_attribute_tuples::call(mcx, index_relation, None, None)
}

/* ===========================================================================
 * BuildDummyIndexInfo
 * ========================================================================= */

/// `BuildDummyIndexInfo(index)` (catalog/index.c): like [`BuildIndexInfo`] but
/// never runs user code — it uses `RelationGetDummyIndexExpressions` (null
/// `Const`s of the right type) for expressions and ignores predicate /
/// exclusion. Used when truncating an index (only the rowtype tupdesc is
/// needed).
pub fn BuildDummyIndexInfo<'mcx>(
    mcx: Mcx<'mcx>,
    index: &Relation<'mcx>,
) -> PgResult<IndexInfo<'mcx>> {
    /* check the number of keys, and copy attr numbers into the IndexInfo */
    let num_atts = relcache::rd_index_indnatts::call(index)?.unwrap_or(0) as i32;
    if num_atts < 1 || num_atts > INDEX_MAX_KEYS as i32 {
        return elog_error(alloc::format!(
            "invalid indnatts {num_atts} for index {}",
            index.rd_id
        ));
    }

    let indnkeyatts = relcache::rd_index_indnkeyatts::call(index)?.unwrap_or(0) as i32;
    let relam = relcache::rd_rel_relam::call(index)?;
    let indisunique = relcache::rd_index_indisunique::call(index)?;
    let indnullsnotdistinct = relcache::rd_index_indnullsnotdistinct::call(index)?;
    let indisready = relcache::rd_index_indisready::call(index)?;
    let indisexclusion = relcache::rd_index_indisexclusion::call(index)?;

    let amsummarizing = match relcache::relation_rd_indam::call(index.rd_id) {
        Some(amroutine) => amroutine.amsummarizing,
        None => false,
    };

    /*
     * Create the node, using dummy index expressions, and pretending there is
     * no predicate.
     */
    let ii_Expressions = relcache::relation_get_dummy_index_expressions::call(mcx, index)?;

    let mut ii = IndexInfo {
        ii_NumIndexAttrs: num_atts,
        ii_NumIndexKeyAttrs: indnkeyatts,
        ii_Am: relam,
        ii_Expressions,
        ii_Predicate: None,
        ii_Unique: indisunique,
        ii_NullsNotDistinct: indnullsnotdistinct,
        ii_ReadyForInserts: indisready,
        ii_Concurrent: false,
        ii_Summarizing: amsummarizing,
        ii_WithoutOverlaps: indisexclusion && indisunique,
        ii_IndexAttrNumbers: Default::default(),
        ..Default::default()
    };

    /* fill in attribute numbers */
    let indkey = relcache::rd_index_indkey::call(index)?.unwrap_or_default();
    for i in 0..num_atts as usize {
        ii.ii_IndexAttrNumbers[i] = indkey[i];
    }

    /* We ignore the exclusion constraint if any */

    Ok(ii)
}

/* ===========================================================================
 * CompareIndexInfo
 * ========================================================================= */

/// `CompareIndexInfo(info1, info2, collations1, collations2, opfamilies1,
/// opfamilies2, attmap)` (catalog/index.c): are the two indexes (in different
/// tables) the "same" definition, mapping `info2`'s columns through `attmap`?
///
/// `map_variable_attnos` over `info2`'s expression / predicate lists is the
/// rewriteManip `map_variable_attnos_expr_list` seam; the structural-equality
/// check is the equalfuncs `equal_expr_list` seam. `attmap` is the
/// `AttrMap.attnums` slice (`maplen` = its length).
#[allow(clippy::too_many_arguments)]
pub fn CompareIndexInfo<'mcx>(
    mcx: Mcx<'mcx>,
    info1: &IndexInfo<'mcx>,
    info2: &IndexInfo<'mcx>,
    collations1: &[Oid],
    collations2: &[Oid],
    opfamilies1: &[Oid],
    opfamilies2: &[Oid],
    attmap: &[AttrNumber],
) -> PgResult<bool> {
    if info1.ii_Unique != info2.ii_Unique {
        return Ok(false);
    }
    if info1.ii_NullsNotDistinct != info2.ii_NullsNotDistinct {
        return Ok(false);
    }
    /* indexes are only equivalent if they have the same access method */
    if info1.ii_Am != info2.ii_Am {
        return Ok(false);
    }
    /* and same number of attributes */
    if info1.ii_NumIndexAttrs != info2.ii_NumIndexAttrs {
        return Ok(false);
    }
    /* and same number of key attributes */
    if info1.ii_NumIndexKeyAttrs != info2.ii_NumIndexKeyAttrs {
        return Ok(false);
    }

    /*
     * and columns match through the attribute map (actual attribute numbers
     * might differ!)
     */
    for i in 0..info1.ii_NumIndexAttrs as usize {
        if (attmap.len() as i16) < info2.ii_IndexAttrNumbers[i] {
            return elog_error("incorrect attribute map".into());
        }

        /* ignore expressions for now (but check their collation/opfamily) */
        if !(info1.ii_IndexAttrNumbers[i] == 0 && info2.ii_IndexAttrNumbers[i] == 0) {
            /* fail if just one index has an expression in this column */
            if info1.ii_IndexAttrNumbers[i] == 0 || info2.ii_IndexAttrNumbers[i] == 0 {
                return Ok(false);
            }

            /* both are columns, so check for match after mapping */
            if attmap[(info2.ii_IndexAttrNumbers[i] - 1) as usize] != info1.ii_IndexAttrNumbers[i] {
                return Ok(false);
            }
        }

        /* collation and opfamily are not valid for included columns */
        if i >= info1.ii_NumIndexKeyAttrs as usize {
            continue;
        }

        if collations1[i] != collations2[i] {
            return Ok(false);
        }
        if opfamilies1[i] != opfamilies2[i] {
            return Ok(false);
        }
    }

    /*
     * For expression indexes: either both are expression indexes, or neither
     * is; if they are, make sure the expressions match.
     */
    let e1 = info1.ii_Expressions.as_ref().map(|v| !v.is_empty()).unwrap_or(false);
    let e2 = info2.ii_Expressions.as_ref().map(|v| !v.is_empty()).unwrap_or(false);
    if e1 != e2 {
        return Ok(false);
    }
    if e1 {
        // map_variable_attnos((Node *) info2->ii_Expressions, 1, 0, attmap, ...)
        let mut cloned = mcx::vec_with_capacity_in(mcx, info2.ii_Expressions.as_ref().unwrap().len())?;
        for e in info2.ii_Expressions.as_ref().unwrap().iter() {
            cloned.push(e.clone_in(mcx)?);
        }
        let (mapped, found_whole_row) =
            rewritemanip::map_variable_attnos_expr_list::call(mcx, cloned, attmap)?;
        if found_whole_row {
            return Ok(false);
        }
        if !equalfuncs::equal_expr_list::call(
            info1.ii_Expressions.as_ref().unwrap().as_slice(),
            mapped.as_slice(),
        ) {
            return Ok(false);
        }
    }

    /* Partial index predicates must be identical, if they exist */
    let p1 = info1.ii_Predicate.is_some();
    let p2 = info2.ii_Predicate.is_some();
    if p1 != p2 {
        return Ok(false);
    }
    if p1 {
        let mut cloned = mcx::vec_with_capacity_in(mcx, info2.ii_Predicate.as_ref().unwrap().len())?;
        for e in info2.ii_Predicate.as_ref().unwrap().iter() {
            cloned.push(e.clone_in(mcx)?);
        }
        let (mapped, found_whole_row) =
            rewritemanip::map_variable_attnos_expr_list::call(mcx, cloned, attmap)?;
        if found_whole_row {
            return Ok(false);
        }
        if !equalfuncs::equal_expr_list::call(
            info1.ii_Predicate.as_ref().unwrap().as_slice(),
            mapped.as_slice(),
        ) {
            return Ok(false);
        }
    }

    /* No support currently for comparing exclusion indexes. */
    if info1.ii_ExclusionOps.is_some() || info2.ii_ExclusionOps.is_some() {
        return Ok(false);
    }

    Ok(true)
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

    // BuildSpeculativeIndexInfo (ExecOpenIndices speculative; logical-rep
    // conflict detection) — the per-key unique-operator lookup over the index
    // opclasses (relcache rd_opfamily/rd_opcintype + amapi
    // IndexAmTranslateCompareType + lsyscache get_opfamily_member/get_opcode).
    index_seam::build_speculative_index_info::set(BuildSpeculativeIndexInfo);

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
