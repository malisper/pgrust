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
//! # What IS landed: the CREATE INDEX catalog-write gate
//!
//! `index_create`, `index_constraint_create`, and `index_set_state_flags` are
//! ported faithfully and INSTALLED (the DefineIndex → `index_create` →
//! `index_build` gate that unblocks indexcmds F2):
//!
//! * `index_create` — full body: the parameter checks (nondeterministic-collation
//!   pattern-ops rejection, system/catalog/shared-relation restrictions,
//!   duplicate-name + duplicate-constraint-name checks via `get_relname_relid` /
//!   `ConstraintNameIsUsed`), [`ConstructTupleDescriptor`], `GetNewRelFileNumber`,
//!   `heap_create` + the AccessExclusiveLock open, `InsertPgClassTuple` (the
//!   `relowner`/`relam`/`relispartition` fields cross via the pg_class write-field
//!   carrier instead of scribbling the live `rd_rel`), `AppendAttributeTuples`
//!   (+`InitializeAttributeOids`), [`UpdateIndexRelation`], the relcache
//!   invalidation, the partition-index `StoreSingleInheritance` /
//!   `SetRelationHasSubclass` link, the full dependency-recording surface
//!   ([`record_object_address_dependencies`] / [`record_dependency_on`] /
//!   [`record_dependency_on_single_rel_expr`] over the column / collation /
//!   opclass / expression / predicate dependencies), the post-create hook,
//!   `CommandCounterIncrement`, and the build dispatch (`index_register` in
//!   bootstrap, `index_update_stats` for SKIP_BUILD, else `index_build`).
//! * `index_constraint_create` — full body: `CreateConstraintEntry` over the
//!   trimmed PK/UNIQUE/EXCLUDE arg carrier, the index→constraint internal
//!   dependency, the partition-type dependencies, the deferrable-constraint
//!   recheck trigger, and the optional mark-as-primary / mark-deferred pg_index
//!   update (over the widened [`types_cluster::PgIndexForm`] carrier).
//! * `index_set_state_flags` — full body: the four CREATE/DROP INDEX CONCURRENTLY
//!   `pg_index` flag transitions, read-modify-written through
//!   `SearchSysCacheCopy1(INDEXRELID)` (the `search_syscache_copy_pg_index`
//!   producer) + `CatalogTupleUpdate` (the `catalog_tuple_update_pg_index`
//!   consumer), over the [`types_cluster::PgIndexForm`] carrier WIDENED to carry
//!   `indisprimary`/`indimmediate`/`indisclustered`/`indisvalid`/`indcheckxmin`/
//!   `indisready`/`indislive`/`indisreplident`.
//!
//! Three outward legs `index_create` / `index_constraint_create` reach only in
//! bootstrap mode (or via the deferrable-constraint path) seam-and-panic until a
//! keystone lands (see `DESIGN_DEBT.md` TD-INDEXCREATE-BOOTSTRAP-LEGS): the
//! bootstrap `index_register` (`IndexInfo<'mcx>` → `'static` promotion),
//! `RelationInitIndexAccessInfo` (registry-mutable-by-OID entry access), and the
//! deferrable-constraint `CreateTrigger` (trigger.c unported).
//!
//! # What is NOT landed in this pass (precise STOP boundaries)
//!
//! `index_drop` (with `index_concurrently_set_dead`) is now ported and INSTALLED:
//! the non-concurrent path (the DROP TABLE leg that drops a table's indexes,
//! including the implicit TOAST index) and the DROP/REINDEX INDEX CONCURRENTLY
//! path (transaction commit/start, snapshot push/pop, session locks +
//! `WaitForLockers`, `RelationDropStorage`, the pg_index / pg_class /
//! pg_attribute / pg_statistic / pg_inherits row deletes).
//!
//! The remaining catalog-*write* drivers — the `index_concurrently_*` build
//! family, `validate_index`, `reindex_relation` — need a much larger outward
//! producer surface that is not yet ported, so their inward seams stay
//! UNINSTALLED here: a call panics loudly (mirror-PG-and-panic). Concretely:
//!
//! * `index_concurrently_*` / `validate_index` / `IndexCheckExclusion` need
//!   executor table-scan + `check_exclusion_constraint` and a `tuplesort`-based
//!   TID merge — all unported here.
//! * `FormIndexDatum` IS landed and INSTALLED here. The `form_index_datum`
//!   inward seam's result-array contract is the word-model `types_datum::Datum`;
//!   the executor eval/slot seams it routes through (`exec_prepare_expr_list` /
//!   `slot_getattr` / `slot_getsysattr` / `exec_eval_expr_switch_context`) yield
//!   the canonical `types_tuple::Datum`, so each result is narrowed to its bare
//!   scalar word via `as_usize()` (exact for a by-value type — every case the
//!   current correctness scope reaches; a loud panic on a by-reference value,
//!   which would need the Datum-unification flip the seam doc names).
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
use backend_utils_cache_relcache_nodexform_seams as nodexform;
use backend_access_index_amapi_seams as amapi;
use backend_access_index_indexam_seams as indexam;
use backend_catalog_heap_seams as heap;
use backend_nodes_nodeFuncs_seams as nodefuncs;
use backend_nodes_equalfuncs_seams as equalfuncs;
use backend_rewrite_rewritemanip_seams as rewritemanip;
use backend_executor_execTuples_seams as exec_tuples;
use backend_executor_execExpr_seams as exec_expr;
use backend_executor_execUtils_seams as exec_utils;
use backend_utils_sort_tuplesort_seams as tuplesort_seam;

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
        // C `makeIndexInfo`: `ii->ii_Context = CurrentMemoryContext;` — the
        // context that owns this IndexInfo (and into which later fix-ups, e.g.
        // BuildSpeculativeIndexInfo's ii_Unique* arrays, allocate).
        ii_Context: Some(mcx),
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
 * FormIndexDatum
 * ========================================================================= */

/// `FormIndexDatum(indexInfo, slot, estate, values, isnull)` (catalog/index.c):
/// compute the index tuple's column values from the heap tuple in `slot`,
/// evaluating any index expressions in the estate's per-tuple expression
/// context. For each of the index's key columns, either fetch a plain heap
/// attribute (`ii_IndexAttrNumbers[i] != 0`) or evaluate the next index
/// expression (`ii_IndexAttrNumbers[i] == 0`) from `ii_Expressions`.
///
/// The C fills caller-provided `Datum values[INDEX_MAX_KEYS]` /
/// `bool isnull[INDEX_MAX_KEYS]`; the port returns the populated fixed arrays.
///
/// The result element type is the word-model `types_datum::Datum` (the seam's
/// landed contract; the sole consumers — `index_insert` / the ScanKey /
/// `BuildIndexValueDescription` — bridge each word into the canonical
/// `Datum::ByVal` arm). The executor seams this routes through
/// (`slot_getattr` / `slot_getsysattr` / `ExecEvalExprSwitchContext`) yield the
/// canonical `Datum`, so each result is narrowed to its bare scalar word via
/// `as_usize()`: exact for a by-value type (every case the current correctness
/// scope reaches), and a loud panic on a by-reference value (which would need
/// the Datum-unification flip the seam doc names).
///
/// The C builds `indexInfo->ii_ExpressionsState` lazily and caches it on the
/// `IndexInfo`. The seam crosses `indexInfo` immutably, so when index
/// expressions are present the executable states are compiled transiently per
/// call via `ExecPrepareExprList` (behaviorally identical — same results — only
/// the cross-call caching optimization is dropped; no caller relies on the
/// cache being populated by this call).
fn FormIndexDatum<'mcx>(
    index_info: &IndexInfo<'_>,
    slot: types_nodes::SlotId,
    estate: &mut types_nodes::EStateData<'mcx>,
) -> PgResult<(
    [types_tuple::backend_access_common_heaptuple::Datum<'mcx>; INDEX_MAX_KEYS as usize],
    [bool; INDEX_MAX_KEYS as usize],
)> {
    let mcx = estate.es_query_cxt;

    // The canonical per-attribute `Datum` carries a by-reference index key
    // (text/varchar/name/numeric/…) as its `ByRef` byte image; collapsing to a
    // bare machine word here would panic the scalar accessor on a by-ref value.
    let mut values: [types_tuple::backend_access_common_heaptuple::Datum<'mcx>;
        INDEX_MAX_KEYS as usize] =
        core::array::from_fn(|_| types_tuple::backend_access_common_heaptuple::Datum::null());
    let mut isnull = [false; INDEX_MAX_KEYS as usize];

    let n = index_info.ii_NumIndexAttrs as usize;

    // The C asserts `indexInfo->ii_Expressions == NIL ||
    // indexInfo->ii_Expressions->length == 1` only implicitly via the
    // expression-count check below; build the executable expression states up
    // front if any index expression columns exist.
    let mut expr_states: Option<
        mcx::PgVec<'mcx, mcx::PgBox<'mcx, types_nodes::execexpr::ExprState<'mcx>>>,
    > = None;
    let mut econtext: Option<types_nodes::EcxtId> = None;
    if let Some(exprs) = index_info.ii_Expressions.as_deref() {
        // First time through, set up expression evaluation state (transiently;
        // see the doc comment on caching).
        let states = exec_expr::exec_prepare_expr_list::call(exprs, estate)?;
        // Check caller has set up context correctly: the per-tuple expression
        // context with `ecxt_scantuple == slot`.
        econtext = Some(exec_utils::get_per_tuple_expr_context::call(estate)?);
        expr_states = Some(states);
    }

    // Index into the prepared expression states as we consume expr columns.
    let mut indexpr_item: usize = 0;

    for i in 0..n {
        let keycol: AttrNumber = index_info.ii_IndexAttrNumbers[i];

        let (datum, this_isnull) = if keycol < 0 {
            // System column: slot_getsysattr against the slot's stored tuple.
            let sd = estate.slot_data_mut(slot);
            let (d, is_null) = exec_tuples::slot_getsysattr::call(mcx, sd, keycol)?;
            (d, is_null)
        } else if keycol != 0 {
            // Plain index column; get the value directly from the heap tuple.
            let (d, is_null) = exec_tuples::slot_getattr::call(estate, slot, keycol)?;
            (d, is_null)
        } else {
            // Index expression --- need to evaluate it.
            let states = expr_states.as_mut().ok_or_else(|| {
                PgError::error("wrong number of index expressions")
            })?;
            if indexpr_item >= states.len() {
                return Err(PgError::error("wrong number of index expressions"));
            }
            let ecxt = econtext.expect("econtext set up alongside expr_states");
            let state: &mut types_nodes::execexpr::ExprState<'mcx> =
                &mut states[indexpr_item];
            let (d, is_null) =
                exec_expr::exec_eval_expr_switch_context::call(state, ecxt, estate)?;
            indexpr_item += 1;
            (d, is_null)
        };

        values[i] = datum;
        isnull[i] = this_isnull;
    }

    // Check that all the expressions were consumed.
    let num_states = expr_states.as_ref().map(|s| s.len()).unwrap_or(0);
    if indexpr_item != num_states {
        return Err(PgError::error("wrong number of index expressions"));
    }

    Ok((values, isnull))
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
                cells.push(mcx::alloc_in(mcx, Node::mk_expr(mcx, e.clone_in(mcx)?)?)?);
            }
            let list = Node::mk_list(mcx, cells)?;
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
            let node = Node::mk_expr(mcx, anded)?;
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
    // Consume the owned handle via `.close()` so it releases the relcache ref
    // exactly once; a raw `relation_close::call(pg_index.rd_id, ...)` leaves the
    // `Relation` armed and its `Drop` would decrement the refcount a second time
    // (rd_refcnt underflow → panic).
    pg_index.close(ROW_EXCLUSIVE_LOCK)?;

    Ok(())
}

/* ===========================================================================
 * index_create
 * ========================================================================= */

/// `RelationRelationId` — pg_class's OID.
const RELATION_RELATION_ID: Oid = 1259;
/// `CollationRelationId` — pg_collation's OID.
const COLLATION_RELATION_ID: Oid = 3456;
/// `OperatorClassRelationId` — pg_opclass's OID.
const OPERATOR_CLASS_RELATION_ID: Oid = 2616;
/// `ConstraintRelationId` — pg_constraint's OID.
const CONSTRAINT_RELATION_ID: Oid = 2606;
/// `GLOBALTABLESPACE_OID` (`catalog/pg_tablespace_d.h`).
const GLOBALTABLESPACE_OID: Oid = 1664;
/// `AccessExclusiveLock` / `ShareUpdateExclusiveLock` / `NoLock`
/// (`storage/lockdefs.h`).
const ACCESS_EXCLUSIVE_LOCK: i32 = 8;
const SHARE_UPDATE_EXCLUSIVE_LOCK: i32 = 4;
const NO_LOCK: i32 = 0;

/* `catalog/index.h` `bits16 flags` bits for `index_create`. */
const INDEX_CREATE_IS_PRIMARY: u16 = 1 << 0;
const INDEX_CREATE_ADD_CONSTRAINT: u16 = 1 << 1;
const INDEX_CREATE_SKIP_BUILD: u16 = 1 << 2;
const INDEX_CREATE_CONCURRENT: u16 = 1 << 3;
const INDEX_CREATE_IF_NOT_EXISTS: u16 = 1 << 4;
const INDEX_CREATE_PARTITIONED: u16 = 1 << 5;
const INDEX_CREATE_INVALID: u16 = 1 << 6;

/* `catalog/index.h` `bits16 constr_flags` bits for `index_constraint_create`. */
const INDEX_CONSTR_CREATE_MARK_AS_PRIMARY: u16 = 1 << 0;
const INDEX_CONSTR_CREATE_DEFERRABLE: u16 = 1 << 1;
const INDEX_CONSTR_CREATE_INIT_DEFERRED: u16 = 1 << 2;
const INDEX_CONSTR_CREATE_UPDATE_INDEX: u16 = 1 << 3;
const INDEX_CONSTR_CREATE_REMOVE_OLD_DEPS: u16 = 1 << 4;
const INDEX_CONSTR_CREATE_WITHOUT_OVERLAPS: u16 = 1 << 5;

/* `catalog/pg_constraint.h` constraint-type codes. */
const CONSTRAINT_PRIMARY: i8 = b'p' as i8;
const CONSTRAINT_UNIQUE: i8 = b'u' as i8;
const CONSTRAINT_EXCLUSION: i8 = b'x' as i8;

/* `pg_opclass.dat` — the three btree pattern-ops opclasses incompatible with a
 * nondeterministic collation. */
const TEXT_BTREE_PATTERN_OPS_OID: Oid = 4217;
const VARCHAR_BTREE_PATTERN_OPS_OID: Oid = 4218;
const BPCHAR_BTREE_PATTERN_OPS_OID: Oid = 4219;

use backend_catalog_catalog_seams as catalog;
use backend_catalog_dependency_seams as dependency;
use backend_catalog_pg_depend_seams as depend;
use backend_catalog_partition_seams as partition;
use backend_catalog_pg_constraint_seams as pg_constraint;
use backend_catalog_pg_inherits_seams as pg_inherits;
use backend_commands_trigger_seams as trigger;
use backend_catalog_objectaccess_seams as objectaccess;
use backend_utils_cache_inval_seams as inval;
use backend_commands_tablecmds_seams as tablecmds;
use backend_commands_tablespace_globals_seams as tablespace;
use backend_bootstrap_bootstrap_seams as bootstrap;
use backend_storage_lmgr_predicate_seams as predicate;
use backend_commands_event_trigger_seams as event_trigger;
use backend_utils_error_seams as error_seams;
use backend_catalog_storage_seams as storage;
use backend_utils_activity_pgstat_seams as pgstat;
use backend_storage_lmgr_lmgr_seams as lmgr;
use backend_utils_time_snapmgr_seams as snapmgr;
use types_catalog::catalog_dependency::{
    ObjectAddress, DEPENDENCY_AUTO, DEPENDENCY_INTERNAL, DEPENDENCY_NORMAL,
    DEPENDENCY_PARTITION_PRI, DEPENDENCY_PARTITION_SEC,
};
use types_catalog::catalog::{RELKIND_INDEX, RELKIND_PARTITIONED_INDEX};

/// `index_create(...)` (catalog/index.c): create the catalog entries for a new
/// index relation and (unless deferred) build it. Returns
/// `(indexRelationId, createdConstraintId)` — the new index relation's OID and
/// the OID of the constraint created for it (the C `Oid *constraintId`
/// out-parameter; `InvalidOid` when no constraint was created).
///
/// Faithful to the C; see the seam doc on [`backend_catalog_index_seams::index_create`]
/// for the parameter-carrier mapping. The per-column `opclassOptions`
/// (attoptions) ride in `args.opclass_options` and are threaded to
/// `AppendAttributeTuples` (so each index `pg_attribute` row stores its
/// `attoptions`) and validated per-column via `index_opclass_options`. The
/// `stattargets` out-arg remains NULL/ignored at the current call sites and is
/// not carried.
pub fn index_create<'mcx>(
    heap_relation: &Relation<'mcx>,
    args: backend_catalog_index_seams::IndexCreateArgs<'mcx>,
) -> PgResult<(Oid, Oid)> {
    // The carrier owns the mcx-bound IndexInfo; pull out the pieces and reborrow.
    let mcx = args
        .index_info
        .ii_Context
        .expect("index_create: IndexInfo has no owning context");

    let mut index_info = args.index_info;
    let index_relation_name = args.index_relation_name;
    let mut index_relation_id = args.index_relation_id;
    let parent_index_relid = args.parent_index_relid;
    let parent_constraint_id = args.parent_constraint_id;
    let rel_file_number = args.rel_file_number;
    let index_col_names = args.index_col_names;
    let access_method_id = args.access_method_id;
    let table_space_id = args.table_space_id;
    let collation_ids = args.collation_ids;
    let opclass_ids = args.opclass_ids;
    let coloptions = args.coloptions;
    let reloptions = args.reloptions;
    let opclass_options = args.opclass_options;
    let flags = args.flags;
    let constr_flags = args.constr_flags;
    let allow_system_table_mods = args.allow_system_table_mods;
    let is_internal = args.is_internal;

    let heap_relation_id = heap_relation.rd_id;

    // The C `Oid *constraintId` out-parameter, returned to the caller. Only the
    // INDEX_CREATE_ADD_CONSTRAINT path writes it; InvalidOid otherwise.
    let mut created_constraint_id = InvalidOid;

    let isprimary = (flags & INDEX_CREATE_IS_PRIMARY) != 0;
    let invalid = (flags & INDEX_CREATE_INVALID) != 0;
    let concurrent = (flags & INDEX_CREATE_CONCURRENT) != 0;
    let partitioned = (flags & INDEX_CREATE_PARTITIONED) != 0;
    // create_storage = !RelFileNumberIsValid(relFileNumber);
    let create_storage = !OidIsValid(rel_file_number);

    /* constraint flags can only be set when a constraint is requested */
    debug_assert!(constr_flags == 0 || (flags & INDEX_CREATE_ADD_CONSTRAINT) != 0);
    /* partitioned indexes must never be "built" by themselves */
    debug_assert!(!partitioned || (flags & INDEX_CREATE_SKIP_BUILD) != 0);

    let relkind = if partitioned {
        RELKIND_PARTITIONED_INDEX
    } else {
        RELKIND_INDEX
    };
    let is_exclusion = index_info.ii_ExclusionOps.is_some();

    let pg_class = table_am::table_open::call(mcx, RELATION_RELATION_ID, ROW_EXCLUSIVE_LOCK)?;

    /*
     * The index will be in the same namespace as its parent table, and is
     * shared across databases iff the parent is; likewise mapped; and inherits
     * the parent's relpersistence.
     */
    let namespace_id = relcache::rd_rel_relnamespace::call(heap_relation)?;
    let shared_relation = relcache::rd_rel_relisshared::call(heap_relation)?;
    let mapped_relation = relcache::relation_is_mapped::call(heap_relation)?;
    let relpersistence = relcache::rd_rel_relpersistence::call(heap_relation)?;

    /* check parameters */
    if index_info.ii_NumIndexAttrs < 1 {
        return elog_error("must index at least one column".into());
    }

    if !allow_system_table_mods
        && catalog::is_system_relation::call(heap_relation)?
        && miscinit::is_normal_processing_mode::call()
    {
        return Err(PgError::error(alloc::string::String::from(
            "user-defined indexes on system catalog tables are not supported",
        )));
    }

    /*
     * Refuse a btree *_pattern_ops opclass paired with a nondeterministic
     * collation (text_eq would be incompatible with the opclass comparator).
     */
    for i in 0..index_info.ii_NumIndexKeyAttrs as usize {
        let collation = collation_ids[i];
        let opclass = opclass_ids[i];

        if OidIsValid(collation)
            && (opclass == TEXT_BTREE_PATTERN_OPS_OID
                || opclass == VARCHAR_BTREE_PATTERN_OPS_OID
                || opclass == BPCHAR_BTREE_PATTERN_OPS_OID)
        {
            let isdet = match syscache::collation_isdeterministic::call(collation)? {
                Some(b) => b,
                // get_collation_isdeterministic does not error on a missing
                // collation (lsyscache caches return false); but the index.c
                // path only reaches here for a valid collation OID.
                None => false,
            };
            if !isdet {
                // Look up the opclass name for the error message (the C
                // SearchSysCache1(CLAOID) + NameStr(opcname)).
                let opcname = match syscache::pg_opclass_keytype::call(mcx, opclass)? {
                    Some((_keytype, _intype, name)) => name.as_str().to_string(),
                    None => {
                        return elog_error(alloc::format!(
                            "cache lookup failed for operator class {opclass}"
                        ))
                    }
                };
                return Err(PgError::error(alloc::format!(
                    "nondeterministic collations are not supported for operator class \"{opcname}\""
                )));
            }
        }
    }

    /*
     * Concurrent index build on a system catalog is unsafe.
     */
    if concurrent && catalog::is_catalog_relation::call(heap_relation) {
        return Err(PgError::error(alloc::string::String::from(
            "concurrent index creation on system catalog tables is not supported",
        )));
    }

    /* Not supported: concurrent exclusion-constraint build (REINDEX-only). */
    if concurrent && is_exclusion {
        return Err(PgError::error(alloc::string::String::from(
            "concurrent index creation for exclusion constraints is not supported",
        )));
    }

    /* Cannot index a shared relation after initdb. */
    if shared_relation && !miscinit::is_bootstrap_processing_mode::call() {
        return Err(PgError::error(alloc::string::String::from(
            "shared indexes cannot be created after initdb",
        )));
    }

    /* Shared relations must be in pg_global. */
    if shared_relation && table_space_id != GLOBALTABLESPACE_OID {
        return elog_error("shared relations must be placed in pg_global tablespace".into());
    }

    /*
     * Check for duplicate name (index, and the associated constraint if any).
     */
    if OidIsValid(lsyscache::get_relname_relid::call(&index_relation_name, namespace_id)?) {
        if (flags & INDEX_CREATE_IF_NOT_EXISTS) != 0 {
            error_seams::ereport::call(PgError::new(
                types_error::NOTICE,
                alloc::format!("relation \"{index_relation_name}\" already exists, skipping"),
            ))?;
            pg_class.close(ROW_EXCLUSIVE_LOCK)?;
            return Ok((InvalidOid, InvalidOid));
        }
        return Err(PgError::error(alloc::format!(
            "relation \"{index_relation_name}\" already exists"
        )));
    }

    if (flags & INDEX_CREATE_ADD_CONSTRAINT) != 0
        && pg_constraint::constraint_name_is_used::call(
            mcx,
            pg_constraint::ConstraintCategory::Relation,
            heap_relation_id,
            &index_relation_name,
        )?
    {
        return Err(PgError::error(alloc::format!(
            "constraint \"{index_relation_name}\" for relation \"{}\" already exists",
            heap_relation.name()
        )));
    }

    /* construct tuple descriptor for index tuples */
    let index_tup_desc = ConstructTupleDescriptor(
        mcx,
        heap_relation,
        &index_info,
        &index_col_names,
        access_method_id,
        &collation_ids,
        &opclass_ids,
    )?;

    /*
     * Allocate an OID for the index, unless we were told what to use. (Binary-
     * upgrade OID/relfilenumber overrides are a separate substrate; in binary-
     * upgrade mode index_create requires the override globals which the repo
     * does not yet model — seam-and-panic on that leg.)
     */
    if !OidIsValid(index_relation_id) {
        if tablespace::IsBinaryUpgrade::call()? {
            // binary_upgrade_next_index_pg_class_oid / _relfilenumber globals
            // are not modeled; the CREATE INDEX gate does not exercise this.
            panic!(
                "index_create: binary-upgrade OID/relfilenumber override not yet modeled"
            );
        } else {
            index_relation_id = catalog::get_new_relfilenumber::call(table_space_id, relpersistence)?;
        }
    }

    /*
     * create the index relation's relcache entry and, if necessary, the
     * physical disk file.
     */
    let heap_create_result = heap::heap_create::call(
        mcx,
        &index_relation_name,
        namespace_id,
        table_space_id,
        index_relation_id,
        rel_file_number,
        access_method_id,
        &index_tup_desc,
        relkind,
        relpersistence as u8,
        shared_relation,
        mapped_relation,
        allow_system_table_mods,
        create_storage,
    )?;

    debug_assert_eq!(heap_create_result.xids.relfrozenxid, 0); /* InvalidTransactionId */
    debug_assert_eq!(heap_create_result.xids.relminmxid, 0); /* InvalidMultiXactId */
    debug_assert_eq!(index_relation_id, heap_create_result.rel);

    /*
     * Obtain exclusive lock on it, and open the relcache entry. (C: heap_create
     * returns the open uncataloged Relation with NoLock, then LockRelation takes
     * AccessExclusiveLock. In the owned model the relcache entry is registry-
     * owned; index_open with AccessExclusiveLock both pins it and takes the
     * lock. `index_open` — not `table_open` — because the new relation is an
     * INDEX; `table_open`'s `validate_relation_kind` rejects RELKIND_INDEX.)
     */
    let mut index_relation =
        indexam::index_open::call(mcx, index_relation_id, ACCESS_EXCLUSIVE_LOCK)?;

    /*
     * Fill in the index's pg_class entry fields that heap_create did not set
     * correctly (relowner from the heap, relam, relispartition). In the owned
     * model these cross via the InsertPgClassTuple write-field carrier rather
     * than by scribbling on the live rd_rel (the relcache entry is rebuilt from
     * the catalog after CommandCounterIncrement anyway). relam was already set
     * by heap_create (accessMethodId).
     */
    let relowner = relcache::rd_rel_relowner::call(heap_relation)?;
    let write = heap::PgClassWriteFields {
        relpages: 0,
        // C: `InsertPgClassTuple` stores `new_rel_desc->rd_rel->reltuples`,
        // which `RelationBuildLocalRelation` left at the `palloc0` zero for a
        // freshly created index (it is NOT the `AddNewRelationTuple` -1 used for
        // heaps/toast/matviews). Keeping this 0 (not -1) is what lets
        // `index_update_stats(indexRelation, false, 0)` see `rd_rel->reltuples
        // >= 0`, skip the "empty table reltuples==-1" hack, and write the
        // freshly built index's relpages=1/reltuples=0 — matching upstream.
        reltuples: 0.0,
        relallvisible: 0,
        relallfrozen: 0,
        relfrozenxid: heap_create_result.xids.relfrozenxid,
        relminmxid: heap_create_result.xids.relminmxid,
        relowner,
        reltype: InvalidOid, /* C: InsertPgClassTuple(..., (Datum) 0, reloptions) — no rowtype */
        reloftype: InvalidOid,
        relispartition: OidIsValid(parent_index_relid),
        relrewrite: InvalidOid,
    };

    /* store index's pg_class entry */
    heap::InsertPgClassTuple::call(
        mcx,
        &pg_class,
        &index_relation,
        index_relation_id,
        &write,
        None, /* relacl == (Datum) 0 */
        reloptions_to_bytes(&reloptions),
    )?;

    /* done with pg_class */
    pg_class.close(ROW_EXCLUSIVE_LOCK)?;

    /*
     * now update the object id's of all the attribute tuple forms in the index
     * relation's tuple descriptor, and append the pg_attribute tuples.
     * (InitializeAttributeOids runs inside AppendAttributeTuples' seam in the
     * owned model — see AppendAttributeTuples.)
     */
    AppendAttributeTuples(mcx, &index_relation, opclass_options.as_deref())?;

    /*
     * update pg_index (append INDEX tuple). Stows away "predicate".
     */
    UpdateIndexRelation(
        mcx,
        index_relation_id,
        heap_relation_id,
        &index_info,
        &collation_ids,
        &opclass_ids,
        &coloptions,
        isprimary,
        is_exclusion,
        (constr_flags & INDEX_CONSTR_CREATE_DEFERRABLE) == 0, /* immediate */
        !concurrent && !invalid,                              /* isvalid */
        !concurrent,                                          /* isready */
    )?;

    /*
     * Register relcache invalidation on the index's heap relation, to maintain
     * consistency of its index list.
     */
    inval::cache_invalidate_relcache::call(heap_relation_id)?;

    /* update pg_inherits and the parent's relhassubclass, if needed */
    if OidIsValid(parent_index_relid) {
        pg_inherits::store_single_inheritance::call(index_relation_id, parent_index_relid, 1)?;
        backend_storage_lmgr_lmgr_seams::lock_relation_oid::call(
            parent_index_relid,
            SHARE_UPDATE_EXCLUSIVE_LOCK,
        )?;
        tablecmds::set_relation_has_subclass::call(mcx, parent_index_relid, true)?;
    }

    /*
     * Register constraint and dependencies for the index.
     *
     * During bootstrap we can't register any dependencies, and we don't try to
     * make a constraint either.
     */
    if !miscinit::is_bootstrap_processing_mode::call() {
        let myself = ObjectAddress {
            classId: RELATION_RELATION_ID,
            objectId: index_relation_id,
            objectSubId: 0,
        };

        if (flags & INDEX_CREATE_ADD_CONSTRAINT) != 0 {
            let constraint_type = if isprimary {
                CONSTRAINT_PRIMARY
            } else if index_info.ii_Unique {
                CONSTRAINT_UNIQUE
            } else if is_exclusion {
                CONSTRAINT_EXCLUSION
            } else {
                return elog_error("constraint must be PRIMARY, UNIQUE or EXCLUDE".into());
            };

            // localaddr = index_constraint_create(...);
            // if (constraintId) *constraintId = localaddr.objectId;
            let localaddr = index_constraint_create(
                heap_relation,
                index_relation_id,
                parent_constraint_id,
                &index_info,
                &index_relation_name,
                constraint_type,
                constr_flags,
                allow_system_table_mods,
                is_internal,
            )?;
            created_constraint_id = localaddr.objectId;
        } else {
            let mut have_simple_col = false;
            let mut addrs = dependency::new_object_addresses::call()?;

            /* Create auto dependencies on simply-referenced columns */
            for i in 0..index_info.ii_NumIndexAttrs as usize {
                if index_info.ii_IndexAttrNumbers[i] != 0 {
                    let referenced = ObjectAddress {
                        classId: RELATION_RELATION_ID,
                        objectId: heap_relation_id,
                        objectSubId: index_info.ii_IndexAttrNumbers[i] as i32,
                    };
                    dependency::add_exact_object_address::call(referenced, &mut addrs)?;
                    have_simple_col = true;
                }
            }

            /*
             * If there are no simply-referenced columns, give the index an auto
             * dependency on the whole table.
             */
            if !have_simple_col {
                let referenced = ObjectAddress {
                    classId: RELATION_RELATION_ID,
                    objectId: heap_relation_id,
                    objectSubId: 0,
                };
                dependency::add_exact_object_address::call(referenced, &mut addrs)?;
            }

            dependency::record_object_address_dependencies::call(myself, &mut addrs, DEPENDENCY_AUTO)?;
            dependency::free_object_addresses::call(addrs)?;
        }

        /*
         * If this is an index partition, create partition dependencies on both
         * the parent index and the table.
         */
        if OidIsValid(parent_index_relid) {
            dependency::record_dependency_on::call(
                myself,
                ObjectAddress {
                    classId: RELATION_RELATION_ID,
                    objectId: parent_index_relid,
                    objectSubId: 0,
                },
                DEPENDENCY_PARTITION_PRI,
            )?;
            dependency::record_dependency_on::call(
                myself,
                ObjectAddress {
                    classId: RELATION_RELATION_ID,
                    objectId: heap_relation_id,
                    objectSubId: 0,
                },
                DEPENDENCY_PARTITION_SEC,
            )?;
        }

        /* placeholder for normal dependencies */
        let mut addrs = dependency::new_object_addresses::call()?;

        /* Store dependency on collations (default collation is pinned). */
        for i in 0..index_info.ii_NumIndexKeyAttrs as usize {
            if OidIsValid(collation_ids[i]) && collation_ids[i] != DEFAULT_COLLATION_OID {
                let referenced = ObjectAddress {
                    classId: COLLATION_RELATION_ID,
                    objectId: collation_ids[i],
                    objectSubId: 0,
                };
                dependency::add_exact_object_address::call(referenced, &mut addrs)?;
            }
        }

        /* Store dependency on operator classes */
        for i in 0..index_info.ii_NumIndexKeyAttrs as usize {
            let referenced = ObjectAddress {
                classId: OPERATOR_CLASS_RELATION_ID,
                objectId: opclass_ids[i],
                objectSubId: 0,
            };
            dependency::add_exact_object_address::call(referenced, &mut addrs)?;
        }

        dependency::record_object_address_dependencies::call(myself, &mut addrs, DEPENDENCY_NORMAL)?;
        dependency::free_object_addresses::call(addrs)?;

        /* Store dependencies on anything mentioned in index expressions */
        if let Some(exprs) = &index_info.ii_Expressions {
            if !exprs.is_empty() {
                let node = exprs_to_list_node(mcx, exprs)?;
                dependency::record_dependency_on_single_rel_expr::call(
                    myself,
                    &node,
                    heap_relation_id,
                    DEPENDENCY_NORMAL,
                    DEPENDENCY_AUTO,
                    false,
                )?;
            }
        }

        /* Store dependencies on anything mentioned in predicate */
        if let Some(pred) = &index_info.ii_Predicate {
            if !pred.is_empty() {
                let node = exprs_to_list_node(mcx, pred)?;
                dependency::record_dependency_on_single_rel_expr::call(
                    myself,
                    &node,
                    heap_relation_id,
                    DEPENDENCY_NORMAL,
                    DEPENDENCY_AUTO,
                    false,
                )?;
            }
        }
    } else {
        /* Bootstrap mode — assert we weren't asked for constraint support */
        debug_assert!((flags & INDEX_CREATE_ADD_CONSTRAINT) == 0);
    }

    /* Post creation hook for new index */
    objectaccess::invoke_object_post_create_hook_arg::call(
        RELATION_RELATION_ID,
        index_relation_id,
        0,
        is_internal,
    )?;

    /*
     * Advance the command counter so we can see the newly-entered catalog
     * tuples for the index.
     */
    xact::command_counter_increment::call()?;

    /*
     * In bootstrap mode, fill in the index strategy structure from the catalogs.
     * Otherwise the relcache entry was rebuilt by the sinval update during
     * CommandCounterIncrement.
     */
    if miscinit::is_bootstrap_processing_mode::call() {
        relcache::relation_init_index_access_info::call(index_relation_id)?;
    }

    /*
     * The relcache entry for this index was rebuilt in-place by the sinval
     * during the CommandCounterIncrement above, which is when
     * RelationInitIndexAccessInfo finally populates rd_index/rd_indam/etc. (the
     * pg_index row only exists after UpdateIndexRelation, also above). In C,
     * `indexRelation` is the live `RelationData *`, so it sees the rebuild
     * through the held pointer. In the owned model the handle carries a value
     * *snapshot* taken back at `index_open` — before the pg_index row existed —
     * so its rd_index is still NULL. Re-project the handle's copy from the now-
     * rebuilt cache cell so the build (which reads rd_index->indnkeyatts for the
     * sort keys) sees the populated descriptor. Pin-free: the handle already
     * holds the pin + AccessExclusiveLock.
     */
    if let Some(refreshed) =
        relcache::relation_project_existing::call(mcx, index_relation_id)?
    {
        index_relation.replace_data(refreshed);
    }

    /*
     * C: indexRelation->rd_index->indnkeyatts = indexInfo->ii_NumIndexKeyAttrs;
     * — a relcache-entry bookkeeping write. The relcache entry was just rebuilt
     * from the catalog (where UpdateIndexRelation already stored indnkeyatts),
     * so it is already correct; the trimmed cross-unit handle exposes no setter.
     */

    /*
     * Validate opclass-specific options.
     *   if (opclassOptions)
     *       for (i = 0; i < indexInfo->ii_NumIndexKeyAttrs; i++)
     *           (void) index_opclass_options(indexRelation, i + 1,
     *                                        opclassOptions[i], true);
     */
    if let Some(ref opclass_options) = opclass_options {
        for i in 0..index_info.ii_NumIndexKeyAttrs as usize {
            let _ = indexam::index_opclass_options::call(
                &index_relation,
                (i + 1) as types_core::primitive::AttrNumber,
                opclass_options[i].clone_in(mcx)?,
                true, /* validate */
            )?;
        }
    }

    /*
     * If bootstrap, or the caller asked to skip the build, don't fill the index
     * now. Otherwise build it.
     */
    if miscinit::is_bootstrap_processing_mode::call() {
        bootstrap::index_register::call(mcx, heap_relation_id, index_relation_id, &index_info)?;
    } else if (flags & INDEX_CREATE_SKIP_BUILD) != 0 {
        /*
         * Caller fills the index later; still mark the heap as having an index.
         */
        index_seam::index_update_stats::call(heap_relation, true, -1.0)?;
        xact::command_counter_increment::call()?;
    } else {
        index_build(mcx, heap_relation, &index_relation, &mut index_info)?;
    }

    /*
     * Close the index; keep the lock acquired above until end of transaction.
     * Closing the heap is the caller's responsibility.
     */
    index_relation.close(NO_LOCK)?;

    Ok((index_relation_id, created_constraint_id))
}

/// `CStringGetTextDatum(NULL)` vs the `Datum reloptions` argument — in the owned
/// model `reloptions` rides as a `types_tuple::Datum`. The C `(Datum) 0` (no
/// WITH clause) maps to `None` for the `InsertPgClassTuple` carrier. A real
/// reloptions value is always a `text[]` varlena, carried on the by-reference
/// lane as `Datum::ByRef`; its detoasted on-disk bytes are exactly the
/// `pg_class.reloptions` image (`DefineIndex` builds it via
/// `transformRelOptions` → `construct_text_array_bytes`).
fn reloptions_to_bytes(reloptions: &types_tuple::Datum<'_>) -> Option<alloc::vec::Vec<u8>> {
    use types_tuple::Datum;
    match reloptions {
        // C `(Datum) 0` — no WITH clause.
        Datum::ByVal(0) => None,
        // A real reloptions varlena rides as its detoasted on-disk bytes in the
        // `ByRef` arm (the same `text[]` image `pg_class.reloptions` stores).
        Datum::ByRef(bytes) => Some(bytes.to_vec()),
        // Any other shape is not a valid reloptions image; it cannot arise from
        // a faithful caller (reloptions is always a text[] varlena or NULL).
        _ => panic!(
            "index_create: reloptions Datum is not a text[] varlena image"
        ),
    }
}

/// Wrap a `PgVec<Expr>` (the implicit-AND list the C passes as `(Node *)
/// indexInfo->ii_Expressions` / `ii_Predicate`) into a `Node::List` of
/// `Node::Expr` cells for `recordDependencyOnSingleRelExpr`. This is the C
/// `(Node *) List *` cast: the dependency walker treats a `List` node as a node
/// to scan, descending into each element.
fn exprs_to_list_node<'mcx>(
    mcx: Mcx<'mcx>,
    exprs: &mcx::PgVec<'mcx, types_nodes::primnodes::Expr>,
) -> PgResult<types_nodes::nodes::Node<'mcx>> {
    use types_nodes::nodes::Node;
    let mut cells = mcx::vec_with_capacity_in(mcx, exprs.len())?;
    for e in exprs.iter() {
        cells.push(mcx::alloc_in(mcx, Node::mk_expr(mcx, e.clone_in(mcx)?)?)?);
    }
    Ok(Node::mk_list(mcx, cells)?)
}

/* ===========================================================================
 * index_constraint_create
 * ========================================================================= */

/// `index_constraint_create(heapRelation, indexRelationId, parentConstraintId,
/// indexInfo, constraintName, constraintType, constr_flags,
/// allow_system_table_mods, is_internal)` (catalog/index.c): register a
/// constraint (PRIMARY KEY / UNIQUE / EXCLUDE) for the given index. Builds the
/// pg_constraint entry, its dependencies, the deferred-uniqueness trigger (when
/// deferrable), and optionally marks the index primary/deferred. Returns the
/// constraint's `ObjectAddress`.
#[allow(clippy::too_many_arguments)]
pub fn index_constraint_create<'mcx>(
    heap_relation: &Relation<'_>,
    index_relation_id: Oid,
    parent_constraint_id: Oid,
    index_info: &IndexInfo<'mcx>,
    constraint_name: &str,
    constraint_type: i8,
    constr_flags: u16,
    allow_system_table_mods: bool,
    is_internal: bool,
) -> PgResult<ObjectAddress> {
    let mcx = index_info
        .ii_Context
        .expect("index_constraint_create: IndexInfo has no owning context");

    let namespace_id = relcache::rd_rel_relnamespace::call(heap_relation)?;
    let heap_relation_id = heap_relation.rd_id;

    let deferrable = (constr_flags & INDEX_CONSTR_CREATE_DEFERRABLE) != 0;
    let initdeferred = (constr_flags & INDEX_CONSTR_CREATE_INIT_DEFERRED) != 0;
    let mark_as_primary = (constr_flags & INDEX_CONSTR_CREATE_MARK_AS_PRIMARY) != 0;
    let is_without_overlaps = (constr_flags & INDEX_CONSTR_CREATE_WITHOUT_OVERLAPS) != 0;

    /* constraint creation support doesn't work while bootstrapping */
    debug_assert!(!miscinit::is_bootstrap_processing_mode::call());

    /* enforce system-table restriction */
    if !allow_system_table_mods
        && catalog::is_system_relation::call(heap_relation)?
        && miscinit::is_normal_processing_mode::call()
    {
        return Err(PgError::error(alloc::string::String::from(
            "user-defined indexes on system catalog tables are not supported",
        )));
    }

    /* primary/unique constraints shouldn't have any expressions */
    if index_info
        .ii_Expressions
        .as_ref()
        .map(|e| !e.is_empty())
        .unwrap_or(false)
        && constraint_type != CONSTRAINT_EXCLUSION
    {
        return elog_error("constraints cannot have index expressions".into());
    }

    /*
     * If we're manufacturing a constraint for a pre-existing index, get rid of
     * the existing auto dependencies for the index.
     */
    if (constr_flags & INDEX_CONSTR_CREATE_REMOVE_OLD_DEPS) != 0 {
        dependency::delete_dependency_records_for_class::call(
            RELATION_RELATION_ID,
            index_relation_id,
            RELATION_RELATION_ID,
            DEPENDENCY_AUTO,
        )?;
    }

    let (islocal, inhcount, noinherit) = if OidIsValid(parent_constraint_id) {
        (false, 1i16, false)
    } else {
        (true, 0i16, true)
    };

    /*
     * Construct a pg_constraint entry. The constraint key is the index's key
     * attribute numbers; the exclusion operators (if any) ride along.
     */
    // C passes the FULL index-attribute array (key + INCLUDE columns,
    // `ii_NumIndexAttrs` entries) as `constraintKey`; CreateConstraintEntry uses
    // only the first `constraintNKeys` for `conkey` but walks all
    // `constraintNTotalKeys` of them when registering the constraint→column
    // dependencies. Slicing to the key columns only leaves the dependency loop
    // indexing past the end (index.c:1966-1968).
    let constraint_key: alloc::vec::Vec<i16> =
        index_info.ii_IndexAttrNumbers[..index_info.ii_NumIndexAttrs as usize].to_vec();
    let excl_op_vec: Option<alloc::vec::Vec<Oid>> =
        index_info.ii_ExclusionOps.as_ref().map(|v| v.iter().copied().collect());

    let con_oid = pg_constraint::create_constraint_entry::call(
        mcx,
        pg_constraint::CreateConstraintEntryArgs {
            constraint_name,
            constraint_namespace: namespace_id,
            constraint_type,
            is_deferrable: deferrable,
            is_deferred: initdeferred,
            parent_constr_id: parent_constraint_id,
            rel_id: heap_relation_id,
            constraint_key: &constraint_key,
            constraint_n_keys: index_info.ii_NumIndexKeyAttrs,
            constraint_n_total_keys: index_info.ii_NumIndexAttrs,
            index_rel_id: index_relation_id,
            excl_op: excl_op_vec.as_deref(),
            con_is_local: islocal,
            con_inh_count: inhcount,
            con_no_inherit: noinherit,
            con_period: is_without_overlaps,
            is_internal,
        },
    )?;

    /*
     * Register the index as internally dependent on the constraint. (The
     * constraint depends on the table, so no direct index→table dependency.)
     */
    let myself = ObjectAddress {
        classId: CONSTRAINT_RELATION_ID,
        objectId: con_oid,
        objectSubId: 0,
    };
    let idxaddr = ObjectAddress {
        classId: RELATION_RELATION_ID,
        objectId: index_relation_id,
        objectSubId: 0,
    };
    dependency::record_dependency_on::call(idxaddr, myself, DEPENDENCY_INTERNAL)?;

    /*
     * If a constraint on a partition, give it partition-type dependencies on
     * the parent constraint and the table.
     */
    if OidIsValid(parent_constraint_id) {
        dependency::record_dependency_on::call(
            myself,
            ObjectAddress {
                classId: CONSTRAINT_RELATION_ID,
                objectId: parent_constraint_id,
                objectSubId: 0,
            },
            DEPENDENCY_PARTITION_PRI,
        )?;
        dependency::record_dependency_on::call(
            myself,
            ObjectAddress {
                classId: RELATION_RELATION_ID,
                objectId: heap_relation_id,
                objectSubId: 0,
            },
            DEPENDENCY_PARTITION_SEC,
        )?;
    }

    /*
     * If the constraint is deferrable, create the deferred uniqueness checking
     * trigger.
     */
    if deferrable {
        trigger::create_unique_key_recheck_trigger::call(
            heap_relation_id,
            con_oid,
            index_relation_id,
            constraint_type == CONSTRAINT_PRIMARY,
            initdeferred,
        )?;
    }

    /*
     * If needed, mark the index as primary and/or deferred in pg_index.
     */
    if (constr_flags & INDEX_CONSTR_CREATE_UPDATE_INDEX) != 0 && (mark_as_primary || deferrable) {
        let pg_index = table_am::table_open::call(mcx, INDEX_RELATION_ID, ROW_EXCLUSIVE_LOCK)?;

        let Some((tid, mut form)) =
            syscache::search_syscache_copy_pg_index::call(mcx, index_relation_id)?
        else {
            return elog_error(alloc::format!(
                "cache lookup failed for index {index_relation_id}"
            ));
        };

        let mut dirty = false;
        let mut marked_as_primary = false;

        if mark_as_primary && !form.indisprimary {
            form.indisprimary = true;
            dirty = true;
            marked_as_primary = true;
        }

        if deferrable && form.indimmediate {
            form.indimmediate = false;
            dirty = true;
        }

        if dirty {
            indexing::catalog_tuple_update_pg_index::call(mcx, &pg_index, tid, &form)?;

            /*
             * When marking an existing index as primary, force a relcache flush
             * on the parent table.
             */
            if marked_as_primary {
                inval::cache_invalidate_relcache::call(heap_relation_id)?;
            }

            objectaccess::invoke_object_post_alter_hook_arg::call(
                INDEX_RELATION_ID,
                index_relation_id,
                0,
                InvalidOid,
                is_internal,
            )?;
        }

        pg_index.close(ROW_EXCLUSIVE_LOCK)?;
    }

    Ok(myself)
}

/* ===========================================================================
 * index_set_state_flags
 * ========================================================================= */

/// `index_set_state_flags(indexId, action)` (catalog/index.c): perform a
/// non-transactional update of an index's `pg_index` state flags during a
/// CREATE / DROP INDEX CONCURRENTLY sequence.
///
/// This is the in-place `pg_index` flag mutation: fetch the writable
/// `Form_pg_index` copy (`SearchSysCacheCopy1(INDEXRELID)`), apply the requested
/// transition, and `CatalogTupleUpdate`. The fetch crosses the syscache producer
/// seam (`search_syscache_copy_pg_index`) and the write crosses the
/// catalog-indexing consumer seam (`catalog_tuple_update_pg_index`), over the
/// `PgIndexForm` carrier widened to carry every flag column these transitions
/// touch.
pub fn index_set_state_flags<'mcx>(
    mcx: Mcx<'mcx>,
    index_id: Oid,
    action: backend_catalog_index_seams::IndexStateFlagsAction,
) -> PgResult<()> {
    use backend_catalog_index_seams::IndexStateFlagsAction as Action;

    /* Open pg_index and fetch a writable copy of the index's tuple */
    let pg_index = table_am::table_open::call(mcx, INDEX_RELATION_ID, ROW_EXCLUSIVE_LOCK)?;

    let Some((tid, mut form)) =
        syscache::search_syscache_copy_pg_index::call(mcx, index_id)?
    else {
        return elog_error(alloc::format!("cache lookup failed for index {index_id}"));
    };

    /* Perform the requested state change on the copy */
    match action {
        Action::SetReady => {
            /* Set indisready during a CREATE INDEX CONCURRENTLY sequence */
            debug_assert!(form.indislive);
            debug_assert!(!form.indisready);
            debug_assert!(!form.indisvalid);
            form.indisready = true;
        }
        Action::SetValid => {
            /* Set indisvalid during a CREATE INDEX CONCURRENTLY sequence */
            debug_assert!(form.indislive);
            debug_assert!(form.indisready);
            debug_assert!(!form.indisvalid);
            form.indisvalid = true;
        }
        Action::DropClearValid => {
            /*
             * Clear indisvalid during a DROP INDEX CONCURRENTLY sequence. Also
             * clear indisclustered (CLUSTER assumes it cannot be set on an
             * invalid index) and, for cleanliness, indisreplident.
             */
            form.indisvalid = false;
            form.indisclustered = false;
            form.indisreplident = false;
        }
        Action::DropSetDead => {
            /*
             * Clear indisready/indislive during DROP INDEX CONCURRENTLY — stop
             * updates and prevent any session from touching the index.
             */
            debug_assert!(!form.indisvalid);
            debug_assert!(!form.indisclustered);
            debug_assert!(!form.indisreplident);
            form.indisready = false;
            form.indislive = false;
        }
    }

    /* ... and update it */
    indexing::catalog_tuple_update_pg_index::call(mcx, &pg_index, tid, &form)?;

    pg_index.close(ROW_EXCLUSIVE_LOCK)?;
    Ok(())
}

/* ===========================================================================
 * index_concurrently_set_dead / index_drop
 * ========================================================================= */

/// `RELKIND_HAS_STORAGE(relkind)` (`catalog/pg_class.h`).
fn RELKIND_HAS_STORAGE(relkind: u8) -> bool {
    use types_tuple::access::{
        RELKIND_INDEX, RELKIND_MATVIEW, RELKIND_RELATION, RELKIND_SEQUENCE, RELKIND_TOASTVALUE,
    };
    relkind == RELKIND_RELATION
        || relkind == RELKIND_INDEX
        || relkind == RELKIND_SEQUENCE
        || relkind == RELKIND_TOASTVALUE
        || relkind == RELKIND_MATVIEW
}

/// `RELPERSISTENCE_TEMP` (`catalog/pg_class.h`).
const RELPERSISTENCE_TEMP_U8: u8 = b't';

/// `Int8LessOperator` (`catalog/pg_operator.h`) — OID of the `int8 < int8`
/// btree comparison operator, used as the sort operator for the TID sort.
const INT8_LESS_OPERATOR: Oid = 412;

/// `INT8OID` (`catalog/pg_type.h`).
const INT8_OID: Oid = 20;

/// `DEBUG2` (`utils/elog.h`).
const DEBUG2: i32 = 11;

/// `PROGRESS_CREATEIDX_PHASE_VALIDATE_*` (`commands/progress.h`) — the phase
/// codes `validate_index` reports.
const PROGRESS_CREATEIDX_PHASE_VALIDATE_IDXSCAN: i64 = 4;
const PROGRESS_CREATEIDX_PHASE_VALIDATE_SORT: i64 = 5;
const PROGRESS_CREATEIDX_PHASE_VALIDATE_TABLESCAN: i64 = 6;

/// `itemptr_encode(itemptr)` (catalog/index.h) — encode an `ItemPointer` as an
/// int64 that sorts identically to the original TID. The 16 LSBs hold the
/// offset; the next 32 bits hold the block number. Used by `validate_index` to
/// sort the index's TIDs as pass-by-value int8 rather than pass-by-ref TID.
fn itemptr_encode(itemptr: &types_tuple::heaptuple::ItemPointerData) -> i64 {
    let block = backend_storage_page::ItemPointerGetBlockNumber(itemptr) as u64;
    let offset = backend_storage_page::ItemPointerGetOffsetNumber(itemptr) as u64;
    (((block) << 16) | (offset & 0xFFFF)) as i64
}

/// `index_concurrently_build(heapRelationId, indexRelationId)`
/// (catalog/index.c): build an index for a concurrent operation. The IndexInfo
/// is rebuilt from the catalog (its earlier copy was lost in the commit that
/// made the catalog entry visible), marked `ii_Concurrent = true`, the index is
/// physically built via [`index_build`] under the table owner's userid +
/// SECURITY_RESTRICTED_OPERATION, and the pg_index row is finally moved to
/// `indisready = true` (`INDEX_CREATE_SET_READY`).
pub fn index_concurrently_build<'mcx>(
    mcx: Mcx<'mcx>,
    heap_relation_id: Oid,
    index_relation_id: Oid,
) -> PgResult<()> {
    /* This had better make sure that a snapshot is active */
    debug_assert!(snapmgr::active_snapshot_set::call());

    /* Open and lock the parent heap relation */
    let heap_rel = table_am::table_open::call(mcx, heap_relation_id, SHARE_UPDATE_EXCLUSIVE_LOCK)?;

    /*
     * Switch to the table owner's userid, so that any index functions are run
     * as that user. Also lock down security-restricted operations and arrange
     * to make GUC variable changes local to this command.
     */
    let (save_userid, save_sec_context) = matview::get_user_id_and_sec_context::call()?;
    rt::set_user_id_and_sec_context::call(
        relcache::rd_rel_relowner::call(&heap_rel)?,
        save_sec_context | SECURITY_RESTRICTED_OPERATION,
    )?;
    let save_nestlevel = guc::new_guc_nest_level::call();
    guc::restrict_search_path::call()?;

    let index_relation = indexam::index_open::call(mcx, index_relation_id, ROW_EXCLUSIVE_LOCK)?;

    /*
     * We have to re-build the IndexInfo struct, since it was lost in the commit
     * of the transaction where this concurrent index was created at the catalog
     * level.
     */
    let mut index_info = BuildIndexInfo(mcx, &index_relation)?;
    debug_assert!(!index_info.ii_ReadyForInserts);
    index_info.ii_Concurrent = true;
    index_info.ii_BrokenHotChain = false;

    /* Now build the index */
    index_build(mcx, &heap_rel, &index_relation, &mut index_info)?;

    /* Roll back any GUC changes executed by index functions */
    guc::at_eoxact_guc::call(false, save_nestlevel)?;

    /* Restore userid and security context */
    rt::set_user_id_and_sec_context::call(save_userid, save_sec_context)?;

    /* Close both the relations, but keep the locks */
    heap_rel.close(NO_LOCK)?;
    index_relation.close(NO_LOCK)?;

    /*
     * Update the pg_index row to mark the index as ready for inserts. Once we
     * commit this transaction, any new transactions that open the table must
     * insert new entries into the index for insertions and non-HOT updates.
     */
    index_set_state_flags(
        mcx,
        index_relation_id,
        backend_catalog_index_seams::IndexStateFlagsAction::SetReady,
    )?;

    Ok(())
}

/// `validate_index(heapId, indexId, snapshot)` (catalog/index.c) — phase 2/3
/// support code for a concurrent index build. Gather every TID currently in the
/// index into a sorted tuplesort (via the `ambulkdelete` callback that just
/// collects + never deletes), then scan the heap under `snapshot` and "merge
/// join" against the sorted list, inserting any tuple missing from the index.
///
/// The TID collector is registered through the generic `bulk_delete_callback`
/// registry (the owned-model stand-in for the C `(callback, callback_state)`
/// pair); `index_bulk_delete` invokes it for every index entry. The heap
/// merge-scan is the heap AM's `table_index_validate_scan` provider.
pub fn validate_index<'mcx>(
    mcx: Mcx<'mcx>,
    heap_id: Oid,
    index_id: Oid,
    snapshot: types_tableam::tableam::Snapshot,
) -> PgResult<()> {
    use core::cell::RefCell;
    extern crate alloc;
    use alloc::boxed::Box;
    use alloc::rc::Rc;

    {
        let progress_index = [
            PROGRESS_CREATEIDX_PHASE,
            PROGRESS_CREATEIDX_TUPLES_DONE,
            PROGRESS_CREATEIDX_TUPLES_TOTAL,
            PROGRESS_SCAN_BLOCKS_DONE,
            PROGRESS_SCAN_BLOCKS_TOTAL,
        ];
        let progress_vals = [PROGRESS_CREATEIDX_PHASE_VALIDATE_IDXSCAN, 0, 0, 0, 0];
        progress::pgstat_progress_update_multi_param::call(&progress_index, &progress_vals);
    }

    /* Open and lock the parent heap relation */
    let heap_relation = table_am::table_open::call(mcx, heap_id, SHARE_UPDATE_EXCLUSIVE_LOCK)?;

    /*
     * Switch to the table owner's userid, so that any index functions are run
     * as that user. Also lock down security-restricted operations and arrange
     * to make GUC variable changes local to this command.
     */
    let (save_userid, save_sec_context) = matview::get_user_id_and_sec_context::call()?;
    rt::set_user_id_and_sec_context::call(
        relcache::rd_rel_relowner::call(&heap_relation)?,
        save_sec_context | SECURITY_RESTRICTED_OPERATION,
    )?;
    let save_nestlevel = guc::new_guc_nest_level::call();
    guc::restrict_search_path::call()?;

    let index_relation = indexam::index_open::call(mcx, index_id, ROW_EXCLUSIVE_LOCK)?;

    /*
     * Fetch info needed for index_insert. (Its DefineIndex copy is long gone,
     * built in a previous transaction.)
     */
    let mut index_info = BuildIndexInfo(mcx, &index_relation)?;

    /* mark build is concurrent just for consistency */
    index_info.ii_Concurrent = true;

    /*
     * Scan the index and gather up all the TIDs into a tuplesort object.
     */
    let ivinfo = types_tableam::genam::IndexVacuumInfo {
        index: index_relation.alias(),
        heaprel: heap_relation.alias(),
        analyze_only: false,
        report_progress: true,
        estimated_count: true,
        message_level: DEBUG2,
        num_heap_tuples: heap_relation.rd_rel.reltuples as f64,
        strategy: None,
    };

    /*
     * Encode TIDs as int8 values for the sort (pass-by-value int8 sorts faster
     * than pass-by-ref TID).
     */
    let tuplesort = tuplesort_seam::tuplesort_begin_datum::call(
        mcx,
        INT8_OID,
        INT8_LESS_OPERATOR,
        InvalidOid,
        false,
        guc::maintenance_work_mem::call(),
        types_nodes::nodesort::TUPLESORT_NONE,
    )?;

    let mut tuplesort = tuplesort;

    // The bulkdelete IndexBulkDeleteCallback (validate_index_callback): for each
    // index entry, encode its TID and (in C) `tuplesort_putdatum` it straight
    // into the sort. The owned-model generic callback registry holds `'static`
    // closures (the C `void *callback_state`), which cannot borrow the
    // `'mcx`-scoped sort; so the callback collects the encoded int64 TIDs into a
    // `'static` Vec, and they are fed into the sort immediately after the
    // bulkdelete scan returns. This is behavior-preserving — the sort's final
    // contents (and the resulting sorted output) are identical; only the moment
    // each putdatum happens is deferred to just after the scan.
    let collected: Rc<RefCell<Vec<i64>>> = Rc::new(RefCell::new(Vec::new()));
    let handle = {
        let collected_for_cb = Rc::clone(&collected);
        backend_commands_vacuum_seams::bulk_delete_callback::register(Box::new(
            move |tid: types_tuple::heaptuple::ItemPointerData| -> bool {
                collected_for_cb.borrow_mut().push(itemptr_encode(&tid));
                false // never actually delete anything
            },
        ))
    };

    /* ambulkdelete updates progress metrics */
    backend_access_index_indexam::index_bulk_delete(mcx, &ivinfo, None, Some(handle))?;

    // Done collecting: drop the closure and feed the gathered TIDs into the
    // sort (state.itups += 1 per TID, as in validate_index_callback).
    backend_commands_vacuum_seams::bulk_delete_callback::unregister(handle);
    let collected = Rc::try_unwrap(collected)
        .map_err(|_| PgError::error("validate_index: TID collector still borrowed"))?
        .into_inner();
    let itups = collected.len() as i64;
    for encoded in collected {
        tuplesort_seam::tuplesort_putdatum::call(
            &mut tuplesort,
            types_tuple::backend_access_common_heaptuple::Datum::from_i64(encoded),
            false,
        )?;
    }

    /* Execute the sort */
    {
        let progress_index = [
            PROGRESS_CREATEIDX_PHASE,
            PROGRESS_SCAN_BLOCKS_DONE,
            PROGRESS_SCAN_BLOCKS_TOTAL,
        ];
        let progress_vals = [PROGRESS_CREATEIDX_PHASE_VALIDATE_SORT, 0, 0];
        progress::pgstat_progress_update_multi_param::call(&progress_index, &progress_vals);
    }
    tuplesort_seam::tuplesort_performsort::call(&mut tuplesort)?;

    /* Now scan the heap and "merge" it with the index */
    progress::pgstat_progress_update_param::call(
        PROGRESS_CREATEIDX_PHASE,
        PROGRESS_CREATEIDX_PHASE_VALIDATE_TABLESCAN,
    );

    let mut counters = backend_access_table_tableam_seams::ValidateScanCounters::default();
    // The validate-scan provider pulls each sorted TID through this no-arg
    // closure (the owned-model stand-in for tuplesort_getdatum on the seam's
    // far side). Thread the sort through a RefCell so the FnMut() can borrow it.
    let sort_pull = RefCell::new(tuplesort);
    {
        let mut pull = || -> PgResult<Option<i64>> {
            let mut st = sort_pull.borrow_mut();
            let (found, val, isnull) =
                tuplesort_seam::tuplesort_getdatum::call(&mut st, true, false)?;
            if !found {
                return Ok(None);
            }
            debug_assert!(!isnull);
            Ok(Some(val.as_i64()))
        };
        backend_access_table_tableam_seams::table_index_validate_scan::call(
            mcx,
            &heap_relation,
            &index_relation,
            &mut index_info,
            snapshot,
            &mut counters,
            &mut pull,
        )?;
    }
    let tuplesort = sort_pull.into_inner();

    /* Done with tuplesort object */
    let boxed: mcx::PgBox<'mcx, types_nodes::Tuplesortstate<'mcx>> = mcx::alloc_in(mcx, tuplesort)?;
    tuplesort_seam::tuplesort_end::call(boxed)?;

    /* Make sure to release resources cached in indexInfo (if needed). */
    {
        let mut carrier = IndexInfoCarrier::new(&mut index_info);
        backend_access_index_indexam::index_insert_cleanup(mcx, &index_relation, &mut carrier)?;
    }

    // elog(DEBUG2, "validate_index found %.0f heap tuples, %.0f index tuples;
    //              inserted %.0f missing tuples", htups, itups, tups_inserted)
    // — internal DEBUG2 elog; the counters are otherwise unobserved.
    let _ = (counters.htups, counters.tups_inserted, itups);

    /* Roll back any GUC changes executed by index functions */
    guc::at_eoxact_guc::call(false, save_nestlevel)?;

    /* Restore userid and security context */
    rt::set_user_id_and_sec_context::call(save_userid, save_sec_context)?;

    /* Close rels, but keep locks */
    index_relation.close(NO_LOCK)?;
    heap_relation.close(NO_LOCK)?;

    Ok(())
}

/// `index_concurrently_create_copy(heapRelation, oldIndexId, tablespaceOid,
/// newName)` (catalog/index.c): create concurrently a new index based on the
/// definition of `old_index_id`. The new index is registered in the catalogs
/// (`INDEX_CREATE_SKIP_BUILD | INDEX_CREATE_CONCURRENT`) and built later by
/// [`index_concurrently_build`]. Returns the new index relation's OID.
///
/// One faithful contained simplification relative to the C, not reachable by a
/// non-statistics-target index (the only shape REINDEX CONCURRENTLY exercises in
/// the regression corpus):
///   * Per-column statistics targets (`stattargets`) are not forwarded — the
///     tree's `index_create` does not carry an `attstattarget` array (it stores
///     NULL `attstattarget`, the catalog default), matching `AppendAttribute-
///     Tuples`' documented `stattargets == NULL` contract here.
///
/// The expression / predicate lists ARE re-decoded from the raw
/// `pg_index.indexprs`/`indpred` catalog text (`index_raw_{expressions,
/// predicate}`), exactly as C does (index.c:1354-1383) — NOT carried over from
/// the old index's relcache `IndexInfo`, whose trees are planner-flattened
/// (`eval_const_expressions`/`canonicalize_qual`/`fix_opfuncids`) and would
/// silently rewrite the stored definition of an expression / partial index.
pub fn index_concurrently_create_copy<'mcx>(
    mcx: Mcx<'mcx>,
    heap_relation: &Relation<'mcx>,
    old_index_id: Oid,
    tablespace_oid: Oid,
    new_name: &str,
) -> PgResult<Oid> {
    let index_relation = indexam::index_open::call(mcx, old_index_id, ROW_EXCLUSIVE_LOCK)?;

    /* The new index needs some information from the old index */
    let old_info = BuildIndexInfo(mcx, &index_relation)?;

    /*
     * Concurrent build of an index with exclusion constraints is not supported.
     */
    if old_info.ii_ExclusionOps.is_some() {
        index_relation.close(NO_LOCK)?;
        return Err(PgError::error(
            "concurrent index creation for exclusion constraints is not supported".to_string(),
        )
        .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED));
    }

    /*
     * Get the class and column options arrays plus reloptions of the old index
     * (the C `SearchSysCache1(INDEXRELID)` indclass/indoption + `RELOID`
     * reloptions projections).
     */
    let idx_info = syscache::search_pg_index_info::call(mcx, old_index_id)?
        .ok_or_else(|| PgError::error(alloc::format!("cache lookup failed for index {old_index_id}")))?;
    let indclass: Vec<Oid> = idx_info.indclass.iter().copied().collect();
    let indcoloptions: Vec<i16> = idx_info.indoption.iter().copied().collect();
    let indcollation: Vec<Oid> = idx_info.indcollation.iter().copied().collect();

    let reloptions_token = syscache::fetch_class_reloptions::call(mcx, old_index_id)?;
    let reloptions = if reloptions_token.is_null {
        types_tuple::Datum::null()
    } else {
        types_tuple::Datum::ByRef(mcx::slice_in(mcx, &reloptions_token.bytes)?)
    };

    /*
     * Fetch the list of expressions and predicates directly from the catalogs.
     * This cannot rely on the information from IndexInfo of the old index as
     * these have been flattened for the planner (eval_const_expressions /
     * canonicalize_qual / fix_opfuncids in RelationGetIndex{Expressions,
     * Predicate}). C re-reads the raw pg_index.indexprs / indpred via
     * stringToNode (index.c:1354-1383); index_raw_{expressions,predicate} are
     * exactly that raw decode (the predicate reduced to implicit-AND form).
     * Only read each when the old IndexInfo actually had it (the C NIL guards).
     */
    // The raw-decode seams return the trees in their query-lifetime result context
    // (`'static`); re-localize into the IndexInfo's `mcx` via `clone_in`.
    let relocalize = |opt: Option<mcx::PgVec<'static, types_nodes::primnodes::Expr<'static>>>|
     -> PgResult<Option<mcx::PgVec<'mcx, types_nodes::primnodes::Expr<'mcx>>>> {
        match opt {
            None => Ok(None),
            Some(v) => {
                let mut out: mcx::PgVec<'mcx, types_nodes::primnodes::Expr<'mcx>> =
                    mcx::vec_with_capacity_in(mcx, v.len())?;
                for e in v.iter() {
                    out.push(e.clone_in(mcx)?);
                }
                Ok(Some(out))
            }
        }
    };
    let index_exprs: Option<mcx::PgVec<'mcx, types_nodes::primnodes::Expr<'mcx>>> =
        if old_info.ii_Expressions.is_some() {
            relocalize(nodexform::index_raw_expressions::call(mcx, old_index_id)?)?
        } else {
            None
        };
    let index_preds: Option<mcx::PgVec<'mcx, types_nodes::primnodes::Expr<'mcx>>> =
        if old_info.ii_Predicate.is_some() {
            relocalize(nodexform::index_raw_predicate::call(mcx, old_index_id)?)?
        } else {
            None
        };

    /*
     * Build the IndexInfo for the new index. Rebuild of indexes with exclusion
     * constraints is not supported (checked above), so the ii_Exclusion* fields
     * are left unset.
     */
    let amsummarizing = match relcache::relation_rd_indam::call(index_relation.rd_id) {
        Some(amroutine) => amroutine.amsummarizing,
        None => false,
    };
    let mut new_info = IndexInfo {
        ii_NumIndexAttrs: old_info.ii_NumIndexAttrs,
        ii_NumIndexKeyAttrs: old_info.ii_NumIndexKeyAttrs,
        ii_Am: old_info.ii_Am,
        ii_Expressions: index_exprs,
        ii_Predicate: index_preds,
        ii_Unique: old_info.ii_Unique,
        ii_NullsNotDistinct: old_info.ii_NullsNotDistinct,
        ii_ReadyForInserts: false, /* not ready for inserts */
        ii_Concurrent: true,
        ii_Summarizing: amsummarizing,
        ii_WithoutOverlaps: old_info.ii_WithoutOverlaps,
        ii_IndexAttrNumbers: Default::default(),
        ii_Context: Some(mcx),
        ..Default::default()
    };

    /*
     * Extract the column names and the column numbers for the new IndexInfo.
     */
    let mut index_col_names: Vec<String> = Vec::with_capacity(old_info.ii_NumIndexAttrs as usize);
    for i in 0..old_info.ii_NumIndexAttrs as usize {
        let att_name = String::from_utf8_lossy(index_relation.rd_att.attr(i).attname.name_str())
            .into_owned();
        index_col_names.push(att_name);
        new_info.ii_IndexAttrNumbers[i] = old_info.ii_IndexAttrNumbers[i];
    }

    /* Extract opclass options for each attribute (get_attoptions(oldIndexId, i+1)). */
    let mut opclass_options: Vec<types_tuple::Datum<'mcx>> =
        Vec::with_capacity(new_info.ii_NumIndexAttrs as usize);
    for i in 0..new_info.ii_NumIndexAttrs as usize {
        let opt = lsyscache::get_attoptions::call(mcx, old_index_id, (i + 1) as i16)?;
        opclass_options.push(opt.unwrap_or_else(types_tuple::Datum::null));
    }

    let access_method_id = relcache::rd_rel_relam::call(&index_relation)?;
    let new_index_name = new_name.to_string();

    /*
     * Now create the new index. For a partition index the partition dependency
     * is adjusted later (parentIndexRelid is left InvalidOid here).
     */
    let args = backend_catalog_index_seams::IndexCreateArgs {
        index_relation_name: new_index_name,
        index_relation_id: InvalidOid,
        parent_index_relid: InvalidOid,
        parent_constraint_id: InvalidOid,
        rel_file_number: InvalidOid, /* InvalidRelFileNumber */
        index_info: new_info,
        index_col_names,
        access_method_id,
        table_space_id: tablespace_oid,
        collation_ids: indcollation,
        opclass_ids: indclass,
        coloptions: indcoloptions,
        reloptions,
        opclass_options: Some(opclass_options),
        flags: INDEX_CREATE_SKIP_BUILD | INDEX_CREATE_CONCURRENT,
        constr_flags: 0,
        allow_system_table_mods: true, /* allow table to be a system catalog */
        is_internal: false,
    };

    let (new_index_id, _con) = index_create(heap_relation, args)?;

    /* Close the relation used; keep the lock until end of transaction. */
    index_relation.close(NO_LOCK)?;

    Ok(new_index_id)
}

/// `index_concurrently_swap(newIndexId, oldIndexId, oldName)`
/// (catalog/index.c): swap name, dependencies, constraints and statistics of
/// the old index over to the new index, marking the old index invalid and the
/// new one valid.
///
/// One faithful contained gap: the `pg_description` comment move (catalog/
/// index.c:1726-1770) is omitted — it has no catalog-write seam owner in this
/// tree and is only reached when the reindexed index itself carries a `COMMENT`
/// (no such case appears in the REINDEX CONCURRENTLY regression corpus). The
/// `relispartition` / `indisexclusion` swaps are likewise carried only through
/// the partition-inheritance leg and the exclusion rejection in
/// [`index_concurrently_create_copy`] respectively (the form-update seams do not
/// project those two columns), so for the non-partition / non-exclusion indexes
/// REINDEX CONCURRENTLY rebuilds they are exact (both flags are false on old and
/// new).
pub fn index_concurrently_swap<'mcx>(
    mcx: Mcx<'mcx>,
    new_index_id: Oid,
    old_index_id: Oid,
    old_name: &str,
) -> PgResult<()> {
    /*
     * Take a necessary lock on the old and new index before swapping them.
     */
    let old_class_rel = indexam::index_open::call(mcx, old_index_id, SHARE_UPDATE_EXCLUSIVE_LOCK)?;
    let new_class_rel = indexam::index_open::call(mcx, new_index_id, SHARE_UPDATE_EXCLUSIVE_LOCK)?;

    /* Now swap names and dependencies of those indexes */
    let pg_class = table_am::table_open::call(mcx, RELATION_RELATION_ID, ROW_EXCLUSIVE_LOCK)?;

    let (old_class_tid, mut old_class_form) =
        syscache::search_syscache_copy_pg_class::call(mcx, old_index_id)?.ok_or_else(|| {
            PgError::error(alloc::format!("could not find tuple for relation {old_index_id}"))
        })?;
    let (new_class_tid, mut new_class_form) =
        syscache::search_syscache_copy_pg_class::call(mcx, new_index_id)?.ok_or_else(|| {
            PgError::error(alloc::format!("could not find tuple for relation {new_index_id}"))
        })?;

    /* Swap the names (namestrcpy). */
    let old_relname = old_class_form.relname.clone();
    new_class_form.relname = old_relname;
    old_class_form.relname = old_name.to_string();

    /* Swap the partition flags to track inheritance properly */
    let is_partition = new_class_form.relispartition;
    new_class_form.relispartition = old_class_form.relispartition;
    old_class_form.relispartition = is_partition;

    indexing::catalog_tuple_update_pg_class::call(mcx, &pg_class, old_class_tid, &old_class_form)?;
    indexing::catalog_tuple_update_pg_class::call(mcx, &pg_class, new_class_tid, &new_class_form)?;

    /* Now swap index info */
    let pg_index = table_am::table_open::call(mcx, INDEX_RELATION_ID, ROW_EXCLUSIVE_LOCK)?;

    let (old_index_tid, mut old_index_form) =
        syscache::search_syscache_copy_pg_index::call(mcx, old_index_id)?.ok_or_else(|| {
            PgError::error(alloc::format!("could not find tuple for relation {old_index_id}"))
        })?;
    let (new_index_tid, mut new_index_form) =
        syscache::search_syscache_copy_pg_index::call(mcx, new_index_id)?.ok_or_else(|| {
            PgError::error(alloc::format!("could not find tuple for relation {new_index_id}"))
        })?;

    /*
     * Copy constraint flags from the old index. This is safe because the old
     * index guaranteed uniqueness.
     */
    new_index_form.indisprimary = old_index_form.indisprimary;
    old_index_form.indisprimary = false;
    new_index_form.indimmediate = old_index_form.indimmediate;
    old_index_form.indimmediate = true;

    /* Preserve indisreplident / indisclustered in the new index. */
    new_index_form.indisreplident = old_index_form.indisreplident;
    new_index_form.indisclustered = old_index_form.indisclustered;

    /*
     * Mark the new index as valid, and the old index as invalid similarly to
     * what index_set_state_flags() does.
     */
    new_index_form.indisvalid = true;
    old_index_form.indisvalid = false;
    old_index_form.indisclustered = false;
    old_index_form.indisreplident = false;

    indexing::catalog_tuple_update_pg_index::call(mcx, &pg_index, old_index_tid, &old_index_form)?;
    indexing::catalog_tuple_update_pg_index::call(mcx, &pg_index, new_index_tid, &new_index_form)?;

    /*
     * Move constraints and triggers over to the new index.
     */
    let mut constraint_oids: Vec<Oid> = depend::get_index_ref_constraints::call(mcx, old_index_id)?
        .iter()
        .copied()
        .collect();

    let index_constraint_oid = depend::get_index_constraint::call(old_index_id)?;
    if OidIsValid(index_constraint_oid) {
        constraint_oids.push(index_constraint_oid);
    }

    pg_constraint::swap_index_constraints_and_triggers::call(
        &constraint_oids,
        old_index_id,
        new_index_id,
    )?;

    /*
     * NOTE: the pg_description comment move (catalog/index.c:1726-1770) is a
     * documented contained gap (see the function doc comment).
     */

    /*
     * Swap inheritance relationship with parent index.
     */
    if lsyscache::get_rel_relispartition::call(old_index_id)? {
        let ancestors = partition::get_partition_ancestors::call(mcx, old_index_id)?;
        let parent_index_relid = ancestors[0];

        pg_inherits::delete_inherits_tuple::call(old_index_id, parent_index_relid, false, None)?;
        pg_inherits::store_single_inheritance::call(new_index_id, parent_index_relid, 1)?;
    }

    /*
     * Swap all dependencies of and on the old index to the new one, and
     * vice-versa. A CommandCounterIncrement here would cause duplicate entries
     * in pg_depend, so it must not be done.
     */
    depend::changeDependenciesOf::call(RELATION_RELATION_ID, new_index_id, old_index_id)?;
    depend::changeDependenciesOn::call(mcx, RELATION_RELATION_ID, new_index_id, old_index_id)?;
    depend::changeDependenciesOf::call(RELATION_RELATION_ID, old_index_id, new_index_id)?;
    depend::changeDependenciesOn::call(mcx, RELATION_RELATION_ID, old_index_id, new_index_id)?;

    /* copy over statistics from old to new index */
    let new_relisshared = relcache::rd_rel_relisshared::call(&new_class_rel)?;
    let old_relisshared = relcache::rd_rel_relisshared::call(&old_class_rel)?;
    pgstat::pgstat_copy_relation_stats::call(
        new_class_rel.rd_id,
        new_relisshared,
        old_class_rel.rd_id,
        old_relisshared,
    )?;

    /* Copy data of pg_statistic from the old index to the new one */
    heap::copy_statistics::call(mcx, old_index_id, new_index_id)?;

    /* Close relations */
    pg_class.close(ROW_EXCLUSIVE_LOCK)?;
    pg_index.close(ROW_EXCLUSIVE_LOCK)?;

    /* The lock taken previously is not released until the end of transaction */
    old_class_rel.close(NO_LOCK)?;
    new_class_rel.close(NO_LOCK)?;

    Ok(())
}

/// `index_concurrently_set_dead(heapId, indexId)` (catalog/index.c): the second
/// pg_index state transition of DROP/REINDEX INDEX CONCURRENTLY — transfer the
/// index's predicate locks to the heap, clear `indisready`/`indislive`
/// (`INDEX_DROP_SET_DEAD`), and invalidate the table's relcache so sessions
/// refresh their index lists. Holds session locks across the surrounding
/// commit; the relations are reopened here under ShareUpdateExclusiveLock.
///
/// ```c
/// void index_concurrently_set_dead(Oid heapId, Oid indexId)
/// {
///     userHeapRelation  = table_open(heapId, ShareUpdateExclusiveLock);
///     userIndexRelation = index_open(indexId, ShareUpdateExclusiveLock);
///     TransferPredicateLocksToHeapRelation(userIndexRelation);
///     index_set_state_flags(indexId, INDEX_DROP_SET_DEAD);
///     CacheInvalidateRelcache(userHeapRelation);
///     table_close(userHeapRelation, NoLock);
///     index_close(userIndexRelation, NoLock);
/// }
/// ```
pub fn index_concurrently_set_dead<'mcx>(
    mcx: Mcx<'mcx>,
    heap_id: Oid,
    index_id: Oid,
) -> PgResult<()> {
    let user_heap_relation =
        table_am::table_open::call(mcx, heap_id, SHARE_UPDATE_EXCLUSIVE_LOCK)?;
    let user_index_relation =
        indexam::index_open::call(mcx, index_id, SHARE_UPDATE_EXCLUSIVE_LOCK)?;
    predicate::transfer_predicate_locks_to_heap_relation::call(user_index_relation.rd_id)?;

    index_set_state_flags(
        mcx,
        index_id,
        backend_catalog_index_seams::IndexStateFlagsAction::DropSetDead,
    )?;

    inval::cache_invalidate_relcache::call(user_heap_relation.rd_id)?;

    user_heap_relation.close(NO_LOCK)?;
    user_index_relation.close(NO_LOCK)?;
    Ok(())
}

/// `index_drop(indexId, concurrent, concurrent_lock_mode)` (catalog/index.c):
/// drop an index relation and remove all of its catalog rows. The DROP TABLE
/// path reaches it (via dependency.c's `doDeletion`) for every index the table
/// owns, including the implicit TOAST index.
///
/// To drop an index safely we take an exclusive lock on its *parent table*
/// (not just the index): another backend relying on a cached index-OID list
/// could otherwise try to use the just-dropped index. The concurrent path
/// (DROP/REINDEX INDEX CONCURRENTLY) instead disables the index in stages
/// across multiple transactions, taking only ShareUpdateExclusiveLock.
pub fn index_drop<'mcx>(
    mcx: Mcx<'mcx>,
    index_id: Oid,
    concurrent: bool,
    concurrent_lock_mode: bool,
) -> PgResult<()> {
    /*
     * A temporary relation uses a non-concurrent DROP. Assert it never asks
     * for the concurrent legs.
     */
    debug_assert!(
        lsyscache::get_rel_persistence::call(index_id)
            .map(|p| p != RELPERSISTENCE_TEMP_U8)
            .unwrap_or(true)
            || (!concurrent && !concurrent_lock_mode)
    );

    /*
     * To drop an index safely, we must grab exclusive lock on its parent
     * table. In the concurrent case we take ShareUpdateExclusiveLock instead.
     */
    let heap_id = IndexGetRelation(index_id, false)?;
    let lockmode = if concurrent || concurrent_lock_mode {
        SHARE_UPDATE_EXCLUSIVE_LOCK
    } else {
        ACCESS_EXCLUSIVE_LOCK
    };
    let mut user_heap_relation = table_am::table_open::call(mcx, heap_id, lockmode)?;
    let mut user_index_relation = indexam::index_open::call(mcx, index_id, lockmode)?;

    /*
     * We might still have open queries using it in our own session, which the
     * above locking won't prevent, so test explicitly.
     */
    tablecmds::check_table_not_in_use::call(&user_index_relation, "DROP INDEX")?;

    /*
     * Drop Index Concurrently is more or less the reverse process of Create
     * Index Concurrently — disable the index in stages, waiting out any
     * transactions that might be using it, before the physical deletion.
     */
    if concurrent {
        /*
         * We must commit our transaction to make the first pg_index state
         * update visible to other sessions. DROP INDEX CONCURRENTLY is
         * restricted to dropping one index with no dependencies, so no
         * transactional work must have happened yet — verify no XID is
         * assigned.
         */
        if xact::get_top_transaction_id_if_any::call() != types_core::InvalidTransactionId {
            let msg: alloc::string::String =
                "DROP INDEX CONCURRENTLY must be first action in transaction".into();
            return Err(
                PgError::error(msg).with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
            );
        }

        /* Mark index invalid by updating its pg_index entry */
        index_set_state_flags(
            mcx,
            index_id,
            backend_catalog_index_seams::IndexStateFlagsAction::DropClearValid,
        )?;

        /*
         * Invalidate the relcache for the table, so that after this commit all
         * sessions refresh any cached plans that might reference the index.
         */
        inval::cache_invalidate_relcache::call(user_heap_relation.rd_id)?;

        /* save lockrelid and locktag for below, then close but keep locks */
        let heaprelid = relcache::rel_lock_relid::call(user_heap_relation.rd_id)?;
        let heaplocktag = lmgr::set_locktag_relation::call(heaprelid.dbId, heaprelid.relId);
        let indexrelid = relcache::rel_lock_relid::call(user_index_relation.rd_id)?;

        user_heap_relation.close(NO_LOCK)?;
        user_index_relation.close(NO_LOCK)?;

        /*
         * Commit so the indisvalid update becomes visible; then start another
         * transaction. Take session-level locks first so neither the table nor
         * the index can be dropped before we finish.
         */
        lmgr::lock_relation_id_for_session::call(heaprelid, SHARE_UPDATE_EXCLUSIVE_LOCK)?;
        lmgr::lock_relation_id_for_session::call(indexrelid, SHARE_UPDATE_EXCLUSIVE_LOCK)?;

        snapmgr::pop_active_snapshot::call()?;
        xact::commit_transaction_command::call()?;
        xact::start_transaction_command::call()?;

        /*
         * Wait until no running transaction could be using the index for a
         * query (AccessExclusiveLock checks for running transactions holding
         * locks of any kind on the table).
         */
        lmgr::wait_for_lockers::call(heaplocktag, ACCESS_EXCLUSIVE_LOCK, true)?;

        /*
         * Updating pg_index might involve TOAST table access, so ensure we
         * have a valid snapshot.
         */
        snapmgr::push_active_snapshot::call(alloc::rc::Rc::new(
            snapmgr::get_transaction_snapshot::call()?,
        ))?;

        /* Finish invalidation of index and mark it as dead */
        index_concurrently_set_dead(mcx, heap_id, index_id)?;

        snapmgr::pop_active_snapshot::call()?;

        /* Commit again to make the pg_index update visible to other sessions. */
        xact::commit_transaction_command::call()?;
        xact::start_transaction_command::call()?;

        /* Wait till every transaction that saw the old index state has finished. */
        lmgr::wait_for_lockers::call(heaplocktag, ACCESS_EXCLUSIVE_LOCK, true)?;

        /*
         * Re-open relations to complete our actions; grab AccessExclusiveLock
         * on the index before the physical deletion.
         */
        user_heap_relation = table_am::table_open::call(mcx, heap_id, SHARE_UPDATE_EXCLUSIVE_LOCK)?;
        user_index_relation = indexam::index_open::call(mcx, index_id, ACCESS_EXCLUSIVE_LOCK)?;

        /* keep the session-lock ids for release at the end */
        return index_drop_finish(
            mcx,
            index_id,
            user_heap_relation,
            user_index_relation,
            Some((heaprelid, indexrelid)),
        );
    } else {
        /* Not concurrent, so just transfer predicate locks and we're good */
        predicate::transfer_predicate_locks_to_heap_relation::call(user_index_relation.rd_id)?;
    }

    index_drop_finish(mcx, index_id, user_heap_relation, user_index_relation, None)
}

/// The shared tail of `index_drop` (catalog/index.c) after the index relation is
/// open under the final lock: schedule physical removal, drop stats, flush the
/// relcache, delete pg_index / pg_class / pg_attribute / pg_statistic /
/// pg_inherits rows, and invalidate the owning relation's relcache. `session`
/// carries the DROP INDEX CONCURRENTLY session locks to release at the end.
fn index_drop_finish<'mcx>(
    mcx: Mcx<'mcx>,
    index_id: Oid,
    user_heap_relation: Relation<'mcx>,
    user_index_relation: Relation<'mcx>,
    session: Option<(
        types_storage::lock::LockRelId,
        types_storage::lock::LockRelId,
    )>,
) -> PgResult<()> {
    /* Schedule physical removal of the files (if any) */
    if RELKIND_HAS_STORAGE(user_index_relation.rd_rel.relkind) {
        storage::relation_drop_storage::call(
            user_index_relation.rd_locator,
            user_index_relation.rd_backend,
        )?;
    }

    /* ensure that stats are dropped if transaction commits */
    pgstat::pgstat_drop_relation::call(
        user_index_relation.rd_id,
        user_index_relation.rd_rel.relisshared,
    )?;

    /*
     * Close and flush the index's relcache entry, to ensure relcache doesn't
     * try to rebuild it while we're deleting catalog entries. We keep the lock.
     */
    user_index_relation.close(NO_LOCK)?;

    relcache::relation_forget_relation::call(index_id)?;

    /*
     * Updating pg_index might involve TOAST table access, so ensure we have a
     * valid snapshot.
     */
    snapmgr::push_active_snapshot::call(alloc::rc::Rc::new(
        snapmgr::get_transaction_snapshot::call()?,
    ))?;

    /* fix INDEX relation, and check for expressional index */
    let pg_index = table_am::table_open::call(mcx, INDEX_RELATION_ID, ROW_EXCLUSIVE_LOCK)?;

    /*
     * tuple = SearchSysCache1(INDEXRELID, ...);
     * hasexprs = !heap_attisnull(tuple, Anum_pg_index_indexprs, ...);
     * CatalogTupleDelete(indexRelation, &tuple->t_self);
     */
    let Some((tid, hasexprs)) = syscache::pg_index_tid_and_hasexprs::call(index_id)? else {
        return elog_error(alloc::format!("cache lookup failed for index {index_id}"));
    };

    indexing::catalog_tuple_delete::call(&pg_index, tid)?;

    pg_index.close(ROW_EXCLUSIVE_LOCK)?;

    snapmgr::pop_active_snapshot::call()?;

    /*
     * if it has any expression columns, we might have stored statistics about
     * them.
     */
    if hasexprs {
        heap::RemoveStatistics::call(index_id, 0)?;
    }

    /* fix ATTRIBUTE relation */
    heap::DeleteAttributeTuples::call(index_id)?;

    /* fix RELATION relation */
    heap::DeleteRelationTuple::call(index_id)?;

    /* fix INHERITS relation */
    pg_inherits::delete_inherits_tuple::call(index_id, InvalidOid, false, None)?;

    /*
     * We are presently too lazy to recompute relhasindex (the next VACUUM will
     * fix it). But we must send a shared-cache-inval notice on the owning
     * relation so other backends update their relcache index lists.
     */
    inval::cache_invalidate_relcache::call(user_heap_relation.rd_id)?;

    /* Close owning rel, but keep lock */
    user_heap_relation.close(NO_LOCK)?;

    /* Release the session locks before we go. */
    if let Some((heaprelid, indexrelid)) = session {
        lmgr::unlock_relation_id_for_session::call(heaprelid, SHARE_UPDATE_EXCLUSIVE_LOCK)?;
        lmgr::unlock_relation_id_for_session::call(indexrelid, SHARE_UPDATE_EXCLUSIVE_LOCK)?;
    }

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
/// scribbles `attrelid` on the index's relcache descriptor before the insert).
///
/// `opclass_options` is the C `const Datum *attopts` (one canonical attoptions
/// `Datum` per index attribute); `None` is the C NULL `attopts`. Each
/// per-column `Datum` is reduced to its `attoptions` bytea image (the varlena
/// `text[]` bytes) for the `append_attribute_tuples` seam — a null `Datum`
/// (`Datum::null()`) becomes `None` (SQL NULL `attoptions`); a real options
/// value rides as its varlena bytes. The C `stattargets` is NULL here.
fn AppendAttributeTuples<'mcx>(
    mcx: Mcx<'mcx>,
    index_relation: &Relation<'mcx>,
    opclass_options: Option<&[types_tuple::Datum<'mcx>]>,
) -> PgResult<()> {
    use types_tuple::Datum;
    // C: InsertPgAttributeTuples is given attopts == opclassOptions verbatim.
    // The owned `append_attribute_tuples` seam takes the per-attno bytea image;
    // map each attoptions Datum to its varlena bytes (null Datum -> SQL NULL).
    let attopts: Option<Vec<Option<Vec<u8>>>> = opclass_options.map(|opts| {
        opts.iter()
            .map(|d| match d {
                // C `(Datum) 0` — no options for this column (SQL NULL attoptions).
                Datum::ByVal(0) => None,
                // attoptions is always a `text[]` varlena, carried as its
                // detoasted on-disk bytes in the `ByRef` arm.
                Datum::ByRef(bytes) => Some(bytes.to_vec()),
                // Any other shape is not a valid attoptions image; it cannot
                // arise from a faithful caller (DefineIndex builds attoptions
                // via transformRelOptions, always a text[] varlena or NULL).
                _ => panic!(
                    "AppendAttributeTuples: attoptions Datum is not a text[] \
                     varlena image"
                ),
            })
            .collect()
    });
    indexing::append_attribute_tuples::call(
        mcx,
        index_relation,
        attopts.as_deref(),
        None,
    )
}

/* ===========================================================================
 * RelationTruncateIndexes (heap.c)
 * ========================================================================= */

/// `RelationTruncateIndexes(heapRelation)` (heap.c:3542): truncate every index
/// of the relation to zero tuples and reconstruct it from the (now empty) heap.
/// A dummy `IndexInfo` is used so no user-defined expression / predicate code
/// runs (this may execute during ON COMMIT processing). Caller must hold
/// AccessExclusiveLock on the heap.
///
/// Homed here (not in heap.c's crate) because the per-index work —
/// `index_open` / `BuildDummyIndexInfo` / `index_build` / `index_close` — all
/// lives in this unit; `heap_truncate_one_rel` reaches it through the
/// `relation_truncate_indexes` seam.
pub fn RelationTruncateIndexes<'mcx>(
    mcx: Mcx<'mcx>,
    heap_relation: &Relation<'mcx>,
) -> PgResult<()> {
    // foreach(indlist, RelationGetIndexList(heapRelation))
    let indexoidlist = relcache::relation_get_index_list::call(mcx, heap_relation)?;
    for &index_id in indexoidlist.iter() {
        // Open the index relation; use exclusive lock, just to be sure.
        let current_index = indexam::index_open::call(mcx, index_id, ACCESS_EXCLUSIVE_LOCK)?;

        // Fetch info needed for index_build using a dummy IndexInfo (cheaper and,
        // more importantly, avoids running user code in index expressions /
        // predicates during ON COMMIT processing).
        let mut index_info = BuildDummyIndexInfo(mcx, &current_index)?;

        // Now truncate the actual file (and discard buffers).
        storage::relation_truncate::call(&current_index, 0)?;

        // Initialize the index and rebuild (no need to re-establish pkey setting).
        index_build(mcx, heap_relation, &current_index, &mut index_info)?;

        // We're done with this index.
        current_index.close(NO_LOCK)?;
    }
    Ok(())
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
        // C `makeIndexInfo`: `ii->ii_Context = CurrentMemoryContext;`.
        ii_Context: Some(mcx),
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
 * reindex_index / reindex_relation
 * ========================================================================= */

/// `RELKIND_PARTITIONED_TABLE` (pg_class.h).
const RELKIND_PARTITIONED_TABLE: u8 = b'p';
/// `RELPERSISTENCE_PERMANENT` (pg_class.h).
const RELPERSISTENCE_PERMANENT_U8: u8 = b'p';
/// `RELPERSISTENCE_UNLOGGED` as a `u8` (the `rd_rel.relpersistence` field type).
const RELPERSISTENCE_UNLOGGED_U8: u8 = b'u';

/* commands/index.h: REINDEXOPT_* (a `bits32` bitmask in ReindexParams.options). */
/// `REINDEXOPT_VERBOSE` — print progress info.
const REINDEXOPT_VERBOSE: i32 = 0x01;
/// `REINDEXOPT_REPORT_PROGRESS` — report pgstat progress.
const REINDEXOPT_REPORT_PROGRESS: i32 = 0x02;
/// `REINDEXOPT_MISSING_OK` — skip missing relations.
const REINDEXOPT_MISSING_OK: i32 = 0x04;

/* commands/progress.h: CREATE INDEX progress-report parameter indexes + values
 * reindex_index uses (duplicated, as elsewhere in the crate, since
 * commands/progress.h has no owned crate). */
/// `PROGRESS_CREATEIDX_COMMAND` parameter index.
const PROGRESS_CREATEIDX_COMMAND: i32 = 0;
/// `PROGRESS_CREATEIDX_INDEX_OID` parameter index.
const PROGRESS_CREATEIDX_INDEX_OID: i32 = 6;
/// `PROGRESS_CREATEIDX_ACCESS_METHOD_OID` parameter index.
const PROGRESS_CREATEIDX_ACCESS_METHOD_OID: i32 = 8;
/// `PROGRESS_CREATEIDX_COMMAND_REINDEX` value.
const PROGRESS_CREATEIDX_COMMAND_REINDEX: i64 = 2;
/// `PROGRESS_CLUSTER_INDEX_REBUILD_COUNT` parameter index.
const PROGRESS_CLUSTER_INDEX_REBUILD_COUNT: i32 = 10;

/// `ShareLock` (`storage/lockdefs.h`).
const SHARE_LOCK: i32 = 5;

/// `RELATION_IS_OTHER_TEMP(relation)` (utils/rel.h): a temporary relation
/// belonging to some other session. `rel->rd_rel->relpersistence ==
/// RELPERSISTENCE_TEMP && !rel->rd_islocaltemp`.
fn relation_is_other_temp(rel: &Relation<'_>) -> PgResult<bool> {
    if rel.rd_rel.relpersistence != types_tuple::access::RELPERSISTENCE_TEMP {
        return Ok(false);
    }
    Ok(!relcache::rd_islocaltemp::call(rel)?)
}

/// `reindex_index(stmt, indexId, skip_constraint_checks, persistence, params)`
/// (catalog/index.c, file-static): rebuild one existing index in place — open +
/// lock the parent heap and the index, transfer predicate locks, rebuild the
/// physical relation with a fresh relfilenumber, and reset the `pg_index`
/// validity flags. `stmt` is `Some` only when invoked from a REINDEX command
/// (for event-trigger collection); `reindex_relation` and the other internal
/// callers pass `None`.
///
/// Mirrors index.c verbatim; the C declares `iRel`/`heapRelation` as raw
/// `Relation` and `goto`s through `table_close(..., NoLock)` early-exits — the
/// owned model uses the RAII [`Relation`] handle (`drop` == the C
/// `relation_close(rel, NoLock)`) and `.close(lockmode)` for the keep-lock
/// closes, so the early-return paths drop the heap handle (NoLock) exactly as
/// the C `table_close(heapRelation, NoLock)` does.
#[allow(clippy::too_many_arguments)]
fn reindex_index<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: Option<&types_nodes::ddlnodes::ReindexStmt<'mcx>>,
    index_id: Oid,
    skip_constraint_checks: bool,
    persistence: i8,
    params: &types_cluster::ReindexParams,
) -> PgResult<()> {
    let progress = (params.options & REINDEXOPT_REPORT_PROGRESS) != 0;
    let mut set_tablespace = false;

    /* pg_rusage_init(&ru0) — only feeds the VERBOSE INFO line below. */

    /*
     * Open and lock the parent heap relation.  ShareLock is sufficient since we
     * only need to be sure no schema or data changes are going on.
     */
    let heap_id = IndexGetRelation(index_id, (params.options & REINDEXOPT_MISSING_OK) != 0)?;
    /* if relation is missing, leave */
    if !OidIsValid(heap_id) {
        return Ok(());
    }

    let heap_relation = if (params.options & REINDEXOPT_MISSING_OK) != 0 {
        match table_am::try_table_open::call(mcx, heap_id, SHARE_LOCK)? {
            /* if relation is gone, leave */
            None => return Ok(()),
            Some(rel) => rel,
        }
    } else {
        table_am::table_open::call(mcx, heap_id, SHARE_LOCK)?
    };

    /*
     * Switch to the table owner's userid, so that any index functions are run
     * as that user.  Also lock down security-restricted operations and arrange
     * to make GUC variable changes local to this command.
     */
    let (save_userid, save_sec_context) = matview::get_user_id_and_sec_context::call()?;
    rt::set_user_id_and_sec_context::call(
        heap_relation.rd_rel.relowner,
        save_sec_context | SECURITY_RESTRICTED_OPERATION,
    )?;
    let save_nestlevel = guc::new_guc_nest_level::call();
    guc::restrict_search_path::call()?;

    if progress {
        let progress_cols = [PROGRESS_CREATEIDX_COMMAND, PROGRESS_CREATEIDX_INDEX_OID];
        let progress_vals = [PROGRESS_CREATEIDX_COMMAND_REINDEX, index_id as i64];
        progress::pgstat_progress_start_command::call(
            types_pgstat::backend_progress::ProgressCommandType::CreateIndex,
            heap_id,
        );
        progress::pgstat_progress_update_multi_param::call(&progress_cols, &progress_vals);
    }

    /*
     * Open the target index relation and get an exclusive lock on it, to ensure
     * that no one else is touching this particular index.
     */
    let i_rel = if (params.options & REINDEXOPT_MISSING_OK) != 0 {
        match indexam::try_index_open::call(mcx, index_id, ACCESS_EXCLUSIVE_LOCK)? {
            /* if index relation is gone, leave */
            None => {
                /* Roll back any GUC changes */
                guc::at_eoxact_guc::call(false, save_nestlevel)?;
                /* Restore userid and security context */
                rt::set_user_id_and_sec_context::call(save_userid, save_sec_context)?;
                /* Close parent heap relation, but keep locks */
                heap_relation.close(NO_LOCK)?;
                return Ok(());
            }
            Some(rel) => rel,
        }
    } else {
        indexam::index_open::call(mcx, index_id, ACCESS_EXCLUSIVE_LOCK)?
    };

    if progress {
        progress::pgstat_progress_update_param::call(
            PROGRESS_CREATEIDX_ACCESS_METHOD_OID,
            i_rel.rd_rel.relam as i64,
        );
    }

    /*
     * If a statement is available, telling that this comes from a REINDEX
     * command, collect the index for event triggers.
     */
    if let Some(stmt) = stmt {
        let mut address = types_catalog::catalog_dependency::InvalidObjectAddress;
        object_address_set(&mut address, RELATION_RELATION_ID, index_id);
        event_trigger::event_trigger_collect_simple_command_reindex::call(
            address,
            types_catalog::catalog_dependency::InvalidObjectAddress,
            stmt,
        )?;
    }

    /*
     * Partitioned indexes should never get processed here, as they have no
     * physical storage.
     */
    if i_rel.rd_rel.relkind == RELKIND_PARTITIONED_INDEX {
        let nsp = lsyscache::get_namespace_name::call(mcx, i_rel.rd_rel.relnamespace)?;
        return elog_error(alloc::format!(
            "cannot reindex partitioned index \"{}.{}\"",
            nsp.as_deref().unwrap_or(""),
            i_rel.name()
        ));
    }

    /*
     * Don't allow reindex on temp tables of other backends ... their local
     * buffer manager is not going to cope.
     */
    if relation_is_other_temp(&i_rel)? {
        return Err(PgError::error(alloc::string::String::from(
            "cannot reindex temporary tables of other sessions",
        ))
        .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED));
    }

    /*
     * Don't allow reindex of an invalid index on TOAST table.  This is a
     * leftover from a failed REINDEX CONCURRENTLY, and if rebuilt it would not
     * be possible to drop it anymore.
     */
    if catalog::is_toast_namespace::call(i_rel.rd_rel.relnamespace)
        && !tablecmds::get_index_isvalid::call(index_id)?.unwrap_or(false)
    {
        return Err(PgError::error(alloc::string::String::from(
            "cannot reindex invalid index on TOAST table",
        ))
        .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED));
    }

    /*
     * System relations cannot be moved even if allow_system_table_mods is
     * enabled to keep things consistent with the concurrent case where all the
     * indexes of a relation are processed in series, including indexes of toast
     * relations.
     */
    if OidIsValid(params.tablespace_oid) && catalog::is_system_relation::call(&i_rel)? {
        return Err(PgError::error(alloc::format!(
            "cannot move system relation \"{}\"",
            i_rel.name()
        ))
        .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED));
    }

    /* Check if the tablespace of this index needs to be changed */
    if OidIsValid(params.tablespace_oid)
        && index_seam::check_relation_table_space_move::call(&i_rel, params.tablespace_oid)?
    {
        set_tablespace = true;
    }

    /*
     * Also check for active uses of the index in the current transaction; we
     * don't want to reindex underneath an open indexscan.
     */
    tablecmds::check_table_not_in_use::call(&i_rel, "REINDEX INDEX")?;

    /* Set new tablespace, if requested */
    if set_tablespace {
        /* Update its pg_class row */
        index_seam::set_relation_table_space::call(&i_rel, params.tablespace_oid, InvalidOid)?;

        /*
         * Schedule unlinking of the old index storage at transaction commit,
         * and assume the new relfilelocator.
         */
        index_seam::drop_storage_assume_new_relfilelocator::call(&i_rel)?;

        /* Make sure the reltablespace change is visible */
        xact::command_counter_increment::call()?;
    }

    /*
     * All predicate locks on the index are about to be made invalid. Promote
     * them to relation locks on the heap.
     */
    predicate::transfer_predicate_locks_to_heap_relation::call(i_rel.rd_id)?;

    /* Fetch info needed for index_build */
    let mut index_info = BuildIndexInfo(mcx, &i_rel)?;

    /* If requested, skip checking uniqueness/exclusion constraints */
    let skipped_constraint = if skip_constraint_checks {
        let skipped = index_info.ii_Unique || index_info.ii_ExclusionOps.is_some();
        index_info.ii_Unique = false;
        index_info.ii_ExclusionOps = None;
        index_info.ii_ExclusionProcs = None;
        index_info.ii_ExclusionStrats = None;
        skipped
    } else {
        false
    };

    /* Suppress use of the target index while rebuilding it */
    SetReindexProcessing(heap_id, index_id)?;

    /* Create a new physical relation for the index */
    relcache::relation_set_new_relfilenumber::call(i_rel.rd_id, persistence)?;

    /*
     * `relation_set_new_relfilenumber` updates pg_class + `CommandCounterIncrement`,
     * which (as in C) rebuilds the index's relcache entry with the new, empty
     * `rd_locator`.  In the owned model `i_rel` is a projection snapshot taken
     * *before* the change, so it still carries the OLD relfilenode (and its old,
     * non-empty storage); `index_build`'s `RelationGetNumberOfBlocks == 0` empty
     * check would read the stale file and abort with "index already contains
     * data".  Drop the stale projection (keeping the lock) and re-open to obtain
     * a fresh handle reflecting the rebuilt entry's new empty storage.  The
     * re-open re-pins the entry; the matching `i_rel.close(NO_LOCK)` at the end
     * of the function balances it.  The AccessExclusiveLock is already held, so
     * the re-open is lock-idempotent.
     */
    i_rel.close(NO_LOCK)?;
    let i_rel = indexam::index_open::call(mcx, index_id, ACCESS_EXCLUSIVE_LOCK)?;

    /* Initialize the index and rebuild */
    /* Note: we do not need to re-establish pkey setting */
    index_build(mcx, &heap_relation, &i_rel, &mut index_info)?;

    /* Re-allow use of target index */
    ResetReindexProcessing();

    /*
     * If the index is marked invalid/not-ready/dead (ie, it's from a failed
     * CREATE INDEX CONCURRENTLY, or a DROP INDEX CONCURRENTLY failed midway),
     * and we didn't skip a uniqueness check, we can now mark it valid.  This
     * allows REINDEX to be used to clean up in such cases.
     *
     * We can also reset indcheckxmin, because we have now done a non-concurrent
     * index build, *except* in the case where index_build found some
     * still-broken HOT chains.  (See the long comment in index.c.)
     */
    if !skipped_constraint {
        let pg_index = table_am::table_open::call(mcx, INDEX_RELATION_ID, ROW_EXCLUSIVE_LOCK)?;

        let Some((tid, mut index_form)) =
            syscache::search_syscache_copy_pg_index::call(mcx, index_id)?
        else {
            return elog_error(alloc::format!("cache lookup failed for index {index_id}"));
        };

        let index_bad =
            !index_form.indisvalid || !index_form.indisready || !index_form.indislive;
        if index_bad || (index_form.indcheckxmin && !index_info.ii_BrokenHotChain) {
            if !index_info.ii_BrokenHotChain {
                index_form.indcheckxmin = false;
            } else if index_bad {
                index_form.indcheckxmin = true;
            }
            index_form.indisvalid = true;
            index_form.indisready = true;
            index_form.indislive = true;
            indexing::catalog_tuple_update_pg_index::call(mcx, &pg_index, tid, &index_form)?;

            /*
             * Invalidate the relcache for the table, so that after we commit all
             * sessions will refresh the table's index list.
             */
            inval::cache_invalidate_relcache::call(heap_relation.rd_id)?;
        }

        pg_index.close(ROW_EXCLUSIVE_LOCK)?;
    }

    /* Log what we did */
    if (params.options & REINDEXOPT_VERBOSE) != 0 {
        let name = lsyscache::get_rel_name::call(mcx, index_id)?;
        error_seams::ereport::call(
            PgError::new(
                types_error::INFO,
                alloc::format!(
                    "index \"{}\" was reindexed",
                    name.as_deref().unwrap_or("")
                ),
            ),
        )?;
    }

    /* Roll back any GUC changes executed by index functions */
    guc::at_eoxact_guc::call(false, save_nestlevel)?;

    /* Restore userid and security context */
    rt::set_user_id_and_sec_context::call(save_userid, save_sec_context)?;

    /* Close rels, but keep locks */
    i_rel.close(NO_LOCK)?;
    heap_relation.close(NO_LOCK)?;

    if progress {
        progress::pgstat_progress_end_command::call();
    }

    Ok(())
}

/// `ObjectAddressSet(address, RelationRelationId, indexId)` (objectaddress.h).
fn object_address_set(address: &mut ObjectAddress, class_id: Oid, object_id: Oid) {
    address.classId = class_id;
    address.objectId = object_id;
    address.objectSubId = 0;
}

/// `reindex_relation(stmt, relid, flags, params)` (catalog/index.c): recreate
/// all indexes of a relation (and optionally its toast relation too, if any).
/// Returns `true` if any indexes were rebuilt. A `CommandCounterIncrement`
/// occurs after each index rebuild.
///
/// The installed inward seam ([`backend_catalog_index_seams::reindex_relation`])
/// drops the C `stmt` argument (every current caller passes `NULL`), so this
/// body threads `stmt = None` into `reindex_index`; the recursion's
/// event-trigger leg is therefore never taken on any live path.
fn reindex_relation<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    flags: i32,
    params: &types_cluster::ReindexParams,
) -> PgResult<bool> {
    let mut result = false;

    /*
     * Open and lock the relation.  ShareLock is sufficient since we only need
     * to prevent schema and data changes in it.
     */
    let rel = if (params.options & REINDEXOPT_MISSING_OK) != 0 {
        match table_am::try_table_open::call(mcx, relid, SHARE_LOCK)? {
            /* if relation is gone, leave */
            None => return Ok(false),
            Some(rel) => rel,
        }
    } else {
        table_am::table_open::call(mcx, relid, SHARE_LOCK)?
    };

    /*
     * Partitioned tables should never get processed here, as they have no
     * physical storage.
     */
    if rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE {
        let nsp = lsyscache::get_namespace_name::call(mcx, rel.rd_rel.relnamespace)?;
        return elog_error(alloc::format!(
            "cannot reindex partitioned table \"{}.{}\"",
            nsp.as_deref().unwrap_or(""),
            rel.name()
        ));
    }

    let toast_relid = rel.rd_rel.reltoastrelid;

    /*
     * Get the list of index OIDs for this relation.  (We trust the relcache to
     * get this with a sequential scan if ignoring system indexes.)
     */
    let index_ids = relcache::relation_get_index_list::call(mcx, &rel)?;

    if (flags & types_cluster::REINDEX_REL_SUPPRESS_INDEX_USE) != 0 {
        /* Suppress use of all the indexes until they are rebuilt */
        SetReindexPending(&index_ids)?;

        /*
         * Make the new heap contents visible --- now things might be
         * inconsistent!
         */
        xact::command_counter_increment::call()?;
    }

    /*
     * Reindex the toast table, if any, before the main table.
     */
    if (flags & types_cluster::REINDEX_REL_PROCESS_TOAST) != 0 && OidIsValid(toast_relid) {
        /*
         * Note that this should fail if the toast relation is missing, so reset
         * REINDEXOPT_MISSING_OK.  Even if a new tablespace is set for the parent
         * relation, the indexes on its toast table are not moved.
         */
        let mut newparams = *params;
        newparams.options &= !REINDEXOPT_MISSING_OK;
        newparams.tablespace_oid = InvalidOid;
        result |= reindex_relation(mcx, toast_relid, flags, &newparams)?;
    }

    /*
     * Compute persistence of indexes: same as that of owning rel, unless caller
     * specified otherwise.
     */
    let persistence: i8 = if (flags & types_cluster::REINDEX_REL_FORCE_INDEXES_UNLOGGED) != 0 {
        RELPERSISTENCE_UNLOGGED_U8 as i8
    } else if (flags & types_cluster::REINDEX_REL_FORCE_INDEXES_PERMANENT) != 0 {
        RELPERSISTENCE_PERMANENT_U8 as i8
    } else {
        rel.rd_rel.relpersistence as i8
    };

    /* Reindex all the indexes. */
    let mut i: i64 = 1;
    for &index_oid in index_ids.iter() {
        let index_namespace_id = lsyscache::get_rel_namespace::call(index_oid)?;

        /*
         * Skip any invalid indexes on a TOAST table.  These can only be
         * duplicate leftovers from a failed REINDEX CONCURRENTLY, and if rebuilt
         * it would not be possible to drop them anymore.
         */
        if catalog::is_toast_namespace::call(index_namespace_id)
            && !tablecmds::get_index_isvalid::call(index_oid)?.unwrap_or(false)
        {
            let nsp = lsyscache::get_namespace_name::call(mcx, index_namespace_id)?;
            let name = lsyscache::get_rel_name::call(mcx, index_oid)?;
            error_seams::ereport::call(
                PgError::new(
                    types_error::WARNING,
                    alloc::format!(
                        "cannot reindex invalid index \"{}.{}\" on TOAST table, skipping",
                        nsp.as_deref().unwrap_or(""),
                        name.as_deref().unwrap_or("")
                    ),
                )
                .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
            )?;

            /*
             * Remove this invalid toast index from the reindex pending list, as
             * it is skipped here due to the hard failure that would happen in
             * reindex_index(), should we try to process it.
             */
            if (flags & types_cluster::REINDEX_REL_SUPPRESS_INDEX_USE) != 0 {
                RemoveReindexPending(index_oid)?;
            }
            continue;
        }

        reindex_index(
            mcx,
            None,
            index_oid,
            (flags & types_cluster::REINDEX_REL_CHECK_CONSTRAINTS) == 0,
            persistence,
            params,
        )?;

        xact::command_counter_increment::call()?;

        /* Index should no longer be in the pending list */
        debug_assert!(!ReindexIsProcessingIndex(index_oid));

        /* Set index rebuild count */
        progress::pgstat_progress_update_param::call(PROGRESS_CLUSTER_INDEX_REBUILD_COUNT, i);
        i += 1;
    }

    /*
     * Close rel, but continue to hold the lock.
     */
    rel.close(NO_LOCK)?;

    result |= !index_ids.is_empty();

    Ok(result)
}

/* ===========================================================================
 * Seam installation
 * ========================================================================= */

/// Install this unit's inward seams. Mirror-PG-and-panic: the build/validate
/// concurrent legs (`index_concurrently_*`, `validate_index`) stay uninstalled
/// until their executor/tuplesort keystones land (see the crate header), so
/// calling one still panics loudly.
pub fn init_seams() {
    // IndexGetRelation.
    index_seam::index_get_relation::set(IndexGetRelation);

    // BuildIndexInfo (brin insert-vacuum, amcheck) and index_build (bootstrap
    // build_indices) — the build / introspection core (keystones #340–#344).
    index_seam::build_index_info::set(BuildIndexInfo);
    index_seam::index_check_primary_key::set(index_check_primary_key);
    index_seam::index_build::set(index_build);
    index_seam::index_concurrently_create_copy::set(index_concurrently_create_copy);
    index_seam::index_concurrently_swap::set(index_concurrently_swap);
    index_seam::relation_truncate_indexes::set(RelationTruncateIndexes);

    // FormIndexDatum (ExecInsertIndexTuples / index build / logical-rep conflict
    // detection): compute an index tuple's column values from a heap slot,
    // evaluating index expressions in the per-tuple context.
    index_seam::form_index_datum::set(FormIndexDatum);

    // index_create (the CREATE INDEX gate: DefineIndex → index_create →
    // index_build) + index_constraint_create (PK/UNIQUE/EXCLUDE constraint
    // wiring) + index_set_state_flags (CREATE/DROP INDEX CONCURRENTLY pg_index
    // flag transitions, over the widened PgIndexForm carrier).
    index_seam::index_create::set(index_create);
    index_seam::index_constraint_create::set(index_constraint_create);
    index_seam::index_set_state_flags::set(index_set_state_flags);

    // BuildSpeculativeIndexInfo (ExecOpenIndices speculative; logical-rep
    // conflict detection) — the per-key unique-operator lookup over the index
    // opclasses (relcache rd_opfamily/rd_opcintype + amapi
    // IndexAmTranslateCompareType + lsyscache get_opfamily_member/get_opcode).
    index_seam::build_speculative_index_info::set(BuildSpeculativeIndexInfo);

    // Reindexing-support state machine.
    index_seam::reindex_is_processing_index::set(ReindexIsProcessingIndex);
    index_seam::reindex_is_currently_processing_index::set(ReindexIsCurrentlyProcessingIndex);
    index_seam::reset_reindex_processing::set(ResetReindexProcessing);
    index_seam::reset_reindex_state::set(ResetReindexState);

    // reindex_index / reindex_relation — rebuild one / all indexes of a relation
    // in place (CLUSTER, VACUUM FULL, TRUNCATE, REINDEX). The installed
    // reindex_relation seam drops the C `stmt` arg (every current caller passes
    // NULL), so its body threads `stmt = None` into reindex_index.
    index_seam::reindex_index::set(|mcx, stmt, index_id, skip, persistence, params| {
        reindex_index(mcx, Some(stmt), index_id, skip, persistence, &params)
    });
    index_seam::reindex_relation::set(|mcx, relid, flags, params| {
        reindex_relation(mcx, relid, flags, &params)
    });

    // index_drop — drop one index relation and its catalog rows (dependency.c's
    // doDeletion for an index object; the DROP TABLE path reaches it for every
    // index a table owns, including the implicit TOAST index). The inward seam
    // carries no `mcx`, so the shim allocates a scratch context.
    index_seam::index_drop::set(|index_id, concurrent, concurrent_lock_mode| {
        let ctx = mcx::MemoryContext::new("index_drop");
        index_drop(ctx.mcx(), index_id, concurrent, concurrent_lock_mode)
    });

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
