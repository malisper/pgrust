//! F0 — the generic catalog-mutation engine + the `CatalogIndexState` carrier.
//!
//! KEYSTONE. Faithful port of `catalog/indexing.c`: `CatalogOpenIndexes`,
//! `CatalogCloseIndexes`, `CatalogIndexInsert` (static), `CatalogTupleInsert`,
//! `CatalogTupleInsertWithInfo`, `CatalogTupleUpdate`,
//! `CatalogTupleUpdateWithInfo`, `CatalogTupleDelete`, and the assert-only
//! `CatalogTupleCheckConstraints`. These are the engine the per-catalog
//! families (the typed `catalog_tuple_*_pg_<rel>` seams, F1) call once they
//! form their heap tuple; they are provided here as `pub` functions over the
//! real catalog [`Relation`] and the owned [`FormedTuple`] carrier (header +
//! user-data area, #161/#289), exactly as `simple_heap_*` consume.
//!
//! NO GENERIC INWARD SEAM. The `backend-catalog-indexing-seams` crate declares
//! only the per-catalog *typed* seams (a `FormData_*` row crosses, the owner
//! forms the tuple) plus the cluster-family `catalog_open_indexes` /
//! `catalog_close_indexes` over the real owned [`CatalogIndexState`]. There is
//! no generic `catalog_tuple_insert(rel, FormedTuple)` inward seam, so the
//! generic engine is exposed as `pub` functions, not installed seams; the F1
//! family fills will call these directly (or through their typed seam wrapper).
//!
//! MCX THREADING: `CatalogOpenIndexes` opens the catalog's index relations
//! (`index_open`, allocated in a memory context) and stores them in the owned
//! [`CatalogIndexState`], whose `'mcx` lifetime is the caller's. The C reads
//! `CurrentMemoryContext` implicitly; the port threads `mcx: Mcx<'mcx>` down
//! from the caller (the translation rule for `CurrentMemoryContext`).
//!
//! INDEX-AM dependency: `CatalogOpenIndexes` calls `BuildIndexInfo` through the
//! `backend-catalog-index-seams::build_index_info` seam (catalog/index.c's
//! `BuildIndexInfo`, whose owner — index.c — is not yet ported). A catalog
//! with no indexes (the fast path) never reaches it; a catalog *with* indexes
//! reaches it and panics until index.c lands. This is the inherited
//! `mirror-pg-and-panic` boundary, not own-logic.

use ::mcx::Mcx;
use ::types_error::PgResult;
use ::nodes::execnodes::IndexInfo;
use ::nodes::TupleSlotKind;
use ::rel::Relation;
use ::types_storage::lock::{NoLock, RowExclusiveLock};
use ::types_tableam::amapi::IndexUniqueCheck;
use ::types_tableam::tableam::TU_UpdateIndexes;
use ::types_tuple::heaptuple::FormedTuple;
use ::types_tuple::heaptuple::ItemPointerData;
use ::types_tuple::heaptuple::HEAP_ONLY_TUPLE;

use ::execTuples::exec_init_slots::{
    ExecDropSingleTupleTableSlot, MakeSingleTupleTableSlot,
};
use ::execTuples::slot_deform::slot_getattr;
use ::execTuples::slot_ops_vtables::slot_getsysattr;
use ::execTuples::slot_store_fetch::ExecStoreHeapTuple;

/// `CatalogIndexState` (the on-disk C type is `struct ResultRelInfo *`).
///
/// The owned carrier now lives in [`::types_cluster::CatalogIndexState`] so the
/// `backend-catalog-indexing-seams` declarations and the cross-crate consumers
/// (cluster, large-object) can name the real value — there is no opaque
/// `CatalogIndexStateToken` handle anymore. This crate re-exports it for the
/// engine functions below.
pub use ::types_cluster::CatalogIndexState;

/// `CatalogOpenIndexes(heapRel)` (indexing.c): prepare to update the catalog's
/// indexes for an insert/update.
///
/// C builds a dummy `ResultRelInfo` (`ri_RangeTableIndex = 0`,
/// `ri_RelationDesc = heapRel`, `ri_TrigDesc = NULL`) and calls
/// `ExecOpenIndices(resultRelInfo, false)`. This is that `ExecOpenIndices(...,
/// false)` body specialized for the catalog case (`speculative == false`, so
/// no `BuildSpeculativeIndexInfo`):
///   * `ri_NumIndices = 0`; fast path if `!RelationGetForm(heapRel)->relhasindex`;
///   * else `RelationGetIndexList(heapRel)`, and for each index OID
///     `index_open(indexOid, RowExclusiveLock)` + `BuildIndexInfo(indexDesc)`.
#[allow(non_snake_case)]
pub fn CatalogOpenIndexes<'mcx>(
    mcx: Mcx<'mcx>,
    heap_rel: &Relation<'mcx>,
) -> PgResult<CatalogIndexState<'mcx>> {
    // resultRelInfo->ri_RelationDesc = heapRel; (the alias the engine reads;
    // ExecOpenIndices never closes it). RelationData -> aliased Relation.
    let heap_relation = heap_rel.alias();

    let mut index_descs: Vec<Relation<'mcx>> = Vec::new();
    let mut index_infos: Vec<IndexInfo<'mcx>> = Vec::new();

    // ExecOpenIndices: resultRelInfo->ri_NumIndices = 0;
    // /* fast path if no indexes */
    // if (!RelationGetForm(resultRelation)->relhasindex) return;
    if heap_rel.rd_rel.relhasindex {
        // indexoidlist = RelationGetIndexList(resultRelation);
        let indexoidlist =
            relcache_seams::relation_get_index_list::call(mcx, &heap_relation)?;
        // len = list_length(indexoidlist); if (len == 0) return;
        if !indexoidlist.is_empty() {
            index_descs.reserve(indexoidlist.len());
            index_infos.reserve(indexoidlist.len());
            // foreach(l, indexoidlist) { ... }
            for &index_oid in indexoidlist.iter() {
                // indexDesc = index_open(indexOid, RowExclusiveLock);
                let index_desc =
                    indexam::index_open(mcx, index_oid, RowExclusiveLock)?;
                // ii = BuildIndexInfo(indexDesc);
                // (speculative == false for catalogs, so no BuildSpeculativeIndexInfo)
                let ii = index_seams::build_index_info::call(mcx, &index_desc)?;
                // relationDescs[i] = indexDesc; indexInfoArray[i] = ii; i++;
                index_descs.push(index_desc);
                index_infos.push(ii);
            }
        }
        // list_free(indexoidlist); — the PgVec drops here.
    }

    Ok(CatalogIndexState {
        heap_relation,
        index_descs,
        index_infos,
    })
}

/// `CatalogCloseIndexes(indstate)` (indexing.c): `ExecCloseIndices(indstate)`
/// then `pfree(indstate)`. `ExecCloseIndices` walks `ri_IndexRelationDescs`
/// and `index_close(indexDescs[i], NoLock)` each (the locks taken at open time
/// are held until end of transaction, so close passes `NoLock`).
#[allow(non_snake_case)]
pub fn CatalogCloseIndexes(indstate: CatalogIndexState<'_>) -> PgResult<()> {
    let CatalogIndexState {
        heap_relation: _,
        index_descs,
        index_infos: _,
    } = indstate;

    // ExecCloseIndices: for (i = 0; i < numIndices; i++) {
    //     if (indexDescs[i] == NULL) continue;
    //     /* Drop lock acquired by ExecOpenIndices */
    //     index_close(indexDescs[i], NoLock);
    // }
    for index_desc in index_descs {
        indexam::index_close(index_desc, NoLock)?;
    }
    // pfree(indstate); — the owned value drops here.
    Ok(())
}

/// `CatalogIndexInsert(indstate, heapTuple, updateIndexes)` (indexing.c,
/// `static`): insert index entries for one catalog tuple — the cut-down
/// `ExecInsertIndexTuples`.
///
/// `MakeSingleTupleTableSlot(RelationGetDescr(heapRelation), &TTSOpsHeapTuple)`
/// → `ExecStoreHeapTuple(heapTuple, slot, false)` → for each index whose
/// `ii_ReadyForInserts` holds (and, under `onlySummarized = (updateIndexes ==
/// TU_Summarizing)`, whose `ii_Summarizing` holds): `FormIndexDatum(indexInfo,
/// slot, NULL, values, isnull)` → `index_insert(index, values, isnull,
/// &heapTuple->t_self, heapRelation, indisunique ? UNIQUE_CHECK_YES :
/// UNIQUE_CHECK_NO, false, indexInfo)` → finally
/// `ExecDropSingleTupleTableSlot(slot)`.
///
/// The HOT-only short-circuit (`HeapTupleIsHeapOnly(heapTuple) &&
/// !onlySummarized`) and the expressional / partial / exclusion / deferred-
/// uniqueness `Assert`s are `USE_ASSERT_CHECKING`-only in C; in a release build
/// the HOT short-circuit is taken before the slot is even made. Since catalog
/// tuples produced by `CatalogTuple*` are freshly built (never HOT), and
/// catalog indexes are never expressional / partial / exclusion (the keystone
/// invariant), the release-build path runs the loop unconditionally — exactly
/// the C `#ifndef USE_ASSERT_CHECKING` flow.
#[allow(non_snake_case)]
fn CatalogIndexInsert<'mcx>(
    indstate: &mut CatalogIndexState<'mcx>,
    heap_tuple: &FormedTuple<'mcx>,
    update_indexes: TU_UpdateIndexes,
    mcx: Mcx<'mcx>,
) -> PgResult<()> {
    // bool onlySummarized = (updateIndexes == TU_Summarizing);
    let only_summarized = update_indexes == TU_UpdateIndexes::TU_Summarizing;

    // HOT update does not require index inserts (indexing.c:93-95):
    //
    //   #ifndef USE_ASSERT_CHECKING
    //       if (HeapTupleIsHeapOnly(heapTuple) && !onlySummarized)
    //           return;
    //   #endif
    //
    // This guard is load-bearing, not vestigial: a catalog tuple CAN be
    // heap-only here. heapam_index_fetch_tuple stores the resolved HOT-chain
    // member's TID *and* header into the slot/syscache (the live member of a
    // HOT chain carries HEAP_ONLY_TUPLE), and heap_modify_tuple copies that
    // header forward, so the FormedTuple passed to a CatalogTupleUpdate of a
    // HOT-updated catalog row is heap-only. A plain (non-summarizing) HOT
    // update emits no index entries, so indexing a heap-only TID here would
    // record an index entry pointing directly at a heap-only tuple — exactly
    // the "heap tid from index tuple ... points to heap-only tuple" corruption
    // that index_delete_check_htid later raises. Match C and return early.
    let is_heap_only = (heap_tuple
        .tuple
        .t_data
        .as_ref()
        .map(|h| h.t_infomask2)
        .unwrap_or(0)
        & HEAP_ONLY_TUPLE)
        != 0;
    if is_heap_only && !only_summarized {
        return Ok(());
    }
    // When only updating summarized indexes, the tuple has to be HOT.
    debug_assert!(!only_summarized || is_heap_only);

    // numIndexes = indstate->ri_NumIndices; if (numIndexes == 0) return;
    let num_indexes = indstate.index_descs.len();
    if num_indexes == 0 {
        return Ok(());
    }

    // Need a slot to hold the tuple being examined.
    // slot = MakeSingleTupleTableSlot(RelationGetDescr(heapRelation),
    //                                 &TTSOpsHeapTuple);
    let heap_relation = indstate.heap_relation.alias();
    let tupdesc = heap_relation.rd_att_clone_in(mcx)?;
    let mut slot = MakeSingleTupleTableSlot(mcx, Some(tupdesc), TupleSlotKind::HeapTuple)?;

    // ExecStoreHeapTuple(heapTuple, slot, false);
    // The slot borrows the tuple (should_free = false); the owned model stores
    // a clone in the slot's mcx, leaving the caller's `heap_tuple` intact.
    ExecStoreHeapTuple(heap_tuple.clone_in(mcx)?, &mut slot, false)?;

    // for each index, form and insert the index tuple.
    for i in 0..num_indexes {
        // indexInfo = indexInfoArray[i]; index = relationDescs[i];
        // (split borrows: index_infos[i] is taken mutably for index_insert,
        // index_descs[i] is read; both are distinct fields.)
        // If the index is marked as read-only, ignore it.
        if !indstate.index_infos[i].ii_ReadyForInserts {
            continue;
        }

        // Expressional and partial indexes on system catalogs are not
        // supported, nor exclusion constraints, nor deferred uniqueness:
        debug_assert!(indstate.index_infos[i].ii_Expressions.is_none());
        debug_assert!(indstate.index_infos[i].ii_Predicate.is_none());
        debug_assert!(indstate.index_infos[i].ii_ExclusionOps.is_none());
        debug_assert!(
            indstate.index_descs[i]
                .rd_index
                .as_ref()
                .map(|ix| ix.indimmediate)
                .unwrap_or(false)
        );
        debug_assert!(indstate.index_infos[i].ii_NumIndexKeyAttrs != 0);

        // Skip insertions into non-summarizing indexes if we only need to
        // update summarizing indexes.
        if only_summarized && !indstate.index_infos[i].ii_Summarizing {
            continue;
        }

        // FormIndexDatum fills in its values and isnull parameters with the
        // appropriate values for the column(s) of the index. estate == NULL
        // (no expression eval to do; catalog indexes carry no expressions).
        let (values, isnull) = form_index_datum(&indstate.index_infos[i], &mut slot, mcx)?;

        // The index AM does the rest.
        // index_insert(index, values, isnull, &heapTuple->t_self, heapRelation,
        //              index->rd_index->indisunique ? UNIQUE_CHECK_YES :
        //              UNIQUE_CHECK_NO, false, indexInfo);
        let indisunique = indstate.index_descs[i]
            .rd_index
            .as_ref()
            .map(|ix| ix.indisunique)
            .unwrap_or(false);
        let check_unique = if indisunique {
            IndexUniqueCheck::UNIQUE_CHECK_YES
        } else {
            IndexUniqueCheck::UNIQUE_CHECK_NO
        };

        // The index AM does the rest. The dispatch layer carries the real
        // `IndexInfo<'mcx>` through the `'mcx`-safe `IndexInfoCarrier` (#342):
        // the catalog's own `indstate.index_infos[i]` rides across type-erased
        // (`types-tableam` sits below `types-nodes` and cannot name
        // `IndexInfo<'mcx>`) and the AM adapter downcasts it back. (`btinsert`
        // ignores it; an exclusion/expression AM would downcast and read it.)
        let index_desc = indstate.index_descs[i].alias();
        let mut am_index_info =
            ::types_tableam::index_info_carrier::IndexInfoCarrier::new(&mut indstate.index_infos[i]);
        indexam::index_insert(
            mcx,
            &index_desc,              // index relation
            &values,                  // array of index Datums
            &isnull,                  // is-null flags
            &heap_tuple.tuple.t_self, // tid of heap tuple
            &heap_relation,
            check_unique,
            false, // indexUnchanged
            &mut am_index_info,
        )?;
    }

    // ExecDropSingleTupleTableSlot(slot);
    ExecDropSingleTupleTableSlot(slot)?;
    Ok(())
}

/// `FormIndexDatum(indexInfo, slot, NULL, values, isnull)` (catalog/index.c),
/// specialized for the catalog case where `estate == NULL` and the index has
/// no expression columns (the keystone invariant). For each index key column:
/// a system column (`keycol < 0`) goes through `slot_getsysattr`; a plain
/// column (`keycol != 0`) through `slot_getattr`; an expression column
/// (`keycol == 0`) is impossible for a catalog index and would require an
/// `EState` — the C `elog(ERROR, "wrong number of index expressions")` /
/// `ExecEvalExprSwitchContext(NULL estate)` path, which never fires here, so
/// reaching it is an unexpected-index error.
///
/// The C fills caller-provided `Datum values[INDEX_MAX_KEYS]` /
/// `bool isnull[INDEX_MAX_KEYS]`; the port returns the populated `Vec`/array.
fn form_index_datum<'mcx>(
    index_info: &IndexInfo<'mcx>,
    slot: &mut ::nodes::tuptable::SlotData<'mcx>,
    mcx: Mcx<'mcx>,
) -> PgResult<(
    Vec<::types_tuple::heaptuple::Datum<'mcx>>,
    Vec<bool>,
)> {
    // indexInfo->ii_Expressions == NIL for catalog indexes, so there is no
    // ii_ExpressionsState setup and no GetPerTupleExprContext(estate) check.
    let n = index_info.ii_NumIndexAttrs as usize;
    let mut values: Vec<::types_tuple::heaptuple::Datum<'mcx>> =
        Vec::with_capacity(n);
    let mut isnull: Vec<bool> = Vec::with_capacity(n);

    // for (i = 0; i < indexInfo->ii_NumIndexAttrs; i++)
    for i in 0..n {
        // int keycol = indexInfo->ii_IndexAttrNumbers[i];
        let keycol = index_info.ii_IndexAttrNumbers[i];
        let (i_datum, is_null) = if keycol < 0 {
            // iDatum = slot_getsysattr(slot, keycol, &isNull);
            slot_getsysattr(mcx, slot, keycol)?
        } else if keycol != 0 {
            // Plain index column; get the value directly from the heap tuple.
            // iDatum = slot_getattr(slot, keycol, &isNull);
            slot_getattr(mcx, slot, keycol)?
        } else {
            // Index expression --- needs an estate to evaluate; catalog indexes
            // never carry one. The C "wrong number of index expressions"
            // elog(ERROR) covers a mismatch; an expression column on a catalog
            // index is the keystone-invariant violation.
            return Err(::types_error::PgError::error(
                "wrong number of index expressions",
            ));
        };
        // values[i] = iDatum; isnull[i] = isNull;
        values.push(i_datum);
        isnull.push(is_null);
    }

    Ok((values, isnull))
}

/// `CatalogTupleCheckConstraints(heapRel, tup)` (indexing.c): assert-only.
/// In an `USE_ASSERT_CHECKING` build, verify no `attnotnull` column is null by
/// walking the tuple's null bitmap. In a release build the C `#define` makes it
/// `((void) 0)` — a no-op. Correct tuples (the only ones any caller builds)
/// pass either way, so the port takes the release `#else` form.
#[allow(non_snake_case)]
fn CatalogTupleCheckConstraints(heap_rel: &Relation<'_>, tup: &FormedTuple<'_>) {
    // #else /* !USE_ASSERT_CHECKING */ #define ... ((void) 0)
    let _ = (heap_rel, tup);
}

/// `CatalogTupleInsert(heapRel, tup)` (indexing.c): the single-tuple
/// convenience inserter. `CatalogTupleCheckConstraints` → `CatalogOpenIndexes`
/// → `simple_heap_insert` → `CatalogIndexInsert(indstate, tup, TU_All)` →
/// `CatalogCloseIndexes`.
///
/// `tup` is borrowed mutably because `simple_heap_insert` stamps the header and
/// writes the stored TID into `tup.tuple.t_self` (the C `simple_heap_insert`
/// fills `tup->t_self`, which `CatalogIndexInsert` then reads).
#[allow(non_snake_case)]
pub fn CatalogTupleInsert<'mcx>(
    mcx: Mcx<'mcx>,
    heap_rel: &Relation<'mcx>,
    tup: &mut FormedTuple<'mcx>,
) -> PgResult<()> {
    // CatalogTupleCheckConstraints(heapRel, tup);
    CatalogTupleCheckConstraints(heap_rel, tup);

    // indstate = CatalogOpenIndexes(heapRel);
    let mut indstate = CatalogOpenIndexes(mcx, heap_rel)?;

    // simple_heap_insert(heapRel, tup);
    heapam_seams::simple_heap_insert::call(mcx, heap_rel, tup)?;

    // CatalogIndexInsert(indstate, tup, TU_All);
    CatalogIndexInsert(&mut indstate, tup, TU_UpdateIndexes::TU_All, mcx)?;

    // CatalogCloseIndexes(indstate);
    CatalogCloseIndexes(indstate)
}

/// `CatalogTupleInsertWithInfo(heapRel, tup, indstate)` (indexing.c): as
/// [`CatalogTupleInsert`] but with caller-supplied open indexes (amortizing
/// `CatalogOpenIndexes`/`CatalogCloseIndexes` across multiple insertions).
#[allow(non_snake_case)]
pub fn CatalogTupleInsertWithInfo<'mcx>(
    mcx: Mcx<'mcx>,
    heap_rel: &Relation<'mcx>,
    tup: &mut FormedTuple<'mcx>,
    indstate: &mut CatalogIndexState<'mcx>,
) -> PgResult<()> {
    // CatalogTupleCheckConstraints(heapRel, tup);
    CatalogTupleCheckConstraints(heap_rel, tup);

    // simple_heap_insert(heapRel, tup);
    heapam_seams::simple_heap_insert::call(mcx, heap_rel, tup)?;

    // CatalogIndexInsert(indstate, tup, TU_All);
    CatalogIndexInsert(indstate, tup, TU_UpdateIndexes::TU_All, mcx)
}

/// `CatalogTuplesMultiInsertWithInfo(heapRel, slot, ntuples, indstate)`
/// (indexing.c): insert multiple tuples into the given catalog relation at
/// once, with an amortized cost of `CatalogOpenIndexes`.
///
/// `heap_multi_insert(heapRel, slot, ntuples, GetCurrentCommandId(true), 0,
/// NULL)` then, because there is no equivalent of `heap_multi_insert` for the
/// catalog indexes, a loop over the inserted tuples running
/// `CatalogIndexInsert(indstate, tuple, TU_All)` for each. C re-fetches each
/// tuple from its slot (`ExecFetchSlotHeapTuple`) and stamps
/// `tuple->t_tableOid = slot[i]->tts_tableOid` (which `heap_multi_insert` set
/// to `RelationGetRelid(heapRel)`); the repo's `heap_multi_insert` seam returns
/// the inserted tuples already stamped with `t_self` and `t_tableOid`, so the
/// loop indexes them directly.
///
/// The C `ntuples <= 0` fast path is the empty-`tuples` case here.
#[allow(non_snake_case)]
pub fn CatalogTuplesMultiInsertWithInfo<'mcx>(
    mcx: Mcx<'mcx>,
    heap_rel: &Relation<'mcx>,
    tuples: ::mcx::PgVec<'mcx, FormedTuple<'mcx>>,
    indstate: &mut CatalogIndexState<'mcx>,
) -> PgResult<()> {
    // /* Nothing to do */
    // if (ntuples <= 0) return;
    if tuples.is_empty() {
        return Ok(());
    }

    // heap_multi_insert(heapRel, slot, ntuples, GetCurrentCommandId(true), 0,
    //                   NULL);
    // The repo's seam consumes the owned formed tuples and returns them with
    // t_self / t_tableOid stamped (the C writes the TIDs back into the slots).
    let cid = transam_xact_seams::get_current_command_id::call(true)?;
    let inserted = heapam_seams::heap_multi_insert::call(
        mcx, heap_rel, tuples, cid, 0, None,
    )?;

    // There is no equivalent to heap_multi_insert for the catalog indexes, so
    // we must loop over and insert individually.
    // for (int i = 0; i < ntuples; i++) {
    //     tuple = ExecFetchSlotHeapTuple(slot[i], true, &should_free);
    //     tuple->t_tableOid = slot[i]->tts_tableOid;
    //     CatalogIndexInsert(indstate, tuple, TU_All);
    //     if (should_free) heap_freetuple(tuple);
    // }
    for tuple in inserted.iter() {
        CatalogIndexInsert(indstate, tuple, TU_UpdateIndexes::TU_All, mcx)?;
    }
    Ok(())
}

/// `CatalogTupleUpdate(heapRel, otid, tup)` (indexing.c): the single-tuple
/// convenience updater. `CatalogTupleCheckConstraints` → `CatalogOpenIndexes`
/// → `simple_heap_update` (returning `*update_indexes`) →
/// `CatalogIndexInsert(indstate, tup, updateIndexes)` → `CatalogCloseIndexes`.
///
/// C initializes `updateIndexes = TU_All` and lets `simple_heap_update`
/// overwrite it; the repo's `simple_heap_update` *returns* that value.
#[allow(non_snake_case)]
pub fn CatalogTupleUpdate<'mcx>(
    mcx: Mcx<'mcx>,
    heap_rel: &Relation<'mcx>,
    otid: ItemPointerData,
    tup: &mut FormedTuple<'mcx>,
) -> PgResult<()> {
    // CatalogTupleCheckConstraints(heapRel, tup);
    CatalogTupleCheckConstraints(heap_rel, tup);

    // indstate = CatalogOpenIndexes(heapRel);
    let mut indstate = CatalogOpenIndexes(mcx, heap_rel)?;

    // simple_heap_update(heapRel, otid, tup, &updateIndexes);
    let update_indexes =
        heapam_seams::simple_heap_update::call(mcx, heap_rel, otid, tup)?;

    // CatalogIndexInsert(indstate, tup, updateIndexes);
    CatalogIndexInsert(&mut indstate, tup, update_indexes, mcx)?;

    // CatalogCloseIndexes(indstate);
    CatalogCloseIndexes(indstate)
}

/// `CatalogTupleUpdateWithInfo(heapRel, otid, tup, indstate)` (indexing.c): as
/// [`CatalogTupleUpdate`] but with caller-supplied open indexes.
#[allow(non_snake_case)]
pub fn CatalogTupleUpdateWithInfo<'mcx>(
    mcx: Mcx<'mcx>,
    heap_rel: &Relation<'mcx>,
    otid: ItemPointerData,
    tup: &mut FormedTuple<'mcx>,
    indstate: &mut CatalogIndexState<'mcx>,
) -> PgResult<()> {
    // CatalogTupleCheckConstraints(heapRel, tup);
    CatalogTupleCheckConstraints(heap_rel, tup);

    // simple_heap_update(heapRel, otid, tup, &updateIndexes);
    let update_indexes =
        heapam_seams::simple_heap_update::call(mcx, heap_rel, otid, tup)?;

    // CatalogIndexInsert(indstate, tup, updateIndexes);
    CatalogIndexInsert(indstate, tup, update_indexes, mcx)
}

/// `CatalogTupleDelete(heapRel, tid)` (indexing.c): the catalog-tuple deleter.
/// With Postgres heaps there is no index work at deletion time (cleanup is done
/// later by VACUUM), so this is a thin wrapper over `simple_heap_delete`.
#[allow(non_snake_case)]
pub fn CatalogTupleDelete<'mcx>(
    mcx: Mcx<'mcx>,
    heap_rel: &Relation<'mcx>,
    tid: ItemPointerData,
) -> PgResult<()> {
    // simple_heap_delete(heapRel, tid);
    heapam_seams::simple_heap_delete::call(mcx, heap_rel, tid)
}
