//! `executor/execIndexing.c` — inserting index tuples and enforcing unique and
//! exclusion constraints.
//!
//! `ExecInsertIndexTuples` is the main entry point: called after inserting a
//! tuple to the heap, it inserts the corresponding index tuples into all
//! indexes while enforcing unique and exclusion constraints (see the C source
//! header for the detailed unique/exclusion/deferred/speculative discussion).
//!
//! The executor-owned structs are real values addressed by id into the
//! [`EStateData`] pools (`RriId` / `SlotId` / `EcxtId`); the `IndexInfo[i]` for
//! the index being processed is moved out of the result-rel pool into an owned
//! local for the duration of its per-index work (so the seams that need
//! `&mut EStateData` and the ones that need `&IndexInfo` don't alias), then
//! written back — exactly the `ri_IndexRelationInfo[i]` pointer C dereferences
//! and mutates in place.

#![allow(non_snake_case)]
#![allow(clippy::too_many_arguments)]

extern crate alloc;

use alloc::string::ToString;

use ::utils_error::ereport;
use ::types_error::error::{
    ERRCODE_CHECK_VIOLATION, ERRCODE_EXCLUSION_VIOLATION, ERRCODE_INTERNAL_ERROR,
    ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERROR,
};

use mcx::{Mcx, PgVec};

use types_core::{fmgr::INDEX_MAX_KEYS, Oid};
use ::datum::Datum as DatumWord;
use ::types_error::PgResult;
use ::nodes::execnodes::{EStateData, IndexInfo, RriId, SlotId};
use ::nodes::EcxtId;
use ::rel::Relation;
use ::types_scan::sdir::ScanDirection;
use ::types_scan::scankey::ScanKeyData;
use ::snapshot::snapshot::{SnapshotData, SnapshotType};
use ::types_storage::lock::XLTW_Oper;
use ::types_tableam::amapi::IndexUniqueCheck;
use ::types_tableam::index_info_carrier::IndexInfoCarrier;
use ::types_tuple::heaptuple::Datum as DatumV;
use ::types_tuple::heaptuple::{item_pointer_is_valid, ItemPointerData};

use ::nodes_core::bitmapset::{bms_free, bms_is_member, bms_union};

// Direct (acyclic) callees.
use indexam as indexam;
use indexam_seams as indexam_seams;
use table_tableam as tableam;
use execUtils as execUtils;
use ::nodes_core::nodefuncs::expression_tree_walker;

// Outward seams.
use genam_seams as genam_seams;
use index_seams as index_seams;
use execIndexing_seams as inward;
use lmgr_seams as lmgr_seams;
use relcache_seams as relcache_seams;
use typcache_seams as typcache_seams;
use fmgr_seams as fmgr_seams;

use execExpr_seams as expr_seams;
use transam_seams as transam_seams;
use transam_xact_seams as xact_seams;
use snapmgr_seams as snapmgr_seams;

#[cfg(test)]
mod tests;

/// `SK_ISNULL` (`access/skey.h`) — scankey argument is NULL.
use ::types_scan::scankey::SK_ISNULL;
/// `SK_SEARCHNULL` (`access/skey.h`) — scankey is an `IS NULL` search.
use ::types_scan::scankey::SK_SEARCHNULL;

/// `FirstLowInvalidHeapAttributeNumber` (`access/sysattr.h`, PG 18) — `(-7)`.
const FIRST_LOW_INVALID_HEAP_ATTRIBUTE_NUMBER: i32 = -7;

/// `TYPTYPE_RANGE` (`catalog/pg_type.h`).
const TYPTYPE_RANGE: i8 = b'r' as i8;
/// `TYPTYPE_MULTIRANGE` (`catalog/pg_type.h`).
const TYPTYPE_MULTIRANGE: i8 = b'm' as i8;

/// `waitMode` argument to [`check_exclusion_or_unique_constraint`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CeoucWaitMode {
    /// `CEOUC_WAIT`.
    Wait,
    /// `CEOUC_NOWAIT`.
    NoWait,
    /// `CEOUC_LIVELOCK_PREVENTING_WAIT`.
    LivelockPreventingWait,
}

// ===========================================================================
// ExecOpenIndices
// ===========================================================================

/// `ExecOpenIndices(resultRelInfo, speculative)` — find the indices associated
/// with a result relation, open them, and save information about them in the
/// result `ResultRelInfo`. At entry the caller has already opened and locked
/// `ri_RelationDesc`.
pub fn ExecOpenIndices<'mcx>(
    estate: &mut EStateData<'mcx>,
    result_rel_info: RriId,
    speculative: bool,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    let result_relation = estate
        .result_rel(result_rel_info)
        .ri_RelationDesc
        .as_ref()
        .expect("ExecOpenIndices: ri_RelationDesc must be open")
        .alias();

    // resultRelInfo->ri_NumIndices = 0;
    estate.result_rel_mut(result_rel_info).ri_NumIndices = 0;

    // fast path if no indexes
    if !result_relation.rd_rel.relhasindex {
        return Ok(());
    }

    // Get cached list of index OIDs.
    let indexoidlist = relcache_seams::relation_get_index_list::call(mcx, &result_relation)?;
    let len = indexoidlist.len();
    if len == 0 {
        return Ok(());
    }

    // This Assert will fail if ExecOpenIndices is called twice.
    debug_assert!(estate
        .result_rel(result_rel_info)
        .ri_IndexRelationDescs
        .is_none());

    // allocate space for result arrays
    let mut relation_descs: PgVec<'mcx, Option<Relation<'mcx>>> = PgVec::new_in(mcx);
    relation_descs.try_reserve(len).map_err(|_| mcx.oom(len))?;
    let mut index_info_array: PgVec<'mcx, IndexInfo<'mcx>> = PgVec::new_in(mcx);
    index_info_array.try_reserve(len).map_err(|_| mcx.oom(len))?;

    // For each index, open the index relation (RowExclusiveLock, signifying we
    // will update the index) and save its pg_index info. We do this even if the
    // index is not indisready; it's not worth optimizing the case where it isn't.
    for &index_oid in indexoidlist.iter() {
        let index_desc = indexam_seams::index_open::call(mcx, index_oid, ROW_EXCLUSIVE_LOCK)?;

        // extract index key information from the index's pg_index info
        let mut ii = index_seams::build_index_info::call(mcx, &index_desc)?;

        // If the indexes are to be used for speculative insertion, add extra
        // information required by unique index entries.
        let indisexclusion = index_desc
            .rd_index
            .as_ref()
            .map(|i| i.indisexclusion)
            .unwrap_or(false);
        if speculative && ii.ii_Unique && !indisexclusion {
            index_seams::build_speculative_index_info::call(&index_desc, &mut ii)?;
        }

        relation_descs.push(Some(index_desc));
        index_info_array.push(ii);
    }

    let rri = estate.result_rel_mut(result_rel_info);
    rri.ri_NumIndices = len as i32;
    rri.ri_IndexRelationDescs = Some(relation_descs);
    rri.ri_IndexRelationInfo = Some(index_info_array);

    Ok(())
}

/// `RowExclusiveLock` (`storage/lockdefs.h`) — lock mode 3.
const ROW_EXCLUSIVE_LOCK: ::types_storage::lock::LOCKMODE = 3;
/// `RowExclusiveLock`'s release uses the same mode.

// ===========================================================================
// ExecCloseIndices
// ===========================================================================

/// `ExecCloseIndices(resultRelInfo)` — close the index relations stored in
/// `resultRelInfo`.
pub fn ExecCloseIndices<'mcx>(estate: &mut EStateData<'mcx>, result_rel_info: RriId) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    let num_indices = estate.result_rel(result_rel_info).ri_NumIndices;

    for i in 0..num_indices as usize {
        // Take the descriptor + its IndexInfo out so we can hand the AM a live
        // `&Relation` / `&mut IndexInfo` without aliasing the pool.
        let Some(index_desc) = take_index_desc(estate, result_rel_info, i) else {
            // This Assert will fail if ExecCloseIndices is called twice.
            panic!("ExecCloseIndices: index descriptor already closed");
        };
        let mut ii = take_index_info(estate, result_rel_info, i);

        // Give the index a chance to do some post-insert cleanup.
        {
            let mut carrier = IndexInfoCarrier::new(&mut ii);
            indexam::index_insert_cleanup(mcx, &index_desc, &mut carrier)?;
        }

        // Drop lock acquired by ExecOpenIndices, then mark the index as closed.
        index_desc.close(ROW_EXCLUSIVE_LOCK)?;

        // Put the IndexInfo back (the descriptor slot stays None = "closed");
        // FreeExecutorState cleans up the arrays.
        put_index_info(estate, result_rel_info, i, ii);
    }

    Ok(())
}

// ===========================================================================
// ExecInsertIndexTuples
// ===========================================================================

/// `ExecInsertIndexTuples(...)` — insert index tuples into all the relations
/// indexing the result relation when a heap tuple is inserted. Returns the
/// list of index OIDs for any deferred (or, with `no_dup_err`, speculative)
/// unique/exclusion constraints with potential conflicts (the C `List *`), and
/// sets `*spec_conflict` (the C `*specConflict`) when an immediate unique index
/// reported a speculative conflict.
pub fn ExecInsertIndexTuples<'mcx>(
    mcx: Mcx<'mcx>,
    estate: &mut EStateData<'mcx>,
    result_rel_info: RriId,
    slot: SlotId,
    update: bool,
    no_dup_err: bool,
    spec_conflict: Option<&mut bool>,
    arbiter_indexes: &[Oid],
    only_summarizing: bool,
) -> PgResult<PgVec<'mcx, Oid>> {
    let tupleid = estate.slot(slot).tts_tid;
    let mut result: PgVec<'mcx, Oid> = PgVec::new_in(mcx);
    let mut spec_conflict_flag = false;

    debug_assert!(item_pointer_is_valid(&tupleid));

    let num_indices = estate.result_rel(result_rel_info).ri_NumIndices;
    let heap_relation = result_rel_alias(estate, result_rel_info);

    // Sanity check: slot must belong to the same rel as the resultRelInfo.
    debug_assert_eq!(estate.slot(slot).tts_tableOid, heap_relation.rd_id);

    // We use the EState's per-tuple context for evaluating predicates and index
    // expressions; arrange for its scan tuple to be the tuple under test.
    let econtext = execUtils::MakePerTupleExprContext(estate)?;
    estate.ecxt_mut(econtext).ecxt_scantuple = Some(slot);

    // for each index, form and insert the index tuple
    for i in 0..num_indices as usize {
        let Some(index_relation) = take_index_desc(estate, result_rel_info, i) else {
            continue;
        };
        let mut index_info = take_index_info(estate, result_rel_info, i);

        // Run the per-index body with the descriptor + info as owned locals,
        // restoring them afterward regardless of outcome.
        let body = insert_one_index(
            mcx,
            estate,
            result_rel_info,
            i,
            &mut index_info,
            &index_relation,
            &heap_relation,
            slot,
            &tupleid,
            econtext,
            update,
            no_dup_err,
            arbiter_indexes,
            only_summarizing,
        );

        // Restore the moved-out values.
        put_index_desc(estate, result_rel_info, i, index_relation);
        put_index_info(estate, result_rel_info, i, index_info);

        let Some((recheck_oid, spec)) = body? else {
            continue;
        };
        if let Some(oid) = recheck_oid {
            result.try_reserve(1).map_err(|_| mcx.oom(1))?;
            result.push(oid);
        }
        if spec {
            spec_conflict_flag = true;
        }
    }

    if let Some(out) = spec_conflict {
        *out = spec_conflict_flag;
    }
    Ok(result)
}

/// Per-index body of [`ExecInsertIndexTuples`]. Returns `None` when the index
/// is skipped (read-only / non-summarizing / predicate-not-satisfied), or
/// `Some((recheck_oid, spec_conflict))` where `recheck_oid` is the index OID to
/// re-check later (the C `lappend_oid`) when present.
fn insert_one_index<'mcx>(
    mcx: Mcx<'mcx>,
    estate: &mut EStateData<'mcx>,
    result_rel_info: RriId,
    i: usize,
    index_info: &mut IndexInfo<'mcx>,
    index_relation: &Relation<'mcx>,
    heap_relation: &Relation<'mcx>,
    slot: SlotId,
    tupleid: &ItemPointerData,
    econtext: EcxtId,
    update: bool,
    no_dup_err: bool,
    arbiter_indexes: &[Oid],
    only_summarizing: bool,
) -> PgResult<Option<(Option<Oid>, bool)>> {
    // If the index is marked as read-only, ignore it.
    if !index_info.ii_ReadyForInserts {
        return Ok(None);
    }

    // Skip non-summarizing indexes if we only update summarizing indexes.
    if only_summarizing && !index_info.ii_Summarizing {
        return Ok(None);
    }

    // Check for partial index.
    if index_info.ii_Predicate.is_some() {
        // If predicate state not set up yet, create it (in the estate's
        // per-query context).
        if index_info.ii_PredicateState.is_none() {
            let qual = index_info.ii_Predicate.as_deref();
            index_info.ii_PredicateState = expr_seams::exec_prepare_qual::call(qual, estate)?;
        }
        // Skip this index-update if the predicate isn't satisfied.
        if !exec_qual_opt(estate, index_info.ii_PredicateState.as_deref_mut(), econtext)? {
            return Ok(None);
        }
    }

    // FormIndexDatum fills the values/isnull for the index column(s).
    let (values, isnull) = index_seams::form_index_datum::call(index_info, slot, estate)?;

    // Check whether to apply no_dup_err to this index. (C reads
    // indexRelation->rd_index->indexrelid, which is the index's own OID.)
    let index_relid = index_relation.rd_id;
    let apply_no_dup_err =
        no_dup_err && (arbiter_indexes.is_empty() || arbiter_indexes.contains(&index_relid));

    let indisunique = index_relation.rd_index.as_ref().map(|i| i.indisunique).unwrap_or(false);
    let indimmediate = index_relation.rd_index.as_ref().map(|i| i.indimmediate).unwrap_or(false);

    // The index AM does the actual insertion plus uniqueness checking.
    let check_unique = if !indisunique {
        IndexUniqueCheck::UNIQUE_CHECK_NO
    } else if apply_no_dup_err {
        IndexUniqueCheck::UNIQUE_CHECK_PARTIAL
    } else if indimmediate {
        IndexUniqueCheck::UNIQUE_CHECK_YES
    } else {
        IndexUniqueCheck::UNIQUE_CHECK_PARTIAL
    };

    // There's definitely going to be an index_insert() call for this index. If
    // we're part of an UPDATE, consider the 'indexUnchanged' hint.
    let index_unchanged = update
        && index_unchanged_by_update(estate, result_rel_info, i, index_info, index_relation)?;

    // FormIndexDatum yields the canonical per-attribute Datum (a by-reference
    // key crosses as its `ByRef` byte image); index_insert / the ScanKey /
    // BuildIndexValueDescription all consume it directly.
    let num_index_attrs = index_info.ii_NumIndexAttrs as usize;

    let mut satisfies_constraint = {
        let mut carrier = IndexInfoCarrier::new(index_info);
        indexam::index_insert(
            mcx,
            index_relation,
            &values[..num_index_attrs],
            &isnull[..num_index_attrs],
            tupleid,
            heap_relation,
            check_unique,
            index_unchanged,
            &mut carrier,
        )?
    };

    // If the index has an associated exclusion constraint, check that.
    let has_exclusion_ops = index_info.ii_ExclusionOps.is_some();
    if has_exclusion_ops {
        let (violation_ok, wait_mode) = if apply_no_dup_err {
            (true, CeoucWaitMode::LivelockPreventingWait)
        } else if !indimmediate {
            (true, CeoucWaitMode::NoWait)
        } else {
            (false, CeoucWaitMode::Wait)
        };

        let (sat, _conflict_tid) = check_exclusion_or_unique_constraint(
            mcx,
            estate,
            heap_relation,
            index_relation,
            index_info,
            Some(tupleid),
            &values,
            &isnull,
            false,
            wait_mode,
            violation_ok,
            false,
        )?;
        satisfies_constraint = sat;
    }

    if (check_unique == IndexUniqueCheck::UNIQUE_CHECK_PARTIAL || has_exclusion_ops)
        && !satisfies_constraint
    {
        // The tuple potentially violates the uniqueness or exclusion
        // constraint; note the index for a re-check later. Speculative inserters
        // are told if there was a speculative conflict (always needs a restart).
        let recheck = index_relation.rd_id;
        let spec = indimmediate;
        return Ok(Some((Some(recheck), spec)));
    }

    Ok(Some((None, false)))
}

// ===========================================================================
// ExecCheckIndexConstraints
// ===========================================================================

/// `ExecCheckIndexConstraints(...)` — check if a tuple violates any unique or
/// exclusion constraints. Returns `true` if there is no conflict; otherwise
/// `false` and writes the conflicting tuple's TID into `conflict_tid`. With
/// `arbiter_indexes` nonempty only those indexes are checked. `tupleid` is the
/// TID of the recently-inserted tuple (invalid sentinel if none yet), excluded
/// from conflict checking.
pub fn ExecCheckIndexConstraints<'mcx>(
    mcx: Mcx<'mcx>,
    estate: &mut EStateData<'mcx>,
    result_rel_info: RriId,
    slot: SlotId,
    conflict_tid: &mut ItemPointerData,
    tupleid: &ItemPointerData,
    arbiter_indexes: &[Oid],
) -> PgResult<bool> {
    *conflict_tid = ItemPointerData::default(); // ItemPointerSetInvalid(conflictTid)
    let mut checked_index = false;

    let num_indices = estate.result_rel(result_rel_info).ri_NumIndices;
    let heap_relation = result_rel_alias(estate, result_rel_info);

    let econtext = execUtils::MakePerTupleExprContext(estate)?;
    estate.ecxt_mut(econtext).ecxt_scantuple = Some(slot);

    for i in 0..num_indices as usize {
        let Some(index_relation) = take_index_desc(estate, result_rel_info, i) else {
            continue;
        };
        let mut index_info = take_index_info(estate, result_rel_info, i);

        let body = check_one_index_constraint(
            mcx,
            estate,
            &mut index_info,
            &index_relation,
            &heap_relation,
            slot,
            tupleid,
            econtext,
            arbiter_indexes,
            &mut checked_index,
        );

        put_index_desc(estate, result_rel_info, i, index_relation);
        put_index_info(estate, result_rel_info, i, index_info);

        match body? {
            // Conflict found: report it.
            Some(found_tid) => {
                *conflict_tid = found_tid;
                return Ok(false);
            }
            None => {}
        }
    }

    if !arbiter_indexes.is_empty() && !checked_index {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INTERNAL_ERROR)
            .errmsg_internal("unexpected failure to find arbiter index")
            .into_error());
    }

    Ok(true)
}

/// Per-index body of [`ExecCheckIndexConstraints`]. `Ok(Some(tid))` means a
/// conflict was found (the caller returns `false`); `Ok(None)` means this index
/// is satisfied or skipped.
fn check_one_index_constraint<'mcx>(
    mcx: Mcx<'mcx>,
    estate: &mut EStateData<'mcx>,
    index_info: &mut IndexInfo<'mcx>,
    index_relation: &Relation<'mcx>,
    heap_relation: &Relation<'mcx>,
    slot: SlotId,
    tupleid: &ItemPointerData,
    econtext: EcxtId,
    arbiter_indexes: &[Oid],
    checked_index: &mut bool,
) -> PgResult<Option<ItemPointerData>> {
    if !index_info.ii_Unique && index_info.ii_ExclusionOps.is_none() {
        return Ok(None);
    }

    // If the index is marked as read-only, ignore it.
    if !index_info.ii_ReadyForInserts {
        return Ok(None);
    }

    let indimmediate = index_relation.rd_index.as_ref().map(|i| i.indimmediate).unwrap_or(false);
    let index_relid = index_relation.rd_id;

    // When specific arbiter indexes requested, only examine them.
    if !arbiter_indexes.is_empty() && !arbiter_indexes.contains(&index_relid) {
        return Ok(None);
    }

    if !indimmediate {
        // errtableconstraint(heap, RelationGetRelationName(index)) context-attach
        // is a project-wide error-context gap; the user-visible message/SQLSTATE
        // are reproduced verbatim.
        let _ = (heap_relation, index_relation);
        let err = ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg("ON CONFLICT does not support deferrable unique constraints/exclusion constraints as arbiters")
            .into_error();
        return Err(err);
    }

    *checked_index = true;

    // Check for partial index.
    if index_info.ii_Predicate.is_some() {
        if index_info.ii_PredicateState.is_none() {
            let qual = index_info.ii_Predicate.as_deref();
            index_info.ii_PredicateState = expr_seams::exec_prepare_qual::call(qual, estate)?;
        }
        if !exec_qual_opt(estate, index_info.ii_PredicateState.as_deref_mut(), econtext)? {
            return Ok(None);
        }
    }

    let (values, isnull) = index_seams::form_index_datum::call(index_info, slot, estate)?;

    let (satisfies_constraint, found_tid) = check_exclusion_or_unique_constraint(
        mcx,
        estate,
        heap_relation,
        index_relation,
        index_info,
        Some(tupleid),
        &values,
        &isnull,
        false,
        CeoucWaitMode::Wait,
        true,
        true,
    )?;
    if !satisfies_constraint {
        return Ok(Some(found_tid));
    }

    Ok(None)
}

// ===========================================================================
// check_exclusion_or_unique_constraint
// ===========================================================================

/// Check for violation of an exclusion or unique constraint. Returns
/// `(true, _)` if OK, `(false, conflict_tid)` if actual or potential violation
/// (the TID is meaningful only when `want_conflict_tid`). See the C source for
/// the full semantics.
#[allow(clippy::collapsible_else_if)]
fn check_exclusion_or_unique_constraint<'mcx>(
    mcx: Mcx<'mcx>,
    estate: &mut EStateData<'mcx>,
    heap: &Relation<'mcx>,
    index: &Relation<'mcx>,
    index_info: &IndexInfo<'mcx>,
    tupleid: Option<&ItemPointerData>,
    values: &[DatumV<'mcx>; INDEX_MAX_KEYS as usize],
    isnull: &[bool; INDEX_MAX_KEYS as usize],
    new_index: bool,
    wait_mode: CeoucWaitMode,
    violation_ok: bool,
    want_conflict_tid: bool,
) -> PgResult<(bool, ItemPointerData)> {
    let mut conflict_tid = ItemPointerData::default();

    let has_exclusion_ops = index_info.ii_ExclusionOps.is_some();
    let (constr_procs, constr_strats): (&PgVec<'mcx, Oid>, &PgVec<'mcx, u16>) = if has_exclusion_ops
    {
        (
            index_info
                .ii_ExclusionProcs
                .as_ref()
                .expect("ii_ExclusionProcs present with ii_ExclusionOps"),
            index_info
                .ii_ExclusionStrats
                .as_ref()
                .expect("ii_ExclusionStrats present with ii_ExclusionOps"),
        )
    } else {
        (
            index_info
                .ii_UniqueProcs
                .as_ref()
                .expect("ii_UniqueProcs present for a unique-constraint check"),
            index_info
                .ii_UniqueStrats
                .as_ref()
                .expect("ii_UniqueStrats present for a unique-constraint check"),
        )
    };

    let index_collations = &index.rd_indcollation;
    let indnkeyatts = index.indnkeyatts() as usize;

    // If this is a WITHOUT OVERLAPS constraint, we must also forbid empty
    // ranges/multiranges. This must happen before we look for NULLs below, or a
    // UNIQUE constraint could insert an empty range along with a NULL scalar.
    if index_info.ii_WithoutOverlaps {
        // Look up the type from the heap tuple, but check the Datum from the
        // index tuple.
        let attno = index_info.ii_IndexAttrNumbers[indnkeyatts - 1];

        if !isnull[indnkeyatts - 1] {
            let tupdesc = &heap.rd_att;
            let att = tupdesc.attr((attno - 1) as usize);
            let atttypid = att.atttypid;
            let attname = alloc::string::String::from_utf8_lossy(att.attname.name_str()).into_owned();
            let typtype = typcache_seams::type_cache_typtype::call(atttypid)?;

            ExecWithoutOverlapsNotEmpty(mcx, heap, &attname, &values[indnkeyatts - 1], typtype)?;
        }
    }

    // If any input values are NULL and the index uses the default
    // nulls-are-distinct mode, the constraint check is assumed to pass (the
    // operators are strict). Otherwise we interpret the constraint as IS NULL
    // for each NULL column.
    if !index_info.ii_NullsNotDistinct {
        for i in 0..indnkeyatts {
            if isnull[i] {
                return Ok((true, conflict_tid));
            }
        }
    }

    // Build the scan keys; the search covers tuples not yet visible (dirty
    // snapshot).
    let mut scankeys: alloc::vec::Vec<ScanKeyData<'mcx>> = alloc::vec::Vec::new();
    scankeys.try_reserve(indnkeyatts).map_err(|_| mcx.oom(indnkeyatts))?;
    for i in 0..indnkeyatts {
        let mut entry = ScanKeyData::empty();
        scankey::ScanKeyEntryInitialize(
            &mut entry,
            if isnull[i] {
                SK_ISNULL | SK_SEARCHNULL
            } else {
                0
            },
            (i + 1) as i16,
            constr_strats[i],
            0, // InvalidOid
            index_collations[i],
            constr_procs[i] as ::types_core::primitive::RegProcedure,
            // Carry the canonical per-attribute value into the scan key. A
            // by-reference key (text/numeric/uuid/…) crosses as its `ByRef`
            // byte image; collapsing it to a bare word would panic the scalar
            // accessor on a by-ref value.
            values[i].clone_in(mcx)?,
        )?;
        scankeys.push(entry);
    }

    // Need a TupleTableSlot to put existing tuples in; address it by id in the
    // EState pool so FormIndexDatum/ecxt_scantuple can reference it. (C frees
    // this standalone slot at the end; in the owned pool model the slot stays
    // in es_tupleTable and is reclaimed at FreeExecutorState — the same
    // compromise the other FormIndexDatum callers make.)
    let existing_slot_data = tableam::table_slot_create(mcx, heap)?;
    let existing_slot = estate.push_slot_data(existing_slot_data)?;

    let per_tuple = econtext_of(estate);
    let save_scantuple = estate.ecxt(per_tuple).ecxt_scantuple;
    estate.ecxt_mut(per_tuple).ecxt_scantuple = Some(existing_slot);

    // May have to restart scan from this point on a potential conflict.
    let result = exclusion_scan_loop(
        mcx,
        estate,
        heap,
        index,
        index_info,
        constr_procs,
        tupleid,
        values,
        isnull,
        existing_slot,
        &scankeys,
        indnkeyatts,
        has_exclusion_ops,
        new_index,
        wait_mode,
        violation_ok,
        want_conflict_tid,
        &mut conflict_tid,
    );

    // Restore the caller's scantuple (the existing-tuple slot is reclaimed with
    // the EState; see the create comment).
    estate.ecxt_mut(per_tuple).ecxt_scantuple = save_scantuple;

    // C: `ExecDropSingleTupleTableSlot(existing_slot);` (execIndexing.c:944). The
    // owned model keeps `existing_slot` in the EState pool (it can't drop a single
    // pool slot), but its `index_getnext_slot` fetch pins a heap buffer; since
    // FreeExecutorState does NOT release buffer pins, that pin would leak
    // ("resource was not closed") when the deferred-exclusion `unique_key_recheck`
    // path runs this and the EState is later only torn down (not reset). Clear the
    // slot here to release the pin, on both the conflict and no-conflict paths.
    execTuples_seams::exec_clear_tuple::call(estate, existing_slot)?;

    result
}

/// The `retry:`/scan loop of [`check_exclusion_or_unique_constraint`].
fn exclusion_scan_loop<'mcx>(
    mcx: Mcx<'mcx>,
    estate: &mut EStateData<'mcx>,
    heap: &Relation<'mcx>,
    index: &Relation<'mcx>,
    index_info: &IndexInfo<'mcx>,
    constr_procs: &PgVec<'mcx, Oid>,
    tupleid: Option<&ItemPointerData>,
    values: &[DatumV<'mcx>; INDEX_MAX_KEYS as usize],
    isnull: &[bool; INDEX_MAX_KEYS as usize],
    existing_slot: SlotId,
    scankeys: &[ScanKeyData<'mcx>],
    indnkeyatts: usize,
    has_exclusion_ops: bool,
    new_index: bool,
    wait_mode: CeoucWaitMode,
    violation_ok: bool,
    want_conflict_tid: bool,
    conflict_tid: &mut ItemPointerData,
) -> PgResult<(bool, ItemPointerData)> {
    'retry: loop {
        let mut conflict = false;
        let mut found_self = false;

        // InitDirtySnapshot(DirtySnapshot): a dirty snapshot whose satisfies-fn
        // is HeapTupleSatisfiesDirty (only the type discriminates here; the AM
        // fills xmin/xmax/speculativeToken during the scan).
        let dirty = SnapshotData::sentinel(SnapshotType::SNAPSHOT_DIRTY);
        let mut index_scan = indexam::index_beginscan(
            mcx,
            heap,
            index,
            dirty,
            None,
            indnkeyatts as i32,
            0,
        )?;
        // Re-key with the freshly-built scan keys.
        let keys_owned: alloc::vec::Vec<ScanKeyData<'mcx>> = {
            let mut v = alloc::vec::Vec::new();
            v.try_reserve(indnkeyatts).map_err(|_| mcx.oom(indnkeyatts))?;
            for k in &scankeys[..indnkeyatts] {
                v.push(k.clone_in(mcx)?);
            }
            v
        };
        indexam::index_rescan(mcx, &mut index_scan, &keys_owned, indnkeyatts as i32, &[], 0)?;

        loop {
            if !indexam_seams::index_getnext_slot::call(
                &mut index_scan,
                ScanDirection::ForwardScanDirection,
                estate,
                existing_slot,
            )? {
                break;
            }

            let existing_tid = estate.slot(existing_slot).tts_tid;

            // Ignore the entry for the tuple we're trying to check.
            if let Some(tid) = tupleid {
                if item_pointer_is_valid(tid) && *tid == existing_tid {
                    if found_self {
                        // should not happen
                        return Err(ereport(ERROR)
                            .errcode(ERRCODE_INTERNAL_ERROR)
                            .errmsg_internal(alloc::format!(
                                "found self tuple multiple times in index \"{}\"",
                                index.name()
                            ))
                            .into_error());
                    }
                    found_self = true;
                    continue;
                }
            }

            // Extract the index column values/isnull from the existing tuple.
            let (existing_values, existing_isnull) =
                index_seams::form_index_datum::call(index_info, existing_slot, estate)?;

            // If lossy indexscan, must recheck the condition.
            if index_scan.xs_recheck
                && !index_recheck_constraint(
                    mcx,
                    index,
                    constr_procs,
                    &existing_values,
                    &existing_isnull,
                    values,
                    indnkeyatts,
                )?
            {
                // tuple doesn't actually match, so no conflict
                continue;
            }

            // At this point we have either a conflict or a potential conflict.
            // If an in-progress transaction affects this tuple's visibility, we
            // wait for it and recheck by restarting the whole scan (unless the
            // caller requested not to).
            let snap = index_scan
                .xs_snapshot
                .clone()
                .expect("index scan has a dirty snapshot");
            let xwait = if snap.xmin != 0 { snap.xmin } else { snap.xmax };

            if xwait != 0
                && (wait_mode == CeoucWaitMode::Wait
                    || (wait_mode == CeoucWaitMode::LivelockPreventingWait
                        && snap.speculativeToken != 0
                        && transam_seams::transaction_id_precedes::call(
                            xact_seams::get_current_transaction_id::call()?,
                            xwait,
                        )))
            {
                let reason_wait = if has_exclusion_ops {
                    XLTW_Oper::RecheckExclusionConstr
                } else {
                    XLTW_Oper::InsertIndex
                };
                indexam::index_endscan(mcx, index_scan)?;
                if snap.speculativeToken != 0 {
                    lmgr_seams::speculative_insertion_wait::call(snap.xmin, snap.speculativeToken)?;
                } else {
                    lmgr_seams::xact_lock_table_wait::call(
                        xwait,
                        heap.name().to_string(),
                        existing_tid,
                        reason_wait,
                    )?;
                }
                continue 'retry;
            }

            // We have a definite conflict (or a potential one, but the caller
            // didn't want to wait). Return it, or report it.
            if violation_ok {
                conflict = true;
                if want_conflict_tid {
                    *conflict_tid = existing_tid;
                }
                break;
            }

            // Build the error and raise it (errtableconstraint context-attach is
            // a project-wide error-context gap; message/detail are verbatim).
            let error_new = genam_seams::build_index_value_description::call(
                mcx,
                index,
                &values[..indnkeyatts],
                &isnull[..indnkeyatts],
            )?;
            let error_existing = genam_seams::build_index_value_description::call(
                mcx,
                index,
                &existing_values[..indnkeyatts],
                &existing_isnull[..indnkeyatts],
            )?;
            let index_name = index.name();
            let (errmsg, detail) = if new_index {
                let detail = match (&error_new, &error_existing) {
                    (Some(n), Some(e)) => {
                        alloc::format!("Key {} conflicts with key {}.", n.as_str(), e.as_str())
                    }
                    _ => "Key conflicts exist.".to_string(),
                };
                (
                    alloc::format!("could not create exclusion constraint \"{}\"", index_name),
                    detail,
                )
            } else {
                let detail = match (&error_new, &error_existing) {
                    (Some(n), Some(e)) => alloc::format!(
                        "Key {} conflicts with existing key {}.",
                        n.as_str(),
                        e.as_str()
                    ),
                    _ => "Key conflicts with existing key.".to_string(),
                };
                (
                    alloc::format!(
                        "conflicting key value violates exclusion constraint \"{}\"",
                        index_name
                    ),
                    detail,
                )
            };
            // errtableconstraint(heap, RelationGetRelationName(index)) attach is
            // a project-wide error-context gap; message/detail are verbatim.
            return Err(ereport(ERROR)
                .errcode(ERRCODE_EXCLUSION_VIOLATION)
                .errmsg(errmsg)
                .errdetail(detail)
                .into_error());
        }

        indexam::index_endscan(mcx, index_scan)?;

        // Ordinarily the search should have found the originally-inserted tuple,
        // but some exclusion operators (e.g. <>) make that untrue, so we no
        // longer complain if found_self is still false.
        let _ = found_self;
        return Ok((!conflict, *conflict_tid));
    }
}

// ===========================================================================
// IndexCheckExclusion
// ===========================================================================

/// `IndexCheckExclusion(heapRelation, indexRelation, indexInfo)` (catalog/index.c).
///
/// After building an exclusion-constraint index, make a second pass over the
/// heap to verify that the constraint is satisfied: scan all live tuples in the
/// base relation, form each one's index values, and probe the (now fully built)
/// index via [`check_exclusion_constraint`], which raises a clean
/// `exclusion_violation` `ERROR` on a conflict.
///
/// This is the body the `index_check_exclusion` seam (catalog-index-seams) is
/// installed with from this crate's [`init_seams`]; the catalog `index_build`
/// driver calls it through that seam for an exclusion index. (Homed here, not in
/// catalog/index.c's crate, because it needs the executor table-scan +
/// `check_exclusion_constraint` substrate this crate owns — exactly why the seam
/// header pins its owner to the executor layer.)
pub fn IndexCheckExclusion<'mcx>(
    mcx: Mcx<'mcx>,
    heap_relation: &Relation<'mcx>,
    index_relation: &Relation<'mcx>,
    index_info: &IndexInfo<'mcx>,
) -> PgResult<()> {
    // If we are reindexing the target index, mark it as no longer being
    // reindexed, to forestall an Assert in index_beginscan when we try to use
    // the index for probes. This is OK because the index is now fully valid.
    if index_seams::reindex_is_currently_processing_index::call(index_relation.rd_id) {
        index_seams::reset_reindex_processing::call();
    }

    // Need an EState for evaluation of index expressions and partial-index
    // predicates. Also a slot to hold the current tuple. The owned model builds
    // the EState directly in the caller's `mcx` (which plays the role of C's
    // private per-query "ExecutorState" context — the surrounding index_build
    // transaction owns and reclaims it); we run the same non-memory teardown as
    // `FreeExecutorState` (firing ExprContext shutdown callbacks) before
    // returning.
    let mut estate = EStateData::new_in(mcx);

    // econtext = GetPerTupleExprContext(estate)
    let econtext = execUtils::MakePerTupleExprContext(&mut estate)?;

    // slot = table_slot_create(heapRelation, NULL); held in the EState pool so
    // FormIndexDatum/ecxt_scantuple can address it by id, and reclaimed with the
    // EState (the same compromise the crate's other FormIndexDatum callers make
    // for their standalone slots).
    let slot_data = tableam::table_slot_create(mcx, heap_relation)?;
    let slot = estate.push_slot_data(slot_data)?;

    // Arrange for econtext's scan tuple to be the tuple under test.
    estate.ecxt_mut(econtext).ecxt_scantuple = Some(slot);

    // Set up execution state for predicate, if any.
    let mut predicate = expr_seams::exec_prepare_qual::call(
        index_info.ii_Predicate.as_deref(),
        &mut estate,
    )?;

    // Run the scan body, capturing its result so the EState teardown runs on
    // either outcome (mirroring C's straight-line cleanup; the C version lets an
    // ereport escape mid-teardown, but reclamation is via the surrounding
    // context either way).
    let result = index_check_exclusion_scan(
        mcx,
        &mut estate,
        heap_relation,
        index_relation,
        index_info,
        econtext,
        slot,
        predicate.as_deref_mut(),
    );

    // ExecDropSingleTupleTableSlot(slot): clear the scan slot to release the
    // buffer pin a BufferHeapTupleTableSlot holds on the last fetched heap page
    // (its ExecClearTuple -> tts_buffer_heap_clear -> ReleaseBuffer), so the pin
    // is dropped from the resource owner before commit (else "resource was not
    // closed" at REINDEX-transaction end). C drops this single slot; here we also
    // clear `check_exclusion_constraint`'s internal `existing_slot` (which the
    // owned model leaves in the EState pool rather than dropping standalone), so
    // ExecResetTupleTable over the whole pool is the faithful release. Run this
    // even on the scan body's error path. The slot structs are reclaimed with the
    // EState's `mcx`.
    let drop_slot = execUtils::ExecResetTupleTable(&mut estate, false);

    // FreeExecutorState(estate): fire any ExprContext shutdown callbacks / release
    // JIT + partition directory. The per-query memory is reclaimed when the
    // caller resets its `mcx`.
    let teardown = execUtils::free_executor_state_teardown(&mut estate);

    // ii_Expressions/PredicateState pointed into the now-gone estate; in the
    // owned model `index_info` is borrowed immutably (the caller's IndexInfo), so
    // the C `indexInfo->ii_ExpressionsState = NIL / ii_PredicateState = NULL`
    // reset is performed by the caller (index_build) against its owned copy when
    // it drops the EState-tied states — no aliasing write needed here.

    result?;
    drop_slot?;
    teardown
}

/// The heap-rescan loop of [`IndexCheckExclusion`], factored out so the EState
/// teardown in the caller runs on either outcome.
fn index_check_exclusion_scan<'mcx>(
    mcx: Mcx<'mcx>,
    estate: &mut EStateData<'mcx>,
    heap_relation: &Relation<'mcx>,
    index_relation: &Relation<'mcx>,
    index_info: &IndexInfo<'mcx>,
    econtext: EcxtId,
    slot: SlotId,
    mut predicate: Option<&mut ::nodes::execexpr::ExprState<'mcx>>,
) -> PgResult<()> {
    // Scan all live tuples in the base relation.
    // snapshot = RegisterSnapshot(GetLatestSnapshot())
    let snapshot =
        snapmgr_seams::register_snapshot::call(snapmgr_seams::get_latest_snapshot::call()?)?;

    let mut scan = tableam::table_beginscan_strat(
        mcx,
        heap_relation,
        Some(snapshot.clone()),
        0,                       // number of keys
        PgVec::new_in(mcx),      // scan key
        true,                    // buffer access strategy OK
        true,                    // syncscan OK
    )?;

    let body = (|| -> PgResult<()> {
        loop {
            // table_scan_getnextslot(scan, ForwardScanDirection, slot)
            if !tableam::table_scan_getnextslot(
                mcx,
                &mut scan,
                ScanDirection::ForwardScanDirection,
                estate.slot_data_mut(slot),
            )? {
                break;
            }

            // CHECK_FOR_INTERRUPTS(): cooperative-cancellation point; the owned
            // model has no signal machinery reachable here (procsignal unported),
            // so it is a no-op.

            // In a partial index, ignore tuples that don't satisfy the predicate.
            if let Some(pred) = predicate.as_deref_mut() {
                if !expr_seams::exec_qual::call(pred, econtext, estate)? {
                    continue;
                }
            }

            // Extract index column values, including computing expressions.
            let (values, isnull) =
                index_seams::form_index_datum::call(index_info, slot, estate)?;

            // Check that this tuple has no conflicts.
            let tupleid = estate.slot(slot).tts_tid;
            check_exclusion_constraint(
                mcx,
                estate,
                heap_relation,
                index_relation,
                index_info,
                Some(&tupleid),
                &values,
                &isnull,
                true, // newIndex
            )?;

            // MemoryContextReset(econtext->ecxt_per_tuple_memory)
            execUtils::ResetExprContext(estate.ecxt_mut(econtext));
        }
        Ok(())
    })();

    // table_endscan(scan); UnregisterSnapshot(snapshot) — run regardless of the
    // loop's outcome so the scan/snapshot resources are released even on a
    // constraint-violation error (then propagate the body's result).
    tableam::table_endscan(scan)?;
    snapmgr_seams::unregister_snapshot::call(snapshot);

    body
}

// ===========================================================================
// check_exclusion_constraint
// ===========================================================================

/// `check_exclusion_constraint(...)` — a dumbed-down version of
/// [`check_exclusion_or_unique_constraint`] for external callers (the
/// `IndexCheckExclusion` second pass; they don't need the special modes).
///
/// Exported C API (`extern` in `executor/executor.h`). Its only caller,
/// `IndexCheckExclusion` (catalog/index.c), drives the heap re-scan and is not
/// yet ported; until it lands this has no in-repo caller (hence `pub`).
pub fn check_exclusion_constraint<'mcx>(
    mcx: Mcx<'mcx>,
    estate: &mut EStateData<'mcx>,
    heap: &Relation<'mcx>,
    index: &Relation<'mcx>,
    index_info: &IndexInfo<'mcx>,
    tupleid: Option<&ItemPointerData>,
    values: &[DatumV<'mcx>; INDEX_MAX_KEYS as usize],
    isnull: &[bool; INDEX_MAX_KEYS as usize],
    new_index: bool,
) -> PgResult<()> {
    check_exclusion_or_unique_constraint(
        mcx,
        estate,
        heap,
        index,
        index_info,
        tupleid,
        values,
        isnull,
        new_index,
        CeoucWaitMode::Wait,
        false,
        false,
    )?;
    Ok(())
}

// ===========================================================================
// index_recheck_constraint
// ===========================================================================

/// Check existing tuple's index values to see if it really matches the
/// exclusion condition against the `new_values`. Returns true if conflict.
fn index_recheck_constraint<'mcx>(
    mcx: Mcx<'mcx>,
    index: &Relation<'mcx>,
    constr_procs: &PgVec<'mcx, Oid>,
    existing_values: &[DatumV<'mcx>; INDEX_MAX_KEYS as usize],
    existing_isnull: &[bool; INDEX_MAX_KEYS as usize],
    new_values: &[DatumV<'mcx>; INDEX_MAX_KEYS as usize],
    indnkeyatts: usize,
) -> PgResult<bool> {
    for i in 0..indnkeyatts {
        // Assume the exclusion operators are strict.
        if existing_isnull[i] {
            return Ok(false);
        }

        // !DatumGetBool(OidFunctionCall2Coll(constr_procs[i],
        //     index->rd_indcollation[i], existing_values[i], new_values[i]))
        // Cross the canonical-value lane so a by-reference key (range, etc.)
        // carries its byte image into fmgr instead of a bare word.
        let res = fmgr_seams::function_call2_coll_datum::call(
            mcx,
            constr_procs[i],
            index.rd_indcollation[i],
            existing_values[i].clone_in(mcx)?,
            new_values[i].clone_in(mcx)?,
        )?;
        if !res.as_bool() {
            return Ok(false);
        }
    }

    Ok(true)
}

// ===========================================================================
// index_unchanged_by_update
// ===========================================================================

/// Check if `ExecInsertIndexTuples` should pass the `indexUnchanged` hint for
/// one single index when the executor performs an UPDATE that requires a new
/// round of index tuples.
fn index_unchanged_by_update<'mcx>(
    estate: &mut EStateData<'mcx>,
    result_rel_info: RriId,
    _index_i: usize,
    index_info: &mut IndexInfo<'mcx>,
    index_relation: &Relation<'mcx>,
) -> PgResult<bool> {
    let mcx = estate.es_query_cxt;

    // Check cache first.
    if index_info.ii_CheckedUnchanged {
        return Ok(index_info.ii_IndexUnchanged);
    }
    index_info.ii_CheckedUnchanged = true;

    // Check for indexed key-attribute overlap with updated columns. (Only key
    // columns: a change to an INCLUDE non-key column doesn't count.)
    let updated_cols = execUtils::ExecGetUpdatedCols(estate, result_rel_info, mcx)?;
    let extra_updated_cols = execUtils::ExecGetExtraUpdatedCols(estate, result_rel_info, mcx)?;

    let mut hasexpression = false;
    for attr in 0..index_info.ii_NumIndexKeyAttrs as usize {
        let keycol = index_info.ii_IndexAttrNumbers[attr] as i32;

        if keycol <= 0 {
            // Skip expressions for now, but remember to deal with them later.
            hasexpression = true;
            continue;
        }

        if bms_is_member(
            keycol - FIRST_LOW_INVALID_HEAP_ATTRIBUTE_NUMBER,
            updated_cols.as_deref(),
        ) || bms_is_member(
            keycol - FIRST_LOW_INVALID_HEAP_ATTRIBUTE_NUMBER,
            extra_updated_cols.as_deref(),
        ) {
            // Changed key column -- don't hint for this index.
            index_info.ii_IndexUnchanged = false;
            return Ok(false);
        }
    }

    // No expressions and no key overlap -> the index is logically unchanged.
    if !hasexpression {
        index_info.ii_IndexUnchanged = true;
        return Ok(true);
    }

    // Need to pass only one bms to the walker; avoid allocating when there are
    // no extra cols.
    let has_extra = extra_updated_cols.is_some();
    let all_updated_cols = if !has_extra {
        // (C: allUpdatedCols = updatedCols) — alias, no copy/free.
        updated_cols
    } else {
        bms_union(mcx, updated_cols.as_deref(), extra_updated_cols.as_deref())?
    };

    // Try to find Vars in the indexed expressions that overlap known-updated
    // columns. If any match, don't pass the hint.
    let idx_exprs = relcache_seams::relation_get_index_expressions::call(mcx, index_relation)?;
    hasexpression = index_expressions_changed(idx_exprs.as_deref(), all_updated_cols.as_deref());

    if has_extra {
        bms_free(all_updated_cols);
    }

    if hasexpression {
        index_info.ii_IndexUnchanged = false;
        return Ok(false);
    }

    // Deliberately don't consider index predicates.
    index_info.ii_IndexUnchanged = true;
    Ok(true)
}

/// Run [`index_expression_changed_walker`] over a `List *` of indexed
/// expression trees (`RelationGetIndexExpressions` output).
fn index_expressions_changed(
    idx_exprs: Option<&[::nodes::primnodes::Expr]>,
    all_updated_cols: Option<&::nodes::bitmapset::Bitmapset>,
) -> bool {
    let Some(exprs) = idx_exprs else {
        return false;
    };
    for e in exprs.iter() {
        if index_expression_changed_walker(Some(e), all_updated_cols) {
            return true;
        }
    }
    false
}

/// Indexed-expression helper for [`index_unchanged_by_update`]. Returns true
/// when a `Var` that appears within `all_updated_cols` is located.
fn index_expression_changed_walker(
    node: Option<&::nodes::primnodes::Expr>,
    all_updated_cols: Option<&::nodes::bitmapset::Bitmapset>,
) -> bool {
    let Some(node) = node else {
        return false;
    };

    if let ::nodes::primnodes::Expr::Var(var) = node {
        if bms_is_member(
            var.varattno as i32 - FIRST_LOW_INVALID_HEAP_ATTRIBUTE_NUMBER,
            all_updated_cols,
        ) {
            // Var was updated -- indicates that we should not hint.
            return true;
        }
        // Still haven't found a reason to not pass the hint.
        return false;
    }

    expression_tree_walker(Some(node), &mut |child| {
        index_expression_changed_walker(Some(child), all_updated_cols)
    })
}

// ===========================================================================
// ExecWithoutOverlapsNotEmpty
// ===========================================================================

/// `ExecWithoutOverlapsNotEmpty(rel, attname, attval, typtype, atttypid)` —
/// raise an error if the tuple has an empty range or multirange in the given
/// attribute.
fn ExecWithoutOverlapsNotEmpty<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    attname: &str,
    attval: &DatumV<'mcx>,
    typtype: i8,
) -> PgResult<()> {
    // The range/multirange `*_is_empty` seams are on the pointer-model word
    // `Datum`: a range value is by-reference, so cross its owned varlena image
    // as the `DatumGetPointer` word (`as_byref_word`) the seam detoasts.
    let attval_word = DatumWord::from_usize(attval.as_byref_word());
    let isempty = match typtype {
        TYPTYPE_RANGE => {
            rangetypes_seams::range_is_empty::call(mcx, attval_word)?
        }
        TYPTYPE_MULTIRANGE => {
            multirangetypes_seams::multirange_is_empty::call(mcx, attval_word)?
        }
        _ => {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INTERNAL_ERROR)
                .errmsg_internal(alloc::format!(
                    "WITHOUT OVERLAPS column \"{}\" is not a range or multirange",
                    attname
                ))
                .into_error());
        }
    };

    // Report a CHECK_VIOLATION.
    if isempty {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_CHECK_VIOLATION)
            .errmsg(alloc::format!(
                "empty WITHOUT OVERLAPS value found in column \"{}\" in relation \"{}\"",
                attname,
                rel.name()
            ))
            .into_error());
    }

    Ok(())
}

// ===========================================================================
// Small helpers — move-out/back of pooled index descriptor + IndexInfo.
// ===========================================================================

/// Alias the result relation's open `Relation`.
fn result_rel_alias<'mcx>(estate: &EStateData<'mcx>, rri: RriId) -> Relation<'mcx> {
    estate
        .result_rel(rri)
        .ri_RelationDesc
        .as_ref()
        .expect("result relation must be open")
        .alias()
}

/// Move the `IndexInfo[i]` out of the pool into an owned local (replacing it
/// with `IndexInfo::default()` — the slot is written back by [`put_index_info`]
/// once per-index work finishes).
fn take_index_info<'mcx>(estate: &mut EStateData<'mcx>, rri: RriId, i: usize) -> IndexInfo<'mcx> {
    let arr = estate
        .result_rel_mut(rri)
        .ri_IndexRelationInfo
        .as_mut()
        .expect("ri_IndexRelationInfo present");
    core::mem::take(&mut arr[i])
}

/// Write the (possibly mutated) `IndexInfo` back into the pool slot `i`.
fn put_index_info<'mcx>(estate: &mut EStateData<'mcx>, rri: RriId, i: usize, ii: IndexInfo<'mcx>) {
    let arr = estate
        .result_rel_mut(rri)
        .ri_IndexRelationInfo
        .as_mut()
        .expect("ri_IndexRelationInfo present");
    arr[i] = ii;
}

/// Take the open index descriptor `i` out of the pool (leaving `None`, the C
/// NULL slot of a closed index). `None` if the slot is already closed.
fn take_index_desc<'mcx>(
    estate: &mut EStateData<'mcx>,
    rri: RriId,
    i: usize,
) -> Option<Relation<'mcx>> {
    let arr = estate
        .result_rel_mut(rri)
        .ri_IndexRelationDescs
        .as_mut()
        .expect("ri_IndexRelationDescs present");
    arr[i].take()
}

/// Put the open index descriptor back into pool slot `i`.
fn put_index_desc<'mcx>(estate: &mut EStateData<'mcx>, rri: RriId, i: usize, rel: Relation<'mcx>) {
    let arr = estate
        .result_rel_mut(rri)
        .ri_IndexRelationDescs
        .as_mut()
        .expect("ri_IndexRelationDescs present");
    arr[i] = Some(rel);
}

/// The per-tuple ExprContext id (created on first use by the caller).
fn econtext_of<'mcx>(estate: &mut EStateData<'mcx>) -> EcxtId {
    estate
        .es_per_tuple_exprcontext
        .expect("per-tuple ExprContext created by the caller")
}

/// `ExecQual(predicate, econtext)` — a `None` predicate is always-true.
fn exec_qual_opt<'mcx>(
    estate: &mut EStateData<'mcx>,
    predicate: Option<&mut ::nodes::execexpr::ExprState<'mcx>>,
    econtext: EcxtId,
) -> PgResult<bool> {
    match predicate {
        None => Ok(true),
        Some(state) => expr_seams::exec_qual::call(state, econtext, estate),
    }
}

// ===========================================================================
// Seam installation
// ===========================================================================

/// Install every seam this unit owns + the executor-layer seams homed in other
/// crates' seam modules (`ExecCheckIndexConstraints`, `check_exclusion_constraint`
/// via `index_check_exclusion`).
pub fn init_seams() {
    inward::exec_open_indices::set(seam_exec_open_indices);
    inward::exec_close_indices::set(ExecCloseIndices);
    inward::exec_insert_index_tuples::set(seam_exec_insert_index_tuples);
    inward::exec_check_index_constraints::set(seam_exec_check_index_constraints);

    // IndexCheckExclusion (catalog/index.c's exclusion-constraint second pass)
    // is homed here because it needs this crate's executor table-scan +
    // check_exclusion_constraint substrate; install the catalog-index-seams
    // `index_check_exclusion` seam its index_build driver calls.
    index_seams::index_check_exclusion::set(IndexCheckExclusion);
}

fn seam_exec_open_indices<'mcx>(
    estate: &mut EStateData<'mcx>,
    result_rel_info: RriId,
    speculative: bool,
) -> PgResult<()> {
    ExecOpenIndices(estate, result_rel_info, speculative)
}

fn seam_exec_insert_index_tuples<'mcx>(
    mcx: Mcx<'mcx>,
    estate: &mut EStateData<'mcx>,
    result_rel_info: RriId,
    slot: SlotId,
    update: bool,
    no_dup_err: bool,
    spec_conflict: Option<&mut bool>,
    arbiter_indexes: &[Oid],
    only_summarizing: bool,
) -> PgResult<PgVec<'mcx, Oid>> {
    ExecInsertIndexTuples(
        mcx,
        estate,
        result_rel_info,
        slot,
        update,
        no_dup_err,
        spec_conflict,
        arbiter_indexes,
        only_summarizing,
    )
}

fn seam_exec_check_index_constraints<'mcx>(
    estate: &mut EStateData<'mcx>,
    result_rel_info: RriId,
    slot: SlotId,
    conflict_tid: &mut ItemPointerData,
    tupleid: &ItemPointerData,
    arbiter_indexes: &[Oid],
) -> PgResult<bool> {
    let mcx = estate.es_query_cxt;
    ExecCheckIndexConstraints(
        mcx,
        estate,
        result_rel_info,
        slot,
        conflict_tid,
        tupleid,
        arbiter_indexes,
    )
}

