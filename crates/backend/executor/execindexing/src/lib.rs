//! execIndexing.c, INSERT + ON CONFLICT arms: ExecOpenIndices/ExecCloseIndices/
//! ExecInsertIndexTuples/ExecCheckIndexConstraints + FormIndexDatum
//! (catalog/index.c) over the btree AM. Loud: index expressions/predicates,
//! exclusion constraints, deferred unique rechecks, summarizing-only updates.
#![allow(non_snake_case)]

use ::datum::Datum;
use ::mcx::{Mcx, PgVec};
use ::types_core::{AttrNumber, Oid, INDEX_MAX_KEYS};
use ::types_error::{PgError, PgResult, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE};
use ::types_nbtree::genam::IndexUniqueCheck;
use ::types_rel::{Relation, RowExclusiveLock};
use ::types_slot::SlotData;
use ::types_tuple::itemptr::{ItemPointerEquals, ItemPointerIsValid, ItemPointerSetInvalid};
use ::types_tuple::ItemPointerData;

#[cfg(test)]
mod tests;

mod build_scan;
pub use build_scan::table_index_build_scan;

#[cold]
#[inline(never)]
pub(crate) fn unported(what: &str) -> ! {
    panic!("unported: execIndexing {what}")
}

// IndexInfo (nodes/execnodes.h) trimmed to the insert lane's fields; the
// expression/predicate state slots land with the expression-index lane.
pub struct IndexInfo {
    pub ii_NumIndexAttrs: i32,
    // C ii_AmCache (per-statement AM scratch; gist stores its GISTSTATE).
    pub ii_AmCache: Option<Box<dyn core::any::Any>>,
    pub ii_NumIndexKeyAttrs: i32,
    pub ii_IndexAttrNumbers: [AttrNumber; INDEX_MAX_KEYS as usize],
    pub ii_Unique: bool,
    pub ii_NullsNotDistinct: bool,
    pub ii_ReadyForInserts: bool,
    pub ii_Summarizing: bool,
    pub ii_Concurrent: bool,
    pub ii_BrokenHotChain: bool,
    // BuildSpeculativeIndexInfo fills these (empty otherwise); ii_UniqueOps
    // is only consulted by the exclusion lane, kept for C shape.
    pub ii_UniqueOps: [Oid; INDEX_MAX_KEYS as usize],
    pub ii_UniqueProcs: [Oid; INDEX_MAX_KEYS as usize],
    pub ii_UniqueStrats: [u16; INDEX_MAX_KEYS as usize],
}

/// BuildIndexInfo (catalog/index.c), pg_index arm.
pub fn BuildIndexInfo(index: &Relation<'_>) -> IndexInfo {
    let indexstruct = index.rd_index.as_ref().expect("index relation");

    if indexstruct.indisexclusion {
        unported("exclusion constraints (BuildIndexInfo ii_ExclusionOps)");
    }
    if indexstruct.has_indpred {
        unported("partial-index predicates (ii_Predicate)");
    }

    let numatts = indexstruct.indnatts as i32;
    let mut attrs = [0 as AttrNumber; INDEX_MAX_KEYS as usize];
    for i in 0..numatts as usize {
        attrs[i] = indexstruct.indkey[i];
        if attrs[i] == 0 {
            unported("expression index columns (ii_Expressions)");
        }
    }

    IndexInfo {
        ii_NumIndexAttrs: numatts,
        ii_AmCache: None,
        ii_NumIndexKeyAttrs: indexstruct.indnkeyatts as i32,
        ii_IndexAttrNumbers: attrs,
        ii_Unique: indexstruct.indisunique,
        ii_NullsNotDistinct: indexstruct.indnullsnotdistinct,
        ii_ReadyForInserts: indexstruct.indisready && indexstruct.indisvalid,
        ii_Summarizing: false, // btree only (relam gates in indexam)
        ii_Concurrent: false,
        ii_BrokenHotChain: false,
        ii_UniqueOps: [0; INDEX_MAX_KEYS as usize],
        ii_UniqueProcs: [0; INDEX_MAX_KEYS as usize],
        ii_UniqueStrats: [0; INDEX_MAX_KEYS as usize],
    }
}

/// BuildSpeculativeIndexInfo (catalog/index.c), btree arm: equality operator
/// strategy/operator/proc per key column for the ON CONFLICT arbiter probe.
pub fn BuildSpeculativeIndexInfo(index: &Relation<'_>, ii: &mut IndexInfo) -> PgResult<()> {
    debug_assert!(ii.ii_Unique);
    if index.rd_rel.relam != ::types_core::catalog::BTREE_AM_OID {
        unported("BuildSpeculativeIndexInfo over a non-btree AM");
    }
    let indnkeyatts = ii.ii_NumIndexKeyAttrs as usize;
    for i in 0..indnkeyatts {
        // IndexAmTranslateCompareType(COMPARE_EQ, BTREE) = BTEqualStrategyNumber.
        let strat = ::types_scan::scankey::BTEqualStrategyNumber;
        let opno = lsyscache::amop::get_opfamily_member(
            index.rd_opfamily[i],
            index.rd_opcintype[i],
            index.rd_opcintype[i],
            strat as i16,
        )?;
        if opno == 0 {
            panic!(
                "missing operator {}({},{}) in opfamily {}",
                strat, index.rd_opcintype[i], index.rd_opcintype[i], index.rd_opfamily[i]
            );
        }
        ii.ii_UniqueStrats[i] = strat;
        ii.ii_UniqueOps[i] = opno;
        ii.ii_UniqueProcs[i] = lsyscache::operator::get_opcode(opno)?;
    }
    Ok(())
}

// The per-result-relation index slice of C's ResultRelInfo (ri_NumIndices /
// ri_IndexRelationDescs / ri_IndexRelationInfo); executils::ResultRelInfo is
// the estate-resident stub, so the owning node carries this by value.
pub struct ResultRelIndexState<'mcx> {
    pub descs: PgVec<'mcx, Relation<'mcx>>,
    pub infos: PgVec<'mcx, IndexInfo>,
}

impl ResultRelIndexState<'_> {
    #[inline]
    pub fn num_indices(&self) -> usize {
        self.descs.len()
    }
}

/// ExecOpenIndices. `speculative` unique-info arm is the ON CONFLICT lane.
pub fn ExecOpenIndices<'mcx>(
    mcx: Mcx<'mcx>,
    result_relation: &Relation<'mcx>,
    speculative: bool,
) -> PgResult<ResultRelIndexState<'mcx>> {
    let mut state = ResultRelIndexState {
        descs: PgVec::new_in(mcx),
        infos: PgVec::new_in(mcx),
    };

    if !result_relation.rd_rel.relhasindex {
        return Ok(state);
    }

    let indexoidlist: PgVec<'mcx, Oid> =
        relcache_seams::relation_get_index_list::call(mcx, result_relation.rd_id)?;
    if indexoidlist.is_empty() {
        return Ok(state);
    }

    for &indexOid in indexoidlist.iter() {
        let indexDesc = indexam::index_open(mcx, indexOid, RowExclusiveLock)?;
        let mut ii = BuildIndexInfo(&indexDesc);
        if speculative && ii.ii_Unique {
            BuildSpeculativeIndexInfo(&indexDesc, &mut ii)?;
        }
        state.descs.push(indexDesc);
        state.infos.push(ii);
    }

    Ok(state)
}

/// ExecCloseIndices.
pub fn ExecCloseIndices(state: ResultRelIndexState<'_>) -> PgResult<()> {
    for indexDesc in state.descs.iter() {
        indexam::index_insert_cleanup(indexDesc)?;
    }
    // index_close(RowExclusiveLock): the Relation close hook runs on drop.
    Ok(())
}

/// FormIndexDatum (catalog/index.c), plain-column arm; expression and system
/// columns are loud in BuildIndexInfo / here.
pub fn FormIndexDatum<'mcx>(
    indexInfo: &IndexInfo,
    slot: &mut SlotData<'mcx>,
    values: &mut [Datum],
    isnull: &mut [bool],
) -> PgResult<()> {
    for i in 0..indexInfo.ii_NumIndexAttrs as usize {
        let keycol = indexInfo.ii_IndexAttrNumbers[i];
        if keycol < 0 {
            unported("system-attribute index columns (slot_getsysattr)");
        }
        debug_assert!(keycol != 0, "expression columns rejected in BuildIndexInfo");
        let mut null = false;
        values[i] = exectuples::slot_getattr(slot, keycol as i32, &mut null);
        isnull[i] = null;
    }
    Ok(())
}

/// ExecInsertIndexTuples, INSERT + ON CONFLICT arms (`update`/
/// `onlySummarizing` are the UPDATE-hint and BRIN lanes). With `noDupErr`,
/// arbiter (or all, if `arbiter_indexes` is empty) unique indexes get
/// UNIQUE_CHECK_PARTIAL and a potential conflict sets `*spec_conflict`
/// instead of erroring; C's recheck-oid result list only feeds the deferred
/// lane, loud below.
pub fn ExecInsertIndexTuples<'mcx>(
    mcx: Mcx<'mcx>,
    state: &mut ResultRelIndexState<'mcx>,
    heap_relation: &Relation<'mcx>,
    slot: &mut SlotData<'mcx>,
    noDupErr: bool,
    mut spec_conflict: Option<&mut bool>,
    arbiter_indexes: &[Oid],
) -> PgResult<()> {
    let tupleid = slot.base().tts_tid;
    debug_assert!(ItemPointerIsValid(&tupleid));
    debug_assert!(slot.base().tts_tableOid == heap_relation.rd_id);

    let mut values = [Datum::null(); INDEX_MAX_KEYS as usize];
    let mut isnull = [false; INDEX_MAX_KEYS as usize];

    for i in 0..state.descs.len() {
        let indexInfo = &state.infos[i];
        if !indexInfo.ii_ReadyForInserts {
            continue;
        }

        FormIndexDatum(indexInfo, slot, &mut values, &mut isnull)?;
        let n_index_attrs = indexInfo.ii_NumIndexAttrs as usize;

        let indexRelation = &state.descs[i];
        let index_form = indexRelation.rd_index.as_ref().expect("index relation");
        let applyNoDupErr = noDupErr
            && (arbiter_indexes.is_empty()
                || arbiter_indexes.contains(&index_form.indexrelid));
        let checkUnique = if !index_form.indisunique {
            IndexUniqueCheck::UNIQUE_CHECK_NO
        } else if applyNoDupErr {
            IndexUniqueCheck::UNIQUE_CHECK_PARTIAL
        } else if index_form.indimmediate {
            IndexUniqueCheck::UNIQUE_CHECK_YES
        } else {
            unported("deferred unique constraint recheck (trigger queue)");
        };

        let satisfiesConstraint = indexam::index_insert(
            mcx,
            indexRelation,
            &values[..n_index_attrs],
            &isnull[..n_index_attrs],
            &tupleid,
            heap_relation,
            checkUnique,
            false,
            &mut state.infos[i].ii_AmCache,
        )?;

        if checkUnique == IndexUniqueCheck::UNIQUE_CHECK_PARTIAL && !satisfiesConstraint {
            if index_form.indimmediate {
                if let Some(flag) = spec_conflict.as_deref_mut() {
                    *flag = true;
                }
            }
        }
    }

    Ok(())
}

/// ExecCheckIndexConstraints: true if no arbiter (or any unique, when
/// `arbiter_indexes` is empty) constraint conflicts with `slot`; otherwise
/// false with the committed conflicting tuple's TID in `conflict_tid`.
/// `tupleid` excludes an already-inserted self tuple from the recheck;
/// `existing_slot` is caller-owned scratch in the result relation's format.
#[allow(clippy::too_many_arguments)]
pub fn ExecCheckIndexConstraints<'mcx>(
    mcx: Mcx<'mcx>,
    state: &ResultRelIndexState<'mcx>,
    heap_relation: &Relation<'mcx>,
    slot: &mut SlotData<'mcx>,
    existing_slot: &mut SlotData<'mcx>,
    tupleid: &ItemPointerData,
    arbiter_indexes: &[Oid],
    conflict_tid: &mut ItemPointerData,
) -> PgResult<bool> {
    ItemPointerSetInvalid(conflict_tid);
    let mut checked_index = false;

    let mut values = [Datum::null(); INDEX_MAX_KEYS as usize];
    let mut isnull = [false; INDEX_MAX_KEYS as usize];

    for i in 0..state.descs.len() {
        let indexInfo = &state.infos[i];
        // ii_ExclusionOps is loud at BuildIndexInfo, so unique-only here.
        if !indexInfo.ii_Unique || !indexInfo.ii_ReadyForInserts {
            continue;
        }
        let indexRelation = &state.descs[i];
        let index_form = indexRelation.rd_index.as_ref().expect("index relation");
        if !arbiter_indexes.is_empty()
            && !arbiter_indexes.contains(&index_form.indexrelid)
        {
            continue;
        }
        if !index_form.indimmediate {
            return Err(deferrable_arbiter(heap_relation, indexRelation));
        }
        checked_index = true;

        FormIndexDatum(indexInfo, slot, &mut values, &mut isnull)?;

        if !check_unique_constraint(
            mcx,
            heap_relation,
            indexRelation,
            indexInfo,
            tupleid,
            &values,
            &isnull,
            existing_slot,
            conflict_tid,
        )? {
            return Ok(false);
        }
    }

    if !arbiter_indexes.is_empty() && !checked_index {
        panic!("unexpected failure to find arbiter index");
    }
    Ok(true)
}

/// check_exclusion_or_unique_constraint, unique-index pre-check arm
/// (CEOUC_WAIT + violationOK, the only mode our callers use; the exclusion
/// and deferred arms stay loud upstream). Probes the index under a dirty
/// snapshot and waits out in-progress inserters/deleters before deciding.
#[allow(clippy::too_many_arguments)]
fn check_unique_constraint<'mcx>(
    mcx: Mcx<'mcx>,
    heap_relation: &Relation<'mcx>,
    index_relation: &Relation<'mcx>,
    index_info: &IndexInfo,
    tupleid: &ItemPointerData,
    values: &[Datum],
    isnull: &[bool],
    existing_slot: &mut SlotData<'mcx>,
    conflict_tid: &mut ItemPointerData,
) -> PgResult<bool> {
    let indnkeyatts = index_info.ii_NumIndexKeyAttrs as usize;

    if !index_info.ii_NullsNotDistinct {
        for &null in &isnull[..indnkeyatts] {
            if null {
                return Ok(true);
            }
        }
    }

    let dirty = std::rc::Rc::new(::types_snapshot::SnapshotData::sentinel(
        mcx,
        ::types_snapshot::SnapshotType::SNAPSHOT_DIRTY,
    ));

    let mut scankeys: PgVec<'mcx, ::types_scan::scankey::ScanKeyData> = PgVec::new_in(mcx);
    for i in 0..indnkeyatts {
        let mut key = ::types_scan::scankey::ScanKeyData::empty();
        key.sk_flags = if isnull[i] {
            ::types_scan::scankey::SK_ISNULL | ::types_scan::scankey::SK_SEARCHNULL
        } else {
            0
        };
        key.sk_attno = (i + 1) as AttrNumber;
        key.sk_strategy = index_info.ii_UniqueStrats[i];
        key.sk_subtype = 0;
        key.sk_collation = index_relation.rd_indcollation[i];
        fmgr_core::fmgr_info_into(index_info.ii_UniqueProcs[i], &mut key.sk_func)?;
        key.sk_argument = values[i];
        scankeys.push(key);
    }

    'retry: loop {
        let mut conflict = false;
        let mut found_self = false;
        let mut scan = indexam::index_beginscan(
            mcx,
            heap_relation,
            index_relation,
            dirty.clone(),
            indnkeyatts as i32,
            0,
        )?;
        indexam::index_rescan(&mut scan, Some(&scankeys), None)?;

        while indexam::index_getnext_slot(
            mcx,
            &mut scan,
            ::types_scan::ScanDirection::ForwardScanDirection,
            existing_slot,
        )? {
            if scan.xs_recheck {
                unported("lossy-index recheck (index_recheck_constraint)");
            }
            let existing_tid = existing_slot.base().tts_tid;
            if ItemPointerIsValid(tupleid) && ItemPointerEquals(tupleid, &existing_tid) {
                assert!(
                    !found_self,
                    "found self tuple multiple times in index \"{}\"",
                    index_relation.name()
                );
                found_self = true;
                continue;
            }

            let (dirty_xmin, dirty_xmax, dirty_token) = (
                dirty.dirty_xmin.get(),
                dirty.dirty_xmax.get(),
                dirty.dirty_speculative_token.get(),
            );
            let xwait = if dirty_xmin != 0 { dirty_xmin } else { dirty_xmax };
            if xwait != 0 {
                indexam::index_endscan(scan)?;
                if dirty_token != 0 {
                    lmgr::SpeculativeInsertionWait(dirty_xmin, dirty_token)?;
                } else {
                    lmgr::XactLockTableWait(
                        xwait,
                        Some(heap_relation),
                        Some(&existing_tid),
                        ::types_storage::lock::XLTW_Oper::InsertIndex,
                    )?;
                }
                continue 'retry;
            }

            conflict = true;
            *conflict_tid = existing_tid;
            break;
        }

        indexam::index_endscan(scan)?;
        exectuples::exec_clear_tuple(existing_slot, mcx);
        return Ok(!conflict);
    }
}

#[cold]
#[inline(never)]
fn deferrable_arbiter(heap: &Relation<'_>, index: &Relation<'_>) -> Box<PgError> {
    Box::new(
        PgError::error(
            "ON CONFLICT does not support deferrable unique constraints/exclusion \
             constraints as arbiters",
        )
        .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
        .with_table_name(heap.name().to_owned())
        .with_constraint_name(index.name().to_owned()),
    )
}
