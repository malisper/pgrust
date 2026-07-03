//! execIndexing.c, INSERT + ON CONFLICT arms: ExecOpenIndices/ExecCloseIndices/
//! ExecInsertIndexTuples/ExecCheckIndexConstraints + FormIndexDatum
//! (catalog/index.c) over the btree AM. Loud: exclusion constraints, deferred
//! unique rechecks, summarizing-only updates.
#![allow(non_snake_case)]

use ::datum::Datum;
use ::mcx::{Mcx, PgBox, PgVec};
use ::types_nodes::NodeList;
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

// IndexInfo (nodes/execnodes.h) trimmed to the insert/build lanes' fields.
pub struct IndexInfo<'mcx> {
    pub ii_NumIndexAttrs: i32,
    // C ii_AmCache (per-statement AM scratch; gist stores its GISTSTATE).
    pub ii_AmCache: Option<Box<dyn core::any::Any>>,
    pub ii_NumIndexKeyAttrs: i32,
    pub ii_IndexAttrNumbers: [AttrNumber; INDEX_MAX_KEYS as usize],
    pub ii_Expressions: NodeList<'mcx>,
    pub ii_ExpressionsState: PgVec<'mcx, PgBox<'mcx, execexpr::ExprState<'mcx>>>,
    pub ii_Predicate: NodeList<'mcx>,
    pub ii_PredicateState: Option<PgBox<'mcx, execexpr::ExprState<'mcx>>>,
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

// RelationGetIndexExpressions (relcache.c): the Form caches the nodeToString
// source instead of a parsed tree (no copyObject port); eval_const_expressions
// here is required for planner qual matching, exactly as C.
pub fn RelationGetIndexExpressions<'mcx>(
    mcx: Mcx<'mcx>,
    index: &Relation<'_>,
) -> PgResult<NodeList<'mcx>> {
    let form = index.rd_index.as_ref().expect("index relation");
    let Some(src) = form.indexprs_src.as_ref() else {
        return Ok(NodeList::nil());
    };
    let node = readfuncs::stringToNode(mcx, src.as_str())?;
    let list = node.as_list().expect("indexprs is a List");
    let mut out = NodeList::nil();
    for e in list.iter() {
        out.lappend(mcx, clauses::eval_const_expressions(mcx, e)?)?;
    }
    Ok(out)
}

/// RelationGetIndexPredicate (relcache.c), implicit-AND result. DIVERGENCE:
/// C canonicalize_quals here (relcache.c:5254-5257); this executor path skips
/// it — ExecQual truth values are form-independent — and the planner copy
/// (plancat.rs get_relation_info) canonicalizes independently.
pub fn RelationGetIndexPredicate<'mcx>(
    mcx: Mcx<'mcx>,
    index: &Relation<'_>,
) -> PgResult<NodeList<'mcx>> {
    let form = index.rd_index.as_ref().expect("index relation");
    let Some(src) = form.indpred_src.as_ref() else {
        return Ok(NodeList::nil());
    };
    let node = readfuncs::stringToNode(mcx, src.as_str())?;
    let folded = clauses::eval_const_expressions(mcx, node)?;
    clauses::make_ands_implicit(mcx, Some(folded))
}

/// BuildIndexInfo (catalog/index.c), pg_index arm.
pub fn BuildIndexInfo<'mcx>(mcx: Mcx<'mcx>, index: &Relation<'_>) -> PgResult<IndexInfo<'mcx>> {
    let indexstruct = index.rd_index.as_ref().expect("index relation");

    if indexstruct.indisexclusion {
        unported("exclusion constraints (BuildIndexInfo ii_ExclusionOps)");
    }

    let numatts = indexstruct.indnatts as i32;
    let mut attrs = [0 as AttrNumber; INDEX_MAX_KEYS as usize];
    for i in 0..numatts as usize {
        attrs[i] = indexstruct.indkey[i];
    }

    Ok(IndexInfo {
        ii_NumIndexAttrs: numatts,
        ii_AmCache: None,
        ii_NumIndexKeyAttrs: indexstruct.indnkeyatts as i32,
        ii_IndexAttrNumbers: attrs,
        ii_Expressions: RelationGetIndexExpressions(mcx, index)?,
        ii_ExpressionsState: PgVec::new_in(mcx),
        ii_Predicate: RelationGetIndexPredicate(mcx, index)?,
        ii_PredicateState: None,
        ii_Unique: indexstruct.indisunique,
        ii_NullsNotDistinct: indexstruct.indnullsnotdistinct,
        // indisready only (index.c:2452): invalid-but-ready still gets inserts.
        ii_ReadyForInserts: indexstruct.indisready,
        ii_Summarizing: false, // btree only (relam gates in indexam)
        ii_Concurrent: false,
        ii_BrokenHotChain: false,
        ii_UniqueOps: [0; INDEX_MAX_KEYS as usize],
        ii_UniqueProcs: [0; INDEX_MAX_KEYS as usize],
        ii_UniqueStrats: [0; INDEX_MAX_KEYS as usize],
    })
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
    pub infos: PgVec<'mcx, IndexInfo<'mcx>>,
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
        let mut ii = BuildIndexInfo(mcx, &indexDesc)?;
        if speculative && ii.ii_Unique {
            BuildSpeculativeIndexInfo(&indexDesc, &mut ii)?;
        }
        state.descs.push(indexDesc);
        state.infos.push(ii);
    }

    Ok(state)
}

/// ExecCloseIndices.
pub fn ExecCloseIndices(mut state: ResultRelIndexState<'_>) -> PgResult<()> {
    for (i, indexDesc) in state.descs.iter().enumerate() {
        indexam::index_insert_cleanup(indexDesc, &mut state.infos[i].ii_AmCache)?;
    }
    // index_close(RowExclusiveLock): the Relation close hook runs on drop.
    Ok(())
}

/// FormIndexDatum (catalog/index.c); system columns stay loud. The expression
/// states resolve once onto the IndexInfo (C's lazy ExecPrepareExprList; the
/// exprs already passed eval_const_expressions in RelationGetIndexExpressions,
/// so ExecPrepareExpr's expression_planner rerun is skipped as a no-op).
/// `eval_mcx` is C's per-tuple context: the caller resets it per row.
pub fn FormIndexDatum<'mcx>(
    mcx: Mcx<'mcx>,
    eval_mcx: Mcx<'_>,
    indexInfo: &mut IndexInfo<'mcx>,
    slot: &mut SlotData<'mcx>,
    values: &mut [Datum],
    isnull: &mut [bool],
) -> PgResult<()> {
    if !indexInfo.ii_Expressions.is_nil() && indexInfo.ii_ExpressionsState.is_empty() {
        for expr in indexInfo.ii_Expressions.iter() {
            let state = execexpr::exec_init_expr(mcx, Some(expr), execexpr::ParamBind::NONE)?
                .expect("index expression");
            indexInfo.ii_ExpressionsState.push(state);
        }
    }
    for state in indexInfo.ii_ExpressionsState.iter_mut() {
        // SAFETY: eval_mcx outlives this call; by-ref results are consumed
        // (copied into the index tuple) before the caller resets it.
        unsafe { state.arm_result_mcx_raw(eval_mcx) };
    }
    let mut indexpr_item = indexInfo.ii_ExpressionsState.iter_mut();

    for i in 0..indexInfo.ii_NumIndexAttrs as usize {
        let keycol = indexInfo.ii_IndexAttrNumbers[i];
        if keycol < 0 {
            unported("system-attribute index columns (slot_getsysattr)");
        }
        if keycol != 0 {
            let mut null = false;
            values[i] = exectuples::slot_getattr(slot, keycol as i32, &mut null);
            isnull[i] = null;
        } else {
            let state = indexpr_item.next().expect("wrong number of index expressions");
            let mut slots = execexpr::EvalSlots { scan: Some(slot), inner: None, outer: None };
            let r = execexpr::exec_eval_expr(state, &mut slots)?;
            values[i] = r.value;
            isnull[i] = r.isnull;
        }
    }
    if indexpr_item.next().is_some() {
        panic!("wrong number of index expressions");
    }
    Ok(())
}

// The ii_PredicateState arm of C's ExecInsertIndexTuples /
// ExecCheckIndexConstraints / heapam_index_build_range_scan: lazy
// ExecPrepareQual + ExecQual over the scan slot.
pub fn index_predicate_passes<'mcx>(
    mcx: Mcx<'mcx>,
    eval_mcx: Mcx<'_>,
    indexInfo: &mut IndexInfo<'mcx>,
    slot: &mut SlotData<'mcx>,
) -> PgResult<bool> {
    debug_assert!(!indexInfo.ii_Predicate.is_nil());
    if indexInfo.ii_PredicateState.is_none() {
        indexInfo.ii_PredicateState =
            execexpr::exec_init_qual(mcx, &indexInfo.ii_Predicate, execexpr::ParamBind::NONE)?;
    }
    if let Some(state) = indexInfo.ii_PredicateState.as_deref_mut() {
        // SAFETY: eval_mcx outlives this call; the qual result is consumed
        // before the caller resets it.
        unsafe { state.arm_result_mcx_raw(eval_mcx) };
    }
    let mut slots = execexpr::EvalSlots { scan: Some(slot), inner: None, outer: None };
    execexpr::exec_qual(indexInfo.ii_PredicateState.as_deref_mut(), &mut slots)
}

/// ExecInsertIndexTuples, INSERT + ON CONFLICT arms (`update`/
/// `onlySummarizing` are the UPDATE-hint and BRIN lanes). With `noDupErr`,
/// arbiter (or all, if `arbiter_indexes` is empty) unique indexes get
/// UNIQUE_CHECK_PARTIAL and a potential conflict sets `*spec_conflict`
/// instead of erroring; C's recheck-oid result list only feeds the deferred
/// lane, loud below.
pub fn ExecInsertIndexTuples<'mcx>(
    mcx: Mcx<'mcx>,
    eval_mcx: Mcx<'_>,
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
        let indexInfo = &mut state.infos[i];
        if !indexInfo.ii_ReadyForInserts {
            continue;
        }

        if !indexInfo.ii_Predicate.is_nil()
            && !index_predicate_passes(mcx, eval_mcx, indexInfo, slot)?
        {
            continue;
        }

        FormIndexDatum(mcx, eval_mcx, indexInfo, slot, &mut values, &mut isnull)?;
        let n_index_attrs = indexInfo.ii_NumIndexAttrs as usize;

        let indexInfo = &state.infos[i];
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
    eval_mcx: Mcx<'_>,
    state: &mut ResultRelIndexState<'mcx>,
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
        let indexInfo = &mut state.infos[i];
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

        if !indexInfo.ii_Predicate.is_nil()
            && !index_predicate_passes(mcx, eval_mcx, indexInfo, slot)?
        {
            continue;
        }

        FormIndexDatum(mcx, eval_mcx, indexInfo, slot, &mut values, &mut isnull)?;
        let indexInfo = &state.infos[i];

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
    index_info: &IndexInfo<'mcx>,
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
