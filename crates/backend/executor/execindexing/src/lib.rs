//! execIndexing.c, INSERT + ON CONFLICT + exclusion arms: ExecOpenIndices/
//! ExecCloseIndices/ExecInsertIndexTuples/ExecCheckIndexConstraints/
//! check_exclusion_constraint + FormIndexDatum (catalog/index.c). Loud:
//! deferred unique rechecks, summarizing-only updates.
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
pub use build_scan::{
    table_index_build_range_scan, table_index_build_range_scan_with_xmin, table_index_build_scan,
};
mod validate_scan;
pub use validate_scan::{table_index_validate_scan, ValidateIndexState};

// unported: user-reachable unported-feature lanes raise a clean
// ERRCODE_FEATURE_NOT_SUPPORTED error (sites sit at arm entry; the
// transaction aborts and unwinds normally).
#[cold]
#[inline(never)]
pub(crate) fn unported(what: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!("{what} is not yet implemented"))
            .with_sqlstate(::types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
    )
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
    // C: ii_HasExclusion ⇔ ii_ExclusionOps != NULL.
    pub ii_HasExclusion: bool,
    pub ii_ExclusionOps: [Oid; INDEX_MAX_KEYS as usize],
    pub ii_ExclusionProcs: [Oid; INDEX_MAX_KEYS as usize],
    pub ii_ExclusionStrats: [u16; INDEX_MAX_KEYS as usize],
    pub ii_WithoutOverlaps: bool,
}

// C rd_indexprs/rd_indpred: processed trees cached per index relid, deep-copied
// out per call (copyObject), cleared by relcache inval on the index.
struct IdxExprCache {
    mcx: Mcx<'static>,
    exprs: mcx::PgHashMap<'static, Oid, NodeList<'static>>,
    preds: mcx::PgHashMap<'static, Oid, NodeList<'static>>,
    callbacks_registered: bool,
}

thread_local! {
    static IDX_EXPR_CACHE: core::cell::RefCell<Option<core::mem::ManuallyDrop<IdxExprCache>>> =
        const { core::cell::RefCell::new(None) };
}

fn with_expr_cache<R>(f: impl FnOnce(&mut IdxExprCache) -> R) -> R {
    IDX_EXPR_CACHE.with(|cell| {
        let mut slot = cell.borrow_mut();
        let st = slot.get_or_insert_with(|| {
            let mcx = mcx::session_root("IndexExprContext").mcx();
            core::mem::ManuallyDrop::new(IdxExprCache {
                mcx,
                exprs: mcx::PgHashMap::new_in(mcx),
                preds: mcx::PgHashMap::new_in(mcx),
                callbacks_registered: false,
            })
        });
        f(st)
    })
}

fn IdxExprRelCallback(_arg: Datum, relid: Oid) {
    with_expr_cache(|st| {
        if relid != types_core::InvalidOid {
            st.exprs.remove(&relid);
            st.preds.remove(&relid);
        } else {
            st.exprs.clear();
            st.preds.clear();
        }
    });
}

fn expr_cache_arm() -> PgResult<()> {
    if !with_expr_cache(|st| st.callbacks_registered) {
        inval::invalidate::CacheRegisterRelcacheCallback(
            IdxExprRelCallback,
            Datum::from_oid(types_core::InvalidOid),
        )?;
        with_expr_cache(|st| st.callbacks_registered = true);
    }
    Ok(())
}

fn copy_list_in<'d>(mcx: Mcx<'d>, list: &NodeList<'_>) -> PgResult<NodeList<'d>> {
    let mut out = NodeList::nil();
    for e in list.iter() {
        out.lappend(mcx, copyfuncs::copy_object(mcx, e)?)?;
    }
    Ok(out)
}

// RelationGetIndexExpressions (relcache.c): stringToNode +
// eval_const_expressions + fix_opfuncids, cached per C rd_indexprs.
pub fn RelationGetIndexExpressions<'mcx>(
    mcx: Mcx<'mcx>,
    index: &Relation<'_>,
) -> PgResult<NodeList<'mcx>> {
    let form = index.rd_index.as_ref().expect("index relation");
    let Some(src) = form.indexprs_src.as_ref() else {
        return Ok(NodeList::nil());
    };
    if let Some(hit) = with_expr_cache(|st| st.exprs.get(&index.rd_id).map(|l| copy_list_in(mcx, l)))
    {
        return hit;
    }
    expr_cache_arm()?;
    let node = readfuncs::stringToNode(mcx, src.as_str())?;
    let list = node.as_list().expect("indexprs is a List");
    let mut out = NodeList::nil();
    for e in list.iter() {
        let folded = clauses::eval_const_expressions(mcx, e)?;
        nodes_core::fix_opfuncids(folded)?;
        out.lappend(mcx, folded)?;
    }
    let cmcx = with_expr_cache(|st| st.mcx);
    let cached = copy_list_in(cmcx, &out)?;
    with_expr_cache(|st| st.exprs.insert(index.rd_id, cached));
    Ok(out)
}

/// RelationGetIndexPredicate (relcache.c), implicit-AND result, cached per C
/// rd_indpred. DIVERGENCE: C canonicalize_quals here (relcache.c:5254-5257);
/// this executor path skips it — ExecQual truth values are form-independent —
/// and the planner copy (plancat.rs get_relation_info) canonicalizes
/// independently.
pub fn RelationGetIndexPredicate<'mcx>(
    mcx: Mcx<'mcx>,
    index: &Relation<'_>,
) -> PgResult<NodeList<'mcx>> {
    let form = index.rd_index.as_ref().expect("index relation");
    let Some(src) = form.indpred_src.as_ref() else {
        return Ok(NodeList::nil());
    };
    if let Some(hit) = with_expr_cache(|st| st.preds.get(&index.rd_id).map(|l| copy_list_in(mcx, l)))
    {
        return hit;
    }
    expr_cache_arm()?;
    let node = readfuncs::stringToNode(mcx, src.as_str())?;
    let folded = clauses::eval_const_expressions(mcx, node)?;
    let out = clauses::make_ands_implicit(mcx, Some(folded))?;
    for e in out.iter() {
        nodes_core::fix_opfuncids(e)?;
    }
    let cmcx = with_expr_cache(|st| st.mcx);
    let cached = copy_list_in(cmcx, &out)?;
    with_expr_cache(|st| st.preds.insert(index.rd_id, cached));
    Ok(out)
}

/// RelationGetExclusionInfo (relcache.c). DIVERGENCE: C caches the arrays in
/// rd_indexcxt; recomputed per BuildIndexInfo here (per-statement write path).
pub fn RelationGetExclusionInfo(
    mcx: Mcx<'_>,
    index: &Relation<'_>,
    ops: &mut [Oid; INDEX_MAX_KEYS as usize],
    procs: &mut [Oid; INDEX_MAX_KEYS as usize],
    strats: &mut [u16; INDEX_MAX_KEYS as usize],
) -> PgResult<()> {
    let indexstruct = index.rd_index.as_ref().expect("index relation");
    let indnkeyatts = indexstruct.indnkeyatts as usize;
    let conexclop = relcache_build_seams::scan_exclusion_ops::call(
        mcx,
        indexstruct.indrelid,
        index.rd_id,
    )?;
    assert!(
        conexclop.len() == indnkeyatts,
        "conexclop is not a 1-D Oid array"
    );
    for i in 0..indnkeyatts {
        ops[i] = conexclop[i];
        procs[i] = lsyscache::operator::get_opcode(ops[i])?;
        let strat = lsyscache::amop::get_op_opfamily_strategy(ops[i], index.rd_opfamily[i])?;
        if strat == 0 {
            panic!(
                "could not find strategy for operator {} in family {}",
                ops[i], index.rd_opfamily[i]
            );
        }
        strats[i] = strat as u16;
    }
    Ok(())
}

/// RelationGetDummyIndexExpressions (relcache.c): null Consts with the raw
/// expressions' types — no user-defined code (not even const-folding) runs.
pub fn RelationGetDummyIndexExpressions<'mcx>(
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
    for raw in list.iter() {
        let c = ::types_nodes::Node::mk(
            mcx,
            ::types_nodes::primnodes::Const {
                consttype: nodes_core::expr_type(raw),
                consttypmod: nodes_core::expr_typmod(raw),
                constcollid: nodes_core::expr_collation(raw),
                constlen: 1,
                constvalue: Datum::null(),
                constisnull: true,
                constbyval: true,
                location: -1,
            },
        )?;
        out.lappend(mcx, c)?;
    }
    Ok(out)
}

/// BuildDummyIndexInfo (catalog/index.c): dummy exprs, no predicate.
pub fn BuildDummyIndexInfo<'mcx>(mcx: Mcx<'mcx>, index: &Relation<'_>) -> PgResult<IndexInfo<'mcx>> {
    let indexstruct = index.rd_index.as_ref().expect("index relation");
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
        ii_Expressions: RelationGetDummyIndexExpressions(mcx, index)?,
        ii_ExpressionsState: PgVec::new_in(mcx),
        ii_Predicate: NodeList::nil(),
        ii_PredicateState: None,
        ii_Unique: indexstruct.indisunique,
        ii_NullsNotDistinct: indexstruct.indnullsnotdistinct,
        ii_ReadyForInserts: indexstruct.indisready,
        ii_Summarizing: ::types_relscan::IndexAmKind::from_relam(index.rd_rel.relam)
            .amsummarizing(),
        ii_Concurrent: false,
        ii_BrokenHotChain: false,
        ii_UniqueOps: [0; INDEX_MAX_KEYS as usize],
        ii_UniqueProcs: [0; INDEX_MAX_KEYS as usize],
        ii_UniqueStrats: [0; INDEX_MAX_KEYS as usize],
        // C BuildDummyIndexInfo ignores any exclusion constraint.
        ii_HasExclusion: false,
        ii_ExclusionOps: [0; INDEX_MAX_KEYS as usize],
        ii_ExclusionProcs: [0; INDEX_MAX_KEYS as usize],
        ii_ExclusionStrats: [0; INDEX_MAX_KEYS as usize],
        ii_WithoutOverlaps: false,
    })
}

/// BuildIndexInfo (catalog/index.c), pg_index arm.
pub fn BuildIndexInfo<'mcx>(mcx: Mcx<'mcx>, index: &Relation<'_>) -> PgResult<IndexInfo<'mcx>> {
    let indexstruct = index.rd_index.as_ref().expect("index relation");

    let mut excl_ops = [0 as Oid; INDEX_MAX_KEYS as usize];
    let mut excl_procs = [0 as Oid; INDEX_MAX_KEYS as usize];
    let mut excl_strats = [0u16; INDEX_MAX_KEYS as usize];
    if indexstruct.indisexclusion {
        RelationGetExclusionInfo(mcx, index, &mut excl_ops, &mut excl_procs, &mut excl_strats)?;
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
        ii_Summarizing: ::types_relscan::IndexAmKind::from_relam(index.rd_rel.relam)
            .amsummarizing(),
        ii_Concurrent: false,
        ii_BrokenHotChain: false,
        ii_UniqueOps: [0; INDEX_MAX_KEYS as usize],
        ii_UniqueProcs: [0; INDEX_MAX_KEYS as usize],
        ii_UniqueStrats: [0; INDEX_MAX_KEYS as usize],
        ii_HasExclusion: indexstruct.indisexclusion,
        ii_ExclusionOps: excl_ops,
        ii_ExclusionProcs: excl_procs,
        ii_ExclusionStrats: excl_strats,
        ii_WithoutOverlaps: indexstruct.indisexclusion && indexstruct.indisunique,
    })
}

/// BuildSpeculativeIndexInfo (catalog/index.c), btree arm: equality operator
/// strategy/operator/proc per key column for the ON CONFLICT arbiter probe.
pub fn BuildSpeculativeIndexInfo(index: &Relation<'_>, ii: &mut IndexInfo) -> PgResult<()> {
    debug_assert!(ii.ii_Unique);
    if index.rd_rel.relam != ::types_core::catalog::BTREE_AM_OID {
        return Err(unported("ON CONFLICT arbiter over a non-btree unique index"));
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
        // C skips indisexclusion here (temporal unique gist has no btree
        // equality strategy; the exclusion recheck arbitrates instead).
        if speculative
            && ii.ii_Unique
            && !indexDesc.rd_index.as_ref().expect("index relation").indisexclusion
        {
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
    // Drop the lock acquired by ExecOpenIndices (execIndexing.c). Retaining
    // it to xact end deadlocks catalog writers against VACUUM FULL pg_class:
    // the writer's explicit pg_class close releases the heap lock while the
    // drop hook would keep the index lock, inverting the heap-then-index
    // order every other locker follows.
    while let Some(indexDesc) = state.descs.pop() {
        indexam::index_close(indexDesc, RowExclusiveLock)?;
    }
    Ok(())
}

/// FormIndexDatum (catalog/index.c); system columns stay loud. The expression
/// states resolve once onto the IndexInfo (C's lazy ExecPrepareExprList,
/// including its expression_planner step — required on the CREATE INDEX build
/// path, where ii_Expressions are raw parse trees from ComputeIndexAttrs, not
/// the pre-folded RelationGetIndexExpressions copies).
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
            let planned = clauses::eval_const_expressions(mcx, expr)?;
            nodes_core::fix_opfuncids(planned)?;
            let state = execexpr::exec_init_expr(mcx, Some(planned), execexpr::ParamBind::NONE)?
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
            // unported: slot_getsysattr lane.
            return Err(unported("system-attribute index columns"));
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

/// C ExecPrepareQual over ii_Predicate: expression_planner folds each
/// implicit-AND arm independently, so a folding error (e.g. non-contiguous
/// range difference) surfaces even beside a constant-false sibling arm.
/// Scan-path callers (build/validate/exclusion-check/analyze) must run this
/// eagerly before the loop like C — errors surface on empty tables too.
pub fn prepare_index_predicate<'mcx>(
    mcx: Mcx<'mcx>,
    indexInfo: &mut IndexInfo<'mcx>,
) -> PgResult<()> {
    if indexInfo.ii_Predicate.is_nil() || indexInfo.ii_PredicateState.is_some() {
        return Ok(());
    }
    let mut planned = NodeList::nil();
    for e in indexInfo.ii_Predicate.iter() {
        let folded = clauses::eval_const_expressions(mcx, e)?;
        nodes_core::fix_opfuncids(folded)?;
        planned.lappend(mcx, folded)?;
    }
    indexInfo.ii_PredicateState =
        execexpr::exec_init_qual(mcx, &planned, execexpr::ParamBind::NONE)?;
    Ok(())
}

// The ii_PredicateState arm of C's ExecInsertIndexTuples /
// ExecCheckIndexConstraints: lazy ExecPrepareQual + ExecQual over the scan
// slot.
pub fn index_predicate_passes<'mcx>(
    mcx: Mcx<'mcx>,
    eval_mcx: Mcx<'_>,
    indexInfo: &mut IndexInfo<'mcx>,
    slot: &mut SlotData<'mcx>,
) -> PgResult<bool> {
    debug_assert!(!indexInfo.ii_Predicate.is_nil());
    prepare_index_predicate(mcx, indexInfo)?;
    if let Some(state) = indexInfo.ii_PredicateState.as_deref_mut() {
        // SAFETY: eval_mcx outlives this call; the qual result is consumed
        // before the caller resets it.
        unsafe { state.arm_result_mcx_raw(eval_mcx) };
    }
    let mut slots = execexpr::EvalSlots { scan: Some(slot), inner: None, outer: None };
    execexpr::exec_qual(indexInfo.ii_PredicateState.as_deref_mut(), &mut slots)
}

/// ExecInsertIndexTuples, INSERT + ON CONFLICT + summarizing-only UPDATE arms
/// (the `update` indexUnchanged hint is the UPDATE-hint lane). With `noDupErr`,
/// arbiter (or all, if `arbiter_indexes` is empty) unique indexes get
/// UNIQUE_CHECK_PARTIAL and a potential conflict sets `*spec_conflict`
/// instead of erroring. Returns C's recheck-oid list (deferred-exclusion
/// trigger filtering).
pub fn ExecInsertIndexTuples<'mcx>(
    mcx: Mcx<'mcx>,
    eval_mcx: Mcx<'_>,
    state: &mut ResultRelIndexState<'mcx>,
    heap_relation: &Relation<'mcx>,
    slot: &mut SlotData<'mcx>,
    noDupErr: bool,
    mut spec_conflict: Option<&mut bool>,
    arbiter_indexes: &[Oid],
    only_summarizing: bool,
) -> PgResult<PgVec<'mcx, Oid>> {
    let tupleid = slot.base().tts_tid;
    debug_assert!(ItemPointerIsValid(&tupleid));
    debug_assert!(slot.base().tts_tableOid == heap_relation.rd_id);

    let mut recheck_indexes: PgVec<'mcx, Oid> = PgVec::new_in(mcx);
    let mut values = [Datum::null(); INDEX_MAX_KEYS as usize];
    let mut isnull = [false; INDEX_MAX_KEYS as usize];

    for i in 0..state.descs.len() {
        let indexInfo = &mut state.infos[i];
        if !indexInfo.ii_ReadyForInserts {
            continue;
        }

        if only_summarizing && !indexInfo.ii_Summarizing {
            continue;
        }

        if !indexInfo.ii_Predicate.is_nil()
            && !index_predicate_passes(mcx, eval_mcx, indexInfo, slot)?
        {
            continue;
        }

        FormIndexDatum(mcx, eval_mcx, indexInfo, slot, &mut values, &mut isnull)?;
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
            // Deferred constraint: the after-trigger recheck (unique_key_recheck)
            // enforces it, so a conflict here only queues the oid.
            IndexUniqueCheck::UNIQUE_CHECK_PARTIAL
        };
        let indimmediate = index_form.indimmediate;

        let mut satisfiesConstraint = indexam::index_insert(
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

        if state.infos[i].ii_HasExclusion {
            let (violation_ok, wait_mode) = if applyNoDupErr {
                (true, CeoucWaitMode::LivelockPreventingWait)
            } else if !indimmediate {
                (true, CeoucWaitMode::NoWait)
            } else {
                (false, CeoucWaitMode::Wait)
            };
            let mut existing_slot = exectuples::make_tuple_table_slot(
                mcx,
                ::types_slot::TupleSlotKind::BufferHeapTuple,
                Some(heap_relation.rd_att.clone()),
            );
            let indexRelation = &state.descs[i];
            satisfiesConstraint = check_exclusion_or_unique_constraint(
                mcx,
                eval_mcx,
                heap_relation,
                indexRelation,
                &mut state.infos[i],
                &tupleid,
                &values,
                &isnull,
                false,
                wait_mode,
                violation_ok,
                &mut existing_slot,
                None,
            )?;
        }

        if (checkUnique == IndexUniqueCheck::UNIQUE_CHECK_PARTIAL
            || state.infos[i].ii_HasExclusion)
            && !satisfiesConstraint
        {
            recheck_indexes.push(index_form.indexrelid);
            if indimmediate {
                if let Some(flag) = spec_conflict.as_deref_mut() {
                    *flag = true;
                }
            }
        }
    }

    Ok(recheck_indexes)
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
        if (!indexInfo.ii_Unique && !indexInfo.ii_HasExclusion)
            || !indexInfo.ii_ReadyForInserts
        {
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

        if !check_exclusion_or_unique_constraint(
            mcx,
            eval_mcx,
            heap_relation,
            indexRelation,
            &mut state.infos[i],
            tupleid,
            &values,
            &isnull,
            false,
            CeoucWaitMode::Wait,
            true,
            existing_slot,
            Some(conflict_tid),
        )? {
            return Ok(false);
        }
    }

    if !arbiter_indexes.is_empty() && !checked_index {
        panic!("unexpected failure to find arbiter index");
    }
    Ok(true)
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum CeoucWaitMode {
    Wait,
    NoWait,
    LivelockPreventingWait,
}

/// check_exclusion_or_unique_constraint. Probes the index under a dirty
/// snapshot and (per waitMode) waits out in-progress inserters/deleters
/// before deciding.
#[allow(clippy::too_many_arguments)]
fn check_exclusion_or_unique_constraint<'mcx>(
    mcx: Mcx<'mcx>,
    eval_mcx: Mcx<'_>,
    heap_relation: &Relation<'mcx>,
    index_relation: &Relation<'mcx>,
    index_info: &mut IndexInfo<'mcx>,
    tupleid: &ItemPointerData,
    values: &[Datum],
    isnull: &[bool],
    new_index: bool,
    wait_mode: CeoucWaitMode,
    violation_ok: bool,
    existing_slot: &mut SlotData<'mcx>,
    mut conflict_tid: Option<&mut ItemPointerData>,
) -> PgResult<bool> {
    let indnkeyatts = index_info.ii_NumIndexKeyAttrs as usize;
    let exclusion = index_info.ii_HasExclusion;
    let (constr_procs, constr_strats) = if exclusion {
        (index_info.ii_ExclusionProcs, index_info.ii_ExclusionStrats)
    } else {
        (index_info.ii_UniqueProcs, index_info.ii_UniqueStrats)
    };

    // C: WITHOUT OVERLAPS also forbids empty ranges/multiranges, before the
    // NULL check (a UNIQUE constraint could otherwise insert an empty range
    // alongside a NULL scalar part).
    if index_info.ii_WithoutOverlaps && !isnull[indnkeyatts - 1] {
        let attno = index_info.ii_IndexAttrNumbers[indnkeyatts - 1];
        let att = heap_relation.rd_att.attr(attno as usize - 1);
        exec_without_overlaps_not_empty(
            mcx,
            heap_relation,
            &att.attname,
            values[indnkeyatts - 1],
            lsyscache::get_typtype(att.atttypid)?,
        )?;
    }

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
        key.sk_strategy = constr_strats[i];
        key.sk_subtype = 0;
        key.sk_collation = index_relation.rd_indcollation[i];
        fmgr_core::fmgr_info_into(constr_procs[i], &mut key.sk_func)?;
        key.sk_argument = values[i];
        scankeys.push(key);
    }

    let mut existing_values = [Datum::null(); INDEX_MAX_KEYS as usize];
    let mut existing_isnull = [false; INDEX_MAX_KEYS as usize];

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

            FormIndexDatum(
                mcx,
                eval_mcx,
                index_info,
                existing_slot,
                &mut existing_values,
                &mut existing_isnull,
            )?;

            if scan.xs_recheck
                && !index_recheck_constraint(
                    mcx,
                    index_relation,
                    &constr_procs,
                    &existing_values,
                    &existing_isnull,
                    values,
                    indnkeyatts,
                )?
            {
                continue;
            }

            let (dirty_xmin, dirty_xmax, dirty_token) = (
                dirty.dirty_xmin.get(),
                dirty.dirty_xmax.get(),
                dirty.dirty_speculative_token.get(),
            );
            let xwait = if dirty_xmin != 0 { dirty_xmin } else { dirty_xmax };
            if xwait != 0
                && (wait_mode == CeoucWaitMode::Wait
                    || (wait_mode == CeoucWaitMode::LivelockPreventingWait
                        && dirty_token != 0
                        && ::types_core::xact::TransactionIdPrecedes(
                            xact::GetCurrentTransactionId()?,
                            xwait,
                        )))
            {
                indexam::index_endscan(scan)?;
                if dirty_token != 0 {
                    lmgr::SpeculativeInsertionWait(dirty_xmin, dirty_token)?;
                } else {
                    lmgr::XactLockTableWait(
                        xwait,
                        Some(heap_relation),
                        Some(&existing_tid),
                        if exclusion {
                            ::types_storage::lock::XLTW_Oper::RecheckExclusionConstr
                        } else {
                            ::types_storage::lock::XLTW_Oper::InsertIndex
                        },
                    )?;
                }
                continue 'retry;
            }

            if violation_ok {
                conflict = true;
                if let Some(tid) = conflict_tid.as_deref_mut() {
                    *tid = existing_tid;
                }
                break;
            }

            return Err(exclusion_violation(
                mcx,
                heap_relation,
                index_relation,
                new_index,
                values,
                isnull,
                &existing_values,
                &existing_isnull,
            ));
        }

        indexam::index_endscan(scan)?;
        exectuples::exec_clear_tuple(existing_slot, mcx);
        return Ok(!conflict);
    }
}

/// IndexCheckExclusion (catalog/index.c): validate existing rows against a
/// freshly built exclusion index (ALTER TABLE ADD EXCLUDE over live data).
pub fn IndexCheckExclusion<'mcx>(
    mcx: Mcx<'mcx>,
    heap_relation: &Relation<'mcx>,
    index_relation: &Relation<'mcx>,
    index_info: &mut IndexInfo<'mcx>,
) -> PgResult<()> {
    // C clears reindex-processing here so the probe scans below may open the
    // now-fully-valid index (index.c:3213).
    if ::types_rel::reindex::ReindexIsCurrentlyProcessingIndex(index_relation.rd_id) {
        ::types_rel::reindex::reset_reindex_processing();
    }
    let eval_cx = ::mcx::MemoryContext::new("IndexCheckExclusion");
    let eval_mcx = eval_cx.mcx();
    let mut slot = exectuples::make_tuple_table_slot(
        mcx,
        ::types_slot::TupleSlotKind::BufferHeapTuple,
        Some(heap_relation.rd_att.clone()),
    );
    let mut existing_slot = exectuples::make_tuple_table_slot(
        mcx,
        ::types_slot::TupleSlotKind::BufferHeapTuple,
        Some(heap_relation.rd_att.clone()),
    );
    let snapshot = snapmgr::RegisterSnapshot(Some(&snapmgr::GetLatestSnapshot()?))?
        .expect("registered snapshot");
    let flags = ::tableam_vocab::SO_TYPE_SEQSCAN
        | ::tableam_vocab::SO_ALLOW_STRAT
        | ::tableam_vocab::SO_ALLOW_SYNC
        | ::tableam_vocab::SO_ALLOW_PAGEMODE;
    let mut scan = heapam::heap_beginscan(
        mcx,
        heap_relation,
        Some(snapshot.clone()),
        0,
        PgVec::new_in(mcx),
        None,
        flags,
    )?;

    // C index.c IndexCheckExclusion: ExecPrepareQual runs before the scan.
    prepare_index_predicate(mcx, index_info)?;

    let mut values = [Datum::null(); INDEX_MAX_KEYS as usize];
    let mut isnull = [false; INDEX_MAX_KEYS as usize];
    while heapam::heap_getnextslot(
        mcx,
        &mut scan,
        ::types_scan::ScanDirection::ForwardScanDirection,
        &mut slot,
    )? {
        if !index_info.ii_Predicate.is_nil()
            && !index_predicate_passes(mcx, eval_mcx, index_info, &mut slot)?
        {
            continue;
        }
        FormIndexDatum(mcx, eval_mcx, index_info, &mut slot, &mut values, &mut isnull)?;
        let tupleid = slot.base().tts_tid;
        check_exclusion_or_unique_constraint(
            mcx,
            eval_mcx,
            heap_relation,
            index_relation,
            index_info,
            &tupleid,
            &values,
            &isnull,
            true,
            CeoucWaitMode::Wait,
            false,
            &mut existing_slot,
            None,
        )?;
    }
    heapam::heap_endscan(scan)?;
    snapmgr::UnregisterSnapshot(Some(&snapshot));
    Ok(())
}

/// check_exclusion_constraint (execIndexing.c), the external-caller wrapper.
pub fn check_exclusion_constraint<'mcx>(
    mcx: Mcx<'mcx>,
    eval_mcx: Mcx<'_>,
    heap_relation: &Relation<'mcx>,
    index_relation: &Relation<'mcx>,
    index_info: &mut IndexInfo<'mcx>,
    tupleid: &ItemPointerData,
    values: &[Datum],
    isnull: &[bool],
    new_index: bool,
) -> PgResult<()> {
    let mut existing_slot = exectuples::make_tuple_table_slot(
        mcx,
        ::types_slot::TupleSlotKind::BufferHeapTuple,
        Some(heap_relation.rd_att.clone()),
    );
    check_exclusion_or_unique_constraint(
        mcx,
        eval_mcx,
        heap_relation,
        index_relation,
        index_info,
        tupleid,
        values,
        isnull,
        new_index,
        CeoucWaitMode::Wait,
        false,
        &mut existing_slot,
        None,
    )
    .map(|_| ())
}

// index_recheck_constraint (execIndexing.c): exclusion operators assumed
// strict; returns true on a real conflict.
fn index_recheck_constraint(
    mcx: Mcx<'_>,
    index: &Relation<'_>,
    constr_procs: &[Oid],
    existing_values: &[Datum],
    existing_isnull: &[bool],
    new_values: &[Datum],
    indnkeyatts: usize,
) -> PgResult<bool> {
    for i in 0..indnkeyatts {
        if existing_isnull[i] {
            return Ok(false);
        }
        // Armed frame: bool-returning operators may still detoast args in the
        // result mcx (multirange_eq/overlaps).
        let mut finfo = fmgr_core::fmgr_info(constr_procs[i])?;
        let matched = fmgr_core::function_call2_coll_in(
            &mut finfo,
            index.rd_indcollation[i],
            mcx,
            existing_values[i],
            new_values[i],
        )?
        .as_bool();
        if !matched {
            return Ok(false);
        }
    }
    Ok(true)
}

#[cold]
#[inline(never)]
#[allow(clippy::too_many_arguments)]
fn exclusion_violation(
    mcx: Mcx<'_>,
    heap: &Relation<'_>,
    index: &Relation<'_>,
    new_index: bool,
    values: &[Datum],
    isnull: &[bool],
    existing_values: &[Datum],
    existing_isnull: &[bool],
) -> Box<PgError> {
    let n = index.rd_index.as_ref().expect("index relation").indnkeyatts as usize;
    let error_new =
        genam_seams::build_index_value_description::call(index, &values[..n], &isnull[..n])
            .ok()
            .flatten();
    let error_existing = genam_seams::build_index_value_description::call(
        index,
        &existing_values[..n],
        &existing_isnull[..n],
    )
    .ok()
    .flatten();
    let (msg, detail) = if new_index {
        (
            format!("could not create exclusion constraint \"{}\"", index.name()),
            match (&error_new, &error_existing) {
                (Some(n), Some(e)) => format!("Key {n} conflicts with key {e}."),
                _ => "Key conflicts exist.".to_string(),
            },
        )
    } else {
        (
            format!(
                "conflicting key value violates exclusion constraint \"{}\"",
                index.name()
            ),
            match (&error_new, &error_existing) {
                (Some(n), Some(e)) => format!("Key {n} conflicts with existing key {e}."),
                _ => "Key conflicts with existing key.".to_string(),
            },
        )
    };
    let mut e = PgError::error(msg)
        .with_sqlstate(types_error::ERRCODE_EXCLUSION_VIOLATION)
        .with_detail(detail)
        .with_table_name(heap.name().to_owned())
        .with_constraint_name(index.name().to_owned());
    if let Ok(Some(nsp)) = lsyscache::misc::get_namespace_name(mcx, heap.rd_rel.relnamespace) {
        e = e.with_schema_name(nsp.as_str().to_owned());
    }
    Box::new(e)
}

// ExecWithoutOverlapsNotEmpty (execIndexing.c): CHECK_VIOLATION on an empty
// range/multirange in the WITHOUT OVERLAPS column.
fn exec_without_overlaps_not_empty(
    mcx: Mcx<'_>,
    heap: &Relation<'_>,
    attname: &::types_tuple::tupdesc::NameData,
    attval: Datum,
    typtype: i8,
) -> PgResult<()> {
    // PG_DETOAST_DATUM: values come from FormIndexDatum, possibly toasted.
    // SAFETY: non-null by-ref range/multirange varlena datum (caller checked
    // isnull); readable through its full VARSIZE_ANY.
    let raw = unsafe {
        let p = attval.as_usize() as *const u8;
        core::slice::from_raw_parts(p, ::types_tuple::varatt::varsize_any(p))
    };
    let flat;
    let bytes: &[u8] = if raw[0] & 0x03 == 0 {
        raw
    } else {
        flat = ::detoast_seams::detoast_attr::call(mcx, raw)?;
        &flat
    };
    let name = String::from_utf8_lossy(attname.name_str()).into_owned();
    let isempty = match typtype {
        TYPTYPE_RANGE => ::adt_rangetypes::range_is_empty(bytes),
        TYPTYPE_MULTIRANGE => ::adt_multirangetypes::multirange_is_empty(bytes),
        _ => {
            return Err(Box::new(PgError::error(format!(
                "WITHOUT OVERLAPS column \"{name}\" is not a range or multirange"
            ))))
        }
    };
    if isempty {
        return Err(Box::new(
            PgError::error(format!(
                "empty WITHOUT OVERLAPS value found in column \"{name}\" in relation \"{}\"",
                heap.name()
            ))
            .with_sqlstate(types_error::ERRCODE_CHECK_VIOLATION),
        ));
    }
    Ok(())
}

// pg_type.h
const TYPTYPE_RANGE: i8 = b'r' as i8;
const TYPTYPE_MULTIRANGE: i8 = b'm' as i8;

#[track_caller]
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
