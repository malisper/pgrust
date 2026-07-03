//! execIndexing.c, INSERT arm: ExecOpenIndices/ExecCloseIndices/
//! ExecInsertIndexTuples + FormIndexDatum (catalog/index.c) over the btree AM.
//! Loud: index expressions/predicates, exclusion constraints, deferred/
//! speculative unique checks, summarizing-only updates.
#![allow(non_snake_case)]

use ::datum::Datum;
use ::mcx::{Mcx, PgVec};
use ::types_core::{AttrNumber, Oid, INDEX_MAX_KEYS};
use ::types_error::PgResult;
use ::types_nbtree::genam::IndexUniqueCheck;
use ::types_rel::{Relation, RowExclusiveLock};
use ::types_slot::SlotData;
use ::types_tuple::itemptr::ItemPointerIsValid;

#[cfg(test)]
mod tests;

#[cold]
#[inline(never)]
fn unported(what: &str) -> ! {
    panic!("unported: execIndexing {what}")
}

// IndexInfo (nodes/execnodes.h) trimmed to the insert lane's fields; the
// expression/predicate state slots land with the expression-index lane.
pub struct IndexInfo {
    pub ii_NumIndexAttrs: i32,
    pub ii_NumIndexKeyAttrs: i32,
    pub ii_IndexAttrNumbers: [AttrNumber; INDEX_MAX_KEYS as usize],
    pub ii_Unique: bool,
    pub ii_NullsNotDistinct: bool,
    pub ii_ReadyForInserts: bool,
    pub ii_Summarizing: bool,
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
        ii_NumIndexKeyAttrs: indexstruct.indnkeyatts as i32,
        ii_IndexAttrNumbers: attrs,
        ii_Unique: indexstruct.indisunique,
        ii_NullsNotDistinct: indexstruct.indnullsnotdistinct,
        ii_ReadyForInserts: indexstruct.indisready && indexstruct.indisvalid,
        ii_Summarizing: false, // btree only (relam gates in indexam)
    }
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
        let ii = BuildIndexInfo(&indexDesc);
        if speculative && ii.ii_Unique {
            unported("BuildSpeculativeIndexInfo (ON CONFLICT lane)");
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

/// ExecInsertIndexTuples, INSERT arm (`update`/`noDupErr`/`arbiterIndexes`/
/// `onlySummarizing` are the UPDATE and ON CONFLICT lanes; deferred unique
/// indexes route to the loud PARTIAL arm inside nbtree).
pub fn ExecInsertIndexTuples<'mcx>(
    mcx: Mcx<'mcx>,
    state: &mut ResultRelIndexState<'mcx>,
    heap_relation: &Relation<'mcx>,
    slot: &mut SlotData<'mcx>,
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

        let indexRelation = &state.descs[i];
        let index_form = indexRelation.rd_index.as_ref().expect("index relation");
        let checkUnique = if !index_form.indisunique {
            IndexUniqueCheck::UNIQUE_CHECK_NO
        } else if index_form.indimmediate {
            IndexUniqueCheck::UNIQUE_CHECK_YES
        } else {
            IndexUniqueCheck::UNIQUE_CHECK_PARTIAL // loud inside nbtree
        };

        indexam::index_insert(
            mcx,
            indexRelation,
            &values[..indexInfo.ii_NumIndexAttrs as usize],
            &isnull[..indexInfo.ii_NumIndexAttrs as usize],
            &tupleid,
            heap_relation,
            checkUnique,
            false,
        )?;
    }

    Ok(())
}
