//! Port of `src/backend/executor/execJunk.c` — junk-attribute (junk-filter)
//! support.
//!
//! An attribute of a tuple living inside the executor can be a normal attribute
//! or a "junk" attribute. Junk attributes never make it out of the executor
//! (never printed, returned, or stored); they only carry information useful to
//! the executor itself (system attributes like `ctid`, or sort-key columns not
//! to be output). A target list is a list of [`TargetEntry`] nodes, each with a
//! `resjunk` flag. `ExecInitJunkFilter` builds a [`JunkFilter`];
//! `ExecFindJunkAttribute`/`ExecFindJunkAttributeInTlist` locate junk
//! attributes of interest; `ExecFilterJunk` removes all junk attributes from a
//! tuple, producing a clean virtual tuple.
//!
//! # Owned-EState model
//!
//! Following the repo's executor model, the C `TupleTableSlot *` becomes a
//! [`SlotId`] into [`EStateData::es_tupleTable`], and the C
//! `CurrentMemoryContext` is the EState's per-query context
//! (`estate.es_query_cxt`). `ExecCleanTypeFromTL` (the cleaned descriptor) is a
//! direct call into execTuples; the slot pool operations (`ExecSetSlotDescriptor`,
//! the virtual-slot alloc, `slot_getattr`, and the clear/fill/store of the
//! result slot) go through execTuples' seam crate — those pool-id payload ops
//! are seam-and-panic until the slot payload model is wired into the EState
//! pool, which is correct.

#![no_std]
#![allow(non_snake_case)]
#![allow(clippy::result_large_err)]

extern crate alloc;

use ::execTuples::exectype_tupoutput::ExecCleanTypeFromTL;
use execTuples_seams as execTuples;
use ::mcx::{alloc_in, vec_with_capacity_in, Mcx};
use ::types_core::primitive::{AttrNumber, InvalidAttrNumber};
use ::types_error::{PgError, PgResult};
use ::nodes::executor::TupleSlotKind;
use ::nodes::execnodes::{EStateData, JunkFilter, SlotId};
use ::nodes::nodes::T_JunkFilter;
use ::nodes::primnodes::TargetEntry;
use ::types_tuple::heaptuple::TupleDesc;

/// `tupdesc->natts` with a NULL-descriptor guard (the C descriptor is always
/// non-NULL here; the idiomatic `TupleDesc` is an `Option`, so surface a NULL
/// as an internal error, matching the C `Assert`).
fn tuple_desc_natts(tuple_desc: &TupleDesc) -> PgResult<usize> {
    let td = tuple_desc
        .as_ref()
        .ok_or_else(|| PgError::error("junk filter: null TupleDesc"))?;
    usize::try_from(td.natts)
        .map_err(|_| PgError::error("junk filter: TupleDesc has invalid natts"))
}

/// Deep-copy a `TupleDesc` into `mcx`. C shares one refcounted `TupleDesc *`
/// between the result slot and the junk filter (`PinTupleDesc`); the owned
/// model gives each its own copy.
fn clone_tupdesc<'mcx>(mcx: Mcx<'mcx>, td: &TupleDesc<'mcx>) -> PgResult<TupleDesc<'mcx>> {
    match td.as_ref() {
        Some(d) => Ok(Some(alloc_in(mcx, d.clone_in(mcx)?)?)),
        None => Ok(None),
    }
}

// ===========================================================================
// ExecInitJunkFilter
// ===========================================================================

/// `ExecInitJunkFilter(targetList, slot)` (execJunk.c).
///
/// Initialize the junk filter. The output tuple descriptor is built from the
/// non-junk tlist entries (`ExecCleanTypeFromTL`). An optional `slot` can be
/// passed; otherwise a new virtual slot is created.
///
/// The map between the original tuple's attributes and the "clean" tuple's
/// attributes is an array of `cleanLength` attribute numbers (one per clean
/// attribute), each holding the 1-based attribute number of the corresponding
/// original attribute. (Zero would indicate a NULL output attribute, which this
/// routine does not use.)
pub fn ExecInitJunkFilter<'mcx>(
    estate: &mut EStateData<'mcx>,
    targetList: ::mcx::PgVec<'mcx, TargetEntry<'mcx>>,
    slot: Option<SlotId>,
) -> PgResult<JunkFilter<'mcx>> {
    let mcx = estate.es_query_cxt;

    // cleanTupType = ExecCleanTypeFromTL(targetList);
    let cleanTupType = ExecCleanTypeFromTL(mcx, &targetList)?;
    // The filter keeps its own copy of the descriptor (C shares one refcounted
    // pointer; the owned slot seam takes the descriptor by move).
    let jfCleanTupType = clone_tupdesc(mcx, &cleanTupType)?;

    // Use the given slot, or make a new slot if we weren't given one.
    //   if (slot) ExecSetSlotDescriptor(slot, cleanTupType);
    //   else slot = MakeSingleTupleTableSlot(cleanTupType, &TTSOpsVirtual);
    let resultSlot = match slot {
        Some(slot) => {
            execTuples::exec_set_slot_descriptor::call(estate, slot, cleanTupType)?;
            slot
        }
        None => {
            execTuples::exec_alloc_table_slot::call(estate, cleanTupType, TupleSlotKind::Virtual)?
        }
    };

    // cleanLength = cleanTupType->natts;
    let cleanLength = tuple_desc_natts(&jfCleanTupType)?;
    let cleanMap = if cleanLength > 0 {
        // cleanMap = (AttrNumber *) palloc(cleanLength * sizeof(AttrNumber));
        let mut cleanMap = vec_with_capacity_in::<AttrNumber>(mcx, cleanLength)?;
        // cleanResno = 0;
        let mut cleanResno: usize = 0;
        // foreach(t, targetList) if (!tle->resjunk) cleanMap[cleanResno++] = tle->resno;
        for tle in targetList.iter() {
            if !tle.resjunk {
                if cleanResno >= cleanLength {
                    return Err(PgError::error(
                        "junk filter target list has too many non-junk entries",
                    ));
                }
                cleanMap.push(tle.resno);
                cleanResno += 1;
            }
        }
        // Assert(cleanResno == cleanLength);
        debug_assert_eq!(cleanResno, cleanLength);
        cleanMap
    } else {
        // cleanMap = NULL;
        ::mcx::PgVec::new_in(mcx)
    };

    // Finally create and initialize the JunkFilter struct.
    Ok(JunkFilter {
        type_: T_JunkFilter,
        jf_targetList: targetList,
        jf_cleanTupType: jfCleanTupType,
        jf_cleanMap: cleanMap,
        jf_resultSlot: resultSlot,
    })
}

// ===========================================================================
// ExecInitJunkFilterConversion
// ===========================================================================

/// `ExecInitJunkFilterConversion(targetList, cleanTupType, slot)` (execJunk.c).
///
/// Initialize a [`JunkFilter`] for rowtype conversions. The target "clean"
/// tuple descriptor is given rather than inferred from the target list; it can
/// contain deleted columns (the caller has checked that the non-deleted columns
/// match the non-junk columns of the target list). The map stores zero for any
/// deleted (dropped) attribute, marking that a NULL is needed in the output.
pub fn ExecInitJunkFilterConversion<'mcx>(
    estate: &mut EStateData<'mcx>,
    targetList: ::mcx::PgVec<'mcx, TargetEntry<'mcx>>,
    cleanTupType: TupleDesc<'mcx>,
    slot: Option<SlotId>,
) -> PgResult<JunkFilter<'mcx>> {
    let mcx = estate.es_query_cxt;

    // The result slot gets its own copy of the descriptor (C shares one
    // refcounted pointer; the owned slot seam takes the descriptor by move).
    let slotCleanTupType = clone_tupdesc(mcx, &cleanTupType)?;

    // Use the given slot, or make a new slot if we weren't given one.
    let resultSlot = match slot {
        Some(slot) => {
            execTuples::exec_set_slot_descriptor::call(estate, slot, slotCleanTupType)?;
            slot
        }
        None => execTuples::exec_alloc_table_slot::call(
            estate,
            slotCleanTupType,
            TupleSlotKind::Virtual,
        )?,
    };

    // cleanLength = cleanTupType->natts;
    let cleanLength = tuple_desc_natts(&cleanTupType)?;
    let cleanMap = if cleanLength > 0 {
        // cleanMap = (AttrNumber *) palloc0(cleanLength * sizeof(AttrNumber));
        let mut cleanMap = vec_with_capacity_in::<AttrNumber>(mcx, cleanLength)?;
        for _ in 0..cleanLength {
            cleanMap.push(0);
        }

        // t = list_head(targetList);
        let mut t: usize = 0;
        // for (i = 0; i < cleanLength; i++)
        for i in 0..cleanLength {
            // if (TupleDescCompactAttr(cleanTupType, i)->attisdropped)
            //     continue;  /* map entry is already zero */
            let cdesc = cleanTupType
                .as_ref()
                .ok_or_else(|| PgError::error("junk filter: null TupleDesc"))?;
            if i >= cdesc.compact_attrs.len() {
                return Err(PgError::error(
                    "junk filter: attribute number out of range",
                ));
            }
            if cdesc.compact_attr(i).attisdropped {
                continue;
            }
            // for (;;) { tle = lfirst(t); t = lnext(targetList, t);
            //            if (!tle->resjunk) { cleanMap[i] = tle->resno; break; } }
            loop {
                let tle = targetList.get(t).ok_or_else(|| {
                    PgError::error("junk filter conversion target list ended early")
                })?;
                t += 1;
                if !tle.resjunk {
                    cleanMap[i] = tle.resno;
                    break;
                }
            }
        }
        cleanMap
    } else {
        // cleanMap = NULL;
        ::mcx::PgVec::new_in(mcx)
    };

    // Finally create and initialize the JunkFilter struct.
    Ok(JunkFilter {
        type_: T_JunkFilter,
        jf_targetList: targetList,
        jf_cleanTupType: cleanTupType,
        jf_cleanMap: cleanMap,
        jf_resultSlot: resultSlot,
    })
}

// ===========================================================================
// ExecFindJunkAttribute / ExecFindJunkAttributeInTlist
// ===========================================================================

/// `ExecFindJunkAttribute(junkfilter, attrName)` (execJunk.c): locate the
/// junk attribute in the junk filter's target list, returning its `resno`.
/// Returns [`InvalidAttrNumber`] if not found.
pub fn ExecFindJunkAttribute(junkfilter: &JunkFilter, attrName: &str) -> AttrNumber {
    ExecFindJunkAttributeInTlist(&junkfilter.jf_targetList, attrName)
}

/// `ExecFindJunkAttributeInTlist(targetlist, attrName)` (execJunk.c): find a
/// junk attribute given a subplan's target list (not necessarily part of a
/// [`JunkFilter`]). Returns [`InvalidAttrNumber`] if not found.
pub fn ExecFindJunkAttributeInTlist(targetlist: &[TargetEntry], attrName: &str) -> AttrNumber {
    // foreach(t, targetlist)
    for tle in targetlist {
        //   if (tle->resjunk && tle->resname && strcmp(tle->resname, attrName) == 0)
        if tle.resjunk {
            if let Some(resname) = &tle.resname {
                if resname.as_str() == attrName {
                    // We found it!
                    return tle.resno;
                }
            }
        }
    }

    InvalidAttrNumber
}

// ===========================================================================
// ExecFilterJunk
// ===========================================================================

/// `ExecFilterJunk(junkfilter, slot)` (execJunk.c): construct (in the filter's
/// own result slot) and return a slot with all the junk attributes removed.
///
/// The input `slot` is fully deconstructed (`slot_getallattrs`), its values are
/// transposed into the result slot through the clean map, and the result slot
/// is stored as a virtual tuple. Returns the (now-populated) result slot id.
pub fn ExecFilterJunk<'mcx>(
    estate: &mut EStateData<'mcx>,
    junkfilter: &JunkFilter<'mcx>,
    slot: SlotId,
) -> PgResult<SlotId> {
    let mcx = estate.es_query_cxt;

    // get info from the junk filter:
    //   cleanTupType = junkfilter->jf_cleanTupType;
    //   cleanLength = cleanTupType->natts;
    //   cleanMap = junkfilter->jf_cleanMap;
    let cleanLength = tuple_desc_natts(&junkfilter.jf_cleanTupType)?;
    if junkfilter.jf_cleanMap.len() < cleanLength {
        return Err(PgError::error("junk filter clean map is inconsistent"));
    }

    // Transpose data into proper fields of the new tuple.
    //
    //   slot_getallattrs(slot);                          (deform on demand below)
    //   old_values = slot->tts_values; old_isnull = slot->tts_isnull;
    //   for (i = 0; i < cleanLength; i++) {
    //       int j = cleanMap[i];
    //       if (j == 0) { values[i] = (Datum) 0; isnull[i] = true; }
    //       else        { values[i] = old_values[j - 1]; isnull[i] = old_isnull[j - 1]; }
    //   }
    // `values` flows verbatim into `store_virtual_values` and is fed from
    // `slot_getattr_by_id`; both are the execTuples slot-payload seam, whose
    // canonical carrier is `::types_tuple::heaptuple::Datum`
    // (the ByVal/ByRef enum). No scalar is constructed or inspected here — the
    // per-column `tts_values` images are carried whole through the canonical
    // type, so by-reference values cross intact (no bare-word `as_usize` edge).
    let mut values = vec_with_capacity_in::<
        ::types_tuple::heaptuple::Datum<'_>,
    >(mcx, cleanLength)?;
    let mut isnull = vec_with_capacity_in::<bool>(mcx, cleanLength)?;
    for i in 0..cleanLength {
        let j = junkfilter.jf_cleanMap[i];
        if j == 0 {
            values.push(::types_tuple::heaptuple::Datum::null());
            isnull.push(true);
        } else {
            // old_values[j - 1] / old_isnull[j - 1]: read source attribute j
            // (1-based) of the input slot (slot_getattr deforms up to j,
            // equivalent to the C slot_getallattrs + array index).
            let attr = execTuples::slot_getattr_by_id::call(estate, slot, j)?;
            values.push(attr.value);
            isnull.push(attr.isnull);
        }
    }

    // Prepare to build a virtual result tuple:  ExecClearTuple(resultSlot);
    //   values = resultSlot->tts_values; isnull = resultSlot->tts_isnull;
    //   ... (filled above) ...
    // And return the virtual tuple:  return ExecStoreVirtualTuple(resultSlot);
    execTuples::store_virtual_values::call(estate, junkfilter.jf_resultSlot, &values, &isnull)?;

    Ok(junkfilter.jf_resultSlot)
}

/// Install this crate's owned seams. `backend-executor-execJunk` declares no
/// inward seam crate (no ported owner crosses a cycle to reach it: the only
/// consumer, execExpr, parks the junk filter as an address), so there is
/// nothing to install. Wired into `seams-init::init_all()` for symmetry and to
/// satisfy the recurrence guard.
pub fn init_seams() {}
