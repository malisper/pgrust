//! Family: tuple-descriptor constructors + tuple output — `ExecTypeFromTL` /
//! `ExecCleanTypeFromTL` / `ExecTypeFromExprList` / `ExecTypeSetColNames` /
//! `BlessTupleDesc` / `TupleDescGetAttInMetadata` / `BuildTupleFromCStrings` /
//! `HeapTupleHeaderGetDatum` and the `begin/do/end_tup_output` convenience
//! routines (execTuples.c).
//!
//! `CreateTemplateTupleDesc`/`TupleDescInitEntry`/`TupleDescInitEntryCollation`
//! are a cycle-free direct dep on `backend-access-common-tupdesc`. The
//! per-expression `exprType`/`exprTypmod`/`exprCollation` (nodeFuncs.c), the
//! type input-function lookup (`getTypeInputInfo`/`fmgr`), the record-type
//! typmod assignment (`assign_record_type_typmod`, typcache.c), the toast-aware
//! composite-Datum production (`HeapTupleHeaderGetDatum`, heaptoast), and the
//! `DestReceiver` virtual dispatch (`rStartup`/`receiveSlot`/`rShutdown`,
//! tcop/dest.h) are reached through their owners' seams.

use mcx::Mcx;
use types_error::PgResult;
use types_nodes::primnodes::{Expr, TargetEntry};
use types_nodes::tuptable::{AttInMetadata, SlotData, TupOutputState};
// The canonical value enum; `TupleValue` is its transitional alias.
use types_tuple::backend_access_common_heaptuple::{Datum, TupleValue};
use types_tuple::heaptuple::{HeapTuple, TupleDesc, TupleDescData, RECORDOID};

use backend_access_common_tupdesc::{
    CreateTemplateTupleDesc, TupleDescInitEntry, TupleDescInitEntryCollation,
};

/// `ExecTargetListLength(targetList)` (tlist.c): number of items in a tlist
/// (including resjunk items). Inlined here (it is a one-liner) to keep the
/// dependency surface minimal.
fn exec_target_list_length(target_list: &[TargetEntry<'_>]) -> i32 {
    target_list.len() as i32
}

/// `ExecCleanTargetListLength(targetList)` (tlist.c): number of items in a
/// tlist, not including any resjunk items.
fn exec_clean_target_list_length(target_list: &[TargetEntry<'_>]) -> i32 {
    let mut len = 0;
    for cur_tle in target_list {
        if !cur_tle.resjunk {
            len += 1;
        }
    }
    len
}

/// `ExecTypeFromTL(targetList)` (execTuples.c): build a tuple descriptor from a
/// target list (including resjunk entries).
pub fn ExecTypeFromTL<'mcx>(
    mcx: Mcx<'mcx>,
    target_list: &[TargetEntry<'mcx>],
) -> PgResult<TupleDesc<'mcx>> {
    ExecTypeFromTLInternal(mcx, target_list, false)
}

/// `ExecCleanTypeFromTL(targetList)` (execTuples.c): like `ExecTypeFromTL` but
/// omitting resjunk columns.
pub fn ExecCleanTypeFromTL<'mcx>(
    mcx: Mcx<'mcx>,
    target_list: &[TargetEntry<'mcx>],
) -> PgResult<TupleDesc<'mcx>> {
    ExecTypeFromTLInternal(mcx, target_list, true)
}

/// `ExecInitResultTypeTL(planstate)` (execTuples.c): initialize the node's
/// result type, using the plan node's target list.
///
/// ```c
/// void
/// ExecInitResultTypeTL(PlanState *planstate)
/// {
///     TupleDesc tupDesc = ExecTypeFromTL(planstate->plan->targetlist);
///     planstate->ps_ResultTupleDesc = tupDesc;
/// }
/// ```
///
/// The descriptor is allocated in the executor's per-query context
/// (`estate.es_query_cxt`, the C `CurrentMemoryContext` during plan init).
pub fn ExecInitResultTypeTL<'mcx>(
    planstate: &mut types_nodes::execnodes::PlanStateData<'mcx>,
    estate: &mut types_nodes::EStateData<'mcx>,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    // `planstate->plan->targetlist` — the plan node lives in the per-query
    // arena (`&'mcx Node`), so reading the tlist reference releases the
    // `&mut planstate` borrow before the store below.
    let plan: &'mcx types_nodes::nodes::Node<'mcx> = planstate
        .plan
        .expect("ExecInitResultTypeTL: PlanState has no plan");
    let targetlist: &'mcx [TargetEntry<'mcx>] = plan
        .plan_head()
        .targetlist
        .as_deref()
        .unwrap_or(&[]);
    let tup_desc = ExecTypeFromTL(mcx, targetlist)?;
    planstate.ps_ResultTupleDesc = tup_desc;
    Ok(())
}

/// `ExecTypeFromTLInternal(targetList, skipjunk)` (execTuples.c).
fn ExecTypeFromTLInternal<'mcx>(
    mcx: Mcx<'mcx>,
    target_list: &[TargetEntry<'mcx>],
    skipjunk: bool,
) -> PgResult<TupleDesc<'mcx>> {
    let mut cur_resno: i32 = 1;

    let len = if skipjunk {
        exec_clean_target_list_length(target_list)
    } else {
        exec_target_list_length(target_list)
    };
    let mut type_info = CreateTemplateTupleDesc(mcx, len)?;

    for tle in target_list {
        if skipjunk && tle.resjunk {
            continue;
        }
        // exprType / exprTypmod / exprCollation of (Node *) tle->expr.
        let info = backend_nodes_nodeFuncs_seams::expr_type_info::call(
            tle.expr
                .as_deref()
                .expect("ExecTypeFromTLInternal: target entry has no expr"),
        )?;
        TupleDescInitEntry(
            &mut type_info,
            cur_resno as i16,
            tle.resname.as_ref().map(|s| s.as_str()),
            info.typid,
            info.typmod,
            0,
        )?;
        TupleDescInitEntryCollation(&mut type_info, cur_resno as i16, info.collation)?;
        cur_resno += 1;
    }

    Ok(Some(mcx::alloc_in(mcx, type_info)?))
}

/// `ExecTypeFromExprList(exprList)` (execTuples.c): build a tuple descriptor
/// from a bare list of expressions (no names).
pub fn ExecTypeFromExprList<'mcx>(
    mcx: Mcx<'mcx>,
    expr_list: &[Expr],
) -> PgResult<TupleDesc<'mcx>> {
    let mut cur_resno: i32 = 1;

    let mut type_info = CreateTemplateTupleDesc(mcx, expr_list.len() as i32)?;

    for e in expr_list {
        let info = backend_nodes_nodeFuncs_seams::expr_type_info::call(e)?;
        TupleDescInitEntry(&mut type_info, cur_resno as i16, None, info.typid, info.typmod, 0)?;
        TupleDescInitEntryCollation(&mut type_info, cur_resno as i16, info.collation)?;
        cur_resno += 1;
    }

    Ok(Some(mcx::alloc_in(mcx, type_info)?))
}

/// `ExecTypeSetColNames(typeInfo, namesList)` (execTuples.c): apply column
/// names to an already-built descriptor.
pub fn ExecTypeSetColNames<'mcx>(
    type_info: &mut TupleDescData<'mcx>,
    names_list: &[&str],
) -> PgResult<()> {
    // It's only OK to change col names in a not-yet-blessed RECORD type.
    debug_assert_eq!(type_info.tdtypeid, RECORDOID);
    debug_assert!(type_info.tdtypmod < 0);

    let mut colno: usize = 0;
    for cname in names_list {
        // Guard against too-long names list (probably can't happen).
        if colno as i32 >= type_info.natts {
            break;
        }
        let attr = type_info.attr_mut(colno);
        colno += 1;

        // Do nothing for empty aliases or dropped columns.
        if cname.is_empty() || attr.attisdropped {
            continue;
        }

        // OK, assign the column name: namestrcpy(&attr->attname, cname).
        namestrcpy(&mut attr.attname, cname);
    }
    Ok(())
}

/// `namestrcpy(&att->attname, src)` (`utils/adt/name.c`): zero the fixed-size
/// `NameData` buffer, then copy at most `NAMEDATALEN - 1` bytes, always leaving
/// a NUL terminator.
fn namestrcpy(dst: &mut types_tuple::heaptuple::NameData, src: &str) {
    dst.data.fill(0);
    let bytes = src.as_bytes();
    let len = bytes.len().min(types_core::NAMEDATALEN as usize - 1);
    dst.data[..len].copy_from_slice(&bytes[..len]);
}

/// `BlessTupleDesc(tupdesc)` (execTuples.c): register a transient record type
/// for an anonymous descriptor and return it.
pub fn BlessTupleDesc<'mcx>(
    _mcx: Mcx<'mcx>,
    mut tupdesc: TupleDesc<'mcx>,
) -> PgResult<TupleDesc<'mcx>> {
    if let Some(td) = tupdesc.as_deref_mut() {
        if td.tdtypeid == RECORDOID && td.tdtypmod < 0 {
            backend_utils_cache_typcache_seams::assign_record_type_typmod::call(td)?;
        }
    }
    Ok(tupdesc) // just for notational convenience
}

/// `TupleDescGetAttInMetadata(tupdesc)` (execTuples.c): build the per-attribute
/// input-function metadata for `BuildTupleFromCStrings`.
pub fn TupleDescGetAttInMetadata<'mcx>(
    mcx: Mcx<'mcx>,
    tupdesc: TupleDesc<'mcx>,
) -> PgResult<AttInMetadata<'mcx>> {
    let natts = tupdesc
        .as_deref()
        .map(|td| td.natts)
        .unwrap_or(0) as usize;

    // "Bless" the tupledesc so that we can make rowtype datums with it.
    let tupdesc = BlessTupleDesc(mcx, tupdesc)?;

    // Gather info needed later to call the "in" function for each attribute.
    let mut attinfuncs: mcx::PgVec<'mcx, types_core::fmgr::FmgrInfo> =
        mcx::PgVec::new_in(mcx);
    let mut attioparams: mcx::PgVec<'mcx, types_core::primitive::Oid> = mcx::PgVec::new_in(mcx);
    let mut atttypmods: mcx::PgVec<'mcx, i32> = mcx::PgVec::new_in(mcx);

    let td = tupdesc
        .as_deref()
        .expect("TupleDescGetAttInMetadata: tupdesc is NULL");
    for i in 0..natts {
        let att = td.attr(i);
        // Ignore dropped attributes (leave palloc0 zeroes / InvalidOid).
        if !att.attisdropped {
            // getTypeInputInfo(atttypeid, &attinfuncid, &attioparams[i]).
            let (attinfuncid, ioparam) =
                backend_utils_cache_lsyscache_seams::get_type_input_info::call(att.atttypid)?;
            // fmgr_info(attinfuncid, &attinfuncinfo[i]) — the owned `FmgrInfo`
            // carries only the OID; resolution is deferred to call time.
            attinfuncs.push(types_core::fmgr::FmgrInfo { fn_oid: attinfuncid, ..Default::default() });
            attioparams.push(ioparam);
            atttypmods.push(att.atttypmod);
        } else {
            attinfuncs.push(types_core::fmgr::FmgrInfo {
                fn_oid: types_core::primitive::InvalidOid,
                ..Default::default()
            });
            attioparams.push(types_core::primitive::InvalidOid);
            atttypmods.push(0);
        }
    }

    Ok(AttInMetadata {
        tupdesc,
        attinfuncs,
        attioparams,
        atttypmods,
    })
}

/// `BuildTupleFromCStrings(attinmeta, values)` (execTuples.c): build a heap
/// tuple from an array of C-string column values via the type input functions.
pub fn BuildTupleFromCStrings<'mcx>(
    mcx: Mcx<'mcx>,
    attinmeta: &AttInMetadata<'mcx>,
    values: &[Option<&str>],
) -> PgResult<HeapTuple<'mcx>> {
    let tupdesc = attinmeta
        .tupdesc
        .as_deref()
        .expect("BuildTupleFromCStrings: tupdesc is NULL");
    let natts = tupdesc.natts as usize;

    let mut dvalues: Vec<TupleValue<'mcx>> = Vec::with_capacity(natts);
    let mut nulls: Vec<bool> = Vec::with_capacity(natts);

    // Call the "in" function for each non-dropped attribute, even for nulls,
    // to support domains.
    for i in 0..natts {
        if !tupdesc.compact_attr(i).attisdropped {
            // Non-dropped attributes.
            let value = backend_utils_fmgr_fmgr_seams::input_function_call_for_heap_form::call(
                mcx,
                attinmeta.attinfuncs[i].fn_oid,
                values[i],
                attinmeta.attioparams[i],
                attinmeta.atttypmods[i],
                tupdesc.attr(i).attbyval,
            )?;
            dvalues.push(value);
            nulls.push(values[i].is_none());
        } else {
            // Handle dropped attributes by setting to NULL.
            dvalues.push(Datum::null());
            nulls.push(true);
        }
    }

    // Form a tuple.
    let formed = backend_access_common_heaptuple::heap_form_tuple(mcx, tupdesc, &dvalues, &nulls)
        .map_err(|e| types_error::PgError::error(format!("heap_form_tuple failed: {e:?}")))?;

    Ok(Some(formed.tuple))
}

/// `HeapTupleHeaderGetDatum(tuple)` (execTuples.c): wrap a heap tuple's header
/// as a composite `Datum` (flattening external TOAST pointers if present).
pub fn HeapTupleHeaderGetDatum<'mcx>(
    mcx: Mcx<'mcx>,
    tuple: HeapTuple<'mcx>,
) -> PgResult<(HeapTuple<'mcx>, TupleValue<'mcx>)> {
    // No work if there are no external TOAST pointers in the tuple. The
    // composite-Datum production (`PointerGetDatum`) and the detoast/flatten
    // path are the heap/datum owner's concern; reach both through its seam.
    backend_access_heap_heaptoast_seams::heap_tuple_header_get_datum::call(mcx, tuple)
}

/// `begin_tup_output_tupdesc(dest, tupdesc, tts_ops)` (execTuples.c): set up a
/// `TupOutputState` for sending rows of `tupdesc` to `dest`.
pub fn begin_tup_output_tupdesc<'mcx>(
    mcx: Mcx<'mcx>,
    dest: types_nodes::parsestmt::DestReceiverHandle,
    tupdesc: TupleDesc<'mcx>,
    tts_ops: types_nodes::TupleSlotKind,
) -> PgResult<TupOutputState<'mcx>> {
    // tstate->slot = MakeSingleTupleTableSlot(tupdesc, tts_ops);
    // C shares the `tupdesc` pointer between the slot and the rStartup call; the
    // owned model gives the slot its own copy and keeps the original for the
    // startup notification.
    let slot_desc: TupleDesc<'mcx> = match tupdesc.as_deref() {
        Some(td) => Some(mcx::alloc_in(mcx, td.clone_in(mcx)?)?),
        None => None,
    };
    let slot = crate::exec_init_slots::MakeSingleTupleTableSlot(mcx, slot_desc, tts_ops)?;

    // tstate->dest->rStartup(tstate->dest, (int) CMD_SELECT, tupdesc);
    if let Some(td) = tupdesc.as_deref() {
        backend_tcop_dest_seams::dest_rstartup::call(
            dest,
            types_nodes::nodes::CmdType::CMD_SELECT,
            td,
        )?;
    }

    Ok(TupOutputState { slot, dest })
}

/// `do_tup_output(tstate, values, isnull)` (execTuples.c): store one row into
/// the output slot and send it to the receiver.
pub fn do_tup_output<'mcx>(
    _mcx: Mcx<'mcx>,
    tstate: &mut TupOutputState<'mcx>,
    values: &[types_datum::Datum],
    isnull: &[bool],
) -> PgResult<()> {
    // int natts = slot->tts_tupleDescriptor->natts;
    let natts = tstate
        .slot
        .base()
        .tts_tupleDescriptor
        .as_deref()
        .map(|td| td.natts)
        .unwrap_or(0) as usize;

    // make sure the slot is clear
    crate::slot_store_fetch::ExecClearTuple(&mut tstate.slot)?;

    // insert data: memcpy(slot->tts_values, values, natts * sizeof(Datum));
    //              memcpy(slot->tts_isnull, isnull, natts * sizeof(bool));
    //
    // `do_tup_output`'s caller passes a bare `Datum *values` (C ABI); the slot's
    // expanded tts_values holds a `TupleValue`. These are the convenience-output
    // path's already-formed column words, carried verbatim as `ByVal` (matching
    // C's direct `tts_values[i] = values[i]` word copy).
    {
        let base = tstate.slot.base_mut();
        for i in 0..natts {
            base.tts_values[i] = TupleValue::ByVal(values[i]);
            base.tts_isnull[i] = isnull[i];
        }
    }

    // mark slot as containing a virtual tuple
    crate::slot_store_fetch::ExecStoreVirtualTuple(&mut tstate.slot)?;

    // send the tuple to the receiver
    let _ = backend_tcop_dest_seams::dest_receive_slot::call(&tstate.slot, tstate.dest)?;

    // clean up
    crate::slot_store_fetch::ExecClearTuple(&mut tstate.slot)?;
    Ok(())
}

/// `do_text_output_multiline(tstate, txt)` (execTuples.c): emit `txt` as one
/// single-text-column row per line.
pub fn do_text_output_multiline<'mcx>(
    mcx: Mcx<'mcx>,
    tstate: &mut TupOutputState<'mcx>,
    txt: &str,
) -> PgResult<()> {
    let isnull = [false];

    let mut rest = txt;
    while !rest.is_empty() {
        // eol = strchr(txt, '\n');
        let (line, next) = match rest.find('\n') {
            Some(pos) => (&rest[..pos], &rest[pos + 1..]),
            None => (rest, &rest[rest.len()..]),
        };

        // values[0] = PointerGetDatum(cstring_to_text_with_len(txt, len)); the
        // `line` slice already spans exactly `len` bytes.
        let datum = backend_utils_adt_varlena_seams::cstring_to_text::call(mcx, line)?;
        let values = [datum];
        do_tup_output(mcx, tstate, &values, &isnull)?;
        // pfree(DatumGetPointer(values[0])) — owned drop.
        rest = next;
    }
    Ok(())
}

/// `end_tup_output(tstate)` (execTuples.c): shut down the receiver and drop the
/// output slot.
pub fn end_tup_output<'mcx>(tstate: TupOutputState<'mcx>) -> PgResult<()> {
    // tstate->dest->rShutdown(tstate->dest);
    backend_tcop_dest_seams::dest_rshutdown::call(tstate.dest)?;
    // note that destroying the dest is not ours to do
    // ExecDropSingleTupleTableSlot(tstate->slot);
    crate::exec_init_slots::ExecDropSingleTupleTableSlot(tstate.slot)?;
    Ok(())
}

/// `&Slot` use marker — keeps the live-slot type referenced from the output
/// family's documented surface (the output slot is a [`SlotData`]).
#[allow(dead_code)]
fn _output_slot_marker(_slot: &SlotData) {}
