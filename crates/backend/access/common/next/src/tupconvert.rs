//! `backend/access/common/tupconvert.c` — tuple conversion support.
//!
//! Conversion between rowtypes that are logically equivalent but differ in
//! column order or dropped-column sets. The setup routines check compatibility
//! via `attmap.rs`, return `Ok(None)` (C `NULL`) when no runtime conversion is
//! needed, and otherwise build a [`TupleConversionMap`].
//!
//! The repo [`TupleConversionMap`] carries only `indesc`/`outdesc`/`attrMap`;
//! the C struct's per-conversion workspace arrays (`invalues`/`outvalues`/...)
//! are recomputed locally by [`execute_attr_map_tuple`] over the owned tuple
//! model. The descriptors are cloned into the map (C references the caller's
//! descriptors and documents that they must outlive the map; the owned map
//! carries its own copies, satisfying that trivially).

use mcx::{alloc_in, vec_with_capacity_in, Mcx, PgBox};
use types_error::{PgError, PgResult};
use nodes::{Bitmapset, EStateData, SlotId};
use ::types_tuple::attmap::AttrMap;
use ::types_tuple::heaptuple::{Datum, FormedTuple};
use ::types_tuple::heaptuple::{
    FirstLowInvalidHeapAttributeNumber, HeapTupleData, TupleDesc, TupleDescData,
};
use ::types_tuple::tupconvert::TupleConversionMap;

use heaptuple::{heap_deform_tuple, heap_form_tuple, HeapTupleError};
use ::nodes_core::bitmapset::{bms_add_member, bms_is_member};

use crate::attmap::{build_attrmap_by_name_if_req, build_attrmap_by_position};

/// Map a [`HeapTupleError`] from the form/deform core back to a [`PgError`],
/// preserving the C `ereport(ERROR, ...)` it stands for.
fn from_heaptuple(e: HeapTupleError) -> PgError {
    match e {
        HeapTupleError::TooManyColumns { columns, limit } => PgError::error(format!(
            "number of columns ({columns}) exceeds limit ({limit})"
        )),
        HeapTupleError::InvalidColumnNumber { attnum } => {
            PgError::error(format!("invalid column number {attnum}"))
        }
        HeapTupleError::Pg(err) => err,
    }
}

/// `convert_tuples_by_position(indesc, outdesc, msg)` (tupconvert.c) — set up
/// for tuple conversion, matching input and output columns by position
/// (dropped columns ignored). `Ok(None)` when no runtime conversion is needed.
pub fn convert_tuples_by_position<'mcx>(
    mcx: Mcx<'mcx>,
    indesc: &TupleDescData<'_>,
    outdesc: &TupleDescData<'_>,
    msg: &str,
) -> PgResult<Option<PgBox<'mcx, TupleConversionMap<'mcx>>>> {
    // Verify compatibility and prepare the attribute-number map.
    let Some(attr_map) = build_attrmap_by_position(mcx, indesc, outdesc, msg)? else {
        // Runtime conversion is not needed.
        return Ok(None);
    };

    // C also preallocates the in/out workspace Datum arrays here; the owned
    // model recomputes them per-conversion in execute_attr_map_tuple, so the
    // map carries only the (cloned) descriptors and attribute map.
    let in_owned = Some(alloc_in(mcx, indesc.clone_in(mcx)?)?);
    let out_owned = Some(alloc_in(mcx, outdesc.clone_in(mcx)?)?);
    let map = build_map(mcx, in_owned, out_owned, attr_map)?;
    Ok(Some(map))
}

/// `convert_tuples_by_name(indesc, outdesc)` (tupconvert.c) — set up for tuple
/// conversion, matching input and output columns by name (dropped columns
/// ignored), expecting an exact match of type and typmod.
pub fn convert_tuples_by_name<'mcx>(
    mcx: Mcx<'mcx>,
    indesc: &TupleDescData<'_>,
    outdesc: &TupleDescData<'_>,
) -> PgResult<Option<PgBox<'mcx, TupleConversionMap<'mcx>>>> {
    // Verify compatibility and prepare the attribute-number map.
    let Some(attr_map) = build_attrmap_by_name_if_req(mcx, indesc, outdesc, false)? else {
        // Runtime conversion is not needed.
        return Ok(None);
    };

    // The by-name-attrmap variant takes ownership of the descriptors; clone the
    // borrowed ones into mcx-owned copies (C references the caller's, which it
    // documents must outlive the map).
    let in_owned = Some(alloc_in(mcx, indesc.clone_in(mcx)?)?);
    let out_owned = Some(alloc_in(mcx, outdesc.clone_in(mcx)?)?);
    convert_tuples_by_name_attrmap(mcx, in_owned, out_owned, attr_map).map(Some)
}

/// `convert_tuples_by_name_attrmap(indesc, outdesc, attrMap)` (tupconvert.c) —
/// set up tuple conversion using the given (non-identity) [`AttrMap`]. The
/// owned descriptors move into the map.
pub fn convert_tuples_by_name_attrmap<'mcx>(
    mcx: Mcx<'mcx>,
    indesc: TupleDesc<'mcx>,
    outdesc: TupleDesc<'mcx>,
    attr_map: PgBox<'mcx, AttrMap<'mcx>>,
) -> PgResult<PgBox<'mcx, TupleConversionMap<'mcx>>> {
    // C: Assert(attrMap != NULL) -- `attr_map` is non-optional here.
    build_map(mcx, indesc, outdesc, attr_map)
}

/// Shared body of the setup routines: assemble the [`TupleConversionMap`] from
/// the (already mcx-owned) descriptors and attribute map.
fn build_map<'mcx>(
    mcx: Mcx<'mcx>,
    indesc: TupleDesc<'mcx>,
    outdesc: TupleDesc<'mcx>,
    attr_map: PgBox<'mcx, AttrMap<'mcx>>,
) -> PgResult<PgBox<'mcx, TupleConversionMap<'mcx>>> {
    alloc_in(
        mcx,
        TupleConversionMap {
            indesc,
            outdesc,
            attrMap: attr_map,
        },
    )
}

/// `execute_attr_map_tuple(tuple, map)` (tupconvert.c) — convert a tuple
/// according to the map, returning the formed output tuple.
///
/// In the owned heaptuple model a tuple's user-data area travels alongside the
/// header (see `heaptuple`), so the caller supplies the
/// source tuple plus its `data` area (the bytes at `t_data + t_hoff`).
pub fn execute_attr_map_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    tuple: &HeapTupleData<'_>,
    data: &[u8],
    map: &TupleConversionMap<'_>,
) -> PgResult<FormedTuple<'mcx>> {
    let indesc = map
        .indesc
        .as_ref()
        .ok_or_else(|| PgError::error("tuple conversion map has no input descriptor"))?;
    let outdesc = map
        .outdesc
        .as_ref()
        .ok_or_else(|| PgError::error("tuple conversion map has no output descriptor"))?;
    let attr_map = &map.attrMap;

    // Extract all values of the old tuple, offsetting the arrays so invalues[0]
    // is NULL and invalues[1] is the first source attribute (matching the
    // 1-based numbering in attrMap).
    //
    // C: heap_deform_tuple(tuple, map->indesc, invalues + 1, inisnull + 1);
    let deformed = heap_deform_tuple(mcx, tuple, indesc, data)?;

    let in_n = indesc.natts as usize + 1;
    let mut invalues: ::mcx::PgVec<'mcx, Datum<'mcx>> = vec_with_capacity_in(mcx, in_n)?;
    let mut inisnull: ::mcx::PgVec<'mcx, bool> = vec_with_capacity_in(mcx, in_n)?;
    invalues.push(Datum::null()); // the NULL entry
    inisnull.push(true);
    for (value, isnull) in deformed.iter() {
        invalues.push(value.clone_in(mcx)?);
        inisnull.push(*isnull);
    }

    // Transpose into proper fields of the new tuple.
    // C: Assert(attrMap->maplen == map->outdesc->natts);
    debug_assert_eq!(attr_map.attnums.len() as i32, outdesc.natts);

    let maplen = attr_map.attnums.len();
    let mut outvalues: ::mcx::PgVec<'mcx, Datum<'mcx>> = vec_with_capacity_in(mcx, maplen)?;
    let mut outisnull: ::mcx::PgVec<'mcx, bool> = vec_with_capacity_in(mcx, maplen)?;

    for &attnum in attr_map.attnums.iter() {
        // int j = attrMap->attnums[i]; outvalues[i] = invalues[j];
        let j = usize::try_from(attnum)
            .map_err(|_| PgError::error("invalid attribute map entry"))?;
        let value = invalues
            .get(j)
            .ok_or_else(|| PgError::error("attribute map index out of range"))?
            .clone_in(mcx)?;
        let isnull = inisnull[j];
        outvalues.push(value);
        outisnull.push(isnull);
    }

    // Now form the new tuple.
    heap_form_tuple(mcx, outdesc, &outvalues, &outisnull).map_err(from_heaptuple)
}

/// `execute_attr_map_slot(attrMap, in_slot, out_slot)` (tupconvert.c) — remap
/// the attributes of the tuple in `in_slot` through `attr_map` into `out_slot`
/// (a virtual tuple), returning the id of `out_slot`.
///
/// The slot value/null payload arrays (`tts_values`/`tts_isnull`) are owned by
/// the executor tuple-table layer (`execTuples`); the trimmed [`TupleTableSlot`]
/// here carries only header bits. The conversion -- materialize the input
/// slot, clear the output slot, transpose the value arrays, store a virtual
/// tuple -- therefore belongs to the slot-payload owner. We delegate to
/// execTuples' `execute_attr_map_slot_explicit` (the AttrMap-supplied variant),
/// which panics with "seam not installed" until execTuples lands.
pub fn execute_attr_map_slot<'mcx>(
    estate: &mut EStateData<'mcx>,
    attr_map: &AttrMap<'_>,
    in_slot: SlotId,
    out_slot: SlotId,
) -> PgResult<SlotId> {
    // The delegate's contract ties the map's allocation to the query lifetime
    // (`&AttrMap<'mcx>`), so copy the borrowed attnums into the per-query
    // context (C shares the pointer; the owned model duplicates it,
    // behavior-preserving since the transpose only reads the map).
    let mcx = estate.es_query_cxt;
    let attnums = ::mcx::slice_in(mcx, &attr_map.attnums)?;
    let owned = ::mcx::alloc_in(mcx, AttrMap { attnums })?;
    execTuples_seams::execute_attr_map_slot_explicit::call(
        estate, &owned, in_slot, out_slot,
    )
}

/// `execute_attr_map_cols(attrMap, in_cols)` (tupconvert.c) — convert a bitmap
/// of columns according to the map. The input and output bitmaps are offset by
/// `FirstLowInvalidHeapAttributeNumber` to accommodate system columns (like the
/// column-bitmaps in `RangeTblEntry`).
pub fn execute_attr_map_cols<'mcx>(
    mcx: Mcx<'mcx>,
    attr_map: &AttrMap<'_>,
    in_cols: Option<&Bitmapset<'_>>,
) -> PgResult<Option<PgBox<'mcx, Bitmapset<'mcx>>>> {
    // Fast path for the common trivial case.
    let Some(in_cols) = in_cols else {
        return Ok(None);
    };

    // For each output column, check which input column it corresponds to.
    let mut out_cols: Option<PgBox<'mcx, Bitmapset<'mcx>>> = None;

    let maplen = attr_map.attnums.len() as i32;
    let mut out_attnum: i32 = FirstLowInvalidHeapAttributeNumber as i32;
    while out_attnum <= maplen {
        let in_attnum: i32;

        if out_attnum < 0 {
            // System column. No mapping.
            in_attnum = out_attnum;
        } else if out_attnum == 0 {
            out_attnum += 1;
            continue;
        } else {
            // Normal user column.
            let mapped = attr_map.attnums[(out_attnum - 1) as usize];
            if mapped == 0 {
                out_attnum += 1;
                continue;
            }
            in_attnum = mapped as i32;
        }

        let in_member = in_attnum - FirstLowInvalidHeapAttributeNumber as i32;
        if bms_is_member(in_member, Some(in_cols)) {
            let out_member = out_attnum - FirstLowInvalidHeapAttributeNumber as i32;
            out_cols = Some(bms_add_member(mcx, out_cols, out_member)?);
        }

        out_attnum += 1;
    }

    Ok(out_cols)
}

/// `free_conversion_map(map)` (tupconvert.c) — free a [`TupleConversionMap`].
/// In the owned model the map and its workspace and descriptor copies are
/// released by dropping the value; taking it by value reproduces the C
/// `pfree`s. (C's "indesc and outdesc are not ours to free" applies to the
/// caller's *referenced* descriptors; the owned map holds its own copies.)
pub fn free_conversion_map(map: PgBox<'_, TupleConversionMap<'_>>) {
    drop(map);
}
