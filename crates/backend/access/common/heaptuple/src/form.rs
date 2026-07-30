use alloc::boxed::Box;

use ::datum::Datum;
use ::mcx::{vec_with_capacity_in, Mcx};
use ::types_core::InvalidOid;
use ::types_error::{PgError, PgResult, ERRCODE_TOO_MANY_COLUMNS};
use ::types_tuple::tupmacs::{att_addlength_datum, att_datum_alignby};
use ::types_tuple::{
    heap_deform_tuple, HeapTupleData, HeapTupleHeaderData, ItemPointerData, ItemPointerSetInvalid,
    MinimalTupleData, TupleDescData, BITMAPLEN, MAXALIGN, MINIMAL_TUPLE_OFFSET,
    MaxTupleAttributeNumber, SizeofHeapTupleHeader, SizeofMinimalTupleHeader,
};

use crate::fill::{fill_val, heap_fill_tuple, heap_compute_data_size};
use crate::tuple::{alloc_image, HeapTuple, MinimalTuple};

#[track_caller]
#[cold]
#[inline(never)]
fn too_many_columns(natts: usize) -> Box<PgError> {
    Box::new(
        PgError::error(alloc::format!(
            "number of columns ({natts}) exceeds limit ({MaxTupleAttributeNumber})"
        ))
        .with_sqlstate(ERRCODE_TOO_MANY_COLUMNS),
    )
}

pub fn heap_form_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    tupleDescriptor: &TupleDescData<'_>,
    values: &[Datum],
    isnull: &[bool],
) -> PgResult<HeapTuple<'mcx>> {
    let natts = tupleDescriptor.natts as usize;
    if natts > MaxTupleAttributeNumber as usize {
        return Err(too_many_columns(natts));
    }

    let hasnull = isnull[..natts].contains(&true);

    let mut len = SizeofHeapTupleHeader;
    if hasnull {
        len += BITMAPLEN(natts as i32) as usize;
    }
    let hoff = MAXALIGN(len);
    let data_len = heap_compute_data_size(tupleDescriptor, values, isnull);
    let len = hoff + data_len;

    let image = alloc_image(mcx, len, true)?;

    // SAFETY: fresh zeroed MAXALIGN'd image of len >= SizeofHeapTupleHeader bytes.
    unsafe {
        let td = &mut *image.as_ptr().cast::<HeapTupleHeaderData>();
        td.set_datum_length(len as u32);
        td.set_type_id(tupleDescriptor.tdtypeid);
        td.set_typmod(tupleDescriptor.tdtypmod);
        ItemPointerSetInvalid(&mut td.t_ctid);
        td.set_natts(natts as u16);
        td.t_hoff = hoff as u8;

        heap_fill_tuple(
            tupleDescriptor,
            values,
            isnull,
            image.as_ptr().add(hoff),
            data_len,
            &mut (*image.as_ptr().cast::<HeapTupleHeaderData>()).t_infomask,
            hasnull.then(|| image.as_ptr().add(SizeofHeapTupleHeader)),
        );

        Ok(HeapTuple::from_image(
            mcx,
            image,
            len as u32,
            ItemPointerData::invalid(),
            InvalidOid,
        ))
    }
}

/// execute_attr_map_tuple (tupconvert.c): remap `tuple` from `indesc`'s
/// rowtype to `outdesc`'s via attmap[out-1] = in attno (0 = null). C
/// preallocates the datum arrays in its TupleConversionMap; per-call vecs are
/// the cold inheritance-conversion lane only.
pub fn execute_attr_map_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    tuple: &HeapTupleData<'_>,
    indesc: &TupleDescData<'_>,
    outdesc: &TupleDescData<'_>,
    attmap: &[i16],
) -> PgResult<HeapTuple<'mcx>> {
    let in_natts = indesc.natts as usize;
    let out_natts = outdesc.natts as usize;
    debug_assert!(attmap.len() == out_natts);

    let mut invalues = vec_with_capacity_in(mcx, in_natts)?;
    let mut innulls = vec_with_capacity_in(mcx, in_natts)?;
    invalues.extend(core::iter::repeat(Datum::null()).take(in_natts));
    innulls.extend(core::iter::repeat(true).take(in_natts));
    heap_deform_tuple(tuple, indesc, &mut invalues, &mut innulls);

    let mut outvalues = vec_with_capacity_in(mcx, out_natts)?;
    let mut outnulls = vec_with_capacity_in(mcx, out_natts)?;
    for &m in attmap {
        if m > 0 {
            outvalues.push(invalues[m as usize - 1]);
            outnulls.push(innulls[m as usize - 1]);
        } else {
            outvalues.push(Datum::null());
            outnulls.push(true);
        }
    }
    heap_form_tuple(mcx, outdesc, &outvalues, &outnulls)
}

/// `extra` MAXALIGN'd bytes precede the tuple, zeroed (tuplestore/hashjoin metadata).
pub fn heap_form_minimal_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    tupleDescriptor: &TupleDescData<'_>,
    values: &[Datum],
    isnull: &[bool],
    extra: usize,
) -> PgResult<MinimalTuple<'mcx>> {
    debug_assert!(extra == MAXALIGN(extra));
    let natts = tupleDescriptor.natts as usize;
    if natts > MaxTupleAttributeNumber as usize {
        return Err(too_many_columns(natts));
    }

    let hasnull = isnull[..natts].contains(&true);

    let mut len = SizeofMinimalTupleHeader;
    if hasnull {
        len += BITMAPLEN(natts as i32) as usize;
    }
    let hoff = MAXALIGN(len);
    let data_len = heap_compute_data_size(tupleDescriptor, values, isnull);
    let len = hoff + data_len;

    let mut tuple = MinimalTuple::alloc(mcx, len, extra, true)?;

    // SAFETY: fresh zeroed image of len >= SizeofMinimalTupleHeader bytes.
    unsafe {
        let tp = tuple.tuple_mut_ptr();
        let mt = &mut *tp.cast::<MinimalTupleData>();
        mt.t_len = len as u32;
        mt.set_natts(natts as u16);
        mt.t_hoff = (hoff + MINIMAL_TUPLE_OFFSET) as u8;

        heap_fill_tuple(
            tupleDescriptor,
            values,
            isnull,
            tp.add(hoff),
            data_len,
            &mut (*tp.cast::<MinimalTupleData>()).t_infomask,
            hasnull.then(|| tp.add(SizeofMinimalTupleHeader)),
        );
    }

    Ok(tuple)
}

/// Whole-tuple copy; C's NULL-input check dissolves into the reference type.
pub fn heap_copytuple<'mcx>(
    mcx: Mcx<'mcx>,
    tuple: &HeapTupleData<'_>,
) -> PgResult<HeapTuple<'mcx>> {
    let len = tuple.t_len as usize;
    let image = alloc_image(mcx, len, false)?;
    // SAFETY: source readable for t_len (HeapTupleData invariant); dest fresh.
    unsafe {
        core::ptr::copy_nonoverlapping(tuple.header_ptr(), image.as_ptr(), len);
        Ok(HeapTuple::from_image(
            mcx,
            image,
            tuple.t_len,
            tuple.t_self,
            tuple.t_tableOid,
        ))
    }
}

/// Copy as a composite-type Datum; the image is context-owned (C's palloc'd return).
pub fn heap_copy_tuple_as_datum<'mcx>(
    mcx: Mcx<'mcx>,
    tuple: &HeapTupleData<'_>,
    tupleDesc: &TupleDescData<'_>,
) -> PgResult<Datum> {
    // Composite-type Datums must not contain external TOAST pointers.
    if tuple.has_external() {
        return ::detoast_seams::toast_flatten_tuple_to_datum::call(mcx, tuple, tupleDesc);
    }

    let len = tuple.t_len as usize;
    let image = alloc_image(mcx, len, false)?;
    // SAFETY: source readable for t_len; dest fresh, header-aligned.
    unsafe {
        core::ptr::copy_nonoverlapping(tuple.header_ptr(), image.as_ptr(), len);
        let td = &mut *image.as_ptr().cast::<HeapTupleHeaderData>();
        td.set_datum_length(tuple.t_len);
        td.set_type_id(tupleDesc.tdtypeid);
        td.set_typmod(tupleDesc.tdtypmod);
    }
    Ok(Datum::from_usize(image.as_ptr() as usize))
}

const MODIFY_STACK_ATTS: usize = 32;

// Deform scratch: stack for narrow rows (beats C's palloc/pfree pair).
#[inline]
fn with_deformed<'mcx, R>(
    mcx: Mcx<'mcx>,
    tuple: &HeapTupleData<'_>,
    tupleDesc: &TupleDescData<'_>,
    f: impl FnOnce(&mut [Datum], &mut [bool]) -> R,
) -> PgResult<R> {
    let natts = tupleDesc.natts as usize;
    if natts <= MODIFY_STACK_ATTS {
        let mut values = [Datum::null(); MODIFY_STACK_ATTS];
        let mut isnull = [true; MODIFY_STACK_ATTS];
        heap_deform_tuple(tuple, tupleDesc, &mut values[..natts], &mut isnull[..natts]);
        Ok(f(&mut values[..natts], &mut isnull[..natts]))
    } else {
        let mut values = vec_with_capacity_in::<Datum>(mcx, natts)?;
        let mut isnull = vec_with_capacity_in::<bool>(mcx, natts)?;
        values.resize(natts, Datum::null());
        isnull.resize(natts, true);
        heap_deform_tuple(tuple, tupleDesc, &mut values, &mut isnull);
        Ok(f(&mut values, &mut isnull))
    }
}

pub fn heap_modify_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    tuple: &HeapTupleData<'_>,
    tupleDesc: &TupleDescData<'_>,
    replValues: &[Datum],
    replIsnull: &[bool],
    doReplace: &[bool],
) -> PgResult<HeapTuple<'mcx>> {
    let natts = tupleDesc.natts as usize;
    let mut newTuple = with_deformed(mcx, tuple, tupleDesc, |values, isnull| {
        for attoff in 0..natts {
            if doReplace[attoff] {
                values[attoff] = replValues[attoff];
                isnull[attoff] = replIsnull[attoff];
            }
        }
        heap_form_tuple(mcx, tupleDesc, values, isnull)
    })??;

    newTuple.t_data_mut().t_ctid = tuple.t_data().t_ctid;
    newTuple.t_self = tuple.t_self;
    newTuple.t_tableOid = tuple.t_tableOid;

    Ok(newTuple)
}

/// Like [`heap_modify_tuple`] with 1-based target column numbers.
pub fn heap_modify_tuple_by_cols<'mcx>(
    mcx: Mcx<'mcx>,
    tuple: &HeapTupleData<'_>,
    tupleDesc: &TupleDescData<'_>,
    replCols: &[i32],
    replValues: &[Datum],
    replIsnull: &[bool],
) -> PgResult<HeapTuple<'mcx>> {
    let natts = tupleDesc.natts as usize;
    let mut newTuple = with_deformed(mcx, tuple, tupleDesc, |values, isnull| {
        for (i, &attnum) in replCols.iter().enumerate() {
            if attnum <= 0 || attnum > natts as i32 {
                panic!("invalid column number {attnum}");
            }
            values[(attnum - 1) as usize] = replValues[i];
            isnull[(attnum - 1) as usize] = replIsnull[i];
        }
        heap_form_tuple(mcx, tupleDesc, values, isnull)
    })??;

    newTuple.t_data_mut().t_ctid = tuple.t_data().t_ctid;
    newTuple.t_self = tuple.t_self;
    newTuple.t_tableOid = tuple.t_tableOid;

    Ok(newTuple)
}

/// `mtup` is the flat minimal-tuple image (`len == t_len`).
pub fn heap_copy_minimal_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    mtup: &[u8],
    extra: usize,
) -> PgResult<MinimalTuple<'mcx>> {
    debug_assert!(extra == MAXALIGN(extra));
    let mut result = MinimalTuple::alloc(mcx, mtup.len(), extra, false)?;
    // SAFETY: dest owns t_len bytes at the tuple offset.
    unsafe { core::ptr::copy_nonoverlapping(mtup.as_ptr(), result.tuple_mut_ptr(), mtup.len()) };
    debug_assert_eq!(result.data().t_len as usize, mtup.len());
    Ok(result)
}

/// HeapTuple from a flat minimal-tuple image; system columns zeroed.
pub fn heap_tuple_from_minimal_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    mtup: &[u8],
) -> PgResult<HeapTuple<'mcx>> {
    let len = mtup.len() + MINIMAL_TUPLE_OFFSET;
    let image = alloc_image(mcx, len, false)?;
    // SAFETY: fresh image of len bytes; offset 18 == offsetof(t_infomask2).
    unsafe {
        core::ptr::copy_nonoverlapping(
            mtup.as_ptr(),
            image.as_ptr().add(MINIMAL_TUPLE_OFFSET),
            mtup.len(),
        );
        core::ptr::write_bytes(
            image.as_ptr(),
            0,
            core::mem::offset_of!(HeapTupleHeaderData, t_infomask2),
        );
        Ok(HeapTuple::from_image(
            mcx,
            image,
            len as u32,
            ItemPointerData::invalid(),
            InvalidOid,
        ))
    }
}

pub fn minimal_tuple_from_heap_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    htup: &HeapTupleData<'_>,
    extra: usize,
) -> PgResult<MinimalTuple<'mcx>> {
    debug_assert!(extra == MAXALIGN(extra));
    assert!(htup.t_len as usize > MINIMAL_TUPLE_OFFSET);
    let len = htup.t_len as usize - MINIMAL_TUPLE_OFFSET;
    let mut result = MinimalTuple::alloc(mcx, len, extra, false)?;
    // SAFETY: source readable for t_len (HeapTupleData invariant); dest owns len bytes.
    unsafe {
        core::ptr::copy_nonoverlapping(
            htup.header_ptr().add(MINIMAL_TUPLE_OFFSET),
            result.tuple_mut_ptr(),
            len,
        );
    }
    result.data_mut().t_len = len as u32;
    Ok(result)
}

struct ExpandPlan {
    source_natts: usize,
    source_data_len: usize,
    source_null_len: usize,
    target_data_len: usize,
    target_null_len: usize,
}

fn expand_plan(sourceTuple: &HeapTupleData<'_>, tupleDesc: &TupleDescData<'_>) -> ExpandPlan {
    let source_header = sourceTuple.t_data();
    let source_natts = source_header.natts() as usize;
    let natts = tupleDesc.natts as usize;
    let mut has_nulls = sourceTuple.has_nulls();
    debug_assert!(source_natts < natts);

    let source_null_len = if has_nulls {
        BITMAPLEN(source_natts as i32) as usize
    } else {
        0
    };
    let source_data_len = sourceTuple.t_len as usize - source_header.t_hoff as usize;
    let mut target_data_len = source_data_len;

    match tupleDesc.constr.as_ref().filter(|c| !c.missing.is_empty()) {
        Some(constr) => {
            let attrmiss = &constr.missing;
            for attnum in source_natts..natts {
                if attrmiss[attnum].am_present {
                    let att = &tupleDesc.compact_attrs[attnum];
                    // SAFETY: am_value points at a live descriptor-owned value.
                    unsafe {
                        target_data_len = att_datum_alignby(
                            target_data_len,
                            att.attalignby,
                            att.attlen as i32,
                            attrmiss[attnum].am_value,
                        );
                        target_data_len = att_addlength_datum(
                            target_data_len,
                            att.attlen as i32,
                            attrmiss[attnum].am_value,
                        );
                    }
                } else {
                    has_nulls = true;
                }
            }
        }
        None => has_nulls = true,
    }

    ExpandPlan {
        source_natts,
        source_data_len,
        source_null_len,
        target_data_len,
        target_null_len: if has_nulls {
            BITMAPLEN(tupleDesc.natts) as usize
        } else {
            0
        },
    }
}

/// # Safety
/// `null_bits`/`target_data` point into a fresh zeroed target image laid out
/// per `plan`; `infomask` aliases nothing reachable from them.
unsafe fn expand_fill_tail(
    plan: &ExpandPlan,
    sourceTuple: &HeapTupleData<'_>,
    tupleDesc: &TupleDescData<'_>,
    null_bits: Option<*mut u8>,
    target_data: *mut u8,
    infomask: &mut u16,
) {
    let natts = tupleDesc.natts as usize;

    if let Some(bits) = null_bits {
        if plan.source_null_len > 0 {
            core::ptr::copy_nonoverlapping(
                sourceTuple.header_ptr().add(SizeofHeapTupleHeader),
                bits,
                plan.source_null_len,
            );
        } else {
            // all existing attributes are NOT NULL; clear excess bits in the last byte
            let src_bitmap_len = BITMAPLEN(plan.source_natts as i32) as usize;
            core::ptr::write_bytes(bits, 0xff, src_bitmap_len);
            if plan.source_natts & 0x07 != 0 {
                *bits.add(src_bitmap_len - 1) = !(0xffu8 << (plan.source_natts & 0x07));
            }
        }
    }

    core::ptr::copy_nonoverlapping(sourceTuple.getstruct(), target_data, plan.source_data_len);

    let attrmiss = tupleDesc.constr.as_ref().map(|c| &c.missing);
    let mut off = plan.source_data_len;
    for attnum in plan.source_natts..natts {
        let attr = &tupleDesc.compact_attrs[attnum];
        match attrmiss.filter(|m| !m.is_empty() && m[attnum].am_present) {
            Some(m) => fill_val(
                attr,
                null_bits.map(|bp| (bp, attnum)),
                target_data,
                &mut off,
                infomask,
                m[attnum].am_value,
                false,
            ),
            None => fill_val(
                attr,
                null_bits.map(|bp| (bp, attnum)),
                target_data,
                &mut off,
                infomask,
                Datum::null(),
                true,
            ),
        }
    }
    debug_assert_eq!(off, plan.target_data_len);
}

/// Widen a tuple to `tupleDesc.natts` attributes, filling missing values or nulls.
pub fn heap_expand_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    sourceTuple: &HeapTupleData<'_>,
    tupleDesc: &TupleDescData<'_>,
) -> PgResult<HeapTuple<'mcx>> {
    let plan = expand_plan(sourceTuple, tupleDesc);
    let hoff = MAXALIGN(SizeofHeapTupleHeader + plan.target_null_len);
    let len = hoff + plan.target_data_len;
    let image = alloc_image(mcx, len, true)?;

    // SAFETY: fresh zeroed image laid out per plan.
    unsafe {
        let source_header = sourceTuple.t_data();
        let td = &mut *image.as_ptr().cast::<HeapTupleHeaderData>();
        td.t_infomask = source_header.t_infomask;
        td.t_hoff = hoff as u8;
        td.set_natts(tupleDesc.natts as u16);
        td.set_datum_length(len as u32);
        td.set_type_id(tupleDesc.tdtypeid);
        td.set_typmod(tupleDesc.tdtypmod);
        ItemPointerSetInvalid(&mut td.t_ctid);

        expand_fill_tail(
            &plan,
            sourceTuple,
            tupleDesc,
            (plan.target_null_len > 0).then(|| image.as_ptr().add(SizeofHeapTupleHeader)),
            image.as_ptr().add(hoff),
            &mut (*image.as_ptr().cast::<HeapTupleHeaderData>()).t_infomask,
        );

        Ok(HeapTuple::from_image(
            mcx,
            image,
            len as u32,
            sourceTuple.t_self,
            sourceTuple.t_tableOid,
        ))
    }
}

pub fn minimal_expand_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    sourceTuple: &HeapTupleData<'_>,
    tupleDesc: &TupleDescData<'_>,
) -> PgResult<MinimalTuple<'mcx>> {
    let plan = expand_plan(sourceTuple, tupleDesc);
    let hoff = MAXALIGN(SizeofMinimalTupleHeader + plan.target_null_len);
    let len = hoff + plan.target_data_len;
    let mut tuple = MinimalTuple::alloc(mcx, len, 0, true)?;

    // SAFETY: fresh zeroed image laid out per plan.
    unsafe {
        let tp = tuple.tuple_mut_ptr();
        let mt = &mut *tp.cast::<MinimalTupleData>();
        mt.t_len = len as u32;
        mt.t_hoff = (hoff + MINIMAL_TUPLE_OFFSET) as u8;
        mt.t_infomask = sourceTuple.t_data().t_infomask;
        mt.set_natts(tupleDesc.natts as u16);

        expand_fill_tail(
            &plan,
            sourceTuple,
            tupleDesc,
            (plan.target_null_len > 0).then(|| tp.add(SizeofMinimalTupleHeader)),
            tp.add(hoff),
            &mut (*tp.cast::<MinimalTupleData>()).t_infomask,
        );
    }

    Ok(tuple)
}
