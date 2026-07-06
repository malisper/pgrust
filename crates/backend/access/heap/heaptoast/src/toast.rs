use ::datum::Datum;
use ::mcx::{Mcx, PgVec};
use ::types_error::PgResult;
use ::types_rel::{RelationData, RELKIND_MATVIEW, RELKIND_RELATION};
use ::types_tuple::{
    heap_deform_tuple, HeapTupleData, TupleDescData, BITMAPLEN, HEAP2_XACT_MASK, HEAP_XACT_MASK,
    MAXALIGN, SizeofHeapTupleHeader, TYPSTORAGE_EXTENDED,
};
use heaptuple::{heap_compute_data_size, heap_fill_tuple, heap_form_tuple, HeapTuple};

use crate::helper::{
    toast_delete_external, toast_tuple_cleanup, toast_tuple_externalize,
    toast_tuple_find_biggest_attribute, toast_tuple_init, toast_tuple_try_compression, va_slice,
    ToastAttrInfo, ToastTupleContext, TOASTCOL_INCOMPRESSIBLE, TOAST_HAS_NULLS,
    TOAST_NEEDS_CHANGE,
};
use crate::internals::reltoastrelid_valid;
use crate::{TOAST_TUPLE_TARGET, TOAST_TUPLE_TARGET_MAIN};

// HEAP_INSERT_SPECULATIVE (heapam.h); mirrored from heapam::hio.
const HEAP_INSERT_SPECULATIVE: i32 = 0x0010;

fn deform<'mcx>(
    mcx: Mcx<'mcx>,
    tup: &HeapTupleData<'_>,
    desc: &TupleDescData<'_>,
) -> (PgVec<'mcx, Datum>, PgVec<'mcx, bool>) {
    let natts = desc.natts as usize;
    let mut values = ::mcx::vec_from_elem_in(mcx, Datum::null(), natts);
    let mut isnull = ::mcx::vec_from_elem_in(mcx, false, natts);
    heap_deform_tuple(tup, desc, &mut values, &mut isnull);
    (values, isnull)
}

/// C `heap_toast_delete`.
pub fn heap_toast_delete<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &RelationData<'_>,
    oldtup: &HeapTupleData<'_>,
    is_speculative: bool,
) -> PgResult<()> {
    debug_assert!(
        rel.rd_rel.relkind == RELKIND_RELATION || rel.rd_rel.relkind == RELKIND_MATVIEW
    );
    let tuple_desc = rel.rd_att.clone();
    let (toast_values, toast_isnull) = deform(mcx, oldtup, &tuple_desc);
    toast_delete_external(mcx, rel, &toast_values, &toast_isnull, is_speculative)
}

/// C `heap_toast_insert_or_update`; `None` is C's "return newtup" (no change).
/// `rd_toastoid` is C's transient NewHeap->rd_toastoid: InvalidOid everywhere
/// but the CLUSTER/VACUUM FULL rewrite (rewriteheap threads it through).
pub fn heap_toast_insert_or_update<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &RelationData<'_>,
    newtup: &HeapTupleData<'_>,
    oldtup: Option<&HeapTupleData<'_>>,
    rd_toastoid: ::types_core::Oid,
    options: i32,
) -> PgResult<Option<HeapTuple<'mcx>>> {
    let options = options & !HEAP_INSERT_SPECULATIVE;

    debug_assert!(
        rel.rd_rel.relkind == RELKIND_RELATION || rel.rd_rel.relkind == RELKIND_MATVIEW
    );

    let tuple_desc = rel.rd_att.clone();
    let num_attrs = tuple_desc.natts as usize;

    let (mut toast_values, toast_isnull) = deform(mcx, newtup, &tuple_desc);
    let old = oldtup.map(|t| deform(mcx, t, &tuple_desc));

    let mut toast_attr =
        ::mcx::vec_from_elem_in(
            mcx,
            ToastAttrInfo {
                tai_oldexternal: None,
                tai_size: 0,
                tai_colflags: 0,
                tai_compression: 0,
            },
            num_attrs,
        );
    let mut ttc = ToastTupleContext {
        ttc_rel: rel,
        ttc_values: &mut toast_values,
        ttc_isnull: &toast_isnull,
        ttc_oldvalues: old.as_ref().map(|(v, _)| v.as_slice()),
        ttc_oldisnull: old.as_ref().map(|(_, n)| n.as_slice()),
        ttc_attr: &mut toast_attr,
        ttc_flags: 0,
        ttc_toastoid: rd_toastoid,
    };
    toast_tuple_init(mcx, &mut ttc)?;

    let mut hoff = SizeofHeapTupleHeader;
    if (ttc.ttc_flags & TOAST_HAS_NULLS) != 0 {
        hoff += BITMAPLEN(num_attrs as i32) as usize;
    }
    let hoff = MAXALIGN(hoff);
    let mut max_data_len =
        rel.get_toast_tuple_target(TOAST_TUPLE_TARGET as i32) as usize - hoff;

    // Pass 1: compress EXTENDED attrs; push any still-oversized value out.
    while heap_compute_data_size(&tuple_desc, ttc.ttc_values, ttc.ttc_isnull) > max_data_len {
        let biggest = toast_tuple_find_biggest_attribute(&ttc, true, false);
        if biggest < 0 {
            break;
        }
        let biggest = biggest as usize;

        if tuple_desc.attrs[biggest].attstorage == TYPSTORAGE_EXTENDED {
            toast_tuple_try_compression(mcx, &mut ttc, biggest)?;
        } else {
            ttc.ttc_attr[biggest].tai_colflags |= TOASTCOL_INCOMPRESSIBLE;
        }

        if ttc.ttc_attr[biggest].tai_size > max_data_len as i32 && reltoastrelid_valid(rel) {
            toast_tuple_externalize(mcx, &mut ttc, biggest, options)?;
        }
    }

    // Pass 2: move EXTENDED/EXTERNAL attrs out until it fits.
    while heap_compute_data_size(&tuple_desc, ttc.ttc_values, ttc.ttc_isnull) > max_data_len
        && reltoastrelid_valid(rel)
    {
        let biggest = toast_tuple_find_biggest_attribute(&ttc, false, false);
        if biggest < 0 {
            break;
        }
        toast_tuple_externalize(mcx, &mut ttc, biggest as usize, options)?;
    }

    // Pass 3: compress MAIN attrs.
    while heap_compute_data_size(&tuple_desc, ttc.ttc_values, ttc.ttc_isnull) > max_data_len {
        let biggest = toast_tuple_find_biggest_attribute(&ttc, true, true);
        if biggest < 0 {
            break;
        }
        toast_tuple_try_compression(mcx, &mut ttc, biggest as usize)?;
    }

    // Pass 4: move MAIN attrs out, against the whole-page target.
    max_data_len = TOAST_TUPLE_TARGET_MAIN - hoff;
    while heap_compute_data_size(&tuple_desc, ttc.ttc_values, ttc.ttc_isnull) > max_data_len
        && reltoastrelid_valid(rel)
    {
        let biggest = toast_tuple_find_biggest_attribute(&ttc, false, true);
        if biggest < 0 {
            break;
        }
        toast_tuple_externalize(mcx, &mut ttc, biggest as usize, options)?;
    }

    let result = if (ttc.ttc_flags & TOAST_NEEDS_CHANGE) != 0 {
        // Recompute the header: an old pre-ALTER-TABLE tuple can carry a
        // different natts, hence a different bitmap size than newtup's hoff.
        let mut new_header_len = SizeofHeapTupleHeader;
        if (ttc.ttc_flags & TOAST_HAS_NULLS) != 0 {
            new_header_len += BITMAPLEN(num_attrs as i32) as usize;
        }
        let new_header_len = MAXALIGN(new_header_len);
        let new_data_len = heap_compute_data_size(&tuple_desc, ttc.ttc_values, ttc.ttc_isnull);
        let new_tuple_len = new_header_len + new_data_len;

        let mut result_tuple = HeapTuple::alloc_zeroed(mcx, new_tuple_len)?;
        result_tuple.as_tuple_mut().t_self = newtup.t_self;
        result_tuple.as_tuple_mut().t_tableOid = newtup.t_tableOid;

        // SAFETY: newtup header is SizeofHeapTupleHeader readable bytes.
        let old_header = unsafe {
            core::slice::from_raw_parts(newtup.header_ptr(), SizeofHeapTupleHeader)
        };
        result_tuple.image_mut()[..SizeofHeapTupleHeader].copy_from_slice(old_header);
        {
            let hdr = result_tuple.as_tuple_mut().t_data_mut();
            hdr.set_natts(num_attrs as u16);
            hdr.t_hoff = new_header_len as u8;
        }

        let has_nulls = (ttc.ttc_flags & TOAST_HAS_NULLS) != 0;
        let image = result_tuple.as_tuple_mut().t_data_mut() as *mut _ as *mut u8;
        // SAFETY: fresh owned image, values/isnull sized to the descriptor.
        unsafe {
            heap_fill_tuple(
                &tuple_desc,
                ttc.ttc_values,
                ttc.ttc_isnull,
                image.add(new_header_len),
                new_data_len,
                &mut result_tuple.as_tuple_mut().t_data_mut().t_infomask,
                has_nulls.then(|| image.add(SizeofHeapTupleHeader)),
            );
        }
        Some(result_tuple)
    } else {
        None
    };

    toast_tuple_cleanup(mcx, &mut ttc)?;

    Ok(result)
}

/// C `toast_flatten_tuple`: expand out-of-line fields (not compressed or
/// short-header ones).
pub fn toast_flatten_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    tup: &HeapTupleData<'_>,
    tuple_desc: &TupleDescData<'_>,
) -> PgResult<HeapTuple<'mcx>> {
    let num_attrs = tuple_desc.natts as usize;
    let (mut toast_values, toast_isnull) = deform(mcx, tup, tuple_desc);

    for i in 0..num_attrs {
        if !toast_isnull[i] && tuple_desc.compact_attrs[i].attlen == -1 {
            // SAFETY: non-null deformed varlena datum.
            let v = unsafe { va_slice(toast_values[i]) };
            if v[0] == 0x01 {
                let flat = detoast::detoast_external_attr(mcx, v)?;
                toast_values[i] = crate::helper::leak_datum(flat);
            }
        }
    }

    let mut new_tuple = heap_form_tuple(mcx, tuple_desc, &toast_values, &toast_isnull)?;

    new_tuple.as_tuple_mut().t_self = tup.t_self;
    new_tuple.as_tuple_mut().t_tableOid = tup.t_tableOid;
    {
        let old = tup.t_data();
        let hdr = new_tuple.as_tuple_mut().t_data_mut();
        hdr.t_choice = old.t_choice;
        hdr.t_ctid = old.t_ctid;
        hdr.t_infomask = (hdr.t_infomask & !HEAP_XACT_MASK) | (old.t_infomask & HEAP_XACT_MASK);
        hdr.t_infomask2 =
            (hdr.t_infomask2 & !HEAP2_XACT_MASK) | (old.t_infomask2 & HEAP2_XACT_MASK);
    }
    Ok(new_tuple)
}

/// C `toast_flatten_tuple_to_datum`: composite-type Datums must not carry
/// external TOAST pointers; inline them (decompressing compressed fields too)
/// and return the tuple as a Datum with the composite header set.
pub fn toast_flatten_tuple_to_datum<'mcx>(
    mcx: Mcx<'mcx>,
    tup: &HeapTupleData<'_>,
    tuple_desc: &TupleDescData<'_>,
) -> PgResult<Datum> {
    let num_attrs = tuple_desc.natts as usize;
    let (mut toast_values, toast_isnull) = deform(mcx, tup, tuple_desc);

    let mut has_nulls = false;
    for i in 0..num_attrs {
        if toast_isnull[i] {
            has_nulls = true;
        } else if tuple_desc.compact_attrs[i].attlen == -1 {
            // SAFETY: non-null deformed varlena datum.
            let v = unsafe { va_slice(toast_values[i]) };
            if crate::helper::is_external(v) || crate::helper::is_compressed(v) {
                let flat = detoast::detoast_attr(mcx, v)?;
                toast_values[i] = crate::helper::leak_datum(flat);
            }
        }
    }

    let mut new_header_len = SizeofHeapTupleHeader;
    if has_nulls {
        new_header_len += BITMAPLEN(num_attrs as i32) as usize;
    }
    let new_header_len = MAXALIGN(new_header_len);
    let new_data_len = heap_compute_data_size(tuple_desc, &toast_values, &toast_isnull);
    let new_tuple_len = new_header_len + new_data_len;

    let mut result = HeapTuple::alloc_zeroed(mcx, new_tuple_len)?;
    // SAFETY: tup header is SizeofHeapTupleHeader readable bytes.
    let old_header =
        unsafe { core::slice::from_raw_parts(tup.header_ptr(), SizeofHeapTupleHeader) };
    result.image_mut()[..SizeofHeapTupleHeader].copy_from_slice(old_header);
    {
        let hdr = result.as_tuple_mut().t_data_mut();
        hdr.set_natts(num_attrs as u16);
        hdr.t_hoff = new_header_len as u8;
        hdr.set_datum_length(new_tuple_len as u32);
        hdr.set_type_id(tuple_desc.tdtypeid);
        hdr.set_typmod(tuple_desc.tdtypmod);
    }

    let image = result.as_tuple_mut().t_data_mut() as *mut _ as *mut u8;
    // SAFETY: fresh owned image, values/isnull sized to the descriptor.
    unsafe {
        heap_fill_tuple(
            tuple_desc,
            &toast_values,
            &toast_isnull,
            image.add(new_header_len),
            new_data_len,
            &mut result.as_tuple_mut().t_data_mut().t_infomask,
            has_nulls.then(|| image.add(SizeofHeapTupleHeader)),
        );
    }

    let d = Datum::from_usize(result.image().as_ptr() as usize);
    core::mem::forget(result);
    Ok(d)
}

/// C `toast_build_flattened_tuple`: heap_form_tuple with external pointers
/// expanded first.
pub fn toast_build_flattened_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    tuple_desc: &TupleDescData<'_>,
    values: &[Datum],
    isnull: &[bool],
) -> PgResult<HeapTuple<'mcx>> {
    let num_attrs = tuple_desc.natts as usize;
    let mut new_values = ::mcx::vec_with_capacity_in(mcx, num_attrs)?;
    new_values.extend_from_slice(&values[..num_attrs]);

    for i in 0..num_attrs {
        if !isnull[i] && tuple_desc.compact_attrs[i].attlen == -1 {
            // SAFETY: non-null varlena datum per the caller's contract.
            let v = unsafe { va_slice(new_values[i]) };
            if v[0] == 0x01 {
                let flat = detoast::detoast_external_attr(mcx, v)?;
                new_values[i] = crate::helper::leak_datum(flat);
            }
        }
    }

    heap_form_tuple(mcx, tuple_desc, &new_values, isnull)
}
