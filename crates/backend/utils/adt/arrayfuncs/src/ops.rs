use alloc::boxed::Box;
use alloc::format;
use std::rc::Rc;

use ::datum::Datum;
use ::mcx::{vec_with_capacity_in, Mcx, PgVec};
use ::types_core::{InvalidOid, Oid, FLOAT8OID, RECORDOID};
use ::types_error::{
    PgError, PgResult, ERRCODE_ARRAY_SUBSCRIPT_ERROR, ERRCODE_DATATYPE_MISMATCH,
    ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_NULL_VALUE_NOT_ALLOWED, ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERRCODE_UNDEFINED_FUNCTION,
};
use ::types_fmgr::{byref_result, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, LocalFcinfo};

use crate::builtins::arg_array_bytes;
use crate::construct::{array_contains_nulls, construct_empty_array, construct_md_array, deconstruct_array};
use crate::foundation::{
    arr_data_offset, arr_elemtype, arr_ndim, arr_nullbitmap_off, arr_overhead_nonulls,
    arr_overhead_withnulls, array_cast_and_set, att_addlength_pointer, att_align_nominal,
    fetch_att, read_dims_lbounds, varsize_any, MAXDIM, MAX_ALLOC_SIZE,
};

// Element-by-element walk over a flat image (C array_iter, flat arm; expanded
// inputs are flattened at argument fetch — see arg_array_bytes).
pub(crate) struct FlatIter<'a> {
    img: &'a [u8],
    off: usize,
    bitmap_off: Option<usize>,
    idx: usize,
}

impl<'a> FlatIter<'a> {
    pub(crate) fn new(img: &'a [u8]) -> Self {
        FlatIter {
            img,
            off: arr_data_offset(img),
            bitmap_off: arr_nullbitmap_off(img),
            idx: 0,
        }
    }

    pub(crate) fn next(&mut self, typlen: i32, typbyval: bool, typalign: u8) -> (Datum, bool) {
        let i = self.idx;
        self.idx += 1;
        if let Some(bo) = self.bitmap_off {
            if self.img[bo + i / 8] & (1 << (i % 8)) == 0 {
                return (Datum::null(), true);
            }
        }
        let p = self.img[self.off..].as_ptr();
        let d = fetch_att(p, typbyval, typlen);
        self.off = att_addlength_pointer(self.off, typlen, p);
        self.off = att_align_nominal(self.off, typalign);
        (d, false)
    }
}

// pub (was pub(crate)) for proofs/typcache-inst: visibility-only, no behavior change.
#[derive(Clone, Copy)]
pub struct ElemMeta {
    pub typlen: i32,
    pub typbyval: bool,
    pub typalign: u8,
}

// fn_extra memo: typcache entry (+ the hash_array RECORD fake finfo, which C
// builds outside the type cache).
struct TcMemo {
    entry: Rc<::typcache::TypeCacheEntry>,
    record_hash: Option<FmgrInfo>,
}

impl TcMemo {
    fn meta(&self) -> ElemMeta {
        ElemMeta {
            typlen: self.entry.typlen() as i32,
            typbyval: self.entry.typbyval(),
            typalign: self.entry.typalign() as u8,
        }
    }
}

enum TcWant {
    Eq,
    Cmp,
    Hash,
    HashExtended,
}

const F_HASH_RECORD: Oid = 6192;

fn cached_typentry<'f>(
    flinfo: &'f mut FmgrInfo,
    element_type: Oid,
    want: TcWant,
) -> PgResult<&'f mut TcMemo> {
    let need = match flinfo.fn_extra_ref::<TcMemo>() {
        Some(m) => m.entry.type_id != element_type,
        None => true,
    };
    if need {
        flinfo.set_fn_extra(fresh_typentry(element_type, want)?);
    }
    Ok(flinfo.fn_extra_mut::<TcMemo>().unwrap())
}

fn fresh_typentry(element_type: Oid, want: TcWant) -> PgResult<TcMemo> {
    let flags = match want {
        TcWant::Eq => ::typcache::TYPECACHE_EQ_OPR_FINFO,
        TcWant::Cmp => ::typcache::TYPECACHE_CMP_PROC_FINFO,
        TcWant::Hash => ::typcache::TYPECACHE_HASH_PROC_FINFO,
        TcWant::HashExtended => ::typcache::TYPECACHE_HASH_EXTENDED_PROC_FINFO,
    };
    let entry = ::typcache::lookup_type_cache(element_type, flags)?;
    let (valid, what) = match want {
        TcWant::Eq => (entry.eq_opr_finfo().fn_oid != InvalidOid, "an equality operator"),
        TcWant::Cmp => (
            entry.cmp_proc_finfo().fn_oid != InvalidOid,
            "a comparison function",
        ),
        TcWant::Hash => (entry.hash_proc_finfo().fn_oid != InvalidOid, "a hash function"),
        TcWant::HashExtended => (
            entry.hash_extended_proc_finfo().fn_oid != InvalidOid,
            "an extended hash function",
        ),
    };
    let mut record_hash = None;
    if !valid {
        if matches!(want, TcWant::Hash) && element_type == RECORDOID {
            record_hash = Some(::fmgr_seams::fmgr_info::call(F_HASH_RECORD)?);
        } else {
            return Err(Box::new(
                PgError::error(format!(
                    "could not identify {what} for type {}",
                    ::format_type::format_type_be(element_type)?
                ))
                .with_sqlstate(ERRCODE_UNDEFINED_FUNCTION),
            ));
        }
    }
    Ok(TcMemo { entry, record_hash })
}

fn elem_type_mismatch() -> Box<PgError> {
    Box::new(
        PgError::error("cannot compare arrays of different element types")
            .with_sqlstate(ERRCODE_DATATYPE_MISMATCH),
    )
}

// Loop of C array_eq after the dims fast path (which the caller runs first,
// matching C's check order relative to the typcache lookup).
// pub (was pub(crate)) for proofs/typcache-inst: visibility-only, no behavior change.
pub fn array_eq_loop(
    mcx: Mcx<'_>,
    array1: &[u8],
    array2: &[u8],
    collation: Oid,
    meta: ElemMeta,
    eqfn: &mut FmgrInfo,
) -> PgResult<bool> {
    let (ndims1, dims1, _lbs) = read_dims_lbounds(array1);
    let nitems = ::arrayutils::array_get_n_items(ndims1, &dims1)?;
    let mut it1 = FlatIter::new(array1);
    let mut it2 = FlatIter::new(array2);
    let mut lfc = LocalFcinfo::<2>::fresh(collation);
    // SAFETY: mcx outlives every invoke through this stack frame.
    unsafe { lfc.set_result_mcx(mcx) };

    for _ in 0..nitems {
        let (elt1, isnull1) = it1.next(meta.typlen, meta.typbyval, meta.typalign);
        let (elt2, isnull2) = it2.next(meta.typlen, meta.typbyval, meta.typalign);
        if isnull1 && isnull2 {
            continue;
        }
        if isnull1 || isnull2 {
            return Ok(false);
        }
        lfc.rearm(collation);
        lfc.set_arg(0, elt1);
        lfc.set_arg(1, elt2);
        let r = eqfn.invoke(&mut lfc)?;
        if lfc.isnull || !r.as_bool() {
            return Ok(false);
        }
    }
    Ok(true)
}

pub fn array_eq_internal(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<bool> {
    let mcx = fcinfo.result_mcx();
    let array1 = arg_array_bytes(fcinfo, 0, mcx)?;
    let array2 = arg_array_bytes(fcinfo, 1, mcx)?;
    let collation = fcinfo.get_collation();
    let (ndims1, dims1, lbs1) = read_dims_lbounds(&array1);
    let (ndims2, dims2, lbs2) = read_dims_lbounds(&array2);
    let element_type = arr_elemtype(&array1);

    if element_type != arr_elemtype(&array2) {
        return Err(elem_type_mismatch());
    }
    let n = ndims1.max(0) as usize;
    if ndims1 != ndims2 || dims1[..n] != dims2[..n] || lbs1[..n] != lbs2[..n] {
        return Ok(false);
    }

    let flinfo = flinfo.expect("array_eq: NULL flinfo");
    let memo = cached_typentry(flinfo, element_type, TcWant::Eq)?;
    let meta = memo.meta();
    let mut eqfn = memo.entry.eq_opr_finfo();
    array_eq_loop(mcx, &array1, &array2, collation, meta, &mut eqfn)
}

// pub (was pub(crate)) for proofs/typcache-inst: visibility-only, no behavior change.
pub fn array_cmp_core(
    mcx: Mcx<'_>,
    array1: &[u8],
    array2: &[u8],
    collation: Oid,
    meta: ElemMeta,
    cmpfn: &mut FmgrInfo,
) -> PgResult<i32> {
    let (ndims1, dims1, lbs1) = read_dims_lbounds(array1);
    let (ndims2, dims2, lbs2) = read_dims_lbounds(array2);
    let nitems1 = ::arrayutils::array_get_n_items(ndims1, &dims1)?;
    let nitems2 = ::arrayutils::array_get_n_items(ndims2, &dims2)?;

    let min_nitems = nitems1.min(nitems2);
    let mut it1 = FlatIter::new(array1);
    let mut it2 = FlatIter::new(array2);
    let mut lfc = LocalFcinfo::<2>::fresh(collation);
    // SAFETY: mcx outlives every invoke through this stack frame.
    unsafe { lfc.set_result_mcx(mcx) };

    let mut result = 0i32;
    for _ in 0..min_nitems {
        let (elt1, isnull1) = it1.next(meta.typlen, meta.typbyval, meta.typalign);
        let (elt2, isnull2) = it2.next(meta.typlen, meta.typbyval, meta.typalign);
        if isnull1 && isnull2 {
            continue;
        }
        if isnull1 {
            result = 1;
            break;
        }
        if isnull2 {
            result = -1;
            break;
        }
        lfc.rearm(collation);
        lfc.set_arg(0, elt1);
        lfc.set_arg(1, elt2);
        let cmpresult = cmpfn.invoke(&mut lfc)?.as_i32();
        if cmpresult == 0 {
            continue;
        }
        result = if cmpresult < 0 { -1 } else { 1 };
        break;
    }

    if result == 0 {
        if nitems1 != nitems2 {
            result = if nitems1 < nitems2 { -1 } else { 1 };
        } else if ndims1 != ndims2 {
            result = if ndims1 < ndims2 { -1 } else { 1 };
        } else {
            for i in 0..ndims1 as usize {
                if dims1[i] != dims2[i] {
                    result = if dims1[i] < dims2[i] { -1 } else { 1 };
                    break;
                }
            }
            if result == 0 {
                for i in 0..ndims1 as usize {
                    if lbs1[i] != lbs2[i] {
                        result = if lbs1[i] < lbs2[i] { -1 } else { 1 };
                        break;
                    }
                }
            }
        }
    }
    Ok(result)
}

/// C array_cmp: -1/0/1; exported for array_larger/array_smaller.
pub fn array_cmp(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<i32> {
    let mcx = fcinfo.result_mcx();
    let array1 = arg_array_bytes(fcinfo, 0, mcx)?;
    let array2 = arg_array_bytes(fcinfo, 1, mcx)?;
    let collation = fcinfo.get_collation();
    let element_type = arr_elemtype(&array1);
    if element_type != arr_elemtype(&array2) {
        return Err(elem_type_mismatch());
    }
    // flinfo-less callers exist (tuplesort's comparison shim carries no
    // FmgrInfo); the per-call typcache probe replaces the fn_extra memo there.
    let shim_memo;
    let memo = match flinfo {
        Some(f) => &*cached_typentry(f, element_type, TcWant::Cmp)?,
        None => {
            shim_memo = fresh_typentry(element_type, TcWant::Cmp)?;
            &shim_memo
        }
    };
    let meta = memo.meta();
    let mut cmpfn = memo.entry.cmp_proc_finfo();
    array_cmp_core(mcx, &array1, &array2, collation, meta, &mut cmpfn)
}

pub fn fc_array_eq(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_bool(array_eq_internal(flinfo, fcinfo)?))
}

pub fn fc_array_ne(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_bool(!array_eq_internal(flinfo, fcinfo)?))
}

pub fn fc_array_lt(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_bool(array_cmp(flinfo, fcinfo)? < 0))
}

pub fn fc_array_gt(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_bool(array_cmp(flinfo, fcinfo)? > 0))
}

pub fn fc_array_le(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_bool(array_cmp(flinfo, fcinfo)? <= 0))
}

pub fn fc_array_ge(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_bool(array_cmp(flinfo, fcinfo)? >= 0))
}

pub fn fc_btarraycmp(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_i32(array_cmp(flinfo, fcinfo)?))
}

pub(crate) fn hash_array_core(
    mcx: Mcx<'_>,
    array: &[u8],
    collation: Oid,
    meta: ElemMeta,
    hashfn: &mut FmgrInfo,
    seed: Option<Datum>,
) -> PgResult<u64> {
    let (ndims, dims, _lbs) = read_dims_lbounds(array);
    let nitems = ::arrayutils::array_get_n_items(ndims, &dims)?;
    let mut iter = FlatIter::new(array);
    let mut lfc = LocalFcinfo::<2>::fresh(collation);
    // SAFETY: mcx outlives every invoke through this stack frame.
    unsafe { lfc.set_result_mcx(mcx) };

    let mut result: u64 = 1;
    let mask: u64 = if seed.is_some() { u64::MAX } else { u32::MAX as u64 };
    for _ in 0..nitems {
        let (elt, isnull) = iter.next(meta.typlen, meta.typbyval, meta.typalign);
        let elthash = if isnull {
            0
        } else {
            lfc.rearm(collation);
            lfc.set_arg(0, elt);
            if let Some(s) = seed {
                lfc.set_arg(1, s);
            }
            hashfn.invoke(&mut lfc)?.as_u64() & mask
        };
        result = ((result << 5).wrapping_sub(result).wrapping_add(elthash)) & mask;
    }
    Ok(result)
}

pub fn fc_hash_array(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let array = arg_array_bytes(fcinfo, 0, mcx)?;
    let collation = fcinfo.get_collation();
    let element_type = arr_elemtype(&array);
    let flinfo = flinfo.expect("hash_array: NULL flinfo");
    let memo = cached_typentry(flinfo, element_type, TcWant::Hash)?;
    let meta = memo.meta();
    let h = match &mut memo.record_hash {
        Some(f) => hash_array_core(mcx, &array, collation, meta, f, None)?,
        None => {
            let mut f = memo.entry.hash_proc_finfo();
            hash_array_core(mcx, &array, collation, meta, &mut f, None)?
        }
    };
    Ok(Datum::from_u64(h))
}

pub fn fc_hash_array_extended(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let array = arg_array_bytes(fcinfo, 0, mcx)?;
    let seed = fcinfo.arg(1);
    let collation = fcinfo.get_collation();
    let element_type = arr_elemtype(&array);
    let flinfo = flinfo.expect("hash_array_extended: NULL flinfo");
    let memo = cached_typentry(flinfo, element_type, TcWant::HashExtended)?;
    let meta = memo.meta();
    let mut hashfn = memo.entry.hash_extended_proc_finfo();
    let h = hash_array_core(mcx, &array, collation, meta, &mut hashfn, Some(seed))?;
    Ok(Datum::from_u64(h))
}

// array_contain_compare: matchall=true → all of array1 in array2;
// matchall=false → any of array1 in array2.
pub(crate) fn contain_core(
    mcx: Mcx<'_>,
    array1: &[u8],
    array2: &[u8],
    collation: Oid,
    matchall: bool,
    meta: ElemMeta,
    eqfn: &mut FmgrInfo,
) -> PgResult<bool> {
    let (values2, nulls2) =
        deconstruct_array(mcx, array2, meta.typlen, meta.typbyval, meta.typalign, true)?;
    let (ndims1, dims1, _lbs1) = read_dims_lbounds(array1);
    let nelems1 = ::arrayutils::array_get_n_items(ndims1, &dims1)?;
    let mut it1 = FlatIter::new(array1);
    let mut lfc = LocalFcinfo::<2>::fresh(collation);
    // SAFETY: mcx outlives every invoke through this stack frame.
    unsafe { lfc.set_result_mcx(mcx) };

    let mut result = matchall;
    for _ in 0..nelems1 {
        let (elt1, isnull1) = it1.next(meta.typlen, meta.typbyval, meta.typalign);
        if isnull1 {
            if matchall {
                result = false;
                break;
            }
            continue;
        }
        let mut found = false;
        for j in 0..values2.len() {
            if nulls2[j] {
                continue;
            }
            lfc.rearm(collation);
            lfc.set_arg(0, elt1);
            lfc.set_arg(1, values2[j]);
            let r = eqfn.invoke(&mut lfc)?;
            if !lfc.isnull && r.as_bool() {
                found = true;
                break;
            }
        }
        if found {
            if !matchall {
                result = true;
                break;
            }
        } else if matchall {
            result = false;
            break;
        }
    }
    Ok(result)
}

fn contain_common(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
    swap: bool,
    matchall: bool,
) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let a = arg_array_bytes(fcinfo, 0, mcx)?;
    let b = arg_array_bytes(fcinfo, 1, mcx)?;
    let (array1, array2) = if swap { (&b, &a) } else { (&a, &b) };
    let collation = fcinfo.get_collation();
    let element_type = arr_elemtype(array1);
    if element_type != arr_elemtype(array2) {
        return Err(elem_type_mismatch());
    }
    let flinfo = flinfo.expect("array_contain_compare: NULL flinfo");
    let memo = cached_typentry(flinfo, element_type, TcWant::Eq)?;
    let meta = memo.meta();
    let mut eqfn = memo.entry.eq_opr_finfo();
    let r = contain_core(mcx, array1, array2, collation, matchall, meta, &mut eqfn)?;
    Ok(Datum::from_bool(r))
}

pub fn fc_arrayoverlap(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    contain_common(flinfo, fcinfo, false, false)
}

pub fn fc_arraycontains(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    contain_common(flinfo, fcinfo, true, true)
}

pub fn fc_arraycontained(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    contain_common(flinfo, fcinfo, false, true)
}

// PG_DETOAST_DATUM for an element datum; the returned image (if any) must
// outlive the datum's use.
fn detoast_elem<'m>(mcx: Mcx<'m>, d: Datum) -> PgResult<(Datum, Option<PgVec<'m, u8>>)> {
    let p = d.as_usize() as *const u8;
    // SAFETY: by-ref varlena datum points at a live image.
    unsafe {
        if (*p & 0x03) != 0 {
            let raw = core::slice::from_raw_parts(p, varsize_any(p));
            let img = ::detoast_seams::detoast_attr::call(mcx, raw)?;
            let nd = Datum::from_usize(img.as_ptr() as usize);
            Ok((nd, Some(img)))
        } else {
            Ok((d, None))
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn replace_core<'m>(
    mcx: Mcx<'m>,
    array: PgVec<'m, u8>,
    mut search: Datum,
    search_isnull: bool,
    mut replace: Datum,
    replace_isnull: bool,
    remove: bool,
    collation: Oid,
    meta: ElemMeta,
    eqfn: &mut FmgrInfo,
) -> PgResult<PgVec<'m, u8>> {
    let element_type = arr_elemtype(&array);
    let (ndim, mut dims, lbs) = read_dims_lbounds(&array);
    let nitems = ::arrayutils::array_get_n_items(ndim, &dims)?;

    let mut _search_img = None;
    let mut _replace_img = None;
    if meta.typlen == -1 {
        if !search_isnull {
            let (d, img) = detoast_elem(mcx, search)?;
            search = d;
            _search_img = img;
        }
        if !replace_isnull {
            let (d, img) = detoast_elem(mcx, replace)?;
            replace = d;
            _replace_img = img;
        }
    }

    let mut lfc = LocalFcinfo::<2>::fresh(collation);
    // SAFETY: mcx outlives every invoke through this stack frame.
    unsafe { lfc.set_result_mcx(mcx) };

    let mut values: PgVec<'m, Datum> = vec_with_capacity_in(mcx, nitems as usize)?;
    let mut nulls: PgVec<'m, bool> = vec_with_capacity_in(mcx, nitems as usize)?;
    let mut iter = FlatIter::new(&array);
    let mut changed = false;

    for _ in 0..nitems {
        let (elt, elt_isnull) = iter.next(meta.typlen, meta.typbyval, meta.typalign);
        let mut skip = false;
        let mut out = elt;
        let mut out_isnull = elt_isnull;
        if elt_isnull {
            if search_isnull {
                if remove {
                    skip = true;
                    changed = true;
                } else if !replace_isnull {
                    out = replace;
                    out_isnull = false;
                    changed = true;
                }
            }
        } else if !search_isnull {
            lfc.rearm(collation);
            lfc.set_arg(0, elt);
            lfc.set_arg(1, search);
            let r = eqfn.invoke(&mut lfc)?;
            if !lfc.isnull && r.as_bool() {
                changed = true;
                if remove {
                    skip = true;
                } else {
                    out = replace;
                    out_isnull = replace_isnull;
                }
            }
        }
        if !skip {
            values.push(out);
            nulls.push(out_isnull);
        }
    }

    if !changed {
        drop(iter);
        return Ok(array);
    }

    let nresult = values.len();
    if nresult == 0 {
        return construct_empty_array(mcx, element_type);
    }

    if remove {
        dims[0] = nresult as i32;
    }
    construct_md_array(
        mcx,
        values.as_slice(),
        Some(nulls.as_slice()),
        ndim,
        &dims[..ndim as usize],
        &lbs[..ndim as usize],
        element_type,
        meta.typlen,
        meta.typbyval,
        meta.typalign,
    )
}

fn replace_common(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
    remove: bool,
) -> PgResult<Datum> {
    if fcinfo.argisnull(0) {
        return Ok(fcinfo.return_null());
    }
    let mcx = fcinfo.result_mcx();
    let array = arg_array_bytes(fcinfo, 0, mcx)?;
    let search = fcinfo.arg(1);
    let search_isnull = fcinfo.argisnull(1);
    let (replace, replace_isnull) = if remove {
        (Datum::null(), true)
    } else {
        (fcinfo.arg(2), fcinfo.argisnull(2))
    };
    let collation = fcinfo.get_collation();

    let element_type = arr_elemtype(&array);
    let (ndim, dims, _lbs) = read_dims_lbounds(&array);
    let nitems = ::arrayutils::array_get_n_items(ndim, &dims)?;
    if nitems <= 0 {
        return byref_result(mcx, &array);
    }
    if remove && ndim > 1 {
        return Err(Box::new(
            PgError::error("removing elements from multidimensional arrays is not supported")
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
        ));
    }

    let flinfo = flinfo.expect("array_replace_internal: NULL flinfo");
    let memo = cached_typentry(flinfo, element_type, TcWant::Eq)?;
    let meta = memo.meta();
    let mut eqfn = memo.entry.eq_opr_finfo();
    let out = replace_core(
        mcx,
        array,
        search,
        search_isnull,
        replace,
        replace_isnull,
        remove,
        collation,
        meta,
        &mut eqfn,
    )?;
    byref_result(mcx, &out)
}

pub fn fc_array_remove(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    replace_common(flinfo, fcinfo, true)
}

pub fn fc_array_replace(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    replace_common(flinfo, fcinfo, false)
}

pub fn fc_array_ndims(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let ndim = {
        let mcx = fcinfo.result_mcx();
        let array = arg_array_bytes(fcinfo, 0, mcx)?;
        arr_ndim(&array)
    };
    if ndim <= 0 || ndim > MAXDIM as i32 {
        return Ok(fcinfo.return_null());
    }
    Ok(Datum::from_i32(ndim))
}

pub(crate) fn dims_text(ndim: i32, dims: &[i32], lbs: &[i32]) -> alloc::string::String {
    use core::fmt::Write;
    let mut s = alloc::string::String::new();
    for i in 0..ndim as usize {
        write!(s, "[{}:{}]", lbs[i], dims[i] + lbs[i] - 1).unwrap();
    }
    s
}

pub fn fc_array_dims(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let (ndim, dims, lbs) = {
        let mcx = fcinfo.result_mcx();
        let array = arg_array_bytes(fcinfo, 0, mcx)?;
        read_dims_lbounds(&array)
    };
    if ndim <= 0 || ndim > MAXDIM as i32 {
        return Ok(fcinfo.return_null());
    }
    text_result(fcinfo.result_mcx(), dims_text(ndim, &dims, &lbs).as_bytes())
}

pub fn fc_array_lower(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let (ndim, _dims, lbs) = {
        let mcx = fcinfo.result_mcx();
        let array = arg_array_bytes(fcinfo, 0, mcx)?;
        read_dims_lbounds(&array)
    };
    let reqdim = fcinfo.arg(1).as_i32();
    if ndim <= 0 || ndim > MAXDIM as i32 || reqdim <= 0 || reqdim > ndim {
        return Ok(fcinfo.return_null());
    }
    Ok(Datum::from_i32(lbs[(reqdim - 1) as usize]))
}

pub fn fc_array_upper(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let (ndim, dims, lbs) = {
        let mcx = fcinfo.result_mcx();
        let array = arg_array_bytes(fcinfo, 0, mcx)?;
        read_dims_lbounds(&array)
    };
    let reqdim = fcinfo.arg(1).as_i32();
    if ndim <= 0 || ndim > MAXDIM as i32 || reqdim <= 0 || reqdim > ndim {
        return Ok(fcinfo.return_null());
    }
    let i = (reqdim - 1) as usize;
    Ok(Datum::from_i32(dims[i] + lbs[i] - 1))
}

pub fn fc_array_cardinality(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let array = arg_array_bytes(fcinfo, 0, mcx)?;
    let (ndim, dims, _lbs) = read_dims_lbounds(&array);
    Ok(Datum::from_i32(::arrayutils::array_get_n_items(ndim, &dims)?))
}

pub(crate) fn text_result<'m>(mcx: Mcx<'m>, bytes: &[u8]) -> PgResult<Datum> {
    let total = 4 + bytes.len();
    let mut img: PgVec<'m, u8> = vec_with_capacity_in(mcx, total)?;
    ::mcx::vec_append_bytes(&mut img, &::datum::varlena::set_varsize_4b(total))?;
    ::mcx::vec_append_bytes(&mut img, bytes)?;
    byref_result(mcx, &img)
}

struct GenerateSubscriptsFctx {
    lower: i32,
    upper: i32,
    reverse: bool,
}

pub fn fc_generate_subscripts(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("generate_subscripts: NULL flinfo");
    if !flinfo.has_fn_extra() {
        let (ndim, dims, lbs) = {
            let mcx = fcinfo.result_mcx();
            let array = arg_array_bytes(fcinfo, 0, mcx)?;
            read_dims_lbounds(&array)
        };
        let reqdim = fcinfo.arg(1).as_i32();
        let state = if ndim <= 0 || ndim > MAXDIM as i32 || reqdim <= 0 || reqdim > ndim {
            GenerateSubscriptsFctx { lower: 1, upper: 0, reverse: false }
        } else {
            let i = (reqdim - 1) as usize;
            GenerateSubscriptsFctx {
                lower: lbs[i],
                upper: dims[i] + lbs[i] - 1,
                reverse: if fcinfo.nargs() < 3 { false } else { fcinfo.arg(2).as_bool() },
            }
        };
        let fctx = ::funcapi_srf::init_MultiFuncCall(flinfo, fcinfo)?;
        fctx.user_fctx = Some(Box::new(state));
    }
    let fctx = ::funcapi_srf::per_MultiFuncCall(flinfo)
        .user_fctx
        .as_mut()
        .expect("generate_subscripts: user_fctx set at first call")
        .downcast_mut::<GenerateSubscriptsFctx>()
        .expect("generate_subscripts: user_fctx is GenerateSubscriptsFctx");
    if fctx.lower <= fctx.upper {
        let v = if !fctx.reverse {
            let v = fctx.lower;
            fctx.lower += 1;
            v
        } else {
            let v = fctx.upper;
            fctx.upper -= 1;
            v
        };
        Ok(::funcapi_srf::srf_return_next(flinfo, fcinfo, Datum::from_i32(v)))
    } else {
        Ok(::funcapi_srf::srf_return_done(flinfo, fcinfo))
    }
}

fn null_dim_bound_error() -> Box<PgError> {
    Box::new(
        PgError::error("dimension array or low bound array cannot be null")
            .with_sqlstate(ERRCODE_NULL_VALUE_NOT_ALLOWED),
    )
}

// Reads the int4[] dims/lbs argument per array_fill_internal's checks;
// returns C's raw count (ARR_DIMS(a)[0], before validation) plus the values.
fn fill_param_ints<'m>(mcx: Mcx<'m>, img: &[u8]) -> PgResult<(i32, PgVec<'m, i32>)> {
    let ndim = arr_ndim(img);
    if ndim > 1 {
        return Err(Box::new(
            PgError::error("wrong number of array subscripts")
                .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR)
                .with_detail("Dimension array must be one dimensional."),
        ));
    }
    if crate::construct::array_contains_nulls(img) {
        return Err(Box::new(
            PgError::error("dimension values cannot be null")
                .with_sqlstate(ERRCODE_NULL_VALUE_NOT_ALLOWED),
        ));
    }
    let n = if ndim > 0 { crate::foundation::arr_dim(img, 0) } else { 0 };
    let mut out: PgVec<'m, i32> = vec_with_capacity_in(mcx, n.max(0) as usize)?;
    let base = arr_data_offset(img);
    for i in 0..n.max(0) as usize {
        out.push(i32::from_ne_bytes(img[base + 4 * i..base + 4 * i + 4].try_into().unwrap()));
    }
    Ok((n, out))
}

pub(crate) fn array_fill_core<'m>(
    mcx: Mcx<'m>,
    dims_img: &[u8],
    lbs_img: Option<&[u8]>,
    mut value: Datum,
    isnull: bool,
    elmtype: Oid,
    meta: ElemMeta,
) -> PgResult<PgVec<'m, u8>> {
    let (ndims, dimv) = fill_param_ints(mcx, dims_img)?;

    if ndims < 0 {
        return Err(Box::new(
            PgError::error(format!("invalid number of dimensions: {ndims}"))
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
        ));
    }
    if ndims > MAXDIM as i32 {
        return Err(Box::new(
            PgError::error(format!(
                "number of array dimensions ({ndims}) exceeds the maximum allowed ({MAXDIM})"
            ))
            .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
        ));
    }

    let deflbs = [1i32; MAXDIM];
    let lbsv_store;
    let lbsv: &[i32] = match lbs_img {
        Some(img) => {
            let (nlbs, store) = fill_param_ints(mcx, img)?;
            if ndims != nlbs {
                return Err(Box::new(
                    PgError::error("wrong number of array subscripts")
                        .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR)
                        .with_detail("Low bound array has different size than dimensions array."),
                ));
            }
            lbsv_store = store;
            lbsv_store.as_slice()
        }
        None => &deflbs[..ndims as usize],
    };

    let nitems = ::arrayutils::array_get_n_items(ndims, &dimv)?;
    ::arrayutils::array_check_bounds(ndims, &dimv, lbsv)?;

    if nitems <= 0 {
        return construct_empty_array(mcx, elmtype);
    }

    let elmlen = meta.typlen;
    let elmbyval = meta.typbyval;
    let elmalign = meta.typalign;

    if !isnull {
        let mut _value_img = None;
        if elmlen == -1 {
            let (d, img) = detoast_elem(mcx, value)?;
            value = d;
            _value_img = img;
        }
        let nbytes = if elmlen > 0 {
            elmlen as usize
        } else {
            att_addlength_pointer(0, elmlen, value.as_usize() as *const u8)
        };
        let nbytes = att_align_nominal(nbytes, elmalign);
        debug_assert!(nbytes > 0);

        let totbytes = nbytes as i64 * nitems as i64;
        if totbytes > MAX_ALLOC_SIZE as i64 {
            return Err(Box::new(
                PgError::error(format!(
                    "array size exceeds the maximum allowed ({MAX_ALLOC_SIZE})"
                ))
                .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
            ));
        }
        let totbytes = totbytes as usize + arr_overhead_nonulls(ndims);

        let mut img: PgVec<'m, u8> = vec_with_capacity_in(mcx, totbytes)?;
        img.resize(totbytes, 0);
        write_envelope(&mut img, totbytes, ndims, 0, elmtype, &dimv, lbsv);
        let mut off = arr_data_offset(&img);
        for _ in 0..nitems {
            off += array_cast_and_set(value, elmlen, elmbyval, elmalign, &mut img[off..]);
        }
        Ok(img)
    } else {
        let dataoffset = arr_overhead_withnulls(ndims, nitems);
        let mut img: PgVec<'m, u8> = vec_with_capacity_in(mcx, dataoffset)?;
        img.resize(dataoffset, 0);
        write_envelope(&mut img, dataoffset, ndims, dataoffset as i32, elmtype, &dimv, lbsv);
        Ok(img)
    }
}

fn write_envelope(
    out: &mut [u8],
    nbytes: usize,
    ndims: i32,
    dataoffset: i32,
    elmtype: Oid,
    dimv: &[i32],
    lbsv: &[i32],
) {
    out[0..4].copy_from_slice(&::datum::varlena::set_varsize_4b(nbytes));
    out[4..8].copy_from_slice(&ndims.to_ne_bytes());
    out[8..12].copy_from_slice(&dataoffset.to_ne_bytes());
    out[12..16].copy_from_slice(&(elmtype as u32).to_ne_bytes());
    let mut off = 16usize;
    for d in dimv.iter().take(ndims as usize) {
        out[off..off + 4].copy_from_slice(&d.to_ne_bytes());
        off += 4;
    }
    for l in lbsv.iter().take(ndims as usize) {
        out[off..off + 4].copy_from_slice(&l.to_ne_bytes());
        off += 4;
    }
}

fn fill_common(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
    with_lbs: bool,
) -> PgResult<Datum> {
    if fcinfo.argisnull(1) || (with_lbs && fcinfo.argisnull(2)) {
        return Err(null_dim_bound_error());
    }
    let mcx = fcinfo.result_mcx();
    let dims = arg_array_bytes(fcinfo, 1, mcx)?;
    let lbs = if with_lbs { Some(arg_array_bytes(fcinfo, 2, mcx)?) } else { None };
    let (value, isnull) = if !fcinfo.argisnull(0) {
        (fcinfo.arg(0), false)
    } else {
        (Datum::null(), true)
    };
    let flinfo = flinfo.expect("array_fill: NULL flinfo");
    let elmtype = ::fmgr_seams::get_fn_expr_argtype::call(flinfo, 0);
    if elmtype == InvalidOid {
        return Err(Box::new(PgError::error("could not determine data type of input")));
    }

    let need = match flinfo.fn_extra_ref::<crate::expanded::ArrayMetaState>() {
        Some(m) => m.element_type != elmtype,
        None => true,
    };
    if need {
        let (typlen, typbyval, typalign) = ::lsyscache::get_typlenbyvalalign(elmtype)?;
        flinfo.set_fn_extra(crate::expanded::ArrayMetaState {
            element_type: elmtype,
            typlen,
            typbyval,
            typalign: typalign as u8,
        });
    }
    let m = flinfo.fn_extra_ref::<crate::expanded::ArrayMetaState>().unwrap();
    let meta = ElemMeta { typlen: m.typlen as i32, typbyval: m.typbyval, typalign: m.typalign };

    let out = array_fill_core(mcx, &dims, lbs.as_deref(), value, isnull, elmtype, meta)?;
    byref_result(mcx, &out)
}

pub fn fc_array_fill(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    fill_common(flinfo, fcinfo, false)
}

pub fn fc_array_fill_with_lower_bounds(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    fill_common(flinfo, fcinfo, true)
}

pub(crate) fn width_bucket_array_float8(operand: Datum, thresholds: &[u8], nitems: i32) -> i32 {
    let op = operand.as_f64();
    let mut left = 0i32;
    let mut right = nitems;
    let off = arr_data_offset(thresholds);
    let elt = |i: i32| -> f64 {
        let p = thresholds[off + i as usize * 8..off + i as usize * 8 + 8]
            .try_into()
            .unwrap();
        f64::from_ne_bytes(p)
    };

    // NaN sorts greater than every threshold (including other NaNs), so it
    // never needs a search.
    if op.is_nan() {
        return right;
    }
    while left < right {
        let mid = (left + right) / 2;
        let t = elt(mid);
        if t.is_nan() || op < t {
            right = mid;
        } else {
            left = mid + 1;
        }
    }
    left
}

pub(crate) fn width_bucket_array_fixed(
    mcx: Mcx<'_>,
    operand: Datum,
    thresholds: &[u8],
    collation: Oid,
    meta: ElemMeta,
    cmpfn: &mut FmgrInfo,
    nitems: i32,
) -> PgResult<i32> {
    let off = arr_data_offset(thresholds);
    let typlen = meta.typlen as usize;
    let mut lfc = LocalFcinfo::<2>::fresh(collation);
    // SAFETY: mcx outlives every invoke through this stack frame.
    unsafe { lfc.set_result_mcx(mcx) };

    let mut left = 0i32;
    let mut right = nitems;
    while left < right {
        let mid = (left + right) / 2;
        let p = thresholds[off + mid as usize * typlen..].as_ptr();
        lfc.rearm(collation);
        lfc.set_arg(0, operand);
        lfc.set_arg(1, fetch_att(p, meta.typbyval, meta.typlen));
        let cmpresult = cmpfn.invoke(&mut lfc)?.as_i32();
        if cmpresult < 0 {
            right = mid;
        } else {
            left = mid + 1;
        }
    }
    Ok(left)
}

pub(crate) fn width_bucket_array_variable(
    mcx: Mcx<'_>,
    operand: Datum,
    thresholds: &[u8],
    collation: Oid,
    meta: ElemMeta,
    cmpfn: &mut FmgrInfo,
    nitems: i32,
) -> PgResult<i32> {
    let mut lfc = LocalFcinfo::<2>::fresh(collation);
    // SAFETY: mcx outlives every invoke through this stack frame.
    unsafe { lfc.set_result_mcx(mcx) };
    let mut left = 0i32;
    let mut right = nitems;
    let mut thresholds_off = arr_data_offset(thresholds);

    while left < right {
        let mid = (left + right) / 2;
        // Variable-width elements aren't randomly addressable; walk from the
        // last-known `left` offset instead of the array start (keeps the
        // search O(N) total, not O(N log N)).
        let mut off = thresholds_off;
        let mut p = thresholds[off..].as_ptr();
        for _ in left..mid {
            off = att_addlength_pointer(off, meta.typlen, p);
            off = att_align_nominal(off, meta.typalign);
            p = thresholds[off..].as_ptr();
        }

        lfc.rearm(collation);
        lfc.set_arg(0, operand);
        lfc.set_arg(1, fetch_att(p, meta.typbyval, meta.typlen));
        let cmpresult = cmpfn.invoke(&mut lfc)?.as_i32();
        if cmpresult < 0 {
            right = mid;
        } else {
            left = mid + 1;
            off = att_addlength_pointer(off, meta.typlen, p);
            thresholds_off = att_align_nominal(off, meta.typalign);
        }
    }
    Ok(left)
}

pub fn fc_width_bucket_array(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let operand = fcinfo.arg(0);
    let thresholds = arg_array_bytes(fcinfo, 1, mcx)?;
    let collation = fcinfo.get_collation();
    let element_type = arr_elemtype(&thresholds);

    if arr_ndim(&thresholds) > 1 {
        return Err(Box::new(
            PgError::error("thresholds must be one-dimensional array")
                .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
        ));
    }
    if array_contains_nulls(&thresholds) {
        return Err(Box::new(
            PgError::error("thresholds array must not contain NULLs")
                .with_sqlstate(ERRCODE_NULL_VALUE_NOT_ALLOWED),
        ));
    }

    let (ndim, dims, _lbs) = read_dims_lbounds(&thresholds);
    let nitems = ::arrayutils::array_get_n_items(ndim, &dims)?;

    let result = if element_type == FLOAT8OID {
        width_bucket_array_float8(operand, &thresholds, nitems)
    } else {
        let flinfo = flinfo.expect("width_bucket_array: NULL flinfo");
        let memo = cached_typentry(flinfo, element_type, TcWant::Cmp)?;
        let meta = memo.meta();
        let mut cmpfn = memo.entry.cmp_proc_finfo();

        if meta.typlen > 0 {
            width_bucket_array_fixed(mcx, operand, &thresholds, collation, meta, &mut cmpfn, nitems)?
        } else {
            width_bucket_array_variable(mcx, operand, &thresholds, collation, meta, &mut cmpfn, nitems)?
        }
    };

    Ok(Datum::from_i32(result))
}
