pub mod builtins;
#[cfg(test)]
mod tests;

use ::arrayfuncs::build::accum_array_result;
use ::arrayfuncs::construct::{construct_empty_array, construct_md_array, deconstruct_array};
use ::arrayfuncs::element::{array_bitmap_copy, array_set_element};
use ::arrayfuncs::foundation::{
    arr_data_offset, arr_elemtype, arr_ndim, arr_nullbitmap_off, arr_overhead_nonulls,
    arr_overhead_withnulls, arr_size, read_dims_lbounds, varsize_any, ARRAYTYPE_HDRSZ, MAXDIM,
};
use ::datum::array_build::{ArrayBuildState, ArrayBuildStateArr};
use ::datum::{varlena::set_varsize_4b, Datum, NullableDatum};
use ::mcx::{vec_append_bytes, vec_with_capacity_in, Mcx, PgVec};
use ::types_core::Oid;
use ::types_error::{
    PgError, PgResult, ERRCODE_ARRAY_ELEMENT_ERROR, ERRCODE_ARRAY_SUBSCRIPT_ERROR,
    ERRCODE_DATATYPE_MISMATCH, ERRCODE_DATA_EXCEPTION, ERRCODE_NULL_VALUE_NOT_ALLOWED,
    ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE, ERRCODE_PROGRAM_LIMIT_EXCEEDED,
};
use ::types_fmgr::FmgrInfo;

pub const ARRAY_LT_OP: Oid = 1072;
pub const ARRAY_GT_OP: Oid = 1073;
pub const F_BTARRAYCMP: Oid = 382;

#[derive(Clone, Copy)]
pub struct ElemMeta {
    pub element_type: Oid,
    pub typlen: i32,
    pub typbyval: bool,
    pub typalign: u8,
}

#[cold]
fn integer_out_of_range() -> Box<PgError> {
    Box::new(
        PgError::error("integer out of range").with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
    )
}

#[cold]
fn not_one_dimensional() -> Box<PgError> {
    Box::new(
        PgError::error("argument must be empty or one-dimensional array")
            .with_sqlstate(ERRCODE_DATA_EXCEPTION),
    )
}

#[cold]
fn incompatible_cat(detail: String) -> Box<PgError> {
    Box::new(
        PgError::error("cannot concatenate incompatible arrays")
            .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR)
            .with_detail(detail),
    )
}

#[cold]
fn diff_dimensionality() -> Box<PgError> {
    Box::new(
        PgError::error("cannot accumulate arrays of different dimensionality")
            .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
    )
}

fn append_prepend_index(array: &[u8], is_append: bool) -> PgResult<(i32, i32)> {
    let (ndims, dims, lbs) = read_dims_lbounds(array);
    if ndims == 1 {
        if is_append {
            let indx = lbs[0].checked_add(dims[0]).ok_or_else(integer_out_of_range)?;
            Ok((indx, lbs[0]))
        } else {
            let indx = lbs[0].checked_sub(1).ok_or_else(integer_out_of_range)?;
            Ok((indx, lbs[0]))
        }
    } else if ndims == 0 {
        Ok((1, 1))
    } else {
        Err(not_one_dimensional())
    }
}

fn set_lbound0(image: &mut [u8], lb: i32) {
    let ndim = arr_ndim(image) as usize;
    let off = ARRAYTYPE_HDRSZ + 4 * ndim;
    image[off..off + 4].copy_from_slice(&lb.to_ne_bytes());
}

pub fn array_append_internal<'m>(
    mcx: Mcx<'m>,
    array: &[u8],
    elem: Datum,
    isnull: bool,
    meta: &ElemMeta,
) -> PgResult<PgVec<'m, u8>> {
    let (indx, _lb0) = append_prepend_index(array, true)?;
    array_set_element(mcx, array, &[indx], elem, isnull, -1, meta.typlen, meta.typbyval, meta.typalign)
}

pub fn array_prepend_internal<'m>(
    mcx: Mcx<'m>,
    array: &[u8],
    elem: Datum,
    isnull: bool,
    meta: &ElemMeta,
) -> PgResult<PgVec<'m, u8>> {
    let (indx, lb0) = append_prepend_index(array, false)?;
    let mut out = array_set_element(
        mcx, array, &[indx], elem, isnull, -1, meta.typlen, meta.typbyval, meta.typalign,
    )?;
    // C keeps the input's lower bound after prepending (lbound[0] restored).
    if arr_ndim(&out) == 1 {
        set_lbound0(&mut out, lb0);
    }
    Ok(out)
}

pub fn array_cat_internal<'m>(
    mcx: Mcx<'m>,
    v1: &[u8],
    v2: &[u8],
) -> PgResult<PgVec<'m, u8>> {
    let element_type1 = arr_elemtype(v1);
    let element_type2 = arr_elemtype(v2);
    if element_type1 != element_type2 {
        return Err(Box::new(
            PgError::error("cannot concatenate incompatible arrays")
                .with_sqlstate(ERRCODE_DATATYPE_MISMATCH)
                .with_detail(format!(
                    "Arrays with element types {} and {} are not compatible for concatenation.",
                    ::format_type::format_type_be(element_type1)?,
                    ::format_type::format_type_be(element_type2)?
                )),
        ));
    }
    let element_type = element_type1;

    let (ndims1, dims1, lbs1) = read_dims_lbounds(v1);
    let (ndims2, dims2, lbs2) = read_dims_lbounds(v2);

    if ndims1 == 0 && ndims2 > 0 {
        return ::mcx::slice_in(mcx, v2);
    }
    if ndims2 == 0 {
        return ::mcx::slice_in(mcx, v1);
    }

    if ndims1 != ndims2 && ndims1 != ndims2 - 1 && ndims1 != ndims2 + 1 {
        return Err(incompatible_cat(format!(
            "Arrays of {ndims1} and {ndims2} dimensions are not compatible for concatenation."
        )));
    }

    let nitems1 = ::arrayutils::array_get_n_items(ndims1, &dims1)?;
    let nitems2 = ::arrayutils::array_get_n_items(ndims2, &dims2)?;
    let ndatabytes1 = arr_size(v1) - arr_data_offset(v1);
    let ndatabytes2 = arr_size(v2) - arr_data_offset(v2);

    let ndims;
    let mut dims;
    let lbs;
    if ndims1 == ndims2 {
        ndims = ndims1;
        dims = dims1;
        lbs = lbs1;
        dims[0] = dims1[0].wrapping_add(dims2[0]);
        for i in 1..ndims as usize {
            if dims1[i] != dims2[i] || lbs1[i] != lbs2[i] {
                return Err(incompatible_cat(String::from(
                    "Arrays with differing element dimensions are not compatible for concatenation.",
                )));
            }
        }
    } else if ndims1 == ndims2 - 1 {
        ndims = ndims2;
        dims = dims2;
        lbs = lbs2;
        dims[0] = dims[0].wrapping_add(1);
        for i in 0..ndims1 as usize {
            if dims1[i] != dims[i + 1] || lbs1[i] != lbs[i + 1] {
                return Err(incompatible_cat(String::from(
                    "Arrays with differing dimensions are not compatible for concatenation.",
                )));
            }
        }
    } else {
        ndims = ndims1;
        dims = dims1;
        lbs = lbs1;
        dims[0] = dims[0].wrapping_add(1);
        for i in 0..ndims2 as usize {
            if dims2[i] != dims[i + 1] || lbs2[i] != lbs[i + 1] {
                return Err(incompatible_cat(String::from(
                    "Arrays with differing dimensions are not compatible for concatenation.",
                )));
            }
        }
    }

    let nitems = ::arrayutils::array_get_n_items(ndims, &dims)?;
    ::arrayutils::array_check_bounds(ndims, &dims, &lbs)?;

    let hasnulls = arr_nullbitmap_off(v1).is_some() || arr_nullbitmap_off(v2).is_some();
    let ndatabytes = ndatabytes1 + ndatabytes2;
    let (dataoffset, nbytes) = if hasnulls {
        let d = arr_overhead_withnulls(ndims, nitems);
        (d as i32, ndatabytes + d)
    } else {
        (0, ndatabytes + arr_overhead_nonulls(ndims))
    };

    let mut out: PgVec<u8> = vec_with_capacity_in(mcx, nbytes)?;
    out.resize(nbytes, 0);
    out[0..4].copy_from_slice(&set_varsize_4b(nbytes));
    out[4..8].copy_from_slice(&ndims.to_ne_bytes());
    out[8..12].copy_from_slice(&dataoffset.to_ne_bytes());
    out[12..16].copy_from_slice(&(element_type as u32).to_ne_bytes());
    write_dims_lbs(&mut out, ndims, &dims, &lbs);

    let dst = arr_data_offset(&out);
    let s1 = arr_data_offset(v1);
    let s2 = arr_data_offset(v2);
    out[dst..dst + ndatabytes1].copy_from_slice(&v1[s1..s1 + ndatabytes1]);
    out[dst + ndatabytes1..dst + ndatabytes1 + ndatabytes2]
        .copy_from_slice(&v2[s2..s2 + ndatabytes2]);

    if hasnulls {
        let bo = ARRAYTYPE_HDRSZ + 2 * 4 * ndims as usize;
        array_bitmap_copy(&mut out, bo, 0, arr_nullbitmap_off(v1).map(|b| (v1, b)), 0, nitems1);
        array_bitmap_copy(
            &mut out, bo, nitems1, arr_nullbitmap_off(v2).map(|b| (v2, b)), 0, nitems2,
        );
    }
    Ok(out)
}

fn write_dims_lbs(out: &mut [u8], ndims: i32, dims: &[i32], lbs: &[i32]) {
    let mut off = ARRAYTYPE_HDRSZ;
    for i in 0..ndims as usize {
        out[off..off + 4].copy_from_slice(&dims[i].to_ne_bytes());
        off += 4;
    }
    for i in 0..ndims as usize {
        out[off..off + 4].copy_from_slice(&lbs[i].to_ne_bytes());
        off += 4;
    }
}

pub struct PositionSearch {
    pub searched: Datum,
    pub null_search: bool,
    pub collation: Oid,
    pub position_min: Option<i32>,
}

pub fn array_position_internal(
    mcx: Mcx<'_>,
    array: &[u8],
    s: &PositionSearch,
    meta: &ElemMeta,
    eqproc: &mut FmgrInfo,
) -> PgResult<Option<i32>> {
    let (_nd, _dims, lbs) = read_dims_lbounds(array);
    let mut position = lbs[0] - 1;
    let position_min = s.position_min.unwrap_or(lbs[0]);
    let (elems, nulls) = deconstruct_array(mcx, array, meta.typlen, meta.typbyval, meta.typalign, true)?;
    for (i, &value) in elems.iter().enumerate() {
        position += 1;
        if position < position_min {
            continue;
        }
        let isnull = nulls[i];
        if isnull || s.null_search {
            if isnull && s.null_search {
                return Ok(Some(position));
            }
            continue;
        }
        if ::types_fmgr::function_call2_coll_in(eqproc, s.collation, mcx, s.searched, value)?
            .as_bool()
        {
            return Ok(Some(position));
        }
    }
    Ok(None)
}

pub fn array_positions_internal<'m>(
    mcx: Mcx<'m>,
    array: &[u8],
    s: &PositionSearch,
    meta: &ElemMeta,
    eqproc: &mut FmgrInfo,
) -> PgResult<PgVec<'m, u8>> {
    const INT4OID: Oid = 23;
    let mut astate = ArrayBuildState::new(mcx, INT4OID, false)?;
    astate.typlen = 4;
    astate.typbyval = true;
    astate.typalign = b'i';

    let (_nd, _dims, lbs) = read_dims_lbounds(array);
    let mut position = lbs[0] - 1;
    let (elems, nulls) = deconstruct_array(mcx, array, meta.typlen, meta.typbyval, meta.typalign, true)?;
    for (i, &value) in elems.iter().enumerate() {
        position += 1;
        let isnull = nulls[i];
        let hit = if isnull || s.null_search {
            isnull && s.null_search
        } else {
            ::types_fmgr::function_call2_coll_in(eqproc, s.collation, mcx, s.searched, value)?
                .as_bool()
        };
        if hit {
            astate = accum_array_result(mcx, Some(astate), Datum::from_i32(position), false, INT4OID)?;
        }
    }
    ::arrayfuncs::build::make_array_result(mcx, &astate)
}

pub fn init_array_result_arr<'m>(
    mcx: Mcx<'m>,
    array_type: Oid,
    element_type: Oid,
) -> PgResult<ArrayBuildStateArr<'m>> {
    let element_type = if element_type != 0 {
        element_type
    } else {
        let et = ::lsyscache::get_element_type(array_type)?;
        if et == 0 {
            return Err(Box::new(
                PgError::error(format!(
                    "data type {} is not an array type",
                    ::format_type::format_type_be(array_type)?
                ))
                .with_sqlstate(ERRCODE_DATATYPE_MISMATCH),
            ));
        }
        et
    };
    Ok(ArrayBuildStateArr {
        mcx,
        data: PgVec::new_in(mcx),
        nullbitmap: None,
        nbytes: 0,
        nitems: 0,
        ndims: 0,
        dims: [0; MAXDIM],
        lbs: [0; MAXDIM],
        array_type,
        element_type,
        private_cxt: false,
    })
}

pub fn accum_array_result_arr<'m>(
    mcx: Mcx<'m>,
    astate: Option<ArrayBuildStateArr<'m>>,
    arg: Option<&[u8]>,
    array_type: Oid,
) -> PgResult<ArrayBuildStateArr<'m>> {
    let Some(arg) = arg else {
        return Err(Box::new(
            PgError::error("cannot accumulate null arrays")
                .with_sqlstate(ERRCODE_NULL_VALUE_NOT_ALLOWED),
        ));
    };
    let mut st = match astate {
        Some(s) => {
            debug_assert_eq!(s.array_type, array_type);
            s
        }
        None => init_array_result_arr(mcx, array_type, 0)?,
    };

    let (ndims, dims, lbs) = read_dims_lbounds(arg);
    let nitems = ::arrayutils::array_get_n_items(ndims, &dims)?;
    let data_off = arr_data_offset(arg);
    let ndatabytes = arr_size(arg) - data_off;

    if st.ndims == 0 {
        if ndims == 0 {
            return Err(Box::new(
                PgError::error("cannot accumulate empty arrays")
                    .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
            ));
        }
        if ndims as usize + 1 > MAXDIM {
            return Err(Box::new(
                PgError::error(format!(
                    "number of array dimensions ({}) exceeds the maximum allowed ({MAXDIM})",
                    ndims + 1
                ))
                .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
            ));
        }
        st.ndims = ndims + 1;
        st.dims[0] = 0;
        st.dims[1..=ndims as usize].copy_from_slice(&dims[..ndims as usize]);
        st.lbs[0] = 1;
        st.lbs[1..=ndims as usize].copy_from_slice(&lbs[..ndims as usize]);
    } else {
        if st.ndims != ndims + 1 {
            return Err(diff_dimensionality());
        }
        for i in 0..ndims as usize {
            if st.dims[i + 1] != dims[i] || st.lbs[i + 1] != lbs[i] {
                return Err(diff_dimensionality());
            }
        }
    }

    vec_append_bytes(&mut st.data, &arg[data_off..data_off + ndatabytes])?;
    st.nbytes += ndatabytes as i32;

    let arg_bitmap = arr_nullbitmap_off(arg);
    if st.nullbitmap.is_some() || arg_bitmap.is_some() {
        let newnitems = st.nitems + nitems;
        let need = newnitems as usize / 8 + 1;
        match st.nullbitmap.as_mut() {
            None => {
                let mut bm: PgVec<u8> = vec_with_capacity_in(mcx, need)?;
                bm.resize(need, 0);
                array_bitmap_copy(&mut bm, 0, 0, None, 0, st.nitems);
                st.nullbitmap = Some(bm);
            }
            Some(bm) => {
                if bm.len() < need {
                    bm.resize(need, 0);
                }
            }
        }
        let bm = st.nullbitmap.as_mut().unwrap();
        array_bitmap_copy(bm, 0, st.nitems, arg_bitmap.map(|b| (arg, b)), 0, nitems);
    }

    st.nitems += nitems;
    st.dims[0] += 1;
    Ok(st)
}

pub fn make_array_result_arr<'m>(
    mcx: Mcx<'m>,
    st: &ArrayBuildStateArr<'_>,
) -> PgResult<PgVec<'m, u8>> {
    if st.ndims == 0 {
        return construct_empty_array(mcx, st.element_type);
    }
    let _ = ::arrayutils::array_get_n_items(st.ndims, &st.dims)?;
    ::arrayutils::array_check_bounds(st.ndims, &st.dims, &st.lbs)?;

    let mut nbytes = st.nbytes as usize;
    let dataoffset = if st.nullbitmap.is_some() {
        let d = arr_overhead_withnulls(st.ndims, st.nitems);
        nbytes += d;
        d as i32
    } else {
        nbytes += arr_overhead_nonulls(st.ndims);
        0
    };

    let mut out: PgVec<u8> = vec_with_capacity_in(mcx, nbytes)?;
    out.resize(nbytes, 0);
    out[0..4].copy_from_slice(&set_varsize_4b(nbytes));
    out[4..8].copy_from_slice(&st.ndims.to_ne_bytes());
    out[8..12].copy_from_slice(&dataoffset.to_ne_bytes());
    out[12..16].copy_from_slice(&(st.element_type as u32).to_ne_bytes());
    write_dims_lbs(&mut out, st.ndims, &st.dims, &st.lbs);
    let dst = arr_data_offset(&out);
    out[dst..dst + st.nbytes as usize].copy_from_slice(&st.data[..st.nbytes as usize]);
    if let Some(bm) = st.nullbitmap.as_deref() {
        let bo = ARRAYTYPE_HDRSZ + 2 * 4 * st.ndims as usize;
        array_bitmap_copy(&mut out, bo, 0, Some((bm, 0)), 0, st.nitems);
    }
    Ok(out)
}

pub fn trim_array_internal<'m>(
    mcx: Mcx<'m>,
    v: &[u8],
    n: i32,
    elmlen: i32,
    elmalign: u8,
) -> PgResult<PgVec<'m, u8>> {
    let (ndim, dims, lbs) = read_dims_lbounds(v);
    let array_length = if ndim > 0 { dims[0] } else { 0 };
    if n < 0 || n > array_length {
        return Err(Box::new(
            PgError::error(format!(
                "number of elements to trim must be between 0 and {array_length}"
            ))
            .with_sqlstate(ERRCODE_ARRAY_ELEMENT_ERROR),
        ));
    }
    let mut lower = [0i32; MAXDIM];
    let mut upper = [0i32; MAXDIM];
    let lower_provided = [false; MAXDIM];
    let mut upper_provided = [false; MAXDIM];
    if ndim > 0 {
        upper[0] = lbs[0] + array_length - n - 1;
        upper_provided[0] = true;
    }
    ::arrayfuncs::element::array_get_slice(
        mcx, v, 1, &mut upper, &mut lower, &upper_provided, &lower_provided, -1, elmlen, elmalign,
    )
}

pub fn array_shuffle_n<'m>(
    mcx: Mcx<'m>,
    array: &[u8],
    n: i32,
    keep_lb: bool,
    meta: &ElemMeta,
) -> PgResult<PgVec<'m, u8>> {
    let (ndim, dims, lbs) = read_dims_lbounds(array);
    if ndim < 1 || dims[0] < 1 || n < 1 {
        return construct_empty_array(mcx, meta.element_type);
    }
    let (mut elems, mut nulls) =
        deconstruct_array(mcx, array, meta.typlen, meta.typbyval, meta.typalign, true)?;
    let nitem = dims[0];
    let nelm = elems.len() as i32 / nitem;

    for i in 0..n {
        let j = ::pg_prng::global_prng(|p| p.u64_range(i as u64, (nitem - 1) as u64)) as i32 * nelm;
        let base = i * nelm;
        for k in 0..nelm {
            elems.swap((base + k) as usize, (j + k) as usize);
            nulls.swap((base + k) as usize, (j + k) as usize);
        }
    }

    let mut rdims = dims;
    let mut rlbs = lbs;
    rdims[0] = n;
    if !keep_lb {
        rlbs[0] = 1;
    }
    construct_md_array(
        mcx, &elems, Some(&nulls), ndim, &rdims, &rlbs, meta.element_type, meta.typlen,
        meta.typbyval, meta.typalign,
    )
}

pub fn array_reverse_n<'m>(
    mcx: Mcx<'m>,
    array: &[u8],
    meta: &ElemMeta,
) -> PgResult<PgVec<'m, u8>> {
    let (ndim, dims, lbs) = read_dims_lbounds(array);
    let (mut elems, mut nulls) =
        deconstruct_array(mcx, array, meta.typlen, meta.typbyval, meta.typalign, true)?;
    let nitem = dims[0];
    let nelm = elems.len() as i32 / nitem;

    for i in 0..nitem / 2 {
        let a = i * nelm;
        let b = (nitem - i - 1) * nelm;
        for k in 0..nelm {
            elems.swap((a + k) as usize, (b + k) as usize);
            nulls.swap((a + k) as usize, (b + k) as usize);
        }
    }
    construct_md_array(
        mcx, &elems, Some(&nulls), ndim, &dims, &lbs, meta.element_type, meta.typlen,
        meta.typbyval, meta.typalign,
    )
}

// The datum sort is injected (the fmgr wrapper passes the tuplesort seam; a
// direct tuplesort dep would cycle through fmgr_core). `subarray_type` =
// Some(array typoid) when sorting the first dimension of a multidim array.
pub fn array_sort_with<'m>(
    mcx: Mcx<'m>,
    array: &[u8],
    meta: &ElemMeta,
    subarray_type: Option<Oid>,
    sorter: impl FnOnce(&[NullableDatum]) -> PgResult<PgVec<'m, NullableDatum>>,
) -> PgResult<PgVec<'m, u8>> {
    let (ndim, dims, lbs) = read_dims_lbounds(array);
    let (elems, nulls) =
        deconstruct_array(mcx, array, meta.typlen, meta.typbyval, meta.typalign, true)?;

    let mut subimages: Vec<PgVec<'m, u8>> = Vec::new();
    let mut items: PgVec<'m, NullableDatum> = vec_with_capacity_in(mcx, elems.len())?;
    match subarray_type {
        None => {
            for (i, &d) in elems.iter().enumerate() {
                items.push(NullableDatum { value: d, isnull: nulls[i] });
            }
        }
        Some(_) => {
            let nitem = dims[0];
            let nelm = elems.len() as i32 / nitem;
            let subndim = ndim - 1;
            for i in 0..nitem {
                let a = (i * nelm) as usize;
                let b = a + nelm as usize;
                let img = construct_md_array(
                    mcx, &elems[a..b], Some(&nulls[a..b]), subndim, &dims[1..], &lbs[1..],
                    meta.element_type, meta.typlen, meta.typbyval, meta.typalign,
                )?;
                items.push(NullableDatum {
                    value: Datum::from_usize(img.as_ptr() as usize),
                    isnull: false,
                });
                subimages.push(img);
            }
        }
    }
    let sorted = sorter(&items)?;

    match subarray_type {
        None => {
            let mut st = ArrayBuildState::new(mcx, meta.element_type, false)?;
            st.typlen = meta.typlen as i16;
            st.typbyval = meta.typbyval;
            st.typalign = meta.typalign;
            for nd in sorted.iter() {
                st = accum_array_result(mcx, Some(st), nd.value, nd.isnull, meta.element_type)?;
            }
            let mut out = ::arrayfuncs::build::make_array_result(mcx, &st)?;
            set_lbound0(&mut out, lbs[0]);
            Ok(out)
        }
        Some(arrtyp) => {
            let mut st = Some(init_array_result_arr(mcx, arrtyp, meta.element_type)?);
            for nd in sorted.iter() {
                let p = nd.value.as_usize() as *const u8;
                // SAFETY: sorted by-ref datums point at plain flat images the
                // sorter copied into `mcx`.
                let img = unsafe { core::slice::from_raw_parts(p, varsize_any(p)) };
                st = Some(accum_array_result_arr(mcx, st, Some(img), arrtyp)?);
            }
            let st = st.expect("array_sort: dims[0] >= 2 items were put");
            let mut out = make_array_result_arr(mcx, &st)?;
            set_lbound0(&mut out, lbs[0]);
            Ok(out)
        }
    }
}
