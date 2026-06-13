//! SQL family: `array_larger` / `array_smaller`, `generate_subscripts`,
//! `array_fill` / `array_remove` / `array_replace`, `width_bucket_array`,
//! `trim_array`, the array iterator (`array_create_iterator` /
//! `array_iterate` / `array_free_iterator`), and `array_map`.

use mcx::{vec_with_capacity_in, Mcx, PgVec};
use types_array::ArrayElementDatum;
use types_core::Oid;
use types_datum::datum::Datum;
use types_error::{
    PgResult, ERRCODE_ARRAY_ELEMENT_ERROR, ERRCODE_ARRAY_SUBSCRIPT_ERROR,
    ERRCODE_INTERNAL_ERROR, ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_NULL_VALUE_NOT_ALLOWED,
    ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERRCODE_UNDEFINED_FUNCTION, ERROR,
};

use backend_utils_error::ereport;

use crate::construct;
use crate::element_slice;
use crate::foundation::{self, FLOAT8OID, MAX_ALLOC_SIZE, MAX_DIM};
use crate::ops;

use backend_utils_adt_arrayutils_seams as arrayutils;
use backend_utils_cache_lsyscache_seams as lsyscache;
use backend_utils_cache_typcache_seams as typcache;
use backend_utils_fmgr_fmgr_seams as fmgr;

/// `InvalidOid` (`postgres_ext.h`).
const INVALID_OID: Oid = 0;

// ---------------------------------------------------------------------------
// Larger/smaller (arrayfuncs.c).
// ---------------------------------------------------------------------------

/// `array_larger(array1, array2)` (arrayfuncs.c): the greater per `array_cmp`.
///
/// C (arrayfuncs.c:5892):
/// ```c
/// if (array_cmp(fcinfo) > 0) PG_RETURN_DATUM(PG_GETARG_DATUM(0));
/// else                       PG_RETURN_DATUM(PG_GETARG_DATUM(1));
/// ```
pub fn array_larger<'mcx>(
    mcx: Mcx<'mcx>,
    array1: &[u8],
    array2: &[u8],
    collation: Oid,
) -> PgResult<PgVec<'mcx, u8>> {
    if ops::array_cmp(array1, array2, collation)? > 0 {
        mcx::slice_in(mcx, array1)
    } else {
        mcx::slice_in(mcx, array2)
    }
}

/// `array_smaller(array1, array2)` (arrayfuncs.c): the lesser per `array_cmp`.
///
/// C (arrayfuncs.c:5901):
/// ```c
/// if (array_cmp(fcinfo) < 0) PG_RETURN_DATUM(PG_GETARG_DATUM(0));
/// else                       PG_RETURN_DATUM(PG_GETARG_DATUM(1));
/// ```
pub fn array_smaller<'mcx>(
    mcx: Mcx<'mcx>,
    array1: &[u8],
    array2: &[u8],
    collation: Oid,
) -> PgResult<PgVec<'mcx, u8>> {
    if ops::array_cmp(array1, array2, collation)? < 0 {
        mcx::slice_in(mcx, array1)
    } else {
        mcx::slice_in(mcx, array2)
    }
}

// ---------------------------------------------------------------------------
// generate_subscripts (arrayfuncs.c) — set-returning.
// ---------------------------------------------------------------------------

/// `generate_subscripts(array, dim [, reverse])` (arrayfuncs.c): the subscript
/// range of dimension `dim`, materialized in order.
///
/// C (arrayfuncs.c:5922) is an SRF whose `generate_subscripts_fctx` carries
/// `{lower, upper, reverse}`; per call it emits `Int32GetDatum(lower++)` (or
/// `upper--` when reversed) while `lower <= upper`. The simplified seam returns
/// the whole materialized sequence at once, preserving the C emission order.
///
/// On the first-call sanity checks the C SRF takes `SRF_RETURN_DONE` (an empty
/// result set), which here is an empty vector:
/// ```c
/// if (AARR_NDIM(v) <= 0 || AARR_NDIM(v) > MAXDIM)  SRF_RETURN_DONE();
/// if (reqdim <= 0 || reqdim > AARR_NDIM(v))        SRF_RETURN_DONE();
/// lower = lb[reqdim-1];  upper = dimv[reqdim-1] + lb[reqdim-1] - 1;
/// ```
pub fn generate_subscripts<'mcx>(
    mcx: Mcx<'mcx>,
    array: &[u8],
    dim: i32,
    reverse: bool,
) -> PgResult<PgVec<'mcx, i32>> {
    let ndim = foundation::arr_ndim(array);

    // Sanity check: does it look like an array at all?
    if ndim <= 0 || ndim > MAX_DIM {
        return Ok(PgVec::new_in(mcx));
    }

    // Sanity check: was the requested dim valid.
    let reqdim = dim;
    if reqdim <= 0 || reqdim > ndim {
        return Ok(PgVec::new_in(mcx));
    }

    let lb = foundation::arr_lbound(array, (reqdim - 1) as usize);
    let dimv = foundation::arr_dim(array, (reqdim - 1) as usize);

    let lower: i32 = lb;
    let upper: i32 = dimv.wrapping_add(lb).wrapping_sub(1);

    // Materialize the per-call SRF emission sequence in order.
    let mut out = PgVec::new_in(mcx);
    if !reverse {
        let mut cur = lower;
        while cur <= upper {
            out.push(cur);
            cur += 1;
        }
    } else {
        let mut cur = upper;
        while lower <= cur {
            out.push(cur);
            cur -= 1;
        }
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// fill / remove / replace (arrayfuncs.c).
// ---------------------------------------------------------------------------

/// `array_fill(value, dims [, lbs])` (arrayfuncs.c): an array of `value`s with
/// the given dimensions.
///
/// The simplified seam delivers the `dims`/`lbs` arrays already deconstructed
/// into `&[i32]` (the C `array_fill`/`array_fill_with_lower_bounds` wrappers
/// take `int4[]` arrays, reject NULL/multi-dim/null-element inputs, and pull
/// out `ARR_DATA_PTR` as `int *`). `elmtype` arrives as the resolved input
/// element type (the C `get_fn_expr_argtype` result). When `lbs` is empty the C
/// `lbsv = deflbs` (all-ones) default is applied.
///
/// Mirrors `array_fill_internal` (arrayfuncs.c:6090) for the dimension/overflow
/// checks; the element buffer assembly (the C `create_array_envelope` +
/// `ArrayCastAndSet` loop, and the all-null bitmap path) is produced through
/// the construct family's `construct_md_array` / `construct_empty_array`, which
/// own that byte math in this crate.
pub fn array_fill<'mcx>(
    mcx: Mcx<'mcx>,
    value: Datum,
    is_null: bool,
    elmtype: Oid,
    dims: &[i32],
    lbs: &[i32],
) -> PgResult<PgVec<'mcx, u8>> {
    // ndims == the single element of the (1-D) dims array, or 0 for a 0-D dims
    // array. The seam passes the dims-array *contents*; its element count is
    // ndims. C: `ndims = (ARR_NDIM(dims) > 0) ? ARR_DIMS(dims)[0] : 0;` where
    // ARR_DIMS(dims)[0] is the count of int values in the dims data. Here the
    // count of supplied dimension extents IS `dims.len()`.
    let ndims = dims.len() as i32;

    if ndims < 0 {
        // (Unreachable for a Rust slice length, but mirror the C guard.)
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!("invalid number of dimensions: {ndims}"))
            .into_error());
    }
    if ndims > MAX_DIM {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
            .errmsg(format!(
                "number of array dimensions ({ndims}) exceeds the maximum allowed ({MAX_DIM})"
            ))
            .into_error());
    }

    // lbs handling: if provided it must have the same size as dims; otherwise
    // the C default is all-ones (deflbs).
    let mut deflbs = [1i32; MAX_DIM as usize];
    let lbsv: &[i32] = if !lbs.is_empty() {
        if lbs.len() as i32 != ndims {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR)
                .errmsg("wrong number of array subscripts")
                .errdetail("Low bound array has different size than dimensions array.")
                .into_error());
        }
        lbs
    } else {
        for d in deflbs.iter_mut().take(ndims as usize) {
            *d = 1;
        }
        &deflbs[..ndims as usize]
    };

    // This checks for overflow of the array dimensions, and validates bounds.
    let nitems = arrayutils::array_get_n_items::call(ndims, dims)?;
    arrayutils::array_check_bounds::call(ndims, dims, lbsv)?;

    // Fast track for empty array.
    if nitems <= 0 {
        return construct::construct_empty_array(mcx, elmtype);
    }

    // Look up element type storage info (C my_extra cache / get_typlenbyvalalign).
    let tlbva = lsyscache::get_typlenbyvalalign::call(elmtype)?;
    let elmlen = tlbva.typlen as i32;
    let elmbyval = tlbva.typbyval;
    let elmalign = tlbva.typalign as u8;

    if !is_null {
        // Make sure data is not toasted (C: if elmlen == -1, detoast).
        let detoasted;
        let value = if elmlen == -1 {
            detoasted =
                backend_access_common_detoast_seams::detoast_attr::call(mcx, datum_varlena(value))?;
            Datum::from_usize(detoasted.as_ptr() as usize)
        } else {
            value
        };

        // Build `nitems` copies of `value`, with no NULLs, then let the
        // construct family lay out the on-disk bytes (this is the safe-model
        // equivalent of create_array_envelope + the ArrayCastAndSet loop, and
        // performs the same att_addlength_datum / AllocSizeIsValid overflow
        // checks internally).
        let mut elems = vec_with_capacity_in::<Datum>(mcx, nitems as usize)?;
        for _ in 0..nitems {
            elems.push(value);
        }
        construct::construct_md_array(
            mcx, &elems, None, ndims, dims, lbsv, elmtype, elmlen, elmbyval, elmalign,
        )
    } else {
        // All-NULL array: build `nitems` NULL elements.
        let mut elems = vec_with_capacity_in::<Datum>(mcx, nitems as usize)?;
        let mut nulls = vec_with_capacity_in::<bool>(mcx, nitems as usize)?;
        for _ in 0..nitems {
            elems.push(Datum::null());
            nulls.push(true);
        }
        construct::construct_md_array(
            mcx,
            &elems,
            Some(&nulls),
            ndims,
            dims,
            lbsv,
            elmtype,
            elmlen,
            elmbyval,
            elmalign,
        )
    }
}

/// `array_remove(array, search)` (arrayfuncs.c): the array with every element
/// equal to `search` removed.
///
/// C (arrayfuncs.c:6644): NULL input array yields SQL NULL (here represented as
/// an empty `PgVec`, the caller's NULL sentinel for varlena results — see the
/// scaffold note); otherwise `array_replace_internal(array, search,
/// search_isnull, 0, true, /*remove=*/true, collation)`.
pub fn array_remove<'mcx>(
    mcx: Mcx<'mcx>,
    array: &[u8],
    search: Datum,
    search_isnull: bool,
    collation: Oid,
) -> PgResult<PgVec<'mcx, u8>> {
    array_replace_internal(mcx, array, search, search_isnull, None, collation)
}

/// `array_replace(array, search, replace)` (arrayfuncs.c).
///
/// C (arrayfuncs.c:6666): `array_replace_internal(array, search, search_isnull,
/// replace, replace_isnull, /*remove=*/false, collation)`.
pub fn array_replace<'mcx>(
    mcx: Mcx<'mcx>,
    array: &[u8],
    search: Datum,
    search_isnull: bool,
    replace: Datum,
    replace_isnull: bool,
    collation: Oid,
) -> PgResult<PgVec<'mcx, u8>> {
    array_replace_internal(
        mcx,
        array,
        search,
        search_isnull,
        Some((replace, replace_isnull)),
        collation,
    )
}

/// `array_replace_internal(...)` (arrayfuncs.c): the shared remove/replace
/// engine (`replace=None` means remove).
///
/// C (arrayfuncs.c:6386) caches `lookup_type_cache(element_type,
/// TYPECACHE_EQ_OPR_FINFO)` in `fcinfo->flinfo->fn_extra`, then scans the array
/// applying `FunctionCallInvoke(eq_opr_finfo)` per element to find matches.
///
/// This depends on the element type's **equality-operator finfo**, which the
/// shared `TypeCacheEntry` (`types-typcache`) does not yet expose: it carries
/// only the `pg_type` storage fields (`typlen`/`typbyval`/`typalign`/…), not
/// `eq_opr_finfo`. The fmgr owner's `element_eq` seam needs that resolved proc
/// OID. Until the typcache owner lands the richer entry (the `eq_opr_finfo`
/// field, exactly as `utils/typcache.h` defines it), the comparison cannot be
/// driven — so this mirrors the C structure but loudly panics at the operator
/// resolution boundary rather than inventing a stand-in. (Mirror-and-panic.)
pub fn array_replace_internal<'mcx>(
    mcx: Mcx<'mcx>,
    array: &[u8],
    search: Datum,
    search_isnull: bool,
    replace: Option<(Datum, bool)>,
    collation: Oid,
) -> PgResult<PgVec<'mcx, u8>> {
    let _ = (search, search_isnull, collation);
    let remove = replace.is_none();

    let element_type = foundation::arr_elemtype(array);
    let ndim = foundation::arr_ndim(array);
    let nitems = arrayutils::array_get_n_items::call(ndim, &dims_vec(array))?;

    // Return input array unmodified if it is empty.
    if nitems <= 0 {
        return mcx::slice_in(mcx, array);
    }

    // We can't remove elements from multi-dimensional arrays.
    if remove && ndim > 1 {
        return Err(ereport(ERROR)
            .errcode(types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("removing elements from multidimensional arrays is not supported")
            .into_error());
    }

    // The remaining body needs the element type's cached equality-operator
    // finfo (C: lookup_type_cache(element_type, TYPECACHE_EQ_OPR_FINFO) ->
    // typentry->eq_opr_finfo, driven through fmgr `element_eq`). The shared
    // TypeCacheEntry does not yet carry eq_opr_finfo; route through the typcache
    // owner once it exposes it.
    let _ = element_type;
    unimplemented!(
        "array_replace_internal: element equality requires the typcache owner to expose \
         TypeCacheEntry.eq_opr_finfo (TYPECACHE_EQ_OPR_FINFO) for the fmgr element_eq seam; \
         not yet landed (mirror-and-panic per Mirror-PG-and-panic)"
    )
}

// ---------------------------------------------------------------------------
// width_bucket / trim (arrayfuncs.c).
// ---------------------------------------------------------------------------

/// `width_bucket_array(operand, thresholds)` (arrayfuncs.c).
///
/// C (arrayfuncs.c:6695) validates the `thresholds` array (1-D, no NULLs), then
/// dispatches on element type: a dedicated `float8` binary search
/// (`width_bucket_array_float8`), or the generic fixed-/variable-width searches
/// driven by `lookup_type_cache(element_type, TYPECACHE_CMP_PROC_FINFO)` +
/// `FunctionCallInvoke(cmp_proc_finfo)`.
///
/// The `float8` fast path needs no comparator and is reproduced in full. The
/// generic path needs the element type's cached comparison-proc finfo, which
/// the shared `TypeCacheEntry` does not yet carry (`cmp_proc_finfo`); it is
/// routed to the typcache/fmgr owners via mirror-and-panic until landed.
pub fn width_bucket_array(
    operand: Datum,
    thresholds: &[u8],
    collation: Oid,
) -> PgResult<i32> {
    let element_type = foundation::arr_elemtype(thresholds);

    // Check input.
    if foundation::arr_ndim(thresholds) > 1 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR)
            .errmsg("thresholds must be one-dimensional array")
            .into_error());
    }

    if construct::array_contains_nulls(thresholds) {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED)
            .errmsg("thresholds array must not contain NULLs")
            .into_error());
    }

    // We have a dedicated implementation for float8 data.
    if element_type == FLOAT8OID {
        return Ok(width_bucket_array_float8(operand, thresholds)?);
    }

    // Cache information about the input type: the element type's btree
    // comparison support proc OID (C: lookup_type_cache(element_type,
    // TYPECACHE_CMP_PROC_FINFO) -> typentry->cmp_proc_finfo.fn_oid), resolved
    // through the typcache owner's lookup_element_cmp_proc seam.
    let cmp_proc = typcache::lookup_element_cmp_proc::call(element_type)?;
    if cmp_proc == INVALID_OID {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_FUNCTION)
            .errmsg(format!(
                "could not identify a comparison function for type {element_type}"
            ))
            .into_error());
    }

    // We have separate implementation paths for fixed- and variable-width
    // types, since indexing the array is a lot cheaper in the first case.
    let s = lsyscache::get_typlenbyvalalign::call(element_type)?;
    let typlen = s.typlen as i32;
    let typbyval = s.typbyval;
    let typalign = s.typalign as u8;
    if typlen > 0 {
        width_bucket_array_fixed(operand, thresholds, collation, cmp_proc, typlen, typbyval)
    } else {
        width_bucket_array_variable(
            operand, thresholds, collation, cmp_proc, typlen, typbyval, typalign,
        )
    }
}

/// `width_bucket_array_fixed(operand, thresholds, collation, typentry)`
/// (arrayfuncs.c:6802): binary search over a sorted, NULL-free array of a
/// generic fixed-width element type, indexing the data directly (`ptr =
/// thresholds_data + mid * typlen`) and comparing `operand` against each probed
/// element via the cached btree comparison proc (fmgr `element_cmp`).
fn width_bucket_array_fixed(
    operand: Datum,
    thresholds: &[u8],
    collation: Oid,
    cmp_proc: Oid,
    typlen: i32,
    typbyval: bool,
) -> PgResult<i32> {
    let data_off = foundation::arr_data_ptr_off(thresholds);

    // operand is itself an element of the (fixed-width) element type. For a
    // fixed-width type the comparator arg0 carries the operand value by-value;
    // a fixed by-reference type would need the operand's on-disk bytes, which a
    // bare Datum does not expose in the safe model (see the variable path).
    let arg0 = if typbyval {
        ArrayElementDatum::ByValue(operand)
    } else {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INTERNAL_ERROR)
            .errmsg(
                "width_bucket_array (fixed by-reference element): the operand bytes behind a bare \
                 Datum are not exposed by the safe model; lands with the element-bytes carrier \
                 (mirror-and-panic)",
            )
            .into_error());
    };

    let mut left: i32 = 0;
    let mut right: i32 = arrayutils::array_get_n_items::call(
        foundation::arr_ndim(thresholds),
        &dims_vec(thresholds),
    )?;

    while left < right {
        let mid = (left + right) / 2;
        let ptr = data_off + (mid as usize) * (typlen as usize);
        let arg1 = element_datum_at(thresholds, ptr, typlen, typbyval);

        let cmpresult = fmgr::element_cmp::call(cmp_proc, collation, arg0, arg1)?;

        if cmpresult < 0 {
            right = mid;
        } else {
            left = mid + 1;
        }
    }

    Ok(left)
}

/// `width_bucket_array_variable(operand, thresholds, collation, typentry)`
/// (arrayfuncs.c:6857): binary search over a sorted, NULL-free array of a
/// generic variable-width element type, walking the data pointer
/// (`att_addlength_pointer` + `att_align_nominal`) to reach the mid'th element
/// and advancing the base past confirmed-lower elements to keep the indexing
/// work O(N) rather than O(N^2).
fn width_bucket_array_variable(
    operand: Datum,
    thresholds: &[u8],
    collation: Oid,
    cmp_proc: Oid,
    typlen: i32,
    typbyval: bool,
    typalign: u8,
) -> PgResult<i32> {
    // The operand is a variable-width (by-reference) element; its on-disk bytes
    // live behind a bare Datum pointer-word that the safe model does not
    // expose. The threshold elements are walked from the array buffer, but
    // arg0 (the operand) cannot be materialized here. Route the operand-bytes
    // need across the (unported) element-bytes carrier boundary.
    let _ = (operand, thresholds, collation, cmp_proc, typlen, typbyval, typalign);
    unimplemented!(
        "width_bucket_array (variable-width element): the operand's on-disk bytes live behind a \
         bare Datum pointer-word the safe model does not expose (same boundary as array_fill's \
         varlena element); lands with the detoast/element-bytes carrier (mirror-and-panic)"
    )
}

/// Materialize the element at byte offset `off` in `buf` as an
/// [`ArrayElementDatum`] for the comparison seams: by-value types carry the
/// fetched `Datum`; by-reference types carry the element's on-disk byte window
/// (`att_addlength_pointer` gives its raw end before alignment).
fn element_datum_at<'a>(
    buf: &'a [u8],
    off: usize,
    typlen: i32,
    typbyval: bool,
) -> ArrayElementDatum<'a> {
    if typbyval {
        ArrayElementDatum::ByValue(foundation::fetch_att(buf, off, typbyval, typlen))
    } else {
        let end = foundation::att_addlength_pointer(off, typlen, buf, off);
        ArrayElementDatum::ByRef(&buf[off..end])
    }
}

/// `width_bucket_array_float8(operand, thresholds)` (arrayfuncs.c:6758): binary
/// search over a sorted, NULL-free `float8[]`.
fn width_bucket_array_float8(operand: Datum, thresholds: &[u8]) -> PgResult<i32> {
    let op: f64 = operand.as_f64();

    // Since we know the array contains no NULLs, we can index it directly.
    let data_off = foundation::arr_data_ptr_off(thresholds);

    let mut left: i32 = 0;
    let mut right: i32 =
        arrayutils::array_get_n_items::call(foundation::arr_ndim(thresholds), &dims_vec(thresholds))?;

    // A NaN probe is >= all thresholds (incl. NaNs), so we need not search.
    if op.is_nan() {
        return Ok(right);
    }

    // Find the bucket.
    while left < right {
        let mid = (left + right) / 2;
        let off = data_off + (mid as usize) * core::mem::size_of::<f64>();
        let bytes: [u8; 8] = thresholds[off..off + 8].try_into().unwrap();
        let t = f64::from_le_bytes(bytes);

        if t.is_nan() || op < t {
            right = mid;
        } else {
            left = mid + 1;
        }
    }

    Ok(left)
}

/// `trim_array(array, n)` (arrayfuncs.c): drop the last `n` elements of a
/// one-dimensional array.
///
/// C (arrayfuncs.c:6927): bounds-check `n`, build a slice whose first upper
/// bound is `ARR_LBOUND(v)[0] + array_length - n - 1` (all other bounds
/// unprovided), look up the element storage info, and call `array_get_slice`.
pub fn trim_array<'mcx>(mcx: Mcx<'mcx>, array: &[u8], n: i32) -> PgResult<PgVec<'mcx, u8>> {
    let ndim = foundation::arr_ndim(array);
    let array_length = if ndim > 0 {
        foundation::arr_dim(array, 0)
    } else {
        0
    };

    // Per spec, throw an error if out of bounds.
    if n < 0 || n > array_length {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_ARRAY_ELEMENT_ERROR)
            .errmsg(format!(
                "number of elements to trim must be between 0 and {array_length}"
            ))
            .into_error());
    }

    // Set all the bounds as unprovided except the first upper bound.
    let lower = [0i32; MAX_DIM as usize];
    let mut upper = [0i32; MAX_DIM as usize];
    let lower_provided = [false; MAX_DIM as usize];
    let mut upper_provided = [false; MAX_DIM as usize];
    if ndim > 0 {
        upper[0] = foundation::arr_lbound(array, 0) + array_length - n - 1;
        upper_provided[0] = true;
    }

    // Fetch the needed information about the element type.
    let tlbva = lsyscache::get_typlenbyvalalign::call(foundation::arr_elemtype(array))?;

    // Get the slice (arraytyplen = -1).
    element_slice::array_get_slice(
        mcx,
        array,
        1,
        &upper,
        &lower,
        &upper_provided,
        &lower_provided,
        -1,
        tlbva.typlen as i32,
        tlbva.typbyval,
        tlbva.typalign as u8,
    )
}

// ---------------------------------------------------------------------------
// Iterator (arrayfuncs.c).
// ---------------------------------------------------------------------------

/// `array_create_iterator(arr, slice_ndim, mstate)` (arrayfuncs.c:4602).
///
/// The C builds a heap-allocated `ArrayIteratorData` (data pointer cursor,
/// null-bitmap cursor, `nitems`, element typlen/byval/align, slice workspace)
/// and threads it through `array_iterate` / `array_free_iterator`. The
/// scaffold signature returns `()` and the iterate/free helpers carry no
/// iterator argument, so there is no place to hold that cross-call state. The
/// real `ArrayIterator` / `ArrayIteratorData` vocabulary type is not yet
/// defined; introducing one here would be an invented opaque handle. Mirror the
/// C entry point and panic loudly until the iterator type lands.
pub fn array_create_iterator<'mcx>(
    mcx: Mcx<'mcx>,
    array: &[u8],
    slice_ndim: i32,
) -> PgResult<()> {
    let _ = (mcx, array, slice_ndim);
    unimplemented!(
        "array_create_iterator: needs the ArrayIteratorData state type (data/bitmap cursors, \
         nitems, slice workspace) which the simplified scaffold signature cannot carry and \
         types-array does not yet define; lands with the iterator vocabulary \
         (mirror-and-panic per Opacity-inherited-never-introduced)"
    )
}

/// `array_iterate(iterator, &value, &isnull)` (arrayfuncs.c): yield the next
/// element (or slice); `Ok(None)` at exhaustion (C: returns `false`).
///
/// Cannot advance without the `ArrayIteratorData` cursor state described on
/// [`array_create_iterator`]; mirror-and-panic until that type lands.
pub fn array_iterate(/* iterator: &mut ArrayIterator */) -> PgResult<Option<(Datum, bool)>> {
    unimplemented!(
        "array_iterate: needs the ArrayIteratorData cursor state (not yet defined); see \
         array_create_iterator (mirror-and-panic)"
    )
}

/// `array_free_iterator(iterator)` (arrayfuncs.c).
///
/// Frees the iterator's slice workspace and the iterator itself; with no
/// iterator state type defined there is nothing to free. Mirror-and-panic until
/// the type lands.
pub fn array_free_iterator() {
    unimplemented!(
        "array_free_iterator: needs the ArrayIteratorData state type (not yet defined); see \
         array_create_iterator (mirror-and-panic)"
    )
}

// ---------------------------------------------------------------------------
// array_map (arrayfuncs.c).
// ---------------------------------------------------------------------------

/// `array_map(arrayd, exprstate, econtext, retType, amstate)` (arrayfuncs.c):
/// apply a per-element expression to produce a new array. The element
/// expression evaluation crosses the executor boundary.
///
/// C (arrayfuncs.c:3200) iterates the input array, writes each element into the
/// `ExprState`'s `innermost_caseval` / `innermost_casenull`, calls
/// `ExecEvalExpr(exprstate, econtext, ...)` to transform it, and assembles the
/// result. The transform is the heart of the function — but the scaffold
/// signature dropped `exprstate` and `econtext` entirely (only `mcx`, the input
/// `array`, and `ret_type` remain), so there is no expression to apply. The
/// executor `ExecEvalExpr` callee also has no seam wired into this crate.
/// Implementing anything here would be inventing own logic; mirror the C entry
/// point and panic loudly until the executor seam + full signature land.
pub fn array_map<'mcx>(
    mcx: Mcx<'mcx>,
    array: &[u8],
    ret_type: Oid,
) -> PgResult<PgVec<'mcx, u8>> {
    let _ = (mcx, array, ret_type, MAX_ALLOC_SIZE);
    unimplemented!(
        "array_map: the per-element transform needs the ExprState/ExprContext arguments the \
         scaffold signature dropped, and the executor ExecEvalExpr seam (not wired into this \
         crate); lands with the executor boundary (mirror-and-panic per Mirror-PG-and-panic)"
    )
}

// ---------------------------------------------------------------------------
// Local helpers.
// ---------------------------------------------------------------------------

/// The `ARR_DIMS(a)` slice as an owned `Vec<i32>` for the arrayutils seams
/// (which take `&[i32]`). Mirrors reading the C `int *` ARR_DIMS pointer.
fn dims_vec(a: &[u8]) -> Vec<i32> {
    let ndim = foundation::arr_ndim(a);
    let mut v: Vec<i32> = Vec::new();
    for i in 0..ndim.max(0) as usize {
        v.push(foundation::arr_dim(a, i));
    }
    v
}

/// A pass-by-reference (`typlen == -1`) element `Datum` carries the varlena
/// pointer word; the safe model has no raw memory behind it, so a toasted
/// varlena element cannot be dereferenced as a `&[u8]` here. The detoast seam
/// takes the element bytes; this is the boundary where the C
/// `PG_DETOAST_DATUM(value)` reads through the `Datum` pointer.
fn datum_varlena<'a>(_value: Datum) -> &'a [u8] {
    unimplemented!(
        "array_fill: detoasting a varlena element Datum needs the element bytes behind the \
         pointer-word Datum, which the safe model does not expose from a bare Datum; lands with \
         the detoast/varlena element-bytes carrier (mirror-and-panic)"
    )
}
