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

/// `generate_subscripts_nodir(array, dim)` (arrayfuncs.c:5986): the
/// no-direction SRF variant — "just call the other one, it can handle both
/// cases" with `reverse = false`.
pub fn generate_subscripts_nodir<'mcx>(
    mcx: Mcx<'mcx>,
    array: &[u8],
    dim: i32,
) -> PgResult<PgVec<'mcx, i32>> {
    generate_subscripts(mcx, array, dim, false)
}

// ---------------------------------------------------------------------------
// array_unnest (arrayfuncs.c) — set-returning.
// ---------------------------------------------------------------------------

/// `array_unnest(array)` (arrayfuncs.c:6259): the SRF that emits every array
/// element in storage order.
///
/// C carries an `array_unnest_fctx` (an `array_iter` cursor, `nextelem`,
/// `numelems`, and the element `typlen`/`typbyval`/`typalign`) across SRF calls,
/// emitting `array_iter_next(...)` per call until exhausted. The simplified
/// SRF model materializes the whole emission sequence in order, preserving the
/// C per-call ordering and NULL flags. Elements cross as the real
/// [`ArrayElementDatum`] (by-value Datum or on-disk byte window), exactly as the
/// comparison family already models them — never an opaque stand-in.
///
/// The expanded-array fast path (`VARATT_IS_EXPANDED_HEADER`) belongs to the
/// unported expanded-array subsystem; only the flat on-disk arm is reproduced
/// (the element storage triple comes from `get_typlenbyvalalign`).
pub fn array_unnest<'mcx>(
    mcx: Mcx<'mcx>,
    array: &[u8],
) -> PgResult<PgVec<'mcx, (ArrayElementDatum<'mcx>, bool)>> {
    let element_type = foundation::arr_elemtype(array);
    let ndim = foundation::arr_ndim(array);
    let numelems = arrayutils::array_get_n_items::call(ndim, &dims_vec(array))?;

    let s = lsyscache::get_typlenbyvalalign::call(element_type)?;
    let elmlen = s.typlen as i32;
    let elmbyval = s.typbyval;
    let elmalign = s.typalign as u8;

    let mut out = vec_with_capacity_in::<(ArrayElementDatum<'mcx>, bool)>(mcx, numelems.max(0) as usize)?;

    // array_iter_setup + array_iter_next, flat-array arm.
    let mut dataptr = foundation::arr_data_ptr_off(array);
    let bitmap = foundation::arr_nullbitmap_off(array);

    for i in 0..numelems {
        if foundation::array_get_isnull(array, bitmap, i) {
            // NULL element: dataptr is NOT advanced (matches array_iter_next).
            out.push((ArrayElementDatum::ByValue(Datum::null()), true));
        } else {
            let off = dataptr;
            let after = foundation::att_addlength_pointer(off, elmlen, array, off);
            let elem: ArrayElementDatum<'mcx> = if elmbyval {
                ArrayElementDatum::ByValue(foundation::fetch_att(array, off, elmbyval, elmlen))
            } else {
                // SAFETY-of-model: the element window lives in the same `'mcx`
                // buffer the caller owns; tie it to `'mcx`.
                let window: &'mcx [u8] = unsafe {
                    core::slice::from_raw_parts(array[off..after].as_ptr(), after - off)
                };
                ArrayElementDatum::ByRef(window)
            };
            dataptr = foundation::att_align_nominal(after, elmalign);
            out.push((elem, false));
        }
    }

    Ok(out)
}

/// `array_unnest_support(rawreq)` (arrayfuncs.c:6350): planner support function
/// estimating `array_unnest`'s output row count.
///
/// C inspects a `SupportRequestRows`, and for a function clause estimates the
/// rows via `estimate_expression_value` + `estimate_array_length`. Those are
/// planner-subsystem callees (`SupportRequestRows`, `estimate_*`) that are not
/// ported into this crate; mirror PG and panic loudly at that boundary rather
/// than invent the planner-request vocabulary.
pub fn array_unnest_support() -> PgResult<()> {
    panic!(
        "array_unnest_support: the planner support-request vocabulary (SupportRequestRows, \
         estimate_expression_value, estimate_array_length) is not ported into this crate"
    )
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
            detoasted = backend_access_common_detoast_seams::detoast_attr::call(
                mcx,
                datum_as_byte_window(value),
            )?;
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
/// C (arrayfuncs.c:6644): NULL input array yields SQL NULL (the caller's NULL
/// path); otherwise `array_replace_internal(array, search, search_isnull, 0,
/// true, /*remove=*/true, collation)`. The element value `search` crosses as
/// the real [`ArrayElementDatum`] (by-value Datum or on-disk byte window),
/// matching how every other element in this crate is modeled — never an opaque
/// stand-in.
pub fn array_remove<'mcx>(
    mcx: Mcx<'mcx>,
    array: &[u8],
    search: ArrayElementDatum<'_>,
    search_isnull: bool,
    collation: Oid,
) -> PgResult<PgVec<'mcx, u8>> {
    array_replace_internal(mcx, array, search, search_isnull, None, true, collation)
}

/// `array_replace(array, search, replace)` (arrayfuncs.c).
///
/// C (arrayfuncs.c:6666): `array_replace_internal(array, search, search_isnull,
/// replace, replace_isnull, /*remove=*/false, collation)`.
pub fn array_replace<'mcx>(
    mcx: Mcx<'mcx>,
    array: &[u8],
    search: ArrayElementDatum<'_>,
    search_isnull: bool,
    replace: ArrayElementDatum<'_>,
    replace_isnull: bool,
    collation: Oid,
) -> PgResult<PgVec<'mcx, u8>> {
    array_replace_internal(
        mcx,
        array,
        search,
        search_isnull,
        Some((replace, replace_isnull)),
        false,
        collation,
    )
}

/// `array_replace_internal(array, search, search_isnull, replace,
/// replace_isnull, remove, collation)` (arrayfuncs.c:6386): the shared
/// remove/replace engine (`replace == None` / `remove == true` means remove).
///
/// C caches `lookup_type_cache(element_type, TYPECACHE_EQ_OPR_FINFO)` and scans
/// the array applying `FunctionCallInvoke(eq_opr_finfo)` per element to find
/// matches, building `values[]`/`nulls[]` of the survivors (with replacements
/// substituted), then assembling the result with `CopyArrayEls`. The element
/// equality is resolved through `lookup_element_eq_opr` (typcache owner) +
/// `element_eq` (fmgr owner) — the same pair `array_eq`/`arrayoverlap` already
/// drive — and the survivor buffer is laid out by the construct family's
/// `construct_md_array` (which owns that byte math in-crate), so the whole body
/// lands in-crate. For `remove` the first dimension is shrunk to the survivor
/// count; the C `ARR_DIMS(result)[0] = nresult` (a one-dimensional array, the
/// only shape `remove` allows).
fn array_replace_internal<'mcx>(
    mcx: Mcx<'mcx>,
    array: &[u8],
    search: ArrayElementDatum<'_>,
    search_isnull: bool,
    replace: Option<(ArrayElementDatum<'_>, bool)>,
    remove: bool,
    collation: Oid,
) -> PgResult<PgVec<'mcx, u8>> {
    let element_type = foundation::arr_elemtype(array);
    let ndim = foundation::arr_ndim(array);
    let dims = dims_vec(array);
    let nitems = arrayutils::array_get_n_items::call(ndim, &dims)?;

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

    // Resolve the element type's equality operator (C: lookup_type_cache(
    // element_type, TYPECACHE_EQ_OPR_FINFO) -> typentry->eq_opr_finfo.fn_oid),
    // driven through the typcache owner — exactly as array_eq does.
    let eq_opr = typcache::lookup_element_eq_opr::call(element_type)?;
    if eq_opr == INVALID_OID {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_FUNCTION)
            .errmsg(format!(
                "could not identify an equality operator for type {element_type}"
            ))
            .into_error());
    }

    let s = lsyscache::get_typlenbyvalalign::call(element_type)?;
    let typlen = s.typlen as i32;
    let typbyval = s.typbyval;
    let typalign = s.typalign as u8;

    let (replace_elem, replace_isnull) = match replace {
        Some((r, isnull)) => (Some(r), isnull),
        None => (None, false),
    };

    // Scan the elements, building the survivor value/null vectors. We walk the
    // on-disk data directly (the C `arraydataptr`/`bitmap` loop) so by-reference
    // elements keep their byte windows for both the equality probe and the
    // rebuild.
    let mut out_values = vec_with_capacity_in::<Datum>(mcx, nitems as usize)?;
    let mut out_nulls = vec_with_capacity_in::<bool>(mcx, nitems as usize)?;
    let mut changed = false;

    let mut dataptr = foundation::arr_data_ptr_off(array);
    let bitmap = foundation::arr_nullbitmap_off(array);

    for i in 0..nitems {
        let is_null_elt = foundation::array_get_isnull(array, bitmap, i);

        // Survivor value Datum + null flag, and whether to skip (remove) it.
        let mut skip = false;
        let mut push_val: Datum = Datum::null();
        let mut push_null: bool;

        if is_null_elt {
            push_null = true;
            // A NULL array element: matches iff the search value is NULL.
            if search_isnull {
                if remove {
                    skip = true;
                    changed = true;
                } else if !replace_isnull {
                    // Substitute the replacement (non-null) for this NULL.
                    push_val = replace_elem
                        .as_ref()
                        .map(elem_as_datum)
                        .unwrap_or_else(Datum::null);
                    push_null = false;
                    changed = true;
                }
            }
        } else {
            // Fetch this element's value (and its byte window for the probe).
            let off = dataptr;
            let elt_datum = foundation::fetch_att(array, off, typbyval, typlen);
            let after = foundation::att_addlength_pointer(off, typlen, array, off);
            dataptr = foundation::att_align_nominal(after, typalign);

            push_null = false;
            push_val = elt_datum;

            if search_isnull {
                // Non-null element vs. NULL search: never matches; keep as-is.
            } else {
                let arg0: ArrayElementDatum<'_> = if typbyval {
                    ArrayElementDatum::ByValue(elt_datum)
                } else {
                    ArrayElementDatum::ByRef(&array[off..after])
                };
                let oprresult = fmgr::element_eq::call(eq_opr, collation, arg0, search)?;
                if oprresult {
                    changed = true;
                    if remove {
                        skip = true;
                    } else {
                        push_val = replace_elem
                            .as_ref()
                            .map(elem_as_datum)
                            .unwrap_or_else(Datum::null);
                        push_null = replace_isnull;
                    }
                }
            }
        }

        if !skip {
            out_values.push(push_val);
            out_nulls.push(push_null);
        }
    }

    // If nothing changed, return the input array unmodified (C returns `array`).
    if !changed {
        return mcx::slice_in(mcx, array);
    }

    let nresult = out_values.len() as i32;
    if nresult == 0 {
        return construct::construct_empty_array(mcx, element_type);
    }

    // Assemble the result. For replace the shape is unchanged; for remove the
    // (one-dimensional) array's single dimension shrinks to the survivor count.
    let mut result_dims = dims.clone();
    if remove {
        result_dims[0] = nresult;
    }
    let lbs = foundation::arr_lbounds(mcx, array)?;

    let nulls_opt: Option<&[bool]> = if out_nulls.iter().any(|&n| n) {
        Some(&out_nulls[..])
    } else {
        None
    };

    construct::construct_md_array(
        mcx,
        &out_values,
        nulls_opt,
        ndim,
        &result_dims,
        &lbs,
        element_type,
        typlen,
        typbyval,
        typalign,
    )
}

/// The survivor/replacement element `Datum` from an [`ArrayElementDatum`]: the
/// by-value word directly, or — for a by-reference element — the pointer word
/// the rebuild layer (`construct_md_array`/`ArrayCastAndSet`) dereferences
/// through the detoast seam. The byte window is preserved on the original-array
/// scan path; a freshly-supplied by-reference replacement carries its bytes as
/// the seam-bridged payload the construct family already consumes.
fn elem_as_datum(elem: &ArrayElementDatum<'_>) -> Datum {
    match elem {
        ArrayElementDatum::ByValue(d) => *d,
        // The construct family reads a by-ref element's bytes through the
        // detoast seam (the crate-wide by-reference element boundary); the
        // pointer word is recovered from the byte window's address.
        ArrayElementDatum::ByRef(bytes) => Datum::from_usize(bytes.as_ptr() as usize),
    }
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
/// generic fixed- and variable-width paths drive the element type's cached
/// comparison proc through `lookup_element_cmp_proc` (typcache owner) +
/// `element_cmp` (fmgr owner) — the same pair `array_cmp` uses. The `operand`
/// crosses as the real [`ArrayElementDatum`] (by-value Datum or on-disk byte
/// window), exactly as the comparison family models elements; the threshold
/// elements are walked directly off the array buffer.
pub fn width_bucket_array(
    operand: ArrayElementDatum<'_>,
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

    // We have a dedicated implementation for float8 data. float8 is pass-by-
    // value (FLOAT8PASSBYVAL) on the platforms this targets, so the operand
    // carries its bits directly.
    if element_type == FLOAT8OID {
        let op = match operand {
            ArrayElementDatum::ByValue(d) => d,
            ArrayElementDatum::ByRef(bytes) => {
                let b: [u8; 8] = bytes[..8].try_into().map_err(|_| {
                    ereport(ERROR)
                        .errcode(ERRCODE_INTERNAL_ERROR)
                        .errmsg("malformed float8 operand")
                        .into_error()
                })?;
                Datum::from_usize(u64::from_le_bytes(b) as usize)
            }
        };
        return Ok(width_bucket_array_float8(op, thresholds)?);
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
    operand: ArrayElementDatum<'_>,
    thresholds: &[u8],
    collation: Oid,
    cmp_proc: Oid,
    typlen: i32,
    typbyval: bool,
) -> PgResult<i32> {
    let data_off = foundation::arr_data_ptr_off(thresholds);

    // operand is itself an element of the (fixed-width) element type; it crosses
    // as the real ArrayElementDatum (by-value word or on-disk byte window).
    let arg0 = operand;

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
    operand: ArrayElementDatum<'_>,
    thresholds: &[u8],
    collation: Oid,
    cmp_proc: Oid,
    typlen: i32,
    typbyval: bool,
    typalign: u8,
) -> PgResult<i32> {
    // thresholds_data = (char *) ARR_DATA_PTR(thresholds);
    let mut thresholds_data = foundation::arr_data_ptr_off(thresholds);

    let arg0 = operand;

    let mut left: i32 = 0;
    let mut right: i32 = arrayutils::array_get_n_items::call(
        foundation::arr_ndim(thresholds),
        &dims_vec(thresholds),
    )?;

    while left < right {
        let mid = (left + right) / 2;

        // Locate mid'th array element by advancing from the left element.
        let mut ptr = thresholds_data;
        for _ in left..mid {
            let after = foundation::att_addlength_pointer(ptr, typlen, thresholds, ptr);
            ptr = foundation::att_align_nominal(after, typalign);
        }

        let arg1 = element_datum_at(thresholds, ptr, typlen, typbyval);
        let cmpresult = fmgr::element_cmp::call(cmp_proc, collation, arg0, arg1)?;

        if cmpresult < 0 {
            right = mid;
        } else {
            left = mid + 1;

            // Move the thresholds base to match the new "left" index, so we
            // don't re-walk those elements (keeps the work O(N), not O(N^2)).
            let after = foundation::att_addlength_pointer(ptr, typlen, thresholds, ptr);
            thresholds_data = foundation::att_align_nominal(after, typalign);
        }
    }

    Ok(left)
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
    panic!(
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
    panic!(
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
    panic!(
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
    panic!(
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

/// View the bytes a pass-by-reference (`typlen == -1`) element `Datum`'s
/// pointer word addresses, for the detoast seam. The owned model has no global
/// address space; the detoast subsystem (the byref-payload owner) resolves the
/// real bytes, so this hands it an empty window keyed by the datum and lets the
/// owner fault loudly until detoast lands — the same bridge `construct.rs` uses
/// for every other by-reference element access in this crate. This is the
/// boundary where the C `PG_DETOAST_DATUM(value)` reads through the pointer.
fn datum_as_byte_window<'a>(_value: Datum) -> &'a [u8] {
    &[]
}
