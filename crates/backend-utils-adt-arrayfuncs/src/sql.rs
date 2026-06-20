//! SQL family: `array_larger` / `array_smaller`, `generate_subscripts`,
//! `array_fill` / `array_remove` / `array_replace`, `width_bucket_array`,
//! `trim_array`, the array iterator (`array_create_iterator` /
//! `array_iterate` / `array_free_iterator`), and `array_map`.

use mcx::{vec_with_capacity_in, Mcx, PgVec};
use types_array::ArrayElementDatum;
use types_core::Oid;
use types_datum::datum::Datum;
use types_error::{
    PgError, PgResult, ERRCODE_ARRAY_ELEMENT_ERROR, ERRCODE_ARRAY_SUBSCRIPT_ERROR,
    ERRCODE_INTERNAL_ERROR, ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_NULL_VALUE_NOT_ALLOWED,
    ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERRCODE_UNDEFINED_FUNCTION, ERROR,
};

use backend_utils_error::ereport;

use crate::construct;
use crate::element_slice;
use crate::foundation::{self, FLOAT8OID, MAX_DIM};
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
    array: &'mcx [u8],
) -> PgResult<PgVec<'mcx, (ArrayElementDatum<'mcx>, bool)>> {
    // C: arr = PG_GETARG_ANY_ARRAY_P(0) == DatumGetArrayTypeP == pg_detoast_datum.
    // A stored array column (e.g. pg_proc.proallargtypes) arrives heap-packed
    // with a 1-byte SHORT varlena header; the `foundation::arr_*` readers below
    // read `ArrayType` fields at FIXED 4-byte-header offsets (elemtype at byte
    // 12), so a short image misreads ARR_ELEMTYPE as ARR_DIMS[0] ("cache lookup
    // failed for type N"). detoast_attr expands a short header to 4-byte; run it
    // so every field read sees a normalized header. (No-op on an already-4-byte
    // uncompressed image — VARATT_IS_4B_U.)
    let array: &'mcx [u8] = if !array.is_empty() && (array[0] & 0x03) != 0x00 {
        let detoasted = backend_access_common_detoast_seams::detoast_attr::call(mcx, array)?;
        &*detoasted.leak()
    } else {
        array
    };
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
                // The element window is a sub-slice of the caller's `'mcx`
                // buffer, so slicing already yields a `&'mcx [u8]`.
                let window: &'mcx [u8] = &array[off..after];
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
            // Fetch this element's byte window (the probe arg AND the survivor).
            let off = dataptr;
            let after = foundation::att_addlength_pointer(off, typlen, array, off);
            dataptr = foundation::att_align_nominal(after, typalign);

            // The element as it crosses the comparison seam AND feeds the result
            // rebuild. For a by-value element this is the bare word; for a
            // by-reference element it is the on-disk byte window. Crucially the
            // survivor `Datum` handed to `construct_md_array` / `ArrayCastAndSet`
            // must be a LIVE POINTER into the element bytes — NOT the in-buffer
            // offset that `fetch_att` returns for the by-ref case — because the
            // construct layer dereferences the pointer word (`datum_byref_image`
            // / `PG_DETOAST_DATUM`). `elem_as_datum` yields exactly that pointer
            // word for a by-reference element (`bytes.as_ptr()`), matching how a
            // freshly-supplied replacement element is carried. Using the raw
            // `fetch_att` offset here faulted (`EXC_BAD_ACCESS` at the small
            // offset address) for by-reference element types (e.g. text[]).
            let arg0: ArrayElementDatum<'_> = if typbyval {
                ArrayElementDatum::ByValue(foundation::fetch_att(array, off, typbyval, typlen))
            } else {
                ArrayElementDatum::ByRef(&array[off..after])
            };

            push_null = false;
            push_val = elem_as_datum(&arg0);

            if search_isnull {
                // Non-null element vs. NULL search: never matches; keep as-is.
            } else {
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

/// `ArrayIteratorData` (arrayfuncs.c:68) — working state for `array_iterate()`.
///
/// `ArrayIteratorData` is private to arrayfuncs.c (the C `array.h` exposes only
/// the opaque `typedef struct ArrayIteratorData *ArrayIterator`), so the struct
/// lives here in the porting crate rather than in `types-array`, exactly where C
/// keeps it. The byte model substitutes byte offsets for the C raw `char *` /
/// `bits8 *` cursors and owns the slice workspace in `mcx`-allocated `PgVec`s
/// instead of bare `palloc`'d arrays; the borrowed array buffer carries `'mcx`
/// (the C contract requires the array to outlive the iterator).
pub struct ArrayIteratorData<'mcx> {
    /* basic info about the array, set up during array_create_iterator() */
    /// `arr` — array we're iterating through (borrowed for the iterator's life).
    arr: &'mcx [u8],
    /// `nullbitmap` — its null bitmap offset, if any (`ARR_NULLBITMAP`).
    nullbitmap: Option<usize>,
    /// `nitems` — total number of elements in array.
    nitems: i32,
    /// `typlen` — element type's length.
    typlen: i16,
    /// `typbyval` — element type's byval property.
    typbyval: bool,
    /// `typalign` — element type's align property.
    typalign: u8,

    /* information about the requested slice size */
    /// `slice_ndim` — slice dimension, or 0 if not slicing.
    slice_ndim: i32,
    /// `slice_len` — number of elements per slice.
    slice_len: i32,
    /// `slice_dims` — slice dims array (rightmost N dims of `arr`).
    slice_dims: PgVec<'mcx, i32>,
    /// `slice_lbound` — slice lbound array (rightmost N lbounds of `arr`).
    slice_lbound: PgVec<'mcx, i32>,
    /// `slice_values` — workspace of length `slice_len`.
    slice_values: PgVec<'mcx, Datum>,
    /// `slice_nulls` — workspace of length `slice_len`.
    slice_nulls: PgVec<'mcx, bool>,

    /* current position information, updated on each iteration */
    /// `data_ptr` — our current position (byte offset) in the array.
    data_ptr: usize,
    /// `current_item` — the item # we're at in the array.
    current_item: i32,
}

/// `array_create_iterator(arr, slice_ndim, mstate)` (arrayfuncs.c:4602): set up
/// to iterate through an array.
///
/// If `slice_ndim` is zero, we iterate element-by-element; the returned datums
/// are of the array's element type. If `slice_ndim` is `1..ARR_NDIM(arr)`, we
/// iterate by slices (datums of the same array type, sized to the rightmost N
/// dimensions). The passed-in `array` must remain valid for the iterator's life
/// (`'mcx`).
///
/// `mstate` (C `ArrayMetaState *`) supplies a cached element storage triple; the
/// iterator only ever reads its `typlen`/`typbyval`/`typalign`, so it is modeled
/// as `Option<TypLenByValAlign>` (the lsyscache seam's existing vocabulary)
/// rather than inventing the full `ArrayMetaState` I/O-function record the
/// iterator never touches. `None` mirrors the C `mstate == NULL` arm
/// (`get_typlenbyvalalign(ARR_ELEMTYPE(arr), ...)`).
pub fn array_create_iterator<'mcx>(
    mcx: Mcx<'mcx>,
    array: &'mcx [u8],
    slice_ndim: i32,
    mstate: Option<lsyscache::TypLenByValAlign>,
) -> PgResult<ArrayIteratorData<'mcx>> {
    // Sanity-check inputs --- caller should have got this right already.
    // Assert(PointerIsValid(arr));
    let ndim = foundation::arr_ndim(array);
    if slice_ndim < 0 || slice_ndim > ndim {
        // elog(ERROR, "invalid arguments to array_create_iterator");
        return Err(PgError::error("invalid arguments to array_create_iterator")
            .with_sqlstate(ERRCODE_INTERNAL_ERROR));
    }

    // Remember basic info about the array and its element type.
    //   iterator->arr = arr;
    //   iterator->nullbitmap = ARR_NULLBITMAP(arr);
    //   iterator->nitems = ArrayGetNItems(ARR_NDIM(arr), ARR_DIMS(arr));
    let nullbitmap = foundation::arr_nullbitmap_off(array);
    let dims = foundation::arr_dims(mcx, array)?;
    let nitems = arrayutils::array_get_n_items::call(ndim, &dims)?;

    let (typlen, typbyval, typalign) = if let Some(ms) = mstate {
        // Assert(mstate->element_type == ARR_ELEMTYPE(arr));
        // iterator->typlen/typbyval/typalign = mstate->...;
        (ms.typlen, ms.typbyval, ms.typalign as u8)
    } else {
        // get_typlenbyvalalign(ARR_ELEMTYPE(arr), &typlen, &typbyval, &typalign);
        let s = lsyscache::get_typlenbyvalalign::call(foundation::arr_elemtype(array))?;
        (s.typlen, s.typbyval, s.typalign as u8)
    };

    // Remember the slicing parameters.
    //   iterator->slice_ndim = slice_ndim;
    let mut slice_len: i32 = 0;
    let mut slice_dims: PgVec<'mcx, i32> = PgVec::new_in(mcx);
    let mut slice_lbound: PgVec<'mcx, i32> = PgVec::new_in(mcx);
    let mut slice_values: PgVec<'mcx, Datum> = PgVec::new_in(mcx);
    let mut slice_nulls: PgVec<'mcx, bool> = PgVec::new_in(mcx);

    if slice_ndim > 0 {
        // Get pointers into the array's dims and lbound arrays to represent the
        // dims/lbound arrays of a slice. These are the same as the rightmost N
        // dimensions of the array.
        //   iterator->slice_dims = ARR_DIMS(arr) + ARR_NDIM(arr) - slice_ndim;
        //   iterator->slice_lbound = ARR_LBOUND(arr) + ARR_NDIM(arr) - slice_ndim;
        let base = (ndim - slice_ndim) as usize;
        for i in 0..slice_ndim as usize {
            slice_dims.push(foundation::arr_dim(array, base + i));
            slice_lbound.push(foundation::arr_lbound(array, base + i));
        }

        // Compute number of elements in a slice.
        //   iterator->slice_len = ArrayGetNItems(slice_ndim, iterator->slice_dims);
        slice_len = arrayutils::array_get_n_items::call(slice_ndim, &slice_dims)?;

        // Create workspace for building sub-arrays.
        //   iterator->slice_values = palloc(slice_len * sizeof(Datum));
        //   iterator->slice_nulls  = palloc(slice_len * sizeof(bool));
        slice_values = vec_with_capacity_in::<Datum>(mcx, slice_len.max(0) as usize)?;
        slice_values.resize(slice_len.max(0) as usize, Datum::null());
        slice_nulls = vec_with_capacity_in::<bool>(mcx, slice_len.max(0) as usize)?;
        slice_nulls.resize(slice_len.max(0) as usize, false);
    }

    // Initialize our data pointer and linear element number. These will advance
    // through the array during array_iterate().
    //   iterator->data_ptr = ARR_DATA_PTR(arr);
    //   iterator->current_item = 0;
    Ok(ArrayIteratorData {
        arr: array,
        nullbitmap,
        nitems,
        typlen,
        typbyval,
        typalign,
        slice_ndim,
        slice_len,
        slice_dims,
        slice_lbound,
        slice_values,
        slice_nulls,
        data_ptr: foundation::arr_data_ptr_off(array),
        current_item: 0,
    })
}

/// One item yielded by [`array_iterate`] — the two arms of the C
/// `*value`/`*isnull` out-params.
///
/// The C function writes a single `Datum`/`bool` pair for both arms, but the
/// slice arm's `Datum` is `PointerGetDatum(result)` for a freshly `palloc`'d
/// sub-array; the owned byte model has no global address space, so the built
/// buffer is carried out by value here (`Slice`) instead of as a dangling
/// pointer word. The scalar arm matches C directly (`Scalar`): a by-value Datum,
/// or — for by-reference element types — the `fetch_att` byte offset into the
/// iterator's array buffer.
pub enum ArrayIterateItem<'mcx> {
    /// `slice_ndim == 0`: one element value (`*value`, `*isnull`).
    Scalar { value: Datum, isnull: bool },
    /// `slice_ndim > 0`: a freshly built sub-array (C: `PointerGetDatum(result)`,
    /// `*isnull = false`).
    Slice(PgVec<'mcx, u8>),
}

/// `array_iterate(iterator, &value, &isnull)` (arrayfuncs.c:4682): iterate
/// through the array referenced by `iterator`.
///
/// As long as there is another element (or slice), return it as `Some(item)`
/// (C: `true`); return `Ok(None)` when no more data (C: `false`). For the
/// by-reference scalar case the returned `Datum` is a byte offset into the
/// iterator's array buffer (the byte-model `fetch_att` convention); the slice
/// case returns the freshly built sub-array buffer by value.
pub fn array_iterate<'mcx>(
    mcx: Mcx<'mcx>,
    iterator: &mut ArrayIteratorData<'mcx>,
) -> PgResult<Option<ArrayIterateItem<'mcx>>> {
    // Done if we have reached the end of the array.
    if iterator.current_item >= iterator.nitems {
        return Ok(None);
    }

    if iterator.slice_ndim == 0 {
        // Scalar case: return one element.
        let is_null = foundation::array_get_isnull(
            iterator.arr,
            iterator.nullbitmap,
            iterator.current_item,
        );
        iterator.current_item += 1;
        if is_null {
            Ok(Some(ArrayIterateItem::Scalar {
                value: Datum::null(),
                isnull: true,
            }))
        } else {
            // non-NULL, so fetch the individual Datum to return.
            let p = iterator.data_ptr;
            let value = foundation::fetch_att(
                iterator.arr,
                p,
                iterator.typbyval,
                iterator.typlen as i32,
            );
            // Move our data pointer forward to the next element.
            //   p = att_addlength_pointer(p, typlen, p);
            //   p = att_align_nominal(p, typalign);
            let p = foundation::att_addlength_pointer(p, iterator.typlen as i32, iterator.arr, p);
            iterator.data_ptr = foundation::att_align_nominal(p, iterator.typalign);
            Ok(Some(ArrayIterateItem::Scalar {
                value,
                isnull: false,
            }))
        }
    } else {
        // Slice case: build and return an array of the requested size.
        let mut p = iterator.data_ptr;
        // Record per-element byte windows for the by-ref construct path; the
        // workspace `slice_values` carries the C `fetch_att` Datums (mirroring
        // the persistent workspace), and the windows feed `construct_md_array`'s
        // by-reference element bytes exactly as `array_replace_internal` does.
        let mut windows: PgVec<'mcx, Option<&'mcx [u8]>> =
            vec_with_capacity_in(mcx, iterator.slice_len.max(0) as usize)?;
        for i in 0..iterator.slice_len as usize {
            let is_null = foundation::array_get_isnull(
                iterator.arr,
                iterator.nullbitmap,
                iterator.current_item,
            );
            iterator.current_item += 1;
            if is_null {
                iterator.slice_nulls[i] = true;
                iterator.slice_values[i] = Datum::null();
                windows.push(None);
            } else {
                iterator.slice_nulls[i] = false;
                iterator.slice_values[i] = foundation::fetch_att(
                    iterator.arr,
                    p,
                    iterator.typbyval,
                    iterator.typlen as i32,
                );
                // Move our data pointer forward to the next element.
                let after =
                    foundation::att_addlength_pointer(p, iterator.typlen as i32, iterator.arr, p);
                if !iterator.typbyval {
                    // The element window is a sub-slice of the iterator's `'mcx`
                    // array buffer, so slicing already yields a `&'mcx [u8]` (the
                    // by-ref element convention shared with
                    // `array_unnest`/`array_replace`); no raw-pointer rebuild.
                    let window: &'mcx [u8] = &iterator.arr[p..after];
                    windows.push(Some(window));
                } else {
                    windows.push(None);
                }
                p = foundation::att_align_nominal(after, iterator.typalign);
            }
        }
        iterator.data_ptr = p;

        // result = construct_md_array(values, nulls, slice_ndim, slice_dims,
        //                             slice_lbound, ARR_ELEMTYPE(arr),
        //                             typlen, typbyval, typalign);
        let elmtype = foundation::arr_elemtype(iterator.arr);
        // The by-reference element Datums handed to construct_md_array must carry
        // the element bytes (the construct family reads them via the detoast
        // seam, keyed by the pointer word); recover the pointer word from each
        // saved window exactly as `elem_as_datum` does on the rebuild path.
        let values: PgVec<'mcx, Datum> = {
            let mut v = vec_with_capacity_in::<Datum>(mcx, iterator.slice_len.max(0) as usize)?;
            for i in 0..iterator.slice_len as usize {
                if let Some(w) = windows[i] {
                    v.push(Datum::from_usize(w.as_ptr() as usize));
                } else {
                    v.push(iterator.slice_values[i]);
                }
            }
            v
        };
        let result = construct::construct_md_array(
            mcx,
            &values,
            Some(&iterator.slice_nulls[..]),
            iterator.slice_ndim,
            &iterator.slice_dims,
            &iterator.slice_lbound,
            elmtype,
            iterator.typlen as i32,
            iterator.typbyval,
            iterator.typalign,
        )?;

        // *isnull = false; *value = PointerGetDatum(result);
        // The owned byte model returns the freshly built sub-array buffer by
        // value rather than as a dangling pointer word.
        Ok(Some(ArrayIterateItem::Slice(result)))
    }
}

/// `array_free_iterator(iterator)` (arrayfuncs.c:4765): release an
/// `ArrayIteratorData`.
///
/// The C frees the slice workspace (`slice_values`/`slice_nulls`) and the
/// iterator itself with `pfree`. In the owned model the iterator and its
/// `mcx`-allocated workspace are dropped when ownership ends; consuming
/// `iterator` by value mirrors the C teardown (drop runs the `PgVec`
/// destructors, releasing the workspace).
pub fn array_free_iterator(iterator: ArrayIteratorData<'_>) {
    // if (iterator->slice_ndim > 0) { pfree(slice_values); pfree(slice_nulls); }
    // pfree(iterator);
    drop(iterator);
}

// ---------------------------------------------------------------------------
// array_map (arrayfuncs.c) is split at the per-element ExecEvalExpr boundary:
// its front half (deconstruct the source array) and back half (assemble the
// coerced result reusing the source dims) live in `construct.rs`
// (`array_map_deconstruct` / `array_map_build`) and are installed as the
// `array_map_*` seams; the interpreter (execExprInterp's ExecEvalArrayCoerce)
// runs the per-element transform loop between them, since `ExecEvalExpr` /
// `ExprState` / `ExprContext` live in the executor.
// ---------------------------------------------------------------------------

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

/// `DatumGetPointer(value)` over a pass-by-reference (`typlen == -1`) element
/// `Datum`: the verbatim varlena image at the pointer word, bounded by
/// `VARSIZE_ANY`, handed to the detoast seam. This is the boundary where the C
/// `PG_DETOAST_DATUM(value)` reads through the pointer. The owned model carries
/// a live pointer into an `mcx`-owned varlena image in the Datum word (datum.c's
/// `Datum` contract), so the deref is sound.
fn datum_as_byte_window<'a>(value: Datum) -> &'a [u8] {
    use types_datum::varlena::VARHDRSZ;
    unsafe {
        let p = value.as_usize() as *const u8;
        // Read the leading header word to compute VARSIZE_ANY, then return the
        // full varlena image.
        let head = core::slice::from_raw_parts(p, VARHDRSZ);
        let total = foundation::varsize_any(head, 0);
        core::slice::from_raw_parts(p, total)
    }
}

#[cfg(test)]
mod iterator_tests {
    use super::*;
    use mcx::MemoryContext;
    use std::sync::{Mutex, MutexGuard};

    /// `array_get_n_items` is a process-global seam (`OnceLock`-backed). The
    /// `if !is_installed() { set(..) }` guard below is check-then-act: under
    /// parallel `cargo test`, two tests can both observe "not installed" and
    /// race — one hits `set`-twice ("seam installed twice"), or a `call`
    /// observes the slot mid-install ("seam not installed"). Each test that
    /// installs/uses this seam holds this lock for its full body so
    /// install->use is atomic w.r.t. the sibling tests.
    static SEAM_MUTEX: Mutex<()> = Mutex::new(());

    fn lock_arrayutils_seams() -> MutexGuard<'static, ()> {
        SEAM_MUTEX.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Install the pure `arrayutils.c` integer-math seams the iterator drives
    /// (`ArrayGetNItems`), with the faithful overflow-checked product. Idempotent
    /// across tests via the seam's set-once semantics tolerating re-set.
    /// Callers must hold [`lock_arrayutils_seams`] for the duration.
    fn install_arrayutils_seams() {
        // ArrayGetNItems(ndim, dims): product of dims (bounded by MaxArraySize in
        // C; the small fixtures here never approach it). `set` is set-once, so
        // guard against the second test installing it again.
        if !arrayutils::array_get_n_items::is_installed() {
            arrayutils::array_get_n_items::set(|ndim: i32, dims: &[i32]| {
                let mut ret: i32 = 1;
                for i in 0..ndim.max(0) as usize {
                    ret = ret.checked_mul(dims[i]).expect("ArrayGetNItems overflow");
                }
                Ok(ret)
            });
        }
    }

    /// Build a 1-D no-nulls `int4[]` buffer holding `vals` (lbound 1).
    fn build_int4_array(vals: &[i32]) -> Vec<u8> {
        let ndim = 1i32;
        let dataoff = foundation::arr_overhead_nonulls(ndim); // MAXALIGN(16+8)=24
        let total = dataoff + vals.len() * 4;
        let mut a = vec![0u8; total];
        foundation::set_header(&mut a, total, ndim, 0, foundation::INT4OID);
        foundation::write_dims(&mut a, &[vals.len() as i32]);
        foundation::write_lbounds(&mut a, ndim, &[1]);
        for (i, &v) in vals.iter().enumerate() {
            let off = dataoff + i * 4;
            a[off..off + 4].copy_from_slice(&v.to_ne_bytes());
        }
        a
    }

    #[test]
    fn element_iteration_int4() {
        let _guard = lock_arrayutils_seams();
        install_arrayutils_seams();
        let ctx = MemoryContext::new("array_iterate_test");
        let mcx = ctx.mcx();

        let buf = build_int4_array(&[10, 20, 30]);
        let mstate = lsyscache::TypLenByValAlign {
            typlen: 4,
            typbyval: true,
            typalign: b'i' as i8,
        };
        let mut it = array_create_iterator(mcx, &buf, 0, Some(mstate)).unwrap();

        let mut got = Vec::new();
        while let Some(item) = array_iterate(mcx, &mut it).unwrap() {
            match item {
                ArrayIterateItem::Scalar { value, isnull } => {
                    assert!(!isnull);
                    got.push(value.as_i32());
                }
                ArrayIterateItem::Slice(_) => panic!("unexpected slice item"),
            }
        }
        assert_eq!(got, vec![10, 20, 30]);
        // No more items after exhaustion.
        assert!(array_iterate(mcx, &mut it).unwrap().is_none());
        array_free_iterator(it);
    }

    #[test]
    fn invalid_slice_ndim_errors() {
        let _guard = lock_arrayutils_seams();
        install_arrayutils_seams();
        let ctx = MemoryContext::new("array_iterate_test2");
        let mcx = ctx.mcx();
        let buf = build_int4_array(&[1, 2]);
        let mstate = lsyscache::TypLenByValAlign {
            typlen: 4,
            typbyval: true,
            typalign: b'i' as i8,
        };
        // slice_ndim > ARR_NDIM(arr) (== 1) is rejected.
        assert!(array_create_iterator(mcx, &buf, 2, Some(mstate)).is_err());
        // negative slice_ndim is rejected.
        assert!(array_create_iterator(mcx, &buf, -1, Some(mstate)).is_err());
    }
}
