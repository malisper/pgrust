//! Port of `src/backend/utils/adt/array_userfuncs.c` — the SQL-callable array
//! "user" functions (`array_cat`, `array_position`, `array_position_start`,
//! `array_positions`, `array_shuffle`, `array_sample`, `array_reverse`,
//! `array_sort` + order/nulls_first; `array_append`/`array_prepend` and the
//! `array_agg` family STOP on the keystones noted below).
//!
//! `array_userfuncs.c` is a distinct `.c` unit, but its functions operate on the
//! same `ArrayType` byte machinery this crate already owns (construct/deconstruct,
//! the iterator, `array_set_element`, the element-equality fmgr dispatch), so the
//! port lives here, alongside `arrayfuncs.c`, exactly as the C build links them
//! into the same translation set.
//!
//! # fmgr boundary / registration
//!
//! All five ported functions take/return by-reference types (`anyarray`,
//! `anyelement`) at the fmgr boundary, so they are *body* ports — the marshalable
//! `fc_`/`register_builtin` wrappers are not written (nothing here crosses the
//! current value-typed fmgr boundary). `array_position`/`array_positions`
//! additionally dispatch the element type's equality operator through the
//! `fmgr::element_eq` seam (C `FunctionCall2Coll(typentry->eq_opr_finfo, ...)`),
//! exactly as `array_eq`/`array_remove` already do.
//!
//! # STOP: `array_append` / `array_prepend` (expanded-array-infra-blocked)
//!
//! `array_append` and `array_prepend` are *not* ported here. Both are built on
//! the **expanded-array** subsystem (`array_expanded.c`): they call
//! `fetch_array_arg_replace_nulls(fcinfo, n)` → `expand_array(...)` to obtain a
//! live, deconstructed `ExpandedArrayHeader`, then mutate it *in place* via
//! `array_set_element(EOHPGetRWDatum(&eah->hdr), ...)` (the
//! `VARATT_IS_EXTERNAL_EXPANDED` branch of `array_set_element`). The owned model
//! has none of that machinery:
//!
//!  * no concrete `ExpandedArrayHeader` carrier (only the generic
//!    `types_datum::ExpandedObject` flatten-only trait + the read-only
//!    `ExpandedObjectRef` byte handle exist);
//!  * no `EOH_init_header` + owning child `MemoryContext` model, no `EA_MAGIC`;
//!  * no `EOHPGetRWDatum` read-write-expanded-datum constructor, and the
//!    `Datum::Expanded(Box<dyn ExpandedObject>)` arm is a by-value flatten box,
//!    not a live mutable handle the set path can write through;
//!  * `element_slice::array_set_element` has no `VARATT_IS_EXTERNAL_EXPANDED`
//!    in-place-mutate path ("expanded-array dispatch handled at the caller
//!    boundary").
//!
//! This is the same wall `construct::construct_empty_expanded_array` already
//! STOPs at. Porting `array_append`/`array_prepend` would require inventing the
//! expanded-array carrier + RW-datum + in-place set path first — a separate
//! expanded-object-infra keystone, not this lane. They are left out (no hollow
//! stub) per Mirror-PG-and-panic.
//!
//! # STOP: the `array_agg` family (agg-context-channel-blocked)
//!
//! `array_agg_transfn` / `array_agg_combine` / `array_agg_serialize` /
//! `array_agg_deserialize` / `array_agg_finalfn` and the `array_agg_array_*`
//! variants are *not* ported. Their aggregate transition type is `internal` — a
//! bare `ArrayBuildState *` (or `ArrayBuildStateArr *`) carried through
//! `nodeAgg.c` as a pass-by-value Datum word. Two things make them un-invokable
//! in the owned model:
//!
//!  * `array_agg_transfn` calls `AggCheckCallContext(fcinfo, &aggcontext)` and
//!    *requires* the returned `aggcontext` to `initArrayResult(arg1_typeid,
//!    aggcontext, false)` (the build state, and every accumulated element copy,
//!    must live in the per-aggregate context, not the per-tuple context). The
//!    `nodeAgg` transition dispatch threads transfns by `fn_oid` through
//!    `function_call_invoke_datum` and explicitly does **not** carry the fcinfo
//!    `(Node *) aggstate` context (the deferred K2 re-sign — see
//!    `backend-executor-nodeAgg::transition`), so a transfn that reads
//!    `AggCheckCallContext` cannot obtain its context. Unlike count/sum/avg
//!    (which don't), `array_agg` cannot run without it.
//!  * the `internal` transition state would ride in `Datum::Internal(Box<dyn
//!    Any>)`, but the by-OID transition dispatch has no way to construct/thread
//!    that boxed state in the per-aggregate context.
//!
//! Porting the bodies now would produce a shell no caller can reach (no fmgr/agg
//! wiring threads the context). They are left out (no hollow stub) until the
//! agg-context channel (nodeAgg K2) lands; the bodies are then a mechanical
//! `ArrayBuildState`-over-`accumArrayResult` port (the build-state machinery
//! they need already lives in `construct`).

use mcx::{vec_with_capacity_in, Mcx, PgVec};
use types_array::ArrayElementDatum;
use types_core::Oid;
use types_datum::datum::Datum;
use types_error::{
    PgResult, ERRCODE_ARRAY_SUBSCRIPT_ERROR, ERRCODE_DATATYPE_MISMATCH,
    ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_NULL_VALUE_NOT_ALLOWED, ERRCODE_UNDEFINED_FUNCTION,
    ERROR,
};

use backend_utils_error::ereport;

use crate::construct::{self, array_contains_nulls};
use crate::foundation::{self, INT4OID};

use backend_utils_adt_arrayutils_seams as arrayutils;
use backend_utils_cache_lsyscache_seams as lsyscache;
use backend_utils_cache_typcache_seams as typcache;
use backend_utils_fmgr_fmgr_seams as fmgr;
use backend_utils_sort_tuplesort_seams as tuplesort;
use pg_prng_seams as prng;

use crate::sql;
use types_error::PgError;

/// `format_type_be(element_type)`-style identifier for the "could not identify"
/// error message; the real `format_type_be` lives in a not-yet-reachable owner,
/// so the OID is reported (same convention as `ops::format_type_be`).
fn format_type_be(element_type: Oid) -> String {
    format!("type {element_type}")
}

/// `array_cat(v1, v2)` (array_userfuncs.c:316): concatenate two nD arrays into an
/// nD array, or push an (n-1)D array onto the end of an nD array. NOT strict —
/// a NULL input returns the other input.
///
/// The C function takes/returns by-reference `anyarray`, so this is a body port:
/// the inputs are the canonical flat array byte buffers, `None` modeling the C
/// `PG_ARGISNULL`. The byte layout of the result mirrors C field-for-field
/// (`SET_VARSIZE`, `ndim`/`dataoffset`/`elemtype`, `ARR_DIMS`/`ARR_LBOUND`, the
/// two data areas concatenated, and the null bitmap copied via
/// `array_bitmap_copy`).
pub fn array_cat<'mcx>(
    mcx: Mcx<'mcx>,
    v1: Option<&[u8]>,
    v2: Option<&[u8]>,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    // Concatenating a null array is a no-op, just return the other input.
    let (v1, v2) = match (v1, v2) {
        (None, None) => return Ok(None),                 // PG_RETURN_NULL()
        (None, Some(v2)) => return Ok(Some(mcx::slice_in(mcx, v2)?)),
        (Some(v1), None) => return Ok(Some(mcx::slice_in(mcx, v1)?)),
        (Some(v1), Some(v2)) => (v1, v2),
    };

    let element_type1 = foundation::arr_elemtype(v1);
    let element_type2 = foundation::arr_elemtype(v2);

    // Check we have matching element types.
    if element_type1 != element_type2 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_DATATYPE_MISMATCH)
            .errmsg("cannot concatenate incompatible arrays")
            .errdetail(format!(
                "Arrays with element types {} and {} are not compatible for concatenation.",
                format_type_be(element_type1),
                format_type_be(element_type2)
            ))
            .into_error());
    }

    // OK, use it.
    let element_type = element_type1;

    let ndims1 = foundation::arr_ndim(v1);
    let ndims2 = foundation::arr_ndim(v2);

    // short circuit - if one input array is empty, and the other is not, we
    // return the non-empty one as the result; if both are empty, return the
    // first one.
    if ndims1 == 0 && ndims2 > 0 {
        return Ok(Some(mcx::slice_in(mcx, v2)?));
    }
    if ndims2 == 0 {
        return Ok(Some(mcx::slice_in(mcx, v1)?));
    }

    // the rest fall under rule 3, 4, or 5.
    if ndims1 != ndims2 && ndims1 != ndims2 - 1 && ndims1 != ndims2 + 1 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR)
            .errmsg("cannot concatenate incompatible arrays")
            .errdetail(format!(
                "Arrays of {ndims1} and {ndims2} dimensions are not compatible for concatenation."
            ))
            .into_error());
    }

    // get argument array details
    let nitems1 = arrayutils::array_get_n_items::call(ndims1, &dims_slice(mcx, v1)?)?;
    let nitems2 = arrayutils::array_get_n_items::call(ndims2, &dims_slice(mcx, v2)?)?;
    let ndatabytes1 = foundation::arr_size(v1) - foundation::arr_data_offset(v1);
    let ndatabytes2 = foundation::arr_size(v2) - foundation::arr_data_offset(v2);

    let ndims: i32;
    let mut dims: PgVec<'mcx, i32>;
    let mut lbs: PgVec<'mcx, i32>;

    if ndims1 == ndims2 {
        // resulting array is made up of the elements (possibly arrays
        // themselves) of the input argument arrays
        ndims = ndims1;
        dims = vec_with_capacity_in(mcx, ndims as usize)?;
        lbs = vec_with_capacity_in(mcx, ndims as usize)?;

        dims.push(foundation::arr_dim(v1, 0) + foundation::arr_dim(v2, 0));
        lbs.push(foundation::arr_lbound(v1, 0));

        for i in 1..ndims as usize {
            if foundation::arr_dim(v1, i) != foundation::arr_dim(v2, i)
                || foundation::arr_lbound(v1, i) != foundation::arr_lbound(v2, i)
            {
                return Err(incompatible_dims_err());
            }
            dims.push(foundation::arr_dim(v1, i));
            lbs.push(foundation::arr_lbound(v1, i));
        }
    } else if ndims1 == ndims2 - 1 {
        // resulting array has the second argument as the outer array, with the
        // first argument inserted at the front of the outer dimension
        ndims = ndims2;
        dims = foundation::arr_dims(mcx, v2)?;
        lbs = foundation::arr_lbounds(mcx, v2)?;

        // increment number of elements in outer array
        dims[0] += 1;

        // make sure the added element matches our existing elements
        for i in 0..ndims1 as usize {
            if foundation::arr_dim(v1, i) != dims[i + 1] || foundation::arr_lbound(v1, i) != lbs[i + 1]
            {
                return Err(incompatible_dims_err());
            }
        }
    } else {
        // (ndims1 == ndims2 + 1)
        //
        // resulting array has the first argument as the outer array, with the
        // second argument appended to the end of the outer dimension
        ndims = ndims1;
        dims = foundation::arr_dims(mcx, v1)?;
        lbs = foundation::arr_lbounds(mcx, v1)?;

        // increment number of elements in outer array
        dims[0] += 1;

        // make sure the added element matches our existing elements
        for i in 0..ndims2 as usize {
            if foundation::arr_dim(v2, i) != dims[i + 1] || foundation::arr_lbound(v2, i) != lbs[i + 1]
            {
                return Err(incompatible_dims_err());
            }
        }
    }

    // Do this mainly for overflow checking
    let nitems = arrayutils::array_get_n_items::call(ndims, &dims)?;
    arrayutils::array_check_bounds::call(ndims, &dims, &lbs)?;

    // build the result array
    let ndatabytes = ndatabytes1 + ndatabytes2;
    let dataoffset: i32;
    let nbytes: usize;
    if foundation::arr_hasnull(v1) || foundation::arr_hasnull(v2) {
        dataoffset = foundation::arr_overhead_withnulls(ndims, nitems) as i32;
        nbytes = ndatabytes + dataoffset as usize;
    } else {
        dataoffset = 0; // marker for no null bitmap
        nbytes = ndatabytes + foundation::arr_overhead_nonulls(ndims);
    }

    let mut result = vec_with_capacity_in::<u8>(mcx, nbytes)?;
    result.resize(nbytes, 0); // palloc0
    foundation::set_header(&mut result, nbytes, ndims, dataoffset, element_type);
    foundation::write_dims(&mut result, &dims[..ndims as usize]);
    foundation::write_lbounds(&mut result, ndims, &lbs[..ndims as usize]);

    // data area is arg1 then arg2.
    let dst_data = foundation::arr_data_offset(&result);
    let src1 = foundation::arr_data_offset(v1);
    let src2 = foundation::arr_data_offset(v2);
    result[dst_data..dst_data + ndatabytes1].copy_from_slice(&v1[src1..src1 + ndatabytes1]);
    result[dst_data + ndatabytes1..dst_data + ndatabytes1 + ndatabytes2]
        .copy_from_slice(&v2[src2..src2 + ndatabytes2]);

    // handle the null bitmap if needed.
    if foundation::arr_hasnull(&result) {
        let dest_bm = foundation::arr_nullbitmap_off(&result).expect("result has null bitmap");
        foundation::array_bitmap_copy(
            &mut result,
            dest_bm,
            0,
            v1,
            foundation::arr_nullbitmap_off(v1),
            0,
            nitems1,
        );
        // The second copy reads from v2 into result; do it as a separate call.
        foundation::array_bitmap_copy(
            &mut result,
            dest_bm,
            nitems1,
            v2,
            foundation::arr_nullbitmap_off(v2),
            0,
            nitems2,
        );
    }

    Ok(Some(result))
}

/// `errdetail("Arrays with differing ... dimensions ...")` for `array_cat`.
fn incompatible_dims_err() -> types_error::PgError {
    ereport(ERROR)
        .errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR)
        .errmsg("cannot concatenate incompatible arrays")
        .errdetail("Arrays with differing element dimensions are not compatible for concatenation.")
        .into_error()
}

/// Helper: `ARR_DIMS(a)` as an `&[i32]`-able owned vector (for the seam).
fn dims_slice<'mcx>(mcx: Mcx<'mcx>, a: &[u8]) -> PgResult<PgVec<'mcx, i32>> {
    foundation::arr_dims(mcx, a)
}

/// `makeArrayResult(astate, rcontext)` (arrayfuncs.c) — a one-dimensional array
/// of the accumulated elements (`ndims = nelems>0 ? 1 : 0`, `dims[0] = nelems`,
/// `lbs[0] = 1`). Mirrors the private `construct::make_array_result` via the
/// public `make_md_array_result`.
fn make_array_result<'mcx>(
    mcx: Mcx<'mcx>,
    astate: &types_datum::array_build::ArrayBuildState,
) -> PgResult<PgVec<'mcx, u8>> {
    let ndims = if astate.nelems > 0 { 1 } else { 0 };
    let dims = [astate.nelems];
    let lbs = [1];
    construct::make_md_array_result(mcx, astate, ndims, &dims, &lbs)
}

/// `array_position_common(fcinfo)` (array_userfuncs.c:1320): the shared engine
/// for `array_position` and `array_position_start`. Returns the 1-based position
/// of the first element equal to `searched_element` (or matching NULL when
/// `searched_element` is NULL), or `None` (C `PG_RETURN_NULL`).
///
/// `array` is the by-reference `anyarray` input (`None` = `PG_ARGISNULL(0)`).
/// `searched_element` is the `anyelement` to find, modeled as the canonical
/// [`ArrayElementDatum`] when present and `None` for the C null-search case.
/// `start` is the optional `array_position_start` third argument.
///
/// `collation` is the C `PG_GET_COLLATION()` threaded in (the body cannot read
/// the fcinfo). The element equality dispatch is the
/// `lookup_type_cache(TYPECACHE_EQ_OPR_FINFO)` + `FunctionCall2Coll` pair, reached
/// through `typcache::lookup_element_eq_opr` + `fmgr::element_eq` exactly as
/// `array_eq` / `array_remove` do.
fn array_position_common(
    mcx: Mcx<'_>,
    array: Option<&[u8]>,
    searched_element: Option<ArrayElementDatum<'_>>,
    start: Option<i32>,
    collation: Oid,
) -> PgResult<Option<i32>> {
    // if (PG_ARGISNULL(0)) PG_RETURN_NULL();
    let array = match array {
        None => return Ok(None),
        Some(a) => a,
    };

    // We refuse to search for elements in multi-dimensional arrays.
    if foundation::arr_ndim(array) > 1 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("searching for elements in multidimensional arrays is not supported")
            .into_error());
    }

    // Searching in an empty array is well-defined, though: it always fails.
    if foundation::arr_ndim(array) < 1 {
        return Ok(None);
    }

    // PG_ARGISNULL(1) handling.
    let null_search = match &searched_element {
        None => {
            // fast return when the array doesn't have nulls
            if !array_contains_nulls(array) {
                return Ok(None);
            }
            true
        }
        Some(_) => false,
    };

    let element_type = foundation::arr_elemtype(array);
    let mut position = foundation::arr_lbound(array, 0) - 1;

    // figure out where to start (array_position_start passes a 3rd arg, which
    // must not be NULL).
    let position_min = match start {
        Some(p) => p,
        None => foundation::arr_lbound(array, 0),
    };
    // (For array_position_start the C code raises if PG_ARGISNULL(2); the
    // caller wrapper enforces a non-null `start`, so a NULL there is reported
    // by passing `start = None` to array_position, not by this function.)
    let _ = ERRCODE_NULL_VALUE_NOT_ALLOWED;

    // Resolve the element type's equality operator and storage triple, once.
    let s = lsyscache::get_typlenbyvalalign::call(element_type)?;
    let typlen = s.typlen as i32;
    let typbyval = s.typbyval;
    let typalign = s.typalign as u8;

    let eq_opr = typcache::lookup_element_eq_opr::call(element_type)?;
    if eq_opr == 0 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_FUNCTION)
            .errmsg(format!(
                "could not identify an equality operator for type {}",
                format_type_be(element_type)
            ))
            .into_error());
    }

    // Examine each array element until we find a match. C drives this with
    // array_create_iterator + array_iterate; for a one-dimensional array (the
    // only shape allowed here) that is an element-by-element walk, which we do
    // directly so each by-reference element keeps its on-disk byte window for
    // the equality probe (the iterator's scalar arm hands back a bare fetch_att
    // offset). This is exactly the scan array_remove/array_replace_internal use.
    let nitems = arrayutils::array_get_n_items::call(1, &[foundation::arr_dim(array, 0)])?;
    let bitmap = foundation::arr_nullbitmap_off(array);
    let mut dataptr = foundation::arr_data_ptr_off(array);

    let mut found = false;
    for i in 0..nitems {
        position += 1;

        let isnull = foundation::array_get_isnull(array, bitmap, i);

        // Advance the data pointer past this element (only for non-nulls, as in
        // the C iterator).
        let value_window: Option<&[u8]> = if isnull {
            None
        } else {
            let off = dataptr;
            let after = foundation::att_addlength_pointer(off, typlen, array, off);
            let window: &[u8] = &array[off..after];
            dataptr = foundation::att_align_nominal(after, typalign);
            Some(window)
        };

        // skip initial elements if caller requested so
        if position < position_min {
            continue;
        }

        // Can't look at the array element's value if it's null; but if we
        // search for null, we have a hit and are done.
        if isnull || null_search {
            if isnull && null_search {
                found = true;
                break;
            } else {
                continue;
            }
        }

        // not nulls, so run the operator
        let elt2: ArrayElementDatum<'_> = if typbyval {
            ArrayElementDatum::ByValue(foundation::fetch_att(
                array,
                dataptr_back(value_window, array),
                typbyval,
                typlen,
            ))
        } else {
            ArrayElementDatum::ByRef(value_window.unwrap())
        };
        // searched_element is Some here (null_search == false).
        let arg0 = searched_element.unwrap();
        if fmgr::element_eq::call(eq_opr, collation, arg0, elt2)? {
            found = true;
            break;
        }
    }

    if !found {
        return Ok(None);
    }
    let _ = mcx;
    Ok(Some(position))
}

/// Recover the by-value element's `fetch_att` offset from its byte window: for a
/// by-value element the window starts at the element bytes, and `fetch_att`
/// reads the by-value word from that offset.
fn dataptr_back(window: Option<&[u8]>, array: &[u8]) -> usize {
    // window is a sub-slice of `array`; its offset is its start within `array`.
    match window {
        Some(w) => {
            // SAFETY-free pointer arithmetic on slices: the window is &array[off..],
            // so off = w.as_ptr() - array.as_ptr().
            (w.as_ptr() as usize) - (array.as_ptr() as usize)
        }
        None => 0,
    }
}

/// `array_position(array, element)` (array_userfuncs.c:1301). NOT strict.
pub fn array_position(
    mcx: Mcx<'_>,
    array: Option<&[u8]>,
    searched_element: Option<ArrayElementDatum<'_>>,
    collation: Oid,
) -> PgResult<Option<i32>> {
    array_position_common(mcx, array, searched_element, None, collation)
}

/// `array_position_start(array, element, start)` (array_userfuncs.c:1306). NOT
/// strict; `start` (the 3rd argument) must not be NULL.
pub fn array_position_start(
    mcx: Mcx<'_>,
    array: Option<&[u8]>,
    searched_element: Option<ArrayElementDatum<'_>>,
    start: Option<i32>,
    collation: Oid,
) -> PgResult<Option<i32>> {
    // if (PG_ARGISNULL(2)) ereport(ERROR, ... "initial position must not be null")
    // — only reachable when the array arg itself is non-null (C evaluates this
    // inside array_position_common after the PG_ARGISNULL(0) early-out).
    if array.is_some() && foundation::arr_ndim(array.unwrap()) >= 1 && start.is_none() {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED)
            .errmsg("initial position must not be null")
            .into_error());
    }
    array_position_common(mcx, array, searched_element, start, collation)
}

// ---------------------------------------------------------------------------
// array_shuffle / array_sample / array_reverse / array_sort
//
// All four are by-reference `anyarray` body ports: the input is the canonical
// flat array byte buffer, the result is a freshly built flat array buffer. They
// `deconstruct_array` the input into per-element value-lane Datums (so a
// by-reference element keeps its real stored bytes, NOT the bare-word offset
// surrogate), rearrange whole first-dimension *items* (`nelm` elements each),
// then `construct_md_array_values` the rearranged elements back into a buffer.
// The element storage triple comes from `lookup_type_cache(elmtyp, 0)`, exactly
// as the C `fcinfo->flinfo->fn_extra` `TypeCacheEntry` cache supplies it (the
// owned model re-looks-up each call rather than caching on the flinfo).
// ---------------------------------------------------------------------------

/// Helper: split a `Vec<(Datum, bool)>` (the value-lane `deconstruct_array`
/// output) into parallel `values`/`nulls` slices for `construct_md_array_values`.
fn split_values_nulls<'mcx>(
    mcx: Mcx<'mcx>,
    elems: &[(types_tuple::Datum<'mcx>, bool)],
) -> PgResult<(PgVec<'mcx, types_tuple::Datum<'mcx>>, PgVec<'mcx, bool>)> {
    let mut values = vec_with_capacity_in::<types_tuple::Datum<'mcx>>(mcx, elems.len())?;
    let mut nulls = vec_with_capacity_in::<bool>(mcx, elems.len())?;
    for (d, n) in elems {
        values.push(d.clone());
        nulls.push(*n);
    }
    Ok((values, nulls))
}

/// `array_shuffle_n(array, n, keep_lb, elmtyp, typentry)` (array_userfuncs.c:1612):
/// return a copy of `array` with `n` randomly chosen first-dimension items
/// (Fisher-Yates partial shuffle). The first dimension's lower bound is preserved
/// if `keep_lb`, else set to 1; lower-order dimensions are preserved.
fn array_shuffle_n<'mcx>(
    mcx: Mcx<'mcx>,
    array: &[u8],
    n: i32,
    keep_lb: bool,
    elmtyp: Oid,
    typlen: i32,
    elmbyval: bool,
    elmalign: u8,
) -> PgResult<PgVec<'mcx, u8>> {
    let ndim = foundation::arr_ndim(array);
    let dims = foundation::arr_dims(mcx, array)?;
    let lbs = foundation::arr_lbounds(mcx, array)?;

    // If the target array is empty, exit fast.
    if ndim < 1 || dims[0] < 1 || n < 1 {
        return construct::construct_empty_array(mcx, elmtyp);
    }

    let mut elems =
        construct::deconstruct_array_values(mcx, array, elmtyp, typlen, elmbyval, elmalign)?;
    let nelm_total = elems.len() as i32;

    let nitem = dims[0]; // total number of items
    let nelm = nelm_total / nitem; // number of elements per item

    debug_assert!(n <= nitem);

    // Shuffle using Fisher-Yates; swap item i (nelm datums at i*nelm) with a
    // randomly chosen later item j. Stop after n iterations.
    for i in 0..n as usize {
        let j = prng::pg_global_prng_uint64_range::call(i as u64, (nitem - 1) as u64) as i32
            * nelm;
        let j = j as usize;
        let ibase = i * nelm as usize;
        for k in 0..nelm as usize {
            elems.swap(ibase + k, j + k);
        }
    }

    // Set up dimensions of the result.
    let mut rdims = dims;
    let mut rlbs = lbs;
    rdims[0] = n;
    if !keep_lb {
        rlbs[0] = 1;
    }

    let (values, nulls) = split_values_nulls(mcx, &elems[..(n * nelm) as usize])?;
    construct::construct_md_array_values(
        mcx,
        &values,
        Some(&nulls),
        ndim,
        &rdims[..ndim as usize],
        &rlbs[..ndim as usize],
        elmtyp,
        typlen,
        elmbyval,
        elmalign,
    )
}

/// `array_shuffle(array)` (array_userfuncs.c:1701): an array with the same
/// dimensions as the input, with its first-dimension items in random order.
pub fn array_shuffle<'mcx>(mcx: Mcx<'mcx>, array: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    // There is no point in shuffling empty arrays or arrays with < 2 items.
    if foundation::arr_ndim(array) < 1 || foundation::arr_dim(array, 0) < 2 {
        return Ok(mcx::slice_in(mcx, array)?);
    }
    let elmtyp = foundation::arr_elemtype(array);
    let te = typcache::lookup_type_cache::call(elmtyp, 0)?;
    array_shuffle_n(
        mcx,
        array,
        foundation::arr_dim(array, 0),
        true,
        elmtyp,
        te.typlen as i32,
        te.typbyval,
        te.typalign as u8,
    )
}

/// `array_sample(array, n)` (array_userfuncs.c:1736): an array of `n` randomly
/// chosen first-dimension items from the input.
pub fn array_sample<'mcx>(mcx: Mcx<'mcx>, array: &[u8], n: i32) -> PgResult<PgVec<'mcx, u8>> {
    let nitem = if foundation::arr_ndim(array) < 1 {
        0
    } else {
        foundation::arr_dim(array, 0)
    };

    if n < 0 || n > nitem {
        return Err(ereport(ERROR)
            .errcode(types_error::ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!("sample size must be between 0 and {nitem}"))
            .into_error());
    }

    let elmtyp = foundation::arr_elemtype(array);
    let te = typcache::lookup_type_cache::call(elmtyp, 0)?;
    array_shuffle_n(
        mcx,
        array,
        n,
        false,
        elmtyp,
        te.typlen as i32,
        te.typbyval,
        te.typalign as u8,
    )
}

/// `array_reverse_n(array, elmtyp, typentry)` (array_userfuncs.c:1774): a copy of
/// `array` with reversed first-dimension items.
fn array_reverse_n<'mcx>(
    mcx: Mcx<'mcx>,
    array: &[u8],
    elmtyp: Oid,
    typlen: i32,
    elmbyval: bool,
    elmalign: u8,
) -> PgResult<PgVec<'mcx, u8>> {
    let ndim = foundation::arr_ndim(array);
    let dims = foundation::arr_dims(mcx, array)?;
    let lbs = foundation::arr_lbounds(mcx, array)?;

    let mut elems =
        construct::deconstruct_array_values(mcx, array, elmtyp, typlen, elmbyval, elmalign)?;
    let nelm_total = elems.len() as i32;

    let nitem = dims[0];
    let nelm = nelm_total / nitem;

    // Reverse the array: swap item i with item (nitem-i-1).
    for i in 0..(nitem / 2) as usize {
        let j = ((nitem - i as i32 - 1) * nelm) as usize;
        let ibase = i * nelm as usize;
        for k in 0..nelm as usize {
            elems.swap(ibase + k, j + k);
        }
    }

    let mut rdims = dims;
    let mut rlbs = lbs;
    rdims[0] = nitem;

    let (values, nulls) = split_values_nulls(mcx, &elems)?;
    construct::construct_md_array_values(
        mcx,
        &values,
        Some(&nulls),
        ndim,
        &rdims[..ndim as usize],
        &rlbs[..ndim as usize],
        elmtyp,
        typlen,
        elmbyval,
        elmalign,
    )
}

/// `array_reverse(array)` (array_userfuncs.c:1849): an array with the same
/// dimensions as the input, with its first-dimension items in reverse order.
pub fn array_reverse<'mcx>(mcx: Mcx<'mcx>, array: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    // There is no point in reversing empty arrays or arrays with < 2 items.
    if foundation::arr_ndim(array) < 1 || foundation::arr_dim(array, 0) < 2 {
        return Ok(mcx::slice_in(mcx, array)?);
    }
    let elmtyp = foundation::arr_elemtype(array);
    let te = typcache::lookup_type_cache::call(elmtyp, 0)?;
    array_reverse_n(
        mcx,
        array,
        elmtyp,
        te.typlen as i32,
        te.typbyval,
        te.typalign as u8,
    )
}

/// `ARRAY_LT_OP` / `ARRAY_GT_OP` (pg_operator.dat) — the array `<` / `>`
/// operators used to sort the sub-arrays of a multi-dimensional input.
const ARRAY_LT_OP: Oid = 1072;
const ARRAY_GT_OP: Oid = 1073;

/// `array_sort_internal(array, descending, nulls_first, fcinfo)`
/// (array_userfuncs.c:1882): sort the first dimension of `array`.
///
/// `collation` is the C `PG_GET_COLLATION()` threaded in. For a 1-D input the
/// element type is sorted (operator = element `<`/`>`); for an nD input the
/// sub-arrays are sorted (operator = `ARRAY_LT_OP`/`ARRAY_GT_OP`). The owned
/// model sources the things-to-be-sorted losslessly — 1-D from
/// `deconstruct_array_values` (the iterator's by-reference scalar arm hands back
/// only a byte offset, so the value-lane deconstruct is used to keep each
/// element's real bytes), nD from `array_create_iterator(ndim-1)`'s slice arm
/// (already a freshly built sub-array buffer) — then feeds them through
/// `tuplesort_begin_datum` and accumulates the sorted output via the bare-mcx
/// `accumArrayResultAny` over `CurrentMemoryContext`.
fn array_sort_internal<'mcx>(
    mcx: Mcx<'mcx>,
    array: &'mcx [u8],
    descending: bool,
    nulls_first: bool,
    collation: Oid,
) -> PgResult<PgVec<'mcx, u8>> {
    use types_tuple::backend_access_common_heaptuple::Datum as TDatum;

    let ndim = foundation::arr_ndim(array);
    let lbs = foundation::arr_lbounds(mcx, array)?;

    // Quick exit if we don't need to sort.
    if ndim < 1 || foundation::arr_dim(array, 0) < 2 {
        return Ok(mcx::slice_in(mcx, array)?);
    }

    let elmtyp = foundation::arr_elemtype(array);
    let te = typcache::lookup_type_cache::call(elmtyp, 0)?;
    let typlen = te.typlen as i32;
    let typbyval = te.typbyval;
    let typalign = te.typalign as u8;

    // Identify the sort type and operator.
    let sort_typ: Oid;
    let sort_opr: Oid;
    if ndim == 1 {
        sort_typ = elmtyp;
        let (lt_opr, _eq_opr, gt_opr, _) = typcache::sort_group_operators::call(elmtyp, false)?;
        sort_opr = if descending { gt_opr } else { lt_opr };
    } else {
        let typarray = lsyscache::get_array_type::call(elmtyp)?.unwrap_or(0);
        if typarray == 0 {
            return Err(ereport(ERROR)
                .errcode(types_error::ERRCODE_UNDEFINED_OBJECT)
                .errmsg(format!(
                    "could not find array type for data type {}",
                    format_type_be(elmtyp)
                ))
                .into_error());
        }
        sort_typ = typarray;
        sort_opr = if descending { ARRAY_GT_OP } else { ARRAY_LT_OP };
    }

    if sort_opr == 0 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(format!(
                "could not identify a comparison function for type {}",
                format_type_be(elmtyp)
            ))
            .into_error());
    }

    // Put the things to be sorted (elements or sub-arrays) into a tuplesort.
    let work_mem = backend_utils_misc_guc_tables::vars::work_mem.read();
    let mut tss = tuplesort::tuplesort_begin_datum::call(
        mcx,
        sort_typ,
        sort_opr,
        collation,
        nulls_first,
        work_mem,
        0, // TUPLESORT_NONE
    )?;

    if ndim == 1 {
        // C: array_create_iterator(array, 0, &mstate) — element walk. Sourced
        // here from the value-lane deconstruct to keep by-reference element bytes.
        let elems =
            construct::deconstruct_array_values(mcx, array, elmtyp, typlen, typbyval, typalign)?;
        for (value, isnull) in elems.iter() {
            tuplesort::tuplesort_putdatum::call(&mut tss, value.clone(), *isnull)?;
        }
    } else {
        // C: array_create_iterator(array, ndim-1, &mstate) — slice walk.
        let mstate = lsyscache::TypLenByValAlign {
            typlen: typlen as i16,
            typbyval,
            typalign: typalign as i8,
        };
        let mut iter = sql::array_create_iterator(mcx, array, ndim - 1, Some(mstate))?;
        while let Some(item) = sql::array_iterate(mcx, &mut iter)? {
            match item {
                sql::ArrayIterateItem::Slice(buf) => {
                    let d = TDatum::ByRef(buf);
                    tuplesort::tuplesort_putdatum::call(&mut tss, d, false)?;
                }
                sql::ArrayIterateItem::Scalar { .. } => {
                    // Unreachable for slice_ndim > 0, but mirror the C: a slice
                    // iterator never yields a scalar.
                    return Err(PgError::error("array_sort: slice iterator yielded a scalar")
                        .with_sqlstate(types_error::ERRCODE_INTERNAL_ERROR));
                }
            }
        }
    }

    // Do the sort.
    tuplesort::tuplesort_performsort::call(&mut tss)?;

    // Extract the sorted things into a new array.
    let mut newarray: PgVec<'mcx, u8>;
    if ndim == 1 {
        // makeArrayResultAny over the scalar (element) state, collected directly
        // into the value lane so by-reference elements keep their real bytes
        // (the scalar accumArrayResult's pointer-word by-ref copy bottoms out on
        // the global-address-space detoast resolution the owned model lacks).
        let mut values: PgVec<'mcx, TDatum<'mcx>> = PgVec::new_in(mcx);
        let mut nulls: PgVec<'mcx, bool> = PgVec::new_in(mcx);
        loop {
            let (found, value, isnull) =
                tuplesort::tuplesort_getdatum::call(&mut tss, true, false)?;
            if !found {
                break;
            }
            values.push(value);
            nulls.push(isnull);
        }
        tuplesort::tuplesort_end::call(mcx::alloc_in(mcx, tss)?)?;

        let nelems = values.len() as i32;
        let nd = if nelems > 0 { 1 } else { 0 };
        newarray = construct::construct_md_array_values(
            mcx,
            &values,
            Some(&nulls),
            nd,
            &[nelems],
            &[lbs[0]],
            sort_typ,
            typlen,
            typbyval,
            typalign,
        )?;
    } else {
        // Multi-dimensional: the sorted things are sub-arrays; rebuild the nD
        // array via accumArrayResultAny (the array case).
        let mut astate: Option<types_datum::array_build::ArrayBuildStateAny> = None;
        loop {
            let (found, value, _isnull) =
                tuplesort::tuplesort_getdatum::call(&mut tss, true, false)?;
            if !found {
                break;
            }
            let bytes: &[u8] = match &value {
                TDatum::ByRef(b) => &b[..],
                _ => {
                    return Err(PgError::error(
                        "array_sort: sub-array sort output is not a by-ref array",
                    )
                    .with_sqlstate(types_error::ERRCODE_INTERNAL_ERROR))
                }
            };
            astate = Some(construct::accum_array_result_any_mcx(
                mcx,
                astate.take(),
                Datum::null(),
                Some(bytes),
                false,
                sort_typ,
            )?);
        }
        tuplesort::tuplesort_end::call(mcx::alloc_in(mcx, tss)?)?;

        let astate =
            astate.unwrap_or(construct::init_array_result_any_mcx(sort_typ, true)?);
        newarray = construct::make_array_result_any_mcx(mcx, &astate)?;
        // Adjust lower bound to match the input.
        let nd = foundation::arr_ndim(&newarray);
        foundation::write_lbounds(&mut newarray, nd, &[lbs[0]]);
    }

    Ok(newarray)
}

/// `array_sort(array)` (array_userfuncs.c:2004): sort the first dimension in
/// ascending order, NULLs last.
pub fn array_sort<'mcx>(mcx: Mcx<'mcx>, array: &'mcx [u8], collation: Oid) -> PgResult<PgVec<'mcx, u8>> {
    array_sort_internal(mcx, array, false, false, collation)
}

/// `array_sort_order(array, descending)` (array_userfuncs.c:2015).
pub fn array_sort_order<'mcx>(
    mcx: Mcx<'mcx>,
    array: &'mcx [u8],
    descending: bool,
    collation: Oid,
) -> PgResult<PgVec<'mcx, u8>> {
    array_sort_internal(mcx, array, descending, descending, collation)
}

/// `array_sort_order_nulls_first(array, descending, nulls_first)`
/// (array_userfuncs.c:2027).
pub fn array_sort_order_nulls_first<'mcx>(
    mcx: Mcx<'mcx>,
    array: &'mcx [u8],
    descending: bool,
    nulls_first: bool,
    collation: Oid,
) -> PgResult<PgVec<'mcx, u8>> {
    array_sort_internal(mcx, array, descending, nulls_first, collation)
}

/// `array_positions(array, element)` (array_userfuncs.c:1475): an int4 array of
/// the 1-based positions of every element equal to `searched_element` (IS NOT
/// DISTINCT FROM semantics). Returns `None` when the input array is NULL, and an
/// empty int4 array when the value is not found. NOT strict.
///
/// Returns the result as the flat int4-array byte buffer (the C
/// `makeArrayResult(astate, ...)`); a body port (`anyarray` arg, `int[]` result).
pub fn array_positions<'mcx>(
    mcx: Mcx<'mcx>,
    array: Option<&[u8]>,
    searched_element: Option<ArrayElementDatum<'_>>,
    collation: Oid,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    // if (PG_ARGISNULL(0)) PG_RETURN_NULL();
    let array = match array {
        None => return Ok(None),
        Some(a) => a,
    };

    // We refuse to search for elements in multi-dimensional arrays.
    if foundation::arr_ndim(array) > 1 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("searching for elements in multidimensional arrays is not supported")
            .into_error());
    }

    // astate = initArrayResult(INT4OID, CurrentMemoryContext, false);
    let mut astate = Some(construct::init_array_result(INT4OID, false)?);

    // Searching in an empty array is well-defined, though: it always fails.
    if foundation::arr_ndim(array) < 1 {
        return Ok(Some(make_array_result(mcx, &astate.take().unwrap())?));
    }

    // PG_ARGISNULL(1) handling.
    let null_search = match &searched_element {
        None => {
            if !array_contains_nulls(array) {
                return Ok(Some(make_array_result(mcx, &astate.take().unwrap())?));
            }
            true
        }
        Some(_) => false,
    };

    let element_type = foundation::arr_elemtype(array);
    let mut position = foundation::arr_lbound(array, 0) - 1;

    let s = lsyscache::get_typlenbyvalalign::call(element_type)?;
    let typlen = s.typlen as i32;
    let typbyval = s.typbyval;
    let typalign = s.typalign as u8;

    let eq_opr = typcache::lookup_element_eq_opr::call(element_type)?;
    if eq_opr == 0 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_FUNCTION)
            .errmsg(format!(
                "could not identify an equality operator for type {}",
                format_type_be(element_type)
            ))
            .into_error());
    }

    // Examine each array element (one-dimensional walk, as in array_position).
    let nitems = arrayutils::array_get_n_items::call(1, &[foundation::arr_dim(array, 0)])?;
    let bitmap = foundation::arr_nullbitmap_off(array);
    let mut dataptr = foundation::arr_data_ptr_off(array);

    for i in 0..nitems {
        position += 1;

        let isnull = foundation::array_get_isnull(array, bitmap, i);

        let value_window: Option<&[u8]> = if isnull {
            None
        } else {
            let off = dataptr;
            let after = foundation::att_addlength_pointer(off, typlen, array, off);
            let window: &[u8] = &array[off..after];
            dataptr = foundation::att_align_nominal(after, typalign);
            Some(window)
        };

        // Can't look at the array element's value if it's null; but if we
        // search for null, we have a hit.
        let matched = if isnull || null_search {
            isnull && null_search
        } else {
            let elt2: ArrayElementDatum<'_> = if typbyval {
                ArrayElementDatum::ByValue(foundation::fetch_att(
                    array,
                    dataptr_back(value_window, array),
                    typbyval,
                    typlen,
                ))
            } else {
                ArrayElementDatum::ByRef(value_window.unwrap())
            };
            let arg0 = searched_element.unwrap();
            fmgr::element_eq::call(eq_opr, collation, arg0, elt2)?
        };

        if matched {
            // accumArrayResult(astate, Int32GetDatum(position), false, INT4OID, ...)
            astate = Some(construct::accum_array_result(
                mcx,
                astate.take(),
                Datum::from_usize(position as u32 as usize),
                false,
                INT4OID,
            )?);
        }
    }

    Ok(Some(make_array_result(mcx, &astate.take().unwrap())?))
}
