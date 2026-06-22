//! Construct family: `construct_array` / `construct_md_array` /
//! `construct_empty_array` / `deconstruct_array` plus the
//! `initArrayResult*` / `accumArrayResult*` / `makeArrayResult*` build-state
//! accumulators.
//!
//! This family OWNS the inward `backend-utils-adt-arrayfuncs-seams` and is the
//! source of the functions [`crate::init_seams`] installs. The public function
//! signatures below match those seam signatures exactly.

extern crate alloc;

use mcx::{Mcx, PgString, PgVec};
use types_array::{ArrayType, ARRAYTYPE_HDRSZ, MAXDIM};
use types_core::Oid;
use types_datum::array_build::{ArrayBuildState, ArrayBuildStateArr};
use types_datum::datum::Datum;
use types_error::{
    PgError, PgResult, ERRCODE_ARRAY_SUBSCRIPT_ERROR, ERRCODE_DATATYPE_MISMATCH,
    ERRCODE_INTERNAL_ERROR, ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_NULL_VALUE_NOT_ALLOWED,
    ERRCODE_PROGRAM_LIMIT_EXCEEDED,
};
use types_nodes::{EStateData, EcxtId};
use types_tuple::heaptuple::ItemPointerData;

use backend_utils_adt_arrayfuncs_seams::{ArrayBuildCtx, ArrayBuildStateAnyHandle};

use crate::foundation::{self, MAX_ALLOC_SIZE};

// Outward seams to unported neighbors.
use backend_access_common_detoast_seams as detoast_seam;
use backend_utils_adt_arrayutils_seams as arrayutils_seam;
use backend_utils_cache_lsyscache_seams as lsyscache_seam;

// `TYPALIGN_*` codes (`catalog/pg_type_d.h`), reused from the builtin tables.
const TYPALIGN_CHAR: u8 = b'c';
const TYPALIGN_SHORT: u8 = b's';
const TYPALIGN_INT: u8 = b'i';
const TYPALIGN_DOUBLE: u8 = b'd';

// ---------------------------------------------------------------------------
// construct_* / deconstruct_* (arrayfuncs.c) — in-process API (no seam).
// ---------------------------------------------------------------------------

/// `construct_array(elems, nelems, elmtype, elmlen, elmbyval, elmalign)`
/// (arrayfuncs.c): build a one-dimensional array from element `Datum`s.
pub fn construct_array<'mcx>(
    mcx: Mcx<'mcx>,
    elems: &[Datum],
    elmtype: Oid,
    elmlen: i32,
    elmbyval: bool,
    elmalign: u8,
) -> PgResult<PgVec<'mcx, u8>> {
    // C:
    //   int dims[1]; int lbs[1];
    //   dims[0] = nelems; lbs[0] = 1;
    //   return construct_md_array(elems, NULL, 1, dims, lbs, ...);
    let nelems = elems.len() as i32;
    let dims = [nelems];
    let lbs = [1];
    construct_md_array(
        mcx, elems, None, 1, &dims, &lbs, elmtype, elmlen, elmbyval, elmalign,
    )
}

/// `construct_md_array(elems, nulls, ndims, dims, lbs, elmtype, elmlen,
/// elmbyval, elmalign)` (arrayfuncs.c): build a multi-dimensional array.
pub fn construct_md_array<'mcx>(
    mcx: Mcx<'mcx>,
    elems: &[Datum],
    nulls: Option<&[bool]>,
    ndims: i32,
    dims: &[i32],
    lbs: &[i32],
    elmtype: Oid,
    elmlen: i32,
    elmbyval: bool,
    elmalign: u8,
) -> PgResult<PgVec<'mcx, u8>> {
    if ndims < 0 {
        // we do allow zero-dimension arrays
        return Err(PgError::error(format!(
            "invalid number of dimensions: {ndims}"
        ))
        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
    }
    if ndims > MAXDIM {
        return Err(PgError::error(format!(
            "number of array dimensions ({ndims}) exceeds the maximum allowed ({MAXDIM})"
        ))
        .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED));
    }

    // This checks for overflow of the array dimensions.
    let nelems = arrayutils_seam::array_get_n_items::call(ndims, dims)?;
    arrayutils_seam::array_check_bounds::call(ndims, dims, lbs)?;

    // if ndims <= 0 or any dims[i] == 0, return empty array
    if nelems <= 0 {
        return construct_empty_array(mcx, elmtype);
    }

    // compute required space; collect detoasted (where needed) element bytes
    // and their aligned data offsets in a single pass, exactly as C does.
    let mut nbytes: i32 = 0;
    let mut hasnulls = false;
    // Per-element prepared data bytes (None for nulls / by-value handled
    // inline by store).
    for i in 0..nelems as usize {
        if nulls.map(|n| n[i]).unwrap_or(false) {
            hasnulls = true;
            continue;
        }
        // make sure data is not toasted (varlena element type)
        // (detoast happens at the data-copy stage via element bytes; the
        // length contribution mirrors att_addlength_datum)
        nbytes = att_addlength_datum(mcx, nbytes, elmlen, elmbyval, elems[i])?;
        nbytes = foundation::att_align_nominal(nbytes as usize, elmalign) as i32;
        // check for overflow of total request
        if !alloc_size_is_valid(nbytes) {
            return Err(PgError::error(format!(
                "array size exceeds the maximum allowed ({MAX_ALLOC_SIZE})"
            ))
            .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED));
        }
    }

    // Allocate and initialize result array.
    let dataoffset: i32;
    if hasnulls {
        dataoffset = foundation::arr_overhead_withnulls(ndims, nelems) as i32;
        nbytes += dataoffset;
    } else {
        dataoffset = 0; // marker for no null bitmap
        nbytes += foundation::arr_overhead_nonulls(ndims) as i32;
    }

    let total = nbytes as usize;
    let mut result = mcx::vec_with_capacity_in::<u8>(mcx, total)?;
    result.resize(total, 0); // palloc0

    // SET_VARSIZE + header fields.
    foundation::set_header(&mut result, total, ndims, dataoffset, elmtype);
    // memcpy(ARR_DIMS(result), dims, ndims * sizeof(int));
    foundation::write_dims(&mut result, &dims[..ndims as usize]);
    // memcpy(ARR_LBOUND(result), lbs, ndims * sizeof(int));
    foundation::write_lbounds(&mut result, ndims, &lbs[..ndims as usize]);

    // CopyArrayEls(result, elems, nulls, nelems, elmlen, elmbyval, elmalign,
    //              false /* freedata */)
    copy_array_els(
        mcx, &mut result, elems, nulls, nelems, elmlen, elmbyval, elmalign,
    )?;

    Ok(result)
}

/// `construct_empty_array(elmtype)` (arrayfuncs.c): a zero-dimensional array.
pub fn construct_empty_array<'mcx>(mcx: Mcx<'mcx>, elmtype: Oid) -> PgResult<PgVec<'mcx, u8>> {
    // result = palloc0(sizeof(ArrayType));
    // SET_VARSIZE(result, sizeof(ArrayType));
    // result->ndim = 0; result->dataoffset = 0; result->elemtype = elmtype;
    let total = ARRAYTYPE_HDRSZ;
    let mut result = mcx::vec_with_capacity_in::<u8>(mcx, total)?;
    result.resize(total, 0);
    foundation::set_header(&mut result, total, 0, 0, elmtype);
    Ok(result)
}

/// `construct_empty_expanded_array(element_type, parentcontext, metacache)`
/// (arrayfuncs.c:3597): build the flat empty array, then hand it to
/// `expand_array` to produce an `ExpandedArrayHeader`.
///
/// C:
/// ```c
/// ArrayType *array = construct_empty_array(element_type);
/// d = expand_array(PointerGetDatum(array), parentcontext, metacache);
/// pfree(array);
/// return (ExpandedArrayHeader *) DatumGetEOHP(d);
/// ```
/// The flat empty array is built in-crate; the `expand_array` step belongs to
/// the expanded-array subsystem (`array_expanded.c`), which is not ported —
/// mirror PG and panic loudly at that boundary rather than invent the
/// `ExpandedArrayHeader` vocabulary (consistent with
/// `array_get_element_expanded` / `array_set_element_expanded`).
pub fn construct_empty_expanded_array<'mcx>(
    mcx: Mcx<'mcx>,
    element_type: Oid,
) -> PgResult<()> {
    let _array = construct_empty_array(mcx, element_type)?;
    panic!(
        "construct_empty_expanded_array: expand_array / ExpandedArrayHeader belong to the \
         expanded-array subsystem (array_expanded.c), which is not ported"
    )
}

/// `deconstruct_array(array, elmtype, elmlen, elmbyval, elmalign, &elemsp,
/// &nullsp, &nelemsp)` (arrayfuncs.c): split an array buffer into per-element
/// `(Datum, isnull)` pairs.
///
/// The C `nullsp != NULL` form is modeled by always returning the is-null flag
/// alongside each Datum; the `nullsp == NULL` (null-disallowing) form is not
/// distinguished here — callers that cannot tolerate a null inspect the
/// returned flags. (The owner exposes the null-disallowing built-in flavors
/// through `deconstruct_text_array` / `deconstruct_tid_array`, which forward a
/// real `nullsp`.)
pub fn deconstruct_array<'mcx>(
    mcx: Mcx<'mcx>,
    array: &[u8],
    elmtype: Oid,
    elmlen: i32,
    elmbyval: bool,
    elmalign: u8,
) -> PgResult<PgVec<'mcx, (Datum, bool)>> {
    // Assert(ARR_ELEMTYPE(array) == elmtype);
    debug_assert_eq!(foundation::arr_elemtype(array), elmtype);

    // nelems = ArrayGetNItems(ARR_NDIM(array), ARR_DIMS(array));
    let ndim = foundation::arr_ndim(array);
    let dims = foundation::arr_dims(mcx, array)?;
    let nelems = arrayutils_seam::array_get_n_items::call(ndim, &dims)?;

    let mut out = mcx::vec_with_capacity_in::<(Datum, bool)>(mcx, nelems as usize)?;

    // p = ARR_DATA_PTR(array); bitmap = ARR_NULLBITMAP(array); bitmask = 1;
    let mut p = foundation::arr_data_ptr_off(array);
    let bitmap = foundation::arr_nullbitmap_off(array);
    let mut bitmap_byte = bitmap;
    let mut bitmask: i32 = 1;

    for _ in 0..nelems {
        // Get source element, checking for NULL.
        let is_null_here = match bitmap_byte {
            Some(b) => (array[b] as i32 & bitmask) == 0,
            None => false,
        };
        if is_null_here {
            out.push((Datum::null(), true));
        } else {
            // elems[i] = fetch_att(p, elmbyval, elmlen);
            let d = foundation::fetch_att(array, p, elmbyval, elmlen);
            // p = att_addlength_pointer(p, elmlen, p);
            p = foundation::att_addlength_pointer(p, elmlen, array, p);
            // p = (char *) att_align_nominal(p, elmalign);
            p = foundation::att_align_nominal(p, elmalign);
            out.push((d, false));
        }

        // advance bitmap pointer if any
        if let Some(b) = bitmap_byte.as_mut() {
            bitmask <<= 1;
            if bitmask == 0x100 {
                *b += 1;
                bitmask = 1;
            }
        }
    }

    Ok(out)
}

/// `deconstruct_array(array, elmtype, elmlen, elmbyval, elmalign, ...)`
/// (arrayfuncs.c) over the 6-arm value lane: yield each element as a real
/// [`types_tuple::Datum<'mcx>`] value, NOT the bare-word pointer surrogate that
/// [`deconstruct_array`] produces for a by-reference element.
///
/// This is the read-side counterpart of [`construct_md_array_values`]. The C
/// `deconstruct_array` stores a by-reference element's *address into the array
/// buffer* in its output Datum (`fetch_att` → `PointerGetDatum`); the owned
/// model has no global address space, so [`deconstruct_array`]'s by-reference
/// output Datum carries only the in-buffer offset and is not dereferenceable by
/// a consumer. Here, instead, each element is materialized into the canonical
/// value lane: a by-value element becomes [`types_tuple::Datum::ByVal`] (the
/// `fetch_att` scalar word); a by-reference element becomes
/// [`types_tuple::Datum::ByRef`] carrying the element's verbatim stored bytes
/// copied out of the array data area (varlena incl. its natural header for
/// `elmlen == -1`; the fixed `elmlen` bytes for `elmlen > 0`; the
/// NUL-terminated image for `elmlen == -2`). The element walk
/// (`fetch_att` / `att_addlength_pointer` / `att_align_nominal`, null-bitmap
/// handling) is byte-for-byte identical to [`deconstruct_array`].
pub fn deconstruct_array_values<'mcx>(
    mcx: Mcx<'mcx>,
    array: &[u8],
    elmtype: Oid,
    elmlen: i32,
    elmbyval: bool,
    elmalign: u8,
) -> PgResult<PgVec<'mcx, (types_tuple::Datum<'mcx>, bool)>> {
    use types_tuple::backend_access_common_heaptuple::Datum as TDatum;

    // Assert(ARR_ELEMTYPE(array) == elmtype);
    debug_assert_eq!(foundation::arr_elemtype(array), elmtype);

    let ndim = foundation::arr_ndim(array);
    let dims = foundation::arr_dims(mcx, array)?;
    let nelems = arrayutils_seam::array_get_n_items::call(ndim, &dims)?;

    let mut out =
        mcx::vec_with_capacity_in::<(TDatum<'mcx>, bool)>(mcx, nelems as usize)?;

    // p = ARR_DATA_PTR(array); bitmap = ARR_NULLBITMAP(array); bitmask = 1;
    let mut p = foundation::arr_data_ptr_off(array);
    let bitmap = foundation::arr_nullbitmap_off(array);
    let mut bitmap_byte = bitmap;
    let mut bitmask: i32 = 1;

    for _ in 0..nelems {
        let is_null_here = match bitmap_byte {
            Some(b) => (array[b] as i32 & bitmask) == 0,
            None => false,
        };
        if is_null_here {
            out.push((TDatum::null(), true));
        } else if elmbyval {
            // By-value: fetch_att reads `elmlen` bytes into the scalar word.
            let word = foundation::fetch_att(array, p, true, elmlen).as_usize();
            p = foundation::att_addlength_pointer(p, elmlen, array, p);
            p = foundation::att_align_nominal(p, elmalign);
            out.push((TDatum::ByVal(word), false));
        } else {
            // By-reference: the element occupies `[p .. next)` (before the
            // per-element alignment padding). Copy that exact span into the
            // canonical ByRef value (the faithful idiomatic stand-in for C's
            // bare pointer into the array buffer — the bytes the C pointer would
            // address, captured by value).
            let next = foundation::att_addlength_pointer(p, elmlen, array, p);
            let bytes = array.get(p..next).ok_or_else(|| {
                PgError::error("malformed array (truncated element data)")
                    .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR)
            })?;
            out.push((TDatum::ByRef(slice_to_pgvec(mcx, bytes)?), false));
            p = foundation::att_align_nominal(next, elmalign);
        }

        if let Some(b) = bitmap_byte.as_mut() {
            bitmask <<= 1;
            if bitmask == 0x100 {
                *b += 1;
                bitmask = 1;
            }
        }
    }

    Ok(out)
}

/// `deconstruct_array_values` over an on-disk array byte image: detoast the
/// array varlena (`DatumGetArrayTypeP`), then run the value-lane element walk.
/// The owned-model entry point for reading a by-reference array *column* Datum
/// (carried as [`types_tuple::Datum::ByRef`]) into real per-element values.
pub fn deconstruct_array_values_bytes<'mcx>(
    mcx: Mcx<'mcx>,
    bytes: &[u8],
    elmtype: Oid,
    elmlen: i16,
    elmbyval: bool,
    elmalign: core::ffi::c_char,
) -> PgResult<PgVec<'mcx, (types_tuple::Datum<'mcx>, bool)>> {
    let arr = detoast_seam::detoast_attr::call(mcx, bytes)?;
    deconstruct_array_values(mcx, &arr, elmtype, elmlen as i32, elmbyval, elmalign as u8)
}

/// `construct_array(values, 1, elmtype, elmlen, elmbyval, elmalign)` over the
/// canonical value lane, returning the on-disk array varlena bytes — the
/// constructive inverse of [`deconstruct_array_values_bytes`]. The seam-typed
/// wrapper over [`construct_array_values`] (bridging the seam's
/// `i16`/`c_char` metadata to the impl's `i32`/`u8`).
pub fn construct_array_values_bytes<'mcx>(
    mcx: Mcx<'mcx>,
    elems: &[types_tuple::Datum<'mcx>],
    elmtype: Oid,
    elmlen: i16,
    elmbyval: bool,
    elmalign: core::ffi::c_char,
) -> PgResult<PgVec<'mcx, u8>> {
    construct_array_values(mcx, elems, elmtype, elmlen as i32, elmbyval, elmalign as u8)
}

/// `deconstruct_array(DatumGetArrayTypeP(arraydatum), elmtype, elmlen, elmbyval,
/// elmalign, ...)` (arrayfuncs.c) over the canonical unified value type
/// ([`types_tuple::Datum`]). The array `Datum` arrives as a pass-by-reference
/// [`types_tuple::Datum::ByRef`] carrying the verbatim on-disk array varlena
/// bytes; this detoasts it (`DatumGetArrayTypeP`) and runs the value-lane
/// element walk, returning each element as a real `(types_tuple::Datum, isnull)`
/// pair (the array_typanalyze MCELEM path needs the elements as carried values,
/// not bare words). Mirrors [`deconstruct_array_values_bytes`].
pub fn deconstruct_array_v<'mcx>(
    mcx: Mcx<'mcx>,
    arraydatum: types_tuple::backend_access_common_heaptuple::Datum<'mcx>,
    elmtype: Oid,
    elmlen: i16,
    elmbyval: bool,
    elmalign: core::ffi::c_char,
) -> PgResult<PgVec<'mcx, (types_tuple::backend_access_common_heaptuple::Datum<'mcx>, bool)>> {
    // DatumGetArrayTypeP(arraydatum): detoast the array varlena from the
    // by-reference value's verbatim stored bytes.
    let arr = detoast_seam::detoast_attr::call(mcx, arraydatum.as_ref_bytes())?;
    deconstruct_array_values(mcx, &arr, elmtype, elmlen as i32, elmbyval, elmalign as u8)
}

/// Seam adapter for `deconstruct_array(DatumGetArrayTypeP(arraydatum), elmtype,
/// elmlen, elmbyval, elmalign, ...)` (arrayfuncs.c). The seam takes the raw
/// array `Datum` and the element's `(typlen, typbyval, typalign)` exactly as
/// `pg_type` records them (`int16` len, `char` align); this wrapper detoasts
/// the array varlena (`DatumGetArrayTypeP`) and widens the storage attrs to the
/// in-process [`deconstruct_array`]'s `(i32, u8)` shape, mirroring PG.
pub fn deconstruct_array_seam<'mcx>(
    mcx: Mcx<'mcx>,
    arraydatum: Datum,
    elmtype: Oid,
    elmlen: i16,
    elmbyval: bool,
    elmalign: core::ffi::c_char,
) -> PgResult<PgVec<'mcx, (Datum, bool)>> {
    // DatumGetArrayTypeP(arraydatum) — detoast the array varlena.
    let arr = detoast_seam::detoast_attr::call(mcx, datum_as_byte_window(arraydatum))?;
    deconstruct_array(
        mcx,
        &arr,
        elmtype,
        elmlen as i32,
        elmbyval,
        elmalign as u8,
    )
}

/// `array_map`'s front half (arrayfuncs.c:3200): `DatumGetAnyArrayP(arrayd)`
/// (detoast), read the input array's `AARR_NDIM`/`AARR_DIMS`/`AARR_LBOUND`, and
/// deconstruct the whole array into its flat per-element `(Datum, isnull)` list.
/// The interpreter applies the per-element transform over the returned `elems`
/// and assembles the result with [`array_map_build`].
pub fn array_map_deconstruct<'mcx>(
    mcx: Mcx<'mcx>,
    arraydatum: types_tuple::backend_access_common_heaptuple::Datum<'mcx>,
) -> PgResult<backend_utils_adt_arrayfuncs_seams::ArrayMapSource<'mcx>> {
    // AnyArrayType *v = DatumGetAnyArrayP(arrayd);  (detoast the flat array).
    let arr = detoast_seam::detoast_attr::call(mcx, arraydatum.as_ref_bytes())?;

    // ndim = AARR_NDIM(v); dim = AARR_DIMS(v); lbound = AARR_LBOUND(v).
    let ndim = foundation::arr_ndim(&arr);
    let dims = foundation::arr_dims(mcx, &arr)?;
    let lbs = foundation::arr_lbounds(mcx, &arr)?;

    // inpType = AARR_ELEMTYPE(v); get_typlenbyvalalign(inpType, ...) — the input
    // element type's storage attrs (array_map's inp_extra refresh).
    let inp_type = foundation::arr_elemtype(&arr);
    let inp = lsyscache_seam::get_typlenbyvalalign::call(inp_type)?;
    let elems = deconstruct_array_values(
        mcx,
        &arr,
        inp_type,
        inp.typlen as i32,
        inp.typbyval,
        inp.typalign as u8,
    )?;

    Ok(backend_utils_adt_arrayfuncs_seams::ArrayMapSource {
        ndim,
        dims,
        lbs,
        elems,
    })
}

/// `array_map`'s back half (arrayfuncs.c:3200): assemble the coerced result
/// array from the transformed element values, reusing the source array's
/// `ndim`/`dims`/`lbs` (C `memcpy(ARR_DIMS(result), AARR_DIMS(v), ...)` /
/// `ARR_LBOUND`). An empty source (`nitems <= 0`) yields
/// `construct_empty_array(retType)` — exactly what `construct_md_array_values`
/// returns when `ArrayGetNItems(ndim, dims) <= 0`.
pub fn array_map_build<'mcx>(
    mcx: Mcx<'mcx>,
    ndim: i32,
    dims: &[i32],
    lbs: &[i32],
    values: &[types_tuple::backend_access_common_heaptuple::Datum<'mcx>],
    nulls: &[bool],
    ret_type: Oid,
    ret_typlen: i16,
    ret_typbyval: bool,
    ret_typalign: core::ffi::c_char,
) -> PgResult<PgVec<'mcx, u8>> {
    construct_md_array_values(
        mcx,
        values,
        Some(nulls),
        ndim,
        dims,
        lbs,
        ret_type,
        ret_typlen as i32,
        ret_typbyval,
        ret_typalign as u8,
    )
}

/// The binary-compatible `ExecEvalArrayCoerce` branch (execExprInterp.c):
/// `array = DatumGetArrayTypePCopy(arraydatum); ARR_ELEMTYPE(array) =
/// resultelemtype;`. Detoast + copy the array varlena and rewrite only its
/// `elemtype` header field; the element data is left untouched (the types are
/// binary-coercible). The copied image becomes the result `Datum`.
pub fn array_coerce_relabel<'mcx>(
    mcx: Mcx<'mcx>,
    arraydatum: types_tuple::backend_access_common_heaptuple::Datum<'mcx>,
    resultelemtype: Oid,
) -> PgResult<PgVec<'mcx, u8>> {
    // DatumGetArrayTypePCopy(arraydatum): detoast, then take a private copy.
    let arr = detoast_seam::detoast_attr::call(mcx, arraydatum.as_ref_bytes())?;
    let mut result = slice_to_pgvec(mcx, &arr)?;
    // ARR_ELEMTYPE(array) = resultelemtype; — rewrite the elemtype header field
    // (offset 12) in place, leaving size/ndim/dataoffset/dims/data untouched.
    result[12..16].copy_from_slice(&resultelemtype.to_ne_bytes());
    Ok(result)
}

/// `deconstruct_array_builtin(array, elmtype, &elemsp, &nullsp, &nelemsp)`
/// (arrayfuncs.c:3697): the convenience wrapper over [`deconstruct_array`] for
/// the handful of built-in element types whose `(typlen, typbyval, typalign)`
/// are hard-coded (avoiding a syscache lookup). Unsupported `elmtype` raises
/// the C `elog(ERROR, "type %u not supported …")`.
pub fn deconstruct_array_builtin<'mcx>(
    mcx: Mcx<'mcx>,
    array: &[u8],
    elmtype: Oid,
) -> PgResult<PgVec<'mcx, (Datum, bool)>> {
    let (elmlen, elmbyval, elmalign) = deconstruct_builtin_meta(elmtype)?;
    deconstruct_array(mcx, array, elmtype, elmlen, elmbyval, elmalign)
}

/// `array_contains_nulls(array)` (arrayfuncs.c): whether any element is null.
///
/// Operates over the verbatim varlena bytes. Panics only on a structurally
/// impossible bitmap (mirrors the C `Assert`/`ArrayGetNItems` invariants); a
/// malformed `ArrayGetNItems` is surfaced by the caller through the seam, so
/// here we recompute it directly off the header dims (the C does the same with
/// the non-fallible internal form).
pub fn array_contains_nulls(array: &[u8]) -> bool {
    // Easy answer if there's no null bitmap.
    if !foundation::arr_hasnull(array) {
        return false;
    }

    // nelems = ArrayGetNItems(ARR_NDIM(array), ARR_DIMS(array));
    let ndim = foundation::arr_ndim(array);
    let mut nelems: i32 = if ndim <= 0 { 0 } else { 1 };
    for i in 0..ndim as usize {
        nelems = nelems.wrapping_mul(foundation::arr_dim(array, i));
    }

    let mut byte = match foundation::arr_nullbitmap_off(array) {
        Some(b) => b,
        None => return false,
    };

    // check whole bytes of the bitmap byte-at-a-time
    while nelems >= 8 {
        if array[byte] != 0xFF {
            return true;
        }
        byte += 1;
        nelems -= 8;
    }

    // check last partial byte
    let mut bitmask: i32 = 1;
    while nelems > 0 {
        if (array[byte] as i32 & bitmask) == 0 {
            return true;
        }
        bitmask <<= 1;
        nelems -= 1;
    }

    false
}

// ---------------------------------------------------------------------------
// CopyArrayEls / att_addlength_datum / AllocSizeIsValid helpers (arrayfuncs.c /
// c.h). In-crate (no seam): pure byte math over the result buffer + element
// Datums, using the foundation byte primitives.
// ---------------------------------------------------------------------------

/// `AllocSizeIsValid(size)` (memutils.h): `0 <= size <= MaxAllocSize`.
fn alloc_size_is_valid(size: i32) -> bool {
    size >= 0 && (size as usize) <= MAX_ALLOC_SIZE
}

/// `att_addlength_datum(cur_offset, attlen, attdatum)` (tupmacs.h): grow a
/// running byte offset by one element's stored length.
///
/// For pass-by-value and fixed-length pass-by-ref the length is `attlen`. For
/// varlena (`attlen == -1`) and cstring (`attlen == -2`) the length is read
/// from the datum's payload bytes; in the owned model the varlena element is
/// detoasted first (mirroring `construct_md_array`'s
/// `PG_DETOAST_DATUM(elems[i])`).
fn att_addlength_datum<'mcx>(
    mcx: Mcx<'mcx>,
    cur_offset: i32,
    attlen: i32,
    _attbyval: bool,
    attdatum: Datum,
) -> PgResult<i32> {
    if attlen > 0 {
        // fixed length
        return Ok(cur_offset + attlen);
    }
    // Variable-length: the datum carries the element bytes (verbatim varlena /
    // cstring) as its payload. Detoast a varlena first.
    let bytes = datum_payload_bytes(mcx, attlen, attdatum)?;
    let add = if attlen == -1 {
        foundation::varsize_any(&bytes, 0) as i32
    } else {
        // attlen == -2: cstring, length is strlen+1
        (cstring_len(&bytes) + 1) as i32
    };
    Ok(cur_offset + add)
}

/// `CopyArrayEls(array, values, isnull, nitems, typlen, typbyval, typalign,
/// freedata)` (arrayfuncs.c): copy element Datums into the array's data area.
fn copy_array_els<'mcx>(
    mcx: Mcx<'mcx>,
    result: &mut PgVec<'mcx, u8>,
    values: &[Datum],
    isnull: Option<&[bool]>,
    nitems: i32,
    typlen: i32,
    typbyval: bool,
    typalign: u8,
) -> PgResult<()> {
    // char *p = ARR_DATA_PTR(a);
    let mut p = foundation::arr_data_ptr_off(result);
    // bits8 *bitmap = ARR_NULLBITMAP(a); int bitval = 0; int bitmask = 1;
    let bitmap_off = foundation::arr_nullbitmap_off(result);
    let mut bitval: i32 = 0;
    let mut bitmask: i32 = 1;
    // current bitmap byte index
    let mut bm_byte = bitmap_off;

    for i in 0..nitems as usize {
        if isnull.map(|n| n[i]).unwrap_or(false) {
            // bitval stays clear for this bit; mark bitmap if present below.
            // (data not written)
        } else {
            // p += ArrayCastAndSet(values[i], typlen, typbyval, typalign, p);
            p = array_cast_and_set(mcx, result, p, values[i], typlen, typbyval, typalign)?;
            if bitmap_off.is_some() {
                bitval |= bitmask;
            }
        }
        if let Some(b) = bm_byte {
            bitmask <<= 1;
            if bitmask == 0x100 {
                result[b] = bitval as u8;
                bm_byte = Some(b + 1);
                bitval = 0;
                bitmask = 1;
            }
        }
    }
    // flush trailing partial byte
    if let Some(b) = bm_byte {
        if bitmask != 1 {
            result[b] = bitval as u8;
        }
    }
    Ok(())
}

/// `ArrayCastAndSet(src, typlen, typbyval, typalign, dest)` (arrayfuncs.c):
/// store one element `src` Datum into `dest` and return the advanced offset.
fn array_cast_and_set<'mcx>(
    mcx: Mcx<'mcx>,
    result: &mut PgVec<'mcx, u8>,
    mut dest: usize,
    src: Datum,
    typlen: i32,
    typbyval: bool,
    typalign: u8,
) -> PgResult<usize> {
    let inc: usize;
    if typlen > 0 {
        if typbyval {
            // store_att_byval(dest, src, typlen);
            foundation::store_att_byval(result, dest, src, typlen);
        } else {
            // memmove(dest, DatumGetPointer(src), typlen);
            let bytes = datum_payload_bytes(mcx, typlen, src)?;
            result[dest..dest + typlen as usize].copy_from_slice(&bytes[..typlen as usize]);
        }
        inc = typlen as usize;
    } else {
        // varlena (-1) or cstring (-2): copy the variable-length payload.
        let bytes = datum_payload_bytes(mcx, typlen, src)?;
        let len = if typlen == -1 {
            foundation::varsize_any(&bytes, 0)
        } else {
            cstring_len(&bytes) + 1
        };
        result[dest..dest + len].copy_from_slice(&bytes[..len]);
        inc = len;
    }
    // return att_align_nominal(inc, typalign);
    let aligned = foundation::att_align_nominal(inc, typalign);
    dest += aligned;
    Ok(dest)
}

/// Read the verbatim payload bytes a pass-by-ref element `Datum` points at,
/// detoasting a varlena (`attlen == -1`) first (mirrors `PG_DETOAST_DATUM`).
///
/// In the owned model a pass-by-ref `Datum` carries the pointer word into a
/// caller-owned varlena/cstring/fixed-length image (`DatumGetPointer`); this
/// dereferences that pointer and copies the element's stored bytes into `mcx`,
/// detoasting a varlena (`attlen == -1`) through the detoast seam first (mirrors
/// `PG_DETOAST_DATUM`). The length to read is keyed off `attlen` exactly as
/// `att_addlength_pointer` / `datumGetSize`: `attlen > 0` reads `attlen` bytes;
/// `attlen == -1` reads `VARSIZE_ANY` bytes (then detoasts); `attlen == -2`
/// reads the NUL-terminated cstring image (`strlen + 1`).
fn datum_payload_bytes<'mcx>(mcx: Mcx<'mcx>, attlen: i32, src: Datum) -> PgResult<PgVec<'mcx, u8>> {
    if attlen == -1 {
        // Varlena: read the verbatim varlena image at the pointer word and route
        // it through the detoast seam (which materializes the detoasted bytes in
        // `mcx`), exactly as `construct_md_array`'s `PG_DETOAST_DATUM(elems[i])`.
        return detoast_seam::detoast_attr::call(mcx, datum_as_byte_window(src));
    }
    // Fixed-length by-ref (`attlen > 0`) or cstring (`attlen == -2`): copy the
    // exact stored image at the pointer word.
    let bytes = unsafe { datum_byref_image(attlen, src) };
    slice_to_pgvec(mcx, bytes)
}

/// `DatumGetPointer(src)` over the bytes a pass-by-ref `Datum`'s pointer word
/// addresses, bounded by `VARSIZE_ANY` for a varlena (`attlen == -1`),
/// `strlen + 1` for a cstring (`attlen == -2`), or `attlen` bytes for a
/// fixed-length by-ref type. `src` must hold a live pointer into an `mcx`-owned
/// image (e.g. `cstring_bytes_to_text_datum` / `datum_from_buf`).
///
/// # Safety
/// `src` is a by-reference `Datum` whose pointer word targets a live image
/// spanning at least the computed length (datum.c's `Datum` contract).
unsafe fn datum_byref_image<'a>(attlen: i32, src: Datum) -> &'a [u8] {
    if attlen == -1 {
        return datum_varlena_image(src);
    }
    let p = src.as_usize() as *const u8;
    let len = if attlen == -2 {
        // strlen + 1 over the NUL-terminated cstring image.
        let mut n = 0usize;
        while *p.add(n) != 0 {
            n += 1;
        }
        n + 1
    } else {
        attlen as usize
    };
    core::slice::from_raw_parts(p, len)
}

/// `(&[u8]) view of VARSIZE_ANY(DatumGetPointer(src))` for a varlena `Datum`:
/// read the leading header word to learn the total size, then return the full
/// image. Mirrors `backend-utils-adt-scalar-datum-core`'s `varlena_image`.
///
/// # Safety
/// `src` points at a valid varlena the caller keeps alive in `mcx`.
unsafe fn datum_varlena_image<'a>(src: Datum) -> &'a [u8] {
    use types_datum::varlena::VARHDRSZ;
    let p = src.as_usize() as *const u8;
    // Read enough of the header to compute VARSIZE_ANY (the 4-byte length word
    // also covers the 1-byte short / external tag bytes).
    let head = core::slice::from_raw_parts(p, VARHDRSZ);
    let total = foundation::varsize_any(head, 0);
    core::slice::from_raw_parts(p, total)
}

/// `DatumGetArrayTypeP(arraydatum)`'s input window: the verbatim varlena bytes
/// at the array `Datum`'s pointer word (bounded by `VARSIZE_ANY`), handed to the
/// detoast seam by the `array_*` header-projection helpers.
///
/// SAFETY of the deref is upheld by the `Datum` contract (the pointer word
/// targets a live `mcx`-owned varlena); the unsafe is encapsulated here so the
/// many `array_*` header-projection call sites stay safe, exactly as they read.
fn datum_as_byte_window<'a>(src: Datum) -> &'a [u8] {
    unsafe { datum_varlena_image(src) }
}

/// `strlen(cstr)` over a NUL-terminated cstring payload (cstring elements,
/// `attlen == -2`).
fn cstring_len(bytes: &[u8]) -> usize {
    bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len())
}

// ---------------------------------------------------------------------------
// Built-in element-type metadata (construct_array_builtin /
// deconstruct_array_builtin switch tables), transcribed value-by-value.
// ---------------------------------------------------------------------------

/// `(elmlen, elmbyval, elmalign)` for `construct_array_builtin`'s switch.
fn construct_builtin_meta(elmtype: Oid) -> PgResult<(i32, bool, u8)> {
    Ok(match elmtype {
        foundation::CHAROID => (1, true, TYPALIGN_CHAR),
        foundation::CSTRINGOID => (-2, false, TYPALIGN_CHAR),
        foundation::FLOAT4OID => (4, true, TYPALIGN_INT),
        foundation::FLOAT8OID => (8, foundation::FLOAT8PASSBYVAL, TYPALIGN_DOUBLE),
        foundation::INT2OID => (2, true, TYPALIGN_SHORT),
        foundation::INT4OID => (4, true, TYPALIGN_INT),
        foundation::INT8OID => (8, foundation::FLOAT8PASSBYVAL, TYPALIGN_DOUBLE),
        foundation::NAMEOID => (foundation::NAMEDATALEN, false, TYPALIGN_CHAR),
        foundation::OIDOID | foundation::REGTYPEOID => (4, true, TYPALIGN_INT),
        foundation::TEXTOID => (-1, false, TYPALIGN_INT),
        foundation::TIDOID => (foundation::SIZEOF_ITEM_POINTER_DATA, false, TYPALIGN_SHORT),
        foundation::XIDOID => (4, true, TYPALIGN_INT),
        _ => {
            return Err(PgError::error(format!(
                "type {elmtype} not supported by construct_array_builtin()"
            )));
        }
    })
}

/// `(elmlen, elmbyval, elmalign)` for `deconstruct_array_builtin`'s switch.
fn deconstruct_builtin_meta(elmtype: Oid) -> PgResult<(i32, bool, u8)> {
    Ok(match elmtype {
        foundation::CHAROID => (1, true, TYPALIGN_CHAR),
        foundation::CSTRINGOID => (-2, false, TYPALIGN_CHAR),
        foundation::FLOAT8OID => (8, foundation::FLOAT8PASSBYVAL, TYPALIGN_DOUBLE),
        foundation::INT2OID => (2, true, TYPALIGN_SHORT),
        foundation::INT4OID => (4, true, TYPALIGN_INT),
        foundation::OIDOID => (4, true, TYPALIGN_INT),
        foundation::TEXTOID => (-1, false, TYPALIGN_INT),
        foundation::TIDOID => (foundation::SIZEOF_ITEM_POINTER_DATA, false, TYPALIGN_SHORT),
        _ => {
            return Err(PgError::error(format!(
                "type {elmtype} not supported by deconstruct_array_builtin()"
            )));
        }
    })
}

// ---------------------------------------------------------------------------
// ArrayBuildState (single-element accumulator), arrayfuncs.c.
// ---------------------------------------------------------------------------

/// `initArrayResult(element_type, rcontext, subcontext)` (arrayfuncs.c).
pub fn init_array_result(element_type: Oid, subcontext: bool) -> PgResult<ArrayBuildState> {
    // initArrayResultWithSize(element_type, rcontext, subcontext,
    //                         subcontext ? 64 : 8)
    init_array_result_with_size(element_type, subcontext, if subcontext { 64 } else { 8 })
}

/// `initArrayResultWithSize(element_type, rcontext, subcontext, initsize)`
/// (arrayfuncs.c). The owned model holds `dvalues`/`dnulls` as global-allocator
/// `Vec`s (carried in the `ArrayBuildStateAny` slot); `alen` is the capacity.
fn init_array_result_with_size(
    element_type: Oid,
    subcontext: bool,
    initsize: usize,
) -> PgResult<ArrayBuildState> {
    let tlbva = lsyscache_seam::get_typlenbyvalalign::call(element_type)?;
    Ok(ArrayBuildState {
        dvalues: Vec::with_capacity(initsize),
        dnulls: Vec::with_capacity(initsize),
        nelems: 0,
        element_type,
        typlen: tlbva.typlen,
        typbyval: tlbva.typbyval,
        typalign: tlbva.typalign as u8,
        private_cxt: subcontext,
        byref_storage: Vec::new(),
    })
}

/// `accumArrayResult(astate, dvalue, disnull, element_type, rcontext)`
/// (arrayfuncs.c): accumulate one element.
pub fn accum_array_result<'mcx>(
    mcx: Mcx<'mcx>,
    astate: Option<ArrayBuildState>,
    mut dvalue: Datum,
    disnull: bool,
    element_type: Oid,
) -> PgResult<ArrayBuildState> {
    let mut astate = match astate {
        None => init_array_result(element_type, true)?,
        Some(s) => {
            debug_assert_eq!(s.element_type, element_type);
            s
        }
    };

    // enlarge dvalues[]/dnulls[] if needed — Vec growth; the C MaxAllocSize
    // guard on alen*sizeof(Datum) is preserved.
    if astate.nelems as usize >= astate.dvalues.capacity() {
        let new_alen = (astate.dvalues.capacity().max(1)) * 2;
        if !alloc_size_is_valid((new_alen * core::mem::size_of::<Datum>()) as i32) {
            return Err(PgError::error(format!(
                "array size exceeds the maximum allowed ({MAX_ALLOC_SIZE})"
            ))
            .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED));
        }
        astate
            .dvalues
            .reserve(new_alen - astate.dvalues.capacity());
        astate.dnulls.reserve(new_alen - astate.dnulls.capacity());
    }

    // Ensure pass-by-ref stuff is copied (and detoasted if varlena). In the
    // owned model the element bytes are materialized through the detoast seam.
    if !disnull && !astate.typbyval {
        // C: `datumCopy`s the element into the build state's `mcontext` (the
        // private subcontext when `subcontext` is true) and points `dvalues[]`
        // at that copy, which lives until `makeArrayResult(release=true)`
        // deletes the subcontext. We mirror that ownership by keeping the copy
        // in the state's own `byref_storage` (a stable `Box<[u8]>`) rather than
        // leaking it into the caller's `Mcx`: the caller's context (often the
        // short-lived per-tuple eval context) is then never charged, so it can
        // be reset between tuples without a dangling charge, and the copy is
        // reclaimed when the state drops. The `dvalues[]` word is the boxed
        // slice's stable pointer (read by `construct_md_array` at result time).
        let copy: Box<[u8]> = if astate.typlen == -1 {
            // PG_DETOAST_DATUM_COPY(dvalue): a fresh, flat varlena copy.
            let bytes = detoast_seam::detoast_attr::call(mcx, datum_as_byte_window(dvalue))?;
            bytes.as_slice().to_vec().into_boxed_slice()
        } else {
            // datumCopy(dvalue, false, typlen): a fixed-len by-ref copy.
            let bytes = datum_payload_bytes(mcx, astate.typlen as i32, dvalue)?;
            bytes.as_slice().to_vec().into_boxed_slice()
        };
        dvalue = Datum::from_usize(copy.as_ptr() as usize);
        astate.byref_storage.push(copy);
    }

    astate.dvalues.push(dvalue);
    astate.dnulls.push(disnull);
    astate.nelems += 1;

    Ok(astate)
}

/// `makeArrayResult(astate, rcontext)` (arrayfuncs.c) via `makeMdArrayResult`.
fn make_array_result<'mcx>(mcx: Mcx<'mcx>, astate: &ArrayBuildState) -> PgResult<PgVec<'mcx, u8>> {
    // ndims = (astate->nelems > 0) ? 1 : 0; dims[0] = nelems; lbs[0] = 1;
    let ndims = if astate.nelems > 0 { 1 } else { 0 };
    let dims = [astate.nelems];
    let lbs = [1];
    make_md_array_result(mcx, astate, ndims, &dims, &lbs)
}

/// `makeMdArrayResult(astate, ndims, dims, lbs, rcontext, release)`
/// (arrayfuncs.c). `release` (context delete) is modeled by the caller dropping
/// the state.
pub fn make_md_array_result<'mcx>(
    mcx: Mcx<'mcx>,
    astate: &ArrayBuildState,
    ndims: i32,
    dims: &[i32],
    lbs: &[i32],
) -> PgResult<PgVec<'mcx, u8>> {
    construct_md_array(
        mcx,
        &astate.dvalues,
        Some(&astate.dnulls),
        ndims,
        dims,
        lbs,
        astate.element_type,
        astate.typlen as i32,
        astate.typbyval,
        astate.typalign,
    )
}

// ---------------------------------------------------------------------------
// ArrayBuildStateArr (sub-array accumulator), arrayfuncs.c.
// ---------------------------------------------------------------------------

/// `initArrayResultArr(array_type, element_type, rcontext, subcontext)`
/// (arrayfuncs.c).
pub(crate) fn init_array_result_arr(
    array_type: Oid,
    element_type: Oid,
    subcontext: bool,
) -> PgResult<ArrayBuildStateArr> {
    // Lookup element type, unless element_type already provided.
    let element_type = if element_type == 0 {
        match lsyscache_seam::get_element_type::call(array_type)? {
            Some(et) if et != 0 => et,
            _ => {
                return Err(PgError::error(format!(
                    "data type {array_type} is not an array type"
                ))
                .with_sqlstate(ERRCODE_DATATYPE_MISMATCH));
            }
        }
    } else {
        element_type
    };

    // MemoryContextAllocZero — all fields zero.
    let astate = ArrayBuildStateArr {
        data: Vec::new(),
        nullbitmap: None,
        nbytes: 0,
        nitems: 0,
        ndims: 0,
        dims: [0; MAXDIM as usize],
        lbs: [0; MAXDIM as usize],
        array_type,
        element_type,
        private_cxt: subcontext,
    };
    Ok(astate)
}

/// `accumArrayResultArr(astate, dvalue, disnull, array_type, rcontext)`
/// (arrayfuncs.c): accumulate one sub-array.
pub(crate) fn accum_array_result_arr<'mcx>(
    mcx: Mcx<'mcx>,
    astate: Option<ArrayBuildStateArr>,
    dvalue: Datum,
    disnull: bool,
    array_type: Oid,
) -> PgResult<ArrayBuildStateArr> {
    // We disallow accumulating null subarrays.
    if disnull {
        return Err(
            PgError::error("cannot accumulate null arrays").with_sqlstate(ERRCODE_NULL_VALUE_NOT_ALLOWED)
        );
    }

    // Detoast input array in caller's context.
    let arg = detoast_seam::detoast_attr::call(mcx, datum_as_byte_window(dvalue))?;

    let mut astate = match astate {
        None => init_array_result_arr(array_type, 0, true)?,
        Some(s) => {
            debug_assert_eq!(s.array_type, array_type);
            s
        }
    };

    // Collect this input's dimensions.
    let ndims = foundation::arr_ndim(&arg);
    let nitems = arrayutils_seam::array_get_n_items::call(ndims, &arr_dims_vec(&arg))?;
    let ndatabytes = (foundation::arr_size(&arg) - foundation::arr_data_offset(&arg)) as i32;
    let data_off = foundation::arr_data_ptr_off(&arg);

    if astate.ndims == 0 {
        // First input; check/save the dimensionality info.
        if ndims == 0 {
            return Err(PgError::error("cannot accumulate empty arrays")
                .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR));
        }
        if ndims + 1 > MAXDIM {
            return Err(PgError::error(format!(
                "number of array dimensions ({}) exceeds the maximum allowed ({MAXDIM})",
                ndims + 1
            ))
            .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED));
        }

        // The output array will have n+1 dimensions, the ones after the first
        // matching the input's dimensions.
        astate.ndims = ndims + 1;
        astate.dims[0] = 0;
        for i in 0..ndims as usize {
            astate.dims[i + 1] = foundation::arr_dim(&arg, i);
        }
        astate.lbs[0] = 1;
        for i in 0..ndims as usize {
            astate.lbs[i + 1] = foundation::arr_lbound(&arg, i);
        }

        // Allocate at least enough data space for this item.
        let abytes = pg_nextpower2_32(core::cmp::max(1024, ndatabytes + 1));
        astate.data = Vec::with_capacity(abytes as usize);
    } else {
        // Second or later input: must match first input's dimensionality.
        if astate.ndims != ndims + 1 {
            return Err(
                PgError::error("cannot accumulate arrays of different dimensionality")
                    .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
            );
        }
        for i in 0..ndims as usize {
            if astate.dims[i + 1] != foundation::arr_dim(&arg, i)
                || astate.lbs[i + 1] != foundation::arr_lbound(&arg, i)
            {
                return Err(
                    PgError::error("cannot accumulate arrays of different dimensionality")
                        .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
                );
            }
        }
        // Vec growth subsumes the explicit abytes/repalloc enlargement.
    }

    // Copy the data portion of the sub-array.
    astate
        .data
        .extend_from_slice(&arg[data_off..data_off + ndatabytes as usize]);
    astate.nbytes += ndatabytes;

    // Deal with null bitmap if needed.
    if astate.nullbitmap.is_some() || foundation::arr_hasnull(&arg) {
        let newnitems = astate.nitems + nitems;

        if astate.nullbitmap.is_none() {
            // First input with nulls; retrospectively mark all previous items
            // non-null.
            let aitems = pg_nextpower2_32(core::cmp::max(256, newnitems + 1));
            let mut bm = Vec::new();
            bm.resize(((aitems + 7) / 8) as usize, 0u8);
            astate.nullbitmap = Some(bm);
            // array_bitmap_copy(nullbitmap, 0, NULL, 0, astate->nitems)
            let prev = astate.nitems;
            let bm = astate.nullbitmap.as_mut().unwrap();
            array_bitmap_copy_local(bm, 0, None, &[], 0, prev);
        } else {
            // Vec growth subsumes the aitems/repalloc enlargement; ensure room.
            let needed = ((newnitems + 7) / 8) as usize;
            let bm = astate.nullbitmap.as_mut().unwrap();
            if bm.len() < needed {
                bm.resize(needed, 0u8);
            }
        }
        // array_bitmap_copy(nullbitmap, astate->nitems, ARR_NULLBITMAP(arg), 0,
        //                   nitems)
        let dest_off = astate.nitems;
        let src_bm = foundation::arr_nullbitmap_off(&arg);
        let bm = astate.nullbitmap.as_mut().unwrap();
        array_bitmap_copy_local(bm, dest_off, src_bm, &arg, 0, nitems);
    }

    astate.nitems += nitems;
    astate.dims[0] += 1;

    Ok(astate)
}

/// `makeArrayResultArr(astate, rcontext, release)` (arrayfuncs.c).
pub(crate) fn make_array_result_arr<'mcx>(
    mcx: Mcx<'mcx>,
    astate: &ArrayBuildStateArr,
) -> PgResult<PgVec<'mcx, u8>> {
    if astate.ndims == 0 {
        // No inputs, return empty array.
        return construct_empty_array(mcx, astate.element_type);
    }

    // Check for overflow of the array dimensions.
    let _ = arrayutils_seam::array_get_n_items::call(astate.ndims, &astate.dims[..astate.ndims as usize])?;
    arrayutils_seam::array_check_bounds::call(
        astate.ndims,
        &astate.dims[..astate.ndims as usize],
        &astate.lbs[..astate.ndims as usize],
    )?;

    // Compute required space.
    let dataoffset: i32;
    let mut nbytes = astate.nbytes;
    if astate.nullbitmap.is_some() {
        dataoffset = foundation::arr_overhead_withnulls(astate.ndims, astate.nitems) as i32;
        nbytes += dataoffset;
    } else {
        dataoffset = 0;
        nbytes += foundation::arr_overhead_nonulls(astate.ndims) as i32;
    }

    let total = nbytes as usize;
    let mut result = mcx::vec_with_capacity_in::<u8>(mcx, total)?;
    result.resize(total, 0);

    foundation::set_header(&mut result, total, astate.ndims, dataoffset, astate.element_type);
    foundation::write_dims(&mut result, &astate.dims[..astate.ndims as usize]);
    foundation::write_lbounds(&mut result, astate.ndims, &astate.lbs[..astate.ndims as usize]);

    // memcpy(ARR_DATA_PTR(result), astate->data, astate->nbytes);
    let data_off = foundation::arr_data_ptr_off(&result);
    result[data_off..data_off + astate.nbytes as usize]
        .copy_from_slice(&astate.data[..astate.nbytes as usize]);

    if let Some(src_bm) = astate.nullbitmap.as_ref() {
        // array_bitmap_copy(ARR_NULLBITMAP(result), 0, nullbitmap, 0, nitems)
        let dest_bm_off = foundation::arr_nullbitmap_off(&result).expect("withnulls layout");
        copy_bitmap_into(&mut result, dest_bm_off, 0, src_bm, 0, astate.nitems);
    }

    Ok(result)
}

// ---------------------------------------------------------------------------
// Bare-mcx `*ArrayResultAny` over `CurrentMemoryContext` (array_sort's
// `accumArrayResultAny(..., CurrentMemoryContext)` path). The executor-bound
// `*_any` seam wrappers above force an EState/EcxtId frame; these take the
// bare `'mcx` arena directly (the byte-model image of `CurrentMemoryContext`),
// and feed the array case from already-detoasted sub-array bytes (value-lane
// `Datum::ByRef`), bypassing the global-address-space detoast resolution the
// pointer-word `accum_array_result_arr` would need.
// ---------------------------------------------------------------------------

/// `initArrayResultAny(input_type, CurrentMemoryContext, subcontext)` body
/// (arrayfuncs.c) over the bare `'mcx` arena.
pub fn init_array_result_any_mcx(
    input_type: Oid,
    subcontext: bool,
) -> PgResult<types_datum::array_build::ArrayBuildStateAny> {
    init_array_result_any_inner(input_type, subcontext)
}

/// `accumArrayResultAny(astate, dvalue, disnull, input_type, CurrentMemoryContext)`
/// (arrayfuncs.c) over the bare `'mcx` arena.
///
/// `scalar_value` is the value-lane element Datum for the scalar
/// (`get_array_type(input_type) == InvalidOid`) case; `subarray_bytes` is the
/// already-detoasted sub-array buffer for the array case. Exactly one is
/// supplied by the caller depending on `astate`'s shape (mirroring C's single
/// `dvalue` Datum, which is the element word in the scalar case and a pointer to
/// the sub-array in the array case).
pub fn accum_array_result_any_mcx<'mcx>(
    mcx: Mcx<'mcx>,
    astate: Option<types_datum::array_build::ArrayBuildStateAny>,
    scalar_value: Datum,
    subarray_bytes: Option<&[u8]>,
    disnull: bool,
    input_type: Oid,
) -> PgResult<types_datum::array_build::ArrayBuildStateAny> {
    let mut state = match astate {
        Some(s) => s,
        None => init_array_result_any_inner(input_type, true)?,
    };
    if state.scalarstate.is_some() {
        let scalar = state.scalarstate.take().unwrap();
        let scalar = accum_array_result(mcx, Some(scalar), scalar_value, disnull, input_type)?;
        state.scalarstate = Some(scalar);
    } else {
        let arr = state.arraystate.take();
        let bytes = subarray_bytes
            .expect("accum_array_result_any_mcx: array case requires sub-array bytes");
        let arr = accum_array_result_arr_bytes(mcx, arr, bytes, disnull, input_type)?;
        state.arraystate = Some(arr);
    }
    Ok(state)
}

/// `makeArrayResultAny(astate, CurrentMemoryContext, true)` (arrayfuncs.c) over
/// the bare `'mcx` arena. Returns the flat array buffer.
pub fn make_array_result_any_mcx<'mcx>(
    mcx: Mcx<'mcx>,
    astate: &types_datum::array_build::ArrayBuildStateAny,
) -> PgResult<PgVec<'mcx, u8>> {
    if let Some(scalar) = astate.scalarstate.as_ref() {
        let ndims = if scalar.nelems > 0 { 1 } else { 0 };
        let dims = [scalar.nelems];
        let lbs = [1];
        make_md_array_result(mcx, scalar, ndims, &dims, &lbs)
    } else {
        let arr = astate.arraystate.as_ref().expect("arraystate or scalarstate");
        make_array_result_arr(mcx, arr)
    }
}

/// `accumArrayResultArr` body taking the **already-detoasted** sub-array bytes
/// directly (the value-lane `Datum::ByRef` image), rather than resolving a
/// pointer word through the detoast seam. Identical accumulation logic to
/// [`accum_array_result_arr`] otherwise.
fn accum_array_result_arr_bytes<'mcx>(
    mcx: Mcx<'mcx>,
    astate: Option<ArrayBuildStateArr>,
    arg: &[u8],
    disnull: bool,
    array_type: Oid,
) -> PgResult<ArrayBuildStateArr> {
    if disnull {
        return Err(
            PgError::error("cannot accumulate null arrays").with_sqlstate(ERRCODE_NULL_VALUE_NOT_ALLOWED)
        );
    }
    let _ = mcx;

    let mut astate = match astate {
        None => init_array_result_arr(array_type, 0, true)?,
        Some(s) => {
            debug_assert_eq!(s.array_type, array_type);
            s
        }
    };

    let ndims = foundation::arr_ndim(arg);
    let nitems = arrayutils_seam::array_get_n_items::call(ndims, &arr_dims_vec(arg))?;
    let ndatabytes = (foundation::arr_size(arg) - foundation::arr_data_offset(arg)) as i32;
    let data_off = foundation::arr_data_ptr_off(arg);

    if astate.ndims == 0 {
        if ndims == 0 {
            return Err(PgError::error("cannot accumulate empty arrays")
                .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR));
        }
        if ndims + 1 > MAXDIM {
            return Err(PgError::error(format!(
                "number of array dimensions ({}) exceeds the maximum allowed ({MAXDIM})",
                ndims + 1
            ))
            .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED));
        }
        astate.ndims = ndims + 1;
        astate.dims[0] = 0;
        for i in 0..ndims as usize {
            astate.dims[i + 1] = foundation::arr_dim(arg, i);
        }
        astate.lbs[0] = 1;
        for i in 0..ndims as usize {
            astate.lbs[i + 1] = foundation::arr_lbound(arg, i);
        }
        let abytes = pg_nextpower2_32(core::cmp::max(1024, ndatabytes + 1));
        astate.data = Vec::with_capacity(abytes as usize);
    } else {
        if astate.ndims != ndims + 1 {
            return Err(
                PgError::error("cannot accumulate arrays of different dimensionality")
                    .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
            );
        }
        for i in 0..ndims as usize {
            if astate.dims[i + 1] != foundation::arr_dim(arg, i)
                || astate.lbs[i + 1] != foundation::arr_lbound(arg, i)
            {
                return Err(
                    PgError::error("cannot accumulate arrays of different dimensionality")
                        .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
                );
            }
        }
    }

    astate
        .data
        .extend_from_slice(&arg[data_off..data_off + ndatabytes as usize]);
    astate.nbytes += ndatabytes;

    if astate.nullbitmap.is_some() || foundation::arr_hasnull(arg) {
        let newnitems = astate.nitems + nitems;
        if astate.nullbitmap.is_none() {
            let aitems = pg_nextpower2_32(core::cmp::max(256, newnitems + 1));
            let mut bm = Vec::new();
            bm.resize(((aitems + 7) / 8) as usize, 0u8);
            astate.nullbitmap = Some(bm);
            let prev = astate.nitems;
            let bm = astate.nullbitmap.as_mut().unwrap();
            array_bitmap_copy_local(bm, 0, None, &[], 0, prev);
        } else {
            let needed = ((newnitems + 7) / 8) as usize;
            let bm = astate.nullbitmap.as_mut().unwrap();
            if bm.len() < needed {
                bm.resize(needed, 0u8);
            }
        }
        let dest_off = astate.nitems;
        let src_bm = foundation::arr_nullbitmap_off(arg);
        let bm = astate.nullbitmap.as_mut().unwrap();
        array_bitmap_copy_local(bm, dest_off, src_bm, arg, 0, nitems);
    }

    astate.nitems += nitems;
    astate.dims[0] += 1;

    Ok(astate)
}

// ---------------------------------------------------------------------------
// Small helpers shared by the Arr accumulator.
// ---------------------------------------------------------------------------

/// `pg_nextpower2_32(num)` (pg_bitutils.h): least power of 2 >= num (num >= 1).
fn pg_nextpower2_32(num: i32) -> i32 {
    if num <= 1 {
        return 1;
    }
    let u = num as u32;
    // 1 << (32 - leading_zeros(num - 1))
    1i32.wrapping_shl(32 - (u - 1).leading_zeros())
}

/// All `ndim` dimension lengths of an array buffer as an owned `Vec` (global
/// allocator) for passing to the arrayutils seam.
fn arr_dims_vec(a: &[u8]) -> Vec<i32> {
    let ndim = foundation::arr_ndim(a);
    let mut v = Vec::with_capacity(ndim.max(0) as usize);
    for i in 0..ndim.max(0) as usize {
        v.push(foundation::arr_dim(a, i));
    }
    v
}

/// `array_bitmap_copy` over a global-allocator destination bitmap `Vec` (the
/// `ArrayBuildStateArr` working bitmap), reading source bits from a verbatim
/// array buffer (or treating a `None` source as all-non-null).
fn array_bitmap_copy_local(
    dest: &mut [u8],
    dest_offset: i32,
    src_bitmap: Option<usize>,
    src_buf: &[u8],
    src_offset: i32,
    nitems: i32,
) {
    // Mirrors arrayfuncs.c array_bitmap_copy: bit-by-bit (the fast whole-byte
    // path is an optimization; the bitwise loop is the canonical semantics).
    let mut destbitmask: i32 = 1 << (dest_offset % 8);
    let mut destbitval: i32 = dest[(dest_offset / 8) as usize] as i32;
    let mut dest_byte = (dest_offset / 8) as usize;

    let mut srcbitmask: i32 = 1 << (src_offset % 8);
    let mut src_byte = src_bitmap.map(|b| b + (src_offset / 8) as usize);

    for _ in 0..nitems {
        let bit = match (src_bitmap, src_byte) {
            (Some(_), Some(sb)) => (src_buf[sb] as i32 & srcbitmask) != 0,
            _ => true, // NULL source => all non-null
        };
        if bit {
            destbitval |= destbitmask;
        } else {
            destbitval &= !destbitmask;
        }
        destbitmask <<= 1;
        if destbitmask == 0x100 {
            dest[dest_byte] = destbitval as u8;
            dest_byte += 1;
            destbitmask = 1;
            if dest_byte < dest.len() {
                destbitval = dest[dest_byte] as i32;
            } else {
                destbitval = 0;
            }
        }
        if src_bitmap.is_some() {
            srcbitmask <<= 1;
            if srcbitmask == 0x100 {
                srcbitmask = 1;
                src_byte = src_byte.map(|b| b + 1);
            }
        }
    }
    if destbitmask != 1 {
        dest[dest_byte] = destbitval as u8;
    }
}

/// `array_bitmap_copy` writing into a result buffer's bitmap from a working
/// bitmap (`makeArrayResultArr`).
fn copy_bitmap_into(
    dest: &mut [u8],
    dest_bitmap_off: usize,
    dest_offset: i32,
    src: &[u8],
    src_offset: i32,
    nitems: i32,
) {
    let mut destbitmask: i32 = 1 << (dest_offset % 8);
    let mut dest_byte = dest_bitmap_off + (dest_offset / 8) as usize;
    let mut destbitval: i32 = dest[dest_byte] as i32;

    let mut srcbitmask: i32 = 1 << (src_offset % 8);
    let mut src_byte = (src_offset / 8) as usize;

    for _ in 0..nitems {
        let bit = (src[src_byte] as i32 & srcbitmask) != 0;
        if bit {
            destbitval |= destbitmask;
        } else {
            destbitval &= !destbitmask;
        }
        destbitmask <<= 1;
        if destbitmask == 0x100 {
            dest[dest_byte] = destbitval as u8;
            dest_byte += 1;
            destbitmask = 1;
            destbitval = if dest_byte < dest.len() {
                dest[dest_byte] as i32
            } else {
                0
            };
        }
        srcbitmask <<= 1;
        if srcbitmask == 0x100 {
            srcbitmask = 1;
            src_byte += 1;
        }
    }
    if destbitmask != 1 {
        dest[dest_byte] = destbitval as u8;
    }
}

// ---------------------------------------------------------------------------
// Inward seam implementations (installed by crate::init_seams). The
// signatures below MUST match `backend-utils-adt-arrayfuncs-seams`.
//
// The owned model resolves the C `MemoryContext rcontext` off the EState: the
// `ArrayBuildCtx` selector picks the per-query (`es_query_cxt`) or per-tuple
// (`econtext->ecxt_per_tuple_memory`) context.
// ---------------------------------------------------------------------------

/// Resolve the target `Mcx<'mcx>` the `ArrayBuildCtx` names off the EState.
///
/// `PerQuery` is `estate.es_query_cxt`; `PerTuple` is the named ExprContext's
/// `ecxt_per_tuple_memory`. Both contexts live for the EState's `'mcx` arena
/// (the per-tuple context is a real owned child of the per-query context that
/// is reset, not freed, between tuples), so the resolved handle is valid for
/// `'mcx`. C reaches the same context through the ambient
/// `CurrentMemoryContext` / `econtext->ecxt_per_query_memory`.
fn resolve_ctx<'mcx>(
    estate: &EStateData<'mcx>,
    econtext: EcxtId,
    ctx: ArrayBuildCtx,
) -> Mcx<'mcx> {
    match ctx {
        ArrayBuildCtx::PerQuery => estate.es_query_cxt,
        ArrayBuildCtx::PerTuple => {
            // The ExprContext (and its inline per-tuple MemoryContext) is stored
            // in the EState's `'mcx`-arena `es_exprcontexts` pool, so its address
            // is stable for `'mcx`. Extend the resolved handle to `'mcx` to match
            // C's stable-pointer dereference.
            let m: Mcx<'_> = estate.ecxt(econtext).ecxt_per_tuple_memory.mcx();
            // SAFETY: the MemoryContext backing `m` lives inside the EState's
            // `'mcx` arena for the whole `'mcx`; the borrow only looked shorter
            // because it was reached through `&EStateData`. This mirrors the C
            // `econtext->ecxt_per_tuple_memory` pointer, valid for the query.
            unsafe { core::mem::transmute::<Mcx<'_>, Mcx<'mcx>>(m) }
        }
    }
}

/// Seam `init_array_result_any` — `initArrayResultAny` (arrayfuncs.c).
pub fn init_array_result_any<'mcx>(
    estate: &mut EStateData<'mcx>,
    econtext: EcxtId,
    ctx: ArrayBuildCtx,
    input_type: Oid,
) -> PgResult<ArrayBuildStateAnyHandle<'mcx>> {
    let mcx = resolve_ctx(estate, econtext, ctx);
    let astate = init_array_result_any_inner(input_type, true)?;
    Ok(Some(mcx::alloc_in(mcx, astate)?))
}

/// `initArrayResultAny(input_type, rcontext, subcontext)` body — builds the
/// scalar-or-array `ArrayBuildStateAny`.
fn init_array_result_any_inner(
    input_type: Oid,
    subcontext: bool,
) -> PgResult<types_datum::array_build::ArrayBuildStateAny> {
    use types_datum::array_build::ArrayBuildStateAny;

    // int2vector and oidvector satisfy both get_element_type and
    // get_array_type; prefer treating them as scalars => check get_array_type.
    let has_array_type = matches!(
        lsyscache_seam::get_array_type::call(input_type)?,
        Some(at) if at != 0
    );

    if !has_array_type {
        // Array case.
        let arraystate = init_array_result_arr(input_type, 0, subcontext)?;
        Ok(ArrayBuildStateAny {
            scalarstate: None,
            arraystate: Some(arraystate),
        })
    } else {
        // Scalar case.
        let scalarstate = init_array_result(input_type, subcontext)?;
        Ok(ArrayBuildStateAny {
            scalarstate: Some(scalarstate),
            arraystate: None,
        })
    }
}

/// Seam `accum_array_result_any` — `accumArrayResultAny` (arrayfuncs.c).
pub fn accum_array_result_any<'mcx>(
    estate: &mut EStateData<'mcx>,
    econtext: EcxtId,
    ctx: ArrayBuildCtx,
    astate: ArrayBuildStateAnyHandle<'mcx>,
    dvalue: types_tuple::backend_access_common_heaptuple::Datum<'mcx>,
    disnull: bool,
    input_type: Oid,
) -> PgResult<ArrayBuildStateAnyHandle<'mcx>> {
    let mcx = resolve_ctx(estate, econtext, ctx);

    // astate == NULL => initArrayResultAny(input_type, rcontext, true)
    let mut boxed = match astate {
        Some(b) => b,
        None => {
            let inner = init_array_result_any_inner(input_type, true)?;
            mcx::alloc_in(mcx, inner)?
        }
    };

    // Lower the canonical value to the bare pointer-word the inner accumulator
    // path reads. For a by-reference value this materializes a transient image
    // in `mcx`; the accumulator immediately deep-copies it into its own
    // `byref_storage`, so we hold the transient only across the accumulate call
    // and let it drop afterward (uncharging `mcx`) — never leaking it into the
    // caller's (per-tuple) context. (`lower_datum_to_word`'s leak is fine for
    // its other callers, which want the pointer to outlive the call.)
    use types_tuple::backend_access_common_heaptuple::Datum as TDatum;
    let mut held: Option<PgVec<'mcx, u8>> = None;
    let dword: Datum = if disnull {
        Datum::from_usize(0)
    } else {
        match &dvalue {
            TDatum::ByVal(w) => Datum::from_usize(*w),
            TDatum::ByRef(_) | TDatum::Cstring(_) => {
                let buf = slice_to_pgvec(mcx, dvalue.as_ref_bytes())?;
                let ptr = buf.as_ptr() as usize;
                held = Some(buf);
                Datum::from_usize(ptr)
            }
            TDatum::Composite(_) | TDatum::Expanded(_) => {
                let buf = slice_to_pgvec(mcx, &dvalue.as_varlena_bytes())?;
                let ptr = buf.as_ptr() as usize;
                held = Some(buf);
                Datum::from_usize(ptr)
            }
            TDatum::Internal(_) => {
                return Err(PgError::error(
                    "cannot accumulate an internal pseudo-type value into an array",
                )
                .with_sqlstate(ERRCODE_INTERNAL_ERROR))
            }
        }
    };

    if boxed.scalarstate.is_some() {
        let scalar = boxed.scalarstate.take().unwrap();
        let scalar = accum_array_result(mcx, Some(scalar), dword, disnull, input_type)?;
        boxed.scalarstate = Some(scalar);
    } else {
        let arr = boxed.arraystate.take();
        let arr = accum_array_result_arr(mcx, arr, dword, disnull, input_type)?;
        boxed.arraystate = Some(arr);
    }

    // Transient lowered image (if any) is reclaimed here, uncharging `mcx`.
    drop(held);

    Ok(Some(boxed))
}

/// Seam `make_array_result_any` — `makeArrayResultAny` (arrayfuncs.c).
pub fn make_array_result_any<'mcx>(
    estate: &mut EStateData<'mcx>,
    econtext: EcxtId,
    ctx: ArrayBuildCtx,
    astate: ArrayBuildStateAnyHandle<'mcx>,
) -> PgResult<Datum> {
    let result = make_array_result_any_bytes(estate, econtext, ctx, astate)?;
    // PointerGetDatum(result): the carried Datum is the buffer's pointer word.
    Ok(datum_from_buf(result))
}

/// `makeArrayResultAny(astate, ctx, true)` body — the array varlena bytes,
/// shared by the bare-word [`make_array_result_any`] and the unified-value
/// [`make_array_result_any_v`] seams.
fn make_array_result_any_bytes<'mcx>(
    estate: &mut EStateData<'mcx>,
    econtext: EcxtId,
    ctx: ArrayBuildCtx,
    astate: ArrayBuildStateAnyHandle<'mcx>,
) -> PgResult<PgVec<'mcx, u8>> {
    let mcx = resolve_ctx(estate, econtext, ctx);
    let astate = astate.expect("makeArrayResultAny: astate must not be NULL");

    if let Some(scalar) = astate.scalarstate.as_ref() {
        // Must use makeMdArrayResult to support the "release" parameter.
        let ndims = if scalar.nelems > 0 { 1 } else { 0 };
        let dims = [scalar.nelems];
        let lbs = [1];
        make_md_array_result(mcx, scalar, ndims, &dims, &lbs)
    } else {
        let arr = astate.arraystate.as_ref().expect("arraystate or scalarstate");
        make_array_result_arr(mcx, arr)
    }
}

/// Seam `make_array_result_any_v` — `makeArrayResultAny` over the unified value
/// type. An array varlena is always pass-by-reference, so the result is a
/// [`types_tuple::Datum::ByRef`] carrying the built ArrayType bytes — the
/// faithful form for `ARRAY(SELECT ...)` results that flow on into a by-ref
/// fmgr lane (e.g. `array_sort`, array equality), where a bare pointer word
/// would arrive with no by-ref-lane payload.
pub fn make_array_result_any_v<'mcx>(
    estate: &mut EStateData<'mcx>,
    econtext: EcxtId,
    ctx: ArrayBuildCtx,
    astate: ArrayBuildStateAnyHandle<'mcx>,
) -> PgResult<types_tuple::backend_access_common_heaptuple::Datum<'mcx>> {
    use types_tuple::backend_access_common_heaptuple::Datum as TDatum;
    let result = make_array_result_any_bytes(estate, econtext, ctx, astate)?;
    Ok(TDatum::ByRef(result))
}

/// Seam `pfree_array_datum` — free a previously built array `Datum`.
pub fn pfree_array_datum(
    curarray: &types_tuple::backend_access_common_heaptuple::Datum<'_>,
) {
    // pfree(DatumGetPointer(node->curArray)) guarded by != PointerGetDatum(NULL).
    // In the owned model the array buffer lives in its owning MemoryContext and
    // is reclaimed on context reset/delete; an explicit pfree of a non-null
    // pointer word is therefore a no-op here (the bytes are owned elsewhere).
    let _ = curarray;
}

/// Seam `construct_array_builtin` — `construct_array_builtin` (arrayfuncs.c).
pub fn construct_array_builtin<'mcx>(
    mcx: Mcx<'mcx>,
    elems: &[Datum],
    elmtype: Oid,
) -> PgResult<Datum> {
    let (elmlen, elmbyval, elmalign) = construct_builtin_meta(elmtype)?;
    let buf = construct_array(mcx, elems, elmtype, elmlen, elmbyval, elmalign)?;
    Ok(datum_from_buf(buf))
}

/// Seam `construct_array_builtin_v` — `construct_array_builtin` over the unified
/// value type. The array varlena is pass-by-reference, so its raw bytes are
/// carried as a [`types_tuple::Datum::ByRef`] (no bare pointer word).
pub fn construct_array_builtin_v<'mcx>(
    mcx: Mcx<'mcx>,
    elems: &[Datum],
    elmtype: Oid,
) -> PgResult<types_tuple::backend_access_common_heaptuple::Datum<'mcx>> {
    use types_tuple::backend_access_common_heaptuple::Datum as TDatum;
    let (elmlen, elmbyval, elmalign) = construct_builtin_meta(elmtype)?;
    let buf = construct_array(mcx, elems, elmtype, elmlen, elmbyval, elmalign)?;
    Ok(TDatum::ByRef(buf))
}

/// Seam `construct_md_array_like_input_v` — the `construct_md_array(...,
/// ARR_NDIM(param), ARR_DIMS(param), ARR_LBOUND(param), ...)` tail of the
/// ordered-set `percentile_*_multi_final` finalfns (orderedsetaggs.c). The
/// result array reuses the input percentile array's shape: detoast
/// `input_bytes` (`PG_GETARG_ARRAYTYPE_P`), copy its `ndim`/`dims`/`lbound`,
/// and build with the supplied row-major element words + null bitmap. The
/// element varlena is carried as a [`types_tuple::Datum::ByRef`].
pub fn construct_md_array_like_input_v<'mcx>(
    mcx: Mcx<'mcx>,
    input_bytes: &[u8],
    elems: &[Datum],
    nulls: &[bool],
    elmtype: Oid,
    elmlen: i16,
    elmbyval: bool,
    elmalign: core::ffi::c_char,
) -> PgResult<types_tuple::backend_access_common_heaptuple::Datum<'mcx>> {
    use types_tuple::backend_access_common_heaptuple::Datum as TDatum;
    let arr = detoast_seam::detoast_attr::call(mcx, input_bytes)?;
    let ndim = foundation::arr_ndim(&arr);
    let dims = foundation::arr_dims(mcx, &arr)?;
    let lbs = foundation::arr_lbounds(mcx, &arr)?;
    let buf = construct_md_array(
        mcx,
        elems,
        Some(nulls),
        ndim,
        &dims,
        &lbs,
        elmtype,
        elmlen as i32,
        elmbyval,
        elmalign as u8,
    )?;
    Ok(TDatum::ByRef(buf))
}

/// Seam `construct_array_expr` — `ExecEvalArrayExpr`'s array fabrication
/// (execExprInterp.c) over the 6-arm value lane. Dispatches to the scalar 1-D
/// `construct_md_array_values` path or the nested multi-D
/// `construct_md_array_nested` path. Returns the array varlena image; the caller
/// (the interpreter) wraps it as a `Datum::ByRef`.
pub fn construct_array_expr<'mcx>(
    mcx: Mcx<'mcx>,
    elemvalues: &[types_tuple::Datum<'mcx>],
    elemnulls: &[bool],
    elemtype: Oid,
    elemlength: i16,
    elembyval: bool,
    elemalign: u8,
    multidims: bool,
) -> PgResult<PgVec<'mcx, u8>> {
    if !multidims {
        // 1-D array of the given length: ndims = 1; dims[0] = nelems; lbs[0] = 1.
        let nelems = elemvalues.len() as i32;
        let dims = [nelems];
        let lbs = [1];
        construct_md_array_values(
            mcx,
            elemvalues,
            Some(elemnulls),
            1,
            &dims,
            &lbs,
            elemtype,
            elemlength as i32,
            elembyval,
            elemalign,
        )
    } else {
        construct_md_array_nested(mcx, elemvalues, elemnulls, elemtype)
    }
}

/// Seam `build_name_array` — `construct_array_builtin(names, n, NAMEOID)`
/// (arrayfuncs.c) over `name`-typed elements, taking each element's
/// `NAMEDATALEN`-byte `NameData` image directly (the canonical by-reference
/// payload) rather than a pointer-word `Datum` that `datum_as_byte_window`
/// would have to resolve. `NAMEOID` is fixed-length (`NAMEDATALEN`), pass-by-
/// reference, char-aligned, and these arrays never contain NULLs (matching the
/// `current_schemas` build). Mirrors `construct_array`/`construct_md_array`
/// for the 1-D, no-null, fixed-length case.
pub fn build_name_array<'mcx>(mcx: Mcx<'mcx>, elems: &[&[u8]]) -> PgResult<PgVec<'mcx, u8>> {
    let (elmlen, _elmbyval, elmalign) = construct_builtin_meta(foundation::NAMEOID)?;
    let nelems = elems.len() as i32;

    // nelems <= 0 -> construct_empty_array (matches construct_md_array).
    if nelems <= 0 {
        return construct_empty_array(mcx, foundation::NAMEOID);
    }

    // Compute total space exactly as construct_md_array: each fixed-length
    // by-reference element contributes `elmlen` bytes, aligned to `elmalign`.
    let mut nbytes: i32 = 0;
    for &img in elems {
        debug_assert_eq!(img.len(), elmlen as usize);
        nbytes = nbytes
            .checked_add(elmlen)
            .filter(|n| alloc_size_is_valid(*n))
            .ok_or_else(|| {
                PgError::error(format!(
                    "array size exceeds the maximum allowed ({MAX_ALLOC_SIZE})"
                ))
                .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
            })?;
        nbytes = foundation::att_align_nominal(nbytes as usize, elmalign) as i32;
        if !alloc_size_is_valid(nbytes) {
            return Err(PgError::error(format!(
                "array size exceeds the maximum allowed ({MAX_ALLOC_SIZE})"
            ))
            .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED));
        }
    }

    // No nulls: dataoffset == 0, overhead is the non-null header.
    let dataoffset = 0;
    nbytes += foundation::arr_overhead_nonulls(1) as i32;

    let total = nbytes as usize;
    let mut result = mcx::vec_with_capacity_in::<u8>(mcx, total)?;
    result.resize(total, 0); // palloc0

    let dims = [nelems];
    let lbs = [1];
    foundation::set_header(&mut result, total, 1, dataoffset, foundation::NAMEOID);
    foundation::write_dims(&mut result, &dims);
    foundation::write_lbounds(&mut result, 1, &lbs);

    // CopyArrayEls for fixed-length by-reference, no nulls: memcpy each
    // `elmlen`-byte image at the char-aligned data offset.
    let mut p = foundation::arr_data_ptr_off(&result);
    for &img in elems {
        result[p..p + elmlen as usize].copy_from_slice(&img[..elmlen as usize]);
        p += elmlen as usize;
        p = foundation::att_align_nominal(p, elmalign);
    }

    Ok(result)
}

/// `construct_array(text_datums, n, TEXTOID, -1, false, 'i')` (arrayfuncs.c)
/// over `text`-typed elements, taking each element's UTF-8 string directly
/// rather than a pointer-word `Datum` that `datum_as_byte_window` would have to
/// resolve. `TEXTOID` is variable-length (`elmlen = -1`), pass-by-reference,
/// int-aligned, and these arrays never contain NULLs (matching pg_proc.c's
/// `proargnames`/`proconfig` builds, which use `CStringGetTextDatum("")` for an
/// unnamed slot). Mirrors `construct_md_array` for the 1-D, no-null,
/// variable-length case: each element is stored as its `text` varlena image
/// (4-byte `SET_VARSIZE` header + payload), int-aligned.
pub fn build_text_array<'mcx>(mcx: Mcx<'mcx>, elems: &[&str]) -> PgResult<PgVec<'mcx, u8>> {
    let (_elmlen, _elmbyval, elmalign) = construct_builtin_meta(foundation::TEXTOID)?;
    let nelems = elems.len() as i32;

    // nelems <= 0 -> construct_empty_array (matches construct_md_array).
    if nelems <= 0 {
        return construct_empty_array(mcx, foundation::TEXTOID);
    }

    // Compute total data space exactly as construct_md_array: each
    // variable-length element contributes VARHDRSZ + payload bytes
    // (att_addlength_pointer), int-aligned per element (att_align_nominal),
    // with the alignment applied before each element after the first.
    let mut nbytes: i32 = 0;
    for (i, s) in elems.iter().enumerate() {
        let img_len = (4 + s.len()) as i32; // VARHDRSZ + payload (full 4-byte header)
        if i != 0 {
            nbytes = foundation::att_align_nominal(nbytes as usize, elmalign) as i32;
        }
        nbytes = nbytes
            .checked_add(img_len)
            .filter(|n| alloc_size_is_valid(*n))
            .ok_or_else(|| {
                PgError::error(format!(
                    "array size exceeds the maximum allowed ({MAX_ALLOC_SIZE})"
                ))
                .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
            })?;
    }

    // No nulls: dataoffset == 0, overhead is the non-null header.
    let dataoffset = 0;
    nbytes += foundation::arr_overhead_nonulls(1) as i32;
    if !alloc_size_is_valid(nbytes) {
        return Err(PgError::error(format!(
            "array size exceeds the maximum allowed ({MAX_ALLOC_SIZE})"
        ))
        .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED));
    }

    let total = nbytes as usize;
    let mut result = mcx::vec_with_capacity_in::<u8>(mcx, total)?;
    result.resize(total, 0); // palloc0

    let dims = [nelems];
    let lbs = [1];
    foundation::set_header(&mut result, total, 1, dataoffset, foundation::TEXTOID);
    foundation::write_dims(&mut result, &dims);
    foundation::write_lbounds(&mut result, 1, &lbs);

    // CopyArrayEls for variable-length by-reference, no nulls: write each
    // element's text varlena image (SET_VARSIZE header + payload) at the
    // int-aligned data offset.
    let mut p = foundation::arr_data_ptr_off(&result);
    for s in elems {
        p = foundation::att_align_nominal(p, elmalign);
        let payload = s.as_bytes();
        let img_len = 4 + payload.len();
        // SET_VARSIZE(elem, img_len): full 4-byte varlena header.
        let word = (img_len as u32) << 2;
        result[p..p + 4].copy_from_slice(&word.to_ne_bytes());
        result[p + 4..p + img_len].copy_from_slice(payload);
        p += img_len;
    }

    Ok(result)
}

/// `construct_array_builtin(cstring_datums, n, CSTRINGOID)` (arrayfuncs.c) over
/// `cstring`-typed elements, taking each element's string directly rather than a
/// pointer-word `Datum` that `datum_as_byte_window` would have to resolve.
/// `CSTRINGOID` is C-string-length (`elmlen = -2`), pass-by-reference,
/// char-aligned, and these arrays never contain NULLs (the typmodin cstring[]
/// build path in parse_type.c / fmgr). Mirrors `construct_md_array` for the 1-D,
/// no-null, cstring case: each element is its NUL-terminated string image
/// (`strlen + 1` bytes), char-aligned (no padding).
pub fn build_cstring_array<'mcx>(mcx: Mcx<'mcx>, elems: &[&str]) -> PgResult<PgVec<'mcx, u8>> {
    let (_elmlen, _elmbyval, elmalign) = construct_builtin_meta(foundation::CSTRINGOID)?;
    let nelems = elems.len() as i32;

    if nelems <= 0 {
        return construct_empty_array(mcx, foundation::CSTRINGOID);
    }

    // Each cstring element contributes strlen+1 bytes (att_addlength for
    // elmlen == -2), char-aligned (no padding) before each element after the
    // first.
    let mut nbytes: i32 = 0;
    for (i, s) in elems.iter().enumerate() {
        let img_len = (s.len() + 1) as i32; // payload + NUL
        if i != 0 {
            nbytes = foundation::att_align_nominal(nbytes as usize, elmalign) as i32;
        }
        nbytes = nbytes
            .checked_add(img_len)
            .filter(|n| alloc_size_is_valid(*n))
            .ok_or_else(|| {
                PgError::error(format!(
                    "array size exceeds the maximum allowed ({MAX_ALLOC_SIZE})"
                ))
                .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
            })?;
    }

    let dataoffset = 0;
    nbytes += foundation::arr_overhead_nonulls(1) as i32;
    if !alloc_size_is_valid(nbytes) {
        return Err(PgError::error(format!(
            "array size exceeds the maximum allowed ({MAX_ALLOC_SIZE})"
        ))
        .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED));
    }

    let total = nbytes as usize;
    let mut result = mcx::vec_with_capacity_in::<u8>(mcx, total)?;
    result.resize(total, 0); // palloc0

    let dims = [nelems];
    let lbs = [1];
    foundation::set_header(&mut result, total, 1, dataoffset, foundation::CSTRINGOID);
    foundation::write_dims(&mut result, &dims);
    foundation::write_lbounds(&mut result, 1, &lbs);

    // CopyArrayEls for cstring, no nulls: write each NUL-terminated string image
    // at the char-aligned data offset.
    let mut p = foundation::arr_data_ptr_off(&result);
    for s in elems {
        p = foundation::att_align_nominal(p, elmalign);
        let payload = s.as_bytes();
        let img_len = payload.len() + 1;
        result[p..p + payload.len()].copy_from_slice(payload);
        // trailing NUL already zero from palloc0
        p += img_len;
    }

    Ok(result)
}

// ---------------------------------------------------------------------------
// 6-arm value-lane construct path (`types_tuple::Datum<'mcx>`).
//
// `construct_md_array` (above) takes the bare C word `types_datum::Datum`, which
// at the fmgr/ADT ABI boundary carries a by-reference element as a *pointer
// word* into a caller-owned varlena; the owned model has no global address
// space, so that pointer cannot be resolved (`datum_payload_bytes` faults). The
// executor, however, holds element values in the safe 6-arm
// `types_tuple::Datum<'mcx>` lane, where a by-reference element carries its
// bytes inline (`ByRef`/`Cstring`) and a composite/expanded element carries the
// real object. This path mirrors `construct_md_array` byte-for-byte but sources
// each element's stored image directly from the 6-arm value (detoasting a
// varlena through the live `detoast_attr` seam), so the result is a faithful
// `ArrayType` varlena image with no pointer-word resolution. Mirrors the
// `build_name_array` / `build_text_array` variant style.
// ---------------------------------------------------------------------------

/// `construct_array(values, nelems, elmtype, elmlen, elmbyval, elmalign)`
/// (arrayfuncs.c) over the 6-arm value lane: build a one-dimensional array from
/// `types_tuple::Datum<'mcx>` element values.
pub fn construct_array_values<'mcx>(
    mcx: Mcx<'mcx>,
    values: &[types_tuple::Datum<'mcx>],
    elmtype: Oid,
    elmlen: i32,
    elmbyval: bool,
    elmalign: u8,
) -> PgResult<PgVec<'mcx, u8>> {
    let nelems = values.len() as i32;
    let dims = [nelems];
    let lbs = [1];
    construct_md_array_values(
        mcx, values, None, 1, &dims, &lbs, elmtype, elmlen, elmbyval, elmalign,
    )
}

/// `construct_md_array(values, nulls, ndims, dims, lbs, elmtype, elmlen,
/// elmbyval, elmalign)` (arrayfuncs.c) over the 6-arm value lane.
///
/// Identical control flow to [`construct_md_array`]: the same overflow checks,
/// the same null-bitmap / dataoffset accounting, the same `set_header` /
/// `write_dims` / `write_lbounds`, and the same `CopyArrayEls` element loop —
/// only the per-element byte source differs (6-arm value, not pointer word).
pub fn construct_md_array_values<'mcx>(
    mcx: Mcx<'mcx>,
    values: &[types_tuple::Datum<'mcx>],
    nulls: Option<&[bool]>,
    ndims: i32,
    dims: &[i32],
    lbs: &[i32],
    elmtype: Oid,
    elmlen: i32,
    elmbyval: bool,
    elmalign: u8,
) -> PgResult<PgVec<'mcx, u8>> {
    if ndims < 0 {
        return Err(PgError::error(format!(
            "invalid number of dimensions: {ndims}"
        ))
        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
    }
    if ndims > MAXDIM {
        return Err(PgError::error(format!(
            "number of array dimensions ({ndims}) exceeds the maximum allowed ({MAXDIM})"
        ))
        .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED));
    }

    let nelems = arrayutils_seam::array_get_n_items::call(ndims, dims)?;
    arrayutils_seam::array_check_bounds::call(ndims, dims, lbs)?;

    if nelems <= 0 {
        return construct_empty_array(mcx, elmtype);
    }

    // First pass: detoast/prepare each non-null element's stored image bytes and
    // sum the byte lengths (mirrors construct_md_array's PG_DETOAST_DATUM +
    // att_addlength_datum + att_align_nominal accumulation).
    let mut prepared: Vec<Option<PreparedElem<'mcx>>> = Vec::with_capacity(nelems as usize);
    let mut nbytes: i32 = 0;
    let mut hasnulls = false;
    for i in 0..nelems as usize {
        if nulls.map(|n| n[i]).unwrap_or(false) {
            hasnulls = true;
            prepared.push(None);
            continue;
        }
        let pe = prepare_value_elem(mcx, &values[i], elmlen, elmbyval)?;
        nbytes += pe.stored_len(elmlen) as i32;
        nbytes = foundation::att_align_nominal(nbytes as usize, elmalign) as i32;
        if !alloc_size_is_valid(nbytes) {
            return Err(PgError::error(format!(
                "array size exceeds the maximum allowed ({MAX_ALLOC_SIZE})"
            ))
            .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED));
        }
        prepared.push(Some(pe));
    }

    let dataoffset: i32;
    if hasnulls {
        dataoffset = foundation::arr_overhead_withnulls(ndims, nelems) as i32;
        nbytes += dataoffset;
    } else {
        dataoffset = 0;
        nbytes += foundation::arr_overhead_nonulls(ndims) as i32;
    }

    let total = nbytes as usize;
    let mut result = mcx::vec_with_capacity_in::<u8>(mcx, total)?;
    result.resize(total, 0);

    foundation::set_header(&mut result, total, ndims, dataoffset, elmtype);
    foundation::write_dims(&mut result, &dims[..ndims as usize]);
    foundation::write_lbounds(&mut result, ndims, &lbs[..ndims as usize]);

    // CopyArrayEls, sourcing each element image from the prepared 6-arm values.
    copy_array_els_values(
        &mut result, &prepared, nelems, elmlen, elmbyval, elmalign,
    );

    Ok(result)
}

/// A non-null element's stored image, resolved from a 6-arm value.
enum PreparedElem<'mcx> {
    /// By-value element: the bare word to `store_att_byval`.
    ByVal(usize),
    /// By-reference element: the verbatim stored bytes (varlena / cstring image
    /// / fixed-length by-ref payload), already detoasted where applicable.
    Bytes(PgVec<'mcx, u8>),
}

impl<'mcx> PreparedElem<'mcx> {
    /// The number of bytes this element occupies in the array data area (before
    /// the per-element alignment padding), mirroring `att_addlength_datum`.
    fn stored_len(&self, attlen: i32) -> usize {
        match self {
            PreparedElem::ByVal(_) => attlen as usize,
            PreparedElem::Bytes(b) => {
                if attlen > 0 {
                    attlen as usize
                } else if attlen == -1 {
                    foundation::varsize_any(b, 0)
                } else {
                    // attlen == -2: cstring, strlen + 1.
                    cstring_len(b) + 1
                }
            }
        }
    }
}

/// Resolve a 6-arm element value into its stored image (`PreparedElem`),
/// mirroring `construct_md_array`'s `PG_DETOAST_DATUM` for varlena elements.
fn prepare_value_elem<'mcx>(
    mcx: Mcx<'mcx>,
    v: &types_tuple::Datum<'mcx>,
    elmlen: i32,
    elmbyval: bool,
) -> PgResult<PreparedElem<'mcx>> {
    use types_tuple::backend_access_common_heaptuple::Datum as TDatum;
    if elmbyval {
        // By-value: take the bare word (store_att_byval reads `attlen` bytes).
        let word = match v {
            TDatum::ByVal(w) => *w,
            other => panic!(
                "construct_md_array_values: by-value element type but value is \
                 not Datum::ByVal: {other:?}"
            ),
        };
        return Ok(PreparedElem::ByVal(word));
    }

    // By-reference. The 6-arm value carries the bytes inline.
    match v {
        TDatum::ByRef(bytes) => {
            if elmlen == -1 {
                // Varlena: PG_DETOAST_DATUM. Under the header-ful-everywhere
                // convention the executor's `Datum::ByRef` lane always carries a
                // self-describing header-ful varlena image (heap-deform / ref_out
                // / a Const literal framed at the parser boundary), so detoast it
                // verbatim — `varsize_any` reads its length straight off the
                // header (no header-less disambiguation, no restamp).
                let detoasted = detoast_seam::detoast_attr::call(mcx, bytes)?;
                Ok(PreparedElem::Bytes(detoasted))
            } else {
                // Fixed-length by-ref (e.g. NAMEDATALEN / tid): copy verbatim.
                Ok(PreparedElem::Bytes(slice_to_pgvec(mcx, bytes)?))
            }
        }
        TDatum::Cstring(s) => {
            // cstring element image: payload bytes + the terminating NUL.
            let payload = s.as_bytes();
            let mut buf = mcx::vec_with_capacity_in::<u8>(mcx, payload.len() + 1)?;
            buf.extend_from_slice(payload);
            buf.push(0);
            Ok(PreparedElem::Bytes(buf))
        }
        TDatum::Expanded(eo) => {
            // EOH_flatten_into: materialize the flat varlena image (flattened
            // images are never toasted, so no further detoast is needed).
            let flat = types_datum::flatten_expanded(eo.as_ref());
            Ok(PreparedElem::Bytes(slice_to_pgvec(mcx, &flat)?))
        }
        TDatum::Composite(_) => {
            // A composite value already IS a varlena-framed HeapTupleHeader
            // image (C: `struct varlena *`). Materialize its flat datum image
            // and treat it as a varlena element (composite element types are
            // always varlena, elmlen == -1). Flattened images are never toasted.
            let flat = v.as_varlena_bytes();
            Ok(PreparedElem::Bytes(slice_to_pgvec(mcx, &flat)?))
        }
        TDatum::ByVal(_) => panic!(
            "construct_md_array_values: by-reference element type but value is \
             Datum::ByVal"
        ),
        TDatum::Internal(_) => panic!(
            "construct_md_array_values: element value is Internal, \
             which has no flat by-reference image (arrays of internal are \
             not constructed through this path)"
        ),
    }
}

/// `CopyArrayEls` over prepared 6-arm element images.
fn copy_array_els_values(
    result: &mut PgVec<'_, u8>,
    prepared: &[Option<PreparedElem<'_>>],
    nitems: i32,
    typlen: i32,
    typbyval: bool,
    typalign: u8,
) {
    let _ = typbyval;
    let mut p = foundation::arr_data_ptr_off(result);
    let bitmap_off = foundation::arr_nullbitmap_off(result);
    let mut bitval: i32 = 0;
    let mut bitmask: i32 = 1;
    let mut bm_byte = bitmap_off;

    for i in 0..nitems as usize {
        match &prepared[i] {
            None => {
                // null element: data not written, bit left clear.
            }
            Some(pe) => {
                p = store_prepared_elem(result, p, pe, typlen, typalign);
                if bitmap_off.is_some() {
                    bitval |= bitmask;
                }
            }
        }
        if let Some(b) = bm_byte {
            bitmask <<= 1;
            if bitmask == 0x100 {
                result[b] = bitval as u8;
                bm_byte = Some(b + 1);
                bitval = 0;
                bitmask = 1;
            }
        }
    }
    if let Some(b) = bm_byte {
        if bitmask != 1 {
            result[b] = bitval as u8;
        }
    }
}

/// `ArrayCastAndSet` over a prepared 6-arm element image: store it and return
/// the advanced (aligned) offset.
fn store_prepared_elem(
    result: &mut PgVec<'_, u8>,
    mut dest: usize,
    pe: &PreparedElem<'_>,
    typlen: i32,
    typalign: u8,
) -> usize {
    let inc: usize;
    match pe {
        PreparedElem::ByVal(word) => {
            // store_att_byval(dest, src, typlen).
            foundation::store_att_byval(result, dest, Datum::from_usize(*word), typlen);
            inc = typlen as usize;
        }
        PreparedElem::Bytes(bytes) => {
            if typlen > 0 {
                // Fixed-length by-ref: memmove(dest, ptr, typlen).
                result[dest..dest + typlen as usize].copy_from_slice(&bytes[..typlen as usize]);
                inc = typlen as usize;
            } else {
                let len = if typlen == -1 {
                    foundation::varsize_any(bytes, 0)
                } else {
                    cstring_len(bytes) + 1
                };
                result[dest..dest + len].copy_from_slice(&bytes[..len]);
                inc = len;
            }
        }
    }
    let aligned = foundation::att_align_nominal(inc, typalign);
    dest += aligned;
    dest
}

/// Copy a byte slice into a fresh `mcx`-allocated `PgVec`.
fn slice_to_pgvec<'mcx>(mcx: Mcx<'mcx>, src: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let mut buf = mcx::vec_with_capacity_in::<u8>(mcx, src.len())?;
    buf.extend_from_slice(src);
    Ok(buf)
}

/// `ExecEvalArrayExpr`'s multidims branch (execExprInterp.c): build a multi-D
/// array by concatenating the data areas of the sub-array element values.
///
/// Each non-null element is itself an `ArrayType` image carried in the 6-arm
/// `ByRef` lane (detoasted through the live seam). Mirrors the C branch
/// byte-for-byte: validate matching element type and dimensionality, gather the
/// sub-array data areas / null bitmaps, then palloc0 + SET_VARSIZE + header +
/// per-sub-array memcpy + array_bitmap_copy. The all-empty case returns
/// `construct_empty_array`.
pub fn construct_md_array_nested<'mcx>(
    mcx: Mcx<'mcx>,
    values: &[types_tuple::Datum<'mcx>],
    nulls: &[bool],
    element_type: Oid,
) -> PgResult<PgVec<'mcx, u8>> {
    let nelems = values.len();

    // Gathered per-sub-array info (only for accepted, non-empty sub-arrays).
    struct Sub<'mcx> {
        arr: PgVec<'mcx, u8>,
        data_off: usize,
        nbytes: usize,
        nitems: i32,
        hasnull: bool,
    }
    let mut subs: Vec<Sub<'mcx>> = Vec::with_capacity(nelems);

    let mut nbytes: i32 = 0;
    let mut ndims: i32 = 0;
    let mut elem_ndims: i32 = 0;
    let mut elem_dims: Vec<i32> = Vec::new();
    let mut elem_lbs: Vec<i32> = Vec::new();
    let mut firstone = true;
    let mut havenulls = false;
    let mut haveempty = false;

    for elemoff in 0..nelems {
        // temporarily ignore null subarrays
        if nulls[elemoff] {
            haveempty = true;
            continue;
        }

        // array = DatumGetArrayTypeP(arraydatum) — the sub-array, detoasted.
        let array = detoast_value_to_array(mcx, &values[elemoff])?;

        // run-time double-check on element type
        let this_elemtype = foundation::arr_elemtype(&array);
        if element_type != this_elemtype {
            return Err(PgError::error("cannot merge incompatible arrays")
                .with_detail(format!(
                    "Array with element type {this_elemtype} cannot be included \
                     in ARRAY construct with element type {element_type}."
                ))
                .with_sqlstate(ERRCODE_DATATYPE_MISMATCH));
        }

        let this_ndims = foundation::arr_ndim(&array);
        // temporarily ignore zero-dimensional subarrays
        if this_ndims <= 0 {
            haveempty = true;
            continue;
        }

        if firstone {
            elem_ndims = this_ndims;
            ndims = elem_ndims + 1;
            if ndims <= 0 || ndims > MAXDIM {
                return Err(PgError::error(format!(
                    "number of array dimensions ({ndims}) exceeds the maximum allowed ({MAXDIM})"
                ))
                .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED));
            }
            elem_dims = (0..elem_ndims as usize)
                .map(|i| foundation::arr_dim(&array, i))
                .collect();
            elem_lbs = (0..elem_ndims as usize)
                .map(|i| foundation::arr_lbound(&array, i))
                .collect();
            firstone = false;
        } else {
            // Check other sub-arrays are compatible.
            let this_dims: Vec<i32> = (0..this_ndims as usize)
                .map(|i| foundation::arr_dim(&array, i))
                .collect();
            let this_lbs: Vec<i32> = (0..this_ndims as usize)
                .map(|i| foundation::arr_lbound(&array, i))
                .collect();
            if elem_ndims != this_ndims || elem_dims != this_dims || elem_lbs != this_lbs {
                return Err(PgError::error(
                    "multidimensional arrays must have array expressions with matching dimensions",
                )
                .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR));
            }
        }

        let data_off = foundation::arr_data_ptr_off(&array);
        let sub_nbytes = foundation::arr_size(&array) - foundation::arr_data_offset(&array);
        nbytes += sub_nbytes as i32;
        if !alloc_size_is_valid(nbytes) {
            return Err(PgError::error(format!(
                "array size exceeds the maximum allowed ({MAX_ALLOC_SIZE})"
            ))
            .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED));
        }
        let sub_nitems = arrayutils_seam::array_get_n_items::call(
            this_ndims,
            &(0..this_ndims as usize)
                .map(|i| foundation::arr_dim(&array, i))
                .collect::<Vec<_>>(),
        )?;
        let sub_hasnull = foundation::arr_hasnull(&array);
        havenulls |= sub_hasnull;

        subs.push(Sub {
            arr: array,
            data_off,
            nbytes: sub_nbytes,
            nitems: sub_nitems,
            hasnull: sub_hasnull,
        });
    }

    let outer_nelems = subs.len() as i32;

    // All-empty / mixed handling.
    if haveempty {
        if ndims == 0 {
            // didn't find any nonempty array
            return construct_empty_array(mcx, element_type);
        }
        return Err(PgError::error(
            "multidimensional arrays must have array expressions with matching dimensions",
        )
        .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR));
    }

    // setup for multi-D array
    let mut dims = vec![0i32; ndims as usize];
    let mut lbs = vec![0i32; ndims as usize];
    dims[0] = outer_nelems;
    lbs[0] = 1;
    for i in 1..ndims as usize {
        dims[i] = elem_dims[i - 1];
        lbs[i] = elem_lbs[i - 1];
    }

    let nitems = arrayutils_seam::array_get_n_items::call(ndims, &dims)?;
    arrayutils_seam::array_check_bounds::call(ndims, &dims, &lbs)?;

    let dataoffset: i32;
    if havenulls {
        dataoffset = foundation::arr_overhead_withnulls(ndims, nitems) as i32;
        nbytes += dataoffset;
    } else {
        dataoffset = 0;
        nbytes += foundation::arr_overhead_nonulls(ndims) as i32;
    }

    let total = nbytes as usize;
    let mut result = mcx::vec_with_capacity_in::<u8>(mcx, total)?;
    result.resize(total, 0);

    foundation::set_header(&mut result, total, ndims, dataoffset, element_type);
    foundation::write_dims(&mut result, &dims);
    foundation::write_lbounds(&mut result, ndims, &lbs);

    // dat = ARR_DATA_PTR(result); copy each sub-array's data + null bitmap.
    let mut dat = foundation::arr_data_ptr_off(&result);
    let mut iitem: i32 = 0;
    for sub in &subs {
        result[dat..dat + sub.nbytes]
            .copy_from_slice(&sub.arr[sub.data_off..sub.data_off + sub.nbytes]);
        dat += sub.nbytes;
        if havenulls {
            let dest_bm = foundation::arr_nullbitmap_off(&result).expect("withnulls layout");
            let src_bm = foundation::arr_nullbitmap_off(&sub.arr);
            copy_bitmap_from_array(&mut result, dest_bm, iitem, &sub.arr, src_bm, sub.nitems);
        }
        let _ = sub.hasnull;
        iitem += sub.nitems;
    }

    Ok(result)
}

/// `array_bitmap_copy(ARR_NULLBITMAP(result), iitem, ARR_NULLBITMAP(arr), 0,
/// nitems)` — copy a sub-array's null bits (or all-non-null when the sub-array
/// has no bitmap) into the result's bitmap at bit offset `dest_offset`.
fn copy_bitmap_from_array(
    dest: &mut [u8],
    dest_bitmap_off: usize,
    dest_offset: i32,
    src_arr: &[u8],
    src_bitmap_off: Option<usize>,
    nitems: i32,
) {
    let mut destbitmask: i32 = 1 << (dest_offset % 8);
    let mut dest_byte = dest_bitmap_off + (dest_offset / 8) as usize;
    let mut destbitval: i32 = dest[dest_byte] as i32;

    let mut srcbitmask: i32 = 1;
    let mut src_byte = src_bitmap_off;

    for _ in 0..nitems {
        let bit = match (src_bitmap_off, src_byte) {
            (Some(_), Some(sb)) => (src_arr[sb] as i32 & srcbitmask) != 0,
            _ => true, // NULL source => all non-null
        };
        if bit {
            destbitval |= destbitmask;
        } else {
            destbitval &= !destbitmask;
        }
        destbitmask <<= 1;
        if destbitmask == 0x100 {
            dest[dest_byte] = destbitval as u8;
            dest_byte += 1;
            destbitmask = 1;
            destbitval = if dest_byte < dest.len() {
                dest[dest_byte] as i32
            } else {
                0
            };
        }
        if src_bitmap_off.is_some() {
            srcbitmask <<= 1;
            if srcbitmask == 0x100 {
                srcbitmask = 1;
                src_byte = src_byte.map(|b| b + 1);
            }
        }
    }
    if destbitmask != 1 {
        dest[dest_byte] = destbitval as u8;
    }
}

/// `DatumGetArrayTypeP(arraydatum)` over a 6-arm value: the sub-array's verbatim
/// varlena bytes, detoasted through the live seam.
fn detoast_value_to_array<'mcx>(
    mcx: Mcx<'mcx>,
    v: &types_tuple::Datum<'mcx>,
) -> PgResult<PgVec<'mcx, u8>> {
    use types_tuple::backend_access_common_heaptuple::Datum as TDatum;
    match v {
        TDatum::ByRef(bytes) => detoast_seam::detoast_attr::call(mcx, bytes),
        TDatum::Expanded(eo) => {
            let flat = types_datum::flatten_expanded(eo.as_ref());
            slice_to_pgvec(mcx, &flat)
        }
        other => panic!(
            "construct_md_array_nested: sub-array element value is not a \
             by-reference array image: {other:?}"
        ),
    }
}

/// Seam `decode_text_array_to_strings` — the array half of evtcache.c's
/// `DecodeTextArrayToBitmapset`: `DatumGetArrayTypeP` (detoast) + the
/// `ARR_NDIM != 1 || ARR_HASNULL || ARR_ELEMTYPE != TEXTOID` validity check
/// (`elog(ERROR, "expected 1-D text array")`) + `deconstruct_array_builtin`.
pub fn decode_text_array_to_strings<'mcx>(
    mcx: Mcx<'mcx>,
    array: &[u8],
) -> PgResult<PgVec<'mcx, PgString<'mcx>>> {
    // arr = DatumGetArrayTypeP(array);
    let arr = detoast_seam::detoast_attr::call(mcx, array)?;

    // if (ARR_NDIM(arr) != 1 || ARR_HASNULL(arr) || ARR_ELEMTYPE(arr) != TEXTOID)
    //     elog(ERROR, "expected 1-D text array");
    if foundation::arr_ndim(&arr) != 1
        || foundation::arr_hasnull(&arr)
        || foundation::arr_elemtype(&arr) != foundation::TEXTOID
    {
        return Err(PgError::error("expected 1-D text array"));
    }

    // deconstruct_array_builtin(arr, TEXTOID, &elems, NULL, &nelems);
    deconstruct_text_array(mcx, &arr)
}

/// Seam `deconstruct_text_array` — `deconstruct_array_builtin(..., TEXTOID)`.
pub fn deconstruct_text_array<'mcx>(
    mcx: Mcx<'mcx>,
    array: &[u8],
) -> PgResult<PgVec<'mcx, PgString<'mcx>>> {
    // C: deconstruct_array_builtin(DatumGetArrayTypeP(array), TEXTOID, ...).
    // `DatumGetArrayTypeP` is `PG_DETOAST_DATUM`: it un-packs a short (1-byte)
    // varlena header and inlines/decompresses a toasted datum so the
    // fixed-offset `ArrayType` header fields (ndim/dataoffset/elemtype at
    // offsets 4/8/12) read correctly. On-disk reloptions (`pg_class.reloptions`,
    // a `text[]`) come back from `fastgetattr` still SHORT-header packed, so the
    // un-detoasted bytes mis-read `ARR_ELEMTYPE` (e.g. 256 instead of TEXTOID).
    // Detoast first, matching the C caller's `DatumGetArrayTypeP` (no-op on an
    // already-plain 4-byte-header array, e.g. when called from
    // `decode_text_array_to_strings`).
    let arr = detoast_seam::detoast_attr::call(mcx, array)?;

    let (elmlen, elmbyval, elmalign) = deconstruct_builtin_meta(foundation::TEXTOID)?;
    // Use the value-lane element walk: it materializes each by-reference text
    // element's verbatim varlena bytes as a `Datum::ByRef`. (The bare
    // `deconstruct_array` stores only the in-buffer *offset* in its by-ref
    // output Datum, which is not a dereferenceable pointer — projecting it as a
    // varlena image segfaults.)
    let pairs = deconstruct_array_values(
        mcx,
        &arr,
        foundation::TEXTOID,
        elmlen,
        elmbyval,
        elmalign,
    )?;

    let mut out = mcx::vec_with_capacity_in::<PgString<'mcx>>(mcx, pairs.len())?;
    for (d, isnull) in pairs.iter() {
        if *isnull {
            // reloptions arrays have no NULLs; the C C-string projection would
            // dereference a NULL — surface the same null-not-allowed error.
            return Err(PgError::error("null array element not allowed in this context")
                .with_sqlstate(ERRCODE_NULL_VALUE_NOT_ALLOWED));
        }
        // Each element value carries the text varlena bytes; detoast (handles a
        // short/compressed header) and project to its UTF-8 payload.
        let bytes = detoast_value_to_array(mcx, d)?;
        let s = text_to_pgstring(mcx, &bytes)?;
        out.push(s);
    }
    Ok(out)
}

/// Seam `deconstruct_text_array_nullable` —
/// `deconstruct_array_builtin(DatumGetArrayTypeP(array), TEXTOID, &elems,
/// &nulls, &nelems)` (arrayfuncs.c), preserving per-element NULLs. Unlike
/// [`deconstruct_text_array`] (which rejects NULLs), this returns the C
/// `(elems[i], nulls[i])` pairs as `Option<PgString>` (`None` ⇒ the C
/// `nulls[i] == true`), so a caller can apply its own object-specific
/// null-error message (e.g. `textarray_to_strvaluelist`'s "name or argument
/// lists may not contain nulls"). The on-disk array byte image is detoasted
/// (`DatumGetArrayTypeP`), then walked element by element, each non-null
/// `text` element projected to its UTF-8 string. Fallible on detoast /
/// malformed array / invalid UTF-8.
pub fn deconstruct_text_array_nullable<'mcx>(
    mcx: Mcx<'mcx>,
    array: &[u8],
) -> PgResult<PgVec<'mcx, Option<PgString<'mcx>>>> {
    // arr = DatumGetArrayTypeP(array);
    let arr = detoast_seam::detoast_attr::call(mcx, array)?;
    let (elmlen, elmbyval, elmalign) = deconstruct_builtin_meta(foundation::TEXTOID)?;
    // Value-lane walk: materialize each by-reference text element's verbatim
    // varlena bytes (the bare offset-Datum from `deconstruct_array` is not a
    // dereferenceable pointer — see `deconstruct_text_array`).
    let pairs = deconstruct_array_values(
        mcx,
        &arr,
        foundation::TEXTOID,
        elmlen,
        elmbyval,
        elmalign,
    )?;

    let mut out = mcx::vec_with_capacity_in::<Option<PgString<'mcx>>>(mcx, pairs.len())?;
    for (d, isnull) in pairs.iter() {
        if *isnull {
            out.push(None);
            continue;
        }
        // Each element value carries the text varlena bytes; detoast and project
        // to its UTF-8 payload.
        let bytes = detoast_value_to_array(mcx, d)?;
        out.push(Some(text_to_pgstring(mcx, &bytes)?));
    }
    Ok(out)
}

/// Seam `deconstruct_tid_array` — `deconstruct_array_builtin(..., TIDOID)`.
///
/// `array` is the `tid[]` on-disk varlena image (the by-reference array carrier
/// the executor produces for `ARRAY[...]::tid[]`), exactly as
/// `deconstruct_text_array_nullable` takes its `text[]` image.
pub fn deconstruct_tid_array<'mcx>(
    mcx: Mcx<'mcx>,
    array: &[u8],
) -> PgResult<PgVec<'mcx, (ItemPointerData, bool)>> {
    // DatumGetArrayTypeP(arraydatum) — detoast the array varlena.
    let arr = detoast_seam::detoast_attr::call(mcx, array)?;
    let (elmlen, elmbyval, elmalign) = deconstruct_builtin_meta(foundation::TIDOID)?;
    // `tid` is a 6-byte pass-by-reference element type. The bare-word
    // `deconstruct_array` would store only the in-buffer *offset* in each
    // element Datum, which `datum_payload_bytes` (via `datum_to_item_pointer`)
    // would then dereference as a real pointer — SIGSEGV on a real `tid[]`.
    // Use the value-lane element walk, which materializes each element's
    // verbatim 6 stored bytes as a `Datum::ByRef`.
    let pairs = deconstruct_array_values(mcx, &arr, foundation::TIDOID, elmlen, elmbyval, elmalign)?;

    let mut out = mcx::vec_with_capacity_in::<(ItemPointerData, bool)>(mcx, pairs.len())?;
    for (d, isnull) in pairs.iter() {
        // ipdatums[i] reinterpreted via DatumGetPointer as an ItemPointer.
        let ip = if *isnull {
            ItemPointerData::default()
        } else {
            item_pointer_from_value(mcx, d)?
        };
        out.push((ip, *isnull));
    }
    Ok(out)
}

/// Seam `construct_text_array` — `accumArrayResult`/`makeArrayResult` over
/// `TEXTOID`.
pub fn construct_text_array<'mcx>(mcx: Mcx<'mcx>, elems: &[&str]) -> PgResult<Datum> {
    // An empty input yields the C (Datum) 0 (no array).
    if elems.is_empty() {
        return Ok(Datum::null());
    }

    // accumArrayResult over each element's CStringGetTextDatum, then
    // makeArrayResult, all over TEXTOID.
    let mut astate: Option<ArrayBuildState> = None;
    for s in elems {
        let d = cstring_to_text_datum(mcx, s)?;
        astate = Some(accum_array_result(
            mcx,
            astate.take(),
            d,
            false,
            foundation::TEXTOID,
        )?);
    }
    let astate = astate.expect("non-empty input builds a state");
    let buf = make_array_result(mcx, &astate)?;
    Ok(datum_from_buf(buf))
}

/// Seam `construct_text_array_bytes` — `accumArrayResult`/`makeArrayResult`
/// over `TEXTOID`, returning the flat array varlena byte image (the bytes a
/// `ByRef`/`RefPayload::Varlena` carries) rather than a bare pointer word. An
/// empty input yields `construct_empty_array(TEXTOID)` (matching C's
/// `optionListToArray` "pass a null options list as an empty array").
pub fn construct_text_array_bytes_str<'mcx>(
    mcx: Mcx<'mcx>,
    elems: &[&str],
) -> PgResult<PgVec<'mcx, u8>> {
    if elems.is_empty() {
        return construct_empty_array(mcx, foundation::TEXTOID);
    }
    let mut astate: Option<ArrayBuildState> = None;
    for s in elems {
        let d = cstring_to_text_datum(mcx, s)?;
        astate = Some(accum_array_result(
            mcx,
            astate.take(),
            d,
            false,
            foundation::TEXTOID,
        )?);
    }
    let astate = astate.expect("non-empty input builds a state");
    make_array_result(mcx, &astate)
}

/// Seam `text_array_out` — `accumArrayResult`/`makeArrayResult` over `TEXTOID`
/// then `array_out` (the `getTypeOutputInfo(ANYARRAYOID)` +
/// `OidOutputFunctionCall(typoutput, makeArrayResult(...))` pair).
pub fn text_array_out<'mcx>(mcx: Mcx<'mcx>, elems: &[&str]) -> PgResult<PgString<'mcx>> {
    // An empty input renders the C empty-array form `{}`.
    if elems.is_empty() {
        return PgString::from_str_in("{}", mcx);
    }

    let mut astate: Option<ArrayBuildState> = None;
    for elem in elems {
        let d = cstring_to_text_datum(mcx, elem)?;
        astate = Some(accum_array_result(
            mcx,
            astate.take(),
            d,
            false,
            foundation::TEXTOID,
        )?);
    }
    let astate = astate.expect("non-empty input builds a state");
    let buf = make_array_result(mcx, &astate)?;

    // OidOutputFunctionCall(anyarray_out, val) == array_out(val); the rendered
    // bytes are server-encoded text (the per-element `text` output bytes).
    let rendered = crate::io::array_out(mcx, &buf)?;
    PgString::from_utf8(rendered)
        .map_err(|_| PgError::error("array_out produced invalid UTF-8"))
}

/// Seam `build_text_array_nullable` — `accumArrayResult`/`makeArrayResult` over
/// `TEXTOID`, preserving per-element NULLs (the array-build half of
/// `text_to_array` / `text_to_array_null`, varlena.c:4771-4801).
pub fn build_text_array_nullable<'mcx>(
    mcx: Mcx<'mcx>,
    elems: &[Option<&[u8]>],
) -> PgResult<PgVec<'mcx, u8>> {
    // C: text_to_array's `tstate.astate == NULL` branch returns
    // construct_empty_array(TEXTOID) (a zero-element array, not NULL). split_text
    // produces zero fields only for an empty input set.
    if elems.is_empty() {
        return construct_empty_array(mcx, foundation::TEXTOID);
    }

    // accumArrayResult(astate, CStringGetTextDatum(field), is_null, TEXTOID,...)
    // per split field, then makeArrayResult, all over TEXTOID.
    let mut astate: Option<ArrayBuildState> = None;
    for elem in elems {
        let (dvalue, disnull) = match elem {
            Some(bytes) => (cstring_bytes_to_text_datum(mcx, bytes)?, false),
            // C: split_text_accum_result accumulates (Datum) 0 with disnull set.
            None => (Datum::null(), true),
        };
        astate = Some(accum_array_result(
            mcx,
            astate.take(),
            dvalue,
            disnull,
            foundation::TEXTOID,
        )?);
    }
    let astate = astate.expect("non-empty input builds a state");
    make_array_result(mcx, &astate)
}

/// Seam `construct_int4_array` — `construct_array_builtin(datums, n, INT4OID)`
/// (arrayfuncs.c). The `pg_blocking_pids` / `pg_safe_snapshot_blocking_pids`
/// callers pass an `int32[]` slice; an empty input still yields a valid empty
/// array (the C behaviour — `construct_array_builtin` over zero elements).
pub fn construct_int4_array<'mcx>(mcx: Mcx<'mcx>, elems: &[i32]) -> PgResult<Datum> {
    // datums[i] = Int32GetDatum(elems[i]); construct_array_builtin(datums, n, INT4OID).
    let mut datums = mcx::vec_with_capacity_in::<Datum>(mcx, elems.len())?;
    for &e in elems {
        // Int32GetDatum: the i32 value held in the low word of the Datum, exactly
        // as a pass-by-value int4 element is stored.
        datums.push(Datum::from_usize(e as u32 as usize));
    }
    construct_array_builtin(mcx, &datums, foundation::INT4OID)
}

/// Seam `array_get_ndim` — `ARR_NDIM(DatumGetArrayTypeP(arraydatum))`
/// (array.h). Detoast the array varlena, then read the `ndim` header field.
pub fn array_get_ndim<'mcx>(mcx: Mcx<'mcx>, arraydatum: Datum) -> PgResult<i32> {
    let arr = detoast_seam::detoast_attr::call(mcx, datum_as_byte_window(arraydatum))?;
    Ok(foundation::arr_ndim(&arr))
}

/// Seam `array_const_nitems` —
/// `ArrayGetNItems(ARR_NDIM(arrayval), ARR_DIMS(arrayval))` (array.h /
/// arrayfuncs.c) over a non-NULL `Const`'s `constvalue` array bytes. Detoast
/// the array varlena (`DatumGetArrayTypeP`), read the `ndim`/`dims` header,
/// and total the element count. `Err` carries the `ArrayGetNItems` overflow
/// `ereport(ERROR, ERRCODE_PROGRAM_LIMIT_EXCEEDED)` and detoast surface.
pub fn array_const_nitems<'mcx>(mcx: Mcx<'mcx>, arraybytes: &[u8]) -> PgResult<i32> {
    // arrayval = DatumGetArrayTypeP(constvalue) — detoast the array varlena.
    let arr = detoast_seam::detoast_attr::call(mcx, arraybytes)?;
    let ndim = foundation::arr_ndim(&arr);
    // ArrayGetNItems(ARR_NDIM(arrayval), ARR_DIMS(arrayval)).
    let dims = foundation::arr_dims(mcx, &arr)?;
    arrayutils_seam::array_get_n_items::call(ndim, &dims)
}

/// Seam `array_get_elemtype` — `ARR_ELEMTYPE(DatumGetArrayTypeP(arraydatum))`
/// (array.h). Detoast the array varlena, then read the `elemtype` header field.
pub fn array_get_elemtype<'mcx>(mcx: Mcx<'mcx>, arraydatum: Datum) -> PgResult<Oid> {
    let arr = detoast_seam::detoast_attr::call(mcx, datum_as_byte_window(arraydatum))?;
    Ok(foundation::arr_elemtype(&arr))
}

/// Seam `array_get_elemtype_bytes` — `ARR_ELEMTYPE(DatumGetArrayTypeP(bytes))`
/// over the on-disk array byte image (a `Datum::ByRef`), mirroring
/// [`array_get_elemtype`] but reading the bytes directly.
pub fn array_get_elemtype_bytes<'mcx>(mcx: Mcx<'mcx>, bytes: &[u8]) -> PgResult<Oid> {
    let arr = detoast_seam::detoast_attr::call(mcx, bytes)?;
    Ok(foundation::arr_elemtype(&arr))
}

/// Project the `ArrayType` header fields (`ndim`/`dim0`/`hasnull`/`elemtype`)
/// out of a detoasted array buffer. Shared by the funcapi `*_array_datum`
/// seams. The shape-validity checks (and the `elog(ERROR)`) stay on the funcapi
/// caller; this only projects, exactly as the C `DatumGetArrayTypeP` + header
/// reads.
fn project_array_header(arr: &mcx::PgVec<'_, u8>) -> (i32, i32, bool, Oid) {
    let ndim = foundation::arr_ndim(arr);
    // ARR_DIMS(arr)[0] is meaningful only for ndim >= 1; 0 otherwise.
    let dim0 = if ndim >= 1 { foundation::arr_dim(arr, 0) } else { 0 };
    let hasnull = foundation::arr_hasnull(arr);
    let elemtype = foundation::arr_elemtype(arr);
    (ndim, dim0, hasnull, elemtype)
}

/// Seam `oid_array_datum` — `DatumGetArrayTypeP(arraydatum)` (detoast) then
/// project the header + read `ARR_DATA_PTR` as a C `Oid[]` (the funcapi
/// `build_function_result_*` path reads OID arrays directly, not via
/// `deconstruct_array`).
pub fn oid_array_datum<'mcx>(
    mcx: Mcx<'mcx>,
    arraydatum: Datum,
) -> PgResult<types_namespace::OidArrayDatum<'mcx>> {
    let arr = detoast_seam::detoast_attr::call(mcx, datum_as_byte_window(arraydatum))?;
    let (ndim, dim0, hasnull, elemtype) = project_array_header(&arr);

    // values = ARR_DATA_PTR(arr) read as `dim0` Oids — only for a valid 1-D
    // non-null OID array (the consumer validates the shape and elog(ERROR)s
    // otherwise; here we project only the read shape, mirroring the C).
    let values = if ndim == 1 && !hasnull && dim0 >= 0 && elemtype == foundation::OIDOID {
        read_fixed4_oid_array(mcx, &arr, dim0)?
    } else {
        mcx::vec_with_capacity_in::<Oid>(mcx, 0)?
    };

    Ok(types_namespace::OidArrayDatum {
        ndim,
        dim0,
        hasnull,
        elemtype,
        values,
    })
}

/// Seam `char_array_datum` — `DatumGetArrayTypeP(arraydatum)` (detoast) then
/// project the header + read `ARR_DATA_PTR` as a C `"char"[]` (the funcapi path
/// reads `proargmodes` directly as `char[]`).
pub fn char_array_datum<'mcx>(
    mcx: Mcx<'mcx>,
    arraydatum: Datum,
) -> PgResult<types_namespace::CharArrayDatum<'mcx>> {
    let arr = detoast_seam::detoast_attr::call(mcx, datum_as_byte_window(arraydatum))?;
    let (ndim, dim0, hasnull, elemtype) = project_array_header(&arr);

    // "char" is a 1-byte pass-by-value type; read `dim0` raw bytes from
    // ARR_DATA_PTR for a valid 1-D non-null CHAR array.
    let values = if ndim == 1 && !hasnull && dim0 >= 0 && elemtype == foundation::CHAROID {
        let start = foundation::arr_data_ptr_off(&arr);
        let n = dim0 as usize;
        let mut v = mcx::vec_with_capacity_in::<u8>(mcx, n)?;
        for i in 0..n {
            v.push(*arr.get(start + i).ok_or_else(|| {
                PgError::error("malformed char[] array (truncated data)")
                    .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR)
            })?);
        }
        v
    } else {
        mcx::vec_with_capacity_in::<u8>(mcx, 0)?
    };

    Ok(types_namespace::CharArrayDatum {
        ndim,
        dim0,
        hasnull,
        elemtype,
        values,
    })
}

/// Seam `text_array_datum` — `DatumGetArrayTypeP(arraydatum)` (detoast) then
/// project the header + deconstruct the elements via
/// `deconstruct_array_builtin(arr, TEXTOID, ...)` and run each through
/// `TextDatumGetCString`.
pub fn text_array_datum<'mcx>(
    mcx: Mcx<'mcx>,
    arraydatum: Datum,
) -> PgResult<types_namespace::TextArrayDatum<'mcx>> {
    let arr = detoast_seam::detoast_attr::call(mcx, datum_as_byte_window(arraydatum))?;
    let (ndim, dim0, hasnull, elemtype) = project_array_header(&arr);

    // The element strings come from deconstruct_array_builtin + TextDatumGetCString,
    // but only for a valid 1-D non-null TEXT array (the consumer validates the
    // shape and elog(ERROR)s otherwise; here we project only the read shape).
    let values = if ndim == 1 && !hasnull && dim0 >= 0 && elemtype == foundation::TEXTOID {
        deconstruct_text_array(mcx, &arr)?
    } else {
        mcx::vec_with_capacity_in::<PgString<'mcx>>(mcx, 0)?
    };

    Ok(types_namespace::TextArrayDatum {
        ndim,
        dim0,
        hasnull,
        elemtype,
        values,
    })
}

/// Seam `array_get_float4_values` — the `stanumbers` extraction of
/// `get_attstatsslot` (lsyscache.c): detoast + copy the `Datum`
/// (`DatumGetArrayTypePCopy`), verify it is a 1-D no-NULLs `float4` array, and
/// return its element values (`ARR_DATA_PTR` viewed as `float4[narrayelem]`)
/// copied into `mcx`.
pub fn array_get_float4_values<'mcx>(
    mcx: Mcx<'mcx>,
    arraydatum: Datum,
) -> PgResult<mcx::PgVec<'mcx, f32>> {
    // statarray = DatumGetArrayTypePCopy(val);
    let arr = detoast_seam::detoast_attr::call(mcx, datum_as_byte_window(arraydatum))?;

    // narrayelem = ARR_DIMS(statarray)[0];
    let ndim = foundation::arr_ndim(&arr);
    let narrayelem = if ndim >= 1 { foundation::arr_dim(&arr, 0) } else { 0 };

    // if (ARR_NDIM(statarray) != 1 || narrayelem <= 0 ||
    //     ARR_HASNULL(statarray) || ARR_ELEMTYPE(statarray) != FLOAT4OID)
    //     elog(ERROR, "stanumbers is not a 1-D float4 array");
    if ndim != 1
        || narrayelem <= 0
        || foundation::arr_hasnull(&arr)
        || foundation::arr_elemtype(&arr) != foundation::FLOAT4OID
    {
        return Err(PgError::error("stanumbers is not a 1-D float4 array"));
    }

    // sslot->numbers = (float4 *) ARR_DATA_PTR(statarray); sslot->nnumbers = narrayelem;
    let start = foundation::arr_data_ptr_off(&arr);
    let n = narrayelem as usize;
    let mut out = mcx::vec_with_capacity_in::<f32>(mcx, n)?;
    for i in 0..n {
        let off = start + i * 4;
        let bytes = arr
            .get(off..off + 4)
            .ok_or_else(|| PgError::error("stanumbers is not a 1-D float4 array"))?;
        out.push(f32::from_ne_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]));
    }
    Ok(out)
}

/// Read a fixed-width 4-byte pass-by-value `Oid` element region from
/// `ARR_DATA_PTR(arr)` as `count` native-endian words. The C reads
/// `ARR_DATA_PTR` as an `Oid[]` directly (no per-element alignment padding for a
/// 4-byte int-aligned type).
fn read_fixed4_oid_array<'mcx>(
    mcx: Mcx<'mcx>,
    arr: &mcx::PgVec<'mcx, u8>,
    count: i32,
) -> PgResult<mcx::PgVec<'mcx, Oid>> {
    let start = foundation::arr_data_ptr_off(arr);
    let n = count.max(0) as usize;
    let mut v = mcx::vec_with_capacity_in::<Oid>(mcx, n)?;
    for i in 0..n {
        let off = start + i * 4;
        let bytes = arr.get(off..off + 4).ok_or_else(|| {
            PgError::error("malformed array (truncated element data)")
                .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR)
        })?;
        v.push(u32::from_ne_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]));
    }
    Ok(v)
}

// ---------------------------------------------------------------------------
// Byte-image array-decode seams (`&[u8]`-input). The canonical Datum model
// carries a by-reference catalog attribute as a `Datum::ByRef(bytes)` byte
// image (the deformed on-disk varlena). These take that image directly and run
// the standard decode (`DatumGetArrayTypeP` == detoast, then the element walk),
// avoiding the pointer-word `datum_as_byte_window` round-trip the Datum-input
// seams would otherwise need. Faithful to C: `DatumGetArrayTypeP(d)` ==
// `pg_detoast_datum((struct varlena *) DatumGetPointer(d))`.
// ---------------------------------------------------------------------------

/// Seam `deconstruct_array_bytes` — `deconstruct_array(DatumGetArrayTypeP(bytes),
/// elmtype, elmlen, elmbyval, elmalign, ...)` (arrayfuncs.c), reading the
/// on-disk array byte image directly. `DatumGetArrayTypeP` is `detoast_attr`.
pub fn deconstruct_array_bytes<'mcx>(
    mcx: Mcx<'mcx>,
    bytes: &[u8],
    elmtype: Oid,
    elmlen: i16,
    elmbyval: bool,
    elmalign: core::ffi::c_char,
) -> PgResult<PgVec<'mcx, (Datum, bool)>> {
    // DatumGetArrayTypeP(d) — detoast the on-disk array varlena image.
    let arr = detoast_seam::detoast_attr::call(mcx, bytes)?;
    deconstruct_array(
        mcx,
        &arr,
        elmtype,
        elmlen as i32,
        elmbyval,
        elmalign as u8,
    )
}

/// Seam `oidvector_to_oids_bytes` — `(oidvector *) DatumGetPointer(datum)` then
/// `->values[0 .. ->dim1]`, reading the on-disk `oidvector` byte image directly.
///
/// An `oidvector` is a 1-D `ArrayType` of `OIDOID` (4-byte pass-by-value,
/// int-aligned, no NULLs, lower bound 0 — `oidvectorin` constructs it that way),
/// so `ARR_DATA_PTR` is `dim1` consecutive native-endian `Oid` words. The C
/// reads `vec->values` directly (an `oidvector` is laid out as
/// `int32 ndim; int32 dataoffset; Oid elemtype; int dim1; int lbound1; Oid
/// values[];` — identical to a flat `ArrayType` header followed by the OID
/// array data). A zero-dimension vector yields an empty result.
pub fn oidvector_to_oids_bytes<'mcx>(
    mcx: Mcx<'mcx>,
    bytes: &[u8],
) -> PgResult<PgVec<'mcx, Oid>> {
    // DatumGetArrayTypeP(d) — detoast (oidvectors are PLAIN storage, so this is
    // the verbatim-copy fall-through, but route through the seam for parity).
    let arr = detoast_seam::detoast_attr::call(mcx, bytes)?;
    let ndim = foundation::arr_ndim(&arr);
    // dim1 == ARR_DIMS(vec)[0]; a 0-D vector (ndim == 0) has no elements.
    let dim1 = if ndim >= 1 { foundation::arr_dim(&arr, 0) } else { 0 };
    read_fixed4_oid_array(mcx, &arr, dim1)
}

/// Seam `int2vector_to_i16s_bytes` — `(int2vector *) DatumGetPointer(datum)`
/// then `->values[0 .. ->dim1]`, reading the on-disk `int2vector` byte image
/// directly.
///
/// An `int2vector` is a 1-D `ArrayType` of `INT2OID` (2-byte pass-by-value,
/// short-aligned, no NULLs, lower bound 0 — `int2vectorin` constructs it that
/// way), so `ARR_DATA_PTR` is `dim1` consecutive native-endian `int16` words
/// (the C reads `vec->values` directly; an `int2vector` is laid out as
/// `int32 ndim; int32 dataoffset; Oid elemtype; int dim1; int lbound1; int16
/// values[];` — a flat `ArrayType` header followed by the int16 array data).
/// A zero-dimension vector yields an empty result.
pub fn int2vector_to_i16s_bytes<'mcx>(
    mcx: Mcx<'mcx>,
    bytes: &[u8],
) -> PgResult<PgVec<'mcx, i16>> {
    // DatumGetArrayTypeP(d) — detoast (int2vectors are PLAIN storage, so this
    // is the verbatim-copy fall-through, but route through the seam for parity).
    let arr = detoast_seam::detoast_attr::call(mcx, bytes)?;
    let ndim = foundation::arr_ndim(&arr);
    // dim1 == ARR_DIMS(vec)[0]; a 0-D vector (ndim == 0) has no elements.
    let dim1 = if ndim >= 1 { foundation::arr_dim(&arr, 0) } else { 0 };
    let start = foundation::arr_data_ptr_off(&arr);
    let n = dim1.max(0) as usize;
    let mut v = mcx::vec_with_capacity_in::<i16>(mcx, n)?;
    for i in 0..n {
        let off = start + i * 2;
        let b = arr.get(off..off + 2).ok_or_else(|| {
            PgError::error("malformed array (truncated element data)")
                .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR)
        })?;
        v.push(i16::from_ne_bytes([b[0], b[1]]));
    }
    Ok(v)
}

/// Seam `text_array_to_strings_bytes` —
/// `deconstruct_array_builtin(DatumGetArrayTypeP(bytes), TEXTOID, ...)` then
/// `TextDatumGetCString` per element, reading the on-disk `text[]` byte image
/// directly. The text elements live inline in the array data area (each with
/// its natural short / 4-byte varlena header — never individually toasted), so
/// each is projected to its UTF-8 string straight from the buffer (mirroring
/// `VARDATA_ANY` / `VARSIZE_ANY_EXHDR`), without a per-element `Datum`
/// round-trip.
pub fn text_array_to_strings_bytes<'mcx>(
    mcx: Mcx<'mcx>,
    bytes: &[u8],
) -> PgResult<PgVec<'mcx, PgString<'mcx>>> {
    // DatumGetArrayTypeP(d) — detoast the on-disk array varlena image.
    let arr = detoast_seam::detoast_attr::call(mcx, bytes)?;

    // nelems = ArrayGetNItems(ARR_NDIM(array), ARR_DIMS(array));
    let ndim = foundation::arr_ndim(&arr);
    let dims = foundation::arr_dims(mcx, &arr)?;
    let nelems = arrayutils_seam::array_get_n_items::call(ndim, &dims)?;

    let mut out = mcx::vec_with_capacity_in::<PgString<'mcx>>(mcx, nelems as usize)?;

    // p = ARR_DATA_PTR(array); bitmap = ARR_NULLBITMAP(array); bitmask = 1;
    // (TEXT: typlen == -1, typbyval == false, typalign == 'i'.)
    let mut p = foundation::arr_data_ptr_off(&arr);
    let bitmap = foundation::arr_nullbitmap_off(&arr);
    let mut bitmap_byte = bitmap;
    let mut bitmask: i32 = 1;

    for _ in 0..nelems {
        let is_null_here = match bitmap_byte {
            Some(b) => (arr[b] as i32 & bitmask) == 0,
            None => false,
        };
        if is_null_here {
            // reloptions / proconfig text arrays have no NULLs; the C
            // TextDatumGetCString would dereference NULL — surface the same
            // null-not-allowed error.
            return Err(PgError::error("null array element not allowed in this context")
                .with_sqlstate(ERRCODE_NULL_VALUE_NOT_ALLOWED));
        }
        // The element is an inline text varlena at `arr[p..]`; project to its
        // UTF-8 payload (VARDATA_ANY / VARSIZE_ANY_EXHDR over the natural
        // short / 4-byte header), then advance past it (att_addlength_pointer +
        // att_align_nominal, the same walk as deconstruct_array).
        out.push(text_element_to_pgstring(mcx, &arr, p)?);
        // p = att_addlength_pointer(p, -1 /* TEXT typlen */, p);
        p = foundation::att_addlength_pointer(p, -1, &arr, p);
        // p = att_align_nominal(p, 'i');
        p = foundation::att_align_nominal(p, TYPALIGN_INT);

        if let Some(b) = bitmap_byte.as_mut() {
            bitmask <<= 1;
            if bitmask == 0x100 {
                *b += 1;
                bitmask = 1;
            }
        }
    }
    Ok(out)
}

/// `TextDatumGetCString` over an inline `text` element at offset `off` in the
/// array data area: read the natural (short or 4-byte header) varlena's payload
/// and return its UTF-8 string in `mcx`. Mirrors `text_to_cstring` /
/// `VARDATA_ANY` + `VARSIZE_ANY_EXHDR`.
fn text_element_to_pgstring<'mcx>(
    mcx: Mcx<'mcx>,
    arr: &[u8],
    off: usize,
) -> PgResult<PgString<'mcx>> {
    // VARDATA_ANY / VARSIZE_ANY_EXHDR: a 1-byte short header has a 1-byte
    // payload offset and `(VARSIZE_1B - 1)` payload bytes; a 4-byte header has
    // a 4-byte offset and `(VARSIZE_4B - 4)` payload bytes. (Array text
    // elements are never externally toasted.)
    let (data_off, data_len) = if foundation::varatt_is_1b(arr, off) {
        const VARHDRSZ_SHORT: usize = 1;
        (off + VARHDRSZ_SHORT, foundation::varsize_1b(arr, off) - VARHDRSZ_SHORT)
    } else {
        use types_datum::varlena::VARHDRSZ;
        (off + VARHDRSZ, foundation::varsize_4b(arr, off) - VARHDRSZ)
    };
    let payload = arr.get(data_off..data_off + data_len).ok_or_else(|| {
        PgError::error("malformed array (truncated element data)")
            .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR)
    })?;
    let text = core::str::from_utf8(payload).map_err(|_| {
        PgError::error("invalid UTF-8 in text array element")
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
    })?;
    let mut s = PgString::new_in(mcx);
    s.try_push_str(text)?;
    Ok(s)
}

// ---------------------------------------------------------------------------
// tsvector_op.c array<->element bridges (the `backend-utils-adt-array-more`
// seams). These read/build the on-disk array byte image directly — no global
// address space and no per-element Datum pointer round-trip — so they are the
// faithful owner-side bodies for the `text[]` / `"char"[]` / `int2[]` bridges
// that `tsvector_op.c` (and `jsonb`) drive through `deconstruct_array_builtin`
// / `construct_array_builtin`.
// ---------------------------------------------------------------------------

use backend_utils_adt_array_more_seams::ArrayElem;

/// Seam `deconstruct_text_array` — `deconstruct_array_builtin(arr, TEXTOID,
/// &dlexemes, &nulls, &nlexemes)` (tsvector_op.c:315). Explode the on-disk
/// `text[]` byte image into per-element `{ VARDATA(dlexemes[i]) for
/// VARSIZE(dlexemes[i]) - VARHDRSZ bytes, nulls[i] }`. The element bytes are the
/// raw `text` payload (no varlena header, may contain embedded NULs — `text` is
/// not NUL-terminated), captured as an owned `Vec<u8>` so no `mcx` lifetime
/// escapes. NULL elements carry an empty `value` with `is_null == true`.
pub fn deconstruct_text_array_elems(arr: &[u8]) -> PgResult<alloc::vec::Vec<ArrayElem>> {
    Ok(deconstruct_text_array_with_ndim_bytes(arr)?.1)
}

/// Seam `deconstruct_text_array_with_ndim` — like
/// [`deconstruct_text_array_elems`] but also returns `ARR_NDIM(arr)`, for the
/// jsonfuncs `text[]`-path operators that reproduce the C
/// `ARR_NDIM(path) > 1` guard.
pub fn deconstruct_text_array_with_ndim_bytes(
    arr: &[u8],
) -> PgResult<(i32, alloc::vec::Vec<ArrayElem>)> {
    // The seam returns fully-owned data; run the element walk in a private
    // transient context (the C `deconstruct_array_builtin` palloc'd workspace),
    // copying each element's payload out before the context drops.
    let cx = mcx::MemoryContext::new("tsvector deconstruct_text_array");
    let mcx = cx.mcx();

    // DatumGetArrayTypeP(arr) — detoast the array varlena image.
    let array = detoast_seam::detoast_attr::call(mcx, arr)?;

    let ndim = foundation::arr_ndim(&array);
    let dims = foundation::arr_dims(mcx, &array)?;
    let nelems = arrayutils_seam::array_get_n_items::call(ndim, &dims)?;

    let mut out: alloc::vec::Vec<ArrayElem> = alloc::vec::Vec::with_capacity(nelems as usize);

    // p = ARR_DATA_PTR(array); bitmap = ARR_NULLBITMAP(array); bitmask = 1;
    // (TEXT: typlen == -1, typbyval == false, typalign == 'i'.)
    let mut p = foundation::arr_data_ptr_off(&array);
    let bitmap = foundation::arr_nullbitmap_off(&array);
    let mut bitmap_byte = bitmap;
    let mut bitmask: i32 = 1;

    for _ in 0..nelems {
        let is_null_here = match bitmap_byte {
            Some(b) => (array[b] as i32 & bitmask) == 0,
            None => false,
        };
        if is_null_here {
            out.push(ArrayElem { value: alloc::vec::Vec::new(), is_null: true });
        } else {
            // lex = VARDATA(dlexemes[i]); lex_len = VARSIZE(dlexemes[i]) - VARHDRSZ.
            // The element is an inline text varlena at `array[p..]` (natural
            // short / 4-byte header, never externally toasted in array data).
            let (data_off, data_len) = text_element_payload_span(&array, p)?;
            let payload = array.get(data_off..data_off + data_len).ok_or_else(|| {
                PgError::error("malformed array (truncated element data)")
                    .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR)
            })?;
            out.push(ArrayElem { value: payload.to_vec(), is_null: false });
            // p = att_addlength_pointer(p, -1, p); p = att_align_nominal(p, 'i').
            p = foundation::att_addlength_pointer(p, -1, &array, p);
            p = foundation::att_align_nominal(p, TYPALIGN_INT);
        }

        if let Some(b) = bitmap_byte.as_mut() {
            bitmask <<= 1;
            if bitmask == 0x100 {
                *b += 1;
                bitmask = 1;
            }
        }
    }
    Ok((ndim, out))
}

/// Like [`deconstruct_text_array_with_ndim_bytes`] but also returns the full
/// `ARR_DIMS(arr)` vector (the per-dimension extents), for `jsonb_object`'s
/// `ARR_DIMS(in_array)[0]` (even-element) / `ARR_DIMS(in_array)[1]` (two-column)
/// `ndims`-dependent shape checks. The element walk is identical; only the dims
/// vector is additionally captured (copied to an owned `Vec<i32>`).
pub fn deconstruct_text_array_with_dims_bytes(
    arr: &[u8],
) -> PgResult<(i32, alloc::vec::Vec<i32>, alloc::vec::Vec<ArrayElem>)> {
    let cx = mcx::MemoryContext::new("jsonb deconstruct_text_array_with_dims");
    let mcx = cx.mcx();

    // DatumGetArrayTypeP(arr) — detoast the array varlena image.
    let array = detoast_seam::detoast_attr::call(mcx, arr)?;

    let ndim = foundation::arr_ndim(&array);
    let dims_v = foundation::arr_dims(mcx, &array)?;
    let dims_owned: alloc::vec::Vec<i32> = dims_v.iter().copied().collect();
    let nelems = arrayutils_seam::array_get_n_items::call(ndim, &dims_v)?;

    let mut out: alloc::vec::Vec<ArrayElem> = alloc::vec::Vec::with_capacity(nelems as usize);

    // p = ARR_DATA_PTR(array); bitmap = ARR_NULLBITMAP(array); bitmask = 1.
    // (TEXT: typlen == -1, typbyval == false, typalign == 'i'.)
    let mut p = foundation::arr_data_ptr_off(&array);
    let bitmap = foundation::arr_nullbitmap_off(&array);
    let mut bitmap_byte = bitmap;
    let mut bitmask: i32 = 1;

    for _ in 0..nelems {
        let is_null_here = match bitmap_byte {
            Some(b) => (array[b] as i32 & bitmask) == 0,
            None => false,
        };
        if is_null_here {
            out.push(ArrayElem { value: alloc::vec::Vec::new(), is_null: true });
        } else {
            let (data_off, data_len) = text_element_payload_span(&array, p)?;
            let payload = array.get(data_off..data_off + data_len).ok_or_else(|| {
                PgError::error("malformed array (truncated element data)")
                    .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR)
            })?;
            out.push(ArrayElem { value: payload.to_vec(), is_null: false });
            p = foundation::att_addlength_pointer(p, -1, &array, p);
            p = foundation::att_align_nominal(p, TYPALIGN_INT);
        }

        if let Some(b) = bitmap_byte.as_mut() {
            bitmask <<= 1;
            if bitmask == 0x100 {
                *b += 1;
                bitmask = 1;
            }
        }
    }
    Ok((ndim, dims_owned, out))
}

/// Seam `deconstruct_char_array` — `deconstruct_array_builtin(weights, CHAROID,
/// &dweights, &nulls, &nweights)` (tsvector_op.c:836). Explode the on-disk
/// `"char"[]` byte image into per-element `{ DatumGetChar(dweights[i]) as one
/// byte, nulls[i] }`. `"char"` is a 1-byte pass-by-value type (typlen == 1,
/// typbyval == true, typalign == 'c'), so each non-null element is exactly one
/// data byte. NULL elements carry an empty `value` with `is_null == true`.
pub fn deconstruct_char_array_elems(arr: &[u8]) -> PgResult<alloc::vec::Vec<ArrayElem>> {
    let cx = mcx::MemoryContext::new("tsvector deconstruct_char_array");
    let mcx = cx.mcx();

    // DatumGetArrayTypeP(weights) — detoast the array varlena image.
    let array = detoast_seam::detoast_attr::call(mcx, arr)?;

    let ndim = foundation::arr_ndim(&array);
    let dims = foundation::arr_dims(mcx, &array)?;
    let nelems = arrayutils_seam::array_get_n_items::call(ndim, &dims)?;

    let mut out: alloc::vec::Vec<ArrayElem> = alloc::vec::Vec::with_capacity(nelems as usize);

    // p = ARR_DATA_PTR(array); bitmap = ARR_NULLBITMAP(array); bitmask = 1.
    let mut p = foundation::arr_data_ptr_off(&array);
    let bitmap = foundation::arr_nullbitmap_off(&array);
    let mut bitmap_byte = bitmap;
    let mut bitmask: i32 = 1;

    for _ in 0..nelems {
        let is_null_here = match bitmap_byte {
            Some(b) => (array[b] as i32 & bitmask) == 0,
            None => false,
        };
        if is_null_here {
            out.push(ArrayElem { value: alloc::vec::Vec::new(), is_null: true });
        } else {
            // fetch_att(p, true, 1) — a single by-value `"char"` byte.
            let b = *array.get(p).ok_or_else(|| {
                PgError::error("malformed array (truncated element data)")
                    .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR)
            })?;
            out.push(ArrayElem { value: alloc::vec![b], is_null: false });
            // p = att_addlength_pointer(p, 1, p); p = att_align_nominal(p, 'c').
            p = foundation::att_addlength_pointer(p, 1, &array, p);
            p = foundation::att_align_nominal(p, TYPALIGN_CHAR);
        }

        if let Some(b) = bitmap_byte.as_mut() {
            bitmask <<= 1;
            if bitmask == 0x100 {
                *b += 1;
                bitmask = 1;
            }
        }
    }
    Ok(out)
}

/// Seam `construct_text_array` — `construct_array_builtin(elements, n, TEXTOID)`
/// (tsvector_op.c:736, where each `elements[i]` is
/// `cstring_to_text_with_len(...)`). Build a 1-D, no-NULL `text[]` byte image
/// from the per-element raw text payload bytes. Returns the owned array varlena
/// image bytes (the C result of `PointerGetDatum(construct_array_builtin(...))`,
/// captured by value).
pub fn construct_text_array_bytes(elems: &[alloc::vec::Vec<u8>]) -> PgResult<alloc::vec::Vec<u8>> {
    use types_tuple::backend_access_common_heaptuple::Datum as TDatum;

    let cx = mcx::MemoryContext::new("tsvector construct_text_array");
    let mcx = cx.mcx();

    // Each element is cstring_to_text_with_len(bytes, len): a text varlena
    // (4-byte natural header + payload) carried as a ByRef value.
    let mut values: alloc::vec::Vec<TDatum> = alloc::vec::Vec::with_capacity(elems.len());
    for e in elems {
        values.push(TDatum::ByRef(text_varlena_pgvec(mcx, e)?));
    }
    let nulls = alloc::vec![false; elems.len()];

    // construct_array_builtin(elements, n, TEXTOID): 1-D, no multidims.
    let (elmlen, elmbyval, elmalign) = construct_builtin_meta(foundation::TEXTOID)?;
    let buf = construct_array_expr(
        mcx,
        &values,
        &nulls,
        foundation::TEXTOID,
        elmlen as i16,
        elmbyval,
        elmalign,
        false,
    )?;
    Ok(buf.as_slice().to_vec())
}

/// Seam `construct_int2_array` — `construct_array_builtin(positions, npos,
/// INT2OID)` (tsvector_op.c:699). Build a 1-D, no-NULL `int2[]` byte image from
/// the per-element `int16` values. Returns the owned array varlena image bytes.
pub fn construct_int2_array_bytes(elems: &[i16]) -> PgResult<alloc::vec::Vec<u8>> {
    use types_tuple::backend_access_common_heaptuple::Datum as TDatum;

    let cx = mcx::MemoryContext::new("tsvector construct_int2_array");
    let mcx = cx.mcx();

    // INT2OID is 2-byte pass-by-value: each element's scalar word is the i16,
    // matching `Int16GetDatum`.
    let values: alloc::vec::Vec<TDatum> =
        elems.iter().map(|&v| TDatum::ByVal((v as u16) as usize)).collect();
    let nulls = alloc::vec![false; elems.len()];

    let (elmlen, elmbyval, elmalign) = construct_builtin_meta(foundation::INT2OID)?;
    let buf = construct_array_expr(
        mcx,
        &values,
        &nulls,
        foundation::INT2OID,
        elmlen as i16,
        elmbyval,
        elmalign,
        false,
    )?;
    Ok(buf.as_slice().to_vec())
}

/// Seam `construct_int4_array` — `construct_array_builtin(datums, n, INT4OID)`
/// (mcxtfuncs.c `int_list_to_array`). Build a 1-D, no-NULL `int4[]` byte image
/// from the per-element `int32` values (the `path` of
/// `pg_get_backend_memory_contexts`). Returns the owned array varlena image
/// bytes (mirrors [`construct_int2_array_bytes`]).
pub fn construct_int4_array_bytes(elems: &[i32]) -> PgResult<alloc::vec::Vec<u8>> {
    use types_tuple::backend_access_common_heaptuple::Datum as TDatum;

    let cx = mcx::MemoryContext::new("mcxtfuncs construct_int4_array");
    let mcx = cx.mcx();

    // INT4OID is 4-byte pass-by-value: each element's scalar word is the i32,
    // matching `Int32GetDatum`.
    let values: alloc::vec::Vec<TDatum> =
        elems.iter().map(|&v| TDatum::ByVal((v as u32) as usize)).collect();
    let nulls = alloc::vec![false; elems.len()];

    let (elmlen, elmbyval, elmalign) = construct_builtin_meta(foundation::INT4OID)?;
    let buf = construct_array_expr(
        mcx,
        &values,
        &nulls,
        foundation::INT4OID,
        elmlen as i16,
        elmbyval,
        elmalign,
        false,
    )?;
    Ok(buf.as_slice().to_vec())
}

/// Seam `deconstruct_text_array_with_dims` — `deconstruct_array_builtin(arr,
/// TEXTOID, &elems, &nulls, &nelems)` (arrayfuncs.c) keeping the array's shape
/// header. Detoast a `text[]` image, then return `(ARR_NDIM, ARR_DIMS,
/// elements)`, each element `Some(VARDATA_ANY payload)` or `None` for a NULL.
/// The dims vector carries the `xpath_internal` namespace-mapping shape check
/// (`ndim != 2 || dims[1] != 2`) the deconstruction itself omits.
pub fn deconstruct_text_array_with_dims_seam(
    arr: &[u8],
) -> PgResult<(i32, alloc::vec::Vec<i32>, alloc::vec::Vec<Option<alloc::vec::Vec<u8>>>)> {
    let cx = mcx::MemoryContext::new("xpath deconstruct_text_array_with_dims");
    let mcx = cx.mcx();

    // DatumGetArrayTypeP(arr) — detoast the array varlena image.
    let array = detoast_seam::detoast_attr::call(mcx, arr)?;

    let ndim = foundation::arr_ndim(&array);
    let dims_v = foundation::arr_dims(mcx, &array)?;
    let dims_owned: alloc::vec::Vec<i32> = dims_v.iter().copied().collect();
    let nelems = arrayutils_seam::array_get_n_items::call(ndim, &dims_v)?;

    let mut out: alloc::vec::Vec<Option<alloc::vec::Vec<u8>>> =
        alloc::vec::Vec::with_capacity(nelems as usize);

    // p = ARR_DATA_PTR(array); bitmap = ARR_NULLBITMAP(array); bitmask = 1.
    // (TEXT: typlen == -1, typbyval == false, typalign == 'i'.)
    let mut p = foundation::arr_data_ptr_off(&array);
    let bitmap = foundation::arr_nullbitmap_off(&array);
    let mut bitmap_byte = bitmap;
    let mut bitmask: i32 = 1;

    for _ in 0..nelems {
        let is_null_here = match bitmap_byte {
            Some(b) => (array[b] as i32 & bitmask) == 0,
            None => false,
        };
        if is_null_here {
            out.push(None);
        } else {
            let (data_off, data_len) = text_element_payload_span(&array, p)?;
            let payload = array.get(data_off..data_off + data_len).ok_or_else(|| {
                PgError::error("malformed array (truncated element data)")
                    .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR)
            })?;
            out.push(Some(payload.to_vec()));
            p = foundation::att_addlength_pointer(p, -1, &array, p);
            p = foundation::att_align_nominal(p, TYPALIGN_INT);
        }

        if let Some(b) = bitmap_byte.as_mut() {
            bitmask <<= 1;
            if bitmask == 0x100 {
                *b += 1;
                bitmask = 1;
            }
        }
    }
    Ok((ndim, dims_owned, out))
}

/// Seam `construct_xml_array_bytes` — `initArrayResult(XMLOID, ...)` /
/// `accumArrayResult` / `makeArrayResult` over `XMLOID` (the
/// `xml_xpathobjtoxmlarray` accumulation, xml.c:4243). Build a 1-D, no-NULL
/// `xml[]` byte image from per-element raw `xml` payload bytes; each element is
/// a `cstring_to_xmltype` header-ful varlena (binary-compatible with `text`:
/// `elmlen = -1`, `elmbyval = false`, `elmalign = 'i'`). An empty input yields
/// `construct_empty_array(XMLOID)`. Returns the owned flat array varlena image.
pub fn construct_xml_array_bytes(
    elems: &[alloc::vec::Vec<u8>],
) -> PgResult<alloc::vec::Vec<u8>> {
    use types_tuple::backend_access_common_heaptuple::Datum as TDatum;

    let cx = mcx::MemoryContext::new("xpath construct_xml_array");
    let mcx = cx.mcx();

    if elems.is_empty() {
        // makeArrayResult of an empty ArrayBuildState == construct_empty_array.
        let buf = construct_empty_array(mcx, types_tuple::heaptuple::XMLOID)?;
        return Ok(buf.as_slice().to_vec());
    }

    // Each element is cstring_to_xmltype(bytes): a header-ful varlena (4-byte
    // natural header + payload) carried as a ByRef value, identical framing to
    // a text element since `xml` is binary-compatible with `text`.
    let mut values: alloc::vec::Vec<TDatum> = alloc::vec::Vec::with_capacity(elems.len());
    for e in elems {
        values.push(TDatum::ByRef(text_varlena_pgvec(mcx, e)?));
    }
    let nulls = alloc::vec![false; elems.len()];

    // XMLOID storage attributes: elmlen = -1, elmbyval = false, elmalign = 'i'
    // (xml is a varlena type, binary-compatible with text).
    let buf = construct_array_expr(
        mcx,
        &values,
        &nulls,
        types_tuple::heaptuple::XMLOID,
        -1,
        false,
        TYPALIGN_INT,
        false,
    )?;
    Ok(buf.as_slice().to_vec())
}

/// `VARDATA_ANY` / `VARSIZE_ANY_EXHDR` over an inline `text` element at offset
/// `off`: return its `(payload_offset, payload_len)` span (the natural short /
/// 4-byte header form; array text elements are never externally toasted).
fn text_element_payload_span(arr: &[u8], off: usize) -> PgResult<(usize, usize)> {
    if foundation::varatt_is_1b(arr, off) {
        const VARHDRSZ_SHORT: usize = 1;
        Ok((off + VARHDRSZ_SHORT, foundation::varsize_1b(arr, off) - VARHDRSZ_SHORT))
    } else {
        use types_datum::varlena::VARHDRSZ;
        Ok((off + VARHDRSZ, foundation::varsize_4b(arr, off) - VARHDRSZ))
    }
}

/// `cstring_to_text_with_len(bytes, len)` — build a natural (4-byte header)
/// text varlena image in `mcx` from raw payload bytes (may contain embedded
/// NULs), returned as the ByRef payload `PgVec`.
fn text_varlena_pgvec<'mcx>(mcx: Mcx<'mcx>, bytes: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    use types_datum::varlena::VARHDRSZ;
    let total = VARHDRSZ + bytes.len();
    let mut buf = mcx::vec_with_capacity_in::<u8>(mcx, total)?;
    buf.resize(total, 0);
    foundation::set_varsize(&mut buf, total);
    buf[VARHDRSZ..].copy_from_slice(bytes);
    Ok(buf)
}

// ---------------------------------------------------------------------------
// Datum / payload bridges for the byref element model. The text / tid
// projection of element bytes is the text/tableam owner's surface; route it
// through the owner seams (loud panic until they land) rather than inventing a
// local interpretation of the pointer word.
// ---------------------------------------------------------------------------

/// `PointerGetDatum(buf)` — the carried Datum is the result buffer's pointer
/// word.
fn datum_from_buf(buf: PgVec<'_, u8>) -> Datum {
    let ptr = buf.as_ptr() as usize;
    // The buffer lives in `mcx` for `'mcx`; leak the handle so the bytes
    // outlive this frame exactly as the C palloc'd array does (reclaimed by the
    // owning context, not by Rust drop).
    core::mem::forget(buf);
    Datum::from_usize(ptr)
}

/// Copy a verbatim by-reference element image (the full on-disk bytes a by-ref
/// `Datum` points at — e.g. the complete varlena image, the `attlen`-byte
/// fixed-length image, or the NUL-terminated cstring image) into `mcx` and
/// return a `Datum` whose pointer word targets that live copy.
///
/// `array_agg_transfn`'s element arrives on the fmgr by-reference lane
/// (`fcinfo->args[1]` is a pass-by-ref type, so its payload rides
/// `FmgrArgRef`, not the bare by-value word). C reads it with `PG_GETARG_DATUM`,
/// which yields a real pointer into the call's argument image; here we rebuild
/// that pointer by copying the lane bytes into the (aggcontext) `mcx`, so the
/// `accumArrayResult` by-ref copy path (`PG_DETOAST_DATUM_COPY` / `datumCopy`)
/// has a valid pointer to read. Without this, reading the bare by-value word for
/// a by-ref element yields a NULL/garbage pointer and `accumArrayResult`'s
/// varlena-header deref segfaults.
pub fn byref_image_to_datum<'mcx>(mcx: Mcx<'mcx>, image: &[u8]) -> PgResult<Datum> {
    let buf = slice_to_pgvec(mcx, image)?;
    Ok(datum_from_buf(buf))
}

/// `CStringGetTextDatum(s)` over a UTF-8 str: build a text varlena in `mcx` and
/// return its pointer word.
fn cstring_to_text_datum<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<Datum> {
    use types_datum::varlena::VARHDRSZ;
    let total = VARHDRSZ + s.len();
    let mut buf = mcx::vec_with_capacity_in::<u8>(mcx, total)?;
    buf.resize(total, 0);
    // SET_VARSIZE(buf, total): 4-byte header in the natural (non-toasted) form.
    foundation::set_varsize(&mut buf, total);
    buf[VARHDRSZ..].copy_from_slice(s.as_bytes());
    Ok(datum_from_buf(buf))
}

/// `cstring_to_text_with_len(bytes, len)` then `PointerGetDatum`: build a text
/// varlena in `mcx` from raw payload bytes (which may contain embedded NULs, as
/// `text` is not NUL-terminated) and return its pointer word. The bytes carrier
/// of `CStringGetTextDatum` for `text_to_array`'s split fields.
fn cstring_bytes_to_text_datum<'mcx>(mcx: Mcx<'mcx>, bytes: &[u8]) -> PgResult<Datum> {
    use types_datum::varlena::VARHDRSZ;
    let total = VARHDRSZ + bytes.len();
    let mut buf = mcx::vec_with_capacity_in::<u8>(mcx, total)?;
    buf.resize(total, 0);
    foundation::set_varsize(&mut buf, total);
    buf[VARHDRSZ..].copy_from_slice(bytes);
    Ok(datum_from_buf(buf))
}

/// Project a text varlena's payload bytes to a `PgString<'mcx>`.
fn text_to_pgstring<'mcx>(mcx: Mcx<'mcx>, bytes: &[u8]) -> PgResult<PgString<'mcx>> {
    use types_datum::varlena::VARHDRSZ;
    // VARDATA / VARSIZE: payload is [VARHDRSZ .. VARSIZE).
    let total = foundation::varsize_any(bytes, 0);
    let payload = &bytes[VARHDRSZ..total.min(bytes.len())];
    let mut s = PgString::new_in(mcx);
    let text = core::str::from_utf8(payload).map_err(|_| {
        PgError::error("invalid UTF-8 in text array element")
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
    })?;
    s.try_push_str(text)?;
    Ok(s)
}

/// Reinterpret a value-lane `tid` element (the 6 verbatim stored bytes carried
/// as a `Datum::ByRef`, produced by `deconstruct_array_values`) as an
/// `ItemPointerData`. This reads the captured bytes by value — it never
/// dereferences a bare-word offset as a pointer (the SIGSEGV the owned model
/// avoids for pass-by-reference elements).
fn item_pointer_from_value<'mcx>(
    _mcx: Mcx<'mcx>,
    d: &types_tuple::Datum<'mcx>,
) -> PgResult<ItemPointerData> {
    use types_tuple::backend_access_common_heaptuple::Datum as TDatum;
    let bytes: &[u8] = match d {
        TDatum::ByRef(b) => b,
        other => {
            return Err(PgError::error(format!(
                "tid array element is not a by-reference value: {other:?}"
            ))
            .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR));
        }
    };
    // ItemPointerData = { BlockIdData { bi_hi: u16, bi_lo: u16 }, ip_posid: u16 }
    if bytes.len() < 6 {
        return Err(PgError::error("malformed tid array element")
            .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR));
    }
    // Little-endian on-disk layout of ItemPointerData (bi_hi, bi_lo, ip_posid),
    // each a u16, exactly as the catalog stores a `tid`.
    Ok(ItemPointerData {
        ip_blkid: types_tuple::heaptuple::BlockIdData {
            bi_hi: u16::from_le_bytes([bytes[0], bytes[1]]),
            bi_lo: u16::from_le_bytes([bytes[2], bytes[3]]),
        },
        ip_posid: u16::from_le_bytes([bytes[4], bytes[5]]),
    })
}

/// Reinterpret a `tid` element Datum (pointer word into a 6-byte
/// `ItemPointerData`) as the value, reading its bytes through the byref owner.
#[allow(dead_code)]
fn datum_to_item_pointer<'mcx>(mcx: Mcx<'mcx>, d: Datum) -> PgResult<ItemPointerData> {
    let bytes = datum_payload_bytes(mcx, foundation::SIZEOF_ITEM_POINTER_DATA, d)?;
    // ItemPointerData = { BlockIdData { bi_hi: u16, bi_lo: u16 }, ip_posid: u16 }
    if bytes.len() < 6 {
        return Err(PgError::error("malformed tid array element")
            .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR));
    }
    // Little-endian on-disk layout of ItemPointerData (bi_hi, bi_lo, ip_posid),
    // each a u16, exactly as the catalog stores a `tid`.
    let ip = ItemPointerData {
        ip_blkid: types_tuple::heaptuple::BlockIdData {
            bi_hi: u16::from_le_bytes([bytes[0], bytes[1]]),
            bi_lo: u16::from_le_bytes([bytes[2], bytes[3]]),
        },
        ip_posid: u16::from_le_bytes([bytes[4], bytes[5]]),
    };
    Ok(ip)
}

/// Re-export of the on-disk header type for build-state finalizers.
pub type Header = ArrayType;

// ---------------------------------------------------------------------------
// Round-trip tests for the 6-arm value-lane construct path
// (construct_array_values / construct_md_array_values / construct_md_array_nested
// / construct_array_expr) against deconstruct_array.
// ---------------------------------------------------------------------------
#[cfg(test)]
mod value_lane_tests {
    use super::*;
    use mcx::MemoryContext;
    use std::sync::{Mutex, MutexGuard};
    use types_tuple::backend_access_common_heaptuple::Datum as TDatum;

    use backend_access_common_detoast_seams as detoast;
    use backend_utils_adt_arrayutils_seams as arrayutils;

    // The construct/deconstruct paths drive process-global seams; serialize the
    // install->use window across parallel tests (see sql.rs iterator_tests).
    static SEAM_MUTEX: Mutex<()> = Mutex::new(());

    fn lock() -> MutexGuard<'static, ()> {
        SEAM_MUTEX.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn install_seams() {
        if !arrayutils::array_get_n_items::is_installed() {
            arrayutils::array_get_n_items::set(|ndim: i32, dims: &[i32]| {
                let mut ret: i32 = 1;
                for i in 0..ndim.max(0) as usize {
                    ret = ret.checked_mul(dims[i]).expect("ArrayGetNItems overflow");
                }
                Ok(ret)
            });
        }
        if !arrayutils::array_check_bounds::is_installed() {
            // The fixtures here use lbound 1 and small dims, never overflowing.
            arrayutils::array_check_bounds::set(|_ndim, _dims, _lb| Ok(()));
        }
        if !detoast::detoast_attr::is_installed() {
            // Test elements are already flat varlenas; detoast is identity (copy
            // the verbatim bytes into mcx), exactly as PG_DETOAST_DATUM is for a
            // non-toasted value.
            detoast::detoast_attr::set(|mcx: Mcx<'_>, attr: &[u8]| {
                let mut v = mcx::vec_with_capacity_in::<u8>(mcx, attr.len())?;
                v.extend_from_slice(attr);
                Ok(v)
            });
        }
    }

    /// Build a flat `text` varlena image (4-byte header + payload).
    fn text_varlena(s: &str) -> Vec<u8> {
        let total = 4 + s.len();
        let mut b = vec![0u8; total];
        let word = (total as u32) << 2; // SET_VARSIZE (non-toasted, 4-byte header)
        b[0..4].copy_from_slice(&word.to_ne_bytes());
        b[4..].copy_from_slice(s.as_bytes());
        b
    }

    #[test]
    fn roundtrip_int4_1d_byval() {
        let _g = lock();
        install_seams();
        let ctx = MemoryContext::new("rt_int4");
        let mcx = ctx.mcx();

        let vals = [10i32, 20, 30, -7];
        let elems: Vec<TDatum> = vals.iter().map(|&v| TDatum::from_i32(v)).collect();

        // (elmlen=4, byval=true, align='i') for int4.
        let img = construct_array_values(mcx, &elems, foundation::INT4OID, 4, true, TYPALIGN_INT)
            .expect("construct");

        assert_eq!(foundation::arr_ndim(&img), 1);
        assert_eq!(foundation::arr_elemtype(&img), foundation::INT4OID);
        assert!(!foundation::arr_hasnull(&img));

        let out = deconstruct_array(mcx, &img, foundation::INT4OID, 4, true, TYPALIGN_INT)
            .expect("deconstruct");
        assert_eq!(out.len(), vals.len());
        for (i, &v) in vals.iter().enumerate() {
            assert!(!out[i].1);
            assert_eq!(out[i].0.as_i32(), v);
        }
    }

    #[test]
    fn roundtrip_int4_1d_with_null() {
        let _g = lock();
        install_seams();
        let ctx = MemoryContext::new("rt_int4_null");
        let mcx = ctx.mcx();

        let elems = [TDatum::from_i32(1), TDatum::null(), TDatum::from_i32(3)];
        let nulls = [false, true, false];

        let img = construct_md_array_values(
            mcx,
            &elems,
            Some(&nulls),
            1,
            &[3],
            &[1],
            foundation::INT4OID,
            4,
            true,
            TYPALIGN_INT,
        )
        .expect("construct");

        assert!(foundation::arr_hasnull(&img));
        let out = deconstruct_array(mcx, &img, foundation::INT4OID, 4, true, TYPALIGN_INT)
            .expect("deconstruct");
        assert_eq!(out.len(), 3);
        assert!(!out[0].1 && out[0].0.as_i32() == 1);
        assert!(out[1].1);
        assert!(!out[2].1 && out[2].0.as_i32() == 3);
    }

    #[test]
    fn roundtrip_text_1d_byref() {
        let _g = lock();
        install_seams();
        let ctx = MemoryContext::new("rt_text");
        let mcx = ctx.mcx();

        let words = ["alpha", "", "gamma"];
        let elems: Vec<TDatum> = words
            .iter()
            .map(|w| {
                let mut v = mcx::PgVec::<u8>::new_in(mcx);
                v.extend_from_slice(&text_varlena(w));
                TDatum::ByRef(v)
            })
            .collect();

        // (elmlen=-1, byval=false, align='i') for text.
        let img = construct_array_values(mcx, &elems, foundation::TEXTOID, -1, false, TYPALIGN_INT)
            .expect("construct");

        assert_eq!(foundation::arr_ndim(&img), 1);
        assert_eq!(foundation::arr_elemtype(&img), foundation::TEXTOID);

        let out = deconstruct_array(mcx, &img, foundation::TEXTOID, -1, false, TYPALIGN_INT)
            .expect("deconstruct");
        assert_eq!(out.len(), 3);
        for (i, w) in words.iter().enumerate() {
            assert!(!out[i].1);
            // out[i].0 is a pointer-word into the array image; read the varlena
            // payload back out of the image at that offset.
            let off = out[i].0.as_usize();
            let total = foundation::varsize_any(&img, off);
            let payload = &img[off + 4..off + total];
            assert_eq!(payload, w.as_bytes());
        }
    }

    #[test]
    fn roundtrip_array_expr_scalar() {
        let _g = lock();
        install_seams();
        let ctx = MemoryContext::new("rt_expr_scalar");
        let mcx = ctx.mcx();

        // ARRAY[5, 6] of int4 (multidims=false).
        let elems = [TDatum::from_i32(5), TDatum::from_i32(6)];
        let nulls = [false, false];
        let img = construct_array_expr(
            mcx,
            &elems,
            &nulls,
            foundation::INT4OID,
            4,
            true,
            TYPALIGN_INT,
            false,
        )
        .expect("construct_array_expr");

        let out = deconstruct_array(mcx, &img, foundation::INT4OID, 4, true, TYPALIGN_INT)
            .expect("deconstruct");
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].0.as_i32(), 5);
        assert_eq!(out[1].0.as_i32(), 6);
    }

    #[test]
    fn roundtrip_array_expr_nested() {
        let _g = lock();
        install_seams();
        let ctx = MemoryContext::new("rt_expr_nested");
        let mcx = ctx.mcx();

        // Build two 1-D int4 sub-arrays {1,2} and {3,4} as element values, then
        // ARRAY[ {1,2}, {3,4} ] (multidims=true) => a 2x2 array {{1,2},{3,4}}.
        let sub1 = construct_array_values(
            mcx,
            &[TDatum::from_i32(1), TDatum::from_i32(2)],
            foundation::INT4OID,
            4,
            true,
            TYPALIGN_INT,
        )
        .expect("sub1");
        let sub2 = construct_array_values(
            mcx,
            &[TDatum::from_i32(3), TDatum::from_i32(4)],
            foundation::INT4OID,
            4,
            true,
            TYPALIGN_INT,
        )
        .expect("sub2");

        let mut s1 = mcx::PgVec::<u8>::new_in(mcx);
        s1.extend_from_slice(&sub1);
        let mut s2 = mcx::PgVec::<u8>::new_in(mcx);
        s2.extend_from_slice(&sub2);
        let elems = [TDatum::ByRef(s1), TDatum::ByRef(s2)];
        let nulls = [false, false];

        let img = construct_array_expr(
            mcx,
            &elems,
            &nulls,
            foundation::INT4OID,
            4,
            true,
            TYPALIGN_INT,
            true,
        )
        .expect("construct_array_expr nested");

        assert_eq!(foundation::arr_ndim(&img), 2);
        assert_eq!(foundation::arr_dim(&img, 0), 2);
        assert_eq!(foundation::arr_dim(&img, 1), 2);

        let out = deconstruct_array(mcx, &img, foundation::INT4OID, 4, true, TYPALIGN_INT)
            .expect("deconstruct");
        let got: Vec<i32> = out.iter().map(|(d, _)| d.as_i32()).collect();
        assert_eq!(got, vec![1, 2, 3, 4]);
    }

    #[test]
    fn array_expr_all_empty_returns_empty() {
        let _g = lock();
        install_seams();
        let ctx = MemoryContext::new("rt_expr_empty");
        let mcx = ctx.mcx();

        // ARRAY[ NULL::int4[] ] (multidims) -> empty array (haveempty, ndims==0).
        let elems = [TDatum::null()];
        let nulls = [true];
        let img = construct_array_expr(
            mcx,
            &elems,
            &nulls,
            foundation::INT4OID,
            4,
            true,
            TYPALIGN_INT,
            true,
        )
        .expect("construct_array_expr empty");
        assert_eq!(foundation::arr_ndim(&img), 0);
    }
}
