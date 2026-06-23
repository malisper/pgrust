//! Element/slice family: scalar element get/set (`array_get_element` /
//! `array_set_element` and their `_expanded` forms, `array_ref` /
//! `array_set`), slice get/set (`array_get_slice` / `array_set_slice`), and
//! the dimension introspection functions (`array_ndims`, `array_dims`,
//! `array_lower`, `array_upper`, `array_length`, `array_cardinality`).
//!
//! # Datum / detoast boundary
//!
//! The C entry points take a `Datum arraydatum`, then either treat it as a
//! fixed-length array pointer, dispatch to the expanded-array path
//! (`VARATT_IS_EXTERNAL_EXPANDED`), or `DatumGetArrayTypeP` (detoast).  In this
//! port the detoast / expanded-vs-flat dispatch is the caller's responsibility
//! (TOAST + expanded datums are separate subsystems); these functions take the
//! already-flat array byte buffer `array: &[u8]` plus the `arraytyplen` flag so
//! the fixed-length branch is still ported 1:1.
//!
//! For a by-reference element value passed to a set operation, the C reads
//! `DatumGetPointer(src)` directly (after the caller has detoasted it); we
//! mirror that exactly via [`datum_ptr_bytes`].

use mcx::{Mcx, PgVec};
use ::array::MAXDIM;
use ::datum::datum::Datum;
use types_error::{
    PgError, PgResult, ERRCODE_ARRAY_SUBSCRIPT_ERROR, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_NULL_VALUE_NOT_ALLOWED, ERRCODE_PROGRAM_LIMIT_EXCEEDED,
};

use crate::construct::{
    construct_empty_array, construct_md_array, construct_md_array_values, deconstruct_array_values,
};
use crate::foundation::{
    arr_data_offset, arr_data_ptr_off, arr_dim, arr_elemtype, arr_lbound, arr_ndim,
    arr_nullbitmap_off, arr_overhead_nonulls, arr_overhead_withnulls, arr_size, array_bitmap_copy,
    array_get_isnull, array_nelems_size, array_seek, array_set_isnull, att_addlength_pointer,
    att_align_nominal, fetch_att, set_header, store_att_byval, write_dims, write_lbounds,
    MAX_ARRAY_SIZE,
};

use arrayutils_seams as arrayutils;

// ---------------------------------------------------------------------------
// Local helpers mirroring the file-static C routines.
// ---------------------------------------------------------------------------

/// `pg_sub_s32_overflow(a, b, *res)`: `a - b`, `true` on overflow.
#[inline]
fn sub_s32_overflow(a: i32, b: i32) -> Option<i32> {
    a.checked_sub(b)
}

/// `pg_add_s32_overflow(a, b, *res)`: `a + b`, `true` on overflow.
#[inline]
fn add_s32_overflow(a: i32, b: i32) -> Option<i32> {
    a.checked_add(b)
}

/// `ereport(ERROR, ERRCODE_PROGRAM_LIMIT_EXCEEDED, "array size exceeds the
/// maximum allowed (%d)", MaxArraySize)`.
fn size_exceeds_error() -> PgError {
    PgError::error(format!(
        "array size exceeds the maximum allowed ({})",
        MAX_ARRAY_SIZE as i32
    ))
    .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
}

/// The bytes of a by-reference element value `Datum`, mirroring the C
/// `DatumGetPointer(src)` + `att_addlength_datum(0, attlen, src)` length
/// computation.  The caller has guaranteed (per `array_set_element`) that a
/// varlena element value has already been detoasted to a plain in-memory
/// varlena, so reading its `VARSIZE_ANY` / cstring length at the pointer is
/// safe.  `attlen > 0` (fixed-width by-ref, e.g. `name`) yields exactly
/// `attlen` bytes; `attlen == -1` a full varlena; `attlen == -2` a NUL-terminated
/// cstring including the terminator.
///
/// # Safety
///
/// `src` must be a valid pointer to a flat (non-toasted) element value of the
/// indicated `attlen`, exactly as the C `DatumGetPointer(src)` contract
/// requires.
fn datum_ptr_bytes<'a>(src: Datum, attlen: i32) -> &'a [u8] {
    let ptr = src.as_usize() as *const u8;
    // SAFETY: mirrors C DatumGetPointer(src). The element value is a flat,
    // already-detoasted in-memory value (the array_set_element caller detoasts
    // varlena values before reaching here), so the length-determining header
    // bytes at `ptr` are valid to read.
    unsafe {
        if attlen > 0 {
            core::slice::from_raw_parts(ptr, attlen as usize)
        } else if attlen == -1 {
            // VARSIZE_ANY(ptr): inspect the varlena header to find the length.
            let len = varsize_any_at_ptr(ptr);
            core::slice::from_raw_parts(ptr, len)
        } else {
            debug_assert_eq!(attlen, -2);
            // strlen(ptr) + 1
            let mut n = 0usize;
            while *ptr.add(n) != 0 {
                n += 1;
            }
            core::slice::from_raw_parts(ptr, n + 1)
        }
    }
}

/// `VARSIZE_ANY(PTR)` read directly off a raw pointer (varatt.h) — used only by
/// [`datum_ptr_bytes`] for a by-reference varlena element value.
///
/// # Safety
/// `ptr` must point to a valid varlena header.
unsafe fn varsize_any_at_ptr(ptr: *const u8) -> usize {
    let b0 = *ptr;
    // A flat (already-detoasted) element value never carries a 1-byte external
    // TOAST pointer (VARATT_IS_1B_E), so only the 1B / 4B inline cases apply.
    if (b0 & 0x01) != 0 {
        // VARATT_IS_1B: single-byte header, length in high 7 bits.
        ((b0 >> 1) & 0x7f) as usize
    } else {
        // 4-byte header (varatt.h, little-endian build): VARSIZE_4B ==
        // (va_header >> 2) & 0x3FFFFFFF, read native-endian like VARSIZE_4B.
        let raw = u32::from_ne_bytes([*ptr, *ptr.add(1), *ptr.add(2), *ptr.add(3)]);
        ((raw >> 2) & 0x3fff_ffff) as usize
    }
}

// ---------------------------------------------------------------------------
// Scalar element get/set (arrayfuncs.c).
// ---------------------------------------------------------------------------

/// `array_get_element(arraydatum, nSubscripts, indx, arraytyplen, elmlen,
/// elmbyval, elmalign, &isNull)` (arrayfuncs.c).
pub fn array_get_element<'mcx>(
    mcx: Mcx<'mcx>,
    array: &[u8],
    nsubscripts: i32,
    indx: &[i32],
    arraytyplen: i32,
    elmlen: i32,
    elmbyval: bool,
    elmalign: u8,
) -> PgResult<(Datum, bool)> {
    let ndim;
    let dim: PgVec<'mcx, i32>;
    let lb: PgVec<'mcx, i32>;
    let arraydataptr: usize;
    let arraynullsptr: Option<usize>;

    if arraytyplen > 0 {
        // fixed-length arrays -- assumed 1-d, 0-based
        ndim = 1;
        dim = {
            let mut v = PgVec::new_in(mcx);
            v.push(arraytyplen / elmlen);
            v
        };
        lb = {
            let mut v = PgVec::new_in(mcx);
            v.push(0);
            v
        };
        arraydataptr = 0; // DatumGetPointer(arraydatum): base of buffer
        arraynullsptr = None;
    } else {
        // (expanded-array dispatch handled at the caller boundary)
        ndim = arr_ndim(array);
        dim = {
            let mut v = PgVec::new_in(mcx);
            v.extend((0..ndim.max(0) as usize).map(|i| arr_dim(array, i)));
            v
        };
        lb = {
            let mut v = PgVec::new_in(mcx);
            v.extend((0..ndim.max(0) as usize).map(|i| arr_lbound(array, i)));
            v
        };
        arraydataptr = arr_data_ptr_off(array);
        arraynullsptr = arr_nullbitmap_off(array);
    }

    // Return NULL for invalid subscript
    if ndim != nsubscripts || ndim <= 0 || ndim > MAXDIM {
        return Ok((Datum::null(), true));
    }
    for i in 0..ndim as usize {
        if indx[i] < lb[i] || indx[i] >= (dim[i] + lb[i]) {
            return Ok((Datum::null(), true));
        }
    }

    // Calculate the element number
    let offset = arrayutils::array_get_offset::call(nsubscripts, &dim, &lb, indx);

    // Check for NULL array element
    if array_get_isnull(array, arraynullsptr, offset) {
        return Ok((Datum::null(), true));
    }

    // OK, get the element
    let (retptr, _) = array_seek(
        array,
        arraydataptr,
        arraynullsptr,
        0,
        elmlen,
        elmbyval,
        elmalign,
        offset,
    );
    // ArrayCast(retptr, elmbyval, elmlen) == fetch_att(retptr, elmbyval, elmlen)
    Ok((fetch_att(array, retptr, elmbyval, elmlen), false))
}

/// `array_set_element(arraydatum, nSubscripts, indx, dataValue, isNull,
/// arraytyplen, elmlen, elmbyval, elmalign)` (arrayfuncs.c).
pub fn array_set_element<'mcx>(
    mcx: Mcx<'mcx>,
    array: &[u8],
    nsubscripts: i32,
    indx: &[i32],
    data_value: Datum,
    is_null: bool,
    arraytyplen: i32,
    elmlen: i32,
    elmbyval: bool,
    elmalign: u8,
) -> PgResult<PgVec<'mcx, u8>> {
    if arraytyplen > 0 {
        // fixed-length arrays -- 1-d, 0-based; cannot be extended.
        if nsubscripts != 1 {
            return Err(PgError::error("wrong number of array subscripts")
                .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR));
        }
        if indx[0] < 0 || indx[0] >= arraytyplen / elmlen {
            return Err(PgError::error("array subscript out of range")
                .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR));
        }
        if is_null {
            return Err(PgError::error(
                "cannot assign null value to an element of a fixed-length array",
            )
            .with_sqlstate(ERRCODE_NULL_VALUE_NOT_ALLOWED));
        }
        let mut resultarray = PgVec::new_in(mcx);
        resultarray.extend_from_slice(&array[..arraytyplen as usize]);
        let elt_off = (indx[0] * elmlen) as usize;
        array_cast_and_set(
            &mut resultarray,
            elt_off,
            data_value,
            elmlen,
            elmbyval,
            elmalign,
        );
        return Ok(resultarray);
    }

    if nsubscripts <= 0 || nsubscripts > MAXDIM {
        return Err(PgError::error("wrong number of array subscripts")
            .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR));
    }

    // make sure item to be inserted is not toasted: handled at caller boundary
    // (the element value byte view is read via datum_ptr_bytes, which assumes a
    // flat varlena, matching the C PG_DETOAST_DATUM done just above this point).

    // (expanded-array dispatch handled at the caller boundary)

    let ndim = arr_ndim(array);

    // if number of dims is zero, i.e. an empty array, create an array with
    // nSubscripts dimensions, and set the lower bounds to the supplied subscripts
    if ndim == 0 {
        let elmtype = arr_elemtype(array);
        let mut dim = PgVec::new_in(mcx);
        let mut lb = PgVec::new_in(mcx);
        for i in 0..nsubscripts as usize {
            dim.push(1);
            lb.push(indx[i]);
        }
        return construct_md_array(
            mcx,
            &[data_value],
            Some(&[is_null]),
            nsubscripts,
            &dim,
            &lb,
            elmtype,
            elmlen,
            elmbyval,
            elmalign,
        );
    }

    if ndim != nsubscripts {
        return Err(PgError::error("wrong number of array subscripts")
            .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR));
    }

    // copy dim/lb since we may modify them
    let mut dim: PgVec<'mcx, i32> = {
        let mut v = PgVec::new_in(mcx);
        v.extend((0..ndim as usize).map(|i| arr_dim(array, i)));
        v
    };
    let mut lb: PgVec<'mcx, i32> = {
        let mut v = PgVec::new_in(mcx);
        v.extend((0..ndim as usize).map(|i| arr_lbound(array, i)));
        v
    };

    let mut newhasnulls = arr_nullbitmap_off(array).is_some() || is_null;
    let mut addedbefore = 0i32;
    let mut addedafter = 0i32;

    // Check subscripts. 1-D may extend; multi-D may not.
    if ndim == 1 {
        if indx[0] < lb[0] {
            // addedbefore = lb[0] - indx[0];  dim[0] += addedbefore;
            let ab = sub_s32_overflow(lb[0], indx[0]).ok_or_else(size_exceeds_error)?;
            dim[0] = add_s32_overflow(dim[0], ab).ok_or_else(size_exceeds_error)?;
            addedbefore = ab;
            lb[0] = indx[0];
            if addedbefore > 1 {
                newhasnulls = true; // will insert nulls
            }
        }
        if indx[0] >= (dim[0] + lb[0]) {
            // addedafter = indx[0] - (dim[0] + lb[0]) + 1; dim[0] += addedafter;
            let mut aa =
                sub_s32_overflow(indx[0], dim[0] + lb[0]).ok_or_else(size_exceeds_error)?;
            aa = add_s32_overflow(aa, 1).ok_or_else(size_exceeds_error)?;
            dim[0] = add_s32_overflow(dim[0], aa).ok_or_else(size_exceeds_error)?;
            addedafter = aa;
            if addedafter > 1 {
                newhasnulls = true; // will insert nulls
            }
        }
    } else {
        // multi-dimensional arrays cannot be extended during assignment
        for i in 0..ndim as usize {
            if indx[i] < lb[i] || indx[i] >= (dim[i] + lb[i]) {
                return Err(PgError::error("array subscript out of range")
                    .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR));
            }
        }
    }

    // This checks for overflow of the array dimensions
    let newnitems = arrayutils::array_get_n_items::call(ndim, &dim)?;
    arrayutils::array_check_bounds::call(ndim, &dim, &lb)?;

    // Compute sizes of items and areas to copy
    let overheadlen = if newhasnulls {
        arr_overhead_withnulls(ndim, newnitems)
    } else {
        arr_overhead_nonulls(ndim)
    };
    let old_dims: PgVec<'mcx, i32> = {
        let mut v = PgVec::new_in(mcx);
        v.extend((0..ndim as usize).map(|i| arr_dim(array, i)));
        v
    };
    let oldnitems = arrayutils::array_get_n_items::call(ndim, &old_dims)?;
    let oldnullbitmap = arr_nullbitmap_off(array);
    let oldoverheadlen = arr_data_offset(array);
    let olddatasize = (arr_size(array) - oldoverheadlen) as i32;

    let offset;
    let lenbefore: i32;
    let olditemlen: i32;
    let lenafter: i32;
    if addedbefore != 0 {
        offset = 0;
        lenbefore = 0;
        olditemlen = 0;
        lenafter = olddatasize;
    } else if addedafter != 0 {
        offset = oldnitems;
        lenbefore = olddatasize;
        olditemlen = 0;
        lenafter = 0;
    } else {
        offset = arrayutils::array_get_offset::call(nsubscripts, &dim, &lb, indx);
        let (elt_off, _) = array_seek(
            array,
            arr_data_ptr_off(array),
            oldnullbitmap,
            0,
            elmlen,
            elmbyval,
            elmalign,
            offset,
        );
        lenbefore = (elt_off - arr_data_ptr_off(array)) as i32;
        olditemlen = if array_get_isnull(array, oldnullbitmap, offset) {
            0
        } else {
            let l = att_addlength_pointer(0, elmlen, array, elt_off);
            att_align_nominal(l, elmalign) as i32
        };
        lenafter = olddatasize - lenbefore - olditemlen;
    }

    // Materialize the new element's bytes (for by-ref) so we can size/store it.
    let newitembytes: Option<&[u8]> = if is_null {
        None
    } else if elmbyval {
        None
    } else {
        Some(datum_ptr_bytes(data_value, elmlen))
    };

    let newitemlen: i32 = if is_null {
        0
    } else if elmlen > 0 {
        // att_addlength_datum(0, elmlen, dataValue) == elmlen for fixed length.
        att_align_nominal(elmlen as usize, elmalign) as i32
    } else {
        let bytes = newitembytes.expect("by-ref element has bytes");
        let l = att_addlength_pointer(0, elmlen, bytes, 0);
        att_align_nominal(l, elmalign) as i32
    };

    let newsize = overheadlen as i32 + lenbefore + newitemlen + lenafter;

    // OK, create the new array and fill in header/dimensions (palloc0).
    let mut newarray: PgVec<'mcx, u8> = PgVec::new_in(mcx);
    newarray
        .try_reserve(newsize as usize)
        .map_err(|_| size_exceeds_error())?;
    newarray.resize(newsize as usize, 0);
    set_header(
        &mut newarray,
        newsize as usize,
        ndim,
        if newhasnulls { overheadlen as i32 } else { 0 },
        arr_elemtype(array),
    );
    write_dims(&mut newarray, &dim[..ndim as usize]);
    write_lbounds(&mut newarray, ndim, &lb[..ndim as usize]);

    // Fill in data
    newarray[overheadlen..overheadlen + lenbefore as usize]
        .copy_from_slice(&array[oldoverheadlen..oldoverheadlen + lenbefore as usize]);
    if !is_null {
        array_cast_and_set(
            &mut newarray,
            overheadlen + lenbefore as usize,
            data_value,
            elmlen,
            elmbyval,
            elmalign,
        );
    }
    let dst = overheadlen + lenbefore as usize + newitemlen as usize;
    let srcstart = oldoverheadlen + lenbefore as usize + olditemlen as usize;
    newarray[dst..dst + lenafter as usize]
        .copy_from_slice(&array[srcstart..srcstart + lenafter as usize]);

    // Fill in nulls bitmap if needed
    if newhasnulls {
        let newbm = arr_nullbitmap_off(&newarray).expect("newhasnulls => bitmap exists");
        // palloc0 above already marked any inserted positions as nulls.
        // Fix the inserted value.
        if addedafter != 0 {
            array_set_isnull(&mut newarray, newbm, newnitems - 1, is_null);
        } else {
            array_set_isnull(&mut newarray, newbm, offset, is_null);
        }
        // Fix the copied range(s). Snapshot the old bitmap to avoid aliasing.
        let old_bm_snapshot: Option<PgVec<'mcx, u8>> = oldnullbitmap.map(|o| {
            let mut v = PgVec::new_in(mcx);
            v.extend_from_slice(&array[o..]);
            v
        });
        if addedbefore != 0 {
            copy_bitmap_snapshot(
                &mut newarray,
                newbm,
                addedbefore,
                old_bm_snapshot.as_deref(),
                0,
                oldnitems,
            );
        } else {
            copy_bitmap_snapshot(
                &mut newarray,
                newbm,
                0,
                old_bm_snapshot.as_deref(),
                0,
                offset,
            );
            if addedafter == 0 {
                copy_bitmap_snapshot(
                    &mut newarray,
                    newbm,
                    offset + 1,
                    old_bm_snapshot.as_deref(),
                    offset + 1,
                    oldnitems - offset - 1,
                );
            }
        }
    }

    Ok(newarray)
}

/// `ArrayCastAndSet(src, typlen, typbyval, typalign, dest)` (arrayfuncs.c):
/// copy `src` into `dest[dest_off..]`, returning total space used (incl. align
/// padding).  Caller must have handled the NULL case.
fn array_cast_and_set(
    dest: &mut [u8],
    dest_off: usize,
    src: Datum,
    typlen: i32,
    typbyval: bool,
    typalign: u8,
) -> usize {
    if typlen > 0 {
        if typbyval {
            store_att_byval(dest, dest_off, src, typlen);
        } else {
            // memmove(dest, DatumGetPointer(src), typlen)
            let bytes = datum_ptr_bytes(src, typlen);
            dest[dest_off..dest_off + typlen as usize].copy_from_slice(&bytes[..typlen as usize]);
        }
        att_align_nominal(typlen as usize, typalign)
    } else {
        debug_assert!(!typbyval);
        // inc = att_addlength_datum(0, typlen, src); memmove(dest, ptr, inc)
        let bytes = datum_ptr_bytes(src, typlen);
        let inc = att_addlength_pointer(0, typlen, bytes, 0);
        dest[dest_off..dest_off + inc].copy_from_slice(&bytes[..inc]);
        att_align_nominal(inc, typalign)
    }
}

/// Bridge to [`array_bitmap_copy`] taking a snapshot of the source bitmap bytes
/// (offset 0 within the snapshot) so we don't alias the dest buffer.
fn copy_bitmap_snapshot(
    dest: &mut [u8],
    dest_bm: usize,
    dest_offset: i32,
    src_snapshot: Option<&[u8]>,
    src_offset: i32,
    nitems: i32,
) {
    match src_snapshot {
        Some(s) => {
            // The snapshot starts at the old bitmap origin (offset 0).
            array_bitmap_copy(dest, dest_bm, dest_offset, s, Some(0), src_offset, nitems);
        }
        None => {
            // No source bitmap: array_bitmap_copy with src_bitmap_off = None
            // treats every source bit as non-null. Pass dest as a dummy src.
            let empty: [u8; 0] = [];
            array_bitmap_copy(dest, dest_bm, dest_offset, &empty, None, src_offset, nitems);
        }
    }
}

/// `array_get_element_expanded(...)` (arrayfuncs.c).
///
/// Expanded arrays (`expandeddatum` / `array_expanded.c`) are a separate,
/// unported subsystem.  There is no expanded-array support reachable from this
/// flat-byte port, so this entry point is unreachable in the ported surface;
/// mirror PG and panic loudly if it is ever called.
pub fn array_get_element_expanded<'mcx>(
    mcx: Mcx<'mcx>,
    array: &[u8],
    nsubscripts: i32,
    indx: &[i32],
    arraytyplen: i32,
    elmlen: i32,
    elmbyval: bool,
    elmalign: u8,
) -> PgResult<(Datum, bool)> {
    let _ = (mcx, array, nsubscripts, indx, arraytyplen, elmlen, elmbyval, elmalign);
    panic!(
        "array_get_element_expanded: expanded-array subsystem (array_expanded.c) is not ported"
    )
}

/// `array_set_element_expanded(...)` (arrayfuncs.c).
///
/// See [`array_get_element_expanded`] — the expanded-array subsystem is not
/// ported; mirror PG and panic loudly.
pub fn array_set_element_expanded<'mcx>(
    mcx: Mcx<'mcx>,
    array: &[u8],
    nsubscripts: i32,
    indx: &[i32],
    data_value: Datum,
    is_null: bool,
    arraytyplen: i32,
    elmlen: i32,
    elmbyval: bool,
    elmalign: u8,
) -> PgResult<PgVec<'mcx, u8>> {
    let _ = (
        mcx,
        array,
        nsubscripts,
        indx,
        data_value,
        is_null,
        arraytyplen,
        elmlen,
        elmbyval,
        elmalign,
    );
    panic!(
        "array_set_element_expanded: expanded-array subsystem (array_expanded.c) is not ported"
    )
}

/// `array_ref(array, nSubscripts, indx, arraytyplen, elmlen, elmbyval,
/// elmalign, &isNull)` (arrayfuncs.c) — thin wrapper over `array_get_element`.
pub fn array_ref<'mcx>(
    mcx: Mcx<'mcx>,
    array: &[u8],
    nsubscripts: i32,
    indx: &[i32],
    arraytyplen: i32,
    elmlen: i32,
    elmbyval: bool,
    elmalign: u8,
) -> PgResult<(Datum, bool)> {
    array_get_element(
        mcx,
        array,
        nsubscripts,
        indx,
        arraytyplen,
        elmlen,
        elmbyval,
        elmalign,
    )
}

/// `array_set(array, nSubscripts, indx, dataValue, isNull, arraytyplen,
/// elmlen, elmbyval, elmalign)` (arrayfuncs.c) — wrapper over
/// `array_set_element`.
pub fn array_set<'mcx>(
    mcx: Mcx<'mcx>,
    array: &[u8],
    nsubscripts: i32,
    indx: &[i32],
    data_value: Datum,
    is_null: bool,
    arraytyplen: i32,
    elmlen: i32,
    elmbyval: bool,
    elmalign: u8,
) -> PgResult<PgVec<'mcx, u8>> {
    array_set_element(
        mcx,
        array,
        nsubscripts,
        indx,
        data_value,
        is_null,
        arraytyplen,
        elmlen,
        elmbyval,
        elmalign,
    )
}

// ---------------------------------------------------------------------------
// Slice get/set (arrayfuncs.c).
// ---------------------------------------------------------------------------

/// `array_get_slice(arraydatum, nSubscripts, upperIndx, lowerIndx,
/// upperProvided, lowerProvided, arraytyplen, elmlen, elmbyval, elmalign)`
/// (arrayfuncs.c).
pub fn array_get_slice<'mcx>(
    mcx: Mcx<'mcx>,
    array: &[u8],
    nsubscripts: i32,
    upper_indx: &[i32],
    lower_indx: &[i32],
    upper_provided: &[bool],
    lower_provided: &[bool],
    arraytyplen: i32,
    elmlen: i32,
    elmbyval: bool,
    elmalign: u8,
) -> PgResult<PgVec<'mcx, u8>> {
    if arraytyplen > 0 {
        // fixed-length arrays -- cannot slice these (parser labels output as the
        // fixed-length array type).
        return Err(
            PgError::error("slices of fixed-length arrays not implemented")
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
        );
    }

    // (input array already detoasted at the caller boundary)
    let ndim = arr_ndim(array);
    let dim: PgVec<'mcx, i32> = {
        let mut v = PgVec::new_in(mcx);
        v.extend((0..ndim.max(0) as usize).map(|i| arr_dim(array, i)));
        v
    };
    let lb: PgVec<'mcx, i32> = {
        let mut v = PgVec::new_in(mcx);
        v.extend((0..ndim.max(0) as usize).map(|i| arr_lbound(array, i)));
        v
    };
    let elemtype = arr_elemtype(array);
    let arraynullsptr = arr_nullbitmap_off(array);

    // Mutable working copies of the subscript bounds (C scribbles on these).
    // C uses fixed MAXDIM-sized `lowerIndx`/`upperIndx` workspaces; the owned
    // caller passes only the provided subscripts, so pad to MAXDIM with zeros
    // (the loops below overwrite every position actually read, `[0..ndim)`).
    let mut lower: PgVec<'mcx, i32> = {
        let mut v = PgVec::new_in(mcx);
        v.resize(MAXDIM as usize, 0);
        let n = lower_indx.len().min(MAXDIM as usize);
        v[..n].copy_from_slice(&lower_indx[..n]);
        v
    };
    let mut upper: PgVec<'mcx, i32> = {
        let mut v = PgVec::new_in(mcx);
        v.resize(MAXDIM as usize, 0);
        let n = upper_indx.len().min(MAXDIM as usize);
        v[..n].copy_from_slice(&upper_indx[..n]);
        v
    };

    // A slice exceeding the current array limits is silently truncated; an empty
    // slice yields an empty array.
    if ndim < nsubscripts || ndim <= 0 || ndim > MAXDIM {
        return construct_empty_array(mcx, elemtype);
    }

    let mut i = 0usize;
    while i < nsubscripts as usize {
        if !lower_provided[i] || lower[i] < lb[i] {
            lower[i] = lb[i];
        }
        if !upper_provided[i] || upper[i] >= (dim[i] + lb[i]) {
            upper[i] = dim[i] + lb[i] - 1;
        }
        if lower[i] > upper[i] {
            return construct_empty_array(mcx, elemtype);
        }
        i += 1;
    }
    // fill any missing subscript positions with full array range
    while i < ndim as usize {
        lower[i] = lb[i];
        upper[i] = dim[i] + lb[i] - 1;
        if lower[i] > upper[i] {
            return construct_empty_array(mcx, elemtype);
        }
        i += 1;
    }

    let mut span: PgVec<'mcx, i32> = {
        let mut v = PgVec::new_in(mcx);
        v.resize(MAXDIM as usize, 0);
        v
    };
    arrayutils::mda_get_range::call(ndim, &mut span, &lower, &upper);

    let mut bytes = array_slice_size(
        mcx, array, arraynullsptr, ndim, &dim, &lb, &lower, &upper, elmlen, elmbyval, elmalign,
    )? as i32;

    // We put a null bitmap in the result if the source has one.
    let dataoffset: i32;
    if arraynullsptr.is_some() {
        let n = arrayutils::array_get_n_items::call(ndim, &span[..ndim as usize])?;
        dataoffset = arr_overhead_withnulls(ndim, n) as i32;
        bytes += dataoffset;
    } else {
        dataoffset = 0; // marker for no null bitmap
        bytes += arr_overhead_nonulls(ndim) as i32;
    }

    let mut newarray: PgVec<'mcx, u8> = PgVec::new_in(mcx);
    newarray
        .try_reserve(bytes as usize)
        .map_err(|_| size_exceeds_error())?;
    newarray.resize(bytes as usize, 0);
    set_header(&mut newarray, bytes as usize, ndim, dataoffset, elemtype);
    write_dims(&mut newarray, &span[..ndim as usize]);

    // Lower bounds of the new array are set to 1.
    let newlb: PgVec<'mcx, i32> = {
        let mut v = PgVec::new_in(mcx);
        v.resize(ndim as usize, 1);
        v
    };
    write_lbounds(&mut newarray, ndim, &newlb);

    array_extract_slice(
        mcx,
        &mut newarray,
        ndim,
        &dim,
        &lb,
        array,
        arraynullsptr,
        &lower,
        &upper,
        elmlen,
        elmbyval,
        elmalign,
    )?;

    Ok(newarray)
}

/// `array_set_slice(arraydatum, nSubscripts, upperIndx, lowerIndx,
/// upperProvided, lowerProvided, srcArrayDatum, isNull, arraytyplen, elmlen,
/// elmbyval, elmalign)` (arrayfuncs.c).
pub fn array_set_slice<'mcx>(
    mcx: Mcx<'mcx>,
    array: &[u8],
    nsubscripts: i32,
    upper_indx: &[i32],
    lower_indx: &[i32],
    upper_provided: &[bool],
    lower_provided: &[bool],
    src_array: &[u8],
    is_null: bool,
    arraytyplen: i32,
    elmlen: i32,
    elmbyval: bool,
    elmalign: u8,
) -> PgResult<PgVec<'mcx, u8>> {
    // Currently, assignment from a NULL source array is a no-op.
    if is_null {
        let mut v = PgVec::new_in(mcx);
        v.extend_from_slice(array);
        return Ok(v);
    }

    if arraytyplen > 0 {
        return Err(PgError::error(
            "updates on slices of fixed-length arrays not implemented",
        )
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED));
    }

    // (arrays already detoasted at caller boundary; srcArray contains no
    // toasted elements)
    let ndim = arr_ndim(array);

    // Mutable working copies of the subscript bounds. C uses fixed MAXDIM-sized
    // workspaces; the owned caller passes only the provided subscripts, so pad
    // to MAXDIM with zeros (positions actually read are overwritten below).
    let mut lower: PgVec<'mcx, i32> = {
        let mut v = PgVec::new_in(mcx);
        v.resize(MAXDIM as usize, 0);
        let n = lower_indx.len().min(MAXDIM as usize);
        v[..n].copy_from_slice(&lower_indx[..n]);
        v
    };
    let mut upper: PgVec<'mcx, i32> = {
        let mut v = PgVec::new_in(mcx);
        v.resize(MAXDIM as usize, 0);
        let n = upper_indx.len().min(MAXDIM as usize);
        v[..n].copy_from_slice(&upper_indx[..n]);
        v
    };

    // empty array => create an array with nSubscripts dims, bounds from indices.
    if ndim == 0 {
        let elmtype = arr_elemtype(array);
        // Decode the source elements into the dereferenceable value lane
        // (`types_tuple::Datum`, carrying verbatim by-ref bytes). The bare-word
        // `deconstruct_array` Datum stores only an in-buffer offset for a
        // pass-by-ref element, which `construct_md_array` would then deref as a
        // pointer (SIGSEGV). Mirror C's `deconstruct_array` + `construct_md_array`
        // pair via the owned value-lane counterparts.
        let elems = deconstruct_array_values(mcx, src_array, elmtype, elmlen, elmbyval, elmalign)?;
        let nelems = elems.len() as i32;

        let mut dim = PgVec::new_in(mcx);
        let mut lb = PgVec::new_in(mcx);
        for i in 0..nsubscripts as usize {
            if !upper_provided[i] || !lower_provided[i] {
                return Err(PgError::error(
                    "array slice subscript must provide both boundaries",
                )
                .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR)
                .with_detail(
                    "When assigning to a slice of an empty array value, slice boundaries must be fully specified.",
                ));
            }
            // dim[i] = upperIndx[i] - lowerIndx[i] + 1, detecting overflow
            let mut d =
                sub_s32_overflow(upper[i], lower[i]).ok_or_else(size_exceeds_error)?;
            d = add_s32_overflow(d, 1).ok_or_else(size_exceeds_error)?;
            dim.push(d);
            lb.push(lower[i]);
        }

        // complain if too few source items; ignore extras
        let need = arrayutils::array_get_n_items::call(nsubscripts, &dim)?;
        if nelems < need {
            return Err(PgError::error("source array too small")
                .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR));
        }

        let mut dvalues: PgVec<'mcx, types_tuple::Datum<'mcx>> = PgVec::new_in(mcx);
        let mut dnulls: PgVec<'mcx, bool> = PgVec::new_in(mcx);
        for (val, isnull) in elems.into_iter() {
            dvalues.push(val);
            dnulls.push(isnull);
        }
        return construct_md_array_values(
            mcx, &dvalues, Some(&dnulls), nsubscripts, &dim, &lb, elmtype, elmlen, elmbyval,
            elmalign,
        );
    }

    if ndim < nsubscripts || ndim <= 0 || ndim > MAXDIM {
        return Err(PgError::error("wrong number of array subscripts")
            .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR));
    }

    // copy dim/lb since we may modify them
    let mut dim: PgVec<'mcx, i32> = {
        let mut v = PgVec::new_in(mcx);
        v.extend((0..ndim as usize).map(|i| arr_dim(array, i)));
        v
    };
    let mut lb: PgVec<'mcx, i32> = {
        let mut v = PgVec::new_in(mcx);
        v.extend((0..ndim as usize).map(|i| arr_lbound(array, i)));
        v
    };

    let mut newhasnulls =
        arr_nullbitmap_off(array).is_some() || arr_nullbitmap_off(src_array).is_some();
    let mut addedbefore = 0i32;
    // C also tracks `addedafter` here, but it is never read after the subscript
    // checks (only `addedbefore` feeds the bitmap copies below), so we elide it.

    // Check subscripts.
    if ndim == 1 {
        debug_assert_eq!(nsubscripts, 1);
        if !lower_provided[0] {
            lower[0] = lb[0];
        }
        if !upper_provided[0] {
            upper[0] = dim[0] + lb[0] - 1;
        }
        if lower[0] > upper[0] {
            return Err(PgError::error("upper bound cannot be less than lower bound")
                .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR));
        }
        if lower[0] < lb[0] {
            // addedbefore = lb[0] - lowerIndx[0]; dim[0] += addedbefore;
            let ab = sub_s32_overflow(lb[0], lower[0]).ok_or_else(size_exceeds_error)?;
            dim[0] = add_s32_overflow(dim[0], ab).ok_or_else(size_exceeds_error)?;
            addedbefore = ab;
            lb[0] = lower[0];
            if addedbefore > 1 {
                newhasnulls = true;
            }
        }
        if upper[0] >= (dim[0] + lb[0]) {
            // addedafter = upperIndx[0] - (dim[0] + lb[0]) + 1; dim[0] += ...
            let mut aa =
                sub_s32_overflow(upper[0], dim[0] + lb[0]).ok_or_else(size_exceeds_error)?;
            aa = add_s32_overflow(aa, 1).ok_or_else(size_exceeds_error)?;
            dim[0] = add_s32_overflow(dim[0], aa).ok_or_else(size_exceeds_error)?;
            if aa > 1 {
                newhasnulls = true;
            }
        }
    } else {
        // multi-dimensional arrays cannot be extended during assignment
        let mut i = 0usize;
        while i < nsubscripts as usize {
            if !lower_provided[i] {
                lower[i] = lb[i];
            }
            if !upper_provided[i] {
                upper[i] = dim[i] + lb[i] - 1;
            }
            if lower[i] > upper[i] {
                return Err(PgError::error("upper bound cannot be less than lower bound")
                    .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR));
            }
            if lower[i] < lb[i] || upper[i] >= (dim[i] + lb[i]) {
                return Err(PgError::error("array subscript out of range")
                    .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR));
            }
            i += 1;
        }
        // fill any missing subscript positions with full array range
        while i < ndim as usize {
            lower[i] = lb[i];
            upper[i] = dim[i] + lb[i] - 1;
            if lower[i] > upper[i] {
                return Err(PgError::error("upper bound cannot be less than lower bound")
                    .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR));
            }
            i += 1;
        }
    }

    // Do this mainly to check for overflow
    let _nitems = arrayutils::array_get_n_items::call(ndim, &dim)?;
    arrayutils::array_check_bounds::call(ndim, &dim, &lb)?;

    // Make sure source array has enough entries (shape is ignored).
    let mut span: PgVec<'mcx, i32> = {
        let mut v = PgVec::new_in(mcx);
        v.resize(MAXDIM as usize, 0);
        v
    };
    arrayutils::mda_get_range::call(ndim, &mut span, &lower, &upper);
    let nsrcitems = arrayutils::array_get_n_items::call(ndim, &span[..ndim as usize])?;
    let src_ndim = arr_ndim(src_array);
    let src_dims: PgVec<'mcx, i32> = {
        let mut v = PgVec::new_in(mcx);
        v.extend((0..src_ndim.max(0) as usize).map(|i| arr_dim(src_array, i)));
        v
    };
    let src_nitems = arrayutils::array_get_n_items::call(src_ndim, &src_dims)?;
    if nsrcitems > src_nitems {
        return Err(PgError::error("source array too small")
            .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR));
    }

    // Compute space for new entries, replaced entries, and required new size.
    let overheadlen = if newhasnulls {
        arr_overhead_withnulls(ndim, _nitems)
    } else {
        arr_overhead_nonulls(ndim)
    };
    let src_nullsptr = arr_nullbitmap_off(src_array);
    let newitemsize = array_nelems_size(
        src_array,
        arr_data_ptr_off(src_array),
        0,
        src_nullsptr,
        nsrcitems,
        elmlen,
        elmbyval,
        elmalign,
    ) as i32;
    let oldoverheadlen = arr_data_offset(array);
    let olddatasize = (arr_size(array) - oldoverheadlen) as i32;

    let olditemsize: i32;
    let lenbefore: i32;
    let lenafter: i32;
    let itemsbefore: i32;
    let itemsafter: i32;
    let nolditems: i32;
    let oldnullbitmap = arr_nullbitmap_off(array);

    if ndim > 1 {
        // No extension possible for ndim>1.
        olditemsize = array_slice_size(
            mcx, array, oldnullbitmap, ndim, &dim, &lb, &lower, &upper, elmlen, elmbyval, elmalign,
        )? as i32;
        lenbefore = 0;
        lenafter = 0;
        itemsbefore = 0;
        itemsafter = 0;
        nolditems = 0;
    } else {
        // Allow slice larger than orig and/or not adjacent to orig subscripts.
        let oldlb = arr_lbound(array, 0);
        let oldub = oldlb + arr_dim(array, 0) - 1;
        let slicelb = oldlb.max(lower[0]);
        let sliceub = oldub.min(upper[0]);
        let oldarraydata = arr_data_ptr_off(array);
        let oldarraybitmap = oldnullbitmap;

        // old entries before the slice
        itemsbefore = slicelb.min(oldub + 1) - oldlb;
        lenbefore = array_nelems_size(
            array,
            oldarraydata,
            0,
            oldarraybitmap,
            itemsbefore,
            elmlen,
            elmbyval,
            elmalign,
        ) as i32;
        // old entries replaced by the slice
        if slicelb > sliceub {
            nolditems = 0;
            olditemsize = 0;
        } else {
            nolditems = sliceub - slicelb + 1;
            olditemsize = array_nelems_size(
                array,
                oldarraydata + lenbefore as usize,
                itemsbefore,
                oldarraybitmap,
                nolditems,
                elmlen,
                elmbyval,
                elmalign,
            ) as i32;
        }
        // old entries after the slice
        itemsafter = oldub + 1 - (sliceub + 1).max(oldlb);
        lenafter = olddatasize - lenbefore - olditemsize;
    }

    let newsize = overheadlen as i32 + olddatasize - olditemsize + newitemsize;

    let mut newarray: PgVec<'mcx, u8> = PgVec::new_in(mcx);
    newarray
        .try_reserve(newsize as usize)
        .map_err(|_| size_exceeds_error())?;
    newarray.resize(newsize as usize, 0);
    set_header(
        &mut newarray,
        newsize as usize,
        ndim,
        if newhasnulls { overheadlen as i32 } else { 0 },
        arr_elemtype(array),
    );
    write_dims(&mut newarray, &dim[..ndim as usize]);
    write_lbounds(&mut newarray, ndim, &lb[..ndim as usize]);

    if ndim > 1 {
        array_insert_slice(
            mcx,
            &mut newarray,
            array,
            src_array,
            ndim,
            &dim,
            &lb,
            &lower,
            &upper,
            elmlen,
            elmbyval,
            elmalign,
        )?;
    } else {
        // fill in data
        newarray[overheadlen..overheadlen + lenbefore as usize]
            .copy_from_slice(&array[oldoverheadlen..oldoverheadlen + lenbefore as usize]);
        let src_data = arr_data_ptr_off(src_array);
        newarray[overheadlen + lenbefore as usize
            ..overheadlen + lenbefore as usize + newitemsize as usize]
            .copy_from_slice(&src_array[src_data..src_data + newitemsize as usize]);
        let dst = overheadlen + lenbefore as usize + newitemsize as usize;
        let srcstart = oldoverheadlen + lenbefore as usize + olditemsize as usize;
        newarray[dst..dst + lenafter as usize]
            .copy_from_slice(&array[srcstart..srcstart + lenafter as usize]);

        // fill in nulls bitmap if needed
        if newhasnulls {
            let newbm = arr_nullbitmap_off(&newarray).expect("newhasnulls => bitmap");
            // Snapshot the source bitmaps to avoid aliasing the dest buffer.
            let old_bm_snapshot: Option<PgVec<'mcx, u8>> = oldnullbitmap.map(|o| {
                let mut v = PgVec::new_in(mcx);
                v.extend_from_slice(&array[o..]);
                v
            });
            let src_bm_snapshot: Option<PgVec<'mcx, u8>> = src_nullsptr.map(|o| {
                let mut v = PgVec::new_in(mcx);
                v.extend_from_slice(&src_array[o..]);
                v
            });
            // palloc0 already marked inserted positions as nulls.
            copy_bitmap_snapshot(
                &mut newarray,
                newbm,
                addedbefore,
                old_bm_snapshot.as_deref(),
                0,
                itemsbefore,
            );
            copy_bitmap_snapshot(
                &mut newarray,
                newbm,
                lower[0] - lb[0],
                src_bm_snapshot.as_deref(),
                0,
                nsrcitems,
            );
            copy_bitmap_snapshot(
                &mut newarray,
                newbm,
                addedbefore + itemsbefore + nolditems,
                old_bm_snapshot.as_deref(),
                itemsbefore + nolditems,
                itemsafter,
            );
        }
    }

    Ok(newarray)
}

// ---------------------------------------------------------------------------
// Slice byte-walk helpers (file-static in arrayfuncs.c).
// ---------------------------------------------------------------------------

/// `array_slice_size(arraydataptr, arraynullsptr, ndim, dim, lb, st, endp,
/// typlen, typbyval, typalign)` (arrayfuncs.c): byte size of a slice.
#[allow(clippy::too_many_arguments)]
fn array_slice_size<'mcx>(
    mcx: Mcx<'mcx>,
    buf: &[u8],
    arraynullsptr: Option<usize>,
    ndim: i32,
    dim: &[i32],
    lb: &[i32],
    st: &[i32],
    endp: &[i32],
    typlen: i32,
    typbyval: bool,
    typalign: u8,
) -> PgResult<usize> {
    let mut span: PgVec<'mcx, i32> = {
        let mut v = PgVec::new_in(mcx);
        v.resize(MAXDIM as usize, 0);
        v
    };
    arrayutils::mda_get_range::call(ndim, &mut span, st, endp);

    // Pretty easy for fixed element length without nulls ...
    if typlen > 0 && arraynullsptr.is_none() {
        let n = arrayutils::array_get_n_items::call(ndim, &span[..ndim as usize])?;
        return Ok(n as usize * att_align_nominal(typlen as usize, typalign));
    }

    // Else gotta do it the hard way
    let mut src_offset = arrayutils::array_get_offset::call(ndim, dim, lb, st);
    let mut ptr = {
        let (p, _) = array_seek(
            buf,
            arr_data_ptr_off(buf),
            arraynullsptr,
            0,
            typlen,
            typbyval,
            typalign,
            src_offset,
        );
        p
    };
    let mut prod: PgVec<'mcx, i32> = {
        let mut v = PgVec::new_in(mcx);
        v.resize(MAXDIM as usize, 0);
        v
    };
    arrayutils::mda_get_prod::call(ndim, dim, &mut prod);
    let mut dist: PgVec<'mcx, i32> = {
        let mut v = PgVec::new_in(mcx);
        v.resize(MAXDIM as usize, 0);
        v
    };
    arrayutils::mda_get_offset_values::call(ndim, &mut dist, &prod, &span);
    let mut indx: PgVec<'mcx, i32> = {
        let mut v = PgVec::new_in(mcx);
        v.resize(ndim as usize, 0);
        v
    };
    let mut count = 0usize;
    let mut j = ndim - 1;
    loop {
        if dist[j as usize] != 0 {
            let (p, _) = array_seek(
                buf,
                ptr,
                arraynullsptr,
                src_offset,
                typlen,
                typbyval,
                typalign,
                dist[j as usize],
            );
            ptr = p;
            src_offset += dist[j as usize];
        }
        if !array_get_isnull(buf, arraynullsptr, src_offset) {
            let mut inc = att_addlength_pointer(0, typlen, buf, ptr);
            inc = att_align_nominal(inc, typalign);
            ptr += inc;
            count += inc;
        }
        src_offset += 1;
        j = arrayutils::mda_next_tuple::call(ndim, &mut indx, &span);
        if j == -1 {
            break;
        }
    }
    Ok(count)
}

/// `array_extract_slice(newarray, ndim, dim, lb, arraydataptr, arraynullsptr,
/// st, endp, typlen, typbyval, typalign)` (arrayfuncs.c).
#[allow(clippy::too_many_arguments)]
fn array_extract_slice<'mcx>(
    mcx: Mcx<'mcx>,
    newarray: &mut PgVec<'mcx, u8>,
    ndim: i32,
    dim: &[i32],
    lb: &[i32],
    src: &[u8],
    arraynullsptr: Option<usize>,
    st: &[i32],
    endp: &[i32],
    typlen: i32,
    typbyval: bool,
    typalign: u8,
) -> PgResult<()> {
    let destdataptr = arr_data_ptr_off(newarray);
    let destnullsptr = arr_nullbitmap_off(newarray);

    let mut src_offset = arrayutils::array_get_offset::call(ndim, dim, lb, st);
    let mut srcdataptr = {
        let (p, _) = array_seek(
            src,
            arr_data_ptr_off(src),
            arraynullsptr,
            0,
            typlen,
            typbyval,
            typalign,
            src_offset,
        );
        p
    };
    let mut prod: PgVec<'mcx, i32> = {
        let mut v = PgVec::new_in(mcx);
        v.resize(MAXDIM as usize, 0);
        v
    };
    arrayutils::mda_get_prod::call(ndim, dim, &mut prod);
    let mut span: PgVec<'mcx, i32> = {
        let mut v = PgVec::new_in(mcx);
        v.resize(MAXDIM as usize, 0);
        v
    };
    arrayutils::mda_get_range::call(ndim, &mut span, st, endp);
    let mut dist: PgVec<'mcx, i32> = {
        let mut v = PgVec::new_in(mcx);
        v.resize(MAXDIM as usize, 0);
        v
    };
    arrayutils::mda_get_offset_values::call(ndim, &mut dist, &prod, &span);
    let mut indx: PgVec<'mcx, i32> = {
        let mut v = PgVec::new_in(mcx);
        v.resize(ndim as usize, 0);
        v
    };
    let mut dest_offset = 0i32;
    let mut destptr = destdataptr;

    // Snapshot the source bitmap to avoid aliasing the dest buffer in bitmap_copy.
    let src_bm_snapshot: Option<PgVec<'mcx, u8>> = arraynullsptr.map(|o| {
        let mut v = PgVec::new_in(mcx);
        v.extend_from_slice(&src[o..]);
        v
    });

    let mut j = ndim - 1;
    loop {
        if dist[j as usize] != 0 {
            // skip unwanted elements
            let (p, _) = array_seek(
                src,
                srcdataptr,
                arraynullsptr,
                src_offset,
                typlen,
                typbyval,
                typalign,
                dist[j as usize],
            );
            srcdataptr = p;
            src_offset += dist[j as usize];
        }
        // array_copy(destptr, 1, srcptr, src_offset, arraynullsptr, ...)
        // We must read from `src` and write into `newarray`; snapshot the source
        // bytes window so we don't alias.
        let inc = array_copy_cross(
            newarray, destptr, 1, src, srcdataptr, src_offset, arraynullsptr, typlen, typbyval,
            typalign,
        );
        if let Some(dnp) = destnullsptr {
            match src_bm_snapshot.as_deref() {
                Some(s) => array_bitmap_copy(newarray, dnp, dest_offset, s, Some(0), src_offset, 1),
                None => {
                    let empty: [u8; 0] = [];
                    array_bitmap_copy(newarray, dnp, dest_offset, &empty, None, src_offset, 1);
                }
            }
        }
        destptr += inc;
        srcdataptr += inc;
        src_offset += 1;
        dest_offset += 1;
        j = arrayutils::mda_next_tuple::call(ndim, &mut indx, &span);
        if j == -1 {
            break;
        }
    }
    Ok(())
}

/// `array_insert_slice(destArray, origArray, srcArray, ndim, dim, lb, st, endp,
/// typlen, typbyval, typalign)` (arrayfuncs.c).
#[allow(clippy::too_many_arguments)]
fn array_insert_slice<'mcx>(
    mcx: Mcx<'mcx>,
    dest: &mut PgVec<'mcx, u8>,
    orig: &[u8],
    src: &[u8],
    ndim: i32,
    dim: &[i32],
    lb: &[i32],
    st: &[i32],
    endp: &[i32],
    typlen: i32,
    typbyval: bool,
    typalign: u8,
) -> PgResult<()> {
    let mut destptr = arr_data_ptr_off(dest);
    let mut origptr = arr_data_ptr_off(orig);
    let mut srcptr = arr_data_ptr_off(src);
    let destbitmap = arr_nullbitmap_off(dest);
    let origbitmap = arr_nullbitmap_off(orig);
    let srcbitmap = arr_nullbitmap_off(src);
    let orig_ndim = arr_ndim(orig);
    let orig_dims: PgVec<'mcx, i32> = {
        let mut v = PgVec::new_in(mcx);
        v.extend((0..orig_ndim.max(0) as usize).map(|i| arr_dim(orig, i)));
        v
    };
    let orignitems = arrayutils::array_get_n_items::call(orig_ndim, &orig_dims)?;

    // Snapshots of source bitmaps for non-aliasing bitmap copies.
    let orig_bm_snapshot: Option<PgVec<'mcx, u8>> = origbitmap.map(|o| {
        let mut v = PgVec::new_in(mcx);
        v.extend_from_slice(&orig[o..]);
        v
    });
    let src_bm_snapshot: Option<PgVec<'mcx, u8>> = srcbitmap.map(|o| {
        let mut v = PgVec::new_in(mcx);
        v.extend_from_slice(&src[o..]);
        v
    });

    let mut dest_offset = arrayutils::array_get_offset::call(ndim, dim, lb, st);
    // copy items before the slice start
    let mut inc = array_copy_cross(
        dest, destptr, dest_offset, orig, origptr, 0, origbitmap, typlen, typbyval, typalign,
    );
    destptr += inc;
    origptr += inc;
    if let Some(dbm) = destbitmap {
        bitmap_copy_opt(dest, dbm, 0, orig_bm_snapshot.as_deref(), 0, dest_offset);
    }
    let mut orig_offset = dest_offset;
    let mut prod: PgVec<'mcx, i32> = {
        let mut v = PgVec::new_in(mcx);
        v.resize(MAXDIM as usize, 0);
        v
    };
    arrayutils::mda_get_prod::call(ndim, dim, &mut prod);
    let mut span: PgVec<'mcx, i32> = {
        let mut v = PgVec::new_in(mcx);
        v.resize(MAXDIM as usize, 0);
        v
    };
    arrayutils::mda_get_range::call(ndim, &mut span, st, endp);
    let mut dist: PgVec<'mcx, i32> = {
        let mut v = PgVec::new_in(mcx);
        v.resize(MAXDIM as usize, 0);
        v
    };
    arrayutils::mda_get_offset_values::call(ndim, &mut dist, &prod, &span);
    let mut indx: PgVec<'mcx, i32> = {
        let mut v = PgVec::new_in(mcx);
        v.resize(ndim as usize, 0);
        v
    };
    let mut src_offset = 0i32;
    let mut j = ndim - 1;
    loop {
        // Copy/advance over elements between here and next part of slice
        if dist[j as usize] != 0 {
            inc = array_copy_cross(
                dest,
                destptr,
                dist[j as usize],
                orig,
                origptr,
                orig_offset,
                origbitmap,
                typlen,
                typbyval,
                typalign,
            );
            destptr += inc;
            origptr += inc;
            if let Some(dbm) = destbitmap {
                bitmap_copy_opt(
                    dest,
                    dbm,
                    dest_offset,
                    orig_bm_snapshot.as_deref(),
                    orig_offset,
                    dist[j as usize],
                );
            }
            dest_offset += dist[j as usize];
            orig_offset += dist[j as usize];
        }
        // Copy new element at this slice position
        inc = array_copy_cross(
            dest, destptr, 1, src, srcptr, src_offset, srcbitmap, typlen, typbyval, typalign,
        );
        if let Some(dbm) = destbitmap {
            bitmap_copy_opt(dest, dbm, dest_offset, src_bm_snapshot.as_deref(), src_offset, 1);
        }
        destptr += inc;
        srcptr += inc;
        dest_offset += 1;
        src_offset += 1;
        // Advance over old element at this slice position
        let (p, _) = array_seek(
            orig, origptr, origbitmap, orig_offset, typlen, typbyval, typalign, 1,
        );
        origptr = p;
        orig_offset += 1;
        j = arrayutils::mda_next_tuple::call(ndim, &mut indx, &span);
        if j == -1 {
            break;
        }
    }

    // don't miss any data at the end
    array_copy_cross(
        dest,
        destptr,
        orignitems - orig_offset,
        orig,
        origptr,
        orig_offset,
        origbitmap,
        typlen,
        typbyval,
        typalign,
    );
    if let Some(dbm) = destbitmap {
        bitmap_copy_opt(
            dest,
            dbm,
            dest_offset,
            orig_bm_snapshot.as_deref(),
            orig_offset,
            orignitems - orig_offset,
        );
    }
    Ok(())
}

/// `array_copy(destptr, nitems, srcptr, offset, nullbitmap, ...)` across two
/// distinct buffers (`src` -> `dest`).  Snapshots the relevant source window so
/// the foundation [`array_copy`] (which works within one buffer) can be reused
/// without aliasing.  Returns the byte count copied.
#[allow(clippy::too_many_arguments)]
fn array_copy_cross<'mcx>(
    dest: &mut PgVec<'mcx, u8>,
    dest_off: usize,
    nitems: i32,
    src: &[u8],
    src_off: usize,
    src_elem_offset: i32,
    nullbitmap: Option<usize>,
    typlen: i32,
    typbyval: bool,
    typalign: u8,
) -> usize {
    // Determine the byte span of `nitems` source elements starting at src_off
    // (element index `src_elem_offset`, used to position the null bitmap).
    let bytes = array_nelems_size(
        src,
        src_off,
        src_elem_offset,
        nullbitmap,
        nitems,
        typlen,
        typbyval,
        typalign,
    );
    dest[dest_off..dest_off + bytes].copy_from_slice(&src[src_off..src_off + bytes]);
    bytes
}

/// `array_bitmap_copy` over an optional snapshot of the source bitmap.
fn bitmap_copy_opt(
    dest: &mut [u8],
    dest_bm: usize,
    dest_offset: i32,
    src_snapshot: Option<&[u8]>,
    src_offset: i32,
    nitems: i32,
) {
    match src_snapshot {
        Some(s) => array_bitmap_copy(dest, dest_bm, dest_offset, s, Some(0), src_offset, nitems),
        None => {
            let empty: [u8; 0] = [];
            array_bitmap_copy(dest, dest_bm, dest_offset, &empty, None, src_offset, nitems);
        }
    }
}

// ---------------------------------------------------------------------------
// Dimension introspection (arrayfuncs.c) — SQL functions.
// ---------------------------------------------------------------------------

/// `array_ndims(arr)` (arrayfuncs.c): the dimension count, or `None` for a
/// zero-dimensional array (C: SQL NULL).
pub fn array_ndims(array: &[u8]) -> Option<i32> {
    let ndim = arr_ndim(array);
    // Sanity check: does it look like an array at all?
    if ndim <= 0 || ndim > MAXDIM {
        return None;
    }
    Some(ndim)
}

/// `array_dims(arr)` (arrayfuncs.c): the `[lb:ub]...` text form, or `None`.
pub fn array_dims<'mcx>(mcx: Mcx<'mcx>, array: &[u8]) -> PgResult<Option<PgVec<'mcx, u8>>> {
    let ndim = arr_ndim(array);
    // Sanity check: does it look like an array at all?
    if ndim <= 0 || ndim > MAXDIM {
        return Ok(None);
    }

    let mut s = String::new();
    for i in 0..ndim as usize {
        let lbi = arr_lbound(array, i);
        let dimi = arr_dim(array, i);
        // sprintf(p, "[%d:%d]", lb[i], dimv[i] + lb[i] - 1)
        s.push_str(&format!("[{}:{}]", lbi, dimi + lbi - 1));
    }

    // cstring_to_text(buf): a text varlena. The byte form here is the raw UTF-8
    // payload; wrapping into a `text` varlena is the I/O family's concern.
    let mut v = PgVec::new_in(mcx);
    v.extend_from_slice(s.as_bytes());
    Ok(Some(v))
}

/// `array_lower(arr, reqdim)` (arrayfuncs.c): the lower bound of dimension
/// `reqdim`, or `None`.
pub fn array_lower(array: &[u8], reqdim: i32) -> Option<i32> {
    let ndim = arr_ndim(array);
    // Sanity check: does it look like an array at all?
    if ndim <= 0 || ndim > MAXDIM {
        return None;
    }
    // Sanity check: was the requested dim valid?
    if reqdim <= 0 || reqdim > ndim {
        return None;
    }
    Some(arr_lbound(array, (reqdim - 1) as usize))
}

/// `array_upper(arr, reqdim)` (arrayfuncs.c): the upper bound of dimension
/// `reqdim`, or `None`.
pub fn array_upper(array: &[u8], reqdim: i32) -> Option<i32> {
    let ndim = arr_ndim(array);
    if ndim <= 0 || ndim > MAXDIM {
        return None;
    }
    if reqdim <= 0 || reqdim > ndim {
        return None;
    }
    let i = (reqdim - 1) as usize;
    Some(arr_dim(array, i) + arr_lbound(array, i) - 1)
}

/// `array_length(arr, reqdim)` (arrayfuncs.c): the length of dimension
/// `reqdim`, or `None`.
pub fn array_length(array: &[u8], reqdim: i32) -> Option<i32> {
    let ndim = arr_ndim(array);
    if ndim <= 0 || ndim > MAXDIM {
        return None;
    }
    if reqdim <= 0 || reqdim > ndim {
        return None;
    }
    Some(arr_dim(array, (reqdim - 1) as usize))
}

/// `array_cardinality(arr)` (arrayfuncs.c): total element count.
///
/// `ArrayGetNItems(AARR_NDIM(v), AARR_DIMS(v))` is infallible here because the
/// array bytes were produced by a validated array constructor (a stored array
/// has already passed the overflow check); on the impossible overflow we mirror
/// PG and surface the program-limit error by panicking.
pub fn array_cardinality(array: &[u8]) -> i32 {
    let ndim = arr_ndim(array);
    // A plain heap `Vec<i32>` for the dims read (no `Mcx` is threaded into this
    // SQL function's signature).
    let dims: std::vec::Vec<i32> = (0..ndim.max(0) as usize).map(|i| arr_dim(array, i)).collect();
    arrayutils::array_get_n_items::call(ndim, &dims)
        .expect("array_cardinality: ArrayGetNItems overflow on a stored array")
}
