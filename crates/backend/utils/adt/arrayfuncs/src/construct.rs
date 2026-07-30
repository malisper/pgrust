use alloc::boxed::Box;

use ::datum::{varlena::set_varsize_4b, Datum};
use ::mcx::{vec_with_capacity_in, Mcx, PgVec};
use ::types_core::{
    Oid, CHAROID, CSTRINGOID, FLOAT4OID, FLOAT8OID, INT2OID, INT4OID, INT8OID, NAMEOID, OIDOID,
    REGTYPEOID, TEXTOID, TIDOID, XIDOID,
};
use ::types_error::{PgError, PgResult, ERRCODE_NULL_VALUE_NOT_ALLOWED, ERRCODE_PROGRAM_LIMIT_EXCEEDED};

use crate::foundation::*;
use ::arrayutils::{array_check_bounds, array_get_n_items};

const NAMEDATALEN: i32 = 64;
const SIZEOF_ITEMPOINTER: i32 = 6;

// (elmlen, elmbyval, elmalign) for the built-in element types.
pub fn builtin_meta(elmtype: Oid) -> (i32, bool, u8) {
    match elmtype {
        CHAROID => (1, true, TYPALIGN_CHAR),
        CSTRINGOID => (-2, false, TYPALIGN_CHAR),
        FLOAT4OID => (4, true, TYPALIGN_INT),
        FLOAT8OID => (8, true, TYPALIGN_DOUBLE),
        INT2OID => (2, true, TYPALIGN_SHORT),
        INT4OID => (4, true, TYPALIGN_INT),
        INT8OID => (8, true, TYPALIGN_DOUBLE),
        NAMEOID => (NAMEDATALEN, false, TYPALIGN_CHAR),
        OIDOID | REGTYPEOID => (4, true, TYPALIGN_INT),
        TEXTOID => (-1, false, TYPALIGN_INT),
        TIDOID => (SIZEOF_ITEMPOINTER, false, TYPALIGN_SHORT),
        XIDOID => (4, true, TYPALIGN_INT),
        other => panic!("type {other} not supported by construct/deconstruct_array_builtin()"),
    }
}

#[track_caller]
#[cold]
#[inline(never)]
fn array_alloc_exceeded() -> Box<PgError> {
    Box::new(
        PgError::error(alloc::format!(
            "array size exceeds the maximum allowed ({})",
            MAX_ALLOC_SIZE
        ))
        .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
    )
}

// deconstruct_array: extract (Datum, isnull) per element. By-ref Datums point
// into `array`, which must outlive them. `allow_nulls` mirrors C's nullsp==NULL.
pub fn deconstruct_array<'mcx>(
    mcx: Mcx<'mcx>,
    array: &[u8],
    elmlen: i32,
    elmbyval: bool,
    elmalign: u8,
    allow_nulls: bool,
) -> PgResult<(PgVec<'mcx, Datum>, PgVec<'mcx, bool>)> {
    let (ndim, dims, _lbs) = read_dims_lbounds(array);
    let nelems = array_get_n_items(ndim, &dims)? as usize;
    let mut elems: PgVec<Datum> = vec_with_capacity_in(mcx, nelems)?;
    let mut nulls: PgVec<bool> = vec_with_capacity_in(mcx, nelems)?;

    let base = array.as_ptr();
    let mut off = arr_data_offset(array);
    let bitmap_off = arr_nullbitmap_off(array);
    let mut bitmask: u32 = 1;
    let mut bitmap_byte = 0usize;
    let ep = elems.as_mut_ptr();
    let np = nulls.as_mut_ptr();

    for i in 0..nelems {
        let is_null = match bitmap_off {
            Some(bo) => (array[bo + bitmap_byte] as u32 & bitmask) == 0,
            None => false,
        };
        if is_null {
            if !allow_nulls {
                return Err(Box::new(
                    PgError::error("null array element not allowed in this context")
                        .with_sqlstate(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                ));
            }
            // SAFETY: i < nelems = reserved capacity of both vecs; the set_len
            // below only covers slots this loop has written.
            unsafe {
                *ep.add(i) = Datum::null();
                *np.add(i) = true;
            }
        } else {
            // SAFETY: off is within the image; fetch_att reads elmlen bytes;
            // writes as above.
            unsafe {
                let p = base.add(off);
                *ep.add(i) = fetch_att(p, elmbyval, elmlen);
                *np.add(i) = false;
                off = att_addlength_pointer(off, elmlen, p);
            }
            off = att_align_nominal(off, elmalign);
        }
        if bitmap_off.is_some() {
            bitmask <<= 1;
            if bitmask == 0x100 {
                bitmap_byte += 1;
                bitmask = 1;
            }
        }
    }
    // SAFETY: all nelems slots of both vecs initialized above.
    unsafe {
        elems.set_len(nelems);
        nulls.set_len(nelems);
    }
    Ok((elems, nulls))
}

pub fn deconstruct_array_builtin<'mcx>(
    mcx: Mcx<'mcx>,
    array: &[u8],
    elmtype: Oid,
    allow_nulls: bool,
) -> PgResult<(PgVec<'mcx, Datum>, PgVec<'mcx, bool>)> {
    let (elmlen, elmbyval, elmalign) = builtin_meta(elmtype);
    deconstruct_array(mcx, array, elmlen, elmbyval, elmalign, allow_nulls)
}

// array_contains_nulls: accurate null scan over the bitmap.
pub fn array_contains_nulls(array: &[u8]) -> bool {
    let Some(bo) = arr_nullbitmap_off(array) else {
        return false;
    };
    let (ndim, dims, _lbs) = read_dims_lbounds(array);
    let mut nelems = match array_get_n_items(ndim, &dims) {
        Ok(n) => n,
        Err(_) => return true,
    };
    let mut b = bo;
    while nelems >= 8 {
        if array[b] != 0xFF {
            return true;
        }
        b += 1;
        nelems -= 8;
    }
    let mut bitmask: u32 = 1;
    while nelems > 0 {
        if (array[b] as u32 & bitmask) == 0 {
            return true;
        }
        bitmask <<= 1;
        nelems -= 1;
    }
    false
}

pub fn construct_empty_array<'mcx>(mcx: Mcx<'mcx>, elmtype: Oid) -> PgResult<PgVec<'mcx, u8>> {
    let mut out: PgVec<u8> = vec_with_capacity_in(mcx, ARRAYTYPE_HDRSZ)?;
    out.resize(ARRAYTYPE_HDRSZ, 0);
    write_header(&mut out, ARRAYTYPE_HDRSZ, 0, 0, elmtype);
    Ok(out)
}

pub fn construct_array<'mcx>(
    mcx: Mcx<'mcx>,
    elems: &[Datum],
    elmtype: Oid,
    elmlen: i32,
    elmbyval: bool,
    elmalign: u8,
) -> PgResult<PgVec<'mcx, u8>> {
    let dims = [elems.len() as i32];
    let lbs = [1i32];
    construct_md_array(mcx, elems, None, 1, &dims, &lbs, elmtype, elmlen, elmbyval, elmalign)
}

#[allow(clippy::too_many_arguments)]
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
        return Err(Box::new(PgError::error(alloc::format!(
            "invalid number of dimensions: {ndims}"
        ))));
    }
    if ndims as usize > MAXDIM {
        return Err(Box::new(
            PgError::error(alloc::format!(
                "number of array dimensions ({ndims}) exceeds the maximum allowed ({MAXDIM})"
            ))
            .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
        ));
    }
    let nelems = array_get_n_items(ndims, dims)?;
    array_check_bounds(ndims, dims, lbs)?;
    if nelems <= 0 {
        return construct_empty_array(mcx, elmtype);
    }
    let nelems = nelems as usize;

    // Pass 1: compute data space. C detoasts every varlena element here
    // ("make sure data is not toasted", arrayfuncs.c:3534-3538): an array
    // image must never contain an external TOAST pointer (it would dangle
    // once the source relation's toast data goes away), a compressed image,
    // or a short-header varlena — PG_DETOAST_DATUM expands all three to a
    // plain 4B-header value. C mutates the caller's elems[] in place; we
    // substitute a lazily-built scratch copy instead so callers' element
    // arrays are never damaged (the hazard C documents at accumArrayResult,
    // arrayfuncs.c:5393-5397: construct_md_array modifying the array_agg
    // build state would corrupt later finalfn calls).
    let mut detoasted: Option<PgVec<'mcx, Datum>> = None;
    let mut nbytes = 0usize;
    let mut hasnulls = false;
    for i in 0..nelems {
        if let Some(ns) = nulls {
            if ns[i] {
                hasnulls = true;
                continue;
            }
        }
        let mut d = elems[i];
        if elmlen == -1 {
            let p = d.as_usize() as *const u8;
            // SAFETY: non-null by-ref varlena datum addresses at least its
            // header byte. Tag & 0x03 != 0 = VARATT_IS_EXTENDED (external
            // 0x01 / compressed ..10 / short-header xxx1).
            if unsafe { *p } & 0x03 != 0 {
                d = detoast_element(mcx, p)?;
                let dv = match detoasted.as_mut() {
                    Some(v) => v,
                    None => {
                        let mut v: PgVec<'mcx, Datum> = vec_with_capacity_in(mcx, nelems)?;
                        v.extend_from_slice(&elems[..nelems]);
                        detoasted.insert(v)
                    }
                };
                dv[i] = d;
            }
        }
        let p = d.as_usize() as *const u8;
        nbytes = att_addlength_datum_offset(nbytes, elmlen, d, p);
        nbytes = att_align_nominal(nbytes, elmalign);
        if nbytes > MAX_ALLOC_SIZE {
            return Err(array_alloc_exceeded());
        }
    }
    let elems: &[Datum] = detoasted.as_deref().unwrap_or(elems);

    let dataoffset = if hasnulls {
        let d = arr_overhead_withnulls(ndims, nelems as i32);
        nbytes += d;
        d
    } else {
        nbytes += arr_overhead_nonulls(ndims);
        0
    };

    let mut out: PgVec<u8> = vec_with_capacity_in(mcx, nbytes)?;
    out.resize(nbytes, 0);
    write_header(&mut out, nbytes, ndims, dataoffset as i32, elmtype);
    write_dims_lbounds(&mut out, ndims, dims, lbs);
    copy_array_els(&mut out, elems, nulls, nelems, elmlen, elmbyval, elmalign);
    Ok(out)
}

// One toasted element found: replace it with a plain 4B-header copy in mcx.
// C construct_md_array's PG_DETOAST_DATUM (arrayfuncs.c:3536-3538) — fetch
// external, decompress, expand short headers. Cold: only arrays actually
// built from toasted/short datums pay it.
#[cold]
#[inline(never)]
fn detoast_element<'mcx>(mcx: Mcx<'mcx>, p: *const u8) -> PgResult<Datum> {
    // SAFETY: caller guarantees p addresses a live varlena image;
    // varsize_any reads only the header bytes it needs.
    let raw = unsafe { core::slice::from_raw_parts(p, varsize_any(p)) };
    let flat = ::detoast_seams::detoast_attr::call(mcx, raw)?;
    Ok(Datum::from_usize(flat.leak().as_ptr() as usize))
}

// att_addlength for a Datum whose by-ref payload is at p (fixed-len by-val is
// counted by attlen; by-ref uses varsize/strlen).
#[inline]
fn att_addlength_datum_offset(cur: usize, attlen: i32, _datum: Datum, p: *const u8) -> usize {
    if attlen > 0 {
        cur + attlen as usize
    } else {
        att_addlength_pointer(cur, attlen, p)
    }
}

pub(crate) fn write_header(out: &mut [u8], total_size: usize, ndim: i32, dataoffset: i32, elemtype: Oid) {
    out[0..4].copy_from_slice(&set_varsize_4b(total_size));
    out[4..8].copy_from_slice(&ndim.to_ne_bytes());
    out[8..12].copy_from_slice(&dataoffset.to_ne_bytes());
    out[12..16].copy_from_slice(&(elemtype as u32).to_ne_bytes());
}

pub(crate) fn write_dims_lbounds(out: &mut [u8], ndim: i32, dims: &[i32], lbs: &[i32]) {
    let mut off = ARRAYTYPE_HDRSZ;
    for i in 0..ndim as usize {
        out[off..off + 4].copy_from_slice(&dims[i].to_ne_bytes());
        off += 4;
    }
    for i in 0..ndim as usize {
        out[off..off + 4].copy_from_slice(&lbs[i].to_ne_bytes());
        off += 4;
    }
}

// CopyArrayEls over the fully-headered image: writes packed data + null bitmap.
pub(crate) fn copy_array_els(
    image: &mut [u8],
    values: &[Datum],
    nulls: Option<&[bool]>,
    nitems: usize,
    typlen: i32,
    typbyval: bool,
    typalign: u8,
) {
    let mut data_off = arr_data_offset(image);
    let bitmap_off = arr_nullbitmap_off(image);
    let mut bitval: u32 = 0;
    let mut bitmask: u32 = 1;
    let mut bitmap_byte = 0usize;

    for i in 0..nitems {
        let is_null = nulls.map(|n| n[i]).unwrap_or(false);
        if is_null {
            // bitmap bit stays 0
        } else {
            bitval |= bitmask;
            let inc = array_cast_and_set(
                values[i],
                typlen,
                typbyval,
                typalign,
                &mut image[data_off..],
            );
            data_off += inc;
        }
        if let Some(bo) = bitmap_off {
            bitmask <<= 1;
            if bitmask == 0x100 {
                image[bo + bitmap_byte] = bitval as u8;
                bitmap_byte += 1;
                bitval = 0;
                bitmask = 1;
            }
        }
    }
    if let Some(bo) = bitmap_off {
        if bitmask != 1 {
            image[bo + bitmap_byte] = bitval as u8;
        }
    }
}

// C `ArrayGetIntegerTypmods` (arrayutils.c); lives here because it needs
// deconstruct_array and arrayutils sits below this crate.
pub fn array_get_integer_typmods<'mcx>(
    mcx: Mcx<'mcx>,
    arr: &[u8],
) -> PgResult<PgVec<'mcx, i32>> {
    if arr_elemtype(arr) != CSTRINGOID {
        return Err(Box::new(
            PgError::error("typmod array must be type cstring[]")
                .with_sqlstate(::types_error::ERRCODE_ARRAY_ELEMENT_ERROR),
        ));
    }
    if arr_ndim(arr) != 1 {
        return Err(Box::new(
            PgError::error("typmod array must be one-dimensional")
                .with_sqlstate(::types_error::ERRCODE_ARRAY_SUBSCRIPT_ERROR),
        ));
    }
    if array_contains_nulls(arr) {
        return Err(Box::new(
            PgError::error("typmod array must not contain nulls")
                .with_sqlstate(ERRCODE_NULL_VALUE_NOT_ALLOWED),
        ));
    }

    let (elems, _nulls) = deconstruct_array_builtin(mcx, arr, CSTRINGOID, false)?;
    let mut out: PgVec<'mcx, i32> = vec_with_capacity_in(mcx, elems.len())?;
    for d in elems.iter() {
        // SAFETY: each element datum is a live NUL-terminated cstring from
        // the deconstructed image.
        let s = unsafe { core::ffi::CStr::from_ptr(d.as_usize() as *const core::ffi::c_char) };
        out.push(::numutils::pg_strtoint32(&alloc::string::String::from_utf8_lossy(
            s.to_bytes(),
        ))?);
    }
    Ok(out)
}
