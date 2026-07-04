use alloc::boxed::Box;

use ::arrayutils::{
    array_check_bounds, array_get_n_items, array_get_offset, mda_get_offset_values, mda_get_prod,
    mda_get_range, mda_next_tuple,
};
use ::datum::{varlena::set_varsize_4b, Datum};
use ::mcx::{vec_with_capacity_in, Mcx, PgVec};
use ::types_core::Oid;
use ::types_error::{
    PgError, PgResult, ERRCODE_ARRAY_SUBSCRIPT_ERROR, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_NULL_VALUE_NOT_ALLOWED, ERRCODE_PROGRAM_LIMIT_EXCEEDED,
};

use crate::construct::{construct_empty_array, construct_md_array};
use crate::foundation::*;

#[inline]
fn get_isnull(array: &[u8], bitmap_off: Option<usize>, offset: i32) -> bool {
    match bitmap_off {
        None => false,
        Some(bo) => (array[bo + offset as usize / 8] & (1 << (offset % 8))) == 0,
    }
}

fn set_isnull(array: &mut [u8], bitmap_off: usize, offset: i32, is_null: bool) {
    let b = bitmap_off + offset as usize / 8;
    let mask = 1u8 << (offset % 8);
    if is_null {
        array[b] &= !mask;
    } else {
        array[b] |= mask;
    }
}

// array_seek over a byte image: advance `pos` (byte offset into `array`) past
// `nitems` elements, of which the first is linear element `offset`.
fn seek(
    array: &[u8],
    mut pos: usize,
    offset: i32,
    bitmap_off: Option<usize>,
    nitems: i32,
    typlen: i32,
    typalign: u8,
) -> usize {
    if typlen > 0 && bitmap_off.is_none() {
        return pos + nitems as usize * att_align_nominal(typlen as usize, typalign);
    }
    match bitmap_off {
        Some(bo) => {
            let mut bptr = bo + offset as usize / 8;
            let mut bitmask = 1u32 << (offset % 8);
            for _ in 0..nitems {
                if (array[bptr] as u32 & bitmask) != 0 {
                    pos = att_addlength_pointer(pos, typlen, array[pos..].as_ptr());
                    pos = att_align_nominal(pos, typalign);
                }
                bitmask <<= 1;
                if bitmask == 0x100 {
                    bptr += 1;
                    bitmask = 1;
                }
            }
        }
        None => {
            for _ in 0..nitems {
                pos = att_addlength_pointer(pos, typlen, array[pos..].as_ptr());
                pos = att_align_nominal(pos, typalign);
            }
        }
    }
    pos
}

#[inline]
fn nelems_size(
    array: &[u8],
    pos: usize,
    offset: i32,
    bitmap_off: Option<usize>,
    nitems: i32,
    typlen: i32,
    typalign: u8,
) -> usize {
    seek(array, pos, offset, bitmap_off, nitems, typlen, typalign) - pos
}

// array_bitmap_copy; src bitmap absent = all-non-NULL ones.
pub fn array_bitmap_copy(
    dest: &mut [u8],
    dest_bitmap_off: usize,
    destoffset: i32,
    src: Option<(&[u8], usize)>,
    srcoffset: i32,
    nitems: i32,
) {
    if nitems <= 0 {
        return;
    }
    let mut db = dest_bitmap_off + destoffset as usize / 8;
    let mut dmask = 1u8 << (destoffset % 8);
    let mut dval = dest[db];
    match src {
        Some((s, sbo)) => {
            let mut sb = sbo + srcoffset as usize / 8;
            let mut smask = 1u8 << (srcoffset % 8);
            let mut sval = s[sb];
            for _ in 0..nitems {
                if sval & smask != 0 {
                    dval |= dmask;
                } else {
                    dval &= !dmask;
                }
                if dmask == 0x80 {
                    dest[db] = dval;
                    db += 1;
                    dmask = 1;
                    dval = dest[db];
                } else {
                    dmask <<= 1;
                }
                if smask == 0x80 {
                    sb += 1;
                    smask = 1;
                    sval = s[sb];
                } else {
                    smask <<= 1;
                }
            }
        }
        None => {
            for _ in 0..nitems {
                dval |= dmask;
                if dmask == 0x80 {
                    dest[db] = dval;
                    db += 1;
                    dmask = 1;
                    dval = dest[db];
                } else {
                    dmask <<= 1;
                }
            }
        }
    }
    dest[db] = dval;
}

#[cold]
fn wrong_subscripts() -> Box<PgError> {
    Box::new(
        PgError::error("wrong number of array subscripts")
            .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
    )
}

#[cold]
fn subscript_out_of_range() -> Box<PgError> {
    Box::new(
        PgError::error("array subscript out of range")
            .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
    )
}

#[cold]
fn array_size_exceeded() -> Box<PgError> {
    Box::new(
        PgError::error(alloc::format!(
            "array size exceeds the maximum allowed ({})",
            ::arrayutils::MAX_ARRAY_SIZE
        ))
        .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
    )
}

// array_get_element over a detoasted flat image. By-ref result Datums point
// into `array`. Fixed-length container types (arraytyplen > 0) are assumed
// 1-d, 0-based per C. Returns (datum, isnull).
pub fn array_get_element(
    array: &[u8],
    indx: &[i32],
    arraytyplen: i32,
    elmlen: i32,
    elmbyval: bool,
    elmalign: u8,
) -> (Datum, bool) {
    let n_subscripts = indx.len() as i32;
    let (ndim, dims, lbs, data_off, bitmap_off);
    let fixed;
    if arraytyplen > 0 {
        ndim = 1;
        fixed = ([arraytyplen / elmlen, 0, 0, 0, 0, 0], [0i32; MAXDIM]);
        dims = &fixed.0;
        lbs = &fixed.1;
        data_off = 0usize;
        bitmap_off = None;
    } else {
        let (nd, d, l) = read_dims_lbounds(array);
        ndim = nd;
        fixed = (d, l);
        dims = &fixed.0;
        lbs = &fixed.1;
        data_off = arr_data_offset(array);
        bitmap_off = arr_nullbitmap_off(array);
    }

    if ndim != n_subscripts || ndim <= 0 || ndim as usize > MAXDIM {
        return (Datum::null(), true);
    }
    for i in 0..ndim as usize {
        if indx[i] < lbs[i] || indx[i] >= dims[i] + lbs[i] {
            return (Datum::null(), true);
        }
    }

    let offset = array_get_offset(n_subscripts, dims, lbs, indx);
    if get_isnull(array, bitmap_off, offset) {
        return (Datum::null(), true);
    }
    let pos = seek(array, data_off, 0, bitmap_off, offset, elmlen, elmalign);
    (fetch_att(array[pos..].as_ptr(), elmbyval, elmlen), false)
}

// array_slice_size: data bytes of the slice st..endp.
#[allow(clippy::too_many_arguments)]
fn slice_size(
    array: &[u8],
    data_off: usize,
    bitmap_off: Option<usize>,
    ndim: i32,
    dims: &[i32],
    lbs: &[i32],
    st: &[i32],
    endp: &[i32],
    elmlen: i32,
    elmalign: u8,
) -> PgResult<usize> {
    let mut span = [0i32; MAXDIM];
    mda_get_range(ndim, &mut span, st, endp);

    if elmlen > 0 && bitmap_off.is_none() {
        let n = array_get_n_items(ndim, &span)?;
        return Ok(n as usize * att_align_nominal(elmlen as usize, elmalign));
    }

    let mut src_offset = array_get_offset(ndim, dims, lbs, st);
    let mut pos = seek(array, data_off, 0, bitmap_off, src_offset, elmlen, elmalign);
    let mut prod = [0i32; MAXDIM];
    let mut dist = [0i32; MAXDIM];
    let mut indx = [0i32; MAXDIM];
    mda_get_prod(ndim, dims, &mut prod);
    mda_get_offset_values(ndim, &mut dist, &prod, &span);
    let mut count = 0usize;
    let mut j = ndim - 1;
    loop {
        let ju = j as usize;
        if dist[ju] != 0 {
            pos = seek(array, pos, src_offset, bitmap_off, dist[ju], elmlen, elmalign);
            src_offset += dist[ju];
        }
        if !get_isnull(array, bitmap_off, src_offset) {
            let inc = att_align_nominal(
                att_addlength_pointer(0, elmlen, array[pos..].as_ptr()),
                elmalign,
            );
            pos += inc;
            count += inc;
        }
        src_offset += 1;
        j = mda_next_tuple(ndim, &mut indx, &span);
        if j == -1 {
            break;
        }
    }
    Ok(count)
}

// array_get_slice over a detoasted flat image; scribbles on upper/lower like C
// (missing bounds are filled from the array range). Result lower bounds are 1.
#[allow(clippy::too_many_arguments)]
pub fn array_get_slice<'mcx>(
    mcx: Mcx<'mcx>,
    array: &[u8],
    n_subscripts: i32,
    upper: &mut [i32],
    lower: &mut [i32],
    upper_provided: &[bool],
    lower_provided: &[bool],
    arraytyplen: i32,
    elmlen: i32,
    elmalign: u8,
) -> PgResult<PgVec<'mcx, u8>> {
    if arraytyplen > 0 {
        return Err(Box::new(
            PgError::error("slices of fixed-length arrays not implemented")
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
        ));
    }
    let (ndim, dims, lbs) = read_dims_lbounds(array);
    let elemtype: Oid = arr_elemtype(array);
    let data_off = arr_data_offset(array);
    let bitmap_off = arr_nullbitmap_off(array);

    if ndim < n_subscripts || ndim <= 0 || ndim as usize > MAXDIM {
        return construct_empty_array(mcx, elemtype);
    }
    for i in 0..n_subscripts as usize {
        if !lower_provided[i] || lower[i] < lbs[i] {
            lower[i] = lbs[i];
        }
        if !upper_provided[i] || upper[i] >= dims[i] + lbs[i] {
            upper[i] = dims[i] + lbs[i] - 1;
        }
        if lower[i] > upper[i] {
            return construct_empty_array(mcx, elemtype);
        }
    }
    for i in n_subscripts as usize..ndim as usize {
        lower[i] = lbs[i];
        upper[i] = dims[i] + lbs[i] - 1;
        if lower[i] > upper[i] {
            return construct_empty_array(mcx, elemtype);
        }
    }

    let mut span = [0i32; MAXDIM];
    mda_get_range(ndim, &mut span, lower, upper);
    let mut bytes = slice_size(
        array, data_off, bitmap_off, ndim, &dims, &lbs, lower, upper, elmlen, elmalign,
    )?;

    let dataoffset = if bitmap_off.is_some() {
        let d = arr_overhead_withnulls(ndim, array_get_n_items(ndim, &span)?);
        bytes += d;
        d
    } else {
        bytes += arr_overhead_nonulls(ndim);
        0
    };

    let mut out: PgVec<u8> = vec_with_capacity_in(mcx, bytes)?;
    out.resize(bytes, 0);
    out[0..4].copy_from_slice(&set_varsize_4b(bytes));
    out[4..8].copy_from_slice(&ndim.to_ne_bytes());
    out[8..12].copy_from_slice(&(dataoffset as i32).to_ne_bytes());
    out[12..16].copy_from_slice(&(elemtype as u32).to_ne_bytes());
    let mut off = ARRAYTYPE_HDRSZ;
    for i in 0..ndim as usize {
        out[off..off + 4].copy_from_slice(&span[i].to_ne_bytes());
        off += 4;
    }
    for _ in 0..ndim as usize {
        out[off..off + 4].copy_from_slice(&1i32.to_ne_bytes());
        off += 4;
    }

    extract_slice(
        &mut out, array, data_off, bitmap_off, ndim, &dims, &lbs, lower, upper, elmlen, elmalign,
    );
    Ok(out)
}

#[allow(clippy::too_many_arguments)]
fn extract_slice(
    dest: &mut [u8],
    array: &[u8],
    data_off: usize,
    bitmap_off: Option<usize>,
    ndim: i32,
    dims: &[i32],
    lbs: &[i32],
    st: &[i32],
    endp: &[i32],
    elmlen: i32,
    elmalign: u8,
) {
    let dest_data_off = arr_data_offset(dest);
    let dest_bitmap_off = arr_nullbitmap_off(dest);
    let mut src_offset = array_get_offset(ndim, dims, lbs, st);
    let mut src_pos = seek(array, data_off, 0, bitmap_off, src_offset, elmlen, elmalign);
    let mut dest_pos = dest_data_off;

    let mut prod = [0i32; MAXDIM];
    let mut span = [0i32; MAXDIM];
    let mut dist = [0i32; MAXDIM];
    let mut indx = [0i32; MAXDIM];
    mda_get_prod(ndim, dims, &mut prod);
    mda_get_range(ndim, &mut span, st, endp);
    mda_get_offset_values(ndim, &mut dist, &prod, &span);
    let mut dest_offset = 0i32;
    let mut j = ndim - 1;
    loop {
        let ju = j as usize;
        if dist[ju] != 0 {
            src_pos = seek(array, src_pos, src_offset, bitmap_off, dist[ju], elmlen, elmalign);
            src_offset += dist[ju];
        }
        let inc = nelems_size(array, src_pos, src_offset, bitmap_off, 1, elmlen, elmalign);
        dest[dest_pos..dest_pos + inc].copy_from_slice(&array[src_pos..src_pos + inc]);
        if let Some(dbo) = dest_bitmap_off {
            array_bitmap_copy(
                dest,
                dbo,
                dest_offset,
                bitmap_off.map(|bo| (array, bo)),
                src_offset,
                1,
            );
        }
        dest_pos += inc;
        src_pos += inc;
        src_offset += 1;
        dest_offset += 1;
        j = mda_next_tuple(ndim, &mut indx, &span);
        if j == -1 {
            break;
        }
    }
    let _ = dest_offset;
}

// array_set_element over a detoasted flat image; the replacement datum (if
// by-ref) must already be detoasted. Returns a new image (C never updates the
// source in the flat-array case).
#[allow(clippy::too_many_arguments)]
pub fn array_set_element<'mcx>(
    mcx: Mcx<'mcx>,
    array: &[u8],
    indx: &[i32],
    data_value: Datum,
    isnull: bool,
    arraytyplen: i32,
    elmlen: i32,
    elmbyval: bool,
    elmalign: u8,
) -> PgResult<PgVec<'mcx, u8>> {
    let n_subscripts = indx.len() as i32;

    if arraytyplen > 0 {
        // Fixed-length container: 1-d, 0-based, no extension.
        if n_subscripts != 1 {
            return Err(wrong_subscripts());
        }
        if indx[0] < 0 || indx[0] >= arraytyplen / elmlen {
            return Err(subscript_out_of_range());
        }
        if isnull {
            return Err(Box::new(
                PgError::error("cannot assign null value to an element of a fixed-length array")
                    .with_sqlstate(ERRCODE_NULL_VALUE_NOT_ALLOWED),
            ));
        }
        let mut out: PgVec<u8> = vec_with_capacity_in(mcx, arraytyplen as usize)?;
        out.extend_from_slice(&array[..arraytyplen as usize]);
        let pos = indx[0] as usize * elmlen as usize;
        array_cast_and_set(data_value, elmlen, elmbyval, elmalign, &mut out[pos..]);
        return Ok(out);
    }

    if n_subscripts <= 0 || n_subscripts as usize > MAXDIM {
        return Err(wrong_subscripts());
    }

    let (ndim, mut dims, mut lbs) = read_dims_lbounds(array);

    if ndim == 0 {
        let elmtype = arr_elemtype(array);
        let mut dim = [0i32; MAXDIM];
        let mut lb = [0i32; MAXDIM];
        for i in 0..n_subscripts as usize {
            dim[i] = 1;
            lb[i] = indx[i];
        }
        return construct_md_array(
            mcx,
            &[data_value],
            Some(&[isnull]),
            n_subscripts,
            &dim,
            &lb,
            elmtype,
            elmlen,
            elmbyval,
            elmalign,
        );
    }

    if ndim != n_subscripts {
        return Err(wrong_subscripts());
    }

    let bitmap_off = arr_nullbitmap_off(array);
    let data_off = arr_data_offset(array);
    let mut newhasnulls = bitmap_off.is_some() || isnull;
    let mut addedbefore = 0i32;
    let mut addedafter = 0i32;

    if ndim == 1 {
        if indx[0] < lbs[0] {
            let (ab, o1) = lbs[0].overflowing_sub(indx[0]);
            let (nd, o2) = dims[0].overflowing_add(ab);
            if o1 || o2 {
                return Err(array_size_exceeded());
            }
            addedbefore = ab;
            dims[0] = nd;
            lbs[0] = indx[0];
            if addedbefore > 1 {
                newhasnulls = true;
            }
        }
        if indx[0] >= dims[0] + lbs[0] {
            let (aa, o1) = indx[0].overflowing_sub(dims[0] + lbs[0]);
            let (aa, o2) = aa.overflowing_add(1);
            let (nd, o3) = dims[0].overflowing_add(aa);
            if o1 || o2 || o3 {
                return Err(array_size_exceeded());
            }
            addedafter = aa;
            dims[0] = nd;
            if addedafter > 1 {
                newhasnulls = true;
            }
        }
    } else {
        for i in 0..ndim as usize {
            if indx[i] < lbs[i] || indx[i] >= dims[i] + lbs[i] {
                return Err(subscript_out_of_range());
            }
        }
    }

    let newnitems = array_get_n_items(ndim, &dims)?;
    array_check_bounds(ndim, &dims, &lbs)?;

    let overheadlen = if newhasnulls {
        arr_overhead_withnulls(ndim, newnitems)
    } else {
        arr_overhead_nonulls(ndim)
    };
    let (old_ndim, old_dims, _old_lbs) = read_dims_lbounds(array);
    let oldnitems = array_get_n_items(old_ndim, &old_dims)?;
    let olddatasize = arr_size(array) - data_off;

    let (offset, lenbefore, olditemlen, lenafter);
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
        offset = array_get_offset(n_subscripts, &dims, &lbs, indx);
        let elt_pos = seek(array, data_off, 0, bitmap_off, offset, elmlen, elmalign);
        lenbefore = elt_pos - data_off;
        olditemlen = if get_isnull(array, bitmap_off, offset) {
            0
        } else {
            att_align_nominal(
                att_addlength_pointer(0, elmlen, array[elt_pos..].as_ptr()),
                elmalign,
            )
        };
        lenafter = olddatasize - lenbefore - olditemlen;
    }

    let newitemlen = if isnull {
        0
    } else {
        att_align_nominal(
            att_addlength_pointer(0, elmlen, data_value.as_usize() as *const u8),
            elmalign,
        )
    };

    let newsize = overheadlen + lenbefore + newitemlen + lenafter;

    let mut out: PgVec<u8> = vec_with_capacity_in(mcx, newsize)?;
    out.resize(newsize, 0);
    out[0..4].copy_from_slice(&set_varsize_4b(newsize));
    out[4..8].copy_from_slice(&ndim.to_ne_bytes());
    out[8..12].copy_from_slice(&(if newhasnulls { overheadlen as i32 } else { 0 }).to_ne_bytes());
    out[12..16].copy_from_slice(&(arr_elemtype(array) as u32).to_ne_bytes());
    let mut off = ARRAYTYPE_HDRSZ;
    for i in 0..ndim as usize {
        out[off..off + 4].copy_from_slice(&dims[i].to_ne_bytes());
        off += 4;
    }
    for i in 0..ndim as usize {
        out[off..off + 4].copy_from_slice(&lbs[i].to_ne_bytes());
        off += 4;
    }

    out[overheadlen..overheadlen + lenbefore]
        .copy_from_slice(&array[data_off..data_off + lenbefore]);
    if !isnull {
        array_cast_and_set(
            data_value,
            elmlen,
            elmbyval,
            elmalign,
            &mut out[overheadlen + lenbefore..],
        );
    }
    out[overheadlen + lenbefore + newitemlen..overheadlen + lenbefore + newitemlen + lenafter]
        .copy_from_slice(
            &array[data_off + lenbefore + olditemlen..data_off + lenbefore + olditemlen + lenafter],
        );

    if newhasnulls {
        let new_bo = ARRAYTYPE_HDRSZ + 2 * 4 * ndim as usize;
        // resize(0) above left inserted positions as nulls (bit 0).
        if addedafter != 0 {
            set_isnull(&mut out, new_bo, newnitems - 1, isnull);
        } else {
            set_isnull(&mut out, new_bo, offset, isnull);
        }
        if addedbefore != 0 {
            array_bitmap_copy(
                &mut out,
                new_bo,
                addedbefore,
                bitmap_off.map(|bo| (array, bo)),
                0,
                oldnitems,
            );
        } else {
            array_bitmap_copy(
                &mut out,
                new_bo,
                0,
                bitmap_off.map(|bo| (array, bo)),
                0,
                offset,
            );
            if addedafter == 0 {
                array_bitmap_copy(
                    &mut out,
                    new_bo,
                    offset + 1,
                    bitmap_off.map(|bo| (array, bo)),
                    offset + 1,
                    oldnitems - offset - 1,
                );
            }
        }
    }

    Ok(out)
}
