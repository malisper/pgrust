//! detoast.c: retrieve compressed or external varlena attributes, plus the
//! toast_compression.c decompression dispatch (pglz inline via the pglz
//! crate; this build has no LZ4, matching C without USE_LZ4). Values are raw
//! varlena images (`&[u8]`, header included); results are fresh 4B-header
//! images charged to the caller's `Mcx`. External on-disk fetch crosses
//! `toast_internals_seams` (loud until the toast unit lands); indirect arms
//! dereference the embedded pointer (`struct varatt_indirect`); expanded arms
//! flatten through `datum::expandeddatum`.

use mcx::{Mcx, PgVec};
use types_error::{PgError, PgResult, ERRCODE_DATA_CORRUPTED, ERRCODE_FEATURE_NOT_SUPPORTED};
use types_tuple::varatt::{
    self, VARHDRSZ, VARHDRSZ_EXTERNAL, VARHDRSZ_SHORT, VARTAG_INDIRECT, VARTAG_ONDISK,
};

#[cfg(test)]
mod tests;

const VARLENA_EXTSIZE_BITS: u32 = 30;
const VARLENA_EXTSIZE_MASK: u32 = (1 << VARLENA_EXTSIZE_BITS) - 1;
const VARHDRSZ_COMPRESSED: usize = VARHDRSZ + 4;

pub const TOAST_PGLZ_COMPRESSION_ID: u32 = 0;
pub const TOAST_LZ4_COMPRESSION_ID: u32 = 1;

#[inline]
fn is_external(b: &[u8]) -> bool {
    b[0] == 0x01
}

#[inline]
fn is_external_ondisk(b: &[u8]) -> bool {
    is_external(b) && b[1] == VARTAG_ONDISK
}

#[inline]
fn is_external_indirect(b: &[u8]) -> bool {
    is_external(b) && b[1] == VARTAG_INDIRECT
}

#[inline]
fn is_external_expanded(b: &[u8]) -> bool {
    is_external(b) && varatt::vartag_is_expanded(b[1])
}

#[inline]
fn is_compressed(b: &[u8]) -> bool {
    (b[0] & 0x03) == 0x02
}

#[inline]
fn is_short(b: &[u8]) -> bool {
    (b[0] & 0x01) == 0x01
}

#[inline]
fn varsize_4b(b: &[u8]) -> usize {
    varatt::varsize_4b_word(u32::from_ne_bytes([b[0], b[1], b[2], b[3]])) as usize
}

#[inline]
fn varsize_short(b: &[u8]) -> usize {
    ((b[0] >> 1) & 0x7F) as usize
}

pub fn varsize_any(b: &[u8]) -> usize {
    if is_external(b) {
        VARHDRSZ_EXTERNAL + varatt::vartag_size(b[1])
    } else if is_short(b) {
        varsize_short(b)
    } else {
        varsize_4b(b)
    }
}

/// `struct varatt_external` (varatt.h), copied out of the unaligned image.
#[derive(Clone, Copy, Debug)]
pub struct VarattExternal {
    pub va_rawsize: i32,
    pub va_extinfo: u32,
    pub va_valueid: u32,
    pub va_toastrelid: u32,
}

impl VarattExternal {
    pub fn from_image(attr: &[u8]) -> Self {
        let p = &attr[VARHDRSZ_EXTERNAL..VARHDRSZ_EXTERNAL + 16];
        Self {
            va_rawsize: i32::from_ne_bytes([p[0], p[1], p[2], p[3]]),
            va_extinfo: u32::from_ne_bytes([p[4], p[5], p[6], p[7]]),
            va_valueid: u32::from_ne_bytes([p[8], p[9], p[10], p[11]]),
            va_toastrelid: u32::from_ne_bytes([p[12], p[13], p[14], p[15]]),
        }
    }

    #[inline]
    pub fn extsize(&self) -> u32 {
        self.va_extinfo & VARLENA_EXTSIZE_MASK
    }

    #[inline]
    pub fn compress_method(&self) -> u32 {
        self.va_extinfo >> VARLENA_EXTSIZE_BITS
    }

    // C compares uint32 < int32 - 4 under the usual unsigned conversion.
    #[inline]
    pub fn is_compressed(&self) -> bool {
        self.extsize() < (self.va_rawsize as u32).wrapping_sub(VARHDRSZ as u32)
    }
}

#[inline]
fn tcinfo(b: &[u8]) -> u32 {
    u32::from_ne_bytes([b[4], b[5], b[6], b[7]])
}

#[inline]
fn toast_compress_method(b: &[u8]) -> u32 {
    tcinfo(b) >> VARLENA_EXTSIZE_BITS
}

#[inline]
fn toast_compress_extsize(b: &[u8]) -> u32 {
    tcinfo(b) & VARLENA_EXTSIZE_MASK
}

fn copy_verbatim<'mcx>(mcx: Mcx<'mcx>, attr: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    mcx::slice_in(mcx, &attr[..varsize_any(attr)])
}

/// `struct varatt_indirect` (varatt.h) holds a raw pointer to a live varlena;
/// dereference it into a borrowed slice sized off its own header.
///
/// # Safety
/// `attr` is a live `VARTAG_INDIRECT` image (writer invariant: the embedded
/// pointer stays valid for the indirect Datum's lifetime, matching C's
/// `struct varatt_indirect`).
unsafe fn indirect_target<'a>(attr: &[u8]) -> &'a [u8] {
    let raw = usize::from_ne_bytes(
        attr[VARHDRSZ_EXTERNAL..VARHDRSZ_EXTERNAL + 8]
            .try_into()
            .unwrap(),
    );
    let ptr = raw as *const u8;
    // SAFETY: caller contract.
    unsafe {
        let len = varatt::varsize_any(ptr);
        core::slice::from_raw_parts(ptr, len)
    }
}

fn flatten_expanded<'mcx>(mcx: Mcx<'mcx>, attr: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    // SAFETY: an expanded TOAST image embeds a pointer to its live
    // ExpandedObjectHeader (writer invariant; C detoast.c derefs the same);
    // flatten_into fills exactly `n` bytes of the reserved capacity.
    unsafe {
        let eoh = datum::expandeddatum::datum_get_eohp(datum::Datum::from_usize(
            attr.as_ptr() as usize,
        ));
        let n = datum::expandeddatum::eoh_get_flat_size(eoh);
        let mut result = mcx::vec_with_capacity_in(mcx, n)?;
        datum::expandeddatum::eoh_flatten_into(
            eoh,
            result.spare_capacity_mut().as_mut_ptr() as *mut u8,
            n,
        );
        result.set_len(n);
        Ok(result)
    }
}

/// C `detoast_external_attr`: fetch a toasted value back from external
/// storage; the result can still be compressed or have a short header.
pub fn detoast_external_attr<'mcx>(mcx: Mcx<'mcx>, attr: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    if is_external_ondisk(attr) {
        toast_internals_seams::toast_fetch_datum::call(mcx, attr)
    } else if is_external_indirect(attr) {
        // SAFETY: writer invariant (see indirect_target).
        let target = unsafe { indirect_target(attr) };
        debug_assert!(!is_external_indirect(target));
        if is_external(target) {
            detoast_external_attr(mcx, target)
        } else {
            copy_verbatim(mcx, target)
        }
    } else if is_external_expanded(attr) {
        flatten_expanded(mcx, attr)
    } else {
        // C returns `attr` unchanged; this owned port copies verbatim.
        copy_verbatim(mcx, attr)
    }
}

/// C `detoast_attr`: fetch/decompress to non-extended (plain 4B-header) form.
pub fn detoast_attr<'mcx>(mcx: Mcx<'mcx>, attr: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    if is_external_ondisk(attr) {
        let fetched = toast_internals_seams::toast_fetch_datum::call(mcx, attr)?;
        if is_compressed(&fetched) {
            toast_decompress_datum(mcx, &fetched)
        } else {
            Ok(fetched)
        }
    } else if is_external_indirect(attr) {
        // SAFETY: writer invariant (see indirect_target).
        let target = unsafe { indirect_target(attr) };
        debug_assert!(!is_external_indirect(target));
        // C copies iff the recursion returned the dereferenced pointer
        // unchanged; this port's fallthrough already always returns an
        // owned copy, so the plain recursive call matches C's semantics.
        detoast_attr(mcx, target)
    } else if is_external_expanded(attr) {
        let flat = flatten_expanded(mcx, attr)?;
        // C: flatteners are not allowed to produce compressed/short output.
        debug_assert!(!is_external(&flat) && !is_compressed(&flat) && !is_short(&flat));
        Ok(flat)
    } else if is_compressed(attr) {
        toast_decompress_datum(mcx, attr)
    } else if is_short(attr) {
        let data_size = varsize_short(attr) - VARHDRSZ_SHORT;
        let new_size = data_size + VARHDRSZ;
        let mut new_attr = mcx::vec_with_capacity_in(mcx, new_size)?;
        mcx::vec_append_bytes(
            &mut new_attr,
            &varatt::set_varsize_4b_word(new_size as u32).to_ne_bytes(),
        )?;
        mcx::vec_append_bytes(&mut new_attr, &attr[VARHDRSZ_SHORT..VARHDRSZ_SHORT + data_size])?;
        Ok(new_attr)
    } else {
        // C returns `attr` unchanged; this owned port copies verbatim.
        copy_verbatim(mcx, attr)
    }
}

/// C `detoast_attr_slice`: part of a toasted value; `sliceoffset >= 0`,
/// `slicelength < 0` means everything beyond the offset.
pub fn detoast_attr_slice<'mcx>(
    mcx: Mcx<'mcx>,
    attr: &[u8],
    sliceoffset: i32,
    mut slicelength: i32,
) -> PgResult<PgVec<'mcx, u8>> {
    if sliceoffset < 0 {
        return Err(PgError::error(format!("invalid sliceoffset: {sliceoffset}")).into());
    }

    // slicelimit = offset + length, or -1 (fetch all) on overflow.
    let slicelimit = if slicelength < 0 {
        -1
    } else {
        match sliceoffset.checked_add(slicelength) {
            Some(limit) => limit,
            None => {
                slicelength = -1;
                -1
            }
        }
    };

    let preslice_owned: Option<PgVec<'mcx, u8>> = if is_external_ondisk(attr) {
        let toast_pointer = VarattExternal::from_image(attr);

        if !toast_pointer.is_compressed() {
            return toast_internals_seams::toast_fetch_datum_slice::call(
                mcx,
                attr,
                sliceoffset,
                slicelength,
            );
        }

        if slicelimit >= 0 {
            let mut max_size = toast_pointer.extsize() as i32;
            // LZ4 has no prefix-size API; fetch the whole thing for it.
            if toast_pointer.compress_method() == TOAST_PGLZ_COMPRESSION_ID {
                max_size = pglz::pglz_maximum_compressed_size(slicelimit, max_size);
            }
            Some(toast_internals_seams::toast_fetch_datum_slice::call(
                mcx, attr, 0, max_size,
            )?)
        } else {
            Some(toast_internals_seams::toast_fetch_datum::call(mcx, attr)?)
        }
    } else if is_external_indirect(attr) {
        // SAFETY: writer invariant (see indirect_target).
        let target = unsafe { indirect_target(attr) };
        debug_assert!(!is_external_indirect(target));
        return detoast_attr_slice(mcx, target, sliceoffset, slicelength);
    } else if is_external_expanded(attr) {
        Some(flatten_expanded(mcx, attr)?)
    } else {
        None
    };
    let preslice: &[u8] = preslice_owned.as_deref().unwrap_or(attr);
    debug_assert!(!is_external(preslice));

    let decompressed: Option<PgVec<'mcx, u8>> = if is_compressed(preslice) {
        Some(if slicelimit >= 0 {
            toast_decompress_datum_slice(mcx, preslice, slicelimit)?
        } else {
            toast_decompress_datum(mcx, preslice)?
        })
    } else {
        None
    };
    let view: &[u8] = decompressed.as_deref().unwrap_or(preslice);

    let (attrdata, attrsize) = if is_short(view) {
        (
            &view[VARHDRSZ_SHORT..],
            (varsize_short(view) - VARHDRSZ_SHORT) as i32,
        )
    } else {
        (&view[VARHDRSZ..], (varsize_4b(view) - VARHDRSZ) as i32)
    };

    let mut sliceoffset = sliceoffset;
    if sliceoffset >= attrsize {
        sliceoffset = 0;
        slicelength = 0;
    } else if slicelength < 0 || slicelimit > attrsize {
        slicelength = attrsize - sliceoffset;
    }

    let total = slicelength as usize + VARHDRSZ;
    let mut result = mcx::vec_with_capacity_in(mcx, total)?;
    mcx::vec_append_bytes(
        &mut result,
        &varatt::set_varsize_4b_word(total as u32).to_ne_bytes(),
    )?;
    mcx::vec_append_bytes(
        &mut result,
        &attrdata[sliceoffset as usize..(sliceoffset + slicelength) as usize],
    )?;
    Ok(result)
}

#[cold]
#[inline(never)]
fn corrupt_pglz() -> PgError {
    PgError::error("compressed pglz data is corrupt").with_sqlstate(ERRCODE_DATA_CORRUPTED)
}

#[cold]
#[inline(never)]
fn no_lz4_support() -> PgError {
    PgError::error("compression method lz4 not supported")
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
}

#[cold]
#[inline(never)]
fn invalid_compression_id(cmid: u32) -> PgError {
    PgError::error(format!("invalid compression method id {cmid}"))
}

/// C `toast_decompress_datum` + the pglz arm of toast_compression.c's
/// `pglz_decompress_datum`.
pub fn toast_decompress_datum<'mcx>(mcx: Mcx<'mcx>, attr: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    debug_assert!(is_compressed(attr));
    match toast_compress_method(attr) {
        TOAST_PGLZ_COMPRESSION_ID => pglz_decompress_datum(mcx, attr),
        TOAST_LZ4_COMPRESSION_ID => Err(no_lz4_support().into()),
        cmid => Err(invalid_compression_id(cmid).into()),
    }
}

/// C `toast_decompress_datum_slice`: decompress just the first `slicelength`
/// raw bytes (offset handling stays in [`detoast_attr_slice`]).
pub fn toast_decompress_datum_slice<'mcx>(
    mcx: Mcx<'mcx>,
    attr: &[u8],
    slicelength: i32,
) -> PgResult<PgVec<'mcx, u8>> {
    debug_assert!(is_compressed(attr));
    if slicelength as u32 >= toast_compress_extsize(attr) {
        return toast_decompress_datum(mcx, attr);
    }
    match toast_compress_method(attr) {
        TOAST_PGLZ_COMPRESSION_ID => pglz_decompress_datum_slice(mcx, attr, slicelength as usize),
        TOAST_LZ4_COMPRESSION_ID => Err(no_lz4_support().into()),
        cmid => Err(invalid_compression_id(cmid).into()),
    }
}

fn pglz_decompress_datum<'mcx>(mcx: Mcx<'mcx>, value: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let rawsize = toast_compress_extsize(value) as usize;
    let mut result = mcx::vec_with_capacity_in(mcx, VARHDRSZ + rawsize)?;
    mcx::vec_append_bytes(
        &mut result,
        &varatt::set_varsize_4b_word((VARHDRSZ + rawsize) as u32).to_ne_bytes(),
    )?;
    let src = &value[VARHDRSZ_COMPRESSED..varsize_4b(value)];
    let n = pglz::pglz_decompress(src, &mut result.spare_capacity_mut()[..rawsize], true)
        .ok_or_else(corrupt_pglz)?;
    // SAFETY: 4 header bytes appended + n decompressed bytes initialized.
    unsafe { result.set_len(VARHDRSZ + n) };
    Ok(result)
}

fn pglz_decompress_datum_slice<'mcx>(
    mcx: Mcx<'mcx>,
    value: &[u8],
    slicelength: usize,
) -> PgResult<PgVec<'mcx, u8>> {
    let mut result = mcx::vec_with_capacity_in(mcx, VARHDRSZ + slicelength)?;
    mcx::vec_append_bytes(&mut result, &[0u8; VARHDRSZ])?;
    let src = &value[VARHDRSZ_COMPRESSED..varsize_4b(value)];
    let n = pglz::pglz_decompress(src, &mut result.spare_capacity_mut()[..slicelength], false)
        .ok_or_else(corrupt_pglz)?;
    // SAFETY: 4 header bytes appended + n decompressed bytes initialized.
    unsafe { result.set_len(VARHDRSZ + n) };
    let header = varatt::set_varsize_4b_word((VARHDRSZ + n) as u32).to_ne_bytes();
    result[..VARHDRSZ].copy_from_slice(&header);
    Ok(result)
}

/// C `toast_raw_datum_size`: raw (detoasted) size including `VARHDRSZ`.
pub fn toast_raw_datum_size(value: &[u8]) -> usize {
    if is_external_ondisk(value) {
        VarattExternal::from_image(value).va_rawsize as usize
    } else if is_external_indirect(value) {
        // SAFETY: writer invariant (see indirect_target).
        let target = unsafe { indirect_target(value) };
        debug_assert!(!is_external_indirect(target));
        toast_raw_datum_size(target)
    } else if is_external_expanded(value) {
        // SAFETY: expanded image embeds a live header pointer (flatten_expanded).
        unsafe {
            datum::expandeddatum::eoh_get_flat_size(datum::expandeddatum::datum_get_eohp(
                datum::Datum::from_usize(value.as_ptr() as usize),
            ))
        }
    } else if is_compressed(value) {
        toast_compress_extsize(value) as usize + VARHDRSZ
    } else if is_short(value) {
        varsize_short(value) - VARHDRSZ_SHORT + VARHDRSZ
    } else {
        varsize_4b(value)
    }
}

/// C `toast_datum_size`: physical (possibly compressed) storage size.
pub fn toast_datum_size(value: &[u8]) -> usize {
    if is_external_ondisk(value) {
        VarattExternal::from_image(value).extsize() as usize
    } else if is_external_indirect(value) {
        // SAFETY: writer invariant (see indirect_target).
        let target = unsafe { indirect_target(value) };
        debug_assert!(!is_external_indirect(target));
        toast_datum_size(target)
    } else if is_external_expanded(value) {
        // SAFETY: expanded image embeds a live header pointer (flatten_expanded).
        unsafe {
            datum::expandeddatum::eoh_get_flat_size(datum::expandeddatum::datum_get_eohp(
                datum::Datum::from_usize(value.as_ptr() as usize),
            ))
        }
    } else if is_short(value) {
        varsize_short(value)
    } else {
        varsize_4b(value)
    }
}

/// C `VARATT_IS_EXTERNAL_ONDISK`.
pub fn varatt_is_external_ondisk(b: &[u8]) -> bool {
    is_external_ondisk(b)
}

/// C `toast_get_compression_id`: `None` for uncompressed/non-varlena-external.
pub fn toast_get_compression_id(attr: &[u8]) -> Option<u32> {
    if is_external_ondisk(attr) {
        let tp = VarattExternal::from_image(attr);
        tp.is_compressed().then(|| tp.compress_method())
    } else if is_compressed(attr) {
        Some(toast_compress_method(attr))
    } else {
        None
    }
}

pub fn init_seams() {
    detoast_seams::detoast_attr::set(detoast_attr);
    detoast_seams::detoast_attr_slice::set(detoast_attr_slice);
}
