use crate::datum::Datum;
use ::types_core::Oid;
use alloc::boxed::Box;
use mcx::{check_alloc_size, slice_borrow_in, vec_with_capacity_in, Mcx, PgVec};
use types_error::{PgError, PgResult, ERRCODE_DATA_CORRUPTED};

pub const MAXDIM: usize = 6;
const INIT_ELEMS: usize = 64;

// array.h: MaxArraySize == MaxAllocSize / sizeof(Datum) == 0x3fffffff / 8.
const MAX_ARRAY_SIZE: usize = 0x3fff_ffff / 8;

// C's ArrayBuildState private-subcontext model: element storage lives in the
// caller-owned child `mcx`, so teardown is that context's reset.
pub struct ArrayBuildState<'mcx> {
    pub mcx: Mcx<'mcx>,
    pub dvalues: PgVec<'mcx, Datum>,
    pub dnulls: PgVec<'mcx, bool>,
    // C alen: allocated slots; dvalues/dnulls capacity is always >= alen.
    pub alen: i32,
    pub nelems: i32,
    pub element_type: Oid,
    pub typlen: i16,
    pub typbyval: bool,
    pub typalign: u8,
    pub private_cxt: bool,
}

impl<'mcx> ArrayBuildState<'mcx> {
    pub fn new(mcx: Mcx<'mcx>, element_type: Oid, private_cxt: bool) -> PgResult<Self> {
        Self::with_size(mcx, element_type, private_cxt, INIT_ELEMS as i32)
    }

    pub fn with_size(
        mcx: Mcx<'mcx>,
        element_type: Oid,
        private_cxt: bool,
        initsize: i32,
    ) -> PgResult<Self> {
        Ok(ArrayBuildState {
            mcx,
            dvalues: vec_with_capacity_in(mcx, initsize as usize)?,
            dnulls: vec_with_capacity_in(mcx, initsize as usize)?,
            alen: initsize,
            nelems: 0,
            element_type,
            typlen: 0,
            typbyval: false,
            typalign: 0,
            private_cxt,
        })
    }

    pub fn grow(&mut self, alen: i32) -> PgResult<()> {
        let slots = alen as usize;
        let bytes = slots.saturating_mul(core::mem::size_of::<Datum>());
        check_alloc_size(bytes)?;
        self.dvalues
            .try_reserve_exact(slots.saturating_sub(self.dvalues.len()))
            .map_err(|_| self.mcx.oom(bytes))?;
        self.dnulls
            .try_reserve_exact(slots.saturating_sub(self.dnulls.len()))
            .map_err(|_| self.mcx.oom(slots))?;
        self.alen = alen;
        Ok(())
    }

    // C: datumCopy into astate->mcontext; stable chunk addresses outlive the call.
    pub fn copy_byref(&self, bytes: &[u8]) -> PgResult<Datum> {
        let copy = slice_borrow_in(self.mcx, bytes)?;
        Ok(Datum::from_usize(copy.as_ptr() as usize))
    }
}

// abytes/aitems mirror C's allocation-growth bookkeeping: both are wire
// fields of array_agg_array_serialize, so parity requires the C formulas
// even though PgVec manages the real capacity.
pub struct ArrayBuildStateArr<'mcx> {
    pub mcx: Mcx<'mcx>,
    pub data: PgVec<'mcx, u8>,
    pub nullbitmap: Option<PgVec<'mcx, u8>>,
    pub abytes: i32,
    pub aitems: i32,
    pub nbytes: i32,
    pub nitems: i32,
    pub ndims: i32,
    pub dims: [i32; MAXDIM],
    pub lbs: [i32; MAXDIM],
    pub array_type: Oid,
    pub element_type: Oid,
    pub private_cxt: bool,
}

// Exactly one sub-state is Some (the C scalarstate/arraystate pair).
pub struct ArrayBuildStateAny<'mcx> {
    pub scalarstate: Option<ArrayBuildState<'mcx>>,
    pub arraystate: Option<ArrayBuildStateArr<'mcx>>,
}

impl ArrayBuildStateArr<'_> {
    pub fn reserve_data(&mut self) -> PgResult<()> {
        let abytes = self.abytes as usize;
        check_alloc_size(abytes)?;
        self.data
            .try_reserve_exact(abytes.saturating_sub(self.data.len()))
            .map_err(|_| self.mcx.oom(abytes))?;
        Ok(())
    }
}

const ARR_1D_HDRSZ: usize = 24;

#[inline]
fn align_of_typalign(typalign: u8) -> usize {
    match typalign {
        b'c' => 1,
        b's' => 2,
        b'i' => 4,
        b'd' => 8,
        other => panic!("array image codec: unknown typalign {other}"),
    }
}

#[inline]
fn varsize_any(p: *const u8) -> usize {
    // SAFETY: caller guarantees p addresses a live varlena header.
    unsafe {
        let b0 = *p;
        if b0 & 0x01 != 0 {
            assert!(b0 != 0x01, "array varlena is an external toast pointer — detoast lane");
            (b0 as usize >> 1) & 0x7F
        } else {
            let w = u32::from_ne_bytes(core::slice::from_raw_parts(p, 4).try_into().unwrap());
            (w as usize) >> 2
        }
    }
}

/// # Safety
/// `p` must address a NUL-terminated byte string.
#[inline]
unsafe fn cstring_len(p: *const u8) -> usize {
    let mut n = 0;
    while *p.add(n) != 0 {
        n += 1;
    }
    n
}

// Fully safe varlena-size read that never reaches past `buf`. Returns the
// total on-disk length of the varlena whose header begins at buf[0], or None
// when the header (or the length it advertises) does not fit inside `buf`.
// External (on-disk TOAST) pointers are rejected: a raw catalog image walked
// here must be inline. This is the bounds-checked twin of `varsize_any`, used
// by `deconstruct_array_image` where `buf` is attacker-influenceable content.
#[inline]
fn varsize_any_bounded(buf: &[u8]) -> Option<usize> {
    let b0 = *buf.first()?;
    let total = if b0 & 0x01 != 0 {
        if b0 == 0x01 {
            // 1-byte external TOAST pointer: not a self-contained inline datum.
            return None;
        }
        (b0 as usize >> 1) & 0x7F
    } else {
        if buf.len() < 4 {
            return None;
        }
        let w = u32::from_ne_bytes(buf[..4].try_into().unwrap());
        (w as usize) >> 2
    };
    // A varlena's advertised length includes its own header, so it must be at
    // least 1 (short) and cannot exceed the bytes actually present.
    if total == 0 || total > buf.len() {
        None
    } else {
        Some(total)
    }
}

// Bounds-checked cstring scan: length of the NUL-terminated string at buf[0],
// excluding the terminator, or None when no NUL exists within `buf`.
#[inline]
fn cstring_len_bounded(buf: &[u8]) -> Option<usize> {
    buf.iter().position(|&b| b == 0)
}

// buildint2vector/buildoidvector (int.c/oid.c): fixed-len by-val elements,
// lbound 0 (the int2vector/oidvector on-disk shape; arrays use lbound 1).
pub fn construct_vector_image<'mcx>(
    mcx: Mcx<'mcx>,
    values: &[Datum],
    elmtype: Oid,
    elmlen: i16,
    elmalign: u8,
) -> PgResult<PgVec<'mcx, u8>> {
    let mut out = construct_array_image(mcx, values, elmtype, elmlen, true, elmalign)?;
    out[20..24].copy_from_slice(&0i32.to_ne_bytes());
    Ok(out)
}

// construct_empty_array (arrayfuncs.c): 16-byte zero-dimensional image.
pub fn construct_empty_array_image<'mcx>(
    mcx: Mcx<'mcx>,
    elmtype: Oid,
) -> PgResult<PgVec<'mcx, u8>> {
    let mut out: PgVec<'mcx, u8> = vec_with_capacity_in(mcx, 16)?;
    out.resize(16, 0);
    out[0..4].copy_from_slice(&((16i32) << 2).to_ne_bytes());
    out[12..16].copy_from_slice(&(elmtype as i32).to_ne_bytes());
    Ok(out)
}

// construct_array (arrayfuncs.c) restricted to 1-D no-nulls; short varlena
// inputs are canonicalized to 4-byte headers so element alignment holds.
pub fn construct_array_image<'mcx>(
    mcx: Mcx<'mcx>,
    values: &[Datum],
    elmtype: Oid,
    elmlen: i16,
    elmbyval: bool,
    elmalign: u8,
) -> PgResult<PgVec<'mcx, u8>> {
    let align = align_of_typalign(elmalign);
    let mut nbytes = ARR_1D_HDRSZ;
    // C pads AFTER each element (att_align_nominal in construct_md_array), so
    // the last odd-length element carries trailing pad in the stored image.
    for &v in values {
        nbytes += if elmlen > 0 {
            elmlen as usize
        } else if elmlen == -1 {
            let p = v.as_usize() as *const u8;
            let raw = varsize_any(p);
            // SAFETY: v is a live varlena datum.
            if unsafe { *p } & 0x01 != 0 { raw - 1 + crate::VARHDRSZ } else { raw }
        } else if elmlen == -2 {
            // SAFETY: v is a live NUL-terminated cstring datum.
            unsafe { cstring_len(v.as_usize() as *const u8) + 1 }
        } else {
            panic!("construct_array_image: unsupported typlen {elmlen}")
        };
        nbytes = (nbytes + align - 1) & !(align - 1);
    }
    let mut out: PgVec<'mcx, u8> = vec_with_capacity_in(mcx, nbytes)?;
    out.resize(nbytes, 0);
    let w = |out: &mut [u8], off: usize, v: i32| {
        out[off..off + 4].copy_from_slice(&v.to_ne_bytes());
    };
    w(&mut out, 0, (nbytes as i32) << 2);
    w(&mut out, 4, 1);
    w(&mut out, 8, 0);
    w(&mut out, 12, elmtype as i32);
    w(&mut out, 16, values.len() as i32);
    w(&mut out, 20, 1);
    let mut off = ARR_1D_HDRSZ;
    for &v in values {
        if elmbyval {
            let bytes = v.as_u64().to_ne_bytes();
            out[off..off + elmlen as usize].copy_from_slice(&bytes[..elmlen as usize]);
            off += elmlen as usize;
        } else if elmlen > 0 {
            let p = v.as_usize() as *const u8;
            // SAFETY: byref fixed-len datum points at elmlen live bytes.
            let src = unsafe { core::slice::from_raw_parts(p, elmlen as usize) };
            out[off..off + elmlen as usize].copy_from_slice(src);
            off += elmlen as usize;
        } else if elmlen == -2 {
            let p = v.as_usize() as *const u8;
            // SAFETY: v is a live NUL-terminated cstring datum.
            unsafe {
                let n = cstring_len(p) + 1;
                let src = core::slice::from_raw_parts(p, n);
                out[off..off + n].copy_from_slice(src);
                off += n;
            }
        } else {
            let p = v.as_usize() as *const u8;
            // SAFETY: v is a live varlena datum.
            unsafe {
                if *p & 0x01 != 0 {
                    let raw = varsize_any(p);
                    let data = core::slice::from_raw_parts(p.add(1), raw - 1);
                    let total = raw - 1 + crate::VARHDRSZ;
                    out[off..off + 4].copy_from_slice(&((total as u32) << 2).to_ne_bytes());
                    out[off + 4..off + total].copy_from_slice(data);
                    off += total;
                } else {
                    let raw = varsize_any(p);
                    let src = core::slice::from_raw_parts(p, raw);
                    out[off..off + raw].copy_from_slice(src);
                    off += raw;
                }
            }
        }
        off = (off + align - 1) & !(align - 1);
    }
    debug_assert!(off == nbytes);
    Ok(out)
}

// Kill knob (lane law): PGRUST_ARRAY_FETCH_INLINE=0|off restores the
// incumbent runtime-length copy arm of the byval element fetch. Env read
// once per process (OnceLock-cached); checked once per deconstruct call —
// never per element.
#[cfg(not(target_family = "wasm"))]
fn array_fetch_inline_enabled() -> bool {
    extern crate std;
    static KILLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    !*KILLED.get_or_init(|| {
        matches!(
            std::env::var("PGRUST_ARRAY_FETCH_INLINE").as_deref(),
            Ok("0") | Ok("off")
        )
    })
}
#[cfg(target_family = "wasm")]
fn array_fetch_inline_enabled() -> bool {
    true // no env surface on wasm; the default arm stands
}

// C fetch_att's inline width switch (postgres.h): fixed-width loads instead
// of the runtime-length copy_from_slice, which rustc lowers to a libc memcpy
// CALL per element. Both arms produce byte-identical Datum values (the copy
// arm zero-pads w to 8 bytes; the load arm zero-extends the same low-address
// bytes) — result parity by construction on little-endian targets.
#[inline(always)]
fn fetch_byval_datum(image: &[u8], off: usize, elmlen: i16, inline_fetch: bool) -> Datum {
    if inline_fetch {
        let b = &image[off..off + elmlen as usize];
        // SAFETY (each arm): `b` spans exactly `elmlen` in-bounds bytes and
        // read_unaligned tolerates any alignment.
        match elmlen {
            1 => Datum::from_u64(b[0] as u64),
            2 => Datum::from_u64(
                unsafe { core::ptr::read_unaligned(b.as_ptr() as *const u16) } as u64,
            ),
            4 => Datum::from_u64(
                unsafe { core::ptr::read_unaligned(b.as_ptr() as *const u32) } as u64,
            ),
            8 => Datum::from_u64(unsafe { core::ptr::read_unaligned(b.as_ptr() as *const u64) }),
            _ => panic!("deconstruct_array_image: unsupported byval typlen {elmlen}"),
        }
    } else {
        // Incumbent arm (kill-knob restore): runtime-length copy.
        let mut w = [0u8; 8];
        w[..elmlen as usize].copy_from_slice(&image[off..off + elmlen as usize]);
        Datum::from_u64(u64::from_ne_bytes(w))
    }
}

// deconstruct_array (arrayfuncs.c) over an in-memory no-nulls image of any
// dimensionality (elements walked linearly, as C does); byref element datums
// point into `image`, which must outlive them.
pub fn deconstruct_array_image<'mcx>(
    mcx: Mcx<'mcx>,
    image: &[u8],
    elmlen: i16,
    elmbyval: bool,
    elmalign: u8,
) -> PgResult<PgVec<'mcx, Datum>> {
    Ok(deconstruct_1d_image(mcx, image, elmlen, elmbyval, elmalign, false)?.0)
}

// upstream 83671c0da049 (18.4): Fix set of issues with extended statistics on expressions
// deconstruct_array over a 1-D image that may carry a null bitmap: the nulls
// vector is Some exactly when the image has one, and a NULL element yields a
// zero Datum without consuming payload bytes (C's dvalues/dnulls pair).
pub fn deconstruct_array_image_nulls<'mcx>(
    mcx: Mcx<'mcx>,
    image: &[u8],
    elmlen: i16,
    elmbyval: bool,
    elmalign: u8,
) -> PgResult<(PgVec<'mcx, Datum>, Option<PgVec<'mcx, bool>>)> {
    deconstruct_1d_image(mcx, image, elmlen, elmbyval, elmalign, true)
}

fn deconstruct_1d_image<'mcx>(
    mcx: Mcx<'mcx>,
    image: &[u8],
    elmlen: i16,
    elmbyval: bool,
    elmalign: u8,
    allow_nulls: bool,
) -> PgResult<(PgVec<'mcx, Datum>, Option<PgVec<'mcx, bool>>)> {
    let align = align_of_typalign(elmalign);
    // Catchable error for any content-driven inconsistency in the image. C's
    // callers reach deconstruct_array only after array_recv/ArrayGetNItems have
    // validated the header; here the image bytes may come verbatim from an
    // on-disk catalog page, so every read below is bounded against the slice
    // and a violation returns rather than reading out of bounds.
    let corrupt = || -> Box<PgError> {
        Box::new(PgError::error("deconstruct_array_image: malformed array image").with_sqlstate(ERRCODE_DATA_CORRUPTED))
    };
    // Bounds-checked 4-byte header read.
    let rd = |off: usize| -> PgResult<i32> {
        if off + 4 > image.len() {
            return Err(corrupt());
        }
        Ok(i32::from_ne_bytes(image[off..off + 4].try_into().unwrap()))
    };
    if image.len() >= 16 && rd(4)? == 0 {
        // construct_empty_array's zero-dimensional image: no elements.
        return Ok((PgVec::new_in(mcx), None));
    }
    // Any dimensionality up to MAXDIM, as deconstruct_array walks the
    // elements linearly whatever the shape (arrayfuncs.c); a truncated header
    // or an out-of-range ndim is treated as corrupt rather than trusted.
    let ndim = rd(4)?;
    if ndim < 1 || ndim as usize > MAXDIM {
        return Err(corrupt());
    }
    let ndim = ndim as usize;
    // ARR_OVERHEAD_NONULLS(ndim): vl_len + ndim + dataoffset + elemtype, then
    // the dims and lower bounds (16 + 8*ndim, already MAXALIGNed).
    let hdrsz = 16 + 8 * ndim;
    if image.len() < hdrsz {
        return Err(corrupt());
    }
    // nelems is content-controlled: ArrayGetNItems rejects a negative dim and
    // caps the product at MaxArraySize before palloc.
    let mut nelems: usize = 1;
    for i in 0..ndim {
        let dim = rd(16 + 4 * i)?;
        if dim < 0 {
            return Err(corrupt());
        }
        nelems = match nelems.checked_mul(dim as usize) {
            Some(n) if n <= MAX_ARRAY_SIZE => n,
            _ => return Err(corrupt()),
        };
    }
    // dataoffset != 0: a null bitmap follows the header and the payload starts
    // at ARR_OVERHEAD_WITHNULLS(ndim, nelems) (MAXALIGN, 8). Only callers that
    // can represent NULL elements accept that shape.
    let dataoffset = rd(8)?;
    let mut off = hdrsz;
    let mut bitmap: Option<&[u8]> = None;
    if dataoffset != 0 {
        let bitmap_bytes = (nelems + 7) / 8;
        let expected = (hdrsz + bitmap_bytes + 7) & !7;
        if !allow_nulls
            || dataoffset < 0
            || dataoffset as usize != expected
            || expected > image.len()
        {
            return Err(corrupt());
        }
        bitmap = Some(&image[hdrsz..hdrsz + bitmap_bytes]);
        off = expected;
    }
    let inline_fetch = elmbyval && array_fetch_inline_enabled();
    // Every element occupies at least one byte, so a valid count can never
    // exceed the remaining bytes; cap the capacity hint so a bogus (but
    // in-range) nelems cannot force a huge up-front allocation.
    let cap = core::cmp::min(nelems, image.len());
    let mut out: PgVec<'mcx, Datum> = vec_with_capacity_in(mcx, cap)?;
    let mut nulls: Option<PgVec<'mcx, bool>> = match bitmap {
        Some(_) => Some(vec_with_capacity_in(mcx, cap)?),
        None => None,
    };
    for i in 0..nelems {
        if let Some(bm) = bitmap {
            // A set bit marks a present value (arrayfuncs.c bitmask walk).
            let is_null = bm[i / 8] & (1 << (i % 8)) == 0;
            nulls.as_mut().expect("nulls tracks the bitmap").push(is_null);
            if is_null {
                out.push(Datum::null());
                continue;
            }
        }
        // att_align_pointer: a short-varlena header byte is never a pad byte.
        // The header byte read must itself be in bounds.
        let is_short_varlena = elmlen == -1 && *image.get(off).ok_or_else(corrupt)? != 0;
        if !is_short_varlena {
            off = (off + align - 1) & !(align - 1);
        }
        if elmbyval {
            let n = elmlen as usize;
            if elmlen <= 0 || off + n > image.len() {
                return Err(corrupt());
            }
            out.push(fetch_byval_datum(image, off, elmlen, inline_fetch));
            off += n;
        } else if elmlen > 0 {
            let n = elmlen as usize;
            if off + n > image.len() {
                return Err(corrupt());
            }
            out.push(Datum::from_usize(image[off..].as_ptr() as usize));
            off += n;
        } else if elmlen == -1 {
            if off > image.len() {
                return Err(corrupt());
            }
            // Bounds-checked varlena size: the whole element must fit.
            let vsize = varsize_any_bounded(&image[off..]).ok_or_else(corrupt)?;
            out.push(Datum::from_usize(image[off..].as_ptr() as usize));
            off += vsize;
        } else if elmlen == -2 {
            if off > image.len() {
                return Err(corrupt());
            }
            // Bounds-checked NUL scan: a missing terminator is corruption, not
            // a walk into adjacent memory.
            let clen = cstring_len_bounded(&image[off..]).ok_or_else(corrupt)?;
            out.push(Datum::from_usize(image[off..].as_ptr() as usize));
            off += clen + 1;
        } else {
            return Err(corrupt());
        }
        // Redundant given the per-branch checks, but pins the invariant.
        if off > image.len() {
            return Err(corrupt());
        }
    }
    Ok((out, nulls))
}

pub fn array_image_elemtype(image: &[u8]) -> Oid {
    i32::from_ne_bytes(image[12..16].try_into().unwrap()) as Oid
}

pub fn array_image_nelems(image: &[u8]) -> usize {
    if image.len() < ARR_1D_HDRSZ {
        return 0;
    }
    i32::from_ne_bytes(image[16..20].try_into().unwrap()) as usize
}

#[cfg(test)]
mod tests {
    use super::*;
    use mcx::MemoryContext;

    #[test]
    fn array_image_roundtrip_int4() {
        let ctx = MemoryContext::new_bump("arr-int4");
        let mcx = ctx.mcx();
        let vals: [Datum; 3] = [Datum::from_i32(1), Datum::from_i32(-7), Datum::from_i32(500)];
        let img = construct_array_image(mcx, &vals, 23, 4, true, b'i').unwrap();
        assert_eq!(img.len(), 24 + 12);
        assert_eq!(array_image_elemtype(&img), 23);
        assert_eq!(array_image_nelems(&img), 3);
        let out = deconstruct_array_image(mcx, &img, 4, true, b'i').unwrap();
        let got: [i32; 3] = [out[0].as_i32(), out[1].as_i32(), out[2].as_i32()];
        assert_eq!(got, [1, -7, 500]);
    }

    // deconstruct_array walks a multi-dimensional image linearly
    // (arrayfuncs.c: nitems = ArrayGetNItems(ndim, dims)); a catalog text[]
    // such as reloptions may legitimately be stored 2-D.
    #[test]
    fn deconstruct_array_image_walks_two_dimensions() {
        let ctx = MemoryContext::new_bump("arr-2d");
        let mcx = ctx.mcx();
        let mut img: alloc::vec::Vec<u8> = alloc::vec::Vec::new();
        img.extend_from_slice(&0u32.to_ne_bytes()); // vl_len, patched below
        img.extend_from_slice(&2i32.to_ne_bytes()); // ndim
        img.extend_from_slice(&0i32.to_ne_bytes()); // dataoffset (no nulls)
        img.extend_from_slice(&23i32.to_ne_bytes()); // elemtype = INT4OID
        img.extend_from_slice(&2i32.to_ne_bytes()); // dims[0]
        img.extend_from_slice(&2i32.to_ne_bytes()); // dims[1]
        img.extend_from_slice(&1i32.to_ne_bytes()); // lbs[0]
        img.extend_from_slice(&1i32.to_ne_bytes()); // lbs[1]
        for v in [10i32, 20, 30, 40] {
            img.extend_from_slice(&v.to_ne_bytes());
        }
        let len = img.len() as u32;
        img[..4].copy_from_slice(&(len << 2).to_ne_bytes());
        let out = deconstruct_array_image(mcx, &img, 4, true, b'i').unwrap();
        let got: alloc::vec::Vec<i32> = out.iter().map(|d| d.as_i32()).collect();
        assert_eq!(got, [10, 20, 30, 40]);
        // A 2-D image with a null bitmap (dataoffset = MAXALIGN(32 + 1)).
        let mut nimg: alloc::vec::Vec<u8> = alloc::vec::Vec::new();
        nimg.extend_from_slice(&0u32.to_ne_bytes());
        nimg.extend_from_slice(&2i32.to_ne_bytes());
        nimg.extend_from_slice(&40i32.to_ne_bytes()); // dataoffset
        nimg.extend_from_slice(&23i32.to_ne_bytes());
        nimg.extend_from_slice(&2i32.to_ne_bytes());
        nimg.extend_from_slice(&2i32.to_ne_bytes());
        nimg.extend_from_slice(&1i32.to_ne_bytes());
        nimg.extend_from_slice(&1i32.to_ne_bytes());
        nimg.push(0b1011); // element 2 is NULL
        while nimg.len() < 40 {
            nimg.push(0);
        }
        for v in [10i32, 20, 40] {
            nimg.extend_from_slice(&v.to_ne_bytes());
        }
        let len = nimg.len() as u32;
        nimg[..4].copy_from_slice(&(len << 2).to_ne_bytes());
        let (out, nulls) = deconstruct_array_image_nulls(mcx, &nimg, 4, true, b'i').unwrap();
        let nulls = nulls.expect("bitmap present");
        let nulls: alloc::vec::Vec<bool> = nulls.iter().copied().collect();
        assert_eq!(nulls, [false, false, true, false]);
        assert_eq!([out[0].as_i32(), out[1].as_i32(), out[3].as_i32()], [10, 20, 40]);
        // The no-nulls entry point refuses the bitmap shape.
        assert!(deconstruct_array_image(mcx, &nimg, 4, true, b'i').is_err());
    }

    #[test]
    fn array_image_roundtrip_float4() {
        let ctx = MemoryContext::new_bump("arr-f4");
        let mcx = ctx.mcx();
        let vals: [Datum; 2] = [Datum::from_f32(0.5), Datum::from_f32(-1.25)];
        let img = construct_array_image(mcx, &vals, 700, 4, true, b'i').unwrap();
        let out = deconstruct_array_image(mcx, &img, 4, true, b'i').unwrap();
        assert_eq!(out[0].as_f32(), 0.5);
        assert_eq!(out[1].as_f32(), -1.25);
    }

    #[test]
    fn array_image_roundtrip_varlena_mixed_headers() {
        let ctx = MemoryContext::new_bump("arr-text");
        let mcx = ctx.mcx();
        // One short-header (1-byte) and one 4-byte-header varlena input.
        let short: [u8; 4] = [(4 << 1) | 1, b'a', b'b', b'c'];
        let mut long = [0u8; 9];
        long[..4].copy_from_slice(&((4u32 + 5) << 2).to_ne_bytes());
        long[4..].copy_from_slice(b"hello");
        let vals = [
            Datum::from_usize(short.as_ptr() as usize),
            Datum::from_usize(long.as_ptr() as usize),
        ];
        let img = construct_array_image(mcx, &vals, 25, -1, false, b'i').unwrap();
        assert_eq!(array_image_nelems(&img), 2);
        let out = deconstruct_array_image(mcx, &img, -1, false, b'i').unwrap();
        let read = |d: Datum| {
            let p = d.as_usize() as *const u8;
            let w = unsafe { u32::from_ne_bytes(*(p as *const [u8; 4])) };
            let len = (w >> 2) as usize;
            unsafe { core::slice::from_raw_parts(p.add(4), len - 4) }
        };
        assert_eq!(read(out[0]), b"abc");
        assert_eq!(read(out[1]), b"hello");
    }

    #[test]
    #[should_panic(expected = "external toast pointer")]
    fn construct_array_rejects_external_input() {
        let ctx = MemoryContext::new_bump("arr-ext");
        let mut ext = [0u8; 18];
        ext[0] = 0x01;
        ext[1] = 18;
        let vals = [Datum::from_usize(ext.as_ptr() as usize)];
        let _ = construct_array_image(ctx.mcx(), &vals, 25, -1, false, b'i');
    }

    // Differential old-vs-new-arm witness for the inline byval fetch: both
    // arms of fetch_byval_datum must produce byte-identical Datum values for
    // every supported width, at every offset (aligned and unaligned).
    #[test]
    fn byval_fetch_arms_are_byte_identical() {
        let mut buf = [0u8; 64];
        for (i, b) in buf.iter_mut().enumerate() {
            // Sign-bit-rich pattern: high bits set on odd bytes, zeros mixed in.
            *b = match i % 4 {
                0 => 0x00,
                1 => 0xFF,
                2 => (i as u8).wrapping_mul(37),
                _ => 0x80,
            };
        }
        for elmlen in [1i16, 2, 4, 8] {
            for off in 0..(buf.len() - elmlen as usize) {
                let new_arm = fetch_byval_datum(&buf, off, elmlen, true);
                let old_arm = fetch_byval_datum(&buf, off, elmlen, false);
                assert_eq!(
                    new_arm.as_u64(),
                    old_arm.as_u64(),
                    "arm divergence at elmlen {elmlen} off {off}"
                );
            }
        }
    }

    #[test]
    #[should_panic(expected = "unsupported byval typlen")]
    fn byval_inline_fetch_rejects_unsupported_width() {
        let buf = [0u8; 8];
        let _ = fetch_byval_datum(&buf, 0, 3, true);
    }

    #[test]
    fn array_image_roundtrip_int2_int8_char_widths() {
        let ctx = MemoryContext::new_bump("arr-widths");
        let mcx = ctx.mcx();
        // width 2 (int2, align 's')
        let v2: [Datum; 3] =
            [Datum::from_i16(-1), Datum::from_i16(0x7F0F), Datum::from_i16(i16::MIN)];
        let img = construct_array_image(mcx, &v2, 21, 2, true, b's').unwrap();
        let out = deconstruct_array_image(mcx, &img, 2, true, b's').unwrap();
        assert_eq!(
            [out[0].as_i16(), out[1].as_i16(), out[2].as_i16()],
            [-1, 0x7F0F, i16::MIN]
        );
        // width 8 (int8, align 'd')
        let v8: [Datum; 3] = [
            Datum::from_u64(u64::MAX),
            Datum::from_u64(0x8000_0000_0000_0001),
            Datum::from_u64(42),
        ];
        let img = construct_array_image(mcx, &v8, 20, 8, true, b'd').unwrap();
        let out = deconstruct_array_image(mcx, &img, 8, true, b'd').unwrap();
        assert_eq!(
            [out[0].as_u64(), out[1].as_u64(), out[2].as_u64()],
            [u64::MAX, 0x8000_0000_0000_0001, 42]
        );
        // width 1 ("char", align 'c')
        let v1: [Datum; 2] = [Datum::from_u8(0xFF), Datum::from_u8(7)];
        let img = construct_array_image(mcx, &v1, 18, 1, true, b'c').unwrap();
        let out = deconstruct_array_image(mcx, &img, 1, true, b'c').unwrap();
        assert_eq!([out[0].as_u8(), out[1].as_u8()], [0xFF, 7]);
    }

    // A safe fn must never read past `image` for ANY input. These craft the
    // malformed shapes from the finding (idx 167) and require a catchable error
    // instead of an out-of-bounds read.
    #[test]
    fn deconstruct_rejects_malformed_images() {
        let ctx = MemoryContext::new_bump("arr-oob");
        let mcx = ctx.mcx();

        // Helper: build a valid 1-D header, then let the caller mangle it.
        let header = |ndim: i32, dataoffset: i32, nelems: i32| -> [u8; ARR_1D_HDRSZ] {
            let mut h = [0u8; ARR_1D_HDRSZ];
            h[0..4].copy_from_slice(&((ARR_1D_HDRSZ as i32) << 2).to_ne_bytes());
            h[4..8].copy_from_slice(&ndim.to_ne_bytes());
            h[8..12].copy_from_slice(&dataoffset.to_ne_bytes());
            h[12..16].copy_from_slice(&23i32.to_ne_bytes());
            h[16..20].copy_from_slice(&nelems.to_ne_bytes());
            h[20..24].copy_from_slice(&1i32.to_ne_bytes());
            h
        };

        // (a) Truncated header (fewer than ARR_1D_HDRSZ bytes).
        let _ = deconstruct_array_image(mcx, &[0u8; 10], 4, true, b'i')
            .err()
            .unwrap();

        // (b) Negative nelems must not become a huge usize.
        let h = header(1, 0, -1);
        let _ = deconstruct_array_image(mcx, &h, 4, true, b'i').err().unwrap();

        // (c) nelems beyond MaxArraySize.
        let h = header(1, 0, i32::MAX);
        let _ = deconstruct_array_image(mcx, &h, 4, true, b'i').err().unwrap();

        // (d) Byval element count that walks off the end (nelems=2 but only one
        // int4 of payload present).
        let mut img = header(1, 0, 2).to_vec();
        img.extend_from_slice(&7i32.to_ne_bytes());
        let _ = deconstruct_array_image(mcx, &img, 4, true, b'i')
            .err()
            .unwrap();

        // (e) Varlena element whose 4-byte header advertises a length running
        // past the image end (elmlen == -1).
        let mut img = header(1, 0, 1).to_vec();
        img.extend_from_slice(&(((100u32) << 2)).to_ne_bytes()); // claims 100 bytes
        let _ = deconstruct_array_image(mcx, &img, -1, false, b'i')
            .err()
            .unwrap();

        // (f) cstring element with no NUL terminator (elmlen == -2).
        let mut img = header(1, 0, 1).to_vec();
        img.extend_from_slice(b"no terminator here");
        let _ = deconstruct_array_image(mcx, &img, -2, false, b'c')
            .err()
            .unwrap();

        // (g) Null bitmap present (dataoffset != 0): unsupported, not trusted.
        let h = header(1, 32, 1);
        let _ = deconstruct_array_image(mcx, &h, 4, true, b'i').err().unwrap();

        // Valid empty (zero-dim) image still deconstructs to no elements.
        let empty = construct_empty_array_image(mcx, 23).unwrap();
        let out = deconstruct_array_image(mcx, &empty, 4, true, b'i').unwrap();
        assert_eq!(out.len(), 0);
    }

    // upstream 83671c0da049 (18.4): a 1-D int4 image {1, NULL, 3} with its
    // null bitmap (dataoffset = MAXALIGN(24 + 1) = 32) decodes to a
    // dvalues/dnulls pair, while the no-nulls codec keeps rejecting it.
    #[test]
    fn deconstruct_with_null_bitmap() {
        let ctx = MemoryContext::new_bump("arr-nulls");
        let mcx = ctx.mcx();
        let mut img = [0u8; 40];
        img[0..4].copy_from_slice(&(40i32 << 2).to_ne_bytes());
        img[4..8].copy_from_slice(&1i32.to_ne_bytes());
        img[8..12].copy_from_slice(&32i32.to_ne_bytes());
        img[12..16].copy_from_slice(&23i32.to_ne_bytes());
        img[16..20].copy_from_slice(&3i32.to_ne_bytes());
        img[20..24].copy_from_slice(&1i32.to_ne_bytes());
        img[24] = 0b101;
        img[32..36].copy_from_slice(&1i32.to_ne_bytes());
        img[36..40].copy_from_slice(&3i32.to_ne_bytes());
        let (vals, nulls) = deconstruct_array_image_nulls(mcx, &img, 4, true, b'i').unwrap();
        let nulls = nulls.expect("bitmap present");
        assert_eq!((vals.len(), nulls.len()), (3, 3));
        assert_eq!([nulls[0], nulls[1], nulls[2]], [false, true, false]);
        assert_eq!([vals[0].as_i32(), vals[2].as_i32()], [1, 3]);
        assert!(deconstruct_array_image(mcx, &img, 4, true, b'i').is_err());
        // A dataoffset that does not match the bitmap size is corrupt, not trusted.
        img[8..12].copy_from_slice(&40i32.to_ne_bytes());
        assert!(deconstruct_array_image_nulls(mcx, &img, 4, true, b'i').is_err());
        // No bitmap: nulls is None and the payload starts right after the header.
        let plain = construct_array_image(mcx, &[Datum::from_i32(9)], 23, 4, true, b'i').unwrap();
        let (vals, nulls) = deconstruct_array_image_nulls(mcx, &plain, 4, true, b'i').unwrap();
        assert!(nulls.is_none());
        assert_eq!(vals[0].as_i32(), 9);
    }

    #[test]
    fn build_state_accum_in_context() {
        let ctx = MemoryContext::new_bump("array-build-test");
        let mut st = ArrayBuildState::new(ctx.mcx(), 23, true).unwrap();
        st.dvalues.push(Datum::from_i32(7));
        st.dnulls.push(false);
        st.nelems = 1;
        let d = st.copy_byref(b"payload").unwrap();
        let p = d.as_usize() as *const u8;
        let copied = unsafe { core::slice::from_raw_parts(p, 7) };
        assert_eq!(copied, b"payload");
        assert_eq!(st.dvalues.len(), 1);
    }
}
