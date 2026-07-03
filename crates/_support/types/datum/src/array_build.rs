use crate::datum::Datum;
use ::types_core::Oid;
use mcx::{slice_borrow_in, vec_with_capacity_in, Mcx, PgVec};
use types_error::PgResult;

pub const MAXDIM: usize = 6;
const INIT_ELEMS: usize = 64;

// C's ArrayBuildState private-subcontext model: element storage lives in the
// caller-owned child `mcx`, so teardown is that context's reset.
pub struct ArrayBuildState<'mcx> {
    pub mcx: Mcx<'mcx>,
    pub dvalues: PgVec<'mcx, Datum>,
    pub dnulls: PgVec<'mcx, bool>,
    pub nelems: i32,
    pub element_type: Oid,
    pub typlen: i16,
    pub typbyval: bool,
    pub typalign: u8,
    pub private_cxt: bool,
}

impl<'mcx> ArrayBuildState<'mcx> {
    pub fn new(mcx: Mcx<'mcx>, element_type: Oid, private_cxt: bool) -> PgResult<Self> {
        Ok(ArrayBuildState {
            mcx,
            dvalues: vec_with_capacity_in(mcx, INIT_ELEMS)?,
            dnulls: vec_with_capacity_in(mcx, INIT_ELEMS)?,
            nelems: 0,
            element_type,
            typlen: 0,
            typbyval: false,
            typalign: 0,
            private_cxt,
        })
    }

    // C: datumCopy into astate->mcontext; stable chunk addresses outlive the call.
    pub fn copy_byref(&self, bytes: &[u8]) -> PgResult<Datum> {
        let copy = slice_borrow_in(self.mcx, bytes)?;
        Ok(Datum::from_usize(copy.as_ptr() as usize))
    }
}

pub struct ArrayBuildStateArr<'mcx> {
    pub mcx: Mcx<'mcx>,
    pub data: PgVec<'mcx, u8>,
    pub nullbitmap: Option<PgVec<'mcx, u8>>,
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
    for &v in values {
        nbytes = (nbytes + align - 1) & !(align - 1);
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
        off = (off + align - 1) & !(align - 1);
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
    }
    debug_assert!(off == nbytes);
    Ok(out)
}

// deconstruct_array (arrayfuncs.c) over an in-memory 1-D no-nulls image;
// byref element datums point into `image`, which must outlive them.
pub fn deconstruct_array_image<'mcx>(
    mcx: Mcx<'mcx>,
    image: &[u8],
    elmlen: i16,
    elmbyval: bool,
    elmalign: u8,
) -> PgResult<PgVec<'mcx, Datum>> {
    let align = align_of_typalign(elmalign);
    let rd = |off: usize| i32::from_ne_bytes(image[off..off + 4].try_into().unwrap());
    assert!(
        image.len() >= ARR_1D_HDRSZ && rd(4) == 1,
        "deconstruct_array_image: not a 1-D array"
    );
    assert!(rd(8) == 0, "deconstruct_array_image: null bitmap present");
    let nelems = rd(16) as usize;
    let mut out: PgVec<'mcx, Datum> = vec_with_capacity_in(mcx, nelems)?;
    let mut off = ARR_1D_HDRSZ;
    for _ in 0..nelems {
        // att_align_pointer: a short-varlena header byte is never a pad byte.
        if !(elmlen == -1 && image[off] != 0) {
            off = (off + align - 1) & !(align - 1);
        }
        if elmbyval {
            let mut w = [0u8; 8];
            w[..elmlen as usize].copy_from_slice(&image[off..off + elmlen as usize]);
            out.push(Datum::from_u64(u64::from_ne_bytes(w)));
            off += elmlen as usize;
        } else if elmlen > 0 {
            out.push(Datum::from_usize(image[off..].as_ptr() as usize));
            off += elmlen as usize;
        } else if elmlen == -1 {
            out.push(Datum::from_usize(image[off..].as_ptr() as usize));
            off += varsize_any(image[off..].as_ptr());
        } else if elmlen == -2 {
            out.push(Datum::from_usize(image[off..].as_ptr() as usize));
            // SAFETY: cstring element is NUL-terminated within the image.
            off += unsafe { cstring_len(image[off..].as_ptr()) } + 1;
        } else {
            panic!("deconstruct_array_image: unsupported typlen {elmlen}");
        }
        assert!(off <= image.len());
    }
    Ok(out)
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
