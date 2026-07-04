use ::datum::Datum;
use ::types_core::Oid;

pub const MAXDIM: usize = 6;
pub const ARRAYTYPE_HDRSZ: usize = 16;
pub const MAX_ALLOC_SIZE: usize = 0x3FFF_FFFF;

pub const TYPALIGN_CHAR: u8 = b'c';
pub const TYPALIGN_SHORT: u8 = b's';
pub const TYPALIGN_INT: u8 = b'i';
pub const TYPALIGN_DOUBLE: u8 = b'd';

#[inline]
const fn typealign(a: usize, x: usize) -> usize {
    (x + a - 1) & !(a - 1)
}

#[inline]
pub const fn maxalign(x: usize) -> usize {
    typealign(8, x)
}

// att_align_nominal: arrays unconditionally align varlenas by elmalign.
#[inline]
pub fn att_align_nominal(cur: usize, attalign: u8) -> usize {
    match attalign {
        TYPALIGN_INT => typealign(4, cur),
        TYPALIGN_CHAR => cur,
        TYPALIGN_DOUBLE => typealign(8, cur),
        TYPALIGN_SHORT => typealign(2, cur),
        other => panic!("att_align_nominal: unknown typalign {other}"),
    }
}

#[inline]
fn rd_i32(a: &[u8], off: usize) -> i32 {
    i32::from_ne_bytes(a[off..off + 4].try_into().unwrap())
}

// VARSIZE over a 4-byte varlena header (arrays are always 4B-header varlenas).
#[inline]
pub fn arr_size(a: &[u8]) -> usize {
    (u32::from_ne_bytes(a[0..4].try_into().unwrap()) >> 2) as usize
}

#[inline]
pub fn arr_ndim(a: &[u8]) -> i32 {
    rd_i32(a, 4)
}

#[inline]
pub fn arr_dataoffset_field(a: &[u8]) -> i32 {
    rd_i32(a, 8)
}

#[inline]
pub fn arr_hasnull(a: &[u8]) -> bool {
    arr_dataoffset_field(a) != 0
}

#[inline]
pub fn arr_elemtype(a: &[u8]) -> Oid {
    u32::from_ne_bytes(a[12..16].try_into().unwrap()) as Oid
}

#[inline]
pub fn arr_dim(a: &[u8], i: usize) -> i32 {
    rd_i32(a, ARRAYTYPE_HDRSZ + 4 * i)
}

#[inline]
pub fn arr_lbound(a: &[u8], i: usize) -> i32 {
    let ndim = arr_ndim(a) as usize;
    rd_i32(a, ARRAYTYPE_HDRSZ + 4 * ndim + 4 * i)
}

// Read ndim + dims[] + lbound[] into stack arrays.
// inline(always): outlined, the 52-byte tuple returns via an sret stack buffer
// (store-to-load forwarding stall on Neoverse V2 — bench-crate §3b).
#[inline(always)]
pub fn read_dims_lbounds(a: &[u8]) -> (i32, [i32; MAXDIM], [i32; MAXDIM]) {
    let ndim = arr_ndim(a);
    let mut dims = [0i32; MAXDIM];
    let mut lbs = [0i32; MAXDIM];
    for i in 0..ndim as usize {
        dims[i] = arr_dim(a, i);
        lbs[i] = arr_lbound(a, i);
    }
    (ndim, dims, lbs)
}

#[inline]
pub fn arr_overhead_nonulls(ndims: i32) -> usize {
    maxalign(ARRAYTYPE_HDRSZ + 2 * 4 * ndims as usize)
}

#[inline]
pub fn arr_overhead_withnulls(ndims: i32, nitems: i32) -> usize {
    maxalign(ARRAYTYPE_HDRSZ + 2 * 4 * ndims as usize + (nitems as usize + 7) / 8)
}

#[inline]
pub fn arr_data_offset(a: &[u8]) -> usize {
    if arr_hasnull(a) {
        arr_dataoffset_field(a) as usize
    } else {
        arr_overhead_nonulls(arr_ndim(a))
    }
}

// Offset of the null bitmap within the image, or None.
#[inline]
pub fn arr_nullbitmap_off(a: &[u8]) -> Option<usize> {
    if arr_hasnull(a) {
        Some(ARRAYTYPE_HDRSZ + 2 * 4 * arr_ndim(a) as usize)
    } else {
        None
    }
}

// VARSIZE_ANY over a possibly-short varlena pointed to by p.
#[inline]
pub fn varsize_any(p: *const u8) -> usize {
    // SAFETY: p addresses a live varlena header.
    unsafe {
        let b0 = *p;
        if b0 == 0x01 {
            // 1B_E: 2-byte header + tag-determined body (C VARSIZE_EXTERNAL).
            2 + match *p.add(1) {
                1 => 8,          // VARTAG_INDIRECT
                2 | 3 => 8,      // VARTAG_EXPANDED_RO/RW
                18 => 16,        // VARTAG_ONDISK
                other => panic!("unrecognized TOAST vartag {other}"),
            }
        } else if b0 & 0x01 != 0 {
            (b0 as usize >> 1) & 0x7F
        } else {
            let w = u32::from_ne_bytes(core::slice::from_raw_parts(p, 4).try_into().unwrap());
            (w >> 2) as usize
        }
    }
}

// C strlen for a cstring (typlen -2) element.
#[inline]
fn cstr_len(p: *const u8) -> usize {
    let mut n = 0usize;
    // SAFETY: p addresses a NUL-terminated cstring within a live image.
    unsafe {
        while *p.add(n) != 0 {
            n += 1;
        }
    }
    n
}

// fetch_att: byval reads the element word (zero-extended, consumers truncate);
// byref returns a real pointer into the image as the Datum word. Switched
// direct loads (C's fetch_att macro shape) — a variable-length copy here
// compiles to a memcpy call per element.
#[inline(always)]
pub fn fetch_att(p: *const u8, byval: bool, len: i32) -> Datum {
    if byval {
        // SAFETY: len in {1,2,4,8}; p points at len live bytes in the image.
        unsafe {
            match len {
                1 => Datum::from_u64(*p as u64),
                2 => Datum::from_u64((p as *const u16).read_unaligned() as u64),
                4 => Datum::from_u64((p as *const u32).read_unaligned() as u64),
                8 => Datum::from_u64((p as *const u64).read_unaligned()),
                _ => bad_fetch_att_len(),
            }
        }
    } else {
        Datum::from_usize(p as usize)
    }
}

#[cold]
#[inline(never)]
fn bad_fetch_att_len() -> ! {
    panic!("fetch_att: unsupported byval length")
}

// att_addlength over a data pointer for the element at p.
#[inline]
pub fn att_addlength_pointer(cur: usize, attlen: i32, p: *const u8) -> usize {
    if attlen > 0 {
        cur + attlen as usize
    } else if attlen == -1 {
        cur + varsize_any(p)
    } else {
        // attlen == -2 (cstring)
        cur + cstr_len(p) + 1
    }
}

// ArrayCastAndSet: copy src into dest[..], return bytes advanced (incl. align).
// Caller has handled the NULL case. `src_ptr` is used only for by-ref types.
#[inline]
pub fn array_cast_and_set(
    src: Datum,
    typlen: i32,
    typbyval: bool,
    typalign: u8,
    dest: &mut [u8],
) -> usize {
    let inc;
    if typlen > 0 {
        let n = typlen as usize;
        if typbyval {
            let bytes = src.as_u64().to_ne_bytes();
            dest[..n].copy_from_slice(&bytes[..n]);
        } else {
            let p = src.as_usize() as *const u8;
            // SAFETY: by-ref fixed-len datum points at n live bytes.
            let s = unsafe { core::slice::from_raw_parts(p, n) };
            dest[..n].copy_from_slice(s);
        }
        inc = att_align_nominal(n, typalign);
    } else {
        let p = src.as_usize() as *const u8;
        let n = att_addlength_pointer(0, typlen, p);
        // SAFETY: by-ref varlena/cstring datum points at n live bytes.
        let s = unsafe { core::slice::from_raw_parts(p, n) };
        dest[..n].copy_from_slice(s);
        inc = att_align_nominal(n, typalign);
    }
    inc
}
