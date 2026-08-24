#![no_std]
#![allow(non_camel_case_types)]

use ::types_core::Oid;

pub const MAXDIM: i32 = 6;

pub const EA_MAGIC: i32 = 689375833;

// Fixed varlena-array header (array.h); the variable tail is addressed by the
// arrayfuncs owner's ARR_* helpers. Layout-locked to int2vector/oidvector.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(C)]
pub struct ArrayType {
    pub vl_len_: i32,
    pub ndim: i32,
    pub dataoffset: i32,
    pub elemtype: Oid,
}

// int2vector/oidvector (c.h): flexible values tail follows out of line.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(C)]
pub struct int2vector {
    pub vl_len_: i32,
    pub ndim: i32,
    pub dataoffset: i32,
    pub elemtype: Oid,
    pub dim1: i32,
    pub lbound1: i32,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(C)]
pub struct oidvector {
    pub vl_len_: i32,
    pub ndim: i32,
    pub dataoffset: i32,
    pub elemtype: Oid,
    pub dim1: i32,
    pub lbound1: i32,
}

pub const ARRAYTYPE_HDRSZ: usize = core::mem::size_of::<ArrayType>();

// int2vector and oidvector share this fixed header; the flexible values tail
// follows it out of line.
pub const VECTOR_HDRSZ: usize = core::mem::size_of::<oidvector>();

const _: () = assert!(ARRAYTYPE_HDRSZ == 16);
const _: () = assert!(core::mem::size_of::<int2vector>() == 24);
const _: () = assert!(core::mem::size_of::<oidvector>() == 24);
const _: () = assert!(VECTOR_HDRSZ == 24);

/// Does an int2vector/oidvector image's on-image `dim1` fit inside the datum's
/// varlena size? C trusts `dim1` on the OUT/hash/cmp paths because the value
/// was validated at input (int.c int2vectorin / oid.c oidvectorin); a crafted
/// on-image `dim1` that claims more elements than `varsize` can hold would
/// otherwise drive a `from_raw_parts` values slice past the buffer (OOB read).
/// Callers run this only AFTER the structural ndim/dataoffset/elemtype check,
/// so `dim1` is known to be a genuine vector-header field. A valid empty vector
/// (`dim1 == 0`) fits (`VECTOR_HDRSZ <= varsize`) and passes. `elemsz` is the
/// element width (2 for int2, 4 for oid). Arithmetic is checked so a huge
/// `dim1` cannot wrap the bound.
#[inline]
pub fn vector_dim1_fits(varsize: usize, dim1: i32, elemsz: usize) -> bool {
    let n = if dim1 < 0 { 0usize } else { dim1 as usize };
    match n
        .checked_mul(elemsz)
        .and_then(|payload| payload.checked_add(VECTOR_HDRSZ))
    {
        Some(total) => total <= varsize,
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::mem::offset_of;

    #[test]
    fn constants_match_array_h() {
        assert_eq!(MAXDIM, 6);
        assert_eq!(EA_MAGIC, 689375833);
        assert_eq!(ARRAYTYPE_HDRSZ, 16);
    }

    #[test]
    fn vector_dim1_fits_bounds() {
        let elemsz = core::mem::size_of::<Oid>(); // 4
        // Empty vector (dim1 == 0) fits when the image is at least the header.
        assert!(vector_dim1_fits(VECTOR_HDRSZ, 0, elemsz));
        // A 3-element oidvector image is exactly header + 3*4.
        assert!(vector_dim1_fits(VECTOR_HDRSZ + 3 * elemsz, 3, elemsz));
        // One element short of what dim1 claims is rejected.
        assert!(!vector_dim1_fits(VECTOR_HDRSZ + 2 * elemsz, 3, elemsz));
        // Header-only image but dim1 claims elements -> rejected (OOB case).
        assert!(!vector_dim1_fits(VECTOR_HDRSZ, 1000, elemsz));
        // Negative dim1 is treated as zero (matches dim1.max(0) slicing).
        assert!(vector_dim1_fits(VECTOR_HDRSZ, -5, elemsz));
        // A huge dim1 against a realistic image is rejected, and checked
        // arithmetic means dim1*elemsz cannot wrap to a small value that spuriously fits.
        assert!(!vector_dim1_fits(8192, i32::MAX, elemsz));
    }

    #[test]
    fn vector_headers_prefix_matches_arraytype() {
        assert_eq!(offset_of!(ArrayType, vl_len_), offset_of!(int2vector, vl_len_));
        assert_eq!(offset_of!(ArrayType, ndim), offset_of!(int2vector, ndim));
        assert_eq!(
            offset_of!(ArrayType, dataoffset),
            offset_of!(int2vector, dataoffset)
        );
        assert_eq!(offset_of!(ArrayType, elemtype), offset_of!(int2vector, elemtype));
        assert_eq!(offset_of!(int2vector, dim1), 16);
        assert_eq!(offset_of!(int2vector, lbound1), 20);
        assert_eq!(offset_of!(oidvector, dim1), 16);
        assert_eq!(offset_of!(oidvector, lbound1), 20);
    }
}
