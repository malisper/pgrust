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

const _: () = assert!(ARRAYTYPE_HDRSZ == 16);
const _: () = assert!(core::mem::size_of::<int2vector>() == 24);
const _: () = assert!(core::mem::size_of::<oidvector>() == 24);

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
