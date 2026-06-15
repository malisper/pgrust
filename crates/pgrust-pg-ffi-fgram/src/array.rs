//! Array on-disk / Datum ABI types from `src/include/utils/array.h`.
//!
//! A standard varlena array has the following internal structure:
//! ```text
//!   <vl_len_>     - standard varlena header word
//!   <ndim>        - number of dimensions of the array
//!   <dataoffset>  - offset to stored data, or 0 if no nulls bitmap
//!   <elemtype>    - element type OID
//!   <dimensions>  - length of each array axis (C array of int)
//!   <lower bnds>  - lower boundary of each dimension (C array of int)
//!   <null bitmap> - bitmap showing locations of nulls (OPTIONAL)
//!   <actual data> - whatever is the stored data
//! ```
//!
//! Only the fixed `ArrayType` *header* (16 bytes: four 4-byte fields) is
//! expressed here with an ABI-exact `#[repr(C)]` layout;
//! the variable dimensions / lower-bounds / null-bitmap / data follow it out of
//! line and are addressed via the `ARR_*` offset helpers, exactly as the C
//! `array.h` access macros do.  The const-asserts at the bottom of this module
//! gate the layout against the C struct.

use core::mem::{align_of, size_of};

use crate::types::Oid;

/// Maximum number of array subscripts (arbitrary limit).  `array.h:MAXDIM`.
pub const MAXDIM: i32 = 6;

/// `ArrayType` (array.h) -- the fixed header of a standard varlena array.
///
/// CAUTION: if you change this header you must also change the headers for
/// `int2vector` and `oidvector` (which are storage-compatible).
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct ArrayType {
    /// varlena header (do not touch directly! use VARSIZE/SET_VARSIZE).
    pub vl_len_: i32,
    /// # of dimensions.
    pub ndim: i32,
    /// offset to data, or 0 if no bitmap.
    pub dataoffset: i32,
    /// element type OID.
    pub elemtype: Oid,
}

/// `int2vector` (c.h) -- storage-compatible with a 1-D no-nulls array of int2.
///
/// Only the fixed header is expressed; `values[ndim]` follows out of line.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct int2vectorHeader {
    /// varlena header (do not touch directly!).
    pub vl_len_: i32,
    /// always 1 for int2vector.
    pub ndim: i32,
    /// always 0 for int2vector.
    pub dataoffset: i32,
    /// element type OID.
    pub elemtype: Oid,
    /// always 0 for int2vector.
    pub dim1: i32,
    /// always 1 for int2vector.
    pub lbound1: i32,
}

/// `oidvector` (c.h) -- storage-compatible with a 1-D no-nulls array of oid.
///
/// Only the fixed header is expressed; `values[ndim]` follows out of line.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct oidvectorHeader {
    /// varlena header (do not touch directly!).
    pub vl_len_: i32,
    /// always 1 for oidvector.
    pub ndim: i32,
    /// always 0 for oidvector.
    pub dataoffset: i32,
    /// element type OID.
    pub elemtype: Oid,
    /// always 1 for oidvector.
    pub dim1: i32,
    /// always 0 for oidvector.
    pub lbound1: i32,
}

/// `EA_MAGIC` -- ID for debugging crosschecks of expanded arrays (array.h).
pub const EA_MAGIC: i32 = 689375833;

/// `sizeof(ArrayType)` -- offset to the start of the `<dimensions>` array
/// (`ARR_DIMS` base).  C: `sizeof(ArrayType)`.
pub const ARRAYTYPE_HDRSZ: usize = size_of::<ArrayType>();

// ABI gate: ArrayType must match the C struct layout exactly (16 bytes of
// fields, 4-byte aligned; the data area itself starts at a later MAXALIGN
// boundary computed by ARR_OVERHEAD_*).
const _: () = {
    assert!(size_of::<ArrayType>() == 16);
    assert!(align_of::<ArrayType>() == 4);
    assert!(ARRAYTYPE_HDRSZ == 16);
    assert!(size_of::<int2vectorHeader>() == 24);
    assert!(size_of::<oidvectorHeader>() == 24);
};
