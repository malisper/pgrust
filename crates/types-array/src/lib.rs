//! Array on-disk / Datum ABI types from `src/include/utils/array.h` and the
//! `int2vector` / `oidvector` headers from `src/include/c.h`.
//!
//! A standard varlena array has the following internal structure (array.h):
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
//! Only the fixed `ArrayType` *header* (four 4-byte fields) is expressed here;
//! the variable dimensions / lower-bounds / null-bitmap / data follow it out of
//! line and are addressed via the `ARR_*` offset helpers (in the porting
//! crate's `foundation` module), exactly as the C `array.h` access macros do.

#![no_std]
#![allow(non_camel_case_types)]

use types_core::Oid;

/// `MAXDIM` (array.h:75) — maximum number of array subscripts (arbitrary
/// limit). C: `#define MAXDIM 6`.
pub const MAXDIM: i32 = 6;

/// `EA_MAGIC` (array.h:113) — ID for debugging crosschecks of expanded
/// arrays. C: `#define EA_MAGIC 689375833`.
pub const EA_MAGIC: i32 = 689375833;

/// `ArrayType` (array.h) — the fixed header of a standard varlena array.
///
/// CAUTION (from C): if you change this header you must also change the
/// headers for [`int2vector`] and [`oidvector`] (which are
/// storage-compatible).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(C)]
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

/// `int2vector` (c.h) — storage-compatible with a 1-D no-nulls array of int2.
///
/// Only the fixed header is expressed; `values[FLEXIBLE_ARRAY_MEMBER]`
/// (`int16`) follows out of line.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(C)]
pub struct int2vector {
    /// varlena header — these fields must match `ArrayType`!
    pub vl_len_: i32,
    /// always 1 for int2vector.
    pub ndim: i32,
    /// always 0 for int2vector.
    pub dataoffset: i32,
    /// element type OID.
    pub elemtype: Oid,
    /// the (single) dimension length.
    pub dim1: i32,
    /// the (single) lower bound.
    pub lbound1: i32,
}

/// `oidvector` (c.h) — storage-compatible with a 1-D no-nulls array of oid.
///
/// Only the fixed header is expressed; `values[FLEXIBLE_ARRAY_MEMBER]` (`Oid`)
/// follows out of line.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(C)]
pub struct oidvector {
    /// varlena header — these fields must match `ArrayType`!
    pub vl_len_: i32,
    /// always 1 for oidvector.
    pub ndim: i32,
    /// always 0 for oidvector.
    pub dataoffset: i32,
    /// element type OID.
    pub elemtype: Oid,
    /// the (single) dimension length.
    pub dim1: i32,
    /// the (single) lower bound.
    pub lbound1: i32,
}

/// `sizeof(ArrayType)` — offset to the start of the `<dimensions>` array
/// (`ARR_DIMS` base). C: `sizeof(ArrayType)` (four 4-byte fields = 16).
pub const ARRAYTYPE_HDRSZ: usize = core::mem::size_of::<ArrayType>();

const _: () = assert!(ARRAYTYPE_HDRSZ == 16);

// ---------------------------------------------------------------------------
// Seam signature vocabulary.
//
// The array functions in arrayfuncs.c are *element-type polymorphic*: an
// element's bytes are only interpretable through the element type's `pg_type`
// metadata and its support functions, which live in the syscache / typcache /
// fmgr subsystems (unported neighbors). The outward seams to those owners are
// declared in terms of the vocabulary below so the seam crates can name these
// without depending on the porting crate.
// ---------------------------------------------------------------------------

/// `IOFuncSelector` (`utils/lsyscache.h`) — which element I/O function
/// `get_type_io_data` should resolve.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArrayIoFuncSelector {
    Input,
    Output,
    Receive,
    Send,
}

/// A materialized array element value handed across the element-type seams.
///
/// In C the element Datum for a by-reference type is a bare pointer into the
/// array buffer; this safe representation carries the value by-value
/// (`ByValue`) or the element's on-disk bytes (`ByRef`), so the seam provider
/// can build the real `FunctionCallInfo` argument without aliasing the buffer.
/// (`'mcx` lets a by-ref payload borrow from the array buffer / build buffer.)
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArrayElementDatum<'a> {
    /// A pass-by-value element Datum.
    ByValue(Datum),
    /// A pass-by-reference element's on-disk bytes (varlena incl. 4-byte
    /// header for `typlen == -1`; fixed `typlen` bytes otherwise; cstring
    /// incl. terminating NUL for `typlen == -2`).
    ByRef(&'a [u8]),
}

/// `get_type_io_data` result: the element type's storage metadata and the OID
/// of its selected I/O function (mirrors C's by-out-param results of
/// `get_type_io_data`).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ArrayElementIoData {
    pub typlen: i16,
    pub typbyval: bool,
    pub typalign: u8,
    pub typdelim: u8,
    pub typioparam: Oid,
    pub typiofunc: Oid,
}

/// `get_typlenbyvalalign` result: `(typlen, typbyval, typalign)`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ArrayElementStorage {
    pub typlen: i16,
    pub typbyval: bool,
    pub typalign: u8,
}

use types_datum::datum::Datum;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_is_16_bytes() {
        assert_eq!(core::mem::size_of::<ArrayType>(), 16);
        assert_eq!(ARRAYTYPE_HDRSZ, 16);
    }

    #[test]
    fn vector_headers_are_24_bytes() {
        // vl_len_ + ndim + dataoffset + elemtype + dim1 + lbound1 = 6 * 4.
        assert_eq!(core::mem::size_of::<int2vector>(), 24);
        assert_eq!(core::mem::size_of::<oidvector>(), 24);
    }

    #[test]
    fn constants_match_array_h() {
        assert_eq!(MAXDIM, 6);
        assert_eq!(EA_MAGIC, 689375833);
    }
}
