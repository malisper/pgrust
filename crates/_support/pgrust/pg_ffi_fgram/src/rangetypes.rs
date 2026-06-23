//! On-disk ABI for the PostgreSQL `range` and `multirange` types.
//!
//! These layouts are lifted byte-for-byte from
//! `src/include/utils/rangetypes.h` and `src/include/utils/multirangetypes.h`
//! (PostgreSQL 18.3).  Both types are toastable varlena values: the first
//! `int32` of the object is the varlena header (total object size in bytes,
//! manipulated only via `VARSIZE`/`SET_VARSIZE`), followed by the type's own
//! OID and, for multiranges, a `uint32` range count.  After the fixed header
//! the (zero to two) bound values plus a flags byte (range) or the inlined
//! `ShortRangeType` structs (multirange) are stored out of line; only the
//! fixed header layout is expressed here so that `offsetof` / `SET_VARSIZE`
//! computations are ABI-exact.  The layouts are verified at compile time by
//! the const-assert gate at the bottom of this module.
//!
//! There is NO `extern "C"` here.

#![allow(non_upper_case_globals)]

use core::mem::{align_of, offset_of, size_of};

use crate::heaptuple::VARHDRSZ;
use crate::types::{Datum, Oid};

// ---------------------------------------------------------------------------
// RangeType on-disk header (rangetypes.h:25).
//
//   typedef struct {
//       int32 vl_len_;     /* varlena header (do not touch directly!) */
//       Oid   rangetypid;  /* range type's own OID */
//       /* zero to two bound values, then a flags byte, follow */
//   } RangeType;
// ---------------------------------------------------------------------------

/// Fixed header of the toastable varlena `RangeType` (rangetypes.h:25).  The
/// bound value(s) and the trailing flags byte are stored out of line.
#[derive(Copy, Clone, Debug)]
#[repr(C)]
pub struct RangeType {
    /// Varlena header -- access only via `VARSIZE`/`SET_VARSIZE`.
    pub vl_len_: i32,
    /// The range type's own OID.
    pub rangetypid: Oid,
}

/// `offsetof(RangeType, <bounds>)` -- the size of the fixed header that
/// precedes the bound values.
pub const RANGE_HEADER_SIZE: usize = size_of::<RangeType>();

/// The text literal for an empty range (rangetypes.h:32).
pub const RANGE_EMPTY_LITERAL: &str = "empty";

// --- Range flags byte (rangetypes.h:38-45). -------------------------------

/// `RANGE_EMPTY` -- range is empty.
pub const RANGE_EMPTY: u8 = 0x01;
/// `RANGE_LB_INC` -- lower bound is inclusive.
pub const RANGE_LB_INC: u8 = 0x02;
/// `RANGE_UB_INC` -- upper bound is inclusive.
pub const RANGE_UB_INC: u8 = 0x04;
/// `RANGE_LB_INF` -- lower bound is -infinity.
pub const RANGE_LB_INF: u8 = 0x08;
/// `RANGE_UB_INF` -- upper bound is +infinity.
pub const RANGE_UB_INF: u8 = 0x10;
/// `RANGE_LB_NULL` -- lower bound is null (NOT USED).
pub const RANGE_LB_NULL: u8 = 0x20;
/// `RANGE_UB_NULL` -- upper bound is null (NOT USED).
pub const RANGE_UB_NULL: u8 = 0x40;
/// `RANGE_CONTAIN_EMPTY` -- marks a GiST internal-page entry whose subtree
/// contains some empty ranges.
pub const RANGE_CONTAIN_EMPTY: u8 = 0x80;

/// `RANGE_HAS_LBOUND(flags)` (rangetypes.h:48): true unless the range is empty
/// or its lower bound is null / -infinity.
#[inline]
pub fn RANGE_HAS_LBOUND(flags: u8) -> bool {
    (flags & (RANGE_EMPTY | RANGE_LB_NULL | RANGE_LB_INF)) == 0
}

/// `RANGE_HAS_UBOUND(flags)` (rangetypes.h:52): true unless the range is empty
/// or its upper bound is null / +infinity.
#[inline]
pub fn RANGE_HAS_UBOUND(flags: u8) -> bool {
    (flags & (RANGE_EMPTY | RANGE_UB_NULL | RANGE_UB_INF)) == 0
}

// ---------------------------------------------------------------------------
// RangeBound -- the internal (NOT on-disk) representation of a bound
// (rangetypes.h:62).
// ---------------------------------------------------------------------------

/// Internal representation of either bound of a range (rangetypes.h:62).  This
/// is the in-memory working form, not the serialized on-disk layout.
#[derive(Copy, Clone, Debug)]
#[repr(C)]
pub struct RangeBound {
    /// The bound value, if any.
    pub val: Datum,
    /// Bound is +/- infinity.
    pub infinite: bool,
    /// Bound is inclusive (vs exclusive).
    pub inclusive: bool,
    /// This is the lower (vs upper) bound.
    pub lower: bool,
}

// ---------------------------------------------------------------------------
// MultirangeType on-disk header (multirangetypes.h:26).
//
//   typedef struct {
//       int32  vl_len_;          /* varlena header (do not touch directly!) */
//       Oid    multirangetypid;  /* multirange type's own OID */
//       uint32 rangeCount;       /* the number of ranges */
//       /* ShortRangeType structs follow */
//   } MultirangeType;
// ---------------------------------------------------------------------------

/// Fixed header of the toastable varlena `MultirangeType`
/// (multirangetypes.h:26).  The `ShortRangeType` range objects follow the
/// count and are stored out of line (they are themselves variable length).
#[derive(Copy, Clone, Debug)]
#[repr(C)]
pub struct MultirangeType {
    /// Varlena header -- access only via `VARSIZE`/`SET_VARSIZE`.
    pub vl_len_: i32,
    /// The multirange type's own OID.
    pub multirangetypid: Oid,
    /// The number of ranges.
    pub rangeCount: u32,
}

/// `offsetof(MultirangeType, <ranges>)` -- the size of the fixed header that
/// precedes the inlined `ShortRangeType` structs.
pub const MULTIRANGE_HEADER_SIZE: usize = size_of::<MultirangeType>();

// ---------------------------------------------------------------------------
// Compile-time ABI gate.
// ---------------------------------------------------------------------------

const _: () = {
    // RangeType: int32 + Oid, 4-byte aligned (rangetypes.h:25).
    assert!(size_of::<RangeType>() == VARHDRSZ + size_of::<Oid>());
    assert!(align_of::<RangeType>() == 4);
    assert!(offset_of!(RangeType, vl_len_) == 0);
    assert!(offset_of!(RangeType, rangetypid) == VARHDRSZ);
    assert!(RANGE_HEADER_SIZE == 8);

    // MultirangeType: int32 + Oid + uint32, 4-byte aligned
    // (multirangetypes.h:26).
    assert!(size_of::<MultirangeType>() == VARHDRSZ + size_of::<Oid>() + size_of::<u32>());
    assert!(align_of::<MultirangeType>() == 4);
    assert!(offset_of!(MultirangeType, vl_len_) == 0);
    assert!(offset_of!(MultirangeType, multirangetypid) == VARHDRSZ);
    assert!(offset_of!(MultirangeType, rangeCount) == VARHDRSZ + size_of::<Oid>());
    assert!(MULTIRANGE_HEADER_SIZE == 12);

    // Flags byte covers all eight bits without overlap (rangetypes.h:38-45).
    assert!(
        RANGE_EMPTY
            | RANGE_LB_INC
            | RANGE_UB_INC
            | RANGE_LB_INF
            | RANGE_UB_INF
            | RANGE_LB_NULL
            | RANGE_UB_NULL
            | RANGE_CONTAIN_EMPTY
            == 0xFF
    );
};
