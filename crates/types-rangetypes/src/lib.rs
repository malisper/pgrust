//! Range / multirange type vocabulary (`utils/rangetypes.h`,
//! `utils/multirangetypes.h`), trimmed to what the selectivity ports consume.
//!
//! `RangeType` / `MultirangeType` are the fixed toastable-varlena headers; the
//! serialized bound value(s), flags byte (range) and inlined range objects
//! (multirange) follow the header in the range ADT's own private on-disk
//! encoding. Consumers never decode that payload directly -- they always go
//! through `range_deserialize` / `DatumGetRangeTypeP` (the
//! `backend-utils-adt-rangetypes`/`-multirangetypes` seams) -- so the payload
//! stays opaque (semantic opacity the range ADT owns), modeled by the
//! detoasted-pointer handles [`RangeTypeP`] / [`MultirangeTypeP`].

#![no_std]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
#![allow(non_snake_case)]

use core::mem::size_of;

use types_core::primitive::Oid;
use types_datum::datum::Datum;

/// Fixed header of the toastable varlena `RangeType` (rangetypes.h:25). The
/// bound value(s) and the trailing flags byte follow in the ADT's private
/// serialized form.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct RangeType {
    /// Varlena header -- access only via `VARSIZE`/`SET_VARSIZE`.
    pub vl_len_: i32,
    /// The range type's own OID.
    pub rangetypid: Oid,
}

/// `offsetof(RangeType, <bounds>)` -- size of the fixed header preceding the
/// bound values.
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
/// `RANGE_CONTAIN_EMPTY` -- GiST internal-page entry whose subtree contains
/// some empty ranges.
pub const RANGE_CONTAIN_EMPTY: u8 = 0x80;

/// Internal representation of either bound of a range (rangetypes.h:62). The
/// in-memory working form, not the serialized on-disk layout.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
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

/// Fixed header of the toastable varlena `MultirangeType`
/// (multirangetypes.h:26). The inlined `ShortRangeType` range objects follow
/// the count in the ADT's private serialized form.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct MultirangeType {
    /// Varlena header -- access only via `VARSIZE`/`SET_VARSIZE`.
    pub vl_len_: i32,
    /// The multirange type's own OID.
    pub multirangetypid: Oid,
    /// The number of ranges.
    pub rangeCount: u32,
}

/// `offsetof(MultirangeType, <ranges>)` -- size of the fixed header preceding
/// the inlined range objects.
pub const MULTIRANGE_HEADER_SIZE: usize = size_of::<MultirangeType>();

use core::marker::PhantomData;

/// A detoasted `RangeType *` (the result of `DatumGetRangeTypeP`). The serialized
/// payload after the header is the range ADT's private encoding, so this is an
/// opaque handle the range ADT produces and consumes (`range_deserialize`); the
/// `rangetypid` of its header is the only directly-readable field. `'mcx` ties
/// it to the context the detoasted copy lives in.
#[derive(Copy, Clone, Debug)]
pub struct RangeTypeP<'mcx> {
    /// The detoasted varlena's address (range-ADT-owned memory).
    pub ptr: *const RangeType,
    /// Ties the handle to the allocating context's lifetime.
    pub _marker: PhantomData<&'mcx RangeType>,
}

impl<'mcx> RangeTypeP<'mcx> {
    /// Borrow the fixed `RangeType` header as a safe reference.
    ///
    /// `ptr` always points at a valid, fully-detoasted `RangeType` header that
    /// lives for `'mcx` (established at construction in `DatumGetRangeTypeP`),
    /// so reborrowing it as a shared reference is sound. Encapsulating the one
    /// unavoidable raw-pointer reborrow here lets every header-field read be a
    /// safe field access instead of a scattered raw deref.
    #[inline]
    pub fn header(&self) -> &'mcx RangeType {
        // SAFETY: see the doc comment -- `ptr` is a valid, properly-aligned,
        // 'mcx-lived RangeType header; the value is never mutated through any
        // other alias while this shared reference exists.
        unsafe { &*self.ptr }
    }

    /// `RangeTypeGetOid(range)` (rangetypes.h) -- the range type's own OID.
    #[inline]
    pub fn rangetypid(&self) -> Oid {
        self.header().rangetypid
    }
}

/// A detoasted `MultirangeType *` (`DatumGetMultirangeTypeP`). Opaque handle the
/// multirange ADT produces and consumes; `rangeCount` is read via the header.
#[derive(Copy, Clone, Debug)]
pub struct MultirangeTypeP<'mcx> {
    /// The detoasted varlena's address (multirange-ADT-owned memory).
    pub ptr: *const MultirangeType,
    /// Ties the handle to the allocating context's lifetime.
    pub _marker: PhantomData<&'mcx MultirangeType>,
}

impl<'mcx> MultirangeTypeP<'mcx> {
    /// Borrow the fixed `MultirangeType` header as a safe reference.
    ///
    /// `ptr` always points at a valid, fully-detoasted `MultirangeType` header
    /// that lives for `'mcx` (established at construction in
    /// `DatumGetMultirangeTypeP`), so reborrowing it as a shared reference is
    /// sound. Encapsulating the one unavoidable raw-pointer reborrow here lets
    /// every header-field read be a safe field access instead of a scattered raw
    /// deref.
    #[inline]
    pub fn header(&self) -> &'mcx MultirangeType {
        // SAFETY: see the doc comment -- `ptr` is a valid, properly-aligned,
        // 'mcx-lived MultirangeType header; the value is never mutated through
        // any other alias while this shared reference exists.
        unsafe { &*self.ptr }
    }

    /// `MultirangeTypeGetOid(mr)` (multirangetypes.h) -- the multirange type's
    /// own OID.
    #[inline]
    pub fn multirangetypid(&self) -> Oid {
        self.header().multirangetypid
    }

    /// `(mr)->rangeCount` -- the number of member ranges.
    #[inline]
    pub fn range_count(&self) -> u32 {
        self.header().rangeCount
    }
}
