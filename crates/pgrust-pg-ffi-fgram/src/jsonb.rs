//! On-disk / Datum ABI for the PostgreSQL `jsonb` type.
//!
//! These types are lifted byte-for-byte from `postgres-18.3/src/include/utils/
//! jsonb.h`: the on-disk `Jsonb` varlena, the `JsonbContainer` node header, the
//! `JEntry` child header word, and the associated flag/type-bit constants and
//! GIN-opclass marker bytes.  The `#[repr(C)]` layout MUST match the C storage
//! format exactly; layout is verified at compile time by the const-assert gates
//! at the bottom of this module.
//!
//! There is NO `extern "C"` here.  In-memory working types (`JsonbValue`,
//! `JsonbPair`, `JsonbParseState`, `JsonbIterator`, ...) are deliberately *not*
//! defined here -- they are idiomatic Rust types that live in the
//! `backend-utils-adt-jsonb-util` crate, since they are never stored on disk and
//! never cross a C ABI boundary.

#![allow(non_upper_case_globals)]

use core::mem::{align_of, offset_of, size_of};

/// `VARHDRSZ`, the varlena length-header size in bytes.  Re-uses the crate-level
/// definition (`heaptuple::VARHDRSZ`) to keep a single source of truth.
pub use crate::VARHDRSZ;

// ---------------------------------------------------------------------------
// JsonbIteratorToken: tokens used when sequentially processing a jsonb value.
// (Not on-disk; a plain enum mirroring the C enum's discriminant order.)
// ---------------------------------------------------------------------------

/// Tokens returned by `JsonbIteratorNext` / consumed by `pushJsonbValue`
/// (C: `enum JsonbIteratorToken`).  Discriminant order matches jsonb.h.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(C)]
pub enum JsonbIteratorToken {
    WJB_DONE,
    WJB_KEY,
    WJB_VALUE,
    WJB_ELEM,
    WJB_BEGIN_ARRAY,
    WJB_END_ARRAY,
    WJB_BEGIN_OBJECT,
    WJB_END_OBJECT,
}

// ---------------------------------------------------------------------------
// GIN opclass marker bytes (jsonb.h).
// ---------------------------------------------------------------------------

/// Strategy numbers for GIN index opclasses.
pub const JsonbContainsStrategyNumber: u16 = 7;
pub const JsonbExistsStrategyNumber: u16 = 9;
pub const JsonbExistsAnyStrategyNumber: u16 = 10;
pub const JsonbExistsAllStrategyNumber: u16 = 11;
pub const JsonbJsonpathExistsStrategyNumber: u16 = 15;
pub const JsonbJsonpathPredicateStrategyNumber: u16 = 16;

// ---------------------------------------------------------------------------
// GIN access-method shared constants (access/gin.h).  Used by the jsonb GIN
// opclass support functions (`jsonb_gin.c`).  Not on-disk for jsonb itself,
// but they are part of the GIN AM ABI contract.
// ---------------------------------------------------------------------------

/// `GinTernaryValue` is a `char`-sized tri-state (access/gin.h).
pub type GinTernaryValue = i8;

/// `GIN_FALSE`: item is not present / does not match.
pub const GIN_FALSE: GinTernaryValue = 0;
/// `GIN_TRUE`: item is present / matches.
pub const GIN_TRUE: GinTernaryValue = 1;
/// `GIN_MAYBE`: don't know if item is present / don't know if it matches.
pub const GIN_MAYBE: GinTernaryValue = 2;

/// `GIN_SEARCH_MODE_DEFAULT`.
pub const GIN_SEARCH_MODE_DEFAULT: i32 = 0;
/// `GIN_SEARCH_MODE_INCLUDE_EMPTY`.
pub const GIN_SEARCH_MODE_INCLUDE_EMPTY: i32 = 1;
/// `GIN_SEARCH_MODE_ALL`.
pub const GIN_SEARCH_MODE_ALL: i32 = 2;
/// `GIN_SEARCH_MODE_EVERYTHING` (internal use only).
pub const GIN_SEARCH_MODE_EVERYTHING: i32 = 3;

/// First-byte flags for the `jsonb_ops` GIN opclass storage format.
pub const JGINFLAG_KEY: u8 = 0x01; // key (or string array element)
pub const JGINFLAG_NULL: u8 = 0x02; // null value
pub const JGINFLAG_BOOL: u8 = 0x03; // boolean value
pub const JGINFLAG_NUM: u8 = 0x04; // numeric value
pub const JGINFLAG_STR: u8 = 0x05; // string value (if not an array element)
pub const JGINFLAG_HASHED: u8 = 0x10; // OR'd into flag if value was hashed
pub const JGIN_MAXLENGTH: i32 = 125; // max length of text part before hashing

// ---------------------------------------------------------------------------
// JEntry: child header word (jsonb.h).
//
// The least significant 28 bits store either the data length of the entry, or
// its end+1 offset from the start of the variable-length portion of the
// containing object.  The next three bits store the type; the high-order bit
// tells whether the low bits are a length or an offset.
// ---------------------------------------------------------------------------

/// A JEntry header word (`typedef uint32 JEntry`).
pub type JEntry = u32;

pub const JENTRY_OFFLENMASK: u32 = 0x0FFF_FFFF;
pub const JENTRY_TYPEMASK: u32 = 0x7000_0000;
pub const JENTRY_HAS_OFF: u32 = 0x8000_0000;

// Values stored in the type bits.
pub const JENTRY_ISSTRING: u32 = 0x0000_0000;
pub const JENTRY_ISNUMERIC: u32 = 0x1000_0000;
pub const JENTRY_ISBOOL_FALSE: u32 = 0x2000_0000;
pub const JENTRY_ISBOOL_TRUE: u32 = 0x3000_0000;
pub const JENTRY_ISNULL: u32 = 0x4000_0000;
pub const JENTRY_ISCONTAINER: u32 = 0x5000_0000; // array or object

/// `JBE_OFFLENFLD`: low 28 bits of a JEntry (length or offset).
#[inline]
pub const fn jbe_offlenfld(je: JEntry) -> u32 {
    je & JENTRY_OFFLENMASK
}

/// `JBE_HAS_OFF`: the low bits hold an offset (not a length).
#[inline]
pub const fn jbe_has_off(je: JEntry) -> bool {
    (je & JENTRY_HAS_OFF) != 0
}

/// `JBE_ISSTRING`.
#[inline]
pub const fn jbe_isstring(je: JEntry) -> bool {
    (je & JENTRY_TYPEMASK) == JENTRY_ISSTRING
}

/// `JBE_ISNUMERIC`.
#[inline]
pub const fn jbe_isnumeric(je: JEntry) -> bool {
    (je & JENTRY_TYPEMASK) == JENTRY_ISNUMERIC
}

/// `JBE_ISCONTAINER`.
#[inline]
pub const fn jbe_iscontainer(je: JEntry) -> bool {
    (je & JENTRY_TYPEMASK) == JENTRY_ISCONTAINER
}

/// `JBE_ISNULL`.
#[inline]
pub const fn jbe_isnull(je: JEntry) -> bool {
    (je & JENTRY_TYPEMASK) == JENTRY_ISNULL
}

/// `JBE_ISBOOL_TRUE`.
#[inline]
pub const fn jbe_isbool_true(je: JEntry) -> bool {
    (je & JENTRY_TYPEMASK) == JENTRY_ISBOOL_TRUE
}

/// `JBE_ISBOOL_FALSE`.
#[inline]
pub const fn jbe_isbool_false(je: JEntry) -> bool {
    (je & JENTRY_TYPEMASK) == JENTRY_ISBOOL_FALSE
}

/// `JBE_ISBOOL`.
#[inline]
pub const fn jbe_isbool(je: JEntry) -> bool {
    jbe_isbool_true(je) || jbe_isbool_false(je)
}

/// `JB_OFFSET_STRIDE`: store an offset (not a length) every Nth child.
pub const JB_OFFSET_STRIDE: i32 = 32;

// ---------------------------------------------------------------------------
// JsonbContainer: an array/object node within a Jsonb datum (jsonb.h).
// ---------------------------------------------------------------------------

/// A jsonb array or object node header (`struct JsonbContainer`).  The
/// `children` JEntry array (`JEntry children[FLEXIBLE_ARRAY_MEMBER]`) is the
/// flexible member and is read via byte/slice accessors, not this struct.
#[derive(Copy, Clone)]
#[repr(C)]
pub struct JsonbContainer {
    /// Number of elements or key/value pairs, plus flag bits.
    pub header: u32,
    /// Flexible array member (`JEntry children[]`).
    pub children: [JEntry; 0],
}

/// Mask for the count field in `JsonbContainer.header`.
pub const JB_CMASK: u32 = 0x0FFF_FFFF;
/// Header flag: a "raw scalar" pseudo-array.
pub const JB_FSCALAR: u32 = 0x1000_0000;
/// Header flag: object.
pub const JB_FOBJECT: u32 = 0x2000_0000;
/// Header flag: array.
pub const JB_FARRAY: u32 = 0x4000_0000;

/// `JsonContainerSize`.
#[inline]
pub const fn json_container_size(header: u32) -> u32 {
    header & JB_CMASK
}

/// `JsonContainerIsScalar`.
#[inline]
pub const fn json_container_is_scalar(header: u32) -> bool {
    (header & JB_FSCALAR) != 0
}

/// `JsonContainerIsObject`.
#[inline]
pub const fn json_container_is_object(header: u32) -> bool {
    (header & JB_FOBJECT) != 0
}

/// `JsonContainerIsArray`.
#[inline]
pub const fn json_container_is_array(header: u32) -> bool {
    (header & JB_FARRAY) != 0
}

// ---------------------------------------------------------------------------
// Jsonb: the top-level on-disk datum (jsonb.h).
// ---------------------------------------------------------------------------

/// The top-level on-disk format for a `jsonb` datum: a varlena length header
/// followed by the root `JsonbContainer` (`struct { int32 vl_len_;
/// JsonbContainer root; }`).
#[derive(Copy, Clone)]
#[repr(C)]
pub struct Jsonb {
    /// Varlena length header.  Do not touch directly.
    pub vl_len_: i32,
    /// Root container (always an array or object).
    pub root: JsonbContainer,
}

// ---------------------------------------------------------------------------
// jbvType: in-memory JsonbValue type tag (jsonb.h).
//
// This tag participates in jsonb sort order and is referenced by the on-disk
// JsonPathItemType discriminants (jpiNull == jbvNull, etc.), so the explicit
// values must match jsonb.h exactly.
// ---------------------------------------------------------------------------

/// `enum jbvType`: the type tag for an in-memory `JsonbValue`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
#[repr(i32)]
pub enum jbvType {
    /// NULL literal.
    jbvNull = 0x0,
    /// String primitive.
    jbvString = 0x1,
    /// Numeric primitive.
    jbvNumeric = 0x2,
    /// Boolean primitive.
    jbvBool = 0x3,
    /// Array container.
    jbvArray = 0x10,
    /// Object container.
    jbvObject = 0x11,
    /// Binary (`struct Jsonb`) array/object.
    jbvBinary = 0x12,
    /// Virtual datetime type (in-memory processing only).
    jbvDatetime = 0x20,
}

/// `IsAJsonbScalar`: scalar primitives plus the virtual datetime type.
#[inline]
pub const fn is_a_jsonb_scalar(ty: jbvType) -> bool {
    matches!(
        ty,
        jbvType::jbvNull
            | jbvType::jbvString
            | jbvType::jbvNumeric
            | jbvType::jbvBool
            | jbvType::jbvDatetime
    )
}

// ---------------------------------------------------------------------------
// JsonbIterState: iterator phase tag (jsonb.h).  In-memory only.
// ---------------------------------------------------------------------------

/// `enum JsonbIterState`: the iterator's current phase.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(C)]
pub enum JsonbIterState {
    JBI_ARRAY_START,
    JBI_ARRAY_ELEM,
    JBI_OBJECT_START,
    JBI_OBJECT_KEY,
    JBI_OBJECT_VALUE,
}

// ---------------------------------------------------------------------------
// Compile-time layout gates.  Jsonb / JsonbContainer are on-disk ABI.
// ---------------------------------------------------------------------------

const _: () = {
    // JsonbContainer: a single uint32 header followed by a zero-length flexible
    // array of uint32; the struct's stored size is just the header.
    assert!(size_of::<JsonbContainer>() == 4);
    assert!(align_of::<JsonbContainer>() == 4);
    assert!(offset_of!(JsonbContainer, header) == 0);
    assert!(offset_of!(JsonbContainer, children) == 4);

    // Jsonb: int32 varlena header + JsonbContainer.  4-byte aligned overall.
    assert!(size_of::<Jsonb>() == 8);
    assert!(align_of::<Jsonb>() == 4);
    assert!(offset_of!(Jsonb, vl_len_) == 0);
    assert!(offset_of!(Jsonb, root) == 4);

    // JEntry is a plain uint32.
    assert!(size_of::<JEntry>() == 4);

    // jbvType uses the explicit jsonb.h discriminants.
    assert!(jbvType::jbvNull as i32 == 0x0);
    assert!(jbvType::jbvString as i32 == 0x1);
    assert!(jbvType::jbvNumeric as i32 == 0x2);
    assert!(jbvType::jbvBool as i32 == 0x3);
    assert!(jbvType::jbvArray as i32 == 0x10);
    assert!(jbvType::jbvObject as i32 == 0x11);
    assert!(jbvType::jbvBinary as i32 == 0x12);
    assert!(jbvType::jbvDatetime as i32 == 0x20);
};
