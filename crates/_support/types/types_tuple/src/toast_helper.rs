//! Owned model of `access/toast_helper.h`: the per-tuple TOAST pass context
//! shared between the table AM's toaster driver (`heaptoast.c`) and the
//! `toast_helper.c` pass functions.
//!
//! In C the context is a stack struct whose `Datum` arrays alias the caller's
//! deformed-value arrays; here the context owns the deformed values
//! ([`Datum`]) and the relation crosses as its `Oid` (relations cross
//! seams by OID; the relcache resolves them back to the live entry).

use mcx::PgVec;
use types_core::Oid;

use crate::common_heaptuple::Datum;

// Flags indicating the overall state of a TOAST operation (toast_helper.h).

/// One or more old TOAST datums need to be deleted.
pub const TOAST_NEEDS_DELETE_OLD: u8 = 0x0001;
/// One or more TOAST values need to be freed.
pub const TOAST_NEEDS_FREE: u8 = 0x0002;
/// Nulls were found in the tuple being toasted.
pub const TOAST_HAS_NULLS: u8 = 0x0004;
/// A new tuple needs to be built; in other words, the toaster did something.
pub const TOAST_NEEDS_CHANGE: u8 = 0x0008;

// Per-column flags (toast_helper.h).

/// The old TOAST datums for this column need to be deleted.
pub const TOASTCOL_NEEDS_DELETE_OLD: u8 = TOAST_NEEDS_DELETE_OLD;
/// The value for this column needs to be freed.
pub const TOASTCOL_NEEDS_FREE: u8 = TOAST_NEEDS_FREE;
/// The toaster should not further process this column.
pub const TOASTCOL_IGNORE: u8 = 0x0010;
/// Column found incompressible, but could be moved out-of-line.
pub const TOASTCOL_INCOMPRESSIBLE: u8 = 0x0020;

/// `ToastAttrInfo` (toast_helper.h) — information about one column of a tuple
/// being toasted.
///
/// `tai_size` is only made valid for varlena attributes whose colflags do not
/// include `TOASTCOL_IGNORE`.
#[derive(Clone, Debug)]
pub struct ToastAttrInfo<'mcx> {
    /// C's `struct varlena *tai_oldexternal`: the original external TOAST
    /// pointer bytes for this column, if `toast_tuple_init` replaced the
    /// in-tuple value with a fetched-back copy.
    pub tai_oldexternal: Option<PgVec<'mcx, u8>>,
    pub tai_size: i32,
    pub tai_colflags: u8,
    /// `pg_attribute.attcompression` for this column.
    pub tai_compression: i8,
}

impl ToastAttrInfo<'_> {
    /// A blank entry — C leaves the caller-provided array uninitialized;
    /// `toast_tuple_init` fills every field it reads.
    pub const fn empty() -> Self {
        Self {
            tai_oldexternal: None,
            tai_size: 0,
            tai_colflags: 0,
            tai_compression: 0,
        }
    }
}

/// `ToastTupleContext` (toast_helper.h) — information about one tuple being
/// toasted.
///
/// Before calling `toast_tuple_init` the caller initializes every field
/// except `ttc_flags`; `ttc_attr` holds one (blank) entry per attribute. Each
/// array has length `natts` of the relation's descriptor. `ttc_oldvalues` /
/// `ttc_oldisnull` are `None` for an insert.
#[derive(Debug)]
pub struct ToastTupleContext<'mcx> {
    /// The relation that contains the tuple (crosses as its OID).
    pub ttc_rel: Oid,
    /// Values from the tuple columns (mutated in place by the toast passes).
    pub ttc_values: PgVec<'mcx, Datum<'mcx>>,
    /// Null flags for the tuple columns.
    pub ttc_isnull: PgVec<'mcx, bool>,
    /// Values from the previous tuple (UPDATE only).
    pub ttc_oldvalues: Option<PgVec<'mcx, Datum<'mcx>>>,
    /// Null flags from the previous tuple (UPDATE only).
    pub ttc_oldisnull: Option<PgVec<'mcx, bool>>,
    pub ttc_flags: u8,
    pub ttc_attr: PgVec<'mcx, ToastAttrInfo<'mcx>>,
}
