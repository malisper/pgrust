//! Type vocabulary for `access/common/heaptuple.c`'s form/deform core. Lives
//! here (not in the owning crate) so seam-crate signatures can reference
//! [`FormedTuple`] / [`TupleValue`] / [`DeformedColumn`] without depending on
//! the owning crate; `backend-access-common-heaptuple` re-exports them.

extern crate alloc;

use mcx::{alloc_in, slice_in, Mcx, PgBox, PgVec};
use types_error::PgResult;

use crate::heaptuple::HeapTupleData;
use types_datum::Datum;

// Per-attribute value model (the faithful idiomatic `Datum` substitute).

/// A single attribute's value handed to / produced by the tuple
/// (de)serializers, modelling C's per-attribute `Datum` over the safe byte
/// representation (see the `backend-access-common-heaptuple` module docs).
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TupleValue<'mcx> {
    /// Pass-by-value scalar (`att->attbyval`); the machine word itself.
    ByVal(Datum),
    /// By-reference value (varlena `attlen == -1`, cstring `attlen == -2`, or
    /// fixed-length pass-by-reference `attlen > 0`): the verbatim on-disk
    /// bytes, already detoasted, including any varlena header.
    ByRef(PgVec<'mcx, u8>),
}

impl TupleValue<'_> {
    /// `DatumGetPointer(datum)` analogue: borrow the by-reference bytes. Panics
    /// if this is a by-value scalar (a caller bug — C would have a type/length
    /// mismatch here too).
    #[inline]
    pub fn as_ref_bytes(&self) -> &[u8] {
        match self {
            TupleValue::ByRef(b) => b,
            TupleValue::ByVal(_) => {
                panic!("TupleValue::as_ref_bytes called on a by-value attribute")
            }
        }
    }

    /// Deep copy into `mcx` (C: `datumCopy` into the caller's context).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<TupleValue<'b>> {
        Ok(match self {
            TupleValue::ByVal(d) => TupleValue::ByVal(*d),
            TupleValue::ByRef(b) => TupleValue::ByRef(slice_in(mcx, b)?),
        })
    }
}

/// One column produced by `heap_deform_tuple`: a `(value, isnull)` pair.
///
/// `value` for a by-value column is the scalar word (`ByVal`); for a
/// by-reference column it is the column's on-disk bytes copied out of the data
/// area (`ByRef`) — the faithful idiomatic stand-in for C's bare pointer into
/// the tuple (the C contract that the pointer "points into the given tuple" is
/// preserved by copying the exact bytes spanned by the field).
pub type DeformedColumn<'mcx> = (TupleValue<'mcx>, bool);

/// A fully-formed heap tuple: the owned [`HeapTupleData`] plus its user-data
/// area bytes (`td + t_hoff .. td + t_len`).
///
/// In C the header, optional null bitmap, and user data are one contiguous
/// `palloc` chunk; here the header (incl. its `t_bits` null bitmap) lives in the
/// owned `HeapTupleHeaderData` and the user-data area travels alongside as
/// [`FormedTuple::data`]. `tuple.t_len` is the full on-disk length
/// (`t_hoff + data.len()`), matching C. Every piece is allocated in the `'mcx`
/// context the forming function received.
#[derive(Clone, Debug)]
pub struct FormedTuple<'mcx> {
    pub tuple: PgBox<'mcx, HeapTupleData<'mcx>>,
    /// The user-data area (`data_len` bytes), i.e. the bytes at `td + t_hoff`.
    pub data: PgVec<'mcx, u8>,
}

impl FormedTuple<'_> {
    /// Deep copy into `mcx` (C: `heap_copytuple`'s single-block `memcpy` into
    /// the caller's current context).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<FormedTuple<'b>> {
        Ok(FormedTuple {
            tuple: alloc_in(mcx, self.tuple.clone_in(mcx)?)?,
            data: slice_in(mcx, &self.data)?,
        })
    }
}
