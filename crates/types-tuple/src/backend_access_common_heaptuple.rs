//! Type vocabulary for `access/common/heaptuple.c`'s form/deform core. Lives
//! here (not in the owning crate) so seam-crate signatures can reference
//! [`FormedTuple`] / [`TupleValue`] / [`DeformedColumn`] without depending on
//! the owning crate; `backend-access-common-heaptuple` re-exports them.

extern crate alloc;

use mcx::{alloc_in, slice_in, Mcx, PgBox, PgVec};
use types_error::PgResult;

use crate::heaptuple::{HeapTupleData, MinimalTupleData};
// The bare-word newtype (`types_datum::Datum`) is the payload of the by-value
// arm. It is imported under an alias so that the canonical value type defined
// here can take the unqualified name `Datum`.
use types_core::{Oid, TransactionId};
use types_datum::Datum as ScalarWord;

// ---------------------------------------------------------------------------
// The canonical value type (Datum unification — KEYSTONE / type-carrier root).
//
// CANONICAL shape (datum-redesign-plan, Option A — RECOMMENDED):
//     pub enum Datum<'mcx> { ByVal(Datum /*bare word*/), ByRef(PgVec<'mcx, u8>) }
//
// The by-value arm carries the bare-word newtype `types_datum::Datum` (here
// aliased `ScalarWord`). Per the plan, `Datum(usize)` is kept *inside* `ByVal`
// and at the two irreducible ABI edges (the `store_att_byval`/`fetch_att`
// on-disk by-value codec and the `PGFunction -> Datum` fmgr return slot) — it
// is NOT migrated away here. The `from_*` / `as_*` methods below are the
// canonical `*GetDatum` / `DatumGet*` codec API on this enum; migrated
// consumers call them instead of constructing a free-standing
// `types_datum::Datum`, and they faithfully forward to the by-value word
// carried in `ByVal`.
// ---------------------------------------------------------------------------

/// The one canonical value type — the faithful idiomatic substitute for C's
/// `Datum`. A by-value scalar (`att->attbyval`) or a detoasted by-reference
/// image. (Renamed from the former `TupleValue`; the `TupleValue` alias below
/// is a transitional shim removed in cleanup.)
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Datum<'mcx> {
    /// Pass-by-value scalar (`att->attbyval`); the machine word itself,
    /// carried as the bare-word newtype (the sanctioned ABI-edge `Datum`).
    ByVal(ScalarWord),
    /// By-reference value (varlena `attlen == -1`, cstring `attlen == -2`, or
    /// fixed-length pass-by-reference `attlen > 0`): the verbatim on-disk
    /// bytes, already detoasted, including any varlena header.
    ByRef(PgVec<'mcx, u8>),
}

/// Transitional compat alias for the renamed enum. Removed in cleanup once all
/// consumers refer to [`Datum`] directly.
pub type TupleValue<'mcx> = Datum<'mcx>;

impl Datum<'_> {
    /// `DatumGetPointer(datum)` analogue: borrow the by-reference bytes. Panics
    /// if this is a by-value scalar (a caller bug — C would have a type/length
    /// mismatch here too).
    #[inline]
    pub fn as_ref_bytes(&self) -> &[u8] {
        match self {
            Datum::ByRef(b) => b,
            Datum::ByVal(_) => {
                panic!("Datum::as_ref_bytes called on a by-value attribute")
            }
        }
    }

    /// Deep copy into `mcx` (C: `datumCopy` into the caller's context).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<Datum<'b>> {
        Ok(match self {
            Datum::ByVal(d) => Datum::ByVal(*d),
            Datum::ByRef(b) => Datum::ByRef(slice_in(mcx, b)?),
        })
    }

    // -----------------------------------------------------------------------
    // Migration-target conversion API.
    //
    // The `*GetDatum` / `DatumGet*` codec family as constructors/accessors on
    // the canonical enum (the by-value arm). These are what migrated consumers
    // call instead of constructing a free-standing `types_datum::Datum`; the
    // body forwards to the bare-word newtype carried in `ByVal` (the sanctioned
    // by-value ABI edge).
    //
    // `as_*` panic on a `ByRef` value — C would equally read garbage by
    // treating a by-reference image as a scalar word.
    // -----------------------------------------------------------------------

    #[inline]
    fn byval_word(&self) -> ScalarWord {
        match self {
            Datum::ByVal(d) => *d,
            Datum::ByRef(_) => panic!("Datum: scalar accessor called on a by-reference value"),
        }
    }

    /// A SQL NULL / zero scalar word (`(Datum) 0`).
    pub fn null() -> Self {
        Datum::ByVal(ScalarWord::null())
    }

    /// C: `from_usize` — carry a raw machine word.
    pub fn from_usize(value: usize) -> Self {
        Datum::ByVal(ScalarWord::from_usize(value))
    }
    /// C: `as_usize` — the raw machine word.
    pub fn as_usize(&self) -> usize {
        self.byval_word().as_usize()
    }

    /// C: `BoolGetDatum(X)`.
    pub fn from_bool(value: bool) -> Self {
        Datum::ByVal(ScalarWord::from_bool(value))
    }
    /// C: `DatumGetBool(X)`.
    pub fn as_bool(&self) -> bool {
        self.byval_word().as_bool()
    }

    /// C: `CharGetDatum(X)`.
    pub fn from_char(value: i8) -> Self {
        Datum::ByVal(ScalarWord::from_char(value))
    }
    /// C: `DatumGetChar(X)`.
    pub fn as_char(&self) -> i8 {
        self.byval_word().as_char()
    }

    /// C: `Int8GetDatum(X)` (PG 1-byte signed, not SQL int8).
    pub fn from_i8(value: i8) -> Self {
        Datum::ByVal(ScalarWord::from_i8(value))
    }
    /// C: `DatumGetInt8(X)`.
    pub fn as_i8(&self) -> i8 {
        self.byval_word().as_i8()
    }

    /// C: `UInt8GetDatum(X)`.
    pub fn from_u8(value: u8) -> Self {
        Datum::ByVal(ScalarWord::from_u8(value))
    }
    /// C: `DatumGetUInt8(X)`.
    pub fn as_u8(&self) -> u8 {
        self.byval_word().as_u8()
    }

    /// C: `Int16GetDatum(X)`.
    pub fn from_i16(value: i16) -> Self {
        Datum::ByVal(ScalarWord::from_i16(value))
    }
    /// C: `DatumGetInt16(X)`.
    pub fn as_i16(&self) -> i16 {
        self.byval_word().as_i16()
    }

    /// C: `UInt16GetDatum(X)`.
    pub fn from_u16(value: u16) -> Self {
        Datum::ByVal(ScalarWord::from_u16(value))
    }
    /// C: `DatumGetUInt16(X)`.
    pub fn as_u16(&self) -> u16 {
        self.byval_word().as_u16()
    }

    /// C: `Int32GetDatum(X)`.
    pub fn from_i32(value: i32) -> Self {
        Datum::ByVal(ScalarWord::from_i32(value))
    }
    /// C: `DatumGetInt32(X)`.
    pub fn as_i32(&self) -> i32 {
        self.byval_word().as_i32()
    }

    /// C: `UInt32GetDatum(X)`.
    pub fn from_u32(value: u32) -> Self {
        Datum::ByVal(ScalarWord::from_u32(value))
    }
    /// C: `DatumGetUInt32(X)`.
    pub fn as_u32(&self) -> u32 {
        self.byval_word().as_u32()
    }

    /// C: `Int64GetDatum(X)` (SQL int8/bigint).
    pub fn from_i64(value: i64) -> Self {
        Datum::ByVal(ScalarWord::from_i64(value))
    }
    /// C: `DatumGetInt64(X)`.
    pub fn as_i64(&self) -> i64 {
        self.byval_word().as_i64()
    }

    /// C: `UInt64GetDatum(X)`.
    pub fn from_u64(value: u64) -> Self {
        Datum::ByVal(ScalarWord::from_u64(value))
    }
    /// C: `DatumGetUInt64(X)`.
    pub fn as_u64(&self) -> u64 {
        self.byval_word().as_u64()
    }

    /// C: `Float4GetDatum(X)`.
    pub fn from_f32(value: f32) -> Self {
        Datum::ByVal(ScalarWord::from_f32(value))
    }
    /// C: `DatumGetFloat4(X)`.
    pub fn as_f32(&self) -> f32 {
        self.byval_word().as_f32()
    }

    /// C: `Float8GetDatum(X)`.
    pub fn from_f64(value: f64) -> Self {
        Datum::ByVal(ScalarWord::from_f64(value))
    }
    /// C: `DatumGetFloat8(X)`.
    pub fn as_f64(&self) -> f64 {
        self.byval_word().as_f64()
    }

    /// C: `ObjectIdGetDatum(X)`.
    pub fn from_oid(value: Oid) -> Self {
        Datum::ByVal(ScalarWord::from_oid(value))
    }
    /// C: `DatumGetObjectId(X)`.
    pub fn as_oid(&self) -> Oid {
        self.byval_word().as_oid()
    }

    /// C: `TransactionIdGetDatum(X)`.
    pub fn from_transaction_id(value: TransactionId) -> Self {
        Datum::ByVal(ScalarWord::from_transaction_id(value))
    }
    /// C: `DatumGetTransactionId(X)`.
    pub fn as_transaction_id(&self) -> TransactionId {
        self.byval_word().as_transaction_id()
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

/// A fully-formed minimal tuple: the owned [`MinimalTupleData`] header (incl. its
/// `t_bits` null bitmap) plus the user-data area bytes.
///
/// As with [`FormedTuple`], the user data that C lays out contiguously after the
/// header (at `(char *) tuple + hoff`) travels alongside as [`Self::data`]. The
/// header's `t_hoff` is the full on-disk header offset including
/// `MINIMAL_TUPLE_OFFSET`, exactly as C sets it; `t_len` is `hoff + data.len()`
/// *without* `MINIMAL_TUPLE_OFFSET` (per the `t_len` contract).
///
/// Lives here (not in the owning `backend-access-common-heaptuple` crate) so the
/// slot payload model in `types-nodes` can carry it as the
/// `MinimalTupleTableSlot.mintuple` field; the owning crate re-exports it.
#[derive(Clone, Debug)]
pub struct FormedMinimalTuple<'mcx> {
    pub tuple: PgBox<'mcx, MinimalTupleData<'mcx>>,
    /// The user-data area (`data_len` bytes), i.e. the bytes at `tuple + hoff`.
    pub data: PgVec<'mcx, u8>,
}

impl FormedMinimalTuple<'_> {
    /// Deep copy into `mcx` (C: `heap_copy_minimal_tuple`'s single-block copy
    /// into the caller's current context).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<FormedMinimalTuple<'b>> {
        Ok(FormedMinimalTuple {
            tuple: alloc_in(mcx, self.tuple.clone_in(mcx)?)?,
            data: slice_in(mcx, &self.data)?,
        })
    }
}
