//! Type vocabulary for `access/common/heaptuple.c`'s form/deform core. Lives
//! here (not in the owning crate) so seam-crate signatures can reference
//! [`FormedTuple`] / [`Datum`] / [`DeformedColumn`] without depending on
//! the owning crate; `backend-access-common-heaptuple` re-exports them.

extern crate alloc;

use mcx::{alloc_in, slice_in, Mcx, PgBox, PgVec};
use types_error::PgResult;

use crate::heaptuple::{HeapTupleData, MinimalTupleData};
use types_core::{Oid, TransactionId};

// ---------------------------------------------------------------------------
// The canonical value type (Datum unification — KEYSTONE / type-carrier root).
//
// CANONICAL shape (datum-redesign-plan, Option A):
//     pub enum Datum<'mcx> { ByVal(usize /*bare word*/), ByRef(PgVec<'mcx, u8>) }
//
// The by-value arm carries the raw machine word directly as `usize` (C's
// `uintptr_t` Datum). The former `ByVal(types_datum::Datum)` indirection was
// retired in the keystone payload swap: the bare-word codec (`*GetDatum` /
// `DatumGet*` bit-math) is now inlined into the `from_*` / `as_*` methods
// below, which are the canonical codec API on this enum. The bare-word
// `types_datum::Datum` survives only at the two irreducible ABI edges (the
// `store_att_byval`/`fetch_att` on-disk by-value codec and the
// `PGFunction -> Datum` fmgr return slot), reachable here via `from_usize` /
// `as_usize`.
//
// All bit-math below mirrors `postgres.h`'s `*GetDatum` / `DatumGet*` macros
// 1:1 on a 64-bit (`SIZEOF_DATUM == 8`, `USE_FLOAT8_BYVAL`) build.
// ---------------------------------------------------------------------------

/// The one canonical value type — the faithful idiomatic substitute for C's
/// `Datum`. A by-value scalar (`att->attbyval`) or a detoasted by-reference
/// image.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Datum<'mcx> {
    /// Pass-by-value scalar (`att->attbyval`); the machine word itself
    /// (C's `uintptr_t` Datum).
    ByVal(usize),
    /// By-reference value (varlena `attlen == -1`, cstring `attlen == -2`, or
    /// fixed-length pass-by-reference `attlen > 0`): the verbatim on-disk
    /// bytes, already detoasted, including any varlena header.
    ByRef(PgVec<'mcx, u8>),
}

impl Default for Datum<'_> {
    /// C's `(Datum) 0` — the zero machine word, as the by-value arm. Used by
    /// struct-literal `..Default::default()` initializers of
    /// `ExprContext.caseValue_datum`/`domainValue_datum` etc.
    fn default() -> Self {
        Datum::ByVal(0)
    }
}

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
    // The `*GetDatum` / `DatumGet*` codec family as constructors/accessors on
    // the canonical enum's by-value arm. The bit-math is inlined here (it used
    // to forward to the bare-word `types_datum::Datum` newtype).
    //
    // `as_*` panic on a `ByRef` value — C would equally read garbage by
    // treating a by-reference image as a scalar word.
    // -----------------------------------------------------------------------

    /// The raw machine word of a by-value scalar. Panics on a by-reference
    /// value (a caller bug — C would read garbage too).
    #[inline]
    fn byval_word(&self) -> usize {
        match self {
            Datum::ByVal(d) => *d,
            Datum::ByRef(_) => panic!("Datum: scalar accessor called on a by-reference value"),
        }
    }

    /// A SQL NULL / zero scalar word (`(Datum) 0`).
    pub fn null() -> Self {
        Datum::ByVal(0)
    }

    /// C: `from_usize` — carry a raw machine word.
    pub fn from_usize(value: usize) -> Self {
        Datum::ByVal(value)
    }
    /// C: `as_usize` — the raw machine word.
    pub fn as_usize(&self) -> usize {
        self.byval_word()
    }

    /// C: `BoolGetDatum(X)`.
    pub fn from_bool(value: bool) -> Self {
        Datum::ByVal(value as usize)
    }
    /// C: `DatumGetBool(X)` is `((bool) ((X) != 0))` — any nonzero word is true.
    pub fn as_bool(&self) -> bool {
        self.byval_word() != 0
    }

    /// C: `CharGetDatum(X)` — a single signed `char`, sign-extended into the word.
    pub fn from_char(value: i8) -> Self {
        Datum::ByVal(value as usize)
    }
    /// C: `DatumGetChar(X)` — read back the low byte as a signed `char`.
    pub fn as_char(&self) -> i8 {
        self.byval_word() as u8 as i8
    }

    /// C: `Int8GetDatum(X)` (PG 1-byte signed, not SQL int8).
    pub fn from_i8(value: i8) -> Self {
        Datum::ByVal(value as usize)
    }
    /// C: `DatumGetInt8(X)`.
    pub fn as_i8(&self) -> i8 {
        self.byval_word() as u8 as i8
    }

    /// C: `UInt8GetDatum(X)`.
    pub fn from_u8(value: u8) -> Self {
        Datum::ByVal(value as usize)
    }
    /// C: `DatumGetUInt8(X)`.
    pub fn as_u8(&self) -> u8 {
        self.byval_word() as u8
    }

    /// C: `Int16GetDatum(X)`.
    pub fn from_i16(value: i16) -> Self {
        Datum::ByVal(value as usize)
    }
    /// C: `DatumGetInt16(X)`.
    pub fn as_i16(&self) -> i16 {
        self.byval_word() as i16
    }

    /// C: `UInt16GetDatum(X)`.
    pub fn from_u16(value: u16) -> Self {
        Datum::ByVal(value as usize)
    }
    /// C: `DatumGetUInt16(X)`.
    pub fn as_u16(&self) -> u16 {
        self.byval_word() as u16
    }

    /// C: `Int32GetDatum(X)` — sign-extends a negative int32 into the full word.
    pub fn from_i32(value: i32) -> Self {
        Datum::ByVal(value as usize)
    }
    /// C: `DatumGetInt32(X)`.
    pub fn as_i32(&self) -> i32 {
        self.byval_word() as u32 as i32
    }

    /// C: `UInt32GetDatum(X)`.
    pub fn from_u32(value: u32) -> Self {
        Datum::ByVal(value as usize)
    }
    /// C: `DatumGetUInt32(X)`.
    pub fn as_u32(&self) -> u32 {
        self.byval_word() as u32
    }

    /// C: `Int64GetDatum(X)` (SQL int8/bigint; pass-by-value on 64-bit).
    pub fn from_i64(value: i64) -> Self {
        Datum::ByVal(value as usize)
    }
    /// C: `DatumGetInt64(X)`.
    pub fn as_i64(&self) -> i64 {
        self.byval_word() as u64 as i64
    }

    /// C: `UInt64GetDatum(X)`.
    pub fn from_u64(value: u64) -> Self {
        Datum::ByVal(value as usize)
    }
    /// C: `DatumGetUInt64(X)`.
    pub fn as_u64(&self) -> u64 {
        self.byval_word() as u64
    }

    /// C: `Float4GetDatum(X)` — reinterpret the IEEE-754 bits (no numeric cast).
    pub fn from_f32(value: f32) -> Self {
        Datum::ByVal(value.to_bits() as usize)
    }
    /// C: `DatumGetFloat4(X)` — reinterpret the low 32 bits as a `float`.
    pub fn as_f32(&self) -> f32 {
        f32::from_bits(self.byval_word() as u32)
    }

    /// C: `Float8GetDatum(X)` — reinterpret the IEEE-754 bits (pass-by-value on
    /// a `USE_FLOAT8_BYVAL` build).
    pub fn from_f64(value: f64) -> Self {
        Datum::ByVal(value.to_bits() as usize)
    }
    /// C: `DatumGetFloat8(X)` — reinterpret the word as a `double`.
    pub fn as_f64(&self) -> f64 {
        f64::from_bits(self.byval_word() as u64)
    }

    /// C: `ObjectIdGetDatum(X)`.
    pub fn from_oid(value: Oid) -> Self {
        Datum::ByVal(value as usize)
    }
    /// C: `DatumGetObjectId(X)`.
    pub fn as_oid(&self) -> Oid {
        self.byval_word() as u32
    }

    /// C: `TransactionIdGetDatum(X)` — a `TransactionId` (`xid`) is a `uint32`.
    pub fn from_transaction_id(value: TransactionId) -> Self {
        Datum::ByVal(value as usize)
    }
    /// C: `DatumGetTransactionId(X)`.
    pub fn as_transaction_id(&self) -> TransactionId {
        self.byval_word() as TransactionId
    }
}

/// One column produced by `heap_deform_tuple`: a `(value, isnull)` pair.
///
/// `value` for a by-value column is the scalar word (`ByVal`); for a
/// by-reference column it is the column's on-disk bytes copied out of the data
/// area (`ByRef`) — the faithful idiomatic stand-in for C's bare pointer into
/// the tuple (the C contract that the pointer "points into the given tuple" is
/// preserved by copying the exact bytes spanned by the field).
pub type DeformedColumn<'mcx> = (Datum<'mcx>, bool);

/// One read of a slot/tuple attribute: its [`Datum`] value plus its is-null
/// flag. (Not `Copy`: the by-reference `Datum` arm owns a `PgVec`.)
#[derive(Clone, Debug)]
pub struct SlotAttr<'mcx> {
    pub value: Datum<'mcx>,
    pub isnull: bool,
}

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

impl<'mcx> FormedTuple<'mcx> {
    /// Deep copy into `mcx` (C: `heap_copytuple`'s single-block `memcpy` into
    /// the caller's current context).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<FormedTuple<'b>> {
        Ok(FormedTuple {
            tuple: alloc_in(mcx, self.tuple.clone_in(mcx)?)?,
            data: slice_in(mcx, &self.data)?,
        })
    }

    /// Materialize a full on-page heap tuple — header (incl. its `t_bits` null
    /// bitmap) and user-data area — from an item's on-page bytes, the owned
    /// rendering of C's `loctup.t_data = (HeapTupleHeader) PageGetItem(page,
    /// lpp); loctup.t_len = ItemIdGetLength(lpp)`.
    ///
    /// `item` is the full item slice `PageGetItem(page, lpp)` of length
    /// `ItemIdGetLength(lpp)` (= the tuple's `t_len`); `block`/`offset` set the
    /// tuple's self TID. The 23-byte fixed header is decoded as in
    /// [`HeapTupleHeaderData::read_on_page`]; when `HEAP_HASNULL` is set the null
    /// bitmap (the bytes between the fixed header and `t_hoff`) is captured into
    /// `t_bits`, and the user-data area (`item[t_hoff..t_len]`) travels as
    /// [`FormedTuple::data`].
    pub fn read_on_page_full(
        mcx: Mcx<'mcx>,
        item: &[u8],
        block: types_core::primitive::BlockNumber,
        offset: types_core::primitive::OffsetNumber,
        table_oid: Oid,
    ) -> PgResult<FormedTuple<'mcx>> {
        use crate::heaptuple::{
            HeapTupleData, HeapTupleHeaderData, ItemPointerData, ON_PAGE_HEADER_SIZE, HEAP_HASNULL,
        };

        let mut hdr = HeapTupleHeaderData::read_on_page(mcx, item)?;
        let t_hoff = hdr.t_hoff as usize;

        // Capture the on-page null bitmap into the owned header. C leaves it on
        // the page (the `t_data` pointer aliases it); the owned model copies the
        // bytes between the fixed header and `t_hoff`.
        if (hdr.t_infomask & HEAP_HASNULL) != 0 {
            let end = core::cmp::min(t_hoff, item.len());
            if end > ON_PAGE_HEADER_SIZE {
                hdr.t_bits = slice_in(mcx, &item[ON_PAGE_HEADER_SIZE..end])?;
            }
        }

        // User-data area: item[t_hoff..t_len]. t_len == item.len().
        let data = if t_hoff <= item.len() {
            slice_in(mcx, &item[t_hoff..])?
        } else {
            PgVec::new_in(mcx)
        };

        let tuple = HeapTupleData {
            t_len: item.len() as u32,
            t_self: ItemPointerData::new(block, offset),
            t_tableOid: table_oid,
            t_data: Some(alloc_in(mcx, hdr)?),
        };

        Ok(FormedTuple {
            tuple: alloc_in(mcx, tuple)?,
            data,
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
