//! Type vocabulary for `access/common/heaptuple.c`'s form/deform core. Lives
//! here (not in the owning crate) so seam-crate signatures can reference
//! [`FormedTuple`] / [`Datum`] / [`DeformedColumn`] without depending on
//! the owning crate; `backend-access-common-heaptuple` re-exports them.

extern crate alloc;

use alloc::boxed::Box;
use alloc::string::String;

use mcx::{alloc_in, slice_in, Mcx, PgBox, PgVec};
use types_error::PgResult;

use crate::heaptuple::{HeapTupleData, MinimalTupleData};
use types_core::{Oid, TransactionId};
use types_datum::{flatten_expanded, ExpandedObject};

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
/// `Datum`, spanning the full set of physical representations a value can take
/// (mirrors the fmgr-boundary `RefPayload` arms plus the by-value word).
///
/// `Clone`/`Debug`/`PartialEq`/`Eq` are hand-implemented (not derived) because
/// the [`Datum::Expanded`]/[`Datum::Internal`] trait-object arms are not
/// `Clone`/`Eq`; the impls below mirror `RefPayload`'s flatten-on-clone and
/// flatten-then-compare conventions and panic for `Internal` (no flat image).
pub enum Datum<'mcx> {
    /// Pass-by-value scalar (`att->attbyval`); the machine word itself
    /// (C's `uintptr_t` Datum).
    ByVal(usize),
    /// Flat by-reference value (varlena `attlen == -1`, or fixed-length
    /// pass-by-reference `attlen > 0`): the verbatim on-disk bytes, already
    /// detoasted, including any varlena header. (The "Varlena" arm; named
    /// `ByRef` historically — mirrors `RefPayload::Varlena`.)
    ByRef(PgVec<'mcx, u8>),
    /// A C `cstring` (`typlen == -2`): owned text, no varlena header, no
    /// terminating NUL stored. Mirrors `RefPayload::Cstring`.
    Cstring(String),
    /// A record / row-as-value (composite type): a fully-formed tuple carried
    /// by value.
    Composite(FormedTuple<'mcx>),
    /// A live expanded object (PG `VARATT_IS_EXPANDED`). Mirrors
    /// `RefPayload::Expanded`.
    Expanded(Box<dyn ExpandedObject>),
    /// The `internal` pseudo-type: an opaque object passed by reference between
    /// functions within a single backend, never on disk.
    Internal(Box<dyn core::any::Any>),
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
    /// True if this is the `internal` pseudo-type arm (a live `Box<dyn Any>`
    /// state, e.g. an aggregate transition state) that cannot be cloned.
    #[inline]
    pub fn is_internal(&self) -> bool {
        matches!(self, Datum::Internal(_))
    }

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
            Datum::Cstring(s) => s.as_bytes(),
            Datum::Composite(_) | Datum::Expanded(_) | Datum::Internal(_) => {
                panic!("Datum::as_ref_bytes called on a non-flat value (Composite/Expanded/Internal); flatten first")
            }
        }
    }

    /// Deep copy into `mcx` (C: `datumCopy` into the caller's context).
    ///
    /// The `Expanded` arm flattens into its varlena byte image (C: an expanded
    /// datum copied into another context is `EOH_flatten_into`'d — there is no
    /// in-place clone of the live object), landing as the `ByRef` arm. The
    /// `Internal` arm has no copy semantics (C never `datumCopy`s an `internal`
    /// value) and panics.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<Datum<'b>> {
        Ok(match self {
            Datum::ByVal(d) => Datum::ByVal(*d),
            Datum::ByRef(b) => Datum::ByRef(slice_in(mcx, b)?),
            Datum::Cstring(s) => Datum::Cstring(s.clone()),
            Datum::Composite(t) => Datum::Composite(t.clone_in(mcx)?),
            Datum::Expanded(eo) => {
                let flat = flatten_expanded(eo.as_ref());
                Datum::ByRef(slice_in(mcx, &flat)?)
            }
            Datum::Internal(_) => {
                panic!("Datum::Internal cannot be copied into another context (C: internal pseudo-type is never datumCopy'd)")
            }
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
    #[track_caller]
    fn byval_word(&self) -> usize {
        match self {
            Datum::ByVal(d) => *d,
            Datum::ByRef(_)
            | Datum::Cstring(_)
            | Datum::Composite(_)
            | Datum::Expanded(_)
            | Datum::Internal(_) => {
                panic!("Datum: scalar accessor called on a by-reference value")
            }
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
    #[track_caller]
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

    // -----------------------------------------------------------------------
    // Constructors / accessors for the by-reference physical-representation
    // arms (mirror the `RefPayload` accessor family).
    // -----------------------------------------------------------------------

    /// C: `CStringGetDatum(X)` — a `cstring` (`typlen == -2`) value.
    pub fn from_cstring(value: String) -> Self {
        Datum::Cstring(value)
    }
    /// Borrow the `cstring` text, if this is a `Cstring`.
    pub fn as_cstring(&self) -> Option<&str> {
        match self {
            Datum::Cstring(s) => Some(s.as_str()),
            _ => None,
        }
    }

    /// Borrow the composite (record/row) tuple, if this is a `Composite`.
    pub fn as_composite(&self) -> Option<&FormedTuple<'_>> {
        match self {
            Datum::Composite(t) => Some(t),
            _ => None,
        }
    }

    /// PG `VARATT_IS_EXPANDED`: true iff this is a live expanded object.
    pub fn is_expanded(&self) -> bool {
        matches!(self, Datum::Expanded(_))
    }
    /// Borrow the expanded object read-only (PG `VARTAG_EXPANDED_RO`).
    pub fn as_expanded(&self) -> Option<&dyn ExpandedObject> {
        match self {
            Datum::Expanded(eo) => Some(eo.as_ref()),
            _ => None,
        }
    }
    /// Borrow the expanded object read/write (PG `VARTAG_EXPANDED_RW`).
    pub fn as_expanded_mut(&mut self) -> Option<&mut dyn ExpandedObject> {
        match self {
            Datum::Expanded(eo) => Some(eo.as_mut()),
            _ => None,
        }
    }

    /// Borrow the `internal` pseudo-type opaque object, if this is an `Internal`.
    pub fn as_internal(&self) -> Option<&dyn core::any::Any> {
        match self {
            Datum::Internal(a) => Some(a.as_ref()),
            _ => None,
        }
    }
}

impl Clone for Datum<'_> {
    /// In-context clone. `PgVec`/`String`/`FormedTuple` clone in their own
    /// context. The `Expanded`/`Internal` trait-object arms have no `Clone`
    /// (the latter has no copy semantics at all, the former needs an `Mcx` to
    /// re-home its flattened image — use [`Datum::clone_in`] instead) and
    /// panic. Both arms are unproduced until their respective producer waves,
    /// so this is genuinely unreachable today (sanctioned mirror-and-panic).
    fn clone(&self) -> Self {
        match self {
            Datum::ByVal(d) => Datum::ByVal(*d),
            Datum::ByRef(b) => Datum::ByRef(b.clone()),
            Datum::Cstring(s) => Datum::Cstring(s.clone()),
            Datum::Composite(t) => Datum::Composite(t.clone()),
            Datum::Expanded(_) => {
                panic!("Datum::Expanded is not bare-Clone (no Mcx to re-home the flat image); use Datum::clone_in — not yet produced, wave 2")
            }
            Datum::Internal(_) => {
                panic!("Datum::Internal is not Clone (C: internal pseudo-type has no copy) — not yet produced, wave 2")
            }
        }
    }
}

impl core::fmt::Debug for Datum<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Datum::ByVal(d) => f.debug_tuple("ByVal").field(d).finish(),
            Datum::ByRef(b) => f.debug_tuple("ByRef").field(b).finish(),
            Datum::Cstring(s) => f.debug_tuple("Cstring").field(s).finish(),
            Datum::Composite(t) => f.debug_tuple("Composite").field(t).finish(),
            Datum::Expanded(eo) => f
                .debug_struct("Expanded")
                .field("flat_size", &eo.get_flat_size())
                .finish(),
            Datum::Internal(_) => f.debug_struct("Internal").finish_non_exhaustive(),
        }
    }
}

impl PartialEq for Datum<'_> {
    /// Mirrors `RefPayload`: by-value words and flat byte images compare
    /// directly; an `Expanded` value compares by its flattened image; an
    /// `Internal` value has no equality (C: `internal` is never compared) and
    /// panics if compared.
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Datum::ByVal(a), Datum::ByVal(b)) => a == b,
            (Datum::ByRef(a), Datum::ByRef(b)) => a == b,
            (Datum::Cstring(a), Datum::Cstring(b)) => a == b,
            (Datum::Composite(a), Datum::Composite(b)) => {
                a.data == b.data && a.tuple.t_len == b.tuple.t_len
            }
            (Datum::Internal(_), _) | (_, Datum::Internal(_)) => {
                panic!("Datum::Internal is not comparable (C: internal pseudo-type has no equality)")
            }
            // Cross-arm or Expanded: fall back to flattened-byte comparison
            // where both arms have a flat image; otherwise unequal.
            (a, b) => match (a.flat_image(), b.flat_image()) {
                (Some(x), Some(y)) => x == y,
                _ => false,
            },
        }
    }
}

impl Eq for Datum<'_> {}

impl Datum<'_> {
    /// The flat varlena/text byte image of a by-reference arm, used by the
    /// `PartialEq` fallback (mirrors `RefPayload::clone_flat().flatten()`).
    /// Returns `None` for arms that have no flat byte image (`ByVal`,
    /// `Composite`, `Internal`).
    fn flat_image(&self) -> Option<alloc::vec::Vec<u8>> {
        match self {
            Datum::ByRef(b) => Some(b.as_slice().to_vec()),
            Datum::Cstring(s) => Some(s.as_bytes().to_vec()),
            Datum::Expanded(eo) => Some(flatten_expanded(eo.as_ref())),
            Datum::ByVal(_) | Datum::Composite(_) | Datum::Internal(_) => None,
        }
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

    /// Serialize this composite value into the flat `HeapTupleHeader` varlena
    /// image C points a composite `Datum` at — a single contiguous palloc block:
    /// the fixed 23-byte header (`SizeofHeapTupleHeader`), then the `t_bits` null
    /// bitmap, then the user-data area, padded to `t_hoff`. The first four bytes
    /// are the `datum_len_` varlena length word (`VARSIZE`), exactly as
    /// `heap_form_tuple` / `HeapTupleHeaderSetDatumLength` lay it out.
    ///
    /// This is the inverse of [`FormedTuple::from_datum_image`]: the header's
    /// `t_choice` is written 1:1 (the composite-Datum `TDatum` arm carries
    /// `datum_len_`/`datum_typmod`/`datum_typeid`; a stray `THeap` header
    /// serializes its `t_xmin`/`t_xmax`/`t_field3` words instead).
    pub fn to_datum_image(&self) -> alloc::vec::Vec<u8> {
        use crate::heaptuple::{HeapTupleHeaderChoice, ON_PAGE_HEADER_SIZE};

        let td = self
            .tuple
            .t_data
            .as_ref()
            .expect("FormedTuple::to_datum_image: header is None");
        let t_hoff = td.t_hoff as usize;
        let total = t_hoff + self.data.len();
        let mut image = alloc::vec![0u8; total];

        // Fixed 23-byte header. The first 12 bytes are the union; write whichever
        // arm is live (composite Datums always carry `TDatum`).
        let (w0, w4, w8) = match &td.t_choice {
            HeapTupleHeaderChoice::TDatum(d) => {
                (d.datum_len_ as u32, d.datum_typmod as u32, d.datum_typeid)
            }
            HeapTupleHeaderChoice::THeap(f) => {
                let raw = match f.t_field3 {
                    crate::heaptuple::HeapTupleField3::TCid(c) => c,
                    crate::heaptuple::HeapTupleField3::TXvac(x) => x,
                };
                (f.t_xmin, f.t_xmax, raw)
            }
        };
        image[0..4].copy_from_slice(&w0.to_ne_bytes());
        image[4..8].copy_from_slice(&w4.to_ne_bytes());
        image[8..12].copy_from_slice(&w8.to_ne_bytes());
        image[12..14].copy_from_slice(&td.t_ctid.ip_blkid.bi_hi.to_ne_bytes());
        image[14..16].copy_from_slice(&td.t_ctid.ip_blkid.bi_lo.to_ne_bytes());
        image[16..18].copy_from_slice(&td.t_ctid.ip_posid.to_ne_bytes());
        image[18..20].copy_from_slice(&td.t_infomask2.to_ne_bytes());
        image[20..22].copy_from_slice(&td.t_infomask.to_ne_bytes());
        image[22] = td.t_hoff;

        // Null bitmap (the bytes between the fixed header and `t_hoff`).
        let bits_end = core::cmp::min(ON_PAGE_HEADER_SIZE + td.t_bits.len(), t_hoff);
        if bits_end > ON_PAGE_HEADER_SIZE {
            image[ON_PAGE_HEADER_SIZE..bits_end].copy_from_slice(&td.t_bits[..(bits_end - ON_PAGE_HEADER_SIZE)]);
        }
        // (Any padding between the bitmap end and `t_hoff` stays zero.)

        // User-data area.
        image[t_hoff..].copy_from_slice(&self.data);
        image
    }

    /// Reconstruct a composite value from the flat `HeapTupleHeader` varlena
    /// image C points a composite `Datum` at (the inverse of
    /// [`FormedTuple::to_datum_image`]). The first 12 bytes are decoded as the
    /// `DatumTupleFields` (`datum_len_`/`datum_typmod`/`datum_typeid`) union arm
    /// a composite Datum always carries — `HeapTupleHeaderGetTypeId`/`...TypMod`
    /// read those fields back out. `t_len` is the image length.
    pub fn from_datum_image(mcx: Mcx<'mcx>, image: &[u8]) -> PgResult<FormedTuple<'mcx>> {
        use crate::heaptuple::{
            DatumTupleFields, HeapTupleData, HeapTupleHeaderChoice, HeapTupleHeaderData,
            ItemPointerData, BlockIdData, HEAP_HASNULL, ON_PAGE_HEADER_SIZE,
        };

        if image.len() < ON_PAGE_HEADER_SIZE {
            return Err(types_error::PgError::error(
                "composite Datum image shorter than header",
            ));
        }
        let u32_at = |o: usize| u32::from_ne_bytes([image[o], image[o + 1], image[o + 2], image[o + 3]]);
        let u16_at = |o: usize| u16::from_ne_bytes([image[o], image[o + 1]]);

        let datum_len_ = u32_at(0) as i32;
        let datum_typmod = u32_at(4) as i32;
        let datum_typeid = u32_at(8);
        let bi_hi = u16_at(12);
        let bi_lo = u16_at(14);
        let ip_posid = u16_at(16);
        let t_infomask2 = u16_at(18);
        let t_infomask = u16_at(20);
        let t_hoff = image[22];
        let t_hoff_usize = t_hoff as usize;

        let mut t_bits = PgVec::new_in(mcx);
        if (t_infomask & HEAP_HASNULL) != 0 {
            let end = core::cmp::min(t_hoff_usize, image.len());
            if end > ON_PAGE_HEADER_SIZE {
                t_bits = slice_in(mcx, &image[ON_PAGE_HEADER_SIZE..end])?;
            }
        }

        let data = if t_hoff_usize <= image.len() {
            slice_in(mcx, &image[t_hoff_usize..])?
        } else {
            PgVec::new_in(mcx)
        };

        let hdr = HeapTupleHeaderData {
            t_choice: HeapTupleHeaderChoice::TDatum(DatumTupleFields {
                datum_len_,
                datum_typmod,
                datum_typeid,
            }),
            t_ctid: ItemPointerData {
                ip_blkid: BlockIdData { bi_hi, bi_lo },
                ip_posid,
            },
            t_infomask2,
            t_infomask,
            t_hoff,
            t_bits,
        };

        let tuple = HeapTupleData {
            t_len: image.len() as u32,
            t_self: ItemPointerData::new(0, 0),
            t_tableOid: types_core::INVALID_OID,
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
