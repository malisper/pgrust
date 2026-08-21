//! Per-column representation tags (C1) and the FROZEN DictCodes payload ABI.
//!
//! The tag lattice is open-ended by contract — consumers must fail open to
//! the base column on tags they do not specialize for ([`crate::Column::gather_back`]
//! is the degrade). Base tags describe how datum cells are read; overlay
//! tags carry a payload that replaces the datum cells until gathered back.

use core::ptr::NonNull;

use ::datum::Datum;

use crate::strview::StrViews;

/// Per-column representation tag (C1). Open-ended by contract — consumers
/// must fail open to the base column on tags they do not specialize for.
///
/// Base tags (legal [`crate::Column::base`] values): [`ColRep::ByvalWord`],
/// [`ColRep::FixedRef`], [`ColRep::Varlena`], [`ColRep::RowId`]. Overlay
/// tags ([`ColRep::DictCodes`], [`ColRep::Const`]) degrade losslessly to the
/// base via gather-back.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ColRep {
    /// Pass-by-value word: each datum cell IS the value (1/2/4/8-byte
    /// classes, sign-extended per C's *GetDatum).
    ByvalWord,
    /// Fixed-length by-reference pointer; `width` = the type's typlen.
    /// Datum cells are pointers to `width` payload bytes (no varlena
    /// header), aliasing source-owned images per R1–R6.
    FixedRef { width: u16 },
    /// Varlena pointer; `inline_proven` distinguishes proven-inline (every
    /// selected non-null datum is a plain 1B-short or 4B-U image — the
    /// vguard proof, [`crate::Column::prove_inline`] /
    /// [`crate::Column::detoast_selected`]) from raw (possibly
    /// toasted/compressed) pointers, which kernels must never read through
    /// without detoast.
    Varlena { inline_proven: bool },
    /// Dictionary-code overlay: the FROZEN M1-A payload ([`DictCodes`]) —
    /// codes ptr + dict handle + epoch identity (part-global code spaces,
    /// C6). Zero-null-proof rule (C2): M1 publication requires an all-valid
    /// column; nullable dict lanes are a chartered follow-on (C6 O-6),
    /// never silently allowed.
    DictCodes(DictCodes),
    /// Single value replicated over the batch: `datums[0]` carries the
    /// value and validity bit 0 its null-ness; every logical row has that
    /// same (value, validity).
    Const,
    /// Row-identity column; datum cells are packed [`crate::RowId`] words.
    RowId,
    /// String-view overlay (M4-N, `lanev3-strview.md` §2): a lane of
    /// 16-byte [`crate::StrCell`]s riding BESIDE the datum lane. The
    /// overlay law (see [`crate::StrViews`]): the datum cells REMAIN valid
    /// plain varlena pointers for every selected valid row — hosted faces
    /// read datums unchanged, gather-back is a rep flip, retention reduces
    /// to the existing varlena copy-at-the-consumer discipline.
    StrView(StrViews),
}

impl ColRep {
    /// TRUE for the tags legal as a column's base representation.
    #[inline]
    pub fn is_base(self) -> bool {
        matches!(
            self,
            ColRep::ByvalWord | ColRep::FixedRef { .. } | ColRep::Varlena { .. } | ColRep::RowId
        )
    }

    /// TRUE for overlay tags. For `DictCodes`/`Const` the payload replaces
    /// the datum cells until gather-back; `StrView` is the exception —
    /// its cells ride BESIDE a still-authoritative datum lane (the overlay
    /// law, [`crate::StrViews`]), so its gather-back is a rep flip.
    #[inline]
    pub fn is_overlay(self) -> bool {
        !self.is_base()
    }

    /// TRUE when the datum lane is authoritative for every selected valid
    /// row: the base tags, plus `StrView` by the overlay law (its cells
    /// ride BESIDE still-valid plain varlena datum cells — see
    /// [`crate::StrViews`]). FALSE for `DictCodes`/`Const`, whose payload
    /// REPLACES the datum cells until gather-back. Per-row readers that
    /// consume `datums` raw gate on THIS, never on [`Self::is_base`] —
    /// base-ness prices staging admission, not datum readability (the
    /// M4-N composition seam: an `is_base` gate turns a legal StrView
    /// batch into a refusal the overlay law says is unnecessary).
    #[inline]
    pub fn datums_authoritative(self) -> bool {
        self.is_base() || matches!(self, ColRep::StrView(_))
    }

    /// Same base CLASS (ignoring the varlena inline proof, which is a
    /// per-batch property, not a class property).
    #[inline]
    pub fn same_base_class(self, other: ColRep) -> bool {
        match (self, other) {
            (ColRep::ByvalWord, ColRep::ByvalWord) | (ColRep::RowId, ColRep::RowId) => true,
            (ColRep::FixedRef { width: a }, ColRep::FixedRef { width: b }) => a == b,
            (ColRep::Varlena { .. }, ColRep::Varlena { .. }) => true,
            _ => false,
        }
    }
}

/// The FROZEN M1-A DictCodes payload ABI (this PR freezes it; the binding to
/// the real C6 dictionary plane is validated at M3 source-cb kickoff — the
/// M3-validation rider, `lanev3-m1-chunks.md` §5):
///
/// - **codes ptr**: one `u32` global code per staged row (`0..nrows`),
///   aliasing producer-owned decoded state, claim-scoped per R1–R6. u32 is
///   the execution-facing code width (v2 `SoaDictLane` precedent; C6 stores
///   codes bit-packed per granule and widens to u32 at publication).
/// - **dict handle**: erased access to the published dictionary
///   ([`DictHandle`] → [`DictSpace`]), C6 §6/§8 faces.
/// - **epoch identity**: the part-global code-space identity
///   ([`DictEpoch`]); equal epochs certify code-compatible lanes.
///
/// Publication is itself the publishability certificate (C6 lattice):
/// producers may only publish execution-facing dict lanes where
/// code-equality == value-equality holds; classes where it does not
/// (bpchar, numeric, interval, float, jsonb-as-value, extension types) get
/// storage-level dedup at most and never reach this ABI.
#[derive(Clone, Copy)]
pub struct DictCodes {
    codes: NonNull<u32>,
    dict: DictHandle,
    epoch: DictEpoch,
    /// AB-2.2 (v4 delta): the STRUCTURAL per-batch dict epoch tag. The u64
    /// `epoch` stays the cheap comparison currency; this key is the
    /// identity the AD-1 epoch guard compares per batch — a stale tag
    /// raises [`crate::guard::GUARD_EPOCH`] and demotes the batch (AB-7.3).
    epoch_key: DictEpochKey,
}

impl DictCodes {
    /// Publish a dict-code lane.
    ///
    /// # Safety
    ///
    /// The producer asserts the R1–R6 envelope: `codes` points to at least
    /// `nrows` live `u32` codes and the dictionary behind `dict` stays live
    /// and unmodified for the claim scope of the batch this lane is staged
    /// into (worker-private, never crossing threads); every code is
    /// `< dict.space().ncodes()`; the column is all-valid (zero-null-proof
    /// rule, M1) and code-eq == value-eq holds for the column's type class.
    #[inline]
    pub unsafe fn publish(
        codes: NonNull<u32>,
        dict: DictHandle,
        epoch: DictEpoch,
        epoch_key: DictEpochKey,
    ) -> DictCodes {
        DictCodes { codes, dict, epoch, epoch_key }
    }

    /// The raw codes pointer (identity/debugging; prefer [`DictCodes::codes`]).
    #[inline]
    pub fn codes_ptr(self) -> NonNull<u32> {
        self.codes
    }

    /// The per-row codes for a batch staging `nrows` rows.
    ///
    /// # Safety
    ///
    /// Caller asserts the publish contract still holds (claim-scoped, R1) and
    /// `nrows` does not exceed the published row count.
    #[inline]
    pub unsafe fn codes<'a>(self, nrows: usize) -> &'a [u32] {
        // SAFETY: publish contract — nrows live codes, unmodified for the
        // claim scope.
        unsafe { core::slice::from_raw_parts(self.codes.as_ptr(), nrows) }
    }

    #[inline]
    pub fn dict(self) -> DictHandle {
        self.dict
    }

    #[inline]
    pub fn epoch(self) -> DictEpoch {
        self.epoch
    }

    /// AB-2.2: the structural epoch tag the guard compares per batch.
    #[inline]
    pub fn epoch_key(self) -> DictEpochKey {
        self.epoch_key
    }
}

impl PartialEq for DictCodes {
    fn eq(&self, other: &Self) -> bool {
        self.codes == other.codes && self.dict == other.dict && self.epoch == other.epoch
    }
}

impl Eq for DictCodes {}

impl core::fmt::Debug for DictCodes {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("DictCodes")
            .field("codes", &self.codes)
            .field("dict", &self.dict)
            .field("epoch", &self.epoch)
            .finish()
    }
}

/// Handle to a published dictionary (C2 dict-lane publication), frozen by
/// M1-A as an erased reference to a [`DictSpace`] implementor.
///
/// CONTRACT-NOTE (conservative reading): C6 fixes the *storage-side*
/// dictionary plane (framed, byte-rank-sorted, part-global) but no concrete
/// executor-side struct can exist before M3 builds it, so the frozen ABI is
/// behavioral — a 16-byte erased `&dyn DictSpace` whose faces mirror C6
/// §6/§8 (`entry` + stored byte/char lengths + the sorted-order contract).
/// M1 has no dict producer (heap publishes no dict lanes); the M3-validation
/// rider covers the binding to the real plane. The trait can grow defaulted
/// faces without breaking the freeze.
///
/// Equality is provider identity (data pointer), never vtable identity
/// (vtable addresses are not unique across codegen units).
#[derive(Clone, Copy)]
pub struct DictHandle {
    space: NonNull<dyn DictSpace>,
}

impl DictHandle {
    /// Wrap a dictionary provider. Creation is safe; *dereference* is the
    /// claim-scoped act ([`DictHandle::space`]).
    #[inline]
    pub fn new<T: DictSpace>(space: &T) -> DictHandle {
        let r: &(dyn DictSpace + '_) = space;
        // SAFETY: lifetime erasure only (identical layout) — a handle is
        // not a borrow; liveness is the R1 claim-scope contract, asserted
        // at deref ([`DictHandle::space`]).
        let r: &'static (dyn DictSpace + 'static) = unsafe { core::mem::transmute(r) };
        DictHandle { space: NonNull::from(r) }
    }

    /// Access the dictionary faces.
    ///
    /// # Safety
    ///
    /// Caller asserts the R1–R6 envelope: the provider this handle was
    /// created from is still live and unmodified (claim-scoped,
    /// worker-private). The returned borrow must not outlive the claim.
    #[inline]
    pub unsafe fn space<'a>(&self) -> &'a dyn DictSpace {
        // SAFETY: caller contract — provider live for the claim scope.
        unsafe { self.space.as_ref() }
    }
}

impl PartialEq for DictHandle {
    fn eq(&self, other: &Self) -> bool {
        // Provider identity: data pointer only (vtable ptrs are unstable).
        core::ptr::addr_eq(self.space.as_ptr(), other.space.as_ptr())
    }
}

impl Eq for DictHandle {}

impl core::fmt::Debug for DictHandle {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("DictHandle").field("space", &self.space.as_ptr()).finish()
    }
}

/// Part-global stitch-epoch identity for dictionary code spaces (C1
/// overlays / C6 global codes), frozen by M1-A as an opaque producer-minted
/// 64-bit token.
///
/// Law: producers mint distinct values for distinct code spaces (a fresh
/// value per (part, column, dictionary generation)); **equality certifies
/// code-space identity** — two lanes with equal epochs carry directly
/// comparable codes (group-on-codes, once-per-code evaluation, cross-lane
/// code joins). Consumers must treat unequal epochs as incomparable and
/// fail open to gather-back.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct DictEpoch(pub u64);

/// The dictionary faces the frozen [`DictHandle`] exposes, mirroring the C6
/// contract (`docs/design/pgrcolumnar-v2.md` §6 dictionaries, §8
/// `dict_handle` face). M1 has no producer; the M3-validation rider binds
/// this to the real dictionary plane at M3 source-cb kickoff.
pub trait DictSpace {
    /// Number of codes in this code space; valid codes are `0..ncodes()`.
    fn ncodes(&self) -> u32;

    /// The base representation entries decode to — one of the base tags
    /// ([`ColRep::ByvalWord`], [`ColRep::FixedRef`], or
    /// [`ColRep::Varlena`] with `inline_proven: true`; C6 dictionaries
    /// store detoasted plain images). Gather-back flips the column to this.
    fn base_rep(&self) -> ColRep;

    /// Entry `code` as a ready-to-consume datum. Pointer datums alias
    /// dictionary-owned storage — claim-scoped per R1–R6; consumers copy at
    /// the consumer.
    fn entry_datum(&self, code: u32) -> Datum;

    /// Stored byte length of entry `code`'s value payload (C6: entries
    /// carry stored lengths — `length()`-class answers become table
    /// lookups).
    fn byte_len(&self, code: u32) -> u32;

    /// Stored character length of entry `code`'s value payload (C6; equals
    /// [`DictSpace::byte_len`] for single-byte-encoded payloads).
    fn char_len(&self, code: u32) -> u32;

    /// TRUE iff global code order == value order (the C6 sorted-dict
    /// contract, available only where an order embedding exists): MIN/MAX =
    /// code compare, ORDER BY = integer sort, range predicates = code
    /// ranges. FALSE demotes order-consuming kernels to gather-back.
    fn code_order_is_value_order(&self) -> bool;
}

// ---------------------------------------------------------------------------
// v4 deltas (separate cited commit per the copy-first law)
// ---------------------------------------------------------------------------

/// AB-2.2 (v4 delta): the structural dict-epoch identity — a dict epoch IS
/// (part identity, attno, path_ord). Layout-pinned 24 B (`repr(C)`; the
/// pgrc2_read pins suite asserts size/align): this struct is the
/// cross-crate epoch vocabulary, moved here from `pgrc2_read::dicthandle`
/// so the ABI crate is self-contained (`pgrc2_read` re-exports it — spec §7
/// Law A: codes are meaningless across epochs).
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DictEpochKey {
    pub part_uuid: [u8; 16],
    pub attno: u32,
    pub path_ord: u32,
}

/// AB-2.3 (v4 delta): the serviced-escape law's reserved ESCAPE code for
/// u16-width dict-code lanes — the code domain's maximum value; the dict
/// never assigns it. Out-of-dict values carry this code and are serviced
/// from the batch's escape (varlena) side lane.
pub const ESCAPE_CODE_U16: u16 = u16::MAX;

/// AB-2.3: the u32-width escape code.
pub const ESCAPE_CODE_U32: u32 = u32::MAX;

/// AB-2.2: the elected storage width of a dict-code lane.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CodeWidth {
    U16,
    U32,
}

/// AB-2.2: code width is elected by the part's dict entry count. u16 iff
/// every assignable code AND the reserved escape code fit the domain:
/// `entry_count < u16::MAX` (the max value is never assigned — AB-2.3).
#[inline]
pub const fn code_width_for(entry_count: u32) -> CodeWidth {
    if entry_count < ESCAPE_CODE_U16 as u32 {
        CodeWidth::U16
    } else {
        CodeWidth::U32
    }
}
