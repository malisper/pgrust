//! Staging faces (C1): every PG type — including runtime-OID extension
//! types — stages from CATALOG PROPERTIES (typlen/typbyval), never an OID
//! table. typalign governs the source-side tuple walk
//! (`types_tuple::tupmacs`), not the class.

use ::datum::Datum;
use ::types_tuple::tupdesc::CompactAttribute;
use ::types_tuple::tupmacs::fetch_att;
use ::types_tuple::varatt;

use crate::rep::ColRep;
use crate::selection::Selection;
use crate::batch::Column;

/// Catalog-derived staging class: how a type's values stage into datum
/// cells. The whole pg_type universe (and every extension type) maps here
/// by construction:
///
/// - `typbyval` (typlen 1/2/4/8) → [`StagingClass::ByvalWord`]
/// - `typlen > 0 && !typbyval` → [`StagingClass::FixedRef`]
/// - `typlen == -1` → [`StagingClass::VarlenaPtr`]
///
/// `typlen == -2` (cstring) is not table-storable and refuses
/// classification (expression-only currency), as do catalog-invalid
/// (typbyval, typlen) combinations — fail closed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StagingClass {
    /// Pass-by-value word of `width` ∈ {1, 2, 4, 8} bytes (sign-extending
    /// reads per C's *GetDatum).
    ByvalWord { width: u8 },
    /// Fixed-length by-reference: datum = pointer to `width` payload bytes.
    FixedRef { width: u16 },
    /// Varlena: datum = pointer to the image (any form; kernels need the
    /// inline proof or detoast before touching payload bytes).
    VarlenaPtr,
}

impl StagingClass {
    /// Classify from catalog properties (NEVER an OID table).
    pub fn from_catalog(typlen: i16, typbyval: bool) -> Option<StagingClass> {
        if typbyval {
            match typlen {
                1 | 2 | 4 | 8 => Some(StagingClass::ByvalWord { width: typlen as u8 }),
                _ => None,
            }
        } else if typlen > 0 {
            Some(StagingClass::FixedRef { width: typlen as u16 })
        } else if typlen == -1 {
            Some(StagingClass::VarlenaPtr)
        } else {
            // -2 = cstring (not table-storable); anything else is catalog
            // corruption. Fail closed.
            None
        }
    }

    /// Classify from a tuple-descriptor attribute.
    pub fn from_attr(att: &CompactAttribute) -> Option<StagingClass> {
        StagingClass::from_catalog(att.attlen, att.attbyval)
    }

    /// The base [`ColRep`] a column of this class carries.
    #[inline]
    pub fn base_rep(self) -> ColRep {
        match self {
            StagingClass::ByvalWord { .. } => ColRep::ByvalWord,
            StagingClass::FixedRef { width } => ColRep::FixedRef { width },
            StagingClass::VarlenaPtr => ColRep::Varlena { inline_proven: false },
        }
    }

    /// Stage one value from tuple data at `p` into a datum cell. Byval
    /// widths dispatch through `fetch_att`'s mixed ==/> compare ladder —
    /// the reviewed anti-pow2-switch shape (perf playbook: rbit/clz width
    /// dispatch measured 1.15–1.27× on deform; see the tupmacs comment).
    /// By-ref and varlena stage as in-place pointer datums (R3v aliasing).
    ///
    /// # Safety
    ///
    /// `p` points to live tuple data of this class's shape, readable for
    /// the value's extent, outliving the batch per R1–R6.
    #[inline]
    pub unsafe fn stage(self, p: *const u8) -> Datum {
        match self {
            // SAFETY: caller contract.
            StagingClass::ByvalWord { width } => unsafe { fetch_att(p, true, width as i32) },
            StagingClass::FixedRef { .. } | StagingClass::VarlenaPtr => {
                Datum::from_usize(p as usize)
            }
        }
    }
}

/// The TOAST-form lattice of one varlena datum (research pgtypes.md class
/// 11: a universal executor classifies per-DATUM, never per-type).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VarlenaForm {
    /// Plain 4-byte-header uncompressed image.
    Inline4B,
    /// 1-byte-header short image (≤126 payload bytes, unaligned).
    Short,
    /// 4-byte-header inline-compressed image (pglz/lz4) — payload is
    /// ciphertext; kernels must detoast first.
    CompressedInline,
    /// 18-byte on-disk external TOAST pointer — payload lives in a toast
    /// table; refusal-to-touch-without-detoast.
    ExternalOndisk,
    /// Indirect in-memory pointer image.
    ExternalIndirect,
    /// Expanded-object (EOH) image.
    ExternalExpanded,
}

impl VarlenaForm {
    /// Plain inline (1B short or 4B-U): kernels may read the payload
    /// directly — the `inline_proven` classes.
    #[inline]
    pub fn is_plain_inline(self) -> bool {
        matches!(self, VarlenaForm::Inline4B | VarlenaForm::Short)
    }

    /// Everything else must go through [`Column::detoast_selected`] before
    /// any payload byte is read.
    #[inline]
    pub fn needs_detoast(self) -> bool {
        !self.is_plain_inline()
    }
}

/// Classify one varlena image by its header byte(s) — reads at most the
/// first two bytes, NEVER payload bytes (safe on a bare 18-byte toast
/// pointer).
///
/// # Safety
///
/// `p` points to a live varlena image's first byte (plus the tag byte for
/// external forms — every external image is ≥ 2 bytes).
#[inline]
pub unsafe fn classify_varlena(p: *const u8) -> VarlenaForm {
    // SAFETY: caller contract for every header peek below.
    unsafe {
        if varatt::varatt_is_1b_e(p) {
            match varatt::vartag_external(p) {
                varatt::VARTAG_ONDISK => VarlenaForm::ExternalOndisk,
                varatt::VARTAG_INDIRECT => VarlenaForm::ExternalIndirect,
                tag if varatt::vartag_is_expanded(tag) => VarlenaForm::ExternalExpanded,
                tag => unrecognized_vartag(tag),
            }
        } else if varatt::varatt_is_1b(p) {
            VarlenaForm::Short
        } else if varatt_is_4b_c(p) {
            VarlenaForm::CompressedInline
        } else {
            VarlenaForm::Inline4B
        }
    }
}

#[cold]
#[inline(never)]
fn unrecognized_vartag(tag: u8) -> ! {
    panic!("unrecognized TOAST vartag: {tag}")
}

/// 4B-C (inline-compressed) header test — the one form predicate
/// `types_tuple::varatt` does not export.
///
/// # Safety
///
/// As [`varatt::varatt_is_1b`].
#[inline]
pub(crate) unsafe fn varatt_is_4b_c(p: *const u8) -> bool {
    // SAFETY: caller contract.
    #[cfg(target_endian = "little")]
    return unsafe { (*p & 0x03) == 0x02 };
    #[cfg(target_endian = "big")]
    return unsafe { (*p & 0xC0) == 0x40 };
}

impl Column {
    /// The per-batch vguard: prove every SELECTED non-null datum is a plain
    /// inline varlena (1B short or 4B-U). On success the rep upgrades to
    /// `Varlena { inline_proven: true }` and kernels may read payloads
    /// directly; on failure the rep stays raw and the column owes
    /// [`Column::detoast_selected`]. Reads only header bytes — external
    /// toast pointers are never touched past their tag
    /// (refusal-to-touch-without-detoast).
    ///
    /// # Safety
    ///
    /// The column is staged per R1–R6: every selected valid datum points to
    /// a live varlena image (header readable).
    pub unsafe fn prove_inline(&mut self, sel: &Selection) -> bool {
        debug_assert!(matches!(self.base, ColRep::Varlena { .. }));
        for &pos in sel.as_slice() {
            let row = pos as usize;
            if !self.validity.is_valid(row) {
                continue;
            }
            let p = self.datums[row].as_usize() as *const u8;
            // SAFETY: caller contract — live image header.
            if unsafe { classify_varlena(p) }.needs_detoast() {
                return false;
            }
        }
        self.rep = ColRep::Varlena { inline_proven: true };
        true
    }
}
