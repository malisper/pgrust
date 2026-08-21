//! Constant lowering (charter §5's binding list, verbatim): "Constant
//! lowering binds: exact-rescale-or-abstain, fail closed on non-inline
//! varlena const headers, contains_nan blocks float pruning."
//!
//! Lowering turns one query constant into the column's key domain (and its
//! bloom bytes) under the SAME derivation the builder used — one
//! definition, both sides. Every failure is a typed [`AbstainReason`]:
//! an abstained constant produces NO verdict (the prober scans — pruning
//! is advisory-only, so abstention is always sound), never an error and
//! never a guess.
//!
//! Fail-closed cases carried here:
//! - a varlena constant whose header is not the plain inline 4B-U shape
//!   (short 1B headers, compressed 4B-C, external TOAST pointers) is never
//!   dereferenced past its first bytes: [`AbstainReason::NonInlineVarlenaHeader`];
//! - a NaN float constant abstains ([`AbstainReason::FloatNan`]) — the
//!   contains_nan gate (kept even though the crate's float order key is
//!   NaN-canonical: the charter binds the gate at lowering);
//! - a numeric constant that does not rescale EXACTLY to the column's
//!   elected scale abstains ([`AbstainReason::NumericUnrepresentable`]) —
//!   exact-rescale-or-abstain, no directed rounding at M3.

use crate::format::class::StorageClass;
use crate::format::wire::varlena_entry_at;
use crate::key::{
    bool_key, f32_key_from_datum, f64_key_from_datum, interval_cmp_key, memcmp_fixed_exact_key,
    memcmp_fixed_prefix_key, memcmp_var_prefix_key, numeric_pack_at_scale, packed_numeric_key,
    signed_word_key, timetz_utc_key, unsigned_flip_key, unsigned_small_key, KeyDerivation,
    NumericPack, TypedKey,
};
use crate::profile::MetaProfile;

/// One query constant, in the shape the caller holds it.
#[derive(Debug, Clone, Copy)]
pub enum ConstInput<'a> {
    /// Byval datum word (ints, floats, bool) — extension per spec §6.7.
    Word(u64),
    /// Fixed-length byref image (the N bytes).
    Fixed(&'a [u8]),
    /// Varlena image STARTING AT THE HEADER. Only the plain 4B-U shape is
    /// accepted (fail-closed law).
    VarlenaImage(&'a [u8]),
}

/// Typed abstention reasons (counted by callers; never an error).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AbstainReason {
    /// The varlena constant's header is not plain inline 4B-U.
    NonInlineVarlenaHeader,
    /// contains_nan: a NaN float constant blocks float pruning.
    FloatNan,
    /// Numeric does not rescale exactly to the elected scale / overflows.
    NumericUnrepresentable,
    /// Numeric NaN / ±Inf.
    NumericSpecial,
    /// Numeric wire image malformed (fail closed, never a guess).
    NumericMalformed,
    /// The column has no lowerable semantics (no key derivation and no
    /// equality bloom soundness — Opaque, nondeterministic-collation text).
    NoLowerableSemantics,
    /// The input shape does not match the column's storage class.
    ClassMismatch,
    /// A fixed image's length disagrees with the class / derivation.
    FixedLenMismatch,
}

/// A lowered constant: the kind-tagged zone key (when the column has an
/// order embedding and the constant lowered exactly) and the canonical
/// equality bytes (when the column's blooms are sound). At least one is
/// always present — otherwise lowering abstains.
#[derive(Debug, Clone, Copy)]
pub struct LoweredConst<'a> {
    pub key: Option<TypedKey>,
    eq: Option<EqBytesInner<'a>>,
}

#[derive(Debug, Clone, Copy)]
enum EqBytesInner<'a> {
    Inline { buf: [u8; 8], len: u8 },
    Slice(&'a [u8]),
}

impl<'a> LoweredConst<'a> {
    /// The §18.1 canonical bytes for bloom probing, when equality metadata
    /// is sound for this column.
    pub fn eq_bytes(&self) -> Option<&[u8]> {
        match &self.eq {
            Some(EqBytesInner::Inline { buf, len }) => Some(&buf[..*len as usize]),
            Some(EqBytesInner::Slice(s)) => Some(s),
            None => None,
        }
    }

    /// Test-and-integration constructor: a lowered constant from raw parts.
    /// (The verdict machinery treats the key's KIND as authoritative; this
    /// cannot manufacture a coarse Eq AllPass — the evaluator's coarse arm
    /// has no such verdict to return.)
    pub fn from_parts(key: Option<TypedKey>, eq_slice: Option<&'a [u8]>) -> LoweredConst<'a> {
        LoweredConst {
            key,
            eq: eq_slice.map(EqBytesInner::Slice),
        }
    }
}

/// The lowering outcome.
#[derive(Debug, Clone, Copy)]
pub enum Lowering<'a> {
    Lowered(LoweredConst<'a>),
    Abstain(AbstainReason),
}

impl<'a> Lowering<'a> {
    pub fn lowered(self) -> Option<LoweredConst<'a>> {
        match self {
            Lowering::Lowered(c) => Some(c),
            Lowering::Abstain(_) => None,
        }
    }
    pub fn abstained(self) -> Option<AbstainReason> {
        match self {
            Lowering::Lowered(_) => None,
            Lowering::Abstain(r) => Some(r),
        }
    }
}

/// Lower one constant for one column. See the module doc for the bound
/// laws; the derivation and bloomability come from the profile, so build
/// side and probe side cannot drift.
pub fn lower_const<'a>(profile: &MetaProfile, input: ConstInput<'a>) -> Lowering<'a> {
    use AbstainReason::*;
    if profile.key == KeyDerivation::None && !profile.eq_bloomable {
        return Lowering::Abstain(NoLowerableSemantics);
    }
    // Resolve the input against the storage class; produce the value view.
    enum View<'v> {
        Word(u64),
        Bytes(&'v [u8]),
    }
    let view = match (profile.class, input) {
        (
            StorageClass::ByvalWord { .. }
            | StorageClass::F32
            | StorageClass::F64
            | StorageClass::Bool,
            ConstInput::Word(w),
        ) => View::Word(w),
        (StorageClass::Fixed { len }, ConstInput::Fixed(bytes)) => {
            if bytes.len() != len as usize {
                return Lowering::Abstain(FixedLenMismatch);
            }
            View::Bytes(bytes)
        }
        (StorageClass::VarlenaVerbatim, ConstInput::VarlenaImage(image)) => {
            // FAIL-CLOSED: only the plain 4B-U header shape is read.
            match varlena_entry_at(image, 0, "const varlena") {
                Ok((_, payload)) => View::Bytes(payload),
                Err(_) => return Lowering::Abstain(NonInlineVarlenaHeader),
            }
        }
        _ => return Lowering::Abstain(ClassMismatch),
    };
    // The zone key under the column's derivation.
    let key: Option<TypedKey> = match (profile.key, &view) {
        (KeyDerivation::None, _) => None,
        (KeyDerivation::SignedWord, View::Word(w)) => Some(TypedKey::Exact(signed_word_key(*w))),
        (KeyDerivation::UnsignedWordSmall, View::Word(w)) => {
            Some(TypedKey::Exact(unsigned_small_key(*w)))
        }
        (KeyDerivation::UnsignedWordFlip, View::Word(w)) => {
            Some(TypedKey::Exact(unsigned_flip_key(*w)))
        }
        (KeyDerivation::Bool, View::Word(w)) => Some(TypedKey::Exact(bool_key(*w))),
        (KeyDerivation::Float32, View::Word(w)) => {
            if f32::from_bits(*w as u32).is_nan() {
                return Lowering::Abstain(FloatNan);
            }
            Some(TypedKey::Exact(f32_key_from_datum(*w)))
        }
        (KeyDerivation::Float64, View::Word(w)) => {
            if f64::from_bits(*w).is_nan() {
                return Lowering::Abstain(FloatNan);
            }
            Some(TypedKey::Exact(f64_key_from_datum(*w)))
        }
        (KeyDerivation::MemcmpFixed { len }, View::Bytes(b)) => {
            if b.len() != len as usize {
                return Lowering::Abstain(FixedLenMismatch);
            }
            if len <= 8 {
                Some(TypedKey::Exact(memcmp_fixed_exact_key(b)))
            } else {
                Some(TypedKey::Coarse(memcmp_fixed_prefix_key(b)))
            }
        }
        (KeyDerivation::MemcmpVarPrefix, View::Bytes(b)) => {
            Some(TypedKey::Coarse(memcmp_var_prefix_key(b)))
        }
        (KeyDerivation::PackedNumeric { scale }, View::Bytes(b)) => {
            match numeric_pack_at_scale(b, scale) {
                NumericPack::Packed(p) => Some(TypedKey::Exact(packed_numeric_key(p))),
                NumericPack::Unrepresentable => return Lowering::Abstain(NumericUnrepresentable),
                NumericPack::Special => return Lowering::Abstain(NumericSpecial),
                NumericPack::Malformed => return Lowering::Abstain(NumericMalformed),
            }
        }
        (KeyDerivation::IntervalCmpSat, View::Bytes(b)) => match interval_cmp_key(b) {
            Some(k) => Some(TypedKey::Coarse(k)),
            None => return Lowering::Abstain(FixedLenMismatch),
        },
        (KeyDerivation::TimetzUtc, View::Bytes(b)) => match timetz_utc_key(b) {
            Some(k) => Some(TypedKey::Coarse(k)),
            None => return Lowering::Abstain(FixedLenMismatch),
        },
        // Word-derivation with a byte view or vice versa cannot be built
        // through `MetaProfile::derive` + the class match above.
        _ => return Lowering::Abstain(ClassMismatch),
    };
    // Canonical equality bytes for bloom probing (spec §18.1 shapes),
    // only where the profile's lattice says byte-eq == value-eq.
    let eq = if profile.eq_bloomable {
        Some(match (&view, profile.class) {
            (View::Word(w), StorageClass::ByvalWord { width, .. }) => {
                let mut buf = [0u8; 8];
                buf.copy_from_slice(&w.to_le_bytes());
                EqBytesInner::Inline { buf, len: width }
            }
            (View::Word(w), StorageClass::F32) => {
                let mut buf = [0u8; 8];
                buf.copy_from_slice(&w.to_le_bytes());
                EqBytesInner::Inline { buf, len: 4 }
            }
            (View::Word(w), StorageClass::F64) => {
                let mut buf = [0u8; 8];
                buf.copy_from_slice(&w.to_le_bytes());
                EqBytesInner::Inline { buf, len: 8 }
            }
            (View::Word(w), StorageClass::Bool) => {
                let mut buf = [0u8; 8];
                buf[0] = (*w != 0) as u8;
                EqBytesInner::Inline { buf, len: 1 }
            }
            (View::Bytes(b), _) => EqBytesInner::Slice(b),
            (View::Word(_), _) => return Lowering::Abstain(ClassMismatch),
        })
    } else {
        None
    };
    if key.is_none() && eq.is_none() {
        return Lowering::Abstain(NoLowerableSemantics);
    }
    Lowering::Lowered(LoweredConst { key, eq })
}

/// Datum-word adapter for pointer classes.
///
/// # Safety
///
/// `datum` must obey the PG datum contract for `class`: for `Fixed{len}` it
/// points at `len` readable bytes; for `VarlenaVerbatim` it points at a
/// varlena image whose header is readable and, when the header is plain
/// 4B-U declaring total length L, at L readable bytes. Non-4B-U headers are
/// NOT dereferenced past the first byte (the fail-closed law) — the
/// function abstains instead. The returned input borrows the pointed-at
/// memory for `'a`, which the caller must guarantee outlives the lowering.
pub unsafe fn const_input_from_datum<'a>(
    class: StorageClass,
    datum: u64,
) -> Result<ConstInput<'a>, AbstainReason> {
    match class {
        StorageClass::ByvalWord { .. }
        | StorageClass::F32
        | StorageClass::F64
        | StorageClass::Bool => Ok(ConstInput::Word(datum)),
        StorageClass::Fixed { len } => {
            // SAFETY: caller contract — `len` readable bytes.
            let s = unsafe { core::slice::from_raw_parts(datum as *const u8, len as usize) };
            Ok(ConstInput::Fixed(s))
        }
        StorageClass::VarlenaVerbatim => {
            let p = datum as *const u8;
            // SAFETY: caller contract — the first header byte is readable.
            let b0 = unsafe { *p };
            if b0 & 0b11 != 0 {
                // 1B / compressed / external header: fail closed without
                // reading further.
                return Err(AbstainReason::NonInlineVarlenaHeader);
            }
            // SAFETY: caller contract — a 4B-U header's 4 bytes are
            // readable.
            let header = u32::from_le_bytes(
                unsafe { core::slice::from_raw_parts(p, 4) }
                    .try_into()
                    .expect("len 4"),
            );
            let total = (header >> 2) as usize;
            if total < 4 {
                return Err(AbstainReason::NonInlineVarlenaHeader);
            }
            // SAFETY: caller contract — the declared image is readable.
            let s = unsafe { core::slice::from_raw_parts(p, total) };
            Ok(ConstInput::VarlenaImage(s))
        }
    }
}
