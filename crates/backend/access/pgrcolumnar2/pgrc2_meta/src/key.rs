//! The universal signed-i64 order-embedded zone-key plane (charter §5,
//! spec §8.1) — every transform reachable from one file, with the
//! **coarse-key law carried in the types**. The float and memcmp-prefix
//! transform DEFINITIONS live in the shared `order_key` support crate
//! (extracted verbatim per the 2026-08-08 ruling); this file wraps their
//! raw i64 keys in the law types and keeps the format-coupled derivations
//! (interval/timetz/numeric wire-image parsing) local.
//!
//! ## The law (spec §8.1, verbatim)
//!
//! > `Exact` keys may prove Eq AllPass; `Coarse` keys (prefix/saturating
//! > embeds) prove range verdicts only.
//!
//! Conflating the two is a wrong-results class (a coarse Eq AllPass returns
//! rows that do not match). Enforcement, strongest form first:
//!
//! 1. **By type**: [`ExactKey`] and [`CoarseKey`] are distinct types.
//!    Coarse Eq evaluation ([`crate::verdict::coarse_eq`]) takes
//!    [`CoarseKey`]s and returns [`crate::verdict::CoarseEqVerdict`] — an
//!    enum with no AllPass variant. There is no function from coarse keys to
//!    an Eq AllPass; the wrong program does not compile.
//! 2. **By released assert**: every verdict leaves through
//!    [`crate::verdict::finalize`], which `assert!`s (release-effective)
//!    that no eq-flavored probe over a coarse key claims AllPass.
//!
//! ## Semantics of the two kinds
//!
//! - **Exact** (`k`): order-isomorphic AND equality-faithful w.r.t. the
//!   type's comparison semantics: `a < b ⇔ k(a) < k(b)` and
//!   `a = b ⇔ k(a) = k(b)` (`=` is the TYPE's equality: float ±0.0
//!   collapses, every NaN collapses — exactly PG's `float8_cmp` classes).
//! - **Coarse** (`c`): monotone-with-ties: `a ≤ b ⇒ c(a) ≤ c(b)`.
//!   Contrapositive: `c(a) < c(b) ⇒ a < b` — STRICT key inequalities prove
//!   strict value inequalities, which is why coarse range verdicts exist at
//!   all; ties prove nothing (prefix collisions, saturated tails).
//!
//! Per-class derivations are the charter §5/§7 rows; each transform's doc
//! states its class and its exactness argument.

use crate::format::meta::KeyKind;
use crate::format::wire::Cur;

/// An exact zone key: order-isomorphic and equality-faithful (may prove Eq
/// AllPass). Constructed only by the transforms in this file (and by tests
/// through them); the raw i64 is extractable for record encoding, but there
/// is deliberately no `From<i64>` — a record read back from disk re-enters
/// the typed world through [`TypedKeys::from_record_parts`], which consults
/// the record's own `key_kind` witness.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ExactKey(i64);

/// A coarse zone key: monotone-with-ties (range verdicts only — prefix and
/// saturating embeds live here).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct CoarseKey(i64);

impl ExactKey {
    #[inline]
    pub const fn raw(self) -> i64 {
        self.0
    }
}

impl CoarseKey {
    #[inline]
    pub const fn raw(self) -> i64 {
        self.0
    }
}

/// One kind-tagged key — what constant lowering produces and what a
/// derivation yields per value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TypedKey {
    Exact(ExactKey),
    Coarse(CoarseKey),
}

impl TypedKey {
    #[inline]
    pub const fn raw(self) -> i64 {
        match self {
            TypedKey::Exact(k) => k.raw(),
            TypedKey::Coarse(k) => k.raw(),
        }
    }
}

/// One derived key (or the typed absence of one). `Absent` is per-VALUE:
/// e.g. a numeric that does not rescale exactly poisons its granule's keys
/// (the builder degrades the whole grain to `KeyKind::Absent`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DerivedKey {
    Exact(ExactKey),
    Coarse(CoarseKey),
    Absent,
}

impl DerivedKey {
    /// The kind-tagged key, when one was derived.
    #[inline]
    pub fn typed(self) -> Option<TypedKey> {
        match self {
            DerivedKey::Exact(k) => Some(TypedKey::Exact(k)),
            DerivedKey::Coarse(k) => Some(TypedKey::Coarse(k)),
            DerivedKey::Absent => None,
        }
    }
}

/// A min/max key pair of one kind — what a sealed grain carries. The kind is
/// carried once for the pair (a grain never mixes kinds; the derivation is
/// per-column).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TypedKeys {
    Exact { min: ExactKey, max: ExactKey },
    Coarse { min: CoarseKey, max: CoarseKey },
    Absent,
}

impl TypedKeys {
    /// The record fields `(min_key, max_key, key_kind)` (spec §8.1).
    pub fn record_parts(&self) -> (i64, i64, KeyKind) {
        match *self {
            TypedKeys::Exact { min, max } => (min.0, max.0, KeyKind::Exact),
            TypedKeys::Coarse { min, max } => (min.0, max.0, KeyKind::Coarse),
            TypedKeys::Absent => (0, 0, KeyKind::Absent),
        }
    }

    /// Re-enter the typed world from record fields. The record's own
    /// `key_kind` byte is the witness; `EnumRankReserved` (the O-5 slot,
    /// never written at M3) reads back as `Absent` — conservative, and
    /// sound either way O-5's transcription lands.
    pub fn from_record_parts(min_key: i64, max_key: i64, key_kind: KeyKind) -> TypedKeys {
        match key_kind {
            KeyKind::Exact => TypedKeys::Exact {
                min: ExactKey(min_key),
                max: ExactKey(max_key),
            },
            KeyKind::Coarse => TypedKeys::Coarse {
                min: CoarseKey(min_key),
                max: CoarseKey(max_key),
            },
            KeyKind::Absent | KeyKind::EnumRankReserved => TypedKeys::Absent,
        }
    }
}

/// The per-column key derivation (charter §5/§7 rows). Selected by
/// [`crate::profile::MetaProfile::derive`]; the SAME derivation serves the
/// build side (builder) and the probe side (constant lowering) — one
/// definition, one drift surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyDerivation {
    /// No order embedding (non-C text order, jsonb-as-value, composites,
    /// extension types, unpacked numeric, v1 enums): validity + counts only.
    None,
    /// Sign-extended byval word (int2/4/8, date, time, timestamp[tz],
    /// money, "char"-as-signed): the datum word IS the key. Exact.
    SignedWord,
    /// Zero-extended byval word, width ≤ 4 (oid, regclass-family): fits i64
    /// directly. Exact.
    UnsignedWordSmall,
    /// Full-range unsigned 8-byte word (pg_lsn, xid8): sign-flip embed
    /// (`charter §5 "pg_lsn-flipped"`). Exact.
    UnsignedWordFlip,
    /// bool: false=0 < true=1 (PG bool ordering). Exact.
    Bool,
    /// float4 via exact widening to f64, then the canonical order key.
    /// Exact (see [`f64_order_key`]).
    Float32,
    /// float8 canonical order key. Exact (see [`f64_order_key`]).
    Float64,
    /// Fixed-length byref whose semantic order IS memcmp (macaddr 6,
    /// macaddr8 8, uuid 16). `len ≤ 8` embeds injectively → Exact;
    /// `len > 8` is an 8-byte prefix → Coarse (uuid — charter §7).
    MemcmpFixed { len: u32 },
    /// Varlena whose semantic order is memcmp of the payload (bytea
    /// unconditionally — "memcmp IS semantic order"; text only under
    /// collation-class C, gated by the profile): 8-byte payload prefix,
    /// zero-padded. Coarse always (variable length ⇒ prefix ties).
    MemcmpVarPrefix,
    /// numeric elected to the A6b fixed-scale i64 lane: the packed value at
    /// `scale` IS the key. Exact when every value rescales exactly; a value
    /// that does not poisons the grain to Absent (the builder's degrade
    /// rule, coupled to sum validity — see `profile`).
    PackedNumeric { scale: i32 },
    /// interval: PG `interval_cmp_value` (i128 usec span) saturated into
    /// i64. Coarse (saturating embed — spec §8.1 names these coarse).
    IntervalCmpSat,
    /// timetz: PG `timetz_cmp_internal` primary key
    /// `time + zone × USECS_PER_SEC`; the zone tiebreak is dropped, so the
    /// embed has ties across distinct values. Coarse (charter §5).
    TimetzUtc,
}

impl KeyDerivation {
    /// The key kind this derivation produces (when it produces one).
    pub fn kind(&self) -> KeyKind {
        match self {
            KeyDerivation::None => KeyKind::Absent,
            KeyDerivation::SignedWord
            | KeyDerivation::UnsignedWordSmall
            | KeyDerivation::UnsignedWordFlip
            | KeyDerivation::Bool
            | KeyDerivation::Float32
            | KeyDerivation::Float64
            | KeyDerivation::PackedNumeric { .. } => KeyKind::Exact,
            KeyDerivation::MemcmpFixed { len } => {
                if *len <= 8 {
                    KeyKind::Exact
                } else {
                    KeyKind::Coarse
                }
            }
            KeyDerivation::MemcmpVarPrefix
            | KeyDerivation::IntervalCmpSat
            | KeyDerivation::TimetzUtc => KeyKind::Coarse,
        }
    }

    /// The VALUE-FAITHFUL law (metaagg footer answers, Ruling 4 item 12):
    /// a derivation is value-faithful when the key↔value map is a byte-level
    /// bijection on datum words — inverting a key reproduces the ONE datum
    /// word every row carrying that key value holds, so a metadata-answered
    /// MIN/MAX is byte-identical to the scan's pick regardless of row order.
    ///
    /// Exactness (order/equality-faithful) is NOT enough:
    /// - [`KeyDerivation::Float32`]/[`Float64`] are Exact but collapse PG
    ///   equality CLASSES that span byte images (±0.0; every NaN payload) —
    ///   the scan's `float8smaller` keeps the first-encountered member, an
    ///   order-dependent byte choice no key can reproduce. NOT faithful.
    /// - [`KeyDerivation::PackedNumeric`] keys collapse display scales
    ///   (`1.0` and `1.00` share a key; `MIN(numeric)` returns one of them,
    ///   dscale and all). NOT faithful.
    /// - [`KeyDerivation::MemcmpFixed`] `len ≤ 8` is injective (invertible
    ///   in principle) but byref — reconstruction is out of scope while no
    ///   admitted aggregate consumes it. NOT faithful (conservative).
    ///
    /// Faithful: the byval word classes, where ties are identical datums.
    pub fn value_faithful(&self) -> bool {
        matches!(
            self,
            KeyDerivation::SignedWord
                | KeyDerivation::UnsignedWordSmall
                | KeyDerivation::UnsignedWordFlip
                | KeyDerivation::Bool
        )
    }

    /// Invert one EXACT key back to the datum word it embeds — defined only
    /// for [`KeyDerivation::value_faithful`] derivations; `None` for every
    /// other derivation AND for a key outside the derivation's image (a
    /// foreign or corrupt record — consult nothing, the caller declines).
    ///
    /// Inverses (each transform's doc carries the forward direction):
    /// - `SignedWord`: the sign-extended datum word IS the key.
    /// - `UnsignedWordSmall`: identity on `0..=u32::MAX` (width ≤ 4).
    /// - `UnsignedWordFlip`: the sign-bit flip is an involution.
    /// - `Bool`: 0/1 words.
    pub fn exact_key_to_datum_word(&self, key: i64) -> Option<u64> {
        match self {
            KeyDerivation::SignedWord => Some(key as u64),
            KeyDerivation::UnsignedWordSmall => {
                if (0..=u32::MAX as i64).contains(&key) {
                    Some(key as u64)
                } else {
                    None
                }
            }
            KeyDerivation::UnsignedWordFlip => Some((key as u64) ^ (1u64 << 63)),
            KeyDerivation::Bool => {
                if key == 0 || key == 1 {
                    Some(key as u64)
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// word-class transforms
// ---------------------------------------------------------------------------

/// Signed byval word → exact key. The datum word arrives sign-extended
/// (spec §6.7's decode convention; the writer builds inputs the same way),
/// so the word IS the key.
#[inline]
pub fn signed_word_key(datum: u64) -> ExactKey {
    ExactKey(datum as i64)
}

/// Zero-extended unsigned word of width ≤ 4 → exact key (value ≤ u32::MAX
/// fits i64 directly; order preserved).
#[inline]
pub fn unsigned_small_key(datum: u64) -> ExactKey {
    debug_assert!(datum <= u32::MAX as u64, "UnsignedWordSmall is width ≤ 4");
    ExactKey(datum as i64)
}

/// Full-range u64 → exact key by sign-bit flip: `k = (v ^ 2^63) as i64`.
/// Order-isomorphic on the full u64 domain (the charter's "pg_lsn-flipped").
#[inline]
pub fn unsigned_flip_key(datum: u64) -> ExactKey {
    ExactKey((datum ^ (1u64 << 63)) as i64)
}

/// bool → exact key 0/1.
#[inline]
pub fn bool_key(datum: u64) -> ExactKey {
    ExactKey((datum != 0) as i64)
}

// ---------------------------------------------------------------------------
// float order keys (charter §5 "the canonical order-key transform")
// ---------------------------------------------------------------------------
//
// The transform DEFINITIONS live in the shared `order_key` support crate
// (extracted verbatim from this file per the 2026-08-08 ruling — the M3-B
// vendoring charter fulfilled; the exactness arguments travel with them).
// This file wraps the raw i64 keys in the coarse-key-law types: the law
// wraps the functions at the consumer.

/// The canonical f64 order key: a true EXACT key under PG `float8_cmp`
/// semantics (±0 and all NaNs collapse to their PG equality classes; NaN
/// orders greatest). Definition + exactness argument:
/// [`order_key::transform::f64_order_key`].
#[inline]
pub fn f64_order_key(v: f64) -> ExactKey {
    ExactKey(order_key::transform::f64_order_key(v))
}

/// f32 → exact key via exact (injective, monotone) widening to f64.
#[inline]
pub fn f32_order_key(v: f32) -> ExactKey {
    ExactKey(order_key::transform::f32_order_key(v))
}

/// The f64 key of a datum word carrying IEEE bits in its low 8 bytes
/// (spec §6.7's F64 convention).
#[inline]
pub fn f64_key_from_datum(datum: u64) -> ExactKey {
    ExactKey(order_key::transform::f64_key_from_datum(datum))
}

/// The f32 key of a datum word carrying IEEE bits in its low 4 bytes.
#[inline]
pub fn f32_key_from_datum(datum: u64) -> ExactKey {
    ExactKey(order_key::transform::f32_key_from_datum(datum))
}

// ---------------------------------------------------------------------------
// memcmp-order transforms (fixed images, prefixes)
// ---------------------------------------------------------------------------

// Definitions in the shared `order_key` crate (the one big-endian
// prefix embed, `order_key::transform::be_prefix_embed`, serves all
// three); this file assigns the key KIND per the coarse-key law.

/// Fixed-length memcmp-ordered image, `len ≤ 8` (macaddr, macaddr8): the
/// whole image embeds — injective + order-isomorphic → Exact.
#[inline]
pub fn memcmp_fixed_exact_key(image: &[u8]) -> ExactKey {
    ExactKey(order_key::transform::memcmp_fixed_exact_key(image))
}

/// Fixed-length memcmp-ordered image, `len > 8` (uuid): 8-byte prefix →
/// Coarse (distinct values can tie).
#[inline]
pub fn memcmp_fixed_prefix_key(image: &[u8]) -> CoarseKey {
    CoarseKey(order_key::transform::memcmp_fixed_prefix_key(image))
}

/// Varlena payload prefix (bytea always; text only under collation-class C,
/// the profile's gate): 8-byte zero-padded prefix → Coarse. Zero-padding is
/// monotone-with-ties for memcmp-with-length order: a string and its
/// extensions may tie, and ties prove nothing — exactly the coarse
/// contract.
#[inline]
pub fn memcmp_var_prefix_key(payload: &[u8]) -> CoarseKey {
    CoarseKey(order_key::transform::memcmp_var_prefix_key(payload))
}

// ---------------------------------------------------------------------------
// interval / timetz (fixed byref, derived keys — charter §7)
// ---------------------------------------------------------------------------

/// PG's `USECS_PER_DAY`.
const USECS_PER_DAY: i128 = 86_400_000_000;
/// PG's `DAYS_PER_MONTH` as used by `interval_cmp_value` (30).
const DAYS_PER_MONTH: i128 = 30;

/// interval (16-byte image: time i64 LE usec, day i32 LE, month i32 LE):
/// PG `interval_cmp_value` span
/// `time + month × 30 × USECS_PER_DAY + day × USECS_PER_DAY` (an i128),
/// saturated into i64 → Coarse (saturating embed; also PG interval equality
/// is span equality — `1 month = 30 days` — so byte-level identity is NOT
/// value identity, which independently forbids Exact treatment of images).
/// Returns None on a malformed (short) image — the caller degrades to
/// Absent (fail closed, never a panic).
pub fn interval_cmp_key(image: &[u8]) -> Option<CoarseKey> {
    if image.len() != 16 {
        return None;
    }
    let mut c = Cur::new(image);
    let time = c.i64("interval image").ok()?;
    let day = c.u32("interval image").ok()? as i32;
    let month = c.u32("interval image").ok()? as i32;
    let span: i128 = time as i128
        + (month as i128) * DAYS_PER_MONTH * USECS_PER_DAY
        + (day as i128) * USECS_PER_DAY;
    Some(CoarseKey(
        span.clamp(i64::MIN as i128, i64::MAX as i128) as i64
    ))
}

/// timetz (12-byte image: time i64 LE usec, zone i32 LE seconds): PG
/// `timetz_cmp_internal`'s primary comparand `time + zone × 1_000_000`.
/// The zone tiebreak is dropped, so distinct values can tie → Coarse.
/// Returns None on a malformed image.
pub fn timetz_utc_key(image: &[u8]) -> Option<CoarseKey> {
    if image.len() != 12 {
        return None;
    }
    let mut c = Cur::new(image);
    let time = c.i64("timetz image").ok()?;
    let zone = c.u32("timetz image").ok()? as i32;
    Some(CoarseKey(
        time.saturating_add((zone as i64).saturating_mul(1_000_000)),
    ))
}

// ---------------------------------------------------------------------------
// packed numeric (A6b fixed-scale lane; exact-rescale-or-abstain)
// ---------------------------------------------------------------------------

/// The outcome of lowering one numeric wire image to the elected fixed
/// scale. Everything that is not `Packed` is a typed abstention: the
/// builder degrades the grain to Absent; constant lowering abstains
/// (fail closed — never a panic, never a guess).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NumericPack {
    /// `value × 10^scale`, exactly representable in i64.
    Packed(i64),
    /// The value does not rescale exactly (has more fractional digits than
    /// `scale`) or overflows i64 at scale.
    Unrepresentable,
    /// NaN / +Inf / -Inf (PG numeric specials).
    Special,
    /// The image is not a well-formed numeric payload (truncated header or
    /// digits, digit out of base range). Fail-closed abstention.
    Malformed,
}

const NUMERIC_SIGN_MASK: u16 = 0xC000;
const NUMERIC_NEG: u16 = 0x4000;
const NUMERIC_SHORT: u16 = 0x8000;
const NUMERIC_SPECIAL: u16 = 0xC000;
const NUMERIC_SHORT_SIGN_MASK: u16 = 0x2000;
const NUMERIC_SHORT_WEIGHT_SIGN_MASK: u16 = 0x0040;
const NUMERIC_SHORT_WEIGHT_MASK: u16 = 0x003F;
const NBASE: i128 = 10_000;

/// Parse a numeric VARLENA PAYLOAD (header already stripped — the §18.1
/// canonical-bytes form) and rescale it to `scale` exactly. Base-10000
/// digit-walk with checked i128 arithmetic throughout; every failure is a
/// typed [`NumericPack`] outcome, never a panic (the wire image is
/// CRC-validated but this code still refuses garbage — the #66/#340 law).
pub fn numeric_pack_at_scale(payload: &[u8], scale: i32) -> NumericPack {
    let mut c = Cur::new(payload);
    let Ok(header) = c.u16("numeric header") else {
        return NumericPack::Malformed;
    };
    let (neg, weight): (bool, i32) = if header & NUMERIC_SIGN_MASK == NUMERIC_SPECIAL {
        return NumericPack::Special;
    } else if header & NUMERIC_SHORT != 0 {
        // Short form: 7-bit two's-complement weight in the header word.
        let w = (header & NUMERIC_SHORT_WEIGHT_MASK) as i32;
        let w = if header & NUMERIC_SHORT_WEIGHT_SIGN_MASK != 0 {
            w | !(NUMERIC_SHORT_WEIGHT_MASK as i32)
        } else {
            w
        };
        (header & NUMERIC_SHORT_SIGN_MASK != 0, w)
    } else {
        // Long form: 16-bit weight follows the sign/dscale word.
        let Ok(w) = c.u16("numeric weight") else {
            return NumericPack::Malformed;
        };
        (header & NUMERIC_SIGN_MASK == NUMERIC_NEG, w as i16 as i32)
    };
    if c.remaining() % 2 != 0 {
        return NumericPack::Malformed;
    }
    let ndigits = (c.remaining() / 2) as i32;
    // Digit-walk: acc = Σ digit_i × NBASE^(ndigits-1-i), all checked.
    let mut acc: i128 = 0;
    for _ in 0..ndigits {
        let Ok(d) = c.u16("numeric digit") else {
            return NumericPack::Malformed;
        };
        if d as i128 >= NBASE {
            return NumericPack::Malformed;
        }
        let Some(shifted) = acc.checked_mul(NBASE) else {
            return NumericPack::Unrepresentable;
        };
        let Some(next) = shifted.checked_add(d as i128) else {
            return NumericPack::Unrepresentable;
        };
        acc = next;
    }
    // value = acc × 10^(4 × (weight + 1 − ndigits)); target = value × 10^scale.
    let e = 4i64 * (weight as i64 + 1 - ndigits as i64) + scale as i64;
    let packed = if e >= 0 {
        match checked_pow10(e) {
            Some(p) => match acc.checked_mul(p) {
                Some(v) => v,
                None => return NumericPack::Unrepresentable,
            },
            // 10^e overflowed i128; acc == 0 is still exactly 0.
            None if acc == 0 => 0,
            None => return NumericPack::Unrepresentable,
        }
    } else {
        match checked_pow10(-e) {
            Some(p) => {
                if acc % p != 0 {
                    return NumericPack::Unrepresentable;
                }
                acc / p
            }
            // Dividing by a super-i128 power: only exact for 0.
            None if acc == 0 => 0,
            None => return NumericPack::Unrepresentable,
        }
    };
    let signed = if neg { -packed } else { packed };
    if signed < i64::MIN as i128 || signed > i64::MAX as i128 {
        return NumericPack::Unrepresentable;
    }
    NumericPack::Packed(signed as i64)
}

fn checked_pow10(e: i64) -> Option<i128> {
    if e < 0 || e > 38 {
        return None;
    }
    let mut p: i128 = 1;
    for _ in 0..e {
        p = p.checked_mul(10)?;
    }
    Some(p)
}

/// Packed numeric value → exact key (the packed i64 at the shared scale IS
/// the order embedding — charter §5).
#[inline]
pub fn packed_numeric_key(packed: i64) -> ExactKey {
    ExactKey(packed)
}
