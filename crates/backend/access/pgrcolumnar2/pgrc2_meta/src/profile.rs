//! The per-column meta policy — WHICH stats the builder computes and which
//! evidence the prober may consult. The profile is the **measured-only
//! law's anchor**: verdict evaluation consults a `StatsRecord` field only
//! when the profile declares the builder computed it (plus, for keys, the
//! record's own `key_kind` witness).
//!
//! Derivation input is catalog facts + a TYPE-SEMANTICS tag. The tag is
//! deliberately a value the caller supplies (M3-H's AM maps types when it
//! wires DDL): storage classes come from catalog properties (never an OID
//! table — charter §3), but zone/equality semantics (is memcmp the order?
//! is this an interval image?) are type facts a catalog property cannot
//! carry. Inconsistent (class, semantics) pairs are typed errors, the
//! `BadClassHint` pattern (spec §3).
//!
//! ## The collation gate (charter §5; the StrView §6 law)
//!
//! Text ORDER metadata exists only under collation-class C (byte order ==
//! collation order): `TextCollated` lowers to prefix keys ONLY for
//! [`CollationClass::C`]. Other deterministic collations keep EQUALITY
//! metadata (blooms — deterministic collations compare equal iff bytes are
//! equal) but no keys and therefore no order verdicts and no sortedness.
//! Nondeterministic collations get neither keys nor blooms: counts and
//! length stats only.
//!
//! ## The equality-bloomability lattice (charter §6's publishability
//! pattern, applied to blooms)
//!
//! A bloom proves Eq AllFail from canonical-byte absence, so arming
//! requires **byte-eq == value-eq**:
//!
//! | semantics | bloomable | why not |
//! |---|---|---|
//! | signed/unsigned ints, bool | yes | |
//! | memcmp-ordered (uuid, macaddr, bytea) | yes | |
//! | text, deterministic collation (incl. C) | yes | |
//! | text, nondeterministic | no | value-eq ≠ byte-eq |
//! | float | no | ±0.0 / NaN classes span byte images |
//! | numeric (packed or not) | no | 1.0 vs 1.00 |
//! | interval | no | 1 month == 30 days |
//! | timetz | yes | eq ⇔ (time, zone) eq ⇔ image eq |
//! | enum (eq-only) | yes | oid word eq is value eq |
//! | opaque (jsonb-as-value, json, composites, extension) | no | semantics unknowable |

use crate::format::class::{CollationClass, StorageClass};
use crate::format::{FormatError, FormatResult};
use crate::key::KeyDerivation;

/// Type-semantics tag (see module doc). Supplied per column by the seal
/// driver; the profile validates it against the storage class.
///
/// MOVED (metadata-plane wiring lane): the tag now lives on
/// [`crate::format::class::ColSchema`], the per-column descriptor the writer
/// already carries to every seal site — that is what lets `pgrc2_write`
/// build a real profile per stream without inventing a parallel plumbing
/// channel. Re-exported here so every M3-E path (`profile::TypeSemantics`)
/// keeps its spelling; the variants are unchanged.
pub use crate::format::class::TypeSemantics;

/// Which length statistics the builder computes (spec §8.1 carries BOTH
/// units — the #80 lesson; char lengths are UTF-8 code points and exist
/// only for text).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LenStats {
    None,
    Bytes,
    BytesAndChars,
}

/// Which SUM the builder accumulates into `sum_i128` (spec §8.1: "numeric
/// at shared scale, ints" — floats never).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SumKind {
    None,
    /// Sign-extended datum words.
    SignedWord,
    /// Zero-extended datum words (u64 magnitudes for width 8).
    UnsignedWord,
    /// Count of `true`.
    BoolTrues,
    /// Sum of the packed i64 lane values at the elected scale. VALIDITY IS
    /// COUPLED TO THE KEY: a value that fails exact rescale poisons the
    /// grain to `key_kind = Absent`, and consumers must treat `sum_i128`
    /// (and `zero_count`) as uncomputed for such grains — the record has no
    /// per-field mask (frozen layout), so the key kind is the shared
    /// witness. [`crate::verdict`] enforces this coupling.
    PackedI64,
}

/// Which zero-count the builder computes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ZeroKind {
    None,
    /// Datum word == 0.
    Word,
    /// Float value == 0.0 (either zero).
    Float,
    /// bool false.
    BoolFalse,
    /// Packed lane value == 0 (validity coupled to the key, as for
    /// [`SumKind::PackedI64`]).
    PackedI64,
}

/// The per-column meta policy. Constructed only by [`MetaProfile::derive`]
/// (typed refusal on inconsistent inputs), consumed by the builder (what to
/// compute) and the verdict evaluator (what may be consulted).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetaProfile {
    pub class: StorageClass,
    pub collation: CollationClass,
    pub key: KeyDerivation,
    /// Equality blooms may be built/consulted (byte-eq == value-eq).
    pub eq_bloomable: bool,
    pub len_stats: LenStats,
    pub sum: SumKind,
    pub zero: ZeroKind,
    /// NDV registers + estimates are computed.
    pub ndv: bool,
}

impl MetaProfile {
    /// Derive the policy from (storage class, collation class, semantics).
    /// Inconsistent combinations are typed errors — the spec §3
    /// `BadClassHint` pattern, reused verbatim for the meta plane.
    pub fn derive(
        class: StorageClass,
        collation: CollationClass,
        semantics: TypeSemantics,
    ) -> FormatResult<MetaProfile> {
        let bad = |detail: &'static str| Err(FormatError::BadClassHint { detail });
        let p = |key, eq_bloomable, len_stats, sum, zero, ndv| {
            Ok(MetaProfile {
                class,
                collation,
                key,
                eq_bloomable,
                len_stats,
                sum,
                zero,
                ndv,
            })
        };
        match semantics {
            TypeSemantics::Opaque => {
                let len = if matches!(class, StorageClass::VarlenaVerbatim) {
                    LenStats::Bytes
                } else {
                    LenStats::None
                };
                p(
                    KeyDerivation::None,
                    false,
                    len,
                    SumKind::None,
                    ZeroKind::None,
                    false,
                )
            }
            TypeSemantics::SignedInt => match class {
                StorageClass::ByvalWord { signed: true, .. } => p(
                    KeyDerivation::SignedWord,
                    true,
                    LenStats::None,
                    SumKind::SignedWord,
                    ZeroKind::Word,
                    true,
                ),
                _ => bad("SignedInt semantics require a signed byval word class"),
            },
            TypeSemantics::UnsignedInt => match class {
                StorageClass::ByvalWord {
                    signed: false,
                    width,
                } => p(
                    if width == 8 {
                        KeyDerivation::UnsignedWordFlip
                    } else {
                        KeyDerivation::UnsignedWordSmall
                    },
                    true,
                    LenStats::None,
                    SumKind::UnsignedWord,
                    ZeroKind::Word,
                    true,
                ),
                _ => bad("UnsignedInt semantics require an unsigned byval word class"),
            },
            TypeSemantics::Float => match class {
                StorageClass::F32 => p(
                    KeyDerivation::Float32,
                    false,
                    LenStats::None,
                    SumKind::None,
                    ZeroKind::Float,
                    true,
                ),
                StorageClass::F64 => p(
                    KeyDerivation::Float64,
                    false,
                    LenStats::None,
                    SumKind::None,
                    ZeroKind::Float,
                    true,
                ),
                _ => bad("Float semantics require class F32 or F64"),
            },
            TypeSemantics::Bool => match class {
                StorageClass::Bool => p(
                    KeyDerivation::Bool,
                    true,
                    LenStats::None,
                    SumKind::BoolTrues,
                    ZeroKind::BoolFalse,
                    true,
                ),
                _ => bad("Bool semantics require class Bool"),
            },
            TypeSemantics::MemcmpOrdered => match class {
                StorageClass::Fixed { len } => p(
                    KeyDerivation::MemcmpFixed { len },
                    true,
                    LenStats::None,
                    SumKind::None,
                    ZeroKind::None,
                    true,
                ),
                StorageClass::VarlenaVerbatim => p(
                    KeyDerivation::MemcmpVarPrefix,
                    true,
                    LenStats::Bytes,
                    SumKind::None,
                    ZeroKind::None,
                    true,
                ),
                _ => bad("MemcmpOrdered semantics require Fixed or VarlenaVerbatim"),
            },
            TypeSemantics::TextCollated => match class {
                StorageClass::VarlenaVerbatim => {
                    // THE COLLATION GATE (module doc): keys only under C;
                    // blooms only under deterministic collations.
                    let key = if collation == CollationClass::C {
                        KeyDerivation::MemcmpVarPrefix
                    } else {
                        KeyDerivation::None
                    };
                    let bloomable = collation != CollationClass::Nondeterministic;
                    p(
                        key,
                        bloomable,
                        LenStats::BytesAndChars,
                        SumKind::None,
                        ZeroKind::None,
                        bloomable,
                    )
                }
                _ => bad("TextCollated semantics require VarlenaVerbatim"),
            },
            TypeSemantics::PackedNumeric { scale } => match class {
                StorageClass::VarlenaVerbatim => p(
                    KeyDerivation::PackedNumeric { scale },
                    false,
                    LenStats::Bytes,
                    SumKind::PackedI64,
                    ZeroKind::PackedI64,
                    true,
                ),
                _ => bad("PackedNumeric semantics require VarlenaVerbatim"),
            },
            TypeSemantics::NumericUnpacked => match class {
                StorageClass::VarlenaVerbatim => p(
                    KeyDerivation::None,
                    false,
                    LenStats::Bytes,
                    SumKind::None,
                    ZeroKind::None,
                    true,
                ),
                _ => bad("NumericUnpacked semantics require VarlenaVerbatim"),
            },
            TypeSemantics::IntervalCmp => match class {
                StorageClass::Fixed { len: 16 } => p(
                    KeyDerivation::IntervalCmpSat,
                    false,
                    LenStats::None,
                    SumKind::None,
                    ZeroKind::None,
                    true,
                ),
                _ => bad("IntervalCmp semantics require Fixed{16}"),
            },
            TypeSemantics::TimetzUtc => match class {
                StorageClass::Fixed { len: 12 } => p(
                    KeyDerivation::TimetzUtc,
                    true,
                    LenStats::None,
                    SumKind::None,
                    ZeroKind::None,
                    true,
                ),
                _ => bad("TimetzUtc semantics require Fixed{12}"),
            },
            TypeSemantics::EnumEqOnly => match class {
                StorageClass::ByvalWord { width: 4, .. } => p(
                    KeyDerivation::None,
                    true,
                    LenStats::None,
                    SumKind::None,
                    ZeroKind::None,
                    true,
                ),
                _ => bad("EnumEqOnly semantics require a 4-byte byval word (oid)"),
            },
        }
    }

    /// [`MetaProfile::derive`] with the DEGRADE rule applied: an
    /// inconsistent `(class, semantics)` pair falls back to the `Opaque`
    /// profile for that class instead of refusing.
    ///
    /// THIS IS THE ONE FUNCTION BOTH SIDES MUST CALL. The measured-only law
    /// says a prober consults a `StatsRecord` field only when the profile
    /// declares the builder computed it — which is sound only if the SEAL
    /// side and the PROBE side derive the SAME profile from the same column
    /// facts. If the writer degraded a mislabelled column to `Opaque` and a
    /// prober independently derived the un-degraded profile, the prober
    /// would consult aggregates that were never computed and read zeros as
    /// facts. So the fallback lives here, in the crate both sides depend on,
    /// and neither `pgrc2_write` nor the scan build reimplements it.
    ///
    /// Total: the `Opaque` arm inspects only the storage class and accepts
    /// every one of the six, so this cannot fail.
    pub fn derive_or_opaque(
        class: StorageClass,
        collation: CollationClass,
        semantics: TypeSemantics,
    ) -> MetaProfile {
        MetaProfile::derive(class, collation, semantics).unwrap_or_else(|_| {
            MetaProfile::derive(class, collation, TypeSemantics::Opaque)
                .expect("the Opaque profile arm is total over every storage class")
        })
    }

    /// True when the builder buffers per-value canonical bytes for
    /// equality machinery (bloom insert + NDV hashing).
    pub fn hashes_values(&self) -> bool {
        self.eq_bloomable || self.ndv
    }
}
