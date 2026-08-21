//! Storage classes (spec §3, charter §3): every column maps by CATALOG
//! PROPERTIES (typlen/typbyval/typalign) onto one of six classes — never an
//! OID table. The refinement hint (float/bool/signed) is ingest-side election
//! input the properties cannot carry; a wrong hint is a typed error and the
//! encode-side round-trip verify (spec §19.6) catches the residue.

use crate::{FormatError, FormatResult};

/// On-disk storage class ids (the `class` byte of stream entries, spec §6.3).
pub const CLASS_BYVAL: u8 = 0;
pub const CLASS_F32: u8 = 1;
pub const CLASS_F64: u8 = 2;
pub const CLASS_BOOL: u8 = 3;
pub const CLASS_FIXED: u8 = 4;
pub const CLASS_VARLENA: u8 = 5;

/// The six storage classes (spec §3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageClass {
    /// Pass-by-value word, width ∈ {1,2,4,8}; datum extension per `signed`
    /// (the `SIGNED` stream flag on disk).
    ByvalWord {
        width: u8,
        signed: bool,
    },
    F32,
    F64,
    Bool,
    /// Fixed-length by-reference, `len` = typlen.
    Fixed {
        len: u32,
    },
    /// typlen == −1: varlena-shaped images (spec §1).
    VarlenaVerbatim,
}

/// Ingest-side refinement hint (spec §3): catalog properties cannot separate
/// int4 from float4, int1 from bool, or signed from unsigned byval words.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClassHint {
    None,
    Signed,
    Float,
    Bool,
}

/// Collation class (spec §9): gates ORDER metadata + dict-lane election only
/// — a structural election predicate, never an exactness surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CollationClass {
    C = 0,
    OtherDeterministic = 1,
    Nondeterministic = 2,
}

impl CollationClass {
    pub const fn as_u8(self) -> u8 {
        self as u8
    }
    pub fn from_u8(v: u8) -> FormatResult<CollationClass> {
        match v {
            0 => Ok(CollationClass::C),
            1 => Ok(CollationClass::OtherDeterministic),
            2 => Ok(CollationClass::Nondeterministic),
            _ => Err(FormatError::Corrupt {
                at: "CollationClass",
            }),
        }
    }
}

/// The type-semantics tag (spec §19.7's profile input; the metadata plane's
/// half of the "properties, never an OID table" law).
///
/// A storage class fixes the BYTES; it does not fix the ORDER or the
/// EQUALITY those bytes stand for. `uuid` and `interval` are both
/// `Fixed{16}` with `typlen == 16`; `text`, `bytea`, `numeric` and `jsonb`
/// are all `VarlenaVerbatim`. Zone keys, sortedness and equality blooms are
/// only sound when the builder knows WHICH — so the tag is a caller-supplied
/// per-column fact (`pgrc2_am` maps it from `atttypid` at DDL, alongside the
/// byval hint; the offline and test harnesses declare it directly).
///
/// [`TypeSemantics::Opaque`] is the always-sound default: validity + counts
/// (+ byte lengths for varlena), never a key, never a bloom. An unmapped or
/// mis-declared type therefore degrades to exactly the pre-M3-E stand-in's
/// behaviour — it can lose pruning, never change an answer.
///
/// Lives HERE and not in `pgrc2_meta` because it rides [`ColSchema`], the
/// per-column descriptor the writer already carries to every seal site
/// (including shred lanes, which declare their own). `pgrc2_meta::profile`
/// re-exports it, so the M3-E paths are unchanged.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TypeSemantics {
    /// No value semantics available (jsonb-as-value, json, composites,
    /// ranges-at-v1, extension types): validity + counts (+ byte lengths
    /// for varlena) only. THE SOUND DEFAULT.
    #[default]
    Opaque,
    /// Signed integer family (int2/4/8, date, time, timestamp[tz], money):
    /// byval word, sign-extended.
    SignedInt,
    /// Unsigned word family (oid/regclass at width 4, "char" at width 1,
    /// pg_lsn/xid8 at width 8 — width decides the embed).
    UnsignedInt,
    /// float4/float8 (class picks 32 vs 64).
    Float,
    /// bool.
    Bool,
    /// memcmp IS the semantic order (uuid, macaddr, macaddr8 as Fixed;
    /// bytea as varlena — charter §7 "memcmp IS semantic order").
    MemcmpOrdered,
    /// text/varchar (NOT bpchar — its space-padded equality is not byte
    /// equality, so it must be declared `Opaque` at M3): collation decides
    /// keys and blooms per the profile's collation gate.
    TextCollated,
    /// numeric elected to the A6b fixed-scale i64 lane at `scale`
    /// (`aux32` of the stream entry, spec §6.3).
    PackedNumeric { scale: i32 },
    /// numeric NOT packed (no shared scale elected): byte-length stats and
    /// NDV only; no keys, no sums, no blooms.
    NumericUnpacked,
    /// interval (Fixed 16).
    IntervalCmp,
    /// timetz (Fixed 12).
    TimetzUtc,
    /// enums under the O-5/O-M3-5 v1 posture: eq-only, syscache-free. Key
    /// derivation is None and the record's `EnumRankReserved` slot is never
    /// written; blooms may arm (oid-word equality is value equality).
    EnumEqOnly,
}

/// Column schema facts — the fingerprint input (spec §5.5) and the encode
/// currency's class context.
///
/// NOTE (metadata plane): `semantics` is deliberately NOT folded into
/// [`crate::ident::schema_fingerprint`]. The fingerprint identifies the
/// STORAGE shape of a part — what bytes are laid out and how a reader must
/// decode them — and the semantics tag changes neither. It selects which
/// ADVISORY statistics the seal computes. Folding it would re-key every
/// existing part on a metadata-policy change and buy nothing: two parts that
/// differ only in semantics decode byte-identically.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColSchema {
    pub attno: u32,
    pub class: StorageClass,
    pub typlen: i16,
    pub typbyval: bool,
    pub typalign: u8,
    pub collation_class: CollationClass,
    /// Value semantics for the metadata plane (see [`TypeSemantics`]).
    pub semantics: TypeSemantics,
}

impl ColSchema {
    /// The pre-metadata-plane shape: every field but `semantics`, which
    /// takes the sound [`TypeSemantics::Opaque`] default. Test fixtures and
    /// harnesses that do not exercise the metadata plane use this; the
    /// production DDL path (`pgrc2_am::schema`) always declares.
    pub const fn opaque(
        attno: u32,
        class: StorageClass,
        typlen: i16,
        typbyval: bool,
        typalign: u8,
        collation_class: CollationClass,
    ) -> ColSchema {
        ColSchema {
            attno,
            class,
            typlen,
            typbyval,
            typalign,
            collation_class,
            semantics: TypeSemantics::Opaque,
        }
    }
}

impl StorageClass {
    /// The on-disk class id byte.
    pub const fn id(self) -> u8 {
        match self {
            StorageClass::ByvalWord { .. } => CLASS_BYVAL,
            StorageClass::F32 => CLASS_F32,
            StorageClass::F64 => CLASS_F64,
            StorageClass::Bool => CLASS_BOOL,
            StorageClass::Fixed { .. } => CLASS_FIXED,
            StorageClass::VarlenaVerbatim => CLASS_VARLENA,
        }
    }

    /// The `width` byte for stream entries (byval width; 0 elsewhere).
    pub const fn width(self) -> u8 {
        match self {
            StorageClass::ByvalWord { width, .. } => width,
            StorageClass::F32 => 4,
            StorageClass::F64 => 8,
            StorageClass::Bool => 1,
            StorageClass::Fixed { .. } | StorageClass::VarlenaVerbatim => 0,
        }
    }

    /// The `SIGNED` flag (spec §6.3).
    pub const fn signed(self) -> bool {
        matches!(self, StorageClass::ByvalWord { signed: true, .. })
    }

    /// The `fixed_len` field (Fixed's N; 0 elsewhere).
    pub const fn fixed_len(self) -> u32 {
        match self {
            StorageClass::Fixed { len } => len,
            _ => 0,
        }
    }

    /// Rebuild from stream-entry fields (decode side).
    pub fn from_parts(
        id: u8,
        width: u8,
        signed: bool,
        fixed_len: u32,
    ) -> FormatResult<StorageClass> {
        match id {
            CLASS_BYVAL => match width {
                1 | 2 | 4 | 8 => Ok(StorageClass::ByvalWord { width, signed }),
                _ => Err(FormatError::Corrupt {
                    at: "StorageClass byval width",
                }),
            },
            CLASS_F32 => Ok(StorageClass::F32),
            CLASS_F64 => Ok(StorageClass::F64),
            CLASS_BOOL => Ok(StorageClass::Bool),
            CLASS_FIXED => {
                if fixed_len == 0 {
                    return Err(FormatError::Corrupt {
                        at: "StorageClass fixed_len",
                    });
                }
                Ok(StorageClass::Fixed { len: fixed_len })
            }
            CLASS_VARLENA => Ok(StorageClass::VarlenaVerbatim),
            _ => Err(FormatError::UnknownStorageClass { class: id }),
        }
    }

    /// Derivation from catalog properties + refinement hint (spec §3).
    /// cstring (typlen −2) is not a table-column class: typed refusal.
    pub fn derive(
        typlen: i16,
        typbyval: bool,
        typalign: u8,
        hint: ClassHint,
    ) -> FormatResult<StorageClass> {
        if !matches!(typalign, b'c' | b's' | b'i' | b'd') {
            return Err(FormatError::BadClassHint {
                detail: "unknown typalign",
            });
        }
        if typbyval {
            let width = match typlen {
                1 | 2 | 4 | 8 => typlen as u8,
                _ => {
                    return Err(FormatError::BadClassHint {
                        detail: "byval typlen not 1/2/4/8",
                    })
                }
            };
            return match (hint, width) {
                (ClassHint::Float, 4) => Ok(StorageClass::F32),
                (ClassHint::Float, 8) => Ok(StorageClass::F64),
                (ClassHint::Float, _) => Err(FormatError::BadClassHint {
                    detail: "float hint on non-4/8 byval",
                }),
                (ClassHint::Bool, 1) => Ok(StorageClass::Bool),
                (ClassHint::Bool, _) => Err(FormatError::BadClassHint {
                    detail: "bool hint on non-1-byte byval",
                }),
                (ClassHint::Signed, w) => Ok(StorageClass::ByvalWord {
                    width: w,
                    signed: true,
                }),
                (ClassHint::None, w) => Ok(StorageClass::ByvalWord {
                    width: w,
                    signed: false,
                }),
            };
        }
        match typlen {
            -1 => Ok(StorageClass::VarlenaVerbatim),
            -2 => Err(FormatError::BadClassHint {
                detail: "cstring is not a table-column class",
            }),
            n if n > 0 => Ok(StorageClass::Fixed { len: n as u32 }),
            _ => Err(FormatError::BadClassHint {
                detail: "byref typlen",
            }),
        }
    }
}
