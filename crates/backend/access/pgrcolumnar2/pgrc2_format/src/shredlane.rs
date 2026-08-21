//! The shredded-jsonb LANE-KIND vocabulary — the one authority for the
//! lane-class ↔ storage-class mapping both sides of the format speak
//! (write: `pgrc2_write::shred_jsonb` derives lanes and mints their
//! `ColSchema`s; read: the scan face resolves a declared lane against a
//! part's StreamDir entry and refuses typed on disagreement).
//!
//! Provenance: the mapping is the vendored `adt_jsonb_shred` lane table as
//! the codec adapters record it (`pgrc2_codec::jsonbshred` module table)
//! and as the writer minted per-lane schemas since the TY-3 wiring
//! (`pgrc2_write::shred_jsonb::lane_schema`, now a delegation here — one
//! authority, two consumers; origin/lanev4 98987df7cdf lineage). The table:
//!
//! | lane kind | storage class      | notes                               |
//! |-----------|--------------------|-------------------------------------|
//! | Text      | VarlenaVerbatim    | jsonb strings, datum-shaped varlena |
//! | Uuid16    | Fixed(16)          | canonical-form uuid images          |
//! | NumericFs | ByvalWord8 signed  | fixed-scale mantissas; the SCALE    |
//! |           |                    | rides only the in-memory manifest   |
//! |           |                    | (spec §6.5 gap — see below)         |
//! | Bool      | Bool               | one bit, staged as 0/1 words        |
//!
//! **The NumericFs scale slot (RULED 2026-08-14, Michael — the flagged
//! §6.5 gap resolved):** the chunk-shared decimal scale of a NumericFs
//! lane persists in the lane Values entry's `aux32` under
//! `STREAMF_LANE_SCALE` (the ArrayDual-aux32 precedent). Readers
//! adjudicate from the part alone via [`numeric_lane_scale`]: flag-absent
//! numeric lanes (pre-ruling parts) refuse typed as before; scale-0 lanes
//! serve as signed int words (the µs-epoch class); non-zero scales await
//! their rendering family (typed refusal until routed).

use crate::class::{ColSchema, CollationClass, StorageClass, TypeSemantics};
use crate::class::{CLASS_BOOL, CLASS_BYVAL, CLASS_FIXED, CLASS_VARLENA};

/// The four shred lane kinds (vendored `adt_jsonb_shred::Lane` mirror at
/// the format grain — kept separate so the format crate never depends on
/// the shred machinery).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShredLaneKind {
    Text,
    Uuid16,
    NumericFs,
    Bool,
}

impl ShredLaneKind {
    /// Classify a DECLARED column class as a lane kind. `None` = the class
    /// is not a lane class (F32/F64 and non-16 Fixed never carry lanes).
    pub fn of_class(class: &StorageClass) -> Option<ShredLaneKind> {
        match class {
            StorageClass::VarlenaVerbatim => Some(ShredLaneKind::Text),
            StorageClass::Fixed { len: 16 } => Some(ShredLaneKind::Uuid16),
            StorageClass::ByvalWord {
                width: 8,
                signed: true,
            } => Some(ShredLaneKind::NumericFs),
            StorageClass::Bool => Some(ShredLaneKind::Bool),
            _ => None,
        }
    }

    /// Classify a SEALED lane's StreamDir facts. Keyed on the stable entry
    /// fields only (`class` id + `fixed_len`): the entry `width` byte is
    /// per-encoding vocabulary (DictCodes carries max-code width — the
    /// §6.3/§19.5 normalization law) and must never discriminate here.
    pub fn of_entry(class_id: u8, fixed_len: u32) -> Option<ShredLaneKind> {
        match class_id {
            CLASS_VARLENA => Some(ShredLaneKind::Text),
            CLASS_FIXED if fixed_len == 16 => Some(ShredLaneKind::Uuid16),
            CLASS_BYVAL => Some(ShredLaneKind::NumericFs),
            CLASS_BOOL => Some(ShredLaneKind::Bool),
            _ => None,
        }
    }

    /// The per-lane column descriptor: a typed lane's semantics are its
    /// OWN (an int lane shredded out of jsonb is an int — the `meta_wire`
    /// doc's law). `attno` is the PARENT jsonb column's attno: lanes ride
    /// the parent's stream family at `path_ord ≥ 1`.
    pub fn col_schema(&self, attno: u32) -> ColSchema {
        match self {
            ShredLaneKind::Text => ColSchema {
                attno,
                class: StorageClass::VarlenaVerbatim,
                typlen: -1,
                typbyval: false,
                typalign: b'i',
                collation_class: CollationClass::C,
                semantics: TypeSemantics::TextCollated,
            },
            ShredLaneKind::Uuid16 => ColSchema {
                attno,
                class: StorageClass::Fixed { len: 16 },
                typlen: 16,
                typbyval: false,
                typalign: b'c',
                collation_class: CollationClass::C,
                semantics: TypeSemantics::MemcmpOrdered,
            },
            ShredLaneKind::NumericFs => ColSchema {
                attno,
                class: StorageClass::ByvalWord {
                    width: 8,
                    signed: true,
                },
                typlen: 8,
                typbyval: true,
                typalign: b'd',
                collation_class: CollationClass::C,
                semantics: TypeSemantics::SignedInt,
            },
            ShredLaneKind::Bool => ColSchema {
                attno,
                class: StorageClass::Bool,
                typlen: 1,
                typbyval: true,
                typalign: b'c',
                collation_class: CollationClass::C,
                semantics: TypeSemantics::Bool,
            },
        }
    }
}

/// Adjudicate a sealed NUMERIC lane's scale from its StreamDir entry
/// facts (the RULED aux32 slot). `Err` = the part predates the slot
/// (flag absent) — scale unknowable, the reader refuses typed.
pub fn numeric_lane_scale(flags: u16, aux32: u32) -> Result<i32, &'static str> {
    if flags & crate::part::STREAMF_LANE_SCALE == 0 {
        return Err("numeric lane scale unpersisted (pre-ruling part)");
    }
    Ok(aux32 as i32)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The declared-class and sealed-entry classifiers agree with the
    /// schema mint on every kind (one authority, closed loop).
    #[test]
    fn kind_class_schema_roundtrip() {
        for kind in [
            ShredLaneKind::Text,
            ShredLaneKind::Uuid16,
            ShredLaneKind::NumericFs,
            ShredLaneKind::Bool,
        ] {
            let s = kind.col_schema(7);
            assert_eq!(s.attno, 7);
            assert_eq!(ShredLaneKind::of_class(&s.class), Some(kind));
            let fixed_len = match s.class {
                StorageClass::Fixed { len } => len,
                _ => 0,
            };
            assert_eq!(ShredLaneKind::of_entry(s.class.id(), fixed_len), Some(kind));
        }
    }

    /// The scale adjudicator's three arms (the RULED slot): flag-absent
    /// refuses (pre-ruling parts — scale unknowable), scale-0 serves,
    /// non-zero scales surface exactly.
    #[test]
    fn numeric_lane_scale_adjudication() {
        use crate::part::STREAMF_LANE_SCALE;
        assert!(numeric_lane_scale(0, 0).is_err(), "flag-absent refuses");
        assert!(numeric_lane_scale(0, 2).is_err(), "flag-absent refuses at any aux32");
        assert_eq!(numeric_lane_scale(STREAMF_LANE_SCALE, 0), Ok(0));
        assert_eq!(numeric_lane_scale(STREAMF_LANE_SCALE, 2), Ok(2));
    }

    /// Non-lane classes classify to None (the refusal direction).
    #[test]
    fn non_lane_classes_refuse() {
        assert_eq!(ShredLaneKind::of_class(&StorageClass::F64), None);
        assert_eq!(ShredLaneKind::of_class(&StorageClass::F32), None);
        assert_eq!(
            ShredLaneKind::of_class(&StorageClass::Fixed { len: 8 }),
            None
        );
        assert_eq!(
            ShredLaneKind::of_class(&StorageClass::ByvalWord {
                width: 4,
                signed: true
            }),
            None
        );
    }
}
