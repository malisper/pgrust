//! Part identity + fingerprints (spec §11 + §5.5): "equal identity ⇒
//! identical bytes" is the sharing foundation — segment maps, parsed
//! metadata, part cache, and the predicate cache (O-11) all key on
//! [`part_uuid`]. Physical facts are gathered at OPEN (dev/ino are not in
//! the file); a copied part file has a NEW identity, which is exactly why
//! authoritative state (the DV) never keys on it (spec §16).

use crate::class::ColSchema;

/// The physical-instance facts of one open part file (spec §11).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PartIdent {
    pub dev: u64,
    pub ino: u64,
    pub len: u64,
    pub footer_off: u64,
}

/// splitmix64 finalizer — the frozen mix primitive for identity hashing
/// (golden-pinned; NOT a general-purpose hash surface).
#[inline]
pub const fn mix64(mut z: u64) -> u64 {
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// One chain step: absorb `word` into `acc`.
#[inline]
pub const fn fold64(acc: u64, word: u64) -> u64 {
    mix64(acc ^ word.wrapping_mul(0x9E37_79B9_7F4A_7C15))
}

/// Lane seeds for the two-lane uuid chain (ASCII "pgrc2fmt" / "partuuid").
const UUID_SEED_LO: u64 = 0x7067_7263_3266_6d74;
const UUID_SEED_HI: u64 = 0x7061_7274_7575_6964;

/// Derive the 16-byte part uuid from the identity quadruple (spec §11).
/// Deterministic, golden-pinned; the uuid IS the cache key vocabulary.
pub fn part_uuid(ident: &PartIdent) -> [u8; 16] {
    let words = [ident.dev, ident.ino, ident.len, ident.footer_off];
    let mut lo = UUID_SEED_LO;
    let mut hi = UUID_SEED_HI;
    for w in words {
        lo = fold64(lo, w);
        hi = fold64(hi, w ^ 0xA5A5_A5A5_A5A5_A5A5);
    }
    let mut out = [0u8; 16];
    out[..8].copy_from_slice(&lo.to_le_bytes());
    out[8..].copy_from_slice(&hi.to_le_bytes());
    out
}

/// Schema fingerprint (spec §5.5): folded over the catalog-property facts of
/// every column, in attno order. A part/manifest whose fingerprint disagrees
/// with the catalog derivation is refused at open.
pub fn schema_fingerprint(cols: &[ColSchema]) -> u64 {
    let mut acc = fold64(UUID_SEED_LO ^ UUID_SEED_HI, cols.len() as u64);
    for c in cols {
        acc = fold64(acc, c.attno as u64);
        acc = fold64(
            acc,
            (c.class.id() as u64)
                | ((c.class.width() as u64) << 8)
                | ((c.class.signed() as u64) << 16)
                | ((c.class.fixed_len() as u64) << 24),
        );
        acc = fold64(
            acc,
            (c.typlen as u16 as u64)
                | ((c.typbyval as u64) << 16)
                | ((c.typalign as u64) << 24)
                | ((c.collation_class.as_u8() as u64) << 32),
        );
    }
    acc
}

/// The typalign-free schema fingerprint (M5a.bank-adoption-face,
/// `docs/design/pgrc2-bank-adoption.md` §3.2 step 5): the §5.5 fold with
/// the typalign byte MASKED OUT of the property word — everything else
/// (class facts, typlen, typbyval, collation_class, arity, attno order)
/// folds identically to [`schema_fingerprint`]. Two schemas that agree
/// here interpret STORED BYTES identically: stream layout is class-driven
/// (`StorageClass::derive` consumes typalign only as a c/s/i/d validity
/// check) and datum materialization uses the CATALOG's typalign at read
/// time regardless of what a cut stamped. This is the adoption face's
/// structural-equivalence currency — the ONLY relaxation vs the stamped
/// fingerprint, and it is a comparison currency, never a stamped one
/// (parts and manifests keep stamping [`schema_fingerprint`]).
pub fn alignfree_fingerprint(cols: &[ColSchema]) -> u64 {
    let mut acc = fold64(UUID_SEED_LO ^ UUID_SEED_HI, cols.len() as u64);
    for c in cols {
        acc = fold64(acc, c.attno as u64);
        acc = fold64(
            acc,
            (c.class.id() as u64)
                | ((c.class.width() as u64) << 8)
                | ((c.class.signed() as u64) << 16)
                | ((c.class.fixed_len() as u64) << 24),
        );
        acc = fold64(
            acc,
            (c.typlen as u16 as u64)
                | ((c.typbyval as u64) << 16)
                // typalign byte deliberately absent (bit 24..32 stays 0).
                | ((c.collation_class.as_u8() as u64) << 32),
        );
    }
    acc
}

#[cfg(test)]
mod adoption_fingerprint_teeth {
    //! M5a.bank-adoption-face teeth: the alignfree fold differs from the
    //! stamped fold EXACTLY on typalign, and on nothing else.

    use super::*;
    use crate::class::{ColSchema, CollationClass, StorageClass};

    fn col(attno: u32, typalign: u8) -> ColSchema {
        ColSchema::opaque(
            attno,
            StorageClass::ByvalWord { width: 4, signed: true },
            4,
            true,
            typalign,
            CollationClass::C,
        )
    }

    #[test]
    fn typalign_is_the_only_excluded_fact() {
        // The rig-vs-engine divergence shape: same columns, different
        // typalign ('i' hardcoded vs catalog 's'/'d').
        let rig = [col(1, b'i'), col(2, b'i')];
        let engine = [col(1, b's'), col(2, b'd')];
        assert_ne!(
            schema_fingerprint(&rig),
            schema_fingerprint(&engine),
            "the stamped fold sees typalign"
        );
        assert_eq!(
            alignfree_fingerprint(&rig),
            alignfree_fingerprint(&engine),
            "the alignfree fold reconciles typalign-only deltas"
        );
        // Every OTHER fact still splits the alignfree fold.
        let mut wider = [col(1, b'i'), col(2, b'i')];
        wider[1].class = StorageClass::ByvalWord { width: 8, signed: true };
        wider[1].typlen = 8;
        assert_ne!(alignfree_fingerprint(&rig), alignfree_fingerprint(&wider), "class/width");
        let mut collated = [col(1, b'i'), col(2, b'i')];
        collated[1].collation_class = CollationClass::OtherDeterministic;
        assert_ne!(
            alignfree_fingerprint(&rig),
            alignfree_fingerprint(&collated),
            "collation_class is load-bearing and stays folded"
        );
        let fewer = [col(1, b'i')];
        assert_ne!(alignfree_fingerprint(&rig), alignfree_fingerprint(&fewer), "arity");
        let renumbered = [col(1, b'i'), col(3, b'i')];
        assert_ne!(alignfree_fingerprint(&rig), alignfree_fingerprint(&renumbered), "attno");
    }

    #[test]
    fn alignfree_is_the_stamped_fold_with_typalign_masked() {
        // The masking DEFINITION, pinned structurally: the alignfree fold
        // over any schema == the stamped fold over the same schema with
        // typalign zeroed. Adoption verdicts persist across shas
        // (relopt-recorded), so this relationship must never drift — a
        // seed/word refactor that moves only one of the two folds trips
        // here.
        let cols = [col(1, b'i'), col(2, b'd')];
        let zeroed = [col(1, 0), col(2, 0)];
        assert_eq!(alignfree_fingerprint(&cols), schema_fingerprint(&zeroed));
        assert_ne!(alignfree_fingerprint(&cols), schema_fingerprint(&cols));
    }
}
