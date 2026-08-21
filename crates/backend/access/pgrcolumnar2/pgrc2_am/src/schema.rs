//! Catalog attribute → `ColSchema` derivation (spec §2 storage classes;
//! §1.4 universal-type law: classes derive from typlen/typbyval/typalign,
//! never an OID table — the ONE place an OID appears is the byval HINT map
//! below, because signedness/floatness of a byval word is a semantic fact
//! the catalog properties cannot carry).
//!
//! ## The byval hint law (settled here)
//!
//! Datum extension for byval widths < 8 is per the `SIGNED` stream flag
//! (spec §6.7): a mis-hinted signed type would zero-extend negative values
//! into wrong datum words. So byval types are admitted by an EXPLICIT hint
//! table; a byval type with no entry is a typed CREATE-time refusal (the
//! old-AM type-gate precedent). By-reference types need no hint (their
//! images are byte-exact): fixed-length → `Fixed{len}`, typlen == −1 →
//! `VarlenaVerbatim`, typlen == −2 (cstring) refuses.
//!
//! ## The type-semantics map (spec §19.7; the metadata plane's half)
//!
//! [`TypeSemantics`] is the SECOND fact catalog properties cannot carry, and
//! it is settled here for the same reason as the byval hint — this is the
//! only place in the pgrcolumnar2 stack that sees `atttypid`. A storage
//! class fixes the BYTES; it does not fix the ORDER or the EQUALITY those
//! bytes stand for. `uuid` and `interval` are both `Fixed{16}` with
//! `typlen == 16`; `text`, `bytea`, `numeric` and `jsonb` are all
//! `VarlenaVerbatim`. Zone keys, sortedness and equality blooms are sound
//! only when the seal knows which, so the tag rides [`ColSchema`] out to the
//! writer, which reads it and never guesses.
//!
//! The map is DELIBERATELY conservative and closed by an `Opaque` default:
//! an unmapped or not-yet-audited type gets validity + counts (+ byte
//! lengths) and no key, which is exactly what every part carried before the
//! metadata plane was wired. Adding a row can only add pruning; it can never
//! change an answer, because a stat is advisory. Rows are admitted only when
//! byte order IS semantic order (for keys) and byte equality IS value
//! equality (for blooms) — the two properties the profile lattice demands:
//!
//! - `bpchar` is `Opaque`, not `TextCollated`: its space-padded equality is
//!   not byte equality.
//! - `numeric` is `NumericUnpacked` when UNCONSTRAINED; a declared
//!   NUMERIC(P,S) elects `PackedNumeric{S}` ([scale-alg]): post-typmod-cast
//!   every stored value's dscale IS S (a DDL fact), and any value that
//!   refuses the pack (legacy/corrupt image, mantissa over budget) poisons
//!   its grain — the plane abstains, never lies.
//! - `name` is `Opaque`: 64-byte NUL-padded images with strcmp semantics are
//!   not worth an unaudited memcmp claim.
//! - `interval` is `IntervalCmp` (NOT `MemcmpOrdered`) and `timetz` is
//!   `TimetzUtc` (NOT `MemcmpOrdered`) — both are `Fixed` images whose
//!   semantic order is a computed comparison key, not their bytes.

use pgrc2_format::class::{ClassHint, ColSchema, StorageClass, TypeSemantics};
pub use pgrc2_format::class::CollationClass;
use types_core::catalog::{
    BOOLOID, BYTEAOID, CHAROID, CIDOID, DATEOID, FLOAT4OID, FLOAT8OID, INT2OID, INT4OID, INT8OID,
    INTERVALOID, NUMERICOID, OIDOID, REGTYPEOID, TEXTOID, TIMEOID, TIMESTAMPOID, TIMESTAMPTZOID,
    TIMETZOID, VARCHAROID, XIDOID,
};
use types_core::Oid;
use types_error::{PgError, PgResult};
use types_rel::Relation;
use types_tuple::tupdesc::FormData_pg_attribute;

/// C collation oids (pg_collation.dat): "C" = 950, "POSIX" = 951.
const C_COLLATION_OID: Oid = 950;
const POSIX_COLLATION_OID: Oid = 951;
/// The database-default collation sentinel (pg_collation.dat "default").
const DEFAULT_COLLATION_OID: Oid = 100;

/// Type oids with no `types_core::catalog` constant yet (pg_type.dat).
const UUIDOID: Oid = 2950;
const MACADDROID: Oid = 829;
const MACADDR8OID: Oid = 774;

/// The byval hint map. `None` = admitted, unsigned/word semantics.
/// Money (790) is byval int8 on 64-bit; regproc(24)/regclass(2205)-style
/// alias types are NOT listed — they refuse until someone owns their audit.
fn byval_hint(atttypid: Oid) -> Option<ClassHint> {
    match atttypid {
        BOOLOID => Some(ClassHint::Bool),
        INT2OID | INT4OID | INT8OID => Some(ClassHint::Signed),
        DATEOID | TIMEOID | TIMESTAMPOID | TIMESTAMPTZOID => Some(ClassHint::Signed),
        FLOAT4OID | FLOAT8OID => Some(ClassHint::Float),
        // Unsigned word classes.
        OIDOID | XIDOID | CIDOID | CHAROID | REGTYPEOID => Some(ClassHint::None),
        _ => None,
    }
}

/// [scale-alg] NUMERIC(P,S) typmod → the declared scale S (PG's
/// `numeric_typmod_scale`: low 11 bits of typmod−VARHDRSZ, sign-extended
/// from bit 10). `None` for the unconstrained typmod (−1) and for
/// negative declared scales — no uniform-dscale stats lane exists there
/// (stored dscale >= 0). Mirrors `sqe::typmeta::numeric_typmod_scale`
/// (this crate sits below the executor — the 5-line law is restated,
/// not imported).
fn numeric_typmod_scale(typmod: i32) -> Option<i32> {
    if typmod < 4 {
        return None;
    }
    let s = (((typmod - 4) & 0x7ff) ^ 1024) - 1024;
    (s >= 0).then_some(s)
}

/// The type-semantics map (see the module doc). Every row is consistent with
/// the storage class the same oid derives through [`byval_hint`] /
/// [`StorageClass::derive`] — `semantics_match_classes` pins that, and the
/// writer degrades to `Opaque` rather than trusting an inconsistent pair.
///
/// [scale-alg] NUMERIC takes the typmod too: a DECLARED nonnegative
/// scale elects the `PackedNumeric{scale}` ADVISORY stats lane (exact
/// mantissa zone keys + i128 mantissa sums + zero counts — the batch
/// builder's incumbent packed arm; values that refuse the pack poison
/// their grain, so the plane is present-and-exact or honestly absent).
/// Semantics are NOT in the schema fingerprint (class.rs — advisory
/// stats policy, not storage shape): parts sealed before this election
/// simply lack the plane and every witness consumer refuses typed.
fn type_semantics(atttypid: Oid, atttypmod: i32) -> TypeSemantics {
    match atttypid {
        BOOLOID => TypeSemantics::Bool,
        // Signed byval words. date/time/timestamp[tz] are int-shaped and
        // their integer order IS their temporal order.
        INT2OID | INT4OID | INT8OID => TypeSemantics::SignedInt,
        DATEOID | TIMEOID | TIMESTAMPOID | TIMESTAMPTZOID => TypeSemantics::SignedInt,
        FLOAT4OID | FLOAT8OID => TypeSemantics::Float,
        OIDOID | XIDOID | CIDOID | CHAROID | REGTYPEOID => TypeSemantics::UnsignedInt,
        // By-reference: memcmp IS the semantic order.
        UUIDOID | MACADDROID | MACADDR8OID => TypeSemantics::MemcmpOrdered,
        BYTEAOID => TypeSemantics::MemcmpOrdered,
        // Text family: the profile's collation gate decides keys vs blooms.
        TEXTOID | VARCHAROID => TypeSemantics::TextCollated,
        // Fixed images with a COMPUTED comparison key.
        INTERVALOID => TypeSemantics::IntervalCmp,
        TIMETZOID => TypeSemantics::TimetzUtc,
        // [scale-alg] Typmod-scaled numeric: the packed advisory lane
        // (every value's dscale == S post-typmod-cast, so the pack is
        // total on healthy data; refusals poison per grain). The
        // UNCONSTRAINED column keeps counts + byte lengths + NDV — no
        // keys (1.0 vs 1.00 collapse display scales), no blooms.
        NUMERICOID => match numeric_typmod_scale(atttypmod) {
            Some(scale) => TypeSemantics::PackedNumeric { scale },
            None => TypeSemantics::NumericUnpacked,
        },
        // Closed by the sound default: bpchar, name, json/jsonb/xml,
        // bit/varbit, composites, ranges, arrays, extension types.
        _ => TypeSemantics::Opaque,
    }
}

/// FAIL CLOSED (#598): a non-C attcollation is deterministic only when
/// `pg_collation.collisdeterministic` SAYS so. Every UNKNOWN — the probe
/// seam not installed (no catalog reachable), a dangling attcollation, a
/// probe error — classifies `Nondeterministic`: no zone keys, no eq blooms,
/// counts + length stats only. Never guess deterministic: the previous arm
/// mapped every non-C collation to `OtherDeterministic`, and
/// `profile.rs` arms byte-hash eq blooms for anything deterministic — so a
/// case-insensitive ICU collation sealed blooms where equal-under-collation,
/// byte-different probes read definite-absent, silently missing rows the
/// day predicate pushdown wires. The conservative arm only loses advisory
/// pruning (charter §1: a verdict can lose speed, never rows).
///
/// Parts already sealed under the old fail-open classification: both the
/// seal and probe sides classify through THIS function (the `ColSchema`
/// they share, spec §5.5), and `collation_class` is folded into
/// `schema_fingerprint`. A genuinely nondeterministic collation therefore
/// reclassifies here, its table's fingerprint changes, and its pre-fix
/// manifests refuse TYPED at open (`ReadError::OpenMismatch
/// {schema_fingerprint}`) — the reader-side guard: parts carrying unsound
/// blooms are never silently trusted; they error until re-sealed. Every
/// honestly deterministic collation (default, C, POSIX, deterministic ICU)
/// probes to the same class as before, so blessed bank fingerprints are
/// unchanged.
///
/// The probe is deterministic across seal and probe time
/// (`collisdeterministic` is immutable for a live collation), and the one
/// reachable skew — seam installed on one side, not the other — degrades to
/// "blooms not consulted" (bloom evidence is optional at every probe site),
/// never to a wrong verdict.
pub fn collation_class(attcollation: Oid) -> CollationClass {
    if attcollation == 0
        || attcollation == C_COLLATION_OID
        || attcollation == POSIX_COLLATION_OID
    {
        return CollationClass::C;
    }
    // M4-S8 (the S5/FSST engine-grain substrate finding, coordinator-listed
    // for S8): the stock CB DDL carries DEFAULT-collated text, and on a
    // C-locale database (initdb --no-locale — the v4ctas vehicle and every
    // bank rig) DEFAULT *is* memcmp order. Classifying it
    // OtherDeterministic made dict/FSST election structurally unreachable
    // on every engine cut (elect.rs offers no dict policy off a non-C
    // class). Resolve DEFAULT through the engine's own locale answer — the
    // plancat.rs pgrc2 order predicate's exact currency (collate_is_c:
    // memcmp order IS the collation order, so zone keys, order
    // certificates, and char-length facts are all sound). Fail-closed
    // stands: seam uninstalled (library worlds), probe error, or a non-C
    // default all fall through to the conservative arms below.
    // Fingerprint note: collation_class folds into schema_fingerprint, so
    // an engine table sealed pre-fix refuses TYPED at open (the #598
    // reader-side guard) — the v4ctas vehicle cuts fresh per job and no
    // production pgrc2 data predates the cutover, so the reclassification
    // has no silent-trust surface.
    if attcollation == DEFAULT_COLLATION_OID
        && pg_locale_seams::collation_collate_is_c::is_installed()
        && pg_locale_seams::collation_collate_is_c::call(attcollation).unwrap_or(false)
    {
        return CollationClass::C;
    }
    if !syscache_seams::lookup_pg_collation_shape::is_installed() {
        return CollationClass::Nondeterministic;
    }
    match syscache_seams::lookup_pg_collation_shape::call(attcollation) {
        Ok(Some(shape)) if shape.collisdeterministic => CollationClass::OtherDeterministic,
        // Missing row or probe error: UNKNOWN is Nondeterministic. The
        // error is deliberately absorbed — collation class selects
        // advisory-metadata policy, and the conservative class is always
        // sound; a real catalog failure will surface on the next
        // non-advisory catalog access.
        _ => CollationClass::Nondeterministic,
    }
}

/// Why a column is refused (CREATE-time gate message currency).
pub struct UnsupportedColumn {
    pub attname_hint: String,
    pub atttypid: Oid,
}

fn derive_one(att: &FormData_pg_attribute) -> Result<ColSchema, UnsupportedColumn> {
    let refuse = || UnsupportedColumn {
        attname_hint: String::from_utf8_lossy(att.attname.name_str()).into_owned(),
        atttypid: att.atttypid,
    };
    if att.attisdropped {
        // ALTER DROP COLUMN is gated off for pgrcolumnar2 tables; a dropped
        // column can only mean the gate was bypassed — refuse.
        return Err(refuse());
    }
    let class = if att.attbyval {
        let hint = byval_hint(att.atttypid).ok_or_else(refuse)?;
        StorageClass::derive(att.attlen, true, att.attalign as u8, hint).map_err(|_| refuse())?
    } else {
        match att.attlen {
            -1 => StorageClass::VarlenaVerbatim,
            len if len > 0 => StorageClass::Fixed { len: len as u32 },
            _ => return Err(refuse()),
        }
    };
    Ok(ColSchema {
        attno: att.attnum as u32,
        class,
        typlen: att.attlen,
        typbyval: att.attbyval,
        typalign: att.attalign as u8,
        collation_class: collation_class(att.attcollation),
        semantics: type_semantics(att.atttypid, att.atttypmod),
    })
}

/// Derive the full column schema for a pgrcolumnar2 relation. Attnos are
/// the 1-based catalog attnums (strictly increasing — the writer contract).
pub fn col_schemas(rel: &Relation<'_>) -> PgResult<Vec<ColSchema>> {
    let atts = &rel.rd_att.attrs;
    let mut out = Vec::with_capacity(atts.len());
    for att in atts.iter() {
        match derive_one(att) {
            Ok(s) => out.push(s),
            Err(u) => return Err(unsupported_column_error(rel.name(), &u)),
        }
    }
    if out.is_empty() {
        return Err(crate::unsupported("tables with zero columns"));
    }
    Ok(out)
}

/// The 1-based attnos of the relation's jsonb columns — the TY-3 shred
/// arm's registration set (JSON-routing landing). Catalog knowledge: this
/// module is the only place in the pgrcolumnar2 stack that sees
/// `atttypid` (the module-doc law), so the jsonb fact is minted here and
/// handed to the ingest seam as plain attnos.
pub fn jsonb_attnos(rel: &Relation<'_>) -> Vec<u32> {
    use types_core::catalog::JSONBOID;
    rel.rd_att
        .attrs
        .iter()
        .filter(|a| !a.attisdropped && a.atttypid == JSONBOID)
        .map(|a| a.attnum as u32)
        .collect()
}

/// The CREATE-time gate: probe every attribute, so `CREATE TABLE ... USING
/// pgrcolumnar2` refuses typed at DDL time instead of at first COPY.
pub fn check_create_supported(relname: &str, atts: &[FormData_pg_attribute]) -> PgResult<()> {
    for att in atts {
        if att.attisdropped {
            continue;
        }
        if let Err(u) = derive_one(att) {
            return Err(unsupported_column_error(relname, &u));
        }
    }
    Ok(())
}

fn unsupported_column_error(relname: &str, u: &UnsupportedColumn) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "pgrcolumnar2 does not support the type of column \"{}\" of relation \"{}\" \
             (type oid {})",
            u.attname_hint, relname, u.atttypid
        ))
        .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
    )
}

#[cfg(test)]
mod semantics_tests {
    use super::*;
    use pgrc2_meta::profile::MetaProfile;

    /// Storage class for an oid exactly as `derive_one` computes it, from
    /// the catalog properties the type actually has.
    fn class_of(atttypid: Oid, attlen: i16, attbyval: bool, attalign: u8) -> StorageClass {
        if attbyval {
            let hint = byval_hint(atttypid).expect("byval type admitted");
            StorageClass::derive(attlen, true, attalign, hint).expect("class derives")
        } else {
            match attlen {
                -1 => StorageClass::VarlenaVerbatim,
                n if n > 0 => StorageClass::Fixed { len: n as u32 },
                _ => panic!("not a table-column typlen"),
            }
        }
    }

    /// THE CONSISTENCY PIN: every oid this file maps must produce a
    /// (class, semantics) pair `MetaProfile::derive` ACCEPTS. An
    /// inconsistent row would not fail an ingest — the writer degrades to
    /// `Opaque` — it would silently cost the column its pruning forever, so
    /// the pair is pinned here rather than discovered in a benchmark.
    #[test]
    fn semantics_match_classes() {
        // (oid, typlen, typbyval, typalign)
        let rows: &[(Oid, i16, bool, u8)] = &[
            (BOOLOID, 1, true, b'c'),
            (INT2OID, 2, true, b's'),
            (INT4OID, 4, true, b'i'),
            (INT8OID, 8, true, b'd'),
            (DATEOID, 4, true, b'i'),
            (TIMEOID, 8, true, b'd'),
            (TIMESTAMPOID, 8, true, b'd'),
            (TIMESTAMPTZOID, 8, true, b'd'),
            (FLOAT4OID, 4, true, b'i'),
            (FLOAT8OID, 8, true, b'd'),
            (OIDOID, 4, true, b'i'),
            (XIDOID, 4, true, b'i'),
            (CIDOID, 4, true, b'i'),
            (CHAROID, 1, true, b'c'),
            (REGTYPEOID, 4, true, b'i'),
            (UUIDOID, 16, false, b'c'),
            (MACADDROID, 6, false, b'i'),
            (MACADDR8OID, 8, false, b'i'),
            (BYTEAOID, -1, false, b'i'),
            (TEXTOID, -1, false, b'i'),
            (VARCHAROID, -1, false, b'i'),
            (INTERVALOID, 16, false, b'd'),
            (TIMETZOID, 12, false, b'd'),
            (NUMERICOID, -1, false, b'i'),
        ];
        for &(oid, len, byval, align) in rows {
            let class = class_of(oid, len, byval, align);
            let sem = type_semantics(oid, -1);
            assert_ne!(
                sem,
                TypeSemantics::Opaque,
                "oid {oid} is in the pin table but maps to Opaque"
            );
            for coll in [
                CollationClass::C,
                CollationClass::OtherDeterministic,
                CollationClass::Nondeterministic,
            ] {
                MetaProfile::derive(class, coll, sem).unwrap_or_else(|e| {
                    panic!("oid {oid}: ({class:?}, {coll:?}, {sem:?}) rejected: {e:?}")
                });
            }
        }
    }

    /// The tooth: the pin above provably discriminates — a deliberately
    /// wrong pair IS rejected, so a green `semantics_match_classes` is
    /// evidence and not a tautology.
    #[test]
    fn inconsistent_pair_is_rejected() {
        assert!(MetaProfile::derive(
            StorageClass::VarlenaVerbatim,
            CollationClass::C,
            TypeSemantics::SignedInt,
        )
        .is_err());
        // The two Fixed{16} types are exactly the confusion the tag exists
        // to prevent; both are accepted, and they are NOT the same profile.
        let uuid = MetaProfile::derive(
            StorageClass::Fixed { len: 16 },
            CollationClass::C,
            TypeSemantics::MemcmpOrdered,
        )
        .expect("uuid");
        let interval = MetaProfile::derive(
            StorageClass::Fixed { len: 16 },
            CollationClass::C,
            TypeSemantics::IntervalCmp,
        )
        .expect("interval");
        assert_ne!(uuid.key, interval.key);
        assert!(uuid.eq_bloomable && !interval.eq_bloomable);
    }

    /// The closed default: an unmapped type is `Opaque`, never a guess.
    #[test]
    fn unmapped_types_are_opaque() {
        use types_core::catalog::{BPCHAROID, JSONBOID, JSONOID, NAMEOID, XMLOID};
        for oid in [BPCHAROID, JSONOID, JSONBOID, XMLOID, NAMEOID, 1560 /* bit */] {
            assert_eq!(type_semantics(oid, -1), TypeSemantics::Opaque, "oid {oid}");
        }
    }
}

#[cfg(test)]
mod collation_tests {
    use super::*;
    use syscache_seams::{lookup_pg_collation_shape, PgCollationShape};

    fn coll_name(s: &str) -> types_tuple::NameData {
        let mut n = types_tuple::NameData::default();
        n.namestrcpy(s);
        n
    }

    /// BORN-RED (#598 leg 2): the collation gate must FAIL CLOSED. A
    /// non-C/POSIX attcollation is deterministic only if
    /// `pg_collation.collisdeterministic` SAYS so; every UNKNOWN — probe
    /// unreachable, row missing, probe error — classifies Nondeterministic
    /// (no eq blooms), never a deterministic guess. A case-insensitive ICU
    /// collation classified OtherDeterministic seals byte-hash blooms where
    /// equal-under-collation, byte-different probes read definite-absent:
    /// silently missing rows the day predicate pushdown wires.
    ///
    /// ONE test on purpose: the seam is set-once per process, so the
    /// uninstalled arm must be asserted before the installed arms, in order,
    /// in a single body — parallel test threads must not race the install.
    #[test]
    fn collation_classification_fails_closed() {
        // C-class oids never probe (byte order IS the collation order).
        assert_eq!(collation_class(0), CollationClass::C);
        assert_eq!(collation_class(C_COLLATION_OID), CollationClass::C);
        assert_eq!(collation_class(POSIX_COLLATION_OID), CollationClass::C);

        // UNKNOWN: the probe is unreachable in this binary (seam not
        // installed) — classify Nondeterministic, never guess.
        assert!(!lookup_pg_collation_shape::is_installed());
        assert_eq!(
            collation_class(9002),
            CollationClass::Nondeterministic,
            "unreachable pg_collation probe must classify fail-closed"
        );
        // M4-S8: DEFAULT with NEITHER seam installed stays fail-closed —
        // the library world never guesses the database locale.
        assert!(!pg_locale_seams::collation_collate_is_c::is_installed());
        assert_eq!(
            collation_class(DEFAULT_COLLATION_OID),
            CollationClass::Nondeterministic,
            "DEFAULT without a locale answer must classify fail-closed"
        );

        // Install the probe (this test is the only installer in the
        // binary) and classify FROM collisdeterministic.
        lookup_pg_collation_shape::set(|oid| match oid {
            9001 => Ok(Some(PgCollationShape {
                collname: coll_name("icu_det"),
                collnamespace: 11,
                collisdeterministic: true,
            })),
            9002 => Ok(Some(PgCollationShape {
                collname: coll_name("icu_ci"),
                collnamespace: 11,
                collisdeterministic: false,
            })),
            9004 => Err(types_error::PgError::error("collation probe failed").into()),
            _ => Ok(None),
        });
        assert_eq!(
            collation_class(9001),
            CollationClass::OtherDeterministic,
            "a provably deterministic collation keeps equality metadata"
        );
        assert_eq!(
            collation_class(9002),
            CollationClass::Nondeterministic,
            "collisdeterministic = false must never arm byte-hash blooms"
        );
        assert_eq!(
            collation_class(9003),
            CollationClass::Nondeterministic,
            "a dangling attcollation (no pg_collation row) is UNKNOWN: fail closed"
        );
        assert_eq!(
            collation_class(9004),
            CollationClass::Nondeterministic,
            "a probe ERROR is UNKNOWN: fail closed, and blooms are advisory"
        );

        // M4-S8 (the engine-cut substrate finding): with the locale answer
        // installed, DEFAULT-under-C resolves to the honest C class — the
        // class dict/FSST election rides. A non-C default keeps the
        // conservative fallthrough (here: the syscache probe answers None
        // for oid 100 => Nondeterministic).
        assert_eq!(
            collation_class(DEFAULT_COLLATION_OID),
            CollationClass::Nondeterministic,
            "DEFAULT before the locale seam installs stays conservative"
        );
        pg_locale_seams::collation_collate_is_c::set(|oid| {
            Ok(oid == DEFAULT_COLLATION_OID)
        });
        assert_eq!(
            collation_class(DEFAULT_COLLATION_OID),
            CollationClass::C,
            "DEFAULT on a C-locale database is memcmp order: class C"
        );
        assert_eq!(
            collation_class(9001),
            CollationClass::OtherDeterministic,
            "named collations never ride the DEFAULT resolution"
        );
    }
}
