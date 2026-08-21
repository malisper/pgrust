//! P6-5: the ANALYZE-fold CONSUMER (the other half of `pgrc2_am::analyze`,
//! M5a ST-2). The AM folds seal-built facts into `pg_statistic`-shaped
//! per-column output of CANONICAL BYTES; this module owns the render gates
//! the AM deliberately does not: which value lists may be served for which
//! type semantics, and how canonical bytes become datums of the column type.
//!
//! ## The witness membrane (lane constitution)
//!
//! Everything written here lands in `pg_statistic` / `pg_class` and feeds
//! PLANNING ONLY — selectivities, join order, path costs. Nothing here is an
//! engine correctness input; the engine's own sound-witness vocabulary
//! (bankstats et al.) is a separate plane this module never touches.
//!
//! ## Render gates
//!
//! - **Scalar family** (stanullfrac / stawidth / stadistinct / correlation):
//!   always served — exact-at-seal counts, footer-HLL NDV, the FT-6 cluster
//!   witness. No byte rendering involved.
//! - **MCV**: admitted only where BYTE equality is VALUE equality in both
//!   directions modulo frequency splits the planner survives — the integer
//!   word families, bool, memcmp-ordered fixed types, oid-word enums, and
//!   text under a deterministic collation (PG's texteq is bitwise for
//!   deterministic collations). Floats are admitted with the one documented
//!   split (-0.0/+0.0 and NaN payloads byte-differ yet compare equal): a
//!   split only LOWERS an individual MCV frequency — planning-only,
//!   conservative. numeric (1.0 vs 1.00), interval (1 day vs 24 h) and
//!   timetz byte-differ while comparing equal in ways that can HIDE a heavy
//!   hitter under distinct spellings — refused.
//! - **Histogram**: the sidecar's equi-depth bounds live in CANONICAL BYTE
//!   order. Where byte order IS value order (memcmp-ordered types, text
//!   under the C collation) the bounds serve directly. For the fixed-width
//!   word families (ints, dates/timestamps, floats, bool) byte order is a
//!   value-independent permutation of value order (LE words under memcmp),
//!   so the bounds are a SYSTEMATIC SAMPLE of the column's rank space:
//!   decoded and re-sorted by value they are honest approximate quantiles —
//!   the same estimate class as C's sampled histogram, and the planner's
//!   only consumer. Everything else (numeric, non-C text, opaque) serves no
//!   histogram rather than a wrong one.
//!
//! ## Fail-open law (ES-1.3 applied to ANALYZE)
//!
//! Any "cannot serve" here — a decline from the AM, a column the fold does
//! not cover, an inadmissible configuration — returns `None` and the caller
//! falls open to the sampling path (`pgrc2_acquire_sample_rows`). Never an
//! error, never silently-wrong stats.

use datum::Datum;
use mcx::{self, Mcx, PgVec};
use tableam::{
    Pgrc2CollationClass as Coll, Pgrc2ColFold, Pgrc2RelFold, Pgrc2StorageClass as Class,
    Pgrc2TypeSemantics as Sem,
};
use types_error::PgResult;

use crate::{
    ComputeStats, VacAttrStats, STATISTIC_KIND_CORRELATION, STATISTIC_KIND_HISTOGRAM,
    STATISTIC_KIND_MCV,
};

/// PGRUST_PGRC2_ANALYZE_FOLD=0|off: kill switch back to the sampling path
/// (default ON — the fold is the M5a-designed serving arm; banked data
/// declines only on the typed causes the census records).
pub(crate) fn fold_enabled() -> bool {
    !matches!(
        std::env::var("PGRUST_PGRC2_ANALYZE_FOLD").ok().as_deref().map(str::trim),
        Some("0") | Some("off")
    )
}

/// MCV admission: byte identity must adjudicate value identity (module doc).
fn mcv_admissible(sem: Sem, coll: Coll) -> bool {
    match sem {
        Sem::SignedInt | Sem::UnsignedInt | Sem::Bool | Sem::MemcmpOrdered | Sem::EnumEqOnly => {
            true
        }
        // Documented split: -0.0/+0.0 (and NaN payloads) byte-differ while
        // comparing equal; a split can only lower one entry's frequency.
        Sem::Float => true,
        // texteq is bitwise under deterministic collations.
        Sem::TextCollated => coll != Coll::Nondeterministic,
        Sem::Opaque
        | Sem::PackedNumeric { .. }
        | Sem::NumericUnpacked
        | Sem::IntervalCmp
        | Sem::TimetzUtc => false,
    }
}

/// How (whether) the byte-ordered histogram bounds recover value order.
enum HistMode {
    /// memcmp order IS the type's order: serve the bounds as-is.
    ByteOrder,
    /// Fixed-width word family: decode and re-sort by value (systematic
    /// sample of rank space — module doc).
    Resort,
    /// Not recoverable: serve no histogram.
    Refuse,
}

fn hist_mode(sem: Sem, coll: Coll) -> HistMode {
    match sem {
        Sem::MemcmpOrdered => HistMode::ByteOrder,
        Sem::TextCollated if coll == Coll::C => HistMode::ByteOrder,
        Sem::SignedInt | Sem::UnsignedInt | Sem::Float | Sem::Bool => HistMode::Resort,
        // Enum order is catalog sort order, not oid-word order; numeric /
        // interval / timetz orders are not byte orders; opaque has none.
        Sem::EnumEqOnly
        | Sem::TextCollated
        | Sem::Opaque
        | Sem::PackedNumeric { .. }
        | Sem::NumericUnpacked
        | Sem::IntervalCmp
        | Sem::TimetzUtc => HistMode::Refuse,
    }
}

/// The value-order sort key for `HistMode::Resort` (PG btree order for the
/// word families: NaN sorts greatest, NaN == NaN, -0 == +0 — float_cmp).
#[derive(PartialEq, PartialOrd)]
enum ResortKey {
    I(i64),
    U(u64),
    F(f64),
}

fn resort_cmp(a: &ResortKey, b: &ResortKey) -> core::cmp::Ordering {
    use core::cmp::Ordering;
    match (a, b) {
        (ResortKey::I(x), ResortKey::I(y)) => x.cmp(y),
        (ResortKey::U(x), ResortKey::U(y)) => x.cmp(y),
        (ResortKey::F(x), ResortKey::F(y)) => match (x.is_nan(), y.is_nan()) {
            (true, true) => Ordering::Equal,
            (true, false) => Ordering::Greater,
            (false, true) => Ordering::Less,
            (false, false) => x.partial_cmp(y).unwrap_or(Ordering::Equal),
        },
        // Keys within one column are homogeneous by construction.
        _ => Ordering::Equal,
    }
}

/// Canonical word bytes → the datum word (the inverse of
/// `datum_canonical_bytes`' byval arms: LE bytes, width-truncated, datum
/// extension per class).
fn word_from_bytes(bytes: &[u8], width: usize, signed: bool) -> Option<u64> {
    if bytes.len() != width || width > 8 {
        return None;
    }
    let mut le = [0u8; 8];
    le[..width].copy_from_slice(bytes);
    let mut w = u64::from_le_bytes(le);
    if signed && width < 8 {
        let shift = 64 - width as u32 * 8;
        w = (((w << shift) as i64) >> shift) as u64;
    }
    Some(w)
}

/// Render one canonical value into a datum of the column's type in `mcx`.
/// `None` = the bytes do not fit the declared class (refuse-and-sample).
fn render_datum<'mcx>(
    mcx: Mcx<'mcx>,
    class: Class,
    bytes: &[u8],
) -> PgResult<Option<Datum>> {
    Ok(match class {
        Class::ByvalWord { width, signed } => {
            word_from_bytes(bytes, width as usize, signed).map(Datum::from_u64)
        }
        Class::F32 => word_from_bytes(bytes, 4, false).map(Datum::from_u64),
        Class::F64 => word_from_bytes(bytes, 8, false).map(Datum::from_u64),
        Class::Bool => word_from_bytes(bytes, 1, false).map(Datum::from_u64),
        Class::Fixed { len } => {
            if bytes.len() != len as usize {
                None
            } else {
                let copy = mcx::slice_borrow_in(mcx, bytes)?;
                Some(Datum::from_usize(copy.as_ptr() as usize))
            }
        }
        Class::VarlenaVerbatim => {
            // Canonical bytes are the PAYLOAD (header stripped); rebuild the
            // 4B-U image the operators read.
            let total = bytes.len() + 4;
            let mut img = Vec::with_capacity(total);
            img.extend_from_slice(
                &types_tuple::varatt::set_varsize_4b_word(total as u32).to_ne_bytes(),
            );
            img.extend_from_slice(bytes);
            let copy = mcx::slice_borrow_in(mcx, &img)?;
            Some(Datum::from_usize(copy.as_ptr() as usize))
        }
    })
}

/// The value-order key for a rendered word datum under `Resort`.
fn resort_key(class: Class, sem: Sem, word: u64) -> ResortKey {
    match class {
        Class::F32 => ResortKey::F(f32::from_bits(word as u32) as f64),
        Class::F64 => ResortKey::F(f64::from_bits(word)),
        _ => match sem {
            Sem::SignedInt => ResortKey::I(word as i64),
            _ => ResortKey::U(word),
        },
    }
}

/// pg_statistic negative-fraction convention (compute_scalar_stats /
/// the footer-NDV override): d > 10% of rows → a fraction that scales.
fn stadistinct_convention(d: f64, totalrows: f64) -> f32 {
    if d <= 0.0 {
        return 0.0;
    }
    let d = d.min(totalrows.max(1.0));
    if d > 0.1 * totalrows {
        -(d / totalrows.max(1.0)) as f32
    } else {
        d as f32
    }
}

/// Fill `stats` (the examine_attribute output for this relation) from the
/// AM's fold. `false` = some targeted column is not honestly servable from
/// the fold (fail OPEN: the caller samples instead); nothing is written to
/// the stats structs unless every column serves.
pub(crate) fn apply_fold<'mcx>(
    anl_mcx: Mcx<'mcx>,
    fold: &Pgrc2RelFold,
    vacattrstats: &mut [VacAttrStats<'mcx>],
) -> PgResult<bool> {
    let totalrows = fold.total_rows as f64;
    // All-or-nothing admission first: every targeted column must be std
    // (Scalar/Distinct/Trivial) and covered by the fold.
    for s in vacattrstats.iter() {
        if !matches!(
            s.compute,
            ComputeStats::Scalar | ComputeStats::Distinct | ComputeStats::Trivial
        ) {
            return Ok(false);
        }
        if s.tupattnum < 1
            || !fold.cols.iter().any(|c| c.attno == s.tupattnum as u32)
        {
            return Ok(false);
        }
    }

    for i in 0..vacattrstats.len() {
        let s = &mut vacattrstats[i];
        let col: &Pgrc2ColFold = fold
            .cols
            .iter()
            .find(|c| c.attno == s.tupattnum as u32)
            .expect("admission checked coverage");
        if !fill_column(anl_mcx, s, col, totalrows)? {
            // Mid-way render refusal (corrupt-class rarity): scrub every
            // partially-filled struct so the sampling path starts clean —
            // its compute may write fewer slots than the fold did.
            for s in vacattrstats.iter_mut() {
                reset_stats(anl_mcx, s);
            }
            return Ok(false);
        }
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The inverse of `datum_canonical_bytes`' byval arm: LE truncation
    /// round-trips through sign/zero extension.
    #[test]
    fn word_round_trip() {
        // int2 -3: datum word (sign-extended) → canonical 2 LE bytes → word.
        let w = (-3i64) as u64;
        let bytes = w.to_le_bytes();
        assert_eq!(word_from_bytes(&bytes[..2], 2, true), Some(w));
        // oid word zero-extends.
        assert_eq!(word_from_bytes(&0xFFFFFFFFu32.to_le_bytes(), 4, false), Some(0xFFFF_FFFF));
        // Width mismatch refuses.
        assert_eq!(word_from_bytes(&bytes[..3], 2, true), None);
    }

    /// Resort order is PG btree order for the word families: signed ints by
    /// value, floats with NaN greatest.
    #[test]
    fn resort_order_is_value_order() {
        let mut v = vec![ResortKey::I(10), ResortKey::I(-5), ResortKey::I(0)];
        v.sort_by(resort_cmp);
        assert!(matches!(v[0], ResortKey::I(-5)));
        assert!(matches!(v[2], ResortKey::I(10)));
        let mut f = vec![
            ResortKey::F(f64::NAN),
            ResortKey::F(1.5),
            ResortKey::F(-2.0),
        ];
        f.sort_by(resort_cmp);
        assert!(matches!(f[0], ResortKey::F(x) if x == -2.0));
        assert!(matches!(f[2], ResortKey::F(x) if x.is_nan()));
    }

    /// pg_statistic's negative-fraction convention (the footer-NDV override's
    /// exact arithmetic).
    #[test]
    fn stadistinct_negative_fraction_convention() {
        assert_eq!(stadistinct_convention(50.0, 1000.0), 50.0);
        assert_eq!(stadistinct_convention(500.0, 1000.0), -0.5);
        assert_eq!(stadistinct_convention(0.0, 1000.0), 0.0);
        // Clamped to totalrows first: 2000 → 1000 → the -1.0 all-distinct form.
        assert_eq!(stadistinct_convention(2000.0, 1000.0), -1.0);
    }

    /// The admission vocabulary is pinned: byte-eq==value-eq classes serve
    /// MCVs; equality-normalizing types refuse.
    #[test]
    fn mcv_admission_gates() {
        assert!(mcv_admissible(Sem::SignedInt, Coll::C));
        assert!(mcv_admissible(Sem::MemcmpOrdered, Coll::OtherDeterministic));
        assert!(mcv_admissible(Sem::TextCollated, Coll::OtherDeterministic));
        assert!(!mcv_admissible(Sem::TextCollated, Coll::Nondeterministic));
        assert!(!mcv_admissible(Sem::PackedNumeric { scale: 2 }, Coll::C));
        assert!(!mcv_admissible(Sem::IntervalCmp, Coll::C));
        assert!(!mcv_admissible(Sem::Opaque, Coll::C));
    }

    #[test]
    fn hist_admission_gates() {
        assert!(matches!(hist_mode(Sem::MemcmpOrdered, Coll::C), HistMode::ByteOrder));
        assert!(matches!(hist_mode(Sem::TextCollated, Coll::C), HistMode::ByteOrder));
        assert!(matches!(
            hist_mode(Sem::TextCollated, Coll::OtherDeterministic),
            HistMode::Refuse
        ));
        assert!(matches!(hist_mode(Sem::SignedInt, Coll::C), HistMode::Resort));
        assert!(matches!(hist_mode(Sem::Float, Coll::C), HistMode::Resort));
        assert!(matches!(
            hist_mode(Sem::PackedNumeric { scale: 2 }, Coll::C),
            HistMode::Refuse
        ));
        assert!(matches!(hist_mode(Sem::EnumEqOnly, Coll::C), HistMode::Refuse));
    }
}

fn reset_stats<'mcx>(anl_mcx: Mcx<'mcx>, s: &mut VacAttrStats<'mcx>) {
    s.stats_valid = false;
    s.stanullfrac = 0.0;
    s.stawidth = 0;
    s.stadistinct = 0.0;
    for k in 0..s.stakind.len() {
        s.stakind[k] = 0;
        s.staop[k] = types_core::InvalidOid;
        s.stacoll[k] = types_core::InvalidOid;
        s.stanumbers[k] = PgVec::new_in(anl_mcx);
        s.stavalues[k] = PgVec::new_in(anl_mcx);
        s.stavalues_set[k] = false;
    }
}

fn fill_column<'mcx>(
    anl_mcx: Mcx<'mcx>,
    s: &mut VacAttrStats<'mcx>,
    col: &Pgrc2ColFold,
    totalrows: f64,
) -> PgResult<bool> {
    let stat = &col.stat;

    if totalrows <= 0.0 {
        // Committed-empty relations decline AM-side; defensive only.
        return Ok(false);
    }

    // ---- scalar family (always served) ----
    s.stats_valid = true;
    s.stanullfrac = stat.stanullfrac.clamp(0.0, 1.0) as f32;
    s.stawidth = if s.typlen > 0 {
        s.typlen as i32
    } else if stat.stawidth > 0.0 {
        // Fold widths are payload averages; pg_statistic's varlena
        // convention includes the 4-byte header (compute_scalar_stats
        // counts VARSIZE).
        (stat.stawidth + 4.0).round() as i32
    } else {
        0
    };
    if col.nonnull == 0 {
        // Null-only column: C's convention (nullfrac 1, width 0, distinct 0).
        s.stanullfrac = 1.0;
        s.stawidth = 0;
        s.stadistinct = 0.0;
        return Ok(true);
    }
    s.stadistinct = stadistinct_convention(stat.stadistinct, totalrows);

    let has_eq = s.extra.eqopr != types_core::InvalidOid;
    let has_lt = s.extra.ltopr != types_core::InvalidOid;
    let mut slot_idx = 0usize;

    // ---- MCV ----
    if has_eq
        && !stat.sta_mcv.is_empty()
        && mcv_admissible(col.semantics, col.collation_class)
    {
        let n = stat.sta_mcv.len();
        let mut values: PgVec<'mcx, Datum> = mcx::vec_with_capacity_in(anl_mcx, n)?;
        let mut freqs: PgVec<'mcx, f32> = mcx::vec_with_capacity_in(anl_mcx, n)?;
        for (bytes, freq) in &stat.sta_mcv {
            let Some(d) = render_datum(anl_mcx, col.class, bytes)? else {
                return Ok(false);
            };
            values.push(d);
            freqs.push(*freq as f32);
        }
        s.stakind[slot_idx] = STATISTIC_KIND_MCV;
        s.staop[slot_idx] = s.extra.eqopr;
        s.stacoll[slot_idx] = s.attrcollid;
        s.stanumbers[slot_idx] = freqs;
        s.stavalues[slot_idx] = values;
        s.stavalues_set[slot_idx] = true;
        slot_idx += 1;
    }

    // ---- histogram ----
    if has_lt && stat.sta_histogram.len() >= 2 {
        match hist_mode(col.semantics, col.collation_class) {
            HistMode::ByteOrder => {
                let n = stat.sta_histogram.len();
                let mut values: PgVec<'mcx, Datum> = mcx::vec_with_capacity_in(anl_mcx, n)?;
                for bytes in &stat.sta_histogram {
                    let Some(d) = render_datum(anl_mcx, col.class, bytes)? else {
                        return Ok(false);
                    };
                    values.push(d);
                }
                s.stakind[slot_idx] = STATISTIC_KIND_HISTOGRAM;
                s.staop[slot_idx] = s.extra.ltopr;
                s.stacoll[slot_idx] = s.attrcollid;
                s.stavalues[slot_idx] = values;
                s.stavalues_set[slot_idx] = true;
                slot_idx += 1;
            }
            HistMode::Resort => {
                let mut keyed: Vec<(ResortKey, u64)> = Vec::with_capacity(stat.sta_histogram.len());
                for bytes in &stat.sta_histogram {
                    let (width, signed) = match col.class {
                        Class::ByvalWord { width, signed } => (width as usize, signed),
                        Class::F32 => (4, false),
                        Class::F64 => (8, false),
                        Class::Bool => (1, false),
                        _ => return Ok(false),
                    };
                    let Some(w) = word_from_bytes(bytes, width, signed) else {
                        return Ok(false);
                    };
                    keyed.push((resort_key(col.class, col.semantics, w), w));
                }
                keyed.sort_by(|a, b| resort_cmp(&a.0, &b.0));
                let mut values: PgVec<'mcx, Datum> =
                    mcx::vec_with_capacity_in(anl_mcx, keyed.len())?;
                for (_, w) in &keyed {
                    values.push(Datum::from_u64(*w));
                }
                s.stakind[slot_idx] = STATISTIC_KIND_HISTOGRAM;
                s.staop[slot_idx] = s.extra.ltopr;
                s.stacoll[slot_idx] = s.attrcollid;
                s.stavalues[slot_idx] = values;
                s.stavalues_set[slot_idx] = true;
                slot_idx += 1;
            }
            HistMode::Refuse => {}
        }
    }

    // ---- correlation (P-8: seal sortedness + the FT-6 cluster witness) ----
    if has_lt {
        let mut corrs: PgVec<'mcx, f32> = mcx::vec_with_capacity_in(anl_mcx, 1)?;
        corrs.push(stat.stacorrelation as f32);
        s.stakind[slot_idx] = STATISTIC_KIND_CORRELATION;
        s.staop[slot_idx] = s.extra.ltopr;
        s.stacoll[slot_idx] = s.attrcollid;
        s.stanumbers[slot_idx] = corrs;
    }

    Ok(true)
}
