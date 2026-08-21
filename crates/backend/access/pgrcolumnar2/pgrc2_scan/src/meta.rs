//! Verdict-plane consult at scan grain (M3-L3): zone-key pruning + PSMA
//! windows + the bloom probe, wired to the reader through the meta
//! sections L2 seals — with the XC-5 census attributing EXACTLY
//! (`bloom_definite_absent` only when the bloom CHANGED the verdict;
//! `pgrc2_meta::census::evaluate_censused` owns that law).

use std::sync::Arc;

use pgrc2_format::meta::{StatsRecord, Verdict, STATS_RECORD_LEN};
use pgrc2_format::part::{SectionKind, SECTIONF_META_ZSTD};
use pgrc2_format::wire::Cur;
use pgrc2_meta::census::{evaluate_censused, psma_candidates_eq_censused, MetaEngagement};
use pgrc2_meta::key::TypedKey;
use pgrc2_meta::profile::MetaProfile;
use pgrc2_meta::verdict::{grain_keys, BloomEvidence, GrainFacts, ZonePredicate};
use pgrc2_meta::{bloom, psma};
use pgrc2_read::{OpenPart, ReadError, ReadResult};

/// The scan-owned probe constant (owned form of `pgrc2_meta::lower`'s
/// borrowed `LoweredConst` inputs — the caller lowers per column profile
/// through `pgrc2_meta::lower::lower_const` and hands the scan the owned
/// image).
#[derive(Debug, Clone)]
pub enum ScanConst {
    /// Byval datum word (ints, bool, dates at word class).
    Word(u64),
    /// Varlena image starting at the 4B-U header (text-class equality).
    VarlenaImage(Vec<u8>),
}

/// One single-column equality probe (the L3 verdict consumers' shape; the
/// range family rides the same evaluate face and is exercised at unit
/// grain).
#[derive(Debug, Clone)]
pub struct ScanPredicate {
    pub attno: u32,
    pub eq: ScanConst,
}

/// Loaded meta plane of one (column, part): raw (unwrapped) section
/// bodies + the derived profile.
pub struct ColumnMeta {
    pub profile: MetaProfile,
    pub stats_body: Option<Vec<u8>>,
    pub psma_body: Option<Vec<u8>>,
    pub bloom_body: Option<Vec<u8>>,
}

fn section_body(
    part: &Arc<OpenPart>,
    kind: SectionKind,
    attno: u32,
    path_ord: u32,
) -> ReadResult<Option<Vec<u8>>> {
    let Some(idx) = part.find_section(kind, attno, path_ord) else {
        return Ok(None);
    };
    let flags = part.sections()[idx].flags;
    let raw = part.section_bytes(idx)?;
    if flags & SECTIONF_META_ZSTD != 0 {
        // SB-6: the self-describing zstd meta envelope — unwrap before any
        // consult (handing a wrapped body to the block walkers is the
        // typed-refusal shape).
        let body = pgrc2_codec::wrapper::meta_unwrap_body(raw.bytes()).map_err(ReadError::Format)?;
        Ok(Some(body))
    } else {
        Ok(Some(raw.bytes().to_vec()))
    }
}

impl ColumnMeta {
    /// Load the meta plane for (attno, path_ord 0) of one part. Absent
    /// sections are first-class (`None` bodies — consult degrades to
    /// no-verdict, never errors).
    pub fn load(
        part: &Arc<OpenPart>,
        profile: MetaProfile,
        attno: u32,
    ) -> ReadResult<ColumnMeta> {
        // OD-10/OD-11 consult kill switches (the M3.psma-ab / M3.bloom-ab
        // A/B apparatus — the house PGRUST_SCAN_PART_RELEASE idiom): an OFF
        // arm loads the section as ABSENT, so consult degrades exactly as
        // it would on a section-less part (no second code path). Default ON.
        let off = |name: &str| {
            matches!(std::env::var(name).as_deref(), Ok("0") | Ok("off"))
        };
        Ok(ColumnMeta {
            profile,
            stats_body: section_body(part, SectionKind::Stats, attno, 0)?,
            psma_body: if off("PGRUST_PGRC2_PSMA_CONSULT") {
                None
            } else {
                section_body(part, SectionKind::Psma, attno, 0)?
            },
            bloom_body: if off("PGRUST_PGRC2_BLOOM_CONSULT") {
                None
            } else {
                section_body(part, SectionKind::Bloom, attno, 0)?
            },
        })
    }

    /// The granule-grain StatsRecord (stats body = granule records, then
    /// band records, then one part record — granule g sits at g*80).
    pub fn granule_record(&self, g: u32) -> Option<StatsRecord> {
        let body = self.stats_body.as_deref()?;
        let off = g as usize * STATS_RECORD_LEN;
        let rec = body.get(off..off + STATS_RECORD_LEN)?;
        StatsRecord::decode(&mut Cur::new(rec)).ok()
    }
}

/// Per-granule verdict of the composed probe: `None` = scan the granule,
/// `Some(psma_window)` = scan restricted to the candidate row window,
/// pruned granules answer via [`GranuleConsult::AllFail`].
pub enum GranuleConsult {
    /// Zone/bloom verdict erased the granule.
    AllFail,
    /// Scan; PSMA narrowed the candidate rows to `[lo, hi)` when present.
    Scan { psma_window: Option<(u32, u32)> },
}

/// Consult one granule for one predicate, folding engagement into the
/// per-worker census (PC-6.1: plain per-worker struct, folded at drain).
pub fn consult_granule(
    meta: &ColumnMeta,
    pred_lowered: &pgrc2_meta::lower::LoweredConst<'_>,
    g: u32,
    rows_in_granule: u32,
    granule_count: u32,
    census: &mut MetaEngagement,
) -> GranuleConsult {
    let Some(rec) = meta.granule_record(g) else {
        return GranuleConsult::Scan { psma_window: None };
    };
    let probe = ZonePredicate::Eq(*pred_lowered);
    let bloom_ev = meta.bloom_body.as_deref().and_then(|body| {
        bloom::bloom_block_for(body, granule_count, g)
            .ok()
            .flatten()
            .map(|(k, block)| BloomEvidence { k, block })
    });
    let v = evaluate_censused(
        &meta.profile,
        GrainFacts {
            rows: rows_in_granule as u64,
        },
        &rec,
        &probe,
        bloom_ev,
        census,
    );
    if matches!(v, Verdict::AllFail) {
        return GranuleConsult::AllFail;
    }
    // PSMA: exact-key equality only (the measured-only zone-key witness
    // law), subtractive AFTER the zone verdict.
    let psma_window = (|| {
        let body = meta.psma_body.as_deref()?;
        let key = match pred_lowered.key {
            Some(TypedKey::Exact(k)) => k.raw(),
            _ => return None,
        };
        let keys = grain_keys(&meta.profile, &rec);
        let (min_key, max_key) = match keys {
            pgrc2_meta::key::TypedKeys::Exact { min, max } => (min.raw(), max.raw()),
            _ => return None,
        };
        let block = psma::psma_block_for(body, granule_count, g).ok().flatten()?;
        psma_candidates_eq_censused(block, min_key, max_key, key, rows_in_granule, census)
            .map(|(lo, hi)| (lo as u32, hi as u32))
    })();
    GranuleConsult::Scan { psma_window }
}
