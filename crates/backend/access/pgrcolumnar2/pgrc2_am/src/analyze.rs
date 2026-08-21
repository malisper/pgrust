//! M5a: the ENGINE ANALYZE FOLD read leg (ST-2 made LIVE — the M3
//! remainder + the S5 sketch-leg residue). `ANALYZE` on a pgrcolumnar2
//! table folds the seal-built facts — part-grain footer `StatsRecord`s,
//! the Stats-sidecar distribution sketches (ST-1/OD-2), the footer HLL
//! NDV union, and the FT-6/SortKey cluster witness — into
//! `pg_statistic`-shaped per-column output via the ONE fold implementation
//! (`pgrc2_meta::fold::fold_pg_statistic`). **No sampling scan**: the v2
//! comparator pays a whole-table sampling pass (46.9s at 10m); the fold
//! reads footers + sidecars only (ms class).
//!
//! ## The refusal seam (the fail-open law, ES-1.3 applied to ANALYZE)
//!
//! The fold either serves the WHOLE relation or declines TYPED — and a
//! decline is never an error and never wrong stats: the caller falls open
//! to the pre-existing sampling path (`pgrc2_acquire_sample_rows`), the
//! conservative election. Decline causes are a closed vocabulary
//! ([`FoldDecline`]), censused by the caller. On BANKED data the decline
//! path is unreachable by construction (stats are a seal byproduct; banks
//! ship stats-built — the ST-2 law the M3 estimate-probe cells witnessed,
//! with a born-RED stats-stripped seed).
//!
//! Notable causes:
//! - **stand-in vintage** ([`FoldDecline::StatsWitnessAbsent`]): a part
//!   record without the `STATSF_COMPUTED` witness carries exact `nonnull`
//!   with UNCOMPUTED zero aggregates — folding it would deflate widths
//!   silently (the S4-review C2 lesson applied to ANALYZE).
//! - **deletion-bearing** ([`FoldDecline::DeletionBearing`]): seal-time
//!   counts ignore delete vectors; folding them would serve resurrected
//!   rows to the planner (the C4/DM-2 lesson applied to stats).
//!
//! ## Rendering discipline
//!
//! Output values are CANONICAL BYTES (spec §18.1). The AM stays
//! render-agnostic: [`ColFold`] carries the column's `StorageClass` +
//! `TypeSemantics` + `CollationClass`, and the consumer (`commands/
//! analyze`) admits value lists per the byte-faithfulness gates it owns
//! (byte-eq == value-eq for MCVs; recoverable order for histograms).

// Re-exported for the AM facade (`tableam`): the consumer's render gates.
pub use pgrc2_format::class::{CollationClass, StorageClass, TypeSemantics};
pub use pgrc2_format::sidecar::STATS_MCV_K as PGRC2_STATS_MCV_K;
use pgrc2_format::dirlayout::part_file_name;
use pgrc2_format::ident::schema_fingerprint;
use pgrc2_format::meta::{StatsRecord, STATSF_COMPUTED};
use pgrc2_format::part::SectionKind;
use pgrc2_format::sidecar::{ColDistribution, SidecarKind};
use pgrc2_format::sortkey::SortKeyRecord;
use pgrc2_meta::fold::{fold_pg_statistic, PartColInput};
pub use pgrc2_meta::fold::PgStatColumn;
use pgrc2_read::{PartExpect, TableExpect, VfsPartIo};
use types_error::{PgError, PgResult};
use types_rel::Relation;

use crate::probe::{ClogTxnProbe, SnapshotCommitCheck};

/// The typed decline vocabulary (append-only; `cause()` is the census
/// currency). A decline means "sample instead", never "error" and never
/// "serve anyway".
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FoldDecline {
    /// Never-published table: the sampling path scans nothing at ~0 cost
    /// and writes the never-analyzed convention — no fold to serve.
    NoCommittedPublish,
    /// A part referenced by the manifest carries a delete vector; seal
    /// counts ignore Dv (the C4/DM-2 stats lesson).
    DeletionBearing { part_no: u32 },
    /// The Stats sidecar companion is absent for a part (stats-stripped
    /// twin, or a wholly sketch-free part — all-opaque profiles).
    SidecarAbsent { part_no: u32 },
    /// The sidecar failed envelope validation (refuse-and-rebuild class).
    SidecarStale { part_no: u32, why: &'static str },
    /// A column's Stats section is absent in a part.
    StatsSectionAbsent { part_no: u32, attno: u32 },
    /// A part record without the `STATSF_COMPUTED` witness (stand-in
    /// vintage — exact nonnull, zeroed aggregates).
    StatsWitnessAbsent { part_no: u32, attno: u32 },
    /// The NDV supply declined (footer fold unavailable).
    NdvSupplyAbsent,
}

impl FoldDecline {
    /// Stable census token.
    pub fn cause(&self) -> &'static str {
        match self {
            FoldDecline::NoCommittedPublish => "no-committed-publish",
            FoldDecline::DeletionBearing { .. } => "deletion-bearing",
            FoldDecline::SidecarAbsent { .. } => "sidecar-absent",
            FoldDecline::SidecarStale { .. } => "sidecar-stale",
            FoldDecline::StatsSectionAbsent { .. } => "stats-section-absent",
            FoldDecline::StatsWitnessAbsent { .. } => "stats-witness-absent",
            FoldDecline::NdvSupplyAbsent => "ndv-supply-absent",
        }
    }
}

/// One folded column, with the render-gate facts the consumer needs.
#[derive(Debug, Clone)]
pub struct ColFold {
    pub attno: u32,
    pub stat: PgStatColumn,
    pub class: StorageClass,
    pub semantics: TypeSemantics,
    pub collation_class: CollationClass,
    /// Σ nonnull across parts (exact; the correlation/value-count gate).
    pub nonnull: u64,
}

/// The whole-relation fold.
#[derive(Debug, Clone)]
pub struct RelFold {
    /// Exact committed row count (footer facts — `reltuples` truth).
    pub total_rows: u64,
    /// Every live column, attno-ascending.
    pub cols: Vec<ColFold>,
}

/// Fold outcome: served or typed-declined (the fail-open seam).
#[derive(Debug, Clone)]
pub enum AnalyzeFold {
    Folded(RelFold),
    Declined(FoldDecline),
}

/// Attempt the fold. Errors are real IO/corruption refusals (typed, same
/// classes the scan path raises); "cannot serve" is [`AnalyzeFold::
/// Declined`], never an error.
pub fn analyze_fold(rel: &Relation<'_>) -> PgResult<AnalyzeFold> {
    crate::inval::ensure_inval_registered()?;
    let locator = rel.rd_locator.get();
    let relfilenumber = locator.relNumber as u64;
    let schemas = crate::schema::col_schemas(rel)?;
    let fp = schema_fingerprint(&schemas);
    let dir = crate::dirpath::table_dir_path(locator, rel.rd_backend);

    let check = SnapshotCommitCheck::new(None);
    let expect = TableExpect {
        relfilenumber: Some(relfilenumber),
        spc_db: Some((locator.spcOid, locator.dbOid)),
        schema_fingerprint: Some(fp),
    };
    let recovery_probe = ClogTxnProbe::new();
    let eff = crate::scan::resolve_for_scan(&dir, relfilenumber, &recovery_probe, &check, &expect)?;
    check.take_error()?;
    let Some(eff) = eff else {
        return Ok(AnalyzeFold::Declined(FoldDecline::NoCommittedPublish));
    };

    // NDV supply: the footer HLL union, served through the process-grain
    // fact cache (nanoseconds when any session of this process folded
    // this generation).
    let Some(ndv) = crate::footer::footer_ndv(rel)? else {
        return Ok(AnalyzeFold::Declined(FoldDecline::NdvSupplyAbsent));
    };

    let attnos: Vec<u32> = schemas.iter().map(|s| s.attno).collect();
    let ncols = attnos.len();
    // Per column, per part: (part record, part rows, sketch).
    let mut records: Vec<Vec<StatsRecord>> = vec![Vec::new(); ncols];
    let mut sketches: Vec<Vec<Option<ColDistribution>>> = vec![Vec::new(); ncols];
    let mut part_rows: Vec<u64> = Vec::new();
    // FT-6 cluster witness: Some(attno) while every part's SortKey section
    // leads with the same column; None once falsified.
    let mut cluster_lead: Option<u32> = None;
    let mut first_part = true;

    let reg = crate::inval::registry();
    let mut total_rows: u64 = 0;
    for rec in &eff.manifest.parts {
        if rec.dv_gen != 0 {
            return Ok(AnalyzeFold::Declined(FoldDecline::DeletionBearing {
                part_no: rec.part_no,
            }));
        }
        total_rows += rec.rows;
        part_rows.push(rec.rows);

        // The Stats sidecar companion (ST-1): one file per part, gen 1,
        // key fingerprint = the schema fingerprint (the writer's
        // publish_stats_sidecars contract).
        let dir_io = pgrc2_read::io::VfsTableDir::new(dir.clone());
        let consult = pgrc2_read::sidecar::read_sidecar(
            &dir_io,
            rec.part_no,
            SidecarKind::Stats,
            1,
            None,
            Some(fp),
        )
        .map_err(crate::read_error)?;
        let payload = match consult {
            pgrc2_read::sidecar::SidecarConsult::Hit { payload, .. } => payload,
            pgrc2_read::sidecar::SidecarConsult::Absent => {
                return Ok(AnalyzeFold::Declined(FoldDecline::SidecarAbsent {
                    part_no: rec.part_no,
                }))
            }
            pgrc2_read::sidecar::SidecarConsult::Stale(why) => {
                return Ok(AnalyzeFold::Declined(FoldDecline::SidecarStale {
                    part_no: rec.part_no,
                    why,
                }))
            }
        };
        let dist = pgrc2_format::sidecar::decode_stats_payload(&payload).map_err(|e| {
            Box::new(
                PgError::error(format!(
                    "pgrcolumnar2: Stats sidecar payload for part {}: {e:?}",
                    rec.part_no
                ))
                .with_sqlstate(types_error::ERRCODE_DATA_CORRUPTED),
            )
        })?;

        // Pin the part; read every column's part-grain record + SortKey.
        let pexpect =
            PartExpect::from_manifest(rec, fp, relfilenumber, locator.spcOid, locator.dbOid);
        let path = format!("{dir}/{}", part_file_name(rec.part_no));
        let cpath = std::ffi::CString::new(path.clone()).map_err(|_| {
            Box::new(PgError::error(format!(
                "pgrcolumnar2: part path contains NUL: {path}"
            )))
        })?;
        let pin = reg
            .open_pinned(&pexpect, || {
                Ok(Box::new(VfsPartIo::open(&cpath)?) as Box<dyn pgrc2_read::PartIo>)
            })
            .map_err(crate::read_error)?;
        let part = pin.part();
        let ident = part.ident();
        crate::inval::record_part_key(rel.rd_id, (ident.dev, ident.ino, ident.len));
        let granules = part.footer().granule_count;
        let bands = part.footer().band_count;

        for (i, &attno) in attnos.iter().enumerate() {
            let Some(idx) = part.find_section(SectionKind::Stats, attno, 0) else {
                return Ok(AnalyzeFold::Declined(FoldDecline::StatsSectionAbsent {
                    part_no: rec.part_no,
                    attno,
                }));
            };
            let body = crate::footer::meta_section_body(part, idx)?;
            let record = crate::footer::part_stats_record(&body, granules, bands, attno)?;
            if record.flags & STATSF_COMPUTED == 0 {
                return Ok(AnalyzeFold::Declined(FoldDecline::StatsWitnessAbsent {
                    part_no: rec.part_no,
                    attno,
                }));
            }
            records[i].push(record);
            // The top-level column's sketch is the path_ord == 0 entry;
            // absence is honest (empty value lists), never a decline.
            sketches[i].push(
                dist.iter()
                    .find(|(a, path_ord, _)| *a == attno && *path_ord == 0)
                    .map(|(_, _, d)| d.clone()),
            );
        }

        // FT-6: the per-part clustered witness (SortKey section, part
        // grain — attno 0). nkeys == 0 = undeclared (OD-8).
        let lead = part
            .find_section(SectionKind::SortKey, 0, 0)
            .map(|idx| -> PgResult<Option<u32>> {
                let body = crate::footer::meta_section_body(part, idx)?;
                let rec = SortKeyRecord::decode(&body).map_err(|e| {
                    Box::new(
                        PgError::error(format!("pgrcolumnar2: SortKey record: {e:?}"))
                            .with_sqlstate(types_error::ERRCODE_DATA_CORRUPTED),
                    )
                })?;
                Ok(rec.keys.first().map(|k| k.attno))
            })
            .transpose()?
            .flatten();
        if first_part {
            cluster_lead = lead;
            first_part = false;
        } else if cluster_lead != lead {
            cluster_lead = None;
        }
    }

    if part_rows.is_empty() {
        // A committed but part-less generation: zero rows; the sampling
        // path writes the same truth at the same cost.
        return Ok(AnalyzeFold::Declined(FoldDecline::NoCommittedPublish));
    }

    let mut cols = Vec::with_capacity(ncols);
    for (i, s) in schemas.iter().enumerate() {
        let inputs: Vec<PartColInput<'_>> = records[i]
            .iter()
            .zip(part_rows.iter())
            .zip(sketches[i].iter())
            .map(|((record, &rows), sketch)| PartColInput {
                part_record: record,
                rows,
                sketch: sketch.as_ref(),
            })
            .collect();
        let ndv_est = ndv.get(i).copied().unwrap_or(0) as f64;
        let cluster_declared = cluster_lead == Some(s.attno);
        let Some(stat) = fold_pg_statistic(&inputs, ndv_est, cluster_declared) else {
            // Empty relation (total_rows == 0 handled above per part);
            // structurally unreachable here, but decline honestly.
            return Ok(AnalyzeFold::Declined(FoldDecline::NoCommittedPublish));
        };
        let nonnull: u64 = records[i].iter().map(|r| u64::from(r.nonnull)).sum();
        cols.push(ColFold {
            attno: s.attno,
            stat,
            class: s.class,
            semantics: s.semantics,
            collation_class: s.collation_class,
            nonnull,
        });
    }

    // Fold-vintage witness for the S-1 pg_statistic read leg's honest
    // source stamping (this process wrote fold-derived rows for this
    // relation — the consumer records it AFTER the pg_statistic write).
    Ok(AnalyzeFold::Folded(RelFold { total_rows, cols }))
}

/// Record that fold-derived pg_statistic rows were WRITTEN for `rel`
/// (called by the ANALYZE consumer after `update_attstats` commits its
/// work into the command). Process-lifetime witness only.
pub fn record_fold_vintage(rel: &Relation<'_>) {
    let locator = rel.rd_locator.get();
    crate::factcache::record_fold_vintage((
        locator.spcOid,
        locator.dbOid,
        locator.relNumber as u64,
    ));
}

/// True when THIS process wrote `rel`'s pg_statistic rows via the fold
/// (SketchFold-class provenance). False after restart or on
/// sampled-fallback relations — the conservative AnalyzeSample label.
pub fn is_fold_vintage(rel: &Relation<'_>) -> bool {
    let locator = rel.rd_locator.get();
    crate::factcache::is_fold_vintage((
        locator.spcOid,
        locator.dbOid,
        locator.relNumber as u64,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The decline vocabulary is pinned (append-only census tokens — a
    /// rename breaks every consumer's census diff).
    #[test]
    fn decline_cause_vocabulary_is_pinned() {
        let causes: Vec<&'static str> = vec![
            FoldDecline::NoCommittedPublish.cause(),
            FoldDecline::DeletionBearing { part_no: 1 }.cause(),
            FoldDecline::SidecarAbsent { part_no: 1 }.cause(),
            FoldDecline::SidecarStale { part_no: 1, why: "x" }.cause(),
            FoldDecline::StatsSectionAbsent { part_no: 1, attno: 1 }.cause(),
            FoldDecline::StatsWitnessAbsent { part_no: 1, attno: 1 }.cause(),
            FoldDecline::NdvSupplyAbsent.cause(),
        ];
        assert_eq!(
            causes,
            vec![
                "no-committed-publish",
                "deletion-bearing",
                "sidecar-absent",
                "sidecar-stale",
                "stats-section-absent",
                "stats-witness-absent",
                "ndv-supply-absent",
            ]
        );
    }
}
