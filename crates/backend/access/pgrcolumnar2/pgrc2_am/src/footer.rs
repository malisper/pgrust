//! Table-grain footer facts for the planner and ANALYZE (WW-2, census S02 /
//! register U-22): the pgrcolumnar2 sibling of the old AM's
//! `pgrcolumnar::footer_ndv` / `footer_rows` surface (`tableam` re-exports
//! both AMs' faces side by side).
//!
//! ## What is served
//!
//! - [`footer_size`] — `(total rows, total on-disk part bytes)` straight
//!   from the effective manifest's part records (no part opens). Consumed
//!   by `tableam::table_relation_estimate_size`: without it every
//!   pgrcolumnar2 plan sized the relation from the deliberately-EMPTY main
//!   fork (0 blocks ⇒ plan-time tuples 0), which also starved every
//!   NDV consumer downstream (`add_unique_group_var` clamps to
//!   `rel->tuples`, and a negative `stadistinct` multiplies by it).
//! - [`footer_ndv`] — per-column whole-table NDV, the max-per-register
//!   union of every part's `NdvRegisters` section (spec §8.4 dense HLL —
//!   "store the mergeable form"; per-part `ndv_est` scalars cannot be
//!   combined, a sum double-counts values shared across parts). Indexed by
//!   `attnum - 1` (attnos are dense 1-based — `schema::col_schemas`
//!   refuses dropped columns); `0` = unknown for that column (section
//!   absent in some part: Opaque semantics, or a profile without NDV).
//!   Consumed by ANALYZE's footer-NDV override (sampled Duj1 underestimates
//!   heavy-tailed text NDV 100-1500x; the ingest-time whole-stream sketch
//!   wins) and by plancat's no-pg_statistic group-key estimation.
//! - [`footer_col_bytes`] — per-column ON-DISK data bytes, Σ over every
//!   part of the column's data-section lengths (`StreamDir`/`Stream`/
//!   `PathTable` entries in the section table — dict payload and overflow
//!   streams are `Stream` sections under the column's attno, so a
//!   part-global dict's bytes charge to the column that owns it). EXACT:
//!   the section table is the writer's own byte accounting, and the sum
//!   costs zero extra IO (the table is resident on every open). Metadata
//!   sections (Stats/Psma/Bloom/NdvRegisters/SortKey) are excluded: they
//!   are uniform, small, and consulted for pruning regardless of the plan's
//!   column need-set. Consumed by plancat for column-fraction seqscan disk
//!   costing (the pgrc2 arm of `costsize::pgrcolumnar_scan_col_fraction`).
//! - [`footer_avg_widths`] — per-column planner-convention average datum
//!   width (`0` = unknown): round(Σ `byte_len_sum` / Σ `nonnull`) +
//!   VARHDRSZ, folded over every part's PART-grain `StatsRecord`.
//!   DERIVED-EXACT: both sums are ingest-time whole-stream counts (never
//!   sampled), guarded by the `STATSF_COMPUTED` witness (#598) in every
//!   part; the +4 mirrors `stawidth`'s `VARSIZE_ANY` convention for
//!   4-byte-header varlena (short-header slack accepted — cost-model
//!   grade). Only LenStats columns ever answer (fixed-width columns read 0
//!   and the consumer's `get_typavgwidth` fallback is already exact for
//!   them). Consumed by plancat's pgrc2 `attr_widths` pre-fill (the
//!   `set_rel_width` cache), replacing the 32-byte type-default guess on
//!   never-/sampled-ANALYZEd wide text columns.
//!
//! ## Visibility
//!
//! `SnapshotCommitCheck::new(None)` — committed-or-own-transaction, exactly
//! the visibility the AM's ANALYZE scan face runs under
//! (`table_beginscan_analyze` carries no snapshot). Plan-time consumers get
//! advisory statistics, not query answers, so snapshot-exactness is not
//! owed (the old AM's session part cache makes the same call).
//!
//! ## Cost / caching
//!
//! The planner asks on EVERY plan of a pgrcolumnar2 rel (the old-AM lesson:
//! the uncached footer re-read was ~1.3ms of a ~1.9ms plan constant —
//! fixed-overhead audit 2026-07-14). Every ask re-resolves the effective
//! manifest (two small file reads — staleness is structurally impossible),
//! but the folded facts are cached in TWO tiers: the session TLS block
//! keyed by `(relfilenumber, manifest generation)` and, since M5a, the
//! PROCESS-GRAIN [`crate::factcache`] keyed by `(spc, db, relfilenumber)`
//! × `(gen, publisher_fxid, schema fingerprint)` — the deep fold (HLL
//! union + cost facts, one part-open pass) runs once per PROCESS per
//! published generation, not once per session (the M4-S5 revert's
//! measured +3ms/1m, +22ms/10m per cold-session plan is the born-RED this
//! grain exists to keep at ~0). Entries for dropped/truncated tables are
//! dropped by the relcache-invalidation face and otherwise bounded by the
//! tables the process ever touched; a stale GENERATION can never be
//! served because the resolve precedes every cache read and entries are
//! content-addressed by the resolved key.
//!
//! ## Error discipline
//!
//! Decode refusals are typed data-corruption errors, never silent zeros
//! (the `Hll::decode_section` contract). A register-length mismatch
//! ACROSS parts refuses too: `Hll::merge` only debug-asserts equal
//! lengths, so the guard lives here, before the merge.

use pgrc2_format::dirlayout::part_file_name;
use pgrc2_format::ident::schema_fingerprint;
use pgrc2_format::meta::{stats_section_len, StatsRecord, STATSF_COMPUTED, STATS_RECORD_LEN};
use pgrc2_format::part::SectionKind;
use pgrc2_format::wire::Cur;
use pgrc2_meta::ndv::Hll;
use pgrc2_read::{OpenPart, PartExpect, TableExpect, VfsPartIo};
use types_error::{PgError, PgResult};
use types_rel::Relation;

use crate::probe::{ClogTxnProbe, SnapshotCommitCheck};

/// The planner width convention's varlena header allowance (`VARHDRSZ`):
/// `stawidth` averages `VARSIZE_ANY` of the stored datum, which carries the
/// 4-byte header; `byte_len_sum` is payload bytes (builder.rs "varlena
/// payload units").
const VARLENA_HEADER_BYTES: u64 = 4;

/// Per-column cost facts for the planner (attnum-1 indexed), folded from
/// every part in the effective manifest and cached with the NDV union
/// (same part-open pass, same generation key).
#[derive(Debug, Clone)]
pub struct ColCostFacts {
    /// Σ on-disk data-section bytes (StreamDir/Stream/PathTable) per
    /// column — exact, from the section tables alone.
    pub disk_bytes: Vec<u64>,
    /// Planner-convention average datum width (payload avg + VARHDRSZ);
    /// 0 = unknown (Stats section or `STATSF_COMPUTED` witness absent in
    /// any part, non-LenStats column, or empty sums).
    pub avg_width: Vec<i32>,
}

/// The session-cached fold for one relfilenumber (lives in
/// [`crate::session`]'s single census-pinned TLS block).
#[derive(Debug, Clone)]
pub struct FooterFacts {
    /// The manifest generation the facts were folded from.
    pub gen: u64,
    /// Σ part rows.
    pub rows: u64,
    /// Σ part file bytes (on-disk size of the sealed parts).
    pub bytes: u64,
    /// Per-column NDV (attnum-1 indexed; 0 = unknown). `None` = the deep
    /// fold has not run for this generation (size-only asks never open
    /// parts). Invariant: `ndv.is_some() == cols.is_some()` — the deep
    /// fold computes both in one part-open pass.
    pub ndv: Option<Vec<u64>>,
    /// Per-column cost facts; `None` exactly when `ndv` is `None`.
    pub cols: Option<ColCostFacts>,
}

/// Total committed `(rows, on-disk bytes)`; `None` while the table has no
/// committed publish (the never-loaded posture — callers fall through to
/// their pre-existing convention).
pub fn footer_size(rel: &Relation<'_>) -> PgResult<Option<(u64, u64)>> {
    let Some(facts) = with_facts(rel, false)? else {
        return Ok(None);
    };
    Ok(Some((facts.rows, facts.bytes)))
}

/// Per-column whole-table NDV (attnum-1 indexed; 0 = unknown); `None`
/// while the table has no committed publish.
pub fn footer_ndv(rel: &Relation<'_>) -> PgResult<Option<Vec<u64>>> {
    let Some(facts) = with_facts(rel, true)? else {
        return Ok(None);
    };
    Ok(facts.ndv)
}

/// Per-column on-disk data bytes (attnum-1 indexed) for column-fraction
/// disk costing; `None` while the table has no committed publish.
pub fn footer_col_bytes(rel: &Relation<'_>) -> PgResult<Option<Vec<u64>>> {
    let Some(facts) = with_facts(rel, true)? else {
        return Ok(None);
    };
    Ok(facts.cols.map(|c| c.disk_bytes))
}

/// Per-column planner-convention average datum widths (attnum-1 indexed;
/// 0 = unknown); `None` while the table has no committed publish.
pub fn footer_avg_widths(rel: &Relation<'_>) -> PgResult<Option<Vec<i32>>> {
    let Some(facts) = with_facts(rel, true)? else {
        return Ok(None);
    };
    Ok(facts.cols.map(|c| c.avg_width))
}

/// Resolve the effective manifest and serve the (possibly cached) fold.
/// `deep` = open the parts and fold the per-column facts (NDV union +
/// cost facts, one pass) if the cached entry does not carry them yet.
fn with_facts(rel: &Relation<'_>, deep: bool) -> PgResult<Option<FooterFacts>> {
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
        return Ok(None);
    };
    let gen = eff.manifest.header.gen;
    let publisher_fxid = eff.manifest.header.publisher_fxid;
    let slot: crate::factcache::RelSlot =
        (locator.spcOid, locator.dbOid, relfilenumber);
    crate::factcache::record_relid(rel.rd_id, slot);

    // Session cache probe under the resolved generation (a hit never
    // serves a stale generation because the resolve above is
    // unconditional).
    let cached = crate::session::with_footer_facts(|m| {
        m.get(&relfilenumber).filter(|f| f.gen == gen).cloned()
    });
    if let Some(f) = &cached {
        if !deep || f.ndv.is_some() {
            return Ok(cached);
        }
    }

    // M5a: process-grain fact cache (the S-2 re-home; module doc in
    // `factcache`). A deep ask cold in THIS session but folded by any
    // backend of this process for the SAME (gen, publisher_fxid,
    // schema fingerprint) serves in nanoseconds — the per-connection
    // cold fold the S5 revert measured (+3ms/1m, +22ms/10m per plan)
    // runs once per process per published generation instead.
    if deep {
        if let Some(f) = crate::factcache::probe(slot, gen, publisher_fxid, fp) {
            let facts = (*f).clone();
            crate::session::with_footer_facts(|m| {
                m.insert(relfilenumber, facts.clone());
            });
            return Ok(Some(facts));
        }
    }

    let mut rows: u64 = 0;
    let mut bytes: u64 = 0;
    for rec in &eff.manifest.parts {
        rows += rec.rows;
        bytes += rec.file_len;
    }

    let (ndv, cols) = if deep {
        let attnos: Vec<u32> = schemas.iter().map(|s| s.attno).collect();
        let mut acc: Vec<ColNdvAcc> = attnos.iter().map(|_| ColNdvAcc::default()).collect();
        let mut wacc: Vec<ColWidthAcc> = attnos.iter().map(|_| ColWidthAcc::default()).collect();
        let mut disk_bytes: Vec<u64> = vec![0; attnos.len()];
        let reg = crate::inval::registry();
        for rec in &eff.manifest.parts {
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
            let ident = pin.part().ident();
            crate::inval::record_part_key(rel.rd_id, (ident.dev, ident.ino, ident.len));
            fold_part_ndv(pin.part(), &attnos, &mut acc)?;
            fold_part_disk_bytes(pin.part(), &mut disk_bytes);
            fold_part_widths(pin.part(), &attnos, &mut wacc)?;
        }
        (
            Some(acc.into_iter().map(ColNdvAcc::value).collect()),
            Some(ColCostFacts {
                disk_bytes,
                avg_width: wacc.into_iter().map(ColWidthAcc::value).collect(),
            }),
        )
    } else {
        match cached {
            Some(f) => (f.ndv, f.cols),
            None => (None, None),
        }
    };

    let facts = FooterFacts { gen, rows, bytes, ndv, cols };
    if deep {
        // The part-walk ran (the process probe above missed): publish the
        // fold process-wide so every other session of this generation
        // serves it without opening a part.
        crate::factcache::publish(
            slot,
            gen,
            publisher_fxid,
            fp,
            std::sync::Arc::new(facts.clone()),
        );
    }
    crate::session::with_footer_facts(|m| {
        m.insert(relfilenumber, facts.clone());
    });
    Ok(Some(facts))
}

/// One column's running union across parts.
#[derive(Debug, Default)]
struct ColNdvAcc {
    hll: Option<Hll>,
    /// A part without the section for this column: the column's NDV is
    /// unknown at table grain (0), regardless of other parts (a partial
    /// union under-counts and would be served as truth).
    absent: bool,
}

/// Union one part's `NdvRegisters` sections into `acc` (the caller holds
/// the pin; the per-section decode/guard/merge core is [`ColNdvAcc::
/// merge_section_body`], pinnable without a part image).
fn fold_part_ndv(part: &OpenPart, attnos: &[u32], acc: &mut [ColNdvAcc]) -> PgResult<()> {
    debug_assert_eq!(attnos.len(), acc.len());
    for (i, &attno) in attnos.iter().enumerate() {
        let a = &mut acc[i];
        if a.absent {
            continue;
        }
        let Some(idx) = part.find_section(SectionKind::NdvRegisters, attno, 0) else {
            a.mark_absent();
            continue;
        };
        let body = meta_section_body(part, idx)?;
        a.merge_section_body(attno, &body)?;
    }
    Ok(())
}

/// Fetch one META-PLANE section body, unwrapping the SB-6 zstd meta
/// envelope when `SECTIONF_META_ZSTD` marks the stored form (the v4
/// writer's CMP-F wrapped meta-section class — Stats/Psma/Bloom/
/// NdvRegisters ship wrapped when the wrap clears the ≥20% gate; the
/// l4 footer-oracle read pattern, carried to the AM's read leg).
pub(crate) fn meta_section_body(part: &OpenPart, idx: usize) -> PgResult<Vec<u8>> {
    let flags = part.sections()[idx].flags;
    let raw = part.section_bytes(idx).map_err(crate::read_error)?;
    if flags & pgrc2_format::part::SECTIONF_META_ZSTD != 0 {
        pgrc2_codec::wrapper::meta_unwrap_body(raw.bytes())
            .map_err(|e| Box::new(PgError::error(format!("pgrcolumnar2 meta section unwrap: {e}"))))
    } else {
        Ok(raw.bytes().to_vec())
    }
}

/// Sum each column's DATA-section bytes from one part's section table
/// (StreamDir/Stream/PathTable — the bytes a scan of that column reads;
/// metadata sections excluded, doc at [`footer_col_bytes`]). Entries with
/// attno 0 (part-level) or past the schema width are ignored — the sum is
/// a costing fraction's numerator/denominator, never an exactness claim
/// about unknown sections.
fn fold_part_disk_bytes(part: &OpenPart, disk_bytes: &mut [u64]) {
    section_data_bytes(part.sections(), disk_bytes);
}

/// The pure core of [`fold_part_disk_bytes`] (pinnable without a part
/// image): attribute data-section lengths to their columns.
fn section_data_bytes(entries: &[pgrc2_format::part::SectionEntry], disk_bytes: &mut [u64]) {
    for e in entries {
        let data = matches!(
            SectionKind::from_u16(e.kind),
            Ok(SectionKind::StreamDir) | Ok(SectionKind::Stream) | Ok(SectionKind::PathTable)
        );
        if !data || e.attno == 0 {
            continue;
        }
        if let Some(slot) = disk_bytes.get_mut(e.attno as usize - 1) {
            *slot += e.len;
        }
    }
}

/// Fold one part's PART-grain `StatsRecord` per column into the width
/// accumulators. Absence of the Stats section in any part poisons the
/// column (the NDV law: a partial fold served as truth would mis-average).
fn fold_part_widths(part: &OpenPart, attnos: &[u32], acc: &mut [ColWidthAcc]) -> PgResult<()> {
    debug_assert_eq!(attnos.len(), acc.len());
    let granules = part.footer().granule_count;
    let bands = part.footer().band_count;
    for (i, &attno) in attnos.iter().enumerate() {
        let a = &mut acc[i];
        if a.poisoned {
            continue;
        }
        let Some(idx) = part.find_section(SectionKind::Stats, attno, 0) else {
            a.mark_absent();
            continue;
        };
        let body = meta_section_body(part, idx)?;
        let rec = part_stats_record(&body, granules, bands, attno)?;
        a.merge_part_record(&rec);
    }
    Ok(())
}

/// Slice + decode the ONE part-grain record at the tail of a Stats section
/// body (spec §8.1: granule records, then band records, then one part
/// record — counts closed-form from part rows).
pub(crate) fn part_stats_record(
    body: &[u8],
    granules: u32,
    bands: u32,
    attno: u32,
) -> PgResult<StatsRecord> {
    let expect = stats_section_len(granules, bands);
    if body.len() != expect {
        return Err(Box::new(
            PgError::error(format!(
                "pgrcolumnar2: Stats section for column {attno}: body {} bytes, geometry \
                 wants {expect}",
                body.len()
            ))
            .with_sqlstate(types_error::ERRCODE_DATA_CORRUPTED),
        ));
    }
    let off = (granules as usize + bands as usize) * STATS_RECORD_LEN;
    let mut c = Cur::new(&body[off..]);
    StatsRecord::decode(&mut c).map_err(|e| {
        Box::new(
            PgError::error(format!(
                "pgrcolumnar2: Stats part record for column {attno}: {e}"
            ))
            .with_sqlstate(types_error::ERRCODE_DATA_CORRUPTED),
        )
    })
}

/// One column's running width fold across parts.
#[derive(Debug, Default)]
struct ColWidthAcc {
    byte_len_sum: u64,
    nonnull: u64,
    /// Stats section absent in a part, or a record without the
    /// `STATSF_COMPUTED` witness (#598): the average is unknowable at
    /// table grain — a stand-in-sealed part carries exact `nonnull` with
    /// an UNCOMPUTED zero `byte_len_sum`, which would silently deflate
    /// the average if merged.
    poisoned: bool,
}

impl ColWidthAcc {
    fn mark_absent(&mut self) {
        self.poisoned = true;
        self.byte_len_sum = 0;
        self.nonnull = 0;
    }

    fn merge_part_record(&mut self, rec: &StatsRecord) {
        if rec.flags & STATSF_COMPUTED == 0 {
            self.mark_absent();
            return;
        }
        self.byte_len_sum += rec.byte_len_sum;
        self.nonnull += rec.nonnull as u64;
    }

    /// The served width: 0 = unknown. `byte_len_sum == 0` also reads
    /// unknown — LenStats::None columns (fixed-width: the consumer's type
    /// width is already exact) and all-empty-string columns (conservative;
    /// documented) are indistinguishable in the record.
    fn value(self) -> i32 {
        if self.poisoned || self.nonnull == 0 || self.byte_len_sum == 0 {
            return 0;
        }
        let avg = (self.byte_len_sum + self.nonnull / 2) / self.nonnull + VARLENA_HEADER_BYTES;
        avg.min(i32::MAX as u64) as i32
    }
}

impl ColNdvAcc {
    /// A part without the section: unknown at table grain, regardless of
    /// other parts (a partial union served as truth would under-count).
    fn mark_absent(&mut self) {
        self.absent = true;
        self.hll = None;
    }

    /// Decode one section body and union it in. Refusals are typed
    /// data-corruption errors, never silent zeros (the
    /// `Hll::decode_section` contract), and a register-count drift ACROSS
    /// parts refuses too — `Hll::merge` only debug-asserts equal counts.
    fn merge_section_body(&mut self, attno: u32, body: &[u8]) -> PgResult<()> {
        let (_hdr, hll) = Hll::decode_section(body).map_err(|e| {
            Box::new(
                PgError::error(format!(
                    "pgrcolumnar2: NdvRegisters section for column {attno}: {e}"
                ))
                .with_sqlstate(types_error::ERRCODE_DATA_CORRUPTED),
            )
        })?;
        match &mut self.hll {
            Some(u) => {
                if u.regs_len() != hll.regs_len() {
                    return Err(Box::new(
                        PgError::error(format!(
                            "pgrcolumnar2: NdvRegisters register-count mismatch across parts \
                             for column {attno} ({} vs {})",
                            u.regs_len(),
                            hll.regs_len()
                        ))
                        .with_sqlstate(types_error::ERRCODE_DATA_CORRUPTED),
                    ));
                }
                u.merge(&hll);
            }
            None => self.hll = Some(hll),
        }
        Ok(())
    }

    /// The served value: 0 = unknown (absent anywhere, or never present).
    fn value(self) -> u64 {
        match (self.hll, self.absent) {
            (Some(h), false) => h.estimate() as u64,
            _ => 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hll_body(vals: impl Iterator<Item = u64>) -> Vec<u8> {
        let mut h = Hll::default();
        for v in vals {
            h.observe(&v.to_le_bytes());
        }
        let mut b = Vec::new();
        h.encode_section(&mut b);
        b
    }

    /// Fold per-part section presence/bodies for one column the way
    /// `fold_part_ndv` drives the acc: `Some(body)` = the part carries the
    /// section, `None` = absent in that part.
    fn fold_col(parts: &[Option<Vec<u8>>]) -> PgResult<u64> {
        let mut acc = ColNdvAcc::default();
        for p in parts {
            if acc.absent {
                continue;
            }
            match p {
                Some(body) => acc.merge_section_body(1, body)?,
                None => acc.mark_absent(),
            }
        }
        Ok(acc.value())
    }

    /// The register union is the lossless cross-part fold: overlapping
    /// value sets estimate the UNION (a scalar sum would double-count).
    #[test]
    fn cross_part_union_estimates_the_union() {
        let est = fold_col(&[Some(hll_body(0..1000)), Some(hll_body(500..1500))])
            .expect("fold");
        // True union is 1500 (500..1000 shared); precision-10 dense HLL is
        // ~3% RSE — a 10% band is deterministic here (fixed fixture bytes,
        // pure estimate function).
        assert!(
            (1350..=1650).contains(&(est as i64)),
            "union estimate {est} outside the 1500 band"
        );
        // The sum trap the union exists to avoid reads ~2000.
        assert!((est as i64) < 1800, "union estimate {est} looks like a sum");
    }

    /// A column missing its section in ANY part reads 0 (unknown) — a
    /// partial union served as truth would under-count. Absence poisons in
    /// both orders, and a never-present column reads 0 too.
    #[test]
    fn absence_in_any_part_poisons_the_column() {
        assert_eq!(fold_col(&[Some(hll_body(0..50)), None]).expect("fold"), 0);
        assert_eq!(fold_col(&[None, Some(hll_body(0..50))]).expect("fold"), 0);
        assert_eq!(fold_col(&[None, None]).expect("fold"), 0);
    }

    #[test]
    fn small_set_estimate_is_tight() {
        let est = fold_col(&[Some(hll_body(0..100))]).expect("fold");
        assert!(
            (95..=105).contains(&(est as i64)),
            "small-range linear counting drifted: {est}"
        );
    }

    /// `Hll::merge` only debug-asserts equal register counts — the fold
    /// must refuse a cross-part precision drift loudly in release too.
    #[test]
    fn cross_part_precision_drift_refuses() {
        // A structurally-valid precision-8 section image (decode accepts
        // any precision <= 16 whose reg_len matches).
        let mut p8 = vec![1u8, 8u8, 0, 0];
        p8.extend_from_slice(&256u32.to_le_bytes());
        p8.extend(std::iter::repeat(0u8).take(256));
        let err = fold_col(&[Some(hll_body(0..10)), Some(p8)]).expect_err("must refuse");
        assert_eq!(err.sqlstate(), types_error::ERRCODE_DATA_CORRUPTED);
        assert!(
            err.message().contains("register-count mismatch"),
            "wrong refusal: {}",
            err.message()
        );
    }

    /// Corrupt section bodies refuse typed (never a silent 0 — the
    /// versioned-envelope fail-closed law).
    #[test]
    fn corrupt_section_refuses_typed() {
        let mut bad = vec![2u8, 10u8, 0, 0]; // unknown algo
        bad.extend_from_slice(&1024u32.to_le_bytes());
        bad.extend(std::iter::repeat(0u8).take(1024));
        let err = fold_col(&[Some(bad)]).expect_err("must refuse");
        assert_eq!(err.sqlstate(), types_error::ERRCODE_DATA_CORRUPTED);
        assert!(
            err.message().contains("NdvRegisters"),
            "wrong refusal: {}",
            err.message()
        );
    }

    // --- pgrc2-costing: per-column cost-fact folds -----------------------

    use pgrc2_format::part::{SectionEntry, SECTION_OPTIONAL};

    fn sec(kind: u16, attno: u32, len: u64) -> SectionEntry {
        SectionEntry { off: 0, len, kind, flags: 0, attno, path_ord: 0, crc: 0 }
    }

    /// Data-section bytes attribute to their columns; metadata sections,
    /// part-level (attno 0) entries, unknown kinds, and out-of-schema
    /// attnos never contribute. Dict payload/overflow are Stream sections
    /// under the column's attno, so a shared dict charges its owner.
    #[test]
    fn disk_bytes_attribute_data_sections_only() {
        let entries = vec![
            sec(SectionKind::StreamDir.as_u16(), 1, 100),
            sec(SectionKind::Stream.as_u16(), 1, 1_000), // values
            sec(SectionKind::Stream.as_u16(), 1, 5_000), // dict payload
            sec(SectionKind::Stream.as_u16(), 2, 2_000),
            sec(SectionKind::PathTable.as_u16(), 2, 50),
            sec(SectionKind::Stats.as_u16(), 1, 999_999), // metadata: excluded
            sec(SectionKind::Bloom.as_u16(), 2, 999_999), // metadata: excluded
            sec(SectionKind::NdvRegisters.as_u16(), 1, 999_999),
            sec(SectionKind::SidecarDir.as_u16(), 0, 999_999), // part-level
            SectionEntry {
                off: 0,
                len: 777,
                kind: 999, // unknown optional kind: skipped, never counted
                flags: SECTION_OPTIONAL,
                attno: 1,
                path_ord: 0,
                crc: 0,
            },
            sec(SectionKind::Stream.as_u16(), 40, 123), // past the schema
        ];
        let mut disk = vec![0u64; 2];
        section_data_bytes(&entries, &mut disk);
        assert_eq!(disk, vec![6_100, 2_050]);
    }

    fn stats_rec(byte_len_sum: u64, nonnull: u32, computed: bool) -> StatsRecord {
        let mut r = StatsRecord::absent();
        r.byte_len_sum = byte_len_sum;
        r.nonnull = nonnull;
        if computed {
            r.flags |= STATSF_COMPUTED;
        }
        r
    }

    /// The cross-part width fold averages the SUMS (never averages of
    /// averages) and lands on payload-avg + VARHDRSZ.
    #[test]
    fn width_fold_averages_the_sums() {
        let mut a = ColWidthAcc::default();
        // part 1: 10 values, 100 bytes; part 2: 30 values, 900 bytes.
        a.merge_part_record(&stats_rec(100, 10, true));
        a.merge_part_record(&stats_rec(900, 30, true));
        // (1000 / 40 = 25) + 4 header = 29; averaging averages would say 24.
        assert_eq!(a.value(), 29);
    }

    /// Witness-less records poison the column in both orders (the
    /// stand-in-sealed part's uncomputed zero sum would deflate a merged
    /// average), and a poisoned column stays 0 forever.
    #[test]
    fn width_fold_witness_gates() {
        let mut a = ColWidthAcc::default();
        a.merge_part_record(&stats_rec(100, 10, true));
        a.merge_part_record(&stats_rec(0, 50, false));
        assert_eq!(a.value(), 0);

        let mut b = ColWidthAcc::default();
        b.merge_part_record(&stats_rec(0, 50, false));
        b.merge_part_record(&stats_rec(100, 10, true));
        assert_eq!(b.value(), 0);

        let mut c = ColWidthAcc::default();
        c.mark_absent();
        c.merge_part_record(&stats_rec(100, 10, true));
        assert_eq!(c.value(), 0);
    }

    /// Zero sums read unknown: fixed-width (LenStats::None) columns and
    /// all-empty-string columns both decline to the consumer's fallback.
    #[test]
    fn width_fold_zero_sums_are_unknown() {
        let mut a = ColWidthAcc::default();
        a.merge_part_record(&stats_rec(0, 100, true));
        assert_eq!(a.value(), 0);
        let b = ColWidthAcc::default();
        assert_eq!(b.value(), 0);
    }

    /// The part record is the LAST record of the section body; a body
    /// whose length disagrees with the part's geometry refuses typed.
    #[test]
    fn part_stats_record_slices_the_tail() {
        let granules = 3u32;
        let bands = 1u32;
        let mut body = Vec::new();
        for i in 0..(granules + bands) {
            stats_rec(i as u64, i, true).encode_into(&mut body);
        }
        stats_rec(4242, 77, true).encode_into(&mut body);
        let rec = part_stats_record(&body, granules, bands, 5).expect("slice");
        assert_eq!((rec.byte_len_sum, rec.nonnull), (4242, 77));

        let err = part_stats_record(&body[..body.len() - 1], granules, bands, 5)
            .expect_err("short body must refuse");
        assert_eq!(err.sqlstate(), types_error::ERRCODE_DATA_CORRUPTED);
        assert!(err.message().contains("geometry"), "wrong refusal: {}", err.message());
    }
}
