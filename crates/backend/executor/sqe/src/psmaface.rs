//! PsmaFace — the §8.2 Positional-SMA face (the seal-cost redemption
//! lane): per armed granule, a 256-entry leading-byte → candidate row
//! range table that narrows scans INSIDE granules the zone map cannot
//! skip. The probe discipline is the Data Blocks lookup: index the
//! predicate's value (or value range) through the SAME frozen index
//! function the sealer used (pgrc2_meta::psma — one law, never
//! re-implemented) and take the union of the touched buckets' ranges —
//! ONE [lo, hi) row slice per granule, a SUPERSET of the matching rows.
//! Residual evaluation inside the slice stays mandatory; rows OUTSIDE
//! the slice are excluded without evaluation.
//!
//! ## Soundness (the seal-time invariant, verified at builder grain)
//!
//! `pgrc2_meta::builder::observe_rows` stages `(row_in_granule, key)`
//! for every NON-NULL row — `base + r` counts NULL rows too, so PSMA row
//! ordinals ARE decode ordinals — and `finalize` builds the block with
//! `psma_index(kmin, shift, key)` where `rec.min_key/max_key = kmin/kmax`
//! verbatim. The probe therefore reproduces the seal-time bucket index
//! exactly when fed the granule's EXACT zone keys. Rows the slice
//! excludes are (a) non-null rows whose key indexes elsewhere — they
//! cannot satisfy an eq/range probe on the indexed buckets — or (b) NULL
//! rows, which never pass a WHERE comparison (3VL). Both exclusions are
//! sound without evaluation.
//!
//! ## Trust posture
//!
//! PSMA sections ride the sealed part's section integrity wrap exactly
//! like Stats/FlatStats — SAME trust class as the zone maps every
//! zone-skip consumer already believes. A lying PSMA that excludes
//! matching rows is a wrong-answer hazard the residual eval CANNOT
//! catch (the residual only sees the slice), so this face adds nothing
//! the zone maps didn't already assume — and oracle builds gate it
//! anyway: `oracle_check_complement` (the fp-verify pattern) proves the
//! slice complement empty against the decoded granule on every consult.
//!
//! ## Election (never a per-row cost)
//!
//! Consult ONLY when (1) the kill switch is armed (PGRUST_SQE_PSMA;
//! opt-OUT like every default-ON arm), (2) the zone map said "maybe",
//! (3) the conjunct is selective-class — Eq / In2 / Between — on a
//! PSMA-covered column, and (4) the granule's zone keys are EXACT and
//! non-sentinel (a derived-SMA sentinel is indistinguishable from a real
//! full-span zone, and probing with keys that are not the sealer's kmin/
//! kmax would desynchronize the index function — refuse, never guess).
//! One probe per (granule, conjunct); the block read is one lazy
//! section fault per (part, column), cached per the stats-face
//! conventions (sealed = immutable, cache-safe).

use crate::bank::Bank;
use crate::ir::{CmpOp, PredTerm};
use pgrc2_format::part::{SectionKind, SECTIONF_META_ZSTD};
use pgrc2_meta::psma::{psma_block_for, psma_candidates_eq, psma_candidates_range};

/// One part's Psma section body (unwrapped), or None when the part
/// carries no Psma section for the column (consult degrades, must-scan).
struct PsmaPart {
    body: Vec<u8>,
    granule_count: u32,
}

pub struct PsmaFace {
    parts: Vec<Option<PsmaPart>>,
}

impl PsmaFace {
    /// Open the whole-bank Psma plane of one column: one section fault
    /// per part (part-parallel, the F5-capped metadata-face law).
    pub fn open(bank: &Bank, attno: u32, threads: usize) -> PsmaFace {
        let parts = crate::engine::par_parts_meta(threads, bank.parts.len(), |pi| {
            let part = &bank.parts[pi];
            // [json-rung1] lane columns resolve (parent attno, path_ord).
            let (sa, po) = bank.stream_key(pi, attno);
            let idx = part.find_section(SectionKind::Psma, sa, po)?;
            let flags = part.sections()[idx].flags;
            let raw = part.section_bytes(idx).ok()?;
            let body = if flags & SECTIONF_META_ZSTD != 0 {
                pgrc2_codec::wrapper::meta_unwrap_body(raw.bytes()).ok()?
            } else {
                raw.bytes().to_vec()
            };
            Some(PsmaPart { body, granule_count: bank.manifest.parts[pi].granule_count })
        });
        PsmaFace { parts }
    }

    /// Any part armed at all? (Consumers skip the per-granule consult
    /// entirely on columns that sealed without PSMA.)
    pub fn any(&self) -> bool {
        self.parts.iter().any(|p| p.is_some())
    }

    /// The candidate row slice `[lo, hi)` (granule-relative ordinals,
    /// clamped to `rows`) for `term` over granule `g` of part `pi`,
    /// given the granule's EXACT zone keys `(zlo, zhi)` from the flat
    /// SMA face. `None` = no narrowing (unarmed granule, non-selective
    /// op, sentinel zone, malformed block — must scan the full granule);
    /// `Some((0, 0))` = the granule is proven empty of candidates.
    pub fn slice(
        &self,
        pi: usize,
        g: u32,
        rows: u32,
        zlo: i64,
        zhi: i64,
        term: &PredTerm,
    ) -> Option<(u16, u16)> {
        match term.op {
            CmpOp::Eq => self.slice_eq(pi, g, rows, zlo, zhi, term.lo),
            CmpOp::Between => self.slice_range(pi, g, rows, zlo, zhi, term.lo, term.hi),
            CmpOp::In2 => {
                let a = self.slice_eq(pi, g, rows, zlo, zhi, term.lo)?;
                let b = self.slice_eq(pi, g, rows, zlo, zhi, term.hi)?;
                Some(union(a, b))
            }
            // Ne is never selective-class: its candidates are the
            // complement of one bucket — no contiguous narrowing.
            CmpOp::Ne => None,
        }
    }

    /// Equality-probe slice (see `slice`). `key` = the predicate value.
    pub fn slice_eq(
        &self,
        pi: usize,
        g: u32,
        rows: u32,
        zlo: i64,
        zhi: i64,
        key: i64,
    ) -> Option<(u16, u16)> {
        let block = self.block(pi, g, zlo, zhi)?;
        let (lo, hi) = psma_candidates_eq(block, zlo, zhi, key)?;
        Some((lo, hi.min(rows as u16)))
    }

    /// Range-probe slice for keys in `[lo_key, hi_key]` (see `slice`).
    #[allow(clippy::too_many_arguments)]
    pub fn slice_range(
        &self,
        pi: usize,
        g: u32,
        rows: u32,
        zlo: i64,
        zhi: i64,
        lo_key: i64,
        hi_key: i64,
    ) -> Option<(u16, u16)> {
        let block = self.block(pi, g, zlo, zhi)?;
        let (lo, hi) = psma_candidates_range(block, zlo, zhi, lo_key, hi_key)?;
        Some((lo, hi.min(rows as u16)))
    }

    fn block(&self, pi: usize, g: u32, zlo: i64, zhi: i64) -> Option<&[u8]> {
        // Sentinel refusal: the derived SMA arrays encode "no exact
        // zone" as (i64::MIN, i64::MAX). Probing with keys that may not
        // be the sealer's kmin/kmax is unsound — decline.
        if zlo == i64::MIN && zhi == i64::MAX {
            return None;
        }
        let p = self.parts.get(pi)?.as_ref()?;
        // Malformed body = typed refusal downgraded to must-scan here
        // (the face never turns a metadata fault into a wrong answer).
        psma_block_for(&p.body, p.granule_count, g).ok()?
    }
}

/// Union of two candidate ranges (empty = (0,0) per the probe contract).
fn union(a: (u16, u16), b: (u16, u16)) -> (u16, u16) {
    match (a.0 >= a.1, b.0 >= b.1) {
        (true, true) => (0, 0),
        (true, false) => b,
        (false, true) => a,
        (false, false) => (a.0.min(b.0), a.1.max(b.1)),
    }
}

/// Fold `slice` into an intersected row window `[rlo, rhi)` (usize row
/// bounds; `None` slice leaves the window untouched).
#[inline(always)]
pub fn narrow(win: (usize, usize), slice: Option<(u16, u16)>) -> (usize, usize) {
    match slice {
        Some((lo, hi)) => (win.0.max(lo as usize), win.1.min(hi as usize)),
        None => win,
    }
}

/// [oracle] Slice-complement emptiness check (the fp-verify pattern): a
/// PSMA slice that excluded a matching valid row is a lying section or a
/// desynchronized index function — panic loudly, never a wrong answer.
/// Zero code in production builds.
#[cfg(feature = "oracle")]
pub fn oracle_check_complement(
    term: &PredTerm,
    rlo: usize,
    rhi: usize,
    rows: usize,
    valid: impl Fn(usize) -> bool,
    word: impl Fn(usize) -> i64,
) {
    for r in (0..rlo).chain(rhi..rows) {
        assert!(
            !(valid(r) && term.eval(word(r))),
            "sqe oracle: PSMA slice [{rlo},{rhi}) excluded matching row {r} \
             (col {}, op {:?}, lo {}, hi {}) — lying section or index-function drift",
            term.col, term.op, term.lo, term.hi
        );
    }
}
