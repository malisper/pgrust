//! StatsView — the first-class §8.1 Stats face (phase-1 pain point #1).
//!
//! Phase 1 parsed the Stats section ad hoc per kernel (stats_body +
//! stats_record index arithmetic in hot-shape). This face loads the section
//! ONCE per (column, part), owns the grain arithmetic (granule records,
//! then band records, then ONE part record), and exposes typed accessors —
//! exactly what a `pgrc2_read::StatsView` on OpenPart would look like.

use crate::bank::Bank;
use pgrc2_format::meta::{StatsRecord, KeyKind, STATS_RECORD_LEN};
use pgrc2_format::part::{SectionKind, SECTIONF_META_ZSTD};
use pgrc2_format::wire::Cur;

/// Parsed stats plane of ONE (part, attno, path_ord 0).
pub struct PartStats {
    body: Vec<u8>,
    granule_count: u32,
    band_count: u32,
}

impl PartStats {
    /// One part's stats plane for (attno, path_ord 0) — `None` when the
    /// part carries no Stats section for the column. (Split out of
    /// `StatsView::open` by lane hot-shape so a part-owned kernel can consult its
    /// own part's records inside the timed walk.)
    pub fn open(bank: &Bank, pi: usize, attno: u32) -> Option<PartStats> {
        // [fmt-layout] bank-grain stats plane first: one column-contiguous
        // pread serves every part; identical bytes (the plane stores the
        // unwrapped §8.1 bodies). Outer None = plane cannot serve → the
        // per-part section path below (never wrong, only slower).
        // [json-rung1] lane columns resolve (parent attno, path_ord)
        // and skip the plane (root-attno keyed, predates lanes).
        let (sa, po) = bank.stream_key(pi, attno);
        if po == 0 {
            if let Some(pl) = bank.stats_plane.as_ref() {
                if let Some(body) = pl.try_stats_body(attno, pi) {
                    let body = body?;
                    let m = &bank.manifest.parts[pi];
                    return Some(PartStats {
                        body,
                        granule_count: m.granule_count,
                        band_count: m.band_count,
                    });
                }
            }
        }
        let part = &bank.parts[pi];
        let idx = part.find_section(SectionKind::Stats, sa, po)?;
        let flags = part.sections()[idx].flags;
        let raw = part.section_bytes(idx).ok()?;
        let body = if flags & SECTIONF_META_ZSTD != 0 {
            pgrc2_codec::wrapper::meta_unwrap_body(raw.bytes()).ok()?
        } else {
            raw.bytes().to_vec()
        };
        let m = &bank.manifest.parts[pi];
        Some(PartStats {
            body,
            granule_count: m.granule_count,
            band_count: m.band_count,
        })
    }
    fn rec(&self, idx: usize) -> Option<StatsRecord> {
        let b = self
            .body
            .get(idx * STATS_RECORD_LEN..(idx + 1) * STATS_RECORD_LEN)?;
        StatsRecord::decode(&mut Cur::new(b)).ok()
    }
    pub fn granule(&self, g: u32) -> Option<StatsRecord> {
        if g < self.granule_count {
            self.rec(g as usize)
        } else {
            None
        }
    }
    pub fn band(&self, b: u32) -> Option<StatsRecord> {
        if b < self.band_count {
            self.rec((self.granule_count + b) as usize)
        } else {
            None
        }
    }
    pub fn part(&self) -> Option<StatsRecord> {
        self.rec((self.granule_count + self.band_count) as usize)
    }
}

/// The whole-bank stats plane of one column. `None` per part = the part
/// carries no Stats section for the column (consult degrades, never errs).
pub struct StatsView {
    pub parts: Vec<Option<PartStats>>,
}

impl StatsView {
    pub fn open(bank: &Bank, attno: u32, threads: usize) -> StatsView {
        // [coldstart] part-parallel section faults (F2 arm, now the only arm).
        let parts =
            crate::engine::par_parts_meta(threads, bank.parts.len(), |pi| PartStats::open(bank, pi, attno));
        StatsView { parts }
    }

    pub fn granule(&self, pi: usize, g: u32) -> Option<StatsRecord> {
        self.parts.get(pi)?.as_ref()?.granule(g)
    }

    pub fn part(&self, pi: usize) -> Option<StatsRecord> {
        self.parts.get(pi)?.as_ref()?.part()
    }

    /// Exact granule zone keys (KeyKind::Exact only — coarse keys refuse).
    pub fn zone_exact(&self, pi: usize, g: u32) -> Option<(i64, i64)> {
        self.granule(pi, g)
            .filter(|r| r.key_kind == KeyKind::Exact.as_u8())
            .map(|r| (r.min_key, r.max_key))
    }

    /// Bank-total typed SUM folded from the part-grain records, gated on
    /// the COMPUTED witness (None if any part lacks it). SIGNEDNESS LAW
    /// (lane 2B's trap, restated): `sum_i128` is a SIGNED fold — a
    /// consumer cross-checking it against decoded datums must fold the
    /// u64 datum lane through the column's signed width (`as i16/i32/i64`)
    /// first; a naive unsigned datum sum silently diverges whenever any
    /// value has the top bit set.
    pub fn sum_total(&self) -> Option<i128> {
        let mut acc = 0i128;
        for pi in 0..self.parts.len() {
            let r = self.part(pi)?;
            if r.flags & pgrc2_format::meta::STATSF_COMPUTED == 0 {
                return None;
            }
            acc += r.sum_i128;
        }
        Some(acc)
    }

    /// [textslice] Char-exact byte-prefix witness for one varlena lane:
    /// every part record is COMPUTED and either carries ALL_ASCII, or
    /// (`pad` = a bpchar declared char width) every stored image is
    /// exactly `pad` bytes — `pad` chars in `pad` bytes leaves no room
    /// for a multibyte char. Empty banks hold vacuously; any absent
    /// record refuses (fail-closed).
    pub fn single_byte_chars(&self, pad: Option<u32>) -> bool {
        use pgrc2_format::meta::{STATSF_ALL_ASCII, STATSF_COMPUTED};
        for pi in 0..self.parts.len() {
            let Some(r) = self.part(pi) else { return false };
            if r.flags & STATSF_COMPUTED == 0 {
                return false;
            }
            if r.flags & STATSF_ALL_ASCII != 0 {
                continue;
            }
            match pad {
                Some(n) if n > 0 && r.byte_len_min == n && r.byte_len_max == n => {}
                _ => return false,
            }
        }
        true
    }

    /// Σ part-grain ndv_est across parts — the presize hint for group
    /// tables (an UPPER bound: cross-part duplicates counted per part).
    pub fn ndv_est_sum(&self) -> u64 {
        (0..self.parts.len())
            .filter_map(|pi| self.part(pi))
            .map(|r| r.ndv_est as u64)
            .sum()
    }

    /// Bank-wide exact (min,max) over part records (None if any part
    /// refuses exact keys).
    pub fn minmax_exact(&self) -> Option<(i64, i64)> {
        let mut acc: Option<(i64, i64)> = None;
        for pi in 0..self.parts.len() {
            let r = self.part(pi)?;
            if r.key_kind != KeyKind::Exact.as_u8() {
                return None;
            }
            acc = Some(match acc {
                None => (r.min_key, r.max_key),
                Some((lo, hi)) => (lo.min(r.min_key), hi.max(r.max_key)),
            });
        }
        acc
    }
}
