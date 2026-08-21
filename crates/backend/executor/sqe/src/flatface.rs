//! FlatFace — the pgrc2.1 §2.1/§2.2 reader face (reduced port: the
//! sma_build dependency the engine actually consumes — port-study
//! deviation note: port-map.md §3 missed the `sma_build → FlatFace` edge;
//! CodeFace/ndv_sum/census stay in the reference tree with their only
//! callers). Uses the NATIVE pgrc2_format pgrc2.1 APIs (FlatStatsRef,
//! SectionKind::FlatStats) — the harness's pin_compat shim is dead.
//!
//! ONE API, two sources:
//!  - **Mapped**: the part carries a `SectionKind::FlatStats` section —
//!    the face faults the section once and every consult is an array cast.
//!  - **Derived**: pre-2.1 parts — the face parses the §8.1 record stream
//!    once and materializes the SAME arrays.

use crate::bank::Bank;
use pgrc2_format::meta::{
    FlatStatsRef, KeyKind, StatsRecord, FLATSTATSF_COMPUTED, STATSF_COMPUTED, STATS_RECORD_LEN,
};
use pgrc2_format::part::{SectionKind, SECTIONF_META_ZSTD};
use pgrc2_format::wire::Cur;
use pgrc2_read::openpart::SegBuf;

/// One part's flat stats plane.
enum FlatPart {
    /// No Stats/FlatStats for the column: consult degrades (must-scan).
    Absent,
    /// pgrc2.1 FlatStats section, resident + castable. Header facts cached.
    Mapped {
        seg: SegBuf,
        n: usize,
        flags: u16,
        key_kind: u8,
    },
    /// Derived from the §8.1 record stream (pre-2.1 parts).
    Owned {
        mins: Vec<i64>,
        maxs: Vec<i64>,
        zeros: Vec<u64>,
        nonnulls: Vec<u32>,
        sums: Vec<i128>,
        flags: u16,
        key_kind: u8,
    },
}

pub struct FlatFace {
    parts: Vec<FlatPart>,
    /// Parts served by the seal-time FlatStats section.
    pub mapped_parts: usize,
    /// Parts derived from §8.1 (the fallback; the build cost lives here).
    pub derived_parts: usize,
    /// Face build wall (fault or parse, all parts).
    pub build_ms: f64,
}

fn stats_body(bank: &Bank, pi: usize, attno: u32) -> Option<Vec<u8>> {
    // [json-rung1] lane columns resolve (parent attno, path_ord) and
    // skip the plane (root-attno keyed, predates lanes).
    let (sa, po) = bank.stream_key(pi, attno);
    // [fmt-layout] bank-grain stats plane first (see statsview.rs).
    if po == 0 {
        if let Some(pl) = bank.stats_plane.as_ref() {
            if let Some(body) = pl.try_stats_body(attno, pi) {
                return body;
            }
        }
    }
    let part = &bank.parts[pi];
    let idx = part.find_section(SectionKind::Stats, sa, po)?;
    let flags = part.sections()[idx].flags;
    let raw = part.section_bytes(idx).ok()?;
    if flags & SECTIONF_META_ZSTD != 0 {
        pgrc2_codec::wrapper::meta_unwrap_body(raw.bytes()).ok()
    } else {
        Some(raw.bytes().to_vec())
    }
}

impl FlatFace {
    /// Open the whole-bank flat stats plane of one column: FlatStats when
    /// present (mmap-cast law), §8.1 parse when absent — same API.
    pub fn open(bank: &Bank, attno: u32, threads: usize) -> FlatFace {
        let t0 = std::time::Instant::now();
        use std::sync::atomic::{AtomicUsize, Ordering};
        let mapped_ctr = AtomicUsize::new(0);
        let derived_ctr = AtomicUsize::new(0);
        // [coldstart] part-parallel section faults — one cold pread per
        // part serially was the sma face's first-touch tax. F5: metadata
        // faces run at full width (par_parts_meta).
        let parts: Vec<FlatPart> = crate::engine::par_parts_meta(threads, bank.parts.len(), |pi| {
            let part = &bank.parts[pi];
            // [fmt-layout] with the bank-grain plane armed, skip the
            // per-part FlatStats section fault and derive from the
            // plane-served §8.1 body (same values by the MEET law).
            let (sa, po) = bank.stream_key(pi, attno);
            if bank.stats_plane.is_none() || po != 0 {
                if let Some(idx) = part.find_section(SectionKind::FlatStats, sa, po) {
                    if let Ok(seg) = part.section_bytes(idx) {
                        if let Ok(f) = FlatStatsRef::new(seg.bytes()) {
                            let (n, flags, key_kind) = (f.n, f.flags, f.key_kind);
                            mapped_ctr.fetch_add(1, Ordering::Relaxed);
                            return FlatPart::Mapped { seg, n, flags, key_kind };
                        }
                    }
                }
            }
            // Fallback: derive the SAME arrays from §8.1.
            let Some(body) = stats_body(bank, pi, attno) else {
                return FlatPart::Absent;
            };
            let m = &bank.manifest.parts[pi];
            let gc = m.granule_count as usize;
            let bc = m.band_count as usize;
            let n = gc + 1;
            let mut mins = Vec::with_capacity(n);
            let mut maxs = Vec::with_capacity(n);
            let mut zeros = Vec::with_capacity(n);
            let mut nonnulls = Vec::with_capacity(n);
            let mut sums = Vec::with_capacity(n);
            let mut flags = u16::MAX;
            let mut key_kind: Option<u8> = None;
            let mut push = |r: &StatsRecord, flags: &mut u16, key_kind: &mut Option<u8>| {
                mins.push(r.min_key);
                maxs.push(r.max_key);
                zeros.push(r.zero_count);
                nonnulls.push(r.nonnull);
                sums.push(r.sum_i128);
                *flags &= r.flags; // MEET, exactly flatstats_encode's law
                *key_kind = Some(match *key_kind {
                    None => r.key_kind,
                    Some(k) if k == r.key_kind => k,
                    Some(_) => KeyKind::Absent.as_u8(),
                });
            };
            let rec_at = |idx: usize| -> Option<StatsRecord> {
                let b = body.get(idx * STATS_RECORD_LEN..(idx + 1) * STATS_RECORD_LEN)?;
                StatsRecord::decode(&mut Cur::new(b)).ok()
            };
            for g in 0..gc {
                match rec_at(g) {
                    Some(r) => push(&r, &mut flags, &mut key_kind),
                    None => return FlatPart::Absent,
                }
            }
            // Part rollup record sits after granule + band records.
            match rec_at(gc + bc) {
                Some(r) => push(&r, &mut flags, &mut key_kind),
                None => return FlatPart::Absent,
            }
            derived_ctr.fetch_add(1, Ordering::Relaxed);
            FlatPart::Owned {
                mins,
                maxs,
                zeros,
                nonnulls,
                sums,
                flags: if flags == u16::MAX { 0 } else { flags },
                key_kind: key_kind.unwrap_or(KeyKind::Absent.as_u8()),
            }
        });
        FlatFace {
            parts,
            mapped_parts: mapped_ctr.load(Ordering::Relaxed),
            derived_parts: derived_ctr.load(Ordering::Relaxed),
            build_ms: t0.elapsed().as_secs_f64() * 1e3,
        }
    }

    /// Entries 0..n-2 = granule ordinals; entry n-1 = the part rollup.
    pub fn mins(&self, pi: usize) -> Option<&[i64]> {
        match self.parts.get(pi)? {
            FlatPart::Mapped { seg, .. } => FlatStatsRef::new(seg.bytes()).ok()?.mins(),
            FlatPart::Owned { mins, .. } => Some(mins),
            FlatPart::Absent => None,
        }
    }
    pub fn maxs(&self, pi: usize) -> Option<&[i64]> {
        match self.parts.get(pi)? {
            FlatPart::Mapped { seg, .. } => FlatStatsRef::new(seg.bytes()).ok()?.maxs(),
            FlatPart::Owned { maxs, .. } => Some(maxs),
            FlatPart::Absent => None,
        }
    }
    pub fn zero_counts(&self, pi: usize) -> Option<&[u64]> {
        match self.parts.get(pi)? {
            FlatPart::Mapped { seg, .. } => FlatStatsRef::new(seg.bytes()).ok()?.zero_counts(),
            FlatPart::Owned { zeros, .. } => Some(zeros),
            FlatPart::Absent => None,
        }
    }
    pub fn nonnulls(&self, pi: usize) -> Option<&[u32]> {
        match self.parts.get(pi)? {
            FlatPart::Mapped { seg, .. } => FlatStatsRef::new(seg.bytes()).ok()?.nonnulls(),
            FlatPart::Owned { nonnulls, .. } => Some(nonnulls),
            FlatPart::Absent => None,
        }
    }
    /// Part-grain sum (computed-witness gated), from the rollup entry.
    pub fn part_sum(&self, pi: usize) -> Option<i128> {
        match self.parts.get(pi)? {
            FlatPart::Mapped { seg, n, flags, .. } => {
                if flags & FLATSTATSF_COMPUTED == 0 {
                    return None;
                }
                let f = FlatStatsRef::new(seg.bytes()).ok()?;
                Some(f.sum_at(*n - 1))
            }
            FlatPart::Owned { sums, flags, .. } => {
                if flags & STATSF_COMPUTED == 0 {
                    return None;
                }
                sums.last().copied()
            }
            FlatPart::Absent => None,
        }
    }

    /// Whole-part (nonnull, zero_count) from the rollup entry — the hot-shape
    /// COUNT(nonzero) consult. `None` when uncomputed/absent.
    pub fn part_nonnull_zero(&self, pi: usize) -> Option<(u32, u64)> {
        let computed = match self.parts.get(pi)? {
            FlatPart::Mapped { flags, .. } => flags & FLATSTATSF_COMPUTED != 0,
            FlatPart::Owned { flags, .. } => flags & STATSF_COMPUTED != 0,
            FlatPart::Absent => false,
        };
        if !computed {
            return None;
        }
        let nn = *self.nonnulls(pi)?.last()?;
        let zc = *self.zero_counts(pi)?.last()?;
        Some((nn, zc))
    }

    /// Exact granule zone keys (KeyKind::Exact only), flat-array form:
    /// `(mins, maxs)` sliced to granule ordinals (rollup excluded).
    pub fn zones_exact(&self, pi: usize) -> Option<(&[i64], &[i64])> {
        let kk = match self.parts.get(pi)? {
            FlatPart::Mapped { key_kind, .. } => *key_kind,
            FlatPart::Owned { key_kind, .. } => *key_kind,
            FlatPart::Absent => return None,
        };
        if kk != KeyKind::Exact.as_u8() {
            return None;
        }
        let mins = self.mins(pi)?;
        let maxs = self.maxs(pi)?;
        Some((&mins[..mins.len() - 1], &maxs[..maxs.len() - 1]))
    }
}
