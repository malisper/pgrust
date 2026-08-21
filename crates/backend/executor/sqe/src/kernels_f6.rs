//! Extractions from the harness kernels_f6.rs (port-study/port-map.md
//! §3.10): date/time integer arithmetic, FxHasher, and the shared-frame
//! machinery. Reshapes at port:
//!
//! - `shared_frame` is REHOMED as a Faces-owned per-relation memo
//!   (risks.md §1: the harness global was bank-blind) — the frame is "a
//!   cached verdict at rowlist grain" (PLANNER-SPEC §2.6), i.e. condition-
//!   cache-class state, so like `cond` it survives `reset_per_query`.
//! - `build_frame` takes the frame columns' attnos from the caller
//!   (lowering resolved them); the `bank.col("counterid")` name lookups
//!   are dead in the engine (risks.md §3).
//! - The globaldict machinery (GlobalDict/PartDict + memos) is NOT ported
//!   (risks.md §11 — the arm is deleted; part_merge is the vocabulary).
//! - The `esc` duplicate is dead (render::esc is the law); `fmt_timestamp`
//!   lives in render.

use crate::bank::Bank;
use crate::scan::{granule_walk, open_cursor, CurCache, Scratch};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// date/time integer arithmetic (no timestamp library — chartered)
// ---------------------------------------------------------------------------

/// Days from 1970-01-01 (Howard Hinnant's civil algorithm).
pub const fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

/// PG date datum: days since 2000-01-01.
pub const fn pg_date(y: i64, m: i64, d: i64) -> i64 {
    days_from_civil(y, m, d) - days_from_civil(2000, 1, 1)
}

/// Calendar year of a PG date datum (days since 2000-01-01): the civil-
/// from-days inverse of `days_from_civil`, shifted to the PG epoch.
/// Finite dates only — the caller witnesses the domain.
pub const fn pg_date_year(days: i64) -> i64 {
    let z = days + days_from_civil(2000, 1, 1) + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    if m <= 2 { y + 1 } else { y }
}

/// DATE_TRUNC('minute', ts) on the PG i64-microsecond datum: pure integer
/// arithmetic (the epoch 2000-01-01 00:00:00 is minute-aligned).
#[inline(always)]
pub fn trunc_minute(us: i64) -> i64 {
    us - us.rem_euclid(60_000_000)
}

/// FxHash (rustc's multiply-rotate hash), written inline — zero deps. The
/// std SipHash costs ~100ns on the 60-100B URL keys of this family.
#[derive(Default)]
pub struct FxHasher {
    h: u64,
}

impl FxHasher {
    #[inline(always)]
    fn add(&mut self, w: u64) {
        self.h = (self.h.rotate_left(5) ^ w).wrapping_mul(0x517cc1b727220a95);
    }
}

impl std::hash::Hasher for FxHasher {
    #[inline]
    fn write(&mut self, mut b: &[u8]) {
        while b.len() >= 8 {
            self.add(u64::from_le_bytes(b[..8].try_into().unwrap()));
            b = &b[8..];
        }
        if !b.is_empty() {
            let mut w = [0u8; 8];
            w[..b.len()].copy_from_slice(b);
            self.add(u64::from_le_bytes(w));
        }
    }
    #[inline]
    fn finish(&self) -> u64 {
        self.h
    }
}

// ---------------------------------------------------------------------------
// the shared frame
// ---------------------------------------------------------------------------

pub struct FrameGranule {
    pub ord: usize, // ordinal in the whole-bank granule walk
    pub pi: usize,
    pub g: u32,
    pub rows: u32,
    /// In-granule row ordinals passing CounterID = c AND lo<=date<=hi.
    pub rowlist: Vec<u16>,
}

pub struct Frame {
    pub granules: Vec<FrameGranule>,
    pub granules_total: u64,
    pub zone_skipped: u64,
    pub zone_survivors: u64,
    pub frame_rows: u64,
    pub cells_touched: u64,
    pub build_ms: f64,
    /// [oracle, ruling Q4] The FULL structural key this frame was built
    /// from: (cid, edt, counter, dlo, dhi). The memo key on `faces.frames`
    /// is only (counter, dlo, dhi) — the column attnos are an ENCODER
    /// OMISSION by construction (sound today because the hot shape fixes
    /// cid/edt); oracle/CI builds verify them on every hit. Absent in
    /// production builds.
    #[cfg(feature = "oracle")]
    pub skey: (u32, u32, i64, i64, i64),
}

/// [oracle, ruling Q4] Overhead proof: in production builds Frame is
/// bit-identical to a twin without the structural key.
#[cfg(not(feature = "oracle"))]
const _: () = {
    struct Twin {
        _granules: Vec<FrameGranule>,
        _granules_total: u64,
        _zone_skipped: u64,
        _zone_survivors: u64,
        _frame_rows: u64,
        _cells_touched: u64,
        _build_ms: f64,
    }
    assert!(std::mem::size_of::<Frame>() == std::mem::size_of::<Twin>());
};

/// Per-granule (min,max) embeds for one column from the §8.1 Stats section
/// (KeyKind::Exact only; None => must scan).
pub fn zone_minmax(bank: &Bank, attno: u32) -> Vec<Option<(i64, i64)>> {
    let mut out = Vec::new();
    for pi in 0..bank.parts.len() {
        let body = crate::scan::stats_body(bank, pi, attno);
        let cur = open_cursor(bank, pi, attno);
        for g in 0..cur.granule_count() {
            out.push(
                body.as_deref()
                    .and_then(|b| crate::scan::stats_record(b, g as usize))
                    .filter(|r| r.key_kind == pgrc2_format::meta::KeyKind::Exact.as_u8())
                    .map(|r| (r.min_key, r.max_key)),
            );
        }
    }
    out
}

/// Build the hot-shape frame: `cid`/`edt` = the CounterID/EventDate attnos
/// resolved at lowering. `psma_cid`/`psma_edt` = the columns' §8.2 faces
/// (None = kill switch / uncovered — full-granule row walks).
pub fn build_frame(
    bank: &Bank,
    cid: u32,
    edt: u32,
    counter: i64,
    dlo: i64,
    dhi: i64,
    psma_cid: Option<&crate::psmaface::PsmaFace>,
    psma_edt: Option<&crate::psmaface::PsmaFace>,
) -> Frame {
    let t0 = std::time::Instant::now();
    let walk = granule_walk(bank, cid);
    let mm_cid = zone_minmax(bank, cid);
    let mm_edt = zone_minmax(bank, edt);
    let mut s_cid = Scratch::new();
    let mut s_edt = Scratch::new();
    let mut c_cid = CurCache::new(cid);
    let mut c_edt = CurCache::new(edt);
    let mut granules = Vec::new();
    let mut zone_skipped = 0u64;
    let mut zone_survivors = 0u64;
    let mut frame_rows = 0u64;
    let mut cells = 0u64;
    for (ord, &(pi, g, rows, _)) in walk.iter().enumerate() {
        let (clo, chi) = mm_cid[ord].unwrap_or((i64::MIN, i64::MAX));
        let (elo, ehi) = mm_edt[ord].unwrap_or((i64::MIN, i64::MAX));
        if counter < clo || counter > chi || dhi < elo || dlo > ehi {
            zone_skipped += 1;
            continue;
        }
        zone_survivors += 1;
        let n = rows as usize;
        // [psma-consume] Zone said maybe: intersect the eq-probe slice
        // on CounterID with the range-probe slice on EventDate (one
        // probe per column per granule; the zone consult above already
        // proved non-sentinel zones where mm_* is Some).
        let (rlo, rhi) = {
            let w = crate::psmaface::narrow(
                (0, n),
                psma_cid.zip(mm_cid[ord]).and_then(|(pf, (zl, zh))| {
                    pf.slice_eq(pi, g, rows, zl, zh, counter)
                }),
            );
            crate::psmaface::narrow(
                w,
                psma_edt.zip(mm_edt[ord]).and_then(|(pf, (zl, zh))| {
                    pf.slice_range(pi, g, rows, zl, zh, dlo, dhi)
                }),
            )
        };
        if rlo >= rhi {
            continue;
        }
        let dc = s_cid.decode_full(c_cid.get(bank, pi), g, n).to_vec();
        let de = s_edt.decode_full(c_edt.get(bank, pi), g, n);
        cells += 2 * rows as u64;
        // [psma-consume, oracle] slice-complement emptiness gate on the
        // conjunction: a hidden matching row panics loudly.
        #[cfg(feature = "oracle")]
        for r in (0..rlo).chain(rhi..n) {
            let c = crate::stencils::sx(dc[r], 4);
            let d = crate::stencils::sx(de[r], 4);
            assert!(
                !(c == counter && d >= dlo && d <= dhi),
                "sqe oracle: frame PSMA window [{rlo},{rhi}) hid matching row {r}"
            );
        }
        let mut rl: Vec<u16> = Vec::new();
        for r in rlo..rhi {
            let c = crate::stencils::sx(dc[r], 4);
            let d = crate::stencils::sx(de[r], 4);
            if c == counter && d >= dlo && d <= dhi {
                rl.push(r as u16);
            }
        }
        if !rl.is_empty() {
            frame_rows += rl.len() as u64;
            granules.push(FrameGranule { ord, pi, g, rows, rowlist: rl });
        }
    }
    Frame {
        granules,
        granules_total: walk.len() as u64,
        zone_skipped,
        zone_survivors,
        frame_rows,
        cells_touched: cells,
        build_ms: t0.elapsed().as_secs_f64() * 1e3,
        #[cfg(feature = "oracle")]
        skey: (cid, edt, counter, dlo, dhi),
    }
}

/// Frame memo consult: one prune derivation serves all seven hot-shape
/// queries. Rehomed on the relation's Faces (`faces.frames`); Frame holds
/// plain data (no bank borrows) so Arc-sharing across queries is sound.
pub fn shared_frame(
    faces: &crate::engine::Faces,
    bank: &Bank,
    cid: u32,
    edt: u32,
    counter: i64,
    dlo: i64,
    dhi: i64,
) -> Arc<Frame> {
    let f = faces.frames.get_or_build((counter, dlo, dhi), || {
        let t0 = std::time::Instant::now();
        let (p_cid, p_edt) = (faces.psma(bank, cid), faces.psma(bank, edt));
        let f = build_frame(bank, cid, edt, counter, dlo, dhi, p_cid.as_deref(), p_edt.as_deref());
        crate::coldledger::note(
            "frame",
            format!(
                "counter={counter}|dlo={dlo}|dhi={dhi}|survivors={}|rows={}",
                f.zone_survivors, f.frame_rows
            ),
            t0,
            0,
            crate::coldledger::Reason::TouchedByQuery,
        );
        f
    });
    // [oracle, ruling Q4] structural verify on every consult (a fresh
    // build compares trivially equal): the memo key omits cid/edt.
    #[cfg(feature = "oracle")]
    {
        let current = (cid, edt, counter, dlo, dhi);
        assert!(
            f.skey == current,
            "sqe oracle: fingerprint collision or encoder omission on the shared-frame \
             memo (key ({counter}, {dlo}, {dhi})):\n  cached structure:  {:?}\n  \
             current structure: {current:?}",
            f.skey
        );
    }
    f
}

#[cfg(test)]
mod year_tests {
    use super::*;
    #[test]
    fn pg_date_year_roundtrip() {
        for y in [1, 1969, 1970, 1992, 1995, 1996, 1998, 2000, 2004, 2100, 2400, 9999] {
            for (m, d) in [(1, 1), (2, 28), (2, 29), (3, 1), (12, 31)] {
                if (m, d) == (2, 29) && !(y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)) {
                    continue;
                }
                assert_eq!(pg_date_year(pg_date(y, m, d)), y, "{y}-{m}-{d}");
            }
        }
    }
}

