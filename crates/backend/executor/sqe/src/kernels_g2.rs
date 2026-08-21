//! Extractions from the harness kernels_g2.rs (port-study/port-map.md
//! §3.14): `minute_of` and the ORDER-PRESERVING u64 ordinal remap
//! (OrdRemap). `ord_remap_memo` is REHOMED as a Faces-owned per-relation
//! memo (risks.md §1: the harness global was bank-blind); like the shared
//! frame it persisted across queries in the PoC and keeps doing so.

use crate::bank::Bank;
use crate::kernels_g::ColState;
use crate::pool::Pool;
use crate::scan::granule_walk;
use std::sync::Arc;

#[inline(always)]
pub fn minute_of(us: i64) -> u64 {
    // PG extract(minute FROM timestamp): minute of the time of day. The
    // bank's EventTime values are all positive (2013 > the 2000 epoch).
    ((us % 3_600_000_000) / 60_000_000) as u64
}

// ---------------------------------------------------------------------------
// [opt-group lane 2026-08-15] ORDINAL REMAP — the int-key twin of the text
// registry / p2d global dict, authorized as an OPEN-TIME DERIVED structure:
// one pass over the column builds value -> dense ordinal, ORDER-PRESERVING
// (ord ascending == value ascending as u64), and materializes the per-row
// ordinal stream (the "persisted ordinal column" a sealed bank would carry).
// Grouping then runs on dense ordinals with direct-indexed accumulator
// arrays — no hashing in the hot loop at all. Build cost reported
// separately (FINDING|ordremap|...): amortizable across hot-shape
// (and any UserID consumer); if the build dominates, that IS the format
// answer — persist ordinals at seal time.
//
// Build shape (deterministic, pool-parallel):
//   pass 1  decode column, scatter values into 256 RANGE buckets (v >> 56 —
//           order-preserving split; UserID is hash-distributed, so the
//           split is uniform)
//   pass 2  one owner per bucket: sort + dedup -> sorted distinct values +
//           a bucket-local open-addressed value->local-index table
//   stitch  prefix offsets over buckets -> global ordinals; vals concat
//   pass 3  decode column again, write ords[row] via the bucket tables
//           (rows land at granule_walk base offsets — disjoint writes)
// ---------------------------------------------------------------------------

pub struct OrdRemap {
    /// Sorted ASC distinct values; ordinal = index (order-preserving).
    pub vals: Vec<u64>,
    /// Per-row ordinal stream in granule_walk order (unit base offsets).
    pub ords: Vec<u32>,
    pub build_ms: f64,
}

/// Bucket-local value -> local-index open-addressed table (idx+1 stored,
/// 0 = empty; presized 2x, read-only after build).
struct OrdLookup {
    keys: Vec<u64>,
    idx1: Vec<u32>,
    mask: usize,
}

impl OrdLookup {
    fn build(sorted: &[u64]) -> OrdLookup {
        let cap = (sorted.len() * 2).next_power_of_two().max(16);
        let mut t = OrdLookup {
            keys: vec![0; cap],
            idx1: vec![0; cap],
            mask: cap - 1,
        };
        for (i, &v) in sorted.iter().enumerate() {
            let mut slot = (crate::grouped::hash64(v) as usize) & t.mask;
            loop {
                if t.idx1[slot] == 0 {
                    t.keys[slot] = v;
                    t.idx1[slot] = i as u32 + 1;
                    break;
                }
                slot = (slot + 1) & t.mask;
            }
        }
        t
    }
    #[inline(always)]
    fn get(&self, v: u64) -> u32 {
        let mut slot = (crate::grouped::hash64(v) as usize) & self.mask;
        loop {
            if self.idx1[slot] != 0 && self.keys[slot] == v {
                return self.idx1[slot] - 1;
            }
            debug_assert!(self.idx1[slot] != 0, "ordremap covers every value");
            slot = (slot + 1) & self.mask;
        }
    }
}

/// Disjoint-range writer for the flat ord stream (units own [base,
/// base+rows) exclusively — the standard bench-local unsafe idiom).
struct OrdSink(*mut u32);
unsafe impl Send for OrdSink {}
unsafe impl Sync for OrdSink {}

pub fn build_ord_remap(bank: &Bank, attno: u32, pool: &Pool) -> OrdRemap {
    let t0 = std::time::Instant::now();
    let units = granule_walk(bank, attno);
    let rows_total = bank.rows_total() as usize;
    const RB: usize = 256;
    // pass 1: range scatter.
    let pass1 = pool.run(
        units.len(),
        |_| {
            (
                ColState::new(attno),
                (0..RB).map(|_| Vec::new()).collect::<Vec<Vec<u64>>>(),
            )
        },
        |(cs, buckets), i| {
            let (pi, g, rows, _) = units[i];
            for &v in cs.dec(bank, pi, g, rows as usize) {
                buckets[(v >> 56) as usize].push(v);
            }
        },
    );
    let _t1 = t0.elapsed().as_secs_f64() * 1e3;
    // pass 2: per-bucket sort + dedup + lookup table.
    let scattered: Vec<&Vec<Vec<u64>>> = pass1.iter().map(|(_, b)| b).collect();
    let owned = pool.run(
        RB,
        |_| Vec::new(),
        |out: &mut Vec<(usize, Vec<u64>, OrdLookup)>, b| {
            let mut vals: Vec<u64> = Vec::new();
            for s in &scattered {
                vals.extend_from_slice(&s[b]);
            }
            vals.sort_unstable();
            vals.dedup();
            let lk = OrdLookup::build(&vals);
            out.push((b, vals, lk));
        },
    );
    let _t2 = t0.elapsed().as_secs_f64() * 1e3;
    let mut per_bucket: Vec<(usize, Vec<u64>, OrdLookup)> = owned.into_iter().flatten().collect();
    per_bucket.sort_unstable_by_key(|e| e.0);
    let mut offsets = vec![0u32; RB + 1];
    for (b, vals, _) in &per_bucket {
        offsets[b + 1] = vals.len() as u32;
    }
    for b in 0..RB {
        offsets[b + 1] += offsets[b];
    }
    let mut vals: Vec<u64> = Vec::with_capacity(offsets[RB] as usize);
    let mut lookups: Vec<OrdLookup> = Vec::with_capacity(RB);
    for (_, v, lk) in per_bucket {
        vals.extend_from_slice(&v);
        lookups.push(lk);
    }
    // pass 3: materialize the per-row ordinal stream.
    let mut ords: Vec<u32> = vec![0; rows_total];
    let sink = OrdSink(ords.as_mut_ptr());
    let sink_ref = &sink;
    pool.run(
        units.len(),
        |_| ColState::new(attno),
        |cs, i| {
            let (pi, g, rows, base) = units[i];
            let d = cs.dec(bank, pi, g, rows as usize);
            // SAFETY: units own disjoint [base, base+rows) row ranges.
            let dst = unsafe { std::slice::from_raw_parts_mut(sink_ref.0.add(base as usize), rows as usize) };
            for (r, &v) in d.iter().enumerate() {
                let b = (v >> 56) as usize;
                dst[r] = offsets[b] + lookups[b].get(v);
            }
        },
    );
    let build_ms = t0.elapsed().as_secs_f64() * 1e3;
    OrdRemap { vals, ords, build_ms }
}


/// Faces-owned memo consult — open-time-resident; every consumer after
/// the first gets it free.
pub fn ord_remap_memo(
    faces: &crate::engine::Faces,
    bank: &Bank,
    attno: u32,
    pool: &Pool,
) -> Arc<OrdRemap> {
    faces.ord_remaps.get_or_build(attno, || {
        let t0 = std::time::Instant::now();
        let r = build_ord_remap(bank, attno, pool);
        crate::coldledger::note(
            "ord_remap",
            format!("attno={attno}|ndv={}", r.vals.len()),
            t0,
            (r.vals.len() * 8 + r.ords.len() * 4) as u64,
            crate::coldledger::Reason::CheaperThanPlain,
        );
        r
    })
}
