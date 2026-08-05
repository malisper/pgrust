//! GL-HJSP-1 loss ATTRIBUTION microbench (no fixes — measurement only).
//!
//! The fleet skew bench (scratchpad/night/hj-skew-bench.sh, take-2 job
//! ...1ead) measured single-pass losing 1.037-1.114 across the build-key
//! cardinality sweep at 8M fact / 2M build / dop8. This bench decomposes the
//! loss BY PHASE at the same geometry: build-accept vs build-finish
//! (combine or seal) vs probe, two-pass vs single-pass, on identical rows.
//!
//! Suspect isolation design:
//! - W=1 single-pass produces chain order IDENTICAL to two-pass (both are
//!   reverse scan order: two-pass combine walks runs ascending head-inserting;
//!   1-worker single-pass head-inserts in scan order). So the W=1 probe A/B
//!   isolates suspect (d) — the bucket_slice/Arc indirection — with zero
//!   locality confound. The W=8 probe A/B is (d) + chain-walk LOCALITY
//!   (CAS-interleaved chains hop across 8 worker arenas; combine-linked
//!   chains stride within one run); the difference is the locality term.
//! - Build-only times at W=8 across the NDV sweep isolate suspect (b) CAS
//!   contention (uniq spreads heads; hot1 serializes on one head).
//! - Suspect (c) grow-at-seal never fires here (estimate == true count), as
//!   in the fleet bench (ANALYZE'd dims): seal is measured and reported.
//!
//! Run: cargo test -p nodehashjoin --release --test singlepass_attr -- \
//!        --ignored --nocapture
//! Knobs: HJ_ATTR_ROWS (build rows, default 2_000_000), HJ_ATTR_PROBES
//! (default 4x rows), HJ_ATTR_THREADS (default 8), HJ_ATTR_REPS (default 3).

use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use nodehashjoin::shared_build::{
    finish_single_pass, freeze, CombinePlan, FrozenJoinTable, JoinBudget, JoinBuildLocal,
    SharedBuildDir, PARTITIONS,
};

fn knob(name: &str, default: u64) -> u64 {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

/// splitmix64 — the same key->hash shape the in-crate tests use.
fn mix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e3779b97f4a7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
    x ^ (x >> 31)
}

#[inline]
fn hash_of_key(k: u64) -> u32 {
    mix(k.wrapping_mul(0x517c_c1b7_2722_0a95)) as u32
}

#[derive(Clone, Copy, PartialEq)]
enum Case {
    UniqHi,  // dim unique: rows distinct keys (spread heads — designed win)
    Mid,     // 4096 distinct build keys
    SkewLo,  // 16 distinct build keys (hot heads)
    Hot1,    // ~90% of build rows on ONE key (max contention)
}

impl Case {
    fn name(self) -> &'static str {
        match self {
            Case::UniqHi => "uniq_hi",
            Case::Mid => "mid",
            Case::SkewLo => "skew_lo",
            Case::Hot1 => "hot1",
        }
    }

    /// Build-side key for global row g in 1..=rows (mirrors the skew bench's
    /// DIM_KEXPR at DIM_ROWS=rows).
    #[inline]
    fn build_key(self, g: u64, rows: u64) -> u64 {
        match self {
            Case::UniqHi => g - 1,
            Case::Mid => g % 4096,
            Case::SkewLo => g % 16,
            Case::Hot1 => {
                if g % 10 < 9 {
                    0
                } else {
                    g
                }
            }
        }
        .min(rows.saturating_mul(2)) // keep keys bounded (no-op for these)
    }

    /// Probe-side key for probe row j in 1..=probes (mirrors FACT_KEXPR:
    /// uniq probes 1:1 into the build range; the rest probe a large keyspace
    /// so only the few build keys hit and each hit walks the full chain).
    #[inline]
    fn probe_key(self, j: u64, rows: u64, probes: u64) -> u64 {
        match self {
            Case::UniqHi => (j - 1) % rows,
            _ => j % probes,
        }
    }
}

/// Deal 256 contiguous granule ranges round-robin to the workers (the
/// morsel-claim approximation the in-crate tests use).
fn schedule(rows: u64, workers: usize) -> Vec<Vec<Range<u64>>> {
    let granules = 256u64;
    let per = rows.div_ceil(granules);
    let mut sched = vec![Vec::new(); workers];
    for g in 0..granules {
        let s = g * per + 1;
        let e = ((g + 1) * per + 1).min(rows + 1);
        if s < e {
            sched[g as usize % workers].push(s..e);
        }
    }
    sched
}

/// 24-byte payload (~the bench's (int k, int8 pad) minimal tuple).
#[inline]
fn payload_of(g: u64, k: u64) -> [u8; 24] {
    let mut p = [0u8; 24];
    p[..8].copy_from_slice(&g.to_le_bytes());
    p[8..16].copy_from_slice(&k.to_le_bytes());
    p
}

struct BuildOut {
    table: FrozenJoinTable,
    accept_ms: f64,
    finish_ms: f64, // two-pass: plan+combine(256)x8 threads; single-pass: seal
}

fn build(case: Case, rows: u64, workers: usize, single_pass: bool) -> BuildOut {
    let budget = JoinBudget::unlimited();
    let sched = schedule(rows, workers);
    let dir = if single_pass {
        Some(SharedBuildDir::with_estimate(rows, &budget).expect("dir fits"))
    } else {
        None
    };
    let mut locals: Vec<JoinBuildLocal> = (0..workers)
        .map(|w| {
            let mut l = JoinBuildLocal::new(w, Arc::clone(&budget));
            if let Some(d) = &dir {
                l.attach_shared_dir(Arc::clone(d));
            }
            l
        })
        .collect();

    let t0 = Instant::now();
    std::thread::scope(|scope| {
        for (w, l) in locals.iter_mut().enumerate() {
            let claims = &sched[w];
            scope.spawn(move || {
                for range in claims {
                    l.begin_run(range.start);
                    for g in range.clone() {
                        let k = case.build_key(g, rows);
                        l.push(hash_of_key(k), &payload_of(g, k)).unwrap();
                    }
                    l.end_run();
                }
            });
        }
    });
    let accept_ms = t0.elapsed().as_secs_f64() * 1e3;

    let t1 = Instant::now();
    let (plan, finish_ms);
    if let Some(d) = dir {
        plan = Arc::new(finish_single_pass(&locals, d, &budget).expect("seal fits"));
        finish_ms = t1.elapsed().as_secs_f64() * 1e3;
    } else {
        let p = Arc::new(CombinePlan::plan(&locals, &budget).expect("plan fits"));
        let next = AtomicU64::new(0);
        std::thread::scope(|scope| {
            for _ in 0..workers {
                let (p, locals) = (&p, &locals);
                scope.spawn(|| loop {
                    let part = next.fetch_add(1, Ordering::Relaxed);
                    if part >= PARTITIONS as u64 {
                        break;
                    }
                    p.combine_partition(part, locals);
                });
            }
        });
        plan = p;
        finish_ms = t1.elapsed().as_secs_f64() * 1e3;
    }
    let table = freeze(plan, &locals);
    BuildOut { table, accept_ms, finish_ms }
}

/// Probe the table with the case's fact stream across `workers` threads
/// (chunked). Returns (probe_ms, candidates_visited, hash_matches) — the
/// counts double as an OFF/ON equivalence check (identical multisets must
/// yield identical counts).
fn probe(case: Case, table: &FrozenJoinTable, rows: u64, probes: u64, workers: usize) -> (f64, u64, u64) {
    let cand = AtomicU64::new(0);
    let matched = AtomicU64::new(0);
    let t0 = Instant::now();
    let chunk = probes.div_ceil(workers as u64);
    std::thread::scope(|scope| {
        for w in 0..workers as u64 {
            let (cand, matched, table) = (&cand, &matched, &table);
            scope.spawn(move || {
                let (mut c, mut m) = (0u64, 0u64);
                let start = w * chunk + 1;
                let end = ((w + 1) * chunk).min(probes);
                for j in start..=end {
                    let k = case.probe_key(j, rows, probes);
                    let h = hash_of_key(k);
                    for t in table.chain(h) {
                        c += 1;
                        if t.hashvalue() == h {
                            m += 1;
                        }
                    }
                }
                cand.fetch_add(c, Ordering::Relaxed);
                matched.fetch_add(m, Ordering::Relaxed);
            });
        }
    });
    (
        t0.elapsed().as_secs_f64() * 1e3,
        cand.load(Ordering::Relaxed),
        matched.load(Ordering::Relaxed),
    )
}

#[test]
#[ignore = "GL-HJSP-1 attribution instrument: run --release with --ignored --nocapture"]
fn attribution_sweep() {
    let rows = knob("HJ_ATTR_ROWS", 2_000_000);
    let probes = knob("HJ_ATTR_PROBES", rows * 4);
    let workers = knob("HJ_ATTR_THREADS", 8) as usize;
    let reps = knob("HJ_ATTR_REPS", 3) as usize;
    println!(
        "== GL-HJSP-1 attribution: rows={rows} probes={probes} workers={workers} best-of-{reps} =="
    );
    println!(
        "{:<8} {:>5} | {:>10} {:>10} {:>10} | {:>10} {:>10} {:>10} | {:>9} {:>9} {:>9}",
        "case", "W", "2p_accept", "2p_combine", "2p_build", "sp_accept", "sp_seal", "sp_build",
        "2p_probe", "sp_probe", "probe_r"
    );
    for &w in &[workers, 1] {
        for &case in &[Case::UniqHi, Case::Mid, Case::SkewLo, Case::Hot1] {
            let mut best: Option<(BuildOut, BuildOut, f64, f64)> = None;
            let (mut ck2, mut ck1) = ((0, 0), (0, 0));
            for _ in 0..reps {
                let two = build(case, rows, w, false);
                let one = build(case, rows, w, true);
                let (p2, c2, m2) = probe(case, &two.table, rows, probes, w);
                let (p1, c1, m1) = probe(case, &one.table, rows, probes, w);
                ck2 = (c2, m2);
                ck1 = (c1, m1);
                let better = match &best {
                    None => true,
                    Some((bt, bo, bp2, bp1)) => {
                        (two.accept_ms + two.finish_ms + one.accept_ms + one.finish_ms + p2 + p1)
                            < (bt.accept_ms + bt.finish_ms + bo.accept_ms + bo.finish_ms + bp2 + bp1)
                    }
                };
                if better {
                    best = Some((two, one, p2, p1));
                }
            }
            assert_eq!(ck2, ck1, "{}: candidate/match counts diverge two-pass vs single-pass", case.name());
            let (two, one, p2, p1) = best.unwrap();
            let b2 = two.accept_ms + two.finish_ms;
            let b1 = one.accept_ms + one.finish_ms;
            println!(
                "{:<8} {:>5} | {:>10.1} {:>10.1} {:>10.1} | {:>10.1} {:>10.1} {:>10.1} | {:>9.1} {:>9.1} {:>9.3}",
                case.name(), w, two.accept_ms, two.finish_ms, b2, one.accept_ms, one.finish_ms, b1,
                p2, p1, p1 / p2.max(0.001)
            );
        }
    }
    println!("(probe_r > 1 => single-pass table probes SLOWER; W=1 rows isolate the indirection term — chain orders are identical at W=1)");
}
