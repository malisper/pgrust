//! inc-1 property suites (docs/design/vacuum-morsels.md §10):
//!   1. skip-map equivalence — [`VacuumBlockSource::build`] (run-based)
//!      vs an independent transcription of C's cursor state machine
//!      (`heap_vac_scan_next_block` + `find_next_unskippable_block`),
//!      over crafted adversarial VM patterns and seeded-random sweeps;
//!   2. fold determinism — permuted fold orders are byte-identical,
//!      including wraparound-adjacent XID minimums;
//!   3. quiesce protocol — the claim-boundary prefix invariant, resume
//!      correctness, the K-claim overshoot bound, and the §5.2 coverage
//!      guard (incl. fault injection);
//!   4. run-merge parity — merged per-worker runs equal the serial
//!      block-order reference under adversarial claim schedules.
//! Deterministic seeded RNG (xorshift64*), no new dependencies.

use super::*;

struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed.max(1))
    }
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545F4914F6CDD1D)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
    fn chance(&mut self, pct: u64) -> bool {
        self.below(100) < pct
    }
}

// ---------------------------------------------------------------------------
// 1. Skip-map equivalence
// ---------------------------------------------------------------------------

/// Independent reference: a direct transcription of C's CURSOR state
/// machine (REL_18_3 vacuumlazy.c heap_vac_scan_next_block :1571 +
/// find_next_unskippable_block :1676 + the threshold arm :1624),
/// structurally unlike the run-based builder.
fn reference_scan(p: &SkipMapParams, vmap: &[u8]) -> (Vec<ScanBlock>, bool) {
    let vm = |b: u32| vmap.get(b as usize).copied().unwrap_or(0);
    let mut out = Vec::new();
    let mut skippedallvis = false;
    let mut next_block = p.resume_block;
    while next_block < p.rel_pages {
        // find_next_unskippable_block from next_block
        let mut b = next_block;
        let mut skipsallvis = false;
        let (next_unskippable, unskippable_allvis) = loop {
            let status = vm(b);
            let av = status & VM_ALL_VISIBLE != 0;
            let af = status & VM_ALL_FROZEN != 0;
            if b == p.rel_pages - 1 {
                break (b, av); // last block: never skippable (:1728)
            }
            if !p.skipwithvm {
                break (b, av); // DISABLE_PAGE_SKIPPING (:1732)
            }
            if !av {
                break (b, av);
            }
            if !af {
                if p.aggressive {
                    break (b, av); // av-not-af unskippable when aggressive (:1746)
                }
                skipsallvis = true; // (:1764)
            }
            b += 1;
        };
        if next_unskippable - next_block >= SKIP_PAGES_THRESHOLD {
            if skipsallvis {
                skippedallvis = true; // committed only when actually skipped (:1628)
            }
            next_block = next_unskippable;
        }
        // Case 2 (:1633): short-run blocks are scanned, all-visible per VM.
        for blk in next_block..next_unskippable {
            out.push(ScanBlock { block: blk, all_visible_according_to_vm: true });
        }
        // Case 3 (:1645): the unskippable block itself.
        out.push(ScanBlock {
            block: next_unskippable,
            all_visible_according_to_vm: unskippable_allvis,
        });
        next_block = next_unskippable + 1;
    }
    (out, skippedallvis)
}

fn check_equivalence(p: &SkipMapParams, vmap: &[u8]) {
    let built = VacuumBlockSource::build(p, |b| vmap.get(b as usize).copied().unwrap_or(0));
    let (ref_entries, ref_sav) = reference_scan(p, vmap);
    assert_eq!(built.entries(), ref_entries.as_slice(), "scan set diverged: {p:?} vm={vmap:?}");
    assert_eq!(built.skipsallvis(), ref_sav, "skipsallvis diverged: {p:?} vm={vmap:?}");
}

fn params(rel_pages: u32, resume: u32, aggressive: bool, skipwithvm: bool) -> SkipMapParams {
    SkipMapParams { rel_pages, resume_block: resume, aggressive, skipwithvm }
}

const AV: u8 = VM_ALL_VISIBLE;
const AF: u8 = VM_ALL_VISIBLE | VM_ALL_FROZEN;

#[test]
fn skipmap_crafted_adversarial() {
    for &aggr in &[false, true] {
        for &swvm in &[true, false] {
            // Degenerate relations.
            check_equivalence(&params(0, 0, aggr, swvm), &[]);
            check_equivalence(&params(1, 0, aggr, swvm), &[AF]);
            check_equivalence(&params(1, 0, aggr, swvm), &[0]);
            check_equivalence(&params(2, 0, aggr, swvm), &[AF, AF]);
            // Threshold boundaries: 31 / 32 / 33 frozen blocks then plain.
            for run in [31u32, 32, 33] {
                let mut vm = vec![AF; run as usize];
                vm.push(0);
                vm.push(0);
                check_equivalence(&params(run + 2, 0, aggr, swvm), &vm);
                // Same run but av-not-af (skipsallvis commits only at >=32
                // and only when actually skipped and not aggressive).
                let mut vm = vec![AV; run as usize];
                vm.push(0);
                vm.push(0);
                check_equivalence(&params(run + 2, 0, aggr, swvm), &vm);
            }
            // Run truncated by the never-skippable LAST page.
            check_equivalence(&params(40, 0, aggr, swvm), &vec![AF; 40]);
            check_equivalence(&params(40, 0, aggr, swvm), &vec![AV; 40]);
            // Mixed av/af inside one long run (skipsallvis from one page).
            let mut vm = vec![AF; 64];
            vm[20] = AV;
            vm.push(0);
            check_equivalence(&params(65, 0, aggr, swvm), &vm);
            // Resume offsets: start of run, mid-run, at an unskippable, end.
            let mut vm = vec![0, 0];
            vm.extend(vec![AF; 40]);
            vm.extend(vec![0, 0]);
            let rp = vm.len() as u32;
            for resume in [0, 1, 2, 10, 41, 42, rp - 1, rp] {
                check_equivalence(&params(rp, resume, aggr, swvm), &vm);
            }
            // Frozen-bit-only corruption shape (not ALL_VISIBLE): unskippable.
            check_equivalence(&params(40, 0, aggr, swvm), &vec![VM_ALL_FROZEN; 40]);
        }
    }
}

#[test]
fn skipmap_seeded_random_sweep() {
    let mut rng = Rng::new(0x5eed_5ca9);
    for _ in 0..4000 {
        let rel_pages = rng.below(180) as u32;
        let vmap: Vec<u8> = (0..rel_pages)
            .map(|_| match rng.below(10) {
                0..=3 => AF,             // frozen-heavy: exercises long skips
                4..=6 => AV,             // av-not-af: skipsallvis + aggressive arm
                7 => VM_ALL_FROZEN,      // corruption shape
                _ => 0,
            })
            .collect();
        let resume = if rel_pages == 0 { 0 } else { rng.below(rel_pages as u64 + 1) as u32 };
        let p = params(rel_pages, resume, rng.chance(30), rng.chance(85));
        check_equivalence(&p, &vmap);
    }
}

#[test]
fn skipmap_source_contract() {
    use runtime::MorselSource;
    let vm = [0, AF, AF, 0, 0];
    let src = VacuumBlockSource::build(&params(5, 0, false, true), |b| vm[b as usize]);
    // Short frozen run (2 < 32): everything is a granule.
    assert_eq!(src.total_granules(), 5);
    assert_eq!(src.startup_c0(), 16);
    // Default boundaries: one claim may span the whole space.
    assert_eq!(src.next_boundary_after(0), 5);
    assert_eq!(src.block_of(1), ScanBlock { block: 1, all_visible_according_to_vm: true });
    assert!(!src.skipsallvis());
}

/// Independent reference for the inc-2 partial-commit surface: the same
/// cursor transcription as [`reference_scan`], additionally recording each
/// COMMITTED skip (>= threshold) as (position in emitted-granule space,
/// had-av-not-af).
fn reference_skip_positions(p: &SkipMapParams, vmap: &[u8]) -> Vec<(u64, bool)> {
    let vm = |b: u32| vmap.get(b as usize).copied().unwrap_or(0);
    let mut positions = Vec::new();
    let mut emitted = 0u64;
    let mut next_block = p.resume_block;
    while next_block < p.rel_pages {
        let mut b = next_block;
        let mut skipsallvis = false;
        let next_unskippable = loop {
            let status = vm(b);
            let av = status & VM_ALL_VISIBLE != 0;
            let af = status & VM_ALL_FROZEN != 0;
            if b == p.rel_pages - 1 || !p.skipwithvm || !av {
                break b;
            }
            if !af {
                if p.aggressive {
                    break b;
                }
                skipsallvis = true;
            }
            b += 1;
        };
        if next_unskippable - next_block >= SKIP_PAGES_THRESHOLD {
            positions.push((emitted, skipsallvis));
            next_block = next_unskippable;
        }
        emitted += (next_unskippable - next_block) as u64 + 1; // short run + the unskippable block
        next_block = next_unskippable + 1;
    }
    positions
}

#[test]
fn skipsallvis_partial_commit_matches_reference() {
    // Crafted: av run / plain gap / av run / plain tail — resume points
    // below, between, and above the two committed runs.
    let mut vm = vec![0u8; 2];
    vm.extend(vec![AV; 40]); // run 1 (av-not-af), position 2
    vm.extend(vec![0u8; 3]);
    vm.extend(vec![AF; 40]); // run 2 (frozen — never commits skipsallvis)
    vm.extend(vec![AV; 40]); // run 3 (av-not-af), same maximal run as run 2
    vm.extend(vec![0u8; 3]);
    let p = params(vm.len() as u32, 0, false, true);
    let src = VacuumBlockSource::build(&p, |b| vm[b as usize]);
    let positions = reference_skip_positions(&p, &vm);
    let total = src.entries().len() as u64;
    for r in 0..=total {
        let expect = positions.iter().any(|&(pos, av)| av && r >= pos);
        assert_eq!(
            src.skipsallvis_before(r),
            expect,
            "partial commit diverged at resume {r} (positions {positions:?})"
        );
    }
    assert_eq!(src.skipsallvis_before(total), src.skipsallvis());
    // Monotone in the resume bound (more consumed decisions never un-commit).
    let mut prev = false;
    for r in 0..=total {
        let v = src.skipsallvis_before(r);
        assert!(v >= prev);
        prev = v;
    }
}

#[test]
fn skipsallvis_partial_commit_seeded_sweep() {
    let mut rng = Rng::new(0x5eed_beef);
    for _ in 0..1500 {
        let rel_pages = rng.below(180) as u32;
        let vmap: Vec<u8> = (0..rel_pages)
            .map(|_| match rng.below(10) {
                0..=3 => AF,
                4..=6 => AV,
                7 => VM_ALL_FROZEN,
                _ => 0,
            })
            .collect();
        let resume = if rel_pages == 0 { 0 } else { rng.below(rel_pages as u64 + 1) as u32 };
        let p = params(rel_pages, resume, rng.chance(30), rng.chance(85));
        let src = VacuumBlockSource::build(&p, |b| vmap.get(b as usize).copied().unwrap_or(0));
        let positions = reference_skip_positions(&p, &vmap);
        let total = src.entries().len() as u64;
        for r in [0, total / 2, total.saturating_sub(1), total] {
            let expect = positions.iter().any(|&(pos, av)| av && r >= pos);
            assert_eq!(src.skipsallvis_before(r), expect, "{p:?} vm={vmap:?} r={r}");
        }
        assert_eq!(src.skipsallvis_before(total), src.skipsallvis());
    }
}

// ---------------------------------------------------------------------------
// 2. Fold determinism
// ---------------------------------------------------------------------------

fn random_counters(rng: &mut Rng, xid_base: TransactionId) -> ScanCounters {
    let mut c = ScanCounters::seed(
        xid_base.wrapping_add(rng.below(1 << 20) as u32),
        xid_base.wrapping_add(rng.below(1 << 20) as u32),
    );
    c.scanned_pages = rng.below(1000);
    c.tuples_deleted = rng.below(100_000);
    c.lpdead_items = rng.below(100_000);
    c.live_tuples = rng.below(1_000_000);
    c.recently_dead_tuples = rng.below(1000);
    c.nonempty_pages = rng.below(1 << 20) as u32;
    c
}

#[test]
fn fold_accepts_fresh_cluster_mxid() {
    // Fresh cluster: OldestMxact == FirstMultiXactId == 1 — a valid
    // MultiXactId that is NOT "normal" by XID rules. The inc-2 fleet
    // battery caught the original fold assert panicking on the very first
    // engaged claim fold (job pgrust-combined-gates-1784013375-80750);
    // this pins the fixed contract.
    let mut acc = ScanCounters::seed(1000, 1);
    let mut claim = ScanCounters::seed(1000, 1);
    claim.scanned_pages = 3;
    claim.NewRelfrozenXid = 900; // ratcheted back by a page with older xids
    acc.fold(&claim);
    assert_eq!(acc.NewRelfrozenXid, 900);
    assert_eq!(acc.NewRelminMxid, 1);
    // And the mxid min-fold is modular (MultiXactIdPrecedes), no
    // bootstrap/frozen special cases.
    let mut older = ScanCounters::seed(1000, 5);
    let newer = ScanCounters::seed(1000, 2);
    older.fold(&newer);
    assert_eq!(older.NewRelminMxid, 2);
}

#[test]
fn fold_is_order_insensitive_exact() {
    let mut rng = Rng::new(0xf01d);
    // Include a wraparound-adjacent base: XID minimums must fold under
    // TransactionIdPrecedes' modular order, not integer order.
    for &base in &[100u32, 0xFFFF_FF00u32, 0x7FFF_FFF0u32] {
        for _ in 0..200 {
            let n = 1 + rng.below(8) as usize;
            let parts: Vec<ScanCounters> = (0..n).map(|_| random_counters(&mut rng, base)).collect();
            let seed = ScanCounters::seed(base.wrapping_add(1 << 21), base.wrapping_add(1 << 21));

            let fold_in = |order: &[usize]| {
                let mut acc = seed;
                for &i in order {
                    acc.fold(&parts[i]);
                }
                acc
            };
            let forward: Vec<usize> = (0..n).collect();
            let reference = fold_in(&forward);
            // Permutations: reversed + a few seeded shuffles.
            let mut order: Vec<usize> = (0..n).rev().collect();
            assert_eq!(fold_in(&order), reference);
            for _ in 0..4 {
                for i in (1..n).rev() {
                    order.swap(i, rng.below(i as u64 + 1) as usize);
                }
                assert_eq!(fold_in(&order), reference, "fold diverged under permutation");
            }
        }
    }
}

// ---------------------------------------------------------------------------
// 3. Quiesce protocol + coverage guard
// ---------------------------------------------------------------------------

const MAX_CLAIM: u64 = 8;

struct SimResult {
    locals: Vec<VacScanLocal>,
    total: u64,
    workers: usize,
    tripped: bool,
    post_trip_scanned: u64,
}

/// Simulate K workers claiming off one cursor under a random interleaving,
/// with per-block byte accrual and claim-boundary trip observation — the
/// §3.3 protocol exactly as the scan drive will implement it.
fn simulate_round(seed: u64) -> SimResult {
    let mut rng = Rng::new(seed);
    let total = 1 + rng.below(400);
    let workers = 1 + rng.below(8) as usize;
    let max_bytes = 1 + rng.below(4000);
    let q = QuiesceState::new(max_bytes);

    let mut locals: Vec<VacScanLocal> =
        (0..workers).map(|w| VacScanLocal::new(w, 1000, 1000)).collect();
    // (claim, next_block_index) per worker; None = between claims.
    let mut in_flight: Vec<Option<(u64, u64, u64)>> = vec![None; workers];
    let mut cursor = 0u64;
    let mut post_trip_scanned = 0u64;

    loop {
        let idle = in_flight.iter().all(|f| f.is_none());
        if cursor >= total && idle {
            break;
        }
        let w = rng.below(workers as u64) as usize;
        match in_flight[w] {
            None => {
                if cursor >= total {
                    continue; // exhausted; other workers still draining
                }
                let want = 1 + rng.below(MAX_CLAIM);
                let end = (cursor + want).min(total);
                let start = cursor;
                cursor = end;
                if q.tripped() {
                    // Claim-boundary trip: post-trip claims are no-ops.
                    locals[w].claims.push(ClaimRecord { start, end, scanned: false });
                } else {
                    in_flight[w] = Some((start, end, start));
                }
            }
            Some((start, end, pos)) => {
                // Scan one block; a claim started before the trip runs to
                // completion (the amended §3.3 rule).
                if q.tripped() {
                    post_trip_scanned += 1;
                }
                q.add_bytes(rng.below(64));
                locals[w].counters.scanned_pages += 1;
                let pos = pos + 1;
                if pos == end {
                    locals[w].claims.push(ClaimRecord { start, end, scanned: true });
                    in_flight[w] = None;
                } else {
                    in_flight[w] = Some((start, end, pos));
                }
            }
        }
    }
    SimResult { locals, total, workers, tripped: q.tripped(), post_trip_scanned }
}

#[test]
fn quiesce_prefix_invariant_and_overshoot() {
    let mut tripped_cases = 0;
    for seed in 1..=600u64 {
        let r = simulate_round(seed);
        let resume = resume_granule(&r.locals, r.total);
        // The load-bearing property (§3.3): scanned set == exact prefix.
        verify_prefix_coverage(&r.locals, resume)
            .unwrap_or_else(|e| panic!("seed {seed}: coverage violated: {e:?}"));
        // Counter/coverage consistency: fold of scanned_pages == resume.
        let scanned: u64 = r.locals.iter().map(|l| l.counters.scanned_pages).sum();
        assert_eq!(scanned, resume, "seed {seed}: scanned-block fold != prefix size");
        if r.tripped {
            tripped_cases += 1;
            assert!(resume <= r.total);
            // Overshoot bound: blocks scanned after the trip <= K in-flight
            // claims' worth (the amended bound).
            assert!(
                r.post_trip_scanned <= r.workers as u64 * MAX_CLAIM,
                "seed {seed}: overshoot {} > K*claim {}",
                r.post_trip_scanned,
                r.workers as u64 * MAX_CLAIM
            );
        } else {
            assert_eq!(resume, r.total, "seed {seed}: untripped round must scan everything");
        }
    }
    assert!(tripped_cases > 100, "sweep must exercise the trip arm (got {tripped_cases})");
}

#[test]
fn coverage_guard_fault_injection() {
    let mk = |claims: &[(u64, u64, bool)]| {
        let mut l = VacScanLocal::new(0, 1000, 1000);
        l.claims = claims.iter().map(|&(start, end, scanned)| ClaimRecord { start, end, scanned }).collect();
        l
    };
    // Clean tiling.
    let a = mk(&[(0, 5, true), (10, 12, false)]);
    let b = mk(&[(5, 10, true)]);
    let locals = vec![a, b];
    let resume = resume_granule(&locals, 12);
    assert_eq!(resume, 10);
    assert_eq!(verify_prefix_coverage(&locals, resume), Ok(()));
    // Lost Local => hole => advancement suppressed, never wrong (§5.2).
    let lost = vec![mk(&[(0, 5, true), (10, 12, false)])];
    assert_eq!(verify_prefix_coverage(&lost, 10), Err(CoverageError::HoleAt(5)));
    // Overlap (double-claim) is caught.
    let overlap = vec![mk(&[(0, 5, true), (3, 8, true)])];
    assert_eq!(verify_prefix_coverage(&overlap, 8), Err(CoverageError::OverlapAt(3)));
    // Scanned work above the resume point is a protocol breach.
    let beyond = vec![mk(&[(0, 5, true), (7, 9, true)])];
    assert!(matches!(
        verify_prefix_coverage(&beyond, 5),
        Err(CoverageError::ScannedBeyondResume { start: 7, resume: 5 })
            | Err(CoverageError::HoleAt(5))
    ));
}

// ---------------------------------------------------------------------------
// 4. Run-merge parity
// ---------------------------------------------------------------------------

#[test]
fn run_merge_matches_serial_reference() {
    let mut rng = Rng::new(0xdead_71d5);
    for _ in 0..400 {
        let workers = 1 + rng.below(8) as usize;
        let total_blocks = rng.below(300) as u32;
        let mut locals: Vec<VacScanLocal> =
            (0..workers).map(|w| VacScanLocal::new(w, 1000, 1000)).collect();
        let mut reference: Vec<DeadRun> = Vec::new();
        // Carve claims sequentially (the cursor discipline), assign each to
        // a random worker, give each block a random (possibly empty) dead set.
        let mut b = 0u32;
        while b < total_blocks {
            let end = (b + 1 + rng.below(MAX_CLAIM) as u32).min(total_blocks);
            let w = rng.below(workers as u64) as usize;
            for blk in b..end {
                if rng.chance(60) {
                    let n = 1 + rng.below(12) as usize;
                    let mut offs: Vec<u16> =
                        (0..n).map(|i| (i as u16 + 1) * (1 + rng.below(20) as u16)).collect();
                    offs.sort_unstable();
                    offs.dedup();
                    locals[w].record_dead(blk, offs.clone());
                    reference.push(DeadRun { block: blk, offsets: offs });
                }
            }
            b = end;
        }
        let merged = merge_dead_runs(&locals);
        assert_eq!(merged, reference, "merge diverged from serial block order");
    }
}

#[test]
fn run_merge_degenerate_shapes() {
    // No locals / empty locals.
    assert_eq!(merge_dead_runs(&[]), Vec::<DeadRun>::new());
    let empty = VacScanLocal::new(0, 1000, 1000);
    assert_eq!(merge_dead_runs(&[empty]), Vec::<DeadRun>::new());
    // Everything in one local.
    let mut one = VacScanLocal::new(0, 1000, 1000);
    one.record_dead(3, vec![1, 2]);
    one.record_dead(9, vec![5]);
    let merged = merge_dead_runs(&[one]);
    assert_eq!(merged.len(), 2);
    assert_eq!(merged[0].block, 3);
    assert_eq!(merged[1].block, 9);
}
