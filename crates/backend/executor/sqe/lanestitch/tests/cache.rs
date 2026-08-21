// Gates for the Tier-B stitched-body cache (production-plan §3 P1-3:
// "stitched bodies cached by shape fingerprint; prepared statements never
// re-stitch"): hit/miss identity, golden-encoding stability, LRU eviction
// under the byte cap, refcount-while-executing (bodies never freed
// mid-execution), concurrent get_or_stitch, the forged-collision
// structural-verify membrane (the Q4 pattern), per-handle sticky refusal
// over shared pages, and the generation retirement hook. Every body that
// runs here is compared against the oracle interpreter.
//
// Requires the `oracle` feature. Off-aarch64 every get_or_stitch refuses
// and the tests reduce to that refusal.

use datum::{Datum, NullableDatum};
use sqe_lanestitch::{
    eval_project, eval_qual, ArithOp, Batch, CmpOp, Lane, OutLane, ProjOutcome, Program,
    QualOutcome, SelVec, Step, StitchCache, StitchedProgram, STITCH_CACHE_CAP_BYTES,
};

// ---- shape + batch helpers -----------------------------------------------

/// `col0 > k` — one body shape per distinct k (identical instruction
/// count, so every instance maps the same number of pages).
fn gt_prog(k: i32) -> Program {
    let mut p = Program::new();
    let kk = p.push_const(NullableDatum { value: Datum::from_i32(k), isnull: false });
    p.steps.extend([
        Step::LoadLane { col: 0, out: 0 },
        Step::LoadConst { k: kk, out: 1 },
        Step::Cmp { op: CmpOp::Int4Gt, a: 0, b: 1, out: 2 },
        Step::Qual { a: 2 },
    ]);
    p
}

/// `100 / col0 > 1` — the erroring shape (division by zero refuses).
fn div_prog() -> Program {
    let mut p = Program::new();
    let k100 = p.push_const(NullableDatum { value: Datum::from_i32(100), isnull: false });
    let k1 = p.push_const(NullableDatum { value: Datum::from_i32(1), isnull: false });
    p.steps.extend([
        Step::LoadConst { k: k100, out: 0 },
        Step::LoadLane { col: 0, out: 1 },
        Step::Arith { op: ArithOp::Div4, a: 0, b: 1, out: 2 },
        Step::LoadConst { k: k1, out: 3 },
        Step::Cmp { op: CmpOp::Int4Gt, a: 2, b: 3, out: 4 },
        Step::Qual { a: 4 },
    ]);
    p
}

struct Col {
    values: Vec<Datum>,
    isnull: Vec<bool>,
}

fn col_i32(vals: &[i32]) -> Col {
    Col {
        values: vals.iter().map(|&v| Datum::from_i32(v)).collect(),
        isnull: vec![false; vals.len()],
    }
}

fn run_bits(jit: &StitchedProgram, col: &Col) -> (QualOutcome, Vec<bool>) {
    let n = col.values.len() as u32;
    let lanes = [Lane { values: &col.values, isnull: &col.isnull }];
    let mut sel = SelVec::all(n);
    let oc = jit.run_lanes(n, &lanes, &mut sel);
    (oc, (0..n).map(|i| sel.contains(i)).collect())
}

fn oracle_bits(prog: &Program, col: &Col) -> Vec<bool> {
    let n = col.values.len() as u32;
    let batch = Batch { nrows: n, lanes: vec![Lane { values: &col.values, isnull: &col.isnull }] };
    let mut sel = SelVec::all(n);
    eval_qual(prog, &batch, &mut sel).expect("oracle must not error on this batch");
    (0..n).map(|i| sel.contains(i)).collect()
}

/// Assert one handle answers exactly like the oracle on `vals`.
fn assert_oracle_parity(jit: &StitchedProgram, prog: &Program, vals: &[i32]) {
    let col = col_i32(vals);
    let (oc, got) = run_bits(jit, &col);
    assert_eq!(oc, QualOutcome::Stitched);
    assert_eq!(got, oracle_bits(prog, &col), "cached body diverged from the oracle");
}

/// Page-rounded bytes one gt_prog body retains in a cache (the cap
/// currency for the eviction geometry below).
fn body_bytes() -> usize {
    let c = StitchCache::new(STITCH_CACHE_CAP_BYTES);
    c.get_or_stitch(&gt_prog(0), 1).expect("stitch");
    let b = c.stats().bytes;
    assert!(b > 0);
    b
}

const VALS: &[i32] = &[i32::MIN, -7, -1, 0, 1, 2, 3, 5, 100, i32::MAX];

// ---- the gates -----------------------------------------------------------

#[test]
fn hit_miss_identity_and_golden_bytes() {
    let cache = StitchCache::new(STITCH_CACHE_CAP_BYTES);
    let Some(a) = cache.get_or_stitch(&gt_prog(3), 1) else {
        assert!(!sqe_lanestitch::available());
        return;
    };
    let s = cache.stats();
    assert_eq!((s.hits, s.misses, s.entries), (0, 1, 1));

    // Hit: no re-stitch, same installed body, byte-identical to a fresh
    // direct stitch (golden-encoding stability).
    let b = cache.get_or_stitch(&gt_prog(3), 1).expect("hit");
    let s = cache.stats();
    assert_eq!((s.hits, s.misses, s.entries), (1, 1, 1));
    assert_eq!(a.entry_addr(), b.entry_addr(), "a hit must reuse the installed pages");
    let fresh = StitchedProgram::compile(&gt_prog(3), 1).expect("direct stitch");
    assert_eq!(b.code(), fresh.code(), "cached body bytes != fresh-stitch bytes");

    // Different const, ncols, or opts = different canonical key = miss.
    assert!(cache.get_or_stitch(&gt_prog(4), 1).is_some());
    assert!(cache.get_or_stitch(&gt_prog(3), 2).is_some());
    let s = cache.stats();
    assert_eq!((s.hits, s.misses, s.entries), (1, 3, 3));

    // Both handles are oracle-correct.
    assert_oracle_parity(&a, &gt_prog(3), VALS);
    assert_oracle_parity(&b, &gt_prog(3), VALS);
}

#[test]
fn lru_eviction_under_byte_cap() {
    if !sqe_lanestitch::available() {
        return;
    }
    let e = body_bytes();
    let cache = StitchCache::new(3 * e);
    for k in 1..=4 {
        cache.get_or_stitch(&gt_prog(k), 1).expect("stitch");
    }
    // Cap holds 3: inserting the 4th evicted the LRU (shape 1).
    let s = cache.stats();
    assert_eq!((s.entries, s.evictions), (3, 1));
    assert!(s.bytes <= 3 * e, "cap violated: {} > {}", s.bytes, 3 * e);

    // Touch shape 2 (now MRU), insert shape 5: the victim must be the
    // LRU shape 3, not the recently used 2.
    cache.get_or_stitch(&gt_prog(2), 1).expect("hit");
    cache.get_or_stitch(&gt_prog(5), 1).expect("stitch");
    let before = cache.stats();
    cache.get_or_stitch(&gt_prog(2), 1).expect("shape 2 must have survived");
    assert_eq!(cache.stats().hits, before.hits + 1, "shape 2 probe must be a hit");
    cache.get_or_stitch(&gt_prog(3), 1).expect("re-stitch");
    assert_eq!(cache.stats().misses, before.misses + 1, "shape 3 must have been evicted");
}

#[test]
fn cap_smaller_than_one_body_caches_nothing_but_handles_run() {
    if !sqe_lanestitch::available() {
        return;
    }
    let cache = StitchCache::new(1);
    let prog = gt_prog(2);
    let jit = cache.get_or_stitch(&prog, 1).expect("stitch");
    let s = cache.stats();
    assert_eq!((s.entries, s.bytes), (0, 0), "a sub-body cap retains nothing");
    assert!(s.evictions >= 1);
    // The handle's Arc keeps the pages mapped regardless.
    assert_oracle_parity(&jit, &prog, VALS);
}

/// Refcount-while-executing: evicting a body a live handle shares must
/// not unmap it — the handle still runs correctly afterwards.
#[test]
fn evicted_body_stays_mapped_while_a_handle_lives() {
    if !sqe_lanestitch::available() {
        return;
    }
    let e = body_bytes();
    let cache = StitchCache::new(e); // exactly one resident body
    let prog = gt_prog(7);
    let held = cache.get_or_stitch(&prog, 1).expect("stitch");
    let held_code: Vec<u8> = held.code().to_vec();
    // Evict it by inserting other shapes, and scribble fresh bodies over
    // any recycled address space.
    for k in 100..108 {
        let other = cache.get_or_stitch(&gt_prog(k), 1).expect("stitch");
        assert_oracle_parity(&other, &gt_prog(k), VALS);
    }
    assert!(cache.stats().evictions >= 7);
    // The held handle's pages are intact (bytes unchanged) and executable.
    assert_eq!(held.code(), &held_code[..], "evicted body's pages changed under a live handle");
    assert_oracle_parity(&held, &prog, VALS);
    // Re-probing after eviction is a miss that re-stitches identically.
    let again = cache.get_or_stitch(&prog, 1).expect("re-stitch");
    assert_eq!(again.code(), &held_code[..], "re-stitch after eviction must be byte-identical");
}

#[test]
fn concurrent_get_or_stitch_shares_one_body_per_shape() {
    if !sqe_lanestitch::available() {
        return;
    }
    let cache = std::sync::Arc::new(StitchCache::new(STITCH_CACHE_CAP_BYTES));
    const SHAPES: i32 = 6;
    const THREADS: usize = 8;
    const ITERS: usize = 50;
    let mut handles = Vec::new();
    for t in 0..THREADS {
        let cache = std::sync::Arc::clone(&cache);
        handles.push(std::thread::spawn(move || {
            for i in 0..ITERS {
                let k = ((t + i) as i32) % SHAPES;
                let prog = gt_prog(k);
                let jit = cache.get_or_stitch(&prog, 1).expect("stitch");
                // Execute the (possibly shared, possibly just-installed)
                // body and check it against the scalar truth.
                let col = col_i32(VALS);
                let (oc, got) = run_bits(&jit, &col);
                assert_eq!(oc, QualOutcome::Stitched);
                let want: Vec<bool> = VALS.iter().map(|&v| v > k).collect();
                assert_eq!(got, want, "thread {t} iter {i}: shared body diverged");
            }
        }));
    }
    for h in handles {
        h.join().expect("worker panicked");
    }
    let s = cache.stats();
    // The stitch/stitch race resolves by adoption: exactly one resident
    // body per shape, and every probe either hit or filled.
    assert_eq!(s.entries, SHAPES as usize);
    assert_eq!(s.evictions, 0);
    assert_eq!(s.hits + s.misses, (THREADS * ITERS) as u64);
    assert!(s.misses >= SHAPES as u64);
}

/// The Q4 forged-collision gate: two semantically different programs
/// forced onto ONE fingerprint index (simulating a broken/omitting hash)
/// must NOT share a body — the always-on structural-verify membrane (full
/// canonical-byte compare on every hit) keeps them distinct entries.
#[test]
fn forged_fingerprint_collision_never_shares_a_body() {
    if !sqe_lanestitch::available() {
        return;
    }
    let cache = StitchCache::new(STITCH_CACHE_CAP_BYTES);
    const FORGED_FP: u64 = 0xDEAD_BEEF_DEAD_BEEF;
    let pa = gt_prog(5);
    let pb = gt_prog(7); // different shape, same forced index
    let a = cache.get_or_stitch_forced_fp(FORGED_FP, &pa, 1).expect("stitch a");
    let b = cache.get_or_stitch_forced_fp(FORGED_FP, &pb, 1).expect("stitch b");
    assert_eq!(cache.stats().entries, 2, "colliding shapes must occupy distinct entries");
    assert_ne!(a.entry_addr(), b.entry_addr(), "colliding shapes must not share pages");
    assert_ne!(a.code(), b.code());
    // Each body answers ITS OWN program's semantics, oracle-checked.
    assert_oracle_parity(&a, &pa, VALS);
    assert_oracle_parity(&b, &pb, VALS);
    // Re-probes under the forged index hit their own entry, not the
    // sibling: hits climb, and the bytes match the original body.
    let a2 = cache.get_or_stitch_forced_fp(FORGED_FP, &pa, 1).expect("hit a");
    let b2 = cache.get_or_stitch_forced_fp(FORGED_FP, &pb, 1).expect("hit b");
    assert_eq!(cache.stats().hits, 2);
    assert_eq!(a2.entry_addr(), a.entry_addr());
    assert_eq!(b2.entry_addr(), b.entry_addr());
}

/// Sticky refusal is a PER-HANDLE rail: one statement's data-error must
/// not poison other statements sharing the same cached pages.
#[test]
fn sticky_refusal_stays_per_handle_over_shared_pages() {
    if !sqe_lanestitch::available() {
        return;
    }
    let cache = StitchCache::new(STITCH_CACHE_CAP_BYTES);
    let prog = div_prog();
    let h1 = cache.get_or_stitch(&prog, 1).expect("stitch");
    let h2 = cache.get_or_stitch(&prog, 1).expect("hit");
    assert_eq!(h1.entry_addr(), h2.entry_addr(), "premise: shared pages");

    // h1 trips the erroring stencil (col0 = 0) and refuses stickily.
    let trap = col_i32(&[4, 0, 9]);
    let (oc, _) = run_bits(&h1, &trap);
    assert_eq!(oc, QualOutcome::Refused);
    let clean = col_i32(&[4, 9, 200]);
    let (oc, _) = run_bits(&h1, &clean);
    assert_eq!(oc, QualOutcome::Refused, "refusal must be sticky on the refusing handle");

    // h2 and a fresh third handle keep running the same pages clean.
    assert_oracle_parity(&h2, &prog, &[4, 9, 200, 1, -50]);
    let h3 = cache.get_or_stitch(&prog, 1).expect("hit");
    assert_eq!(h3.entry_addr(), h1.entry_addr());
    assert_oracle_parity(&h3, &prog, &[4, 9, 200]);
}

#[test]
fn generation_bump_retires_every_entry() {
    if !sqe_lanestitch::available() {
        return;
    }
    let cache = StitchCache::new(STITCH_CACHE_CAP_BYTES);
    let held = cache.get_or_stitch(&gt_prog(1), 1).expect("stitch");
    cache.get_or_stitch(&gt_prog(2), 1).expect("stitch");
    assert_eq!(cache.stats().entries, 2);

    cache.bump_generation();
    let s = cache.stats();
    assert_eq!((s.entries, s.bytes), (0, 0), "bump must retire and reap every entry");
    assert_eq!(s.evictions, 2);

    // Old-generation probes miss and re-stitch byte-identically; the
    // held pre-bump handle still executes (its Arc outlives retirement).
    let renew = cache.get_or_stitch(&gt_prog(1), 1).expect("re-stitch");
    assert_eq!(cache.stats().misses, 3);
    assert_eq!(renew.code(), held.code());
    assert_oracle_parity(&held, &gt_prog(1), VALS);
    assert_oracle_parity(&renew, &gt_prog(1), VALS);
}

#[test]
fn projection_cache_hit_matches_oracle() {
    let cache = StitchCache::new(STITCH_CACHE_CAP_BYTES);
    // out0 = col0 (int4 identity projection).
    let mut prog = Program::new();
    prog.steps.extend([Step::LoadLane { col: 0, out: 0 }, Step::StoreOut { a: 0, out: 0 }]);
    let Some(first) = cache.get_or_stitch_project(&prog, 1, 1) else {
        assert!(!sqe_lanestitch::available());
        return;
    };
    let hit = cache.get_or_stitch_project(&prog, 1, 1).expect("hit");
    let s = cache.stats();
    assert_eq!((s.hits, s.misses, s.entries), (1, 1, 1));
    assert_eq!(first.entry_addr(), hit.entry_addr());
    assert_eq!(hit.code(), first.code());

    // A qual program under the SAME (ncols) must not alias the proj entry
    // (the kind byte splits the canonical key space).
    cache.get_or_stitch(&gt_prog(0), 1).expect("stitch");
    assert_eq!(cache.stats().entries, 2);

    let col = col_i32(&[5, -7, 0, 42, i32::MIN]);
    let n = col.values.len();
    let sel = SelVec::all(n as u32);
    let nwords = n.div_ceil(64);
    let batch =
        Batch { nrows: n as u32, lanes: vec![Lane { values: &col.values, isnull: &col.isnull }] };

    let mut want_v = vec![Datum::null(); n];
    let mut want_n = vec![false; n];
    {
        let mut outs = [OutLane { values: &mut want_v, isnull: &mut want_n }];
        eval_project(&prog, &batch, &sel, &mut outs).expect("oracle projects");
    }
    let mut got_v = vec![Datum::null(); n];
    let mut got_n = vec![false; n];
    {
        let mut outs = [OutLane { values: &mut got_v, isnull: &mut got_n }];
        assert_eq!(
            hit.run_into(n as u32, &batch.lanes, &sel.words[..nwords], &mut outs),
            ProjOutcome::Stitched
        );
    }
    assert_eq!(
        got_v.iter().map(|d| d.as_i64()).collect::<Vec<_>>(),
        want_v.iter().map(|d| d.as_i64()).collect::<Vec<_>>()
    );
    assert_eq!(got_n, want_n);
}
