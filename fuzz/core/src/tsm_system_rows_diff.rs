//! tsm_system_rows_diff: differential fuzz driver — shipped Rust
//! `tsm_system_rows` vs vendored PostgreSQL 18.3 (Stamp-18.3, upstream sha
//! 62d6c7d3df) C (csrc/pg_tsm_system_rows_io.c, assembled by
//! csrc/gen/assemble_tsmrows.sh). Crate under test:
//! crates/contrib/tsm_system_rows.
//!
//! Comparison planes: value (every returned BlockNumber / OffsetNumber /
//! size estimate, bit-exact for the f64), error-verdict, and
//! errcode/sqlstate class (2202H <-> C class 1). Message text out of scope.
//!
//! stub:prng (census stub_req): the sampler seed is DERIVED ONCE from the
//! fuzz input and passed as the same value to BOTH sides (it is an explicit
//! compared input — C `system_rows_beginsamplescan(seed)` argument vs Rust
//! `begin_sample_scan(_, seed)`); `stubs::prng::pin_seed` is additionally
//! set from it so the global-prng analogs stay aligned per the STUBS.md
//! contract. Neither side reads a global prng in this family (the C sampler
//! seeds a local pg_prng_state via sampler_random_init_state).
//!
//! Input layout: [selector][payload]; selector % 6 picks the arm:
//!   0 beginsamplescan: [ntuples i64][seed u32][nblocks u16] — verdict +
//!     sqlstate planes (2202H on ntuples < 0), C use_pagemode branch
//!     witness, plus one nextsampleblock probe on the Ok path.
//!   1 scan walk: [seed u32][ntuples u16][nblocks0 u16][per-draw bytes
//!     (maxoffset u8, nblocks-delta u8)...] — full block/tuple sequence
//!     compared value-by-value, donetuples maintained executor-style
//!     (incremented per returned tuple offset).
//!   2 rescan walk: [seed u32][ntuples u16][nblocks0 u16][seed2 u32]
//!     [ntuples2 u16][per-draw bytes...] — two scans over one sampler;
//!     exercises the retained nblocks/firstblock/step path, lb reinit, and
//!     the shrink do-while (second-scan nblocks varied below the first's).
//!   3 samplescangetsamplesize: [flags u8][limit i64][pages u32][tuples
//!     f64-bits] — POST-FOLDING comparison per the census carve (the C
//!     oracle's estimate_expression_value is an identity shim; the driver
//!     pre-folds to Const / null-Const / non-Const). tuples sanitized to a
//!     finite |x| <= 1e18 f64 (fuzz-domain bound: keeps C's
//!     `(int64) baserel->tuples` cast defined; planner reltuples never
//!     approaches 2^63 — documented bound, not a behavior carve).
//!   4 random_relative_prime: [n u32][seed u64] — result + one raw
//!     post-draw from each side's prng state (witnesses the consumed-draw
//!     count, not just the returned value).
//!   5 registry plumbing (RUST-ONLY plane, no C counterpart): init_seams ->
//!     dfmgr lookup of HANDLER_SYMBOL -> fc handler call must error. The C
//!     handler (tsm_system_rows.c lines 77-99) is fmgr/makeNode TsmRoutine
//!     construction with no computation and is deliberately NOT vendored
//!     (contribafam precedent); the Rust handler is registry plumbing whose
//!     "cannot be called directly" verdict this arm executes and asserts.
//!
//! Fuzz-domain bounds (documented, all on COMPARED-EQUAL derived inputs so
//! neither side sees a value the other doesn't):
//!   - walk arms: nblocks0 <= 65535, per-draw nblocks stays within +/-8 of
//!     the scan's first nblocks and >= 1 once the sampler is initialized
//!     (nblocks == 0 after initialization would spin BOTH implementations
//!     forever in the lb-advance loop — upstream C has no interrupt check
//!     there either; unreachable via the executor, which re-reads
//!     rs_nblocks of a live relation). nblocks0 == 0 itself IS exercised
//!     (the empty-relation InvalidBlockNumber arm).
//!   - walk arms: <= 48 block draws / <= 300 tuple probes per block;
//!     maxoffset <= 255; ntuples <= 65535 (arm 0 passes full i64).
//!   - arm 3 limit bounded to |limit| < 2^62 (clamp_row_est round-trips
//!     through double; 2^63-adjacent int64 -> double -> int64 casts are UB
//!     in C — same defined-domain argument as above).
//!
//! SKIPPED (recorded per the fuzzuproof-crate exception rules):
//!   - C CHECK_FOR_INTERRUPTS in random_relative_prime: signal plumbing,
//!     no-op'd in the oracle; Rust documents the same elision (lib.rs
//!     comment) — the loop terminates for every input on both sides.
//!   - use_pagemode: set by the C callback (line 198) but by the RUST
//!     CALLER per the shipped API contract (census note). The arm asserts
//!     the C branch executed; there is no Rust field to compare.

use std::sync::Once;

use datum::{Datum, NullableDatum};
use tsm_system_rows::{
    random_relative_prime, sample_scan_get_sample_size, SystemRowsSampler, HANDLER_SYMBOL,
};
use types_error::ERRCODE_INVALID_TABLESAMPLE_ARGUMENT;
use types_fmgr::LocalFcinfo;

/// Oracle errcode class for 2202H (csrc/pg_tsm_system_rows_io.c).
const C_ERRCLASS_TABLESAMPLE: i32 = 1;

const INVALID_BLOCK: u32 = 0xFFFF_FFFF;
const INVALID_OFFSET: u16 = 0;

extern "C" {
    // Shared TLS errcode accessor (defined in csrc/pg_float_io.c).
    fn pg_diff_errcode_get() -> i32;
    // Oracle entries (csrc/pg_tsm_system_rows_io.c section 6).
    fn pg_tsmrows_scan_reset();
    fn pg_tsmrows_beginsamplescan(ntuples: i64, seed: u32) -> i32;
    fn pg_tsmrows_use_pagemode() -> i32;
    fn pg_tsmrows_nextsampleblock(nblocks: u32, donetuples: i64) -> u32;
    fn pg_tsmrows_nextsampletuple(blockno: u32, maxoffset: u16, donetuples: i64) -> u16;
    fn pg_tsmrows_getsamplesize(
        has_const: i32,
        isnull: i32,
        limitval: i64,
        pages: u32,
        tuples: f64,
        out_pages: *mut u32,
        out_tuples: *mut f64,
    );
    fn pg_tsmrows_random_relative_prime(n: u32, seed: u64, post_draw: *mut u64) -> u32;
}

// ---------------------------------------------------------------------------
// Little-endian field reader (zero-fills past the payload end so short
// inputs still exercise the arms).
// ---------------------------------------------------------------------------

struct Rd<'a>(&'a [u8]);

impl Rd<'_> {
    fn bytes<const N: usize>(&mut self) -> [u8; N] {
        let mut out = [0u8; N];
        let n = N.min(self.0.len());
        out[..n].copy_from_slice(&self.0[..n]);
        self.0 = &self.0[n..];
        out
    }
    fn u8(&mut self) -> u8 {
        u8::from_le_bytes(self.bytes::<1>())
    }
    fn u16(&mut self) -> u16 {
        u16::from_le_bytes(self.bytes::<2>())
    }
    fn u32(&mut self) -> u32 {
        u32::from_le_bytes(self.bytes::<4>())
    }
    fn u64(&mut self) -> u64 {
        u64::from_le_bytes(self.bytes::<8>())
    }
    fn i64(&mut self) -> i64 {
        i64::from_le_bytes(self.bytes::<8>())
    }
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

pub fn tsm_system_rows_diff(data: &[u8]) {
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    let Some((&sel, payload)) = data.split_first() else {
        return;
    };
    match sel % 6 {
        0 => begin_diff(payload),
        1 => walk_diff(payload, false),
        2 => walk_diff(payload, true),
        3 => getsamplesize_diff(payload),
        4 => random_relative_prime_diff(payload),
        _ => registry_plumbing(payload),
    }
}

// ---------------------------------------------------------------------------
// Shared: begin on both sides, compare verdict + errcode planes.
// Returns Some(sampler) when both sides accepted.
// ---------------------------------------------------------------------------

fn begin_both(
    sampler: &mut SystemRowsSampler,
    ntuples: i64,
    seed: u32,
) -> bool {
    crate::stubs::prng::pin_seed(seed as u64);
    let cst = unsafe { pg_tsmrows_beginsamplescan(ntuples, seed) };
    let cerr = unsafe { pg_diff_errcode_get() };
    match sampler.begin_sample_scan(ntuples, seed) {
        Ok(()) => {
            assert!(
                cst == 0,
                "beginsamplescan verdict DIVERGENCE ntuples={ntuples} seed={seed}: \
                 C=err({cerr}) Rust=Ok"
            );
            // C-branch witness: the verbatim callback forced pagemode
            // (tsm_system_rows.c line 198). Rust-side: caller contract.
            assert!(
                unsafe { pg_tsmrows_use_pagemode() } == 1,
                "C use_pagemode branch did not execute (oracle wiring dead?)"
            );
            true
        }
        Err(e) => {
            assert!(
                cst == 1
                    && cerr == C_ERRCLASS_TABLESAMPLE
                    && e.sqlstate == ERRCODE_INVALID_TABLESAMPLE_ARGUMENT,
                "beginsamplescan error DIVERGENCE ntuples={ntuples} seed={seed}: \
                 C=(st {cst}, err {cerr}) Rust=Err(sqlstate {:?})",
                e.sqlstate
            );
            false
        }
    }
}

// ---------------------------------------------------------------------------
// Arm 0: beginsamplescan verdict/errcode + a single block probe.
// ---------------------------------------------------------------------------

fn begin_diff(payload: &[u8]) {
    let mut rd = Rd(payload);
    let ntuples = rd.i64(); // FULL i64 domain, including the negative arm
    let seed = rd.u32();
    let nblocks = rd.u16() as u32;

    unsafe { pg_tsmrows_scan_reset() };
    let mut sampler = SystemRowsSampler::default();
    if begin_both(&mut sampler, ntuples, seed) {
        let cb = unsafe { pg_tsmrows_nextsampleblock(nblocks, 0) };
        let rb = sampler.next_sample_block(nblocks, 0);
        assert_eq!(
            cb, rb,
            "nextsampleblock probe DIVERGENCE ntuples={ntuples} seed={seed} nblocks={nblocks}"
        );
    }
}

// ---------------------------------------------------------------------------
// Arms 1/2: full scan walk (and rescan), executor-style donetuples.
// ---------------------------------------------------------------------------

/// One scan: draw blocks until either side says done (asserting equality at
/// every step), probing tuples within each returned block. Returns the
/// final donetuples count.
fn walk_scan(
    sampler: &mut SystemRowsSampler,
    rd: &mut Rd<'_>,
    ntuples: i64,
    nblocks0: u32,
) -> i64 {
    let mut donetuples: i64 = 0;
    let mut pinned_nblocks: Option<u32> = None;
    for _ in 0..48 {
        // Vary nblocks per draw within +/-8 of the scan's first value, >= 1
        // once the sampler holds a nonzero nblocks (see module header).
        let delta = (rd.u8() % 17) as i64 - 8;
        let nblocks = match pinned_nblocks {
            None => nblocks0,
            Some(first) => (first as i64 + delta).max(1) as u32,
        };
        let cb = unsafe { pg_tsmrows_nextsampleblock(nblocks, donetuples) };
        let rb = sampler.next_sample_block(nblocks, donetuples);
        assert_eq!(
            cb, rb,
            "nextsampleblock DIVERGENCE nblocks={nblocks} donetuples={donetuples}"
        );
        if rb == INVALID_BLOCK {
            break;
        }
        if pinned_nblocks.is_none() && nblocks != 0 {
            pinned_nblocks = Some(nblocks);
        }
        let maxoffset = rd.u8() as u16;
        for _ in 0..300 {
            let ct = unsafe { pg_tsmrows_nextsampletuple(rb, maxoffset, donetuples) };
            let rt = sampler.next_sample_tuple(maxoffset, donetuples);
            assert_eq!(
                ct, rt,
                "nextsampletuple DIVERGENCE block={rb} maxoffset={maxoffset} \
                 donetuples={donetuples}"
            );
            if rt == INVALID_OFFSET {
                break;
            }
            donetuples += 1; // executor: one tuple returned
        }
    }
    donetuples
}

fn walk_diff(payload: &[u8], rescan: bool) {
    let mut rd = Rd(payload);
    let seed = rd.u32();
    let ntuples = rd.u16() as i64;
    let nblocks0 = rd.u16() as u32;

    unsafe { pg_tsmrows_scan_reset() };
    let mut sampler = SystemRowsSampler::default();
    if !begin_both(&mut sampler, ntuples, seed) {
        return; // unreachable (ntuples >= 0) but keep the verdict plane armed
    }
    walk_scan(&mut sampler, &mut rd, ntuples, nblocks0);

    if rescan {
        // Second scan over the SAME sampler: pattern (nblocks/firstblock/
        // step) must be retained even under a different executor seed.
        let seed2 = rd.u32();
        let ntuples2 = rd.u16() as i64;
        if !begin_both(&mut sampler, ntuples2, seed2) {
            return;
        }
        walk_scan(&mut sampler, &mut rd, ntuples2, nblocks0);
    }
}

// ---------------------------------------------------------------------------
// Arm 3: samplescangetsamplesize (post-folding, census carve).
// ---------------------------------------------------------------------------

fn getsamplesize_diff(payload: &[u8]) {
    let mut rd = Rd(payload);
    let flags = rd.u8();
    let has_const = flags & 1 != 0;
    let isnull = flags & 2 != 0;
    // |limit| < 2^62 (defined-domain bound, see module header).
    let limit = rd.i64() % (1i64 << 62);
    let pages = rd.u32();
    // Finite |tuples| <= 1e18 (defined-domain bound, see module header).
    let mut tuples = f64::from_bits(rd.u64());
    if !tuples.is_finite() || tuples.abs() > 1e18 {
        tuples = (tuples.to_bits() % (2_000_000_000_000_000_001)) as f64 - 1e18;
    }

    let mut c_pages: u32 = 0;
    let mut c_tuples: f64 = 0.0;
    unsafe {
        pg_tsmrows_getsamplesize(
            has_const as i32,
            isnull as i32,
            limit,
            pages,
            tuples,
            &mut c_pages,
            &mut c_tuples,
        );
    }
    let limit_opt = if has_const && !isnull { Some(limit) } else { None };
    let (r_pages, r_tuples) = sample_scan_get_sample_size(limit_opt, pages, tuples);
    assert!(
        c_pages == r_pages && c_tuples.to_bits() == r_tuples.to_bits(),
        "getsamplesize DIVERGENCE limit={limit_opt:?} pages={pages} tuples={tuples}: \
         C=({c_pages}, {c_tuples}) Rust=({r_pages}, {r_tuples})"
    );
}

// ---------------------------------------------------------------------------
// Arm 4: random_relative_prime + post-draw state witness.
// ---------------------------------------------------------------------------

fn random_relative_prime_diff(payload: &[u8]) {
    let mut rd = Rd(payload);
    let n = rd.u32();
    let seed = rd.u64();
    crate::stubs::prng::pin_seed(seed);

    let mut c_post: u64 = 0;
    let cr = unsafe { pg_tsmrows_random_relative_prime(n, seed, &mut c_post) };

    let mut prng = pg_prng::PgPrng::seeded(seed);
    let rr = random_relative_prime(n, &mut prng);
    let r_post = prng.next_u64();
    assert!(
        cr == rr && c_post == r_post,
        "random_relative_prime DIVERGENCE n={n} seed={seed}: \
         C=({cr}, post {c_post}) Rust=({rr}, post {r_post})"
    );
}

// ---------------------------------------------------------------------------
// Arm 5: registry plumbing (Rust-only plane — see module header).
// ---------------------------------------------------------------------------

static INIT: Once = Once::new();

fn registry_plumbing(_payload: &[u8]) {
    INIT.call_once(tsm_system_rows::init_seams);
    let f = dfmgr::load_external_function("tsm_system_rows", HANDLER_SYMBOL, true)
        .expect("registered handler must resolve")
        .expect("registered handler must be Some");
    let cx = mcx::MemoryContext::new("tsmrows_fc");
    let m = cx.mcx();
    let mut fcinfo = LocalFcinfo::<1>::new(0);
    // SAFETY: `cx` outlives this single call.
    unsafe { fcinfo.set_result_mcx(m) };
    fcinfo.args[0] = NullableDatum::value(Datum::from_usize(0));
    let r = f(None, &mut fcinfo);
    assert!(
        r.is_err(),
        "fc_tsm_system_rows_handler must refuse direct calls"
    );
    // An unknown symbol misses exactly like C's lookup miss.
    assert!(
        dfmgr::load_external_function("tsm_system_rows", "no_such_symbol", false)
            .expect("suppressed miss is Ok")
            .is_none(),
        "unknown symbol must miss"
    );
}

// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Replay every checked-in seed (catches shim/link errors before the
    /// nightly fuzz campaign).
    #[test]
    fn seed_corpus_replays_clean() {
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/tsm_system_rows_diff");
        let mut n = 0;
        for e in std::fs::read_dir(dir).expect("corpus/tsm_system_rows_diff missing") {
            let p = e.unwrap().path();
            if p.is_file() && p.file_name().is_some_and(|f| f != ".gitkeep") {
                tsm_system_rows_diff(&std::fs::read(&p).unwrap());
                n += 1;
            }
        }
        assert!(n >= 30, "expected >=30 seeds, found {n}");
    }

    /// Per-arm smoke: ok + error shapes per arm.
    #[test]
    fn arms_smoke() {
        // Arm 0: ok begin + probe (ntuples 5, seed 1, nblocks 7).
        let mut v = vec![0u8];
        v.extend(5i64.to_le_bytes());
        v.extend(1u32.to_le_bytes());
        v.extend(7u16.to_le_bytes());
        tsm_system_rows_diff(&v);
        // Arm 0: negative ntuples -> 2202H both sides.
        let mut v = vec![0u8];
        v.extend((-3i64).to_le_bytes());
        v.extend(1u32.to_le_bytes());
        v.extend(7u16.to_le_bytes());
        tsm_system_rows_diff(&v);
        // Arm 1: walk (seed 42, ntuples 100, nblocks 13).
        let mut v = vec![1u8];
        v.extend(42u32.to_le_bytes());
        v.extend(100u16.to_le_bytes());
        v.extend(13u16.to_le_bytes());
        v.extend([0x55u8; 64]);
        tsm_system_rows_diff(&v);
        // Arm 1: empty relation (nblocks0 = 0).
        let mut v = vec![1u8];
        v.extend(42u32.to_le_bytes());
        v.extend(100u16.to_le_bytes());
        v.extend(0u16.to_le_bytes());
        tsm_system_rows_diff(&v);
        // Arm 2: rescan with a different executor seed.
        let mut v = vec![2u8];
        v.extend(7u32.to_le_bytes());
        v.extend(50u16.to_le_bytes());
        v.extend(11u16.to_le_bytes());
        v.extend([0x20u8; 32]);
        v.extend(999u32.to_le_bytes());
        v.extend(50u16.to_le_bytes());
        v.extend([0x10u8; 32]);
        tsm_system_rows_diff(&v);
        // Arm 3: Const limit / non-Const / null-Const.
        for flags in [1u8, 0, 3] {
            let mut v = vec![3u8, flags];
            v.extend(500i64.to_le_bytes());
            v.extend(64u32.to_le_bytes());
            v.extend(1000.0f64.to_bits().to_le_bytes());
            tsm_system_rows_diff(&v);
        }
        // Arm 4: n = 0, 1, small, large.
        for n in [0u32, 1, 12, 97, 0xFFFF_FFFF] {
            let mut v = vec![4u8];
            v.extend(n.to_le_bytes());
            v.extend(0xDEADBEEFu64.to_le_bytes());
            tsm_system_rows_diff(&v);
        }
        // Arm 5: registry plumbing.
        tsm_system_rows_diff(&[5u8]);
    }
}
