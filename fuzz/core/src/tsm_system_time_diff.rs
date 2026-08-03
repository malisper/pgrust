//! tsm_system_time_diff: differential fuzz driver — shipped Rust
//! `tsm_system_time` vs vendored PostgreSQL 18.3 (Stamp-18.3, upstream sha
//! 62d6c7d3df) C (csrc/pg_tsm_system_time_io.c, assembled by
//! csrc/gen/assemble_tsmtime.sh). Crate under test:
//! crates/contrib/tsm_system_time. Sibling of tsm_system_rows_diff (same
//! family shape; census carve note: file-grain, same shape as
//! tsm_system_rows, plus the monotonic-clock read).
//!
//! Comparison planes: value (every returned BlockNumber / OffsetNumber /
//! size estimate, bit-exact for the f64), error-verdict, and
//! errcode/sqlstate class (2202H <-> C class 1). Message text out of scope.
//!
//! stub:prng (census stub_req): the sampler seed is DERIVED ONCE from the
//! fuzz input and passed as the same value to BOTH sides (explicit compared
//! input — C `system_time_beginsamplescan(seed)` argument vs Rust
//! `begin_sample_scan(_, seed)`); `stubs::prng::pin_seed` is additionally
//! set from it so the global-prng analogs stay aligned per the STUBS.md
//! contract. Neither side reads a global prng in this family.
//!
//! stub:clock (census stub_req): `system_time_nextsampleblock` reads the
//! monotonic clock (C INSTR_TIME_SET_CURRENT / Rust `pg_clock::mono_ns`).
//! The clock SEQUENCE is derived from the fuzz input as base + per-draw
//! increments and pinned identically on both sides before every paired
//! call via `stubs::clock::pin_mono_ns` (Rust: pg_clock's fuzz_mono_pin
//! feature cell; C: pg_stub_get_mono_ns, which the oracle TU's
//! INSTR_TIME_SET_CURRENT shim reads — all elapsed ARITHMETIC after the
//! read is verbatim instr_time.h). The pin is constant within one paired
//! call and advances only between draws, so both sides see identical
//! readings by construction.
//!
//! Input layout: [selector][payload]; selector % 6 picks the arm:
//!   0 beginsamplescan: [millis f64-bits][seed u32][nblocks u16]
//!     [clockbase u32] — verdict + sqlstate planes (2202H on millis < 0 or
//!     NaN; FULL f64 bit domain incl. -0.0/inf/NaN), plus one
//!     nextsampleblock probe on the Ok path under a pinned clock.
//!   1 scan walk: [seed u32][millis-q u32][nblocks0 u16][clockbase u32]
//!     [per-draw bytes (delta u16, nblocks-delta u8, maxoffset u8)...] —
//!     full block/tuple sequence compared value-by-value; the time budget
//!     is exercised deterministically (millis = q/16.0 ms; per-draw clock
//!     increment = delta<<16 ns, i.e. up to ~4295 ms per draw).
//!   2 rescan walk: arm-1 layout + [seed2 u32][millis2-q u32][per-draw
//!     bytes...] — two scans over one sampler; exercises the retained
//!     nblocks/firstblock/step path, lb/start_time reinit, and the shrink
//!     do-while (second-scan nblocks varied below the first's). The clock
//!     keeps advancing monotonically across the rescan.
//!   3 samplescangetsamplesize: [flags u8][limit f64-bits][spc f64-bits]
//!     [pages u32][tuples f64-bits] — POST-FOLDING comparison per the
//!     census carve (the C oracle's estimate_expression_value is an
//!     identity shim; the driver pre-folds to Const / null-Const /
//!     non-Const). C get_tablespace_page_costs is shimmed to report the
//!     driver's spc value — the SAME compared input the shipped Rust entry
//!     point takes as its spc_random_page_cost argument.
//!   4 random_relative_prime: [n u32][seed u64] — result + one raw
//!     post-draw from each side's prng state (witnesses the consumed-draw
//!     count, not just the returned value). Domain u32 x u64 seed = 2^96 —
//!     NOT exhaustible (a0 checked; an n-only 2^32 slice would pin the
//!     seed, so the fuzz route is retained).
//!   5 registry plumbing (RUST-ONLY plane, no C counterpart): init_seams ->
//!     dfmgr lookup of HANDLER_SYMBOL -> fc handler call must error. The C
//!     handler (tsm_system_time.c lines 77-99) is fmgr/makeNode TsmRoutine
//!     construction with no computation and is deliberately NOT vendored
//!     (tsm_system_rows/contribafam precedent).
//!
//! Fuzz-domain bounds (documented, all on COMPARED-EQUAL derived inputs so
//! neither side sees a value the other doesn't):
//!   - walk arms: nblocks0 <= 65535, per-draw nblocks stays within +/-8 of
//!     the scan's first nblocks and >= 1 once the sampler is initialized
//!     (nblocks == 0 after initialization would spin BOTH implementations
//!     forever in the lb-advance do-while — tsm_system_time.c lines
//!     271-275 have the SAME shrink loop as tsm_system_rows; unreachable
//!     via the executor, which re-reads rs_nblocks of a live relation).
//!     nblocks0 == 0 itself IS exercised (the empty-relation
//!     InvalidBlockNumber arm).
//!   - walk arms: <= 48 block draws per scan / <= 300 tuple probes per
//!     block; maxoffset <= 255.
//!   - walk arms derive millis = (u32)/16.0 — a nonnegative ms budget up
//!     to ~2.7e8 ms, overlapping the reachable pinned-elapsed range
//!     (<= ~4.1e11 ns per scan) so BOTH branches of the time check fire;
//!     millis < 0 / NaN (the 2202H arm) is exercised by arm 0's full-bits
//!     millis.
//!   - pinned clock sequence: base = (u32)<<16 ns (< 2^48), increments
//!     (u16)<<16 ns — non-decreasing and far below i64 wrap, keeping the
//!     Rust u64 `wrapping_sub` and the C verbatim int64 INSTR_TIME_SUBTRACT
//!     in the identical defined region (a REAL monotonic clock never
//!     regresses either; documented bound, not a behavior carve).
//!   - arm 3 spc_random_page_cost folded to a finite value in [0, ~4.2e6]
//!     when the raw f64 is negative/NaN/inf (GUC random_page_cost is
//!     bounded [0, DBL_MAX] and never NaN/inf — legal-range derivation per
//!     the STUBS.md pin rule; limit and tuples stay FULL f64 bit domain,
//!     defined on both sides — no int64 cast exists in this family's C,
//!     unlike tsm_system_rows).
//!
//! SKIPPED (recorded per the fuzzuproof-crate exception rules):
//!   - C CHECK_FOR_INTERRUPTS in random_relative_prime: signal plumbing,
//!     no-op'd in the oracle; Rust documents the same elision (lib.rs
//!     comment) — the loop terminates for every input on both sides.

use std::sync::Once;

use datum::{Datum, NullableDatum};
use tsm_system_time::{
    random_relative_prime, sample_scan_get_sample_size, SystemTimeSampler, HANDLER_SYMBOL,
};
use types_error::ERRCODE_INVALID_TABLESAMPLE_ARGUMENT;
use types_fmgr::LocalFcinfo;

/// Oracle errcode class for 2202H (csrc/pg_tsm_system_time_io.c).
const C_ERRCLASS_TABLESAMPLE: i32 = 1;

const INVALID_BLOCK: u32 = 0xFFFF_FFFF;
const INVALID_OFFSET: u16 = 0;

extern "C" {
    // Shared TLS errcode accessor (defined in csrc/pg_float_io.c).
    fn pg_diff_errcode_get() -> i32;
    // Oracle entries (csrc/pg_tsm_system_time_io.c section 12).
    fn pg_tsmtime_scan_reset();
    fn pg_tsmtime_beginsamplescan(millis_bits: u64, seed: u32) -> i32;
    fn pg_tsmtime_nextsampleblock(nblocks: u32) -> u32;
    fn pg_tsmtime_nextsampletuple(blockno: u32, maxoffset: u16) -> u16;
    fn pg_tsmtime_getsamplesize(
        has_const: i32,
        isnull: i32,
        limit_bits: u64,
        spc_random_page_cost: f64,
        pages: u32,
        tuples: f64,
        out_pages: *mut u32,
        out_tuples: *mut f64,
    );
    fn pg_tsmtime_random_relative_prime(n: u32, seed: u64, post_draw: *mut u64) -> u32;
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
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

pub fn tsm_system_time_diff(data: &[u8]) {
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
// Returns true when both sides accepted.
// ---------------------------------------------------------------------------

fn begin_both(sampler: &mut SystemTimeSampler, millis: f64, seed: u32) -> bool {
    crate::stubs::prng::pin_seed(seed as u64);
    let cst = unsafe { pg_tsmtime_beginsamplescan(millis.to_bits(), seed) };
    let cerr = unsafe { pg_diff_errcode_get() };
    match sampler.begin_sample_scan(millis, seed) {
        Ok(()) => {
            assert!(
                cst == 0,
                "beginsamplescan verdict DIVERGENCE millis={millis:?} seed={seed}: \
                 C=err({cerr}) Rust=Ok"
            );
            true
        }
        Err(e) => {
            assert!(
                cst == 1
                    && cerr == C_ERRCLASS_TABLESAMPLE
                    && e.sqlstate == ERRCODE_INVALID_TABLESAMPLE_ARGUMENT,
                "beginsamplescan error DIVERGENCE millis={millis:?} seed={seed}: \
                 C=(st {cst}, err {cerr}) Rust=Err(sqlstate {:?})",
                e.sqlstate
            );
            false
        }
    }
}

// ---------------------------------------------------------------------------
// Arm 0: beginsamplescan verdict/errcode (FULL f64 millis) + a block probe.
// ---------------------------------------------------------------------------

fn begin_diff(payload: &[u8]) {
    let mut rd = Rd(payload);
    let millis = f64::from_bits(rd.u64()); // FULL bit domain incl. NaN/-x/inf
    let seed = rd.u32();
    let nblocks = rd.u16() as u32;
    let base_ns = (rd.u32() as u64) << 16;

    unsafe { pg_tsmtime_scan_reset() };
    let mut sampler = SystemTimeSampler::default();
    if begin_both(&mut sampler, millis, seed) {
        crate::stubs::clock::pin_mono_ns(base_ns);
        let cb = unsafe { pg_tsmtime_nextsampleblock(nblocks) };
        let rb = sampler.next_sample_block(nblocks);
        assert_eq!(
            cb, rb,
            "nextsampleblock probe DIVERGENCE millis={millis:?} seed={seed} nblocks={nblocks}"
        );
    }
}

// ---------------------------------------------------------------------------
// Arms 1/2: full scan walk (and rescan) under a pinned clock sequence.
// ---------------------------------------------------------------------------

/// One scan: draw blocks until either side says done (asserting equality at
/// every step), probing tuples within each returned block. The pinned clock
/// starts at `*clock_ns` and advances by fuzz-derived increments between
/// draws; the final reading is left in `*clock_ns` (monotonic across a
/// rescan).
fn walk_scan(sampler: &mut SystemTimeSampler, rd: &mut Rd<'_>, nblocks0: u32, clock_ns: &mut u64) {
    let mut pinned_nblocks: Option<u32> = None;
    for _ in 0..48 {
        // Per-draw clock increment (<<16 ns => up to ~4295 ms per draw).
        let dt = (rd.u16() as u64) << 16;
        // Vary nblocks per draw within +/-8 of the scan's first value, >= 1
        // once the sampler holds a nonzero nblocks (see module header).
        let delta = (rd.u8() % 17) as i64 - 8;
        let maxoffset = rd.u8() as u16;
        let nblocks = match pinned_nblocks {
            None => nblocks0,
            Some(first) => (first as i64 + delta).max(1) as u32,
        };
        crate::stubs::clock::pin_mono_ns(*clock_ns);
        let cb = unsafe { pg_tsmtime_nextsampleblock(nblocks) };
        let rb = sampler.next_sample_block(nblocks);
        assert_eq!(
            cb, rb,
            "nextsampleblock DIVERGENCE nblocks={nblocks} clock_ns={clock_ns}"
        );
        *clock_ns += dt;
        if rb == INVALID_BLOCK {
            break;
        }
        if pinned_nblocks.is_none() && nblocks != 0 {
            pinned_nblocks = Some(nblocks);
        }
        for _ in 0..300 {
            let ct = unsafe { pg_tsmtime_nextsampletuple(rb, maxoffset) };
            let rt = sampler.next_sample_tuple(maxoffset);
            assert_eq!(
                ct, rt,
                "nextsampletuple DIVERGENCE block={rb} maxoffset={maxoffset}"
            );
            if rt == INVALID_OFFSET {
                break;
            }
        }
    }
}

fn walk_diff(payload: &[u8], rescan: bool) {
    let mut rd = Rd(payload);
    let seed = rd.u32();
    let millis = rd.u32() as f64 / 16.0; // nonnegative ms budget (see header)
    let nblocks0 = rd.u16() as u32;
    let mut clock_ns = (rd.u32() as u64) << 16;

    unsafe { pg_tsmtime_scan_reset() };
    let mut sampler = SystemTimeSampler::default();
    if !begin_both(&mut sampler, millis, seed) {
        return; // unreachable (millis >= 0) but keep the verdict plane armed
    }
    walk_scan(&mut sampler, &mut rd, nblocks0, &mut clock_ns);

    if rescan {
        // Second scan over the SAME sampler: pattern (nblocks/firstblock/
        // step) must be retained even under a different executor seed;
        // start_time re-reads the (still advancing) pinned clock.
        let seed2 = rd.u32();
        let millis2 = rd.u32() as f64 / 16.0;
        if !begin_both(&mut sampler, millis2, seed2) {
            return;
        }
        walk_scan(&mut sampler, &mut rd, nblocks0, &mut clock_ns);
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
    let limit_bits = rd.u64(); // FULL f64 bit domain (guarded in the body)
    // spc_random_page_cost folded to the GUC-legal shape: finite, >= 0
    // (see module header).
    let spc_bits = rd.u64();
    let spc_raw = f64::from_bits(spc_bits);
    let spc = if spc_raw.is_finite() && spc_raw >= 0.0 {
        spc_raw
    } else {
        (spc_bits % 68_719_476_737) as f64 / 16384.0
    };
    let pages = rd.u32();
    let tuples = f64::from_bits(rd.u64()); // FULL f64 bit domain

    let mut c_pages: u32 = 0;
    let mut c_tuples: f64 = 0.0;
    unsafe {
        pg_tsmtime_getsamplesize(
            has_const as i32,
            isnull as i32,
            limit_bits,
            spc,
            pages,
            tuples,
            &mut c_pages,
            &mut c_tuples,
        );
    }
    let limit_opt = if has_const && !isnull {
        Some(f64::from_bits(limit_bits))
    } else {
        None
    };
    let (r_pages, r_tuples) = sample_scan_get_sample_size(limit_opt, spc, pages, tuples);
    assert!(
        c_pages == r_pages && c_tuples.to_bits() == r_tuples.to_bits(),
        "getsamplesize DIVERGENCE limit={limit_opt:?} spc={spc} pages={pages} tuples={tuples}: \
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
    let cr = unsafe { pg_tsmtime_random_relative_prime(n, seed, &mut c_post) };

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
    INIT.call_once(tsm_system_time::init_seams);
    let f = dfmgr::load_external_function("tsm_system_time", HANDLER_SYMBOL, true)
        .expect("registered handler must resolve")
        .expect("registered handler must be Some");
    let cx = mcx::MemoryContext::new("tsmtime_fc");
    let m = cx.mcx();
    let mut fcinfo = LocalFcinfo::<1>::new(0);
    // SAFETY: `cx` outlives this single call.
    unsafe { fcinfo.set_result_mcx(m) };
    fcinfo.args[0] = NullableDatum::value(Datum::from_usize(0));
    let r = f(None, &mut fcinfo);
    assert!(
        r.is_err(),
        "fc_tsm_system_time_handler must refuse direct calls"
    );
    // An unknown symbol misses exactly like C's lookup miss.
    assert!(
        dfmgr::load_external_function("tsm_system_time", "no_such_symbol", false)
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
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/tsm_system_time_diff");
        let mut n = 0;
        for e in std::fs::read_dir(dir).expect("corpus/tsm_system_time_diff missing") {
            let p = e.unwrap().path();
            if p.is_file() && p.file_name().is_some_and(|f| f != ".gitkeep") {
                tsm_system_time_diff(&std::fs::read(&p).unwrap());
                n += 1;
            }
        }
        assert!(n >= 30, "expected >=30 seeds, found {n}");
    }

    /// Per-arm smoke: ok + error shapes per arm.
    #[test]
    fn arms_smoke() {
        // Arm 0: ok begin + probe (millis 5.0, seed 1, nblocks 7).
        let mut v = vec![0u8];
        v.extend(5.0f64.to_bits().to_le_bytes());
        v.extend(1u32.to_le_bytes());
        v.extend(7u16.to_le_bytes());
        v.extend(100u32.to_le_bytes());
        tsm_system_time_diff(&v);
        // Arm 0: negative millis -> 2202H both sides; NaN too.
        for bad in [-3.0f64, f64::NAN] {
            let mut v = vec![0u8];
            v.extend(bad.to_bits().to_le_bytes());
            v.extend(1u32.to_le_bytes());
            v.extend(7u16.to_le_bytes());
            v.extend(0u32.to_le_bytes());
            tsm_system_time_diff(&v);
        }
        // Arm 1: walk with a generous budget (seed 42, millis-q 16000 =
        // 1000ms, nblocks 13) and with a zero budget (immediate expiry).
        for q in [16000u32, 0] {
            let mut v = vec![1u8];
            v.extend(42u32.to_le_bytes());
            v.extend(q.to_le_bytes());
            v.extend(13u16.to_le_bytes());
            v.extend(5u32.to_le_bytes());
            v.extend([0x55u8; 64]);
            tsm_system_time_diff(&v);
        }
        // Arm 1: empty relation (nblocks0 = 0).
        let mut v = vec![1u8];
        v.extend(42u32.to_le_bytes());
        v.extend(16000u32.to_le_bytes());
        v.extend(0u16.to_le_bytes());
        v.extend(5u32.to_le_bytes());
        tsm_system_time_diff(&v);
        // Arm 2: rescan with a different executor seed + budget.
        let mut v = vec![2u8];
        v.extend(7u32.to_le_bytes());
        v.extend(16000u32.to_le_bytes());
        v.extend(11u16.to_le_bytes());
        v.extend(9u32.to_le_bytes());
        v.extend([0x20u8; 32]);
        v.extend(999u32.to_le_bytes());
        v.extend(8000u32.to_le_bytes());
        v.extend([0x10u8; 32]);
        tsm_system_time_diff(&v);
        // Arm 3: Const limit / non-Const / null-Const.
        for flags in [1u8, 0, 3] {
            let mut v = vec![3u8, flags];
            v.extend(500.0f64.to_bits().to_le_bytes());
            v.extend(4.0f64.to_bits().to_le_bytes());
            v.extend(64u32.to_le_bytes());
            v.extend(1000.0f64.to_bits().to_le_bytes());
            tsm_system_time_diff(&v);
        }
        // Arm 4: n = 0, 1, small, large.
        for n in [0u32, 1, 12, 97, 0xFFFF_FFFF] {
            let mut v = vec![4u8];
            v.extend(n.to_le_bytes());
            v.extend(0xDEADBEEFu64.to_le_bytes());
            tsm_system_time_diff(&v);
        }
        // Arm 5: registry plumbing.
        tsm_system_time_diff(&[5u8]);
    }

    /// stub:clock monotonic-half MUST-FAIL CONTROL (STUBS.md law): (a)
    /// parity through the REAL verbatim consumer under matched pins, then
    /// (b) a deliberate ONE-SIDED clock advance that the block-sequence
    /// plane MUST see as a divergence (the C side keeps the old reading,
    /// the Rust side jumps past the time budget).
    #[test]
    fn control_clock_mono_pin() {
        let _oracle = crate::c_oracle_serial();
        // (a) matched pins: 1ms budget; draw 1 in-budget, draw 2 expires
        // BOTH sides after a matched 2ms advance.
        unsafe { pg_tsmtime_scan_reset() };
        let mut sampler = SystemTimeSampler::default();
        assert!(begin_both(&mut sampler, 1.0, 7));
        crate::stubs::clock::pin_mono_ns(1_000);
        let cb = unsafe { pg_tsmtime_nextsampleblock(13) };
        let rb = sampler.next_sample_block(13);
        assert_eq!(cb, rb);
        assert_ne!(rb, INVALID_BLOCK);
        crate::stubs::clock::pin_mono_ns(1_000 + 2_000_000); // +2ms >= 1ms
        let cb = unsafe { pg_tsmtime_nextsampleblock(13) };
        let rb = sampler.next_sample_block(13);
        assert_eq!(cb, rb);
        assert_eq!(rb, INVALID_BLOCK, "matched 2ms advance must expire BOTH");

        // (b) one-sided pin: advance ONLY the Rust cell; C still reads the
        // old value => Rust expires, C returns a block. The comparator MUST
        // flag it — a control that cannot fail is a dead plane.
        unsafe { pg_tsmtime_scan_reset() };
        let mut sampler = SystemTimeSampler::default();
        assert!(begin_both(&mut sampler, 1.0, 7));
        crate::stubs::clock::pin_mono_ns(1_000);
        let cb = unsafe { pg_tsmtime_nextsampleblock(13) };
        let rb = sampler.next_sample_block(13);
        assert_eq!(cb, rb);
        pg_clock::fuzz_mono_pin::set(1_000 + 2_000_000); // Rust side ONLY
        let mut sampler2 = sampler;
        let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let cb = unsafe { pg_tsmtime_nextsampleblock(13) };
            let rb = sampler2.next_sample_block(13);
            assert_eq!(cb, rb, "one-sided clock advance");
        }));
        assert!(
            caught.is_err(),
            "mismatched mono pins MUST be visible (clock plane dead?)"
        );
        crate::stubs::clock::pin_mono_ns(0); // re-align for later tests
    }
}
