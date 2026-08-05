//! tablesample_diff: differential fuzz driver — shipped Rust
//! `tablesample` (backend/access/tablesample: BERNOULLI + SYSTEM built-in
//! TSMs) vs vendored PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) C
//! (csrc/pg_tablesample_io.c, assembled by csrc/gen/assemble_tsmpl.sh).
//! Crate under test: crates/backend/access/tablesample/tablesample.
//!
//! Comparison planes: value (every returned BlockNumber / OffsetNumber,
//! the (use_bulkread, use_pagemode) flag pair), error-verdict, and
//! errcode/sqlstate class (2202H <-> C class 1). Message text out of scope.
//!
//! stub:prng (census stub_req): BERNOULLI/SYSTEM consume no prng at all
//! (pure hash_bytes sweeps over (block, offset, seed)); the seed is an
//! explicit compared input passed identically to both sides. The dispatch
//! arm's SystemRows/SystemTime legs pin `stubs::prng::pin_seed` (and
//! `stubs::clock::pin_mono_ns` for SystemTime) per the STUBS.md contract,
//! exactly as the sibling tsm_system_rows_diff / tsm_system_time_diff
//! targets do.
//!
//! Input layout: [selector][payload]; selector % 5 picks the arm:
//!   0 beginsamplescan: [m u8][pctbits u32][seed u32][probe u16][maxoff u8]
//!     — m&1 picks Bernoulli/System; pctbits = FULL f32 bit domain (NaN,
//!     +/-inf, negatives, denormals, >100). Verdict + sqlstate planes +
//!     the compared (use_bulkread, use_pagemode) pair, plus one post-begin
//!     probe (System: nextsampleblock; Bernoulli: one nextsampletuple).
//!   1 system scan walk: [pctbits u32][seed u32][per-draw (nblocks u16,
//!     maxoffset u8)...] — full block sequence compared draw-by-draw with
//!     per-draw varying nblocks (the executor re-reads rs_nblocks), tuple
//!     offsets compared within each returned block, donetuples maintained
//!     executor-style on the Rust side (both methods ignore it; C entries
//!     don't take it — bernoulli.c/system.c never read donetuples). The
//!     walk continues through ONE InvalidBlockNumber (exercising C's
//!     "reset nextblock to 0 for safety" then a fresh sweep) and stops at
//!     the second.
//!   2 bernoulli tuple walk: [pctbits u32][seed u32][per-block (blockno
//!     u32, maxoffset u8)...] — up to 16 blocks; within each, the hash
//!     sweep is compared offset-by-offset until InvalidOffsetNumber.
//!   3 rescan: [m u8][pct1 u32][seed1 u32][walk bytes][pct2 u32][seed2
//!     u32][walk bytes] — two begin_sample_scan calls on ONE TsmState
//!     (lt/nextblock reinit path); the second begin may error on BOTH
//!     sides (both C and Rust validate percent before touching sampler
//!     state), after which the walk continues on the retained first-scan
//!     state.
//!   4 registry/dispatch: (a) Rust-only vtable-literal plane — Tsm::
//!     from_handler / parameter_types / repeatable_across_* /
//!     has_next_sample_block / init_state asserted against the literal
//!     values of the NOT-vendored C handlers (bernoulli.c tsm_bernoulli_
//!     handler lines 61-80, system.c tsm_system_handler lines 63-82,
//!     contrib handler vtables; fmgr/makeNode plumbing with no
//!     computation — tsmrows precedent); (b) TsmState::SystemRows
//!     dispatch legs diffed against the pg_tsmrows_* oracle
//!     (csrc/pg_tsm_system_rows_io.c); (c) TsmState::SystemTime dispatch
//!     legs diffed against the pg_tsmtime_* oracle under a pinned
//!     monotonic clock (csrc/pg_tsm_system_time_io.c).
//!
//! Fuzz-domain bounds (documented, all on COMPARED-EQUAL derived inputs):
//!   - maxoffset <= 255 (u8): the executor's maxoffset is
//!     MaxHeapTuplesPerPage-bounded (291 @ 8K); an unbounded u16 sweep at
//!     cutoff 0 with maxoffset 65535 would wrap OffsetNumber in C
//!     (uint16 tupoffset++ never exceeds 65535 -> infinite loop) and
//!     overflow-panic in Rust — unreachable via the executor.
//!   - nblocks <= 65535 (u16) per draw; <= 48 block draws, <= 300 tuple
//!     probes per block, <= 16 bernoulli blocks (throughput bounds).
//!   - arm 3's per-scan walk is shortened (<= 8 draws) to keep rescan
//!     execs cheap.
//!
//! SKIPPED (census OUT, exception rows in phase1-exceptions.tsv):
//!   - Tsm::get / Tsm::from_symbol / not_a_tsm_routine (lib.rs 41-62,
//!     341-349): the GetTsmRoutine syscache/fmgr seam — unit-test
//!     witnessed (tests::registry_dispatch, tests::unknown_handler_is_
//!     clean_error).
//!   - Tsm::sample_scan_get_sample_size + extract_fraction + clamp_row_est
//!     (lib.rs 86-167): planner fold (estimate_expression_value) — unit-
//!     test witnessed.
//!   - TsmState::next_sample_block's Bernoulli panic arm (lib.rs 230-232):
//!     defensive guard for a tsmapi.h contract C encodes as
//!     NextSampleBlock == NULL (the executor never calls it when
//!     has_next_sample_block() is false) — unit-test witnessed
//!     (#[should_panic]).

use datum::Datum;
use tablesample::{Tsm, TsmState, F_TSM_BERNOULLI_HANDLER, F_TSM_SYSTEM_HANDLER};
use tableam_vocab::SampleScanDriver;
use types_core::catalog::{FLOAT4OID, FLOAT8OID, INT8OID};
use types_error::ERRCODE_INVALID_TABLESAMPLE_ARGUMENT;

/// Oracle errcode class for 2202H (csrc/pg_tablesample_io.c; the tsmrows /
/// tsmtime oracles use the same class value).
const C_ERRCLASS_TABLESAMPLE: i32 = 1;

const INVALID_BLOCK: u32 = 0xFFFF_FFFF;
const INVALID_OFFSET: u16 = 0;

extern "C" {
    // Shared TLS errcode accessor (defined in csrc/pg_float_io.c).
    fn pg_diff_errcode_get() -> i32;
    // Oracle entries (csrc/pg_tablesample_io.c section 14).
    fn pg_tsmpl_scan_reset(method: i32);
    fn pg_tsmpl_beginsamplescan(
        method: i32,
        pctbits: u32,
        seed: u32,
        out_bulkread: *mut i32,
        out_pagemode: *mut i32,
    ) -> i32;
    fn pg_tsmpl_nextsampleblock(nblocks: u32) -> u32;
    fn pg_tsmpl_nextsampletuple(method: i32, blockno: u32, maxoffset: u16) -> u16;
    // Sibling oracles for the dispatch arm (csrc/pg_tsm_system_rows_io.c,
    // csrc/pg_tsm_system_time_io.c).
    fn pg_tsmrows_scan_reset();
    fn pg_tsmrows_beginsamplescan(ntuples: i64, seed: u32) -> i32;
    fn pg_tsmrows_nextsampleblock(nblocks: u32, donetuples: i64) -> u32;
    fn pg_tsmrows_nextsampletuple(blockno: u32, maxoffset: u16, donetuples: i64) -> u16;
    fn pg_tsmtime_scan_reset();
    fn pg_tsmtime_beginsamplescan(millis_bits: u64, seed: u32) -> i32;
    fn pg_tsmtime_nextsampleblock(nblocks: u32) -> u32;
    fn pg_tsmtime_nextsampletuple(blockno: u32, maxoffset: u16) -> u16;
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
    fn i64(&mut self) -> i64 {
        i64::from_le_bytes(self.bytes::<8>())
    }
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

pub fn tablesample_diff(data: &[u8]) {
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    let Some((&sel, payload)) = data.split_first() else {
        return;
    };
    match sel % 5 {
        0 => begin_diff(payload),
        1 => system_walk_diff(payload),
        2 => bernoulli_walk_diff(payload),
        3 => rescan_diff(payload),
        _ => dispatch_diff(payload),
    }
}

// ---------------------------------------------------------------------------
// Shared: begin on both sides (C node already reset for `method`), compare
// verdict + sqlstate + the (use_bulkread, use_pagemode) pair. Returns true
// when both sides accepted.
// ---------------------------------------------------------------------------

fn begin_both(st: &mut TsmState, method: i32, pctbits: u32, seed: u32) -> bool {
    let pct = f32::from_bits(pctbits);
    let mut c_bulkread: i32 = -1;
    let mut c_pagemode: i32 = -1;
    let cst = unsafe {
        pg_tsmpl_beginsamplescan(method, pctbits, seed, &mut c_bulkread, &mut c_pagemode)
    };
    let cerr = unsafe { pg_diff_errcode_get() };
    match st.begin_sample_scan(&[Datum::from_f32(pct)], seed) {
        Ok((r_bulkread, r_pagemode)) => {
            assert!(
                cst == 0,
                "beginsamplescan verdict DIVERGENCE method={method} pct={pct:?} seed={seed}: \
                 C=err({cerr}) Rust=Ok"
            );
            assert!(
                c_bulkread == r_bulkread as i32 && c_pagemode == r_pagemode as i32,
                "beginsamplescan flags DIVERGENCE method={method} pct={pct:?}: \
                 C=(bulkread {c_bulkread}, pagemode {c_pagemode}) \
                 Rust=({r_bulkread}, {r_pagemode})"
            );
            true
        }
        Err(e) => {
            assert!(
                cst == 1
                    && cerr == C_ERRCLASS_TABLESAMPLE
                    && e.sqlstate == ERRCODE_INVALID_TABLESAMPLE_ARGUMENT,
                "beginsamplescan error DIVERGENCE method={method} pct={pct:?} seed={seed}: \
                 C=(st {cst}, err {cerr}) Rust=Err(sqlstate {:?})",
                e.sqlstate
            );
            false
        }
    }
}

fn method_tsm(method: i32) -> Tsm {
    if method == 0 { Tsm::Bernoulli } else { Tsm::System }
}

// ---------------------------------------------------------------------------
// Arm 0: beginsamplescan verdict/sqlstate/flags (FULL f32 pct domain) + one
// post-begin probe.
// ---------------------------------------------------------------------------

fn begin_diff(payload: &[u8]) {
    let mut rd = Rd(payload);
    let method = (rd.u8() & 1) as i32;
    let pctbits = rd.u32(); // FULL f32 bit domain incl. NaN/inf/-x/>100
    let seed = rd.u32();
    let probe = rd.u16() as u32;
    let maxoffset = rd.u8() as u16;

    unsafe { pg_tsmpl_scan_reset(method) };
    let mut st = method_tsm(method).init_state();
    if begin_both(&mut st, method, pctbits, seed) {
        if method == 0 {
            // Bernoulli: one tuple-sweep probe (blockno = probe).
            let ct = unsafe { pg_tsmpl_nextsampletuple(0, probe, maxoffset) };
            let rt = st.next_sample_tuple(probe, maxoffset, 0);
            assert_eq!(
                ct, rt,
                "bernoulli nextsampletuple probe DIVERGENCE pctbits={pctbits:#x} seed={seed} \
                 blockno={probe} maxoffset={maxoffset}"
            );
        } else {
            // System: one block probe.
            let cb = unsafe { pg_tsmpl_nextsampleblock(probe) };
            let rb = st.next_sample_block(probe, 0);
            assert_eq!(
                cb, rb,
                "system nextsampleblock probe DIVERGENCE pctbits={pctbits:#x} seed={seed} \
                 nblocks={probe}"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Arm 1: SYSTEM scan walk — block sequence + within-block tuple offsets,
// driven through the SampleScanDriver trait (the executor's view).
// ---------------------------------------------------------------------------

/// One SYSTEM scan of up to `max_draws` block draws over an already-begun
/// state; per-draw nblocks from the payload. Continues through one
/// InvalidBlockNumber (C resets nextblock to 0), stops at the second.
fn system_walk(st: &mut TsmState, rd: &mut Rd<'_>, max_draws: u32) {
    assert!(
        SampleScanDriver::has_next_sample_block(st),
        "SYSTEM must report a NextSampleBlock (system.c vtable)"
    );
    let mut donetuples: i64 = 0;
    let mut exhausted = 0;
    for _ in 0..max_draws {
        let nblocks = rd.u16() as u32;
        let maxoffset = rd.u8() as u16;
        let cb = unsafe { pg_tsmpl_nextsampleblock(nblocks) };
        let rb = SampleScanDriver::next_sample_block(st, nblocks, donetuples);
        assert_eq!(
            cb, rb,
            "system nextsampleblock DIVERGENCE nblocks={nblocks} donetuples={donetuples}"
        );
        if rb == INVALID_BLOCK {
            exhausted += 1;
            if exhausted >= 2 {
                break;
            }
            continue; // C reset nextblock to 0; keep walking (fresh sweep)
        }
        for _ in 0..300 {
            let ct = unsafe { pg_tsmpl_nextsampletuple(1, rb, maxoffset) };
            let rt = SampleScanDriver::next_sample_tuple(st, rb, maxoffset, donetuples);
            assert_eq!(
                ct, rt,
                "system nextsampletuple DIVERGENCE block={rb} maxoffset={maxoffset}"
            );
            if rt == INVALID_OFFSET {
                break;
            }
            donetuples += 1; // executor: one tuple returned
        }
    }
}

fn system_walk_diff(payload: &[u8]) {
    let mut rd = Rd(payload);
    let pctbits = rd.u32();
    let seed = rd.u32();

    unsafe { pg_tsmpl_scan_reset(1) };
    let mut st = Tsm::System.init_state();
    if !begin_both(&mut st, 1, pctbits, seed) {
        return; // error verdicts already compared
    }
    system_walk(&mut st, &mut rd, 48);
}

// ---------------------------------------------------------------------------
// Arm 2: BERNOULLI tuple walk — the per-TID hash sweep, block by block.
// ---------------------------------------------------------------------------

/// Up to `max_blocks` blocks of an already-begun BERNOULLI state; within
/// each block the sweep is compared offset-by-offset until Invalid.
fn bernoulli_walk(st: &mut TsmState, rd: &mut Rd<'_>, max_blocks: u32) {
    assert!(
        !SampleScanDriver::has_next_sample_block(st),
        "BERNOULLI must have no NextSampleBlock (bernoulli.c vtable: NULL)"
    );
    let mut donetuples: i64 = 0;
    for _ in 0..max_blocks {
        let blockno = rd.u32();
        let maxoffset = rd.u8() as u16;
        for _ in 0..300 {
            let ct = unsafe { pg_tsmpl_nextsampletuple(0, blockno, maxoffset) };
            let rt = SampleScanDriver::next_sample_tuple(st, blockno, maxoffset, donetuples);
            assert_eq!(
                ct, rt,
                "bernoulli nextsampletuple DIVERGENCE block={blockno} maxoffset={maxoffset}"
            );
            if rt == INVALID_OFFSET {
                break;
            }
            donetuples += 1;
        }
    }
}

fn bernoulli_walk_diff(payload: &[u8]) {
    let mut rd = Rd(payload);
    let pctbits = rd.u32();
    let seed = rd.u32();

    unsafe { pg_tsmpl_scan_reset(0) };
    let mut st = Tsm::Bernoulli.init_state();
    if !begin_both(&mut st, 0, pctbits, seed) {
        return;
    }
    bernoulli_walk(&mut st, &mut rd, 16);
}

// ---------------------------------------------------------------------------
// Arm 3: rescan — two begins on ONE state (lt/nextblock reinit); a failed
// second begin leaves the first scan's state intact on BOTH sides (both
// validate percent before mutating), and the walk continues on it.
// ---------------------------------------------------------------------------

fn rescan_diff(payload: &[u8]) {
    let mut rd = Rd(payload);
    let method = (rd.u8() & 1) as i32;
    let pct1 = rd.u32();
    let seed1 = rd.u32();

    unsafe { pg_tsmpl_scan_reset(method) };
    let mut st = method_tsm(method).init_state();
    if !begin_both(&mut st, method, pct1, seed1) {
        return;
    }
    if method == 0 {
        bernoulli_walk(&mut st, &mut rd, 4);
    } else {
        system_walk(&mut st, &mut rd, 8);
    }

    // Re-begin the SAME state (executor rescan); pct2 may be invalid — the
    // verdict is compared inside begin_both, and either way the subsequent
    // walk compares the retained-or-reinitialized state.
    let pct2 = rd.u32();
    let seed2 = rd.u32();
    begin_both(&mut st, method, pct2, seed2);
    if method == 0 {
        bernoulli_walk(&mut st, &mut rd, 4);
    } else {
        system_walk(&mut st, &mut rd, 8);
    }
}

// ---------------------------------------------------------------------------
// Arm 4: registry/dispatch — vtable-literal plane (Rust-only) + the
// TsmState::SystemRows / TsmState::SystemTime delegation legs diffed
// against the sibling verbatim oracles.
// ---------------------------------------------------------------------------

fn dispatch_diff(payload: &[u8]) {
    let mut rd = Rd(payload);
    let b = rd.u8();

    // (a) Vtable-literal plane. Expected values are the literal fields of
    // the NOT-vendored C handler bodies: bernoulli.c lines 61-80
    // (parameterTypes FLOAT4OID, repeatable true/true, NextSampleBlock
    // NULL), system.c lines 63-82 (FLOAT4OID, true/true, NextSampleBlock
    // set), tsm_system_rows.c lines 77-99 (INT8OID, false/true, set),
    // tsm_system_time.c (FLOAT8OID, false/false, set).
    tablesample::init_seams();
    let oid = rd.u32();
    let expected = match oid {
        F_TSM_BERNOULLI_HANDLER => Some(Tsm::Bernoulli),
        F_TSM_SYSTEM_HANDLER => Some(Tsm::System),
        _ => None,
    };
    assert_eq!(Tsm::from_handler(oid), expected, "from_handler({oid})");
    for (tsm, ptypes, raq, ras, hnsb) in [
        (Tsm::Bernoulli, &[FLOAT4OID][..], true, true, false),
        (Tsm::System, &[FLOAT4OID][..], true, true, true),
        (Tsm::SystemRows, &[INT8OID][..], false, true, true),
        (Tsm::SystemTime, &[FLOAT8OID][..], false, false, true),
    ] {
        assert_eq!(tsm.parameter_types(), ptypes, "{tsm:?} parameterTypes");
        assert_eq!(tsm.repeatable_across_queries(), raq, "{tsm:?} raq");
        assert_eq!(tsm.repeatable_across_scans(), ras, "{tsm:?} ras");
        assert_eq!(tsm.has_next_sample_block(), hnsb, "{tsm:?} NextSampleBlock");
        assert_eq!(tsm.init_state().has_next_sample_block(), hnsb, "{tsm:?} state hnsb");
    }

    // (b) TsmState::SystemRows delegation vs the pg_tsmrows_* oracle.
    {
        let ntuples = rd.i64(); // full i64 incl. the negative 2202H arm
        let seed = rd.u32();
        crate::stubs::prng::pin_seed(seed as u64);
        unsafe { pg_tsmrows_scan_reset() };
        let mut st = Tsm::SystemRows.init_state();
        let cst = unsafe { pg_tsmrows_beginsamplescan(ntuples, seed) };
        let cerr = unsafe { pg_diff_errcode_get() };
        match st.begin_sample_scan(&[Datum::from_i64(ntuples)], seed) {
            Ok(flags) => {
                assert!(
                    cst == 0 && flags == (true, true),
                    "SystemRows begin DIVERGENCE ntuples={ntuples}: C=err({cerr}) \
                     Rust=Ok({flags:?})"
                );
                let mut donetuples: i64 = 0;
                let nblocks = (rd.u16() as u32).max(1);
                for _ in 0..8 {
                    let cb = unsafe { pg_tsmrows_nextsampleblock(nblocks, donetuples) };
                    let rb = SampleScanDriver::next_sample_block(&mut st, nblocks, donetuples);
                    assert_eq!(cb, rb, "SystemRows dispatch nextsampleblock DIVERGENCE");
                    if rb == INVALID_BLOCK {
                        break;
                    }
                    let maxoffset = rd.u8() as u16;
                    for _ in 0..300 {
                        let ct = unsafe { pg_tsmrows_nextsampletuple(rb, maxoffset, donetuples) };
                        let rt =
                            SampleScanDriver::next_sample_tuple(&mut st, rb, maxoffset, donetuples);
                        assert_eq!(ct, rt, "SystemRows dispatch nextsampletuple DIVERGENCE");
                        if rt == INVALID_OFFSET {
                            break;
                        }
                        donetuples += 1;
                    }
                }
            }
            Err(e) => {
                assert!(
                    cst == 1
                        && cerr == C_ERRCLASS_TABLESAMPLE
                        && e.sqlstate == ERRCODE_INVALID_TABLESAMPLE_ARGUMENT,
                    "SystemRows begin error DIVERGENCE ntuples={ntuples}: C=(st {cst}, \
                     err {cerr}) Rust=Err({:?})",
                    e.sqlstate
                );
            }
        }
    }

    // (c) TsmState::SystemTime delegation vs the pg_tsmtime_* oracle under
    // a pinned monotonic clock (stub:clock, tsm_system_time_diff pattern).
    {
        let seed = rd.u32();
        // Nonnegative ms budget normally; b&2 forces the negative 2202H arm.
        let millis = if b & 2 != 0 { -1.0f64 } else { rd.u32() as f64 / 16.0 };
        let nblocks = rd.u16() as u32;
        let mut clock_ns = (rd.u32() as u64) << 16;
        crate::stubs::prng::pin_seed(seed as u64);
        unsafe { pg_tsmtime_scan_reset() };
        let mut st = Tsm::SystemTime.init_state();
        let cst = unsafe { pg_tsmtime_beginsamplescan(millis.to_bits(), seed) };
        let cerr = unsafe { pg_diff_errcode_get() };
        match st.begin_sample_scan(&[Datum::from_f64(millis)], seed) {
            Ok(flags) => {
                assert!(
                    cst == 0 && flags == (true, true),
                    "SystemTime begin DIVERGENCE millis={millis:?}: C=err({cerr}) \
                     Rust=Ok({flags:?})"
                );
                for _ in 0..4 {
                    crate::stubs::clock::pin_mono_ns(clock_ns);
                    let cb = unsafe { pg_tsmtime_nextsampleblock(nblocks) };
                    let rb = SampleScanDriver::next_sample_block(&mut st, nblocks, 0);
                    assert_eq!(cb, rb, "SystemTime dispatch nextsampleblock DIVERGENCE");
                    clock_ns += (rd.u16() as u64) << 16;
                    if rb == INVALID_BLOCK {
                        break;
                    }
                    let maxoffset = rd.u8() as u16;
                    for _ in 0..300 {
                        let ct = unsafe { pg_tsmtime_nextsampletuple(rb, maxoffset) };
                        let rt = SampleScanDriver::next_sample_tuple(&mut st, rb, maxoffset, 0);
                        assert_eq!(ct, rt, "SystemTime dispatch nextsampletuple DIVERGENCE");
                        if rt == INVALID_OFFSET {
                            break;
                        }
                    }
                }
            }
            Err(e) => {
                assert!(
                    cst == 1
                        && cerr == C_ERRCLASS_TABLESAMPLE
                        && e.sqlstate == ERRCODE_INVALID_TABLESAMPLE_ARGUMENT,
                    "SystemTime begin error DIVERGENCE millis={millis:?}: C=(st {cst}, \
                     err {cerr}) Rust=Err({:?})",
                    e.sqlstate
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Replay every checked-in seed (catches shim/link errors before the
    /// nightly fuzz campaign).
    #[test]
    fn seed_corpus_replays_clean() {
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/tablesample_diff");
        let mut n = 0;
        for e in std::fs::read_dir(dir).expect("corpus/tablesample_diff missing") {
            let p = e.unwrap().path();
            if p.is_file() && p.file_name().is_some_and(|f| f != ".gitkeep") {
                tablesample_diff(&std::fs::read(&p).unwrap());
                n += 1;
            }
        }
        assert!(n >= 30, "expected >=30 seeds, found {n}");
    }

    fn begin_bytes(sel: u8, m: u8, pct: f32, seed: u32) -> Vec<u8> {
        let mut v = vec![sel, m];
        if sel != 0 {
            v.truncate(1); // arms 1/2 have no method byte
        }
        v.extend(pct.to_bits().to_le_bytes());
        v.extend(seed.to_le_bytes());
        v
    }

    /// Per-arm smoke: ok + error shapes per arm.
    #[test]
    fn arms_smoke() {
        // Arm 0: both methods, ok + bogus percents.
        for m in [0u8, 1] {
            for pct in [0.0f32, 0.5, 1.0, 24.9, 25.0, 50.0, 100.0, -0.0, -1.0, 100.001,
                        f32::NAN, f32::INFINITY, f32::NEG_INFINITY] {
                let mut v = begin_bytes(0, m, pct, 42);
                v.extend(13u16.to_le_bytes());
                v.push(20);
                tablesample_diff(&v);
            }
        }
        // Arm 1: system walk, incl. exhaust + restart.
        let mut v = begin_bytes(1, 0, 40.0, 99);
        for _ in 0..12 {
            v.extend(17u16.to_le_bytes());
            v.push(9);
        }
        tablesample_diff(&v);
        // Arm 1: empty relation (nblocks 0).
        let mut v = begin_bytes(1, 0, 40.0, 99);
        v.extend(0u16.to_le_bytes());
        v.push(9);
        tablesample_diff(&v);
        // Arm 2: bernoulli walk over several blocks.
        let mut v = begin_bytes(2, 0, 30.0, 1234);
        for blk in [0u32, 1, 7, 0xFFFF_FFFF] {
            v.extend(blk.to_le_bytes());
            v.push(50);
        }
        tablesample_diff(&v);
        // Arm 3: rescan, both methods, ok->ok and ok->error second begin.
        for m in [0u8, 1] {
            for pct2 in [75.0f32, -3.0] {
                let mut v = vec![3u8, m];
                v.extend(30.0f32.to_bits().to_le_bytes());
                v.extend(7u32.to_le_bytes());
                v.extend([0x33u8; 24]);
                v.extend(pct2.to_bits().to_le_bytes());
                v.extend(8u32.to_le_bytes());
                v.extend([0x44u8; 24]);
                tablesample_diff(&v);
            }
        }
        // Arm 4: dispatch (ok + forced SystemTime error via b&2), and the
        // SystemRows negative-ntuples error.
        let mut v = vec![4u8, 0u8];
        v.extend(F_TSM_BERNOULLI_HANDLER.to_le_bytes());
        v.extend(100i64.to_le_bytes());
        v.extend(11u32.to_le_bytes());
        v.extend([0x21u8; 32]);
        tablesample_diff(&v);
        let mut v = vec![4u8, 2u8];
        v.extend(9999u32.to_le_bytes());
        v.extend((-5i64).to_le_bytes());
        v.extend(11u32.to_le_bytes());
        v.extend([0x21u8; 32]);
        tablesample_diff(&v);
    }
}
