//! tsrank_diff: differential fuzz driver — shipped Rust `adt_tsrank` vs vendored
//! PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) C
//! (csrc/pg_tsrank_io.c). Crate under test: crates/backend/utils/adt/tsrank.
//!
//! GENERATED SKELETON (fuzz/scaffold.py) — every TODO(scaffold) below is
//! hand-work; see fuzz/README-TODO-tsrank_diff.md for the ordered checklist.
//!
//! Comparison planes (float_in_diff conventions): value bytes/bits,
//! error-verdict, and errcode/sqlstate class. Message text is out of scope.
//!
//! Input layout: [selector][payload]; selector % 8 picks the arm:
//!   0 ts_rank_wttf  (oid 3703, C: tsrank.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   1 ts_rank_wtt  (oid 3704, C: tsrank.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   2 ts_rank_ttf  (oid 3705, C: tsrank.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   3 ts_rank_tt  (oid 3706, C: tsrank.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   4 ts_rankcd_wttf  (oid 3707, C: tsrank.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   5 ts_rankcd_wtt  (oid 3708, C: tsrank.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   6 ts_rankcd_ttf  (oid 3709, C: tsrank.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   7 ts_rankcd_tt  (oid 3710, C: tsrank.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!
//! FC-WRAPPER PLANE: each arm additionally routes its (already core-vs-C
//! checked) input through the crate's builtins.rs fc_* wrapper via a native
//! types_fmgr::LocalFcinfo frame and asserts wrapper == core (Datum value /
//! returned bytes / error verdict + sqlstate). C-parity keeps being carried
//! by the core comparison; the plane makes the wrapper lines execute every
//! iteration with an in-harness oracle.
//!
//! SKIPPED: TODO(scaffold) — record here every excluded row (stateful /
//! PRNG / clock / locale carve-outs) and WHY, per the fuzzuproof-crate
//! skill's exception rules.

// Scaffold state: helpers below are exercised only once the arms are
// implemented. Remove this allow together with the last todo!().
#![allow(dead_code)]

use datum::{Datum, NullableDatum};
use stringinfo::StringInfo;
use types_error::PgResult;
use types_fmgr::{LocalFcinfo, PGFunction};

extern "C" {
    // Shared TLS errcode accessor (defined in csrc/pg_float_io.c).
    fn pg_diff_errcode_get() -> i32;
    // TODO(scaffold): declare the pg_diff_* oracle entries as you write them
    // in csrc/pg_tsrank_io.c (declarations are link-inert until called, so
    // `cargo check` and `cargo test` stay green while sites are unfilled):
    // TODO(scaffold): fn pg_diff_ts_rank_wttf(...) -> i32;   [oid 3703, tsrank.c]
    // TODO(scaffold): fn pg_diff_ts_rank_wtt(...) -> i32;   [oid 3704, tsrank.c]
    // TODO(scaffold): fn pg_diff_ts_rank_ttf(...) -> i32;   [oid 3705, tsrank.c]
    // TODO(scaffold): fn pg_diff_ts_rank_tt(...) -> i32;   [oid 3706, tsrank.c]
    // TODO(scaffold): fn pg_diff_ts_rankcd_wttf(...) -> i32;   [oid 3707, tsrank.c]
    // TODO(scaffold): fn pg_diff_ts_rankcd_wtt(...) -> i32;   [oid 3708, tsrank.c]
    // TODO(scaffold): fn pg_diff_ts_rankcd_ttf(...) -> i32;   [oid 3709, tsrank.c]
    // TODO(scaffold): fn pg_diff_ts_rankcd_tt(...) -> i32;   [oid 3710, tsrank.c]
}

// ---------------------------------------------------------------------------
// fc-wrapper plane plumbing (native LocalFcinfo, real mcx — the proofs
// wrapper-level pattern run without kani; verbatim from uuid_diff.rs).
// ---------------------------------------------------------------------------

/// Invoke an fc_* wrapper over non-null args; returns (result, isnull flag).
fn fc_call<const N: usize>(
    f: PGFunction,
    m: mcx::Mcx<'_>,
    args: [Datum; N],
) -> (PgResult<Datum>, bool) {
    let mut fcinfo = LocalFcinfo::<N>::new(0);
    // SAFETY: the context owning `m` outlives this single call (caller scope).
    unsafe { fcinfo.set_result_mcx(m) };
    for (i, a) in args.into_iter().enumerate() {
        fcinfo.args[i] = NullableDatum::value(a);
    }
    let r = f(None, &mut fcinfo);
    (r, fcinfo.isnull)
}

/// First `n` bytes behind a by-ref result Datum. Caller contract: `d` came
/// from a wrapper that returned an `n`-byte-or-longer allocation still live
/// in the arming context (or thread-local out scratch).
fn datum_bytes<'a>(d: Datum, n: usize) -> &'a [u8] {
    // SAFETY: caller contract above.
    unsafe { core::slice::from_raw_parts(d.as_usize() as *const u8, n) }
}

/// A StringInfo image over `bytes` in `m` (None = alloc failure: skip plane).
fn make_si<'a>(m: mcx::Mcx<'a>, bytes: &[u8]) -> Option<StringInfo<'a>> {
    let mut vec = mcx::vec_with_capacity_in::<u8>(m, bytes.len()).ok()?;
    mcx::vec_append_bytes(&mut vec, bytes).ok()?;
    StringInfo::from_vec(vec).ok()
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

pub fn tsrank_diff(data: &[u8]) {
    let Some((&sel, payload)) = data.split_first() else {
        return;
    };
    match sel % 8 {
        0 => ts_rank_wttf_diff(payload),
        1 => ts_rank_wtt_diff(payload),
        2 => ts_rank_ttf_diff(payload),
        3 => ts_rank_tt_diff(payload),
        4 => ts_rankcd_wttf_diff(payload),
        5 => ts_rankcd_wtt_diff(payload),
        6 => ts_rankcd_ttf_diff(payload),
        _ => ts_rankcd_tt_diff(payload),
    }
}

// ---------------------------------------------------------------------------
// Arm: ts_rank_wttf (oid 3703; C source: tsrank.c).
// ---------------------------------------------------------------------------

fn ts_rank_wttf_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (tsrank_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_tsrank_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_ts_rank_wttf(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_tsrank::ts_rank_wttf(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_tsrank::builtins::fc_ts_rank_wttf via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(tsrank_diff): ts_rank_wttf arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: ts_rank_wtt (oid 3704; C source: tsrank.c).
// ---------------------------------------------------------------------------

fn ts_rank_wtt_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (tsrank_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_tsrank_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_ts_rank_wtt(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_tsrank::ts_rank_wtt(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_tsrank::builtins::fc_ts_rank_wtt via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(tsrank_diff): ts_rank_wtt arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: ts_rank_ttf (oid 3705; C source: tsrank.c).
// ---------------------------------------------------------------------------

fn ts_rank_ttf_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (tsrank_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_tsrank_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_ts_rank_ttf(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_tsrank::ts_rank_ttf(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_tsrank::builtins::fc_ts_rank_ttf via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(tsrank_diff): ts_rank_ttf arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: ts_rank_tt (oid 3706; C source: tsrank.c).
// ---------------------------------------------------------------------------

fn ts_rank_tt_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (tsrank_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_tsrank_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_ts_rank_tt(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_tsrank::ts_rank_tt(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_tsrank::builtins::fc_ts_rank_tt via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(tsrank_diff): ts_rank_tt arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: ts_rankcd_wttf (oid 3707; C source: tsrank.c).
// ---------------------------------------------------------------------------

fn ts_rankcd_wttf_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (tsrank_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_tsrank_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_ts_rankcd_wttf(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_tsrank::ts_rankcd_wttf(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_tsrank::builtins::fc_ts_rankcd_wttf via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(tsrank_diff): ts_rankcd_wttf arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: ts_rankcd_wtt (oid 3708; C source: tsrank.c).
// ---------------------------------------------------------------------------

fn ts_rankcd_wtt_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (tsrank_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_tsrank_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_ts_rankcd_wtt(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_tsrank::ts_rankcd_wtt(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_tsrank::builtins::fc_ts_rankcd_wtt via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(tsrank_diff): ts_rankcd_wtt arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: ts_rankcd_ttf (oid 3709; C source: tsrank.c).
// ---------------------------------------------------------------------------

fn ts_rankcd_ttf_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (tsrank_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_tsrank_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_ts_rankcd_ttf(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_tsrank::ts_rankcd_ttf(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_tsrank::builtins::fc_ts_rankcd_ttf via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(tsrank_diff): ts_rankcd_ttf arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: ts_rankcd_tt (oid 3710; C source: tsrank.c).
// ---------------------------------------------------------------------------

fn ts_rankcd_tt_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (tsrank_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_tsrank_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_ts_rankcd_tt(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_tsrank::ts_rankcd_tt(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_tsrank::builtins::fc_ts_rankcd_tt via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(tsrank_diff): ts_rankcd_tt arm not implemented");
}

// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Replay every checked-in seed (catches shim/link errors before the
    /// nightly fuzz campaign). TODO(scaffold): un-ignore once the arms are
    /// implemented and ../corpus/tsrank_diff/ is seeded (>=30 seeds; corpora
    /// are COMMITTED — plain `git add`, no -f needed).
    #[test]
    #[ignore = "scaffold(tsrank_diff): arms not implemented yet"]
    fn seed_corpus_replays_clean() {
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/tsrank_diff");
        let mut n = 0;
        for e in std::fs::read_dir(dir).expect("corpus/tsrank_diff missing") {
            let p = e.unwrap().path();
            if p.is_file() {
                tsrank_diff(&std::fs::read(&p).unwrap());
                n += 1;
            }
        }
        assert!(n >= 30, "expected >=30 seeds, found {n}");
    }

    /// TODO(scaffold): per-arm smoke tests on stable (ok + error shapes per
    /// arm, fc-plane smoke driving every wrapper at least once — see
    /// uuid_diff.rs tests for the expected shape). Start by un-ignoring:
    #[test]
    #[ignore = "scaffold(tsrank_diff): arms not implemented yet"]
    fn arms_smoke() {
        // Arm 0 example: selector byte 0, then a payload for ts_rank_wttf.
        tsrank_diff(&[0u8]);
    }
}
