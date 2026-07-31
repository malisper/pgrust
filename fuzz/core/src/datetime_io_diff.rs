//! datetime_io_diff: differential fuzz driver — shipped Rust `adt_date` vs vendored
//! PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) C
//! (csrc/pg_datetime_io_io.c). Crate under test: crates/backend/utils/adt/adt_date.
//!
//! GENERATED SKELETON (fuzz/scaffold.py) — every TODO(scaffold) below is
//! hand-work; see fuzz/README-TODO-datetime_io_diff.md for the ordered checklist.
//!
//! Comparison planes (float_in_diff conventions): value bytes/bits,
//! error-verdict, and errcode/sqlstate class. Message text is out of scope.
//!
//! Input layout: [selector][payload]; selector % 9 picks the arm:
//!   0 date_in  (oid 1084, C: date.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   1 date_out  (oid 1085, C: date.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   2 time_in  (oid 1143, C: date.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   3 time_out  (oid 1144, C: date.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   4 timetz_in  (oid 1350, C: date.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   5 timetz_out  (oid 1351, C: date.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   6 time_part  (oid 1385, C: date.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   7 make_time  (oid 3847, C: date.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   8 make_date  (oid 3846, C: date.c) — TODO(scaffold): document
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
    // in csrc/pg_datetime_io_io.c (declarations are link-inert until called, so
    // `cargo check` and `cargo test` stay green while sites are unfilled):
    // TODO(scaffold): fn pg_diff_date_in(...) -> i32;   [oid 1084, date.c]
    // TODO(scaffold): fn pg_diff_date_out(...) -> i32;   [oid 1085, date.c]
    // TODO(scaffold): fn pg_diff_time_in(...) -> i32;   [oid 1143, date.c]
    // TODO(scaffold): fn pg_diff_time_out(...) -> i32;   [oid 1144, date.c]
    // TODO(scaffold): fn pg_diff_timetz_in(...) -> i32;   [oid 1350, date.c]
    // TODO(scaffold): fn pg_diff_timetz_out(...) -> i32;   [oid 1351, date.c]
    // TODO(scaffold): fn pg_diff_time_part(...) -> i32;   [oid 1385, date.c]
    // TODO(scaffold): fn pg_diff_make_time(...) -> i32;   [oid 3847, date.c]
    // TODO(scaffold): fn pg_diff_make_date(...) -> i32;   [oid 3846, date.c]
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

pub fn datetime_io_diff(data: &[u8]) {
    let Some((&sel, payload)) = data.split_first() else {
        return;
    };
    match sel % 9 {
        0 => date_in_diff(payload),
        1 => date_out_diff(payload),
        2 => time_in_diff(payload),
        3 => time_out_diff(payload),
        4 => timetz_in_diff(payload),
        5 => timetz_out_diff(payload),
        6 => time_part_diff(payload),
        7 => make_time_diff(payload),
        _ => make_date_diff(payload),
    }
}

// ---------------------------------------------------------------------------
// Arm: date_in (oid 1084; C source: date.c).
// ---------------------------------------------------------------------------

fn date_in_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (datetime_io_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_datetime_io_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_date_in(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_date::date_in(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_date::builtins::fc_date_in via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(datetime_io_diff): date_in arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: date_out (oid 1085; C source: date.c).
// ---------------------------------------------------------------------------

fn date_out_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (datetime_io_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_datetime_io_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_date_out(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_date::date_out(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_date::builtins::fc_date_out via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(datetime_io_diff): date_out arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: time_in (oid 1143; C source: date.c).
// ---------------------------------------------------------------------------

fn time_in_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (datetime_io_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_datetime_io_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_time_in(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_date::time_in(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_date::builtins::fc_time_in via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(datetime_io_diff): time_in arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: time_out (oid 1144; C source: date.c).
// ---------------------------------------------------------------------------

fn time_out_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (datetime_io_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_datetime_io_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_time_out(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_date::time_out(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_date::builtins::fc_time_out via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(datetime_io_diff): time_out arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: timetz_in (oid 1350; C source: date.c).
// ---------------------------------------------------------------------------

fn timetz_in_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (datetime_io_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_datetime_io_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_timetz_in(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_date::timetz_in(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_date::builtins::fc_timetz_in via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(datetime_io_diff): timetz_in arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: timetz_out (oid 1351; C source: date.c).
// ---------------------------------------------------------------------------

fn timetz_out_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (datetime_io_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_datetime_io_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_timetz_out(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_date::timetz_out(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_date::builtins::fc_timetz_out via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(datetime_io_diff): timetz_out arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: time_part (oid 1385; C source: date.c).
// ---------------------------------------------------------------------------

fn time_part_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (datetime_io_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_datetime_io_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_time_part(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_date::time_part(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_date::builtins::fc_time_part via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(datetime_io_diff): time_part arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: make_time (oid 3847; C source: date.c).
// ---------------------------------------------------------------------------

fn make_time_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (datetime_io_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_datetime_io_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_make_time(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_date::make_time(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_date::builtins::fc_make_time via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(datetime_io_diff): make_time arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: make_date (oid 3846; C source: date.c).
// ---------------------------------------------------------------------------

fn make_date_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (datetime_io_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_datetime_io_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_make_date(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_date::make_date(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_date::builtins::fc_make_date via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(datetime_io_diff): make_date arm not implemented");
}

// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Replay every checked-in seed (catches shim/link errors before the
    /// nightly fuzz campaign). TODO(scaffold): un-ignore once the arms are
    /// implemented and ../corpus/datetime_io_diff/ is seeded (>=30 seeds; corpora
    /// are COMMITTED — plain `git add`, no -f needed).
    #[test]
    #[ignore = "scaffold(datetime_io_diff): arms not implemented yet"]
    fn seed_corpus_replays_clean() {
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/datetime_io_diff");
        let mut n = 0;
        for e in std::fs::read_dir(dir).expect("corpus/datetime_io_diff missing") {
            let p = e.unwrap().path();
            if p.is_file() {
                datetime_io_diff(&std::fs::read(&p).unwrap());
                n += 1;
            }
        }
        assert!(n >= 30, "expected >=30 seeds, found {n}");
    }

    /// TODO(scaffold): per-arm smoke tests on stable (ok + error shapes per
    /// arm, fc-plane smoke driving every wrapper at least once — see
    /// uuid_diff.rs tests for the expected shape). Start by un-ignoring:
    #[test]
    #[ignore = "scaffold(datetime_io_diff): arms not implemented yet"]
    fn arms_smoke() {
        // Arm 0 example: selector byte 0, then a payload for date_in.
        datetime_io_diff(&[0u8]);
    }
}
