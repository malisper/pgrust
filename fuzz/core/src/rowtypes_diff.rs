//! rowtypes_diff: differential fuzz driver — shipped Rust `adt_rowtypes` vs vendored
//! PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) C
//! (csrc/pg_rowtypes_io.c). Crate under test: crates/backend/utils/adt/rowtypes.
//!
//! GENERATED SKELETON (fuzz/scaffold.py) — every TODO(scaffold) below is
//! hand-work; see fuzz/README-TODO-rowtypes_diff.md for the ordered checklist.
//!
//! Comparison planes (float_in_diff conventions): value bytes/bits,
//! error-verdict, and errcode/sqlstate class. Message text is out of scope.
//!
//! Input layout: [selector][payload]; selector % 10 picks the arm:
//!   0 record_in  (oid 2290, C: rowtypes.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   1 record_out  (oid 2291, C: rowtypes.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   2 record_recv  (oid 2402, C: rowtypes.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   3 record_send  (oid 2403, C: rowtypes.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   4 record_image_cmp  (oid 3187, C: rowtypes.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   5 record_image_eq  (oid 3181, C: rowtypes.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   6 hash_record  (oid 6192, C: rowtypes.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   7 hash_record_extended  (oid 6193, C: rowtypes.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   8 record_larger  (oid 6375, C: rowtypes.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   9 record_smaller  (oid 6376, C: rowtypes.c) — TODO(scaffold): document
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
    // in csrc/pg_rowtypes_io.c (declarations are link-inert until called, so
    // `cargo check` and `cargo test` stay green while sites are unfilled):
    // TODO(scaffold): fn pg_diff_record_in(...) -> i32;   [oid 2290, rowtypes.c]
    // TODO(scaffold): fn pg_diff_record_out(...) -> i32;   [oid 2291, rowtypes.c]
    // TODO(scaffold): fn pg_diff_record_recv(...) -> i32;   [oid 2402, rowtypes.c]
    // TODO(scaffold): fn pg_diff_record_send(...) -> i32;   [oid 2403, rowtypes.c]
    // TODO(scaffold): fn pg_diff_record_image_cmp(...) -> i32;   [oid 3187, rowtypes.c]
    // TODO(scaffold): fn pg_diff_record_image_eq(...) -> i32;   [oid 3181, rowtypes.c]
    // TODO(scaffold): fn pg_diff_hash_record(...) -> i32;   [oid 6192, rowtypes.c]
    // TODO(scaffold): fn pg_diff_hash_record_extended(...) -> i32;   [oid 6193, rowtypes.c]
    // TODO(scaffold): fn pg_diff_record_larger(...) -> i32;   [oid 6375, rowtypes.c]
    // TODO(scaffold): fn pg_diff_record_smaller(...) -> i32;   [oid 6376, rowtypes.c]
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

pub fn rowtypes_diff(data: &[u8]) {
    let Some((&sel, payload)) = data.split_first() else {
        return;
    };
    match sel % 10 {
        0 => record_in_diff(payload),
        1 => record_out_diff(payload),
        2 => record_recv_diff(payload),
        3 => record_send_diff(payload),
        4 => record_image_cmp_diff(payload),
        5 => record_image_eq_diff(payload),
        6 => hash_record_diff(payload),
        7 => hash_record_extended_diff(payload),
        8 => record_larger_diff(payload),
        _ => record_smaller_diff(payload),
    }
}

// ---------------------------------------------------------------------------
// Arm: record_in (oid 2290; C source: rowtypes.c).
// ---------------------------------------------------------------------------

fn record_in_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (rowtypes_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_rowtypes_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_record_in(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_rowtypes::record_in(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_rowtypes::builtins::fc_record_in via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(rowtypes_diff): record_in arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: record_out (oid 2291; C source: rowtypes.c).
// ---------------------------------------------------------------------------

fn record_out_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (rowtypes_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_rowtypes_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_record_out(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_rowtypes::record_out(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_rowtypes::builtins::fc_record_out via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(rowtypes_diff): record_out arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: record_recv (oid 2402; C source: rowtypes.c).
// ---------------------------------------------------------------------------

fn record_recv_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (rowtypes_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_rowtypes_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_record_recv(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_rowtypes::record_recv(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_rowtypes::builtins::fc_record_recv via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(rowtypes_diff): record_recv arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: record_send (oid 2403; C source: rowtypes.c).
// ---------------------------------------------------------------------------

fn record_send_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (rowtypes_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_rowtypes_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_record_send(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_rowtypes::record_send(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_rowtypes::builtins::fc_record_send via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(rowtypes_diff): record_send arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: record_image_cmp (oid 3187; C source: rowtypes.c).
// ---------------------------------------------------------------------------

fn record_image_cmp_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (rowtypes_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_rowtypes_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_record_image_cmp(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_rowtypes::record_image_cmp(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_rowtypes::builtins::fc_record_image_cmp via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(rowtypes_diff): record_image_cmp arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: record_image_eq (oid 3181; C source: rowtypes.c).
// ---------------------------------------------------------------------------

fn record_image_eq_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (rowtypes_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_rowtypes_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_record_image_eq(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_rowtypes::record_image_eq(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_rowtypes::builtins::fc_record_image_eq via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(rowtypes_diff): record_image_eq arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: hash_record (oid 6192; C source: rowtypes.c).
// ---------------------------------------------------------------------------

fn hash_record_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (rowtypes_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_rowtypes_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_hash_record(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_rowtypes::hash_record(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_rowtypes::builtins::fc_hash_record via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(rowtypes_diff): hash_record arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: hash_record_extended (oid 6193; C source: rowtypes.c).
// ---------------------------------------------------------------------------

fn hash_record_extended_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (rowtypes_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_rowtypes_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_hash_record_extended(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_rowtypes::hash_record_extended(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_rowtypes::builtins::fc_hash_record_extended via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(rowtypes_diff): hash_record_extended arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: record_larger (oid 6375; C source: rowtypes.c).
// ---------------------------------------------------------------------------

fn record_larger_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (rowtypes_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_rowtypes_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_record_larger(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_rowtypes::record_larger(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_rowtypes::builtins::fc_record_larger via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(rowtypes_diff): record_larger arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: record_smaller (oid 6376; C source: rowtypes.c).
// ---------------------------------------------------------------------------

fn record_smaller_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (rowtypes_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_rowtypes_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_record_smaller(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_rowtypes::record_smaller(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_rowtypes::builtins::fc_record_smaller via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(rowtypes_diff): record_smaller arm not implemented");
}

// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Replay every checked-in seed (catches shim/link errors before the
    /// nightly fuzz campaign). TODO(scaffold): un-ignore once the arms are
    /// implemented and ../corpus/rowtypes_diff/ is seeded (>=30 seeds; corpora
    /// are COMMITTED — plain `git add`, no -f needed).
    #[test]
    #[ignore = "scaffold(rowtypes_diff): arms not implemented yet"]
    fn seed_corpus_replays_clean() {
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/rowtypes_diff");
        let mut n = 0;
        for e in std::fs::read_dir(dir).expect("corpus/rowtypes_diff missing") {
            let p = e.unwrap().path();
            if p.is_file() {
                rowtypes_diff(&std::fs::read(&p).unwrap());
                n += 1;
            }
        }
        assert!(n >= 30, "expected >=30 seeds, found {n}");
    }

    /// TODO(scaffold): per-arm smoke tests on stable (ok + error shapes per
    /// arm, fc-plane smoke driving every wrapper at least once — see
    /// uuid_diff.rs tests for the expected shape). Start by un-ignoring:
    #[test]
    #[ignore = "scaffold(rowtypes_diff): arms not implemented yet"]
    fn arms_smoke() {
        // Arm 0 example: selector byte 0, then a payload for record_in.
        rowtypes_diff(&[0u8]);
    }
}
