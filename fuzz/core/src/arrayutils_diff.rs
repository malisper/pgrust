//! arrayutils_diff: differential fuzz driver — shipped Rust `arrayutils` vs vendored
//! PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) C
//! (csrc/pg_arrayutils_io.c). Crate under test: crates/backend/utils/adt/arrayutils.
//!
//! GENERATED SKELETON (fuzz/scaffold.py) — every TODO(scaffold) below is
//! hand-work; see fuzz/README-TODO-arrayutils_diff.md for the ordered checklist.
//!
//! Comparison planes (float_in_diff conventions): value bytes/bits,
//! error-verdict, and errcode/sqlstate class. Message text is out of scope.
//!
//! Input layout: [selector][payload]; selector % 9 picks the arm:
//!   0 ArrayGetOffset  (oid 0, C: arrayutils.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   1 ArrayGetNItems  (oid 0, C: arrayutils.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   2 ArrayGetNItemsSafe  (oid 0, C: arrayutils.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   3 ArrayCheckBounds  (oid 0, C: arrayutils.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   4 ArrayCheckBoundsSafe  (oid 0, C: arrayutils.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   5 mda_get_range  (oid 0, C: arrayutils.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   6 mda_get_prod  (oid 0, C: arrayutils.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   7 mda_get_offset_values  (oid 0, C: arrayutils.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   8 mda_next_tuple  (oid 0, C: arrayutils.c) — TODO(scaffold): document
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
    // in csrc/pg_arrayutils_io.c (declarations are link-inert until called, so
    // `cargo check` and `cargo test` stay green while sites are unfilled):
    // TODO(scaffold): fn pg_diff_ArrayGetOffset(...) -> i32;   [oid 0, arrayutils.c]
    // TODO(scaffold): fn pg_diff_ArrayGetNItems(...) -> i32;   [oid 0, arrayutils.c]
    // TODO(scaffold): fn pg_diff_ArrayGetNItemsSafe(...) -> i32;   [oid 0, arrayutils.c]
    // TODO(scaffold): fn pg_diff_ArrayCheckBounds(...) -> i32;   [oid 0, arrayutils.c]
    // TODO(scaffold): fn pg_diff_ArrayCheckBoundsSafe(...) -> i32;   [oid 0, arrayutils.c]
    // TODO(scaffold): fn pg_diff_mda_get_range(...) -> i32;   [oid 0, arrayutils.c]
    // TODO(scaffold): fn pg_diff_mda_get_prod(...) -> i32;   [oid 0, arrayutils.c]
    // TODO(scaffold): fn pg_diff_mda_get_offset_values(...) -> i32;   [oid 0, arrayutils.c]
    // TODO(scaffold): fn pg_diff_mda_next_tuple(...) -> i32;   [oid 0, arrayutils.c]
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

pub fn arrayutils_diff(data: &[u8]) {
    let Some((&sel, payload)) = data.split_first() else {
        return;
    };
    match sel % 9 {
        0 => ArrayGetOffset_diff(payload),
        1 => ArrayGetNItems_diff(payload),
        2 => ArrayGetNItemsSafe_diff(payload),
        3 => ArrayCheckBounds_diff(payload),
        4 => ArrayCheckBoundsSafe_diff(payload),
        5 => mda_get_range_diff(payload),
        6 => mda_get_prod_diff(payload),
        7 => mda_get_offset_values_diff(payload),
        _ => mda_next_tuple_diff(payload),
    }
}

// ---------------------------------------------------------------------------
// Arm: ArrayGetOffset (oid 0; C source: arrayutils.c).
// ---------------------------------------------------------------------------

fn ArrayGetOffset_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (arrayutils_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_arrayutils_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_ArrayGetOffset(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: arrayutils::ArrayGetOffset(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      arrayutils::builtins::fc_ArrayGetOffset via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(arrayutils_diff): ArrayGetOffset arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: ArrayGetNItems (oid 0; C source: arrayutils.c).
// ---------------------------------------------------------------------------

fn ArrayGetNItems_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (arrayutils_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_arrayutils_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_ArrayGetNItems(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: arrayutils::ArrayGetNItems(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      arrayutils::builtins::fc_ArrayGetNItems via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(arrayutils_diff): ArrayGetNItems arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: ArrayGetNItemsSafe (oid 0; C source: arrayutils.c).
// ---------------------------------------------------------------------------

fn ArrayGetNItemsSafe_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (arrayutils_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_arrayutils_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_ArrayGetNItemsSafe(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: arrayutils::ArrayGetNItemsSafe(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      arrayutils::builtins::fc_ArrayGetNItemsSafe via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(arrayutils_diff): ArrayGetNItemsSafe arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: ArrayCheckBounds (oid 0; C source: arrayutils.c).
// ---------------------------------------------------------------------------

fn ArrayCheckBounds_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (arrayutils_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_arrayutils_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_ArrayCheckBounds(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: arrayutils::ArrayCheckBounds(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      arrayutils::builtins::fc_ArrayCheckBounds via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(arrayutils_diff): ArrayCheckBounds arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: ArrayCheckBoundsSafe (oid 0; C source: arrayutils.c).
// ---------------------------------------------------------------------------

fn ArrayCheckBoundsSafe_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (arrayutils_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_arrayutils_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_ArrayCheckBoundsSafe(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: arrayutils::ArrayCheckBoundsSafe(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      arrayutils::builtins::fc_ArrayCheckBoundsSafe via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(arrayutils_diff): ArrayCheckBoundsSafe arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: mda_get_range (oid 0; C source: arrayutils.c).
// ---------------------------------------------------------------------------

fn mda_get_range_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (arrayutils_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_arrayutils_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_mda_get_range(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: arrayutils::mda_get_range(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      arrayutils::builtins::fc_mda_get_range via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(arrayutils_diff): mda_get_range arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: mda_get_prod (oid 0; C source: arrayutils.c).
// ---------------------------------------------------------------------------

fn mda_get_prod_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (arrayutils_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_arrayutils_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_mda_get_prod(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: arrayutils::mda_get_prod(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      arrayutils::builtins::fc_mda_get_prod via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(arrayutils_diff): mda_get_prod arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: mda_get_offset_values (oid 0; C source: arrayutils.c).
// ---------------------------------------------------------------------------

fn mda_get_offset_values_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (arrayutils_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_arrayutils_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_mda_get_offset_values(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: arrayutils::mda_get_offset_values(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      arrayutils::builtins::fc_mda_get_offset_values via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(arrayutils_diff): mda_get_offset_values arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: mda_next_tuple (oid 0; C source: arrayutils.c).
// ---------------------------------------------------------------------------

fn mda_next_tuple_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (arrayutils_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_arrayutils_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_mda_next_tuple(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: arrayutils::mda_next_tuple(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      arrayutils::builtins::fc_mda_next_tuple via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(arrayutils_diff): mda_next_tuple arm not implemented");
}

// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Replay every checked-in seed (catches shim/link errors before the
    /// nightly fuzz campaign). TODO(scaffold): un-ignore once the arms are
    /// implemented and ../corpus/arrayutils_diff/ is seeded (>=30 seeds; corpora
    /// are COMMITTED — plain `git add`, no -f needed).
    #[test]
    #[ignore = "scaffold(arrayutils_diff): arms not implemented yet"]
    fn seed_corpus_replays_clean() {
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/arrayutils_diff");
        let mut n = 0;
        for e in std::fs::read_dir(dir).expect("corpus/arrayutils_diff missing") {
            let p = e.unwrap().path();
            if p.is_file() {
                arrayutils_diff(&std::fs::read(&p).unwrap());
                n += 1;
            }
        }
        assert!(n >= 30, "expected >=30 seeds, found {n}");
    }

    /// TODO(scaffold): per-arm smoke tests on stable (ok + error shapes per
    /// arm, fc-plane smoke driving every wrapper at least once — see
    /// uuid_diff.rs tests for the expected shape). Start by un-ignoring:
    #[test]
    #[ignore = "scaffold(arrayutils_diff): arms not implemented yet"]
    fn arms_smoke() {
        // Arm 0 example: selector byte 0, then a payload for ArrayGetOffset.
        arrayutils_diff(&[0u8]);
    }
}
