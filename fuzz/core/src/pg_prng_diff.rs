//! pg_prng_diff: differential fuzz driver — shipped Rust `pg_prng` vs vendored
//! PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) C
//! (csrc/pg_pg_prng_io.c). Crate under test: crates/common/pg_prng.
//!
//! GENERATED SKELETON (fuzz/scaffold.py) — every TODO(scaffold) below is
//! hand-work; see fuzz/README-TODO-pg_prng_diff.md for the ordered checklist.
//!
//! Comparison planes (float_in_diff conventions): value bytes/bits,
//! error-verdict, and errcode/sqlstate class. Message text is out of scope.
//!
//! Input layout: [selector][payload]; selector % 14 picks the arm:
//!   0 pg_prng_seed  (oid 0, C: pg_prng.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   1 pg_prng_fseed  (oid 0, C: pg_prng.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   2 pg_prng_seed_check  (oid 0, C: pg_prng.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   3 pg_prng_uint64  (oid 0, C: pg_prng.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   4 pg_prng_uint64_range  (oid 0, C: pg_prng.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   5 pg_prng_int64  (oid 0, C: pg_prng.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   6 pg_prng_int64p  (oid 0, C: pg_prng.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   7 pg_prng_int64_range  (oid 0, C: pg_prng.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   8 pg_prng_uint32  (oid 0, C: pg_prng.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   9 pg_prng_int32  (oid 0, C: pg_prng.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   10 pg_prng_int32p  (oid 0, C: pg_prng.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   11 pg_prng_double  (oid 0, C: pg_prng.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   12 pg_prng_double_normal  (oid 0, C: pg_prng.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   13 pg_prng_bool  (oid 0, C: pg_prng.c) — TODO(scaffold): document
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
    // in csrc/pg_pg_prng_io.c (declarations are link-inert until called, so
    // `cargo check` and `cargo test` stay green while sites are unfilled):
    // TODO(scaffold): fn pg_diff_pg_prng_seed(...) -> i32;   [oid 0, pg_prng.c]
    // TODO(scaffold): fn pg_diff_pg_prng_fseed(...) -> i32;   [oid 0, pg_prng.c]
    // TODO(scaffold): fn pg_diff_pg_prng_seed_check(...) -> i32;   [oid 0, pg_prng.c]
    // TODO(scaffold): fn pg_diff_pg_prng_uint64(...) -> i32;   [oid 0, pg_prng.c]
    // TODO(scaffold): fn pg_diff_pg_prng_uint64_range(...) -> i32;   [oid 0, pg_prng.c]
    // TODO(scaffold): fn pg_diff_pg_prng_int64(...) -> i32;   [oid 0, pg_prng.c]
    // TODO(scaffold): fn pg_diff_pg_prng_int64p(...) -> i32;   [oid 0, pg_prng.c]
    // TODO(scaffold): fn pg_diff_pg_prng_int64_range(...) -> i32;   [oid 0, pg_prng.c]
    // TODO(scaffold): fn pg_diff_pg_prng_uint32(...) -> i32;   [oid 0, pg_prng.c]
    // TODO(scaffold): fn pg_diff_pg_prng_int32(...) -> i32;   [oid 0, pg_prng.c]
    // TODO(scaffold): fn pg_diff_pg_prng_int32p(...) -> i32;   [oid 0, pg_prng.c]
    // TODO(scaffold): fn pg_diff_pg_prng_double(...) -> i32;   [oid 0, pg_prng.c]
    // TODO(scaffold): fn pg_diff_pg_prng_double_normal(...) -> i32;   [oid 0, pg_prng.c]
    // TODO(scaffold): fn pg_diff_pg_prng_bool(...) -> i32;   [oid 0, pg_prng.c]
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

pub fn pg_prng_diff(data: &[u8]) {
    let Some((&sel, payload)) = data.split_first() else {
        return;
    };
    match sel % 14 {
        0 => pg_prng_seed_diff(payload),
        1 => pg_prng_fseed_diff(payload),
        2 => pg_prng_seed_check_diff(payload),
        3 => pg_prng_uint64_diff(payload),
        4 => pg_prng_uint64_range_diff(payload),
        5 => pg_prng_int64_diff(payload),
        6 => pg_prng_int64p_diff(payload),
        7 => pg_prng_int64_range_diff(payload),
        8 => pg_prng_uint32_diff(payload),
        9 => pg_prng_int32_diff(payload),
        10 => pg_prng_int32p_diff(payload),
        11 => pg_prng_double_diff(payload),
        12 => pg_prng_double_normal_diff(payload),
        _ => pg_prng_bool_diff(payload),
    }
}

// ---------------------------------------------------------------------------
// Arm: pg_prng_seed (oid 0; C source: pg_prng.c).
// ---------------------------------------------------------------------------

fn pg_prng_seed_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (pg_prng_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_pg_prng_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_pg_prng_seed(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: pg_prng::pg_prng_seed(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      pg_prng::builtins::fc_pg_prng_seed via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(pg_prng_diff): pg_prng_seed arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: pg_prng_fseed (oid 0; C source: pg_prng.c).
// ---------------------------------------------------------------------------

fn pg_prng_fseed_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (pg_prng_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_pg_prng_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_pg_prng_fseed(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: pg_prng::pg_prng_fseed(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      pg_prng::builtins::fc_pg_prng_fseed via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(pg_prng_diff): pg_prng_fseed arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: pg_prng_seed_check (oid 0; C source: pg_prng.c).
// ---------------------------------------------------------------------------

fn pg_prng_seed_check_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (pg_prng_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_pg_prng_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_pg_prng_seed_check(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: pg_prng::pg_prng_seed_check(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      pg_prng::builtins::fc_pg_prng_seed_check via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(pg_prng_diff): pg_prng_seed_check arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: pg_prng_uint64 (oid 0; C source: pg_prng.c).
// ---------------------------------------------------------------------------

fn pg_prng_uint64_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (pg_prng_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_pg_prng_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_pg_prng_uint64(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: pg_prng::pg_prng_uint64(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      pg_prng::builtins::fc_pg_prng_uint64 via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(pg_prng_diff): pg_prng_uint64 arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: pg_prng_uint64_range (oid 0; C source: pg_prng.c).
// ---------------------------------------------------------------------------

fn pg_prng_uint64_range_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (pg_prng_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_pg_prng_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_pg_prng_uint64_range(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: pg_prng::pg_prng_uint64_range(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      pg_prng::builtins::fc_pg_prng_uint64_range via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(pg_prng_diff): pg_prng_uint64_range arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: pg_prng_int64 (oid 0; C source: pg_prng.c).
// ---------------------------------------------------------------------------

fn pg_prng_int64_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (pg_prng_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_pg_prng_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_pg_prng_int64(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: pg_prng::pg_prng_int64(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      pg_prng::builtins::fc_pg_prng_int64 via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(pg_prng_diff): pg_prng_int64 arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: pg_prng_int64p (oid 0; C source: pg_prng.c).
// ---------------------------------------------------------------------------

fn pg_prng_int64p_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (pg_prng_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_pg_prng_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_pg_prng_int64p(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: pg_prng::pg_prng_int64p(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      pg_prng::builtins::fc_pg_prng_int64p via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(pg_prng_diff): pg_prng_int64p arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: pg_prng_int64_range (oid 0; C source: pg_prng.c).
// ---------------------------------------------------------------------------

fn pg_prng_int64_range_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (pg_prng_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_pg_prng_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_pg_prng_int64_range(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: pg_prng::pg_prng_int64_range(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      pg_prng::builtins::fc_pg_prng_int64_range via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(pg_prng_diff): pg_prng_int64_range arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: pg_prng_uint32 (oid 0; C source: pg_prng.c).
// ---------------------------------------------------------------------------

fn pg_prng_uint32_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (pg_prng_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_pg_prng_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_pg_prng_uint32(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: pg_prng::pg_prng_uint32(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      pg_prng::builtins::fc_pg_prng_uint32 via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(pg_prng_diff): pg_prng_uint32 arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: pg_prng_int32 (oid 0; C source: pg_prng.c).
// ---------------------------------------------------------------------------

fn pg_prng_int32_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (pg_prng_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_pg_prng_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_pg_prng_int32(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: pg_prng::pg_prng_int32(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      pg_prng::builtins::fc_pg_prng_int32 via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(pg_prng_diff): pg_prng_int32 arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: pg_prng_int32p (oid 0; C source: pg_prng.c).
// ---------------------------------------------------------------------------

fn pg_prng_int32p_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (pg_prng_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_pg_prng_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_pg_prng_int32p(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: pg_prng::pg_prng_int32p(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      pg_prng::builtins::fc_pg_prng_int32p via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(pg_prng_diff): pg_prng_int32p arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: pg_prng_double (oid 0; C source: pg_prng.c).
// ---------------------------------------------------------------------------

fn pg_prng_double_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (pg_prng_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_pg_prng_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_pg_prng_double(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: pg_prng::pg_prng_double(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      pg_prng::builtins::fc_pg_prng_double via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(pg_prng_diff): pg_prng_double arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: pg_prng_double_normal (oid 0; C source: pg_prng.c).
// ---------------------------------------------------------------------------

fn pg_prng_double_normal_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (pg_prng_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_pg_prng_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_pg_prng_double_normal(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: pg_prng::pg_prng_double_normal(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      pg_prng::builtins::fc_pg_prng_double_normal via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(pg_prng_diff): pg_prng_double_normal arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: pg_prng_bool (oid 0; C source: pg_prng.c).
// ---------------------------------------------------------------------------

fn pg_prng_bool_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (pg_prng_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_pg_prng_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_pg_prng_bool(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: pg_prng::pg_prng_bool(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      pg_prng::builtins::fc_pg_prng_bool via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(pg_prng_diff): pg_prng_bool arm not implemented");
}

// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Replay every checked-in seed (catches shim/link errors before the
    /// nightly fuzz campaign). TODO(scaffold): un-ignore once the arms are
    /// implemented and ../corpus/pg_prng_diff/ is seeded (>=30 seeds; corpora
    /// are COMMITTED — plain `git add`, no -f needed).
    #[test]
    #[ignore = "scaffold(pg_prng_diff): arms not implemented yet"]
    fn seed_corpus_replays_clean() {
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/pg_prng_diff");
        let mut n = 0;
        for e in std::fs::read_dir(dir).expect("corpus/pg_prng_diff missing") {
            let p = e.unwrap().path();
            if p.is_file() {
                pg_prng_diff(&std::fs::read(&p).unwrap());
                n += 1;
            }
        }
        assert!(n >= 30, "expected >=30 seeds, found {n}");
    }

    /// TODO(scaffold): per-arm smoke tests on stable (ok + error shapes per
    /// arm, fc-plane smoke driving every wrapper at least once — see
    /// uuid_diff.rs tests for the expected shape). Start by un-ignoring:
    #[test]
    #[ignore = "scaffold(pg_prng_diff): arms not implemented yet"]
    fn arms_smoke() {
        // Arm 0 example: selector byte 0, then a payload for pg_prng_seed.
        pg_prng_diff(&[0u8]);
    }
}
