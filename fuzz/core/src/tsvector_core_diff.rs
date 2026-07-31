//! tsvector_core_diff: differential fuzz driver — shipped Rust `adt_tsvector_core` vs vendored
//! PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) C
//! (csrc/pg_tsvector_core_io.c). Crate under test: crates/backend/utils/adt/tsvector_core.
//!
//! GENERATED SKELETON (fuzz/scaffold.py) — every TODO(scaffold) below is
//! hand-work; see fuzz/README-TODO-tsvector_core_diff.md for the ordered checklist.
//!
//! Comparison planes (float_in_diff conventions): value bytes/bits,
//! error-verdict, and errcode/sqlstate class. Message text is out of scope.
//!
//! Input layout: [selector][payload]; selector % 23 picks the arm:
//!   0 tsvectorin  (oid 3610, C: tsvector.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   1 tsvectorout  (oid 3611, C: tsvector.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   2 tsvectorsend  (oid 3638, C: tsvector.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   3 tsvectorrecv  (oid 3639, C: tsvector.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   4 tsvector_lt  (oid 3616, C: tsvector_op.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   5 tsvector_le  (oid 3617, C: tsvector_op.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   6 tsvector_eq  (oid 3618, C: tsvector_op.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   7 tsvector_ne  (oid 3619, C: tsvector_op.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   8 tsvector_ge  (oid 3620, C: tsvector_op.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   9 tsvector_gt  (oid 3621, C: tsvector_op.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   10 tsvector_cmp  (oid 3622, C: tsvector_op.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   11 tsvector_strip  (oid 3623, C: tsvector_op.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   12 tsvector_setweight  (oid 3624, C: tsvector_op.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   13 tsvector_concat  (oid 3625, C: tsvector_op.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   14 tsvector_length  (oid 3711, C: tsvector_op.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   15 tsvector_filter  (oid 3319, C: tsvector_op.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   16 tsvector_setweight_by_filter  (oid 3320, C: tsvector_op.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   17 tsvector_delete_str  (oid 3321, C: tsvector_op.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   18 tsvector_delete_arr  (oid 3323, C: tsvector_op.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   19 tsvector_to_array  (oid 3326, C: tsvector_op.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   20 array_to_tsvector  (oid 3327, C: tsvector_op.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   21 ts_match_vq  (oid 3634, C: tsvector_op.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   22 ts_match_qv  (oid 3635, C: tsvector_op.c) — TODO(scaffold): document
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
    // in csrc/pg_tsvector_core_io.c (declarations are link-inert until called, so
    // `cargo check` and `cargo test` stay green while sites are unfilled):
    // TODO(scaffold): fn pg_diff_tsvectorin(...) -> i32;   [oid 3610, tsvector.c]
    // TODO(scaffold): fn pg_diff_tsvectorout(...) -> i32;   [oid 3611, tsvector.c]
    // TODO(scaffold): fn pg_diff_tsvectorsend(...) -> i32;   [oid 3638, tsvector.c]
    // TODO(scaffold): fn pg_diff_tsvectorrecv(...) -> i32;   [oid 3639, tsvector.c]
    // TODO(scaffold): fn pg_diff_tsvector_lt(...) -> i32;   [oid 3616, tsvector_op.c]
    // TODO(scaffold): fn pg_diff_tsvector_le(...) -> i32;   [oid 3617, tsvector_op.c]
    // TODO(scaffold): fn pg_diff_tsvector_eq(...) -> i32;   [oid 3618, tsvector_op.c]
    // TODO(scaffold): fn pg_diff_tsvector_ne(...) -> i32;   [oid 3619, tsvector_op.c]
    // TODO(scaffold): fn pg_diff_tsvector_ge(...) -> i32;   [oid 3620, tsvector_op.c]
    // TODO(scaffold): fn pg_diff_tsvector_gt(...) -> i32;   [oid 3621, tsvector_op.c]
    // TODO(scaffold): fn pg_diff_tsvector_cmp(...) -> i32;   [oid 3622, tsvector_op.c]
    // TODO(scaffold): fn pg_diff_tsvector_strip(...) -> i32;   [oid 3623, tsvector_op.c]
    // TODO(scaffold): fn pg_diff_tsvector_setweight(...) -> i32;   [oid 3624, tsvector_op.c]
    // TODO(scaffold): fn pg_diff_tsvector_concat(...) -> i32;   [oid 3625, tsvector_op.c]
    // TODO(scaffold): fn pg_diff_tsvector_length(...) -> i32;   [oid 3711, tsvector_op.c]
    // TODO(scaffold): fn pg_diff_tsvector_filter(...) -> i32;   [oid 3319, tsvector_op.c]
    // TODO(scaffold): fn pg_diff_tsvector_setweight_by_filter(...) -> i32;   [oid 3320, tsvector_op.c]
    // TODO(scaffold): fn pg_diff_tsvector_delete_str(...) -> i32;   [oid 3321, tsvector_op.c]
    // TODO(scaffold): fn pg_diff_tsvector_delete_arr(...) -> i32;   [oid 3323, tsvector_op.c]
    // TODO(scaffold): fn pg_diff_tsvector_to_array(...) -> i32;   [oid 3326, tsvector_op.c]
    // TODO(scaffold): fn pg_diff_array_to_tsvector(...) -> i32;   [oid 3327, tsvector_op.c]
    // TODO(scaffold): fn pg_diff_ts_match_vq(...) -> i32;   [oid 3634, tsvector_op.c]
    // TODO(scaffold): fn pg_diff_ts_match_qv(...) -> i32;   [oid 3635, tsvector_op.c]
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

pub fn tsvector_core_diff(data: &[u8]) {
    let Some((&sel, payload)) = data.split_first() else {
        return;
    };
    match sel % 23 {
        0 => tsvectorin_diff(payload),
        1 => tsvectorout_diff(payload),
        2 => tsvectorsend_diff(payload),
        3 => tsvectorrecv_diff(payload),
        4 => tsvector_lt_diff(payload),
        5 => tsvector_le_diff(payload),
        6 => tsvector_eq_diff(payload),
        7 => tsvector_ne_diff(payload),
        8 => tsvector_ge_diff(payload),
        9 => tsvector_gt_diff(payload),
        10 => tsvector_cmp_diff(payload),
        11 => tsvector_strip_diff(payload),
        12 => tsvector_setweight_diff(payload),
        13 => tsvector_concat_diff(payload),
        14 => tsvector_length_diff(payload),
        15 => tsvector_filter_diff(payload),
        16 => tsvector_setweight_by_filter_diff(payload),
        17 => tsvector_delete_str_diff(payload),
        18 => tsvector_delete_arr_diff(payload),
        19 => tsvector_to_array_diff(payload),
        20 => array_to_tsvector_diff(payload),
        21 => ts_match_vq_diff(payload),
        _ => ts_match_qv_diff(payload),
    }
}

// ---------------------------------------------------------------------------
// Arm: tsvectorin (oid 3610; C source: tsvector.c).
// ---------------------------------------------------------------------------

fn tsvectorin_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (tsvector_core_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_tsvector_core_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_tsvectorin(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_tsvector_core::tsvectorin(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_tsvector_core::builtins::fc_tsvectorin via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(tsvector_core_diff): tsvectorin arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: tsvectorout (oid 3611; C source: tsvector.c).
// ---------------------------------------------------------------------------

fn tsvectorout_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (tsvector_core_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_tsvector_core_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_tsvectorout(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_tsvector_core::tsvectorout(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_tsvector_core::builtins::fc_tsvectorout via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(tsvector_core_diff): tsvectorout arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: tsvectorsend (oid 3638; C source: tsvector.c).
// ---------------------------------------------------------------------------

fn tsvectorsend_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (tsvector_core_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_tsvector_core_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_tsvectorsend(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_tsvector_core::tsvectorsend(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_tsvector_core::builtins::fc_tsvectorsend via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(tsvector_core_diff): tsvectorsend arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: tsvectorrecv (oid 3639; C source: tsvector.c).
// ---------------------------------------------------------------------------

fn tsvectorrecv_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (tsvector_core_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_tsvector_core_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_tsvectorrecv(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_tsvector_core::tsvectorrecv(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_tsvector_core::builtins::fc_tsvectorrecv via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(tsvector_core_diff): tsvectorrecv arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: tsvector_lt (oid 3616; C source: tsvector_op.c).
// ---------------------------------------------------------------------------

fn tsvector_lt_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (tsvector_core_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_tsvector_core_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_tsvector_lt(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_tsvector_core::tsvector_lt(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_tsvector_core::builtins::fc_tsvector_lt via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(tsvector_core_diff): tsvector_lt arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: tsvector_le (oid 3617; C source: tsvector_op.c).
// ---------------------------------------------------------------------------

fn tsvector_le_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (tsvector_core_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_tsvector_core_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_tsvector_le(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_tsvector_core::tsvector_le(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_tsvector_core::builtins::fc_tsvector_le via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(tsvector_core_diff): tsvector_le arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: tsvector_eq (oid 3618; C source: tsvector_op.c).
// ---------------------------------------------------------------------------

fn tsvector_eq_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (tsvector_core_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_tsvector_core_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_tsvector_eq(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_tsvector_core::tsvector_eq(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_tsvector_core::builtins::fc_tsvector_eq via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(tsvector_core_diff): tsvector_eq arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: tsvector_ne (oid 3619; C source: tsvector_op.c).
// ---------------------------------------------------------------------------

fn tsvector_ne_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (tsvector_core_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_tsvector_core_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_tsvector_ne(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_tsvector_core::tsvector_ne(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_tsvector_core::builtins::fc_tsvector_ne via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(tsvector_core_diff): tsvector_ne arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: tsvector_ge (oid 3620; C source: tsvector_op.c).
// ---------------------------------------------------------------------------

fn tsvector_ge_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (tsvector_core_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_tsvector_core_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_tsvector_ge(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_tsvector_core::tsvector_ge(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_tsvector_core::builtins::fc_tsvector_ge via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(tsvector_core_diff): tsvector_ge arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: tsvector_gt (oid 3621; C source: tsvector_op.c).
// ---------------------------------------------------------------------------

fn tsvector_gt_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (tsvector_core_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_tsvector_core_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_tsvector_gt(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_tsvector_core::tsvector_gt(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_tsvector_core::builtins::fc_tsvector_gt via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(tsvector_core_diff): tsvector_gt arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: tsvector_cmp (oid 3622; C source: tsvector_op.c).
// ---------------------------------------------------------------------------

fn tsvector_cmp_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (tsvector_core_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_tsvector_core_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_tsvector_cmp(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_tsvector_core::tsvector_cmp(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_tsvector_core::builtins::fc_tsvector_cmp via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(tsvector_core_diff): tsvector_cmp arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: tsvector_strip (oid 3623; C source: tsvector_op.c).
// ---------------------------------------------------------------------------

fn tsvector_strip_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (tsvector_core_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_tsvector_core_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_tsvector_strip(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_tsvector_core::tsvector_strip(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_tsvector_core::builtins::fc_tsvector_strip via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(tsvector_core_diff): tsvector_strip arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: tsvector_setweight (oid 3624; C source: tsvector_op.c).
// ---------------------------------------------------------------------------

fn tsvector_setweight_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (tsvector_core_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_tsvector_core_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_tsvector_setweight(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_tsvector_core::tsvector_setweight(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_tsvector_core::builtins::fc_tsvector_setweight via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(tsvector_core_diff): tsvector_setweight arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: tsvector_concat (oid 3625; C source: tsvector_op.c).
// ---------------------------------------------------------------------------

fn tsvector_concat_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (tsvector_core_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_tsvector_core_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_tsvector_concat(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_tsvector_core::tsvector_concat(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_tsvector_core::builtins::fc_tsvector_concat via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(tsvector_core_diff): tsvector_concat arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: tsvector_length (oid 3711; C source: tsvector_op.c).
// ---------------------------------------------------------------------------

fn tsvector_length_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (tsvector_core_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_tsvector_core_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_tsvector_length(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_tsvector_core::tsvector_length(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_tsvector_core::builtins::fc_tsvector_length via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(tsvector_core_diff): tsvector_length arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: tsvector_filter (oid 3319; C source: tsvector_op.c).
// ---------------------------------------------------------------------------

fn tsvector_filter_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (tsvector_core_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_tsvector_core_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_tsvector_filter(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_tsvector_core::tsvector_filter(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_tsvector_core::builtins::fc_tsvector_filter via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(tsvector_core_diff): tsvector_filter arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: tsvector_setweight_by_filter (oid 3320; C source: tsvector_op.c).
// ---------------------------------------------------------------------------

fn tsvector_setweight_by_filter_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (tsvector_core_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_tsvector_core_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_tsvector_setweight_by_filter(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_tsvector_core::tsvector_setweight_by_filter(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_tsvector_core::builtins::fc_tsvector_setweight_by_filter via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(tsvector_core_diff): tsvector_setweight_by_filter arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: tsvector_delete_str (oid 3321; C source: tsvector_op.c).
// ---------------------------------------------------------------------------

fn tsvector_delete_str_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (tsvector_core_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_tsvector_core_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_tsvector_delete_str(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_tsvector_core::tsvector_delete_str(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_tsvector_core::builtins::fc_tsvector_delete_str via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(tsvector_core_diff): tsvector_delete_str arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: tsvector_delete_arr (oid 3323; C source: tsvector_op.c).
// ---------------------------------------------------------------------------

fn tsvector_delete_arr_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (tsvector_core_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_tsvector_core_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_tsvector_delete_arr(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_tsvector_core::tsvector_delete_arr(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_tsvector_core::builtins::fc_tsvector_delete_arr via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(tsvector_core_diff): tsvector_delete_arr arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: tsvector_to_array (oid 3326; C source: tsvector_op.c).
// ---------------------------------------------------------------------------

fn tsvector_to_array_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (tsvector_core_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_tsvector_core_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_tsvector_to_array(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_tsvector_core::tsvector_to_array(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_tsvector_core::builtins::fc_tsvector_to_array via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(tsvector_core_diff): tsvector_to_array arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: array_to_tsvector (oid 3327; C source: tsvector_op.c).
// ---------------------------------------------------------------------------

fn array_to_tsvector_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (tsvector_core_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_tsvector_core_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_array_to_tsvector(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_tsvector_core::array_to_tsvector(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_tsvector_core::builtins::fc_array_to_tsvector via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(tsvector_core_diff): array_to_tsvector arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: ts_match_vq (oid 3634; C source: tsvector_op.c).
// ---------------------------------------------------------------------------

fn ts_match_vq_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (tsvector_core_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_tsvector_core_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_ts_match_vq(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_tsvector_core::ts_match_vq(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_tsvector_core::builtins::fc_ts_match_vq via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(tsvector_core_diff): ts_match_vq arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: ts_match_qv (oid 3635; C source: tsvector_op.c).
// ---------------------------------------------------------------------------

fn ts_match_qv_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (tsvector_core_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_tsvector_core_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_ts_match_qv(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_tsvector_core::ts_match_qv(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_tsvector_core::builtins::fc_ts_match_qv via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(tsvector_core_diff): ts_match_qv arm not implemented");
}

// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Replay every checked-in seed (catches shim/link errors before the
    /// nightly fuzz campaign). TODO(scaffold): un-ignore once the arms are
    /// implemented and ../corpus/tsvector_core_diff/ is seeded (>=30 seeds; corpora
    /// are COMMITTED — plain `git add`, no -f needed).
    #[test]
    #[ignore = "scaffold(tsvector_core_diff): arms not implemented yet"]
    fn seed_corpus_replays_clean() {
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/tsvector_core_diff");
        let mut n = 0;
        for e in std::fs::read_dir(dir).expect("corpus/tsvector_core_diff missing") {
            let p = e.unwrap().path();
            if p.is_file() {
                tsvector_core_diff(&std::fs::read(&p).unwrap());
                n += 1;
            }
        }
        assert!(n >= 30, "expected >=30 seeds, found {n}");
    }

    /// TODO(scaffold): per-arm smoke tests on stable (ok + error shapes per
    /// arm, fc-plane smoke driving every wrapper at least once — see
    /// uuid_diff.rs tests for the expected shape). Start by un-ignoring:
    #[test]
    #[ignore = "scaffold(tsvector_core_diff): arms not implemented yet"]
    fn arms_smoke() {
        // Arm 0 example: selector byte 0, then a payload for tsvectorin.
        tsvector_core_diff(&[0u8]);
    }
}
