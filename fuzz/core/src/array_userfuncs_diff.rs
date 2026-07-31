//! array_userfuncs_diff: differential fuzz driver — shipped Rust `array_userfuncs` vs vendored
//! PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) C
//! (csrc/pg_array_userfuncs_io.c). Crate under test: crates/backend/utils/adt/array_userfuncs.
//!
//! GENERATED SKELETON (fuzz/scaffold.py) — every TODO(scaffold) below is
//! hand-work; see fuzz/README-TODO-array_userfuncs_diff.md for the ordered checklist.
//!
//! Comparison planes (float_in_diff conventions): value bytes/bits,
//! error-verdict, and errcode/sqlstate class. Message text is out of scope.
//!
//! Input layout: [selector][payload]; selector % 13 picks the arm:
//!   0 array_append  (oid 378, C: array_userfuncs.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   1 array_prepend  (oid 379, C: array_userfuncs.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   2 array_cat  (oid 383, C: array_userfuncs.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   3 array_position  (oid 3277, C: array_userfuncs.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   4 array_position_start  (oid 3278, C: array_userfuncs.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   5 array_positions  (oid 3279, C: array_userfuncs.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   6 trim_array  (oid 6172, C: array_userfuncs.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   7 array_reverse  (oid 6381, C: array_userfuncs.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   8 array_shuffle  (oid 6215, C: array_userfuncs.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   9 array_sample  (oid 6216, C: array_userfuncs.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   10 array_agg_array_serialize  (oid 6297, C: array_userfuncs.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   11 array_agg_array_deserialize  (oid 6298, C: array_userfuncs.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   12 array_agg_array_combine  (oid 6296, C: array_userfuncs.c) — TODO(scaffold): document
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
    // in csrc/pg_array_userfuncs_io.c (declarations are link-inert until called, so
    // `cargo check` and `cargo test` stay green while sites are unfilled):
    // TODO(scaffold): fn pg_diff_array_append(...) -> i32;   [oid 378, array_userfuncs.c]
    // TODO(scaffold): fn pg_diff_array_prepend(...) -> i32;   [oid 379, array_userfuncs.c]
    // TODO(scaffold): fn pg_diff_array_cat(...) -> i32;   [oid 383, array_userfuncs.c]
    // TODO(scaffold): fn pg_diff_array_position(...) -> i32;   [oid 3277, array_userfuncs.c]
    // TODO(scaffold): fn pg_diff_array_position_start(...) -> i32;   [oid 3278, array_userfuncs.c]
    // TODO(scaffold): fn pg_diff_array_positions(...) -> i32;   [oid 3279, array_userfuncs.c]
    // TODO(scaffold): fn pg_diff_trim_array(...) -> i32;   [oid 6172, array_userfuncs.c]
    // TODO(scaffold): fn pg_diff_array_reverse(...) -> i32;   [oid 6381, array_userfuncs.c]
    // TODO(scaffold): fn pg_diff_array_shuffle(...) -> i32;   [oid 6215, array_userfuncs.c]
    // TODO(scaffold): fn pg_diff_array_sample(...) -> i32;   [oid 6216, array_userfuncs.c]
    // TODO(scaffold): fn pg_diff_array_agg_array_serialize(...) -> i32;   [oid 6297, array_userfuncs.c]
    // TODO(scaffold): fn pg_diff_array_agg_array_deserialize(...) -> i32;   [oid 6298, array_userfuncs.c]
    // TODO(scaffold): fn pg_diff_array_agg_array_combine(...) -> i32;   [oid 6296, array_userfuncs.c]
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

pub fn array_userfuncs_diff(data: &[u8]) {
    let Some((&sel, payload)) = data.split_first() else {
        return;
    };
    match sel % 13 {
        0 => array_append_diff(payload),
        1 => array_prepend_diff(payload),
        2 => array_cat_diff(payload),
        3 => array_position_diff(payload),
        4 => array_position_start_diff(payload),
        5 => array_positions_diff(payload),
        6 => trim_array_diff(payload),
        7 => array_reverse_diff(payload),
        8 => array_shuffle_diff(payload),
        9 => array_sample_diff(payload),
        10 => array_agg_array_serialize_diff(payload),
        11 => array_agg_array_deserialize_diff(payload),
        _ => array_agg_array_combine_diff(payload),
    }
}

// ---------------------------------------------------------------------------
// Arm: array_append (oid 378; C source: array_userfuncs.c).
// ---------------------------------------------------------------------------

fn array_append_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (array_userfuncs_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_array_userfuncs_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_array_append(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: array_userfuncs::array_append(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      array_userfuncs::builtins::fc_array_append via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(array_userfuncs_diff): array_append arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: array_prepend (oid 379; C source: array_userfuncs.c).
// ---------------------------------------------------------------------------

fn array_prepend_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (array_userfuncs_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_array_userfuncs_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_array_prepend(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: array_userfuncs::array_prepend(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      array_userfuncs::builtins::fc_array_prepend via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(array_userfuncs_diff): array_prepend arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: array_cat (oid 383; C source: array_userfuncs.c).
// ---------------------------------------------------------------------------

fn array_cat_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (array_userfuncs_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_array_userfuncs_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_array_cat(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: array_userfuncs::array_cat(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      array_userfuncs::builtins::fc_array_cat via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(array_userfuncs_diff): array_cat arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: array_position (oid 3277; C source: array_userfuncs.c).
// ---------------------------------------------------------------------------

fn array_position_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (array_userfuncs_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_array_userfuncs_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_array_position(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: array_userfuncs::array_position(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      array_userfuncs::builtins::fc_array_position via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(array_userfuncs_diff): array_position arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: array_position_start (oid 3278; C source: array_userfuncs.c).
// ---------------------------------------------------------------------------

fn array_position_start_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (array_userfuncs_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_array_userfuncs_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_array_position_start(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: array_userfuncs::array_position_start(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      array_userfuncs::builtins::fc_array_position_start via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(array_userfuncs_diff): array_position_start arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: array_positions (oid 3279; C source: array_userfuncs.c).
// ---------------------------------------------------------------------------

fn array_positions_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (array_userfuncs_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_array_userfuncs_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_array_positions(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: array_userfuncs::array_positions(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      array_userfuncs::builtins::fc_array_positions via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(array_userfuncs_diff): array_positions arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: trim_array (oid 6172; C source: array_userfuncs.c).
// ---------------------------------------------------------------------------

fn trim_array_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (array_userfuncs_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_array_userfuncs_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_trim_array(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: array_userfuncs::trim_array(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      array_userfuncs::builtins::fc_trim_array via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(array_userfuncs_diff): trim_array arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: array_reverse (oid 6381; C source: array_userfuncs.c).
// ---------------------------------------------------------------------------

fn array_reverse_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (array_userfuncs_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_array_userfuncs_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_array_reverse(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: array_userfuncs::array_reverse(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      array_userfuncs::builtins::fc_array_reverse via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(array_userfuncs_diff): array_reverse arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: array_shuffle (oid 6215; C source: array_userfuncs.c).
// ---------------------------------------------------------------------------

fn array_shuffle_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (array_userfuncs_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_array_userfuncs_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_array_shuffle(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: array_userfuncs::array_shuffle(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      array_userfuncs::builtins::fc_array_shuffle via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(array_userfuncs_diff): array_shuffle arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: array_sample (oid 6216; C source: array_userfuncs.c).
// ---------------------------------------------------------------------------

fn array_sample_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (array_userfuncs_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_array_userfuncs_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_array_sample(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: array_userfuncs::array_sample(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      array_userfuncs::builtins::fc_array_sample via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(array_userfuncs_diff): array_sample arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: array_agg_array_serialize (oid 6297; C source: array_userfuncs.c).
// ---------------------------------------------------------------------------

fn array_agg_array_serialize_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (array_userfuncs_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_array_userfuncs_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_array_agg_array_serialize(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: array_userfuncs::array_agg_array_serialize(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      array_userfuncs::builtins::fc_array_agg_array_serialize via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(array_userfuncs_diff): array_agg_array_serialize arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: array_agg_array_deserialize (oid 6298; C source: array_userfuncs.c).
// ---------------------------------------------------------------------------

fn array_agg_array_deserialize_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (array_userfuncs_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_array_userfuncs_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_array_agg_array_deserialize(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: array_userfuncs::array_agg_array_deserialize(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      array_userfuncs::builtins::fc_array_agg_array_deserialize via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(array_userfuncs_diff): array_agg_array_deserialize arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: array_agg_array_combine (oid 6296; C source: array_userfuncs.c).
// ---------------------------------------------------------------------------

fn array_agg_array_combine_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (array_userfuncs_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_array_userfuncs_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_array_agg_array_combine(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: array_userfuncs::array_agg_array_combine(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      array_userfuncs::builtins::fc_array_agg_array_combine via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(array_userfuncs_diff): array_agg_array_combine arm not implemented");
}

// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Replay every checked-in seed (catches shim/link errors before the
    /// nightly fuzz campaign). TODO(scaffold): un-ignore once the arms are
    /// implemented and ../corpus/array_userfuncs_diff/ is seeded (>=30 seeds; corpora
    /// are COMMITTED — plain `git add`, no -f needed).
    #[test]
    #[ignore = "scaffold(array_userfuncs_diff): arms not implemented yet"]
    fn seed_corpus_replays_clean() {
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/array_userfuncs_diff");
        let mut n = 0;
        for e in std::fs::read_dir(dir).expect("corpus/array_userfuncs_diff missing") {
            let p = e.unwrap().path();
            if p.is_file() {
                array_userfuncs_diff(&std::fs::read(&p).unwrap());
                n += 1;
            }
        }
        assert!(n >= 30, "expected >=30 seeds, found {n}");
    }

    /// TODO(scaffold): per-arm smoke tests on stable (ok + error shapes per
    /// arm, fc-plane smoke driving every wrapper at least once — see
    /// uuid_diff.rs tests for the expected shape). Start by un-ignoring:
    #[test]
    #[ignore = "scaffold(array_userfuncs_diff): arms not implemented yet"]
    fn arms_smoke() {
        // Arm 0 example: selector byte 0, then a payload for array_append.
        array_userfuncs_diff(&[0u8]);
    }
}
