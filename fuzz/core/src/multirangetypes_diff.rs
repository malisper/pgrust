//! multirangetypes_diff: differential fuzz driver — shipped Rust `adt_multirangetypes` vs vendored
//! PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) C
//! (csrc/pg_multirangetypes_io.c). Crate under test: crates/backend/utils/adt/multirangetypes.
//!
//! GENERATED SKELETON (fuzz/scaffold.py) — every TODO(scaffold) below is
//! hand-work; see fuzz/README-TODO-multirangetypes_diff.md for the ordered checklist.
//!
//! Comparison planes (float_in_diff conventions): value bytes/bits,
//! error-verdict, and errcode/sqlstate class. Message text is out of scope.
//!
//! Input layout: [selector][payload]; selector % 28 picks the arm:
//!   0 multirange_in  (oid 4231, C: multirangetypes.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   1 multirange_out  (oid 4232, C: multirangetypes.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   2 multirange_recv  (oid 4233, C: multirangetypes.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   3 multirange_send  (oid 4234, C: multirangetypes.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   4 multirange_constructor0  (oid 4280, C: multirangetypes.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   5 multirange_constructor1  (oid 4281, C: multirangetypes.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   6 multirange_constructor2  (oid 4282, C: multirangetypes.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   7 multirange_lower  (oid 4235, C: multirangetypes.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   8 multirange_upper  (oid 4236, C: multirangetypes.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   9 multirange_empty  (oid 4237, C: multirangetypes.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   10 multirange_lower_inc  (oid 4238, C: multirangetypes.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   11 multirange_upper_inc  (oid 4239, C: multirangetypes.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   12 multirange_lower_inf  (oid 4240, C: multirangetypes.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   13 multirange_upper_inf  (oid 4241, C: multirangetypes.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   14 multirange_eq  (oid 4244, C: multirangetypes.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   15 multirange_cmp  (oid 4273, C: multirangetypes.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   16 multirange_contains_elem  (oid 4249, C: multirangetypes.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   17 multirange_contains_range  (oid 4250, C: multirangetypes.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   18 multirange_contains_multirange  (oid 4251, C: multirangetypes.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   19 multirange_overlaps_multirange  (oid 4248, C: multirangetypes.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   20 multirange_adjacent_multirange  (oid 4256, C: multirangetypes.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   21 multirange_before_multirange  (oid 4260, C: multirangetypes.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   22 multirange_union  (oid 4270, C: multirangetypes.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   23 multirange_minus  (oid 4271, C: multirangetypes.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   24 multirange_intersect  (oid 4272, C: multirangetypes.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   25 hash_multirange  (oid 4278, C: multirangetypes.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   26 hash_multirange_extended  (oid 4279, C: multirangetypes.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   27 range_merge_from_multirange  (oid 4228, C: multirangetypes.c) — TODO(scaffold): document
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
    // in csrc/pg_multirangetypes_io.c (declarations are link-inert until called, so
    // `cargo check` and `cargo test` stay green while sites are unfilled):
    // TODO(scaffold): fn pg_diff_multirange_in(...) -> i32;   [oid 4231, multirangetypes.c]
    // TODO(scaffold): fn pg_diff_multirange_out(...) -> i32;   [oid 4232, multirangetypes.c]
    // TODO(scaffold): fn pg_diff_multirange_recv(...) -> i32;   [oid 4233, multirangetypes.c]
    // TODO(scaffold): fn pg_diff_multirange_send(...) -> i32;   [oid 4234, multirangetypes.c]
    // TODO(scaffold): fn pg_diff_multirange_constructor0(...) -> i32;   [oid 4280, multirangetypes.c]
    // TODO(scaffold): fn pg_diff_multirange_constructor1(...) -> i32;   [oid 4281, multirangetypes.c]
    // TODO(scaffold): fn pg_diff_multirange_constructor2(...) -> i32;   [oid 4282, multirangetypes.c]
    // TODO(scaffold): fn pg_diff_multirange_lower(...) -> i32;   [oid 4235, multirangetypes.c]
    // TODO(scaffold): fn pg_diff_multirange_upper(...) -> i32;   [oid 4236, multirangetypes.c]
    // TODO(scaffold): fn pg_diff_multirange_empty(...) -> i32;   [oid 4237, multirangetypes.c]
    // TODO(scaffold): fn pg_diff_multirange_lower_inc(...) -> i32;   [oid 4238, multirangetypes.c]
    // TODO(scaffold): fn pg_diff_multirange_upper_inc(...) -> i32;   [oid 4239, multirangetypes.c]
    // TODO(scaffold): fn pg_diff_multirange_lower_inf(...) -> i32;   [oid 4240, multirangetypes.c]
    // TODO(scaffold): fn pg_diff_multirange_upper_inf(...) -> i32;   [oid 4241, multirangetypes.c]
    // TODO(scaffold): fn pg_diff_multirange_eq(...) -> i32;   [oid 4244, multirangetypes.c]
    // TODO(scaffold): fn pg_diff_multirange_cmp(...) -> i32;   [oid 4273, multirangetypes.c]
    // TODO(scaffold): fn pg_diff_multirange_contains_elem(...) -> i32;   [oid 4249, multirangetypes.c]
    // TODO(scaffold): fn pg_diff_multirange_contains_range(...) -> i32;   [oid 4250, multirangetypes.c]
    // TODO(scaffold): fn pg_diff_multirange_contains_multirange(...) -> i32;   [oid 4251, multirangetypes.c]
    // TODO(scaffold): fn pg_diff_multirange_overlaps_multirange(...) -> i32;   [oid 4248, multirangetypes.c]
    // TODO(scaffold): fn pg_diff_multirange_adjacent_multirange(...) -> i32;   [oid 4256, multirangetypes.c]
    // TODO(scaffold): fn pg_diff_multirange_before_multirange(...) -> i32;   [oid 4260, multirangetypes.c]
    // TODO(scaffold): fn pg_diff_multirange_union(...) -> i32;   [oid 4270, multirangetypes.c]
    // TODO(scaffold): fn pg_diff_multirange_minus(...) -> i32;   [oid 4271, multirangetypes.c]
    // TODO(scaffold): fn pg_diff_multirange_intersect(...) -> i32;   [oid 4272, multirangetypes.c]
    // TODO(scaffold): fn pg_diff_hash_multirange(...) -> i32;   [oid 4278, multirangetypes.c]
    // TODO(scaffold): fn pg_diff_hash_multirange_extended(...) -> i32;   [oid 4279, multirangetypes.c]
    // TODO(scaffold): fn pg_diff_range_merge_from_multirange(...) -> i32;   [oid 4228, multirangetypes.c]
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

pub fn multirangetypes_diff(data: &[u8]) {
    let Some((&sel, payload)) = data.split_first() else {
        return;
    };
    match sel % 28 {
        0 => multirange_in_diff(payload),
        1 => multirange_out_diff(payload),
        2 => multirange_recv_diff(payload),
        3 => multirange_send_diff(payload),
        4 => multirange_constructor0_diff(payload),
        5 => multirange_constructor1_diff(payload),
        6 => multirange_constructor2_diff(payload),
        7 => multirange_lower_diff(payload),
        8 => multirange_upper_diff(payload),
        9 => multirange_empty_diff(payload),
        10 => multirange_lower_inc_diff(payload),
        11 => multirange_upper_inc_diff(payload),
        12 => multirange_lower_inf_diff(payload),
        13 => multirange_upper_inf_diff(payload),
        14 => multirange_eq_diff(payload),
        15 => multirange_cmp_diff(payload),
        16 => multirange_contains_elem_diff(payload),
        17 => multirange_contains_range_diff(payload),
        18 => multirange_contains_multirange_diff(payload),
        19 => multirange_overlaps_multirange_diff(payload),
        20 => multirange_adjacent_multirange_diff(payload),
        21 => multirange_before_multirange_diff(payload),
        22 => multirange_union_diff(payload),
        23 => multirange_minus_diff(payload),
        24 => multirange_intersect_diff(payload),
        25 => hash_multirange_diff(payload),
        26 => hash_multirange_extended_diff(payload),
        _ => range_merge_from_multirange_diff(payload),
    }
}

// ---------------------------------------------------------------------------
// Arm: multirange_in (oid 4231; C source: multirangetypes.c).
// ---------------------------------------------------------------------------

fn multirange_in_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (multirangetypes_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_multirangetypes_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_multirange_in(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_multirangetypes::multirange_in(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_multirangetypes::builtins::fc_multirange_in via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(multirangetypes_diff): multirange_in arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: multirange_out (oid 4232; C source: multirangetypes.c).
// ---------------------------------------------------------------------------

fn multirange_out_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (multirangetypes_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_multirangetypes_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_multirange_out(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_multirangetypes::multirange_out(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_multirangetypes::builtins::fc_multirange_out via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(multirangetypes_diff): multirange_out arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: multirange_recv (oid 4233; C source: multirangetypes.c).
// ---------------------------------------------------------------------------

fn multirange_recv_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (multirangetypes_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_multirangetypes_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_multirange_recv(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_multirangetypes::multirange_recv(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_multirangetypes::builtins::fc_multirange_recv via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(multirangetypes_diff): multirange_recv arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: multirange_send (oid 4234; C source: multirangetypes.c).
// ---------------------------------------------------------------------------

fn multirange_send_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (multirangetypes_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_multirangetypes_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_multirange_send(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_multirangetypes::multirange_send(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_multirangetypes::builtins::fc_multirange_send via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(multirangetypes_diff): multirange_send arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: multirange_constructor0 (oid 4280; C source: multirangetypes.c).
// ---------------------------------------------------------------------------

fn multirange_constructor0_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (multirangetypes_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_multirangetypes_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_multirange_constructor0(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_multirangetypes::multirange_constructor0(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_multirangetypes::builtins::fc_multirange_constructor0 via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(multirangetypes_diff): multirange_constructor0 arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: multirange_constructor1 (oid 4281; C source: multirangetypes.c).
// ---------------------------------------------------------------------------

fn multirange_constructor1_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (multirangetypes_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_multirangetypes_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_multirange_constructor1(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_multirangetypes::multirange_constructor1(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_multirangetypes::builtins::fc_multirange_constructor1 via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(multirangetypes_diff): multirange_constructor1 arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: multirange_constructor2 (oid 4282; C source: multirangetypes.c).
// ---------------------------------------------------------------------------

fn multirange_constructor2_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (multirangetypes_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_multirangetypes_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_multirange_constructor2(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_multirangetypes::multirange_constructor2(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_multirangetypes::builtins::fc_multirange_constructor2 via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(multirangetypes_diff): multirange_constructor2 arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: multirange_lower (oid 4235; C source: multirangetypes.c).
// ---------------------------------------------------------------------------

fn multirange_lower_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (multirangetypes_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_multirangetypes_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_multirange_lower(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_multirangetypes::multirange_lower(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_multirangetypes::builtins::fc_multirange_lower via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(multirangetypes_diff): multirange_lower arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: multirange_upper (oid 4236; C source: multirangetypes.c).
// ---------------------------------------------------------------------------

fn multirange_upper_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (multirangetypes_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_multirangetypes_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_multirange_upper(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_multirangetypes::multirange_upper(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_multirangetypes::builtins::fc_multirange_upper via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(multirangetypes_diff): multirange_upper arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: multirange_empty (oid 4237; C source: multirangetypes.c).
// ---------------------------------------------------------------------------

fn multirange_empty_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (multirangetypes_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_multirangetypes_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_multirange_empty(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_multirangetypes::multirange_empty(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_multirangetypes::builtins::fc_multirange_empty via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(multirangetypes_diff): multirange_empty arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: multirange_lower_inc (oid 4238; C source: multirangetypes.c).
// ---------------------------------------------------------------------------

fn multirange_lower_inc_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (multirangetypes_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_multirangetypes_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_multirange_lower_inc(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_multirangetypes::multirange_lower_inc(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_multirangetypes::builtins::fc_multirange_lower_inc via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(multirangetypes_diff): multirange_lower_inc arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: multirange_upper_inc (oid 4239; C source: multirangetypes.c).
// ---------------------------------------------------------------------------

fn multirange_upper_inc_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (multirangetypes_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_multirangetypes_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_multirange_upper_inc(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_multirangetypes::multirange_upper_inc(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_multirangetypes::builtins::fc_multirange_upper_inc via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(multirangetypes_diff): multirange_upper_inc arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: multirange_lower_inf (oid 4240; C source: multirangetypes.c).
// ---------------------------------------------------------------------------

fn multirange_lower_inf_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (multirangetypes_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_multirangetypes_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_multirange_lower_inf(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_multirangetypes::multirange_lower_inf(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_multirangetypes::builtins::fc_multirange_lower_inf via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(multirangetypes_diff): multirange_lower_inf arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: multirange_upper_inf (oid 4241; C source: multirangetypes.c).
// ---------------------------------------------------------------------------

fn multirange_upper_inf_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (multirangetypes_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_multirangetypes_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_multirange_upper_inf(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_multirangetypes::multirange_upper_inf(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_multirangetypes::builtins::fc_multirange_upper_inf via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(multirangetypes_diff): multirange_upper_inf arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: multirange_eq (oid 4244; C source: multirangetypes.c).
// ---------------------------------------------------------------------------

fn multirange_eq_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (multirangetypes_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_multirangetypes_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_multirange_eq(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_multirangetypes::multirange_eq(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_multirangetypes::builtins::fc_multirange_eq via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(multirangetypes_diff): multirange_eq arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: multirange_cmp (oid 4273; C source: multirangetypes.c).
// ---------------------------------------------------------------------------

fn multirange_cmp_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (multirangetypes_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_multirangetypes_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_multirange_cmp(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_multirangetypes::multirange_cmp(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_multirangetypes::builtins::fc_multirange_cmp via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(multirangetypes_diff): multirange_cmp arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: multirange_contains_elem (oid 4249; C source: multirangetypes.c).
// ---------------------------------------------------------------------------

fn multirange_contains_elem_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (multirangetypes_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_multirangetypes_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_multirange_contains_elem(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_multirangetypes::multirange_contains_elem(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_multirangetypes::builtins::fc_multirange_contains_elem via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(multirangetypes_diff): multirange_contains_elem arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: multirange_contains_range (oid 4250; C source: multirangetypes.c).
// ---------------------------------------------------------------------------

fn multirange_contains_range_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (multirangetypes_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_multirangetypes_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_multirange_contains_range(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_multirangetypes::multirange_contains_range(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_multirangetypes::builtins::fc_multirange_contains_range via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(multirangetypes_diff): multirange_contains_range arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: multirange_contains_multirange (oid 4251; C source: multirangetypes.c).
// ---------------------------------------------------------------------------

fn multirange_contains_multirange_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (multirangetypes_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_multirangetypes_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_multirange_contains_multirange(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_multirangetypes::multirange_contains_multirange(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_multirangetypes::builtins::fc_multirange_contains_multirange via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(multirangetypes_diff): multirange_contains_multirange arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: multirange_overlaps_multirange (oid 4248; C source: multirangetypes.c).
// ---------------------------------------------------------------------------

fn multirange_overlaps_multirange_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (multirangetypes_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_multirangetypes_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_multirange_overlaps_multirange(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_multirangetypes::multirange_overlaps_multirange(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_multirangetypes::builtins::fc_multirange_overlaps_multirange via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(multirangetypes_diff): multirange_overlaps_multirange arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: multirange_adjacent_multirange (oid 4256; C source: multirangetypes.c).
// ---------------------------------------------------------------------------

fn multirange_adjacent_multirange_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (multirangetypes_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_multirangetypes_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_multirange_adjacent_multirange(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_multirangetypes::multirange_adjacent_multirange(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_multirangetypes::builtins::fc_multirange_adjacent_multirange via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(multirangetypes_diff): multirange_adjacent_multirange arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: multirange_before_multirange (oid 4260; C source: multirangetypes.c).
// ---------------------------------------------------------------------------

fn multirange_before_multirange_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (multirangetypes_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_multirangetypes_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_multirange_before_multirange(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_multirangetypes::multirange_before_multirange(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_multirangetypes::builtins::fc_multirange_before_multirange via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(multirangetypes_diff): multirange_before_multirange arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: multirange_union (oid 4270; C source: multirangetypes.c).
// ---------------------------------------------------------------------------

fn multirange_union_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (multirangetypes_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_multirangetypes_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_multirange_union(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_multirangetypes::multirange_union(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_multirangetypes::builtins::fc_multirange_union via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(multirangetypes_diff): multirange_union arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: multirange_minus (oid 4271; C source: multirangetypes.c).
// ---------------------------------------------------------------------------

fn multirange_minus_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (multirangetypes_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_multirangetypes_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_multirange_minus(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_multirangetypes::multirange_minus(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_multirangetypes::builtins::fc_multirange_minus via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(multirangetypes_diff): multirange_minus arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: multirange_intersect (oid 4272; C source: multirangetypes.c).
// ---------------------------------------------------------------------------

fn multirange_intersect_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (multirangetypes_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_multirangetypes_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_multirange_intersect(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_multirangetypes::multirange_intersect(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_multirangetypes::builtins::fc_multirange_intersect via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(multirangetypes_diff): multirange_intersect arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: hash_multirange (oid 4278; C source: multirangetypes.c).
// ---------------------------------------------------------------------------

fn hash_multirange_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (multirangetypes_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_multirangetypes_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_hash_multirange(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_multirangetypes::hash_multirange(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_multirangetypes::builtins::fc_hash_multirange via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(multirangetypes_diff): hash_multirange arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: hash_multirange_extended (oid 4279; C source: multirangetypes.c).
// ---------------------------------------------------------------------------

fn hash_multirange_extended_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (multirangetypes_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_multirangetypes_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_hash_multirange_extended(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_multirangetypes::hash_multirange_extended(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_multirangetypes::builtins::fc_hash_multirange_extended via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(multirangetypes_diff): hash_multirange_extended arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: range_merge_from_multirange (oid 4228; C source: multirangetypes.c).
// ---------------------------------------------------------------------------

fn range_merge_from_multirange_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (multirangetypes_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_multirangetypes_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_range_merge_from_multirange(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_multirangetypes::range_merge_from_multirange(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_multirangetypes::builtins::fc_range_merge_from_multirange via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(multirangetypes_diff): range_merge_from_multirange arm not implemented");
}

// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Replay every checked-in seed (catches shim/link errors before the
    /// nightly fuzz campaign). TODO(scaffold): un-ignore once the arms are
    /// implemented and ../corpus/multirangetypes_diff/ is seeded (>=30 seeds; corpora
    /// are COMMITTED — plain `git add`, no -f needed).
    #[test]
    #[ignore = "scaffold(multirangetypes_diff): arms not implemented yet"]
    fn seed_corpus_replays_clean() {
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/multirangetypes_diff");
        let mut n = 0;
        for e in std::fs::read_dir(dir).expect("corpus/multirangetypes_diff missing") {
            let p = e.unwrap().path();
            if p.is_file() {
                multirangetypes_diff(&std::fs::read(&p).unwrap());
                n += 1;
            }
        }
        assert!(n >= 30, "expected >=30 seeds, found {n}");
    }

    /// TODO(scaffold): per-arm smoke tests on stable (ok + error shapes per
    /// arm, fc-plane smoke driving every wrapper at least once — see
    /// uuid_diff.rs tests for the expected shape). Start by un-ignoring:
    #[test]
    #[ignore = "scaffold(multirangetypes_diff): arms not implemented yet"]
    fn arms_smoke() {
        // Arm 0 example: selector byte 0, then a payload for multirange_in.
        multirangetypes_diff(&[0u8]);
    }
}
