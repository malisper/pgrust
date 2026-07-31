//! rangetypes_diff: differential fuzz driver — shipped Rust `adt_rangetypes` vs vendored
//! PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) C
//! (csrc/pg_rangetypes_io.c). Crate under test: crates/backend/utils/adt/rangetypes.
//!
//! GENERATED SKELETON (fuzz/scaffold.py) — every TODO(scaffold) below is
//! hand-work; see fuzz/README-TODO-rangetypes_diff.md for the ordered checklist.
//!
//! Comparison planes (float_in_diff conventions): value bytes/bits,
//! error-verdict, and errcode/sqlstate class. Message text is out of scope.
//!
//! Input layout: [selector][payload]; selector % 31 picks the arm:
//!   0 range_in  (oid 3834, C: rangetypes.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   1 range_out  (oid 3835, C: rangetypes.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   2 range_recv  (oid 3836, C: rangetypes.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   3 range_send  (oid 3837, C: rangetypes.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   4 range_constructor2  (oid 3840, C: rangetypes.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   5 range_constructor3  (oid 3841, C: rangetypes.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   6 range_lower  (oid 3848, C: rangetypes.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   7 range_upper  (oid 3849, C: rangetypes.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   8 range_empty  (oid 3850, C: rangetypes.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   9 range_lower_inc  (oid 3851, C: rangetypes.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   10 range_upper_inc  (oid 3852, C: rangetypes.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   11 range_lower_inf  (oid 3853, C: rangetypes.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   12 range_upper_inf  (oid 3854, C: rangetypes.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   13 range_adjacent  (oid 3862, C: rangetypes.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   14 range_overleft  (oid 3865, C: rangetypes.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   15 range_overright  (oid 3866, C: rangetypes.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   16 range_union  (oid 3867, C: rangetypes.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   17 range_intersect  (oid 3868, C: rangetypes.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   18 range_minus  (oid 3869, C: rangetypes.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   19 range_merge  (oid 4057, C: rangetypes.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   20 hash_range  (oid 3902, C: rangetypes.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   21 hash_range_extended  (oid 3417, C: rangetypes.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   22 int4range_canonical  (oid 3914, C: rangetypes.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   23 int8range_canonical  (oid 3928, C: rangetypes.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   24 daterange_canonical  (oid 3915, C: rangetypes.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   25 int4range_subdiff  (oid 3922, C: rangetypes.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   26 int8range_subdiff  (oid 3923, C: rangetypes.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   27 numrange_subdiff  (oid 3924, C: rangetypes.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   28 daterange_subdiff  (oid 3925, C: rangetypes.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   29 tsrange_subdiff  (oid 3929, C: rangetypes.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   30 tstzrange_subdiff  (oid 3930, C: rangetypes.c) — TODO(scaffold): document
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
    // in csrc/pg_rangetypes_io.c (declarations are link-inert until called, so
    // `cargo check` and `cargo test` stay green while sites are unfilled):
    // TODO(scaffold): fn pg_diff_range_in(...) -> i32;   [oid 3834, rangetypes.c]
    // TODO(scaffold): fn pg_diff_range_out(...) -> i32;   [oid 3835, rangetypes.c]
    // TODO(scaffold): fn pg_diff_range_recv(...) -> i32;   [oid 3836, rangetypes.c]
    // TODO(scaffold): fn pg_diff_range_send(...) -> i32;   [oid 3837, rangetypes.c]
    // TODO(scaffold): fn pg_diff_range_constructor2(...) -> i32;   [oid 3840, rangetypes.c]
    // TODO(scaffold): fn pg_diff_range_constructor3(...) -> i32;   [oid 3841, rangetypes.c]
    // TODO(scaffold): fn pg_diff_range_lower(...) -> i32;   [oid 3848, rangetypes.c]
    // TODO(scaffold): fn pg_diff_range_upper(...) -> i32;   [oid 3849, rangetypes.c]
    // TODO(scaffold): fn pg_diff_range_empty(...) -> i32;   [oid 3850, rangetypes.c]
    // TODO(scaffold): fn pg_diff_range_lower_inc(...) -> i32;   [oid 3851, rangetypes.c]
    // TODO(scaffold): fn pg_diff_range_upper_inc(...) -> i32;   [oid 3852, rangetypes.c]
    // TODO(scaffold): fn pg_diff_range_lower_inf(...) -> i32;   [oid 3853, rangetypes.c]
    // TODO(scaffold): fn pg_diff_range_upper_inf(...) -> i32;   [oid 3854, rangetypes.c]
    // TODO(scaffold): fn pg_diff_range_adjacent(...) -> i32;   [oid 3862, rangetypes.c]
    // TODO(scaffold): fn pg_diff_range_overleft(...) -> i32;   [oid 3865, rangetypes.c]
    // TODO(scaffold): fn pg_diff_range_overright(...) -> i32;   [oid 3866, rangetypes.c]
    // TODO(scaffold): fn pg_diff_range_union(...) -> i32;   [oid 3867, rangetypes.c]
    // TODO(scaffold): fn pg_diff_range_intersect(...) -> i32;   [oid 3868, rangetypes.c]
    // TODO(scaffold): fn pg_diff_range_minus(...) -> i32;   [oid 3869, rangetypes.c]
    // TODO(scaffold): fn pg_diff_range_merge(...) -> i32;   [oid 4057, rangetypes.c]
    // TODO(scaffold): fn pg_diff_hash_range(...) -> i32;   [oid 3902, rangetypes.c]
    // TODO(scaffold): fn pg_diff_hash_range_extended(...) -> i32;   [oid 3417, rangetypes.c]
    // TODO(scaffold): fn pg_diff_int4range_canonical(...) -> i32;   [oid 3914, rangetypes.c]
    // TODO(scaffold): fn pg_diff_int8range_canonical(...) -> i32;   [oid 3928, rangetypes.c]
    // TODO(scaffold): fn pg_diff_daterange_canonical(...) -> i32;   [oid 3915, rangetypes.c]
    // TODO(scaffold): fn pg_diff_int4range_subdiff(...) -> i32;   [oid 3922, rangetypes.c]
    // TODO(scaffold): fn pg_diff_int8range_subdiff(...) -> i32;   [oid 3923, rangetypes.c]
    // TODO(scaffold): fn pg_diff_numrange_subdiff(...) -> i32;   [oid 3924, rangetypes.c]
    // TODO(scaffold): fn pg_diff_daterange_subdiff(...) -> i32;   [oid 3925, rangetypes.c]
    // TODO(scaffold): fn pg_diff_tsrange_subdiff(...) -> i32;   [oid 3929, rangetypes.c]
    // TODO(scaffold): fn pg_diff_tstzrange_subdiff(...) -> i32;   [oid 3930, rangetypes.c]
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

pub fn rangetypes_diff(data: &[u8]) {
    let Some((&sel, payload)) = data.split_first() else {
        return;
    };
    match sel % 31 {
        0 => range_in_diff(payload),
        1 => range_out_diff(payload),
        2 => range_recv_diff(payload),
        3 => range_send_diff(payload),
        4 => range_constructor2_diff(payload),
        5 => range_constructor3_diff(payload),
        6 => range_lower_diff(payload),
        7 => range_upper_diff(payload),
        8 => range_empty_diff(payload),
        9 => range_lower_inc_diff(payload),
        10 => range_upper_inc_diff(payload),
        11 => range_lower_inf_diff(payload),
        12 => range_upper_inf_diff(payload),
        13 => range_adjacent_diff(payload),
        14 => range_overleft_diff(payload),
        15 => range_overright_diff(payload),
        16 => range_union_diff(payload),
        17 => range_intersect_diff(payload),
        18 => range_minus_diff(payload),
        19 => range_merge_diff(payload),
        20 => hash_range_diff(payload),
        21 => hash_range_extended_diff(payload),
        22 => int4range_canonical_diff(payload),
        23 => int8range_canonical_diff(payload),
        24 => daterange_canonical_diff(payload),
        25 => int4range_subdiff_diff(payload),
        26 => int8range_subdiff_diff(payload),
        27 => numrange_subdiff_diff(payload),
        28 => daterange_subdiff_diff(payload),
        29 => tsrange_subdiff_diff(payload),
        _ => tstzrange_subdiff_diff(payload),
    }
}

// ---------------------------------------------------------------------------
// Arm: range_in (oid 3834; C source: rangetypes.c).
// ---------------------------------------------------------------------------

fn range_in_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (rangetypes_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_rangetypes_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_range_in(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_rangetypes::range_in(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_rangetypes::builtins::fc_range_in via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(rangetypes_diff): range_in arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: range_out (oid 3835; C source: rangetypes.c).
// ---------------------------------------------------------------------------

fn range_out_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (rangetypes_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_rangetypes_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_range_out(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_rangetypes::range_out(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_rangetypes::builtins::fc_range_out via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(rangetypes_diff): range_out arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: range_recv (oid 3836; C source: rangetypes.c).
// ---------------------------------------------------------------------------

fn range_recv_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (rangetypes_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_rangetypes_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_range_recv(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_rangetypes::range_recv(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_rangetypes::builtins::fc_range_recv via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(rangetypes_diff): range_recv arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: range_send (oid 3837; C source: rangetypes.c).
// ---------------------------------------------------------------------------

fn range_send_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (rangetypes_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_rangetypes_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_range_send(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_rangetypes::range_send(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_rangetypes::builtins::fc_range_send via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(rangetypes_diff): range_send arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: range_constructor2 (oid 3840; C source: rangetypes.c).
// ---------------------------------------------------------------------------

fn range_constructor2_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (rangetypes_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_rangetypes_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_range_constructor2(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_rangetypes::range_constructor2(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_rangetypes::builtins::fc_range_constructor2 via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(rangetypes_diff): range_constructor2 arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: range_constructor3 (oid 3841; C source: rangetypes.c).
// ---------------------------------------------------------------------------

fn range_constructor3_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (rangetypes_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_rangetypes_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_range_constructor3(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_rangetypes::range_constructor3(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_rangetypes::builtins::fc_range_constructor3 via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(rangetypes_diff): range_constructor3 arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: range_lower (oid 3848; C source: rangetypes.c).
// ---------------------------------------------------------------------------

fn range_lower_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (rangetypes_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_rangetypes_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_range_lower(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_rangetypes::range_lower(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_rangetypes::builtins::fc_range_lower via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(rangetypes_diff): range_lower arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: range_upper (oid 3849; C source: rangetypes.c).
// ---------------------------------------------------------------------------

fn range_upper_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (rangetypes_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_rangetypes_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_range_upper(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_rangetypes::range_upper(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_rangetypes::builtins::fc_range_upper via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(rangetypes_diff): range_upper arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: range_empty (oid 3850; C source: rangetypes.c).
// ---------------------------------------------------------------------------

fn range_empty_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (rangetypes_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_rangetypes_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_range_empty(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_rangetypes::range_empty(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_rangetypes::builtins::fc_range_empty via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(rangetypes_diff): range_empty arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: range_lower_inc (oid 3851; C source: rangetypes.c).
// ---------------------------------------------------------------------------

fn range_lower_inc_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (rangetypes_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_rangetypes_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_range_lower_inc(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_rangetypes::range_lower_inc(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_rangetypes::builtins::fc_range_lower_inc via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(rangetypes_diff): range_lower_inc arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: range_upper_inc (oid 3852; C source: rangetypes.c).
// ---------------------------------------------------------------------------

fn range_upper_inc_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (rangetypes_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_rangetypes_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_range_upper_inc(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_rangetypes::range_upper_inc(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_rangetypes::builtins::fc_range_upper_inc via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(rangetypes_diff): range_upper_inc arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: range_lower_inf (oid 3853; C source: rangetypes.c).
// ---------------------------------------------------------------------------

fn range_lower_inf_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (rangetypes_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_rangetypes_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_range_lower_inf(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_rangetypes::range_lower_inf(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_rangetypes::builtins::fc_range_lower_inf via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(rangetypes_diff): range_lower_inf arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: range_upper_inf (oid 3854; C source: rangetypes.c).
// ---------------------------------------------------------------------------

fn range_upper_inf_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (rangetypes_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_rangetypes_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_range_upper_inf(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_rangetypes::range_upper_inf(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_rangetypes::builtins::fc_range_upper_inf via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(rangetypes_diff): range_upper_inf arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: range_adjacent (oid 3862; C source: rangetypes.c).
// ---------------------------------------------------------------------------

fn range_adjacent_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (rangetypes_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_rangetypes_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_range_adjacent(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_rangetypes::range_adjacent(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_rangetypes::builtins::fc_range_adjacent via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(rangetypes_diff): range_adjacent arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: range_overleft (oid 3865; C source: rangetypes.c).
// ---------------------------------------------------------------------------

fn range_overleft_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (rangetypes_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_rangetypes_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_range_overleft(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_rangetypes::range_overleft(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_rangetypes::builtins::fc_range_overleft via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(rangetypes_diff): range_overleft arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: range_overright (oid 3866; C source: rangetypes.c).
// ---------------------------------------------------------------------------

fn range_overright_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (rangetypes_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_rangetypes_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_range_overright(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_rangetypes::range_overright(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_rangetypes::builtins::fc_range_overright via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(rangetypes_diff): range_overright arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: range_union (oid 3867; C source: rangetypes.c).
// ---------------------------------------------------------------------------

fn range_union_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (rangetypes_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_rangetypes_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_range_union(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_rangetypes::range_union(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_rangetypes::builtins::fc_range_union via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(rangetypes_diff): range_union arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: range_intersect (oid 3868; C source: rangetypes.c).
// ---------------------------------------------------------------------------

fn range_intersect_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (rangetypes_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_rangetypes_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_range_intersect(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_rangetypes::range_intersect(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_rangetypes::builtins::fc_range_intersect via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(rangetypes_diff): range_intersect arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: range_minus (oid 3869; C source: rangetypes.c).
// ---------------------------------------------------------------------------

fn range_minus_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (rangetypes_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_rangetypes_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_range_minus(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_rangetypes::range_minus(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_rangetypes::builtins::fc_range_minus via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(rangetypes_diff): range_minus arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: range_merge (oid 4057; C source: rangetypes.c).
// ---------------------------------------------------------------------------

fn range_merge_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (rangetypes_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_rangetypes_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_range_merge(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_rangetypes::range_merge(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_rangetypes::builtins::fc_range_merge via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(rangetypes_diff): range_merge arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: hash_range (oid 3902; C source: rangetypes.c).
// ---------------------------------------------------------------------------

fn hash_range_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (rangetypes_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_rangetypes_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_hash_range(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_rangetypes::hash_range(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_rangetypes::builtins::fc_hash_range via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(rangetypes_diff): hash_range arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: hash_range_extended (oid 3417; C source: rangetypes.c).
// ---------------------------------------------------------------------------

fn hash_range_extended_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (rangetypes_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_rangetypes_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_hash_range_extended(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_rangetypes::hash_range_extended(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_rangetypes::builtins::fc_hash_range_extended via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(rangetypes_diff): hash_range_extended arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: int4range_canonical (oid 3914; C source: rangetypes.c).
// ---------------------------------------------------------------------------

fn int4range_canonical_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (rangetypes_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_rangetypes_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_int4range_canonical(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_rangetypes::int4range_canonical(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_rangetypes::builtins::fc_int4range_canonical via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(rangetypes_diff): int4range_canonical arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: int8range_canonical (oid 3928; C source: rangetypes.c).
// ---------------------------------------------------------------------------

fn int8range_canonical_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (rangetypes_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_rangetypes_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_int8range_canonical(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_rangetypes::int8range_canonical(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_rangetypes::builtins::fc_int8range_canonical via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(rangetypes_diff): int8range_canonical arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: daterange_canonical (oid 3915; C source: rangetypes.c).
// ---------------------------------------------------------------------------

fn daterange_canonical_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (rangetypes_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_rangetypes_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_daterange_canonical(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_rangetypes::daterange_canonical(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_rangetypes::builtins::fc_daterange_canonical via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(rangetypes_diff): daterange_canonical arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: int4range_subdiff (oid 3922; C source: rangetypes.c).
// ---------------------------------------------------------------------------

fn int4range_subdiff_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (rangetypes_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_rangetypes_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_int4range_subdiff(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_rangetypes::int4range_subdiff(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_rangetypes::builtins::fc_int4range_subdiff via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(rangetypes_diff): int4range_subdiff arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: int8range_subdiff (oid 3923; C source: rangetypes.c).
// ---------------------------------------------------------------------------

fn int8range_subdiff_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (rangetypes_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_rangetypes_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_int8range_subdiff(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_rangetypes::int8range_subdiff(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_rangetypes::builtins::fc_int8range_subdiff via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(rangetypes_diff): int8range_subdiff arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: numrange_subdiff (oid 3924; C source: rangetypes.c).
// ---------------------------------------------------------------------------

fn numrange_subdiff_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (rangetypes_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_rangetypes_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_numrange_subdiff(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_rangetypes::numrange_subdiff(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_rangetypes::builtins::fc_numrange_subdiff via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(rangetypes_diff): numrange_subdiff arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: daterange_subdiff (oid 3925; C source: rangetypes.c).
// ---------------------------------------------------------------------------

fn daterange_subdiff_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (rangetypes_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_rangetypes_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_daterange_subdiff(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_rangetypes::daterange_subdiff(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_rangetypes::builtins::fc_daterange_subdiff via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(rangetypes_diff): daterange_subdiff arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: tsrange_subdiff (oid 3929; C source: rangetypes.c).
// ---------------------------------------------------------------------------

fn tsrange_subdiff_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (rangetypes_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_rangetypes_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_tsrange_subdiff(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_rangetypes::tsrange_subdiff(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_rangetypes::builtins::fc_tsrange_subdiff via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(rangetypes_diff): tsrange_subdiff arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: tstzrange_subdiff (oid 3930; C source: rangetypes.c).
// ---------------------------------------------------------------------------

fn tstzrange_subdiff_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (rangetypes_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_rangetypes_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_tstzrange_subdiff(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_rangetypes::tstzrange_subdiff(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_rangetypes::builtins::fc_tstzrange_subdiff via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(rangetypes_diff): tstzrange_subdiff arm not implemented");
}

// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Replay every checked-in seed (catches shim/link errors before the
    /// nightly fuzz campaign). TODO(scaffold): un-ignore once the arms are
    /// implemented and ../corpus/rangetypes_diff/ is seeded (>=30 seeds; corpora
    /// are COMMITTED — plain `git add`, no -f needed).
    #[test]
    #[ignore = "scaffold(rangetypes_diff): arms not implemented yet"]
    fn seed_corpus_replays_clean() {
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/rangetypes_diff");
        let mut n = 0;
        for e in std::fs::read_dir(dir).expect("corpus/rangetypes_diff missing") {
            let p = e.unwrap().path();
            if p.is_file() {
                rangetypes_diff(&std::fs::read(&p).unwrap());
                n += 1;
            }
        }
        assert!(n >= 30, "expected >=30 seeds, found {n}");
    }

    /// TODO(scaffold): per-arm smoke tests on stable (ok + error shapes per
    /// arm, fc-plane smoke driving every wrapper at least once — see
    /// uuid_diff.rs tests for the expected shape). Start by un-ignoring:
    #[test]
    #[ignore = "scaffold(rangetypes_diff): arms not implemented yet"]
    fn arms_smoke() {
        // Arm 0 example: selector byte 0, then a payload for range_in.
        rangetypes_diff(&[0u8]);
    }
}
