//! regexp_diff: differential fuzz driver — shipped Rust `adt_regexp` vs vendored
//! PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) C
//! (csrc/pg_regexp_io.c). Crate under test: crates/backend/utils/adt/regexp.
//!
//! GENERATED SKELETON (fuzz/scaffold.py) — every TODO(scaffold) below is
//! hand-work; see fuzz/README-TODO-regexp_diff.md for the ordered checklist.
//!
//! Comparison planes (float_in_diff conventions): value bytes/bits,
//! error-verdict, and errcode/sqlstate class. Message text is out of scope.
//!
//! Input layout: [selector][payload]; selector % 19 picks the arm:
//!   0 textregexeq  (oid 1254, C: regexp.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   1 textregexne  (oid 1256, C: regexp.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   2 texticregexeq  (oid 1238, C: regexp.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   3 texticregexne  (oid 1239, C: regexp.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   4 nameregexeq  (oid 79, C: regexp.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   5 nameregexne  (oid 1252, C: regexp.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   6 similar_escape  (oid 1623, C: regexp.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   7 similar_to_escape_1  (oid 1987, C: regexp.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   8 similar_to_escape_2  (oid 1986, C: regexp.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   9 textregexsubstr  (oid 2073, C: regexp.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   10 textregexreplace_noopt  (oid 2284, C: regexp.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   11 textregexreplace  (oid 2285, C: regexp.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   12 regexp_count  (oid 6256, C: regexp.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   13 regexp_instr  (oid 6262, C: regexp.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   14 regexp_like  (oid 6264, C: regexp.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   15 regexp_substr  (oid 6269, C: regexp.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   16 regexp_match  (oid 3397, C: regexp.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   17 regexp_split_to_array  (oid 2768, C: regexp.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   18 textregexreplace_extended  (oid 6251, C: regexp.c) — TODO(scaffold): document
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
    // in csrc/pg_regexp_io.c (declarations are link-inert until called, so
    // `cargo check` and `cargo test` stay green while sites are unfilled):
    // TODO(scaffold): fn pg_diff_textregexeq(...) -> i32;   [oid 1254, regexp.c]
    // TODO(scaffold): fn pg_diff_textregexne(...) -> i32;   [oid 1256, regexp.c]
    // TODO(scaffold): fn pg_diff_texticregexeq(...) -> i32;   [oid 1238, regexp.c]
    // TODO(scaffold): fn pg_diff_texticregexne(...) -> i32;   [oid 1239, regexp.c]
    // TODO(scaffold): fn pg_diff_nameregexeq(...) -> i32;   [oid 79, regexp.c]
    // TODO(scaffold): fn pg_diff_nameregexne(...) -> i32;   [oid 1252, regexp.c]
    // TODO(scaffold): fn pg_diff_similar_escape(...) -> i32;   [oid 1623, regexp.c]
    // TODO(scaffold): fn pg_diff_similar_to_escape_1(...) -> i32;   [oid 1987, regexp.c]
    // TODO(scaffold): fn pg_diff_similar_to_escape_2(...) -> i32;   [oid 1986, regexp.c]
    // TODO(scaffold): fn pg_diff_textregexsubstr(...) -> i32;   [oid 2073, regexp.c]
    // TODO(scaffold): fn pg_diff_textregexreplace_noopt(...) -> i32;   [oid 2284, regexp.c]
    // TODO(scaffold): fn pg_diff_textregexreplace(...) -> i32;   [oid 2285, regexp.c]
    // TODO(scaffold): fn pg_diff_regexp_count(...) -> i32;   [oid 6256, regexp.c]
    // TODO(scaffold): fn pg_diff_regexp_instr(...) -> i32;   [oid 6262, regexp.c]
    // TODO(scaffold): fn pg_diff_regexp_like(...) -> i32;   [oid 6264, regexp.c]
    // TODO(scaffold): fn pg_diff_regexp_substr(...) -> i32;   [oid 6269, regexp.c]
    // TODO(scaffold): fn pg_diff_regexp_match(...) -> i32;   [oid 3397, regexp.c]
    // TODO(scaffold): fn pg_diff_regexp_split_to_array(...) -> i32;   [oid 2768, regexp.c]
    // TODO(scaffold): fn pg_diff_textregexreplace_extended(...) -> i32;   [oid 6251, regexp.c]
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

pub fn regexp_diff(data: &[u8]) {
    let Some((&sel, payload)) = data.split_first() else {
        return;
    };
    match sel % 19 {
        0 => textregexeq_diff(payload),
        1 => textregexne_diff(payload),
        2 => texticregexeq_diff(payload),
        3 => texticregexne_diff(payload),
        4 => nameregexeq_diff(payload),
        5 => nameregexne_diff(payload),
        6 => similar_escape_diff(payload),
        7 => similar_to_escape_1_diff(payload),
        8 => similar_to_escape_2_diff(payload),
        9 => textregexsubstr_diff(payload),
        10 => textregexreplace_noopt_diff(payload),
        11 => textregexreplace_diff(payload),
        12 => regexp_count_diff(payload),
        13 => regexp_instr_diff(payload),
        14 => regexp_like_diff(payload),
        15 => regexp_substr_diff(payload),
        16 => regexp_match_diff(payload),
        17 => regexp_split_to_array_diff(payload),
        _ => textregexreplace_extended_diff(payload),
    }
}

// ---------------------------------------------------------------------------
// Arm: textregexeq (oid 1254; C source: regexp.c).
// ---------------------------------------------------------------------------

fn textregexeq_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (regexp_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_regexp_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_textregexeq(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_regexp::textregexeq(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_regexp::builtins::fc_textregexeq via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(regexp_diff): textregexeq arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: textregexne (oid 1256; C source: regexp.c).
// ---------------------------------------------------------------------------

fn textregexne_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (regexp_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_regexp_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_textregexne(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_regexp::textregexne(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_regexp::builtins::fc_textregexne via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(regexp_diff): textregexne arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: texticregexeq (oid 1238; C source: regexp.c).
// ---------------------------------------------------------------------------

fn texticregexeq_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (regexp_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_regexp_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_texticregexeq(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_regexp::texticregexeq(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_regexp::builtins::fc_texticregexeq via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(regexp_diff): texticregexeq arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: texticregexne (oid 1239; C source: regexp.c).
// ---------------------------------------------------------------------------

fn texticregexne_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (regexp_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_regexp_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_texticregexne(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_regexp::texticregexne(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_regexp::builtins::fc_texticregexne via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(regexp_diff): texticregexne arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: nameregexeq (oid 79; C source: regexp.c).
// ---------------------------------------------------------------------------

fn nameregexeq_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (regexp_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_regexp_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_nameregexeq(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_regexp::nameregexeq(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_regexp::builtins::fc_nameregexeq via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(regexp_diff): nameregexeq arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: nameregexne (oid 1252; C source: regexp.c).
// ---------------------------------------------------------------------------

fn nameregexne_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (regexp_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_regexp_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_nameregexne(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_regexp::nameregexne(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_regexp::builtins::fc_nameregexne via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(regexp_diff): nameregexne arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: similar_escape (oid 1623; C source: regexp.c).
// ---------------------------------------------------------------------------

fn similar_escape_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (regexp_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_regexp_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_similar_escape(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_regexp::similar_escape(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_regexp::builtins::fc_similar_escape via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(regexp_diff): similar_escape arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: similar_to_escape_1 (oid 1987; C source: regexp.c).
// ---------------------------------------------------------------------------

fn similar_to_escape_1_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (regexp_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_regexp_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_similar_to_escape_1(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_regexp::similar_to_escape_1(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_regexp::builtins::fc_similar_to_escape_1 via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(regexp_diff): similar_to_escape_1 arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: similar_to_escape_2 (oid 1986; C source: regexp.c).
// ---------------------------------------------------------------------------

fn similar_to_escape_2_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (regexp_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_regexp_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_similar_to_escape_2(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_regexp::similar_to_escape_2(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_regexp::builtins::fc_similar_to_escape_2 via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(regexp_diff): similar_to_escape_2 arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: textregexsubstr (oid 2073; C source: regexp.c).
// ---------------------------------------------------------------------------

fn textregexsubstr_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (regexp_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_regexp_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_textregexsubstr(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_regexp::textregexsubstr(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_regexp::builtins::fc_textregexsubstr via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(regexp_diff): textregexsubstr arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: textregexreplace_noopt (oid 2284; C source: regexp.c).
// ---------------------------------------------------------------------------

fn textregexreplace_noopt_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (regexp_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_regexp_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_textregexreplace_noopt(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_regexp::textregexreplace_noopt(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_regexp::builtins::fc_textregexreplace_noopt via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(regexp_diff): textregexreplace_noopt arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: textregexreplace (oid 2285; C source: regexp.c).
// ---------------------------------------------------------------------------

fn textregexreplace_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (regexp_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_regexp_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_textregexreplace(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_regexp::textregexreplace(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_regexp::builtins::fc_textregexreplace via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(regexp_diff): textregexreplace arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: regexp_count (oid 6256; C source: regexp.c).
// ---------------------------------------------------------------------------

fn regexp_count_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (regexp_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_regexp_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_regexp_count(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_regexp::regexp_count(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_regexp::builtins::fc_regexp_count via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(regexp_diff): regexp_count arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: regexp_instr (oid 6262; C source: regexp.c).
// ---------------------------------------------------------------------------

fn regexp_instr_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (regexp_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_regexp_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_regexp_instr(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_regexp::regexp_instr(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_regexp::builtins::fc_regexp_instr via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(regexp_diff): regexp_instr arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: regexp_like (oid 6264; C source: regexp.c).
// ---------------------------------------------------------------------------

fn regexp_like_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (regexp_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_regexp_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_regexp_like(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_regexp::regexp_like(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_regexp::builtins::fc_regexp_like via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(regexp_diff): regexp_like arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: regexp_substr (oid 6269; C source: regexp.c).
// ---------------------------------------------------------------------------

fn regexp_substr_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (regexp_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_regexp_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_regexp_substr(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_regexp::regexp_substr(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_regexp::builtins::fc_regexp_substr via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(regexp_diff): regexp_substr arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: regexp_match (oid 3397; C source: regexp.c).
// ---------------------------------------------------------------------------

fn regexp_match_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (regexp_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_regexp_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_regexp_match(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_regexp::regexp_match(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_regexp::builtins::fc_regexp_match via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(regexp_diff): regexp_match arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: regexp_split_to_array (oid 2768; C source: regexp.c).
// ---------------------------------------------------------------------------

fn regexp_split_to_array_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (regexp_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_regexp_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_regexp_split_to_array(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_regexp::regexp_split_to_array(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_regexp::builtins::fc_regexp_split_to_array via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(regexp_diff): regexp_split_to_array arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: textregexreplace_extended (oid 6251; C source: regexp.c).
// ---------------------------------------------------------------------------

fn textregexreplace_extended_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (regexp_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_regexp_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_textregexreplace_extended(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_regexp::textregexreplace_extended(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_regexp::builtins::fc_textregexreplace_extended via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(regexp_diff): textregexreplace_extended arm not implemented");
}

// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Replay every checked-in seed (catches shim/link errors before the
    /// nightly fuzz campaign). TODO(scaffold): un-ignore once the arms are
    /// implemented and ../corpus/regexp_diff/ is seeded (>=30 seeds; corpora
    /// are COMMITTED — plain `git add`, no -f needed).
    #[test]
    #[ignore = "scaffold(regexp_diff): arms not implemented yet"]
    fn seed_corpus_replays_clean() {
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/regexp_diff");
        let mut n = 0;
        for e in std::fs::read_dir(dir).expect("corpus/regexp_diff missing") {
            let p = e.unwrap().path();
            if p.is_file() {
                regexp_diff(&std::fs::read(&p).unwrap());
                n += 1;
            }
        }
        assert!(n >= 30, "expected >=30 seeds, found {n}");
    }

    /// TODO(scaffold): per-arm smoke tests on stable (ok + error shapes per
    /// arm, fc-plane smoke driving every wrapper at least once — see
    /// uuid_diff.rs tests for the expected shape). Start by un-ignoring:
    #[test]
    #[ignore = "scaffold(regexp_diff): arms not implemented yet"]
    fn arms_smoke() {
        // Arm 0 example: selector byte 0, then a payload for textregexeq.
        regexp_diff(&[0u8]);
    }
}
