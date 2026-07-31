//! like_diff: differential fuzz driver — shipped Rust `adt_like` vs vendored
//! PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) C
//! (csrc/pg_like_io.c). Crate under test: crates/backend/utils/adt/like.
//!
//! GENERATED SKELETON (fuzz/scaffold.py) — every TODO(scaffold) below is
//! hand-work; see fuzz/README-TODO-like_diff.md for the ordered checklist.
//!
//! Comparison planes (float_in_diff conventions): value bytes/bits,
//! error-verdict, and errcode/sqlstate class. Message text is out of scope.
//!
//! Input layout: [selector][payload]; selector % 12 picks the arm:
//!   0 textlike  (oid 850, C: like.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   1 textnlike  (oid 851, C: like.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   2 namelike  (oid 858, C: like.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   3 namenlike  (oid 859, C: like.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   4 texticlike  (oid 1633, C: like.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   5 texticnlike  (oid 1634, C: like.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   6 nameiclike  (oid 1635, C: like.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   7 nameicnlike  (oid 1636, C: like.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   8 like_escape  (oid 1637, C: like.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   9 bytealike  (oid 2005, C: like.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   10 byteanlike  (oid 2006, C: like.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   11 like_escape_bytea  (oid 2009, C: like.c) — TODO(scaffold): document
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
    // in csrc/pg_like_io.c (declarations are link-inert until called, so
    // `cargo check` and `cargo test` stay green while sites are unfilled):
    // TODO(scaffold): fn pg_diff_textlike(...) -> i32;   [oid 850, like.c]
    // TODO(scaffold): fn pg_diff_textnlike(...) -> i32;   [oid 851, like.c]
    // TODO(scaffold): fn pg_diff_namelike(...) -> i32;   [oid 858, like.c]
    // TODO(scaffold): fn pg_diff_namenlike(...) -> i32;   [oid 859, like.c]
    // TODO(scaffold): fn pg_diff_texticlike(...) -> i32;   [oid 1633, like.c]
    // TODO(scaffold): fn pg_diff_texticnlike(...) -> i32;   [oid 1634, like.c]
    // TODO(scaffold): fn pg_diff_nameiclike(...) -> i32;   [oid 1635, like.c]
    // TODO(scaffold): fn pg_diff_nameicnlike(...) -> i32;   [oid 1636, like.c]
    // TODO(scaffold): fn pg_diff_like_escape(...) -> i32;   [oid 1637, like.c]
    // TODO(scaffold): fn pg_diff_bytealike(...) -> i32;   [oid 2005, like.c]
    // TODO(scaffold): fn pg_diff_byteanlike(...) -> i32;   [oid 2006, like.c]
    // TODO(scaffold): fn pg_diff_like_escape_bytea(...) -> i32;   [oid 2009, like.c]
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

pub fn like_diff(data: &[u8]) {
    let Some((&sel, payload)) = data.split_first() else {
        return;
    };
    match sel % 12 {
        0 => textlike_diff(payload),
        1 => textnlike_diff(payload),
        2 => namelike_diff(payload),
        3 => namenlike_diff(payload),
        4 => texticlike_diff(payload),
        5 => texticnlike_diff(payload),
        6 => nameiclike_diff(payload),
        7 => nameicnlike_diff(payload),
        8 => like_escape_diff(payload),
        9 => bytealike_diff(payload),
        10 => byteanlike_diff(payload),
        _ => like_escape_bytea_diff(payload),
    }
}

// ---------------------------------------------------------------------------
// Arm: textlike (oid 850; C source: like.c).
// ---------------------------------------------------------------------------

fn textlike_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (like_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_like_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_textlike(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_like::textlike(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_like::builtins::fc_textlike via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(like_diff): textlike arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: textnlike (oid 851; C source: like.c).
// ---------------------------------------------------------------------------

fn textnlike_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (like_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_like_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_textnlike(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_like::textnlike(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_like::builtins::fc_textnlike via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(like_diff): textnlike arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: namelike (oid 858; C source: like.c).
// ---------------------------------------------------------------------------

fn namelike_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (like_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_like_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_namelike(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_like::namelike(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_like::builtins::fc_namelike via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(like_diff): namelike arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: namenlike (oid 859; C source: like.c).
// ---------------------------------------------------------------------------

fn namenlike_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (like_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_like_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_namenlike(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_like::namenlike(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_like::builtins::fc_namenlike via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(like_diff): namenlike arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: texticlike (oid 1633; C source: like.c).
// ---------------------------------------------------------------------------

fn texticlike_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (like_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_like_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_texticlike(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_like::texticlike(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_like::builtins::fc_texticlike via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(like_diff): texticlike arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: texticnlike (oid 1634; C source: like.c).
// ---------------------------------------------------------------------------

fn texticnlike_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (like_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_like_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_texticnlike(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_like::texticnlike(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_like::builtins::fc_texticnlike via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(like_diff): texticnlike arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: nameiclike (oid 1635; C source: like.c).
// ---------------------------------------------------------------------------

fn nameiclike_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (like_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_like_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_nameiclike(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_like::nameiclike(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_like::builtins::fc_nameiclike via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(like_diff): nameiclike arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: nameicnlike (oid 1636; C source: like.c).
// ---------------------------------------------------------------------------

fn nameicnlike_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (like_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_like_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_nameicnlike(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_like::nameicnlike(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_like::builtins::fc_nameicnlike via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(like_diff): nameicnlike arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: like_escape (oid 1637; C source: like.c).
// ---------------------------------------------------------------------------

fn like_escape_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (like_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_like_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_like_escape(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_like::like_escape(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_like::builtins::fc_like_escape via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(like_diff): like_escape arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: bytealike (oid 2005; C source: like.c).
// ---------------------------------------------------------------------------

fn bytealike_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (like_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_like_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_bytealike(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_like::bytealike(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_like::builtins::fc_bytealike via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(like_diff): bytealike arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: byteanlike (oid 2006; C source: like.c).
// ---------------------------------------------------------------------------

fn byteanlike_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (like_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_like_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_byteanlike(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_like::byteanlike(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_like::builtins::fc_byteanlike via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(like_diff): byteanlike arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: like_escape_bytea (oid 2009; C source: like.c).
// ---------------------------------------------------------------------------

fn like_escape_bytea_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (like_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_like_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_like_escape_bytea(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: adt_like::like_escape_bytea(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      adt_like::builtins::fc_like_escape_bytea via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(like_diff): like_escape_bytea arm not implemented");
}

// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Replay every checked-in seed (catches shim/link errors before the
    /// nightly fuzz campaign). TODO(scaffold): un-ignore once the arms are
    /// implemented and ../corpus/like_diff/ is seeded (>=30 seeds; corpora
    /// are COMMITTED — plain `git add`, no -f needed).
    #[test]
    #[ignore = "scaffold(like_diff): arms not implemented yet"]
    fn seed_corpus_replays_clean() {
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/like_diff");
        let mut n = 0;
        for e in std::fs::read_dir(dir).expect("corpus/like_diff missing") {
            let p = e.unwrap().path();
            if p.is_file() {
                like_diff(&std::fs::read(&p).unwrap());
                n += 1;
            }
        }
        assert!(n >= 30, "expected >=30 seeds, found {n}");
    }

    /// TODO(scaffold): per-arm smoke tests on stable (ok + error shapes per
    /// arm, fc-plane smoke driving every wrapper at least once — see
    /// uuid_diff.rs tests for the expected shape). Start by un-ignoring:
    #[test]
    #[ignore = "scaffold(like_diff): arms not implemented yet"]
    fn arms_smoke() {
        // Arm 0 example: selector byte 0, then a payload for textlike.
        like_diff(&[0u8]);
    }
}
