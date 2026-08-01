//! trgm_diff: differential fuzz driver — shipped Rust `pg_trgm` vs vendored
//! PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) C
//! (csrc/pg_trgm_io.c). Crate under test: crates/contrib/pg_trgm.
//!
//! GENERATED SKELETON (fuzz/scaffold.py) — every TODO(scaffold) below is
//! hand-work; see fuzz/README-TODO-trgm_diff.md for the ordered checklist.
//!
//! Comparison planes (float_in_diff conventions): value bytes/bits,
//! error-verdict, and errcode/sqlstate class. Message text is out of scope.
//!
//! Input layout: [selector][payload]; selector % 6 picks the arm:
//!   0 similarity  (oid 0, C: trgm_op.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   1 word_similarity  (oid 0, C: trgm_op.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   2 strict_word_similarity  (oid 0, C: trgm_op.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   3 show_trgm  (oid 0, C: trgm_op.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   4 generate_wildcard_trgm  (oid 0, C: trgm_op.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   5 createTrgmNFA  (oid 0, C: trgm_regexp.c) — TODO(scaffold): document
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
    // in csrc/pg_trgm_io.c (declarations are link-inert until called, so
    // `cargo check` and `cargo test` stay green while sites are unfilled):
    // TODO(scaffold): fn pg_diff_similarity(...) -> i32;   [oid 0, trgm_op.c]
    // TODO(scaffold): fn pg_diff_word_similarity(...) -> i32;   [oid 0, trgm_op.c]
    // TODO(scaffold): fn pg_diff_strict_word_similarity(...) -> i32;   [oid 0, trgm_op.c]
    // TODO(scaffold): fn pg_diff_show_trgm(...) -> i32;   [oid 0, trgm_op.c]
    // TODO(scaffold): fn pg_diff_generate_wildcard_trgm(...) -> i32;   [oid 0, trgm_op.c]
    // TODO(scaffold): fn pg_diff_createTrgmNFA(...) -> i32;   [oid 0, trgm_regexp.c]
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

pub fn trgm_diff(data: &[u8]) {
    let Some((&sel, payload)) = data.split_first() else {
        return;
    };
    match sel % 6 {
        0 => similarity_diff(payload),
        1 => word_similarity_diff(payload),
        2 => strict_word_similarity_diff(payload),
        3 => show_trgm_diff(payload),
        4 => generate_wildcard_trgm_diff(payload),
        _ => createTrgmNFA_diff(payload),
    }
}

// ---------------------------------------------------------------------------
// Arm: similarity (oid 0; C source: trgm_op.c).
// ---------------------------------------------------------------------------

fn similarity_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (trgm_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_trgm_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_similarity(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: pg_trgm::similarity(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      pg_trgm::builtins::fc_similarity via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(trgm_diff): similarity arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: word_similarity (oid 0; C source: trgm_op.c).
// ---------------------------------------------------------------------------

fn word_similarity_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (trgm_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_trgm_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_word_similarity(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: pg_trgm::word_similarity(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      pg_trgm::builtins::fc_word_similarity via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(trgm_diff): word_similarity arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: strict_word_similarity (oid 0; C source: trgm_op.c).
// ---------------------------------------------------------------------------

fn strict_word_similarity_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (trgm_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_trgm_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_strict_word_similarity(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: pg_trgm::strict_word_similarity(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      pg_trgm::builtins::fc_strict_word_similarity via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(trgm_diff): strict_word_similarity arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: show_trgm (oid 0; C source: trgm_op.c).
// ---------------------------------------------------------------------------

fn show_trgm_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (trgm_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_trgm_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_show_trgm(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: pg_trgm::show_trgm(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      pg_trgm::builtins::fc_show_trgm via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(trgm_diff): show_trgm arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: generate_wildcard_trgm (oid 0; C source: trgm_op.c).
// ---------------------------------------------------------------------------

fn generate_wildcard_trgm_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (trgm_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_trgm_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_generate_wildcard_trgm(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: pg_trgm::generate_wildcard_trgm(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      pg_trgm::builtins::fc_generate_wildcard_trgm via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(trgm_diff): generate_wildcard_trgm arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: createTrgmNFA (oid 0; C source: trgm_regexp.c).
// ---------------------------------------------------------------------------

fn createTrgmNFA_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (trgm_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_trgm_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_createTrgmNFA(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: pg_trgm::createTrgmNFA(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      pg_trgm::builtins::fc_createTrgmNFA via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(trgm_diff): createTrgmNFA arm not implemented");
}

// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Replay every checked-in seed (catches shim/link errors before the
    /// nightly fuzz campaign). TODO(scaffold): un-ignore once the arms are
    /// implemented and ../corpus/trgm_diff/ is seeded (>=30 seeds; corpora
    /// are COMMITTED — plain `git add`, no -f needed).
    #[test]
    #[ignore = "scaffold(trgm_diff): arms not implemented yet"]
    fn seed_corpus_replays_clean() {
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/trgm_diff");
        let mut n = 0;
        for e in std::fs::read_dir(dir).expect("corpus/trgm_diff missing") {
            let p = e.unwrap().path();
            if p.is_file() {
                trgm_diff(&std::fs::read(&p).unwrap());
                n += 1;
            }
        }
        assert!(n >= 30, "expected >=30 seeds, found {n}");
    }

    /// TODO(scaffold): per-arm smoke tests on stable (ok + error shapes per
    /// arm, fc-plane smoke driving every wrapper at least once — see
    /// uuid_diff.rs tests for the expected shape). Start by un-ignoring:
    #[test]
    #[ignore = "scaffold(trgm_diff): arms not implemented yet"]
    fn arms_smoke() {
        // Arm 0 example: selector byte 0, then a payload for similarity.
        trgm_diff(&[0u8]);
    }
}
