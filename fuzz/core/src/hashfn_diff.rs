//! hashfn_diff: differential fuzz driver — shipped Rust `hashfn` vs vendored
//! PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) C
//! (csrc/pg_hashfn_io.c). Crate under test: crates/common/hashfn.
//!
//! GENERATED SKELETON (fuzz/scaffold.py) — every TODO(scaffold) below is
//! hand-work; see fuzz/README-TODO-hashfn_diff.md for the ordered checklist.
//!
//! Comparison planes (float_in_diff conventions): value bytes/bits,
//! error-verdict, and errcode/sqlstate class. Message text is out of scope.
//!
//! Input layout: [selector][payload]; selector % 12 picks the arm:
//!   0 hash_bytes  (oid 0, C: hashfn.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   1 hash_bytes_extended  (oid 0, C: hashfn.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   2 hash_bytes_uint32  (oid 0, C: hashfn.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   3 hash_bytes_uint32_extended  (oid 0, C: hashfn.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   4 string_hash  (oid 0, C: hashfn.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   5 tag_hash  (oid 0, C: hashfn.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   6 uint32_hash  (oid 0, C: hashfn.c) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   7 hash_combine  (oid 0, C: hashfn.h) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   8 hash_combine64  (oid 0, C: hashfn.h) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   9 murmurhash32  (oid 0, C: hashfn.h) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   10 murmurhash64  (oid 0, C: hashfn.h) — TODO(scaffold): document
//!     the payload this arm decodes.
//!   11 rotate_high_and_low_32bits  (oid 0, C: hashfn.h) — TODO(scaffold): document
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
    // in csrc/pg_hashfn_io.c (declarations are link-inert until called, so
    // `cargo check` and `cargo test` stay green while sites are unfilled):
    // TODO(scaffold): fn pg_diff_hash_bytes(...) -> i32;   [oid 0, hashfn.c]
    // TODO(scaffold): fn pg_diff_hash_bytes_extended(...) -> i32;   [oid 0, hashfn.c]
    // TODO(scaffold): fn pg_diff_hash_bytes_uint32(...) -> i32;   [oid 0, hashfn.c]
    // TODO(scaffold): fn pg_diff_hash_bytes_uint32_extended(...) -> i32;   [oid 0, hashfn.c]
    // TODO(scaffold): fn pg_diff_string_hash(...) -> i32;   [oid 0, hashfn.c]
    // TODO(scaffold): fn pg_diff_tag_hash(...) -> i32;   [oid 0, hashfn.c]
    // TODO(scaffold): fn pg_diff_uint32_hash(...) -> i32;   [oid 0, hashfn.c]
    // TODO(scaffold): fn pg_diff_hash_combine(...) -> i32;   [oid 0, hashfn.h]
    // TODO(scaffold): fn pg_diff_hash_combine64(...) -> i32;   [oid 0, hashfn.h]
    // TODO(scaffold): fn pg_diff_murmurhash32(...) -> i32;   [oid 0, hashfn.h]
    // TODO(scaffold): fn pg_diff_murmurhash64(...) -> i32;   [oid 0, hashfn.h]
    // TODO(scaffold): fn pg_diff_rotate_high_and_low_32bits(...) -> i32;   [oid 0, hashfn.h]
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

pub fn hashfn_diff(data: &[u8]) {
    let Some((&sel, payload)) = data.split_first() else {
        return;
    };
    match sel % 12 {
        0 => hash_bytes_diff(payload),
        1 => hash_bytes_extended_diff(payload),
        2 => hash_bytes_uint32_diff(payload),
        3 => hash_bytes_uint32_extended_diff(payload),
        4 => string_hash_diff(payload),
        5 => tag_hash_diff(payload),
        6 => uint32_hash_diff(payload),
        7 => hash_combine_diff(payload),
        8 => hash_combine64_diff(payload),
        9 => murmurhash32_diff(payload),
        10 => murmurhash64_diff(payload),
        _ => rotate_high_and_low_32bits_diff(payload),
    }
}

// ---------------------------------------------------------------------------
// Arm: hash_bytes (oid 0; C source: hashfn.c).
// ---------------------------------------------------------------------------

fn hash_bytes_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (hashfn_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_hashfn_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_hash_bytes(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: hashfn::hash_bytes(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      hashfn::builtins::fc_hash_bytes via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(hashfn_diff): hash_bytes arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: hash_bytes_extended (oid 0; C source: hashfn.c).
// ---------------------------------------------------------------------------

fn hash_bytes_extended_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (hashfn_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_hashfn_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_hash_bytes_extended(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: hashfn::hash_bytes_extended(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      hashfn::builtins::fc_hash_bytes_extended via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(hashfn_diff): hash_bytes_extended arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: hash_bytes_uint32 (oid 0; C source: hashfn.c).
// ---------------------------------------------------------------------------

fn hash_bytes_uint32_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (hashfn_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_hashfn_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_hash_bytes_uint32(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: hashfn::hash_bytes_uint32(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      hashfn::builtins::fc_hash_bytes_uint32 via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(hashfn_diff): hash_bytes_uint32 arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: hash_bytes_uint32_extended (oid 0; C source: hashfn.c).
// ---------------------------------------------------------------------------

fn hash_bytes_uint32_extended_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (hashfn_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_hashfn_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_hash_bytes_uint32_extended(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: hashfn::hash_bytes_uint32_extended(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      hashfn::builtins::fc_hash_bytes_uint32_extended via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(hashfn_diff): hash_bytes_uint32_extended arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: string_hash (oid 0; C source: hashfn.c).
// ---------------------------------------------------------------------------

fn string_hash_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (hashfn_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_hashfn_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_string_hash(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: hashfn::string_hash(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      hashfn::builtins::fc_string_hash via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(hashfn_diff): string_hash arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: tag_hash (oid 0; C source: hashfn.c).
// ---------------------------------------------------------------------------

fn tag_hash_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (hashfn_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_hashfn_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_tag_hash(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: hashfn::tag_hash(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      hashfn::builtins::fc_tag_hash via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(hashfn_diff): tag_hash arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: uint32_hash (oid 0; C source: hashfn.c).
// ---------------------------------------------------------------------------

fn uint32_hash_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (hashfn_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_hashfn_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_uint32_hash(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: hashfn::uint32_hash(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      hashfn::builtins::fc_uint32_hash via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(hashfn_diff): uint32_hash arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: hash_combine (oid 0; C source: hashfn.h).
// ---------------------------------------------------------------------------

fn hash_combine_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (hashfn_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_hashfn_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_hash_combine(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: hashfn::hash_combine(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      hashfn::builtins::fc_hash_combine via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(hashfn_diff): hash_combine arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: hash_combine64 (oid 0; C source: hashfn.h).
// ---------------------------------------------------------------------------

fn hash_combine64_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (hashfn_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_hashfn_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_hash_combine64(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: hashfn::hash_combine64(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      hashfn::builtins::fc_hash_combine64 via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(hashfn_diff): hash_combine64 arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: murmurhash32 (oid 0; C source: hashfn.h).
// ---------------------------------------------------------------------------

fn murmurhash32_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (hashfn_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_hashfn_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_murmurhash32(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: hashfn::murmurhash32(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      hashfn::builtins::fc_murmurhash32 via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(hashfn_diff): murmurhash32 arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: murmurhash64 (oid 0; C source: hashfn.h).
// ---------------------------------------------------------------------------

fn murmurhash64_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (hashfn_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_hashfn_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_murmurhash64(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: hashfn::murmurhash64(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      hashfn::builtins::fc_murmurhash64 via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(hashfn_diff): murmurhash64 arm not implemented");
}

// ---------------------------------------------------------------------------
// Arm: rotate_high_and_low_32bits (oid 0; C source: hashfn.h).
// ---------------------------------------------------------------------------

fn rotate_high_and_low_32bits_diff(payload: &[u8]) {
    let _ = payload;
    // TODO(scaffold): implement this arm (hashfn_diff conventions; copy the
    // shape from uuid_diff.rs / cash_diff.rs in the lane worktrees):
    //   1. C oracle: uncomment/adjust the extern decl above, fill the
    //      csrc/pg_hashfn_io.c paste site, uncomment the build.rs line, then:
    //        let cst = unsafe { pg_diff_rotate_high_and_low_32bits(/* payload views + out bufs */) };
    //        let cerr = unsafe { pg_diff_errcode_get() };
    //   2. Shipped Rust core: hashfn::rotate_high_and_low_32bits(...), then compare ALL planes:
    //        - value plane:    exact result bytes/bits vs the C out-buffer
    //        - verdict plane:  Ok/Err agreement with cst
    //        - sqlstate plane: e.sqlstate vs the oracle errcode class (cerr)
    //      (message text out of scope; document any ratified platform
    //      carve-outs in the module header).
    //   3. fc-wrapper plane: route the same input through
    //      hashfn::builtins::fc_rotate_high_and_low_32bits via fc_call::<N>(..) (helpers above) and
    //      assert wrapper == core (Datum value / returned bytes / error
    //      verdict + sqlstate). Soft-error (ErrorSaveNode) shape too, where
    //      the wrapper takes an escontext.
    todo!("scaffold(hashfn_diff): rotate_high_and_low_32bits arm not implemented");
}

// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Replay every checked-in seed (catches shim/link errors before the
    /// nightly fuzz campaign). TODO(scaffold): un-ignore once the arms are
    /// implemented and ../corpus/hashfn_diff/ is seeded (>=30 seeds; corpora
    /// are COMMITTED — plain `git add`, no -f needed).
    #[test]
    #[ignore = "scaffold(hashfn_diff): arms not implemented yet"]
    fn seed_corpus_replays_clean() {
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/hashfn_diff");
        let mut n = 0;
        for e in std::fs::read_dir(dir).expect("corpus/hashfn_diff missing") {
            let p = e.unwrap().path();
            if p.is_file() {
                hashfn_diff(&std::fs::read(&p).unwrap());
                n += 1;
            }
        }
        assert!(n >= 30, "expected >=30 seeds, found {n}");
    }

    /// TODO(scaffold): per-arm smoke tests on stable (ok + error shapes per
    /// arm, fc-plane smoke driving every wrapper at least once — see
    /// uuid_diff.rs tests for the expected shape). Start by un-ignoring:
    #[test]
    #[ignore = "scaffold(hashfn_diff): arms not implemented yet"]
    fn arms_smoke() {
        // Arm 0 example: selector byte 0, then a payload for hash_bytes.
        hashfn_diff(&[0u8]);
    }
}
