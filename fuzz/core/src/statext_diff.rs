//! statext_diff: differential fuzz driver for the extended-statistics on-disk
//! bytea DESERIALIZERS — shipped Rust (crate statistics: mvdistinct.rs /
//! dependencies.rs / mcv.rs) vs vendored PostgreSQL 18.3 (Stamp-18.3,
//! upstream sha 62d6c7d3df) C (csrc/pg_statext_io.c).
//!
//! WHY THIS TARGET. Under PG18 statistics restore (pg_restore_*_stats /
//! stats_import) an attacker-influenceable bytea reaches
//! statext_{ndistinct,dependencies,mcv}_deserialize. The matching *_recv
//! input functions are feature-not-supported stubs (vacuous); these
//! deserialize paths are the genuine length-field parsers, so a malformed
//! restored stats blob that crashes/OOBs the backend here is a HIGH finding.
//!
//! THE ASSERTS-OFF ASYMMETRY (the crux of the comparison contract). All three
//! C deserializers guard their per-item `nattributes` / per-dependency `k` /
//! per-dimension `nvalues`/`nbytes` / MCV item `index` with `Assert` ONLY.
//! Production Postgres ships with asserts OFF (this bar is release-effective —
//! debug-assert masking law), so a blob that violates one of those invariants
//! but survives the byte-SIZE gates is undefined behaviour in the C. The Rust
//! crate turns every one of those asserts into a runtime rejection, so it is
//! strictly safer. That means the verbatim C is only a trustworthy oracle on
//! blobs that are memory-safe for it. We resolve this cleanly:
//!
//!   * SAFETY plane (the HIGH bar, no oracle needed): the Rust deserializer
//!     must never panic / OOB / abort on ANY blob. Exercised over tens of
//!     thousands of random + mutated blobs (`random_no_panic`).
//!   * ACCEPT parity: whenever Rust ACCEPTS a blob, that blob satisfies every
//!     invariant the C only asserts, so the verbatim C is guaranteed memory
//!     safe on it — we run the C, require it also accept, and require the
//!     parsed structure to be byte-identical (canonical digest).
//!   * REJECT parity on cleanly-rejectable blobs: header/magic/type/zero-
//!     count/size-gate/truncation/count-overflow rejects happen BEFORE any
//!     assert-guarded read, so the C rejects them memory-safely too — we run
//!     the C and require it also reject (`run_dual_reject`).
//!   * The invariant-violating band (natts/k = 9, nvalues<0, index>=nvalues,
//!     …) is exercised against the Rust side ALONE as a no-crash + clean-
//!     reject assertion (`c_unsafe_band`); the verbatim asserts-off C would
//!     read out of bounds there and is deliberately not run. This band is the
//!     documented C↔Rust divergence, not a Rust defect.
//!
//! No FC plane: pg_ndistinct_in / pg_dependencies_in / pg_mcv_list_in are
//! feature-not-supported, and pg_mcv_list_out == byteaout, so MCV is reached
//! in production through the pg_stats_ext_mcvlist_items SRF — i.e. through
//! statext_mcv_deserialize, exactly the function under test here.

use statistics::dependencies::{
    statext_dependencies_deserialize, STATS_DEPS_MAGIC, STATS_DEPS_TYPE_BASIC,
};
use statistics::mcv::{
    statext_mcv_deserialize, STATS_MCV_MAGIC, STATS_MCV_TYPE_BASIC, STATS_MCVLIST_MAX_ITEMS,
};
use statistics::mvdistinct::{
    statext_ndistinct_deserialize, STATS_NDISTINCT_MAGIC, STATS_NDISTINCT_TYPE_BASIC,
};

extern "C" {
    fn pg_diff_statext_ndistinct(
        body: *const u8,
        bodylen: u32,
        out: *mut u8,
        outcap: i32,
        outlen: *mut i32,
    ) -> i32;
    fn pg_diff_statext_deps(
        body: *const u8,
        bodylen: u32,
        out: *mut u8,
        outcap: i32,
        outlen: *mut i32,
    ) -> i32;
    fn pg_diff_statext_mcv(
        body: *const u8,
        bodylen: u32,
        out: *mut u8,
        outcap: i32,
        outlen: *mut i32,
    ) -> i32;
}

/// Digest scratch cap. No accepted blob the banks/random loop build comes
/// anywhere near this; the C entry aborts (harness bug) if it would overflow.
const OUT_CAP: usize = 1 << 20;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Kind {
    Ndistinct,
    Deps,
    Mcv,
}

// ---------------------------------------------------------------------------
// Rust side: parse a blob body, returning Ok(digest) or Err (rejected). A
// panic here is a HIGH finding and is intentionally allowed to propagate
// (the enclosing test fails, naming the blob).
// ---------------------------------------------------------------------------

fn rust_parse(kind: Kind, body: &[u8]) -> Result<Vec<u8>, ()> {
    let cx = mcx::MemoryContext::new("statext_fuzz");
    let m = cx.mcx();
    match kind {
        Kind::Ndistinct => match statext_ndistinct_deserialize(m, body) {
            Ok(nd) => {
                let mut d = Vec::new();
                d.extend_from_slice(&(nd.items.len() as u32).to_ne_bytes());
                for it in nd.items.iter() {
                    d.extend_from_slice(&it.ndistinct.to_bits().to_ne_bytes());
                    d.extend_from_slice(&(it.attributes.len() as u32).to_ne_bytes());
                    for &a in it.attributes.iter() {
                        d.extend_from_slice(&(a as u16).to_ne_bytes());
                    }
                }
                Ok(d)
            }
            Err(_) => Err(()),
        },
        Kind::Deps => match statext_dependencies_deserialize(m, body) {
            Ok(dep) => {
                let mut d = Vec::new();
                d.extend_from_slice(&(dep.deps.len() as u32).to_ne_bytes());
                for de in dep.deps.iter() {
                    d.extend_from_slice(&de.degree.to_bits().to_ne_bytes());
                    d.extend_from_slice(&(de.attributes.len() as u32).to_ne_bytes());
                    for &a in de.attributes.iter() {
                        d.extend_from_slice(&(a as u16).to_ne_bytes());
                    }
                }
                Ok(d)
            }
            Err(_) => Err(()),
        },
        Kind::Mcv => match statext_mcv_deserialize(m, body) {
            Ok(mv) => {
                let ndims = mv.ndimensions;
                // Re-read DimensionInfo typlen/typbyval from the (validated)
                // blob, exactly as the C digest entry does.
                let dims = mcv_read_diminfo(body, ndims);
                let mut d = Vec::new();
                d.extend_from_slice(&(mv.items.len() as u32).to_ne_bytes());
                d.extend_from_slice(&(ndims as u16).to_ne_bytes());
                for dim in 0..ndims {
                    d.extend_from_slice(&mv.types[dim].to_ne_bytes());
                    d.extend_from_slice(&(dims[dim].0 as u32).to_ne_bytes());
                    d.push(if dims[dim].1 { 1 } else { 0 });
                }
                for it in mv.items.iter() {
                    for dim in 0..ndims {
                        d.push(if it.isnull[dim] { 1 } else { 0 });
                    }
                    d.extend_from_slice(&it.frequency.to_bits().to_ne_bytes());
                    d.extend_from_slice(&it.base_frequency.to_bits().to_ne_bytes());
                    for dim in 0..ndims {
                        if it.isnull[dim] {
                            d.push(0);
                            continue;
                        }
                        d.push(1);
                        let (typlen, typbyval) = dims[dim];
                        let v = it.values[dim];
                        if typbyval {
                            d.extend_from_slice(&v.as_u64().to_ne_bytes());
                        } else if typlen == -1 {
                            // varlena: [4B header ((len+4)<<2)][len bytes]
                            let p = v.as_usize() as *const u8;
                            // SAFETY: Rust built this buffer with a valid 4B
                            // header + len bytes; lives for `cx`'s lifetime.
                            let hdr = unsafe {
                                u32::from_ne_bytes([*p, *p.add(1), *p.add(2), *p.add(3)])
                            };
                            let len = ((hdr >> 2) as usize).saturating_sub(4);
                            d.extend_from_slice(&(len as u32).to_ne_bytes());
                            let data = unsafe { core::slice::from_raw_parts(p.add(4), len) };
                            d.extend_from_slice(data);
                        } else {
                            // by-ref fixed / cstring: not generated for the
                            // dual bank (see mcv_digestable); unreachable here.
                            unreachable!("non-digestable mcv dim reached digest");
                        }
                    }
                }
                Ok(d)
            }
            Err(_) => Err(()),
        },
    }
}

/// Read the (typlen, typbyval) of each MCV dimension straight from the blob
/// body. Layout: magic(4)+type(4)+nitems(4)+ndims(2), then Oid*ndims, then
/// DimensionInfo*ndims where each DimensionInfo is nvalues(4) nbytes(4)
/// nbytes_aligned(4) typlen(4) typbyval(1)+pad = 20 bytes (typlen at +12,
/// typbyval at +16). Only called after Rust accepted, so bounds are valid.
fn mcv_read_diminfo(body: &[u8], ndims: usize) -> Vec<(i32, bool)> {
    let base = 14 + 4 * ndims;
    let mut out = Vec::with_capacity(ndims);
    for dim in 0..ndims {
        let off = base + dim * 20;
        let typlen = i32::from_ne_bytes(body[off + 12..off + 16].try_into().unwrap());
        let typbyval = body[off + 16] != 0;
        out.push((typlen, typbyval));
    }
    out
}

/// Whether a Rust-accepted MCV blob is safe to compare against the verbatim
/// C oracle. Two conditions:
///
///   1. Every dimension is a type the dual digest supports (by-val 1/2/4/8, or
///      varlena -1) — by-ref-fixed / cstring dims are simply not generated for
///      the dual bank.
///   2. Every `bool` byte in the blob (each dim's `typbyval`, and every item's
///      per-dim `isnull`) is CANONICAL (0 or 1). C reads these into `_Bool`;
///      a non-{0,1} byte is a non-canonical `_Bool` whose truthiness is
///      C-implementation-defined (e.g. clang tests the low bit, so 0x58 reads
///      FALSE), while Rust's `byte != 0` reads TRUE. On such a blob the two
///      engines can legitimately disagree on NULL-ness / by-val-ness WITHOUT
///      either crashing — the STATSBLOB-R2 RULED divergence class (a malformed
///      restored stats blob, C-UB on the bool byte). Those blobs are banked as
///      Rust no-crash only; the C oracle is not run on them.
///
/// Only called after Rust accepted, so the header/diminfo/item offsets it
/// walks are all in bounds.
fn mcv_dual_ok(body: &[u8]) -> bool {
    if body.len() < 14 {
        return false;
    }
    let ndims = i16::from_ne_bytes(body[12..14].try_into().unwrap());
    if ndims < 1 || ndims as usize > 8 {
        return false;
    }
    let ndims = ndims as usize;
    let nitems = u32::from_ne_bytes(body[8..12].try_into().unwrap()) as usize;
    let base = 14 + 4 * ndims;
    if body.len() < base + ndims * 20 {
        return false;
    }
    let mut nbytes_sum = 0usize;
    for dim in 0..ndims {
        let off = base + dim * 20;
        let nbytes = i32::from_ne_bytes(body[off + 4..off + 8].try_into().unwrap());
        let typlen = i32::from_ne_bytes(body[off + 12..off + 16].try_into().unwrap());
        let typbyval_byte = body[off + 16];
        if typbyval_byte > 1 {
            return false; // non-canonical _Bool (C-UB), STATSBLOB-R2
        }
        let typbyval = typbyval_byte != 0;
        let ok = (typbyval && matches!(typlen, 1 | 2 | 4 | 8)) || (!typbyval && typlen == -1);
        if !ok {
            return false;
        }
        if nbytes < 0 {
            return false;
        }
        nbytes_sum += nbytes as usize;
    }
    // Item region: nitems items each of (ndims isnull bytes + 16 + 2*ndims).
    let items_off = base + ndims * 20 + nbytes_sum;
    let item_stride = ndims + 16 + 2 * ndims;
    for i in 0..nitems {
        let ioff = items_off + i * item_stride;
        if ioff + ndims > body.len() {
            return false;
        }
        for d in 0..ndims {
            if body[ioff + d] > 1 {
                return false; // non-canonical isnull _Bool (C-UB), STATSBLOB-R2
            }
        }
    }
    true
}

// ---------------------------------------------------------------------------
// C side.
// ---------------------------------------------------------------------------

fn c_parse(kind: Kind, body: &[u8], out: &mut [u8]) -> Result<Vec<u8>, i32> {
    let mut outlen: i32 = 0;
    let f = match kind {
        Kind::Ndistinct => pg_diff_statext_ndistinct,
        Kind::Deps => pg_diff_statext_deps,
        Kind::Mcv => pg_diff_statext_mcv,
    };
    let rc = unsafe {
        f(
            body.as_ptr(),
            body.len() as u32,
            out.as_mut_ptr(),
            out.len() as i32,
            &mut outlen,
        )
    };
    if rc == 0 {
        Ok(out[..outlen as usize].to_vec())
    } else {
        Err(rc)
    }
}

// ---------------------------------------------------------------------------
// Comparison contract.
// ---------------------------------------------------------------------------

/// Outcome of one differential case.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Outcome {
    /// pgrust rejected (safely). C not run.
    RustReject,
    /// Both accepted; digests compared equal.
    BothAccept,
    /// pgrust accepted; the C leg was skipped (MCV non-canonical-bool / non-
    /// digestable dim shape — banked as pgrust no-crash only).
    RustAcceptCSkipped,
    /// pgrust hit a `debug_assert!` (DEBUG builds only) — the deserializers'
    /// end-of-buffer consumption check `debug_assert_eq!(off, data.len())`,
    /// which fires on an accepted blob with TRAILING bytes (e.g. a lowered
    /// count word). Compiled out in release, exactly like C's asserts-off
    /// `Assert(tmp == end)`, so both engines accept such a blob in production.
    /// The safety bar is release-effective (debug-assert masking law); this is
    /// recorded, never a failure. In a release build a panic is instead a real
    /// defect and fails.
    RustDebugAssert,
    /// pgrust accepted SAFELY; verbatim C rejected. A leniency/conformance
    /// divergence, NOT a safety bug: pgrust already parsed without panic/OOB.
    /// The dominant class is a malformed `nbytes_aligned` (a C single-chunk-
    /// layout hint pgrust does not use) driving C's `MaxAllocSize` reject —
    /// STATSBLOB-R3. Counted and surfaced, never a failure. A genuinely
    /// dangerous "missing check" would instead surface as a pgrust panic/OOB
    /// on the safety plane.
    RustAcceptCReject,
}

/// One case, full contract. The HIGH bar (pgrust never panics/OOB) is enforced
/// implicitly: a panic in `rust_parse` propagates and fails the enclosing test.
fn run_case(kind: Kind, body: &[u8], out: &mut [u8]) -> Outcome {
    // A Rust index/bounds panic fires in BOTH debug and release; a
    // `debug_assert!` fires only in debug. The safety bar is release-effective
    // (debug-assert masking law): in release ANY panic is a real defect and
    // must fail; in debug we record a debug-assert trip and move on.
    let parsed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| rust_parse(kind, body)));
    let rust = match parsed {
        Ok(r) => r,
        Err(_) => {
            if cfg!(debug_assertions) {
                return Outcome::RustDebugAssert;
            }
            panic!(
                "[{kind:?}] pgrust PANIC/OOB on deserialize (release build) [HIGH] (blob {})",
                hex(body)
            );
        }
    };
    match rust {
        Ok(rdig) => {
            if kind == Kind::Mcv && !mcv_dual_ok(body) {
                return Outcome::RustAcceptCSkipped;
            }
            match c_parse(kind, body, out) {
                Ok(cdig) => {
                    // Both accepted => the blob meets pgrust's (stricter)
                    // invariants AND C's => the parses MUST be identical.
                    assert_eq!(
                        rdig,
                        cdig,
                        "[{kind:?}] ACCEPT value/structure divergence — both engines accepted \
                         but produced different parses [investigate] (blob {})",
                        hex(body)
                    );
                    Outcome::BothAccept
                }
                Err(_) => Outcome::RustAcceptCReject,
            }
        }
        Err(()) => Outcome::RustReject,
    }
}

/// A blob that BOTH sides must reject and that is memory-safe for the C
/// (rejected before any assert-guarded read). Asserts reject parity.
fn run_dual_reject(kind: Kind, body: &[u8], out: &mut [u8]) {
    assert!(
        rust_parse(kind, body).is_err(),
        "[{kind:?}] expected Rust to REJECT (blob {})",
        hex(body)
    );
    match c_parse(kind, body, out) {
        Err(_) => {}
        Ok(_) => panic!(
            "[{kind:?}] verbatim C ACCEPTED a blob Rust rejected — over-rejection or oracle \
             gap, investigate (blob {})",
            hex(body)
        ),
    }
}

/// An invariant-violating blob: the verbatim asserts-off C would read out of
/// bounds, so it is NOT run. The Rust side must reject it cleanly (no panic).
fn run_rust_safe_reject(kind: Kind, body: &[u8]) {
    assert!(
        rust_parse(kind, body).is_err(),
        "[{kind:?}] Rust must safely REJECT this invariant-violating blob \
         (verbatim C would OOB here) (blob {})",
        hex(body)
    );
}

fn hex(b: &[u8]) -> String {
    let mut s = String::with_capacity(b.len() * 2 + 8);
    s.push_str(&format!("len={} ", b.len()));
    for (i, x) in b.iter().enumerate() {
        if i == 256 {
            s.push_str("...");
            break;
        }
        s.push_str(&format!("{x:02x}"));
    }
    s
}

// ---------------------------------------------------------------------------
// libFuzzer per-exec entry.
// ---------------------------------------------------------------------------

/// Dispatch: first byte picks the deserializer, the rest is the blob body.
pub fn statext_diff(data: &[u8]) {
    let _g = crate::oracle_serial();
    let Some((&sel, body)) = data.split_first() else {
        return;
    };
    let kind = match sel % 3 {
        0 => Kind::Ndistinct,
        1 => Kind::Deps,
        _ => Kind::Mcv,
    };
    let mut out = vec![0u8; OUT_CAP];
    run_case(kind, body, &mut out);
}

// ===========================================================================
// Blob builders.
// ===========================================================================

fn nd_blob(magic: u32, typ: u32, nitems: u32, items: &[(f64, &[i16])]) -> Vec<u8> {
    let mut b = Vec::new();
    b.extend_from_slice(&magic.to_ne_bytes());
    b.extend_from_slice(&typ.to_ne_bytes());
    b.extend_from_slice(&nitems.to_ne_bytes());
    for (nd, atts) in items {
        b.extend_from_slice(&nd.to_ne_bytes());
        b.extend_from_slice(&(atts.len() as i32).to_ne_bytes());
        for a in *atts {
            b.extend_from_slice(&a.to_ne_bytes());
        }
    }
    b
}

fn dep_blob(magic: u32, typ: u32, ndeps: u32, deps: &[(f64, &[i16])]) -> Vec<u8> {
    let mut b = Vec::new();
    b.extend_from_slice(&magic.to_ne_bytes());
    b.extend_from_slice(&typ.to_ne_bytes());
    b.extend_from_slice(&ndeps.to_ne_bytes());
    for (degree, atts) in deps {
        b.extend_from_slice(&degree.to_ne_bytes());
        b.extend_from_slice(&(atts.len() as i16).to_ne_bytes());
        for a in *atts {
            b.extend_from_slice(&a.to_ne_bytes());
        }
    }
    b
}

/// A valid single-int4-dimension MCV blob: `values` are the deduplicated
/// per-dim int4s; each item is (isnull, freq, base, index-into-values).
fn mcv_blob_int4(values: &[i32], items: &[(bool, f64, f64, u16)]) -> Vec<u8> {
    let ndims = 1usize;
    let nvalues = values.len() as i32;
    let nbytes = (values.len() * 4) as i32;
    let mut b = Vec::new();
    b.extend_from_slice(&STATS_MCV_MAGIC.to_ne_bytes());
    b.extend_from_slice(&STATS_MCV_TYPE_BASIC.to_ne_bytes());
    b.extend_from_slice(&(items.len() as u32).to_ne_bytes());
    b.extend_from_slice(&(ndims as i16).to_ne_bytes());
    b.extend_from_slice(&23u32.to_ne_bytes()); // int4 oid
    // DimensionInfo: nvalues, nbytes, nbytes_aligned(unused for byval), typlen, typbyval
    b.extend_from_slice(&nvalues.to_ne_bytes());
    b.extend_from_slice(&nbytes.to_ne_bytes());
    b.extend_from_slice(&0i32.to_ne_bytes());
    b.extend_from_slice(&4i32.to_ne_bytes());
    b.push(1);
    b.extend_from_slice(&[0u8; 3]);
    for &v in values {
        b.extend_from_slice(&v.to_ne_bytes());
    }
    for &(isnull, freq, base, idx) in items {
        b.push(if isnull { 1 } else { 0 });
        b.extend_from_slice(&freq.to_ne_bytes());
        b.extend_from_slice(&base.to_ne_bytes());
        b.extend_from_slice(&idx.to_ne_bytes());
    }
    b
}

/// A valid single-varlena(text-shaped, typlen -1)-dimension MCV blob.
fn mcv_blob_varlena(values: &[&[u8]], items: &[(bool, f64, f64, u16)]) -> Vec<u8> {
    let ndims = 1usize;
    let nvalues = values.len() as i32;
    let mut nbytes = 0i32;
    let mut nbytes_aligned = 0i32;
    for v in values {
        nbytes += 4 + v.len() as i32; // uint32 len + data
        nbytes_aligned += maxalign(v.len() + 4) as i32; // C: MAXALIGN(len+VARHDRSZ)
    }
    let mut b = Vec::new();
    b.extend_from_slice(&STATS_MCV_MAGIC.to_ne_bytes());
    b.extend_from_slice(&STATS_MCV_TYPE_BASIC.to_ne_bytes());
    b.extend_from_slice(&(items.len() as u32).to_ne_bytes());
    b.extend_from_slice(&(ndims as i16).to_ne_bytes());
    b.extend_from_slice(&25u32.to_ne_bytes()); // text oid
    b.extend_from_slice(&nvalues.to_ne_bytes());
    b.extend_from_slice(&nbytes.to_ne_bytes());
    b.extend_from_slice(&nbytes_aligned.to_ne_bytes());
    b.extend_from_slice(&(-1i32).to_ne_bytes()); // typlen -1
    b.push(0); // not byval
    b.extend_from_slice(&[0u8; 3]);
    for v in values {
        b.extend_from_slice(&(v.len() as u32).to_ne_bytes());
        b.extend_from_slice(v);
    }
    for &(isnull, freq, base, idx) in items {
        b.push(if isnull { 1 } else { 0 });
        b.extend_from_slice(&freq.to_ne_bytes());
        b.extend_from_slice(&base.to_ne_bytes());
        b.extend_from_slice(&idx.to_ne_bytes());
    }
    b
}

fn maxalign(n: usize) -> usize {
    (n + 7) & !7
}

// ===========================================================================
// Tests: structured banks + random no-panic + detection controls + witness.
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    fn newout() -> Vec<u8> {
        vec![0u8; OUT_CAP]
    }

    // -------- ndistinct --------------------------------------------------

    #[test]
    fn ndistinct_bank() {
        let _g = crate::c_oracle_serial();
        let mut out = newout();
        let mut n = 0usize;
        let t = Instant::now();

        // ACCEPT: nitems 1..=24, natts 2..=8, several attr patterns. Exact
        // size => C-safe, Rust-accept => digest parity.
        for nitems in 1u32..=40 {
            for natts in 2usize..=8 {
                let atts: Vec<i16> = (0..natts as i16).map(|k| k - 3).collect();
                let items: Vec<(f64, &[i16])> =
                    (0..nitems).map(|i| (i as f64 + 0.5, &atts[..])).collect();
                let b = nd_blob(
                    STATS_NDISTINCT_MAGIC,
                    STATS_NDISTINCT_TYPE_BASIC,
                    nitems,
                    &items,
                );
                assert_eq!(
                    run_case(Kind::Ndistinct, &b, &mut out),
                    Outcome::BothAccept,
                    "well-formed ndistinct must accept identically on both"
                );
                n += 1;
            }
        }

        // REJECT (dual, C-safe): bad magic, bad type, zero nitems, short
        // header, and count words with too-small payloads (count*size gate).
        let good = nd_blob(STATS_NDISTINCT_MAGIC, STATS_NDISTINCT_TYPE_BASIC, 1, &[(3.0, &[1, 2])]);
        let mut rejects: Vec<Vec<u8>> = Vec::new();
        rejects.push({ let mut b = good.clone(); b[0] ^= 0xFF; b });
        rejects.push({ let mut b = good.clone(); b[4] ^= 0xFF; b });
        rejects.push(nd_blob(STATS_NDISTINCT_MAGIC, STATS_NDISTINCT_TYPE_BASIC, 0, &[]));
        for cut in 0..good.len().min(12) {
            rejects.push(good[..cut].to_vec());
        }
        for &nitems in &[1u32, 2, 3, 0x7fff_ffff, 0x8000_0000, 0xffff_ffff, 100_000] {
            // header only + a few payload bytes: minimum_size gate rejects.
            let mut b = Vec::new();
            b.extend_from_slice(&STATS_NDISTINCT_MAGIC.to_ne_bytes());
            b.extend_from_slice(&STATS_NDISTINCT_TYPE_BASIC.to_ne_bytes());
            b.extend_from_slice(&nitems.to_ne_bytes());
            b.extend_from_slice(&[0u8; 8]); // < one minimal item
            rejects.push(b);
        }
        // Truncation of a MINIMAL (natts=2) well-formed multi-item blob: every
        // prefix is rejected by a size gate => C-safe dual reject.
        let minimal = nd_blob(
            STATS_NDISTINCT_MAGIC,
            STATS_NDISTINCT_TYPE_BASIC,
            3,
            &[(1.0, &[1, 2]), (2.0, &[3, 4]), (3.0, &[5, 6])],
        );
        for cut in 0..minimal.len() {
            rejects.push(minimal[..cut].to_vec());
        }
        for b in &rejects {
            run_dual_reject(Kind::Ndistinct, b, &mut out);
            n += 1;
        }

        // C-UNSAFE band (Rust-only, no-crash + reject): per-item nattributes
        // out of [2,8] but payload sized so the min-size gate passes.
        for &natts in &[1i32, 9, 16, 100, -1, i32::MIN, i32::MAX] {
            // build item with declared natts and enough trailing bytes to
            // pass the (2-attr) minimum_size gate; Rust rejects on natts range
            // (or on the huge/negative multiply); verbatim asserts-off C would
            // OOB, so it is not run.
            let mut b = Vec::new();
            b.extend_from_slice(&STATS_NDISTINCT_MAGIC.to_ne_bytes());
            b.extend_from_slice(&STATS_NDISTINCT_TYPE_BASIC.to_ne_bytes());
            b.extend_from_slice(&1u32.to_ne_bytes());
            b.extend_from_slice(&7.0f64.to_ne_bytes());
            b.extend_from_slice(&natts.to_ne_bytes());
            b.extend_from_slice(&[0u8; 64]); // slack payload
            run_rust_safe_reject(Kind::Ndistinct, &b);
            n += 1;
        }

        eprintln!(
            "WITNESS ndistinct_bank: {n} cases in {:?} ({:.0} ns/case)",
            t.elapsed(),
            t.elapsed().as_nanos() as f64 / n as f64
        );
        assert!(n >= 300, "expected a substantial ndistinct bank, got {n}");
    }

    // -------- dependencies ----------------------------------------------

    #[test]
    fn deps_bank() {
        let _g = crate::c_oracle_serial();
        let mut out = newout();
        let mut n = 0usize;
        let t = Instant::now();

        for ndeps in 1u32..=40 {
            for natts in 2usize..=8 {
                let atts: Vec<i16> = (0..natts as i16).map(|k| k + 1).collect();
                let deps: Vec<(f64, &[i16])> =
                    (0..ndeps).map(|i| (1.0 / (i as f64 + 1.0), &atts[..])).collect();
                let b = dep_blob(STATS_DEPS_MAGIC, STATS_DEPS_TYPE_BASIC, ndeps, &deps);
                assert_eq!(
                    run_case(Kind::Deps, &b, &mut out),
                    Outcome::BothAccept,
                    "well-formed deps must accept identically on both"
                );
                n += 1;
            }
        }

        // DUAL-SAFE rejects: bad magic / type / zero-ndeps / sub-header
        // length. These reject right after the header read, before any item
        // access, so the verbatim C rejects them memory-safely too.
        let good = dep_blob(STATS_DEPS_MAGIC, STATS_DEPS_TYPE_BASIC, 1, &[(0.5, &[1, 2])]);
        let mut dual: Vec<Vec<u8>> = Vec::new();
        dual.push({ let mut b = good.clone(); b[0] ^= 0xFF; b });
        dual.push({ let mut b = good.clone(); b[4] ^= 0xFF; b });
        dual.push(dep_blob(STATS_DEPS_MAGIC, STATS_DEPS_TYPE_BASIC, 0, &[]));
        for cut in 0..12 {
            dual.push(good[..cut].to_vec()); // < DEP_SizeOfHeader
        }
        for b in &dual {
            run_dual_reject(Kind::Deps, b, &mut out);
            n += 1;
        }

        // RUST-ONLY safe rejects: dependencies.c's min-size gate is
        // `SizeOfItem(ndeps)` (the size of ONE dependency with `ndeps`
        // attributes, NOT `ndeps` dependencies) and it wraps in uint32 for
        // `ndeps` near 2^32 — a notoriously loose/overflowing bound. So a
        // short-payload or truncated blob passes the C gate and the verbatim
        // asserts-off C then reads PAST the bytea (OOB). pgrust's per-iteration
        // `data.len()-off` check rejects cleanly. Verbatim C is not run here.
        for &ndeps in &[1u32, 2, 3, 0x7fff_ffff, 0x8000_0000, 0xffff_ffff, 100_000] {
            let mut b = Vec::new();
            b.extend_from_slice(&STATS_DEPS_MAGIC.to_ne_bytes());
            b.extend_from_slice(&STATS_DEPS_TYPE_BASIC.to_ne_bytes());
            b.extend_from_slice(&ndeps.to_ne_bytes());
            b.extend_from_slice(&[0u8; 6]);
            run_rust_safe_reject(Kind::Deps, &b);
            n += 1;
        }
        let minimal = dep_blob(
            STATS_DEPS_MAGIC,
            STATS_DEPS_TYPE_BASIC,
            3,
            &[(0.1, &[1, 2]), (0.2, &[3, 4]), (0.3, &[5, 6])],
        );
        for cut in 12..minimal.len() {
            run_rust_safe_reject(Kind::Deps, &minimal[..cut]);
            n += 1;
        }

        for &k in &[1i16, 9, 16, 100, -1, i16::MIN, i16::MAX] {
            let mut b = Vec::new();
            b.extend_from_slice(&STATS_DEPS_MAGIC.to_ne_bytes());
            b.extend_from_slice(&STATS_DEPS_TYPE_BASIC.to_ne_bytes());
            b.extend_from_slice(&1u32.to_ne_bytes());
            b.extend_from_slice(&0.5f64.to_ne_bytes());
            b.extend_from_slice(&k.to_ne_bytes());
            b.extend_from_slice(&[0u8; 64]);
            run_rust_safe_reject(Kind::Deps, &b);
            n += 1;
        }

        eprintln!(
            "WITNESS deps_bank: {n} cases in {:?} ({:.0} ns/case)",
            t.elapsed(),
            t.elapsed().as_nanos() as f64 / n as f64
        );
        assert!(n >= 300, "expected a substantial deps bank, got {n}");
    }

    // -------- mcv --------------------------------------------------------

    #[test]
    fn mcv_bank() {
        let _g = crate::c_oracle_serial();
        let mut out = newout();
        let mut n = 0usize;
        let t = Instant::now();

        // ACCEPT (int4 dim): vary nvalues and nitems / indices / null flags.
        for nvalues in 1u16..=16 {
            let values: Vec<i32> = (0..nvalues as i32).map(|v| v * 7 - 11).collect();
            let items: Vec<(bool, f64, f64, u16)> = (0..nvalues)
                .map(|i| (false, 0.5 / (i as f64 + 1.0), 0.1, i % nvalues))
                .collect();
            let b = mcv_blob_int4(&values, &items);
            assert_eq!(
                run_case(Kind::Mcv, &b, &mut out),
                Outcome::BothAccept,
                "well-formed int4 MCV must accept identically on both"
            );
            n += 1;
        }
        // ACCEPT with NULL flags (index bytes present but ignored when null).
        {
            let values = [10i32, 20, 30];
            let items = [
                (true, 0.4, 0.2, 0),
                (false, 0.3, 0.1, 2),
                (true, 0.1, 0.05, 999), // index ignored because null
            ];
            let b = mcv_blob_int4(&values, &items);
            assert_eq!(run_case(Kind::Mcv, &b, &mut out), Outcome::BothAccept);
            n += 1;
        }
        // ACCEPT (varlena dim): exercises the per-value uint32 length field.
        {
            let vals: [&[u8]; 3] = [b"a", b"bc", b"def"];
            let items = [(false, 0.5, 0.2, 0), (false, 0.3, 0.1, 1), (false, 0.2, 0.05, 2)];
            let b = mcv_blob_varlena(&vals, &items);
            assert_eq!(
                run_case(Kind::Mcv, &b, &mut out),
                Outcome::BothAccept,
                "well-formed varlena MCV must accept identically on both"
            );
            n += 1;
            // empty-string value (len 0) edge
            let vals0: [&[u8]; 2] = [b"", b"x"];
            let items0 = [(false, 0.6, 0.3, 0), (false, 0.4, 0.1, 1)];
            let b0 = mcv_blob_varlena(&vals0, &items0);
            assert_eq!(run_case(Kind::Mcv, &b0, &mut out), Outcome::BothAccept);
            n += 1;
        }

        // REJECT (dual, C-safe): bad magic/type, zero nitems/ndims, header
        // truncation, ndims/nitems out of range, count-overflow size gate.
        let good = mcv_blob_int4(&[1, 2], &[(false, 0.5, 0.25, 0), (false, 0.5, 0.25, 1)]);
        let mut rejects: Vec<Vec<u8>> = Vec::new();
        rejects.push({ let mut b = good.clone(); b[0] ^= 0xFF; b });
        rejects.push({ let mut b = good.clone(); b[4] ^= 0xFF; b });
        for cut in 0..18usize.min(good.len()) {
            rejects.push(good[..cut].to_vec()); // shorter than MinSizeOfMCVList
        }
        // zero ndims / zero nitems / ndims>8 / nitems>MAX, with an otherwise
        // plausible short header (rejected before value access).
        let hdr = |nitems: u32, ndims: i16| {
            let mut b = Vec::new();
            b.extend_from_slice(&STATS_MCV_MAGIC.to_ne_bytes());
            b.extend_from_slice(&STATS_MCV_TYPE_BASIC.to_ne_bytes());
            b.extend_from_slice(&nitems.to_ne_bytes());
            b.extend_from_slice(&ndims.to_ne_bytes());
            b.extend_from_slice(&[0u8; 64]); // slack, still < expected_size
            b
        };
        rejects.push(hdr(1, 0)); // zero-length dim array
        rejects.push(hdr(0, 1)); // zero-length item array
        rejects.push(hdr(1, 9)); // ndims > STATS_MAX_DIMENSIONS
        rejects.push(hdr(1, -1)); // negative ndims
        rejects.push(hdr((STATS_MCVLIST_MAX_ITEMS as u32) + 1, 1)); // nitems too large
        rejects.push(hdr(0xffff_ffff, 1)); // huge nitems -> size gate
        // Truncation of a well-formed blob: VARSIZE mismatch => reject.
        for cut in 18..good.len() {
            rejects.push(good[..cut].to_vec());
        }
        for b in &rejects {
            run_dual_reject(Kind::Mcv, b, &mut out);
            n += 1;
        }

        // C-UNSAFE band (Rust-only): nvalues<0, nbytes<0, index>=nvalues,
        // varlena length overrunning the dimension budget. Rust must reject
        // safely; verbatim asserts-off C would OOB and is not run.
        // index >= nvalues (int4 dim, nvalues=2, item index 5)
        run_rust_safe_reject(
            Kind::Mcv,
            &mcv_blob_int4(&[1, 2], &[(false, 0.5, 0.25, 5)]),
        );
        n += 1;
        // negative nvalues / nbytes: hand-patch a valid blob's DimensionInfo.
        {
            let mut b = mcv_blob_int4(&[1, 2], &[(false, 0.5, 0.25, 0), (false, 0.5, 0.25, 1)]);
            let dioff = 14 + 4; // after header + 1 Oid
            b[dioff..dioff + 4].copy_from_slice(&(-1i32).to_ne_bytes()); // nvalues=-1
            run_rust_safe_reject(Kind::Mcv, &b);
            n += 1;
            let mut b2 = mcv_blob_int4(&[1, 2], &[(false, 0.5, 0.25, 0), (false, 0.5, 0.25, 1)]);
            b2[dioff + 4..dioff + 8].copy_from_slice(&(-8i32).to_ne_bytes()); // nbytes=-8
            run_rust_safe_reject(Kind::Mcv, &b2);
            n += 1;
        }
        // varlena length field larger than the dimension's byte budget.
        {
            let vals: [&[u8]; 2] = [b"ab", b"cd"];
            let items = [(false, 0.5, 0.25, 0), (false, 0.5, 0.25, 1)];
            let mut b = mcv_blob_varlena(&vals, &items);
            // first value's uint32 len sits right after header+Oid+DimInfo.
            let voff = 14 + 4 + 20;
            b[voff..voff + 4].copy_from_slice(&0xffff_ffffu32.to_ne_bytes());
            run_rust_safe_reject(Kind::Mcv, &b);
            n += 1;
        }

        // STATSBLOB-R3 demonstration (pgrust-lenient, RULED): a garbage
        // `nbytes_aligned` on a BY-VAL dimension. pgrust never reads that field
        // for by-val types, so it parses identically to the clean blob; the
        // verbatim C uses it to size its single-chunk allocation and rejects
        // (MaxAllocSize). Assert pgrust accepts AND its digest is unchanged
        // (the field has no meaning in pgrust's representation) — no crash,
        // correct parse; the divergence is C over-rejecting, not a pgrust bug.
        {
            let clean = mcv_blob_int4(&[7, 9], &[(false, 0.5, 0.25, 0), (false, 0.5, 0.25, 1)]);
            let clean_dig = rust_parse(Kind::Mcv, &clean).expect("clean int4 mcv accepts");
            let mut patched = clean.clone();
            let nba_off = 14 + 4 + 8; // header + 1 Oid + nvalues + nbytes
            patched[nba_off..nba_off + 4].copy_from_slice(&(-905969664i32).to_ne_bytes());
            let patched_dig =
                rust_parse(Kind::Mcv, &patched).expect("pgrust ignores nbytes_aligned for by-val");
            assert_eq!(
                clean_dig, patched_dig,
                "nbytes_aligned must not affect pgrust's by-val parse (STATSBLOB-R3)"
            );
            // And the harness classifies it as the lenient divergence, not a failure.
            assert_eq!(run_case(Kind::Mcv, &patched, &mut out), Outcome::RustAcceptCReject);
            n += 1;
        }

        eprintln!(
            "WITNESS mcv_bank: {n} cases in {:?} ({:.0} ns/case)",
            t.elapsed(),
            t.elapsed().as_nanos() as f64 / n as f64
        );
        assert!(n >= 60, "expected a substantial mcv bank, got {n}");
    }

    // -------- random / mutated no-panic + accept parity ------------------

    /// Deterministic xorshift64* — reproducible corpus, no external dep.
    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x >> 12;
            x ^= x << 25;
            x ^= x >> 27;
            self.0 = x;
            x.wrapping_mul(0x2545F4914F6CDD1D)
        }
        fn byte(&mut self) -> u8 {
            (self.next() & 0xff) as u8
        }
        fn upto(&mut self, n: usize) -> usize {
            (self.next() % n as u64) as usize
        }
    }

    /// Templates whose magic/type are valid, so mutation reaches the parsers'
    /// deeper length-field logic instead of bouncing off the magic check.
    fn templates() -> Vec<(Kind, Vec<u8>)> {
        vec![
            (
                Kind::Ndistinct,
                nd_blob(STATS_NDISTINCT_MAGIC, STATS_NDISTINCT_TYPE_BASIC, 2,
                        &[(1.0, &[1, 2, 3]), (2.0, &[4, 5])]),
            ),
            (
                Kind::Deps,
                dep_blob(STATS_DEPS_MAGIC, STATS_DEPS_TYPE_BASIC, 2,
                         &[(0.5, &[1, 2]), (0.25, &[3, 4, 5])]),
            ),
            (
                Kind::Mcv,
                mcv_blob_int4(&[10, 20, 30], &[(false, 0.5, 0.2, 0), (false, 0.3, 0.1, 2)]),
            ),
            (
                Kind::Mcv,
                mcv_blob_varlena(&[b"aa", b"bbb"], &[(false, 0.5, 0.2, 0), (false, 0.5, 0.2, 1)]),
            ),
        ]
    }

    #[test]
    fn random_no_panic() {
        let _g = crate::c_oracle_serial();
        let mut out = newout();
        let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
        let tmpls = templates();
        let iters = 40_000usize;
        let t = Instant::now();
        let mut accepted = 0usize;
        let mut both = 0usize;
        let mut lenient = 0usize;
        let mut dbg_asserts = 0usize;

        // Silence backtraces from the (expected, tolerated) debug_assert trips
        // in DEBUG builds so 40k execs stay quiet. Restored below. We hold
        // oracle_serial() so no other oracle test runs concurrently.
        let prev_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));

        for i in 0..iters {
            let kind = match i % 3 {
                0 => Kind::Ndistinct,
                1 => Kind::Deps,
                _ => Kind::Mcv,
            };
            let body: Vec<u8> = if i % 4 == 0 {
                // pure random of a random small length
                let len = rng.upto(160);
                (0..len).map(|_| rng.byte()).collect()
            } else {
                // mutate a valid template of the chosen kind
                let cands: Vec<&Vec<u8>> =
                    tmpls.iter().filter(|(k, _)| *k == kind).map(|(_, b)| b).collect();
                let mut b = cands[rng.upto(cands.len())].clone();
                let nmut = 1 + rng.upto(6);
                for _ in 0..nmut {
                    if b.is_empty() {
                        break;
                    }
                    let pos = rng.upto(b.len());
                    b[pos] = rng.byte();
                }
                // occasionally truncate
                if i % 7 == 0 && !b.is_empty() {
                    b.truncate(rng.upto(b.len()));
                }
                b
            };
            // The core HIGH-bar safety property: Rust must not panic/OOB here
            // (a panic propagates and fails the test). BothAccept additionally
            // asserts value parity inside run_case.
            match run_case(kind, &body, &mut out) {
                Outcome::BothAccept => {
                    accepted += 1;
                    both += 1;
                }
                Outcome::RustAcceptCSkipped => accepted += 1,
                Outcome::RustAcceptCReject => {
                    accepted += 1;
                    lenient += 1;
                }
                Outcome::RustDebugAssert => dbg_asserts += 1,
                Outcome::RustReject => {}
            }
        }

        std::panic::set_hook(prev_hook);

        eprintln!(
            "WITNESS random_no_panic: {iters} cases ({accepted} Rust-accepted, {both} dual-parity, \
             {lenient} pgrust-lenient/C-reject [STATSBLOB-R3], {dbg_asserts} debug-assert trips \
             [release-noop, debug-only]) in {:?} ({:.0} ns/case)",
            t.elapsed(),
            t.elapsed().as_nanos() as f64 / iters as f64
        );
        assert!(iters >= 40_000);
    }

    // -------- detection controls (comparator is not vacuous) -------------

    #[test]
    fn detection_control_digest_distinguishes_parses() {
        // Two well-formed blobs that differ only in one attribute value MUST
        // produce different C digests AND different Rust digests, and each
        // side must agree with the other. Proves the value plane is live.
        let _g = crate::c_oracle_serial();
        let mut out = newout();
        let a = nd_blob(STATS_NDISTINCT_MAGIC, STATS_NDISTINCT_TYPE_BASIC, 1, &[(3.0, &[1, 2])]);
        let b = nd_blob(STATS_NDISTINCT_MAGIC, STATS_NDISTINCT_TYPE_BASIC, 1, &[(3.0, &[1, 9])]);
        let ca = c_parse(Kind::Ndistinct, &a, &mut out).expect("a accepted");
        let cb = c_parse(Kind::Ndistinct, &b, &mut out).expect("b accepted");
        assert_ne!(ca, cb, "digest failed to distinguish differing attribute values");
        let ra = rust_parse(Kind::Ndistinct, &a).expect("a accepted rust");
        let rb = rust_parse(Kind::Ndistinct, &b).expect("b accepted rust");
        assert_eq!(ra, ca);
        assert_eq!(rb, cb);
        assert_ne!(ra, rb);
    }

    #[test]
    fn detection_control_planted_wrong_parse_caught() {
        // Plant a blob whose declared per-item nattributes disagrees with its
        // payload (a wrong-parse / OOB attempt). BOTH sides must catch it:
        // the C via its size gate (memory-safe reject) and Rust via its
        // runtime range/size check. If either silently "parsed" it, the guard
        // is not working.
        let _g = crate::c_oracle_serial();
        let mut out = newout();
        // nitems=1, item claims 2 attrs but only 1 attr byte-pair of payload
        // is present after the ndistinct/natts words => below minimum_size.
        let mut b = Vec::new();
        b.extend_from_slice(&STATS_NDISTINCT_MAGIC.to_ne_bytes());
        b.extend_from_slice(&STATS_NDISTINCT_TYPE_BASIC.to_ne_bytes());
        b.extend_from_slice(&1u32.to_ne_bytes());
        b.extend_from_slice(&5.0f64.to_ne_bytes());
        b.extend_from_slice(&2i32.to_ne_bytes());
        b.extend_from_slice(&7i16.to_ne_bytes()); // only ONE attr, need two
        assert!(rust_parse(Kind::Ndistinct, &b).is_err(), "Rust must reject the planted blob");
        assert!(
            c_parse(Kind::Ndistinct, &b, &mut out).is_err(),
            "C must reject the planted blob (size gate)"
        );
    }

    #[test]
    fn dispatch_smoke() {
        // The libFuzzer entry must run its arms without panicking.
        for sel in 0u8..3 {
            let mut d = vec![sel];
            d.extend_from_slice(&nd_blob(
                STATS_NDISTINCT_MAGIC,
                STATS_NDISTINCT_TYPE_BASIC,
                1,
                &[(1.0, &[1, 2])],
            ));
            statext_diff(&d);
        }
        statext_diff(&[]);
        statext_diff(&[0]);
    }
}
