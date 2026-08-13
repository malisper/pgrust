//! pglz_diff (VENDOR-TOAST) — SHIPPED Rust `pglz::pglz_decompress`
//! (crates/common/pglz) vs VERBATIM vendored PostgreSQL 18.3 C
//! `pglz_decompress` (csrc/pg_pglz_io.c; src/common/pg_lzcompress.c @ upstream
//! sha 62d6c7d3df). This is the highest bug-yield surface of the VENDOR wave:
//! TOAST decompression of malformed / crafted compressed data — the classic
//! out-of-bounds / decompression-bomb memory-safety class.
//!
//! THE BAR: pgrust must produce BYTE-IDENTICAL output on valid input AND
//! reject/handle malformed input IDENTICALLY to the verbatim C — same
//! accept/reject verdict (C's `-1` == Rust's `None`), same written length,
//! same output bytes. A pgrust PANIC / OOB / assert (or a silent OOB
//! read/write) where C safely returns `-1` on a bad back-reference, a bad
//! `rawsize`, or a truncated stream is the **HIGH**-severity finding this lane
//! hunts. Any mismatch panics — libFuzzer minimizes it into the reproducer.
//!
//! The bug class, concretely (the decode loop copies attacker-controlled
//! back-references from OUTPUT to OUTPUT):
//!   * back-reference `off == 0`      -> infinite loop unless rejected;
//!   * `off > (dp - dest)`            -> read before the output buffer start;
//!   * unclamped match `len`          -> write past the output buffer end;
//!   * truncated tag / extension byte -> OOB read of the source;
//!   * huge `rawsize` + `off==1,len=273` -> decompression bomb (must be
//!     bounded by the physical output buffer, i.e. `rawsize`).
//! C guards all of these (the `sp > srcend || off == 0 || off > dp - dest`
//! reject + the `Min(len, destend - dp)` clamp); pgrust's port carries the
//! matching checks (crates/common/pglz/src/lib.rs) — this driver proves they
//! agree on every crafted / truncated / random input.
//!
//! lz4/zstd TOAST decompress are NOT covered (documented block, VENDOR #882
//! precedent): pgrust's detoast dispatch is built WITHOUT USE_LZ4, and the
//! only lz4 in the tree is pgrcolumnar's pure-Rust `lz4_flex`, which has no
//! verbatim-C TOAST counterpart to diff against. See findings-vendor-toast.md.
//!
//! Driver input layout: `[selector][rawsize: 4 bytes LE][source...]`.
//!   - selector bit0: `check_complete` (C's `check_complete` arg — demand the
//!     source AND dest end exactly together).
//!   - `rawsize`: declared decompressed size = the output buffer capacity.
//!     Clamped to `MAX_RAWSIZE` so a crafted huge `rawsize` bounds physical
//!     memory (both sides only ever write up to `rawsize`).
//!   - `source`: the (possibly malformed) compressed stream, capped at
//!     `MAX_SRC`. Any bytes are legal input — both decompressors are total
//!     functions over `(source, rawsize, check_complete)`.

use std::ffi::c_int;
use std::mem::MaybeUninit;

extern "C" {
    /// Verbatim-C `pglz_decompress` over caller-owned buffers; returns bytes
    /// written (>=0) or `-1` (corrupt). See csrc/pg_pglz_io.c.
    fn pg_diff_pglz_decompress(
        source: *const u8,
        slen: c_int,
        dest: *mut u8,
        rawsize: c_int,
        check_complete: c_int,
    ) -> c_int;
}

/// Bound the physical output so a crafted huge `rawsize` (decompression bomb)
/// cannot exhaust memory: 1 MiB is far larger than any real detoast chunk yet
/// trivially allocatable per case.
const MAX_RAWSIZE: usize = 1 << 20;
/// Cap the compressed source so a fuzzer seed cannot balloon a single case.
const MAX_SRC: usize = 1 << 16;

/// Run the verbatim C decompressor. Returns `(rc, dest)` where `rc >= 0` is the
/// written length and `rc < 0` is C's corrupt verdict; `dest` is the full
/// `rawsize` buffer (only `dest[..rc]` is meaningful on success).
fn c_decompress(source: &[u8], rawsize: usize, cc: bool) -> (i32, Vec<u8>) {
    let mut dest = vec![0u8; rawsize];
    let rc = unsafe {
        pg_diff_pglz_decompress(
            source.as_ptr(),
            source.len() as c_int,
            dest.as_mut_ptr(),
            rawsize as c_int,
            cc as c_int,
        )
    };
    (rc, dest)
}

/// Run the shipped Rust decompressor. `None` is Rust's corrupt verdict (C's
/// `-1`); `Some(bytes)` is the initialized output prefix.
fn rust_decompress(source: &[u8], rawsize: usize, cc: bool) -> Option<Vec<u8>> {
    let mut dest = vec![MaybeUninit::<u8>::uninit(); rawsize];
    pglz::pglz_decompress(source, &mut dest, cc).map(|n| {
        // SAFETY: pglz_decompress initialized exactly dest[..n].
        dest[..n].iter().map(|b| unsafe { b.assume_init() }).collect()
    })
}

/// The differential comparison itself — factored out so the detection-power
/// control can drive it with planted (deliberately divergent) inputs and
/// confirm it actually panics. Panics (== a Finding) on ANY divergence
/// between the verbatim C and the shipped Rust.
fn assert_agreement(rc: i32, cdest: &[u8], rust: &Option<Vec<u8>>, source: &[u8], rawsize: usize, cc: bool) {
    let hex = |b: &[u8]| -> String {
        let show = &b[..b.len().min(64)];
        let mut s: String = show.iter().map(|x| format!("{x:02x}")).collect();
        if b.len() > 64 {
            s.push_str("...");
        }
        s
    };
    match rust {
        None => {
            // Rust rejected. C must also have rejected.
            assert!(
                rc < 0,
                "pglz VERDICT divergence: Rust REJECTED but C ACCEPTED ({rc} bytes) \
                 — a pgrust corrupt-verdict where C decodes cleanly. \
                 cc={cc} rawsize={rawsize} source={}",
                hex(source)
            );
        }
        Some(out) => {
            // Rust accepted. C must also have accepted, same length, same bytes.
            assert!(
                rc >= 0,
                "pglz VERDICT divergence: Rust ACCEPTED ({} bytes) but C REJECTED (-1) \
                 — a pgrust decode where C detects corruption (the OOB/bomb class). \
                 cc={cc} rawsize={rawsize} source={}",
                out.len(),
                hex(source)
            );
            assert_eq!(
                out.len(),
                rc as usize,
                "pglz LENGTH divergence: Rust wrote {} bytes, C wrote {rc}. \
                 cc={cc} rawsize={rawsize} source={}",
                out.len(),
                hex(source)
            );
            assert_eq!(
                out.as_slice(),
                &cdest[..rc as usize],
                "pglz OUTPUT-BYTE divergence over {} bytes. \
                 cc={cc} rawsize={rawsize} source={}",
                rc,
                hex(source)
            );
        }
    }
}

/// One differential exec: decompress `source` into a `rawsize` buffer on both
/// sides (with `check_complete = cc`) and assert they agree.
fn compare_one(source: &[u8], rawsize: usize, cc: bool) {
    let (rc, cdest) = c_decompress(source, rawsize, cc);
    let rust = rust_decompress(source, rawsize, cc);
    assert_agreement(rc, &cdest, &rust, source, rawsize, cc);
}

/// Differential driver entry. Serializes through the process-global C oracle
/// mutex (the oracle-guard holder check is process-global even though
/// `pglz_decompress` is pure).
pub fn pglz_diff(data: &[u8]) {
    let _serial = crate::c_oracle_serial();
    if data.is_empty() {
        return;
    }
    let selector = data[0];
    let cc = selector & 1 != 0;

    let (raw_rawsize, source): (u32, &[u8]) = if data.len() >= 5 {
        (
            u32::from_le_bytes([data[1], data[2], data[3], data[4]]),
            &data[5..],
        )
    } else {
        (0, &data[1..])
    };
    let rawsize = (raw_rawsize as usize) % (MAX_RAWSIZE + 1);
    let source = &source[..source.len().min(MAX_SRC)];

    // Cross the fuzzer's source/rawsize with BOTH cc planes and a small set of
    // structurally-interesting rawsizes derived from the source, so a single
    // seed exercises the accept path, the truncation path, and the bomb bound.
    compare_one(source, rawsize, cc);
    compare_one(source, rawsize, !cc);
    for &rs in &[0usize, 1, source.len(), source.len().saturating_mul(4).min(MAX_RAWSIZE)] {
        compare_one(source, rs, cc);
    }
}

// ===========================================================================
// Corpus generators (shared by the campaign + tests).
// ===========================================================================

struct Lcg(u64);
impl Lcg {
    fn new(seed: u64) -> Self {
        Lcg(seed)
    }
    fn next_u64(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0
    }
    fn byte(&mut self) -> u8 {
        (self.next_u64() >> 56) as u8
    }
}

/// Compress a few repetitive inputs so we hold VALID streams with a known
/// decompressed image. Returns `(compressed, rawsize, raw)`. Uses the ALWAYS
/// strategy so even short repetitive inputs compress.
fn valid_streams() -> Vec<(Vec<u8>, usize, Vec<u8>)> {
    let mut out = Vec::new();
    let mut push = |raw: Vec<u8>| {
        let mut dest = vec![MaybeUninit::<u8>::uninit(); pglz::pglz_max_output(raw.len())];
        if let Some(n) = pglz::pglz_compress_into(&raw, &mut dest, &pglz::PGLZ_STRATEGY_ALWAYS) {
            let comp: Vec<u8> = dest[..n].iter().map(|b| unsafe { b.assume_init() }).collect();
            out.push((comp, raw.len(), raw));
        }
    };

    // Highly repetitive inputs (compress well; exercise long match runs +
    // the doubling-offset copy-back loop).
    push(vec![0u8; 512]);
    push(vec![b'A'; 300]);
    let phrase = b"the quick brown fox jumps over a lazy dog #";
    push((0..1024).map(|i| phrase[i % phrase.len()]).collect());
    push((0..2048).map(|i| phrase[i % phrase.len()]).collect());
    // Periodic replay of an earlier window (mix of literals + matches).
    {
        let mut prng = Lcg::new(0x243f6a8885a308d3);
        let n = 1500;
        let mut buf = Vec::with_capacity(n);
        for i in 0..n {
            let b = if i % 61 >= 37 && i >= 64 { buf[i - 64] } else { prng.byte() };
            buf.push(b);
        }
        push(buf);
    }
    // Small ones so EVERY prefix is cheap to enumerate.
    push(vec![0u8; 40]);
    push((0..80).map(|i| phrase[i % phrase.len()]).collect());

    out
}

/// Hand-crafted adversarial streams targeting each corrupt-data branch and the
/// bomb bound. Raw bytes fed as `source`.
fn crafted_streams() -> Vec<Vec<u8>> {
    vec![
        // Empty / degenerate.
        vec![],
        vec![0x00],                     // one all-literal ctrl, no items follow
        vec![0xff],                     // one all-match ctrl, no tag bytes follow
        vec![0x00, 0x41],               // ctrl + one literal 'A'
        // Back-reference with off > (dp - dest) == 0  -> reject (before start).
        vec![0x01, 0x10, 0x00],         // len 3, off 256
        vec![0x01, 0x00, 0x01],         // len 3, off 1 (still > 0 output so far)
        vec![0x01, 0xf0, 0xff],         // off 0xfff (max), len 3
        // off == 0 -> reject (would be an infinite loop).
        vec![0x01, 0x00, 0x00],         // len 3, off 0
        // Literal then a VALID off=1 back-reference (builds "AAAA...").
        vec![0x02, 0x41, 0x00, 0x01],   // 'A' then copy 3 @ off 1
        // Decompression bomb: 'A' then len==273 @ off 1 (ext byte 0xff).
        vec![0x02, 0x41, 0x0f, 0x01, 0xff],
        // len==18 extension byte TRUNCATED (source ends before ext byte).
        vec![0x02, 0x41, 0x0f, 0x01],
        // Match tag truncated: ctrl says match but only one tag byte present.
        vec![0x01, 0x10],
        vec![0x01],                     // match ctrl, zero tag bytes
        // len==18 path with off huge -> reject after reading ext byte.
        vec![0x01, 0xff, 0xff],
        // A run of literals filling exactly, then a valid match at the end.
        {
            let mut v = vec![0x00]; // ctrl: 8 literals
            v.extend_from_slice(b"ABCDEFGH");
            v.push(0x01); // next ctrl: match
            v.push(0x50); // len (0)+3=3, off high nibble 5
            v.push(0x00); // off low -> off = 0x500 = 1280 > 8 -> reject
            v
        },
        // Same but a valid small offset (off=4) -> repeats "EFGH".
        {
            let mut v = vec![0x00];
            v.extend_from_slice(b"ABCDEFGH");
            v.push(0x01);
            v.push(0x00); // len 3
            v.push(0x04); // off 4 (<= 8) valid
            v
        },
        // All-match ctrl with 8 back-to-back off=1 matches (heavy copy-back).
        {
            let mut v = vec![0x00, 0x41]; // seed one literal 'A' under a literal ctrl...
            // then a full ctrl of matches
            v.push(0xff);
            for _ in 0..8 {
                v.push(0x00); // len 3
                v.push(0x01); // off 1
            }
            v
        },
    ]
}

/// Random source vectors of assorted lengths — breadth over the byte space.
fn random_streams(count: usize) -> Vec<Vec<u8>> {
    let mut prng = Lcg::new(0x9e3779b97f4a7c15);
    let mut out = Vec::with_capacity(count);
    for i in 0..count {
        let len = (prng.next_u64() as usize % 48) + (i % 7);
        out.push((0..len).map(|_| prng.byte()).collect());
    }
    out
}

/// The full source corpus: crafted + every truncated prefix of every valid
/// stream + random. "Every truncated prefix of a valid compressed image" is
/// enumerated exactly for the valid streams (task requirement).
fn source_corpus() -> Vec<Vec<u8>> {
    let mut out = crafted_streams();
    for (comp, _rawsize, _raw) in valid_streams() {
        for take in 0..=comp.len() {
            out.push(comp[..take].to_vec());
        }
    }
    out.extend(random_streams(2000));
    out
}

/// The rawsize ladder crossed against every source (too-small / exact / too-big
/// / degenerate / bomb-bound).
fn rawsize_ladder(src_len: usize) -> Vec<usize> {
    vec![
        0,
        1,
        8,
        64,
        src_len,
        src_len.saturating_mul(2).min(MAX_RAWSIZE),
        4096,
    ]
}

/// Run the full differential sweep. Returns `(cases, elapsed_ns)`. A genuine
/// divergence PANICS out of here (that is the Finding); a clean return with a
/// large `cases` and non-trivial ns/case is the 0-finding witness.
pub fn run_pglz_campaign() -> (usize, u128) {
    let corpus = source_corpus();
    let t0 = std::time::Instant::now();
    let mut cases = 0usize;
    for src in &corpus {
        for &rs in &rawsize_ladder(src.len()) {
            compare_one(src, rs, false);
            compare_one(src, rs, true);
            cases += 2;
        }
    }
    (cases, t0.elapsed().as_nanos())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The full differential campaign. Green = pgrust matched the verbatim C on
    /// every crafted / truncated / random case (verdict + length + output
    /// bytes). A red here is exactly the OOB / bomb / verdict-divergence class
    /// this lane hunts. Prints the execution witness (cases + ns/case) so a
    /// vacuous sweep is visible.
    #[test]
    fn pglz_campaign_full() {
        let _serial = crate::c_oracle_serial();
        let (cases, elapsed_ns) = run_pglz_campaign();
        let ns_per_case = if cases == 0 { 0.0 } else { elapsed_ns as f64 / cases as f64 };
        eprintln!(
            "pglz_diff campaign: {cases} cases in {} ms ({ns_per_case:.1} ns/case)",
            elapsed_ns / 1_000_000
        );
        assert!(cases > 20_000, "pglz campaign ran too few cases: {cases}");
        // Witness the sweep actually executed work (not a no-op).
        assert!(ns_per_case > 1.0, "pglz campaign near-vacuous: {ns_per_case} ns/case");
    }

    /// Valid-stream roundtrip on the accept plane: every compressed image
    /// decodes byte-identically on both sides at its exact rawsize with
    /// check_complete. Fast, focused, always green.
    #[test]
    fn valid_streams_decode_identically() {
        let _serial = crate::c_oracle_serial();
        let streams = valid_streams();
        assert!(!streams.is_empty(), "no valid streams generated");
        for (comp, rawsize, raw) in &streams {
            compare_one(comp, *rawsize, true);
            // And confirm the decoded bytes equal the ORIGINAL raw input.
            let rust = rust_decompress(comp, *rawsize, true).expect("valid stream rejected");
            assert_eq!(&rust, raw, "valid stream did not roundtrip to original");
        }
    }

    /// Decompression-bomb bound: a 5-byte source claiming 273-byte matches at
    /// off=1 must produce output bounded EXACTLY by rawsize on both sides, for
    /// rawsize up to MAX_RAWSIZE. Proves neither side over-writes on a bomb.
    #[test]
    fn decompression_bomb_bounded() {
        let _serial = crate::c_oracle_serial();
        let bomb = vec![0x02u8, 0x41, 0x0f, 0x01, 0xff]; // 'A' then len 273 @ off 1
        for rawsize in [8usize, 273, 1024, 65536, MAX_RAWSIZE] {
            // check_complete=false: partial fill is allowed, both stop at rawsize.
            compare_one(&bomb, rawsize, false);
            let rust = rust_decompress(&bomb, rawsize, false).expect("bomb rejected unexpectedly");
            assert!(rust.len() <= rawsize, "bomb output {} exceeded rawsize {rawsize}", rust.len());
        }
    }

    /// DETECTION-POWER CONTROL (must-fail control) — always green, but only
    /// because the planted divergences ARE caught.
    ///
    /// A 0-finding sweep is only evidence if the comparator CAN see a
    /// divergence. This drives the real `assert_agreement` with deliberately
    /// mismatched inputs and confirms each plane (verdict-accept, verdict-
    /// reject, length, output-byte) panics. Without this, "0 findings" is
    /// indistinguishable from a comparator that checks nothing.
    #[test]
    fn detection_control_catches_planted_divergence() {
        let _serial = crate::c_oracle_serial();

        // 0. Real agreement on a valid stream (no panic).
        let (comp, rawsize, _raw) = valid_streams().into_iter().next().unwrap();
        compare_one(&comp, rawsize, true);

        let catch = |f: fn()| std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)).is_err();
        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));

        // 1. Verdict plane: C accepts (rc>=0) but Rust rejected (None) -> panic.
        let p_reject = catch(|| {
            assert_agreement(4, &[1, 2, 3, 4], &None, b"\x00", 4, false);
        });
        // 2. Verdict plane: C rejects (-1) but Rust accepted (Some) -> panic.
        let p_accept = catch(|| {
            assert_agreement(-1, &[], &Some(vec![1, 2, 3]), b"\x01", 4, false);
        });
        // 3. Length plane: both accept but lengths differ -> panic.
        let p_len = catch(|| {
            assert_agreement(4, &[1, 2, 3, 4], &Some(vec![1, 2, 3]), b"\x00", 4, false);
        });
        // 4. Output-byte plane: same length, one byte differs -> panic.
        let p_bytes = catch(|| {
            assert_agreement(3, &[1, 2, 3], &Some(vec![1, 2, 99]), b"\x00", 4, false);
        });
        // 5. Agreement (both accept, same bytes) must NOT panic.
        let p_ok = catch(|| {
            assert_agreement(3, &[1, 2, 3], &Some(vec![1, 2, 3]), b"\x00", 4, false);
        });

        std::panic::set_hook(prev);

        assert!(p_reject, "detection control FAILED: verdict-accept divergence not caught");
        assert!(p_accept, "detection control FAILED: verdict-reject divergence not caught");
        assert!(p_len, "detection control FAILED: length divergence not caught");
        assert!(p_bytes, "detection control FAILED: output-byte divergence not caught");
        assert!(!p_ok, "detection control FAILED: comparator panicked on AGREEMENT");
    }

    /// Execution-witness floor: the corpus is non-trivial and every source
    /// survives to a real comparison (not a silent no-op).
    #[test]
    fn execution_witness_floor() {
        let corpus = source_corpus();
        assert!(corpus.len() > 3000, "source corpus too small: {}", corpus.len());
        // The valid-stream prefix enumeration must contribute (every prefix).
        assert!(!valid_streams().is_empty());
        // A quick real sweep over a slice witnesses the comparator executes.
        let _serial = crate::c_oracle_serial();
        let mut executed = 0usize;
        for src in corpus.iter().take(50) {
            compare_one(src, 256, false);
            executed += 1;
        }
        assert!(executed >= 50, "witness executed only {executed} cases");
    }

    /// The driver entry point survives arbitrary short/edge seeds without
    /// panicking on well-formed agreement (smoke over the `[sel][rawsize][src]`
    /// framing incl. sub-5-byte inputs).
    #[test]
    fn driver_entry_smoke() {
        let _serial = crate::c_oracle_serial();
        pglz_diff(&[]);
        pglz_diff(&[0x00]);
        pglz_diff(&[0x01, 0x00]);
        pglz_diff(&[0x01, 0x00, 0x10, 0x00, 0x00, 0x41, 0x42]);
        // A framed valid stream: sel=1 (cc), rawsize=exact, source=compressed.
        let (comp, rawsize, _raw) = valid_streams().into_iter().next().unwrap();
        let mut d = vec![1u8];
        d.extend_from_slice(&(rawsize as u32).to_le_bytes());
        d.extend_from_slice(&comp);
        pglz_diff(&d);
    }
}
