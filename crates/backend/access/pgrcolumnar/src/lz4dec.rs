//! Purpose-built LZ4 block decoder for pgrcolumnar's frames (the Stage 2.4
//! "unchecked LZ4 inner loop" lever, docs/design/pgrcolumnar-v2-beat-clickhouse-plan.md).
//!
//! Provenance: implements the LZ4 block format per the reference spec
//! (lz4/doc/lz4_Block_format.md, github.com/lz4/lz4 @ v1.10), with the
//! copy strategy of ClickHouse's tuned `LZ4::decompress` variants
//! (notes/clickhouse-executor-catalog-2026-07-13.md): unconditional wide
//! (16/32 B) copies legalized by a caller-guaranteed output tail pad, plus
//! offset-class-specialized overlap handling for small-offset matches (the
//! RLE-ish shapes compressible int columns produce).
//!
//! # Safety argument
//!
//! This is a SAFE decoder: malformed frames return `Err`, never UB.
//!
//! - INPUT reads are always bounds-checked against `input.len()`. The wide
//!   literal copy engages only when the input has `len + 31` bytes of slack
//!   past the cursor, so its 32 B loads stay inside `input`; otherwise an
//!   exact `copy_from_slice` runs.
//! - OUTPUT writes go through `&mut [u8]` whose length the caller must make
//!   `>= raw_len + OUT_PAD` (asserted on entry). Every sequence first checks
//!   `op + len <= raw_len`; wide copies then write at most
//!   `ceil(len/32)*32 <= len + 31 < len + OUT_PAD` bytes, so wild stores can
//!   spill only into the pad, never past the slice.
//! - Match sources are validated (`0 < offset <= op`) before any copy, so
//!   loads read only previously written output bytes — except the small-
//!   offset expansion loads, which stay within `[match_src, match_src + 16)`
//!   and are covered by the invariant proved at `copy_match`.
//! - Length accumulators use `checked_add`; the cursor monotonically
//!   advances, so the loop terminates.
//!
//! The pad bytes after `raw_len` are scratch: callers must treat them as
//! clobbered garbage (the reader's arena keeps them out of published Datums).

/// Required writable slack past `raw_len` in the output buffer.
pub const OUT_PAD: usize = 64;

pub type Lz4Error = &'static str;

const ERR_TRUNC: Lz4Error = "truncated LZ4 block";
const ERR_LEN: Lz4Error = "LZ4 length overflow";
const ERR_OUT: Lz4Error = "LZ4 output overrun";
const ERR_OFFSET: Lz4Error = "invalid LZ4 match offset";
const ERR_SHORT: Lz4Error = "LZ4 block ended early";

/// Decompress one LZ4 block into `out[..raw_len]`.
///
/// `out.len()` must be at least `raw_len + OUT_PAD`; bytes in
/// `out[raw_len..]` may be clobbered. Returns `Err` (never panics, never UB)
/// on any malformed input.
pub fn decompress_padded(input: &[u8], out: &mut [u8], raw_len: usize) -> Result<(), Lz4Error> {
    assert!(
        out.len() >= raw_len.checked_add(OUT_PAD).expect("raw_len overflow"),
        "lz4dec: output buffer must carry OUT_PAD tail slack"
    );
    let ilen = input.len();
    let mut ip = 0usize;
    let mut op = 0usize;
    loop {
        // Fast path (the reference decoder's "shortcut", CH-style): a short
        // literal run (< 15) + a short match at offset >= 16 — the dominant
        // sequence shape on text — handled with two blind wide copies.
        // Margins: token(1) + wild 16 B literal load + offset(2) need
        // ip + 19 <= ilen; output wild stores (16 B literal, 2x16 B match)
        // stay under raw_len + OUT_PAD because op <= raw_len and
        // lit <= 14 / mlen <= 18 are checked before the cursor advances.
        if ip + 19 <= ilen {
            let token = input[ip];
            let lit = (token >> 4) as usize;
            if lit < 15 {
                // SAFETY: ip + 1 + 16 <= ilen (margin above); op + 16 <=
                // raw_len + OUT_PAD (op <= raw_len invariant).
                unsafe {
                    let src = input.as_ptr().add(ip + 1);
                    let dst = out.as_mut_ptr().add(op);
                    core::ptr::copy_nonoverlapping(src, dst, 16);
                }
                let op_end = op + lit;
                if op_end > raw_len {
                    return Err(ERR_OUT);
                }
                ip += 1 + lit;
                op = op_end;
                // ip + 2 <= ilen (and ip < ilen, so this can't be the
                // terminal literal run): lit <= 14 means ip <= old_ip + 15,
                // and the ip + 19 margin leaves >= 4 bytes — frames end via
                // the general path below.
                let offset =
                    u16::from_le_bytes(input[ip..ip + 2].try_into().unwrap()) as usize;
                ip += 2;
                // One unsigned compare for (offset != 0 && offset <= op).
                if offset.wrapping_sub(1) >= op {
                    return Err(ERR_OFFSET);
                }
                let mlen = (token & 0x0F) as usize + 4;
                if mlen < 19 {
                    let op_end = op + mlen;
                    if op_end > raw_len {
                        return Err(ERR_OUT);
                    }
                    if offset >= 16 {
                        // SAFETY: two sequential 16 B copies; each pair of
                        // src/dst ranges is disjoint (distance = offset >=
                        // 16), and the second load may only re-read bytes
                        // the first store just wrote (correct match bytes).
                        // Stores end <= op + 32 <= raw_len + OUT_PAD.
                        unsafe {
                            let base = out.as_mut_ptr();
                            let src = base.add(op - offset);
                            let dst = base.add(op);
                            core::ptr::copy_nonoverlapping(src, dst, 16);
                            core::ptr::copy_nonoverlapping(src.add(16), dst.add(16), 16);
                        }
                        op = op_end;
                        continue;
                    }
                    // Small offset: the specialized overlap kernels.
                    // SAFETY: contract checked above (0 < offset <= op,
                    // op + mlen <= raw_len).
                    unsafe {
                        copy_match(out.as_mut_ptr(), op, offset, mlen);
                    }
                    op = op_end;
                    continue;
                }
                // Long match (extension bytes): rejoin the general path
                // after the literals, with the token's nibble re-read there.
                match match_tail(input, &mut ip, out, &mut op, raw_len, token, offset) {
                    Ok(()) => continue,
                    Err(e) => return Err(e),
                }
            }
        }
        if ip >= ilen {
            // A block must end on a literal run inside the loop body; running
            // off the input without hitting that return is a truncation.
            return Err(ERR_TRUNC);
        }
        let token = input[ip];
        ip += 1;

        // --- literals ---
        let mut lit = (token >> 4) as usize;
        if lit == 15 {
            loop {
                if ip >= ilen {
                    return Err(ERR_TRUNC);
                }
                let b = input[ip];
                ip += 1;
                lit = lit.checked_add(b as usize).ok_or(ERR_LEN)?;
                if b != 255 {
                    break;
                }
            }
        }
        if lit != 0 {
            let ip_end = ip.checked_add(lit).ok_or(ERR_LEN)?;
            if ip_end > ilen {
                return Err(ERR_TRUNC);
            }
            let op_end = op + lit; // op <= raw_len and lit <= ilen: no overflow
            if op_end > raw_len {
                return Err(ERR_OUT);
            }
            if ip_end + 31 <= ilen {
                // Wild copy: 32 B strides. Input loads bounded by the slack
                // check; output stores spill only into the pad (see header).
                unsafe {
                    wide_copy(input.as_ptr().add(ip), out.as_mut_ptr().add(op), lit);
                }
            } else {
                out[op..op_end].copy_from_slice(&input[ip..ip_end]);
            }
            ip = ip_end;
            op = op_end;
        }
        if ip == ilen {
            // Block ends with its literal run (spec: the last sequence has
            // no match part).
            return if op == raw_len { Ok(()) } else { Err(ERR_SHORT) };
        }

        // --- match ---
        if ip + 2 > ilen {
            return Err(ERR_TRUNC);
        }
        let offset = u16::from_le_bytes(input[ip..ip + 2].try_into().unwrap()) as usize;
        ip += 2;
        if offset.wrapping_sub(1) >= op {
            return Err(ERR_OFFSET);
        }
        match_tail(input, &mut ip, out, &mut op, raw_len, token, offset)?;
    }
}

/// Match-length extension + validated copy, shared by the fast path's long-
/// match spill and the general path. `offset` is already validated
/// (0 < offset <= op).
#[inline(always)]
fn match_tail(
    input: &[u8],
    ip: &mut usize,
    out: &mut [u8],
    op: &mut usize,
    raw_len: usize,
    token: u8,
    offset: usize,
) -> Result<(), Lz4Error> {
    let ilen = input.len();
    let mut mlen = (token & 0x0F) as usize + 4;
    if mlen == 19 {
        loop {
            if *ip >= ilen {
                return Err(ERR_TRUNC);
            }
            let b = input[*ip];
            *ip += 1;
            mlen = mlen.checked_add(b as usize).ok_or(ERR_LEN)?;
            if b != 255 {
                break;
            }
        }
    }
    let op_end = op.checked_add(mlen).ok_or(ERR_LEN)?;
    if op_end > raw_len {
        return Err(ERR_OUT);
    }
    // SAFETY: 0 < offset <= op (source fully inside written output),
    // op + mlen <= raw_len and out.len() >= raw_len + OUT_PAD (wild
    // stores stay inside the slice; see copy_match's invariants).
    unsafe {
        copy_match(out.as_mut_ptr(), *op, offset, mlen);
    }
    *op = op_end;
    Ok(())
}

/// Copy `ceil(len/32)*32` bytes from src to dst in 32 B strides.
/// Caller guarantees: len > 0, `src..src+len+31` readable,
/// `dst..dst+len+31` writable, regions disjoint.
#[inline(always)]
unsafe fn wide_copy(mut src: *const u8, mut dst: *mut u8, len: usize) {
    let end = dst.add(len);
    loop {
        core::ptr::copy_nonoverlapping(src, dst, 32);
        src = src.add(32);
        dst = dst.add(32);
        if dst >= end {
            return;
        }
    }
}

/// Match copy with the CH-style offset-class specialization.
///
/// Contract (upheld by the caller): `0 < offset <= op`,
/// `op + len <= raw_len`, and the buffer at `out` is writable (and already
/// initialized) for `raw_len + OUT_PAD` bytes. Wild stores write at most
/// 31 B past `op + len`, i.e. inside the pad. All loads read bytes at
/// offsets `< op + len` that prior iterations (or earlier sequences) wrote,
/// or pattern bytes staged on the stack.
#[inline(always)]
unsafe fn copy_match(out: *mut u8, op: usize, offset: usize, len: usize) {
    let dst0 = out.add(op);
    let src0 = out.add(op - offset);
    if offset == 1 {
        // RLE byte run (the dominant shape on constant int stretches).
        core::ptr::write_bytes(dst0, *src0, len);
        return;
    }
    // Unified overlap-tolerant copy. Explicit [u8; 16] read_unaligned /
    // write_unaligned pairs (NOT copy_nonoverlapping of a pattern array —
    // the compiler scalarizes that into per-byte strb stores, measured 3.4x
    // slower on CounterID-class RLE runs) so each 16 B moves as one
    // q-register load + store, and overlapping load/store is well-defined.
    //
    // Phase 1 (offset < 16): distance-doubling. Each 16 B store commits
    // `dist` bytes; the load window at `dst - dist` never precedes the
    // pattern window at `op - offset`, and its committed prefix (the next
    // store's first `dist * 2` bytes) reads only bytes already correct.
    // Phase 2 (dist >= 16): 32 B per iteration as two ordered 16 B moves;
    // the second load's window ends at `dst + 32 - dist <= dst + 16`, i.e.
    // entirely within bytes the first move just committed.
    //
    // Bounds: phase-1 stores end < dst0 + len + 16, phase-2 stores end
    // <= dst0 + ceil(len/32)*32 <= dst0 + len + 31 — both inside OUT_PAD
    // (caller guarantees op + len <= raw_len).
    let mut dist = offset;
    let mut dst = dst0;
    let end = dst0.add(len);
    while dist < 16 {
        let chunk = core::ptr::read_unaligned(dst.sub(dist) as *const [u8; 16]);
        core::ptr::write_unaligned(dst as *mut [u8; 16], chunk);
        dst = dst.add(dist);
        if dst >= end {
            return;
        }
        dist *= 2;
    }
    let mut src = dst.sub(dist);
    loop {
        let a = core::ptr::read_unaligned(src as *const [u8; 16]);
        core::ptr::write_unaligned(dst as *mut [u8; 16], a);
        let b = core::ptr::read_unaligned(src.add(16) as *const [u8; 16]);
        core::ptr::write_unaligned(dst.add(16) as *mut [u8; 16], b);
        src = src.add(32);
        dst = dst.add(32);
        if dst >= end {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dec(input: &[u8], raw_len: usize) -> Result<Vec<u8>, Lz4Error> {
        let mut out = vec![0xAAu8; raw_len + OUT_PAD];
        decompress_padded(input, &mut out, raw_len)?;
        out.truncate(raw_len);
        Ok(out)
    }

    fn roundtrip(data: &[u8]) {
        let comp = lz4_flex::compress(data);
        let got = dec(&comp, data.len()).expect("valid frame decodes");
        assert_eq!(got, data, "roundtrip mismatch len={}", data.len());
    }

    #[test]
    fn roundtrip_empty_and_tiny() {
        roundtrip(&[]);
        roundtrip(&[7]);
        roundtrip(&[7; 5]);
        roundtrip(b"abcdefghijklmnop");
        roundtrip(&[0; 15]);
        roundtrip(&[0; 16]);
        roundtrip(&[0; 17]);
    }

    #[test]
    fn roundtrip_offset_classes() {
        // Force matches at each small-offset class via periodic data.
        for period in [1usize, 2, 3, 4, 5, 6, 7, 8, 9, 11, 13, 15, 16, 17, 31, 40] {
            let data: Vec<u8> = (0..40_000).map(|i| (i % period) as u8).collect();
            roundtrip(&data);
        }
    }

    #[test]
    fn roundtrip_int_column_shapes() {
        // Sorted u32s (FOR-ish), sorted u64s with small deltas, constant runs
        // with breaks: the compressible-int shapes from the day-0 gap.
        let sorted32: Vec<u8> = (0..100_000u32).flat_map(|v| (v / 7).to_le_bytes()).collect();
        roundtrip(&sorted32);
        let sorted64: Vec<u8> =
            (0..50_000u64).flat_map(|v| (1_000_000 + v * 3).to_le_bytes()).collect();
        roundtrip(&sorted64);
        let mut runs = Vec::new();
        for r in 0..200u32 {
            runs.extend(std::iter::repeat(r.to_le_bytes()).take(197).flatten());
        }
        roundtrip(&runs);
    }

    #[test]
    fn roundtrip_text_and_random() {
        let text: Vec<u8> = b"http://example.com/some/path?q=search+phrase&id="
            .iter()
            .cycle()
            .take(300_000)
            .copied()
            .collect();
        roundtrip(&text);
        // xorshift random: incompressible, exercises long-literal tails.
        let mut s = 0x243F_6A88_85A3_08D3u64;
        let rand: Vec<u8> = (0..123_457)
            .map(|_| {
                s ^= s << 13;
                s ^= s >> 7;
                s ^= s << 17;
                s as u8
            })
            .collect();
        roundtrip(&rand);
    }

    #[test]
    fn roundtrip_extension_length_boundaries() {
        // Literal runs of len 14/15/16, 269/270/271 (15+255 boundary) and a
        // long match crossing the 19 and 19+255 boundaries.
        let mut s = 1u64;
        for n in [14usize, 15, 16, 269, 270, 271, 526] {
            let mut data: Vec<u8> = (0..n)
                .map(|_| {
                    s = s.wrapping_mul(6364136223846793005).wrapping_add(1);
                    (s >> 33) as u8
                })
                .collect();
            data.extend(std::iter::repeat(0u8).take(600)); // long match tail
            roundtrip(&data);
        }
    }

    #[test]
    fn matches_lz4_flex_on_valid_frames() {
        // Differential vs the incumbent on mixed-entropy blocks.
        let mut s = 0x9E37_79B9_7F4A_7C15u64;
        for trial in 0..50 {
            let n = 1 + (trial * 977) % 30_000;
            let data: Vec<u8> = (0..n)
                .map(|i| {
                    s ^= s << 13;
                    s ^= s >> 7;
                    s ^= s << 17;
                    if s % 3 == 0 { (i % 251) as u8 } else { (s >> 24) as u8 }
                })
                .collect();
            let comp = lz4_flex::compress(&data);
            let theirs = lz4_flex::decompress(&comp, data.len()).unwrap();
            let ours = dec(&comp, data.len()).unwrap();
            assert_eq!(ours, theirs);
        }
    }

    // ---- malformed-frame corpus: must Err cleanly, never panic/UB ----

    #[test]
    fn malformed_truncations_error() {
        let data: Vec<u8> = (0..10_000).map(|i| (i % 97) as u8).collect();
        let comp = lz4_flex::compress(&data);
        for cut in 0..comp.len() {
            // Every strict prefix must fail (op can't reach raw_len exactly
            // AND consume all input unless the frame is complete).
            let r = dec(&comp[..cut], data.len());
            assert!(r.is_err(), "truncation at {cut} decoded");
        }
    }

    #[test]
    fn malformed_handcrafted_error() {
        // offset 0
        assert!(dec(&[0x10, b'a', 0x00, 0x00], 100).is_err());
        // offset beyond written output
        assert!(dec(&[0x10, b'a', 0x05, 0x00], 100).is_err());
        // literal run past raw_len
        assert!(dec(&[0xF0, 200, b'x'], 4).is_err());
        // match past raw_len
        assert!(dec(&[0x1F, b'a', 0x01, 0x00, 255, 255, 255, 0], 8).is_err());
        // truncated offset
        assert!(dec(&[0x14, b'a', 0x01], 100).is_err());
        // empty input
        assert!(dec(&[], 1).is_err());
        // trailing garbage sequence with zero literals then truncation
        assert!(dec(&[0x00], 5).is_err());
        // extension bytes run off the input
        assert!(dec(&[0xF0, 255, 255], 4096).is_err());
    }

    #[test]
    fn malformed_length_saturation_errors_not_overflow() {
        // Huge accumulated literal length: many 255 extension bytes. Must
        // hit ERR_TRUNC/ERR_LEN, not wrap.
        let mut frame = vec![0xF0u8];
        frame.extend(std::iter::repeat(255u8).take(64 * 1024));
        frame.push(0);
        assert!(dec(&frame, 1 << 20).is_err());
    }

    // ---- mutational fuzz: seeded, deterministic; cross-checked vs lz4_flex.
    // Default budget keeps CI fast; PGRCOLUMNAR_LZ4_FUZZ_ITERS raises it for the
    // meaningful run (see notes in the branch report).
    #[test]
    fn fuzz_mutated_frames() {
        let iters: u64 = std::env::var("CBSTORE_LZ4_FUZZ_ITERS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(5_000);
        let mut s = 0xDEAD_BEEF_CAFE_F00Du64;
        let mut rng = move || {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            s
        };
        // Seed corpus: compressible, periodic, random.
        let mut corpus: Vec<Vec<u8>> = Vec::new();
        for period in [1usize, 3, 4, 8, 13, 64] {
            corpus.push((0..4_096).map(|i| (i % period) as u8).collect());
        }
        corpus.push((0..4_096).map(|_| rng() as u8).collect());
        corpus.push((0..2_048u32).flat_map(|v| (v / 5).to_le_bytes()).collect());

        for it in 0..iters {
            let data = &corpus[(rng() as usize) % corpus.len()];
            let mut frame = lz4_flex::compress(data);
            // Mutate: flip 1-8 bytes, sometimes truncate or extend.
            for _ in 0..(1 + rng() % 8) {
                if frame.is_empty() {
                    break;
                }
                let i = (rng() as usize) % frame.len();
                frame[i] ^= rng() as u8;
            }
            match rng() % 4 {
                0 if !frame.is_empty() => {
                    frame.truncate((rng() as usize) % frame.len());
                }
                1 => frame.extend((0..rng() % 16).map(|_| rng() as u8)),
                _ => {}
            }
            // Claimed raw_len also fuzzes around the true one.
            let raw_len = match rng() % 3 {
                0 => data.len(),
                1 => (rng() as usize) % (2 * data.len() + 1),
                _ => data.len().saturating_sub((rng() as usize) % 64),
            };
            let ours = dec(&frame, raw_len);
            // Cross-check: when both engines accept, bytes must agree.
            if let (Ok(a), Ok(b)) = (&ours, lz4_flex::decompress(&frame, raw_len)) {
                assert_eq!(a, &b, "fuzz iter {it}: divergence on accepted frame");
            }
        }
    }
}
