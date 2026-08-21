//! LZ4 block-format codec (compress + safe decompress), self-contained.
//!
//! Wrapper id 1 (spec §4/§6.4) names the LZ4 *block* format
//! (lz4.org block spec v1.5.1 — token / literal run / 16-bit offset /
//! match run). pgrust is zero-external-dependency and the old crate's
//! decoder is old-format-coupled (design-only donor, `lanev3-m3-chunks.md`
//! §0), so this is a fresh implementation:
//!
//! - [`compress`]: greedy hash-chain-free matcher (single-probe hash table,
//!   the reference "fast" shape). Deterministic: pure function of the input
//!   bytes. Honors the end restrictions (last 5 bytes literal, no match
//!   starting within the final 12 bytes), so any conformant decoder reads
//!   its output.
//! - [`decompress_into`]: bounds-validated at every step — typed error,
//!   never UB, never a panic, on ANY input bytes (the #66/#340 incident
//!   class law). Output size must match the caller's expectation exactly
//!   (`uncompressed_len` from the section header).
//!
//! Wrappers are elected for cold/loser chunks only (charter §7); nothing
//! here sits on a hot fixed-width decode path.

use pgrc2_format::{FormatError, FormatResult};

const MIN_MATCH: usize = 4;
/// No match may START within the last 12 bytes (block-spec end rule).
const MATCH_START_MARGIN: usize = 12;
/// The last 5 bytes of the input are always emitted as literals.
const LAST_LITERALS: usize = 5;
const HASH_LOG: u32 = 12;
const HASH_TABLE_LEN: usize = 1 << HASH_LOG;
const MAX_OFFSET: usize = 0xFFFF;

#[inline]
fn hash4(v: u32) -> usize {
    (v.wrapping_mul(2_654_435_761) >> (32 - HASH_LOG)) as usize
}

#[inline]
fn read_u32(b: &[u8], i: usize) -> u32 {
    u32::from_le_bytes(b[i..i + 4].try_into().expect("len 4"))
}

fn put_len_ext(out: &mut Vec<u8>, mut n: usize) {
    while n >= 255 {
        out.push(255);
        n -= 255;
    }
    out.push(n as u8);
}

fn put_sequence(out: &mut Vec<u8>, literals: &[u8], match_len: usize, offset: usize) {
    debug_assert!(match_len == 0 || (MIN_MATCH..=usize::MAX).contains(&match_len));
    debug_assert!(offset <= MAX_OFFSET);
    let lit_tok = literals.len().min(15) as u8;
    let mat_tok = if match_len == 0 {
        0
    } else {
        (match_len - MIN_MATCH).min(15) as u8
    };
    out.push((lit_tok << 4) | mat_tok);
    if literals.len() >= 15 {
        put_len_ext(out, literals.len() - 15);
    }
    out.extend_from_slice(literals);
    if match_len > 0 {
        out.extend_from_slice(&(offset as u16).to_le_bytes());
        if match_len - MIN_MATCH >= 15 {
            put_len_ext(out, match_len - MIN_MATCH - 15);
        }
    }
}

/// Compress `input` into `out` (appended). Always succeeds; the caller runs
/// the wrapper election on the size (an incompressible input simply loses).
pub fn compress(input: &[u8], out: &mut Vec<u8>) {
    let n = input.len();
    if n == 0 {
        // One empty-literal token: decodes to zero bytes.
        out.push(0);
        return;
    }
    // Small-block guard (v4 fix; QA-corpus catch at the M3 L1 landing): no
    // match may START within the last MATCH_START_MARGIN bytes, so any block
    // of n <= MATCH_START_MARGIN is all-literals BY THE SPEC — and the v3
    // arithmetic below (`n - MATCH_START_MARGIN`) usize-UNDERFLOWED for
    // 9 <= n <= 11, driving read_u32 past the buffer (panic, not a typed
    // refusal). v3 never sealed a 9..=11-byte section; the corpus's tiny
    // shred-lane sections did. Mirrors the reference LZ4_MFLIMIT+1 rule.
    if n <= MATCH_START_MARGIN {
        put_sequence(out, input, 0, 0);
        return;
    }
    let mut table = [0u32; HASH_TABLE_LEN];
    let match_limit = n - MATCH_START_MARGIN;
    let match_end_limit = n - LAST_LITERALS;
    let mut anchor = 0usize;
    let mut i = 0usize;
    while i < match_limit {
        let h = hash4(read_u32(input, i));
        let cand = table[h] as usize;
        table[h] = i as u32;
        if i > cand && i - cand <= MAX_OFFSET && read_u32(input, cand) == read_u32(input, i) {
            // Extend the match forward, capped by the end rule.
            let mut len = MIN_MATCH;
            while i + len < match_end_limit && input[cand + len] == input[i + len] {
                len += 1;
            }
            put_sequence(out, &input[anchor..i], len, i - cand);
            i += len;
            anchor = i;
        } else {
            i += 1;
        }
    }
    put_sequence(out, &input[anchor..], 0, 0);
}

/// Decompress exactly `out.len()` bytes into `out`; every read and write is
/// bounds-validated. Errors are typed [`FormatError::Corrupt`]/`Bounds` —
/// a wrapped section that does not decode to exactly `uncompressed_len`
/// bytes is refused.
pub fn decompress_into(src: &[u8], out: &mut [u8]) -> FormatResult<()> {
    let mut s = 0usize;
    let mut d = 0usize;
    loop {
        let token = *src
            .get(s)
            .ok_or(FormatError::Truncated { at: "lz4 token" })?;
        s += 1;
        // Literal run.
        let mut lit = (token >> 4) as usize;
        if lit == 15 {
            loop {
                let b = *src
                    .get(s)
                    .ok_or(FormatError::Truncated { at: "lz4 litlen" })?;
                s += 1;
                lit += b as usize;
                if b != 255 {
                    break;
                }
                if lit > out.len() {
                    return Err(FormatError::Corrupt { at: "lz4 litlen" });
                }
            }
        }
        let lsrc = src
            .get(s..s + lit)
            .ok_or(FormatError::Truncated { at: "lz4 literals" })?;
        let ldst = out.get_mut(d..d + lit).ok_or(FormatError::Corrupt {
            at: "lz4 output overflow",
        })?;
        ldst.copy_from_slice(lsrc);
        s += lit;
        d += lit;
        if s == src.len() {
            // Block ends on a literal run.
            if d != out.len() {
                return Err(FormatError::Corrupt {
                    at: "lz4 short output",
                });
            }
            return Ok(());
        }
        // Match.
        let ob = src
            .get(s..s + 2)
            .ok_or(FormatError::Truncated { at: "lz4 offset" })?;
        let offset = u16::from_le_bytes(ob.try_into().expect("len 2")) as usize;
        s += 2;
        if offset == 0 || offset > d {
            return Err(FormatError::Corrupt { at: "lz4 offset" });
        }
        let mut mlen = (token & 0x0F) as usize + MIN_MATCH;
        if mlen == 15 + MIN_MATCH {
            loop {
                let b = *src
                    .get(s)
                    .ok_or(FormatError::Truncated { at: "lz4 matlen" })?;
                s += 1;
                mlen += b as usize;
                if b != 255 {
                    break;
                }
                if mlen > out.len() {
                    return Err(FormatError::Corrupt { at: "lz4 matlen" });
                }
            }
        }
        if d + mlen > out.len() {
            return Err(FormatError::Corrupt {
                at: "lz4 output overflow",
            });
        }
        // Overlapping copy semantics: byte-at-a-time from d - offset.
        for k in 0..mlen {
            out[d + k] = out[d - offset + k];
        }
        d += mlen;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn splitmix(state: &mut u64) -> u64 {
        *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = *state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn roundtrip(input: &[u8]) {
        let mut c = Vec::new();
        compress(input, &mut c);
        let mut out = vec![0u8; input.len()];
        decompress_into(&c, &mut out).expect("decompress");
        assert_eq!(out, input);
    }

    #[test]
    fn roundtrip_shapes() {
        roundtrip(b"");
        roundtrip(b"a");
        roundtrip(b"abcd");
        roundtrip(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        // The 9..=12-byte small-block band: the v3 `n - MATCH_START_MARGIN`
        // usize-underflow read past the buffer here (QA-corpus catch, M3 L1
        // — tiny shred-lane sections). Every length in the band, plus the
        // boundary neighbors, must round-trip as all-literals.
        for n in 8..=13usize {
            let buf: Vec<u8> = (0..n as u8).collect();
            roundtrip(&buf);
            roundtrip(&vec![0xAB; n]);
        }
        roundtrip(&[0u8; 100_000]);
        let rep: Vec<u8> = b"the quick brown fox "
            .iter()
            .copied()
            .cycle()
            .take(50_000)
            .collect();
        roundtrip(&rep);
    }

    #[test]
    fn roundtrip_random_and_mixed() {
        let mut st = 0xC0DEC_u64;
        for len in [1usize, 5, 12, 17, 255, 256, 300, 4096, 65_536, 100_003] {
            // Incompressible.
            let noise: Vec<u8> = (0..len).map(|_| splitmix(&mut st) as u8).collect();
            roundtrip(&noise);
            // Compressible with noise islands.
            let mixed: Vec<u8> = (0..len)
                .map(|i| {
                    if (i / 64) % 2 == 0 {
                        (i % 7) as u8
                    } else {
                        splitmix(&mut st) as u8
                    }
                })
                .collect();
            roundtrip(&mixed);
        }
    }

    #[test]
    fn long_runs_cross_length_extension_boundaries() {
        // Literal and match runs around the 15 / 15+255 token boundaries.
        for len in [14usize, 15, 16, 18, 19, 20, 269, 270, 271, 524, 525] {
            let lits: Vec<u8> = (0..len).map(|i| (i * 31 % 251) as u8).collect();
            roundtrip(&lits);
            let mut runs = vec![7u8; len];
            runs.extend_from_slice(b"tailnoise");
            roundtrip(&runs);
        }
    }

    #[test]
    fn corrupt_inputs_refuse_typed() {
        let input: Vec<u8> = b"compressible compressible compressible data data data"
            .iter()
            .copied()
            .cycle()
            .take(4096)
            .collect();
        let mut c = Vec::new();
        compress(&input, &mut c);
        let mut out = vec![0u8; input.len()];
        // Truncations at every prefix must refuse (or produce short output,
        // also refused) — never panic, never UB.
        for cut in 0..c.len().min(200) {
            let r = decompress_into(&c[..cut], &mut out);
            assert!(r.is_err(), "truncation at {cut} must refuse");
        }
        // Seeded byte corruption: refuse OR decode to full length; a full-
        // length wrong decode is caught one level up by the section CRC.
        let mut st = 0xBADF00D_u64;
        for _ in 0..500 {
            let mut bad = c.clone();
            let i = (splitmix(&mut st) as usize) % bad.len();
            bad[i] ^= (splitmix(&mut st) as u8) | 1;
            let _ = decompress_into(&bad, &mut out);
        }
        // Wrong expected size refuses.
        let mut short = vec![0u8; input.len() - 1];
        assert!(decompress_into(&c, &mut short).is_err());
        let mut long = vec![0u8; input.len() + 1];
        assert!(decompress_into(&c, &mut long).is_err());
    }

    #[test]
    fn offset_zero_and_overlong_offset_refuse() {
        // Hand-built: 4 literals then a match with offset 0.
        let mut bad = vec![0x40u8];
        bad.extend_from_slice(b"abcd");
        bad.extend_from_slice(&0u16.to_le_bytes());
        let mut out = vec![0u8; 12];
        assert!(decompress_into(&bad, &mut out).is_err());
        // Offset reaching before the output start.
        let mut bad2 = vec![0x40u8];
        bad2.extend_from_slice(b"abcd");
        bad2.extend_from_slice(&9u16.to_le_bytes());
        assert!(decompress_into(&bad2, &mut out).is_err());
    }
}
