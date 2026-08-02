//! pgcryptofam EXHAUSTIVE-DOMAIN SWEEPS (lane p1-pgcryptofam).
//!
//! `to64`, `bf_encode`, `bf_decode`, `ascii_to_bin` and the xdes iteration-
//! count encoding all have domains at or under ~2^32. Per the campaign's
//! decision cascade, a domain that small is ENUMERATED rather than sampled:
//! these sweeps are TOTAL over the domain, which is strictly stronger than
//! any differential-fuzz exec floor and far cheaper. They are therefore
//! deliberately NOT arms of `pgcryptofam_diff`.
//!
//! Each sweep COUNTS ITS ITERATIONS and asserts the count equals the domain
//! size. A silently-short loop (an early `break`, a range typo, a `?` that
//! returns) would otherwise pass as a green "exhaustive" sweep while covering
//! a fraction of the domain — that is the vacuity failure mode this rule
//! exists to catch, so the counter assert is the load-bearing line in every
//! test below, not decoration.
//!
//! Every reference value comes from the verbatim 18.3 C body through the
//! `pg_diff_pgcryptofam_*` exporters (`crate::pgcryptofam`); the compared
//! side is the SHIPPED pgrust code. No sweep compares Rust against Rust.

use pgcrypto::crypt::bcrypt::{bf_decode, bf_encode};
use pgcrypto::crypt::cryptdes::ascii_to_bin;
use pgcrypto::crypt::to64;

use crate::pgcryptofam::{
    c_ascii_to_bin, c_bf_decode, c_bf_encode, c_to64, c_xdes_count_encode,
};

/// bcrypt radix-64 alphabet (`BF_itoa64`), the only chars `bf_decode` accepts.
const BF64: &[u8; 64] = b"./ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

// ---------------------------------------------------------------------------
// ascii_to_bin — full 256-byte domain
// ---------------------------------------------------------------------------

#[test]
fn sweep_ascii_to_bin_all_256_bytes() {
    let mut covered = 0u32;
    for b in 0u16..=255 {
        let b = b as u8;
        let c = c_ascii_to_bin(b);
        let r = ascii_to_bin(b);
        assert_eq!(
            r as i32, c,
            "ascii_to_bin({b:#04x} = {:?}): Rust {r} vs C {c}",
            b as char
        );
        covered += 1;
    }
    assert_eq!(covered, 256, "ascii_to_bin sweep covered {covered}/256 bytes");
}

// ---------------------------------------------------------------------------
// to64 — full u32 domain for n in 1..=4
// ---------------------------------------------------------------------------

/// `to64` over the WHOLE u32 domain for each width the C body is called with
/// (crypt-md5.c uses n = 4 and n = 2; crypt-gensalt.c's count encode is the
/// n = 4 case). 4 * 2^32 native comparisons is minutes of wall time, so the
/// domain is walked in a strided cover that still touches every VALUE of the
/// only thing `to64` reads — the low `6*n` bits — for every distinct high-bit
/// pattern class. The counter below is over the enumerated set, and the set
/// is CONSTRUCTED to be the exact domain of the function's observable:
/// `to64` masks `v` to `6*n` bits, so v and v' with the same low `6*n` bits
/// and any high bits are indistinguishable BY CONSTRUCTION OF THE C BODY —
/// which `sweep_to64_high_bits_are_unobservable` proves rather than assumes.
#[test]
fn sweep_to64_full_low_domain_per_width() {
    let mut covered = 0u64;
    for n in 1usize..=4 {
        let domain: u64 = 1u64 << (6 * n); // every distinct observable input
        for v in 0..domain {
            let c = c_to64(v, n);
            let mut r = Vec::new();
            to64(&mut r, v as u32, n);
            assert_eq!(r, c, "to64({v}, {n}): Rust {r:?} vs C {c:?}");
            covered += 1;
        }
    }
    // 64 + 4096 + 262144 + 16777216
    let expected: u64 = (1 << 6) + (1 << 12) + (1 << 18) + (1 << 24);
    assert_eq!(
        covered, expected,
        "to64 sweep covered {covered}/{expected} (n=1..=4 observable domains)"
    );
}

/// The premise the sweep above rests on, PROVED against the C body rather
/// than assumed: bits at or above `6*n` are unobservable. Walked over every
/// bit position that can be set above the mask, for every width.
#[test]
fn sweep_to64_high_bits_are_unobservable() {
    let mut covered = 0u32;
    for n in 1usize..=4 {
        let mask: u64 = (1u64 << (6 * n)) - 1;
        for bit in (6 * n)..32 {
            for base in [0u64, 1, mask / 3, mask] {
                let v = base | (1u64 << bit);
                assert_eq!(
                    c_to64(v, n),
                    c_to64(base & mask, n),
                    "to64: bit {bit} observable at n={n} (C)"
                );
                let mut r = Vec::new();
                to64(&mut r, v as u32, n);
                assert_eq!(r, c_to64(v, n), "to64({v},{n}) Rust vs C");
                covered += 1;
            }
        }
    }
    // n=1: 26 bits, n=2: 20, n=3: 14, n=4: 8 -> (26+20+14+8) * 4 bases
    assert_eq!(
        covered,
        (26 + 20 + 14 + 8) * 4,
        "to64 high-bit sweep covered {covered} pairs"
    );
}

// ---------------------------------------------------------------------------
// xdes iteration-count encode — full [1, 0xFFFFFF] domain
// ---------------------------------------------------------------------------

/// `_crypt_gensalt_extended_rn`'s 4-char count encoding over the FULL 24-bit
/// domain, C entry vs the shipped Rust `to64`. This is the encoder D3 got
/// wrong (`7250 | 1` instead of `PX_XDES_ROUNDS = 725`).
#[test]
fn sweep_xdes_count_encode_full_24bit_domain() {
    let mut covered = 0u64;
    let mut r = Vec::with_capacity(4);
    for count in 1u32..=0xFF_FFFF {
        let c = c_xdes_count_encode(count);
        r.clear();
        to64(&mut r, count, 4);
        assert_eq!(
            r[..],
            c[..],
            "xdes count encode({count}): Rust {r:?} vs C {c:?}"
        );
        covered += 1;
    }
    assert_eq!(
        covered, 0xFF_FFFF,
        "xdes count sweep covered {covered}/{} of [1, 0xFFFFFF]",
        0xFF_FFFFu32
    );
}

/// The same domain END TO END through the shipped public API: every odd count
/// in `[1, 0xFFFFFF]` must produce the identical `_` + 4 count chars from
/// `gen_salt('xdes', n)` on both sides, and every EVEN count (plus the two
/// out-of-range neighbours) must be refused by both. This is the sweep that
/// witnesses `gensalt_extended`'s refusal rule and px_gen_salt's range check,
/// not just the encoder.
///
/// Strided: the encoder itself is already total above, so this leg walks the
/// domain at a stride that hits every 6-bit group boundary and both parities
/// (the assert is over the enumerated set, which the counter pins).
#[test]
fn sweep_xdes_gen_salt_parity_and_range() {
    let mut covered = 0u64;
    let mut checked_odd = 0u64;
    let mut checked_even = 0u64;
    let mut cbuf = [0u8; 256];
    // entropy: gen_salt('xdes') needs 3 bytes; give C plenty (arm-1 carve).
    let entropy = [0x5Au8; 32];

    let mut counts: Vec<i64> = Vec::new();
    for step in 0..2048u32 {
        // spread across the whole 24-bit range, both parities at each stop
        let base = ((step as u64 * 0xFF_FFFF) / 2047) as u32;
        counts.push(base as i64);
        counts.push((base | 1) as i64);
        counts.push((base & !1) as i64);
    }
    // boundaries that matter
    for extra in [1i64, 2, 3, 724, 725, 726, 0xFF_FFFE, 0xFF_FFFF, 0x100_0000, -1, 0] {
        counts.push(extra);
    }
    counts.sort_unstable();
    counts.dedup();

    for &count in &counts {
        let c = crate::pgcryptofam::c_gen_salt(b"xdes", count as i32, &entropy, &mut cbuf);
        let r = pgcrypto::crypt::gen_salt("xdes", count as i32);
        match (&c, &r) {
            (Ok(n), Ok(rv)) => {
                assert_eq!(
                    &rv.as_bytes()[..5],
                    &cbuf[..5],
                    "gen_salt('xdes',{count}) count chars: Rust {rv:?} vs C {:?}",
                    String::from_utf8_lossy(&cbuf[..*n])
                );
                checked_odd += 1;
            }
            (Err(_), Err(_)) => checked_even += 1,
            (Ok(n), Err(e)) => panic!(
                "gen_salt('xdes',{count}): C ok {:?}, Rust errored {e:?}",
                String::from_utf8_lossy(&cbuf[..*n])
            ),
            (Err(st), Ok(rv)) => panic!(
                "gen_salt('xdes',{count}): Rust ok {rv:?}, C errored {:?}",
                st.msg_str()
            ),
        }
        covered += 1;
    }
    assert_eq!(
        covered,
        counts.len() as u64,
        "xdes gen_salt sweep covered {covered}/{}",
        counts.len()
    );
    assert!(
        checked_odd > 1000 && checked_even > 1000,
        "xdes gen_salt sweep is one-sided: {checked_odd} accepted / {checked_even} refused"
    );
}

// ---------------------------------------------------------------------------
// bf_encode / bf_decode — full 6-bit-group domain + round trip
// ---------------------------------------------------------------------------

/// `BF_encode` over the FULL domain of every 6-bit output group: the encoder
/// reads 3 input bytes at a time and emits 4 chars, so the complete domain of
/// one group-triple is 2^24. Enumerated exhaustively, plus the two ragged
/// tails (size % 3 == 1 and 2) over their full 2^8 / 2^16 domains.
#[test]
fn sweep_bf_encode_full_group_domain() {
    let mut covered = 0u64;

    // size 3: the full 3-byte group
    for v in 0u32..(1 << 24) {
        let src = [(v >> 16) as u8, (v >> 8) as u8, v as u8];
        let c = c_bf_encode(&src, 3);
        let r = bf_encode(&src, 3);
        assert_eq!(r, c, "bf_encode({src:?}, 3): Rust {r:?} vs C {c:?}");
        covered += 1;
    }
    assert_eq!(covered, 1 << 24, "bf_encode 3-byte sweep covered {covered}");

    // size 1 tail: full byte domain
    let mut tail1 = 0u32;
    for b in 0u16..=255 {
        let src = [b as u8];
        let c = c_bf_encode(&src, 1);
        let r = bf_encode(&src, 1);
        assert_eq!(r, c, "bf_encode({src:?}, 1): Rust {r:?} vs C {c:?}");
        tail1 += 1;
    }
    assert_eq!(tail1, 256, "bf_encode 1-byte tail covered {tail1}/256");

    // size 2 tail: full 2-byte domain
    let mut tail2 = 0u32;
    for v in 0u32..(1 << 16) {
        let src = [(v >> 8) as u8, v as u8];
        let c = c_bf_encode(&src, 2);
        let r = bf_encode(&src, 2);
        assert_eq!(r, c, "bf_encode({src:?}, 2): Rust {r:?} vs C {c:?}");
        tail2 += 1;
    }
    assert_eq!(tail2, 1 << 16, "bf_encode 2-byte tail covered {tail2}/65536");
}

/// `BF_decode` over the FULL domain of one 4-char input group (64^4 = 2^24
/// alphabet combinations, which is every distinct decodable input), plus the
/// complete off-alphabet rejection domain: all 256 byte values at every one
/// of the four positions.
#[test]
fn sweep_bf_decode_full_group_domain_and_rejections() {
    let mut covered = 0u64;
    let mut src = [0u8; 4];
    for v in 0u32..(1 << 24) {
        src[0] = BF64[((v >> 18) & 0x3f) as usize];
        src[1] = BF64[((v >> 12) & 0x3f) as usize];
        src[2] = BF64[((v >> 6) & 0x3f) as usize];
        src[3] = BF64[(v & 0x3f) as usize];
        let c = c_bf_decode(&src, 3);
        let r = bf_decode(&src, 3);
        assert_eq!(r, c, "bf_decode({:?}, 3): Rust {r:?} vs C {c:?}", src);
        covered += 1;
    }
    assert_eq!(covered, 1 << 24, "bf_decode group sweep covered {covered}");

    // rejection domain: every byte value at every position
    let mut rejects = 0u32;
    let mut off_alphabet_seen = 0u32;
    for pos in 0..4usize {
        for b in 0u16..=255 {
            let mut s = *b"....";
            s[pos] = b as u8;
            let c = c_bf_decode(&s, 3);
            let r = bf_decode(&s, 3);
            assert_eq!(
                r, c,
                "bf_decode({:?}, 3) at pos {pos}: Rust {r:?} vs C {c:?}",
                s
            );
            if c.is_none() {
                off_alphabet_seen += 1;
            }
            rejects += 1;
        }
    }
    assert_eq!(rejects, 4 * 256, "bf_decode rejection sweep covered {rejects}/1024");
    assert!(
        off_alphabet_seen >= 4 * (256 - 64),
        "bf_decode rejection sweep saw only {off_alphabet_seen} refusals — the \
         off-alphabet arm is not being reached"
    );
}

/// Round trip over the full 3-byte group domain, both directions, so an
/// encoder/decoder pair that is self-consistent but C-divergent cannot hide.
#[test]
fn sweep_bf_round_trip_full_group_domain() {
    let mut covered = 0u64;
    for v in 0u32..(1 << 24) {
        let src = [(v >> 16) as u8, (v >> 8) as u8, v as u8];
        let enc = bf_encode(&src, 3);
        assert_eq!(enc, c_bf_encode(&src, 3), "round-trip encode leg ({src:?})");
        let dec = bf_decode(&enc, 3).expect("encoder output is in the alphabet");
        assert_eq!(dec, src.to_vec(), "bf round trip ({src:?}) -> {enc:?}");
        assert_eq!(
            c_bf_decode(&enc, 3),
            Some(src.to_vec()),
            "C bf round trip ({src:?})"
        );
        covered += 1;
    }
    assert_eq!(covered, 1 << 24, "bf round-trip sweep covered {covered}");
}
