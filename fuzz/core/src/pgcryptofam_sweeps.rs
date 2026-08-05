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
//! test below, not decoration. (Verified by construction: truncating three of
//! these loops made exactly those asserts fire.)
//!
//! ROUTE. All five helpers are FILE-STATIC in C and PRIVATE in pgrust, and
//! the product's visibility is not widened for a harness (coordinator ruling,
//! 2026-08-02). Each sweep therefore drives the pgrust side through the
//! SHIPPED fc wrapper that contains the helper, choosing the wrapper and the
//! input position so that the helper's ENTIRE domain is still enumerated:
//!
//!   helper            pgrust route                       domain swept
//!   ---------------   --------------------------------   --------------------
//!   ascii_to_bin      fc pg_crypt, `_`+count chars        all 256 byte values
//!                     (two independent 6-bit positions)   x 2 positions
//!   to64 (n = 4) /    fc pg_gen_salt_rounds('xdes', n)    every count in
//!   xdes count enc.   -> `_` + 4 deterministic chars      [1, 0xFFFFFF]
//!   bf_decode         fc pg_crypt, `$2a$04$` + 22 chars   all 256 byte values
//!                                                         x every position
//!
//!   bf_encode and to64 at n = 1..3 have NO addressable shipped entry point:
//!   their outputs only ever appear as a function of values the caller cannot
//!   choose (bcrypt ciphertext, gen_salt entropy, md5 digest words). Those
//!   two are covered TRANSITIVELY — every bcrypt hash compared byte-for-byte
//!   in arm 0 is 31 chars of `bf_encode` output, and every `$1$` hash is 22
//!   chars of `to64` output — and the ORACLE side is additionally swept over
//!   its full domain below so an oracle regression cannot hide. Those two
//!   sweeps are labelled ORACLE-INTEGRITY and are explicitly NOT
//!   pgrust-vs-C differentials; nothing here compares Rust against Rust.

use crate::pgcryptofam::{
    c_ascii_to_bin, c_bf_decode, c_bf_encode, c_crypt_status, c_gen_salt_status, c_to64,
    c_xdes_count_encode,
};
use crate::pgcryptofam_diff::{fc_call, guc_store_ready, lookup, result_payload, seams_setup, text_image};
use datum::Datum;

/// itoa64 (`_crypt_itoa64`), the xdes count/salt alphabet.
const ITOA64: &[u8; 64] = b"./0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
/// bcrypt radix-64 alphabet (`BF_itoa64`), the only chars `bf_decode` accepts.
const BF64: &[u8; 64] = b"./ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

/// One `crypt(pw, setting)` through the SHIPPED wrapper, against the C
/// oracle. Returns `(rust_bytes, c_bytes)` on mutual success, `None` when
/// both sides refuse (with the SQLSTATEs compared), and panics on any
/// one-sided verdict.
fn crypt_pair(pw: &[u8], setting: &[u8]) -> Option<(Vec<u8>, Vec<u8>)> {
    let mut out = vec![0u8; 1024];
    let (cn, cst) = c_crypt_status(pw, setting, &mut out);
    let ctx = mcx::MemoryContext::new("pgcryptofam_sweep");
    let pwi = text_image(pw);
    let sti = text_image(setting);
    let fc = fc_call(
        lookup("pg_crypt"),
        ctx.mcx(),
        [
            Datum::from_usize(pwi.as_ptr() as usize),
            Datum::from_usize(sti.as_ptr() as usize),
        ],
    );
    match (cn, fc) {
        (Some(n), Ok(d)) => {
            // SAFETY: fc_pg_crypt returns a live text varlena in ctx.
            let rv = unsafe { result_payload(d) }.to_vec();
            Some((rv, out[..n].to_vec()))
        }
        (None, Err(e)) => {
            assert_eq!(
                e.sqlstate.0,
                cst.sqlstate,
                "crypt({:?}) SQLSTATE: Rust {:?} vs C {}",
                String::from_utf8_lossy(setting),
                e.message,
                cst.sqlstate
            );
            None
        }
        (Some(_), Err(e)) => panic!(
            "crypt({:?}): C ok, Rust errored {:?}",
            String::from_utf8_lossy(setting),
            e.message
        ),
        (None, Ok(_)) => panic!(
            "crypt({:?}): Rust ok, C errored {:?}",
            String::from_utf8_lossy(setting),
            cst.msg_str()
        ),
    }
}

// ---------------------------------------------------------------------------
// ascii_to_bin — full 256-byte domain, twice
// ---------------------------------------------------------------------------

/// `ascii_to_bin` (crypt-des.c) over ALL 256 byte values, observed through
/// the shipped `pg_crypt` wrapper at two independent 6-bit positions of the
/// xdes iteration count (`setting[1]`, shift 0, and `setting[2]`, shift 6).
/// The decoded count is the whole observable: a wrong decode changes the
/// number of DES iterations and therefore the hash, or crosses the count == 0
/// boundary and changes the VERDICT. The C helper entry
/// `pg_diff_pgcryptofam_ascii_to_bin` supplies the reference value each byte
/// must decode to, so the sweep also pins WHICH count each byte selects
/// instead of only checking that the two sides agree.
///
/// Both positions stay far under the driver's xdes cost pin: position 1 gives
/// count <= 63, position 2 gives count <= 63 << 6 = 4032.
#[test]
fn sweep_ascii_to_bin_all_256_bytes_through_crypt() {
    seams_setup();
    assert!(guc_store_ready(), "GUC store needed for the fc route");
    let mut covered = 0u32;
    let mut executed = 0u32;
    let mut nul_carved = 0u32;
    let mut zero_count_seen = 0u32;
    let mut hashed = 0u32;
    for pos in 1usize..=2 {
        for b in 0u16..=255 {
            let b = b as u8;
            // The C helper entry takes a RAW byte, so its 256-value domain is
            // enumerated unconditionally; this is the reference each byte's
            // decode is pinned to.
            let decoded = c_ascii_to_bin(b) as u32;
            assert_eq!(
                decoded,
                itoa64_index(b).unwrap_or(0),
                "C ascii_to_bin({b:#04x}) off the ./0-9A-Za-z contract"
            );
            covered += 1;
            if b == 0 {
                // DOMAIN CARVE (same as the driver's `text_field`): PG `text`
                // cannot carry NUL, and C stops at it (strlen) while the
                // shipped Rust is slice-based, so an embedded NUL is a
                // documented non-SQL-reachable difference (lane p1-pgcrypto
                // GROUND-TRUTH §F), not a defect. The decode itself is still
                // pinned above for this byte.
                nul_carved += 1;
                continue;
            }
            let count = decoded << ((pos - 1) * 6);
            let mut setting = *b"_....abcd";
            setting[pos] = b;
            match crypt_pair(b"password", &setting) {
                Some((rv, cv)) => {
                    assert_eq!(
                        rv, cv,
                        "xdes crypt with setting[{pos}] = {b:#04x} (count {count})"
                    );
                    // The count chars are copied verbatim into the output, so
                    // a decode-side defect that ALSO changed them would be
                    // caught here rather than only in the hash.
                    assert_eq!(&cv[..5], &setting[..5], "count chars are copied out");
                    hashed += 1;
                }
                None => {
                    // count == 0 is C's NULL return ("crypt(3) returned NULL")
                    assert_eq!(count, 0, "setting[{pos}]={b:#04x} refused with count {count}");
                    zero_count_seen += 1;
                }
            }
            executed += 1;
        }
    }
    assert_eq!(
        covered, 512,
        "ascii_to_bin sweep covered {covered}/512 (256 bytes x 2 positions)"
    );
    assert_eq!(nul_carved, 2, "NUL carve fired {nul_carved} times, expected 2");
    assert_eq!(executed, 510, "crypt-observed legs: {executed}/510");
    // 192 of the 256 bytes are off the itoa64 alphabet and decode to 0; '.'
    // is on the alphabet and also decodes to 0. Minus the carved NUL, that is
    // 192 zero-count bytes per position.
    assert_eq!(
        zero_count_seen,
        2 * 192,
        "expected 192 zero-count bytes per position, saw {zero_count_seen} total"
    );
    assert_eq!(hashed, 510 - 2 * 192, "hashing legs: {hashed}");
    println!(
        "SWEEP ascii_to_bin: {covered}/512 decode values pinned against the C entry \
         (all 256 bytes x 2 count positions); {executed} run through fc pg_crypt \
         ({hashed} hashed / {zero_count_seen} count-zero refusals), {nul_carved} NUL-carved"
    );
}

/// Index of `b` in the itoa64 alphabet — the value `ascii_to_bin` must
/// report. `None` for every byte outside `./0-9A-Za-z` (C returns 0 for all
/// of them, which the caller folds in).
fn itoa64_index(b: u8) -> Option<u32> {
    ITOA64.iter().position(|&c| c == b).map(|p| p as u32)
}

// ---------------------------------------------------------------------------
// xdes iteration-count encode (to64 at n = 4) — full [1, 0xFFFFFF] domain
// ---------------------------------------------------------------------------

/// `gen_salt('xdes', n)` emits `_` + `to64(n, 4)` — four itoa64 chars of the
/// count, low 6 bits first — and everything after those 5 bytes is entropy.
/// This walks the ENTIRE `[1, 0xFFFFFF]` count domain through the shipped
/// `pg_gen_salt_rounds` wrapper and compares that deterministic 5-byte prefix
/// against the C oracle, so it is a total pgrust-vs-C sweep of the encoder
/// D3 got wrong (`7250 | 1` instead of `PX_XDES_ROUNDS = 725`) AND of
/// `_crypt_gensalt_extended_rn`'s even-count refusal AND of px_gen_salt's
/// `[1, 0xFFFFFF]` range check — all three in one enumeration.
///
/// Marked `#[ignore]`: 16.7M paired wrapper calls, each drawing OS entropy on
/// the pgrust side, is minutes of wall time — too slow for the default
/// `cargo test` gate, and the campaign's rule is that an exhaustive sweep
/// must be RUN and RECORDED, not run on every edit. The default-gated
/// `sweep_xdes_count_encode_boundaries` below walks the same encoder over the
/// boundary-and-stride set on every run.
///   cargo test --release -p decoder_fuzz \
///     pgcryptofam_sweeps::sweep_xdes_count_encode_full_24bit_domain -- --ignored
#[test]
#[ignore = "16.7M paired gen_salt calls (minutes); run explicitly and record"]
fn sweep_xdes_count_encode_full_24bit_domain() {
    let (covered, odd, even) = xdes_count_sweep(1..=0xFF_FFFF);
    assert_eq!(
        covered, 0xFF_FFFF,
        "xdes count sweep covered {covered}/{} of [1, 0xFFFFFF]",
        0xFF_FFFFu32
    );
    println!(
        "SWEEP xdes count encode: {covered}/{} of [1, 0xFFFFFF] \
         ({odd} accepted / {even} refused)",
        0xFF_FFFFu32
    );
}

/// The default-gated leg of the sweep above: every boundary value plus a
/// stride that hits all four 6-bit groups and both parities.
#[test]
fn sweep_xdes_count_encode_boundaries() {
    let mut counts: Vec<u32> = Vec::new();
    for step in 0..4096u32 {
        let base = ((step as u64 * 0xFF_FFFF) / 4095) as u32;
        counts.push(base);
        counts.push(base | 1);
        counts.push(base & !1);
    }
    for extra in [1, 2, 3, 63, 64, 65, 724, 725, 726, 4095, 4096, 0xFF_FFFE, 0xFF_FFFF] {
        counts.push(extra);
    }
    counts.retain(|&c| c >= 1);
    counts.sort_unstable();
    counts.dedup();
    let want = counts.len() as u64;
    let (covered, odd, even) = xdes_count_sweep_over(&counts);
    assert_eq!(covered, want, "xdes boundary sweep covered {covered}/{want}");
    assert!(
        odd > 1000 && even > 1000,
        "xdes boundary sweep is one-sided: {odd} accepted / {even} refused"
    );
    println!("SWEEP xdes count encode (boundaries): {covered}/{want} ({odd} accepted / {even} refused)");
}

fn xdes_count_sweep(range: std::ops::RangeInclusive<u32>) -> (u64, u64, u64) {
    let counts: Vec<u32> = range.collect();
    xdes_count_sweep_over(&counts)
}

fn xdes_count_sweep_over(counts: &[u32]) -> (u64, u64, u64) {
    seams_setup();
    assert!(guc_store_ready(), "GUC store needed for the fc route");
    let entropy = [0x5Au8; 32];
    let algo = text_image(b"xdes");
    let f = lookup("pg_gen_salt_rounds");
    let mut cbuf = [0u8; 256];
    let (mut covered, mut odd, mut even) = (0u64, 0u64, 0u64);
    for &count in counts {
        let (cn, cst) = c_gen_salt_status(b"xdes", count as i32, &entropy, &mut cbuf);
        let ctx = mcx::MemoryContext::new("pgcryptofam_sweep");
        let fc = fc_call(
            f,
            ctx.mcx(),
            [
                Datum::from_usize(algo.as_ptr() as usize),
                Datum::from_i32(count as i32),
            ],
        );
        match (cn, fc) {
            (Some(_), Ok(d)) => {
                // SAFETY: fc_pg_gen_salt_rounds returns a live text varlena.
                let rv = unsafe { result_payload(d) };
                let want: Vec<u8> = std::iter::once(b'_')
                    .chain((0..4).map(|i| ITOA64[((count >> (6 * i)) & 0x3f) as usize]))
                    .collect();
                assert_eq!(
                    &cbuf[..5],
                    &want[..],
                    "C xdes count chars for {count} are not to64(count,4)"
                );
                assert_eq!(
                    &rv[..5],
                    &cbuf[..5],
                    "gen_salt('xdes',{count}) count chars: Rust {:?} vs C {:?}",
                    String::from_utf8_lossy(&rv[..5]),
                    String::from_utf8_lossy(&cbuf[..5])
                );
                odd += 1;
            }
            (None, Err(e)) => {
                assert_eq!(
                    e.sqlstate.0, cst.sqlstate,
                    "gen_salt('xdes',{count}) refusal SQLSTATE"
                );
                even += 1;
            }
            (Some(_), Err(e)) => panic!(
                "gen_salt('xdes',{count}): C ok, Rust errored {:?}",
                e.message
            ),
            (None, Ok(_)) => panic!(
                "gen_salt('xdes',{count}): Rust ok, C errored {:?}",
                cst.msg_str()
            ),
        }
        covered += 1;
    }
    (covered, odd, even)
}

// ---------------------------------------------------------------------------
// bf_decode — full per-position byte domain through the bcrypt salt
// ---------------------------------------------------------------------------

/// `BF_decode` (crypt-blowfish.c) is reached through the 22 radix-64 salt
/// chars of a `$2a$04$` setting. This walks ALL 256 byte values at EVERY one
/// of the 22 positions: an off-alphabet byte makes both sides raise "invalid
/// salt", and an on-alphabet byte changes the decoded 16-byte salt and
/// therefore the hash. Total over the per-position domain (22 x 256), which
/// is the complete domain of the alphabet test the function performs.
#[test]
fn sweep_bf_decode_all_bytes_at_every_salt_position() {
    seams_setup();
    assert!(guc_store_ready(), "GUC store needed for the fc route");
    let mut covered = 0u32;
    let mut accepted = 0u32;
    let mut refused = 0u32;
    for pos in 0..22usize {
        for b in 0u16..=255 {
            let b = b as u8;
            let mut salt = [b'.'; 22];
            salt[pos] = b;
            let mut setting = Vec::with_capacity(29);
            setting.extend_from_slice(b"$2a$04$");
            setting.extend_from_slice(&salt);
            match crypt_pair(b"foox", &setting) {
                Some((rv, cv)) => {
                    assert_eq!(rv, cv, "bcrypt salt[{pos}] = {b:#04x}");
                    assert!(BF64.contains(&b), "off-alphabet byte {b:#04x} was accepted");
                    accepted += 1;
                }
                None => {
                    assert!(
                        !BF64.contains(&b),
                        "on-alphabet byte {b:#04x} was refused at position {pos}"
                    );
                    refused += 1;
                }
            }
            covered += 1;
        }
    }
    assert_eq!(
        covered,
        22 * 256,
        "bf_decode sweep covered {covered}/{} (22 positions x 256 bytes)",
        22 * 256
    );
    assert_eq!(accepted, 22 * 64, "on-alphabet legs: {accepted}");
    assert_eq!(refused, 22 * 192, "off-alphabet legs: {refused}");
    println!(
        "SWEEP bf_decode: {covered}/{} (all 256 byte values x 22 bcrypt salt positions); \
         {accepted} accepted / {refused} refused",
        22 * 256
    );
}

// ---------------------------------------------------------------------------
// ORACLE-INTEGRITY sweeps (NOT pgrust-vs-C — see the module banner)
// ---------------------------------------------------------------------------

/// `BF_encode` over the FULL domain of one 3-byte group (2^24) plus both
/// ragged tails, C entry vs the documented `BF_itoa64` packing contract.
/// The pgrust `bf_encode` has no addressable shipped entry point, so this is
/// ORACLE INTEGRITY ONLY: it proves the oracle's exported encoder still obeys
/// the bcrypt radix-64 contract, and it round-trips that output through
/// `BF_decode` so an encoder/decoder pair that drifted together is caught.
/// The pgrust encoder is covered transitively by arm 0's bcrypt value plane
/// (31 of every `$2a$` hash's characters are its output, compared byte-exact).
#[test]
fn oracle_sweep_bf_encode_full_group_domain() {
    let mut covered = 0u64;
    for v in 0u32..(1 << 24) {
        let src = [(v >> 16) as u8, (v >> 8) as u8, v as u8];
        let enc = c_bf_encode(&src, 3);
        let want = [
            BF64[(src[0] >> 2) as usize],
            BF64[(((src[0] & 0x03) << 4) | (src[1] >> 4)) as usize],
            BF64[(((src[1] & 0x0f) << 2) | (src[2] >> 6)) as usize],
            BF64[(src[2] & 0x3f) as usize],
        ];
        assert_eq!(enc, want, "C BF_encode({src:?}) off the BF_itoa64 contract");
        assert_eq!(
            c_bf_decode(&enc, 3),
            Some(src.to_vec()),
            "C bf round trip ({src:?})"
        );
        covered += 1;
    }
    assert_eq!(covered, 1 << 24, "bf_encode oracle sweep covered {covered}");

    let mut tail1 = 0u32;
    for b in 0u16..=255 {
        let src = [b as u8];
        let enc = c_bf_encode(&src, 1);
        assert_eq!(enc.len(), 2, "1-byte tail emits 2 chars");
        assert_eq!(enc[0], BF64[(src[0] >> 2) as usize]);
        assert_eq!(enc[1], BF64[((src[0] & 0x03) << 4) as usize]);
        tail1 += 1;
    }
    assert_eq!(tail1, 256, "bf_encode 1-byte tail covered {tail1}/256");

    let mut tail2 = 0u32;
    for v in 0u32..(1 << 16) {
        let src = [(v >> 8) as u8, v as u8];
        let enc = c_bf_encode(&src, 2);
        assert_eq!(enc.len(), 3, "2-byte tail emits 3 chars");
        assert_eq!(enc[0], BF64[(src[0] >> 2) as usize]);
        assert_eq!(enc[1], BF64[(((src[0] & 0x03) << 4) | (src[1] >> 4)) as usize]);
        assert_eq!(enc[2], BF64[((src[1] & 0x0f) << 2) as usize]);
        tail2 += 1;
    }
    assert_eq!(tail2, 1 << 16, "bf_encode 2-byte tail covered {tail2}/65536");
    println!(
        "SWEEP bf_encode (ORACLE INTEGRITY): {covered}/16777216 3-byte groups, \
         {tail1}/256 + {tail2}/65536 tails"
    );
}

/// `BF_decode`'s off-alphabet rejection domain against the oracle entry: all
/// 256 byte values at every one of the four positions of a group. The pgrust
/// side of the same domain is swept through the bcrypt salt above; this leg
/// pins the ORACLE's own refusal set so a drifting oracle cannot make the
/// pgrust sweep vacuously agree.
#[test]
fn oracle_sweep_bf_decode_rejection_domain() {
    let mut covered = 0u32;
    let mut refused = 0u32;
    for pos in 0..4usize {
        for b in 0u16..=255 {
            let mut s = *b"....";
            s[pos] = b as u8;
            let got = c_bf_decode(&s, 3);
            assert_eq!(
                got.is_none(),
                !BF64.contains(&(b as u8)),
                "C BF_decode refusal set disagrees at {b:#04x}"
            );
            if got.is_none() {
                refused += 1;
            }
            covered += 1;
        }
    }
    assert_eq!(covered, 4 * 256, "bf_decode rejection sweep covered {covered}/1024");
    assert_eq!(refused, 4 * 192, "off-alphabet refusals: {refused}");
    println!(
        "SWEEP bf_decode rejections (ORACLE INTEGRITY): {covered}/1024 ({refused} refused)"
    );
}

/// `_crypt_to64` over the WHOLE observable domain for every width the C body
/// is called with (crypt-md5.c uses n = 4 and n = 2; the xdes count encode is
/// the n = 4 case). ORACLE INTEGRITY: pgrust's `to64` is only addressable at
/// n = 4 (the xdes sweep above, total over [1, 0xFFFFFF]); n = 1..3 appear
/// only as a function of md5 digest words the caller cannot choose, and are
/// covered transitively by arm 0's `$1$` value plane.
///
/// `to64` masks `v` to the low `6*n` bits, so the observable domain is
/// `2^(6n)` — which `oracle_sweep_to64_high_bits_are_unobservable` PROVES
/// against the C body rather than assuming.
#[test]
fn oracle_sweep_to64_full_observable_domain_per_width() {
    let mut covered = 0u64;
    for n in 1usize..=4 {
        let domain: u64 = 1u64 << (6 * n);
        for v in 0..domain {
            let c = c_to64(v, n);
            let want: Vec<u8> = (0..n).map(|i| ITOA64[((v >> (6 * i)) & 0x3f) as usize]).collect();
            assert_eq!(c, want, "C to64({v}, {n}) off the itoa64 contract");
            covered += 1;
        }
    }
    let expected: u64 = (1 << 6) + (1 << 12) + (1 << 18) + (1 << 24);
    assert_eq!(
        covered, expected,
        "to64 sweep covered {covered}/{expected} (n = 1..=4 observable domains)"
    );
    // ...and the n = 4 slice must agree with the dedicated xdes exporter.
    let mut xdes = 0u32;
    for v in [1u32, 2, 63, 64, 725, 4095, 0x155555, 0xFF_FFFF] {
        assert_eq!(c_xdes_count_encode(v)[..], c_to64(v as u64, 4)[..]);
        xdes += 1;
    }
    assert_eq!(xdes, 8);
    println!(
        "SWEEP to64 (ORACLE INTEGRITY): {covered}/{expected} (n = 1..=4 observable domains)"
    );
}

/// The premise the sweep above rests on, PROVED against the C body rather
/// than assumed: bits at or above `6*n` are unobservable. Walked over every
/// bit position that can be set above the mask, for every width.
#[test]
fn oracle_sweep_to64_high_bits_are_unobservable() {
    let mut covered = 0u32;
    for n in 1usize..=4 {
        let mask: u64 = (1u64 << (6 * n)) - 1;
        for bit in (6 * n)..32 {
            for base in [0u64, 1, mask / 3, mask] {
                let v = base | (1u64 << bit);
                assert_eq!(
                    c_to64(v, n),
                    c_to64(base & mask, n),
                    "to64: bit {bit} observable at n = {n} (C)"
                );
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
    println!(
        "SWEEP to64 high-bit unobservability (ORACLE INTEGRITY): {covered}/{} pairs",
        (26 + 20 + 14 + 8) * 4
    );
}
