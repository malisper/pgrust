//! Keyed 128-bit string-identity fingerprint for the sqe engine.
//!
//! `entry_fp128` = SipHash-2-4 with 128-bit output (the SipHash paper's
//! 128-bit variant, as used by ClickHouse uniqExact) over the entry
//! bytes, keyed by a boot-time random seed (`fp_seed`). Replaces the old
//! unkeyed wordwise mixer (`url_hash128`): identity decisions (fp-combine
//! planes, text128 folds, distinct sketches) now rest on a keyed PRF, so
//! collisions cannot be constructed offline against a fixed function.
//!
//! Seed lifetime = process lifetime = fpcache lifetime by design (ruling:
//! caches and seed die together). Fingerprints are never persisted across
//! processes and never compared across processes.
//!
//! Hand-written on purpose — repo convention: no new external crate
//! dependencies. Correctness is pinned by the official 64-entry
//! `vectors_sip128` table from the reference implementation
//! (veorq/SipHash) in the unit tests below.

use std::sync::OnceLock;

#[inline(always)]
fn sip_round(v0: &mut u64, v1: &mut u64, v2: &mut u64, v3: &mut u64) {
    *v0 = v0.wrapping_add(*v1);
    *v1 = v1.rotate_left(13);
    *v1 ^= *v0;
    *v0 = v0.rotate_left(32);
    *v2 = v2.wrapping_add(*v3);
    *v3 = v3.rotate_left(16);
    *v3 ^= *v2;
    *v0 = v0.wrapping_add(*v3);
    *v3 = v3.rotate_left(21);
    *v3 ^= *v0;
    *v2 = v2.wrapping_add(*v1);
    *v1 = v1.rotate_left(17);
    *v1 ^= *v2;
    *v2 = v2.rotate_left(32);
}

/// SipHash-2-4 with 128-bit output, keyed by (k0, k1).
///
/// Byte-exact against the reference implementation: `to_le_bytes()` of
/// the returned u128 equals the reference's 16-byte output (first
/// finalization word in the low half, second in the high half).
#[inline]
pub fn siphash128(k0: u64, k1: u64, b: &[u8]) -> u128 {
    let mut v0 = k0 ^ 0x736f_6d65_7073_6575u64;
    let mut v1 = k1 ^ 0x646f_7261_6e64_6f6du64;
    let mut v2 = k0 ^ 0x6c79_6765_6e65_7261u64;
    let mut v3 = k1 ^ 0x7465_6462_7974_6573u64;
    v1 ^= 0xee; // 128-bit output variant

    let chunks = b.chunks_exact(8);
    let rem = chunks.remainder();
    for c in chunks {
        let m = u64::from_le_bytes(c.try_into().expect("8"));
        v3 ^= m;
        sip_round(&mut v0, &mut v1, &mut v2, &mut v3);
        sip_round(&mut v0, &mut v1, &mut v2, &mut v3);
        v0 ^= m;
    }
    // Last block: remaining bytes little-endian, length in the top byte.
    let mut last = [0u8; 8];
    last[..rem.len()].copy_from_slice(rem);
    let m = u64::from_le_bytes(last) | ((b.len() as u64) << 56);
    v3 ^= m;
    sip_round(&mut v0, &mut v1, &mut v2, &mut v3);
    sip_round(&mut v0, &mut v1, &mut v2, &mut v3);
    v0 ^= m;

    v2 ^= 0xee;
    sip_round(&mut v0, &mut v1, &mut v2, &mut v3);
    sip_round(&mut v0, &mut v1, &mut v2, &mut v3);
    sip_round(&mut v0, &mut v1, &mut v2, &mut v3);
    sip_round(&mut v0, &mut v1, &mut v2, &mut v3);
    let lo = v0 ^ v1 ^ v2 ^ v3;

    v1 ^= 0xdd;
    sip_round(&mut v0, &mut v1, &mut v2, &mut v3);
    sip_round(&mut v0, &mut v1, &mut v2, &mut v3);
    sip_round(&mut v0, &mut v1, &mut v2, &mut v3);
    sip_round(&mut v0, &mut v1, &mut v2, &mut v3);
    let hi = v0 ^ v1 ^ v2 ^ v3;

    (lo as u128) | ((hi as u128) << 64)
}

/// Boot-time fingerprint seed: initialized once per process from OS
/// entropy (16 bytes of /dev/urandom). If entropy is unavailable, fail
/// LOUD — never fall back to a fixed constant (a fixed key would silently
/// reintroduce the offline-collision surface this lane removes).
///
/// `PGRUST_SQE_FP_SEED=<32 hex chars>` overrides the seed for
/// reproducible debugging ONLY (test/debug use; never set it in
/// production). Layout matches the reference key convention: first 16
/// hex chars = k0 bytes little-endian, last 16 = k1.
pub fn fp_seed() -> (u64, u64) {
    static SEED: OnceLock<(u64, u64)> = OnceLock::new();
    *SEED.get_or_init(|| {
        if let Ok(s) = std::env::var("PGRUST_SQE_FP_SEED") {
            let s = s.trim();
            assert!(
                s.len() == 32 && s.bytes().all(|c| c.is_ascii_hexdigit()),
                "PGRUST_SQE_FP_SEED must be exactly 32 hex chars, got {:?}",
                s
            );
            let mut kb = [0u8; 16];
            for (i, b) in kb.iter_mut().enumerate() {
                *b = u8::from_str_radix(&s[2 * i..2 * i + 2], 16).expect("hex");
            }
            return (
                u64::from_le_bytes(kb[..8].try_into().expect("8")),
                u64::from_le_bytes(kb[8..].try_into().expect("8")),
            );
        }
        let mut kb = [0u8; 16];
        {
            use std::io::Read;
            let mut f = std::fs::File::open("/dev/urandom")
                .expect("sqe fp_seed: /dev/urandom unavailable — refusing to run with a fixed fingerprint key");
            f.read_exact(&mut kb)
                .expect("sqe fp_seed: short read from /dev/urandom — refusing to run with a fixed fingerprint key");
        }
        (
            u64::from_le_bytes(kb[..8].try_into().expect("8")),
            u64::from_le_bytes(kb[8..].try_into().expect("8")),
        )
    })
}

/// 128-bit keyed string-identity fingerprint: SipHash-2-4-128 of the
/// entry bytes under the boot-time seed. THE identity function for every
/// fp plane in the crate (fp-combine, text128 folds, distinct sketches,
/// hot-shape face folds) — engine and oracle must call this same function so
/// differential identity keeps holding.
///
/// ~2-4x slower per byte than the old wordwise mixer; the fpcache ruling
/// amortizes it (fps built once per column per server lifetime).
#[inline(always)]
pub fn entry_fp128(b: &[u8]) -> u128 {
    let (k0, k1) = fp_seed();
    siphash128(k0, k1, b)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Official SipHash-2-4-128 test vectors (`vectors_sip128` from the
    /// reference implementation, veorq/SipHash): key = 00 01 .. 0f,
    /// input[i] = 0 1 .. i-1, for input lengths 0..=63.
    const VECTORS_SIP128: [&str; 64] = [
        "a3817f04ba25a8e66df67214c7550293",
        "da87c1d86b99af44347659119b22fc45",
        "8177228da4a45dc7fca38bdef60affe4",
        "9c70b60c5267a94e5f33b6b02985ed51",
        "f88164c12d9c8faf7d0f6e7c7bcd5579",
        "1368875980776f8854527a07690e9627",
        "14eeca338b208613485ea0308fd7a15e",
        "a1f1ebbed8dbc153c0b84aa61ff08239",
        "3b62a9ba6258f5610f83e264f31497b4",
        "264499060ad9baabc47f8b02bb6d71ed",
        "00110dc378146956c95447d3f3d0fbba",
        "0151c568386b6677a2b4dc6f81e5dc18",
        "d626b266905ef35882634df68532c125",
        "9869e247e9c08b10d029934fc4b952f7",
        "31fcefac66d7de9c7ec7485fe4494902",
        "5493e99933b0a8117e08ec0f97cfc3d9",
        "6ee2a4ca67b054bbfd3315bf85230577",
        "473d06e8738db89854c066c47ae47740",
        "a426e5e423bf4885294da481feaef723",
        "78017731cf65fab074d5208952512eb1",
        "9e25fc833f2290733e9344a5e83839eb",
        "568e495abe525a218a2214cd3e071d12",
        "4a29b54552d16b9a469c10528eff0aae",
        "c9d184ddd5a9f5e0cf8ce29a9abf691c",
        "2db479ae78bd50d8882a8a178a6132ad",
        "8ece5f042d5e447b5051b9eacb8d8f6f",
        "9c0b53b4b3c307e87eaee08678141f66",
        "abf248af69a6eae4bfd3eb2f129eeb94",
        "0664da1668574b88b935f3027358aef4",
        "aa4b9dc4bf337de90cd4fd3c467c6ab7",
        "ea5c7f471faf6bde2b1ad7d4686d2287",
        "2939b0183223fafc1723de4f52c43d35",
        "7c3956ca5eeafc3e363e9d556546eb68",
        "77c6077146f01c32b6b69d5f4ea9ffcf",
        "37a6986cb8847edf0925f0f1309b54de",
        "a705f0e69da9a8f907241a2e923c8cc8",
        "3dc47d1f29c448461e9e76ed904f6711",
        "0d62bf01e6fc0e1a0d3c4751c5d3692b",
        "8c03468bca7c669ee4fd5e084bbee7b5",
        "528a5bb93baf2c9c4473cce5d0d22bd9",
        "df6a301e95c95dad97ae0cc8c6913bd8",
        "801189902c857f39e73591285e70b6db",
        "e617346ac9c231bb3650ae34ccca0c5b",
        "27d93437efb721aa401821dcec5adf89",
        "89237d9ded9c5e78d8b1c9b166cc7342",
        "4a6d8091bf5e7d651189fa94a250b14c",
        "0e33f96055e7ae893ffc0e3dcf492902",
        "e61c432b720b19d18ec8d84bdc63151b",
        "f7e5aef549f782cf379055a608269b16",
        "438d030fd0b7a54fa837f2ad201a6403",
        "a590d3ee4fbf04e3247e0d27f286423f",
        "5fe2c1a172fe93c4b15cd37caef9f538",
        "2c97325cbd06b36eb2133dd08b3a017c",
        "92c814227a6bca949ff0659f002ad39e",
        "dce850110bd8328cfbd50841d6911d87",
        "67f14984c7da791248e32bb5922583da",
        "1938f2cf72d54ee97e94166fa91d2a36",
        "74481e9646ed49fe0f6224301604698e",
        "57fca5de98a9d6d8006438d0583d8a1d",
        "9fecde1cefdc1cbed4763674d9575359",
        "e3040c00eb28f15366ca73cbd872e740",
        "7697009a6a831dfecca91c5993670f7a",
        "5853542321f567a005d547a4f04759bd",
        "5150d1772f50834a503e069a973fbd7c",
    ];

    fn unhex16(s: &str) -> [u8; 16] {
        let mut out = [0u8; 16];
        for (i, b) in out.iter_mut().enumerate() {
            *b = u8::from_str_radix(&s[2 * i..2 * i + 2], 16).expect("hex");
        }
        out
    }

    #[test]
    fn siphash128_reference_vectors() {
        // Reference key: 00 01 02 .. 0f (little-endian words).
        let k0 = u64::from_le_bytes([0, 1, 2, 3, 4, 5, 6, 7]);
        let k1 = u64::from_le_bytes([8, 9, 10, 11, 12, 13, 14, 15]);
        let input: Vec<u8> = (0u8..64).collect();
        for (len, want) in VECTORS_SIP128.iter().enumerate() {
            let got = siphash128(k0, k1, &input[..len]).to_le_bytes();
            assert_eq!(
                got,
                unhex16(want),
                "vectors_sip128[{}] mismatch",
                len
            );
        }
    }

    #[test]
    fn entry_fp128_is_seeded_siphash() {
        let (k0, k1) = fp_seed();
        for b in [&b""[..], b"a", b"http://example.com/some/url?q=1", &[0u8; 300]] {
            assert_eq!(entry_fp128(b), siphash128(k0, k1, b));
        }
        // Distinct inputs must not collide on trivial cases.
        assert_ne!(entry_fp128(b"a"), entry_fp128(b"b"));
        assert_ne!(entry_fp128(b""), entry_fp128(&[0u8]));
    }

    /// Positive witness that the PGRUST_SQE_FP_SEED override actually
    /// reaches the seed: when the run pins the seed to the reference key
    /// 000102...0f, entry_fp128 must reproduce vectors_sip128 exactly.
    /// Under a random boot seed this asserts the opposite direction (the
    /// process seed is vanishingly unlikely to be the reference key).
    #[test]
    fn env_override_reaches_fp_seed() {
        let pinned_ref = std::env::var("PGRUST_SQE_FP_SEED")
            .map(|s| s.eq_ignore_ascii_case("000102030405060708090a0b0c0d0e0f"))
            .unwrap_or(false);
        let input: Vec<u8> = (0u8..64).collect();
        let matches_all = VECTORS_SIP128
            .iter()
            .enumerate()
            .all(|(len, want)| entry_fp128(&input[..len]).to_le_bytes() == unhex16(want));
        assert_eq!(matches_all, pinned_ref, "seed/env disagree");
    }
}
