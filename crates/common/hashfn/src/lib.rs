#![no_std]

#[cfg(target_endian = "big")]
compile_error!("only the little-endian tail layout of hashfn.c is implemented");

const INITVAL: u32 = 0x9e37_79b9;
const SALT: u32 = 3_923_095;

#[inline(always)]
fn init(len: usize) -> u32 {
    debug_assert!(len <= i32::MAX as usize);
    INITVAL.wrapping_add(len as u32).wrapping_add(SALT)
}

#[inline(always)]
fn mix(mut a: u32, mut b: u32, mut c: u32) -> (u32, u32, u32) {
    a = a.wrapping_sub(c);
    a ^= c.rotate_left(4);
    c = c.wrapping_add(b);
    b = b.wrapping_sub(a);
    b ^= a.rotate_left(6);
    a = a.wrapping_add(c);
    c = c.wrapping_sub(b);
    c ^= b.rotate_left(8);
    b = b.wrapping_add(a);
    a = a.wrapping_sub(c);
    a ^= c.rotate_left(16);
    c = c.wrapping_add(b);
    b = b.wrapping_sub(a);
    b ^= a.rotate_left(19);
    a = a.wrapping_add(c);
    c = c.wrapping_sub(b);
    c ^= b.rotate_left(4);
    b = b.wrapping_add(a);
    (a, b, c)
}

#[inline(always)]
fn final_mix(mut a: u32, mut b: u32, mut c: u32) -> (u32, u32, u32) {
    c ^= b;
    c = c.wrapping_sub(b.rotate_left(14));
    a ^= c;
    a = a.wrapping_sub(c.rotate_left(11));
    b ^= a;
    b = b.wrapping_sub(a.rotate_left(25));
    c ^= b;
    c = c.wrapping_sub(b.rotate_left(16));
    a ^= c;
    a = a.wrapping_sub(c.rotate_left(4));
    b ^= a;
    b = b.wrapping_sub(a.rotate_left(14));
    c ^= b;
    c = c.wrapping_sub(b.rotate_left(24));
    (a, b, c)
}

#[inline(always)]
fn word(k: &[u8], off: usize) -> u32 {
    u32::from_ne_bytes(k[off..off + 4].try_into().unwrap())
}

#[inline(always)]
fn bytes3(k: &[u8], off: usize) -> u32 {
    let mut w = 0u32;
    if k.len() > off + 2 {
        w |= u32::from(k[off + 2]) << 16;
    }
    if k.len() > off + 1 {
        w |= u32::from(k[off + 1]) << 8;
    }
    if k.len() > off {
        w |= u32::from(k[off]);
    }
    w
}

// Unaligned word loads produce the same value as C's aligned word fetches and
// its byte-assembled unaligned path on little-endian, so one path serves both.
#[inline(always)]
fn hash_core(mut a: u32, mut b: u32, mut c: u32, mut k: &[u8]) -> (u32, u32) {
    // Four-byte keys (Oids, dynahash tags) dominate; len==4 falls through with
    // no taken branch.
    if k.len() <= 4 {
        if k.len() == 4 {
            a = a.wrapping_add(word(k, 0));
        } else {
            a = a.wrapping_add(bytes3(k, 0));
        }
        let (_, b, c) = final_mix(a, b, c);
        return (b, c);
    }
    while k.len() >= 12 {
        a = a.wrapping_add(word(k, 0));
        b = b.wrapping_add(word(k, 4));
        c = c.wrapping_add(word(k, 8));
        (a, b, c) = mix(a, b, c);
        k = &k[12..];
    }
    match k.len() {
        0 => {}
        1 => a = a.wrapping_add(u32::from(k[0])),
        2 => a = a.wrapping_add(u32::from(k[0]) | u32::from(k[1]) << 8),
        3 => {
            a = a.wrapping_add(u32::from(k[0]) | u32::from(k[1]) << 8 | u32::from(k[2]) << 16)
        }
        4 => a = a.wrapping_add(word(k, 0)),
        5 => {
            a = a.wrapping_add(word(k, 0));
            b = b.wrapping_add(u32::from(k[4]));
        }
        6 => {
            a = a.wrapping_add(word(k, 0));
            b = b.wrapping_add(u32::from(k[4]) | u32::from(k[5]) << 8);
        }
        7 => {
            a = a.wrapping_add(word(k, 0));
            b = b.wrapping_add(u32::from(k[4]) | u32::from(k[5]) << 8 | u32::from(k[6]) << 16);
        }
        8 => {
            a = a.wrapping_add(word(k, 0));
            b = b.wrapping_add(word(k, 4));
        }
        // Cases 9-11: the lowest byte of c is reserved for the length.
        9 => {
            a = a.wrapping_add(word(k, 0));
            b = b.wrapping_add(word(k, 4));
            c = c.wrapping_add(u32::from(k[8]) << 8);
        }
        10 => {
            a = a.wrapping_add(word(k, 0));
            b = b.wrapping_add(word(k, 4));
            c = c.wrapping_add(u32::from(k[8]) << 8 | u32::from(k[9]) << 16);
        }
        _ => {
            a = a.wrapping_add(word(k, 0));
            b = b.wrapping_add(word(k, 4));
            c = c.wrapping_add(
                u32::from(k[8]) << 8 | u32::from(k[9]) << 16 | u32::from(k[10]) << 24,
            );
        }
    }
    let (_, b, c) = final_mix(a, b, c);
    (b, c)
}

pub fn hash_bytes(k: &[u8]) -> u32 {
    let v = init(k.len());
    hash_core(v, v, v, k).1
}

pub fn hash_bytes_extended(k: &[u8], seed: u64) -> u64 {
    let v = init(k.len());
    let (mut a, mut b, mut c) = (v, v, v);
    if seed != 0 {
        a = a.wrapping_add((seed >> 32) as u32);
        b = b.wrapping_add(seed as u32);
        (a, b, c) = mix(a, b, c);
    }
    let (b, c) = hash_core(a, b, c, k);
    (u64::from(b) << 32) | u64::from(c)
}

pub fn hash_bytes_uint32(k: u32) -> u32 {
    let v = init(size_of::<u32>());
    final_mix(v.wrapping_add(k), v, v).2
}

pub fn hash_bytes_uint32_extended(k: u32, seed: u64) -> u64 {
    let v = init(size_of::<u32>());
    let (mut a, mut b, mut c) = (v, v, v);
    if seed != 0 {
        a = a.wrapping_add((seed >> 32) as u32);
        b = b.wrapping_add(seed as u32);
        (a, b, c) = mix(a, b, c);
    }
    let (_, b, c) = final_mix(a.wrapping_add(k), b, c);
    (u64::from(b) << 32) | u64::from(c)
}

// Hashes at most keysize-1 bytes (dynahash truncates stored keys to that
// length); keysize 0 wraps like C's unsigned `keysize - 1` and imposes no cap.
pub fn string_hash(key: &[u8], keysize: usize) -> u32 {
    let strlen = key.iter().position(|&b| b == 0).unwrap_or(key.len());
    hash_bytes(&key[..strlen.min(keysize.wrapping_sub(1))])
}

pub fn tag_hash(key: &[u8], keysize: usize) -> u32 {
    hash_bytes(&key[..keysize])
}

pub fn uint32_hash(k: u32) -> u32 {
    hash_bytes_uint32(k)
}

pub fn rotate_high_and_low_32bits(v: u64) -> u64 {
    ((v << 1) & 0xffff_fffe_ffff_fffe) | ((v >> 31) & 0x0000_0001_0000_0001)
}

pub fn hash_combine(mut a: u32, b: u32) -> u32 {
    a ^= b
        .wrapping_add(INITVAL)
        .wrapping_add(a << 6)
        .wrapping_add(a >> 2);
    a
}

pub fn hash_combine64(mut a: u64, b: u64) -> u64 {
    a ^= b
        .wrapping_add(0x49a0_f4dd_15e5_a8e3)
        .wrapping_add(a << 54)
        .wrapping_add(a >> 7);
    a
}

pub fn murmurhash32(mut h: u32) -> u32 {
    h ^= h >> 16;
    h = h.wrapping_mul(0x85eb_ca6b);
    h ^= h >> 13;
    h = h.wrapping_mul(0xc2b2_ae35);
    h ^= h >> 16;
    h
}

/// Inverse permutation of [`murmurhash32`]. The murmur3 32-bit finalizer is a
/// bijection on u32 (xorshift-by->=width/2 steps are involutions composed
/// with themselves; the multiplications are by odd constants, invertible mod
/// 2^32), so any value it produced can be mapped back to its input exactly.
/// Consumer: the agg table-handoff export rebases stored entry hashes from a
/// participant's variable-IV mapping onto the leader's IV=0 mapping
/// (`TupleHashTable::hash_to_iv0`) — the IV enters every kernel hash linearly
/// BEFORE this finalizer, so un-finalize / strip IV / re-finalize is exact.
pub fn murmurhash32_inverse(mut h: u32) -> u32 {
    // Undo, in reverse order: ^= >>16 (self-inverse), *= 0xc2b2ae35
    // (multiply by its inverse mod 2^32), ^= >>13 (undone by two shifts),
    // *= 0x85ebca6b, ^= >>16.
    h ^= h >> 16;
    h = h.wrapping_mul(0x7ed1_b41d); // modular inverse of 0xc2b2_ae35
    h ^= (h >> 13) ^ (h >> 26);
    h = h.wrapping_mul(0xa5cb_9243); // modular inverse of 0x85eb_ca6b
    h ^= h >> 16;
    h
}

pub fn murmurhash64(mut h: u64) -> u64 {
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51_afd7_ed55_8ccd);
    h ^= h >> 33;
    h = h.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    h ^= h >> 33;
    h
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uint32_paths_match_byte_path() {
        for v in [0u32, 1, 42, 0x0102_0304, u32::MAX] {
            assert_eq!(hash_bytes_uint32(v), hash_bytes(&v.to_ne_bytes()));
            assert_eq!(
                hash_bytes_uint32_extended(v, 0xdead_beef_cafe_f00d),
                hash_bytes_extended(&v.to_ne_bytes(), 0xdead_beef_cafe_f00d)
            );
            assert_eq!(uint32_hash(v), hash_bytes_uint32(v));
        }
    }

    #[test]
    fn extended_zero_seed_low_word_equals_hash_bytes() {
        let buf: [u8; 40] = core::array::from_fn(|i| (i as u8).wrapping_mul(37));
        for len in 0..=40 {
            let k = &buf[..len];
            assert_eq!(hash_bytes_extended(k, 0) as u32, hash_bytes(k));
        }
    }

    #[test]
    fn murmurhash32_inverse_roundtrips() {
        fn check(v: u32) {
            assert_eq!(murmurhash32_inverse(murmurhash32(v)), v);
            assert_eq!(murmurhash32(murmurhash32_inverse(v)), v);
        }
        for v in [0u32, 1, 2, 42, 0x1234_5678, u32::MAX, u32::MAX - 1] {
            check(v);
        }
        // Deterministic LCG spread across the space.
        let mut x = 0x9e37_79b9u32;
        for _ in 0..100_000 {
            x = x.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            check(x);
        }
    }

    #[test]
    fn dynahash_wrappers() {
        assert_eq!(string_hash(b"abc\0def", 16), hash_bytes(b"abc"));
        assert_eq!(string_hash(b"abcdef", 4), hash_bytes(b"abc"));
        assert_eq!(string_hash(b"abcdef", 0), hash_bytes(b"abcdef"));
        assert_eq!(tag_hash(b"abcdef", 4), hash_bytes(b"abcd"));
    }
}
