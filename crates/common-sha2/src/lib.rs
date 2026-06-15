//! Idiomatic 1:1 port of PostgreSQL's `src/common/sha2.c` — the fallback
//! (non-OpenSSL) reference implementation of SHA-224/256/384/512.
//!
//! Reference: `postgres-18.3/src/common/sha2.c`,
//! `postgres-18.3/src/common/sha2_int.h`,
//! `postgres-18.3/src/include/common/sha2.h`.
//!
//! # Faithfulness notes
//!
//! This is the OpenBSD/Aaron Gifford reference algorithm PostgreSQL ships for
//! builds without a crypto backend.  The arithmetic is bit-for-bit identical to
//! the C; the only difference is that the contexts are owned Rust structs
//! (`pg_sha256_ctx` / `pg_sha512_ctx`) rather than caller-`palloc`'d blobs.
//!
//! The C code reinterprets the byte `buffer` field as an array of `uint32`
//! (`W256`) or `uint64` (`W512`) words during the transform and message
//! expansion, doing the big-endian byte assembly by hand on the first 16 words.
//! Rather than carry an aliased word view, this port keeps the working schedule
//! `W` as an explicit array and loads/stores the byte `buffer` big-endian, which
//! reproduces the exact same word values the C computes on any host endianness.
//!
//! The length field that `*_Last` appends is, in the C, the host-order bit
//! count run through `REVERSE64` (so on a little-endian host the bytes written
//! are the big-endian representation of the count).  Because the final transform
//! then re-reads those bytes big-endian, the net effect is simply "store the bit
//! count big-endian into the tail of the final block".  This port stores it that
//! way directly, which is endianness-independent and matches the C result on the
//! little-endian hosts PostgreSQL supports.
//!
//! No heap allocation occurs anywhere in this crate (every context is a
//! fixed-size value), so there is nothing to make OOM-safe and no memory context
//! is involved. No external dependency is reached.

#![no_std]
#![forbid(unsafe_code)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]

// The crate itself is `#![no_std]`; the unit tests use the std test harness.
#[cfg(test)]
extern crate std;

#[cfg(test)]
mod tests;

/// This crate is a pure leaf: no outward calls, no allocation, and it declares
/// no seams, so there is nothing to install. Present for uniform wiring.
pub fn init_seams() {}

/*** SHA224/256/384/512 Various Length Definitions ***********************/
pub const PG_SHA224_BLOCK_LENGTH: usize = 64;
pub const PG_SHA224_DIGEST_LENGTH: usize = 28;
pub const PG_SHA224_DIGEST_STRING_LENGTH: usize = PG_SHA224_DIGEST_LENGTH * 2 + 1;
pub const PG_SHA256_BLOCK_LENGTH: usize = 64;
pub const PG_SHA256_DIGEST_LENGTH: usize = 32;
pub const PG_SHA256_DIGEST_STRING_LENGTH: usize = PG_SHA256_DIGEST_LENGTH * 2 + 1;
pub const PG_SHA384_BLOCK_LENGTH: usize = 128;
pub const PG_SHA384_DIGEST_LENGTH: usize = 48;
pub const PG_SHA384_DIGEST_STRING_LENGTH: usize = PG_SHA384_DIGEST_LENGTH * 2 + 1;
pub const PG_SHA512_BLOCK_LENGTH: usize = 128;
pub const PG_SHA512_DIGEST_LENGTH: usize = 64;
pub const PG_SHA512_DIGEST_STRING_LENGTH: usize = PG_SHA512_DIGEST_LENGTH * 2 + 1;

/*** SHA-256/384/512 Various Length Definitions ***********************/
const PG_SHA256_SHORT_BLOCK_LENGTH: usize = PG_SHA256_BLOCK_LENGTH - 8;
// Retained from the C header for faithfulness; SHA-384 hashing runs through the
// SHA-512 transform/padding path, so this short-block length is never read.
#[allow(dead_code)]
const PG_SHA384_SHORT_BLOCK_LENGTH: usize = PG_SHA384_BLOCK_LENGTH - 16;
const PG_SHA512_SHORT_BLOCK_LENGTH: usize = PG_SHA512_BLOCK_LENGTH - 16;

/*** Context types ***************************************************/
/// SHA-224/256 hashing context (`pg_sha256_ctx` in C).
///
/// SHA-224 shares this layout (`pg_sha224_ctx` is a typedef of
/// `pg_sha256_ctx`); only the initial state and digest length differ.
#[derive(Clone)]
pub struct pg_sha256_ctx {
    pub state: [u32; 8],
    pub bitcount: u64,
    pub buffer: [u8; PG_SHA256_BLOCK_LENGTH],
}

/// SHA-384/512 hashing context (`pg_sha512_ctx` in C).
///
/// SHA-384 shares this layout (`pg_sha384_ctx` is a typedef of
/// `pg_sha512_ctx`); only the initial state and digest length differ.
#[derive(Clone)]
pub struct pg_sha512_ctx {
    pub state: [u64; 8],
    pub bitcount: [u64; 2],
    pub buffer: [u8; PG_SHA512_BLOCK_LENGTH],
}

/// `pg_sha224_ctx` is a typedef of `pg_sha256_ctx`.
pub type pg_sha224_ctx = pg_sha256_ctx;
/// `pg_sha384_ctx` is a typedef of `pg_sha512_ctx`.
pub type pg_sha384_ctx = pg_sha512_ctx;

impl Default for pg_sha256_ctx {
    fn default() -> Self {
        pg_sha256_ctx {
            state: [0; 8],
            bitcount: 0,
            buffer: [0; PG_SHA256_BLOCK_LENGTH],
        }
    }
}

impl Default for pg_sha512_ctx {
    fn default() -> Self {
        pg_sha512_ctx {
            state: [0; 8],
            bitcount: [0; 2],
            buffer: [0; PG_SHA512_BLOCK_LENGTH],
        }
    }
}

/*
 * Macro for incrementally adding the unsigned 64-bit integer n to the
 * unsigned 128-bit integer (represented using a two-element array of
 * 64-bit words):
 */
#[inline]
fn addinc128(w: &mut [u64; 2], n: u64) {
    w[0] = w[0].wrapping_add(n);
    if w[0] < n {
        w[1] = w[1].wrapping_add(1);
    }
}

/*** THE SIX LOGICAL FUNCTIONS ****************************************/
/* Shift-right (used in SHA-256, SHA-384, and SHA-512): */
#[inline]
fn r32(b: u32, x: u32) -> u32 {
    x >> b
}
#[inline]
fn r64(b: u64, x: u64) -> u64 {
    x >> b
}
/* 32-bit Rotate-right (used in SHA-256): */
#[inline]
fn s32(b: u32, x: u32) -> u32 {
    (x >> b) | (x << (32 - b))
}
/* 64-bit Rotate-right (used in SHA-384 and SHA-512): */
#[inline]
fn s64(b: u64, x: u64) -> u64 {
    (x >> b) | (x << (64 - b))
}

/* Two of six logical functions used in SHA-256, SHA-384, and SHA-512: */
#[inline]
fn ch32(x: u32, y: u32, z: u32) -> u32 {
    (x & y) ^ ((!x) & z)
}
#[inline]
fn maj32(x: u32, y: u32, z: u32) -> u32 {
    (x & y) ^ (x & z) ^ (y & z)
}
#[inline]
fn ch64(x: u64, y: u64, z: u64) -> u64 {
    (x & y) ^ ((!x) & z)
}
#[inline]
fn maj64(x: u64, y: u64, z: u64) -> u64 {
    (x & y) ^ (x & z) ^ (y & z)
}

/* Four of six logical functions used in SHA-256: */
#[inline]
fn big_sigma0_256(x: u32) -> u32 {
    s32(2, x) ^ s32(13, x) ^ s32(22, x)
}
#[inline]
fn big_sigma1_256(x: u32) -> u32 {
    s32(6, x) ^ s32(11, x) ^ s32(25, x)
}
#[inline]
fn small_sigma0_256(x: u32) -> u32 {
    s32(7, x) ^ s32(18, x) ^ r32(3, x)
}
#[inline]
fn small_sigma1_256(x: u32) -> u32 {
    s32(17, x) ^ s32(19, x) ^ r32(10, x)
}

/* Four of six logical functions used in SHA-384 and SHA-512: */
#[inline]
fn big_sigma0_512(x: u64) -> u64 {
    s64(28, x) ^ s64(34, x) ^ s64(39, x)
}
#[inline]
fn big_sigma1_512(x: u64) -> u64 {
    s64(14, x) ^ s64(18, x) ^ s64(41, x)
}
#[inline]
fn small_sigma0_512(x: u64) -> u64 {
    s64(1, x) ^ s64(8, x) ^ r64(7, x)
}
#[inline]
fn small_sigma1_512(x: u64) -> u64 {
    s64(19, x) ^ s64(61, x) ^ r64(6, x)
}

/*** SHA-XYZ INITIAL HASH VALUES AND CONSTANTS ************************/
/* Hash constant words K for SHA-256: */
static K256: [u32; 64] = [
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
];

/* Initial hash value H for SHA-224: */
static sha224_initial_hash_value: [u32; 8] = [
    0xc1059ed8, 0x367cd507, 0x3070dd17, 0xf70e5939, 0xffc00b31, 0x68581511, 0x64f98fa7, 0xbefa4fa4,
];

/* Initial hash value H for SHA-256: */
static sha256_initial_hash_value: [u32; 8] = [
    0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
];

/* Hash constant words K for SHA-384 and SHA-512: */
static K512: [u64; 80] = [
    0x428a2f98d728ae22, 0x7137449123ef65cd, 0xb5c0fbcfec4d3b2f, 0xe9b5dba58189dbbc,
    0x3956c25bf348b538, 0x59f111f1b605d019, 0x923f82a4af194f9b, 0xab1c5ed5da6d8118,
    0xd807aa98a3030242, 0x12835b0145706fbe, 0x243185be4ee4b28c, 0x550c7dc3d5ffb4e2,
    0x72be5d74f27b896f, 0x80deb1fe3b1696b1, 0x9bdc06a725c71235, 0xc19bf174cf692694,
    0xe49b69c19ef14ad2, 0xefbe4786384f25e3, 0x0fc19dc68b8cd5b5, 0x240ca1cc77ac9c65,
    0x2de92c6f592b0275, 0x4a7484aa6ea6e483, 0x5cb0a9dcbd41fbd4, 0x76f988da831153b5,
    0x983e5152ee66dfab, 0xa831c66d2db43210, 0xb00327c898fb213f, 0xbf597fc7beef0ee4,
    0xc6e00bf33da88fc2, 0xd5a79147930aa725, 0x06ca6351e003826f, 0x142929670a0e6e70,
    0x27b70a8546d22ffc, 0x2e1b21385c26c926, 0x4d2c6dfc5ac42aed, 0x53380d139d95b3df,
    0x650a73548baf63de, 0x766a0abb3c77b2a8, 0x81c2c92e47edaee6, 0x92722c851482353b,
    0xa2bfe8a14cf10364, 0xa81a664bbc423001, 0xc24b8b70d0f89791, 0xc76c51a30654be30,
    0xd192e819d6ef5218, 0xd69906245565a910, 0xf40e35855771202a, 0x106aa07032bbd1b8,
    0x19a4c116b8d2d0c8, 0x1e376c085141ab53, 0x2748774cdf8eeb99, 0x34b0bcb5e19b48a8,
    0x391c0cb3c5c95a63, 0x4ed8aa4ae3418acb, 0x5b9cca4f7763e373, 0x682e6ff3d6b2b8a3,
    0x748f82ee5defb2fc, 0x78a5636f43172f60, 0x84c87814a1f0ab72, 0x8cc702081a6439ec,
    0x90befffa23631e28, 0xa4506cebde82bde9, 0xbef9a3f7b2c67915, 0xc67178f2e372532b,
    0xca273eceea26619c, 0xd186b8c721c0c207, 0xeada7dd6cde0eb1e, 0xf57d4f7fee6ed178,
    0x06f067aa72176fba, 0x0a637dc5a2c898a6, 0x113f9804bef90dae, 0x1b710b35131c471b,
    0x28db77f523047d84, 0x32caab7b40c72493, 0x3c9ebe0a15c9bebc, 0x431d67c49c100d4c,
    0x4cc5d4becb3e42b6, 0x597f299cfc657e2a, 0x5fcb6fab3ad6faec, 0x6c44198c4a475817,
];

/* Initial hash value H for SHA-384 */
static sha384_initial_hash_value: [u64; 8] = [
    0xcbbb9d5dc1059ed8,
    0x629a292a367cd507,
    0x9159015a3070dd17,
    0x152fecd8f70e5939,
    0x67332667ffc00b31,
    0x8eb44a8768581511,
    0xdb0c2e0d64f98fa7,
    0x47b5481dbefa4fa4,
];

/* Initial hash value H for SHA-512 */
static sha512_initial_hash_value: [u64; 8] = [
    0x6a09e667f3bcc908,
    0xbb67ae8584caa73b,
    0x3c6ef372fe94f82b,
    0xa54ff53a5f1d36f1,
    0x510e527fade682d1,
    0x9b05688c2b3e6c1f,
    0x1f83d9abfb41bd6b,
    0x5be0cd19137e2179,
];

/*** SHA-256: *********************************************************/
/// `pg_sha256_init`: initialize a SHA-256 context.
pub fn pg_sha256_init(context: &mut pg_sha256_ctx) {
    context.state = sha256_initial_hash_value;
    context.buffer = [0; PG_SHA256_BLOCK_LENGTH];
    context.bitcount = 0;
}

/// SHA-256 block transform (`SHA256_Transform`).
///
/// `data` is one 64-byte block.  The first 16 schedule words are assembled
/// big-endian from `data`; the remaining words come from the message
/// expansion.  We store the assembled words back into the context buffer's
/// word view via the local `W256` array (the C aliases the buffer, but the
/// values are identical).
fn sha256_transform(context: &mut pg_sha256_ctx, data: &[u8]) {
    let mut a = context.state[0];
    let mut b = context.state[1];
    let mut c = context.state[2];
    let mut d = context.state[3];
    let mut e = context.state[4];
    let mut f = context.state[5];
    let mut g = context.state[6];
    let mut h = context.state[7];

    let mut W256 = [0u32; 16];

    let mut j: usize = 0;
    let mut off = 0usize;
    loop {
        W256[j] = (data[off + 3] as u32)
            | ((data[off + 2] as u32) << 8)
            | ((data[off + 1] as u32) << 16)
            | ((data[off] as u32) << 24);
        off += 4;
        /* Apply the SHA-256 compression function to update a..h */
        let t1 = h
            .wrapping_add(big_sigma1_256(e))
            .wrapping_add(ch32(e, f, g))
            .wrapping_add(K256[j])
            .wrapping_add(W256[j]);
        let t2 = big_sigma0_256(a).wrapping_add(maj32(a, b, c));
        h = g;
        g = f;
        f = e;
        e = d.wrapping_add(t1);
        d = c;
        c = b;
        b = a;
        a = t1.wrapping_add(t2);

        j += 1;
        if j >= 16 {
            break;
        }
    }

    loop {
        /* Part of the message block expansion: */
        let s0 = small_sigma0_256(W256[(j + 1) & 0x0f]);
        let s1 = small_sigma1_256(W256[(j + 14) & 0x0f]);

        /* Apply the SHA-256 compression function to update a..h */
        W256[j & 0x0f] = W256[j & 0x0f]
            .wrapping_add(s1)
            .wrapping_add(W256[(j + 9) & 0x0f])
            .wrapping_add(s0);
        let t1 = h
            .wrapping_add(big_sigma1_256(e))
            .wrapping_add(ch32(e, f, g))
            .wrapping_add(K256[j])
            .wrapping_add(W256[j & 0x0f]);
        let t2 = big_sigma0_256(a).wrapping_add(maj32(a, b, c));
        h = g;
        g = f;
        f = e;
        e = d.wrapping_add(t1);
        d = c;
        c = b;
        b = a;
        a = t1.wrapping_add(t2);

        j += 1;
        if j >= 64 {
            break;
        }
    }

    /* Compute the current intermediate hash value */
    context.state[0] = context.state[0].wrapping_add(a);
    context.state[1] = context.state[1].wrapping_add(b);
    context.state[2] = context.state[2].wrapping_add(c);
    context.state[3] = context.state[3].wrapping_add(d);
    context.state[4] = context.state[4].wrapping_add(e);
    context.state[5] = context.state[5].wrapping_add(f);
    context.state[6] = context.state[6].wrapping_add(g);
    context.state[7] = context.state[7].wrapping_add(h);
}

/// `pg_sha256_update`: feed `data` into the SHA-256 context.
pub fn pg_sha256_update(context: &mut pg_sha256_ctx, data: &[u8], len: usize) {
    /* Calling with no data is valid (we do nothing) */
    if len == 0 {
        return;
    }

    let mut data_off = 0usize;
    let mut len = len;

    let usedspace = ((context.bitcount >> 3) as usize) % PG_SHA256_BLOCK_LENGTH;
    if usedspace > 0 {
        /* Calculate how much free space is available in the buffer */
        let freespace = PG_SHA256_BLOCK_LENGTH - usedspace;

        if len >= freespace {
            /* Fill the buffer completely and process it */
            context.buffer[usedspace..usedspace + freespace]
                .copy_from_slice(&data[data_off..data_off + freespace]);
            context.bitcount = context.bitcount.wrapping_add((freespace as u64) << 3);
            len -= freespace;
            data_off += freespace;
            let block = context.buffer;
            sha256_transform(context, &block);
        } else {
            /* The buffer is not yet full */
            context.buffer[usedspace..usedspace + len]
                .copy_from_slice(&data[data_off..data_off + len]);
            context.bitcount = context.bitcount.wrapping_add((len as u64) << 3);
            return;
        }
    }
    while len >= PG_SHA256_BLOCK_LENGTH {
        /* Process as many complete blocks as we can */
        let block: [u8; PG_SHA256_BLOCK_LENGTH] = data[data_off..data_off + PG_SHA256_BLOCK_LENGTH]
            .try_into()
            .unwrap();
        sha256_transform(context, &block);
        context.bitcount = context
            .bitcount
            .wrapping_add((PG_SHA256_BLOCK_LENGTH as u64) << 3);
        len -= PG_SHA256_BLOCK_LENGTH;
        data_off += PG_SHA256_BLOCK_LENGTH;
    }
    if len > 0 {
        /* There's left-overs, so save 'em */
        context.buffer[0..len].copy_from_slice(&data[data_off..data_off + len]);
        context.bitcount = context.bitcount.wrapping_add((len as u64) << 3);
    }
}

/// `SHA256_Last`: pad and run the final transform(s) for SHA-224/256.
fn sha256_last(context: &mut pg_sha256_ctx) {
    let mut usedspace = ((context.bitcount >> 3) as usize) % PG_SHA256_BLOCK_LENGTH;

    /*
     * In the C, the host-order bit count is REVERSE64'd here and later written
     * into the buffer with a host-order `uint64` store; on a little-endian host
     * the net effect is "store the count big-endian into the tail".  We capture
     * the big-endian bytes now and write them below.
     */
    let bitcount_be = context.bitcount.to_be_bytes();

    if usedspace > 0 {
        /* Begin padding with a 1 bit: */
        context.buffer[usedspace] = 0x80;
        usedspace += 1;

        if usedspace <= PG_SHA256_SHORT_BLOCK_LENGTH {
            /* Set-up for the last transform: */
            for byte in &mut context.buffer[usedspace..PG_SHA256_SHORT_BLOCK_LENGTH] {
                *byte = 0;
            }
        } else {
            if usedspace < PG_SHA256_BLOCK_LENGTH {
                for byte in &mut context.buffer[usedspace..PG_SHA256_BLOCK_LENGTH] {
                    *byte = 0;
                }
            }
            /* Do second-to-last transform: */
            let block = context.buffer;
            sha256_transform(context, &block);

            /* And set-up for the last transform: */
            for byte in &mut context.buffer[0..PG_SHA256_SHORT_BLOCK_LENGTH] {
                *byte = 0;
            }
        }
    } else {
        /* Set-up for the last transform: */
        for byte in &mut context.buffer[0..PG_SHA256_SHORT_BLOCK_LENGTH] {
            *byte = 0;
        }

        /* Begin padding with a 1 bit: */
        context.buffer[0] = 0x80;
    }
    /* Set the bit count: */
    context.buffer[PG_SHA256_SHORT_BLOCK_LENGTH..PG_SHA256_SHORT_BLOCK_LENGTH + 8]
        .copy_from_slice(&bitcount_be);

    /* Final transform: */
    let block = context.buffer;
    sha256_transform(context, &block);
}

/// `pg_sha256_final`: produce the 32-byte SHA-256 digest.
pub fn pg_sha256_final(context: &mut pg_sha256_ctx, digest: &mut [u8]) {
    /* If no digest buffer is passed, we don't bother doing this: */
    if !digest.is_empty() {
        sha256_last(context);

        /* Convert TO host byte order then copy out big-endian bytes */
        for j in 0..8 {
            digest[j * 4..j * 4 + 4].copy_from_slice(&context.state[j].to_be_bytes());
        }
    }

    /* Clean up state data: */
    *context = pg_sha256_ctx::default();
}

/*** SHA-512: *********************************************************/
/// `pg_sha512_init`: initialize a SHA-512 context.
pub fn pg_sha512_init(context: &mut pg_sha512_ctx) {
    context.state = sha512_initial_hash_value;
    context.buffer = [0; PG_SHA512_BLOCK_LENGTH];
    context.bitcount[0] = 0;
    context.bitcount[1] = 0;
}

/// SHA-512 block transform (`SHA512_Transform`).
fn sha512_transform(context: &mut pg_sha512_ctx, data: &[u8]) {
    let mut a = context.state[0];
    let mut b = context.state[1];
    let mut c = context.state[2];
    let mut d = context.state[3];
    let mut e = context.state[4];
    let mut f = context.state[5];
    let mut g = context.state[6];
    let mut h = context.state[7];

    let mut W512 = [0u64; 16];

    let mut j: usize = 0;
    let mut off = 0usize;
    loop {
        W512[j] = (data[off + 7] as u64)
            | ((data[off + 6] as u64) << 8)
            | ((data[off + 5] as u64) << 16)
            | ((data[off + 4] as u64) << 24)
            | ((data[off + 3] as u64) << 32)
            | ((data[off + 2] as u64) << 40)
            | ((data[off + 1] as u64) << 48)
            | ((data[off] as u64) << 56);
        off += 8;
        /* Apply the SHA-512 compression function to update a..h */
        let t1 = h
            .wrapping_add(big_sigma1_512(e))
            .wrapping_add(ch64(e, f, g))
            .wrapping_add(K512[j])
            .wrapping_add(W512[j]);
        let t2 = big_sigma0_512(a).wrapping_add(maj64(a, b, c));
        h = g;
        g = f;
        f = e;
        e = d.wrapping_add(t1);
        d = c;
        c = b;
        b = a;
        a = t1.wrapping_add(t2);

        j += 1;
        if j >= 16 {
            break;
        }
    }

    loop {
        /* Part of the message block expansion: */
        let s0 = small_sigma0_512(W512[(j + 1) & 0x0f]);
        let s1 = small_sigma1_512(W512[(j + 14) & 0x0f]);

        /* Apply the SHA-512 compression function to update a..h */
        W512[j & 0x0f] = W512[j & 0x0f]
            .wrapping_add(s1)
            .wrapping_add(W512[(j + 9) & 0x0f])
            .wrapping_add(s0);
        let t1 = h
            .wrapping_add(big_sigma1_512(e))
            .wrapping_add(ch64(e, f, g))
            .wrapping_add(K512[j])
            .wrapping_add(W512[j & 0x0f]);
        let t2 = big_sigma0_512(a).wrapping_add(maj64(a, b, c));
        h = g;
        g = f;
        f = e;
        e = d.wrapping_add(t1);
        d = c;
        c = b;
        b = a;
        a = t1.wrapping_add(t2);

        j += 1;
        if j >= 80 {
            break;
        }
    }

    /* Compute the current intermediate hash value */
    context.state[0] = context.state[0].wrapping_add(a);
    context.state[1] = context.state[1].wrapping_add(b);
    context.state[2] = context.state[2].wrapping_add(c);
    context.state[3] = context.state[3].wrapping_add(d);
    context.state[4] = context.state[4].wrapping_add(e);
    context.state[5] = context.state[5].wrapping_add(f);
    context.state[6] = context.state[6].wrapping_add(g);
    context.state[7] = context.state[7].wrapping_add(h);
}

/// `pg_sha512_update`: feed `data` into the SHA-512 context.
pub fn pg_sha512_update(context: &mut pg_sha512_ctx, data: &[u8], len: usize) {
    /* Calling with no data is valid (we do nothing) */
    if len == 0 {
        return;
    }

    let mut data_off = 0usize;
    let mut len = len;

    let usedspace = ((context.bitcount[0] >> 3) as usize) % PG_SHA512_BLOCK_LENGTH;
    if usedspace > 0 {
        /* Calculate how much free space is available in the buffer */
        let freespace = PG_SHA512_BLOCK_LENGTH - usedspace;

        if len >= freespace {
            /* Fill the buffer completely and process it */
            context.buffer[usedspace..usedspace + freespace]
                .copy_from_slice(&data[data_off..data_off + freespace]);
            addinc128(&mut context.bitcount, (freespace as u64) << 3);
            len -= freespace;
            data_off += freespace;
            let block = context.buffer;
            sha512_transform(context, &block);
        } else {
            /* The buffer is not yet full */
            context.buffer[usedspace..usedspace + len]
                .copy_from_slice(&data[data_off..data_off + len]);
            addinc128(&mut context.bitcount, (len as u64) << 3);
            return;
        }
    }
    while len >= PG_SHA512_BLOCK_LENGTH {
        /* Process as many complete blocks as we can */
        let block: [u8; PG_SHA512_BLOCK_LENGTH] = data[data_off..data_off + PG_SHA512_BLOCK_LENGTH]
            .try_into()
            .unwrap();
        sha512_transform(context, &block);
        addinc128(&mut context.bitcount, (PG_SHA512_BLOCK_LENGTH as u64) << 3);
        len -= PG_SHA512_BLOCK_LENGTH;
        data_off += PG_SHA512_BLOCK_LENGTH;
    }
    if len > 0 {
        /* There's left-overs, so save 'em */
        context.buffer[0..len].copy_from_slice(&data[data_off..data_off + len]);
        addinc128(&mut context.bitcount, (len as u64) << 3);
    }
}

/// `SHA512_Last`: pad and run the final transform(s) for SHA-384/512.
fn sha512_last(context: &mut pg_sha512_ctx) {
    let mut usedspace = ((context.bitcount[0] >> 3) as usize) % PG_SHA512_BLOCK_LENGTH;

    /*
     * As in `sha256_last`, capture the (big-endian) byte image of the two
     * 64-bit halves of the 128-bit bit count; the C REVERSE64's them and stores
     * with host-order `uint64` writes, which on a little-endian host yields the
     * big-endian byte layout we write below.
     */
    let bitcount1_be = context.bitcount[1].to_be_bytes();
    let bitcount0_be = context.bitcount[0].to_be_bytes();

    if usedspace > 0 {
        /* Begin padding with a 1 bit: */
        context.buffer[usedspace] = 0x80;
        usedspace += 1;

        if usedspace <= PG_SHA512_SHORT_BLOCK_LENGTH {
            /* Set-up for the last transform: */
            for byte in &mut context.buffer[usedspace..PG_SHA512_SHORT_BLOCK_LENGTH] {
                *byte = 0;
            }
        } else {
            if usedspace < PG_SHA512_BLOCK_LENGTH {
                for byte in &mut context.buffer[usedspace..PG_SHA512_BLOCK_LENGTH] {
                    *byte = 0;
                }
            }
            /* Do second-to-last transform: */
            let block = context.buffer;
            sha512_transform(context, &block);

            /*
             * And set-up for the last transform.  NOTE: the C clears only
             * `PG_SHA512_BLOCK_LENGTH - 2` (= 126) bytes here, intentionally
             * matching the upstream reference; the trailing 2 bytes are
             * overwritten anyway by the bit-count store below.
             */
            for byte in &mut context.buffer[0..PG_SHA512_BLOCK_LENGTH - 2] {
                *byte = 0;
            }
        }
    } else {
        /* Prepare for final transform: */
        for byte in &mut context.buffer[0..PG_SHA512_SHORT_BLOCK_LENGTH] {
            *byte = 0;
        }

        /* Begin padding with a 1 bit: */
        context.buffer[0] = 0x80;
    }
    /* Store the length of input data (in bits): */
    context.buffer[PG_SHA512_SHORT_BLOCK_LENGTH..PG_SHA512_SHORT_BLOCK_LENGTH + 8]
        .copy_from_slice(&bitcount1_be);
    context.buffer[PG_SHA512_SHORT_BLOCK_LENGTH + 8..PG_SHA512_SHORT_BLOCK_LENGTH + 16]
        .copy_from_slice(&bitcount0_be);

    /* Final transform: */
    let block = context.buffer;
    sha512_transform(context, &block);
}

/// `pg_sha512_final`: produce the 64-byte SHA-512 digest.
pub fn pg_sha512_final(context: &mut pg_sha512_ctx, digest: &mut [u8]) {
    /* If no digest buffer is passed, we don't bother doing this: */
    if !digest.is_empty() {
        sha512_last(context);

        /* Convert TO host byte order then copy out big-endian bytes */
        for j in 0..8 {
            digest[j * 8..j * 8 + 8].copy_from_slice(&context.state[j].to_be_bytes());
        }
    }

    /* Zero out state data */
    *context = pg_sha512_ctx::default();
}

/*** SHA-384: *********************************************************/
/// `pg_sha384_init`: initialize a SHA-384 context.
pub fn pg_sha384_init(context: &mut pg_sha384_ctx) {
    context.state = sha384_initial_hash_value;
    context.buffer = [0; PG_SHA384_BLOCK_LENGTH];
    context.bitcount[0] = 0;
    context.bitcount[1] = 0;
}

/// `pg_sha384_update`: feed `data` into the SHA-384 context.
pub fn pg_sha384_update(context: &mut pg_sha384_ctx, data: &[u8], len: usize) {
    pg_sha512_update(context, data, len);
}

/// `pg_sha384_final`: produce the 48-byte SHA-384 digest.
pub fn pg_sha384_final(context: &mut pg_sha384_ctx, digest: &mut [u8]) {
    /* If no digest buffer is passed, we don't bother doing this: */
    if !digest.is_empty() {
        sha512_last(context);

        /* Convert TO host byte order then copy out big-endian bytes (6 words) */
        for j in 0..6 {
            digest[j * 8..j * 8 + 8].copy_from_slice(&context.state[j].to_be_bytes());
        }
    }

    /* Zero out state data */
    *context = pg_sha512_ctx::default();
}

/*** SHA-224: *********************************************************/
/// `pg_sha224_init`: initialize a SHA-224 context.
pub fn pg_sha224_init(context: &mut pg_sha224_ctx) {
    context.state = sha224_initial_hash_value;
    context.buffer = [0; PG_SHA256_BLOCK_LENGTH];
    context.bitcount = 0;
}

/// `pg_sha224_update`: feed `data` into the SHA-224 context.
pub fn pg_sha224_update(context: &mut pg_sha224_ctx, data: &[u8], len: usize) {
    pg_sha256_update(context, data, len);
}

/// `pg_sha224_final`: produce the 28-byte SHA-224 digest.
pub fn pg_sha224_final(context: &mut pg_sha224_ctx, digest: &mut [u8]) {
    /* If no digest buffer is passed, we don't bother doing this: */
    if !digest.is_empty() {
        sha256_last(context);

        /*
         * Convert TO host byte order then copy out big-endian bytes.  The C
         * reverses all 8 state words but copies out only the first 28 bytes
         * (7 full words); the 8th word's reversal is dead, so emitting 7 words
         * is byte-identical.
         */
        for j in 0..7 {
            digest[j * 4..j * 4 + 4].copy_from_slice(&context.state[j].to_be_bytes());
        }
    }

    /* Clean up state data: */
    *context = pg_sha256_ctx::default();
}
