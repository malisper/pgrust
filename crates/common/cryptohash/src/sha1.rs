//! Idiomatic 1:1 port of PostgreSQL's `src/common/sha1.c` — the fallback
//! (non-OpenSSL) reference implementation of SHA-1 (FIPS 180-1 / RFC 3174).
//!
//! Reference: `postgres-18.3/src/common/sha1.c`,
//! `postgres-18.3/src/common/sha1_int.h`.
//!
//! # Faithfulness notes
//!
//! This is the KAME/itojun reference algorithm PostgreSQL ships for builds
//! without a crypto backend.  The arithmetic is bit-for-bit identical to the C;
//! the only difference is that the context is an owned Rust struct
//! (`pg_sha1_ctx`) rather than a caller-`palloc`'d blob.
//!
//! The C reinterprets the byte message buffer `m.b8[64]` as an array of `uint32`
//! words (`m.b32[16]`) during the transform.  On a little-endian host
//! `sha1_step` first byte-swaps every 4-byte group of `m.b8` so the subsequent
//! `W(n)` reads pick up the big-endian word value; the bit count appended in
//! `sha1_pad` is likewise written most-significant-byte first via the explicit
//! `c.b8[7..0]` `PUTPAD` sequence (little-endian host branch).  Rather than carry
//! an aliased word view plus a host-dependent byte-swap, this port keeps the
//! message buffer as bytes and assembles each schedule word big-endian on the
//! fly, and appends the bit count big-endian — which reproduces the exact same
//! word values and digest the C computes on any host endianness, matching the
//! C result on the little-endian hosts PostgreSQL supports.
//!
//! No heap allocation occurs anywhere in this module (the context is a
//! fixed-size value), so there is nothing to make OOM-safe and no memory context
//! is involved.

// The `(count % 64) == 0` block-boundary tests below are direct transcriptions
// of the C `COUNT % 64 == 0` / `if (ctx->count % 64 == 0)` conditions; keeping
// that exact form (rather than `.is_multiple_of(64)`) preserves the 1:1
// correspondence with `sha1.c` alongside the quoted-C comments.
#![allow(clippy::manual_is_multiple_of)]

/// `SHA1_DIGEST_LENGTH` (common/sha1.h) — SHA-1 produces a 20-byte digest.
pub const SHA1_DIGEST_LENGTH: usize = 20;

/// SHA-1 message block size, in bytes.
const SHA1_BLOCK_LENGTH: usize = 64;

/* constant table (`_K` in sha1.c) */
static K: [u32; 4] = [0x5a827999, 0x6ed9eba1, 0x8f1bbcdc, 0xca62c1d6];

/* `#define K(t)  _K[(t) / 20]` */
#[inline]
fn k(t: usize) -> u32 {
    K[t / 20]
}

/* `#define F0(b, c, d) (((b) & (c)) | ((~(b)) & (d)))` */
#[inline]
fn f0(b: u32, c: u32, d: u32) -> u32 {
    (b & c) | ((!b) & d)
}
/* `#define F1(b, c, d) (((b) ^ (c)) ^ (d))` */
#[inline]
fn f1(b: u32, c: u32, d: u32) -> u32 {
    (b ^ c) ^ d
}
/* `#define F2(b, c, d) (((b) & (c)) | ((b) & (d)) | ((c) & (d)))` */
#[inline]
fn f2(b: u32, c: u32, d: u32) -> u32 {
    (b & c) | (b & d) | (c & d)
}
/* `#define F3(b, c, d) (((b) ^ (c)) ^ (d))` */
#[inline]
fn f3(b: u32, c: u32, d: u32) -> u32 {
    (b ^ c) ^ d
}

/* `#define S(n, x)  (((x) << (n)) | ((x) >> (32 - (n))))` — 32-bit rotate-left. */
#[inline]
fn s(n: u32, x: u32) -> u32 {
    x.rotate_left(n)
}

/// SHA-1 hashing context (`pg_sha1_ctx` in C).
///
/// In C this is three unions over the same bytes, plus a `uint8 count` tracking
/// the byte position within the 64-byte block:
///
/// * `h.b32[5]` / `h.b8[20]`  — the running state words / digest bytes,
/// * `c.b64[0]` / `c.b8[8]`   — the message bit count,
/// * `m.b32[16]` / `m.b8[64]` — the current message block.
///
/// This port stores the state as words (`h`), the bit count as a `u64` (`c`),
/// the message block as bytes (`m`), and `count` as the byte position.  The
/// schedule words `W(n)` are assembled big-endian from `m` inside `step`, which
/// is endianness-independent and matches the C's little-endian byte-swap path.
#[derive(Clone)]
pub struct pg_sha1_ctx {
    /// Running state words (`h.b32`).
    pub h: [u32; 5],
    /// Message bit count (`c.b64[0]`).
    pub c: u64,
    /// Current message block bytes (`m.b8`).
    pub m: [u8; SHA1_BLOCK_LENGTH],
    /// Byte position within the current 64-byte block (`count`).
    pub count: u8,
}

impl Default for pg_sha1_ctx {
    fn default() -> Self {
        pg_sha1_ctx {
            h: [0; 5],
            c: 0,
            m: [0; SHA1_BLOCK_LENGTH],
            count: 0,
        }
    }
}

/// `W(n)` — read schedule word `n` big-endian out of the message block.
///
/// The C aliases `m.b8` as `m.b32` after a host-order byte-swap; assembling the
/// word big-endian here yields the identical value on any host.
#[inline]
fn w_get(m: &[u8; SHA1_BLOCK_LENGTH], n: usize) -> u32 {
    let off = n * 4;
    ((m[off] as u32) << 24)
        | ((m[off + 1] as u32) << 16)
        | ((m[off + 2] as u32) << 8)
        | (m[off + 3] as u32)
}

/// `W(n) = value` — store schedule word `n` big-endian back into the buffer.
#[inline]
fn w_set(m: &mut [u8; SHA1_BLOCK_LENGTH], n: usize, value: u32) {
    let off = n * 4;
    m[off] = (value >> 24) as u8;
    m[off + 1] = (value >> 16) as u8;
    m[off + 2] = (value >> 8) as u8;
    m[off + 3] = value as u8;
}

/// `sha1_step` — the SHA-1 block compression over the 64 bytes in `ctx.m`.
fn sha1_step(ctx: &mut pg_sha1_ctx) {
    // The C `#ifndef WORDS_BIGENDIAN` branch byte-swaps each 4-byte group of
    // `m.b8` so the `W(n)` (== `m.b32[n]`) reads observe the big-endian word.
    // Assembling each `W` big-endian in `w_get`/`w_set` reproduces that value
    // directly, so no buffer byte-swap is needed.

    let mut a = ctx.h[0];
    let mut b = ctx.h[1];
    let mut c = ctx.h[2];
    let mut d = ctx.h[3];
    let mut e = ctx.h[4];

    for t in 0..20usize {
        let sidx = t & 0x0f;
        if t >= 16 {
            let w = s(
                1,
                w_get(&ctx.m, (sidx + 13) & 0x0f)
                    ^ w_get(&ctx.m, (sidx + 8) & 0x0f)
                    ^ w_get(&ctx.m, (sidx + 2) & 0x0f)
                    ^ w_get(&ctx.m, sidx),
            );
            w_set(&mut ctx.m, sidx, w);
        }
        let tmp = s(5, a)
            .wrapping_add(f0(b, c, d))
            .wrapping_add(e)
            .wrapping_add(w_get(&ctx.m, sidx))
            .wrapping_add(k(t));
        e = d;
        d = c;
        c = s(30, b);
        b = a;
        a = tmp;
    }
    for t in 20..40usize {
        let sidx = t & 0x0f;
        let w = s(
            1,
            w_get(&ctx.m, (sidx + 13) & 0x0f)
                ^ w_get(&ctx.m, (sidx + 8) & 0x0f)
                ^ w_get(&ctx.m, (sidx + 2) & 0x0f)
                ^ w_get(&ctx.m, sidx),
        );
        w_set(&mut ctx.m, sidx, w);
        let tmp = s(5, a)
            .wrapping_add(f1(b, c, d))
            .wrapping_add(e)
            .wrapping_add(w_get(&ctx.m, sidx))
            .wrapping_add(k(t));
        e = d;
        d = c;
        c = s(30, b);
        b = a;
        a = tmp;
    }
    for t in 40..60usize {
        let sidx = t & 0x0f;
        let w = s(
            1,
            w_get(&ctx.m, (sidx + 13) & 0x0f)
                ^ w_get(&ctx.m, (sidx + 8) & 0x0f)
                ^ w_get(&ctx.m, (sidx + 2) & 0x0f)
                ^ w_get(&ctx.m, sidx),
        );
        w_set(&mut ctx.m, sidx, w);
        let tmp = s(5, a)
            .wrapping_add(f2(b, c, d))
            .wrapping_add(e)
            .wrapping_add(w_get(&ctx.m, sidx))
            .wrapping_add(k(t));
        e = d;
        d = c;
        c = s(30, b);
        b = a;
        a = tmp;
    }
    for t in 60..80usize {
        let sidx = t & 0x0f;
        let w = s(
            1,
            w_get(&ctx.m, (sidx + 13) & 0x0f)
                ^ w_get(&ctx.m, (sidx + 8) & 0x0f)
                ^ w_get(&ctx.m, (sidx + 2) & 0x0f)
                ^ w_get(&ctx.m, sidx),
        );
        w_set(&mut ctx.m, sidx, w);
        let tmp = s(5, a)
            .wrapping_add(f3(b, c, d))
            .wrapping_add(e)
            .wrapping_add(w_get(&ctx.m, sidx))
            .wrapping_add(k(t));
        e = d;
        d = c;
        c = s(30, b);
        b = a;
        a = tmp;
    }

    ctx.h[0] = ctx.h[0].wrapping_add(a);
    ctx.h[1] = ctx.h[1].wrapping_add(b);
    ctx.h[2] = ctx.h[2].wrapping_add(c);
    ctx.h[3] = ctx.h[3].wrapping_add(d);
    ctx.h[4] = ctx.h[4].wrapping_add(e);

    // `memset(&ctx->m.b8[0], 0, 64);`
    ctx.m = [0; SHA1_BLOCK_LENGTH];
}

/// `PUTPAD(x)` — append one padding byte at the current block position,
/// advancing `count` (mod 64) and running a block step when the buffer fills.
#[inline]
fn putpad(ctx: &mut pg_sha1_ctx, x: u8) {
    ctx.m[(ctx.count as usize) % 64] = x;
    ctx.count = ctx.count.wrapping_add(1);
    ctx.count %= 64;
    if (ctx.count % 64) == 0 {
        sha1_step(ctx);
    }
}

/// `sha1_pad` — append the 0x80 byte, zero padding, and 64-bit big-endian bit
/// count, running the final block step(s).
fn sha1_pad(ctx: &mut pg_sha1_ctx) {
    putpad(ctx, 0x80);

    let mut padstart = (ctx.count as usize) % 64;
    let mut padlen = 64 - padstart;
    if padlen < 8 {
        for byte in &mut ctx.m[padstart..padstart + padlen] {
            *byte = 0;
        }
        ctx.count = ctx.count.wrapping_add(padlen as u8);
        ctx.count %= 64;
        sha1_step(ctx);
        padstart = (ctx.count as usize) % 64; /* should be 0 */
        padlen = 64 - padstart; /* should be 64 */
    }
    for byte in &mut ctx.m[padstart..padstart + (padlen - 8)] {
        *byte = 0;
    }
    ctx.count = ctx.count.wrapping_add((padlen - 8) as u8);
    ctx.count %= 64;

    // The C little-endian branch emits the count bytes most-significant first:
    //   PUTPAD(c.b8[7]); PUTPAD(c.b8[6]); ... PUTPAD(c.b8[0]);
    // i.e. the bit count appended big-endian. (`c.b64[0]` is the bit count.)
    let cbytes = ctx.c.to_be_bytes();
    for &byte in cbytes.iter() {
        putpad(ctx, byte);
    }
}

/// `sha1_result` — emit the 20-byte digest from the state words.
///
/// The C little-endian branch writes each state word `h.b32[n]` most-significant
/// byte first (`digest[..] = h.b8[3], h.b8[2], ...`), i.e. the words big-endian.
fn sha1_result(digest: &mut [u8], ctx: &pg_sha1_ctx) {
    for (n, word) in ctx.h.iter().enumerate() {
        digest[n * 4..n * 4 + 4].copy_from_slice(&word.to_be_bytes());
    }
}

/// `pg_sha1_init` — initialize a SHA-1 context.
pub fn pg_sha1_init(ctx: &mut pg_sha1_ctx) {
    // memset(ctx, 0, sizeof(pg_sha1_ctx));
    *ctx = pg_sha1_ctx::default();
    ctx.h[0] = 0x67452301;
    ctx.h[1] = 0xefcdab89;
    ctx.h[2] = 0x98badcfe;
    ctx.h[3] = 0x10325476;
    ctx.h[4] = 0xc3d2e1f0;
}

/// `pg_sha1_update` — feed `data` (of length `len`) into the SHA-1 context.
pub fn pg_sha1_update(ctx: &mut pg_sha1_ctx, data: &[u8], len: usize) {
    let mut off = 0usize;

    while off < len {
        let gapstart = (ctx.count as usize) % 64;
        let gaplen = 64 - gapstart;

        let copysiz = if gaplen < len - off { gaplen } else { len - off };
        ctx.m[gapstart..gapstart + copysiz].copy_from_slice(&data[off..off + copysiz]);
        ctx.count = ctx.count.wrapping_add(copysiz as u8);
        ctx.count %= 64;
        ctx.c = ctx.c.wrapping_add((copysiz as u64) * 8);
        if (ctx.count % 64) == 0 {
            sha1_step(ctx);
        }
        off += copysiz;
    }
}

/// `pg_sha1_final` — pad and produce the 20-byte SHA-1 digest into `dest`.
pub fn pg_sha1_final(ctx: &mut pg_sha1_ctx, dest: &mut [u8]) {
    sha1_pad(ctx);
    sha1_result(dest, ctx);
}
