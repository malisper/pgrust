//! Idiomatic 1:1 port of PostgreSQL's MD5 support: the in-tree (non-OpenSSL)
//! reference implementation in `src/common/md5.c` plus the shared front-end
//! `src/common/md5_common.c` (`bytesToHex`, `pg_md5_hash`, `pg_md5_binary`,
//! `pg_md5_encrypt`).
//!
//! References:
//! * `postgres-18.3/src/common/md5.c` — the RFC 1321 reference algorithm
//!   PostgreSQL ships for builds without a crypto backend (the
//!   `md5_ctxt` / `md5_init` / `md5_loop` / `md5_pad` / `md5_result` family and
//!   the `md5_calc` block transform).
//! * `postgres-18.3/src/common/md5_common.c` — `bytesToHex`, `pg_md5_hash`,
//!   `pg_md5_binary`, `pg_md5_encrypt` (the SQL/auth-facing wrappers).
//! * `postgres-18.3/src/include/common/md5.h`.
//!
//! # Faithfulness notes
//!
//! The arithmetic of the block transform (`md5_calc`) is bit-for-bit identical
//! to the C reference.  The only structural difference is that the C code
//! reinterprets the byte buffer as an array of host-order `uint32` words during
//! the transform; this port assembles the words *little-endian* by hand, which
//! reproduces the exact same word values the C computes on the little-endian
//! hosts PostgreSQL supports (MD5 is defined little-endian).
//!
//! The `md5_ctxt` context is an owned Rust struct rather than a caller-supplied
//! blob, and no heap allocation occurs in the core algorithm (so there is
//! nothing to make OOM-safe).  `pg_md5_hash` / `pg_md5_binary` / `pg_md5_encrypt`
//! return their digest in caller-supplied byte arrays / owned `String`s, never
//! through `ereport`; the C signatures report failure via a `bool` return + an
//! `*errstr` out-param (only ever set on the OpenSSL path), which here is the
//! `Ok(Err(errstr))` arm of the seam contracts (always `Ok(Ok(..))` for this
//! software implementation).
//!
//! This crate is the owner of `common/md5.c`; from [`init_seams`] it installs
//! the `pg_md5_encrypt` seam (declared in `backend-libpq-crypt-seams`) and the
//! `pg_md5_binary` seam (declared in `backend-libpq-auth-seams`), both of which
//! were left for "the md5 owner" to fill.

#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
#![allow(non_snake_case)]

#[cfg(test)]
mod tests;

/// `MD5_DIGEST_LENGTH` — raw MD5 digest size in bytes (`common/md5.h`).
pub const MD5_DIGEST_LENGTH: usize = 16;
/// `MD5_HASH_LEN` — length of a hex-encoded MD5 digest, excluding the trailing
/// NUL (`common/md5.h`).
pub const MD5_HASH_LEN: usize = 32;

// ---------------------------------------------------------------------------
// The RFC 1321 reference implementation (`src/common/md5.c`).
// ---------------------------------------------------------------------------

/// `md5_ctxt` (`src/common/md5.c`): the running MD5 hashing context.
///
/// The C struct stores `md5_st` as a `union` of four `uint32` words and a
/// 16-byte view; here it is the four words directly.  `md5_buf` is the 64-byte
/// block accumulator and `md5_n` is the total message length in bits.
#[derive(Clone)]
pub struct md5_ctxt {
    /// `md5_sta`/`md5_stb`/`md5_stc`/`md5_std` — the four state words A,B,C,D.
    md5_st: [u32; 4],
    /// `md5_n` — total processed length, in bits (the C uses a 64-bit count).
    md5_n: u64,
    /// `md5_buf` — the 64-byte block buffer.
    md5_buf: [u8; 64],
    /// `md5_i` — current fill of `md5_buf`.
    md5_i: usize,
}

/* MD5 round constants and shift amounts (RFC 1321). */
const MD5_A0: u32 = 0x67452301;
const MD5_B0: u32 = 0xefcdab89;
const MD5_C0: u32 = 0x98badcfe;
const MD5_D0: u32 = 0x10325476;

/* sine-derived per-step constants T[i] = floor(2^32 * abs(sin(i+1))). */
static T: [u32; 64] = [
    0xd76aa478, 0xe8c7b756, 0x242070db, 0xc1bdceee, 0xf57c0faf, 0x4787c62a, 0xa8304613, 0xfd469501,
    0x698098d8, 0x8b44f7af, 0xffff5bb1, 0x895cd7be, 0x6b901122, 0xfd987193, 0xa679438e, 0x49b40821,
    0xf61e2562, 0xc040b340, 0x265e5a51, 0xe9b6c7aa, 0xd62f105d, 0x02441453, 0xd8a1e681, 0xe7d3fbc8,
    0x21e1cde6, 0xc33707d6, 0xf4d50d87, 0x455a14ed, 0xa9e3e905, 0xfcefa3f8, 0x676f02d9, 0x8d2a4c8a,
    0xfffa3942, 0x8771f681, 0x6d9d6122, 0xfde5380c, 0xa4beea44, 0x4bdecfa9, 0xf6bb4b60, 0xbebfbc70,
    0x289b7ec6, 0xeaa127fa, 0xd4ef3085, 0x04881d05, 0xd9d4d039, 0xe6db99e5, 0x1fa27cf8, 0xc4ac5665,
    0xf4292244, 0x432aff97, 0xab9423a7, 0xfc93a039, 0x655b59c3, 0x8f0ccc92, 0xffeff47d, 0x85845dd1,
    0x6fa87e4f, 0xfe2ce6e0, 0xa3014314, 0x4e0811a1, 0xf7537e82, 0xbd3af235, 0x2ad7d2bb, 0xeb86d391,
];

/* per-round left-rotate amounts. */
const S11: u32 = 7;
const S12: u32 = 12;
const S13: u32 = 17;
const S14: u32 = 22;
const S21: u32 = 5;
const S22: u32 = 9;
const S23: u32 = 14;
const S24: u32 = 20;
const S31: u32 = 4;
const S32: u32 = 11;
const S33: u32 = 16;
const S34: u32 = 23;
const S41: u32 = 6;
const S42: u32 = 10;
const S43: u32 = 15;
const S44: u32 = 21;

#[inline]
fn f(x: u32, y: u32, z: u32) -> u32 {
    (x & y) | ((!x) & z)
}
#[inline]
fn g(x: u32, y: u32, z: u32) -> u32 {
    (x & z) | (y & (!z))
}
#[inline]
fn h(x: u32, y: u32, z: u32) -> u32 {
    x ^ y ^ z
}
#[inline]
fn i(x: u32, y: u32, z: u32) -> u32 {
    y ^ (x | (!z))
}
#[inline]
fn rotate_left(x: u32, n: u32) -> u32 {
    x.rotate_left(n)
}

#[inline]
fn ff(a: &mut u32, b: u32, c: u32, d: u32, x: u32, s: u32, ac: u32) {
    *a = a
        .wrapping_add(f(b, c, d))
        .wrapping_add(x)
        .wrapping_add(ac);
    *a = rotate_left(*a, s);
    *a = a.wrapping_add(b);
}
#[inline]
fn gg(a: &mut u32, b: u32, c: u32, d: u32, x: u32, s: u32, ac: u32) {
    *a = a
        .wrapping_add(g(b, c, d))
        .wrapping_add(x)
        .wrapping_add(ac);
    *a = rotate_left(*a, s);
    *a = a.wrapping_add(b);
}
#[inline]
fn hh(a: &mut u32, b: u32, c: u32, d: u32, x: u32, s: u32, ac: u32) {
    *a = a
        .wrapping_add(h(b, c, d))
        .wrapping_add(x)
        .wrapping_add(ac);
    *a = rotate_left(*a, s);
    *a = a.wrapping_add(b);
}
#[inline]
fn ii(a: &mut u32, b: u32, c: u32, d: u32, x: u32, s: u32, ac: u32) {
    *a = a
        .wrapping_add(i(b, c, d))
        .wrapping_add(x)
        .wrapping_add(ac);
    *a = rotate_left(*a, s);
    *a = a.wrapping_add(b);
}

/// `md5_calc` (`src/common/md5.c`): apply the MD5 block transform to one
/// 64-byte block, updating the four state words.
fn md5_calc(ctxt: &mut md5_ctxt, block: &[u8; 64]) {
    /*
     * The C reinterprets the byte block as an array of host-order uint32 (the
     * `X[]` schedule).  We assemble the 16 words little-endian (MD5's defined
     * byte order), which matches the C on the little-endian hosts PostgreSQL
     * supports.
     */
    let mut x = [0u32; 16];
    for j in 0..16 {
        x[j] = (block[j * 4] as u32)
            | ((block[j * 4 + 1] as u32) << 8)
            | ((block[j * 4 + 2] as u32) << 16)
            | ((block[j * 4 + 3] as u32) << 24);
    }

    let mut a = ctxt.md5_st[0];
    let mut b = ctxt.md5_st[1];
    let mut c = ctxt.md5_st[2];
    let mut d = ctxt.md5_st[3];

    /* Round 1 */
    ff(&mut a, b, c, d, x[0], S11, T[0]);
    ff(&mut d, a, b, c, x[1], S12, T[1]);
    ff(&mut c, d, a, b, x[2], S13, T[2]);
    ff(&mut b, c, d, a, x[3], S14, T[3]);
    ff(&mut a, b, c, d, x[4], S11, T[4]);
    ff(&mut d, a, b, c, x[5], S12, T[5]);
    ff(&mut c, d, a, b, x[6], S13, T[6]);
    ff(&mut b, c, d, a, x[7], S14, T[7]);
    ff(&mut a, b, c, d, x[8], S11, T[8]);
    ff(&mut d, a, b, c, x[9], S12, T[9]);
    ff(&mut c, d, a, b, x[10], S13, T[10]);
    ff(&mut b, c, d, a, x[11], S14, T[11]);
    ff(&mut a, b, c, d, x[12], S11, T[12]);
    ff(&mut d, a, b, c, x[13], S12, T[13]);
    ff(&mut c, d, a, b, x[14], S13, T[14]);
    ff(&mut b, c, d, a, x[15], S14, T[15]);

    /* Round 2 */
    gg(&mut a, b, c, d, x[1], S21, T[16]);
    gg(&mut d, a, b, c, x[6], S22, T[17]);
    gg(&mut c, d, a, b, x[11], S23, T[18]);
    gg(&mut b, c, d, a, x[0], S24, T[19]);
    gg(&mut a, b, c, d, x[5], S21, T[20]);
    gg(&mut d, a, b, c, x[10], S22, T[21]);
    gg(&mut c, d, a, b, x[15], S23, T[22]);
    gg(&mut b, c, d, a, x[4], S24, T[23]);
    gg(&mut a, b, c, d, x[9], S21, T[24]);
    gg(&mut d, a, b, c, x[14], S22, T[25]);
    gg(&mut c, d, a, b, x[3], S23, T[26]);
    gg(&mut b, c, d, a, x[8], S24, T[27]);
    gg(&mut a, b, c, d, x[13], S21, T[28]);
    gg(&mut d, a, b, c, x[2], S22, T[29]);
    gg(&mut c, d, a, b, x[7], S23, T[30]);
    gg(&mut b, c, d, a, x[12], S24, T[31]);

    /* Round 3 */
    hh(&mut a, b, c, d, x[5], S31, T[32]);
    hh(&mut d, a, b, c, x[8], S32, T[33]);
    hh(&mut c, d, a, b, x[11], S33, T[34]);
    hh(&mut b, c, d, a, x[14], S34, T[35]);
    hh(&mut a, b, c, d, x[1], S31, T[36]);
    hh(&mut d, a, b, c, x[4], S32, T[37]);
    hh(&mut c, d, a, b, x[7], S33, T[38]);
    hh(&mut b, c, d, a, x[10], S34, T[39]);
    hh(&mut a, b, c, d, x[13], S31, T[40]);
    hh(&mut d, a, b, c, x[0], S32, T[41]);
    hh(&mut c, d, a, b, x[3], S33, T[42]);
    hh(&mut b, c, d, a, x[6], S34, T[43]);
    hh(&mut a, b, c, d, x[9], S31, T[44]);
    hh(&mut d, a, b, c, x[12], S32, T[45]);
    hh(&mut c, d, a, b, x[15], S33, T[46]);
    hh(&mut b, c, d, a, x[2], S34, T[47]);

    /* Round 4 */
    ii(&mut a, b, c, d, x[0], S41, T[48]);
    ii(&mut d, a, b, c, x[7], S42, T[49]);
    ii(&mut c, d, a, b, x[14], S43, T[50]);
    ii(&mut b, c, d, a, x[5], S44, T[51]);
    ii(&mut a, b, c, d, x[12], S41, T[52]);
    ii(&mut d, a, b, c, x[3], S42, T[53]);
    ii(&mut c, d, a, b, x[10], S43, T[54]);
    ii(&mut b, c, d, a, x[1], S44, T[55]);
    ii(&mut a, b, c, d, x[8], S41, T[56]);
    ii(&mut d, a, b, c, x[15], S42, T[57]);
    ii(&mut c, d, a, b, x[6], S43, T[58]);
    ii(&mut b, c, d, a, x[13], S44, T[59]);
    ii(&mut a, b, c, d, x[4], S41, T[60]);
    ii(&mut d, a, b, c, x[11], S42, T[61]);
    ii(&mut c, d, a, b, x[2], S43, T[62]);
    ii(&mut b, c, d, a, x[9], S44, T[63]);

    ctxt.md5_st[0] = ctxt.md5_st[0].wrapping_add(a);
    ctxt.md5_st[1] = ctxt.md5_st[1].wrapping_add(b);
    ctxt.md5_st[2] = ctxt.md5_st[2].wrapping_add(c);
    ctxt.md5_st[3] = ctxt.md5_st[3].wrapping_add(d);
}

/// `md5_init` (`src/common/md5.c`): initialize a context to the RFC 1321
/// starting state.
pub fn md5_init(ctxt: &mut md5_ctxt) {
    ctxt.md5_n = 0;
    ctxt.md5_i = 0;
    ctxt.md5_st[0] = MD5_A0;
    ctxt.md5_st[1] = MD5_B0;
    ctxt.md5_st[2] = MD5_C0;
    ctxt.md5_st[3] = MD5_D0;
    ctxt.md5_buf = [0; 64];
}

impl md5_ctxt {
    /// Allocate a fresh, initialized context (`md5_init` over a zeroed struct).
    pub fn new() -> Self {
        let mut c = md5_ctxt {
            md5_st: [0; 4],
            md5_n: 0,
            md5_buf: [0; 64],
            md5_i: 0,
        };
        md5_init(&mut c);
        c
    }
}

impl Default for md5_ctxt {
    fn default() -> Self {
        md5_ctxt::new()
    }
}

/// `md5_loop` (`src/common/md5.c`): feed `len` bytes of `input` into the
/// running hash.
pub fn md5_loop(ctxt: &mut md5_ctxt, input: &[u8], len: usize) {
    ctxt.md5_n = ctxt.md5_n.wrapping_add((len as u64) * 8);

    let mut off = 0usize;
    let mut remaining = len;
    while remaining > 0 {
        let gap = 64 - ctxt.md5_i;
        let copy = gap.min(remaining);
        ctxt.md5_buf[ctxt.md5_i..ctxt.md5_i + copy].copy_from_slice(&input[off..off + copy]);
        ctxt.md5_i += copy;
        off += copy;
        remaining -= copy;
        if ctxt.md5_i == 64 {
            let block = ctxt.md5_buf;
            md5_calc(ctxt, &block);
            ctxt.md5_i = 0;
        }
    }
}

/// `md5_pad` (`src/common/md5.c`): append the `0x80` terminator, the zero
/// padding, and the 64-bit length, running the final block transform(s).
pub fn md5_pad(ctxt: &mut md5_ctxt) {
    let gap = 64 - ctxt.md5_i;

    if gap > 8 {
        /* room for the 0x80 + length in this block */
        ctxt.md5_buf[ctxt.md5_i] = 0x80;
        for byte in &mut ctxt.md5_buf[ctxt.md5_i + 1..64 - 8] {
            *byte = 0;
        }
    } else {
        /* split: terminator + zeros here, length in a fresh block */
        if gap == 0 {
            /* shouldn't happen: a full block is flushed by md5_loop */
            let block = ctxt.md5_buf;
            md5_calc(ctxt, &block);
            ctxt.md5_i = 0;
        }
        ctxt.md5_buf[ctxt.md5_i] = 0x80;
        for byte in &mut ctxt.md5_buf[ctxt.md5_i + 1..64] {
            *byte = 0;
        }
        let block = ctxt.md5_buf;
        md5_calc(ctxt, &block);
        ctxt.md5_i = 0;
        for byte in &mut ctxt.md5_buf[0..64 - 8] {
            *byte = 0;
        }
    }

    /* append the 64-bit little-endian bit length */
    let n = ctxt.md5_n;
    ctxt.md5_buf[56..64].copy_from_slice(&n.to_le_bytes());
    let block = ctxt.md5_buf;
    md5_calc(ctxt, &block);
}

/// `md5_result` (`src/common/md5.c`): write the 16-byte digest into `digest`
/// (after `md5_pad`).
pub fn md5_result(digest: &mut [u8; 16], ctxt: &md5_ctxt) {
    for j in 0..4 {
        digest[j * 4..j * 4 + 4].copy_from_slice(&ctxt.md5_st[j].to_le_bytes());
    }
}

// ---------------------------------------------------------------------------
// Shared front-end (`src/common/md5_common.c`).
// ---------------------------------------------------------------------------

/// `bytesToHex` (`src/common/md5_common.c`): convert `b` (16 bytes) to its
/// lowercase-hex representation into `s` (33 bytes incl. trailing NUL).
fn bytes_to_hex(b: &[u8; 16], s: &mut [u8; 33]) {
    static HEX: &[u8; 16] = b"0123456789abcdef";
    let mut q = 0usize;
    for &byte in b.iter() {
        s[q] = HEX[(byte >> 4) as usize];
        s[q + 1] = HEX[(byte & 0x0f) as usize];
        q += 2;
    }
    s[q] = 0;
}

/// `pg_md5_binary(buff, len, dest, &errstr)` (`src/common/md5_common.c`):
/// compute the raw 16-byte MD5 digest of `buff`.
///
/// Returns `Ok(digest)`.  The C signature returns a `bool` + an `*errstr`
/// out-param (only set on the OpenSSL backend failure path); this software
/// implementation never fails, mirroring the always-success fallback.
pub fn pg_md5_binary(buff: &[u8]) -> Result<[u8; 16], String> {
    let mut ctxt = md5_ctxt::new();
    md5_loop(&mut ctxt, buff, buff.len());
    md5_pad(&mut ctxt);
    let mut digest = [0u8; 16];
    md5_result(&mut digest, &ctxt);
    Ok(digest)
}

/// `pg_md5_hash(buff, len, hexsum, &errstr)` (`src/common/md5_common.c`):
/// compute the MD5 digest of `buff` and hex-encode it into a 32-char string
/// (the C writes into a caller buffer of `MD5_HASH_LEN + 1` bytes).
///
/// Returns `Ok(hexstring)` (32 ASCII lowercase-hex chars).
pub fn pg_md5_hash(buff: &[u8]) -> Result<String, String> {
    let sum = pg_md5_binary(buff)?;
    let mut hexsum = [0u8; 33];
    bytes_to_hex(&sum, &mut hexsum);
    /* drop the trailing NUL, mirroring cstring semantics */
    Ok(String::from_utf8_lossy(&hexsum[..MD5_HASH_LEN]).into_owned())
}

/// `pg_md5_encrypt(passwd, salt, salt_len, buf, &errstr)`
/// (`src/common/md5_common.c`): compute the MD5 password hash of `passwd`
/// salted with `salt`, formatted as `"md5"` + 32 hex digits.
///
/// Returns `Ok(formatted)`; the C reports failure via `bool` + `*errstr` (only
/// on the OpenSSL backend), which is the never-taken `Err` arm here.
pub fn pg_md5_encrypt(passwd: &[u8], salt: &[u8]) -> Result<String, String> {
    /*
     * C concatenates passwd || salt into a scratch buffer (palloc'd
     * passwd_len + salt_len), hashes that into 32 hex chars after the literal
     * "md5" prefix, then frees the scratch buffer.
     */
    let mut crypt_buf = Vec::with_capacity(passwd.len() + salt.len());
    crypt_buf.extend_from_slice(passwd);
    crypt_buf.extend_from_slice(salt);

    let hex = pg_md5_hash(&crypt_buf)?;
    Ok(format!("md5{hex}"))
}

// ---------------------------------------------------------------------------
// Seam installation.
// ---------------------------------------------------------------------------

/// Install the `pg_md5_*` seams whose owner is `common/md5.c` (this crate):
/// `pg_md5_encrypt` (declared in `backend-libpq-crypt-seams`, consumed by
/// `crypt.c`) and `pg_md5_binary` (declared in `backend-libpq-auth-seams`).
pub fn init_seams() {
    crypt_seams::pg_md5_encrypt::set(seam_pg_md5_encrypt);
    auth_seams::pg_md5_binary::set(seam_pg_md5_binary);
}

/// Seam body for `pg_md5_encrypt`: `Ok(Ok(hash))` on success / `Ok(Err(errstr))`
/// on failure (mirroring the C `bool` + `*errstr`).
fn seam_pg_md5_encrypt(
    passwd: &[u8],
    salt: &[u8],
) -> types_error::PgResult<Result<String, String>> {
    Ok(pg_md5_encrypt(passwd, salt))
}

/// Seam body for `pg_md5_binary`: `Ok(Ok(digest))` on success.
fn seam_pg_md5_binary(buff: Vec<u8>) -> types_error::PgResult<Result<[u8; 16], String>> {
    Ok(pg_md5_binary(&buff))
}
