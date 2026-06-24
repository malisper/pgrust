//! OpenPGP constants + cipher/digest tables (pgp.h, pgp.c) and a small
//! incremental digest wrapper over the `cryptohash` provider (used by S2K and
//! the MDC).

use ::crypto::pg_cryptohash_type::{self, PG_MD5, PG_SHA1, PG_SHA256, PG_SHA384, PG_SHA512};
use ::cryptohash::{
    pg_cryptohash_create, pg_cryptohash_final, pg_cryptohash_free, pg_cryptohash_init,
    pg_cryptohash_update,
};

// S2K modes.
pub const PGP_S2K_SIMPLE: i32 = 0;
pub const PGP_S2K_SALTED: i32 = 1;
pub const PGP_S2K_ISALTED: i32 = 3;
pub const PGP_S2K_SALT: usize = 8;

// Packet tags.
pub const PGP_PKT_PUBENC_SESSKEY: i32 = 1;
pub const PGP_PKT_SYMENC_SESSKEY: i32 = 3;
pub const PGP_PKT_SECRET_KEY: i32 = 5;
pub const PGP_PKT_PUBLIC_KEY: i32 = 6;
pub const PGP_PKT_SECRET_SUBKEY: i32 = 7;
pub const PGP_PKT_COMPRESSED_DATA: i32 = 8;
pub const PGP_PKT_SYMENC_DATA: i32 = 9;
pub const PGP_PKT_MARKER: i32 = 10;
pub const PGP_PKT_LITERAL_DATA: i32 = 11;
pub const PGP_PKT_TRUST: i32 = 12;
pub const PGP_PKT_USER_ID: i32 = 13;
pub const PGP_PKT_PUBLIC_SUBKEY: i32 = 14;
pub const PGP_PKT_USER_ATTR: i32 = 17;
pub const PGP_PKT_SYMENC_DATA_MDC: i32 = 18;
pub const PGP_PKT_MDC: i32 = 19;
pub const PGP_PKT_PRIV_61: i32 = 61;
pub const PGP_PKT_SIGNATURE: i32 = 2;

// Symmetric cipher ids.
pub const PGP_SYM_PLAIN: i32 = 0;
pub const PGP_SYM_DES3: i32 = 2;
pub const PGP_SYM_CAST5: i32 = 3;
pub const PGP_SYM_BLOWFISH: i32 = 4;
pub const PGP_SYM_AES_128: i32 = 7;
pub const PGP_SYM_AES_192: i32 = 8;
pub const PGP_SYM_AES_256: i32 = 9;

// Digest ids.
pub const PGP_DIGEST_MD5: i32 = 1;
pub const PGP_DIGEST_SHA1: i32 = 2;
pub const PGP_DIGEST_RIPEMD160: i32 = 3;
pub const PGP_DIGEST_SHA256: i32 = 8;
pub const PGP_DIGEST_SHA384: i32 = 9;
pub const PGP_DIGEST_SHA512: i32 = 10;

// Compression ids.
pub const PGP_COMPR_NONE: i32 = 0;
pub const PGP_COMPR_ZIP: i32 = 1;
pub const PGP_COMPR_ZLIB: i32 = 2;
pub const PGP_COMPR_BZIP2: i32 = 3;

pub const PGP_MAX_KEY: usize = 32;
pub const PGP_MAX_BLOCK: usize = 16;

pub const MDC_DIGEST_LEN: usize = 20;

/// pgcrypto error strings (px_strerror). Both `PXE_PGP_CORRUPT_DATA` and the
/// generic wrong-key code map to "Wrong key or corrupt data" in px.c.
pub const CORRUPT_DATA: &str = "Wrong key or corrupt data";
pub const WRONG_KEY: &str = "Wrong key or corrupt data";
pub const UNSUPPORTED_CIPHER: &str = "Unsupported cipher algorithm";
pub const UNSUPPORTED_HASH: &str = "Unsupported digest algorithm";
pub const UNSUPPORTED_COMPR: &str = "Unsupported compression algorithm";
pub const NOT_TEXT: &str = "Not text data";
pub const NO_USABLE_KEY: &str = "No encryption key found";

/// `s2k_decode_count` (RFC 4880 §3.7.1.3).
pub fn s2k_decode_count(cval: i32) -> i32 {
    (16 + (cval & 15)) << ((cval >> 4) + 6)
}

/// Cipher info: (key_len, block_len) in bytes, or None for unknown/plain.
pub fn cipher_key_size(code: i32) -> usize {
    match code {
        PGP_SYM_DES3 => 24,
        PGP_SYM_CAST5 => 16,
        PGP_SYM_BLOWFISH => 16,
        PGP_SYM_AES_128 => 16,
        PGP_SYM_AES_192 => 24,
        PGP_SYM_AES_256 => 32,
        _ => 0,
    }
}

pub fn cipher_block_size(code: i32) -> usize {
    match code {
        PGP_SYM_DES3 | PGP_SYM_CAST5 | PGP_SYM_BLOWFISH => 8,
        PGP_SYM_AES_128 | PGP_SYM_AES_192 | PGP_SYM_AES_256 => 16,
        _ => 0,
    }
}

/// The cipher spec string for `cipher::encrypt`/`decrypt` ECB single-block.
pub fn cipher_int_name(code: i32) -> Option<&'static str> {
    match code {
        PGP_SYM_DES3 => Some("3des-ecb"),
        PGP_SYM_CAST5 => Some("cast5-ecb"),
        PGP_SYM_BLOWFISH => Some("bf-ecb"),
        PGP_SYM_AES_128 | PGP_SYM_AES_192 | PGP_SYM_AES_256 => Some("aes-ecb"),
        _ => None,
    }
}

/// `pgp_get_cipher_code(name)` (case-insensitive).
pub fn cipher_code(name: &str) -> Option<i32> {
    match name.to_ascii_lowercase().as_str() {
        "3des" => Some(PGP_SYM_DES3),
        "cast5" => Some(PGP_SYM_CAST5),
        "bf" | "blowfish" => Some(PGP_SYM_BLOWFISH),
        "aes" | "aes128" => Some(PGP_SYM_AES_128),
        "aes192" => Some(PGP_SYM_AES_192),
        "aes256" => Some(PGP_SYM_AES_256),
        "twofish" => Some(10),
        _ => None,
    }
}

/// `pgp_get_digest_code(name)`.
pub fn digest_code(name: &str) -> Option<i32> {
    match name.to_ascii_lowercase().as_str() {
        "md5" => Some(PGP_DIGEST_MD5),
        "sha1" | "sha-1" => Some(PGP_DIGEST_SHA1),
        "ripemd160" => Some(PGP_DIGEST_RIPEMD160),
        "sha256" => Some(PGP_DIGEST_SHA256),
        "sha384" => Some(PGP_DIGEST_SHA384),
        "sha512" => Some(PGP_DIGEST_SHA512),
        _ => None,
    }
}

/// digest code → (cryptohash type, output length).
fn digest_info(code: i32) -> Option<(pg_cryptohash_type, usize)> {
    match code {
        PGP_DIGEST_MD5 => Some((PG_MD5, 16)),
        PGP_DIGEST_SHA1 => Some((PG_SHA1, 20)),
        PGP_DIGEST_SHA256 => Some((PG_SHA256, 32)),
        PGP_DIGEST_SHA384 => Some((PG_SHA384, 48)),
        PGP_DIGEST_SHA512 => Some((PG_SHA512, 64)),
        _ => None,
    }
}

/// An incremental message digest over the `cryptohash` provider — used by S2K
/// (multi-round keyed hashing) and the MDC SHA1.
pub struct Digest {
    ctx: *mut ::crypto::pg_cryptohash_ctx,
    len: usize,
}

impl Digest {
    pub fn new(code: i32) -> Option<Digest> {
        let (ty, len) = digest_info(code)?;
        let ctx = pg_cryptohash_create(ty);
        if ctx.is_null() {
            return None;
        }
        let _ = pg_cryptohash_init(ctx);
        Some(Digest { ctx, len })
    }

    pub fn result_size(&self) -> usize {
        self.len
    }

    pub fn reset(&mut self) {
        let _ = pg_cryptohash_init(self.ctx);
    }

    pub fn update(&mut self, data: &[u8]) {
        let _ = pg_cryptohash_update(self.ctx, data.as_ptr(), data.len());
    }

    /// Finalize into a fresh buffer. Re-inits so the context can be reused.
    pub fn finish(&mut self) -> Vec<u8> {
        let mut out = vec![0u8; self.len];
        let _ = pg_cryptohash_final(self.ctx, out.as_mut_ptr(), out.len());
        let _ = pg_cryptohash_init(self.ctx);
        out
    }
}

impl Drop for Digest {
    fn drop(&mut self) {
        pg_cryptohash_free(self.ctx);
    }
}
