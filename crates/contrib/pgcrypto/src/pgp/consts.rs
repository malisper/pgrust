
pub const PGP_S2K_SIMPLE: i32 = 0;
pub const PGP_S2K_SALTED: i32 = 1;
pub const PGP_S2K_ISALTED: i32 = 3;
pub const PGP_S2K_SALT: usize = 8;

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
#[allow(dead_code)] // C-parity: pgp.h constant set kept complete
pub const PGP_PKT_MDC: i32 = 19;
pub const PGP_PKT_PRIV_61: i32 = 61;
pub const PGP_PKT_SIGNATURE: i32 = 2;

#[allow(dead_code)] // C-parity: pgp.h constant set kept complete
pub const PGP_SYM_PLAIN: i32 = 0;
pub const PGP_SYM_DES3: i32 = 2;
pub const PGP_SYM_CAST5: i32 = 3;
pub const PGP_SYM_BLOWFISH: i32 = 4;
pub const PGP_SYM_AES_128: i32 = 7;
pub const PGP_SYM_AES_192: i32 = 8;
pub const PGP_SYM_AES_256: i32 = 9;
pub const PGP_SYM_TWOFISH: i32 = 10;

pub const PGP_DIGEST_MD5: i32 = 1;
pub const PGP_DIGEST_SHA1: i32 = 2;
pub const PGP_DIGEST_RIPEMD160: i32 = 3;
pub const PGP_DIGEST_SHA256: i32 = 8;
pub const PGP_DIGEST_SHA384: i32 = 9;
pub const PGP_DIGEST_SHA512: i32 = 10;

pub const PGP_COMPR_NONE: i32 = 0;
pub const PGP_COMPR_ZIP: i32 = 1;
pub const PGP_COMPR_ZLIB: i32 = 2;
pub const PGP_COMPR_BZIP2: i32 = 3;

pub const PGP_MAX_KEY: usize = 32;
#[allow(dead_code)] // C-parity: pgp.h constant set kept complete
pub const PGP_MAX_BLOCK: usize = 16;

pub const MDC_DIGEST_LEN: usize = 20;

pub const CORRUPT_DATA: &str = "Wrong key or corrupt data";
pub const WRONG_KEY: &str = "Wrong key or corrupt data";
pub const UNSUPPORTED_CIPHER: &str = "Unsupported cipher algorithm";
pub const UNSUPPORTED_HASH: &str = "Unsupported digest algorithm";
pub const UNSUPPORTED_COMPR: &str = "Unsupported compression algorithm";
pub const NOT_TEXT: &str = "Not text data";
pub const NO_USABLE_KEY: &str = "No encryption key found";
// px.c px_err_list rows this port raises (byte-exact px_strerror text).
pub const ARGUMENT_ERROR: &str = "Illegal argument to function";
pub const KEYPKT_CORRUPT: &str = "Corrupt key packet";
pub const NOT_V4_KEYPKT: &str = "Only V4 key packets are supported";
pub const UNKNOWN_PUBALGO: &str = "Unknown public-key encryption algorithm";
pub const MULTIPLE_KEYS: &str = "Several keys given - pgcrypto does not handle keyring";
pub const BAD_S2K_MODE: &str = "Bad S2K mode";
pub const MATH_FAILED: &str = "Math operation failed";
pub const PGCRYPTO_BUG: &str = "pgcrypto bug";
/// PXE_NO_RANDOM as px_THROW_ERROR renders it (px.c:96-101): the one px
/// error whose SQLSTATE is ERRCODE_INTERNAL_ERROR, not 39000.
pub const NO_RANDOM: &str = "could not generate a random number";

// Test-only switch for `fill_random` (see there).
#[cfg(test)]
thread_local! {
    pub static FORCE_RANDOM_FAILURE: core::cell::Cell<bool> = const { core::cell::Cell::new(false) };
}

/// The crate's single entropy funnel over `pg_strong_random`: product builds
/// are a plain call; `#[cfg(test)]` can force the OS-entropy failure arm so
/// the PXE_NO_RANDOM paths (px.c:96 px_THROW_ERROR) are witnessable —
/// pg_strong_random itself never fails on a healthy host.
#[must_use]
pub fn fill_random(buf: &mut [u8]) -> bool {
    #[cfg(test)]
    if FORCE_RANDOM_FAILURE.with(|f| f.get()) {
        return false;
    }
    ::pg_strong_random::pg_strong_random(buf)
}

/// `s2k_decode_count` (RFC 4880 §3.7.1.3).
pub fn s2k_decode_count(cval: i32) -> i32 {
    (16 + (cval & 15)) << ((cval >> 4) + 6)
}

pub fn cipher_key_size(code: i32) -> usize {
    match code {
        PGP_SYM_DES3 => 24,
        PGP_SYM_CAST5 => 16,
        PGP_SYM_BLOWFISH => 16,
        PGP_SYM_AES_128 => 16,
        PGP_SYM_AES_192 => 24,
        PGP_SYM_AES_256 | PGP_SYM_TWOFISH => 32,
        _ => 0,
    }
}

pub fn cipher_block_size(code: i32) -> usize {
    match code {
        PGP_SYM_DES3 | PGP_SYM_CAST5 | PGP_SYM_BLOWFISH => 8,
        PGP_SYM_AES_128 | PGP_SYM_AES_192 | PGP_SYM_AES_256 | PGP_SYM_TWOFISH => 16,
        _ => 0,
    }
}

pub fn cipher_int_name(code: i32) -> Option<&'static str> {
    match code {
        PGP_SYM_DES3 => Some("3des-ecb"),
        PGP_SYM_CAST5 => Some("cast5-ecb"),
        PGP_SYM_BLOWFISH => Some("bf-ecb"),
        PGP_SYM_AES_128 | PGP_SYM_AES_192 | PGP_SYM_AES_256 => Some("aes-ecb"),
        PGP_SYM_TWOFISH => Some("twofish-ecb"),
        _ => None,
    }
}

pub fn cipher_code(name: &str) -> Option<i32> {
    match name.to_ascii_lowercase().as_str() {
        "3des" => Some(PGP_SYM_DES3),
        "cast5" => Some(PGP_SYM_CAST5),
        "bf" | "blowfish" => Some(PGP_SYM_BLOWFISH),
        "aes" | "aes128" => Some(PGP_SYM_AES_128),
        "aes192" => Some(PGP_SYM_AES_192),
        "aes256" => Some(PGP_SYM_AES_256),
        "twofish" => Some(PGP_SYM_TWOFISH),
        _ => None,
    }
}

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

pub fn digest_name(code: i32) -> Option<&'static str> {
    match code {
        PGP_DIGEST_MD5 => Some("md5"),
        PGP_DIGEST_SHA1 => Some("sha1"),
        PGP_DIGEST_RIPEMD160 => Some("ripemd160"),
        PGP_DIGEST_SHA256 => Some("sha256"),
        PGP_DIGEST_SHA384 => Some("sha384"),
        PGP_DIGEST_SHA512 => Some("sha512"),
        _ => None,
    }
}

/// pgp.c:174 pgp_load_digest over openssl.c px_find_digest.
pub struct Digest {
    md: crate::hashing::OsslDigest,
}

impl Digest {
    pub fn new(code: i32) -> Option<Digest> {
        let name = digest_name(code)?;
        let md = crate::hashing::OsslDigest::find(name).ok()?;
        Some(Digest { md })
    }

    pub fn result_size(&self) -> usize {
        self.md.result_size().unwrap_or(0)
    }

    pub fn reset(&mut self) {
        self.md.reset();
    }

    pub fn update(&mut self, data: &[u8]) {
        self.md.update(data);
    }

    pub fn finish(&mut self) -> Vec<u8> {
        let out = self.md.finish();
        self.reset();
        out
    }
}
