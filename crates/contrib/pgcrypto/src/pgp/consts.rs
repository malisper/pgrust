
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
pub const PGP_PKT_MDC: i32 = 19;
pub const PGP_PKT_PRIV_61: i32 = 61;
pub const PGP_PKT_SIGNATURE: i32 = 2;

pub const PGP_SYM_PLAIN: i32 = 0;
pub const PGP_SYM_DES3: i32 = 2;
pub const PGP_SYM_CAST5: i32 = 3;
pub const PGP_SYM_BLOWFISH: i32 = 4;
pub const PGP_SYM_AES_128: i32 = 7;
pub const PGP_SYM_AES_192: i32 = 8;
pub const PGP_SYM_AES_256: i32 = 9;

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
pub const PGP_MAX_BLOCK: usize = 16;

pub const MDC_DIGEST_LEN: usize = 20;

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

pub fn cipher_int_name(code: i32) -> Option<&'static str> {
    match code {
        PGP_SYM_DES3 => Some("3des-ecb"),
        PGP_SYM_CAST5 => Some("cast5-ecb"),
        PGP_SYM_BLOWFISH => Some("bf-ecb"),
        PGP_SYM_AES_128 | PGP_SYM_AES_192 | PGP_SYM_AES_256 => Some("aes-ecb"),
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
        "twofish" => Some(10),
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

#[derive(Clone)]
enum Hasher {
    Md5(::pg_md5::Md5),
    Sha1(::pg_sha1::Sha1),
    Sha256(::pg_sha2::PgSha256Ctx),
    Sha384(::pg_sha2::PgSha512Ctx),
    Sha512(::pg_sha2::PgSha512Ctx),
}

#[derive(Clone)]
pub struct Digest {
    initial: Hasher,
    state: Hasher,
    len: usize,
}

impl Digest {
    pub fn new(code: i32) -> Option<Digest> {
        let (state, len) = match code {
            PGP_DIGEST_MD5 => (Hasher::Md5(::pg_md5::Md5::new()), 16),
            PGP_DIGEST_SHA1 => (Hasher::Sha1(::pg_sha1::Sha1::init()), 20),
            PGP_DIGEST_SHA256 => (Hasher::Sha256(::pg_sha2::PgSha256Ctx::init_sha256()), 32),
            PGP_DIGEST_SHA384 => (Hasher::Sha384(::pg_sha2::PgSha512Ctx::init_sha384()), 48),
            PGP_DIGEST_SHA512 => (Hasher::Sha512(::pg_sha2::PgSha512Ctx::init_sha512()), 64),
            _ => return None,
        };
        Some(Digest { initial: state.clone(), state, len })
    }

    pub fn result_size(&self) -> usize {
        self.len
    }

    pub fn reset(&mut self) {
        self.state = self.initial.clone();
    }

    pub fn update(&mut self, data: &[u8]) {
        match &mut self.state {
            Hasher::Md5(c) => c.update(data),
            Hasher::Sha1(c) => c.update(data),
            Hasher::Sha256(c) => c.update(data),
            Hasher::Sha384(c) => c.update(data),
            Hasher::Sha512(c) => c.update(data),
        }
    }

    pub fn finish(&mut self) -> Vec<u8> {
        let out = match self.state.clone() {
            Hasher::Md5(c) => c.finish().to_vec(),
            Hasher::Sha1(c) => c.finish().to_vec(),
            Hasher::Sha256(c) => c.final_sha256().to_vec(),
            Hasher::Sha384(c) => c.final_sha384().to_vec(),
            Hasher::Sha512(c) => c.final_sha512().to_vec(),
        };
        self.reset();
        out
    }
}
