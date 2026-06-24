//! digest() and hmac() over PG's in-tree reference hashes (the `cryptohash`
//! crate, `pg_cryptohash_*`). Byte-identical to the C (non-OpenSSL) build.

use ::crypto::pg_cryptohash_type::{
    self, PG_MD5, PG_SHA1, PG_SHA224, PG_SHA256, PG_SHA384, PG_SHA512,
};
use ::cryptohash::{
    pg_cryptohash_create, pg_cryptohash_final, pg_cryptohash_free, pg_cryptohash_init,
    pg_cryptohash_update,
};

/// A hash algorithm's `pg_cryptohash_type`, digest length, and HMAC block size.
struct HashAlgo {
    ty: pg_cryptohash_type,
    digest_len: usize,
    block_size: usize,
}

/// `px_find_digest` / `EVP_get_digestbyname` — resolve a digest name (already
/// lower-cased by the caller; we lower-case here defensively) to its algorithm.
fn find_digest(name: &str) -> Option<HashAlgo> {
    let lower = name.to_ascii_lowercase();
    let (ty, digest_len, block_size) = match lower.as_str() {
        "md5" => (PG_MD5, 16, 64),
        "sha1" => (PG_SHA1, 20, 64),
        "sha224" => (PG_SHA224, 28, 64),
        "sha256" => (PG_SHA256, 32, 64),
        "sha384" => (PG_SHA384, 48, 128),
        "sha512" => (PG_SHA512, 64, 128),
        _ => return None,
    };
    Some(HashAlgo {
        ty,
        digest_len,
        block_size,
    })
}

/// Run one hash over `data`, returning the digest bytes. Drives the
/// `pg_cryptohash_*` lifecycle exactly as C's `cryptohash` provider does.
fn hash_bytes(algo: &HashAlgo, data: &[u8]) -> Vec<u8> {
    let ctx = pg_cryptohash_create(algo.ty);
    assert!(!ctx.is_null(), "pgcrypto: pg_cryptohash_create OOM");
    let _ = pg_cryptohash_init(ctx);
    let _ = pg_cryptohash_update(ctx, data.as_ptr(), data.len());
    let mut out = vec![0u8; algo.digest_len];
    let _ = pg_cryptohash_final(ctx, out.as_mut_ptr(), out.len());
    pg_cryptohash_free(ctx);
    out
}

/// `pg_digest(data, type)` — the digest bytes, or the pgcrypto error string for
/// an unknown algorithm. The error text matches C's `px_strerror(PXE_NO_HASH)`:
/// `Cannot use "<name>": No such hash algorithm`.
pub fn digest(name: &str, data: &[u8]) -> Result<Vec<u8>, String> {
    let algo = find_digest(name)
        .ok_or_else(|| format!("Cannot use \"{name}\": No such hash algorithm"))?;
    Ok(hash_bytes(&algo, data))
}

/// `pg_hmac(data, key, type)` — RFC 2104 HMAC over the resolved hash. Matches
/// C's `px_find_hmac` + `px_hmac_*` (which is HMAC over the same reference
/// hashes). Same unknown-algorithm error string as [`digest`].
pub fn hmac(name: &str, key: &[u8], data: &[u8]) -> Result<Vec<u8>, String> {
    let algo = find_digest(name)
        .ok_or_else(|| format!("Cannot use \"{name}\": No such hash algorithm"))?;

    let b = algo.block_size;

    // K0: key shortened (by hashing) if longer than block size, then zero-padded.
    let mut k0 = vec![0u8; b];
    if key.len() > b {
        let hk = hash_bytes(&algo, key);
        k0[..hk.len()].copy_from_slice(&hk);
    } else {
        k0[..key.len()].copy_from_slice(key);
    }

    let mut ipad = vec![0u8; b];
    let mut opad = vec![0u8; b];
    for i in 0..b {
        ipad[i] = k0[i] ^ 0x36;
        opad[i] = k0[i] ^ 0x5c;
    }

    // inner = H(ipad || data)
    let mut inner_in = ipad;
    inner_in.extend_from_slice(data);
    let inner = hash_bytes(&algo, &inner_in);

    // outer = H(opad || inner)
    let mut outer_in = opad;
    outer_in.extend_from_slice(&inner);
    Ok(hash_bytes(&algo, &outer_in))
}
