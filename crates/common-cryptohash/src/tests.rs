//! Known-answer tests driving the `pg_cryptohash_*` dispatcher over the
//! raw-pointer seam contract, against standard NIST / RFC vectors.

use super::*;
use types_crypto::pg_cryptohash_type;

/// Run `data` through the dispatcher for `type_` and return the digest.
fn digest(type_: pg_cryptohash_type, data: &[u8], digest_len: usize) -> Vec<u8> {
    let ctx = pg_cryptohash_create(type_);
    assert!(!ctx.is_null());
    assert_eq!(pg_cryptohash_init(ctx), 0);
    assert_eq!(
        pg_cryptohash_update(ctx, data.as_ptr(), data.len()),
        0
    );
    let mut out = vec![0u8; digest_len];
    assert_eq!(pg_cryptohash_final(ctx, out.as_mut_ptr(), out.len()), 0);
    pg_cryptohash_free(ctx);
    out
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[test]
fn md5_abc() {
    // RFC 1321 / standard vector for "abc".
    let d = digest(pg_cryptohash_type::PG_MD5, b"abc", MD5_DIGEST_LENGTH);
    assert_eq!(hex(&d), "900150983cd24fb0d6963f7d28e17f72");
}

#[test]
fn md5_empty() {
    let d = digest(pg_cryptohash_type::PG_MD5, b"", MD5_DIGEST_LENGTH);
    assert_eq!(hex(&d), "d41d8cd98f00b204e9800998ecf8427e");
}

#[test]
fn sha1_abc() {
    // FIPS 180-1 vector for "abc".
    let d = digest(pg_cryptohash_type::PG_SHA1, b"abc", SHA1_DIGEST_LENGTH);
    assert_eq!(hex(&d), "a9993e364706816aba3e25717850c26c9cd0d89d");
}

#[test]
fn sha224_abc() {
    let d = digest(pg_cryptohash_type::PG_SHA224, b"abc", PG_SHA224_DIGEST_LENGTH);
    assert_eq!(
        hex(&d),
        "23097d223405d8228642a477bda255b32aadbce4bda0b3f7e36c9da7"
    );
}

#[test]
fn sha256_abc() {
    let d = digest(pg_cryptohash_type::PG_SHA256, b"abc", PG_SHA256_DIGEST_LENGTH);
    assert_eq!(
        hex(&d),
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );
}

#[test]
fn sha384_abc() {
    let d = digest(pg_cryptohash_type::PG_SHA384, b"abc", PG_SHA384_DIGEST_LENGTH);
    assert_eq!(
        hex(&d),
        "cb00753f45a35e8bb5a03d699ac65007272c32ab0eded1631a8b605a43ff5bed8086072ba1e7cc2358baeca134c825a7"
    );
}

#[test]
fn sha512_abc() {
    let d = digest(pg_cryptohash_type::PG_SHA512, b"abc", PG_SHA512_DIGEST_LENGTH);
    assert_eq!(
        hex(&d),
        "ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f"
    );
}

#[test]
fn null_ctx_returns_minus_one() {
    let nullp = core::ptr::null_mut();
    assert_eq!(pg_cryptohash_init(nullp), -1);
    assert_eq!(pg_cryptohash_update(nullp, core::ptr::null(), 0), -1);
    assert_eq!(pg_cryptohash_final(nullp, core::ptr::null_mut(), 0), -1);
    // free(NULL) is a no-op.
    pg_cryptohash_free(nullp);
}

#[test]
fn short_dest_buffer_errors() {
    let ctx = pg_cryptohash_create(pg_cryptohash_type::PG_SHA256);
    assert_eq!(pg_cryptohash_init(ctx), 0);
    let mut out = [0u8; 4];
    // len < PG_SHA256_DIGEST_LENGTH -> -1 (PG_CRYPTOHASH_ERROR_DEST_LEN).
    assert_eq!(pg_cryptohash_final(ctx, out.as_mut_ptr(), out.len()), -1);
    pg_cryptohash_free(ctx);
}
