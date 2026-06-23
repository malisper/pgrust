//! Known-answer tests for the SHA-2 family, using the standard FIPS-180
//! example vectors plus a multi-block / multi-update case to exercise the
//! buffering and padding edge paths.

use super::*;
use std::vec::Vec;

fn hex(bytes: &[u8]) -> std::string::String {
    use std::fmt::Write;
    let mut s = std::string::String::new();
    for b in bytes {
        write!(s, "{:02x}", b).unwrap();
    }
    s
}

fn sha256_hex(data: &[u8]) -> std::string::String {
    let mut ctx = pg_sha256_ctx::default();
    pg_sha256_init(&mut ctx);
    pg_sha256_update(&mut ctx, data, data.len());
    let mut digest = [0u8; PG_SHA256_DIGEST_LENGTH];
    pg_sha256_final(&mut ctx, &mut digest);
    hex(&digest)
}

fn sha224_hex(data: &[u8]) -> std::string::String {
    let mut ctx = pg_sha224_ctx::default();
    pg_sha224_init(&mut ctx);
    pg_sha224_update(&mut ctx, data, data.len());
    let mut digest = [0u8; PG_SHA224_DIGEST_LENGTH];
    pg_sha224_final(&mut ctx, &mut digest);
    hex(&digest)
}

fn sha384_hex(data: &[u8]) -> std::string::String {
    let mut ctx = pg_sha384_ctx::default();
    pg_sha384_init(&mut ctx);
    pg_sha384_update(&mut ctx, data, data.len());
    let mut digest = [0u8; PG_SHA384_DIGEST_LENGTH];
    pg_sha384_final(&mut ctx, &mut digest);
    hex(&digest)
}

fn sha512_hex(data: &[u8]) -> std::string::String {
    let mut ctx = pg_sha512_ctx::default();
    pg_sha512_init(&mut ctx);
    pg_sha512_update(&mut ctx, data, data.len());
    let mut digest = [0u8; PG_SHA512_DIGEST_LENGTH];
    pg_sha512_final(&mut ctx, &mut digest);
    hex(&digest)
}

#[test]
fn sha256_empty() {
    assert_eq!(
        sha256_hex(b""),
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    );
}

#[test]
fn sha256_abc() {
    assert_eq!(
        sha256_hex(b"abc"),
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );
}

#[test]
fn sha256_two_block() {
    // FIPS-180 example: 448-bit message ("abc...nopq"), 56 bytes -> padding
    // pushes it into a single block but exercises the short-block path.
    assert_eq!(
        sha256_hex(b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"),
        "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1"
    );
}

#[test]
fn sha256_million_a() {
    // FIPS-180 example: one million 'a' bytes. Exercises many full blocks
    // and the multi-update buffering path.
    let mut ctx = pg_sha256_ctx::default();
    pg_sha256_init(&mut ctx);
    let chunk = [b'a'; 1000];
    for _ in 0..1000 {
        pg_sha256_update(&mut ctx, &chunk, chunk.len());
    }
    let mut digest = [0u8; PG_SHA256_DIGEST_LENGTH];
    pg_sha256_final(&mut ctx, &mut digest);
    assert_eq!(
        hex(&digest),
        "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0"
    );
}

#[test]
fn sha256_incremental_matches_oneshot() {
    let data: Vec<u8> = (0..=255u8).cycle().take(1000).collect();
    let oneshot = sha256_hex(&data);

    // Feed it one byte at a time.
    let mut ctx = pg_sha256_ctx::default();
    pg_sha256_init(&mut ctx);
    for b in &data {
        pg_sha256_update(&mut ctx, core::slice::from_ref(b), 1);
    }
    let mut digest = [0u8; PG_SHA256_DIGEST_LENGTH];
    pg_sha256_final(&mut ctx, &mut digest);
    assert_eq!(hex(&digest), oneshot);
}

#[test]
fn sha224_empty() {
    assert_eq!(
        sha224_hex(b""),
        "d14a028c2a3a2bc9476102bb288234c415a2b01f828ea62ac5b3e42f"
    );
}

#[test]
fn sha224_abc() {
    assert_eq!(
        sha224_hex(b"abc"),
        "23097d223405d8228642a477bda255b32aadbce4bda0b3f7e36c9da7"
    );
}

#[test]
fn sha224_two_block() {
    assert_eq!(
        sha224_hex(b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"),
        "75388b16512776cc5dba5da1fd890150b0c6455cb4f58b1952522525"
    );
}

#[test]
fn sha512_empty() {
    assert_eq!(
        sha512_hex(b""),
        "cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce\
         47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e"
    );
}

#[test]
fn sha512_abc() {
    assert_eq!(
        sha512_hex(b"abc"),
        "ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a\
         2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f"
    );
}

#[test]
fn sha512_two_block() {
    // FIPS-180 example: 896-bit (112-byte) message exercising the two-block
    // padding path of SHA-512.
    assert_eq!(
        sha512_hex(
            b"abcdefghbcdefghicdefghijdefghijkefghijklfghijklmghijklmn\
              hijklmnoijklmnopjklmnopqklmnopqrlmnopqrsmnopqrstnopqrstu"
        ),
        "8e959b75dae313da8cf4f72814fc143f8f7779c6eb9f7fa17299aeadb6889018\
         501d289e4900f7e4331b99dec4b5433ac7d329eeb6dd26545e96e55b874be909"
    );
}

#[test]
fn sha512_incremental_matches_oneshot() {
    let data: Vec<u8> = (0..=255u8).cycle().take(2000).collect();
    let oneshot = sha512_hex(&data);

    let mut ctx = pg_sha512_ctx::default();
    pg_sha512_init(&mut ctx);
    for b in &data {
        pg_sha512_update(&mut ctx, core::slice::from_ref(b), 1);
    }
    let mut digest = [0u8; PG_SHA512_DIGEST_LENGTH];
    pg_sha512_final(&mut ctx, &mut digest);
    assert_eq!(hex(&digest), oneshot);
}

#[test]
fn sha384_empty() {
    assert_eq!(
        sha384_hex(b""),
        "38b060a751ac96384cd9327eb1b1e36a21fdb71114be0743\
         4c0cc7bf63f6e1da274edebfe76f65fbd51ad2f14898b95b"
    );
}

#[test]
fn sha384_abc() {
    assert_eq!(
        sha384_hex(b"abc"),
        "cb00753f45a35e8bb5a03d699ac65007272c32ab0eded163\
         1a8b605a43ff5bed8086072ba1e7cc2358baeca134c825a7"
    );
}

#[test]
fn sha384_two_block() {
    assert_eq!(
        sha384_hex(
            b"abcdefghbcdefghicdefghijdefghijkefghijklfghijklmghijklmn\
              hijklmnoijklmnopjklmnopqklmnopqrlmnopqrsmnopqrstnopqrstu"
        ),
        "09330c33f71147e83d192fc782cd1b4753111b173b3b05d2\
         2fa08086e3b0f712fcc7c71a557e2db966c3e9fa91746039"
    );
}

#[test]
fn final_with_empty_digest_is_noop_on_output() {
    // Passing an empty digest slice must skip the squeeze (mirrors the
    // `digest != NULL` guard) but still scrub the context.
    let mut ctx = pg_sha256_ctx::default();
    pg_sha256_init(&mut ctx);
    pg_sha256_update(&mut ctx, b"abc", 3);
    let mut empty: [u8; 0] = [];
    pg_sha256_final(&mut ctx, &mut empty);
    // Context fully zeroed afterwards.
    assert_eq!(ctx.state, [0u32; 8]);
    assert_eq!(ctx.bitcount, 0);
}

#[test]
fn update_zero_len_is_noop() {
    let mut ctx = pg_sha256_ctx::default();
    pg_sha256_init(&mut ctx);
    pg_sha256_update(&mut ctx, b"", 0);
    let mut digest = [0u8; PG_SHA256_DIGEST_LENGTH];
    pg_sha256_final(&mut ctx, &mut digest);
    assert_eq!(
        hex(&digest),
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    );
}
