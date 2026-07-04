use super::*;

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[test]
fn sha256_nist_vectors() {
    assert_eq!(
        hex(&sha256(b"")),
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    );
    assert_eq!(
        hex(&sha256(b"abc")),
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );
    assert_eq!(
        hex(&sha256(b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq")),
        "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1"
    );
    assert_eq!(
        hex(&sha256(&[b'a'; 1_000_000])),
        "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0"
    );
}

#[test]
fn sha224_nist_vectors() {
    assert_eq!(
        hex(&sha224(b"")),
        "d14a028c2a3a2bc9476102bb288234c415a2b01f828ea62ac5b3e42f"
    );
    assert_eq!(
        hex(&sha224(b"abc")),
        "23097d223405d8228642a477bda255b32aadbce4bda0b3f7e36c9da7"
    );
    assert_eq!(
        hex(&sha224(b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq")),
        "75388b16512776cc5dba5da1fd890150b0c6455cb4f58b1952522525"
    );
    assert_eq!(
        hex(&sha224(&[b'a'; 1_000_000])),
        "20794655980c91d8bbb4c1ea97618a4bf03f42581948b2ee4ee7ad67"
    );
}

#[test]
fn sha384_nist_vectors() {
    assert_eq!(
        hex(&sha384(b"")),
        "38b060a751ac96384cd9327eb1b1e36a21fdb71114be07434c0cc7bf63f6e1da274edebfe76f65fbd51ad2f14898b95b"
    );
    assert_eq!(
        hex(&sha384(b"abc")),
        "cb00753f45a35e8bb5a03d699ac65007272c32ab0eded1631a8b605a43ff5bed8086072ba1e7cc2358baeca134c825a7"
    );
    assert_eq!(
        hex(&sha384(b"abcdefghbcdefghicdefghijdefghijkefghijklfghijklmghijklmnhijklmnoijklmnopjklmnopqklmnopqrlmnopqrsmnopqrstnopqrstu")),
        "09330c33f71147e83d192fc782cd1b4753111b173b3b05d22fa08086e3b0f712fcc7c71a557e2db966c3e9fa91746039"
    );
    assert_eq!(
        hex(&sha384(&[b'a'; 1_000_000])),
        "9d0e1809716474cb086e834e310a4a1ced149e9c00f248527972cec5704c2a5b07b8b3dc38ecc4ebae97ddd87f3d8985"
    );
}

#[test]
fn sha512_nist_vectors() {
    assert_eq!(
        hex(&sha512(b"")),
        "cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e"
    );
    assert_eq!(
        hex(&sha512(b"abc")),
        "ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f"
    );
    assert_eq!(
        hex(&sha512(b"abcdefghbcdefghicdefghijdefghijkefghijklfghijklmghijklmnhijklmnoijklmnopjklmnopqklmnopqrlmnopqrsmnopqrstnopqrstu")),
        "8e959b75dae313da8cf4f72814fc143f8f7779c6eb9f7fa17299aeadb6889018501d289e4900f7e4331b99dec4b5433ac7d329eeb6dd26545e96e55b874be909"
    );
    assert_eq!(
        hex(&sha512(&[b'a'; 1_000_000])),
        "e718483d0ce769644e2e42c7bc15b4638e1f98b13b2044285632a803afa973ebde0ff244877ea60a4cb0432ce577c31beb009c5c2c49aa2e4eadb217ad8cc09b"
    );
}

// Padding boundary lengths: 55/56 straddle the SHA-256 short block, 111/112
// straddle SHA-512's; +1 block variants hit the two-transform finalization.
#[test]
fn padding_boundaries_match_single_shot() {
    for len in [55, 56, 57, 63, 64, 65, 111, 112, 113, 119, 120, 127, 128, 129, 256] {
        let data = vec![0xa5u8; len];
        for split in [0, 1, len / 2, len.saturating_sub(1)] {
            let mut c256 = PgSha256Ctx::init_sha256();
            c256.update(&data[..split]);
            c256.update(&data[split..]);
            assert_eq!(c256.final_sha256(), sha256(&data), "sha256 len={len} split={split}");

            let mut c512 = PgSha512Ctx::init_sha512();
            c512.update(&data[..split]);
            c512.update(&data[split..]);
            assert_eq!(c512.final_sha512(), sha512(&data), "sha512 len={len} split={split}");
        }
    }
}

#[test]
fn incremental_byte_at_a_time() {
    let data: Vec<u8> = (0..200u8).collect();
    let mut ctx = PgSha256Ctx::init_sha256();
    for b in &data {
        ctx.update(core::slice::from_ref(b));
    }
    assert_eq!(ctx.final_sha256(), sha256(&data));

    let mut ctx = PgSha512Ctx::init_sha384();
    for b in &data {
        ctx.update(core::slice::from_ref(b));
    }
    assert_eq!(ctx.final_sha384(), sha384(&data));
}
