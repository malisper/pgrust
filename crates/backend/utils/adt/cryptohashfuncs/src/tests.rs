//! Known-answer tests for the cryptohashfuncs cores against standard vectors.

use super::*;

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

#[test]
fn md5_known() {
    // md5("abc") = 900150983cd24fb0d6963f7d28e17f72
    assert_eq!(
        md5_text(b"abc").unwrap(),
        b"900150983cd24fb0d6963f7d28e17f72".to_vec()
    );
    assert_eq!(
        md5_bytea(b"").unwrap(),
        b"d41d8cd98f00b204e9800998ecf8427e".to_vec()
    );
}

#[test]
fn sha_known() {
    // NIST/standard vectors for "abc".
    assert_eq!(
        hex(&sha224_bytea(b"abc")),
        "23097d223405d8228642a477bda255b32aadbce4bda0b3f7e36c9da7"
    );
    assert_eq!(
        hex(&sha256_bytea(b"abc")),
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );
    assert_eq!(
        hex(&sha384_bytea(b"abc")),
        "cb00753f45a35e8bb5a03d699ac65007272c32ab0eded1631a8b605a43ff5bed8086072ba1e7cc2358baeca134c825a7"
    );
    assert_eq!(
        hex(&sha512_bytea(b"abc")),
        "ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f"
    );
}

#[test]
fn sha_empty() {
    assert_eq!(
        hex(&sha256_bytea(b"")),
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    );
}
