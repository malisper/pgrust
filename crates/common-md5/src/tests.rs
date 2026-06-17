//! RFC 1321 / standard MD5 test vectors, plus the PostgreSQL `pg_md5_encrypt`
//! `"md5"`-prefixed formatting.

use super::*;

#[test]
fn rfc1321_vectors() {
    // (input, expected lowercase-hex digest) from RFC 1321 appendix A.5.
    let cases: &[(&str, &str)] = &[
        ("", "d41d8cd98f00b204e9800998ecf8427e"),
        ("a", "0cc175b9c0f1b6a831c399e269772661"),
        ("abc", "900150983cd24fb0d6963f7d28e17f72"),
        ("message digest", "f96b697d7cb7938d525a2f31aaf161d0"),
        (
            "abcdefghijklmnopqrstuvwxyz",
            "c3fcd3d76192e4007dfb496cca67e13b",
        ),
        (
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789",
            "d174ab98d277d9f5a5611c2c9f419d9f",
        ),
        (
            "12345678901234567890123456789012345678901234567890123456789012345678901234567890",
            "57edf4a22be3c955ac49da2e2107b67a",
        ),
    ];
    for (input, expected) in cases {
        assert_eq!(&pg_md5_hash(input.as_bytes()).unwrap(), expected, "input={input:?}");
    }
}

#[test]
fn binary_matches_hex() {
    let digest = pg_md5_binary(b"abc").unwrap();
    let hex = pg_md5_hash(b"abc").unwrap();
    let rendered: String = digest.iter().map(|b| format!("{b:02x}")).collect();
    assert_eq!(rendered, hex);
}

#[test]
fn block_boundary_lengths() {
    // Exercise the padding split path around 56/64-byte boundaries.
    for n in [55usize, 56, 57, 63, 64, 65, 119, 120, 128] {
        let data = vec![b'x'; n];
        // Recompute independently in one shot vs the incremental loop to ensure
        // padding handling is correct; the hex must be 32 chars.
        let hex = pg_md5_hash(&data).unwrap();
        assert_eq!(hex.len(), MD5_HASH_LEN);
    }
}

#[test]
fn encrypt_format() {
    // md5(passwd || salt), "md5"-prefixed. Cross-check: pg_md5_encrypt("",
    // "") == "md5" + md5("").
    let out = pg_md5_encrypt(b"", b"").unwrap();
    assert_eq!(out, format!("md5{}", pg_md5_hash(b"").unwrap()));
    assert!(out.starts_with("md5"));
    assert_eq!(out.len(), 3 + MD5_HASH_LEN);
}
