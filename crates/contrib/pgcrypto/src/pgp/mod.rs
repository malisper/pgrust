
pub mod armor;
pub mod cfb;
pub mod compress;
pub mod consts;
pub mod context;
pub mod decrypt;
pub mod encrypt;
pub mod keyid;
pub mod mpi;
pub mod packet;
pub mod pubdec;
pub mod pubenc;
pub mod pubkey;
pub mod s2k;

use context::PgpContext;

#[derive(Debug)]
pub struct DecryptOutput {
    pub plaintext: Vec<u8>,
    pub notices: Vec<String>,
    /// pgp-pgsql.c:517 got_unicode: the literal packet was type 'u'.
    pub unicode: bool,
}

#[derive(Debug)]
pub struct DecryptError {
    pub message: String,
    pub notices: Vec<String>,
}

pub fn sym_encrypt(
    data: &[u8],
    key: &[u8],
    args: Option<&[u8]>,
    is_text: bool,
) -> Result<Vec<u8>, String> {
    let mut ctx = PgpContext::default();
    if let Some(a) = args {
        ctx.parse_args(a)?;
    }
    ctx.text_mode = if is_text { 1 } else { 0 };
    set_symkey(key)?;
    encrypt::encrypt_symmetric(&ctx, data, key)
}

/// pgp.c:362 pgp_set_symkey: `key == NULL || len < 1` is PXE_ARGUMENT_ERROR
/// (raised after the args are parsed, on both wrappers — pgp-pgsql.c:440/505).
fn set_symkey(key: &[u8]) -> Result<(), String> {
    if key.is_empty() {
        return Err(consts::ARGUMENT_ERROR.to_string());
    }
    Ok(())
}

/// pgp-pgsql.c:390 encrypt_internal reads unicode-mode from the parsed args
/// before the data is converted to UTF-8.
pub fn args_unicode_mode(args: Option<&[u8]>) -> Result<bool, String> {
    let mut ctx = PgpContext::default();
    if let Some(a) = args {
        ctx.parse_args(a)?;
    }
    Ok(ctx.unicode_mode != 0)
}

// pgp-pgsql.c:511 decrypt_internal: check_expect runs on the decrypted
// context whether or not pgp_decrypt failed; the NOTICEs precede the ERROR.
fn finish_decrypt(
    exp: &PgpContext,
    ctx: &PgpContext,
    result: Result<Vec<u8>, String>,
) -> Result<DecryptOutput, DecryptError> {
    let mut notices = ctx.debug_notices.clone();
    if exp.expect {
        notices.extend(build_expect_notices(exp, ctx));
    }
    match result {
        Ok(plaintext) => Ok(DecryptOutput {
            plaintext,
            notices,
            unicode: ctx.unicode_mode != 0,
        }),
        Err(message) => Err(DecryptError { message, notices }),
    }
}

pub fn sym_decrypt(
    data: &[u8],
    key: &[u8],
    args: Option<&[u8]>,
    need_text: bool,
) -> Result<DecryptOutput, DecryptError> {
    let mut ctx = PgpContext::default();
    if let Some(a) = args {
        ctx.parse_args(a)
            .map_err(|e| DecryptError { message: e, notices: Vec::new() })?;
    }
    ctx.text_mode = if need_text { 1 } else { 0 };
    set_symkey(key).map_err(|e| DecryptError { message: e, notices: Vec::new() })?;

    let exp = ctx.clone();
    let result = decrypt::decrypt_symmetric(&mut ctx, data, key);
    finish_decrypt(&exp, &ctx, result)
}

fn build_expect_notices(exp: &PgpContext, ctx: &PgpContext) -> Vec<String> {
    let mut out = Vec::new();
    let mut chk = |name: &str, e: i32, g: i32| {
        if e >= 0 && e != g {
            out.push(format!("pgp_decrypt: unexpected {name}: expected {e} got {g}"));
        }
    };
    chk("cipher_algo", exp.exp_cipher_algo, ctx.cipher_algo);
    chk("s2k_mode", exp.exp_s2k_mode, ctx.s2k_mode);
    chk("s2k_count", exp.exp_s2k_count, ctx.s2k_count);
    chk("s2k_digest_algo", exp.exp_s2k_digest_algo, ctx.s2k_digest_algo);
    chk("use_sess_key", exp.exp_use_sess_key, ctx.use_sess_key);
    if ctx.use_sess_key != 0 {
        chk("s2k_cipher_algo", exp.exp_s2k_cipher_algo, ctx.s2k_cipher_algo);
    }
    chk("disable_mdc", exp.exp_disable_mdc, ctx.disable_mdc);
    chk("compress_algo", exp.exp_compress_algo, ctx.compress_algo);
    chk("unicode_mode", exp.exp_unicode_mode, ctx.unicode_mode);
    out
}

pub fn pub_encrypt(
    data: &[u8],
    key: &[u8],
    args: Option<&[u8]>,
    is_text: bool,
) -> Result<Vec<u8>, String> {
    let mut ctx = PgpContext::default();
    if let Some(a) = args {
        ctx.parse_args(a)?;
    }
    ctx.text_mode = if is_text { 1 } else { 0 };

    let pk = pubkey::read_key(key, None, 0)?;

    let klen = consts::cipher_key_size(ctx.cipher_algo);
    let mut sess_key = vec![0u8; klen];
    if !consts::fill_random(&mut sess_key) {
        return Err(consts::NO_RANDOM.to_string());
    }

    let mut out = Vec::new();
    pubenc::write_pubenc_sesskey(&mut out, &pk, ctx.cipher_algo, &sess_key)?;
    encrypt::write_encdata_packet(&ctx, data, &sess_key, &mut out)?;
    Ok(out)
}

pub fn pub_decrypt(
    data: &[u8],
    key: &[u8],
    psw: Option<&[u8]>,
    args: Option<&[u8]>,
    need_text: bool,
) -> Result<DecryptOutput, DecryptError> {
    let mut ctx = PgpContext::default();
    if let Some(a) = args {
        ctx.parse_args(a)
            .map_err(|e| DecryptError { message: e, notices: Vec::new() })?;
    }
    ctx.text_mode = if need_text { 1 } else { 0 };

    let pk = pubkey::read_key(key, psw, 1)
        .map_err(|e| DecryptError { message: e, notices: Vec::new() })?;

    let exp = ctx.clone();
    let result = decrypt::decrypt_pubkey(&mut ctx, data, &mut |ctx, body| {
        let (cipher, key) = pubdec::parse_pubenc_sesskey(ctx, &pk, body)?;
        Ok(decrypt::SessKey { cipher, key })
    });
    finish_decrypt(&exp, &ctx, result)
}

pub fn key_id(data: &[u8]) -> Result<String, String> {
    keyid::pgp_get_keyid(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn armor_known_vectors() {
        let a = armor::armor_encode(b"", &[], &[]);
        let s = String::from_utf8(a).unwrap();
        assert!(s.contains("=twTO"), "got: {s}");
        let a = armor::armor_encode(b"test", &[], &[]);
        let s = String::from_utf8(a).unwrap();
        assert!(s.contains("dGVzdA=="), "got: {s}");
        assert!(s.contains("=+G7Q"), "got: {s}");
    }

    #[test]
    fn dearmor_roundtrip() {
        let a = armor::armor_encode(b"zooka", &[], &[]);
        let d = armor::armor_decode(&a).unwrap();
        assert_eq!(d, b"zooka");
    }

    #[test]
    fn armor_headers() {
        let a = armor::armor_encode(b"zooka", &[b"foo".to_vec()], &[b"bar".to_vec()]);
        let h = armor::extract_armor_headers(&a).unwrap();
        assert_eq!(h.len(), 1);
        assert_eq!(h[0].0, b"foo");
        assert_eq!(h[0].1, b"bar");
    }

    fn roundtrip(data: &[u8], args: Option<&[u8]>, is_text: bool) {
        let ct = sym_encrypt(data, b"key", args, is_text).expect("encrypt");
        let out = sym_decrypt(&ct, b"key", None, is_text).expect("decrypt");
        assert_eq!(out.plaintext, data, "roundtrip mismatch args={args:?}");
    }

    #[test]
    fn sym_roundtrip_default() {
        roundtrip(b"Secret.", None, true);
    }

    #[test]
    fn sym_roundtrip_bf() {
        roundtrip(b"Secret.", Some(b"cipher-algo=bf"), true);
    }

    #[test]
    fn sym_roundtrip_aes192() {
        roundtrip(b"Secret.", Some(b"cipher-algo=aes192"), true);
    }

    #[test]
    fn sym_roundtrip_sesskey() {
        roundtrip(b"Secret.", Some(b"sess-key=1"), true);
        roundtrip(b"Secret.", Some(b"sess-key=1, cipher-algo=aes256"), true);
    }

    #[test]
    fn sym_roundtrip_s2k_modes() {
        roundtrip(b"Secret.", Some(b"s2k-mode=0"), true);
        roundtrip(b"Secret.", Some(b"s2k-mode=1"), true);
        roundtrip(b"Secret.", Some(b"s2k-mode=3"), true);
    }

    #[test]
    fn sym_roundtrip_nomdc() {
        roundtrip(b"Secret.", Some(b"disable-mdc=1"), true);
    }

    #[test]
    fn sym_roundtrip_compress() {
        roundtrip(b"Secret message", Some(b"compress-algo=1"), true);
        roundtrip(b"Secret message", Some(b"compress-algo=2"), true);
    }

    #[test]
    fn sym_roundtrip_compress_large() {
        let data: Vec<u8> = (0..16366u32).map(|i| (i % 251) as u8).collect();
        roundtrip(&data, Some(b"compress-algo=1,compress-level=1"), false);
    }

    const RSA_SECKEY: &str = "\n-----BEGIN PGP PRIVATE KEY BLOCK-----\nVersion: GnuPG v1.4.1 (GNU/Linux)\n\n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\n=UKh3\n-----END PGP PRIVATE KEY BLOCK-----\n";
    const RSA_MSG: &str = "\n-----BEGIN PGP MESSAGE-----\nVersion: GnuPG v1.4.1 (GNU/Linux)\n\nhQEMA/0CBsQJt0h1AQf+JyYnCiortj26P11zk28MKOGfWpWyAhuIgwbJXsdQ+e6r\npEyyqs9GC6gI7SNF6+J8B/gsMwvkAL4FHAQCvA4ZZ6eeXR1Of4YG22JQGmpWVWZg\nDTyfhA2vkczuqfAD2tgUpMT6sdyGkQ/fnQ0lknlfHgC5GRx7aavOoAKtMqiZW5PR\nyae/qR48mjX7Mb+mLvbagv9mHEgQSmHwFpaq2k456BbcZ23bvCmBnCvqV/90Ggfb\nVP6gkSoFVsJ19RHsOhW1dk9ehbl51WB3zUOO5FZWwUTY9DJvKblRK/frF0+CXjE4\nHfcZXHSpSjx4haGGTsMvEJ85qFjZpr0eTGOdY5cFhNJAAVP8MZfji7OhPRAoOOIK\neRGOCkao12pvPyFTFnPd5vqmyBbdNpK4Q0hS82ljugMJvM0p3vJZVzW402Kz6iBL\nGQ==\n=XHkF\n-----END PGP MESSAGE-----\n";

    #[test]
    fn pub_rsa_decrypt_fixed_ciphertext() {
        let seckey = armor::armor_decode(RSA_SECKEY.as_bytes()).expect("dearmor seckey");
        let msg = armor::armor_decode(RSA_MSG.as_bytes()).expect("dearmor msg");
        let out = pub_decrypt(&msg, &seckey, None, None, true).expect("pub decrypt");
        assert_eq!(out.plaintext, b"Secret message.");
    }

    const RSA_PUBKEY: &str = "\n-----BEGIN PGP PUBLIC KEY BLOCK-----\nVersion: GnuPG v1.4.1 (GNU/Linux)\n\nmQELBELr2m0BCADOrnknlnXI0EzRExf/TgoHvK7Xx/E0keWqV3KrOyC3/tY2KOrj\nUVxaAX5pkFX9wdQObGPIJm06u6D16CH6CildX/vxG7YgvvKzK8JGAbwrXAfk7OIW\nczO2zRaZGDynoK3mAxHRBReyTKtNv8rDQhuZs6AOozJNARdbyUO/yqUnqNNygWuT\n4htFDEuLPIJwAbMSD0BvFW6YQaPdxzaAZm3EWVNbwDzjgbBUdBiUUwRdZIFUhsjJ\ndirFdy5+uuZru6y6CNC1OERkJ7P8EyoFiZckAIE5gshVZzNuyLOZjc5DhWBvLbX4\nNZElAnfiv+4nA6y8wQLSIbmHA3nqJaBklj85AAYptCVSU0EgMjA0OCBFbmMgPHJz\nYTIwNDhlbmNAZXhhbXBsZS5vcmc+iQE0BBMBAgAeBQJC69ptAhsDBgsJCAcDAgMV\nAgMDFgIBAh4BAheAAAoJEMiZ6pNEGVVZHMkIAJtGHHZ9iM8Yq1rr0zl1L6SvlQP8\nJCaxHa31wH3PKqGtq2M+cpb2rXf7gAY/doHJPXggfVzkyFrysmQ1gPbDGYLyOutw\n+IkhihEb5bWxQBNj+3zAFs1YX6v2HXWbSUSmyY1V9/+NTtKk03olDc/swd3lXzku\nUOhcgfpBgIt3Q+MpT6M2+OIF7lVfSb1rWdpwTfGhZzW9szQOeoS4gPvxCCRyuabQ\nRJ6DWH61F8fFIDJg1z+A/Obx4fqX6GOA69RzgZ3oukFBIXxNwV9PZNnAmHtZVYO8\n0g/oVYBbuvOYedffDBeQarhERZ5W2TnIE+nqY61YOLBqosliygdZTXULzNi5AQsE\nQuvaugEIAOuCJZdkzORA6e1lr81Lnr4JzMsVBFA+X/yIkBbV6qX/A4nVSLAZKNPX\nz1YIrMTu+1rMIiy10IWbA6zgMTpzPhJRfgePONgdnCYyK5Ksh5/C5ntzKwwGwxfK\nlAXIxJurCHXTbEa+YvPdn76vJ3HsXOXVEL+fLb4U3l3Ng87YM202Lh1Ha2MeS2zE\nFZcAoKbFqAAjDLEai64SoOFh0W3CsD1DL4zmfp+YZrUPHTtZadsi53i4KKW/ws9U\nrHlolqYNhYze/uRLyfnUx9PN4r/GhEzauyDMV0smo91uB3aewPft+eCpmeWnu0PF\nJVK4xyRmhIq2rVCw16a1pBJirvGM+y0ABimJAR8EGAECAAkFAkLr2roCGwwACgkQ\nyJnqk0QZVVku1wgAg1bLSjPkhw+ldG5HzumpqR84+JKyozdJaJzefu2+1iqYE0B0\nWLz2PJVIiK41xiEkKhBvTOQYuXmtWqAWXptD91P5SoXoNJWLQO3TNwarANhHxkWg\nw/TOUxQqoctlRUej5NDD+4eW5G9lcS1FEGuKDWtX096u80vO+TbyJjvx2eVM1k+X\ndmeYsGOiNgDimCreJGYc14G7eY9jt24gw10n1sMAKI1qm6lcoHqZ9OOyla+wJdro\nPYZGO7R8+1O9R22WrK6BYDT5j/1JwMZqbOESjNvDEVT0yOHClCHRN4CChbt6LhKh\nCLUNdz/udIt0JAC6c/HdPLSW3HnmM3+iNj+Kug==\n=pwU2\n-----END PGP PUBLIC KEY BLOCK-----\n";

    #[test]
    fn pub_rsa_roundtrip() {
        let pubkey = armor::armor_decode(RSA_PUBKEY.as_bytes()).expect("dearmor pubkey");
        let seckey = armor::armor_decode(RSA_SECKEY.as_bytes()).expect("dearmor seckey");
        let ct = pub_encrypt(b"Secret msg", &pubkey, None, true).expect("pub encrypt");
        let out = pub_decrypt(&ct, &seckey, None, None, true).expect("pub decrypt");
        assert_eq!(out.plaintext, b"Secret msg");
    }

    // pgp-pgsql.c:514: expect-* NOTICEs are reported even when pgp_decrypt
    // failed (wrong key, or a stream that ends after the session key).
    #[test]
    fn expect_notices_survive_decrypt_errors() {
        let ct = sym_encrypt(b"x", b"key", None, true).expect("encrypt");
        let err = sym_decrypt(&ct, b"wrong", Some(b"expect-cipher-algo=aes256,expect-s2k-mode=1"), true)
            .unwrap_err();
        assert_eq!(err.message, "Wrong key or corrupt data");
        assert_eq!(
            err.notices,
            [
                "pgp_decrypt: unexpected cipher_algo: expected 9 got 7",
                "pgp_decrypt: unexpected s2k_mode: expected 1 got 3",
            ]
        );
        let ct = sym_encrypt(b"x", b"key", Some(b"s2k-mode=0"), true).expect("encrypt");
        let err = sym_decrypt(&ct[..6], b"key", Some(b"expect-cipher-algo=aes256"), true).unwrap_err();
        assert_eq!(err.message, "Wrong key or corrupt data");
        assert_eq!(err.notices, ["pgp_decrypt: unexpected cipher_algo: expected 9 got 7"]);
    }

    // pgp-pgsql.c:537: the SQL wrapper converts from UTF-8 only for a 'u'
    // literal, which unicode-mode=1 writes.
    #[test]
    fn unicode_literal_is_reported() {
        let ct = sym_encrypt(b"x", b"key", Some(b"unicode-mode=1"), true).expect("encrypt");
        assert!(sym_decrypt(&ct, b"key", None, true).expect("decrypt").unicode);
        let ct = sym_encrypt(b"x", b"key", None, true).expect("encrypt");
        assert!(!sym_decrypt(&ct, b"key", None, true).expect("decrypt").unicode);
        assert!(args_unicode_mode(Some(b"unicode-mode=1")).unwrap());
        assert!(!args_unicode_mode(None).unwrap());
    }

    // pgp-decrypt.c:388 mdc_finish / :144 pgp_parse_pkt_hdr: the MDC trailer
    // is walked as packets after the literal data parsed.
    #[test]
    fn mdc_trailer_debug_follows_the_packet_walk() {
        let ct = sym_encrypt(b"hello world", b"key", Some(b"s2k-mode=0"), true).expect("encrypt");
        let flip = |at: usize| {
            let mut c = ct.clone();
            c[at] ^= 0xff;
            sym_decrypt(&c, b"key", Some(b"debug=1"), true).unwrap_err()
        };
        let err = flip(ct.len() - 1);
        assert_eq!((err.message.as_str(), &err.notices[..]),
            ("Wrong key or corrupt data", &["dbg: mdc_finish: mdc failed".to_string()][..]));
        let err = flip(ct.len() - 22);
        assert_eq!(err.notices, ["dbg: pgp_parse_pkt_hdr: not pkt hdr"]);
        let mut c = ct.clone();
        c[ct.len() - 22] ^= 0x01;
        let err = sym_decrypt(&c, b"key", Some(b"debug=1"), true).unwrap_err();
        assert_eq!(err.notices, ["dbg: process_data_packets: unexpected pkt tag=18"]);
    }

    // pgp-pubdec.c:212 + openssl.c:541: a session key shorter than the AES
    // key size is zero-padded by the cipher init; pgp.c:163: an id outside
    // the cipher table is corrupt data; pgp-pubdec.c:196 + px.c:70: a bad
    // EME block is "Wrong key". Messages built against RSA_SECKEY's subkey
    // (the third against the primary key, so RSA yields garbage).
    const PUB_SHORT_KEY: &str = "c1c04c0300000000000000000107ff6aaf0020462daaf8ce0d7bb9845fa8dde4ab5bbac7aa79fae163a41b0c41eb43202faaff5a2726f96c89530c9e9c4a5e45f62472979b2e3c81f939ed5ff0f2cc734653dc3c989e089bc28ce425f0f8dee84efed0f11504014fda5e10548155b45ff248bb76b09be218a6cb8d0f48ae7c25327e8f124fbe8c3356f1a3f2ffb5a97436ef96481ab1b2132bde28455d69f3ec6a8020837b8f8c85576cadbf3bcf0fdaf7af9bcf37e3a5da023b71d9b35edb4fa378640cbbba38ea1eb68a6d6ad3eb9e0d95ae36e7a84090b9553628c5ae47e30b1262d1e4f6a819762527086bbc198f93e62d7f6f595b2fc1737475af8b5903760207d4e3348acfc5064390a8b4d4d23f012eff0656570042c4e3d6020b1379c7e71d30a537a7dfaec37312368c340dbdb8c8b4d9cc64a6df8983593552c3d677541b31e2ffc783b7a77c8a7d49c5ee";
    const PUB_UNKNOWN_CIPHER: &str = "c1c04c0300000000000000000107fd14f0e30f5463e8dca10f82abed8cf0cd4fe4a389ddbd6e36bae8e23b86376016f1d808671ce21c81b406811cd6ea23c7264aa9dcfd46dc06c520294c64e01e30f3b68ebc8dbadbd6ef101069bd72414a9e003f21d8c33816dda11178249423973a22e0b25af443fb73e4d8a5c38537009e069bffe6fc8cf61e40e6ba2169f34966e3e8aff831e10dbc4b15eeb11e98ebb4c31b051af1296244dad220cdadcba84440dc14bbd1f3625d2cb40ef89b2ef67341bf081164eea8bbec37b2afdea81b18fec183deb0290f6e96f42744c856a33b03cb389122a76aa735f1c1c39bbb689fad1f61129ea1c840fba5393350b07a113906acb42f7f95674c5f5bfb867037d23f0131ca0b3f3fc13b5c46dc5429fc020b3dac226331d4ce8b9ca15a964f804feddb2332dd8d85da4cc74203c82461d995c84c6abe82c1da09140dc5d8696030";
    const PUB_WRONG_KEY: &str = "c1c04c030000000000000000010800b5ec3f8251c8604106669724207eabdae25b849c45da7bd9675a1f3ebc4bc48e4684414b730bff8215501d67de2eb3c491279ced14f138b102215ca61e65495ae2ae3e02d1c63676d894af9e4b961f44d2657c6a0212bd2505a887733ffff086efe6122502641c817daff9c70fd3a1098141db6d6644b618a8ba0bb15904065a25db5c86e2ccf0e15b46df9652b2e0d6f9173a237a89cceb9fd77436897d6ff98cdcb49ed7fac3f1f3a836edd26dfd29d68f3a8afaa1acddf5c87cc05ed8a4adabd1d669a36d93eac2e08331f919e1de05698525381b580c4241223c7b1872a4ede5bc6170ffb732969d509cd7d1afacdc1dea7b844f1bb8b1efe8ba99f557f3d23a01ef1af3bf93c7b2d69442a7abd64857d0f412fde07955a0e69bdcd42c84c8b30ac6cad5c19332abb5641f6b4bbbc63f701f03a475684c4e2c78";

    fn unhex(s: &str) -> Vec<u8> {
        (0..s.len()).step_by(2).map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap()).collect()
    }

    #[test]
    fn pubenc_session_key_follows_c() {
        let seckey = armor::armor_decode(RSA_SECKEY.as_bytes()).expect("dearmor seckey");
        let out = pub_decrypt(&unhex(PUB_SHORT_KEY), &seckey, None, None, false).expect("short key");
        assert_eq!(out.plaintext, b"short key text");
        let err = pub_decrypt(&unhex(PUB_UNKNOWN_CIPHER), &seckey, None, Some(b"debug=1"), false)
            .unwrap_err();
        assert_eq!((err.message.as_str(), err.notices.len()), ("Wrong key or corrupt data", 0));
        let err = pub_decrypt(&unhex(PUB_WRONG_KEY), &seckey, None, Some(b"debug=1"), false)
            .unwrap_err();
        assert_eq!(err.message, "Wrong key");
        assert_eq!(err.notices, ["dbg: check_eme_pkcs1_v15 failed"]);
    }

    #[test]
    fn pub_encrypt_refuses_secret_key() {
        let seckey = armor::armor_decode(RSA_SECKEY.as_bytes()).expect("dearmor seckey");
        let err = pub_encrypt(b"Secret msg", &seckey, None, true).unwrap_err();
        assert_eq!(err, "Refusing to encrypt with secret key");
    }

    #[test]
    fn decrypt_known_zip_message() {
        let armored = "\n-----BEGIN PGP MESSAGE-----\n\nww0ECQMCsci6AdHnELlh0kQB4jFcVwHMJg0Bulop7m3Mi36s15TAhBo0AnzIrRFrdLVCkKohsS6+\nDMcmR53SXfLoDJOv/M8uKj3QSq7oWNIp95pxfA==\n=tbSn\n-----END PGP MESSAGE-----\n";
        let bin = armor::armor_decode(armored.as_bytes()).expect("dearmor");
        let out = sym_decrypt(&bin, b"key", Some(b"expect-compress-algo=1"), true).expect("decrypt");
        assert_eq!(out.plaintext, b"Secret message");
    }

    /// upstream 4c5128ca0b30 (18.6): the pgp-decrypt corpus message that an
    /// OpenSSL lacking Blowfish "encrypted" with cipher-algo=bf. With the
    /// cipher available both decryptions fail alike; ignore-cipher-failure
    /// only has to be accepted, and must not weaken a real decryption.
    #[test]
    fn ignore_cipher_failure_is_accepted_without_weakening_decryption() {
        let armored = "\n-----BEGIN PGP MESSAGE-----\n\nww0EBAMC8wIKbtvzJtxi0jABUleCwFJWGCkYKcsNdABqdtXaU2VjcmV0LtMUlnPH3A2QBmZrcucm\n1GPb/s2Bkdg=\n=6aqD\n-----END PGP MESSAGE-----\n";
        let bin = armor::armor_decode(armored.as_bytes()).expect("dearmor");
        let err = |r: Result<DecryptOutput, DecryptError>| r.err().expect("must fail").message;
        assert_eq!(err(sym_decrypt(&bin, b"wrong key", None, true)), "Wrong key or corrupt data");
        assert_eq!(
            err(sym_decrypt(&bin, b"wrong key", Some(b"ignore-cipher-failure=1"), true)),
            "Wrong key or corrupt data"
        );

        let ct = sym_encrypt(b"Secret.", b"key", Some(b"cipher-algo=bf, ignore-cipher-failure=1"), true)
            .expect("encrypt accepts the option");
        let out = sym_decrypt(&ct, b"key", Some(b"ignore-cipher-failure=1"), true).expect("decrypt");
        assert_eq!(out.plaintext, b"Secret.");
        assert_eq!(
            err(sym_decrypt(&ct, b"nope", Some(b"ignore-cipher-failure=1"), true)),
            "Wrong key or corrupt data"
        );

        let seckey = armor::armor_decode(RSA_SECKEY.as_bytes()).expect("dearmor seckey");
        let msg = armor::armor_decode(RSA_MSG.as_bytes()).expect("dearmor msg");
        let out = pub_decrypt(&msg, &seckey, None, Some(b"ignore-cipher-failure=1"), true)
            .expect("pub decrypt");
        assert_eq!(out.plaintext, b"Secret message.");
    }
}
