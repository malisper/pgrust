//! The OpenPGP subsystem (pgp-*.c) — ASCII armor, symmetric encrypt/decrypt,
//! compression, key-id extraction, and public-key (RSA/ElGamal) encrypt/decrypt.
//! The public-key path (`pubkey`/`pubenc`/`pubdec`/`mpi`) wraps/unwraps the
//! session key per the recipient key and reuses the symmetric data pipeline.

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

/// Outcome of a decrypt that may also emit NOTICE lines (the `expect-*` checks).
pub struct DecryptOutput {
    pub plaintext: Vec<u8>,
    pub notices: Vec<String>,
}

/// A decrypt failure carrying the `dbg:` NOTICE trace to emit before the error.
#[derive(Debug)]
pub struct DecryptError {
    pub message: String,
    pub notices: Vec<String>,
}

/// `encrypt_internal` (symmetric path). `is_text` selects literal-data type
/// 'b'/'t' and (with unicode_mode) 'u'. `args` is the option string (or None).
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
    encrypt::encrypt_symmetric(&ctx, data, key)
}

/// `decrypt_internal` (symmetric path). Returns plaintext + the NOTICE lines the
/// `expect-*` debug options would emit.
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

    let exp = ctx.clone();
    let result = decrypt::decrypt_symmetric(&mut ctx, data, key);

    // The `dbg:` NOTICE trace is emitted even when the decrypt then errors.
    let debug_notices = ctx.debug_notices.clone();

    let plaintext = match result {
        Ok(p) => p,
        Err(message) => {
            return Err(DecryptError {
                message,
                notices: debug_notices,
            })
        }
    };

    let mut notices = debug_notices;
    if exp.expect {
        notices.extend(build_expect_notices(&exp, &ctx));
    }
    Ok(DecryptOutput { plaintext, notices })
}

/// `check_expect` — emit `pgp_decrypt: unexpected <field>: expected E got G`.
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

/// `encrypt_internal` (public-key path). Generates a random session key, wraps
/// it for the recipient public key (tag-1 packet), and emits the
/// symmetrically-encrypted data packet. `is_text` selects the literal type.
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

    // pubtype=0: expect a public key (refuse a secret key).
    let pk = pubkey::read_key(key, None, 0)?;

    // init_sess_key: a fresh random session key of the cipher's key size.
    let klen = consts::cipher_key_size(ctx.cipher_algo);
    let mut sess_key = vec![0u8; klen];
    if !::pg_strong_random::pg_strong_random(&mut sess_key) {
        return Err("Failed to generate strong random bits".to_string());
    }

    let mut out = Vec::new();
    pubenc::write_pubenc_sesskey(&mut out, &pk, ctx.cipher_algo, &sess_key)?;
    encrypt::write_encdata_packet(&ctx, data, &sess_key, &mut out)?;
    Ok(out)
}

/// `decrypt_internal` (public-key path). Recovers the session key from the
/// tag-1 packet using the recipient secret key (`psw` decrypts a protected
/// secret key), then runs the symmetric data pipeline. Returns plaintext + the
/// NOTICE lines (for parity with the symmetric debug/expect output).
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

    // pubtype=1: expect a secret key (S2K-decrypt protected secret MPIs).
    let pk = pubkey::read_key(key, psw, 1)
        .map_err(|e| DecryptError { message: e, notices: Vec::new() })?;

    let exp = ctx.clone();
    let result = decrypt::decrypt_pubkey(&mut ctx, data, &mut |body| {
        let (cipher, key) = pubdec::parse_pubenc_sesskey(&pk, body)?;
        Ok(decrypt::SessKey { cipher, key })
    });

    let debug_notices = ctx.debug_notices.clone();
    let plaintext = match result {
        Ok(p) => p,
        Err(message) => {
            return Err(DecryptError {
                message,
                notices: debug_notices,
            })
        }
    };

    let mut notices = debug_notices;
    if exp.expect {
        notices.extend(build_expect_notices(&exp, &ctx));
    }
    Ok(DecryptOutput { plaintext, notices })
}

/// `pgp_get_keyid` over dearmored binary.
pub fn key_id(data: &[u8]) -> Result<String, &'static str> {
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

    // rsaenc2048 secret key (keytbl id 6) from sql/pgp-pubkey-decrypt.sql.
    const RSA_SECKEY: &str = "\n-----BEGIN PGP PRIVATE KEY BLOCK-----\nVersion: GnuPG v1.4.1 (GNU/Linux)\n\n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\n=UKh3\n-----END PGP PRIVATE KEY BLOCK-----\n";
    // encdata id 4: rsaenc2048 / aes128 from gnupg.
    const RSA_MSG: &str = "\n-----BEGIN PGP MESSAGE-----\nVersion: GnuPG v1.4.1 (GNU/Linux)\n\nhQEMA/0CBsQJt0h1AQf+JyYnCiortj26P11zk28MKOGfWpWyAhuIgwbJXsdQ+e6r\npEyyqs9GC6gI7SNF6+J8B/gsMwvkAL4FHAQCvA4ZZ6eeXR1Of4YG22JQGmpWVWZg\nDTyfhA2vkczuqfAD2tgUpMT6sdyGkQ/fnQ0lknlfHgC5GRx7aavOoAKtMqiZW5PR\nyae/qR48mjX7Mb+mLvbagv9mHEgQSmHwFpaq2k456BbcZ23bvCmBnCvqV/90Ggfb\nVP6gkSoFVsJ19RHsOhW1dk9ehbl51WB3zUOO5FZWwUTY9DJvKblRK/frF0+CXjE4\nHfcZXHSpSjx4haGGTsMvEJ85qFjZpr0eTGOdY5cFhNJAAVP8MZfji7OhPRAoOOIK\neRGOCkao12pvPyFTFnPd5vqmyBbdNpK4Q0hS82ljugMJvM0p3vJZVzW402Kz6iBL\nGQ==\n=XHkF\n-----END PGP MESSAGE-----\n";

    #[test]
    fn pub_rsa_decrypt_fixed_ciphertext() {
        let seckey = armor::armor_decode(RSA_SECKEY.as_bytes()).expect("dearmor seckey");
        let msg = armor::armor_decode(RSA_MSG.as_bytes()).expect("dearmor msg");
        let out = pub_decrypt(&msg, &seckey, None, None, true).expect("pub decrypt");
        assert_eq!(out.plaintext, b"Secret message.");
    }

    // rsaenc2048 public key (keytbl id 6) from sql/pgp-pubkey-decrypt.sql.
    const RSA_PUBKEY: &str = "\n-----BEGIN PGP PUBLIC KEY BLOCK-----\nVersion: GnuPG v1.4.1 (GNU/Linux)\n\nmQELBELr2m0BCADOrnknlnXI0EzRExf/TgoHvK7Xx/E0keWqV3KrOyC3/tY2KOrj\nUVxaAX5pkFX9wdQObGPIJm06u6D16CH6CildX/vxG7YgvvKzK8JGAbwrXAfk7OIW\nczO2zRaZGDynoK3mAxHRBReyTKtNv8rDQhuZs6AOozJNARdbyUO/yqUnqNNygWuT\n4htFDEuLPIJwAbMSD0BvFW6YQaPdxzaAZm3EWVNbwDzjgbBUdBiUUwRdZIFUhsjJ\ndirFdy5+uuZru6y6CNC1OERkJ7P8EyoFiZckAIE5gshVZzNuyLOZjc5DhWBvLbX4\nNZElAnfiv+4nA6y8wQLSIbmHA3nqJaBklj85AAYptCVSU0EgMjA0OCBFbmMgPHJz\nYTIwNDhlbmNAZXhhbXBsZS5vcmc+iQE0BBMBAgAeBQJC69ptAhsDBgsJCAcDAgMV\nAgMDFgIBAh4BAheAAAoJEMiZ6pNEGVVZHMkIAJtGHHZ9iM8Yq1rr0zl1L6SvlQP8\nJCaxHa31wH3PKqGtq2M+cpb2rXf7gAY/doHJPXggfVzkyFrysmQ1gPbDGYLyOutw\n+IkhihEb5bWxQBNj+3zAFs1YX6v2HXWbSUSmyY1V9/+NTtKk03olDc/swd3lXzku\nUOhcgfpBgIt3Q+MpT6M2+OIF7lVfSb1rWdpwTfGhZzW9szQOeoS4gPvxCCRyuabQ\nRJ6DWH61F8fFIDJg1z+A/Obx4fqX6GOA69RzgZ3oukFBIXxNwV9PZNnAmHtZVYO8\n0g/oVYBbuvOYedffDBeQarhERZ5W2TnIE+nqY61YOLBqosliygdZTXULzNi5AQsE\nQuvaugEIAOuCJZdkzORA6e1lr81Lnr4JzMsVBFA+X/yIkBbV6qX/A4nVSLAZKNPX\nz1YIrMTu+1rMIiy10IWbA6zgMTpzPhJRfgePONgdnCYyK5Ksh5/C5ntzKwwGwxfK\nlAXIxJurCHXTbEa+YvPdn76vJ3HsXOXVEL+fLb4U3l3Ng87YM202Lh1Ha2MeS2zE\nFZcAoKbFqAAjDLEai64SoOFh0W3CsD1DL4zmfp+YZrUPHTtZadsi53i4KKW/ws9U\nrHlolqYNhYze/uRLyfnUx9PN4r/GhEzauyDMV0smo91uB3aewPft+eCpmeWnu0PF\nJVK4xyRmhIq2rVCw16a1pBJirvGM+y0ABimJAR8EGAECAAkFAkLr2roCGwwACgkQ\nyJnqk0QZVVku1wgAg1bLSjPkhw+ldG5HzumpqR84+JKyozdJaJzefu2+1iqYE0B0\nWLz2PJVIiK41xiEkKhBvTOQYuXmtWqAWXptD91P5SoXoNJWLQO3TNwarANhHxkWg\nw/TOUxQqoctlRUej5NDD+4eW5G9lcS1FEGuKDWtX096u80vO+TbyJjvx2eVM1k+X\ndmeYsGOiNgDimCreJGYc14G7eY9jt24gw10n1sMAKI1qm6lcoHqZ9OOyla+wJdro\nPYZGO7R8+1O9R22WrK6BYDT5j/1JwMZqbOESjNvDEVT0yOHClCHRN4CChbt6LhKh\nCLUNdz/udIt0JAC6c/HdPLSW3HnmM3+iNj+Kug==\n=pwU2\n-----END PGP PUBLIC KEY BLOCK-----\n";

    #[test]
    fn pub_rsa_roundtrip() {
        let pubkey = armor::armor_decode(RSA_PUBKEY.as_bytes()).expect("dearmor pubkey");
        let seckey = armor::armor_decode(RSA_SECKEY.as_bytes()).expect("dearmor seckey");
        let ct = pub_encrypt(b"Secret msg", &pubkey, None, true).expect("pub encrypt");
        let out = pub_decrypt(&ct, &seckey, None, None, true).expect("pub decrypt");
        assert_eq!(out.plaintext, b"Secret msg");
    }

    #[test]
    fn pub_encrypt_refuses_secret_key() {
        let seckey = armor::armor_decode(RSA_SECKEY.as_bytes()).expect("dearmor seckey");
        let err = pub_encrypt(b"Secret msg", &seckey, None, true).unwrap_err();
        assert_eq!(err, "Refusing to encrypt with secret key");
    }

    #[test]
    fn decrypt_known_zip_message() {
        // From pgp-compression.sql: a real gpg-produced ZIP message, key 'key'.
        let armored = "\n-----BEGIN PGP MESSAGE-----\n\nww0ECQMCsci6AdHnELlh0kQB4jFcVwHMJg0Bulop7m3Mi36s15TAhBo0AnzIrRFrdLVCkKohsS6+\nDMcmR53SXfLoDJOv/M8uKj3QSq7oWNIp95pxfA==\n=tbSn\n-----END PGP MESSAGE-----\n";
        let bin = armor::armor_decode(armored.as_bytes()).expect("dearmor");
        let out = sym_decrypt(&bin, b"key", Some(b"expect-compress-algo=1"), true).expect("decrypt");
        assert_eq!(out.plaintext, b"Secret message");
    }
}
