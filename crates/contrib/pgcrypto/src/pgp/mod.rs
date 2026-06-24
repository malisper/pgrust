//! The OpenPGP subsystem (pgp-*.c) — ASCII armor, symmetric encrypt/decrypt,
//! compression, and key-id extraction. Public-key (RSA/ElGamal) encrypt/decrypt
//! is not ported (graceful ERROR at the lib.rs dispatch); `pgp_key_id` works for
//! both symmetric and public-key inputs because it only parses packets.

pub mod armor;
pub mod cfb;
pub mod compress;
pub mod consts;
pub mod context;
pub mod decrypt;
pub mod encrypt;
pub mod keyid;
pub mod packet;
pub mod s2k;

use context::PgpContext;

/// Outcome of a decrypt that may also emit NOTICE lines (the `expect-*` checks).
pub struct DecryptOutput {
    pub plaintext: Vec<u8>,
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
) -> Result<DecryptOutput, String> {
    let mut ctx = PgpContext::default();
    if let Some(a) = args {
        ctx.parse_args(a)?;
    }
    ctx.text_mode = if need_text { 1 } else { 0 };

    let exp = ctx.clone();
    let plaintext = decrypt::decrypt_symmetric(&mut ctx, data, key)?;
    let notices = if exp.expect {
        build_expect_notices(&exp, &ctx)
    } else {
        Vec::new()
    };
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

    #[test]
    fn decrypt_known_zip_message() {
        // From pgp-compression.sql: a real gpg-produced ZIP message, key 'key'.
        let armored = "\n-----BEGIN PGP MESSAGE-----\n\nww0ECQMCsci6AdHnELlh0kQB4jFcVwHMJg0Bulop7m3Mi36s15TAhBo0AnzIrRFrdLVCkKohsS6+\nDMcmR53SXfLoDJOv/M8uKj3QSq7oWNIp95pxfA==\n=tbSn\n-----END PGP MESSAGE-----\n";
        let bin = armor::armor_decode(armored.as_bytes()).expect("dearmor");
        let out = sym_decrypt(&bin, b"key", Some(b"expect-compress-algo=1"), true).expect("decrypt");
        assert_eq!(out.plaintext, b"Secret message");
    }
}
