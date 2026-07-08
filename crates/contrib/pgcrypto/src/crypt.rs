//! crypt() / gen_salt() password hashing. This increment ports md5-crypt
//! (`$1$`, crypt-md5.c) over the in-repo pg_md5, byte-identical to C's
//! non-OpenSSL build. des/bcrypt/xdes crypt (crypt-des.c/crypt-blowfish.c) and
//! sha-crypt (`$5$`/`$6$`, crypt-sha.c) are not yet ported — crypt() on those
//! salt prefixes and gen_salt() on those schemes raise feature-not-supported.

use pg_md5::Md5;
use pg_strong_random::pg_strong_random;

const MD5_SIZE: usize = 16;
const ITOA64: &[u8; 64] = b"./0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

pub enum CryptError {
    Unsupported(&'static str),
    Message(String),
}

fn random_salt_chars(n: usize) -> Result<Vec<u8>, CryptError> {
    let mut raw = vec![0u8; n];
    if !pg_strong_random(&mut raw) {
        return Err(CryptError::Message("Failed to generate random number".to_string()));
    }
    Ok(raw.iter().map(|&b| ITOA64[(b & 0x3f) as usize]).collect())
}

pub fn gen_salt(salt_type: &str, _rounds: i32) -> Result<String, CryptError> {
    match salt_type.to_ascii_lowercase().as_str() {
        "md5" => Ok(format!("$1${}", String::from_utf8_lossy(&random_salt_chars(8)?))),
        "des" => Err(CryptError::Unsupported("gen_salt(\"des\") (crypt-des.c)")),
        "xdes" => Err(CryptError::Unsupported("gen_salt(\"xdes\") (crypt-des.c)")),
        "bf" => Err(CryptError::Unsupported("gen_salt(\"bf\") (crypt-blowfish.c)")),
        "sha256crypt" | "sha512crypt" => {
            Err(CryptError::Unsupported("gen_salt(sha256crypt/sha512crypt) (crypt-sha.c)"))
        }
        _ => Err(CryptError::Message("gen_salt: Unknown salt algorithm".to_string())),
    }
}

fn md5(parts: &[&[u8]]) -> [u8; MD5_SIZE] {
    let mut ctx = Md5::new();
    for p in parts {
        ctx.update(p);
    }
    ctx.finish()
}

fn to64(out: &mut Vec<u8>, mut v: u32, n: usize) {
    for _ in 0..n {
        out.push(ITOA64[(v & 0x3f) as usize]);
        v >>= 6;
    }
}

pub fn crypt(password: &str, salt: &str) -> Result<String, CryptError> {
    let s = salt.as_bytes();
    if s.starts_with(b"$1$") {
        crypt_md5(password.as_bytes(), s)
    } else if s.starts_with(b"$5$") || s.starts_with(b"$6$") {
        Err(CryptError::Unsupported("crypt() sha-crypt $5$/$6$ (crypt-sha.c)"))
    } else if s.starts_with(b"$2a$") || s.starts_with(b"$2x$") || s.starts_with(b"$2b$") {
        Err(CryptError::Unsupported("crypt() bcrypt $2$ (crypt-blowfish.c)"))
    } else if s.first() == Some(&b'_') {
        Err(CryptError::Unsupported("crypt() xdes _ (crypt-des.c)"))
    } else {
        Err(CryptError::Unsupported("crypt() traditional DES (crypt-des.c)"))
    }
}

fn crypt_md5(pw: &[u8], salt: &[u8]) -> Result<String, CryptError> {
    const MAGIC: &[u8] = b"$1$";
    let after = &salt[MAGIC.len()..];
    let mut sl = 0usize;
    while sl < after.len() && sl < 8 && after[sl] != b'$' {
        sl += 1;
    }
    let salt_bytes = &after[..sl];

    let alt = md5(&[pw, salt_bytes, pw]);

    let mut ctx = Md5::new();
    ctx.update(pw);
    ctx.update(MAGIC);
    ctx.update(salt_bytes);
    let mut pl = pw.len();
    while pl > 0 {
        let take = pl.min(MD5_SIZE);
        ctx.update(&alt[..take]);
        pl -= take;
    }
    let mut i = pw.len();
    while i != 0 {
        if i & 1 != 0 {
            ctx.update(&[0u8]);
        } else {
            ctx.update(&pw[..1]);
        }
        i >>= 1;
    }
    let mut digest = ctx.finish();

    for r in 0..1000usize {
        let mut c = Md5::new();
        if r & 1 != 0 {
            c.update(pw);
        } else {
            c.update(&digest);
        }
        if r % 3 != 0 {
            c.update(salt_bytes);
        }
        if r % 7 != 0 {
            c.update(pw);
        }
        if r & 1 != 0 {
            c.update(&digest);
        } else {
            c.update(pw);
        }
        digest = c.finish();
    }

    let d = &digest;
    let mut enc = Vec::with_capacity(22);
    to64(&mut enc, ((d[0] as u32) << 16) | ((d[6] as u32) << 8) | (d[12] as u32), 4);
    to64(&mut enc, ((d[1] as u32) << 16) | ((d[7] as u32) << 8) | (d[13] as u32), 4);
    to64(&mut enc, ((d[2] as u32) << 16) | ((d[8] as u32) << 8) | (d[14] as u32), 4);
    to64(&mut enc, ((d[3] as u32) << 16) | ((d[9] as u32) << 8) | (d[15] as u32), 4);
    to64(&mut enc, ((d[4] as u32) << 16) | ((d[10] as u32) << 8) | (d[5] as u32), 4);
    to64(&mut enc, d[11] as u32, 2);

    Ok(format!(
        "$1${}${}",
        String::from_utf8_lossy(salt_bytes),
        String::from_utf8_lossy(&enc)
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    // The $1$ output shape (magic + 8 salt chars + $ + 22 base64). Exact
    // byte-identity vs C is gated by crypt-md5.sql in the fleet e2e.
    #[test]
    fn md5_crypt_shape_and_roundtrip() {
        let h = crypt("foox", "$1$Szzz0yzz").map_err(|_| ()).unwrap();
        assert!(h.starts_with("$1$Szzz0yzz$"));
        assert_eq!(h.len(), "$1$Szzz0yzz$".len() + 22);
        // crypt(pw, hash) reproduces the hash (the pgcrypto self-check).
        assert_eq!(crypt("foox", &h).map_err(|_| ()).unwrap(), h);
    }

    #[test]
    fn unsupported_schemes_are_loud() {
        assert!(matches!(crypt("x", "$2a$06$abc"), Err(CryptError::Unsupported(_))));
        assert!(matches!(gen_salt("bf", 0), Err(CryptError::Unsupported(_))));
    }
}
