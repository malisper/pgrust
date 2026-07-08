//! crypt() / gen_salt() password hashing (px-crypt.c router). md5-crypt
//! (`$1$`) is inline over the in-repo pg_md5; des/xdes (crypt-des.c), bcrypt
//! (`$2a$`, crypt-blowfish.c), and sha-crypt (`$5$`/`$6$`, crypt-sha.c) live in
//! submodules. None of these touch OpenSSL (pgcrypto's own C), so live-C
//! byte-identity holds on the fleet.
#![allow(deprecated)]

mod bcrypt;
mod cryptdes;
mod desc;
mod shacrypt;

use pg_md5::Md5;
use pg_strong_random::pg_strong_random;

const MD5_SIZE: usize = 16;
const ITOA64: &[u8; 64] = b"./0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

pub enum CryptError {
    Unsupported(&'static str),
    Message(String),
}

impl From<String> for CryptError {
    fn from(m: String) -> Self {
        CryptError::Message(m)
    }
}

fn random_salt_chars(n: usize) -> Result<Vec<u8>, String> {
    let mut raw = vec![0u8; n];
    if !pg_strong_random(&mut raw) {
        return Err("Failed to generate random number".to_string());
    }
    Ok(raw.iter().map(|&b| ITOA64[(b & 0x3f) as usize]).collect())
}

pub fn gen_salt(salt_type: &str, rounds: i32) -> Result<String, CryptError> {
    let lower = salt_type.to_ascii_lowercase();
    Ok(match lower.as_str() {
        "des" => String::from_utf8_lossy(&random_salt_chars(2)?).into_owned(),
        "md5" => format!("$1${}", String::from_utf8_lossy(&random_salt_chars(8)?)),
        "xdes" => {
            let n = if rounds == 0 { 7250 } else { rounds };
            let count = (n as u32) | 1;
            let mut enc = [0u8; 4];
            let mut c = count;
            for b in enc.iter_mut() {
                *b = ITOA64[(c & 0x3f) as usize];
                c >>= 6;
            }
            format!(
                "_{}{}",
                String::from_utf8_lossy(&enc),
                String::from_utf8_lossy(&random_salt_chars(4)?)
            )
        }
        "bf" => {
            let r = if rounds == 0 { 6 } else { rounds };
            if !(4..=31).contains(&r) {
                return Err("gen_salt: Incorrect number of rounds".to_string().into());
            }
            let mut raw = [0u8; 16];
            if !pg_strong_random(&mut raw) {
                return Err("Failed to generate random number".to_string().into());
            }
            format!("$2a${r:02}${}", bcrypt::encode_salt64(&raw))
        }
        "sha256crypt" | "sha512crypt" => {
            let r = if rounds == 0 { 5000 } else { rounds };
            if !(1000..=999_999_999).contains(&r) {
                return Err("gen_salt: Incorrect number of rounds".to_string().into());
            }
            let salt = random_salt_chars(16)?;
            let magic = if lower == "sha256crypt" { '5' } else { '6' };
            format!("${magic}$rounds={r}${}", String::from_utf8_lossy(&salt))
        }
        _ => return Err("gen_salt: Unknown salt algorithm".to_string().into()),
    })
}

pub fn crypt(password: &str, salt: &str) -> Result<String, CryptError> {
    let s = salt.as_bytes();
    let out = if s.starts_with(b"$1$") {
        crypt_md5(password.as_bytes(), s)
    } else if s.starts_with(b"$5$") || s.starts_with(b"$6$") {
        shacrypt::crypt_sha(password, salt)
    } else if s.starts_with(b"$2a$") || s.starts_with(b"$2x$") || s.starts_with(b"$2b$") {
        bcrypt::crypt_bf(password.as_bytes(), s)
    } else if s.first() == Some(&b'_') {
        desc::crypt_xdes(password.as_bytes(), s)
    } else {
        desc::crypt_des(password.as_bytes(), s)
    };
    out.map_err(CryptError::Message)
}

fn to64(out: &mut Vec<u8>, mut v: u32, n: usize) {
    for _ in 0..n {
        out.push(ITOA64[(v & 0x3f) as usize]);
        v >>= 6;
    }
}

fn crypt_md5(pw: &[u8], salt: &[u8]) -> Result<String, String> {
    const MAGIC: &[u8] = b"$1$";
    let after = &salt[MAGIC.len()..];
    let mut sl = 0usize;
    while sl < after.len() && sl < 8 && after[sl] != b'$' {
        sl += 1;
    }
    let salt_bytes = &after[..sl];

    let mut alt_ctx = Md5::new();
    alt_ctx.update(pw);
    alt_ctx.update(salt_bytes);
    alt_ctx.update(pw);
    let alt = alt_ctx.finish();

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

    #[test]
    fn md5_crypt_shape_and_roundtrip() {
        let h = crypt("foox", "$1$Szzz0yzz").map_err(|_| ()).unwrap();
        assert!(h.starts_with("$1$Szzz0yzz$"));
        assert_eq!(h.len(), "$1$Szzz0yzz$".len() + 22);
        assert_eq!(crypt("foox", &h).map_err(|_| ()).unwrap(), h);
    }

    // Traditional/xdes DES known vectors (crypt-des.c), incl. adversarial salt.
    #[test]
    fn des_known_vectors() {
        assert_eq!(crypt("foob", "rl").map_err(|_| ()).unwrap(), "rlK6kmJqyMjZM");
        assert_eq!(
            crypt("password", "_/!!!!!!!").map_err(|_| ()).unwrap(),
            "_/!!!!!!!zqM49hRzxko"
        );
    }

    // bcrypt $2a$ roundtrip: crypt(pw, hash) reproduces the hash.
    #[test]
    fn bcrypt_roundtrip() {
        let setting = "$2a$06$......................";
        let h = crypt("foox", setting).map_err(|_| ()).unwrap();
        assert!(h.starts_with("$2a$06$"));
        assert_eq!(crypt("foox", &h).map_err(|_| ()).unwrap(), h);
    }

    // sha-crypt $5$/$6$ roundtrip.
    #[test]
    fn shacrypt_roundtrip() {
        let h5 = crypt("foox", "$5$Szzz0yzz").map_err(|_| ()).unwrap();
        assert!(h5.starts_with("$5$Szzz0yzz$"));
        assert_eq!(crypt("foox", &h5).map_err(|_| ()).unwrap(), h5);
        let h6 = crypt("foox", "$6$Szzz0yzz").map_err(|_| ()).unwrap();
        assert!(h6.starts_with("$6$Szzz0yzz$"));
        assert_eq!(crypt("foox", &h6).map_err(|_| ()).unwrap(), h6);
    }
}
