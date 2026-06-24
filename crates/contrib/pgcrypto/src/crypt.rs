//! crypt() / gen_salt() — password hashing.
//!
//! The `pwhash` traditional/extended-DES and sha-crypt entry points are marked
//! `deprecated` (they are legacy algorithms), but pgcrypto must support them for
//! `crypt(3)` compatibility, so `#![allow(deprecated)]` is intentional.
#![allow(deprecated)]
//!
//! Implements the md5-crypt (`$1$`) algorithm fully (byte-for-byte with C's
//! `crypt-md5.c`) and traditional des-crypt (`crypt-des.c`). bcrypt (`$2a$`) and
//! xdes (`_`) are routed but rely on [`bcrypt`]/[`xdes`] helpers; gen_salt
//! supports des/md5/xdes/bf salt-string generation.

mod desc;
mod md5c;

use ::pg_strong_random::pg_strong_random;

/// `_crypt_itoa64` — the base-64 alphabet crypt uses (`./0-9A-Za-z`).
const ITOA64: &[u8; 64] = b"./0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

/// Fill `n` random base-64 salt characters.
fn random_salt_chars(n: usize) -> Result<Vec<u8>, String> {
    let mut raw = vec![0u8; n];
    if !pg_strong_random(&mut raw) {
        return Err("Failed to generate random number".to_string());
    }
    Ok(raw.iter().map(|&b| ITOA64[(b & 0x3f) as usize]).collect())
}

/// `px_gen_salt(salt_type, buf, rounds)` — generate a salt string for the named
/// scheme. `rounds == 0` means "use the scheme default".
pub fn gen_salt(salt_type: &str, rounds: i32) -> Result<String, String> {
    let lower = salt_type.to_ascii_lowercase();
    match lower.as_str() {
        "des" => {
            // Two base-64 salt characters.
            let s = random_salt_chars(2)?;
            Ok(String::from_utf8_lossy(&s).into_owned())
        }
        "md5" => {
            // "$1$" + 8 salt chars.
            let s = random_salt_chars(8)?;
            Ok(format!("$1${}", String::from_utf8_lossy(&s)))
        }
        "xdes" => {
            // "_" + 4 rounds chars + 4 salt chars (BSDI extended DES).
            let n = if rounds == 0 { 7250 } else { rounds };
            // Round count must be odd to avoid a weak-key class; C uses the
            // default 0x269 (725) scaled, but the salt encodes the iteration
            // count directly in 4 base-64 chars.
            let count = (n as u32) | 1;
            let mut enc = [0u8; 4];
            let mut c = count;
            for b in enc.iter_mut() {
                *b = ITOA64[(c & 0x3f) as usize];
                c >>= 6;
            }
            let s = random_salt_chars(4)?;
            Ok(format!(
                "_{}{}",
                String::from_utf8_lossy(&enc),
                String::from_utf8_lossy(&s)
            ))
        }
        "bf" => {
            let r = if rounds == 0 { 6 } else { rounds };
            if !(4..=31).contains(&r) {
                return Err("gen_salt: Incorrect number of rounds".to_string());
            }
            // "$2a$NN$" + 22 base-64 chars of 16 random salt bytes.
            let mut raw = [0u8; 16];
            if !pg_strong_random(&mut raw) {
                return Err("Failed to generate random number".to_string());
            }
            let enc = bcrypt::encode_salt64(&raw);
            Ok(format!("$2a${r:02}${enc}"))
        }
        "sha256crypt" | "sha512crypt" => {
            // PX_SHACRYPT_ROUNDS_{DEFAULT,MIN,MAX} = 5000 / 1000 / 999999999.
            let r = if rounds == 0 { 5000 } else { rounds };
            if !(1000..=999_999_999).contains(&r) {
                return Err("gen_salt: Incorrect number of rounds".to_string());
            }
            // 16 random base-64 salt chars (the crypt itoa64 alphabet).
            let salt = random_salt_chars(16)?;
            let magic = if lower == "sha256crypt" { '5' } else { '6' };
            Ok(format!(
                "${magic}$rounds={r}${}",
                String::from_utf8_lossy(&salt)
            ))
        }
        _ => Err("gen_salt: Unknown salt algorithm".to_string()),
    }
}

/// `px_crypt(psw, salt)` — dispatch on the salt prefix (`crypt-blowfish.c`'s
/// `px_crypt` router).
pub fn crypt(password: &str, salt: &str) -> Result<String, String> {
    let s = salt.as_bytes();
    if s.starts_with(b"$1$") {
        md5c::crypt_md5(password.as_bytes(), s)
    } else if s.starts_with(b"$5$") || s.starts_with(b"$6$") {
        shacrypt::crypt_sha(password, salt)
    } else if s.starts_with(b"$2a$") || s.starts_with(b"$2x$") || s.starts_with(b"$2b$") {
        bcrypt::crypt_bf(password.as_bytes(), s)
    } else if s.first() == Some(&b'_') {
        desc::crypt_xdes(password.as_bytes(), s)
    } else {
        // Traditional DES crypt: needs a 2-char salt.
        if s.len() < 2 {
            return Err("invalid salt".to_string());
        }
        desc::crypt_des(password.as_bytes(), s)
    }
}

mod bcrypt;
mod shacrypt;
