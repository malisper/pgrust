//! Traditional DES crypt and BSDI extended DES (`_`, xdes) entry points.
//!
//! The actual FreeSec DES algorithm lives in [`super::cryptdes`], a self-contained
//! native port of `contrib/pgcrypto/crypt-des.c`. This module only wraps it with
//! the length checks that C raises as `ereport(ERROR, "invalid salt")` and maps
//! C's NULL return to `"crypt(3) returned NULL"`, so error text and ordering
//! match the C byte-for-byte.

use super::cryptdes::px_crypt_des;

/// `crypt_des(key, setting)` — traditional 13-char DES crypt. `setting` must
/// carry a 2-char salt (C errors `invalid salt` otherwise).
pub fn crypt_des(pw: &[u8], setting: &[u8]) -> Result<String, String> {
    if setting.len() < 2 {
        return Err("invalid salt".to_string());
    }
    match px_crypt_des(pw, setting) {
        Some(out) => Ok(String::from_utf8_lossy(&out).into_owned()),
        None => Err("crypt(3) returned NULL".to_string()),
    }
}

/// BSDI extended DES (`_`, xdes). `setting` is `_<4 rounds><4 salt>...`.
pub fn crypt_xdes(pw: &[u8], setting: &[u8]) -> Result<String, String> {
    // C requires at least the `_` + 4 round chars + 4 salt chars.
    if setting.len() < 9 {
        return Err("invalid salt".to_string());
    }
    match px_crypt_des(pw, setting) {
        Some(out) => Ok(String::from_utf8_lossy(&out).into_owned()),
        None => Err("crypt(3) returned NULL".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn desc_xdes_known_vectors() {
        assert_eq!(
            crypt_xdes(b"", b"_J9..j2zz").unwrap(),
            "_J9..j2zzR/nIRDK3pPc"
        );
        assert_eq!(
            crypt_xdes(b"foox", b"_J9..j2zz").unwrap(),
            "_J9..j2zzAYKMvO2BYRY"
        );
        assert_eq!(
            crypt_xdes(b"longlongpassword", b"_J9..j2zz").unwrap(),
            "_J9..j2zz4BeseiQNwUg"
        );
    }

    #[test]
    fn desc_xdes_adversarial_bang_salt() {
        assert_eq!(
            crypt_xdes(b"password", b"_/!!!!!!!").unwrap(),
            "_/!!!!!!!zqM49hRzxko"
        );
    }

    #[test]
    fn desc_xdes_count_zero_returns_null() {
        // count == 0 → px_crypt_des returns None → "crypt(3) returned NULL"
        assert_eq!(
            crypt_xdes(b"password", b"_........").unwrap_err(),
            "crypt(3) returned NULL"
        );
        assert_eq!(
            crypt_xdes(b"password", b"_..!!!!!!").unwrap_err(),
            "crypt(3) returned NULL"
        );
    }

    #[test]
    fn desc_xdes_short_setting_invalid_salt() {
        assert_eq!(crypt_xdes(b"foox", b"_J9..BWH").unwrap_err(), "invalid salt");
    }

    #[test]
    fn desc_traditional_known_vector() {
        // Traditional DES crypt: 2-char salt, classic crypt(3) vector.
        assert_eq!(crypt_des(b"foob", b"rl").unwrap(), "rlK6kmJqyMjZM");
    }
}
