//! Traditional DES crypt and BSDI extended DES (`_`, xdes) entry points.
//!
//! The actual FreeSec DES algorithm lives in [`super::cryptdes`], a self-contained
//! native port of `contrib/pgcrypto/crypt-des.c`. This module only wraps it with
//! the length checks that C raises as `ereport(ERROR, "invalid salt")` and maps
//! C's NULL return to `"crypt(3) returned NULL"`, so error text and ordering
//! match the C byte-for-byte.

use super::cryptdes::px_crypt_des;
use super::CryptError;

/// `crypt_des(key, setting)` — traditional 13-char DES crypt. `setting` must
/// carry a 2-char salt (C errors `invalid salt` otherwise).
pub fn crypt_des(pw: &[u8], setting: &[u8]) -> Result<Vec<u8>, CryptError> {
    if setting.len() < 2 {
        return Err(CryptError::Message("invalid salt".to_string()));
    }
    // D21: C copies setting[0..2] VERBATIM into the result (crypt-des.c), so
    // the hash is raw bytes — never re-encode it as UTF-8.
    px_crypt_des(pw, setting)
}

/// `run_crypt_des` (px-crypt.c:37) — the handler BOTH the `"_"` and the
/// zero-length catch-all rows of `px_crypt_list` point at. `px_crypt_des`
/// picks the xdes or the traditional branch off `setting[0] == '_'`
/// (crypt-des.c:681), and each branch carries its own length check.
pub fn run_crypt_des(pw: &[u8], setting: &[u8]) -> Result<Vec<u8>, CryptError> {
    if setting.first() == Some(&b'_') {
        crypt_xdes(pw, setting)
    } else {
        crypt_des(pw, setting)
    }
}

/// BSDI extended DES (`_`, xdes). `setting` is `_<4 rounds><4 salt>...`.
pub fn crypt_xdes(pw: &[u8], setting: &[u8]) -> Result<Vec<u8>, CryptError> {
    // C requires at least the `_` + 4 round chars + 4 salt chars.
    if setting.len() < 9 {
        return Err(CryptError::Message("invalid salt".to_string()));
    }
    // D21: C strlcpy's up to 9 setting bytes VERBATIM into the result.
    px_crypt_des(pw, setting)
}

#[cfg(test)]
mod tests {
    use super::super::cfi_test_support::{arm_cfi, cfi_calls};
    use super::*;

    fn msg(e: CryptError) -> String {
        match e {
            CryptError::Message(m) => m,
            CryptError::Unsupported(m) => panic!("unexpected Unsupported: {m}"),
            CryptError::Pg(e) => panic!("unexpected raised error: {}", e.message),
        }
    }

    #[test]
    fn desc_xdes_known_vectors() {
        arm_cfi(u64::MAX);
        assert_eq!(crypt_xdes(b"", b"_J9..j2zz").unwrap(), b"_J9..j2zzR/nIRDK3pPc");
        assert_eq!(crypt_xdes(b"foox", b"_J9..j2zz").unwrap(), b"_J9..j2zzAYKMvO2BYRY");
        assert_eq!(
            crypt_xdes(b"longlongpassword", b"_J9..j2zz").unwrap(),
            b"_J9..j2zz4BeseiQNwUg"
        );
    }

    #[test]
    fn desc_xdes_adversarial_bang_salt() {
        arm_cfi(u64::MAX);
        assert_eq!(crypt_xdes(b"password", b"_/!!!!!!!").unwrap(), b"_/!!!!!!!zqM49hRzxko");
    }

    #[test]
    fn desc_xdes_count_zero_returns_null() {
        // count == 0 -> px_crypt_des fails where C returns NULL
        assert_eq!(
            msg(crypt_xdes(b"password", b"_........").unwrap_err()),
            "crypt(3) returned NULL"
        );
        assert_eq!(
            msg(crypt_xdes(b"password", b"_..!!!!!!").unwrap_err()),
            "crypt(3) returned NULL"
        );
    }

    #[test]
    fn desc_xdes_short_setting_invalid_salt() {
        assert_eq!(msg(crypt_xdes(b"foox", b"_J9..BWH").unwrap_err()), "invalid salt");
    }

    #[test]
    fn desc_traditional_known_vector() {
        arm_cfi(u64::MAX);
        // Traditional DES crypt: 2-char salt, classic crypt(3) vector.
        assert_eq!(crypt_des(b"foob", b"rl").unwrap(), b"rlK6kmJqyMjZM");
        // C runs CHECK_FOR_INTERRUPTS once per count iteration; traditional
        // DES uses count=25.
        assert_eq!(cfi_calls(), 25);
    }

    // D19 (coordinator sweep): the DES count loop honors CHECK_FOR_INTERRUPTS
    // (C crypt-des.c:541). `_zzzz<salt>` encodes the max 24-bit xdes count -
    // an unbounded, uncancellable CPU burn before this fix. The budget keeps
    // the witness bounded either way.
    #[test]
    fn des_loop_is_cancellable() {
        arm_cfi(5);
        match crypt_xdes(b"password", b"_zzzzJ9..") {
            Err(CryptError::Pg(e)) => {
                assert_eq!(e.sqlstate, types_error::ERRCODE_QUERY_CANCELED);
            }
            Ok(_) => panic!("max-count xdes completed: interrupts not honored"),
            Err(_) => panic!("unexpected error kind"),
        }
        assert_eq!(cfi_calls(), 6); // 5 Ok iterations + the cancelling call
    }
}
