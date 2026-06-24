//! Traditional DES crypt (`crypt-des.c`) and BSDI extended DES (`_`, xdes),
//! driven through the `pwhash` crate's `unix_crypt` / `bsdi_crypt` (which
//! implement the salt-perturbed DES used by `crypt(3)`, byte-identical to
//! pgcrypto's bundled `crypt-des.c`).

/// `crypt_des(key, setting)` — traditional 13-char DES crypt. `setting` must
/// carry a 2-char salt (C errors `invalid salt` otherwise).
pub fn crypt_des(pw: &[u8], setting: &[u8]) -> Result<String, String> {
    if setting.len() < 2 {
        return Err("invalid salt".to_string());
    }
    let pw_s = String::from_utf8_lossy(pw);
    let setting_s = String::from_utf8_lossy(setting);
    ::pwhash::unix_crypt::hash_with(setting_s.as_ref(), pw_s.as_ref())
        .map_err(|_| "invalid salt".to_string())
}

/// BSDI extended DES (`_`, xdes). `setting` is `_<4 rounds><4 salt>...`.
pub fn crypt_xdes(pw: &[u8], setting: &[u8]) -> Result<String, String> {
    // C requires at least the `_` + 4 round chars + 4 salt chars.
    if setting.len() < 9 {
        return Err("invalid salt".to_string());
    }
    let pw_s = String::from_utf8_lossy(pw);
    let setting_s = String::from_utf8_lossy(setting);
    match ::pwhash::bsdi_crypt::hash_with(setting_s.as_ref(), pw_s.as_ref()) {
        Ok(h) => Ok(h),
        // C's px_crypt returns NULL (→ `crypt(3) returned NULL`) when the round
        // count encodes to 0 (invalid encoding / explicit 0).
        Err(_) => Err("crypt(3) returned NULL".to_string()),
    }
}
