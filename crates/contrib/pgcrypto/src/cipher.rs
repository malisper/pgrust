//! encrypt() / decrypt() (+ _iv) over the RustCrypto block ciphers, matching
//! pgcrypto's OpenSSL-backed `px_combo_*` byte-for-byte:
//!
//! * cipher spec `"<cipher>-<mode>[/pad:pkcs|none]"` (e.g. `aes-cbc`,
//!   `bf-ecb/pad:none`), with the alias table from `openssl.c`.
//! * ECB / CBC modes; PKCS#7 padding on (`pkcs`, default) or off (`none`).
//! * IV defaults to all-zeros when not supplied (the `encrypt`/`decrypt`
//!   non-iv variants).
//! * Key handling per `ossl_*_init`: AES zero-extends to 128/192/256;
//!   DES = 8 bytes (zero-pad/truncate); 3DES = 24 bytes; Blowfish/CAST5 use the
//!   key verbatim.

use ::aes::{Aes128, Aes192, Aes256};
use ::blowfish::Blowfish;
use ::cast5::Cast5;
use ::cipher::{
    block_padding::{NoPadding, Pkcs7},
    AsyncStreamCipher, BlockDecryptMut, BlockEncryptMut, KeyInit, KeyIvInit,
};
use ::des::{Des, TdesEde3};

/// A cipher operation failure, mapped by the caller to pgcrypto's exact error
/// text (`px_strerror`): a missing cipher (`Cannot use "<spec>": No such cipher
/// algorithm`) or an encrypt/decrypt failure (`<op> error: <Encryption|
/// Decryption> failed`).
pub enum CipherError {
    /// `PXE_NO_CIPHER` — the spec named no known cipher. Carries the downcased
    /// spec for the `Cannot use "%s"` message.
    NoCipher(String),
    /// `PXE_ENCRYPT_FAILED` / a bad key / unparsable spec on the encrypt path.
    EncryptFailed,
    /// `PXE_DECRYPT_FAILED` — bad padding / bad key on the decrypt path.
    DecryptFailed,
}

/// A block cipher family (selects key handling + block size).
#[derive(Clone, Copy, PartialEq)]
enum CipherKind {
    Bf,
    Des,
    Des3,
    Cast5,
    Aes,
}

#[derive(Clone, Copy, PartialEq)]
enum Mode {
    Ecb,
    Cbc,
    Cfb,
}

struct Spec {
    kind: CipherKind,
    mode: Mode,
    padding: bool,
}

/// `px_find_combo` — `px_resolve_alias(ossl_aliases, name)` then split off the
/// `[/pad:...]` options. On an unknown cipher, the error carries the FULL
/// downcased spec string (C's `find_provider` builds `Cannot use "%s"` from the
/// whole downcased `type` text, not just the cipher name).
fn parse_spec(spec: &str) -> Result<Spec, CipherError> {
    let lower = spec.to_ascii_lowercase();
    // Split cipher-name from pad option(s) on '/'.
    let mut parts = lower.split('/');
    let cipher_part = parts.next().unwrap_or("");

    // Default padding = pkcs (1).
    let mut padding = true;
    for opt in parts {
        // each option is "pad:pkcs" / "pad:none".
        let Some((k, v)) = opt.split_once(':') else {
            return Err(CipherError::NoCipher(lower.clone()));
        };
        if k != "pad" {
            return Err(CipherError::NoCipher(lower.clone()));
        }
        padding = match v {
            "pkcs" => true,
            "none" => false,
            _ => return Err(CipherError::NoCipher(lower.clone())),
        };
    }

    // Resolve alias to canonical "<cipher>-<mode>".
    let canon = resolve_alias(cipher_part);
    let (kind, mode) = match canon.as_str() {
        "bf-ecb" => (CipherKind::Bf, Mode::Ecb),
        "bf-cbc" => (CipherKind::Bf, Mode::Cbc),
        "bf-cfb" => (CipherKind::Bf, Mode::Cfb),
        "des-ecb" => (CipherKind::Des, Mode::Ecb),
        "des-cbc" => (CipherKind::Des, Mode::Cbc),
        "des3-ecb" => (CipherKind::Des3, Mode::Ecb),
        "des3-cbc" => (CipherKind::Des3, Mode::Cbc),
        "cast5-ecb" => (CipherKind::Cast5, Mode::Ecb),
        "cast5-cbc" => (CipherKind::Cast5, Mode::Cbc),
        "aes-ecb" => (CipherKind::Aes, Mode::Ecb),
        "aes-cbc" => (CipherKind::Aes, Mode::Cbc),
        "aes-cfb" => (CipherKind::Aes, Mode::Cfb),
        _ => return Err(CipherError::NoCipher(lower)),
    };
    Ok(Spec {
        kind,
        mode,
        padding,
    })
}

/// `ossl_aliases` from openssl.c.
fn resolve_alias(name: &str) -> String {
    match name {
        "bf" | "blowfish" | "blowfish-cbc" => "bf-cbc",
        "blowfish-ecb" => "bf-ecb",
        "blowfish-cfb" => "bf-cfb",
        "des" => "des-cbc",
        "3des" | "3des-cbc" => "des3-cbc",
        "3des-ecb" => "des3-ecb",
        "cast5" => "cast5-cbc",
        "aes" | "rijndael" | "rijndael-cbc" => "aes-cbc",
        "rijndael-ecb" => "aes-ecb",
        "rijndael-cfb" => "aes-cfb",
        other => other,
    }
    .to_string()
}

/// Block size of the cipher family.
fn block_size(kind: CipherKind) -> usize {
    match kind {
        CipherKind::Aes => 16,
        _ => 8,
    }
}

/// Prepare the key buffer per the family's `*_init` (zero-extend/truncate).
/// `None` = the cipher's `*_init` would reject the key (`PXE_KEY_TOO_BIG`).
fn prepare_key(kind: CipherKind, key: &[u8]) -> Option<Vec<u8>> {
    Some(match kind {
        CipherKind::Aes => {
            // klen rounds up to 16/24/32, zero-extended.
            let target = if key.len() <= 16 {
                16
            } else if key.len() <= 24 {
                24
            } else if key.len() <= 32 {
                32
            } else {
                return None;
            };
            let mut k = vec![0u8; target];
            k[..key.len()].copy_from_slice(key);
            k
        }
        CipherKind::Des => {
            let mut k = vec![0u8; 8];
            let n = key.len().min(8);
            k[..n].copy_from_slice(&key[..n]);
            k
        }
        CipherKind::Des3 => {
            let mut k = vec![0u8; 24];
            let n = key.len().min(24);
            k[..n].copy_from_slice(&key[..n]);
            k
        }
        CipherKind::Bf => {
            if key.is_empty() {
                return None;
            }
            // OpenSSL's BF_set_key cycles the key bytes through the P-array, so a
            // sub-4-byte key is accepted. RustCrypto's Blowfish key schedule
            // cycles identically but rejects len < 4. Repeating the key to the
            // smallest multiple of its length that is >= 4 reproduces the exact
            // cycling sequence (period = orig len), so the schedule matches
            // OpenSSL byte-for-byte. Keys >= 4 are passed through unchanged.
            if key.len() >= 4 {
                key.to_vec()
            } else {
                let orig = key.len();
                let mut target = orig;
                while target < 4 {
                    target += orig;
                }
                let mut k = Vec::with_capacity(target);
                for i in 0..target {
                    k.push(key[i % orig]);
                }
                k
            }
        }
        CipherKind::Cast5 => {
            if key.is_empty() {
                return None;
            }
            // OpenSSL's CAST5 treats a key of <= 10 bytes as a "small key" and
            // zero-pads it internally to 16 bytes; RustCrypto does the same but
            // rejects len < 5. Zero-padding a sub-5-byte key up to 5 keeps the
            // small-key flag (len <= 10) and the zero-padded schedule identical
            // to OpenSSL. Keys in [5, 16] pass through unchanged.
            if key.len() >= 5 {
                key.to_vec()
            } else {
                let mut k = vec![0u8; 5];
                k[..key.len()].copy_from_slice(key);
                k
            }
        }
    })
}

/// Prepare the IV buffer (block_size bytes; zero-filled if not supplied).
fn prepare_iv(kind: CipherKind, iv: &[u8]) -> Vec<u8> {
    let bs = block_size(kind);
    let mut v = vec![0u8; bs];
    let n = iv.len().min(bs);
    v[..n].copy_from_slice(&iv[..n]);
    v
}

/// Drive one encrypt over the chosen cipher/mode/padding. Generic over the
/// RustCrypto block-cipher type. `EncryptFailed` mirrors OpenSSL's
/// `PXE_ENCRYPT_FAILED` (e.g. `pad:none` with a non-block-multiple input, or a
/// rejected key).
macro_rules! do_encrypt {
    ($C:ty, $spec:expr, $key:expr, $iv:expr, $data:expr) => {{
        match ($spec.mode, $spec.padding) {
            (Mode::Ecb, true) => {
                let enc = ::ecb::Encryptor::<$C>::new_from_slice($key)
                    .map_err(|_| CipherError::EncryptFailed)?;
                Ok(enc.encrypt_padded_vec_mut::<Pkcs7>($data))
            }
            (Mode::Ecb, false) => {
                if $data.len() % block_size($spec.kind) != 0 {
                    return Err(CipherError::EncryptFailed);
                }
                let enc = ::ecb::Encryptor::<$C>::new_from_slice($key)
                    .map_err(|_| CipherError::EncryptFailed)?;
                Ok(enc.encrypt_padded_vec_mut::<NoPadding>($data))
            }
            (Mode::Cbc, true) => {
                let enc = ::cbc::Encryptor::<$C>::new_from_slices($key, $iv)
                    .map_err(|_| CipherError::EncryptFailed)?;
                Ok(enc.encrypt_padded_vec_mut::<Pkcs7>($data))
            }
            (Mode::Cbc, false) => {
                if $data.len() % block_size($spec.kind) != 0 {
                    return Err(CipherError::EncryptFailed);
                }
                let enc = ::cbc::Encryptor::<$C>::new_from_slices($key, $iv)
                    .map_err(|_| CipherError::EncryptFailed)?;
                Ok(enc.encrypt_padded_vec_mut::<NoPadding>($data))
            }
            // CFB is a self-synchronizing stream mode: no padding, any length.
            (Mode::Cfb, _) => {
                let enc = ::cfb_mode::Encryptor::<$C>::new_from_slices($key, $iv)
                    .map_err(|_| CipherError::EncryptFailed)?;
                let mut buf = $data.to_vec();
                enc.encrypt(&mut buf);
                Ok(buf)
            }
        }
    }};
}

macro_rules! do_decrypt {
    ($C:ty, $spec:expr, $key:expr, $iv:expr, $data:expr) => {{
        match ($spec.mode, $spec.padding) {
            (Mode::Ecb, true) => {
                let dec = ::ecb::Decryptor::<$C>::new_from_slice($key)
                    .map_err(|_| CipherError::DecryptFailed)?;
                dec.decrypt_padded_vec_mut::<Pkcs7>($data)
                    .map_err(|_| CipherError::DecryptFailed)
            }
            (Mode::Ecb, false) => {
                if $data.len() % block_size($spec.kind) != 0 {
                    return Err(CipherError::DecryptFailed);
                }
                let dec = ::ecb::Decryptor::<$C>::new_from_slice($key)
                    .map_err(|_| CipherError::DecryptFailed)?;
                dec.decrypt_padded_vec_mut::<NoPadding>($data)
                    .map_err(|_| CipherError::DecryptFailed)
            }
            (Mode::Cbc, true) => {
                let dec = ::cbc::Decryptor::<$C>::new_from_slices($key, $iv)
                    .map_err(|_| CipherError::DecryptFailed)?;
                dec.decrypt_padded_vec_mut::<Pkcs7>($data)
                    .map_err(|_| CipherError::DecryptFailed)
            }
            (Mode::Cbc, false) => {
                if $data.len() % block_size($spec.kind) != 0 {
                    return Err(CipherError::DecryptFailed);
                }
                let dec = ::cbc::Decryptor::<$C>::new_from_slices($key, $iv)
                    .map_err(|_| CipherError::DecryptFailed)?;
                dec.decrypt_padded_vec_mut::<NoPadding>($data)
                    .map_err(|_| CipherError::DecryptFailed)
            }
            (Mode::Cfb, _) => {
                let dec = ::cfb_mode::Decryptor::<$C>::new_from_slices($key, $iv)
                    .map_err(|_| CipherError::DecryptFailed)?;
                let mut buf = $data.to_vec();
                dec.decrypt(&mut buf);
                Ok(buf)
            }
        }
    }};
}

pub fn encrypt(spec: &str, key: &[u8], iv: &[u8], data: &[u8]) -> Result<Vec<u8>, CipherError> {
    let spec = parse_spec(spec)?;
    let k = prepare_key(spec.kind, key).ok_or(CipherError::EncryptFailed)?;
    let v = prepare_iv(spec.kind, iv);
    match spec.kind {
        CipherKind::Bf => do_encrypt!(Blowfish, spec, &k, &v, data),
        CipherKind::Des => do_encrypt!(Des, spec, &k, &v, data),
        CipherKind::Des3 => do_encrypt!(TdesEde3, spec, &k, &v, data),
        CipherKind::Cast5 => do_encrypt!(Cast5, spec, &k, &v, data),
        CipherKind::Aes => match k.len() {
            16 => do_encrypt!(Aes128, spec, &k, &v, data),
            24 => do_encrypt!(Aes192, spec, &k, &v, data),
            _ => do_encrypt!(Aes256, spec, &k, &v, data),
        },
    }
}

pub fn decrypt(spec: &str, key: &[u8], iv: &[u8], data: &[u8]) -> Result<Vec<u8>, CipherError> {
    let spec = parse_spec(spec)?;
    let k = prepare_key(spec.kind, key).ok_or(CipherError::DecryptFailed)?;
    let v = prepare_iv(spec.kind, iv);
    match spec.kind {
        CipherKind::Bf => do_decrypt!(Blowfish, spec, &k, &v, data),
        CipherKind::Des => do_decrypt!(Des, spec, &k, &v, data),
        CipherKind::Des3 => do_decrypt!(TdesEde3, spec, &k, &v, data),
        CipherKind::Cast5 => do_decrypt!(Cast5, spec, &k, &v, data),
        CipherKind::Aes => match k.len() {
            16 => do_decrypt!(Aes128, spec, &k, &v, data),
            24 => do_decrypt!(Aes192, spec, &k, &v, data),
            _ => do_decrypt!(Aes256, spec, &k, &v, data),
        },
    }
}
