//! encrypt()/decrypt()(+_iv) over RustCrypto block ciphers, byte-identical to
//! pgcrypto's OpenSSL px_combo_* (openssl.c aliases, key/IV handling, padding).

use ::aes::{Aes128, Aes192, Aes256};
use ::blowfish::Blowfish;
use ::cast5::Cast5;
use ::cipher::{
    block_padding::{NoPadding, Pkcs7},
    generic_array::GenericArray,
    AsyncStreamCipher, BlockDecryptMut, BlockEncrypt, BlockEncryptMut, KeyInit, KeyIvInit,
};
use ::des::{Des, TdesEde3};

/// A single-block ECB encryptor over one supported cipher, driving the OpenPGP
/// CFB keystream (`fre = ECB_encrypt(fr)`). Caller supplies the exact-length
/// key (S2K-derived or the session key).
pub enum BlockEncryptor {
    Bf(Box<Blowfish>),
    Des3(Box<TdesEde3>),
    Cast5(Box<Cast5>),
    Aes128(Box<Aes128>),
    Aes192(Box<Aes192>),
    Aes256(Box<Aes256>),
}

impl BlockEncryptor {
    pub fn new(int_name: &str, key: &[u8]) -> Option<BlockEncryptor> {
        Some(match int_name {
            "bf-ecb" => BlockEncryptor::Bf(Box::new(Blowfish::new_from_slice(key).ok()?)),
            "3des-ecb" => BlockEncryptor::Des3(Box::new(TdesEde3::new_from_slice(key).ok()?)),
            "cast5-ecb" => BlockEncryptor::Cast5(Box::new(Cast5::new_from_slice(key).ok()?)),
            "aes-ecb" => match key.len() {
                16 => BlockEncryptor::Aes128(Box::new(Aes128::new_from_slice(key).ok()?)),
                24 => BlockEncryptor::Aes192(Box::new(Aes192::new_from_slice(key).ok()?)),
                32 => BlockEncryptor::Aes256(Box::new(Aes256::new_from_slice(key).ok()?)),
                _ => return None,
            },
            _ => return None,
        })
    }

    pub fn block_size(&self) -> usize {
        match self {
            BlockEncryptor::Aes128(_) | BlockEncryptor::Aes192(_) | BlockEncryptor::Aes256(_) => 16,
            _ => 8,
        }
    }

    pub fn encrypt_block(&self, block: &mut [u8]) {
        match self {
            BlockEncryptor::Bf(c) => c.encrypt_block(GenericArray::from_mut_slice(block)),
            BlockEncryptor::Des3(c) => c.encrypt_block(GenericArray::from_mut_slice(block)),
            BlockEncryptor::Cast5(c) => c.encrypt_block(GenericArray::from_mut_slice(block)),
            BlockEncryptor::Aes128(c) => c.encrypt_block(GenericArray::from_mut_slice(block)),
            BlockEncryptor::Aes192(c) => c.encrypt_block(GenericArray::from_mut_slice(block)),
            BlockEncryptor::Aes256(c) => c.encrypt_block(GenericArray::from_mut_slice(block)),
        }
    }
}

pub enum CipherError {
    // The three carry the downcased/truncated spec (C find_provider's
    // "Cannot use \"%s\": %s", pgcrypto.c:513) and map to px_strerror text:
    // PXE_NO_CIPHER / PXE_BAD_OPTION / PXE_BAD_FORMAT (px.c:273-276).
    NoCipher(String),
    BadOption(String),
    BadFormat(String),
    EncryptFailed,
    DecryptFailed,
    /// An ereport raised while normalising the name (allocation failure).
    Pg(Box<types_error::PgError>),
}

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

/// px.c:246 parse_cipher_name + px.c:288 px_find_combo over the name that
/// find_provider (pgcrypto.c:504) already ran through
/// downcase_truncate_identifier.
fn parse_spec(spec: &str) -> Result<Spec, CipherError> {
    let lower = crate::provider_name(spec).map_err(CipherError::Pg)?;
    let mut parts = lower.split('/');
    let cipher_part = parts.next().unwrap_or("");

    // parse_cipher_name: empty segments are skipped; "key:value" with any
    // key but "pad" is PXE_BAD_OPTION; a segment without ':' is
    // PXE_BAD_FORMAT. Options are parsed BEFORE the cipher lookup.
    let mut pad: Option<&str> = None;
    for opt in parts {
        if opt.is_empty() {
            continue;
        }
        let Some((k, v)) = opt.split_once(':') else {
            return Err(CipherError::BadFormat(lower.clone()));
        };
        if k != "pad" {
            return Err(CipherError::BadOption(lower.clone()));
        }
        pad = Some(v);
    }
    // px_find_combo: an unknown pad value is PXE_NO_CIPHER (err1).
    let padding = match pad {
        None | Some("pkcs") => true,
        Some("none") => false,
        Some(_) => return Err(CipherError::NoCipher(lower.clone())),
    };

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

fn block_size(kind: CipherKind) -> usize {
    match kind {
        CipherKind::Aes => 16,
        _ => 8,
    }
}

/// px.c:204 combo_init: `if (klen > ks) klen = ks` — the key is cut to the
/// cipher's max key size (openssl.c cipher table: AES 32, Blowfish 56,
/// CAST5 16, DES 8, 3DES 24) before init, never refused for being long.
fn prepare_key(kind: CipherKind, key: &[u8]) -> Option<Vec<u8>> {
    Some(match kind {
        CipherKind::Aes => {
            let key = &key[..key.len().min(32)];
            // ossl_aes_init: 128/192/256 by key length (zero-padded).
            let target = if key.len() <= 16 {
                16
            } else if key.len() <= 24 {
                24
            } else {
                32
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
            let key = &key[..key.len().min(56)];
            // OpenSSL BF_set_key cycles the key through the P-array; RustCrypto
            // rejects len<4. Repeat to the smallest multiple >=4 to reproduce
            // the exact cycling (period = orig len) byte-for-byte.
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
            let key = &key[..key.len().min(16)];
            // OpenSSL CAST5 zero-pads a <=10-byte "small key" to 16; RustCrypto
            // rejects len<5. Zero-pad up to 5 to keep the small-key schedule.
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

fn prepare_iv(kind: CipherKind, iv: &[u8]) -> Vec<u8> {
    let bs = block_size(kind);
    let mut v = vec![0u8; bs];
    let n = iv.len().min(bs);
    v[..n].copy_from_slice(&iv[..n]);
    v
}

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

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(b: &[u8]) -> String {
        b.iter().map(|x| format!("{x:02x}")).collect()
    }

    // Canonical Blowfish ECB KAT (all-zero key+block), matching the value the
    // PGDG C build computes when its OpenSSL legacy provider IS available, and
    // the pg-side output observed on the CI cluster. bf/cast5/des are not e2e-gated
    // (PGDG's OpenSSL-3 legacy provider is off, so live C errors on them).
    #[test]
    fn blowfish_ecb_known_answer() {
        let ct = encrypt("bf-ecb/pad:none", &[0u8; 8], &[], &[0u8; 8]).map_err(|_| ()).unwrap();
        assert_eq!(hex(&ct), "4ef997456198dd78");
    }

    // Standard single-DES ECB KAT (all-zero key+block).
    #[test]
    fn des_ecb_known_answer() {
        let ct = encrypt("des-ecb/pad:none", &[0u8; 8], &[], &[0u8; 8]).map_err(|_| ()).unwrap();
        assert_eq!(hex(&ct), "8ca64de9c1b123a7");
    }

    // encrypt/decrypt inverse over each family (locks correctness regardless of
    // the C legacy-provider availability).
    #[test]
    fn roundtrips() {
        for spec in ["bf-cbc", "des-cbc", "cast5-cbc", "aes-cbc", "3des-cbc"] {
            let key = [1u8; 16];
            let pt = b"pgcrypto roundtrip payload!!";
            let ct = encrypt(spec, &key, &[], pt).map_err(|_| ()).unwrap();
            let back = decrypt(spec, &key, &[], &ct).map_err(|_| ()).unwrap();
            assert_eq!(&back, pt, "{spec}");
        }
    }

    #[test]
    fn unknown_cipher_errors() {
        assert!(matches!(encrypt("nope-cbc", &[0u8; 8], &[], &[0u8; 8]), Err(CipherError::NoCipher(_))));
    }
}
