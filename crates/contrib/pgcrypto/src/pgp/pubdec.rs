//! `pgp_parse_pubenc_sesskey` (pgp-pubdec.c) — decrypt the Public-Key Encrypted
//! Session Key packet (tag 1) with the recipient secret key, strip the
//! EME-PKCS1-v1.5 padding, validate the 16-bit checksum, and recover the
//! (cipher_algo, session key) pair.

use super::consts::*;
use super::mpi::{self, read_mpi, Mpi};
use super::pubkey::{KeyMaterial, PubKey, PGP_PUB_ELG_ENCRYPT, PGP_PUB_RSA_ENCRYPT,
    PGP_PUB_RSA_ENCRYPT_SIGN};

/// `check_eme_pkcs1_v15` — verify `02 || PS(>=8 nonzero) || 00 || M` and return
/// the offset of `M`. The padded value's leading 0x02 may have been dropped by
/// the MPI canonicalization (leading-zero / leading-byte trimming), so the
/// caller normalizes first.
fn check_eme_pkcs1_v15(data: &[u8]) -> Option<usize> {
    if data.len() < 1 + 8 + 1 {
        return None;
    }
    if data[0] != 2 {
        return None;
    }
    let mut p = 1usize;
    let mut rnd = 0usize;
    while p < data.len() && data[p] != 0 {
        p += 1;
        rnd += 1;
    }
    if p == data.len() {
        return None;
    }
    // data[p] == 0 here.
    if rnd < 8 {
        return None;
    }
    Some(p + 1)
}

/// `control_cksum` — `secret message` = `1 byte algo, sesskey, 2 byte cksum`;
/// the checksum covers the bytes between (algo ignored) and the trailing 2.
fn control_cksum(msg: &[u8]) -> Result<(), String> {
    if msg.len() < 3 {
        return Err(WRONG_KEY.to_string());
    }
    let mut my: u32 = 0;
    for &b in &msg[1..msg.len() - 2] {
        my += b as u32;
    }
    my &= 0xFFFF;
    let got = ((msg[msg.len() - 2] as u32) << 8) + msg[msg.len() - 1] as u32;
    if my != got {
        return Err(WRONG_KEY.to_string());
    }
    Ok(())
}

/// Normalize a decrypted MPI value into the `full_bytes`-wide buffer the C code
/// hands to `check_eme_pkcs1_v15`. In C the MPI is allocated at the modulus
/// bit-width and BN_bn2bin left-pads with zeros; the leading 0x02 sits at the
/// top. The recovered value `m` (with `bits = full_bytes*8 - 6`) therefore has
/// `data` of `full_bytes - 1` bytes whose first byte is 0x02 — we use it as is.
fn padded_bytes(m: &Mpi) -> Vec<u8> {
    m.data.clone()
}

/// `pgp_parse_pubenc_sesskey` — `body` is the tag-1 packet body. On success
/// returns `(cipher_algo, session_key)`.
pub fn parse_pubenc_sesskey(pk: &PubKey, body: &[u8]) -> Result<(i32, Vec<u8>), String> {
    if body.len() < 10 {
        return Err(CORRUPT_DATA.to_string());
    }
    if body[0] != 3 {
        return Err(CORRUPT_DATA.to_string());
    }
    let key_id = &body[1..9];
    let any_key = [0u8; 8];
    if key_id != any_key && key_id != pk.key_id {
        // "key_id's does not match" → PXE_PGP_WRONG_KEY ("Wrong key").
        return Err("Wrong key".to_string());
    }
    let algo = body[9] as i32;
    let mut pos = 10usize;

    let m: Mpi = match algo {
        PGP_PUB_ELG_ENCRYPT => {
            if pk.algo != PGP_PUB_ELG_ENCRYPT {
                return Err("Wrong key".to_string());
            }
            if let KeyMaterial::Elg { p, x, .. } = &pk.material {
                let x = x.as_ref().ok_or_else(|| WRONG_KEY.to_string())?;
                let c1 = read_mpi(body, &mut pos)?;
                let c2 = read_mpi(body, &mut pos)?;
                mpi::elgamal_decrypt(p, x, &c1, &c2)?
            } else {
                return Err("Wrong key".to_string());
            }
        }
        PGP_PUB_RSA_ENCRYPT | PGP_PUB_RSA_ENCRYPT_SIGN => {
            if pk.algo != PGP_PUB_RSA_ENCRYPT && pk.algo != PGP_PUB_RSA_ENCRYPT_SIGN {
                return Err("Wrong key".to_string());
            }
            if let KeyMaterial::Rsa { n, d, .. } = &pk.material {
                let d = d.as_ref().ok_or_else(|| WRONG_KEY.to_string())?;
                let c = read_mpi(body, &mut pos)?;
                mpi::rsa_decrypt(n, d, &c)
            } else {
                return Err("Wrong key".to_string());
            }
        }
        _ => return Err("Unknown public-key encryption algorithm".to_string()),
    };

    let padded = padded_bytes(&m);
    let off = check_eme_pkcs1_v15(&padded).ok_or_else(|| WRONG_KEY.to_string())?;
    let msg = &padded[off..];
    control_cksum(msg)?;

    let sess_key_len = msg.len() - 3;
    if sess_key_len > PGP_MAX_KEY {
        return Err("Session key too big".to_string());
    }
    let cipher_algo = msg[0] as i32;
    let sess_key = msg[1..1 + sess_key_len].to_vec();
    Ok((cipher_algo, sess_key))
}
