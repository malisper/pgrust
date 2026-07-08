
use super::consts::*;
use super::mpi::{self, write_mpi, Mpi};
use super::packet::write_packet;
use super::pubkey::{KeyMaterial, PubKey, PGP_PUB_ELG_ENCRYPT, PGP_PUB_RSA_ENCRYPT,
    PGP_PUB_RSA_ENCRYPT_SIGN};
use ::pg_strong_random::pg_strong_random;

/// `res_len` bytes (pad must be >= 8 nonzero random bytes).
fn pad_eme_pkcs1_v15(data: &[u8], res_len: usize) -> Result<Vec<u8>, String> {
    if res_len < data.len() + 2 {
        return Err("pgcrypto bug".to_string());
    }
    let pad_len = res_len - 2 - data.len();
    if pad_len < 8 {
        return Err("pgcrypto bug".to_string());
    }
    let mut buf = vec![0u8; res_len];
    buf[0] = 0x02;
    if !pg_strong_random(&mut buf[1..1 + pad_len]) {
        return Err("Failed to generate strong random bits".to_string());
    }
    for i in 1..1 + pad_len {
        while buf[i] == 0 {
            let mut one = [0u8; 1];
            if !pg_strong_random(&mut one) {
                return Err("Failed to generate strong random bits".to_string());
            }
            buf[i] = one[0];
        }
    }
    buf[pad_len + 1] = 0;
    buf[pad_len + 2..].copy_from_slice(data);
    Ok(buf)
}

fn create_secmsg(cipher_algo: i32, sess_key: &[u8], full_bytes: usize) -> Result<Mpi, String> {
    let klen = sess_key.len();
    let mut cksum: u32 = 0;
    for &b in sess_key {
        cksum += b as u32;
    }
    let mut secmsg = Vec::with_capacity(klen + 3);
    secmsg.push(cipher_algo as u8);
    secmsg.extend_from_slice(sess_key);
    secmsg.push(((cksum >> 8) & 0xFF) as u8);
    secmsg.push((cksum & 0xFF) as u8);

    let padded = pad_eme_pkcs1_v15(&secmsg, full_bytes)?;
    let full_bits = full_bytes * 8 - 6;
    Ok(Mpi::from_bytes(padded, full_bits))
}

pub fn write_pubenc_sesskey(
    out: &mut Vec<u8>,
    pk: &PubKey,
    cipher_algo: i32,
    sess_key: &[u8],
) -> Result<(), String> {
    let mut body = Vec::new();
    body.push(3); // version
    body.extend_from_slice(&pk.key_id);
    body.push(pk.algo as u8);

    match pk.algo {
        PGP_PUB_ELG_ENCRYPT => {
            if let KeyMaterial::Elg { p, g, y, .. } = &pk.material {
                let m = create_secmsg(cipher_algo, sess_key, p.nbytes() - 1)?;
                let (c1, c2) = mpi::elgamal_encrypt(p, g, y, &m)?;
                write_mpi(&mut body, &c1);
                write_mpi(&mut body, &c2);
            } else {
                return Err("pgcrypto bug".to_string());
            }
        }
        PGP_PUB_RSA_ENCRYPT | PGP_PUB_RSA_ENCRYPT_SIGN => {
            if let KeyMaterial::Rsa { n, e, .. } = &pk.material {
                let m = create_secmsg(cipher_algo, sess_key, n.nbytes() - 1)?;
                let c = mpi::rsa_encrypt(n, e, &m);
                write_mpi(&mut body, &c);
            } else {
                return Err("pgcrypto bug".to_string());
            }
        }
        _ => return Err("Unknown public-key encryption algorithm".to_string()),
    }

    write_packet(out, PGP_PKT_PUBENC_SESSKEY, &body);
    Ok(())
}
