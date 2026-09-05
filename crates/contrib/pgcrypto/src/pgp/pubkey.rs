
use super::cfb::PgpCfb;
use super::consts::*;
use super::mpi::{mpi_cksum, read_mpi, Mpi};
use super::packet::PktReader;
use super::s2k::S2k;

pub const PGP_PUB_RSA_ENCRYPT_SIGN: i32 = 1;
pub const PGP_PUB_RSA_ENCRYPT: i32 = 2;
pub const PGP_PUB_RSA_SIGN: i32 = 3;
pub const PGP_PUB_ELG_ENCRYPT: i32 = 16;
pub const PGP_PUB_DSA_SIGN: i32 = 17;

const HIDE_CLEAR: i32 = 0;
const HIDE_CKSUM: i32 = 255;
const HIDE_SHA1: i32 = 254;

#[derive(Clone)]
pub enum KeyMaterial {
    Rsa {
        n: Mpi,
        e: Mpi,
        d: Option<Mpi>,
        p: Option<Mpi>,
        q: Option<Mpi>,
        u: Option<Mpi>,
    },
    Elg {
        p: Mpi,
        g: Mpi,
        y: Mpi,
        x: Option<Mpi>,
    },
    /// DSA (sign-only): the public MPIs only feed calc_key_id; the secret
    /// `x` is read and checksummed like C (pgp-pubkey.c:434) so a signing
    /// subkey is validated then skipped, not mistaken for corrupt data.
    Dsa {
        x: Option<Mpi>,
    },
}

#[derive(Clone)]
pub struct PubKey {
    #[allow(dead_code)]
    pub ver: u8,
    #[allow(dead_code)]
    pub time: [u8; 4],
    pub algo: i32,
    pub key_id: [u8; 8],
    pub can_encrypt: bool,
    pub material: KeyMaterial,
}

/// pgp-pubkey.c:158 _pgp_read_public_key — field by field, in C's order:
/// the version byte is read and checked (PXE_PGP_NOT_V4_KEYPKT) before the
/// time/algo bytes are required, and every short read is PXE_PGP_CORRUPT_DATA
/// (pullf_read_fixed).
pub(super) fn read_public_key(body: &[u8], pos: &mut usize) -> Result<PubKey, String> {
    let ver = *body.get(*pos).ok_or_else(|| CORRUPT_DATA.to_string())?;
    *pos += 1;
    if ver != 4 {
        return Err(NOT_V4_KEYPKT.to_string());
    }
    if *pos + 4 > body.len() {
        return Err(CORRUPT_DATA.to_string());
    }
    let mut time = [0u8; 4];
    time.copy_from_slice(&body[*pos..*pos + 4]);
    *pos += 4;
    let algo = *body.get(*pos).ok_or_else(|| CORRUPT_DATA.to_string())? as i32;
    *pos += 1;

    let (material, can_encrypt, pub_mpis): (KeyMaterial, bool, Vec<Mpi>) = match algo {
        PGP_PUB_DSA_SIGN => {
            let p = read_mpi(body, pos)?;
            let q = read_mpi(body, pos)?;
            let g = read_mpi(body, pos)?;
            let y = read_mpi(body, pos)?;
            (KeyMaterial::Dsa { x: None }, false, vec![p, q, g, y])
        }
        PGP_PUB_RSA_SIGN | PGP_PUB_RSA_ENCRYPT | PGP_PUB_RSA_ENCRYPT_SIGN => {
            let n = read_mpi(body, pos)?;
            let e = read_mpi(body, pos)?;
            // pgp-pubkey.c:214: only PGP_PUB_RSA_SIGN cannot encrypt; the
            // secret material (d, p, q, u) is read for all three.
            let can = algo != PGP_PUB_RSA_SIGN;
            let pubm = vec![n.clone(), e.clone()];
            let mat = KeyMaterial::Rsa {
                n,
                e,
                d: None,
                p: None,
                q: None,
                u: None,
            };
            (mat, can, pubm)
        }
        PGP_PUB_ELG_ENCRYPT => {
            let p = read_mpi(body, pos)?;
            let g = read_mpi(body, pos)?;
            let y = read_mpi(body, pos)?;
            let pubm = vec![p.clone(), g.clone(), y.clone()];
            (
                KeyMaterial::Elg {
                    p,
                    g,
                    y,
                    x: None,
                },
                true,
                pubm,
            )
        }
        _ => return Err(UNKNOWN_PUBALGO.to_string()),
    };

    let key_id = calc_key_id(ver, &time, algo, &pub_mpis)?;
    Ok(PubKey {
        ver,
        time,
        algo,
        key_id,
        can_encrypt,
        material,
    })
}

fn calc_key_id(ver: u8, time: &[u8; 4], algo: i32, mpis: &[Mpi]) -> Result<[u8; 8], String> {
    let mut len = 1 + 4 + 1usize;
    for m in mpis {
        len += 2 + m.nbytes();
    }
    let mut md = Digest::new(PGP_DIGEST_SHA1).ok_or(UNSUPPORTED_HASH.to_string())?;
    let hdr = [0x99u8, (len >> 8) as u8, (len & 0xFF) as u8];
    md.update(&hdr);
    md.update(&[ver]);
    md.update(time);
    md.update(&[algo as u8]);
    for m in mpis {
        md.update(&[(m.bits >> 8) as u8, (m.bits & 0xFF) as u8]);
        md.update(&m.data);
    }
    let hash = md.finish();
    let mut id = [0u8; 8];
    id.copy_from_slice(&hash[12..20]);
    Ok(id)
}

fn process_secret_key(
    body: &[u8],
    psw: Option<&[u8]>,
) -> Result<PubKey, String> {
    let mut pos = 0usize;
    let mut pk = read_public_key(body, &mut pos)?;

    if pos >= body.len() {
        return Err(CORRUPT_DATA.to_string());
    }
    let hide_type = body[pos] as i32;
    pos += 1;

    let (sec_bytes, sha1_mode) = if hide_type == HIDE_SHA1 || hide_type == HIDE_CKSUM {
        let psw = psw.ok_or_else(|| "Need password for secret key".to_string())?;
        if pos >= body.len() {
            return Err(CORRUPT_DATA.to_string());
        }
        let cipher_algo = body[pos] as i32;
        pos += 1;
        let (mut s2k, consumed) = S2k::read(&body[pos..]).map_err(|e| e.to_string())?;
        pos += consumed;
        s2k.process(cipher_algo, psw).map_err(|e| e.to_string())?;

        let bs = cipher_block_size(cipher_algo);
        if bs == 0 {
            return Err(UNSUPPORTED_CIPHER.to_string());
        }
        if pos + bs > body.len() {
            return Err(CORRUPT_DATA.to_string());
        }
        let iv = &body[pos..pos + bs];
        pos += bs;

        // upstream 4c5128ca0b30 (18.6): pgcrypto: Add option to revert to prior decryption behavior
        // (never for the secret-key unlock: pgcrypto did not produce this ciphertext)
        let mut cfb = PgpCfb::create(cipher_algo, &s2k.key, false, Some(iv), false)
            .map_err(|e| e.to_string())?;
        let dec = cfb.decrypt(&body[pos..]);
        (dec, hide_type == HIDE_SHA1)
    } else if hide_type == HIDE_CLEAR {
        (body[pos..].to_vec(), false)
    } else {
        return Err(KEYPKT_CORRUPT.to_string());
    };

    // pgp-pubkey.c:412: the secret MPIs of EVERY supported algorithm are
    // read (RSA sign/encrypt/both: d, p, q, u; Elgamal: x; DSA: x).
    let mut sp = 0usize;
    match &mut pk.material {
        KeyMaterial::Rsa { d, p, q, u, .. } => {
            *d = Some(read_mpi(&sec_bytes, &mut sp)?);
            *p = Some(read_mpi(&sec_bytes, &mut sp)?);
            *q = Some(read_mpi(&sec_bytes, &mut sp)?);
            *u = Some(read_mpi(&sec_bytes, &mut sp)?);
        }
        KeyMaterial::Elg { x, .. } | KeyMaterial::Dsa { x } => {
            *x = Some(read_mpi(&sec_bytes, &mut sp)?);
        }
    }

    let end = if sha1_mode {
        check_key_sha1(&pk, &sec_bytes, sp)?
    } else {
        check_key_cksum(&pk, &sec_bytes, sp)?
    };
    // pgp-pubkey.c:449 pgp_expect_packet_end: anything after the checksum
    // is PXE_PGP_CORRUPT_DATA.
    if end != sec_bytes.len() {
        return Err(CORRUPT_DATA.to_string());
    }

    Ok(pk)
}

/// pgp-pubkey.c:253 check_key_sha1 — SHA1 over the secret MPIs must match
/// the trailing 20 bytes; a mismatch is PXE_PGP_KEYPKT_CORRUPT (:290), a
/// short read PXE_PGP_CORRUPT_DATA. Returns the position after the digest.
fn check_key_sha1(pk: &PubKey, sec: &[u8], at: usize) -> Result<usize, String> {
    if at + 20 > sec.len() {
        return Err(CORRUPT_DATA.to_string());
    }
    let got = &sec[at..at + 20];
    let mut md = Digest::new(PGP_DIGEST_SHA1).ok_or(UNSUPPORTED_HASH.to_string())?;
    for m in secret_mpis(pk) {
        md.update(&[(m.bits >> 8) as u8, (m.bits & 0xFF) as u8]);
        md.update(&m.data);
    }
    if md.finish() != got {
        return Err(KEYPKT_CORRUPT.to_string());
    }
    Ok(at + 20)
}

/// pgp-pubkey.c:299 check_key_cksum — 16-bit sum over the secret MPIs must
/// match the 2 trailing bytes; a mismatch is PXE_PGP_KEYPKT_CORRUPT (:331).
/// Returns the position after the checksum.
fn check_key_cksum(pk: &PubKey, sec: &[u8], at: usize) -> Result<usize, String> {
    if at + 2 > sec.len() {
        return Err(CORRUPT_DATA.to_string());
    }
    let got = ((sec[at] as u32) << 8) + sec[at + 1] as u32;
    let mut my = 0u32;
    for m in secret_mpis(pk) {
        my = mpi_cksum(my, m);
    }
    if my != got {
        return Err(KEYPKT_CORRUPT.to_string());
    }
    Ok(at + 2)
}

fn secret_mpis(pk: &PubKey) -> Vec<&Mpi> {
    match &pk.material {
        KeyMaterial::Rsa { d, p, q, u, .. } => {
            let mut v = Vec::new();
            for o in [d, p, q, u] {
                if let Some(m) = o {
                    v.push(m);
                }
            }
            v
        }
        KeyMaterial::Elg { x, .. } | KeyMaterial::Dsa { x } => x.iter().collect(),
    }
}

pub fn read_key(data: &[u8], psw: Option<&[u8]>, pubtype: i32) -> Result<PubKey, String> {
    let mut rdr = PktReader::new(data);
    let mut enc_key: Option<PubKey> = None;
    let mut got_main_key = false;

    loop {
        let hdr = match rdr.read_hdr().map_err(|_| CORRUPT_DATA.to_string())? {
            None => break,
            Some(h) => h,
        };
        let body = rdr.read_body(&hdr).map_err(|_| CORRUPT_DATA.to_string())?;

        let mut found: Option<PubKey> = None;
        match hdr.tag {
            t if t == PGP_PKT_PUBLIC_KEY || t == PGP_PKT_SECRET_KEY => {
                if got_main_key {
                    return Err(MULTIPLE_KEYS.to_string());
                }
                got_main_key = true;
            }
            t if t == PGP_PKT_PUBLIC_SUBKEY => {
                if pubtype != 0 {
                    return Err("Cannot decrypt with public key".to_string());
                }
                let mut pos = 0;
                found = Some(read_public_key(&body, &mut pos)?);
            }
            t if t == PGP_PKT_SECRET_SUBKEY => {
                if pubtype != 1 {
                    return Err("Refusing to encrypt with secret key".to_string());
                }
                found = Some(process_secret_key(&body, psw)?);
            }
            t if t == PGP_PKT_SIGNATURE
                || t == PGP_PKT_MARKER
                || t == PGP_PKT_TRUST
                || t == PGP_PKT_USER_ID
                || t == PGP_PKT_USER_ATTR
                || t == PGP_PKT_PRIV_61 => {}
            _ => return Err("Unexpected packet in key data".to_string()),
        }

        if let Some(pk) = found {
            if pk.can_encrypt {
                if enc_key.is_none() {
                    enc_key = Some(pk);
                } else {
                    return Err("Several subkeys not supported".to_string());
                }
            }
        }
    }

    enc_key.ok_or_else(|| NO_USABLE_KEY.to_string())
}

use super::consts::Digest;
