//! OpenPGP public/secret key parsing (pgp-pubkey.c) — read a dearmored key
//! block, locate the single usable encryption subkey, and (for secret keys)
//! S2K-decrypt the protected secret MPIs. RSA (n,e / d,p,q,u) and ElGamal
//! (p,g,y / x) only; DSA/sign-only keys are parsed but never usable for encrypt.

use super::cfb::PgpCfb;
use super::consts::*;
use super::mpi::{mpi_cksum, read_mpi, Mpi};
use super::packet::PktReader;
use super::s2k::S2k;

// Public-key algorithm ids (pgp.h).
pub const PGP_PUB_RSA_ENCRYPT_SIGN: i32 = 1;
pub const PGP_PUB_RSA_ENCRYPT: i32 = 2;
pub const PGP_PUB_RSA_SIGN: i32 = 3;
pub const PGP_PUB_ELG_ENCRYPT: i32 = 16;
pub const PGP_PUB_DSA_SIGN: i32 = 17;

// Secret-key protection ("hide") types.
const HIDE_CLEAR: i32 = 0;
const HIDE_CKSUM: i32 = 255;
const HIDE_SHA1: i32 = 254;

/// The per-algorithm key material. Public components are always present;
/// secret components are present only after `process_secret_key`.
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
    /// DSA / sign-only RSA — parsed for key-id but never `can_encrypt`.
    SignOnly,
}

/// A parsed OpenPGP key (`PGP_PubKey`).
#[derive(Clone)]
pub struct PubKey {
    // `ver`/`time` are retained for struct fidelity with C's PGP_PubKey (they
    // feed `calc_key_id` at parse time and are not read again afterward).
    #[allow(dead_code)]
    pub ver: u8,
    #[allow(dead_code)]
    pub time: [u8; 4],
    pub algo: i32,
    pub key_id: [u8; 8],
    pub can_encrypt: bool,
    pub material: KeyMaterial,
}

/// `_pgp_read_public_key` + `calc_key_id`. `body`/`pos` is a cursor into the
/// packet body (secret-key packets reuse this for their public prefix).
fn read_public_key(body: &[u8], pos: &mut usize) -> Result<PubKey, String> {
    if *pos + 6 > body.len() {
        return Err(CORRUPT_DATA.to_string());
    }
    let ver = body[*pos];
    if ver != 4 {
        return Err("Only V4 key packets are supported".to_string());
    }
    let mut time = [0u8; 4];
    time.copy_from_slice(&body[*pos + 1..*pos + 5]);
    let algo = body[*pos + 5] as i32;
    *pos += 6;

    let (material, can_encrypt, pub_mpis): (KeyMaterial, bool, Vec<Mpi>) = match algo {
        PGP_PUB_DSA_SIGN => {
            let p = read_mpi(body, pos)?;
            let q = read_mpi(body, pos)?;
            let g = read_mpi(body, pos)?;
            let y = read_mpi(body, pos)?;
            (KeyMaterial::SignOnly, false, vec![p, q, g, y])
        }
        PGP_PUB_RSA_SIGN | PGP_PUB_RSA_ENCRYPT | PGP_PUB_RSA_ENCRYPT_SIGN => {
            let n = read_mpi(body, pos)?;
            let e = read_mpi(body, pos)?;
            let can = algo != PGP_PUB_RSA_SIGN;
            let pubm = vec![n.clone(), e.clone()];
            let mat = if can {
                KeyMaterial::Rsa {
                    n,
                    e,
                    d: None,
                    p: None,
                    q: None,
                    u: None,
                }
            } else {
                KeyMaterial::SignOnly
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
        _ => return Err("Unknown public-key encryption algorithm".to_string()),
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

/// `calc_key_id` — SHA1 over `0x99 || len2 || ver || time4 || algo || pub mpis`,
/// taking the low 8 bytes of the fingerprint.
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

/// `process_secret_key` — parse the public prefix, then (optionally S2K-decrypt
/// and) read the secret MPIs, verifying the trailing checksum / SHA1.
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

    // The cleartext secret bytes (after any CFB decryption) and whether the
    // trailing integrity field is a 20-byte SHA1 (vs 2-byte checksum).
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

        let mut cfb = PgpCfb::create(cipher_algo, &s2k.key, false, Some(iv))
            .map_err(|e| e.to_string())?;
        let dec = cfb.decrypt(&body[pos..]);
        (dec, hide_type == HIDE_SHA1)
    } else if hide_type == HIDE_CLEAR {
        (body[pos..].to_vec(), false)
    } else {
        return Err("Corrupt key packet".to_string());
    };

    // Read the secret MPIs out of the (decrypted) secret region.
    let mut sp = 0usize;
    match &mut pk.material {
        KeyMaterial::Rsa { d, p, q, u, .. } => {
            *d = Some(read_mpi(&sec_bytes, &mut sp)?);
            *p = Some(read_mpi(&sec_bytes, &mut sp)?);
            *q = Some(read_mpi(&sec_bytes, &mut sp)?);
            *u = Some(read_mpi(&sec_bytes, &mut sp)?);
        }
        KeyMaterial::Elg { x, .. } => {
            *x = Some(read_mpi(&sec_bytes, &mut sp)?);
        }
        KeyMaterial::SignOnly => {
            // DSA/sign-only: skip the single secret MPI (never used for encrypt).
            let _ = read_mpi(&sec_bytes, &mut sp)?;
        }
    }

    // Verify the trailing integrity field over the secret MPIs.
    if sha1_mode {
        check_key_sha1(&pk, &sec_bytes, sp)?;
    } else {
        check_key_cksum(&pk, &sec_bytes, sp)?;
    }

    Ok(pk)
}

/// `check_key_sha1` — SHA1 over the secret MPIs must match the trailing 20 bytes.
fn check_key_sha1(pk: &PubKey, sec: &[u8], at: usize) -> Result<(), String> {
    if at + 20 > sec.len() {
        return Err("Wrong key or corrupt data".to_string());
    }
    let got = &sec[at..at + 20];
    let mut md = Digest::new(PGP_DIGEST_SHA1).ok_or(UNSUPPORTED_HASH.to_string())?;
    for m in secret_mpis(pk) {
        md.update(&[(m.bits >> 8) as u8, (m.bits & 0xFF) as u8]);
        md.update(&m.data);
    }
    if md.finish() != got {
        return Err("Wrong key or corrupt data".to_string());
    }
    Ok(())
}

/// `check_key_cksum` — 16-bit sum over the secret MPIs must match 2 trailing bytes.
fn check_key_cksum(pk: &PubKey, sec: &[u8], at: usize) -> Result<(), String> {
    if at + 2 > sec.len() {
        return Err("Wrong key or corrupt data".to_string());
    }
    let got = ((sec[at] as u32) << 8) + sec[at + 1] as u32;
    let mut my = 0u32;
    for m in secret_mpis(pk) {
        my = mpi_cksum(my, m);
    }
    if my != got {
        return Err("Wrong key or corrupt data".to_string());
    }
    Ok(())
}

/// The secret MPIs in the C checksum order (RSA: d,p,q,u; ElGamal: x).
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
        KeyMaterial::Elg { x, .. } => x.iter().collect(),
        KeyMaterial::SignOnly => Vec::new(),
    }
}

/// `internal_read_key` / `pgp_set_pubkey` — scan the key block for the single
/// usable encryption subkey. `pubtype` 0 = expect a public key (encrypt path),
/// 1 = expect a secret key (decrypt path).
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
                    return Err(
                        "Several keys given - pgcrypto does not handle keyring".to_string(),
                    );
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
