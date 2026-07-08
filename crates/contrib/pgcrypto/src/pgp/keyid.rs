//! `pgp_get_keyid` (pgp-info.c + the keyid part of pgp-pubkey.c) — extract the
//! 16-hex-char key id from a dearmored public/secret key or an encrypted
//! message. Needs only packet parsing + MPI reading + the SHA1 v4 fingerprint
//! (no RSA/ElGamal math).

use super::consts::*;
use super::packet::PktReader;

// Public-key algorithm ids (pgp.h).
const PGP_PUB_RSA_ENCRYPT_SIGN: i32 = 1;
const PGP_PUB_RSA_ENCRYPT: i32 = 2;
const PGP_PUB_RSA_SIGN: i32 = 3;
const PGP_PUB_ELG_ENCRYPT: i32 = 16;
const PGP_PUB_DSA_SIGN: i32 = 17;

const HEXTBL: &[u8; 16] = b"0123456789ABCDEF";

/// Read one MPI: 2-byte bit length, then ceil(bits/8) value bytes.
fn read_mpi(data: &[u8], pos: &mut usize) -> Result<Vec<u8>, ()> {
    if *pos + 2 > data.len() {
        return Err(());
    }
    let bits = ((data[*pos] as usize) << 8) | data[*pos + 1] as usize;
    *pos += 2;
    let bytes = (bits + 7) / 8;
    if *pos + bytes > data.len() {
        return Err(());
    }
    let v = data[*pos..*pos + bytes].to_vec();
    *pos += bytes;
    Ok(v)
}

struct PubKey {
    ver: u8,
    time: [u8; 4],
    algo: u8,
    mpis: Vec<Vec<u8>>,
    key_id: [u8; 8],
}

/// `_pgp_read_public_key` + `calc_key_id`. `body` is the full packet body.
fn read_public_key(body: &[u8]) -> Result<PubKey, ()> {
    if body.is_empty() {
        return Err(());
    }
    let ver = body[0];
    if ver != 4 {
        return Err(());
    }
    if body.len() < 6 {
        return Err(());
    }
    let mut time = [0u8; 4];
    time.copy_from_slice(&body[1..5]);
    let algo = body[5];
    let mut pos = 6usize;
    let mut mpis = Vec::new();
    match algo as i32 {
        PGP_PUB_DSA_SIGN => {
            for _ in 0..4 {
                mpis.push(read_mpi(body, &mut pos)?);
            }
        }
        PGP_PUB_RSA_SIGN | PGP_PUB_RSA_ENCRYPT | PGP_PUB_RSA_ENCRYPT_SIGN => {
            mpis.push(read_mpi(body, &mut pos)?); // n
            mpis.push(read_mpi(body, &mut pos)?); // e
        }
        PGP_PUB_ELG_ENCRYPT => {
            for _ in 0..3 {
                mpis.push(read_mpi(body, &mut pos)?);
            }
        }
        _ => return Err(()),
    }
    let mut pk = PubKey {
        ver,
        time,
        algo,
        mpis,
        key_id: [0u8; 8],
    };
    calc_key_id(&mut pk)?;
    Ok(pk)
}

/// `calc_key_id` — SHA1 over `0x99 || len2 || ver || time4 || algo || mpis`.
fn calc_key_id(pk: &mut PubKey) -> Result<(), ()> {
    let mut len = 1 + 4 + 1;
    for m in &pk.mpis {
        len += 2 + m.len();
    }
    let mut md = Digest::new(PGP_DIGEST_SHA1).ok_or(())?;
    let hdr = [0x99u8, (len >> 8) as u8, (len & 0xFF) as u8];
    md.update(&hdr);
    md.update(&[pk.ver]);
    md.update(&pk.time);
    md.update(&[pk.algo]);
    for m in &pk.mpis {
        let bits = mpi_bit_len(m);
        md.update(&[(bits >> 8) as u8, (bits & 0xFF) as u8]);
        md.update(m);
    }
    let hash = md.finish();
    pk.key_id.copy_from_slice(&hash[12..20]);
    Ok(())
}

/// Number of significant bits of an MPI value (big-endian, leading zeros
/// trimmed). MPIs are stored canonical, so usually the first byte is nonzero.
fn mpi_bit_len(v: &[u8]) -> usize {
    let mut i = 0;
    while i < v.len() && v[i] == 0 {
        i += 1;
    }
    if i >= v.len() {
        return 0;
    }
    let high = 8 - v[i].leading_zeros() as usize;
    high + (v.len() - i - 1) * 8
}

fn print_key(keyid: &[u8; 8]) -> String {
    let mut s = String::with_capacity(16);
    for &c in keyid {
        s.push(HEXTBL[((c >> 4) & 0x0F) as usize] as char);
        s.push(HEXTBL[(c & 0x0F) as usize] as char);
    }
    s
}

/// `pgp_get_keyid` — returns the 16-hex key id string, "SYMKEY", "ANYKEY", or
/// an error. `data` is the dearmored binary.
pub fn pgp_get_keyid(data: &[u8]) -> Result<String, &'static str> {
    let mut rdr = PktReader::new(data);
    let mut got_pub_key = 0i32;
    let mut got_pubenc_key = 0i32;
    let mut got_symenc_key = 0i32;
    let mut got_data = false;
    let mut got_main_key = false;
    let mut keyid_buf = [0u8; 8];

    loop {
        let hdr = match rdr.read_hdr().map_err(|_| CORRUPT_DATA)? {
            None => break,
            Some(h) => h,
        };
        match hdr.tag {
            t if t == PGP_PKT_SECRET_KEY || t == PGP_PKT_PUBLIC_KEY => {
                let _ = rdr.read_body(&hdr).map_err(|_| CORRUPT_DATA)?;
                if got_main_key {
                    return Err("Multiple key packets");
                }
                got_main_key = true;
            }
            t if t == PGP_PKT_SECRET_SUBKEY || t == PGP_PKT_PUBLIC_SUBKEY => {
                let body = rdr.read_body(&hdr).map_err(|_| CORRUPT_DATA)?;
                if let Ok(pk) = read_public_key(&body) {
                    let is_enc = matches!(
                        pk.algo as i32,
                        PGP_PUB_ELG_ENCRYPT | PGP_PUB_RSA_ENCRYPT | PGP_PUB_RSA_ENCRYPT_SIGN
                    );
                    if is_enc {
                        keyid_buf = pk.key_id;
                        got_pub_key += 1;
                    }
                }
            }
            t if t == PGP_PKT_PUBENC_SESSKEY => {
                let body = rdr.read_body(&hdr).map_err(|_| CORRUPT_DATA)?;
                got_pubenc_key += 1;
                if body.len() < 9 || body[0] != 3 {
                    return Err(CORRUPT_DATA);
                }
                keyid_buf.copy_from_slice(&body[1..9]);
            }
            t if t == PGP_PKT_SYMENC_DATA || t == PGP_PKT_SYMENC_DATA_MDC => {
                got_data = true;
            }
            t if t == PGP_PKT_SYMENC_SESSKEY => {
                let _ = rdr.read_body(&hdr).map_err(|_| CORRUPT_DATA)?;
                got_symenc_key += 1;
            }
            t if t == PGP_PKT_SIGNATURE
                || t == PGP_PKT_MARKER
                || t == PGP_PKT_TRUST
                || t == PGP_PKT_USER_ID
                || t == PGP_PKT_USER_ATTR
                || t == PGP_PKT_PRIV_61 =>
            {
                let _ = rdr.read_body(&hdr).map_err(|_| CORRUPT_DATA)?;
            }
            _ => return Err(CORRUPT_DATA),
        }
        if got_data {
            break;
        }
    }

    if got_pub_key > 0 && got_pubenc_key > 0 {
        return Err(CORRUPT_DATA);
    }
    if got_pub_key > 1 || got_pubenc_key > 1 {
        return Err("Multiple key packets");
    }

    if got_pubenc_key > 0 || got_pub_key > 0 {
        if keyid_buf == [0u8; 8] {
            Ok("ANYKEY".to_string())
        } else {
            Ok(print_key(&keyid_buf))
        }
    } else if got_symenc_key > 0 {
        Ok("SYMKEY".to_string())
    } else {
        Err(NO_USABLE_KEY)
    }
}

use super::consts::Digest;
