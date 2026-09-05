//! pgp-info.c: pgp_key_id() — walks the packet stream like C pgp_get_keyid
//! (pgp-info.c:112) and reports the encryption (sub)key id, "ANYKEY",
//! "SYMKEY", or the first packet error.

use super::consts::*;
use super::packet::PktReader;
use super::pubkey::{read_public_key, PGP_PUB_ELG_ENCRYPT, PGP_PUB_RSA_ENCRYPT,
    PGP_PUB_RSA_ENCRYPT_SIGN};

const HEXTBL: &[u8; 16] = b"0123456789ABCDEF";

/// pgp-info.c:37 read_pubkey_keyid: the public part is parsed by
/// _pgp_read_public_key (its errors — NOT_V4, UNKNOWN_PUBALGO, CORRUPT_DATA
/// — propagate); the rest of the packet is skipped. `Some(id)` iff the key
/// can encrypt.
fn read_pubkey_keyid(body: &[u8]) -> Result<Option<[u8; 8]>, String> {
    let mut pos = 0usize;
    let pk = read_public_key(body, &mut pos)?;
    Ok(match pk.algo {
        PGP_PUB_ELG_ENCRYPT | PGP_PUB_RSA_ENCRYPT | PGP_PUB_RSA_ENCRYPT_SIGN => Some(pk.key_id),
        _ => None,
    })
}

/// pgp-info.c:71 read_pubenc_keyid. A version other than 3 returns the raw
/// -1 in C, which px_strerror renders as PXE_NO_HASH's text; kept verbatim.
fn read_pubenc_keyid(body: &[u8]) -> Result<[u8; 8], String> {
    let ver = *body.first().ok_or_else(|| CORRUPT_DATA.to_string())?;
    if ver != 3 {
        return Err("No such hash algorithm".to_string());
    }
    if body.len() < 9 {
        return Err(CORRUPT_DATA.to_string());
    }
    let mut id = [0u8; 8];
    id.copy_from_slice(&body[1..9]);
    Ok(id)
}

fn print_key(keyid: &[u8; 8]) -> String {
    let mut s = String::with_capacity(16);
    for &c in keyid {
        s.push(HEXTBL[((c >> 4) & 0x0F) as usize] as char);
        s.push(HEXTBL[(c & 0x0F) as usize] as char);
    }
    s
}

pub fn pgp_get_keyid(data: &[u8]) -> Result<String, String> {
    // pgp-info.c:133: allow_ctx = 0 (PktReader::new rejects lentype 3).
    let mut rdr = PktReader::new(data);
    let mut got_pub_key = 0i32;
    let mut got_pubenc_key = 0i32;
    let mut got_symenc_key = 0i32;
    let mut got_data = false;
    let mut got_main_key = false;
    let mut keyid_buf = [0u8; 8];
    let corrupt = || CORRUPT_DATA.to_string();

    loop {
        let hdr = match rdr.read_hdr().map_err(|_| corrupt())? {
            None => break,
            Some(h) => h,
        };
        match hdr.tag {
            t if t == PGP_PKT_SECRET_KEY || t == PGP_PKT_PUBLIC_KEY => {
                // main key is for signing, so ignore it
                if got_main_key {
                    return Err(MULTIPLE_KEYS.to_string());
                }
                got_main_key = true;
                let _ = rdr.read_body(&hdr).map_err(|_| corrupt())?;
            }
            t if t == PGP_PKT_SECRET_SUBKEY || t == PGP_PKT_PUBLIC_SUBKEY => {
                let body = rdr.read_body(&hdr).map_err(|_| corrupt())?;
                if let Some(id) = read_pubkey_keyid(&body)? {
                    keyid_buf = id;
                    got_pub_key += 1;
                }
            }
            t if t == PGP_PKT_PUBENC_SESSKEY => {
                let body = rdr.read_body(&hdr).map_err(|_| corrupt())?;
                got_pubenc_key += 1;
                keyid_buf = read_pubenc_keyid(&body)?;
            }
            t if t == PGP_PKT_SYMENC_DATA || t == PGP_PKT_SYMENC_DATA_MDC => {
                // don't skip it, just stop
                got_data = true;
            }
            t if t == PGP_PKT_SYMENC_SESSKEY => {
                got_symenc_key += 1;
                let _ = rdr.read_body(&hdr).map_err(|_| corrupt())?;
            }
            t if t == PGP_PKT_SIGNATURE
                || t == PGP_PKT_MARKER
                || t == PGP_PKT_TRUST
                || t == PGP_PKT_USER_ID
                || t == PGP_PKT_USER_ATTR
                || t == PGP_PKT_PRIV_61 =>
            {
                let _ = rdr.read_body(&hdr).map_err(|_| corrupt())?;
            }
            _ => return Err(corrupt()),
        }
        if got_data {
            break;
        }
    }

    // now check sanity (pgp-info.c:201-208; sequential assignments, the
    // last one wins)
    let mut err = None;
    if got_pub_key > 0 && got_pubenc_key > 0 {
        err = Some(corrupt());
    }
    if got_pub_key > 1 || got_pubenc_key > 1 {
        err = Some(MULTIPLE_KEYS.to_string());
    }
    if let Some(e) = err {
        return Err(e);
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
        Err(NO_USABLE_KEY.to_string())
    }
}
