//! `pgp_decrypt` (pgp-decrypt.c) — the symmetric-passphrase OpenPGP decrypt
//! pipeline (the public-key session-key path is not ported).

use super::cfb::PgpCfb;
use super::consts::*;
use super::context::PgpContext;
use super::packet::PktReader;
use super::s2k::S2k;

struct SessKey {
    cipher: i32,
    key: Vec<u8>,
}

/// `pgp_decrypt` for the symmetric path. `ctx` is updated in place with the
/// observed cipher/s2k/compress parameters (for the expect-* checks).
pub fn decrypt_symmetric(
    ctx: &mut PgpContext,
    data: &[u8],
    passphrase: &[u8],
) -> Result<Vec<u8>, String> {
    let mut rdr = PktReader::new(data);
    let mut sess: Option<SessKey> = None;

    loop {
        let hdr = match rdr.read_hdr().map_err(|_| CORRUPT_DATA.to_string())? {
            None => break,
            Some(h) => h,
        };
        match hdr.tag {
            t if t == PGP_PKT_MARKER => {
                let _ = rdr.read_body(&hdr).map_err(|_| CORRUPT_DATA.to_string())?;
            }
            t if t == PGP_PKT_SYMENC_SESSKEY => {
                let body = rdr.read_body(&hdr).map_err(|_| CORRUPT_DATA.to_string())?;
                sess = Some(parse_symenc_sesskey(ctx, &body, passphrase)?);
            }
            t if t == PGP_PKT_SYMENC_DATA || t == PGP_PKT_SYMENC_DATA_MDC => {
                let body = rdr.read_body(&hdr).map_err(|_| CORRUPT_DATA.to_string())?;
                let sk = sess.as_ref().ok_or_else(|| WRONG_KEY.to_string())?;
                let mdc = t == PGP_PKT_SYMENC_DATA_MDC;
                let inner = decrypt_data_packet(sk, &body, mdc)?;
                ctx.disable_mdc = if mdc { 0 } else { 1 };
                return finish_inner(ctx, inner);
            }
            t if t == PGP_PKT_PUBENC_SESSKEY => {
                return Err(WRONG_KEY.to_string());
            }
            _ => {
                let _ = rdr.read_body(&hdr).map_err(|_| CORRUPT_DATA.to_string())?;
            }
        }
    }
    Err(WRONG_KEY.to_string())
}

/// Parse a tag-3 symmetric-key ESK packet and recover the session key.
fn parse_symenc_sesskey(
    ctx: &mut PgpContext,
    body: &[u8],
    passphrase: &[u8],
) -> Result<SessKey, String> {
    if body.len() < 4 || body[0] != 4 {
        return Err(CORRUPT_DATA.to_string());
    }
    let s2k_cipher = body[1] as i32;
    let (mut s2k, consumed) = S2k::read(&body[2..]).map_err(|e| e.to_string())?;
    s2k.process(s2k_cipher, passphrase).map_err(|e| e.to_string())?;

    ctx.s2k_mode = s2k.mode;
    ctx.s2k_digest_algo = s2k.digest_algo;
    ctx.s2k_cipher_algo = s2k_cipher;
    if s2k.mode == PGP_S2K_ISALTED {
        ctx.s2k_count = s2k_decode_count(s2k.iter as i32);
    }

    let rest = &body[2 + consumed..];
    if rest.is_empty() {
        ctx.use_sess_key = 0;
        ctx.cipher_algo = s2k_cipher;
        Ok(SessKey {
            cipher: s2k_cipher,
            key: s2k.key,
        })
    } else {
        ctx.use_sess_key = 1;
        let mut cfb = PgpCfb::create(s2k_cipher, &s2k.key, false, None).map_err(|e| e.to_string())?;
        let dec = cfb.decrypt(rest);
        if dec.is_empty() {
            return Err(CORRUPT_DATA.to_string());
        }
        let cipher = dec[0] as i32;
        let klen = cipher_key_size(cipher);
        if klen == 0 || dec.len() < 1 + klen {
            return Err(CORRUPT_DATA.to_string());
        }
        ctx.cipher_algo = cipher;
        Ok(SessKey {
            cipher,
            key: dec[1..1 + klen].to_vec(),
        })
    }
}

/// CFB-decrypt the symmetrically-encrypted data packet, verify the prefix
/// (and MDC if present), and return the inner packet stream.
fn decrypt_data_packet(sk: &SessKey, body: &[u8], mdc: bool) -> Result<Vec<u8>, String> {
    let bs = cipher_block_size(sk.cipher);
    let ct = if mdc {
        if body.is_empty() || body[0] != 0x01 {
            return Err(CORRUPT_DATA.to_string());
        }
        &body[1..]
    } else {
        body
    };

    let resync = !mdc;
    let mut cfb = PgpCfb::create(sk.cipher, &sk.key, resync, None).map_err(|e| e.to_string())?;
    let plain = cfb.decrypt(ct);

    if plain.len() < bs + 2 {
        return Err(WRONG_KEY.to_string());
    }
    if plain[bs - 2] != plain[bs] || plain[bs - 1] != plain[bs + 1] {
        return Err(WRONG_KEY.to_string());
    }

    let inner_start = bs + 2;
    if mdc {
        if plain.len() < inner_start + 2 + MDC_DIGEST_LEN {
            return Err(CORRUPT_DATA.to_string());
        }
        let mdc_off = plain.len() - (2 + MDC_DIGEST_LEN);
        if plain[mdc_off] != 0xD3 || plain[mdc_off + 1] != 0x14 {
            return Err("Not MDC packet".to_string());
        }
        let mut md = Digest::new(PGP_DIGEST_SHA1).ok_or(UNSUPPORTED_HASH.to_string())?;
        md.update(&plain[..mdc_off + 2]);
        let want = md.finish();
        if want != plain[mdc_off + 2..mdc_off + 2 + MDC_DIGEST_LEN] {
            return Err("MDC mismatch".to_string());
        }
        Ok(plain[inner_start..mdc_off].to_vec())
    } else {
        Ok(plain[inner_start..].to_vec())
    }
}

/// Parse the inner decrypted stream (optionally compressed) down to the
/// literal-data payload.
fn finish_inner(ctx: &mut PgpContext, inner: Vec<u8>) -> Result<Vec<u8>, String> {
    let mut rdr = PktReader::new(&inner);
    let hdr = rdr
        .read_hdr()
        .map_err(|_| CORRUPT_DATA.to_string())?
        .ok_or_else(|| CORRUPT_DATA.to_string())?;
    if hdr.tag == PGP_PKT_COMPRESSED_DATA {
        let body = rdr.read_body(&hdr).map_err(|_| CORRUPT_DATA.to_string())?;
        if body.is_empty() {
            return Err(CORRUPT_DATA.to_string());
        }
        let algo = body[0] as i32;
        ctx.compress_algo = algo;
        let decompressed = match algo {
            PGP_COMPR_NONE => body[1..].to_vec(),
            PGP_COMPR_ZIP => super::compress::inflate_raw(&body[1..])
                .map_err(|_| "decompression failed".to_string())?,
            PGP_COMPR_ZLIB => super::compress::inflate_zlib(&body[1..])
                .map_err(|_| "decompression failed".to_string())?,
            _ => return Err(UNSUPPORTED_COMPR.to_string()),
        };
        return read_literal(ctx, &decompressed);
    }
    ctx.compress_algo = PGP_COMPR_NONE;
    read_literal_from(ctx, hdr, rdr)
}

fn read_literal(ctx: &mut PgpContext, data: &[u8]) -> Result<Vec<u8>, String> {
    let mut rdr = PktReader::new(data);
    let hdr = rdr
        .read_hdr()
        .map_err(|_| CORRUPT_DATA.to_string())?
        .ok_or_else(|| CORRUPT_DATA.to_string())?;
    read_literal_from(ctx, hdr, rdr)
}

fn read_literal_from(
    ctx: &mut PgpContext,
    hdr: super::packet::PktHdr,
    mut rdr: PktReader,
) -> Result<Vec<u8>, String> {
    if hdr.tag != PGP_PKT_LITERAL_DATA {
        return Err(CORRUPT_DATA.to_string());
    }
    let body = rdr.read_body(&hdr).map_err(|_| CORRUPT_DATA.to_string())?;
    if body.len() < 2 {
        return Err(CORRUPT_DATA.to_string());
    }
    let ty = body[0];
    let namelen = body[1] as usize;
    let off = 2 + namelen + 4;
    if body.len() < off {
        return Err(CORRUPT_DATA.to_string());
    }
    if ty == b'u' {
        ctx.unicode_mode = 1;
    }
    let payload = &body[off..];
    if (ty == b't' || ty == b'u') && ctx.convert_crlf != 0 {
        Ok(un_convert_crlf(payload))
    } else {
        Ok(payload.to_vec())
    }
}

/// `\r\n` → `\n` (reverse crlf_filter on text decrypt with convert-crlf).
fn un_convert_crlf(data: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(data.len());
    let mut i = 0;
    while i < data.len() {
        if data[i] == b'\r' && i + 1 < data.len() && data[i + 1] == b'\n' {
            out.push(b'\n');
            i += 2;
        } else {
            out.push(data[i]);
            i += 1;
        }
    }
    out
}

use super::consts::Digest;
