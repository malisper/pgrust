//! `pgp_decrypt` (pgp-decrypt.c) — the symmetric-passphrase OpenPGP decrypt
//! pipeline (the public-key session-key path is not ported).

use super::cfb::PgpCfb;
use super::consts::*;
use super::context::PgpContext;
use super::packet::PktReader;
use super::s2k::S2k;

pub struct SessKey {
    pub cipher: i32,
    pub key: Vec<u8>,
}

/// `pgp_decrypt` for the symmetric path. `ctx` is updated in place with the
/// observed cipher/s2k/compress parameters (for the expect-* checks).
pub fn decrypt_symmetric(
    ctx: &mut PgpContext,
    data: &[u8],
    passphrase: &[u8],
) -> Result<Vec<u8>, String> {
    decrypt_message(ctx, data, &mut |ctx, body| {
        parse_symenc_sesskey(ctx, body, passphrase)
    })
}

/// `pgp_decrypt` for the public-key path: the caller-supplied `pubenc` closure
/// recovers the session key from a tag-1 packet body (which it decrypts with the
/// recipient secret key). The symmetric data-packet pipeline is shared.
pub fn decrypt_pubkey(
    ctx: &mut PgpContext,
    data: &[u8],
    pubenc: &mut dyn FnMut(&[u8]) -> Result<SessKey, String>,
) -> Result<Vec<u8>, String> {
    decrypt_message(ctx, data, &mut |ctx, body| {
        let sk = pubenc(body)?;
        ctx.cipher_algo = sk.cipher;
        Ok(sk)
    })
}

/// The shared decrypt driver: walk the packet stream, recover the session key
/// from whichever ESK packet (`sesskey` callback dispatches symmetric vs
/// public-key), then decrypt the symmetrically-encrypted data packet.
fn decrypt_message(
    ctx: &mut PgpContext,
    data: &[u8],
    sesskey: &mut dyn FnMut(&mut PgpContext, &[u8]) -> Result<SessKey, String>,
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
            t if t == PGP_PKT_SYMENC_SESSKEY || t == PGP_PKT_PUBENC_SESSKEY => {
                let body = rdr.read_body(&hdr).map_err(|_| CORRUPT_DATA.to_string())?;
                sess = Some(sesskey(ctx, &body)?);
            }
            t if t == PGP_PKT_SYMENC_DATA || t == PGP_PKT_SYMENC_DATA_MDC => {
                let body = rdr.read_body(&hdr).map_err(|_| CORRUPT_DATA.to_string())?;
                let sk = sess.as_ref().ok_or_else(|| WRONG_KEY.to_string())?;
                let mdc = t == PGP_PKT_SYMENC_DATA_MDC;
                ctx.disable_mdc = if mdc { 0 } else { 1 };
                // CFB-decrypt and verify the prefix; on a wrong key C does NOT
                // bail — it flags corrupt_prefix and keeps parsing the garbage
                // (emitting debug NOTICEs), then reports "Wrong key" at the end.
                let (inner, corrupt_prefix) = decrypt_data_packet(ctx, sk, &body, mdc)?;
                let parsed = finish_inner(ctx, inner);
                // Deferred MDC debug line comes after the literal-data parse.
                if ctx.pending_bad_mdc {
                    ctx.dbg("mdcbuf_finish: bad MDC pkt hdr");
                    ctx.pending_bad_mdc = false;
                }
                if corrupt_prefix {
                    return Err(WRONG_KEY.to_string());
                }
                let out = parsed?;
                if ctx.unexpected_binary {
                    return Err(NOT_TEXT.to_string());
                }
                return Ok(out);
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
/// (and MDC if present), and return `(inner_stream, corrupt_prefix)`. A
/// corrupt prefix (wrong key) does NOT abort — C flags it and keeps parsing the
/// garbage so debug NOTICEs match, then reports "Wrong key" at the end.
fn decrypt_data_packet(
    ctx: &mut PgpContext,
    sk: &SessKey,
    body: &[u8],
    mdc: bool,
) -> Result<(Vec<u8>, bool), String> {
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
    let mut corrupt_prefix = false;
    if plain[bs - 2] != plain[bs] || plain[bs - 1] != plain[bs + 1] {
        ctx.dbg("prefix_init: corrupt prefix");
        corrupt_prefix = true;
    }

    let inner_start = bs + 2;
    if mdc {
        if plain.len() < inner_start + 2 + MDC_DIGEST_LEN {
            return Err(CORRUPT_DATA.to_string());
        }
        let mdc_off = plain.len() - (2 + MDC_DIGEST_LEN);
        let inner = plain[inner_start..mdc_off].to_vec();
        if plain[mdc_off] != 0xD3 || plain[mdc_off + 1] != 0x14 {
            // Wrong key → garbage MDC header. Defer the "bad MDC pkt hdr" debug
            // line until AFTER the literal-data parse (C's filter order).
            ctx.pending_bad_mdc = true;
            return Ok((inner, true));
        }
        let mut md = Digest::new(PGP_DIGEST_SHA1).ok_or(UNSUPPORTED_HASH.to_string())?;
        md.update(&plain[..mdc_off + 2]);
        let want = md.finish();
        if want != plain[mdc_off + 2..mdc_off + 2 + MDC_DIGEST_LEN] {
            if !corrupt_prefix {
                return Err("MDC mismatch".to_string());
            }
            ctx.pending_bad_mdc = true;
            return Ok((inner, true));
        }
        Ok((inner, corrupt_prefix))
    } else {
        Ok((plain[inner_start..].to_vec(), corrupt_prefix))
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
            PGP_COMPR_BZIP2 => {
                ctx.dbg("parse_compressed_data: bzip2 unsupported");
                return Err(UNSUPPORTED_COMPR.to_string());
            }
            _ => {
                ctx.dbg("parse_compressed_data: unknown compr type");
                return Err(UNSUPPORTED_COMPR.to_string());
            }
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
    // text-mode decrypt of a non-text ('b') literal → "Not text data" later.
    if ctx.text_mode != 0 && ty != b't' && ty != b'u' {
        ctx.dbg(&format!("parse_literal_data: data type={}", ty as char));
        ctx.unexpected_binary = true;
    }
    ctx.unicode_mode = if ty == b'u' { 1 } else { 0 };
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
