
use super::cfb::PgpCfb;
use super::consts::*;
use super::context::PgpContext;
use super::packet::PktReader;
use super::s2k::S2k;

pub struct SessKey {
    pub cipher: i32,
    pub key: Vec<u8>,
}

pub fn decrypt_symmetric(
    ctx: &mut PgpContext,
    data: &[u8],
    passphrase: &[u8],
) -> Result<Vec<u8>, String> {
    decrypt_message(ctx, data, &mut |ctx, body| {
        parse_symenc_sesskey(ctx, body, passphrase)
    })
}

pub fn decrypt_pubkey(
    ctx: &mut PgpContext,
    data: &[u8],
    pubenc: &mut dyn FnMut(&mut PgpContext, &[u8]) -> Result<SessKey, String>,
) -> Result<Vec<u8>, String> {
    decrypt_message(ctx, data, &mut |ctx, body| {
        let sk = pubenc(ctx, body)?;
        ctx.cipher_algo = sk.cipher;
        Ok(sk)
    })
}

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
                let (inner, corrupt_prefix, bad_mdc) = decrypt_data_packet(ctx, sk, &body, mdc)?;
                let parsed = finish_inner(ctx, inner);
                // pgp-decrypt.c:873 process_data_packets reaches the MDC
                // packet only after the data packets parsed cleanly.
                if parsed.is_ok() {
                    if let Some(Some(why)) = &bad_mdc {
                        ctx.dbg(why);
                    }
                }
                if corrupt_prefix || bad_mdc.is_some() {
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
    // pgp-decrypt.c:647: decoded unconditionally — iter is 0 for the simple
    // and salted modes, so expect-s2k-count sees 1024 there, as in C.
    ctx.s2k_count = s2k_decode_count(s2k.iter as i32);
    ctx.s2k_digest_algo = s2k.digest_algo;
    ctx.s2k_cipher_algo = s2k_cipher;

    let rest = &body[2 + consumed..];
    if rest.is_empty() {
        ctx.use_sess_key = 0;
        ctx.cipher_algo = s2k_cipher;
        Ok(SessKey {
            cipher: s2k_cipher,
            key: s2k.key,
        })
    } else {
        // pgp-decrypt.c:681: 17 <= len <= PGP_MAX_KEY + 1, else corrupt.
        if rest.len() < 17 || rest.len() > PGP_MAX_KEY + 1 {
            return Err(CORRUPT_DATA.to_string());
        }
        ctx.use_sess_key = 1;
        // upstream 4c5128ca0b30 (18.6): pgcrypto: Add option to revert to prior decryption behavior
        let ignore = ctx.ignore_cipher_failure != 0;
        let mut cfb = PgpCfb::create(s2k_cipher, &s2k.key, false, None, ignore)
            .map_err(|e| e.to_string())?;
        let dec = cfb.decrypt(rest);
        let cipher = dec[0] as i32;
        ctx.cipher_algo = cipher;
        // pgp-decrypt.c:612 decrypt_key: the key length must be EXACTLY the
        // cipher's key size (trailing bytes are not padding).
        let klen = dec.len() - 1;
        if cipher_key_size(cipher) != klen {
            return Err(CORRUPT_DATA.to_string());
        }
        Ok(SessKey {
            cipher,
            key: dec[1..].to_vec(),
        })
    }
}

type BadMdc = Option<Option<String>>;

fn decrypt_data_packet(
    ctx: &mut PgpContext,
    sk: &SessKey,
    body: &[u8],
    mdc: bool,
) -> Result<(Vec<u8>, bool, BadMdc), String> {
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
    // upstream 4c5128ca0b30 (18.6): pgcrypto: Add option to revert to prior decryption behavior
    let ignore = ctx.ignore_cipher_failure != 0;
    let mut cfb = PgpCfb::create(sk.cipher, &sk.key, resync, None, ignore)
        .map_err(|e| e.to_string())?;
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
        let bad = mdc_trailer_failure(&plain, mdc_off)?;
        Ok((inner, corrupt_prefix, bad))
    } else {
        Ok((plain[inner_start..].to_vec(), corrupt_prefix, None))
    }
}

/// The px_debug C emits when the 22-byte MDC trailer is walked as packets
/// (pgp-decrypt.c:141 pgp_parse_pkt_hdr, :945 process_data_packets, :341
/// mdc_finish); `Some(None)` is a silent PXE_PGP_CORRUPT_DATA.
fn mdc_trailer_failure(plain: &[u8], mdc_off: usize) -> Result<BadMdc, String> {
    let (b0, b1) = (plain[mdc_off], plain[mdc_off + 1]);
    if b0 & 0x80 == 0 {
        return Ok(Some(Some("pgp_parse_pkt_hdr: not pkt hdr".to_string())));
    }
    let (tag, fixed_len) = if b0 & 0x40 != 0 {
        (b0 & 0x3f, b1 < 192)
    } else {
        ((b0 >> 2) & 0x0f, b0 & 3 == 0)
    };
    if tag as i32 != PGP_PKT_MDC {
        return Ok(Some(Some(format!("process_data_packets: unexpected pkt tag={tag}"))));
    }
    if !fixed_len || b1 as usize != MDC_DIGEST_LEN {
        return Ok(Some(None));
    }
    let mut md = Digest::new(PGP_DIGEST_SHA1).ok_or(UNSUPPORTED_HASH.to_string())?;
    md.update(&plain[..mdc_off + 2]);
    if md.finish() != plain[mdc_off + 2..mdc_off + 2 + MDC_DIGEST_LEN] {
        return Ok(Some(Some("mdc_finish: mdc failed".to_string())));
    }
    Ok(None)
}

fn finish_inner(ctx: &mut PgpContext, inner: Vec<u8>) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    process_data_packets(ctx, &inner, true, &mut out)?;
    Ok(out)
}

/// pgp-decrypt.c:873 process_data_packets: every literal packet appends to
/// `dst`; a compressed packet must be the only data packet and is legal only
/// at the top level. The MDC trailer was already peeled off by
/// decrypt_data_packet (C's mdcbuf filter), so an MDC tag here is unexpected.
fn process_data_packets(
    ctx: &mut PgpContext,
    data: &[u8],
    allow_compr: bool,
    dst: &mut Vec<u8>,
) -> Result<(), String> {
    let mut rdr = PktReader::new_allow_ctx(data);
    let mut got_data = false;
    while let Some(hdr) = rdr.read_hdr().map_err(|_| CORRUPT_DATA.to_string())? {
        let body = rdr.read_body(&hdr).map_err(|_| CORRUPT_DATA.to_string())?;
        match hdr.tag {
            t if t == PGP_PKT_LITERAL_DATA => {
                got_data = true;
                parse_literal_data(ctx, &body, dst)?;
            }
            t if t == PGP_PKT_COMPRESSED_DATA => {
                if !allow_compr {
                    ctx.dbg("process_data_packets: unexpected compression");
                    return Err(CORRUPT_DATA.to_string());
                }
                if got_data {
                    ctx.dbg("process_data_packets: only one cmpr pkt allowed");
                    return Err(CORRUPT_DATA.to_string());
                }
                got_data = true;
                parse_compressed_data(ctx, &body, dst)?;
            }
            t if t == PGP_PKT_MDC => {
                ctx.dbg("process_data_packets: unexpected MDC");
                return Err(CORRUPT_DATA.to_string());
            }
            t => {
                ctx.dbg(&format!("process_data_packets: unexpected pkt tag={t}"));
                return Err(CORRUPT_DATA.to_string());
            }
        }
    }
    if !got_data {
        ctx.dbg("process_data_packets: no data");
        return Err(CORRUPT_DATA.to_string());
    }
    Ok(())
}

fn parse_compressed_data(ctx: &mut PgpContext, body: &[u8], dst: &mut Vec<u8>) -> Result<(), String> {
    if body.is_empty() {
        return Err(CORRUPT_DATA.to_string());
    }
    let algo = body[0] as i32;
    ctx.compress_algo = algo;
    let decompressed = match algo {
        PGP_COMPR_NONE => body[1..].to_vec(),
        PGP_COMPR_ZIP => super::compress::inflate_raw(&body[1..])
            .map_err(|e| inflate_error(ctx, e))?,
        PGP_COMPR_ZLIB => super::compress::inflate_zlib(&body[1..])
            .map_err(|e| inflate_error(ctx, e))?,
        PGP_COMPR_BZIP2 => {
            ctx.dbg("parse_compressed_data: bzip2 unsupported");
            return Err(UNSUPPORTED_COMPR.to_string());
        }
        _ => {
            ctx.dbg("parse_compressed_data: unknown compr type");
            // C's parse_compressed_data default case returns the generic
            // PXE_PGP_CORRUPT_DATA (not PXE_PGP_UNSUPPORTED_COMPR, which C
            // reserves for the bzip2 flag path). Keep it generic so the
            // quick-check outcome stays unobservable.
            return Err(CORRUPT_DATA.to_string());
        }
    };
    process_data_packets(ctx, &decompressed, false, dst)
}

// pgp-compress.c:281 decompress_read: zlib's return code goes to px_debug.
fn inflate_error(ctx: &mut PgpContext, e: Option<i32>) -> String {
    if let Some(code) = e {
        ctx.dbg(&format!("decompress_read: inflate error: {code}"));
    }
    CORRUPT_DATA.to_string()
}

/// pgp-decrypt.c:745 parse_literal_data. CRLF conversion follows the SQL
/// caller's text mode and convert-crlf, not the literal packet's own type.
fn parse_literal_data(ctx: &mut PgpContext, body: &[u8], dst: &mut Vec<u8>) -> Result<(), String> {
    if body.len() < 2 {
        return Err(CORRUPT_DATA.to_string());
    }
    let ty = body[0];
    let namelen = body[1] as usize;
    let off = 2 + namelen + 4;
    if body.len() < off {
        return Err(CORRUPT_DATA.to_string());
    }
    if ctx.text_mode != 0 && ty != b't' && ty != b'u' {
        ctx.dbg(&format!("parse_literal_data: data type={}", ty as char));
        ctx.unexpected_binary = true;
    }
    ctx.unicode_mode = if ty == b'u' { 1 } else { 0 };
    let payload = &body[off..];
    if ctx.text_mode != 0 && ctx.convert_crlf != 0 {
        dst.extend_from_slice(&un_convert_crlf(payload));
    } else {
        dst.extend_from_slice(payload);
    }
    Ok(())
}

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

#[cfg(test)]
mod tests {
    use super::*;

    // pgp-compress.c:281: the zlib code is px_debug'd before
    // PXE_PGP_CORRUPT_DATA.
    #[test]
    fn inflate_failure_is_debugged_with_zlib_code() {
        let mut ctx = PgpContext::default();
        ctx.debug = 1;
        let mut out = Vec::new();
        let err = parse_compressed_data(&mut ctx, &[PGP_COMPR_ZIP as u8, 0xff, 0xff, 0xff], &mut out)
            .unwrap_err();
        assert_eq!(err, CORRUPT_DATA);
        assert_eq!(ctx.debug_notices, ["dbg: decompress_read: inflate error: -3"]);
        let comp = super::super::compress::deflate_zlib(b"truncated compressed literal", 6);
        let mut body = vec![PGP_COMPR_ZLIB as u8];
        body.extend_from_slice(&comp[..comp.len() / 2]);
        ctx.debug_notices.clear();
        parse_compressed_data(&mut ctx, &body, &mut out).unwrap_err();
        assert_eq!(ctx.debug_notices, ["dbg: decompress_read: inflate error: -5"]);
    }
}
