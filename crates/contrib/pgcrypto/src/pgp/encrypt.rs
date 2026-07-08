//! `pgp_encrypt` (pgp-encrypt.c) — the symmetric-passphrase OpenPGP encrypt
//! pipeline. Public-key encryption lives in `pubkey.rs`.

use super::cfb::PgpCfb;
use super::consts::*;
use super::context::PgpContext;
use super::packet::{render_newlen, write_packet};
use super::s2k::S2k;
use ::pg_strong_random::pg_strong_random;

/// Build the inner literal-data packet body (`type, namelen=0, mtime[4], data`).
fn build_literal_packet(ctx: &PgpContext, data: &[u8]) -> Vec<u8> {
    let ty = if ctx.text_mode != 0 {
        if ctx.unicode_mode != 0 {
            b'u'
        } else {
            b't'
        }
    } else {
        b'b'
    };
    let t: u32 = 0; // deterministic mtime=0 (round-trip safe; mtime is ignored on decrypt)
    let mut body = Vec::with_capacity(6 + data.len());
    body.push(ty);
    body.push(0); // filename length
    body.extend_from_slice(&t.to_be_bytes());
    body.extend_from_slice(data);
    let mut pkt = Vec::new();
    write_packet(&mut pkt, PGP_PKT_LITERAL_DATA, &body);
    pkt
}

/// `\n` → `\r\n` (crlf_filter).
fn convert_crlf(data: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(data.len());
    for &b in data {
        if b == b'\n' {
            out.push(b'\r');
        }
        out.push(b);
    }
    out
}

/// `pgp_encrypt` for the symmetric path. Returns the raw (no varlena) bytes.
pub fn encrypt_symmetric(
    ctx: &PgpContext,
    data: &[u8],
    passphrase: &[u8],
) -> Result<Vec<u8>, String> {
    if passphrase.is_empty() {
        return Err("pgp: no symmetric key".to_string());
    }
    let mut ctx = ctx.clone();
    if ctx.s2k_cipher_algo < 0 {
        ctx.s2k_cipher_algo = ctx.cipher_algo;
    }

    // S2K key derivation.
    let mut s2k = S2k::fill(ctx.s2k_mode, ctx.s2k_digest_algo, ctx.s2k_count)
        .map_err(|e| e.to_string())?;
    s2k.process(ctx.s2k_cipher_algo, passphrase)
        .map_err(|e| e.to_string())?;

    // Session key.
    let (sess_key, sess_cipher) = if ctx.use_sess_key != 0 {
        let len = cipher_key_size(ctx.cipher_algo);
        let mut k = vec![0u8; len];
        if !pg_strong_random(&mut k) {
            return Err("random failed".to_string());
        }
        (k, ctx.cipher_algo)
    } else {
        (s2k.key.clone(), ctx.cipher_algo)
    };

    let mut out = Vec::new();

    // --- Symmetric-key ESK packet (tag 3) ---
    let mut esk = Vec::new();
    esk.push(4); // version
    esk.push(ctx.s2k_cipher_algo as u8);
    esk.push(s2k.mode as u8);
    esk.push(s2k.digest_algo as u8);
    if s2k.mode > 0 {
        esk.extend_from_slice(&s2k.salt);
    }
    if s2k.mode == PGP_S2K_ISALTED {
        esk.push(s2k.iter);
    }
    if ctx.use_sess_key != 0 {
        // CFB(s2k.key) over (cipher_algo_byte || sess_key)
        let mut cfb = PgpCfb::create(ctx.s2k_cipher_algo, &s2k.key, false, None)
            .map_err(|e| e.to_string())?;
        let mut pt = Vec::with_capacity(1 + sess_key.len());
        pt.push(ctx.cipher_algo as u8);
        pt.extend_from_slice(&sess_key);
        let ct = cfb.encrypt(&pt);
        esk.extend_from_slice(&ct);
    }
    write_packet(&mut out, PGP_PKT_SYMENC_SESSKEY, &esk);

    // --- Build the symmetrically-encrypted data packet with the session key ---
    write_encdata_packet(&ctx, data, &sess_key, &mut out)?;

    let _ = render_newlen; // retained for parity reference
    let _ = sess_cipher;
    Ok(out)
}

/// Build and append the symmetrically-encrypted data packet (prefix + literal
/// [+ MDC], CFB-encrypted under `sess_key`) for the cipher named in `ctx`. This
/// is shared by the symmetric and public-key encrypt entry points.
pub fn write_encdata_packet(
    ctx: &PgpContext,
    data: &[u8],
    sess_key: &[u8],
    out: &mut Vec<u8>,
) -> Result<(), String> {
    let bs = cipher_block_size(ctx.cipher_algo);

    // literal (with optional compression wrapping)
    let mut literal = if ctx.text_mode != 0 && ctx.convert_crlf != 0 {
        build_literal_packet(ctx, &convert_crlf(data))
    } else {
        build_literal_packet(ctx, data)
    };
    if ctx.compress_algo > 0 && ctx.compress_level > 0 {
        literal = build_compressed_packet(ctx, &literal)?;
    }

    // prefix: bs random bytes + 2 repeat
    let mut prefix = vec![0u8; bs + 2];
    if !pg_strong_random(&mut prefix[..bs]) {
        return Err("random failed".to_string());
    }
    prefix[bs] = prefix[bs - 2];
    prefix[bs + 1] = prefix[bs - 1];

    let mdc = ctx.disable_mdc == 0;

    // plaintext to encrypt = prefix || literal [|| MDC packet]
    let mut plaintext = Vec::new();
    plaintext.extend_from_slice(&prefix);
    plaintext.extend_from_slice(&literal);

    if mdc {
        // MDC SHA1 over prefix || literal || 0xD3 0x14
        let mut md = Digest::new(PGP_DIGEST_SHA1).ok_or(UNSUPPORTED_HASH.to_string())?;
        md.update(&prefix);
        md.update(&literal);
        let hdr = [0xD3u8, 0x14u8];
        md.update(&hdr);
        let digest = md.finish();
        plaintext.extend_from_slice(&hdr);
        plaintext.extend_from_slice(&digest);
    }

    // CFB encrypt.
    let resync = !mdc;
    let mut cfb = PgpCfb::create(ctx.cipher_algo, sess_key, resync, None)
        .map_err(|e| e.to_string())?;
    let ciphertext = cfb.encrypt(&plaintext);

    // --- data packet ---
    let tag = if mdc {
        PGP_PKT_SYMENC_DATA_MDC
    } else {
        PGP_PKT_SYMENC_DATA
    };
    // body = [version 0x01 if MDC] || ciphertext
    let mut body = Vec::new();
    if mdc {
        body.push(0x01);
    }
    body.extend_from_slice(&ciphertext);
    write_packet(out, tag, &body);
    Ok(())
}

use super::consts::Digest;

/// Wrap `inner` in a Compressed-Data packet (tag 8).
fn build_compressed_packet(ctx: &PgpContext, inner: &[u8]) -> Result<Vec<u8>, String> {
    let algo = ctx.compress_algo;
    let compressed = match algo {
        PGP_COMPR_ZIP => super::compress::deflate_raw(inner, ctx.compress_level),
        PGP_COMPR_ZLIB => super::compress::deflate_zlib(inner, ctx.compress_level),
        _ => return Err(UNSUPPORTED_COMPR.to_string()),
    };
    let mut body = Vec::with_capacity(1 + compressed.len());
    body.push(algo as u8);
    body.extend_from_slice(&compressed);
    let mut pkt = Vec::new();
    write_packet(&mut pkt, PGP_PKT_COMPRESSED_DATA, &body);
    Ok(pkt)
}
