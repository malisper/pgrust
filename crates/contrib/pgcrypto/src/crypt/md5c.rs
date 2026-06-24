//! md5-crypt (`$1$`) — a faithful port of the classic FreeBSD `crypt-md5.c`
//! algorithm that pgcrypto bundles. Uses the ported `cryptohash` MD5 so output
//! is byte-identical to the C build.

use ::crypto::pg_cryptohash_type::PG_MD5;
use ::cryptohash::{
    pg_cryptohash_create, pg_cryptohash_final, pg_cryptohash_free, pg_cryptohash_init,
    pg_cryptohash_update,
};

const MD5_SIZE: usize = 16;
const ITOA64: &[u8; 64] = b"./0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

/// One full MD5 over the concatenation of the supplied byte slices.
fn md5(parts: &[&[u8]]) -> [u8; MD5_SIZE] {
    let ctx = pg_cryptohash_create(PG_MD5);
    let _ = pg_cryptohash_init(ctx);
    for p in parts {
        let _ = pg_cryptohash_update(ctx, p.as_ptr(), p.len());
    }
    let mut out = [0u8; MD5_SIZE];
    let _ = pg_cryptohash_final(ctx, out.as_mut_ptr(), out.len());
    pg_cryptohash_free(ctx);
    out
}

/// `_crypt_to64` — emit `n` base-64 characters of `v` (little-group-first).
fn to64(out: &mut Vec<u8>, mut v: u32, n: usize) {
    for _ in 0..n {
        out.push(ITOA64[(v & 0x3f) as usize]);
        v >>= 6;
    }
}

/// md5-crypt with the `$1$` magic. `salt` is the full salt string (`$1$....` or
/// `$1$....$...`); only the 8 salt chars after the magic are used.
pub fn crypt_md5(pw: &[u8], salt: &[u8]) -> Result<String, String> {
    const MAGIC: &[u8] = b"$1$";

    // Skip the magic prefix.
    let after = &salt[MAGIC.len()..];
    // Salt runs up to the next '$' or end, max 8 chars.
    let mut sl = 0usize;
    while sl < after.len() && sl < 8 && after[sl] != b'$' {
        sl += 1;
    }
    let salt_bytes = &after[..sl];

    // Primary digest: pw + magic + salt + alt-digest mixing.
    let alt = md5(&[pw, salt_bytes, pw]);

    let ctx = pg_cryptohash_create(PG_MD5);
    let _ = pg_cryptohash_init(ctx);
    let _ = pg_cryptohash_update(ctx, pw.as_ptr(), pw.len());
    let _ = pg_cryptohash_update(ctx, MAGIC.as_ptr(), MAGIC.len());
    let _ = pg_cryptohash_update(ctx, salt_bytes.as_ptr(), salt_bytes.len());

    // For each character of the password, add the alt digest (cycled).
    let mut pl = pw.len();
    while pl > 0 {
        let take = pl.min(MD5_SIZE);
        let _ = pg_cryptohash_update(ctx, alt.as_ptr(), take);
        pl -= take;
    }

    // Then for each bit of pw.len(), add either a NUL or pw[0].
    let zero = [0u8; 1];
    let mut i = pw.len();
    while i != 0 {
        if i & 1 != 0 {
            let _ = pg_cryptohash_update(ctx, zero.as_ptr(), 1);
        } else {
            let _ = pg_cryptohash_update(ctx, pw.as_ptr(), 1);
        }
        i >>= 1;
    }

    let mut digest = [0u8; MD5_SIZE];
    let _ = pg_cryptohash_final(ctx, digest.as_mut_ptr(), digest.len());
    pg_cryptohash_free(ctx);

    // 1000 rounds of strengthening.
    for r in 0..1000usize {
        let cctx = pg_cryptohash_create(PG_MD5);
        let _ = pg_cryptohash_init(cctx);
        if r & 1 != 0 {
            let _ = pg_cryptohash_update(cctx, pw.as_ptr(), pw.len());
        } else {
            let _ = pg_cryptohash_update(cctx, digest.as_ptr(), MD5_SIZE);
        }
        if r % 3 != 0 {
            let _ = pg_cryptohash_update(cctx, salt_bytes.as_ptr(), salt_bytes.len());
        }
        if r % 7 != 0 {
            let _ = pg_cryptohash_update(cctx, pw.as_ptr(), pw.len());
        }
        if r & 1 != 0 {
            let _ = pg_cryptohash_update(cctx, digest.as_ptr(), MD5_SIZE);
        } else {
            let _ = pg_cryptohash_update(cctx, pw.as_ptr(), pw.len());
        }
        let _ = pg_cryptohash_final(cctx, digest.as_mut_ptr(), digest.len());
        pg_cryptohash_free(cctx);
    }

    // Encode the 16-byte digest in the crypt permutation order.
    let mut enc = Vec::with_capacity(22);
    let d = &digest;
    to64(
        &mut enc,
        ((d[0] as u32) << 16) | ((d[6] as u32) << 8) | (d[12] as u32),
        4,
    );
    to64(
        &mut enc,
        ((d[1] as u32) << 16) | ((d[7] as u32) << 8) | (d[13] as u32),
        4,
    );
    to64(
        &mut enc,
        ((d[2] as u32) << 16) | ((d[8] as u32) << 8) | (d[14] as u32),
        4,
    );
    to64(
        &mut enc,
        ((d[3] as u32) << 16) | ((d[9] as u32) << 8) | (d[15] as u32),
        4,
    );
    to64(
        &mut enc,
        ((d[4] as u32) << 16) | ((d[10] as u32) << 8) | (d[5] as u32),
        4,
    );
    to64(&mut enc, d[11] as u32, 2);

    Ok(format!(
        "$1${}${}",
        String::from_utf8_lossy(salt_bytes),
        String::from_utf8_lossy(&enc)
    ))
}
