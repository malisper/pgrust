//! SHA-256 / SHA-512 crypt (`$5$` / `$6$`, `crypt-sha.c`).
//!
//! The rounds option is parsed with C's exact `strtoint` semantics
//! (src/common/string.c: `strtol`, truncate long->int, errno IGNORED), so
//! out-of-range values wrap and clamp exactly as C does — e.g. C turns
//! `rounds=2147483648` into `-2147483648`, notices, and runs 1000 rounds.
//! Parsing into a wider type and clamping to the local max turned a 13-byte
//! setting string into a 999,999,999-round DoS (lane p1-pgcrypto, D12).
//!
//! The digest loop itself is the `pwhash` crate's sha2_crypt algorithm,
//! carried in-tree verbatim over pg_sha2 so the per-round
//! CHECK_FOR_INTERRUPTS that C has (crypt-sha.c step-21 loop) can run —
//! an external crate's loop is not cancellable (D19). Salt handling
//! (truncate to 16, hash64 alphabet check, `$`-delimited) keeps pwhash's
//! behavior bit-for-bit; the crypt-sha.c-native salt semantics are the
//! separate native-port task (#71).

use super::CryptError;
use types_error::PgError;

const ROUNDS_MIN: i32 = 1000;
const ROUNDS_MAX: i32 = 999_999_999;
const ROUNDS_DEFAULT: u32 = 5000;
const MAX_SALT_LEN: usize = 16;

// crypt-sha.c's rounds-clamp diagnostics: a non-throwing client NOTICE.
fn notice(msg: &str) {
    let _ = elog::ereport(types_error::NOTICE)
        .errcode(types_error::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
        .errmsg(msg.to_string())
        .finish(types_error::ErrorLocation { filename: None, lineno: 0, funcname: None });
}

fn null_err() -> CryptError {
    CryptError::Message("crypt(3) returned NULL".to_string())
}

/// C `strtol(str, &endp, 10)` over bytes: skip isspace, optional sign,
/// base-10 digits saturating to `long` (i64) on overflow. Returns
/// (value, end index); on no conversion the end index is 0 (C sets
/// `*endptr = str`, the pre-whitespace start).
fn strtol10(s: &[u8]) -> (i64, usize) {
    let mut i = 0usize;
    while i < s.len() && matches!(s[i], b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r') {
        i += 1;
    }
    let mut neg = false;
    if i < s.len() && (s[i] == b'+' || s[i] == b'-') {
        neg = s[i] == b'-';
        i += 1;
    }
    let mut val: i64 = 0;
    let mut saturated = false;
    let mut any = false;
    while i < s.len() && s[i].is_ascii_digit() {
        any = true;
        let d = (s[i] - b'0') as i64;
        if !saturated {
            // Accumulate negated (like C impls) so i64::MIN is reachable.
            match val.checked_mul(10).and_then(|v| v.checked_sub(d)) {
                Some(v) => val = v,
                None => saturated = true,
            }
        }
        i += 1;
    }
    if !any {
        return (0, 0);
    }
    if saturated || (!neg && val == i64::MIN) {
        return (if neg { i64::MIN } else { i64::MAX }, i);
    }
    (if neg { val } else { -val }, i)
}

// pwhash's CRYPT_HASH64 alphabet (same table as crypt.rs ITOA64).
const HASH64: &[u8; 64] = super::ITOA64;

/// pwhash `bcrypt_hash64_decode`'s validity net over the salt (its decode
/// output is discarded by sha2_crypt — only the error matters): every byte
/// must map through the bcrypt hash64 alphabet.
fn salt_chars_valid(salt: &[u8]) -> bool {
    const BCRYPT_HASH64: &[u8; 64] =
        b"./ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    salt.iter().all(|&b| {
        let v = (b as u32).wrapping_sub(0x20);
        v <= 0x60 && BCRYPT_HASH64.contains(&b)
    })
}

/// pwhash `md5_sha2_hash64_encode` replica.
fn hash64_encode(bs: &[u8]) -> String {
    let ngroups = bs.len().div_ceil(3);
    let mut out = String::with_capacity(ngroups * 4);
    for g in 0..ngroups {
        let mut g_idx = g * 3;
        let mut enc = 0u32;
        for _ in 0..3 {
            let b = (if g_idx < bs.len() { bs[g_idx] } else { 0 }) as u32;
            enc >>= 8;
            enc |= b << 16;
            g_idx += 1;
        }
        for _ in 0..4 {
            out.push(HASH64[(enc & 0x3f) as usize] as char);
            enc >>= 6;
        }
    }
    match bs.len() % 3 {
        1 => {
            out.pop();
            out.pop();
        }
        2 => {
            out.pop();
        }
        _ => (),
    }
    out
}

// pg_sha2's two context types behind one face for the shared crypt loop.
trait ShaCtx: Sized {
    const DSIZE: usize;
    fn init() -> Self;
    fn update(&mut self, data: &[u8]);
    fn finish(self) -> Vec<u8>;
}

struct Ctx256(pg_sha2::PgSha256Ctx);
impl ShaCtx for Ctx256 {
    const DSIZE: usize = 32;
    fn init() -> Self {
        Ctx256(pg_sha2::PgSha256Ctx::init_sha256())
    }
    fn update(&mut self, data: &[u8]) {
        self.0.update(data);
    }
    fn finish(self) -> Vec<u8> {
        self.0.final_sha256().to_vec()
    }
}

struct Ctx512(pg_sha2::PgSha512Ctx);
impl ShaCtx for Ctx512 {
    const DSIZE: usize = 64;
    fn init() -> Self {
        Ctx512(pg_sha2::PgSha512Ctx::init_sha512())
    }
    fn update(&mut self, data: &[u8]) {
        self.0.update(data);
    }
    fn finish(self) -> Vec<u8> {
        self.0.final_sha512().to_vec()
    }
}

const SHA256_TRANSPOSE: &[u8] = b"\x14\x0a\x00\x0b\x01\x15\x02\x16\x0c\x17\x0d\x03\x0e\x04\x18\x05\
      \x19\x0f\x1a\x10\x06\x11\x07\x1b\x08\x1c\x12\x1d\x13\x09\x1e\x1f";
const SHA512_TRANSPOSE: &[u8] = b"\x2a\x15\x00\x01\x2b\x16\x17\x02\x2c\x2d\x18\x03\x04\x2e\x19\x1a\
      \x05\x2f\x30\x1b\x06\x07\x31\x1c\x1d\x08\x32\x33\x1e\x09\x0a\x34\
      \x1f\x20\x0b\x35\x36\x21\x0c\x0d\x37\x22\x23\x0e\x38\x39\x24\x0f\
      \x10\x3a\x25\x26\x11\x3b\x3c\x27\x12\x13\x3d\x28\x29\x14\x3e\x3f";

/// pwhash `sha2_crypt` computation (Drepper's algorithm), CHECK_FOR_INTERRUPTS
/// per round exactly where C's crypt-sha.c step-21 loop has it.
fn sha2_crypt<C: ShaCtx>(
    pass: &[u8],
    salt: &[u8],
    rounds: u32,
    trn_table: &[u8],
) -> Result<Vec<u8>, CryptError> {
    let dsize = C::DSIZE;

    let mut dgst_b = C::init();
    dgst_b.update(pass);
    dgst_b.update(salt);
    dgst_b.update(pass);
    let mut hash_b = dgst_b.finish();

    let mut dgst_a = C::init();
    dgst_a.update(pass);
    dgst_a.update(salt);

    let plen = pass.len();
    let mut p = plen;
    while p > 0 {
        dgst_a.update(&hash_b[..p.min(dsize)]);
        if p < dsize {
            break;
        }
        p -= dsize;
    }

    p = plen;
    while p > 0 {
        if p & 1 == 0 {
            dgst_a.update(pass);
        } else {
            dgst_a.update(&hash_b[..dsize]);
        }
        p >>= 1;
    }

    let mut hash_a = dgst_a.finish();

    let mut dgst_b = C::init();
    for _ in 0..plen {
        dgst_b.update(pass);
    }
    hash_b = dgst_b.finish();
    let mut seq_p = Vec::<u8>::with_capacity(plen.div_ceil(dsize) * dsize);
    p = plen;
    while p > 0 {
        seq_p.extend(&hash_b[..p.min(dsize)]);
        if p < dsize {
            break;
        }
        p -= dsize;
    }

    let mut dgst_b = C::init();
    for _ in 0..MAX_SALT_LEN + (hash_a[0] as usize) {
        dgst_b.update(salt);
    }
    hash_b = dgst_b.finish();
    let mut seq_s = Vec::<u8>::with_capacity(MAX_SALT_LEN);
    seq_s.extend(&hash_b[..salt.len()]);

    for r in 0..rounds {
        // C runs CHECK_FOR_INTERRUPTS() at the top of every round so large
        // "rounds" stay cancellable (crypt-sha.c). A raised cancel/die
        // propagates out as the error.
        postgres_seams::check_for_interrupts::call().map_err(CryptError::Pg)?;

        let mut dgst_a = C::init();
        if r % 2 == 1 {
            dgst_a.update(&seq_p[..]);
        } else {
            dgst_a.update(&hash_a[..dsize]);
        }
        if r % 3 > 0 {
            dgst_a.update(&seq_s[..]);
        }
        if r % 7 > 0 {
            dgst_a.update(&seq_p[..]);
        }
        if r % 2 == 1 {
            dgst_a.update(&hash_a[..dsize]);
        } else {
            dgst_a.update(&seq_p[..]);
        }
        hash_a = dgst_a.finish();
    }

    let mut out = hash_b;
    for (i, &ti) in trn_table.iter().enumerate() {
        out[i] = hash_a[ti as usize];
    }
    out.truncate(trn_table.len());
    Ok(out)
}

pub fn crypt_sha(pw: &str, setting: &str) -> Result<String, CryptError> {
    let is_512 = setting.as_bytes().starts_with(b"$6$");
    let after = &setting[3..];

    // Rounds option: C parity (crypt-sha.c px_crypt_shacrypt rounds branch).
    let (rounds, rounds_custom, salt_rest): (u32, bool, &[u8]) =
        if let Some(rest) = after.strip_prefix("rounds=") {
            let rb = rest.as_bytes();
            let (lval, end) = strtol10(rb);
            // C strtoint: truncate long -> int; overflow/errno ignored.
            let mut srounds = lval as i32;
            if rb.get(end) != Some(&b'$') {
                return Err(CryptError::Pg(
                    PgError::error("could not parse salt options".to_string())
                        .with_sqlstate(types_error::ERRCODE_SYNTAX_ERROR)
                        .into(),
                ));
            }
            if srounds > ROUNDS_MAX {
                notice(&format!(
                    "rounds={srounds} exceeds maximum supported value ({ROUNDS_MAX}), using {ROUNDS_MAX} instead"
                ));
                srounds = ROUNDS_MAX;
            } else if srounds < ROUNDS_MIN {
                notice(&format!(
                    "rounds={srounds} is below supported value ({ROUNDS_MIN}), using {ROUNDS_MIN} instead"
                ));
                srounds = ROUNDS_MIN;
            }
            (srounds as u32, true, &rb[end + 1..])
        } else {
            (ROUNDS_DEFAULT, false, after.as_bytes())
        };

    // Salt: up to the next '$', truncated to 16 bytes, hash64-alphabet only
    // (pwhash hash_with behavior, kept bit-for-bit).
    let salt_field = match salt_rest.iter().position(|&b| b == b'$') {
        Some(i) => &salt_rest[..i],
        None => salt_rest,
    };
    let salt = &salt_field[..salt_field.len().min(MAX_SALT_LEN)];
    if !salt_chars_valid(salt) {
        return Err(null_err());
    }
    let salt_str = core::str::from_utf8(salt).map_err(|_| null_err())?;

    let raw = if is_512 {
        sha2_crypt::<Ctx512>(pw.as_bytes(), salt, rounds, SHA512_TRANSPOSE)?
    } else {
        sha2_crypt::<Ctx256>(pw.as_bytes(), salt, rounds, SHA256_TRANSPOSE)?
    };
    let magic = if is_512 { "$6$" } else { "$5$" };
    let encoded = hash64_encode(&raw);
    Ok(if rounds_custom {
        format!("{magic}rounds={rounds}${salt_str}${encoded}")
    } else {
        format!("{magic}{salt_str}${encoded}")
    })
}
