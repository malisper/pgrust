//! auth-scram.c: secret parse/build/verify, mock salt, and the SASL
//! SCRAM-SHA-256 mechanism (exchange.rs).

mod exchange;
pub use exchange::ScramMech;
#[cfg(test)]
pub(crate) use exchange::{
    test_build_server_final_message, test_scram_init, test_verify_client_proof, ScramState,
};

use mcx::{Mcx, PgString};
use pg_b64::{pg_b64_dec_len, pg_b64_decode, pg_b64_enc_len, pg_b64_encode};
use scram_common::{
    scram_build_secret, scram_salted_password, scram_server_key, SCRAM_DEFAULT_SALT_LEN,
    SCRAM_MAX_KEY_LEN, SCRAM_SHA_256_DEFAULT_ITERATIONS, SCRAM_SHA_256_KEY_LEN,
    SCRAM_SHA_256_NAME,
};
use types_error::{ErrorLocation, PgResult, ERRCODE_INTERNAL_ERROR, ERROR, LOG};

use std::cell::Cell;

#[track_caller]
pub(crate) fn loc(funcname: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, funcname)
}

thread_local! {
    static SCRAM_SHA_256_ITERATIONS: Cell<i32> = const { Cell::new(SCRAM_SHA_256_DEFAULT_ITERATIONS) };
}

pub fn scram_sha_256_iterations() -> i32 {
    SCRAM_SHA_256_ITERATIONS.with(Cell::get)
}

fn set_scram_sha_256_iterations(v: i32) {
    SCRAM_SHA_256_ITERATIONS.with(|c| c.set(v));
}

pub struct ParsedScramSecret {
    pub iterations: i32,
    pub key_length: i32,
    pub salt: String,
    pub stored_key: [u8; SCRAM_MAX_KEY_LEN],
    pub server_key: [u8; SCRAM_MAX_KEY_LEN],
}

pub fn parse_scram_secret(secret: &str) -> Option<ParsedScramSecret> {
    let mut cur = secret.as_bytes();
    let (scheme_str, ok) = strsep(&mut cur, b'$');
    if !ok {
        return None;
    }
    let (iterations_str, ok) = strsep(&mut cur, b':');
    if !ok {
        return None;
    }
    let (salt_str, ok) = strsep(&mut cur, b'$');
    if !ok {
        return None;
    }
    let (storedkey_str, ok) = strsep(&mut cur, b':');
    if !ok {
        return None;
    }
    let serverkey_str = cur;

    if scheme_str != SCRAM_SHA_256_NAME.as_bytes() {
        return None;
    }
    let key_length = SCRAM_SHA_256_KEY_LEN as i32;

    let iterations = strtol_base10_full(iterations_str)?;

    // Salt is validated by decoding but returned encoded, like C.
    b64dec(salt_str)?;

    let stored = b64dec(storedkey_str)?;
    if stored.len() != key_length as usize {
        return None;
    }
    let server = b64dec(serverkey_str)?;
    if server.len() != key_length as usize {
        return None;
    }

    let mut stored_key = [0u8; SCRAM_MAX_KEY_LEN];
    let mut server_key = [0u8; SCRAM_MAX_KEY_LEN];
    stored_key.copy_from_slice(&stored);
    server_key.copy_from_slice(&server);

    Some(ParsedScramSecret {
        iterations,
        key_length,
        salt: String::from_utf8_lossy(salt_str).into_owned(),
        stored_key,
        server_key,
    })
}

pub fn pg_be_scram_build_secret<'mcx>(mcx: Mcx<'mcx>, password: &str) -> PgResult<PgString<'mcx>> {
    let prep = saslprep::pg_saslprep(mcx, password.as_bytes())?;
    let password: &[u8] = match &prep {
        Some(p) => p,
        None => password.as_bytes(),
    };

    let mut saltbuf = [0u8; SCRAM_DEFAULT_SALT_LEN];
    if let Some(fixed) = test_fixed_salt() {
        saltbuf = fixed;
    } else if !pg_strong_random::pg_strong_random(&mut saltbuf) {
        elog::ereport(ERROR)
            .errcode(ERRCODE_INTERNAL_ERROR)
            .errmsg("could not generate random salt")
            .finish(loc("pg_be_scram_build_secret"))?;
    }

    scram_build_secret(mcx, &saltbuf, scram_sha_256_iterations(), password)
}

// Test-only determinism hook (DIVERGENCE, no C counterpart): a fixed salt for
// byte-parity e2es, taken from PGRUST_SCRAM_FIXED_SALT_B64. Malformed = panic.
fn test_fixed_salt() -> Option<[u8; SCRAM_DEFAULT_SALT_LEN]> {
    let v = std::env::var("PGRUST_SCRAM_FIXED_SALT_B64").ok()?;
    let decoded = b64dec(v.as_bytes()).expect("PGRUST_SCRAM_FIXED_SALT_B64: invalid base64");
    let mut salt = [0u8; SCRAM_DEFAULT_SALT_LEN];
    salt.copy_from_slice(&decoded);
    Some(salt)
}

pub fn scram_verify_plain_password(
    mcx: Mcx<'_>,
    username: &str,
    password: &str,
    secret: &str,
) -> PgResult<bool> {
    let Some(parsed) = parse_scram_secret(secret) else {
        elog::ereport(LOG)
            .errmsg(format!("invalid SCRAM secret for user \"{username}\""))
            .finish(loc("scram_verify_plain_password"))?;
        return Ok(false);
    };

    let Some(salt) = b64dec(parsed.salt.as_bytes()) else {
        elog::ereport(LOG)
            .errmsg(format!("invalid SCRAM secret for user \"{username}\""))
            .finish(loc("scram_verify_plain_password"))?;
        return Ok(false);
    };

    let prep = saslprep::pg_saslprep(mcx, password.as_bytes())?;
    let password: &[u8] = match &prep {
        Some(p) => p,
        None => password.as_bytes(),
    };

    let salted_password = scram_salted_password(password, &salt, parsed.iterations)?;
    let computed_key = scram_server_key(&salted_password);

    // C compares with plain memcmp here (not constant-time).
    Ok(computed_key[..parsed.key_length as usize]
        == parsed.server_key[..parsed.key_length as usize])
}

pub struct MockScramSecret {
    pub iterations: i32,
    pub key_length: i32,
    pub salt: String,
    pub stored_key: [u8; SCRAM_MAX_KEY_LEN],
    pub server_key: [u8; SCRAM_MAX_KEY_LEN],
}

pub fn mock_scram_secret(username: &str) -> PgResult<MockScramSecret> {
    let key_length = SCRAM_SHA_256_KEY_LEN as i32;

    let raw_salt = scram_mock_salt(username);

    let cap = pg_b64_enc_len(SCRAM_DEFAULT_SALT_LEN as i32);
    let mut encoded = vec![0u8; cap as usize];
    let n = pg_b64_encode(&raw_salt[..SCRAM_DEFAULT_SALT_LEN], SCRAM_DEFAULT_SALT_LEN as i32, &mut encoded, cap);
    if n < 0 {
        elog::ereport(ERROR)
            .errcode(ERRCODE_INTERNAL_ERROR)
            .errmsg_internal("could not encode salt")
            .finish(loc("mock_scram_secret"))?;
    }
    encoded.truncate(n as usize);

    Ok(MockScramSecret {
        iterations: SCRAM_SHA_256_DEFAULT_ITERATIONS,
        key_length,
        salt: String::from_utf8(encoded).unwrap(),
        // StoredKey and ServerKey are not used in a doomed authentication.
        stored_key: [0; SCRAM_MAX_KEY_LEN],
        server_key: [0; SCRAM_MAX_KEY_LEN],
    })
}

pub fn scram_mock_salt(username: &str) -> [u8; SCRAM_SHA_256_KEY_LEN] {
    const { assert!(SCRAM_SHA_256_KEY_LEN >= SCRAM_DEFAULT_SALT_LEN) };
    let nonce = transam_xlog::GetMockAuthenticationNonce();
    let mut ctx = pg_sha2::PgSha256Ctx::init_sha256();
    ctx.update(username.as_bytes());
    ctx.update(&nonce);
    ctx.final_sha256()
}

fn strsep<'a>(stringp: &mut &'a [u8], delim: u8) -> (&'a [u8], bool) {
    let s = *stringp;
    match s.iter().position(|&c| c == delim) {
        Some(idx) => {
            *stringp = &s[idx + 1..];
            (&s[..idx], true)
        }
        None => {
            *stringp = &[];
            (s, false)
        }
    }
}

// errno = 0; v = strtol(s, &p, 10); reject iff (*p || errno). An empty string
// converts nothing, leaves p at the terminator, and yields 0 — C accepts it.
fn strtol_base10_full(s: &[u8]) -> Option<i32> {
    if s.is_empty() {
        return Some(0);
    }
    let mut i = 0;
    while i < s.len() && matches!(s[i], b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r') {
        i += 1;
    }
    let mut neg = false;
    if i < s.len() && (s[i] == b'+' || s[i] == b'-') {
        neg = s[i] == b'-';
        i += 1;
    }
    let digits_start = i;
    let mut acc: i64 = 0;
    let mut overflow = false;
    while i < s.len() && s[i].is_ascii_digit() {
        let digit = (s[i] - b'0') as i64;
        match acc.checked_mul(10).and_then(|v| v.checked_add(digit)) {
            Some(v) => acc = v,
            None => overflow = true,
        }
        i += 1;
    }
    if i == digits_start || overflow || i != s.len() {
        return None;
    }
    let val = if neg { acc.wrapping_neg() } else { acc };
    Some(val as i32)
}

pub(crate) fn b64dec(src: &[u8]) -> Option<Vec<u8>> {
    let cap = pg_b64_dec_len(src.len() as i32);
    let mut dst = vec![0u8; cap as usize];
    let n = pg_b64_decode(src, src.len() as i32, &mut dst, cap);
    if n < 0 {
        return None;
    }
    dst.truncate(n as usize);
    Some(dst)
}

pub fn init_seams() {
    guc_tables::vars::scram_sha_256_iterations.install(guc_tables::GucVarAccessors {
        get: scram_sha_256_iterations,
        set: set_scram_sha_256_iterations,
    });
}

#[cfg(test)]
mod tests;
