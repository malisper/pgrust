// Specialized to SHA-256: C threads pg_cryptohash_type/key_length through
// every function but scram_build_secret asserts PG_SHA256 and no other hash
// is reachable in 18.3. Crypto legs are infallible here (C's -1/errstr arms
// are OOM/EVP-failure only); PgResult carries CHECK_FOR_INTERRUPTS and OOM.

use mcx::{Mcx, PgString};
use pg_b64::{pg_b64_enc_len, pg_b64_encode};
use pg_hmac::{hmac_sha256, PgHmacCtx, Sha256};
use pg_sha2::PG_SHA256_DIGEST_LENGTH;
use secure_zero::{secure_zero, secure_zero_slice};
use types_error::PgResult;

pub const SCRAM_SHA_256_NAME: &str = "SCRAM-SHA-256";
pub const SCRAM_SHA_256_PLUS_NAME: &str = "SCRAM-SHA-256-PLUS";
pub const SCRAM_SHA_256_KEY_LEN: usize = PG_SHA256_DIGEST_LENGTH;
pub const SCRAM_MAX_KEY_LEN: usize = SCRAM_SHA_256_KEY_LEN;
pub const SCRAM_RAW_NONCE_LEN: usize = 18;
pub const SCRAM_DEFAULT_SALT_LEN: usize = 16;
pub const SCRAM_SHA_256_DEFAULT_ITERATIONS: i32 = 4096;

pub fn scram_salted_password(
    password: &[u8],
    salt: &[u8],
    iterations: i32,
) -> PgResult<[u8; SCRAM_SHA_256_KEY_LEN]> {
    let one = 1u32.to_be_bytes();

    let mut ctx = PgHmacCtx::<Sha256>::init(password);
    ctx.update(salt);
    ctx.update(&one);
    let mut ui_prev = ctx.finalize();

    let mut result = ui_prev;
    let mut i = 1;
    while i < iterations {
        // Interruptible: scram_iterations may be set very large. Wipe the
        // key-derived intermediates before bailing on an interrupt. C
        // compiles the check out of FRONTEND builds: a client binary never
        // installs the seam.
        if postgres_seams::check_for_interrupts::is_installed() {
            if let Err(e) = postgres_seams::check_for_interrupts::call() {
                secure_zero_slice(&mut result);
                secure_zero_slice(&mut ui_prev);
                return Err(e);
            }
        }

        let mut ui = hmac_sha256(password, &ui_prev);
        for j in 0..SCRAM_SHA_256_KEY_LEN {
            result[j] ^= ui[j];
        }
        ui_prev = ui;
        // `ui` was copied into `ui_prev`; wipe this iteration's copy.
        secure_zero_slice(&mut ui);
        i += 1;
    }

    // `ui_prev` holds the last U_i (key-derived); wipe it. `result` is the
    // SaltedPassword returned to the caller, whose duty it is to wipe it.
    secure_zero_slice(&mut ui_prev);
    Ok(result)
}

pub fn scram_h(input: &[u8; SCRAM_SHA_256_KEY_LEN]) -> [u8; SCRAM_SHA_256_KEY_LEN] {
    pg_sha2::sha256(input)
}

pub fn scram_client_key(
    salted_password: &[u8; SCRAM_SHA_256_KEY_LEN],
) -> [u8; SCRAM_SHA_256_KEY_LEN] {
    hmac_sha256(salted_password, b"Client Key")
}

pub fn scram_server_key(
    salted_password: &[u8; SCRAM_SHA_256_KEY_LEN],
) -> [u8; SCRAM_SHA_256_KEY_LEN] {
    hmac_sha256(salted_password, b"Server Key")
}

pub fn scram_build_secret<'mcx>(
    mcx: Mcx<'mcx>,
    salt: &[u8],
    iterations: i32,
    password: &[u8],
) -> PgResult<PgString<'mcx>> {
    assert!(iterations > 0);

    let mut salted_password = scram_salted_password(password, salt, iterations)?;
    let mut client_key = scram_client_key(&salted_password);
    let mut stored_key = scram_h(&client_key);
    let mut server_key = scram_server_key(&salted_password);

    let encoded_salt = b64(salt); // salt is public (stored verbatim in the catalog)
    let mut encoded_stored = b64(&stored_key);
    let mut encoded_server = b64(&server_key);

    // Cold DDL path: one std String temp for the format, then one mcx copy.
    let mut secret = format!(
        "{SCRAM_SHA_256_NAME}${iterations}:{encoded_salt}${encoded_stored}:{encoded_server}"
    );
    let out = PgString::from_str_in(&secret, mcx);

    // Wipe every temporary carrying key-equivalent / verifier material before
    // it is dropped onto the shared process heap (runs on the OOM error path
    // of from_str_in too). SAFETY: as_bytes_mut leaves valid UTF-8 (all-zero)
    // and the strings are dropped immediately after.
    secure_zero(unsafe { secret.as_bytes_mut() });
    secure_zero(unsafe { encoded_server.as_bytes_mut() });
    secure_zero(unsafe { encoded_stored.as_bytes_mut() });
    secure_zero_slice(&mut salted_password);
    secure_zero_slice(&mut client_key);
    secure_zero_slice(&mut stored_key);
    secure_zero_slice(&mut server_key);

    out
}

fn b64(src: &[u8]) -> String {
    let cap = pg_b64_enc_len(src.len() as i32);
    let mut dst = vec![0u8; cap as usize];
    let n = pg_b64_encode(src, src.len() as i32, &mut dst, cap);
    assert!(n >= 0);
    dst.truncate(n as usize);
    String::from_utf8(dst).unwrap()
}

#[cfg(test)]
mod tests;
