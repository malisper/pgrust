//! Port of `src/backend/libpq/auth-scram.c` — the server-side SASL
//! SCRAM-SHA-256 mechanism — together with the SCRAM arm of the
//! `auth-sasl.c:CheckSASLAuth` driver loop.
//!
//! ## In-crate (real logic)
//!
//!   * The full exchange state machine: [`scram_init`], [`scram_exchange`]
//!     (read client-first → build server-first → read client-final → verify →
//!     build server-final), `read_client_first_message`,
//!     `read_client_final_message`, `build_server_first_message`,
//!     `build_server_final_message`, `verify_client_proof`,
//!     `verify_final_nonce`.
//!   * [`parse_scram_secret`], `mock_scram_secret` (anti-enumeration deterministic
//!     salt via the cluster nonce + cryptohash), `scram_mock_salt`.
//!   * [`pg_be_scram_build_secret`], [`scram_verify_plain_password`].
//!   * The message-syntax helpers `read_attr_value`, `read_any_attr`,
//!     `is_scram_printable`, `sanitize_char`, `sanitize_str`, and the C string
//!     primitives `strsep`/`strtol`/`strlen` they share.
//!   * `scram_get_mechanisms` + the `CheckSASLAuth` message loop for SCRAM,
//!     installed as the inward seam [`auth_seams::check_scram_sasl_auth`]
//!     that `auth.c`'s `CheckPWChallengeAuth` consumes.
//!
//! ## Seamed (genuinely-absent / unported owners)
//!
//! The SCRAM crypto kernel (`common/scram-common.c`: `scram_H`/
//! `scram_SaltedPassword`/`scram_ServerKey`/`scram_build_secret`), HMAC
//! (`common/hmac.c`), SASLprep (`common/saslprep.c`), the CSPRNG
//! (`port/pg_strong_random.c`), the cluster mock nonce (`access/xlog.c`), and
//! the cryptohash primitive (`common/cryptohash.c`) all cross seams. The live
//! socket I/O reuses `pqcomm`/`pqformat` directly, and `sendAuthRequest` reuses
//! the already-ported `backend-libpq-auth` entry point. Zero `extern "C"`.
//!
//! Channel binding (`USE_SSL`) is built unconditionally-off here, matching a
//! `!USE_SSL` build: the PLUS mechanism is never advertised, the `'p'`
//! cbind-flag and the `c=` binding-data arms `ereport` exactly as the C
//! `#else` paths do.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(clippy::result_large_err)]
#![allow(clippy::too_many_arguments)]

use std::sync::atomic::{AtomicI32, Ordering};

use utils_error::{elog, ereport};
use prng_base64::base64::{pg_b64_dec_len, pg_b64_decode, pg_b64_enc_len, pg_b64_encode};
use sha2::PG_SHA256_DIGEST_LENGTH;
use control::MOCK_AUTH_NONCE_LEN;
use crypto::pg_cryptohash_type;
use types_error::{
    ErrorLocation, PgError, PgResult, ERROR, LOG, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INTERNAL_ERROR, ERRCODE_PROTOCOL_VIOLATION,
};
use net::Port;

use scram_seams as scram_seams;

// ===========================================================================
// common/scram-common.h + common/cryptohash.h constants
// ===========================================================================

/// `PG_SHA256` (`common/cryptohash.h`).
pub const PG_SHA256: pg_cryptohash_type = pg_cryptohash_type::PG_SHA256;

/// `SCRAM_SHA_256_KEY_LEN` = `PG_SHA256_DIGEST_LENGTH` = 32.
pub const SCRAM_SHA_256_KEY_LEN: usize = PG_SHA256_DIGEST_LENGTH;

/// `SCRAM_MAX_KEY_LEN` = `SCRAM_SHA_256_KEY_LEN` = 32.
pub const SCRAM_MAX_KEY_LEN: usize = SCRAM_SHA_256_KEY_LEN;

/// `SCRAM_RAW_NONCE_LEN` = 18.
pub const SCRAM_RAW_NONCE_LEN: usize = 18;

/// `SCRAM_DEFAULT_SALT_LEN` = 16.
pub const SCRAM_DEFAULT_SALT_LEN: usize = 16;

/// `SCRAM_SHA_256_DEFAULT_ITERATIONS` = 4096.
pub const SCRAM_SHA_256_DEFAULT_ITERATIONS: i32 = 4096;

/// `SCRAM_SHA_256_NAME` (`common/scram-common.h`).
pub const SCRAM_SHA_256_NAME: &[u8] = b"SCRAM-SHA-256";

/// `SCRAM_SHA_256_PLUS_NAME` (`common/scram-common.h`).
pub const SCRAM_SHA_256_PLUS_NAME: &[u8] = b"SCRAM-SHA-256-PLUS";

/// `PG_MAX_SASL_MESSAGE_LENGTH` (`libpq/sasl.h`).
pub const PG_MAX_SASL_MESSAGE_LENGTH: i32 = 1024;

/// `PASSWORD_TYPE_SCRAM_SHA_256` (`libpq/crypt.h` `PasswordType`).
const PASSWORD_TYPE_SCRAM_SHA_256: i32 = authid::PasswordType::ScramSha256 as i32;

// libpq/sasl.h exchange results.
/// `PG_SASL_EXCHANGE_CONTINUE`.
pub const PG_SASL_EXCHANGE_CONTINUE: i32 = 0;
/// `PG_SASL_EXCHANGE_SUCCESS`.
pub const PG_SASL_EXCHANGE_SUCCESS: i32 = 1;
/// `PG_SASL_EXCHANGE_FAILURE`.
pub const PG_SASL_EXCHANGE_FAILURE: i32 = 2;

// c.h status codes.
const STATUS_OK: i32 = 0;
const STATUS_ERROR: i32 = -1;
const STATUS_EOF: i32 = -2;

// libpq/protocol.h.
const PqMsg_SASLResponse: u8 = b'p';

/// `int scram_sha_256_iterations = SCRAM_SHA_256_DEFAULT_ITERATIONS;` — the GUC
/// backing variable controlling how many iterations new secrets use.
pub static SCRAM_SHA_256_ITERATIONS: AtomicI32 = AtomicI32::new(SCRAM_SHA_256_DEFAULT_ITERATIONS);

fn scram_sha_256_iterations() -> i32 {
    SCRAM_SHA_256_ITERATIONS.load(Ordering::Relaxed)
}

/// Write side of the `scram_iterations` GUC accessor (`*conf->variable = ...`).
fn scram_sha_256_iterations_set(value: i32) {
    SCRAM_SHA_256_ITERATIONS.store(value, Ordering::Relaxed);
}

/// `ErrorLocation` for an `ereport(...)` raised from auth-scram.c.
fn here(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("auth-scram.c", 0, funcname)
}

/// `elog(ERROR, msg)` — an internal error (`ERRCODE_INTERNAL_ERROR`),
/// returned as a `PgError` to be raised by the caller.
fn elog_error(msg: impl Into<String>) -> PgError {
    ereport(ERROR)
        .errcode(ERRCODE_INTERNAL_ERROR)
        .errmsg_internal(msg)
        .into_error()
        .with_error_location(here("auth-scram"))
}

// ===========================================================================
// scram_state — per-exchange status tracker (file-internal in C).
// ===========================================================================

#[derive(Clone, Copy, PartialEq, Eq)]
enum scram_state_enum {
    SCRAM_AUTH_INIT,
    SCRAM_AUTH_SALT_SENT,
    SCRAM_AUTH_FINISHED,
}

struct scram_state {
    state: scram_state_enum,

    /// `username` from the startup packet (`port->user_name`), kept for the
    /// `LOG`-only "invalid SCRAM secret" message. Never echoed to the client.
    username: String,

    channel_binding_in_use: bool,

    hash_type: pg_cryptohash_type,
    key_length: i32,

    iterations: i32,
    /// base64-encoded salt.
    salt: Option<String>,
    ClientKey: [u8; SCRAM_MAX_KEY_LEN],
    StoredKey: [u8; SCRAM_MAX_KEY_LEN],
    ServerKey: [u8; SCRAM_MAX_KEY_LEN],

    // Fields of the first message from client.
    cbind_flag: u8,
    client_first_message_bare: Option<Vec<u8>>,
    #[allow(dead_code)]
    client_username: Option<Vec<u8>>,
    client_nonce: Option<Vec<u8>>,

    // Fields from the last message from client.
    client_final_message_without_proof: Option<Vec<u8>>,
    client_final_nonce: Option<Vec<u8>>,
    ClientProof: [u8; SCRAM_MAX_KEY_LEN],

    // Fields generated in the server.
    server_first_message: Option<Vec<u8>>,
    server_nonce: Option<Vec<u8>>,

    doomed: bool,
    logdetail: Option<String>,
}

impl scram_state {
    /// `palloc0(sizeof(scram_state))`.
    fn new() -> Self {
        scram_state {
            state: scram_state_enum::SCRAM_AUTH_INIT,
            username: String::new(),
            channel_binding_in_use: false,
            hash_type: PG_SHA256,
            key_length: 0,
            iterations: 0,
            salt: None,
            ClientKey: [0; SCRAM_MAX_KEY_LEN],
            StoredKey: [0; SCRAM_MAX_KEY_LEN],
            ServerKey: [0; SCRAM_MAX_KEY_LEN],
            cbind_flag: 0,
            client_first_message_bare: None,
            client_username: None,
            client_nonce: None,
            client_final_message_without_proof: None,
            client_final_nonce: None,
            ClientProof: [0; SCRAM_MAX_KEY_LEN],
            server_first_message: None,
            server_nonce: None,
            doomed: false,
            logdetail: None,
        }
    }
}

// ===========================================================================
// scram_get_mechanisms
// ===========================================================================

/// Append the supported SASL mechanism names to `buf`, `'\0'`-separated.
///
/// (`auth-scram.c:scram_get_mechanisms`.) Channel binding is only advertised
/// with SSL; this build is `!USE_SSL`, so only the non-PLUS variant is listed.
fn scram_get_mechanisms(_port: &Port, buf: &mut Vec<u8>) {
    // #ifdef USE_SSL: the PLUS variant goes first when port->ssl_in_use. Not
    // compiled in here.
    buf.extend_from_slice(SCRAM_SHA_256_NAME);
    buf.push(b'\0');
}

// ===========================================================================
// scram_init
// ===========================================================================

/// Initialize a new SCRAM authentication exchange status tracker.
///
/// (`auth-scram.c:scram_init`.) `selected_mech` is the mechanism the client
/// chose; `shadow_pass` is the role's stored secret (`None` ⇒ dummy auth).
fn scram_init(
    port: &Port,
    selected_mech: &[u8],
    shadow_pass: Option<&str>,
) -> PgResult<scram_state> {
    let mut state = scram_state::new();
    state.username = port.user_name.clone().unwrap_or_default();
    state.state = scram_state_enum::SCRAM_AUTH_INIT;

    // Parse the selected mechanism. Without SSL we never advertised the PLUS
    // variant, so selecting it is a protocol violation like any unsupported
    // mechanism.
    // #ifdef USE_SSL: SCRAM_SHA_256_PLUS_NAME && port->ssl_in_use arm omitted.
    if selected_mech == SCRAM_SHA_256_NAME {
        state.channel_binding_in_use = false;
    } else {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg("client selected an invalid SASL authentication mechanism")
            .into_error()
            .with_error_location(here("scram_init")));
    }

    // Parse the stored secret.
    #[allow(unused_assignments)]
    let mut got_secret = false;
    if let Some(shadow_pass) = shadow_pass {
        let password_type =
            user_seams::get_password_type::call(shadow_pass.to_string())? as i32;

        if password_type == PASSWORD_TYPE_SCRAM_SHA_256 {
            let mut salt: Option<String> = None;
            if parse_scram_secret(
                shadow_pass.as_bytes(),
                &mut state.iterations,
                &mut state.hash_type,
                &mut state.key_length,
                &mut salt,
                &mut state.StoredKey,
                &mut state.ServerKey,
            )? {
                state.salt = salt;
                got_secret = true;
            } else {
                // Looked like a SCRAM secret, but could not be parsed.
                ereport(LOG)
                    .errmsg(format!(
                        "invalid SCRAM secret for user \"{}\"",
                        state.username
                    ))
                    .finish(here("scram_init"))?;
                got_secret = false;
            }
        } else {
            // The user doesn't have a SCRAM secret. (You cannot do SCRAM
            // authentication with an MD5 hash.)
            state.logdetail = Some(format!(
                "User \"{}\" does not have a valid SCRAM secret.",
                state.username
            ));
            got_secret = false;
        }
    } else {
        // The caller requested a dummy authentication. This is considered
        // normal, since the caller requested it, so don't set log detail.
        got_secret = false;
    }

    // If the user did not have a valid SCRAM secret, go through the motions
    // with a mock one and fail as if the client supplied an incorrect
    // password. This avoids revealing information to an attacker.
    if !got_secret {
        let mut salt: Option<String> = None;
        mock_scram_secret(
            &state.username,
            &mut state.hash_type,
            &mut state.iterations,
            &mut state.key_length,
            &mut salt,
            &mut state.StoredKey,
            &mut state.ServerKey,
        )?;
        state.salt = salt;
        state.doomed = true;
    }

    Ok(state)
}

// ===========================================================================
// scram_exchange
// ===========================================================================

/// Result of one [`scram_exchange`] step: the SASL result code plus the
/// optional output message to send to the client.
struct ExchangeOutput {
    result: i32,
    output: Option<Vec<u8>>,
}

/// Continue a SCRAM authentication exchange.
///
/// (`auth-scram.c:scram_exchange`.) `input` is the client's SCRAM payload, or
/// `None` for an absent Initial Client Response. On `Ok`, returns the result
/// code and any output message. `state.logdetail` is set on failure by the
/// callee; the caller (driver loop) surfaces it.
fn scram_exchange(
    state: &mut scram_state,
    port: &Port,
    input: Option<&[u8]>,
) -> PgResult<ExchangeOutput> {
    let mut output: Option<Vec<u8>> = None;

    // If the client didn't include an Initial Client Response, send an empty
    // challenge; the client then responds with the usual ICR data.
    let input = match input {
        None => {
            debug_assert!(state.state == scram_state_enum::SCRAM_AUTH_INIT);
            return Ok(ExchangeOutput {
                result: PG_SASL_EXCHANGE_CONTINUE,
                output: Some(Vec::new()), // pstrdup("")
            });
        }
        Some(i) => i,
    };

    // Check that the input length agrees with the string length of the input.
    // (The caller guarantees a NUL terminator at input[inputlen]; `input` here
    // is exactly the payload bytes with no embedded NUL expected.)
    if input.is_empty() {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg("malformed SCRAM message")
            .errdetail("The message is empty.")
            .into_error()
            .with_error_location(here("scram_exchange")));
    }
    if input.len() != strlen(input) {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg("malformed SCRAM message")
            .errdetail("Message length does not match input length.")
            .into_error()
            .with_error_location(here("scram_exchange")));
    }

    let result: i32;
    match state.state {
        scram_state_enum::SCRAM_AUTH_INIT => {
            // Initialization phase. Receive the first message from client and
            // be sure it parsed correctly. Then send the challenge.
            read_client_first_message(state, input)?;

            output = Some(build_server_first_message(state)?);

            state.state = scram_state_enum::SCRAM_AUTH_SALT_SENT;
            result = PG_SASL_EXCHANGE_CONTINUE;
        }

        scram_state_enum::SCRAM_AUTH_SALT_SENT => {
            // Final phase. Receive the response, verify, and let the client
            // know whether everything went well.
            read_client_final_message(state, input)?;

            if !verify_final_nonce(state) {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_PROTOCOL_VIOLATION)
                    .errmsg("invalid SCRAM response")
                    .errdetail("Nonce does not match.")
                    .into_error()
                    .with_error_location(here("scram_exchange")));
            }

            // Check the client proof. We calculate it even in a mock
            // authentication (bound to fail) to thwart timing attacks. The
            // order of these checks is intentional.
            if !verify_client_proof(state)? || state.doomed {
                result = PG_SASL_EXCHANGE_FAILURE;
            } else {
                output = Some(build_server_final_message(state)?);
                result = PG_SASL_EXCHANGE_SUCCESS;
                state.state = scram_state_enum::SCRAM_AUTH_FINISHED;
            }
        }

        scram_state_enum::SCRAM_AUTH_FINISHED => {
            elog(ERROR, "invalid SCRAM exchange state").map_err(|e| {
                e.with_error_location(here("scram_exchange"))
            })?;
            unreachable!("elog(ERROR) does not return");
        }
    }

    // On success at the finished state, the C copies the derived keys into
    // MyProcPort. We surface them through `state` so the driver writes the
    // ambient Port (it holds the &mut Port). See `check_scram_sasl_auth`.
    let _ = port; // `port` is unused in the !USE_SSL build of this function.
    Ok(ExchangeOutput { result, output })
}

// ===========================================================================
// pg_be_scram_build_secret
// ===========================================================================

/// Construct a SCRAM secret for storing in `pg_authid.rolpassword`.
///
/// (`auth-scram.c:pg_be_scram_build_secret`.)
pub fn pg_be_scram_build_secret(password: &[u8]) -> PgResult<String> {
    // Normalize the password with SASLprep; if that doesn't work (not valid
    // UTF-8 / prohibited chars), proceed with the original password.
    let prep = scram_seams::pg_saslprep::call(password.to_vec());
    let password: &[u8] = match prep.as_deref() {
        Some(p) => p,
        None => password,
    };

    // Generate random salt.
    let mut saltbuf = [0u8; SCRAM_DEFAULT_SALT_LEN];
    if !pg_strong_random::pg_strong_random::call(&mut saltbuf) {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INTERNAL_ERROR)
            .errmsg("could not generate random salt")
            .into_error()
            .with_error_location(here("pg_be_scram_build_secret")));
    }

    scram_seams::scram_build_secret::call(
        saltbuf.to_vec(),
        scram_sha_256_iterations(),
        password.to_vec(),
    )
    .map_err(elog_error)
}

// Re-export through a stable local path for clarity.
use pg_strong_random_seams as pg_strong_random;

// ===========================================================================
// scram_verify_plain_password
// ===========================================================================

/// Verify a plaintext password against a SCRAM secret.
///
/// (`auth-scram.c:scram_verify_plain_password`.)
pub fn scram_verify_plain_password(
    username: &str,
    password: &[u8],
    secret: &[u8],
) -> PgResult<bool> {
    let mut iterations: i32 = 0;
    let mut hash_type: pg_cryptohash_type = PG_SHA256;
    let mut key_length: i32 = 0;
    let mut encoded_salt: Option<String> = None;
    let mut stored_key = [0u8; SCRAM_MAX_KEY_LEN];
    let mut server_key = [0u8; SCRAM_MAX_KEY_LEN];

    if !parse_scram_secret(
        secret,
        &mut iterations,
        &mut hash_type,
        &mut key_length,
        &mut encoded_salt,
        &mut stored_key,
        &mut server_key,
    )? {
        // Looked like a SCRAM secret, but could not be parsed.
        ereport(LOG)
            .errmsg(format!("invalid SCRAM secret for user \"{username}\""))
            .finish(here("scram_verify_plain_password"))?;
        return Ok(false);
    }
    let encoded_salt = encoded_salt.expect("parse_scram_secret set salt on success");

    let salt_src = encoded_salt.as_bytes();
    let mut salt = vec![0u8; pg_b64_dec_len(salt_src.len() as i32) as usize];
    let salt_cap = salt.len() as i32;
    let saltlen = pg_b64_decode(salt_src, salt_src.len() as i32, &mut salt, salt_cap);
    if saltlen < 0 {
        ereport(LOG)
            .errmsg(format!("invalid SCRAM secret for user \"{username}\""))
            .finish(here("scram_verify_plain_password"))?;
        return Ok(false);
    }
    let saltlen = saltlen as usize;

    // Normalize the password.
    let prep = scram_seams::pg_saslprep::call(password.to_vec());
    let password: &[u8] = match prep.as_deref() {
        Some(p) => p,
        None => password,
    };

    // Compute Server Key based on the user-supplied plaintext password:
    //   scram_SaltedPassword(...) then scram_ServerKey(...).
    let _ = hash_type;
    let salted = scram_seams::scram_salted_password::call(
        password.to_vec(),
        salt[..saltlen].to_vec(),
        iterations,
    )
    .map_err(|errstr| elog_error(format!("could not compute server key: {errstr}")))?;
    let computed_key = scram_seams::scram_server_key::call(salted.to_vec())
        .map_err(|errstr| elog_error(format!("could not compute server key: {errstr}")))?;

    // Compare the secret's Server Key with the computed one.
    let kl = key_length as usize;
    Ok(computed_key[..kl] == server_key[..kl])
}

// ===========================================================================
// parse_scram_secret
// ===========================================================================

/// Parse and validate the format of a given SCRAM secret.
///
/// On success the iteration count, salt (base64-encoded), stored key, and
/// server key are extracted. Returns `true` on success.
///
/// (`auth-scram.c:parse_scram_secret`.)
pub fn parse_scram_secret(
    secret: &[u8],
    iterations: &mut i32,
    hash_type: &mut pg_cryptohash_type,
    key_length: &mut i32,
    salt: &mut Option<String>,
    stored_key: &mut [u8; SCRAM_MAX_KEY_LEN],
    server_key: &mut [u8; SCRAM_MAX_KEY_LEN],
) -> PgResult<bool> {
    // The secret is of form:
    //   SCRAM-SHA-256$<iterations>:<salt>$<storedkey>:<serverkey>
    let v: Vec<u8> = secret.to_vec();
    let mut cur: &[u8] = &v;

    let scheme_str = match strsep(&mut cur, b"$") {
        (token, true) => token,
        (_, false) => return invalid_secret(salt),
    };
    let iterations_str = match strsep(&mut cur, b":") {
        (token, true) => token,
        (_, false) => return invalid_secret(salt),
    };
    let salt_str = match strsep(&mut cur, b"$") {
        (token, true) => token,
        (_, false) => return invalid_secret(salt),
    };
    let storedkey_str = match strsep(&mut cur, b":") {
        (token, true) => token,
        (_, false) => return invalid_secret(salt),
    };
    let serverkey_str = cur; // serverkey_str = v;

    // Parse the fields.
    if scheme_str != SCRAM_SHA_256_NAME {
        return invalid_secret(salt);
    }
    *hash_type = PG_SHA256;
    *key_length = SCRAM_SHA_256_KEY_LEN as i32;

    // errno = 0; *iterations = strtol(iterations_str, &p, 10); if (*p || errno) goto invalid;
    match strtol_base10_full(iterations_str) {
        Some(value) => *iterations = value,
        None => return invalid_secret(salt),
    }

    // Verify that the salt is valid Base64 by decoding it; we return the
    // encoded version to the caller.
    let mut decoded_salt_buf = vec![0u8; pg_b64_dec_len(strlen(salt_str) as i32) as usize];
    let cap = decoded_salt_buf.len() as i32;
    let dl = pg_b64_decode(salt_str, strlen(salt_str) as i32, &mut decoded_salt_buf, cap);
    if dl < 0 {
        return invalid_secret(salt);
    }
    *salt = Some(bytes_to_str_lossless(&salt_str[..strlen(salt_str)])); // pstrdup(salt_str)

    // Decode StoredKey and ServerKey.
    let mut decoded_stored_buf = vec![0u8; pg_b64_dec_len(strlen(storedkey_str) as i32) as usize];
    let cap = decoded_stored_buf.len() as i32;
    let decoded_len = pg_b64_decode(
        storedkey_str,
        strlen(storedkey_str) as i32,
        &mut decoded_stored_buf,
        cap,
    );
    if decoded_len != *key_length {
        return invalid_secret(salt);
    }
    stored_key[..*key_length as usize]
        .copy_from_slice(&decoded_stored_buf[..*key_length as usize]);

    let mut decoded_server_buf = vec![0u8; pg_b64_dec_len(strlen(serverkey_str) as i32) as usize];
    let cap = decoded_server_buf.len() as i32;
    let decoded_len = pg_b64_decode(
        serverkey_str,
        strlen(serverkey_str) as i32,
        &mut decoded_server_buf,
        cap,
    );
    if decoded_len != *key_length {
        return invalid_secret(salt);
    }
    server_key[..*key_length as usize]
        .copy_from_slice(&decoded_server_buf[..*key_length as usize]);

    Ok(true)
}

/// `invalid_secret:` label of `parse_scram_secret`.
fn invalid_secret(salt: &mut Option<String>) -> PgResult<bool> {
    *salt = None;
    Ok(false)
}

// ===========================================================================
// mock_scram_secret
// ===========================================================================

/// Generate plausible SCRAM secret parameters for mock authentication.
///
/// (`auth-scram.c:mock_scram_secret`.)
fn mock_scram_secret(
    username: &str,
    hash_type: &mut pg_cryptohash_type,
    iterations: &mut i32,
    key_length: &mut i32,
    salt: &mut Option<String>,
    stored_key: &mut [u8; SCRAM_MAX_KEY_LEN],
    server_key: &mut [u8; SCRAM_MAX_KEY_LEN],
) -> PgResult<()> {
    // Enforce the use of SHA-256, which would be realistic enough.
    *hash_type = PG_SHA256;
    *key_length = SCRAM_SHA_256_KEY_LEN as i32;

    // Generate deterministic salt using the cluster's mock-auth nonce. Error
    // messages must stay generic (no info to an attacker).
    let raw_salt = match scram_mock_salt(username, *hash_type, *key_length)? {
        Some(raw_salt) => raw_salt,
        None => return Err(elog_error("could not encode salt")),
    };

    let encoded_len_cap = pg_b64_enc_len(SCRAM_DEFAULT_SALT_LEN as i32);
    // don't forget the zero-terminator
    let mut encoded_salt = vec![0u8; encoded_len_cap as usize + 1];
    let encoded_len = pg_b64_encode(
        &raw_salt[..SCRAM_DEFAULT_SALT_LEN],
        SCRAM_DEFAULT_SALT_LEN as i32,
        &mut encoded_salt[..encoded_len_cap as usize],
        encoded_len_cap,
    );
    if encoded_len < 0 {
        return Err(elog_error("could not encode salt"));
    }
    encoded_salt.truncate(encoded_len as usize); // string ends at the NUL.

    *salt = Some(bytes_to_str_lossless(&encoded_salt));
    *iterations = SCRAM_SHA_256_DEFAULT_ITERATIONS;

    // StoredKey and ServerKey are not used in a doomed authentication.
    *stored_key = [0; SCRAM_MAX_KEY_LEN];
    *server_key = [0; SCRAM_MAX_KEY_LEN];

    Ok(())
}

// ===========================================================================
// read_attr_value / is_scram_printable / sanitize_char / sanitize_str /
// read_any_attr
// ===========================================================================

/// Read the value for a given attribute in a SCRAM exchange message.
///
/// `input` is a cursor into the mutable byte buffer `buf`; on return it points
/// just past the consumed token. Returns the value's byte range `[start, end)`.
///
/// (`auth-scram.c:read_attr_value`.)
fn read_attr_value(buf: &mut [u8], input: &mut usize, attr: u8) -> PgResult<(usize, usize)> {
    let mut begin = *input;

    if buf[begin] != attr {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg("malformed SCRAM message")
            .errdetail(format!(
                "Expected attribute \"{}\" but found \"{}\".",
                attr as char,
                sanitize_char(buf[begin])
            ))
            .into_error()
            .with_error_location(here("read_attr_value")));
    }
    begin += 1;

    if buf[begin] != b'=' {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg("malformed SCRAM message")
            .errdetail(format!(
                "Expected character \"=\" for attribute \"{}\".",
                attr as char
            ))
            .into_error()
            .with_error_location(here("read_attr_value")));
    }
    begin += 1;

    let mut end = begin;
    while buf[end] != 0 && buf[end] != b',' {
        end += 1;
    }

    if buf[end] != 0 {
        buf[end] = b'\0';
        *input = end + 1;
    } else {
        *input = end;
    }

    Ok((begin, end))
}

/// `is_scram_printable`: printable per RFC 5802 `printable = %x21-2B / %x2D-7E`.
///
/// (`auth-scram.c:is_scram_printable`.)
fn is_scram_printable(p: &[u8]) -> bool {
    for &c in p {
        if c == 0 {
            break;
        }
        if c < 0x21 || c > 0x7E || c == 0x2C
        /* comma */
        {
            return false;
        }
    }
    true
}

/// Convert an arbitrary byte to printable form, for error messages.
///
/// (`auth-scram.c:sanitize_char`.)
fn sanitize_char(c: u8) -> String {
    if (0x21..=0x7E).contains(&c) {
        format!("'{}'", c as char)
    } else {
        format!("0x{c:02x}")
    }
}

/// Convert an arbitrary string to printable form (truncated at 30), for error
/// messages: anything non-printable-ASCII becomes `?`.
///
/// (`auth-scram.c:sanitize_str`.)
fn sanitize_str(s: &[u8]) -> String {
    const BUFLEN: usize = 30 + 1;
    let mut buf = [0u8; BUFLEN];
    let mut i = 0;
    while i < BUFLEN - 1 {
        let c = if i < s.len() { s[i] } else { 0 };
        if c == b'\0' {
            break;
        }
        if (0x21..=0x7E).contains(&c) {
            buf[i] = c;
        } else {
            buf[i] = b'?';
        }
        i += 1;
    }
    bytes_to_str_lossless(&buf[..i])
}

/// Read the next attribute and value in a SCRAM exchange message.
///
/// The attribute character is written to `*attr_p`; the value's byte range is
/// returned.
///
/// (`auth-scram.c:read_any_attr`.)
fn read_any_attr(
    buf: &mut [u8],
    input: &mut usize,
    attr_p: Option<&mut u8>,
) -> PgResult<(usize, usize)> {
    let mut begin = *input;
    let attr = buf[begin];

    if attr == b'\0' {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg("malformed SCRAM message")
            .errdetail("Attribute expected, but found end of string.")
            .into_error()
            .with_error_location(here("read_any_attr")));
    }

    // attr-val = ALPHA "=" value
    if !((b'A'..=b'Z').contains(&attr) || (b'a'..=b'z').contains(&attr)) {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg("malformed SCRAM message")
            .errdetail(format!(
                "Attribute expected, but found invalid character \"{}\".",
                sanitize_char(attr)
            ))
            .into_error()
            .with_error_location(here("read_any_attr")));
    }
    if let Some(p) = attr_p {
        *p = attr;
    }
    begin += 1;

    if buf[begin] != b'=' {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg("malformed SCRAM message")
            .errdetail(format!(
                "Expected character \"=\" for attribute \"{}\".",
                attr as char
            ))
            .into_error()
            .with_error_location(here("read_any_attr")));
    }
    begin += 1;

    let mut end = begin;
    while buf[end] != 0 && buf[end] != b',' {
        end += 1;
    }

    if buf[end] != 0 {
        buf[end] = b'\0';
        *input = end + 1;
    } else {
        *input = end;
    }

    Ok((begin, end))
}

// ===========================================================================
// read_client_first_message
// ===========================================================================

/// Read and parse the first message from the client.
///
/// (`auth-scram.c:read_client_first_message`.) Operates on a mutable copy of
/// `input` (NUL-terminated, as C's `pstrdup`).
fn read_client_first_message(state: &mut scram_state, input: &[u8]) -> PgResult<()> {
    // p = pstrdup(input): own a NUL-terminated mutable buffer.
    let mut p: Vec<u8> = input.to_vec();
    p.push(b'\0');
    let mut cur: usize = 0;

    // Read gs2-cbind-flag.
    state.cbind_flag = p[cur];
    match p[cur] {
        b'n' => {
            // Client does not support / use channel binding.
            if state.channel_binding_in_use {
                return Err(cbind_mismatch_err("read_client_first_message"));
            }
            cur += 1;
            if p[cur] != b',' {
                return Err(comma_expected_err(p[cur], "read_client_first_message"));
            }
            cur += 1;
        }
        b'y' => {
            // Client supports channel binding and thinks the server does not.
            if state.channel_binding_in_use {
                return Err(cbind_mismatch_err("read_client_first_message"));
            }
            // #ifdef USE_SSL: if port->ssl_in_use, error — not compiled in.
            cur += 1;
            if p[cur] != b',' {
                return Err(comma_expected_err(p[cur], "read_client_first_message"));
            }
            cur += 1;
        }
        b'p' => {
            // Client requires channel binding.
            if !state.channel_binding_in_use {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_PROTOCOL_VIOLATION)
                    .errmsg("malformed SCRAM message")
                    .errdetail("The client selected SCRAM-SHA-256 without channel binding, but the SCRAM message includes channel binding data.")
                    .into_error()
                    .with_error_location(here("read_client_first_message")));
            }

            let (b, e) = read_attr_value(&mut p, &mut cur, b'p')?;
            let channel_binding_type = &p[b..e];

            // The only channel binding type we support is tls-server-end-point.
            if channel_binding_type != b"tls-server-end-point" {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_PROTOCOL_VIOLATION)
                    .errmsg(format!(
                        "unsupported SCRAM channel-binding type \"{}\"",
                        sanitize_str(channel_binding_type)
                    ))
                    .into_error()
                    .with_error_location(here("read_client_first_message")));
            }
        }
        other => {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_PROTOCOL_VIOLATION)
                .errmsg("malformed SCRAM message")
                .errdetail(format!(
                    "Unexpected channel-binding flag \"{}\".",
                    sanitize_char(other)
                ))
                .into_error()
                .with_error_location(here("read_client_first_message")));
        }
    }

    // Forbid optional authzid (authorization identity); not supported.
    if p[cur] == b'a' {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("client uses authorization identity, but it is not supported")
            .into_error()
            .with_error_location(here("read_client_first_message")));
    }
    if p[cur] != b',' {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg("malformed SCRAM message")
            .errdetail(format!(
                "Unexpected attribute \"{}\" in client-first-message.",
                sanitize_char(p[cur])
            ))
            .into_error()
            .with_error_location(here("read_client_first_message")));
    }
    cur += 1;

    // client_first_message_bare = pstrdup(p)
    state.client_first_message_bare = Some(p[cur..strlen(&p[cur..]) + cur].to_vec());

    // Mandatory extensions: not supported.
    if p[cur] == b'm' {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("client requires an unsupported SCRAM extension")
            .into_error()
            .with_error_location(here("read_client_first_message")));
    }

    // Read username. Ignored — we use the startup-message username — but kept
    // for debugging.
    let (b, e) = read_attr_value(&mut p, &mut cur, b'n')?;
    state.client_username = Some(p[b..e].to_vec());

    // Read nonce; must be printable.
    let (b, e) = read_attr_value(&mut p, &mut cur, b'r')?;
    let client_nonce = p[b..e].to_vec();
    if !is_scram_printable(&client_nonce) {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg("non-printable characters in SCRAM nonce")
            .into_error()
            .with_error_location(here("read_client_first_message")));
    }
    state.client_nonce = Some(client_nonce);

    // Any number of optional extensions follow; we ignore them.
    while p[cur] != b'\0' {
        read_any_attr(&mut p, &mut cur, None)?;
    }

    Ok(())
}

/// The `ereport` raised when the PLUS mechanism was selected but the client
/// message omits channel-binding data (the shared `n`/`y` arm error).
fn cbind_mismatch_err(func: &'static str) -> PgError {
    ereport(ERROR)
        .errcode(ERRCODE_PROTOCOL_VIOLATION)
        .errmsg("malformed SCRAM message")
        .errdetail("The client selected SCRAM-SHA-256-PLUS, but the SCRAM message does not include channel binding data.")
        .into_error()
        .with_error_location(here(func))
}

/// The `ereport` raised when a `,` was expected after the gs2-cbind-flag.
fn comma_expected_err(found: u8, func: &'static str) -> PgError {
    ereport(ERROR)
        .errcode(ERRCODE_PROTOCOL_VIOLATION)
        .errmsg("malformed SCRAM message")
        .errdetail(format!(
            "Comma expected, but found character \"{}\".",
            sanitize_char(found)
        ))
        .into_error()
        .with_error_location(here(func))
}

// ===========================================================================
// verify_final_nonce
// ===========================================================================

/// Verify the final nonce in the last client message.
///
/// (`auth-scram.c:verify_final_nonce`.)
fn verify_final_nonce(state: &scram_state) -> bool {
    let client_nonce = state.client_nonce.as_deref().unwrap_or(b"");
    let server_nonce = state.server_nonce.as_deref().unwrap_or(b"");
    let final_nonce = state.client_final_nonce.as_deref().unwrap_or(b"");

    let client_nonce_len = strlen(client_nonce);
    let server_nonce_len = strlen(server_nonce);
    let final_nonce_len = strlen(final_nonce);

    if final_nonce_len != client_nonce_len + server_nonce_len {
        return false;
    }
    if final_nonce[..client_nonce_len] != client_nonce[..client_nonce_len] {
        return false;
    }
    if final_nonce[client_nonce_len..client_nonce_len + server_nonce_len]
        != server_nonce[..server_nonce_len]
    {
        return false;
    }
    true
}

// ===========================================================================
// verify_client_proof
// ===========================================================================

/// Verify the client proof in the last client message.
///
/// (`auth-scram.c:verify_client_proof`.) Returns `Ok(true)` on a match.
fn verify_client_proof(state: &mut scram_state) -> PgResult<bool> {
    let kl = state.key_length as usize;
    let cfmb = state.client_first_message_bare.as_deref().unwrap_or(b"");
    let sfm = state.server_first_message.as_deref().unwrap_or(b"");
    let cfmwp = state
        .client_final_message_without_proof
        .as_deref()
        .unwrap_or(b"");

    // ClientSignature = HMAC(StoredKey, AuthMessage), where AuthMessage is
    //   client-first-message-bare "," server-first-message ","
    //   client-final-message-without-proof
    // The C feeds these in five pg_hmac_update calls; HMAC over the
    // concatenation is identical.
    let mut msg = Vec::with_capacity(cfmb.len() + 1 + sfm.len() + 1 + cfmwp.len());
    msg.extend_from_slice(&cfmb[..strlen(cfmb)]);
    msg.push(b',');
    msg.extend_from_slice(&sfm[..strlen(sfm)]);
    msg.push(b',');
    msg.extend_from_slice(&cfmwp[..strlen(cfmwp)]);

    let client_signature = scram_seams::pg_hmac_sha256::call(state.StoredKey[..kl].to_vec(), msg)
        .map_err(|e| elog_error(format!("could not calculate client signature: {e}")))?;

    // Extract the ClientKey that the client calculated from the proof.
    for i in 0..kl {
        state.ClientKey[i] = state.ClientProof[i] ^ client_signature[i];
    }

    // Hash it once more, and compare with StoredKey.
    let client_stored_key = scram_seams::scram_h::call(state.ClientKey[..kl].to_vec())
        .map_err(|e| elog_error(format!("could not hash stored key: {e}")))?;

    if client_stored_key[..kl] != state.StoredKey[..kl] {
        return Ok(false);
    }
    Ok(true)
}

// ===========================================================================
// build_server_first_message
// ===========================================================================

/// Build the first server-side message.
///
/// (`auth-scram.c:build_server_first_message`.)
fn build_server_first_message(state: &mut scram_state) -> PgResult<Vec<u8>> {
    // Generate random bytes and base64-encode them for the server nonce.
    let mut raw_nonce = [0u8; SCRAM_RAW_NONCE_LEN];
    if !pg_strong_random::pg_strong_random::call(&mut raw_nonce) {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INTERNAL_ERROR)
            .errmsg("could not generate random nonce")
            .into_error()
            .with_error_location(here("build_server_first_message")));
    }

    let encoded_len_cap = pg_b64_enc_len(SCRAM_RAW_NONCE_LEN as i32);
    // don't forget the zero-terminator
    let mut server_nonce = vec![0u8; encoded_len_cap as usize + 1];
    let encoded_len = pg_b64_encode(
        &raw_nonce,
        SCRAM_RAW_NONCE_LEN as i32,
        &mut server_nonce[..encoded_len_cap as usize],
        encoded_len_cap,
    );
    if encoded_len < 0 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INTERNAL_ERROR)
            .errmsg("could not encode random nonce")
            .into_error()
            .with_error_location(here("build_server_first_message")));
    }
    server_nonce.truncate(encoded_len as usize);
    state.server_nonce = Some(server_nonce);

    let client_nonce = state.client_nonce.as_deref().unwrap_or(b"");
    let server_nonce = state.server_nonce.as_deref().unwrap_or(b"");
    let salt = state.salt.as_deref().unwrap_or("");

    // psprintf("r=%s%s,s=%s,i=%d", client_nonce, server_nonce, salt, iterations)
    let mut sfm: Vec<u8> = Vec::new();
    sfm.extend_from_slice(b"r=");
    sfm.extend_from_slice(&client_nonce[..strlen(client_nonce)]);
    sfm.extend_from_slice(&server_nonce[..strlen(server_nonce)]);
    sfm.extend_from_slice(b",s=");
    sfm.extend_from_slice(salt.as_bytes());
    sfm.extend_from_slice(b",i=");
    sfm.extend_from_slice(format!("{}", state.iterations).as_bytes());

    state.server_first_message = Some(sfm.clone());

    Ok(sfm) // pstrdup(state->server_first_message)
}

// ===========================================================================
// read_client_final_message
// ===========================================================================

/// Read and parse the final message from the client.
///
/// (`auth-scram.c:read_client_final_message`.)
fn read_client_final_message(state: &mut scram_state, input: &[u8]) -> PgResult<()> {
    // begin = p = pstrdup(input)
    let mut p: Vec<u8> = input.to_vec();
    p.push(b'\0');
    let mut cur: usize = 0;

    // Read channel binding.
    let (b, e) = read_attr_value(&mut p, &mut cur, b'c')?;
    let channel_binding = p[b..e].to_vec();
    if state.channel_binding_in_use {
        // #ifdef USE_SSL: compare client value against the expected
        //   base64("p=tls-server-end-point,," || cert-hash). Not compiled in;
        //   the C #else path elogs. We never set channel_binding_in_use without
        //   SSL, so this is unreachable, but mirror the #else guard.
        elog(ERROR, "channel binding not supported by this build")
            .map_err(|er| er.with_error_location(here("read_client_final_message")))?;
        unreachable!("elog(ERROR) does not return");
    } else {
        // Without channel binding, the binding data must be "biws" ("n,,") or
        // "eSws" ("y,,"), matching the flag the client originally sent.
        let ok = (channel_binding == b"biws" && state.cbind_flag == b'n')
            || (channel_binding == b"eSws" && state.cbind_flag == b'y');
        if !ok {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_PROTOCOL_VIOLATION)
                .errmsg("unexpected SCRAM channel-binding attribute in client-final-message")
                .into_error()
                .with_error_location(here("read_client_final_message")));
        }
    }

    let (b, e) = read_attr_value(&mut p, &mut cur, b'r')?;
    state.client_final_nonce = Some(p[b..e].to_vec());

    // Ignore optional extensions; read until the "p" attribute.
    // proof = p - 1 (the byte before the start of each attribute, i.e. the
    // separating comma/start); value = read_any_attr(&p, &attr).
    let mut attr: u8 = 0;
    let mut proof_pos: usize;
    let value_range;
    loop {
        // C: proof = p - 1; here `cur` points at the attribute start, so the
        // separator (or message start) is at cur - 1. At the first iteration
        // C's p is just past the nonce's terminating NUL, so p-1 is that NUL
        // position (== the comma in the original buffer); we use cur-1 likewise.
        proof_pos = cur.wrapping_sub(1);
        let (vb, ve) = read_any_attr(&mut p, &mut cur, Some(&mut attr))?;
        if attr == b'p' {
            value_range = (vb, ve);
            break;
        }
    }
    let (vb, ve) = value_range;
    let value = &p[vb..ve];

    let client_proof_len = pg_b64_dec_len(strlen(value) as i32);
    let mut client_proof = vec![0u8; client_proof_len as usize];
    let decoded = pg_b64_decode(
        value,
        strlen(value) as i32,
        &mut client_proof,
        client_proof_len,
    );
    if decoded != state.key_length {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg("malformed SCRAM message")
            .errdetail("Malformed proof in client-final-message.")
            .into_error()
            .with_error_location(here("read_client_final_message")));
    }
    state.ClientProof[..state.key_length as usize]
        .copy_from_slice(&client_proof[..state.key_length as usize]);

    if p[cur] != b'\0' {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg("malformed SCRAM message")
            .errdetail("Garbage found at the end of client-final-message.")
            .into_error()
            .with_error_location(here("read_client_final_message")));
    }

    // client_final_message_without_proof = input[0 .. proof - begin]
    // proof - begin == proof_pos (begin is offset 0 of the buffer).
    state.client_final_message_without_proof = Some(input[..proof_pos].to_vec());

    Ok(())
}

// ===========================================================================
// build_server_final_message
// ===========================================================================

/// Build the final server-side message.
///
/// (`auth-scram.c:build_server_final_message`.)
fn build_server_final_message(state: &scram_state) -> PgResult<Vec<u8>> {
    let kl = state.key_length as usize;
    let cfmb = state.client_first_message_bare.as_deref().unwrap_or(b"");
    let sfm = state.server_first_message.as_deref().unwrap_or(b"");
    let cfmwp = state
        .client_final_message_without_proof
        .as_deref()
        .unwrap_or(b"");

    // ServerSignature = HMAC(ServerKey, AuthMessage); same five-update message
    // as in verify_client_proof.
    let mut msg = Vec::with_capacity(cfmb.len() + 1 + sfm.len() + 1 + cfmwp.len());
    msg.extend_from_slice(&cfmb[..strlen(cfmb)]);
    msg.push(b',');
    msg.extend_from_slice(&sfm[..strlen(sfm)]);
    msg.push(b',');
    msg.extend_from_slice(&cfmwp[..strlen(cfmwp)]);

    let server_signature = scram_seams::pg_hmac_sha256::call(state.ServerKey[..kl].to_vec(), msg)
        .map_err(|e| elog_error(format!("could not calculate server signature: {e}")))?;

    let siglen_cap = pg_b64_enc_len(state.key_length);
    // don't forget the zero-terminator
    let mut server_signature_base64 = vec![0u8; siglen_cap as usize + 1];
    let siglen = pg_b64_encode(
        &server_signature[..kl],
        state.key_length,
        &mut server_signature_base64[..siglen_cap as usize],
        siglen_cap,
    );
    if siglen < 0 {
        return Err(elog_error("could not encode server signature"));
    }
    server_signature_base64.truncate(siglen as usize);

    // psprintf("v=%s", server_signature_base64)
    let mut out: Vec<u8> = Vec::new();
    out.extend_from_slice(b"v=");
    out.extend_from_slice(&server_signature_base64);
    Ok(out)
}

// ===========================================================================
// scram_mock_salt
// ===========================================================================

/// Deterministically generate salt for mock authentication, via a SHA-256
/// hash of the username and the cluster's mock-auth nonce.
///
/// (`auth-scram.c:scram_mock_salt`.) Returns the digest bytes, or `None` on a
/// crypto failure / missing nonce.
fn scram_mock_salt(
    username: &str,
    hash_type: pg_cryptohash_type,
    key_length: i32,
) -> PgResult<Option<Vec<u8>>> {
    let mock_auth_nonce = match scram_seams::get_mock_authentication_nonce::call() {
        Some(n) => n,
        None => return Ok(None),
    };

    // StaticAssertDecl(PG_SHA256_DIGEST_LENGTH >= SCRAM_DEFAULT_SALT_LEN).
    const _: () = assert!(PG_SHA256_DIGEST_LENGTH >= SCRAM_DEFAULT_SALT_LEN);
    // Assert(hash_type == PG_SHA256).
    debug_assert!(hash_type == PG_SHA256);

    // pg_cryptohash_create / init / update(username) / update(nonce) / final.
    let ctx = cryptohash_seams::pg_cryptohash_create::call(hash_type);
    if ctx.is_null() {
        return Ok(None);
    }

    let username_bytes = username.as_bytes();
    let mut sha_digest = [0u8; SCRAM_MAX_KEY_LEN];

    let failed = cryptohash_seams::pg_cryptohash_init::call(ctx) < 0
        || cryptohash_seams::pg_cryptohash_update::call(
            ctx,
            username_bytes.as_ptr(),
            username_bytes.len(),
        ) < 0
        || cryptohash_seams::pg_cryptohash_update::call(
            ctx,
            mock_auth_nonce.as_ptr(),
            MOCK_AUTH_NONCE_LEN,
        ) < 0
        || cryptohash_seams::pg_cryptohash_final::call(
            ctx,
            sha_digest.as_mut_ptr(),
            key_length as usize,
        ) < 0;

    if failed {
        cryptohash_seams::pg_cryptohash_free::call(ctx);
        return Ok(None);
    }
    cryptohash_seams::pg_cryptohash_free::call(ctx);

    Ok(Some(sha_digest.to_vec()))
}

// ===========================================================================
// CheckSASLAuth driver loop for SCRAM (auth-sasl.c) — inward seam body.
// ===========================================================================

/// `CheckSASLAuth(&pg_be_scram_mech, port, shadow_pass, &logdetail)`
/// (auth-sasl.c) specialized to the SCRAM mechanism. Runs the SASL message
/// loop over the live connection. Returns `(status, logdetail)`.
fn check_scram_sasl_auth_impl(
    port: &mut Port,
    shadow_pass: Option<String>,
) -> PgResult<(i32, Option<String>)> {
    // Send the SASL authentication request, including the supported mechanisms.
    let mut sasl_mechs: Vec<u8> = Vec::new();
    scram_get_mechanisms(port, &mut sasl_mechs);
    // Put another '\0' to mark that the list is finished.
    sasl_mechs.push(b'\0');

    auth::sendAuthRequest(port, auth::AUTH_REQ_SASL, &sasl_mechs)?;

    // Loop through the SASL message exchange. First message is always from the
    // client. All client→server messages are 'p' (SASLResponse) packets.
    let mut opaq: Option<scram_state> = None;
    let mut logdetail: Option<String> = None;
    let mut initial = true;
    let mut result: i32;

    loop {
        pqcomm::pq_startmsgread()?;
        let mtype = pqcomm::pq_getbyte()?;
        if mtype != PqMsg_SASLResponse as i32 {
            if mtype != -1 {
                // Only log error if the client didn't disconnect.
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_PROTOCOL_VIOLATION)
                    .errmsg(format!("expected SASL response, got message type {mtype}"))
                    .into_error()
                    .with_error_location(here("CheckSASLAuth")));
            } else {
                return Ok((STATUS_EOF, logdetail));
            }
        }

        // Get the actual SASL message.
        let mcx = mcx::MemoryContext::new("CheckSASLAuth");
        let mut buf = stringinfo::StringInfo::new_in(mcx.mcx());
        if pqcomm::pq_getmessage(&mut buf, PG_MAX_SASL_MESSAGE_LENGTH)? != 0 {
            // EOF — pq_getmessage already logged the error.
            return Ok((STATUS_ERROR, logdetail));
        }

        // The first SASLInitialResponse indicates the selected mechanism and
        // carries an optional Initial Client Response. Subsequent SASLResponse
        // messages carry just the payload.
        let input: Option<Vec<u8>>;
        if initial {
            let selected_mech = pqformat::pq_getmsgrawstring(&mut buf)?.to_vec();

            // Initialize the status tracker. If the user doesn't exist / has no
            // valid password, scram_init still goes through the motions with
            // the 'doomed' flag set, to avoid revealing which usernames/
            // passwords are valid.
            let state = scram_init(port, &selected_mech, shadow_pass.as_deref())?;
            opaq = Some(state);

            let inputlen = pqformat::pq_getmsgint(&mut buf, 4)? as i32;
            if inputlen == -1 {
                input = None;
            } else {
                input = Some(
                    pqformat::pq_getmsgbytes(&mut buf, inputlen as usize)?.to_vec(),
                );
            }
            initial = false;
        } else {
            let inputlen = buf.len();
            input = Some(pqformat::pq_getmsgbytes(&mut buf, inputlen)?.to_vec());
        }
        pqformat::pq_getmsgend(&buf)?;

        // Hand the incoming message to the mechanism implementation.
        let state = opaq.as_mut().expect("SASL exchange before init");
        let ExchangeOutput { result: r, output } =
            scram_exchange(state, port, input.as_deref())?;
        result = r;

        // Surface the mechanism's logdetail on failure (scram_exchange tail).
        if result == PG_SASL_EXCHANGE_FAILURE {
            if let Some(ld) = state.logdetail.clone() {
                logdetail = Some(ld);
            }
        }

        // On success at the finished state, copy the derived keys into the
        // Port (C: memcpy into MyProcPort; scram_exchange tail).
        if result == PG_SASL_EXCHANGE_SUCCESS
            && state.state == scram_state_enum::SCRAM_AUTH_FINISHED
        {
            port.scram_ClientKey.copy_from_slice(&state.ClientKey);
            port.scram_ServerKey.copy_from_slice(&state.ServerKey);
            port.has_scram_keys = true;
        }

        if let Some(output) = output {
            // PG_SASL_EXCHANGE_FAILURE with output is forbidden by SASL.
            if result == PG_SASL_EXCHANGE_FAILURE {
                elog(ERROR, "output message found after SASL exchange failure")
                    .map_err(|e| e.with_error_location(here("CheckSASLAuth")))?;
            }

            if result == PG_SASL_EXCHANGE_SUCCESS {
                auth::sendAuthRequest(
                    port,
                    auth::AUTH_REQ_SASL_FIN,
                    &output,
                )?;
            } else {
                auth::sendAuthRequest(
                    port,
                    auth::AUTH_REQ_SASL_CONT,
                    &output,
                )?;
            }
        }

        if result != PG_SASL_EXCHANGE_CONTINUE {
            break;
        }
    }

    // Oops, something bad happened.
    if result != PG_SASL_EXCHANGE_SUCCESS {
        return Ok((STATUS_ERROR, logdetail));
    }

    Ok((STATUS_OK, logdetail))
}

/// Seam entry for `auth_seams::check_scram_sasl_auth`: read the
/// ambient `MyProcPort` and run the SCRAM SASL exchange.
fn check_scram_sasl_auth_entry(
    shadow_pass: Option<String>,
) -> PgResult<(i32, Option<String>)> {
    let mut result: PgResult<(i32, Option<String>)> = Ok((STATUS_ERROR, None));
    init_small_seams::with_my_proc_port::call(&mut |port| {
        let port = port.expect("CheckSASLAuth: MyProcPort is NULL");
        result = check_scram_sasl_auth_impl(port, shadow_pass.clone());
    });
    result
}

// ===========================================================================
// C string primitives
// ===========================================================================

/// `strlen(s)` on a (possibly-NUL-terminated) byte slice.
fn strlen(s: &[u8]) -> usize {
    s.iter().position(|&c| c == 0).unwrap_or(s.len())
}

/// `strsep(&stringp, delim)` for a single-char-set delimiter, returning the
/// token and whether a delimiter was found (`*stringp` still non-NULL).
fn strsep<'a>(stringp: &mut &'a [u8], delim: &[u8]) -> (&'a [u8], bool) {
    let s = *stringp;
    match s.iter().position(|c| delim.contains(c)) {
        Some(idx) => {
            let token = &s[..idx];
            *stringp = &s[idx + 1..];
            (token, true)
        }
        None => {
            *stringp = &[];
            (s, false)
        }
    }
}

/// `errno = 0; v = strtol(s, &p, 10); if (*p || errno) reject`.
fn strtol_base10_full(s: &[u8]) -> Option<i32> {
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
    let mut overflow = false; // mirrors errno == ERANGE
    while i < s.len() && s[i].is_ascii_digit() {
        let digit = (s[i] - b'0') as i64;
        match acc.checked_mul(10).and_then(|v| v.checked_add(digit)) {
            Some(v) => acc = v,
            None => overflow = true,
        }
        i += 1;
    }

    if i == digits_start {
        return None; // no digits => *p is first non-digit => reject
    }
    if overflow {
        return None; // errno != 0 => reject
    }
    if i != s.len() {
        return None; // trailing chars => *p != '\0' => reject
    }

    let val = if neg { acc.wrapping_neg() } else { acc };
    Some(val as i32)
}

/// Reinterpret possibly-non-UTF8 bytes as a Rust `String`.
fn bytes_to_str_lossless(b: &[u8]) -> String {
    String::from_utf8_lossy(b).into_owned()
}

// ===========================================================================
// init_seams
// ===========================================================================

/// Install the inward seam this crate owns: `check_scram_sasl_auth`, the SCRAM
/// arm of the `CheckSASLAuth` driver that `auth.c` consumes.
pub fn init_seams() {
    use guc_tables::{vars, GucVarAccessors};

    auth_seams::check_scram_sasl_auth::set(check_scram_sasl_auth_entry);

    // crypt.c (`get_password_type`/`encrypt_password`/`plain_crypt_verify`)
    // consume three `common/scram-common.c` + `auth-scram.c` routines whose real
    // bodies live in this crate. Install them onto the crypt seams (cross-crate
    // install; the owner crypt-seams crate has no body of its own).
    crypt_seams::parse_scram_secret::set(|secret| {
        // crypt.c only needs the yes/no result; run the full parse with throwaway
        // out-params.
        let mut iterations: i32 = 0;
        let mut hash_type: pg_cryptohash_type = PG_SHA256;
        let mut key_length: i32 = 0;
        let mut encoded_salt: Option<String> = None;
        let mut stored_key = [0u8; SCRAM_MAX_KEY_LEN];
        let mut server_key = [0u8; SCRAM_MAX_KEY_LEN];
        parse_scram_secret(
            secret.as_bytes(),
            &mut iterations,
            &mut hash_type,
            &mut key_length,
            &mut encoded_salt,
            &mut stored_key,
            &mut server_key,
        )
    });
    crypt_seams::pg_be_scram_build_secret::set(|password| {
        pg_be_scram_build_secret(password.as_bytes())
    });
    crypt_seams::scram_verify_plain_password::set(|user, password, secret| {
        scram_verify_plain_password(user, password.as_bytes(), secret.as_bytes())
    });

    // The C GUC `int scram_sha_256_iterations` (auth-scram.c:196) is the backing
    // store for the `scram_iterations` GUC and is read straight from the slot at
    // secret-build time (auth-scram.c:508). The GUC machinery reaches our
    // `AtomicI32` through these accessors (`*conf->variable`).
    vars::scram_sha_256_iterations.install(GucVarAccessors {
        get: scram_sha_256_iterations,
        set: scram_sha_256_iterations_set,
    });
}

#[cfg(test)]
mod tests;
