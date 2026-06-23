//! Port of `src/backend/libpq/crypt.c` — the routines for dealing with
//! encrypted passwords stored in `pg_authid.rolpassword`.
//!
//! Every function in `crypt.c` is implemented here:
//!
//!   * [`get_role_password`]   — fetch the stored password for a role,
//!   * [`get_password_type`]   — classify a stored secret (MD5 / SCRAM / plain),
//!   * [`encrypt_password`]    — convert a plaintext password to a secret,
//!   * [`md5_crypt_verify`]    — verify an MD5 authentication response,
//!   * [`plain_crypt_verify`]  — verify a plaintext password against a stored
//!     hash,
//!
//! plus the `md5_password_warnings` GUC backing variable.
//!
//! ## C → Rust shape mapping
//!
//!   * a C `char *` return that can be `NULL` becomes `Option<String>`
//!     (`None` ⇔ the C `NULL`);
//!   * the `const char **logdetail` out-param becomes `&mut Option<String>`
//!     (the C only ever *sets* it on error paths, so we only assign `Some`);
//!   * the `int` STATUS return becomes [`STATUS_OK`] / [`STATUS_ERROR`].
//!
//! ## Outward dependencies (seamed; owners not yet ported or cyclic)
//!
//!   * `AUTHNAME` syscache lookup → `syscache_seams::fetch_role_password`,
//!   * `GetCurrentTimestamp` → `timestamp_seams::get_current_timestamp`,
//!   * `pg_md5_encrypt` and the three SCRAM routines → [`crypt_seams`].

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use core::cell::Cell;

use ::utils_error::ereport;
use ::authid::PasswordType;
use ::types_core::primitive::TimestampTz;
use ::types_error::{
    ErrorLocation, PgResult, ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERRCODE_WARNING_DEPRECATED_FEATURE,
    ERROR, WARNING,
};

use crypt_seams as crypt_seams;
use timestamp_seams as timestamp_seams;
use syscache_seams as syscache_seams;
use ::cache::syscache::RolePasswordLookup;

mod wire;
pub use wire::init_seams;

/// `ErrorLocation` for `ereport(...)` in this module.
fn here(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("../src/backend/libpq/crypt.c", 0, funcname)
}

// --- common/md5.h constants (mirrored exactly) -----------------------------

/// `MD5_PASSWD_CHARSET` (`common/md5.h`): lowercase-hex charset of an MD5 body.
const MD5_PASSWD_CHARSET: &[u8] = b"0123456789abcdef";

/// `MD5_PASSWD_LEN` (`common/md5.h`): total length of an `"md5"`-prefixed
/// password (`3 + 32`).
const MD5_PASSWD_LEN: usize = 35;

// --- libpq/crypt.h constant ------------------------------------------------

/// `MAX_ENCRYPTED_PASSWORD_LEN` (`libpq/crypt.h`): the cap on a stored secret.
const MAX_ENCRYPTED_PASSWORD_LEN: usize = 512;

// --- c.h status codes ------------------------------------------------------

/// `STATUS_OK` (`c.h`).
pub const STATUS_OK: i32 = 0;
/// `STATUS_ERROR` (`c.h`).
pub const STATUS_ERROR: i32 = -1;

// `bool md5_password_warnings = true;` — enables deprecation warnings for MD5
// passwords. Per-backend GUC backing variable (one backend's SET must not leak
// into another), so it lives in a `thread_local` per AGENTS.md, defaulting to
// the C boot value `true`. The GUC machinery installs new values via
// [`set_md5_password_warnings`].
thread_local! {
    static MD5_PASSWORD_WARNINGS: Cell<bool> = const { Cell::new(true) };
}

fn md5_password_warnings() -> bool {
    MD5_PASSWORD_WARNINGS.with(Cell::get)
}

/// Read the `md5_password_warnings` GUC backing variable (`conf->variable`
/// getter half of the GUC accessor installed by [`init_seams`]).
pub fn get_md5_password_warnings() -> bool {
    md5_password_warnings()
}

/// Set the `md5_password_warnings` GUC backing variable (called by the GUC
/// machinery when it lands).
pub fn set_md5_password_warnings(value: bool) {
    MD5_PASSWORD_WARNINGS.with(|c| c.set(value));
}

/// Fetch stored password for a user, for authentication.
///
/// On error, returns `None`, and stores a `String` describing the reason in
/// `*logdetail` (for the postmaster log; must *not* be sent to the client).
///
/// (`crypt.c:get_role_password`.)
pub fn get_role_password(role: &str, logdetail: &mut Option<String>) -> PgResult<Option<String>> {
    let (shadow_pass, valid_until) = match syscache_seams::fetch_role_password::call(role)? {
        RolePasswordLookup::NoSuchRole => {
            *logdetail = Some(format!("Role \"{role}\" does not exist."));
            return Ok(None); // no such user
        }
        RolePasswordLookup::NoPassword => {
            *logdetail = Some(format!("User \"{role}\" has no password assigned."));
            return Ok(None); // user has no password
        }
        RolePasswordLookup::Found {
            shadow_pass,
            valid_until,
        } => (shadow_pass, valid_until),
    };

    // vuntil starts at 0; only assigned when rolvaliduntil is non-null.
    let mut vuntil: TimestampTz = 0;
    let isnull = valid_until.is_none();
    if let Some(v) = valid_until {
        vuntil = v;
    }

    // Password OK, but check to be sure we are not past rolvaliduntil:
    //   if (!isnull && vuntil < GetCurrentTimestamp())
    if !isnull && vuntil < timestamp_seams::get_current_timestamp::call() {
        *logdetail = Some(format!("User \"{role}\" has an expired password."));
        return Ok(None);
    }

    Ok(Some(shadow_pass))
}

/// What kind of a password type is `shadow_pass`? (`crypt.c:get_password_type`.)
pub fn get_password_type(shadow_pass: &str) -> PgResult<PasswordType> {
    let bytes = shadow_pass.as_bytes();

    // if (strncmp(shadow_pass, "md5", 3) == 0 &&
    //     strlen(shadow_pass) == MD5_PASSWD_LEN &&
    //     strspn(shadow_pass + 3, MD5_PASSWD_CHARSET) == MD5_PASSWD_LEN - 3)
    //     return PASSWORD_TYPE_MD5;
    if bytes.len() >= 3
        && &bytes[..3] == b"md5"
        && bytes.len() == MD5_PASSWD_LEN
        && strspn(&bytes[3..], MD5_PASSWD_CHARSET) == MD5_PASSWD_LEN - 3
    {
        return Ok(PasswordType::Md5);
    }

    // if (parse_scram_secret(shadow_pass, ...)) return PASSWORD_TYPE_SCRAM_SHA_256;
    if crypt_seams::parse_scram_secret::call(shadow_pass)? {
        return Ok(PasswordType::ScramSha256);
    }

    Ok(PasswordType::Plaintext)
}

/// `strspn(s, accept)` — length of the initial run of `s` consisting solely of
/// bytes in `accept` (C `<string.h>` semantics, on a byte slice).
fn strspn(s: &[u8], accept: &[u8]) -> usize {
    s.iter().take_while(|b| accept.contains(b)).count()
}

/// Given a user-supplied password, convert it into a secret of `target_type`
/// kind.
///
/// If the password is already in encrypted form, we cannot reverse the hash, so
/// it is stored as it is regardless of the requested type.
///
/// (`crypt.c:encrypt_password`.)
pub fn encrypt_password(
    target_type: PasswordType,
    role: &str,
    password: &str,
) -> PgResult<String> {
    let guessed_type = get_password_type(password)?;
    let encrypted_password: String;

    if guessed_type != PasswordType::Plaintext {
        // Cannot convert an already-encrypted password from one format to
        // another, so return it as it is.  encrypted_password = pstrdup(password);
        encrypted_password = password.to_string();
    } else {
        match target_type {
            PasswordType::Md5 => {
                // if (!pg_md5_encrypt(password, (uint8 *) role, strlen(role),
                //                     encrypted_password, &errstr))
                //     elog(ERROR, "password encryption failed: %s", errstr);
                encrypted_password =
                    match crypt_seams::pg_md5_encrypt::call(password.as_bytes(), role.as_bytes())? {
                        Ok(c) => c,
                        Err(errstr) => {
                            return ereport(ERROR)
                                .errmsg_internal(format!("password encryption failed: {errstr}"))
                                .finish(here("encrypt_password"))
                                .map(|()| String::new());
                        }
                    };
            }
            PasswordType::ScramSha256 => {
                // encrypted_password = pg_be_scram_build_secret(password);
                encrypted_password = crypt_seams::pg_be_scram_build_secret::call(password)?;
            }
            PasswordType::Plaintext => {
                // elog(ERROR, "cannot encrypt password with 'plaintext'");
                return ereport(ERROR)
                    .errmsg_internal("cannot encrypt password with 'plaintext'")
                    .finish(here("encrypt_password"))
                    .map(|()| String::new());
            }
        }
    }

    // Assert(encrypted_password);  -- always set here.

    // Valid password hashes may be very long, but we don't want to store
    // anything that might need out-of-line storage. 512 bytes should be more
    // than enough for all practical use, so fail for anything longer.
    //   if (encrypted_password && strlen(encrypted_password) > MAX_ENCRYPTED_PASSWORD_LEN)
    if encrypted_password.len() > MAX_ENCRYPTED_PASSWORD_LEN {
        // Assert(guessed_type != PASSWORD_TYPE_PLAINTEXT);
        debug_assert!(guessed_type != PasswordType::Plaintext);

        return ereport(ERROR)
            .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
            .errmsg("encrypted password is too long")
            .errdetail(format!(
                "Encrypted passwords must be no longer than {MAX_ENCRYPTED_PASSWORD_LEN} bytes."
            ))
            .finish(here("encrypt_password"))
            .map(|()| String::new());
    }

    // if (md5_password_warnings &&
    //     get_password_type(encrypted_password) == PASSWORD_TYPE_MD5)
    //     ereport(WARNING, ...);
    if md5_password_warnings() && get_password_type(&encrypted_password)? == PasswordType::Md5 {
        ereport(WARNING)
            .errcode(ERRCODE_WARNING_DEPRECATED_FEATURE)
            .errmsg("setting an MD5-encrypted password")
            .errdetail(
                "MD5 password support is deprecated and will be removed in a future release of PostgreSQL.",
            )
            .errhint(
                "Refer to the PostgreSQL documentation for details about migrating to another password type.",
            )
            .finish(here("encrypt_password"))?;
    }

    Ok(encrypted_password)
}

/// Check MD5 authentication response, and return [`STATUS_OK`] or
/// [`STATUS_ERROR`].
///
/// (`crypt.c:md5_crypt_verify`.)
pub fn md5_crypt_verify(
    role: &str,
    shadow_pass: &str,
    client_pass: &str,
    md5_salt: &[u8],
    md5_salt_len: i32,
    logdetail: &mut Option<String>,
) -> PgResult<i32> {
    let retval;

    // Assert(md5_salt_len > 0);
    debug_assert!(md5_salt_len > 0);

    if get_password_type(shadow_pass)? != PasswordType::Md5 {
        // incompatible password hash format.
        *logdetail = Some(format!(
            "User \"{role}\" has a password that cannot be used with MD5 authentication."
        ));
        return Ok(STATUS_ERROR);
    }

    // Compute the correct answer for the MD5 challenge.
    // stored password already encrypted, only do salt:
    //   if (!pg_md5_encrypt(shadow_pass + strlen("md5"),
    //                       md5_salt, md5_salt_len, crypt_pwd, &errstr))
    //   { *logdetail = errstr; return STATUS_ERROR; }
    //
    // `shadow_pass + strlen("md5")` drops the leading "md5" prefix (the type
    // check above guarantees it is present and 35 bytes long). Only the first
    // `md5_salt_len` bytes of the salt are hashed.
    let salt = &md5_salt[..md5_salt_len as usize];
    let crypt_pwd = match crypt_seams::pg_md5_encrypt::call(&shadow_pass.as_bytes()[3..], salt)? {
        Ok(c) => c,
        Err(errstr) => {
            *logdetail = Some(errstr);
            return Ok(STATUS_ERROR);
        }
    };

    if client_pass == crypt_pwd {
        retval = STATUS_OK;
    } else {
        *logdetail = Some(format!("Password does not match for user \"{role}\"."));
        retval = STATUS_ERROR;
    }

    Ok(retval)
}

/// Check given password for given user, and return [`STATUS_OK`] or
/// [`STATUS_ERROR`].
///
/// (`crypt.c:plain_crypt_verify`.)
pub fn plain_crypt_verify(
    role: &str,
    shadow_pass: &str,
    client_pass: &str,
    logdetail: &mut Option<String>,
) -> PgResult<i32> {
    // Client sent password in plaintext. If we have an MD5 hash stored, hash
    // the password the client sent, and compare the hashes. Otherwise compare
    // the plaintext passwords directly.
    match get_password_type(shadow_pass)? {
        PasswordType::ScramSha256 => {
            if crypt_seams::scram_verify_plain_password::call(role, client_pass, shadow_pass)? {
                return Ok(STATUS_OK);
            } else {
                *logdetail = Some(format!("Password does not match for user \"{role}\"."));
                return Ok(STATUS_ERROR);
            }
        }

        PasswordType::Md5 => {
            // if (!pg_md5_encrypt(client_pass, (uint8 *) role, strlen(role),
            //                     crypt_client_pass, &errstr))
            //   { *logdetail = errstr; return STATUS_ERROR; }
            let crypt_client_pass =
                match crypt_seams::pg_md5_encrypt::call(client_pass.as_bytes(), role.as_bytes())? {
                    Ok(c) => c,
                    Err(errstr) => {
                        *logdetail = Some(errstr);
                        return Ok(STATUS_ERROR);
                    }
                };
            if crypt_client_pass == shadow_pass {
                return Ok(STATUS_OK);
            } else {
                *logdetail = Some(format!("Password does not match for user \"{role}\"."));
                return Ok(STATUS_ERROR);
            }
        }

        PasswordType::Plaintext => {
            // We never store passwords in plaintext, so this shouldn't happen.
        }
    }

    // This shouldn't happen. Plain "password" authentication is possible with
    // any kind of stored password hash.
    *logdetail = Some(format!(
        "Password of user \"{role}\" is in unrecognized format."
    ));
    Ok(STATUS_ERROR)
}

#[cfg(test)]
mod tests;
