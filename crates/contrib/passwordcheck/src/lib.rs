//! contrib/passwordcheck (passwordcheck.c): password-strength checks behind
//! commands/user's `check_password_hook` (user.c:398 CreateRole, user.c:848
//! AlterRole). `LOAD 'passwordcheck'` (or shared_preload_libraries) runs
//! `_PG_init`, which defines `passwordcheck.min_password_length`, reserves the
//! `passwordcheck` GUC prefix and installs `check_password`.
//!
//! The cracklib arm (`USE_CRACKLIB`, passwordcheck.c:120-127) is a build
//! option pgrust never sets, exactly like a stock PGDG build.

use core::sync::atomic::{AtomicI32, Ordering};

use datum::Datum;
use mcx::Mcx;
use types_error::{PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE};
use types_fmgr::PGFunction;

const LIBRARY: &str = "passwordcheck";

// passwordcheck.c:37 `static int min_password_length = 8` (PGC_SUSET custom
// GUC, statically defined like auto_explain.*).
static MIN_PASSWORD_LENGTH: AtomicI32 = AtomicI32::new(8);

fn invalid(msg: &str) -> Box<PgError> {
    Box::new(PgError::error(msg.to_string()).with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE))
}

fn min_password_length() -> i32 {
    MIN_PASSWORD_LENGTH.load(Ordering::Relaxed)
}

fn set_min_password_length(v: i32) {
    MIN_PASSWORD_LENGTH.store(v, Ordering::Relaxed);
}

/// `check_password` (passwordcheck.c:44-131). The previous-hook call at the
/// top of C's function is the chain commands/user runs explicitly.
pub fn check_password(
    mcx: Mcx<'_>,
    username: &str,
    shadow_pass: &str,
    password_type: crypt::PasswordType,
    _validuntil_time: Datum,
    _validuntil_null: bool,
) -> PgResult<()> {
    if password_type != crypt::PasswordType::Plaintext {
        // Encrypted: only username = password can be caught (guessing).
        let mut logdetail = None;
        if crypt::plain_crypt_verify(mcx, username, shadow_pass, username.as_bytes(), &mut logdetail)?
            == crypt::STATUS_OK
        {
            return Err(invalid("password must not equal user name"));
        }
        return Ok(());
    }

    check_plaintext(username, shadow_pass, min_password_length())
}

/// The plaintext arm (passwordcheck.c:74-129) over an explicit minimum length.
fn check_plaintext(username: &str, password: &str, min_password_length: i32) -> PgResult<()> {
    let pwdlen = password.len();

    // enforce minimum length
    if (pwdlen as i64) < min_password_length as i64 {
        return Err(Box::new(
            PgError::error("password is too short".to_string())
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
                .with_detail(format!(
                    "password must be at least \"passwordcheck.min_password_length\" ({min_password_length}) bytes long"
                )),
        ));
    }

    // check if the password contains the username (strstr)
    if password.contains(username) {
        return Err(invalid("password must not contain user name"));
    }

    // check if the password contains both letters and non-letters; isalpha()
    // does not work for multibyte encodings, so non-ASCII bytes count as
    // non-letters (passwordcheck.c:104-114).
    let mut pwd_has_letter = false;
    let mut pwd_has_nonletter = false;
    for &b in password.as_bytes() {
        if b.is_ascii_alphabetic() {
            pwd_has_letter = true;
        } else {
            pwd_has_nonletter = true;
        }
    }
    if !pwd_has_letter || !pwd_has_nonletter {
        return Err(invalid("password must contain both letters and nonletters"));
    }

    // all checks passed, password is ok
    Ok(())
}

fn lookup(_function: &str) -> Option<PGFunction> {
    // passwordcheck exposes no SQL functions.
    None
}

/// `_PG_init` (passwordcheck.c:134-155): the custom GUC, the reserved prefix,
/// then the hook.
fn pg_init() -> PgResult<()> {
    guc::MarkGUCPrefixReserved("passwordcheck");
    user::install_check_password_hook(check_password);
    Ok(())
}

pub fn init_seams() {
    guc_tables::vars::passwordcheck_min_password_length.install(guc_tables::GucVarAccessors {
        get: min_password_length,
        set: set_min_password_length,
    });
    dfmgr::register_builtin_library(dfmgr::BuiltinLibraryEntry {
        name: LIBRARY,
        lookup,
        pg_init: Some(pg_init),
    });
}

#[cfg(test)]
mod tests;
