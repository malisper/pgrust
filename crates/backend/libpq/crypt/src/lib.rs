//! crypt.c.

use mcx::{Mcx, PgString};
use pg_md5::{pg_md5_encrypt, MD5_PASSWD_CHARSET, MD5_PASSWD_LEN};
use types_error::{
    ErrorLocation, PgResult, ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERRCODE_WARNING_DEPRECATED_FEATURE,
    ERROR, WARNING,
};

use std::cell::Cell;

pub const MAX_ENCRYPTED_PASSWORD_LEN: usize = 512;

pub const STATUS_OK: i32 = 0;
pub const STATUS_ERROR: i32 = -1;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum PasswordType {
    Plaintext = 0,
    Md5 = 1,
    ScramSha256 = 2,
}

impl PasswordType {
    // password_encryption GUC values (guc_tables PASSWORD_TYPE_*).
    pub fn from_guc(v: i32) -> PasswordType {
        match v {
            1 => PasswordType::Md5,
            2 => PasswordType::ScramSha256,
            _ => panic!("invalid password_encryption GUC value {v}"),
        }
    }
}

#[track_caller]
fn loc(funcname: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, funcname)
}

thread_local! {
    static MD5_PASSWORD_WARNINGS: Cell<bool> = const { Cell::new(true) };
}

fn md5_password_warnings() -> bool {
    MD5_PASSWORD_WARNINGS.with(Cell::get)
}

fn set_md5_password_warnings(v: bool) {
    MD5_PASSWORD_WARNINGS.with(|c| c.set(v));
}

pub fn get_role_password(role: &str, logdetail: &mut Option<String>) -> PgResult<Option<String>> {
    let scratch = mcx::MemoryContext::new("get_role_password");
    let Some(shape) = syscache_seams::lookup_authid_rolpassword::call(scratch.mcx(), role)? else {
        *logdetail = Some(format!("Role \"{role}\" does not exist."));
        return Ok(None);
    };
    let Some(shadow_pass) = shape.rolpassword else {
        *logdetail = Some(format!("User \"{role}\" has no password assigned."));
        return Ok(None);
    };
    if let Some(vuntil) = shape.rolvaliduntil {
        if vuntil < timestamp_seams::get_current_timestamp::call() {
            *logdetail = Some(format!("User \"{role}\" has an expired password."));
            return Ok(None);
        }
    }
    Ok(Some(shadow_pass.as_str().to_owned()))
}

pub fn get_password_type(shadow_pass: &str) -> PasswordType {
    let bytes = shadow_pass.as_bytes();
    if bytes.starts_with(b"md5")
        && bytes.len() == MD5_PASSWD_LEN
        && strspn(&bytes[3..], MD5_PASSWD_CHARSET) == MD5_PASSWD_LEN - 3
    {
        return PasswordType::Md5;
    }
    if auth_scram::parse_scram_secret(shadow_pass).is_some() {
        return PasswordType::ScramSha256;
    }
    PasswordType::Plaintext
}

fn strspn(s: &[u8], accept: &[u8]) -> usize {
    s.iter().take_while(|b| accept.contains(b)).count()
}

pub fn encrypt_password<'mcx>(
    mcx: Mcx<'mcx>,
    target_type: PasswordType,
    role: &str,
    password: &str,
) -> PgResult<PgString<'mcx>> {
    let guessed_type = get_password_type(password);

    let encrypted_password = if guessed_type != PasswordType::Plaintext {
        // Cannot convert an already-encrypted password from one format to
        // another, so return it as it is.
        PgString::from_str_in(password, mcx)?
    } else {
        match target_type {
            PasswordType::Md5 => {
                let buf = pg_md5_encrypt(password.as_bytes(), role.as_bytes());
                PgString::from_str_in(core::str::from_utf8(&buf).unwrap(), mcx)?
            }
            PasswordType::ScramSha256 => auth_scram::pg_be_scram_build_secret(mcx, password)?,
            PasswordType::Plaintext => {
                elog::ereport(ERROR)
                    .errmsg_internal("cannot encrypt password with 'plaintext'")
                    .finish(loc("encrypt_password"))?;
                unreachable!()
            }
        }
    };

    // De-TOASTing is impossible during authentication (no database selected),
    // so anything that might need out-of-line storage is rejected.
    if encrypted_password.as_str().len() > MAX_ENCRYPTED_PASSWORD_LEN {
        debug_assert!(guessed_type != PasswordType::Plaintext);
        elog::ereport(ERROR)
            .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
            .errmsg("encrypted password is too long")
            .errdetail(format!(
                "Encrypted passwords must be no longer than {MAX_ENCRYPTED_PASSWORD_LEN} bytes."
            ))
            .finish(loc("encrypt_password"))?;
    }

    if md5_password_warnings() && get_password_type(encrypted_password.as_str()) == PasswordType::Md5
    {
        elog::ereport(WARNING)
            .errcode(ERRCODE_WARNING_DEPRECATED_FEATURE)
            .errmsg("setting an MD5-encrypted password")
            .errdetail("MD5 password support is deprecated and will be removed in a future release of PostgreSQL.")
            .errhint("Refer to the PostgreSQL documentation for details about migrating to another password type.")
            .finish(loc("encrypt_password"))?;
    }

    Ok(encrypted_password)
}

pub fn md5_crypt_verify(
    role: &str,
    shadow_pass: &str,
    client_pass: &str,
    md5_salt: &[u8],
    logdetail: &mut Option<String>,
) -> PgResult<i32> {
    assert!(!md5_salt.is_empty());

    if get_password_type(shadow_pass) != PasswordType::Md5 {
        *logdetail = Some(format!(
            "User \"{role}\" has a password that cannot be used with MD5 authentication."
        ));
        return Ok(STATUS_ERROR);
    }

    // Stored password already encrypted, only do salt.
    let crypt_pwd = pg_md5_encrypt(&shadow_pass.as_bytes()[3..], md5_salt);

    if client_pass.as_bytes() == &crypt_pwd[..] {
        Ok(STATUS_OK)
    } else {
        *logdetail = Some(format!("Password does not match for user \"{role}\"."));
        Ok(STATUS_ERROR)
    }
}

pub fn plain_crypt_verify(
    mcx: Mcx<'_>,
    role: &str,
    shadow_pass: &str,
    client_pass: &str,
    logdetail: &mut Option<String>,
) -> PgResult<i32> {
    match get_password_type(shadow_pass) {
        PasswordType::ScramSha256 => {
            if auth_scram::scram_verify_plain_password(mcx, role, client_pass, shadow_pass)? {
                return Ok(STATUS_OK);
            }
            *logdetail = Some(format!("Password does not match for user \"{role}\"."));
            return Ok(STATUS_ERROR);
        }
        PasswordType::Md5 => {
            let crypt_client_pass = pg_md5_encrypt(client_pass.as_bytes(), role.as_bytes());
            if &crypt_client_pass[..] == shadow_pass.as_bytes() {
                return Ok(STATUS_OK);
            }
            *logdetail = Some(format!("Password does not match for user \"{role}\"."));
            return Ok(STATUS_ERROR);
        }
        // We never store passwords in plaintext.
        PasswordType::Plaintext => {}
    }

    *logdetail = Some(format!(
        "Password of user \"{role}\" is in unrecognized format."
    ));
    Ok(STATUS_ERROR)
}

pub fn init_seams() {
    guc_tables::vars::md5_password_warnings.install(guc_tables::GucVarAccessors {
        get: md5_password_warnings,
        set: set_md5_password_warnings,
    });
}

#[cfg(test)]
mod tests;
