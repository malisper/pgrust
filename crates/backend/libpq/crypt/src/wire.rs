//! Install the inward seams that other (cyclic / already-merged) crates use to
//! reach `crypt.c`. The owner installs every seam it owns from `init_seams()`.
//!
//! crypt.c's five exported entry points are declared across two existing seam
//! crates (created by the consumers before crypt landed):
//!
//!   * `auth_seams`: `get_role_password`, `md5_crypt_verify`
//!     (consumed by `auth.c`),
//!   * `user_seams`: `encrypt_password`, `plain_crypt_verify`,
//!     `get_password_type` (consumed by `user.c`; `auth.c` re-uses the latter
//!     two).
//!
//! Each adapter below marshals the seam's owned-argument shape (`String`,
//! `Vec<u8>`, `Mcx`/`PgString`, tuple returns) onto crypt's internal `&str` /
//! `&mut Option<String>` API. The adapters are thin: no logic lives here.

use mcx::{Mcx, PgString};
use authid::PasswordType;
use types_error::PgResult;

use crate::{
    encrypt_password, get_password_type, get_role_password, md5_crypt_verify, plain_crypt_verify,
};

/// Adapter for `auth_seams::get_role_password`: the seam returns
/// `(Some(secret), _)` on success and `(None, Some(logdetail))` on the C
/// NULL-return paths.
fn seam_get_role_password(role: String) -> PgResult<(Option<String>, Option<String>)> {
    let mut logdetail: Option<String> = None;
    let pass = get_role_password(&role, &mut logdetail)?;
    Ok((pass, logdetail))
}

/// Adapter for `auth_seams::md5_crypt_verify`: returns
/// `(status, logdetail)`.
fn seam_md5_crypt_verify(
    role: String,
    shadow_pass: String,
    client_pass: String,
    md5_salt: Vec<u8>,
) -> PgResult<(i32, Option<String>)> {
    let mut logdetail: Option<String> = None;
    let status = md5_crypt_verify(
        &role,
        &shadow_pass,
        &client_pass,
        &md5_salt,
        md5_salt.len() as i32,
        &mut logdetail,
    )?;
    Ok((status, logdetail))
}

/// Adapter for `user_seams::encrypt_password`. The C
/// `Password_encryption` GUC holds a `PasswordType` value; map it back to the
/// enum and allocate the result string in the caller's `mcx`.
fn seam_encrypt_password<'mcx>(
    mcx: Mcx<'mcx>,
    password_encryption: i32,
    role: String,
    password: String,
) -> PgResult<PgString<'mcx>> {
    let target_type = match password_encryption {
        x if x == PasswordType::Plaintext as i32 => PasswordType::Plaintext,
        x if x == PasswordType::Md5 as i32 => PasswordType::Md5,
        x if x == PasswordType::ScramSha256 as i32 => PasswordType::ScramSha256,
        // Any other GUC value is treated as plaintext, matching the C enum's
        // exhaustive switch (encrypt_password errors on the plaintext target).
        _ => PasswordType::Plaintext,
    };
    let encrypted = encrypt_password(target_type, &role, &password)?;
    PgString::from_str_in(&encrypted, mcx)
}

/// Adapter for `user_seams::plain_crypt_verify`.
fn seam_plain_crypt_verify(
    role: String,
    shadow_pass: String,
    client_pass: String,
) -> PgResult<i32> {
    let mut logdetail: Option<String> = None;
    plain_crypt_verify(&role, &shadow_pass, &client_pass, &mut logdetail)
}

/// Adapter for `user_seams::get_password_type`.
fn seam_get_password_type(shadow_pass: String) -> PgResult<PasswordType> {
    get_password_type(&shadow_pass)
}

/// Install every seam owned by `crypt.c`.
pub fn init_seams() {
    auth_seams::get_role_password::set(seam_get_role_password);
    auth_seams::md5_crypt_verify::set(seam_md5_crypt_verify);
    user_seams::encrypt_password::set(seam_encrypt_password);
    user_seams::plain_crypt_verify::set(seam_plain_crypt_verify);
    user_seams::get_password_type::set(seam_get_password_type);

    // GUC variable accessor (`conf->variable`) for `md5_password_warnings`. The
    // C global `bool md5_password_warnings` (crypt.c) is read directly from the
    // GUC slot; install the get/set pair pointing at this crate's `thread_local`
    // backing store so the GUC machinery's writes (and shows) reach it.
    guc_tables::vars::md5_password_warnings.install(
        guc_tables::GucVarAccessors {
            get: crate::get_md5_password_warnings,
            set: crate::set_md5_password_warnings,
        },
    );
}
