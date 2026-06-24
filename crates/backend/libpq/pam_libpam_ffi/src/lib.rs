//! FFI provider for `CheckPAMAuth` (auth.c, `#ifdef USE_PAM`): the PAM
//! conversation against the system `libpam`.
//!
//! This binds the SAME `libpam` PostgreSQL's `--with-pam` links — never a
//! reimplementation of PAM. The exposed `check_pam_auth` runs the C
//! `CheckPAMAuth` flow:
//!
//!   pam_start → pam_set_item(PAM_USER) → [pam_set_item(PAM_RHOST)] →
//!   pam_set_item(PAM_CONV) → pam_authenticate → pam_acct_mgmt → pam_end,
//!   then `set_authn_id(user)` on success.
//!
//! The `pam_passwd_conv_proc` conversation callback (auth.c:1928) is ported
//! faithfully: on a `PAM_PROMPT_ECHO_OFF` prompt it replies with the password;
//! if the password is empty it asks the client (via the supplied
//! `request_password` closure, which performs `sendAuthRequest` +
//! `recv_password_packet`) and stuffs the answer into PAM. `PAM_ERROR_MSG` /
//! `PAM_TEXT_INFO` reply with an empty string; any other style fails the
//! conversation.
//!
//! C uses static globals (`pam_passwd`/`pam_port_cludge`/`pam_no_password`) as a
//! Solaris workaround for PAM not forwarding `appdata_ptr`. We pass a
//! `*mut PamConvCtx` through `appdata_ptr` (which both Linux-PAM and OpenPAM do
//! forward) and the conversation reads it back, which is cleaner and equivalent.

#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]

#[cfg(all(feature = "with-pam", not(target_family = "wasm")))]
mod provider;

#[cfg(all(feature = "with-pam", not(target_family = "wasm")))]
pub use provider::{check_pam_auth, PamOutcome};

/// `USE_PAM` — whether this build links PAM. Read by the hba parser to accept or
/// reject the `pam` auth method (faithful to a `--with-pam` / non-`--with-pam`
/// configure choice).
pub const fn pam_available() -> bool {
    cfg!(all(feature = "with-pam", not(target_family = "wasm")))
}

/// Outcome of the PAM conversation, mapped to the auth.c `STATUS_*` codes by the
/// caller. `Eof` corresponds to the C `pam_no_password` path (client refused a
/// password); the caller turns it into `STATUS_EOF`.
#[cfg(not(all(feature = "with-pam", not(target_family = "wasm"))))]
pub enum PamOutcome {
    Ok,
    Error,
    Eof,
}

/// Stub when PAM is not compiled in: the hba parser never accepts the `pam`
/// method (`pam_available()` is false), so this is unreachable, matching the C
/// `Assert(false)` in a non-`--with-pam` build.
#[cfg(not(all(feature = "with-pam", not(target_family = "wasm"))))]
pub fn check_pam_auth(
    _service: &str,
    _user: &str,
    _rhost: Option<&str>,
    _initial_password: &str,
    _request_password: &mut dyn FnMut() -> Option<String>,
) -> Result<PamOutcome, String> {
    unreachable!("CheckPAMAuth reached without USE_PAM (hba should have rejected `pam`)")
}
