//! Port of `src/backend/libpq/auth.c` (+ the `CheckSASLAuth` driver of
//! `auth-sasl.c`) — client authentication: `ClientAuthentication` and the
//! per-method handlers.
//!
//! ## Ported in-crate (full logic)
//!
//!   * `ClientAuthentication` — the `port->hba->auth_method` dispatch, the
//!     pre-auth clientcert checks, the reject/implicit-reject arms (with the
//!     `HOSTNAME_LOOKUP_DETAIL` composition), the post-dispatch cert check,
//!     the authenticated-connection log, the `ClientAuthentication_hook`, and
//!     the `AUTH_REQ_OK`/`auth_failed` tail.
//!   * `auth_failed`, `set_authn_id`, `sendAuthRequest`, `recv_password_packet`.
//!   * `CheckPasswordAuth`, `CheckPWChallengeAuth`, `CheckMD5Auth`.
//!   * `CheckSASLAuth` driver loop (auth-sasl.c) — but the mechanism vtable
//!     (SCRAM/OAuth) is unported, so each mechanism crosses one seam.
//!   * `ident_inet` + `interpret_ident_response`, `auth_peer`,
//!     `CheckRADIUSAuth` + `radius_add_attribute` + `PerformRadiusTransaction`
//!     (the self-contained socket methods; ported with real libc sockets,
//!     matching the in-repo `common/ip.c` style).
//!   * `CheckCertAuth` (the TLS cert→usermap method).
//!
//! ## Seamed (unported owners)
//!
//!   * `hba.c`: `hba_getauthmethod`, `check_usermap`, `hba_authname`.
//!   * `crypt.c`: `get_role_password`, `md5_crypt_verify` (and via
//!     `backend-commands-user-seams`: `plain_crypt_verify`, `get_password_type`).
//!   * `auth-scram.c`/`auth-oauth.c`: the SASL mechanism exchange.
//!   * `common/md5.c`: `pg_md5_binary` (RADIUS password / response MAC).
//!   * `be-secure.c`: `secure_loaded_verify_locations`.
//!   * GSS/SSPI/PAM/BSD/LDAP: external libraries (`Assert(false)` in this build).

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

#[cfg(target_family = "wasm")]
#[allow(unused_imports)]
use wasm_libc_shim as libc;
mod ident;
mod ldap;
mod pam;
mod peer;
mod radius;

#[cfg(test)]
mod tests;

use ::utils_error::{elog, ereport};
use ::mcx::Mcx;
use ::types_error::{ErrorLocation, PgResult, DEBUG5, ERROR, FATAL, LOG};
use ::types_core::{
    uaBSD, uaCert, uaGSS, uaIdent, uaImplicitReject, uaLDAP, uaMD5, uaOAuth, uaPAM, uaPassword,
    uaPeer, uaRADIUS, uaReject, uaSCRAM, uaSSPI, uaTrust, UserAuth,
};
use ::types_error::{
    ERRCODE_CONFIG_FILE_ERROR, ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION,
    ERRCODE_INVALID_PASSWORD, ERRCODE_PROTOCOL_VIOLATION,
};
use ::net::{clientCertFull, clientCertOff, Port};

pub(crate) use user_seams as user_seams;
pub(crate) use auth_seams as seams;

// --- c.h status codes ------------------------------------------------------

/// `STATUS_OK` (`c.h`).
pub const STATUS_OK: i32 = 0;
/// `STATUS_ERROR` (`c.h`).
pub const STATUS_ERROR: i32 = -1;
/// `STATUS_EOF` (`c.h`).
pub const STATUS_EOF: i32 = -2;

// --- libpq/sasl.h ----------------------------------------------------------

/// `PG_SASL_EXCHANGE_CONTINUE` (`libpq/sasl.h`).
pub const PG_SASL_EXCHANGE_CONTINUE: i32 = 0;
/// `PG_SASL_EXCHANGE_SUCCESS` (`libpq/sasl.h`).
pub const PG_SASL_EXCHANGE_SUCCESS: i32 = 1;
/// `PG_SASL_EXCHANGE_FAILURE` (`libpq/sasl.h`).
pub const PG_SASL_EXCHANGE_FAILURE: i32 = 2;

// --- libpq/protocol.h ------------------------------------------------------

/// `AuthRequest` (`libpq/protocol.h`).
pub type AuthRequest = i32;
pub const AUTH_REQ_OK: AuthRequest = 0;
pub const AUTH_REQ_PASSWORD: AuthRequest = 3;
pub const AUTH_REQ_MD5: AuthRequest = 5;
pub const AUTH_REQ_GSS: AuthRequest = 7;
pub const AUTH_REQ_SSPI: AuthRequest = 9;
pub const AUTH_REQ_SASL: AuthRequest = 10;
pub const AUTH_REQ_SASL_CONT: AuthRequest = 11;
pub const AUTH_REQ_SASL_FIN: AuthRequest = 12;

/// `PqMsg_AuthenticationRequest` = `'R'` (`libpq/protocol.h`).
pub const PqMsg_AuthenticationRequest: u8 = b'R';
/// `PqMsg_PasswordMessage` = `'p'` (`libpq/protocol.h`).
pub const PqMsg_PasswordMessage: u8 = b'p';

/// `PG_MAX_AUTH_TOKEN_LENGTH` (`libpq/auth.h`).
pub const PG_MAX_AUTH_TOKEN_LENGTH: i32 = 65535;

/// `PASSWORD_TYPE_MD5` discriminant (`libpq/crypt.h` `PasswordType`).
const PASSWORD_TYPE_MD5: i32 = authid::PasswordType::Md5 as i32;

/// `src/backend/libpq/auth.c`, for `ErrorLocation`.
const SRCFILE_AUTH: &str = "auth.c";

/// `ErrorLocation` for an `ereport(...)` raised from auth.c.
pub(crate) fn here(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new(SRCFILE_AUTH, 0, funcname)
}

// ---------------------------------------------------------------------------
// auth_failed
// ---------------------------------------------------------------------------

/// `auth_failed(port, status, logdetail)` (`auth.c:238`). Report the failure
/// and (on the non-EOF path) raise `FATAL` (does not return; carried on `Err`).
pub fn auth_failed(port: &Port, status: i32, logdetail: Option<&str>) -> PgResult<()> {
    // If we failed due to EOF from client, just quit; there's no point in
    // trying to send a message to the client. proc_exit does not return.
    if status == STATUS_EOF {
        ipc_seams::proc_exit::call(0);
    }

    let hba = port.hba.as_ref().expect("auth_failed: port->hba is NULL");

    let mut errcode_return = ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION;
    let errstr: &str = match hba.auth_method {
        m if m == uaReject || m == uaImplicitReject => {
            "authentication failed for user \"%s\": host rejected"
        }
        m if m == uaTrust => "\"trust\" authentication failed for user \"%s\"",
        m if m == uaIdent => "Ident authentication failed for user \"%s\"",
        m if m == uaPeer => "Peer authentication failed for user \"%s\"",
        m if m == uaPassword || m == uaMD5 || m == uaSCRAM => {
            // We use it to indicate if a .pgpass password failed.
            errcode_return = ERRCODE_INVALID_PASSWORD;
            "password authentication failed for user \"%s\""
        }
        m if m == uaGSS => "GSSAPI authentication failed for user \"%s\"",
        m if m == uaSSPI => "SSPI authentication failed for user \"%s\"",
        m if m == uaPAM => "PAM authentication failed for user \"%s\"",
        m if m == uaBSD => "BSD authentication failed for user \"%s\"",
        m if m == uaLDAP => "LDAP authentication failed for user \"%s\"",
        m if m == uaCert => "certificate authentication failed for user \"%s\"",
        m if m == uaRADIUS => "RADIUS authentication failed for user \"%s\"",
        m if m == uaOAuth => "OAuth bearer authentication failed for user \"%s\"",
        _ => "authentication failed for user \"%s\": invalid authentication method",
    };

    let cdetail = format!(
        "Connection matched file \"{}\" line {}: \"{}\"",
        hba.sourcefile.as_deref().unwrap_or(""),
        hba.linenumber,
        hba.rawline.as_deref().unwrap_or(""),
    );
    let logdetail = match logdetail {
        Some(ld) => format!("{ld}\n{cdetail}"),
        None => cdetail,
    };

    let user_name = port.user_name.as_deref().unwrap_or("");
    let message = errstr.replacen("%s", user_name, 1);

    Err(ereport(FATAL)
        .errcode(errcode_return)
        .errmsg_internal(message)
        .errdetail_log(logdetail)
        .into_error()
        .with_error_location(here("auth_failed")))
}

// ---------------------------------------------------------------------------
// set_authn_id
// ---------------------------------------------------------------------------

/// `set_authn_id(port, id)` (`auth.c:341`). Record the authenticated identity
/// on `MyClientConnectionInfo` (FATAL if set twice).
pub fn set_authn_id(port: &Port, id: &str) -> PgResult<()> {
    let info = miscinit::client_connection_info();
    if let Some(previous) = info.authn_id {
        return Err(ereport(FATAL)
            .errmsg("authentication identifier set more than once")
            .errdetail_log(format!(
                "previous identifier: \"{previous}\"; new identifier: \"{id}\""
            ))
            .into_error()
            .with_error_location(here("set_authn_id")));
    }

    let auth_method = port_auth_method(port);
    miscinit::set_client_connection_info(Some(id.to_string()), auth_method);

    if seams::log_connection_authentication::call() {
        let mcx = MemCtx::new("set_authn_id");
        let authname = seams::hba_authname_of::call(mcx.mcx(), auth_method)?;
        ereport(LOG)
            .errmsg(format!(
                "connection authenticated: identity=\"{}\" method={} ({}:{})",
                id,
                authname.as_str(),
                port.hba.as_ref().unwrap().sourcefile.as_deref().unwrap_or(""),
                port.hba.as_ref().unwrap().linenumber,
            ))
            .finish(here("set_authn_id"))?;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// sendAuthRequest / recv_password_packet
// ---------------------------------------------------------------------------

/// `sendAuthRequest(port, areq, extradata, extralen)` (`auth.c:677`).
pub fn sendAuthRequest(_port: &Port, areq: AuthRequest, extradata: &[u8]) -> PgResult<()> {
    postgres_seams::check_for_interrupts::call()?;

    let mcx = MemCtx::new("sendAuthRequest");
    let mut buf = pqformat::pq_beginmessage(mcx.mcx(), PqMsg_AuthenticationRequest)?;
    pqformat::pq_sendint32(&mut buf, areq as u32)?;
    if !extradata.is_empty() {
        pqformat::pq_sendbytes(&mut buf, extradata)?;
    }
    pqformat::pq_endmessage(buf)?;

    // Flush message so client will see it, except for AUTH_REQ_OK and
    // AUTH_REQ_SASL_FIN, which need not be sent until we are ready for queries.
    if areq != AUTH_REQ_OK && areq != AUTH_REQ_SASL_FIN {
        pqcomm::pq_flush()?;
    }

    postgres_seams::check_for_interrupts::call()?;
    Ok(())
}

/// `recv_password_packet(port)` (`auth.c:706`). `None` if no password.
pub fn recv_password_packet(_port: &Port) -> PgResult<Option<String>> {
    pqcomm::pq_startmsgread()?;

    // Expect 'p' message type.
    let mtype = pqcomm::pq_getbyte()?;
    if mtype != PqMsg_PasswordMessage as i32 {
        // EOF (-1) is silent; any other wrong type is a protocol violation.
        if mtype != -1 {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_PROTOCOL_VIOLATION)
                .errmsg(format!("expected password response, got message type {mtype}"))
                .into_error()
                .with_error_location(here("recv_password_packet")));
        }
        return Ok(None); // EOF or bad message type
    }

    let mcx = MemCtx::new("recv_password_packet");
    let mut buf = stringinfo::StringInfo::new_in(mcx.mcx());
    if pqcomm::pq_getmessage(&mut buf, PG_MAX_AUTH_TOKEN_LENGTH)? != 0 {
        // EOF — pq_getmessage already logged a suitable message.
        return Ok(None);
    }

    // Sanity check: password packet length should agree with the contained
    // string. Safe to use strlen because StringInfo has an appended '\0'.
    let data = buf.as_bytes();
    let buf_len = data.len();
    let strlen = data.iter().position(|&b| b == 0).unwrap_or(buf_len);
    if strlen + 1 != buf_len {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg("invalid password packet size")
            .into_error()
            .with_error_location(here("recv_password_packet")));
    }

    // Don't allow an empty password.
    if buf_len == 1 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PASSWORD)
            .errmsg("empty password returned by client")
            .into_error()
            .with_error_location(here("recv_password_packet")));
    }

    // Do not echo password to logs, for security.
    elog(DEBUG5, "received password packet")?;

    Ok(Some(String::from_utf8_lossy(&data[..strlen]).into_owned()))
}

// ---------------------------------------------------------------------------
// Password-based methods
// ---------------------------------------------------------------------------

/// `CheckPasswordAuth(port, &logdetail)` (`auth.c:787`).
pub fn CheckPasswordAuth(port: &Port) -> PgResult<(i32, Option<String>)> {
    let result: i32;
    let mut logdetail: Option<String> = None;

    sendAuthRequest(port, AUTH_REQ_PASSWORD, &[])?;

    let passwd = match recv_password_packet(port)? {
        Some(p) => p,
        None => return Ok((STATUS_EOF, logdetail)),
    };

    let user_name = port_user_name(port);

    let (shadow_pass, gl) = seams::get_role_password::call(user_name.clone())?;
    logdetail = gl;
    if let Some(shadow_pass) = shadow_pass {
        // plain_crypt_verify is owned by crypt.c (declared in
        // commands-user-seams without the logdetail out-param; the C only
        // writes *logdetail there on a SCRAM/MD5 stored-secret mismatch, used
        // solely for the server log).
        result = user_seams::plain_crypt_verify::call(user_name.clone(), shadow_pass, passwd)?;
    } else {
        result = STATUS_ERROR;
    }

    if result == STATUS_OK {
        set_authn_id(port, &user_name)?;
    }

    Ok((result, logdetail))
}

/// `CheckPWChallengeAuth(port, &logdetail)` (`auth.c:822`).
pub fn CheckPWChallengeAuth(port: &Port) -> PgResult<(i32, Option<String>)> {
    let auth_result: i32;

    let auth_method = port_auth_method(port);
    debug_assert!(auth_method == uaSCRAM || auth_method == uaMD5);

    let user_name = port_user_name(port);

    // First look up the user's password.
    let (shadow_pass, gl) = seams::get_role_password::call(user_name.clone())?;
    let mut logdetail = gl;

    // If the user does not exist, or has no usable password, go through the
    // motions anyway. Choose md5 vs scram based on the stored password type
    // (or the password_encryption GUC when there is no stored password).
    let pwtype = if let Some(ref sp) = shadow_pass {
        user_seams::get_password_type::call(sp.clone())? as i32
    } else {
        seams::password_encryption::call()
    };

    if auth_method == uaMD5 && pwtype == PASSWORD_TYPE_MD5 {
        let (r, ld) = CheckMD5Auth(port, shadow_pass.clone())?;
        auth_result = r;
        if let Some(ld) = ld {
            logdetail = Some(ld);
        }
    } else {
        let (r, ld) = seams::check_scram_sasl_auth::call(shadow_pass.clone())?;
        auth_result = r;
        if let Some(ld) = ld {
            logdetail = Some(ld);
        }
    }

    if shadow_pass.is_none() {
        // If get_role_password() returned error, auth better not have succeeded.
        debug_assert!(auth_result != STATUS_OK);
    }

    if auth_result == STATUS_OK {
        set_authn_id(port, &user_name)?;
    }

    Ok((auth_result, logdetail))
}

/// `CheckMD5Auth(port, shadow_pass, &logdetail)` (`auth.c:882`).
pub fn CheckMD5Auth(port: &Port, shadow_pass: Option<String>) -> PgResult<(i32, Option<String>)> {
    let result: i32;
    let mut logdetail: Option<String> = None;

    // Include the salt to use for computing the response.
    let mut md5_salt = [0u8; 4];
    if !pg_strong_random(&mut md5_salt) {
        ereport(LOG)
            .errmsg("could not generate random MD5 salt")
            .finish(here("CheckMD5Auth"))?;
        return Ok((STATUS_ERROR, logdetail));
    }

    sendAuthRequest(port, AUTH_REQ_MD5, &md5_salt)?;

    let passwd = match recv_password_packet(port)? {
        Some(p) => p,
        None => return Ok((STATUS_EOF, logdetail)),
    };

    if let Some(shadow_pass) = shadow_pass {
        let user_name = port_user_name(port);
        let (r, ld) =
            seams::md5_crypt_verify::call(user_name, shadow_pass, passwd, md5_salt.to_vec())?;
        result = r;
        logdetail = ld;
    } else {
        result = STATUS_ERROR;
    }

    Ok((result, logdetail))
}

// ---------------------------------------------------------------------------
// CheckSASLAuth driver (auth-sasl.c) — mechanism vtable is seamed per mech.
// ---------------------------------------------------------------------------
//
// The C `CheckSASLAuth(mech, port, shadow_pass, logdetail)` runs the SASL
// message loop with a mechanism vtable (`get_mechanisms`/`init`/`exchange`).
// SCRAM (auth-scram.c) and OAuth (auth-oauth.c) — the only two mechanisms —
// are unported, and the loop is inseparable from the mechanism state, so each
// mechanism's whole `CheckSASLAuth(&mech, ...)` crosses a single seam. The
// loop itself lands with the mechanism owner.

// ---------------------------------------------------------------------------
// ClientAuthentication — the dispatcher.
// ---------------------------------------------------------------------------

/// `ClientAuthentication(port)` (`auth.c:379`). Does not return on error
/// (raises FATAL, carried on `Err`).
pub fn ClientAuthentication(port: &mut Port) -> PgResult<()> {
    #[allow(unused_assignments)]
    let mut status: i32 = STATUS_ERROR;
    let mut logdetail: Option<String> = None;

    // Resolve the auth method for this frontend/database combination.
    // C: hba_getauthmethod(port) — pass the live Port straight through (auth.c:390).
    seams::hba_getauthmethod::call(port)?;

    postgres_seams::check_for_interrupts::call()?;

    // First point with the hba record: pre-auth verifications.
    let clientcert = port.hba.as_ref().expect("ClientAuthentication: port->hba is NULL").clientcert;
    if clientcert != clientCertOff {
        if !seams::secure_loaded_verify_locations::call() {
            return Err(ereport(FATAL)
                .errcode(ERRCODE_CONFIG_FILE_ERROR)
                .errmsg(
                    "client certificates can only be checked if a root certificate store is available",
                )
                .into_error()
                .with_error_location(here("ClientAuthentication")));
        }
        if !port.peer_cert_valid {
            return Err(ereport(FATAL)
                .errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION)
                .errmsg("connection requires a valid client certificate")
                .into_error()
                .with_error_location(here("ClientAuthentication")));
        }
    }

    let auth_method = port_auth_method(port);

    // Now proceed to do the actual authentication check.
    match auth_method {
        m if m == uaReject => {
            return reject_arm(port, true);
        }
        m if m == uaImplicitReject => {
            return reject_arm(port, false);
        }
        m if m == uaGSS => {
            status = seams::check_gss_auth::call()?;
        }
        m if m == uaSSPI => {
            status = seams::check_sspi_auth::call()?;
        }
        m if m == uaPeer => {
            status = peer::auth_peer(port)?;
        }
        m if m == uaIdent => {
            status = ident::ident_inet(port)?;
        }
        m if m == uaMD5 || m == uaSCRAM => {
            let (s, ld) = CheckPWChallengeAuth(port)?;
            status = s;
            logdetail = ld;
        }
        m if m == uaPassword => {
            let (s, ld) = CheckPasswordAuth(port)?;
            status = s;
            logdetail = ld;
        }
        m if m == uaPAM => {
            status = pam::CheckPAMAuth(port)?;
        }
        m if m == uaBSD => {
            status = seams::check_bsd_auth::call()?;
        }
        m if m == uaLDAP => {
            status = ldap::CheckLDAPAuth(port)?;
        }
        m if m == uaRADIUS => {
            status = radius::CheckRADIUSAuth(port)?;
        }
        m if m == uaCert || m == uaTrust => {
            // uaCert is treated as if clientcert=verify-full (uaTrust).
            status = STATUS_OK;
        }
        m if m == uaOAuth => {
            status = seams::check_oauth_sasl_auth::call()?;
        }
        _ => {
            // No other UserAuth value exists.
            status = STATUS_ERROR;
        }
    }

    // Make sure we only check the certificate if we use the cert method or the
    // verify-full option.
    if (status == STATUS_OK && clientcert == clientCertFull) || auth_method == uaCert {
        status = CheckCertAuth(port)?;
    }

    if seams::log_connection_authentication::call()
        && status == STATUS_OK
        && miscinit::client_connection_info().authn_id.is_none()
    {
        let mcx = MemCtx::new("ClientAuthentication");
        let authname = seams::hba_authname_of::call(mcx.mcx(), auth_method)?;
        let hba = port.hba.as_ref().unwrap();
        ereport(LOG)
            .errmsg(format!(
                "connection authenticated: user=\"{}\" method={} ({}:{})",
                port.user_name.as_deref().unwrap_or(""),
                authname.as_str(),
                hba.sourcefile.as_deref().unwrap_or(""),
                hba.linenumber,
            ))
            .finish(here("ClientAuthentication"))?;
    }

    // if (ClientAuthentication_hook) (*ClientAuthentication_hook)(port, status);
    seams::client_authentication_hook::call(status)?;

    if status == STATUS_OK {
        sendAuthRequest(port, AUTH_REQ_OK, &[])?;
    } else {
        auth_failed(port, status, logdetail.as_deref())?;
    }

    Ok(())
}

/// The `uaReject` / `uaImplicitReject` arms of `ClientAuthentication`
/// (`auth.c:424-539`). `explicit_reject` selects the message wording.
fn reject_arm(port: &Port, explicit_reject: bool) -> PgResult<()> {
    let hostinfo = getnameinfo_remote_numeric(port);
    // ENABLE_GSS / USE_SSL are off in this build, so encryption_state reduces
    // to the SSL ternary, then "no encryption".
    let encryption_state = if port.ssl_in_use { "SSL encryption" } else { "no encryption" };

    let user_name = port.user_name.as_deref().unwrap_or("");
    let is_repl = seams_am_walsender() && !walsender_seams::am_db_walsender::call();

    let mut builder = if explicit_reject {
        if is_repl {
            ereport(FATAL).errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION).errmsg(format!(
                "pg_hba.conf rejects replication connection for host \"{hostinfo}\", user \"{user_name}\", {encryption_state}"
            ))
        } else {
            ereport(FATAL).errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION).errmsg(format!(
                "pg_hba.conf rejects connection for host \"{hostinfo}\", user \"{user_name}\", database \"{}\", {encryption_state}",
                port.database_name.as_deref().unwrap_or("")
            ))
        }
    } else if is_repl {
        ereport(FATAL).errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION).errmsg(format!(
            "no pg_hba.conf entry for replication connection from host \"{hostinfo}\", user \"{user_name}\", {encryption_state}"
        ))
    } else {
        ereport(FATAL).errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION).errmsg(format!(
            "no pg_hba.conf entry for host \"{hostinfo}\", user \"{user_name}\", database \"{}\", {encryption_state}",
            port.database_name.as_deref().unwrap_or("")
        ))
    };

    // HOSTNAME_LOOKUP_DETAIL only applies on the implicit-reject path.
    if !explicit_reject {
        if let Some(detail) = hostname_lookup_detail(port) {
            builder = builder.errdetail_log(detail);
        }
    }

    Err(builder.into_error().with_error_location(here("ClientAuthentication")))
}

/// `HOSTNAME_LOOKUP_DETAIL(port)` (`auth.c:500-519`).
fn hostname_lookup_detail(port: &Port) -> Option<String> {
    let resolv = port.remote_hostname_resolv;
    match port.remote_hostname.as_deref() {
        Some(remote_hostname) => match resolv {
            1 => Some(format!(
                "Client IP address resolved to \"{remote_hostname}\", forward lookup matches."
            )),
            0 => Some(format!(
                "Client IP address resolved to \"{remote_hostname}\", forward lookup not checked."
            )),
            -1 => Some(format!(
                "Client IP address resolved to \"{remote_hostname}\", forward lookup does not match."
            )),
            -2 => Some(format!(
                "Could not translate client host name \"{}\" to IP address: {}.",
                remote_hostname,
                gai_strerror(port.remote_hostname_errcode)
            )),
            _ => None,
        },
        None => {
            if resolv == -2 {
                Some(format!(
                    "Could not resolve client IP address to a host name: {}.",
                    gai_strerror(port.remote_hostname_errcode)
                ))
            } else {
                None
            }
        }
    }
}

// ---------------------------------------------------------------------------
// CheckCertAuth (TLS cert method).
// ---------------------------------------------------------------------------

/// `CheckCertAuth(port)` (`auth.c:2687`). `port->ssl` is asserted non-NULL in
/// C; the TLS layer guarantees we only reach here under SSL.
pub fn CheckCertAuth(port: &Port) -> PgResult<i32> {
    let hba = port.hba.as_ref().expect("CheckCertAuth: port->hba is NULL");
    let user_name = port.user_name.as_deref().unwrap_or("");

    // clientCertCN = 0, clientCertDN = 1.
    let peer_username: Option<&str> = match hba.clientcertname {
        ::net::clientCertDN => port.peer_dn.as_deref(),
        _ /* clientCertCN */ => port.peer_cn.as_deref(),
    };

    let peer_username = match peer_username {
        Some(p) if !p.is_empty() => p,
        _ => {
            ereport(LOG)
                .errmsg(format!(
                    "certificate authentication failed for user \"{user_name}\": client certificate contains no user name"
                ))
                .finish(here("CheckCertAuth"))?;
            return Ok(STATUS_ERROR);
        }
    };

    if hba.auth_method == uaCert {
        let peer_dn = match port.peer_dn.as_deref() {
            Some(dn) => dn,
            None => {
                ereport(LOG)
                    .errmsg(format!(
                        "certificate authentication failed for user \"{user_name}\": unable to retrieve subject DN"
                    ))
                    .finish(here("CheckCertAuth"))?;
                return Ok(STATUS_ERROR);
            }
        };
        set_authn_id(port, peer_dn)?;
    }

    let status_check_usermap = seams::check_usermap::call(
        hba.usermap.clone(),
        user_name.to_string(),
        peer_username.to_string(),
        false,
    )?;

    if status_check_usermap != STATUS_OK
        && hba.clientcert == clientCertFull
        && hba.auth_method != uaCert
    {
        match hba.clientcertname {
            ::net::clientCertDN => {
                ereport(LOG)
                    .errmsg(format!(
                        "certificate validation (clientcert=verify-full) failed for user \"{user_name}\": DN mismatch"
                    ))
                    .finish(here("CheckCertAuth"))?;
            }
            _ /* clientCertCN */ => {
                ereport(LOG)
                    .errmsg(format!(
                        "certificate validation (clientcert=verify-full) failed for user \"{user_name}\": CN mismatch"
                    ))
                    .finish(here("CheckCertAuth"))?;
            }
        }
    }

    Ok(status_check_usermap)
}

// ---------------------------------------------------------------------------
// Shared helpers.
// ---------------------------------------------------------------------------

/// `port->hba->auth_method`.
pub(crate) fn port_auth_method(port: &Port) -> UserAuth {
    port.hba.as_ref().expect("port->hba is NULL").auth_method
}

/// `port->user_name` as an owned String (NULL → empty).
pub(crate) fn port_user_name(port: &Port) -> String {
    port.user_name.clone().unwrap_or_default()
}

/// `pg_strong_random(buf, len)` (port/pg_strong_random.c).
pub(crate) fn pg_strong_random(buf: &mut [u8]) -> bool {
    pg_strong_random_seams::pg_strong_random::call(buf)
}

/// `am_walsender` (walsender.c global).
fn seams_am_walsender() -> bool {
    walsender_seams::am_walsender::call()
}

/// `CHECK_FOR_INTERRUPTS()`.
pub(crate) fn check_interrupts() -> PgResult<()> {
    postgres_seams::check_for_interrupts::call()
}

/// `pg_getnameinfo_all(&port->raddr, ..., NI_NUMERICHOST)` → host string.
pub(crate) fn getnameinfo_remote_numeric(port: &Port) -> String {
    let mut host = String::new();
    ip::pg_getnameinfo_all(
        &raddr_sockaddr(port),
        Some(&mut host),
        None,
        libc::NI_NUMERICHOST,
    );
    host
}

/// Build a `::net::SockAddr` from the connection's remote address.
pub(crate) fn raddr_sockaddr(port: &Port) -> ::net::SockAddr {
    ::net::SockAddr { addr: port.raddr.addr, salen: port.raddr.salen }
}

/// `gai_strerror(errcode)`.
pub(crate) fn gai_strerror(errcode: i32) -> String {
    // SAFETY: gai_strerror returns a static NUL-terminated C string.
    unsafe {
        let p = libc::gai_strerror(errcode);
        if p.is_null() {
            return String::new();
        }
        std::ffi::CStr::from_ptr(p).to_string_lossy().into_owned()
    }
}

/// A short-lived `MemoryContext`, the idiomatic stand-in for the implicit
/// `CurrentMemoryContext` C uses to build a `StringInfoData` message buffer.
pub(crate) struct MemCtx(::mcx::MemoryContext);

impl MemCtx {
    pub(crate) fn new(name: &'static str) -> Self {
        MemCtx(::mcx::MemoryContext::new(name))
    }
    pub(crate) fn mcx(&self) -> Mcx<'_> {
        self.0.mcx()
    }
}

// ---------------------------------------------------------------------------
// init_seams — install the four seams postinit consumes.
// ---------------------------------------------------------------------------

/// GUC variable backing storage owned by `auth.c` — the C `conf->variable`
/// targets named in `guc_tables.c` (`&pg_krb_caseins_users`,
/// `&pg_gss_accept_delegation`, `&pg_krb_server_keyfile`). auth.c reads these
/// globals directly at runtime (auth.c:937/1014/1136); they are *not* taken
/// from the ControlFile. The GUC engine seeds them from the boot values in
/// guc_tables.c and writes them on assignment through the installed accessors.
pub mod gucvars {
    use std::cell::Cell;

    thread_local! {
        /// `bool pg_krb_caseins_users` (auth.c:174). Boot value `false`.
        static KRB_CASEINS_USERS: Cell<bool> = const { Cell::new(false) };
        /// `bool pg_gss_accept_delegation` (auth.c:175). Boot value `false`.
        static GSS_ACCEPT_DELEGATION: Cell<bool> = const { Cell::new(false) };
    }

    thread_local! {
        /// `char *pg_krb_server_keyfile` (auth.c:173). Boot value
        /// `PG_KRB_SRVTAB`, which is `""` (guc_tables.c:112) — distinct from a
        /// NULL `char *`, so `Some(String::new())`.
        static KRB_SERVER_KEYFILE: std::cell::RefCell<Option<String>> =
            std::cell::RefCell::new(Some(String::new()));
    }

    pub fn krb_caseins_users() -> bool {
        KRB_CASEINS_USERS.with(|c| c.get())
    }
    pub fn set_krb_caseins_users(v: bool) {
        KRB_CASEINS_USERS.with(|c| c.set(v));
    }

    pub fn gss_accept_delegation() -> bool {
        GSS_ACCEPT_DELEGATION.with(|c| c.get())
    }
    pub fn set_gss_accept_delegation(v: bool) {
        GSS_ACCEPT_DELEGATION.with(|c| c.set(v));
    }

    pub fn krb_server_keyfile() -> Option<String> {
        KRB_SERVER_KEYFILE.with(|c| c.borrow().clone())
    }
    pub fn set_krb_server_keyfile(v: Option<String>) {
        KRB_SERVER_KEYFILE.with(|c| *c.borrow_mut() = v);
    }
}

/// Install every seam this crate owns: the `client_authentication` dispatcher
/// and the three connection-status accessors `postinit` reads.
pub fn init_seams() {
    auth_seams::client_authentication::set(client_authentication_entry);
    auth_seams::authentication_timeout::set(authentication_timeout_entry);
    // `log_connection_authorization` reads the AUTHORIZATION aspect bit of the
    // `log_connections` mask, which is owned by backend_startup; that unit
    // installs this seam (against LOG_CONNECTION_AUTHORIZATION). Installing it
    // here would route to the wrong (authentication) bit.
    auth_seams::client_authn_id::set(client_authn_id_entry);

    // `ClientAuthentication_hook` (auth.c global function pointer) — the optional
    // auth-extension plugin point. It is NULL unless a loadable module assigns it
    // (`auth_delay`, custom auth modules); the C call site is guarded
    // `if (ClientAuthentication_hook) (*ClientAuthentication_hook)(port, status)`.
    // No such module is loaded in this build, so the hook is NULL and the call is
    // a no-op. The global lives in this unit, so the seam is installed here to the
    // NULL-hook behavior (do nothing, succeed).
    auth_seams::client_authentication_hook::set(|_status| Ok(()));

    // GUC variable accessors over auth.c's `conf->variable` backing storage.
    // Read directly by auth.c at runtime (none come from the ControlFile);
    // the GUC engine reads via `.read()` and writes assignments via `.write()`.
    {
        use ::guc_tables::{vars, GucVarAccessors};
        vars::pg_krb_caseins_users.install(GucVarAccessors {
            get: gucvars::krb_caseins_users,
            set: gucvars::set_krb_caseins_users,
        });
        vars::pg_gss_accept_delegation.install(GucVarAccessors {
            get: gucvars::gss_accept_delegation,
            set: gucvars::set_gss_accept_delegation,
        });
        vars::pg_krb_server_keyfile.install(GucVarAccessors {
            get: gucvars::krb_server_keyfile,
            set: gucvars::set_krb_server_keyfile,
        });
    }
}

/// `client_authentication()` seam: read the ambient `MyProcPort` and run
/// `ClientAuthentication`.
fn client_authentication_entry() -> PgResult<()> {
    let mut result: PgResult<()> = Ok(());
    init_small_seams::with_my_proc_port::call(&mut |port| {
        let port = port.expect("ClientAuthentication: MyProcPort is NULL");
        result = ClientAuthentication(port);
    });
    result
}

/// `AuthenticationTimeout` (auth.c GUC). Default in PG is 60 seconds; the GUC
/// owner installs the real value — until then the compiled-in default applies.
fn authentication_timeout_entry() -> i32 {
    60
}

/// `MyClientConnectionInfo.authn_id`.
fn client_authn_id_entry(mcx: Mcx<'_>) -> PgResult<Option<::mcx::PgString<'_>>> {
    let info = miscinit::client_connection_info();
    match info.authn_id {
        Some(id) => Ok(Some(::mcx::PgString::from_str_in(&id, mcx)?)),
        None => Ok(None),
    }
}
