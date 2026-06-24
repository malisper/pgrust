//! `CheckPAMAuth` (`auth.c:2029`) + `pam_passwd_conv_proc` (`auth.c:1928`) —
//! PAM authentication. The `libpam` FFI + the conversation loop live in the
//! `pam_libpam_ffi` provider; this module supplies the `Port`-derived inputs
//! (service name, RHOST, the password-recv closure) and maps the provider's LOG
//! lines to `ereport(LOG)`, then records `set_authn_id` on success — exactly the
//! auth.c structure.

use ::net::Port;
use ::types_error::{PgResult, LOG};
use ::utils_error::ereport;
use pam_libpam_ffi::PamOutcome;

use crate::{
    here, recv_password_packet, sendAuthRequest, set_authn_id, AUTH_REQ_PASSWORD, STATUS_EOF,
    STATUS_ERROR, STATUS_OK,
};

/// `PGSQL_PAM_SERVICE` (auth.c:95): the default PAM service name.
const PGSQL_PAM_SERVICE: &str = "postgresql";

/// `CheckPAMAuth(port, port->user_name, "")` (auth.c:598/2029).
pub fn CheckPAMAuth(port: &Port) -> PgResult<i32> {
    let user = port.user_name.as_deref().unwrap_or("");

    let hba = port.hba.as_ref().expect("CheckPAMAuth: port->hba is NULL");

    // Optionally set the service name from pg_hba.conf.
    let service = match hba.pamservice.as_deref() {
        Some(s) if !s.is_empty() => s,
        _ => PGSQL_PAM_SERVICE,
    };

    // PAM_RHOST: only for non-local connections (auth.c:2082). numeric unless
    // pam_use_hostname=1.
    let rhost: Option<String> = if hba.conntype != ::net::ctLocal {
        let flags = if hba.pam_use_hostname {
            0
        } else {
            libc::NI_NUMERICHOST | libc::NI_NUMERICSERV
        };
        let mut host = String::new();
        let rc = ::ip::pg_getnameinfo_all(
            &crate::raddr_sockaddr(port),
            Some(&mut host),
            None,
            flags,
        );
        if rc != 0 {
            ereport(::types_error::WARNING)
                .errmsg_internal(format!(
                    "pg_getnameinfo_all() failed: {}",
                    crate::gai_strerror(rc)
                ))
                .finish(here("CheckPAMAuth"))?;
            return Ok(STATUS_ERROR);
        }
        Some(host)
    } else {
        None
    };

    // The conversation's password-fetch step (auth.c:1975): sendAuthRequest +
    // recv_password_packet. A sendAuthRequest/recv error (FATAL/ERROR) is
    // captured in `recv_err` and re-raised after the FFI call returns (it must
    // not unwind across the C frames).
    let mut recv_err: Option<::types_error::PgError> = None;
    let result = {
        let recv_err_ref = &mut recv_err;
        let mut closure = || -> Option<String> {
            if let Err(e) = sendAuthRequest(port, AUTH_REQ_PASSWORD, &[]) {
                *recv_err_ref = Some(e);
                return None;
            }
            match recv_password_packet(port) {
                Ok(p) => p,
                Err(e) => {
                    *recv_err_ref = Some(e);
                    None
                }
            }
        };
        pam_libpam_ffi::check_pam_auth(service, user, rhost.as_deref(), "", &mut closure)
    };

    // Re-raise a recv_password_packet/sendAuthRequest error (FATAL/ERROR).
    if let Some(e) = recv_err {
        return Err(e);
    }

    let (outcome, logs) = match result {
        Ok(v) => v,
        Err(msg) => {
            // A pre-conversation setup failure (e.g. invalid C string); log + error.
            ereport(LOG).errmsg(msg).finish(here("CheckPAMAuth"))?;
            return Ok(STATUS_ERROR);
        }
    };

    // Emit the provider's accumulated LOG lines as the C ereport(LOG)s.
    for line in logs {
        ereport(LOG).errmsg_internal(line).finish(here("CheckPAMAuth"))?;
    }

    let status = match outcome {
        PamOutcome::Ok => STATUS_OK,
        PamOutcome::Eof => STATUS_EOF,
        PamOutcome::Error => STATUS_ERROR,
    };

    if status == STATUS_OK {
        set_authn_id(port, user)?;
    }

    Ok(status)
}
