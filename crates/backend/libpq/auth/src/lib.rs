//! auth.c: ClientAuthentication and the per-method handlers. Live end-to-end:
//! the trust arm, the reject / implicit-reject arms (SQLSTATE 28000 exact),
//! sendAuthRequest / recv_password_packet, set_authn_id, auth_failed. Loud:
//! the password-family arms (pending backend-libpq-crypt + auth-scram), peer /
//! ident / radius / oauth. gss / sspi / pam / bsd / ldap / cert never reach
//! dispatch — hba rejects those methods in this build.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

#[cfg(test)]
mod tests;

use elog::{elog, ereport};
use mcx::MemoryContext;
use types_core::init::{
    uaBSD, uaCert, uaGSS, uaIdent, uaImplicitReject, uaLDAP, uaMD5, uaOAuth, uaPAM, uaPassword,
    uaPeer, uaRADIUS, uaReject, uaSCRAM, uaTrust, UserAuth,
};
use types_error::{
    ErrorLocation, PgResult, DEBUG5, ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION,
    ERRCODE_INVALID_PASSWORD, ERRCODE_PROTOCOL_VIOLATION, ERROR, FATAL, LOG,
};
use types_startup::{clientCertFull, clientCertOff, Port};

pub const STATUS_OK: i32 = 0;
pub const STATUS_ERROR: i32 = -1;
pub const STATUS_EOF: i32 = -2;

pub type AuthRequest = u32;
pub const AUTH_REQ_OK: AuthRequest = 0;
pub const AUTH_REQ_PASSWORD: AuthRequest = 3;
pub const AUTH_REQ_MD5: AuthRequest = 5;
pub const AUTH_REQ_GSS: AuthRequest = 7;
pub const AUTH_REQ_SSPI: AuthRequest = 9;
pub const AUTH_REQ_SASL: AuthRequest = 10;
pub const AUTH_REQ_SASL_CONT: AuthRequest = 11;
pub const AUTH_REQ_SASL_FIN: AuthRequest = 12;

pub const PqMsg_AuthenticationRequest: u8 = b'R';
pub const PqMsg_PasswordMessage: u8 = b'p';

pub const PG_MAX_AUTH_TOKEN_LENGTH: i32 = 65535;

fn loc(line: i32, func: &'static str) -> ErrorLocation {
    ErrorLocation::new("src/backend/libpq/auth.c", line, func)
}

pub fn auth_failed(port: &Port, status: i32, logdetail: Option<&str>) -> PgResult<()> {
    if status == STATUS_EOF {
        ipc_seams::proc_exit::call(0, init_small::globals::MyProcPid());
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
        m if m == types_core::init::uaSSPI => "SSPI authentication failed for user \"%s\"",
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
        hba.sourcefile, hba.linenumber, hba.rawline
    );
    let logdetail = match logdetail {
        Some(ld) => format!("{ld}\n{cdetail}"),
        None => cdetail,
    };

    ereport(FATAL)
        .errcode(errcode_return)
        .errmsg(errstr.replacen("%s", port.user_name.as_deref().unwrap_or(""), 1))
        .errdetail_log(logdetail)
        .finish(loc(316, "auth_failed"))
}

pub fn set_authn_id(port: &Port, id: &str) -> PgResult<()> {
    let (authn_id, _) = miscinit::client_connection_info();
    if let Some(previous) = authn_id {
        ereport(FATAL)
            .errmsg("authentication identifier set more than once")
            .errdetail_log(format!(
                "previous identifier: \"{previous}\"; new identifier: \"{id}\""
            ))
            .finish(loc(356, "set_authn_id"))?;
    }

    let auth_method = port_auth_method(port);
    miscinit::set_client_connection_info(Some(id), auth_method);

    if backend_startup::log_connections::get() & backend_startup::LOG_CONNECTION_AUTHENTICATION
        != 0
    {
        let hba = port.hba.as_ref().expect("port->hba is NULL");
        ereport(LOG)
            .errmsg(format!(
                "connection authenticated: identity=\"{id}\" method={} ({}:{})",
                hba::hba_authname(auth_method),
                hba.sourcefile,
                hba.linenumber,
            ))
            .finish(loc(371, "set_authn_id"))?;
    }
    Ok(())
}

pub fn ClientAuthentication(port: &mut Port) -> PgResult<()> {
    let logdetail: Option<String> = None;

    hba::hba_getauthmethod(port)?;

    postgres_seams::check_for_interrupts::call()?;

    // Pre-auth clientcert check: unreachable, hostssl records cannot match
    // in this no-SSL build.
    let clientcert = port
        .hba
        .as_ref()
        .expect("ClientAuthentication: port->hba is NULL")
        .clientcert;
    if clientcert != clientCertOff {
        panic!(
            "ClientAuthentication: clientcert set on matched hba line — \
             SSL is not supported by this build"
        );
    }

    let auth_method = port_auth_method(port);

    let status: i32 = match auth_method {
        m if m == uaReject => return reject_arm(port, true),
        m if m == uaImplicitReject => return reject_arm(port, false),
        m if m == uaPeer => deferred_arm("peer", "auth_peer (getpeereid + check_usermap)"),
        m if m == uaIdent => deferred_arm("ident", "ident_inet"),
        m if m == uaMD5 || m == uaSCRAM => deferred_arm(
            if m == uaMD5 { "md5" } else { "scram-sha-256" },
            "CheckPWChallengeAuth (backend-libpq-crypt + backend-libpq-auth-scram)",
        ),
        m if m == uaPassword => {
            deferred_arm("password", "CheckPasswordAuth (backend-libpq-crypt)")
        }
        m if m == uaRADIUS => deferred_arm("radius", "CheckRADIUSAuth"),
        m if m == uaOAuth => deferred_arm("oauth", "CheckSASLAuth(oauth)"),
        m if m == uaCert || m == uaTrust => STATUS_OK,
        // gss/sspi/pam/bsd/ldap: hba rejects these methods in this build.
        m => unreachable!("ClientAuthentication: unreachable auth method {m}"),
    };

    // CheckCertAuth applies only under SSL (cert method / verify-full).
    if (status == STATUS_OK && clientcert == clientCertFull) || auth_method == uaCert {
        panic!("CheckCertAuth: SSL is not supported by this build");
    }

    if backend_startup::log_connections::get() & backend_startup::LOG_CONNECTION_AUTHENTICATION
        != 0
        && status == STATUS_OK
        && miscinit::client_connection_info().0.is_none()
    {
        // set_authn_id never ran (e.g. trust): log the connection here.
        let hba = port.hba.as_ref().expect("port->hba is NULL");
        ereport(LOG)
            .errmsg(format!(
                "connection authenticated: user=\"{}\" method={} ({}:{})",
                port.user_name.as_deref().unwrap_or(""),
                hba::hba_authname(auth_method),
                hba.sourcefile,
                hba.linenumber,
            ))
            .finish(loc(659, "ClientAuthentication"))?;
    }

    // ClientAuthentication_hook: NULL in this build (no auth module loaded).

    if status == STATUS_OK {
        sendAuthRequest(port, AUTH_REQ_OK, &[])?;
    } else {
        auth_failed(port, status, logdetail.as_deref())?;
    }

    Ok(())
}

#[cold]
fn deferred_arm(method: &str, what: &str) -> i32 {
    panic!("ClientAuthentication: \"{method}\" arm deferred — {what} unported");
}

fn reject_arm(port: &Port, explicit_reject: bool) -> PgResult<()> {
    let mut hostinfo = String::new();
    ip::pg_getnameinfo_all(&port.raddr, Some(&mut hostinfo), None, libc::NI_NUMERICHOST);

    let encryption_state = if port.ssl_in_use {
        "SSL encryption"
    } else {
        "no encryption"
    };

    let user_name = port.user_name.as_deref().unwrap_or("");
    let database_name = port.database_name.as_deref().unwrap_or("");
    // walsender.c unported: am_walsender statically false (postinit
    // precedent); the replication wordings are dead here.

    let mut builder = if explicit_reject {
        ereport(FATAL)
            .errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION)
            .errmsg(format!(
                "pg_hba.conf rejects connection for host \"{hostinfo}\", user \"{user_name}\", database \"{database_name}\", {encryption_state}"
            ))
    } else {
        ereport(FATAL)
            .errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION)
            .errmsg(format!(
                "no pg_hba.conf entry for host \"{hostinfo}\", user \"{user_name}\", database \"{database_name}\", {encryption_state}"
            ))
    };

    // HOSTNAME_LOOKUP_DETAIL applies only to the implicit-reject path.
    if !explicit_reject {
        if let Some(detail) = hostname_lookup_detail(port) {
            builder = builder.errdetail_log(detail);
        }
    }

    builder.finish(loc(
        if explicit_reject { 463 } else { 530 },
        "ClientAuthentication",
    ))
}

fn hostname_lookup_detail(port: &Port) -> Option<String> {
    let resolv = port.remote_hostname_resolv;
    match port.remote_hostname.as_deref() {
        Some(h) => match resolv {
            1 => Some(format!(
                "Client IP address resolved to \"{h}\", forward lookup matches."
            )),
            0 => Some(format!(
                "Client IP address resolved to \"{h}\", forward lookup not checked."
            )),
            -1 => Some(format!(
                "Client IP address resolved to \"{h}\", forward lookup does not match."
            )),
            -2 => Some(format!(
                "Could not translate client host name \"{h}\" to IP address: {}.",
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

pub fn sendAuthRequest(_port: &Port, areq: AuthRequest, extradata: &[u8]) -> PgResult<()> {
    postgres_seams::check_for_interrupts::call()?;

    let scratch = MemoryContext::new("sendAuthRequest");
    let mut buf = pqformat::pq_beginmessage(scratch.mcx(), PqMsg_AuthenticationRequest)?;
    pqformat::pq_sendint32(&mut buf, areq)?;
    if !extradata.is_empty() {
        pqformat::pq_sendbytes(&mut buf, extradata)?;
    }
    pqformat::pq_endmessage(buf)?;

    // AUTH_REQ_OK / AUTH_REQ_SASL_FIN need not be sent until ready for queries.
    if areq != AUTH_REQ_OK && areq != AUTH_REQ_SASL_FIN {
        pqcomm::pq_flush()?;
    }

    postgres_seams::check_for_interrupts::call()?;
    Ok(())
}

pub fn recv_password_packet(_port: &Port) -> PgResult<Option<String>> {
    pqcomm::pq_startmsgread()?;

    let mtype = pqcomm::pq_getbyte()?;
    if mtype != PqMsg_PasswordMessage as i32 {
        // EOF (-1) is silent; any other type is a protocol violation.
        if mtype != -1 {
            ereport(ERROR)
                .errcode(ERRCODE_PROTOCOL_VIOLATION)
                .errmsg(format!(
                    "expected password response, got message type {mtype}"
                ))
                .finish(loc(722, "recv_password_packet"))?;
        }
        return Ok(None);
    }

    let scratch = MemoryContext::new("recv_password_packet");
    let mut buf = stringinfo::StringInfo::new_in(scratch.mcx())?;
    if pqcomm::pq_getmessage(&mut buf, PG_MAX_AUTH_TOKEN_LENGTH)? != 0 {
        return Ok(None); // EOF; pq_getmessage already logged
    }

    // Packet length must agree with the contained NUL-terminated string.
    let data = buf.as_bytes();
    let strlen = data.iter().position(|&b| b == 0).unwrap_or(data.len());
    if strlen + 1 != data.len() {
        ereport(ERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg("invalid password packet size")
            .finish(loc(741, "recv_password_packet"))?;
    }

    if data.len() == 1 {
        ereport(ERROR)
            .errcode(ERRCODE_INVALID_PASSWORD)
            .errmsg("empty password returned by client")
            .finish(loc(752, "recv_password_packet"))?;
    }

    // Do not echo password to logs, for security.
    elog(DEBUG5, "received password packet")?;

    Ok(Some(String::from_utf8_lossy(&data[..strlen]).into_owned()))
}

pub(crate) fn port_auth_method(port: &Port) -> UserAuth {
    port.hba.as_ref().expect("port->hba is NULL").auth_method
}

fn gai_strerror(errcode: i32) -> String {
    // SAFETY: gai_strerror returns a static NUL-terminated C string.
    unsafe {
        let p = libc::gai_strerror(errcode);
        if p.is_null() {
            return String::new();
        }
        std::ffi::CStr::from_ptr(p).to_string_lossy().into_owned()
    }
}

pub fn init_seams() {
    auth_seams::client_authentication::set(client_authentication_entry);
}

fn client_authentication_entry() -> PgResult<()> {
    init_small::globals::WithMyProcPort(ClientAuthentication)
}
