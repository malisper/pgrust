//! `CheckLDAPAuth` (`auth.c:2436`) + `InitializeLDAPConnection` (`auth.c:2217`)
//! + `FormatSearchFilter` (`auth.c:2413`) — LDAP authentication. The OpenLDAP
//! `libldap` FFI and the bind/search flow live in the `ldap_openldap_ffi`
//! provider; this module performs the auth.c pre-flow (validate the server,
//! default the port, `sendAuthRequest` + `recv_password_packet`), invokes the
//! provider, maps its LOG lines to `ereport(LOG)`, and records `set_authn_id`
//! on success.

use ::net::Port;
use ::types_error::{PgResult, LOG};
use ::utils_error::ereport;
use ldap_openldap_ffi::{LdapConfig, LdapOutcome, LDAPS_PORT, LDAP_PORT};

use crate::{
    here, recv_password_packet, sendAuthRequest, set_authn_id, AUTH_REQ_PASSWORD, STATUS_EOF,
    STATUS_ERROR, STATUS_OK,
};

/// `CheckLDAPAuth(port)` (auth.c:2436).
pub fn CheckLDAPAuth(port: &Port) -> PgResult<i32> {
    let hba = port.hba.as_ref().expect("CheckLDAPAuth: port->hba is NULL");
    let user_name = port.user_name.as_deref().unwrap_or("");

    let have_server = hba.ldapserver.as_deref().is_some_and(|s| !s.is_empty());
    let have_basedn = hba.ldapbasedn.as_deref().is_some_and(|s| !s.is_empty());

    // HAVE_LDAP_INITIALIZE: allow an empty server iff we have a basedn (DNS SRV).
    if !have_server && !have_basedn {
        ereport(LOG)
            .errmsg("LDAP server not specified, and no ldapbasedn")
            .finish(here("CheckLDAPAuth"))?;
        return Ok(STATUS_ERROR);
    }

    // Default the port from the scheme (auth.c:2473).
    let mut ldapport = hba.ldapport;
    if ldapport == 0 {
        ldapport = if hba.ldapscheme.as_deref() == Some("ldaps") {
            LDAPS_PORT
        } else {
            LDAP_PORT
        };
    }

    sendAuthRequest(port, AUTH_REQ_PASSWORD, &[])?;

    let passwd = match recv_password_packet(port)? {
        Some(p) => p,
        None => return Ok(STATUS_EOF), // client wouldn't send password
    };

    let cfg = LdapConfig {
        ldapscheme: hba.ldapscheme.clone(),
        ldapserver: hba.ldapserver.clone(),
        ldapport,
        ldaptls: hba.ldaptls,
        ldapbasedn: hba.ldapbasedn.clone(),
        ldapbinddn: hba.ldapbinddn.clone(),
        ldapbindpasswd: hba.ldapbindpasswd.clone(),
        ldapsearchattribute: hba.ldapsearchattribute.clone(),
        ldapsearchfilter: hba.ldapsearchfilter.clone(),
        ldapscope: hba.ldapscope,
        ldapprefix: hba.ldapprefix.clone(),
        ldapsuffix: hba.ldapsuffix.clone(),
    };

    let (outcome, logs) = match ldap_openldap_ffi::check_ldap_auth(&cfg, user_name, &passwd) {
        Ok(v) => v,
        Err(msg) => {
            ereport(LOG).errmsg(msg).finish(here("CheckLDAPAuth"))?;
            return Ok(STATUS_ERROR);
        }
    };

    for line in logs {
        ereport(LOG).errmsg_internal(line).finish(here("CheckLDAPAuth"))?;
    }

    match outcome {
        LdapOutcome::Ok(dn) => {
            // Save the original bind DN as the authenticated identity.
            set_authn_id(port, &dn)?;
            Ok(STATUS_OK)
        }
        LdapOutcome::Error => Ok(STATUS_ERROR),
    }
}
