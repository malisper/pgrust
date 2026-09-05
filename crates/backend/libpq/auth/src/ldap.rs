//! auth.c LDAP arms: InitializeLDAPConnection / CheckLDAPAuth (simple bind
//! and search+bind modes), backed by ldapber's in-tree LDAPv3 client instead
//! of libldap. Feature surface mirrors an OpenLDAP build minus TLS and DNS
//! SRV: `ldaps` and `ldaptls` fail cleanly at connection setup with
//! ldap_err2string(-12) "Not Supported", and the empty-ldapserver DNS SRV
//! path fails with C's could-not-find-SRV-records message after a C-exact
//! ldap_dn2domain extraction.

use elog::ereport;
use hba::ldapurl::ldap_err2string;
use types_error::{PgResult, LOG};
use types_startup::{HbaLine, Port};

use crate::ldapber::{parse_search_filter, LdapConn, LDAP_FILTER_ERROR, LDAP_SUCCESS};
use crate::{
    loc, recv_password_packet, sendAuthRequest, set_authn_id, AUTH_REQ_PASSWORD, STATUS_EOF,
    STATUS_ERROR, STATUS_OK,
};

const LDAP_PORT: i32 = 389;
const LDAPS_PORT: i32 = 636;
const LDAP_NO_ATTRS: &str = "1.1";

const LPH_USERNAME: &str = "$username";

// ldap_dn2domain: ou=blah,dc=foo,dc=bar -> foo.bar. Non-empty non-DN input
// (no '=' in a component) is a parse failure, C's "could not extract domain
// name from ldapbasedn" arm.
fn ldap_dn2domain(dn: &str) -> Result<String, ()> {
    let mut parts: Vec<&str> = Vec::new();
    for rdn in dn.split(',') {
        let rdn = rdn.trim();
        if rdn.is_empty() {
            continue;
        }
        let Some((attr, val)) = rdn.split_once('=') else {
            return Err(());
        };
        if attr.trim().eq_ignore_ascii_case("dc") {
            parts.push(val.trim());
        }
    }
    Ok(parts.join("."))
}

fn format_search_filter(pattern: &str, user_name: &str) -> String {
    pattern.replace(LPH_USERNAME, user_name)
}

// errdetail_for_ldap: attach the server's diagnostic message if present.
fn errdetail_for_ldap(b: elog::ErrorBuilder, conn: &LdapConn) -> elog::ErrorBuilder {
    match conn.diagnostic_message() {
        Some(m) if !m.is_empty() => b.errdetail(format!("LDAP diagnostics: {m}")),
        _ => b,
    }
}

// InitializeLDAPConnection (auth.c:2217): resolve the target host list and
// scheme into an unconnected LdapConn (like ldap_initialize, the TCP connect
// happens at the first operation). STATUS_ERROR arms log their own message.
fn initialize_ldap_connection(port: &Port, hba: &HbaLine) -> PgResult<Result<LdapConn, ()>> {
    let _ = port;
    let scheme = hba.ldapscheme.as_deref().unwrap_or("ldap");

    if scheme == "ldaps" {
        // In-tree client has no TLS; C's OpenLDAP build would connect.
        ereport(LOG)
            .errmsg(format!(
                "could not initialize LDAP: {}",
                ldap_err2string(-12)
            ))
            .finish(loc(2330, "InitializeLDAPConnection"))?;
        return Ok(Err(()));
    }

    let hosts: Vec<(String, i32)> = match &hba.ldapserver {
        Some(s) if !s.is_empty() => s
            .split(' ')
            .filter(|h| !h.is_empty())
            .map(|h| (h.to_string(), hba.ldapport))
            .collect(),
        _ => {
            // No hostnames: C asks OpenLDAP for DNS SRV records derived from
            // the base DN. The extraction is ported; the SRV lookup is not,
            // so it fails with C's could-not-find message.
            let basedn = hba.ldapbasedn.as_deref().unwrap_or("");
            let domain = match ldap_dn2domain(basedn) {
                Ok(d) => d,
                Err(()) => {
                    ereport(LOG)
                        .errmsg("could not extract domain name from ldapbasedn")
                        .finish(loc(2268, "InitializeLDAPConnection"))?;
                    return Ok(Err(()));
                }
            };
            ereport(LOG)
                .errmsg(format!(
                    "LDAP authentication could not find DNS SRV records for \"{domain}\""
                ))
                .errhint("Set an LDAP server name explicitly.")
                .finish(loc(2276, "InitializeLDAPConnection"))?;
            return Ok(Err(()));
        }
    };

    let conn = LdapConn::new(hosts);

    if hba.ldaptls {
        // ldap_start_tls_s: no TLS in the in-tree client.
        ereport(LOG)
            .errmsg(format!(
                "could not start LDAP TLS session: {}",
                ldap_err2string(-12)
            ))
            .finish(loc(2380, "InitializeLDAPConnection"))?;
        return Ok(Err(()));
    }

    Ok(Ok(conn))
}

pub(crate) fn CheckLDAPAuth(port: &mut Port) -> PgResult<i32> {
    let mut hba = port
        .hba
        .as_ref()
        .expect("CheckLDAPAuth: port->hba is NULL")
        .clone();

    if hba.ldapserver.as_deref().unwrap_or("").is_empty()
        && hba.ldapbasedn.as_deref().unwrap_or("").is_empty()
    {
        ereport(LOG)
            .errmsg("LDAP server not specified, and no ldapbasedn")
            .finish(loc(2454, "CheckLDAPAuth"))?;
        return Ok(STATUS_ERROR);
    }

    let server_name = hba.ldapserver.clone().unwrap_or_default();

    if hba.ldapport == 0 {
        hba.ldapport = if hba.ldapscheme.as_deref() == Some("ldaps") {
            LDAPS_PORT
        } else {
            LDAP_PORT
        };
    }

    sendAuthRequest(port, AUTH_REQ_PASSWORD, &[])?;
    let Some(passwd) = recv_password_packet(port)? else {
        return Ok(STATUS_EOF); // client wouldn't send password
    };

    let mut ldap = match initialize_ldap_connection(port, &hba)? {
        Ok(c) => c,
        Err(()) => return Ok(STATUS_ERROR), // error message already sent
    };

    let user_name = port.user_name.clone().unwrap_or_default();

    let fulluser: String = if let Some(basedn) = hba.ldapbasedn.as_deref() {
        if user_name
            .chars()
            .any(|c| matches!(c, '*' | '(' | ')' | '\\' | '/'))
        {
            ereport(LOG)
                .errmsg("invalid character in user name for LDAP authentication")
                .finish(loc(2521, "CheckLDAPAuth"))?;
            ldap.unbind();
            return Ok(STATUS_ERROR);
        }

        let binddn = hba.ldapbinddn.clone().unwrap_or_default();
        let bindpasswd = hba.ldapbindpasswd.clone().unwrap_or_default();
        let r = ldap.simple_bind(&binddn, bindpasswd.as_bytes());
        if r != LDAP_SUCCESS {
            errdetail_for_ldap(
                ereport(LOG).errmsg(format!(
                    "could not perform initial LDAP bind for ldapbinddn \"{binddn}\" on server \"{server_name}\": {}",
                    ldap_err2string(r)
                )),
                &ldap,
            )
            .finish(loc(2540, "CheckLDAPAuth"))?;
            ldap.unbind();
            return Ok(STATUS_ERROR);
        }

        let filter = if let Some(f) = hba.ldapsearchfilter.as_deref() {
            format_search_filter(f, &user_name)
        } else if let Some(attr) = hba.ldapsearchattribute.as_deref() {
            format!("({attr}={user_name})")
        } else {
            format!("(uid={user_name})")
        };

        let search = match parse_search_filter(&filter) {
            Ok(parsed) => ldap.search(basedn, hba.ldapscope, &parsed, &[LDAP_NO_ATTRS]),
            Err(()) => Err(LDAP_FILTER_ERROR),
        };
        let entries = match search {
            Ok(entries) => entries,
            Err(r) => {
                errdetail_for_ldap(
                    ereport(LOG).errmsg(format!(
                        "could not search LDAP for filter \"{filter}\" on server \"{server_name}\": {}",
                        ldap_err2string(r)
                    )),
                    &ldap,
                )
                .finish(loc(2569, "CheckLDAPAuth"))?;
                ldap.unbind();
                return Ok(STATUS_ERROR);
            }
        };

        let count = entries.len();
        if count != 1 {
            if count == 0 {
                ereport(LOG)
                    .errmsg(format!("LDAP user \"{user_name}\" does not exist"))
                    .errdetail(format!(
                        "LDAP search for filter \"{filter}\" on server \"{server_name}\" returned no entries."
                    ))
                    .finish(loc(2586, "CheckLDAPAuth"))?;
            } else {
                ereport(LOG)
                    .errmsg(format!("LDAP user \"{user_name}\" is not unique"))
                    .errdetail_plural(
                        format!(
                            "LDAP search for filter \"{filter}\" on server \"{server_name}\" returned {count} entry."
                        ),
                        format!(
                            "LDAP search for filter \"{filter}\" on server \"{server_name}\" returned {count} entries."
                        ),
                        count as u64,
                    )
                    .finish(loc(2591, "CheckLDAPAuth"))?;
            }
            ldap.unbind();
            return Ok(STATUS_ERROR);
        }

        entries.into_iter().next().expect("count == 1")
    } else {
        format!(
            "{}{}{}",
            hba.ldapprefix.as_deref().unwrap_or(""),
            user_name,
            hba.ldapsuffix.as_deref().unwrap_or("")
        )
    };

    let r = ldap.simple_bind(&fulluser, &passwd);
    if r != LDAP_SUCCESS {
        errdetail_for_ldap(
            ereport(LOG).errmsg(format!(
                "LDAP login failed for user \"{fulluser}\" on server \"{server_name}\": {}",
                ldap_err2string(r)
            )),
            &ldap,
        )
        .finish(loc(2652, "CheckLDAPAuth"))?;
        ldap.unbind();
        return Ok(STATUS_ERROR);
    }

    set_authn_id(port, &fulluser)?;

    ldap.unbind();
    Ok(STATUS_OK)
}

#[cfg(all(test, not(target_family = "wasm")))]
mod ldap_tests {
    use super::*;

    #[test]
    fn dn2domain_extracts_dc_components() {
        assert_eq!(
            ldap_dn2domain("ou=blah,dc=foo,dc=bar").unwrap(),
            "foo.bar"
        );
        assert_eq!(ldap_dn2domain("ou=people,ou=x").unwrap(), "");
        assert!(ldap_dn2domain("garbage").is_err());
    }

    #[test]
    fn search_filter_placeholder_replacement() {
        assert_eq!(
            format_search_filter("(|(uid=$username)(mail=$username))", "alice"),
            "(|(uid=alice)(mail=alice))"
        );
    }
}
