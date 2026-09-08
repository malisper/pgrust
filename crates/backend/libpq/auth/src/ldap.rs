//! auth.c LDAP arms: InitializeLDAPConnection / CheckLDAPAuth (simple bind
//! and search+bind modes), backed by ldapber's in-tree LDAPv3 client instead
//! of libldap. Feature surface mirrors an OpenLDAP (HAVE_LDAP_INITIALIZE)
//! build: `ldap://` and `ldaps://` URI lists, the empty-ldapserver DNS SRV
//! discovery (ldap_dn2domain + ldap_domain2hostlist, dnssrv), `ldaptls`
//! via ldap_start_tls_s, the client TLS policy from libldap's ldap.conf /
//! LDAP* environment (ldapconf).

use elog::ereport;
use hba::ldapurl::ldap_err2string;
use types_error::{PgResult, LOG};
use types_startup::{HbaLine, Port};

use crate::dnssrv::{ldap_dn2domain, ldap_domain2hostlist, ResolvConf, LDAP_PARAM_ERROR};
use crate::ldapber::{parse_search_filter, LdapConn, LDAP_FILTER_ERROR, LDAP_SUCCESS};
use crate::ldapconf::LdapOptions;
use crate::{
    loc, recv_password_packet, sendAuthRequest, set_authn_id, AUTH_REQ_PASSWORD, STATUS_EOF,
    STATUS_ERROR, STATUS_OK,
};

const LDAP_PORT: i32 = 389;
const LDAPS_PORT: i32 = 636;
const LDAP_NO_ATTRS: &str = "1.1";

const LPH_USERNAME: &str = "$username";

// Test seam for the DNS SRV path (auth.c:2275 ldap_domain2hostlist): the
// nameservers the SRV lookup consults instead of /etc/resolv.conf. C's
// libresolv has no such override — a unit witness needs one to point the
// lookup at an in-process fake. Test-only, absent from production builds.
#[cfg(test)]
pub(crate) static SRV_NAMESERVERS: pgsync::Mutex<Option<Vec<std::net::SocketAddr>>> =
    pgsync::Mutex::new(None);

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

// InitializeLDAPConnection (auth.c:2217-2386): the target URI list — the
// pg_hba.conf hosts with the configured port, or the DNS SRV targets of the
// base DN's domain with their own ports (auth.c:2258-2290) — under the
// configured scheme, then the protocol version and the optional StartTLS.
// STATUS_ERROR arms log their own message.
fn initialize_ldap_connection(port: &Port, hba: &HbaLine) -> PgResult<Result<LdapConn, ()>> {
    let _ = port;
    let scheme = hba.ldapscheme.as_deref().unwrap_or("ldap");

    // ldap_int_initialize: the libldap global options a fresh backend
    // reads at its first libldap call (ldap.conf, ldaprc, LDAP* env).
    let opts = LdapOptions::initialize();

    let hosts: Vec<(String, i32)> = match &hba.ldapserver {
        Some(s) if !s.is_empty() => s
            .split(' ')
            .filter(|h| !h.is_empty())
            .map(|h| (h.to_string(), hba.ldapport))
            .collect(),
        _ => {
            // No hostnames: extract a domain name from the base DN and look
            // up DNS SRV records for _ldap._tcp.<domain> (auth.c:2263-2283).
            let basedn = hba.ldapbasedn.as_deref().unwrap_or("");
            let domain = match ldap_dn2domain(basedn) {
                Ok(d) => d,
                Err(()) => {
                    ereport(LOG)
                        .errmsg("could not extract domain name from ldapbasedn")
                        .finish(loc(2271, "InitializeLDAPConnection"))?;
                    return Ok(Err(()));
                }
            };
            // A base DN with no trailing DC run gives libldap a NULL domain,
            // which C's ldap_domain2hostlist asserts on (dnssrv.c:285); the
            // could-not-find report below is the non-aborting arm of that.
            let domain = domain.unwrap_or_default();
            let list = if domain.is_empty() {
                Err(LDAP_PARAM_ERROR)
            } else {
                ldap_domain2hostlist(&domain, &ResolvConf::system())
            };
            match list {
                Ok(list) => list.into_iter().map(|(h, p)| (h, p as i32)).collect(),
                Err(_) => {
                    ereport(LOG)
                        .errmsg(format!(
                            "LDAP authentication could not find DNS SRV records for \"{domain}\""
                        ))
                        .errhint("Set an LDAP server name explicitly.")
                        .finish(loc(2279, "InitializeLDAPConnection"))?;
                    return Ok(Err(()));
                }
            }
        }
    };

    // ldap_initialize(uris) (auth.c:2328): no I/O until the first operation.
    let mut conn = LdapConn::new(hosts, scheme == "ldaps", opts);

    // ldap_set_option(LDAP_OPT_PROTOCOL_VERSION, LDAPv3) (auth.c:2358):
    // the in-tree client speaks LDAPv3 only; the call cannot fail.

    if hba.ldaptls {
        // ldap_start_tls_s (auth.c:2370-2384)
        let r = conn.start_tls();
        if r != LDAP_SUCCESS {
            errdetail_for_ldap(
                ereport(LOG).errmsg(format!(
                    "could not start LDAP TLS session: {}",
                    ldap_err2string(r)
                )),
                &conn,
            )
            .finish(loc(2377, "InitializeLDAPConnection"))?;
            conn.unbind();
            return Ok(Err(()));
        }
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
    fn search_filter_placeholder_replacement() {
        assert_eq!(
            format_search_filter("(|(uid=$username)(mail=$username))", "alice"),
            "(|(uid=alice)(mail=alice))"
        );
    }
}
