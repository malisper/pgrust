use std::net::IpAddr;

use ip::SockAddr;
use types_core::init::uaImplicitReject;
use types_core::Oid;
use types_error::{PgResult, DEBUG2, ERRCODE_INTERNAL_ERROR, LOG};
use types_startup::{
    ctHostGSS, ctHostSSL, ctLocal, ipCmpAll, ipCmpMask, ipCmpSameHost, ipCmpSameNet, AuthToken,
    HbaLine, IPCompareMethod, Port,
};

use crate::parse_hba::enable_gss;
use crate::{
    pg_strcasecmp, report_plain, token_has_regexp, token_is_keyword, token_is_member_check,
    token_matches, token_matches_insensitive, with_parsed_hba_lines,
};

pub(crate) fn ss_family(sa: &SockAddr) -> i32 {
    if sa.salen == 0 {
        return ip::sys::AF_UNSPEC;
    }
    ip::sockaddr_family(sa)
}

pub(crate) fn sockaddr_to_ipaddr(sa: &SockAddr) -> Option<IpAddr> {
    match ss_family(sa) {
        f if f == ip::sys::AF_INET => {
            // Copy the unaligned buffer bytes into an aligned local before
            // reading sin_addr — never form a & to the misaligned buffer.
            // SAFETY: family is AF_INET, so the buffer holds a sockaddr_in.
            let sin: ip::sys::sockaddr_in = unsafe {
                let mut tmp = core::mem::MaybeUninit::<ip::sys::sockaddr_in>::zeroed();
                core::ptr::copy_nonoverlapping(
                    sa.addr.as_ptr(),
                    tmp.as_mut_ptr().cast::<u8>(),
                    core::mem::size_of::<ip::sys::sockaddr_in>(),
                );
                tmp.assume_init()
            };
            Some(IpAddr::V4(std::net::Ipv4Addr::from(u32::from_be(
                sin.sin_addr.s_addr,
            ))))
        }
        f if f == ip::sys::AF_INET6 => {
            // SAFETY: family is AF_INET6, so the buffer holds a sockaddr_in6.
            let sin6: ip::sys::sockaddr_in6 = unsafe {
                let mut tmp = core::mem::MaybeUninit::<ip::sys::sockaddr_in6>::zeroed();
                core::ptr::copy_nonoverlapping(
                    sa.addr.as_ptr(),
                    tmp.as_mut_ptr().cast::<u8>(),
                    core::mem::size_of::<ip::sys::sockaddr_in6>(),
                );
                tmp.assume_init()
            };
            Some(IpAddr::V6(std::net::Ipv6Addr::from(sin6.sin6_addr.s6_addr)))
        }
        _ => None,
    }
}

pub(crate) fn ipaddr_to_sockaddr(ipn: &IpAddr) -> SockAddr {
    let mut sa = SockAddr::zeroed();
    match ipn {
        IpAddr::V4(v4) => {
            // SAFETY: zeroed sockaddr_in is a valid all-fields-init value.
            let mut sin: ip::sys::sockaddr_in =
                unsafe { core::mem::MaybeUninit::zeroed().assume_init() };
            sin.sin_family = ip::sys::AF_INET as ip::sys::sa_family_t;
            sin.sin_addr.s_addr = u32::from(*v4).to_be();
            // SAFETY: sizeof(sockaddr_in) bytes into the storage-sized buffer.
            unsafe {
                core::ptr::copy_nonoverlapping(
                    core::ptr::from_ref(&sin).cast::<u8>(),
                    sa.addr.as_mut_ptr(),
                    core::mem::size_of::<ip::sys::sockaddr_in>(),
                );
            }
            sa.salen = core::mem::size_of::<ip::sys::sockaddr_in>() as u32;
        }
        IpAddr::V6(v6) => {
            // SAFETY: zeroed sockaddr_in6 is a valid all-fields-init value.
            let mut sin6: ip::sys::sockaddr_in6 =
                unsafe { core::mem::MaybeUninit::zeroed().assume_init() };
            sin6.sin6_family = ip::sys::AF_INET6 as ip::sys::sa_family_t;
            sin6.sin6_addr.s6_addr = v6.octets();
            // SAFETY: sizeof(sockaddr_in6) bytes into the storage-sized buffer.
            unsafe {
                core::ptr::copy_nonoverlapping(
                    core::ptr::from_ref(&sin6).cast::<u8>(),
                    sa.addr.as_mut_ptr(),
                    core::mem::size_of::<ip::sys::sockaddr_in6>(),
                );
            }
            sa.salen = core::mem::size_of::<ip::sys::sockaddr_in6>() as u32;
        }
    }
    sa
}

// is_member (hba.c:924): userid's non-super membership of role.
fn is_member(userid: Oid, role: &str) -> PgResult<bool> {
    if userid == 0 {
        return Ok(false); // if user not exist, say "no"
    }
    let roleid = acl_seams::get_role_oid::call(role, true)?;
    if roleid == 0 {
        return Ok(false); // if target role not exist, say "no"
    }
    acl_seams::is_member_of_role_nosuper::call(userid, roleid)
}

pub(crate) fn check_role(
    role: &str,
    roleid: Oid,
    tokens: &[AuthToken],
    case_insensitive: bool,
) -> PgResult<bool> {
    for tok in tokens {
        if token_is_member_check(tok) {
            if is_member(roleid, &tok.string[1..])? {
                return Ok(true);
            }
        } else if token_is_keyword(tok, "all") {
            return Ok(true);
        } else if token_has_regexp(tok) {
            if crate::token::regexec_auth_token(role, tok, &mut [])?.unwrap_or(false) {
                return Ok(true);
            }
        } else if case_insensitive {
            if token_matches_insensitive(tok, role.as_bytes()) {
                return Ok(true);
            }
        } else if token_matches(tok, role.as_bytes()) {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(crate) fn check_db(
    dbname: &str,
    role: &str,
    roleid: Oid,
    tokens: &[AuthToken],
) -> PgResult<bool> {
    let am_walsender = walsender_seams::am_walsender();
    let am_db_walsender = walsender_seams::am_db_walsender();

    for tok in tokens {
        if am_walsender && !am_db_walsender {
            // physical replication walsender connections match only the
            // replication keyword
            if token_is_keyword(tok, "replication") {
                return Ok(true);
            }
        } else if token_is_keyword(tok, "all") {
            return Ok(true);
        } else if token_is_keyword(tok, "sameuser") {
            if dbname == role {
                return Ok(true);
            }
        } else if token_is_keyword(tok, "samegroup") || token_is_keyword(tok, "samerole") {
            if is_member(roleid, dbname)? {
                return Ok(true);
            }
        } else if token_is_keyword(tok, "replication") {
            continue; // never match this if not walsender
        } else if token_has_regexp(tok) {
            if crate::token::regexec_auth_token(dbname, tok, &mut [])?.unwrap_or(false) {
                return Ok(true);
            }
        } else if token_matches(tok, dbname.as_bytes()) {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(crate) fn hostname_match(pattern: &[u8], actual_hostname: &[u8]) -> bool {
    if pattern.first() == Some(&b'.') {
        // suffix match
        if actual_hostname.len() < pattern.len() {
            return false;
        }
        pg_strcasecmp(pattern, &actual_hostname[actual_hostname.len() - pattern.len()..]) == 0
    } else {
        pg_strcasecmp(pattern, actual_hostname) == 0
    }
}

pub(crate) fn check_hostname(port: &mut Port, hostname: &str) -> PgResult<bool> {
    // Quick out if remote host name already known bad.
    if port.remote_hostname_resolv < 0 {
        return Ok(false);
    }

    if port.remote_hostname.is_none() {
        let mut node = String::new();
        let ret = ip::pg_getnameinfo_all(&port.raddr, Some(&mut node), None, ip::sys::NI_NAMEREQD);
        if ret != 0 {
            // remember failure; don't complain in the postmaster log yet
            port.remote_hostname_resolv = -2;
            port.remote_hostname_errcode = ret;
            return Ok(false);
        }
        port.remote_hostname = Some(node);
    }

    let remote_hostname = port.remote_hostname.clone().expect("set above");
    if !hostname_match(hostname.as_bytes(), remote_hostname.as_bytes()) {
        return Ok(false);
    }

    // If we already verified the forward lookup, we're done.
    if port.remote_hostname_resolv == 1 {
        return Ok(true);
    }

    // Lookup IP from host name and check against original IP.
    let mut gai_result: Vec<ip::PgAddrInfo> = Vec::new();
    let hint = ip::AddrInfoHint::default();
    let ret = ip::pg_getaddrinfo_all(Some(&remote_hostname), None, &hint, &mut gai_result);
    if ret != 0 {
        port.remote_hostname_resolv = -2;
        port.remote_hostname_errcode = ret;
        return Ok(false);
    }

    let client_ip = sockaddr_to_ipaddr(&port.raddr);
    let found = client_ip.is_some()
        && gai_result
            .iter()
            .any(|gai| sockaddr_to_ipaddr(&gai.addr) == client_ip);

    if !found {
        report_plain(
            DEBUG2,
            1155,
            "check_hostname",
            ERRCODE_INTERNAL_ERROR,
            format!(
                "pg_hba.conf host name \"{hostname}\" rejected because address resolution did not return a match with IP address of client"
            ),
        )?;
    }

    port.remote_hostname_resolv = if found { 1 } else { -1 };
    Ok(found)
}

pub(crate) fn check_ip(raddr: &SockAddr, addr: &SockAddr, mask: &SockAddr) -> bool {
    if ss_family(raddr) != ss_family(addr) {
        return false;
    }
    match (
        sockaddr_to_ipaddr(raddr),
        sockaddr_to_ipaddr(addr),
        sockaddr_to_ipaddr(mask),
    ) {
        (Some(r), Some(a), Some(m)) => ifaddr::pg_range_sockaddr(&r, &a, &m),
        _ => false,
    }
}

pub(crate) fn check_same_host_or_net(
    raddr: &SockAddr,
    method: IPCompareMethod,
) -> PgResult<bool> {
    let mut result = false;
    let res = ifaddr::pg_foreach_ifaddr(|addr, netmask| {
        if result {
            return;
        }
        if method == ipCmpSameHost {
            match ifaddr::pg_sockaddr_cidr_mask(
                None,
                match addr {
                    IpAddr::V4(_) => ifaddr::AddressFamily::Inet,
                    IpAddr::V6(_) => ifaddr::AddressFamily::Inet6,
                },
            ) {
                Ok(mask) => {
                    result = check_ip(raddr, &ipaddr_to_sockaddr(&addr), &ipaddr_to_sockaddr(&mask));
                }
                Err(_) => {}
            }
        } else {
            result = check_ip(
                raddr,
                &ipaddr_to_sockaddr(&addr),
                &ipaddr_to_sockaddr(&netmask),
            );
        }
    });

    if res.is_err() {
        report_plain(
            LOG,
            1249,
            "check_same_host_or_net",
            ERRCODE_INTERNAL_ERROR,
            "error enumerating network interfaces".to_string(),
        )?;
        return Ok(false);
    }
    Ok(result)
}

pub fn check_hba(port: &mut Port) -> PgResult<()> {
    // get_role_oid(port->user_name, true) — missing role folds to InvalidOid.
    let user_name = port.user_name.clone().unwrap_or_default();
    let roleid = acl_seams::get_role_oid::call(&user_name, true)?;
    let dbname = port.database_name.clone().unwrap_or_default();

    let matched: Option<HbaLine> = with_parsed_hba_lines(|lines| -> PgResult<Option<HbaLine>> {
        'lines: for hba in lines {
            // Check connection type.
            if hba.conntype == ctLocal {
                if ss_family(&port.raddr) != ip::sys::AF_UNIX {
                    continue;
                }
            } else {
                if ss_family(&port.raddr) == ip::sys::AF_UNIX {
                    continue;
                }

                // Check SSL state: SSL matches host+hostssl, plain matches
                // host+hostnossl.
                if port.ssl_in_use {
                    if hba.conntype == types_startup::ctHostNoSSL {
                        continue;
                    }
                } else if hba.conntype == ctHostSSL {
                    continue;
                }

                // Check GSSAPI state (no-GSS build: gssenc never true).
                if !enable_gss() && hba.conntype == ctHostGSS {
                    continue;
                }

                // Check IP address.
                if hba.ip_cmp_method == ipCmpMask {
                    if let Some(hostname) = hba.hostname.clone() {
                        if !check_hostname(port, &hostname)? {
                            continue;
                        }
                    } else if !check_ip(&port.raddr, &hba.addr, &hba.mask) {
                        continue;
                    }
                } else if hba.ip_cmp_method == ipCmpAll {
                    // matches anything
                } else if hba.ip_cmp_method == ipCmpSameHost
                    || hba.ip_cmp_method == ipCmpSameNet
                {
                    if !check_same_host_or_net(&port.raddr, hba.ip_cmp_method)? {
                        continue;
                    }
                } else {
                    // shouldn't get here, but deem it no-match if so
                    continue 'lines;
                }
            }

            if !check_db(&dbname, &user_name, roleid, &hba.databases)? {
                continue;
            }
            if !check_role(&user_name, roleid, &hba.roles, false)? {
                continue;
            }

            // Found a record that matched!
            return Ok(Some(hba.clone()));
        }
        Ok(None)
    })?;

    port.hba = Some(match matched {
        Some(line) => line,
        None => {
            // No matching entry, so tell the user we fell through: reject.
            let mut hba = HbaLine::new_zeroed();
            hba.auth_method = uaImplicitReject;
            hba
        }
    });
    Ok(())
}
