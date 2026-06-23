//! The connection-matching half of `hba.c`: role/db membership checks
//! (`is_member`, `check_role`, `check_db`) and the IP/hostname matchers
//! (`hostname_match`, `check_hostname`, `check_ip`, `check_network_callback`,
//! `check_same_host_or_net`).
//!
//! Ported from `src/backend/libpq/hba.c` (lines 924-1232). The C `ipv4eq` /
//! `ipv6eq` byte comparisons reduce to comparing the family + the in_addr /
//! in6_addr bytes (via [`std::net::IpAddr`] for the `backend-libpq-ifaddr`
//! arithmetic); the mask/range helpers (`pg_range_sockaddr` /
//! `pg_sockaddr_cidr_mask` / `pg_foreach_ifaddr`) are reused from the ported
//! `backend-libpq-ifaddr` crate.

use std::net::IpAddr;

use ::ifaddr::{self as ifaddr, AddressFamily};
use acl_seams as acl;
use user_seams as user;
use walsender_seams as walsender;
use ::types_core::Oid;
use ::types_error::PgResult;
use ::net::{ipCmpSameHost, AuthToken, IPCompareMethod, Port, SockAddr};

use crate::{
    pg_strcasecmp, report_plain, tok_str, token_has_regexp, token_is_keyword,
    token_is_member_check, token_matches, token_matches_insensitive, CheckNetworkData, LOG,
    REG_OKAY,
};

/// `OidIsValid(objectId)` (c.h).
#[inline]
fn oid_is_valid(object_id: Oid) -> bool {
    object_id != 0
}

/// `sa.addr.ss_family` (`sockaddr_storage`).
pub(crate) fn ss_family(sa: &SockAddr) -> i32 {
    if sa.salen == 0 {
        return libc::AF_UNSPEC;
    }
    // `sa.addr` is a raw `[u8; 128]` byte buffer with no guaranteed alignment, so
    // read `ss_family` with an unaligned read rather than forming a reference to
    // a misaligned `sockaddr_storage` (UB → aborts under Rust's
    // misaligned-pointer-dereference check). Using `addr_of!` of the named field
    // honors the platform-correct offset (on macOS `ss_family` sits after the
    // `ss_len` byte, not at offset 0).
    // SAFETY: `addr` is a real `sockaddr_storage` byte buffer.
    unsafe {
        let p = sa.addr.as_ptr() as *const libc::sockaddr_storage;
        core::ptr::addr_of!((*p).ss_family).read_unaligned() as i32
    }
}

/// `&SockAddr` -> [`IpAddr`], reading the in_addr / in6_addr bytes for the
/// AF_INET / AF_INET6 families (`None` for AF_UNIX / AF_UNSPEC).
pub(crate) fn sockaddr_to_ipaddr(sa: &SockAddr) -> Option<IpAddr> {
    match ss_family(sa) {
        f if f == libc::AF_INET => {
            // Copy the unaligned `sockaddr_in` bytes into an aligned local (what
            // C effectively has, since its structs are aligned) before reading
            // `sin_addr` — never form a `&` to the misaligned byte buffer.
            // SAFETY: family is AF_INET, so the buffer holds a sockaddr_in.
            let sin: libc::sockaddr_in = unsafe {
                let mut tmp = core::mem::MaybeUninit::<libc::sockaddr_in>::zeroed();
                core::ptr::copy_nonoverlapping(
                    sa.addr.as_ptr(),
                    tmp.as_mut_ptr().cast::<u8>(),
                    core::mem::size_of::<libc::sockaddr_in>(),
                );
                tmp.assume_init()
            };
            let raw = u32::from_be(sin.sin_addr.s_addr);
            Some(IpAddr::V4(std::net::Ipv4Addr::from(raw)))
        }
        f if f == libc::AF_INET6 => {
            // SAFETY: family is AF_INET6, so the buffer holds a sockaddr_in6.
            let sin6: libc::sockaddr_in6 = unsafe {
                let mut tmp = core::mem::MaybeUninit::<libc::sockaddr_in6>::zeroed();
                core::ptr::copy_nonoverlapping(
                    sa.addr.as_ptr(),
                    tmp.as_mut_ptr().cast::<u8>(),
                    core::mem::size_of::<libc::sockaddr_in6>(),
                );
                tmp.assume_init()
            };
            Some(IpAddr::V6(std::net::Ipv6Addr::from(sin6.sin6_addr.s6_addr)))
        }
        _ => None,
    }
}

/// Build a `SockAddr` (`sockaddr_in` / `sockaddr_in6` byte buffer + salen) from
/// an [`IpAddr`] — the inverse of [`sockaddr_to_ipaddr`], used to store a
/// computed CIDR mask back into `HbaLine.mask`.
pub(crate) fn ipaddr_to_sockaddr(ip: &IpAddr) -> SockAddr {
    let mut sa = SockAddr::zeroed();
    match ip {
        IpAddr::V4(v4) => {
            // Fill an aligned local `sockaddr_in`, then copy its bytes into the
            // unaligned storage buffer — never write through a misaligned `&mut`.
            // SAFETY: zeroed sockaddr_in is a valid all-fields-init value.
            let mut sin: libc::sockaddr_in =
                unsafe { core::mem::MaybeUninit::zeroed().assume_init() };
            sin.sin_family = libc::AF_INET as libc::sa_family_t;
            sin.sin_addr.s_addr = u32::from(*v4).to_be();
            // SAFETY: copying size_of::<sockaddr_in>() bytes into the 128-byte buf.
            unsafe {
                core::ptr::copy_nonoverlapping(
                    (&sin as *const libc::sockaddr_in).cast::<u8>(),
                    sa.addr.as_mut_ptr(),
                    core::mem::size_of::<libc::sockaddr_in>(),
                );
            }
            sa.salen = core::mem::size_of::<libc::sockaddr_in>() as u32;
        }
        IpAddr::V6(v6) => {
            // SAFETY: zeroed sockaddr_in6 is a valid all-fields-init value.
            let mut sin6: libc::sockaddr_in6 =
                unsafe { core::mem::MaybeUninit::zeroed().assume_init() };
            sin6.sin6_family = libc::AF_INET6 as libc::sa_family_t;
            sin6.sin6_addr.s6_addr = v6.octets();
            // SAFETY: copying size_of::<sockaddr_in6>() bytes into the 128-byte buf.
            unsafe {
                core::ptr::copy_nonoverlapping(
                    (&sin6 as *const libc::sockaddr_in6).cast::<u8>(),
                    sa.addr.as_mut_ptr(),
                    core::mem::size_of::<libc::sockaddr_in6>(),
                );
            }
            sa.salen = core::mem::size_of::<libc::sockaddr_in6>() as u32;
        }
    }
    sa
}

/// Family of an [`IpAddr`] for the `backend-libpq-ifaddr` API.
#[inline]
fn family_of(ip: &IpAddr) -> AddressFamily {
    match ip {
        IpAddr::V4(_) => AddressFamily::Inet,
        IpAddr::V6(_) => AddressFamily::Inet6,
    }
}

/// `static bool is_member(Oid userid, const char *role)` (hba.c:924). Is
/// `userid` (directly or indirectly, non-super) a member of `role`?
pub(crate) fn is_member(userid: Oid, role: &[u8]) -> PgResult<bool> {
    // if (!OidIsValid(userid)) return false;
    if !oid_is_valid(userid) {
        return Ok(false); // if user not exist, say "no"
    }

    // roleid = get_role_oid(role, true);
    let role_str = String::from_utf8_lossy(role);
    let roleid = acl::get_role_oid::call(&role_str, true)?;

    // if (!OidIsValid(roleid)) return false;
    if !oid_is_valid(roleid) {
        return Ok(false); // if target role not exist, say "no"
    }

    // A superuser is not considered automatically a member of the role.
    user::is_member_of_role_nosuper::call(userid, roleid)
}

/// `static bool check_role(const char *role, Oid roleid, List *tokens, bool
/// case_insensitive)` (hba.c:953). Match an [`AuthToken`] list to `role`.
pub(crate) fn check_role(
    role: &[u8],
    roleid: Oid,
    tokens: &[AuthToken],
    case_insensitive: bool,
) -> PgResult<bool> {
    for tok in tokens {
        if token_is_member_check(tok) {
            if is_member(roleid, &tok_str(tok)[1..])? {
                return Ok(true);
            }
        } else if token_is_keyword(tok, b"all") {
            return Ok(true);
        } else if token_has_regexp(tok) {
            let (rc, _m, _e) = crate::token::regexec_auth_token(role, tok, 0)?;
            if rc == REG_OKAY {
                return Ok(true);
            }
        } else if case_insensitive {
            if token_matches_insensitive(tok, role) {
                return Ok(true);
            }
        } else if token_matches(tok, role) {
            return Ok(true);
        }
    }
    Ok(false)
}

/// `static bool check_db(const char *dbname, const char *role, Oid roleid, List
/// *tokens)` (hba.c:992).
pub(crate) fn check_db(
    dbname: &[u8],
    role: &[u8],
    roleid: Oid,
    tokens: &[AuthToken],
) -> PgResult<bool> {
    let am_walsender = walsender::am_walsender::call();
    let am_db_walsender = walsender::am_db_walsender::call();

    for tok in tokens {
        if am_walsender && !am_db_walsender {
            // physical replication walsender connections can only match the
            // replication keyword
            if token_is_keyword(tok, b"replication") {
                return Ok(true);
            }
        } else if token_is_keyword(tok, b"all") {
            return Ok(true);
        } else if token_is_keyword(tok, b"sameuser") {
            if dbname == role {
                return Ok(true);
            }
        } else if token_is_keyword(tok, b"samegroup") || token_is_keyword(tok, b"samerole") {
            if is_member(roleid, dbname)? {
                return Ok(true);
            }
        } else if token_is_keyword(tok, b"replication") {
            continue; // never match this if not walsender
        } else if token_has_regexp(tok) {
            let (rc, _m, _e) = crate::token::regexec_auth_token(dbname, tok, 0)?;
            if rc == REG_OKAY {
                return Ok(true);
            }
        } else if token_matches(tok, dbname) {
            return Ok(true);
        }
    }
    Ok(false)
}

/// `static bool hostname_match(const char *pattern, const char
/// *actual_hostname)` (hba.c:1057).
pub(crate) fn hostname_match(pattern: &[u8], actual_hostname: &[u8]) -> bool {
    if pattern.first() == Some(&b'.') {
        // suffix match
        let plen = pattern.len();
        let hlen = actual_hostname.len();
        if hlen < plen {
            return false;
        }
        pg_strcasecmp(pattern, &actual_hostname[(hlen - plen)..]) == 0
    } else {
        pg_strcasecmp(pattern, actual_hostname) == 0
    }
}

/// `static bool check_hostname(hbaPort *port, const char *hostname)`
/// (hba.c:1077). Reverse-then-forward DNS match of the connecting IP against
/// `hostname`, caching the result in `port`.
pub(crate) fn check_hostname(port: &mut Port, hostname: &[u8]) -> PgResult<bool> {
    // Quick out if remote host name already known bad.
    if port.remote_hostname_resolv < 0 {
        return Ok(false);
    }

    // Lookup remote host name if not already done.
    if port.remote_hostname.is_none() {
        // ret = pg_getnameinfo_all(&raddr.addr, salen, buf, ..., NULL, 0, NI_NAMEREQD);
        let mut node = String::new();
        let ret = ip::pg_getnameinfo_all(
            &port.raddr,
            Some(&mut node),
            None,
            libc::NI_NAMEREQD,
        );
        if ret != 0 {
            // remember failure; don't complain in the postmaster log yet
            port.remote_hostname_resolv = -2;
            port.remote_hostname_errcode = ret;
            return Ok(false);
        }
        port.remote_hostname = Some(node);
    }

    // Now see if remote host name matches this pg_hba line.
    let remote_hostname = port
        .remote_hostname
        .as_deref()
        .expect("check_hostname: remote_hostname set above");
    if !hostname_match(hostname, remote_hostname.as_bytes()) {
        return Ok(false);
    }

    // If we already verified the forward lookup, we're done.
    if port.remote_hostname_resolv == 1 {
        return Ok(true);
    }

    // Lookup IP from host name and check against original IP.
    // ret = getaddrinfo(port->remote_hostname, NULL, NULL, &gai_result);
    let mut gai_result: Vec<::net::PgAddrInfo> = Vec::new();
    let hint = ::net::AddrInfoHint::default(); // hints == NULL in C
    let ret = ip::pg_getaddrinfo_all(
        Some(remote_hostname),
        None,
        &hint,
        &mut gai_result,
    );
    if ret != 0 {
        port.remote_hostname_resolv = -2;
        port.remote_hostname_errcode = ret;
        return Ok(false);
    }

    let client_ip = sockaddr_to_ipaddr(&port.raddr);
    let mut found = false;
    for gai in &gai_result {
        // Compare same-family addresses (C `ipv4eq` / `ipv6eq` after the
        // ai_family == raddr family check).
        if sockaddr_to_ipaddr(&gai.addr) == client_ip && client_ip.is_some() {
            found = true;
            break;
        }
    }

    if !found {
        // elog(DEBUG2, "pg_hba.conf host name \"%s\" rejected because address
        //   resolution did not return a match with IP address of client", hostname)
        let h = String::from_utf8_lossy(hostname);
        report_plain(
            crate::DEBUG2,
            "check_hostname",
            ::types_error::ERRCODE_INTERNAL_ERROR,
            format!(
                "pg_hba.conf host name \"{h}\" rejected because address resolution did not return a match with IP address of client"
            ),
        )?;
    }

    port.remote_hostname_resolv = if found { 1 } else { -1 };
    Ok(found)
}

/// `static bool check_ip(SockAddr *raddr, struct sockaddr *addr, struct sockaddr
/// *mask)` (hba.c:1168). Does `raddr` fall within `addr`/`mask`?
pub(crate) fn check_ip(raddr: &SockAddr, addr: &SockAddr, mask: &SockAddr) -> bool {
    // if (raddr->addr.ss_family == addr->sa_family && pg_range_sockaddr(...))
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

/// `static void check_network_callback(struct sockaddr *addr, struct sockaddr
/// *netmask, void *cb_data)` (hba.c:1182). The `pg_foreach_ifaddr` callback
/// that tests one interface against the client address.
pub(crate) fn check_network_callback(
    raddr: &SockAddr,
    addr: &IpAddr,
    netmask: &IpAddr,
    cn: &mut CheckNetworkData,
) {
    // Already found a match?
    if cn.result {
        return;
    }

    if cn.method == ipCmpSameHost {
        // Make an all-ones netmask of appropriate length for family.
        // pg_sockaddr_cidr_mask(&mask, NULL, addr->sa_family);
        match ifaddr::pg_sockaddr_cidr_mask(None, family_of(addr)) {
            Ok(mask) => {
                let addr_sa = ipaddr_to_sockaddr(addr);
                let mask_sa = ipaddr_to_sockaddr(&mask);
                cn.result = check_ip(raddr, &addr_sa, &mask_sa);
            }
            // C ignores pg_sockaddr_cidr_mask's return here; a failure leaves
            // the result unchanged.
            Err(_) => {}
        }
    } else {
        // Use the netmask of the interface itself.
        let addr_sa = ipaddr_to_sockaddr(addr);
        let mask_sa = ipaddr_to_sockaddr(netmask);
        cn.result = check_ip(raddr, &addr_sa, &mask_sa);
    }
}

/// `static bool check_same_host_or_net(SockAddr *raddr, IPCompareMethod method)`
/// (hba.c:1209). Use `pg_foreach_ifaddr` to test a `samehost` / `samenet` match.
pub(crate) fn check_same_host_or_net(
    raddr: &SockAddr,
    method: IPCompareMethod,
) -> PgResult<bool> {
    let mut cn = CheckNetworkData { method, result: false };

    // errno = 0; if (pg_foreach_ifaddr(check_network_callback, &cn) < 0) ...
    let res = ifaddr::pg_foreach_ifaddr(|addr, netmask| {
        check_network_callback(raddr, &addr, &netmask, &mut cn);
    });

    if res.is_err() {
        // ereport(LOG, (errmsg("error enumerating network interfaces: %m")));
        report_plain(
            LOG,
            "check_same_host_or_net",
            ::types_error::ERRCODE_INTERNAL_ERROR,
            "error enumerating network interfaces".to_string(),
        )?;
        return Ok(false);
    }

    Ok(cn.result)
}
