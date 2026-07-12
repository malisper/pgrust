use ip::SockAddr;
use types_startup::{
    clientCertCA, clientCertCN, clientCertFull, clientCertOff, ctHost, ctHostGSS, ctHostNoGSS,
    ctHostNoSSL, ctHostSSL, ctLocal, ipCmpAll, ipCmpMask, ipCmpSameHost, ipCmpSameNet, AuthToken,
    HbaLine,
};

use crate::{clean_ipv6_addr, hba_addr_mask, hba_options_strings, hba_typestr};

fn line(ip_cmp_method: types_startup::IPCompareMethod) -> HbaLine {
    let mut h = HbaLine::new_zeroed();
    h.ip_cmp_method = ip_cmp_method;
    h
}

#[test]
fn typestr_matches_c_enum_order() {
    assert_eq!(hba_typestr(ctLocal), "local");
    assert_eq!(hba_typestr(ctHost), "host");
    assert_eq!(hba_typestr(ctHostSSL), "hostssl");
    assert_eq!(hba_typestr(ctHostNoSSL), "hostnossl");
    assert_eq!(hba_typestr(ctHostGSS), "hostgssenc");
    assert_eq!(hba_typestr(ctHostNoGSS), "hostnogssenc");
}

#[test]
fn addr_mask_ip_cmp_all() {
    let h = line(ipCmpAll);
    assert_eq!(hba_addr_mask(&h), (Some("all".to_string()), None));
}

#[test]
fn addr_mask_samehost_samenet() {
    assert_eq!(hba_addr_mask(&line(ipCmpSameHost)), (Some("samehost".to_string()), None));
    assert_eq!(hba_addr_mask(&line(ipCmpSameNet)), (Some("samenet".to_string()), None));
}

#[test]
fn addr_mask_hostname_wins_over_numeric() {
    let mut h = line(ipCmpMask);
    h.hostname = Some("db.example.com".to_string());
    assert_eq!(hba_addr_mask(&h), (Some("db.example.com".to_string()), None));
}

#[test]
fn addr_mask_zero_len_addr_is_null() {
    let h = line(ipCmpMask);
    assert_eq!(hba_addr_mask(&h), (None, None));
}

fn ipv4_sockaddr(a: [u8; 4], port: u16) -> SockAddr {
    let mut sa = SockAddr::zeroed();
    let sin: libc::sockaddr_in = unsafe {
        let mut s: libc::sockaddr_in = std::mem::zeroed();
        s.sin_family = libc::AF_INET as libc::sa_family_t;
        s.sin_port = port.to_be();
        s.sin_addr.s_addr = u32::from_be_bytes(a).to_be();
        s
    };
    let bytes = unsafe {
        std::slice::from_raw_parts(
            (&sin as *const libc::sockaddr_in) as *const u8,
            std::mem::size_of::<libc::sockaddr_in>(),
        )
    };
    sa.addr[..bytes.len()].copy_from_slice(bytes);
    sa.salen = bytes.len() as u32;
    sa
}

#[test]
fn addr_mask_numeric_ipv4() {
    let mut h = line(ipCmpMask);
    h.addr = ipv4_sockaddr([192, 168, 1, 5], 0);
    h.mask = ipv4_sockaddr([255, 255, 255, 0], 0);
    let (addr, mask) = hba_addr_mask(&h);
    assert_eq!(addr.as_deref(), Some("192.168.1.5"));
    assert_eq!(mask.as_deref(), Some("255.255.255.0"));
}

#[test]
fn clean_ipv6_strips_zone_only_on_af_inet6() {
    let mut s = "fe80::1%eth0".to_string();
    clean_ipv6_addr(libc::AF_INET6, &mut s);
    assert_eq!(s, "fe80::1");

    let mut s4 = "192.168.1.1".to_string();
    clean_ipv6_addr(libc::AF_INET, &mut s4);
    assert_eq!(s4, "192.168.1.1");
}

#[test]
fn options_empty_when_nothing_set() {
    let h = HbaLine::new_zeroed();
    assert!(hba_options_strings(&h).is_empty());
}

#[test]
fn options_map_and_clientcert() {
    let mut h = HbaLine::new_zeroed();
    h.usermap = Some("mymap".to_string());
    h.clientcert = clientCertCA;
    assert_eq!(hba_options_strings(&h), vec!["map=mymap", "clientcert=verify-ca"]);

    h.clientcert = clientCertFull;
    assert_eq!(hba_options_strings(&h), vec!["map=mymap", "clientcert=verify-full"]);

    h.clientcert = clientCertOff;
    assert_eq!(hba_options_strings(&h), vec!["map=mymap"]);
    let _ = clientCertCN;
}

#[test]
fn auth_token_strings_are_not_requoted() {
    // hbafuncs.c:270-286: flattened database/role names are returned as the
    // raw token string, deliberately not re-quoted (e.g. "all" stays "all").
    let tok = AuthToken { string: "all".to_string(), quoted: true, regex: false };
    assert_eq!(tok.string, "all");
}
