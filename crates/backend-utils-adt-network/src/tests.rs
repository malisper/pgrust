//! Parity tests for the `network.c` core, cross-checked against PostgreSQL 18.3
//! `src/test/regress/expected/inet.out`.

use super::*;

fn inet(src: &str) -> inet_struct {
    inet_in(src.as_bytes(), None).unwrap().unwrap()
}

fn cidr(src: &str) -> inet_struct {
    cidr_in(src.as_bytes(), None).unwrap().unwrap()
}

fn out_inet(v: &inet_struct) -> String {
    String::from_utf8(inet_out(v).unwrap()).unwrap()
}

fn out_cidr(v: &inet_struct) -> String {
    String::from_utf8(cidr_out(v).unwrap()).unwrap()
}

#[test]
fn inet_in_out_roundtrip_ipv4() {
    assert_eq!(out_inet(&inet("192.168.1.226/24")), "192.168.1.226/24");
    // default /32 is suppressed on output
    assert_eq!(out_inet(&inet("192.168.1.226")), "192.168.1.226");
    assert_eq!(out_inet(&inet("10.1.2.3/8")), "10.1.2.3/8");
}

#[test]
fn cidr_in_out_roundtrip() {
    assert_eq!(out_cidr(&cidr("192.168.1")), "192.168.1.0/24");
    assert_eq!(out_cidr(&cidr("10")), "10.0.0.0/8");
    assert_eq!(out_cidr(&cidr("10.1.2.3")), "10.1.2.3/32");
}

#[test]
fn ipv6_in_out() {
    assert_eq!(out_inet(&inet("10:23::f1/64")), "10:23::f1/64");
    assert_eq!(out_inet(&inet("::ffff:1.2.3.4")), "::ffff:1.2.3.4");
}

#[test]
fn cidr_rejects_bits_right_of_mask() {
    // 192.168.1.1/24 has host bits set -> not a valid cidr.
    let mut esc = SoftErrorContext::new(true);
    let r = cidr_in(b"192.168.1.1/24", Some(&mut esc)).unwrap();
    assert!(r.is_none());
    assert!(esc.error_occurred());
}

#[test]
fn family_and_masklen() {
    assert_eq!(network_family(&inet("192.168.1.1")), 4);
    assert_eq!(network_family(&inet("::1")), 6);
    assert_eq!(network_masklen(&inet("192.168.1.0/24")), 24);
}

#[test]
fn host_network_broadcast_netmask_hostmask() {
    let v = inet("192.168.1.226/24");
    assert_eq!(String::from_utf8(network_host(&v).unwrap()).unwrap(), "192.168.1.226");
    assert_eq!(out_inet(&network_network(&v)), "192.168.1.0/24");
    assert_eq!(out_inet(&network_broadcast(&v)), "192.168.1.255/24");
    assert_eq!(out_inet(&network_netmask(&v)), "255.255.255.0");
    assert_eq!(out_inet(&network_hostmask(&v)), "0.0.0.255");
}

#[test]
fn comparison_and_containment() {
    let a = inet("192.168.1.5/24");
    let b = inet("192.168.1.0/24");
    assert!(network_gt(&a, &b));
    assert!(network_lt(&b, &a));

    // 192.168.1.5 is a sub of the 192.168.1/24 network.
    let host = inet("192.168.1.5/32");
    let net = cidr("192.168.1.0/24");
    assert!(network_sub(&host, &net));
    assert!(network_subeq(&host, &net));
    assert!(network_sup(&net, &host));
    assert!(!network_sub(&net, &host));
    assert!(network_overlap(&host, &net));
}

#[test]
fn set_masklen_and_to_cidr() {
    let v = inet("192.168.1.226/24");
    assert_eq!(out_inet(&inet_set_masklen(&v, 16).unwrap()), "192.168.1.226/16");
    assert_eq!(out_cidr(&inet_to_cidr(&v).unwrap()), "192.168.1.0/24");
    assert!(inet_set_masklen(&v, 99).is_err());
}

#[test]
fn merge_and_same_family() {
    let a = inet("192.168.1.5/24");
    let b = inet("192.168.2.5/24");
    assert!(inet_same_family(&a, &b));
    // smallest common cidr of the two /24s
    assert_eq!(out_cidr(&inet_merge(&a, &b).unwrap()), "192.168.0.0/22");
    let v6 = inet("::1");
    assert!(inet_merge(&a, &v6).is_err());
}

#[test]
fn bitwise_not_and_or() {
    let a = inet("192.168.1.0/24");
    // ~ flips all octets
    assert_eq!(out_inet(&inetnot(&a)), "63.87.254.255/24");
    let b = inet("0.0.0.255/24");
    assert_eq!(out_inet(&inetand(&a, &b).unwrap()), "0.0.0.0/24");
    assert_eq!(out_inet(&inetor(&a, &b).unwrap()), "192.168.1.255/24");
    let v6 = inet("::1");
    assert!(inetand(&a, &v6).is_err());
}

#[test]
fn inetpl_inetmi() {
    let a = inet("10.0.0.0/8");
    assert_eq!(out_inet(&inetpl(&a, 256).unwrap()), "10.0.1.0/8");
    assert_eq!(out_inet(&inetmi_int8(&a, 256).unwrap()), "9.255.255.0/8");
    let b = inet("10.0.1.0/8");
    assert_eq!(inetmi(&b, &a).unwrap(), 256);
    // different families -> error
    let v6 = inet("::1");
    assert!(inetmi(&a, &v6).is_err());
}

#[test]
fn recv_send_roundtrip() {
    let ctx = mcx::MemoryContext::new("network-test");
    let v = inet("192.168.1.226/24");
    let wire = inet_send(ctx.mcx(), &v).unwrap();
    // family, bits, is_cidr, nb, then 4 octets.
    assert_eq!(wire, vec![PGSQL_AF_INET, 24, 0, 4, 192, 168, 1, 226]);
    let back = inet_recv(&wire).unwrap();
    assert_eq!(back, v);
}

#[test]
fn recv_rejects_bad_family() {
    let bad = vec![99u8, 24, 0, 4, 192, 168, 1, 226];
    assert!(inet_recv(&bad).is_err());
}

#[test]
fn hashinet_bytes() {
    let ctx = mcx::MemoryContext::new("network-test");
    let v = inet("192.168.1.226/24");
    // family, bits, then 4 address octets (addrsize + 2).
    let h = hashinet(ctx.mcx(), &v).unwrap();
    assert_eq!(h, vec![PGSQL_AF_INET, 24, 192, 168, 1, 226]);
}

#[test]
fn convert_scalar_ordering() {
    // scalar ordering tracks address ordering for selectivity.
    let lo = convert_network_to_scalar(&inet("10.0.0.0/8"));
    let hi = convert_network_to_scalar(&inet("11.0.0.0/8"));
    assert!(lo < hi);
}

#[test]
fn abbrev_accessors() {
    let v = inet("10:23::f1/64");
    assert_eq!(String::from_utf8(inet_abbrev(&v).unwrap()).unwrap(), "10:23::f1/64");
    let c = cidr("10:23::/64");
    assert_eq!(String::from_utf8(cidr_abbrev(&c).unwrap()).unwrap(), "10:23::/64");
}

#[test]
fn clean_ipv6_addr_strips_zone() {
    let mut addr = b"fe80::1%eth0".to_vec();
    clean_ipv6_addr(super::system_af::AF_INET6, &mut addr);
    assert_eq!(addr, b"fe80::1");
    // wrong family: no strip
    let mut addr2 = b"fe80::1%eth0".to_vec();
    clean_ipv6_addr(0, &mut addr2);
    assert_eq!(addr2, b"fe80::1%eth0");
}

#[test]
fn bitncmp_bitncommon() {
    let a = [192, 168, 1, 0];
    let b = [192, 168, 2, 0];
    assert!(bitncmp(&a, &b, 32) < 0);
    assert_eq!(bitncmp(&a, &b, 16), 0);
    assert_eq!(bitncommon(&a, &b, 32), 22);
}
