//! network.c: the inet/cidr varlena types over the ISC inet_net_pton/ntop +
//! inet_cidr_ntop kernels (pton.rs/ntop.rs/cidr_ntop.rs).
//! Deferred loud (unregistered OIDs resolve to fmgr's not-ported panic):
//! network_sortsupport 5033 (abbrev keys), network_subset_support 1173
//! (planner support node), inet_client/server_* 2196-2199 (MyProcPort),
//! GiST/SP-GiST/selfuncs rows (their own catalog units).

pub mod abbrev;
pub mod builtins;
mod cidr_ntop;
mod ntop;
mod pton;
#[cfg(test)]
mod tests;

use datum::Bytea;
use mcx::Mcx;
use stringinfo::StringInfo;
use types_error::{
    ereturn, PgError, PgResult, SoftErrorContext, ERRCODE_INVALID_BINARY_REPRESENTATION,
    ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_INVALID_TEXT_REPRESENTATION,
    ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
};

pub const PGSQL_AF_INET: u8 = 2;
pub const PGSQL_AF_INET6: u8 = 3;

// sizeof("xxxx:xxxx:xxxx:xxxx:xxxx:xxxx:255.255.255.255/128")
pub const INET_OUT_BUFLEN: usize = 50;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InetValue {
    pub family: u8,
    pub bits: u8,
    pub ipaddr: [u8; 16],
}

impl InetValue {
    #[inline]
    pub fn addrsize(&self) -> usize {
        if self.family == PGSQL_AF_INET {
            4
        } else {
            16
        }
    }

    #[inline]
    pub fn maxbits(&self) -> u8 {
        if self.family == PGSQL_AF_INET {
            32
        } else {
            128
        }
    }

    // SET_INET_VARSIZE image: 4B header + family + bits + addrsize addr bytes.
    pub fn image(&self) -> ([u8; 22], usize) {
        let len = 4 + 2 + self.addrsize();
        let mut img = [0u8; 22];
        img[..4].copy_from_slice(&datum::varlena::set_varsize_4b(len));
        img[4] = self.family;
        img[5] = self.bits;
        img[6..6 + self.addrsize()].copy_from_slice(&self.ipaddr[..self.addrsize()]);
        (img, len)
    }

    #[inline]
    pub fn iref(&self) -> InetRef<'_> {
        InetRef {
            family: self.family,
            bits: self.bits,
            addr: &self.ipaddr,
        }
    }
}

/// C's `inet *` through ip_family/ip_bits/ip_addr over VARDATA_ANY: fields
/// read in place off the borrowed arg payload, no copy on comparison paths.
#[derive(Clone, Copy, Debug)]
pub struct InetRef<'a> {
    pub family: u8,
    pub bits: u8,
    pub addr: &'a [u8],
}

impl<'a> InetRef<'a> {
    pub fn from_payload(p: &'a [u8]) -> InetRef<'a> {
        InetRef {
            family: p[0],
            bits: p[1],
            addr: &p[2..],
        }
    }

    #[inline]
    pub fn addrsize(&self) -> usize {
        if self.family == PGSQL_AF_INET {
            4
        } else {
            16
        }
    }

    #[inline]
    pub fn maxbits(&self) -> u8 {
        if self.family == PGSQL_AF_INET {
            32
        } else {
            128
        }
    }

    pub fn to_value(&self) -> InetValue {
        let mut ipaddr = [0u8; 16];
        let n = self.addrsize().min(self.addr.len());
        ipaddr[..n].copy_from_slice(&self.addr[..n]);
        InetValue {
            family: self.family,
            bits: self.bits,
            ipaddr,
        }
    }
}

#[cold]
#[inline(never)]
fn invalid_input_err(is_cidr: bool, src: &str) -> PgError {
    PgError::error(format!(
        "invalid input syntax for type {}: \"{src}\"",
        if is_cidr { "cidr" } else { "inet" }
    ))
    .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION)
}

#[cold]
#[inline(never)]
fn invalid_cidr_err(src: &str) -> PgError {
    PgError::error(format!("invalid cidr value: \"{src}\""))
        .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION)
        .with_detail("Value has bits set to right of mask.")
}

#[cold]
#[inline(never)]
fn could_not_format_err() -> PgError {
    // C appends %m; unreachable off well-formed stored values (divergence).
    PgError::error("could not format inet value")
        .with_sqlstate(ERRCODE_INVALID_BINARY_REPRESENTATION)
}

#[cold]
#[inline(never)]
fn mask_length_err(bits: i32) -> PgError {
    PgError::error(format!("invalid mask length: {bits}"))
        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
}

#[cold]
#[inline(never)]
fn family_mismatch_err(what: &str) -> PgError {
    PgError::error(format!("cannot {what} inet values of different sizes"))
        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
}

#[cold]
#[inline(never)]
fn out_of_range_err() -> PgError {
    PgError::error("result is out of range").with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
}

#[cold]
#[inline(never)]
fn recv_err(msg: String) -> PgError {
    PgError::error(msg).with_sqlstate(ERRCODE_INVALID_BINARY_REPRESENTATION)
}

pub fn network_in(
    src: &str,
    is_cidr: bool,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<InetValue>> {
    let family = if src.contains(':') {
        PGSQL_AF_INET6
    } else {
        PGSQL_AF_INET
    };
    let mut dst = InetValue {
        family,
        bits: 0,
        ipaddr: [0u8; 16],
    };
    let size: isize = if is_cidr { dst.addrsize() as isize } else { -1 };
    let maxbits = dst.maxbits() as i32;
    let bits = match pton::pg_inet_net_pton(family as i32, src.as_bytes(), &mut dst.ipaddr, size) {
        Some(b) if b <= maxbits => b,
        _ => return ereturn(escontext, None, invalid_input_err(is_cidr, src)),
    };

    if is_cidr && !address_ok(&dst.ipaddr, bits, family) {
        return ereturn(escontext, None, invalid_cidr_err(src));
    }

    dst.bits = bits as u8;
    Ok(Some(dst))
}

pub fn network_out_into(src: InetRef<'_>, is_cidr: bool, buf: &mut [u8]) -> PgResult<usize> {
    let mut len = ntop::pg_inet_net_ntop(src.family as i32, src.addr, src.bits as i32, buf)
        .ok_or_else(|| Box::new(could_not_format_err()))?;
    if is_cidr && !buf[..len].contains(&b'/') {
        buf[len] = b'/';
        len += 1;
        len += write_decimal(buf, len, src.bits as u32);
    }
    Ok(len)
}

fn write_decimal(dst: &mut [u8], at: usize, v: u32) -> usize {
    let mut tmp = [0u8; 10];
    let mut n = 0;
    let mut x = v;
    loop {
        tmp[n] = b'0' + (x % 10) as u8;
        x /= 10;
        n += 1;
        if x == 0 {
            break;
        }
    }
    for k in 0..n {
        dst[at + k] = tmp[n - 1 - k];
    }
    n
}

pub fn network_recv(buf: &mut StringInfo<'_>, is_cidr: bool) -> PgResult<InetValue> {
    let what = if is_cidr { "cidr" } else { "inet" };
    let family = pqformat::pq_getmsgbyte(buf)? as u8;
    if family != PGSQL_AF_INET && family != PGSQL_AF_INET6 {
        return Err(Box::new(recv_err(format!(
            "invalid address family in external \"{what}\" value"
        ))));
    }
    let mut addr = InetValue {
        family,
        bits: 0,
        ipaddr: [0u8; 16],
    };
    let bits = pqformat::pq_getmsgbyte(buf)?;
    if bits < 0 || bits > addr.maxbits() as i32 {
        return Err(Box::new(recv_err(format!(
            "invalid bits in external \"{what}\" value"
        ))));
    }
    addr.bits = bits as u8;
    let _is_cidr_byte = pqformat::pq_getmsgbyte(buf)?;
    let nb = pqformat::pq_getmsgbyte(buf)?;
    if nb != addr.addrsize() as i32 {
        return Err(Box::new(recv_err(format!(
            "invalid length in external \"{what}\" value"
        ))));
    }
    for i in 0..nb as usize {
        addr.ipaddr[i] = pqformat::pq_getmsgbyte(buf)? as u8;
    }

    if is_cidr && !address_ok(&addr.ipaddr, bits, family) {
        return Err(Box::new(
            recv_err("invalid external \"cidr\" value".into())
                .with_detail("Value has bits set to right of mask."),
        ));
    }
    Ok(addr)
}

pub fn network_send<'mcx>(
    mcx: Mcx<'mcx>,
    addr: InetRef<'_>,
    is_cidr: bool,
) -> PgResult<Bytea<'mcx>> {
    let mut buf = pqformat::pq_begintypsend(mcx)?;
    pqformat::pq_sendbyte(&mut buf, addr.family)?;
    pqformat::pq_sendbyte(&mut buf, addr.bits)?;
    pqformat::pq_sendbyte(&mut buf, is_cidr as u8)?;
    let nb = addr.addrsize();
    pqformat::pq_sendbyte(&mut buf, nb as u8)?;
    for i in 0..nb {
        pqformat::pq_sendbyte(&mut buf, addr.addr[i])?;
    }
    Ok(pqformat::pq_endtypsend(buf))
}

pub fn inet_to_cidr(src: InetRef<'_>) -> PgResult<InetValue> {
    let bits = src.bits as i32;
    if bits > src.maxbits() as i32 {
        return Err(Box::new(PgError::error(format!(
            "invalid inet bit length: {bits}"
        ))));
    }
    Ok(cidr_set_masklen_internal(src, bits))
}

pub fn inet_set_masklen(src: InetRef<'_>, bits: i32) -> PgResult<InetValue> {
    let bits = if bits == -1 { src.maxbits() as i32 } else { bits };
    if bits < 0 || bits > src.maxbits() as i32 {
        return Err(Box::new(mask_length_err(bits)));
    }
    let mut dst = src.to_value();
    dst.bits = bits as u8;
    Ok(dst)
}

pub fn cidr_set_masklen(src: InetRef<'_>, bits: i32) -> PgResult<InetValue> {
    let bits = if bits == -1 { src.maxbits() as i32 } else { bits };
    if bits < 0 || bits > src.maxbits() as i32 {
        return Err(Box::new(mask_length_err(bits)));
    }
    Ok(cidr_set_masklen_internal(src, bits))
}

pub fn cidr_set_masklen_internal(src: InetRef<'_>, bits: i32) -> InetValue {
    let mut dst = InetValue {
        family: src.family,
        bits: bits as u8,
        ipaddr: [0u8; 16],
    };
    if bits > 0 {
        let nbytes = ((bits + 7) / 8) as usize;
        dst.ipaddr[..nbytes].copy_from_slice(&src.addr[..nbytes]);
        if bits % 8 != 0 {
            dst.ipaddr[(bits / 8) as usize] &= !(0xFFu8 >> (bits % 8));
        }
    }
    dst
}

// convert_network_to_scalar (network.c), inet/cidr arm; IPv6 uses only the
// first 5 address bytes.
pub fn convert_network_to_scalar(ip: InetRef<'_>) -> f64 {
    let len = if ip.family == PGSQL_AF_INET { 4 } else { 5 };
    let mut res = ip.family as f64;
    for i in 0..len {
        res *= 256.0;
        res += ip.addr[i] as f64;
    }
    res
}

pub fn network_cmp_internal(a1: InetRef<'_>, a2: InetRef<'_>) -> i32 {
    if a1.family == a2.family {
        let order = bitncmp(a1.addr, a2.addr, a1.bits.min(a2.bits) as i32);
        if order != 0 {
            return order;
        }
        let order = a1.bits as i32 - a2.bits as i32;
        if order != 0 {
            return order;
        }
        return bitncmp(a1.addr, a2.addr, a1.maxbits() as i32);
    }
    a1.family as i32 - a2.family as i32
}

pub fn network_sub(a1: InetRef<'_>, a2: InetRef<'_>) -> bool {
    a1.family == a2.family && a1.bits > a2.bits && bitncmp(a1.addr, a2.addr, a2.bits as i32) == 0
}

pub fn network_subeq(a1: InetRef<'_>, a2: InetRef<'_>) -> bool {
    a1.family == a2.family && a1.bits >= a2.bits && bitncmp(a1.addr, a2.addr, a2.bits as i32) == 0
}

pub fn network_sup(a1: InetRef<'_>, a2: InetRef<'_>) -> bool {
    a1.family == a2.family && a1.bits < a2.bits && bitncmp(a1.addr, a2.addr, a1.bits as i32) == 0
}

pub fn network_supeq(a1: InetRef<'_>, a2: InetRef<'_>) -> bool {
    a1.family == a2.family && a1.bits <= a2.bits && bitncmp(a1.addr, a2.addr, a1.bits as i32) == 0
}

pub fn network_overlap(a1: InetRef<'_>, a2: InetRef<'_>) -> bool {
    a1.family == a2.family && bitncmp(a1.addr, a2.addr, a1.bits.min(a2.bits) as i32) == 0
}

pub fn network_host_into(ip: InetRef<'_>, buf: &mut [u8]) -> PgResult<usize> {
    let len = ntop::pg_inet_net_ntop(ip.family as i32, ip.addr, ip.maxbits() as i32, buf)
        .ok_or_else(|| Box::new(could_not_format_err()))?;
    Ok(match buf[..len].iter().position(|&c| c == b'/') {
        Some(p) => p,
        None => len,
    })
}

pub fn network_show_into(ip: InetRef<'_>, buf: &mut [u8]) -> PgResult<usize> {
    let mut len = ntop::pg_inet_net_ntop(ip.family as i32, ip.addr, ip.maxbits() as i32, buf)
        .ok_or_else(|| Box::new(could_not_format_err()))?;
    if !buf[..len].contains(&b'/') {
        buf[len] = b'/';
        len += 1;
        len += write_decimal(buf, len, ip.bits as u32);
    }
    Ok(len)
}

pub fn inet_abbrev_into(ip: InetRef<'_>, buf: &mut [u8]) -> PgResult<usize> {
    ntop::pg_inet_net_ntop(ip.family as i32, ip.addr, ip.bits as i32, buf)
        .ok_or_else(|| Box::new(could_not_format_err()))
        .map_err(|e| e as Box<PgError>)
}

pub fn cidr_abbrev_into(ip: InetRef<'_>, buf: &mut [u8]) -> PgResult<usize> {
    cidr_ntop::pg_inet_cidr_ntop(ip.family as i32, ip.addr, ip.bits as i32, buf).ok_or_else(|| {
        Box::new(
            PgError::error("could not format cidr value")
                .with_sqlstate(ERRCODE_INVALID_BINARY_REPRESENTATION),
        )
    })
}

pub fn network_family(ip: InetRef<'_>) -> i32 {
    match ip.family {
        PGSQL_AF_INET => 4,
        PGSQL_AF_INET6 => 6,
        _ => 0,
    }
}

pub fn network_broadcast(ip: InetRef<'_>) -> InetValue {
    let mut dst = InetValue {
        family: ip.family,
        bits: ip.bits,
        ipaddr: [0u8; 16],
    };
    let mut bits = ip.bits as i32;
    for byte in 0..ip.addrsize() {
        let mask: u8 = if bits >= 8 {
            bits -= 8;
            0x00
        } else if bits == 0 {
            0xff
        } else {
            let m = 0xffu8 >> bits;
            bits = 0;
            m
        };
        dst.ipaddr[byte] = ip.addr[byte] | mask;
    }
    dst
}

pub fn network_network(ip: InetRef<'_>) -> InetValue {
    let mut dst = InetValue {
        family: ip.family,
        bits: ip.bits,
        ipaddr: [0u8; 16],
    };
    let mut bits = ip.bits as i32;
    let mut byte = 0;
    while bits != 0 {
        let mask: u8 = if bits >= 8 {
            bits -= 8;
            0xff
        } else {
            let m = 0xffu8 << (8 - bits);
            bits = 0;
            m
        };
        dst.ipaddr[byte] = ip.addr[byte] & mask;
        byte += 1;
    }
    dst
}

pub fn network_scan_first(ip: InetRef<'_>) -> InetValue {
    network_network(ip)
}

// Broadcast address with masklen maxed out (192.168.0.255/24 sorts before
// 192.168.0.255/32).
pub fn network_scan_last(ip: InetRef<'_>) -> PgResult<InetValue> {
    let b = network_broadcast(ip);
    inet_set_masklen(b.iref(), -1)
}

pub fn network_netmask(ip: InetRef<'_>) -> InetValue {
    let mut dst = InetValue {
        family: ip.family,
        bits: ip.maxbits(),
        ipaddr: [0u8; 16],
    };
    let mut bits = ip.bits as i32;
    let mut byte = 0;
    while bits != 0 {
        let mask: u8 = if bits >= 8 {
            bits -= 8;
            0xff
        } else {
            let m = 0xffu8 << (8 - bits);
            bits = 0;
            m
        };
        dst.ipaddr[byte] = mask;
        byte += 1;
    }
    dst
}

pub fn network_hostmask(ip: InetRef<'_>) -> InetValue {
    let mut dst = InetValue {
        family: ip.family,
        bits: ip.maxbits(),
        ipaddr: [0u8; 16],
    };
    let mut bits = ip.maxbits() as i32 - ip.bits as i32;
    let mut byte = ip.addrsize() as i32 - 1;
    while bits != 0 {
        let mask: u8 = if bits >= 8 {
            bits -= 8;
            0xff
        } else {
            let m = 0xffu8 >> (8 - bits);
            bits = 0;
            m
        };
        dst.ipaddr[byte as usize] = mask;
        byte -= 1;
    }
    dst
}

pub fn inet_same_family(a1: InetRef<'_>, a2: InetRef<'_>) -> bool {
    a1.family == a2.family
}

pub fn inet_merge(a1: InetRef<'_>, a2: InetRef<'_>) -> PgResult<InetValue> {
    if a1.family != a2.family {
        return Err(Box::new(
            PgError::error("cannot merge addresses from different families")
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
        ));
    }
    let commonbits = bitncommon(a1.addr, a2.addr, a1.bits.min(a2.bits) as i32);
    Ok(cidr_set_masklen_internal(a1, commonbits))
}

pub fn bitncmp(l: &[u8], r: &[u8], n: i32) -> i32 {
    let b = (n / 8) as usize;
    let x = memcmp(&l[..b], &r[..b]);
    if x != 0 || n % 8 == 0 {
        return x;
    }
    let mut lb = l[b] as u32;
    let mut rb = r[b] as u32;
    for _ in 0..(n % 8) {
        if (lb & 0x80) != (rb & 0x80) {
            return if lb & 0x80 != 0 { 1 } else { -1 };
        }
        lb <<= 1;
        rb <<= 1;
    }
    0
}

// libc memcmp convention (first differing byte difference) — network_cmp's
// int result is wire-visible via SELECT network_cmp(...).
fn memcmp(l: &[u8], r: &[u8]) -> i32 {
    for i in 0..l.len() {
        if l[i] != r[i] {
            return l[i] as i32 - r[i] as i32;
        }
    }
    0
}

pub fn bitncommon(l: &[u8], r: &[u8], n: i32) -> i32 {
    let mut nbits = n % 8;
    let nbytes = (n / 8) as usize;
    let mut byte = nbytes;
    // zipped whole-byte scan keeps the loop bounds-check-free (penalty gate).
    for (i, (a, b)) in l[..nbytes].iter().zip(&r[..nbytes]).enumerate() {
        if a != b {
            nbits = 7;
            byte = i;
            break;
        }
    }
    if nbits != 0 {
        let diff = (l[byte] ^ r[byte]) as u32;
        while (diff >> (8 - nbits)) != 0 {
            nbits -= 1;
        }
    }
    8 * byte as i32 + nbits
}

fn address_ok(a: &[u8; 16], bits: i32, family: u8) -> bool {
    let (maxbits, maxbytes) = if family == PGSQL_AF_INET {
        (32, 4)
    } else {
        (128, 16)
    };
    if bits == maxbits {
        return true;
    }
    let mut byte = (bits / 8) as usize;
    let nbits = bits % 8;
    let mut mask = 0xffu8;
    if bits != 0 {
        mask >>= nbits;
    }
    while byte < maxbytes {
        if a[byte] & mask != 0 {
            return false;
        }
        mask = 0xff;
        byte += 1;
    }
    true
}

pub fn inetnot(ip: InetRef<'_>) -> InetValue {
    let mut dst = InetValue {
        family: ip.family,
        bits: ip.bits,
        ipaddr: [0u8; 16],
    };
    for nb in 0..ip.addrsize() {
        dst.ipaddr[nb] = !ip.addr[nb];
    }
    dst
}

pub fn inetand(ip: InetRef<'_>, ip2: InetRef<'_>) -> PgResult<InetValue> {
    if ip.family != ip2.family {
        return Err(Box::new(family_mismatch_err("AND")));
    }
    let mut dst = InetValue {
        family: ip.family,
        bits: ip.bits.max(ip2.bits),
        ipaddr: [0u8; 16],
    };
    for nb in 0..ip.addrsize() {
        dst.ipaddr[nb] = ip.addr[nb] & ip2.addr[nb];
    }
    Ok(dst)
}

pub fn inetor(ip: InetRef<'_>, ip2: InetRef<'_>) -> PgResult<InetValue> {
    if ip.family != ip2.family {
        return Err(Box::new(family_mismatch_err("OR")));
    }
    let mut dst = InetValue {
        family: ip.family,
        bits: ip.bits.max(ip2.bits),
        ipaddr: [0u8; 16],
    };
    for nb in 0..ip.addrsize() {
        dst.ipaddr[nb] = ip.addr[nb] | ip2.addr[nb];
    }
    Ok(dst)
}

pub fn internal_inetpl(ip: InetRef<'_>, mut addend: i64) -> PgResult<InetValue> {
    let mut dst = InetValue {
        family: ip.family,
        bits: ip.bits,
        ipaddr: [0u8; 16],
    };
    let mut carry: i32 = 0;
    let mut nb = ip.addrsize();
    while nb > 0 {
        nb -= 1;
        carry = ip.addr[nb] as i32 + (addend & 0xFF) as i32 + carry;
        dst.ipaddr[nb] = (carry & 0xFF) as u8;
        carry >>= 8;
        addend &= !0xFFi64;
        addend /= 0x100;
    }
    if !((addend == 0 && carry == 0) || (addend == -1 && carry == 1)) {
        return Err(Box::new(out_of_range_err()));
    }
    Ok(dst)
}

pub fn inetmi(ip: InetRef<'_>, ip2: InetRef<'_>) -> PgResult<i64> {
    if ip.family != ip2.family {
        return Err(Box::new(family_mismatch_err("subtract")));
    }
    let mut res: i64 = 0;
    let mut nb = ip.addrsize();
    let mut byte = 0usize;
    let mut carry: i32 = 1;
    while nb > 0 {
        nb -= 1;
        carry = ip.addr[nb] as i32 + (!ip2.addr[nb] as i32 & 0xFF) + carry;
        let lobyte = carry & 0xFF;
        if byte < 8 {
            res |= (lobyte as i64) << (byte * 8);
        } else if if res < 0 { lobyte != 0xFF } else { lobyte != 0 } {
            return Err(Box::new(out_of_range_err()));
        }
        carry >>= 8;
        byte += 1;
    }
    if carry == 0 && byte < 8 {
        res |= ((!0u64) << (byte * 8)) as i64;
    }
    Ok(res)
}

pub fn hashinet_bytes(ip: InetRef<'_>) -> u32 {
    let mut buf = [0u8; 18];
    let n = ip.addrsize() + 2;
    buf[0] = ip.family;
    buf[1] = ip.bits;
    buf[2..n].copy_from_slice(&ip.addr[..n - 2]);
    hashfn::hash_bytes(&buf[..n])
}

pub fn hashinet_bytes_extended(ip: InetRef<'_>, seed: u64) -> u64 {
    let mut buf = [0u8; 18];
    let n = ip.addrsize() + 2;
    buf[0] = ip.family;
    buf[1] = ip.bits;
    buf[2..n].copy_from_slice(&ip.addr[..n - 2]);
    hashfn::hash_bytes_extended(&buf[..n], seed)
}
