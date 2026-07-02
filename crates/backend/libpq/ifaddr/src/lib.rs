//! ifaddr.c: IP netmask arithmetic. pg_foreach_ifaddr (interface
//! enumeration for samehost/samenet) is deferred loud.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AddressFamily {
    Inet,
    Inet6,
    Other,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SockAddrError {
    InvalidBits,
    UnsupportedFamily,
}

pub fn pg_range_sockaddr(addr: &IpAddr, netaddr: &IpAddr, netmask: &IpAddr) -> bool {
    match (addr, netaddr, netmask) {
        (IpAddr::V4(a), IpAddr::V4(n), IpAddr::V4(m)) => {
            ((a.to_bits() ^ n.to_bits()) & m.to_bits()) == 0
        }
        (IpAddr::V6(a), IpAddr::V6(n), IpAddr::V6(m)) => a
            .octets()
            .iter()
            .zip(n.octets())
            .zip(m.octets())
            .all(|((a, n), m)| ((a ^ n) & m) == 0),
        _ => false,
    }
}

pub fn pg_sockaddr_cidr_mask(
    numbits: Option<&str>,
    family: AddressFamily,
) -> Result<IpAddr, SockAddrError> {
    let bits = match numbits {
        Some(s) => parse_strtol_base10(s).ok_or(SockAddrError::InvalidBits)?,
        None => match family {
            AddressFamily::Inet => 32,
            _ => 128,
        },
    };
    match family {
        AddressFamily::Inet => {
            if !(0..=32).contains(&bits) {
                return Err(SockAddrError::InvalidBits);
            }
            // avoid "x << 32", which is not portable
            let mask: u32 = if bits > 0 {
                ((0xffff_ffff_u64 << (32 - bits as u32)) & 0xffff_ffff) as u32
            } else {
                0
            };
            Ok(IpAddr::V4(Ipv4Addr::from_bits(mask)))
        }
        AddressFamily::Inet6 => {
            if !(0..=128).contains(&bits) {
                return Err(SockAddrError::InvalidBits);
            }
            let mut remaining = bits;
            let mut octets = [0u8; 16];
            for byte in &mut octets {
                *byte = if remaining <= 0 {
                    0
                } else if remaining >= 8 {
                    0xff
                } else {
                    ((0xff_u16 << (8 - remaining as u8)) & 0xff) as u8
                };
                remaining -= 8;
            }
            Ok(IpAddr::V6(Ipv6Addr::from(octets)))
        }
        AddressFamily::Other => Err(SockAddrError::UnsupportedFamily),
    }
}

#[cold]
pub fn pg_foreach_ifaddr<F>(_callback: F) -> std::io::Result<()>
where
    F: FnMut(IpAddr, IpAddr),
{
    panic!(
        "pg_foreach_ifaddr: backend-libpq-ifaddr interface enumeration unported \
         (pg_hba samehost/samenet records)"
    );
}

// strtol(s, &endptr, 10) + the C caller's *numbits=='\0' || *endptr!='\0' check.
fn parse_strtol_base10(s: &str) -> Option<i64> {
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() && (bytes[i].is_ascii_whitespace() || bytes[i] == 0x0b) {
        i += 1;
    }
    let negative = match bytes.get(i) {
        Some(b'+') => {
            i += 1;
            false
        }
        Some(b'-') => {
            i += 1;
            true
        }
        _ => false,
    };
    let digits_start = i;
    let mut value: i64 = 0;
    while i < bytes.len() && bytes[i].is_ascii_digit() {
        value = value
            .checked_mul(10)?
            .checked_add((bytes[i] - b'0') as i64)?;
        i += 1;
    }
    if i == digits_start || i != bytes.len() {
        return None;
    }
    Some(if negative { -value } else { value })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cidr_masks() {
        assert_eq!(
            pg_sockaddr_cidr_mask(Some("32"), AddressFamily::Inet).unwrap(),
            IpAddr::V4(Ipv4Addr::new(255, 255, 255, 255))
        );
        assert_eq!(
            pg_sockaddr_cidr_mask(Some("24"), AddressFamily::Inet).unwrap(),
            IpAddr::V4(Ipv4Addr::new(255, 255, 255, 0))
        );
        assert_eq!(
            pg_sockaddr_cidr_mask(Some("0"), AddressFamily::Inet).unwrap(),
            IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0))
        );
        assert_eq!(
            pg_sockaddr_cidr_mask(Some("128"), AddressFamily::Inet6).unwrap(),
            IpAddr::V6(Ipv6Addr::from([0xffu8; 16]))
        );
        let IpAddr::V6(m64) = pg_sockaddr_cidr_mask(Some("64"), AddressFamily::Inet6).unwrap()
        else {
            unreachable!()
        };
        assert_eq!(m64.octets()[..8], [0xff; 8]);
        assert_eq!(m64.octets()[8..], [0; 8]);
        assert_eq!(
            pg_sockaddr_cidr_mask(Some("33"), AddressFamily::Inet),
            Err(SockAddrError::InvalidBits)
        );
        assert_eq!(
            pg_sockaddr_cidr_mask(Some("12x"), AddressFamily::Inet),
            Err(SockAddrError::InvalidBits)
        );
        assert_eq!(
            pg_sockaddr_cidr_mask(Some(""), AddressFamily::Inet),
            Err(SockAddrError::InvalidBits)
        );
        assert_eq!(
            pg_sockaddr_cidr_mask(None, AddressFamily::Inet).unwrap(),
            IpAddr::V4(Ipv4Addr::new(255, 255, 255, 255))
        );
        assert_eq!(
            pg_sockaddr_cidr_mask(Some("1"), AddressFamily::Other),
            Err(SockAddrError::UnsupportedFamily)
        );
    }

    #[test]
    fn range_checks() {
        let a: IpAddr = "127.0.0.1".parse().unwrap();
        let m32 = pg_sockaddr_cidr_mask(Some("32"), AddressFamily::Inet).unwrap();
        assert!(pg_range_sockaddr(&a, &a, &m32));
        let net10: IpAddr = "10.0.0.0".parse().unwrap();
        let m8 = pg_sockaddr_cidr_mask(Some("8"), AddressFamily::Inet).unwrap();
        assert!(!pg_range_sockaddr(&a, &net10, &m8));
        let a10: IpAddr = "10.1.2.3".parse().unwrap();
        assert!(pg_range_sockaddr(&a10, &net10, &m8));
        let v6: IpAddr = "::1".parse().unwrap();
        let m128 = pg_sockaddr_cidr_mask(Some("128"), AddressFamily::Inet6).unwrap();
        assert!(pg_range_sockaddr(&v6, &v6, &m128));
        assert!(!pg_range_sockaddr(&a, &v6, &m128));
    }
}
