//! IP netmask calculations, and enumerating network interfaces.
//!
//! Port of `src/backend/libpq/ifaddr.c`. The C file operates on raw
//! `struct sockaddr_storage` buffers; the supported families are `AF_INET`
//! and `AF_INET6`, modelled here with [`std::net::IpAddr`], so the public
//! API is safe Rust.
//!
//! `pg_foreach_ifaddr` is the `getifaddrs()` build variant (BSDs, macOS,
//! Solaris, illumos and Linux) on Unix; on other targets it is the C file's
//! last-resort fallback that reports only the standard loopback addresses.
//!
//! mcx audit (per `docs/mctx-design.md`): `ifaddr.c` performs no
//! memory-context allocation. Its only heap use is libc `malloc`/`realloc`
//! scratch buffers in the WIN32/SIOCGIFCONF variants (freed before return,
//! failure surfaced as `-1`/`ENOMEM`, never `ereport`); the getifaddrs
//! buffer is libc-owned and released via `freeifaddrs`. Every function here
//! is therefore a pure scalar computation or a callback-driven enumerator:
//! nothing takes `Mcx` and nothing returns `PgResult` — `PgResult` would
//! misstate the C failure surface, which is an int return code. Callers
//! that collect addresses allocate in their own context inside the callback.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

/// Address family selector for [`pg_sockaddr_cidr_mask`], mirroring the
/// `AF_*` constants passed to the C function.
///
/// `AF_INET` and `AF_INET6` are the only families the C function builds a
/// mask for; every other family falls into the `default: return -1` arm,
/// modelled by [`Other`](AddressFamily::Other).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AddressFamily {
    /// `AF_INET`.
    Inet,
    /// `AF_INET6`.
    Inet6,
    /// Any other family (unsupported by the mask builder).
    Other,
}

/// Error returned by [`pg_sockaddr_cidr_mask`], distinguishing the two
/// failure modes the C function collapses into its single `-1` return code.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SockAddrError {
    /// `numbits` was empty, contained trailing garbage, or was out of range
    /// for the requested family.
    InvalidBits,
    /// The requested family is neither `AF_INET` nor `AF_INET6`.
    UnsupportedFamily,
}

/// `pg_range_sockaddr` - is `addr` within the subnet specified by
/// `netaddr`/`netmask`?
///
/// Note: in C the caller must already have verified that all three addresses
/// are in the same address family, and AF_UNIX addresses are not supported.
/// Here the family is encoded in the [`IpAddr`] variant; addresses in
/// different families are never in range (matching C, where an unsupported
/// family returns 0).
pub fn pg_range_sockaddr(addr: &IpAddr, netaddr: &IpAddr, netmask: &IpAddr) -> bool {
    match (addr, netaddr, netmask) {
        (IpAddr::V4(addr), IpAddr::V4(netaddr), IpAddr::V4(netmask)) => {
            range_sockaddr_af_inet(addr, netaddr, netmask)
        }
        (IpAddr::V6(addr), IpAddr::V6(netaddr), IpAddr::V6(netmask)) => {
            range_sockaddr_af_inet6(addr, netaddr, netmask)
        }
        _ => false,
    }
}

fn range_sockaddr_af_inet(addr: &Ipv4Addr, netaddr: &Ipv4Addr, netmask: &Ipv4Addr) -> bool {
    ((addr.to_bits() ^ netaddr.to_bits()) & netmask.to_bits()) == 0
}

fn range_sockaddr_af_inet6(addr: &Ipv6Addr, netaddr: &Ipv6Addr, netmask: &Ipv6Addr) -> bool {
    addr.octets()
        .iter()
        .zip(netaddr.octets().iter())
        .zip(netmask.octets().iter())
        .all(|((addr, netaddr), netmask)| ((addr ^ netaddr) & netmask) == 0)
}

/// `pg_sockaddr_cidr_mask` - make a network mask of the appropriate family
/// and required number of significant bits.
///
/// `numbits` may be `None`, in which case the mask is fully set (32 bits for
/// `AF_INET`, 128 bits for `AF_INET6`). When `Some`, it is parsed with the
/// same `strtol(numbits, &endptr, 10)` semantics as the C: leading whitespace
/// and an optional sign are accepted, but an empty string or trailing garbage
/// is rejected.
///
/// Returns the resulting mask, or [`SockAddrError`] on failure (the C
/// function's `-1` return).
pub fn pg_sockaddr_cidr_mask(
    numbits: Option<&str>,
    family: AddressFamily,
) -> Result<IpAddr, SockAddrError> {
    let bits = match numbits {
        Some(numbits) => parse_strtol_base10(numbits).ok_or(SockAddrError::InvalidBits)?,
        None => match family {
            AddressFamily::Inet => 32,
            _ => 128,
        },
    };

    match family {
        AddressFamily::Inet => ipv4_cidr_mask(bits),
        AddressFamily::Inet6 => ipv6_cidr_mask(bits),
        AddressFamily::Other => Err(SockAddrError::UnsupportedFamily),
    }
}

fn ipv4_cidr_mask(bits: i64) -> Result<IpAddr, SockAddrError> {
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

fn ipv6_cidr_mask(bits: i64) -> Result<IpAddr, SockAddrError> {
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

/// `strtol(s, &endptr, 10)` followed by the C caller's
/// `*numbits == '\0' || *endptr != '\0'` validity check.
///
/// Accepts optional leading ASCII whitespace and an optional `+`/`-` sign,
/// then base-10 digits, and requires the entire string to be consumed.
/// Overflow is reported as failure (the result is bounds-checked by the
/// callers anyway, so an out-of-range magnitude maps to `InvalidBits` either
/// way).
fn parse_strtol_base10(s: &str) -> Option<i64> {
    let bytes = s.as_bytes();
    let mut i = 0;

    // C-locale isspace(): space, \t, \n, \v, \f, \r. Note that Rust's
    // `is_ascii_whitespace` excludes vertical tab (0x0b), which strtol skips.
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
    while i < bytes.len() {
        let c = bytes[i];
        if !c.is_ascii_digit() {
            break;
        }
        let digit = (c - b'0') as i64;
        value = value.checked_mul(10)?.checked_add(digit)?;
        i += 1;
    }

    // No digits consumed: strtol leaves endptr at the start of the string,
    // which the C check rejects.
    if i == digits_start {
        return None;
    }

    // Trailing garbage: the C `*endptr != '\0'` check rejects it.
    if i != bytes.len() {
        return None;
    }

    Some(if negative { -value } else { value })
}

/// Run the callback for the addr/mask, after making sure the mask is sane
/// for the addr (the C static `run_ifaddr_callback`).
///
/// If the mask's family differs from the addr's, or the mask is the all-zero
/// (`INADDR_ANY` / `IN6_IS_ADDR_UNSPECIFIED`) address, a fully-set mask is
/// generated for the addr's family instead.
fn run_ifaddr_callback<F>(callback: &mut F, addr: IpAddr, mask: Option<IpAddr>)
where
    F: FnMut(IpAddr, IpAddr),
{
    // Check that the mask is valid.
    let mask = mask.filter(|mask| mask_is_valid_for_addr(&addr, mask));

    // If mask is invalid, generate our own fully-set mask. The C
    // pg_sockaddr_cidr_mask(&fullmask, NULL, family) cannot fail for the
    // AF_INET/AF_INET6 families that reach here.
    let mask = match mask {
        Some(mask) => mask,
        None => pg_sockaddr_cidr_mask(None, address_family(&addr))
            .expect("full mask for INET/INET6 always succeeds"),
    };

    callback(addr, mask);
}

fn mask_is_valid_for_addr(addr: &IpAddr, mask: &IpAddr) -> bool {
    match (addr, mask) {
        // `mask->sa_family != addr->sa_family` -> invalid.
        (IpAddr::V4(_), IpAddr::V6(_)) | (IpAddr::V6(_), IpAddr::V4(_)) => false,
        // `sin_addr.s_addr == INADDR_ANY` -> invalid.
        (IpAddr::V4(_), IpAddr::V4(mask)) => *mask != Ipv4Addr::UNSPECIFIED,
        // `IN6_IS_ADDR_UNSPECIFIED(...)` -> invalid.
        (IpAddr::V6(_), IpAddr::V6(mask)) => *mask != Ipv6Addr::UNSPECIFIED,
    }
}

fn address_family(addr: &IpAddr) -> AddressFamily {
    match addr {
        IpAddr::V4(_) => AddressFamily::Inet,
        IpAddr::V6(_) => AddressFamily::Inet6,
    }
}

/// Decode a raw `struct sockaddr` into an [`IpAddr`].
///
/// Returns `None` for a null pointer or a family other than
/// `AF_INET`/`AF_INET6`. The C code passes such addresses through untouched
/// and lets the callback inspect `sa_family`; since this port's callback
/// vocabulary is [`IpAddr`], non-IP entries (e.g. `AF_LINK`/`AF_PACKET`) are
/// filtered here instead — every PostgreSQL callback ignores them anyway.
///
/// # Safety
///
/// `sa`, when non-null, must point to a valid sockaddr of at least the size
/// implied by its `sa_family` (guaranteed by `getifaddrs`).
#[cfg(unix)]
unsafe fn sockaddr_to_ipaddr(sa: *const libc::sockaddr) -> Option<IpAddr> {
    if sa.is_null() {
        return None;
    }
    match (*sa).sa_family as i32 {
        libc::AF_INET => {
            let sin = &*(sa as *const libc::sockaddr_in);
            Some(IpAddr::V4(Ipv4Addr::from_bits(u32::from_be(
                sin.sin_addr.s_addr,
            ))))
        }
        libc::AF_INET6 => {
            let sin6 = &*(sa as *const libc::sockaddr_in6);
            Some(IpAddr::V6(Ipv6Addr::from(sin6.sin6_addr.s6_addr)))
        }
        _ => None,
    }
}

/// `pg_foreach_ifaddr` - enumerate the system's network interface addresses
/// and call `callback` for each one.
///
/// This is the `getifaddrs()` build variant of the C function. Returns
/// `Ok(())` if successful, or the OS error if `getifaddrs` fails (the C `-1`
/// return).
#[cfg(unix)]
pub fn pg_foreach_ifaddr<F>(mut callback: F) -> std::io::Result<()>
where
    F: FnMut(IpAddr, IpAddr),
{
    unsafe {
        let mut ifa: *mut libc::ifaddrs = std::ptr::null_mut();
        if libc::getifaddrs(&mut ifa) < 0 {
            return Err(std::io::Error::last_os_error());
        }

        let mut l = ifa;
        while !l.is_null() {
            // The C run_ifaddr_callback returns immediately for a null addr;
            // non-IP families are likewise skipped (see sockaddr_to_ipaddr).
            if let Some(addr) = sockaddr_to_ipaddr((*l).ifa_addr) {
                let mask = sockaddr_to_ipaddr((*l).ifa_netmask);
                run_ifaddr_callback(&mut callback, addr, mask);
            }
            l = (*l).ifa_next;
        }

        libc::freeifaddrs(ifa);
    }
    Ok(())
}

/// `pg_foreach_ifaddr` - fallback when there is no known way to get the
/// interface addresses: just report the standard loopback addresses
/// `127.0.0.1/8` and `::1/128`.
#[cfg(not(unix))]
pub fn pg_foreach_ifaddr<F>(mut callback: F) -> std::io::Result<()>
where
    F: FnMut(IpAddr, IpAddr),
{
    // addr 127.0.0.1/8
    let addr = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
    let mask =
        pg_sockaddr_cidr_mask(Some("8"), AddressFamily::Inet).expect("8-bit IPv4 mask is valid");
    run_ifaddr_callback(&mut callback, addr, Some(mask));

    // addr ::1/128
    let addr6 = IpAddr::V6(Ipv6Addr::LOCALHOST);
    let mask = pg_sockaddr_cidr_mask(Some("128"), AddressFamily::Inet6)
        .expect("128-bit IPv6 mask is valid");
    run_ifaddr_callback(&mut callback, addr6, Some(mask));

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ipv4_cidr_masks_match_prefix_lengths() {
        let mask = pg_sockaddr_cidr_mask(Some("24"), AddressFamily::Inet).expect("valid mask");
        assert_eq!(mask, IpAddr::V4(Ipv4Addr::new(255, 255, 255, 0)));
    }

    #[test]
    fn ipv4_cidr_mask_zero_bits_is_all_zero() {
        let mask = pg_sockaddr_cidr_mask(Some("0"), AddressFamily::Inet).expect("valid mask");
        assert_eq!(mask, IpAddr::V4(Ipv4Addr::UNSPECIFIED));
    }

    #[test]
    fn ipv4_cidr_mask_full_bits_is_all_ones() {
        let mask = pg_sockaddr_cidr_mask(Some("32"), AddressFamily::Inet).expect("valid mask");
        assert_eq!(mask, IpAddr::V4(Ipv4Addr::new(255, 255, 255, 255)));
        // None == fully set, same result.
        assert_eq!(pg_sockaddr_cidr_mask(None, AddressFamily::Inet), Ok(mask));
    }

    #[test]
    fn ipv6_cidr_masks_match_prefix_lengths() {
        let mask = pg_sockaddr_cidr_mask(Some("73"), AddressFamily::Inet6).expect("valid mask");
        let bytes = match mask {
            IpAddr::V6(addr) => addr.octets(),
            _ => panic!("expected IPv6 mask"),
        };

        assert_eq!(&bytes[..9], &[0xff; 9]);
        assert_eq!(bytes[9], 0x80);
        assert!(bytes[10..].iter().all(|byte| *byte == 0));
    }

    #[test]
    fn ipv6_cidr_mask_default_is_full() {
        let mask = pg_sockaddr_cidr_mask(None, AddressFamily::Inet6).expect("valid mask");
        assert_eq!(mask, IpAddr::V6(Ipv6Addr::from([0xffu8; 16])));
    }

    #[test]
    fn cidr_mask_rejects_bad_input() {
        assert_eq!(
            pg_sockaddr_cidr_mask(Some("abc"), AddressFamily::Inet).err(),
            Some(SockAddrError::InvalidBits)
        );
        assert_eq!(
            pg_sockaddr_cidr_mask(Some("33"), AddressFamily::Inet).err(),
            Some(SockAddrError::InvalidBits)
        );
        assert_eq!(
            pg_sockaddr_cidr_mask(Some("129"), AddressFamily::Inet6).err(),
            Some(SockAddrError::InvalidBits)
        );
        assert_eq!(
            pg_sockaddr_cidr_mask(Some("1"), AddressFamily::Other).err(),
            Some(SockAddrError::UnsupportedFamily)
        );
    }

    #[test]
    fn cidr_mask_rejects_empty_and_trailing_garbage() {
        assert_eq!(
            pg_sockaddr_cidr_mask(Some(""), AddressFamily::Inet).err(),
            Some(SockAddrError::InvalidBits)
        );
        assert_eq!(
            pg_sockaddr_cidr_mask(Some("24x"), AddressFamily::Inet).err(),
            Some(SockAddrError::InvalidBits)
        );
        // Out-of-range negative still maps to InvalidBits.
        assert_eq!(
            pg_sockaddr_cidr_mask(Some("-1"), AddressFamily::Inet).err(),
            Some(SockAddrError::InvalidBits)
        );
    }

    #[test]
    fn cidr_mask_accepts_strtol_style_whitespace_and_sign() {
        let a = pg_sockaddr_cidr_mask(Some("  24"), AddressFamily::Inet).expect("valid");
        let b = pg_sockaddr_cidr_mask(Some("+24"), AddressFamily::Inet).expect("valid");
        let c = pg_sockaddr_cidr_mask(Some("24"), AddressFamily::Inet).expect("valid");
        // C-locale isspace() includes vertical tab, which strtol skips.
        let d = pg_sockaddr_cidr_mask(Some("\x0b\t24"), AddressFamily::Inet).expect("valid");
        assert_eq!(a, c);
        assert_eq!(b, c);
        assert_eq!(d, c);
    }

    #[test]
    fn range_sockaddr_matches_ipv4_subnets() {
        let addr = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 42));
        let netaddr = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 0));
        let mask = pg_sockaddr_cidr_mask(Some("24"), AddressFamily::Inet).expect("valid mask");

        assert!(pg_range_sockaddr(&addr, &netaddr, &mask));

        let other = IpAddr::V4(Ipv4Addr::new(192, 168, 2, 42));
        assert!(!pg_range_sockaddr(&other, &netaddr, &mask));
    }

    #[test]
    fn range_sockaddr_matches_ipv6_subnets() {
        let addr = IpAddr::V6(Ipv6Addr::from([
            0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1,
        ]));
        let netaddr = IpAddr::V6(Ipv6Addr::from([
            0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        ]));
        let mask = pg_sockaddr_cidr_mask(Some("32"), AddressFamily::Inet6).expect("valid mask");

        assert!(pg_range_sockaddr(&addr, &netaddr, &mask));

        let other = IpAddr::V6(Ipv6Addr::from([
            0x20, 0x01, 0x0d, 0xb9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1,
        ]));
        assert!(!pg_range_sockaddr(&other, &netaddr, &mask));
    }

    #[test]
    fn range_sockaddr_different_families_never_in_range() {
        let v4 = IpAddr::V4(Ipv4Addr::UNSPECIFIED);
        let v6 = IpAddr::V6(Ipv6Addr::UNSPECIFIED);
        assert!(!pg_range_sockaddr(&v4, &v6, &v6));
    }

    #[test]
    fn run_ifaddr_callback_substitutes_full_mask() {
        let mut seen = Vec::new();
        let addr = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));

        // No mask at all.
        run_ifaddr_callback(&mut |a, m| seen.push((a, m)), addr, None);
        // All-zero (INADDR_ANY) mask.
        run_ifaddr_callback(
            &mut |a, m| seen.push((a, m)),
            addr,
            Some(IpAddr::V4(Ipv4Addr::UNSPECIFIED)),
        );
        // Mask family differs from addr family.
        run_ifaddr_callback(
            &mut |a, m| seen.push((a, m)),
            addr,
            Some(IpAddr::V6(Ipv6Addr::from([0xffu8; 16]))),
        );

        let full = IpAddr::V4(Ipv4Addr::new(255, 255, 255, 255));
        assert_eq!(seen, vec![(addr, full), (addr, full), (addr, full)]);

        // A valid mask passes through unchanged.
        let mut seen = Vec::new();
        let mask = IpAddr::V4(Ipv4Addr::new(255, 0, 0, 0));
        run_ifaddr_callback(&mut |a, m| seen.push((a, m)), addr, Some(mask));
        assert_eq!(seen, vec![(addr, mask)]);
    }

    /// The caller-side mcx pattern this crate's docs prescribe: the
    /// enumerator itself is allocation-free, and a caller that wants an
    /// address list builds it in its own context inside the callback
    /// (fallibly, per the OOM rule), so the bytes are charged there.
    fn collect_ifaddrs_in(
        mcx: mcx::Mcx<'_>,
    ) -> types_error::PgResult<mcx::PgVec<'_, (IpAddr, IpAddr)>> {
        let mut list = mcx::PgVec::new_in(mcx);
        let mut oom = Ok(());
        pg_foreach_ifaddr(|addr, mask| {
            if oom.is_err() {
                return;
            }
            match list.try_reserve(1) {
                Ok(()) => list.push((addr, mask)),
                Err(_) => oom = Err(mcx.oom(core::mem::size_of::<(IpAddr, IpAddr)>())),
            }
        })
        .expect("interface enumeration succeeds");
        oom?;
        Ok(list)
    }

    #[test]
    fn collected_address_list_is_charged_to_the_context() {
        let ctx = mcx::MemoryContext::new("ifaddr-test");
        let list = collect_ifaddrs_in(ctx.mcx()).expect("no OOM");
        assert!(!list.is_empty(), "every machine has a loopback interface");
        assert_eq!(
            ctx.used(),
            list.capacity() * core::mem::size_of::<(IpAddr, IpAddr)>(),
            "the list's bytes are accounted in the caller's context"
        );
    }

    #[test]
    fn collected_address_list_bytes_return_on_drop() {
        let ctx = mcx::MemoryContext::new("ifaddr-test");
        {
            let list = collect_ifaddrs_in(ctx.mcx()).expect("no OOM");
            assert!(ctx.used() > 0, "collected list charges the context");
            drop(list);
        }
        assert_eq!(ctx.used(), 0, "dropping the list returns every byte");
    }

    #[cfg(unix)]
    #[test]
    fn foreach_ifaddr_reports_loopback_and_sane_masks() {
        let mut seen: Vec<(IpAddr, IpAddr)> = Vec::new();
        pg_foreach_ifaddr(|addr, mask| seen.push((addr, mask))).expect("getifaddrs succeeds");

        // Every machine running the test suite has a loopback interface.
        assert!(seen
            .iter()
            .any(|(addr, _)| *addr == IpAddr::V4(Ipv4Addr::LOCALHOST)));

        // run_ifaddr_callback guarantees the mask matches the addr's family
        // and is never the unspecified address.
        for (addr, mask) in &seen {
            assert_eq!(addr.is_ipv4(), mask.is_ipv4());
            match mask {
                IpAddr::V4(m) => assert_ne!(*m, Ipv4Addr::UNSPECIFIED),
                IpAddr::V6(m) => assert_ne!(*m, Ipv6Addr::UNSPECIFIED),
            }
        }
    }
}
