//! Port of `src/backend/utils/adt/inet_cidr_ntop.c` — convert a network number
//! from network (binary) form to CIDR-style presentation text.
//!
//! Every function in `inet_cidr_ntop.c` is ported here with logic identical to
//! the PostgreSQL 18.3 C source (branch order and the exact byte-level output
//! preserved, including the `size` buffer-capacity checks the ISC code performs
//! as it walks the destination). The ISC code returns `NULL` on error and sets
//! `errno` (`EAFNOSUPPORT` / `EINVAL` / `EMSGSIZE`); this port mirrors that via
//! [`InetCidrNtopError`] whose [`InetCidrNtopError::errno`] gives the same value.

use core::error::Error;
use core::fmt;

use ::types_network::{PGSQL_AF_INET, PGSQL_AF_INET6};

/// POSIX `errno` values mirrored by [`InetCidrNtopError::errno`] (build-target
/// `libc` `E*` values, without depending on `libc`).
mod errno {
    #[cfg(target_os = "linux")]
    pub const EINVAL: i32 = 22;
    #[cfg(target_os = "linux")]
    pub const EMSGSIZE: i32 = 90;
    #[cfg(target_os = "linux")]
    pub const EAFNOSUPPORT: i32 = 97;

    #[cfg(any(target_os = "macos", target_os = "ios"))]
    pub const EINVAL: i32 = 22;
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    pub const EMSGSIZE: i32 = 40;
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    pub const EAFNOSUPPORT: i32 = 47;

    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "ios")))]
    pub const EINVAL: i32 = 22;
    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "ios")))]
    pub const EMSGSIZE: i32 = 90;
    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "ios")))]
    pub const EAFNOSUPPORT: i32 = 97;
}

/// Error returned by [`pg_inet_cidr_ntop`] and its IPv4/IPv6 helpers; mirrors the
/// ISC `errno` values.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InetCidrNtopError {
    /// C: `errno = EAFNOSUPPORT` — unknown address family.
    AddressFamilyNotSupported,
    /// C: `errno = EINVAL` — `bits` out of range for the family.
    InvalidArgument,
    /// C: `errno = EMSGSIZE` — destination buffer too small.
    MessageTooLong,
}

impl InetCidrNtopError {
    /// The `errno` value the C code would have set for this failure.
    pub const fn errno(self) -> i32 {
        match self {
            Self::AddressFamilyNotSupported => errno::EAFNOSUPPORT,
            Self::InvalidArgument => errno::EINVAL,
            Self::MessageTooLong => errno::EMSGSIZE,
        }
    }
}

impl fmt::Display for InetCidrNtopError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AddressFamilyNotSupported => f.write_str("address family not supported"),
            Self::InvalidArgument => f.write_str("invalid bits for address family"),
            Self::MessageTooLong => f.write_str("destination buffer is too small"),
        }
    }
}

impl Error for InetCidrNtopError {}

/// C: `char *pg_inet_cidr_ntop(int af, const void *src, int bits, char *dst, size_t size)`.
///
/// Convert the network number in `src` to CIDR-style presentation text written
/// into `dst`. The slice length of `dst` plays the C `size` parameter. On success
/// returns the number of bytes written (not counting the trailing NUL, which is
/// written at that offset), or the matching [`InetCidrNtopError`].
pub fn pg_inet_cidr_ntop(
    af: i32,
    src: &[u8],
    bits: i32,
    dst: &mut [u8],
) -> Result<usize, InetCidrNtopError> {
    match af {
        a if a == PGSQL_AF_INET as i32 => inet_cidr_ntop_ipv4(src, bits, dst),
        a if a == PGSQL_AF_INET6 as i32 => inet_cidr_ntop_ipv6(src, bits, dst),
        _ => Err(InetCidrNtopError::AddressFamilyNotSupported),
    }
}

/// C: `static char *inet_cidr_ntop_ipv4(const u_char *src, int bits, char *dst, size_t size)`.
fn inet_cidr_ntop_ipv4(src: &[u8], bits: i32, dst: &mut [u8]) -> Result<usize, InetCidrNtopError> {
    // The `sizeof "..."` literals in C include the trailing NUL, so e.g.
    // `sizeof "255."` == 5. All comparisons are kept byte-identical.
    let mut d: usize = 0;
    let odst: usize = 0;
    let mut size: usize = dst.len();
    let mut si: usize = 0; // index walking `src`, mirrors `*src++`

    if !(0..=32).contains(&bits) {
        return Err(InetCidrNtopError::InvalidArgument);
    }

    if bits == 0 {
        // if (size < sizeof "0")  -> size < 2
        if size < 2 {
            return Err(InetCidrNtopError::MessageTooLong);
        }
        dst[d] = b'0';
        d += 1;
        size -= 1;
        dst[d] = b'\0';
    }

    // Format whole octets.
    let mut b = bits / 8;
    while b > 0 {
        // if (size <= sizeof "255.")  -> size <= 5
        if size <= 5 {
            return Err(InetCidrNtopError::MessageTooLong);
        }
        let t = d;
        d += write_u(dst, d, src[si] as u32);
        si += 1;
        if b > 1 {
            dst[d] = b'.';
            d += 1;
            dst[d] = b'\0';
        }
        size -= d - t;
        b -= 1;
    }

    // Format partial octet.
    let b = bits % 8;
    if b > 0 {
        // if (size <= sizeof ".255")  -> size <= 5
        if size <= 5 {
            return Err(InetCidrNtopError::MessageTooLong);
        }
        let t = d;
        if d != odst {
            dst[d] = b'.';
            d += 1;
        }
        let m: u32 = ((1u32 << b) - 1) << (8 - b);
        d += write_u(dst, d, src[si] as u32 & m);
        size -= d - t;
    }

    // Format CIDR /width.
    // if (size <= sizeof "/32")  -> size <= 4
    if size <= 4 {
        return Err(InetCidrNtopError::MessageTooLong);
    }
    dst[d] = b'/';
    d += 1;
    d += write_u(dst, d, bits as u32);
    Ok(d)
}

/// C: `static char *inet_cidr_ntop_ipv6(const u_char *src, int bits, char *dst, size_t size)`.
///
/// C uses a fixed-size 50-byte `outbuf` scratch (formatting can never overflow
/// it) then `strlen(outbuf) + 1 > size` checks before `strcpy`-ing into `dst`. We
/// mirror that: build into a working buffer (the automatic-storage analog), then
/// bounds-check once at the end.
fn inet_cidr_ntop_ipv6(src: &[u8], bits: i32, dst: &mut [u8]) -> Result<usize, InetCidrNtopError> {
    let outbuf = build_ipv6_outbuf(src, bits)?;
    // if (strlen(outbuf) + 1 > size) goto emsgsize;
    if outbuf.len() + 1 > dst.len() {
        return Err(InetCidrNtopError::MessageTooLong);
    }
    // strcpy(dst, outbuf);
    dst[..outbuf.len()].copy_from_slice(&outbuf);
    dst[outbuf.len()] = b'\0';
    Ok(outbuf.len())
}

/// Build the IPv6 CIDR presentation text into a `Vec<u8>` (the working scratch).
fn build_ipv6_outbuf(src: &[u8], bits: i32) -> Result<Vec<u8>, InetCidrNtopError> {
    let mut outbuf: Vec<u8> = Vec::new();

    if !(0..=128).contains(&bits) {
        return Err(InetCidrNtopError::InvalidArgument);
    }

    if bits == 0 {
        outbuf.push(b':');
        outbuf.push(b':');
        // C writes a trailing '\0' here; we omit it (the final NUL is written
        // when copying into `dst`).
    } else {
        // Copy src to private buffer.  Zero host part.
        let mut inbuf = [0u8; 16];
        let p = ((bits + 7) / 8) as usize;
        inbuf[..p].copy_from_slice(&src[..p]);
        let b = bits % 8;
        if b != 0 {
            // m = ((u_int) ~0) << (8 - b); truncated to a byte by the &= assign.
            let m: u32 = (!0u32) << (8 - b);
            inbuf[p - 1] = (inbuf[p - 1] as u32 & m) as u8;
        }

        // `s` walks `inbuf`; mirror it with an index.
        let mut si: usize = 0;

        // how many words need to be displayed in output
        let mut words = (bits + 15) / 16;
        if words == 1 {
            words = 2;
        }

        // Find the longest substring of zero's.
        let mut zero_s: i32 = 0;
        let mut zero_l: i32 = 0;
        let mut tmp_zero_s: i32 = 0;
        let mut tmp_zero_l: i32 = 0;
        let mut i = 0;
        while i < words * 2 {
            if (inbuf[i as usize] | inbuf[(i + 1) as usize]) == 0 {
                if tmp_zero_l == 0 {
                    tmp_zero_s = i / 2;
                }
                tmp_zero_l += 1;
            } else if tmp_zero_l != 0 && zero_l < tmp_zero_l {
                zero_s = tmp_zero_s;
                zero_l = tmp_zero_l;
                tmp_zero_l = 0;
            }
            i += 2;
        }

        if tmp_zero_l != 0 && zero_l < tmp_zero_l {
            zero_s = tmp_zero_s;
            zero_l = tmp_zero_l;
        }

        // C condition (operator precedence preserved):
        //   zero_l != words && zero_s == 0 &&
        //   ( zero_l == 6
        //     || ( (zero_l == 5 && s[10] == 0xff && s[11] == 0xff)
        //          || (zero_l == 7 && s[14] != 0 && s[15] != 1) ) )
        let is_ipv4 = zero_l != words
            && zero_s == 0
            && (zero_l == 6
                || ((zero_l == 5 && inbuf[10] == 0xff && inbuf[11] == 0xff)
                    || (zero_l == 7 && inbuf[14] != 0 && inbuf[15] != 1)));

        // Format whole words.
        format_words(&mut outbuf, &inbuf, &mut si, words, zero_s, zero_l, is_ipv4, bits);
    }

    // Format CIDR /width.  C: (void) SPRINTF((cp, "/%u", bits));
    outbuf.push(b'/');
    push_u(&mut outbuf, bits as u32);

    Ok(outbuf)
}

/// The IPv6 whole-words formatting loop.
#[allow(clippy::too_many_arguments)]
fn format_words(
    outbuf: &mut Vec<u8>,
    inbuf: &[u8; 16],
    si: &mut usize,
    words: i32,
    zero_s: i32,
    zero_l: i32,
    is_ipv4: bool,
    bits: i32,
) {
    let mut p = 0;
    while p < words {
        if zero_l != 0 && p >= zero_s && p < zero_s + zero_l {
            // Time to skip some zeros.
            if p == zero_s {
                outbuf.push(b':');
            }
            if p == words - 1 {
                outbuf.push(b':');
            }
            *si += 1;
            *si += 1;
            p += 1;
            continue;
        }

        if is_ipv4 && p > 5 {
            outbuf.push(if p == 6 { b':' } else { b'.' });
            push_u(outbuf, inbuf[*si] as u32);
            *si += 1;
            // we can potentially drop the last octet
            if p != 7 || bits > 120 {
                outbuf.push(b'.');
                push_u(outbuf, inbuf[*si] as u32);
                *si += 1;
            }
        } else {
            if !outbuf.is_empty() {
                outbuf.push(b':');
            }
            // SPRINTF((cp, "%x", *s * 256 + s[1]))
            push_x(outbuf, inbuf[*si] as u32 * 256 + inbuf[*si + 1] as u32);
            *si += 2;
        }
        p += 1;
    }
}

/// Equivalent of `sprintf(dst + at, "%u", v)`: writes the decimal form of `v` at
/// byte offset `at`, then writes a trailing NUL, and returns the digit count (the
/// C `SPRINTF` return value; the NUL is not counted).
fn write_u(dst: &mut [u8], at: usize, v: u32) -> usize {
    let n = write_decimal(dst, at, v);
    dst[at + n] = b'\0';
    n
}

/// Render `v` in decimal into `dst` starting at `at`; returns the digit count. No
/// NUL is written (used as a building block).
fn write_decimal(dst: &mut [u8], at: usize, v: u32) -> usize {
    if v == 0 {
        dst[at] = b'0';
        return 1;
    }
    let mut tmp = [0u8; 10];
    let mut n = 0;
    let mut x = v;
    while x != 0 {
        tmp[n] = b'0' + (x % 10) as u8;
        x /= 10;
        n += 1;
    }
    for k in 0..n {
        dst[at + k] = tmp[n - 1 - k];
    }
    n
}

/// `sprintf(cp, "%u", v)` appended to the IPv6 scratch buffer.
fn push_u(buf: &mut Vec<u8>, v: u32) {
    if v == 0 {
        buf.push(b'0');
        return;
    }
    let mut tmp = [0u8; 10];
    let mut n = 0;
    let mut x = v;
    while x != 0 {
        tmp[n] = b'0' + (x % 10) as u8;
        x /= 10;
        n += 1;
    }
    for k in (0..n).rev() {
        buf.push(tmp[k]);
    }
}

/// `sprintf(cp, "%x", v)` appended to the IPv6 scratch buffer (lower-case hex, no
/// leading zeros — matching C's `%x`).
fn push_x(buf: &mut Vec<u8>, v: u32) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    if v == 0 {
        buf.push(b'0');
        return;
    }
    let mut tmp = [0u8; 8];
    let mut n = 0;
    let mut x = v;
    while x != 0 {
        tmp[n] = HEX[(x & 0xf) as usize];
        x >>= 4;
        n += 1;
    }
    for k in (0..n).rev() {
        buf.push(tmp[k]);
    }
}

#[cfg(test)]
mod ntop_tests {
    use super::*;

    const AF_INET: i32 = PGSQL_AF_INET as i32;
    const AF_INET6: i32 = PGSQL_AF_INET6 as i32;

    fn ntop(af: i32, src: &[u8], bits: i32) -> Result<String, InetCidrNtopError> {
        let mut dst = [0u8; 64];
        let n = pg_inet_cidr_ntop(af, src, bits, &mut dst)?;
        Ok(String::from_utf8(dst[..n].to_vec()).unwrap())
    }

    #[test]
    fn ipv4_full_octets() {
        assert_eq!(ntop(AF_INET, &[192, 5, 5, 0], 24).unwrap(), "192.5.5/24");
    }

    #[test]
    fn ipv4_partial_octet() {
        assert_eq!(ntop(AF_INET, &[192, 5, 5, 240], 28).unwrap(), "192.5.5.240/28");
    }

    #[test]
    fn ipv4_zero_bits() {
        assert_eq!(ntop(AF_INET, &[0, 0, 0, 0], 0).unwrap(), "0/0");
    }

    #[test]
    fn ipv4_partial_first_octet() {
        assert_eq!(ntop(AF_INET, &[128, 0, 0, 0], 8).unwrap(), "128/8");
        assert_eq!(ntop(AF_INET, &[0xC0, 0, 0, 0], 4).unwrap(), "192/4");
    }

    #[test]
    fn ipv4_invalid_bits() {
        assert_eq!(ntop(AF_INET, &[1, 2, 3, 4], 33), Err(InetCidrNtopError::InvalidArgument));
        assert_eq!(ntop(AF_INET, &[1, 2, 3, 4], -1), Err(InetCidrNtopError::InvalidArgument));
    }

    #[test]
    fn ipv6_zero_bits() {
        assert_eq!(ntop(AF_INET6, &[0u8; 16], 0).unwrap(), "::/0");
    }

    #[test]
    fn ipv6_simple() {
        let mut addr = [0u8; 16];
        addr[0] = 0x20;
        addr[1] = 0x01;
        addr[2] = 0x0d;
        addr[3] = 0xb8;
        assert_eq!(ntop(AF_INET6, &addr, 32).unwrap(), "2001:db8/32");
    }

    #[test]
    fn ipv6_full_128() {
        let addr = [0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];
        assert_eq!(ntop(AF_INET6, &addr, 128).unwrap(), "2001:db8::1/128");
    }

    #[test]
    fn ipv6_embedded_ipv4_mapped() {
        let addr = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 1, 2, 3, 4];
        assert_eq!(ntop(AF_INET6, &addr, 128).unwrap(), "::ffff:1.2.3.4/128");
    }

    #[test]
    fn ipv6_regress_golden() {
        let addr = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 3, 2, 1];
        assert_eq!(ntop(AF_INET6, &addr, 24).unwrap(), "::/24");

        let addr = [0x00, 0x10, 0x00, 0x23, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff];
        assert_eq!(ntop(AF_INET6, &addr, 128).unwrap(), "10:23::ffff/128");

        let addr = [0x00, 0x10, 0x00, 0x23, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xf1];
        assert_eq!(ntop(AF_INET6, &addr, 64).unwrap(), "10:23::/64");
    }

    #[test]
    fn buffer_too_small() {
        let mut dst = [0u8; 4];
        assert_eq!(
            pg_inet_cidr_ntop(AF_INET, &[192, 5, 5, 240], 28, &mut dst),
            Err(InetCidrNtopError::MessageTooLong)
        );
    }

    #[test]
    fn unsupported_family() {
        assert_eq!(ntop(9999, &[0u8; 16], 0), Err(InetCidrNtopError::AddressFamilyNotSupported));
    }
}
