//! Port of `src/backend/utils/adt/inet_net_pton.c` — convert a network number
//! from presentation (text) form to network (binary) form. Accepts hex octets,
//! hex strings, decimal octets, and `/CIDR`.
//!
//! Every function in `inet_net_pton.c` is ported here with logic identical to
//! the PostgreSQL 18.3 C source (branch order and accepted grammar preserved).
//! The ISC code returns `-1` on error and sets `errno` (`EAFNOSUPPORT` /
//! `ENOENT` / `EMSGSIZE`); this port mirrors that via [`InetNetPtonError`] whose
//! [`InetNetPtonError::errno`] gives the same value.
//!
//! `pg_inet_net_pton` takes a signed `size`: `size == -1` selects the lenient
//! "host address with included netmask" parsers, any other value selects the
//! strict CIDR parsers with `size` as the `dst` byte budget. The canonical
//! `network.c` caller always passes a 16-byte `dst` with `size` of 4/16 or `-1`,
//! so the over-budget heap fallback (a `size > 16` non-canonical caller) is never
//! taken; it exists only so a mis-wired caller cannot overflow.

use core::error::Error;
use core::fmt;

use types_network::{PGSQL_AF_INET, PGSQL_AF_INET6};

/// `NS_IN6ADDRSZ` — size of an IPv6 address in bytes.
pub const NS_IN6ADDRSZ: usize = 16;
/// `NS_INT16SZ` — size of a 16-bit IPv6 word in bytes.
pub const NS_INT16SZ: usize = 2;
/// `NS_INADDRSZ` — size of an IPv4 address in bytes.
pub const NS_INADDRSZ: usize = 4;

/// POSIX `errno` values mirrored by [`InetNetPtonError::errno`]. We carry the
/// numeric values matching the build target's `libc` `E*` macros rather than
/// pulling in `libc`. `ENOENT` is `2` on every POSIX target; `EMSGSIZE` /
/// `EAFNOSUPPORT` differ between Linux and the BSDs.
mod errno {
    pub const ENOENT: i32 = 2;

    #[cfg(target_os = "linux")]
    pub const EMSGSIZE: i32 = 90;
    #[cfg(target_os = "linux")]
    pub const EAFNOSUPPORT: i32 = 97;

    #[cfg(any(target_os = "macos", target_os = "ios"))]
    pub const EMSGSIZE: i32 = 40;
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    pub const EAFNOSUPPORT: i32 = 47;

    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "ios")))]
    pub const EMSGSIZE: i32 = 90;
    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "ios")))]
    pub const EAFNOSUPPORT: i32 = 97;
}

/// Error returned by [`pg_inet_net_pton`] and its helpers; mirrors the ISC
/// `errno` values.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InetNetPtonError {
    /// C: `errno = EAFNOSUPPORT` — unknown address family.
    AddressFamilyNotSupported,
    /// C: `errno = ENOENT` — not a valid network specification.
    NotFound,
    /// C: `errno = EMSGSIZE` — destination buffer too small / prefix too wide.
    MessageTooLong,
}

impl InetNetPtonError {
    /// The `errno` value the C code would have set for this failure.
    pub const fn errno(self) -> i32 {
        match self {
            Self::AddressFamilyNotSupported => errno::EAFNOSUPPORT,
            Self::NotFound => errno::ENOENT,
            Self::MessageTooLong => errno::EMSGSIZE,
        }
    }
}

impl fmt::Display for InetNetPtonError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AddressFamilyNotSupported => f.write_str("address family not supported"),
            Self::NotFound => f.write_str("invalid network specification"),
            Self::MessageTooLong => f.write_str("destination buffer is too small"),
        }
    }
}

impl Error for InetNetPtonError {}

/// C `isdigit((unsigned char) ch)` over the ASCII `'0'..='9'` set. The catalog
/// text functions run under the C locale, so the ASCII set is exact.
#[inline]
fn is_digit(ch: i32) -> bool {
    (b'0' as i32..=b'9' as i32).contains(&ch)
}

/// C `isxdigit((unsigned char) ch)` over the ASCII hex set.
#[inline]
fn is_xdigit(ch: i32) -> bool {
    (b'0' as i32..=b'9' as i32).contains(&ch)
        || (b'a' as i32..=b'f' as i32).contains(&ch)
        || (b'A' as i32..=b'F' as i32).contains(&ch)
}

/// C `isupper((unsigned char) ch)` over the ASCII upper-case set.
#[inline]
fn is_upper(ch: i32) -> bool {
    (b'A' as i32..=b'Z' as i32).contains(&ch)
}

/// C `tolower((unsigned char) ch)` for ASCII.
#[inline]
fn to_lower(ch: i32) -> i32 {
    if is_upper(ch) {
        ch + (b'a' as i32 - b'A' as i32)
    } else {
        ch
    }
}

/// `strchr(xdigits, ch) - xdigits` for the lower-case hex digit table. Returns
/// the nibble value `0..=15`, or `None` if `ch` is not a lower-case hex digit.
#[inline]
fn xdigit_value(ch: i32) -> Option<i32> {
    const XDIGITS: &[u8; 16] = b"0123456789abcdef";
    XDIGITS.iter().position(|&c| c as i32 == ch).map(|p| p as i32)
}

/// Fetch byte `i` of `src` as the C code's `*src` would: positions past the
/// logical end of the string read the implicit NUL terminator (`0`).
#[inline]
fn at(src: &[u8], i: usize) -> i32 {
    if i < src.len() {
        src[i] as i32
    } else {
        0
    }
}

/// C: `pg_inet_net_pton(af, src, dst, size)`.
///
/// On success returns the number of bits; on failure the matching
/// [`InetNetPtonError`]. All parsers write into a private 16-byte scratch (the
/// ISC contract that `dst` is the physical 16-byte `ipaddr` field), then the
/// parsed bytes are copied back into `dst` clamped to `dst.len()`. The canonical
/// 16-byte caller is byte-for-byte identical to the C; a shorter `dst` receives
/// only its leading bytes rather than panicking.
pub fn pg_inet_net_pton(
    af: i32,
    src: &str,
    dst: &mut [u8],
    size: isize,
) -> Result<i32, InetNetPtonError> {
    let src = src.as_bytes();
    let mut stack = [0u8; NS_IN6ADDRSZ];

    let needed = match size {
        -1 => NS_IN6ADDRSZ,
        _ => (size as usize).max(NS_IN6ADDRSZ),
    };

    if needed <= NS_IN6ADDRSZ {
        // Canonical path: fixed-size stack scratch, no heap allocation.
        return dispatch_into(af, src, size, &mut stack, dst);
    }

    // Over-budget fallback (non-canonical `size > 16`, which the C code never
    // receives): the parse target must hold up to `size` bytes, so use a heap
    // scratch. C allocates this in CurrentMemoryContext and frees before
    // returning; the local Vec is the faithful analog (dropped on return).
    //
    // Cap against PG's MaxAllocSize (1 GB) rather than aborting on OOM.
    const MAX_ALLOC_SIZE: usize = 0x3fff_ffff;
    if needed > MAX_ALLOC_SIZE {
        return Err(InetNetPtonError::MessageTooLong);
    }
    let mut heap = vec![0u8; needed];
    dispatch_into(af, src, size, &mut heap, dst)
}

/// Dispatch to the family-specific parser, writing into `scratch`, then copy the
/// parsed bytes back into the caller's `dst`. `scratch` must be at least
/// [`NS_IN6ADDRSZ`] bytes and large enough for the chosen parser's `size` budget.
fn dispatch_into(
    af: i32,
    src: &[u8],
    size: isize,
    scratch: &mut [u8],
    dst: &mut [u8],
) -> Result<i32, InetNetPtonError> {
    let result = match af {
        a if a == PGSQL_AF_INET as i32 => {
            if size == -1 {
                inet_net_pton_ipv4(src, scratch)
            } else {
                inet_cidr_pton_ipv4(src, scratch, size as usize)
            }
        }
        a if a == PGSQL_AF_INET6 as i32 => {
            if size == -1 {
                inet_net_pton_ipv6(src, scratch)
            } else {
                inet_cidr_pton_ipv6(src, scratch, size as usize)
            }
        }
        _ => return Err(InetNetPtonError::AddressFamilyNotSupported),
    };
    if result.is_ok() {
        let n = dst.len().min(NS_IN6ADDRSZ);
        dst[..n].copy_from_slice(&scratch[..n]);
    }
    result
}

/// C: `static int inet_cidr_pton_ipv4(src, dst, size)`.
fn inet_cidr_pton_ipv4(src: &[u8], dst: &mut [u8], size: usize) -> Result<i32, InetNetPtonError> {
    let mut size = size;
    let mut dp: usize = 0;
    let odst: usize = 0;
    let mut si: usize = 0;
    let mut tmp: i32 = 0;
    let mut bits: i32;

    let mut ch = at(src, si);
    si += 1;

    if ch == b'0' as i32
        && (at(src, si) == b'x' as i32 || at(src, si) == b'X' as i32)
        && is_xdigit(at(src, si + 1))
    {
        // Hexadecimal: Eat nybble string.
        if size == 0 {
            return Err(InetNetPtonError::MessageTooLong);
        }
        let mut dirty = 0;
        si += 1; // skip x or X.
        loop {
            ch = at(src, si);
            si += 1;
            if !(ch != 0 && is_xdigit(ch)) {
                break;
            }
            if is_upper(ch) {
                ch = to_lower(ch);
            }
            let n = xdigit_value(ch).ok_or(InetNetPtonError::NotFound)?;
            if dirty == 0 {
                tmp = n;
            } else {
                tmp = (tmp << 4) | n;
            }
            dirty += 1;
            if dirty == 2 {
                if size == 0 {
                    return Err(InetNetPtonError::MessageTooLong);
                }
                size -= 1;
                dst[dp] = tmp as u8;
                dp += 1;
                dirty = 0;
            }
        }
        if dirty != 0 {
            // Odd trailing nybble?
            if size == 0 {
                return Err(InetNetPtonError::MessageTooLong);
            }
            size -= 1;
            dst[dp] = (tmp << 4) as u8;
            dp += 1;
        }
    } else if is_digit(ch) {
        // Decimal: eat dotted digit string.
        loop {
            tmp = 0;
            loop {
                let n = ch - b'0' as i32;
                tmp *= 10;
                tmp += n;
                if tmp > 255 {
                    return Err(InetNetPtonError::NotFound);
                }
                ch = at(src, si);
                si += 1;
                if !(ch != 0 && is_digit(ch)) {
                    break;
                }
            }
            if size == 0 {
                return Err(InetNetPtonError::MessageTooLong);
            }
            size -= 1;
            dst[dp] = tmp as u8;
            dp += 1;
            if ch == 0 || ch == b'/' as i32 {
                break;
            }
            if ch != b'.' as i32 {
                return Err(InetNetPtonError::NotFound);
            }
            ch = at(src, si);
            si += 1;
            if !is_digit(ch) {
                return Err(InetNetPtonError::NotFound);
            }
        }
    } else {
        return Err(InetNetPtonError::NotFound);
    }

    bits = -1;
    if ch == b'/' as i32 && is_digit(at(src, si)) && dp > odst {
        // CIDR width specifier.  Nothing can follow it.
        ch = at(src, si); // Skip over the /.
        si += 1;
        bits = 0;
        loop {
            let n = ch - b'0' as i32;
            // C accumulates over a plain `int` and only tests `bits > 32` after
            // the loop; an over-long run would overflow (UB). Saturating
            // arithmetic leaves accepted values (`<= 32`) unchanged and forces an
            // over-long run above 32, so the same EMSGSIZE rejection fires.
            bits = bits.saturating_mul(10).saturating_add(n);
            ch = at(src, si);
            si += 1;
            if !(ch != 0 && is_digit(ch)) {
                break;
            }
        }
        if ch != 0 {
            return Err(InetNetPtonError::NotFound);
        }
        if bits > 32 {
            return Err(InetNetPtonError::MessageTooLong);
        }
    }

    // Fiery death and destruction unless we prefetched EOS.
    if ch != 0 {
        return Err(InetNetPtonError::NotFound);
    }

    // If nothing was written to the destination, we found no address.
    if dp == odst {
        return Err(InetNetPtonError::NotFound);
    }
    // If no CIDR spec was given, infer width from net class.
    if bits == -1 {
        let first = dst[odst] as i32;
        if first >= 240 {
            // Class E
            bits = 32;
        } else if first >= 224 {
            // Class D
            bits = 8;
        } else if first >= 192 {
            // Class C
            bits = 24;
        } else if first >= 128 {
            // Class B
            bits = 16;
        } else {
            // Class A
            bits = 8;
        }
        // If imputed mask is narrower than specified octets, widen.
        if (bits as isize) < ((dp - odst) as isize) * 8 {
            bits = ((dp - odst) * 8) as i32;
        }
        // If there are no additional bits specified for a class D address adjust
        // bits to 4.
        if bits == 8 && dst[odst] as i32 == 224 {
            bits = 4;
        }
    }
    // Extend network to cover the actual mask.
    while bits as isize > ((dp - odst) as isize) * 8 {
        if size == 0 {
            return Err(InetNetPtonError::MessageTooLong);
        }
        size -= 1;
        dst[dp] = b'\0';
        dp += 1;
    }
    Ok(bits)
}

/// C: `static int inet_net_pton_ipv4(src, dst)`.
fn inet_net_pton_ipv4(src: &[u8], dst: &mut [u8]) -> Result<i32, InetNetPtonError> {
    let mut dp: usize = 0;
    let odst: usize = 0;
    let mut si: usize = 0;
    let mut tmp: i32;
    let mut bits: i32;
    let mut size: usize = 4;

    // Get the mantissa.
    let mut ch;
    loop {
        ch = at(src, si);
        si += 1;
        if !is_digit(ch) {
            break;
        }
        tmp = 0;
        loop {
            let n = ch - b'0' as i32;
            tmp *= 10;
            tmp += n;
            if tmp > 255 {
                return Err(InetNetPtonError::NotFound);
            }
            ch = at(src, si);
            si += 1;
            if !(ch != 0 && is_digit(ch)) {
                break;
            }
        }
        if size == 0 {
            return Err(InetNetPtonError::MessageTooLong);
        }
        size -= 1;
        dst[dp] = tmp as u8;
        dp += 1;
        if ch == 0 || ch == b'/' as i32 {
            break;
        }
        if ch != b'.' as i32 {
            return Err(InetNetPtonError::NotFound);
        }
    }

    // Get the prefix length if any.
    bits = -1;
    if ch == b'/' as i32 && is_digit(at(src, si)) && dp > odst {
        // CIDR width specifier.  Nothing can follow it.
        ch = at(src, si); // Skip over the /.
        si += 1;
        bits = 0;
        loop {
            let n = ch - b'0' as i32;
            bits = bits.saturating_mul(10).saturating_add(n);
            ch = at(src, si);
            si += 1;
            if !(ch != 0 && is_digit(ch)) {
                break;
            }
        }
        if ch != 0 {
            return Err(InetNetPtonError::NotFound);
        }
        if bits > 32 {
            return Err(InetNetPtonError::MessageTooLong);
        }
    }

    // Fiery death and destruction unless we prefetched EOS.
    if ch != 0 {
        return Err(InetNetPtonError::NotFound);
    }

    // Prefix length can default to /32 only if all four octets spec'd.
    if bits == -1 {
        if dp - odst == 4 {
            bits = 32;
        } else {
            return Err(InetNetPtonError::NotFound);
        }
    }

    // If nothing was written to the destination, we found no address.
    if dp == odst {
        return Err(InetNetPtonError::NotFound);
    }

    // If prefix length overspecifies mantissa, life is bad.
    if (bits / 8) as isize > (dp - odst) as isize {
        return Err(InetNetPtonError::NotFound);
    }

    // Extend address to four octets.
    while size > 0 {
        size -= 1;
        dst[dp] = 0;
        dp += 1;
    }

    Ok(bits)
}

/// C: `static int getbits(src, *bitsp)`. Parse a decimal prefix-length suffix.
/// Rejects leading zeros and values above 128. `Some(bits)` on success (C `1`),
/// `None` on failure (C `0`).
fn getbits(src: &[u8]) -> Option<i32> {
    let mut val: i32 = 0;
    let mut n: i32 = 0;
    let mut si: usize = 0;
    loop {
        let ch = at(src, si);
        si += 1;
        if ch == 0 {
            break;
        }
        if is_digit(ch) {
            let prev_n = n;
            n += 1;
            if prev_n != 0 && val == 0 {
                // no leading zeros
                return None;
            }
            val *= 10;
            val += ch - b'0' as i32;
            if val > 128 {
                // range
                return None;
            }
            continue;
        }
        return None;
    }
    if n == 0 {
        return None;
    }
    Some(val)
}

/// C: `static int getv4(src, dst, *bitsp)`. Parse an embedded dotted-decimal IPv4
/// address (for `::ffff:1.2.3.4` tails), optionally followed by `/bits`. Returns
/// `Some((written, bits))`: `written` octets stored, `bits` `Some` if a `/bits`
/// suffix was parsed. `None` on failure (C `0`).
fn getv4(src: &[u8], dst: &mut [u8]) -> Option<(usize, Option<i32>)> {
    let mut dp: usize = 0;
    let odst: usize = 0;
    let mut val: u32 = 0;
    let mut n: i32 = 0;
    let mut si: usize = 0;
    loop {
        let ch = at(src, si);
        si += 1;
        if ch == 0 {
            break;
        }
        if is_digit(ch) {
            let prev_n = n;
            n += 1;
            if prev_n != 0 && val == 0 {
                // no leading zeros
                return None;
            }
            val = val.wrapping_mul(10);
            val = val.wrapping_add((ch - b'0' as i32) as u32);
            if val > 255 {
                // range
                return None;
            }
            continue;
        }
        if ch == b'.' as i32 || ch == b'/' as i32 {
            if dp - odst > 3 {
                // too many octets?
                return None;
            }
            dst[dp] = val as u8;
            dp += 1;
            if ch == b'/' as i32 {
                let bits = getbits(&src[si..])?;
                return Some((dp, Some(bits)));
            }
            val = 0;
            n = 0;
            continue;
        }
        return None;
    }
    if n == 0 {
        return None;
    }
    if dp - odst > 3 {
        // too many octets?
        return None;
    }
    dst[dp] = val as u8;
    dp += 1;
    Some((dp, None))
}

/// C: `static int inet_net_pton_ipv6(src, dst)` — forwards to the CIDR parser
/// with `size = 16`.
fn inet_net_pton_ipv6(src: &[u8], dst: &mut [u8]) -> Result<i32, InetNetPtonError> {
    inet_cidr_pton_ipv6(src, dst, NS_IN6ADDRSZ)
}

/// C: `static int inet_cidr_pton_ipv6(src, dst, size)`.
//
// The `(... as u8) & 0xff` writes mirror the C `(u_char) (...) & 0xff`. On a `u8`
// the mask is a no-op; kept for source-level fidelity (clippy `identity_op`).
#[allow(clippy::identity_op)]
fn inet_cidr_pton_ipv6(src: &[u8], dst: &mut [u8], size: usize) -> Result<i32, InetNetPtonError> {
    if size < NS_IN6ADDRSZ {
        return Err(InetNetPtonError::MessageTooLong);
    }

    let mut tmp = [0u8; NS_IN6ADDRSZ];
    let mut tp: usize = 0; // index into tmp (C `tp`)
    let mut endp: usize = NS_IN6ADDRSZ; // C `endp = tp + NS_IN6ADDRSZ`
    let mut colonp: Option<usize> = None; // C `colonp` (NULL == None)

    let mut si: usize = 0; // index into src (C `*src++`)

    // Leading :: requires some special handling.
    if at(src, si) == b':' as i32 {
        si += 1;
        if at(src, si) != b':' as i32 {
            return Err(InetNetPtonError::NotFound);
        }
    }

    let mut curtok: usize = si; // C `curtok = src`
    let mut saw_xdigit = 0;
    let mut val: u32 = 0;
    let mut digits = 0;
    let mut bits: i32 = -1;

    loop {
        let ch = at(src, si);
        si += 1;
        if ch == 0 {
            break;
        }

        if let Some(nibble) = xdigit_nibble(ch) {
            val <<= 4;
            val |= nibble;
            digits += 1;
            if digits > 4 {
                return Err(InetNetPtonError::NotFound);
            }
            saw_xdigit = 1;
            continue;
        }
        if ch == b':' as i32 {
            curtok = si;
            if saw_xdigit == 0 {
                if colonp.is_some() {
                    return Err(InetNetPtonError::NotFound);
                }
                colonp = Some(tp);
                continue;
            } else if at(src, si) == 0 {
                return Err(InetNetPtonError::NotFound);
            }
            if tp + NS_INT16SZ > endp {
                return Err(InetNetPtonError::NotFound);
            }
            tmp[tp] = ((val >> 8) as u8) & 0xff;
            tp += 1;
            tmp[tp] = (val as u8) & 0xff;
            tp += 1;
            saw_xdigit = 0;
            digits = 0;
            val = 0;
            continue;
        }
        if ch == b'.' as i32 && (tp + NS_INADDRSZ) <= endp {
            if let Some((written, v4bits)) = getv4(&src[curtok..], &mut tmp[tp..]) {
                let _ = written; // C advances tp by NS_INADDRSZ unconditionally
                if let Some(b) = v4bits {
                    bits = b;
                }
                tp += NS_INADDRSZ;
                saw_xdigit = 0;
                break; // '\0' was seen by inet_pton4().
            }
        }
        if ch == b'/' as i32 {
            if let Some(b) = getbits(&src[si..]) {
                bits = b;
                break;
            }
        }
        return Err(InetNetPtonError::NotFound);
    }

    if saw_xdigit != 0 {
        if tp + NS_INT16SZ > endp {
            return Err(InetNetPtonError::NotFound);
        }
        tmp[tp] = ((val >> 8) as u8) & 0xff;
        tp += 1;
        tmp[tp] = (val as u8) & 0xff;
        tp += 1;
    }
    if bits == -1 {
        bits = 128;
    }

    endp = NS_IN6ADDRSZ; // C `endp = tmp + 16;`

    if let Some(colon) = colonp {
        // Some memmove()'s mishandle overlap, so do the shift by hand.
        let n = tp - colon;
        if tp == endp {
            return Err(InetNetPtonError::NotFound);
        }
        let mut i = 1;
        while i <= n {
            tmp[endp - i] = tmp[colon + (n - i)];
            tmp[colon + (n - i)] = 0;
            i += 1;
        }
        tp = endp;
    }
    if tp != endp {
        return Err(InetNetPtonError::NotFound);
    }

    // Copy out the result.
    dst[..NS_IN6ADDRSZ].copy_from_slice(&tmp);

    Ok(bits)
}

/// `strchr(xdigits_l, ch)` then `strchr(xdigits_u, ch)`, returning the nibble
/// value with the same precedence the ISC IPv6 loop uses (`'A'`/`'a'` both map to
/// `0xa`). `None` when `ch` is not a hex digit.
#[inline]
fn xdigit_nibble(ch: i32) -> Option<u32> {
    const XDIGITS_L: &[u8; 16] = b"0123456789abcdef";
    const XDIGITS_U: &[u8; 16] = b"0123456789ABCDEF";
    if let Some(p) = XDIGITS_L.iter().position(|&c| c as i32 == ch) {
        return Some(p as u32);
    }
    XDIGITS_U
        .iter()
        .position(|&c| c as i32 == ch)
        .map(|p| p as u32)
}

#[cfg(test)]
mod pton_tests {
    use super::*;

    const AF_INET: i32 = PGSQL_AF_INET as i32;
    const AF_INET6: i32 = PGSQL_AF_INET6 as i32;

    #[test]
    fn ipv4_net_full_dotted_quad_defaults_32() {
        let mut dst = [0u8; 4];
        let bits = pg_inet_net_pton(AF_INET, "192.168.1.2", &mut dst, -1).unwrap();
        assert_eq!(bits, 32);
        assert_eq!(dst, [192, 168, 1, 2]);
    }

    #[test]
    fn ipv4_net_with_cidr() {
        let mut dst = [0u8; 4];
        let bits = pg_inet_net_pton(AF_INET, "10.0.0.0/8", &mut dst, -1).unwrap();
        assert_eq!(bits, 8);
        assert_eq!(dst, [10, 0, 0, 0]);
    }

    #[test]
    fn ipv4_net_partial_requires_cidr() {
        let mut dst = [0u8; 4];
        assert_eq!(
            pg_inet_net_pton(AF_INET, "10.0", &mut dst, -1),
            Err(InetNetPtonError::NotFound)
        );
        let mut dst = [0u8; 4];
        let bits = pg_inet_net_pton(AF_INET, "10.0/16", &mut dst, -1).unwrap();
        assert_eq!(bits, 16);
        assert_eq!(dst, [10, 0, 0, 0]);
    }

    #[test]
    fn ipv4_cidr_class_inference() {
        let mut dst = [0u8; 4];
        let bits = pg_inet_net_pton(AF_INET, "192", &mut dst, 4).unwrap();
        assert_eq!(bits, 24);
        assert_eq!(&dst[..3], &[192, 0, 0]);

        let mut dst = [0u8; 4];
        let bits = pg_inet_net_pton(AF_INET, "224", &mut dst, 4).unwrap();
        assert_eq!(bits, 4);
        assert_eq!(dst[0], 224);

        let mut dst = [0u8; 4];
        let bits = pg_inet_net_pton(AF_INET, "240", &mut dst, 4).unwrap();
        assert_eq!(bits, 32);
        assert_eq!(dst, [240, 0, 0, 0]);
    }

    #[test]
    fn ipv4_cidr_hex_string() {
        let mut dst = [0u8; 4];
        let bits = pg_inet_net_pton(AF_INET, "0xC0A80101", &mut dst, 4).unwrap();
        assert_eq!(bits, 32);
        assert_eq!(dst, [192, 168, 1, 1]);
    }

    #[test]
    fn ipv6_full() {
        let mut dst = [0u8; 16];
        let bits = pg_inet_net_pton(AF_INET6, "2001:db8::1", &mut dst, -1).unwrap();
        assert_eq!(bits, 128);
        assert_eq!(dst, [0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
    }

    #[test]
    fn ipv6_embedded_ipv4() {
        let mut dst = [0u8; 16];
        let bits = pg_inet_net_pton(AF_INET6, "::ffff:1.2.3.4", &mut dst, -1).unwrap();
        assert_eq!(bits, 128);
        assert_eq!(dst, [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 1, 2, 3, 4]);
    }

    #[test]
    fn ipv6_double_compression_rejected() {
        let mut dst = [0u8; 16];
        assert_eq!(
            pg_inet_net_pton(AF_INET6, "1::2::3", &mut dst, -1),
            Err(InetNetPtonError::NotFound)
        );
    }

    #[test]
    fn ipv4_cidr_overlong_suffix_does_not_panic() {
        let mut dst = [0u8; 4];
        assert_eq!(
            pg_inet_net_pton(AF_INET, "1.2.3.4/99999999999999", &mut dst, 4),
            Err(InetNetPtonError::MessageTooLong)
        );
    }

    #[test]
    fn unsupported_family() {
        let mut dst = [0u8; 16];
        assert_eq!(
            pg_inet_net_pton(9999, "1.2.3.4", &mut dst, -1),
            Err(InetNetPtonError::AddressFamilyNotSupported)
        );
    }

    #[test]
    fn oversize_budget_does_not_panic() {
        let mut dst = [0u8; 4];
        let bits = pg_inet_net_pton(AF_INET, "1.2.3.4/32", &mut dst, 64).unwrap();
        assert_eq!(bits, 32);
        assert_eq!(dst, [1, 2, 3, 4]);
    }
}
