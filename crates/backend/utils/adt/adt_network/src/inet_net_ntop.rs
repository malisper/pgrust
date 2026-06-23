//! Port of `src/port/inet_net_ntop.c` — convert a host/network address from
//! network (binary) form to presentation (text) format.
//!
//! Every function of the C translation unit is ported 1:1 (ISC code, Paul Vixie
//! 1998; PostgreSQL 18.3 copy). The C functions return `NULL` on error with
//! `errno` set (`EINVAL` / `EMSGSIZE` / `EAFNOSUPPORT`); this port returns `None`
//! for every error arm (the only caller, `network.c`, distinguishes NULL vs
//! not). The byte-level output — octet rendering, `::` shorthanding,
//! encapsulated-IPv4 tails and the `/bits` suffix rules — is preserved exactly,
//! including the `sizeof`-based buffer-capacity checks so an undersized `dst`
//! fails on the same boundaries the C does.

use types_network::{PGSQL_AF_INET, PGSQL_AF_INET6};

/// `NS_IN6ADDRSZ` (16) / `NS_INT16SZ` (2).
const NS_IN6ADDRSZ: usize = 16;
const NS_INT16SZ: usize = 2;

/// `sprintf(dst+d, "%u", v)` for a `u_char` octet value: write the decimal
/// digits at `d`, plus a trailing NUL (sprintf always NUL-terminates). Returns
/// the digit count (the C `SPRINTF` value). The caller has already performed the
/// C size check guaranteeing room.
fn sprintf_u(dst: &mut [u8], d: usize, v: u32) -> usize {
    let s = itoa(v);
    dst[d..d + s.len()].copy_from_slice(&s);
    if d + s.len() < dst.len() {
        dst[d + s.len()] = 0;
    }
    s.len()
}

/// `sprintf(dst+d, "%x", v)`.
fn sprintf_x(dst: &mut [u8], d: usize, v: u32) -> usize {
    let mut buf = [0u8; 8];
    let mut n = 0usize;
    let mut x = v;
    if x == 0 {
        buf[0] = b'0';
        n = 1;
    } else {
        let mut tmp = [0u8; 8];
        let mut t = 0;
        while x > 0 {
            tmp[t] = b"0123456789abcdef"[(x & 0xf) as usize];
            x >>= 4;
            t += 1;
        }
        while t > 0 {
            t -= 1;
            buf[n] = tmp[t];
            n += 1;
        }
    }
    dst[d..d + n].copy_from_slice(&buf[..n]);
    if d + n < dst.len() {
        dst[d + n] = 0;
    }
    n
}

/// Decimal digits of `v` (`u32` max 10 digits).
fn itoa(v: u32) -> Vec<u8> {
    let mut out = Vec::with_capacity(10);
    let mut x = v;
    if x == 0 {
        out.push(b'0');
        return out;
    }
    let mut tmp = [0u8; 10];
    let mut t = 0;
    while x > 0 {
        tmp[t] = b'0' + (x % 10) as u8;
        x /= 10;
        t += 1;
    }
    while t > 0 {
        t -= 1;
        out.push(tmp[t]);
    }
    out
}

/// C: `pg_inet_net_ntop(af, src, bits, dst, size)`.
///
/// Returns `Some(len)` — the number of text bytes written at `dst[..len]`
/// (`dst[len]` is NUL when it fits, per sprintf semantics) — or `None` for the C
/// `NULL` (errno EINVAL / EMSGSIZE / EAFNOSUPPORT) arms.
pub fn pg_inet_net_ntop(af: i32, src: &[u8], bits: i32, dst: &mut [u8]) -> Option<usize> {
    if af == PGSQL_AF_INET as i32 {
        inet_net_ntop_ipv4(src, bits, dst)
    } else if af == PGSQL_AF_INET6 as i32 {
        inet_net_ntop_ipv6(src, bits, dst)
    } else {
        None // errno = EAFNOSUPPORT; return (NULL);
    }
}

/// C: `static char *inet_net_ntop_ipv4(src, bits, dst, size)`.
///
/// Network byte order assumed; always formats all four octets regardless of mask
/// length, then `/bits` unless `bits == 32`.
fn inet_net_ntop_ipv4(src: &[u8], bits: i32, dst: &mut [u8]) -> Option<usize> {
    let odst = 0usize;
    let mut d = 0usize; // the walking `dst` pointer
    let mut size = dst.len();
    let mut si = 0usize; // `*src++`

    if !(0..=32).contains(&bits) {
        return None; // errno = EINVAL
    }

    // Always format all four octets, regardless of mask length.
    let len = 4;
    for _b in (1..=len).rev() {
        // if (size <= sizeof ".255") goto emsgsize;   (sizeof ".255" == 5)
        if size <= 5 {
            return None; // errno = EMSGSIZE
        }
        let t = d;
        if d != odst {
            dst[d] = b'.';
            d += 1;
        }
        if si >= src.len() {
            // C reads out of bounds here; a short slice is a caller bug — fail
            // like EMSGSIZE rather than panic (callers pass 4+ bytes).
            return None;
        }
        d += sprintf_u(dst, d, src[si] as u32);
        si += 1;
        size -= d - t;
    }

    // don't print masklen if 32 bits
    if bits != 32 {
        // if (size <= sizeof "/32") goto emsgsize;   (sizeof "/32" == 4)
        if size <= 4 {
            return None; // errno = EMSGSIZE
        }
        dst[d] = b'/';
        d += 1;
        d += sprintf_u(dst, d, bits as u32);
    }

    Some(d)
}

/// C: `static int decoct(src, bytes, dst, size)` — the dotted-decimal tail of an
/// encapsulated-IPv4 IPv6 address. Returns 0 on EMSGSIZE (the C return 0), else
/// the bytes written.
fn decoct(src: &[u8], bytes: usize, dst: &mut [u8], d0: usize) -> usize {
    let odst = d0;
    let mut d = d0;
    let mut size = dst.len() - d0;
    let mut si = 0usize;

    for b in 1..=bytes {
        // if (size <= sizeof "255.") return (0);   (sizeof "255." == 5)
        if size <= 5 {
            return 0;
        }
        let t = d;
        d += sprintf_u(dst, d, src[si] as u32);
        si += 1;
        if b != bytes {
            dst[d] = b'.';
            d += 1;
            if d < dst.len() {
                dst[d] = 0;
            }
        }
        size -= d - t;
    }
    d - odst
}

/// C: `static char *inet_net_ntop_ipv6(src, bits, dst, size)`.
fn inet_net_ntop_ipv6(src: &[u8], bits: i32, dst: &mut [u8]) -> Option<usize> {
    // char tmp[sizeof "ffff:ffff:ffff:ffff:ffff:ffff:255.255.255.255/128"];
    let mut tmp = [0u8; 64];
    let mut tp = 0usize;

    if !(-1..=128).contains(&bits) {
        return None; // errno = EINVAL
    }
    if src.len() < NS_IN6ADDRSZ {
        return None; // C would read out of bounds; loud-safe refusal
    }

    // Copy the input (bytewise) array into a wordwise array. Find the longest run
    // of 0x00's in src[] for :: shorthanding.
    let nwords = NS_IN6ADDRSZ / NS_INT16SZ;
    let mut words = [0u32; NS_IN6ADDRSZ / NS_INT16SZ];
    for i in 0..NS_IN6ADDRSZ {
        words[i / 2] |= (src[i] as u32) << ((1 - (i % 2)) << 3);
    }
    let mut best_base: i32 = -1;
    let mut best_len: i32 = 0;
    let mut cur_base: i32 = -1;
    let mut cur_len: i32 = 0;
    for (i, w) in words.iter().enumerate().take(nwords) {
        if *w == 0 {
            if cur_base == -1 {
                cur_base = i as i32;
                cur_len = 1;
            } else {
                cur_len += 1;
            }
        } else if cur_base != -1 {
            if best_base == -1 || cur_len > best_len {
                best_base = cur_base;
                best_len = cur_len;
            }
            cur_base = -1;
        }
    }
    if cur_base != -1 && (best_base == -1 || cur_len > best_len) {
        best_base = cur_base;
        best_len = cur_len;
    }
    if best_base != -1 && best_len < 2 {
        best_base = -1;
    }

    // Format the result.
    for i in 0..nwords {
        // Are we inside the best run of 0x00's?
        if best_base != -1 && (i as i32) >= best_base && (i as i32) < best_base + best_len {
            if i as i32 == best_base {
                tmp[tp] = b':';
                tp += 1;
            }
            continue;
        }
        // Are we following an initial run of 0x00s or any real hex?
        if i != 0 {
            tmp[tp] = b':';
            tp += 1;
        }
        // Is this address an encapsulated IPv4?
        if i == 6
            && best_base == 0
            && (best_len == 6
                || (best_len == 7 && words[7] != 0x0001)
                || (best_len == 5 && words[5] == 0xffff))
        {
            let n = decoct(&src[12..16], 4, &mut tmp, tp);
            if n == 0 {
                return None; // errno = EMSGSIZE
            }
            tp += n;
            break;
        }
        tp += sprintf_x(&mut tmp, tp, words[i]);
    }

    // Was it a trailing run of 0x00's?
    if best_base != -1 && best_base + best_len == nwords as i32 {
        tmp[tp] = b':';
        tp += 1;
    }
    tmp[tp] = 0;

    if bits != -1 && bits != 128 {
        tmp[tp] = b'/';
        tp += 1;
        tp += sprintf_u(&mut tmp, tp, bits as u32);
    }

    // Check for overflow, copy, and we're done.
    //   if ((size_t) (tp - tmp) > size) { errno = EMSGSIZE; return (NULL); }
    //   strcpy(dst, tmp);
    if tp > dst.len() {
        return None;
    }
    dst[..tp].copy_from_slice(&tmp[..tp]);
    if tp < dst.len() {
        dst[tp] = 0;
    }
    Some(tp)
}

#[cfg(test)]
mod ntop_tests {
    use super::*;

    fn run4(src: [u8; 4], bits: i32) -> Option<String> {
        let mut buf = [0u8; 64];
        pg_inet_net_ntop(PGSQL_AF_INET as i32, &src, bits, &mut buf)
            .map(|n| String::from_utf8_lossy(&buf[..n]).into_owned())
    }

    fn run6(src: [u8; 16], bits: i32) -> Option<String> {
        let mut buf = [0u8; 64];
        pg_inet_net_ntop(PGSQL_AF_INET6 as i32, &src, bits, &mut buf)
            .map(|n| String::from_utf8_lossy(&buf[..n]).into_owned())
    }

    #[test]
    fn ipv4_full_and_masked() {
        assert_eq!(run4([192, 168, 1, 0], 24).unwrap(), "192.168.1.0/24");
        assert_eq!(run4([10, 1, 2, 3], 32).unwrap(), "10.1.2.3");
        assert_eq!(run4([0, 0, 0, 0], 0).unwrap(), "0.0.0.0/0");
        assert!(run4([1, 2, 3, 4], 33).is_none());
        assert!(run4([1, 2, 3, 4], -1).is_none());
    }

    #[test]
    fn ipv6_shorthand_and_v4_tail() {
        let mut a = [0u8; 16];
        a[15] = 1;
        assert_eq!(run6(a, 128).unwrap(), "::1");
        assert_eq!(run6([0u8; 16], 128).unwrap(), "::");
        let mut m = [0u8; 16];
        m[10] = 0xff;
        m[11] = 0xff;
        m[12] = 1;
        m[13] = 2;
        m[14] = 3;
        m[15] = 4;
        assert_eq!(run6(m, 128).unwrap(), "::ffff:1.2.3.4");
        let mut f = [0u8; 16];
        f[0] = 0xfe;
        f[1] = 0x80;
        assert_eq!(run6(f, 64).unwrap(), "fe80::/64");
    }
}
