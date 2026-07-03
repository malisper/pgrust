//! inet_net_pton.c (ISC): presentation -> network form. Returns Some(bits) or
//! None for every C `-1`/errno arm (network.c's only read of errno is via the
//! generic invalid-input message). size == -1 selects the lenient inet
//! parsers, else the strict CIDR parsers with `size` as the byte budget.

use crate::{PGSQL_AF_INET, PGSQL_AF_INET6};

const NS_IN6ADDRSZ: usize = 16;
const NS_INT16SZ: usize = 2;
const NS_INADDRSZ: usize = 4;

#[inline]
fn is_digit(ch: i32) -> bool {
    (b'0' as i32..=b'9' as i32).contains(&ch)
}

#[inline]
fn is_xdigit(ch: i32) -> bool {
    is_digit(ch)
        || (b'a' as i32..=b'f' as i32).contains(&ch)
        || (b'A' as i32..=b'F' as i32).contains(&ch)
}

#[inline]
fn is_upper(ch: i32) -> bool {
    (b'A' as i32..=b'Z' as i32).contains(&ch)
}

// C reads past the logical end as the NUL terminator.
#[inline]
fn at(src: &[u8], i: usize) -> i32 {
    if i < src.len() {
        src[i] as i32
    } else {
        0
    }
}

pub fn pg_inet_net_pton(af: i32, src: &[u8], dst: &mut [u8; 16], size: isize) -> Option<i32> {
    assert!(size <= NS_IN6ADDRSZ as isize, "non-canonical pton budget");
    if af == PGSQL_AF_INET as i32 {
        if size == -1 {
            inet_net_pton_ipv4(src, dst)
        } else {
            inet_cidr_pton_ipv4(src, dst, size as usize)
        }
    } else if af == PGSQL_AF_INET6 as i32 {
        if size == -1 {
            inet_cidr_pton_ipv6(src, dst, NS_IN6ADDRSZ)
        } else {
            inet_cidr_pton_ipv6(src, dst, size as usize)
        }
    } else {
        None
    }
}

fn inet_cidr_pton_ipv4(src: &[u8], dst: &mut [u8; 16], size: usize) -> Option<i32> {
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
        if size == 0 {
            return None;
        }
        let mut dirty = 0;
        si += 1;
        loop {
            ch = at(src, si);
            si += 1;
            if !(ch != 0 && is_xdigit(ch)) {
                break;
            }
            let mut c = ch;
            if is_upper(c) {
                c += b'a' as i32 - b'A' as i32;
            }
            let n = b"0123456789abcdef".iter().position(|&x| x as i32 == c)? as i32;
            if dirty == 0 {
                tmp = n;
            } else {
                tmp = (tmp << 4) | n;
            }
            dirty += 1;
            if dirty == 2 {
                if size == 0 {
                    return None;
                }
                size -= 1;
                dst[dp] = tmp as u8;
                dp += 1;
                dirty = 0;
            }
        }
        if dirty != 0 {
            if size == 0 {
                return None;
            }
            size -= 1;
            dst[dp] = (tmp << 4) as u8;
            dp += 1;
        }
    } else if is_digit(ch) {
        loop {
            tmp = 0;
            loop {
                tmp = tmp * 10 + (ch - b'0' as i32);
                if tmp > 255 {
                    return None;
                }
                ch = at(src, si);
                si += 1;
                if !(ch != 0 && is_digit(ch)) {
                    break;
                }
            }
            if size == 0 {
                return None;
            }
            size -= 1;
            dst[dp] = tmp as u8;
            dp += 1;
            if ch == 0 || ch == b'/' as i32 {
                break;
            }
            if ch != b'.' as i32 {
                return None;
            }
            ch = at(src, si);
            si += 1;
            if !is_digit(ch) {
                return None;
            }
        }
    } else {
        return None;
    }

    bits = -1;
    if ch == b'/' as i32 && is_digit(at(src, si)) && dp > odst {
        ch = at(src, si);
        si += 1;
        bits = 0;
        loop {
            // C accumulates over int and only checks >32 post-loop; saturate
            // instead of overflowing (same rejections).
            bits = bits.saturating_mul(10).saturating_add(ch - b'0' as i32);
            ch = at(src, si);
            si += 1;
            if !(ch != 0 && is_digit(ch)) {
                break;
            }
        }
        if ch != 0 {
            return None;
        }
        if bits > 32 {
            return None;
        }
    }

    if ch != 0 {
        return None;
    }
    if dp == odst {
        return None;
    }
    if bits == -1 {
        let first = dst[odst] as i32;
        bits = if first >= 240 {
            32
        } else if first >= 224 {
            8
        } else if first >= 192 {
            24
        } else if first >= 128 {
            16
        } else {
            8
        };
        if (bits as isize) < ((dp - odst) as isize) * 8 {
            bits = ((dp - odst) * 8) as i32;
        }
        if bits == 8 && dst[odst] == 224 {
            bits = 4;
        }
    }
    while bits as isize > ((dp - odst) as isize) * 8 {
        if size == 0 {
            return None;
        }
        size -= 1;
        dst[dp] = 0;
        dp += 1;
    }
    Some(bits)
}

fn inet_net_pton_ipv4(src: &[u8], dst: &mut [u8; 16]) -> Option<i32> {
    let mut dp: usize = 0;
    let odst: usize = 0;
    let mut si: usize = 0;
    let mut tmp: i32;
    let mut bits: i32;
    let mut size: usize = 4;

    let mut ch;
    loop {
        ch = at(src, si);
        si += 1;
        if !is_digit(ch) {
            break;
        }
        tmp = 0;
        loop {
            tmp = tmp * 10 + (ch - b'0' as i32);
            if tmp > 255 {
                return None;
            }
            ch = at(src, si);
            si += 1;
            if !(ch != 0 && is_digit(ch)) {
                break;
            }
        }
        if size == 0 {
            return None;
        }
        size -= 1;
        dst[dp] = tmp as u8;
        dp += 1;
        if ch == 0 || ch == b'/' as i32 {
            break;
        }
        if ch != b'.' as i32 {
            return None;
        }
    }

    bits = -1;
    if ch == b'/' as i32 && is_digit(at(src, si)) && dp > odst {
        ch = at(src, si);
        si += 1;
        bits = 0;
        loop {
            bits = bits.saturating_mul(10).saturating_add(ch - b'0' as i32);
            ch = at(src, si);
            si += 1;
            if !(ch != 0 && is_digit(ch)) {
                break;
            }
        }
        if ch != 0 {
            return None;
        }
        if bits > 32 {
            return None;
        }
    }

    if ch != 0 {
        return None;
    }
    if bits == -1 {
        if dp - odst == 4 {
            bits = 32;
        } else {
            return None;
        }
    }
    if dp == odst {
        return None;
    }
    if (bits / 8) as isize > (dp - odst) as isize {
        return None;
    }
    while size > 0 {
        size -= 1;
        dst[dp] = 0;
        dp += 1;
    }
    Some(bits)
}

// Rejects leading zeros and values above 128, like C getbits().
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
        if !is_digit(ch) {
            return None;
        }
        if n != 0 && val == 0 {
            return None;
        }
        n += 1;
        val = val * 10 + (ch - b'0' as i32);
        if val > 128 {
            return None;
        }
    }
    if n == 0 {
        return None;
    }
    Some(val)
}

// Embedded dotted-quad tail; Some((octets_written, cidr_suffix)).
fn getv4(src: &[u8], dst: &mut [u8]) -> Option<(usize, Option<i32>)> {
    let mut dp: usize = 0;
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
            if n != 0 && val == 0 {
                return None;
            }
            n += 1;
            val = val * 10 + (ch - b'0' as i32) as u32;
            if val > 255 {
                return None;
            }
            continue;
        }
        if ch == b'.' as i32 || ch == b'/' as i32 {
            if dp > 3 {
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
    if dp > 3 {
        return None;
    }
    dst[dp] = val as u8;
    dp += 1;
    Some((dp, None))
}

fn inet_cidr_pton_ipv6(src: &[u8], dst: &mut [u8; 16], size: usize) -> Option<i32> {
    if size < NS_IN6ADDRSZ {
        return None;
    }

    let mut tmp = [0u8; NS_IN6ADDRSZ];
    let mut tp: usize = 0;
    let mut endp: usize = NS_IN6ADDRSZ;
    let mut colonp: Option<usize> = None;
    let mut si: usize = 0;

    if at(src, si) == b':' as i32 {
        si += 1;
        if at(src, si) != b':' as i32 {
            return None;
        }
    }

    let mut curtok: usize = si;
    let mut saw_xdigit = false;
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
            val = (val << 4) | nibble;
            digits += 1;
            if digits > 4 {
                return None;
            }
            saw_xdigit = true;
            continue;
        }
        if ch == b':' as i32 {
            curtok = si;
            if !saw_xdigit {
                if colonp.is_some() {
                    return None;
                }
                colonp = Some(tp);
                continue;
            } else if at(src, si) == 0 {
                return None;
            }
            if tp + NS_INT16SZ > endp {
                return None;
            }
            tmp[tp] = (val >> 8) as u8;
            tp += 1;
            tmp[tp] = val as u8;
            tp += 1;
            saw_xdigit = false;
            digits = 0;
            val = 0;
            continue;
        }
        if ch == b'.' as i32 && (tp + NS_INADDRSZ) <= endp {
            if let Some((_written, v4bits)) = getv4(&src[curtok..], &mut tmp[tp..]) {
                if let Some(b) = v4bits {
                    bits = b;
                }
                tp += NS_INADDRSZ;
                saw_xdigit = false;
                break;
            }
        }
        if ch == b'/' as i32 {
            if let Some(b) = getbits(&src[si..]) {
                bits = b;
                break;
            }
        }
        return None;
    }

    if saw_xdigit {
        if tp + NS_INT16SZ > endp {
            return None;
        }
        tmp[tp] = (val >> 8) as u8;
        tp += 1;
        tmp[tp] = val as u8;
        tp += 1;
    }
    if bits == -1 {
        bits = 128;
    }

    endp = NS_IN6ADDRSZ;

    if let Some(colon) = colonp {
        let n = tp - colon;
        if tp == endp {
            return None;
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
        return None;
    }

    dst[..NS_IN6ADDRSZ].copy_from_slice(&tmp);
    Some(bits)
}

#[inline]
fn xdigit_nibble(ch: i32) -> Option<u32> {
    match ch as u8 {
        c @ b'0'..=b'9' => Some((c - b'0') as u32),
        c @ b'a'..=b'f' => Some((c - b'a' + 10) as u32),
        c @ b'A'..=b'F' => Some((c - b'A' + 10) as u32),
        _ => None,
    }
}
