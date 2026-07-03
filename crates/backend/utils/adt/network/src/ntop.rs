//! src/port/inet_net_ntop.c (ISC): network form -> presentation text.
//! Returns Some(len) with the text at dst[..len], None for the C NULL arms.
//! The C sizeof-literal capacity checks are kept so an undersized dst fails
//! on the same boundaries.

use crate::{PGSQL_AF_INET, PGSQL_AF_INET6};

const NS_IN6ADDRSZ: usize = 16;
const NS_INT16SZ: usize = 2;

fn sprintf_u(dst: &mut [u8], d: usize, v: u32) -> usize {
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
        dst[d + k] = tmp[n - 1 - k];
    }
    n
}

fn sprintf_x(dst: &mut [u8], d: usize, v: u32) -> usize {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut tmp = [0u8; 8];
    let mut n = 0;
    let mut x = v;
    loop {
        tmp[n] = HEX[(x & 0xf) as usize];
        x >>= 4;
        n += 1;
        if x == 0 {
            break;
        }
    }
    for k in 0..n {
        dst[d + k] = tmp[n - 1 - k];
    }
    n
}

pub fn pg_inet_net_ntop(af: i32, src: &[u8], bits: i32, dst: &mut [u8]) -> Option<usize> {
    if af == PGSQL_AF_INET as i32 {
        inet_net_ntop_ipv4(src, bits, dst)
    } else if af == PGSQL_AF_INET6 as i32 {
        inet_net_ntop_ipv6(src, bits, dst)
    } else {
        None
    }
}

fn inet_net_ntop_ipv4(src: &[u8], bits: i32, dst: &mut [u8]) -> Option<usize> {
    let odst = 0usize;
    let mut d = 0usize;
    let mut size = dst.len();
    let mut si = 0usize;

    if !(0..=32).contains(&bits) {
        return None;
    }

    for _b in (1..=4).rev() {
        if size <= 5 {
            return None;
        }
        let t = d;
        if d != odst {
            dst[d] = b'.';
            d += 1;
        }
        if si >= src.len() {
            return None;
        }
        d += sprintf_u(dst, d, src[si] as u32);
        si += 1;
        size -= d - t;
    }

    if bits != 32 {
        if size <= 4 {
            return None;
        }
        dst[d] = b'/';
        d += 1;
        d += sprintf_u(dst, d, bits as u32);
    }

    Some(d)
}

fn decoct(src: &[u8], bytes: usize, dst: &mut [u8], d0: usize) -> usize {
    let odst = d0;
    let mut d = d0;
    let mut size = dst.len() - d0;
    let mut si = 0usize;

    for b in 1..=bytes {
        if size <= 5 {
            return 0;
        }
        let t = d;
        d += sprintf_u(dst, d, src[si] as u32);
        si += 1;
        if b != bytes {
            dst[d] = b'.';
            d += 1;
        }
        size -= d - t;
    }
    d - odst
}

fn inet_net_ntop_ipv6(src: &[u8], bits: i32, dst: &mut [u8]) -> Option<usize> {
    let mut tmp = [0u8; 64];
    let mut tp = 0usize;

    if !(-1..=128).contains(&bits) {
        return None;
    }
    if src.len() < NS_IN6ADDRSZ {
        return None;
    }

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

    for i in 0..nwords {
        if best_base != -1 && (i as i32) >= best_base && (i as i32) < best_base + best_len {
            if i as i32 == best_base {
                tmp[tp] = b':';
                tp += 1;
            }
            continue;
        }
        if i != 0 {
            tmp[tp] = b':';
            tp += 1;
        }
        if i == 6
            && best_base == 0
            && (best_len == 6
                || (best_len == 7 && words[7] != 0x0001)
                || (best_len == 5 && words[5] == 0xffff))
        {
            let n = decoct(&src[12..16], 4, &mut tmp, tp);
            if n == 0 {
                return None;
            }
            tp += n;
            break;
        }
        tp += sprintf_x(&mut tmp, tp, words[i]);
    }

    if best_base != -1 && best_base + best_len == nwords as i32 {
        tmp[tp] = b':';
        tp += 1;
    }

    if bits != -1 && bits != 128 {
        tmp[tp] = b'/';
        tp += 1;
        tp += sprintf_u(&mut tmp, tp, bits as u32);
    }

    if tp > dst.len() {
        return None;
    }
    dst[..tp].copy_from_slice(&tmp[..tp]);
    Some(tp)
}
