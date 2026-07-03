//! inet_cidr_ntop.c (ISC): network form -> CIDR-style presentation text
//! (cidr_abbrev's engine; drops trailing zero octets/words). Returns
//! Some(len) or None for the C NULL/errno arms.

use crate::{PGSQL_AF_INET, PGSQL_AF_INET6};

pub fn pg_inet_cidr_ntop(af: i32, src: &[u8], bits: i32, dst: &mut [u8]) -> Option<usize> {
    if af == PGSQL_AF_INET as i32 {
        inet_cidr_ntop_ipv4(src, bits, dst)
    } else if af == PGSQL_AF_INET6 as i32 {
        inet_cidr_ntop_ipv6(src, bits, dst)
    } else {
        None
    }
}

fn write_u(dst: &mut [u8], at: usize, v: u32) -> usize {
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

fn inet_cidr_ntop_ipv4(src: &[u8], bits: i32, dst: &mut [u8]) -> Option<usize> {
    let odst: usize = 0;
    let mut d: usize = 0;
    let mut size: usize = dst.len();
    let mut si: usize = 0;

    if !(0..=32).contains(&bits) {
        return None;
    }

    if bits == 0 {
        if size < 2 {
            return None;
        }
        dst[d] = b'0';
        d += 1;
        size -= 1;
    }

    let mut b = bits / 8;
    while b > 0 {
        if size <= 5 {
            return None;
        }
        let t = d;
        d += write_u(dst, d, src[si] as u32);
        si += 1;
        if b > 1 {
            dst[d] = b'.';
            d += 1;
        }
        size -= d - t;
        b -= 1;
    }

    let b = bits % 8;
    if b > 0 {
        if size <= 5 {
            return None;
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

    if size <= 4 {
        return None;
    }
    dst[d] = b'/';
    d += 1;
    d += write_u(dst, d, bits as u32);
    Some(d)
}

fn inet_cidr_ntop_ipv6(src: &[u8], bits: i32, dst: &mut [u8]) -> Option<usize> {
    if !(0..=128).contains(&bits) {
        return None;
    }
    // C builds into a 50-byte outbuf then strcpy-checks against size.
    let mut outbuf = [0u8; 50];
    let mut o = 0usize;

    if bits == 0 {
        outbuf[0] = b':';
        outbuf[1] = b':';
        o = 2;
    } else {
        let mut inbuf = [0u8; 16];
        let p = ((bits + 7) / 8) as usize;
        inbuf[..p].copy_from_slice(&src[..p]);
        let b = bits % 8;
        if b != 0 {
            let m: u32 = (!0u32) << (8 - b);
            inbuf[p - 1] = (inbuf[p - 1] as u32 & m) as u8;
        }

        let mut si: usize = 0;
        let mut words = (bits + 15) / 16;
        if words == 1 {
            words = 2;
        }

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

        let is_ipv4 = zero_l != words
            && zero_s == 0
            && (zero_l == 6
                || ((zero_l == 5 && inbuf[10] == 0xff && inbuf[11] == 0xff)
                    || (zero_l == 7 && inbuf[14] != 0 && inbuf[15] != 1)));

        let mut p = 0;
        while p < words {
            if zero_l != 0 && p >= zero_s && p < zero_s + zero_l {
                if p == zero_s {
                    outbuf[o] = b':';
                    o += 1;
                }
                if p == words - 1 {
                    outbuf[o] = b':';
                    o += 1;
                }
                si += 2;
                p += 1;
                continue;
            }

            if is_ipv4 && p > 5 {
                outbuf[o] = if p == 6 { b':' } else { b'.' };
                o += 1;
                o += write_u(&mut outbuf, o, inbuf[si] as u32);
                si += 1;
                if p != 7 || bits > 120 {
                    outbuf[o] = b'.';
                    o += 1;
                    o += write_u(&mut outbuf, o, inbuf[si] as u32);
                    si += 1;
                }
            } else {
                if o != 0 {
                    outbuf[o] = b':';
                    o += 1;
                }
                o += write_x(&mut outbuf, o, inbuf[si] as u32 * 256 + inbuf[si + 1] as u32);
                si += 2;
            }
            p += 1;
        }
    }

    outbuf[o] = b'/';
    o += 1;
    o += write_u(&mut outbuf, o, bits as u32);

    if o + 1 > dst.len() {
        return None;
    }
    dst[..o].copy_from_slice(&outbuf[..o]);
    Some(o)
}

fn write_x(dst: &mut [u8], at: usize, v: u32) -> usize {
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
        dst[at + k] = tmp[n - 1 - k];
    }
    n
}
