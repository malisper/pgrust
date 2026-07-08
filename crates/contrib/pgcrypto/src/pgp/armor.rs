//! PGP ASCII-armor (Radix-64 + CRC24) — a faithful port of `pgp-armor.c`'s
//! `pgp_armor_encode` / `pgp_armor_decode` / `pgp_extract_armor_headers`, plus
//! the `pg_base64_*` codec the file duplicates.

/// `_base64` alphabet from pgp-armor.c.
const BASE64: &[u8; 64] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

const ARMOR_HEADER: &str = "-----BEGIN PGP MESSAGE-----\n";
const ARMOR_FOOTER: &str = "\n-----END PGP MESSAGE-----\n";

/// pgcrypto's `PXE_PGP_CORRUPT_ARMOR` → "Corrupt ascii-armor".
pub const CORRUPT_ARMOR: &str = "Corrupt ascii-armor";

/// `pg_base64_encode` (pgp-armor.c) — 76-char line wrap, '=' padding.
fn base64_encode(src: &[u8]) -> Vec<u8> {
    let mut dst: Vec<u8> = Vec::with_capacity((src.len() + 2) / 3 * 4 + src.len() / 57 + 4);
    let mut pos: i32 = 2;
    let mut buf: u64 = 0;
    // C tracks a moving line-end pointer `lend = dst + 76`; after writing past
    // it, it inserts '\n' and resets lend to p + 76. Track chars-since-newline.
    let mut line_chars = 0usize;
    for &s in src {
        buf |= (s as u64) << (pos << 3);
        pos -= 1;
        if pos < 0 {
            for shift in [18, 12, 6, 0] {
                dst.push(BASE64[((buf >> shift) & 0x3f) as usize]);
                line_chars += 1;
            }
            pos = 2;
            buf = 0;
        }
        if line_chars >= 76 {
            dst.push(b'\n');
            line_chars = 0;
        }
    }
    if pos != 2 {
        dst.push(BASE64[((buf >> 18) & 0x3f) as usize]);
        dst.push(BASE64[((buf >> 12) & 0x3f) as usize]);
        dst.push(if pos == 0 {
            BASE64[((buf >> 6) & 0x3f) as usize]
        } else {
            b'='
        });
        dst.push(b'=');
    }
    dst
}

/// `pg_base64_decode` (pgp-armor.c). Returns the decoded bytes, or `Err` on a
/// corrupt sequence (`PXE_PGP_CORRUPT_ARMOR`).
fn base64_decode(src: &[u8]) -> Result<Vec<u8>, ()> {
    let mut dst = Vec::with_capacity((src.len() * 3) >> 2);
    let mut buf: u64 = 0;
    let mut pos = 0i32;
    let mut end = 0i32;
    for &c in src {
        let b: u64 = if c.is_ascii_uppercase() {
            (c - b'A') as u64
        } else if c.is_ascii_lowercase() {
            (c - b'a' + 26) as u64
        } else if c.is_ascii_digit() {
            (c - b'0' + 52) as u64
        } else if c == b'+' {
            62
        } else if c == b'/' {
            63
        } else if c == b'=' {
            if end == 0 {
                if pos == 2 {
                    end = 1;
                } else if pos == 3 {
                    end = 2;
                } else {
                    return Err(());
                }
            }
            0
        } else if c == b' ' || c == b'\t' || c == b'\n' || c == b'\r' {
            continue;
        } else {
            return Err(());
        };
        buf = (buf << 6) + b;
        pos += 1;
        if pos == 4 {
            dst.push(((buf >> 16) & 255) as u8);
            if end == 0 || end > 1 {
                dst.push(((buf >> 8) & 255) as u8);
            }
            if end == 0 || end > 2 {
                dst.push((buf & 255) as u8);
            }
            buf = 0;
            pos = 0;
        }
    }
    if pos != 0 {
        return Err(());
    }
    Ok(dst)
}

/// `crc24` (rfc2440).
fn crc24(data: &[u8]) -> u32 {
    const CRC24_INIT: u32 = 0x00b7_04ce;
    const CRC24_POLY: u32 = 0x0186_4cfb;
    let mut crc = CRC24_INIT;
    for &b in data {
        crc ^= (b as u32) << 16;
        for _ in 0..8 {
            crc <<= 1;
            if crc & 0x0100_0000 != 0 {
                crc ^= CRC24_POLY;
            }
        }
    }
    crc & 0x00ff_ffff
}

/// `pgp_armor_encode` — produce the armored text for `src` with optional
/// header lines.
pub fn armor_encode(src: &[u8], keys: &[Vec<u8>], values: &[Vec<u8>]) -> Vec<u8> {
    let crc = crc24(src);
    let mut dst: Vec<u8> = Vec::new();
    dst.extend_from_slice(ARMOR_HEADER.as_bytes());
    for (k, v) in keys.iter().zip(values.iter()) {
        dst.extend_from_slice(k);
        dst.extend_from_slice(b": ");
        dst.extend_from_slice(v);
        dst.push(b'\n');
    }
    dst.push(b'\n');

    let b64 = base64_encode(src);
    dst.extend_from_slice(&b64);

    if dst.last() != Some(&b'\n') {
        dst.push(b'\n');
    }
    dst.push(b'=');
    dst.push(BASE64[((crc >> 18) & 0x3f) as usize]);
    dst.push(BASE64[((crc >> 12) & 0x3f) as usize]);
    dst.push(BASE64[((crc >> 6) & 0x3f) as usize]);
    dst.push(BASE64[(crc & 0x3f) as usize]);
    dst.extend_from_slice(ARMOR_FOOTER.as_bytes());
    dst
}

/// `find_str` — locate `needle` in `data`.
fn find_str(data: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || data.len() < needle.len() {
        return None;
    }
    let mut p = 0usize;
    while p < data.len() {
        match data[p..].iter().position(|&b| b == needle[0]) {
            None => return None,
            Some(off) => {
                p += off;
                if p + needle.len() > data.len() {
                    return None;
                }
                if data[p..p + needle.len()] == *needle {
                    return Some(p);
                }
                p += 1;
            }
        }
    }
    None
}

/// `find_header` — returns `(start_offset, header_len)` where `start_offset` is
/// the absolute index of the `-----BEGIN`/`-----END` and `header_len` is the
/// length of the full header line (so `start + header_len` is the byte after
/// the line terminator). `Err(())` = corrupt.
fn find_header(data: &[u8], is_end: bool) -> Result<(usize, usize), ()> {
    let sep: &[u8] = if is_end { b"-----END" } else { b"-----BEGIN" };
    let mut search_from = 0usize;
    let start;
    loop {
        match find_str(&data[search_from..], sep) {
            None => return Err(()),
            Some(rel) => {
                let abs = search_from + rel;
                // must start at beginning of line
                if abs == 0 || data[abs - 1] == b'\n' {
                    start = abs;
                    break;
                }
                search_from = abs + sep.len();
            }
        }
    }
    let mut p = start + sep.len();
    // header text: anything >= ' ' until a '-'
    while p < data.len() && data[p] != b'-' {
        if data[p] >= b' ' {
            p += 1;
            continue;
        }
        return Err(());
    }
    if data.len() - p < 5 || data[p..p + 5] != sep[..5] {
        return Err(());
    }
    p += 5;
    // at end of line
    if p < data.len() {
        if data[p] != b'\n' && data[p] != b'\r' {
            return Err(());
        }
        if data[p] == b'\r' {
            p += 1;
        }
        if p < data.len() && data[p] == b'\n' {
            p += 1;
        }
    }
    Ok((start, p - start))
}

/// `pgp_armor_decode` — dearmor; returns the binary payload or `Err(())`.
pub fn armor_decode(src: &[u8]) -> Result<Vec<u8>, ()> {
    // armor start
    let (start_off, hlen) = find_header(src, false)?;
    if hlen == 0 {
        return Err(());
    }
    let mut p = start_off + hlen;

    // armor end (search from p)
    let (end_rel, ehlen) = find_header(&src[p..], true)?;
    if ehlen == 0 {
        return Err(());
    }
    let armor_end = p + end_rel;

    // skip comments — find empty line
    while p < armor_end && src[p] != b'\n' && src[p] != b'\r' {
        match src[p..armor_end].iter().position(|&b| b == b'\n') {
            None => return Err(()),
            Some(off) => p = p + off + 1,
        }
    }
    let base64_start = p;

    // find crc pos: scan backward from armor_end for '='
    let mut crc_eq: Option<usize> = None;
    let mut q = armor_end;
    loop {
        if src[q] == b'=' {
            crc_eq = Some(q);
            break;
        }
        if q == base64_start {
            break;
        }
        q -= 1;
    }
    let crc_eq = crc_eq.ok_or(())?;
    let base64_end = crc_eq - 1;

    // decode crc: 4 chars after '='
    if crc_eq + 5 > src.len() {
        return Err(());
    }
    let crc_buf = base64_decode(&src[crc_eq + 1..crc_eq + 5])?;
    if crc_buf.len() != 3 {
        return Err(());
    }
    let crc = ((crc_buf[0] as u32) << 16) + ((crc_buf[1] as u32) << 8) + crc_buf[2] as u32;

    // decode data
    let res = base64_decode(&src[base64_start..base64_end])?;
    if crc24(&res) == crc {
        Ok(res)
    } else {
        Err(())
    }
}

/// `pgp_extract_armor_headers` — returns the list of `(key, value)` header
/// pairs, or `Err(())` (corrupt). Keys/values are returned as raw bytes (UTF-8).
pub fn extract_armor_headers(src: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ()> {
    // armor start
    let (start_off, hlen) = find_header(src, false)?;
    if hlen == 0 {
        return Err(());
    }
    let armor_start = start_off + hlen;

    // armor end
    let (end_rel, ehlen) = find_header(&src[armor_start..], true)?;
    if ehlen == 0 {
        return Err(());
    }
    let armor_end = armor_start + end_rel;

    // count header lines: walk lines until an empty line
    let mut p = armor_start;
    while p < armor_end && src[p] != b'\n' && src[p] != b'\r' {
        match src[p..armor_end].iter().position(|&b| b == b'\n') {
            None => return Err(()),
            Some(off) => p = p + off + 1,
        }
    }
    let base64_start = p;

    // The header region is [armor_start, base64_start).
    let buf = &src[armor_start..base64_start];

    // Split lines at '\n' and ": " separators.
    let mut headers: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    let mut line_start = 0usize;
    loop {
        // find end of line
        let eol = match buf[line_start..].iter().position(|&b| b == b'\n') {
            None => break,
            Some(off) => line_start + off,
        };
        let nextline = eol + 1;
        // strip trailing CR
        let mut line_end = eol;
        if line_end > line_start && buf[line_end - 1] == b'\r' {
            line_end -= 1;
        }
        let line = &buf[line_start..line_end];
        // find ": " separator
        let colon = find_subslice(line, b": ").ok_or(())?;
        let key = line[..colon].to_vec();
        let value = line[colon + 2..].to_vec();
        headers.push((key, value));
        line_start = nextline;
    }
    Ok(headers)
}

/// `strstr(line, ": ")`.
fn find_subslice(hay: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || hay.len() < needle.len() {
        return None;
    }
    hay.windows(needle.len()).position(|w| w == needle)
}
