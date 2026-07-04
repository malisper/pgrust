// base64.c contract: caller-provided dst sized by the *_len helpers; -1 return
// zeroes all of dst (avoids leaking partial decodes of secrets). Not RFC-MIME:
// no whitespace tolerated, C's end-flag quirk on '=' preserved.

const BASE64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

const B64LOOKUP: [i8; 128] = [
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 62, -1, -1, -1, 63,
    52, 53, 54, 55, 56, 57, 58, 59, 60, 61, -1, -1, -1, -1, -1, -1,
    -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,
    15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, -1, -1, -1, -1, -1,
    -1, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
    41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, -1, -1, -1, -1, -1,
];

#[cold]
fn error(dst: &mut [u8], dstlen: usize) -> i32 {
    dst[..dstlen].fill(0);
    -1
}

pub fn pg_b64_encode(src: &[u8], len: i32, dst: &mut [u8], dstlen: i32) -> i32 {
    let len = len as usize;
    let dstlen = dstlen as usize;
    let mut p = 0usize;
    let mut pos: i32 = 2;
    let mut buf: u32 = 0;

    for &byte in &src[..len] {
        buf |= (byte as u32) << (pos << 3);
        pos -= 1;
        if pos < 0 {
            if p + 4 > dstlen {
                return error(dst, dstlen);
            }
            dst[p] = BASE64[((buf >> 18) & 0x3f) as usize];
            dst[p + 1] = BASE64[((buf >> 12) & 0x3f) as usize];
            dst[p + 2] = BASE64[((buf >> 6) & 0x3f) as usize];
            dst[p + 3] = BASE64[(buf & 0x3f) as usize];
            p += 4;
            pos = 2;
            buf = 0;
        }
    }

    if pos != 2 {
        if p + 4 > dstlen {
            return error(dst, dstlen);
        }
        dst[p] = BASE64[((buf >> 18) & 0x3f) as usize];
        dst[p + 1] = BASE64[((buf >> 12) & 0x3f) as usize];
        dst[p + 2] = if pos == 0 { BASE64[((buf >> 6) & 0x3f) as usize] } else { b'=' };
        dst[p + 3] = b'=';
        p += 4;
    }

    debug_assert!(p <= dstlen);
    p as i32
}

pub fn pg_b64_decode(src: &[u8], len: i32, dst: &mut [u8], dstlen: i32) -> i32 {
    let len = len as usize;
    let dstlen = dstlen as usize;
    let mut p = 0usize;
    let mut buf: u32 = 0;
    let mut pos: i32 = 0;
    let mut end: i32 = 0;

    for &c in &src[..len] {
        if c == b' ' || c == b'\t' || c == b'\n' || c == b'\r' {
            return error(dst, dstlen);
        }

        let b: i32;
        if c == b'=' {
            if end == 0 {
                if pos == 2 {
                    end = 1;
                } else if pos == 3 {
                    end = 2;
                } else {
                    return error(dst, dstlen);
                }
            }
            b = 0;
        } else {
            // C `char` may be signed: bytes >= 0x80 fail the c > 0 test.
            let c = c as i8 as i32;
            let lu = if c > 0 && c < 127 { B64LOOKUP[c as usize] as i32 } else { -1 };
            if lu < 0 {
                return error(dst, dstlen);
            }
            b = lu;
        }
        buf = (buf << 6).wrapping_add(b as u32);
        pos += 1;
        if pos == 4 {
            if p + 1 > dstlen {
                return error(dst, dstlen);
            }
            dst[p] = ((buf >> 16) & 255) as u8;
            p += 1;
            if end == 0 || end > 1 {
                if p + 1 > dstlen {
                    return error(dst, dstlen);
                }
                dst[p] = ((buf >> 8) & 255) as u8;
                p += 1;
            }
            if end == 0 || end > 2 {
                if p + 1 > dstlen {
                    return error(dst, dstlen);
                }
                dst[p] = (buf & 255) as u8;
                p += 1;
            }
            buf = 0;
            pos = 0;
        }
    }

    if pos != 0 {
        return error(dst, dstlen);
    }

    debug_assert!(p <= dstlen);
    p as i32
}

pub fn pg_b64_enc_len(srclen: i32) -> i32 {
    (srclen + 2) / 3 * 4
}

pub fn pg_b64_dec_len(srclen: i32) -> i32 {
    (srclen * 3) >> 2
}

#[cfg(test)]
mod tests {
    use super::*;

    fn enc(src: &[u8]) -> Vec<u8> {
        let cap = pg_b64_enc_len(src.len() as i32);
        let mut dst = vec![0u8; cap as usize];
        let n = pg_b64_encode(src, src.len() as i32, &mut dst, cap);
        assert!(n >= 0);
        dst.truncate(n as usize);
        dst
    }

    fn dec(src: &[u8]) -> Option<Vec<u8>> {
        let cap = pg_b64_dec_len(src.len() as i32);
        let mut dst = vec![0u8; cap as usize];
        let n = pg_b64_decode(src, src.len() as i32, &mut dst, cap);
        if n < 0 {
            return None;
        }
        dst.truncate(n as usize);
        Some(dst)
    }

    #[test]
    fn rfc4648_vectors_round_trip() {
        let cases: &[(&[u8], &[u8])] = &[
            (b"", b""),
            (b"f", b"Zg=="),
            (b"fo", b"Zm8="),
            (b"foo", b"Zm9v"),
            (b"foob", b"Zm9vYg=="),
            (b"fooba", b"Zm9vYmE="),
            (b"foobar", b"Zm9vYmFy"),
            (b"\0\xff\x10", b"AP8Q"),
        ];
        for (plain, encoded) in cases {
            assert_eq!(&enc(plain), encoded);
            assert_eq!(dec(encoded).as_deref(), Some(*plain));
        }
    }

    #[test]
    fn all_bytes_round_trip() {
        let data: Vec<u8> = (0..=255u8).collect();
        assert_eq!(dec(&enc(&data)).as_deref(), Some(&data[..]));
    }

    #[test]
    fn rejects_whitespace_padding_and_high_bytes() {
        for invalid in [
            b"Z g==".as_slice(), b"Zg=\n", b"=", b"Z=", b"Zg", b"Zg==A", b"Zm9\xc3",
        ] {
            assert_eq!(dec(invalid), None);
        }
    }

    #[test]
    fn c_end_flag_quirk() {
        assert_eq!(dec(b"Zm=9").as_deref(), Some(b"f".as_slice()));
    }

    #[test]
    fn errors_zero_dst() {
        let mut dst = [b'x'; 3];
        assert_eq!(pg_b64_encode(b"foo", 3, &mut dst, 3), -1);
        assert_eq!(dst, [0; 3]);
        let mut dst = [b'x'; 2];
        assert_eq!(pg_b64_decode(b"Zm9v", 4, &mut dst, 2), -1);
        assert_eq!(dst, [0; 2]);
    }

    #[test]
    fn len_helpers() {
        for len in 0..64i32 {
            assert_eq!(pg_b64_enc_len(len), (len + 2) / 3 * 4);
            assert_eq!(pg_b64_dec_len(len), (len * 3) >> 2);
        }
    }
}
