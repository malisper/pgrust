//! Encoding and decoding routines for base64 without whitespace.
//!
//! Faithful port of PostgreSQL's `src/common/base64.c`.
//!
//! The C routines operate on caller-provided output buffers and report failure
//! by returning `-1` after zeroing the destination (`memset(dst, 0, dstlen)`).
//! This port preserves that exact contract: `dst` is a caller-owned `&mut [u8]`
//! sized to `dstlen`, the return is the number of bytes written, and `-1`
//! signals an error after zeroing all of `dst`. No allocation occurs (the C
//! does none), so there is no `Mcx`/`PgResult` here.

/// `static const char _base64[]` — the base64 alphabet (64 symbols).
const BASE64: &[u8; 64] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// `static const int8 b64lookup[128]` — reverse map; -1 marks an invalid symbol.
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

/// `pg_b64_encode`
///
/// Encode the `src` byte array into base64. Returns the length of the encoded
/// string, and `-1` in the event of an error with the result buffer zeroed for
/// safety.
///
/// `len` is the number of valid bytes in `src`; `dstlen` is the capacity of
/// `dst` (both passed explicitly to mirror the C signature, where they index
/// the caller's raw buffers).
pub fn pg_b64_encode(src: &[u8], len: i32, dst: &mut [u8], dstlen: i32) -> i32 {
    let len = len as usize;
    let dstlen = dstlen as usize;

    // `s` and `p` are indices into `src` and `dst`; `end = src + len`.
    let end = len;
    let mut s = 0usize;
    let mut p = 0usize;
    let mut pos: i32 = 2;
    let mut buf: u32 = 0;

    while s < end {
        // buf |= *s << (pos << 3);  -- *s widened to int (uint32 buf).
        buf |= (src[s] as u32) << (pos << 3);
        pos -= 1;
        s += 1;

        // write it out
        if pos < 0 {
            // Leave if there is an overflow in the area allocated for the
            // encoded string.
            if p + 4 > dstlen {
                return error_encode(dst, dstlen);
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
        // Leave if there is an overflow in the area allocated for the encoded
        // string.
        if p + 4 > dstlen {
            return error_encode(dst, dstlen);
        }

        dst[p] = BASE64[((buf >> 18) & 0x3f) as usize];
        dst[p + 1] = BASE64[((buf >> 12) & 0x3f) as usize];
        dst[p + 2] = if pos == 0 {
            BASE64[((buf >> 6) & 0x3f) as usize]
        } else {
            b'='
        };
        dst[p + 3] = b'=';
        p += 4;
    }

    debug_assert!(p <= dstlen);
    p as i32
}

/// `pg_b64_decode`
///
/// Decode the given base64 string. Returns the length of the decoded string on
/// success, and `-1` in the event of an error with the result buffer zeroed for
/// safety.
pub fn pg_b64_decode(src: &[u8], len: i32, dst: &mut [u8], dstlen: i32) -> i32 {
    let len = len as usize;
    let dstlen = dstlen as usize;

    let srcend = len; // srcend = src + len
    let mut s = 0usize;
    let mut p = 0usize;
    let mut b: i32;
    let mut buf: u32 = 0;
    let mut pos: i32 = 0;
    let mut end: i32 = 0;

    while s < srcend {
        let c = src[s] as i8 as i32; // `char` may be signed; mirror C `char c`
        s += 1;

        // Leave if a whitespace is found
        if c == b' ' as i32 || c == b'\t' as i32 || c == b'\n' as i32 || c == b'\r' as i32 {
            return error_decode(dst, dstlen);
        }

        if c == b'=' as i32 {
            // end sequence
            if end == 0 {
                if pos == 2 {
                    end = 1;
                } else if pos == 3 {
                    end = 2;
                } else {
                    // Unexpected "=" character found while decoding base64
                    // sequence.
                    return error_decode(dst, dstlen);
                }
            }
            b = 0;
        } else {
            b = -1;
            if c > 0 && c < 127 {
                b = B64LOOKUP[c as usize] as i32;
            }
            if b < 0 {
                // invalid symbol found
                return error_decode(dst, dstlen);
            }
        }
        // add it to buffer
        buf = (buf << 6).wrapping_add(b as u32);
        pos += 1;
        if pos == 4 {
            // Leave if there is an overflow in the area allocated for the
            // decoded string.
            if p + 1 > dstlen {
                return error_decode(dst, dstlen);
            }
            dst[p] = ((buf >> 16) & 255) as u8;
            p += 1;

            if end == 0 || end > 1 {
                // overflow check
                if p + 1 > dstlen {
                    return error_decode(dst, dstlen);
                }
                dst[p] = ((buf >> 8) & 255) as u8;
                p += 1;
            }
            if end == 0 || end > 2 {
                // overflow check
                if p + 1 > dstlen {
                    return error_decode(dst, dstlen);
                }
                dst[p] = (buf & 255) as u8;
                p += 1;
            }
            buf = 0;
            pos = 0;
        }
    }

    if pos != 0 {
        // base64 end sequence is invalid. Input data is missing padding, is
        // truncated or is otherwise corrupted.
        return error_decode(dst, dstlen);
    }

    debug_assert!(p <= dstlen);
    p as i32
}

/// The `error:` label of both C routines: `memset(dst, 0, dstlen); return -1;`.
#[inline]
fn error_encode(dst: &mut [u8], dstlen: usize) -> i32 {
    dst[..dstlen].fill(0);
    -1
}

#[inline]
fn error_decode(dst: &mut [u8], dstlen: usize) -> i32 {
    dst[..dstlen].fill(0);
    -1
}

/// `pg_b64_enc_len`
///
/// Returns to caller the length of the string if it were encoded with base64
/// based on the length provided by caller. 3 bytes will be converted to 4.
pub fn pg_b64_enc_len(srclen: i32) -> i32 {
    (srclen + 2) / 3 * 4
}

/// `pg_b64_dec_len`
///
/// Returns to caller the length of the string if it were to be decoded with
/// base64, based on the length given by caller.
pub fn pg_b64_dec_len(srclen: i32) -> i32 {
    (srclen * 3) >> 2
}

#[cfg(test)]
mod tests {
    use super::*;

    fn enc(src: &[u8]) -> alloc::vec::Vec<u8> {
        let cap = pg_b64_enc_len(src.len() as i32);
        let mut dst = alloc::vec![0u8; cap as usize];
        let n = pg_b64_encode(src, src.len() as i32, &mut dst, cap);
        assert!(n >= 0);
        dst.truncate(n as usize);
        dst
    }

    fn dec(src: &[u8]) -> Option<alloc::vec::Vec<u8>> {
        let cap = pg_b64_dec_len(src.len() as i32);
        let mut dst = alloc::vec![0u8; cap as usize];
        let n = pg_b64_decode(src, src.len() as i32, &mut dst, cap);
        if n < 0 {
            return None;
        }
        dst.truncate(n as usize);
        Some(dst)
    }

    #[test]
    fn length_helpers() {
        for len in 0..32i32 {
            assert_eq!(pg_b64_enc_len(len), (len + 2) / 3 * 4);
            assert_eq!(pg_b64_dec_len(len), (len * 3) >> 2);
        }
    }

    #[test]
    fn encode_reference_vectors() {
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
        }
    }

    #[test]
    fn decode_reference_vectors() {
        let cases: &[(&[u8], &[u8])] = &[
            (b"", b""),
            (b"Zg==", b"f"),
            (b"Zm8=", b"fo"),
            (b"Zm9v", b"foo"),
            (b"Zm9vYg==", b"foob"),
            (b"Zm9vYmE=", b"fooba"),
            (b"Zm9vYmFy", b"foobar"),
            (b"AP8Q", b"\0\xff\x10"),
        ];
        for (encoded, plain) in cases {
            assert_eq!(dec(encoded).as_deref(), Some(*plain));
        }
    }

    #[test]
    fn decode_rejects_whitespace_and_bad_padding() {
        for invalid in [b"Z g==".as_slice(), b"Zg=\n", b"=", b"Z=", b"Zg", b"Zg==A"] {
            assert_eq!(dec(invalid), None);
        }
    }

    #[test]
    fn decode_padding_edge_case_matches_c() {
        // C's `end` flag is only set on the first '=', so "Zm=9" decodes to "f".
        assert_eq!(dec(b"Zm=9").as_deref(), Some(b"f".as_slice()));
    }

    #[test]
    fn encode_overflow_zeroes_dst() {
        let mut dst = [b'x'; 3];
        assert_eq!(pg_b64_encode(b"foo", 3, &mut dst, 3), -1);
        assert_eq!(dst, [0; 3]);
    }

    #[test]
    fn decode_overflow_zeroes_dst() {
        let mut dst = [b'x'; 2];
        assert_eq!(pg_b64_decode(b"Zm9v", 4, &mut dst, 2), -1);
        assert_eq!(dst, [0; 2]);
    }
}
