// C routes MD5 through cryptohash_openssl in target builds; this build links
// no OpenSSL, so the digest engine is C's fallback md5.c. Identical output by
// construction (RFC 1321); infallible here — C's false/errstr arm is
// OOM/EVP-failure only.

pub const MD5_DIGEST_LENGTH: usize = 16;
pub const MD5_HASH_LEN: usize = 32;

const S: [u32; 64] = [
    7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, //
    5, 9, 14, 20, 5, 9, 14, 20, 5, 9, 14, 20, 5, 9, 14, 20, //
    4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, //
    6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21,
];

const K: [u32; 64] = [
    0xd76aa478, 0xe8c7b756, 0x242070db, 0xc1bdceee, 0xf57c0faf, 0x4787c62a, 0xa8304613,
    0xfd469501, 0x698098d8, 0x8b44f7af, 0xffff5bb1, 0x895cd7be, 0x6b901122, 0xfd987193,
    0xa679438e, 0x49b40821, 0xf61e2562, 0xc040b340, 0x265e5a51, 0xe9b6c7aa, 0xd62f105d,
    0x02441453, 0xd8a1e681, 0xe7d3fbc8, 0x21e1cde6, 0xc33707d6, 0xf4d50d87, 0x455a14ed,
    0xa9e3e905, 0xfcefa3f8, 0x676f02d9, 0x8d2a4c8a, 0xfffa3942, 0x8771f681, 0x6d9d6122,
    0xfde5380c, 0xa4beea44, 0x4bdecfa9, 0xf6bb4b60, 0xbebfbc70, 0x289b7ec6, 0xeaa127fa,
    0xd4ef3085, 0x04881d05, 0xd9d4d039, 0xe6db99e5, 0x1fa27cf8, 0xc4ac5665, 0xf4292244,
    0x432aff97, 0xab9423a7, 0xfc93a039, 0x655b59c3, 0x8f0ccc92, 0xffeff47d, 0x85845dd1,
    0x6fa87e4f, 0xfe2ce6e0, 0xa3014314, 0x4e0811a1, 0xf7537e82, 0xbd3af235, 0x2ad7d2bb,
    0xeb86d391,
];

#[derive(Clone)]
pub struct Md5 {
    state: [u32; 4],
    len: u64,
    buf: [u8; 64],
    fill: usize,
}

impl Md5 {
    pub fn new() -> Md5 {
        Md5 {
            state: [0x67452301, 0xefcdab89, 0x98badcfe, 0x10325476],
            len: 0,
            buf: [0; 64],
            fill: 0,
        }
    }

    fn block(&mut self, block: &[u8; 64]) {
        let mut m = [0u32; 16];
        for (i, w) in m.iter_mut().enumerate() {
            *w = u32::from_le_bytes(block[i * 4..i * 4 + 4].try_into().unwrap());
        }
        let [mut a, mut b, mut c, mut d] = self.state;
        for i in 0..64 {
            let (f, g) = match i / 16 {
                0 => ((b & c) | (!b & d), i),
                1 => ((d & b) | (!d & c), (5 * i + 1) % 16),
                2 => (b ^ c ^ d, (3 * i + 5) % 16),
                _ => (c ^ (b | !d), (7 * i) % 16),
            };
            let tmp = d;
            d = c;
            c = b;
            b = b.wrapping_add(
                a.wrapping_add(f)
                    .wrapping_add(K[i])
                    .wrapping_add(m[g])
                    .rotate_left(S[i]),
            );
            a = tmp;
        }
        self.state[0] = self.state[0].wrapping_add(a);
        self.state[1] = self.state[1].wrapping_add(b);
        self.state[2] = self.state[2].wrapping_add(c);
        self.state[3] = self.state[3].wrapping_add(d);
    }

    pub fn update(&mut self, mut data: &[u8]) {
        self.len += data.len() as u64;
        if self.fill > 0 {
            let take = data.len().min(64 - self.fill);
            self.buf[self.fill..self.fill + take].copy_from_slice(&data[..take]);
            self.fill += take;
            data = &data[take..];
            if self.fill == 64 {
                let block = self.buf;
                self.block(&block);
                self.fill = 0;
            }
            if data.is_empty() {
                return;
            }
        }
        while data.len() >= 64 {
            self.block(data[..64].try_into().unwrap());
            data = &data[64..];
        }
        self.buf[..data.len()].copy_from_slice(data);
        self.fill = data.len();
    }

    pub fn finish(mut self) -> [u8; MD5_DIGEST_LENGTH] {
        let bits = self.len.wrapping_mul(8);
        self.update(&[0x80]);
        while self.fill != 56 {
            self.update(&[0]);
        }
        self.update(&bits.to_le_bytes());
        debug_assert_eq!(self.fill, 0);
        let mut out = [0u8; MD5_DIGEST_LENGTH];
        for (i, s) in self.state.iter().enumerate() {
            out[i * 4..i * 4 + 4].copy_from_slice(&s.to_le_bytes());
        }
        out
    }
}

pub fn pg_md5_binary(data: &[u8]) -> [u8; MD5_DIGEST_LENGTH] {
    let mut ctx = Md5::new();
    ctx.update(data);
    ctx.finish()
}

pub fn pg_md5_hash(data: &[u8]) -> [u8; MD5_HASH_LEN] {
    bytes_to_hex(pg_md5_binary(data))
}

pub fn bytes_to_hex(b: [u8; MD5_DIGEST_LENGTH]) -> [u8; MD5_HASH_LEN] {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = [0u8; MD5_HASH_LEN];
    for i in 0..MD5_DIGEST_LENGTH {
        s[i * 2] = HEX[(b[i] >> 4) as usize];
        s[i * 2 + 1] = HEX[(b[i] & 0x0f) as usize];
    }
    s
}

pub const MD5_PASSWD_LEN: usize = 35;

pub const MD5_PASSWD_CHARSET: &[u8] = b"0123456789abcdef";

// md5_common.c pg_md5_encrypt: md5(password || salt), lowercase hex, "md5"
// prefix. Infallible here (C's false/errstr arm is OOM/engine-failure only);
// the concatenation feeds one incremental engine, no temp buffer.
pub fn pg_md5_encrypt(passwd: &[u8], salt: &[u8]) -> [u8; MD5_PASSWD_LEN] {
    let mut ctx = Md5::new();
    ctx.update(passwd);
    ctx.update(salt);
    let hexsum = bytes_to_hex(ctx.finish());
    let mut buf = [0u8; MD5_PASSWD_LEN];
    buf[..3].copy_from_slice(b"md5");
    buf[3..].copy_from_slice(&hexsum);
    buf
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(data: &[u8]) -> String {
        String::from_utf8(pg_md5_hash(data).to_vec()).unwrap()
    }

    #[test]
    fn rfc1321_vectors() {
        assert_eq!(hex(b""), "d41d8cd98f00b204e9800998ecf8427e");
        assert_eq!(hex(b"a"), "0cc175b9c0f1b6a831c399e269772661");
        assert_eq!(hex(b"abc"), "900150983cd24fb0d6963f7d28e17f72");
        assert_eq!(hex(b"message digest"), "f96b697d7cb7938d525a2f31aaf161d0");
        assert_eq!(
            hex(b"abcdefghijklmnopqrstuvwxyz"),
            "c3fcd3d76192e4007dfb496cca67e13b"
        );
        assert_eq!(
            hex(b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"),
            "d174ab98d277d9f5a5611c2c9f419d9f"
        );
        assert_eq!(
            hex(b"12345678901234567890123456789012345678901234567890123456789012345678901234567890"),
            "57edf4a22be3c955ac49da2e2107b67a"
        );
    }

    #[test]
    fn boundary_lengths() {
        // 55/56/57/63/64/65 exercise every padding arm.
        for (n, want) in [
            (55usize, "ef1772b6dff9a122358552954ad0df65"),
            (56, "3b0c8ac703f828b04c6c197006d17218"),
            (57, "652b906d60af96844ebd21b674f35e93"),
            (63, "b06521f39153d618550606be297466d5"),
            (64, "014842d480b571495a4a0363793f7367"),
            (65, "c743a45e0d2e6a95cb859adae0248435"),
        ] {
            assert_eq!(hex(&vec![b'a'; n]), want);
        }
    }

    #[test]
    fn split_updates_match_oneshot() {
        let data: Vec<u8> = (0..1000u32).map(|i| (i % 251) as u8).collect();
        let oneshot = pg_md5_binary(&data);
        for split in [1usize, 7, 63, 64, 65, 130] {
            let mut ctx = Md5::new();
            for chunk in data.chunks(split) {
                ctx.update(chunk);
            }
            assert_eq!(ctx.finish(), oneshot);
        }
    }

    #[test]
    fn pg_md5_encrypt_role_salt() {
        assert_eq!(
            &pg_md5_encrypt(b"secret", b"postgres")[..],
            b"md553f48b7c4b76a86ce72276c5755f217d"
        );
        assert_eq!(&pg_md5_encrypt(b"foo", b"bar")[..], b"md53858f62230ac3c915f300c664312c63f");
    }
}
