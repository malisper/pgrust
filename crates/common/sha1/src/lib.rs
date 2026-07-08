//! src/common/sha1.c — the KAME reference SHA-1 (FIPS 180-1) PostgreSQL
//! ships for builds without a crypto backend.

pub const SHA1_DIGEST_LENGTH: usize = 20;
const BLOCK: usize = 64;

static K: [u32; 4] = [0x5a827999, 0x6ed9eba1, 0x8f1bbcdc, 0xca62c1d6];

#[derive(Clone)]
pub struct Sha1 {
    h: [u32; 5],
    // Message bit count.
    c: u64,
    m: [u8; BLOCK],
    // Byte position within the current block.
    count: u8,
}

// C aliases m.b8 as m.b32 after a host-order byte-swap; assembling each
// schedule word big-endian reproduces the identical value on any host.
#[inline]
fn w_get(m: &[u8; BLOCK], n: usize) -> u32 {
    u32::from_be_bytes(m[n * 4..n * 4 + 4].try_into().unwrap())
}

#[inline]
fn w_set(m: &mut [u8; BLOCK], n: usize, v: u32) {
    m[n * 4..n * 4 + 4].copy_from_slice(&v.to_be_bytes());
}

fn step(ctx: &mut Sha1) {
    let [mut a, mut b, mut c, mut d, mut e] = ctx.h;
    for t in 0..80usize {
        let sidx = t & 0x0f;
        if t >= 16 {
            let w = (w_get(&ctx.m, (sidx + 13) & 0x0f)
                ^ w_get(&ctx.m, (sidx + 8) & 0x0f)
                ^ w_get(&ctx.m, (sidx + 2) & 0x0f)
                ^ w_get(&ctx.m, sidx))
            .rotate_left(1);
            w_set(&mut ctx.m, sidx, w);
        }
        let f = match t / 20 {
            0 => (b & c) | ((!b) & d),
            2 => (b & c) | (b & d) | (c & d),
            _ => b ^ c ^ d,
        };
        let tmp = a
            .rotate_left(5)
            .wrapping_add(f)
            .wrapping_add(e)
            .wrapping_add(w_get(&ctx.m, sidx))
            .wrapping_add(K[t / 20]);
        e = d;
        d = c;
        c = b.rotate_left(30);
        b = a;
        a = tmp;
    }
    for (h, v) in ctx.h.iter_mut().zip([a, b, c, d, e]) {
        *h = h.wrapping_add(v);
    }
    ctx.m = [0; BLOCK];
}

impl Sha1 {
    pub fn init() -> Self {
        Sha1 {
            h: [0x67452301, 0xefcdab89, 0x98badcfe, 0x10325476, 0xc3d2e1f0],
            c: 0,
            m: [0; BLOCK],
            count: 0,
        }
    }

    pub fn update(&mut self, data: &[u8]) {
        let mut off = 0;
        while off < data.len() {
            let gapstart = (self.count as usize) % BLOCK;
            let copysiz = (BLOCK - gapstart).min(data.len() - off);
            self.m[gapstart..gapstart + copysiz].copy_from_slice(&data[off..off + copysiz]);
            self.count = self.count.wrapping_add(copysiz as u8) % BLOCK as u8;
            self.c = self.c.wrapping_add(copysiz as u64 * 8);
            if self.count == 0 {
                step(self);
            }
            off += copysiz;
        }
    }

    pub fn finish(mut self) -> [u8; SHA1_DIGEST_LENGTH] {
        self.putpad(0x80);
        let mut padstart = (self.count as usize) % BLOCK;
        let mut padlen = BLOCK - padstart;
        if padlen < 8 {
            self.m[padstart..].fill(0);
            self.count = 0;
            step(&mut self);
            padstart = 0;
            padlen = BLOCK;
        }
        self.m[padstart..padstart + padlen - 8].fill(0);
        self.count = self.count.wrapping_add((padlen - 8) as u8) % BLOCK as u8;
        for byte in self.c.to_be_bytes() {
            self.putpad(byte);
        }
        let mut digest = [0u8; SHA1_DIGEST_LENGTH];
        for (n, word) in self.h.iter().enumerate() {
            digest[n * 4..n * 4 + 4].copy_from_slice(&word.to_be_bytes());
        }
        digest
    }

    fn putpad(&mut self, x: u8) {
        self.m[(self.count as usize) % BLOCK] = x;
        self.count = self.count.wrapping_add(1) % BLOCK as u8;
        if self.count == 0 {
            step(self);
        }
    }
}

pub fn sha1(data: &[u8]) -> [u8; SHA1_DIGEST_LENGTH] {
    let mut ctx = Sha1::init();
    ctx.update(data);
    ctx.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(d: &[u8]) -> String {
        d.iter().map(|b| format!("{b:02x}")).collect()
    }

    // FIPS 180-1 / RFC 3174 vectors.
    #[test]
    fn vectors() {
        assert_eq!(hex(&sha1(b"abc")), "a9993e364706816aba3e25717850c26c9cd0d89d");
        assert_eq!(
            hex(&sha1(b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq")),
            "84983e441c3bd26ebaae4aa1f95129e5e54670f1"
        );
        assert_eq!(hex(&sha1(b"")), "da39a3ee5e6b4b0d3255bfef95601890afd80709");
        let million_a = vec![b'a'; 1_000_000];
        assert_eq!(hex(&sha1(&million_a)), "34aa973cd4c4daa4f61eeb2bdbad27316534016f");
    }

    #[test]
    fn incremental_matches_oneshot() {
        let data = b"the quick brown fox jumps over the lazy dog and keeps going";
        let mut ctx = Sha1::init();
        for chunk in data.chunks(7) {
            ctx.update(chunk);
        }
        assert_eq!(ctx.finish(), sha1(data));
    }
}
