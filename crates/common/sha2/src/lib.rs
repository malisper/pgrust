// C routes SHA-2 through cryptohash_openssl in target builds; this build links
// no OpenSSL, so the digest engines are C's fallback sha2.c (OpenBSD/Gifford
// reference). Output identical by construction (FIPS 180-4); infallible here —
// C's -1/errstr arms are OOM/EVP-failure only.

pub const PG_SHA224_BLOCK_LENGTH: usize = 64;
pub const PG_SHA224_DIGEST_LENGTH: usize = 28;
pub const PG_SHA256_BLOCK_LENGTH: usize = 64;
pub const PG_SHA256_DIGEST_LENGTH: usize = 32;
pub const PG_SHA384_BLOCK_LENGTH: usize = 128;
pub const PG_SHA384_DIGEST_LENGTH: usize = 48;
pub const PG_SHA512_BLOCK_LENGTH: usize = 128;
pub const PG_SHA512_DIGEST_LENGTH: usize = 64;

const SHA256_SHORT_BLOCK: usize = PG_SHA256_BLOCK_LENGTH - 8;
const SHA512_SHORT_BLOCK: usize = PG_SHA512_BLOCK_LENGTH - 16;

const K256: [u32; 64] = [
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
];

const SHA224_H0: [u32; 8] = [
    0xc1059ed8, 0x367cd507, 0x3070dd17, 0xf70e5939, 0xffc00b31, 0x68581511, 0x64f98fa7, 0xbefa4fa4,
];

const SHA256_H0: [u32; 8] = [
    0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
];

const K512: [u64; 80] = [
    0x428a2f98d728ae22, 0x7137449123ef65cd, 0xb5c0fbcfec4d3b2f, 0xe9b5dba58189dbbc,
    0x3956c25bf348b538, 0x59f111f1b605d019, 0x923f82a4af194f9b, 0xab1c5ed5da6d8118,
    0xd807aa98a3030242, 0x12835b0145706fbe, 0x243185be4ee4b28c, 0x550c7dc3d5ffb4e2,
    0x72be5d74f27b896f, 0x80deb1fe3b1696b1, 0x9bdc06a725c71235, 0xc19bf174cf692694,
    0xe49b69c19ef14ad2, 0xefbe4786384f25e3, 0x0fc19dc68b8cd5b5, 0x240ca1cc77ac9c65,
    0x2de92c6f592b0275, 0x4a7484aa6ea6e483, 0x5cb0a9dcbd41fbd4, 0x76f988da831153b5,
    0x983e5152ee66dfab, 0xa831c66d2db43210, 0xb00327c898fb213f, 0xbf597fc7beef0ee4,
    0xc6e00bf33da88fc2, 0xd5a79147930aa725, 0x06ca6351e003826f, 0x142929670a0e6e70,
    0x27b70a8546d22ffc, 0x2e1b21385c26c926, 0x4d2c6dfc5ac42aed, 0x53380d139d95b3df,
    0x650a73548baf63de, 0x766a0abb3c77b2a8, 0x81c2c92e47edaee6, 0x92722c851482353b,
    0xa2bfe8a14cf10364, 0xa81a664bbc423001, 0xc24b8b70d0f89791, 0xc76c51a30654be30,
    0xd192e819d6ef5218, 0xd69906245565a910, 0xf40e35855771202a, 0x106aa07032bbd1b8,
    0x19a4c116b8d2d0c8, 0x1e376c085141ab53, 0x2748774cdf8eeb99, 0x34b0bcb5e19b48a8,
    0x391c0cb3c5c95a63, 0x4ed8aa4ae3418acb, 0x5b9cca4f7763e373, 0x682e6ff3d6b2b8a3,
    0x748f82ee5defb2fc, 0x78a5636f43172f60, 0x84c87814a1f0ab72, 0x8cc702081a6439ec,
    0x90befffa23631e28, 0xa4506cebde82bde9, 0xbef9a3f7b2c67915, 0xc67178f2e372532b,
    0xca273eceea26619c, 0xd186b8c721c0c207, 0xeada7dd6cde0eb1e, 0xf57d4f7fee6ed178,
    0x06f067aa72176fba, 0x0a637dc5a2c898a6, 0x113f9804bef90dae, 0x1b710b35131c471b,
    0x28db77f523047d84, 0x32caab7b40c72493, 0x3c9ebe0a15c9bebc, 0x431d67c49c100d4c,
    0x4cc5d4becb3e42b6, 0x597f299cfc657e2a, 0x5fcb6fab3ad6faec, 0x6c44198c4a475817,
];

const SHA384_H0: [u64; 8] = [
    0xcbbb9d5dc1059ed8, 0x629a292a367cd507, 0x9159015a3070dd17, 0x152fecd8f70e5939,
    0x67332667ffc00b31, 0x8eb44a8768581511, 0xdb0c2e0d64f98fa7, 0x47b5481dbefa4fa4,
];

const SHA512_H0: [u64; 8] = [
    0x6a09e667f3bcc908, 0xbb67ae8584caa73b, 0x3c6ef372fe94f82b, 0xa54ff53a5f1d36f1,
    0x510e527fade682d1, 0x9b05688c2b3e6c1f, 0x1f83d9abfb41bd6b, 0x5be0cd19137e2179,
];

#[derive(Clone)]
pub struct PgSha256Ctx {
    state: [u32; 8],
    bitcount: u64,
    buffer: [u8; PG_SHA256_BLOCK_LENGTH],
}

#[derive(Clone)]
pub struct PgSha512Ctx {
    state: [u64; 8],
    bitcount: [u64; 2],
    buffer: [u8; PG_SHA512_BLOCK_LENGTH],
}

pub type PgSha224Ctx = PgSha256Ctx;
pub type PgSha384Ctx = PgSha512Ctx;

fn sha256_transform(state: &mut [u32; 8], data: &[u8; PG_SHA256_BLOCK_LENGTH]) {
    let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut h] = *state;
    let mut w = [0u32; 16];
    for j in 0..64 {
        let wj = if j < 16 {
            let v = u32::from_be_bytes(data[j * 4..j * 4 + 4].try_into().unwrap());
            w[j] = v;
            v
        } else {
            let x0 = w[(j + 1) & 0x0f];
            let s0 = x0.rotate_right(7) ^ x0.rotate_right(18) ^ (x0 >> 3);
            let x1 = w[(j + 14) & 0x0f];
            let s1 = x1.rotate_right(17) ^ x1.rotate_right(19) ^ (x1 >> 10);
            let v = w[j & 0x0f]
                .wrapping_add(s1)
                .wrapping_add(w[(j + 9) & 0x0f])
                .wrapping_add(s0);
            w[j & 0x0f] = v;
            v
        };
        let t1 = h
            .wrapping_add(e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25))
            .wrapping_add((e & f) ^ (!e & g))
            .wrapping_add(K256[j])
            .wrapping_add(wj);
        let t2 = (a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22))
            .wrapping_add((a & b) ^ (a & c) ^ (b & c));
        h = g;
        g = f;
        f = e;
        e = d.wrapping_add(t1);
        d = c;
        c = b;
        b = a;
        a = t1.wrapping_add(t2);
    }
    for (s, v) in state.iter_mut().zip([a, b, c, d, e, f, g, h]) {
        *s = s.wrapping_add(v);
    }
}

fn sha512_transform(state: &mut [u64; 8], data: &[u8; PG_SHA512_BLOCK_LENGTH]) {
    let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut h] = *state;
    let mut w = [0u64; 16];
    for j in 0..80 {
        let wj = if j < 16 {
            let v = u64::from_be_bytes(data[j * 8..j * 8 + 8].try_into().unwrap());
            w[j] = v;
            v
        } else {
            let x0 = w[(j + 1) & 0x0f];
            let s0 = x0.rotate_right(1) ^ x0.rotate_right(8) ^ (x0 >> 7);
            let x1 = w[(j + 14) & 0x0f];
            let s1 = x1.rotate_right(19) ^ x1.rotate_right(61) ^ (x1 >> 6);
            let v = w[j & 0x0f]
                .wrapping_add(s1)
                .wrapping_add(w[(j + 9) & 0x0f])
                .wrapping_add(s0);
            w[j & 0x0f] = v;
            v
        };
        let t1 = h
            .wrapping_add(e.rotate_right(14) ^ e.rotate_right(18) ^ e.rotate_right(41))
            .wrapping_add((e & f) ^ (!e & g))
            .wrapping_add(K512[j])
            .wrapping_add(wj);
        let t2 = (a.rotate_right(28) ^ a.rotate_right(34) ^ a.rotate_right(39))
            .wrapping_add((a & b) ^ (a & c) ^ (b & c));
        h = g;
        g = f;
        f = e;
        e = d.wrapping_add(t1);
        d = c;
        c = b;
        b = a;
        a = t1.wrapping_add(t2);
    }
    for (s, v) in state.iter_mut().zip([a, b, c, d, e, f, g, h]) {
        *s = s.wrapping_add(v);
    }
}

impl PgSha256Ctx {
    pub fn init_sha256() -> Self {
        PgSha256Ctx { state: SHA256_H0, bitcount: 0, buffer: [0; PG_SHA256_BLOCK_LENGTH] }
    }

    pub fn init_sha224() -> Self {
        PgSha256Ctx { state: SHA224_H0, bitcount: 0, buffer: [0; PG_SHA256_BLOCK_LENGTH] }
    }

    pub fn update(&mut self, mut data: &[u8]) {
        if data.is_empty() {
            return;
        }
        let usedspace = ((self.bitcount >> 3) as usize) % PG_SHA256_BLOCK_LENGTH;
        if usedspace > 0 {
            let freespace = PG_SHA256_BLOCK_LENGTH - usedspace;
            if data.len() >= freespace {
                self.buffer[usedspace..].copy_from_slice(&data[..freespace]);
                self.bitcount = self.bitcount.wrapping_add((freespace as u64) << 3);
                data = &data[freespace..];
                let block = self.buffer;
                sha256_transform(&mut self.state, &block);
            } else {
                self.buffer[usedspace..usedspace + data.len()].copy_from_slice(data);
                self.bitcount = self.bitcount.wrapping_add((data.len() as u64) << 3);
                return;
            }
        }
        while data.len() >= PG_SHA256_BLOCK_LENGTH {
            let block: &[u8; PG_SHA256_BLOCK_LENGTH] =
                data[..PG_SHA256_BLOCK_LENGTH].try_into().unwrap();
            sha256_transform(&mut self.state, block);
            self.bitcount = self.bitcount.wrapping_add((PG_SHA256_BLOCK_LENGTH as u64) << 3);
            data = &data[PG_SHA256_BLOCK_LENGTH..];
        }
        if !data.is_empty() {
            self.buffer[..data.len()].copy_from_slice(data);
            self.bitcount = self.bitcount.wrapping_add((data.len() as u64) << 3);
        }
    }

    fn last(&mut self) {
        let mut usedspace = ((self.bitcount >> 3) as usize) % PG_SHA256_BLOCK_LENGTH;
        let bitcount_be = self.bitcount.to_be_bytes();
        if usedspace > 0 {
            self.buffer[usedspace] = 0x80;
            usedspace += 1;
            if usedspace <= SHA256_SHORT_BLOCK {
                self.buffer[usedspace..SHA256_SHORT_BLOCK].fill(0);
            } else {
                if usedspace < PG_SHA256_BLOCK_LENGTH {
                    self.buffer[usedspace..].fill(0);
                }
                let block = self.buffer;
                sha256_transform(&mut self.state, &block);
                self.buffer[..SHA256_SHORT_BLOCK].fill(0);
            }
        } else {
            self.buffer[..SHA256_SHORT_BLOCK].fill(0);
            self.buffer[0] = 0x80;
        }
        self.buffer[SHA256_SHORT_BLOCK..SHA256_SHORT_BLOCK + 8].copy_from_slice(&bitcount_be);
        let block = self.buffer;
        sha256_transform(&mut self.state, &block);
    }

    pub fn final_sha256(mut self) -> [u8; PG_SHA256_DIGEST_LENGTH] {
        self.last();
        let mut digest = [0u8; PG_SHA256_DIGEST_LENGTH];
        for j in 0..8 {
            digest[j * 4..j * 4 + 4].copy_from_slice(&self.state[j].to_be_bytes());
        }
        digest
    }

    pub fn final_sha224(mut self) -> [u8; PG_SHA224_DIGEST_LENGTH] {
        self.last();
        let mut digest = [0u8; PG_SHA224_DIGEST_LENGTH];
        for j in 0..7 {
            digest[j * 4..j * 4 + 4].copy_from_slice(&self.state[j].to_be_bytes());
        }
        digest
    }
}

impl PgSha512Ctx {
    pub fn init_sha512() -> Self {
        PgSha512Ctx { state: SHA512_H0, bitcount: [0; 2], buffer: [0; PG_SHA512_BLOCK_LENGTH] }
    }

    pub fn init_sha384() -> Self {
        PgSha512Ctx { state: SHA384_H0, bitcount: [0; 2], buffer: [0; PG_SHA512_BLOCK_LENGTH] }
    }

    fn addinc128(&mut self, n: u64) {
        self.bitcount[0] = self.bitcount[0].wrapping_add(n);
        if self.bitcount[0] < n {
            self.bitcount[1] = self.bitcount[1].wrapping_add(1);
        }
    }

    pub fn update(&mut self, mut data: &[u8]) {
        if data.is_empty() {
            return;
        }
        let usedspace = ((self.bitcount[0] >> 3) as usize) % PG_SHA512_BLOCK_LENGTH;
        if usedspace > 0 {
            let freespace = PG_SHA512_BLOCK_LENGTH - usedspace;
            if data.len() >= freespace {
                self.buffer[usedspace..].copy_from_slice(&data[..freespace]);
                self.addinc128((freespace as u64) << 3);
                data = &data[freespace..];
                let block = self.buffer;
                sha512_transform(&mut self.state, &block);
            } else {
                self.buffer[usedspace..usedspace + data.len()].copy_from_slice(data);
                self.addinc128((data.len() as u64) << 3);
                return;
            }
        }
        while data.len() >= PG_SHA512_BLOCK_LENGTH {
            let block: &[u8; PG_SHA512_BLOCK_LENGTH] =
                data[..PG_SHA512_BLOCK_LENGTH].try_into().unwrap();
            sha512_transform(&mut self.state, block);
            self.addinc128((PG_SHA512_BLOCK_LENGTH as u64) << 3);
            data = &data[PG_SHA512_BLOCK_LENGTH..];
        }
        if !data.is_empty() {
            self.buffer[..data.len()].copy_from_slice(data);
            self.addinc128((data.len() as u64) << 3);
        }
    }

    fn last(&mut self) {
        let mut usedspace = ((self.bitcount[0] >> 3) as usize) % PG_SHA512_BLOCK_LENGTH;
        let hi_be = self.bitcount[1].to_be_bytes();
        let lo_be = self.bitcount[0].to_be_bytes();
        if usedspace > 0 {
            self.buffer[usedspace] = 0x80;
            usedspace += 1;
            if usedspace <= SHA512_SHORT_BLOCK {
                self.buffer[usedspace..SHA512_SHORT_BLOCK].fill(0);
            } else {
                if usedspace < PG_SHA512_BLOCK_LENGTH {
                    self.buffer[usedspace..].fill(0);
                }
                let block = self.buffer;
                sha512_transform(&mut self.state, &block);
                // C clears only BLOCK-2 bytes here (upstream reference quirk);
                // the tail is overwritten by the bit-count store below.
                self.buffer[..PG_SHA512_BLOCK_LENGTH - 2].fill(0);
            }
        } else {
            self.buffer[..SHA512_SHORT_BLOCK].fill(0);
            self.buffer[0] = 0x80;
        }
        self.buffer[SHA512_SHORT_BLOCK..SHA512_SHORT_BLOCK + 8].copy_from_slice(&hi_be);
        self.buffer[SHA512_SHORT_BLOCK + 8..].copy_from_slice(&lo_be);
        let block = self.buffer;
        sha512_transform(&mut self.state, &block);
    }

    pub fn final_sha512(mut self) -> [u8; PG_SHA512_DIGEST_LENGTH] {
        self.last();
        let mut digest = [0u8; PG_SHA512_DIGEST_LENGTH];
        for j in 0..8 {
            digest[j * 8..j * 8 + 8].copy_from_slice(&self.state[j].to_be_bytes());
        }
        digest
    }

    pub fn final_sha384(mut self) -> [u8; PG_SHA384_DIGEST_LENGTH] {
        self.last();
        let mut digest = [0u8; PG_SHA384_DIGEST_LENGTH];
        for j in 0..6 {
            digest[j * 8..j * 8 + 8].copy_from_slice(&self.state[j].to_be_bytes());
        }
        digest
    }
}

pub fn sha224(data: &[u8]) -> [u8; PG_SHA224_DIGEST_LENGTH] {
    let mut ctx = PgSha256Ctx::init_sha224();
    ctx.update(data);
    ctx.final_sha224()
}

pub fn sha256(data: &[u8]) -> [u8; PG_SHA256_DIGEST_LENGTH] {
    let mut ctx = PgSha256Ctx::init_sha256();
    ctx.update(data);
    ctx.final_sha256()
}

pub fn sha384(data: &[u8]) -> [u8; PG_SHA384_DIGEST_LENGTH] {
    let mut ctx = PgSha512Ctx::init_sha384();
    ctx.update(data);
    ctx.final_sha384()
}

pub fn sha512(data: &[u8]) -> [u8; PG_SHA512_DIGEST_LENGTH] {
    let mut ctx = PgSha512Ctx::init_sha512();
    ctx.update(data);
    ctx.final_sha512()
}

#[cfg(test)]
mod tests;
