//! FreeSec DES crypt — a self-contained native Rust port of pgcrypto's bundled
//! `contrib/pgcrypto/crypt-des.c` (FreeSec / David Burren). Implements the
//! traditional DES `crypt(3)` and the BSDI "extended" DES (`_`-prefixed, xdes)
//! so the salt-perturbed DES matches the C byte-for-byte — including the
//! adversarial masking behaviour of `ascii_to_bin` for chars outside
//! `./0-9A-Za-z` (e.g. `crypt('password', '_/!!!!!!!')`).
//!
//! No `pwhash` crate is used for DES/xdes: `pwhash` errors on `!` in the salt,
//! whereas C silently maps such chars to 0 and copies the literal setting prefix
//! into the output. This port reproduces that behaviour exactly.

use std::num::Wrapping as W;
use std::sync::OnceLock;

const CRYPT_A64: &[u8; 64] =
    b"./0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

const IP: [u8; 64] = [
    58, 50, 42, 34, 26, 18, 10, 2, 60, 52, 44, 36, 28, 20, 12, 4, 62, 54, 46, 38, 30, 22, 14, 6,
    64, 56, 48, 40, 32, 24, 16, 8, 57, 49, 41, 33, 25, 17, 9, 1, 59, 51, 43, 35, 27, 19, 11, 3, 61,
    53, 45, 37, 29, 21, 13, 5, 63, 55, 47, 39, 31, 23, 15, 7,
];

const KEY_PERM: [u8; 56] = [
    57, 49, 41, 33, 25, 17, 9, 1, 58, 50, 42, 34, 26, 18, 10, 2, 59, 51, 43, 35, 27, 19, 11, 3, 60,
    52, 44, 36, 63, 55, 47, 39, 31, 23, 15, 7, 62, 54, 46, 38, 30, 22, 14, 6, 61, 53, 45, 37, 29,
    21, 13, 5, 28, 20, 12, 4,
];

const KEY_SHIFTS: [u8; 16] = [1, 1, 2, 2, 2, 2, 2, 2, 1, 2, 2, 2, 2, 2, 2, 1];

const COMP_PERM: [u8; 48] = [
    14, 17, 11, 24, 1, 5, 3, 28, 15, 6, 21, 10, 23, 19, 12, 4, 26, 8, 16, 7, 27, 20, 13, 2, 41, 52,
    31, 37, 47, 55, 30, 40, 51, 45, 33, 48, 44, 49, 39, 56, 34, 53, 46, 42, 50, 36, 29, 32,
];

const SBOX: [[u8; 64]; 8] = [
    [
        14, 4, 13, 1, 2, 15, 11, 8, 3, 10, 6, 12, 5, 9, 0, 7, 0, 15, 7, 4, 14, 2, 13, 1, 10, 6, 12,
        11, 9, 5, 3, 8, 4, 1, 14, 8, 13, 6, 2, 11, 15, 12, 9, 7, 3, 10, 5, 0, 15, 12, 8, 2, 4, 9,
        1, 7, 5, 11, 3, 14, 10, 0, 6, 13,
    ],
    [
        15, 1, 8, 14, 6, 11, 3, 4, 9, 7, 2, 13, 12, 0, 5, 10, 3, 13, 4, 7, 15, 2, 8, 14, 12, 0, 1,
        10, 6, 9, 11, 5, 0, 14, 7, 11, 10, 4, 13, 1, 5, 8, 12, 6, 9, 3, 2, 15, 13, 8, 10, 1, 3, 15,
        4, 2, 11, 6, 7, 12, 0, 5, 14, 9,
    ],
    [
        10, 0, 9, 14, 6, 3, 15, 5, 1, 13, 12, 7, 11, 4, 2, 8, 13, 7, 0, 9, 3, 4, 6, 10, 2, 8, 5,
        14, 12, 11, 15, 1, 13, 6, 4, 9, 8, 15, 3, 0, 11, 1, 2, 12, 5, 10, 14, 7, 1, 10, 13, 0, 6,
        9, 8, 7, 4, 15, 14, 3, 11, 5, 2, 12,
    ],
    [
        7, 13, 14, 3, 0, 6, 9, 10, 1, 2, 8, 5, 11, 12, 4, 15, 13, 8, 11, 5, 6, 15, 0, 3, 4, 7, 2,
        12, 1, 10, 14, 9, 10, 6, 9, 0, 12, 11, 7, 13, 15, 1, 3, 14, 5, 2, 8, 4, 3, 15, 0, 6, 10, 1,
        13, 8, 9, 4, 5, 11, 12, 7, 2, 14,
    ],
    [
        2, 12, 4, 1, 7, 10, 11, 6, 8, 5, 3, 15, 13, 0, 14, 9, 14, 11, 2, 12, 4, 7, 13, 1, 5, 0, 15,
        10, 3, 9, 8, 6, 4, 2, 1, 11, 10, 13, 7, 8, 15, 9, 12, 5, 6, 3, 0, 14, 11, 8, 12, 7, 1, 14,
        2, 13, 6, 15, 0, 9, 10, 4, 5, 3,
    ],
    [
        12, 1, 10, 15, 9, 2, 6, 8, 0, 13, 3, 4, 14, 7, 5, 11, 10, 15, 4, 2, 7, 12, 9, 5, 6, 1, 13,
        14, 0, 11, 3, 8, 9, 14, 15, 5, 2, 8, 12, 3, 7, 0, 4, 10, 1, 13, 11, 6, 4, 3, 2, 12, 9, 5,
        15, 10, 11, 14, 1, 7, 6, 0, 8, 13,
    ],
    [
        4, 11, 2, 14, 15, 0, 8, 13, 3, 12, 9, 7, 5, 10, 6, 1, 13, 0, 11, 7, 4, 9, 1, 10, 14, 3, 5,
        12, 2, 15, 8, 6, 1, 4, 11, 13, 12, 3, 7, 14, 10, 15, 6, 8, 0, 5, 9, 2, 6, 11, 13, 8, 1, 4,
        10, 7, 9, 5, 0, 15, 14, 2, 3, 12,
    ],
    [
        13, 2, 8, 4, 6, 15, 11, 1, 10, 9, 3, 14, 5, 0, 12, 7, 1, 15, 13, 8, 10, 3, 7, 4, 12, 5, 6,
        11, 0, 14, 9, 2, 7, 11, 4, 1, 9, 12, 14, 2, 0, 6, 10, 13, 15, 3, 5, 8, 2, 1, 14, 7, 4, 10,
        8, 13, 15, 12, 9, 0, 3, 5, 6, 11,
    ],
];

const PBOX: [u8; 32] = [
    16, 7, 20, 21, 29, 12, 28, 17, 1, 15, 23, 26, 5, 18, 31, 10, 2, 8, 24, 14, 32, 27, 3, 9, 19,
    13, 30, 6, 22, 11, 4, 25,
];

const CRYPT_BITS32: [u32; 32] = [
    0x80000000, 0x40000000, 0x20000000, 0x10000000, 0x08000000, 0x04000000, 0x02000000, 0x01000000,
    0x00800000, 0x00400000, 0x00200000, 0x00100000, 0x00080000, 0x00040000, 0x00020000, 0x00010000,
    0x00008000, 0x00004000, 0x00002000, 0x00001000, 0x00000800, 0x00000400, 0x00000200, 0x00000100,
    0x00000080, 0x00000040, 0x00000020, 0x00000010, 0x00000008, 0x00000004, 0x00000002, 0x00000001,
];

const CRYPT_BITS8: [u8; 8] = [0x80, 0x40, 0x20, 0x10, 0x08, 0x04, 0x02, 0x01];

/// The immutable lookup tables built once by `des_init` (mirrors C's globals).
struct DesTables {
    m_sbox: [[u8; 4096]; 4],
    psbox: [[u32; 256]; 4],
    ip_maskl: [[u32; 256]; 8],
    ip_maskr: [[u32; 256]; 8],
    fp_maskl: [[u32; 256]; 8],
    fp_maskr: [[u32; 256]; 8],
    key_perm_maskl: [[u32; 128]; 8],
    key_perm_maskr: [[u32; 128]; 8],
    comp_maskl: [[u32; 128]; 8],
    comp_maskr: [[u32; 128]; 8],
}

fn des_init() -> Box<DesTables> {
    // bits28 = &CRYPT_BITS32[4..]; bits24 = &CRYPT_BITS32[8..]
    let bits28 = |o: usize| CRYPT_BITS32[4 + o];
    let bits24 = |o: usize| CRYPT_BITS32[8 + o];

    // Invert the S-boxes, reordering the input bits.
    let mut u_sbox = [[0u8; 64]; 8];
    for i in 0..8 {
        for j in 0..64usize {
            let b = (j & 0x20) | ((j & 1) << 4) | ((j >> 1) & 0xf);
            u_sbox[i][j] = SBOX[i][b];
        }
    }

    // Convert the inverted S-boxes into 4 arrays of 8 bits.
    let mut m_sbox = [[0u8; 4096]; 4];
    for b in 0..4 {
        for i in 0..64usize {
            for j in 0..64usize {
                m_sbox[b][(i << 6) | j] = (u_sbox[b << 1][i] << 4) | u_sbox[(b << 1) + 1][j];
            }
        }
    }

    // Set up initial & final permutations; initialise inverted key permutation.
    let mut init_perm = [0u8; 64];
    let mut final_perm = [0u8; 64];
    let mut inv_key_perm = [255u8; 64];
    for i in 0..64usize {
        final_perm[i] = IP[i] - 1;
        init_perm[(IP[i] - 1) as usize] = i as u8;
        inv_key_perm[i] = 255;
    }

    // Invert the key permutation; init inverted key compression permutation.
    let mut inv_comp_perm = [255u8; 56];
    for i in 0..56usize {
        inv_key_perm[(KEY_PERM[i] - 1) as usize] = i as u8;
        inv_comp_perm[i] = 255;
    }

    // Invert the key compression permutation.
    for i in 0..48usize {
        inv_comp_perm[(COMP_PERM[i] - 1) as usize] = i as u8;
    }

    let mut ip_maskl = [[0u32; 256]; 8];
    let mut ip_maskr = [[0u32; 256]; 8];
    let mut fp_maskl = [[0u32; 256]; 8];
    let mut fp_maskr = [[0u32; 256]; 8];
    let mut key_perm_maskl = [[0u32; 128]; 8];
    let mut key_perm_maskr = [[0u32; 128]; 8];
    let mut comp_maskl = [[0u32; 128]; 8];
    let mut comp_maskr = [[0u32; 128]; 8];

    for k in 0..8usize {
        for i in 0..256usize {
            let mut il = 0u32;
            let mut ir = 0u32;
            let mut fl = 0u32;
            let mut fr = 0u32;
            for j in 0..8usize {
                let inbit = 8 * k + j;
                if (i as u8 & CRYPT_BITS8[j]) != 0 {
                    let obit = init_perm[inbit] as usize;
                    if obit < 32 {
                        il |= CRYPT_BITS32[obit];
                    } else {
                        ir |= CRYPT_BITS32[obit - 32];
                    }
                    let obit = final_perm[inbit] as usize;
                    if obit < 32 {
                        fl |= CRYPT_BITS32[obit];
                    } else {
                        fr |= CRYPT_BITS32[obit - 32];
                    }
                }
            }
            ip_maskl[k][i] = il;
            ip_maskr[k][i] = ir;
            fp_maskl[k][i] = fl;
            fp_maskr[k][i] = fr;
        }
        for i in 0..128usize {
            let mut il = 0u32;
            let mut ir = 0u32;
            for j in 0..7usize {
                let inbit = 8 * k + j;
                if (i as u8 & CRYPT_BITS8[j + 1]) != 0 {
                    let obit = inv_key_perm[inbit];
                    if obit == 255 {
                        continue;
                    }
                    let obit = obit as usize;
                    if obit < 28 {
                        il |= bits28(obit);
                    } else {
                        ir |= bits28(obit - 28);
                    }
                }
            }
            key_perm_maskl[k][i] = il;
            key_perm_maskr[k][i] = ir;

            let mut il = 0u32;
            let mut ir = 0u32;
            for j in 0..7usize {
                let inbit = 7 * k + j;
                if (i as u8 & CRYPT_BITS8[j + 1]) != 0 {
                    let obit = inv_comp_perm[inbit];
                    if obit == 255 {
                        continue;
                    }
                    let obit = obit as usize;
                    if obit < 24 {
                        il |= bits24(obit);
                    } else {
                        ir |= bits24(obit - 24);
                    }
                }
            }
            comp_maskl[k][i] = il;
            comp_maskr[k][i] = ir;
        }
    }

    // Invert the P-box permutation, convert into OR-masks for the S-box output.
    let mut un_pbox = [0u8; 32];
    for i in 0..32usize {
        un_pbox[(PBOX[i] - 1) as usize] = i as u8;
    }

    let mut psbox = [[0u32; 256]; 4];
    for b in 0..4usize {
        for i in 0..256usize {
            let mut p = 0u32;
            for j in 0..8usize {
                if (i as u8 & CRYPT_BITS8[j]) != 0 {
                    p |= CRYPT_BITS32[un_pbox[8 * b + j] as usize];
                }
            }
            psbox[b][i] = p;
        }
    }

    Box::new(DesTables {
        m_sbox,
        psbox,
        ip_maskl,
        ip_maskr,
        fp_maskl,
        fp_maskr,
        key_perm_maskl,
        key_perm_maskr,
        comp_maskl,
        comp_maskr,
    })
}

/// Accessor returning the once-initialised DES lookup tables (mirrors C's
/// `des_init()` building file-scoped statics, but reentrant via `OnceLock`).
fn tables() -> &'static DesTables {
    static TABLES: OnceLock<Box<DesTables>> = OnceLock::new();
    TABLES.get_or_init(des_init)
}

/// Per-call mutable DES state (C used file-scoped statics; we keep them local
/// so the port is reentrant). The `old_*` memoization in C is purely a
/// performance cache and does not affect results, so we omit it.
struct DesState {
    saltbits: u32,
    en_keysl: [u32; 16],
    en_keysr: [u32; 16],
    de_keysl: [u32; 16],
    de_keysr: [u32; 16],
}

impl DesState {
    fn new() -> Self {
        DesState {
            saltbits: 0,
            en_keysl: [0; 16],
            en_keysr: [0; 16],
            de_keysl: [0; 16],
            de_keysr: [0; 16],
        }
    }

    fn setup_salt(&mut self, salt: i64) {
        let mut saltbits = 0u32;
        let mut saltbit: u32 = 1;
        let mut obit: u32 = 0x800000;
        for _ in 0..24 {
            if (salt as u64 & saltbit as u64) != 0 {
                saltbits |= obit;
            }
            saltbit <<= 1;
            obit >>= 1;
        }
        self.saltbits = saltbits;
    }

    fn des_setkey(&mut self, key: &[u8; 8]) {
        let t = tables();
        let rawkey0 = u32::from_be_bytes([key[0], key[1], key[2], key[3]]);
        let rawkey1 = u32::from_be_bytes([key[4], key[5], key[6], key[7]]);

        // Do key permutation and split into two 28-bit subkeys.
        let kpl = &t.key_perm_maskl;
        let kpr = &t.key_perm_maskr;
        let k0 = kpl[0][(rawkey0 >> 25) as usize]
            | kpl[1][((rawkey0 >> 17) & 0x7f) as usize]
            | kpl[2][((rawkey0 >> 9) & 0x7f) as usize]
            | kpl[3][((rawkey0 >> 1) & 0x7f) as usize]
            | kpl[4][(rawkey1 >> 25) as usize]
            | kpl[5][((rawkey1 >> 17) & 0x7f) as usize]
            | kpl[6][((rawkey1 >> 9) & 0x7f) as usize]
            | kpl[7][((rawkey1 >> 1) & 0x7f) as usize];
        let k1 = kpr[0][(rawkey0 >> 25) as usize]
            | kpr[1][((rawkey0 >> 17) & 0x7f) as usize]
            | kpr[2][((rawkey0 >> 9) & 0x7f) as usize]
            | kpr[3][((rawkey0 >> 1) & 0x7f) as usize]
            | kpr[4][(rawkey1 >> 25) as usize]
            | kpr[5][((rawkey1 >> 17) & 0x7f) as usize]
            | kpr[6][((rawkey1 >> 9) & 0x7f) as usize]
            | kpr[7][((rawkey1 >> 1) & 0x7f) as usize];

        // Rotate subkeys and do compression permutation.
        let cml = &t.comp_maskl;
        let cmr = &t.comp_maskr;
        let mut shifts = 0u32;
        for round in 0..16usize {
            shifts += KEY_SHIFTS[round] as u32;

            let t0 = (W(k0) << shifts as usize).0 | (k0 >> (28 - shifts) as usize);
            let t1 = (W(k1) << shifts as usize).0 | (k1 >> (28 - shifts) as usize);

            let el = cml[0][((t0 >> 21) & 0x7f) as usize]
                | cml[1][((t0 >> 14) & 0x7f) as usize]
                | cml[2][((t0 >> 7) & 0x7f) as usize]
                | cml[3][(t0 & 0x7f) as usize]
                | cml[4][((t1 >> 21) & 0x7f) as usize]
                | cml[5][((t1 >> 14) & 0x7f) as usize]
                | cml[6][((t1 >> 7) & 0x7f) as usize]
                | cml[7][(t1 & 0x7f) as usize];
            self.en_keysl[round] = el;
            self.de_keysl[15 - round] = el;

            let er = cmr[0][((t0 >> 21) & 0x7f) as usize]
                | cmr[1][((t0 >> 14) & 0x7f) as usize]
                | cmr[2][((t0 >> 7) & 0x7f) as usize]
                | cmr[3][(t0 & 0x7f) as usize]
                | cmr[4][((t1 >> 21) & 0x7f) as usize]
                | cmr[5][((t1 >> 14) & 0x7f) as usize]
                | cmr[6][((t1 >> 7) & 0x7f) as usize]
                | cmr[7][(t1 & 0x7f) as usize];
            self.en_keysr[round] = er;
            self.de_keysr[15 - round] = er;
        }
    }

    /// Returns Some((l_out, r_out)) on success; None if count == 0
    /// (mirrors C's `do_des` returning 1 → `px_crypt_des` returns NULL).
    fn do_des(&self, l_in: u32, r_in: u32, count: i32) -> Option<(u32, u32)> {
        let t = tables();

        let (kl1, kr1, mut count) = if count == 0 {
            return None;
        } else if count > 0 {
            (&self.en_keysl, &self.en_keysr, count)
        } else {
            (&self.de_keysl, &self.de_keysr, -count)
        };

        let saltbits = self.saltbits;

        // Initial permutation (IP).
        let ipl = &t.ip_maskl;
        let ipr = &t.ip_maskr;
        let mut l = ipl[0][(l_in >> 24) as usize]
            | ipl[1][((l_in >> 16) & 0xff) as usize]
            | ipl[2][((l_in >> 8) & 0xff) as usize]
            | ipl[3][(l_in & 0xff) as usize]
            | ipl[4][(r_in >> 24) as usize]
            | ipl[5][((r_in >> 16) & 0xff) as usize]
            | ipl[6][((r_in >> 8) & 0xff) as usize]
            | ipl[7][(r_in & 0xff) as usize];
        let mut r = ipr[0][(l_in >> 24) as usize]
            | ipr[1][((l_in >> 16) & 0xff) as usize]
            | ipr[2][((l_in >> 8) & 0xff) as usize]
            | ipr[3][(l_in & 0xff) as usize]
            | ipr[4][(r_in >> 24) as usize]
            | ipr[5][((r_in >> 16) & 0xff) as usize]
            | ipr[6][((r_in >> 8) & 0xff) as usize]
            | ipr[7][(r_in & 0xff) as usize];

        let psbox = &t.psbox;
        let m_sbox = &t.m_sbox;

        let mut f = 0u32;
        while count != 0 {
            count -= 1;
            let kl = kl1;
            let kr = kr1;
            let mut round = 16usize;
            let mut ki = 0usize;
            while round != 0 {
                round -= 1;
                // Expand R to 48 bits (simulate the E-box).
                let mut r48l = ((r & 0x00000001) << 23)
                    | ((r & 0xf8000000) >> 9)
                    | ((r & 0x1f800000) >> 11)
                    | ((r & 0x01f80000) >> 13)
                    | ((r & 0x001f8000) >> 15);
                let mut r48r = ((r & 0x0001f800) << 7)
                    | ((r & 0x00001f80) << 5)
                    | ((r & 0x000001f8) << 3)
                    | ((r & 0x0000001f) << 1)
                    | ((r & 0x80000000) >> 31);

                // Salting + XOR with permuted key.
                f = (r48l ^ r48r) & saltbits;
                r48l ^= f ^ kl[ki];
                r48r ^= f ^ kr[ki];
                ki += 1;

                // S-box lookups + P-box permutation.
                f = psbox[0][m_sbox[0][(r48l >> 12) as usize] as usize]
                    | psbox[1][m_sbox[1][(r48l & 0xfff) as usize] as usize]
                    | psbox[2][m_sbox[2][(r48r >> 12) as usize] as usize]
                    | psbox[3][m_sbox[3][(r48r & 0xfff) as usize] as usize];

                f ^= l;
                l = r;
                r = f;
            }
            r = l;
            l = f;
        }

        // Final permutation (inverse of IP).
        let fpl = &t.fp_maskl;
        let fpr = &t.fp_maskr;
        let l_out = fpl[0][(l >> 24) as usize]
            | fpl[1][((l >> 16) & 0xff) as usize]
            | fpl[2][((l >> 8) & 0xff) as usize]
            | fpl[3][(l & 0xff) as usize]
            | fpl[4][(r >> 24) as usize]
            | fpl[5][((r >> 16) & 0xff) as usize]
            | fpl[6][((r >> 8) & 0xff) as usize]
            | fpl[7][(r & 0xff) as usize];
        let r_out = fpr[0][(l >> 24) as usize]
            | fpr[1][((l >> 16) & 0xff) as usize]
            | fpr[2][((l >> 8) & 0xff) as usize]
            | fpr[3][(l & 0xff) as usize]
            | fpr[4][(r >> 24) as usize]
            | fpr[5][((r >> 16) & 0xff) as usize]
            | fpr[6][((r >> 8) & 0xff) as usize]
            | fpr[7][(r & 0xff) as usize];

        Some((l_out, r_out))
    }

    /// `des_cipher(in, out, salt, count)` — encrypt one 8-byte block.
    /// Returns None on failure (count == 0).
    fn des_cipher(&mut self, input: &[u8; 8], salt: i64, count: i32) -> Option<[u8; 8]> {
        self.setup_salt(salt);
        let rawl = u32::from_be_bytes([input[0], input[1], input[2], input[3]]);
        let rawr = u32::from_be_bytes([input[4], input[5], input[6], input[7]]);
        let (l_out, r_out) = self.do_des(rawl, rawr, count)?;
        let mut out = [0u8; 8];
        // pg_hton32 = to_big_endian
        out[0..4].copy_from_slice(&l_out.to_be_bytes());
        out[4..8].copy_from_slice(&r_out.to_be_bytes());
        Some(out)
    }
}

/// `ascii_to_bin(ch)` — masks chars outside `./0-9A-Za-z` to 0.
fn ascii_to_bin(ch: u8) -> u32 {
    let ch = ch as i32;
    if ch > b'z' as i32 {
        return 0;
    }
    if ch >= b'a' as i32 {
        return (ch - b'a' as i32 + 38) as u32;
    }
    if ch > b'Z' as i32 {
        return 0;
    }
    if ch >= b'A' as i32 {
        return (ch - b'A' as i32 + 12) as u32;
    }
    if ch > b'9' as i32 {
        return 0;
    }
    if ch >= b'.' as i32 {
        return (ch - b'.' as i32) as u32;
    }
    0
}

/// Faithful port of `px_crypt_des(key, setting)`. Returns the encoded crypt
/// string, or `None` when the C returns NULL (e.g. count == 0).
///
/// The length checks that C raises as `ereport(ERROR, "invalid salt")` are NOT
/// done here — they are handled by the callers (`crypt_des` / `crypt_xdes`) so
/// the error text and ordering match exactly.
pub fn px_crypt_des(key: &[u8], setting: &[u8]) -> Option<Vec<u8>> {
    let mut st = DesState::new();

    // Copy the key, shifting each character up by one bit and padding with
    // zeros. `key` mirrors C's NUL-terminated string: once we hit the end we
    // stop advancing and pad with 0.
    let mut keybuf = [0u8; 8];
    let mut kpos = 0usize; // index into key (treating key as NUL-terminated)
    for byte in keybuf.iter_mut() {
        let c = if kpos < key.len() { key[kpos] } else { 0 };
        *byte = c.wrapping_shl(1); // *key << 1, low byte kept
        if c != 0 {
            kpos += 1;
        }
    }
    st.des_setkey(&keybuf);

    let count: i32;
    let salt: i64;
    let mut output: Vec<u8> = Vec::with_capacity(21);

    if setting.first() == Some(&b'_') {
        // "new"-style (xdes). Caller guarantees setting.len() >= 9.
        let mut c: u32 = 0;
        for i in 1..5usize {
            c |= ascii_to_bin(setting[i]) << ((i - 1) * 6);
        }
        let mut s: u32 = 0;
        for i in 5..9usize {
            s |= ascii_to_bin(setting[i]) << ((i - 5) * 6);
        }
        count = c as i32;
        salt = s as i64;

        // Encrypt the key with itself, XORing with successive 8-char chunks.
        while kpos < key.len() && key[kpos] != 0 {
            keybuf = st.des_cipher(&keybuf, 0, 1)?;
            let mut q = 0usize;
            while q < 8 && kpos < key.len() && key[kpos] != 0 {
                keybuf[q] ^= key[kpos].wrapping_shl(1);
                q += 1;
                kpos += 1;
            }
            st.des_setkey(&keybuf);
        }

        // strlcpy(output, setting, 10): copy up to 9 chars (NUL-stop).
        for &b in setting.iter().take(9) {
            if b == 0 {
                break;
            }
            output.push(b);
        }
    } else {
        // "old"-style (traditional). Caller guarantees setting.len() >= 2.
        count = 25;
        salt = ((ascii_to_bin(setting[1]) << 6) | ascii_to_bin(setting[0])) as i64;

        output.push(setting[0]);
        // output[1] = setting[1] ? setting[1] : output[0]
        output.push(if setting[1] != 0 {
            setting[1]
        } else {
            setting[0]
        });
    }

    st.setup_salt(salt);

    // Do it.
    let (r0, r1) = st.do_des(0, 0, count)?;

    // Encode the result.
    let a64 = CRYPT_A64;
    let l = r0 >> 8;
    output.push(a64[((l >> 18) & 0x3f) as usize]);
    output.push(a64[((l >> 12) & 0x3f) as usize]);
    output.push(a64[((l >> 6) & 0x3f) as usize]);
    output.push(a64[(l & 0x3f) as usize]);

    let l = (r0 << 16) | ((r1 >> 16) & 0xffff);
    output.push(a64[((l >> 18) & 0x3f) as usize]);
    output.push(a64[((l >> 12) & 0x3f) as usize]);
    output.push(a64[((l >> 6) & 0x3f) as usize]);
    output.push(a64[(l & 0x3f) as usize]);

    let l = r1 << 2;
    output.push(a64[((l >> 12) & 0x3f) as usize]);
    output.push(a64[((l >> 6) & 0x3f) as usize]);
    output.push(a64[(l & 0x3f) as usize]);

    Some(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper mirroring the desc.rs callers, so we test px_crypt_des end-to-end.
    fn xdes(pw: &[u8], setting: &[u8]) -> Result<String, String> {
        if setting.len() < 9 {
            return Err("invalid salt".to_string());
        }
        match px_crypt_des(pw, setting) {
            Some(out) => Ok(String::from_utf8_lossy(&out).into_owned()),
            None => Err("crypt(3) returned NULL".to_string()),
        }
    }

    fn des(pw: &[u8], setting: &[u8]) -> Result<String, String> {
        if setting.len() < 2 {
            return Err("invalid salt".to_string());
        }
        match px_crypt_des(pw, setting) {
            Some(out) => Ok(String::from_utf8_lossy(&out).into_owned()),
            None => Err("crypt(3) returned NULL".to_string()),
        }
    }

    #[test]
    fn cryptdes_xdes_known_vectors() {
        assert_eq!(xdes(b"", b"_J9..j2zz").unwrap(), "_J9..j2zzR/nIRDK3pPc");
        assert_eq!(xdes(b"foox", b"_J9..j2zz").unwrap(), "_J9..j2zzAYKMvO2BYRY");
        assert_eq!(
            xdes(b"longlongpassword", b"_J9..j2zz").unwrap(),
            "_J9..j2zz4BeseiQNwUg"
        );
    }

    #[test]
    fn cryptdes_xdes_adversarial_bang_salt() {
        assert_eq!(xdes(b"password", b"_/!!!!!!!").unwrap(), "_/!!!!!!!zqM49hRzxko");
    }

    #[test]
    fn cryptdes_xdes_count_zero_returns_null() {
        assert_eq!(
            xdes(b"password", b"_........").unwrap_err(),
            "crypt(3) returned NULL"
        );
        assert_eq!(
            xdes(b"password", b"_..!!!!!!").unwrap_err(),
            "crypt(3) returned NULL"
        );
    }

    #[test]
    fn cryptdes_xdes_short_setting_invalid_salt() {
        assert_eq!(xdes(b"foox", b"_J9..BWH").unwrap_err(), "invalid salt");
    }

    #[test]
    fn cryptdes_traditional_known_vector() {
        // Traditional DES crypt: 2-char salt "rl", classic vector.
        assert_eq!(des(b"foob", b"rl").unwrap(), "rlK6kmJqyMjZM");
    }
}
