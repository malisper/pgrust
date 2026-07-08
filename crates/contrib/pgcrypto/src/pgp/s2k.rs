//! PGP String-to-Key (pgp-s2k.c) — the three S2K modes (simple/salted/
//! iterated-salted) with the multi-round output-extension scheme.

use super::consts::*;
use ::pg_strong_random::pg_strong_random;

/// A PGP S2K specifier + the derived key.
#[derive(Clone)]
pub struct S2k {
    pub mode: i32,
    pub digest_algo: i32,
    pub salt: [u8; PGP_S2K_SALT],
    pub iter: u8,
    pub key: Vec<u8>,
    pub key_len: usize,
}

impl S2k {
    /// `pgp_s2k_fill` — generate the salt/iter parameters for encryption.
    /// `count` is the configured s2k_count (-1 = float default).
    pub fn fill(mode: i32, digest_algo: i32, count: i32) -> Result<S2k, &'static str> {
        let mut s = S2k {
            mode,
            digest_algo,
            salt: [0u8; PGP_S2K_SALT],
            iter: 0,
            key: Vec::new(),
            key_len: 0,
        };
        match mode {
            PGP_S2K_SIMPLE => {}
            PGP_S2K_SALTED => {
                if !pg_strong_random(&mut s.salt) {
                    return Err("random failed");
                }
            }
            PGP_S2K_ISALTED => {
                if !pg_strong_random(&mut s.salt) {
                    return Err("random failed");
                }
                let mut tmp = [0u8; 1];
                if !pg_strong_random(&mut tmp) {
                    return Err("random failed");
                }
                s.iter = decide_s2k_iter(tmp[0], count);
            }
            _ => return Err("bad s2k mode"),
        }
        Ok(s)
    }

    /// `pgp_s2k_read` — parse an S2K specifier off a byte stream, returning the
    /// number of bytes consumed.
    pub fn read(src: &[u8]) -> Result<(S2k, usize), &'static str> {
        if src.len() < 2 {
            return Err(CORRUPT_DATA);
        }
        let mode = src[0] as i32;
        let digest_algo = src[1] as i32;
        let mut s = S2k {
            mode,
            digest_algo,
            salt: [0u8; PGP_S2K_SALT],
            iter: 0,
            key: Vec::new(),
            key_len: 0,
        };
        let consumed = match mode {
            PGP_S2K_SIMPLE => 2,
            PGP_S2K_SALTED => {
                if src.len() < 10 {
                    return Err(CORRUPT_DATA);
                }
                s.salt.copy_from_slice(&src[2..10]);
                10
            }
            PGP_S2K_ISALTED => {
                if src.len() < 11 {
                    return Err(CORRUPT_DATA);
                }
                s.salt.copy_from_slice(&src[2..10]);
                s.iter = src[10];
                11
            }
            _ => return Err("Bad S2K mode"),
        };
        Ok((s, consumed))
    }

    /// `pgp_s2k_process` — derive `self.key` of length `cipher_key_size(cipher)`
    /// from the passphrase `key`.
    pub fn process(&mut self, cipher: i32, key: &[u8]) -> Result<(), &'static str> {
        let target = cipher_key_size(cipher);
        if target == 0 {
            return Err(UNSUPPORTED_CIPHER);
        }
        self.key_len = target;
        let mut md = super::consts::Digest::new(self.digest_algo).ok_or(UNSUPPORTED_HASH)?;
        let md_rlen = md.result_size();
        let mut out = vec![0u8; target];
        let mut filled = 0usize;
        let mut preload = 0usize;
        let zeros = [0u8; 128];
        while filled < target {
            md.reset();
            if preload > 0 {
                md.update(&zeros[..preload]);
            }
            preload += 1;
            match self.mode {
                PGP_S2K_SIMPLE => {
                    md.update(key);
                }
                PGP_S2K_SALTED => {
                    md.update(&self.salt);
                    md.update(key);
                }
                PGP_S2K_ISALTED => {
                    let count = s2k_decode_count(self.iter as i32) as usize;
                    md.update(&self.salt);
                    md.update(key);
                    let mut curcnt = PGP_S2K_SALT + key.len();
                    while curcnt < count {
                        let c = if curcnt + PGP_S2K_SALT < count {
                            PGP_S2K_SALT
                        } else {
                            count - curcnt
                        };
                        md.update(&self.salt[..c]);
                        curcnt += c;
                        let c = if curcnt + key.len() < count {
                            key.len()
                        } else if curcnt < count {
                            count - curcnt
                        } else {
                            break;
                        };
                        md.update(&key[..c]);
                        curcnt += c;
                    }
                }
                _ => return Err("bad s2k mode"),
            }
            let h = md.finish();
            let remain = target - filled;
            let n = remain.min(md_rlen);
            out[filled..filled + n].copy_from_slice(&h[..n]);
            filled += n;
        }
        self.key = out;
        Ok(())
    }
}

/// `decide_s2k_iter(rand_byte, count)`.
fn decide_s2k_iter(rand_byte: u8, count: i32) -> u8 {
    if count == -1 {
        return 96 + (rand_byte & 0x1f);
    }
    for iter in 0..=255i32 {
        if s2k_decode_count(iter) >= count {
            return iter as u8;
        }
    }
    255
}
