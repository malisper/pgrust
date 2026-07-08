//! OpenPGP CFB (pgp-cfb.c) — both the normal CFB (tag 18 / MDC) and the
//! resync CFB (tag 9, old) variants, driven over a single-block ECB encryptor.

use super::consts::*;
use crate::cipher::BlockEncryptor;

pub struct PgpCfb {
    ciph: BlockEncryptor,
    block_size: usize,
    pos: usize,
    block_no: i32,
    resync: bool,
    fr: Vec<u8>,
    fre: Vec<u8>,
    encbuf: Vec<u8>,
}

impl PgpCfb {
    /// `pgp_cfb_create` — `iv = None` ⇒ FR starts all-zero.
    pub fn create(
        algo: i32,
        key: &[u8],
        resync: bool,
        iv: Option<&[u8]>,
    ) -> Result<PgpCfb, &'static str> {
        let int_name = cipher_int_name(algo).ok_or(UNSUPPORTED_CIPHER)?;
        let ciph = BlockEncryptor::new(int_name, key).ok_or(UNSUPPORTED_CIPHER)?;
        let bs = ciph.block_size();
        let mut fr = vec![0u8; bs];
        if let Some(iv) = iv {
            let n = iv.len().min(bs);
            fr[..n].copy_from_slice(&iv[..n]);
        }
        Ok(PgpCfb {
            ciph,
            block_size: bs,
            pos: 0,
            block_no: 0,
            resync,
            fr,
            fre: vec![0u8; bs],
            encbuf: vec![0u8; bs],
        })
    }

    pub fn block_size(&self) -> usize {
        self.block_size
    }

    fn ecb(&mut self) {
        self.fre.copy_from_slice(&self.fr);
        self.ciph.encrypt_block(&mut self.fre);
    }

    /// Encrypt `data` in place into a freshly-returned buffer.
    pub fn encrypt(&mut self, data: &[u8]) -> Vec<u8> {
        let mut dst = vec![0u8; data.len()];
        self.process(data, &mut dst, true);
        dst
    }

    /// Decrypt `data` into a freshly-returned buffer.
    pub fn decrypt(&mut self, data: &[u8]) -> Vec<u8> {
        let mut dst = vec![0u8; data.len()];
        self.process(data, &mut dst, false);
        dst
    }

    /// `cfb_process` — the driver. `enc` selects encrypt vs decrypt direction.
    fn process(&mut self, data: &[u8], dst: &mut [u8], enc: bool) {
        let bs = self.block_size;
        let mut di = 0usize; // index into data/dst
        let mut len = data.len();

        // Phase 1: finish a partially-consumed keystream block.
        while len > 0 && self.pos > 0 {
            let mut n = bs - self.pos;
            if len < n {
                n = len;
            }
            let consumed = self.mix(&data[di..di + n], &mut dst[di..di + n], enc);
            di += consumed;
            len -= consumed;
            if self.pos == bs {
                self.fr.copy_from_slice(&self.encbuf);
                self.pos = 0;
            }
        }

        // Phase 2: full fresh blocks.
        while len > 0 {
            self.ecb();
            if self.block_no < 5 {
                self.block_no += 1;
            }
            let mut n = bs;
            if len < n {
                n = len;
            }
            let consumed = self.mix(&data[di..di + n], &mut dst[di..di + n], enc);
            di += consumed;
            len -= consumed;
            if self.pos == bs {
                self.fr.copy_from_slice(&self.encbuf);
                self.pos = 0;
            }
        }
    }

    /// `mix_*` — returns the number of bytes consumed.
    fn mix(&mut self, data: &[u8], dst: &mut [u8], enc: bool) -> usize {
        if self.resync {
            self.mix_resync(data, dst, enc)
        } else {
            self.mix_normal(data, dst, enc)
        }
    }

    fn mix_normal(&mut self, data: &[u8], dst: &mut [u8], enc: bool) -> usize {
        let len = data.len();
        for k in 0..len {
            let i = self.pos + k;
            if enc {
                self.encbuf[i] = self.fre[i] ^ data[k];
                dst[k] = self.encbuf[i];
            } else {
                self.encbuf[i] = data[k];
                dst[k] = self.fre[i] ^ self.encbuf[i];
            }
        }
        self.pos += len;
        len
    }

    fn mix_resync(&mut self, data: &[u8], dst: &mut [u8], enc: bool) -> usize {
        let bs = self.block_size;
        if self.block_no == 2 {
            let mut n = 2 - self.pos;
            if data.len() < n {
                n = data.len();
            }
            for k in 0..n {
                let i = self.pos + k;
                if enc {
                    self.encbuf[i] = self.fre[i] ^ data[k];
                    dst[k] = self.encbuf[i];
                } else {
                    self.encbuf[i] = data[k];
                    dst[k] = self.fre[i] ^ self.encbuf[i];
                }
            }
            self.pos += n;
            if self.pos == 2 {
                // resync rotate: fr = encbuf[2..bs] ++ encbuf[0..2]
                let mut newfr = vec![0u8; bs];
                newfr[..bs - 2].copy_from_slice(&self.encbuf[2..bs]);
                newfr[bs - 2..].copy_from_slice(&self.encbuf[..2]);
                self.fr.copy_from_slice(&newfr);
                self.pos = 0;
            }
            return n;
        }
        let len = data.len();
        for k in 0..len {
            let i = self.pos + k;
            if enc {
                self.encbuf[i] = self.fre[i] ^ data[k];
                dst[k] = self.encbuf[i];
            } else {
                self.encbuf[i] = data[k];
                dst[k] = self.fre[i] ^ self.encbuf[i];
            }
        }
        self.pos += len;
        len
    }
}
