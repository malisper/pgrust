//! OpenPGP multi-precision integers (pgp-mpi.c, pgp-mpi-internal.c / -openssl.c)
//! — read/write the `bits16 || ceil(bits/8) bytes` wire form, plus the RSA and
//! ElGamal modular-arithmetic primitives over `num-bigint` (mirroring
//! pgp-mpi-openssl.c's BN_mod_exp / BN_mod_inverse / BN_mod_mul math). The k
//! random-bit choice in ElGamal encrypt follows `decide_k_bits` exactly.

use super::consts::*;
use ::num_bigint::BigUint;
use ::num_traits::{One, Zero};
use ::pg_strong_random::pg_strong_random;

/// A PGP MPI: the canonical big-endian value bytes and the declared bit length.
/// `bytes` is `ceil(bits/8)` and equals the canonical minimal encoding (no
/// leading zero byte), exactly as gpg / pgcrypto emit.
#[derive(Clone)]
pub struct Mpi {
    pub bits: usize,
    pub data: Vec<u8>,
}

impl Mpi {
    /// `pgp_mpi_create` over a raw big-endian buffer of the given bit length.
    /// The buffer is `ceil(bits/8)` bytes (the caller passes a padded value).
    pub fn from_bytes(data: Vec<u8>, bits: usize) -> Mpi {
        Mpi { bits, data }
    }

    /// Build an MPI from a `BigUint` (`bn_to_mpi`): canonical big-endian with the
    /// significant-bit count as the declared length.
    pub fn from_biguint(n: &BigUint) -> Mpi {
        let data = if n.is_zero() {
            Vec::new()
        } else {
            n.to_bytes_be()
        };
        Mpi {
            bits: n.bits() as usize,
            data,
        }
    }

    /// `mpi_to_bn` — interpret the value bytes as a big-endian unsigned integer.
    pub fn to_biguint(&self) -> BigUint {
        BigUint::from_bytes_be(&self.data)
    }

    /// The number of value bytes (`PGP_MPI.bytes`).
    pub fn nbytes(&self) -> usize {
        self.data.len()
    }
}

/// `pgp_mpi_read` — parse one MPI (2-byte big-endian bit length, then
/// `ceil(bits/8)` value bytes) from `data` at `*pos`, advancing the cursor.
/// A short read / over-long length is `PXE_PGP_CORRUPT_DATA` = the
/// "Wrong key or corrupt data" string (matters for the wrong-password path).
pub fn read_mpi(data: &[u8], pos: &mut usize) -> Result<Mpi, String> {
    if *pos + 2 > data.len() {
        return Err(WRONG_KEY.to_string());
    }
    let bits = ((data[*pos] as usize) << 8) | data[*pos + 1] as usize;
    *pos += 2;
    if bits > 0xFFFF {
        return Err(WRONG_KEY.to_string());
    }
    let nbytes = (bits + 7) / 8;
    if *pos + nbytes > data.len() {
        return Err(WRONG_KEY.to_string());
    }
    let value = data[*pos..*pos + nbytes].to_vec();
    *pos += nbytes;
    Ok(Mpi::from_bytes(value, bits))
}

/// `pgp_mpi_write` — append the 2-byte bit length + value bytes.
pub fn write_mpi(dst: &mut Vec<u8>, m: &Mpi) {
    dst.push((m.bits >> 8) as u8);
    dst.push((m.bits & 0xFF) as u8);
    dst.extend_from_slice(&m.data);
}

/// `pgp_mpi_cksum` — running 16-bit sum over the wire form of an MPI.
pub fn mpi_cksum(mut cksum: u32, m: &Mpi) -> u32 {
    cksum += (m.bits >> 8) as u32;
    cksum += (m.bits & 0xFF) as u32;
    for &b in &m.data {
        cksum += b as u32;
    }
    cksum & 0xFFFF
}

/// `decide_k_bits` (pgp-mpi-openssl.c) — number of random bits for the ElGamal
/// ephemeral `k`. Encryption is randomized; only the round-trip matters.
fn decide_k_bits(p_bits: usize) -> usize {
    if p_bits <= 5120 {
        p_bits / 10 + 160
    } else {
        (p_bits / 8 + 200) * 3 / 2
    }
}

/// A uniformly random `BigUint` with exactly `bits` bits (top bit forced set,
/// mirroring OpenSSL `BN_rand(k, bits, 0, 0)`'s top=0 → MSB set).
fn rand_bits(bits: usize) -> Result<BigUint, String> {
    if bits == 0 {
        return Ok(BigUint::zero());
    }
    let nbytes = (bits + 7) / 8;
    let mut buf = vec![0u8; nbytes];
    if !pg_strong_random(&mut buf) {
        return Err("Failed to generate strong random bits".to_string());
    }
    // Mask off the excess high bits, then force the MSB on (BN_rand top=0).
    let excess = nbytes * 8 - bits;
    buf[0] &= 0xFF >> excess;
    buf[0] |= 0x80 >> excess;
    Ok(BigUint::from_bytes_be(&buf))
}

/// `pgp_rsa_encrypt` — `c = m^e mod n`.
pub fn rsa_encrypt(n: &Mpi, e: &Mpi, m: &Mpi) -> Mpi {
    let c = m.to_biguint().modpow(&e.to_biguint(), &n.to_biguint());
    Mpi::from_biguint(&c)
}

/// `pgp_rsa_decrypt` — `m = c^d mod n`.
pub fn rsa_decrypt(n: &Mpi, d: &Mpi, c: &Mpi) -> Mpi {
    let m = c.to_biguint().modpow(&d.to_biguint(), &n.to_biguint());
    Mpi::from_biguint(&m)
}

/// `pgp_elgamal_encrypt` — `c1 = g^k mod p`, `c2 = m * y^k mod p`.
pub fn elgamal_encrypt(p: &Mpi, g: &Mpi, y: &Mpi, m: &Mpi) -> Result<(Mpi, Mpi), String> {
    let p = p.to_biguint();
    let g = g.to_biguint();
    let y = y.to_biguint();
    let m = m.to_biguint();

    let k_bits = decide_k_bits(p.bits() as usize);
    let k = rand_bits(k_bits)?;

    let c1 = g.modpow(&k, &p);
    let yk = y.modpow(&k, &p);
    let c2 = (&m * &yk) % &p;
    Ok((Mpi::from_biguint(&c1), Mpi::from_biguint(&c2)))
}

/// `pgp_elgamal_decrypt` — `m = c2 / (c1^x) mod p`.
pub fn elgamal_decrypt(p: &Mpi, x: &Mpi, c1: &Mpi, c2: &Mpi) -> Result<Mpi, String> {
    let p = p.to_biguint();
    let x = x.to_biguint();
    let c1 = c1.to_biguint();
    let c2 = c2.to_biguint();

    let c1x = c1.modpow(&x, &p);
    let inv = mod_inverse(&c1x, &p).ok_or_else(|| "Math operation failed".to_string())?;
    let m = (&c2 * &inv) % &p;
    Ok(Mpi::from_biguint(&m))
}

/// Modular inverse `a^-1 mod m` via the extended Euclidean algorithm (BN_mod_inverse).
fn mod_inverse(a: &BigUint, m: &BigUint) -> Option<BigUint> {
    use ::num_bigint::BigInt;
    use ::num_bigint::Sign;
    let a = BigInt::from_biguint(Sign::Plus, a.clone());
    let m_i = BigInt::from_biguint(Sign::Plus, m.clone());

    let (mut old_r, mut r) = (a.clone(), m_i.clone());
    let (mut old_s, mut s) = (BigInt::one(), BigInt::zero());
    while !r.is_zero() {
        let q = &old_r / &r;
        let new_r = &old_r - &q * &r;
        old_r = std::mem::replace(&mut r, new_r);
        let new_s = &old_s - &q * &s;
        old_s = std::mem::replace(&mut s, new_s);
    }
    // gcd must be 1 for an inverse to exist.
    if old_r != BigInt::one() {
        return None;
    }
    // Reduce old_s into [0, m).
    let m_i2 = m_i.clone();
    let mut res = old_s % &m_i2;
    if res.sign() == Sign::Minus {
        res += &m_i2;
    }
    res.to_biguint()
}
