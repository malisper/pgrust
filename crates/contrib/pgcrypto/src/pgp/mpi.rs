
use super::consts::*;
use ::num_bigint::BigUint;
use ::num_traits::{One, Zero};
use ::pg_strong_random::pg_strong_random;

#[derive(Clone)]
pub struct Mpi {
    pub bits: usize,
    pub data: Vec<u8>,
}

impl Mpi {
    pub fn from_bytes(data: Vec<u8>, bits: usize) -> Mpi {
        Mpi { bits, data }
    }

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

    pub fn to_biguint(&self) -> BigUint {
        BigUint::from_bytes_be(&self.data)
    }

    pub fn nbytes(&self) -> usize {
        self.data.len()
    }
}

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

pub fn write_mpi(dst: &mut Vec<u8>, m: &Mpi) {
    dst.push((m.bits >> 8) as u8);
    dst.push((m.bits & 0xFF) as u8);
    dst.extend_from_slice(&m.data);
}

pub fn mpi_cksum(mut cksum: u32, m: &Mpi) -> u32 {
    cksum += (m.bits >> 8) as u32;
    cksum += (m.bits & 0xFF) as u32;
    for &b in &m.data {
        cksum += b as u32;
    }
    cksum & 0xFFFF
}

fn decide_k_bits(p_bits: usize) -> usize {
    if p_bits <= 5120 {
        p_bits / 10 + 160
    } else {
        (p_bits / 8 + 200) * 3 / 2
    }
}

fn rand_bits(bits: usize) -> Result<BigUint, String> {
    if bits == 0 {
        return Ok(BigUint::zero());
    }
    let nbytes = (bits + 7) / 8;
    let mut buf = vec![0u8; nbytes];
    if !pg_strong_random(&mut buf) {
        return Err("Failed to generate strong random bits".to_string());
    }
    let excess = nbytes * 8 - bits;
    buf[0] &= 0xFF >> excess;
    buf[0] |= 0x80 >> excess;
    Ok(BigUint::from_bytes_be(&buf))
}

pub fn rsa_encrypt(n: &Mpi, e: &Mpi, m: &Mpi) -> Mpi {
    let c = m.to_biguint().modpow(&e.to_biguint(), &n.to_biguint());
    Mpi::from_biguint(&c)
}

pub fn rsa_decrypt(n: &Mpi, d: &Mpi, c: &Mpi) -> Mpi {
    let m = c.to_biguint().modpow(&d.to_biguint(), &n.to_biguint());
    Mpi::from_biguint(&m)
}

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
    let m_i2 = m_i.clone();
    let mut res = old_s % &m_i2;
    if res.sign() == Sign::Minus {
        res += &m_i2;
    }
    res.to_biguint()
}
