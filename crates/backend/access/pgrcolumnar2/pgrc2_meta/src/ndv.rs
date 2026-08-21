//! Mergeable NDV registers (spec §8.4): dense HyperLogLog — "store the
//! mergeable form" (v8's lesson, charter §5). Registers merge by
//! max-per-register: commutative, associative, idempotent — the property
//! suite pins all three — so granule → band → part → cross-part rollups
//! are all the same operation.
//!
//! `ndv_est` in the stats record (spec §8.1) is the u32 POINT ESTIMATE from
//! the grain's registers; the part's register blob is what the section
//! stores (one `NdvRegisters` section per column: header + registers).
//! Estimates are estimates — the metadata-answered-aggregate obligations
//! (bit-for-bit vs decode) apply to COUNT/MIN/MAX/SUM, never to `ndv_est`;
//! verdicts never consume `ndv_est` (arming policy does).

use crate::format::meta::NdvRegistersHdr;
use crate::format::wire::{put_u16, put_u32, put_u8, Cur};
use crate::format::{FormatError, FormatResult};
use crate::hash::ndv_hash;

/// Register-index bits: 2^10 = 1024 registers (1 KiB per column per part).
pub const NDV_PRECISION: u8 = 10;
/// The `algo` byte of the section header (spec §8.4: 1 = dense HLL).
pub const NDV_ALGO_DENSE_HLL: u8 = 1;

/// One dense-HLL register set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hll {
    regs: Vec<u8>,
}

impl Default for Hll {
    fn default() -> Self {
        Hll {
            regs: vec![0u8; 1usize << NDV_PRECISION],
        }
    }
}

impl Hll {
    /// Observe one canonical value image (spec §18.1 bytes).
    pub fn observe(&mut self, canonical: &[u8]) {
        self.observe_hash(ndv_hash(canonical));
    }

    /// Observe a pre-computed hash (the builder hashes once per value and
    /// feeds bloom + NDV from the same image).
    pub fn observe_hash(&mut self, h: u64) {
        let p = NDV_PRECISION as u32;
        let idx = (h >> (64 - p)) as usize;
        let w = h << p;
        let rank = if w == 0 {
            (64 - p + 1) as u8
        } else {
            (w.leading_zeros() + 1) as u8
        };
        if self.regs[idx] < rank {
            self.regs[idx] = rank;
        }
    }

    /// Register count (the merge-compatibility fact: [`Hll::merge`] only
    /// debug-asserts equal counts, so cross-part consumers guard on this
    /// before merging).
    pub fn regs_len(&self) -> usize {
        self.regs.len()
    }

    /// Max-per-register merge (commutative, associative, idempotent).
    pub fn merge(&mut self, other: &Hll) {
        debug_assert_eq!(self.regs.len(), other.regs.len());
        for (a, b) in self.regs.iter_mut().zip(other.regs.iter()) {
            if *a < *b {
                *a = *b;
            }
        }
    }

    /// The point estimate, clamped to u32 (the `ndv_est` field's domain).
    pub fn estimate(&self) -> u32 {
        let m = self.regs.len() as f64;
        let alpha = 0.7213 / (1.0 + 1.079 / m);
        let mut sum = 0.0f64;
        let mut zeros = 0u32;
        for &r in &self.regs {
            sum += f64::exp2(-f64::from(r));
            if r == 0 {
                zeros += 1;
            }
        }
        let raw = alpha * m * m / sum;
        let est = if raw <= 2.5 * m && zeros > 0 {
            // Small-range correction (linear counting).
            m * (m / f64::from(zeros)).ln()
        } else {
            raw
        };
        if est >= u32::MAX as f64 {
            u32::MAX
        } else {
            est as u32
        }
    }

    /// Encode the `NdvRegisters` section body (spec §8.4: header +
    /// registers).
    pub fn encode_section(&self, out: &mut Vec<u8>) {
        put_u8(out, NDV_ALGO_DENSE_HLL);
        put_u8(out, NDV_PRECISION);
        put_u16(out, 0);
        put_u32(out, self.regs.len() as u32);
        out.extend_from_slice(&self.regs);
    }

    /// Decode a section body (probe side / cross-part merge). Typed
    /// refusal on unknown algo, precision/length disagreement, truncation.
    pub fn decode_section(body: &[u8]) -> FormatResult<(NdvRegistersHdr, Hll)> {
        let mut c = Cur::new(body);
        let hdr = NdvRegistersHdr {
            algo: c.u8("NdvRegistersHdr")?,
            precision: c.u8("NdvRegistersHdr")?,
            pad: c.u16("NdvRegistersHdr")?,
            reg_len: c.u32("NdvRegistersHdr")?,
        };
        if hdr.algo != NDV_ALGO_DENSE_HLL {
            return Err(FormatError::Corrupt {
                at: "NdvRegisters algo",
            });
        }
        if hdr.precision > 16 || hdr.reg_len != 1u32 << hdr.precision {
            return Err(FormatError::Corrupt {
                at: "NdvRegisters precision/len",
            });
        }
        let regs = c.take(hdr.reg_len as usize, "NdvRegisters registers")?;
        Ok((
            hdr,
            Hll {
                regs: regs.to_vec(),
            },
        ))
    }
}
