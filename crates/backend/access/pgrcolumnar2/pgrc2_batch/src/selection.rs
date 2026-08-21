//! Position-list selection (C1: Photon evidence — position lists beat
//! byte-masks in all but trivial queries; kernels may use masks internally,
//! so word-granular converters are part of the ABI).

use crate::validity::{words_for, Validity};

/// Position-list selection: **strictly ascending** row indexes into the
/// batch (`0..nrows`). Kernels iterate the selection, never `0..nrows`.
/// Delete masks (ducklake) and visibility compose into this one currency.
#[derive(Clone, Debug, Default)]
pub struct Selection {
    pub positions: Vec<u32>,
}

impl Selection {
    #[inline]
    pub fn new() -> Selection {
        Selection { positions: Vec::new() }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.positions.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.positions.is_empty()
    }

    #[inline]
    pub fn clear(&mut self) {
        self.positions.clear();
    }

    #[inline]
    pub fn as_slice(&self) -> &[u32] {
        &self.positions
    }

    /// Append one surviving position (must keep the list strictly
    /// ascending).
    #[inline]
    pub fn push(&mut self, pos: u32) {
        debug_assert!(self.positions.last().is_none_or(|&p| p < pos), "selection must ascend");
        self.positions.push(pos);
    }

    /// Reset to the identity selection `0..nrows` (every staged row
    /// survives).
    pub fn fill_identity(&mut self, nrows: u32) {
        self.positions.clear();
        self.positions.extend(0..nrows);
    }

    /// Word-granular append: expand the set bits of `mask` — covering rows
    /// `word_index*64 .. word_index*64+64` — into positions. The batch-side
    /// face for kernels that compute 64-row verdict masks internally.
    /// (Bit iteration via `trailing_zeros` is the sanctioned use of
    /// rbit+clz on aarch64 — the banned pattern is pow2-switch *width
    /// dispatch* on deform paths; see the perf playbook.)
    pub fn push_mask_word(&mut self, word_index: usize, mut mask: u64) {
        let base = (word_index * 64) as u32;
        debug_assert!(
            self.positions.last().is_none_or(|&p| mask == 0 || p < base + mask.trailing_zeros()),
            "selection must ascend"
        );
        while mask != 0 {
            let bit = mask.trailing_zeros();
            self.positions.push(base + bit);
            mask &= mask - 1;
        }
    }

    /// Rebuild from a validity bitmask over `nrows` rows: selection = the
    /// valid rows (visibility/delete masks become ordinary selections).
    pub fn fill_from_validity(&mut self, validity: &Validity, nrows: usize) {
        self.positions.clear();
        for i in 0..words_for(nrows) {
            self.push_mask_word(i, validity.word(i));
        }
    }

    /// Keep only positions whose validity bit is set (selection ∧ validity,
    /// the NULL-stripping step of the selection algebra).
    pub fn retain_valid(&mut self, validity: &Validity) {
        self.positions.retain(|&p| validity.is_valid(p as usize));
    }

    /// In-place intersection with another ascending selection
    /// (visibility ∩ delete-vector ∩ qual survivors compose into ONE
    /// position list — C2).
    pub fn intersect_sorted(&mut self, other: &Selection) {
        let mut out = 0usize;
        let mut j = 0usize;
        let b = &other.positions;
        for i in 0..self.positions.len() {
            let p = self.positions[i];
            while j < b.len() && b[j] < p {
                j += 1;
            }
            if j < b.len() && b[j] == p {
                self.positions[out] = p;
                out += 1;
                j += 1;
            }
        }
        self.positions.truncate(out);
    }

    /// Scatter the selection into 64-row mask words (kernels may use masks
    /// internally per C1). `out` must cover the batch
    /// (`>= words_for(nrows)` words); it is zeroed first.
    pub fn to_mask_words(&self, out: &mut [u64]) {
        out.fill(0);
        for &p in &self.positions {
            out[(p / 64) as usize] |= 1u64 << (p % 64);
        }
    }
}
