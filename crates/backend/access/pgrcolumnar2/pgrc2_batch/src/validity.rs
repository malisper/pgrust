//! Per-column validity bitmask (C1 base column: Datum words + validity).

/// Per-column validity bitmask; a set bit = value present (non-NULL).
/// Bit `r` of word `r / 64` (bit index `r % 64`) covers row `r`.
///
/// **Tail invariant**: bits at positions `>= nrows` are ZERO. Every method
/// here maintains it (it is what makes [`Validity::count_valid`] /
/// [`Validity::all_valid`] straight popcounts); word-granular writers
/// ([`Validity::word_mut`]) inherit the obligation and can re-establish it
/// with [`Validity::mask_tail`].
#[derive(Clone, Debug, Default)]
pub struct Validity {
    pub words: Vec<u64>,
}

/// Mask words needed to cover `nrows` rows.
#[inline]
pub(crate) const fn words_for(nrows: usize) -> usize {
    nrows.div_ceil(64)
}

impl Validity {
    #[inline]
    pub fn new() -> Validity {
        Validity { words: Vec::new() }
    }

    /// Reset to all-valid over `nrows` rows (tail bits zero).
    pub fn reset_all_valid(&mut self, nrows: usize) {
        let nwords = words_for(nrows);
        self.words.clear();
        self.words.resize(nwords, !0u64);
        self.mask_tail(nrows);
    }

    /// Reset to all-NULL over `nrows` rows.
    pub fn reset_all_null(&mut self, nrows: usize) {
        let nwords = words_for(nrows);
        self.words.clear();
        self.words.resize(nwords, 0u64);
    }

    /// Zero every bit at positions `>= nrows` (re-establish the tail
    /// invariant after word-granular writes).
    pub fn mask_tail(&mut self, nrows: usize) {
        let nwords = words_for(nrows);
        for w in self.words.iter_mut().skip(nwords) {
            *w = 0;
        }
        let tail = nrows % 64;
        if tail != 0 {
            if let Some(w) = self.words.get_mut(nwords - 1) {
                *w &= (1u64 << tail) - 1;
            }
        }
    }

    #[inline]
    pub fn is_valid(&self, row: usize) -> bool {
        (self.words[row / 64] >> (row % 64)) & 1 != 0
    }

    #[inline]
    pub fn set_valid(&mut self, row: usize) {
        self.words[row / 64] |= 1u64 << (row % 64);
    }

    #[inline]
    pub fn set_null(&mut self, row: usize) {
        self.words[row / 64] &= !(1u64 << (row % 64));
    }

    /// Word-granular read (kernels consuming masks directly).
    #[inline]
    pub fn word(&self, i: usize) -> u64 {
        self.words[i]
    }

    /// Word-granular write; the writer owns the tail invariant
    /// ([`Validity::mask_tail`]).
    #[inline]
    pub fn word_mut(&mut self, i: usize) -> &mut u64 {
        &mut self.words[i]
    }

    /// Valid-row count over `nrows` staged rows (straight popcount under the
    /// tail invariant).
    pub fn count_valid(&self, nrows: usize) -> usize {
        debug_assert!(self.words.len() >= words_for(nrows));
        self.words[..words_for(nrows)].iter().map(|w| w.count_ones() as usize).sum()
    }

    /// All-valid short-circuit (the zero-null proof consumers key on).
    pub fn all_valid(&self, nrows: usize) -> bool {
        self.count_valid(nrows) == nrows
    }

    /// `self &= other` at word granularity over `nrows` rows (validity
    /// algebra: composing masks stays word-parallel).
    pub fn and_words(&mut self, other: &Validity, nrows: usize) {
        let nwords = words_for(nrows);
        debug_assert!(self.words.len() >= nwords && other.words.len() >= nwords);
        for i in 0..nwords {
            self.words[i] &= other.words[i];
        }
    }

    /// `self |= other` at word granularity over `nrows` rows, then re-mask
    /// the tail.
    pub fn or_words(&mut self, other: &Validity, nrows: usize) {
        let nwords = words_for(nrows);
        debug_assert!(self.words.len() >= nwords && other.words.len() >= nwords);
        for i in 0..nwords {
            self.words[i] |= other.words[i];
        }
        self.mask_tail(nrows);
    }
}
