//! First-class survivor mask (M4X-Q staged-prewhere vocabulary): a bitmask
//! over `n` slots that CARRIES its popcount and represents the two constant
//! extremes without allocation.
//!
//! The three-outcome prune step produces masks whose common cases are the
//! constants — an AllPass granule's row mask is const-true (the filter is
//! droppable), an AllFail granule's is const-false (the read is droppable) —
//! and every consumer decision ("skip this decode?", "drop this filter?",
//! "how many survivors?") is a popcount question. Recomputing popcounts at
//! each consumer is the C1 anti-pattern the cached count removes; the const
//! reps make "provably everything/nothing" a REPRESENTATION, not a scan.
//!
//! Normalization invariant: a freshly built mask whose popcount equals `n`
//! (or 0) NORMALIZES to the const rep, so [`Mask::is_const_true`] /
//! [`Mask::is_const_false`] are complete — "not const" means genuinely
//! mixed. Word-rep masks keep the [`Validity`] tail invariant (bits at
//! positions `>= n` are zero); the popcount cache makes every count O(1).
//!
//! Distinct from [`Validity`] (per-column NULL-ness, no popcount cache,
//! word-mutable) and [`Selection`] (position-list row currency — C1:
//! position lists beat byte-masks downstream). The mask is the VERDICT
//! currency between prune/filter steps; it lowers into a `Selection` at the
//! staging face ([`Mask::to_selection`]).

use crate::selection::Selection;
use crate::validity::{words_for, Validity};

/// Representation: constants carry no words; the word rep carries its
/// popcount from build time.
#[derive(Clone, Debug, PartialEq, Eq)]
enum MaskRep {
    /// Every slot set.
    ConstTrue,
    /// No slot set.
    ConstFalse,
    /// Genuinely mixed (normalization: `0 < popcount < n`).
    Words { words: Vec<u64>, popcount: u32 },
}

/// A popcount-carrying bitmask over `n` slots (rows of a window, granules
/// of a part — the slot grain is the caller's).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Mask {
    n: u32,
    rep: MaskRep,
}

impl Mask {
    /// The const-true mask: every slot set, no allocation.
    pub fn const_true(n: u32) -> Mask {
        Mask { n, rep: MaskRep::ConstTrue }
    }

    /// The const-false mask: no slot set, no allocation.
    pub fn const_false(n: u32) -> Mask {
        Mask { n, rep: MaskRep::ConstFalse }
    }

    /// Build from mask words covering `n` slots. Tail bits (positions
    /// `>= n`) are masked off, the popcount is computed ONCE, and the
    /// all/none extremes normalize to the const reps.
    pub fn from_words(n: u32, mut words: Vec<u64>) -> Mask {
        let nwords = words_for(n as usize);
        assert!(words.len() >= nwords, "mask words must cover n slots");
        words.truncate(nwords);
        let tail = (n as usize) % 64;
        if tail != 0 {
            if let Some(w) = words.last_mut() {
                *w &= (1u64 << tail) - 1;
            }
        }
        let popcount: u32 = words.iter().map(|w| w.count_ones()).sum();
        if popcount == n {
            Mask::const_true(n)
        } else if popcount == 0 {
            Mask::const_false(n)
        } else {
            Mask { n, rep: MaskRep::Words { words, popcount } }
        }
    }

    /// Build from a per-slot predicate (normalizing, one pass).
    pub fn from_fn(n: u32, mut f: impl FnMut(u32) -> bool) -> Mask {
        let mut words = vec![0u64; words_for(n as usize)];
        for i in 0..n {
            if f(i) {
                words[(i / 64) as usize] |= 1u64 << (i % 64);
            }
        }
        Mask::from_words(n, words)
    }

    /// Slot count.
    #[inline]
    pub fn len(&self) -> u32 {
        self.n
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.n == 0
    }

    /// Set-slot count — O(1), carried since build.
    #[inline]
    pub fn popcount(&self) -> u32 {
        match &self.rep {
            MaskRep::ConstTrue => self.n,
            MaskRep::ConstFalse => 0,
            MaskRep::Words { popcount, .. } => *popcount,
        }
    }

    /// TRUE iff every slot is set. Complete under normalization: any mask
    /// whose popcount equals `n` answers true regardless of how it was
    /// built.
    #[inline]
    pub fn all_true(&self) -> bool {
        self.popcount() == self.n
    }

    /// TRUE iff no slot is set (complete under normalization).
    #[inline]
    pub fn all_false(&self) -> bool {
        self.popcount() == 0
    }

    /// Rep probe: the mask is the const-true REPRESENTATION (equivalent to
    /// [`Mask::all_true`] by normalization; spelled separately so consumers
    /// branching on representation say what they mean).
    #[inline]
    pub fn is_const_true(&self) -> bool {
        matches!(self.rep, MaskRep::ConstTrue)
    }

    #[inline]
    pub fn is_const_false(&self) -> bool {
        matches!(self.rep, MaskRep::ConstFalse)
    }

    /// One slot's bit.
    #[inline]
    pub fn get(&self, i: u32) -> bool {
        debug_assert!(i < self.n, "mask slot out of range");
        match &self.rep {
            MaskRep::ConstTrue => true,
            MaskRep::ConstFalse => false,
            MaskRep::Words { words, .. } => (words[(i / 64) as usize] >> (i % 64)) & 1 != 0,
        }
    }

    /// `self &= other` (slot counts must agree). Const reps short-circuit:
    /// true is identity, false is absorbing — no words are touched. The
    /// word ∧ word case recomputes the popcount in the same pass and
    /// re-normalizes.
    pub fn and_with(&mut self, other: &Mask) {
        assert_eq!(self.n, other.n, "mask AND over disagreeing slot counts");
        match (&mut self.rep, &other.rep) {
            (_, MaskRep::ConstTrue) => {}
            (MaskRep::ConstFalse, _) => {}
            (_, MaskRep::ConstFalse) => self.rep = MaskRep::ConstFalse,
            (MaskRep::ConstTrue, MaskRep::Words { .. }) => {
                self.rep = other.rep.clone();
            }
            (MaskRep::Words { words, popcount }, MaskRep::Words { words: ow, .. }) => {
                let mut pc = 0u32;
                for (w, o) in words.iter_mut().zip(ow.iter()) {
                    *w &= *o;
                    pc += w.count_ones();
                }
                *popcount = pc;
                if pc == 0 {
                    self.rep = MaskRep::ConstFalse;
                } else if pc == self.n {
                    self.rep = MaskRep::ConstTrue;
                }
            }
        }
    }

    /// Lower into the position-list row currency (replacing `out`'s
    /// contents). Const-true fills the identity selection without a bit
    /// walk.
    pub fn to_selection(&self, out: &mut Selection) {
        match &self.rep {
            MaskRep::ConstTrue => out.fill_identity(self.n),
            MaskRep::ConstFalse => out.clear(),
            MaskRep::Words { words, .. } => {
                out.clear();
                for (i, &w) in words.iter().enumerate() {
                    out.push_mask_word(i, w);
                }
            }
        }
    }

    /// Build from a validity bitmask over `n` rows (NULL-stripping as a
    /// mask; normalizing — an all-valid column yields const-true).
    pub fn from_validity(n: u32, validity: &Validity) -> Mask {
        let nwords = words_for(n as usize);
        debug_assert!(validity.words.len() >= nwords);
        Mask::from_words(n, validity.words[..nwords].to_vec())
    }

    /// Lift a position-list selection back into the mask currency
    /// (normalizing — the identity selection yields const-true, the empty
    /// one const-false). Inverse of [`Mask::to_selection`]; the M4X-Q2
    /// staged-conjunct loop enters here (a staged batch's selection becomes
    /// the row-grain alive mask the per-conjunct verdicts AND into).
    pub fn from_selection(n: u32, sel: &Selection) -> Mask {
        if sel.positions.len() == n as usize {
            // Positions are strictly ascending row ordinals in 0..n, so a
            // full-length selection IS the identity — no bit walk.
            return Mask::const_true(n);
        }
        let mut words = vec![0u64; words_for(n as usize)];
        for &p in &sel.positions {
            debug_assert!(p < n, "selection position outside the mask");
            words[(p / 64) as usize] |= 1u64 << (p % 64);
        }
        Mask::from_words(n, words)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn const_reps_carry_popcount_without_words() {
        let t = Mask::const_true(130);
        let f = Mask::const_false(130);
        assert_eq!(t.popcount(), 130);
        assert_eq!(f.popcount(), 0);
        assert!(t.all_true() && t.is_const_true() && !t.all_false());
        assert!(f.all_false() && f.is_const_false() && !f.all_true());
        assert!(t.get(0) && t.get(129));
        assert!(!f.get(0) && !f.get(129));
    }

    #[test]
    fn from_words_normalizes_and_masks_tail() {
        // 70 slots; words carry garbage tail bits — they must not count.
        let all = vec![!0u64; 2];
        let m = Mask::from_words(70, all.clone());
        assert!(m.is_const_true(), "all-set normalizes to const-true");
        assert_eq!(m.popcount(), 70);

        let none = Mask::from_words(70, vec![0, !0u64 << 6]); // only tail bits
        assert!(none.is_const_false(), "tail-only bits normalize to const-false");

        let mixed = Mask::from_words(70, vec![0b1010, 0]);
        assert!(!mixed.is_const_true() && !mixed.is_const_false());
        assert_eq!(mixed.popcount(), 2);
        assert!(mixed.get(1) && mixed.get(3) && !mixed.get(0) && !mixed.get(69));
    }

    #[test]
    fn from_fn_matches_per_slot_reads() {
        let m = Mask::from_fn(200, |i| i % 3 == 0);
        assert_eq!(m.popcount(), 67);
        for i in 0..200 {
            assert_eq!(m.get(i), i % 3 == 0, "slot {i}");
        }
    }

    #[test]
    fn and_composition_short_circuits_and_renormalizes() {
        let mixed = Mask::from_fn(100, |i| i < 50);
        let other = Mask::from_fn(100, |i| i >= 25);

        // true is identity.
        let mut a = mixed.clone();
        a.and_with(&Mask::const_true(100));
        assert_eq!(a, mixed);

        // false is absorbing (both directions).
        let mut b = mixed.clone();
        b.and_with(&Mask::const_false(100));
        assert!(b.is_const_false());
        let mut c = Mask::const_false(100);
        c.and_with(&mixed);
        assert!(c.is_const_false());

        // const-true ∧ mixed adopts the mixed words.
        let mut d = Mask::const_true(100);
        d.and_with(&mixed);
        assert_eq!(d, mixed);

        // word ∧ word recomputes the count and can re-normalize to false.
        let mut e = mixed.clone();
        e.and_with(&other);
        assert_eq!(e.popcount(), 25);
        for i in 0..100 {
            assert_eq!(e.get(i), (25..50).contains(&i));
        }
        let mut g = Mask::from_fn(100, |i| i < 25);
        g.and_with(&Mask::from_fn(100, |i| i >= 75));
        assert!(g.is_const_false());
    }

    #[test]
    fn to_selection_matches_bits_and_identity_fast_path() {
        let mut sel = Selection::new();
        Mask::const_true(5).to_selection(&mut sel);
        assert_eq!(sel.as_slice(), &[0, 1, 2, 3, 4]);

        Mask::const_false(5).to_selection(&mut sel);
        assert!(sel.is_empty());

        let m = Mask::from_fn(130, |i| i % 7 == 0);
        m.to_selection(&mut sel);
        let want: Vec<u32> = (0..130).filter(|i| i % 7 == 0).collect();
        assert_eq!(sel.as_slice(), want.as_slice());
        assert_eq!(sel.len() as u32, m.popcount());
    }

    #[test]
    fn from_selection_roundtrips_and_normalizes() {
        // Identity → const-true without a bit walk.
        let mut sel = Selection::new();
        sel.positions.extend(0..70u32);
        assert!(Mask::from_selection(70, &sel).is_const_true());

        // Empty → const-false.
        sel.positions.clear();
        assert!(Mask::from_selection(70, &sel).is_const_false());

        // Mixed roundtrips through to_selection bit-exactly.
        let m = Mask::from_fn(130, |i| i % 5 == 2);
        let mut lowered = Selection::new();
        m.to_selection(&mut lowered);
        let back = Mask::from_selection(130, &lowered);
        assert_eq!(back, m);
        assert_eq!(back.popcount(), m.popcount());
    }

    #[test]
    fn from_validity_strips_nulls() {
        let mut v = Validity::new();
        v.reset_all_valid(66);
        v.set_null(3);
        v.set_null(65);
        let m = Mask::from_validity(66, &v);
        assert_eq!(m.popcount(), 64);
        assert!(!m.get(3) && !m.get(65) && m.get(0) && m.get(64));

        v.reset_all_valid(66);
        assert!(Mask::from_validity(66, &v).is_const_true());
    }
}
