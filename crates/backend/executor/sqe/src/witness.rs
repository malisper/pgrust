//! [ruling 2] The `Witness<T>` membrane — the two-tier evidence law's
//! type-level enforcement (election-inputs-law.md §2.3, RATIFIED
//! 2026-08-18):
//!
//!   - **Witness-grade** facts (sound bounds, never wrong direction) may
//!     feed correctness gates: admission caps, truncation bounds, domain
//!     witnesses. Sources: exact part-record min/max domains, narrow-word
//!     type domains, per-part dict entry sums (sealed exact), row counts,
//!     algebra-derived constants (minute-of-hour 61).
//!   - **Estimate-grade** facts (`ndv_est` HLL, survivor fractions) may
//!     only choose between correct alternatives — sizing, thread counts,
//!     family flips. They can NEVER produce a `Witness`.
//!
//! The membrane is the constructor set: every constructor takes the
//! witness-grade SOURCE (a bank, a stats face, a dict face sweep), never
//! a bare number an estimate could be laundered through. An
//! estimate-grade value flows into a witness-grade consumer only by
//! being REPLACED with a witness — never "estimate + slack". Compile-time
//! enforcement, zero runtime cost (`#[repr(transparent)]`, all inlined).
//!
//! This is the aecd62886d2 comment ("estimates never decide verdicts",
//! planner.rs `check_server_grouped`) promoted to a type.

use crate::bank::Bank;
use crate::engine::Faces;
use crate::statsview::StatsView;

/// A witness-grade value: a sound bound derived from an exact source.
/// No `From`/`Default`/public field — the named constructors below are
/// the only doors in.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Witness<T>(T);

impl<T: Copy> Witness<T> {
    /// Read the witnessed value. Consumers gate on this; producing one
    /// required a witness-grade source.
    #[inline(always)]
    pub fn value(&self) -> T {
        self.0
    }
}

impl Witness<u64> {
    /// The multiplicative identity (an empty key product bounds 1 group).
    #[inline]
    pub fn one() -> Witness<u64> {
        Witness(1)
    }

    /// Algebra-derived constant: minute-of-hour has at most 61 values
    /// (leap second included). Format/algebra facts, never scale guesses.
    #[inline]
    pub fn minute_of_hour() -> Witness<u64> {
        Witness(61)
    }

    #[inline]
    pub fn hour_of_day() -> Witness<u64> {
        Witness(24)
    }

    /// The bank manifest's total row count — exact by construction
    /// (sealed part records), and `groups <= rows` is always sound.
    #[inline]
    pub fn rows_total(bank: &Bank) -> Witness<u64> {
        Witness(bank.rows_total())
    }

    /// Per-part dict entry sum `Σ_p ncodes(p)` — exact per part because
    /// dicts are sealed exact; a sound UPPER bound on column NDV
    /// (double-counts cross-part repeats, never under-counts).
    #[inline]
    pub fn dict_entry_sum(bank: &Bank, faces: &Faces, col: u32) -> Witness<u64> {
        Witness(faces.dicts_all(bank, col).iter().map(|d| d.ncodes as u64).sum())
    }

    /// Upper bound (inclusive of the NULL group) on one group key's
    /// distinct count, from SOUND witnesses only: per-part dict entry
    /// sums, exact part-record min/max domains, narrow-word type domains.
    /// `None` = no witness (a verbatim varlena part, coarse stats keys).
    /// The HLL `ndv_est` is deliberately absent — it is a point estimate
    /// and may under-count (ndv.rs: verdicts never consume `ndv_est`).
    pub fn key_count(bank: &Bank, faces: &Faces, col: u32) -> Option<Witness<u64>> {
        let w = crate::stencils::col_width(bank, col);
        if w == 0 {
            let dfs = faces.dicts_all(bank, col);
            let mut n = 0u64;
            for df in dfs.iter() {
                df.dh.as_ref()?;
                n += df.ncodes as u64;
            }
            return Some(Witness(n.saturating_add(1)));
        }
        let dom = faces.stats(bank, col).minmax_exact().map(|(lo, hi)| {
            let d = (hi as i128 - lo as i128 + 1).max(0) as u128;
            u64::try_from(d.saturating_add(1)).unwrap_or(u64::MAX)
        });
        match dom {
            Some(d) => Some(Witness(d)),
            // Narrow-word fallback: the TYPE's own domain bounds the count.
            None if w <= 2 => Some(Witness((1u64 << (8 * w as u32)) + 1)),
            None => None,
        }
    }

    /// Zone-plane survivor upper bound for a row-returning scan: the row
    /// sum of every granule no int conjunct's SMA face excludes (varlena
    /// conjuncts prune nothing here — sound, they only shrink). With no
    /// int conjuncts the bound is the whole bank's row count.
    pub fn scan_survivor_bound(
        bank: &Bank,
        faces: &Faces,
        node: &crate::ir::PlanNode,
    ) -> Witness<u64> {
        let iterms: Vec<&crate::ir::PredTerm> =
            node.pred.iter().flat_map(|p| p.terms.iter()).collect();
        if iterms.is_empty() {
            return Witness::rows_total(bank);
        }
        let units = faces.walk(bank, node.cols[0]);
        let smas: Vec<_> = iterms.iter().map(|t| faces.sma(bank, t.col)).collect();
        let mut bound = 0u64;
        for ui in 0..units.len() {
            let dead = iterms
                .iter()
                .enumerate()
                .any(|(i, t)| !t.zone_may_pass(smas[i].mins[ui], smas[i].maxs[ui]));
            if !dead {
                bound = bound.saturating_add(units[ui].2 as u64);
            }
        }
        Witness(bound)
    }

    /// Minute-bucket bound for a `date_trunc('minute', col)`-class key
    /// from the column's EXACT part-record domain: every bucket lies in
    /// `[trunc(lo), trunc(hi)]`, and flooring widens the span by at most
    /// one minute — `(hi-lo)/60s + 2` buckets, algebra over an exact
    /// domain (never an estimate). `None` = no exact domain, no witness.
    pub fn trunc_minute_count(sv: &StatsView) -> Option<Witness<u64>> {
        sv.minmax_exact().map(|(lo, hi)| {
            let span = (hi as i128 - lo as i128).max(0);
            let n = (span / 60_000_000).saturating_add(2);
            Witness(u64::try_from(n).unwrap_or(u64::MAX))
        })
    }

    /// One extra group for a constant expression arm (the CASE `ELSE ''`
    /// class): a pure per-row derivation whose outputs are `f(col) ∪
    /// {const}` has at most `key_count + 1` distinct values — algebra
    /// over an existing witness, never a new estimate door.
    #[inline]
    pub fn plus_const_arm(self) -> Witness<u64> {
        Witness(self.0.saturating_add(1))
    }

    /// Product of two witnessed bounds is a witnessed bound (saturating:
    /// u64::MAX still sits on the sound side of every cap it can meet).
    #[inline]
    pub fn saturating_mul(self, rhs: Witness<u64>) -> Witness<u64> {
        Witness(self.0.saturating_mul(rhs.0))
    }

    /// The min of two sound upper bounds is a sound upper bound.
    #[inline]
    pub fn min(self, rhs: Witness<u64>) -> Witness<u64> {
        Witness(self.0.min(rhs.0))
    }

    /// TEST-ONLY door: unit tests exercising gate laws without a bank.
    /// Never a production path — the rig feature gates it out of the
    /// server build.
    #[cfg(feature = "rig")]
    pub fn assume_for_test(v: u64) -> Witness<u64> {
        Witness(v)
    }
}

impl Witness<(i64, i64)> {
    /// Exact column domain `(lo, hi)` from the part records — present
    /// only when EVERY part carries exact min/max (the stats face's own
    /// exactness law); coarse or missing stats witness nothing.
    #[inline]
    pub fn exact_domain(sv: &StatsView) -> Option<Witness<(i64, i64)>> {
        sv.minmax_exact().map(Witness)
    }
}
