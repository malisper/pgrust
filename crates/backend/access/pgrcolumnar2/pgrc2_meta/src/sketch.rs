//! ST-1 (OD-2, RULED 2026-08-12): the part-grain distribution sketch —
//! MCV top-k with EXACT per-part counts + equi-depth histogram bounds over
//! canonical value bytes, accumulated at seal (stats are a seal byproduct;
//! "banks ship stats-built" is structural). HLL NDV stays in-footer (spec
//! §8.4); this module never replaces it.
//!
//! Exactness class: EXACT-AT-SEAL per part-generation. Values longer than
//! [`STATS_MCV_VALUE_MAX`] participate in COUNTS but are ineligible for
//! the value lists (`long_values` records how many observations that
//! excluded — the fold reads it and degrades honestly instead of serving
//! a histogram that silently dropped the long tail).
//!
//! DISTACC-KILL (the prof-ingest finding): the original accumulator was a
//! `BTreeMap<Vec<u8>, u64>` — every observation paid an O(log D) tree
//! descent of byte compares, and that memcmp lane alone was ~13% of ALL
//! ingest CPU (~21% with the tree insert; profile of record
//! CI-results/prof-ingest). The accumulator is now an open-addressing
//! hash table keyed on the 128-bit `meta_hash128` the builder ALREADY
//! computes for bloom/HLL: an observation costs one masked probe, a
//! 128-bit hash compare, and ONE byte-equality confirm on the (almost
//! always hit) full-hash match — the per-value byte-rank comparisons are
//! gone. Exactness is untouched: byte equality still adjudicates identity
//! (a 128-bit collision cannot merge two values), and the byte order the
//! BTreeMap provided for free is recovered by ONE finalize-time sort over
//! the surviving distinct set (D log D once per part, not N log D during
//! observe).
//!
//! Determinism: both arms feed [`ColDistribution`] through the same
//! byte-ordered finish path — iteration order of the hash table never
//! reaches sidecar bytes (the finalize sort is total: distinct entries
//! have distinct bytes). MCV ties break by byte order, as before.
//!
//! Kill switch: `PGRUST_PGRC2_DISTACC_HASH=0|off` restores the BTreeMap
//! reference arm (the A/B control and the equivalence witness; both arms
//! are pinned output-equal by the adversarial battery below).
//!
//! DICT-DEDUP (the #971 follow-up finding): on dict-elected columns the
//! WRITER already maintains the part's exact counted distinct set — the
//! dict election's builder observes every non-null value to mint the
//! byte-rank dictionary. Maintaining a second distinct structure here over
//! the same bytes double-accounts the most expensive columns (strings).
//! [`distribution_from_sorted_counts`] is the dedup seam: the seal driver
//! hands the dict build's counted, byte-rank-sorted entry set to the meta
//! builder, which serves `distribution()` through the SAME finish path
//! ([`finish_distribution`]) this accumulator uses — one implementation of
//! the MCV/histogram finish, so the two producers cannot drift. Non-dict
//! columns, demoted dict arms, and the count-free D2 inherit path keep the
//! accumulator below.

use std::collections::BTreeMap;

use crate::format::sidecar::{
    ColDistribution, STATS_HIST_BOUNDS, STATS_MCV_K, STATS_MCV_VALUE_MAX,
};
use crate::hash::meta_hash128;

/// The DISTACC arm switch (read at accumulator construction — the
/// `populate_on` parse: unset or anything but "0"/"off" = hash arm).
fn hash_arm_on() -> bool {
    !matches!(
        std::env::var("PGRUST_PGRC2_DISTACC_HASH").as_deref(),
        Ok("0") | Ok("off")
    )
}

/// One distinct eligible value in the hash arm: the precomputed 128-bit
/// identity, the exact count, and the canonical bytes (stored once, on
/// first sight — the confirm + finalize-sort key).
#[derive(Debug)]
struct Entry {
    h1: u64,
    h2: u64,
    count: u64,
    val: Vec<u8>,
}

#[derive(Debug)]
enum Acc {
    /// Default: open addressing over `meta_hash128`. `slots` holds
    /// index+1 into `entries` (0 = empty); power-of-two capacity, linear
    /// probing, grown below 3/4 load. Probe compares the stored 128-bit
    /// hash first; bytes are compared exactly once, on the full-hash
    /// match that adjudicates identity.
    Hash { slots: Vec<u32>, entries: Vec<Entry> },
    /// `PGRUST_PGRC2_DISTACC_HASH=0|off`: the original byte-keyed tree
    /// (kept verbatim as the A/B control / equivalence witness).
    BTree(BTreeMap<Vec<u8>, u64>),
}

/// The accumulating half (fed per nonnull value by `builder.rs`).
#[derive(Debug)]
pub struct DistAcc {
    acc: Acc,
    long_values: u64,
    nonnull: u64,
}

impl Default for DistAcc {
    fn default() -> DistAcc {
        DistAcc::with_hash_arm(hash_arm_on())
    }
}

impl DistAcc {
    /// Arm-explicit constructor (tests + the env default above).
    pub fn with_hash_arm(hash: bool) -> DistAcc {
        DistAcc {
            acc: if hash {
                Acc::Hash {
                    slots: Vec::new(),
                    entries: Vec::new(),
                }
            } else {
                Acc::BTree(BTreeMap::new())
            },
            long_values: 0,
            nonnull: 0,
        }
    }

    /// Observe one canonical value (the §18.1 bytes the hash lane already
    /// holds — no second canonicalization). Convenience face: hashes here.
    /// The builder's hot path uses [`DistAcc::observe_hashed`] with the
    /// `meta_hash128` it already computed for bloom/HLL.
    pub fn observe(&mut self, canonical: &[u8]) {
        let (h1, h2) = meta_hash128(canonical);
        self.observe_hashed(h1, h2, canonical);
    }

    /// Observe one canonical value whose `meta_hash128` is already in
    /// hand. `(h1, h2)` MUST be `meta_hash128(canonical)` — the builder's
    /// single hash site guarantees it; the compat face above recomputes.
    pub fn observe_hashed(&mut self, h1: u64, h2: u64, canonical: &[u8]) {
        self.nonnull += 1;
        if canonical.len() > STATS_MCV_VALUE_MAX {
            self.long_values += 1;
            return;
        }
        match &mut self.acc {
            Acc::BTree(counts) => {
                // to_vec only on first sight of a value (the common path
                // is a hit).
                if let Some(c) = counts.get_mut(canonical) {
                    *c += 1;
                } else {
                    counts.insert(canonical.to_vec(), 1);
                }
            }
            Acc::Hash { slots, entries } => {
                // Grow below 3/4 load (also the lazy init: 0 slots grows
                // to the initial table before the first probe).
                if (entries.len() + 1) * 4 > slots.len() * 3 {
                    grow(slots, entries);
                }
                let mask = slots.len() - 1;
                let mut i = probe_start(slots, h1).expect("grown");
                loop {
                    let s = slots[i];
                    if s == 0 {
                        debug_assert!(entries.len() < u32::MAX as usize);
                        entries.push(Entry {
                            h1,
                            h2,
                            count: 1,
                            val: canonical.to_vec(),
                        });
                        slots[i] = entries.len() as u32;
                        return;
                    }
                    let e = &mut entries[(s - 1) as usize];
                    if e.h1 == h1 && e.h2 == h2 && e.val.as_slice() == canonical {
                        e.count += 1;
                        return;
                    }
                    i = (i + 1) & mask;
                }
            }
        }
    }

    /// D-STATS: touch the slot word `h1` would probe first — the batched
    /// hash lane runs this a few values ahead of the scalar
    /// [`DistAcc::observe_hashed`] inserts so the probe's cache line is in
    /// flight when the insert lands. A plain masked read (no unsafe, no
    /// state change); `black_box` keeps the load. No-op on the BTree
    /// control arm and before the lazy table exists.
    #[inline]
    pub fn prefetch(&self, h1: u64) {
        if let Acc::Hash { slots, .. } = &self.acc {
            if let Some(i) = probe_start(slots, h1) {
                std::hint::black_box(slots[i]);
            }
        }
    }

    #[cfg(test)]
    fn prefetch_slot(&self, h1: u64) -> Option<usize> {
        match &self.acc {
            Acc::Hash { slots, .. } => probe_start(slots, h1),
            Acc::BTree(_) => None,
        }
    }

    /// pgrc2.1 §2.2: the EXACT part NDV when the distinct set is complete
    /// (no long-value residue — values over the eligibility bound are
    /// tallied in `long_values`, never deduped, so any residue makes the
    /// exact count unknowable). O(1).
    pub fn distinct_exact(&self) -> Option<u64> {
        if self.long_values != 0 {
            return None;
        }
        Some(match &self.acc {
            Acc::BTree(counts) => counts.len() as u64,
            Acc::Hash { entries, .. } => entries.len() as u64,
        })
    }

    /// Finalize into the sidecar's [`ColDistribution`]: MCV top-k
    /// (count-descending, byte-order tiebreak) + equi-depth bounds. Both
    /// arms converge on the same byte-ordered finish path
    /// ([`finish_distribution`] — shared with the DICT-DEDUP feed).
    pub fn finalize(&self) -> ColDistribution {
        // The byte-ordered distinct set: the BTreeMap iterates in byte
        // order; the hash arm recovers it with one sort over D entries
        // (total order — distinct entries have distinct bytes).
        let ordered: Vec<(&[u8], u64)> = match &self.acc {
            Acc::BTree(counts) => counts.iter().map(|(k, &v)| (k.as_slice(), v)).collect(),
            Acc::Hash { entries, .. } => {
                let mut v: Vec<(&[u8], u64)> = entries
                    .iter()
                    .map(|e| (e.val.as_slice(), e.count))
                    .collect();
                v.sort_unstable_by(|a, b| a.0.cmp(b.0));
                v
            }
        };
        finish_distribution(&ordered, self.nonnull, self.long_values)
    }
}

/// The ONE byte-ordered finish path: MCV top-k (count-descending,
/// byte-order tiebreak) + equi-depth histogram bounds over `ordered` — the
/// byte-ascending ELIGIBLE (len ≤ [`STATS_MCV_VALUE_MAX`]) distinct set
/// with exact counts. Shared by [`DistAcc::finalize`] and
/// [`distribution_from_sorted_counts`] so the accumulator arm and the
/// DICT-DEDUP feed arm cannot drift: sidecar bytes are this function's
/// output on both, a pure function of the (set, counts, nonnull,
/// long_values) facts.
fn finish_distribution(
    ordered: &[(&[u8], u64)],
    nonnull: u64,
    long_values: u64,
) -> ColDistribution {
    debug_assert!(
        ordered.windows(2).all(|w| w[0].0 < w[1].0),
        "finish over a non-byte-ordered distinct set"
    );
    let eligible_total: u64 = ordered.iter().map(|&(_, c)| c).sum();
    // MCV top-k: stable selection — sort (count desc, bytes asc).
    let mut by_count = ordered.to_vec();
    by_count.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(b.0)));
    let mcv: Vec<(Vec<u8>, u64)> = by_count
        .iter()
        .take(STATS_MCV_K)
        .map(|&(k, v)| (k.to_vec(), v))
        .collect();
    // Equi-depth bounds over eligible values in byte order: bound i is
    // the value at cumulative rank ceil(i * total / (nbounds-1)),
    // bound 0 the min, the last bound the max.
    let mut hist_bounds: Vec<Vec<u8>> = Vec::new();
    if eligible_total > 0 {
        let nb = STATS_HIST_BOUNDS as u64;
        let mut targets: Vec<u64> = (0..nb)
            .map(|i| (i * (eligible_total - 1)) / (nb - 1))
            .collect();
        targets.dedup();
        let mut cum = 0u64;
        let mut ti = 0usize;
        for &(k, c) in ordered {
            let lo = cum;
            cum += c;
            while ti < targets.len() && targets[ti] >= lo && targets[ti] < cum {
                hist_bounds.push(k.to_vec());
                ti += 1;
            }
            if ti >= targets.len() {
                break;
            }
        }
    }
    hist_bounds.dedup();
    ColDistribution {
        nonnull,
        ndv_eligible: ordered.len() as u64,
        long_values,
        mcv,
        hist_bounds,
    }
}

/// DICT-DEDUP: [`ColDistribution`] from an externally maintained counted
/// distinct set — the dict election's byte-rank-sorted entry list with
/// exact per-part counts (one structure, two consumers; module doc).
///
/// Contract (the dict build guarantees both by construction — its strict
/// byte-rank sort is contractual and mechanically re-proven at emit):
/// `entries` is strictly byte-ascending (distinct), and each count is the
/// value's exact non-null occurrence count over the part, so Σ counts IS
/// the part's nonnull row count (the dict observes every non-null row —
/// oversize values demote the dict arm before this face can be reached).
///
/// The [`STATS_MCV_VALUE_MAX`] eligibility gate the accumulator applies at
/// OBSERVE grain is applied here at ENTRY grain — the same partition of
/// the same multiset (eligibility is a pure function of value length), so
/// the output equals [`DistAcc::finalize`] over the expanded value
/// sequence byte-for-byte (pinned by the equivalence battery below).
pub fn distribution_from_sorted_counts(entries: &[(Vec<u8>, u64)]) -> ColDistribution {
    let nonnull: u64 = entries.iter().map(|(_, c)| c).sum();
    let mut long_values = 0u64;
    let mut ordered: Vec<(&[u8], u64)> = Vec::with_capacity(entries.len());
    for (val, count) in entries {
        if val.len() > STATS_MCV_VALUE_MAX {
            long_values += count;
        } else {
            ordered.push((val.as_slice(), *count));
        }
    }
    finish_distribution(&ordered, nonnull, long_values)
}

/// Grow (or lazily create) the slot table and rehash by the STORED h1 —
/// no byte access on the rehash path.
/// The slot `h1` probes first (the seeded index shared with `grow`); None
/// before the lazy table exists.
#[inline]
fn probe_start(slots: &[u32], h1: u64) -> Option<usize> {
    if slots.is_empty() {
        return None;
    }
    Some(crate::hash::slot_index(h1, slots.len() - 1))
}

fn grow(slots: &mut Vec<u32>, entries: &[Entry]) {
    let ncap = if slots.is_empty() { 16 } else { slots.len() * 2 };
    let mask = ncap - 1;
    let mut ns = vec![0u32; ncap];
    for (idx, e) in entries.iter().enumerate() {
        let mut i = crate::hash::slot_index(e.h1, mask);
        while ns[i] != 0 {
            i = (i + 1) & mask;
        }
        ns[i] = idx as u32 + 1;
    }
    *slots = ns;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_counts_topk_and_bounds() {
        let mut d = DistAcc::default();
        // 100 x "a", 10 x "b", 1 x each of c00..c49.
        for _ in 0..100 {
            d.observe(b"a");
        }
        for _ in 0..10 {
            d.observe(b"b");
        }
        for i in 0..50 {
            d.observe(format!("c{i:02}").as_bytes());
        }
        let out = d.finalize();
        assert_eq!(out.nonnull, 160);
        assert_eq!(out.ndv_eligible, 52);
        assert_eq!(out.long_values, 0);
        assert_eq!(out.mcv[0], (b"a".to_vec(), 100));
        assert_eq!(out.mcv[1], (b"b".to_vec(), 10));
        // singles tiebreak by byte order
        assert_eq!(out.mcv[2].0, b"c00".to_vec());
        assert_eq!(out.mcv.len(), STATS_MCV_K.min(52));
        // bounds: byte-ordered, min first, max last
        assert_eq!(out.hist_bounds.first().unwrap(), &b"a".to_vec());
        assert_eq!(out.hist_bounds.last().unwrap(), &b"c49".to_vec());
        let mut sorted = out.hist_bounds.clone();
        sorted.sort();
        assert_eq!(sorted, out.hist_bounds, "bounds are byte-ordered");
    }

    // The look-ahead hint must warm the line the insert's first probe
    // touches: with one entry in a fresh table, that is where it landed.
    #[test]
    fn prefetch_targets_the_insert_probe_slot() {
        let mut d = DistAcc::default();
        assert_eq!(d.prefetch_slot(1), None);
        for v in [&b"v"[..], b"w", b"xyz", b"0123456789"] {
            let mut d = DistAcc::default();
            let (h1, h2) = crate::hash::meta_hash128(v);
            d.observe_hashed(h1, h2, v);
            let Acc::Hash { slots, .. } = &d.acc else { panic!("hash arm") };
            let landed = slots.iter().position(|&s| s == 1).unwrap();
            assert_eq!(probe_start(slots, h1), Some(landed));
            assert_eq!(d.prefetch_slot(h1), Some(landed));
        }
        d.observe(b"v");
        assert!(d.prefetch_slot(1).is_some());
    }

    #[test]
    fn long_values_counted_but_ineligible() {
        let mut d = DistAcc::default();
        let long = vec![7u8; STATS_MCV_VALUE_MAX + 1];
        d.observe(&long);
        d.observe(b"x");
        let out = d.finalize();
        assert_eq!(out.nonnull, 2);
        assert_eq!(out.long_values, 1);
        assert_eq!(out.ndv_eligible, 1);
        assert!(out.mcv.iter().all(|(v, _)| v.len() <= STATS_MCV_VALUE_MAX));
    }

    #[test]
    fn deterministic_across_observation_order() {
        let vals: Vec<Vec<u8>> = (0..200u32).map(|i| i.to_le_bytes().to_vec()).collect();
        let mut a = DistAcc::default();
        for v in &vals {
            a.observe(v);
        }
        let mut b = DistAcc::default();
        for v in vals.iter().rev() {
            b.observe(v);
        }
        assert_eq!(a.finalize(), b.finalize());
    }

    // ---- DISTACC-KILL equivalence battery (born-RED charter) ------------
    //
    // The hash arm must be OUTPUT-IDENTICAL to the BTreeMap reference arm
    // on every corpus — ColDistribution is sealed sidecar bytes (nonnull,
    // ndv_eligible, long_values, MCV values+counts+order, hist bounds),
    // so any divergence here is a byte-law break, not a tuning delta.

    /// xorshift64* — deterministic corpus generator (no external entropy).
    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x >> 12;
            x ^= x << 25;
            x ^= x >> 27;
            self.0 = x;
            x.wrapping_mul(0x2545F4914F6CDD1D)
        }
    }

    fn assert_arms_equal(corpus: &[Vec<u8>], what: &str) {
        let mut h = DistAcc::with_hash_arm(true);
        let mut r = DistAcc::with_hash_arm(false);
        for v in corpus {
            h.observe(v);
            r.observe(v);
        }
        let (fh, fr) = (h.finalize(), r.finalize());
        assert_eq!(fh, fr, "hash arm diverged from BTreeMap reference: {what}");
        // And observation order must not matter on the hash arm either.
        let mut h2 = DistAcc::with_hash_arm(true);
        for v in corpus.iter().rev() {
            h2.observe(v);
        }
        assert_eq!(h2.finalize(), fr, "hash arm order-sensitive: {what}");
    }

    #[test]
    fn arms_equal_per_width_families() {
        // The canonical-byte families the builder actually feeds: 1-byte
        // (bool), 4/8-byte LE words, fixed-16, and variable text.
        let mut rng = Rng(0x5EED_1);
        let bools: Vec<Vec<u8>> = (0..1000).map(|_| vec![(rng.next() & 1) as u8]).collect();
        assert_arms_equal(&bools, "bool family");

        let mut rng = Rng(0x5EED_2);
        let w4: Vec<Vec<u8>> = (0..20_000)
            .map(|_| ((rng.next() % 3000) as u32).to_le_bytes().to_vec())
            .collect();
        assert_arms_equal(&w4, "4-byte word family (duplicate-heavy)");

        let mut rng = Rng(0x5EED_3);
        let w8: Vec<Vec<u8>> = (0..20_000).map(|_| rng.next().to_le_bytes().to_vec()).collect();
        assert_arms_equal(&w8, "8-byte word family (all-distinct)");

        let mut rng = Rng(0x5EED_4);
        let f16: Vec<Vec<u8>> = (0..5000)
            .map(|_| {
                let mut v = vec![0u8; 16];
                v[..8].copy_from_slice(&(rng.next() % 500).to_le_bytes());
                v
            })
            .collect();
        assert_arms_equal(&f16, "fixed-16 family");

        let mut rng = Rng(0x5EED_5);
        let text: Vec<Vec<u8>> = (0..10_000)
            .map(|_| format!("url/{}/page", rng.next() % 4000).into_bytes())
            .collect();
        assert_arms_equal(&text, "text family (zipf-ish)");
    }

    #[test]
    fn arms_equal_adversarial_probe_chains() {
        // Values filtered to share low h1 bits: they pile into the same
        // initial buckets, forcing long linear-probe chains and rehash
        // relocations — the collision paths the reference arm never has.
        let mut corpus = Vec::new();
        let mut i: u64 = 0;
        while corpus.len() < 3000 {
            let v = i.to_le_bytes().to_vec();
            let (h1, _) = meta_hash128(&v);
            if h1 & 0x3F == 0 {
                corpus.push(v);
            }
            i += 1;
        }
        // Duplicate a slice of them heavily so hit-confirms also traverse
        // the chains.
        let dups: Vec<Vec<u8>> = corpus.iter().take(50).cloned().collect();
        for _ in 0..40 {
            corpus.extend(dups.iter().cloned());
        }
        assert_arms_equal(&corpus, "same-bucket probe chains + dup hits");
    }

    #[test]
    fn arms_equal_edges_ties_and_long_values() {
        // Empty value, the exact eligibility boundary, long values, and
        // an all-ties MCV field (every count equal — tiebreak is pure
        // byte order, the exact sealed-order hazard).
        let mut corpus: Vec<Vec<u8>> = Vec::new();
        corpus.push(Vec::new()); // b""
        corpus.push(vec![0u8]);
        corpus.push(vec![0u8; STATS_MCV_VALUE_MAX]); // last eligible len
        corpus.push(vec![0u8; STATS_MCV_VALUE_MAX + 1]); // first long len
        corpus.push(vec![255u8; STATS_MCV_VALUE_MAX + 7]);
        for i in 0..100u8 {
            for _ in 0..3 {
                corpus.push(vec![i, i ^ 0x55]); // 100 values, count 3 each
            }
        }
        assert_arms_equal(&corpus, "edges + all-ties MCV");
    }

    #[test]
    fn arms_equal_growth_and_rehash() {
        // Enough distinct values to force many doublings from the lazy
        // 16-slot start; counts must survive every rehash.
        let corpus: Vec<Vec<u8>> = (0..100_000u64)
            .map(|i| i.to_le_bytes().to_vec())
            .collect();
        assert_arms_equal(&corpus, "growth ladder to 100k distinct");
    }

    // ---- DICT-DEDUP feed equivalence battery (born-RED charter) ----------
    //
    // `distribution_from_sorted_counts` over a counted distinct set must be
    // OUTPUT-IDENTICAL to the accumulator (BOTH #971 arms) over the
    // expanded value sequence — the feed becomes sealed sidecar bytes on
    // dict-elected columns, so any divergence is a byte-law break.

    /// Counted set (byte-sorted) from a corpus + the corpus expanded in a
    /// deterministic non-sorted interleave, then all three producers.
    fn assert_feed_equals_acc(corpus: &[Vec<u8>], what: &str) {
        let mut counts: BTreeMap<Vec<u8>, u64> = BTreeMap::new();
        for v in corpus {
            *counts.entry(v.clone()).or_insert(0) += 1;
        }
        let entries: Vec<(Vec<u8>, u64)> = counts.into_iter().collect();
        let fed = distribution_from_sorted_counts(&entries);
        let mut h = DistAcc::with_hash_arm(true);
        let mut r = DistAcc::with_hash_arm(false);
        for v in corpus {
            h.observe(v);
            r.observe(v);
        }
        assert_eq!(fed, h.finalize(), "feed diverged from hash acc: {what}");
        assert_eq!(fed, r.finalize(), "feed diverged from BTree acc: {what}");
    }

    #[test]
    fn feed_equals_accumulator_families() {
        let mut rng = Rng(0xD1C7_1);
        let text: Vec<Vec<u8>> = (0..10_000)
            .map(|_| format!("url/{}/page", rng.next() % 4000).into_bytes())
            .collect();
        assert_feed_equals_acc(&text, "text family (zipf-ish)");

        let mut rng = Rng(0xD1C7_2);
        let dup4: Vec<Vec<u8>> = (0..20_000)
            .map(|_| ((rng.next() % 300) as u32).to_le_bytes().to_vec())
            .collect();
        assert_feed_equals_acc(&dup4, "duplicate-heavy 4-byte family");

        let mut rng = Rng(0xD1C7_3);
        let uniq: Vec<Vec<u8>> = (0..5_000).map(|_| rng.next().to_le_bytes().to_vec()).collect();
        assert_feed_equals_acc(&uniq, "all-distinct family");
    }

    #[test]
    fn feed_equals_accumulator_edges_long_values_and_ties() {
        // Empty value, the exact eligibility boundary, LONG dict entries
        // (a dict entry may exceed STATS_MCV_VALUE_MAX — the entry-grain
        // gate must reproduce the observe-grain gate's counts exactly),
        // and an all-ties MCV field.
        let mut corpus: Vec<Vec<u8>> = Vec::new();
        corpus.push(Vec::new());
        corpus.push(vec![0u8; STATS_MCV_VALUE_MAX]); // last eligible len
        for _ in 0..7 {
            corpus.push(vec![9u8; STATS_MCV_VALUE_MAX + 1]); // long, count 7
        }
        for _ in 0..3 {
            corpus.push(vec![255u8; STATS_MCV_VALUE_MAX + 40]); // long, count 3
        }
        for i in 0..100u8 {
            for _ in 0..3 {
                corpus.push(vec![i, i ^ 0x55]); // all-ties MCV
            }
        }
        assert_feed_equals_acc(&corpus, "edges + long entries + ties");
    }

    #[test]
    fn feed_empty_set_matches_empty_accumulator() {
        assert_eq!(
            distribution_from_sorted_counts(&[]),
            DistAcc::default().finalize(),
            "zero-entry feed must equal a zero-observation accumulator"
        );
    }

    #[test]
    fn observe_hashed_matches_observe() {
        // The builder feeds the precomputed hash; the compat face hashes
        // internally. Same bytes -> same accumulator state.
        let mut rng = Rng(0xFEED);
        let corpus: Vec<Vec<u8>> = (0..5000)
            .map(|_| (rng.next() % 1000).to_le_bytes().to_vec())
            .collect();
        let mut a = DistAcc::with_hash_arm(true);
        let mut b = DistAcc::with_hash_arm(true);
        for v in &corpus {
            a.observe(v);
            let (h1, h2) = meta_hash128(v);
            b.observe_hashed(h1, h2, v);
        }
        assert_eq!(a.finalize(), b.finalize());
    }
}
