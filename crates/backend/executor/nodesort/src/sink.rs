// M3 sort — top-N sink kernels (docs/design/m3-sort.md §3, inc-1).
//
// The POD bounded (key, rowref) heap on the tie-ordering rule-2 total order
// (docs/conformance/tie-ordering.md): an entry is ONE u128 word packing
// (null tier, direction-folded key word, 48-bit physical rowref), compared
// as a plain integer. Rowrefs are unique across a scan (disjoint granule
// claims — the caller's invariant), so two entries can never compare equal
// and the `bound` smallest elements of any union are a PURE FUNCTION of the
// table contents — independent of morsel claim order, worker count, and
// arrival interleaving. That is the whole determinism argument of the
// parallel top-N sink: no tie tracking, no demote ladder, nothing to
// ratify (the property rule 2 bought the serial adaptive feed, inherited).
//
// Deliberately NOT a Tuplesort: the tuplesort is Mcx-bound (a Send story
// would need lifetime erasure, the m2-distinct lane's unsafe argument) and
// its C-parity machinery buys nothing for a ≤ 65536-entry POD heap whose
// selection semantics are defined by ratified rule 2, not by C's
// heap-shape accidents. The serial sort arms remain the parity oracle.
//
// Phase-1 vocabulary: single int-family sort key widened to i64 at the
// read leg (the refsort/zone-entry mapping); DESC and NULLS FIRST/LAST
// fold into the encoding (§ TopnEntry::encode). The SealedParallelSink
// impl over these kernels lands with the engagement arm (execmain
// lanev2/runtime_sort.rs, inc-2) — the m2 house pattern: pure kernels in
// the operator crate, the contract impl at the engagement seam. Kernel
// worker roles: accept = TopnHeap::push (+ `admits` for the keep mask),
// seal = TopnHeap::into_sorted (parallel per Local), combine =
// topn_merge (partitions() = 1), leader gather decodes TopnEntry::rowref.

use std::collections::BinaryHeap;

/// Bound cap — the serial caps' agreement (`REFSORT_MAX_BOUND` and
/// `ADAPTIVE_TOPK_MAX_BOUND` are both 1<<16): past this a top-N is
/// scan-shaped anyway. Admission (inc-2) refuses larger bounds.
pub const TOPN_MAX_BOUND: usize = 1 << 16;

pub const ROWREF_BITS: u32 = 48;
/// Largest encodable rowref: `refsort_encode`'s (row_group << 32) | row
/// address space (48 bits, monotone in physical position).
pub const TOPN_MAX_ROWREF: u64 = (1 << ROWREF_BITS) - 1;

/// One packed heap entry. Ordering IS `u128` ordering:
///
/// ```text
/// bits 112..113  null tier   0 = the first-emitted class (nulls iff
///                            NULLS FIRST), 1 = the other class
/// bits  48..112  key word    direction-folded order-preserving image of
///                            the i64 key (0 for nulls — canonical, so
///                            equal nulls tie-break on rowref alone)
/// bits   0..48   rowref      physical (rg, row) address, ascending =
///                            physically earlier (rule-2 tie-break)
/// ```
///
/// "Smaller entry" = "emitted earlier". The winner set of a top-N is the
/// `bound` smallest entries.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct TopnEntry(u128);

impl TopnEntry {
    /// Fold one key observation into the total order. `key` is the sort
    /// key widened to i64 (int2/int4/int8/oid/date/time/timestamp — the
    /// int-family read-leg contract); `desc`/`nulls_first` are the plan's
    /// per-key flags (PG resolves the defaults: ASC ⇒ NULLS LAST, DESC ⇒
    /// NULLS FIRST; explicit NULLS FIRST/LAST overrides ride the same
    /// flag). `rowref` is the 48-bit physical address.
    #[inline]
    pub fn encode(key: i64, null: bool, desc: bool, nulls_first: bool, rowref: u64) -> TopnEntry {
        debug_assert!(rowref <= TOPN_MAX_ROWREF, "rowref exceeds the 48-bit contract");
        // Tier 0 is emitted first: nulls land there exactly when NULLS
        // FIRST (null XOR nulls_first == 0 covers both agreeing cases).
        let tier = (null ^ nulls_first) as u128;
        let word = if null {
            0 // canonical null image: equal nulls order by rowref alone
        } else {
            // Order-preserving signed→unsigned map, then bit-flip for DESC
            // (descending key order = ascending flipped-word order).
            let asc = (key as u64) ^ (1 << 63);
            if desc {
                !asc
            } else {
                asc
            }
        };
        TopnEntry((tier << 112) | ((word as u128) << 48) | rowref as u128)
    }

    /// The physical (rg, row) address — what the leader's late-mat gather
    /// decodes (`refsort_encode`'s inverse lives scan-side).
    #[inline]
    pub fn rowref(self) -> u64 {
        (self.0 as u64) & TOPN_MAX_ROWREF
    }

    /// Raw packed word (tests / probes).
    #[inline]
    pub fn raw(self) -> u128 {
        self.0
    }

    /// The entry's [`cut64`] image (leading-key null tier + direction-folded
    /// word, rowref dropped) — the shared-cutoff comparison space (GCUT,
    /// night/sort-merge-redesign inc-2).
    #[inline]
    pub fn cut64(self) -> u64 {
        cut64((self.0 >> 112) & 1 != 0, (self.0 >> 48) as u64)
    }
}

/// Direction-folded order-preserving word of a NON-NULL key — exactly the
/// word [`TopnEntry::encode`]/`key_word128` pack (exposed so zone-metadata
/// cutoff comparisons share the fold law; drift here would break the GCUT
/// prune-safety argument).
#[inline]
pub fn key_order_word(key: i64, desc: bool) -> u64 {
    let asc = (key as u64) ^ (1 << 63);
    if desc {
        !asc
    } else {
        asc
    }
}

/// The 64-bit SHARED-CUTOFF comparison space (GCUT, night/sort-merge-
/// redesign inc-2): `(tier:1 | word>>1:63)` of an entry's LEADING key.
/// The 65-bit (tier, word) prefix is truncated by one word bit so it fits
/// one `AtomicU64`; comparisons are STRICT `>`, so the truncation is
/// conservative — `a.cut64() > b.cut64()` implies the full entry order
/// `a > b` (tier dominates, then word; equal truncated words never prune).
/// Prune safety: any worker's full-heap floor f satisfies f >= the final
/// global k-th entry G (a subset's k-th best only tightens toward the
/// union's), and entries are unique (disjoint rowrefs), so pruning e with
/// `e.cut64() > min_floors.cut64()` implies e > f >= G — e cannot be in
/// the global top-k.
#[inline]
pub fn cut64(tier: bool, word: u64) -> u64 {
    ((tier as u64) << 63) | (word >> 1)
}

/// Multi-key cap (inc-5): entries carry up to this many packed key words.
/// Past it the shape refuses to the serial arms (admission, not data).
pub const TOPN_MAX_KEYS: usize = 4;

/// One packed per-key word for the WIDE entry: (tier:1 | word:64) in a
/// u128, upper bits zero — the exact per-key law of `TopnEntry::encode`
/// (null tier per nulls_first; direction-folded order-preserving word;
/// canonical zero word for nulls).
#[inline]
fn key_word128(key: i64, null: bool, desc: bool, nulls_first: bool) -> u128 {
    let tier = (null ^ nulls_first) as u128;
    let word = if null {
        0
    } else {
        let asc = (key as u64) ^ (1 << 63);
        if desc {
            !asc
        } else {
            asc
        }
    };
    (tier << 64) | word as u128
}

/// Wide (multi-key) heap entry: lexicographic over the packed key words,
/// then rowref ascending — the rule-2 total order at key arity 2..=4.
/// Unused key slots are zero and identical across one sort's entries, so
/// the derived full-array Ord equals the nkeys-prefix ordering.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct WideEntry {
    keys: [u128; TOPN_MAX_KEYS],
    rowref: u64,
}

impl WideEntry {
    /// `keys` = per-key (widened i64, isnull) observations in sort-key
    /// order; `flags` = the plan's per-key (desc, nulls_first). Lengths
    /// equal, `1..=TOPN_MAX_KEYS` (the top-N HEAP uses `TopnEntry` at
    /// arity 1 — cheaper entry; the full-sort run entries use WideEntry
    /// at every arity, m3-sort-b shape b).
    #[inline]
    pub fn encode(keys: &[(i64, bool)], flags: &[(bool, bool)], rowref: u64) -> WideEntry {
        debug_assert!(keys.len() == flags.len());
        debug_assert!((1..=TOPN_MAX_KEYS).contains(&keys.len()));
        debug_assert!(rowref <= TOPN_MAX_ROWREF);
        let mut packed = [0u128; TOPN_MAX_KEYS];
        for (i, (&(k, n), &(d, nf))) in keys.iter().zip(flags).enumerate() {
            packed[i] = key_word128(k, n, d, nf);
        }
        WideEntry { keys: packed, rowref }
    }

    /// The physical (rg, row) address for the leader's late-mat gather.
    #[inline]
    pub fn rowref(self) -> u64 {
        self.rowref
    }

    /// The entry's [`cut64`] image over the LEADING key (lexicographic
    /// order: a strictly greater leading (tier, word) makes the whole
    /// entry strictly greater, so the shared-cutoff prune law holds at
    /// every key arity).
    #[inline]
    pub fn cut64(self) -> u64 {
        let k0 = self.keys[0];
        cut64((k0 >> 64) & 1 != 0, k0 as u64)
    }

    /// The packed per-key (tier | word) images (mjmerge's key-PREFIX
    /// vocabulary). Unused slots (arity < [`TOPN_MAX_KEYS`]) are zero and
    /// identical across one sort's entries, so full-array lexicographic
    /// comparison of two entries' words equals their nkeys-prefix ordering
    /// — the rowref tiebreak deliberately excluded (a JOIN groups rows by
    /// key equality; the rowref would make every "group" a singleton).
    #[inline]
    pub fn key_words(self) -> [u128; TOPN_MAX_KEYS] {
        self.keys
    }

    /// Was key `k` NULL at encode time, given the column's `nulls_first`
    /// flag? Inverse of [`key_word128`]'s tier law (`tier = null ^
    /// nulls_first`): the tier bit alone is direction-relative, so the
    /// caller must supply the same flag the entry was encoded under.
    #[inline]
    pub fn key_is_null(self, k: usize, nulls_first: bool) -> bool {
        ((self.keys[k] >> 64) & 1 != 0) ^ nulls_first
    }
}

/// The per-worker bounded heap (`SealedParallelSink::Local`): keeps the
/// `bound` smallest entries seen so far under the entry type's total order
/// (`TopnEntry` = single-key u128; `WideEntry` = multi-key). Plain owned
/// data — `Send` by construction, no arena borrows (the m2-agg-sink
/// decision-3 rule). Memory: ≤ bound × size_of::<T>() (16 B narrow, 72 B
/// wide), no work_mem interaction (design §7).
pub struct BoundedTopnHeap<T: Ord + Copy> {
    bound: usize,
    // std max-heap: the WORST retained entry sits at the root (the current
    // k-th boundary), so replace-top is `peek_mut` (sift-on-drop).
    heap: BinaryHeap<T>,
}

/// The single-key heap (the shipped inc-1 surface, unchanged semantics).
pub type TopnHeap = BoundedTopnHeap<TopnEntry>;
/// The multi-key heap (inc-5).
pub type TopnWideHeap = BoundedTopnHeap<WideEntry>;

impl<T: Ord + Copy> BoundedTopnHeap<T> {
    /// `bound` per the plan's LIMIT arithmetic; admission guarantees
    /// `1 ≤ bound ≤ TOPN_MAX_BOUND` (asserted here — a violation is an
    /// admission bug, not data).
    pub fn new(bound: usize) -> BoundedTopnHeap<T> {
        assert!(
            (1..=TOPN_MAX_BOUND).contains(&bound),
            "top-N sink bound {bound} outside admission envelope"
        );
        BoundedTopnHeap { bound, heap: BinaryHeap::with_capacity(bound) }
    }

    pub fn len(&self) -> usize {
        self.heap.len()
    }

    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }

    /// The current k-th boundary entry (the keep-mask floor), once the heap
    /// is full: an entry ≥ the floor can never enter. `None` while filling
    /// (everything admits).
    #[inline]
    pub fn floor(&self) -> Option<T> {
        if self.heap.len() == self.bound {
            self.heap.peek().copied()
        } else {
            None
        }
    }

    /// Keep-mask predicate for staged-batch prefiltering (the
    /// `TopkCutState` donor discipline, per-worker floor): `false` means
    /// `push` would provably be a no-op.
    #[inline]
    pub fn admits(&self, e: T) -> bool {
        match self.floor() {
            Some(f) => e < f,
            None => true,
        }
    }

    /// Accept one observation: replace-top under the total order.
    #[inline]
    pub fn push(&mut self, e: T) {
        if self.heap.len() < self.bound {
            self.heap.push(e);
            return;
        }
        // Full: strict improvement only. Equality is impossible (rowrefs
        // unique), so `<` is exact.
        let mut top = self.heap.peek_mut().expect("bound >= 1, heap full");
        if e < *top {
            *top = e;
        }
    }

    /// SEAL: consume into the ascending winner run (the combine's input).
    /// O(len log len), parallel across Local slots under the sealed sink.
    pub fn into_sorted(self) -> Vec<T> {
        self.heap.into_sorted_vec()
    }
}

/// COMBINE kernel (`partitions() = 1`): the `bound` smallest entries of the
/// union of sealed runs, each sorted ascending (`into_sorted`'s output).
/// K-way head merge with early exit at `bound`; the result is the winner
/// list in emission order. Deterministic for ANY input arrangement: the
/// total order has no ties, so the output is the unique sorted prefix of
/// the union — claim-order independence needs no further argument.
pub fn topn_merge<T: Ord + Copy>(sealed: &[Vec<T>], bound: usize) -> Vec<T> {
    use std::cmp::Reverse;
    debug_assert!(sealed.iter().all(|run| run.windows(2).all(|w| w[0] < w[1])), "unsorted sealed run");
    let mut pos = vec![1usize; sealed.len()];
    let mut heads: BinaryHeap<Reverse<(T, usize)>> = sealed
        .iter()
        .enumerate()
        .filter(|(_, run)| !run.is_empty())
        .map(|(i, run)| Reverse((run[0], i)))
        .collect();
    let cap = bound.min(sealed.iter().map(Vec::len).sum());
    let mut out = Vec::with_capacity(cap);
    while out.len() < bound {
        let Some(Reverse((e, run))) = heads.pop() else { break };
        out.push(e);
        let p = pos[run];
        if p < sealed[run].len() {
            heads.push(Reverse((sealed[run][p], run)));
            pos[run] = p + 1;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// GL-ASSERTMASK-1 R1 — the row-ref ENVELOPE, locked.
    ///
    /// `TopnEntry` packs `(tier << 112) | (word << 48) | rowref`, so the
    /// row ref gets exactly [`ROWREF_BITS`] bits and the only thing standing
    /// between an oversized ref and a silently corrupted sort key is a
    /// `debug_assert!` that no shipped profile compiles. The mints must
    /// therefore refuse anything this pack cannot represent; the columnar
    /// scan's `staged_rowref_base` does (`rg > u16::MAX => None`) and its
    /// sibling `window_ref` does not — see the letter's R1 row.
    ///
    /// This test locks the arithmetic both mints' refusal threshold rests on,
    /// with real `assert!`s so it is release-effective. It is a boundary bar,
    /// NOT a born-RED for the mint fix: driving `window_ref` past the
    /// threshold needs a relation with 2^16 row groups (> 2^32 rows), which
    /// no fixture in this tree can build.
    #[test]
    fn rowref_envelope_boundary_is_exactly_where_the_mints_refuse() {
        // A row group holds at most RG_ROWS = 2^16 rows, so an rg-local row
        // index spans 0..=65535 and the pack is (rg << 32) | row.
        const MAX_ROW: u64 = (1 << 16) - 1;
        const MAX_ADMISSIBLE_RG: u64 = u16::MAX as u64;
        let pack = |rg: u64, row: u64| (rg << 32) | row;

        // Every address the mints admit must survive the carrier intact.
        for rg in [0, 1, 2, MAX_ADMISSIBLE_RG - 1, MAX_ADMISSIBLE_RG] {
            for row in [0, 1, MAX_ROW / 2, MAX_ROW] {
                let rr = pack(rg, row);
                assert!(rr <= TOPN_MAX_ROWREF, "admissible (rg={rg}, row={row}) does not fit");
                let e = TopnEntry::encode(-42, false, false, false, rr);
                assert_eq!(e.rowref(), rr, "carrier lost an admissible ref ({rg}, {row})");
                let clean = TopnEntry::encode(-42, false, false, false, 0);
                assert_eq!(
                    e.raw() >> ROWREF_BITS,
                    clean.raw() >> ROWREF_BITS,
                    "an admissible ref disturbed the key image ({rg}, {row})"
                );
            }
        }

        // And the FIRST address past the threshold must not — this is the
        // damage the mints exist to prevent, and it is why the threshold sits
        // at u16::MAX rather than anywhere else.
        let over = pack(MAX_ADMISSIBLE_RG + 1, 0);
        assert!(over > TOPN_MAX_ROWREF, "the refusal threshold is not the carrier's limit");
        // Mirror `encode`'s pack rather than CALLING it. `encode` guards exactly
        // this case with a `debug_assert!`, which the dev tier still has, and
        // the whole point of this arm is what happens in the profiles where that
        // guard does not exist. `clean.raw()` is `(tier << 112) | (word << 48)`,
        // so OR-ing the ref reproduces `encode`'s output for it exactly — which
        // keeps this arm live in BOTH profiles instead of only the shipped ones.
        let clean = TopnEntry::encode(-42, false, false, false, 0);
        let packed_over = clean.raw() | over as u128;
        assert_eq!(
            (packed_over as u64) & TOPN_MAX_ROWREF,
            0,
            "the first unrepresentable ref aliases (rg=0, row=0)"
        );
        assert_ne!(
            packed_over >> ROWREF_BITS,
            clean.raw() >> ROWREF_BITS,
            "expected the overflow to corrupt the key image"
        );
    }

    /// Deterministic PRNG (splitmix64) — no rand dependency, reproducible
    /// failures.
    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 = self.0.wrapping_add(0x9e3779b97f4a7c15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
            z ^ (z >> 31)
        }
    }

    /// Reference comparator over RAW observations — the semantics the
    /// encoding must reproduce: null tier per nulls_first, key per
    /// asc/desc, rowref ascending.
    fn ref_cmp(
        a: (i64, bool, u64),
        b: (i64, bool, u64),
        desc: bool,
        nulls_first: bool,
    ) -> std::cmp::Ordering {
        use std::cmp::Ordering::*;
        let tier = |null: bool| if null == nulls_first { 0 } else { 1 };
        match tier(a.1).cmp(&tier(b.1)) {
            Equal => {}
            o => return o,
        }
        if !a.1 && !b.1 {
            let k = if desc { b.0.cmp(&a.0) } else { a.0.cmp(&b.0) };
            if k != Equal {
                return k;
            }
        }
        a.2.cmp(&b.2)
    }

    const KEY_SAMPLE: &[i64] =
        &[i64::MIN, i64::MIN + 1, -3, -2, -1, 0, 1, 2, 3, 1 << 40, i64::MAX - 1, i64::MAX];

    /// Encoding law: for every flag combination, encode order == reference
    /// order over the exhaustive key sample × null × rowref grid.
    #[test]
    fn encode_matches_reference_comparator() {
        let mut obs = Vec::new();
        for &k in KEY_SAMPLE {
            for null in [false, true] {
                for rowref in [0u64, 1, 7, TOPN_MAX_ROWREF] {
                    obs.push((k, null, rowref));
                }
            }
        }
        for desc in [false, true] {
            for nulls_first in [false, true] {
                for &a in &obs {
                    for &b in &obs {
                        if a == b {
                            continue;
                        }
                        let ea = TopnEntry::encode(a.0, a.1, desc, nulls_first, a.2);
                        let eb = TopnEntry::encode(b.0, b.1, desc, nulls_first, b.2);
                        // Null key images are canonical: same-null pairs
                        // with equal rowref collide only if a == b.
                        if ea == eb {
                            assert!(a.1 && b.1 && a.2 == b.2, "distinct obs encoded equal: {a:?} {b:?}");
                            continue;
                        }
                        assert_eq!(
                            ea.cmp(&eb),
                            ref_cmp(a, b, desc, nulls_first),
                            "desc={desc} nulls_first={nulls_first} a={a:?} b={b:?}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn rowref_roundtrip() {
        for rowref in [0u64, 1, (1 << 32) | 5, TOPN_MAX_ROWREF] {
            let e = TopnEntry::encode(-42, false, true, true, rowref);
            assert_eq!(e.rowref(), rowref);
        }
    }

    /// Random observation set: distinct rowrefs (the scan invariant),
    /// heavily duplicated keys (dense boundary ties), some nulls.
    fn random_obs(rng: &mut Rng, n: usize, key_space: i64) -> Vec<(i64, bool, u64)> {
        (0..n)
            .map(|i| {
                let null = rng.next() % 8 == 0;
                let key = (rng.next() % (key_space as u64)) as i64 - key_space / 2;
                (key, null, i as u64) // rowref = physical position
            })
            .collect()
    }

    fn encode_all(
        obs: &[(i64, bool, u64)],
        desc: bool,
        nulls_first: bool,
    ) -> Vec<TopnEntry> {
        obs.iter().map(|&(k, n, r)| TopnEntry::encode(k, n, desc, nulls_first, r)).collect()
    }

    /// The reference winner list: sort the whole union, take bound.
    fn reference_winners(entries: &[TopnEntry], bound: usize) -> Vec<TopnEntry> {
        let mut v = entries.to_vec();
        v.sort_unstable();
        v.truncate(bound);
        v
    }

    /// Single heap == reference selection, across bounds and flags,
    /// including bound ≥ n and dense-tie key spaces.
    #[test]
    fn heap_matches_reference() {
        let mut rng = Rng(0x1357);
        for &key_space in &[2i64, 5, 1 << 30] {
            for desc in [false, true] {
                for nulls_first in [false, true] {
                    let obs = random_obs(&mut rng, 500, key_space);
                    let entries = encode_all(&obs, desc, nulls_first);
                    for &bound in &[1usize, 2, 10, 499, 500, 512] {
                        let mut heap = TopnHeap::new(bound);
                        for &e in &entries {
                            heap.push(e);
                        }
                        assert_eq!(
                            heap.into_sorted(),
                            reference_winners(&entries, bound),
                            "key_space={key_space} desc={desc} nf={nulls_first} bound={bound}"
                        );
                    }
                }
            }
        }
    }

    /// Claim-order independence (the load-bearing property): partition the
    /// observations across W workers under multiple pseudo-random
    /// assignments AND per-worker arrival shuffles; per-worker heaps →
    /// seal → merge must equal the global reference every time.
    #[test]
    fn merge_is_claim_order_independent() {
        let mut rng = Rng(0xdecaf);
        let obs = random_obs(&mut rng, 800, 7); // 7 distinct keys: every boundary ties
        for desc in [false, true] {
            let entries = encode_all(&obs, desc, false);
            for &bound in &[1usize, 16, 100, 799, 800] {
                let want = reference_winners(&entries, bound);
                for trial in 0..8 {
                    let workers = 1 + (trial % 5); // W = 1..5
                    let mut streams: Vec<Vec<TopnEntry>> = vec![Vec::new(); workers];
                    for &e in &entries {
                        streams[(rng.next() as usize) % workers].push(e);
                    }
                    // Shuffle each worker's arrival order (Fisher-Yates).
                    for s in &mut streams {
                        for i in (1..s.len()).rev() {
                            s.swap(i, (rng.next() as usize) % (i + 1));
                        }
                    }
                    let sealed: Vec<Vec<TopnEntry>> = streams
                        .into_iter()
                        .map(|s| {
                            let mut h = TopnHeap::new(bound);
                            for e in s {
                                h.push(e);
                            }
                            h.into_sorted()
                        })
                        .collect();
                    assert_eq!(
                        topn_merge(&sealed, bound),
                        want,
                        "desc={desc} bound={bound} trial={trial}"
                    );
                }
            }
        }
    }

    /// Rule-2 face: all keys equal ⇒ winners are exactly the `bound`
    /// physically-earliest rowrefs, whatever the claim split.
    #[test]
    fn all_ties_select_physically_earliest() {
        let obs: Vec<(i64, bool, u64)> = (0..300).map(|i| (7, false, i as u64)).collect();
        let entries = encode_all(&obs, false, false);
        let bound = 10;
        let sealed: Vec<Vec<TopnEntry>> = entries
            .chunks(37) // arbitrary uneven split
            .map(|c| {
                let mut h = TopnHeap::new(bound);
                // reversed arrival inside each worker
                for &e in c.iter().rev() {
                    h.push(e);
                }
                h.into_sorted()
            })
            .collect();
        let winners = topn_merge(&sealed, bound);
        let got: Vec<u64> = winners.iter().map(|e| e.rowref()).collect();
        assert_eq!(got, (0..bound as u64).collect::<Vec<_>>());
    }

    /// All-null keys: one tier, rowref order; tier position honors
    /// nulls_first against a lone non-null row.
    #[test]
    fn null_tiers() {
        let nulls: Vec<TopnEntry> =
            (0..20).map(|i| TopnEntry::encode(0, true, false, false, i)).collect();
        let non_null = TopnEntry::encode(i64::MAX, false, false, false, 99);
        // NULLS LAST: the non-null MAX beats every null.
        assert!(nulls.iter().all(|&n| non_null < n));
        // NULLS FIRST flips it.
        let nulls_f: Vec<TopnEntry> =
            (0..20).map(|i| TopnEntry::encode(0, true, false, true, i)).collect();
        let non_null_f = TopnEntry::encode(i64::MIN, false, false, true, 99);
        assert!(nulls_f.iter().all(|&n| n < non_null_f));
        // Within the null tier: rowref ascending.
        let mut h = TopnHeap::new(5);
        for &e in nulls_f.iter().rev() {
            h.push(e);
        }
        let got: Vec<u64> = h.into_sorted().iter().map(|e| e.rowref()).collect();
        assert_eq!(got, vec![0, 1, 2, 3, 4]);
    }

    /// Keep-mask floor law: None while filling; once full, `admits` is
    /// exact (push of a non-admitted entry changes nothing; an admitted
    /// entry evicts the floor).
    #[test]
    fn floor_and_admits() {
        let mut h = TopnHeap::new(3);
        assert!(h.floor().is_none());
        for i in 0..3u64 {
            assert!(h.admits(TopnEntry::encode(10, false, false, false, i)));
            h.push(TopnEntry::encode(10, false, false, false, i));
        }
        let floor = h.floor().expect("full");
        assert_eq!(floor.rowref(), 2); // worst retained = (10, rowref 2)
        let rejected = TopnEntry::encode(10, false, false, false, 5);
        assert!(!h.admits(rejected));
        h.push(rejected);
        assert_eq!(h.floor().expect("full").rowref(), 2, "non-admitted push mutated the heap");
        let better = TopnEntry::encode(9, false, false, false, 9);
        assert!(h.admits(better));
        h.push(better);
        let got: Vec<u64> = h.into_sorted().iter().map(|e| e.rowref()).collect();
        assert_eq!(got, vec![9, 0, 1]); // (9,r9) then (10,r0), (10,r1)
    }

    /// Merge edges: empty runs, single run, unequal lengths, bound larger
    /// than the union, zero runs.
    #[test]
    fn merge_edges() {
        let e = |k: i64, r: u64| TopnEntry::encode(k, false, false, false, r);
        assert!(topn_merge::<TopnEntry>(&[], 10).is_empty());
        assert!(topn_merge::<TopnEntry>(&[vec![], vec![]], 10).is_empty());
        let single = vec![e(1, 0), e(2, 1)];
        assert_eq!(topn_merge(&[single.clone()], 10), single);
        let got = topn_merge(&[vec![e(5, 4)], vec![], vec![e(1, 0), e(9, 8)], vec![e(2, 1)]], 3);
        assert_eq!(got, vec![e(1, 0), e(2, 1), e(5, 4)]);
    }

    /// Wide-entry reference comparator over raw multi-key observations:
    /// lexicographic per-key (tier per that key's nulls_first, value per
    /// its asc/desc), then rowref.
    fn ref_cmp_wide(
        a: (&[(i64, bool)], u64),
        b: (&[(i64, bool)], u64),
        flags: &[(bool, bool)],
    ) -> std::cmp::Ordering {
        use std::cmp::Ordering::*;
        for (i, &(desc, nulls_first)) in flags.iter().enumerate() {
            let (ka, kb) = (a.0[i], b.0[i]);
            let tier = |null: bool| if null == nulls_first { 0 } else { 1 };
            match tier(ka.1).cmp(&tier(kb.1)) {
                Equal => {}
                o => return o,
            }
            if !ka.1 && !kb.1 {
                let k = if desc { kb.0.cmp(&ka.0) } else { ka.0.cmp(&kb.0) };
                if k != Equal {
                    return k;
                }
            }
        }
        a.1.cmp(&b.1)
    }

    /// Wide encoding law: exhaustive small grid over 2-key and 3-key
    /// shapes × per-key flag combinations.
    #[test]
    fn wide_encode_matches_reference() {
        let kvals: &[i64] = &[i64::MIN, -1, 0, 1, i64::MAX];
        let mut obs: Vec<(Vec<(i64, bool)>, u64)> = Vec::new();
        let mut r = 0u64;
        for &k0 in kvals {
            for n0 in [false, true] {
                for &k1 in &[-2i64, 0, 5] {
                    for n1 in [false, true] {
                        obs.push((vec![(k0, n0), (k1, n1)], r));
                        r += 1;
                    }
                }
            }
        }
        for f0 in [(false, false), (false, true), (true, false), (true, true)] {
            for f1 in [(false, false), (true, true)] {
                let flags = [f0, f1];
                for a in &obs {
                    for b in &obs {
                        if a == b {
                            continue;
                        }
                        let ea = WideEntry::encode(&a.0, &flags, a.1);
                        let eb = WideEntry::encode(&b.0, &flags, b.1);
                        if ea == eb {
                            // Only all-null-key pairs with equal rowrefs
                            // could collide; rowrefs are unique here.
                            panic!("distinct wide obs encoded equal: {a:?} {b:?}");
                        }
                        assert_eq!(
                            ea.cmp(&eb),
                            ref_cmp_wide((&a.0, a.1), (&b.0, b.1), &flags),
                            "flags={flags:?} a={a:?} b={b:?}"
                        );
                    }
                }
            }
        }
    }

    /// Wide claim-order independence + merge over dense key0 ties (key1
    /// resolves some, rowref the rest) — the multi-key rule-2 face.
    #[test]
    fn wide_merge_is_claim_order_independent() {
        let mut rng = Rng(0x71de);
        let flags = [(false, false), (true, false)];
        let obs: Vec<(Vec<(i64, bool)>, u64)> = (0..600)
            .map(|i| {
                let k0 = (rng.next() % 5) as i64; // dense: 5 distinct
                let null1 = rng.next() % 9 == 0;
                let k1 = (rng.next() % 40) as i64 - 20;
                (vec![(k0, false), (k1, null1)], i as u64)
            })
            .collect();
        let entries: Vec<WideEntry> =
            obs.iter().map(|(ks, r)| WideEntry::encode(ks, &flags, *r)).collect();
        for &bound in &[1usize, 7, 50, 599, 600] {
            let mut want = entries.clone();
            want.sort_unstable();
            want.truncate(bound);
            for trial in 0..6 {
                let workers = 1 + (trial % 4);
                let mut streams: Vec<Vec<WideEntry>> = vec![Vec::new(); workers];
                for &e in &entries {
                    streams[(rng.next() as usize) % workers].push(e);
                }
                for s in &mut streams {
                    for i in (1..s.len()).rev() {
                        s.swap(i, (rng.next() as usize) % (i + 1));
                    }
                }
                let sealed: Vec<Vec<WideEntry>> = streams
                    .into_iter()
                    .map(|s| {
                        let mut h = TopnWideHeap::new(bound);
                        for e in s {
                            h.push(e);
                        }
                        h.into_sorted()
                    })
                    .collect();
                assert_eq!(topn_merge(&sealed, bound), want, "bound={bound} trial={trial}");
            }
        }
    }

    #[test]
    fn cut64_prune_law_narrow() {
        // The GCUT safety pin: a.cut64() > b.cut64() must imply the full
        // entry order a > b, across keys × null × flags × rowrefs (strict
        // `>` only — equal truncated words never prune).
        let mut obs = Vec::new();
        for &k in KEY_SAMPLE {
            for null in [false, true] {
                for rowref in [0u64, 1, TOPN_MAX_ROWREF] {
                    obs.push((k, null, rowref));
                }
            }
        }
        for desc in [false, true] {
            for nf in [false, true] {
                for &a in &obs {
                    for &b in &obs {
                        let ea = TopnEntry::encode(a.0, a.1, desc, nf, a.2);
                        let eb = TopnEntry::encode(b.0, b.1, desc, nf, b.2);
                        if ea.cut64() > eb.cut64() {
                            assert!(ea > eb, "cut64 prune law: {a:?} vs {b:?} desc={desc} nf={nf}");
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn cut64_prune_law_wide() {
        // Leading-key cut64 dominance at arity 2 (lexicographic order).
        let flags = [(false, false), (true, false)];
        let mut es = Vec::new();
        let mut r = 0u64;
        for &k0 in &[i64::MIN, -1, 0, 1, i64::MAX] {
            for n0 in [false, true] {
                for &k1 in &[-5i64, 0, 5] {
                    es.push(WideEntry::encode(&[(k0, n0), (k1, false)], &flags, r));
                    r += 1;
                }
            }
        }
        for &a in &es {
            for &b in &es {
                if a.cut64() > b.cut64() {
                    assert!(a > b, "wide cut64 prune law: {a:?} vs {b:?}");
                }
            }
        }
    }

    #[test]
    fn key_order_word_matches_encode() {
        // The zone-metadata fold must be encode's own word fold.
        for &k in KEY_SAMPLE {
            for desc in [false, true] {
                let e = TopnEntry::encode(k, false, desc, false, 0);
                let word = (e.raw() >> 48) as u64;
                assert_eq!(word, key_order_word(k, desc), "k={k} desc={desc}");
            }
        }
    }

    /// key0 strictly dominates key1 (lexicographic law).
    #[test]
    fn wide_key0_dominates() {
        let flags = [(false, false), (false, false)];
        let small_k0 = WideEntry::encode(&[(1, false), (1000, false)], &flags, 9);
        let big_k0 = WideEntry::encode(&[(2, false), (-1000, false)], &flags, 1);
        assert!(small_k0 < big_k0);
    }

    #[test]
    #[should_panic(expected = "admission envelope")]
    fn bound_zero_refused() {
        let _ = TopnHeap::new(0);
    }

    #[test]
    #[should_panic(expected = "admission envelope")]
    fn bound_over_cap_refused() {
        let _ = TopnHeap::new(TOPN_MAX_BOUND + 1);
    }
}
