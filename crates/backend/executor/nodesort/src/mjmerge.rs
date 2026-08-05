// MJSORT — range-partitioned merge-join pair kernels over two full-sort
// run sets (the "merge join after sort" tier-2 car; rides the shape-(b)
// full-sort output of fullsort.rs).
//
// Inputs: the OUTER and INNER sides' sealed runs (each `FullRun.entries`
// sorted under the (keys, rowref) total order, both sides' key columns
// encoded under IDENTICAL per-column (desc, nulls_first) flags — the
// arm's cross-side admission law, so equal key VALUES have equal packed
// words and both sides run the same direction).
//
//   SPLIT   sample both sides' runs on the key-PREFIX space (the packed
//           key words WITHOUT the rowref tiebreak) and pick `nparts − 1`
//           prefix boundaries. Boundaries affect BALANCE only, never
//           content: every equal-key group falls entirely inside exactly
//           one partition on BOTH sides (a boundary is a prefix value and
//           the slicing predicate is prefix-strict), so partitions join
//           independently — the range-partitioned merge (the Leis
//           separator argument, extended to two aligned run sets).
//   MERGE   partition p slices every run of both sides to the half-open
//           prefix range [s_{p−1}, s_p) by binary search, then walks the
//           two sides' equal-prefix GROUPS in prefix order: matching
//           groups emit their cross product OUTER-MAJOR (for each outer
//           entry in (keys, rowref) order, every inner entry in (keys,
//           rowref) order) — the serial merge-join emit cadence over
//           canonically-ordered children. NULL-keyed groups never match
//           (SQL equality is strict); rows whose any key is NULL are
//           skipped wholesale.
//   OUTPUT  per-partition `(orun, obufrow, irun, ibufrow)` pair refs into
//           the two sides' RunBufs; concatenation in partition order is
//           the whole join in the global outer-key order.
//
// Everything here is PURE plain-Rust data (no executor, no Mcx): `Send`
// by construction, unit-testable exhaustively — the fullsort.rs
// discipline. The pair budget is the caller's (a shared atomic counter;
// crossing it aborts the engagement into the serial rerun, R5).

use crate::fullsort::RunEnt;
use crate::sink::TOPN_MAX_KEYS;

/// One joined pair: (outer run, outer bufrow, inner run, inner bufrow).
pub type MjPair = (u16, u32, u16, u32);

/// A key-prefix boundary: the packed per-key words with unused slots zero
/// (comparable full-array; see `WideEntry::key_words`).
pub type MjPrefix = [u128; TOPN_MAX_KEYS];

#[inline]
fn prefix(e: &RunEnt) -> MjPrefix {
    e.key.key_words()
}

/// Pick `nparts − 1` prefix boundaries from BOTH sides' sealed runs:
/// sample evenly per run, merge, take evenly spaced ranks — the
/// `fullsort_splitters` recipe on the prefix space. Pure balance
/// heuristic; content is boundary-independent (tested).
pub fn mj_splitters(oruns: &[&[RunEnt]], iruns: &[&[RunEnt]], nparts: usize) -> Vec<MjPrefix> {
    const SAMPLES_PER_RUN: usize = 4096;
    debug_assert!(nparts >= 1);
    let mut samples: Vec<MjPrefix> = Vec::new();
    for r in oruns.iter().chain(iruns) {
        if r.is_empty() {
            continue;
        }
        let step = r.len().div_ceil(SAMPLES_PER_RUN).max(1);
        samples.extend(r.iter().step_by(step).map(prefix));
    }
    samples.sort_unstable();
    let mut out = Vec::with_capacity(nparts.saturating_sub(1));
    if samples.is_empty() {
        return out;
    }
    for p in 1..nparts {
        let idx = (p * samples.len()) / nparts;
        out.push(samples[idx.min(samples.len() - 1)]);
    }
    out
}

/// One side's slice set for a partition: every run cut to
/// [`splitters[part-1]`, `splitters[part]`) on the PREFIX order (ends
/// open at the edges; cut points are unique per run because runs are
/// sorted and the predicate is monotone).
fn slice_side<'a>(
    runs: &[&'a [RunEnt]],
    splitters: &[MjPrefix],
    part: usize,
) -> Vec<&'a [RunEnt]> {
    let lo = part.checked_sub(1).map(|i| splitters[i]);
    let hi = splitters.get(part).copied();
    runs.iter()
        .map(|r| {
            let a = match lo {
                Some(s) => r.partition_point(|e| prefix(e) < s),
                None => 0,
            };
            let b = match hi {
                Some(s) => r.partition_point(|e| prefix(e) < s),
                None => r.len(),
            };
            &r[a..b]
        })
        .collect()
}

/// Cursor over one side's slices yielding equal-prefix GROUPS in prefix
/// order, each group's entries in the full (keys, rowref) total order.
struct GroupCursor<'a> {
    slices: Vec<&'a [RunEnt]>,
    /// Per-slice next-unconsumed index.
    pos: Vec<usize>,
}

impl<'a> GroupCursor<'a> {
    fn new(slices: Vec<&'a [RunEnt]>) -> GroupCursor<'a> {
        let pos = vec![0; slices.len()];
        GroupCursor { slices, pos }
    }

    /// The minimum head prefix across slices; `None` = side exhausted.
    fn peek(&self) -> Option<MjPrefix> {
        let mut min: Option<MjPrefix> = None;
        for (sl, &p) in self.slices.iter().zip(&self.pos) {
            if let Some(e) = sl.get(p) {
                let pf = prefix(e);
                if min.is_none_or(|m| pf < m) {
                    min = Some(pf);
                }
            }
        }
        min
    }

    /// Skip the whole group with prefix `pf` (heads only — callers pass
    /// the current [`GroupCursor::peek`] value).
    fn skip(&mut self, pf: MjPrefix) {
        for (sl, p) in self.slices.iter().zip(&mut self.pos) {
            *p += sl[*p..].partition_point(|e| prefix(e) == pf);
        }
    }

    /// Collect the whole group with prefix `pf` into `buf` — (run index,
    /// entry) pairs in the full (keys, rowref) total order. Within one
    /// slice the group is a contiguous sorted span; the cross-slice merge
    /// is a collect + unstable sort on the entry key (rowrefs unique ⇒
    /// total, so the result order is a pure function of the data).
    fn collect(&mut self, pf: MjPrefix, buf: &mut Vec<(u16, RunEnt)>) {
        buf.clear();
        for (ri, (sl, p)) in self.slices.iter().zip(&mut self.pos).enumerate() {
            let n = sl[*p..].partition_point(|e| prefix(e) == pf);
            buf.extend(sl[*p..*p + n].iter().map(|&e| (ri as u16, e)));
            *p += n;
        }
        buf.sort_unstable_by_key(|&(_, e)| e.key);
    }
}

/// Pair budget: shared across partitions/workers (a plain atomic the
/// caller sizes); crossing it stops the merge — the caller aborts the
/// engagement into the serial rerun (nothing was emitted).
pub struct PairBudget {
    pub emitted: std::sync::atomic::AtomicU64,
    pub cap: u64,
}

impl PairBudget {
    pub fn new(cap: u64) -> PairBudget {
        PairBudget { emitted: std::sync::atomic::AtomicU64::new(0), cap }
    }

    /// Reserve `n` pairs; `false` = budget crossed (sticky — the total
    /// stays over cap so every subsequent claim also refuses).
    #[inline]
    fn reserve(&self, n: u64) -> bool {
        self.emitted.fetch_add(n, std::sync::atomic::Ordering::Relaxed) + n <= self.cap
    }
}

/// Merge one partition's slices of both sides into joined pair refs.
/// `nulls_first` = the per-key encode flags (both sides identical — the
/// admission law); a group whose any key word decodes NULL is skipped on
/// both sides (strict SQL equality). Returns `false` when the pair budget
/// was crossed (output is then meaningless; the caller aborts).
pub fn mj_partition_pairs(
    oruns: &[&[RunEnt]],
    iruns: &[&[RunEnt]],
    splitters: &[MjPrefix],
    part: usize,
    nkeys: usize,
    nulls_first: &[bool],
    budget: &PairBudget,
    out: &mut Vec<MjPair>,
) -> bool {
    debug_assert!(nkeys >= 1 && nulls_first.len() >= nkeys);
    // A short splitter list (empty inputs ⇒ zero splitters) leaves the
    // trailing partitions empty; partition 0 owns everything then.
    if part > splitters.len() {
        return true;
    }
    let mut oc = GroupCursor::new(slice_side(oruns, splitters, part));
    let mut ic = GroupCursor::new(slice_side(iruns, splitters, part));
    let mut og: Vec<(u16, RunEnt)> = Vec::new();
    let mut ig: Vec<(u16, RunEnt)> = Vec::new();
    let null_group = |pf: &MjPrefix| {
        (0..nkeys).any(|k| (((pf[k] >> 64) & 1) != 0) ^ nulls_first[k])
    };
    loop {
        let (Some(opf), Some(ipf)) = (oc.peek(), ic.peek()) else {
            return true; // either side exhausted — no more matches
        };
        match opf.cmp(&ipf) {
            core::cmp::Ordering::Less => oc.skip(opf),
            core::cmp::Ordering::Greater => ic.skip(ipf),
            core::cmp::Ordering::Equal => {
                if null_group(&opf) {
                    // SQL strict equality: NULL keys never join.
                    oc.skip(opf);
                    ic.skip(ipf);
                    continue;
                }
                oc.collect(opf, &mut og);
                ic.collect(ipf, &mut ig);
                if !budget.reserve(og.len() as u64 * ig.len() as u64) {
                    return false;
                }
                out.reserve(og.len() * ig.len());
                for &(orun, oe) in &og {
                    for &(irun, ie) in &ig {
                        out.push((orun, oe.bufrow, irun, ie.bufrow));
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sink::WideEntry;

    fn ent(keys: &[(i64, bool)], flags: &[(bool, bool)], rowref: u64, bufrow: u32) -> RunEnt {
        RunEnt { key: WideEntry::encode(keys, flags, rowref), bufrow }
    }

    /// Deterministic pseudo-random (no dev-deps in this crate).
    fn rng(seed: &mut u64) -> u64 {
        *seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        *seed >> 11
    }

    /// Reference: nested-loop join of all entries under strict non-NULL
    /// key equality, outer-major in the (keys, rowref) total order.
    fn reference(
        oruns: &[Vec<RunEnt>],
        iruns: &[Vec<RunEnt>],
        keyvals: &dyn Fn(&RunEnt) -> Vec<Option<i64>>,
    ) -> Vec<MjPair> {
        let mut oall: Vec<(u16, RunEnt)> = Vec::new();
        for (ri, r) in oruns.iter().enumerate() {
            oall.extend(r.iter().map(|&e| (ri as u16, e)));
        }
        oall.sort_unstable_by_key(|&(_, e)| e.key);
        let mut iall: Vec<(u16, RunEnt)> = Vec::new();
        for (ri, r) in iruns.iter().enumerate() {
            iall.extend(r.iter().map(|&e| (ri as u16, e)));
        }
        iall.sort_unstable_by_key(|&(_, e)| e.key);
        let mut out = Vec::new();
        for &(orun, oe) in &oall {
            let ok = keyvals(&oe);
            if ok.iter().any(Option::is_none) {
                continue;
            }
            for &(irun, ie) in &iall {
                let ik = keyvals(&ie);
                if ik.iter().any(Option::is_none) {
                    continue;
                }
                if ok == ik {
                    out.push((orun, oe.bufrow, irun, ie.bufrow));
                }
            }
        }
        out
    }

    fn concat_partitions(
        oruns: &[Vec<RunEnt>],
        iruns: &[Vec<RunEnt>],
        splitters: &[MjPrefix],
        nparts: usize,
        nkeys: usize,
        nulls_first: &[bool],
        cap: u64,
    ) -> Option<Vec<MjPair>> {
        let ov: Vec<&[RunEnt]> = oruns.iter().map(|r| r.as_slice()).collect();
        let iv: Vec<&[RunEnt]> = iruns.iter().map(|r| r.as_slice()).collect();
        let budget = PairBudget::new(cap);
        let mut out = Vec::new();
        for p in 0..nparts {
            let mut part_out = Vec::new();
            if !mj_partition_pairs(
                &ov, &iv, splitters, p, nkeys, nulls_first, &budget, &mut part_out,
            ) {
                return None;
            }
            out.extend(part_out);
        }
        Some(out)
    }

    /// Random dup-heavy runs on both sides (nulls included), several
    /// arities/directions: concatenated partition pairs == the
    /// nested-loop reference, for sampled AND adversarial boundaries.
    #[test]
    fn partitions_concatenate_to_the_reference_join() {
        let mut seed = 7u64;
        for &(arity, no, ni, nparts) in
            &[(1usize, 1usize, 1usize, 1usize), (1, 3, 2, 8), (2, 4, 3, 16), (3, 2, 5, 7)]
        {
            let flags: Vec<(bool, bool)> =
                (0..arity).map(|i| (i % 2 == 1, i % 3 == 0)).collect();
            let nulls_first: Vec<bool> = flags.iter().map(|&(_, nf)| nf).collect();
            let mut rowref = 0u64;
            let mut mk = |nruns: usize, seed: &mut u64| -> Vec<Vec<RunEnt>> {
                (0..nruns)
                    .map(|_| {
                        let n = (rng(seed) % 200) as usize;
                        let mut v: Vec<RunEnt> = (0..n)
                            .map(|i| {
                                let keys: Vec<(i64, bool)> = (0..arity)
                                    .map(|_| {
                                        let null = rng(seed) % 11 == 0;
                                        ((rng(seed) % 5) as i64 - 2, null)
                                    })
                                    .collect();
                                rowref += 1 + rng(seed) % 3;
                                ent(&keys, &flags, rowref, i as u32)
                            })
                            .collect();
                        v.sort_unstable();
                        v
                    })
                    .collect()
            };
            let oruns = mk(no, &mut seed);
            let iruns = mk(ni, &mut seed);
            // Decode the (value, null) tuple per key back from the packed
            // words for the reference (inverse of key_word128).
            let flags2 = flags.clone();
            let keyvals = move |e: &RunEnt| -> Vec<Option<i64>> {
                let w = e.key.key_words();
                (0..arity)
                    .map(|k| {
                        let (desc, nf) = flags2[k];
                        if e.key.key_is_null(k, nf) {
                            return None;
                        }
                        let word = w[k] as u64;
                        let asc = if desc { !word } else { word };
                        Some((asc ^ (1 << 63)) as i64)
                    })
                    .collect()
            };
            let reference = reference(&oruns, &iruns, &keyvals);

            let ov: Vec<&[RunEnt]> = oruns.iter().map(|r| r.as_slice()).collect();
            let iv: Vec<&[RunEnt]> = iruns.iter().map(|r| r.as_slice()).collect();
            let sampled = mj_splitters(&ov, &iv, nparts);
            assert!(sampled.windows(2).all(|w| w[0] <= w[1]));
            assert_eq!(
                concat_partitions(
                    &oruns, &iruns, &sampled, nparts, arity, &nulls_first, u64::MAX
                )
                .expect("budget unbounded"),
                reference,
                "sampled boundaries, arity={arity} o={no} i={ni} parts={nparts}"
            );

            // Adversarial boundaries: arbitrary sorted prefixes including
            // duplicates and extremes — content must not move.
            let mut adversarial: Vec<MjPrefix> = (0..nparts.saturating_sub(1))
                .map(|_| {
                    let keys: Vec<(i64, bool)> = (0..arity)
                        .map(|_| ((rng(&mut seed) % 7) as i64 - 3, rng(&mut seed) % 9 == 0))
                        .collect();
                    WideEntry::encode(&keys, &flags, 0).key_words()
                })
                .collect();
            adversarial.sort_unstable();
            assert_eq!(
                concat_partitions(
                    &oruns, &iruns, &adversarial, nparts, arity, &nulls_first, u64::MAX
                )
                .expect("budget unbounded"),
                reference,
                "adversarial boundaries, arity={arity} o={no} i={ni} parts={nparts}"
            );
        }
    }

    /// The pair budget stops the merge once total emitted pairs cross the
    /// cap, from any partition.
    #[test]
    fn pair_budget_crossing_stops() {
        let flags = [(false, false)];
        // 40 outer × 40 inner of one key value = 1600 pairs.
        let oruns: Vec<Vec<RunEnt>> =
            vec![(0..40).map(|i| ent(&[(1, false)], &flags, i as u64, i as u32)).collect()];
        let iruns: Vec<Vec<RunEnt>> = vec![(0..40)
            .map(|i| ent(&[(1, false)], &flags, 100 + i as u64, i as u32))
            .collect()];
        assert!(
            concat_partitions(&oruns, &iruns, &[], 1, 1, &[false], 1599).is_none(),
            "1600 pairs must cross a 1599 cap"
        );
        assert_eq!(
            concat_partitions(&oruns, &iruns, &[], 1, 1, &[false], 1600)
                .expect("exactly at cap")
                .len(),
            1600
        );
    }

    /// NULL keys never join, at every arity position, on either side.
    #[test]
    fn null_keys_never_join() {
        let flags = [(false, false), (true, true)];
        let nf = [false, true];
        let oruns: Vec<Vec<RunEnt>> = vec![{
            let mut v = vec![
                ent(&[(1, false), (2, false)], &flags, 1, 0),
                ent(&[(1, false), (2, true)], &flags, 2, 1), // null 2nd key
                ent(&[(3, true), (2, false)], &flags, 3, 2), // null 1st key
            ];
            v.sort_unstable();
            v
        }];
        let iruns: Vec<Vec<RunEnt>> = vec![{
            let mut v = vec![
                ent(&[(1, false), (2, false)], &flags, 10, 0),
                ent(&[(1, false), (2, true)], &flags, 11, 1),
                ent(&[(3, true), (2, false)], &flags, 12, 2),
            ];
            v.sort_unstable();
            v
        }];
        let got =
            concat_partitions(&oruns, &iruns, &[], 1, 2, &nf, u64::MAX).expect("no budget");
        // Only the fully-non-NULL (1,2) rows join: exactly one pair.
        assert_eq!(got, vec![(0u16, 0u32, 0u16, 0u32)]);
    }

    /// Empty sides and empty partitions are clean no-ops.
    #[test]
    fn empty_and_degenerate() {
        let ov: Vec<&[RunEnt]> = vec![&[]];
        let iv: Vec<&[RunEnt]> = vec![&[]];
        assert!(mj_splitters(&ov, &iv, 8).is_empty());
        let budget = PairBudget::new(0);
        let mut out = Vec::new();
        for p in 0..8 {
            assert!(mj_partition_pairs(&ov, &iv, &[], p, 1, &[false], &budget, &mut out));
        }
        assert!(out.is_empty());
    }
}
