// M3 sort — shape (b) FULL-SORT kernels (docs/design/m3-sort.md §5,
// m3-sort-b car 2).
//
// Per-worker sorted runs + splitter-sliced partition-parallel merge:
//
//   ACCEPT  workers append survivors as SELF-CONTAINED rows into the
//           Local's `RunBuf` (datums + byref arena; budget-metered by the
//           engagement against work_mem per participant) and stamp one
//           `RunEnt` per row: the packed (sort keys, GLOBAL rowref) image
//           (`WideEntry` — DESC/NULLS folded at encode) + the row's buf
//           index.
//   SEAL    sort each Local's entries under the entry total order
//           ((keys, rowref) — rowrefs unique across the scan, so the
//           order is TOTAL and the run order is a pure function of the
//           data) + resolve the buf's byref datums (arena is final).
//   SPLIT   sample every run, pick P−1 splitter entries (`fullsort_
//           splitters`). Splitters affect BALANCE, never content:
//           partition p owns the half-open entry range [s_{p−1}, s_p)
//           under the total order, every row falls in exactly one
//           partition, and the concatenation of partition outputs in
//           partition order IS the k-way merge of all runs — independent
//           of splitter choice (the Leis separator argument;
//           property-tested below).
//   COMBINE partition p binary-searches every sorted run for its slice
//           (total order ⇒ unique cut points) and k-way merges the W
//           slices into `Vec<(run, bufrow)>` — parallel across P.
//   EMIT    the leader streams partitions in index order, indexing rows
//           straight out of the sealed RunBufs (no re-copy; the bufs ride
//           the published payload).
//
// Everything here is PURE plain-Rust data (no executor, no Mcx): `Send` by
// construction, unit-testable exhaustively. Spill posture: NONE — phase 1
// refuses sorts that cannot complete in memory (admission estimate +
// runtime budget crossing ⇒ serial rerun; the serial arm spills through
// the ported external sort correctly). The m35-spill lane owns run spill.

use crate::sink::WideEntry;
use ::datum::Datum;

/// Output column metadata for the self-contained row copy (from the outer
/// desc at admission): `byval` copies the datum word; byref copies `len`
/// bytes (`len == -1` = varlena, size read from the header). cstring
/// (`len == -2`) never admits.
#[derive(Clone, Copy)]
pub struct RunCol {
    pub byval: bool,
    pub len: i16,
}

/// One run entry: the packed sort-key image (global-rowref tiebreak —
/// the run order's total-order law) + the row's index in the run's
/// `RunBuf`. Derived Ord: key first; bufrow is payload (keys can only
/// compare equal if the whole (keys, rowref) image ties, which unique
/// rowrefs exclude).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct RunEnt {
    pub key: WideEntry,
    pub bufrow: u32,
}

/// A self-contained row buffer: `natts` datums + null flags per row,
/// byref payloads copied into the buf's own arena. During ACCEPT byref
/// cells hold ARENA OFFSETS (the arena may reallocate as it grows);
/// `seal_fixup` resolves them to absolute pointers once the arena is
/// final — nothing worker-owned survives in the published buf (the
/// SinkEmitBuf discipline).
#[derive(Default)]
pub struct RunBuf {
    pub natts: usize,
    pub values: Vec<Datum>,
    pub nulls: Vec<bool>,
    pub arena: Vec<u8>,
    pub nrows: usize,
    fixed: bool,
}

impl RunBuf {
    pub fn new(natts: usize) -> RunBuf {
        RunBuf { natts, ..RunBuf::default() }
    }

    /// Heap bytes retained (the budget meter's read).
    pub fn bytes(&self) -> usize {
        self.values.capacity() * core::mem::size_of::<Datum>()
            + self.nulls.capacity()
            + self.arena.capacity()
    }

    /// Append one row (datums in outer-desc order). Byref cells copy
    /// their image into the arena; the datum cell holds the arena offset
    /// until [`RunBuf::seal_fixup`].
    ///
    /// # Safety
    /// Byref non-null datums must point at live, fully-detoasted images
    /// whose size law matches `cols` (varlena header at `len == -1`,
    /// exactly `len` bytes otherwise) — the accept drain's contract.
    pub unsafe fn push_row(&mut self, vals: &[Datum], nulls: &[bool], cols: &[RunCol]) {
        debug_assert_eq!(vals.len(), self.natts);
        debug_assert!(!self.fixed, "push after seal_fixup");
        for (c, (&v, &isnull)) in cols.iter().zip(vals.iter().zip(nulls)).take(self.natts) {
            if isnull {
                self.values.push(Datum::null());
                self.nulls.push(true);
                continue;
            }
            self.nulls.push(false);
            if c.byval {
                self.values.push(v);
                continue;
            }
            // Byref: copy the image, 8-aligned (varlena consumers read
            // 4-byte headers + aligned payloads; fixed-len byref types
            // (interval, name) keep their natural alignment at 8).
            let p = v.as_usize() as *const u8;
            let size = if c.len == -1 {
                // SAFETY: caller contract — live varlena image.
                unsafe { ::types_tuple::varatt::varsize_any(p) }
            } else {
                debug_assert!(c.len > 0, "cstring columns never admit");
                c.len as usize
            };
            let pad = (8 - self.arena.len() % 8) % 8;
            self.arena.resize(self.arena.len() + pad, 0);
            let off = self.arena.len();
            // SAFETY: caller contract — `size` readable bytes at `p`.
            self.arena.extend_from_slice(unsafe { core::slice::from_raw_parts(p, size) });
            self.values.push(Datum::from_usize(off));
        }
        self.nrows += 1;
    }

    /// Resolve byref offset cells to absolute pointers (arena final —
    /// called at SEAL, after which the buf never grows or moves its
    /// arena heap buffer).
    pub fn seal_fixup(&mut self, cols: &[RunCol]) {
        debug_assert!(!self.fixed);
        self.fixed = true;
        let base = self.arena.as_ptr() as usize;
        for row in 0..self.nrows {
            for (a, c) in cols.iter().enumerate().take(self.natts) {
                if c.byval {
                    continue;
                }
                let i = row * self.natts + a;
                if !self.nulls[i] {
                    self.values[i] = Datum::from_usize(base + self.values[i].as_usize());
                }
            }
        }
    }

    /// Row `i`'s datums + null flags (post-fixup: byref datums are
    /// absolute pointers into this buf's arena).
    #[inline]
    pub fn row(&self, i: usize) -> (&[Datum], &[bool]) {
        debug_assert!(self.fixed, "row read before seal_fixup");
        let base = i * self.natts;
        (&self.values[base..base + self.natts], &self.nulls[base..base + self.natts])
    }
}

/// One sealed per-worker run: entries sorted under the total order, the
/// row buffer fixed up (byref datums absolute). Published behind `Arc` —
/// finalize clones pointers (O(W)), never row data.
pub struct FullRun {
    pub entries: Vec<RunEnt>,
    pub buf: RunBuf,
}

/// The leader's adopted full-sort result: sealed runs + per-partition
/// merged `(run, bufrow)` outputs in partition order. Concatenation in
/// partition order IS the canonical (keys, rowref) sort. Iterated by the
/// Sort node's runtime emit face; dropped at reset/rescan/end.
pub struct FullAdopted {
    pub runs: Vec<std::sync::Arc<FullRun>>,
    pub parts: Vec<Vec<(u16, u32)>>,
    part: usize,
    pos: usize,
}

impl FullAdopted {
    pub fn new(runs: Vec<std::sync::Arc<FullRun>>, parts: Vec<Vec<(u16, u32)>>) -> FullAdopted {
        FullAdopted { runs, parts, part: 0, pos: 0 }
    }

    /// Next row in the canonical order; `None` = drained.
    #[inline]
    pub fn next_row(&mut self) -> Option<(&[Datum], &[bool])> {
        loop {
            let p = self.parts.get(self.part)?;
            match p.get(self.pos) {
                Some(&(run, bufrow)) => {
                    self.pos += 1;
                    return Some(self.runs[run as usize].buf.row(bufrow as usize));
                }
                None => {
                    self.part += 1;
                    self.pos = 0;
                }
            }
        }
    }

    pub fn total_rows(&self) -> usize {
        self.parts.iter().map(Vec::len).sum()
    }
}

/// Pick `nparts − 1` splitter entries from the sealed (sorted) runs:
/// sample ≈`SPLIT_SAMPLES_PER_RUN` evenly spaced entries per run, merge,
/// take evenly spaced sample ranks. Pure balance heuristic — content
/// independence from the choice is the partition law (tested).
pub fn fullsort_splitters(runs: &[&[RunEnt]], nparts: usize) -> Vec<WideEntry> {
    const SPLIT_SAMPLES_PER_RUN: usize = 4096;
    debug_assert!(nparts >= 1);
    let mut samples: Vec<WideEntry> = Vec::new();
    for r in runs {
        if r.is_empty() {
            continue;
        }
        let step = r.len().div_ceil(SPLIT_SAMPLES_PER_RUN).max(1);
        samples.extend(r.iter().step_by(step).map(|e| e.key));
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

/// One partition's k-way merge: slice every sorted run to the half-open
/// entry range [`splitters[part-1]`, `splitters[part]`) (ends open at the
/// edges) by binary search — the total order makes cut points unique —
/// then merge the slices. Output rows in the global total order, as
/// `(run index, bufrow)` pairs into the runs' RunBufs.
pub fn fullsort_partition_merge(
    runs: &[&[RunEnt]],
    splitters: &[WideEntry],
    part: usize,
) -> Vec<(u16, u32)> {
    // A short splitter list (empty input ⇒ zero splitters) leaves the
    // trailing partitions empty; partition 0 owns everything then.
    if part > splitters.len() {
        return Vec::new();
    }
    let lo = part.checked_sub(1).map(|i| splitters[i]);
    let hi = splitters.get(part).copied();
    // Slice bounds per run.
    let mut slices: Vec<&[RunEnt]> = Vec::with_capacity(runs.len());
    let mut total = 0usize;
    for r in runs {
        let a = match lo {
            Some(s) => r.partition_point(|e| e.key < s),
            None => 0,
        };
        let b = match hi {
            Some(s) => r.partition_point(|e| e.key < s),
            None => r.len(),
        };
        let sl = &r[a..b];
        total += sl.len();
        slices.push(sl);
    }
    let mut out = Vec::with_capacity(total);
    // K-way heap merge over the slices (min-heap via Reverse on the
    // entry; unique (keys, rowref) images make the order total).
    use std::cmp::Reverse;
    let mut heads: std::collections::BinaryHeap<Reverse<(WideEntry, usize)>> =
        std::collections::BinaryHeap::with_capacity(slices.len());
    let mut cursor = vec![0usize; slices.len()];
    for (ri, sl) in slices.iter().enumerate() {
        if let Some(e) = sl.first() {
            heads.push(Reverse((e.key, ri)));
        }
    }
    while let Some(Reverse((_, ri))) = heads.pop() {
        let e = slices[ri][cursor[ri]];
        out.push((ri as u16, e.bufrow));
        cursor[ri] += 1;
        if let Some(next) = slices[ri].get(cursor[ri]) {
            heads.push(Reverse((next.key, ri)));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ent(keys: &[(i64, bool)], flags: &[(bool, bool)], rowref: u64, bufrow: u32) -> RunEnt {
        RunEnt { key: WideEntry::encode(keys, flags, rowref), bufrow }
    }

    /// Deterministic pseudo-random (no dev-deps in this crate).
    fn rng(seed: &mut u64) -> u64 {
        *seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        *seed >> 11
    }

    /// Reference: globally sort all entries under the total order.
    fn global_reference(runs: &[Vec<RunEnt>]) -> Vec<(u16, u32)> {
        let mut all: Vec<(WideEntry, u16, u32)> = Vec::new();
        for (ri, r) in runs.iter().enumerate() {
            for e in r {
                all.push((e.key, ri as u16, e.bufrow));
            }
        }
        all.sort_unstable();
        all.into_iter().map(|(_, ri, br)| (ri, br)).collect()
    }

    fn concat_partitions(
        runs: &[Vec<RunEnt>],
        splitters: &[WideEntry],
        nparts: usize,
    ) -> Vec<(u16, u32)> {
        let views: Vec<&[RunEnt]> = runs.iter().map(|r| r.as_slice()).collect();
        let mut out = Vec::new();
        for p in 0..nparts {
            out.extend(fullsort_partition_merge(&views, splitters, p));
        }
        out
    }

    /// Random runs (dense key ties across runs, unique rowrefs), several
    /// arities/directions: concatenated partitions == the global sort,
    /// for BOTH sampled splitters and arbitrary (adversarial) splitter
    /// choices — content is splitter-independent.
    #[test]
    fn partitions_concatenate_to_the_global_sort() {
        let mut seed = 42u64;
        for &(arity, nruns, nparts) in
            &[(1usize, 1usize, 1usize), (1, 4, 8), (2, 3, 16), (4, 5, 7), (1, 6, 256)]
        {
            let flags: Vec<(bool, bool)> =
                (0..arity).map(|i| (i % 2 == 1, i % 3 == 0)).collect();
            let mut rowref = 0u64;
            let runs: Vec<Vec<RunEnt>> = (0..nruns)
                .map(|_| {
                    let n = (rng(&mut seed) % 300) as usize;
                    let mut v: Vec<RunEnt> = (0..n)
                        .map(|i| {
                            let keys: Vec<(i64, bool)> = (0..arity)
                                .map(|_| {
                                    let null = rng(&mut seed) % 13 == 0;
                                    ((rng(&mut seed) % 7) as i64 - 3, null)
                                })
                                .collect();
                            rowref += 1 + rng(&mut seed) % 5;
                            ent(&keys, &flags, rowref, i as u32)
                        })
                        .collect();
                    v.sort_unstable();
                    v
                })
                .collect();
            let reference = global_reference(&runs);
            let views: Vec<&[RunEnt]> = runs.iter().map(|r| r.as_slice()).collect();

            // Sampled splitters.
            let sampled = fullsort_splitters(&views, nparts);
            assert_eq!(sampled.len().min(nparts - 1), sampled.len());
            assert!(sampled.windows(2).all(|w| w[0] <= w[1]));
            assert_eq!(
                concat_partitions(&runs, &sampled, nparts),
                reference,
                "sampled splitters, arity={arity} runs={nruns} parts={nparts}"
            );

            // Adversarial splitters: arbitrary sorted entry values,
            // including duplicates and out-of-range extremes.
            let mut adversarial: Vec<WideEntry> = (0..nparts.saturating_sub(1))
                .map(|_| {
                    let keys: Vec<(i64, bool)> = (0..arity)
                        .map(|_| ((rng(&mut seed) % 9) as i64 - 4, rng(&mut seed) % 11 == 0))
                        .collect();
                    WideEntry::encode(&keys, &flags, rng(&mut seed) % 1000)
                })
                .collect();
            adversarial.sort_unstable();
            assert_eq!(
                concat_partitions(&runs, &adversarial, nparts),
                reference,
                "adversarial splitters, arity={arity} runs={nruns} parts={nparts}"
            );
        }
    }

    #[test]
    fn empty_and_degenerate_runs() {
        let views: Vec<&[RunEnt]> = vec![&[], &[]];
        assert!(fullsort_splitters(&views, 8).is_empty());
        let sp = fullsort_splitters(&views, 8);
        // Zero splitters + a full 8-partition claim space: every partition
        // (including the out-of-range tail) is empty, no panic.
        for p in 0..8 {
            assert!(fullsort_partition_merge(&views, &sp, p).is_empty());
        }
        // All-identical keys (rowref-only order).
        let flags = [(false, false)];
        let runs: Vec<Vec<RunEnt>> = (0..3)
            .map(|ri| {
                (0..50)
                    .map(|i| ent(&[(7, false)], &flags, (ri * 100 + i) as u64, i as u32))
                    .collect()
            })
            .collect();
        let views: Vec<&[RunEnt]> = runs.iter().map(|r| r.as_slice()).collect();
        let sp = fullsort_splitters(&views, 16);
        assert_eq!(concat_partitions(&runs, &sp, 16), global_reference(&runs));
    }

    #[test]
    fn runbuf_roundtrip_byval_and_varlena() {
        // Two cols: int8 byval + a varlena (short text image built by
        // hand: 4-byte header, 4B length law of varsize_any on 4b).
        let cols =
            [RunCol { byval: true, len: 8 }, RunCol { byval: false, len: -1 }];
        let mut buf = RunBuf::new(2);
        let mut images: Vec<Vec<u8>> = Vec::new();
        for i in 0..40i64 {
            let body = format!("row-{i}-{}", "x".repeat((i as usize) % 17));
            let total = 4 + body.len();
            let mut img = Vec::with_capacity(total);
            img.extend_from_slice(&(((total as u32) << 2)).to_le_bytes());
            img.extend_from_slice(body.as_bytes());
            images.push(img);
        }
        for (i, img) in images.iter().enumerate() {
            let vals =
                [Datum::from_i64(i as i64 * 3), Datum::from_usize(img.as_ptr() as usize)];
            let nulls = [false, i % 7 == 0];
            // SAFETY: img is a live 4b varlena image.
            unsafe { buf.push_row(&vals, &nulls, &cols) };
        }
        assert_eq!(buf.nrows, 40);
        assert!(buf.bytes() > 0);
        buf.seal_fixup(&cols);
        for i in 0..40usize {
            let (vals, nulls) = buf.row(i);
            assert_eq!(vals[0].as_i64(), i as i64 * 3);
            assert_eq!(nulls[1], i % 7 == 0);
            if !nulls[1] {
                let p = vals[1].as_usize() as *const u8;
                // SAFETY: fixed-up datum points into buf.arena.
                let sz = unsafe { ::types_tuple::varatt::varsize_any(p) };
                assert_eq!(sz, images[i].len());
                let got = unsafe { core::slice::from_raw_parts(p, sz) };
                assert_eq!(got, images[i].as_slice(), "row {i} image");
                // 8-aligned arena placement.
                assert_eq!((p as usize) % 8, 0);
            }
        }
    }
}
