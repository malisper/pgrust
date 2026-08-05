// C divergence (structural lever): LSD radix partition over abbreviated
// datum1 words replaces the level-0 comparison sort; equal-word groups are
// sorted with the authoritative comparator. Byte-exact by mksort's certificate
// discipline: comparator-equal keys have equal abbrevs, so every
// full-comparator-equal pair ends adjacent inside one group, is detected
// there, and forces fallback with `tuples` untouched (all radix work happens
// in scratch). Unique full keys => unique sorted permutation == pg_qsort's.

use ::mcx::PgVec;
use ::types_error::PgResult;

use crate::qsort::qsort_tuple;
use crate::ssup::SortComparator;
use crate::{cfi, CmpCtx, SortTuple, TuplesortData};

// Below this the scratch + histogram + copy-back fixed costs beat the
// n log n comparison savings (crossover: docs/optimizations/radix-abbrev.md).
const RADIX_MIN: usize = 1024;

const CFI_STRIDE: usize = 1 << 16;

impl<'m> TuplesortData<'m> {
    pub(crate) fn radix_applies(&self, n: usize) -> bool {
        #[cfg(test)]
        if crate::testhooks::RADIX_DISABLE.with(|c| c.get()) {
            return false;
        }
        n >= RADIX_MIN
            && self.abbrev.is_some()
            && matches!(self.sort_keys[0].comparator, SortComparator::Unsigned)
    }

    /// Ok(true) = `tuples` sorted, C-byte-identical. Ok(false) = a
    /// full-comparator-equal pair exists; `tuples` is UNMODIFIED and the
    /// caller must re-sort on the exact existing pg_qsort/mksort path.
    pub(crate) fn radix_sort_abbrev(&self, tuples: &mut [SortTuple]) -> PgResult<bool> {
        #[cfg(test)]
        crate::testhooks::RADIX_ATTEMPTS.with(|c| c.set(c.get() + 1));
        let done = self.radix_sort_abbrev_inner(tuples)?;
        #[cfg(test)]
        if done {
            crate::testhooks::RADIX_COMPLETED.with(|c| c.set(c.get() + 1));
        }
        Ok(done)
    }

    fn radix_sort_abbrev_inner(&self, tuples: &mut [SortTuple]) -> PgResult<bool> {
        let n = tuples.len();
        let key0 = &self.sort_keys[0];
        let flip: u64 = if key0.ssup_reverse { u64::MAX } else { 0 };

        let mut hist = [[0u32; 256]; 8];
        let mut nnulls = 0usize;
        let mut prev = 0u64;
        let mut inc = true;
        let mut first = true;
        for t in tuples.iter() {
            if t.isnull1 {
                nnulls += 1;
                inc = false;
                continue;
            }
            let k = t.datum1.as_u64() ^ flip;
            inc &= first | (k > prev);
            (prev, first) = (k, false);
            for (b, h) in hist.iter_mut().enumerate() {
                h[(k >> (b * 8)) as usize & 0xff] += 1;
            }
        }
        cfi()?;
        // Strictly increasing words = key0-distinct rows already in the
        // unique sorted permutation (abbrev order refines full order).
        if inc && nnulls == 0 {
            return Ok(true);
        }
        let nn = n - nnulls;

        // Constant bytes (one bucket holds all nn) contribute no ordering.
        let mut plan = [0usize; 8];
        let mut npasses = 0usize;
        for (b, h) in hist.iter().enumerate() {
            if nn > 0 && !h.iter().any(|&c| c as usize == nn) {
                plan[npasses] = b;
                npasses += 1;
            }
        }

        let nulls_first = key0.ssup_nulls_first;
        let (null_base, nonnull_base) = if nulls_first { (0, nnulls) } else { (nn, 0) };

        let mut buf_a: PgVec<'_, SortTuple> = ::mcx::vec_with_capacity_in(self.mcx, n)?;
        let mut buf_b: PgVec<'_, SortTuple> = if npasses >= 2 {
            ::mcx::vec_with_capacity_in(self.mcx, nn)?
        } else {
            PgVec::new_in(self.mcx)
        };
        let a = buf_a.as_mut_ptr();
        let pa = unsafe { a.add(nonnull_base) };
        let pb = buf_b.as_mut_ptr();

        // Pass 0: split nulls to their region, scatter (or append) non-nulls.
        // SAFETY (whole function): a/pa/pb address >= n / nn fresh slots;
        // counting-sort writes each dest slot exactly once per pass, so every
        // slot read below was written; src/dst regions never alias.
        unsafe {
            let mut null_cur = a.add(null_base);
            if npasses == 0 {
                let mut seq = pa;
                for t in tuples.iter() {
                    if t.isnull1 {
                        core::ptr::write(null_cur, *t);
                        null_cur = null_cur.add(1);
                    } else {
                        core::ptr::write(seq, *t);
                        seq = seq.add(1);
                    }
                }
            } else {
                let mut starts = prefix_starts(&hist[plan[0]]);
                let shift = plan[0] * 8;
                for chunk in tuples.chunks(CFI_STRIDE) {
                    for t in chunk {
                        if t.isnull1 {
                            core::ptr::write(null_cur, *t);
                            null_cur = null_cur.add(1);
                        } else {
                            let d = ((t.datum1.as_u64() ^ flip) >> shift) as usize & 0xff;
                            core::ptr::write(pa.add(starts[d] as usize), *t);
                            starts[d] += 1;
                        }
                    }
                    cfi()?;
                }
            }
        }

        let mut src = pa;
        let mut dst = pb;
        for &b in plan.get(1..npasses).unwrap_or(&[]) {
            let mut starts = prefix_starts(&hist[b]);
            let shift = b * 8;
            let mut done = 0usize;
            while done < nn {
                let stop = (done + CFI_STRIDE).min(nn);
                // SAFETY: contract above; [done, stop) in bounds of both.
                unsafe {
                    for i in done..stop {
                        let t = core::ptr::read(src.add(i));
                        let d = ((t.datum1.as_u64() ^ flip) >> shift) as usize & 0xff;
                        core::ptr::write(dst.add(starts[d] as usize), t);
                        starts[d] += 1;
                    }
                }
                done = stop;
                cfi()?;
            }
            core::mem::swap(&mut src, &mut dst);
        }

        // SAFETY: both regions fully initialized by the passes above.
        let (nonnull, nulls) = unsafe {
            (
                core::slice::from_raw_parts_mut(src, nn),
                core::slice::from_raw_parts_mut(a.add(null_base), nnulls),
            )
        };
        let ctx = CmpCtx {
            mcx: self.mcx,
            keys: &self.sort_keys,
            only_key: self.only_key,
            abbrev: &self.abbrev,
            variant: &self.variant,
            unique_violation: &self.unique_violation,
        };
        let ctx = &ctx;
        let cmp =
            |x: &SortTuple, y: &SortTuple| ctx.comparetup_spec(SortComparator::Unsigned, x, y);
        let mut i = 0;
        while i < nn {
            let w = nonnull[i].datum1.as_u64();
            let mut j = i + 1;
            while j < nn && nonnull[j].datum1.as_u64() == w {
                j += 1;
            }
            if j - i > 1 && !group_sorted_unique(&mut nonnull[i..j], cmp)? {
                return Ok(false);
            }
            i = j;
            cfi()?;
        }
        if nnulls > 1 && !group_sorted_unique(nulls, cmp)? {
            return Ok(false);
        }

        // SAFETY: regions disjoint, in bounds; scratch outlives the copy.
        unsafe {
            let tp = tuples.as_mut_ptr();
            core::ptr::copy_nonoverlapping(a.add(null_base), tp.add(null_base), nnulls);
            core::ptr::copy_nonoverlapping(src, tp.add(nonnull_base), nn);
        }
        Ok(true)
    }
}

#[inline]
fn prefix_starts(h: &[u32; 256]) -> [u32; 256] {
    let mut s = [0u32; 256];
    let mut acc = 0u32;
    for (slot, &c) in s.iter_mut().zip(h) {
        *slot = acc;
        acc += c;
    }
    s
}

fn group_sorted_unique(
    seg: &mut [SortTuple],
    cmp: impl Fn(&SortTuple, &SortTuple) -> i32 + Copy,
) -> PgResult<bool> {
    qsort_tuple(seg, cmp)?;
    for k in 1..seg.len() {
        if cmp(&seg[k - 1], &seg[k]) == 0 {
            return Ok(false);
        }
    }
    Ok(true)
}
