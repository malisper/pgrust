//! Port of `utils/sort/qsort_interruptible.c` (PostgreSQL 18.3): the
//! `qsort_arg`-style generic quicksort with `CHECK_FOR_INTERRUPTS()` woven into
//! the inner loops, used by callers that may run very large sorts (extended
//! statistics, per-column ANALYZE, etc.).
//!
//! The C file is a one-line instantiation of `lib/sort_template.h`:
//!
//! ```c
//! #define ST_SORT qsort_interruptible
//! #define ST_ELEMENT_TYPE_VOID
//! #define ST_COMPARATOR_TYPE_NAME qsort_arg_comparator
//! #define ST_COMPARE_RUNTIME_POINTER
//! #define ST_COMPARE_ARG_TYPE void
//! #define ST_SCOPE
//! #define ST_DEFINE
//! #define ST_CHECK_FOR_INTERRUPTS
//! #include "lib/sort_template.h"
//! ```
//!
//! so the real algorithm lives in `sort_template.h`: the Bentley–McIlroy
//! "Engineering a sort function" quicksort, with PostgreSQL's modifications
//! (an already-sorted-input fast path, recurse-on-the-smaller-partition to bound
//! stack depth, and — because `ST_CHECK_FOR_INTERRUPTS` is defined —
//! `CHECK_FOR_INTERRUPTS()` calls at the loop entry, in the presorted scan, and
//! in the two partition scan loops).
//!
//! ## Model
//!
//! The C instantiation is the generic `void *` qsort: `ST_ELEMENT_TYPE_VOID`
//! gives it a runtime `element_size`, `ST_COMPARE_RUNTIME_POINTER` a runtime
//! comparator function pointer, and `ST_COMPARE_ARG_TYPE void` the
//! `qsort_arg`-style extra `arg`. This port is generic over the element type
//! `T` and sorts `data` in place with a runtime comparator returning the C
//! three-way `int` (`< 0`, `0`, `> 0`); the pass-through `arg` is captured by
//! the `FnMut` closure. `ST_POINTER_STEP` is one element, so the template's
//! `uint8`-byte pointer arithmetic becomes element-index arithmetic and the
//! `ST_SWAP`/`ST_SWAPN` byte swaps become `slice::swap`. The `goto loop`
//! tail-iteration is a `loop` / `continue`.
//!
//! `CHECK_FOR_INTERRUPTS()` is the workspace's centralized interrupt seam
//! ([`::postgres_seams::check_for_interrupts`]): in C the macro
//! services a pending interrupt and may `ereport(ERROR/FATAL)` (e.g. on query
//! cancel), which here surfaces as an `Err` that aborts the sort and is
//! propagated to the caller. The owner of that seam is `tcop/postgres.c`; until
//! it lands a call panics, exactly as for any other unported seam.

use ::postgres_seams::check_for_interrupts;
use ::types_error::PgResult;

/// `Min(a, b)` (`c.h`), over the signed offset distances used by the partition
/// (`pa - a`, `pb - pa`, `pd - pc`, `pn - pd - 1` — the C pointer differences,
/// which can legitimately be evaluated where intermediate values dip to `-1`).
#[inline]
fn min_isize(a: isize, b: isize) -> isize {
    if a < b {
        a
    } else {
        b
    }
}

/// `qsort_interruptible(void *data, size_t n, size_t element_size,
/// qsort_arg_comparator compare, void *arg)` (qsort_interruptible.c via
/// `lib/sort_template.h` `ST_SORT`).
///
/// Sorts `data` in place. `compare(a, b)` returns the C three-way result:
/// negative if `a` sorts before `b`, zero if equal, positive if after. The
/// pass-through `arg` of the C `qsort_arg_comparator` is captured by the
/// closure.
///
/// Returns `Err` only if `CHECK_FOR_INTERRUPTS()` fires (query cancel,
/// termination, recovery conflict); on success the slice is fully ordered.
pub fn qsort_interruptible<T, F>(data: &mut [T], mut compare: F) -> PgResult<()>
where
    F: FnMut(&T, &T) -> i32,
{
    let n = data.len();
    // Recursion threads `&mut F` (not a freshly-wrapped closure) so the worker
    // monomorphizes to a single type and recurses on itself, matching the C
    // `ST_SORT` recursion on the same comparator/arg.
    qsort_worker(data, 0, n, &mut compare)
}

/// The `ST_SORT` body over `data[a .. a + n]`. The C `goto loop` tail-iteration
/// is a `loop` / `continue 'loop_`; "recurse on the smaller partition" calls
/// this worker again with the same comparator reference.
fn qsort_worker<T, F>(data: &mut [T], mut a: usize, mut n: usize, compare: &mut F) -> PgResult<()>
where
    F: FnMut(&T, &T) -> i32,
{
    // `a` is the base offset of the current (sub)array within `data`; `n` is
    // its length. This pair is what the C `goto loop` rewinds.
    'loop_: loop {
        // DO_CHECK_FOR_INTERRUPTS();
        check_for_interrupts::call()?;

        if n < 7 {
            // Insertion sort:
            //   for (pm = a + STEP; pm < a + n*STEP; pm += STEP)
            //     for (pl = pm; pl > a && COMPARE(pl-STEP, pl) > 0; pl -= STEP)
            //       SWAP(pl, pl - STEP);
            let mut pm = 1;
            while pm < n {
                let mut pl = pm;
                while pl > 0 && compare(&data[a + pl - 1], &data[a + pl]) > 0 {
                    data.swap(a + pl, a + pl - 1);
                    pl -= 1;
                }
                pm += 1;
            }
            return Ok(());
        }

        // Check for already-sorted input (PostgreSQL's addition):
        //   presorted = 1;
        //   for (pm = a + STEP; pm < a + n*STEP; pm += STEP) {
        //     DO_CHECK_FOR_INTERRUPTS();
        //     if (COMPARE(pm - STEP, pm) > 0) { presorted = 0; break; }
        //   }
        //   if (presorted) return;
        let mut presorted = true;
        {
            let mut pm = 1;
            while pm < n {
                check_for_interrupts::call()?;
                if compare(&data[a + pm - 1], &data[a + pm]) > 0 {
                    presorted = false;
                    break;
                }
                pm += 1;
            }
        }
        if presorted {
            return Ok(());
        }

        // Choose a pivot: median-of-three, or median-of-medians ("ninther") for
        // n > 40, and swap it to the front. Offsets are relative to `a`.
        //   pm = a + (n/2)*STEP;
        //   if (n > 7) {
        //     pl = a; pn = a + (n-1)*STEP;
        //     if (n > 40) {
        //       d = (n/8)*STEP;
        //       pl = MED3(pl, pl+d, pl+2d);
        //       pm = MED3(pm-d, pm, pm+d);
        //       pn = MED3(pn-2d, pn-d, pn);
        //     }
        //     pm = MED3(pl, pm, pn);
        //   }
        //   SWAP(a, pm);
        let mut pm = n / 2;
        if n > 7 {
            let mut pl = 0;
            let mut pn = n - 1;
            if n > 40 {
                let d = n / 8;
                pl = med3(data, a, pl, pl + d, pl + 2 * d, compare);
                pm = med3(data, a, pm - d, pm, pm + d, compare);
                pn = med3(data, a, pn - 2 * d, pn - d, pn, compare);
            }
            pm = med3(data, a, pl, pm, pn, compare);
        }
        data.swap(a, a + pm);

        // Partition (the classic Bentley–McIlroy fat-pivot three-way scan).
        // `pa..pb` accumulates pivot-equal elements on the left, `pc..pd` on the
        // right. These are signed offsets relative to `a` mirroring the C
        // pointer differences (`pc`/`pd` may walk to `a - 1`, i.e. offset -1).
        // The pivot lives at `a`.
        //   pa = pb = a + STEP;
        //   pc = pd = a + (n-1)*STEP;
        let nn = n as isize;
        let mut pa: isize = 1;
        let mut pb: isize = 1;
        let mut pc: isize = nn - 1;
        let mut pd: isize = nn - 1;
        loop {
            // while (pb <= pc && (r = COMPARE(pb, a)) <= 0) {
            //   if (r == 0) { SWAP(pa, pb); pa += STEP; }
            //   pb += STEP; DO_CHECK_FOR_INTERRUPTS();
            // }
            while pb <= pc {
                let r = compare(&data[a + pb as usize], &data[a]);
                if r > 0 {
                    break;
                }
                if r == 0 {
                    data.swap(a + pa as usize, a + pb as usize);
                    pa += 1;
                }
                pb += 1;
                check_for_interrupts::call()?;
            }
            // while (pb <= pc && (r = COMPARE(pc, a)) >= 0) {
            //   if (r == 0) { SWAP(pc, pd); pd -= STEP; }
            //   pc -= STEP; DO_CHECK_FOR_INTERRUPTS();
            // }
            while pb <= pc {
                let r = compare(&data[a + pc as usize], &data[a]);
                if r < 0 {
                    break;
                }
                if r == 0 {
                    data.swap(a + pc as usize, a + pd as usize);
                    pd -= 1;
                }
                pc -= 1;
                check_for_interrupts::call()?;
            }
            if pb > pc {
                break;
            }
            // SWAP(pb, pc); pb += STEP; pc -= STEP;
            data.swap(a + pb as usize, a + pc as usize);
            pb += 1;
            pc -= 1;
        }

        // Swap the pivot-equal runs from the two ends back into the middle.
        //   pn = a + n*STEP;
        //   d1 = Min(pa - a, pb - pa);   SWAPN(a, pb - d1, d1);
        //   d1 = Min(pd - pc, pn - pd - STEP); SWAPN(pb, pn - d1, d1);
        let pn: isize = nn; // C `a + n*STEP`, i.e. offset `n` past `a`.
        let d1 = min_isize(pa, pb - pa);
        swapn(data, a, a + (pb - d1) as usize, d1 as usize);
        let d1 = min_isize(pd - pc, pn - pd - 1);
        swapn(data, a + pb as usize, a + (pn - d1) as usize, d1 as usize);

        //   d1 = pb - pa;  d2 = pd - pc;
        let d1 = pb - pa;
        let d2 = pd - pc;

        if d1 <= d2 {
            // Recurse on left partition, then iterate on the right.
            //   if (d1 > STEP) DO_SORT(a, d1/STEP);
            //   if (d2 > STEP) { a = pn - d2; n = d2/STEP; goto loop; }
            if d1 > 1 {
                qsort_worker(data, a, d1 as usize, compare)?;
            }
            if d2 > 1 {
                a += (pn - d2) as usize;
                n = d2 as usize;
                continue 'loop_;
            }
        } else {
            // Recurse on right partition, then iterate on the left.
            //   if (d2 > STEP) DO_SORT(pn - d2, d2/STEP);
            //   if (d1 > STEP) { n = d1/STEP; goto loop; }
            if d2 > 1 {
                qsort_worker(data, a + (pn - d2) as usize, d2 as usize, compare)?;
            }
            if d1 > 1 {
                n = d1 as usize;
                continue 'loop_;
            }
        }
        return Ok(());
    }
}

/// `ST_MED3` (sort_template.h): the median of three elements at offsets `i`,
/// `j`, `k` (relative to base `a`), returning the chosen offset.
///
/// ```c
/// return COMPARE(a, b) < 0 ?
///   (COMPARE(b, c) < 0 ? b : (COMPARE(a, c) < 0 ? c : a))
///   : (COMPARE(b, c) > 0 ? b : (COMPARE(a, c) < 0 ? a : c));
/// ```
fn med3<T, F>(data: &[T], a: usize, i: usize, j: usize, k: usize, compare: &mut F) -> usize
where
    F: FnMut(&T, &T) -> i32,
{
    if compare(&data[a + i], &data[a + j]) < 0 {
        if compare(&data[a + j], &data[a + k]) < 0 {
            j
        } else if compare(&data[a + i], &data[a + k]) < 0 {
            k
        } else {
            i
        }
    } else if compare(&data[a + j], &data[a + k]) > 0 {
        j
    } else if compare(&data[a + i], &data[a + k]) < 0 {
        i
    } else {
        k
    }
}

/// `ST_SWAPN` (sort_template.h): swap `n` consecutive elements starting at
/// offsets `x` and `y` within `data` (here one element per step, so the
/// template's `element_size`-byte runs are element swaps).
fn swapn<T>(data: &mut [T], x: usize, y: usize, n: usize) {
    for i in 0..n {
        data.swap(x + i, y + i);
    }
}

/// This crate installs no inward seams: `qsort_interruptible` is a generic
/// leaf utility called directly by its consumers (extended statistics,
/// per-column ANALYZE), exactly as the C callers `#include "miscadmin.h"` and
/// call it directly — there is no fn-pointer indirection or dependency cycle to
/// break. Provided for the workspace's uniform `init_seams()` convention.
pub fn init_seams() {}

#[cfg(test)]
mod tests;
