// lib/sort_template.h (Bentley & McIlroy) over SortTuple. The exact algorithm
// is a parity requirement: equal-key output order must match C's qsort.
// One monomorphization per comparator call site = C's ST_DEFINE instances.

use ::types_error::PgResult;

use crate::cfi;
use crate::SortTuple;

#[inline(never)]
fn med3<C: Fn(&SortTuple, &SortTuple) -> i32 + Copy>(
    a: &[SortTuple],
    i: usize,
    j: usize,
    k: usize,
    cmp: C,
) -> usize {
    if cmp(&a[i], &a[j]) < 0 {
        if cmp(&a[j], &a[k]) < 0 {
            j
        } else if cmp(&a[i], &a[k]) < 0 {
            k
        } else {
            i
        }
    } else if cmp(&a[j], &a[k]) > 0 {
        j
    } else if cmp(&a[i], &a[k]) < 0 {
        i
    } else {
        k
    }
}

fn swapn(a: &mut [SortTuple], mut i: usize, mut j: usize, n: usize) {
    for _ in 0..n {
        a.swap(i, j);
        i += 1;
        j += 1;
    }
}

pub(crate) fn qsort_tuple<C: Fn(&SortTuple, &SortTuple) -> i32 + Copy>(
    a: &mut [SortTuple],
    cmp: C,
) -> PgResult<()> {
    let mut lo = 0usize;
    let mut n = a.len();

    loop {
        cfi()?;
        if n < 7 {
            for pm in lo + 1..lo + n {
                let mut pl = pm;
                while pl > lo && cmp(&a[pl - 1], &a[pl]) > 0 {
                    a.swap(pl, pl - 1);
                    pl -= 1;
                }
            }
            return Ok(());
        }
        let mut presorted = true;
        for pm in lo + 1..lo + n {
            cfi()?;
            if cmp(&a[pm - 1], &a[pm]) > 0 {
                presorted = false;
                break;
            }
        }
        if presorted {
            return Ok(());
        }
        let mut pm = lo + n / 2;
        if n > 7 {
            let mut pl = lo;
            let mut pn = lo + n - 1;
            if n > 40 {
                let d = n / 8;
                pl = med3(a, pl, pl + d, pl + 2 * d, cmp);
                pm = med3(a, pm - d, pm, pm + d, cmp);
                pn = med3(a, pn - 2 * d, pn - d, pn, cmp);
            }
            pm = med3(a, pl, pm, pn, cmp);
        }
        a.swap(lo, pm);
        let mut pa = lo + 1;
        let mut pb = lo + 1;
        let mut pc = lo + n - 1;
        let mut pd = lo + n - 1;
        loop {
            while pb <= pc {
                let r = cmp(&a[pb], &a[lo]);
                if r > 0 {
                    break;
                }
                if r == 0 {
                    a.swap(pa, pb);
                    pa += 1;
                }
                pb += 1;
                cfi()?;
            }
            while pb <= pc {
                let r = cmp(&a[pc], &a[lo]);
                if r < 0 {
                    break;
                }
                if r == 0 {
                    a.swap(pc, pd);
                    pd -= 1;
                }
                pc -= 1;
                cfi()?;
            }
            if pb > pc {
                break;
            }
            a.swap(pb, pc);
            pb += 1;
            pc -= 1;
        }
        let pn = lo + n;
        let mut d1 = (pa - lo).min(pb - pa);
        swapn(a, lo, pb - d1, d1);
        d1 = (pd - pc).min(pn - pd - 1);
        swapn(a, pb, pn - d1, d1);
        d1 = pb - pa;
        let d2 = pd - pc;
        if d1 <= d2 {
            if d1 > 1 {
                qsort_tuple(&mut a[lo..lo + d1], cmp)?;
            }
            if d2 > 1 {
                lo = pn - d2;
                n = d2;
                continue;
            }
        } else {
            if d2 > 1 {
                qsort_tuple(&mut a[pn - d2..pn], cmp)?;
            }
            if d1 > 1 {
                n = d1;
                continue;
            }
        }
        return Ok(());
    }
}
