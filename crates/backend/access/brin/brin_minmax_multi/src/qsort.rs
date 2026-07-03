//! lib/sort_template.h (pg_qsort/qsort_arg) over Copy elements. The exact
//! algorithm is a parity requirement: equal-key output order decides which
//! gaps reduce_expanded_ranges keeps, which the index byte-parity gate
//! observes. Comparators are fallible (fmgr); an error aborts the sort with
//! the first error propagated.

use ::types_error::PgResult;

fn med3<T: Copy>(
    v: &[T],
    a: usize,
    b: usize,
    c: usize,
    cmp: &mut impl FnMut(&T, &T) -> PgResult<i32>,
) -> PgResult<usize> {
    Ok(if cmp(&v[a], &v[b])? < 0 {
        if cmp(&v[b], &v[c])? < 0 {
            b
        } else if cmp(&v[a], &v[c])? < 0 {
            c
        } else {
            a
        }
    } else if cmp(&v[b], &v[c])? > 0 {
        b
    } else if cmp(&v[a], &v[c])? < 0 {
        a
    } else {
        c
    })
}

pub fn pg_qsort_arg<T: Copy>(
    v: &mut [T],
    mut cmp: impl FnMut(&T, &T) -> PgResult<i32>,
) -> PgResult<()> {
    let n = v.len();
    pg_qsort_range(v, 0, n, &mut cmp)
}

fn pg_qsort_range<T: Copy>(
    v: &mut [T],
    mut a: usize,
    mut n: usize,
    cmp: &mut impl FnMut(&T, &T) -> PgResult<i32>,
) -> PgResult<()> {
    loop {
        if n < 7 {
            let mut pm = a + 1;
            while pm < a + n {
                let mut pl = pm;
                while pl > a && cmp(&v[pl - 1], &v[pl])? > 0 {
                    v.swap(pl, pl - 1);
                    pl -= 1;
                }
                pm += 1;
            }
            return Ok(());
        }
        let mut presorted = true;
        let mut pm = a + 1;
        while pm < a + n {
            if cmp(&v[pm - 1], &v[pm])? > 0 {
                presorted = false;
                break;
            }
            pm += 1;
        }
        if presorted {
            return Ok(());
        }
        let mut pm = a + n / 2;
        if n > 7 {
            let mut pl = a;
            let mut pn = a + n - 1;
            if n > 40 {
                let d = n / 8;
                pl = med3(v, pl, pl + d, pl + 2 * d, cmp)?;
                pm = med3(v, pm - d, pm, pm + d, cmp)?;
                pn = med3(v, pn - 2 * d, pn - d, pn, cmp)?;
            }
            pm = med3(v, pl, pm, pn, cmp)?;
        }
        v.swap(a, pm);
        let mut pa = a + 1;
        let mut pb = pa;
        let mut pc = a + n - 1;
        let pd_end = pc;
        let mut pd = pd_end;
        loop {
            let mut r;
            while pb <= pc {
                r = cmp(&v[pb], &v[a])?;
                if r > 0 {
                    break;
                }
                if r == 0 {
                    v.swap(pa, pb);
                    pa += 1;
                }
                pb += 1;
            }
            while pb <= pc {
                r = cmp(&v[pc], &v[a])?;
                if r < 0 {
                    break;
                }
                if r == 0 {
                    v.swap(pc, pd);
                    pd -= 1;
                }
                pc -= 1;
            }
            if pb > pc {
                break;
            }
            v.swap(pb, pc);
            pb += 1;
            pc -= 1;
        }
        let pn = a + n;
        let mut d1 = core::cmp::min(pa - a, pb - pa);
        vecswap(v, a, pb - d1, d1);
        d1 = core::cmp::min(pd - pc, pn - pd - 1);
        vecswap(v, pb, pn - d1, d1);
        d1 = pb - pa;
        let d2 = pd - pc;
        if d1 <= d2 {
            if d1 > 1 {
                pg_qsort_range(v, a, d1, cmp)?;
            }
            if d2 > 1 {
                a = pn - d2;
                n = d2;
                continue;
            }
            return Ok(());
        } else {
            if d2 > 1 {
                pg_qsort_range(v, pn - d2, d2, cmp)?;
            }
            if d1 > 1 {
                n = d1;
                continue;
            }
            return Ok(());
        }
    }
}

fn vecswap<T: Copy>(v: &mut [T], a: usize, b: usize, n: usize) {
    for i in 0..n {
        v.swap(a + i, b + i);
    }
}
