//! Migration equivalence gate: verbatim copies of the pre-consolidation
//! in-tree pg_qsort ports (gistproc/analyze infallible-slice shape,
//! rangetypes_gist/brin_minmax_multi fallible-slice shape, statistics
//! sortitem FnMut-slice shape) must produce bit-identical output —
//! including equal-key order — to the shared crate over randomized
//! dup-heavy inputs. The tuplesort pointer-shape copy is the direct
//! ancestor of the shared core and is additionally covered by tuplesort's
//! own tie-order pinning tests (mksort_ties_match_pgqsort_order).
//!
//! The only edits to the verbatim text: `use ::types_error::PgResult` is
//! replaced by a local `type PgResult<T>` alias (keeps this crate dep-free);
//! items are wrapped in modules.

#![allow(dead_code)]

mod legacy_gistproc {
    // Verbatim: crates/backend/access/gist/gistproc/src/qsort.rs
    #[inline]
    fn med3<T: Copy>(v: &[T], a: usize, b: usize, c: usize, cmp: &impl Fn(&T, &T) -> i32) -> usize {
        if cmp(&v[a], &v[b]) < 0 {
            if cmp(&v[b], &v[c]) < 0 {
                b
            } else if cmp(&v[a], &v[c]) < 0 {
                c
            } else {
                a
            }
        } else if cmp(&v[b], &v[c]) > 0 {
            b
        } else if cmp(&v[a], &v[c]) < 0 {
            a
        } else {
            c
        }
    }

    pub fn pg_qsort<T: Copy>(v: &mut [T], cmp: impl Fn(&T, &T) -> i32) {
        pg_qsort_range(v, 0, v.len(), &cmp);
    }

    fn pg_qsort_range<T: Copy>(v: &mut [T], mut a: usize, mut n: usize, cmp: &impl Fn(&T, &T) -> i32) {
        loop {
            if n < 7 {
                let mut pm = a + 1;
                while pm < a + n {
                    let mut pl = pm;
                    while pl > a && cmp(&v[pl - 1], &v[pl]) > 0 {
                        v.swap(pl, pl - 1);
                        pl -= 1;
                    }
                    pm += 1;
                }
                return;
            }
            let mut presorted = true;
            let mut pm = a + 1;
            while pm < a + n {
                if cmp(&v[pm - 1], &v[pm]) > 0 {
                    presorted = false;
                    break;
                }
                pm += 1;
            }
            if presorted {
                return;
            }
            let mut pm = a + n / 2;
            if n > 7 {
                let mut pl = a;
                let mut pn = a + n - 1;
                if n > 40 {
                    let d = n / 8;
                    pl = med3(v, pl, pl + d, pl + 2 * d, cmp);
                    pm = med3(v, pm - d, pm, pm + d, cmp);
                    pn = med3(v, pn - 2 * d, pn - d, pn, cmp);
                }
                pm = med3(v, pl, pm, pn, cmp);
            }
            v.swap(a, pm);
            let mut pa = a + 1;
            let mut pb = a + 1;
            let mut pc = a + n - 1;
            let mut pd = a + n - 1;
            loop {
                while pb <= pc {
                    let r = cmp(&v[pb], &v[a]);
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
                    let r = cmp(&v[pc], &v[a]);
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
            let d1 = (pa - a).min(pb - pa);
            for i in 0..d1 {
                v.swap(a + i, pb - d1 + i);
            }
            let d1r = (pd - pc).min(pn - pd - 1);
            for i in 0..d1r {
                v.swap(pb + i, pn - d1r + i);
            }
            let d1 = pb - pa;
            let d2 = pd - pc;
            if d1 <= d2 {
                if d1 > 1 {
                    pg_qsort_range(v, a, d1, cmp);
                }
                if d2 > 1 {
                    a = pn - d2;
                    n = d2;
                    continue;
                }
            } else {
                if d2 > 1 {
                    pg_qsort_range(v, pn - d2, d2, cmp);
                }
                if d1 > 1 {
                    n = d1;
                    continue;
                }
            }
            return;
        }
    }

}

mod legacy_rangetypes {
    // Verbatim: crates/backend/utils/adt/rangetypes_gist/src/qsort.rs
    // (identical algorithm text to brin_minmax_multi's copy)
    type PgResult<T> = Result<T, ()>;


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
}

mod legacy_sortitem {
    // Verbatim: crates/backend/statistics/statistics/src/sortitem.rs pg_qsort
    pub fn pg_qsort<T: Copy, C: FnMut(&T, &T) -> i32>(a: &mut [T], mut cmp: C) {
        if a.len() > 1 {
            let n = a.len();
            qsort_rec(a, 0, n, &mut cmp);
        }
    }

    fn qsort_rec<T: Copy, C: FnMut(&T, &T) -> i32>(a: &mut [T], mut lo: usize, mut n: usize, cmp: &mut C) {
        loop {
            if n < 7 {
                for pm in lo + 1..lo + n {
                    let mut pl = pm;
                    while pl > lo && cmp(&a[pl - 1], &a[pl]) > 0 {
                        a.swap(pl, pl - 1);
                        pl -= 1;
                    }
                }
                return;
            }
            let mut presorted = true;
            for pm in lo + 1..lo + n {
                if cmp(&a[pm - 1], &a[pm]) > 0 {
                    presorted = false;
                    break;
                }
            }
            if presorted {
                return;
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
            let mut pb = pa;
            let mut pc = lo + n - 1;
            let mut pd = pc;
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
                    qsort_rec(a, lo, d1, cmp);
                }
                if d2 > 1 {
                    lo = pn - d2;
                    n = d2;
                    continue;
                }
            } else {
                if d2 > 1 {
                    qsort_rec(a, pn - d2, d2, cmp);
                }
                if d1 > 1 {
                    n = d1;
                    continue;
                }
            }
            return;
        }
    }

    fn med3<T: Copy, C: FnMut(&T, &T) -> i32>(
        a: &[T],
        x: usize,
        y: usize,
        z: usize,
        cmp: &mut C,
    ) -> usize {
        if cmp(&a[x], &a[y]) < 0 {
            if cmp(&a[y], &a[z]) < 0 {
                y
            } else if cmp(&a[x], &a[z]) < 0 {
                z
            } else {
                x
            }
        } else if cmp(&a[y], &a[z]) > 0 {
            y
        } else if cmp(&a[x], &a[z]) < 0 {
            x
        } else {
            z
        }
    }
    fn swapn<T: Copy>(a: &mut [T], mut x: usize, mut y: usize, n: usize) {
        for _ in 0..n {
            a.swap(x, y);
            x += 1;
            y += 1;
        }
    }
}

fn lcg(state: &mut u64) -> u64 {
    *state = state
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    *state >> 33
}

fn cases() -> Vec<Vec<(i32, u32)>> {
    let mut out = Vec::new();
    let mut rng = 0xfeed_beef_u64;
    for n in [0usize, 1, 2, 5, 6, 7, 8, 13, 39, 40, 41, 64, 100, 500, 1000, 4096] {
        for &kmod in &[2u64, 7, 16, 1000] {
            out.push((0..n).map(|i| ((lcg(&mut rng) % kmod) as i32, i as u32)).collect());
        }
        out.push((0..n).map(|i| ((i / 3) as i32, i as u32)).collect());
        out.push((0..n).map(|i| (-((i / 3) as i32), i as u32)).collect());
        out.push((0..n).map(|i| (7, i as u32)).collect());
    }
    out
}

fn cmp3(a: &(i32, u32), b: &(i32, u32)) -> i32 {
    (a.0 > b.0) as i32 - (a.0 < b.0) as i32
}

#[test]
fn legacy_gistproc_shape_matches_shared() {
    for case in cases() {
        let mut old = case.clone();
        let mut new = case.clone();
        legacy_gistproc::pg_qsort(&mut old, cmp3);
        pg_qsort::pg_qsort(&mut new, cmp3);
        assert_eq!(old, new);
    }
}

#[test]
fn legacy_rangetypes_shape_matches_shared() {
    for case in cases() {
        let mut old = case.clone();
        let mut new = case.clone();
        legacy_rangetypes::pg_qsort_arg(&mut old, |a, b| Ok(cmp3(a, b))).unwrap();
        pg_qsort::pg_qsort_arg(&mut new, |a, b| Ok::<i32, ()>(cmp3(a, b))).unwrap();
        assert_eq!(old, new);
    }
}

#[test]
fn legacy_sortitem_shape_matches_shared() {
    for case in cases() {
        let mut old = case.clone();
        let mut new = case.clone();
        legacy_sortitem::pg_qsort(&mut old, cmp3);
        pg_qsort::pg_qsort(&mut new, cmp3);
        assert_eq!(old, new);
    }
}
