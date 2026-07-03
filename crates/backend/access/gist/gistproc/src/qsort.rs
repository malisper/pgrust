//! lib/sort_template.h (pg_qsort) over Copy elements. The exact algorithm is
//! a parity requirement: equal-key output order decides gist picksplit page
//! assignment, which the index byte-parity gate observes.

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sorts_with_dup_heavy_inputs() {
        let mut rng = 123456789u64;
        for n in [0usize, 1, 2, 6, 7, 8, 40, 41, 100, 1000] {
            let mut v: Vec<i32> = (0..n)
                .map(|_| {
                    rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
                    ((rng >> 33) % 7) as i32
                })
                .collect();
            let mut expect = v.clone();
            expect.sort_unstable();
            pg_qsort(&mut v, |a, b| a - b);
            assert_eq!(v, expect);
        }
    }
}
