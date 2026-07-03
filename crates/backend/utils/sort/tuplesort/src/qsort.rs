// lib/sort_template.h (Bentley & McIlroy) over SortTuple, in C's exact
// pointer shape (bounds checks in the partition loops cost ~10% wall). The
// exact algorithm is a parity requirement: equal-key output order must match
// C's qsort. One monomorphization per comparator call site = ST_DEFINE.

use ::types_error::PgResult;

use crate::cfi;
use crate::SortTuple;

// SAFETY contract (all fns): pointers derived from one live &mut [SortTuple]
// region of n elements; every deref stays inside it (the template's own
// invariant). SortTuple is Copy, so ptr::swap/read need no drop care.

#[inline(never)]
unsafe fn med3<C: Fn(&SortTuple, &SortTuple) -> i32 + Copy>(
    a: *mut SortTuple,
    b: *mut SortTuple,
    c: *mut SortTuple,
    cmp: C,
) -> *mut SortTuple {
    unsafe {
        if cmp(&*a, &*b) < 0 {
            if cmp(&*b, &*c) < 0 {
                b
            } else if cmp(&*a, &*c) < 0 {
                c
            } else {
                a
            }
        } else if cmp(&*b, &*c) > 0 {
            b
        } else if cmp(&*a, &*c) < 0 {
            a
        } else {
            c
        }
    }
}

unsafe fn swapn(mut a: *mut SortTuple, mut b: *mut SortTuple, n: usize) {
    for _ in 0..n {
        unsafe {
            core::ptr::swap(a, b);
            a = a.add(1);
            b = b.add(1);
        }
    }
}

pub(crate) fn qsort_tuple<C: Fn(&SortTuple, &SortTuple) -> i32 + Copy>(
    data: &mut [SortTuple],
    cmp: C,
) -> PgResult<()> {
    // SAFETY: the region is exactly data's; see the contract above.
    unsafe { qsort_rec(data.as_mut_ptr(), data.len(), cmp) }
}

unsafe fn qsort_rec<C: Fn(&SortTuple, &SortTuple) -> i32 + Copy>(
    mut a: *mut SortTuple,
    mut n: usize,
    cmp: C,
) -> PgResult<()> {
    unsafe {
        loop {
            cfi()?;
            if n < 7 {
                let mut pm = a.add(1);
                while pm < a.add(n) {
                    let mut pl = pm;
                    while pl > a && cmp(&*pl.sub(1), &*pl) > 0 {
                        core::ptr::swap(pl, pl.sub(1));
                        pl = pl.sub(1);
                    }
                    pm = pm.add(1);
                }
                return Ok(());
            }
            let mut presorted = true;
            let mut pm = a.add(1);
            while pm < a.add(n) {
                cfi()?;
                if cmp(&*pm.sub(1), &*pm) > 0 {
                    presorted = false;
                    break;
                }
                pm = pm.add(1);
            }
            if presorted {
                return Ok(());
            }
            let mut pm = a.add(n / 2);
            if n > 7 {
                let mut pl = a;
                let mut pn = a.add(n - 1);
                if n > 40 {
                    let d = n / 8;
                    pl = med3(pl, pl.add(d), pl.add(2 * d), cmp);
                    pm = med3(pm.sub(d), pm, pm.add(d), cmp);
                    pn = med3(pn.sub(2 * d), pn.sub(d), pn, cmp);
                }
                pm = med3(pl, pm, pn, cmp);
            }
            core::ptr::swap(a, pm);
            let mut pa = a.add(1);
            let mut pb = pa;
            let mut pc = a.add(n - 1);
            let mut pd = pc;
            loop {
                while pb <= pc {
                    let r = cmp(&*pb, &*a);
                    if r > 0 {
                        break;
                    }
                    if r == 0 {
                        core::ptr::swap(pa, pb);
                        pa = pa.add(1);
                    }
                    pb = pb.add(1);
                    cfi()?;
                }
                while pb <= pc {
                    let r = cmp(&*pc, &*a);
                    if r < 0 {
                        break;
                    }
                    if r == 0 {
                        core::ptr::swap(pc, pd);
                        pd = pd.sub(1);
                    }
                    pc = pc.sub(1);
                    cfi()?;
                }
                if pb > pc {
                    break;
                }
                core::ptr::swap(pb, pc);
                pb = pb.add(1);
                pc = pc.sub(1);
            }
            let pn = a.add(n);
            let mut d1 = (pa.offset_from(a) as usize).min(pb.offset_from(pa) as usize);
            swapn(a, pb.sub(d1), d1);
            d1 = (pd.offset_from(pc) as usize).min(pn.offset_from(pd) as usize - 1);
            swapn(pb, pn.sub(d1), d1);
            d1 = pb.offset_from(pa) as usize;
            let d2 = pd.offset_from(pc) as usize;
            if d1 <= d2 {
                if d1 > 1 {
                    qsort_rec(a, d1, cmp)?;
                }
                if d2 > 1 {
                    a = pn.sub(d2);
                    n = d2;
                    continue;
                }
            } else {
                if d2 > 1 {
                    qsort_rec(pn.sub(d2), d2, cmp)?;
                }
                if d1 > 1 {
                    n = d1;
                    continue;
                }
            }
            return Ok(());
        }
    }
}
