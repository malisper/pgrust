//! Canonical port of PostgreSQL's qsort (port/qsort.c instantiating
//! lib/sort_template.h; Bentley & McIlroy "Engineering a sort function",
//! plus PG's presorted check and recurse-on-smaller-partition change).
//!
//! The EXACT algorithm is a parity requirement, not an implementation
//! detail: pg_qsort is deterministic, and its equal-key output order is
//! observable through on-disk index layout (gist/brin picksplit), serialized
//! statistics bytes, plan shape, and function outputs. Order-exact rulings
//! lean on "same comparator + same input => same permutation as C". Do not
//! swap in a Rust std sort or "improve" the algorithm; speed must come from
//! codegen, never from reordering comparisons. See
//! proofs/AUDIT-QSORT-TIE-ORDER.md (audit/qsort-tie-order).
//!
//! A second, independent reason ANALYZE uses this over std sorts: user btree
//! comparators may be intransitive (contrib cube's cube_cmp is), and std's
//! driver panics on non-total orders where C's qsort just produces an
//! arbitrary-but-safe permutation. This port, like C, never inspects
//! comparator consistency and never reads out of bounds regardless of the
//! comparator (all cursor movement is guarded by the same bounds C uses).
//!
//! Three entry points, one core (the generic core is written in C's exact
//! pointer shape; slice indexing's bounds checks cost ~10% wall in the
//! partition loops):
//!   - [`pg_qsort`]: infallible comparator (C's pg_qsort).
//!   - [`pg_qsort_arg`]: fallible comparator (C's qsort_arg where the
//!     comparator is an fmgr call); first error aborts the sort, leaving a
//!     valid permutation of the input.
//!   - [`pg_qsort_interruptible`]: infallible comparator + fallible
//!     interrupt check (C's qsort_interruptible / ST_CHECK_FOR_INTERRUPTS);
//!     check placement matches the template exactly.

#![cfg_attr(not(test), no_std)]

use core::convert::Infallible;

/// C's pg_qsort: sort `v` with `cmp` (strcmp-style return). Equal-key output
/// order is bit-exact with C's pg_qsort given the same comparator.
#[inline]
pub fn pg_qsort<T: Copy>(v: &mut [T], mut cmp: impl FnMut(&T, &T) -> i32) {
    let n = v.len();
    // SAFETY: the pointer region is exactly v's n elements (core contract).
    let r: Result<(), Infallible> =
        unsafe { qsort_rec(v.as_mut_ptr(), n, &mut |a, b| Ok(cmp(a, b)), &mut || Ok(())) };
    match r {
        Ok(()) => {}
        Err(e) => match e {},
    }
}

/// C's qsort_arg with a fallible comparator (fmgr): the first comparator
/// error aborts the sort and is propagated; the slice is left as some valid
/// permutation of its input (as in C, where the error longjmps out).
#[inline]
pub fn pg_qsort_arg<T: Copy, E>(
    v: &mut [T],
    mut cmp: impl FnMut(&T, &T) -> Result<i32, E>,
) -> Result<(), E> {
    let n = v.len();
    // SAFETY: the pointer region is exactly v's n elements (core contract).
    unsafe { qsort_rec(v.as_mut_ptr(), n, &mut cmp, &mut || Ok(())) }
}

/// C's qsort_interruptible: infallible comparator, plus an interrupt check
/// (`check`) called exactly where ST_CHECK_FOR_INTERRUPTS sits in
/// lib/sort_template.h (partition-loop tails, presorted-scan iterations, and
/// each outer iteration). An `Err` from `check` aborts the sort and is
/// propagated; the slice is left as some valid permutation of its input.
#[inline]
pub fn pg_qsort_interruptible<T: Copy, E>(
    v: &mut [T],
    mut cmp: impl FnMut(&T, &T) -> i32,
    mut check: impl FnMut() -> Result<(), E>,
) -> Result<(), E> {
    let n = v.len();
    // SAFETY: the pointer region is exactly v's n elements (core contract).
    unsafe { qsort_rec(v.as_mut_ptr(), n, &mut |a, b| Ok(cmp(a, b)), &mut check) }
}

// SAFETY contract (all fns below): pointers are derived from one live
// &mut [T] region of n elements; every dereference stays inside it (the
// template's own invariant: pa..pd cursors are bounds-guarded by the same
// comparisons C uses). T: Copy, so ptr::swap needs no drop care.

// C's ST_MED3 is pg_noinline: "performance seems to be best if the
// comparator is inlined here, but the med3 function is not inlined in the
// qsort function" (lib/sort_template.h). Mirror that shape: the
// monomorphized comparator still inlines INTO med3.
#[inline(never)]
unsafe fn med3<T: Copy, E>(
    a: *mut T,
    b: *mut T,
    c: *mut T,
    cmp: &mut impl FnMut(&T, &T) -> Result<i32, E>,
) -> Result<*mut T, E> {
    unsafe {
        Ok(if cmp(&*a, &*b)? < 0 {
            if cmp(&*b, &*c)? < 0 {
                b
            } else if cmp(&*a, &*c)? < 0 {
                c
            } else {
                a
            }
        } else if cmp(&*b, &*c)? > 0 {
            b
        } else if cmp(&*a, &*c)? < 0 {
            a
        } else {
            c
        })
    }
}

unsafe fn swapn<T: Copy>(mut a: *mut T, mut b: *mut T, n: usize) {
    for _ in 0..n {
        unsafe {
            core::ptr::swap(a, b);
            a = a.add(1);
            b = b.add(1);
        }
    }
}

unsafe fn qsort_rec<T, E, C, K>(mut a: *mut T, mut n: usize, cmp: &mut C, chk: &mut K) -> Result<(), E>
where
    T: Copy,
    C: FnMut(&T, &T) -> Result<i32, E>,
    K: FnMut() -> Result<(), E>,
{
    unsafe {
        loop {
            chk()?;
            if n < 7 {
                let mut pm = a.add(1);
                while pm < a.add(n) {
                    let mut pl = pm;
                    while pl > a && cmp(&*pl.sub(1), &*pl)? > 0 {
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
                chk()?;
                if cmp(&*pm.sub(1), &*pm)? > 0 {
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
                    pl = med3(pl, pl.add(d), pl.add(2 * d), cmp)?;
                    pm = med3(pm.sub(d), pm, pm.add(d), cmp)?;
                    pn = med3(pn.sub(2 * d), pn.sub(d), pn, cmp)?;
                }
                pm = med3(pl, pm, pn, cmp)?;
            }
            core::ptr::swap(a, pm);
            let mut pa = a.add(1);
            let mut pb = pa;
            let mut pc = a.add(n - 1);
            let mut pd = pc;
            loop {
                while pb <= pc {
                    let r = cmp(&*pb, &*a)?;
                    if r > 0 {
                        break;
                    }
                    if r == 0 {
                        core::ptr::swap(pa, pb);
                        pa = pa.add(1);
                    }
                    pb = pb.add(1);
                    chk()?;
                }
                while pb <= pc {
                    let r = cmp(&*pc, &*a)?;
                    if r < 0 {
                        break;
                    }
                    if r == 0 {
                        core::ptr::swap(pc, pd);
                        pd = pd.sub(1);
                    }
                    pc = pc.sub(1);
                    chk()?;
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
                // Recurse on left partition, then iterate on right partition.
                if d1 > 1 {
                    qsort_rec(a, d1, cmp, chk)?;
                }
                if d2 > 1 {
                    a = pn.sub(d2);
                    n = d2;
                    continue;
                }
            } else {
                // Recurse on right partition, then iterate on left partition.
                if d2 > 1 {
                    qsort_rec(pn.sub(d2), d2, cmp, chk)?;
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

#[cfg(test)]
mod tests {
    use super::*;

    fn lcg(state: &mut u64) -> u64 {
        *state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        *state >> 33
    }

    /// Dup-heavy (key, original index) inputs across the n<7 / n>7 / n>40
    /// algorithm boundaries; comparator on key only so ties are real.
    fn cases() -> Vec<Vec<(i32, u32)>> {
        let mut out = Vec::new();
        let mut rng = 0x5eed_1234_u64;
        for n in [0usize, 1, 2, 5, 6, 7, 8, 13, 39, 40, 41, 64, 100, 500, 1000, 4096] {
            for &kmod in &[2u64, 7, 16, 1000] {
                out.push(
                    (0..n)
                        .map(|i| ((lcg(&mut rng) % kmod) as i32, i as u32))
                        .collect(),
                );
            }
            // Presorted and reverse-sorted classes.
            out.push((0..n).map(|i| ((i / 3) as i32, i as u32)).collect());
            out.push((0..n).map(|i| (-((i / 3) as i32), i as u32)).collect());
            // All-equal.
            out.push((0..n).map(|i| (42, i as u32)).collect());
        }
        out
    }

    #[test]
    fn sorts_correctly() {
        for case in cases() {
            let mut v = case.clone();
            pg_qsort(&mut v, |a, b| a.0 - b.0);
            let mut expect = case.clone();
            expect.sort_by_key(|e| e.0);
            assert_eq!(
                v.iter().map(|e| e.0).collect::<Vec<_>>(),
                expect.iter().map(|e| e.0).collect::<Vec<_>>()
            );
            // Output is a permutation of the input.
            let mut seen: Vec<u32> = v.iter().map(|e| e.1).collect();
            seen.sort_unstable();
            assert_eq!(seen, (0..case.len() as u32).collect::<Vec<_>>());
        }
    }

    #[test]
    fn three_entry_points_agree_bit_exactly() {
        for case in cases() {
            let mut a = case.clone();
            let mut b = case.clone();
            let mut c = case.clone();
            pg_qsort(&mut a, |x, y| x.0 - y.0);
            pg_qsort_arg(&mut b, |x, y| Ok::<i32, ()>(x.0 - y.0)).unwrap();
            pg_qsort_interruptible(&mut c, |x, y| x.0 - y.0, || Ok::<(), ()>(())).unwrap();
            assert_eq!(a, b);
            assert_eq!(a, c);
        }
    }

    #[test]
    fn fallible_comparator_error_aborts_with_valid_permutation() {
        let mut rng = 77u64;
        let orig: Vec<(i32, u32)> = (0..200).map(|i| ((lcg(&mut rng) % 5) as i32, i)).collect();
        for fail_at in [0usize, 1, 3, 17, 100] {
            let mut v = orig.clone();
            let mut calls = 0usize;
            let r = pg_qsort_arg(&mut v, |a, b| {
                if calls == fail_at {
                    return Err(fail_at);
                }
                calls += 1;
                Ok(a.0 - b.0)
            });
            assert_eq!(r, Err(fail_at));
            let mut seen: Vec<u32> = v.iter().map(|e| e.1).collect();
            seen.sort_unstable();
            assert_eq!(seen, (0..orig.len() as u32).collect::<Vec<_>>());
        }
    }

    #[test]
    fn interrupt_check_error_aborts() {
        let mut rng = 99u64;
        let mut v: Vec<(i32, u32)> = (0..500).map(|i| ((lcg(&mut rng) % 3) as i32, i)).collect();
        let mut checks = 0usize;
        let r = pg_qsort_interruptible(
            &mut v,
            |a, b| a.0 - b.0,
            || {
                checks += 1;
                if checks > 10 { Err("interrupted") } else { Ok(()) }
            },
        );
        assert_eq!(r, Err("interrupted"));
    }

    /// Intransitive comparator (the contrib-cube class): must terminate and
    /// return a permutation, never read out of bounds or panic.
    #[test]
    fn intransitive_comparator_is_safe() {
        let mut rng = 0xabcdef_u64;
        for n in [7usize, 41, 100, 1000] {
            let orig: Vec<(i32, u32)> =
                (0..n).map(|i| ((lcg(&mut rng) % 4) as i32, i as u32)).collect();
            let mut v = orig.clone();
            // Rock-paper-scissors on key % 3: cyclic, intransitive.
            pg_qsort(&mut v, |a, b| {
                let (x, y) = (a.0.rem_euclid(3), b.0.rem_euclid(3));
                if x == y { 0 } else if (x + 1) % 3 == y { -1 } else { 1 }
            });
            let mut seen: Vec<u32> = v.iter().map(|e| e.1).collect();
            seen.sort_unstable();
            assert_eq!(seen, (0..n as u32).collect::<Vec<_>>());
        }
    }
}
