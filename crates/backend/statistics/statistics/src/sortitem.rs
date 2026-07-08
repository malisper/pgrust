use std::rc::Rc;

use datum::Datum;
use types_core::Oid;
use types_error::PgResult;
use typcache::TypeCacheEntry;
use types_fmgr::FmgrInfo;

// `cmp` is a copy of the entry's cmp_proc_finfo: comparators may re-enter
// typcache (range_cmp/record_cmp fn_extra fills), so the entry's RefCell must
// stay unborrowed across the call.
pub struct SortDim {
    pub entry: Rc<TypeCacheEntry>,
    pub cmp: FmgrInfo,
    pub collation: Oid,
}

// Rc payloads (typcache pins) can't live in arena vecs; std Vec justified:
// bounded by ndims (<= 8), ANALYZE/planner cold path.
pub struct MultiSort {
    pub dims: Vec<SortDim>,
}

impl MultiSort {
    pub fn init(ndims: usize) -> MultiSort {
        MultiSort { dims: Vec::with_capacity(ndims) }
    }

    pub fn add_dimension(&mut self, typid: Oid, collation: Oid) -> PgResult<()> {
        let entry = typcache::lookup_type_cache(
            typid,
            typcache::TYPECACHE_LT_OPR | typcache::TYPECACHE_CMP_PROC_FINFO,
        )?;
        if entry.lt_opr() == types_core::InvalidOid {
            panic!("cache lookup failed for ordering operator for type {typid}");
        }
        let cmp = entry.cmp_proc_finfo().clone();
        self.dims.push(SortDim { entry, cmp, collation });
        Ok(())
    }

    // ApplySortComparator (sortsupport.h): nulls sort last, forward order.
    pub fn compare_dim(&mut self, dim: usize, a: Datum, an: bool, b: Datum, bn: bool) -> i32 {
        if an {
            if bn {
                return 0;
            }
            return 1;
        }
        if bn {
            return -1;
        }
        let d = &mut self.dims[dim];
        // Comparators (numeric_cmp etc.) detoast by-ref args through the
        // result mcx; call-lifetime scratch (ANALYZE cold path).
        let scratch = ::mcx::MemoryContext::new("multi_sort compare_dim");
        types_fmgr::function_call2_coll_in(&mut d.cmp, d.collation, scratch.mcx(), a, b)
            .unwrap_or_else(|e| panic!("multi_sort_compare: comparison failed: {e:?}"))
            .as_i32()
    }
}

// SortItem (extended_stats_internal.h): the row's values live in flat arrays
// owned by SortItems; `off` is the row slot the item currently labels.
#[derive(Clone, Copy)]
pub struct SortItem {
    pub off: u32,
    pub count: i32,
}

pub struct ItemStore<'mcx> {
    pub values: mcx::PgVec<'mcx, Datum>,
    pub isnull: mcx::PgVec<'mcx, bool>,
    pub width: usize,
}

impl<'mcx> ItemStore<'mcx> {
    #[inline]
    pub fn value(&self, item: SortItem, dim: usize) -> (Datum, bool) {
        let i = item.off as usize * self.width + dim;
        (self.values[i], self.isnull[i])
    }

    pub fn compare(&self, mss: &mut MultiSort, a: SortItem, b: SortItem) -> i32 {
        for dim in 0..mss.dims.len() {
            let (av, an) = self.value(a, dim);
            let (bv, bn) = self.value(b, dim);
            let c = mss.compare_dim(dim, av, an, bv, bn);
            if c != 0 {
                return c;
            }
        }
        0
    }

    pub fn compare_dims(
        &self,
        mss: &mut MultiSort,
        start: usize,
        end: usize,
        a: SortItem,
        b: SortItem,
    ) -> i32 {
        for dim in start..=end {
            let (av, an) = self.value(a, dim);
            let (bv, bn) = self.value(b, dim);
            let c = mss.compare_dim(dim, av, an, bv, bn);
            if c != 0 {
                return c;
            }
        }
        0
    }
}

// port/qsort.c (Bentley & McIlroy), exact algorithm: equal-key output order
// is a byte-format parity requirement for the serialized statistics.
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
