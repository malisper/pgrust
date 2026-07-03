use datum::Datum;
use types_core::Oid;
use types_core::fmgr::FmgrInfo;
use types_error::PgResult;

pub struct SortDim {
    pub cmp: FmgrInfo,
    pub collation: Oid,
}

pub struct MultiSort<'mcx> {
    pub dims: mcx::PgVec<'mcx, SortDim>,
}

impl<'mcx> MultiSort<'mcx> {
    pub fn init(mcx: mcx::Mcx<'mcx>, ndims: usize) -> PgResult<MultiSort<'mcx>> {
        Ok(MultiSort { dims: mcx::vec_with_capacity_in(mcx, ndims)? })
    }

    pub fn add_dimension(&mut self, typid: Oid, collation: Oid) -> PgResult<()> {
        let entry = typcache::lookup_type_cache(
            typid,
            typcache::TYPECACHE_LT_OPR | typcache::TYPECACHE_CMP_PROC_FINFO,
        )?;
        if entry.lt_opr() == types_core::InvalidOid {
            panic!("cache lookup failed for ordering operator for type {typid}");
        }
        self.dims.push(SortDim { cmp: entry.cmp_proc_finfo().clone(), collation });
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
        types_fmgr::function_call2_coll(&mut d.cmp, d.collation, a, b)
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

    pub fn compare(&self, mss: &mut MultiSort<'_>, a: SortItem, b: SortItem) -> i32 {
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
        mss: &mut MultiSort<'_>,
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

// port/qsort.c (Bentley & McIlroy), exact algorithm: equal-key output order is
// a byte-format parity requirement for the serialized statistics.
pub fn pg_qsort<T: Copy, C: FnMut(&T, &T) -> i32>(a: &mut [T], mut cmp: C) {
    if a.len() > 1 {
        qsort_rec(a, &mut cmp);
    }
}

fn qsort_rec<T: Copy, C: FnMut(&T, &T) -> i32>(mut a: &mut [T], cmp: &mut C) {
    loop {
        let n = a.len();
        if n < 7 {
            for pm in 1..n {
                let mut pl = pm;
                while pl > 0 && cmp(&a[pl - 1], &a[pl]) > 0 {
                    a.swap(pl, pl - 1);
                    pl -= 1;
                }
            }
            return;
        }
        let mut presorted = true;
        for pm in 1..n {
            if cmp(&a[pm - 1], &a[pm]) > 0 {
                presorted = false;
                break;
            }
        }
        if presorted {
            return;
        }
        let mut pm = n / 2;
        {
            let mut pl = 0usize;
            let mut pn = n - 1;
            if n > 40 {
                let d = n / 8;
                pl = med3(a, pl, pl + d, pl + 2 * d, cmp);
                pm = med3(a, pm - d, pm, pm + d, cmp);
                pn = med3(a, pn - 2 * d, pn - d, pn, cmp);
            }
            pm = med3(a, pl, pm, pn, cmp);
        }
        a.swap(0, pm);
        let mut pa = 1usize;
        let mut pb = 1usize;
        let mut pc = n - 1;
        let mut pd = n - 1;
        loop {
            while pb <= pc {
                let r = cmp(&a[pb], &a[0]);
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
                let r = cmp(&a[pc], &a[0]);
                if r < 0 {
                    break;
                }
                if r == 0 {
                    a.swap(pc, pd);
                    pd -= 1;
                }
                if pc == 0 {
                    break;
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
        let mut d1 = pa.min(pb - pa);
        swapn(a, 0, pb - d1, d1);
        d1 = (pd - pc).min(n - pd - 1);
        swapn(a, pb, n - d1, d1);
        d1 = pb - pa;
        let d2 = pd - pc;
        if d1 <= d2 {
            if d1 > 1 {
                let (lo, _) = a.split_at_mut(d1);
                qsort_rec(lo, cmp);
            }
            if d2 > 1 {
                let start = n - d2;
                a = &mut a[start..];
                continue;
            }
        } else {
            if d2 > 1 {
                let start = n - d2;
                let (_, hi) = a.split_at_mut(start);
                qsort_rec(hi, cmp);
            }
            if d1 > 1 {
                a = &mut a[..d1];
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
