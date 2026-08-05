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
// Canonical shared port: crates/_support/pg_qsort.
pub use ::pg_qsort::pg_qsort;
