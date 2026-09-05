use std::rc::Rc;

use datum::Datum;
use types_core::Oid;
use types_error::{PgError, PgResult};
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
            return Err(missing_lt_opr(typid));
        }
        let cmp = entry.cmp_proc_finfo().clone();
        self.dims.push(SortDim { entry, cmp, collation });
        Ok(())
    }

    // ApplySortComparator (sortsupport.h): nulls sort last, forward order.
    // The comparator is an fmgr call (extended_stats.c:881 multi_sort_compare,
    // mcv.c:471 sort_item_compare): a user-defined btree support function
    // can raise, and that ereport propagates as the Err (C longjmps out of
    // the sort); it must never become a panic.
    pub fn compare_dim(
        &mut self,
        dim: usize,
        a: Datum,
        an: bool,
        b: Datum,
        bn: bool,
    ) -> PgResult<i32> {
        if an {
            if bn {
                return Ok(0);
            }
            return Ok(1);
        }
        if bn {
            return Ok(-1);
        }
        let d = &mut self.dims[dim];
        // Comparators (numeric_cmp etc.) detoast by-ref args through the
        // result mcx; call-lifetime scratch (ANALYZE cold path).
        let scratch = ::mcx::MemoryContext::new("multi_sort compare_dim");
        Ok(types_fmgr::function_call2_coll_in(&mut d.cmp, d.collation, scratch.mcx(), a, b)?
            .as_i32())
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

    // multi_sort_compare (extended_stats.c:872).
    pub fn compare(&self, mss: &mut MultiSort, a: SortItem, b: SortItem) -> PgResult<i32> {
        for dim in 0..mss.dims.len() {
            let (av, an) = self.value(a, dim);
            let (bv, bn) = self.value(b, dim);
            let c = mss.compare_dim(dim, av, an, bv, bn)?;
            if c != 0 {
                return Ok(c);
            }
        }
        Ok(0)
    }

    pub fn compare_dims(
        &self,
        mss: &mut MultiSort,
        start: usize,
        end: usize,
        a: SortItem,
        b: SortItem,
    ) -> PgResult<i32> {
        for dim in start..=end {
            let (av, an) = self.value(a, dim);
            let (bv, bn) = self.value(b, dim);
            let c = mss.compare_dim(dim, av, an, bv, bn)?;
            if c != 0 {
                return Ok(c);
            }
        }
        Ok(0)
    }
}

// CHECK_FOR_INTERRUPTS() (miscadmin.h): the InterruptPending fast path, then
// ProcessInterrupts through the tcop seam (a raised cancel/die is the Err).
pub fn check_for_interrupts() -> PgResult<()> {
    if init_small::globals::InterruptPending() {
        return postgres_seams::check_for_interrupts::call();
    }
    Ok(())
}

// port/qsort.c (Bentley & McIlroy), exact algorithm: equal-key output order
// is a byte-format parity requirement for the serialized statistics.
// Canonical shared port: crates/_support/pg_qsort.
//
// Every sort in this crate is C's qsort_interruptible (extended_stats.c:1110
// build_sorted_items, mvdistinct.c:491, mcv.c:456/527/695): a long sort of a
// large multi-column sample must answer a query cancel. The two entry points:
//
// - `qsort_interruptible`: infallible comparator, the interrupt check placed
//   exactly at lib/sort_template.h's ST_CHECK_FOR_INTERRUPTS points
//   (mcv.c:456 compare_sort_item_count).
// - `qsort_interruptible_arg`: fmgr comparator that can raise (multi_sort_compare,
//   sort_item_compare, compare_datums_simple). The first comparator error
//   aborts the sort and propagates, as C's ereport longjmps out. The
//   CHECK_FOR_INTERRUPTS rides the comparator call: it runs at least at every
//   template check point (each of which is adjacent to a comparison), never
//   touches the data, and so leaves the permutation pg_qsort-exact; the
//   InterruptPending fast path makes the extra checks a thread-local load.
pub fn qsort_interruptible<T: Copy>(
    v: &mut [T],
    cmp: impl FnMut(&T, &T) -> i32,
) -> PgResult<()> {
    ::pg_qsort::pg_qsort_interruptible(v, cmp, check_for_interrupts)
}

pub fn qsort_interruptible_arg<T: Copy>(
    v: &mut [T],
    mut cmp: impl FnMut(&T, &T) -> PgResult<i32>,
) -> PgResult<()> {
    ::pg_qsort::pg_qsort_arg(v, |a, b| {
        check_for_interrupts()?;
        cmp(a, b)
    })
}

fn missing_lt_opr(typid: Oid) -> Box<PgError> {
    PgError::error(format!(
        "cache lookup failed for ordering operator for type {typid}"
    ))
    .into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use types_error::ERRCODE_INTERNAL_ERROR;

    // A comparator that raises (a user-defined btree support function) must
    // abort the sort with its own error, as C's ereport longjmps out of
    // qsort_interruptible; pre-fix the port panicked in compare_dim.
    #[test]
    fn comparator_error_propagates_out_of_the_sort() {
        let mut v = [5u32, 3, 9, 1, 7, 2, 8, 6, 4];
        let e = qsort_interruptible_arg(&mut v, |a, b| {
            if *a == 9 || *b == 9 {
                return Err(PgError::error("comparator raised").into());
            }
            Ok((*a as i64 - *b as i64).signum() as i32)
        })
        .unwrap_err();
        assert_eq!(e.message(), "comparator raised");
        let mut w = [5u32, 3, 9, 1, 7, 2, 8, 6, 4];
        qsort_interruptible_arg(&mut w, |a, b| Ok((*a as i64 - *b as i64).signum() as i32))
            .unwrap();
        assert_eq!(w, [1, 2, 3, 4, 5, 6, 7, 8, 9]);
    }

    // qsort_interruptible: a pending interrupt (query cancel) is answered
    // from inside the sort, through the CHECK_FOR_INTERRUPTS seam.
    #[test]
    fn pending_interrupt_aborts_the_sort() {
        fn cancel() -> PgResult<()> {
            init_small::globals::SetInterruptPending(false);
            Err(PgError::error("canceling statement due to user request").into())
        }
        if !postgres_seams::check_for_interrupts::is_installed() {
            postgres_seams::check_for_interrupts::set(cancel);
        }
        let mut v = [5u32, 3, 9, 1, 7, 2, 8, 6, 4];
        init_small::globals::SetInterruptPending(true);
        let e = qsort_interruptible_arg(&mut v, |a, b| Ok((*a as i64 - *b as i64).signum() as i32))
            .unwrap_err();
        assert_eq!(e.message(), "canceling statement due to user request");
        init_small::globals::SetInterruptPending(true);
        let e = qsort_interruptible(&mut v, |a, b| (*a as i64 - *b as i64).signum() as i32)
            .unwrap_err();
        assert_eq!(e.message(), "canceling statement due to user request");
        assert!(!init_small::globals::InterruptPending());
        qsort_interruptible(&mut v, |a, b| (*a as i64 - *b as i64).signum() as i32).unwrap();
        assert_eq!(v, [1, 2, 3, 4, 5, 6, 7, 8, 9]);
    }

    #[test]
    fn json_missing_lt_opr_is_ereport_xx000() {
        let e = missing_lt_opr(types_core::JSONOID);
        assert_eq!(e.sqlstate(), ERRCODE_INTERNAL_ERROR);
        assert_eq!(
            e.message(),
            "cache lookup failed for ordering operator for type 114"
        );
    }
}
