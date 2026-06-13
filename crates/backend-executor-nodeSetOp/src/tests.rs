//! Unit tests for the pure, seam-free logic of nodeSetOp: the `ApplySortComparator`
//! null/reverse arithmetic (the non-null path dispatches through a seam and is
//! not exercised here).

use super::apply_sort_comparator;
use mcx::MemoryContext;
use types_sortsupport::SortSupportData;
use types_datum::Datum;

const D: Datum = Datum::null();

fn ssup(nulls_first: bool, reverse: bool) -> SortSupportData<'static> {
    // Leak a context so the borrowed Mcx is 'static for the test.
    let ctx: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("test")));
    let mut s = SortSupportData::new(ctx.mcx());
    s.ssup_nulls_first = nulls_first;
    s.ssup_reverse = reverse;
    s
}

#[test]
fn null_vs_null_is_equal() {
    let s = ssup(false, false);
    assert_eq!(apply_sort_comparator(D, true, D, true, &s).unwrap(), 0);
    let s = ssup(true, false);
    assert_eq!(apply_sort_comparator(D, true, D, true, &s).unwrap(), 0);
}

#[test]
fn null_vs_notnull_respects_nulls_first() {
    // nulls_last (default): NULL ">" NOT_NULL  => +1
    let s = ssup(false, false);
    assert_eq!(apply_sort_comparator(D, true, D, false, &s).unwrap(), 1);
    // nulls_first: NULL "<" NOT_NULL => -1
    let s = ssup(true, false);
    assert_eq!(apply_sort_comparator(D, true, D, false, &s).unwrap(), -1);
}

#[test]
fn notnull_vs_null_respects_nulls_first() {
    // nulls_last: NOT_NULL "<" NULL => -1
    let s = ssup(false, false);
    assert_eq!(apply_sort_comparator(D, false, D, true, &s).unwrap(), -1);
    // nulls_first: NOT_NULL ">" NULL => +1
    let s = ssup(true, false);
    assert_eq!(apply_sort_comparator(D, false, D, true, &s).unwrap(), 1);
}
