// lib/sort_template.h (Bentley & McIlroy) over SortTuple, in C's exact
// pointer shape. The exact algorithm is a parity requirement: equal-key
// output order must match C's qsort. One monomorphization per comparator
// call site = ST_DEFINE. Canonical shared port: crates/_support/pg_qsort;
// qsort_tuple is C's qsort_interruptible instantiation (ST_CHECK_FOR_
// INTERRUPTS), with cfi() at the template's exact check points.

use ::types_error::PgResult;

use crate::cfi;
use crate::SortTuple;

pub(crate) fn qsort_tuple<C: Fn(&SortTuple, &SortTuple) -> i32 + Copy>(
    data: &mut [SortTuple],
    cmp: C,
) -> PgResult<()> {
    ::pg_qsort::pg_qsort_interruptible(data, cmp, cfi)
}
