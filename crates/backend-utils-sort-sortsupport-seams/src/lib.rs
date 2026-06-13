//! Seam declarations for the `backend-utils-sort-sortsupport` unit
//! (`utils/sort/sortsupport.c`): the two ways a caller installs a comparator
//! into a `SortSupportData`, plus invoking an installed comparator.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly. The resolved comparator is held as a
//! [`types_sortsupport::SortComparatorId`] token the owner interprets.

#![allow(non_snake_case)]

use types_core::Oid;
use types_datum::Datum;
use types_error::PgResult;
use types_sortsupport::SortSupportData;

seam_core::seam!(
    /// `OidFunctionCall1(sortfunc, PointerGetDatum(&ssup))` for a
    /// `BTSORTSUPPORT_PROC` function (sortsupport.c usage in `MJExamineQuals`):
    /// invoke the type's sort-support function, which may fill `ssup.comparator`
    /// (returning a token) or leave it unset (`Ok(None)`, the C `comparator ==
    /// NULL` after the call). `Err` carries the fmgr `ereport(ERROR)`s.
    pub fn oid_function_call_1_sortsupport(
        sortfunc: Oid,
        ssup: &mut SortSupportData<'_>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `PrepareSortSupportComparisonShim(cmpFunc, ssup)` (sortsupport.c): set up
    /// `ssup.comparator` to a shim that calls the old-style btree comparison
    /// function `cmpFunc`. Allocates the shim state in `ssup.ssup_cxt`; fallible
    /// on OOM / fmgr `ereport(ERROR)`.
    pub fn prepare_sort_support_comparison_shim(
        cmp_func: Oid,
        ssup: &mut SortSupportData<'_>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `PrepareSortSupportFromOrderingOp(orderingOp, ssup)` (sortsupport.c):
    /// fill in `ssup` (setting `ssup_reverse` and installing `ssup.comparator`)
    /// for the given btree ordering operator. `elog(ERROR)` when `orderingOp`
    /// is not a valid ordering operator (carried on `Err`). Allocates the
    /// comparator state in `ssup.ssup_cxt`.
    pub fn prepare_sort_support_from_ordering_op(
        ordering_op: Oid,
        ssup: &mut SortSupportData<'_>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `ssup->comparator(datum1, datum2, ssup)` (sortsupport.h): invoke the
    /// installed comparator (identified by the `SortComparatorId` carried in
    /// `ssup.comparator`) on two non-null datums, returning `<0`/`0`/`>0`. The
    /// caller has already verified `ssup.comparator.is_some()`. `Err` carries
    /// the comparison function's `ereport(ERROR)`s.
    pub fn apply_sort_comparator(
        datum1: Datum,
        datum2: Datum,
        ssup: &SortSupportData<'_>,
    ) -> PgResult<i32>
);
