//! Seam declarations for the `backend-utils-sort-sortsupport` unit
//! (`utils/sort/sortsupport.c`): the two ways a caller installs a comparator
//! into a `SortSupportData`, plus invoking an installed comparator.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly. The resolved comparator is held as a
//! [`::types_sortsupport::SortComparatorId`] token the owner interprets.

#![allow(non_snake_case)]

use ::types_core::Oid;
use ::types_error::PgResult;
use ::rel::Relation;
use ::types_sortsupport::SortSupportData;
// Canonical value type (`Datum<'mcx>`: `ByVal` word / `ByRef` bytes). The
// comparator-invocation operands of `apply_sort_comparator` carry this canonical
// carrier with `'mcx` threaded (a by-reference sort key crosses as `ByRef`), not
// the bare-word shim.
use ::types_tuple::Datum;

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
    /// `ssup->comparator(datum1, datum2, ssup)` (sortsupport.h): invoke the
    /// installed comparator (identified by the `SortComparatorId` carried in
    /// `ssup.comparator`) on two non-null datums, returning `<0`/`0`/`>0`. The
    /// caller has already verified `ssup.comparator.is_some()`. `Err` carries
    /// the comparison function's `ereport(ERROR)`s.
    ///
    /// The two operands are the canonical `Datum<'mcx>` carrier (a `ByVal`
    /// scalar word or a `ByRef` detoasted image) — a by-reference sort key
    /// (e.g. text) crosses here as `ByRef`, so this is NOT a bare-word edge:
    /// the carrier is threaded through rather than collapsed to a machine word.
    ///
    /// The operands are passed BY REFERENCE: a sort comparison only READS the two
    /// datums (C passes the raw `Datum` word), so the comparator does not need an
    /// owned copy. Borrowing here removes the per-comparison `Datum::clone_in`
    /// (and, for a by-ref key, the `mcx::slice_in`) the by-value form forced on
    /// the O(n log n) `comparetup` hot path.
    pub fn apply_sort_comparator(
        datum1: &Datum<'_>,
        datum2: &Datum<'_>,
        ssup: &SortSupportData<'_>,
    ) -> PgResult<i32>
);

seam_core::seam!(
    /// `PrepareSortSupportFromOrderingOp(orderingOp, ssup)` (sortsupport.c):
    /// fill in `ssup` (sets `ssup_reverse` and resolves/installs the type's
    /// comparator) from the ordering operator `orderingOp` (a "<" or ">" btree
    /// operator). Catalog lookups and comparator setup allocate / can
    /// `ereport(ERROR)`, hence `PgResult`. `ssup.ssup_cxt` selects the context
    /// the comparator state is built in.
    pub fn prepare_sort_support_from_ordering_op(
        ordering_op: Oid,
        ssup: &mut SortSupportData<'_>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `PrepareSortSupportFromIndexRel(indexRel, reverse, ssup)` (sortsupport.c):
    /// fill in `ssup` from a btree (amcanorder) index relation and the
    /// `ssup_attno` column already stored in `ssup`. Sets `ssup_reverse` from
    /// `reverse` and resolves/installs the comparator from the index column's
    /// opfamily/opcintype. Catalog lookups + comparator setup can
    /// `ereport(ERROR)`, hence `PgResult`. `tuplesort` uses this for an
    /// index-ordered btree sort.
    pub fn prepare_sort_support_from_index_rel(
        index_rel: &Relation<'_>,
        reverse: bool,
        ssup: &mut SortSupportData<'_>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `PrepareSortSupportFromGistIndexRel(indexRel, ssup)` (sortsupport.c): fill
    /// in `ssup` from a GiST index relation and the `ssup_attno` column already
    /// stored in `ssup`. Sets `ssup_reverse = false` and installs the type's
    /// GiST sort-support comparator (no old-style btree shim fallback). Fallible
    /// on catalog lookups / comparator setup.
    pub fn prepare_sort_support_from_gist_index_rel(
        index_rel: &Relation<'_>,
        ssup: &mut SortSupportData<'_>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `ssup->abbrev_converter(original, ssup)` (sortsupport.h): invoke the
    /// installed abbreviation converter (identified by the `AbbrevConverterId`
    /// carried in `ssup.abbrev_converter`) on the original (NOT NULL,
    /// pass-by-reference) datum, returning the pass-by-value abbreviated-key
    /// `Datum`. The caller has verified `ssup.abbrev_converter.is_some()`. `Err`
    /// carries the converter's `ereport(ERROR)`s.
    pub fn apply_sort_abbrev_converter(
        original: Datum<'_>,
        ssup: &SortSupportData<'_>,
    ) -> PgResult<Datum<'static>>
);

seam_core::seam!(
    /// `ssup->abbrev_abort(memtupcount, ssup)` (sortsupport.h): poll the
    /// installed abort-abbreviation cost-model callback (identified by the
    /// `AbbrevAbortId` carried in `ssup.abbrev_abort`), returning whether the
    /// sort should abandon abbreviation. The caller has verified
    /// `ssup.abbrev_abort.is_some()`. `Err` carries any `ereport(ERROR)`.
    pub fn apply_sort_abbrev_abort(
        memtupcount: i32,
        ssup: &mut SortSupportData<'_>,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `ssup->abbrev_full_comparator(x, y, ssup)` (sortsupport.h,
    /// `ApplySortAbbrevFullComparator` non-null dispatch): invoke the full,
    /// authoritative comparator (identified by the `SortComparatorId` carried in
    /// `ssup.abbrev_full_comparator`) on two non-null full-representation datums,
    /// used when an abbreviated comparison was inconclusive. The caller has
    /// verified `ssup.abbrev_full_comparator.is_some()`. `Err` carries the
    /// comparator's `ereport(ERROR)`s.
    ///
    /// Operands are passed BY REFERENCE (read-only comparison; see
    /// [`apply_sort_comparator`]).
    pub fn apply_sort_abbrev_full_comparator(
        datum1: &Datum<'_>,
        datum2: &Datum<'_>,
        ssup: &SortSupportData<'_>,
    ) -> PgResult<i32>
);
