//! Sort-support comparison vocabulary (`utils/sortsupport.h`), trimmed to the
//! fields the merge-join executor consumes.
//!
//! The C `SortSupportData` carries the resolved comparison function pointer
//! `comparator` plus the abbreviation hooks. In the owned model the comparator
//! (and the abbreviation converter/abort) is a function the catalog/fmgr layer
//! resolves and installs; that owner is not ported, so the comparator is held
//! as a `Copy` token ([`SortComparatorId`]) the installing unit hands back and
//! interprets when invoked through its seam. The merge-join `ApplySortComparator`
//! null/reverse arithmetic reads only the data fields below
//! (`ssup_collation`/`ssup_reverse`/`ssup_nulls_first`) and dispatches the
//! non-null comparison through that token.
//!
//! The three abbreviated-key hooks (`abbrev_converter`, `abbrev_abort`,
//! `abbrev_full_comparator`) follow the same token model: each is a distinct
//! `Copy` token ([`AbbrevConverterId`] / [`AbbrevAbortId`], and a
//! [`SortComparatorId`] for the full comparator since it has the same
//! `int (*)(Datum, Datum, SortSupport)` signature as `comparator`) the
//! sortsupport / tuplesort substrate hands back and interprets through its
//! abbreviation seams. `None` is the C `NULL` (hook not installed).

#![no_std]

use mcx::Mcx;
use types_core::primitive::{AttrNumber, Oid};
use datum::Datum;

/// `int BTORDER_PROC` (access/nbtree.h) — opfamily support-function number of
/// the btree comparison (`cmp`) function.
pub const BTORDER_PROC: i16 = 1;
/// `int BTSORTSUPPORT_PROC` (access/nbtree.h) — opfamily support-function
/// number of the optional sortsupport function.
pub const BTSORTSUPPORT_PROC: i16 = 2;

/// `int COMPARE_EQ` (access/cmptype.h) — the equality comparison type, the
/// value `IndexAmTranslateStrategy` returns for a btree equality strategy.
pub const COMPARE_EQ: i32 = 3;

/// `int COMPARE_GT` (access/cmptype.h) — the greater-than comparison type; an
/// ordering operator with this comparison type sorts descending.
pub const COMPARE_GT: i32 = 5;

/// `int GIST_SORTSUPPORT_PROC` (access/gist.h) — opclass support-function
/// number of the optional GiST sort-support function.
pub const GIST_SORTSUPPORT_PROC: i16 = 11;

/// `Oid GIST_AM_OID` (catalog/pg_am.dat) — the OID of the `gist` access
/// method.
pub const GIST_AM_OID: Oid = 783;

/// The resolved `comparator` function pointer in the owned model: a `Copy`
/// token the sortsupport/fmgr owner hands back from
/// `PrepareSortSupportComparisonShim` / `OidFunctionCall1(BTSORTSUPPORT_PROC)`
/// and interprets when its comparator-call seam runs. `None` is the C
/// `comparator == NULL` (no comparator installed yet).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SortComparatorId(pub u32);

/// The resolved `abbrev_converter` function pointer in the owned model
/// (C's `Datum (*)(Datum original, SortSupport ssup)`): a `Copy` token the
/// abbreviation-providing unit (`varstr_abbrev_convert`, `numeric_abbrev_convert`,
/// ...) hands back through its install seam and the sort substrate interprets
/// when it converts an original Datum to its abbreviated key. `None` is the C
/// `abbrev_converter == NULL` (no converter installed — abbreviation not in
/// play).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct AbbrevConverterId(pub u32);

/// The resolved `abbrev_abort` function pointer in the owned model (C's
/// `bool (*)(int memtupcount, SortSupport ssup)`): a `Copy` token the
/// abbreviation-providing unit hands back through its install seam and the sort
/// substrate interprets when it polls whether to abandon abbreviation. `None`
/// is the C `abbrev_abort == NULL`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct AbbrevAbortId(pub u32);

/// `SortSupportData` (utils/sortsupport.h).
///
/// The `ssup_extra` workspace field is owned/filled by the comparator-providing
/// unit and not carried (the owner keeps that scratch state keyed by the
/// comparator/abbrev tokens). The `comparator` is the [`SortComparatorId`] token
/// described above; the three abbreviated-key hooks are the
/// [`AbbrevConverterId`] / [`AbbrevAbortId`] tokens plus a [`SortComparatorId`]
/// for the full comparator (same signature as `comparator`), each `None` when
/// the C field is `NULL`.
#[derive(Clone, Copy, Debug)]
pub struct SortSupportData<'mcx> {
    /// `MemoryContext ssup_cxt` — memory context holding any working state of
    /// the support functions (set to the caller's current context at setup).
    pub ssup_cxt: Mcx<'mcx>,
    /// `Oid ssup_collation` — collation to use for comparisons.
    pub ssup_collation: Oid,
    /// `bool ssup_reverse` — descending-order sort?
    pub ssup_reverse: bool,
    /// `bool ssup_nulls_first` — sort nulls first?
    pub ssup_nulls_first: bool,
    /// `AttrNumber ssup_attno` — column number being sorted (not used by merge
    /// join, but part of the struct).
    pub ssup_attno: AttrNumber,
    /// `bool abbreviate` — whether the abbreviation optimization is applicable.
    /// Always false for merge joins.
    pub abbreviate: bool,
    /// `int (*comparator)(...)` — the resolved comparison function, held as a
    /// `Copy` token the comparator owner interprets. `None` = C `NULL`.
    ///
    /// This may be either the authoritative comparator or the abbreviated
    /// comparator (when abbreviation is in play, the sortsupport routine moves
    /// the authoritative comparator to `abbrev_full_comparator` and installs the
    /// cheap abbreviated comparator here).
    pub comparator: Option<SortComparatorId>,
    /// `Datum (*abbrev_converter)(Datum original, SortSupport ssup)` — the
    /// converter to abbreviated format, held as a `Copy` token. `None` = C
    /// `NULL` (abbreviation not in play). Tested by core code to decide whether
    /// abbreviation should proceed.
    pub abbrev_converter: Option<AbbrevConverterId>,
    /// `bool (*abbrev_abort)(int memtupcount, SortSupport ssup)` — the
    /// abort-abbreviation cost-model callback, held as a `Copy` token. `None` =
    /// C `NULL`.
    pub abbrev_abort: Option<AbbrevAbortId>,
    /// `int (*abbrev_full_comparator)(Datum x, Datum y, SortSupport ssup)` — the
    /// full, authoritative comparator used when an abbreviated comparison was
    /// inconclusive (or to replace `comparator` if core decides against
    /// abbreviation). Same signature as `comparator`, so held as a
    /// [`SortComparatorId`] token. `None` = C `NULL`.
    pub abbrev_full_comparator: Option<SortComparatorId>,
}

impl<'mcx> SortSupportData<'mcx> {
    /// A zeroed `SortSupportData` with its context set to `mcx`, equivalent to
    /// the C `palloc0` plus `ssup.ssup_cxt = CurrentMemoryContext`.
    pub fn new(mcx: Mcx<'mcx>) -> Self {
        SortSupportData {
            ssup_cxt: mcx,
            ssup_collation: 0,
            ssup_reverse: false,
            ssup_nulls_first: false,
            ssup_attno: 0,
            abbreviate: false,
            comparator: None,
            abbrev_converter: None,
            abbrev_abort: None,
            abbrev_full_comparator: None,
        }
    }
}

/// The resolved `decrement` / `increment` function pointer in the owned model
/// (C's `SkipSupportIncDec`, a `Datum (*)(Relation, Datum, bool *overflow)`): a
/// `Copy` token the skip-support owner (`PrepareSkipSupportFromOpclass`, not yet
/// ported) hands back / interprets when it runs the increment / decrement.
/// `None` is the C `decrement == NULL` / `increment == NULL` (no callback
/// installed yet — a `BTSKIPSUPPORT_PROC` must set both).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SkipSupportIncDecId(pub u32);

/// `SkipSupportData` (`utils/skipsupport.h`).
///
/// State/callbacks used by skip arrays to procedurally generate elements. A
/// `BTSKIPSUPPORT_PROC` function must set each and every field when called
/// (there are no optional fields).
///
/// `low_elem` / `high_elem` are plain boundary [`Datum`]s the strategy routine
/// computes and stores directly. The C `decrement` / `increment` function
/// pointers are held as [`SkipSupportIncDecId`] tokens the skip-support owner
/// interprets (the owner is not yet ported; the install path is a seam).
#[derive(Clone, Copy, Debug)]
pub struct SkipSupportData {
    /// `Datum low_elem` — lowest sorting / leftmost non-NULL value (assuming
    /// ascending order).
    pub low_elem: Datum,
    /// `Datum high_elem` — highest sorting / rightmost non-NULL value.
    pub high_elem: Datum,
    /// `SkipSupportIncDec decrement` — returns a decremented copy of the
    /// caller's datum (or sets `*overflow` at `low_elem`). Held as a token.
    pub decrement: Option<SkipSupportIncDecId>,
    /// `SkipSupportIncDec increment` — returns an incremented copy of the
    /// caller's datum (or sets `*overflow` at `high_elem`). Held as a token.
    pub increment: Option<SkipSupportIncDecId>,
}

impl SkipSupportData {
    /// A zeroed `SkipSupportData` (the C `palloc0` of a `SkipSupportData` before
    /// the `BTSKIPSUPPORT_PROC` fills every field).
    pub fn new() -> Self {
        SkipSupportData {
            low_elem: Datum::null(),
            high_elem: Datum::null(),
            decrement: None,
            increment: None,
        }
    }
}

impl Default for SkipSupportData {
    fn default() -> Self {
        Self::new()
    }
}
