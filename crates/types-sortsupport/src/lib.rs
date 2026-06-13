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

#![no_std]

use mcx::Mcx;
use types_core::primitive::{AttrNumber, Oid};

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

/// `SortSupportData` (utils/sortsupport.h), trimmed.
///
/// The `ssup_extra`, `abbrev_converter`, `abbrev_abort`, and
/// `abbrev_full_comparator` fields are owned/filled by the comparator-providing
/// unit; merge join never reads them, so they are not carried. The
/// `comparator` is the [`SortComparatorId`] token described above.
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
    pub comparator: Option<SortComparatorId>,
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
        }
    }
}
