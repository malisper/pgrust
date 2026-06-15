//! Seam declarations for `parser/parse_clause.c` (the subset `parse_agg.c`
//! consumes): `transformSortClause`, `transformDistinctClause`,
//! `addTargetToSortList`.
//!
//! parse_clause.c is not yet ported. The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.
//!
//! In C, `transformSortClause`/`transformDistinctClause` take `List **targetlist`
//! (in/out — they may append resjunk columns). The owned model takes the
//! targetlist by value and returns the (possibly-extended) list alongside the
//! produced sort/distinct clause list.

use mcx::{Mcx, PgVec};
use types_error::PgResult;
use types_nodes::nodes::NodePtr;
use types_nodes::parsestmt::{ParseExprKind, ParseState};
use types_nodes::primnodes::TargetEntry;
use types_nodes::rawnodes::{SortBy, SortGroupClause};

/// Result of [`transform_sort_clause`] — the produced sort list plus the
/// (possibly-extended) target list (C's in/out `List **targetlist`).
pub struct SortClauseResult<'mcx> {
    /// `List *sortlist` (of `SortGroupClause`) returned by the C function.
    pub sortlist: PgVec<'mcx, SortGroupClause>,
    /// `*targetlist` after the function appended any resjunk ORDER BY columns.
    pub targetlist: PgVec<'mcx, TargetEntry<'mcx>>,
}

/// Result of [`transform_distinct_clause`].
pub struct DistinctClauseResult<'mcx> {
    /// `List *result` (of `SortGroupClause`) — the distinct list.
    pub distinctlist: PgVec<'mcx, SortGroupClause>,
    /// `*targetlist` after the function appended any resjunk columns.
    pub targetlist: PgVec<'mcx, TargetEntry<'mcx>>,
}

seam_core::seam!(
    /// `transformSortClause(pstate, orderlist, &targetlist, exprKind, useSQL99)`
    /// (parse_clause.c). `orderlist` is the raw list of `SortBy` nodes.
    pub fn transform_sort_clause<'mcx>(
        mcx: Mcx<'mcx>,
        pstate: &mut ParseState<'mcx>,
        orderlist: PgVec<'mcx, NodePtr<'mcx>>,
        targetlist: PgVec<'mcx, TargetEntry<'mcx>>,
        expr_kind: ParseExprKind,
        use_sql99: bool,
    ) -> PgResult<SortClauseResult<'mcx>>
);

seam_core::seam!(
    /// `transformDistinctClause(pstate, &targetlist, sortClause, is_agg)`
    /// (parse_clause.c).
    pub fn transform_distinct_clause<'mcx>(
        mcx: Mcx<'mcx>,
        pstate: &mut ParseState<'mcx>,
        targetlist: PgVec<'mcx, TargetEntry<'mcx>>,
        sort_clause: &[SortGroupClause],
        is_agg: bool,
    ) -> PgResult<DistinctClauseResult<'mcx>>
);

seam_core::seam!(
    /// `addTargetToSortList(pstate, tle, sortlist, targetlist, sortby)`
    /// (parse_clause.c) — append the `tle` to `sortlist` as a `SortGroupClause`,
    /// resolving the sort/eq operators from `sortby`. Returns the extended
    /// sortlist.
    pub fn add_target_to_sort_list<'mcx>(
        mcx: Mcx<'mcx>,
        pstate: &mut ParseState<'mcx>,
        tle: &TargetEntry<'mcx>,
        sortlist: PgVec<'mcx, SortGroupClause>,
        targetlist: &[TargetEntry<'mcx>],
        sortby: &SortBy<'mcx>,
    ) -> PgResult<PgVec<'mcx, SortGroupClause>>
);
