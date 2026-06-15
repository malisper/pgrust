//! Seam declarations for `optimizer/path/equivclass.c`, arena-shaped over
//! [`types_pathnodes::PlannerInfo`].
//!
//! indxpath.c's `match_eclass_clauses_to_index` calls
//! `generate_implied_equalities_for_column` to materialize join clauses from
//! EquivalenceClasses that match an index column. In C the column-matching test
//! is a callback (`ec_member_matches_indexcol`, ported in-crate); since
//! equivclass.c is unported here, the whole generator is a seam that takes the
//! index + index column it must match and returns the generated `RestrictInfo`
//! handles. Defaults to a loud panic until equivclass.c is ported.

extern crate alloc;

use alloc::vec::Vec;

use types_pathnodes::{IndexOptInfo, PlannerInfo, RelId, RinfoId};

seam_core::seam!(
    /// `generate_implied_equalities_for_column(root, rel, callback, callback_arg,
    /// prohibited_rels)` (equivclass.c) — for each EC, generate the implied
    /// equality join clauses whose this-rel side matches the given index column
    /// (the C callback is `ec_member_matches_indexcol`, supplied with `index` +
    /// `indexcol`). Returns the generated `RestrictInfo` handles. The
    /// `index`/`indexcol` carry the callback context; the matcher logic lives in
    /// indxpath (`ec_member_matches_indexcol`), so a real owner must invoke it.
    pub fn generate_implied_equalities_for_column(
        root: &mut PlannerInfo,
        rel: RelId,
        index: &IndexOptInfo,
        indexcol: i32
    ) -> Vec<RinfoId>
);
