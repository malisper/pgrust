//! `AppendRelInfo` plan-data carrier (`nodes/pathnodes.h`).
//!
//! In C, `AppendRelInfo` is defined in pathnodes.h (planner-side), but the
//! flattened, RT-offset-adjusted list is recorded on `PlannerGlobal.appendRelations`
//! and carried onto `PlannedStmt.appendRelations` (plannodes.h) so the deparser
//! (`ruleutils.c` `get_variable`) can map an Append/MergeAppend child Var up to
//! its inheritance parent for EXPLAIN display.
//!
//! The planner-side full `AppendRelInfo` lives in `types-pathnodes`, which
//! depends on `types-nodes` (one-way), so the value cannot be referenced from
//! `PlannedStmt` here. This is the trimmed `'static` plan-data carrier the
//! deparser consumes: it holds exactly the fields `get_variable`'s child->parent
//! mapping reads. `set_plan_references` builds it from the owned
//! `PlannerInfo.append_rel_list` values (after the RT-index bumps), mirroring
//! C's `glob->appendRelations = lappend(glob->appendRelations, appinfo)`.

extern crate alloc;

use alloc::vec::Vec;

use types_core::primitive::{AttrNumber, Index, Oid};

/// Trimmed `AppendRelInfo` carried on `PlannedStmt.appendRelations`.
///
/// Fields mirror the C `AppendRelInfo` members read by the deparser:
/// `parent_relid`, `child_relid`, `num_child_cols`, `parent_colnos`,
/// `parent_reloid`. (`translated_vars`/`parent_reltype`/`child_reltype` are not
/// needed for deparse and are intentionally omitted from the carrier.)
#[derive(Clone, Debug, Default)]
pub struct AppendRelInfoCarrier {
    /// `Index parent_relid` — RT index of the append parent rel.
    pub parent_relid: Index,
    /// `Index child_relid` — RT index of the append child rel.
    pub child_relid: Index,
    /// `int num_child_cols` — length of `parent_colnos`.
    pub num_child_cols: i32,
    /// `AttrNumber *parent_colnos` — per child column, the 1-based parent
    /// column number (0 if dropped or absent in parent).
    pub parent_colnos: Vec<AttrNumber>,
    /// `Oid parent_reloid` — OID of the parent relation (InvalidOid for
    /// UNION ALL).
    pub parent_reloid: Oid,
}
