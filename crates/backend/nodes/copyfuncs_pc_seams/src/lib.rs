//! plancache's slice of the node toolkit (`nodes/list.c`'s
//! `list_member_oid`). plancache walks a relation-OID `List` when checking
//! plan dependencies. The owning node unit installs this; until then a call
//! panics.
//!
//! The querytree / plan / parse-tree copy + dependency-extraction seams that
//! used to live here were retired by the #159 STEP C plancache de-handle:
//! plancache now owns its node values (`Query`/`PlannedStmt`/`RawStmt`/`Expr`)
//! and clones them via `clone_in` into private `MemoryContext`s, calling the
//! value seams (`extract_query_dependencies_value` /
//! `expression_planner_with_deps_value`) instead of the opaque-token handle
//! forms. Only the bare `list_member_oid` primitive (a `list.c` helper, not
//! part of the de-handle slice) remains.

use types_core::primitive::Oid;
use types_error::PgResult;

seam_core::seam!(
    /// `list_member_oid(relationOids, oid)` over a relation-OID `List`.
    pub fn list_member_oid(list: &[Oid], oid: Oid) -> PgResult<bool>
);
