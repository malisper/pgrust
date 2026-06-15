//! Simple per-RTE-kind pathlist setters that build a single scan path:
//! `set_function_pathlist` (allpaths.c:2795), `set_values_pathlist` (2862),
//! `set_tablefunc_pathlist` (2882), `set_namedtuplestore_pathlist` (2985),
//! `set_result_pathlist` (3012).
//!
//! (`set_cte_pathlist`/`set_worktable_pathlist` are in [`crate::subquery`] —
//! they resolve a CTE by name out of the unported Query subtree.)

use types_core::primitive::Index;
use types_error::PgResult;
use types_pathnodes::{PlannerInfo, RelId};

use backend_optimizer_util_pathnode_seams as pathnode;
use backend_optimizer_util_relnode_seams as bms;
use backend_optimizer_rte_seams as rte;

use crate::build_ordinality_pathkeys;

/// `set_function_pathlist` (allpaths.c:2795) — the single access path for a
/// function RTE. Ordered by the ordinal column when `WITH ORDINALITY` and that
/// column is referenced in an EquivalenceClass.
pub fn set_function_pathlist(root: &mut PlannerInfo, rel: RelId, rti: Index) -> PgResult<()> {
    // Function scans can't take join clauses; LATERAL refs may parameterize.
    let required_outer = bms::relids_copy::call(&root.rel(rel).lateral_relids);

    let pathkeys = if rte::rte_funcordinality::call(root, rti) {
        build_ordinality_pathkeys(root, rel)
    } else {
        alloc::vec::Vec::new()
    };

    let path = pathnode::create_functionscan_path::call(root, rel, pathkeys, &required_outer)?;
    pathnode::add_path::call(root, rel, path)?;
    Ok(())
}

/// `set_values_pathlist` (allpaths.c:2862) — the single access path for a VALUES
/// RTE.
pub fn set_values_pathlist(root: &mut PlannerInfo, rel: RelId) -> PgResult<()> {
    let required_outer = bms::relids_copy::call(&root.rel(rel).lateral_relids);
    let path = pathnode::create_valuesscan_path::call(root, rel, &required_outer)?;
    pathnode::add_path::call(root, rel, path)?;
    Ok(())
}

/// `set_tablefunc_pathlist` (allpaths.c:2882) — the single access path for a
/// table-function RTE.
pub fn set_tablefunc_pathlist(root: &mut PlannerInfo, rel: RelId) -> PgResult<()> {
    let required_outer = bms::relids_copy::call(&root.rel(rel).lateral_relids);
    let path = pathnode::create_tablefuncscan_path::call(root, rel, &required_outer)?;
    pathnode::add_path::call(root, rel, path)?;
    Ok(())
}

/// `set_namedtuplestore_pathlist` (allpaths.c:2985) — the single access path for
/// a named tuplestore RTE.
pub fn set_namedtuplestore_pathlist(root: &mut PlannerInfo, rel: RelId) -> PgResult<()> {
    // Mark rel with estimated output rows, width, etc.
    backend_optimizer_path_costsize::sizeest::set_namedtuplestore_size_estimates(root, rel);
    let required_outer = bms::relids_copy::call(&root.rel(rel).lateral_relids);
    let path = pathnode::create_namedtuplestorescan_path::call(root, rel, &required_outer)?;
    pathnode::add_path::call(root, rel, path)?;
    Ok(())
}

/// `set_result_pathlist` (allpaths.c:3012) — the single access path for an
/// `RTE_RESULT` RTE.
pub fn set_result_pathlist(root: &mut PlannerInfo, rel: RelId) -> PgResult<()> {
    // Mark rel with estimated output rows, width, etc.
    backend_optimizer_path_costsize::sizeest::set_result_size_estimates(root, rel);
    let required_outer = bms::relids_copy::call(&root.rel(rel).lateral_relids);
    let path = pathnode::create_resultscan_path::call(root, rel, &required_outer)?;
    pathnode::add_path::call(root, rel, path)?;
    Ok(())
}

extern crate alloc;
