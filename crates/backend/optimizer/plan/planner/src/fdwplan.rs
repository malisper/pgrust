//! Planner half of the FdwRoutine callback table (fdwapi.h): per-provider fn
//! tables over planner-internal types, installed at init_seams time and keyed
//! by [`FdwKind`] (see types_nodes::fdw for the split-routine rationale).

use std::sync::OnceLock;

use mcx::PgVec;
use types_core::Oid;
use types_error::PgResult;
use types_nodes::list::NodeList;
use types_nodes::{FdwKind, Node, NUM_FDW_KINDS};
use types_pathnodes::{PathId, RelId, RinfoId};

use crate::run::PlannerRun;

/// GetForeignPlan: `scan_clauses` arrives order_qual_clauses-sorted; the FDW
/// typically runs extract_actual_clauses over it and calls make_foreignscan.
pub type GetForeignPlan = for<'mcx> fn(
    &mut PlannerRun<'mcx>,
    RelId,
    Oid,
    PathId,
    NodeList<'mcx>,
    PgVec<'mcx, RinfoId>,
    Option<Node<'mcx>>,
) -> PgResult<Node<'mcx>>;

pub struct FdwPlanRoutine {
    pub get_foreign_rel_size: for<'mcx> fn(&mut PlannerRun<'mcx>, RelId, Oid) -> PgResult<()>,
    pub get_foreign_paths: for<'mcx> fn(&mut PlannerRun<'mcx>, RelId, Oid) -> PgResult<()>,
    pub get_foreign_plan: GetForeignPlan,
    pub is_foreign_scan_parallel_safe:
        Option<for<'mcx> fn(&mut PlannerRun<'mcx>, RelId) -> PgResult<bool>>,
}

static ROUTINES: [OnceLock<&'static FdwPlanRoutine>; NUM_FDW_KINDS] =
    [const { OnceLock::new() }; NUM_FDW_KINDS];

pub fn install_fdw_plan_routine(kind: FdwKind, r: &'static FdwPlanRoutine) {
    if ROUTINES[kind.index()].set(r).is_err() {
        panic!("install_fdw_plan_routine: FdwPlanRoutine already installed for {kind:?}");
    }
}

pub(crate) fn fdw_plan_routine(kind: FdwKind) -> &'static FdwPlanRoutine {
    ROUTINES[kind.index()]
        .get()
        .unwrap_or_else(|| panic!("no FdwPlanRoutine installed for {kind:?}"))
}
