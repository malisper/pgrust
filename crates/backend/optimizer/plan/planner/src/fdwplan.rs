//! Planner half of the FdwRoutine callback table (fdwapi.h): per-provider fn
//! tables over planner-internal types, installed at init_seams time and keyed
//! by [`FdwKind`] (see types_nodes::fdw for the split-routine rationale).

//! No parallel entries: the parallel-FDW ABI was deleted outright (Michael's
//! 2026-07-20 ruling — the ABI is ours to change, only FDW UX is frozen; the
//! worker lane was never implemented). Foreign scans are serial-only by
//! construction; a future parallel foreign scan would arrive as a
//! morsel-native source, not as an IsForeignScanParallelSafe revival.

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

/// AddForeignUpdateTargets: the provider registers row-identity junk columns
/// through `register(expr, rowid_name)` — the caller adapts it to its tlist
/// (preprocess_targetlist) or add_row_identity_var (inheritance expansion).
pub type AddForeignUpdateTargets = for<'mcx> fn(
    mcx::Mcx<'mcx>,
    u32, // rtindex
    &mut dyn FnMut(Node<'mcx>, &'static str) -> PgResult<()>,
) -> PgResult<()>;

/// PlanDirectModify: may rewrite the ModifyTable's ForeignScan subplan into a
/// direct remote UPDATE/DELETE; true = rewritten (skip PlanForeignModify and
/// record the subplan in fdwDirectModifyPlans).
pub type PlanDirectModify = for<'mcx> fn(
    &mut PlannerRun<'mcx>,
    &mut types_nodes::plannodes::ModifyTable<'mcx>,
    u32,   // resultRelation rti
    usize, // subplan_index
) -> PgResult<bool>;

/// PlanForeignModify: returns the per-result-rel fdw_private list appended to
/// ModifyTable.fdwPrivLists (createplan.c make_modifytable's FDW loop).
pub type PlanForeignModify = for<'mcx> fn(
    &mut PlannerRun<'mcx>,
    &types_nodes::plannodes::ModifyTable<'mcx>,
    u32,   // resultRelation rti
    usize, // subplan_index
) -> PgResult<NodeList<'mcx>>;

/// GetForeignJoinPaths: (joinrel, outerrel, innerrel, jointype, sjinfo,
/// extra->restrictlist) — called from add_paths_to_joinrel when the joinrel
/// carries an fdwroutine (both sides same server/user).
pub type GetForeignJoinPaths = for<'mcx> fn(
    &mut PlannerRun<'mcx>,
    RelId,
    RelId,
    RelId,
    u32,
    &types_pathnodes::SpecialJoinInfo<'mcx>,
    &[RinfoId],
) -> PgResult<()>;

/// GetForeignUpperPaths (UPPERREL_GROUP_AGG lane): (stage, input_rel,
/// output_rel, extra->havingQual). ORDERED/FINAL stages are not yet offered.
pub type GetForeignUpperPaths = for<'mcx> fn(
    &mut PlannerRun<'mcx>,
    types_pathnodes::UpperRelationKind,
    RelId,
    RelId,
    Option<Node<'mcx>>,
) -> PgResult<()>;

pub struct FdwPlanRoutine {
    pub get_foreign_rel_size: for<'mcx> fn(&mut PlannerRun<'mcx>, RelId, Oid) -> PgResult<()>,
    pub get_foreign_paths: for<'mcx> fn(&mut PlannerRun<'mcx>, RelId, Oid) -> PgResult<()>,
    pub get_foreign_plan: GetForeignPlan,
    /// None = provider has no join pushdown (C's NULL slot).
    pub get_foreign_join_paths: Option<GetForeignJoinPaths>,
    /// None = provider has no upper-rel pushdown (C's NULL slot).
    pub get_foreign_upper_paths: Option<GetForeignUpperPaths>,
    pub add_foreign_update_targets: Option<AddForeignUpdateTargets>,
    pub plan_direct_modify: Option<PlanDirectModify>,
    pub plan_foreign_modify: Option<PlanForeignModify>,
    /// IsForeignPathAsyncCapable; None = never async (C's NULL slot).
    pub is_foreign_path_async_capable:
        Option<for<'mcx> fn(&PlannerRun<'mcx>, PathId) -> bool>,
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
