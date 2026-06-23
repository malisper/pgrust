//! ModifyTable plan-node ABI vocabulary shared with the `nodeModifyTable`
//! executor node crate.
//!
//! `ExecInitModifyTable` takes a `ModifyTable *` plan node (`nodes/plannodes.h`)
//! — the `Plan` base plus the INSERT/UPDATE/DELETE/MERGE descriptors. This
//! module mirrors the PostgreSQL 18.3 `#[repr(C)]` layout so the node-state
//! crate can read `node->operation`, `node->canSetTag`, the per-target-table
//! lists, the ON CONFLICT descriptors, and the MERGE action lists while
//! navigating the plan tree. Compile-time size/align/offset assertions pin the
//! layout where it crosses the ABI.
//!
//! The `Plan` base is reused from [`crate::nodeindexscan::Plan`] so every plan
//! node in this codebase shares one canonical `Plan` layout.

use crate::nodeindexscan::Plan;
use crate::{Bitmapset, Index, List, Node};

/// `CmdType` (`nodes/nodes.h`) — which command a query/plan node performs.
///
/// ```c
/// typedef enum CmdType
/// {
///     CMD_UNKNOWN,
///     CMD_SELECT,    /* select stmt */
///     CMD_UPDATE,    /* update stmt */
///     CMD_INSERT,    /* insert stmt */
///     CMD_DELETE,    /* delete stmt */
///     CMD_MERGE,     /* merge stmt */
///     CMD_UTILITY,   /* cmds like create, destroy, copy, vacuum, etc. */
///     CMD_NOTHING,   /* dummy command for instead nothing rules with qual */
/// } CmdType;
/// ```
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CmdType {
    CMD_UNKNOWN = 0,
    CMD_SELECT = 1,
    CMD_UPDATE = 2,
    CMD_INSERT = 3,
    CMD_DELETE = 4,
    CMD_MERGE = 5,
    CMD_UTILITY = 6,
    CMD_NOTHING = 7,
}

/// `OnConflictAction` (`nodes/nodes.h`) — the `ON CONFLICT` clause of an INSERT.
///
/// ```c
/// typedef enum OnConflictAction
/// {
///     ONCONFLICT_NONE,     /* No "ON CONFLICT" clause */
///     ONCONFLICT_NOTHING,  /* ON CONFLICT ... DO NOTHING */
///     ONCONFLICT_UPDATE,   /* ON CONFLICT ... DO UPDATE */
/// } OnConflictAction;
/// ```
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OnConflictAction {
    ONCONFLICT_NONE = 0,
    ONCONFLICT_NOTHING = 1,
    ONCONFLICT_UPDATE = 2,
}

/// `ModifyTable` (`nodes/plannodes.h`) — the plan node that applies an
/// INSERT/UPDATE/DELETE/MERGE to one or more target relations.
///
/// The executor reads `operation` and `canSetTag` directly off this node and
/// reaches the plan children through the `plan` base (`lefttree`). All other
/// fields drive `ExecInitModifyTable` when wiring up the per-target
/// `ResultRelInfo[]`, the projections, ON CONFLICT, RETURNING, and MERGE.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ModifyTable {
    pub plan: Plan,
    /// INSERT, UPDATE, DELETE, or MERGE
    pub operation: CmdType,
    /// do we set the command tag/es_processed?
    pub canSetTag: bool,
    /// Parent RT index for use of EXPLAIN
    pub nominalRelation: Index,
    /// Root RT index, if partitioned/inherited
    pub rootRelation: Index,
    /// some part key in hierarchy updated?
    pub partColsUpdated: bool,
    /// integer list of RT indexes
    pub resultRelations: *mut List,
    /// per-target-table update_colnos lists
    pub updateColnosLists: *mut List,
    /// per-target-table WCO lists
    pub withCheckOptionLists: *mut List,
    /// alias for OLD in RETURNING lists
    pub returningOldAlias: *mut core::ffi::c_char,
    /// alias for NEW in RETURNING lists
    pub returningNewAlias: *mut core::ffi::c_char,
    /// per-target-table RETURNING tlists
    pub returningLists: *mut List,
    /// per-target-table FDW private data lists
    pub fdwPrivLists: *mut List,
    /// indices of FDW DM plans
    pub fdwDirectModifyPlans: *mut Bitmapset,
    /// PlanRowMarks (non-locking only)
    pub rowMarks: *mut List,
    /// ID of Param for EvalPlanQual re-eval
    pub epqParam: core::ffi::c_int,
    /// ON CONFLICT action
    pub onConflictAction: OnConflictAction,
    /// List of ON CONFLICT arbiter index OIDs
    pub arbiterIndexes: *mut List,
    /// INSERT ON CONFLICT DO UPDATE targetlist
    pub onConflictSet: *mut List,
    /// target column numbers for onConflictSet
    pub onConflictCols: *mut List,
    /// WHERE for ON CONFLICT UPDATE
    pub onConflictWhere: *mut Node,
    /// RTI of the EXCLUDED pseudo relation
    pub exclRelRTI: Index,
    /// tlist of the EXCLUDED pseudo relation
    pub exclRelTlist: *mut List,
    /// per-target-table lists of actions for MERGE
    pub mergeActionLists: *mut List,
    /// per-target-table join conditions for MERGE
    pub mergeJoinConditions: *mut List,
}

#[cfg(test)]
mod abi_tests {
    use super::*;
    use core::mem::{align_of, offset_of, size_of};

    #[test]
    fn modifytable_abi_layout() {
        // `Plan` base sits at offset 0; `operation` follows immediately.
        assert_eq!(offset_of!(ModifyTable, plan), 0);
        assert_eq!(offset_of!(ModifyTable, operation), size_of::<Plan>());
        // C enums are `int`-sized.
        assert_eq!(size_of::<CmdType>(), 4);
        assert_eq!(size_of::<OnConflictAction>(), 4);
        assert_eq!(align_of::<CmdType>(), 4);
        // Pointer-aligned overall (largest member is an 8-byte pointer on 64-bit).
        assert_eq!(align_of::<ModifyTable>(), align_of::<*mut List>());
    }
}
