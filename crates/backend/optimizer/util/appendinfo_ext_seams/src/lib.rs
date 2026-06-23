#![forbid(unsafe_code)]

//! Outward seam declarations for the not-yet-ported externals that
//! `optimizer/util/appendinfo.c` calls and that no other `-seams` crate already
//! declares — namely the FDW `AddForeignUpdateTargets` callback path of
//! `add_row_identity_columns`.
//!
//! These belong to the (per-FDW) foreign-table machinery whose routine
//! callbacks are not modeled at this layer, so each call panics loudly until the
//! real owner lands ("mirror PG and panic"). Homed here in a single
//! consumer-side seam crate with NO owner directory; the
//! `every_declared_seam_is_installed_by_its_owner` guard skips it because no
//! `backend-optimizer-util-appendinfo-ext` owner directory exists.

extern crate alloc;

use ::types_core::primitive::Oid;
use ::types_error::PgResult;
use ::nodes::nodes::CmdType;
use ::pathnodes::PlannerInfo;

seam_core::seam!(
    /// `GetFdwRoutineForRelation(target_relation, false)->AddForeignUpdateTargets
    /// != NULL` (appendinfo.c `add_row_identity_columns`) — whether the foreign
    /// table's FDW provides an `AddForeignUpdateTargets` callback.
    pub fn fdw_has_add_foreign_update_targets(relid: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `fdwroutine->AddForeignUpdateTargets(root, rtindex, target_rte,
    /// target_relation)` (appendinfo.c `add_row_identity_columns`) — let the
    /// foreign table's FDW add its row-identity resjunk TLEs to
    /// `root->processed_tlist`. `command_type` is `root->parse->commandType`,
    /// which the callback inspects for the UPDATE-vs-DELETE whole-row decision.
    pub fn fdw_add_foreign_update_targets(
        root: &mut PlannerInfo,
        rtindex: u32,
        relid: Oid,
        command_type: CmdType,
    ) -> PgResult<()>
);
