//! Seam declarations for the `backend-commands-event-trigger` unit
//! (`commands/event_trigger.c`), including the command-collection entry points
//! opclasscmds.c calls.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly. The collection routines append to the
//! `currentEventTriggerState` command list (the unported owner's backend
//! state); they no-op when no event-trigger collection is in progress.

use types_catalog::catalog_dependency::ObjectAddress;
use types_core::Oid;
use types_error::PgResult;
use types_opclass::{AlterOpFamilyStmt, CreateOpClassStmt, CreateOpFamilyStmt, OpFamilyMember};

seam_core::seam!(
    /// `AlterEventTriggerOwner_oid(trigOid, newOwnerId)` (event_trigger.c):
    /// change an event trigger's owner during REASSIGN OWNED. Can
    /// `ereport(ERROR)`, carried on `Err`.
    pub fn alter_event_trigger_owner_oid(trig_oid: Oid, new_owner_id: Oid) -> PgResult<()>
);

seam_core::seam!(
    /// `get_event_trigger_oid(trigname, missing_ok)` (event_trigger.c): the OID
    /// of the named event trigger, or `InvalidOid` with `missing_ok = true`.
    /// With `missing_ok = false` a miss raises `ERRCODE_UNDEFINED_OBJECT`
    /// (`Err`).
    pub fn get_event_trigger_oid(trigname: &str, missing_ok: bool) -> PgResult<Oid>
);

seam_core::seam!(
    /// `EventTriggerCollectCreateOpClass(stmt, opclassoid, operators,
    /// procedures)` (event_trigger.c): record a CREATE OPERATOR CLASS for
    /// possibly-interested event triggers. `Err` carries any allocation
    /// failure of the collected command.
    pub fn event_trigger_collect_create_opclass(
        stmt: &CreateOpClassStmt,
        opclassoid: Oid,
        operators: &[OpFamilyMember],
        procedures: &[OpFamilyMember],
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `EventTriggerCollectAlterOpFam(stmt, opfamilyoid, operators,
    /// procedures)` (event_trigger.c): record an ALTER OPERATOR FAMILY
    /// ADD/DROP. `Err` carries any allocation failure of the collected
    /// command.
    pub fn event_trigger_collect_alter_opfam(
        stmt: &AlterOpFamilyStmt,
        opfamilyoid: Oid,
        operators: &[OpFamilyMember],
        procedures: &[OpFamilyMember],
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `EventTriggerCollectSimpleCommand(address, secondaryObject, stmt)`
    /// (event_trigger.c): record a simple DDL command (here, CREATE OPERATOR
    /// FAMILY). `Err` carries any allocation failure of the collected command.
    pub fn event_trigger_collect_simple_command(
        address: ObjectAddress,
        secondary_object: ObjectAddress,
        stmt: &CreateOpFamilyStmt,
    ) -> PgResult<()>
);
