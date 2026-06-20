//! Seam declarations for the `backend-commands-event-trigger` unit
//! (`commands/event_trigger.c`), including the command-collection entry points
//! opclasscmds.c calls.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly. The collection routines append to the
//! `currentEventTriggerState` command list (the unported owner's backend
//! state); they no-op when no event-trigger collection is in progress.

#![allow(non_snake_case)]

use types_catalog::catalog_dependency::ObjectAddress;
use types_core::Oid;
use types_error::PgResult;
use types_opclass::{AlterOpFamilyStmt, CreateOpClassStmt, CreateOpFamilyStmt, OpFamilyMember};

seam_core::seam!(
    /// `EventTriggerCollectSimpleCommand(address, secondaryObject, (Node *) stmt)`
    /// (event_trigger.c) for a `ReindexStmt` — `reindex_index` records the REINDEX
    /// command for interested event triggers when invoked from a REINDEX command
    /// (`stmt != NULL`). `Err` carries any allocation failure of the collected
    /// command.
    pub fn event_trigger_collect_simple_command_reindex<'mcx>(
        address: ObjectAddress,
        secondary_object: ObjectAddress,
        stmt: &types_nodes::ddlnodes::ReindexStmt<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `EventTriggerTableRewrite(parsetree, tableOid, reason)` (event_trigger.c)
    /// — fire any `table_rewrite` event triggers before `ATRewriteTables`
    /// rewrites a table's heap. A no-op without an active event-trigger
    /// collection state (`currentEventTriggerState == NULL`, the standalone /
    /// no-relevant-trigger case); the active firing path
    /// (`EventTriggerCommonSetup` + `EventTriggerInvoke`) is part of the
    /// event-trigger firing sub-campaign and stops loudly until it lands.
    pub fn event_trigger_table_rewrite<'mcx>(
        parsetree: Option<&types_nodes::nodes::Node<'mcx>>,
        table_oid: Oid,
        reason: i32,
    ) -> PgResult<()>
);

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
    /// `EventTriggerOnLogin()` (event_trigger.c): fire any login event triggers
    /// at the start of a backend's main loop. Fast-exits unless a login event
    /// trigger is present in the connected database (`MyDatabaseHasLoginEventTriggers`);
    /// runs the triggers in a fresh transaction or, if the flag is stale, clears
    /// it in place. `Err` carries the fired triggers' / catalog `ereport(ERROR)`s.
    pub fn event_trigger_on_login() -> PgResult<()>
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

seam_core::seam!(
    /// `EventTriggerCollectSimpleCommand(address, secondaryObject, (Node *) stmt)`
    /// (event_trigger.c): record a CREATE SCHEMA command before its component
    /// subcommands are reported. `Err` carries any allocation failure of the
    /// collected command.
    pub fn event_trigger_collect_simple_command_create_schema(
        address: ObjectAddress,
        secondary_object: ObjectAddress,
        stmt: &types_nodes::ddlnodes::CreateSchemaStmt<'_>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `trackDroppedObjectsNeeded()` (event_trigger.c): whether any
    /// `ddl_command_end`/`sql_drop` event trigger (or the affected-object
    /// statistics) is interested in dropped objects, so dependency.c knows
    /// whether to track them. Can `ereport(ERROR)`, carried on `Err`.
    pub fn trackDroppedObjectsNeeded() -> PgResult<bool>
);

seam_core::seam!(
    /// `EventTriggerSupportsObject(object)` (event_trigger.c): whether the
    /// given object's class participates in event-trigger / SQL-drop reporting.
    /// Can `ereport(ERROR)`, carried on `Err`.
    pub fn EventTriggerSupportsObject(object: &ObjectAddress) -> PgResult<bool>
);

seam_core::seam!(
    /// `EventTriggerSQLDropAddObject(object, original, normal)`
    /// (event_trigger.c): record a dropped object in the current
    /// `sql_drop` event-trigger collection state. `original`/`normal` mirror
    /// the C flags distinguishing user-named vs dependency-implied drops. Can
    /// `ereport(ERROR)`, carried on `Err`.
    pub fn EventTriggerSQLDropAddObject(
        object: &ObjectAddress,
        original: bool,
        normal: bool,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `AlterEventTriggerOwner(const char *name, Oid newOwnerId)`
    /// (event_trigger.c) — ALTER EVENT TRIGGER ... OWNER TO.
    pub fn AlterEventTriggerOwner(
        name: &str,
        new_owner_id: Oid,
    ) -> PgResult<ObjectAddress>
);

seam_core::seam!(
    /// `estate->evtrigdata->event` (pl_exec.c `PLPGSQL_PROMISE_TG_EVENT`) — the
    /// event name string (`"ddl_command_start"` etc.) of the event trigger
    /// currently firing on this backend thread. `Ok(None)` mirrors the C
    /// `elog(ERROR, "event trigger promise is not in an event trigger function")`
    /// guard (`estate->evtrigdata == NULL`); the caller raises that error.
    pub fn event_trigger_get_event() -> PgResult<Option<String>>
);

seam_core::seam!(
    /// `GetCommandTagName(estate->evtrigdata->tag)` (pl_exec.c
    /// `PLPGSQL_PROMISE_TG_TAG`) — the command-tag name (e.g. `"CREATE TABLE"`)
    /// of the event trigger currently firing. `Ok(None)` is the same
    /// `evtrigdata == NULL` guard as [`event_trigger_get_event`].
    pub fn event_trigger_get_tag_name() -> PgResult<Option<String>>
);

seam_core::seam!(
    /// `EventTriggerCollectSimpleCommand(address, secondaryObject, (Node *) stmt)`
    /// (event_trigger.c) keyed by an owned `AlterPublicationStmt` — the
    /// publication CREATE/ALTER command-collection path (publicationcmds.c
    /// `PublicationAddTables` / `PublicationAddSchemas` / `AlterPublicationOptions`).
    pub fn event_trigger_collect_simple_command_publication<'mcx>(
        address: ObjectAddress,
        secondary_object: ObjectAddress,
        stmt: types_nodes::ddlnodes::AlterPublicationStmt<'mcx>,
    ) -> PgResult<()>
);
