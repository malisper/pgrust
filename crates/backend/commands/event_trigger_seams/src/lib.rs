use pg_depend::ObjectAddress;
use types_error::PgResult;

seam_core::seam!(
    // trackDroppedObjectsNeeded (event_trigger.c): any sql_drop /
    // table_rewrite / ddl_command_end event trigger exists.
    pub fn track_dropped_objects_needed<'mcx>(mcx: mcx::Mcx<'mcx>) -> PgResult<bool>
);

seam_core::seam!(
    // EventTriggerSupportsObject (event_trigger.c).
    pub fn event_trigger_supports_object(object: &ObjectAddress) -> bool
);

seam_core::seam!(
    // EventTriggerSQLDropAddObject (event_trigger.c); no-op without an active
    // event-trigger query state.
    pub fn event_trigger_sql_drop_add_object<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        object: &ObjectAddress,
        original: bool,
        normal: bool,
    ) -> PgResult<()>
);

seam_core::seam!(
    // EventTriggerCollectGrant (event_trigger.c), including C's
    // EventTriggerSupportsObjectType guard at the ExecGrantStmt_oids call
    // site (aclchk.c:654-655).
    pub fn event_trigger_collect_grant(
        is_grant: bool,
        objtype: types_nodes::parsenodes::ObjectType,
    )
);

seam_core::seam!(
    // EventTriggerOnLogin (event_trigger.c), called from PostgresMain
    // (postgres.c:4369) below the crate graph's event_trigger node.
    pub fn event_trigger_on_login<'mcx>(mcx: mcx::Mcx<'mcx>) -> PgResult<()>
);
