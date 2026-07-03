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
