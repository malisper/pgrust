use std::rc::Rc;

seam_core::seam!(
    // ApplySetting(snapshot, databaseid, roleid, relsetting, source)
    // (catalog/pg_db_role_setting.c): scan the open pg_db_role_setting rel for
    // the (databaseid, roleid) row and apply its setconfig array.
    pub fn apply_setting<'a, 'mcx>(
        snapshot: &'a Rc<types_snapshot::SnapshotData<'static>>,
        databaseid: types_core::Oid,
        roleid: types_core::Oid,
        relsetting: &'a types_rel::Relation<'mcx>,
        source: types_guc::GucSource,
    ) -> types_error::PgResult<()>
);
